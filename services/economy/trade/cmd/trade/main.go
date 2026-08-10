// Pandora trade 服务入口(2026-06-16)。
//
// 职责:玩家间两阶段确认交易;订单存 Redis(强依赖);结算走 ResourceLedger
// 原子扣减 + 幂等键 = order_id(不变量 §9.7);状态流转快照发 kafka
// pandora.trade.audit(弱依赖,审计 / 对账)。
//
// 启动顺序(对齐 team):
//  1. 解析 -conf 路径,加载 yaml
//  2. conf.Defaults 填默认值
//  3. log.Setup → 全局 zap logger
//  4. Redis client + Ping(强依赖:订单状态机不可降级)
//  5. Snowflake Node(order_id 生成)
//  6. kafka producer(topic=pandora.trade.audit)→ tradeAuditPusher(弱依赖)
//  7. ResourceLedger:真实账本未接入前需显式 allow_noop_ledger=true 才退回 NoopResourceLedger,否则 fail-fast
//  8. 装配 TradeUsecase → TradeService → gRPC/HTTP server
//  9. kratos.New(...).Run() 阻塞
package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/pkg/cellroute/etcdtable"
	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	"github.com/luyuancpp/pandora/pkg/snowflake/etcdnode"
	tradev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/trade/v1"

	"github.com/luyuancpp/pandora/services/economy/trade/internal/biz"
	"github.com/luyuancpp/pandora/services/economy/trade/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/trade/internal/data"
	"github.com/luyuancpp/pandora/services/economy/trade/internal/server"
	"github.com/luyuancpp/pandora/services/economy/trade/internal/service"
)

const serviceName = "trade"

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/trade-dev.yaml", "config file path")
}

func main() {
	flag.Parse()

	// 1. Logger
	logger := plog.Setup(serviceName)
	helper := plog.NewHelper(logger)
	helper.Infow("msg", "service_starting", "conf", flagConf)

	// 2. 加载 yaml
	cfgPath, err := filepath.Abs(flagConf)
	if err != nil {
		helper.Errorw("msg", "abs_conf_path_failed", "err", err)
		os.Exit(1)
	}
	c := kconfig.New(kconfig.WithSource(file.NewSource(cfgPath)))
	defer func() { _ = c.Close() }()

	if err := c.Load(); err != nil {
		helper.Errorw("msg", "config_load_failed", "err", err, "path", cfgPath)
		os.Exit(1)
	}

	var cfg conf.Config
	if err := c.Scan(&cfg); err != nil {
		helper.Errorw("msg", "config_scan_failed", "err", err)
		os.Exit(1)
	}
	cfg.Defaults()

	// 3. Redis(强依赖:订单状态机不可降级)
	// 单实例填 host,Redis Cluster / Sentinel 只填 addrs,两者皆空才算未配置。
	rc := cfg.Node.RedisClient
	if rc.Host == "" && len(rc.Addrs) == 0 {
		helper.Errorw("msg", "redis_endpoint_required",
			"hint", "set node.redis_client.host (single) or node.redis_client.addrs (cluster)")
		os.Exit(1)
	}
	rdb := redisx.NewUniversalClient(rc)
	defer func() { _ = rdb.Close() }()

	pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancel()
		helper.Errorw("msg", "redis_ping_failed", "err", err, "addr", rc.Host, "addrs", rc.Addrs)
		os.Exit(1)
	}
	cancel()
	helper.Infow("msg", "redis_connected", "addr", rc.Host, "addrs", rc.Addrs)

	// 4. Snowflake(order_id 生成；node_id_source=static 静态，=etcd 走 etcd 自动抢占，失租自动退出)
	sf, sfCloser := etcdnode.MustProvideSnowflake(serviceName, cfg.Node.NodeId, cfg.Snowflake)
	defer func() { _ = sfCloser.Close() }()

	// 5. kafka producer → tradeAuditPusher(弱依赖:broker 不通则 warn 并继续,审计静默 fail)
	auditTopic := config.BuildTopic("trade", "audit") // pandora.trade.audit
	var audit biz.TradeAuditPusher
	if len(cfg.Kafka.Brokers) > 0 {
		producer, perr := kafkax.NewKeyOrderedProducer(cfg.Kafka, auditTopic)
		if perr != nil {
			helper.Warnw("msg", "kafka_producer_init_failed", "err", perr,
				"hint", "trade audit silently dropped until kafka is available")
		} else {
			defer func() { _ = producer.Close() }()
			audit = &tradeAuditPusher{p: producer}
			helper.Infow("msg", "kafka_producer_ready", "topic", auditTopic)
		}
	} else {
		helper.Warnw("msg", "kafka_brokers_empty", "hint", "trade audit disabled")
	}

	// 6. ResourceLedger:优先接真实 inventory P2P 原子对转(GrpcResourceLedger);
	// 配 trade.inventory_addr → 直连 inventory;未配且 allow_noop_ledger=true → 退回 NoopResourceLedger;
	// 都没有 → fail-fast,拒绝以「成交不扣减资产」静默启动(审计:trade 静默 Noop 结算降级)。
	var ledger biz.ResourceLedger
	if cfg.Trade.InventoryAddr != "" {
		grpcLedger := data.NewGrpcResourceLedger(cfg.Trade.InventoryAddr)
		defer func() { _ = grpcLedger.Close() }()
		ledger = grpcLedger
		helper.Infow("msg", "resource_ledger_grpc", "inventory_addr", cfg.Trade.InventoryAddr)
	} else if cfg.Trade.AllowNoopLedger {
		helper.Warnw("msg", "resource_ledger_noop",
			"hint", "trade 结算走 NoopResourceLedger(总成功,不真实扣转背包/货币);仅限联调/单测")
		ledger = biz.NoopResourceLedger{}
	} else {
		helper.Errorw("msg", "resource_ledger_not_configured",
			"hint", "设 trade.inventory_addr 接真实 inventory P2P 原子对转;联调/单测可显式设 trade.allow_noop_ledger=true,生产不可空跑")
		os.Exit(1)
	}

	// 7. 装配链
	repo := data.NewRedisTradeRepo(rdb)
	uc := biz.NewTradeUsecase(repo, ledger, audit, sf, cfg.Trade)
	// 下单/撤单频率配额(anti-abuse §6 第 6 项):复用共享 rdb,窗口固定 1 分钟。
	uc.SetRateQuota(&redisx.ActionQuota{
		RDB: rdb, Domain: "trade",
		Limit: int64(cfg.Trade.RateQuotaPerMin), Window: time.Minute,
	})
	helper.Infow("msg", "trade_rate_quota_ready", "per_min", cfg.Trade.RateQuotaPerMin)
	if closeCell, e := etcdtable.WireRouter(context.Background(), cfg.CellRoute, uc.SetCellRouter); e != nil {
		helper.Errorw("msg", "cellroute_init_failed", "err", e)
		os.Exit(1)
	} else if closeCell != nil {
		defer func() { _ = closeCell() }()
	}
	svc := service.NewTradeService(uc)

	// 会话现行性门(R5 复审 P0-1,INC-20260722-004):客户端面请求 jti 必须是 login
	// 会话权威(pandora:sess,node.redis_client 指向的共享 Redis)当前一代;
	// prod 生成器机械置 session_gate.require=true(漏配端点拒启)。
	sessGate, sgClose := sessiongate.MustBuild(cfg.Node.RedisClient, cfg.SessionGate.Require)
	defer sgClose()

	grpcSrv := server.NewGRPCServer(&cfg, svc, sessGate)
	httpSrv := server.NewHTTPServer(&cfg)

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"redis_addr", rc.Host,
		"kafka_brokers", cfg.Kafka.Brokers,
		"order_expire", cfg.Trade.OrderExpire.String(),
	)

	// 8. Kratos App
	app := kratos.New(
		kratos.Name(serviceName),
		kratos.Logger(logger),
		kratos.Server(grpcSrv, httpSrv),
	)
	if err := app.Run(); err != nil {
		helper.Errorw("msg", "app_run_failed", "err", err)
		os.Exit(1)
	}
}

// tradeAuditPusher 把 biz.TradeAuditPusher 接口适配到 kafkax.KeyOrderedProducer。
// kafka key = order_id(不变量 §9:同一订单的审计事件保序)。
type tradeAuditPusher struct {
	p *kafkax.KeyOrderedProducer
}

func (k *tradeAuditPusher) PushAudit(ctx context.Context, order *tradev1.Order) error {
	return k.p.Send(ctx, strconv.FormatUint(order.GetOrderId(), 10), order)
}
