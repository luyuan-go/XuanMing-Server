// mail 服务启动入口(2026-06-29)。
//
// 装配链:logger → MySQL(强依赖)→ Snowflake → repo/usecase/service → Kratos.Run。
// mail 不依赖 kafka(系统/公会邮件拉取式,个人邮件落库即达;红点推送复用 system.notify 由运营侧发)。
package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/mysqlx"
	"github.com/luyuancpp/pandora/pkg/safego"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	"github.com/luyuancpp/pandora/pkg/snowflake/etcdnode"

	"github.com/luyuancpp/pandora/services/social/mail/internal/biz"
	"github.com/luyuancpp/pandora/services/social/mail/internal/conf"
	"github.com/luyuancpp/pandora/services/social/mail/internal/data"
	"github.com/luyuancpp/pandora/services/social/mail/internal/server"
	"github.com/luyuancpp/pandora/services/social/mail/internal/service"
)

const serviceName = "mail"

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/mail-dev.yaml", "config file path")
}

func main() {
	flag.Parse()

	logger := plog.Setup(serviceName)
	helper := plog.NewHelper(logger)
	helper.Infow("msg", "service_starting", "conf", flagConf)

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

	// MySQL 强依赖(pandora_social:邮件 + 公会成员表)
	if cfg.Node.MySQLClient.DSN == "" {
		helper.Errorw("msg", "mysql_dsn_required", "hint", "node.mysql_client.dsn required (pandora_social)")
		os.Exit(1)
	}
	db := mysqlx.MustNewClient(cfg.Node.MySQLClient)
	defer func() { _ = db.Close() }()
	helper.Infow("msg", "mysql_connected", "dsn", maskDSN(cfg.Node.MySQLClient.DSN))

	// 严格模式断言(§9.24):非严格 sql_mode 下超长写入会被 MySQL **静默截断**
	// (err=nil 但数据被砍断),等于无声的数据损坏,故 fail-fast 而不是继续产生坏数据。
	if serr := dbguard.AssertStrictModeStartup(db); serr != nil {
		helper.Errorw("msg", "mysql_strict_mode_required", "err", serr)
		os.Exit(1)
	}

	// 容量巡检(§9.24):启动即跑一轮拿基线(上线时就已超限当场可见),之后每小时一轮。
	// 走 information_schema 估算(毫秒级不锁表);超预算只打 ERROR 日志 + metric,不阻止启动。
	capacityCtx, capacityCancel := context.WithCancel(context.Background())
	defer capacityCancel()
	go runCapacityGuard(capacityCtx, dbguard.New(db, "pandora_social", data.Budgets(), data.BigFields()))

	// 系统/公会邮件单节点生成,channel 内 mail_id 严格递增(游标比较零漏拉)
	// node_id_source=static 静态；=etcd 走 etcd 自动抢占独占 nodeID，失租自动退出
	//
	// ⚠️ etcd 模式只解决"重号",**不解决"多副本"**:ListMail 的系统/公会增量按
	// AdvanceCursor(max mail_id) 推水位,这依赖"同一 channel 的 mail_id 递增顺序 = 提交
	// 顺序"。该单调性只在单个发号器内成立 —— 两个副本各自铸号并发落库时,ID 大小与提交
	// 顺序脱钩,水位推过大 ID 后晚提交的小 ID 邮件会被**永久跳过**(玩家收不到)。
	// 所以 mail 扩到 >1 副本(含金丝雀)前必须先二选一:①系统/公会写路径保持单写者
	// (leader election + fencing,复用 pkg/dsauthfence/writerlease 模式);②游标改用
	// DB 自增 / 提交水位列,mail_id 只当主键不当游标。滚更的新旧并存窗口同样受此约束。
	sf, sfCloser := etcdnode.MustProvideSnowflake(serviceName, cfg.Node.NodeId, cfg.Snowflake)
	defer func() { _ = sfCloser.Close() }()

	repo := data.NewMySQLMailRepo(db)

	// inventory 客户端:领附件入库用。地址缺省且非测试空领 → 拒启,防裸奔丢奖
	var granter biz.ItemGranter
	var instGranter biz.InstanceGranter
	var xferClaimer biz.TransferClaimer
	if cfg.Mail.InventoryAddr != "" {
		g := data.NewGrpcItemGranter(cfg.Mail.InventoryAddr)
		defer func() { _ = g.Close() }()
		granter = g
		instGranter = g // 同一连接:装备实例型附件领取走 GrantInstances
		xferClaimer = g // 同一连接:transfer 托管转移附件领取走 ClaimTransferInstances
		helper.Infow("msg", "inventory_client_ready", "addr", cfg.Mail.InventoryAddr)
	} else if !cfg.Mail.AllowNoopGrant {
		helper.Errorw("msg", "inventory_addr_required", "hint", "mail.inventory_addr required, or set mail.allow_noop_grant for test")
		os.Exit(1)
	} else {
		helper.Warnw("msg", "inventory_noop_grant", "hint", "claim will only mark, no items granted (transfer claim stays rejected)")
	}

	uc := biz.NewMailUsecase(repo, cfg.Mail, granter)
	if instGranter != nil {
		uc.SetInstanceGranter(instGranter)
	}
	if xferClaimer != nil {
		uc.SetTransferClaimer(xferClaimer)
		// 同一连接:DS 三段式 Mark 消托管行(GrpcItemGranter 同时实现 TransferEscrowConsumer)。
		if consumer, ok := xferClaimer.(biz.TransferEscrowConsumer); ok {
			uc.SetTransferEscrowConsumer(consumer)
		}
	}
	// DS 三段式领取意图展开铸 instance ID(与系统/公会邮件 mail_id 共用同一雪花节点)。
	uc.SetInstanceIDGen(sf)
	mailSvc := service.NewMailService(uc, sf)

	// 过期清理:周期批量回收过期邮件 / 领取记录 / 归档,保证各表增长有界(biz/sweep.go)。
	// 多副本各自跑,删除幂等无需锁(对齐 leaderboard 发奖补扫模式)。
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	defer sweepCancel()
	go runMailSweep(sweepCtx, uc, cfg.Mail.SweepInterval.Std())

	// 会话现行性门(R5 复审 P0-1,INC-20260722-004):客户端面请求 jti 必须是 login
	// 会话权威(pandora:sess,node.redis_client 指向的共享 Redis)当前一代;
	// prod 生成器机械置 session_gate.require=true(漏配端点拒启)。
	sessGate, sgClose := sessiongate.MustBuild(cfg.Node.RedisClient, cfg.SessionGate.Require)
	defer sgClose()

	grpcSrv := server.NewGRPCServer(&cfg, mailSvc, sessGate)
	httpSrv := server.NewHTTPServer(&cfg)

	helper.Infow("msg", "service_ready", "grpc", cfg.Server.Grpc.Addr, "http", cfg.Server.Http.Addr)

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

// runMailSweep 周期跑一轮过期清理(对齐 dialogue runSessionSweep / leaderboard 补扫模式)。
func runMailSweep(ctx context.Context, uc *biz.MailUsecase, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// panic 兜底(压测审核【必修-6】同类点位):单轮 panic 只丢本轮,下轮继续。
			safego.Run(ctx, "mail_expired_sweep", func() { uc.SweepExpired(ctx, time.Now().UnixMilli()) })
		}
	}
}

// runCapacityGuard 启动即跑一轮容量巡检拿基线,之后每小时一轮(§9.24)。
// 走 information_schema 估算(毫秒级、不锁表、不扫数据),放启动路径安全;
// 绝不用 COUNT(*)(千万行表几十秒,会拖垮滚动更新)。超预算只告警不阻断。
func runCapacityGuard(ctx context.Context, g *dbguard.Guard) {
	safego.Run(ctx, "db_capacity_guard_initial", func() { g.Check(ctx) })
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			safego.Run(ctx, "db_capacity_guard", func() { g.Check(ctx) })
		}
	}
}

func maskDSN(dsn string) string {
	at, colon := -1, -1
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == ':' && colon == -1 {
			colon = i
		}
		if dsn[i] == '@' {
			at = i
			break
		}
	}
	if colon != -1 && at != -1 && at > colon {
		return dsn[:colon+1] + "***" + dsn[at:]
	}
	return dsn
}
