// Pandora auction 服务入口(全服拍卖行 / 撮合,2026-06-19)。
//
// 职责(docs/design/decision-revisit-auction-engine.md):
//
//	挂单 / 出价按 market_id 分片，单写者从 MySQL 精确物品候选做价格-时间优先撮合;
//	撮合候选与订单状态权威落 MySQL(pandora_auction,强依赖);Redis ZSET 保留作旧版本兼容缓存;
//	成交经 MySQL outbox 至少一次发 pandora.auction.match；流转 audit 仍是弱依赖。
//
// 启动顺序(对齐 trade / inventory):
//  1. Logger
//  2. 加载 yaml → conf.Defaults
//  3. MySQL(强依赖:撮合权威;单库 dsn 或分库 shards 按 market_id 路由)
//  4. Redis + Ping(当前部署强依赖:跨实例锁 + 旧版本兼容缓存)
//  5. Snowflake(order_id / match_id 生成)
//  6. kafka producer(match outbox 强持久、audit 弱依赖)→ pusher
//  7. SettlementLedger(配 inventory_addr 走真实结算;留空且 allow_noop_settlement=true 才退 Noop,否则 fail-fast)
//  8. 装配 AuctionUsecase → AuctionService → gRPC/HTTP server
//  9. kratos.New(...).Run() 阻塞
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/mysqlx"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/pkg/safego"
	"github.com/luyuancpp/pandora/pkg/snowflake/etcdnode"
	auctionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/auction/v1"

	"github.com/luyuancpp/pandora/services/economy/auction/internal/biz"
	"github.com/luyuancpp/pandora/services/economy/auction/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/auction/internal/data"
	"github.com/luyuancpp/pandora/services/economy/auction/internal/server"
	"github.com/luyuancpp/pandora/services/economy/auction/internal/service"
)

const (
	serviceName             = "auction"
	maxSupportedMySQLShards = 2
)

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/auction-dev.yaml", "config file path")
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

	// 3. MySQL(强依赖:撮合权威库 pandora_auction)。分库优先,否则单库。
	var router data.DBRouter
	var topologyDSNs []string
	if shardCount := len(cfg.Node.MySQLClient.Shards); shardCount > maxSupportedMySQLShards {
		helper.Errorw("msg", "auction_mysql_shard_count_unsupported", "shards", shardCount,
			"max_supported", maxSupportedMySQLShards,
			"hint", "扩到更多分片前必须完成 owner idempotency registry 回填与完成标记")
		os.Exit(1)
	} else if shardCount == 1 {
		helper.Errorw("msg", "auction_mysql_single_shard_list_invalid",
			"hint", "单库必须使用 node.mysql_client.dsn；shards 只允许恰好 2 个有序 DSN")
		os.Exit(1)
	}
	switch {
	case len(cfg.Node.MySQLClient.Shards) > 0:
		set, serr := mysqlx.NewShardSet(cfg.Node.MySQLClient)
		if serr != nil {
			helper.Errorw("msg", "mysql_shardset_failed", "err", serr)
			os.Exit(1)
		}
		defer func() {
			for _, db := range set.All() {
				_ = db.Close()
			}
		}()
		router = data.ShardedDB{Set: set}
		topologyDSNs = append([]string(nil), cfg.Node.MySQLClient.Shards...)
		helper.Infow("msg", "mysql_connected", "mode", "sharded", "shards", set.Count())
	case cfg.Node.MySQLClient.DSN != "":
		db := mysqlx.MustNewClient(cfg.Node.MySQLClient)
		defer func() { _ = db.Close() }()
		router = data.SingleDB{DB: db}
		topologyDSNs = []string{cfg.Node.MySQLClient.DSN}
		helper.Infow("msg", "mysql_connected", "mode", "single", "dsn", maskDSN(cfg.Node.MySQLClient.DSN))
	default:
		helper.Errorw("msg", "mysql_required", "hint", "node.mysql_client.dsn or .shards required (pandora_auction)")
		os.Exit(1)
	}
	// 严格模式断言(§9.24):**逐分片**断言——各分片可能连不同 MySQL 实例,
	// 配置漂移完全可能只出现在个别分片上,只查一个会漏。非严格 sql_mode 下超长写入
	// 被静默截断(err=nil 但数据被砍断),等于无声的数据损坏,故 fail-fast。
	for i, shardDB := range router.All() {
		if serr := dbguard.AssertStrictModeStartup(shardDB); serr != nil {
			helper.Errorw("msg", "mysql_strict_mode_required", "shard_index", i, "err", serr)
			os.Exit(1)
		}
	}

	// 容量巡检(§9.24):**逐分片**各建一个 Guard——分片可能连不同实例,
	// 且预算是按单分片量级给的(见 data.Budgets 注释),不能把总量摊在一个分片上。
	capacityCtx, capacityCancel := context.WithCancel(context.Background())
	defer capacityCancel()
	for _, shardDB := range router.All() {
		go runCapacityGuard(capacityCtx, dbguard.New(shardDB, "pandora_auction", data.Budgets(), nil))
	}

	topologyCtx, topologyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	topologyErr := data.ValidateShardTopology(topologyCtx, router, data.ShardTopologyOptions{
		Generation:     cfg.Auction.ShardTopologyGeneration,
		DSNs:           topologyDSNs,
		AllowBootstrap: cfg.Auction.AllowShardTopologyBootstrap,
	})
	topologyCancel()
	if topologyErr != nil {
		helper.Errorw("msg", "auction_mysql_shard_topology_rejected", "err", topologyErr,
			"hint", "do not change shard count/order/identity; first 2-shard start requires reviewed bootstrap")
		os.Exit(1)
	}
	helper.Infow("msg", "auction_mysql_shard_topology_verified",
		"generation", cfg.Auction.ShardTopologyGeneration, "shards", len(topologyDSNs))

	// 4. Redis(当前部署仍强依赖:跨实例锁 + 旧版本兼容缓存；不再承担撮合候选正确性)
	// 单实例填 host,Redis Cluster / Sentinel 只填 addrs,两者皆空才算未配置。
	rc := cfg.Node.RedisClient
	if rc.Host == "" && len(rc.Addrs) == 0 {
		helper.Errorw("msg", "redis_endpoint_required",
			"hint", "set node.redis_client.host (single) or node.redis_client.addrs (cluster)")
		os.Exit(1)
	}
	rdb := redisx.NewDeadlineUniversalClient(rc)
	defer func() { _ = rdb.Close() }()

	pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		cancel()
		helper.Errorw("msg", "redis_ping_failed", "err", err, "addr", rc.Host, "addrs", rc.Addrs)
		os.Exit(1)
	}
	cancel()
	helper.Infow("msg", "redis_connected", "addr", rc.Host, "addrs", rc.Addrs)

	// 5. Snowflake(node_id_source=static 静态，=etcd 走 etcd 自动抢占，失租自动退出)
	//
	// order_id 与 match_id 是两个独立 ID 空间，各取一个发号器：拍卖是全服最高频写入域，
	// 一笔大单在撮合循环里会连铸多个 match_id，与并发挂单的 order_id 各走各的 step 池
	// (每池 32768/s)。两者共用同一 nodeID、同一 lease，失租仍只有一处退出。
	// ⚠️ 共用 nodeID ⇒ 两个空间会发出逐位相同的 ID，各自留在 auction_orders /
	// auction_matches 里，禁止混进同一容器比较（见 etcdnode.ProvideSnowflakeN）。
	sfs, sfCloser := etcdnode.MustProvideSnowflakeN(serviceName, cfg.Node.NodeId, cfg.Snowflake, 2)
	defer func() { _ = sfCloser.Close() }()
	orderSF, matchSF := sfs[0], sfs[1]

	// 6. match 事件由 MySQL outbox 保证至少一次；broker 暂时不可用时 producer 可延迟重建，
	// marker 绝不清除。audit 仍是弱依赖。只有显式本地开关才允许完全禁用 match 事件。
	var events biz.AuctionEventPusher
	if len(cfg.Kafka.Brokers) > 0 {
		matchTopic := config.BuildTopic("auction", "match") // pandora.auction.match
		auditTopic := config.BuildTopic("auction", "audit") // pandora.auction.audit
		pusher := &auctionEventPusher{cfg: cfg.Kafka, matchTopic: matchTopic}
		if p, perr := kafkax.NewKeyOrderedProducer(cfg.Kafka, matchTopic); perr != nil {
			helper.Warnw("msg", "kafka_match_producer_init_failed_outbox_will_retry", "err", perr)
		} else {
			pusher.match = p
			helper.Infow("msg", "kafka_producer_ready", "topic", matchTopic)
		}
		if p, perr := kafkax.NewKeyOrderedProducer(cfg.Kafka, auditTopic); perr != nil {
			helper.Warnw("msg", "kafka_audit_producer_init_failed", "err", perr)
		} else {
			pusher.audit = p
			helper.Infow("msg", "kafka_producer_ready", "topic", auditTopic)
		}
		defer func() { _ = pusher.Close() }()
		events = pusher
	} else if cfg.Auction.AllowNoopMatchEvents {
		helper.Warnw("msg", "kafka_match_events_explicitly_disabled",
			"hint", "allow_noop_match_events=true; only valid for local no-event integration")
	} else {
		helper.Errorw("msg", "kafka_brokers_required",
			"hint", "configure kafka.brokers; only local no-event integration may set auction.allow_noop_match_events=true")
		os.Exit(1)
	}

	// 7. SettlementLedger:配了 inventory_addr → 走真实结算(inventory 卖↔买资产原子对转 +
	//    match_id 幂等);留空 → 仅当 allow_noop_settlement=true 才退回 NoopSettlementLedger
	//    占位(无交易联调 / 单测),否则 fail-fast 防生产漏配后静默以「成交不结算」启动。
	var ledger biz.SettlementLedger
	if addr := cfg.Auction.InventoryAddr; addr != "" {
		gl := data.NewGrpcInventoryLedger(addr)
		defer func() { _ = gl.Close() }()
		ledger = gl
		helper.Infow("msg", "settlement_ledger_ready", "mode", "inventory_grpc", "inventory_addr", addr)
	} else if cfg.Auction.AllowNoopSettlement {
		ledger = biz.NoopSettlementLedger{}
		helper.Warnw("msg", "settlement_ledger_noop", "hint", "auction.inventory_addr empty; matches settle as no-op (allow_noop_settlement=true)")
	} else {
		helper.Errorw("msg", "settlement_ledger_missing",
			"hint", "auction.inventory_addr 必填(真实结算);仅联调/单测可显式设 auction.allow_noop_settlement=true")
		os.Exit(1)
	}

	// 8. 装配链
	repo := data.NewMySQLAuctionRepo(router)
	book := data.NewRedisBookStore(rdb)
	ownerSlots := data.NewRedisOwnerSlotLimiter(rdb)
	uc := biz.NewAuctionUsecase(repo, book, ownerSlots, ledger, events, orderSF, matchSF, cfg.Auction)
	defer uc.Close() // 必须先停 audit worker，再由更早注册的 defer 关闭 Kafka producer。
	if mr, ok := biz.NewMarketRouter(cfg.CellRoute.MarketSelf, cfg.CellRoute.MarketPeerList()); ok {
		uc.SetMarketRouter(mr)
		helper.Infow("msg", "market_router_enabled", "self", mr.Self(), "peers", mr.PeerCount())
	}

	// 8a. Redis 本身已是强依赖，新二进制始终装配跨实例 market 锁。保留 cross_instance_lock
	// 字段只为旧版配置兼容；不能让自定义线上配置漏字段后静默退回进程锁而破坏单写者。
	if !cfg.Auction.CrossInstanceLock {
		helper.Warnw("msg", "cross_instance_lock_forced_on",
			"hint", "field retained for old-version compatibility; new auction always enables Redis market lock")
	}
	locker := data.NewRedisMarketLocker(rdb,
		time.Duration(cfg.Auction.MarketLockTTLSeconds)*time.Second,
		time.Duration(cfg.Auction.MarketLockMaxWaitMs)*time.Millisecond,
		0)
	uc.SetMarketLocker(locker)
	helper.Infow("msg", "market_locker_ready", "mode", "redis_cross_instance",
		"ttl_s", cfg.Auction.MarketLockTTLSeconds, "max_wait_ms", cfg.Auction.MarketLockMaxWaitMs)

	svc := service.NewAuctionService(uc)

	grpcSrv := server.NewGRPCServer(&cfg, svc)
	httpSrv := server.NewHTTPServer(&cfg)

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"redis_addr", rc.Host,
		"kafka_brokers", cfg.Kafka.Brokers,
	)

	// 9. Kratos App
	app := kratos.New(
		kratos.Name(serviceName),
		kratos.Logger(logger),
		kratos.Server(grpcSrv, httpSrv),
	)

	if cfg.Auction.PassiveWarmup {
		// R3 green 与旧 matcher 共存时必须完全只读：尤其不能先把 legacy 单置 verified，
		// 否则旧实例在 Settle 后崩溃会让新 matcher 按陈旧 remaining 再次 Reserve。
		helper.Warnw("msg", "auction_passive_warmup_enabled",
			"hint", "writes, legacy verifier, side-effect reconciler and expiry sweeper are disabled until all old auction instances stop")
	} else {
		// 9a. 持久副作用补偿：事务内只预留成交/终态并登记 PENDING，事务提交后调用 inventory。
		// Settle/Release 短暂失败或实例退出都不会丢意图；所有实例可并发扫描，幂等键与条件更新兜底。
		reconcileCtx, stopReconcile := context.WithCancel(context.Background())
		defer stopReconcile()
		reconcileInterval := time.Duration(cfg.Auction.SideEffectReconcileIntervalSeconds) * time.Second
		go func() {
			// 单轮 panic 兜底(压测审核【必修-6】同类点位):补偿是幂等 at-least-once,
			// 丢一轮下轮重试;不兜底则单点 panic 崩整个 auction 进程。
			run := func() {
				safego.Run(reconcileCtx, "auction_side_effect_reconcile", func() {
					settled, released, rerr := uc.ReconcilePendingSideEffects(reconcileCtx)
					if rerr != nil {
						helper.Warnw("msg", "auction_side_effect_reconcile_incomplete", "err", rerr,
							"settled", settled, "released", released)
					} else if settled > 0 || released > 0 {
						helper.Infow("msg", "auction_side_effect_reconcile", "settled", settled, "released", released)
					}
				})
			}
			run() // 启动即恢复上次进程遗留，不先空等一个周期。
			ticker := time.NewTicker(reconcileInterval)
			defer ticker.Stop()
			for {
				select {
				case <-reconcileCtx.Done():
					return
				case <-ticker.C:
					run()
				}
			}
		}()
		helper.Infow("msg", "side_effect_reconciler_ready",
			"interval_s", cfg.Auction.SideEffectReconcileIntervalSeconds,
			"batch_per_shard", cfg.Auction.SideEffectReconcileBatch)

		// Kafka outbox 必须与资产补偿分 goroutine：底层同步 producer 不可靠响应 context，
		// broker 故障只能阻塞事件 worker，不能阻塞下一轮 Settle/Release。
		eventCtx, stopEvents := context.WithCancel(context.Background())
		defer stopEvents()
		go func() {
			run := func() {
				safego.Run(eventCtx, "auction_match_event_reconcile", func() {
					published, perr := uc.ReconcilePendingMatchEvents(eventCtx)
					if perr != nil {
						helper.Warnw("msg", "auction_match_event_reconcile_incomplete", "err", perr,
							"published", published)
					} else if published > 0 {
						helper.Infow("msg", "auction_match_event_reconcile", "published", published)
					}
				})
			}
			run()
			ticker := time.NewTicker(reconcileInterval)
			defer ticker.Stop()
			for {
				select {
				case <-eventCtx.Done():
					return
				case <-ticker.C:
					run()
				}
			}
		}()
		helper.Infow("msg", "match_event_reconciler_ready",
			"interval_s", cfg.Auction.SideEffectReconcileIntervalSeconds,
			"batch_per_shard", cfg.Auction.SideEffectReconcileBatch)

		// 9c. 保留期清理(§9.24):终态挂单 / 已结算成交流水 / 超期幂等键映射逐分片批删,
		//     只增表增长有界(data/retention.go)。多副本各自跑,DELETE 幂等无需锁。
		retentionCtx, stopRetention := context.WithCancel(context.Background())
		defer stopRetention()
		go func() {
			interval := time.Duration(cfg.Auction.RetentionSweepIntervalSeconds) * time.Second
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-retentionCtx.Done():
					return
				case <-ticker.C:
					safego.Run(retentionCtx, "auction_retention_sweep", func() {
						cutoffMs := time.Now().AddDate(0, 0, -cfg.Auction.RetentionDays).UnixMilli()
						// mode 默认 report_only:各表待清理量由 dbguard.SweepTable 统一 WARN 告警,
						// 这里只在真删发生时补一条业务 INFO。
						out, rerr := repo.SweepRetention(retentionCtx, cfg.Auction.RetentionMode(),
							cutoffMs, cfg.Auction.RetentionSweepBatch)
						if rerr != nil {
							helper.Warnw("msg", "auction_retention_sweep_failed", "err", rerr,
								"orders", out.Orders.Deleted, "matches", out.Matches.Deleted, "idem_keys", out.IdemKeys.Deleted)
						} else if out.Cleaned() {
							helper.Infow("msg", "auction_retention_swept",
								"orders", out.Orders.Deleted, "matches", out.Matches.Deleted, "idem_keys", out.IdemKeys.Deleted,
								"retention_days", cfg.Auction.RetentionDays)
						}
					})
				}
			}
		}()
		helper.Infow("msg", "retention_sweeper_ready",
			"retention_days", cfg.Auction.RetentionDays,
			"interval_s", cfg.Auction.RetentionSweepIntervalSeconds,
			"batch_per_shard", cfg.Auction.RetentionSweepBatch)

		// 9b. 过期清扫(限制#1 补偿):OrderTTLSeconds > 0 时起后台 ticker,周期把超 TTL 仍未成交的
		//     挂单置 EXPIRED、移出簿、退还 escrow。随 app 生命周期退出(ctx 取消)。
		if cfg.Auction.OrderTTLSeconds > 0 {
			sweepCtx, stopSweep := context.WithCancel(context.Background())
			defer stopSweep()
			interval := time.Duration(cfg.Auction.ExpirySweepIntervalSeconds) * time.Second
			go func() {
				ticker := time.NewTicker(interval)
				defer ticker.Stop()
				for {
					select {
					case <-sweepCtx.Done():
						return
					case <-ticker.C:
						safego.Run(sweepCtx, "auction_expiry_sweep", func() {
							n, serr := uc.ExpireDueOrders(sweepCtx)
							if serr != nil {
								helper.Warnw("msg", "auction_expiry_sweep_failed", "err", serr)
							} else if n > 0 {
								helper.Infow("msg", "auction_expiry_sweep", "expired", n)
							}
						})
					}
				}
			}()
			helper.Infow("msg", "expiry_sweeper_ready", "ttl_s", cfg.Auction.OrderTTLSeconds, "interval_s", cfg.Auction.ExpirySweepIntervalSeconds)
		}
	}

	if err := app.Run(); err != nil {
		helper.Errorw("msg", "app_run_failed", "err", err)
		os.Exit(1)
	}
}

// auctionEventPusher 把 biz.AuctionEventPusher 适配到 kafkax.KeyOrderedProducer。
//   - 成交 → pandora.auction.match,kafka key = match_id(同一成交保序,不变量 §9)
//   - 流转 → pandora.auction.audit,kafka key = order_id(同一挂单保序)
type auctionEventPusher struct {
	mu          sync.Mutex
	matchInitMu sync.Mutex
	cfg         config.KafkaConfig
	matchTopic  string
	match       *kafkax.KeyOrderedProducer
	audit       *kafkax.KeyOrderedProducer
	closed      bool
}

func (k *auctionEventPusher) PushMatch(ctx context.Context, e *auctionv1.AuctionMatchEvent) error {
	p, err := k.matchProducer()
	if err != nil {
		return err
	}
	return p.Send(ctx, strconv.FormatUint(e.GetMatchId(), 10), e)
}

func (k *auctionEventPusher) PushAudit(ctx context.Context, o *auctionv1.AuctionOrder) error {
	k.mu.Lock()
	p := k.audit
	closed := k.closed
	k.mu.Unlock()
	if closed || p == nil {
		return nil
	}
	return p.Send(ctx, strconv.FormatUint(o.GetOrderId(), 10), o)
}

func (k *auctionEventPusher) matchProducer() (*kafkax.KeyOrderedProducer, error) {
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return nil, fmt.Errorf("auction event pusher closed")
	}
	if k.match != nil {
		p := k.match
		k.mu.Unlock()
		return p, nil
	}
	k.mu.Unlock()

	// 只串行 producer 构造，不持状态锁做网络连接；否则 match broker 初始化会阻塞健康 audit
	// 读取状态。业务 audit 另有有界异步队列，即使这里长时间失败也不会反压交易路径。
	k.matchInitMu.Lock()
	defer k.matchInitMu.Unlock()
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return nil, fmt.Errorf("auction event pusher closed")
	}
	if k.match != nil {
		p := k.match
		k.mu.Unlock()
		return p, nil
	}
	k.mu.Unlock()

	p, err := kafkax.NewKeyOrderedProducer(k.cfg, k.matchTopic)
	if err != nil {
		return nil, fmt.Errorf("reconnect auction match producer: %w", err)
	}
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		_ = p.Close()
		return nil, fmt.Errorf("auction event pusher closed")
	}
	k.match = p
	k.mu.Unlock()
	return p, nil
}

func (k *auctionEventPusher) Close() error {
	k.mu.Lock()
	if k.closed {
		k.mu.Unlock()
		return nil
	}
	k.closed = true
	match, audit := k.match, k.audit
	k.match, k.audit = nil, nil
	k.mu.Unlock()
	var firstErr error
	if match != nil {
		firstErr = match.Close()
	}
	if audit != nil {
		if err := audit.Close(); firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// runCapacityGuard 启动即跑一轮容量巡检拿基线,之后每小时一轮(§9.24)。
// 走 information_schema 估算(毫秒级、不锁表);超预算只告警不阻断。
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

// maskDSN 脱敏 DSN 里的密码(对齐 trade / inventory main.go)。
func maskDSN(dsn string) string {
	at := -1
	colon := -1
	for i := 0; i < len(dsn); i++ {
		if dsn[i] == ':' && colon == -1 {
			colon = i
		}
		if dsn[i] == '@' {
			at = i
			break
		}
	}
	if colon >= 0 && at > colon {
		return dsn[:colon+1] + "***" + dsn[at:]
	}
	return dsn
}
