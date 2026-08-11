// Pandora battle_result 服务入口(W4 ③,2026-06-06)。
//
// 职责:Model-B 经受 Guard + Redis active 校验的同步 ReportResult 幂等落库并算 MMR；
// legacy/off 才可选消费 pandora.battle.result。始终消费 pandora.ds.lifecycle 的
// ABANDONED 做 DS 崩溃补偿(不变量 §4),
// 落库同事务写 player.update 出箱 + 后台发布器可靠投递(W4 ⑨ 不变量 §4),
// 并提供战绩查询 RPC。
//
// 启动顺序(对齐 ds_allocator / push):
//  1. 解析 -conf 路径,加载 yaml
//  2. conf.Defaults 填默认值
//  3. log.Setup → 全局 zap logger
//  4. MySQL client + Ping(强依赖:结算落库不可降级)
//  5. MMR reader(W4 ③ player 未上线 → StaticMMRReader)
//  6. player.update kafka producer(弱依赖:broker 不通则 warn;player.update 已写事务出箱,
//     producer/broker 不可用时出箱积压不丢,等 producer 可用后由发布器补发,当前需重启/重配)
//  7. 装配 BattleResultUsecase → BattleResultService → gRPC/HTTP server
//  8. 按 ConsumeTopics 每 topic 一个 KafkaConsumer；Model-B 只允许 ds.lifecycle
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
	klog "github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/cellroute/etcdtable"
	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/dsauthfence"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/mysqlx"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/pkg/safego"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"

	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/biz"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/data"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/server"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/service"
)

const serviceName = "battle_result"

// Kafka 消费失败处理:业务瞬时错误进程内重试 dlqMaxRetries 次(间隔 dlqRetryBackoff)后进 DLQ
// (infra.md §4.4「失败 3 次进 DLQ」)。
const (
	dlqMaxRetries   = 3
	dlqRetryBackoff = 500 * time.Millisecond
)

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/battle_result-dev.yaml", "config file path")
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
	if err := cfg.DSAuth.ValidateRedisFence(); err != nil {
		helper.Errorw("msg", "ds_auth_fence_config_invalid", "err", err)
		os.Exit(1)
	}
	if err := cfg.ValidateRedisAuthorityIngress(); err != nil {
		helper.Errorw("msg", "battle_result_ingress_invalid", "err", err,
			"hint", "Model-B 只允许受 Guard/Redis active/receipt 保护的 ReportResult RPC；Kafka 只保留 ds.lifecycle")
		os.Exit(1)
	}
	// 保留期清理模式必须能被识别(§9.24 fail-fast):本服默认真删(战报只留六个月),
	// 拼错的模式值会静默回落 report_only —— 库继续无界增长且没人发现,必须拒启。
	if err := cfg.Battle.ValidateRetentionMode(); err != nil {
		helper.Errorw("msg", "battle_retention_mode_invalid", "err", err,
			"hint", "battle.retention_mode 只接受 \"delete\"(留空即此) 或 \"report_only\"")
		os.Exit(1)
	}

	// 3. MySQL(强依赖:结算落库不可降级)
	if cfg.Node.MySQLClient.DSN == "" {
		helper.Errorw("msg", "mysql_dsn_required", "hint", "node.mysql_client.dsn required (pandora_battle)")
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
	go runCapacityGuard(capacityCtx, dbguard.New(db, "pandora_battle", data.Budgets(), data.BigFields()))

	// 4. MMR reader(W4 ④ player 上线 → 接真实 player gRPC reader;PlayerAddr 空则静态 BaseMMR 兜底)
	var mmr biz.MMRReader
	if cfg.Battle.PlayerAddr != "" {
		reader := data.NewGrpcMMRReader(cfg.Battle.PlayerAddr)
		defer func() { _ = reader.Close() }()
		mmr = reader
		helper.Infow("msg", "mmr_reader_grpc", "player_addr", cfg.Battle.PlayerAddr)
	} else {
		mmr = biz.NewStaticMMRReader(cfg.Battle.BaseMMR)
		helper.Infow("msg", "mmr_reader_static", "base_mmr", cfg.Battle.BaseMMR,
			"hint", "player_addr 未配置 → StaticMMRReader 兜底")
	}

	// 5. player.update producer(出箱发布器使用;init 失败则出箱积压等 producer 可用,不丢)
	var pusher biz.PlayerUpdatePusher
	if len(cfg.Kafka.Brokers) > 0 {
		producer, perr := kafkax.NewKeyOrderedProducer(cfg.Kafka, kafkax.TopicPlayerUpdate)
		if perr != nil {
			helper.Warnw("msg", "player_update_producer_init_failed", "err", perr,
				"hint", "outbox rows accumulate (not dropped); publisher resumes when producer is available")
		} else {
			defer func() { _ = producer.Close() }()
			pusher = &playerUpdatePusher{p: producer}
			helper.Infow("msg", "player_update_producer_ready", "topic", kafkax.TopicPlayerUpdate)
		}
	} else {
		helper.Warnw("msg", "kafka_brokers_empty", "hint", "outbox publisher idle until brokers configured")
	}

	// 6. 装配链
	repo := data.NewMySQLBattleRepo(db)
	schemaCtx, schemaCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := repo.ValidateRecoveryOutboxSchema(schemaCtx); err != nil {
		schemaCancel()
		helper.Errorw("msg", "battle_recovery_outbox_schema_invalid", "err", err,
			"hint", "apply pandora_battle migration 000003_match_release_outbox")
		os.Exit(1)
	}
	// 实时进度三表在每次结算都被无条件访问(settleProgressStreamTx 收口),
	// 与 killswitch 无关,必须启动即探测(不能 Ready 后首个结算才炸,§16.4)。
	if err := repo.ValidateProgressSchema(schemaCtx); err != nil {
		schemaCancel()
		helper.Errorw("msg", "battle_progress_schema_invalid", "err", err,
			"hint", "apply pandora_battle migrations 000005_battle_progress + 000006_battle_progress_player + 000008_battle_progress_stopped")
		os.Exit(1)
	}
	schemaCancel()
	var terminalRelay *data.GrpcTerminalReleaseRelay
	var authRedis redis.UniversalClient
	if cfg.DSAuth.AuthorityModeRedis() {
		// 只有精确 v2 schema 已迁移，才允许构造 relay、注册 capability、接收结算。
		// 不能让新副本先 Ready 后在首个 ReportResult 才发现表缺失。
		probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		probeErr := repo.ValidateTerminalReleaseSchema(probeCtx)
		cancel()
		if probeErr != nil {
			helper.Errorw("msg", "terminal_release_schema_invalid", "err", probeErr,
				"hint", "先执行 pandora_battle/000002_terminal_release_outbox migration")
			os.Exit(1)
		}
		terminalRelay = data.NewGrpcTerminalReleaseRelay(cfg.Battle.DSAllocatorAddr)
		defer func() { _ = terminalRelay.Close() }()

		rc := cfg.Node.RedisClient
		if rc.Host == "" && len(rc.Addrs) == 0 {
			helper.Errorw("msg", "battle_auth_redis_required")
			os.Exit(1)
		}
		authRedis = redisx.NewUniversalClient(rc)
		defer func() { _ = authRedis.Close() }()
		pingCtx, pingCancel := context.WithTimeout(context.Background(), 3*time.Second)
		if err := authRedis.Ping(pingCtx).Err(); err != nil {
			pingCancel()
			helper.Errorw("msg", "battle_auth_redis_ping_failed", "err", err)
			os.Exit(1)
		}
		pingCancel()
		helper.Infow("msg", "terminal_release_dependencies_ready",
			"ds_allocator_addr", cfg.Battle.DSAllocatorAddr,
			"grace", cfg.Battle.TerminalReleaseGrace.String())
	}

	// 6.0 matchmaker releaser. Redis authority treats this as a startup
	// dependency: match/ticket/player claims are durable and intentionally have
	// no non-terminal TTL, so silently disabling the outbox consumer would
	// strand every settled player indefinitely. Local legacy profiles retain the
	// old weak-dependency behavior for isolated development.
	// 用于结算/废弃落库后调 matchmaker.ReleaseMatch 释放残留撮合状态,
	// 修复"结算返回 Hub 后玩家无法再次匹配(StartMatch 4002)"。
	var releaser biz.MatchReleaser
	if cfg.Battle.MatchmakerAddr != "" {
		mr := data.NewGrpcMatchReleaser(cfg.Battle.MatchmakerAddr)
		defer func() { _ = mr.Close() }()
		releaser = mr
		helper.Infow("msg", "match_releaser_grpc", "matchmaker_addr", cfg.Battle.MatchmakerAddr)
	} else {
		if cfg.DSAuth.AuthorityModeRedis() {
			helper.Errorw("msg", "match_releaser_required",
				"hint", "Redis authority requires battle.matchmaker_addr; durable claims have no fallback TTL")
			os.Exit(1)
		}
		helper.Warnw("msg", "match_releaser_disabled",
			"hint", "local legacy profile only: matchmaker_addr is empty and match release publisher is disabled")
	}

	// 战斗 DS 绝不在 ReportResult 同步响应路径回收。Model-B 把完整服务端 proof 与战绩
	// 同事务写 terminal outbox，先留 grace 让 DS 通知客户端，再由 worker 经 ds_allocator
	// 做永久 terminal + UID delete → MySQL durable ACK → finalize TTL。ended 心跳仍可低延迟
	// 收尾，但资源安全不依赖客户端/DS 一定收到响应。
	uc := biz.NewBattleResultUsecase(repo, mmr, pusher, releaser, cfg.Battle)
	if terminalRelay != nil {
		uc.SetTerminalReleaseRelay(terminalRelay)
	}
	if closeCell, e := etcdtable.WireRouter(context.Background(), cfg.CellRoute, uc.SetCellRouter); e != nil {
		helper.Errorw("msg", "cellroute_init_failed", "err", e)
		os.Exit(1)
	} else if closeCell != nil {
		defer func() { _ = closeCell() }()
	}

	// 6.2 inventory 装备掉落发放器(W5 ④,弱依赖:InventoryAddr 空 → 不发放战斗装备掉落,
	// drop 出箱积压不丢,等地址配好重启补发)。GrantInstances 是系统接口,走内网 insecure 直连。
	if cfg.Battle.InventoryAddr != "" {
		granter := data.NewGrpcInstanceGranter(cfg.Battle.InventoryAddr)
		defer func() { _ = granter.Close() }()
		uc.SetInstanceGranter(granter)
		helper.Infow("msg", "drop_granter_grpc", "inventory_addr", cfg.Battle.InventoryAddr,
			"item_rules_source", "configtable/drop+item")
	} else {
		helper.Warnw("msg", "drop_granter_disabled",
			"hint", "inventory_addr 未配置 → 战斗装备掉落不发放(drop 出箱积压不丢,配好地址重启补发)")
	}

	// 6.1.9 配置表(不变量 §9.15):怪物击杀经验的**唯一数值权威**(role_level 表的「击杀经验」列)。
	//
	// 与 matchmaker 的可选模式不同,这里是**启动强依赖**:progress 通道开着却没有经验表时,
	// 每条击杀事实都会被 ReportProgress 按可重试错误退回,DS 原批重试到天荒地老 —— 与其
	// 让服务带病跑,不如起不来。progress 通道关闭时表也照样加载(结算路径不用它,但配置齐备
	// 是部署契约的一部分,少一张表说明 ConfigMap 没挂对,早失败早发现)。
	ctStore := configtable.NewStore()
	ctDir := cfg.ConfigTable.Dir
	if ctDir == "" {
		helper.Errorw("msg", "configtable_dir_required",
			"hint", "battle_result 的怪物击杀经验只来自 role_level 表;请配置 config_table.dir 并挂载 configtable 卷")
		os.Exit(1)
	}
	ctRes, err := ctStore.Load(ctDir, 0)
	if err != nil {
		helper.Errorw("msg", "configtable_load_failed", "dir", ctDir, "err", err)
		os.Exit(1)
	}
	for _, w := range ctRes.Warnings {
		helper.Warnw("msg", "configtable_load_warning", "warning", w)
	}
	helper.Infow("msg", "configtable_loaded", "dir", ctDir, "version", ctRes.Version,
		"role_level_rows", ctStore.Tables().RoleLevel.Count(),
		"item_rows", ctStore.Tables().Item.Count(), "drop_rows", ctStore.Tables().Drop.Count())
	// 注入的是"当前批次"的表指针。热更(ReloadConfigTable)后 Store 原子换指针,
	// 而这里注入的是快照 —— 经验表要跟随热更必须注入 Store 而非 Tables。
	uc.SetMonsterExpTable(monsterExpFromStore{store: ctStore})
	uc.SetBattleItemCatalog(battleItemCatalogFromStore{store: ctStore})

	// 6.2.0 player 经验入账器(实时成长,弱依赖:PlayerAddr 空 → 进度出箱经验行积压不丢,
	// 配好地址重启补发)。AddExperience 是系统接口,走内网 insecure 直连(复用 MMR reader 地址)。
	if cfg.Battle.PlayerAddr != "" {
		expGranter := data.NewGrpcExperienceGranter(cfg.Battle.PlayerAddr)
		defer func() { _ = expGranter.Close() }()
		uc.SetExperienceGranter(expGranter)
		helper.Infow("msg", "experience_granter_grpc", "player_addr", cfg.Battle.PlayerAddr,
			"progress_enabled", cfg.Battle.ProgressEnabled)
	} else {
		helper.Warnw("msg", "experience_granter_disabled",
			"hint", "player_addr 未配置 → 击杀经验不入账(进度出箱积压不丢,配好地址重启补发)")
	}

	// 6.2.1 背包满溢出转邮件发送器(弱依赖:MailAddr 空 → 背包满掉落留在出箱轮询重试,退化为历史行为)。
	// 传源键 battle_drop:{match}:{player} 至 mail,领取时 GrantInstances 同键去重(直发与邮件链至多一次)。
	if cfg.Battle.MailAddr != "" {
		mailSender := data.NewGrpcMailSender(cfg.Battle.MailAddr)
		defer func() { _ = mailSender.Close() }()
		uc.SetMailSender(mailSender)
		helper.Infow("msg", "drop_overflow_mail_grpc", "mail_addr", cfg.Battle.MailAddr)
	} else {
		helper.Warnw("msg", "drop_overflow_mail_disabled",
			"hint", "mail_addr 未配置 → 背包满掉落留在出箱轮询重试(不丢,不转邮件)")
	}

	svc := service.NewBattleResultService(uc)
	if cfg.DSAuth.AuthorityModeRedis() {
		svc.SetBattleCredentialStateChecker(service.NewBattleCredentialStateChecker(
			data.NewRedisBattleAuthReader(authRedis), cfg.DSAuth.ActiveHeartbeatMaxAge.Std()))
		helper.Infow("msg", "battle_active_credential_checker_ready", "authority_mode", "redis")
	}

	// DS 回调令牌守卫(审核 P1 #1):校验 Battle DS 经 :8444 的 ReportResult。
	// mode=off(默认)→ dsGuard 为 nil,不校验。
	dsGuard, derr := middleware.NewDSCallbackGuardFromConf(cfg.DSAuth)
	if derr != nil {
		helper.Errorw("msg", "ds_auth_guard_init_failed", "err", derr)
		os.Exit(1)
	}
	svc.SetDSCallbackGuard(dsGuard)
	if dsGuard != nil {
		helper.Infow("msg", "ds_callback_guard_ready", "mode", dsGuard.Mode().String())
	}

	grpcSrv := server.NewGRPCServer(&cfg, svc)
	httpSrv := server.NewHTTPServer(&cfg)

	// 6.1 后台 player.update 出箱发布器(W4 ⑨ 可靠补偿,随进程生命周期启停)
	pubCtx, pubCancel := context.WithCancel(context.Background())
	defer pubCancel()

	// 6.3 后台战斗装备掉落出箱发布器(W5 ④,at-least-once + GrantInstances 幂等)。
	// granter==nil(inventory_addr 未配)时内部直接返回,不空转。

	// 7. KafkaConsumer:按 ConsumeTopics 每 topic 一个,handler 按 topic 路由
	consumers, dlqProducers := mustBuildConsumers(&cfg, uc, helper)
	if cfg.DSAuth.AuthorityModeRedis() {
		fence, err := dsauthfence.AcquireRuntime(context.Background(), dsauthfence.RuntimeConfig{
			Endpoints: cfg.DSAuth.Fence.EtcdEndpoints, Prefix: cfg.DSAuth.Fence.EtcdPrefix,
			Service: serviceName, KeysetRevision: cfg.DSAuth.Fence.KeysetRevision,
			WriterEpoch: dsauthfence.ProtocolEpochV2,
			Features:    []string{"battle-terminal-outbox-v1"},
			LeaseTTLSec: cfg.DSAuth.Fence.EtcdLeaseTTLSec, DialTimeout: cfg.DSAuth.Fence.EtcdDialTimeout.Std(),
		})
		if err != nil {
			helper.Errorw("msg", "ds_auth_fence_acquire_failed", "err", err)
			os.Exit(1)
		}
		defer func() { _ = fence.Close() }()
		go func() {
			<-fence.Lost()
			helper.Errorw("msg", "ds_auth_fence_lost", "reason", fence.LostReason(),
				"hint", "立即退出，禁止失租/旧 epoch 副本继续结算")
			os.Exit(1)
		}()
		helper.Infow("msg", "ds_auth_fence_ready", "required_writer_epoch", fence.RequiredEpoch(), "reclaimed_stale_capability", fence.Reclaimed())
	}
	// publisher/consumer 都会产生外部副作用；capability 未取得前禁止启动。
	go uc.RunOutboxPublisher(pubCtx)
	go uc.RunDropPublisher(pubCtx)
	go uc.RunProgressPublisher(pubCtx)
	go uc.RunMatchReleasePublisher(pubCtx)
	// 保留期清理:battles/stats + 已结算进度水位超期批删,只增表增长有界(§9.24,biz/retention.go)。
	go uc.RunRetentionSweep(pubCtx)
	if terminalRelay != nil {
		go uc.RunTerminalReleasePublisher(pubCtx)
	}
	for _, kc := range consumers {
		kc.Start()
	}
	defer func() {
		for _, kc := range consumers {
			if cerr := kc.Close(); cerr != nil {
				helper.Warnw("msg", "kafka_consumer_close_failed", "err", cerr)
			}
		}
		for _, dp := range dlqProducers {
			_ = dp.Close()
		}
	}()

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"kafka_brokers", cfg.Kafka.Brokers,
		"kafka_group", cfg.Kafka.GroupID,
		"consume_topics", cfg.Battle.ConsumeTopics,
		"elo_k", cfg.Battle.EloKFactor,
		"base_mmr", cfg.Battle.BaseMMR,
		"outbox_interval", cfg.Battle.OutboxPublishInterval.String(),
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

// mustBuildConsumers 按 cfg.Battle.ConsumeTopics 起 KafkaConsumer,handler 按 topic 路由。
// brokers / topics 空时致命(battle_result 不可降级:不消费就不结算)。
//
// 每个消费者配一个 DLQ producer(topic=pandora.dlq.<topic>):解码毒丸直接进 DLQ,
// 业务瞬时错误重试 dlqMaxRetries 次后进 DLQ(infra.md §4.4「失败 3 次进 DLQ」)。
// DLQ producer 构造失败致命:battle_result 不可静默降级为丢消息模式。
func mustBuildConsumers(cfg *conf.Config, uc *biz.BattleResultUsecase, h *klog.Helper) ([]*kafkax.KeyOrderedConsumer, []*kafkax.KeyOrderedProducer) {
	if len(cfg.Kafka.Brokers) == 0 {
		h.Errorw("msg", "kafka_brokers_empty", "hint", "kafka.brokers required")
		os.Exit(1)
	}
	if len(cfg.Battle.ConsumeTopics) == 0 {
		h.Errorw("msg", "consume_topics_empty", "hint", "battle.consume_topics required")
		os.Exit(1)
	}

	out := make([]*kafkax.KeyOrderedConsumer, 0, len(cfg.Battle.ConsumeTopics))
	dlqProducers := make([]*kafkax.KeyOrderedProducer, 0, len(cfg.Battle.ConsumeTopics))
	for _, topic := range cfg.Battle.ConsumeTopics {
		var handler kafkax.Handler
		switch topic {
		case kafkax.TopicBattleResult:
			handler = uc.BattleResultHandler()
		case kafkax.TopicDSLifecycle:
			handler = uc.DSLifecycleHandler()
		default:
			h.Warnw("msg", "unknown_consume_topic_skipped", "topic", topic)
			continue
		}
		dlqTopic := kafkax.BuildDLQTopic(topic)
		dlq, derr := kafkax.NewKeyOrderedProducer(cfg.Kafka, dlqTopic)
		if derr != nil {
			h.Errorw("msg", "dlq_producer_init_failed", "topic", topic, "dlq_topic", dlqTopic, "err", derr,
				"hint", "battle_result 不可静默降级,DLQ 必须可用")
			os.Exit(1)
		}
		dlqProducers = append(dlqProducers, dlq)
		kc, err := kafkax.NewKeyOrderedConsumer(kafkax.ConsumerConfig{
			Brokers:        cfg.Kafka.Brokers,
			Topic:          topic,
			GroupID:        cfg.Kafka.GroupID,
			PartitionCount: cfg.Kafka.PartitionCnt,
			RetryPolicy:    kafkax.RetryPolicy{MaxRetries: dlqMaxRetries, Backoff: dlqRetryBackoff},
			DLQ:            dlq,
		}, handler)
		if err != nil {
			h.Errorw("msg", "kafka_consumer_new_failed", "topic", topic, "err", err)
			os.Exit(1)
		}
		out = append(out, kc)
		h.Infow("msg", "kafka_consumer_ready", "topic", topic, "group", cfg.Kafka.GroupID, "dlq_topic", dlqTopic)
	}
	if len(out) == 0 {
		h.Errorw("msg", "no_valid_consumer", "hint", "consume_topics 全部无效")
		os.Exit(1)
	}
	return out, dlqProducers
}

// playerUpdatePusher 把 biz.PlayerUpdatePusher 适配到 kafkax.KeyOrderedProducer。
// key=player_id(不变量 §9 同玩家事件保序)。
type playerUpdatePusher struct {
	p *kafkax.KeyOrderedProducer
}

func (k *playerUpdatePusher) PushPlayerUpdate(ctx context.Context, playerID uint64, payload []byte) error {
	return k.p.SendRaw(ctx, strconv.FormatUint(playerID, 10), payload)
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

// maskDSN 脱敏 DSN 里的密码(对齐 login main.go)。
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
	if colon != -1 && at != -1 && at > colon {
		return dsn[:colon+1] + "***" + dsn[at:]
	}
	return dsn
}

// monsterExpFromStore 把 configtable Store 适配成 biz.MonsterExpTable。
//
// 刻意持有 *Store 而不是 *Tables 快照:ReloadConfigTable 热更时 Store 原子换批次指针,
// 持快照会让服务一直用启动那一版的经验表,策划改完数值要重启才生效(§9.15 热更流水线的意义
// 就在于不重启)。每次查询经 Tables() 取当前批次,读的是 atomic 指针,无锁无拷贝。
type monsterExpFromStore struct {
	store *configtable.Store
}

// battleItemCatalogFromStore 让掉落过滤、堆叠/实例路由与局内可消费规则共用 UE 同源表。
// 持 Store 而不是 Tables 快照，热更整批切换后读路径立即生效。
type battleItemCatalogFromStore struct {
	store *configtable.Store
}

func (c battleItemCatalogFromStore) Lookup(itemConfigID uint32) (biz.BattleItemDefinition, bool) {
	tb := c.store.Tables()
	if tb == nil || tb.Item == nil || tb.Drop == nil {
		return biz.BattleItemDefinition{}, false
	}
	item, ok := tb.Item.ByID(itemConfigID)
	if !ok {
		return biz.BattleItemDefinition{}, false
	}
	droppable := false
	for _, row := range tb.Drop.All() {
		if row.GetItemConfigId() == itemConfigID {
			droppable = true
			break
		}
	}
	return biz.BattleItemDefinition{
		Equipment:    item.GetType() == configpb.ItemType_ITEM_TYPE_EQUIPMENT,
		BattleUsable: item.GetUsable(),
		Droppable:    droppable,
		MaxStack:     item.GetMaxStackSize(),
	}, true
}

// KillExpOf 查 (怪物角色 ID, 等级) 的整份击杀经验。
// 表未加载(理论上不可达:启动已 fail-fast)时返回 ok=false,由调用方按"配置漏项"处理。
func (m monsterExpFromStore) KillExpOf(roleID, level uint32) (uint64, bool) {
	tb := m.store.Tables()
	if tb == nil || tb.RoleLevel == nil {
		return 0, false
	}
	return tb.RoleLevel.KillExpOf(roleID, level)
}
