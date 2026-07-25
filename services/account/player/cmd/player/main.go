// Pandora player 服务入口(W4 ④,2026-06-06)。
//
// 职责:玩家档案 / 段位 MMR / 英雄池;消费 pandora.player.update 幂等 UpdateMMR
// (不变量 §2,idempotency_key=match_id);GetMMR 供 battle_result 当真实 MMRReader。
//
// 启动顺序(对齐 battle_result):
//  1. 解析 -conf 路径,加载 yaml
//  2. conf.Defaults 填默认值
//  3. log.Setup → 全局 zap logger
//  4. 加载并校验玩家等级经验配置表(强依赖,唯一数值源)
//  5. MySQL client + Ping(强依赖:玩家档案落库不可降级)
//  6. 装配 PlayerUsecase → PlayerService → gRPC/HTTP server
//  7. 按 ConsumeTopics 每 topic 一个 KafkaConsumer(player.update)
//  8. kratos.New(...).Run() 阻塞
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	klog "github.com/go-kratos/kratos/v2/log"

	"github.com/luyuancpp/pandora/pkg/cellroute/etcdtable"
	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/mysqlx"
	"github.com/luyuancpp/pandora/pkg/sessiongate"

	"github.com/luyuancpp/pandora/services/account/player/internal/biz"
	"github.com/luyuancpp/pandora/services/account/player/internal/conf"
	"github.com/luyuancpp/pandora/services/account/player/internal/data"
	"github.com/luyuancpp/pandora/services/account/player/internal/server"
	"github.com/luyuancpp/pandora/services/account/player/internal/service"
)

const serviceName = "player"

// Kafka 消费失败处理:业务瞬时错误进程内重试 dlqMaxRetries 次(间隔 dlqRetryBackoff)后进 DLQ
// (infra.md §4.4「失败 3 次进 DLQ」)。
const (
	dlqMaxRetries   = 3
	dlqRetryBackoff = 500 * time.Millisecond
)

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/player-dev.yaml", "config file path")
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

	// 3. 玩家等级经验配置表:策划 j_玩家等级经验.xlsx 是唯一数值源。
	// player 不保留 YAML 曲线兜底；目录缺失/坏批次均在监听端口前 fail-closed。
	if cfg.ConfigTable.Dir == "" {
		helper.Errorw("msg", "configtable_dir_required",
			"hint", "config_table.dir required; player experience reads j_玩家等级经验.xlsx only")
		os.Exit(1)
	}
	ctStore := configtable.NewStore()
	ctStore.AddValidator(func(tb *configtable.Tables) error {
		if tb.PlayerLevelExp == nil {
			return fmt.Errorf("缺少 player_level_exp 配置表")
		}
		if err := tb.PlayerLevelExp.ValidateCurve(); err != nil {
			return err
		}
		// 配置热更不得缩短最高等级，否则已有高等级玩家会在后续入账时被错误降级。
		if current := ctStore.Tables(); current != nil && current.PlayerLevelExp != nil &&
			tb.PlayerLevelExp.MaxLevel() < current.PlayerLevelExp.MaxLevel() {
			return fmt.Errorf("玩家最高等级不允许从 %d 降到 %d",
				current.PlayerLevelExp.MaxLevel(), tb.PlayerLevelExp.MaxLevel())
		}

		// 出战养成两张权威表(2026-07-25):SetEquipment 靠道具表判 isEquip/slotMatch,
		// SetTalents 靠专精表判等级上限/前置/总消耗。缺表则这两条写路径只能 fail-closed 拒绝,
		// 所以在加载边界就拒掉,不让服务带着半张表进入可服务状态。
		if tb.Item == nil {
			return fmt.Errorf("缺少 item 配置表(SetEquipment 的装备/部位校验依赖它)")
		}
		if tb.Talent == nil {
			return fmt.Errorf("缺少 talent 配置表(SetTalents 的等级上限/前置/消耗校验依赖它)")
		}
		// 前置关系是自引用外键,生成器不做自引用 FK 校验,只能在这里整表校验一次。
		if err := tb.Talent.ValidateTree(); err != nil {
			return err
		}
		return nil
	})
	loadResult, err := ctStore.Load(cfg.ConfigTable.Dir, 0)
	if err != nil {
		helper.Errorw("msg", "configtable_load_failed", "dir", cfg.ConfigTable.Dir, "err", err)
		os.Exit(1)
	}
	for _, warning := range loadResult.Warnings {
		helper.Warnw("msg", "configtable_load_warning", "warning", warning)
	}
	levelTable := ctStore.Tables().PlayerLevelExp
	helper.Infow("msg", "player_level_exp_loaded", "dir", cfg.ConfigTable.Dir,
		"version", loadResult.Version, "levels", levelTable.Count(), "max_level", levelTable.MaxLevel())

	// 4. MySQL(强依赖:玩家档案落库不可降级)
	if cfg.Node.MySQLClient.DSN == "" {
		helper.Errorw("msg", "mysql_dsn_required", "hint", "node.mysql_client.dsn required (pandora_player)")
		os.Exit(1)
	}
	db := mysqlx.MustNewClient(cfg.Node.MySQLClient)
	defer func() { _ = db.Close() }()
	helper.Infow("msg", "mysql_connected", "dsn", maskDSN(cfg.Node.MySQLClient.DSN))

	// 5. 装配链
	repo := data.NewMySQLPlayerRepo(db)
	// 启动 schema gate:经验相关表列缺失时 fail-fast,不能让副本 Ready 后在首个
	// GetProfile / AddExperience 才大面积报错(迁移顺序错误要在发布时拦住)。
	schemaCtx, schemaCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := repo.ValidateExperienceSchema(schemaCtx); err != nil {
		schemaCancel()
		helper.Errorw("msg", "player_experience_schema_invalid", "err", err,
			"hint", "先执行 pandora_player migration 000002_experience(players.exp / exp_history / player_push_outbox)")
		os.Exit(1)
	}
	if err := repo.ValidateExperienceLevels(schemaCtx, levelTable.MaxLevel()); err != nil {
		schemaCancel()
		helper.Errorw("msg", "player_experience_level_invalid", "err", err,
			"hint", "修复 players.level 脏数据或发布不低于现存等级上限的玩家等级经验表")
		os.Exit(1)
	}
	schemaCancel()
	uc := biz.NewPlayerUsecase(repo, cfg.Player)
	uc.SetConfigTables(ctStore)

	// 出战装备预设的拥有权校验器(2026-07-25):经 inventory 系统 RPC CheckItemsOwned 判定。
	// 未配 inventory_addr 时不接线 —— SetEquipment 随即 fail-closed 拒绝(见 biz 说明),
	// 不会退化成"不校验就放行",因此这里只警告不退出;真正的门在 loadout_customize_enabled。
	if cfg.Player.InventoryAddr != "" {
		checker := data.NewGrpcItemOwnershipChecker(cfg.Player.InventoryAddr)
		defer func() { _ = checker.Close() }()
		uc.SetItemOwnershipChecker(checker)
		helper.Infow("msg", "item_ownership_checker_grpc", "inventory_addr", cfg.Player.InventoryAddr)
	} else if cfg.Player.LoadoutCustomizeEnabled {
		helper.Warnw("msg", "item_ownership_checker_missing",
			"hint", "loadout_customize_enabled=true 但未配 player.inventory_addr → SetEquipment 将一律拒绝")
	}

	if closeCell, e := etcdtable.WireRouter(context.Background(), cfg.CellRoute, uc.SetCellRouter); e != nil {
		helper.Errorw("msg", "cellroute_init_failed", "err", e)
		os.Exit(1)
	} else if closeCell != nil {
		defer func() { _ = closeCell() }()
	}
	svc := service.NewPlayerService(uc)
	ctAdmin := configtable.NewAdminService(ctStore, cfg.ConfigTable.Dir)

	// 会话现行性门(R5 复审 P0-1,INC-20260722-004):客户端面请求 jti 必须是 login
	// 会话权威(pandora:sess,node.redis_client 指向的共享 Redis)当前一代;
	// prod 生成器机械置 session_gate.require=true(漏配端点拒启)。
	sessGate, sgClose := sessiongate.MustBuild(cfg.Node.RedisClient, cfg.SessionGate.Require)
	defer sgClose()

	grpcSrv := server.NewGRPCServer(&cfg, svc, ctAdmin, sessGate)
	httpSrv := server.NewHTTPServer(&cfg)

	// 6. 经验推送出箱发布器(实时成长):producer 可用才注入,失败只警告(出箱积压不丢,
	// 与 battle_result player.update producer 同语义)。event_type 走 kafka header,push 透传。
	// ⚠️ 经验事件走独立 topic pandora.player.experience,绝不能发 pandora.player.update:
	// 旧 player 副本消费 player.update 时不看 event_type header,会把经验事件误解码成
	// MMR 事件污染段位(金丝雀混跑,不变量 §21;详见 kafkax.TopicPlayerUpdate 注释)。
	pubCtx, pubCancel := context.WithCancel(context.Background())
	defer pubCancel()
	if len(cfg.Kafka.Brokers) > 0 {
		producer, perr := kafkax.NewKeyOrderedProducer(cfg.Kafka, kafkax.TopicPlayerExperience)
		if perr != nil {
			helper.Warnw("msg", "player_push_producer_init_failed", "err", perr,
				"hint", "经验推送出箱积压不丢,producer 可用后重启补发")
		} else {
			defer func() { _ = producer.Close() }()
			uc.SetExperiencePusher(&playerEventPusher{p: producer})
			helper.Infow("msg", "player_push_producer_ready", "topic", kafkax.TopicPlayerExperience)
		}
	}
	go uc.RunPushOutboxPublisher(pubCtx)
	go uc.RunExpHistoryJanitor(pubCtx)
	// mmr_history / 点数授予幂等历史保留期清理(默认关,前置条件见 conf,§9.24)。
	go uc.RunHistoryJanitor(pubCtx)

	// 7. KafkaConsumer:按 ConsumeTopics 每 topic 一个,handler 按 topic 路由
	consumers, dlqProducers := mustBuildConsumers(&cfg, uc, helper)
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
		"consume_topics", cfg.Player.ConsumeTopics,
		"base_mmr", cfg.Player.BaseMMR,
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

// mustBuildConsumers 按 cfg.Player.ConsumeTopics 起 KafkaConsumer,handler 按 topic 路由。
// brokers / topics 空时致命(player 不消费 player.update 就无法做幂等 UpdateMMR)。
//
// 每个消费者配一个 DLQ producer(topic=pandora.dlq.<topic>):解码毒丸直接进 DLQ,
// 业务瞬时错误重试 dlqMaxRetries 次后进 DLQ。DLQ producer 构造失败致命:不可静默丢 MMR 更新。
func mustBuildConsumers(cfg *conf.Config, uc *biz.PlayerUsecase, h *klog.Helper) ([]*kafkax.KeyOrderedConsumer, []*kafkax.KeyOrderedProducer) {
	if len(cfg.Kafka.Brokers) == 0 {
		h.Errorw("msg", "kafka_brokers_empty", "hint", "kafka.brokers required")
		os.Exit(1)
	}
	if len(cfg.Player.ConsumeTopics) == 0 {
		h.Errorw("msg", "consume_topics_empty", "hint", "player.consume_topics required")
		os.Exit(1)
	}

	out := make([]*kafkax.KeyOrderedConsumer, 0, len(cfg.Player.ConsumeTopics))
	dlqProducers := make([]*kafkax.KeyOrderedProducer, 0, len(cfg.Player.ConsumeTopics))
	for _, topic := range cfg.Player.ConsumeTopics {
		var handler kafkax.Handler
		switch topic {
		case kafkax.TopicPlayerUpdate:
			handler = uc.PlayerUpdateHandler()
		default:
			h.Warnw("msg", "unknown_consume_topic_skipped", "topic", topic)
			continue
		}
		dlqTopic := kafkax.BuildDLQTopic(topic)
		dlq, derr := kafkax.NewKeyOrderedProducer(cfg.Kafka, dlqTopic)
		if derr != nil {
			h.Errorw("msg", "dlq_producer_init_failed", "topic", topic, "dlq_topic", dlqTopic, "err", derr,
				"hint", "player.update 不可静默降级,DLQ 必须可用")
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

// playerEventPusher 把 biz.ExperiencePusher 适配到 kafkax.KeyOrderedProducer。
// key=player_id(不变量 §9 同玩家事件保序);event_type 走 kafka header(push.proto 域内路由)。
type playerEventPusher struct {
	p *kafkax.KeyOrderedProducer
}

func (k *playerEventPusher) PushPlayerEvent(ctx context.Context, playerID uint64, eventType uint32, payload []byte) error {
	return k.p.SendRawWithEventType(ctx, strconv.FormatUint(playerID, 10), payload, eventType)
}

// maskDSN 脱敏 DSN 里的密码(对齐 battle_result / login main.go)。
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
