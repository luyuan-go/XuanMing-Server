// Pandora matchmaker 服务入口(W4 ①,2026-06-06)。
//
// 启动顺序:
//  1. 解析 -conf 路径,加载 yaml
//  2. conf.Defaults 填默认值
//  3. log.Setup → 全局 zap logger
//  4. Redis client 连通性 Ping(强依赖)
//  5. Snowflake Node(node_id 来自 yaml)
//  6. team gRPC reader(team_addr 留空则跳过 team 校验)
//  7. kafkax.KeyOrderedProducer(topic=pandora.match.progress) → matchPusher(brokers 配置时为启动强依赖)
//  8. 装配链:RedisMatchRepo → MatchUsecase → MatchService → gRPC/HTTP server
//  9. 后台 RunMatchLoop(撮合 + 确认期超时扫描)
//  10. kratos.New(...).Run() 阻塞
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/cellroute/etcdtable"
	pconfig "github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	"github.com/luyuancpp/pandora/pkg/leader/etcdleader"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	"github.com/luyuancpp/pandora/pkg/snowflake/etcdnode"

	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/biz"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/data"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/server"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/service"
)

const serviceName = "matchmaker"

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/matchmaker-dev.yaml", "config file path")
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
	if err := cfg.Validate(); err != nil {
		helper.Errorw("msg", "config_validation_failed", "err", err)
		os.Exit(1)
	}

	// 2.5 配置表(不变量 §9.15):config_table.dir 配置后是启动强依赖,加载失败直接退出
	// (fail-closed);未配置则不启用,StartMatch 跳过 map_id 表校验(历史行为)。
	var ctStore *configtable.Store
	if dir := cfg.ConfigTable.Dir; dir != "" {
		ctStore = configtable.NewStore()
		// 兜底默认副本(match.map_id)必须是关卡表里的战斗类关卡。注册为批次级校验器:
		// 启动首载与之后每次热 reload 走同一门禁(审计 P1:只查启动时,坏批次热更后
		// 默认 map_id 请求会全部失败),失败整批不切换保留旧表。
		defaultMapID := cfg.Match.MapId
		ctStore.AddValidator(func(tb *configtable.Tables) error {
			if !tb.Level.IsBattleLevel(defaultMapID) {
				return fmt.Errorf("match.map_id %d 不是关卡表中的战斗类关卡(g_关卡.xlsx)", defaultMapID)
			}
			return nil
		})
		res, err := ctStore.Load(dir, 0)
		if err != nil {
			helper.Errorw("msg", "configtable_load_failed", "dir", dir, "err", err)
			os.Exit(1)
		}
		for _, w := range res.Warnings {
			helper.Warnw("msg", "configtable_load_warning", "warning", w)
		}
		helper.Infow("msg", "configtable_loaded", "dir", dir,
			"version", res.Version, "levels", ctStore.Tables().Level.Count())
	} else {
		helper.Warnw("msg", "configtable_disabled",
			"hint", "config_table.dir empty; StartMatch map_id will not be validated against level table")
	}

	// 3. Redis(强依赖)
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

	// 4. Snowflake(node_id_source=static 静态，=etcd 走 etcd 自动抢占，失租自动退出)
	//
	// ticket_id 与 match_id 是两个独立 ID 空间，各取一个发号器(共用同一 nodeID / lease)。
	// ⚠️ 共用 nodeID ⇒ 两个空间会发出逐位相同的 ID，禁止跨空间放进同一容器比较
	// (见 etcdnode.ProvideSnowflakeN)。
	sfs, sfCloser := etcdnode.MustProvideSnowflakeN(serviceName, cfg.Node.NodeId, cfg.Snowflake, 2)
	defer func() { _ = sfCloser.Close() }()
	ticketSF, matchSF := sfs[0], sfs[1]

	// 5. team gRPC reader(弱依赖:team_addr 留空 → 跳过队伍校验)
	var reader biz.TeamReader
	if cfg.Match.TeamAddr != "" {
		tr := data.NewGrpcTeamReader(cfg.Match.TeamAddr)
		defer func() { _ = tr.Close() }()
		reader = tr
		helper.Infow("msg", "team_reader_ready", "team_addr", cfg.Match.TeamAddr)
	} else {
		helper.Warnw("msg", "team_addr_empty", "hint", "StartMatch will skip team validation")
	}

	// 6. Kafka producer → matchPusher。
	// kafka.brokers 非空表示启用 match 进度推送；此时 producer 是启动强依赖。组队匹配只有
	// 队长持有 StartMatch 返回的 match_id 可轮询 GetMatchProgress 兜底，其余成员得知成局 /
	// READY / Battle 落点的唯一通道就是 pandora.match.progress 推送；初始化失败必须在对外
	// Ready 前退出，让 Kubernetes 保留旧 Pod 并在 Kafka 恢复后重试新 Pod，不能再以
	// pusher=nil 受理匹配后把整场进度推送静默丢弃（非队长成员会一直停在 Hub）。
	var pusher biz.MatchEventPusher
	publication, perr := initializeMatchPublication(cfg.Kafka, func(kcfg pconfig.KafkaConfig, topic string) (rawMatchProducer, error) {
		return kafkax.NewKeyOrderedProducer(kcfg, topic)
	})
	if perr != nil {
		helper.Errorw("msg", "kafka_producer_required_but_unavailable", "err", perr,
			"hint", "matchmaker exits before Ready so the orchestrator can retry after Kafka recovers")
		os.Exit(1)
	}
	if publication.producer != nil {
		defer func() { _ = publication.producer.Close() }()
		pusher = publication.pusher
		helper.Infow("msg", "kafka_producer_ready", "topic", kafkax.TopicMatchProgress, "required", true)
	} else {
		helper.Warnw("msg", "kafka_producer_disabled_dev_only", "reason", publication.disabledReason,
			"hint", "match progress push disabled; only the captain can see READY via GetMatchProgress polling in this explicit no-Kafka development mode")
	}

	// 7. 装配链
	repo := data.NewRedisMatchRepo(rdb, cfg.Match.GameMode)

	// 会话现行性门(R5 复审 P0-1,INC-20260722-004):客户端面请求 jti 必须是 login
	// 会话权威(pandora:sess,node.redis_client 指向的共享 Redis)当前一代;
	// prod 生成器机械置 session_gate.require=true(漏配端点拒启)。
	// R7 复审 P0-2:同一 gate 提前构建,还要注入 DS allocator——READY 批签的 battle 票
	// 逐玩家携带当前会话 jti(sjti claim),兑换点复核后旧设备残票作废。
	sessGate, sgClose := sessiongate.MustBuild(cfg.Node.RedisClient, cfg.SessionGate.Require)
	defer sgClose()

	// DSAllocator:ds_allocator_addr 非空 → 真 gRPC 拉 DS + 签 battle 票据;否则 W4 ① 打桩
	var allocator biz.DSAllocator
	if cfg.Match.DSAllocatorAddr != "" {
		// 真实 DS 分配链固定使用 Model-B RS256 实例绑定票。配置漂移时禁止回退到
		// legacy HS256，否则线上 Fleet（只有 public JWKS）会全量拒票，且重新引入玩家 HMAC。
		//
		// 唯一例外:Windows 本机联调档 local-off-v1(match.ds_local_profile 显式声明)。
		// 战斗票的签/验必须同档 —— hub_allocator / ds_allocator 在 mode=local 下【只接受】
		// legacy(auth.ValidateDSLocalProfileOffV1),并强制给 UE DS 注入
		// PANDORA_DS_LOCAL_PROFILE=local-off-v1,UE 据此锁进 HS256LocalOff 档且不交叉接受。
		// 此处若强签 RS256 v2,DS 会把每个玩家拒在 PreLogin:服务起得来、打不了,比起不来更难查。
		var v2Signer *auth.DSTicketSigner
		var legacySigner *auth.Signer
		switch {
		case cfg.DSTicket.SignerEnabled():
			// 互斥:v2 私钥与本机档同时配置 = 姿态自相矛盾,拒启,不猜。
			if cfg.Match.DSLocalProfile != "" {
				helper.Errorw("msg", "ds_ticket_profile_conflict",
					"ds_local_profile", cfg.Match.DSLocalProfile,
					"hint", "ds_ticket.private_key_file (v2/RS256) and match.ds_local_profile (local HS256) are mutually exclusive; pick one")
				os.Exit(1)
			}
			s, verr := auth.NewDSTicketSignerFromConf(cfg.DSTicket)
			if verr != nil {
				helper.Errorw("msg", "ds_ticket_v2_signer_init_failed", "err", verr,
					"hint", "check ds_ticket.private_key_file / active_kid / ttl")
				os.Exit(1)
			}
			v2Signer = s
			helper.Infow("msg", "ds_ticket_v2_signer_ready", "kid", s.Kid(), "ttl", s.TTL().String())
		case cfg.Match.DSLocalProfile == auth.DSLocalProfileOffV1:
			// 本机档:签 legacy HS256 battle 票,密钥与 login / UE 占位密钥同源(jwt.secret)。
			s, lerr := auth.NewSigner(auth.Config{
				Issuer:      cfg.JWT.Issuer,
				Audience:    cfg.JWT.Audience,
				Secret:      []byte(cfg.JWT.Secret),
				SessionTTL:  cfg.JWT.SessionTTL.Std(),
				DSTicketTTL: cfg.JWT.DSTicketTTL.Std(),
			})
			if lerr != nil {
				helper.Errorw("msg", "local_legacy_signer_init_failed", "err", lerr,
					"hint", "local-off-v1 signs HS256 battle tickets from jwt.secret; it must match login and the UE DS secret")
				os.Exit(1)
			}
			legacySigner = s
			helper.Warnw("msg", "ds_ticket_local_off_v1_legacy_signer",
				"profile", auth.DSLocalProfileOffV1,
				"hint", "Windows local co-debug only: battle tickets are legacy HS256 to match the UE HS256LocalOff profile; never enable in production")
		default:
			helper.Errorw("msg", "ds_allocator_requires_ds_ticket_v2",
				"hint", "configure revisioned ds_ticket.private_key_file + active_kid; "+
					"for Windows local co-debug set match.ds_local_profile=local-off-v1; silent legacy fallback is forbidden")
			os.Exit(1)
		}
		abortSigner, abortErr := internalrpcauth.NewSigner(cfg.Match.AllocationAbortAuthSecret,
			serviceName, cfg.Match.AllocationAbortAuthAudience)
		if abortErr != nil {
			helper.Errorw("msg", "allocation_abort_service_auth_init_failed", "err", abortErr)
			os.Exit(1)
		}
		ga := data.NewGrpcDSAllocator(cfg.Match.DSAllocatorAddr, legacySigner, v2Signer, abortSigner,
			cfg.Match.MapId, cfg.Match.GameMode, cfg.Match.DSAllocateTimeout.Std())
		ga.SetSessionGate(sessGate) // R7 P0-2:READY 批签票据绑定当前会话代际
		if ctStore != nil {
			// 本局计分模式(关卡表 rating_mode)在发出 AllocateBattle 那一刻定格,
			// 与 game_mode / map_id 同源同时刻;未注入时回落旧口径(见 ratingModeForMap)。
			ga.SetConfigTables(ctStore)
		}
		defer func() { _ = ga.Close() }()
		allocator = ga
		helper.Infow("msg", "ds_allocator_grpc_ready", "ds_allocator_addr", cfg.Match.DSAllocatorAddr,
			"map_id", cfg.Match.MapId, "game_mode", cfg.Match.GameMode)
	} else {
		allocator = biz.NewStubDSAllocator("") // W4 ① 打桩;无 ds_allocator_addr 时兜底
		helper.Warnw("msg", "ds_allocator_addr_empty", "hint", "using StubDSAllocator (mock ds_addr + mock tickets)")
	}
	// player_locator gRPC notifier（弱依赖：locator_addr 留空 → 不上报位置）
	// 撮合成局→MATCHING、全员确认就绪→BATTLE（不变量 §1）
	var locator biz.LocationNotifier
	if cfg.Match.LocatorAddr != "" {
		locatorConn := grpcclient.MustDialInsecure(cfg.Match.LocatorAddr)
		ln := data.NewGrpcLocationNotifier(locatorConn)
		defer func() { _ = ln.Close() }()
		locator = ln
		helper.Infow("msg", "locator_notifier_ready", "locator_addr", cfg.Match.LocatorAddr)
	} else {
		helper.Warnw("msg", "locator_addr_empty", "hint", "match state (MATCHING/BATTLE) will not be reported to player_locator")
	}
	uc := biz.NewMatchUsecase(repo, reader, pusher, allocator, matchSF, locator, cfg.Match)
	if ctStore != nil {
		uc.SetConfigTables(ctStore)
	}
	// 进场侧限流(anti-abuse §6 第 2/3/7/8 项):StartMatch 冷却 + 成局级冷却 +
	// 容量耗尽静默窗 + no-show 退避执行。复用共享 rdb;背压非权威门,故障 fail-open。
	uc.SetEntryLimiter(data.NewRedisEntryLimiter(rdb))
	helper.Infow("msg", "entry_ratelimiter_ready",
		"start_match_cooldown", cfg.Match.StartMatchCooldown.Std().String(),
		"match_form_cooldown", cfg.Match.MatchFormCooldown.Std().String(),
		"no_capacity_requeue_delay", cfg.Match.NoCapacityRequeueDelay.Std().String())

	// 蜂窝扩容:按 cfg.CellRoute 装配确定性 region/cell 路由(off/static/etcd 统一口）。
	// 单 Cell(mode 空）→ router=nil,行为不变;多 Cell → 两级撮合 + battle 放置感知 region。
	if router, cellClose, cerr := etcdtable.BuildRouter(context.Background(), cfg.CellRoute); cerr != nil {
		helper.Errorw("msg", "cellroute_init_failed", "err", cerr)
		os.Exit(1)
	} else if router != nil {
		if cellClose != nil {
			defer func() { _ = cellClose() }()
		}
		uc.SetCellRouter(router)
		uc.SetRegionPolicy(biz.DefaultRegionMatchPolicy())
		helper.Infow("msg", "cellroute_enabled", "mode", cfg.CellRoute.Mode,
			"self_region", cfg.CellRoute.SelfRegion, "self_cell", cfg.CellRoute.SelfCell)
	}
	resumeReplayStore, replayErr := internalrpcauth.NewRedisReplayStore(rdb,
		"pandora:matchmaker:resolve-context:nonce:")
	if replayErr != nil {
		helper.Errorw("msg", "match_resume_replay_store_init_failed", "err", replayErr)
		os.Exit(1)
	}
	loginResumeAuth, authErr := internalrpcauth.NewVerifier(cfg.Match.MatchResumeAuthSecret,
		"login", cfg.Match.MatchResumeAuthAudience, 30*time.Second, resumeReplayStore)
	if authErr != nil {
		helper.Errorw("msg", "match_resume_service_auth_init_failed", "err", authErr)
		os.Exit(1)
	}
	// ResolvePlayerMatchContext has two legitimate internal callers, each with its
	// own key (see conf.Match.TeamResumeAuthSecret). Team's key being absent keeps
	// Team rejected rather than falling back to Login's — but that also means
	// Team's join gate stays fail-closed, so say so loudly at startup instead of
	// letting it look healthy.
	resumeVerifiers := []*internalrpcauth.Verifier{loginResumeAuth}
	if cfg.Match.TeamResumeAuthSecret != "" {
		teamResumeAuth, teamAuthErr := internalrpcauth.NewVerifier(cfg.Match.TeamResumeAuthSecret,
			"team", cfg.Match.MatchResumeAuthAudience, 30*time.Second, resumeReplayStore)
		if teamAuthErr != nil {
			helper.Errorw("msg", "team_resume_service_auth_init_failed", "err", teamAuthErr)
			os.Exit(1)
		}
		resumeVerifiers = append(resumeVerifiers, teamResumeAuth)
	} else {
		helper.Warnw("msg", "team_resume_service_auth_disabled",
			"hint", "match.team_resume_auth_secret is empty: team ListOpenTeams returns empty and team join is rejected with ERR_UNAVAILABLE")
	}
	resumeAuth, multiErr := internalrpcauth.NewMultiCallerVerifier(resumeVerifiers...)
	if multiErr != nil {
		helper.Errorw("msg", "match_resume_service_auth_init_failed", "err", multiErr)
		os.Exit(1)
	}
	svc := service.NewMatchService(uc, ticketSF, resumeAuth)

	// 8. gRPC + HTTP(配置表启用时同端口挂热更入口,内部接口不经 Envoy)
	var ctAdmin *service.ConfigTableAdminService
	if ctStore != nil {
		ctAdmin = service.NewConfigTableAdminService(ctStore, cfg.ConfigTable.Dir)
	}
	grpcSrv := server.NewGRPCServer(&cfg, svc, ctAdmin, sessGate)
	httpSrv := server.NewHTTPServer(&cfg)

	// 9. 后台撮合循环(随进程生命周期启停)
	//
	// 单写者(见 docs/design/decision-revisit-matchmaker-single-writer.md):撮合循环在共享队列上
	// 做全局优化,天然是单写者问题。多副本部署时若每个副本都无条件跑,会重复成局(同一玩家进两场
	// match,违反不变量 §1)。
	//   - Leader.Enabled=false(默认):本副本直接跑(单副本 / dev 行为不变)。
	//   - Leader.Enabled=true:经 etcd 选举,仅当选副本跑;失主取消 loop 的 ctx 但进程不退出,继续
	//     服务 RPC,新 leader 在 lease TTL 内接管(不停机滚动更新,不变量 §16)。
	loopCtx, loopCancel := context.WithCancel(context.Background())
	defer loopCancel()
	if cfg.Match.Leader.Enabled {
		// 分片键 = game_mode × region:同一 (mode, region) 的副本竞争同一个 leader。
		electionName := fmt.Sprintf("matchmaker/%s/r%d", cfg.Match.GameMode, cfg.CellRoute.SelfRegion)
		go func() {
			err := etcdleader.Run(loopCtx, etcdleader.Config{
				Endpoints:   cfg.Match.Leader.EtcdEndpoints,
				Election:    electionName,
				Prefix:      cfg.Match.Leader.Prefix,
				LeaseTTLSec: cfg.Match.Leader.LeaseTTLSec,
			}, uc.RunMatchLoop)
			if err != nil && loopCtx.Err() == nil {
				helper.Errorw("msg", "match_leader_run_failed", "election", electionName, "err", err)
			}
		}()
		helper.Infow("msg", "match_loop_leader_gated", "election", electionName,
			"etcd_endpoints", cfg.Match.Leader.EtcdEndpoints)
	} else {
		go uc.RunMatchLoop(loopCtx)
		helper.Infow("msg", "match_loop_direct", "hint", "single-replica / leader election disabled")
	}

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"redis_addr", rc.Host,
		"team_addr", cfg.Match.TeamAddr,
		"confirm_timeout", cfg.Match.ConfirmTimeout.String(),
		"match_interval", cfg.Match.MatchInterval.String(),
		"team_size", cfg.Match.TeamSize,
		"walk_in", cfg.Match.WalkIn,
		"auto_confirm_match", cfg.Match.AutoConfirmMatch,
	)

	// 废弃键告警:enable_solo_match 已于 2026-07-25 正名为 walk_in,Defaults() 仍兼容读取旧键
	// (漏迁移时保住 PVE 的 walk-in 行为)。这条 Warn 是 contract 阶段删除旧字段前的迁移进度信号——
	// 线上不再出现它,才说明所有部署的 yaml / ConfigMap 都已改用新键。
	if cfg.Match.EnableSoloMatch {
		helper.Warnw("msg", "deprecated_config_key",
			"key", "match.enable_solo_match", "replacement", "match.walk_in",
			"hint", "旧键仍生效(已并入 walk_in),请迁移配置后再删除")
	}

	// 10. Kratos App
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

// rawMatchProducer 是启动门禁测试可替换的 Kafka 最小能力面。
type rawMatchProducer interface {
	PushToPlayers(context.Context, uint64, []uint64, []byte) (int, error)
	Close() error
}

type matchPublicationInit struct {
	pusher         biz.MatchEventPusher
	producer       rawMatchProducer
	disabledReason string
}

// initializeMatchPublication 集中约束 match 进度推送的启动语义（与 team 同口径）：
//   - kafka.brokers 显式为空时保留纯 RPC 本地调试模式（仅队长可轮询）；
//   - 只要配置了 broker，producer 就是强依赖，初始化失败或返回 nil 都拒绝启动。
//
// 该门禁必须发生在 gRPC server 构造与 app.Run 之前，确保 readiness 永远不会把
// “StartMatch 受理成功但 READY 推送永久关闭”的进程加入 Service Endpoints。
func initializeMatchPublication(
	cfg pconfig.KafkaConfig,
	factory func(pconfig.KafkaConfig, string) (rawMatchProducer, error),
) (matchPublicationInit, error) {
	configured := false
	for _, broker := range cfg.Brokers {
		if strings.TrimSpace(broker) != "" {
			configured = true
			break
		}
	}
	if !configured {
		return matchPublicationInit{disabledReason: "kafka.brokers is empty"}, nil
	}

	producer, err := factory(cfg, kafkax.TopicMatchProgress)
	if err != nil {
		return matchPublicationInit{}, fmt.Errorf("initialize required %s producer: %w", kafkax.TopicMatchProgress, err)
	}
	if producer == nil {
		return matchPublicationInit{}, fmt.Errorf("initialize required %s producer: factory returned nil", kafkax.TopicMatchProgress)
	}
	return matchPublicationInit{
		pusher:   &kafkaPusher{p: producer},
		producer: producer,
	}, nil
}

// kafkaPusher 把 biz.MatchEventPusher 接口适配到 Kafka producer。
type kafkaPusher struct {
	p rawMatchProducer
}

func (k *kafkaPusher) PushMatchProgress(ctx context.Context, callerPlayerID uint64, toPlayerIDs []uint64, payload []byte) (int, error) {
	return k.p.PushToPlayers(ctx, callerPlayerID, toPlayerIDs, payload)
}
