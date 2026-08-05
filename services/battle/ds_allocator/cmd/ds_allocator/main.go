// Pandora ds_allocator 服务入口(W4 ②,2026-06-06)。
//
// 职责:战斗 DS 调度。matchmaker 全员确认后调 AllocateBattle 申请 DS,
// 战斗 DS 每 5s 调 Heartbeat 续命,心跳超时由后台扫描标记 abandoned。
//
// 启动顺序:
//  1. 解析 -conf 路径,加载 yaml
//  2. conf.Defaults 填默认值
//  3. log.Setup → 全局 zap logger
//  4. Redis client 连通性 Ping(强依赖:DS 状态镜像)
//  5. 装配链:RedisBattleRepo → (Agones 或 Mock) GameServerAllocator → AllocatorUsecase → AllocatorService → gRPC/HTTP server
//  6. 后台 RunHeartbeatSweep(心跳超时扫描)
//  7. kratos.New(...).Run() 阻塞
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2"
	kconfig "github.com/go-kratos/kratos/v2/config"
	"github.com/go-kratos/kratos/v2/config/file"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/auth"
	pconfig "github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/dsauthfence"
	"github.com/luyuancpp/pandora/pkg/dsauthfence/writerlease"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/pkg/releasetrack"
	configv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/biz"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/gm"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/server"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/service"
)

const serviceName = "ds_allocator"

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/ds_allocator-dev.yaml", "config file path")
}

func main() {
	flag.Parse()
	// The activation controller consumes this mode as exact machine JSON.
	// Run it before logger setup so stdout contains one JSON value and never a
	// service banner mixed into the evidence stream.
	if flagPodUIDReleasePreflightCompareConfigs {
		if flagPodUIDReleasePreflight {
			fmt.Fprintln(os.Stderr, "pod_uid config comparison mode conflict")
			os.Exit(2)
		}
		if err := runPodUIDConfigCompare(os.Stdin, os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "pod_uid config comparison failed: %v\n", err)
			os.Exit(1)
		}
		return
	}

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
	// Explicit one-shot release gate used by the strict Model-B activation Job.
	// It exits before service config validation, allocator wiring, background
	// workers, or any listener is created. It is deliberately not a Pod init
	// container: the new writer must be able to run first and backfill safe
	// legacy identities before operators execute this final activation proof.
	if flagPodUIDReleasePreflight {
		if flagPodUIDReleasePreflightTimeout <= 0 || flagPodUIDReleasePreflightScanCount <= 0 {
			helper.Errorw("msg", "pod_uid_release_preflight_flags_invalid",
				"timeout", flagPodUIDReleasePreflightTimeout,
				"scan_count", flagPodUIDReleasePreflightScanCount)
			_ = c.Close()
			os.Exit(2)
		}
		ctx, cancel := context.WithTimeout(context.Background(), flagPodUIDReleasePreflightTimeout)
		credentials, credentialErr := loadPodUIDPreflightRedisCredentials()
		if credentialErr != nil {
			cancel()
			helper.Errorw("msg", "pod_uid_release_preflight_redis_credentials_invalid", "err", credentialErr)
			_ = c.Close()
			os.Exit(1)
		}
		err := runPodUIDReleasePreflight(ctx, cfg.Node.RedisClient,
			flagPodUIDReleasePreflightScanCount, podUIDPreflightEvidence{
				RunID: flagPodUIDReleasePreflightRunID, Phase: flagPodUIDReleasePreflightPhase,
				ImageDigest:            flagPodUIDReleasePreflightImageDigest,
				ExpectedTargetIdentity: flagPodUIDReleasePreflightExpectedTargetIdentity,
			}, credentials, os.Stdout, os.Stderr)
		cancel()
		if err != nil {
			helper.Errorw("msg", "pod_uid_release_preflight_failed", "err", err)
			_ = c.Close()
			os.Exit(1)
		}
		return
	}
	if err := cfg.DSAuth.ValidateRedisFence(); err != nil {
		helper.Errorw("msg", "ds_auth_fence_config_invalid", "err", err)
		os.Exit(1)
	}
	if err := cfg.ValidateLifecyclePublicationConfig(); err != nil {
		helper.Errorw("msg", "ds_lifecycle_config_invalid", "err", err)
		os.Exit(1)
	}
	if err := cfg.ValidateBattleDepartureConfig(); err != nil {
		helper.Errorw("msg", "battle_departure_config_invalid", "err", err)
		os.Exit(1)
	}
	if err := cfg.ValidateAllocationAbortAuthConfig(); err != nil {
		helper.Errorw("msg", "allocation_abort_auth_config_invalid", "err", err)
		os.Exit(1)
	}
	if err := cfg.ValidateLocalMapSourceConfig(); err != nil {
		helper.Errorw("msg", "local_map_source_config_invalid", "err", err)
		os.Exit(1)
	}

	// 2.5 策划配置表(不变量 §9.15):config_table.dir 配置后是启动强依赖,加载失败直接退出。
	// ds_allocator 只用其中的关卡表:mode=local 起 DS 时按 map_id 现查 g_关卡.xlsx 拼关卡 URL
	// (asset_path + game_mode_class),取代 2026-08-04 之前那张手抄的 local_ds.maps 影子表。
	// mode=agones 不读它(关卡由 DS 侧 Loader GameMode 查同一张表决定),留空即可。
	var ctStore *configtable.Store
	if dir := strings.TrimSpace(cfg.ConfigTable.Dir); dir != "" {
		ctStore = configtable.NewStore()
		// 批次级校验器:关卡表里**每一张**战斗类关卡都必须能构造出合法启动 URL。
		// 启动首载与之后每次热 reload 走同一门禁,坏批次整批不切换、保留旧表——
		// 把"某张图资源列填错"挡在加载边界,而不是等玩家恰好选中那张图才炸。
		ctStore.AddValidator(func(tb *configtable.Tables) error { return tb.Level.ValidateBattleLaunchURLs() })
		res, lerr := ctStore.Load(dir, 0)
		if lerr != nil {
			helper.Errorw("msg", "configtable_load_failed", "dir", dir, "err", lerr)
			os.Exit(1)
		}
		for _, w := range res.Warnings {
			helper.Warnw("msg", "configtable_load_warning", "warning", w)
		}
		helper.Infow("msg", "configtable_loaded", "dir", dir,
			"version", res.Version, "levels", ctStore.Tables().Level.Count())
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

	// 4. 装配链
	repo := data.NewRedisBattleRepo(rdb)

	// 4.0 DS 回调服务令牌(审核 P1 #1):签发器(分配时下发给战斗 DS)+ 守卫(校验 DS 回调)。
	// secret 未配 → dsSigner=nil 不签发;mode=off → dsGuard=nil 不校验(默认,现行为不变)。
	dsSigner, err := middleware.NewDSCallbackSignerFromConf(cfg.DSAuth)
	if err != nil {
		helper.Errorw("msg", "ds_auth_signer_init_failed", "err", err)
		os.Exit(1)
	}
	dsGuard, err := middleware.NewDSCallbackGuardFromConf(cfg.DSAuth)
	if err != nil {
		helper.Errorw("msg", "ds_auth_guard_init_failed", "err", err)
		os.Exit(1)
	}
	// 启动期 TTL 正值/最小值校验(审核 P1):本服务签发(dsSigner!=nil)或校验(guard!=nil)DS 回调
	// 令牌时,BattleTokenTTL/HubTokenTTL 必须 >= 最小值,否则令牌签发即过期属误配,启动即拒。
	if err := cfg.DSAuth.Validate(dsSigner != nil || dsGuard != nil); err != nil {
		helper.Errorw("msg", "ds_auth_ttl_invalid", "err", err)
		os.Exit(1)
	}
	// 战斗令牌不续期(一局一签、DS 一局一销毁),TTL 必须覆盖「战斗镜像 TTL(battle_ttl,最长对局 +
	// 补偿重试上界)+ 重连/ready 余量」。否则活跃对局跑到一半令牌过期,battle_result / 心跳等 DS 回调被
	// enforce 守卫全拒、赛果无法结算(审核 P1:battle 令牌下限须关联 battle_ttl,不能只看固定 30m/1h)。
	if dsSigner != nil {
		const battleReconnectMargin = 15 * time.Minute
		needTTL := cfg.Allocator.BattleTTL.Std() + battleReconnectMargin
		if cfg.DSAuth.BattleTokenTTL.Std() < needTTL {
			helper.Errorw("msg", "ds_auth_battle_token_ttl_too_small_vs_battle_ttl",
				"battle_token_ttl", cfg.DSAuth.BattleTokenTTL.Std().String(),
				"battle_ttl", cfg.Allocator.BattleTTL.Std().String(),
				"need_at_least", needTTL.String(),
				"hint", "战斗令牌不续期,须 >= battle_ttl + 15m 重连余量;调大 ds_auth.battle_token_ttl 或调小 allocator.battle_ttl")
			os.Exit(1)
		}
	}
	// 签发回调:battle 令牌绑 match_id(pod 分配时未知,pod↔match 绑定由心跳 pod_mismatch 逻辑兜底)。
	issueBattleToken := func(matchID uint64) (string, error) {
		tok, _, serr := dsSigner.SignDSCallback(auth.DSTypeBattle, "", matchID, cfg.DSAuth.BattleTokenTTL.Std())
		return tok, serr
	}
	// local-off-v1 不接 Redis pending/ACK，但仍必须给 UE 完整 Model-B tuple，不能回退 legacy JWT。
	// 每个本机进程有随机实例 UID 与 jti；一局一实例，epoch/gen 从 1 起且不会在实例内回退。
	//
	// 注意:"不接 Redis ACK" 只免掉了 staged→active 的提升,**没有**免掉心跳应答的 ACK 回显。
	// UE 的 SendBattleHeartbeat 无条件按 uid/epoch/gen/jti/writer_epoch 五项比对应答 ACK,
	// 不过就丢掉整个 Command 与驱逐单,所以这里必须把 jti/writer_epoch 一并回吐给分配器留存,
	// 供 legacy 心跳逐字段回显(2026-08-05;与 hub 侧 issueLocalHubCredential 同形)。
	// gen 恒 1 是自洽的:一局一进程、实例内不换令牌,不需要 Redis 单调计数器。
	issueLocalBattleCredential := func(matchID uint64, podName, instanceUID string, instanceEpoch uint32) (string, data.BattleCredentialIdentity, error) {
		const localBattleGen uint64 = 1
		jti := uuid.NewString()
		res, serr := dsSigner.SignBattleCredential(
			matchID, podName, instanceUID, instanceEpoch, localBattleGen, jti, cfg.DSAuth.BattleTokenTTL.Std())
		if serr != nil {
			return "", data.BattleCredentialIdentity{}, serr
		}
		return res.Token, data.BattleCredentialIdentity{
			PodName:       podName,
			InstanceUID:   instanceUID,
			InstanceEpoch: instanceEpoch,
			Gen:           localBattleGen,
			JTI:           jti,
			ExpMs:         uint64(res.ExpMs),
			Kid:           res.Kid,
			TokenSHA256:   res.TokenSHA256,
			WriterEpoch:   res.WriterEpoch,
		}, nil
	}
	// enforce 下签发失败必须 fail-closed(不分配无令牌的 DS,否则回调被守卫全拒)。
	dsEnforce := dsGuard.Mode() == middleware.DSAuthEnforce
	modelB := cfg.DSAuth.AuthorityModeRedis()
	if modelB && (cfg.Mode != conf.ModeAgones || !dsEnforce || dsSigner == nil) {
		helper.Errorw("msg", "battle_model_b_invalid_activation",
			"allocator_mode", cfg.Mode, "guard_mode", dsGuard.Mode().String(), "signer_ready", dsSigner != nil,
			"hint", "authority_mode=redis requires mode=agones + ds_auth.mode=enforce + signing key; no legacy fallback")
		os.Exit(1)
	}
	if dsSigner != nil {
		helper.Infow("msg", "ds_callback_token_issuer_ready",
			"battle_token_ttl", cfg.DSAuth.BattleTokenTTL.Std().String(), "guard_mode", dsGuard.Mode().String())
	}

	// DS 启动方式由 cfg.Mode 单一开关决定(标准两模式 + 离线兜底),biz 逻辑零改:
	//   - mode=agones → 真 GameServerAllocation(Linux 生产)
	//   - mode=local  → 本机拉起 Windows DS 进程(Windows 单机自测)
	//   - mode=mock   → Mock 确定性假地址(无真实 DS,离线联调)
	var allocator biz.GameServerAllocator
	var agonesAlloc *data.AgonesGameServerAllocator // 仅 mode=agones 非空,供 Fleet 容量巡检
	switch cfg.Mode {
	case conf.ModeAgones:
		ag, aerr := data.NewAgonesGameServerAllocator(cfg.Agones)
		if aerr != nil {
			helper.Errorw("msg", "agones_allocator_init_failed", "err", aerr,
				"hint", "检查 agones.fleet_name / ca_path 配置")
			os.Exit(1)
		}
		allocator = ag
		agonesAlloc = ag
		if dsSigner != nil && !modelB {
			ag.SetDSTokenIssuer(issueBattleToken, dsEnforce) // 令牌经 GameServerAllocation annotation 下发
		}
		helper.Infow("msg", "agones_allocator_ready",
			"api_server", cfg.Agones.APIServer, "namespace", cfg.Agones.Namespace, "fleet", cfg.Agones.FleetName)
	case conf.ModeLocal:
		if perr := auth.ValidateDSLocalProfileOffV1(dsGuard.Mode().String(), cfg.DSAuth.AuthorityMode, dsSigner != nil); perr != nil {
			helper.Errorw("msg", "local_battle_auth_profile_invalid",
				"err", perr,
				"hint", "mode=local requires ds_auth.mode=off + authority_mode=legacy + signing key (local-off-v1); Redis Model-B local authority is not implemented")
			os.Exit(1)
		}
		ld, lerr := data.NewLocalGameServerAllocator(cfg.LocalDS)
		if lerr != nil {
			helper.Errorw("msg", "local_ds_allocator_init_failed", "err", lerr,
				"launcher", cfg.LocalDS.Launcher,
				"hint", "mode=local 两种 DS 形态二选一:launcher=packaged 需 local_ds.executable_path 指向打包好的 UE Windows DS(PandoraServer.exe);"+
					"launcher=editor 需 executable_path 指向 UnrealEditor.exe 且 project_path 指向 Pandora.uproject(免出包,直接读未 cook 的工程内容)。"+
					"一键脚本:start.ps1 -Mode local -DsLauncher editor 会自动探测引擎与工程并经 PANDORA_DS_LAUNCHER/PANDORA_DS_EXE/PANDORA_DS_UPROJECT 注入。")
			os.Exit(1)
		}
		// 进程退出时杀掉全部在管 DS,避免遗留孤儿。
		defer func() { _ = ld.Close() }()
		allocator = ld
		ld.SetDSTokenIssuer(issueLocalBattleCredential, true) // 完整 tuple 经 env 下发；失败必须 fail-closed
		// 关卡解析器:每局现查关卡表(唯一权威源 g_关卡.xlsx),不再有第二份 yaml 映射。
		// 现查而非启动时快照 —— 这样 ReloadConfigTable 之后新增的副本无需重启本服务即可开局。
		// ctStore 为空时不注入:此时必然配了 loader_map(ValidateLocalMapSourceConfig 已挡),
		// DS 侧 Loader GameMode 查同一张表决定目标图。
		if ctStore != nil {
			ld.SetMapURLResolver(func(mapID uint32) (string, error) {
				tb := ctStore.Tables()
				if tb == nil {
					return "", fmt.Errorf("配置表尚未加载成功")
				}
				return tb.Level.BattleLaunchURL(mapID)
			})
		}
		helper.Infow("msg", "local_ds_allocator_ready",
			"launcher", cfg.LocalDS.Launcher, "project", cfg.LocalDS.ProjectPath,
			"executable", cfg.LocalDS.ExecutablePath,
			"map_source", localMapSource(cfg.LocalDS.LoaderMap, ctStore != nil),
			"loader_map", cfg.LocalDS.LoaderMap,
			"advertise_host", cfg.LocalDS.AdvertiseHost,
			"port_base", cfg.LocalDS.PortBase, "port_range", cfg.LocalDS.PortRange)
	default:
		allocator = biz.NewMockGameServerAllocator(cfg.Allocator)
		helper.Warnw("msg", "mock_allocator_active",
			"mode", cfg.Mode, "hint", "mode=mock,用确定性假地址(无真实 DS)")
	}
	uc := biz.NewAllocatorUsecase(repo, allocator, cfg.Allocator)
	lifecycleRequired := cfg.RequiresReliableLifecyclePublication()
	uc.SetLifecyclePusherRequired(lifecycleRequired)

	// owner 权威实例租约双写(owner-authority.md migrate ⑥):owner_addr 空 = 不启用。
	// 弱/强依赖语义见 conf.OwnerLeaseRequired 注释。
	if cfg.Allocator.OwnerAddr != "" {
		ownerLease := data.NewGrpcOwnerLeaseRenewer(cfg.Allocator.OwnerAddr)
		defer func() { _ = ownerLease.Close() }()
		uc.SetOwnerLeaseRenewer(ownerLease, cfg.Allocator.OwnerLeaseRequired)
		// migrate ②/③:READY 交付 Begin(BATTLE) + census 代提交 Admit(同一连接,弱依赖)。
		uc.SetOwnerAuthority(ownerLease)
		helper.Infow("msg", "owner_lease_dual_write_enabled",
			"owner_addr", cfg.Allocator.OwnerAddr, "required", cfg.Allocator.OwnerLeaseRequired)
	}
	releasePolicy, policyErr := releasetrack.New(cfg.Agones.CanaryPercent, cfg.Agones.CanarySeed)
	if policyErr != nil {
		helper.Errorw("msg", "battle_release_track_policy_invalid", "err", policyErr)
		os.Exit(1)
	}
	uc.SetReleaseTrackPolicy(releasePolicy)
	helper.Infow("msg", "battle_release_track_policy_ready",
		"canary_percent", cfg.Agones.CanaryPercent, "canary_seed_configured", cfg.Agones.CanarySeed != "")
	battleAuthRepo := data.NewRedisBattleAuthRepo(rdb)
	if modelB {
		if err := uc.EnableRedisAuthority(battleAuthRepo, dsSigner, cfg.DSAuth.BattleTokenTTL.Std()); err != nil {
			helper.Errorw("msg", "battle_model_b_init_failed", "err", err)
			os.Exit(1)
		}
		helper.Infow("msg", "battle_model_b_enabled", "required_writer_epoch", data.BattleDSWriterEpochV2,
			"authority", "redis", "k8s_role", "delivery-only")
	}
	if cfg.Mode == conf.ModeLocal {
		// local 模式 UE DS 无 Agones,收到 stop 指令不会自杀 → 让后端在 orphan/pod_mismatch/终态
		// 心跳时主动 kill 该 DS,防幽灵进程占端口污染下一局(配合端口 bind 探测双保险)。
		uc.SetKillOrphanOnStop(true)
	}

	// 4.1 ds.lifecycle producer。Redis authority / Agones+enforce 下是恢复强依赖：
	// abandoned 必须抵达 battle_result，后者才会持久化 match release + battle exit proof。
	// 只有显式 local/off 开发配置允许无 Kafka，并打出清晰降级告警。
	lifecycleInit, lifecycleErr := initializeLifecyclePublication(cfg, func(kcfg pconfig.KafkaConfig, topic string) (rawLifecycleProducer, error) {
		return kafkax.NewKeyOrderedProducer(kcfg, topic)
	})
	if lifecycleErr != nil {
		helper.Errorw("msg", "ds_lifecycle_producer_required_but_unavailable", "err", lifecycleErr)
		os.Exit(1)
	}
	if lifecycleInit.producer != nil {
		defer func() { _ = lifecycleInit.producer.Close() }()
		uc.SetLifecyclePusher(lifecycleInit.pusher)
		helper.Infow("msg", "ds_lifecycle_producer_ready", "topic", kafkax.TopicDSLifecycle,
			"required", lifecycleRequired)
	} else {
		helper.Warnw("msg", "ds_lifecycle_disabled_dev_only", "reason", lifecycleInit.disabledReason,
			"hint", "only local/off development may run without abandoned recovery publication")
	}
	if err := uc.ValidateLifecyclePusherReady(); err != nil {
		helper.Errorw("msg", "ds_lifecycle_startup_gate_failed", "err", err)
		os.Exit(1)
	}

	// 4.2 player_locator 客户端(弱依赖):续期短 TTL BATTLE presence(玩家在线/战斗中的唯一路由信号)。
	// locator_addr 留空不续期 presence，长对局中途重登可能因位置过期退化为回大厅。
	if cfg.LocatorAddr != "" {
		conn := grpcclient.MustDialInsecure(cfg.LocatorAddr)
		defer func() { _ = conn.Close() }()
		locatorClient := data.NewGrpcLocationRefresher(conn)
		uc.SetLocationRefresher(locatorClient)
		helper.Infow("msg", "locator_client_ready", "locator_addr", cfg.LocatorAddr)
	} else {
		helper.Warnw("msg", "locator_addr_empty",
			"hint", "BATTLE presence 不续期；玩家战斗中无法被 login 检测到")
	}

	svc := service.NewAllocatorService(uc)
	svc.SetDSCallbackGuard(dsGuard) // DS 回调令牌校验(Heartbeat);nil=off
	if modelB {
		abortReplayStore, replayErr := internalrpcauth.NewRedisReplayStore(rdb,
			"pandora:ds-allocator:allocation-abort:nonce:")
		if replayErr != nil {
			helper.Errorw("msg", "allocation_abort_replay_store_init_failed", "err", replayErr)
			os.Exit(1)
		}
		abortVerifier, verifierErr := internalrpcauth.NewVerifier(
			cfg.Allocator.AllocationAbortAuthSecret, "matchmaker",
			cfg.Allocator.AllocationAbortAuthAudience, 30*time.Second, abortReplayStore)
		if verifierErr != nil {
			helper.Errorw("msg", "allocation_abort_verifier_init_failed", "err", verifierErr)
			os.Exit(1)
		}
		svc.SetAllocationAbortVerifier(abortVerifier)
		helper.Infow("msg", "allocation_abort_service_auth_ready",
			"audience", cfg.Allocator.AllocationAbortAuthAudience)
	}

	// GmService(GM / 运维指令下发):与 ds_allocator 同进程复用 gRPC 端口。
	// 运维 GM 工具 SendCommand 入 Redis 队列 → 战斗 DS 轮询 PollCommands 拉取执行(如给玩家发道具)。
	// 内部接口,不经 Envoy 暴露给玩家客户端。
	gmSvc := gm.NewService(rdb, logger)
	gmSvc.SetDSCallbackGuard(dsGuard) // DS 回调令牌校验(PollCommands/AckCommand);nil=off
	if modelB {
		if err := gmSvc.EnableRedisAuthority(battleAuthRepo); err != nil {
			helper.Errorw("msg", "gm_battle_model_b_init_failed", "err", err)
			os.Exit(1)
		}
	}
	// SendCommand 前置校验目标对局是否有活跃战斗镜像:typo / 已结束的 match_id 立即拒,
	// 避免静默入僵尸队列(repo 天然满足 BattleLivenessChecker,复用同一 Redis)。
	gmSvc.SetBattleChecker(repo)

	// 5. gRPC + HTTP
	// 配置表热更入口(§9.15 标准流水线):启用配置表时一并注册,策划改完 g_关卡.xlsx 重导表后
	// 直接 ReloadConfigTable 即可让新副本可开局,无需重启 ds_allocator(通用实现,内部接口不对玩家开放)。
	var ctAdmin configv1.ConfigTableAdminServiceServer
	if ctStore != nil {
		ctAdmin = configtable.NewAdminService(ctStore, cfg.ConfigTable.Dir)
	}
	grpcSrv := server.NewGRPCServer(&cfg, svc, gmSvc, ctAdmin)
	writerHealth := &server.WriterHealthHolder{}
	httpSrv := server.NewHTTPServer(&cfg, writerHealth)

	// sweep/capacity watcher 也是 writer；capability 未取得前禁止启动任何后台循环或 RPC。
	if modelB {
		fence, err := dsauthfence.AcquireRuntime(context.Background(), dsauthfence.RuntimeConfig{
			Endpoints: cfg.DSAuth.Fence.EtcdEndpoints, Prefix: cfg.DSAuth.Fence.EtcdPrefix,
			Service: serviceName, KeysetRevision: cfg.DSAuth.Fence.KeysetRevision,
			WriterEpoch: dsauthfence.ProtocolEpochV2,
			Features: []string{
				"battle-release-expected-tuple-v1",
				"battle-storage-pod-uid-write-invariant-v1",
			},
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
				"hint", "立即退出，禁止失租/旧 epoch allocator 继续分配或接收 DS 写回")
			os.Exit(1)
		}()
		helper.Infow("msg", "ds_auth_fence_ready", "required_writer_epoch", fence.RequiredEpoch(), "reclaimed_stale_capability", fence.Reclaimed())

		// 5.1 心跳扫描的写者继任租约(2026-07-29 事故闭环;推导见 conf.WriterLeaseMode 注释)。
		//
		// 目的不是给 sweep 加防脑裂——那由既有的按 match 凭据 CAS 承担(见 biz.SweepWriterLease
		// 的边界说明);目的是让 ds_allocator 能安全地跑**多副本 + RollingUpdate**,从而使
		// "单副本重启 = 全服 Heartbeat 不可用 = 所有 Battle DS 在 20s 后踢人" 这条链断开。
		// capability key 按 (service, PodUID) 唯一,多副本天然共存,故此处不需要放宽任何 fencing。
		writerMode, wmErr := cfg.Allocator.ResolveWriterLeaseMode()
		if wmErr != nil {
			helper.Errorw("msg", "ds_writer_lease_mode_invalid", "err", wmErr)
			os.Exit(1)
		}
		// 机械门禁(与 hub_allocator R11 P0-5 同款):writer_lease_mode != enforce 时"单扫描者"
		// 只由部署策略(单副本 Recreate)保证;若实际部署是 RollingUpdate,重叠窗口里新旧副本
		// 都会扫描。进程看不到 spec.strategy,故由 Deployment 把策略作为 annotation 注入 env,
		// 并由 main_test.go 的清单契约测试钉住 annotation 与真实 strategy 一致。
		//   · 受管 k8s 内 + env 缺失 → fail-closed 退出(清单回归必须炸,不能靠人看日志);
		//   · 非 k8s(本机裸跑/dev)+ env 缺失 → 只告警(阻断会把开发环境一起打死)。
		strategy := strings.TrimSpace(os.Getenv("PANDORA_DEPLOY_STRATEGY"))
		inManagedK8s := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST")) != ""
		if strategy != "" {
			if strings.EqualFold(strategy, "RollingUpdate") && writerMode != conf.WriterLeaseEnforce {
				helper.Errorw("msg", "ds_writer_lease_rollingupdate_without_enforce",
					"strategy", strategy, "mode", writerMode,
					"hint", "RollingUpdate × writer_lease_mode!=enforce = 滚动重叠期出现并发心跳扫描者;"+
						"要么把 allocator.writer_lease_mode 改 enforce,要么把 Deployment 改回单副本 Recreate")
				os.Exit(1)
			}
			helper.Infow("msg", "ds_writer_lease_strategy_checked", "strategy", strategy, "mode", writerMode)
		} else if inManagedK8s {
			helper.Errorw("msg", "ds_writer_lease_strategy_annotation_missing", "mode", writerMode,
				"hint", "受管 k8s 内必须注入 PANDORA_DEPLOY_STRATEGY(取自 Deployment 的 "+
					"pandora.dev/deploy-strategy annotation);缺失则无法机械校验 RollingUpdate×非 enforce "+
					"的并发扫描组合,fail-closed 退出。见 deploy/k8s/services/services.yaml")
			os.Exit(1)
		} else {
			helper.Warnw("msg", "ds_writer_lease_strategy_unknown", "mode", writerMode,
				"hint", "非 k8s 环境(本机裸跑/dev):跳过部署策略机械校验")
		}
		if writerMode == conf.WriterLeaseOff {
			helper.Warnw("msg", "ds_writer_lease_disabled",
				"hint", "writer_lease_mode=off:单扫描者只由部署策略保证,只允许单副本 Recreate")
		} else {
			hostname, _ := os.Hostname()
			writerLease, wlErr := writerlease.Start(context.Background(), writerlease.Config{
				Endpoints:   cfg.DSAuth.Fence.EtcdEndpoints,
				Election:    "ds_allocator/sweep",
				Identity:    fmt.Sprintf("%s/%d", hostname, os.Getpid()),
				LeaseTTLSec: int(cfg.DSAuth.Fence.EtcdLeaseTTLSec),
				DialTimeout: cfg.DSAuth.Fence.EtcdDialTimeout.Std(),
				// 无 OnElected:接任不需要推进任何 fence 水位(sweep 不携带跨轮次权威意图)。
			})
			if wlErr != nil {
				helper.Errorw("msg", "ds_writer_lease_start_failed", "err", wlErr)
				os.Exit(1)
			}
			defer func() { _ = writerLease.Close() }()
			if writerMode == conf.WriterLeaseEnforce {
				uc.SetSweepWriterLease(writerLease)
			}
			writerHealth.Set(writerLease, writerMode)
			helper.Infow("msg", "ds_writer_lease_started",
				"election", "ds_allocator/sweep", "mode", writerMode,
				"hint", "enforce:只有当选副本跑心跳超时扫描,热备副本照常服务 Heartbeat/AllocateBattle;"+
					"warmup:只竞选观测 token 单调,不改扫描行为")
		}
	}

	// 6. 后台心跳超时扫描(随进程生命周期启停)
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	defer sweepCancel()
	go uc.RunHeartbeatSweep(sweepCtx)

	// 6.1 Fleet 容量巡检(仅 agones 模式):定期 GET Fleet status → 暴露
	// pandora_ds_allocator_fleet_* 指标,容量快到上限时打预警日志
	// (ds_fleet_capacity_near_limit / ds_fleet_capacity_exhausted),让运维在打满前扩 Fleet。
	// capacity_watch_interval 设负值可禁用(NewCapacityWatcher 返 nil)。
	if agonesAlloc != nil {
		if watcher := biz.NewCapacityWatcher(agonesAlloc, cfg.Agones); watcher != nil {
			go watcher.Run(sweepCtx)
			helper.Infow("msg", "fleet_capacity_watch_enabled",
				"interval", cfg.Agones.CapacityWatchInterval.String(),
				"warn_ratio", cfg.Agones.CapacityWarnRatio,
				"fleets", agonesAlloc.WatchedFleets())
		}
	}

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"redis_addr", rc.Host,
		"heartbeat_timeout", cfg.Allocator.HeartbeatTimeout.String(),
		"sweep_interval", cfg.Allocator.SweepInterval.String(),
		"allocator_mode", cfg.Mode,
	)
	// 7. Kratos App
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

// dsLifecyclePusher 把 biz.DSLifecyclePusher 适配到 kafkax.KeyOrderedProducer。
// key=match_id(不变量 §9 同对局事件保序)。
type dsLifecyclePusher struct {
	p rawLifecycleProducer
}

func (d *dsLifecyclePusher) PublishLifecycle(ctx context.Context, evt *dsv1.DSLifecycleEvent) error {
	payload, err := proto.Marshal(evt)
	if err != nil {
		return err
	}
	return d.p.SendRaw(ctx, strconv.FormatUint(evt.GetMatchId(), 10), payload)
}

// localMapSource 只用于启动日志:说清 mode=local 这台 allocator 的关卡到底由谁查表决定,
// 免得下一个人再去 yaml 里找那张已经删掉的 maps 映射。
func localMapSource(loaderMap string, configTableReady bool) string {
	if strings.TrimSpace(loaderMap) != "" {
		return "loader_map(DS 侧 Loader GameMode 查 g_关卡.xlsx)"
	}
	if configTableReady {
		return "config_table(allocator 现查 g_关卡.xlsx)"
	}
	return "none"
}

// rawLifecycleProducer 是启动测试可替换的 Kafka 最小能力面。
type rawLifecycleProducer interface {
	SendRaw(context.Context, string, []byte) error
	Close() error
}

type lifecyclePublicationInit struct {
	pusher         biz.DSLifecyclePusher
	producer       rawLifecycleProducer
	disabledReason string
}

// initializeLifecyclePublication 把“生产必须成功初始化、开发可显式降级”的决策集中在
// 一个可单测的启动函数里，避免 main 的后续重构重新引入 warn-and-continue。
func initializeLifecyclePublication(
	cfg conf.Config,
	factory func(pconfig.KafkaConfig, string) (rawLifecycleProducer, error),
) (lifecyclePublicationInit, error) {
	required := cfg.RequiresReliableLifecyclePublication()
	configured := false
	for _, broker := range cfg.Kafka.Brokers {
		if len(strings.TrimSpace(broker)) > 0 {
			configured = true
			break
		}
	}
	if !configured {
		if required {
			return lifecyclePublicationInit{}, fmt.Errorf("reliable %s producer is required but kafka.brokers is empty", kafkax.TopicDSLifecycle)
		}
		return lifecyclePublicationInit{disabledReason: "kafka.brokers is empty"}, nil
	}
	producer, err := factory(cfg.Kafka, kafkax.TopicDSLifecycle)
	if err != nil {
		if required {
			return lifecyclePublicationInit{}, fmt.Errorf("initialize required %s producer: %w", kafkax.TopicDSLifecycle, err)
		}
		return lifecyclePublicationInit{disabledReason: "optional producer initialization failed: " + err.Error()}, nil
	}
	if producer == nil {
		if required {
			return lifecyclePublicationInit{}, fmt.Errorf("initialize required %s producer: factory returned nil", kafkax.TopicDSLifecycle)
		}
		return lifecyclePublicationInit{disabledReason: "optional producer factory returned nil"}, nil
	}
	return lifecyclePublicationInit{
		pusher:   &dsLifecyclePusher{p: producer},
		producer: producer,
	}, nil
}
