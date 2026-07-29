// Pandora hub_allocator 服务入口(W4 ⑤,2026-06-06)。
//
// 职责:大厅 DS 分片调度。login 登录成功后调 AssignHub 给玩家分一个 hub DS 分片并签 hub 票据;
// Hub DS 每 5s 调 Heartbeat 续命,心跳超时由后台扫描标记 draining 停止分配。
//
// 启动顺序:
//  1. 解析 -conf 路径,加载 yaml
//  2. conf.Defaults 填默认值
//  3. log.Setup → 全局 zap logger
//  4. Redis client 连通性 Ping(强依赖:分片镜像 + 玩家归属)
//  5. pkg/auth.Signer 构造(强依赖:AssignHub 必须签 hub DSTicket)
//  6. 装配链:RedisHubRepo → MockHubFleetProvider → HubUsecase → HubService → gRPC/HTTP server
//  7. 后台 RunHeartbeatSweep(心跳超时扫描)
//  8. kratos.New(...).Run() 阻塞
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
	"github.com/google/uuid"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/dsauthfence"
	"github.com/luyuancpp/pandora/pkg/dsauthfence/writerlease"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	"github.com/luyuancpp/pandora/pkg/kafkax"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/pkg/releasetrack"
	"github.com/luyuancpp/pandora/pkg/sessiongate"

	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/biz"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/data"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/server"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/service"
)

const serviceName = "hub_allocator"

// hubTokenGenKey 是某 Hub DS pod 的令牌代际计数器 key(Redis INCR 权威、独立、单调)。
// hashtag {pod} 锁 cluster slot,与该 pod 的分片镜像 key 同 slot,便于 cluster 部署。
func hubTokenGenKey(pod string) string { return "pandora:hub:tokengen:{" + pod + "}" }

var flagConf string

func init() {
	flag.StringVar(&flagConf, "conf", "etc/hub_allocator-dev.yaml", "config file path")
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
	if cfg.DSAuth.AuthorityModeRedis() {
		if cfg.Mode != conf.ModeAgones {
			helper.Errorw("msg", "ds_auth_redis_authority_requires_agones", "mode", cfg.Mode)
			os.Exit(1)
		}
		if err := cfg.DSAuth.ValidateRedisFence(); err != nil {
			helper.Errorw("msg", "ds_auth_fence_config_invalid", "err", err)
			os.Exit(1)
		}
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

	// 4. DSTicket v2(RS256,方案 B):配置了私钥即启用;启用后 hub 票全部走 v2
	// 实例绑定签发,不再签 legacy HS256 票。加载失败直接拒绝启动(fail-closed)。
	var dstV2 *auth.DSTicketSigner
	if cfg.DSTicket.SignerEnabled() {
		v2, verr := auth.NewDSTicketSignerFromConf(cfg.DSTicket)
		if verr != nil {
			helper.Errorw("msg", "ds_ticket_v2_signer_init_failed", "err", verr,
				"hint", "check ds_ticket.private_key_file / active_kid / ttl")
			os.Exit(1)
		}
		dstV2 = v2
		helper.Infow("msg", "ds_ticket_v2_signer_ready", "kid", v2.Kid(), "ttl", v2.TTL().String())
	}
	if cfg.Mode == conf.ModeAgones && dstV2 == nil {
		helper.Errorw("msg", "agones_requires_ds_ticket_v2",
			"hint", "B1 k8s Hub 只允许 RS256；配置 ds_ticket.private_key_file + active_kid")
		os.Exit(1)
	}
	// legacy HS256 signer 只在非 B1 兼容模式构造；RS256 路径不加载玩家 HMAC secret。
	var signer *auth.Signer
	if dstV2 == nil {
		var serr error
		signer, serr = auth.NewSigner(auth.Config{
			Issuer:            cfg.JWT.Issuer,
			Audience:          cfg.JWT.Audience,
			Secret:            []byte(cfg.JWT.Secret),
			AdditionalSecrets: auth.AdditionalSecretsBytes(cfg.JWT.AdditionalSecrets),
			SessionTTL:        cfg.JWT.SessionTTL.Std(),
			DSTicketTTL:       cfg.JWT.DSTicketTTL.Std(),
		})
		if serr != nil {
			helper.Errorw("msg", "hub_ticket_signer_init_failed", "err", serr,
				"hint", "jwt.secret must be >=32 bytes and match login/envoy")
			os.Exit(1)
		}
		helper.Infow("msg", "hub_ticket_legacy_signer_ready", "ds_ticket_ttl", cfg.JWT.DSTicketTTL.String())
	}

	// 4.1 DS 回调服务令牌(审核 P1 #1):签发器(发现 ready Hub DS 时签 hub 令牌下发)
	// + 守卫(校验 Hub DS Heartbeat 回调)。secret 未配 → 不签发;mode=off → 不校验(默认)。
	dsSigner, derr := middleware.NewDSCallbackSignerFromConf(cfg.DSAuth)
	if derr != nil {
		helper.Errorw("msg", "ds_auth_signer_init_failed", "err", derr)
		os.Exit(1)
	}
	dsGuard, derr := middleware.NewDSCallbackGuardFromConf(cfg.DSAuth)
	if derr != nil {
		helper.Errorw("msg", "ds_auth_guard_init_failed", "err", derr)
		os.Exit(1)
	}
	// 启动期 TTL 正值/最小值校验(审核 P1):签发(dsSigner!=nil)或校验(guard!=nil)DS 回调令牌时,
	// HubTokenTTL 必须 >= 最小值,否则令牌签发即过期(续期判据 TTL/3 也会退化),启动即拒。
	if derr := cfg.DSAuth.Validate(dsSigner != nil || dsGuard != nil); derr != nil {
		helper.Errorw("msg", "ds_auth_ttl_invalid", "err", derr)
		os.Exit(1)
	}
	// DS 回调令牌验签器(审核 P1):agones 续期判据除“外置 exp 未近”外,还须实测 annotation 令牌
	// 验签通过(挡住空/损坏/旧密钥签发的令牌被误判可用)。secret 未配 → nil,续期只看 exp(不启用)。
	dsVerifier, derr := middleware.NewDSCallbackVerifierFromConf(cfg.DSAuth)
	if derr != nil {
		helper.Errorw("msg", "ds_auth_verifier_init_failed", "err", derr)
		os.Exit(1)
	}
	// 签发回调:hub 令牌绑 pod(sub),不带 match_id,并携带 Redis INCR 权威、独立、单调的「代际」gen。
	// 每次(重)签经 hubTokenGenKey(pod)领取严格递增的 gen 签进 ds_gen claim,DS 心跳原样回显,
	// 服务端精确相等比较判定是否当前代际(替代秒级 exp 代际,消除同秒重签碰撞;审核 P1-6)。
	// INCR 只在真实(重)签路径触发(agones 续期判定不通过 / local 拉起),不随发现空跑。
	issueHubToken := func(pod string) (string, int64, uint64, error) {
		genCtx, genCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer genCancel()
		gen, gerr := rdb.Incr(genCtx, hubTokenGenKey(pod)).Result()
		if gerr != nil {
			return "", 0, 0, fmt.Errorf("hub token gen incr for pod %s: %w", pod, gerr)
		}
		// 代际计数器 TTL 兜底 key 增长:pod 名唯一不复用(Agones generateName / local UUID 后缀),
		// 且续期(TTL/3)远早于此过期,故计数器在 pod 生命周期内绝不过期回退,单调性保持。
		genTTL := cfg.DSAuth.HubTokenTTL.Std() * 2
		if genTTL < 48*time.Hour {
			genTTL = 48 * time.Hour
		}
		_ = rdb.Expire(genCtx, hubTokenGenKey(pod), genTTL).Err()
		tok, exp, serr := dsSigner.SignDSCallbackWithGen(auth.DSTypeHub, pod, 0, uint64(gen), cfg.DSAuth.HubTokenTTL.Std())
		return tok, exp, uint64(gen), serr
	}
	// enforce 下签发/patch 失败必须 fail-closed(该 Hub DS 不进候选,否则客户端被路由到回调被全拒的 Hub)。
	dsEnforce := dsGuard.Mode() == middleware.DSAuthEnforce
	// modelBAuthority:Model B「Redis 唯一授权权威」(§7)仅在 agones + enforce + authority_mode=redis
	// 三者齐备时启用。启用后签发走两阶段 pending 凭据 + authRepo,legacy 代际镜像门由 Model B 取代关闭。
	modelBAuthority := cfg.Mode == conf.ModeAgones && dsEnforce && cfg.DSAuth.AuthorityModeRedis()
	if modelBAuthority {
		const dsVerifierMaxLeeway = 15 * time.Second
		reservationTTL := cfg.Hub.ReservationTTL.Std()
		minimumReservationTTL := dstV2.TTL() + dsVerifierMaxLeeway
		if reservationTTL < minimumReservationTTL || reservationTTL > cfg.Hub.AssignmentTTL.Std() {
			helper.Errorw("msg", "hub_reservation_ttl_invalid", "reservation_ttl", reservationTTL,
				"minimum", minimumReservationTTL, "assignment_ttl", cfg.Hub.AssignmentTTL.Std(),
				"hint", "reservation_ttl must cover DSTicket TTL + 15s verifier leeway and not exceed assignment_ttl")
			os.Exit(1)
		}
	}
	// hubAuthRepo:Model B 授权记录仓(agones+enforce+redis 时构造,注入 fleet 与 usecase)。
	var hubAuthRepo data.HubAuthRepo
	// authRecordTTL:授权记录 Redis TTL(>= 令牌 TTL,常驻 Hub 心跳持续刷新;与代际计数器同量级)。
	authRecordTTL := cfg.DSAuth.HubTokenTTL.Std() * 2
	if authRecordTTL < 48*time.Hour {
		authRecordTTL = 48 * time.Hour
	}
	// issueHubCredential 是 Model B pending 凭据签发器(§7):领单调 gen(复用 hubTokenGenKey)+
	// 生成 jti(uuid v4)+ SignHubCredential(绑 uid/epoch/gen/jti),返回 Bearer token 与凭据身份。
	// StagePending / annotation 投递由 fleet provider 完成;此处只管签发。
	issueHubCredential := func(pod, instanceUID string, epoch uint32) (string, *hubv1.HubDSCredential, error) {
		genCtx, genCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer genCancel()
		gen, gerr := rdb.Incr(genCtx, hubTokenGenKey(pod)).Result()
		if gerr != nil {
			return "", nil, fmt.Errorf("hub credential gen incr for pod %s: %w", pod, gerr)
		}
		_ = rdb.Expire(genCtx, hubTokenGenKey(pod), authRecordTTL).Err()
		jti := uuid.NewString()
		res, serr := dsSigner.SignHubCredential(pod, instanceUID, epoch, uint64(gen), jti, cfg.DSAuth.HubTokenTTL.Std())
		if serr != nil {
			return "", nil, fmt.Errorf("hub credential sign for pod %s: %w", pod, serr)
		}
		cred := &hubv1.HubDSCredential{
			Gen: uint64(gen),
			Jti: jti,
			// SignHubCredential 回显的是实际序列化进 JWT NumericDate 的 claim 值；原样存储，
			// 才能与验签后 claims.exp 做严格相等比较。
			ExpMs:         uint64(res.ExpMs),
			Kid:           res.Kid,
			InstanceUid:   instanceUID,
			ProtocolEpoch: epoch,
			TokenSha256:   res.TokenSHA256,
			WriterEpoch:   res.WriterEpoch,
		}
		return res.Token, cred, nil
	}
	// local-off-v1 仍只把 K8s/Redis 权威功能关闭，不会退回 legacy JWT。UE 对所有受保护
	// DS RPC 一律要求完整 Model-B tuple；本地实例以唯一 UID、epoch=1、随机 jti 和持久 gen
	// 签发一次性凭据，随后由 UE 的机械隔离 profile 直接设为 active（不等待 Redis ACK）。
	issueLocalHubCredential := func(pod, instanceUID string, epoch uint32) (string, int64, uint64, error) {
		genCtx, genCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer genCancel()
		gen, gerr := rdb.Incr(genCtx, hubTokenGenKey(pod)).Result()
		if gerr != nil {
			return "", 0, 0, fmt.Errorf("local hub credential gen incr for pod %s: %w", pod, gerr)
		}
		res, serr := dsSigner.SignHubCredential(
			pod, instanceUID, epoch, uint64(gen), uuid.NewString(), cfg.DSAuth.HubTokenTTL.Std())
		if serr != nil {
			return "", 0, 0, fmt.Errorf("local hub credential sign for pod %s: %w", pod, serr)
		}
		return res.Token, res.ExpMs, uint64(gen), nil
	}
	// verifyHubCredential 把 pkg/auth 已完整验签的 JWT claims 映射成投递侧严格比对 tuple。
	// annotation 的 gen/exp 永远只是镜像,不能绕过这里的签名/iss/aud/exp 校验。
	verifyHubCredential := func(token string) (*biz.HubCredentialClaims, error) {
		if dsVerifier == nil {
			return nil, fmt.Errorf("hub credential verifier is not configured")
		}
		claims, verr := dsVerifier.VerifyDSCallback(token)
		if verr != nil {
			return nil, verr
		}
		if claims.DSType != string(auth.DSTypeHub) || claims.Pod() == "" || claims.MatchID != 0 || claims.ExpiresAt == nil {
			return nil, fmt.Errorf("hub credential claims scope/exp invalid")
		}
		expMs := claims.ExpiresAt.Time.UnixMilli()
		if expMs <= 0 {
			return nil, fmt.Errorf("hub credential claims exp invalid")
		}
		return &biz.HubCredentialClaims{
			Pod:           claims.Pod(),
			InstanceUID:   claims.UID(),
			ProtocolEpoch: claims.Epoch(),
			Gen:           claims.Gen(),
			JTI:           claims.JTI(),
			ExpMs:         uint64(expMs),
			Kid:           claims.Kid(),
			WriterEpoch:   claims.WriterEpoch(),
		}, nil
	}
	// 4.2 玩家面 / DS 回调面密钥集不相交(P0,二审 #7):hub_allocator 是唯一同时装配两面密钥的服务
	// (jwt 签 hub DSTicket + ds_auth 签/验 DS 回调令牌)。任一交叉 = 泄露一面即可伪造另一面。
	// enforce(生产姿态)下启动即拒;off/permissive 只告警 —— dev 模板两面共用公开 dev 密钥,
	// 硬拒会打断本地一键启动(CLAUDE.md §14.2:默认路径不许坏)。集合含主密钥 + 全部 additional。
	if cfg.DSAuth.Secret != "" {
		playerKeys := append([][]byte{[]byte(cfg.JWT.Secret)}, auth.AdditionalSecretsBytes(cfg.JWT.AdditionalSecrets)...)
		dsKeys := append([][]byte{[]byte(cfg.DSAuth.Secret)}, auth.AdditionalSecretsBytes(cfg.DSAuth.AdditionalSecrets)...)
		if derr := auth.AssertDisjointSecrets(playerKeys, dsKeys); derr != nil {
			if dsEnforce {
				helper.Errorw("msg", "jwt_ds_auth_secret_overlap", "err", derr,
					"hint", "玩家面 jwt.secret/additional_secrets 与 ds_auth.secret/additional_secrets 必须是两套完全独立的密钥(P0);enforce 下启动即拒")
				os.Exit(1)
			}
			helper.Warnw("msg", "jwt_ds_auth_secret_overlap_dev", "err", derr,
				"hint", "玩家面与 DS 回调面密钥交叉,仅 dev 可容忍;生产(enforce)会启动即拒")
		}
	}
	if dsSigner != nil {
		helper.Infow("msg", "ds_callback_token_issuer_ready",
			"hub_token_ttl", cfg.DSAuth.HubTokenTTL.Std().String(), "guard_mode", dsGuard.Mode().String())
	}
	// 5. 装配链
	repo := data.NewRedisHubRepo(rdb)
	// Hub DS 分片来源由 cfg.Mode 单一开关决定(标准两模式 + 离线兜底),biz 逻辑零改:
	//   - mode=agones → 真 GameServer 列表发现分片拓扑(Linux 线上)
	//   - mode=local  → 本机 exec 一个常驻 Windows Hub DS(Windows 单机自测)
	//   - mode=mock   → 确定性假分片(无真实 Hub DS,离线联调)
	var fleet biz.HubFleetProvider
	switch cfg.Mode {
	case conf.ModeAgones:
		af, ferr := biz.NewAgonesHubFleetProvider(cfg)
		if ferr != nil {
			helper.Errorw("msg", "agones_fleet_provider_init_failed", "err", ferr,
				"hint", "检查 agones.fleet_name / ca_path 配置")
			os.Exit(1)
		}
		fleet = af
		if dsSigner != nil {
			if modelBAuthority {
				// Model B:两阶段 pending 凭据投递 + Redis 授权记录仓(§7)。annotation 只投递,
				// 授权由 DS 首个合法 pending 心跳在 authRepo 上原子激活。取代 legacy 代际门。
				hubAuthRepo = data.NewRedisHubAuthRepo(rdb)
				af.SetHubAuthority(hubAuthRepo, issueHubCredential, verifyHubCredential,
					cfg.DSAuth.HubTokenTTL.Std()/3, authRecordTTL)
				helper.Infow("msg", "hub_authority_model_b_ready",
					"authority_mode", "redis", "auth_record_ttl", authRecordTTL.String(),
					"hint", "Model B:Redis 唯一授权权威 + active/pending 两阶段令牌状态机")
			} else {
				// 发现式签发:ListShards 扫到 ready Hub DS 时签发/续期 annotation(剩余寿命 < TTL/3 重签)。
				af.SetDSTokenIssuer(issueHubToken, cfg.DSAuth.HubTokenTTL.Std()/3, dsEnforce)
				if dsVerifier != nil {
					// 续期判据加实测验签:annotation 令牌须验签通过且 sub==pod、ds_type==hub,否则重签
					// (挡空/损坏/旧密钥令牌被误判可用,审核 P1)。
					af.SetDSTokenVerifier(func(token, pod string) error {
						claims, verr := dsVerifier.VerifyDSCallback(token)
						if verr != nil {
							return verr
						}
						if claims.DSType != string(auth.DSTypeHub) || claims.Subject != pod {
							return fmt.Errorf("ds token scope mismatch: ds_type=%q sub=%q want hub/%s", claims.DSType, claims.Subject, pod)
						}
						return nil
					})
				}
			}
		}
		helper.Infow("msg", "agones_fleet_provider_ready",
			"api_server", cfg.Agones.APIServer, "namespace", cfg.Agones.Namespace, "fleet", cfg.Agones.FleetName)
	case conf.ModeLocal:
		// 新 UE 不接受 legacy JWT，也不会在没有 Redis pending/ACK 的本地链路自动降级。
		// 因而本机模式只允许显式 local-off-v1；其他组合若继续启动只会得到永远 staged 的 DS。
		if perr := auth.ValidateDSLocalHubProfileOffV1(
			dsGuard.Mode().String(), cfg.DSAuth.AuthorityMode, dsSigner != nil, cfg.DSAuth.HubTokenTTL.Std()); perr != nil {
			helper.Errorw("msg", "local_hub_auth_profile_invalid",
				"err", perr,
				"hint", "mode=local requires ds_auth.mode=off + authority_mode=legacy + signing key (local-off-v1); Redis Model-B local authority is not implemented")
			os.Exit(1)
		}
		lf, lerr := biz.NewLocalHubFleetProvider(cfg.LocalHub)
		if lerr != nil {
			helper.Errorw("msg", "local_hub_fleet_provider_init_failed", "err", lerr,
				"hint", "mode=local 需 local_hub.executable_path 指向打包好的 UE Windows DS 可执行文件")
			os.Exit(1)
		}
		// 进程随 hub_allocator 退出而 Kill,避免遗留孤儿 Hub DS。
		defer func() { _ = lf.Close() }()
		fleet = lf
		lf.SetDSTokenIssuer(issueLocalHubCredential, true) // 完整 tuple 经 env 下发；签发失败必须 fail-closed
		helper.Infow("msg", "local_hub_fleet_provider_ready",
			"executable", cfg.LocalHub.ExecutablePath, "map", cfg.LocalHub.MapName,
			"advertise_host", cfg.LocalHub.AdvertiseHost, "port", cfg.LocalHub.Port)
	default:
		fleet = biz.NewMockHubFleetProvider(cfg.Hub)
		helper.Warnw("msg", "mock_fleet_provider_active",
			"mode", cfg.Mode, "hint", "mode=mock,用确定性假分片(无真实 Hub DS)")
		// Mock 是拓扑-only 不实现 HubFleetScaler:autoscale/consolidation 在此模式下不会运行。
		// 明确告警避免“yaml 开了但实际没生效”的误导。
		if cfg.Hub.AutoScaleEnabled || cfg.Hub.ConsolidationEnabled {
			helper.Warnw("msg", "autoscale_inert_under_mock",
				"autoscale_enabled", cfg.Hub.AutoScaleEnabled,
				"consolidation_enabled", cfg.Hub.ConsolidationEnabled,
				"hint", "Mock 无真实 Fleet scaler:自动扩缩容/强制整合不会运行,需 mode=agones")
		}
	}
	uc := biz.NewHubUsecase(repo, fleet, &hubTicketSigner{signer: signer, v2: dstV2}, cfg.Hub)

	// owner 权威实例租约双写(owner-authority.md migrate ⑥):owner_addr 空 = 不启用。
	// 弱/强依赖语义见 conf.OwnerLeaseRequired 注释。
	if cfg.Hub.OwnerAddr != "" {
		ownerLease := data.NewGrpcOwnerLeaseRenewer(cfg.Hub.OwnerAddr)
		defer func() { _ = ownerLease.Close() }()
		uc.SetOwnerLeaseRenewer(ownerLease, cfg.Hub.OwnerLeaseRequired)
		// migrate ①/③④:签票点 Begin(HUB) + census 代提交 Admit(同一连接,弱依赖)。
		uc.SetOwnerAuthority(ownerLease)
		helper.Infow("msg", "owner_lease_dual_write_enabled",
			"owner_addr", cfg.Hub.OwnerAddr, "required", cfg.Hub.OwnerLeaseRequired)
	}
	canaryPercent, canarySeed := uint32(0), ""
	if cfg.Mode == conf.ModeAgones {
		canaryPercent, canarySeed = cfg.Agones.CanaryPercent, cfg.Agones.CanarySeed
	}
	releasePolicy, policyErr := releasetrack.New(canaryPercent, canarySeed)
	if policyErr != nil {
		helper.Errorw("msg", "hub_release_track_policy_invalid", "err", policyErr)
		os.Exit(1)
	}
	uc.SetReleaseTrackPolicy(releasePolicy)
	helper.Infow("msg", "hub_release_track_policy_ready", "canary_percent", canaryPercent)
	// agones 真 DS 链路:分片先 warming,等首个通过 Guard 的 Hub DS 心跳才转 ready(审核 P1:
	// PATCH/发现成功 ≠ 收到过真实鉴权心跳,避免把玩家路由到从未心跳的 Hub)。mock/local 不置,保持
	// 现有 dev/离线联调直接 ready 行为不变。
	if cfg.Mode == conf.ModeAgones {
		uc.SetRequireHeartbeatReady(true)
		if modelBAuthority {
			// Model B:注入授权记录仓 → 心跳走 ActivateHeartbeat 单事务线性化点、AssignHub/TransferHub
			// 走 ReserveRoutableSeat/CheckRoutable 原子终态门(§7)。legacy 代际镜像门由 Model B 取代,
			// 故关闭 DSTokenGeneration(避免与 promote 双门叠加)。authRecordTTL 独立注入(CE8:授权键
			// 不被 shardTTL 缩短)。
			uc.SetAuthRepo(hubAuthRepo)
			uc.SetAuthTTL(authRecordTTL)
			uc.SetDSTokenGeneration(false)
			helper.Infow("msg", "hub_usecase_model_b_authority",
				"hint", "Model B:心跳 ActivateHeartbeat 单事务 + Assign/Transfer 原子授权终态门;legacy 代际门已关闭",
				"auth_record_ttl", authRecordTTL.String())
		} else {
			// 令牌代际绑定(二审 #3/#4):仅 enforce 下开启——镜像记录当前令牌 exp,重签/轮换后分片
			// 复位 warming,只有携带新代际已验签令牌的心跳才翻回 ready(挡旧令牌迟到心跳)。
			// off/permissive 下心跳无已验签 claims,开了会自锁,故不开。
			uc.SetDSTokenGeneration(dsEnforce)
		}
		if !dsEnforce {
			// A#2:agones 真 DS 链路却未 enforce → warming→ready 翻转只是「活性」信号,不是鉴权证明,
			// 任何能连上本服务的进程都能伪造心跳把分片置 ready/伪造在场玩家列表。生产必须 enforce
			// (gen_cluster_config.ps1 -Prod 默认改写 ds_auth.mode=enforce);本地 minikube dev 可忽略。
			helper.Warnw("msg", "agones_ds_auth_not_enforce", "guard_mode", dsGuard.Mode().String(),
				"hint", "mode=agones 且 ds_auth.mode!=enforce:Hub 心跳未经令牌鉴权,warming→ready 仅是活性信号;生产环境必须 enforce")
		}
	}

	// 5.1 Kafka producer → migratePusher(弱依赖:broker 不通则 warn 并继续,迁移推送静默丢弃,
	// Hub DS drain 心跳指令仍兜底让客户端重连到新分片)。强制整合 consolidation 才需要。
	if len(cfg.Kafka.Brokers) > 0 {
		producer, perr := kafkax.NewKeyOrderedProducer(cfg.Kafka, kafkax.TopicHubMigrate)
		if perr != nil {
			helper.Warnw("msg", "kafka_producer_init_failed", "err", perr,
				"hint", "hub migrate push will be silently dropped until kafka is available")
		} else {
			defer func() { _ = producer.Close() }()
			uc.SetMigratePusher(&kafkaMigratePusher{p: producer})
			helper.Infow("msg", "kafka_producer_ready", "topic", kafkax.TopicHubMigrate)
		}
	} else if cfg.Hub.ConsolidationEnabled {
		helper.Warnw("msg", "kafka_brokers_empty",
			"hint", "consolidation_enabled 但无 kafka:迁移仅靠 Hub DS drain 心跳兜底,无无缝倒计时推送")
	}

	// 5.2 player_locator gRPC client → HubLocationChecker(弱依赖:玩家主动切线护栏。
	// addr 空则跳过战斗/匹配中检查;真正的"一人一 DS"仍由 DS 侧 SetLocation 强制)。
	if cfg.Hub.LocatorAddr != "" {
		conn := grpcclient.MustDialInsecure(cfg.Hub.LocatorAddr)
		defer func() { _ = conn.Close() }()
		uc.SetLocationChecker(data.NewGrpcHubLocationChecker(conn))
		helper.Infow("msg", "locator_client_ready", "locator_addr", cfg.Hub.LocatorAddr)
	} else {
		helper.Warnw("msg", "locator_addr_empty",
			"hint", "玩家切线不做战斗/匹配中检查(弱依赖,DS 侧 SetLocation 仍强制一人一 DS)")
	}

	svc := service.NewHubService(uc)
	svc.SetDSCallbackGuard(dsGuard)         // DS 回调令牌校验(Heartbeat);nil=off
	svc.SetModelBAuthority(modelBAuthority) // Model B:心跳必须携带 Model B 凭据,legacy 令牌一律拒(CE1/CE2)

	// 6. gRPC + HTTP
	// 会话现行性门(R5 复审 P0-1,INC-20260722-004):ListHubLines/TransferToLine 两个
	// 客户端面 method 的 jti 必须是 login 会话权威当前一代;内部/DS RPC 无 payload 头放行。
	sessGate, sgClose := sessiongate.MustBuild(cfg.Node.RedisClient, cfg.SessionGate.Require)
	defer sgClose()
	// R7 复审 P0-3:biz 也持有 session gate——迁移重签取当前会话 jti 签进 sjti;
	// AcknowledgeAdmission 在消费 reservation 前后双复核票据 sjti 现行性(R7 收口 P0-2)。
	uc.SetSessionGate(sessGate)
	// R7 收口(P0-5):票据 sjti 绑定强制门分阶段激活。默认兼容档(空 sjti 告警放行,
	// 旧 Hub DS/旧签发面残票混版可用);全 fleet DS 转发 sjti + 旧 DS 排空 + 等满票据
	// 最大 TTL 后置 session_gate.require_ticket_sjti=true 收口。
	uc.SetSessionGateRequireSJTI(cfg.SessionGate.RequireTicketSJTI)
	// R9 复审 P1(开关依赖门禁):require_ticket_sjti=true 依赖 session gate 存在
	// (sjti 现行性对照会话权威)。gate 未装配(Redis 未配置)时开关只会静默变形为
	// "永不复核",安全开关必须 fail-fast 而不是装饰性存在。
	if cfg.SessionGate.RequireTicketSJTI && sessGate == nil {
		helper.Errorw("msg", "require_ticket_sjti_needs_session_gate",
			"hint", "配置 node.redis_client(会话权威)或按 rollout 文档显式关闭 session_gate.require_ticket_sjti")
		os.Exit(1)
	}
	if sessGate != nil {
		if cfg.SessionGate.RequireTicketSJTI {
			helper.Infow("msg", "hub_admission_sjti_require_active",
				"note", "空 sjti 硬拒;前提=全 fleet DS 已转发 sjti 且旧票已过期")
		} else {
			helper.Infow("msg", "hub_admission_sjti_tolerant",
				"note", "空 sjti 告警放行(混版兼容窗);排空后开 session_gate.require_ticket_sjti")
		}
	}

	grpcSrv := server.NewGRPCServer(&cfg, svc, sessGate)
	// writerHealth 由下方写者租约启动后注入(/healthz/writer 只做观测,不参与流量门,
	// 理由见 internal/server/http.go 的 WriterHealthHolder 注释)。
	writerHealth := &server.WriterHealthHolder{}
	httpSrv := server.NewHTTPServer(&cfg, writerHealth)

	// 任何后台 reconcile/sweep 与 RPC server 启动前先取得 capability。失败进程零业务写。
	if cfg.DSAuth.AuthorityModeRedis() {
		var features []string
		if modelBAuthority {
			features = []string{"hub-reservation-ledger-v1", "hub-heartbeat-capacity-v1",
				"hub-owner-cleanup-v1", "hub-physical-eviction-v1", "hub-successor-lease-v1"}
		}
		fence, err := dsauthfence.AcquireRuntime(context.Background(), dsauthfence.RuntimeConfig{
			Endpoints: cfg.DSAuth.Fence.EtcdEndpoints, Prefix: cfg.DSAuth.Fence.EtcdPrefix,
			Service: serviceName, KeysetRevision: cfg.DSAuth.Fence.KeysetRevision,
			WriterEpoch: dsauthfence.ProtocolEpochV2,
			Features:    features,
			LeaseTTLSec: cfg.DSAuth.Fence.EtcdLeaseTTLSec, DialTimeout: cfg.DSAuth.Fence.EtcdDialTimeout.Std(),
		})
		if err != nil {
			helper.Errorw("msg", "ds_auth_fence_acquire_failed", "err", err)
			os.Exit(1)
		}
		defer func() { _ = fence.Close() }()
		if fence.RequiredPolicyGeneration() != dsauthfence.RequiredPolicyGenerationV3 {
			helper.Warnw("msg", "ds_auth_fence_staging_only",
				"required_policy_generation", fence.RequiredPolicyGeneration(),
				"required_policy", dsauthfence.RequiredPolicyV3,
				"hint", "capability 仅供 V3 激活审计；V3 生效并触发 Lost/重启前禁止启动 RPC 与后台 writer")
			<-fence.Lost()
			_ = fence.Close()
			os.Exit(0)
		}
		go func() {
			<-fence.Lost()
			helper.Errorw("msg", "ds_auth_fence_lost", "reason", fence.LostReason(),
				"hint", "立即退出，禁止旧 writer 在失租/epoch 回退后继续写")
			os.Exit(1)
		}()
		helper.Infow("msg", "ds_auth_fence_ready", "required_writer_epoch", fence.RequiredEpoch(), "reclaimed_stale_capability", fence.Reclaimed())

		// R9 P0-7:写者继任租约(session-generation-rollout.md §5)。把「单写者」从部署
		// 策略(Recreate)下沉为运行时协议:全体副本竞选同一 etcd election,仅当选
		// 副本可写(biz 入口 gate),存储层在同一 Redis 事务内比较/推进单调 fencing
		// token(data/writer_fence.go),迟到旧写者零写入。etcd 已是本模式硬依赖
		// (上方 AcquireRuntime),此处不新增依赖类别;启动失败 fail-fast。
		// 档位(R10 P0-5,rollout §5.4):enforce=稳态;warmup=只竞选不接线(首次引导
		// 升级的第一跳);off=不启动(仅历史 Recreate 单副本)。非法值 fail-fast。
		writerMode, wmErr := cfg.Hub.ResolveWriterLeaseMode()
		if wmErr != nil {
			helper.Errorw("msg", "hub_writer_lease_mode_invalid", "err", wmErr)
			os.Exit(1)
		}
		// R11 复审 P0-5:机械门禁。writer_lease_mode != enforce 时单写者**只**由部署策略
		// (单副本 Recreate)保证;若部署实际是 RollingUpdate,滚动重叠窗口里新旧副本都
		// 照常写 = 无保护双写(§9.1 一人一 hub、§9.22 单写者直接破)。此前仓库里没有任何
		// 机制阻止 warmup/off × RollingUpdate 这个组合——warmup 连 off 的告警都没有。
		//
		// 进程看不到 spec.strategy,故由 Deployment 把策略作为 annotation 注入 env
		// (deploy/k8s/services/services.yaml + main_test.go 的清单契约测试钉住 annotation
		// 与真实 strategy 一致)。
		//
		// R11 二轮复审收口:env 缺失时**不能一律只告警**。那让"机械门禁"变成有条件的——
		// 只要清单漏注入(或有人手工 kubectl run),危险组合就照样放行,而这正是最可能出错的
		// 场景。判据改成"能不能确认自己跑在受管 k8s 里":k8s 恒注入 KUBERNETES_SERVICE_HOST,
		// 本机裸跑/dev 一定没有。
		//   · 受管 k8s 内 + env 缺失 → **fail-closed 退出**(清单回归必须炸,不能靠人看日志);
		//   · 非 k8s(本机裸跑)+ env 缺失 → 只告警(阻断会把开发环境一起打死)。
		strategy := strings.TrimSpace(os.Getenv("PANDORA_DEPLOY_STRATEGY"))
		inManagedK8s := strings.TrimSpace(os.Getenv("KUBERNETES_SERVICE_HOST")) != ""
		if strategy != "" {
			switch {
			case !strings.EqualFold(strategy, "RollingUpdate"):
				helper.Infow("msg", "hub_writer_lease_strategy_checked",
					"strategy", strategy, "mode", writerMode)
			case writerMode != conf.WriterLeaseEnforce:
				helper.Errorw("msg", "hub_writer_lease_rollingupdate_without_enforce",
					"strategy", strategy, "mode", writerMode,
					"hint", "RollingUpdate × writer_lease_mode!=enforce = 滚动重叠期无保护双写;"+
						"要么把 writer_lease_mode 改 enforce,要么把 Deployment 改回单副本 Recreate(rollout §5.4 引导升级每跳 Recreate)")
				os.Exit(1)
			default:
				helper.Infow("msg", "hub_writer_lease_strategy_checked",
					"strategy", strategy, "mode", writerMode)
			}
		} else if inManagedK8s {
			helper.Errorw("msg", "hub_writer_lease_strategy_annotation_missing",
				"mode", writerMode,
				"hint", "受管 k8s 内必须注入 PANDORA_DEPLOY_STRATEGY(取自 Deployment 的 "+
					"pandora.dev/deploy-strategy annotation);缺失则无法机械校验 RollingUpdate×非 enforce "+
					"的无保护双写组合,fail-closed 退出。见 deploy/k8s/services/services.yaml")
			os.Exit(1)
		} else {
			helper.Warnw("msg", "hub_writer_lease_strategy_unknown",
				"mode", writerMode,
				"hint", "非 k8s 环境(本机裸跑/dev):跳过部署策略机械校验")
		}
		if writerMode == conf.WriterLeaseOff {
			helper.Warnw("msg", "hub_writer_lease_disabled",
				"hint", "writer_lease_mode=off:单写者不再由运行时协议保证,只允许单副本 Recreate 部署;RollingUpdate 下必须改回 enforce")
		} else {
			hostname, _ := os.Hostname()
			identity := fmt.Sprintf("%s/%d", hostname, os.Getpid())
			leaseCfg := writerlease.Config{
				Endpoints:   cfg.DSAuth.Fence.EtcdEndpoints,
				Election:    "hub_allocator/writer",
				Identity:    identity,
				LeaseTTLSec: int(cfg.DSAuth.Fence.EtcdLeaseTTLSec),
				DialTimeout: cfg.DSAuth.Fence.EtcdDialTimeout.Std(),
			}
			if writerMode == conf.WriterLeaseEnforce {
				// 接流前硬门(R10 P0-4):当选后先把**全部已知 pod** 的 fence 水位推进到
				// 本届 token,成功才宣告持有领导权。此前推扫挂在后台 sweep tick 上懒执行,
				// 于是"当选即接写、推扫尚未完成"的窗口里,前任在未被触碰的 {pod} slot 上
				// 仍能写。钩子失败 = 让位重选,本副本恒不持有(写请求继续可重试拒绝)。
				leaseCfg.OnElected = func(ctx context.Context, token uint64) error {
					return repo.AdvanceWriterFencesForToken(ctx, token)
				}
			}
			writerLease, wlErr := writerlease.Start(context.Background(), leaseCfg)
			if wlErr != nil {
				helper.Errorw("msg", "hub_writer_lease_start_failed", "err", wlErr)
				os.Exit(1)
			}
			defer func() { _ = writerLease.Close() }()
			if writerMode == conf.WriterLeaseEnforce {
				uc.SetWriterFence(writerLease)
				repo.SetWriterFence(writerLease)
				if authRepo, ok := hubAuthRepo.(*data.RedisHubAuthRepo); ok {
					authRepo.SetWriterFence(writerLease)
				}
			}
			writerHealth.Set(writerLease, writerMode)
			helper.Infow("msg", "hub_writer_lease_started",
				"election", "hub_allocator/writer", "identity", identity, "mode", writerMode,
				"hint", "enforce:未当选副本拒写(可重试)+接流前推扫硬门+存储级 fencing;warmup:只竞选观测 token 单调,不改写路径")
		}
	}

	// 7. 后台心跳超时扫描(随进程生命周期启停)
	sweepCtx, sweepCancel := context.WithCancel(context.Background())
	defer sweepCancel()
	go uc.RunHeartbeatSweep(sweepCtx)

	helper.Infow(
		"msg", "service_ready",
		"grpc", cfg.Server.Grpc.Addr,
		"http", cfg.Server.Http.Addr,
		"redis_addr", rc.Host,
		"heartbeat_timeout", cfg.Hub.HeartbeatTimeout.String(),
		"sweep_interval", cfg.Hub.SweepInterval.String(),
		"default_region", cfg.Hub.DefaultRegion,
		"mock_shard_count", cfg.Hub.MockShardCount,
		"fleet_mode", cfg.Mode,
		"autoscale_enabled", cfg.Hub.AutoScaleEnabled,
		"consolidation_enabled", cfg.Hub.ConsolidationEnabled,
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

// hubTicketSigner 把 biz.TicketSigner 适配到 pkg/auth.Signer。
// hub DSTicket:ds_type=hub,match_id=0(不变量 §3 短时效 5min;jti=uuid v4 防重放)。
// roleID(选角权威化 2026-07-08):>0 时盖进票据 role_id claim;region/cell 仍为 0
// (hub_allocator 不做 cell 路由,与历史行为一致)。
//
// v2(方案 B)非 nil 时:改签 RS256 实例绑定票。v2 票据**必须**带完整实例绑定
// (pod / instance_uid / instance_epoch / hub_assignment_id),无绑定路径 fail-closed 拒签;
// v2 故意不绑 credential gen/jti——回调凭据轮换不得作废玩家票(决策文档 §7.3)。
type hubTicketSigner struct {
	signer *auth.Signer
	v2     *auth.DSTicketSigner
}

func (h *hubTicketSigner) SignHubTicket(playerID uint64, roleID uint32, binding biz.HubTicketBinding) (string, int64, error) {
	jti := uuid.NewString()
	if h.v2 != nil {
		if binding.PodName == "" || binding.InstanceUID == "" ||
			binding.ProtocolEpoch == 0 || binding.HubAssignmentID == "" || !releasetrack.Valid(binding.ReleaseTrack) {
			return "", 0, fmt.Errorf(
				"ds_ticket v2: hub 票必须带完整实例绑定(pod=%q uid=%q epoch=%d assignment=%q track=%q),拒签无绑定票",
				binding.PodName, binding.InstanceUID, binding.ProtocolEpoch, binding.HubAssignmentID, binding.ReleaseTrack)
		}
		return h.v2.SignHubTicket(playerID, 0, 0, roleID, jti, auth.DSTicketTarget{
			DSPodName:       binding.PodName,
			DSInstanceUID:   binding.InstanceUID,
			DSInstanceEpoch: binding.ProtocolEpoch,
			HubAssignmentID: binding.HubAssignmentID,
			ReleaseTrack:    binding.ReleaseTrack,
			SourceMatchID:   binding.SourceMatchID,
			SessionJTI:      binding.SessionJTI, // R6 P0-3:会话绑定,VerifyDSTicket 核销时复核
		})
	}
	if binding.PodName == "" {
		return h.signer.SignHubDSTicketFull(playerID, 0, 0, roleID, binding.SourceMatchID, jti)
	}
	return h.signer.SignBoundHubDSTicket(playerID, 0, 0, roleID, binding.SourceMatchID, jti, auth.DSTicketBinding{
		DSPodName: binding.PodName, DSInstanceUID: binding.InstanceUID,
		ProtocolEpoch: binding.ProtocolEpoch, CredentialGen: binding.CredentialGen,
		CredentialJTI: binding.CredentialJTI, HubAssignmentID: binding.HubAssignmentID,
		WriterEpoch: binding.WriterEpoch,
	})
}

// kafkaMigratePusher 把 biz.HubMigratePusher 适配到 kafkax.KeyOrderedProducer。
// 强制整合时把 HubMigrateEvent payload 按 player_id(kafka key)推给被迁移玩家本人。
type kafkaMigratePusher struct {
	p *kafkax.KeyOrderedProducer
}

func (k *kafkaMigratePusher) PushMigrate(ctx context.Context, playerID uint64, payload []byte) error {
	_, err := k.p.PushToPlayers(ctx, 0, []uint64{playerID}, payload)
	return err
}
