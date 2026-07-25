// agones_fleet.go — 真 Agones HubFleetProvider(W4 ⑬,2026-06-09)。
//
// 与 ds_allocator 的战斗 DS 模型不同:大厅 Hub DS 是「常驻分片」而非「按需分配」。
// Hub DS GameServer 持续以 Ready 状态运行,hub_allocator 自己在 Redis 里维护各分片的
// player_count 做容量判定(不走 Agones GameServerAllocation)。因此本 provider 的职责是
// 「发现拓扑」——LIST Fleet 下的 GameServer(按 region 标签过滤),把可承载玩家的实例
// 映射成 ShardCandidate,供 biz.ensureShards lazy-seed 到 Redis。
//
// 实现路线与 ds_allocator/data/agones_allocator.go 一致:用标准库 net/http 直连
// k8s apiserver REST 查 agones.dev/v1 GameServer 列表,不引入 agones/client-go 重依赖,
// 保持 hub_allocator go.mod 干净、本地可编译可单测;Agones API 与 k8s provider 无关
// (ACK / 自建 / minikube 一致),故 provider-agnostic。
//
// 阶段限制(W4 ⑬):ensureShards 仅在 region 首次无分片时 lazy-seed,Fleet 扩缩容后的
// 新 GameServer 不会被自动发现(周期性 reconcile 留后续)。本地 dev 联调够用。
package biz

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/releasetrack"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/data"
)

const (
	// fleetLabelKey 是 Agones Fleet 给其 GameServer 打的标签 key(selector 用)。
	fleetLabelKey = "agones.dev/fleet"
	// regionLabelKey 是 Pandora 给 Hub DS GameServer 打的分区标签(按 region 过滤分片)。
	regionLabelKey = "pandora.dev/region"
	// shardIDLabelKey 可选:Hub DS 显式声明的稳定 shard_id(缺省则按 pod 名哈希派生)。
	shardIDLabelKey = "pandora.dev/shard-id"
	// capacityLabelKey 可选:单分片人数上限(缺省用 cfg.DefaultCapacity)。
	capacityLabelKey = "pandora.dev/capacity"
	// releaseTrackMetadataKey 同时用作精确 selector 与实际轨 annotation 审计字段。
	releaseTrackMetadataKey = "pandora.dev/release-track"
)

// hubReadyStates 是「可承载玩家」的 GameServer 状态集合。
// Hub DS 常驻 Ready;运维若对其做过 Allocation 保护(防缩容)则为 Allocated;
// Reserved 也短暂可用。其余(Shutdown / Error / Unhealthy / Scheduled 等)排除。
var hubReadyStates = map[string]struct{}{
	"Ready":     {},
	"Allocated": {},
	"Reserved":  {},
}

// AgonesHubFleetProvider 经 k8s apiserver REST 查 Agones GameServer 列表发现 Hub 分片拓扑。
type AgonesHubFleetProvider struct {
	apiServer       string // 已去尾部 /
	namespace       string
	fleetName       string
	canaryFleetName string
	advertiseHost   string
	tokenPath       string // "" 或 "-" → 不带 Authorization
	listTimeout     time.Duration
	capacity        int32
	httpClient      *http.Client

	// dsTokenIssuer 签发 DS 回调服务令牌(审核 P1 #1;main 在 ds_auth.secret 已配时注入)。
	// Hub DS 是常驻分片(不走 GameServerAllocation),所以采用「发现式签发」:ListShards
	// 扫到 ready GameServer 时,若其 pandora.dev/ds-token annotation 缺失或剩余寿命不足
	// dsTokenRenewBefore,就重签并 merge-patch 回 GameServer。DS 经 Agones SDK
	// WatchGameServer 观察 annotation 变更即可拿到新令牌(续期对 DS 透明)。
	// 签发/补丁失败只告警不阻断拓扑发现。
	dsTokenIssuer      func(pod string) (token string, expiresAtMs int64, gen uint64, err error)
	dsTokenRenewBefore time.Duration
	// dsTokenVerify 实测 annotation 里现存令牌是否仍有效(验签 + iss/aud/exp + ds_type=hub + sub=pod)。
	// 可选:main 在 ds_auth.secret 已配时注入。nil 时续期只看外置 exp annotation(旧行为)。
	// 作用(审核 P1):annotation 有 token 字段且 exp 未近,但 token 为空 / 损坏 / 由旧密钥(轮换前)
	// 签发时,旧逻辑会误判“可用”而不续期,导致 DS 持一张验不过的令牌。加验签后这些一律触发重签。
	dsTokenVerify func(token, pod string) error
	// dsTokenRequired 为 guard=enforce 时 true:签发/patch 失败时该 Hub DS 不进候选(否则
	// 客户端会被路由到一个回调必被 enforce 守卫全拒的 Hub)。off/permissive 下 false,
	// 失败只告警不影响拓扑发现。
	dsTokenRequired bool

	// ===== Model B「Redis 唯一授权权威」两阶段令牌投递(decision-revisit §7)=====
	// 仅 ds_auth.authority_mode=redis 时由 main 注入(SetHubAuthority);nil = 走上面 legacy
	// 代际门单阶段签发。装配后 ListShards 对每个 ready Hub GS 走 ensureHubCredential:
	// InitAuth(绑 gs.uid)→ StagePending(签 pending 凭据)→ annotation 投递 → MarkDelivered;
	// DS 首个合法 pending 心跳在 authRepo 上原子激活。annotation 只负责投递,Redis 是唯一权威。
	authRepo data.HubAuthRepo
	// hubCredIssuer 签发 Model B pending 凭据:main 内做「INCR 领单调 gen + 生成 jti +
	// auth.Signer.SignHubCredential」,返回 Bearer token 与凭据身份(gen/jti/exp/kid/sha256)。
	hubCredIssuer func(pod, instanceUID string, epoch uint32) (token string, cred *hubv1.HubDSCredential, err error)
	// hubCredVerifier 对 annotation 中的 Bearer 做完整验签,返回 JWT 内的授权 tuple。
	// Model B 不允许只比 annotation gen/exp:annotation 永远只是投递镜像,JWT claims + Redis
	// pending 才共同决定该 bundle 是否为本次签发的同一凭据。
	hubCredVerifier func(token string) (*HubCredentialClaims, error)
	// authTTL 授权记录 Redis TTL(建议 >= HubTokenTTL,常驻 Hub 心跳持续续期)。
	authTTL time.Duration
	// hubAuthorityConfigured 区分「完全没启用 Model B」与「Model B 装配缺依赖」。后者必须
	// fail-closed,绝不能静默回退 legacy/no-token 路径。
	hubAuthorityConfigured bool
}

// HubCredentialClaims 是已经由 pkg/auth.Verifier 完整验签后的 Model B JWT tuple。
// Kid 同时存在于签名 claim 与 JWT header；token_sha256 再绑定完整 header.payload.signature。
type HubCredentialClaims struct {
	Pod           string
	InstanceUID   string
	ProtocolEpoch uint32
	Gen           uint64
	JTI           string
	ExpMs         uint64
	Kid           string
	WriterEpoch   uint32
}

// SetHubAuthority 启用 Model B「Redis 唯一授权权威」两阶段令牌投递(§7;仅 authority_mode=redis)。
// authRepo:授权记录仓;issuer:签发并返回 pending 凭据;verifier:验 JWT 并返回 claims;
// renewBefore:平滑轮换提前量;authTTL:授权记录 TTL。调用本方法后任一依赖缺失都 fail-closed。
func (a *AgonesHubFleetProvider) SetHubAuthority(
	authRepo data.HubAuthRepo,
	issuer func(pod, instanceUID string, epoch uint32) (string, *hubv1.HubDSCredential, error),
	verifier func(token string) (*HubCredentialClaims, error),
	renewBefore, authTTL time.Duration,
) {
	a.hubAuthorityConfigured = true
	a.authRepo = authRepo
	a.hubCredIssuer = issuer
	a.hubCredVerifier = verifier
	a.dsTokenRequired = true
	if renewBefore > 0 {
		a.dsTokenRenewBefore = renewBefore
	}
	if authTTL > 0 {
		a.authTTL = authTTL
	}
}

// modelBActive 报告本 provider 是否处于 Model B 两阶段投递模式。
func (a *AgonesHubFleetProvider) modelBActive() bool {
	return a.hubAuthorityConfigured && a.authRepo != nil && a.hubCredIssuer != nil && a.hubCredVerifier != nil && a.authTTL > 0
}

const (
	// dsTokenAnnotationKey 下发 DS 回调令牌的 GameServer annotation key(与 ds_allocator 一致)。
	dsTokenAnnotationKey = "pandora.dev/ds-token"
	// dsTokenExpAnnotationKey 记录令牌过期时刻(UnixMilli),供续期判定,避免解 JWT。
	dsTokenExpAnnotationKey = "pandora.dev/ds-token-exp-ms"
	// dsTokenGenAnnotationKey 记录令牌代际(Redis INCR 单调值),供拓扑对账写入分片记录做精确代际比较。
	dsTokenGenAnnotationKey      = "pandora.dev/ds-token-gen"
	dsTokenJTIAnnotationKey      = "pandora.dev/ds-token-jti"
	dsInstanceUIDAnnotationKey   = "pandora.dev/ds-instance-uid"
	dsInstanceEpochAnnotationKey = "pandora.dev/ds-instance-epoch"
	dsWriterEpochAnnotationKey   = "pandora.dev/ds-writer-epoch"
	dsTokenKidAnnotationKey      = "pandora.dev/ds-token-kid"
	dsTokenHashAnnotationKey     = "pandora.dev/ds-token-sha256"
)

var dsCredentialAnnotationKeys = []string{
	dsTokenAnnotationKey, dsTokenExpAnnotationKey, dsTokenGenAnnotationKey,
	dsTokenJTIAnnotationKey, dsInstanceUIDAnnotationKey, dsInstanceEpochAnnotationKey,
	dsWriterEpochAnnotationKey, dsTokenKidAnnotationKey, dsTokenHashAnnotationKey,
}

// SetDSTokenIssuer 注入 DS 回调令牌签发器(可选依赖,main 在 ds_auth.secret 已配时调用)。
// renewBefore:剩余寿命小于此值时重签续期(建议 TTL/3)。
// required=true(guard=enforce)时签发/patch 失败的 Hub DS 会在 ListShards 被跳过(fail-closed)。
func (a *AgonesHubFleetProvider) SetDSTokenIssuer(f func(pod string) (string, int64, uint64, error), renewBefore time.Duration, required bool) {
	a.dsTokenIssuer = f
	a.dsTokenRequired = required
	if renewBefore > 0 {
		a.dsTokenRenewBefore = renewBefore
	}
}

// SetDSTokenVerifier 注入现存令牌验签器(可选,须在首次 ListShards 前调用)。
// 供 ensureDSToken 在“exp 未近”之外再实测 annotation 令牌确实验签通过(挡空/损坏/旧密钥令牌)。
func (a *AgonesHubFleetProvider) SetDSTokenVerifier(f func(token, pod string) error) {
	a.dsTokenVerify = f
}

// NewAgonesHubFleetProvider 构造真 Agones 分片发现器。
//
// 失败场景(返 error,main 据此 fatal 或回退):
//   - Enabled 但 FleetName 空(无法选择 GameServer)
//   - CA 文件配置了却解析失败
func NewAgonesHubFleetProvider(cfg conf.Config) (*AgonesHubFleetProvider, error) {
	ag := cfg.Agones
	if ag.FleetName == "" {
		return nil, fmt.Errorf("agones: fleet_name required when enabled")
	}
	if _, err := releasetrack.New(ag.CanaryPercent, ag.CanarySeed); err != nil {
		return nil, fmt.Errorf("agones: invalid canary policy: %w", err)
	}
	if ag.CanaryPercent > 0 && strings.TrimSpace(ag.CanaryFleetName) == "" {
		return nil, fmt.Errorf("agones: canary_fleet_name required when canary_percent > 0")
	}
	// 当前 scaler 只治理 stable Fleet；双轨时继续启用会把两轨总负载错误作用到单轨。
	if strings.TrimSpace(ag.CanaryFleetName) != "" && cfg.Hub.AutoScaleEnabled {
		return nil, fmt.Errorf("agones: hub autoscale is not supported with split stable/canary fleets")
	}

	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: ag.InsecureSkipTLSVerify, //nolint:gosec // 仅 dev 显式开启
	}
	if !ag.InsecureSkipTLSVerify && ag.CAPath != "" {
		// CA 文件存在才加载;in-cluster 默认路径在集群外不存在 → 跳过用系统根证书池。
		if pem, err := os.ReadFile(ag.CAPath); err == nil {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("agones: parse CA %s failed", ag.CAPath)
			}
			tlsCfg.RootCAs = pool
		}
	}

	timeout := ag.ListTimeout.Std()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	capacity := cfg.Hub.DefaultCapacity
	if capacity <= 0 {
		capacity = 500
	}

	return &AgonesHubFleetProvider{
		apiServer:       strings.TrimRight(ag.APIServer, "/"),
		namespace:       ag.Namespace,
		fleetName:       ag.FleetName,
		canaryFleetName: strings.TrimSpace(ag.CanaryFleetName),
		advertiseHost:   strings.TrimSpace(ag.AdvertiseHost),
		tokenPath:       ag.TokenPath,
		listTimeout:     timeout,
		capacity:        capacity,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
		},
	}, nil
}

// ── GameServer list 响应 JSON(只声明用到的字段)──────────────────────────────

type gsPort struct {
	Name string `json:"name"`
	Port int32  `json:"port"`
}

type gsStatus struct {
	State   string   `json:"state"`
	Address string   `json:"address"`
	Ports   []gsPort `json:"ports"`
}

type gsMetadata struct {
	Name        string            `json:"name"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	// ResourceVersion 供令牌 annotation 重签做乐观并发 CAS(审核 P1 #9):merge-patch 携带它,
	// apiserver 在 live 对象 rv 已变时回 409 Conflict,冲突方重读再判定,避免多副本交错覆盖。
	ResourceVersion string `json:"resourceVersion"`
	UID             string `json:"uid"`
}

type ownerReference struct {
	Kind string `json:"kind"`
	UID  string `json:"uid"`
}

type podMetadata struct {
	Name            string           `json:"name"`
	UID             string           `json:"uid"`
	OwnerReferences []ownerReference `json:"ownerReferences"`
}

type kubernetesPod struct {
	Metadata podMetadata `json:"metadata"`
}

type gameServer struct {
	Metadata gsMetadata `json:"metadata"`
	Status   gsStatus   `json:"status"`
}

type gsListResponse struct {
	Items []gameServer `json:"items"`
}

type fleetSpec struct {
	Replicas int32 `json:"replicas"`
}

type fleetResponse struct {
	Spec fleetSpec `json:"spec"`
}

// ListShards 分别发现 stable/canary。配置只决定要查哪个 Fleet；最终 ReleaseTrack
// 必须由 GameServer label+annotation 一致证明，不能把 cohort 意图当成实际命中轨。
func (a *AgonesHubFleetProvider) ListShards(ctx context.Context, region string) ([]ShardCandidate, error) {
	stable, err := a.listTrackShards(ctx, region, a.fleetName, releasetrack.Stable)
	if err != nil {
		return nil, err
	}
	out := stable
	if a.canaryFleetName != "" {
		canary, err := a.listTrackShards(ctx, region, a.canaryFleetName, releasetrack.Canary)
		if err != nil {
			return nil, err
		}
		out = append(out, canary...)
	}
	return out, nil
}

// ObserveShardInstance reads the unfiltered GameServer and its Pod.  It does
// not use readiness, health, address, release-track, or callback-token gates:
// those are routing facts, not physical-liveness facts.
func (a *AgonesHubFleetProvider) ObserveShardInstance(ctx context.Context, pod string) (HubInstanceObservation, error) {
	if strings.TrimSpace(pod) == "" {
		return HubInstanceObservation{}, fmt.Errorf("agones: observe hub shard requires pod")
	}
	obs := HubInstanceObservation{}
	gsURL := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers/%s",
		a.apiServer, a.namespace, url.PathEscape(pod))
	gsBytes, gsStatus, err := a.do(ctx, http.MethodGet, gsURL, nil, "")
	if err != nil {
		return HubInstanceObservation{}, fmt.Errorf("agones: observe gameserver %s: %w", pod, err)
	}
	switch {
	case gsStatus == http.StatusNotFound:
	case gsStatus >= 200 && gsStatus < 300:
		var gs gameServer
		if err := json.Unmarshal(gsBytes, &gs); err != nil {
			return HubInstanceObservation{}, fmt.Errorf("agones: decode observed gameserver %s: %w", pod, err)
		}
		if gs.Metadata.Name != pod || gs.Metadata.UID == "" {
			return HubInstanceObservation{}, fmt.Errorf("agones: observed gameserver %s missing exact identity", pod)
		}
		obs.GameServerFound = true
		obs.GameServerUID = gs.Metadata.UID
	default:
		return HubInstanceObservation{}, fmt.Errorf("agones: observe gameserver %s http %d: %s",
			pod, gsStatus, truncateBody(gsBytes, 256))
	}

	podURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s",
		a.apiServer, a.namespace, url.PathEscape(pod))
	podBytes, podStatus, err := a.do(ctx, http.MethodGet, podURL, nil, "")
	if err != nil {
		return HubInstanceObservation{}, fmt.Errorf("agones: observe pod %s: %w", pod, err)
	}
	switch {
	case podStatus == http.StatusNotFound:
		return obs, nil
	case podStatus >= 200 && podStatus < 300:
		var p kubernetesPod
		if err := json.Unmarshal(podBytes, &p); err != nil {
			return HubInstanceObservation{}, fmt.Errorf("agones: decode observed pod %s: %w", pod, err)
		}
		if p.Metadata.Name != pod || p.Metadata.UID == "" {
			return HubInstanceObservation{}, fmt.Errorf("agones: observed pod %s missing exact identity", pod)
		}
		obs.PodFound = true
		for _, owner := range p.Metadata.OwnerReferences {
			if owner.Kind == "GameServer" && owner.UID != "" {
				obs.PodOwnerGameServerUID = owner.UID
				break
			}
		}
		return obs, nil
	default:
		return HubInstanceObservation{}, fmt.Errorf("agones: observe pod %s http %d: %s",
			pod, podStatus, truncateBody(podBytes, 256))
	}
}

func (a *AgonesHubFleetProvider) listTrackShards(ctx context.Context, region, fleetName, releaseTrack string) ([]ShardCandidate, error) {
	if fleetName == "" || !releasetrack.Valid(releaseTrack) {
		return nil, fmt.Errorf("agones: invalid fleet/release track pair fleet=%q track=%q", fleetName, releaseTrack)
	}
	selector := fleetLabelKey + "=" + fleetName + "," + releaseTrackMetadataKey + "=" + releaseTrack
	if region != "" {
		selector += "," + regionLabelKey + "=" + region
	}
	q := url.Values{}
	q.Set("labelSelector", selector)
	listURL := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers?%s",
		a.apiServer, a.namespace, q.Encode())

	respBytes, status, err := a.do(ctx, http.MethodGet, listURL, nil, "")
	if err != nil {
		return nil, fmt.Errorf("agones: list gameservers fleet=%s track=%s region=%s: %w", fleetName, releaseTrack, region, err)
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("agones: list gameservers fleet=%s track=%s region=%s http %d: %s",
			fleetName, releaseTrack, region, status, truncateBody(respBytes, 256))
	}

	var resp gsListResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, fmt.Errorf("agones: decode gameserver list: %w", err)
	}

	out := make([]ShardCandidate, 0, len(resp.Items))
	for i := range resp.Items {
		gs := &resp.Items[i]
		if gs.Metadata.Labels[fleetLabelKey] != fleetName ||
			gs.Metadata.Labels[releaseTrackMetadataKey] != releaseTrack ||
			gs.Metadata.Annotations[releaseTrackMetadataKey] != releaseTrack {
			plog.With(ctx).Warnw("msg", "hub_gameserver_release_track_metadata_invalid",
				"pod", gs.Metadata.Name, "fleet", fleetName, "release_track", releaseTrack)
			continue
		}
		if _, ok := hubReadyStates[gs.Status.State]; !ok {
			continue
		}
		if gs.Status.Address == "" || len(gs.Status.Ports) == 0 {
			continue // 尚未就绪(无 address/port),跳过
		}
		// DS 回调服务令牌:annotation 缺失/即将过期/验签不过 → 重签 + patch。
		// enforce(dsTokenRequired)下签发/patch 失败 → TokenReady=false:该分片仍在拓扑里返回
		// (供对账区分“Fleet 里没有” vs “Fleet 里有但令牌不可用”),但不会被当作可用镜像分配出去
		// (审核 P1:原来直接 skip 会让全 region 令牌失败时对账误判 Fleet 空而保留旧 ready 镜像)。
		// off/permissive 下令牌失败不影响可用性,TokenReady 恒 true。
		tokenReady := true
		tokenExpMs, tokenGen, terr := a.ensureDSTokenOrCredential(ctx, gs)
		if terr != nil && a.dsTokenRequired {
			tokenReady = false
			plog.With(ctx).Warnw("msg", "hub_ds_token_required_unusable", "pod", gs.Metadata.Name, "err", terr,
				"mode", "enforce", "hint", "ds_auth.mode=enforce 下签发/patch 失败:该 Hub 令牌不可用,不进可用镜像")
		}
		host := gs.Status.Address
		if a.advertiseHost != "" {
			host = a.advertiseHost
		}
		out = append(out, ShardCandidate{
			PodName:      gs.Metadata.Name,
			Addr:         fmt.Sprintf("%s:%d", host, gs.Status.Ports[0].Port),
			Region:       region,
			ShardID:      shardIDFor(gs),
			Capacity:     capacityFor(gs, a.capacity),
			ReleaseTrack: releaseTrack,
			TokenReady:   tokenReady,
			TokenExpMs:   tokenExpMs,
			TokenGen:     tokenGen,
		})
	}
	return out, nil
}

// ensureDSTokenOrCredential 是令牌供给分发器:Model B(authority_mode=redis)走两阶段 pending
// 凭据投递 ensureHubCredential;否则走 legacy 代际门单阶段 ensureDSToken。返回值语义一致:
// (当前生效令牌 exp_ms, gen, err),供 ListShards 判定 TokenReady / 写镜像。
func (a *AgonesHubFleetProvider) ensureDSTokenOrCredential(ctx context.Context, gs *gameServer) (int64, uint64, error) {
	if a.hubAuthorityConfigured {
		if !a.modelBActive() {
			return 0, 0, fmt.Errorf("hub credential authority dependencies incomplete")
		}
		return a.ensureHubCredential(ctx, gs)
	}
	return a.ensureDSToken(ctx, gs)
}

// ensureHubCredential 是 Model B 两阶段令牌投递(§7):
//  1. InitAuth:确保授权记录存在并绑定当前 GameServer 实例(gs.uid);换实例/首见 → 复位 BOOTSTRAP。
//  2. 若 annotation 现存令牌严格匹配 Redis active/pending 完整 tuple → 复用,不重签(收敛)。
//  3. 否则领单调 gen + 生成 jti + 签发 pending 凭据(hubCredIssuer),StagePending 暂存(WATCH CAS)。
//  4. 用 JSON Patch 同时 test GameServer uid+resourceVersion 后投递 annotation bundle。
//  5. 无论 PATCH 返回 2xx/409/超时/坏 body,都 GET 严格确认对象终态 + Redis 当前 pending,
//     再用 expected tuple CAS MarkDelivered。没有任何本地 gen/exp/rv fallback。
//
// annotation 只负责把 token 送到 DS 手上;是否「授权生效」完全由 Redis 授权记录决定——DS 拿 pending
// token 发第一个合法心跳时,RedisHubAuthRepo.ActivateHeartbeat 才把 pending 与 shard 原子激活。
// 返回 (pending/active 令牌 exp_ms, gen, err);失败时 enforce 下该 Hub 不进可用镜像(fail-closed)。
func (a *AgonesHubFleetProvider) ensureHubCredential(ctx context.Context, gs *gameServer) (int64, uint64, error) {
	pod := gs.Metadata.Name
	uid := gs.Metadata.UID
	if uid == "" {
		// GameServer 尚未被 apiserver 赋 uid(极早期);本轮跳过,下轮对账再投递。
		return 0, 0, fmt.Errorf("hub_credential: gameserver %s has empty uid", pod)
	}
	// 1. 绑定实例身份(换 DS 实例 → 复位 epoch++、清 active/pending)。
	rec, err := a.authRepo.InitAuth(ctx, pod, uid, a.authTTL)
	if err != nil {
		plog.With(ctx).Warnw("msg", "hub_credential_init_failed", "pod", pod, "uid", uid, "err", err)
		return 0, 0, err
	}
	if rec.Phase != hubv1.HubAuthPhase_HUB_AUTH_PHASE_BOOTSTRAP &&
		rec.Phase != hubv1.HubAuthPhase_HUB_AUTH_PHASE_ACTIVE &&
		rec.Phase != hubv1.HubAuthPhase_HUB_AUTH_PHASE_ROTATING {
		return 0, 0, fmt.Errorf("hub credential auth phase does not allow delivery: %s", rec.Phase.String())
	}
	// 2a. 当前 annotation 严格等于 Redis active:已激活凭据仍可复用。
	if rec.Active != nil && a.credentialBundleMatches(gs, rec.Active, true) == nil {
		return int64(rec.Active.ExpMs), rec.Active.Gen, nil
	}
	// 2b. 当前 annotation 严格等于 Redis pending:必须再 GET 当前对象终态,并以 expected
	// tuple CAS 记 delivered。LIST 快照/annotation gen 数字本身没有推进授权的能力。
	if rec.Pending != nil && a.credentialBundleMatches(gs, rec.Pending, true) == nil {
		// plog.Detach(§16.7):detached 确认读只复制日志字段,不携带请求级 transport/取消。
		rv, cerr := a.confirmCredentialDelivery(plog.Detach(ctx), pod, rec.Pending)
		if cerr != nil {
			return 0, 0, cerr
		}
		if derr := a.authRepo.MarkDelivered(ctx, pod, rec.Pending, rv, a.authTTL); derr != nil {
			return 0, 0, derr
		}
		return int64(rec.Pending.ExpMs), rec.Pending.Gen, nil
	}
	// 3. 签发新 pending 凭据并暂存(gen 单调 + jti 唯一,绑 uid+epoch)。
	token, cred, err := a.hubCredIssuer(pod, uid, rec.ProtocolEpoch)
	if err != nil {
		plog.With(ctx).Warnw("msg", "hub_credential_sign_failed", "pod", pod, "err", err)
		return 0, 0, err
	}
	// 签发器的 token 与返回 credential 必须在写 Redis 前就自洽。否则拒绝 stage,
	// 防“凭据记录是一套、JWT 实际 claims 是另一套”的永久分裂。
	if verr := a.verifyTokenAgainstCredential(token, pod, cred, false); verr != nil {
		plog.With(ctx).Warnw("msg", "hub_credential_issuer_mismatch", "pod", pod, "err", verr)
		return 0, 0, verr
	}
	if _, serr := a.authRepo.StagePending(ctx, pod, cred, a.authTTL); serr != nil {
		plog.With(ctx).Warnw("msg", "hub_credential_stage_failed", "pod", pod, "gen", cred.GetGen(), "err", serr,
			"hint", "StagePending CAS 失败(uid/epoch/gen 竞态):本轮跳过,下轮对账重试")
		return 0, 0, serr
	}
	// 4-5. uid+rv 条件投递 + GET 严格确认最终对象与 Redis 当前 pending。
	rv, err := a.patchCredentialAnnotation(ctx, gs, token, cred)
	if err != nil {
		return 0, 0, err
	}
	// MarkDelivered 不是 best-effort:它必须以同一 expected tuple 再做一次 Redis CAS。
	// PATCH 后若 pending 已被更高代际替换,旧响应只会失败,绝不污染新 pending。
	if derr := a.authRepo.MarkDelivered(ctx, pod, cred, rv, a.authTTL); derr != nil {
		plog.With(ctx).Warnw("msg", "hub_credential_mark_delivered_failed", "pod", pod, "err", derr)
		return 0, 0, derr
	}
	plog.With(ctx).Infow("msg", "hub_credential_staged", "pod", pod, "uid", uid,
		"epoch", rec.ProtocolEpoch, "gen", cred.GetGen(), "exp_ms", cred.GetExpMs())
	return int64(cred.GetExpMs()), cred.GetGen(), nil
}

// jsonPatchOperation 是 RFC 6902 JSON Patch 操作。Model B 用 test 同时绑定不可变 uid 与
// 当前 resourceVersion,避免同名 GameServer 重建后把旧实例 token 投递给新实例。
type jsonPatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// patchCredentialAnnotation 严格投递 Model B annotation bundle。PATCH 只发一次,不靠重试
// 猜结果；无论 transport error、409、非 2xx、2xx 空/坏/缺字段或正常 2xx,都随后 GET 当前
// GameServer 并验证 JWT claims + annotation mirror + Redis 当前 pending 完全一致。
func (a *AgonesHubFleetProvider) patchCredentialAnnotation(ctx context.Context, gs *gameServer, token string, expected *hubv1.HubDSCredential) (string, error) {
	if gs == nil || gs.Metadata.Name == "" || gs.Metadata.UID == "" || gs.Metadata.ResourceVersion == "" {
		return "", fmt.Errorf("hub credential patch requires gameserver name/uid/resourceVersion")
	}
	if expected == nil || gs.Metadata.UID != expected.InstanceUid {
		return "", fmt.Errorf("hub credential patch instance uid mismatch")
	}
	if verr := a.verifyTokenAgainstCredential(token, gs.Metadata.Name, expected, false); verr != nil {
		return "", verr
	}

	annotations := hubCredentialAnnotations(token, expected)
	ops := []jsonPatchOperation{
		{Op: "test", Path: "/metadata/uid", Value: gs.Metadata.UID},
		{Op: "test", Path: "/metadata/resourceVersion", Value: gs.Metadata.ResourceVersion},
	}
	if gs.Metadata.Annotations == nil {
		// annotations 整体缺失时一次创建;RV test 保证不会覆盖并发新增的 annotations。
		ops = append(ops, jsonPatchOperation{Op: "add", Path: "/metadata/annotations", Value: annotations})
	} else {
		for _, key := range dsCredentialAnnotationKeys {
			ops = append(ops, jsonPatchOperation{
				Op: "add", Path: "/metadata/annotations/" + escapeJSONPointer(key), Value: annotations[key],
			})
		}
	}
	patch, err := json.Marshal(ops)
	if err != nil {
		return "", err
	}
	gsURL := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers/%s",
		a.apiServer, a.namespace, gs.Metadata.Name)
	_, status, patchErr := a.do(ctx, http.MethodPatch, gsURL, patch, "application/json-patch+json")

	// PATCH 的任何返回都不是授权事实。即使调用方 ctx 已取消,也给安全确认一次独立、有界的
	// GET 机会(内部 getGameServer 仍受 listTimeout 限制),避免“timeout 但实际已应用”被误判。
	// plog.Detach(§16.7):detached 确认读只复制日志字段,不携带请求级 transport/取消。
	rv, confirmErr := a.confirmCredentialDelivery(plog.Detach(ctx), gs.Metadata.Name, expected)
	if confirmErr == nil && rv != gs.Metadata.ResourceVersion {
		return rv, nil
	}
	if confirmErr == nil {
		confirmErr = fmt.Errorf("gameserver resourceVersion did not advance")
	}
	if patchErr != nil {
		return "", fmt.Errorf("hub credential patch outcome unconfirmed after transport error: %v; get confirm: %w", patchErr, confirmErr)
	}
	return "", fmt.Errorf("hub credential patch outcome unconfirmed (http %d): %w", status, confirmErr)
}

func hubCredentialAnnotations(token string, expected *hubv1.HubDSCredential) map[string]string {
	if expected == nil {
		return nil
	}
	return map[string]string{
		dsTokenAnnotationKey:         token,
		dsTokenExpAnnotationKey:      strconv.FormatUint(expected.ExpMs, 10),
		dsTokenGenAnnotationKey:      strconv.FormatUint(expected.Gen, 10),
		dsTokenJTIAnnotationKey:      expected.Jti,
		dsInstanceUIDAnnotationKey:   expected.InstanceUid,
		dsInstanceEpochAnnotationKey: strconv.FormatUint(uint64(expected.ProtocolEpoch), 10),
		dsWriterEpochAnnotationKey:   strconv.FormatUint(uint64(expected.WriterEpoch), 10),
		dsTokenKidAnnotationKey:      expected.Kid,
		dsTokenHashAnnotationKey:     expected.TokenSha256,
	}
}

func escapeJSONPointer(s string) string {
	s = strings.ReplaceAll(s, "~", "~0")
	return strings.ReplaceAll(s, "/", "~1")
}

// confirmCredentialDelivery GET 当前 GameServer,严格确认对象 uid/RV、annotation bundle、JWT
// claims 与 Redis **当前 pending** 都等于 expected。任何一项缺失/损坏/漂移都失败关闭。
func (a *AgonesHubFleetProvider) confirmCredentialDelivery(ctx context.Context, pod string, expected *hubv1.HubDSCredential) (string, error) {
	confirmCtx, cancel := context.WithTimeout(ctx, a.listTimeout)
	defer cancel()
	current, err := a.getGameServer(confirmCtx, pod)
	if err != nil {
		return "", err
	}
	if current.Metadata.Name != pod || current.Metadata.ResourceVersion == "" {
		return "", fmt.Errorf("hub credential confirm missing gameserver identity/resourceVersion")
	}
	if err := a.credentialBundleMatches(current, expected, false); err != nil {
		return "", fmt.Errorf("hub credential confirm bundle mismatch: %w", err)
	}
	rec, found, err := a.authRepo.GetAuth(confirmCtx, pod)
	if err != nil {
		return "", fmt.Errorf("hub credential confirm redis auth: %w", err)
	}
	if !found || rec == nil || rec.InstanceUid != expected.InstanceUid ||
		rec.ProtocolEpoch != expected.ProtocolEpoch || !credentialFieldsEqual(rec.Pending, expected) {
		return "", fmt.Errorf("hub credential confirm pending changed")
	}
	if rec.Phase == hubv1.HubAuthPhase_HUB_AUTH_PHASE_QUARANTINED ||
		rec.Phase == hubv1.HubAuthPhase_HUB_AUTH_PHASE_TERMINATING {
		return "", fmt.Errorf("hub credential confirm auth phase locked")
	}
	return current.Metadata.ResourceVersion, nil
}

// credentialBundleMatches 只把 annotation 当作 expected 的投递镜像验证,绝不让 annotation 数字
// 自己选择/推进 Redis 凭据。token 必须验签,且 claims、hash、外置 gen/exp 均精确等于 expected。
func (a *AgonesHubFleetProvider) credentialBundleMatches(gs *gameServer, expected *hubv1.HubDSCredential, requireFresh bool) error {
	if gs == nil || expected == nil || gs.Metadata.Name == "" || gs.Metadata.UID == "" || gs.Metadata.ResourceVersion == "" {
		return fmt.Errorf("gameserver or expected credential incomplete")
	}
	if gs.Metadata.UID != expected.InstanceUid {
		return fmt.Errorf("gameserver uid mismatch")
	}
	annotations := gs.Metadata.Annotations
	token := annotations[dsTokenAnnotationKey]
	if token == "" {
		return fmt.Errorf("token annotation missing")
	}
	gen, err := strconv.ParseUint(annotations[dsTokenGenAnnotationKey], 10, 64)
	if err != nil || gen == 0 || gen != expected.Gen {
		return fmt.Errorf("token gen annotation mismatch")
	}
	expMs, err := strconv.ParseUint(annotations[dsTokenExpAnnotationKey], 10, 64)
	if err != nil || expMs == 0 || expMs != expected.ExpMs {
		return fmt.Errorf("token exp annotation mismatch")
	}
	if annotations[dsTokenJTIAnnotationKey] == "" || annotations[dsTokenJTIAnnotationKey] != expected.Jti {
		return fmt.Errorf("token jti annotation mismatch")
	}
	if annotations[dsInstanceUIDAnnotationKey] == "" || annotations[dsInstanceUIDAnnotationKey] != expected.InstanceUid {
		return fmt.Errorf("instance uid annotation mismatch")
	}
	epoch, err := strconv.ParseUint(annotations[dsInstanceEpochAnnotationKey], 10, 32)
	if err != nil || epoch == 0 || uint32(epoch) != expected.ProtocolEpoch {
		return fmt.Errorf("instance epoch annotation mismatch")
	}
	writer, err := strconv.ParseUint(annotations[dsWriterEpochAnnotationKey], 10, 32)
	if err != nil || writer == 0 || uint32(writer) != expected.WriterEpoch {
		return fmt.Errorf("writer epoch annotation mismatch")
	}
	if annotations[dsTokenKidAnnotationKey] == "" || annotations[dsTokenKidAnnotationKey] != expected.Kid {
		return fmt.Errorf("token kid annotation mismatch")
	}
	if annotations[dsTokenHashAnnotationKey] == "" || annotations[dsTokenHashAnnotationKey] != expected.TokenSha256 {
		return fmt.Errorf("token hash annotation mismatch")
	}
	return a.verifyTokenAgainstCredential(token, gs.Metadata.Name, expected, requireFresh)
}

func (a *AgonesHubFleetProvider) verifyTokenAgainstCredential(token, pod string, expected *hubv1.HubDSCredential, requireFresh bool) error {
	if expected == nil || expected.Gen == 0 || expected.Jti == "" || expected.ExpMs == 0 ||
		expected.Kid == "" || expected.InstanceUid == "" || expected.ProtocolEpoch == 0 || expected.TokenSha256 == "" ||
		expected.WriterEpoch == 0 {
		return fmt.Errorf("expected hub credential incomplete")
	}
	nowMs := time.Now().UnixMilli()
	if nowMs < 0 || expected.ExpMs <= uint64(nowMs) {
		return fmt.Errorf("expected hub credential expired")
	}
	if requireFresh && a.dsTokenRenewBefore > 0 {
		remainingMs := expected.ExpMs - uint64(nowMs)
		if remainingMs <= uint64(a.dsTokenRenewBefore/time.Millisecond) {
			return fmt.Errorf("expected hub credential within renew window")
		}
	}
	sum := sha256.Sum256([]byte(token))
	if hex.EncodeToString(sum[:]) != expected.TokenSha256 {
		return fmt.Errorf("hub credential token hash mismatch")
	}
	claims, err := a.hubCredVerifier(token)
	if err != nil {
		return fmt.Errorf("hub credential jwt verify: %w", err)
	}
	if claims == nil || claims.Pod != pod || claims.InstanceUID != expected.InstanceUid ||
		claims.ProtocolEpoch != expected.ProtocolEpoch || claims.Gen != expected.Gen ||
		claims.JTI != expected.Jti || claims.ExpMs != expected.ExpMs || claims.Kid != expected.Kid ||
		claims.WriterEpoch != expected.WriterEpoch {
		return fmt.Errorf("hub credential jwt tuple mismatch")
	}
	return nil
}

func credentialFieldsEqual(a, b *hubv1.HubDSCredential) bool {
	return a != nil && b != nil && a.Gen == b.Gen && a.Jti == b.Jti && a.ExpMs == b.ExpMs &&
		a.Kid == b.Kid && a.InstanceUid == b.InstanceUid && a.ProtocolEpoch == b.ProtocolEpoch &&
		a.TokenSha256 == b.TokenSha256 && a.WriterEpoch == b.WriterEpoch
}

// ensureDSToken 保证 ready 的 Hub DS GameServer 持有未过期的 DS 回调令牌 annotation。
//
// 并发安全(审核 P1 #9-#12):重签 PATCH 携带 metadata.resourceVersion 做**乐观并发 CAS**。
// 多副本交错 patch 同一 GameServer 时,基于旧 rv 的写被 apiserver 以 409 Conflict 拒,冲突方
// **重读对象再判定**——对方可能已写好当前代际令牌则直接复用(不再 INCR),避免「后到低代际 PATCH
// 覆盖高代际」导致 K8s 最终 gen 与 Redis CurrentTokenGen 分裂。返回非 nil error 时代表签发/patch/
// CAS 耗尽:off/permissive 下调用方忽略(只告警),enforce 下调用方据此跳过该 Hub(fail-closed)。
//
// 补偿收敛(审核 P1 #12):PATCH 成功但随后 Redis 写失败时,annotation 已持久持有新代际 →
// 下轮对账 tokenStillValid 命中直接复用该代际 → UpdateShardWithLock 重试写 Redis;且代际推进会
// 复位分片 warming,Redis 追平前分片保持不可分配(fail-closed),不会把玩家路由到代际未落地的 Hub。
func (a *AgonesHubFleetProvider) ensureDSToken(ctx context.Context, gs *gameServer) (int64, uint64, error) {
	if a.dsTokenIssuer == nil {
		return 0, 0, nil
	}
	const maxCAS = 3
	cur := gs
	gsURL := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers/%s",
		a.apiServer, a.namespace, gs.Metadata.Name)
	for attempt := 0; attempt < maxCAS; attempt++ {
		// 现存令牌仍可复用(exp 未近 + 验签过 + enforce 下有合法 gen)→ 不重签。
		if expMs, gen, ok := a.tokenStillValid(ctx, cur); ok {
			return expMs, gen, nil
		}
		// 需重签:领取新单调代际并签发。
		tok, expMs, gen, err := a.dsTokenIssuer(cur.Metadata.Name)
		if err != nil {
			plog.With(ctx).Warnw("msg", "hub_ds_token_sign_failed", "pod", cur.Metadata.Name, "err", err)
			return 0, 0, err
		}
		patch, err := json.Marshal(map[string]any{
			"metadata": map[string]any{
				"resourceVersion": cur.Metadata.ResourceVersion, // 乐观并发前置:rv 不匹配 → 409
				"annotations": map[string]string{
					dsTokenAnnotationKey:    tok,
					dsTokenExpAnnotationKey: strconv.FormatInt(expMs, 10),
					dsTokenGenAnnotationKey: strconv.FormatUint(gen, 10),
				},
			},
		})
		if err != nil {
			plog.With(ctx).Warnw("msg", "hub_ds_token_patch_marshal_failed", "pod", cur.Metadata.Name, "err", err)
			return 0, 0, err
		}
		respBytes, status, err := a.do(ctx, http.MethodPatch, gsURL, patch, "application/merge-patch+json")
		if err != nil {
			plog.With(ctx).Warnw("msg", "hub_ds_token_patch_failed", "pod", cur.Metadata.Name, "err", err)
			return 0, 0, err
		}
		if status == http.StatusConflict {
			// CAS 冲突:另一副本已改该 GameServer。重读拿最新 rv/annotation 再重试
			// (下轮 tokenStillValid 可能直接命中对方写的当前代际令牌,天然收敛不重复发号)。
			refreshed, gerr := a.getGameServer(ctx, cur.Metadata.Name)
			if gerr != nil {
				plog.With(ctx).Warnw("msg", "hub_ds_token_conflict_reget_failed", "pod", cur.Metadata.Name, "err", gerr)
				return 0, 0, gerr
			}
			plog.With(ctx).Debugw("msg", "hub_ds_token_patch_conflict_retry", "pod", cur.Metadata.Name, "attempt", attempt+1)
			cur = refreshed
			continue
		}
		if status < 200 || status >= 300 {
			plog.With(ctx).Warnw("msg", "hub_ds_token_patch_failed",
				"pod", cur.Metadata.Name, "http_status", status, "body", truncateBody(respBytes, 256),
				"hint", "检查 RBAC 是否对 gameservers 资源授予 patch 动词(deploy/k8s/agones/10-rbac-allocator.yaml)")
			return 0, 0, fmt.Errorf("hub_ds_token patch %s http %d", cur.Metadata.Name, status)
		}
		// read-after-write(审核 P1 #11):以服务器返回的最终对象 annotation 为准,不盲信本地 gen。
		effExp, effGen := expMs, gen
		var updated gameServer
		if json.Unmarshal(respBytes, &updated) == nil {
			if g := genFromAnnotations(&updated); g != 0 {
				effGen = g
			}
			if e := expFromAnnotations(&updated); e != 0 {
				effExp = e
			}
		}
		plog.With(ctx).Infow("msg", "hub_ds_token_issued", "pod", cur.Metadata.Name, "exp_ms", effExp, "gen", effGen)
		return effExp, effGen, nil
	}
	plog.With(ctx).Warnw("msg", "hub_ds_token_cas_exhausted", "pod", gs.Metadata.Name,
		"hint", "resourceVersion CAS 连续冲突耗尽:本轮不把该 Hub 计入可用(enforce 下 fail-closed),下轮对账重试")
	return 0, 0, fmt.Errorf("hub_ds_token CAS retries exhausted for %s", gs.Metadata.Name)
}

// tokenStillValid 判定 GameServer 现存 annotation 令牌是否仍可复用(无需重签)。需同时满足:
// ①有非空 token ②外置 exp 未近 ③(启用验签时)实测验签通过 ④enforce 下有合法代际(gen!=0)。
// 满足返回 (expMs, gen, true);任一不满足返回 false 触发重签 —— 挡空/损坏/旧密钥令牌及 legacy gen0。
func (a *AgonesHubFleetProvider) tokenStillValid(ctx context.Context, gs *gameServer) (int64, uint64, bool) {
	tok, hasTok := gs.Metadata.Annotations[dsTokenAnnotationKey]
	if !hasTok || tok == "" {
		return 0, 0, false
	}
	expStr, ok := gs.Metadata.Annotations[dsTokenExpAnnotationKey]
	if !ok {
		return 0, 0, false
	}
	expMs, err := strconv.ParseInt(expStr, 10, 64)
	if err != nil || time.Until(time.UnixMilli(expMs)) <= a.dsTokenRenewBefore {
		return 0, 0, false
	}
	annGen := genFromAnnotations(gs)
	if a.dsTokenRequired && annGen == 0 {
		// enforce 下缺合法代际(缺失/非法/legacy gen0):强制重签补齐,否则该分片以 gen=0 进对账 →
		// 心跳侧 genRequired 一律判 stale → 永不可分配;更挡「legacy gen0 被当有效而关闭代际门」(P1 #1/#2)。
		plog.With(ctx).Warnw("msg", "hub_ds_token_missing_gen_resign", "pod", gs.Metadata.Name,
			"hint", "enforce 下 annotation 无合法 ds-token-gen,强制重签补齐单调代际")
		return 0, 0, false
	}
	if a.dsTokenVerify != nil {
		if verr := a.dsTokenVerify(tok, gs.Metadata.Name); verr != nil {
			plog.With(ctx).Warnw("msg", "hub_ds_token_verify_failed_resign", "pod", gs.Metadata.Name,
				"hint", "annotation 令牌验签不过(空/损坏/密钥轮换),触发重签")
			return 0, 0, false
		}
	}
	return expMs, annGen, true
}

// getGameServer 重读单个 GameServer(CAS 冲突后拿最新 resourceVersion + annotation)。
func (a *AgonesHubFleetProvider) getGameServer(ctx context.Context, name string) (*gameServer, error) {
	gsURL := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers/%s",
		a.apiServer, a.namespace, name)
	respBytes, status, err := a.do(ctx, http.MethodGet, gsURL, nil, "")
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("agones: get gameserver %s http %d: %s", name, status, truncateBody(respBytes, 256))
	}
	var gs gameServer
	if err := json.Unmarshal(respBytes, &gs); err != nil {
		return nil, fmt.Errorf("agones: decode gameserver %s: %w", name, err)
	}
	return &gs, nil
}

// expFromAnnotations 从 GameServer 读回令牌 exp(pandora.dev/ds-token-exp-ms);缺失/非法为 0。
func expFromAnnotations(gs *gameServer) int64 {
	if s, ok := gs.Metadata.Annotations[dsTokenExpAnnotationKey]; ok {
		if e, err := strconv.ParseInt(s, 10, 64); err == nil {
			return e
		}
	}
	return 0
}

// genFromAnnotations 从 GameServer 读回当前令牌代际(pandora.dev/ds-token-gen);缺失/非法为 0。
// 保留旧令牌(未重签)时用它把 annotation 镜像的代际透传给拓扑对账,避免代际被误清 0。
func genFromAnnotations(gs *gameServer) uint64 {
	if s, ok := gs.Metadata.Annotations[dsTokenGenAnnotationKey]; ok {
		if g, err := strconv.ParseUint(s, 10, 64); err == nil {
			return g
		}
	}
	return 0
}

// GetFleetReplicas 读取 Fleet 当前 spec.replicas。
func (a *AgonesHubFleetProvider) GetFleetReplicas(ctx context.Context) (int32, error) {
	fleetURL := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/fleets/%s",
		a.apiServer, a.namespace, a.fleetName)

	respBytes, status, err := a.do(ctx, http.MethodGet, fleetURL, nil, "")
	if err != nil {
		return 0, fmt.Errorf("agones: get fleet %s: %w", a.fleetName, err)
	}
	if status < 200 || status >= 300 {
		return 0, fmt.Errorf("agones: get fleet %s http %d: %s",
			a.fleetName, status, truncateBody(respBytes, 256))
	}

	var fleet fleetResponse
	if err := json.Unmarshal(respBytes, &fleet); err != nil {
		return 0, fmt.Errorf("agones: decode fleet %s: %w", a.fleetName, err)
	}
	return fleet.Spec.Replicas, nil
}

// SetFleetReplicas PATCH Fleet spec.replicas。
func (a *AgonesHubFleetProvider) SetFleetReplicas(ctx context.Context, replicas int32) error {
	if replicas < 0 {
		replicas = 0
	}
	fleetURL := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/fleets/%s",
		a.apiServer, a.namespace, a.fleetName)
	patchBody := []byte(fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas))

	respBytes, status, err := a.do(ctx, http.MethodPatch, fleetURL, patchBody, "application/merge-patch+json")
	if err != nil {
		return fmt.Errorf("agones: patch fleet replicas=%d: %w", replicas, err)
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("agones: patch fleet replicas=%d http %d: %s",
			replicas, status, truncateBody(respBytes, 256))
	}
	return nil
}

// do 发一次带鉴权的 REST 请求,返回 (body, statusCode, transportErr)。
func (a *AgonesHubFleetProvider) do(ctx context.Context, method, reqURL string, body []byte, contentType string) ([]byte, int, error) {
	reqCtx, cancel := context.WithTimeout(ctx, a.listTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, method, reqURL, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Accept", "application/json")
	if len(body) > 0 && contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	// 每次请求重读 token(容忍 in-cluster 投影 token 轮转);"-" 或空 → 不带。
	if a.tokenPath != "" && a.tokenPath != "-" {
		if tok, terr := os.ReadFile(a.tokenPath); terr == nil {
			req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
		}
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return respBytes, resp.StatusCode, nil
}

// shardIDFor 取 GameServer 的稳定 shard_id:优先读 pandora.dev/shard-id 标签,
// 缺省/非法则按 pod 名 FNV-1a 哈希派生非零 uint32(同 pod 名稳定,仅作并列 tiebreak/展示)。
func shardIDFor(gs *gameServer) uint32 {
	if v, ok := gs.Metadata.Labels[shardIDLabelKey]; ok {
		if n, err := strconv.ParseUint(v, 10, 32); err == nil && n != 0 {
			return uint32(n)
		}
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(gs.Metadata.Name))
	id := h.Sum32()
	if id == 0 {
		id = 1
	}
	return id
}

// capacityFor 取 GameServer 的容量:优先读 pandora.dev/capacity 标签,缺省/非法用 fallback。
func capacityFor(gs *gameServer, fallback int32) int32 {
	if v, ok := gs.Metadata.Labels[capacityLabelKey]; ok {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil && n > 0 {
			return int32(n)
		}
	}
	return fallback
}

// truncateBody 截断 body 给错误信息用,避免日志过长。
func truncateBody(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
