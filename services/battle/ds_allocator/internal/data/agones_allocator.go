// agones_allocator.go — 真 Agones GameServerAllocator(W4 ⑫)。
//
// 用 k8s apiserver REST 直连 allocation.agones.dev/v1 GameServerAllocation,
// 不引入 agones / client-go 重依赖(只用标准库 net/http + crypto/tls + encoding/json),
// 保持 ds_allocator go.mod 干净、本地可编译可单测。Agones 的分配 API 与 k8s provider
// 无关(ACK / 自建 / minikube 上的 Agones controller 一致),故本实现 provider-agnostic。
//
// 职责切分(对齐 biz.GameServerAllocator 接口,biz 零改):
//   - Allocate:POST GameServerAllocation,从 status 取 gameServerName + address:port。
//     status.state != "Allocated"(无空闲 GameServer)→ ErrDSNoAvailable。
//   - Release:DELETE 该 GameServer,Fleet 自动补一个新的;404 视作已释放(幂等)。
//
// 鉴权:in-cluster 读 ServiceAccount token(每次请求重读,容忍 token 轮转)+ CA;
// 集群外联调可显式配 api_server + token_path(或经 kubectl proxy 不带 token)。
package data

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/luyuancpp/pandora/pkg/dsmetadata"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/releasetrack"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
)

// fleetLabelKey 是 Agones Fleet 给其 GameServer 打的标签 key(selector 用)。
const (
	fleetLabelKey                     = "agones.dev/fleet"
	releaseTrackMetadataKey           = "pandora.dev/release-track"
	battleRosterAnnotationKey         = "pandora.dev/roster"
	battleCombatFactionsAnnotationKey = "pandora.dev/combat-factions"
	battleAllocationMetadataKey       = "pandora.dev/allocation-id"
)

type battleFleetRoute struct {
	stable string
	canary string
}

// AgonesGameServerAllocator 经 k8s REST 调 Agones GameServerAllocation。
type AgonesGameServerAllocator struct {
	apiServer       string // 已去尾部 /
	namespace       string
	fleetName       string
	canaryFleetName string
	mapFleets       map[uint32]battleFleetRoute // map_id → stable/canary 专属预热 Fleet
	advertiseHost   string
	tokenPath       string // "" 或 "-" → 不带 Authorization
	allocateTimeout time.Duration
	httpClient      *http.Client

	// dsTokenIssuer 签发 DS 回调服务令牌(审核 P1 #1;main 在 ds_auth.secret 已配时注入)。
	// 非 nil 时 Allocate 把令牌写进 GameServerAllocation 的 metadata.annotations
	// pandora.dev/ds-token,DS 经 Agones SDK GameServer() 读到后回调时带 Bearer 头。
	dsTokenIssuer func(matchID uint64) (token string, err error)
	// dsTokenRequired 为 guard=enforce 时 true:签发失败必须 fail-closed(不分配无令牌的 DS,
	// 否则该 DS 回调会被 enforce 守卫全拒,等于开了个连不回来的对局)。off/permissive 下 false,
	// 签发失败降级为无令牌分配以保对局可开。
	dsTokenRequired bool
}

// dsTokenAnnotationKey 是下发 DS 回调令牌的 GameServer annotation key。
// label 有 63 字符 / 字符集限制放不下 JWT,annotation 无此限制。
const dsTokenAnnotationKey = "pandora.dev/ds-token"

// SetDSTokenIssuer 注入 DS 回调令牌签发器(可选依赖,main 在 ds_auth.secret 已配时调用)。
// required=true(guard=enforce)时签发失败会让 Allocate 返回错误(fail-closed)。
func (a *AgonesGameServerAllocator) SetDSTokenIssuer(f func(matchID uint64) (string, error), required bool) {
	a.dsTokenIssuer = f
	a.dsTokenRequired = required
}

// NewAgonesGameServerAllocator 构造真 Agones 分配器。
//
// 失败场景(返 error,main 据此 fatal 或回退):
//   - Enabled 但 FleetName 空(无法选择 GameServer)
//   - CA 文件配置了却读不出 / 解析失败
func NewAgonesGameServerAllocator(cfg conf.AgonesConf) (*AgonesGameServerAllocator, error) {
	if cfg.FleetName == "" {
		return nil, fmt.Errorf("agones: fleet_name required when enabled")
	}
	if cfg.CanaryPercent > 100 {
		return nil, fmt.Errorf("agones: canary_percent=%d out of range [0,100]", cfg.CanaryPercent)
	}
	if cfg.CanaryPercent > 0 && (cfg.CanaryFleetName == "" || cfg.CanarySeed == "") {
		return nil, fmt.Errorf("agones: canary_percent>0 requires canary_fleet_name and canary_seed")
	}

	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipTLSVerify, //nolint:gosec // 仅 dev 显式开启
	}
	if !cfg.InsecureSkipTLSVerify && cfg.CAPath != "" {
		// CA 文件存在才加载;in-cluster 默认路径在集群外不存在 → 跳过用系统根证书池。
		if pem, err := os.ReadFile(cfg.CAPath); err == nil {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("agones: parse CA %s failed", cfg.CAPath)
			}
			tlsCfg.RootCAs = pool
		}
	}

	timeout := cfg.AllocateTimeout.Std()
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	mapFleets := make(map[uint32]battleFleetRoute, len(cfg.MapFleets))
	for _, mf := range cfg.MapFleets {
		if mf.MapID > 0 && (mf.FleetName != "" || mf.CanaryFleetName != "") {
			mapFleets[mf.MapID] = battleFleetRoute{stable: mf.FleetName, canary: mf.CanaryFleetName}
		}
	}

	return &AgonesGameServerAllocator{
		apiServer:       strings.TrimRight(cfg.APIServer, "/"),
		namespace:       cfg.Namespace,
		fleetName:       cfg.FleetName,
		canaryFleetName: cfg.CanaryFleetName,
		mapFleets:       mapFleets,
		advertiseHost:   strings.TrimSpace(cfg.AdvertiseHost),
		tokenPath:       cfg.TokenPath,
		allocateTimeout: timeout,
		httpClient: &http.Client{
			Timeout:   timeout,
			Transport: &http.Transport{TLSClientConfig: tlsCfg},
		},
	}, nil
}

// ── GameServerAllocation 请求 / 响应 JSON(只声明用到的字段)──────────────────

type gsaSelector struct {
	MatchLabels map[string]string `json:"matchLabels,omitempty"`
}

type gsaMetadata struct {
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

type gsaSpec struct {
	Selectors []gsaSelector `json:"selectors,omitempty"`
	Metadata  *gsaMetadata  `json:"metadata,omitempty"`
}

type gsaRequest struct {
	APIVersion string  `json:"apiVersion"`
	Kind       string  `json:"kind"`
	Spec       gsaSpec `json:"spec"`
}

type gsaPort struct {
	Name string `json:"name"`
	Port int32  `json:"port"`
}

type gsaStatus struct {
	State          string    `json:"state"`
	GameServerName string    `json:"gameServerName"`
	Address        string    `json:"address"`
	Ports          []gsaPort `json:"ports"`
}

type gsaResponse struct {
	Status gsaStatus `json:"status"`
}

// AuthoritativeGameServerAllocation 是 Model B 分配结果。UID/resourceVersion 只来自选中后
// 的严格 GameServer GET；AnnotationsPresent 决定 JSON Patch 应新增整个 map 还是单独成员。
type AuthoritativeGameServerAllocation struct {
	PodName            string
	Addr               string
	InstanceUID        string
	PodUID             string
	InstanceEpoch      uint32
	ResourceVersion    string
	AllocationID       string
	ReleaseTrack       string
	AnnotationsPresent bool
}

type gameServerResponse struct {
	Metadata struct {
		Name            string            `json:"name"`
		UID             string            `json:"uid"`
		ResourceVersion string            `json:"resourceVersion"`
		Labels          map[string]string `json:"labels"`
		Annotations     map[string]string `json:"annotations"`
		// DeletionTimestamp 非空 = 删除已受理、对象处于终止宽限(graceful termination);
		// 它不是物理消失证明,但足以证明无需再发 DELETE。
		DeletionTimestamp string `json:"deletionTimestamp"`
	} `json:"metadata"`
	Status struct {
		State string `json:"state"`
	} `json:"status"`
}

// agonesStateUnhealthy 是 Agones 控制器依据 SDK health ping 断流写下的判死状态;
// 进入该状态的 GameServer 不再参与分配,随后被 GameServerSet 删除替换。
const agonesStateUnhealthy = "Unhealthy"

type gameServerListResponse struct {
	Items []gameServerResponse `json:"items"`
}

type k8sOwnerReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Controller bool   `json:"controller"`
}

type podResponse struct {
	Metadata struct {
		Name            string              `json:"name"`
		UID             string              `json:"uid"`
		ResourceVersion string              `json:"resourceVersion"`
		OwnerReferences []k8sOwnerReference `json:"ownerReferences"`
	} `json:"metadata"`
}

type jsonPatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

type deleteOptions struct {
	APIVersion    string               `json:"apiVersion"`
	Kind          string               `json:"kind"`
	Preconditions *deletePreconditions `json:"preconditions,omitempty"`
}

type deletePreconditions struct {
	UID string `json:"uid"`
}

// Allocate POST 一个 GameServerAllocation,返回 (gameServerName, address:port)。
//
// selectors 有序(Agones 按顺序尝试,选中首个有空闲 GameServer 的):
//  1. 若 mapID 配了专属预热 Fleet(map_fleets)→ 首选它(Pod 已预加载目标图,分配即可玩);
//  2. 通用 Fleet(Loader 模式,分配后按 map-id label travel)作兜底。
func (a *AgonesGameServerAllocator) Allocate(ctx context.Context, matchID uint64, mapID uint32, gameMode, releaseTrack string) (string, string, string, error) {
	if !releasetrack.Valid(releaseTrack) {
		return "", "", "", errcode.New(errcode.ErrInvalidArg, "agones: invalid release_track %q", releaseTrack)
	}
	meta := &gsaMetadata{Labels: map[string]string{
		"pandora.dev/match-id":  fmt.Sprintf("%d", matchID),
		"pandora.dev/map-id":    fmt.Sprintf("%d", mapID),
		"pandora.dev/game-mode": sanitizeLabelValue(gameMode),
	}}
	// DS 回调服务令牌经 annotation 下发(DS 拿不到签名密钥,只持有短期令牌)。
	// enforce(dsTokenRequired=true):签发失败 fail-closed,返回分配失败,不产生连不回来的对局;
	// off/permissive:签发失败降级为无令牌分配,先保对局可开。
	if a.dsTokenIssuer != nil {
		if tok, terr := a.dsTokenIssuer(matchID); terr != nil {
			if a.dsTokenRequired {
				plog.With(ctx).Errorw("msg", "ds_callback_token_sign_failed", "match_id", matchID, "err", terr,
					"mode", "enforce", "hint", "ds_auth.mode=enforce 下签发失败即 fail-closed;检查 ds_auth.secret / 签名配置")
				return "", "", "", errcode.New(errcode.ErrDSAllocationFailed,
					"ds_callback_token sign failed under enforce for match %d: %v", matchID, terr)
			}
			plog.With(ctx).Warnw("msg", "ds_callback_token_sign_failed", "match_id", matchID, "err", terr)
		} else {
			meta.Annotations = map[string]string{dsTokenAnnotationKey: tok}
		}
	}

	pod, addr, actualTrack, err := a.allocateWithMetadata(ctx, matchID, mapID, releaseTrack, meta)
	return pod, addr, actualTrack, err
}

// AllocateAuthoritative 执行 Model B 的 K8s 分配半段：GSA POST 永不携带令牌；选中后必须
// 严格 GET GameServer 取得 UID/resourceVersion，任一字段缺失都 fail-closed。
func (a *AgonesGameServerAllocator) AllocateAuthoritative(
	ctx context.Context,
	matchID uint64,
	allocationID string,
	playerIDs []uint64,
	combatFactionByPlayer map[uint64]uint32,
	mapID uint32,
	gameMode, releaseTrack string,
) (*AuthoritativeGameServerAllocation, error) {
	parsedAllocationID, parseErr := uuid.Parse(allocationID)
	if matchID == 0 || parseErr != nil || parsedAllocationID == uuid.Nil ||
		parsedAllocationID.Version() != uuid.Version(4) || parsedAllocationID.Variant() != uuid.RFC4122 ||
		parsedAllocationID.String() != allocationID ||
		!releasetrack.Valid(releaseTrack) {
		return nil, errcode.New(errcode.ErrInvalidArg, "agones: match_id and allocation_id required")
	}
	canonicalPlayers, roster, rosterErr := dsmetadata.CanonicalRoster(playerIDs)
	if rosterErr != nil || len(canonicalPlayers) == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "agones: invalid battle roster: %v", rosterErr)
	}
	combatFactions := ""
	if len(combatFactionByPlayer) > 0 {
		var factionErr error
		canonicalPlayers, combatFactions, factionErr = dsmetadata.CanonicalCombatFactions(
			canonicalPlayers, combatFactionByPlayer)
		if factionErr != nil {
			return nil, errcode.New(errcode.ErrInvalidArg,
				"agones: invalid battle combat factions: %v", factionErr)
		}
	}
	partial := &AuthoritativeGameServerAllocation{AllocationID: allocationID}
	meta := &gsaMetadata{Labels: map[string]string{
		"pandora.dev/match-id":      fmt.Sprintf("%d", matchID),
		"pandora.dev/map-id":        fmt.Sprintf("%d", mapID),
		"pandora.dev/game-mode":     sanitizeLabelValue(gameMode),
		battleAllocationMetadataKey: sanitizeLabelValue(allocationID),
	}, Annotations: map[string]string{
		battleRosterAnnotationKey:   roster,
		battleAllocationMetadataKey: allocationID,
	}}
	if combatFactions != "" {
		meta.Annotations[battleCombatFactionsAnnotationKey] = combatFactions
	}
	podName, addr, selectedTrack, err := a.allocateWithMetadata(ctx, matchID, mapID, releaseTrack, meta)
	if err != nil {
		// 即使 POST 没有可解析响应，也必须把 allocation_id 交还调用方。它是未知结果
		// 对账/回收的唯一 fencing token；返回 nil 会让调用方删 claim 后再次分配第二个 Pod。
		return partial, err
	}
	partial.PodName, partial.Addr = podName, addr
	gs, err := a.getGameServer(ctx, podName)
	if err != nil {
		return partial, errcode.New(errcode.ErrDSAllocationFailed,
			"agones: strict GET selected gameserver %s failed: %v", podName, err)
	}
	actualReleaseTrack := gs.Metadata.Labels[releaseTrackMetadataKey]
	if gs.Metadata.Name != podName || gs.Metadata.UID == "" || gs.Metadata.ResourceVersion == "" ||
		gs.Metadata.Labels["pandora.dev/match-id"] != strconv.FormatUint(matchID, 10) ||
		gs.Metadata.Labels[battleAllocationMetadataKey] != sanitizeLabelValue(allocationID) ||
		gs.Metadata.Annotations[battleAllocationMetadataKey] != allocationID ||
		gs.Metadata.Annotations[battleRosterAnnotationKey] != roster ||
		!exactOptionalAnnotation(gs.Metadata.Annotations, battleCombatFactionsAnnotationKey, combatFactions) ||
		!releasetrack.Valid(actualReleaseTrack) || actualReleaseTrack != selectedTrack ||
		gs.Metadata.Annotations[releaseTrackMetadataKey] != actualReleaseTrack {
		return partial, errcode.New(errcode.ErrDSAllocationFailed,
			"agones: selected gameserver identity/binding incomplete: want_name=%q name=%q uid=%q rv=%q",
			podName, gs.Metadata.Name, gs.Metadata.UID, gs.Metadata.ResourceVersion)
	}
	partial.InstanceUID = gs.Metadata.UID
	partial.ResourceVersion = gs.Metadata.ResourceVersion
	pod, err := a.getPod(ctx, podName)
	if err != nil {
		return partial, errcode.New(errcode.ErrDSAllocationFailed,
			"agones: strict GET selected pod %s failed: %v", podName, err)
	}
	if !podOwnedByGameServer(pod, podName, gs.Metadata.UID) {
		return partial, errcode.New(errcode.ErrDSAllocationFailed,
			"agones: selected pod identity/owner incomplete: pod=%q pod_uid=%q gameserver_uid=%q",
			podName, pod.Metadata.UID, gs.Metadata.UID)
	}
	return &AuthoritativeGameServerAllocation{
		PodName:            podName,
		Addr:               addr,
		InstanceUID:        gs.Metadata.UID,
		PodUID:             pod.Metadata.UID,
		ResourceVersion:    gs.Metadata.ResourceVersion,
		AllocationID:       allocationID,
		ReleaseTrack:       actualReleaseTrack,
		AnnotationsPresent: gs.Metadata.Annotations != nil,
	}, nil
}

// ResolveAllocationByID reconciles an allocation whose POST result was
// unknown.  allocation_id is the idempotency/fencing label written by the GSA
// request before any credential exists.  This method is strictly read-only:
// zero objects is authoritative absence, exactly one object must match the
// complete original match/roster metadata and its owned Pod, and multiple
// objects are an ambiguity that must be retried/operated manually rather than
// guessed.
func (a *AgonesGameServerAllocator) ResolveAllocationByID(
	ctx context.Context,
	matchID uint64,
	allocationID string,
	playerIDs []uint64,
	combatFactionByPlayer map[uint64]uint32,
	mapID uint32,
	gameMode string,
) (*AuthoritativeGameServerAllocation, bool, error) {
	parsedAllocationID, parseErr := uuid.Parse(allocationID)
	canonicalPlayers, roster, rosterErr := dsmetadata.CanonicalRoster(playerIDs)
	combatFactions := ""
	if rosterErr == nil && len(combatFactionByPlayer) > 0 {
		var factionErr error
		canonicalPlayers, combatFactions, factionErr = dsmetadata.CanonicalCombatFactions(
			canonicalPlayers, combatFactionByPlayer)
		if factionErr != nil {
			rosterErr = factionErr
		}
	}
	if matchID == 0 || parseErr != nil || parsedAllocationID == uuid.Nil ||
		parsedAllocationID.Version() != uuid.Version(4) || parsedAllocationID.Variant() != uuid.RFC4122 ||
		parsedAllocationID.String() != allocationID ||
		rosterErr != nil || len(canonicalPlayers) == 0 {
		return nil, false, errcode.New(errcode.ErrInvalidArg,
			"agones: complete uncertain allocation identity required")
	}
	selector := battleAllocationMetadataKey + "=" + sanitizeLabelValue(allocationID)
	listURL := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers?labelSelector=%s",
		a.apiServer, a.namespace, url.QueryEscape(selector))
	body, status, err := a.do(ctx, http.MethodGet, listURL, nil)
	if err != nil {
		return nil, false, errcode.New(errcode.ErrDSAllocationFailed,
			"agones: resolve allocation_id %s: %v", allocationID, err)
	}
	if status < 200 || status >= 300 {
		return nil, false, errcode.New(errcode.ErrDSAllocationFailed,
			"agones: resolve allocation_id %s http %d: %s",
			allocationID, status, truncate(body, 256))
	}
	var list gameServerListResponse
	if err := json.Unmarshal(body, &list); err != nil {
		return nil, false, errcode.New(errcode.ErrDSAllocationFailed,
			"agones: decode allocation_id %s list: %v", allocationID, err)
	}
	switch len(list.Items) {
	case 0:
		return nil, false, nil
	case 1:
		// Continue below.
	default:
		return nil, false, errcode.New(errcode.ErrDSAllocationFailed,
			"agones: allocation_id %s is ambiguous: gameservers=%d", allocationID, len(list.Items))
	}

	gs := &list.Items[0]
	actualTrack := gs.Metadata.Labels[releaseTrackMetadataKey]
	if gs.Metadata.Name == "" || gs.Metadata.UID == "" || gs.Metadata.ResourceVersion == "" ||
		gs.Metadata.Labels["pandora.dev/match-id"] != strconv.FormatUint(matchID, 10) ||
		gs.Metadata.Labels["pandora.dev/map-id"] != strconv.FormatUint(uint64(mapID), 10) ||
		gs.Metadata.Labels["pandora.dev/game-mode"] != sanitizeLabelValue(gameMode) ||
		gs.Metadata.Labels[battleAllocationMetadataKey] != sanitizeLabelValue(allocationID) ||
		gs.Metadata.Annotations[battleAllocationMetadataKey] != allocationID ||
		gs.Metadata.Annotations[battleRosterAnnotationKey] != roster ||
		!exactOptionalAnnotation(gs.Metadata.Annotations, battleCombatFactionsAnnotationKey, combatFactions) ||
		!releasetrack.Valid(actualTrack) || gs.Metadata.Annotations[releaseTrackMetadataKey] != actualTrack {
		return nil, false, errcode.New(errcode.ErrDSAllocationFailed,
			"agones: allocation_id %s resolved GameServer binding is incomplete or conflicting",
			allocationID)
	}
	pod, err := a.getPod(ctx, gs.Metadata.Name)
	if err != nil {
		return nil, false, errcode.New(errcode.ErrDSAllocationFailed,
			"agones: resolve allocation_id %s pod %s: %v", allocationID, gs.Metadata.Name, err)
	}
	if !podOwnedByGameServer(pod, gs.Metadata.Name, gs.Metadata.UID) {
		return nil, false, errcode.New(errcode.ErrDSAllocationFailed,
			"agones: allocation_id %s associated Pod identity is incomplete", allocationID)
	}
	return &AuthoritativeGameServerAllocation{
		PodName:            gs.Metadata.Name,
		InstanceUID:        gs.Metadata.UID,
		PodUID:             pod.Metadata.UID,
		ResourceVersion:    gs.Metadata.ResourceVersion,
		AllocationID:       allocationID,
		ReleaseTrack:       actualTrack,
		AnnotationsPresent: gs.Metadata.Annotations != nil,
	}, true, nil
}

// exactOptionalAnnotation 区分 legacy 缺席与新协议精确值：期望空串时 key 必须不存在，
// 不能把意外的空值/旧分配残留当作同一权威快照。
func exactOptionalAnnotation(annotations map[string]string, key, expected string) bool {
	value, found := annotations[key]
	if expected == "" {
		return !found
	}
	return found && value == expected
}

func (a *AgonesGameServerAllocator) allocateWithMetadata(
	ctx context.Context,
	matchID uint64,
	mapID uint32,
	desiredReleaseTrack string,
	meta *gsaMetadata,
) (string, string, string, error) {
	if !releasetrack.Valid(desiredReleaseTrack) {
		return "", "", "", errcode.New(errcode.ErrInvalidArg,
			"agones: invalid desired release track %q", desiredReleaseTrack)
	}
	tracks := []string{desiredReleaseTrack}
	if desiredReleaseTrack == releasetrack.Canary {
		// 只有明确的 UnAllocated/NoAvailable 才进入第二次 stable POST；transport/
		// decode 等结果未知时立即停，绝不冒险产生第二个已分配 GameServer。
		tracks = append(tracks, releasetrack.Stable)
	}
	for i, track := range tracks {
		attemptMeta := cloneGSAMetadata(meta)
		attemptMeta.Labels[releaseTrackMetadataKey] = track
		attemptMeta.Annotations[releaseTrackMetadataKey] = track
		pod, addr, err := a.allocateOnceWithMetadata(ctx, matchID, mapID, track, attemptMeta)
		if err == nil {
			return pod, addr, track, nil
		}
		if errcode.As(err) != errcode.ErrDSNoAvailable || i == len(tracks)-1 {
			return "", "", "", err
		}
		plog.With(ctx).Warnw("msg", "battle_canary_capacity_fallback_stable",
			"match_id", matchID, "map_id", mapID)
	}
	return "", "", "", errcode.New(errcode.ErrDSNoAvailable, "agones: no gameserver")
}

func cloneGSAMetadata(meta *gsaMetadata) *gsaMetadata {
	out := &gsaMetadata{Labels: make(map[string]string), Annotations: make(map[string]string)}
	if meta == nil {
		return out
	}
	for key, value := range meta.Labels {
		out.Labels[key] = value
	}
	for key, value := range meta.Annotations {
		out.Annotations[key] = value
	}
	return out
}

func (a *AgonesGameServerAllocator) allocateOnceWithMetadata(
	ctx context.Context,
	matchID uint64,
	mapID uint32,
	releaseTrack string,
	meta *gsaMetadata,
) (string, string, error) {
	generalFleet := a.fleetName
	dedicatedFleet := a.mapFleets[mapID].stable
	if releaseTrack == releasetrack.Canary {
		generalFleet = a.canaryFleetName
		dedicatedFleet = a.mapFleets[mapID].canary
	}
	if generalFleet == "" {
		// INC-20260724-001 L8:fleet 名未配置是**配置错误**,不是容量事实。
		// 报 ErrDSNoAvailable(5001) 会把它混进"无空闲副本"的容量口径,让运维照着扩容排查。
		// 上游 matchmaker 对 5001/5002 处理完全一致(都算确定性失败),故改码不影响行为,
		// 只把语义归位。
		return "", "", errcode.New(errcode.ErrDSAllocationFailed,
			"agones: no %s fleet configured", releaseTrack)
	}
	selectorLabels := func(fleet string) map[string]string {
		return map[string]string{fleetLabelKey: fleet, releaseTrackMetadataKey: releaseTrack}
	}
	selectors := make([]gsaSelector, 0, 2)
	if dedicatedFleet != "" && dedicatedFleet != generalFleet {
		selectors = append(selectors, gsaSelector{MatchLabels: selectorLabels(dedicatedFleet)})
	}
	selectors = append(selectors, gsaSelector{MatchLabels: selectorLabels(generalFleet)})

	reqBody := gsaRequest{
		APIVersion: "allocation.agones.dev/v1",
		Kind:       "GameServerAllocation",
		Spec: gsaSpec{
			Selectors: selectors,
			// 把业务标识打到被分配的 GameServer 上,便于运维 / 排障关联对局。
			Metadata: meta,
		},
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return "", "", errcode.New(errcode.ErrDSAllocationFailed, "agones: marshal request: %v", err)
	}

	url := fmt.Sprintf("%s/apis/allocation.agones.dev/v1/namespaces/%s/gameserverallocations",
		a.apiServer, a.namespace)

	respBytes, status, err := a.do(ctx, http.MethodPost, url, payload)
	if err != nil {
		return "", "", errcode.New(errcode.ErrDSAllocationFailed, "agones: allocate match %d: %v", matchID, err)
	}
	if status < 200 || status >= 300 {
		return "", "", errcode.New(errcode.ErrDSAllocationFailed,
			"agones: allocate match %d http %d: %s", matchID, status, truncate(respBytes, 256))
	}

	var resp gsaResponse
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return "", "", errcode.New(errcode.ErrDSAllocationFailed, "agones: decode response: %v", err)
	}
	// state 只有 "Allocated" 才表示拿到了 GameServer;UnAllocated / Contention = 无空闲。
	if resp.Status.State != "Allocated" {
		return "", "", errcode.New(errcode.ErrDSNoAvailable,
			"agones: no gameserver for match %d (state=%q)", matchID, resp.Status.State)
	}
	if resp.Status.GameServerName == "" || resp.Status.Address == "" || len(resp.Status.Ports) == 0 {
		return "", "", errcode.New(errcode.ErrDSAllocationFailed,
			"agones: incomplete status for match %d: name=%q addr=%q ports=%d",
			matchID, resp.Status.GameServerName, resp.Status.Address, len(resp.Status.Ports))
	}

	host := resp.Status.Address
	if a.advertiseHost != "" {
		host = a.advertiseHost
	}
	addr := fmt.Sprintf("%s:%d", host, resp.Status.Ports[0].Port)
	return resp.Status.GameServerName, addr, nil
}

// DeliverCredential 用 UID+resourceVersion JSON Patch 投递 Redis pending 的镜像。PATCH 的
// HTTP 结果从不直接作为成功依据：无论 2xx 空/坏响应、409，还是 transport timeout，均再做
// 一次严格 GET；只有 UID 未变且全部 annotation 与期望精确相等才成功，绝不本地 fallback。
func (a *AgonesGameServerAllocator) DeliverCredential(
	ctx context.Context,
	allocation *AuthoritativeGameServerAllocation,
	annotations map[string]string,
) (string, error) {
	if allocation == nil || allocation.PodName == "" || allocation.InstanceUID == "" ||
		allocation.ResourceVersion == "" || len(annotations) == 0 {
		return "", errcode.New(errcode.ErrInvalidArg, "agones: incomplete credential delivery input")
	}
	for k, v := range annotations {
		if k == "" || v == "" {
			return "", errcode.New(errcode.ErrInvalidArg,
				"agones: credential annotation key/value must be non-empty")
		}
	}
	ops := []jsonPatchOperation{
		{Op: "test", Path: "/metadata/uid", Value: allocation.InstanceUID},
		{Op: "test", Path: "/metadata/resourceVersion", Value: allocation.ResourceVersion},
	}
	if !allocation.AnnotationsPresent {
		ops = append(ops, jsonPatchOperation{Op: "add", Path: "/metadata/annotations", Value: annotations})
	} else {
		keys := make([]string, 0, len(annotations))
		for key := range annotations {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			pathKey := strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
			ops = append(ops, jsonPatchOperation{Op: "add", Path: "/metadata/annotations/" + pathKey, Value: annotations[key]})
		}
	}
	payload, err := json.Marshal(ops)
	if err != nil {
		return "", errcode.New(errcode.ErrDSAllocationFailed, "agones: marshal credential patch: %v", err)
	}
	url := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers/%s",
		a.apiServer, a.namespace, allocation.PodName)
	patchBody, patchStatus, patchErr := a.doWithContentType(
		ctx, http.MethodPatch, url, payload, "application/json-patch+json")

	// PATCH 结果未知时，确认读不能复用一个可能已因入站超时/取消而失效的 ctx。
	// 独立确认仍由 do() 的 allocateTimeout 严格限时；它只读 K8s 当前事实，不延长业务写。
	// plog.Detach(§16.7):detached 确认读只复制日志字段,不携带请求级 transport/取消。
	confirmed, confirmErr := a.getGameServer(plog.Detach(ctx), allocation.PodName)
	if confirmErr == nil {
		confirmErr = confirmCredentialDelivery(confirmed, allocation, annotations)
	}
	if confirmErr == nil {
		return confirmed.Metadata.ResourceVersion, nil
	}
	return "", errcode.New(errcode.ErrDSAllocationFailed,
		"agones: credential PATCH not strictly confirmed: patch_status=%d patch_err=%v patch_body=%q confirm_err=%v",
		patchStatus, patchErr, truncate(patchBody, 256), confirmErr)
}

func (a *AgonesGameServerAllocator) getGameServer(ctx context.Context, podName string) (*gameServerResponse, error) {
	url := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers/%s",
		a.apiServer, a.namespace, podName)
	body, status, err := a.do(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("GET gameserver http %d: %s", status, truncate(body, 256))
	}
	var gs gameServerResponse
	if err := json.Unmarshal(body, &gs); err != nil {
		return nil, fmt.Errorf("decode gameserver: %w", err)
	}
	return &gs, nil
}

func (a *AgonesGameServerAllocator) getPod(ctx context.Context, podName string) (*podResponse, error) {
	url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", a.apiServer, a.namespace, podName)
	body, status, err := a.do(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if status < 200 || status >= 300 {
		return nil, fmt.Errorf("GET pod http %d: %s", status, truncate(body, 256))
	}
	var pod podResponse
	if err := json.Unmarshal(body, &pod); err != nil {
		return nil, fmt.Errorf("decode pod: %w", err)
	}
	return &pod, nil
}

func podOwnedByGameServer(pod *podResponse, podName, gameServerUID string) bool {
	if pod == nil || pod.Metadata.Name != podName || pod.Metadata.UID == "" || gameServerUID == "" {
		return false
	}
	for _, owner := range pod.Metadata.OwnerReferences {
		if owner.Controller && owner.Kind == "GameServer" && owner.UID == gameServerUID {
			return true
		}
	}
	return false
}

func confirmCredentialDelivery(
	gs *gameServerResponse,
	allocation *AuthoritativeGameServerAllocation,
	annotations map[string]string,
) error {
	if gs == nil || gs.Metadata.Name != allocation.PodName || gs.Metadata.UID != allocation.InstanceUID ||
		gs.Metadata.ResourceVersion == "" {
		return fmt.Errorf("gameserver identity mismatch or incomplete")
	}
	for key, want := range annotations {
		if got := gs.Metadata.Annotations[key]; got != want {
			return fmt.Errorf("annotation %q mismatch: got=%q", key, got)
		}
	}
	return nil
}

// ResolveExpectedPodUID performs the only allowed rolling-upgrade backfill
// for BattleStorageRecord records written before pod_uid existed.  Both
// Kubernetes objects are read before any delete, and every durable binding is
// checked.  A missing object or a same-name replacement is deliberately not
// treated as success because the old Pod UID cannot then be proven.
func (a *AgonesGameServerAllocator) ResolveExpectedPodUID(
	ctx context.Context,
	allocation *AuthoritativeGameServerAllocation,
) (string, error) {
	if allocation == nil {
		return "", errcode.New(errcode.ErrInvalidArg,
			"agones: pod UID preflight requires gameserver name, uid and allocation_id")
	}
	parsedAllocationID, parseErr := uuid.Parse(allocation.AllocationID)
	if allocation.PodName == "" || allocation.InstanceUID == "" ||
		parseErr != nil || parsedAllocationID == uuid.Nil || parsedAllocationID.Version() != uuid.Version(4) ||
		parsedAllocationID.Variant() != uuid.RFC4122 ||
		parsedAllocationID.String() != allocation.AllocationID {
		return "", errcode.New(errcode.ErrInvalidArg,
			"agones: pod UID preflight requires gameserver name, uid and allocation_id")
	}
	gs, err := a.getGameServer(ctx, allocation.PodName)
	if err != nil {
		return "", errcode.New(errcode.ErrDSAllocationFailed,
			"agones: pod UID preflight gameserver GET failed: %v", err)
	}
	if gs.Metadata.Name != allocation.PodName || gs.Metadata.UID != allocation.InstanceUID ||
		gs.Metadata.Labels[battleAllocationMetadataKey] != sanitizeLabelValue(allocation.AllocationID) ||
		gs.Metadata.Annotations[battleAllocationMetadataKey] != allocation.AllocationID {
		return "", errcode.New(errcode.ErrDSAllocationFailed,
			"agones: pod UID preflight gameserver identity/binding changed: pod=%q uid=%q allocation_id=%q",
			gs.Metadata.Name, gs.Metadata.UID, allocation.AllocationID)
	}
	pod, err := a.getPod(ctx, allocation.PodName)
	if err != nil {
		return "", errcode.New(errcode.ErrDSAllocationFailed,
			"agones: pod UID preflight pod GET failed: %v", err)
	}
	if !podOwnedByGameServer(pod, allocation.PodName, allocation.InstanceUID) {
		return "", errcode.New(errcode.ErrDSAllocationFailed,
			"agones: pod UID preflight owner/identity changed: pod=%q pod_uid=%q gameserver_uid=%q",
			allocation.PodName, pod.Metadata.UID, allocation.InstanceUID)
	}
	return pod.Metadata.UID, nil
}

// ProbeExpectedInstanceGone 只读探测 exact 实例是否已可证死亡,供 warming 冷加载
// 宽限期的 sweep 加速判弃(biz.WarmingInstanceProber):
//   - GameServer NotFound / UID 已换代,且(有持久 podUID 时)关联 Pod 同样
//     NotFound / UID 已换代 → (true, nil):物理实例已确认消失;
//   - GameServer 同 UID 且 Status.State=Unhealthy → (true, nil):Agones 依据
//     SDK health ping(DS 侧独立线程 pinger)断流判死,该实例不可能再服务本局;
//   - 同 UID 且非 Unhealthy → (false, nil):实例存活(可能仍在冷加载);
//   - 任何读失败/不确定 → (false, err):调用方必须回退时间界,不得据此回收。
// 零写副作用;判弃权威仍是 Redis 事务(AbandonIfStale 的 WATCH 单赢家)。
func (a *AgonesGameServerAllocator) ProbeExpectedInstanceGone(
	ctx context.Context,
	podName, instanceUID, podUID string,
) (bool, error) {
	if podName == "" || instanceUID == "" {
		return false, errcode.New(errcode.ErrInvalidArg,
			"agones: instance probe requires gameserver name and uid")
	}
	gsURL := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers/%s",
		a.apiServer, a.namespace, podName)
	body, status, err := a.do(ctx, http.MethodGet, gsURL, nil)
	if err != nil {
		return false, err
	}
	switch {
	case status == http.StatusNotFound:
		// GameServer 已消失,继续向下做 Pod 双确认。
	case status >= 200 && status < 300:
		var gs gameServerResponse
		if uerr := json.Unmarshal(body, &gs); uerr != nil {
			return false, fmt.Errorf("decode gameserver probe: %w", uerr)
		}
		if gs.Metadata.UID == "" {
			return false, fmt.Errorf("gameserver probe missing metadata.uid")
		}
		if gs.Metadata.UID == instanceUID {
			return gs.Status.State == agonesStateUnhealthy, nil
		}
		// 同名对象已是新实例:旧 UID 物理消失,继续 Pod 双确认。
	default:
		return false, fmt.Errorf("GET gameserver probe http %d: %s", status, truncate(body, 128))
	}
	if podUID == "" {
		// 无持久 Pod UID 可对账时只认 GameServer 消失(modelB 分配在 finalize 即持久化
		// pod_uid,该回退只覆盖历史残留记录)。
		return true, nil
	}
	podURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", a.apiServer, a.namespace, podName)
	podGone, perr := a.resourceUIDGone(ctx, podURL, podUID)
	if perr != nil {
		return false, perr
	}
	return podGone, nil
}

// ErrReleaseDeletionPending:exact 删除已被 apiserver 受理(对象带 deletionTimestamp),
// 物理消失需等待 Pod terminationGracePeriod(默认 30s)。调用方应按分配身份退避后重试
// 确认,不得重复 DELETE,更不得据此写 teardown proof(nil 返回才是物理消失证明)。
var ErrReleaseDeletionPending = errors.New("agones: exact release deletion pending")

// exactResourceProbe 是 exact 对象(按 name GET + 期望 UID 对账)的三态快照。
type exactResourceProbe struct {
	gone     bool // NotFound / 同名对象 UID 已换代:期望实例物理消失
	deleting bool // 同 UID 且带 deletionTimestamp:删除已受理,处于终止宽限
}

func (a *AgonesGameServerAllocator) probeExactResource(
	ctx context.Context,
	resourceURL, expectedUID string,
) (exactResourceProbe, error) {
	body, status, err := a.do(ctx, http.MethodGet, resourceURL, nil)
	if err != nil {
		return exactResourceProbe{}, err
	}
	if status == http.StatusNotFound {
		return exactResourceProbe{gone: true}, nil
	}
	if status < 200 || status >= 300 {
		return exactResourceProbe{}, fmt.Errorf("GET exact resource http %d: %s", status, truncate(body, 128))
	}
	var object struct {
		Metadata struct {
			UID               string `json:"uid"`
			DeletionTimestamp string `json:"deletionTimestamp"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &object); err != nil {
		return exactResourceProbe{}, fmt.Errorf("decode exact resource identity: %w", err)
	}
	if object.Metadata.UID == "" {
		return exactResourceProbe{}, fmt.Errorf("exact resource missing metadata.uid")
	}
	if object.Metadata.UID != expectedUID {
		return exactResourceProbe{gone: true}, nil
	}
	return exactResourceProbe{deleting: object.Metadata.DeletionTimestamp != ""}, nil
}

// ReleaseExpected 只删除 UID 仍等于本次分配实例的 GameServer。
// DELETE 2xx 只是 deletion request accepted，不是物理消失证明；本方法
// 会继续轮询 exact GameServer UID 及分配时捕获的关联 Pod UID，
// 只有两者都 NotFound/UID changed 才返回 nil。
// 同 UID 已带 deletionTimestamp(删除已受理、Pod 处终止宽限)时**跳过重复 DELETE**,
// 并快速返回 ErrReleaseDeletionPending 而非空耗轮询——宽限内对象不可能物理消失,
// 空转只会占住 sweep 队头(INC-20260727-001 复审 P1-2)。
func (a *AgonesGameServerAllocator) ReleaseExpected(
	ctx context.Context,
	allocation *AuthoritativeGameServerAllocation,
) error {
	if allocation == nil || (allocation.InstanceUID == "" && allocation.AllocationID == "") {
		return errcode.New(errcode.ErrInvalidArg,
			"agones: expected release requires uid or allocation_id")
	}
	if allocation.InstanceUID == "" {
		// POST 已选中但 UID GET 不确定时，不能按名字删除。allocation_id 是本次 GSA 写入
		// 选中对象的唯一 UUID label，用 DeleteCollection 精确回收；同名重建的新对象不会
		// 带旧 allocation_id，因此旧 cleanup 不会误杀新实例。
		selector := "pandora.dev/allocation-id=" + sanitizeLabelValue(allocation.AllocationID)
		deleteBody, err := json.Marshal(deleteOptions{APIVersion: "v1", Kind: "DeleteOptions"})
		if err != nil {
			return errcode.New(errcode.ErrDSAllocationFailed, "agones: marshal allocation delete: %v", err)
		}
		collectionURL := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers?labelSelector=%s",
			a.apiServer, a.namespace, url.QueryEscape(selector))
		// LIST 先行(复审必修):对象已全部处于删除宽限时直接返回 pending,不重复
		// DeleteCollection。空集合或含存活对象时才发 DeleteCollection——空集合仍保留
		// DeleteCollection+后置 LIST 的 timeout-late-apply 防线(此前一次 DeleteCollection
		// 可能已被受理但响应丢失,幂等再发无害且能捕获迟到 apply 出现的新对象)。
		preListBody, preListStatus, preListErr := a.do(ctx, http.MethodGet, collectionURL, nil)
		if preListErr == nil && preListStatus >= 200 && preListStatus < 300 {
			var preList gameServerListResponse
			if uerr := json.Unmarshal(preListBody, &preList); uerr == nil && len(preList.Items) > 0 {
				allDeleting := true
				for _, item := range preList.Items {
					if item.Metadata.DeletionTimestamp == "" {
						allDeleting = false
						break
					}
				}
				if allDeleting {
					return fmt.Errorf("agones: allocation-id release awaiting termination grace (pre-list): allocation_id=%s items=%d: %w",
						allocation.AllocationID, len(preList.Items), ErrReleaseDeletionPending)
				}
			}
		}
		deleteResp, deleteStatus, deleteErr := a.do(ctx, http.MethodDelete, collectionURL, deleteBody)
		// DeleteCollection 的响应/超时同样不构成完成证据；严格 LIST 确认该唯一 label
		// 已无对象。timeout-but-applied 可幂等成功，2xx 但仍有对象则保留 Redis claim。
		listBody, listStatus, listErr := a.do(ctx, http.MethodGet, collectionURL, nil)
		if listErr == nil && listStatus >= 200 && listStatus < 300 {
			var list gameServerListResponse
			if uerr := json.Unmarshal(listBody, &list); uerr == nil {
				if len(list.Items) == 0 {
					return nil
				}
				allDeleting := true
				for _, item := range list.Items {
					if item.Metadata.DeletionTimestamp == "" {
						allDeleting = false
						break
					}
				}
				if allDeleting {
					// 全部对象删除已受理:等终止宽限,调用方退避后重试确认,不算失败。
					return fmt.Errorf("agones: allocation-id release awaiting termination grace: allocation_id=%s items=%d: %w",
						allocation.AllocationID, len(list.Items), ErrReleaseDeletionPending)
				}
			}
		}
		return errcode.New(errcode.ErrDSAllocationFailed,
			"agones: allocation-id release not confirmed: allocation_id=%s delete_status=%d delete_err=%v delete_body=%q list_status=%d list_err=%v list_body=%q",
			allocation.AllocationID, deleteStatus, deleteErr, truncate(deleteResp, 128),
			listStatus, listErr, truncate(listBody, 128))
	}
	if allocation.PodName == "" {
		return errcode.New(errcode.ErrInvalidArg, "agones: UID release requires gameserver name")
	}
	if allocation.PodUID == "" {
		// Never capture this only in process memory and then DELETE. A crash after
		// Kubernetes accepted the delete would lose the Pod UID forever, so a
		// restarted reconciler could no longer prove that the exact physical Pod
		// disappeared. New allocations persist PodUID before any external delete;
		// legacy records must be drained/migrated by the release preflight.
		return errcode.New(errcode.ErrDSAllocationFailed,
			"agones: durable associated pod UID required before exact release: pod=%s gameserver_uid=%s",
			allocation.PodName, allocation.InstanceUID)
	}
	url := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers/%s",
		a.apiServer, a.namespace, allocation.PodName)
	// 删除前先读 exact 状态:已消失或已带 deletionTimestamp 都无需再发 DELETE
	//(重复 DELETE 是纯噪声;实测每实例 7 次重复,INC-20260727-001 复审 P1-2)。
	preProbe, preErr := a.probeExactResource(ctx, url, allocation.InstanceUID)
	if preErr != nil {
		return errcode.New(errcode.ErrDSAllocationFailed,
			"agones: exact release pre-probe failed: pod=%s gs_uid=%s err=%v",
			allocation.PodName, allocation.InstanceUID, preErr)
	}
	var respBody []byte
	var status int
	var deleteErr error
	if !preProbe.gone && !preProbe.deleting {
		body, err := json.Marshal(deleteOptions{
			APIVersion: "v1", Kind: "DeleteOptions",
			Preconditions: &deletePreconditions{UID: allocation.InstanceUID},
		})
		if err != nil {
			return errcode.New(errcode.ErrDSAllocationFailed, "agones: marshal expected delete: %v", err)
		}
		respBody, status, deleteErr = a.do(ctx, http.MethodDelete, url, body)
	}
	if confirmErr := a.waitExpectedInstanceGone(ctx, allocation); confirmErr != nil {
		if errors.Is(confirmErr, ErrReleaseDeletionPending) {
			// 保持哨兵可被 errors.Is 识别:调用方按分配身份退避,不重复 DELETE。
			return fmt.Errorf("agones: exact release awaiting termination grace: pod=%s gs_uid=%s pod_uid=%s: %w",
				allocation.PodName, allocation.InstanceUID, allocation.PodUID, ErrReleaseDeletionPending)
		}
		return errcode.New(errcode.ErrDSAllocationFailed,
			"agones: exact release not physically confirmed: pod=%s gs_uid=%s pod_uid=%s delete_status=%d delete_err=%v delete_body=%q confirm_err=%v",
			allocation.PodName, allocation.InstanceUID, allocation.PodUID, status, deleteErr,
			truncate(respBody, 128), confirmErr)
	}
	return nil
}

func (a *AgonesGameServerAllocator) waitExpectedInstanceGone(
	ctx context.Context,
	allocation *AuthoritativeGameServerAllocation,
) error {
	wait := a.allocateTimeout
	if wait <= 0 {
		wait = 5 * time.Second
	}
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	gsURL := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers/%s",
		a.apiServer, a.namespace, allocation.PodName)
	podURL := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s",
		a.apiServer, a.namespace, allocation.PodName)
	var lastGS, lastPod exactResourceProbe
	var lastGSErr, lastPodErr error
	for {
		lastGS, lastGSErr = a.probeExactResource(waitCtx, gsURL, allocation.InstanceUID)
		lastPod, lastPodErr = a.probeExactResource(waitCtx, podURL, allocation.PodUID)
		if lastGSErr == nil && lastPodErr == nil {
			if lastGS.gone && lastPod.gone {
				return nil
			}
			// 任一对象带 deletionTimestamp:删除已受理,物理消失要等终止宽限(默认 30s,
			// 远超本确认窗口),继续轮询只是空耗——立即返回 pending 哨兵交调用方退避。
			if lastGS.deleting || lastPod.deleting {
				return fmt.Errorf("gs_gone=%t gs_deleting=%t pod_gone=%t pod_deleting=%t: %w",
					lastGS.gone, lastGS.deleting, lastPod.gone, lastPod.deleting, ErrReleaseDeletionPending)
			}
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("timeout waiting exact objects gone: gs=%+v gs_err=%v pod=%+v pod_err=%v: %w",
				lastGS, lastGSErr, lastPod, lastPodErr, waitCtx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// resourceUIDGone 保留「物理消失才算 gone」语义(deleting 不算),供判死 probe 等
// 只关心 gone/存活 的调用方使用。
func (a *AgonesGameServerAllocator) resourceUIDGone(
	ctx context.Context,
	resourceURL, expectedUID string,
) (bool, error) {
	probe, err := a.probeExactResource(ctx, resourceURL, expectedUID)
	if err != nil {
		return false, err
	}
	return probe.gone, nil
}

// Release DELETE 该 GameServer(Fleet 自动补新);404 视作已释放(幂等)。
func (a *AgonesGameServerAllocator) Release(ctx context.Context, podName string) error {
	if podName == "" {
		return nil
	}
	url := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/gameservers/%s",
		a.apiServer, a.namespace, podName)

	respBytes, status, err := a.do(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return errcode.New(errcode.ErrDSAllocationFailed, "agones: release %s: %v", podName, err)
	}
	if status == http.StatusNotFound {
		return nil // 已不存在 = 已释放,幂等
	}
	if status < 200 || status >= 300 {
		return errcode.New(errcode.ErrDSAllocationFailed,
			"agones: release %s http %d: %s", podName, status, truncate(respBytes, 256))
	}
	return nil
}

// ── Fleet 容量巡检(K8s 快上限预警,2026-07-10)──────────────────────────────

// FleetCapacity 一个 Agones Fleet 的容量快照(取自 Fleet status 子对象)。
type FleetCapacity struct {
	Fleet     string // Fleet 名
	Replicas  uint32 // status.replicas:当前总副本数(容量上限)
	Ready     uint32 // status.readyReplicas:空闲可分配
	Allocated uint32 // status.allocatedReplicas:已被对局占用

	// Desired = spec.replicas:运维/autoscaler **期望**的副本数,与 status 三项来自同一次 GET。
	// 用于区分「被负载打满(desired>0 且 ready==0)」与「本就没配容量(desired==0)」——
	// INC-20260724-001:未做金丝雀发布时 canary Fleet 常态 desired=0,旧逻辑把它恒判 exhausted,
	// 每 5 分钟一条 Error + Grafana critical 长期 firing,把真实的 stable ready=0 信号淹没了。
	Desired uint32
	// DesiredKnown 显式区分「解码到 spec.replicas 且为 0」与「没解码到」。
	// 不得让解码失败静默取零值 —— 那正是 §9.22 禁止的「把 UNKNOWN 冒充成确定值」。
	DesiredKnown bool
	// Canary 标记该 Fleet 属金丝雀轨。只有 canary 轨的 desired==0 才是常态(可静默);
	// stable 轨被缩到 0 仍必须照常告警(否则运维误缩零会被静音)。
	Canary bool
}

// fleetStatusResponse 只声明容量巡检用到的 Fleet spec/status 字段。
type fleetStatusResponse struct {
	Spec struct {
		// 指针:区分「字段缺失」与「显式为 0」。
		Replicas *uint32 `json:"replicas"`
	} `json:"spec"`
	Status struct {
		Replicas          uint32 `json:"replicas"`
		ReadyReplicas     uint32 `json:"readyReplicas"`
		AllocatedReplicas uint32 `json:"allocatedReplicas"`
	} `json:"status"`
}

// WatchedFleets 返回容量巡检要盯的 Fleet 集合:通用池 fleetName + 全部 map_fleets 专属预热池
// (去重;通用池在前,专属池按名字典序,保证顺序稳定)。
func (a *AgonesGameServerAllocator) WatchedFleets() []string {
	out := make([]string, 0, 2+2*len(a.mapFleets))
	seen := make(map[string]bool)
	add := func(name string) {
		if name != "" && !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	add(a.fleetName)
	add(a.canaryFleetName)
	dedicated := make([]string, 0, 2*len(a.mapFleets))
	for _, route := range a.mapFleets {
		for _, name := range []string{route.stable, route.canary} {
			if name != "" && !seen[name] {
				seen[name] = true
				dedicated = append(dedicated, name)
			}
		}
	}
	sort.Strings(dedicated)
	return append(out, dedicated...)
}

// canaryFleets 返回「只属金丝雀轨」的 Fleet 名集合。
//
// 同名兜底:若配置把 stable 与 canary 指到同一个 Fleet(误配),该名字按 **stable** 处理
// (与 WatchedFleets 里 stable 先入的去重顺序一致),不享受 canary 的 desired==0 静默,
// 宁可多报也不静音真实容量问题。
func (a *AgonesGameServerAllocator) canaryFleets() map[string]bool {
	stable := map[string]bool{}
	if a.fleetName != "" {
		stable[a.fleetName] = true
	}
	for _, route := range a.mapFleets {
		if route.stable != "" {
			stable[route.stable] = true
		}
	}
	canary := map[string]bool{}
	add := func(name string) {
		if name != "" && !stable[name] {
			canary[name] = true
		}
	}
	add(a.canaryFleetName)
	for _, route := range a.mapFleets {
		add(route.canary)
	}
	return canary
}

// ListFleetCapacities GET 每个受管 Fleet 的 status,返回容量快照。
// 单个 Fleet 查询失败不影响其余(部分成功也返回),错误经 errors.Join 汇总供上层打日志。
func (a *AgonesGameServerAllocator) ListFleetCapacities(ctx context.Context) ([]FleetCapacity, error) {
	var (
		out  []FleetCapacity
		errs []error
	)
	canaryTrack := a.canaryFleets()
	for _, fleet := range a.WatchedFleets() {
		url := fmt.Sprintf("%s/apis/agones.dev/v1/namespaces/%s/fleets/%s",
			a.apiServer, a.namespace, fleet)
		respBytes, status, err := a.do(ctx, http.MethodGet, url, nil)
		if err != nil {
			errs = append(errs, fmt.Errorf("agones: get fleet %s: %w", fleet, err))
			continue
		}
		if status < 200 || status >= 300 {
			errs = append(errs, fmt.Errorf("agones: get fleet %s http %d: %s",
				fleet, status, truncate(respBytes, 256)))
			continue
		}
		var resp fleetStatusResponse
		if err := json.Unmarshal(respBytes, &resp); err != nil {
			errs = append(errs, fmt.Errorf("agones: decode fleet %s: %w", fleet, err))
			continue
		}
		capacity := FleetCapacity{
			Fleet:     fleet,
			Replicas:  resp.Status.Replicas,
			Ready:     resp.Status.ReadyReplicas,
			Allocated: resp.Status.AllocatedReplicas,
			Canary:    canaryTrack[fleet],
		}
		if resp.Spec.Replicas != nil {
			capacity.Desired, capacity.DesiredKnown = *resp.Spec.Replicas, true
		}
		out = append(out, capacity)
	}
	return out, errors.Join(errs...)
}

// do 发一次带鉴权的 REST 请求,返回 (body, statusCode, transportErr)。
func (a *AgonesGameServerAllocator) do(ctx context.Context, method, url string, body []byte) ([]byte, int, error) {
	return a.doWithContentType(ctx, method, url, body, "application/json")
}

func (a *AgonesGameServerAllocator) doWithContentType(
	ctx context.Context,
	method, url string,
	body []byte,
	contentType string,
) ([]byte, int, error) {
	reqCtx, cancel := context.WithTimeout(ctx, a.allocateTimeout)
	defer cancel()

	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(reqCtx, method, url, rdr)
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Accept", "application/json")
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

// sanitizeLabelValue 把 game_mode 收敛成合法 k8s label value(≤63 字符,首尾字母数字,
// 中间允许 -_.);非法字符替换为 '-',空值 / 全非法值返回 "unknown"。
func sanitizeLabelValue(s string) string {
	if s == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	out := b.String()
	if len(out) > 63 {
		out = out[:63]
	}
	out = strings.Trim(out, "-_.")
	if out == "" {
		return "unknown"
	}
	return out
}

// truncate 截断 body 给错误信息用,避免日志过长。
func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
