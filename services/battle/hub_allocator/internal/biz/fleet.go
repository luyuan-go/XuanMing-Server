// fleet.go — Hub DS Fleet 分片拓扑抽象 + W4 ⑤ Mock 实现。
//
// W4+ 接 Agones:实现一个 AgonesHubFleetProvider,查 Agones Fleet/GameServer 列表
// (label region=...),把 Ready 的 GameServer 映射成 ShardCandidate。本接口签名保持不变,
// 只换实现 + main 装配,biz 逻辑零改动。
package biz

import (
	"context"
	"fmt"

	"github.com/luyuancpp/pandora/pkg/releasetrack"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/conf"
)

// ShardCandidate 是一个候选大厅 DS 分片(拓扑信息,不含实时负载)。
// 实时 player_count / state 以 Redis 分片镜像为准。
type ShardCandidate struct {
	PodName  string
	Addr     string
	Region   string
	ShardID  uint32
	Capacity int32
	// ReleaseTrack 必须来自实际 GameServer metadata；本地/mock 固定 stable。
	ReleaseTrack string
	// TokenReady 表示该分片的 DS 回调令牌已就绪(enforce 下签发/续期成功)。
	// false 仅出现在 agones + enforce 且令牌签发/patch 失败时:此分片虽在 Fleet 里 ready,但其
	// DS 回调会被守卫全拒 —— 拓扑对账据此不把它当可用镜像(不新建 ready、清理已有 ready 镜像),
	// 避免 AssignHub 仍分配到一个回调打不回来的 Hub(审核 P1)。mock / local / off / permissive 恒 true。
	TokenReady bool
	// TokenExpMs 是该分片当前 DS 回调令牌的 exp(unix ms;annotation 镜像)。0 = 未知/未启用。
	// 【仅供拓扑续期判定;不再当代际】重签后 exp 必变,但 exp 为秒精度、同秒重签会碰撞(审核 P1-6),
	// 代际标识改用 TokenGen。
	TokenExpMs int64
	// TokenGen 是该分片当前 DS 回调令牌的「代际」(Redis INCR 权威、独立、单调;annotation 镜像)。
	// 0 = 未知/未启用(off/permissive/mock/local-未签发)。每次重签由签发器经 Redis INCR 领取严格
	// 递增的 gen 并签进令牌,拓扑对账据此复位分片为 warming 并记录新代际,心跳侧只有携带 gen 精确
	// 相等的已验签令牌才能翻回 ready(替代 TokenExpMs 秒级代际,消除同秒重签碰撞,审核 P1-6/P1-8)。
	TokenGen uint64
	// InstanceUID / ProtocolEpoch 是该候选分片背后的 **exact DS 实例身份**(§9.22 四元组的前两项)。
	// 仅 mode=local 播种:本机 Hub DS 是本进程 exec 出来的,身份(进程级 UUID + protocol_epoch=1)
	// 在拉起时就已确定且全程不变,拓扑对账把它写进分片镜像 gameserver_uid / auth_epoch,
	// 供 legacy 归属定案拼出完整 owner 目标(否则 owner 权威里永远没有记录,见 hub.go
	// ownerTargetForHubTicket 注释)。
	// agones 留空:线上实例身份由 Model B 授权记录 promote 后投影,不能由拓扑发现抢先写。
	// mock 留空:没有真实实例,不允许伪造身份。
	InstanceUID   string
	ProtocolEpoch uint32
}

// HubFleetProvider 返回某 region 的候选分片拓扑(W4 ⑤ Mock / W4+ Agones Fleet)。
type HubFleetProvider interface {
	// ListShards 返回 region 下的全部候选分片(静态拓扑,不含实时负载)。
	ListShards(ctx context.Context, region string) ([]ShardCandidate, error)
}

// HubInstanceObservation is a physical-liveness observation, deliberately
// separate from ShardCandidate.  A GameServer can be Scheduled, Unhealthy, or
// temporarily lack a callback credential while its process/Pod still exists;
// those states make it unroutable but are not teardown proof.
type HubInstanceObservation struct {
	GameServerFound       bool
	GameServerUID         string
	PodFound              bool
	PodOwnerGameServerUID string
}

// ProvesTeardown returns true only when the exact expected GameServer UID can
// no longer own either the GameServer object or its Pod.  Unknown/malformed
// ownership is fail-closed.
func (o HubInstanceObservation) ProvesTeardown(expectedGameServerUID string) bool {
	if expectedGameServerUID == "" {
		return false
	}
	if o.GameServerFound && o.GameServerUID == expectedGameServerUID {
		return false
	}
	if o.PodFound {
		return o.GameServerFound && o.GameServerUID != "" &&
			o.GameServerUID != expectedGameServerUID &&
			o.PodOwnerGameServerUID == o.GameServerUID
	}
	return !o.GameServerFound || o.GameServerUID != expectedGameServerUID
}

// HubFleetPhysicalObserver is an optional production capability.  Reconcile
// may use it to mint an exact UID teardown proof, but absence from
// HubFleetProvider.ListShards alone must never be treated as physical death.
type HubFleetPhysicalObserver interface {
	ObserveShardInstance(ctx context.Context, pod string) (HubInstanceObservation, error)
}

// HubFleetScaler 是 Hub Fleet 副本扩缩容能力(可选)。
//
// **仅真 Agones provider(AgonesHubFleetProvider)实现**。MockHubFleetProvider 刻意不实现本接口:
// 否则它会提供退化语义(Get 返回固定假副本数、Set 是 no-op),让 HubUsecase.autoScaleEnabled()
// 在 Mock 下误以为“可扩缩容”——每轮 reconcile 都跑但实际不变,还会对假分片/假玩家跑 consolidation。
// 故 autoscale/consolidation 仅在接真 Agones(agones.enabled=true)时生效。
type HubFleetScaler interface {
	GetFleetReplicas(ctx context.Context) (int32, error)
	SetFleetReplicas(ctx context.Context, replicas int32) error
}

// MockHubFleetProvider 是 W4 ⑤ 的打桩实现:不连 k8s,按 region 生成 MockShardCount 个确定性假分片。
//
// pod   = pandora-hub-<region>-<i>(i 从 1 起)
// addr  = MockHubAddrHost:(MockHubPortBase + i)
// 保证同 region 多次列举分片拓扑稳定(实时负载在 Redis,这里只给拓扑)。
//
// **拓扑-only:不实现 HubFleetScaler**。故 Mock 模式下 scaler==nil → autoScaleEnabled()==false,
// 自动扩缩容/强制整合都不会跑(避免退化 no-op 误导)。
type MockHubFleetProvider struct {
	cfg conf.HubConf
}

// NewMockHubFleetProvider 构造 Mock 分片拓扑提供者。
func NewMockHubFleetProvider(cfg conf.HubConf) *MockHubFleetProvider {
	return &MockHubFleetProvider{cfg: cfg}
}

// ListShards 返回 region 的确定性假分片拓扑。
func (m *MockHubFleetProvider) ListShards(_ context.Context, region string) ([]ShardCandidate, error) {
	out := make([]ShardCandidate, 0, m.cfg.MockShardCount)
	for i := 1; i <= m.cfg.MockShardCount; i++ {
		out = append(out, ShardCandidate{
			PodName:      fmt.Sprintf("pandora-hub-%s-%d", region, i),
			Addr:         fmt.Sprintf("%s:%d", m.cfg.MockHubAddrHost, m.cfg.MockHubPortBase+i),
			Region:       region,
			ShardID:      uint32(i),
			Capacity:     m.cfg.DefaultCapacity,
			ReleaseTrack: releasetrack.Stable,
			TokenReady:   true,
		})
	}
	return out, nil
}
