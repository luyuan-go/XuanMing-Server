// gameserver.go — DS pod 分配抽象 + W4 ② Mock 实现。
//
// 真 Agones 实现见 internal/data/agones_allocator.go(W4 ⑫ AgonesGameServerAllocator,
// 经 k8s apiserver REST 调 allocation.agones.dev/v1 GameServerAllocation)。Mock / Local /
// Agones 都回传实际 release track，避免 canary 容量回退后把意图误当成终态。
package biz

import (
	"context"
	"fmt"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// GameServerAllocator 向底层编排(W4 ② Mock / W4 ③ Agones)申请/释放一个战斗 DS pod。
type GameServerAllocator interface {
	// Allocate 申请一个战斗 DS,返回 pod 名、地址和编排层实际命中的发布轨。
	Allocate(ctx context.Context, matchID uint64, mapID uint32, gameMode, releaseTrack string) (podName, addr, actualReleaseTrack string, err error)
	// Release 释放(回收)一个战斗 DS pod。
	Release(ctx context.Context, podName string) error
}

// AuthoritativeGameServerAllocator 是 Agones Model B 的额外能力：分配时先取得实例 UID/RV，
// Redis stage 成功后再用 UID+RV 条件 PATCH 投递 annotation。K8s 仅是投递镜像，不是授权权威。
type AuthoritativeGameServerAllocator interface {
	AllocateAuthoritative(
		ctx context.Context,
		matchID uint64,
		allocationID string,
		playerIDs []uint64,
		combatFactionByPlayer map[uint64]uint32,
		mapID uint32,
		gameMode, releaseTrack string,
	) (*data.AuthoritativeGameServerAllocation, error)
	DeliverCredential(
		ctx context.Context,
		allocation *data.AuthoritativeGameServerAllocation,
		annotations map[string]string,
	) (confirmedResourceVersion string, err error)
	// ResolveExpectedPodUID is the rolling-upgrade preflight for legacy Redis
	// battle records created before pod_uid was persisted.  It must use an
	// exact GameServer GET (name + UID + allocation_id binding) followed by an
	// owned Pod GET and return only that Pod's UID.  Missing/recreated objects
	// are an error: callers must never guess an identity before delete.
	ResolveExpectedPodUID(
		ctx context.Context,
		allocation *data.AuthoritativeGameServerAllocation,
	) (podUID string, err error)
	// ReleaseExpected 用 Kubernetes UID delete precondition 回收本实例；同名 GameServer 已重建
	// 时必须失败且零删除，禁止旧 cleanup 按名字误杀新实例。
	ReleaseExpected(ctx context.Context, allocation *data.AuthoritativeGameServerAllocation) error
}

// WarmingInstanceProber 是 warming 冷加载宽限期 sweep 的可选加速能力(advisory):
// 只读探测 exact 实例(GameServer name+UID,可选关联 Pod UID)是否已被编排层权威
// 确认死亡(物理消失或 Agones 依据 SDK health ping 判 Unhealthy)。返回 (true, nil)
// 时 sweep 才允许放弃 ready_wait 时间宽限,提前交 AbandonIfStale 事务判弃;任何
// error 都必须回退时间界。不实现本接口的分配器(local/mock)只按时间界回收。
type WarmingInstanceProber interface {
	ProbeExpectedInstanceGone(ctx context.Context, podName, instanceUID, podUID string) (bool, error)
}

// 生产 Agones 分配器必须具备探测能力;接线断裂在编译期暴露,不靠运行时静默降级。
var _ WarmingInstanceProber = (*data.AgonesGameServerAllocator)(nil)

// UncertainGameServerAllocationResolver is the read-only reconciliation
// capability used after a GameServerAllocation POST result is unknown.  It
// must never issue another allocation.  The implementation resolves the
// immutable allocation_id label and returns either one fully verified
// GameServer+Pod identity, authoritative absence, or an error for every
// unknown/ambiguous result.
//
// It is deliberately an optional interface instead of another method on
// AuthoritativeGameServerAllocator: older rolling-version writers and test
// doubles that do not understand reconciliation remain fail-closed on the
// permanent allocation_uncertain fence.
type UncertainGameServerAllocationResolver interface {
	ResolveAllocationByID(
		ctx context.Context,
		matchID uint64,
		allocationID string,
		playerIDs []uint64,
		combatFactionByPlayer map[uint64]uint32,
		mapID uint32,
		gameMode string,
	) (allocation *data.AuthoritativeGameServerAllocation, found bool, err error)
}

// MockGameServerAllocator 是 W4 ② 的打桩实现:不连 k8s,按 match_id 计算确定性假地址。
//
// 端口 = MockDSPortBase + (matchID % MockDSPortRange),保证同 match 多次分配地址稳定
// (幂等场景下 biz 会先查镜像不重复 Allocate,这里只保证可复现)。
type MockGameServerAllocator struct {
	cfg conf.AllocatorConf
}

// NewMockGameServerAllocator 构造 Mock 分配器。
func NewMockGameServerAllocator(cfg conf.AllocatorConf) *MockGameServerAllocator {
	return &MockGameServerAllocator{cfg: cfg}
}

// Allocate 返回确定性假 pod / addr。
func (m *MockGameServerAllocator) Allocate(_ context.Context, matchID uint64, _ uint32, _, releaseTrack string) (string, string, string, error) {
	port := m.cfg.MockDSPortBase + int(matchID%uint64(m.cfg.MockDSPortRange))
	podName := fmt.Sprintf("pandora-battle-%d", matchID)
	addr := fmt.Sprintf("%s:%d", m.cfg.MockDSAddrHost, port)
	return podName, addr, releaseTrack, nil
}

// Release 对 Mock 无操作(无真实 pod 可回收)。
func (m *MockGameServerAllocator) Release(_ context.Context, _ string) error {
	return nil
}
