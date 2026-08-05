// legacy_heartbeat_owner_test.go — legacy 战斗心跳的 owner 接线回归(2026-08-04)。
//
// 背景:legacy 面心跳既不续 owner 实例租约、也不代提交在场玩家 Admit,于是玩家已 travel
// 进战斗 DS、连接也建好了,owner 记录却恒 PENDING、实例租约恒过期。login 的
// applyOwnerPlacement 只在「ADMITTED 且租约剩余 > 安全余量」才报 STABLE,客户端因此恒收到
// "post-travel owner target is still PENDING",撑到 30s deadline 弹兜底面板,进不去副本。
// 修复 = HeartbeatWithCensus 用战斗记录的 exact 实例身份续租 + 按真实在场名单 Admit。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// recordingLeaseRenewer 记录 owner 实例租约续写调用。
type recordingLeaseRenewer struct {
	calls int
	pod   string
	uid   string
	epoch uint32
	track string
}

func (r *recordingLeaseRenewer) RenewInstanceLease(_ context.Context,
	podName, instanceUID string, instanceEpoch uint32, releaseTrack string) error {
	r.calls++
	r.pod, r.uid, r.epoch, r.track = podName, instanceUID, instanceEpoch, releaseTrack
	return nil
}

// legacyOwnerFixture 装配一个已 ready 的本地面对局(带 exact 实例身份)。
func legacyOwnerFixture(t *testing.T, matchID uint64, playerIDs []uint64) (
	*AllocatorUsecase, *recordingLeaseRenewer, string,
) {
	t.Helper()
	cfg := testCfg()
	alloc := &localIdentityAllocator{
		MockGameServerAllocator: NewMockGameServerAllocator(cfg),
		uid:                     "uid-legacy-battle",
		epoch:                   1,
	}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)
	lease := &recordingLeaseRenewer{}
	uc.SetOwnerLeaseRenewer(lease, false)

	res := allocateReady(t, uc, repo, matchID, playerIDs, 7, "pve_coop")
	if res.GameserverUID == "" {
		t.Fatal("夹具前置:分配必须已回填实例身份")
	}
	return uc, lease, res.DSPodName
}

// legacy 心跳必须以战斗记录的 exact 实例身份续写 owner 实例租约。
func TestLegacyBattleHeartbeat_RenewsOwnerInstanceLease(t *testing.T) {
	const matchID uint64 = 91001
	uc, lease, pod := legacyOwnerFixture(t, matchID, []uint64{2001})
	before := lease.calls

	if _, err := uc.HeartbeatWithCensus(context.Background(), matchID, pod, 1, "running",
		time.Now().UnixMilli(), true, []uint64{2001}); err != nil {
		t.Fatalf("HeartbeatWithCensus err: %v", err)
	}
	if lease.calls <= before {
		t.Fatal("legacy 战斗心跳必须续写 owner 实例租约")
	}
	if lease.pod != pod || lease.uid != "uid-legacy-battle" || lease.epoch != 1 {
		t.Fatalf("续租身份必须取自战斗记录, got %+v", lease)
	}
}

// 未上报在场名单时不得代提交 Admit(不能拿 roster 替未到场玩家宣称已准入),
// 但租约仍须续写(实例确实在服务)。
func TestLegacyBattleHeartbeat_NoCensusStillRenewsLeaseOnly(t *testing.T) {
	const matchID uint64 = 91002
	uc, lease, pod := legacyOwnerFixture(t, matchID, []uint64{2002})
	admits := &countingOwnerAuthority{}
	uc.SetOwnerAuthority(admits)
	before := lease.calls

	if _, err := uc.HeartbeatWithCensus(context.Background(), matchID, pod, 1, "running",
		time.Now().UnixMilli(), false, nil); err != nil {
		t.Fatalf("HeartbeatWithCensus err: %v", err)
	}
	if lease.calls <= before {
		t.Fatal("无 census 时仍必须续写实例租约")
	}
	if admits.queries != 0 {
		t.Fatalf("无 census 时不得代提交 Admit, queries=%d", admits.queries)
	}
}

// 旧签名 Heartbeat 保持零 census 语义(既有调用方行为不变)。
func TestLegacyBattleHeartbeat_PlainSignatureKeepsNoCensus(t *testing.T) {
	const matchID uint64 = 91003
	uc, lease, pod := legacyOwnerFixture(t, matchID, []uint64{2003})
	admits := &countingOwnerAuthority{}
	uc.SetOwnerAuthority(admits)
	before := lease.calls

	if _, err := uc.Heartbeat(context.Background(), matchID, pod, 1, "running", time.Now().UnixMilli()); err != nil {
		t.Fatalf("Heartbeat err: %v", err)
	}
	if lease.calls <= before {
		t.Fatal("旧签名心跳同样必须续写实例租约")
	}
	if admits.queries != 0 {
		t.Fatalf("旧签名不带 census,不得 Admit, queries=%d", admits.queries)
	}
}

// countingOwnerAuthority 只计数,不产生真实归属副作用。
type countingOwnerAuthority struct {
	queries int
	admits  int
}

func (c *countingOwnerAuthority) QueryOwner(context.Context, uint64) (data.OwnerRecordView, error) {
	c.queries++
	return data.OwnerRecordView{}, nil
}

func (c *countingOwnerAuthority) BeginTransition(context.Context, uint64, uint64, string, int8,
	data.OwnerTargetView) (data.OwnerRecordView, error) {
	return data.OwnerRecordView{}, nil
}

func (c *countingOwnerAuthority) Admit(context.Context, uint64, uint64, string,
	data.OwnerTargetView) (int64, error) {
	c.admits++
	return 0, nil
}

func (c *countingOwnerAuthority) ReleaseOwner(context.Context, uint64, uint64, string) error {
	return nil
}
