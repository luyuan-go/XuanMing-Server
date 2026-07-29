// sweep_writer_lease_test.go — 心跳扫描的写者继任租约门(2026-07-29 事故闭环)。
//
// 被钉住的行为:
//   - 未注入租约 → 无条件扫描(单副本 Recreate 的历史形态,不得因本改动退化);
//   - 注入且未持有领导权 → **完全不触碰权威存储**,热备副本不产生第二个扫描者;
//   - 接任领导权 → 立即恢复扫描,不需要等待任何额外状态。
//
// 观察量选 allocator.releases:它是 sweepOnce 对外唯一的副作用计数,比断言 Redis
// 内部键更贴近"扫描是否真的发生"。
package biz

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// stubSweepLease 是可切换的领导权来源,满足 SweepWriterLease。
type stubSweepLease struct {
	held  atomic.Bool
	token atomic.Uint64
}

func (s *stubSweepLease) Current() (uint64, bool) { return s.token.Load(), s.held.Load() }

// primeTombstoneForSweep 构造一个"下一次 sweep 必定发出一次 release"的权威状态:
// POST 响应丢失 → 不确定分配 → 空 LIST 对账落成永久清理墓碑,墓碑在 active 索引里
// 每被扫到一次就再发一次幂等 release。返回可复位的 active 索引写入器。
func primeTombstoneForSweep(t *testing.T) (*AllocatorUsecase, *uncertainResolverTestAllocator, func()) {
	t.Helper()
	ctx := context.Background()
	const matchID = uint64(90731)
	allocator := &uncertainResolverTestAllocator{
		authoritativeTestAllocator: authoritativeTestAllocator{
			delivered:   make(chan map[string]string, 1),
			allocateErr: errors.New("GSA POST response lost"),
		},
		resolveFound: false,
	}
	uc, _, mr := newUsecaseWithAlloc(t, allocator)
	enableModelBForTest(t, uc, mr)
	uc.SetLifecyclePusher(&mockLifecycle{})

	if _, err := uc.AllocateBattle(ctx, matchID, []uint64{11, 12}, 1, "ranked"); err == nil {
		t.Fatal("lost POST response must fail the allocation")
	}
	readd := func() {
		t.Helper()
		if _, err := mr.ZAdd("pandora:ds:active", 0, "90731"); err != nil {
			t.Fatal(err)
		}
	}
	// 先跑一轮把不确定分配收敛成永久墓碑,后续每轮扫描只剩幂等 release。
	readd()
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("prime sweep: %v", err)
	}
	return uc, allocator, readd
}

func TestHeartbeatSweepTickWithoutLeaseAlwaysSweeps(t *testing.T) {
	ctx := context.Background()
	uc, allocator, readd := primeTombstoneForSweep(t)
	before := allocator.releases.Load()
	readd()
	uc.heartbeatSweepTick(ctx)
	if got := allocator.releases.Load(); got != before+1 {
		t.Fatalf("未注入租约时必须无条件扫描: releases %d → %d", before, got)
	}
}

func TestHeartbeatSweepTickSkipsWhileNotLeader(t *testing.T) {
	ctx := context.Background()
	uc, allocator, readd := primeTombstoneForSweep(t)
	lease := &stubSweepLease{}
	uc.SetSweepWriterLease(lease)

	// 热备:不持有领导权 → 一次副作用都不许发生。
	before := allocator.releases.Load()
	readd()
	uc.heartbeatSweepTick(ctx)
	if got := allocator.releases.Load(); got != before {
		t.Fatalf("热备副本不得执行扫描: releases %d → %d", before, got)
	}

	// 接任:立即恢复扫描,不需要额外状态或等待。
	lease.token.Store(7)
	lease.held.Store(true)
	uc.heartbeatSweepTick(ctx)
	if got := allocator.releases.Load(); got != before+1 {
		t.Fatalf("接任后必须立即恢复扫描: releases %d → %d", before, got)
	}

	// 让位:重新停手,且不残留"已扫描过一次"之类的状态。
	lease.held.Store(false)
	readd()
	uc.heartbeatSweepTick(ctx)
	if got := allocator.releases.Load(); got != before+1 {
		t.Fatalf("让位后必须重新停手: releases %d → %d", before+1, got)
	}
}

// TestRunHeartbeatSweepInitialPassRespectsLeadership 钉住启动首扫也必须过门:
// 竞选是异步的,启动瞬间通常尚未当选,无条件首扫会在滚动升级重叠窗口里制造并发扫描者。
func TestRunHeartbeatSweepInitialPassRespectsLeadership(t *testing.T) {
	ctx := context.Background()
	uc, allocator, readd := primeTombstoneForSweep(t)
	uc.SetSweepWriterLease(&stubSweepLease{})
	before := allocator.releases.Load()
	readd()
	if uc.sweepIsLeader(ctx) {
		t.Fatal("未当选时 sweepIsLeader 必须为 false")
	}
	if got := allocator.releases.Load(); got != before {
		t.Fatalf("领导权判定本身不得产生副作用: releases %d → %d", before, got)
	}
}
