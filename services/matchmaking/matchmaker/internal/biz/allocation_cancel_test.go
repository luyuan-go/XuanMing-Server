package biz

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/placement"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/model"
)

// INC-20260724-001 回归:ALLOCATING 期玩家出口(§9.20 禁止"按钮不可用 / 只能杀进程恢复")。
//
// 事故背景:DS 缺容 + k8s 控制面超时时分配长时间重试,而 expireOnce 对 stageAllocating
// 显式 keepActive 永不判失败;此前唯一会终止 ALLOCATING 的是成局最终门的 presence 误杀,
// 该门已因本事故关闭 ⇒ 必须补一个 **未 checkpoint 才允许** 的取消出口,否则误杀变永久卡死。
//
// 修复前:ConfirmMatch 对 stageAllocating 的 reject 一律 ErrInvalidState,
// TestConfirmMatchRejectDuringUncheckpointedAllocatingCancels 必失败。

// seedUncheckpointedAllocating 造一个「已 ALLOCATING、尚未 checkpoint 任何 battle target」
// 的 canonical match(阶段值显式给定,因为 seedAllocatingMatch 留空 = UNSPECIFIED)。
func seedUncheckpointedAllocating(
	t *testing.T,
	ctx context.Context,
	f *fixture,
	matchID uint64,
	phase matchv1.MatchAllocationPhase,
) {
	t.Helper()
	seedAllocatingMatch(t, ctx, f, matchID, time.Now().Add(time.Minute).UnixMilli())
	if err := f.repo.UpdateMatchWithLock(ctx, matchID, f.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			rec.AllocationOperationId = allocationOperationID()
			rec.AllocationPhase = phase
			return nil
		}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatalf("seed allocation phase %v: %v", phase, err)
	}
}

func raceBattleAllocation(tag string) *model.BattleAllocation {
	return &model.BattleAllocation{
		Address: "10.0.0.9:7777",
		Target: placement.Target{
			PodName: "battle-" + tag, InstanceUID: "uid-" + tag, InstanceEpoch: 3,
			AllocationID: "allocation-" + tag, ReleaseTrack: "stable",
		},
	}
}

// V2-1(修复前失败):未 checkpoint 的 ALLOCATING 允许玩家取消;取消者票据按主动取消语义
// 判责删除,其余成员票据无过错退回队列并续 claim —— 等待中的队友不能被连坐删票。
func TestConfirmMatchRejectDuringUncheckpointedAllocatingCancels(t *testing.T) {
	for _, phase := range []matchv1.MatchAllocationPhase{
		matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_PENDING,
		matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING,
	} {
		t.Run(phase.String(), func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t, 7001)
			seedUncheckpointedAllocating(t, ctx, f, 7001, phase)

			if err := f.uc.ConfirmMatch(ctx, 1, 7001, false); err != nil {
				t.Fatalf("cancel during uncheckpointed ALLOCATING(%v): %v", phase, err)
			}

			m, found, err := f.repo.GetMatch(ctx, 7001)
			if err != nil || !found {
				t.Fatalf("get match after cancel: found=%v err=%v", found, err)
			}
			if m.GetStage() != stageFailed {
				t.Fatalf("stage = %v, want FAILED", m.GetStage())
			}
			if m.GetAllocationPhase() != matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_FAILED {
				t.Fatalf("alloc phase = %v, want FAILED", m.GetAllocationPhase())
			}
			if m.GetBattleTarget() != nil {
				t.Fatalf("cancel must not leave a battle target: %+v", m.GetBattleTarget())
			}

			// 取消者(player 1)所在票据 100 判责删除 + 归属释放 → 可立刻重新匹配,不撞 4002。
			if _, found, err := f.repo.GetTicket(ctx, 100); err != nil || found {
				t.Fatalf("canceller ticket 100 still present: found=%v err=%v", found, err)
			}
			for pid := uint64(1); pid <= 5; pid++ {
				if _, found, err := f.repo.GetPlayerTicket(ctx, pid); err != nil || found {
					t.Fatalf("canceller side player %d claim not released: found=%v err=%v", pid, found, err)
				}
			}

			// 其余成员(票据 200)无过错:退回队列(MatchId 清零)且归属保留。
			tb, found, err := f.repo.GetTicket(ctx, 200)
			if err != nil || !found {
				t.Fatalf("bystander ticket 200 must be requeued, not deleted: found=%v err=%v", found, err)
			}
			if tb.GetMatchId() != 0 {
				t.Fatalf("bystander ticket 200 match_id = %d, want 0 (requeued)", tb.GetMatchId())
			}
			for pid := uint64(6); pid <= 10; pid++ {
				tid, found, err := f.repo.GetPlayerTicket(ctx, pid)
				if err != nil || !found || tid != 200 {
					t.Fatalf("bystander player %d claim lost: ticket=%d found=%v err=%v", pid, tid, found, err)
				}
			}
		})
	}
}

// V2-2:已 checkpoint exact target 后必须维持既有拒绝语义 —— 那时 DS 已固化 / 票据可能已签,
// 假装取消会让客户端与随后的 READY 推送打架(既有注释写明的理由),边界不得被本次修复误伤。
func TestConfirmMatchRejectAfterCheckpointStillRejected(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 7002)
	seedUncheckpointedAllocating(t, ctx, f, 7002,
		matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING)

	allocation := raceBattleAllocation("checkpointed")
	if err := f.repo.UpdateMatchWithLock(ctx, 7002, f.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			rec.BattleTarget = battleTargetStorage(allocation)
			return nil
		}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatalf("checkpoint target: %v", err)
	}

	err := f.uc.ConfirmMatch(ctx, 1, 7002, false)
	if errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("reject after checkpoint = %v, want ErrInvalidState", err)
	}
	m, found, gerr := f.repo.GetMatch(ctx, 7002)
	if gerr != nil || !found || m.GetStage() != stageAllocating {
		t.Fatalf("checkpointed match must stay ALLOCATING: stage=%v found=%v err=%v",
			m.GetStage(), found, gerr)
	}
	assertAllocationOwnershipIntact(t, ctx, f, 7002)
}

// ABORTING(abort fence 在途)与未知阶段一律 fail-closed:不确定就不放行(§9.22)。
func TestConfirmMatchRejectDuringAbortingOrUnknownPhaseRejected(t *testing.T) {
	for _, phase := range []matchv1.MatchAllocationPhase{
		matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING,
		matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_UNSPECIFIED,
	} {
		t.Run(phase.String(), func(t *testing.T) {
			ctx := context.Background()
			f := newFixture(t, 7003)
			seedUncheckpointedAllocating(t, ctx, f, 7003, phase)

			if err := f.uc.ConfirmMatch(ctx, 1, 7003, false); errcode.As(err) != errcode.ErrInvalidState {
				t.Fatalf("reject at phase %v = %v, want ErrInvalidState", phase, err)
			}
			m, found, gerr := f.repo.GetMatch(ctx, 7003)
			if gerr != nil || !found || m.GetStage() != stageAllocating {
				t.Fatalf("match must stay ALLOCATING: stage=%v found=%v err=%v",
					m.GetStage(), found, gerr)
			}
			assertAllocationOwnershipIntact(t, ctx, f, 7003)
		})
	}
}

// V2-3:取消与 checkpoint 互斥 —— 二者只能有一个成功,且不得出现「已 FAILED 却挂着 target」
// 这种撕裂状态(§16.1 并发原子性)。
func TestConfirmMatchCancelAndCheckpointAreMutuallyExclusive(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 7004)
	seedUncheckpointedAllocating(t, ctx, f, 7004,
		matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING)

	job, found, err := f.repo.GetMatch(ctx, 7004)
	if err != nil || !found {
		t.Fatalf("read allocation job: found=%v err=%v", found, err)
	}
	allocation := raceBattleAllocation("cancel-race")

	var (
		wg            sync.WaitGroup
		cancelErr     error
		checkpointErr error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		cancelErr = f.uc.ConfirmMatch(ctx, 1, 7004, false)
	}()
	go func() {
		defer wg.Done()
		_, checkpointErr = f.uc.checkpointBattleAllocation(ctx, job, allocation)
	}()
	wg.Wait()

	if cancelErr == nil && checkpointErr == nil {
		t.Fatal("cancel and checkpoint both succeeded — allocation fence broken")
	}
	if cancelErr != nil && checkpointErr != nil {
		t.Fatalf("cancel and checkpoint both failed: cancel=%v checkpoint=%v", cancelErr, checkpointErr)
	}

	m, found, gerr := f.repo.GetMatch(ctx, 7004)
	if gerr != nil || !found {
		t.Fatalf("get match after race: found=%v err=%v", found, gerr)
	}
	if cancelErr == nil {
		// 取消赢:必须是干净的 FAILED,绝不能留下 target(否则等于遗弃一台已交付 DS)。
		if m.GetStage() != stageFailed || m.GetBattleTarget() != nil {
			t.Fatalf("cancel won but state torn: stage=%v target=%+v", m.GetStage(), m.GetBattleTarget())
		}
		return
	}
	// checkpoint 赢:match 仍是 ALLOCATING 且持有 exact target,取消必须已被拒绝。
	if m.GetStage() != stageAllocating || m.GetBattleTarget() == nil {
		t.Fatalf("checkpoint won but state torn: stage=%v target=%+v", m.GetStage(), m.GetBattleTarget())
	}
	if errcode.As(cancelErr) != errcode.ErrInvalidState {
		t.Fatalf("losing cancel = %v, want ErrInvalidState", cancelErr)
	}
}
