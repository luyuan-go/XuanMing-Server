// sweep_fairness_test.go — INC-20260724-001:sweep 队头公平性 + 单轮墙钟预算。
//
// 事故背景:k8s/Agones 控制面持续 context deadline exceeded 时,allocation_uncertain /
// preactive_releasing / abort 这类「永久墓碑」项永远收敛不了,而 RangeStaleBattles 是按
// active ZSET 分数升序的全量扫描、它们的分数不变 ⇒ 恒排队头,串行吃掉整轮时间,
// 把队尾 abandoned 对局的 §9.4 心跳超时补偿无限推后(事故当天控制面超时约 40s 即此形态)。
//
// 修复:进入分支工作**之前**先把这些状态的 score 推到当前时刻(让出队头),
// 并给单轮加墙钟预算。修复前 TestSweepDefersStuckReconcileStatesOffQueueHead 必失败。
package biz

import (
	"context"
	"testing"
	"time"

	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
)

const activeZSetKey = "pandora:ds:active"

// 卡住的墓碑项必须在本轮就让出队头(score 前移),否则它每轮都排在最前面饿死其它项。
//
// 用 legacy(非 modelB)usecase:此时这些分支是只读跳过的,记录内容不会变,
// 唯一应该发生的可观察变化就是「让出队头」——判据干净,无需 stub 分配器。
func TestSweepDefersStuckReconcileStatesOffQueueHead(t *testing.T) {
	ctx := context.Background()
	for _, state := range []string{
		stateAllocationUncertain,
		stateAllocationReconciling,
		stateAllocationEmptyFence,
		statePreactiveReleasing,
		stateAllocationAbort,
	} {
		t.Run(state, func(t *testing.T) {
			uc, repo, mr := newUsecaseWithAlloc(t, NewMockGameServerAllocator(testCfg()))
			const matchID = uint64(90001)
			if err := repo.CreateBattle(ctx, &dsv1.BattleStorageRecord{
				MatchId: matchID, State: state, AllocationId: "stuck-allocation",
				PlayerIds: []uint64{1, 2},
			}, time.Minute); err != nil {
				t.Fatalf("seed battle: %v", err)
			}
			// score=0 ⇒ 永远排在 RangeStaleBattles 结果的最前面。
			if _, err := mr.ZAdd(activeZSetKey, 0, "90001"); err != nil {
				t.Fatal(err)
			}

			if err := uc.sweepOnce(ctx); err != nil {
				t.Fatalf("sweepOnce: %v", err)
			}

			scores, err := mr.ZMembers(activeZSetKey)
			if err != nil || len(scores) == 0 {
				t.Fatalf("卡住的墓碑项不应被移出 active(它还没收敛): members=%v err=%v", scores, err)
			}
			score, err := mr.ZScore(activeZSetKey, "90001")
			if err != nil {
				t.Fatalf("ZScore: %v", err)
			}
			if score <= 0 {
				t.Fatalf("state=%s 的项 score 仍为 %v,没有让出队头 —— 它会每轮霸占队头饿死 §9.4 补偿", state, score)
			}
		})
	}
}

// 边界:非「卡住墓碑」状态不受本次改动影响,分数不被无故前移
// (尤其 abandoned 补偿的最后一棒必须保持最高优先级重试)。
func TestSweepDoesNotDeferAbandonedCompensation(t *testing.T) {
	if stuckReconcileState(stateAbandoned) {
		t.Fatal("abandoned 不应被列入退避集合:它是 §9.4 补偿的最后一棒,必须保持最高优先级")
	}
	if stuckReconcileState(stateAllocating) {
		t.Fatal("allocating 不应被列入退避集合")
	}
	if stuckReconcileState(stateEnded) {
		t.Fatal("ended 不应被列入退避集合")
	}
}

// 单轮墙钟预算必须来自既有 sweep_interval 配置,不新增配置项;未配置时有安全默认值。
func TestSweepRoundBudgetFromExistingInterval(t *testing.T) {
	uc, _ := newUsecase(t)
	got := uc.sweepRoundBudget()
	want := uc.cfg.SweepInterval.Std()
	if want > 0 && got != want {
		t.Fatalf("sweepRoundBudget = %v, want 既有 sweep_interval %v", got, want)
	}
	if got <= 0 {
		t.Fatalf("sweepRoundBudget = %v, 必须为正(否则每轮只推进一项)", got)
	}
}
