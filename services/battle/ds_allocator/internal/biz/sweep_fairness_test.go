// sweep_fairness_test.go — INC-20260724-001:sweep 队头公平性 + 单轮墙钟预算。
//
// 事故背景:k8s/Agones 控制面持续 context deadline exceeded 时,allocation_uncertain /
// preactive_releasing / abort 这类「永久墓碑」项永远收敛不了,而 RangeStaleBattles 是按
// active ZSET 分数升序的**全量**扫描、它们的分数不变 ⇒ 恒排队头,串行吃掉整轮时间,
// 把队尾 abandoned 对局的 §9.4 心跳超时补偿无限推后。
//
// 退避必须记在**进程内**,绝不能写 active ZSET score —— 本文件的 TestSweepMustNotRewrite*
// 两例就是这条的回归守卫(初版实现曾用 TouchActive 写 score,会同时踩三个坑:
// 破坏 score==0「立即对账」哨兵、把 last_heartbeat_ms 语义挪作调度时间戳、
// 以及借无条件 ZADD 把已 ZRem 的终态项复活回补偿 outbox)。
package biz

import (
	"context"
	"testing"
	"time"

	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
)

const activeZSetKey = "pandora:ds:active"

var stuckStates = []string{
	stateAllocationUncertain,
	stateAllocationReconciling,
	stateAllocationEmptyFence,
	statePreactiveReleasing,
	stateAllocationAbort,
}

func seedStuckBattle(t *testing.T, ctx context.Context, repo interface {
	CreateBattle(context.Context, *dsv1.BattleStorageRecord, time.Duration) error
}, matchID uint64, state string) {
	t.Helper()
	if err := repo.CreateBattle(ctx, &dsv1.BattleStorageRecord{
		MatchId: matchID, State: state, AllocationId: "stuck-allocation",
		PlayerIds: []uint64{1, 2},
	}, time.Minute); err != nil {
		t.Fatalf("seed battle: %v", err)
	}
}

// 卡住的墓碑项被扫过一轮后必须登记进程内退避,使下一轮让出队头。
func TestSweepRegistersInProcessDeferralForStuckStates(t *testing.T) {
	ctx := context.Background()
	for _, state := range stuckStates {
		t.Run(state, func(t *testing.T) {
			uc, repo, mr := newUsecaseWithAlloc(t, NewMockGameServerAllocator(testCfg()))
			const matchID = uint64(90001)
			seedStuckBattle(t, ctx, repo, matchID, state)
			// score=0 ⇒ 永远排在 RangeStaleBattles 结果最前面(同时也是「立即对账」哨兵)。
			if _, err := mr.ZAdd(activeZSetKey, 0, "90001"); err != nil {
				t.Fatal(err)
			}

			if err := uc.sweepOnce(ctx); err != nil {
				t.Fatalf("sweepOnce: %v", err)
			}

			d, ok := uc.sweepDeferUntil[matchID]
			if !ok {
				t.Fatalf("state=%s 未登记队头退避 —— 它会每轮霸占队头饿死 §9.4 补偿", state)
			}
			if d.state != state {
				t.Fatalf("退避记录的 state = %q, want %q(状态失效校验会失灵)", d.state, state)
			}
			now := time.Now()
			if !uc.sweepDeferralActive(matchID, state, now) {
				t.Fatalf("state=%s 登记后下一轮未让出队头", state)
			}
		})
	}
}

// 回归守卫(初版实现踩过):退避绝不能改写 active ZSET score。
// score==0 是 abort fence(data/battle_abort.go)与 auth quarantine(data/battle_auth.go)
// 用来要求「下一轮 sweep 立即对账/回收」的哨兵,data/battle_active_reconciler.go 专门用
// ZADD NX 保护它;改写它等于把高危态的立即对账降级成延迟对账。
func TestSweepMustNotRewriteActiveScoreSentinel(t *testing.T) {
	ctx := context.Background()
	for _, state := range stuckStates {
		t.Run(state, func(t *testing.T) {
			uc, repo, mr := newUsecaseWithAlloc(t, NewMockGameServerAllocator(testCfg()))
			const matchID = uint64(90002)
			seedStuckBattle(t, ctx, repo, matchID, state)
			if _, err := mr.ZAdd(activeZSetKey, 0, "90002"); err != nil {
				t.Fatal(err)
			}

			if err := uc.sweepOnce(ctx); err != nil {
				t.Fatalf("sweepOnce: %v", err)
			}

			score, err := mr.ZScore(activeZSetKey, "90002")
			if err != nil {
				t.Fatalf("ZScore: %v", err)
			}
			if score != 0 {
				t.Fatalf("state=%s 的 active score 被改写成 %v —— 破坏了 score==0「立即对账」哨兵;"+
					"退避必须记在进程内,不得写存储", state, score)
			}
		})
	}
}

// 状态一变立即作废退避:被并发 RPC 推进、进入 §9.4 补偿链、或 abort fence 要求立即对账时,
// 调度优化不得继续拖慢终态收敛。
func TestSweepDeferralInvalidatedOnStateChange(t *testing.T) {
	uc, _ := newUsecase(t)
	const matchID = uint64(90003)
	now := time.Now()
	uc.noteSweepDeferral(context.Background(), matchID, stateAllocationUncertain, now)

	if !uc.sweepDeferralActive(matchID, stateAllocationUncertain, now) {
		t.Fatal("同状态、未到期时应仍在退避")
	}
	if uc.sweepDeferralActive(matchID, stateAbandoned, now) {
		t.Fatal("状态已变(进入 §9.4 补偿链)必须立即作废退避")
	}
	if _, ok := uc.sweepDeferUntil[matchID]; ok {
		t.Fatal("作废后应同时清掉表项")
	}
}

// 退避有到期,且到期后立刻可再试;退避表不得随历史 match_id 无界增长(§9.18)。
func TestSweepDeferralExpiresAndPrunes(t *testing.T) {
	uc, _ := newUsecase(t)
	const matchID = uint64(90004)
	now := time.Now()
	uc.noteSweepDeferral(context.Background(), matchID, stateAllocationUncertain, now)

	later := now.Add(uc.cfg.HeartbeatTimeout.Std() + time.Second)
	if uc.sweepDeferralActive(matchID, stateAllocationUncertain, later) {
		t.Fatal("退避到期后必须立刻可再试")
	}

	uc.noteSweepDeferral(context.Background(), matchID, stateAllocationUncertain, now)
	uc.pruneSweepDeferrals(later)
	if len(uc.sweepDeferUntil) != 0 {
		t.Fatalf("到期项未被 prune 清掉,表会随历史 match_id 无界增长: %v", uc.sweepDeferUntil)
	}
}

// 边界:§9.4 补偿的最后一棒与零外部调用的状态不得被退避。
func TestSweepDoesNotDeferAbandonedCompensation(t *testing.T) {
	if stuckReconcileState(stateAbandoned) {
		t.Fatal("abandoned 不应被退避:它是 §9.4 补偿的最后一棒,必须保持最高优先级重试")
	}
	if stuckReconcileState(stateAllocating) {
		t.Fatal("allocating 不应被退避(纯 Redis DEL,零外部调用)")
	}
	if stuckReconcileState(stateEnded) {
		t.Fatal("ended 不应被退避(直接 RemoveActive)")
	}
}

// 单轮墙钟预算必须来自既有 sweep_interval 配置,不新增配置项。
func TestSweepRoundBudgetFromExistingInterval(t *testing.T) {
	uc, _ := newUsecase(t)
	got := uc.sweepRoundBudget()
	want := uc.cfg.SweepInterval.Std()
	if want > 0 && got != want {
		t.Fatalf("sweepRoundBudget = %v, want 既有 sweep_interval %v", got, want)
	}
	if got <= 0 {
		t.Fatalf("sweepRoundBudget = %v, 必须为正", got)
	}
}
