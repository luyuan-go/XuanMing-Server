// allocation_parallel_test.go —— DS 分配有界并发回归(压测审核【必修-3】,2026-07-26)。
//
// 用「屏障分配器」确定性证明并发:AllocateBattle 阻塞到 N 路同时在途才放行。
// 串行实现(旧行为)第一路会等满超时而失败;并发实现三路同时进入屏障,全部成功。
// 不依赖计时断言,无 flake。
package biz

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"

	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/model"
)

// barrierAllocator 在 AllocateBattle 里等到 want 路并发同时在途才放行;超时报错。
type barrierAllocator struct {
	StubDSAllocator
	want    int32
	arrived atomic.Int32
	release chan struct{}
	once    atomic.Bool
}

func newBarrierAllocator(want int32) *barrierAllocator {
	return &barrierAllocator{
		StubDSAllocator: StubDSAllocator{MockAddr: "127.0.0.1:7777"},
		want:            want,
		release:         make(chan struct{}),
	}
}

func (b *barrierAllocator) AllocateBattle(ctx context.Context, matchID uint64, playerIDs []uint64, mapID uint32) (*model.BattleAllocation, error) {
	if b.arrived.Add(1) >= b.want && b.once.CompareAndSwap(false, true) {
		close(b.release)
	}
	select {
	case <-b.release:
		return b.StubDSAllocator.AllocateBattle(ctx, matchID, playerIDs, mapID)
	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("barrier timeout: only %d/%d allocations concurrent (serial regression?)",
			b.arrived.Load(), b.want)
	}
}

// seedAllocatingMatchN 造一个独立的 ALLOCATING match(票据/玩家 ID 按 base 区分,互不冲突)。
func seedAllocatingMatchN(t *testing.T, ctx context.Context, f *fixture, matchID, base uint64) {
	t.Helper()
	ticketA, ticketB := base, base+100
	playersA := []uint64{base + 1, base + 2, base + 3, base + 4, base + 5}
	playersB := []uint64{base + 6, base + 7, base + 8, base + 9, base + 10}
	f.seedTicket(t, ctx, ticketA, playersA, 1000)
	f.seedTicket(t, ctx, ticketB, playersB, 1000)
	ta, _, _ := f.repo.GetTicket(ctx, ticketA)
	tb, _, _ := f.repo.GetTicket(ctx, ticketB)
	members := make([]*matchv1.MatchMemberStorageRecord, 0, 10)
	for _, m := range ta.Members {
		members = append(members, &matchv1.MatchMemberStorageRecord{PlayerId: m.PlayerId, TeamId: m.TeamId, Side: 0, Confirm: confirmAccepted})
	}
	for _, m := range tb.Members {
		members = append(members, &matchv1.MatchMemberStorageRecord{PlayerId: m.PlayerId, TeamId: m.TeamId, Side: 1, Confirm: confirmAccepted})
	}
	deadlineMs := time.Now().Add(time.Minute).UnixMilli()
	match := &matchv1.MatchStorageRecord{
		MatchId:           matchID,
		Stage:             stageAllocating,
		Members:           members,
		TicketIds:         []uint64{ticketA, ticketB},
		CreatedAtMs:       deadlineMs - 15000,
		ConfirmDeadlineMs: deadlineMs,
	}
	ta.MatchId = matchID
	tb.MatchId = matchID
	if err := f.repo.ReserveTicket(ctx, ta, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatalf("reserve ticket %d: %v", ticketA, err)
	}
	if err := f.repo.ReserveTicket(ctx, tb, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatalf("reserve ticket %d: %v", ticketB, err)
	}
	if err := f.repo.CreateMatch(ctx, match, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatalf("create match %d: %v", matchID, err)
	}
}

func TestAdvanceAllocations_RunConcurrently(t *testing.T) {
	ctx := context.Background()
	f := newFixtureWith(t, 999, func(c *conf.MatchConf) {
		c.AllocationWorkers = 3
	})
	allocator := newBarrierAllocator(3)
	f.uc.allocator = allocator
	// fixture 用旧 cfg 构造过一次,直接替换 usecase 的信号量以匹配 workers=3。
	// (newFixtureWith 已把 cfg 传进 NewMatchUsecase,此行防御未来 fixture 改动。)
	if cap(f.uc.allocSem) != 3 {
		t.Fatalf("allocSem cap = %d, want 3", cap(f.uc.allocSem))
	}

	seedAllocatingMatchN(t, ctx, f, 9951, 30000)
	seedAllocatingMatchN(t, ctx, f, 9952, 31000)
	seedAllocatingMatchN(t, ctx, f, 9953, 32000)

	if err := f.uc.advanceAllocationsOnce(ctx); err != nil {
		t.Fatalf("parallel allocations failed: %v", err)
	}
	for _, mid := range []uint64{9951, 9952, 9953} {
		m, found, gerr := f.repo.GetMatch(ctx, mid)
		if gerr != nil || !found || m.GetStage() != stageReady {
			t.Fatalf("match %d not READY after parallel allocation: found=%v stage=%v err=%v",
				mid, found, m.GetStage(), gerr)
		}
	}
}
