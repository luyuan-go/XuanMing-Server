// requeue_guard_test.go —— 守卫退队回归(2026-08-10,封 failMatch/rollback 盲写复活竞态)。
// 核心:票据已被取消删除时 RequeueTicketIfOwned 必须 no-op(不复活进队列);
// 归属他局的票据不被窃取;正常归属本局的票据正确退队并重入 queue。
package data

import (
	"context"
	"testing"

	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
)

func seedReservedTicket(t *testing.T, repo *RedisMatchRepo, ticketID, matchID uint64) {
	t.Helper()
	ctx := context.Background()
	tk := &matchv1.MatchTicketStorageRecord{
		TicketId: ticketID,
		MatchId:  matchID,
		AvgMmr:   1000,
		Members:  []*matchv1.MatchMemberStorageRecord{{PlayerId: ticketID + 1}},
	}
	if err := repo.CreateTicketRecord(ctx, tk, testTTL); err != nil {
		t.Fatalf("seed ticket %d: %v", ticketID, err)
	}
}

func TestRequeueTicketIfOwned_GoneTicketNotResurrected(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepo(t)
	seedReservedTicket(t, repo, 100, 9000)

	// 模拟并发 CancelMatch 已删该票。
	if err := repo.DeleteTicket(ctx, 100); err != nil {
		t.Fatalf("delete: %v", err)
	}
	tk := &matchv1.MatchTicketStorageRecord{TicketId: 100, MatchId: 0}
	requeued, err := repo.RequeueTicketIfOwned(ctx, tk, 9000, testTTL)
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if requeued {
		t.Fatal("gone ticket must NOT be requeued (resurrection race)")
	}
	// 票据记录仍不存在,queue 里也没有它。
	if _, found, _ := repo.GetTicket(ctx, 100); found {
		t.Fatal("cancelled ticket record must stay deleted")
	}
	ids, _ := repo.RangeQueueTickets(ctx)
	for _, id := range ids {
		if id == 100 {
			t.Fatal("cancelled ticket must not reappear in queue")
		}
	}
}

func TestRequeueTicketIfOwned_ForeignMatchNotStolen(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepo(t)
	// 票据当前归属他局 9002。
	seedReservedTicket(t, repo, 101, 9002)

	tk := &matchv1.MatchTicketStorageRecord{TicketId: 101, MatchId: 0}
	// 本局 expected=9001,但存储态属 9002 → 不得窃取。
	requeued, err := repo.RequeueTicketIfOwned(ctx, tk, 9001, testTTL)
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if requeued {
		t.Fatal("ticket owned by another match must NOT be requeued")
	}
	got, found, _ := repo.GetTicket(ctx, 101)
	if !found || got.GetMatchId() != 9002 {
		t.Fatalf("foreign ticket mutated: found=%v match_id=%d", found, got.GetMatchId())
	}
}

func TestRequeueTicketIfOwned_OwnedTicketRequeued(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepo(t)
	seedReservedTicket(t, repo, 102, 9003)

	tk := &matchv1.MatchTicketStorageRecord{
		TicketId: 102, MatchId: 0, AvgMmr: 1000,
		Members: []*matchv1.MatchMemberStorageRecord{{PlayerId: 103}},
	}
	requeued, err := repo.RequeueTicketIfOwned(ctx, tk, 9003, testTTL)
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if !requeued {
		t.Fatal("ticket owned by this match must be requeued")
	}
	// match_id 清零、重入 queue。
	got, found, _ := repo.GetTicket(ctx, 102)
	if !found || got.GetMatchId() != 0 {
		t.Fatalf("requeued ticket match_id = %d, want 0", got.GetMatchId())
	}
	ids, _ := repo.RangeQueueTickets(ctx)
	var inQueue bool
	for _, id := range ids {
		if id == 102 {
			inQueue = true
		}
	}
	if !inQueue {
		t.Fatal("requeued ticket must be back in queue ZSET")
	}
}
