// queue_admission_test.go —— StartMatch 队列准入上限回归(压测审核【必修-4】,2026-07-26)。
//
// 覆盖:队列达上限 → ERR_RATE_LIMITED 拒绝且零副作用(不建 start operation);
// 队列未满 → 正常受理;上限 <=0 → 不限(联调兼容)。
package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"

	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
)

// fillQueue 经正常 StartMatch → saga 推进,把 n 张单人票据真实入队。
func fillQueue(t *testing.T, f *fixture, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		ticketID := uint64(7000 + i)
		playerID := uint64(8000 + i)
		if _, err := f.uc.StartMatch(ctx, ticketID, ticketID, playerID, 0, entryUnset); err != nil {
			t.Fatalf("seed StartMatch #%d: %v", i, err)
		}
	}
	// 推进 start saga:票据主体 + claim + ZADD 入队。
	if err := f.uc.advanceStartOperationsOnce(ctx); err != nil {
		t.Fatalf("advance start operations: %v", err)
	}
}

func TestStartMatch_QueueAdmissionRejectsWhenFull(t *testing.T) {
	f := newFixtureWith(t, 9000, func(c *conf.MatchConf) {
		c.MaxQueueTickets = 2
	})
	ctx := context.Background()
	fillQueue(t, f, 2)

	qlen, err := f.repo.QueueLen(ctx)
	if err != nil || qlen != 2 {
		t.Fatalf("queue len = %d err=%v, want 2", qlen, err)
	}

	// 队列已满:新 StartMatch 必须被准入门拒绝,且不留 start operation(零副作用)。
	_, serr := f.uc.StartMatch(ctx, 7100, 7100, 8100, 0, entryUnset)
	if errcode.As(serr) != errcode.ErrRateLimited {
		t.Fatalf("want ErrRateLimited, got %v", serr)
	}
	if _, found, gerr := f.repo.GetStartOperation(ctx, 7100); gerr != nil || found {
		t.Fatalf("rejected StartMatch must not create start operation: found=%v err=%v", found, gerr)
	}
}

func TestStartMatch_QueueAdmissionAllowsBelowCap(t *testing.T) {
	f := newFixtureWith(t, 9000, func(c *conf.MatchConf) {
		c.MaxQueueTickets = 3
	})
	ctx := context.Background()
	fillQueue(t, f, 2)

	if _, err := f.uc.StartMatch(ctx, 7100, 7100, 8100, 0, entryUnset); err != nil {
		t.Fatalf("below-cap StartMatch must pass, got %v", err)
	}
}

func TestStartMatch_QueueAdmissionDisabled(t *testing.T) {
	f := newFixtureWith(t, 9000, func(c *conf.MatchConf) {
		c.MaxQueueTickets = -1 // 显式关闭(联调)
	})
	ctx := context.Background()
	fillQueue(t, f, 2)

	if _, err := f.uc.StartMatch(ctx, 7100, 7100, 8100, 0, entryUnset); err != nil {
		t.Fatalf("disabled cap must not throttle, got %v", err)
	}
}
