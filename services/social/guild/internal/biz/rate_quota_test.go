// rate_quota_test.go —— 入会申请频率配额回归(anti-abuse §6 第 6 项,验收同第 2 项)。
package biz

import (
	"context"
	"errors"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// fakeGuildQuota 可编程频率配额。
type fakeGuildQuota struct {
	allowed bool
	err     error
	calls   int
	lastPID uint64
}

func (f *fakeGuildQuota) Allow(_ context.Context, _ string, subject uint64) (bool, error) {
	f.calls++
	f.lastPID = subject
	return f.allowed, f.err
}

func TestApplyJoin_RateQuotaRejectsZeroSideEffect(t *testing.T) {
	repo := newFakeGuildRepo()
	pusher := &fakeGuildPusher{}
	uc := newGuildUC(repo, pusher)
	quota := &fakeGuildQuota{allowed: false}
	uc.SetRateQuota(quota)
	ctx := context.Background()

	if _, err := uc.CreateGuild(ctx, 900, "g", 501); err != nil {
		t.Fatalf("create guild: %v", err)
	}
	_, err := uc.ApplyJoin(ctx, 901, 501, 7001)
	if errcode.As(err) != errcode.ErrRateLimited {
		t.Fatalf("want ErrRateLimited, got %v", err)
	}
	if quota.calls != 1 || quota.lastPID != 901 {
		t.Fatalf("quota call mismatch: %+v", quota)
	}
	// 零副作用:未创建申请(会长视角列表为空)。
	reqs, _, lerr := uc.ListJoinRequests(ctx, 900, 0, 10)
	if lerr != nil || len(reqs) != 0 {
		t.Fatalf("rejected ApplyJoin must not create request: reqs=%d err=%v", len(reqs), lerr)
	}
}

func TestApplyJoin_RateQuotaAllowedAndFailOpen(t *testing.T) {
	repo := newFakeGuildRepo()
	pusher := &fakeGuildPusher{}
	uc := newGuildUC(repo, pusher)
	ctx := context.Background()

	if _, err := uc.CreateGuild(ctx, 910, "g2", 502); err != nil {
		t.Fatalf("create guild: %v", err)
	}
	uc.SetRateQuota(&fakeGuildQuota{allowed: true})
	if _, err := uc.ApplyJoin(ctx, 911, 502, 7002); err != nil {
		t.Fatalf("allowed ApplyJoin: %v", err)
	}
	// 判定失败 fail-open 放行(§2 铁律)。
	uc.SetRateQuota(&fakeGuildQuota{allowed: false, err: errors.New("redis down")})
	if _, err := uc.ApplyJoin(ctx, 912, 502, 7003); err != nil {
		t.Fatalf("quota error must fail-open, got %v", err)
	}
}
