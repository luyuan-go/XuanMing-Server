// rate_quota_test.go —— 好友申请频率配额回归(anti-abuse §6 第 6 项,验收同第 2 项)。
package biz

import (
	"context"
	"errors"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// fakeQuota 可编程频率配额:记录调用并按预设返回。
type fakeQuota struct {
	allowed bool
	err     error
	calls   int
	actions []string
	lastPID uint64
}

func (f *fakeQuota) Allow(_ context.Context, action string, subject uint64) (bool, error) {
	f.calls++
	f.actions = append(f.actions, action)
	f.lastPID = subject
	return f.allowed, f.err
}

func TestAddFriend_RateQuotaRejectsZeroSideEffect(t *testing.T) {
	repo := newFakeRepo()
	pusher := &fakePusher{}
	uc := newUC(repo, pusher, nil)
	quota := &fakeQuota{allowed: false}
	uc.SetRateQuota(quota)

	_, err := uc.AddFriend(context.Background(), 100, 200, 999)
	if errcode.As(err) != errcode.ErrRateLimited {
		t.Fatalf("want ErrRateLimited, got %v", err)
	}
	if quota.calls != 1 || quota.actions[0] != "request" || quota.lastPID != 100 {
		t.Fatalf("quota call mismatch: %+v", quota)
	}
	// 零副作用:未写申请、未推送。
	if len(pusher.events) != 0 {
		t.Fatalf("rejected AddFriend must not push, got %d events", len(pusher.events))
	}
	reqs, lerr := uc.ListFriendRequests(context.Background(), 200)
	if lerr != nil || len(reqs) != 0 {
		t.Fatalf("rejected AddFriend must not create request: reqs=%d err=%v", len(reqs), lerr)
	}
}

func TestAddFriend_RateQuotaAllowed(t *testing.T) {
	repo := newFakeRepo()
	pusher := &fakePusher{}
	uc := newUC(repo, pusher, nil)
	uc.SetRateQuota(&fakeQuota{allowed: true})

	reqID, err := uc.AddFriend(context.Background(), 100, 200, 999)
	if err != nil || reqID == 0 {
		t.Fatalf("allowed AddFriend = (%d, %v), want success", reqID, err)
	}
}

func TestAddFriend_RateQuotaFailOpen(t *testing.T) {
	repo := newFakeRepo()
	pusher := &fakePusher{}
	uc := newUC(repo, pusher, nil)
	uc.SetRateQuota(&fakeQuota{allowed: false, err: errors.New("redis down")})

	// 判定失败 fail-open 放行(§2 铁律)。
	if _, err := uc.AddFriend(context.Background(), 100, 200, 999); err != nil {
		t.Fatalf("quota error must fail-open, got %v", err)
	}
}
