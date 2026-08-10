// rate_quota_test.go —— 下单/撤单频率配额回归(anti-abuse §6 第 6 项,验收同第 2 项)。
package biz

import (
	"context"
	"errors"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// fakeTradeQuota 可编程频率配额。
type fakeTradeQuota struct {
	allowed bool
	err     error
	actions []string
	lastPID uint64
}

func (f *fakeTradeQuota) Allow(_ context.Context, action string, subject uint64) (bool, error) {
	f.actions = append(f.actions, action)
	f.lastPID = subject
	return f.allowed, f.err
}

func TestCreateOrder_RateQuotaRejectsZeroSideEffect(t *testing.T) {
	repo := newFakeRepo()
	uc, audit := newUC(repo, &fakeLedger{})
	quota := &fakeTradeQuota{allowed: false}
	uc.SetRateQuota(quota)

	_, err := uc.CreateOrder(context.Background(), 11, 22, items(), nil, 100)
	if errcode.As(err) != errcode.ErrRateLimited {
		t.Fatalf("want ErrRateLimited, got %v", err)
	}
	if len(quota.actions) != 1 || quota.actions[0] != "order" || quota.lastPID != 11 {
		t.Fatalf("quota call mismatch: %+v", quota)
	}
	// 零副作用:未落订单、未出审计。
	if len(repo.orders) != 0 || audit.count != 0 {
		t.Fatalf("rejected CreateOrder must be side-effect free: orders=%d audits=%d", len(repo.orders), audit.count)
	}
}

func TestCancelOrder_RateQuotaRejects(t *testing.T) {
	repo := newFakeRepo()
	uc, _ := newUC(repo, &fakeLedger{})
	uc.SetRateQuota(&fakeTradeQuota{allowed: true})

	orderID, err := uc.CreateOrder(context.Background(), 11, 22, items(), nil, 100)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	uc.SetRateQuota(&fakeTradeQuota{allowed: false})
	if cerr := uc.CancelOrder(context.Background(), 11, orderID); errcode.As(cerr) != errcode.ErrRateLimited {
		t.Fatalf("cancel want ErrRateLimited, got %v", cerr)
	}
}

func TestTradeRateQuota_FailOpen(t *testing.T) {
	repo := newFakeRepo()
	uc, _ := newUC(repo, &fakeLedger{})
	uc.SetRateQuota(&fakeTradeQuota{allowed: false, err: errors.New("redis down")})

	// 判定失败 fail-open 放行(§2 铁律)。
	if _, err := uc.CreateOrder(context.Background(), 11, 22, items(), nil, 100); err != nil {
		t.Fatalf("quota error must fail-open, got %v", err)
	}
}
