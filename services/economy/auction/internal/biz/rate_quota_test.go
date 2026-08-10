// rate_quota_test.go —— 挂单/出价/撤单频率配额回归(anti-abuse §6 第 6 项,验收同第 2 项)。
package biz

import (
	"context"
	"errors"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// fakeAuctionQuota 可编程频率配额。
type fakeAuctionQuota struct {
	allowed bool
	err     error
	actions []string
	lastPID uint64
}

func (f *fakeAuctionQuota) Allow(_ context.Context, action string, subject uint64) (bool, error) {
	f.actions = append(f.actions, action)
	f.lastPID = subject
	return f.allowed, f.err
}

func TestPlaceOrder_RateQuotaRejectsZeroSideEffect(t *testing.T) {
	uc, repo, ledger := newTestUsecase(t)
	quota := &fakeAuctionQuota{allowed: false}
	uc.SetRateQuota(quota)

	_, err := uc.PlaceOrder(context.Background(), 1, 100, 200, 10, 100, "rq1")
	if errcode.As(err) != errcode.ErrRateLimited {
		t.Fatalf("want ErrRateLimited, got %v", err)
	}
	if len(quota.actions) != 1 || quota.actions[0] != "order" || quota.lastPID != 1 {
		t.Fatalf("quota call mismatch: %+v", quota)
	}
	// 零副作用:未登记订单、未冻结资产。
	if len(repo.orders) != 0 {
		t.Fatalf("rejected PlaceOrder must not register order, got %d", len(repo.orders))
	}
	if len(ledger.freezes) != 0 {
		t.Fatalf("rejected PlaceOrder must not freeze assets, got %v", ledger.freezes)
	}
}

func TestCancelOrder_RateQuotaRejects(t *testing.T) {
	uc, _, _ := newTestUsecase(t)
	uc.SetRateQuota(&fakeAuctionQuota{allowed: true})
	ctx := context.Background()

	o, err := uc.PlaceOrder(ctx, 2, 100, 200, 10, 100, "rq2")
	if err != nil {
		t.Fatalf("place: %v", err)
	}
	uc.SetRateQuota(&fakeAuctionQuota{allowed: false})
	if cerr := uc.CancelOrder(ctx, 2, 100, o.GetOrderId()); errcode.As(cerr) != errcode.ErrRateLimited {
		t.Fatalf("cancel want ErrRateLimited, got %v", cerr)
	}
}

func TestAuctionRateQuota_FailOpen(t *testing.T) {
	uc, _, _ := newTestUsecase(t)
	uc.SetRateQuota(&fakeAuctionQuota{allowed: false, err: errors.New("redis down")})

	// 判定失败 fail-open 放行(§2 铁律)。
	if _, err := uc.PlaceOrder(context.Background(), 3, 100, 200, 10, 100, "rq3"); err != nil {
		t.Fatalf("quota error must fail-open, got %v", err)
	}
}
