// rate_quota_test.go —— 申请/邀请频率配额回归(anti-abuse §6 第 6 项,验收同第 2 项):
// 窗内超额拒绝且零副作用 / Redis 故障 fail-open / 窗过自动恢复。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/redisx"
)

// newRateQuota 起独立 miniredis 承载配额键(与队伍存储解耦,便于 FastForward)。
func newRateQuota(t *testing.T, limit int64) (*miniredis.Miniredis, *redisx.ActionQuota) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, &redisx.ActionQuota{RDB: rdb, Domain: "team", Limit: limit, Window: time.Minute}
}

func TestApplyToTeam_RateQuotaRejectsThenRecovers(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	mr, quota := newRateQuota(t, 2)
	uc.SetRateQuota(quota)
	ctx := context.Background()

	mustCreateTeam(t, uc, 9701, 7701)
	// 同一申请人连打:前 2 次在配额内(重复申请业务上幂等刷新),第 3 次被频率配额拒。
	for i := 0; i < 2; i++ {
		if _, _, _, err := uc.ApplyToTeam(ctx, 9701, 7801); err != nil {
			t.Fatalf("apply #%d within quota: %v", i+1, err)
		}
	}
	_, _, _, err := uc.ApplyToTeam(ctx, 9701, 7801)
	if errcode.As(err) != errcode.ErrRateLimited {
		t.Fatalf("apply #3 want ErrRateLimited, got %v", err)
	}
	// 窗口过后自动恢复。
	mr.FastForward(time.Minute + time.Millisecond)
	if _, _, _, err := uc.ApplyToTeam(ctx, 9701, 7801); errcode.As(err) == errcode.ErrRateLimited {
		t.Fatalf("apply after window must not be rate limited, got %v", err)
	}
}

func TestApplyToTeam_RateQuotaZeroSideEffectOnReject(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	_, quota := newRateQuota(t, 1)
	uc.SetRateQuota(quota)
	ctx := context.Background()

	mustCreateTeam(t, uc, 9702, 7702)
	if _, _, _, err := uc.ApplyToTeam(ctx, 9702, 7810); err != nil {
		t.Fatalf("apply #1: %v", err)
	}
	// 第 2 个申请人被频率配额拒(limit=1 按发起方计,此处换人故用同一人重申)。
	if _, _, _, err := uc.ApplyToTeam(ctx, 9702, 7810); errcode.As(err) != errcode.ErrRateLimited {
		t.Fatalf("apply #2 want ErrRateLimited, got %v", err)
	}
	// 零副作用:被拒的申请没有写入任何 pending 名额。
	apps, err := uc.ListTeamApplications(ctx, 9702, 7702)
	if err != nil {
		t.Fatalf("list applications: %v", err)
	}
	if len(apps) != 1 {
		t.Fatalf("rejected apply must not occupy pending slot, got %d entries", len(apps))
	}
}

func TestInvite_RateQuotaRejects(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	_, quota := newRateQuota(t, 1)
	uc.SetRateQuota(quota)
	ctx := context.Background()

	mustCreateTeam(t, uc, 9703, 7703)
	if _, err := uc.Invite(ctx, 501, 9703, 7703, 8801); err != nil {
		t.Fatalf("invite #1: %v", err)
	}
	if _, err := uc.Invite(ctx, 502, 9703, 7703, 8802); errcode.As(err) != errcode.ErrRateLimited {
		t.Fatalf("invite #2 want ErrRateLimited, got %v", err)
	}
}

func TestRateQuota_FailOpenOnRedisError(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	brokenMR := miniredis.RunT(t)
	brokenRdb := redis.NewClient(&redis.Options{Addr: brokenMR.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = brokenRdb.Close() })
	brokenMR.Close()
	uc.SetRateQuota(&redisx.ActionQuota{RDB: brokenRdb, Domain: "team", Limit: 1, Window: time.Minute})
	ctx := context.Background()

	mustCreateTeam(t, uc, 9704, 7704)
	// 配额 Redis 故障:连续申请全部放行(fail-open,§2 铁律)。
	for i := uint64(0); i < 3; i++ {
		if _, _, _, err := uc.ApplyToTeam(ctx, 9704, 7900+i); errcode.As(err) == errcode.ErrRateLimited {
			t.Fatalf("fail-open apply #%d must not be rate limited, got %v", i+1, err)
		}
	}
}
