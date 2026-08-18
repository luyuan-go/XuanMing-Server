// login_ratelimit_test.go —— 登录失败 Quota 回归(anti-abuse §6 第 4 项验收):
// 连续失败触发、成功登录不计数、锁定期过后自动恢复、Redis 故障 fail-open、
// IP 维度跨账号生效(依赖 Envoy 受信头,空 IP 自动退化为仅账号维度)。
package biz

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/snowflake"
	"github.com/luyuancpp/pandora/services/account/login/internal/data"
)

// newQuotaUsecase 构造带失败配额的最小 Login 用例(hubAssigner=nil 自签回退)。
// limit=3 便于测试;window/lock 由各测试指定。
func newQuotaUsecase(t *testing.T, window, lock time.Duration) (*LoginUsecase, *miniredis.Miniredis) {
	t.Helper()
	cfg := auth.Config{Secret: []byte(testSecret)}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	repo := &fakeAccountRepo{playerID: 42, passwordHash: mustBcrypt(t, "pw")}
	sf := snowflake.NewNode(1)
	uc := NewLoginUsecase(repo, newFakeSessionRepo(), nil, nil, nil, sf, "127.0.0.1:7777", "cn", signer, verifier, nil, false, false, nil, false)

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	uc.SetLoginRateLimiter(data.NewRedisLoginRateLimiter(rdb, 3, window, lock))
	return uc, mr
}

func TestLogin_FailQuotaLocksThenAutoRecovers(t *testing.T) {
	uc, mr := newQuotaUsecase(t, 15*time.Minute, 5*time.Minute)
	ctx := context.Background()

	// 连续 3 次密码错:每次都拿到密码错(未提前锁),第 3 次布锁。
	for i := 0; i < 3; i++ {
		if _, err := uc.Login(ctx, "acc", "wrong", "dev", false); errcode.As(err) != errcode.ErrLoginPasswordMismatch {
			t.Fatalf("attempt #%d want password mismatch, got %v", i+1, err)
		}
	}
	// 锁窗内:即使密码正确也被拒,错误信息带可见倒计时。
	_, err := uc.Login(ctx, "acc", "pw", "dev", false)
	if errcode.As(err) != errcode.ErrRateLimited || !strings.Contains(err.Error(), "retry after") {
		t.Fatalf("locked login want ErrRateLimited with countdown, got %v", err)
	}
	// 锁定期过后自动恢复(PX 自过期,无需人工解锁)。
	mr.FastForward(5*time.Minute + time.Millisecond)
	if res, err := uc.Login(ctx, "acc", "pw", "dev", false); err != nil || res == nil {
		t.Fatalf("login after lock expiry = (%v, %v), want success", res, err)
	}
}

func TestLogin_SuccessDoesNotCount(t *testing.T) {
	uc, _ := newQuotaUsecase(t, 15*time.Minute, 5*time.Minute)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if _, err := uc.Login(ctx, "acc", "wrong", "dev", false); errcode.As(err) != errcode.ErrLoginPasswordMismatch {
			t.Fatalf("fail #%d: %v", i+1, err)
		}
	}
	// 成功登录不计数(计数停在 2,未触发锁)。
	if _, err := uc.Login(ctx, "acc", "pw", "dev", false); err != nil {
		t.Fatalf("success at count=2 must pass: %v", err)
	}
	// 第 3 次失败才触发锁。
	if _, err := uc.Login(ctx, "acc", "wrong", "dev", false); errcode.As(err) != errcode.ErrLoginPasswordMismatch {
		t.Fatalf("fail #3: %v", err)
	}
	if _, err := uc.Login(ctx, "acc", "pw", "dev", false); errcode.As(err) != errcode.ErrRateLimited {
		t.Fatalf("after 3 failures want locked, got %v", err)
	}
}

func TestLogin_IPDimensionLocksAcrossAccounts(t *testing.T) {
	uc, _ := newQuotaUsecase(t, 15*time.Minute, 5*time.Minute)
	ipCtx := WithClientIP(context.Background(), "203.0.113.9")

	// 同 IP 换着账号名撞库:3 次失败后 IP 被锁,换任何账号(即使密码正确)都被拒。
	for i, acct := range []string{"a1", "a2", "a3"} {
		if _, err := uc.Login(ipCtx, acct, "wrong", "dev", false); errcode.As(err) != errcode.ErrLoginPasswordMismatch {
			t.Fatalf("fail #%d: %v", i+1, err)
		}
	}
	if _, err := uc.Login(ipCtx, "a9", "pw", "dev", false); errcode.As(err) != errcode.ErrRateLimited {
		t.Fatalf("same-IP login after 3 failures want ErrRateLimited, got %v", err)
	}
	// 其它 IP / 无 IP(不同来源)不受该 IP 锁波及;账号维度也未达限(每账号各 1 次)。
	if _, err := uc.Login(context.Background(), "a9", "pw", "dev", false); err != nil {
		t.Fatalf("other-source login must pass, got %v", err)
	}
}

// TestLogin_FailQuotaCaseVariantsShareCounter:大小写/尾空格变体必须共享同一账号维度计数,
// 否则撞库可用 alice/Alice/ALICE 各享独立预算绕过账号锁(账号权威是大小写不敏感 collation)。
func TestLogin_FailQuotaCaseVariantsShareCounter(t *testing.T) {
	uc, _ := newQuotaUsecase(t, 15*time.Minute, 5*time.Minute)
	ctx := context.Background()

	// 对同一真实账号用 3 个变体各失败一次 → 归一化后共享计数 → 第 3 次即达限布锁。
	for _, variant := range []string{"acc", "ACC", " Acc "} {
		if _, err := uc.Login(ctx, variant, "wrong", "dev", false); errcode.As(err) != errcode.ErrLoginPasswordMismatch {
			t.Fatalf("variant %q want password mismatch, got %v", variant, err)
		}
	}
	// 任一变体(含原文)此时都应被锁——账号维度未被大小写绕过。
	if _, err := uc.Login(ctx, "acc", "pw", "dev", false); errcode.As(err) != errcode.ErrRateLimited {
		t.Fatalf("after 3 case-variant failures account must be locked, got %v", err)
	}
}

// TestLogin_LockResetsCounterSoSingleFailureDoesNotRelock:锁到期后单次失败不得立即重锁
// (计数窗 > 锁窗时的续锁攻击);锁时清零计数,恢复后需重新攒满 limit 次。
func TestLogin_LockResetsCounterSoSingleFailureDoesNotRelock(t *testing.T) {
	uc, mr := newQuotaUsecase(t, 15*time.Minute, 5*time.Minute) // window 15m > lock 5m
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := uc.Login(ctx, "acc", "wrong", "dev", false); errcode.As(err) != errcode.ErrLoginPasswordMismatch {
			t.Fatalf("fail #%d: %v", i+1, err)
		}
	}
	// 锁到期。
	mr.FastForward(5*time.Minute + time.Millisecond)
	// 单次失败不应立即重锁(计数已在布锁时清零,现在只是第 1 次)。
	if _, err := uc.Login(ctx, "acc", "wrong", "dev", false); errcode.As(err) != errcode.ErrLoginPasswordMismatch {
		t.Fatalf("single post-lock failure should be plain mismatch, got %v", err)
	}
	// 正确密码此刻必须能进(未被单次失败重锁)。
	if _, err := uc.Login(ctx, "acc", "pw", "dev", false); err != nil {
		t.Fatalf("must not be re-locked by a single post-expiry failure, got %v", err)
	}
}

func TestLogin_FailQuotaFailOpenOnRedisError(t *testing.T) {
	cfg := auth.Config{Secret: []byte(testSecret)}
	signer, _ := auth.NewSigner(cfg)
	verifier, _ := auth.NewVerifier(cfg)
	repo := &fakeAccountRepo{playerID: 42, passwordHash: mustBcrypt(t, "pw")}
	sf := snowflake.NewNode(1)
	uc := NewLoginUsecase(repo, newFakeSessionRepo(), nil, nil, nil, sf, "127.0.0.1:7777", "cn", signer, verifier, nil, false, false, nil, false)

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	mr.Close()
	uc.SetLoginRateLimiter(data.NewRedisLoginRateLimiter(rdb, 3, 15*time.Minute, 5*time.Minute))

	ctx := context.Background()
	// Redis 故障:密码错仍返回业务错(不被配额挡),密码对正常登录(fail-open,§2 铁律)。
	if _, err := uc.Login(ctx, "acc", "wrong", "dev", false); errcode.As(err) != errcode.ErrLoginPasswordMismatch {
		t.Fatalf("redis down wrong pw want mismatch, got %v", err)
	}
	if _, err := uc.Login(ctx, "acc", "pw", "dev", false); err != nil {
		t.Fatalf("redis down correct pw must pass, got %v", err)
	}
}
