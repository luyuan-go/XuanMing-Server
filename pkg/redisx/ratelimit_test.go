// ratelimit_test.go —— 限流原语契约测试(anti-abuse-scene-entry.md §6 第 1 项验收):
// 窗口内拒第二次、窗口后放行、error 时 fail-open 返回 allow。
package redisx

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newRL(t *testing.T) (*miniredis.Miniredis, redis.UniversalClient) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

// brokenClient 返回一个指向已关闭 miniredis 的客户端,用来注入 Redis 故障。
func brokenClient(t *testing.T) redis.UniversalClient {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	mr.Close()
	return rdb
}

func TestCooldownRejectsSecondWithinWindow(t *testing.T) {
	mr, rdb := newRL(t)
	ctx := context.Background()
	key := RLKey("match", "start", 1001)

	if ok, err := Cooldown(ctx, rdb, key, 3*time.Second); err != nil || !ok {
		t.Fatalf("first acquire = (%v, %v), want (true, nil)", ok, err)
	}
	if ok, err := Cooldown(ctx, rdb, key, 3*time.Second); err != nil || ok {
		t.Fatalf("second acquire within window = (%v, %v), want (false, nil)", ok, err)
	}
	// 窗口过后必须重新放行(PX 自过期,无后台清理)。
	mr.FastForward(3*time.Second + time.Millisecond)
	if ok, err := Cooldown(ctx, rdb, key, 3*time.Second); err != nil || !ok {
		t.Fatalf("acquire after window = (%v, %v), want (true, nil)", ok, err)
	}
}

func TestCooldownDisabledByNonPositiveWindow(t *testing.T) {
	_, rdb := newRL(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if ok, err := Cooldown(ctx, rdb, RLKey("x", "y", 1), 0); err != nil || !ok {
			t.Fatalf("window=0 must always allow, got (%v, %v)", ok, err)
		}
	}
}

func TestCooldownFailOpenOnRedisError(t *testing.T) {
	rdb := brokenClient(t)
	ok, err := Cooldown(context.Background(), rdb, RLKey("x", "y", 1), time.Second)
	if !ok {
		t.Fatal("redis error must fail-open (allow=true)")
	}
	if err == nil {
		t.Fatal("redis error must be surfaced for the caller to Warn")
	}
}

func TestClearCooldownReleasesWindow(t *testing.T) {
	_, rdb := newRL(t)
	ctx := context.Background()
	key := RLKey("match", "start", 1002)

	if ok, _ := Cooldown(ctx, rdb, key, time.Minute); !ok {
		t.Fatal("first acquire must pass")
	}
	if err := ClearCooldown(ctx, rdb, key); err != nil {
		t.Fatalf("clear: %v", err)
	}
	// 失败释放后必须立即可重试(§9.20:业务失败不得让玩家白等一个冷却窗)。
	if ok, err := Cooldown(ctx, rdb, key, time.Minute); err != nil || !ok {
		t.Fatalf("acquire after clear = (%v, %v), want (true, nil)", ok, err)
	}
}

func TestQuotaAllowsUpToLimitThenRejects(t *testing.T) {
	mr, rdb := newRL(t)
	ctx := context.Background()
	key := RLKey("friend", "request", 2001)

	for i := 1; i <= 2; i++ {
		if ok, err := Quota(ctx, rdb, key, 2, time.Minute); err != nil || !ok {
			t.Fatalf("request #%d = (%v, %v), want allow", i, ok, err)
		}
	}
	if ok, err := Quota(ctx, rdb, key, 2, time.Minute); err != nil || ok {
		t.Fatalf("request #3 = (%v, %v), want reject", ok, err)
	}
	// 窗口过后计数归零。
	mr.FastForward(time.Minute + time.Millisecond)
	if ok, err := Quota(ctx, rdb, key, 2, time.Minute); err != nil || !ok {
		t.Fatalf("request after window = (%v, %v), want allow", ok, err)
	}
}

func TestQuotaDisabledByNonPositiveLimitOrWindow(t *testing.T) {
	_, rdb := newRL(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if ok, err := Quota(ctx, rdb, RLKeyString("login", "fail", "ip1"), 0, time.Minute); err != nil || !ok {
			t.Fatalf("limit=0 must always allow, got (%v, %v)", ok, err)
		}
		if ok, err := Quota(ctx, rdb, RLKeyString("login", "fail", "ip1"), 3, 0); err != nil || !ok {
			t.Fatalf("window=0 must always allow, got (%v, %v)", ok, err)
		}
	}
}

func TestQuotaFailOpenOnRedisError(t *testing.T) {
	rdb := brokenClient(t)
	ok, err := Quota(context.Background(), rdb, RLKey("x", "y", 1), 1, time.Second)
	if !ok {
		t.Fatal("redis error must fail-open (allow=true)")
	}
	if err == nil {
		t.Fatal("redis error must be surfaced")
	}
}

func TestIncrWindowCountsAtomicallyAndExpires(t *testing.T) {
	mr, rdb := newRL(t)
	ctx := context.Background()
	key := RLKey("match", "noshow", 3001)

	for want := int64(1); want <= 3; want++ {
		n, err := IncrWindow(ctx, rdb, key, 10*time.Minute)
		if err != nil || n != want {
			t.Fatalf("incr = (%d, %v), want (%d, nil)", n, err, want)
		}
	}
	// 窗口起点是首次计数:过窗后从 1 重来。
	mr.FastForward(10*time.Minute + time.Millisecond)
	if n, err := IncrWindow(ctx, rdb, key, 10*time.Minute); err != nil || n != 1 {
		t.Fatalf("incr after window = (%d, %v), want (1, nil)", n, err)
	}
}

func TestIncrWindowKeyAlwaysHasTTL(t *testing.T) {
	mr, rdb := newRL(t)
	ctx := context.Background()
	key := RLKey("match", "noshow", 3002)

	if _, err := IncrWindow(ctx, rdb, key, time.Minute); err != nil {
		t.Fatal(err)
	}
	// 内存有界契约:计数键必须自带过期,绝不允许留下永久键。
	if ttl := mr.TTL(key); ttl <= 0 || ttl > time.Minute {
		t.Fatalf("counter key TTL = %v, want (0, 1m]", ttl)
	}
}

func TestArmPenaltyAndRemaining(t *testing.T) {
	mr, rdb := newRL(t)
	ctx := context.Background()
	key := RLKey("match", "noshowcd", 4001)

	// 未布罚时剩余 0。
	if d, err := PenaltyRemaining(ctx, rdb, key); err != nil || d != 0 {
		t.Fatalf("remaining before arm = (%v, %v), want (0, nil)", d, err)
	}
	if err := ArmPenalty(ctx, rdb, key, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if d, err := PenaltyRemaining(ctx, rdb, key); err != nil || d <= 0 || d > 30*time.Second {
		t.Fatalf("remaining after arm = (%v, %v), want (0, 30s]", d, err)
	}
	// 覆盖布设:新罚顶替旧罚剩余(60s > 原 30s)。
	if err := ArmPenalty(ctx, rdb, key, time.Minute); err != nil {
		t.Fatal(err)
	}
	if d, _ := PenaltyRemaining(ctx, rdb, key); d <= 30*time.Second {
		t.Fatalf("re-arm must extend, remaining = %v", d)
	}
	mr.FastForward(time.Minute + time.Millisecond)
	if d, err := PenaltyRemaining(ctx, rdb, key); err != nil || d != 0 {
		t.Fatalf("remaining after expiry = (%v, %v), want (0, nil)", d, err)
	}
}

func TestPenaltyFailOpenOnRedisError(t *testing.T) {
	rdb := brokenClient(t)
	d, err := PenaltyRemaining(context.Background(), rdb, RLKey("x", "y", 1))
	if d != 0 {
		t.Fatal("redis error must report 0 remaining (caller fail-opens)")
	}
	if err == nil {
		t.Fatal("redis error must be surfaced")
	}
}

func TestActionQuotaAllowRejectAndFailOpen(t *testing.T) {
	mr, rdb := newRL(t)
	ctx := context.Background()
	q := &ActionQuota{RDB: rdb, Domain: "friend", Limit: 2, Window: time.Minute}

	for i := 1; i <= 2; i++ {
		if ok, err := q.Allow(ctx, "request", 501); err != nil || !ok {
			t.Fatalf("action #%d = (%v, %v), want allow", i, ok, err)
		}
	}
	if ok, err := q.Allow(ctx, "request", 501); err != nil || ok {
		t.Fatalf("action #3 = (%v, %v), want reject", ok, err)
	}
	// 动作维度独立:同玩家其它动作不受影响。
	if ok, err := q.Allow(ctx, "block", 501); err != nil || !ok {
		t.Fatalf("other action = (%v, %v), want allow", ok, err)
	}
	mr.FastForward(time.Minute + time.Millisecond)
	if ok, err := q.Allow(ctx, "request", 501); err != nil || !ok {
		t.Fatalf("after window = (%v, %v), want allow", ok, err)
	}

	broken := &ActionQuota{RDB: brokenClient(t), Domain: "friend", Limit: 1, Window: time.Minute}
	if ok, err := broken.Allow(ctx, "request", 501); !ok || err == nil {
		t.Fatalf("redis error must fail-open with surfaced err, got (%v, %v)", ok, err)
	}
}

func TestRLKeyShapes(t *testing.T) {
	if got := RLKey("match", "start", 1234567); got != "pandora:rl:match:start:1234567" {
		t.Fatalf("RLKey = %q", got)
	}
	if got := RLKeyString("login", "fail", "1.2.3.4"); got != "pandora:rl:login:fail:1.2.3.4" {
		t.Fatalf("RLKeyString = %q", got)
	}
}
