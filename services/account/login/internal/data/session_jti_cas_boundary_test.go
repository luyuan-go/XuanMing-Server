package data

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// TestRedisSessionSetRetryAfterLostReplyIsIdempotent 钉住 go-redis 的默认命令
// 重试语义：第一次 Lua 已落地、只有应答丢失时，第二次相同 (jti,gen) 必须视为
// 同一次写的幂等成功，而不是误报 ErrSessionSuperseded。
func TestRedisSessionSetRetryAfterLostReplyIsIdempotent(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewRedisSessionRepo(rdb)
	ctx := context.Background()
	const playerID = uint64(7091)

	if n, err := setIfNewerGenScript.Run(ctx, rdb, []string{sessKey(playerID)},
		"token-A", "jti-A", "device-A", time.Now().Add(time.Hour).UnixMilli(), 11,
		time.Hour.Milliseconds()).Int64(); err != nil || n != 1 {
		t.Fatalf("seed first committed attempt: n=%d err=%v", n, err)
	}
	if err := repo.Set(ctx, playerID, "token-A", "jti-A", "device-A", time.Hour, 11); err != nil {
		t.Fatalf("same command retry must be idempotent success: %v", err)
	}
	got, err := rdb.HGetAll(ctx, sessKey(playerID)).Result()
	if err != nil || got["jti"] != "jti-A" || got["gen"] != "11" {
		t.Fatalf("idempotent retry changed authority: state=%+v err=%v", got, err)
	}
	for _, field := range []string{"_rollback_token", "_rollback_jti", "_rollback_device_id", "_rollback_exp_ms"} {
		if _, exists := got[field]; exists {
			t.Fatalf("acknowledged idempotent retry must clear rollback metadata %q: %+v", field, got)
		}
	}
}

func TestRedisSessionSetSameGenerationDifferentJTIIsRetryableConflict(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewRedisSessionRepo(rdb)
	ctx := context.Background()
	const playerID = uint64(7092)

	if err := repo.Set(ctx, playerID, "token-A", "jti-A", "device-A", time.Hour, 12); err != nil {
		t.Fatalf("seed session: %v", err)
	}
	err := repo.Set(ctx, playerID, "token-B", "jti-B", "device-B", time.Hour, 12)
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("same generation with another jti is an integrity conflict, got %v", err)
	}
	if got, found, getErr := repo.GetJTI(ctx, playerID); getErr != nil || !found || got != "jti-A" {
		t.Fatalf("conflict changed current session: jti=%q found=%v err=%v", got, found, getErr)
	}
}

func TestRedisSessionSetDependencyFailureIsRetryable(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{
		Addr: "127.0.0.1:1", DialTimeout: 10 * time.Millisecond,
		ReadTimeout: 10 * time.Millisecond, WriteTimeout: 10 * time.Millisecond,
		MaxRetries: -1,
	})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewRedisSessionRepo(rdb)
	for _, gen := range []uint64{0, 1} {
		err := repo.Set(context.Background(), 7093+gen, "token", "jti", "device", time.Hour, gen)
		if errcode.As(err) != errcode.ErrUnavailable {
			t.Fatalf("gen=%d dependency failure must be retryable ErrUnavailable, got %v", gen, err)
		}
	}
}

// TestRedisSessionDeleteIfJTIRejectsConcurrentLateLogoutAfterRotation 验证旧设备迟到的
// Logout 即使并发重试，也不能删除新设备刚轮换出的 session。
func TestRedisSessionDeleteIfJTIRejectsConcurrentLateLogoutAfterRotation(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	repo := NewRedisSessionRepo(rdb)
	ctx := context.Background()
	const playerID = uint64(7001)

	if err := repo.Set(ctx, playerID, "old-token", "old-jti", "old-device", time.Hour, 1); err != nil {
		t.Fatalf("set old session: %v", err)
	}
	if err := repo.Set(ctx, playerID, "new-token", "new-jti", "new-device", time.Hour, 2); err != nil {
		t.Fatalf("rotate session: %v", err)
	}

	const attempts = 32
	errCh := make(chan error, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func(attempt int) {
			defer wg.Done()
			deleted, err := repo.DeleteIfJTI(ctx, playerID, "old-jti")
			if err != nil {
				errCh <- fmt.Errorf("late logout attempt %d: %w", attempt, err)
				return
			}
			if deleted {
				errCh <- fmt.Errorf("late logout attempt %d deleted the rotated session", attempt)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	jti, found, err := repo.GetJTI(ctx, playerID)
	if err != nil || !found || jti != "new-jti" {
		t.Fatalf("rotated session changed after stale logout retries: jti=%q found=%v err=%v", jti, found, err)
	}

	deleted, err := repo.DeleteIfJTI(ctx, playerID, "new-jti")
	if err != nil || !deleted {
		t.Fatalf("current logout should delete exactly once: deleted=%v err=%v", deleted, err)
	}
	deleted, err = repo.DeleteIfJTI(ctx, playerID, "new-jti")
	if err != nil || deleted {
		t.Fatalf("replayed current logout should be idempotent: deleted=%v err=%v", deleted, err)
	}
	if jti, found, err = repo.GetJTI(ctx, playerID); err != nil || found || jti != "" {
		t.Fatalf("session should be absent after current logout: jti=%q found=%v err=%v", jti, found, err)
	}
}

// TestRedisSessionSetGenerationOrdering 验证并发 Login 定序(R7 收口):迟到的低代际
// 条件写必须被拒且零覆盖,两存储收敛到最高代际;dev(gen=0)保持无条件覆盖。
func TestRedisSessionSetGenerationOrdering(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	repo := NewRedisSessionRepo(rdb)
	ctx := context.Background()
	const playerID = uint64(7002)

	// P0-1 复现交错:B(gen2)先完成 Redis 写,A(gen1)迟到 → 必拒,会话仍是 B。
	if err := repo.Set(ctx, playerID, "token-B", "jti-B", "device-B", time.Hour, 2); err != nil {
		t.Fatalf("set gen2: %v", err)
	}
	err := repo.Set(ctx, playerID, "token-A", "jti-A", "device-A", time.Hour, 1)
	if err == nil {
		t.Fatal("late lower-generation write must be rejected")
	}
	if jti, found, gerr := repo.GetJTI(ctx, playerID); gerr != nil || !found || jti != "jti-B" {
		t.Fatalf("session must remain the highest generation: jti=%q found=%v err=%v", jti, found, gerr)
	}

	// 同代际重放同样拒(代际每登录唯一,相等 = 重放)。
	if err := repo.Set(ctx, playerID, "token-B2", "jti-B2", "device-B", time.Hour, 2); err == nil {
		t.Fatal("equal-generation replay must be rejected")
	}

	// 更高代际正常覆盖。
	if err := repo.Set(ctx, playerID, "token-C", "jti-C", "device-C", time.Hour, 3); err != nil {
		t.Fatalf("higher generation must overwrite: %v", err)
	}
	if jti, _, _ := repo.GetJTI(ctx, playerID); jti != "jti-C" {
		t.Fatalf("want jti-C after gen3 write, got %q", jti)
	}
	if exists, err := rdb.HExists(ctx, sessKey(playerID), "_rollback_jti").Result(); err != nil || exists {
		t.Fatalf("acknowledged Set must clear legacy rollback metadata: exists=%v err=%v", exists, err)
	}

	// dev 裸跑(gen=0):无条件覆盖,且清掉残留 gen 字段,后续 dev 登录不被误拒。
	if err := repo.Set(ctx, playerID, "token-D", "jti-D", "device-D", time.Hour, 0); err != nil {
		t.Fatalf("gen0 unconditional overwrite: %v", err)
	}
	if err := repo.Set(ctx, playerID, "token-E", "jti-E", "device-E", time.Hour, 0); err != nil {
		t.Fatalf("second gen0 overwrite must not be fenced: %v", err)
	}
	if jti, _, _ := repo.GetJTI(ctx, playerID); jti != "jti-E" {
		t.Fatalf("want jti-E after dev overwrites, got %q", jti)
	}
}

func TestRedisSessionFenceFailedSetClearsCapabilityAndKeepsGeneration(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewRedisSessionRepo(rdb)
	ctx := context.Background()
	const playerID = uint64(7003)

	if n, err := setIfNewerGenScript.Run(ctx, rdb, []string{sessKey(playerID)},
		"token-B", "jti-B", "device-B", time.Now().Add(time.Hour).UnixMilli(), 8,
		time.Hour.Milliseconds()).Int64(); err != nil || n != 1 {
		t.Fatalf("commit unacknowledged session: n=%d err=%v", n, err)
	}
	fenced, err := repo.FenceFailedSet(ctx, playerID, "jti-B", 8, time.Hour)
	if err != nil || !fenced {
		t.Fatalf("fence unacknowledged set: fenced=%v err=%v", fenced, err)
	}
	if jti, found, err := repo.GetJTI(ctx, playerID); err != nil || found || jti != "" {
		t.Fatalf("failed session still exposes capability: jti=%q found=%v err=%v", jti, found, err)
	}
	if gen, err := rdb.HGet(ctx, sessKey(playerID), "gen").Uint64(); err != nil || gen != 8 {
		t.Fatalf("failed fence generation=%d err=%v, want 8", gen, err)
	}
	if ttl := mr.TTL(sessKey(playerID)); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("fence TTL=%v, want bounded positive <=1h", ttl)
	}
}

func TestRedisSessionFenceFailedSetRejectsNonPersistentFence(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewRedisSessionRepo(rdb)
	ctx := context.Background()
	const playerID = uint64(7007)

	for _, tc := range []struct {
		name string
		jti  string
		gen  uint64
		ttl  time.Duration
	}{
		{name: "empty jti", jti: "", gen: 8, ttl: time.Hour},
		{name: "zero generation", jti: "jti-B", gen: 0, ttl: time.Hour},
		{name: "zero ttl", jti: "jti-B", gen: 8, ttl: 0},
		{name: "sub-millisecond ttl", jti: "jti-B", gen: 8, ttl: time.Microsecond},
		{name: "negative ttl", jti: "jti-B", gen: 8, ttl: -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fenced, err := repo.FenceFailedSet(ctx, playerID, tc.jti, tc.gen, tc.ttl)
			if errcode.As(err) != errcode.ErrInvalidArg || fenced {
				t.Fatalf("FenceFailedSet() fenced=%v err=%v, want false/ErrInvalidArg", fenced, err)
			}
			if exists := mr.Exists(sessKey(playerID)); exists {
				t.Fatal("invalid fence must not create an immediately-expiring generation key")
			}
		})
	}
}

func TestRedisSessionSetRejectsTTLThatCannotPersist(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewRedisSessionRepo(rdb)
	ctx := context.Background()
	const playerID = uint64(7008)

	for _, ttl := range []time.Duration{-time.Second, 0, time.Microsecond} {
		err := repo.Set(ctx, playerID, "token", "jti", "device", ttl, 8)
		if errcode.As(err) != errcode.ErrInvalidArg {
			t.Fatalf("Set(ttl=%s) err=%v, want ErrInvalidArg", ttl, err)
		}
		if exists := mr.Exists(sessKey(playerID)); exists {
			t.Fatalf("Set(ttl=%s) must not create an immediately-expiring session", ttl)
		}
	}
}

func TestRedisSessionFenceFailedSetNeverTouchesNewerWinner(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewRedisSessionRepo(rdb)
	ctx := context.Background()
	const playerID = uint64(7004)

	for _, tc := range []struct {
		token, jti, device string
		gen                uint64
	}{{"token-A", "jti-A", "device-A", 7}, {"token-C", "jti-C", "device-C", 9}} {
		if err := repo.Set(ctx, playerID, tc.token, tc.jti, tc.device, time.Hour, tc.gen); err != nil {
			t.Fatalf("set gen %d: %v", tc.gen, err)
		}
	}
	fenced, err := repo.FenceFailedSet(ctx, playerID, "jti-B", 8, time.Hour)
	if err != nil || fenced {
		t.Fatalf("stale compensation must no-op: fenced=%v err=%v", fenced, err)
	}
	if jti, found, err := repo.GetJTI(ctx, playerID); err != nil || !found || jti != "jti-C" {
		t.Fatalf("winner changed by stale compensation: jti=%q found=%v err=%v", jti, found, err)
	}
}

// C 的 Redis Set 可能完全没落地，此时 key 仍停在未交付的 B。补偿若只精确匹配
// (C,gen3) 会 no-op；按 <=failedGen fence 才能清掉 B 并阻止迟到低代际复活。
func TestRedisSessionFenceFailedSetClearsOlderUndeliveredCandidate(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewRedisSessionRepo(rdb)
	ctx := context.Background()
	const playerID = uint64(7005)

	if n, err := setIfNewerGenScript.Run(ctx, rdb, []string{sessKey(playerID)},
		"token-B", "jti-B", "device-B", time.Now().Add(time.Hour).UnixMilli(), 2,
		time.Hour.Milliseconds()).Int64(); err != nil || n != 1 {
		t.Fatalf("commit unacknowledged B: n=%d err=%v", n, err)
	}
	fenced, err := repo.FenceFailedSet(ctx, playerID, "jti-C", 3, time.Hour)
	if err != nil || !fenced {
		t.Fatalf("failed C must fence older B: fenced=%v err=%v", fenced, err)
	}
	if jti, found, err := repo.GetJTI(ctx, playerID); err != nil || found || jti != "" {
		t.Fatalf("older B survived failed C: jti=%q found=%v err=%v", jti, found, err)
	}
	if gen, err := rdb.HGet(ctx, sessKey(playerID), "gen").Uint64(); err != nil || gen != 3 {
		t.Fatalf("fence generation=%d err=%v, want 3", gen, err)
	}
}

// 常驻 P0 回归：A 已交付，B/C 都未交付。C 补偿后不得恢复即时前代 B；B 的迟到
// 补偿也不得降低 gen3 水位。下一次 D/gen4 能自愈。
func TestRedisSessionFailedABCInterleavingNeverRestoresUndeliveredB(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := NewRedisSessionRepo(rdb)
	ctx := context.Background()
	const playerID = uint64(7006)

	if err := repo.Set(ctx, playerID, "token-A", "jti-A", "device-A", time.Hour, 1); err != nil {
		t.Fatalf("deliver A: %v", err)
	}
	for _, x := range []struct {
		token, jti, device string
		gen                uint64
	}{{"token-B", "jti-B", "device-B", 2}, {"token-C", "jti-C", "device-C", 3}} {
		if n, err := setIfNewerGenScript.Run(ctx, rdb, []string{sessKey(playerID)},
			x.token, x.jti, x.device, time.Now().Add(time.Hour).UnixMilli(), x.gen,
			time.Hour.Milliseconds()).Int64(); err != nil || n != 1 {
			t.Fatalf("commit unacknowledged %s: n=%d err=%v", x.jti, n, err)
		}
	}
	if fenced, err := repo.FenceFailedSet(ctx, playerID, "jti-C", 3, time.Hour); err != nil || !fenced {
		t.Fatalf("fence C: fenced=%v err=%v", fenced, err)
	}
	if fenced, err := repo.FenceFailedSet(ctx, playerID, "jti-B", 2, time.Hour); err != nil || fenced {
		t.Fatalf("late B fence must not lower gen3: fenced=%v err=%v", fenced, err)
	}
	if jti, found, err := repo.GetJTI(ctx, playerID); err != nil || found || jti != "" {
		t.Fatalf("undelivered session restored: jti=%q found=%v err=%v", jti, found, err)
	}
	if err := repo.Set(ctx, playerID, "token-D", "jti-D", "device-D", time.Hour, 4); err != nil {
		t.Fatalf("newer retry D must self-heal: %v", err)
	}
	if jti, found, err := repo.GetJTI(ctx, playerID); err != nil || !found || jti != "jti-D" {
		t.Fatalf("D did not become current: jti=%q found=%v err=%v", jti, found, err)
	}
}
