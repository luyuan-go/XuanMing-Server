// realredis_test.go — 对着**真 Redis** 跑的调度队列语义验证(2026-08-06)。
//
// 其余用例跑在 miniredis 上,但本包的排期全押在 `ZADD` 的 `GT` / `XX` 修饰符上:
//   - GT 保证迟到 / 重复的离场事件只能把复查**往后推**,不能拉早(拉早=白查一轮);
//   - XX 保证已经出队的成员不会被 reschedule 重新塞回队列。
//
// miniredis 是 Go 仿真件,这些修饰符的语义不保证与真 Redis 一致,而判错的后果是
// 「把在线玩家踢出队伍」。仿真件绿灯不能当作真 Redis 绿灯。
//
// 用法(不设环境变量则整份跳过):
//
//	PANDORA_TEST_REDIS_ADDR=127.0.0.1:6380 go test ./pkg/offlinewatch/ -run RealRedis
package offlinewatch

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func newRealRedisWatcher(t *testing.T, reader PresenceReader, h Handler, nowMs int64) (*Watcher, *redis.Client) {
	t.Helper()
	addr := os.Getenv("PANDORA_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("未设 PANDORA_TEST_REDIS_ADDR,跳过真 Redis 验证(miniredis 覆盖不了 ZADD GT/XX 语义)")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("连不上真 Redis %s: %v", addr, err)
	}
	w, err := New(client, reader, h, Options{
		Namespace: "realredis-test",
		Threshold: 180 * time.Second,
		Interval:  15 * time.Second,
		Budget:    200,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	w.now = func() time.Time { return time.UnixMilli(nowMs) }
	t.Cleanup(func() {
		client.Del(ctx, w.dueKey, w.evidenceKey)
		_ = client.Close()
	})
	// 每轮开始先清干净。**两个 key 都要清**:due 只是「下次何时尝试」,
	// evidence 才是「离线时刻基线」,claim 会拿两者做 CAS —— 只清一个会让残留的
	// evidence 与新 due 失配,claim 恒返回 status=0,整轮什么都不做。
	client.Del(ctx, w.dueKey, w.evidenceKey)
	return w, client
}

func dueScore(t *testing.T, w *Watcher, member string) (float64, bool) {
	t.Helper()
	score, err := w.rdb.ZScore(context.Background(), w.dueKey, member).Result()
	if err == redis.Nil {
		return 0, false
	}
	if err != nil {
		t.Fatalf("ZScore: %v", err)
	}
	return score, true
}

// ZADD GT:迟到 / 重复事件只能往后推,不能拉早。
func TestRealRedis_Enqueue只往后推不往前拉(t *testing.T) {
	const now = 10_000_000
	w, _ := newRealRedisWatcher(t, &fakeReader{}, &recordingHandler{}, now)
	ctx := context.Background()

	later := int64(now - 10_000)
	earlier := int64(now - 500_000)

	if err := w.Enqueue(ctx, 42, later); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	// 迟到的旧事件(kafka 重投 / 乱序)不得把到期时间拉回更早。
	if err := w.Enqueue(ctx, 42, earlier); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	want := float64(later + w.opts.Threshold.Milliseconds())
	if got, ok := dueScore(t, w, "42"); !ok || got != want {
		t.Fatalf("到期时刻被旧事件拉早了(ZADD GT 语义不符): got=%v want=%v ok=%v", got, want, ok)
	}

	// 更晚的新事件(玩家回来又走了)必须把复查推后。
	newest := int64(now + 5_000)
	if err := w.Enqueue(ctx, 42, newest); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	want = float64(newest + w.opts.Threshold.Milliseconds())
	if got, _ := dueScore(t, w, "42"); got != want {
		t.Fatalf("新事件应把到期时刻推后: got=%v want=%v", got, want)
	}
}

// 完整一轮:离线满阈值 → 调 Handler → 出队;在线 → 直接出队不调 Handler。
func TestRealRedis_Sweep判定与出队(t *testing.T) {
	const now = 10_000_000
	reader := &fakeReader{
		online:   map[uint64]bool{43: true},
		lastSeen: map[uint64]int64{42: now - 200_000, 43: now - 200_000},
	}
	h := &recordingHandler{}
	w, _ := newRealRedisWatcher(t, reader, h, now)
	ctx := context.Background()

	if err := w.Enqueue(ctx, 42, now-200_000); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := w.Enqueue(ctx, 43, now-200_000); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	w.Sweep(ctx)

	if len(h.seen) != 1 || h.seen[0] != 42 {
		t.Fatalf("只有确认离线满阈值的才该触发业务动作, got=%v", h.seen)
	}
	if _, ok := dueScore(t, w, "42"); ok {
		t.Fatal("处理成功后必须出队,否则每轮重复调 Handler")
	}
	if _, ok := dueScore(t, w, "43"); ok {
		t.Fatal("确认在线后应当出队")
	}
}

// 依赖不可用必须 fail-closed:一个都不处理,且项目留在队列里等下轮。
func TestRealRedis_Sweep依赖不可用时零动作(t *testing.T) {
	const now = 10_000_000
	reader := &fakeReader{err: context.DeadlineExceeded}
	h := &recordingHandler{}
	w, _ := newRealRedisWatcher(t, reader, h, now)
	ctx := context.Background()

	if err := w.Enqueue(ctx, 42, now-200_000); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	w.Sweep(ctx)

	if len(h.seen) != 0 {
		t.Fatalf("locator 查不通必须 fail-closed,绝不能把整批当离线, got=%v", h.seen)
	}
	if _, ok := dueScore(t, w, "42"); !ok {
		t.Fatal("查不通时该项必须留在队列里等下轮,不能丢")
	}
}

// 已处理完出队的成员不得被后续轮次复活 —— 否则同一个玩家会被反复触发业务动作。
// 走公开 API 验证(内部改期用的是带 CAS 的 claim/retry,不直接断言其形态)。
func TestRealRedis_出队后不再复活(t *testing.T) {
	const now = 10_000_000
	reader := &fakeReader{lastSeen: map[uint64]int64{42: now - 200_000}}
	h := &recordingHandler{}
	w, _ := newRealRedisWatcher(t, reader, h, now)
	ctx := context.Background()

	if err := w.Enqueue(ctx, 42, now-200_000); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	w.Sweep(ctx)
	if len(h.seen) != 1 {
		t.Fatalf("首轮应处理一次, got=%v", h.seen)
	}
	if _, ok := dueScore(t, w, "42"); ok {
		t.Fatal("处理成功后必须出队")
	}

	// 再跑两轮:队列已空,不得凭空复活该成员,也不得再调 Handler。
	w.Sweep(ctx)
	w.Sweep(ctx)
	if len(h.seen) != 1 {
		t.Fatalf("已出队成员不得被复活重复处理, got=%v", h.seen)
	}
	if _, ok := dueScore(t, w, "42"); ok {
		t.Fatal("空轮不得把已处理完的成员塞回队列")
	}
}
