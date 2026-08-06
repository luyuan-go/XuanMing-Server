// offlinewatch_test.go — 离线复查骨架单测(2026-08-06)。
//
// 本包的判定错一次的后果是「把在线玩家踢出队伍」这类不可逆动作,所以测试重点不是
// happy path,而是三条安全性质:
//  1. 判定不了(locator 查不通 / 拿不到离开时刻)一律不动作(fail-closed);
//  2. 玩家此刻在线(含 travel 去战斗的 MATCHING/BATTLE)一律不动作;
//  3. 迟到 / 重复的事件只能把复查推后,不能拉早。
package offlinewatch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// ── 测试替身 ────────────────────────────────────────────────────────────────

type fakeReader struct {
	online   map[uint64]bool
	lastSeen map[uint64]int64
	err      error
	calls    int
}

func (f *fakeReader) BatchOnline(_ context.Context, ids []uint64) (map[uint64]bool, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	out := map[uint64]bool{}
	for _, id := range ids {
		if f.online[id] {
			out[id] = true
		}
	}
	return out, nil
}

func (f *fakeReader) BatchLastSeen(_ context.Context, ids []uint64) (map[uint64]int64, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := map[uint64]int64{}
	for _, id := range ids {
		if ms, ok := f.lastSeen[id]; ok {
			out[id] = ms
		}
	}
	return out, nil
}

type recordingHandler struct {
	seen []uint64
	err  error
}

func (h *recordingHandler) OnPlayerOffline(_ context.Context, playerID uint64, _ int64) error {
	h.seen = append(h.seen, playerID)
	return h.err
}

// newTestWatcher 造一个用 miniredis + 受控时钟驱动的 Watcher。
func newTestWatcher(t *testing.T, reader PresenceReader, h Handler, nowMs int64) (*Watcher, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	w, err := New(rdb, reader, h, Options{
		Namespace: "test",
		Threshold: 180 * time.Second,
		Interval:  15 * time.Second,
		Budget:    200,
	})
	if err != nil {
		t.Fatalf("New 失败: %v", err)
	}
	w.now = func() time.Time { return time.UnixMilli(nowMs) }
	return w, mr
}

func dueMembers(t *testing.T, w *Watcher) map[string]float64 {
	t.Helper()
	out, err := w.rdb.ZRangeWithScores(context.Background(), w.dueKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("读调度队列失败: %v", err)
	}
	m := map[string]float64{}
	for _, z := range out {
		m[z.Member.(string)] = z.Score
	}
	return m
}

// ── classify:全部判定逻辑的穷举 ─────────────────────────────────────────────

func TestClassify(t *testing.T) {
	const now = 1_000_000
	threshold := 180 * time.Second

	cases := []struct {
		name     string
		online   map[uint64]bool
		lastSeen map[uint64]int64
		hint     int64
		want     Verdict
	}{
		{
			name:   "在线(位置查得到)→ 不动作",
			online: map[uint64]bool{42: true},
			// 即使 last-seen 是很久以前的旧记录也不能判离线:人已经回来了。
			lastSeen: map[uint64]int64{42: now - 999_999},
			want:     VerdictOnline,
		},
		{
			name:     "离线且已满阈值 → 可动作",
			lastSeen: map[uint64]int64{42: now - threshold.Milliseconds()},
			want:     VerdictOffline,
		},
		{
			name:     "离线但差 1ms 没满 → 继续等",
			lastSeen: map[uint64]int64{42: now - threshold.Milliseconds() + 1},
			want:     VerdictWaiting,
		},
		{
			name: "查不到位置也拿不到离开时刻 → UNKNOWN,绝不当离线",
			want: VerdictUnknown,
		},
		{
			name: "只有事件旁证(last-seen 缺席)→ 用旁证判",
			hint: now - threshold.Milliseconds(),
			want: VerdictOffline,
		},
		{
			name:     "last-seen 比旁证更晚 → 取更晚的,偏保守",
			lastSeen: map[uint64]int64{42: now - 1000},
			hint:     now - threshold.Milliseconds(),
			want:     VerdictWaiting,
		},
		{
			name:     "旁证比 last-seen 更晚 → 同样取更晚的",
			lastSeen: map[uint64]int64{42: now - threshold.Milliseconds()},
			hint:     now - 1000,
			want:     VerdictWaiting,
		},
		{
			name:     "时钟回拨导致离开时刻在未来 → 按刚离开处理,不提前触发",
			lastSeen: map[uint64]int64{42: now + 60_000},
			want:     VerdictWaiting,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := classify(now, 42, tc.online, tc.lastSeen, tc.hint, threshold)
			if got != tc.want {
				t.Fatalf("判定错误: got=%s want=%s", got, tc.want)
			}
		})
	}
}

func TestClassify_零值必须是不动作那一档(t *testing.T) {
	// 防呆:任何忘记赋值 / map 取不到的路径落到的都必须是 Unknown。
	var v Verdict
	if v != VerdictUnknown {
		t.Fatalf("Verdict 零值必须是 UNKNOWN,否则漏赋值会变成「直接踢人」: got=%s", v)
	}
}

// ── Sweep:到期复查 ─────────────────────────────────────────────────────────

func TestSweep_离线满阈值才调Handler并出队(t *testing.T) {
	const now = 10_000_000
	reader := &fakeReader{lastSeen: map[uint64]int64{42: now - 200_000}}
	h := &recordingHandler{}
	w, _ := newTestWatcher(t, reader, h, now)

	if err := w.Enqueue(context.Background(), 42, now-200_000); err != nil {
		t.Fatalf("Enqueue 失败: %v", err)
	}
	w.Sweep(context.Background())

	if len(h.seen) != 1 || h.seen[0] != 42 {
		t.Fatalf("应当恰好处理玩家 42, got=%v", h.seen)
	}
	if len(dueMembers(t, w)) != 0 {
		t.Fatal("处理成功后必须出队,否则每轮重复调 Handler")
	}
}

func TestSweep_在线则丢弃不调Handler(t *testing.T) {
	const now = 10_000_000
	// 典型场景:玩家 travel 去战斗 —— Hub Logout 发过事件,但此刻位置是 BATTLE。
	reader := &fakeReader{
		online:   map[uint64]bool{42: true},
		lastSeen: map[uint64]int64{42: now - 200_000},
	}
	h := &recordingHandler{}
	w, _ := newTestWatcher(t, reader, h, now)

	_ = w.Enqueue(context.Background(), 42, now-200_000)
	w.Sweep(context.Background())

	if len(h.seen) != 0 {
		t.Fatalf("玩家在线时绝不能触发业务动作, got=%v", h.seen)
	}
	if len(dueMembers(t, w)) != 0 {
		t.Fatal("确认在线后应当出队")
	}
}

func TestSweep_locator查不通时一个都不处理(t *testing.T) {
	const now = 10_000_000
	reader := &fakeReader{err: errors.New("locator unavailable")}
	h := &recordingHandler{}
	w, _ := newTestWatcher(t, reader, h, now)

	_ = w.Enqueue(context.Background(), 42, now-200_000)
	w.Sweep(context.Background())

	if len(h.seen) != 0 {
		t.Fatalf("依赖不可用必须 fail-closed,绝不能把整批当离线, got=%v", h.seen)
	}
	due := dueMembers(t, w)
	if _, ok := due["42"]; !ok {
		t.Fatal("查不通时该项必须留在队列里等下轮,不能丢")
	}
}

func TestSweep_没满阈值则推迟到真正到期时刻(t *testing.T) {
	const now = 10_000_000
	leftAt := int64(now - 60_000) // 才离开 60s,阈值 180s
	reader := &fakeReader{lastSeen: map[uint64]int64{42: leftAt}}
	h := &recordingHandler{}
	w, _ := newTestWatcher(t, reader, h, now)

	// 先塞一个「已到期」的旧条目(模拟事件里 left_at 偏早),逼 Sweep 捞出来复查。
	_ = w.Enqueue(context.Background(), 42, now-200_000)
	w.Sweep(context.Background())

	if len(h.seen) != 0 {
		t.Fatalf("未满阈值不得动作, got=%v", h.seen)
	}
	due := dueMembers(t, w)
	wantDue := float64(leftAt + w.opts.Threshold.Milliseconds())
	if got := due["42"]; got != wantDue {
		t.Fatalf("应按权威 last-seen 重排到期时刻: got=%v want=%v", got, wantDue)
	}
}

func TestSweep_Handler失败则留队重试(t *testing.T) {
	const now = 10_000_000
	reader := &fakeReader{lastSeen: map[uint64]int64{42: now - 200_000}}
	h := &recordingHandler{err: errors.New("team service busy")}
	w, _ := newTestWatcher(t, reader, h, now)

	_ = w.Enqueue(context.Background(), 42, now-200_000)
	w.Sweep(context.Background())

	if len(h.seen) != 1 {
		t.Fatalf("应当尝试过一次, got=%v", h.seen)
	}
	due := dueMembers(t, w)
	if _, ok := due["42"]; !ok {
		t.Fatal("Handler 失败必须留在队列里重试,不能静默丢掉")
	}
	if due["42"] <= float64(now) {
		t.Fatalf("失败后应退避到未来,否则同一轮内会被反复捞出: got=%v now=%v", due["42"], float64(now))
	}
}

func TestSweep_拿不到离开时刻则出队不猜(t *testing.T) {
	const now = 10_000_000
	// locator 答得好好的,只是这个玩家没有 last-seen 记录(时刻已超保留期等)。
	reader := &fakeReader{lastSeen: map[uint64]int64{}}
	h := &recordingHandler{}
	w, _ := newTestWatcher(t, reader, h, now)

	_ = w.Enqueue(context.Background(), 42, now-200_000)
	w.Sweep(context.Background())

	if len(h.seen) != 0 {
		t.Fatalf("没有离开时刻依据就动手 = 猜,绝不允许, got=%v", h.seen)
	}
	if len(dueMembers(t, w)) != 0 {
		t.Fatal("这是持久条件不是抖动,必须出队;留着只会每轮白查一次、永不收敛")
	}
}

func TestSweep_预算封顶(t *testing.T) {
	const now = 10_000_000
	reader := &fakeReader{lastSeen: map[uint64]int64{}}
	h := &recordingHandler{}
	w, mr := newTestWatcher(t, reader, h, now)
	_ = mr
	w.opts.Budget = 3

	for i := uint64(1); i <= 10; i++ {
		reader.lastSeen[i] = now - 200_000
		if err := w.Enqueue(context.Background(), i, now-200_000); err != nil {
			t.Fatalf("Enqueue 失败: %v", err)
		}
	}
	w.Sweep(context.Background())

	if len(h.seen) != 3 {
		t.Fatalf("单轮处理量必须被 Budget 卡住(防积压打爆下游): got=%d want=3", len(h.seen))
	}
	if len(dueMembers(t, w)) != 7 {
		t.Fatalf("剩余项应留到下轮: got=%d want=7", len(dueMembers(t, w)))
	}
}

// ── Enqueue:迟到 / 重复事件不得把复查拉早 ────────────────────────────────────

func TestEnqueue_只往后推不往前拉(t *testing.T) {
	const now = 10_000_000
	w, _ := newTestWatcher(t, &fakeReader{}, &recordingHandler{}, now)
	ctx := context.Background()

	later := int64(now - 10_000)
	earlier := int64(now - 500_000)

	if err := w.Enqueue(ctx, 42, later); err != nil {
		t.Fatalf("Enqueue 失败: %v", err)
	}
	// 迟到的旧事件(kafka 重投 / 乱序)不能把到期时间拉回更早,否则会在还没满阈值时
	// 就去查一轮,白花一次 locator 往返。
	if err := w.Enqueue(ctx, 42, earlier); err != nil {
		t.Fatalf("Enqueue 失败: %v", err)
	}
	want := float64(later + w.opts.Threshold.Milliseconds())
	if got := dueMembers(t, w)["42"]; got != want {
		t.Fatalf("到期时刻被旧事件拉早了: got=%v want=%v", got, want)
	}

	// 更晚的新事件(玩家回来又走了)应当把复查推后。
	newest := int64(now + 5_000)
	if err := w.Enqueue(ctx, 42, newest); err != nil {
		t.Fatalf("Enqueue 失败: %v", err)
	}
	want = float64(newest + w.opts.Threshold.Milliseconds())
	if got := dueMembers(t, w)["42"]; got != want {
		t.Fatalf("新事件应把到期时刻推后: got=%v want=%v", got, want)
	}
}

// ── Inspect:业务读路径的兜底复查 ────────────────────────────────────────────

func TestInspect_兜底复查不依赖事件(t *testing.T) {
	const now = 10_000_000
	reader := &fakeReader{
		online: map[uint64]bool{7: true},
		lastSeen: map[uint64]int64{
			42: now - 200_000, // 离线且超阈值
			43: now - 60_000,  // 离线但没超
		},
	}
	w, _ := newTestWatcher(t, reader, &recordingHandler{}, now)

	got, err := w.Inspect(context.Background(), []uint64{7, 42, 43, 44, 0, 42})
	if err != nil {
		t.Fatalf("Inspect 失败: %v", err)
	}
	want := map[uint64]Verdict{
		7:  VerdictOnline,
		42: VerdictOffline,
		43: VerdictWaiting,
		44: VerdictUnknown, // 从没有过记录
	}
	for pid, wv := range want {
		if got[pid] != wv {
			t.Fatalf("player=%d 判定错误: got=%s want=%s", pid, got[pid], wv)
		}
	}
	if _, ok := got[0]; ok {
		t.Fatal("player_id=0 必须被剔除")
	}
	// 队列没被 Inspect 污染:兜底路径是纯查询,不该产生调度副作用。
	if len(dueMembers(t, w)) != 0 {
		t.Fatal("Inspect 不应写调度队列")
	}
}

func TestInspect_查不通必须整体报错(t *testing.T) {
	w, _ := newTestWatcher(t, &fakeReader{err: errors.New("boom")}, &recordingHandler{}, 10_000_000)
	if _, err := w.Inspect(context.Background(), []uint64{42}); err == nil {
		t.Fatal("locator 查不通必须报错,让调用方 fail-closed;返回半份结果会被误当成判定成立")
	}
}

// ── 构造参数校验 ────────────────────────────────────────────────────────────

func TestNew_阈值必填(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	if _, err := New(rdb, &fakeReader{}, &recordingHandler{}, Options{Namespace: "x"}); err == nil {
		t.Fatal("Threshold 没有安全默认值,必须报错而不是猜一个 —— 猜等于替业务决定什么时候踢人")
	}
	if _, err := New(rdb, &fakeReader{}, &recordingHandler{}, Options{Threshold: time.Minute}); err == nil {
		t.Fatal("Namespace 必填(队列 key 与 consumer group 都靠它隔离)")
	}
}

func TestDueKey_单slot(t *testing.T) {
	w, _ := newTestWatcher(t, &fakeReader{}, &recordingHandler{}, 0)
	want := "pandora:offlinewatch:{test}:due"
	if w.dueKey != want {
		t.Fatalf("调度队列 key 必须带 hash tag 固定单 slot(否则 Cluster 下 ZRANGEBYSCORE 取不到全量): got=%s want=%s", w.dueKey, want)
	}
}

func TestChunkIDs(t *testing.T) {
	ids := make([]uint64, 0, 7)
	for i := uint64(1); i <= 7; i++ {
		ids = append(ids, i)
	}
	got := chunkIDs(ids, 3)
	if len(got) != 3 || len(got[0]) != 3 || len(got[2]) != 1 {
		t.Fatalf("切批不对: %v", got)
	}
	if len(chunkIDs(ids, 0)) != 1 {
		t.Fatal("size<=0 应当不切")
	}
}

func TestDedupeIDs(t *testing.T) {
	got := dedupeIDs([]uint64{3, 0, 3, 1, 1, 2})
	want := []uint64{3, 1, 2}
	if len(got) != len(want) {
		t.Fatalf("去重结果不对: got=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("去重应保持首次出现顺序: got=%v want=%v", got, want)
		}
	}
}
