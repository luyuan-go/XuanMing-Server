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
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/config"
)

// ── 测试替身 ────────────────────────────────────────────────────────────────

type fakeReader struct {
	online       map[uint64]bool
	lastSeen     map[uint64]int64
	err          error
	calls        int
	onlineHook   func()
	lastSeenHook func()
}

func (f *fakeReader) BatchOnline(_ context.Context, ids []uint64) (map[uint64]bool, error) {
	f.calls++
	if f.onlineHook != nil {
		f.onlineHook()
	}
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
	if f.lastSeenHook != nil {
		f.lastSeenHook()
	}
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
	fn   func(context.Context, uint64) error
}

func (h *recordingHandler) OnPlayerOffline(ctx context.Context, playerID uint64, _ int64) error {
	h.seen = append(h.seen, playerID)
	if h.fn != nil {
		return h.fn(ctx, playerID)
	}
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

func evidenceMembers(t *testing.T, w *Watcher) map[string]int64 {
	t.Helper()
	raw, err := w.rdb.HGetAll(context.Background(), w.evidenceKey).Result()
	if err != nil {
		t.Fatalf("读 evidence 失败: %v", err)
	}
	out := make(map[string]int64, len(raw))
	for member, value := range raw {
		ms, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Fatalf("evidence 坏值 member=%s value=%q: %v", member, value, err)
		}
		out[member] = ms
	}
	return out
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
		want     verdict
	}{
		{
			name:   "在线(位置查得到)→ 不动作",
			online: map[uint64]bool{42: true},
			// 即使 last-seen 是很久以前的旧记录也不能判离线:人已经回来了。
			lastSeen: map[uint64]int64{42: now - 999_999},
			want:     verdictOnline,
		},
		{
			name:     "离线且已满阈值 → 可动作",
			lastSeen: map[uint64]int64{42: now - threshold.Milliseconds()},
			want:     verdictOffline,
		},
		{
			name:     "离线但差 1ms 没满 → 继续等",
			lastSeen: map[uint64]int64{42: now - threshold.Milliseconds() + 1},
			want:     verdictWaiting,
		},
		{
			name: "查不到位置也拿不到离开时刻 → UNKNOWN,绝不当离线",
			want: verdictUnknown,
		},
		{
			name: "只有事件旁证(last-seen 缺席)→ 用旁证判",
			hint: now - threshold.Milliseconds(),
			want: verdictOffline,
		},
		{
			name:     "last-seen 比旁证更晚 → 取更晚的,偏保守",
			lastSeen: map[uint64]int64{42: now - 1000},
			hint:     now - threshold.Milliseconds(),
			want:     verdictWaiting,
		},
		{
			name:     "旁证比 last-seen 更晚 → 同样取更晚的",
			lastSeen: map[uint64]int64{42: now - threshold.Milliseconds()},
			hint:     now - 1000,
			want:     verdictWaiting,
		},
		{
			name:     "时钟回拨导致离开时刻在未来 → 按刚离开处理,不提前触发",
			lastSeen: map[uint64]int64{42: now + 60_000},
			want:     verdictWaiting,
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
	var v verdict
	if v != verdictUnknown {
		t.Fatalf("verdict 零值必须是 UNKNOWN,否则漏赋值会变成「直接踢人」: got=%s", v)
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

func TestSweep_Handler暂缓不会永久丢任务(t *testing.T) {
	now := int64(10_000_000)
	reader := &fakeReader{lastSeen: map[uint64]int64{42: now - 200_000}}
	attempts := 0
	h := &recordingHandler{fn: func(_ context.Context, _ uint64) error {
		attempts++
		if attempts == 1 {
			return ErrDeferred
		}
		return nil
	}}
	w, _ := newTestWatcher(t, reader, h, now)
	w.now = func() time.Time { return time.UnixMilli(now) }

	_ = w.Enqueue(context.Background(), 42, now-200_000)
	w.Sweep(context.Background())
	if attempts != 1 || len(dueMembers(t, w)) != 1 || len(evidenceMembers(t, w)) != 1 {
		t.Fatalf("暂缓后必须保留 due+evidence: attempts=%d due=%v evidence=%v",
			attempts, dueMembers(t, w), evidenceMembers(t, w))
	}

	now += w.opts.RetryBackoff.Milliseconds()
	w.Sweep(context.Background())
	if attempts != 2 {
		t.Fatalf("暂缓条件释放后必须自动重试: attempts=%d", attempts)
	}
	if len(dueMembers(t, w)) != 0 || len(evidenceMembers(t, w)) != 0 {
		t.Fatal("第二次处理成功后应原子清理 due+evidence")
	}
}

func TestSweep_Handler遵守ctx时单次尝试有界(t *testing.T) {
	const now = 10_000_000
	reader := &fakeReader{lastSeen: map[uint64]int64{42: now - 200_000}}
	h := &recordingHandler{fn: func(ctx context.Context, _ uint64) error {
		<-ctx.Done()
		return ctx.Err()
	}}
	w, _ := newTestWatcher(t, reader, h, now)
	w.opts.AttemptTimeout = 20 * time.Millisecond

	_ = w.Enqueue(context.Background(), 42, now-200_000)
	started := time.Now()
	w.Sweep(context.Background())
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("Handler 遵守 ctx 时 Sweep 必须有界返回: elapsed=%s", elapsed)
	}
	if len(dueMembers(t, w)) != 1 || len(evidenceMembers(t, w)) != 1 {
		t.Fatal("Handler 超时后任务必须保留重试")
	}
}

func TestSweep_升级前只有Due没有Evidence必须FailClosed丢弃(t *testing.T) {
	now := int64(10_000_000)
	reader := &fakeReader{lastSeen: map[uint64]int64{}}
	h := &recordingHandler{}
	w, _ := newTestWatcher(t, reader, h, now)
	w.now = func() time.Time { return time.UnixMilli(now) }

	// 模拟旧版本残留:只有已到期 due,没有可证明离线起点的 evidence。
	if err := w.rdb.ZAdd(context.Background(), w.dueKey,
		redis.Z{Score: float64(now - 1), Member: "42"}).Err(); err != nil {
		t.Fatalf("写旧 due: %v", err)
	}
	w.Sweep(context.Background())

	if len(h.seen) != 0 {
		t.Fatalf("没有 evidence 就动手 = 猜,绝不允许, got=%v", h.seen)
	}
	if len(evidenceMembers(t, w)) != 0 || len(dueMembers(t, w)) != 0 {
		t.Fatal("无权威 evidence 的旧 due 必须条件清掉,不能用本地 now 补基线")
	}

	now += 10 * w.opts.Threshold.Milliseconds()
	w.Sweep(context.Background())
	if len(h.seen) != 0 {
		t.Fatalf("无 evidence 时经过多久都不得处理: got=%v", h.seen)
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
	if got := evidenceMembers(t, w)["42"]; got != newest {
		t.Fatalf("evidence 必须保留最新离场代次: got=%d want=%d", got, newest)
	}
}

func TestEnqueue_同毫秒事件按幂等重投处理(t *testing.T) {
	const now = 10_000_000
	w, _ := newTestWatcher(t, &fakeReader{}, &recordingHandler{}, now)
	ctx := context.Background()
	leftAt := int64(now - 1_000)
	if err := w.Enqueue(ctx, 42, leftAt); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	wantDue := dueMembers(t, w)["42"]
	if err := w.Enqueue(ctx, 42, leftAt); err != nil {
		t.Fatalf("duplicate enqueue: %v", err)
	}
	if got := evidenceMembers(t, w)["42"]; got != leftAt {
		t.Fatalf("相同毫秒重投不得制造新 evidence: got=%d want=%d", got, leftAt)
	}
	if got := dueMembers(t, w)["42"]; got != wantDue {
		t.Fatalf("相同毫秒重投不得改写排期: got=%v want=%v", got, wantDue)
	}
}

func TestEnqueue_真实Unix毫秒必须保存为十进制整数(t *testing.T) {
	const unixMs = int64(1_775_000_000_123)
	w, mr := newTestWatcher(t, &fakeReader{}, &recordingHandler{}, unixMs)
	if err := w.Enqueue(context.Background(), 42, unixMs); err != nil {
		t.Fatalf("enqueue real unix ms: %v", err)
	}
	raw := mr.HGet(w.evidenceKey, "42")
	if raw != "1775000000123" {
		t.Fatalf("evidence 必须保留十进制整数，不能写成科学计数法: got=%q", raw)
	}
	if got := evidenceMembers(t, w)["42"]; got != unixMs {
		t.Fatalf("真实 Unix 毫秒必须可按 int64 回读: got=%d want=%d", got, unixMs)
	}
}

func TestEnqueue_没有权威离场时刻不得用本地Now代填(t *testing.T) {
	w, _ := newTestWatcher(t, &fakeReader{}, &recordingHandler{}, 10_000_000)
	if err := w.Enqueue(context.Background(), 42, 0); err == nil {
		t.Fatal("left_at_ms 缺失必须拒绝,不能用消费端 now 猜离线基线")
	}
	if len(dueMembers(t, w)) != 0 || len(evidenceMembers(t, w)) != 0 {
		t.Fatal("拒绝坏事件后不得留下任何调度状态")
	}
}

func TestClaim_旧扫描不得覆盖更新离场事件(t *testing.T) {
	const now = 10_000_000
	w, _ := newTestWatcher(t, &fakeReader{}, &recordingHandler{}, now)
	ctx := context.Background()
	oldSince := int64(now - 200_000)
	newSince := int64(now + 1_000)

	if err := w.Enqueue(ctx, 42, oldSince); err != nil {
		t.Fatalf("Enqueue old: %v", err)
	}
	oldDue := int64(dueMembers(t, w)["42"])
	if err := w.Enqueue(ctx, 42, newSince); err != nil {
		t.Fatalf("Enqueue new: %v", err)
	}

	status, _, err := w.claim(ctx, 42, oldDue, now, now+30_000)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if status != 0 {
		t.Fatalf("旧 expected_due 必须 CAS 失败, got status=%d", status)
	}
	if got := evidenceMembers(t, w)["42"]; got != newSince {
		t.Fatalf("旧 claim 不得覆盖新 evidence: got=%d want=%d", got, newSince)
	}
}

func TestClaim_旧Attempt不得完成或重排新副本同Evidence的Claim(t *testing.T) {
	const now = 10_000_000
	w, _ := newTestWatcher(t, &fakeReader{}, &recordingHandler{}, now)
	ctx := context.Background()
	since := int64(now - 200_000)
	_ = w.Enqueue(ctx, 42, since)
	firstDue := int64(dueMembers(t, w)["42"])
	firstClaimUntil := int64(now + 10_000)
	status, evidence, err := w.claim(ctx, 42, firstDue, now, firstClaimUntil)
	if err != nil || status != 1 {
		t.Fatalf("first claim: status=%d evidence=%d err=%v", status, evidence, err)
	}

	secondClaimUntil := int64(now + 30_000)
	status, evidence2, err := w.claim(ctx, 42, firstClaimUntil, firstClaimUntil, secondClaimUntil)
	if err != nil || status != 1 || evidence2 != evidence {
		t.Fatalf("second claim: status=%d evidence=%d err=%v", status, evidence2, err)
	}

	if err := w.finishClaim(ctx, 42, evidence, firstClaimUntil); err != nil {
		t.Fatalf("old finish: %v", err)
	}
	w.retry(ctx, 42, evidence, firstClaimUntil, now+5_000)
	if got := dueMembers(t, w)["42"]; got != float64(secondClaimUntil) {
		t.Fatalf("旧 attempt 不能删/覆盖新 claim: got due=%v want=%v", got, float64(secondClaimUntil))
	}
	if got := evidenceMembers(t, w)["42"]; got != evidence {
		t.Fatalf("旧 attempt 不能删 evidence: got=%d want=%d", got, evidence)
	}
}

func TestSweep_旧Handler成功不得删除并发新离场代次(t *testing.T) {
	const now = 10_000_000
	oldSince := int64(now - 200_000)
	newSince := int64(now + 1_000)
	reader := &fakeReader{lastSeen: map[uint64]int64{42: oldSince}}
	var w *Watcher
	h := &recordingHandler{fn: func(ctx context.Context, _ uint64) error {
		return w.Enqueue(ctx, 42, newSince)
	}}
	w, _ = newTestWatcher(t, reader, h, now)

	_ = w.Enqueue(context.Background(), 42, oldSince)
	w.Sweep(context.Background())

	if got := evidenceMembers(t, w)["42"]; got != newSince {
		t.Fatalf("旧 finish 不能删新 evidence: got=%d want=%d", got, newSince)
	}
	wantDue := float64(newSince + w.opts.Threshold.Milliseconds())
	if got := dueMembers(t, w)["42"]; got != wantDue {
		t.Fatalf("旧 finish 不能删新 due: got=%v want=%v", got, wantDue)
	}
}

// ── Observe:业务读路径的统一观测 + 排期 ─────────────────────────────────────

func TestObserve_兜底复查不依赖事件且不暴露两阶段接口(t *testing.T) {
	const now = 10_000_000
	reader := &fakeReader{
		online: map[uint64]bool{7: true},
		lastSeen: map[uint64]int64{
			42: now - 200_000, // 离线且超阈值
			43: now - 60_000,  // 离线但没超
		},
	}
	w, _ := newTestWatcher(t, reader, &recordingHandler{}, now)

	if err := w.Observe(context.Background(), []uint64{7, 42, 43, 44, 0, 42}); err != nil {
		t.Fatalf("Observe 失败: %v", err)
	}
	due := dueMembers(t, w)
	if got := len(due); got != 2 {
		t.Fatalf("只有具备权威 last-seen 的两个玩家应排期, got=%d", got)
	}
	if got := due["42"]; got != float64(reader.lastSeen[42]+w.opts.Threshold.Milliseconds()) {
		t.Fatalf("超阈值玩家也只由 Observe 排期、交给 Sweep: due=%v", got)
	}
	if got := due["43"]; got != float64(reader.lastSeen[43]+w.opts.Threshold.Milliseconds()) {
		t.Fatalf("未满阈值玩家应排到真实到期时刻: due=%v", got)
	}
	evidence := evidenceMembers(t, w)
	if _, ok := evidence["7"]; ok {
		t.Fatal("在线玩家不得留下调度 evidence")
	}
	if _, ok := evidence["44"]; ok {
		t.Fatal("无 last-seen 时不得用本地 now 创建可触发 Handler 的 evidence")
	}
}

func TestObserve_无权威LastSeen时永不触发_出现后才按阈值判断(t *testing.T) {
	now := int64(10_000_000)
	reader := &fakeReader{lastSeen: map[uint64]int64{}}
	h := &recordingHandler{}
	w, _ := newTestWatcher(t, reader, h, now)
	w.now = func() time.Time { return time.UnixMilli(now) }

	if err := w.Observe(context.Background(), []uint64{42}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if len(dueMembers(t, w)) != 0 || len(evidenceMembers(t, w)) != 0 {
		t.Fatal("key miss + 无 last-seen 必须保持 UNKNOWN,不得用本地 now 排破坏性任务")
	}

	// 即使本地时间跨过多个阈值、反复 Observe,也不能把持续 key miss 猜成持续离线。
	now += 10 * w.opts.Threshold.Milliseconds()
	if err := w.Observe(context.Background(), []uint64{42}); err != nil {
		t.Fatalf("second Observe: %v", err)
	}
	w.Sweep(context.Background())
	if len(h.seen) != 0 {
		t.Fatal("没有权威 last-seen 时无论经过多久都不得触发 Handler")
	}

	// locator 后来给出权威离场时刻,此刻才开始按完整阈值判断。
	leftAt := now
	reader.lastSeen[42] = leftAt
	if err := w.Observe(context.Background(), []uint64{42}); err != nil {
		t.Fatalf("Observe with last-seen: %v", err)
	}
	if got := evidenceMembers(t, w)["42"]; got != leftAt {
		t.Fatalf("应保存权威 last-seen: got=%d want=%d", got, leftAt)
	}

	now += w.opts.Threshold.Milliseconds() - 1
	w.Sweep(context.Background())
	if len(h.seen) != 0 {
		t.Fatal("权威 last-seen 差 1ms 未满阈值不得动作")
	}
	now++
	w.Sweep(context.Background())
	if len(h.seen) != 1 || h.seen[0] != 42 {
		t.Fatalf("权威 last-seen 满阈值后应处理: got=%v", h.seen)
	}
}

func TestObserve_在线清理不得删除读取期间到达的新离场事件(t *testing.T) {
	const now = 10_000_000
	oldSince := int64(now - 200_000)
	newSince := int64(now + 1_000)
	reader := &fakeReader{online: map[uint64]bool{42: true}}
	w, _ := newTestWatcher(t, reader, &recordingHandler{}, now)
	_ = w.Enqueue(context.Background(), 42, oldSince)

	fired := false
	reader.onlineHook = func() {
		if fired {
			return
		}
		fired = true
		if err := w.Enqueue(context.Background(), 42, newSince); err != nil {
			t.Fatalf("并发 Enqueue: %v", err)
		}
	}
	if err := w.Observe(context.Background(), []uint64{42}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if got := evidenceMembers(t, w)["42"]; got != newSince {
		t.Fatalf("旧在线观测不得清掉读取期间的新代次: got=%d want=%d", got, newSince)
	}
}

func TestObserve_查不通必须整体报错(t *testing.T) {
	w, _ := newTestWatcher(t, &fakeReader{err: errors.New("boom")}, &recordingHandler{}, 10_000_000)
	if err := w.Observe(context.Background(), []uint64{42}); err == nil {
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
	wantDue := "pandora:offlinewatch:{test}:due"
	wantEvidence := "pandora:offlinewatch:{test}:evidence"
	if w.dueKey != wantDue || w.evidenceKey != wantEvidence {
		t.Fatalf("due+evidence 必须共用 hash tag 固定单 slot: due=%s evidence=%s", w.dueKey, w.evidenceKey)
	}
}

func TestPresenceConsumer_Enqueue瞬时失败有有限重试(t *testing.T) {
	w, _ := newTestWatcher(t, &fakeReader{}, &recordingHandler{}, 0)
	cfg := w.consumerConfig(config.KafkaConfig{Brokers: []string{"127.0.0.1:9092"}}, 3)
	if cfg.RetryPolicy.MaxRetries != 3 || cfg.RetryPolicy.Backoff != 200*time.Millisecond {
		t.Fatalf("Redis Enqueue 失败不能沿用 0 次重试: got=%+v", cfg.RetryPolicy)
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
