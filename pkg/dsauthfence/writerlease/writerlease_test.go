package writerlease

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeTerm 是确定性任期:token 由测试注入,Lost/Resign 可控。
type fakeTerm struct {
	token        uint64
	lost         chan struct{}
	mu           sync.Mutex
	resigned     bool
	remainingTTL time.Duration
	confirmErr   error
	confirmCalls int
	confirmFn    func(call int) (time.Duration, error)
}

func newFakeTerm(token uint64) *fakeTerm {
	return &fakeTerm{token: token, lost: make(chan struct{}), remainingTTL: 15 * time.Second}
}

func (t *fakeTerm) Token() uint64         { return t.token }
func (t *fakeTerm) Lost() <-chan struct{} { return t.lost }
func (t *fakeTerm) RemainingTTL(context.Context) (time.Duration, error) {
	t.mu.Lock()
	t.confirmCalls++
	call, fn := t.confirmCalls, t.confirmFn
	remaining, err := t.remainingTTL, t.confirmErr
	t.mu.Unlock()
	if fn != nil {
		return fn(call)
	}
	return remaining, err
}
func (t *fakeTerm) Resign(context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.resigned = true
	return nil
}

func (t *fakeTerm) wasResigned() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.resigned
}

func (t *fakeTerm) ttlProofCalls() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.confirmCalls
}

// fakeBackend 按脚本依次颁发任期;脚本耗尽后 Campaign 阻塞到 ctx 取消。
type fakeBackend struct {
	mu     sync.Mutex
	terms  []*fakeTerm
	closed bool
}

func (b *fakeBackend) Campaign(ctx context.Context, _ string) (Term, error) {
	b.mu.Lock()
	if len(b.terms) > 0 {
		term := b.terms[0]
		b.terms = b.terms[1:]
		b.mu.Unlock()
		return term, nil
	}
	b.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (b *fakeBackend) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestElectedExposesTokenAndHeld(t *testing.T) {
	term := newFakeTerm(42)
	backend := &fakeBackend{terms: []*fakeTerm{term}}
	lease := StartWithBackend(context.Background(), backend, Config{Election: "hub_allocator/writer"})
	defer func() { _ = lease.Close() }()

	waitFor(t, "elected", func() bool {
		token, held := lease.Current()
		return held && token == 42
	})
}

func TestLostStepsDownImmediatelyAndReelectsWithLargerToken(t *testing.T) {
	first := newFakeTerm(100)
	second := newFakeTerm(250)
	backend := &fakeBackend{terms: []*fakeTerm{first, second}}
	lease := StartWithBackend(context.Background(), backend, Config{Election: "hub_allocator/writer"})
	defer func() { _ = lease.Close() }()

	waitFor(t, "first term", func() bool {
		token, held := lease.Current()
		return held && token == 100
	})

	close(first.lost)
	// 失主必须先撤本地持有权,再清理旧任期。
	waitFor(t, "step down", func() bool {
		_, held := lease.Current()
		return !held || func() bool { token, _ := lease.Current(); return token == 250 }()
	})
	waitFor(t, "old term resigned", first.wasResigned)
	waitFor(t, "second term with strictly larger token", func() bool {
		token, held := lease.Current()
		return held && token == 250
	})
}

func TestCloseResignsAndStopsCampaign(t *testing.T) {
	term := newFakeTerm(7)
	backend := &fakeBackend{terms: []*fakeTerm{term}}
	lease := StartWithBackend(context.Background(), backend, Config{Election: "hub_allocator/writer"})

	waitFor(t, "elected", func() bool {
		_, held := lease.Current()
		return held
	})

	if err := lease.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, held := lease.Current(); held {
		t.Fatal("lease must not report held after Close")
	}
	if !term.wasResigned() {
		t.Fatal("term must be resigned on Close")
	}
	backend.mu.Lock()
	closed := backend.closed
	backend.mu.Unlock()
	if !closed {
		t.Fatal("backend must be closed on Close")
	}
	// Close 幂等。
	if err := lease.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestCampaignErrorRetriesWithoutHolding(t *testing.T) {
	// 空脚本:Campaign 阻塞到 ctx 取消,期间绝不能报告持有。
	backend := &fakeBackend{}
	lease := StartWithBackend(context.Background(), backend, Config{Election: "hub_allocator/writer"})
	defer func() { _ = lease.Close() }()

	time.Sleep(30 * time.Millisecond)
	if _, held := lease.Current(); held {
		t.Fatal("lease must not report held while campaigning")
	}
}

// failNTimesBackend:前 n 次 Campaign 返回错误,之后按脚本颁发任期。
type failNTimesBackend struct {
	fakeBackend
	mu2   sync.Mutex
	fails int
}

func (b *failNTimesBackend) Campaign(ctx context.Context, id string) (Term, error) {
	b.mu2.Lock()
	if b.fails > 0 {
		b.fails--
		b.mu2.Unlock()
		return nil, errors.New("etcd unreachable")
	}
	b.mu2.Unlock()
	return b.fakeBackend.Campaign(ctx, id)
}

// 复审 P0-6:竞选失败必须可观测——Health() 暴露连续失败计数与最近错误;
// 当选后计数清零、错误清空。
func TestCampaignFailureObservableViaHealth(t *testing.T) {
	term := newFakeTerm(9)
	backend := &failNTimesBackend{fails: 2}
	backend.terms = []*fakeTerm{term}
	lease := StartWithBackend(context.Background(), backend, Config{Election: "hub_allocator/writer"})
	defer func() { _ = lease.Close() }()

	waitFor(t, "campaign failures counted", func() bool {
		h := lease.Health()
		return h.ConsecutiveCampaignErrs >= 1 && h.LastCampaignErr == "etcd unreachable"
	})
	// 失败期间不得报告持有。
	if _, held := lease.Current(); held {
		t.Fatal("lease must not report held while campaign is failing")
	}
	// 退避 2s×2 后当选:计数清零、错误清空(用长等待覆盖两次退避)。
	waitFor2(t, "elected after failures", 6*time.Second, func() bool {
		_, held := lease.Current()
		return held
	})
	h := lease.Health()
	if h.ConsecutiveCampaignErrs != 0 || h.LastCampaignErr != "" {
		t.Fatalf("election must reset health counters, got %+v", h)
	}
	if !h.Held || h.Token != 9 {
		t.Fatalf("health snapshot must mirror Current(), got %+v", h)
	}
}

// R10 复审 P0-4:继任 sweep 必须是**接流前硬门**——激活钩子成功之前绝不宣告持有,
// 否则"当选到推扫完成"之间本副本已在接写,而前任在未被推扫触碰的 slot 上仍能写。
func TestActivationHookGatesWritability(t *testing.T) {
	term := newFakeTerm(11)
	backend := &fakeBackend{terms: []*fakeTerm{term}}
	release := make(chan struct{})
	var activatedToken atomic.Uint64
	lease := StartWithBackend(context.Background(), backend, Config{
		Election: "hub_allocator/writer",
		OnElected: func(_ context.Context, token uint64) error {
			activatedToken.Store(token)
			<-release
			return nil
		},
	})
	defer func() { _ = lease.Close() }()

	waitFor(t, "activation hook invoked", func() bool { return activatedToken.Load() == 11 })
	if _, held := lease.Current(); held {
		t.Fatal("lease must not report held while the activation hook is still running")
	}
	close(release)
	waitFor(t, "held after activation", func() bool {
		_, held := lease.Current()
		return held
	})
}

// 业务激活成功后仍必须先取得 etcd 服务端 TTL 证明。证明失败或剩余寿命不高于
// 安全余量时，本届从未进入 active，避免用配置 TTL 为已经异常的 lease 凭空续命。
func TestInitialTTLProofGatesWritability(t *testing.T) {
	cases := []struct {
		name      string
		remaining time.Duration
		proofErr  error
	}{
		{name: "proof failed", remaining: 15 * time.Second, proofErr: errors.New("etcd quorum unavailable")},
		{name: "remaining at margin", remaining: time.Duration(holdSafetyMarginSec) * time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			term := newFakeTerm(12)
			term.remainingTTL = tc.remaining
			term.confirmErr = tc.proofErr
			backend := &fakeBackend{terms: []*fakeTerm{term}}
			var activated atomic.Bool
			lease := StartWithBackend(context.Background(), backend, Config{
				Election: "hub_allocator/writer",
				OnElected: func(context.Context, uint64) error {
					activated.Store(true)
					return nil
				},
			})
			defer func() { _ = lease.Close() }()

			waitFor(t, "business activation succeeded", activated.Load)
			waitFor(t, "term resigned after initial TTL proof rejection", term.wasResigned)
			if state := lease.active.Load(); state != nil {
				t.Fatalf("initial TTL proof rejection must never publish active state, got token=%d", state.token)
			}
			if token, held := lease.Current(); held || token != 0 {
				t.Fatalf("initial TTL proof rejection must never expose held, got token=%d held=%v", token, held)
			}
			if snap := lease.Health(); snap.ConsecutiveActivationErrs == 0 || snap.LastActivationErr == "" {
				t.Fatalf("initial TTL proof rejection must be observable as activation failure, got %+v", snap)
			}
		})
	}
}

// R11 复审 P0-2 缺口 1/2:激活钩子**永久阻塞**(不是返回错误)必须在有界时间内转成
// 计数中的失败并让位。修复前 activate 用 context.WithCancel 无期限,阻塞时:本副本
// 永远不可写、同时占着 leader key 不让位 → 全集群无写者,而 degraded 恒 false 静默。
//
// 断言:① 阻塞被期限打断并计入 ConsecutiveActivationErrs;② 任期被让位(不霸占
// leader key);③ 连续阻塞最终把 Degraded() 抬成 true(告警可见);④ 全程不持有。
func TestBlockedActivationIsBoundedResignsAndDegrades(t *testing.T) {
	terms := make([]*fakeTerm, 0, 32)
	for i := 0; i < 32; i++ {
		terms = append(terms, newFakeTerm(uint64(300+i)))
	}
	backend := &fakeBackend{terms: terms}
	blocked := make(chan struct{})
	defer close(blocked)
	var hookCalls atomic.Uint64
	lease := StartWithBackend(context.Background(), backend, Config{
		Election: "hub_allocator/writer",
		// 钩子永久阻塞,只由期限打断(真实形态:etcd/Redis 卡住不返回错误)。
		OnElected: func(ctx context.Context, _ uint64) error {
			hookCalls.Add(1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-blocked:
				return nil
			}
		},
		ActivationTimeout: 30 * time.Millisecond,
	})
	defer func() { _ = lease.Close() }()

	waitFor(t, "blocked activation counted as failure", func() bool {
		return lease.Health().ConsecutiveActivationErrs >= 1
	})
	if _, held := lease.Current(); held {
		t.Fatal("a blocked activation must never announce held")
	}
	if !terms[0].wasResigned() {
		t.Fatal("a blocked activation must resign its term; holding the leader key starves every other replica")
	}
	// 让位后必须继续重新竞选(热备语义:不退出进程、不放弃)。等一个 recampaignBackoff。
	waitFor(t, "activation retried after resigning", func() bool { return hookCalls.Load() >= 2 })
	// 计数持续累加即可抬起 Degraded(阈值语义见 TestDegradedPredicate;这里只证明阻塞
	// 确实进入了同一条计数通道,不再是"永久阻塞 → 计数恒 0 → 静默")。
	snap := lease.Health()
	if snap.EscalateAfter == 0 {
		t.Fatal("health snapshot must expose the alerting threshold")
	}
	if snap.LastActivationErr == "" {
		t.Fatal("blocked activation must record a reason for the operator")
	}
}

// R11 二轮复审:Current() 必须**自带时间过期**,不依赖竞选 goroutine 及时消费 term.Lost()。
//
// 缺陷现场:进程被暂停(宿主换页/长 GC/容器 freeze)数十秒后恢复。etcd 侧 session lease
// 早已过期、任期实际没了,但 `current` 只在竞选 goroutine 观察到 Lost() 后才清零。
// 恢复瞬间若业务请求 goroutine 先被调度,它就带着作废 token 去写存储 —— 而 assignment
// 侧的 fencing 墓碑只有有限 TTL,暂停够久时墓碑已过期,拦不住这一笔。
//
// 本测试不去真的暂停进程(不可控),而是直接断言"本地截止时间到了就没有写权",
// 这正是修复的语义本体:无论调度顺序如何,过期后第一笔写就拿不到写权。
func TestCurrentExpiresByLocalDeadlineWithoutTermLostSignal(t *testing.T) {
	term := newFakeTerm(77)
	backend := &fakeBackend{terms: []*fakeTerm{term}}
	// LeaseTTLSec 取到最小:窗口被钳到 1s,渲染出一个可在测试内观察到的过期。
	lease := StartWithBackend(context.Background(), backend, Config{
		Election:    "hub_allocator/writer",
		LeaseTTLSec: 1,
	})
	defer func() { _ = lease.Close() }()

	waitFor(t, "elected", func() bool {
		_, held := lease.Current()
		return held
	})

	// 模拟"进程暂停":直接把本地截止时间推到过去。term.Lost() **故意不关闭** ——
	// 证明不依赖那个信号。
	state := lease.active.Load()
	if state == nil {
		t.Fatal("elected lease must expose an active hold state")
	}
	state.validUntilUnixNano.Store(nowMonotonicNanos() - int64(time.Second))
	if token, held := lease.Current(); held || token != 0 {
		t.Fatalf("本地截止时间过期后必须立即报不持有(不等 term.Lost()),got token=%d held=%v",
			token, held)
	}
	select {
	case <-term.Lost():
		t.Fatal("本测试的前提是 term.Lost() 未关闭;否则证明不了不依赖该信号")
	default:
	}
	// Health 也必须跟着变(告警口径与写权判定同源)。
	if snap := lease.Health(); snap.Held || snap.Token != 0 {
		t.Fatalf("Health 必须与 Current 同源,got %+v", snap)
	}
}

// Current 一旦观察到本地截止过期，本届必须进入不可逆 self-fenced 终态。否则交错为：
// 业务 goroutine 先看到过期并拒写 → 续证 goroutine 随后拿到迟到 TTL proof、重写 deadline
// → 同一 token 再次 held，旧任期发生“死而复生”。
func TestExpiredHoldCannotBeRevivedByLateTTLProof(t *testing.T) {
	lost := make(chan struct{})
	l := &Lease{}
	state := &holdState{token: 781, lost: lost}
	state.validUntilUnixNano.Store(nowMonotonicNanos() - int64(time.Second))
	l.active.Store(state)

	if token, held := l.Current(); held || token != 0 {
		t.Fatalf("expired hold must reject before late proof: token=%d held=%v", token, held)
	}
	if !state.selfFenced.Load() {
		t.Fatal("Current observing expiry must permanently self-fence the term")
	}
	if renewed := l.renewHold(state, 15*time.Second); renewed {
		t.Fatal("late TTL proof must not renew an already self-fenced term")
	}
	if token, held := l.Current(); held || token != 0 {
		t.Fatalf("same token revived after late proof: token=%d held=%v", token, held)
	}
	select {
	case <-lost:
		t.Fatal("test requires Lost to remain open; fencing must come from monotonic local state")
	default:
	}
}

// TTL 证明的 remaining 必须锚定请求开始时刻。若进程在服务端响应后、active 发布前
// 暂停超过证明窗口，恢复后不得用“恢复时刻 + 陈旧 remaining”宣告持有。
func TestStaleTTLProofCannotBeShiftedForwardAtPublish(t *testing.T) {
	l := &Lease{}
	state := &holdState{token: 782, lost: make(chan struct{})}
	proofStartedAt := nowMonotonicNanos() - int64(20*time.Second)
	if applied := l.applyTTLProof(state, 15*time.Second, proofStartedAt); applied {
		t.Fatal("stale TTL proof must not be shifted forward and published as a live hold")
	}
	if !state.selfFenced.Load() {
		t.Fatal("stale proof rejection must permanently self-fence that term")
	}
}

// 健康任期内本地截止时间必须被持续续期,否则会在窗口耗尽后凭空丢失写权。
func TestHoldDeadlineRenewedWhileTermAlive(t *testing.T) {
	term := newFakeTerm(78)
	term.remainingTTL = 5 * time.Second
	backend := &fakeBackend{terms: []*fakeTerm{term}}
	lease := StartWithBackend(context.Background(), backend, Config{
		Election:    "hub_allocator/writer",
		LeaseTTLSec: 5, // 窗口 2s、确认间隔 ~666ms
	})
	defer func() { _ = lease.Close() }()

	waitFor(t, "elected", func() bool {
		_, held := lease.Current()
		return held
	})
	state := lease.active.Load()
	if state == nil {
		t.Fatal("elected lease must expose an active hold state")
	}
	first := state.validUntilUnixNano.Load()
	// 等过一个续期间隔:截止时间必须被推后,且始终保持持有。
	waitFor(t, "hold deadline renewed", func() bool {
		return state.validUntilUnixNano.Load() > first
	})
	if _, held := lease.Current(); !held {
		t.Fatal("健康任期内不得丢失写权(本地续期必须跟上)")
	}
}

// Lost channel 已关闭时，Current 必须自行观察到失主，不能等待竞选 goroutine 先清状态。
// 这覆盖进程恢复后业务 goroutine 比竞选循环先被调度的确定性交错。
func TestCurrentObservesClosedLostChannelDirectly(t *testing.T) {
	lost := make(chan struct{})
	l := &Lease{}
	state := &holdState{token: 79, lost: lost}
	state.validUntilUnixNano.Store(nowMonotonicNanos() + int64(time.Minute))
	l.active.Store(state)
	close(lost)
	if token, held := l.Current(); held || token != 0 {
		t.Fatalf("closed Lost channel must fence immediately, got token=%d held=%v", token, held)
	}
}

// 单次续证失败只说明"这一拍没拿到新证据"，不构成"任期已被接管"的证否：上一拍成功
// 证明给出的本地安全截止仍然有效，本届必须继续持有到截止，不得立刻让位。
//
// 回归对象(2026-07-29 hub 自我 fencing)：修复前这里一次 etcd 慢读即 selfFence + 让位，
// 单副本 hub_allocator 没有任何竞争者却反复把自己踢下台，每次约 8s 无写者，期间
// writer 权威 RPC 全返回 errcode=10，hub DS 心跳连续被拒后 20s 授权租约饿死。
func TestTransientTTLProofFailureKeepsTermUntilDeadline(t *testing.T) {
	term := newFakeTerm(805)
	// remaining 给足(窗口 27s)，确认间隔仍由 LeaseTTLSec 决定为 ~666ms：
	// 保证测试在截止到期前就能观察到"失败一拍 → 下一拍恢复"。
	term.confirmFn = func(call int) (time.Duration, error) {
		if call == 2 || call == 3 {
			return 0, errors.New("etcd read timeout")
		}
		return 30 * time.Second, nil
	}
	backend := &fakeBackend{terms: []*fakeTerm{term}}
	lease := StartWithBackend(context.Background(), backend, Config{
		Election:    "hub_allocator/writer",
		LeaseTTLSec: 5,
	})
	defer func() { _ = lease.Close() }()

	waitFor(t, "elected", func() bool {
		token, held := lease.Current()
		return held && token == 805
	})
	// 失败期间：必须可观测(计数 + 最近原因)，但既不让位也不报 Degraded。
	waitFor(t, "proof retry counted", func() bool {
		return lease.Health().ConsecutiveActivationErrs >= 1
	})
	snap := lease.Health()
	if !snap.Held || snap.Token != 805 {
		t.Fatalf("续证失败期间不得让位: %+v", snap)
	}
	if snap.LastActivationErr == "" {
		t.Fatalf("续证重试必须记录最近原因: %+v", snap)
	}
	if snap.Degraded() {
		t.Fatalf("仍持有写权时不得报 Degraded: %+v", snap)
	}
	// 恢复之后：同一届继续持有，从未 Resign，计数被稳定续证清零。
	waitFor(t, "proof recovered and hold stabilized", func() bool {
		s := lease.Health()
		return term.ttlProofCalls() >= 5 && s.ConsecutiveActivationErrs == 0
	})
	if token, held := lease.Current(); !held || token != 805 {
		t.Fatalf("瞬时续证失败不得让位: token=%d held=%v", token, held)
	}
	if term.wasResigned() {
		t.Fatal("瞬时续证失败不得 Resign 本届")
	}
}

// 持续续证失败必须在本地安全截止到期后自 fencing + Resign：窗口是有界的，
// "读失败不算证否"不得变成无限续命。
func TestPersistentTTLProofFailureSelfFencesAtDeadline(t *testing.T) {
	term := newFakeTerm(80)
	term.remainingTTL = 4 * time.Second // 安全余量 3s → 本地窗口 1s
	backend := &fakeBackend{terms: []*fakeTerm{term}}
	lease := StartWithBackend(context.Background(), backend, Config{
		Election:    "hub_allocator/writer",
		LeaseTTLSec: 5, // 确认间隔 ~666ms
	})
	defer func() { _ = lease.Close() }()

	waitFor(t, "elected", func() bool {
		token, held := lease.Current()
		return held && token == 80
	})
	term.mu.Lock()
	term.confirmErr = errors.New("etcd quorum unavailable")
	term.mu.Unlock()
	waitFor(t, "self-fenced after the local safety deadline elapsed", func() bool {
		_, held := lease.Current()
		return !held
	})
	waitFor(t, "failed proof term resigned", term.wasResigned)
	if snap := lease.Health(); snap.ConsecutiveActivationErrs == 0 || snap.LastActivationErr == "" {
		t.Fatalf("holding-period TTL proof failure must be visible in Health, got %+v", snap)
	}
}

// 持有期证明失败要跨重选累计，不能在“首次 proof 成功、刚发布 held”时立刻清零；
// 新任期至少再完成一轮稳定续证后才清掉历史失败。
func TestHoldingProofFailureClearsOnlyAfterStableRenewal(t *testing.T) {
	term1 := newFakeTerm(801)
	term1.remainingTTL = 5 * time.Second
	term1.confirmFn = func(call int) (time.Duration, error) {
		if call == 1 {
			return 5 * time.Second, nil // 首次 proof，允许发布 held
		}
		return 0, errors.New("etcd quorum unavailable during hold")
	}
	term2 := newFakeTerm(802)
	term2.remainingTTL = 5 * time.Second
	lease := StartWithBackend(context.Background(), &fakeBackend{terms: []*fakeTerm{term1, term2}}, Config{
		Election:    "hub_allocator/writer",
		LeaseTTLSec: 5,
	})
	defer func() { _ = lease.Close() }()

	waitFor(t, "holding proof failure counted", func() bool {
		return lease.Health().ConsecutiveActivationErrs >= 1
	})
	waitFor(t, "failed first term resigned", term1.wasResigned)

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		token, held := lease.Current()
		snap := lease.Health()
		if held && token == 802 && term2.ttlProofCalls() >= 2 &&
			snap.ConsecutiveActivationErrs == 0 && snap.LastActivationErr == "" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("stable successor did not clear hold-proof failure: health=%+v term2_proofs=%d",
		lease.Health(), term2.ttlProofCalls())
}

// Go 无法强杀不尊重 ctx 的回调。激活包装必须仍能按期限让位，且在旧执行体退出前
// 不得每次重选再叠加一个永久阻塞 goroutine。
func TestActivationIgnoringContextIsBoundedAndSingleFlight(t *testing.T) {
	terms := []*fakeTerm{newFakeTerm(501), newFakeTerm(502), newFakeTerm(503)}
	backend := &fakeBackend{terms: terms}
	release := make(chan struct{})
	var calls atomic.Uint64
	lease := StartWithBackend(context.Background(), backend, Config{
		Election: "hub_allocator/writer",
		OnElected: func(context.Context, uint64) error {
			calls.Add(1)
			<-release // 故意完全忽略 ctx
			return nil
		},
		ActivationTimeout: 20 * time.Millisecond,
	})
	defer func() {
		close(release)
		_ = lease.Close()
	}()

	waitFor(t, "non-cooperative activation term resigned", terms[0].wasResigned)
	if _, held := lease.Current(); held {
		t.Fatal("timed-out non-cooperative activation must never announce held")
	}
	// 至少跨过一次重竞选退避；旧回调仍在时 hook 调用数必须保持 1。
	time.Sleep(recampaignBackoff + 100*time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("timed-out activation must be single-flight, calls=%d", got)
	}
}

// TTL 未知(LeaseTTLSec<=0,normalize 会补默认值,此处直接构造)时退化为纯事件等待,
// 不引入"莫名失去写权"。
func TestUnknownLeaseTTLKeepsEventOnlyHold(t *testing.T) {
	l := &Lease{}
	l.active.Store(&holdState{token: 9, lost: make(chan struct{})})
	if token, held := l.Current(); !held || token != 9 {
		t.Fatalf("未设本地截止时间时必须保持旧行为,got token=%d held=%v", token, held)
	}
	if l.holdRenewInterval() != 0 {
		t.Fatalf("TTL 未知时不应有续期节奏,got %v", l.holdRenewInterval())
	}
}

// Degraded() 是告警表达式的语义本体(此前全仓零测试覆盖)。它必须:持有写权时恒 false
// (热备/正常写者不报警),不持有且竞选或激活连续失败达阈值时 true。
func TestDegradedPredicate(t *testing.T) {
	cases := []struct {
		name string
		snap HealthSnapshot
		want bool
	}{
		{"holding writer is never degraded", HealthSnapshot{
			Held: true, ConsecutiveCampaignErrs: 99, ConsecutiveActivationErrs: 99, EscalateAfter: 15}, false},
		{"healthy hot standby is not degraded", HealthSnapshot{EscalateAfter: 15}, false},
		{"campaign failing below threshold", HealthSnapshot{
			ConsecutiveCampaignErrs: 14, EscalateAfter: 15}, false},
		{"campaign failing at threshold", HealthSnapshot{
			ConsecutiveCampaignErrs: 15, EscalateAfter: 15}, true},
		{"activation failing at threshold", HealthSnapshot{
			ConsecutiveActivationErrs: 15, EscalateAfter: 15}, true},
	}
	for _, c := range cases {
		if got := c.snap.Degraded(); got != c.want {
			t.Fatalf("%s: Degraded()=%v want %v (%+v)", c.name, got, c.want, c.snap)
		}
	}
}

// 钩子吞掉 ctx 错误、期限已到却返回 nil 时,同样不许宣告持有(否则用作废 token 接流)。
func TestActivationSwallowingDeadlineStillNotHeld(t *testing.T) {
	terms := []*fakeTerm{newFakeTerm(401), newFakeTerm(402)}
	backend := &fakeBackend{terms: terms}
	lease := StartWithBackend(context.Background(), backend, Config{
		Election: "hub_allocator/writer",
		OnElected: func(ctx context.Context, _ uint64) error {
			<-ctx.Done()
			return nil // 故意吞掉期限错误
		},
		ActivationTimeout: 20 * time.Millisecond,
	})
	defer func() { _ = lease.Close() }()

	waitFor(t, "swallowed deadline counted as failure", func() bool {
		return lease.Health().ConsecutiveActivationErrs >= 1
	})
	if _, held := lease.Current(); held {
		t.Fatal("activation that ran past its budget must not announce held even if it returns nil")
	}
}

// 激活失败 = 让位重选,期间恒不持有;失败可经 Health() 观测(Degraded 用于告警)。
func TestActivationFailureNeverAnnouncesHeld(t *testing.T) {
	terms := make([]*fakeTerm, 0, 4)
	for i := 0; i < 4; i++ {
		terms = append(terms, newFakeTerm(uint64(20+i)))
	}
	backend := &fakeBackend{terms: terms}
	lease := StartWithBackend(context.Background(), backend, Config{
		Election:  "hub_allocator/writer",
		OnElected: func(context.Context, uint64) error { return errors.New("fence sweep failed") },
	})
	defer func() { _ = lease.Close() }()

	waitFor(t, "activation failure counted", func() bool {
		return lease.Health().ConsecutiveActivationErrs >= 1
	})
	if _, held := lease.Current(); held {
		t.Fatal("failed activation must never announce writability")
	}
	h := lease.Health()
	if h.LastActivationErr != "fence sweep failed" {
		t.Fatalf("activation error must be observable, got %+v", h)
	}
	if !terms[0].wasResigned() {
		t.Fatal("failed activation must resign the term so another replica can take over")
	}
}

// waitFor2:自定义超时版 waitFor(竞选退避 2s,默认 3s 不够)。
func waitFor2(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestStartValidatesConfig(t *testing.T) {
	if _, err := Start(context.Background(), Config{Election: "x"}); err == nil {
		t.Fatal("empty endpoints must fail fast")
	}
	if _, err := Start(context.Background(), Config{Endpoints: []string{"127.0.0.1:2379"}}); err == nil {
		t.Fatal("empty election must fail fast")
	}
}
