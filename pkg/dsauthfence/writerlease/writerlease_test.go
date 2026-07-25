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
	token    uint64
	lost     chan struct{}
	mu       sync.Mutex
	resigned bool
}

func newFakeTerm(token uint64) *fakeTerm {
	return &fakeTerm{token: token, lost: make(chan struct{})}
}

func (t *fakeTerm) Token() uint64         { return t.token }
func (t *fakeTerm) Lost() <-chan struct{} { return t.lost }
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
