// presence_test.go — PresenceHub fan-out 单测(2026-06-19)。
//
// 用受控时钟 + 直接驱动 step(),不起后台 ticker,验证:
//   - 去抖:上线后窗口内回退 → 不推(抖动吸收)
//   - 上线广播:窗口结算后状态变化 → 推给订阅者
//   - 合并:同订阅者多好友变更攒一条
//   - 只推订阅者:没订阅的人收不到;退订后不再收
//   - killswitch 降级:丢弃在途事件,不推
package biz

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakePusher 记录推送调用。
type fakePusher struct {
	mu    sync.Mutex
	calls []pushCall
}

type pushCall struct {
	sub     uint64
	changes []PresenceChangeOut
}

func (p *fakePusher) PushPresence(_ context.Context, sub uint64, changes []PresenceChangeOut) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cp := make([]PresenceChangeOut, len(changes))
	copy(cp, changes)
	p.calls = append(p.calls, pushCall{sub: sub, changes: cp})
	return nil
}

func (p *fakePusher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.calls)
}

// newTestHub 构造一个受控时钟的 hub(debounce 8s, tick 1s, 无 killswitch)。
func newTestHub(pusher PresencePusher, ks KillSwitchFunc) (*PresenceHub, *time.Time) {
	h := NewPresenceHub(pusher, 8*time.Second, time.Second, ks)
	base := time.Unix(1_700_000_000, 0)
	cur := base
	h.clock = func() time.Time { return cur }
	return h, &cur
}

func TestPresence_DebounceFlapSuppressed(t *testing.T) {
	pusher := &fakePusher{}
	h, clk := newTestHub(pusher, nil)
	h.Subscribe(100, []uint64{200})

	// t0:200 上线
	h.Notify(200, LocationStateHub)
	// t0+5s:窗口未到,step 不结算
	*clk = clk.Add(5 * time.Second)
	h.step(context.Background(), *clk)
	if pusher.count() != 0 {
		t.Fatalf("debounce 窗口内不应推送,got %d", pusher.count())
	}
	// t0+6s:200 又下线(抖动)
	*clk = clk.Add(time.Second)
	h.Notify(200, LocationStateOffline)
	// t0+9s:窗口到点,最终态=离线=基线 → 不推
	*clk = clk.Add(3 * time.Second)
	h.step(context.Background(), *clk)
	if pusher.count() != 0 {
		t.Fatalf("抖动(上线后又下线)应被吸收不推,got %d", pusher.count())
	}
}

func TestPresence_OnlineBroadcastToSubscriber(t *testing.T) {
	pusher := &fakePusher{}
	h, clk := newTestHub(pusher, nil)
	h.Subscribe(100, []uint64{200})

	h.Notify(200, LocationStateHub) // 上线
	*clk = clk.Add(9 * time.Second) // 过窗口
	h.step(context.Background(), *clk)

	if pusher.count() != 1 {
		t.Fatalf("窗口结算后应推 1 条,got %d", pusher.count())
	}
	call := pusher.calls[0]
	if call.sub != 100 {
		t.Errorf("应推给订阅者 100,got %d", call.sub)
	}
	if len(call.changes) != 1 || call.changes[0].PlayerID != 200 || call.changes[0].Status != PresenceOnline {
		t.Errorf("变更内容错误:%+v", call.changes)
	}
}

func TestPresence_CoalesceMultiple(t *testing.T) {
	pusher := &fakePusher{}
	h, clk := newTestHub(pusher, nil)
	h.Subscribe(100, []uint64{200, 300})

	h.Notify(200, LocationStateHub)    // 在线
	h.Notify(300, LocationStateBattle) // 游戏中
	*clk = clk.Add(9 * time.Second)
	h.step(context.Background(), *clk)

	if pusher.count() != 1 {
		t.Fatalf("两个好友变更应合并成 1 条推送,got %d", pusher.count())
	}
	if got := len(pusher.calls[0].changes); got != 2 {
		t.Fatalf("合并后应含 2 条变更,got %d", got)
	}
	// 校验状态映射
	byID := map[uint64]int32{}
	for _, c := range pusher.calls[0].changes {
		byID[c.PlayerID] = c.Status
	}
	if byID[200] != PresenceOnline {
		t.Errorf("200 应为 ONLINE,got %d", byID[200])
	}
	if byID[300] != PresenceInGame {
		t.Errorf("300 应为 IN_GAME,got %d", byID[300])
	}
}

func TestPresence_OnlyToSubscribers(t *testing.T) {
	pusher := &fakePusher{}
	h, clk := newTestHub(pusher, nil)
	h.Subscribe(100, []uint64{200}) // 只有 100 订阅 200
	// 400 订阅别人,不关注 200
	h.Subscribe(400, []uint64{999})

	h.Notify(200, LocationStateHub)
	*clk = clk.Add(9 * time.Second)
	h.step(context.Background(), *clk)

	if pusher.count() != 1 {
		t.Fatalf("只该推给订阅 200 的人,got %d", pusher.count())
	}
	if pusher.calls[0].sub != 100 {
		t.Errorf("应推给 100,got %d", pusher.calls[0].sub)
	}
}

func TestPresence_UnsubscribeStops(t *testing.T) {
	pusher := &fakePusher{}
	h, clk := newTestHub(pusher, nil)
	h.Subscribe(100, []uint64{200})
	h.Unsubscribe(100)

	h.Notify(200, LocationStateHub)
	*clk = clk.Add(9 * time.Second)
	h.step(context.Background(), *clk)

	if pusher.count() != 0 {
		t.Fatalf("退订后不应再收到推送,got %d", pusher.count())
	}
}

func TestPresence_ResubscribeReplaces(t *testing.T) {
	pusher := &fakePusher{}
	h, clk := newTestHub(pusher, nil)
	h.Subscribe(100, []uint64{200})
	h.Subscribe(100, []uint64{300}) // 替换:不再关注 200,只关注 300

	h.Notify(200, LocationStateHub) // 200 上线,但 100 已不关注
	*clk = clk.Add(9 * time.Second)
	h.step(context.Background(), *clk)

	if pusher.count() != 0 {
		t.Fatalf("重订阅替换后,旧关注(200)不应再推,got %d", pusher.count())
	}
}

func TestPresence_KillSwitchDegrade(t *testing.T) {
	pusher := &fakePusher{}
	disabled := true
	ks := func() (bool, string) { return disabled, "洪峰降级" }
	h, clk := newTestHub(pusher, ks)
	h.Subscribe(100, []uint64{200})

	h.Notify(200, LocationStateHub)
	*clk = clk.Add(9 * time.Second)
	h.step(context.Background(), *clk) // 降级中 → 丢弃,不推
	if pusher.count() != 0 {
		t.Fatalf("killswitch 降级时应丢弃事件不推,got %d", pusher.count())
	}

	// 恢复后,新事件正常推
	disabled = false
	h.Notify(200, LocationStateHub)
	*clk = clk.Add(9 * time.Second)
	h.step(context.Background(), *clk)
	if pusher.count() != 1 {
		t.Fatalf("恢复后应正常推,got %d", pusher.count())
	}
}

func TestPresence_StartCloseSmoke(t *testing.T) {
	pusher := &fakePusher{}
	h := NewPresenceHub(pusher, time.Second, 10*time.Millisecond, nil)
	h.Start()
	h.Subscribe(100, []uint64{200})
	h.Notify(200, LocationStateHub)
	time.Sleep(50 * time.Millisecond)
	h.Close() // 不应 hang
}

func TestCoarsePresence(t *testing.T) {
	cases := []struct {
		state int32
		want  int32
	}{
		{LocationStateUnspecified, PresenceOffline},
		{LocationStateOffline, PresenceOffline},
		{LocationStateLoginPending, PresenceOnline},
		{LocationStateHub, PresenceOnline},
		{LocationStateMatching, PresenceInGame},
		{LocationStateBattle, PresenceInGame},
	}
	for _, c := range cases {
		if got := coarsePresence(c.state); got != c.want {
			t.Errorf("coarsePresence(%d)=%d, want %d", c.state, got, c.want)
		}
	}
}
