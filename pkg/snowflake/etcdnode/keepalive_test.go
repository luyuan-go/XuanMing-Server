package etcdnode

import (
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// newTestHolder 构造一个只带 keepAliveLoop 所需字段的 Holder。
// ttlSec=1 ⇒ 自 fencing deadline = 666ms,测试整体可在 3s 内跑完。
func newTestHolder() *Holder {
	return &Holder{
		ttlSec: 1,
		lost:   make(chan struct{}),
		key:    "/test/snowflake/node/x/0",
	}
}

// waitLost 等待 Lost 关闭,返回是否在 d 内等到。
func waitLost(h *Holder, d time.Duration) bool {
	select {
	case <-h.Lost():
		return true
	case <-time.After(d):
		return false
	}
}

// TestKeepAliveLoop_SelfFenceTripsWithoutRenewal 钉住自 fencing 核心契约:
// 即便 channel 一直不关(模拟 clientv3 感知滞后 / 分区中还没到 client 端 deadline),
// 距上次续租确认超过 TTL*2/3 也必须触发 Lost —— 先于服务端过期点停止发号。
func TestKeepAliveLoop_SelfFenceTripsWithoutRenewal(t *testing.T) {
	h := newTestHolder()
	kaCh := make(chan *clientv3.LeaseKeepAliveResponse) // 永不发送、永不关闭
	go h.keepAliveLoop(kaCh)

	if !waitLost(h, 3*time.Second) {
		t.Fatal("无续租确认时,自 fencing 应在 TTL*2/3(666ms)左右触发 Lost,3s 内未触发")
	}
}

// TestKeepAliveLoop_RenewalsResetSelfFence 续租确认按期到达时不得误报 Lost;
// 停止续租后必须再度触发。
func TestKeepAliveLoop_RenewalsResetSelfFence(t *testing.T) {
	h := newTestHolder()
	kaCh := make(chan *clientv3.LeaseKeepAliveResponse)
	go h.keepAliveLoop(kaCh)

	// 每 200ms 一拍确认,持续 1.4s(> 666ms 自 fencing 窗口的两倍):期间不得 Lost。
	deadline := time.Now().Add(1400 * time.Millisecond)
	for time.Now().Before(deadline) {
		select {
		case kaCh <- &clientv3.LeaseKeepAliveResponse{}:
		case <-h.Lost():
			t.Fatal("续租确认按期到达时不应触发 Lost")
		}
		select {
		case <-time.After(200 * time.Millisecond):
		case <-h.Lost():
			t.Fatal("续租确认按期到达时不应触发 Lost")
		}
	}

	// 停止续租:应在 666ms 左右触发。
	if !waitLost(h, 3*time.Second) {
		t.Fatal("停止续租后自 fencing 未触发 Lost")
	}
}

// TestKeepAliveLoop_ChannelCloseSignalsLost channel 关闭(clientv3 判定失租)必须触发 Lost。
func TestKeepAliveLoop_ChannelCloseSignalsLost(t *testing.T) {
	h := newTestHolder()
	kaCh := make(chan *clientv3.LeaseKeepAliveResponse)
	go h.keepAliveLoop(kaCh)
	close(kaCh)

	if !waitLost(h, time.Second) {
		t.Fatal("channel 关闭后应立即触发 Lost")
	}
}

// TestKeepAliveLoop_IntentionalCloseSuppressesLost Close() 主动关闭不得误报 Lost
// (channel 关闭路径与 timer 到期路径都要压制)。
func TestKeepAliveLoop_IntentionalCloseSuppressesLost(t *testing.T) {
	// channel 关闭路径。
	h := newTestHolder()
	h.intentional.Store(true)
	kaCh := make(chan *clientv3.LeaseKeepAliveResponse)
	go h.keepAliveLoop(kaCh)
	close(kaCh)
	if waitLost(h, 300*time.Millisecond) {
		t.Fatal("intentional 置位后 channel 关闭不应触发 Lost")
	}

	// timer 到期路径。
	h2 := newTestHolder()
	h2.intentional.Store(true)
	kaCh2 := make(chan *clientv3.LeaseKeepAliveResponse) // 不发送,等 timer 到期
	go h2.keepAliveLoop(kaCh2)
	if waitLost(h2, 1500*time.Millisecond) {
		t.Fatal("intentional 置位后自 fencing 到期不应触发 Lost")
	}
}
