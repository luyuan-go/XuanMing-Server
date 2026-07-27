// consumer_test.go — per-consumer 失败处理策略(retry / poison / DLQ)单测。
//
// 直接构造 KeyOrderedConsumer 的 handler/retry/dlq 字段调 processMessage/toDLQ,
// 不依赖真实 sarama ConsumerGroup;**测试只能在 package kafkax 内**(访问未导出方法)。
package kafkax

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

// fakeDLQ 是 DLQProducer 的内存桩,记录投递次数并可注入失败。
type fakeDLQ struct {
	calls       int32
	lastKey     string
	lastHeaders []sarama.RecordHeader
	failErr     error
}

func (f *fakeDLQ) SendRaw(_ context.Context, key string, _ []byte) error {
	atomic.AddInt32(&f.calls, 1)
	f.lastKey = key
	return f.failErr
}

func (f *fakeDLQ) SendRawWithHeaders(_ context.Context, key string, _ []byte, headers []sarama.RecordHeader) error {
	atomic.AddInt32(&f.calls, 1)
	f.lastKey = key
	f.lastHeaders = headers
	return f.failErr
}

func newTestConsumer(handler Handler, retry RetryPolicy, dlq DLQProducer) *KeyOrderedConsumer {
	return &KeyOrderedConsumer{handler: handler, retry: retry, dlq: dlq}
}

// DLQ 投递必须原样保留原消息 header(event_type 等)并附带溯源 header,
// 否则回放 DLQ 时无 header 消息会再次被当 legacy 解码(审计 P2)。
func TestToDLQ_ForwardsHeadersAndProvenance(t *testing.T) {
	dlq := &fakeDLQ{}
	k := newTestConsumer(nil, RetryPolicy{}, dlq)
	msg := testMsg()
	msg.Headers = []*sarama.RecordHeader{{Key: []byte("event_type"), Value: []byte("7")}}
	if !k.toDLQ(context.Background(), msg) {
		t.Fatal("DLQ 投递成功应 ack")
	}
	got := map[string]string{}
	for _, h := range dlq.lastHeaders {
		got[string(h.Key)] = string(h.Value)
	}
	want := map[string]string{
		"event_type":        "7",
		"dlq-src-topic":     "pandora.battle.result",
		"dlq-src-partition": "0",
		"dlq-src-offset":    "1",
	}
	for key, val := range want {
		if got[key] != val {
			t.Fatalf("header %s=%q want %q (all=%v)", key, got[key], val, got)
		}
	}
}

func testMsg() *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{Topic: "pandora.battle.result", Partition: 0, Offset: 1, Key: []byte("42"), Value: []byte("payload")}
}

// 成功 → ack,handler 只调一次,不碰 DLQ。
func TestProcessMessage_SuccessAcks(t *testing.T) {
	var calls int32
	dlq := &fakeDLQ{}
	c := newTestConsumer(func(context.Context, *sarama.ConsumerMessage) error {
		atomic.AddInt32(&calls, 1)
		return nil
	}, RetryPolicy{MaxRetries: 3, Backoff: time.Millisecond}, dlq)

	if !c.processMessage(context.Background(), testMsg()) {
		t.Fatal("success 应 ack(返回 true)")
	}
	if calls != 1 {
		t.Fatalf("handler 调用次数=%d, want 1", calls)
	}
	if dlq.calls != 0 {
		t.Fatalf("成功不应投 DLQ, calls=%d", dlq.calls)
	}
}

// 毒丸(PoisonError)→ 不重试,直接 DLQ,ack。
func TestProcessMessage_PoisonGoesStraightToDLQ(t *testing.T) {
	var calls int32
	dlq := &fakeDLQ{}
	c := newTestConsumer(func(context.Context, *sarama.ConsumerMessage) error {
		atomic.AddInt32(&calls, 1)
		return Poison(errors.New("decode failed"))
	}, RetryPolicy{MaxRetries: 3, Backoff: time.Millisecond}, dlq)

	if !c.processMessage(context.Background(), testMsg()) {
		t.Fatal("毒丸进 DLQ 后应 ack(返回 true)")
	}
	if calls != 1 {
		t.Fatalf("毒丸不应重试, handler calls=%d want 1", calls)
	}
	if dlq.calls != 1 {
		t.Fatalf("毒丸应投 DLQ 一次, calls=%d", dlq.calls)
	}
	if dlq.lastKey != "42" {
		t.Fatalf("DLQ key=%q, want 42(保留原消息 key)", dlq.lastKey)
	}
}

// 业务瞬时错误重试耗尽 → DLQ,ack;handler 调用 1+MaxRetries 次。
func TestProcessMessage_TransientExhaustedToDLQ(t *testing.T) {
	var calls int32
	dlq := &fakeDLQ{}
	c := newTestConsumer(func(context.Context, *sarama.ConsumerMessage) error {
		atomic.AddInt32(&calls, 1)
		return errors.New("db timeout")
	}, RetryPolicy{MaxRetries: 3, Backoff: time.Millisecond}, dlq)

	if !c.processMessage(context.Background(), testMsg()) {
		t.Fatal("重试耗尽进 DLQ 后应 ack")
	}
	if calls != 4 {
		t.Fatalf("handler calls=%d, want 4(首次 + 3 重试)", calls)
	}
	if dlq.calls != 1 {
		t.Fatalf("应投 DLQ 一次, calls=%d", dlq.calls)
	}
}

// 重试中途成功 → ack,不投 DLQ。
func TestProcessMessage_RetrySucceeds(t *testing.T) {
	var calls int32
	dlq := &fakeDLQ{}
	c := newTestConsumer(func(context.Context, *sarama.ConsumerMessage) error {
		if atomic.AddInt32(&calls, 1) >= 2 {
			return nil // 第二次成功
		}
		return errors.New("transient")
	}, RetryPolicy{MaxRetries: 3, Backoff: time.Millisecond}, dlq)

	if !c.processMessage(context.Background(), testMsg()) {
		t.Fatal("重试成功应 ack")
	}
	if calls != 2 {
		t.Fatalf("handler calls=%d, want 2", calls)
	}
	if dlq.calls != 0 {
		t.Fatalf("重试成功不应投 DLQ, calls=%d", dlq.calls)
	}
}

// DLQ 投递失败 → 不 ack(返回 false),保证不可丢事件由 ConsumeClaim 重放。
func TestProcessMessage_DLQSendFailDoesNotAck(t *testing.T) {
	dlq := &fakeDLQ{failErr: errors.New("dlq broker down")}
	c := newTestConsumer(func(context.Context, *sarama.ConsumerMessage) error {
		return Poison(errors.New("decode failed"))
	}, RetryPolicy{MaxRetries: 0, Backoff: time.Millisecond}, dlq)

	if c.processMessage(context.Background(), testMsg()) {
		t.Fatal("DLQ 投递失败时不应 ack(返回 false)")
	}
}

// 无 DLQ 通道(loss-tolerant 消费者)→ 失败后 log + ack(返回 true,沿用旧行为)。
func TestProcessMessage_NoDLQLossTolerantAcks(t *testing.T) {
	c := newTestConsumer(func(context.Context, *sarama.ConsumerMessage) error {
		return errors.New("handler error")
	}, RetryPolicy{MaxRetries: 0}, nil)

	if !c.processMessage(context.Background(), testMsg()) {
		t.Fatal("无 DLQ 通道应 log + ack(返回 true)")
	}
}

// handler panic 必须被归一化为毒丸 → DLQ + ack(压测审核【必修-6】/P2-1):
// 任其展开会崩掉消费 goroutine,重启重放同 offset → CrashLoop 分区永久卡死。
func TestProcessMessage_HandlerPanicGoesToDLQ(t *testing.T) {
	dlq := &fakeDLQ{}
	k := newTestConsumer(func(context.Context, *sarama.ConsumerMessage) error {
		panic("deterministic handler panic")
	}, RetryPolicy{MaxRetries: 3}, dlq)

	if !k.processMessage(context.Background(), testMsg()) {
		t.Fatal("panic 消息投 DLQ 成功后应 ack(否则重放同 offset 再 panic)")
	}
	if got := atomic.LoadInt32(&dlq.calls); got != 1 {
		t.Fatalf("DLQ calls = %d, want 1(panic 即毒丸,不走瞬时错误重试)", got)
	}
}

// panic 且 DLQ 投递失败 → 不 ack(消息保留重放;不可丢事件不因 panic 被静默丢弃)。
func TestProcessMessage_HandlerPanicDLQFailNoAck(t *testing.T) {
	dlq := &fakeDLQ{failErr: errors.New("dlq down")}
	k := newTestConsumer(func(context.Context, *sarama.ConsumerMessage) error {
		panic("boom")
	}, RetryPolicy{MaxRetries: 1}, dlq)

	if k.processMessage(context.Background(), testMsg()) {
		t.Fatal("DLQ 投递失败时不得 ack")
	}
}
