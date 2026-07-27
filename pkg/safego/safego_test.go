// safego_test.go —— panic 兜底回归(压测审核【必修-6】,2026-07-26)。
package safego

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecover_SwallowsPanic(t *testing.T) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer Recover(context.Background(), "test_recover")
		panic("boom")
	}()
	select {
	case <-done:
		// panic 被吞,goroutine 正常退出,进程存活(测试没崩即证明)。
	case <-time.After(2 * time.Second):
		t.Fatal("goroutine did not finish")
	}
}

func TestGo_PanicDoesNotCrash(t *testing.T) {
	var ran atomic.Bool
	Go(context.Background(), "test_go", func() {
		ran.Store(true)
		panic("boom")
	})
	deadline := time.Now().Add(2 * time.Second)
	for !ran.Load() {
		if time.Now().After(deadline) {
			t.Fatal("fn never ran")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestLoop_SurvivesPanicAndStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var runs atomic.Int64
	Loop(ctx, "test_loop", 10*time.Millisecond, func(context.Context) {
		n := runs.Add(1)
		if n == 1 {
			panic("boom on first round") // 单轮 panic 不得终止循环
		}
	})

	deadline := time.Now().Add(3 * time.Second)
	for runs.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("loop did not survive panic, runs=%d", runs.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	// 取消后不再增长(留一个 tick 的余量)。
	time.Sleep(50 * time.Millisecond)
	after := runs.Load()
	time.Sleep(100 * time.Millisecond)
	if runs.Load() != after {
		t.Fatalf("loop kept running after cancel: %d -> %d", after, runs.Load())
	}
}

func TestLoop_InvalidIntervalNoSpin(t *testing.T) {
	var runs atomic.Int64
	Loop(context.Background(), "test_loop_bad", 0, func(context.Context) { runs.Add(1) })
	time.Sleep(50 * time.Millisecond)
	if runs.Load() != 0 {
		t.Fatalf("interval<=0 must not run, runs=%d", runs.Load())
	}
}
