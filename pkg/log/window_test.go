// window_test.go — 依赖降级日志限流器(模式 C)行为锁定。
//
// Window 把热路径上「每请求/每条消息一条 Warn/Error」压成「首错 + 每窗口一条 + 期间累计
// + 恢复一条」。它是并发原语(会被按 partition 并发的消费循环、并发 RPC handler 共享),
// 故必须覆盖:首错必打、窗口内抑制、窗口到期再打、Recovered 归零语义、并发下首错只打一次。
package log

import (
	"sync"
	"testing"
)

const testWindowMs = 5000

func TestWindow_FirstFailureAlwaysLogs(t *testing.T) {
	var w Window
	ok, streak := w.Admit(1_000_000, testWindowMs)
	if !ok {
		t.Fatal("首错必须打印(故障要立刻可见)")
	}
	if streak != 1 {
		t.Fatalf("streak = %d, want 1", streak)
	}
}

func TestWindow_SuppressesWithinWindow(t *testing.T) {
	var w Window
	now := int64(1_000_000)
	if ok, _ := w.Admit(now, testWindowMs); !ok {
		t.Fatal("首错应打印")
	}
	// 同窗口内的后续失败只累加不打印。
	for i := 0; i < 100; i++ {
		if ok, _ := w.Admit(now+int64(i), testWindowMs); ok {
			t.Fatalf("窗口内第 %d 次失败不应打印", i+2)
		}
	}
	// 窗口到期后再打一条,且带上累计次数。
	ok, streak := w.Admit(now+testWindowMs, testWindowMs)
	if !ok {
		t.Fatal("窗口到期应再打印一条")
	}
	if streak != 102 {
		t.Fatalf("streak = %d, want 102(1 首错 + 100 抑制 + 本次)", streak)
	}
}

func TestWindow_ZeroWindowLogsEveryTime(t *testing.T) {
	var w Window
	for i := 0; i < 5; i++ {
		if ok, _ := w.Admit(int64(i), 0); !ok {
			t.Fatalf("windowMs<=0 应退化为每次都打, 第 %d 次被抑制", i+1)
		}
	}
}

func TestWindow_RecoveredReportsAndResets(t *testing.T) {
	var w Window
	now := int64(1_000_000)
	w.Admit(now, testWindowMs)
	w.Admit(now+1, testWindowMs)
	w.AddExtra(7)
	w.AddExtra(3)

	failed, extra := w.Recovered()
	if failed != 2 {
		t.Fatalf("failed = %d, want 2", failed)
	}
	if extra != 10 {
		t.Fatalf("extra = %d, want 10", extra)
	}
	// 归零后再 Recovered 应返回 0(不重复打恢复日志)。
	if failed2, _ := w.Recovered(); failed2 != 0 {
		t.Fatalf("重复 Recovered 应返回 0, got %d", failed2)
	}
	// 归零后下一次失败重新算作首错(必打)。
	if ok, streak := w.Admit(now+2, testWindowMs); !ok || streak != 1 {
		t.Fatalf("恢复后再失败应视为首错必打, ok=%v streak=%d", ok, streak)
	}
}

func TestWindow_ConcurrentAdmitLogsFirstOnce(t *testing.T) {
	var w Window
	const goroutines = 64
	now := int64(1_000_000)

	var mu sync.Mutex
	logged := 0
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			// 同一时刻并发失败:只应有一个调用方拿到"该打印"(首错),其余被窗口抑制。
			if ok, _ := w.Admit(now, testWindowMs); ok {
				mu.Lock()
				logged++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if logged != 1 {
		t.Fatalf("并发首错只应打印 1 次, got %d", logged)
	}
	if got := w.streak.Load(); got != goroutines {
		t.Fatalf("streak = %d, want %d(每次失败都要计数)", got, goroutines)
	}
}
