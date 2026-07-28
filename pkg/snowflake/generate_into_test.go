package snowflake

import (
	"sync"
	"sync/atomic"
	"testing"
)

// assertStrictlyIncreasingUniqueNode 校验一批 ID 严格递增、互不重复、node 段正确。
func assertStrictlyIncreasingUniqueNode(t *testing.T, ids []uint64, wantNode uint64) {
	t.Helper()
	seen := make(map[uint64]struct{}, len(ids))
	var prev uint64
	for i, id := range ids {
		if got := (id >> nodeShift) & NodeMask; got != wantNode {
			t.Fatalf("ids[%d]=%d 的 node 段=%d, want %d", i, id, got, wantNode)
		}
		if i > 0 && id <= prev {
			t.Fatalf("ids[%d]=%d 未严格递增(前一个 %d)", i, id, prev)
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("ids[%d]=%d 重复", i, id)
		}
		seen[id] = struct{}{}
		prev = id
	}
}

// TestGenerateInto_UniqueMonotonicWithinSecond 覆盖最常见路径:一批完全落在当前秒内。
func TestGenerateInto_UniqueMonotonicWithinSecond(t *testing.T) {
	vclock, restore := withVirtualClock(int64(Epoch) + 100)
	defer restore()
	_ = vclock

	n := NewNode(5)
	ids := make([]uint64, 1000)
	n.GenerateInto(ids)

	assertStrictlyIncreasingUniqueNode(t, ids, 5)

	// 单秒内必须是连续的(无空洞),这是「一次 CAS 预留整段」的直接推论。
	for i := 1; i < len(ids); i++ {
		if ids[i] != ids[i-1]+1 {
			t.Fatalf("同一秒内应连续: ids[%d]=%d, ids[%d]=%d", i-1, ids[i-1], i, ids[i])
		}
	}
}

// TestGenerateInto_SpansSecondBoundary 覆盖一批超过单秒 step 池(32768)的情况:
// 必须填满本秒后自动滚到下一秒继续,总数不少一个、不重一个。
func TestGenerateInto_SpansSecondBoundary(t *testing.T) {
	vclock, restore := withVirtualClock(int64(Epoch) + 200)
	defer restore()

	n := NewNode(3)
	// 让虚拟时钟随 waitNextTime 的轮询自动前进,避免测试卡死。
	var advancing atomic.Bool
	advancing.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for advancing.Load() {
			vclock.Add(1)
		}
	}()

	const want = stepMask + 1 + 5000 // 一整池 + 跨到下一秒 5000 个
	ids := make([]uint64, want)
	n.GenerateInto(ids)

	advancing.Store(false)
	<-done

	if len(ids) != want {
		t.Fatalf("want %d 个 ID, got %d", want, len(ids))
	}
	assertStrictlyIncreasingUniqueNode(t, ids, 3)
}

// TestGenerateInto_StepNeverOverflowsIntoNode 保证批量路径不会像手写循环那样
// 把 step 写溢出到 node 段——预留量必须被当前秒剩余额度钳住。
func TestGenerateInto_StepNeverOverflowsIntoNode(t *testing.T) {
	vclock, restore := withVirtualClock(int64(Epoch) + 300)
	defer restore()

	const nodeID = NodeMask // 用最大 node 号,溢出会立刻踩坏 node 段
	n := NewNode(nodeID)

	// 先把当前秒消耗到只剩一点点,再要一大批,强制走「钳住 + 滚下一秒」。
	warm := make([]uint64, stepMask-10)
	n.GenerateInto(warm)

	var advancing atomic.Bool
	advancing.Store(true)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for advancing.Load() {
			vclock.Add(1)
		}
	}()

	ids := make([]uint64, 5000)
	n.GenerateInto(ids)

	advancing.Store(false)
	<-done

	assertStrictlyIncreasingUniqueNode(t, ids, nodeID)
}

// TestGenerateInto_EmptyIsNoop 保证 len(dst)==0 不改动状态,
// 不会白白吃掉一个 step 或推进时间段。
func TestGenerateInto_EmptyIsNoop(t *testing.T) {
	vclock, restore := withVirtualClock(int64(Epoch) + 400)
	defer restore()
	_ = vclock

	n := NewNode(1)
	before := n.state.Load()
	n.GenerateInto(nil)
	n.GenerateInto([]uint64{})
	if after := n.state.Load(); after != before {
		t.Fatalf("空批次改动了状态: before=%d after=%d", before, after)
	}
}

// TestGenerateInto_MixedWithGenerate_NoDuplicates 是本方法最关键的回归测试:
// 批量预留与逐个发号在同一个 Node 上并发混用时,全体 ID 仍必须互不重复。
// 若 GenerateInto 的 CAS 段边界算错(少 1 / 多 1),这里会直接抓到重号。
func TestGenerateInto_MixedWithGenerate_NoDuplicates(t *testing.T) {
	vclock, restore := withVirtualClock(int64(Epoch) + 500)
	defer restore()

	n := NewNode(9)

	// 时钟持续前进,保证 step 池不会耗尽把测试卡住。
	var advancing atomic.Bool
	advancing.Store(true)
	clockDone := make(chan struct{})
	go func() {
		defer close(clockDone)
		for advancing.Load() {
			vclock.Add(1)
		}
	}()

	const singleGoroutines = 8
	const singlePerGoroutine = 2000
	const batchGoroutines = 8
	const batchSize = 500
	const batchesPerGoroutine = 4

	var mu sync.Mutex
	all := make([]uint64, 0,
		singleGoroutines*singlePerGoroutine+batchGoroutines*batchSize*batchesPerGoroutine)

	var wg sync.WaitGroup
	for g := 0; g < singleGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]uint64, 0, singlePerGoroutine)
			for i := 0; i < singlePerGoroutine; i++ {
				local = append(local, n.Generate())
			}
			mu.Lock()
			all = append(all, local...)
			mu.Unlock()
		}()
	}
	for g := 0; g < batchGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]uint64, 0, batchSize*batchesPerGoroutine)
			buf := make([]uint64, batchSize)
			for b := 0; b < batchesPerGoroutine; b++ {
				n.GenerateInto(buf)
				// 单个批次内部必须严格递增
				for i := 1; i < len(buf); i++ {
					if buf[i] <= buf[i-1] {
						t.Errorf("批次内非递增: buf[%d]=%d buf[%d]=%d", i-1, buf[i-1], i, buf[i])
						return
					}
				}
				local = append(local, buf...)
			}
			mu.Lock()
			all = append(all, local...)
			mu.Unlock()
		}()
	}
	wg.Wait()

	advancing.Store(false)
	<-clockDone

	seen := make(map[uint64]struct{}, len(all))
	for _, id := range all {
		if _, dup := seen[id]; dup {
			t.Fatalf("Generate 与 GenerateInto 混用产生重复 ID: %d", id)
		}
		seen[id] = struct{}{}
		if got := (id >> nodeShift) & NodeMask; got != 9 {
			t.Fatalf("ID %d 的 node 段=%d, want 9", id, got)
		}
	}
	want := singleGoroutines*singlePerGoroutine + batchGoroutines*batchSize*batchesPerGoroutine
	if len(seen) != want {
		t.Fatalf("唯一 ID 数=%d, want %d", len(seen), want)
	}
}
