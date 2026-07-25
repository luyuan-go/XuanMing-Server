// writer_fence_test.go — 写者继任存储级 fencing 测试(R9 P0-7;miniredis)。
// 覆盖:未持有拒写、落后 token 拒写(零写入)、推进水位、幂等同 token 放行、
// 损坏 fence 值 fail-closed、nil fence 保持 legacy 行为、auth 记录仓同规则。
package data

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// fakeWriterFence 是可编程 WriterFence(测试专用)。
type fakeWriterFence struct {
	token uint64
	held  bool
}

func (f *fakeWriterFence) Current() (uint64, bool) { return f.token, f.held }

func TestWriterFence_NotHeldRejectsWriteZeroMutation(t *testing.T) {
	ctx := context.Background()
	repo, mr := newRepo(t)
	const pod = "pandora-hub-global-1"
	_ = repo.CreateShard(ctx, sampleShard(pod, 1, 0), testTTL)
	repo.SetWriterFence(&fakeWriterFence{token: 0, held: false})

	err := repo.UpdateShardWithLock(ctx, pod, 3, func(s *hubv1.HubShardStorageRecord) error {
		s.PlayerCount = 99
		return nil
	}, testTTL)
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("not-held write must be ErrUnavailable, got %v", err)
	}
	got, _, _ := repo.GetShard(ctx, pod)
	if got.PlayerCount != 0 {
		t.Fatalf("rejected write must not mutate shard, got count %d", got.PlayerCount)
	}
	if mr.Exists(wfenceKey(pod)) {
		t.Fatal("rejected write must not create fence key")
	}
}

func TestWriterFence_StaleTokenRejectedZeroMutation(t *testing.T) {
	ctx := context.Background()
	repo, mr := newRepo(t)
	const pod = "pandora-hub-global-1"
	_ = repo.CreateShard(ctx, sampleShard(pod, 1, 0), testTTL)
	// 继任者已把水位推进到 6;本副本还拿着第 5 届 token。
	mr.Set(wfenceKey(pod), "6")
	repo.SetWriterFence(&fakeWriterFence{token: 5, held: true})

	err := repo.UpdateShardWithLock(ctx, pod, 3, func(s *hubv1.HubShardStorageRecord) error {
		s.PlayerCount = 99
		return nil
	}, testTTL)
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("stale-token write must be ErrUnavailable, got %v", err)
	}
	got, _, _ := repo.GetShard(ctx, pod)
	if got.PlayerCount != 0 {
		t.Fatalf("stale write must not mutate shard, got count %d", got.PlayerCount)
	}
	if v, _ := mr.Get(wfenceKey(pod)); v != "6" {
		t.Fatalf("stale writer must not move the fence, got %q", v)
	}

	// 心跳同规则:失主镜像变更被拒,零写入。
	if _, err := repo.HeartbeatShard(ctx, pod, 42, "ready", 0, 0, false, testTTL); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("stale-token heartbeat must be ErrUnavailable, got %v", err)
	}
	got, _, _ = repo.GetShard(ctx, pod)
	if got.PlayerCount != 0 {
		t.Fatalf("stale heartbeat must not mutate shard, got count %d", got.PlayerCount)
	}
}

func TestWriterFence_AdvancesWatermarkThenIdempotent(t *testing.T) {
	ctx := context.Background()
	repo, mr := newRepo(t)
	const pod = "pandora-hub-global-1"
	_ = repo.CreateShard(ctx, sampleShard(pod, 1, 0), testTTL)
	mr.Set(wfenceKey(pod), "6")
	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})

	if err := repo.UpdateShardWithLock(ctx, pod, 3, func(s *hubv1.HubShardStorageRecord) error {
		s.PlayerCount = 1
		return nil
	}, testTTL); err != nil {
		t.Fatalf("newer-token write must pass: %v", err)
	}
	if v, _ := mr.Get(wfenceKey(pod)); v != "7" {
		t.Fatalf("write must advance fence to 7, got %q", v)
	}
	if mr.TTL(wfenceKey(pod)) != 0 {
		t.Fatal("fence key must be persistent (no TTL): the watermark must outlive shard records")
	}
	// cur == mine:放行且不再写 fence。
	if err := repo.UpdateShardWithLock(ctx, pod, 3, func(s *hubv1.HubShardStorageRecord) error {
		s.PlayerCount = 2
		return nil
	}, testTTL); err != nil {
		t.Fatalf("same-token write must pass: %v", err)
	}
	if v, _ := mr.Get(wfenceKey(pod)); v != "7" {
		t.Fatalf("fence must stay 7, got %q", v)
	}
	// 推进后旧 token 永久出局。
	repo.SetWriterFence(&fakeWriterFence{token: 6, held: true})
	if err := repo.UpdateShardWithLock(ctx, pod, 3, func(s *hubv1.HubShardStorageRecord) error {
		s.PlayerCount = 3
		return nil
	}, testTTL); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("superseded token must stay rejected, got %v", err)
	}
	got, _, _ := repo.GetShard(ctx, pod)
	if got.PlayerCount != 2 {
		t.Fatalf("superseded write leaked: count %d", got.PlayerCount)
	}
}

func TestWriterFence_CorruptValueFailsClosed(t *testing.T) {
	ctx := context.Background()
	repo, mr := newRepo(t)
	const pod = "pandora-hub-global-1"
	_ = repo.CreateShard(ctx, sampleShard(pod, 1, 0), testTTL)
	mr.Set(wfenceKey(pod), "not-a-number")
	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})

	err := repo.UpdateShardWithLock(ctx, pod, 3, func(s *hubv1.HubShardStorageRecord) error {
		s.PlayerCount = 1
		return nil
	}, testTTL)
	if err == nil {
		t.Fatal("corrupt fence value must fail closed")
	}
	got, _, _ := repo.GetShard(ctx, pod)
	if got.PlayerCount != 0 {
		t.Fatalf("corrupt-fence write must not mutate shard, got count %d", got.PlayerCount)
	}
}

func TestWriterFence_NilFenceLegacyBehavior(t *testing.T) {
	ctx := context.Background()
	repo, mr := newRepo(t)
	const pod = "pandora-hub-global-1"
	_ = repo.CreateShard(ctx, sampleShard(pod, 1, 0), testTTL)

	if err := repo.UpdateShardWithLock(ctx, pod, 3, func(s *hubv1.HubShardStorageRecord) error {
		s.PlayerCount = 1
		return nil
	}, testTTL); err != nil {
		t.Fatalf("nil fence must keep legacy behavior: %v", err)
	}
	if mr.Exists(wfenceKey(pod)) {
		t.Fatal("nil fence must not create fence key")
	}
}

// 继任者水位推扫(覆盖边界 ③):把分片 SET ∪ 待清理 saga 源 pod 的 fence 一次性推进
// 到本届 token;推扫后前任 token 在全部 pod 上永久出局;幂等;被继任时 fail-closed。
func TestWriterFence_AdvanceWriterFencesSweep(t *testing.T) {
	ctx := context.Background()
	repo, mr := newRepo(t)
	const podA, podB, podC = "pandora-hub-global-1", "pandora-hub-global-2", "pandora-hub-global-3"
	_ = repo.CreateShard(ctx, sampleShard(podA, 1, 0), testTTL)
	_ = repo.CreateShard(ctx, sampleShard(podB, 2, 0), testTTL)
	// podC 不在分片 SET,仅作为待清理 saga 源 pod(推扫必须并入)。
	if err := repo.RegisterTransferCleanup(ctx, podC, TransferCleanupRef{PlayerID: 1001, TargetAssignmentID: "a1"}); err != nil {
		t.Fatalf("register cleanup: %v", err)
	}
	mr.Set(wfenceKey(podA), "3") // podA 有旧水位;podB/podC 从未被触碰(懒推进盲区)
	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})

	if err := repo.AdvanceWriterFences(ctx); err != nil {
		t.Fatalf("advance sweep: %v", err)
	}
	for _, pod := range []string{podA, podB, podC} {
		if v, _ := mr.Get(wfenceKey(pod)); v != "7" {
			t.Fatalf("pod %s fence must be swept to 7, got %q", pod, v)
		}
		if mr.TTL(wfenceKey(pod)) != 0 {
			t.Fatalf("pod %s fence must be persistent", pod)
		}
	}
	// 幂等:同 token 再推不出错、水位不变。
	if err := repo.AdvanceWriterFences(ctx); err != nil {
		t.Fatalf("idempotent sweep: %v", err)
	}
	// 推扫后,前任(token 5)在从未被逐 slot 触碰过的 podB 上也永久出局。
	repo.SetWriterFence(&fakeWriterFence{token: 5, held: true})
	err := repo.UpdateShardWithLock(ctx, podB, 3, func(s *hubv1.HubShardStorageRecord) error {
		s.PlayerCount = 99
		return nil
	}, testTTL)
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("stale writer on untouched pod must be rejected after sweep, got %v", err)
	}
	// 前任自己跑推扫:立即发现被继任,fail-closed 且不回退水位。
	if err := repo.AdvanceWriterFences(ctx); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("superseded sweep must be ErrUnavailable, got %v", err)
	}
	if v, _ := mr.Get(wfenceKey(podA)); v != "7" {
		t.Fatalf("superseded sweep must not regress fence, got %q", v)
	}
	// 失主:推扫直接拒。
	repo.SetWriterFence(&fakeWriterFence{token: 9, held: false})
	if err := repo.AdvanceWriterFences(ctx); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("not-held sweep must be ErrUnavailable, got %v", err)
	}
	// nil fence:no-op。
	repo.SetWriterFence(nil)
	if err := repo.AdvanceWriterFences(ctx); err != nil {
		t.Fatalf("nil fence sweep must be no-op: %v", err)
	}
}

// R10 复审 P0-4 ③:接流前硬门用**显式 token**推扫(此时租约还没宣告 held,
// fence.Current() 故意不可用)。token=0 必须拒绝,避免把哨兵值写成水位。
func TestWriterFence_AdvanceForTokenWorksBeforeLeaseAnnounced(t *testing.T) {
	ctx := context.Background()
	repo, mr := newRepo(t)
	const pod = "pandora-hub-global-1"
	_ = repo.CreateShard(ctx, sampleShard(pod, 1, 0), testTTL)
	// 激活钩子运行时的真实状态:已当选但尚未宣告持有。
	repo.SetWriterFence(&fakeWriterFence{token: 0, held: false})

	if err := repo.AdvanceWriterFencesForToken(ctx, 12); err != nil {
		t.Fatalf("activation sweep must not depend on an announced lease: %v", err)
	}
	if v, _ := mr.Get(wfenceKey(pod)); v != "12" {
		t.Fatalf("activation sweep must advance the fence to the elected token, got %q", v)
	}
	// 幂等:同 token 重复推扫零变化。
	if err := repo.AdvanceWriterFencesForToken(ctx, 12); err != nil {
		t.Fatalf("activation sweep must be idempotent: %v", err)
	}
	// 已被更高代继任 → fail-closed。
	mr.Set(wfenceKey(pod), "20")
	if err := repo.AdvanceWriterFencesForToken(ctx, 12); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("superseded activation sweep must fail closed, got %v", err)
	}
	if err := repo.AdvanceWriterFencesForToken(ctx, 0); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("token 0 is the not-held sentinel and must be rejected, got %v", err)
	}
}

// R10 复审 P0-4 ⑤:per-player 归属键无 hashtag,进不了 {pod} slot 事务,水位改记在
// 归属记录自身。落后 token 的旧写者既不能覆盖也不能删除继任者写的归属(零写入)。
func TestWriterFence_AssignmentCarriesPerPlayerWatermark(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepo(t)
	const playerID = uint64(1001)
	newRec := func(pod string) *hubv1.HubAssignmentStorageRecord {
		return &hubv1.HubAssignmentStorageRecord{
			PlayerId: playerID, HubPodName: pod, HubAddr: "127.0.0.1:7778",
			ShardId: 1, Region: "global", AssignedAtMs: 2000, AssignmentId: "a-" + pod,
		}
	}

	// 第 7 届写者建立归属:记录必须带上本届水位。
	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})
	swapped, err := repo.CompareAndSwapAssignment(ctx, playerID, nil, newRec("pod-A"), testTTL)
	if err != nil || !swapped {
		t.Fatalf("create assignment: swapped=%v err=%v", swapped, err)
	}
	stored, found, err := repo.GetAssignment(ctx, playerID)
	if err != nil || !found {
		t.Fatalf("read back assignment: found=%v err=%v", found, err)
	}
	if stored.GetWriterToken() != 7 {
		t.Fatalf("assignment must carry the writing term's fencing token, got %d", stored.GetWriterToken())
	}

	// 第 9 届继任者接管同一玩家:水位只进不退。
	repo.SetWriterFence(&fakeWriterFence{token: 9, held: true})
	next := newRec("pod-B")
	if swapped, err = repo.CompareAndSwapAssignment(ctx, playerID, stored, next, testTTL); err != nil || !swapped {
		t.Fatalf("successor swap: swapped=%v err=%v", swapped, err)
	}
	if next.GetWriterToken() != 0 {
		t.Fatal("stamping must not mutate the caller's message (biz reuses it for later comparisons)")
	}
	successorRec, _, _ := repo.GetAssignment(ctx, playerID)
	if successorRec.GetWriterToken() != 9 || successorRec.GetHubPodName() != "pod-B" {
		t.Fatalf("successor must own the record, got %+v", successorRec)
	}

	// 前任(第 7 届)带着自己读到的旧快照迟到回来:覆盖与删除都必须零写入被拒。
	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})
	if _, err = repo.CompareAndSwapAssignment(ctx, playerID, successorRec, newRec("pod-C"), testTTL); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("superseded writer must be rejected on overwrite, got %v", err)
	}
	if _, err = repo.CompareAndSwapAssignment(ctx, playerID, successorRec, nil, 0); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("superseded writer must be rejected on delete, got %v", err)
	}
	after, _, _ := repo.GetAssignment(ctx, playerID)
	if after.GetHubPodName() != "pod-B" || after.GetWriterToken() != 9 {
		t.Fatalf("rejected writes must not mutate the successor's assignment, got %+v", after)
	}

	// 失主副本连读旧记录都不许写(入口快路径)。
	repo.SetWriterFence(&fakeWriterFence{token: 9, held: false})
	if _, err = repo.CompareAndSwapAssignment(ctx, playerID, after, nil, 0); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("not-held replica must be rejected, got %v", err)
	}

	// nil fence(dev / 未启用继任租约)保持 legacy 行为:不比较、不盖水位。
	repo.SetWriterFence(nil)
	legacy := newRec("pod-D")
	if swapped, err = repo.CompareAndSwapAssignment(ctx, playerID, after, legacy, testTTL); err != nil || !swapped {
		t.Fatalf("legacy path must keep working: swapped=%v err=%v", swapped, err)
	}
	legacyStored, _, _ := repo.GetAssignment(ctx, playerID)
	if legacyStored.GetWriterToken() != 0 {
		t.Fatalf("legacy path must not stamp a watermark, got %d", legacyStored.GetWriterToken())
	}
}

// hookedWriterFence 是可编程 WriterFence:每次 Current() 先跑钩子,用来把并发交错
// **精确插进**「WATCH 已注册、事务体尚未读键」的窗口里(R11 复审 P0-4 的两处交错都
// 落在这个窗口,静态 fake 无法表达)。
type hookedWriterFence struct {
	token  uint64
	held   bool
	calls  int
	onCall func(f *hookedWriterFence, call int)
}

func (f *hookedWriterFence) Current() (uint64, bool) {
	f.calls++
	if f.onCall != nil {
		f.onCall(f, f.calls)
	}
	return f.token, f.held
}

func assignmentFixture(playerID uint64, pod string) *hubv1.HubAssignmentStorageRecord {
	return &hubv1.HubAssignmentStorageRecord{
		PlayerId: playerID, HubPodName: pod, HubAddr: "127.0.0.1:7778",
		ShardId: 1, Region: "global", AssignedAtMs: 2000, AssignmentId: "a-" + pod,
	}
}

// R11 复审 P0-4 问题 A(删除即复位 / 借尸还魂)。交错:
//
//	旧写者 token=7 暂停 → 继任者 token=9 创建并**合法删除**归属 → 旧写者恢复
//
// 关闭标准:旧写者零写入、零删除。修复前:删除是裸 DEL,水位随业务记录消失,旧写者
// 看到键不存在便以 token=7 重建归属。
func TestWriterFence_AssignmentDeleteThenReviveByStaleWriterRejected(t *testing.T) {
	ctx := context.Background()
	repo, mr := newRepo(t)
	const playerID = uint64(1001)

	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})
	if swapped, err := repo.CompareAndSwapAssignment(ctx, playerID, nil,
		assignmentFixture(playerID, "pod-A"), testTTL); err != nil || !swapped {
		t.Fatalf("create assignment: swapped=%v err=%v", swapped, err)
	}
	stored, _, _ := repo.GetAssignment(ctx, playerID)

	// 继任者接管后**合法删除**(ReleaseHub 正常路径)。
	repo.SetWriterFence(&fakeWriterFence{token: 9, held: true})
	if swapped, err := repo.CompareAndSwapAssignment(ctx, playerID, stored,
		assignmentFixture(playerID, "pod-B"), testTTL); err != nil || !swapped {
		t.Fatalf("successor swap: swapped=%v err=%v", swapped, err)
	}
	successor, _, _ := repo.GetAssignment(ctx, playerID)
	if deleted, err := repo.CompareAndSwapAssignment(ctx, playerID, successor, nil, 0); err != nil || !deleted {
		t.Fatalf("successor delete: deleted=%v err=%v", deleted, err)
	}
	// 业务上必须无归属,但水位必须留存(否则删除即复位)。
	if _, found, err := repo.GetAssignment(ctx, playerID); err != nil || found {
		t.Fatalf("after delete the player must have no assignment: found=%v err=%v", found, err)
	}
	if !mr.Exists(assignKey(playerID)) {
		t.Fatal("delete must leave a fencing tombstone; a bare DEL resets the watermark")
	}

	// 前任恢复。它读到的是「业务上键不存在」,于是走 expected=nil 的创建路径——
	// 正是审核指出的复活入口。必须零写入被拒。
	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})
	if _, err := repo.CompareAndSwapAssignment(ctx, playerID, nil,
		assignmentFixture(playerID, "pod-C"), testTTL); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("stale writer must not revive a deleted assignment, got %v", err)
	}
	if _, found, _ := repo.GetAssignment(ctx, playerID); found {
		t.Fatal("stale writer revived the assignment: zero-write contract violated")
	}
	// 删除路径同样不得抹掉水位(否则两步就能绕过:先删墓碑再重建)。
	if deleted, err := repo.DeleteAssignmentIfPodMatches(ctx, playerID, "pod-B"); deleted ||
		errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("stale writer must not delete through the pod-match path: deleted=%v err=%v", deleted, err)
	}
	if !mr.Exists(assignKey(playerID)) {
		t.Fatal("stale writer erased the fencing tombstone")
	}

	// 当前写者(第 9 届)在墓碑之上正常重建归属:墓碑不能变成永久拒服。
	repo.SetWriterFence(&fakeWriterFence{token: 9, held: true})
	if swapped, err := repo.CompareAndSwapAssignment(ctx, playerID, nil,
		assignmentFixture(playerID, "pod-D"), testTTL); err != nil || !swapped {
		t.Fatalf("current writer must be able to re-create over a tombstone: swapped=%v err=%v", swapped, err)
	}
	rebuilt, found, _ := repo.GetAssignment(ctx, playerID)
	if !found || rebuilt.GetHubPodName() != "pod-D" || rebuilt.GetWriterToken() != 9 {
		t.Fatalf("re-created assignment wrong: found=%v rec=%+v", found, rebuilt)
	}
}

// R11 复审 P0-4 问题 B(检查后执行:Current() 读在事务外)。交错:
//
//	旧写者进入 CAS → 第 1 次 attempt 期间被继任者写脏 WATCH 键并**失租** → 重试
//
// 关闭标准:重试必须重新读租约并零写入。修复前 mine/held 只在重试循环外读一次,
// 第 2 次 attempt 带着陈旧的「我持有」继续把事务做完。
func TestWriterFence_AssignmentLeaseLostBetweenAttemptsRejected(t *testing.T) {
	ctx := context.Background()
	repo, _ := newRepo(t)
	const playerID = uint64(1002)

	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})
	if swapped, err := repo.CompareAndSwapAssignment(ctx, playerID, nil,
		assignmentFixture(playerID, "pod-A"), testTTL); err != nil || !swapped {
		t.Fatalf("create assignment: swapped=%v err=%v", swapped, err)
	}
	stored, _, _ := repo.GetAssignment(ctx, playerID)

	// 钩子在第 1 次 attempt 里把归属键**原值重写**一次。这一步同时验证两件事:
	//   ① Current() 确实在 Watch 回调内被调用——只有那样,这次写才落在 WATCH 注册之后,
	//      从而写脏乐观锁让 EXEC 失败(若 Current() 仍读在事务外,写发生在 WATCH 之前,
	//      EXEC 会成功,本次 CAS 直接把 pod-C 写下去,测试失败);
	//   ② 重试必须重新读租约——第 2 次 attempt 时本副本已失租。
	// 原值重写而非写入更高 token,是为了把问题 B 与问题 A 的水位比较隔离开:本测试只证
	// 「失租」这一条,不借水位帮忙。
	payload, merr := proto.Marshal(stored)
	if merr != nil {
		t.Fatalf("marshal stored record: %v", merr)
	}
	fence := &hookedWriterFence{token: 7, held: true}
	fence.onCall = func(f *hookedWriterFence, call int) {
		switch call {
		case 1:
			if err := repo.rdb.Set(ctx, assignKey(playerID), payload, testTTL).Err(); err != nil {
				t.Fatalf("dirty the watched key: %v", err)
			}
		case 2:
			f.held = false // 重试时本副本已失租
		}
	}
	repo.SetWriterFence(fence)

	swapped, err := repo.CompareAndSwapAssignment(ctx, playerID, stored,
		assignmentFixture(playerID, "pod-C"), testTTL)
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("writer that lost its lease mid-CAS must be rejected, got swapped=%v err=%v", swapped, err)
	}
	if fence.calls < 2 {
		t.Fatalf("lease must be re-read inside every attempt, got %d reads", fence.calls)
	}
	after, found, _ := repo.GetAssignment(ctx, playerID)
	if !found || after.GetHubPodName() != "pod-A" {
		t.Fatalf("superseded writer mutated the assignment: found=%v rec=%+v", found, after)
	}
}

// 未启用继任租约(dev / 单副本 Recreate)时删除仍是裸 DEL:不引入墓碑,行为不变。
func TestWriterFence_LegacyDeleteStaysBareWithoutFence(t *testing.T) {
	ctx := context.Background()
	repo, mr := newRepo(t)
	const playerID = uint64(1003)
	repo.SetWriterFence(nil)

	if swapped, err := repo.CompareAndSwapAssignment(ctx, playerID, nil,
		assignmentFixture(playerID, "pod-A"), testTTL); err != nil || !swapped {
		t.Fatalf("legacy create: swapped=%v err=%v", swapped, err)
	}
	stored, _, _ := repo.GetAssignment(ctx, playerID)
	if deleted, err := repo.CompareAndSwapAssignment(ctx, playerID, stored, nil, 0); err != nil || !deleted {
		t.Fatalf("legacy delete: deleted=%v err=%v", deleted, err)
	}
	if mr.Exists(assignKey(playerID)) {
		t.Fatal("legacy delete must remove the key outright (no tombstone without fencing)")
	}
}

// 启用 fencing 后无条件 SetAssignment 是 fencing 旁路,必须 fail-closed。
func TestWriterFence_UnconditionalSetAssignmentRejectedWhenFenced(t *testing.T) {
	ctx := context.Background()
	repo, mr := newRepo(t)
	const playerID = uint64(1004)
	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})

	if err := repo.SetAssignment(ctx, assignmentFixture(playerID, "pod-A"), testTTL); errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("unconditional Set must be rejected under fencing, got %v", err)
	}
	if mr.Exists(assignKey(playerID)) {
		t.Fatal("rejected unconditional Set must not write")
	}
}

// R11 复审 P0-4 续:assignment 之外的写入口同样不许被失主旧写者穿过。
//
// **两级契约,不许混为一谈**(键的 slot 决定能做到哪一级):
//
//	A 级(原子 fencing):键与 pod 同 hashtag,水位比较与业务写同一 EXEC。
//	  既挡"已知失主",也挡"被更高 token 继任"。归属/席位/账本/清理索引必须是 A 级。
//	B 级(入口级校验):键无 hashtag(per-team 提示、per-player 冷却),进不了 {pod} 事务。
//	  只挡"已知失主";旧写者若尚未察觉失租仍能写进去。**仅允许**用于带 TTL、
//	  丢失即自愈、不参与准入/归属/容量判定的键。
//
// 本测试把两级分开断言,防止下一轮把 B 级当 A 级宣称收口(或反过来把 B 级当漏洞重报)。
func TestWriterFence_ShardWritePathsRejectStaleWriter(t *testing.T) {
	ctx := context.Background()
	const pod = "pandora-hub-global-1"
	const playerID = uint64(2001)
	const teamID = uint64(3001)
	ref := TransferCleanupRef{PlayerID: playerID, TargetAssignmentID: "target-a"}

	cases := []struct {
		name string
		// atomic=true 即 A 级:落后 token 也必须被拒。
		atomic bool
		// seed 建立"继任者已写入"的既有状态(用无 fence 的仓库完成,避开被拒)。
		seed func(t *testing.T, repo *RedisHubRepo)
		call func(repo *RedisHubRepo) error
		// key 是必须零变化的目标键(空串=只检查返回码)。
		key string
	}{
		{
			name: "RemoveShard", atomic: true, key: shardKey(pod),
			seed: func(t *testing.T, repo *RedisHubRepo) {
				if err := repo.CreateShard(ctx, sampleShard(pod, 1, 0), testTTL); err != nil {
					t.Fatalf("seed shard: %v", err)
				}
			},
			call: func(repo *RedisHubRepo) error { return repo.RemoveShard(ctx, pod) },
		},
		{
			name: "AddShardMember", atomic: true, key: membersKey(pod),
			call: func(repo *RedisHubRepo) error {
				return repo.AddShardMember(ctx, pod, playerID, testTTL)
			},
		},
		{
			name: "RemoveShardMember", atomic: true, key: membersKey(pod),
			seed: func(t *testing.T, repo *RedisHubRepo) {
				if err := repo.AddShardMember(ctx, pod, playerID, testTTL); err != nil {
					t.Fatalf("seed member: %v", err)
				}
			},
			call: func(repo *RedisHubRepo) error {
				return repo.RemoveShardMember(ctx, pod, playerID)
			},
		},
		{
			// 注意:本入口的**全局 pod 索引**(跨 slot)只有 B 级保护,旧写者可能多加一个
			// pod 名。那是持久 superset,只让 reconciler 多扫一轮,不破坏不变量;这里断言的
			// 是承载不变量的 per-pod ref 集合零变化。
			name: "RegisterTransferCleanup", atomic: true, key: transferCleanupKey(pod),
			call: func(repo *RedisHubRepo) error {
				return repo.RegisterTransferCleanup(ctx, pod, ref)
			},
		},
		{
			name: "RemoveTransferCleanup", atomic: true, key: transferCleanupKey(pod),
			seed: func(t *testing.T, repo *RedisHubRepo) {
				if err := repo.RegisterTransferCleanup(ctx, pod, ref); err != nil {
					t.Fatalf("seed cleanup ref: %v", err)
				}
			},
			call: func(repo *RedisHubRepo) error {
				return repo.RemoveTransferCleanup(ctx, pod, ref)
			},
		},
		{
			name: "SetTeamShard", atomic: false, key: teamKey(teamID),
			call: func(repo *RedisHubRepo) error {
				return repo.SetTeamShard(ctx, teamID, pod, testTTL)
			},
		},
		{
			name: "ClearTransferCooldown", atomic: false, key: transferCooldownKey(playerID),
			call: func(repo *RedisHubRepo) error {
				return repo.ClearTransferCooldown(ctx, playerID)
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			repo, mr := newRepo(t)
			if c.seed != nil {
				c.seed(t, repo) // 无 fence 建种子:继任者已经写好的既有状态
			}
			before, _ := mr.Get(c.key)
			// 继任者(第 9 届)已推扫过本 pod 的水位;本副本仍以为自己是第 7 届写者。
			mr.Set(wfenceKey(pod), "9")
			repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})

			err := c.call(repo)
			if c.atomic {
				if errcode.As(err) != errcode.ErrUnavailable {
					t.Fatalf("A 级入口必须拒绝落后 token,got %v", err)
				}
				if after, _ := mr.Get(c.key); after != before {
					t.Fatalf("被拒的写改动了 %s: %q → %q", c.key, before, after)
				}
			} else if err != nil {
				// B 级契约的诚实边界:尚未察觉失租的旧写者**能**写进去。
				// 若这里开始返回错误,说明有人把它升级成了 A 级——好事,但契约变了,
				// 必须同步 writer_fence.go 的分级注释与本用例。
				t.Fatalf("B 级入口在「以为持有」时不应报错(契约已变?),got %v", err)
			}

			// 两级共有的下界:已知失主的副本一律零写入。
			repo.SetWriterFence(&fakeWriterFence{token: 9, held: false})
			lostBefore, _ := mr.Get(c.key)
			if err := c.call(repo); errcode.As(err) != errcode.ErrUnavailable {
				t.Fatalf("已知失主的副本必须被拒,got %v", err)
			}
			if after, _ := mr.Get(c.key); after != lostBefore {
				t.Fatalf("失主副本改动了 %s: %q → %q", c.key, lostBefore, after)
			}
		})
	}
}

// 冷却占坑走 (bool, error) 签名:失主副本不得返回"占坑成功"(B 级下界)。
func TestWriterFence_TryTransferCooldownRejectsLostLease(t *testing.T) {
	ctx := context.Background()
	repo, mr := newRepo(t)
	const playerID = uint64(2002)
	repo.SetWriterFence(&fakeWriterFence{token: 9, held: false})

	ok, err := repo.TryTransferCooldown(ctx, playerID, testTTL)
	if ok || errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("失主副本不得占到冷却坑: ok=%v err=%v", ok, err)
	}
	if mr.Exists(transferCooldownKey(playerID)) {
		t.Fatal("被拒的冷却占坑不得建键")
	}
}

// 当届写者必须能正常通过所有入口,并顺带把水位推进到本届(只进不退)。
func TestWriterFence_CurrentWriterPassesAllShardWritePaths(t *testing.T) {
	ctx := context.Background()
	repo, mr := newRepo(t)
	const pod = "pandora-hub-global-2"
	const playerID = uint64(2003)
	repo.SetWriterFence(&fakeWriterFence{token: 11, held: true})

	if err := repo.CreateShard(ctx, sampleShard(pod, 1, 0), testTTL); err != nil {
		t.Fatalf("CreateShard: %v", err)
	}
	if err := repo.AddShardMember(ctx, pod, playerID, testTTL); err != nil {
		t.Fatalf("AddShardMember: %v", err)
	}
	if err := repo.RegisterTransferCleanup(ctx, pod,
		TransferCleanupRef{PlayerID: playerID, TargetAssignmentID: "t-1"}); err != nil {
		t.Fatalf("RegisterTransferCleanup: %v", err)
	}
	if err := repo.RemoveTransferCleanup(ctx, pod,
		TransferCleanupRef{PlayerID: playerID, TargetAssignmentID: "t-1"}); err != nil {
		t.Fatalf("RemoveTransferCleanup: %v", err)
	}
	if err := repo.RemoveShardMember(ctx, pod, playerID); err != nil {
		t.Fatalf("RemoveShardMember: %v", err)
	}
	if err := repo.SetTeamShard(ctx, 3002, pod, testTTL); err != nil {
		t.Fatalf("SetTeamShard: %v", err)
	}
	if v, _ := mr.Get(wfenceKey(pod)); v != "11" {
		t.Fatalf("current writer must advance the fence to its own term, got %q", v)
	}
	if err := repo.RemoveShard(ctx, pod); err != nil {
		t.Fatalf("RemoveShard: %v", err)
	}
	// 水位是持久键:RemoveShard 删业务镜像但绝不能把水位一起删掉。
	if v, _ := mr.Get(wfenceKey(pod)); v != "11" {
		t.Fatalf("RemoveShard must not erase the fence watermark, got %q", v)
	}
}

func TestWriterFence_AuthRepoInitAndTeardownProofFenced(t *testing.T) {
	ctx := context.Background()
	repo, mr := newAuthRepo(t)
	const pod = "pandora-hub-global-1"
	mr.Set(wfenceKey(pod), "9")
	repo.SetWriterFence(&fakeWriterFence{token: 5, held: true})

	// 授权记录写路径被拒且零写入。
	if _, err := repo.InitAuth(ctx, pod, "uid-A", testTTL); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("stale-token InitAuth must be ErrUnavailable, got %v", err)
	}
	if mr.Exists(authKey(pod)) {
		t.Fatal("rejected InitAuth must not create auth record")
	}
	// teardown proof(解锁 ownership 清理的能力)同样受 fence 约束。
	if err := repo.RecordInstanceTeardownProof(ctx, pod, "uid-A", testTTL); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("stale-token teardown proof must be ErrUnavailable, got %v", err)
	}
	if mr.Exists(instanceTeardownProofKey(pod)) {
		t.Fatal("rejected teardown proof must not be recorded")
	}

	// 当前写者正常通过并推进水位。
	repo.SetWriterFence(&fakeWriterFence{token: 10, held: true})
	if _, err := repo.InitAuth(ctx, pod, "uid-A", testTTL); err != nil {
		t.Fatalf("current-writer InitAuth must pass: %v", err)
	}
	if v, _ := mr.Get(wfenceKey(pod)); v != "10" {
		t.Fatalf("InitAuth must advance fence to 10, got %q", v)
	}
	if err := repo.RecordInstanceTeardownProof(ctx, pod, "uid-A", testTTL); err != nil {
		t.Fatalf("current-writer teardown proof must pass: %v", err)
	}
}

