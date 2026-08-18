// owner_authority_test.go — census 代提交 Admit 缓存有界性回归测试(压测前审核 P1)。
//
// 覆盖 ownerAdmitCensusWeak 的 last-touch time.Time 值 + 本实例 census 剪枝,以及
// sweepStaleOwnerAdmitted 对死实例(UID 不再心跳续期)项的 TTL 清扫——防 ownerAdmitted
// sync.Map 随累计对局的历史 InstanceUID 无界增长导致 ds_allocator OOM。
package biz

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/placement"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// scriptedOwnerAuthority:QueryOwner 按注入记录应答;记录 Admit / Release / Begin 调用。
type scriptedOwnerAuthority struct {
	mu       sync.Mutex
	records  map[uint64]data.OwnerRecordView
	admits   []uint64
	releases []uint64
	// releaseErr 非 nil 时 ReleaseOwner 直接失败,用于验证弱依赖只告警不中断。
	releaseErr error

	// Begin 侧故障注入(contract 强依赖回归):beginOps 记录收到的 operation_id,
	// queries 记录 Query 次数(验证冲突后不得拿旧 target 盲重试)。
	beginOps                []string
	queries                 int
	queryErr                error
	queryErrAfter           int // >0 = 前 N 次 Query 成功，之后注入 queryErr
	beginErr                error
	beginErrForPlayer       uint64 // 非 0 = 只对该玩家注入 beginErr
	beginCommitErr          error
	beginCommitErrForPlayer uint64 // 非 0 = 只对该玩家 commit 后丢失回包
	conflictTimes           int    // 剩余需返回 EPOCH_CONFLICT 的次数(独立于 beginErr)
	conflictRecord          *data.OwnerRecordView
	beginResult             *data.OwnerRecordView
	beginStarted            chan struct{}
	beginProceed            <-chan struct{}
	beginStartOnce          sync.Once
	mintSeq                 int // 模拟权威铸造 operation_id 的自增序号
}

func (s *scriptedOwnerAuthority) QueryOwner(_ context.Context, playerID uint64) (data.OwnerRecordView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries++
	if s.queryErr != nil && (s.queryErrAfter == 0 || s.queries > s.queryErrAfter) {
		return data.OwnerRecordView{}, s.queryErr
	}
	return s.records[playerID], nil
}

func (s *scriptedOwnerAuthority) BeginTransition(ctx context.Context, playerID, _ uint64, operationID string, ownerType int8, target data.OwnerTargetView) (data.OwnerRecordView, error) {
	if s.beginStarted != nil {
		s.beginStartOnce.Do(func() { close(s.beginStarted) })
	}
	if s.beginProceed != nil {
		select {
		case <-s.beginProceed:
		case <-ctx.Done():
			return data.OwnerRecordView{}, ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beginOps = append(s.beginOps, operationID)
	if s.conflictTimes > 0 {
		s.conflictTimes--
		if s.conflictRecord != nil {
			s.records[playerID] = *s.conflictRecord
		}
		// 冲突时权威回传的是**当前记录**(owner.proto BeginTransitionResponse.record)。
		return s.records[playerID], errcode.New(errcode.ErrOwnerEpochConflict, "injected epoch conflict")
	}
	if s.beginErr != nil && (s.beginErrForPlayer == 0 || s.beginErrForPlayer == playerID) {
		return data.OwnerRecordView{}, s.beginErr
	}
	if s.beginResult != nil {
		return *s.beginResult, nil
	}
	rec := s.records[playerID]
	// 模拟 owner 行锁内的 same-target no-op：返回既有 operation，不把请求方 operation
	// 当成新写落库。DS 侧据此判定 created=false，不能回滚别人的合法 owner。
	if ownerRecordExactlyTargets(rec, ownerType, target) {
		return rec, nil
	}
	// 模拟 owner 行锁事务:epoch+1、铸造 operation_id、记录改指向本次目标。
	// operation_id 必须真的铸出来并回传——调用方的精确回滚正是拿它 + epoch 做
	// compare-delete(见下方 ReleaseOwner),回传空值会让回滚静默失效而测试仍然"通过"。
	minted := operationID
	if minted == "" {
		s.mintSeq++
		minted = fmt.Sprintf("op-%d-%d", playerID, s.mintSeq)
	}
	next := data.OwnerRecordView{
		OwnerEpoch: rec.OwnerEpoch + 1, OwnerType: ownerType, Phase: ownerPhasePending,
		PodName: target.PodName, InstanceUID: target.InstanceUID, InstanceEpoch: target.InstanceEpoch,
		AssignmentOrAllocationID: target.AssignmentOrAllocationID, ReleaseTrack: target.ReleaseTrack,
		OperationID: minted,
	}
	s.records[playerID] = next
	if s.beginCommitErr != nil &&
		(s.beginCommitErrForPlayer == 0 || s.beginCommitErrForPlayer == playerID) {
		// 模拟服务端已 commit requested operation，但 transport 丢失响应。
		return data.OwnerRecordView{}, s.beginCommitErr
	}
	return next, nil
}

// contract 阶段:READY 交付点 Begin 是强依赖——写不进 owner 权威即整体失败,
// 调用方据此走既有分配失败补偿链,绝不把 ds_addr 交付出去。
func TestOwnerBeginPlayers_StrongFailClosed(t *testing.T) {
	target := data.OwnerTargetView{PodName: "battle-1", InstanceUID: "uid-1", InstanceEpoch: 1,
		AssignmentOrAllocationID: "alloc-1", ReleaseTrack: "stable"}
	ctx := context.Background()

	failing := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{},
		beginErr: errcode.New(errcode.ErrUnavailable, "owner down")}
	if err := ownerBeginPlayers(ctx, failing, []uint64{1001}, ownerTypeBattle, target, time.Second); err == nil {
		t.Fatal("Begin 失败必须上抛,不得交付 READY")
	} else if errors.Is(err, errOwnerBeginOutcomeUnknown) {
		t.Fatalf("readback 明确 non-exact 时应返回原 Begin 错误,不能误报 outcome unknown: %v", err)
	}

	// Query 不可判定同样 fail-closed(§9.22 禁冒充 OFFLINE/空闲)。
	qfail := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{},
		queryErr: errcode.New(errcode.ErrUnavailable, "owner unreachable")}
	if err := ownerBeginPlayers(ctx, qfail, []uint64{1001}, ownerTypeBattle, target, time.Second); err == nil {
		t.Fatal("Query 不可判定必须 fail-closed")
	}

	// 一局里任一玩家失败即整局失败:不留"部分玩家有归属、部分没有"的半截状态。
	partial := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{},
		beginErrForPlayer: 2002, beginErr: errcode.New(errcode.ErrUnavailable, "owner down")}
	if err := ownerBeginPlayers(ctx, partial, []uint64{1001, 2002, 3003}, ownerTypeBattle, target, time.Second); err == nil {
		t.Fatal("一局内任一玩家失败必须整局失败")
	}

	// owner 未部署(auth==nil)不阻断分配。
	if err := ownerBeginPlayers(ctx, nil, []uint64{1001}, ownerTypeBattle, target, time.Second); err != nil {
		t.Fatalf("auth==nil 不应报错: %v", err)
	}

	// DS 批量补偿必须携显式 UUIDv4 operation:真实写原样回传后才可证明是本调用 grant；
	// same-target no-op 会保留既有 operation，因此不会被误纳入 rollback。
	ok := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{}}
	if err := ownerBeginPlayers(ctx, ok, []uint64{1001, 2002}, ownerTypeBattle, target, time.Second); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	for _, op := range ok.beginOps {
		if !placement.ValidOperationID(op) {
			t.Fatalf("operation 必须是 canonical UUIDv4,实得 %q", op)
		}
	}

	// 部分失败必须**精确回滚**已写进权威的那几个玩家。
	//
	// 不回滚的后果(本回归要钉死的就是它):失败点之前的归属记录指向的 Pod,紧接着就被
	// 调用方 cleanupAllocatedBattle 删掉;而 owner 侧没有任何归属记录的 TTL / 回收
	// (唯一的 sweep 是 RunTransitionLogSweep,清的是审计流水)。玩家若就此离线,记录
	// 永久停在 PENDING 指向死 Pod;下次登录时 login 的 query-first
	// (account/login/internal/biz/owner_query.go applyOwnerPlacement)在屏障已开时会把它
	// 翻译成 TARGET+PENDING,把死 Pod 当 exact target 下发,客户端反复连一台不存在的 DS。
	rollback := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{},
		beginErrForPlayer: 3003, beginErr: errcode.New(errcode.ErrUnavailable, "owner down")}
	if err := ownerBeginPlayers(ctx, rollback, []uint64{1001, 2002, 3003}, ownerTypeBattle, target, time.Second); err == nil {
		t.Fatal("一局内任一玩家失败必须整局失败")
	}
	// 1001/2002 已写进权威 → 必须被撤销;3003 从未写成功 → 不该被 Release。
	if len(rollback.releases) != 2 {
		t.Fatalf("已写入的 2 个玩家必须回滚,实得 releases=%v", rollback.releases)
	}
	for _, pid := range []uint64{1001, 2002} {
		if _, ok := rollback.records[pid]; ok {
			t.Fatalf("玩家 %d 的归属记录必须被撤销,实际仍残留指向已回收实例", pid)
		}
	}
	// 逆序释放:先撤最后写进去的。
	if rollback.releases[0] != 2002 || rollback.releases[1] != 1001 {
		t.Fatalf("回滚应逆序释放,实得 %v", rollback.releases)
	}

	// 全部成功时一次 Release 都不许发:补偿只撤销"本次未交付"的写(验收底线第 4 条),
	// 绝不能顺手把玩家已生效的归属也撤掉。
	allOK := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{}}
	if err := ownerBeginPlayers(ctx, allOK, []uint64{1001, 2002}, ownerTypeBattle, target, time.Second); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if len(allOK.releases) != 0 {
		t.Fatalf("全部成功不得回滚,实得 releases=%v", allOK.releases)
	}
	if len(allOK.records) != 2 {
		t.Fatalf("全部成功后两份归属都应留在权威,实得 %d 份", len(allOK.records))
	}

	// 回滚本身失败只告警不改变返回值:此时 owner 本就不可用,把回滚错误盖掉原始错误
	// 会让上层误判失败原因。
	rbFail := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{},
		beginErrForPlayer: 2002, beginErr: errcode.New(errcode.ErrUnavailable, "owner down"),
		releaseErr: errors.New("owner still down")}
	err := ownerBeginPlayers(ctx, rbFail, []uint64{1001, 2002}, ownerTypeBattle, target, time.Second)
	if err == nil {
		t.Fatal("Begin 失败必须上抛")
	}
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("上抛的必须是 Begin 的原始错误,实得 %v", err)
	}
}

// 同 target 幂等 Begin 返回的是既有 owner，不是本调用创建的 grant。若后续 roster 玩家
// 失败，补偿绝不能 Release 这份已生效归属；否则一次失败重试会删掉合法 owner。
func TestOwnerBeginPlayers_DoesNotRollbackPreexistingExactNoop(t *testing.T) {
	target := data.OwnerTargetView{
		PodName: "battle-1", InstanceUID: "uid-1", InstanceEpoch: 1,
		AssignmentOrAllocationID: "allocation-1", ReleaseTrack: "stable",
	}
	existing := data.OwnerRecordView{
		OwnerEpoch: 7, OwnerType: ownerTypeBattle, Phase: ownerPhaseAdmitted,
		PodName: target.PodName, InstanceUID: target.InstanceUID, InstanceEpoch: target.InstanceEpoch,
		AssignmentOrAllocationID: target.AssignmentOrAllocationID, ReleaseTrack: target.ReleaseTrack,
		OperationID: "00000000-0000-4000-8000-000000000007",
	}
	auth := &scriptedOwnerAuthority{
		records: map[uint64]data.OwnerRecordView{
			1001: existing,
		},
		beginErrForPlayer: 2002,
		beginErr:          errcode.New(errcode.ErrUnavailable, "owner down"),
	}

	err := ownerBeginPlayers(context.Background(), auth, []uint64{1001, 2002}, ownerTypeBattle, target, time.Second)
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("第二名玩家 Begin 失败必须上抛: code=%v err=%v", errcode.As(err), err)
	}
	if len(auth.releases) != 0 {
		t.Fatalf("既有 exact no-op 不是本次 grant,不得回滚: releases=%v", auth.releases)
	}
	if got := auth.records[1001]; got != existing {
		t.Fatalf("后续玩家失败误改了既有 exact owner: got=%+v want=%+v", got, existing)
	}
	if len(auth.beginOps) != 2 || !placement.ValidOperationID(auth.beginOps[0]) ||
		auth.beginOps[0] == existing.OperationID {
		t.Fatalf("每次 Begin 应携新显式 operation 供 provenance 判定: ops=%v existing=%q",
			auth.beginOps, existing.OperationID)
	}
}

// Begin 已在服务端 commit requested operation、但 gRPC 回包丢失时，非冲突错误后的
// one-shot Query 应把确定提交收为成功，不能误走 cleanup 把 owner 指向的 Pod 删掉。
func TestBeginOnePlayer_AdoptsCommittedBeginAfterLostResponse(t *testing.T) {
	target := data.OwnerTargetView{
		PodName: "battle-commit", InstanceUID: "uid-commit", InstanceEpoch: 9,
		AssignmentOrAllocationID: "allocation-commit", ReleaseTrack: "stable",
	}
	auth := &scriptedOwnerAuthority{
		records:        map[uint64]data.OwnerRecordView{},
		beginCommitErr: errcode.New(errcode.ErrUnavailable, "response lost after commit"),
	}

	got, created, err := beginOnePlayer(context.Background(), auth, 1001, ownerTypeBattle, target)
	if err != nil || !created {
		t.Fatalf("commit 后丢回包应由 exact readback 收敛成功: %v", err)
	}
	if auth.queries != 2 || len(auth.beginOps) != 1 {
		t.Fatalf("应为初始 Query+单次 Begin+一次 readback: queries=%d begins=%d",
			auth.queries, len(auth.beginOps))
	}
	if !ownerRecordExactlyTargets(got, ownerTypeBattle, target) || got.OperationID != auth.beginOps[0] {
		t.Fatalf("readback 未认领本次 requested operation: got=%+v ops=%v", got, auth.beginOps)
	}
	if len(auth.releases) != 0 {
		t.Fatalf("已确认提交的 Begin 不得被 Release: releases=%v", auth.releases)
	}
}

// Begin 返回非冲突错误，但回读发现已有另一 operation 的 full-exact target 时，这是
// 服务端同 target no-op 的确定结果；应成功复用且 created=false，避免误入本次补偿集。
func TestBeginOnePlayer_AdoptsPreexistingExactAfterBeginError(t *testing.T) {
	target := data.OwnerTargetView{
		PodName: "battle-existing", InstanceUID: "uid-existing", InstanceEpoch: 3,
		AssignmentOrAllocationID: "allocation-existing", ReleaseTrack: "stable",
	}
	existing := data.OwnerRecordView{
		OwnerEpoch: 4, OwnerType: ownerTypeBattle, Phase: ownerPhaseAdmitted,
		PodName: target.PodName, InstanceUID: target.InstanceUID, InstanceEpoch: target.InstanceEpoch,
		AssignmentOrAllocationID: target.AssignmentOrAllocationID, ReleaseTrack: target.ReleaseTrack,
		OperationID: "00000000-0000-4000-8000-000000000004",
	}
	auth := &scriptedOwnerAuthority{
		records:  map[uint64]data.OwnerRecordView{1001: existing},
		beginErr: errcode.New(errcode.ErrUnavailable, "same-target response lost"),
	}

	got, created, err := beginOnePlayer(context.Background(), auth, 1001, ownerTypeBattle, target)
	if err != nil || created || got != existing {
		t.Fatalf("existing exact readback 应按 no-op 收敛: got=%+v created=%v err=%v", got, created, err)
	}
	if auth.queries != 2 || len(auth.beginOps) != 1 || auth.beginOps[0] == existing.OperationID {
		t.Fatalf("应为单次 Begin 后回读既有 operation: queries=%d ops=%v existing=%q",
			auth.queries, auth.beginOps, existing.OperationID)
	}
}

// 若 Begin 可能已提交且判定 Query 也失败，整批结果不可判定。此时此前成功 grants 与
// 当前可能提交的 owner 都必须原样保留；回滚半批会主动破坏恢复后只读收敛的可能性。
func TestOwnerBeginPlayers_OutcomeUnknownRetainsWholeBatch(t *testing.T) {
	target := data.OwnerTargetView{
		PodName: "battle-unknown", InstanceUID: "uid-unknown", InstanceEpoch: 11,
		AssignmentOrAllocationID: "allocation-unknown", ReleaseTrack: "canary",
	}
	auth := &scriptedOwnerAuthority{
		records:                 map[uint64]data.OwnerRecordView{},
		queryErr:                errcode.New(errcode.ErrUnavailable, "readback unavailable"),
		queryErrAfter:           2, // p1/p2 初始 Query 成功，p2 commit 后 readback 失败。
		beginCommitErr:          errcode.New(errcode.ErrUnavailable, "response lost after commit"),
		beginCommitErrForPlayer: 2002,
	}

	err := ownerBeginPlayers(context.Background(), auth, []uint64{1001, 2002}, ownerTypeBattle, target, time.Second)
	if errcode.As(err) != errcode.ErrUnavailable || !errors.Is(err, errOwnerBeginOutcomeUnknown) {
		t.Fatalf("回读也失败必须返回 outcome-unknown sentinel: code=%v err=%v",
			errcode.As(err), err)
	}
	if auth.queries != 3 || len(auth.beginOps) != 2 {
		t.Fatalf("不得重放 Begin，只允许失败后 Query 判定: queries=%d begins=%d",
			auth.queries, len(auth.beginOps))
	}
	if len(auth.releases) != 0 {
		t.Fatalf("outcome unknown 不得回滚此前 grants: releases=%v", auth.releases)
	}
	for i, playerID := range []uint64{1001, 2002} {
		got := auth.records[playerID]
		if !ownerRecordExactlyTargets(got, ownerTypeBattle, target) || got.OperationID != auth.beginOps[i] {
			t.Fatalf("玩家 %d 的潜在可收敛 owner 被破坏: got=%+v ops=%v", playerID, got, auth.beginOps)
		}
	}
}

// EPOCH_CONFLICT 可能表示更新的 allocation/target 已经赢得 owner CAS。helper 不得重查
// epoch 后继续拿旧 target 写第二次；必须把冲突交给外层整条分配链重读 allocation。
func TestOwnerBeginPlayers_DoesNotBlindRetryEpochConflict(t *testing.T) {
	staleTarget := data.OwnerTargetView{
		PodName: "battle-old", InstanceUID: "uid-old", InstanceEpoch: 1,
		AssignmentOrAllocationID: "allocation-old", ReleaseTrack: "stable",
	}
	winner := data.OwnerRecordView{
		OwnerEpoch: 8, OwnerType: ownerTypeBattle, Phase: ownerPhasePending,
		PodName: "battle-new", InstanceUID: "uid-new", InstanceEpoch: 2,
		AssignmentOrAllocationID: "allocation-new", ReleaseTrack: "canary",
		OperationID: "00000000-0000-4000-8000-000000000008",
	}
	auth := &scriptedOwnerAuthority{
		records: map[uint64]data.OwnerRecordView{
			1001: {
				OwnerEpoch: 7, OwnerType: ownerTypeHub, Phase: ownerPhaseAdmitted,
				PodName: "hub-old", InstanceUID: "hub-uid-old", InstanceEpoch: 1,
				AssignmentOrAllocationID: "assignment-old", ReleaseTrack: "stable",
				OperationID: "00000000-0000-4000-8000-000000000007",
			},
		},
		conflictTimes:  1,
		conflictRecord: &winner,
	}

	err := ownerBeginPlayers(context.Background(), auth, []uint64{1001}, ownerTypeBattle, staleTarget, time.Second)
	if errcode.As(err) != errcode.ErrOwnerEpochConflict {
		t.Fatalf("epoch conflict 必须原样 fail-closed: code=%v err=%v", errcode.As(err), err)
	}
	if auth.queries != 1 || len(auth.beginOps) != 1 {
		t.Fatalf("冲突后不得拿旧 target 盲重试: queries=%d begins=%d", auth.queries, len(auth.beginOps))
	}
	got := auth.records[1001]
	if got != winner {
		t.Fatalf("冲突 winner 被旧 allocation 覆盖: got=%+v want=%+v", got, winner)
	}
	if len(auth.releases) != 0 {
		t.Fatalf("未成功写入的冲突请求不得 Release winner: releases=%v", auth.releases)
	}
}

// 滚动升级期间旧 owner binary 可能把同物理实例、不同 allocation 当 no-op：RPC nil，
// 但 response.record 仍是旧 target。调用方必须检查完整回包，不能交付 READY。
func TestOwnerBeginPlayers_RejectsNonExactBeginResult(t *testing.T) {
	target := data.OwnerTargetView{
		PodName: "battle-1", InstanceUID: "uid-1", InstanceEpoch: 1,
		AssignmentOrAllocationID: "allocation-new", ReleaseTrack: "stable",
	}
	valid := data.OwnerRecordView{
		OwnerEpoch: 7, OwnerType: ownerTypeBattle, Phase: ownerPhaseAdmitted,
		PodName: "battle-1", InstanceUID: "uid-1", InstanceEpoch: 1,
		AssignmentOrAllocationID: "allocation-new", ReleaseTrack: "stable",
		OperationID: "00000000-0000-4000-8000-000000000007",
	}
	tests := []struct {
		name   string
		mutate func(*data.OwnerRecordView)
	}{
		{name: "owner_epoch", mutate: func(rec *data.OwnerRecordView) { rec.OwnerEpoch = 0 }},
		{name: "operation_id", mutate: func(rec *data.OwnerRecordView) { rec.OperationID = " " }},
		{name: "owner_type", mutate: func(rec *data.OwnerRecordView) { rec.OwnerType = ownerTypeHub }},
		{name: "phase", mutate: func(rec *data.OwnerRecordView) { rec.Phase = 0 }},
		{name: "pod", mutate: func(rec *data.OwnerRecordView) { rec.PodName = "battle-old" }},
		{name: "instance_uid", mutate: func(rec *data.OwnerRecordView) { rec.InstanceUID = "uid-old" }},
		{name: "instance_epoch", mutate: func(rec *data.OwnerRecordView) { rec.InstanceEpoch = 2 }},
		{name: "allocation", mutate: func(rec *data.OwnerRecordView) {
			rec.AssignmentOrAllocationID = "allocation-old"
		}},
		{name: "release_track", mutate: func(rec *data.OwnerRecordView) { rec.ReleaseTrack = "canary" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stale := valid
			tc.mutate(&stale)
			auth := &scriptedOwnerAuthority{
				records:     map[uint64]data.OwnerRecordView{1001: stale},
				beginResult: &stale,
			}

			err := ownerBeginPlayers(context.Background(), auth, []uint64{1001}, ownerTypeBattle, target, time.Second)
			if errcode.As(err) != errcode.ErrInvalidState {
				t.Fatalf("non-exact Begin record 必须拒绝 READY: code=%v err=%v", errcode.As(err), err)
			}
			if auth.queries != 1 || len(auth.beginOps) != 1 {
				t.Fatalf("non-exact 回包只允许一轮 Query+Begin: queries=%d begins=%d", auth.queries, len(auth.beginOps))
			}
			if auth.records[1001] != stale {
				t.Fatalf("拒绝 non-exact 回包不得改写既有 owner: got=%+v want=%+v", auth.records[1001], stale)
			}
			if len(auth.releases) != 0 {
				t.Fatalf("non-exact no-op 不是本次 grant,不得 Release 既有 owner: releases=%v", auth.releases)
			}
		})
	}
}

func (s *scriptedOwnerAuthority) Admit(_ context.Context, playerID, _ uint64, _ string, _ data.OwnerTargetView) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.admits = append(s.admits, playerID)
	rec := s.records[playerID]
	rec.Phase = ownerPhaseAdmitted
	s.records[playerID] = rec
	return 0, nil
}

func (s *scriptedOwnerAuthority) ReleaseOwner(_ context.Context, playerID, ownerEpoch uint64, operationID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.releaseErr != nil {
		return s.releaseErr
	}
	// compare-delete 语义:epoch/operation 必须与当前记录一致才生效,否则视为陈旧释放。
	rec := s.records[playerID]
	if rec.OwnerEpoch != ownerEpoch || rec.OperationID != operationID {
		return errStaleOwnerReleaseForTest
	}
	s.releases = append(s.releases, playerID)
	delete(s.records, playerID)
	return nil
}

// errStaleOwnerReleaseForTest 模拟 owner 侧对 epoch 不匹配释放的拒绝。
var errStaleOwnerReleaseForTest = errors.New("stale owner release rejected")

func pendingBattleRecord(pod, uid string, epoch uint64) data.OwnerRecordView {
	return data.OwnerRecordView{
		OwnerEpoch: epoch, OwnerType: ownerTypeBattle, Phase: ownerPhasePending,
		PodName: pod, InstanceUID: uid, InstanceEpoch: 1,
		AssignmentOrAllocationID: "a1", ReleaseTrack: "stable", OperationID: "op1",
	}
}

// 玩家离场再回流(owner epoch 推进、新 PENDING)后,census 缓存必须被剪枝并重新 Admit,
// 不得被上一纪元的 admitted 缓存误吞。修复前值为 struct{}{}、无剪枝,回流玩家会被误吞。
func TestOwnerAdmitCensus_CachePrunedOnDepartureThenReadmits(t *testing.T) {
	const pod, uid = "battle-1", "uid-1"
	auth := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{
		1001: pendingBattleRecord(pod, uid, 3),
	}}
	var admitted sync.Map
	ctx := context.Background()

	// 第一轮:PENDING → Admit,进缓存。
	ownerAdmitCensusWeak(ctx, auth, &admitted, []uint64{1001}, ownerTypeBattle, pod, uid, time.Second)
	if len(auth.admits) != 1 {
		t.Fatalf("first census must admit once, got %d", len(auth.admits))
	}
	// 缓存值必须是 last-touch time.Time(不再是 struct{}{}),否则 TTL sweep 无法判老化。
	if v, ok := admitted.Load(uid + "|1001"); !ok {
		t.Fatal("census 应把 1001 写入缓存")
	} else if _, isTime := v.(time.Time); !isTime {
		t.Fatalf("缓存值必须为 time.Time(last-touch),实得 %T", v)
	}
	// 第二轮:仍在场,缓存命中,零新 Admit。
	ownerAdmitCensusWeak(ctx, auth, &admitted, []uint64{1001}, ownerTypeBattle, pod, uid, time.Second)
	if len(auth.admits) != 1 {
		t.Fatalf("cached player must not re-admit, got %d", len(auth.admits))
	}
	// 玩家离场:census 换成另一在场玩家 → 1001 被剪枝。
	auth.records[2002] = pendingBattleRecord(pod, uid, 1)
	ownerAdmitCensusWeak(ctx, auth, &admitted, []uint64{2002}, ownerTypeBattle, pod, uid, time.Second)
	if _, ok := admitted.Load(uid + "|1001"); ok {
		t.Fatal("离场玩家的 admitted 缓存项必须被剪枝")
	}
	// 回流:owner epoch 已推进、新 PENDING → 必须重新 Admit。
	auth.records[1001] = pendingBattleRecord(pod, uid, 9)
	ownerAdmitCensusWeak(ctx, auth, &admitted, []uint64{1001, 2002}, ownerTypeBattle, pod, uid, time.Second)
	admitsFor1001 := 0
	for _, id := range auth.admits {
		if id == 1001 {
			admitsFor1001++
		}
	}
	if admitsFor1001 != 2 {
		t.Fatalf("returning player with advanced epoch must be re-admitted, got %d admits", admitsFor1001)
	}
}

// sweepStaleOwnerAdmitted:已销毁实例(UID 不再心跳续期)的项老化超 cutoff 被清,活实例
// (刚续期)的项保留;非 time.Time 值 fail-safe 清除。防 ownerAdmitted 随历史 UID 无界增长。
func TestSweepStaleOwnerAdmitted(t *testing.T) {
	const pod, uid = "battle-1", "uid-1"
	auth := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{
		1001: pendingBattleRecord(pod, uid, 3),
	}}
	var admitted sync.Map
	// 一轮 census:1001 进缓存(值为 last-touch time.Time)。
	ownerAdmitCensusWeak(context.Background(), auth, &admitted, []uint64{1001}, ownerTypeBattle, pod, uid, time.Second)
	if _, ok := admitted.Load(uid + "|1001"); !ok {
		t.Fatal("census 应把 1001 写入缓存")
	}
	// 模拟死实例遗留项(旧 UID,last-touch 很久以前)。
	admitted.Store("uid-DEAD|2002", time.Now().Add(-time.Hour))
	// cutoff = now-ownerAdmittedStaleTTL:活实例项(刚续期)保留,死实例项清除。
	sweepStaleOwnerAdmitted(&admitted, time.Now().Add(-ownerAdmittedStaleTTL))
	if _, ok := admitted.Load(uid + "|1001"); !ok {
		t.Fatal("活实例(刚续期)缓存项不应被清")
	}
	if _, ok := admitted.Load("uid-DEAD|2002"); ok {
		t.Fatal("死实例(超 TTL 未续期)缓存项必须被清")
	}
	// 非 time.Time 值(修复前的 struct{}{})也应被 fail-safe 清除。
	admitted.Store("uid-BAD|3003", struct{}{})
	sweepStaleOwnerAdmitted(&admitted, time.Now().Add(-ownerAdmittedStaleTTL))
	if _, ok := admitted.Load("uid-BAD|3003"); ok {
		t.Fatal("非 time.Time 值应被 fail-safe 清除")
	}
}

// ── 判弃后释放 owner(INC-20260729-002 P0-B1)────────────────────────────────

// 记录仍指向被判弃实例 → 必须释放,否则 query-first 恢复会一直返回已删除的 battle Pod。
func TestOwnerReleaseAbandoned_ReleasesPlayersStillOwnedBySelf(t *testing.T) {
	const pod, uid = "battle-9", "uid-9"
	auth := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{
		2001: pendingBattleRecord(pod, uid, 5),
		2002: pendingBattleRecord(pod, uid, 6),
	}}

	ownerReleaseAbandonedPlayersWeak(context.Background(), auth,
		[]uint64{2001, 2002}, pod, uid, time.Second)

	if len(auth.releases) != 2 {
		t.Fatalf("仍归属本实例的玩家必须全部释放, got %v", auth.releases)
	}
	if len(auth.records) != 0 {
		t.Fatalf("释放后记录应清空, 剩余 %d", len(auth.records))
	}
}

// 玩家已被迁到别的 DS(pod/uid 已变)→ 绝不能释放,否则会误删活归属(双 DS / 掉线)。
func TestOwnerReleaseAbandoned_SkipsPlayersMigratedElsewhere(t *testing.T) {
	const deadPod, deadUID = "battle-old", "uid-old"
	auth := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{
		// 已迁到新实例。
		3001: pendingBattleRecord("battle-new", "uid-new", 8),
		// 已回 Hub(类型不同)。
		3002: {OwnerEpoch: 9, OwnerType: ownerTypeHub, Phase: ownerPhaseAdmitted,
			PodName: deadPod, InstanceUID: deadUID, OperationID: "op-hub"},
	}}

	ownerReleaseAbandonedPlayersWeak(context.Background(), auth,
		[]uint64{3001, 3002}, deadPod, deadUID, time.Second)

	if len(auth.releases) != 0 {
		t.Fatalf("不指向本实例的记录一律不得释放, got %v", auth.releases)
	}
	if len(auth.records) != 2 {
		t.Fatalf("跳过的记录必须原样保留, 剩余 %d", len(auth.records))
	}
}

// owner 抖动只降级为告警:释放失败不得让调用方(deliverAbandoned)中断补偿链。
func TestOwnerReleaseAbandoned_WeakOnFailure(t *testing.T) {
	const pod, uid = "battle-7", "uid-7"
	auth := &scriptedOwnerAuthority{
		records:    map[uint64]data.OwnerRecordView{4001: pendingBattleRecord(pod, uid, 2)},
		releaseErr: errors.New("owner unavailable"),
	}

	// 不 panic、正常返回即为通过(弱依赖语义);记录保持原样等下一轮 sweep 重试。
	ownerReleaseAbandonedPlayersWeak(context.Background(), auth, []uint64{4001}, pod, uid, time.Second)

	if len(auth.releases) != 0 {
		t.Fatalf("释放失败不应记为已释放, got %v", auth.releases)
	}
	if len(auth.records) != 1 {
		t.Fatal("释放失败时记录必须保留,等待下一轮重试")
	}
}

// auth 为 nil(owner 未启用)或身份缺失时必须直接 no-op,不得 panic。
func TestOwnerReleaseAbandoned_NilAndMissingIdentityAreNoop(t *testing.T) {
	ownerReleaseAbandonedPlayersWeak(context.Background(), nil, []uint64{1}, "pod", "uid", time.Second)

	auth := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{
		5001: pendingBattleRecord("pod", "uid", 1),
	}}
	// 缺 pod / uid 时无法做 exact 身份门,必须整体跳过而不是盲删。
	ownerReleaseAbandonedPlayersWeak(context.Background(), auth, []uint64{5001}, "", "uid", time.Second)
	ownerReleaseAbandonedPlayersWeak(context.Background(), auth, []uint64{5001}, "pod", "", time.Second)
	if len(auth.releases) != 0 {
		t.Fatalf("身份缺失时不得释放任何记录, got %v", auth.releases)
	}
}
