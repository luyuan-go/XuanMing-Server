// owner_authority_test.go — 签票点强 Begin(contract 阶段 fail-closed)、census 代提交
// Admit 缓存剪枝(复审 P1-2)与漂移自愈 Begin(复审 P1-3)单元测试。
package biz

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"

	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/data"
)

// scriptedOwnerAuthority:QueryOwner 按注入记录应答;记录 Admit/Begin 调用。
// 故障注入:queryErr / beginErr(可限定 beginErrForPlayer),conflictTimes 控制前 N 次
// Begin 返回 EPOCH_CONFLICT 后转成功(模拟"被另一写者插队后重查即可推进")。
type scriptedOwnerAuthority struct {
	mu      sync.Mutex
	records map[uint64]data.OwnerRecordView
	admits  []uint64
	begins  []uint64
	// beginOps 记录每次 Begin 收到的 operation_id(断言调用方不再自铸)。
	beginOps []string
	queries  int

	queryErr          error
	beginErr          error
	beginErrForPlayer uint64 // 非 0 = 只对该玩家注入 beginErr
	conflictTimes     int    // 剩余需返回 EPOCH_CONFLICT 的次数
}

func (s *scriptedOwnerAuthority) QueryOwner(_ context.Context, playerID uint64) (data.OwnerRecordView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries++
	if s.queryErr != nil {
		return data.OwnerRecordView{}, s.queryErr
	}
	return s.records[playerID], nil
}

func (s *scriptedOwnerAuthority) BeginTransition(_ context.Context, playerID, _ uint64, operationID string, _ int8, target data.OwnerTargetView) (data.OwnerRecordView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beginOps = append(s.beginOps, operationID)
	// 冲突注入独立于 beginErr:耗尽后本次调用按正常路径推进,否则"冲突 N 次后成功"
	// 这个场景根本构造不出来(耗尽了还会掉进 beginErr 分支继续报错)。
	if s.conflictTimes > 0 {
		s.conflictTimes--
		// 冲突时权威回传的是**当前记录**(owner.proto BeginTransitionResponse.record)。
		return s.records[playerID], errcode.New(errcode.ErrOwnerEpochConflict, "injected epoch conflict")
	}
	if s.beginErr != nil && (s.beginErrForPlayer == 0 || s.beginErrForPlayer == playerID) {
		return data.OwnerRecordView{}, s.beginErr
	}
	s.begins = append(s.begins, playerID)
	// 模拟 owner 侧推进:记录改为指向目标、PENDING、epoch+1。
	rec := s.records[playerID]
	next := data.OwnerRecordView{
		OwnerEpoch: rec.OwnerEpoch + 1, OwnerType: ownerTypeHub, Phase: ownerPhasePending,
		PodName: target.PodName, InstanceUID: target.InstanceUID, InstanceEpoch: target.InstanceEpoch,
		AssignmentOrAllocationID: target.AssignmentOrAllocationID, ReleaseTrack: target.ReleaseTrack,
	}
	s.records[playerID] = next
	// 成功时回传**新记录**:调用方据此拿 owner_epoch + operation_id。
	return next, nil
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

func pendingRecord(pod, uid string, epoch uint64) data.OwnerRecordView {
	return data.OwnerRecordView{
		OwnerEpoch: epoch, OwnerType: ownerTypeHub, Phase: ownerPhasePending,
		PodName: pod, InstanceUID: uid, InstanceEpoch: 1,
		AssignmentOrAllocationID: "a1", ReleaseTrack: "stable", OperationID: "op1",
	}
}

// 复审 P1-2:玩家离场再回流(owner epoch 推进、新 PENDING)后,census 缓存必须
// 被剪枝并重新 Admit,不得被上一纪元的 admitted 缓存误吞。
func TestOwnerAdmitCensus_CachePrunedOnDepartureThenReadmits(t *testing.T) {
	const pod, uid = "hub-1", "uid-1"
	auth := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{
		1001: pendingRecord(pod, uid, 3),
	}}
	var admitted sync.Map
	ctx := context.Background()

	// 第一轮:PENDING → Admit,进缓存。
	ownerAdmitCensusWeak(ctx, auth, &admitted, []uint64{1001}, ownerTypeHub, pod, uid, time.Second, nil)
	if len(auth.admits) != 1 {
		t.Fatalf("first census must admit once, got %d", len(auth.admits))
	}
	// 第二轮:仍在场,缓存命中,零新调用。
	ownerAdmitCensusWeak(ctx, auth, &admitted, []uint64{1001}, ownerTypeHub, pod, uid, time.Second, nil)
	if len(auth.admits) != 1 {
		t.Fatalf("cached player must not re-admit, got %d", len(auth.admits))
	}
	// 玩家离场:census 不含 1001 → 缓存剪枝(用另一在场玩家触发本轮)。
	auth.records[2002] = pendingRecord(pod, uid, 1)
	ownerAdmitCensusWeak(ctx, auth, &admitted, []uint64{2002}, ownerTypeHub, pod, uid, time.Second, nil)
	// 回流:owner epoch 已推进、新 PENDING → 必须重新 Admit。
	auth.records[1001] = pendingRecord(pod, uid, 9)
	ownerAdmitCensusWeak(ctx, auth, &admitted, []uint64{1001, 2002}, ownerTypeHub, pod, uid, time.Second, nil)
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

// 复审 P1-4:最后一名玩家离场使 census 为空时,缓存仍必须被剪枝——否则该玩家回流
// 同实例(epoch 已推进、新 PENDING)会被上一纪元 admitted 缓存误吞、跳过 Admit。
// 本用例直接传空 census(旧实现早退不剪枝时,回流玩家不会被重新 Admit,断言失败)。
func TestOwnerAdmitCensus_EmptyCensusStillPrunesThenReadmits(t *testing.T) {
	const pod, uid = "hub-1", "uid-1"
	auth := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{
		1001: pendingRecord(pod, uid, 3),
	}}
	var admitted sync.Map
	ctx := context.Background()

	// 第一轮:PENDING → Admit,进缓存。
	ownerAdmitCensusWeak(ctx, auth, &admitted, []uint64{1001}, ownerTypeHub, pod, uid, time.Second, nil)
	if len(auth.admits) != 1 {
		t.Fatalf("first census must admit once, got %d", len(auth.admits))
	}
	// 最后一名玩家离场:census 为空,仍须剪枝该玩家缓存项。
	ownerAdmitCensusWeak(ctx, auth, &admitted, nil, ownerTypeHub, pod, uid, time.Second, nil)
	if _, ok := admitted.Load(uid + "|1001"); ok {
		t.Fatal("empty census must still prune departed player's admitted cache entry")
	}
	// 回流同实例:epoch 已推进、新 PENDING → 必须重新 Admit(未被 stale 缓存误吞)。
	auth.records[1001] = pendingRecord(pod, uid, 9)
	ownerAdmitCensusWeak(ctx, auth, &admitted, []uint64{1001}, ownerTypeHub, pod, uid, time.Second, nil)
	admitsFor1001 := 0
	for _, id := range auth.admits {
		if id == 1001 {
			admitsFor1001++
		}
	}
	if admitsFor1001 != 2 {
		t.Fatalf("returning player after empty census must be re-admitted, got %d admits", admitsFor1001)
	}
}

// 复审 P1-5:census 准入缓存按 last-touch TTL 清死实例项。已销毁实例(UID 不再心跳续期)
// 的项老化超 cutoff 被清,活实例(刚续期)的项保留,防 ownerAdmitted 随历史 UID 无界增长。
func TestSweepStaleOwnerAdmitted(t *testing.T) {
	const pod, uid = "hub-1", "uid-1"
	auth := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{
		1001: pendingRecord(pod, uid, 3),
	}}
	var admitted sync.Map
	// 一轮 census:1001 进缓存(值为 last-touch time.Time)。
	ownerAdmitCensusWeak(context.Background(), auth, &admitted, []uint64{1001}, ownerTypeHub, pod, uid, time.Second, nil)
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
	// 非 time.Time 值也应被 fail-safe 清除。
	admitted.Store("uid-BAD|3003", struct{}{})
	sweepStaleOwnerAdmitted(&admitted, time.Now().Add(-ownerAdmittedStaleTTL))
	if _, ok := admitted.Load("uid-BAD|3003"); ok {
		t.Fatal("非 time.Time 值应被 fail-safe 清除")
	}
}

// contract 阶段:签票点 Begin 是**强依赖**——写不进 owner 权威即整体失败,调用方据此拒签票。
// (同实例收敛与 operation 铸造已下沉权威,本地不再判定;对应覆盖见 owner 服务的
// SameInstanceRedeliveryKeepsOperationAndEpoch / InstanceEpochBumpIsRealMigration。)
func TestOwnerBeginPlayer_StrongFailClosed(t *testing.T) {
	target := data.OwnerTargetView{PodName: "hub-1", InstanceUID: "uid-1", InstanceEpoch: 1,
		AssignmentOrAllocationID: "a1", ReleaseTrack: "stable"}
	ctx := context.Background()

	// Begin 失败 → 失败(不再"告警放行")。
	failing := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{},
		beginErr: errcode.New(errcode.ErrUnavailable, "owner down")}
	if err := ownerBeginPlayer(ctx, failing, 1001, ownerTypeHub, target, time.Second); err == nil {
		t.Fatal("Begin 失败必须上抛,不得放行")
	}

	// Query 失败(结果不可判定)同样 fail-closed:绝不当"无归属"继续(§9.22 禁冒充 OFFLINE)。
	qfail := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{},
		queryErr: errcode.New(errcode.ErrUnavailable, "owner unreachable")}
	if err := ownerBeginPlayer(ctx, qfail, 1001, ownerTypeHub, target, time.Second); err == nil {
		t.Fatal("Query 不可判定必须 fail-closed")
	}

	// owner 未部署(auth==nil)不阻断:属部署形态,不在本函数收敛。
	if err := ownerBeginPlayer(ctx, nil, 1001, ownerTypeHub, target, time.Second); err != nil {
		t.Fatalf("auth==nil 不应报错: %v", err)
	}
}

// operation_id 必须传空,由权威铸造(§9.23 稳定 operation 的落点)。
// 调用方自铸 uuid 恰恰破坏稳定性:每次投递一个新 operation,幂等键形同虚设。
func TestOwnerBeginPlayer_LeavesOperationToAuthority(t *testing.T) {
	auth := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{}}
	target := data.OwnerTargetView{PodName: "hub-1", InstanceUID: "uid-1", InstanceEpoch: 1}
	if err := ownerBeginPlayer(context.Background(), auth, 1001, ownerTypeHub, target, time.Second); err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if len(auth.beginOps) != 1 || auth.beginOps[0] != "" {
		t.Fatalf("operation 必须留空交权威铸造,实得 %q", auth.beginOps)
	}
}

// EPOCH_CONFLICT = Query 与 Begin 之间被另一写者推进(CAS 设计内竞争):
// 重查一次再试;仍冲突则 fail-closed 交调用方整体重试,不盲目循环抢。
func TestOwnerBeginPlayer_RetriesOnceOnEpochConflict(t *testing.T) {
	target := data.OwnerTargetView{PodName: "hub-1", InstanceUID: "uid-1", InstanceEpoch: 1}
	ctx := context.Background()

	// 第一次冲突、第二次成功 → 整体成功,恰好两轮。
	once := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{}, conflictTimes: 1}
	if err := ownerBeginPlayer(ctx, once, 1001, ownerTypeHub, target, time.Second); err != nil {
		t.Fatalf("一次冲突后应重试成功: %v", err)
	}
	if once.queries != 2 {
		t.Fatalf("冲突重试必须重查权威取新 expect_epoch,实得 %d 次 Query", once.queries)
	}

	// 持续冲突 → 上抛,不无限重试。
	always := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{}, conflictTimes: 99}
	if err := ownerBeginPlayer(ctx, always, 1001, ownerTypeHub, target, time.Second); err == nil {
		t.Fatal("持续冲突必须上抛")
	}
	if always.queries > 2 {
		t.Fatalf("重试上限为 2 轮,实得 %d 次 Query", always.queries)
	}
}

// 复审 P1-3:owner 记录漂移(不指向本实例)但归属镜像仍指向本实例 → census 补弱
// Begin 自愈;下一轮 Admit 收敛。归属指向他处/缺失 → 不干预。
func TestOwnerAdmitCensus_HealsDriftedRecordViaResolver(t *testing.T) {
	const pod, uid = "hub-1", "uid-1"
	auth := &scriptedOwnerAuthority{records: map[uint64]data.OwnerRecordView{
		1001: {OwnerEpoch: 4, OwnerType: ownerTypeHub, Phase: ownerPhaseAdmitted,
			PodName: "hub-OLD", InstanceUID: "uid-OLD"}, // 漂移:签票点 Begin 失败留下的旧指向
		2002: {OwnerEpoch: 2, OwnerType: ownerTypeHub, Phase: ownerPhaseAdmitted,
			PodName: "hub-OTHER", InstanceUID: "uid-OTHER"}, // 真实迁移:归属也指向他处
	}}
	resolver := func(_ context.Context, playerID uint64) (data.OwnerTargetView, bool) {
		if playerID == 1001 {
			return data.OwnerTargetView{PodName: pod, InstanceUID: uid, InstanceEpoch: 1,
				AssignmentOrAllocationID: "a1", ReleaseTrack: "stable"}, true
		}
		return data.OwnerTargetView{}, false
	}
	var admitted sync.Map
	ctx := context.Background()

	ownerAdmitCensusWeak(ctx, auth, &admitted, []uint64{1001, 2002}, ownerTypeHub, pod, uid, time.Second, resolver)
	if len(auth.begins) != 1 || auth.begins[0] != 1001 {
		t.Fatalf("only the drifted player backed by local assignment may be healed, got begins=%v", auth.begins)
	}
	// 自愈后下一轮:记录已 PENDING 指向本实例 → Admit 收敛。
	ownerAdmitCensusWeak(ctx, auth, &admitted, []uint64{1001, 2002}, ownerTypeHub, pod, uid, time.Second, resolver)
	if len(auth.admits) != 1 || auth.admits[0] != 1001 {
		t.Fatalf("healed record must converge to admit, got admits=%v", auth.admits)
	}
	if len(auth.begins) != 1 {
		t.Fatalf("heal begin must not repeat after convergence, got begins=%v", auth.begins)
	}
}
