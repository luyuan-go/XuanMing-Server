// owner_authority_test.go — census 代提交 Admit 缓存有界性回归测试(压测前审核 P1)。
//
// 覆盖 ownerAdmitCensusWeak 的 last-touch time.Time 值 + 本实例 census 剪枝,以及
// sweepStaleOwnerAdmitted 对死实例(UID 不再心跳续期)项的 TTL 清扫——防 ownerAdmitted
// sync.Map 随累计对局的历史 InstanceUID 无界增长导致 ds_allocator OOM。
package biz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// scriptedOwnerAuthority:QueryOwner 按注入记录应答;记录 Admit / Release 调用。
type scriptedOwnerAuthority struct {
	mu       sync.Mutex
	records  map[uint64]data.OwnerRecordView
	admits   []uint64
	releases []uint64
	// releaseErr 非 nil 时 ReleaseOwner 直接失败,用于验证弱依赖只告警不中断。
	releaseErr error
}

func (s *scriptedOwnerAuthority) QueryOwner(_ context.Context, playerID uint64) (data.OwnerRecordView, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.records[playerID], nil
}

func (s *scriptedOwnerAuthority) BeginTransition(_ context.Context, _, _ uint64, _ string, _ int8, _ data.OwnerTargetView) error {
	return nil
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
