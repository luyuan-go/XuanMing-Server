// orphan_gameserver_test.go — 孤儿 Allocated GameServer 对账清扫(2026-08-03)。
//
// 清扫唯一不可接受的错误方向是**误删被引用/可能载人的 GS**,用例围绕四重防误删展开:
// 证据不可得不删且候选重新起算、候选须跨轮观察满阈值、复核失效作废候选、
// 台账查无(权威视图分裂/存量泄漏)保留不删;引用匹配覆盖 pod 名 / GS UID /
// allocation_id 三条通道;另覆盖单轮删除封顶与节流(节流用真实调用计数断言,
// 前版"空测"已被对抗审查的变异实验证伪)。
package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// fakeOrphanReconciler 脚本化 List/Delete;嵌入 Mock 分配器满足 GameServerAllocator。
type fakeOrphanReconciler struct {
	*MockGameServerAllocator

	listResult []data.AllocatedGameServerInfo
	listErr    error
	listCalls  int

	deleteResult bool
	deleteErr    error
	deleted      []string // "name/uid/allocation_id" 调用记录
}

func (f *fakeOrphanReconciler) ListAllocatedGameServers(
	_ context.Context,
) ([]data.AllocatedGameServerInfo, error) {
	f.listCalls++
	return f.listResult, f.listErr
}

func (f *fakeOrphanReconciler) DeleteAllocatedGameServerExact(
	_ context.Context, name, uid, expectedAllocationID string,
) (bool, error) {
	f.deleted = append(f.deleted, name+"/"+uid+"/"+expectedAllocationID)
	return f.deleteResult, f.deleteErr
}

func newOrphanTestUsecase(t *testing.T) (*AllocatorUsecase, *data.RedisBattleRepo, *fakeOrphanReconciler) {
	t.Helper()
	fake := &fakeOrphanReconciler{
		MockGameServerAllocator: NewMockGameServerAllocator(testCfg()),
		deleteResult:            true,
	}
	uc, repo, _ := newUsecaseWithAlloc(t, fake)
	if uc.orphanGSReconciler == nil {
		t.Fatalf("orphan reconciler not wired via interface assertion")
	}
	if uc.allocationLedger == nil {
		t.Fatalf("allocation ledger not wired via interface assertion")
	}
	return uc, repo, fake
}

func seedReferencedBattle(t *testing.T, repo *data.RedisBattleRepo,
	matchID uint64, pod, gsUID, allocationID string) {
	t.Helper()
	if err := repo.CreateBattle(context.Background(), &dsv1.BattleStorageRecord{
		MatchId: matchID, State: "running", DsPodName: pod,
		GameserverUid: gsUID, AllocationId: allocationID,
		LastHeartbeatMs: time.Now().UnixMilli(),
	}, time.Minute); err != nil {
		t.Fatalf("seed battle: %v", err)
	}
}

// seedLedger 把 allocation_id 记入本权威台账(防误删④的出身证明)。
func seedLedger(t *testing.T, repo *data.RedisBattleRepo, allocationID string) {
	t.Helper()
	if err := repo.RecordAllocationLedger(context.Background(), allocationID, time.Now().UnixMilli()); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
}

func orphanGS(name, uid, allocID string) data.AllocatedGameServerInfo {
	return data.AllocatedGameServerInfo{
		Name: name, UID: uid, Fleet: "battle-fleet", AllocationID: allocID,
	}
}

// 有任何一条权威引用(pod 名 / GS UID / allocation_id)的 Allocated GS 永不成为候选。
func TestOrphanReconcileProtectsReferencedGameServers(t *testing.T) {
	uc, repo, fake := newOrphanTestUsecase(t)
	ctx := context.Background()
	seedReferencedBattle(t, repo, 91001, "gs-by-pod", "", "")
	seedReferencedBattle(t, repo, 91002, "other-pod-a", "uid-ref", "")
	seedReferencedBattle(t, repo, 91003, "other-pod-b", "", "alloc-ref")
	// 即便台账里都有,引用检查也必须先挡下(台账只是删除的必要条件,不是充分条件)。
	for _, id := range []string{"alloc-1", "alloc-2", "alloc-ref"} {
		seedLedger(t, repo, id)
	}
	fake.listResult = []data.AllocatedGameServerInfo{
		orphanGS("gs-by-pod", "uid-1", "alloc-1"),     // pod 名命中
		orphanGS("gs-by-uid", "uid-ref", "alloc-2"),   // GS UID 命中
		orphanGS("gs-by-alloc", "uid-3", "alloc-ref"), // allocation_id 命中
	}

	now := time.Now()
	uc.reconcileOrphanGameServers(ctx, now)
	uc.reconcileOrphanGameServers(ctx, now.Add(time.Hour)) // 远超阈值也不删

	if len(fake.deleted) != 0 {
		t.Fatalf("referenced gameservers must never be deleted, got %v", fake.deleted)
	}
	if len(uc.orphanGSFirstSeen) != 0 {
		t.Fatalf("referenced gameservers must not stay candidates, got %v", uc.orphanGSFirstSeen)
	}
}

// 无引用且台账可证明出身的候选:首见只登记;未满阈值不删;满阈值后按观察到的
// allocation_id 精确删除。
func TestOrphanReconcileReclaimsAfterThreshold(t *testing.T) {
	uc, repo, fake := newOrphanTestUsecase(t)
	ctx := context.Background()
	seedLedger(t, repo, "alloc-leak")
	fake.listResult = []data.AllocatedGameServerInfo{orphanGS("gs-leak", "uid-leak", "alloc-leak")}

	start := time.Now()
	uc.reconcileOrphanGameServers(ctx, start)
	if len(fake.deleted) != 0 {
		t.Fatalf("first observation must not delete, got %v", fake.deleted)
	}
	uc.reconcileOrphanGameServers(ctx, start.Add(uc.orphanGSReclaimAfter()-time.Second))
	if len(fake.deleted) != 0 {
		t.Fatalf("below threshold must not delete, got %v", fake.deleted)
	}
	uc.reconcileOrphanGameServers(ctx, start.Add(uc.orphanGSReclaimAfter()+time.Second))
	if len(fake.deleted) != 1 || fake.deleted[0] != "gs-leak/uid-leak/alloc-leak" {
		t.Fatalf("expected exact reclaim of gs-leak, got %v", fake.deleted)
	}
	if len(uc.orphanGSFirstSeen) != 0 {
		t.Fatalf("reclaimed candidate must leave first-seen table, got %v", uc.orphanGSFirstSeen)
	}
}

// 防误删④(P0「权威视图零绑定」回归):台账查无 = 无法证明该 GS 出身本权威,
// 永不删除——空/错配 Redis 的台账必然为空,整个集群一台都删不掉。
func TestOrphanReconcileLedgerMissNeverDeletes(t *testing.T) {
	uc, _, fake := newOrphanTestUsecase(t)
	ctx := context.Background()
	// 模拟权威视图分裂:refs 为空(本 Redis 无任何记录)、台账也为空,
	// 而集群里全是「看起来无主」的 Allocated GS。
	fake.listResult = []data.AllocatedGameServerInfo{
		orphanGS("gs-live-1", "uid-l1", "alloc-other-authority-1"),
		orphanGS("gs-live-2", "uid-l2", "alloc-other-authority-2"),
	}
	start := time.Now()
	uc.reconcileOrphanGameServers(ctx, start)
	uc.reconcileOrphanGameServers(ctx, start.Add(uc.orphanGSReclaimAfter()+time.Minute))
	uc.reconcileOrphanGameServers(ctx, start.Add(uc.orphanGSReclaimAfter()+time.Hour))
	if len(fake.deleted) != 0 {
		t.Fatalf("ledger-miss candidates must never be deleted, got %v", fake.deleted)
	}
	if len(uc.orphanGSFirstSeen) != 2 {
		t.Fatalf("unprovable candidates must be retained for alerting, got %v", uc.orphanGSFirstSeen)
	}
}

// 防误删④:无 allocation-id label(手工 GSA / 非本系统分配)永不删除。
func TestOrphanReconcileNoLabelNeverDeletes(t *testing.T) {
	uc, _, fake := newOrphanTestUsecase(t)
	ctx := context.Background()
	fake.listResult = []data.AllocatedGameServerInfo{orphanGS("gs-manual", "uid-m", "")}
	start := time.Now()
	uc.reconcileOrphanGameServers(ctx, start)
	uc.reconcileOrphanGameServers(ctx, start.Add(uc.orphanGSReclaimAfter()+time.Hour))
	if len(fake.deleted) != 0 {
		t.Fatalf("label-less candidates must never be deleted, got %v", fake.deleted)
	}
}

// 防误删①:GS 清单读失败 → 整轮什么都不做。
func TestOrphanReconcileListFailureDeletesNothing(t *testing.T) {
	uc, repo, fake := newOrphanTestUsecase(t)
	ctx := context.Background()
	seedLedger(t, repo, "alloc-leak")
	fake.listResult = []data.AllocatedGameServerInfo{orphanGS("gs-leak", "uid-leak", "alloc-leak")}
	start := time.Now()
	uc.reconcileOrphanGameServers(ctx, start) // 建立候选

	fake.listErr = errors.New("apiserver down")
	uc.reconcileOrphanGameServers(ctx, start.Add(time.Hour))
	if len(fake.deleted) != 0 {
		t.Fatalf("list failure must delete nothing, got %v", fake.deleted)
	}
}

// 防误删①:权威记录读失败 → 不删、已有候选保留、且观察起点被重置为当前时刻
// (证据中断的墙钟时间不得计入阈值)。
func TestOrphanReconcileAuthorityFailureResetsCandidateClock(t *testing.T) {
	fake := &fakeOrphanReconciler{
		MockGameServerAllocator: NewMockGameServerAllocator(testCfg()),
		deleteResult:            true,
		listResult:              []data.AllocatedGameServerInfo{orphanGS("gs-leak", "uid-leak", "alloc-leak")},
	}
	uc, _, mr := newUsecaseWithAlloc(t, fake)
	ctx := context.Background()

	start := time.Now()
	uc.reconcileOrphanGameServers(ctx, start) // 建立候选
	if len(uc.orphanGSFirstSeen) != 1 {
		t.Fatalf("expected 1 candidate, got %v", uc.orphanGSFirstSeen)
	}

	mr.Close() // 权威存储不可用
	outage := start.Add(time.Hour)
	uc.reconcileOrphanGameServers(ctx, outage)
	if len(fake.deleted) != 0 {
		t.Fatalf("authority failure must delete nothing, got %v", fake.deleted)
	}
	if len(uc.orphanGSFirstSeen) != 1 {
		t.Fatalf("existing candidate must survive authority outage, got %v", uc.orphanGSFirstSeen)
	}
	for key, first := range uc.orphanGSFirstSeen {
		if !first.Equal(outage) {
			t.Fatalf("candidate %s clock must reset to outage round time, got %v", key, first)
		}
	}
}

// 复核失效(DeleteAllocatedGameServerExact 返回 false):候选作废,重新观察,不立即重删。
func TestOrphanReconcileRecheckMissResetsCandidate(t *testing.T) {
	uc, repo, fake := newOrphanTestUsecase(t)
	ctx := context.Background()
	seedLedger(t, repo, "alloc-a")
	fake.listResult = []data.AllocatedGameServerInfo{orphanGS("gs-flip", "uid-flip", "alloc-a")}
	fake.deleteResult = false // 服务端复核失效

	start := time.Now()
	uc.reconcileOrphanGameServers(ctx, start)
	uc.reconcileOrphanGameServers(ctx, start.Add(uc.orphanGSReclaimAfter()+time.Second))
	if len(fake.deleted) != 1 {
		t.Fatalf("expected exactly one delete attempt, got %v", fake.deleted)
	}
	if len(uc.orphanGSFirstSeen) != 0 {
		t.Fatalf("recheck miss must reset candidate, got %v", uc.orphanGSFirstSeen)
	}

	// 下一轮重新首见 → 观察期重新起算,不会立即再删。
	uc.reconcileOrphanGameServers(ctx, start.Add(uc.orphanGSReclaimAfter()+2*time.Second))
	if len(fake.deleted) != 1 {
		t.Fatalf("reset candidate must restart observation, got %v", fake.deleted)
	}
}

// 删除失败:候选保留(首见时间不变),下一轮继续重试。
func TestOrphanReconcileDeleteFailureKeepsCandidate(t *testing.T) {
	uc, repo, fake := newOrphanTestUsecase(t)
	ctx := context.Background()
	seedLedger(t, repo, "alloc-leak")
	fake.listResult = []data.AllocatedGameServerInfo{orphanGS("gs-leak", "uid-leak", "alloc-leak")}
	fake.deleteErr = errors.New("apiserver timeout")

	start := time.Now()
	uc.reconcileOrphanGameServers(ctx, start)
	uc.reconcileOrphanGameServers(ctx, start.Add(uc.orphanGSReclaimAfter()+time.Second))
	if len(fake.deleted) != 1 {
		t.Fatalf("expected one delete attempt, got %v", fake.deleted)
	}
	if len(uc.orphanGSFirstSeen) != 1 {
		t.Fatalf("delete failure must keep candidate for retry, got %v", uc.orphanGSFirstSeen)
	}
	fake.deleteErr = nil
	uc.reconcileOrphanGameServers(ctx, start.Add(uc.orphanGSReclaimAfter()+2*time.Second))
	if len(fake.deleted) != 2 {
		t.Fatalf("expected retry delete, got %v", fake.deleted)
	}
}

// 单轮删除封顶(对抗审查 P2「饿死判弃链」整改):满阈值候选再多,单轮最多
// orphanGSMaxReclaimPerRound 次删除尝试;剩余候选保留,下一轮继续。
func TestOrphanReconcileCapsReclaimsPerRound(t *testing.T) {
	uc, repo, fake := newOrphanTestUsecase(t)
	ctx := context.Background()
	const total = 5
	list := make([]data.AllocatedGameServerInfo, 0, total)
	for i := 0; i < total; i++ {
		id := string(rune('a' + i))
		seedLedger(t, repo, "alloc-"+id)
		list = append(list, orphanGS("gs-"+id, "uid-"+id, "alloc-"+id))
	}
	fake.listResult = list

	start := time.Now()
	uc.reconcileOrphanGameServers(ctx, start)
	uc.reconcileOrphanGameServers(ctx, start.Add(uc.orphanGSReclaimAfter()+time.Second))
	if len(fake.deleted) != orphanGSMaxReclaimPerRound {
		t.Fatalf("round must cap delete attempts at %d, got %d (%v)",
			orphanGSMaxReclaimPerRound, len(fake.deleted), fake.deleted)
	}
	uc.reconcileOrphanGameServers(ctx, start.Add(uc.orphanGSReclaimAfter()+2*time.Second))
	if len(fake.deleted) != total {
		t.Fatalf("remaining candidates must be reclaimed next round, got %d (%v)",
			len(fake.deleted), fake.deleted)
	}
}

// 删除宽限中的 GS 跳过;从清单消失的候选被修剪(首见表容量有界)。
func TestOrphanReconcileSkipsDeletingAndPrunesVanished(t *testing.T) {
	uc, _, fake := newOrphanTestUsecase(t)
	ctx := context.Background()
	deleting := orphanGS("gs-grace", "uid-grace", "")
	deleting.Deleting = true
	fake.listResult = []data.AllocatedGameServerInfo{
		deleting,
		orphanGS("gs-vanish", "uid-vanish", ""),
	}
	start := time.Now()
	uc.reconcileOrphanGameServers(ctx, start)
	if len(uc.orphanGSFirstSeen) != 1 {
		t.Fatalf("deleting GS must not be candidate, got %v", uc.orphanGSFirstSeen)
	}

	fake.listResult = nil // 全部消失
	uc.reconcileOrphanGameServers(ctx, start.Add(orphanGSReconcileInterval))
	if len(uc.orphanGSFirstSeen) != 0 {
		t.Fatalf("vanished candidates must be pruned, got %v", uc.orphanGSFirstSeen)
	}
	if len(fake.deleted) != 0 {
		t.Fatalf("nothing should be deleted, got %v", fake.deleted)
	}
}

// IfDue 节流:按真实 List 调用数断言(对抗审查已用变异实验证伪前版"看时间戳"的空测:
// 删掉节流守卫时间戳断言照样过)。3 次 IfDue(t0 / t0+1s / t0+interval+1s)只准产生
// 2 次 List;非 Agones 分配器(未实现接口)完全禁用。
func TestOrphanReconcileThrottleAndDisable(t *testing.T) {
	uc, _, fake := newOrphanTestUsecase(t)
	ctx := context.Background()

	start := time.Now()
	uc.reconcileOrphanGameServersIfDue(ctx, start)
	uc.reconcileOrphanGameServersIfDue(ctx, start.Add(time.Second)) // 间隔内,不得执行
	uc.reconcileOrphanGameServersIfDue(ctx, start.Add(orphanGSReconcileInterval+time.Second))
	if fake.listCalls != 2 {
		t.Fatalf("throttle must allow exactly 2 rounds (first + after interval), got %d list calls", fake.listCalls)
	}

	// mock 分配器不实现接口 → 构造期不注入,清扫禁用
	plain, _ := newUsecase(t)
	if plain.orphanGSReconciler != nil {
		t.Fatalf("mock allocator must not enable orphan reconcile")
	}
	plain.reconcileOrphanGameServersIfDue(ctx, start) // 不 panic 即可
}

// 阈值钳制:0/负值走默认,低于下限被钳到下限。
func TestOrphanReclaimAfterClamp(t *testing.T) {
	uc, _ := newUsecase(t)
	if got := uc.orphanGSReclaimAfter(); got != orphanGSReclaimAfterDefault {
		t.Fatalf("zero config must use default, got %v", got)
	}
	uc.cfg.OrphanGsReclaimAfter = config.Duration(30 * time.Second)
	if got := uc.orphanGSReclaimAfter(); got != orphanGSReclaimAfterFloor {
		t.Fatalf("below-floor config must clamp to floor, got %v", got)
	}
	uc.cfg.OrphanGsReclaimAfter = config.Duration(30 * time.Minute)
	if got := uc.orphanGSReclaimAfter(); got != 30*time.Minute {
		t.Fatalf("valid config must pass through, got %v", got)
	}
}

// 分配路径把 allocation_id 写入台账(防误删④的另一半:出身证明的生产端)。
func TestAllocateBattleRecordsAllocationLedger(t *testing.T) {
	uc, repo := newUsecase(t)
	res := allocateReady(t, uc, repo, 93001, []uint64{7}, 8, "pve_coop")
	if res == nil {
		t.Fatalf("allocate returned nil result")
	}
	b, found, err := repo.GetBattle(context.Background(), 93001)
	if err != nil || !found || b.GetAllocationId() == "" {
		t.Fatalf("battle record missing allocation id: found=%v err=%v", found, err)
	}
	known, err := repo.AllocationLedgerContains(context.Background(), b.GetAllocationId())
	if err != nil || !known {
		t.Fatalf("allocation id must be recorded in ledger: known=%v err=%v", known, err)
	}
}
