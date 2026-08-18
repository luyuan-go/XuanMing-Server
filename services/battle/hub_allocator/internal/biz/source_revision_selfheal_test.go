// source_revision_selfheal_test.go — census 自愈路径的来源版本回填(INC-20260818-003)。
//
// 这一格值得单独钉住,因为漏填是**静默**的:自愈失败走弱依赖分支(owner_authority.go
// healFailed++),不会打挂心跳、不会踢玩家、也不会让任何既有用例变红,线上只表现为
// 「owner 漂移一直修不掉 + 聚合 Warn 一直红」,没有硬报错指向根因。
//
// 契约:签票路径(ownerTargetForHubTicket)与自愈路径(resolveOwnerTargetFromAssignment)
// 必须**同源**地把来源版本取自已发布的 assignment 记录。任一侧漏填,该玩家水位非零后
// 就会被 owner 按 legacy_after_versioned 拒掉。
package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/placement"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"
)

func boundAssignmentWithRevision(playerID, revision uint64) *hubv1.HubAssignmentStorageRecord {
	return &hubv1.HubAssignmentStorageRecord{
		PlayerId:        playerID,
		HubPodName:      "pandora-hub-global-1",
		HubInstanceUid:  "uid-selfheal",
		AuthEpoch:       3,
		AuthGen:         1,
		AuthJti:         "jti-selfheal",
		AssignmentId:    "assign-selfheal",
		AuthWriterEpoch: auth.DSAuthWriterEpochV2,
		SourceRevision:  revision,
	}
}

// TestResolveOwnerTargetFromAssignmentCarriesSourceRevision:自愈 target 必须带上
// assignment 记录里那个号,而不是现铸、也不是留空。
func TestResolveOwnerTargetFromAssignmentCarriesSourceRevision(t *testing.T) {
	const playerID = uint64(90001)
	revision, err := placement.ComposeSourceRevision(7, 42)
	if err != nil {
		t.Fatalf("构造来源版本失败: %v", err)
	}

	repo := newFakeRepo()
	repo.assignments[playerID] = boundAssignmentWithRevision(playerID, revision)
	u := &HubUsecase{repo: repo}

	got, ok := u.resolveOwnerTargetFromAssignment(context.Background(), playerID)
	if !ok {
		t.Fatalf("绑定完整时必须解出 target,却拿到 ok=false")
	}
	if got.SourceRevision != revision {
		t.Fatalf("自愈 target 丢了来源版本:got %d,want %d"+
			"(0 会被 owner 判 legacy_after_versioned 拒掉,自愈通道 100%% 失效)",
			got.SourceRevision, revision)
	}
}

// TestResolveOwnerTargetFromAssignmentMatchesTicketPath:自愈路径与签票路径必须同源。
//
// 分开写两个断言而不是只测前一个:即便将来有人给自愈路径改成「现铸一个号」,前一个用例
// 也可能因为号非零而误绿,只有和签票路径逐字段比对才能钉住「同源」这个真正的契约。
func TestResolveOwnerTargetFromAssignmentMatchesTicketPath(t *testing.T) {
	const playerID = uint64(90002)
	revision, err := placement.ComposeSourceRevision(9, 1)
	if err != nil {
		t.Fatalf("构造来源版本失败: %v", err)
	}
	a := boundAssignmentWithRevision(playerID, revision)

	repo := newFakeRepo()
	repo.assignments[playerID] = a
	u := &HubUsecase{repo: repo}

	healed, ok := u.resolveOwnerTargetFromAssignment(context.Background(), playerID)
	if !ok {
		t.Fatalf("绑定完整时必须解出 target,却拿到 ok=false")
	}
	ticket, tok := u.ownerTargetForHubTicket(context.Background(), a)
	if !tok {
		t.Fatalf("同一份 assignment 在签票路径上必须也解得出 target")
	}
	if healed != ticket {
		t.Fatalf("自愈 target 与签票 target 不同源:\n自愈=%+v\n签票=%+v", healed, ticket)
	}
}

// TestResolveOwnerTargetFromAssignmentKeepsLegacyZero:未滚上本协议的旧记录保持 0。
//
// 反向不变式:自愈路径不许「好心」给 legacy 记录补一个号 —— 那会让一个旧来源看起来更新,
// 正是 INC-20260818-003 要防的事。
func TestResolveOwnerTargetFromAssignmentKeepsLegacyZero(t *testing.T) {
	const playerID = uint64(90003)
	repo := newFakeRepo()
	repo.assignments[playerID] = boundAssignmentWithRevision(playerID, placement.SourceRevisionLegacy)
	u := &HubUsecase{repo: repo}

	got, ok := u.resolveOwnerTargetFromAssignment(context.Background(), playerID)
	if !ok {
		t.Fatalf("绑定完整时必须解出 target,却拿到 ok=false")
	}
	if got.SourceRevision != placement.SourceRevisionLegacy {
		t.Fatalf("自愈路径不该给 legacy 记录现铸号:got %d,want 0", got.SourceRevision)
	}
}

// ── 存量 legacy 回填(INC-20260818-003,rollout 第 5 步的前置条件)──────────────
//
// 返回值契约是这组用例的重点,不只是「rec 有没有被改」。
//
// 调用方靠返回值决定**要不要打 hub_assignment_source_revision_backfilled 成功事件**,
// 而且必须等到 CompareAndSwapAssignment 返回 swapped==true 之后再打。原因:CAS 报错或
// 竞争落败时这次补号根本没落进 Redis,若成功事件已经打出去,Loki 上就出现「已回填」而存储
// 仍是 0。rollout 第 5 步「证明不存在 source_revision=0 的存活 assignment」正是拿这个事件
// 流判空的 —— 污染它 = 提前打开 reject_legacy_source_revision = 存量玩家持续进不了大厅。
//
// 所以「返回非零 ⟺ 确实写进了 rec」必须逐格钉住:返回值多报一次,就是一条伪证。

// TestBackfillSourceRevisionMintsForLegacyRecord:持有写者租约时,0 必须被补成非零。
//
// 不补的话存量 0 永远不会自然收敛(复用/续期都 proto.Clone 原样带走),
// 打开 reject_legacy_source_revision 后这些玩家一律被判 legacy_rejected_globally。
func TestBackfillSourceRevisionMintsForLegacyRecord(t *testing.T) {
	u := &HubUsecase{writerFence: &fakeWriterFence{token: 7, held: true}}
	rec := boundAssignmentWithRevision(90101, placement.SourceRevisionLegacy)

	got := u.backfillSourceRevision(context.Background(), rec)

	if rec.GetSourceRevision() == placement.SourceRevisionLegacy {
		t.Fatal("持租约时 legacy 记录必须被补上号,却仍是 0")
	}
	if term, _ := placement.SplitSourceRevision(rec.GetSourceRevision()); term != 7 {
		t.Fatalf("补出来的号任期不对:got term=%d,want 7", term)
	}
	if got != rec.GetSourceRevision() {
		t.Fatalf("返回值必须等于真正写进 rec 的号:got %d,rec=%d", got, rec.GetSourceRevision())
	}
}

// TestBackfillSourceRevisionLeavesVersionedRecordAlone:已有号的记录一个字节都不许动。
//
// 这是「补水位」与「抬水位」的分界:重写一个已存在的号 = 让同一份来源看起来更新,
// 正是 mintSourceRevision 三条纪律要防的事。
func TestBackfillSourceRevisionLeavesVersionedRecordAlone(t *testing.T) {
	existing, err := placement.ComposeSourceRevision(3, 11)
	if err != nil {
		t.Fatalf("构造来源版本失败: %v", err)
	}
	u := &HubUsecase{writerFence: &fakeWriterFence{token: 99, held: true}}
	rec := boundAssignmentWithRevision(90102, existing)

	got := u.backfillSourceRevision(context.Background(), rec)

	if rec.GetSourceRevision() != existing {
		t.Fatalf("已有号被改写了:got %d,want %d(补水位不是抬水位)", rec.GetSourceRevision(), existing)
	}
	if got != 0 {
		t.Fatalf("什么都没补却返回了 %d —— 调用方会据此打出一条不存在的回填成功事件", got)
	}
}

// TestBackfillSourceRevisionKeepsLegacyWhenLeaseLost:铸不出号时保持 0,**不阻断**复用。
//
// 失败不阻断是刻意的:本改动之前这条路径就是带着 0 走完的,补不上只是回到原行为;
// 若在这里 fail-closed,会把一次本来能成功的复用拒掉 —— 那是新增的可用性回归。
// 真正的 fail-closed 在两处且都不在这里:置换点 mintSourceRevision 直接上抛,
// 以及仓储侧 writer fence(失租的写者根本发布不出去)。
func TestBackfillSourceRevisionKeepsLegacyWhenLeaseLost(t *testing.T) {
	u := &HubUsecase{writerFence: &fakeWriterFence{token: 7, held: false}}
	rec := boundAssignmentWithRevision(90103, placement.SourceRevisionLegacy)

	got := u.backfillSourceRevision(context.Background(), rec)

	if rec.GetSourceRevision() != placement.SourceRevisionLegacy {
		t.Fatalf("未持租约时不该铸出号:got %d,want 0", rec.GetSourceRevision())
	}
	if got != 0 {
		t.Fatalf("铸号失败却返回了 %d —— 会在 Loki 上伪造一条回填成功事件", got)
	}
}

// TestBackfillSourceRevisionNoopWithoutWriterFence:未启用写者租约的部署(dev)保持 0。
func TestBackfillSourceRevisionNoopWithoutWriterFence(t *testing.T) {
	u := &HubUsecase{} // writerFence == nil
	rec := boundAssignmentWithRevision(90104, placement.SourceRevisionLegacy)

	got := u.backfillSourceRevision(context.Background(), rec)

	if rec.GetSourceRevision() != placement.SourceRevisionLegacy {
		t.Fatalf("dev 部署不该铸号:got %d,want 0", rec.GetSourceRevision())
	}
	if got != 0 {
		t.Fatalf("dev 部署什么都没补却返回了 %d", got)
	}
}

// TestBackfillSourceRevisionReturnZeroForNilRecord:nil 记录不得 panic,且返回 0。
func TestBackfillSourceRevisionReturnZeroForNilRecord(t *testing.T) {
	u := &HubUsecase{writerFence: &fakeWriterFence{token: 7, held: true}}
	if got := u.backfillSourceRevision(context.Background(), nil); got != 0 {
		t.Fatalf("nil 记录必须返回 0,got %d", got)
	}
}
