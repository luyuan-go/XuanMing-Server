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
