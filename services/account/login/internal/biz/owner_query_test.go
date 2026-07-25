// owner_query_test.go — §9.23 query-first placement 叠加纯函数单测(migrate ①)。
package biz

import (
	"testing"

	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"

	"github.com/luyuancpp/pandora/services/account/login/internal/data"
)

func TestApplyOwnerPlacement(t *testing.T) {
	const now int64 = 1_000_000
	hubBase := ResumeContextResult{Route: loginv1.ResumeRoute_RESUME_ROUTE_HUB}
	battleBase := ResumeContextResult{Route: loginv1.ResumeRoute_RESUME_ROUTE_BATTLE, MatchID: 42}

	t.Run("HUB ADMITTED 租约有效 → 叠加 STABLE + 实例身份", func(t *testing.T) {
		v := data.OwnerPlacementView{
			OwnerType: ownerTypeHub, Phase: ownerPhaseAdmitted,
			PodName: "hub-1", InstanceUID: "uid-1", InstanceEpoch: 3,
			AssignmentOrAllocationID: "asg-9", ReleaseTrack: "stable",
			OperationID: "op-1", LeaseDeadlineMs: now + 5000,
		}
		got := applyOwnerPlacement(hubBase, v, now)
		if got.Route != loginv1.ResumeRoute_RESUME_ROUTE_HUB {
			t.Fatalf("Route 不应被改动,得 %v", got.Route)
		}
		if got.PlacementState != loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_STABLE {
			t.Fatalf("ADMITTED+租约有效应 STABLE,得 %v", got.PlacementState)
		}
		if got.DSPodName != "hub-1" || got.DSInstanceUID != "uid-1" || got.DSInstanceEpoch != 3 ||
			got.OperationID != "op-1" || got.ReleaseTrack != "stable" || got.HubAssignmentID != "asg-9" {
			t.Fatalf("HUB 实例身份未正确叠加:%+v", got)
		}
		if got.AllocationID != "" {
			t.Fatalf("HUB 路由不应填 AllocationID,得 %q", got.AllocationID)
		}
	})

	t.Run("HUB PENDING → PENDING", func(t *testing.T) {
		v := data.OwnerPlacementView{OwnerType: ownerTypeHub, Phase: 1, PodName: "hub-1", LeaseDeadlineMs: now + 5000}
		if got := applyOwnerPlacement(hubBase, v, now); got.PlacementState != loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_PENDING {
			t.Fatalf("PENDING 阶段应 PENDING,得 %v", got.PlacementState)
		}
	})

	t.Run("ADMITTED 但租约过期 → PENDING(未确证不当 STABLE)", func(t *testing.T) {
		v := data.OwnerPlacementView{OwnerType: ownerTypeHub, Phase: ownerPhaseAdmitted, PodName: "hub-1", LeaseDeadlineMs: now - 1}
		if got := applyOwnerPlacement(hubBase, v, now); got.PlacementState != loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_PENDING {
			t.Fatalf("租约过期不应 STABLE,得 %v", got.PlacementState)
		}
	})

	t.Run("BATTLE owner 填 AllocationID 不填 HubAssignmentID", func(t *testing.T) {
		v := data.OwnerPlacementView{OwnerType: ownerTypeBattle, Phase: ownerPhaseAdmitted,
			PodName: "battle-1", AssignmentOrAllocationID: "alloc-7", LeaseDeadlineMs: now + 5000}
		got := applyOwnerPlacement(battleBase, v, now)
		if got.AllocationID != "alloc-7" || got.HubAssignmentID != "" {
			t.Fatalf("BATTLE 应填 AllocationID:%+v", got)
		}
		if got.MatchID != 42 {
			t.Fatalf("既有 MatchID 不应被叠加抹掉,得 %d", got.MatchID)
		}
	})

	t.Run("owner 路由与既有解析不一致 → 原样返回(不改路由,不叠加)", func(t *testing.T) {
		v := data.OwnerPlacementView{OwnerType: ownerTypeBattle, Phase: ownerPhaseAdmitted, PodName: "battle-1", LeaseDeadlineMs: now + 5000}
		got := applyOwnerPlacement(hubBase, v, now) // owner=BATTLE 但既有=HUB
		if got != hubBase {
			t.Fatalf("路由不一致应原样返回 out,得 %+v", got)
		}
	})

	t.Run("owner NONE → 原样返回", func(t *testing.T) {
		v := data.OwnerPlacementView{OwnerType: ownerTypeNone}
		if got := applyOwnerPlacement(hubBase, v, now); got != hubBase {
			t.Fatalf("owner none 应原样返回,得 %+v", got)
		}
	})
}
