// owner_query_test.go — §9.23 query-first placement 叠加纯函数单测(migrate ①)。
package biz

import (
	"context"
	"testing"
	"time"

	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"

	"github.com/luyuancpp/pandora/pkg/errcode"
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

	// ── R11 复审 架构 P0:契约由 overlay 改为 query-first,以下是新契约 ──────────
	//
	// 被删掉的两条旧断言(「路由不一致 → 原样返回」「owner NONE → 原样返回」)正是
	// 让它只能算 overlay 的东西:前者让 owner 永远无法纠正错误路由,后者把"无归属"
	// 和"不叠加"混成一件事。现在这两个判定都上移到 resolveResumeFromOwner:
	// NONE → decided=false 交首次进场链;路由不一致 → owner 直接赢。

	t.Run("owner 权威决定路由:owner=BATTLE 时不受既有 HUB 解析影响", func(t *testing.T) {
		v := data.OwnerPlacementView{
			OwnerType: ownerTypeBattle, Phase: ownerPhaseAdmitted, OwnerEpoch: 8,
			PodName: "battle-1", AssignmentOrAllocationID: "alloc-1", LeaseDeadlineMs: now + 5000,
		}
		// query-first 下 base 由 owner 类型构造,不再继承 locator 的路由。
		got := applyOwnerPlacement(
			ResumeContextResult{Route: loginv1.ResumeRoute_RESUME_ROUTE_BATTLE}, v, now)
		if got.Route != loginv1.ResumeRoute_RESUME_ROUTE_BATTLE {
			t.Fatalf("owner 应决定路由,得 %v", got.Route)
		}
		if got.OwnerEpoch != 8 {
			t.Fatalf("owner_epoch 必须出参(客户端幂等 no-op 三元组之一),得 %d", got.OwnerEpoch)
		}
		if got.EntryState != loginv1.ResumeEntryState_RESUME_ENTRY_STATE_TARGET {
			t.Fatalf("有归属且屏障已开应为 TARGET,得 %v", got.EntryState)
		}
	})

	t.Run("admit_not_before 屏障未开 → WAIT + ADMIT_BARRIER + 有界 retry_after", func(t *testing.T) {
		v := data.OwnerPlacementView{
			OwnerType: ownerTypeHub, Phase: ownerPhaseAdmitted, OwnerEpoch: 3,
			PodName: "hub-1", AdmitNotBeforeMs: now + 1500, LeaseDeadlineMs: now + 30000,
		}
		got := applyOwnerPlacement(hubBase, v, now)
		if got.EntryState != loginv1.ResumeEntryState_RESUME_ENTRY_STATE_WAIT {
			t.Fatalf("屏障未开必须 WAIT(此刻放行就是双 DS),得 %v", got.EntryState)
		}
		if got.WaitReason != loginv1.ResumeWaitReason_RESUME_WAIT_REASON_ADMIT_BARRIER {
			t.Fatalf("WAIT 必须带原因,得 %v", got.WaitReason)
		}
		if got.RetryAfterMs != 1500 {
			t.Fatalf("retry_after 应由屏障推导,得 %d", got.RetryAfterMs)
		}
		if got.PlacementState == loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_STABLE {
			t.Fatal("屏障未开绝不能报 STABLE")
		}
		// exact target 与 epoch 仍要带回:客户端要续用同一 operation。
		if got.DSPodName != "hub-1" || got.OwnerEpoch != 3 {
			t.Fatalf("WAIT 也必须保留 exact target 与 owner_epoch:%+v", got)
		}
	})

	t.Run("屏障过远 → retry_after 被钳到上界(等待必须有界)", func(t *testing.T) {
		v := data.OwnerPlacementView{
			OwnerType: ownerTypeHub, Phase: ownerPhaseAdmitted, PodName: "hub-1",
			AdmitNotBeforeMs: now + 600_000, LeaseDeadlineMs: now + 700_000,
		}
		got := applyOwnerPlacement(hubBase, v, now)
		if int64(got.RetryAfterMs) != ownerRetryAfterCeilingMs {
			t.Fatalf("retry_after 应钳到 %d,得 %d", ownerRetryAfterCeilingMs, got.RetryAfterMs)
		}
	})

	t.Run("租约剩余不足安全余量 → 只报 PENDING(不贴边宣称 STABLE)", func(t *testing.T) {
		v := data.OwnerPlacementView{
			OwnerType: ownerTypeHub, Phase: ownerPhaseAdmitted, PodName: "hub-1",
			LeaseDeadlineMs: now + ownerLeaseSkewMarginMs - 1,
		}
		got := applyOwnerPlacement(hubBase, v, now)
		if got.PlacementState != loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_PENDING {
			t.Fatalf("租约贴边不得报 STABLE(§9.22 旧 DS 停止 < 新 DS 开始),得 %v", got.PlacementState)
		}
	})
}

// fakeOwnerPlacementQuerier 是 §9.23 query-first 的可编程 owner 权威(此前全仓没有,
// 于是 owner 不可达 / owner 与 locator 分歧这两条最关键的分支从来没被执行过)。
type fakeOwnerPlacementQuerier struct {
	view  data.OwnerPlacementView
	err   error
	calls int
}

func (f *fakeOwnerPlacementQuerier) QueryOwnerPlacement(context.Context, uint64) (data.OwnerPlacementView, error) {
	f.calls++
	if f.err != nil {
		return data.OwnerPlacementView{}, f.err
	}
	return f.view, nil
}

// owner 不可达时**绝不能**回落到 locator 路由:presence key miss 不能证明玩家已离开
// 旧 DS,更不能授权进入另一台 DS(§9.22)。必须 WAIT/UNKNOWN + retry_after。
func TestResolveResumeFromOwner_UnavailableReturnsWaitNeverFallsBack(t *testing.T) {
	uc := &LoginUsecase{}
	q := &fakeOwnerPlacementQuerier{err: errcode.New(errcode.ErrUnavailable, "owner unreachable")}
	uc.SetOwnerPlacementQuerier(q)

	decided, out := uc.resolveResumeFromOwner(context.Background(), 1001)
	if !decided {
		t.Fatal("owner 不可判定必须由 owner 分支收口,不得交回旧链")
	}
	if out.EntryState != loginv1.ResumeEntryState_RESUME_ENTRY_STATE_WAIT {
		t.Fatalf("必须 WAIT,得 %v", out.EntryState)
	}
	if out.WaitReason != loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN {
		t.Fatalf("WAIT 原因应为 OWNER_UNKNOWN,得 %v", out.WaitReason)
	}
	if out.Route != loginv1.ResumeRoute_RESUME_ROUTE_UNKNOWN {
		t.Fatalf("路由必须明示 UNKNOWN,不得冒充 HUB,得 %v", out.Route)
	}
	if out.RetryAfterMs == 0 {
		t.Fatal("WAIT 必须带 retry_after,否则是无出口等待")
	}
	if q.calls != 1 {
		t.Fatalf("owner 应被查询恰好一次,得 %d", q.calls)
	}
}

// owner 明确"无归属" = 首次进场,交旧链继续(这不是回落,是 owner 给了确定答案)。
func TestResolveResumeFromOwner_NoRecordDelegatesToFirstEntryChain(t *testing.T) {
	uc := &LoginUsecase{}
	uc.SetOwnerPlacementQuerier(&fakeOwnerPlacementQuerier{
		view: data.OwnerPlacementView{OwnerType: ownerTypeNone},
	})
	decided, out := uc.resolveResumeFromOwner(context.Background(), 1001)
	if decided {
		t.Fatalf("无归属应交首次进场链处理,得 decided=true out=%+v", out)
	}
}

// owner 与 locator 分歧时 owner 赢——这正是旧 overlay 做不到的事。
func TestResolveResumeFromOwner_OwnerDecidesRoute(t *testing.T) {
	uc := &LoginUsecase{}
	uc.SetOwnerPlacementQuerier(&fakeOwnerPlacementQuerier{
		view: data.OwnerPlacementView{
			OwnerType: ownerTypeBattle, Phase: ownerPhaseAdmitted, OwnerEpoch: 12,
			PodName: "battle-9", AssignmentOrAllocationID: "alloc-9",
			LeaseDeadlineMs: time.Now().UnixMilli() + 60_000,
		},
	})
	decided, out := uc.resolveResumeFromOwner(context.Background(), 1001)
	if !decided || out.Route != loginv1.ResumeRoute_RESUME_ROUTE_BATTLE {
		t.Fatalf("owner=BATTLE 应决定路由,得 decided=%v route=%v", decided, out.Route)
	}
	if out.OwnerEpoch != 12 || out.AllocationID != "alloc-9" {
		t.Fatalf("必须带回 owner_epoch 与 exact target:%+v", out)
	}
	if out.EntryState != loginv1.ResumeEntryState_RESUME_ENTRY_STATE_TARGET ||
		out.PlacementState != loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_STABLE {
		t.Fatalf("ADMITTED + 租约充裕应为 TARGET/STABLE:%+v", out)
	}
}
