// owner_query.go — §9.23 query-first placement 叠加(owner-authority.md migrate ①,2026-07-24)。
//
// login 恢复上下文在既有 presence/match 路由解析之上,叠加 owner 权威的 exact placement
// (pod/uid/epoch/operation/阶段),供客户端做 §9.23 的 TARGET/ADMITTED 幂等 no-op。
// **安全边界(migrate 阶段,默认关)**:只在 owner 路由与既有解析一致时叠加,**绝不改变
// Route 决策**——owner 记录 stale/不一致/查询失败时保持既有 out(新旧双门并行,不会误路由)。
// owner 覆盖式权威(owner 直接决定路由)+ §9.23 全量(owner_epoch/retry_after/统一状态枚举,
// 需 proto 扩展)属 contract 阶段,须真实 owner 服务 + 脑裂分区故障注入后再切。
package biz

import (
	"context"
	"time"

	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"

	plog "github.com/luyuancpp/pandora/pkg/log"

	"github.com/luyuancpp/pandora/services/account/login/internal/data"
)

// owner 类型 / 阶段常量(对齐 owner.proto OwnerType/OwnerPhase;biz 不依赖生成枚举)。
const (
	ownerTypeNone     int8 = 0
	ownerTypeHub      int8 = 1
	ownerTypeBattle   int8 = 2
	ownerPhaseAdmitted int8 = 2
)

// OwnerPlacementQuerier 查询完整 owner placement(§9.23 query-first;由 data.GrpcOwnerReleaser 实现)。
// 与 OwnerReleaser 分开是为了不破坏既有登出释放接口的实现方(含测试 fake)。可为 nil。
type OwnerPlacementQuerier interface {
	QueryOwnerPlacement(ctx context.Context, playerID uint64) (data.OwnerPlacementView, error)
}

// SetOwnerPlacementQuerier 注入 owner placement 查询器(nil-safe)。
func (u *LoginUsecase) SetOwnerPlacementQuerier(q OwnerPlacementQuerier) {
	u.ownerPlacement = q
}

// SetOwnerQueryFirst 开关 §9.23 query-first 叠加(默认 false;true 才在恢复路径查 owner)。
func (u *LoginUsecase) SetOwnerQueryFirst(enabled bool) {
	u.ownerQueryFirst = enabled
}

// ownerTypeToResumeRoute 把 owner 类型映射为恢复路由(none → UNSPECIFIED)。
func ownerTypeToResumeRoute(ownerType int8) loginv1.ResumeRoute {
	switch ownerType {
	case ownerTypeHub:
		return loginv1.ResumeRoute_RESUME_ROUTE_HUB
	case ownerTypeBattle:
		return loginv1.ResumeRoute_RESUME_ROUTE_BATTLE
	default:
		return loginv1.ResumeRoute_RESUME_ROUTE_UNSPECIFIED
	}
}

// overlayOwnerPlacement 查 owner placement 并叠加到既有解析 out 上(查询失败 = UNKNOWN,
// migrate 阶段保持 out 不变,记 WARN——禁冒充 OFFLINE/改路由)。
func (u *LoginUsecase) overlayOwnerPlacement(ctx context.Context, playerID uint64, out ResumeContextResult) ResumeContextResult {
	v, err := u.ownerPlacement.QueryOwnerPlacement(ctx, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "owner_query_first_unavailable",
			"player_id", playerID, "err", err, "hint", "migrate 双门并行:保持既有路由,不改路由决策")
		return out
	}
	return applyOwnerPlacement(out, v, time.Now().UnixMilli())
}

// applyOwnerPlacement 纯函数:owner 路由与既有解析一致时叠加权威 placement 字段,**不改 Route**。
// 一致条件之外(owner 无记录 / 路由不一致)一律原样返回 out(migrate 双门并行,安全优先)。
// STABLE 判定:ADMITTED 且实例租约未过期;否则 PENDING(屏障/租约未确证,客户端保留 operation 重试)。
func applyOwnerPlacement(out ResumeContextResult, v data.OwnerPlacementView, nowMs int64) ResumeContextResult {
	route := ownerTypeToResumeRoute(v.OwnerType)
	if route == loginv1.ResumeRoute_RESUME_ROUTE_UNSPECIFIED || route != out.Route {
		return out
	}
	out.OperationID = v.OperationID
	out.DSPodName = v.PodName
	out.DSInstanceUID = v.InstanceUID
	out.DSInstanceEpoch = v.InstanceEpoch
	out.ReleaseTrack = v.ReleaseTrack
	if v.OwnerType == ownerTypeHub {
		out.HubAssignmentID = v.AssignmentOrAllocationID
	} else {
		out.AllocationID = v.AssignmentOrAllocationID
	}
	if v.Phase == ownerPhaseAdmitted && v.LeaseDeadlineMs > nowMs {
		out.PlacementState = loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_STABLE
	} else {
		out.PlacementState = loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_PENDING
	}
	return out
}
