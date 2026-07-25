// owner_query.go — §9.23 query-first 进场路由(owner-authority.md;R11 复审 架构 P0 收口)。
//
// 契约变更(R11 复审):本文件此前是「默认关、同路由、只叠加字段」的 overlay——owner 查询
// 失败会回落旧路由,owner 路由与 locator 解析不一致时直接放弃叠加,owner 从不改变路由决策。
// 那只能叫「同路由 owner 信息叠加」,不是 query-first。现在:
//
//	① **owner 是路由的第一权威**:先查 owner,有归属记录即由它决定 Route 与 exact target,
//	   locator/matchmaker 降级为富化(match_stage / game_mode / map_id)。
//	② **查询失败不回落**:结果不确定一律返回 WAIT + OWNER_UNKNOWN + retry_after
//	   (§9.22:查询超时/依赖故障/结果不确定必须 UNKNOWN 并重试或 fail-closed,
//	   禁止冒充 OFFLINE、空闲、成功或默认 Hub)。
//	③ **屏障可见**:admit_not_before 未开时返回 WAIT + ADMIT_BARRIER + 由屏障推导的
//	   retry_after,而不是把它丢掉(此前 AdmitNotBeforeMs 取回来就没用过)。
//	④ **owner_epoch 出参**:此前 data 层取回后被静默丢弃,客户端拿不到 §9.23 要求的
//	   (state, exact target, owner_epoch) 三元组,幂等 no-op 无从判定。
//	⑤ **STABLE 不再由 login 本地时钟裸判**:租约剩余寿命必须超过时钟/网络安全余量,
//	   否则只报 PENDING(宁可让客户端多重查一次,也不谎报"已稳定可玩")。
//
// 「无 owner 记录」是唯一委派给旧链的情况:那是首次进场(尚未有任何归属),由角色/撮合/
// 首个 Hub 分配链处理。这不是回落——owner 明确回答了"没有归属"。
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
	ownerTypeNone      int8 = 0
	ownerTypeHub       int8 = 1
	ownerTypeBattle    int8 = 2
	ownerPhaseAdmitted int8 = 2
)

const (
	// ownerUnknownRetryAfterMs 是 owner 权威不可达时给客户端的建议退避。
	// 取值依据:owner 是 TiDB 支撑的线性一致权威,一次故障切换/重选举量级在秒内;
	// 1s 足够跨过瞬时抖动,又不会让玩家在真实恢复后还傻等。客户端仍会叠加自己的
	// 退避+jitter,这里只是下界提示。
	ownerUnknownRetryAfterMs uint32 = 1000
	// ownerLeaseSkewMarginMs 是判定 STABLE 时要求的租约剩余寿命下界(时钟偏移 + 网络
	// 往返 + 客户端处理的安全余量)。低于它就只报 PENDING:§9.22 要求
	// 「旧 DS 最晚停止可玩时间 < 新 DS 最早开始可玩时间」,login 用自己的时钟贴边判定
	// STABLE 会把这条时序悄悄破掉。
	ownerLeaseSkewMarginMs int64 = 2000
	// ownerRetryAfterCeilingMs 钳住由屏障推导的 retry_after,避免权威给出异常大的
	// admit_not_before 时让客户端长时间不重查(§9.19 每次等待必须有界)。
	ownerRetryAfterCeilingMs int64 = 10000
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

// SetOwnerQueryFirst 开关 §9.23 query-first(默认 false = 保持旧的 locator-first 路由)。
// 打开后 owner 成为路由第一权威,失败返回 WAIT 而不是回落旧路由。
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

// waitResume 构造一个 §9.23 WAIT 结果:路由 UNKNOWN(明示"不知道",不是 Hub),
// 带原因与有界 retry_after。客户端必须保留 session 与原 operation_id 后重查。
func waitResume(reason loginv1.ResumeWaitReason, retryAfterMs uint32) ResumeContextResult {
	return ResumeContextResult{
		Route:        loginv1.ResumeRoute_RESUME_ROUTE_UNKNOWN,
		EntryState:   loginv1.ResumeEntryState_RESUME_ENTRY_STATE_WAIT,
		WaitReason:   reason,
		RetryAfterMs: retryAfterMs,
	}
}

// resolveResumeFromOwner 按 query-first 语义先问 owner。
//
// 返回 decided=false 只有一种情况:owner 明确回答"该玩家当前没有归属记录"——此时交给
// 首次进场链(角色 → 撮合 → 首个 Hub)。其余情况 owner 的答案就是最终答案。
func (u *LoginUsecase) resolveResumeFromOwner(ctx context.Context, playerID uint64) (bool, ResumeContextResult) {
	v, err := u.ownerPlacement.QueryOwnerPlacement(ctx, playerID)
	if err != nil {
		// 不可判定 → WAIT。**绝不回落旧路由**:locator presence 只是投影,key miss
		// 不能证明玩家已离开旧 DS,更不能授权进入另一台 DS(§9.22)。
		plog.With(ctx).Warnw("msg", "owner_query_first_unavailable",
			"player_id", playerID, "err", err,
			"hint", "query-first:返回 WAIT/UNKNOWN 让客户端退避重查,不猜路由")
		return true, waitResume(loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN,
			ownerUnknownRetryAfterMs)
	}
	route := ownerTypeToResumeRoute(v.OwnerType)
	if route == loginv1.ResumeRoute_RESUME_ROUTE_UNSPECIFIED {
		// owner 权威明确回答"无归属":这是首次进场,不是故障,交旧链继续。
		return false, ResumeContextResult{}
	}
	return true, applyOwnerPlacement(ResumeContextResult{Route: route}, v, time.Now().UnixMilli())
}

// applyOwnerPlacement 纯函数:把 owner 权威记录翻译成 §9.23 的进场状态。
//
// 判定顺序(先安全后可玩):
//
//	① admit_not_before 屏障未开 → WAIT + ADMIT_BARRIER(旧 DS 可能仍在可玩窗口内,
//	   此刻放行就是双 DS)。retry_after 由屏障推导并钳上界。
//	② ADMITTED 且租约剩余 > 安全余量 → TARGET + STABLE(唯一可以宣称"可幂等 no-op"的态)。
//	③ 其余 → TARGET + PENDING(目标已定但未确证,客户端保留 operation 重试)。
func applyOwnerPlacement(out ResumeContextResult, v data.OwnerPlacementView, nowMs int64) ResumeContextResult {
	out.OwnerEpoch = v.OwnerEpoch
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

	if v.AdmitNotBeforeMs > nowMs {
		// 屏障未开:保留 exact target 与 owner_epoch(客户端要拿它续用同一 operation),
		// 但状态是 WAIT——不得让客户端此刻 Travel/占座。
		remain := v.AdmitNotBeforeMs - nowMs
		if remain > ownerRetryAfterCeilingMs {
			remain = ownerRetryAfterCeilingMs
		}
		out.EntryState = loginv1.ResumeEntryState_RESUME_ENTRY_STATE_WAIT
		out.WaitReason = loginv1.ResumeWaitReason_RESUME_WAIT_REASON_ADMIT_BARRIER
		out.RetryAfterMs = uint32(remain)
		out.PlacementState = loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_PENDING
		return out
	}

	out.EntryState = loginv1.ResumeEntryState_RESUME_ENTRY_STATE_TARGET
	if v.Phase == ownerPhaseAdmitted && v.LeaseDeadlineMs-nowMs > ownerLeaseSkewMarginMs {
		out.PlacementState = loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_STABLE
	} else {
		out.PlacementState = loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_PENDING
	}
	return out
}
