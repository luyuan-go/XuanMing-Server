// Package service — owner 权威 gRPC service 层(pandora.owner.v1)。
//
// 鉴权边界:全部 RPC 是内部系统接口(调用方 = login / allocator / DS 回调链等内网服务);
// 带玩家 JWT 的客户端调用一律拒(Envoy 侧对 /pandora.owner.v1/ 前缀另有 403 拦截,双保险)。
package service

import (
	"context"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	ownerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/owner/v1"

	"github.com/luyuancpp/pandora/services/runtime/owner/internal/biz"
	"github.com/luyuancpp/pandora/services/runtime/owner/internal/data"
)

// OwnerService 实现 ownerv1.OwnerServiceServer。
type OwnerService struct {
	ownerv1.UnimplementedOwnerServiceServer
	uc *biz.OwnerUsecase
}

// NewOwnerService 构造。
func NewOwnerService(uc *biz.OwnerUsecase) *OwnerService {
	return &OwnerService{uc: uc}
}

// rejectClientCaller 系统接口守卫:带玩家 JWT 的调用(callerID>0)一律拒。
func rejectClientCaller(ctx context.Context) bool {
	return pmw.PlayerIDFromContext(ctx) != 0
}

// logClientCallerDenied 记录一次玩家 JWT 直敲 owner 内部接口的拒绝。
//
// ERR_PERMISSION_DENY 是 in-band Code,handler 返 nil error → access log 只记 DEBUG。
// 稳态下本日志应恒 0(Envoy 已在前置 403);一旦出现就是「Envoy 拦截漏了」或
// 「内部调用方错把玩家上下文透传进来了」,必须可见。
func logClientCallerDenied(ctx context.Context, rpc string, reqPlayerID uint64) {
	plog.With(ctx).Warnw("msg", "owner_caller_denied",
		"rpc", rpc, "reason", "client_caller_on_internal_rpc",
		"caller_player_id", pmw.PlayerIDFromContext(ctx), "player_id", reqPlayerID)
}

func toProtoCode(err error) commonv1.ErrCode {
	return commonv1.ErrCode(errcode.As(err))
}

func toProtoTarget(t data.OwnerTarget) *ownerv1.OwnerTarget {
	return &ownerv1.OwnerTarget{
		PodName:                  t.PodName,
		InstanceUid:              t.InstanceUID,
		InstanceEpoch:            t.InstanceEpoch,
		AssignmentOrAllocationId: t.AssignmentOrAllocationID,
		ReleaseTrack:             t.ReleaseTrack,
	}
}

func fromProtoTarget(t *ownerv1.OwnerTarget) data.OwnerTarget {
	return data.OwnerTarget{
		PodName:                  t.GetPodName(),
		InstanceUID:              t.GetInstanceUid(),
		InstanceEpoch:            t.GetInstanceEpoch(),
		AssignmentOrAllocationID: t.GetAssignmentOrAllocationId(),
		ReleaseTrack:             t.GetReleaseTrack(),
	}
}

func toProtoRecord(rec data.OwnerRecord) *ownerv1.OwnerRecord {
	return &ownerv1.OwnerRecord{
		PlayerId:         rec.PlayerID,
		OwnerEpoch:       rec.OwnerEpoch,
		OwnerType:        ownerv1.OwnerType(rec.OwnerType),
		Phase:            ownerv1.OwnerPhase(rec.Phase),
		Target:           toProtoTarget(rec.Target),
		OperationId:      rec.OperationID,
		AdmitNotBeforeMs: rec.AdmitNotBeforeMs,
		LeaseDeadlineMs:  rec.LeaseDeadlineMs,
		UpdatedAtMs:      rec.UpdatedAtMs,
		// 高水位下发给调用方(INC-20260818-003):allocator 据此判断本部署是否已进入
		// 带版本阶段,也让排障时不必开库就能看到门的位置。
		HubSourceRevision: rec.HubSourceRevision,
	}
}

// QueryOwner 读当前 owner 记录。
func (s *OwnerService) QueryOwner(ctx context.Context, req *ownerv1.QueryOwnerRequest) (*ownerv1.QueryOwnerResponse, error) {
	if rejectClientCaller(ctx) {
		logClientCallerDenied(ctx, "QueryOwner", req.GetPlayerId())
		return &ownerv1.QueryOwnerResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	rec, err := s.uc.Query(ctx, req.GetPlayerId())
	if err != nil {
		return &ownerv1.QueryOwnerResponse{Code: toProtoCode(err)}, nil
	}
	// 高频只读路径 → DEBUG(§11.3 R4)。开 debug 后它直接回答「这玩家当前归谁、
	// epoch 多少、哪个 operation 推的」—— 与下游的 UNKNOWN 退避日志对账用。
	plog.With(ctx).Debugw("msg", "owner_queried",
		"player_id", req.GetPlayerId(), "owner_epoch", rec.OwnerEpoch,
		"owner_type", rec.OwnerType, "phase", rec.Phase,
		"cur_pod", rec.Target.PodName, "cur_instance_uid", rec.Target.InstanceUID,
		"cur_instance_epoch", rec.Target.InstanceEpoch,
		"assignment_id", rec.Target.AssignmentOrAllocationID,
		"release_track", rec.Target.ReleaseTrack,
		"operation_id", rec.OperationID,
		"admit_not_before_ms", rec.AdmitNotBeforeMs,
		"lease_deadline_ms", rec.LeaseDeadlineMs, "updated_at_ms", rec.UpdatedAtMs)
	return &ownerv1.QueryOwnerResponse{Code: commonv1.ErrCode_OK, Record: toProtoRecord(rec)}, nil
}

// BeginTransition 发起 owner 迁移(EPOCH_CONFLICT 时响应仍携带当前记录供重查决策)。
func (s *OwnerService) BeginTransition(ctx context.Context, req *ownerv1.BeginTransitionRequest) (*ownerv1.BeginTransitionResponse, error) {
	if rejectClientCaller(ctx) {
		logClientCallerDenied(ctx, "BeginTransition", req.GetPlayerId())
		return &ownerv1.BeginTransitionResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	rec, err := s.uc.BeginTransition(ctx, req.GetPlayerId(), req.GetExpectEpoch(),
		req.GetOperationId(), int8(req.GetOwnerType()), fromProtoTarget(req.GetTarget()),
		req.GetSourceRevision())
	if err != nil {
		resp := &ownerv1.BeginTransitionResponse{Code: toProtoCode(err)}
		if errcode.As(err) == errcode.ErrOwnerEpochConflict {
			resp.Record = toProtoRecord(rec) // 冲突附当前记录(§9.23 query-first 重查依据)
		}
		return resp, nil
	}
	return &ownerv1.BeginTransitionResponse{Code: commonv1.ErrCode_OK, Record: toProtoRecord(rec)}, nil
}

// Admit 准入提交(BARRIER_NOT_OPEN 带 retry_after_ms;已 ADMITTED 幂等重放)。
func (s *OwnerService) Admit(ctx context.Context, req *ownerv1.AdmitRequest) (*ownerv1.AdmitResponse, error) {
	if rejectClientCaller(ctx) {
		logClientCallerDenied(ctx, "Admit", req.GetPlayerId())
		return &ownerv1.AdmitResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	rec, retryAfter, err := s.uc.Admit(ctx, req.GetPlayerId(), req.GetOwnerEpoch(),
		req.GetOperationId(), fromProtoTarget(req.GetTarget()))
	if err != nil {
		resp := &ownerv1.AdmitResponse{Code: toProtoCode(err), RetryAfterMs: retryAfter}
		if errcode.As(err) == errcode.ErrOwnerBarrierNotOpen {
			resp.Record = toProtoRecord(rec)
		}
		return resp, nil
	}
	return &ownerv1.AdmitResponse{Code: commonv1.ErrCode_OK, Record: toProtoRecord(rec)}, nil
}

// RenewInstanceLease 实例租约续期。
func (s *OwnerService) RenewInstanceLease(ctx context.Context, req *ownerv1.RenewInstanceLeaseRequest) (*ownerv1.RenewInstanceLeaseResponse, error) {
	if rejectClientCaller(ctx) {
		logClientCallerDenied(ctx, "RenewInstanceLease", 0)
		return &ownerv1.RenewInstanceLeaseResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	deadline, err := s.uc.RenewInstanceLease(ctx, fromProtoTarget(req.GetTarget()), req.GetLeaseSeconds())
	if err != nil {
		return &ownerv1.RenewInstanceLeaseResponse{Code: toProtoCode(err)}, nil
	}
	return &ownerv1.RenewInstanceLeaseResponse{Code: commonv1.ErrCode_OK, LeaseDeadlineMs: deadline}, nil
}

// ReleaseOwner 显式释放(迟到调用幂等 no-op 返回当前记录)。
func (s *OwnerService) ReleaseOwner(ctx context.Context, req *ownerv1.ReleaseOwnerRequest) (*ownerv1.ReleaseOwnerResponse, error) {
	if rejectClientCaller(ctx) {
		logClientCallerDenied(ctx, "ReleaseOwner", req.GetPlayerId())
		return &ownerv1.ReleaseOwnerResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	rec, err := s.uc.Release(ctx, req.GetPlayerId(), req.GetOwnerEpoch(), req.GetOperationId())
	if err != nil {
		return &ownerv1.ReleaseOwnerResponse{Code: toProtoCode(err)}, nil
	}
	return &ownerv1.ReleaseOwnerResponse{Code: commonv1.ErrCode_OK, Record: toProtoRecord(rec)}, nil
}
