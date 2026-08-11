// Package service 是 mission 服务的 gRPC service 层(2026-08-11)。
//
// 职责(对齐 dialogue / player):
//   - 实现 missionv1.MissionServiceServer;
//   - 客户端 RPC 从 ctx 取 JWT player_id(R5:忽略请求体身份,防伪造);
//   - 系统 RPC(ReportMissionFacts / CompleteAllMissions)systemOnly:带玩家 JWT 一律
//     PERMISSION_DENY(Envoy 侧另有精确 path direct_response 403 双保险);
//   - errcode.Code → commonv1.ErrCode 1:1 映射,错误 in-band 返回(transport error 恒 nil)。
package service

import (
	"context"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	missionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/mission/v1"

	"github.com/luyuancpp/pandora/services/social/mission/internal/biz"
)

// MissionService 实现 missionv1.MissionServiceServer。
type MissionService struct {
	missionv1.UnimplementedMissionServiceServer
	uc *biz.MissionUsecase
}

// NewMissionService 构造。
func NewMissionService(uc *biz.MissionUsecase) *MissionService {
	return &MissionService{uc: uc}
}

// ── 客户端 RPC(JWT 身份)────────────────────────────────────────────────────

// ListMissions 权威快照(也是 push resync 的回源接口)。
func (s *MissionService) ListMissions(ctx context.Context, _ *missionv1.ListMissionsRequest) (*missionv1.ListMissionsResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &missionv1.ListMissionsResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	active, completed, err := s.uc.ListMissions(ctx, playerID)
	if err != nil {
		return &missionv1.ListMissionsResponse{Code: toProtoCode(err)}, nil
	}
	return &missionv1.ListMissionsResponse{Code: commonv1.ErrCode_OK, Active: active, Completed: completed}, nil
}

// AcceptMission 接取任务。
func (s *MissionService) AcceptMission(ctx context.Context, req *missionv1.AcceptMissionRequest) (*missionv1.AcceptMissionResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &missionv1.AcceptMissionResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetMissionConfigId() == 0 {
		return &missionv1.AcceptMissionResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	mission, err := s.uc.Accept(ctx, playerID, req.GetMissionConfigId())
	if err != nil {
		return &missionv1.AcceptMissionResponse{Code: toProtoCode(err)}, nil
	}
	return &missionv1.AcceptMissionResponse{Code: commonv1.ErrCode_OK, Mission: mission}, nil
}

// AbandonMission 放弃任务。
func (s *MissionService) AbandonMission(ctx context.Context, req *missionv1.AbandonMissionRequest) (*missionv1.AbandonMissionResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &missionv1.AbandonMissionResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetMissionConfigId() == 0 {
		return &missionv1.AbandonMissionResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.Abandon(ctx, playerID, req.GetMissionConfigId()); err != nil {
		return &missionv1.AbandonMissionResponse{Code: toProtoCode(err)}, nil
	}
	return &missionv1.AbandonMissionResponse{Code: commonv1.ErrCode_OK}, nil
}

// ClaimMissionReward 领奖(受理即权威;内容 at-least-once 必达)。
func (s *MissionService) ClaimMissionReward(ctx context.Context, req *missionv1.ClaimMissionRewardRequest) (*missionv1.ClaimMissionRewardResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &missionv1.ClaimMissionRewardResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetMissionConfigId() == 0 {
		return &missionv1.ClaimMissionRewardResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.Claim(ctx, playerID, req.GetMissionConfigId()); err != nil {
		return &missionv1.ClaimMissionRewardResponse{Code: toProtoCode(err)}, nil
	}
	return &missionv1.ClaimMissionRewardResponse{Code: commonv1.ErrCode_OK}, nil
}

// ── 系统 RPC(内网 callerID==0)──────────────────────────────────────────────

// ReportMissionFacts 条件事实入账(任务进度唯一写入通道;上游 battle_result 出箱)。
func (s *MissionService) ReportMissionFacts(ctx context.Context, req *missionv1.ReportMissionFactsRequest) (*missionv1.ReportMissionFactsResponse, error) {
	if code := systemOnly(ctx); code != commonv1.ErrCode_OK {
		return &missionv1.ReportMissionFactsResponse{Code: code}, nil
	}
	if req.GetPlayerId() == 0 || len(req.GetFacts()) == 0 || req.GetIdempotencyKey() == "" {
		return &missionv1.ReportMissionFactsResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	facts := make([]biz.Fact, 0, len(req.GetFacts()))
	for _, f := range req.GetFacts() {
		facts = append(facts, biz.Fact{
			Category:   uint32(f.GetConditionCategory()),
			SlotValues: f.GetConditionIds(),
			Amount:     f.GetAmount(),
		})
	}
	already, err := s.uc.ReportFacts(ctx, req.GetPlayerId(), facts, req.GetIdempotencyKey())
	if err != nil {
		return &missionv1.ReportMissionFactsResponse{Code: toProtoCode(err)}, nil
	}
	return &missionv1.ReportMissionFactsResponse{Code: commonv1.ErrCode_OK, Already: already}, nil
}

// CompleteAllMissions GM 批量完成(无副作用路径,与正常完成扇出刻意分离,不得合并)。
func (s *MissionService) CompleteAllMissions(ctx context.Context, req *missionv1.CompleteAllMissionsRequest) (*missionv1.CompleteAllMissionsResponse, error) {
	if code := systemOnly(ctx); code != commonv1.ErrCode_OK {
		return &missionv1.CompleteAllMissionsResponse{Code: code}, nil
	}
	if req.GetPlayerId() == 0 {
		return &missionv1.CompleteAllMissionsResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	count, err := s.uc.CompleteAll(ctx, req.GetPlayerId())
	if err != nil {
		return &missionv1.CompleteAllMissionsResponse{Code: toProtoCode(err)}, nil
	}
	return &missionv1.CompleteAllMissionsResponse{Code: commonv1.ErrCode_OK, CompletedCount: count}, nil
}

// ── 辅助 ───────────────────────────────────────────────────────────────────

// callerID 从 ctx 取 Envoy jwt_authn 注入的 player_id。
func callerID(ctx context.Context) uint64 {
	id, _ := ctx.Value(plog.CtxKeyPlayerID).(uint64)
	return id
}

// systemOnly 系统 RPC 只认内网直连(callerID==0);带玩家 JWT = 越权尝试(对齐 player)。
func systemOnly(ctx context.Context) commonv1.ErrCode {
	if id := callerID(ctx); id != 0 {
		return commonv1.ErrCode_ERR_PERMISSION_DENY
	}
	return commonv1.ErrCode_OK
}

// toProtoCode 把 pkg/errcode 1:1 映射成 proto enum(数值相同)。
func toProtoCode(err error) commonv1.ErrCode {
	return commonv1.ErrCode(errcode.As(err))
}
