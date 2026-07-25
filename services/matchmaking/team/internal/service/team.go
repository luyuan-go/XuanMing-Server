// Package service 是 team 服务的 gRPC service 层(W3 ⑦ Phase 4,2026-06-05)。
//
// 职责:
//   - 实现 teamv1.TeamServiceServer 接口
//   - 从 ctx 取 JWT player_id(R5:override request 字段,防伪造他人身份)
//   - proto Request/Response ↔ biz 入参/出参互转(R1:Response 包含完整 Team 快照)
//   - errcode.Code → commonv1.ErrCode 1:1 映射
//
// 协议原则(R5):所有写 RPC 强制用 ctx 中的 player_id 覆盖 request 的对应字段。
// player_id=0 时返回 ERR_UNAUTHORIZED(Envoy jwt_authn 已在路由层 require JWT)。
package service

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"

	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/biz"
)

// TeamService 实现 teamv1.TeamServiceServer。
type TeamService struct {
	teamv1.UnimplementedTeamServiceServer
	uc *biz.TeamUsecase
	sf snowflakeGen
}

// snowflakeGen 是 snowflake.Node 的最小接口,避免 service 直接依赖 snowflake 包。
type snowflakeGen interface {
	Generate() uint64
}

// NewTeamService 构造 TeamService。
func NewTeamService(uc *biz.TeamUsecase, sf snowflakeGen) *TeamService {
	return &TeamService{uc: uc, sf: sf}
}

// ── 9 RPC ─────────────────────────────────────────────────────────────────────

// CreateTeam 创建队伍。player_id 以 JWT ctx 为准(R5)。
func (s *TeamService) CreateTeam(ctx context.Context, _ *teamv1.CreateTeamRequest) (*teamv1.CreateTeamResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &teamv1.CreateTeamResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}

	teamID := s.sf.Generate()
	rec, err := s.uc.CreateTeam(ctx, teamID, playerID)
	if err != nil {
		return &teamv1.CreateTeamResponse{Code: toProtoCode(err)}, nil
	}
	return &teamv1.CreateTeamResponse{
		Code:   commonv1.ErrCode_OK,
		TeamId: rec.TeamId,
		Team:   biz.RecordToProto(rec),
	}, nil
}

// Invite 邀请玩家。inviter_id 以 JWT ctx 为准(R5)。
func (s *TeamService) Invite(ctx context.Context, req *teamv1.InviteRequest) (*teamv1.InviteResponse, error) {
	inviterID := callerID(ctx)
	if inviterID == 0 {
		return &teamv1.InviteResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 || req.GetTargetPlayerId() == 0 {
		return &teamv1.InviteResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}

	inviteID := s.sf.Generate()
	rec, err := s.uc.Invite(ctx, inviteID, req.GetTeamId(), inviterID, req.GetTargetPlayerId())
	if err != nil {
		return &teamv1.InviteResponse{Code: toProtoCode(err)}, nil
	}
	// expires_at_ms 以"现在"为锚点,与 biz.SetInvite 写 redis 的 TTL 起算点一致;
	// 不能用 rec.UpdatedAtMs(那是队伍上次变更时间,Invite 不改队伍,会偏早过期)。
	expiresAtMs := time.Now().UnixMilli() + s.uc.InviteTTLMs()
	return &teamv1.InviteResponse{
		Code:        commonv1.ErrCode_OK,
		Team:        biz.RecordToProto(rec),
		InviteId:    inviteID,
		ExpiresAtMs: expiresAtMs,
	}, nil
}

// AcceptInvite 接受邀请。player_id 以 JWT ctx 为准(R5)。
func (s *TeamService) AcceptInvite(ctx context.Context, req *teamv1.AcceptInviteRequest) (*teamv1.AcceptInviteResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &teamv1.AcceptInviteResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 {
		return &teamv1.AcceptInviteResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}

	rec, err := s.uc.AcceptInvite(ctx, req.GetInviteId(), req.GetTeamId(), playerID)
	if err != nil {
		return &teamv1.AcceptInviteResponse{Code: toProtoCode(err)}, nil
	}
	return &teamv1.AcceptInviteResponse{
		Code: commonv1.ErrCode_OK,
		Team: biz.RecordToProto(rec),
	}, nil
}

// LeaveTeam 离队。player_id 以 JWT ctx 为准(R5)。
func (s *TeamService) LeaveTeam(ctx context.Context, req *teamv1.LeaveTeamRequest) (*teamv1.LeaveTeamResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &teamv1.LeaveTeamResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 {
		return &teamv1.LeaveTeamResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}

	rec, err := s.uc.LeaveTeam(ctx, req.GetTeamId(), playerID)
	if err != nil {
		return &teamv1.LeaveTeamResponse{Code: toProtoCode(err)}, nil
	}
	return &teamv1.LeaveTeamResponse{
		Code: commonv1.ErrCode_OK,
		Team: biz.RecordToProto(rec),
	}, nil
}

// Kick 踢人。captain_id 以 JWT ctx 为准(R5)。
func (s *TeamService) Kick(ctx context.Context, req *teamv1.KickRequest) (*teamv1.KickResponse, error) {
	captainID := callerID(ctx)
	if captainID == 0 {
		return &teamv1.KickResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 || req.GetTargetPlayerId() == 0 {
		return &teamv1.KickResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}

	rec, err := s.uc.Kick(ctx, req.GetTeamId(), captainID, req.GetTargetPlayerId())
	if err != nil {
		return &teamv1.KickResponse{Code: toProtoCode(err)}, nil
	}
	return &teamv1.KickResponse{
		Code: commonv1.ErrCode_OK,
		Team: biz.RecordToProto(rec),
	}, nil
}

// SetReady 设置准备状态。player_id 以 JWT ctx 为准(R5)。
func (s *TeamService) SetReady(ctx context.Context, req *teamv1.SetReadyRequest) (*teamv1.SetReadyResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &teamv1.SetReadyResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTeamId() == 0 {
		return &teamv1.SetReadyResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}

	rec, err := s.uc.SetReady(ctx, req.GetTeamId(), playerID, req.GetReady(), req.GetHeroId())
	if err != nil {
		return &teamv1.SetReadyResponse{Code: toProtoCode(err)}, nil
	}
	return &teamv1.SetReadyResponse{
		Code: commonv1.ErrCode_OK,
		Team: biz.RecordToProto(rec),
	}, nil
}

// GetTeam 查询队伍(只读,无鉴权要求,team_id 即授权)。
func (s *TeamService) GetTeam(ctx context.Context, req *teamv1.GetTeamRequest) (*teamv1.GetTeamResponse, error) {
	if req.GetTeamId() == 0 {
		return &teamv1.GetTeamResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}

	rec, err := s.uc.GetTeam(ctx, req.GetTeamId())
	if err != nil {
		return &teamv1.GetTeamResponse{Code: toProtoCode(err)}, nil
	}
	return &teamv1.GetTeamResponse{
		Code: commonv1.ErrCode_OK,
		Team: biz.RecordToProto(rec),
	}, nil
}

// GetMyTeam 查询自己当前所在队伍的完整快照(队伍主界面直接渲染)。player_id 以 JWT ctx 为准(R5)。
// 没队伍是正常态:返 OK + has_team_msg=false,不用 errcode 表达。
func (s *TeamService) GetMyTeam(ctx context.Context, _ *teamv1.GetMyTeamRequest) (*teamv1.GetMyTeamResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &teamv1.GetMyTeamResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}

	rec, hasTeam, err := s.uc.GetMyTeam(ctx, playerID)
	if err != nil {
		return &teamv1.GetMyTeamResponse{Code: toProtoCode(err)}, nil
	}
	if !hasTeam {
		return &teamv1.GetMyTeamResponse{Code: commonv1.ErrCode_OK, HasTeamMsg: false}, nil
	}
	return &teamv1.GetMyTeamResponse{
		Code:       commonv1.ErrCode_OK,
		HasTeamMsg: true,
		Team:       biz.RecordToProto(rec),
	}, nil
}

// ListMyPendingInvites 查询"发给我的待处理邀请"(只读,拉取兜底)。player_id 以 JWT ctx 为准(R5)。
// 推送(TeamInviteEvent)只是投影/加速器,这里才是邀请令牌的权威查询(不变量 §9-22):
// 客户端在登录、回前台、打开组队 UI 时调用,推送丢帧最多延迟弹窗,不丢邀请。
// 没有邀请是正常态:返 OK + 空列表。
func (s *TeamService) ListMyPendingInvites(ctx context.Context, _ *teamv1.ListMyPendingInvitesRequest) (*teamv1.ListMyPendingInvitesResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &teamv1.ListMyPendingInvitesResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}

	recs, err := s.uc.ListPendingInvites(ctx, playerID)
	if err != nil {
		return &teamv1.ListMyPendingInvitesResponse{Code: toProtoCode(err)}, nil
	}
	invites := make([]*teamv1.PendingInvite, 0, len(recs))
	for _, r := range recs {
		invites = append(invites, &teamv1.PendingInvite{
			TeamId:      r.TeamID,
			InviteId:    r.InviteID,
			InviterId:   r.InviterID,
			ExpiresAtMs: r.ExpiresAtMs,
		})
	}
	return &teamv1.ListMyPendingInvitesResponse{
		Code:    commonv1.ErrCode_OK,
		Invites: invites,
	}, nil
}

// ── 辅助 ──────────────────────────────────────────────────────────────────────

// callerID 从 ctx 取 JWT 注入的 player_id。
func callerID(ctx context.Context) uint64 {
	id, _ := ctx.Value(plog.CtxKeyPlayerID).(uint64)
	return id
}

// toProtoCode 把 pkg/errcode 1:1 映射成 proto enum(数值相同)。
func toProtoCode(err error) commonv1.ErrCode {
	return commonv1.ErrCode(errcode.As(err))
}
