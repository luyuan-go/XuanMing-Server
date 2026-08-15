// Package service 是 matchmaker 服务的 gRPC service 层(W4 ①,2026-06-06)。
//
// 职责:
//   - 实现 matchv1.MatchServiceServer 接口
//   - 从 ctx 取 JWT player_id(防伪造他人身份),覆盖 request 中的 player_id
//   - proto Request/Response ↔ biz 入参/出参互转
//   - errcode.Code → commonv1.ErrCode 1:1 映射
//
// 4 个 RPC 全"已受理型"(协议原则 3):返回 code/match_id 后,客户端 UI 状态机
// 由 pandora.match.progress push 驱动。
package service

import (
	"context"
	"errors"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"

	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/biz"
)

// MatchService 实现 matchv1.MatchServiceServer。
type MatchService struct {
	matchv1.UnimplementedMatchServiceServer
	uc             *biz.MatchUsecase
	resumeResolver playerMatchContextResolver
	resumeAuth     internalRequestVerifier
	sf             snowflakeGen
}

// snowflakeGen 是 snowflake.Node 的最小接口。
type snowflakeGen interface {
	Generate() uint64
}

type playerMatchContextResolver interface {
	ResolvePlayerMatchContext(context.Context, uint64) (*matchv1.ResolvePlayerMatchContextResponse, error)
}

type internalRequestVerifier interface {
	Verify(context.Context, string, uint64) error
}

// NewMatchService 构造 MatchService。
func NewMatchService(uc *biz.MatchUsecase, sf snowflakeGen, resumeAuth internalRequestVerifier) *MatchService {
	return &MatchService{uc: uc, resumeResolver: uc, resumeAuth: resumeAuth, sf: sf}
}

// StartMatch 把队伍加入撮合队列。captain_id 以 JWT ctx 为准。
func (s *MatchService) StartMatch(ctx context.Context, req *matchv1.StartMatchRequest) (*matchv1.StartMatchResponse, error) {
	captainID := callerID(ctx)
	if captainID == 0 {
		plog.With(ctx).Warnw("msg", "match_rpc_rejected", "rpc", "StartMatch",
			"reason", "missing_caller_identity", "team_id", req.GetTeamId(), "map_id", req.GetMapId())
		return &matchv1.StartMatchResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	// team_id==0 是合法的**单人入口**(单排撮合 / 单人进副本),名单即 JWT 身份本人。
	// 不再要求「先建一个 1 人队」——那是用组队机制模拟单人,多一次 RPC 与一个失败点。
	// 成员解析与人数校验统一在 biz.resolveMembers,这里不重复判定。

	// join key(infra.md §11.3 R3):team_id 写进 ctx,本请求后续所有日志自动带上,
	// 不必每条手写(plog.With 只自动注入 JWT 面的 player_id)。
	if teamID := req.GetTeamId(); teamID != 0 {
		ctx = plog.WithTeamID(ctx, teamID)
	}

	// entry_mode 是玩家的**选择**(§17.2),不是权威数据:能不能这么进由 biz 按关卡表
	// fail-closed 判定。这里只透传,不在 service 层预判——第二份判定就是漂移的来源。
	ticketID := s.sf.Generate()
	id, err := s.uc.StartMatch(ctx, ticketID, req.GetTeamId(), captainID, req.GetMapId(), req.GetEntryMode())
	if err != nil {
		// 具体被哪道门拒(gate/reason)由 biz 打 match_start_rejected,这里不重复。
		return &matchv1.StartMatchResponse{Code: toProtoCode(err)}, nil
	}
	return &matchv1.StartMatchResponse{Code: commonv1.ErrCode_OK, MatchId: id}, nil
}

// CancelMatch 取消匹配。player_id 以 JWT ctx 为准。
//   - 客户端路径:Envoy jwt_authn 注入 callerID(!=0),req.player_id 被忽略——客户端伪造
//     请求体里的 player_id 无效,只能取消自己的排队。
//   - 内部路径(callerID==0,不经 Envoy):team 服务离队/踢人联动撤票时按 req.player_id
//     取消(该成员所在整张票据)。信任模型与 ReleaseMatch 相同:内网直连才可能 callerID==0。
func (s *MatchService) CancelMatch(ctx context.Context, req *matchv1.CancelMatchRequest) (*matchv1.CancelMatchResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		playerID = req.GetPlayerId()
	}
	if playerID == 0 {
		plog.With(ctx).Warnw("msg", "match_rpc_rejected", "rpc", "CancelMatch",
			"reason", "missing_caller_identity")
		return &matchv1.CancelMatchResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	// 内部路径(team 联动撤票)callerID==0,plog 不会自动注入 player_id,手写进 ctx。
	ctx = plog.WithPlayerID(ctx, playerID)
	if err := s.uc.CancelMatch(ctx, playerID); err != nil {
		return &matchv1.CancelMatchResponse{Code: toProtoCode(err)}, nil
	}
	return &matchv1.CancelMatchResponse{Code: commonv1.ErrCode_OK}, nil
}

// ConfirmMatch 确认/拒绝匹配。player_id 以 JWT ctx 为准。
func (s *MatchService) ConfirmMatch(ctx context.Context, req *matchv1.ConfirmMatchRequest) (*matchv1.ConfirmMatchResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		plog.With(ctx).Warnw("msg", "match_rpc_rejected", "rpc", "ConfirmMatch",
			"reason", "missing_caller_identity", "match_id", req.GetMatchId())
		return &matchv1.ConfirmMatchResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetMatchId() == 0 {
		plog.With(ctx).Warnw("msg", "match_rpc_rejected", "rpc", "ConfirmMatch",
			"reason", "missing_match_id", "accept", req.GetAccept())
		return &matchv1.ConfirmMatchResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	ctx = plog.WithMatchID(ctx, req.GetMatchId())
	if err := s.uc.ConfirmMatch(ctx, playerID, req.GetMatchId(), req.GetAccept()); err != nil {
		return &matchv1.ConfirmMatchResponse{Code: toProtoCode(err)}, nil
	}
	return &matchv1.ConfirmMatchResponse{Code: commonv1.ErrCode_OK}, nil
}

// GetMatchProgress 查询匹配进度。
//   - 身份以 JWT ctx 为准:callerID==0 直接拒(防伪造 / 未鉴权)。
//   - match_id 可为 0:重新登录 / 换设备丢句柄时,biz 按 callerID 反查本人票据(重连兜底)。
//   - 鉴权下沉 biz:caller 必须是该 match/ticket 成员,否则按"不存在"处理,防外挂拉别人对局。
func (s *MatchService) GetMatchProgress(ctx context.Context, req *matchv1.GetMatchProgressRequest) (*matchv1.GetMatchProgressResponse, error) {
	caller := callerID(ctx)
	if caller == 0 {
		plog.With(ctx).Warnw("msg", "match_rpc_rejected", "rpc", "GetMatchProgress",
			"reason", "missing_caller_identity", "match_id", req.GetMatchId())
		return &matchv1.GetMatchProgressResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetMatchId() != 0 {
		ctx = plog.WithMatchID(ctx, req.GetMatchId())
	}
	prog, err := s.uc.GetMatchProgress(ctx, caller, req.GetMatchId())
	if err != nil {
		return &matchv1.GetMatchProgressResponse{Code: toProtoCode(err)}, nil
	}
	return &matchv1.GetMatchProgressResponse{Code: commonv1.ErrCode_OK, Progress: prog}, nil
}

// ReleaseMatch 释放一场已结束(结算 / abandoned)对局的撮合状态。
//   - 后端内部接口(battle_result 结算落库后调用),不经 Envoy / 不取 JWT player_id。
//   - 系统接口拒绝玩家 JWT(镜像 leaderboard.SubmitScore 模式):经 Envoy 进来的客户端请求
//     一定带 jwt_authn 注入的 x-pandora-player-id(callerID!=0),直接拒——否则任何登录玩家
//     可用任意 match_id 摧毁他人在局撮合状态(删票据/claim/match,griefing + 绕过不变量 §1)。
//   - 按 match_id(+ 兜底 player_ids)操作;幂等,重复调用 / 已释放均返回 OK。
func (s *MatchService) ReleaseMatch(ctx context.Context, req *matchv1.ReleaseMatchRequest) (*matchv1.ReleaseMatchResponse, error) {
	if caller := callerID(ctx); caller != 0 {
		plog.With(ctx).Warnw("msg", "match_rpc_rejected", "rpc", "ReleaseMatch",
			"reason", "player_jwt_not_allowed", "match_id", req.GetMatchId(), "player_id", caller)
		return &matchv1.ReleaseMatchResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	if req.GetMatchId() == 0 {
		plog.With(ctx).Warnw("msg", "match_rpc_rejected", "rpc", "ReleaseMatch",
			"reason", "missing_match_id", "players", len(req.GetPlayerIds()))
		return &matchv1.ReleaseMatchResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// 内部面(battle_result 调用,不带玩家 JWT):match_id 手写进 ctx,
	// 释放链后续日志自动带 join key。
	ctx = plog.WithMatchID(ctx, req.GetMatchId())
	if err := s.uc.ReleaseMatch(ctx, req.GetMatchId(), req.GetPlayerIds()); err != nil {
		plog.With(ctx).Warnw("msg", "match_release_rejected", "reason", "usecase_failed",
			"code", int(errcode.As(err)), "players", len(req.GetPlayerIds()), "err", err)
		return &matchv1.ReleaseMatchResponse{Code: toProtoCode(err)}, nil
	}
	return &matchv1.ReleaseMatchResponse{Code: commonv1.ErrCode_OK}, nil
}

// ResolvePlayerMatchContext is an internal read used by login while resolving
// durable matchmaking state. The client Envoy exact-path denies it;
// this application guard rejects player JWTs and requires a fresh request-bound
// Login service HMAC whose nonce is atomically consumed in shared Redis. READY
// credentials are generated from the canonical match's persisted exact target;
// Login consumes them for cold reconnect instead of rebuilding a weaker ticket
// from the short-TTL roster projection.
func (s *MatchService) ResolvePlayerMatchContext(ctx context.Context, req *matchv1.ResolvePlayerMatchContextRequest) (*matchv1.ResolvePlayerMatchContextResponse, error) {
	if caller := callerID(ctx); caller != 0 {
		plog.With(ctx).Warnw("msg", "match_rpc_rejected", "rpc", "ResolvePlayerMatchContext",
			"reason", "player_jwt_not_allowed", "player_id", caller)
		return &matchv1.ResolvePlayerMatchContextResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	if req.GetPlayerId() == 0 {
		plog.With(ctx).Warnw("msg", "match_rpc_rejected", "rpc", "ResolvePlayerMatchContext",
			"reason", "missing_player_id")
		return &matchv1.ResolvePlayerMatchContextResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// 内部面无 JWT,player_id 必须手写进 ctx(§11.3 R3)。
	ctx = plog.WithPlayerID(ctx, req.GetPlayerId())
	if s.resumeAuth == nil {
		plog.With(ctx).Warnw("msg", "match_rpc_rejected", "rpc", "ResolvePlayerMatchContext",
			"reason", "resume_auth_unconfigured")
		return &matchv1.ResolvePlayerMatchContextResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}, nil
	}
	if err := s.resumeAuth.Verify(ctx, matchv1.MatchService_ResolvePlayerMatchContext_FullMethodName, req.GetPlayerId()); err != nil {
		code := commonv1.ErrCode_ERR_PERMISSION_DENY
		reason := "resume_auth_signature_rejected"
		if errors.Is(err, internalrpcauth.ErrUnavailable) {
			code = commonv1.ErrCode_ERR_UNAVAILABLE
			reason = "resume_auth_replay_store_unavailable"
		}
		plog.With(ctx).Warnw("msg", "resolve_match_context_service_auth_rejected",
			"reason", reason,
			"player_id", req.GetPlayerId(), "replay_authority_unavailable", code == commonv1.ErrCode_ERR_UNAVAILABLE)
		return &matchv1.ResolvePlayerMatchContextResponse{Code: code}, nil
	}
	if s.resumeResolver == nil {
		plog.With(ctx).Warnw("msg", "match_rpc_rejected", "rpc", "ResolvePlayerMatchContext",
			"reason", "resume_resolver_unconfigured")
		return &matchv1.ResolvePlayerMatchContextResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}, nil
	}
	resolved, err := s.resumeResolver.ResolvePlayerMatchContext(ctx, req.GetPlayerId())
	if err != nil {
		plog.With(ctx).Warnw("msg", "resolve_match_context_failed", "reason", "resolve_unavailable",
			"code", int(errcode.As(err)), "player_id", req.GetPlayerId(), "err", err)
		return &matchv1.ResolvePlayerMatchContextResponse{Code: toProtoCode(err)}, nil
	}
	resolved.Code = commonv1.ErrCode_OK
	plog.With(ctx).Infow("msg", "resolve_match_context_ok",
		"player_id", req.GetPlayerId(), "state", resolved.GetState().String(),
		"stage", resolved.GetStage().String(), "ticket_id", resolved.GetTicketId(),
		"match_id", resolved.GetMatchId(), "map_id", resolved.GetMapId(),
		"game_mode", resolved.GetGameMode(), "has_battle_ticket", resolved.GetBattleTicket() != "")
	return resolved, nil
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
