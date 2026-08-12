// Package service 是 guild 服务的 gRPC service 层(2026-06-27)。
//
// 职责:
//   - 实现 guildv1.GuildServiceServer 接口
//   - 从 ctx 取 JWT player_id(R5:override request 字段,防伪造他人身份)
//   - proto Request/Response ↔ biz 入参/出参互转
//   - errcode.Code → commonv1.ErrCode 1:1 映射
//
// 协议原则(R5):所有写 RPC 强制用 ctx 中的 player_id,忽略请求体里的 player_id 字段;
// player_id=0 → ERR_UNAUTHORIZED(Envoy jwt_authn 已在路由层 require JWT,这里兜底)。
package service

import (
	"context"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	guildv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/guild/v1"

	"github.com/luyuancpp/pandora/services/social/guild/internal/biz"
)

// snowflakeGen 是 snowflake.Node 的最小接口,避免 service 直接依赖 snowflake 包。
type snowflakeGen interface {
	Generate() uint64
}

// GuildService 实现 guildv1.GuildServiceServer。
type GuildService struct {
	guildv1.UnimplementedGuildServiceServer
	uc *biz.GuildUsecase

	// guild_id 与 request_id 是两个互不相干的 ID 空间(公会主键 / 入会申请主键),
	// 各持一个独立发号器,各走各的 step 池。
	//
	// ⚠️ 两者共用同一 nodeID,发出的 ID 会逐位相同(见 etcdnode.ProvideSnowflakeN)。
	// 必须各自留在自己的表 / 唯一键里,禁止混进同一容器比较。
	guildSF   snowflakeGen
	requestSF snowflakeGen
}

// NewGuildService 构造。
//
// guildSF / requestSF 由 main.go 经 etcdnode.MustProvideSnowflakeN 取得。测试可传同一实例;
// 生产接线必须分开,否则失去拆分带来的容量隔离。
func NewGuildService(uc *biz.GuildUsecase, guildSF, requestSF snowflakeGen) *GuildService {
	return &GuildService{uc: uc, guildSF: guildSF, requestSF: requestSF}
}

// CreateGuild 创建公会。创建者以 JWT ctx 为准(R5)。
func (s *GuildService) CreateGuild(ctx context.Context, req *guildv1.CreateGuildRequest) (*guildv1.CreateGuildResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &guildv1.CreateGuildResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	guildID, err := s.uc.CreateGuild(ctx, playerID, req.GetName(), s.guildSF.Generate())
	if err != nil {
		return &guildv1.CreateGuildResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.CreateGuildResponse{Code: commonv1.ErrCode_OK, GuildId: guildID}, nil
}

// ApplyJoin 申请加入公会。申请人以 JWT ctx 为准(R5)。
func (s *GuildService) ApplyJoin(ctx context.Context, req *guildv1.ApplyJoinRequest) (*guildv1.ApplyJoinResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &guildv1.ApplyJoinResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	requestID, err := s.uc.ApplyJoin(ctx, playerID, req.GetGuildId(), s.requestSF.Generate())
	if err != nil {
		return &guildv1.ApplyJoinResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.ApplyJoinResponse{Code: commonv1.ErrCode_OK, RequestId: requestID}, nil
}

// ApproveJoin 审批通过。审批人以 JWT ctx 为准(R5)。
func (s *GuildService) ApproveJoin(ctx context.Context, req *guildv1.ApproveJoinRequest) (*guildv1.ApproveJoinResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &guildv1.ApproveJoinResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if err := s.uc.ApproveJoin(ctx, playerID, req.GetRequestId()); err != nil {
		return &guildv1.ApproveJoinResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.ApproveJoinResponse{Code: commonv1.ErrCode_OK}, nil
}

// RejectJoin 拒绝申请。审批人以 JWT ctx 为准(R5)。
func (s *GuildService) RejectJoin(ctx context.Context, req *guildv1.RejectJoinRequest) (*guildv1.RejectJoinResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &guildv1.RejectJoinResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if err := s.uc.RejectJoin(ctx, playerID, req.GetRequestId()); err != nil {
		return &guildv1.RejectJoinResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.RejectJoinResponse{Code: commonv1.ErrCode_OK}, nil
}

// LeaveGuild 退会。player_id 以 JWT ctx 为准(R5)。
func (s *GuildService) LeaveGuild(ctx context.Context, _ *guildv1.LeaveGuildRequest) (*guildv1.LeaveGuildResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &guildv1.LeaveGuildResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if err := s.uc.LeaveGuild(ctx, playerID); err != nil {
		return &guildv1.LeaveGuildResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.LeaveGuildResponse{Code: commonv1.ErrCode_OK}, nil
}

// KickMember 踢出成员。操作人以 JWT ctx 为准(R5)。
func (s *GuildService) KickMember(ctx context.Context, req *guildv1.KickMemberRequest) (*guildv1.KickMemberResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &guildv1.KickMemberResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTargetId() == 0 {
		return &guildv1.KickMemberResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.KickMember(ctx, playerID, req.GetTargetId()); err != nil {
		return &guildv1.KickMemberResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.KickMemberResponse{Code: commonv1.ErrCode_OK}, nil
}

// DisbandGuild 解散公会。player_id 以 JWT ctx 为准(R5)。
func (s *GuildService) DisbandGuild(ctx context.Context, _ *guildv1.DisbandGuildRequest) (*guildv1.DisbandGuildResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &guildv1.DisbandGuildResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if err := s.uc.DisbandGuild(ctx, playerID); err != nil {
		return &guildv1.DisbandGuildResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.DisbandGuildResponse{Code: commonv1.ErrCode_OK}, nil
}

// TransferLeader 转让会长。现任会长以 JWT ctx 为准(R5)。
func (s *GuildService) TransferLeader(ctx context.Context, req *guildv1.TransferLeaderRequest) (*guildv1.TransferLeaderResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &guildv1.TransferLeaderResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTargetId() == 0 {
		return &guildv1.TransferLeaderResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.TransferLeader(ctx, playerID, req.GetTargetId()); err != nil {
		return &guildv1.TransferLeaderResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.TransferLeaderResponse{Code: commonv1.ErrCode_OK}, nil
}

// SetOfficer 任命 / 撤销官员。会长以 JWT ctx 为准(R5)。
func (s *GuildService) SetOfficer(ctx context.Context, req *guildv1.SetOfficerRequest) (*guildv1.SetOfficerResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &guildv1.SetOfficerResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	if req.GetTargetId() == 0 {
		return &guildv1.SetOfficerResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.SetOfficer(ctx, playerID, req.GetTargetId(), req.GetIsOfficer()); err != nil {
		return &guildv1.SetOfficerResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.SetOfficerResponse{Code: commonv1.ErrCode_OK}, nil
}

// GetGuild 查公会(只读,任意人可查)。
func (s *GuildService) GetGuild(ctx context.Context, req *guildv1.GetGuildRequest) (*guildv1.GetGuildResponse, error) {
	if req.GetGuildId() == 0 {
		return &guildv1.GetGuildResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	g, err := s.uc.GetGuild(ctx, req.GetGuildId())
	if err != nil {
		return &guildv1.GetGuildResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.GetGuildResponse{Code: commonv1.ErrCode_OK, Guild: g}, nil
}

// GetMyGuild 查"我的公会"。player_id 以 JWT ctx 为准(R5);不在任何公会时 code=OK 且 guild 为空。
func (s *GuildService) GetMyGuild(ctx context.Context, _ *guildv1.GetMyGuildRequest) (*guildv1.GetMyGuildResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &guildv1.GetMyGuildResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	g, err := s.uc.GetMyGuild(ctx, playerID)
	if err != nil {
		return &guildv1.GetMyGuildResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.GetMyGuildResponse{Code: commonv1.ErrCode_OK, Guild: g}, nil
}

// GetPlayerGuild 按 player_id 反查公会编号(内部东西向,DS 出生编制专用)。
//
// 有了它,客户端就不必靠本机名册反推会友 —— 名册按游标分页,没翻到的那一页会把会友判成路人。
// 复用 GetMyGuild 的读路径(含 member→guild 反查缓存与权威回源),不另起一条缓存语义;
// 但只把编号放进响应,整份 Guild 快照不外发(§9.14 最小视图)。
func (s *GuildService) GetPlayerGuild(ctx context.Context, req *guildv1.GetPlayerGuildRequest) (*guildv1.GetPlayerGuildResponse, error) {
	if code := systemOnly(ctx); code != commonv1.ErrCode_OK {
		return &guildv1.GetPlayerGuildResponse{Code: code}, nil
	}
	playerID := req.GetPlayerId()
	if playerID == 0 {
		return &guildv1.GetPlayerGuildResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}

	g, err := s.uc.GetMyGuild(ctx, playerID)
	if err != nil {
		return &guildv1.GetPlayerGuildResponse{Code: toProtoCode(err)}, nil
	}
	if g == nil || g.GetGuildId() == 0 {
		// 不在任何公会是正常态,不是错误。
		return &guildv1.GetPlayerGuildResponse{Code: commonv1.ErrCode_OK, HasGuild: false}, nil
	}
	return &guildv1.GetPlayerGuildResponse{
		Code:     commonv1.ErrCode_OK,
		HasGuild: true,
		GuildId:  g.GetGuildId(),
	}, nil
}

// ListMembers 列公会成员(只读)。
func (s *GuildService) ListMembers(ctx context.Context, req *guildv1.ListMembersRequest) (*guildv1.ListMembersResponse, error) {
	if req.GetGuildId() == 0 {
		return &guildv1.ListMembersResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	members, next, err := s.uc.ListMembers(ctx, req.GetGuildId(), req.GetCursor(), int(req.GetLimit()))
	if err != nil {
		return &guildv1.ListMembersResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.ListMembersResponse{Code: commonv1.ErrCode_OK, Members: members, NextCursor: next}, nil
}

// ListJoinRequests 列挂起申请。请求人以 JWT ctx 为准(R5),须 LEADER / OFFICER。
func (s *GuildService) ListJoinRequests(ctx context.Context, req *guildv1.ListJoinRequestsRequest) (*guildv1.ListJoinRequestsResponse, error) {
	playerID := callerID(ctx)
	if playerID == 0 {
		return &guildv1.ListJoinRequestsResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	requests, next, err := s.uc.ListJoinRequests(ctx, playerID, req.GetCursor(), int(req.GetLimit()))
	if err != nil {
		return &guildv1.ListJoinRequestsResponse{Code: toProtoCode(err)}, nil
	}
	return &guildv1.ListJoinRequestsResponse{Code: commonv1.ErrCode_OK, Requests: requests, NextCursor: next}, nil
}

// ── 辅助 ──────────────────────────────────────────────────────────────────────

// callerID 从 ctx 取 JWT 注入的 player_id。
func callerID(ctx context.Context) uint64 {
	id, _ := ctx.Value(plog.CtxKeyPlayerID).(uint64)
	return id
}

// systemOnly 内部东西向接口鉴权:带玩家 JWT 的调用一律拒。
//
// Envoy 按 /pandora.guild.v1.GuildService/ **整前缀**路由到本服务,没有按方法的白名单,
// 因此"内部方法"在客户端面同样可达。少了这道门,GetPlayerGuild 就成了「查任意玩家属于
// 哪个公会」的 IDOR 口子。拒绝分支以 response Code + nil error 返回,统一 access log 会
// 记成 rpc_ok,不打日志则越权尝试零可见性(与 player 服务 logAuthzDeny 同口径)。
func systemOnly(ctx context.Context) commonv1.ErrCode {
	if id := callerID(ctx); id != 0 {
		plog.With(ctx).Warnw("msg", "guild_authz_denied",
			"reason", "system_rpc_by_client", "caller_id", id)
		return commonv1.ErrCode_ERR_PERMISSION_DENY
	}
	return commonv1.ErrCode_OK
}

// toProtoCode 把 pkg/errcode 1:1 映射成 proto enum(数值相同)。
func toProtoCode(err error) commonv1.ErrCode {
	return commonv1.ErrCode(errcode.As(err))
}
