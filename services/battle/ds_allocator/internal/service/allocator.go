// Package service 是 ds_allocator 服务的 gRPC service 层(W4 ②,2026-06-06)。
//
// 职责:
//   - 实现 dsv1.DSAllocatorServiceServer 接口
//   - proto Request/Response ↔ biz 入参/出参互转
//   - errcode.Code → commonv1.ErrCode 1:1 映射
//
// 说明:本服务调用方是后端内部(matchmaker 调 AllocateBattle/ReleaseBattle、
// 战斗 DS 调 Heartbeat),不是玩家客户端,因此不从 ctx 取 player_id。
package service

import (
	"context"
	"errors"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/battleabort"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/placement"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/biz"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// AllocatorService 实现 dsv1.DSAllocatorServiceServer。
type AllocatorService struct {
	dsv1.UnimplementedDSAllocatorServiceServer
	uc        *biz.AllocatorUsecase
	dsGuard   *middleware.DSCallbackGuard // DS 回调令牌守卫(审核 P1 #1);nil 等价 off
	abortAuth *internalrpcauth.Verifier   // Matchmaker-only, full-payload-bound destructive RPC
}

// NewAllocatorService 构造 AllocatorService。
func NewAllocatorService(uc *biz.AllocatorUsecase) *AllocatorService {
	return &AllocatorService{uc: uc}
}

// SetDSCallbackGuard 注入 DS 回调令牌守卫(可选依赖,main 在 ds_auth 已配时调用)。
func (s *AllocatorService) SetDSCallbackGuard(g *middleware.DSCallbackGuard) { s.dsGuard = g }

// SetAllocationAbortVerifier injects the independent Matchmaker→allocator
// service-auth verifier. It is deliberately unrelated to player JWT,
// placement proofs, Login resume auth, and DS callback credentials.
func (s *AllocatorService) SetAllocationAbortVerifier(v *internalrpcauth.Verifier) {
	s.abortAuth = v
}

// AllocateBattle 为 match 申请战斗 DS(matchmaker 全员确认后调)。
func (s *AllocatorService) AllocateBattle(ctx context.Context, req *dsv1.AllocateBattleRequest) (*dsv1.AllocateBattleResponse, error) {
	if req.GetMatchId() == 0 {
		return &dsv1.AllocateBattleResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	combatFactionByPlayer, err := combatFactionMap(req.GetPlayerCombatFactions())
	if err != nil {
		return &dsv1.AllocateBattleResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// rating_mode 原样透传:allocator 不解释本局算不算段位,只把 matchmaker 定格的值
	// 存进 canonical BattleStorageRecord 供 battle_result 结算时取用(见 biz 注释)。
	res, err := s.uc.AllocateBattleWithCombatFactions(
		ctx, req.GetMatchId(), req.GetPlayerIds(), combatFactionByPlayer, req.GetMapId(),
		req.GetGameMode(), req.GetRatingMode())
	if err != nil {
		return &dsv1.AllocateBattleResponse{Code: toProtoCode(err)}, nil
	}
	return &dsv1.AllocateBattleResponse{
		Code:          commonv1.ErrCode_OK,
		DsAddr:        res.DSAddr,
		DsPodName:     res.DSPodName,
		AllocatedAtMs: res.AllocatedAtMs,
		// DSTicket v2 实例绑定(方案 B):与 ds_addr 同源同快照,matchmaker 签票用。
		GameserverUid: res.GameserverUID,
		InstanceEpoch: res.InstanceEpoch,
		AllocationId:  res.AllocationID,
		ReleaseTrack:  res.ReleaseTrack,
	}, nil
}

func combatFactionMap(records []*dsv1.BattlePlayerCombatFaction) (map[uint64]uint32, error) {
	if len(records) == 0 {
		return nil, nil
	}
	factions := make(map[uint64]uint32, len(records))
	for _, record := range records {
		if record == nil || record.GetPlayerId() == 0 {
			return nil, errors.New("player combat faction requires player_id")
		}
		if _, duplicate := factions[record.GetPlayerId()]; duplicate {
			return nil, errors.New("duplicate player combat faction")
		}
		factions[record.GetPlayerId()] = record.GetCombatFactionId()
	}
	return factions, nil
}

// ResolveBattleTarget 只读返回当前可重连目标并核验 roster 成员。它与 AllocateBattle
// 分离，确保重签票据永远不会产生 GameServerAllocation 副作用。
func (s *AllocatorService) ResolveBattleTarget(
	ctx context.Context,
	req *dsv1.ResolveBattleTargetRequest,
) (*dsv1.ResolveBattleTargetResponse, error) {
	if req.GetMatchId() == 0 || req.GetPlayerId() == 0 {
		return &dsv1.ResolveBattleTargetResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	res, err := s.uc.ResolveBattleTarget(ctx, req.GetMatchId(), req.GetPlayerId())
	if err != nil {
		return &dsv1.ResolveBattleTargetResponse{Code: toProtoCode(err)}, nil
	}
	return &dsv1.ResolveBattleTargetResponse{
		Code: commonv1.ErrCode_OK, DsAddr: res.DSAddr, DsPodName: res.DSPodName,
		AllocatedAtMs: res.AllocatedAtMs, GameserverUid: res.GameserverUID,
		InstanceEpoch: res.InstanceEpoch, AllocationId: res.AllocationID,
		ReleaseTrack: res.ReleaseTrack,
	}, nil
}

// ReleaseBattle 回收战斗 DS(对局结束/异常)。
func (s *AllocatorService) ReleaseBattle(ctx context.Context, req *dsv1.ReleaseBattleRequest) (*dsv1.ReleaseBattleResponse, error) {
	if req.GetMatchId() == 0 {
		return &dsv1.ReleaseBattleResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	var err error
	if s.uc.RedisAuthorityEnabled() {
		if (req.GetReason() != "completed" && req.GetReason() != "completed-finalize") || req.GetAuthExpMs() <= 0 {
			return &dsv1.ReleaseBattleResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
		}
		expected := data.BattleExpectedInstance{
			AllocationID: req.GetAllocationId(), InstanceUID: req.GetGameserverUid(),
			InstanceEpoch: req.GetInstanceEpoch(),
		}
		proof := data.BattleResultAuthorizationProof{
			Credential: data.BattleCredentialIdentity{
				PodName: req.GetDsPodName(), InstanceUID: req.GetGameserverUid(),
				InstanceEpoch: req.GetInstanceEpoch(), Gen: req.GetAuthGen(),
				JTI: req.GetAuthJti(), ExpMs: uint64(req.GetAuthExpMs()), Kid: req.GetAuthKid(),
				TokenSHA256: req.GetAuthTokenSha256(), WriterEpoch: req.GetAuthWriterEpoch(),
			},
			AuthorizedAtMs: req.GetAuthorizedAtMs(),
		}
		if req.GetReason() == "completed" {
			err = s.uc.ReleaseBattleExpected(
				ctx, req.GetMatchId(), req.GetReason(), req.GetDsPodName(), expected, proof)
		} else {
			err = s.uc.FinalizeBattleReleaseExpected(
				ctx, req.GetMatchId(), req.GetDsPodName(), expected, proof)
		}
	} else {
		err = s.uc.ReleaseBattle(ctx, req.GetMatchId(), req.GetReason())
	}
	if err != nil {
		return &dsv1.ReleaseBattleResponse{Code: toProtoCode(err)}, nil
	}
	return &dsv1.ReleaseBattleResponse{Code: commonv1.ErrCode_OK}, nil
}

// AbortPreactiveBattle is the signed allocation-saga compensation endpoint.
// Authentication binds the canonical full body and consumes a shared Redis
// nonce before the usecase may fence Redis or touch Kubernetes.
func (s *AllocatorService) AbortPreactiveBattle(
	ctx context.Context,
	req *dsv1.AbortPreactiveBattleRequest,
) (*dsv1.AbortPreactiveBattleResponse, error) {
	request := battleabort.Request{
		MatchID: req.GetMatchId(), OperationID: req.GetAllocationOperationId(),
		Target: placement.Target{
			PodName: req.GetDsPodName(), InstanceUID: req.GetGameserverUid(),
			InstanceEpoch: req.GetInstanceEpoch(), AllocationID: req.GetAllocationId(),
			ReleaseTrack: req.GetReleaseTrack(),
		},
	}
	if !request.Complete() {
		return &dsv1.AbortPreactiveBattleResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if s.abortAuth == nil {
		return &dsv1.AbortPreactiveBattleResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}, nil
	}
	if err := s.abortAuth.VerifyWithPayload(ctx,
		dsv1.DSAllocatorService_AbortPreactiveBattle_FullMethodName,
		request.MatchID, request.Canonical()); err != nil {
		if errors.Is(err, internalrpcauth.ErrUnavailable) {
			return &dsv1.AbortPreactiveBattleResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}, nil
		}
		return &dsv1.AbortPreactiveBattleResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	if err := s.uc.AbortPreactiveBattle(ctx, request); err != nil {
		return &dsv1.AbortPreactiveBattleResponse{Code: toProtoCode(err)}, nil
	}
	return &dsv1.AbortPreactiveBattleResponse{Code: commonv1.ErrCode_OK}, nil
}

// EnsurePlayerDeparture：placement 路由体系已删除（硬切），旧调用方一律拒绝。
func (s *AllocatorService) EnsurePlayerDeparture(
	ctx context.Context,
	req *dsv1.EnsurePlayerDepartureRequest,
) (*dsv1.EnsurePlayerDepartureResponse, error) {
	return &dsv1.EnsurePlayerDepartureResponse{
		Code: commonv1.ErrCode(errcode.ErrServiceDisabled),
	}, nil
}

// Heartbeat 处理战斗 DS 心跳上报(DS 每 5s 调)。
// dsReportableStates 是 DS 心跳允许自报的生命周期状态白名单(§9.6:DS 写权限有范围)。
// abandoned 是**后端专属判决**(空场超时 / 补偿),绝不接受 DS 自报——否则被攻破 / 出 bug
// 的 DS 可直接把对局推成 abandoned,进而触发 no-show 记罚给玩家铸造处罚(anti-abuse §6 第 8 项),
// 越过后端自己的 150s 超时证据。内部 allocation_* fence 态同理只属后端。
var dsReportableStates = map[string]struct{}{
	"warming": {}, "ready": {}, "running": {}, "ended": {},
}

// sanitizeReportedState 把 DS 上报的 state 收敛到白名单;非白名单值(含 abandoned / 乱码)
// 归一化为空串 = 「本跳不更新 state」,两条心跳路径都已按 `if state != ""` 处理。
// 于是 abandoned 的唯一来源仍是后端超时 switch,与 legacy 路径语义对齐(不再两轨漂移)。
func sanitizeReportedState(ctx context.Context, state string) string {
	if state == "" {
		return ""
	}
	if _, ok := dsReportableStates[state]; ok {
		return state
	}
	plog.With(ctx).Warnw("msg", "ds_reported_state_rejected", "state", state,
		"hint", "abandoned/internal states are backend-only (§9.6); ignoring DS self-report")
	return ""
}

func (s *AllocatorService) Heartbeat(ctx context.Context, req *dsv1.HeartbeatRequest) (*dsv1.HeartbeatResponse, error) {
	if req.GetMatchId() == 0 {
		return &dsv1.HeartbeatResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	reportedState := sanitizeReportedState(ctx, req.GetState())
	var res *biz.HeartbeatResult
	var err error
	if s.uc.RedisAuthorityEnabled() {
		// Model B 必须同时绑定 match+pod，并要求完整 JWT credential；legacy/nil credential
		// 不允许在 permissive 语义下回退，任何 Redis 副作用前即拒绝。
		_, verified, checkErr := s.dsGuard.CheckBattleCredential(ctx, middleware.DSScope{
			Type: auth.DSTypeBattle, MatchID: req.GetMatchId(), Pod: req.GetDsPodName(), RequireToken: true,
		})
		if checkErr != nil {
			return &dsv1.HeartbeatResponse{Code: toProtoCode(checkErr)}, nil
		}
		if verified == nil || verified.ExpMs <= 0 {
			return &dsv1.HeartbeatResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
		}
		res, err = s.uc.HeartbeatAuthorizedWithPlayers(ctx, req.GetMatchId(), data.BattleCredentialIdentity{
			PodName:       verified.Pod,
			InstanceUID:   verified.InstanceUID,
			InstanceEpoch: verified.ProtocolEpoch,
			Gen:           verified.Gen,
			JTI:           verified.JTI,
			ExpMs:         uint64(verified.ExpMs),
			Kid:           verified.Kid,
			TokenSHA256:   verified.TokenSHA256,
			WriterEpoch:   verified.WriterEpoch,
		}, req.GetPlayerCount(), reportedState, req.GetTsMs(),
			req.GetActivePlayerSnapshotPresent(), req.GetPlayerCensusCapabilityVersion(),
			req.GetPlayerCensusId(), req.GetActivePlayerIds(), req.GetAcknowledgedDepartureIds())
	} else {
		// Legacy/off 灰度路径保持既有范围语义；Model B 开启后不会落到这里。
		if checkErr := s.dsGuard.Check(ctx, middleware.DSScope{
			Type: auth.DSTypeBattle, MatchID: req.GetMatchId(), RequireToken: true,
		}); checkErr != nil {
			return &dsv1.HeartbeatResponse{Code: toProtoCode(checkErr)}, nil
		}
		// census 一并透传:legacy 面据此续 owner 实例租约并代提交在场玩家 Admit
		// (不传则 owner 恒 PENDING、租约恒过期,客户端永远等不到 STABLE,2026-08-04)。
		res, err = s.uc.HeartbeatWithCensus(ctx, req.GetMatchId(), req.GetDsPodName(),
			req.GetPlayerCount(), reportedState, req.GetTsMs(),
			req.GetActivePlayerSnapshotPresent(), req.GetActivePlayerIds())
	}
	if err != nil {
		return &dsv1.HeartbeatResponse{Code: toProtoCode(err)}, nil
	}
	return &dsv1.HeartbeatResponse{
		Code:                  commonv1.ErrCode_OK,
		Command:               res.Command,
		AcceptedTokenGen:      res.AcceptedTokenGen,
		AcceptedTokenJti:      res.AcceptedTokenJTI,
		AcceptedInstanceUid:   res.AcceptedInstanceUID,
		AcceptedInstanceEpoch: res.AcceptedInstanceEpoch,
		AcceptedWriterEpoch:   res.AcceptedWriterEpoch,
		EvictionOrders:        res.EvictionOrders,
	}, nil
}

// ListBattles 列出当前战斗实例(运维/调试)。
func (s *AllocatorService) ListBattles(ctx context.Context, req *dsv1.ListBattlesRequest) (*dsv1.ListBattlesResponse, error) {
	battles, err := s.uc.ListBattles(ctx, req.GetStateFilter())
	if err != nil {
		return &dsv1.ListBattlesResponse{Code: toProtoCode(err)}, nil
	}
	return &dsv1.ListBattlesResponse{Code: commonv1.ErrCode_OK, Battles: battles}, nil
}

// ── 辅助 ──────────────────────────────────────────────────────────────────────

// toProtoCode 把 pkg/errcode 1:1 映射成 proto enum(数值相同)。
func toProtoCode(err error) commonv1.ErrCode {
	return commonv1.ErrCode(errcode.As(err))
}
