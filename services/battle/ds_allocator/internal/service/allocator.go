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
	"time"

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

// 拒绝 / 降级 reason 枚举(infra.md §11.3 R2)。
//
// 为什么必须由业务代码自己打:本 service 的每个 RPC 都把错误折进响应 Code 后
// **返回 nil error**,access log 只看得到 rpc_ok;即使返 error,ErrInvalidArg /
// ErrUnauthorized / ErrInvalidState 与全部 fencing 码(>999)也都不属
// errcode.IsServerFault,线上默认 info 级下一条都不出。也就是说:不在这里打,
// "matchmaker 说分配失败 / DS 说心跳被拒"在后端日志里**完全没有痕迹**。
const (
	reasonMatchIDRequired        = "match_id_required"
	reasonPlayerIDRequired       = "player_id_required"
	reasonCombatFactionInvalid   = "combat_factions_invalid"
	reasonUsecaseFailed          = "usecase_failed"
	reasonReleaseReasonInvalid   = "release_reason_not_allowed"
	reasonReleaseAuthExpMissing  = "release_auth_exp_missing"
	reasonAbortRequestIncomplete = "abort_request_incomplete"
	reasonAbortVerifierUnset     = "abort_verifier_not_configured"
	reasonAbortAuthUnavailable   = "abort_auth_unavailable"
	reasonAbortAuthDenied        = "abort_auth_denied"
	reasonDSCredentialRejected   = "ds_credential_rejected"
	reasonDSCredentialIncomplete = "ds_credential_incomplete"
	reasonServiceDisabled        = "service_disabled"
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
		plog.With(ctx).Warnw("msg", "battle_allocate_rejected", "reason", reasonMatchIDRequired)
		return &dsv1.AllocateBattleResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// R3 join key:本面是后端内部面(matchmaker→allocator),ctx 里没有 player_id/match_id,
	// plog.With 自动带不上。解析成功即写进 ctx,让 biz/data 层同请求日志全部自动带 match_id。
	ctx = plog.WithMatchID(ctx, req.GetMatchId())
	combatFactionByPlayer, err := combatFactionMap(req.GetPlayerCombatFactions())
	if err != nil {
		plog.With(ctx).Warnw("msg", "battle_allocate_rejected", "reason", reasonCombatFactionInvalid,
			"factions", len(req.GetPlayerCombatFactions()), "err", err)
		return &dsv1.AllocateBattleResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// 阶段耗时(§11.3 判据 5「慢在哪」):AllocateBattle 是整条进入链上唯一会阻塞到
	// DS 冷启动的调用(Agones 分配 + ready 等待,生产档上界 120s)。boundary 是唯一
	// 能算出整段耗时的位置——biz 内部各段日志各自只覆盖一小截。
	startedAt := time.Now()
	// rating_mode 原样透传:allocator 不解释本局算不算段位,只把 matchmaker 定格的值
	// 存进 canonical BattleStorageRecord 供 battle_result 结算时取用(见 biz 注释)。
	res, err := s.uc.AllocateBattleWithCombatFactions(
		ctx, req.GetMatchId(), req.GetPlayerIds(), combatFactionByPlayer, req.GetMapId(),
		req.GetGameMode(), req.GetRatingMode(), req.GetRatingPool())
	if err != nil {
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "battle_allocate_rejected", "reason", reasonUsecaseFailed,
			"code", int32(code), "players", len(req.GetPlayerIds()), "map_id", req.GetMapId(),
			"game_mode", req.GetGameMode(), "elapsed_ms", time.Since(startedAt).Milliseconds(),
			"err", err,
			"hint", "玩家进不去副本:按 match_id 上溯同 trace_id 的 gameserver_allocate_failed /"+
				" battle_ready_wait_* / battle_abandoned_* 定位断点")
		return &dsv1.AllocateBattleResponse{Code: code}, nil
	}
	plog.With(ctx).Infow("msg", "battle_allocate_ok", "pod", res.DSPodName, "ds_addr", res.DSAddr,
		"uid", res.GameserverUID, "epoch", res.InstanceEpoch, "allocation_id", res.AllocationID,
		"release_track", res.ReleaseTrack, "players", len(req.GetPlayerIds()),
		"map_id", req.GetMapId(), "elapsed_ms", time.Since(startedAt).Milliseconds())
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
	// R2:一个 if 收敛了两个条件,必须拆成两个 reason —— 否则"重连重签被拒"查得到、
	// "缺的是 match_id 还是 player_id"查不到。
	if req.GetMatchId() == 0 {
		plog.With(ctx).Warnw("msg", "battle_target_resolve_rejected", "reason", reasonMatchIDRequired,
			"player_id", req.GetPlayerId())
		return &dsv1.ResolveBattleTargetResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if req.GetPlayerId() == 0 {
		plog.With(ctx).Warnw("msg", "battle_target_resolve_rejected", "reason", reasonPlayerIDRequired,
			"match_id", req.GetMatchId())
		return &dsv1.ResolveBattleTargetResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// R3 重连链 join key:本面由 login 代玩家调用,ctx 无玩家 JWT,player_id/match_id
	// 都必须手写进 ctx,否则整条重连日志定位不到人。
	ctx = plog.WithMatchID(plog.WithPlayerID(ctx, req.GetPlayerId()), req.GetMatchId())
	res, err := s.uc.ResolveBattleTarget(ctx, req.GetMatchId(), req.GetPlayerId())
	if err != nil {
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "battle_target_resolve_rejected", "reason", reasonUsecaseFailed,
			"code", int32(code), "err", err,
			"hint", "玩家重连拿不到 battle 目标,将退化为回大厅;判定依据见同 trace_id 的 battle_target_*")
		return &dsv1.ResolveBattleTargetResponse{Code: code}, nil
	}
	plog.With(ctx).Infow("msg", "battle_target_resolve_ok", "pod", res.DSPodName, "ds_addr", res.DSAddr,
		"uid", res.GameserverUID, "epoch", res.InstanceEpoch, "allocation_id", res.AllocationID,
		"release_track", res.ReleaseTrack)
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
		plog.With(ctx).Warnw("msg", "battle_release_rejected", "reason", reasonMatchIDRequired,
			"release_reason", req.GetReason())
		return &dsv1.ReleaseBattleResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	ctx = plog.WithMatchID(ctx, req.GetMatchId())
	var err error
	if s.uc.RedisAuthorityEnabled() {
		// R2:原来一个 if 收敛了两件完全不同的事(调用方用了不被允许的 reason /
		// 结算证明缺凭据过期时刻),拆成两个 reason —— 前者是调用方走错口,
		// 后者是 battle_result 的授权证明没带齐,排查方向不同。
		if req.GetReason() != "completed" && req.GetReason() != "completed-finalize" {
			plog.With(ctx).Warnw("msg", "battle_release_rejected", "reason", reasonReleaseReasonInvalid,
				"release_reason", req.GetReason(), "pod", req.GetDsPodName(),
				"hint", "Model B 正常结算只接受 completed / completed-finalize;abandoned 由内部 sweep 回收")
			return &dsv1.ReleaseBattleResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
		}
		if req.GetAuthExpMs() <= 0 {
			plog.With(ctx).Warnw("msg", "battle_release_rejected", "reason", reasonReleaseAuthExpMissing,
				"release_reason", req.GetReason(), "pod", req.GetDsPodName(),
				"uid", req.GetGameserverUid(), "epoch", req.GetInstanceEpoch())
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
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "battle_release_rejected", "reason", reasonUsecaseFailed,
			"code", int32(code), "release_reason", req.GetReason(), "pod", req.GetDsPodName(),
			"uid", req.GetGameserverUid(), "epoch", req.GetInstanceEpoch(),
			"allocation_id", req.GetAllocationId(), "err", err,
			"hint", "DS 未被回收:调用方会重试,持续失败则 pod 占位泄漏,查同 match_id 的 sweep 链")
		return &dsv1.ReleaseBattleResponse{Code: code}, nil
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
	if request.MatchID != 0 {
		ctx = plog.WithMatchID(ctx, request.MatchID)
	}
	if !request.Complete() {
		plog.With(ctx).Warnw("msg", "battle_allocation_abort_rejected", "reason", reasonAbortRequestIncomplete,
			"operation_id", request.OperationID, "pod", request.Target.PodName,
			"uid", request.Target.InstanceUID, "epoch", request.Target.InstanceEpoch,
			"allocation_id", request.Target.AllocationID, "release_track", request.Target.ReleaseTrack)
		return &dsv1.AbortPreactiveBattleResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if s.abortAuth == nil {
		plog.With(ctx).Errorw("msg", "battle_allocation_abort_rejected", "reason", reasonAbortVerifierUnset,
			"operation_id", request.OperationID,
			"hint", "matchmaker 的分配 saga 补偿无法执行,warming 实例只能等 sweep 兜底回收")
		return &dsv1.AbortPreactiveBattleResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}, nil
	}
	if err := s.abortAuth.VerifyWithPayload(ctx,
		dsv1.DSAllocatorService_AbortPreactiveBattle_FullMethodName,
		request.MatchID, request.Canonical()); err != nil {
		// R2:验签失败的两种结局(依赖不可用 / 签名不认)错误码不同、处置也不同,分两个 reason。
		if errors.Is(err, internalrpcauth.ErrUnavailable) {
			plog.With(ctx).Warnw("msg", "battle_allocation_abort_rejected", "reason", reasonAbortAuthUnavailable,
				"operation_id", request.OperationID, "err", err)
			return &dsv1.AbortPreactiveBattleResponse{Code: commonv1.ErrCode_ERR_UNAVAILABLE}, nil
		}
		plog.With(ctx).Warnw("msg", "battle_allocation_abort_rejected", "reason", reasonAbortAuthDenied,
			"operation_id", request.OperationID, "pod", request.Target.PodName, "err", err)
		return &dsv1.AbortPreactiveBattleResponse{Code: commonv1.ErrCode_ERR_PERMISSION_DENY}, nil
	}
	if err := s.uc.AbortPreactiveBattle(ctx, request); err != nil {
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "battle_allocation_abort_rejected", "reason", reasonUsecaseFailed,
			"code", int32(code), "operation_id", request.OperationID, "pod", request.Target.PodName,
			"allocation_id", request.Target.AllocationID, "err", err)
		return &dsv1.AbortPreactiveBattleResponse{Code: code}, nil
	}
	return &dsv1.AbortPreactiveBattleResponse{Code: commonv1.ErrCode_OK}, nil
}

// EnsurePlayerDeparture：placement 路由体系已删除（硬切），旧调用方一律拒绝。
func (s *AllocatorService) EnsurePlayerDeparture(
	ctx context.Context,
	req *dsv1.EnsurePlayerDepartureRequest,
) (*dsv1.EnsurePlayerDepartureResponse, error) {
	// 已删除的 RPC 仍被调用 = 有一个没跟上硬切的旧调用方。不打日志的话,调用方那边
	// 只看到一个业务码,而这一侧完全无痕,没人知道"谁还在调"。
	plog.With(ctx).Warnw("msg", "battle_departure_rpc_rejected", "reason", reasonServiceDisabled,
		"match_id", req.GetMatchId(), "player_id", req.GetPlayerId(), "pod", req.GetDsPodName(),
		"hint", "placement 路由体系已硬切删除;调用方需改走 Heartbeat 携带 census 的离场闭环")
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
		plog.With(ctx).Warnw("msg", "battle_heartbeat_rejected", "reason", reasonMatchIDRequired,
			"pod", req.GetDsPodName())
		return &dsv1.HeartbeatResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// R3:DS 回调面走 AuthOptional 且 DS 不带 x-pandora-player-id,ctx 里什么 join key
	// 都没有。写进 match_id 后,整条心跳链(含 data 层 fencing 拒绝)才串得起来。
	ctx = plog.WithMatchID(ctx, req.GetMatchId())
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
			// DS 凭据被拒时后端此前完全无痕(fencing 码 >999 与 ErrUnauthorized 在
			// access log 里都只落 rpc_ok=DEBUG),排查只能靠 DS 侧日志。
			// 这一条是"DS 明明在跑却像没心跳"类故障的唯一后端证据。
			code := toProtoCode(checkErr)
			plog.With(ctx).Warnw("msg", "battle_heartbeat_rejected", "reason", reasonDSCredentialRejected,
				"code", int32(code), "pod", req.GetDsPodName(), "state", req.GetState(),
				"player_count", req.GetPlayerCount(), "err", checkErr,
				"hint", "该 DS 的心跳不会推进任何权威状态,15s 后会被 sweep 判弃")
			return &dsv1.HeartbeatResponse{Code: code}, nil
		}
		if verified == nil || verified.ExpMs <= 0 {
			plog.With(ctx).Warnw("msg", "battle_heartbeat_rejected", "reason", reasonDSCredentialIncomplete,
				"pod", req.GetDsPodName(), "verified", verified != nil)
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
			code := toProtoCode(checkErr)
			plog.With(ctx).Warnw("msg", "battle_heartbeat_rejected", "reason", reasonDSCredentialRejected,
				"code", int32(code), "pod", req.GetDsPodName(), "authority", "legacy", "err", checkErr)
			return &dsv1.HeartbeatResponse{Code: code}, nil
		}
		// census 一并透传:legacy 面据此续 owner 实例租约并代提交在场玩家 Admit
		// (不传则 owner 恒 PENDING、租约恒过期,客户端永远等不到 STABLE,2026-08-04)。
		res, err = s.uc.HeartbeatWithCensus(ctx, req.GetMatchId(), req.GetDsPodName(),
			req.GetPlayerCount(), reportedState, req.GetTsMs(),
			req.GetActivePlayerSnapshotPresent(), req.GetActivePlayerIds())
	}
	if err != nil {
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "battle_heartbeat_rejected", "reason", reasonUsecaseFailed,
			"code", int32(code), "pod", req.GetDsPodName(), "state", reportedState,
			"player_count", req.GetPlayerCount(), "census_present", req.GetActivePlayerSnapshotPresent(),
			"census", len(req.GetActivePlayerIds()), "err", err)
		return &dsv1.HeartbeatResponse{Code: code}, nil
	}
	// R4:心跳是高频路径,成功侧一律 Debugw(LOG_LEVEL=debug 可对单 pod 临时全开)。
	// 有它才能回答"这台 DS 到底有没有在上报、上报的在场人数是多少"。
	plog.With(ctx).Debugw("msg", "battle_heartbeat_accepted", "pod", req.GetDsPodName(),
		"state", reportedState, "player_count", req.GetPlayerCount(),
		"census_present", req.GetActivePlayerSnapshotPresent(), "census", len(req.GetActivePlayerIds()),
		"command", res.Command, "eviction_orders", len(res.EvictionOrders))
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
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "battle_list_rejected", "reason", reasonUsecaseFailed,
			"code", int32(code), "state_filter", req.GetStateFilter(), "err", err)
		return &dsv1.ListBattlesResponse{Code: code}, nil
	}
	return &dsv1.ListBattlesResponse{Code: commonv1.ErrCode_OK, Battles: battles}, nil
}

// ── 辅助 ──────────────────────────────────────────────────────────────────────

// toProtoCode 把 pkg/errcode 1:1 映射成 proto enum(数值相同)。
func toProtoCode(err error) commonv1.ErrCode {
	return commonv1.ErrCode(errcode.As(err))
}
