// Package service 是 hub_allocator 服务的 gRPC service 层(W4 ⑤,2026-06-06)。
//
// 职责:
//   - 实现 hubv1.HubAllocatorServiceServer 接口
//   - proto Request/Response ↔ biz 入参/出参互转
//   - errcode.Code → commonv1.ErrCode 1:1 映射
//
// 说明:调用方是后端内部(login 调 AssignHub、Hub DS 调 Heartbeat),不是玩家客户端,
// 因此不从 ctx 取 player_id;player_id 由 login 等上游服务在请求里显式传入。
//
// 例外:ListHubLines / TransferToLine 是玩家侧 RPC(经 Envoy :8443 客户端面,
// jwt_authn 注入 x-pandora-player-id),player_id 一律从 ctx 取(JWT sub 权威),不信请求体。
package service

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"

	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/biz"
)

// HubService 实现 hubv1.HubAllocatorServiceServer。
type HubService struct {
	hubv1.UnimplementedHubAllocatorServiceServer
	uc      *biz.HubUsecase
	dsGuard *pmw.DSCallbackGuard // DS 回调令牌守卫(审核 P1 #1);nil 等价 off
	// modelBAuthority:Model B「Redis 唯一授权权威」总开关(main 在 ds_auth.authority_mode=redis
	// +agones+enforce 时置 true)。置 true 后心跳**必须**携带 Model B 凭据(cred!=nil);仅带 legacy
	// 令牌(ds_gen 但无 uid/epoch/jti)→ 直接拒 ErrUnauthorized,不给旧令牌借心跳保活/翻 ready
	// (审核二轮 CE1/CE2:彻底删除 Redis 授权下的 legacy 心跳回退分支)。
	modelBAuthority bool
	// localAdmission:mode=local(local-off-v1)专用准入通道。仅 main 在 case conf.ModeLocal
	// 且 local-off-v1 profile 校验通过后置 true;与 modelBAuthority 互斥(agones 恒 false)。
	// 置 true 后 AcknowledgeAdmission 走 uc.AcknowledgeLocalAdmission(归属记录复核 + 与
	// Model B 同一个 owner Admit 完成点),否则本机 DS 会在 Admission ACK 上被拒并踢玩家;
	// AcknowledgeDeparture 同样改走 uc.AcknowledgeLocalDeparture —— 进场与离场必须成对,
	// 只补一半的话离场 ACK 恒 INVALID_ARG,DS 的 Departure 队列每秒重试且永不出队。
	localAdmission bool
}

// NewHubService 构造 HubService。
func NewHubService(uc *biz.HubUsecase) *HubService {
	return &HubService{uc: uc}
}

// SetDSCallbackGuard 注入 DS 回调令牌守卫(可选依赖,main 在 ds_auth 已配时调用)。
func (s *HubService) SetDSCallbackGuard(g *pmw.DSCallbackGuard) { s.dsGuard = g }

// SetModelBAuthority 开启 Model B「Redis 唯一授权权威」(见字段注释;仅 authority_mode=redis 时置 true)。
func (s *HubService) SetModelBAuthority(b bool) { s.modelBAuthority = b }

// SetLocalAdmission 开启 mode=local 专用准入通道(见字段注释;仅 conf.ModeLocal 时置 true)。
func (s *HubService) SetLocalAdmission(b bool) { s.localAdmission = b }

// AssignHub 为玩家分配大厅 DS 分片(login 登录成功后调)。
func (s *HubService) AssignHub(ctx context.Context, req *hubv1.AssignHubRequest) (*hubv1.AssignHubResponse, error) {
	startedAt := time.Now()
	if req.GetPlayerId() == 0 {
		plog.With(ctx).Warnw("msg", "hub_assign_rejected",
			"player_id", req.GetPlayerId(), "region", req.GetRegion(),
			"reason", reasonMissingPlayerID, "code", int32(commonv1.ErrCode_ERR_INVALID_ARG))
		return &hubv1.AssignHubResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// R3 join key:team_id / source_match_id 在这里解析成功,写进 ctx,让本请求后续
	// 所有 plog.With(ctx) 自动带上(plog.WithTeamID / WithMatchID)。player_id 走
	// AuthOptional 面不会自动带,一律逐条手写。
	if teamID := req.GetTeamId(); teamID != 0 {
		ctx = plog.WithTeamID(ctx, teamID)
	}
	if sourceMatchID := req.GetSourceMatchId(); sourceMatchID != 0 {
		ctx = plog.WithMatchID(ctx, sourceMatchID)
	}
	// source_match_id:login 三态门证明原对局终局后透传的 Battle→Hub 回流 fence,
	// 盖进 hub 票据 claim(内部控制面调用,信任链与 role_id 同源)。
	// session_jti(R6 复审 P0-3):login 透传的请求方会话 jti,盖进本次 hub 票据 sjti claim。
	res, err := s.uc.AssignHub(ctx, req.GetPlayerId(), req.GetRegion(), req.GetTeamId(), req.GetRoleId(), req.GetSourceMatchId(), req.GetSessionJti())
	if err != nil {
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "hub_assign_failed",
			"player_id", req.GetPlayerId(), "region", req.GetRegion(), "role_id", req.GetRoleId(),
			"has_session_jti", req.GetSessionJti() != "",
			"reason", bizRejectReason(err), "code", int32(code),
			"elapsed_ms", time.Since(startedAt).Milliseconds(), "err", err)
		return &hubv1.AssignHubResponse{Code: code}, nil
	}
	// R1 阶段推进:AssignHub 返回 = 玩家拿到了进 Hub 的权威路由 + 票据。biz 侧
	// hub_assigned 只覆盖"新占座"分支,幂等重签(已有归属)分支不打——这里补一条
	// 覆盖全部成功出口,并带上跨阶段耗时(§11.3 验收判据 5「慢在哪」)。
	plog.With(ctx).Infow("msg", "hub_assign_ok",
		"player_id", req.GetPlayerId(), "region", req.GetRegion(), "role_id", req.GetRoleId(),
		"ds_pod", res.HubPodName, "shard_id", res.ShardID, "has_ticket", res.HubTicket != "",
		"elapsed_ms", time.Since(startedAt).Milliseconds())
	return &hubv1.AssignHubResponse{
		Code:       commonv1.ErrCode_OK,
		HubDsAddr:  res.HubDSAddr,
		HubTicket:  res.HubTicket,
		HubPodName: res.HubPodName,
		ShardId:    res.ShardID,
	}, nil
}

// ReleaseHub 玩家离开大厅(登出/进战斗)。
func (s *HubService) ReleaseHub(ctx context.Context, req *hubv1.ReleaseHubRequest) (*hubv1.ReleaseHubResponse, error) {
	if req.GetPlayerId() == 0 {
		plog.With(ctx).Warnw("msg", "hub_release_rejected",
			"player_id", req.GetPlayerId(),
			"reason", reasonMissingPlayerID, "code", int32(commonv1.ErrCode_ERR_INVALID_ARG))
		return &hubv1.ReleaseHubResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	if err := s.uc.ReleaseHub(ctx, req.GetPlayerId()); err != nil {
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "hub_release_failed",
			"player_id", req.GetPlayerId(),
			"reason", bizRejectReason(err), "code", int32(code), "err", err)
		return &hubv1.ReleaseHubResponse{Code: code}, nil
	}
	return &hubv1.ReleaseHubResponse{Code: commonv1.ErrCode_OK}, nil
}

// EnsureHubDepartureForBattle：placement 路由体系已删除（硬切），旧调用方一律拒绝。
func (s *HubService) EnsureHubDepartureForBattle(ctx context.Context,
	req *hubv1.EnsureHubDepartureForBattleRequest,
) (*hubv1.EnsureHubDepartureForBattleResponse, error) {
	// 恒拒的死接口:ErrServiceDisabled 不是 IsServerFault,access log 落 DEBUG。
	// 若还有旧调用方在打它(混版部署 / 未升级的 battle 侧),线上必须看得见。
	plog.With(ctx).Warnw("msg", "hub_departure_for_battle_rejected",
		"player_id", req.GetPlayerId(),
		"reason", reasonServiceDisabled, "code", int32(errcode.ErrServiceDisabled))
	return &hubv1.EnsureHubDepartureForBattleResponse{
		Code: commonv1.ErrCode(errcode.ErrServiceDisabled),
	}, nil
}

// TransferHub 跨分片传送(玩家点传送点)。
func (s *HubService) TransferHub(ctx context.Context, req *hubv1.TransferHubRequest) (*hubv1.TransferHubResponse, error) {
	if req.GetPlayerId() == 0 {
		plog.With(ctx).Warnw("msg", "hub_transfer_rejected",
			"player_id", req.GetPlayerId(), "target_hub_id", req.GetTargetHubId(),
			"reason", reasonMissingPlayerID, "code", int32(commonv1.ErrCode_ERR_INVALID_ARG))
		return &hubv1.TransferHubResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	res, err := s.uc.TransferHub(ctx, req.GetPlayerId(), req.GetTargetHubId())
	if err != nil {
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "hub_transfer_failed",
			"player_id", req.GetPlayerId(), "target_hub_id", req.GetTargetHubId(),
			"reason", bizRejectReason(err), "code", int32(code), "err", err)
		return &hubv1.TransferHubResponse{Code: code}, nil
	}
	return &hubv1.TransferHubResponse{
		Code:         commonv1.ErrCode_OK,
		NewHubDsAddr: res.NewHubDSAddr,
		NewHubTicket: res.NewHubTicket,
	}, nil
}

// ListHubs 列出分片负载(运维/调试)。
func (s *HubService) ListHubs(ctx context.Context, req *hubv1.ListHubsRequest) (*hubv1.ListHubsResponse, error) {
	hubs, err := s.uc.ListHubs(ctx, req.GetRegion())
	if err != nil {
		code := toProtoCode(err)
		// R4:成功侧不打(GM/运维轮询),失败侧必须打——它与 AssignHub 共用 ListShards,
		// 这里失败往往就是 "没有可用 hub" 的同源证据。
		plog.With(ctx).Warnw("msg", "hub_list_failed",
			"region", req.GetRegion(),
			"reason", bizRejectReason(err), "code", int32(code), "err", err)
		return &hubv1.ListHubsResponse{Code: code}, nil
	}
	plog.With(ctx).Debugw("msg", "hub_list_ok", "region", req.GetRegion(), "hubs", len(hubs))
	return &hubv1.ListHubsResponse{Code: commonv1.ErrCode_OK, Hubs: hubs}, nil
}

// Heartbeat 处理大厅 DS 心跳上报(Hub DS 每 5s 调)。
func (s *HubService) Heartbeat(ctx context.Context, req *hubv1.HeartbeatRequest) (*hubv1.HeartbeatResponse, error) {
	if req.GetHubPodName() == "" {
		// R4:心跳是高频路径,成功侧一条都不打;失败侧必须打 —— 一台 DS 报不上心跳就永远
		// 出不了 warming、不可被 AssignHub 选中,玩家侧只表现为 ErrHubNoAvailable。
		plog.With(ctx).Warnw("msg", "hub_heartbeat_rejected",
			"pod", req.GetHubPodName(), "player_count", req.GetPlayerCount(), "state", req.GetState(),
			"reason", reasonMissingPod, "code", int32(commonv1.ErrCode_ERR_INVALID_ARG))
		return &hubv1.HeartbeatResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	// DS 回调令牌校验:hub 令牌的 sub(pod)必须等于上报的 hub_pod_name
	// (防拿 A 分片令牌冒充 B 分片心跳/伪造在场玩家列表)。RequireToken:纯 DS 回调,
	// 无合法东西向无令牌调用者,enforce 下无令牌直连一律拒(堵旁路,审核 P1)。
	// CheckHubCredential:enforce 下验签并抽出凭据。Model B 令牌(带 ds_uid/ds_epoch/ds_gen/jti)
	// → 返回非空 cred,走 HeartbeatWithCredential 的 promote 线性化点(§7);legacy 令牌(仅 ds_gen)
	// → cred=nil,走原代际门路径,取 claims.Gen() 透传。off/permissive 下 claims/cred 均 nil → tokenGen=0。
	claims, cred, err := s.dsGuard.CheckHubCredential(ctx, pmw.DSScope{Type: auth.DSTypeHub, Pod: req.GetHubPodName(), RequireToken: true})
	if err != nil {
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "hub_heartbeat_rejected",
			"pod", req.GetHubPodName(), "state", req.GetState(),
			"reason", reasonCredentialCheckFailed, "code", int32(code), "err", err)
		return &hubv1.HeartbeatResponse{Code: code}, nil
	}
	var res *biz.HeartbeatResult
	if cred != nil {
		// Model B 权威模式:走 ActivateHeartbeat 单事务线性化点(stale fail-closed)。
		res, err = s.uc.HeartbeatWithCredential(ctx, req.GetHubPodName(), req.GetPlayerCount(), req.GetPlayerIds(), req.GetMaxPlayers(), req.GetState(), req.GetTsMs(), &biz.HubCredential{
			InstanceUID:   cred.InstanceUID,
			ProtocolEpoch: cred.ProtocolEpoch,
			Gen:           cred.Gen,
			JTI:           cred.JTI,
			TokenSHA256:   cred.TokenSHA256,
			Kid:           cred.Kid,
			WriterEpoch:   cred.WriterEpoch,
		})
	} else if s.modelBAuthority {
		// Model B 权威下**删除 legacy 回退**(审核二轮 CE1/CE2):仅带 legacy 令牌(无 Model B 凭据)
		// 的心跳一律拒,不给旧令牌借心跳保活/翻 ready。off/permissive 不会进此分支(那时不是 Model B)。
		plog.With(ctx).Warnw("msg", "hub_heartbeat_rejected",
			"pod", req.GetHubPodName(), "state", req.GetState(),
			"token_gen", claimsGen(claims),
			"reason", reasonLegacyCredentialUnderModelB,
			"code", int32(commonv1.ErrCode_ERR_UNAUTHORIZED),
			"hint", "DS 只带 legacy 令牌(无 uid/epoch/jti);该 pod 永远翻不到 ready,需重发凭据/重打 DS 包")
		return &hubv1.HeartbeatResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	} else {
		var tokenGen uint64
		if claims != nil {
			tokenGen = claims.Gen()
		}
		res, err = s.uc.Heartbeat(ctx, req.GetHubPodName(), req.GetPlayerCount(), req.GetState(), req.GetTsMs(), tokenGen)
	}
	if err != nil {
		code := toProtoCode(err)
		// R4:心跳成功侧一条不打;失败侧必须打。biz 内部只对部分分支(拓扑未确认)
		// 有日志,ActivateHeartbeat 的 stale/相位锁定 fail-closed 返回 ErrUnauthorized ——
		// 它不是 IsServerFault,access log 只记 rpc_ok(DEBUG),线上完全不可见,
		// 而它正是"这台 DS 永远翻不到 ready → ErrHubNoAvailable"的根因。
		plog.With(ctx).Warnw("msg", "hub_heartbeat_failed",
			"ds_pod", req.GetHubPodName(), "state", req.GetState(),
			"player_count", req.GetPlayerCount(), "model_b", cred != nil,
			"reason", bizRejectReason(err), "code", int32(code), "err", err)
		return &hubv1.HeartbeatResponse{Code: code}, nil
	}
	// R4:心跳是 5s/台 的高频路径,成功侧一律 Debug。但下发控制指令(drain/stop)
	// 与驱逐单是不可逆的阶段推进(R1),只在非空时升 Info——它们只在排空/整合/
	// 顶号清退时出现,频率与分片数同阶,不会冲走 WARN。
	if res.Command != "" || len(res.EvictionOrders) > 0 {
		plog.With(ctx).Infow("msg", "hub_heartbeat_command_issued",
			"ds_pod", req.GetHubPodName(), "command", res.Command,
			"grace_seconds", res.GraceSeconds, "eviction_orders", len(res.EvictionOrders),
			"instance_uid", res.AcceptedInstanceUID, "writer_epoch", res.AcceptedWriterEpoch)
	} else {
		plog.With(ctx).Debugw("msg", "hub_heartbeat_ok",
			"ds_pod", req.GetHubPodName(), "state", req.GetState(),
			"player_count", req.GetPlayerCount(),
			"accepted_gen", res.AcceptedTokenGen, "instance_uid", res.AcceptedInstanceUID)
	}
	// 在线保活:把心跳捎带的在场 player_ids 转发 locator 续 HUB 位置 TTL
	// (biz 内 goroutine 异步 + 独立超时,locator 抖动不拖慢心跳响应)。
	s.uc.RefreshHubPresence(ctx, req.GetHubPodName(), req.GetPlayerIds(), pmw.DSBearerToken(ctx))
	response := &hubv1.HeartbeatResponse{
		Code:                  commonv1.ErrCode_OK,
		Command:               res.Command,
		GraceSeconds:          res.GraceSeconds,
		AcceptedTokenGen:      res.AcceptedTokenGen,
		AcceptedTokenJti:      res.AcceptedTokenJTI,
		AcceptedInstanceUid:   res.AcceptedInstanceUID,
		AcceptedProtocolEpoch: res.AcceptedProtocolEpoch,
		AcceptedWriterEpoch:   res.AcceptedWriterEpoch,
	}
	for _, order := range res.EvictionOrders {
		response.EvictionOrders = append(response.EvictionOrders, &hubv1.HubEvictionOrder{
			PlayerId: order.PlayerID, AssignmentId: order.AssignmentID,
			AdmissionId: order.AdmissionID, AdmissionSeq: order.AdmissionSeq,
			SourceInstanceUid:   order.SourceInstanceUID,
			SourceProtocolEpoch: order.SourceProtocolEpoch,
			SourceWriterEpoch:   order.SourceWriterEpoch,
			CleanupAssignmentId: order.CleanupAssignmentID,
		})
	}
	return response, nil
}

// AcknowledgeAdmission 只接受 :8444 DS callback credential；请求里的 player/assignment
// 仍须由 Redis reservation + 当前 active instance identity 二次核验。
func (s *HubService) AcknowledgeAdmission(ctx context.Context, req *hubv1.AcknowledgeAdmissionRequest) (*hubv1.AcknowledgeAdmissionResponse, error) {
	startedAt := time.Now()
	// local-off-v1:没有 Redis 授权面可消费 reservation,也没有 DS 回调令牌可验
	// (ds_auth.mode=off → CheckHubCredential 恒返回 cred=nil)。走下面的 Model B 分支
	// 只会得到 INVALID_ARG/UNAUTHORIZED,而 DS 侧 ShouldRetry 把这两个码当"明确拒绝"
	// → FailAdmission → KickPlayer,玩家刚连上大厅就被踢。
	// 改走 legacy 准入:仍复核归属三元组,并推进与 Model B 同一个 owner Admit 完成点。
	if !s.modelBAuthority && s.localAdmission {
		if req.GetPlayerId() == 0 || req.GetAssignmentId() == "" || req.GetHubPodName() == "" {
			plog.With(ctx).Warnw("msg", "hub_admission_rejected",
				"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
				"ds_pod", req.GetHubPodName(), "channel", channelLocal,
				"reason", localAdmissionArgReason(req.GetPlayerId(), req.GetAssignmentId(), req.GetHubPodName()),
				"code", int32(commonv1.ErrCode_ERR_INVALID_ARG))
			return &hubv1.AcknowledgeAdmissionResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
		}
		result, err := s.uc.AcknowledgeLocalAdmission(ctx, req.GetPlayerId(),
			req.GetAssignmentId(), req.GetHubPodName())
		if err != nil {
			code := toProtoCode(err)
			plog.With(ctx).Warnw("msg", "hub_admission_failed",
				"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
				"ds_pod", req.GetHubPodName(), "channel", channelLocal,
				"reason", bizRejectReason(err), "code", int32(code),
				"elapsed_ms", time.Since(startedAt).Milliseconds(), "err", err)
			return &hubv1.AcknowledgeAdmissionResponse{Code: code}, nil
		}
		logAdmissionOutcome(ctx, req.GetPlayerId(), req.GetAssignmentId(), req.GetHubPodName(),
			"", 0, channelLocal, result.Admitted, startedAt)
		return &hubv1.AcknowledgeAdmissionResponse{Code: commonv1.ErrCode_OK, Admitted: result.Admitted}, nil
	}
	if !s.modelBAuthority || req.GetPlayerId() == 0 || req.GetAssignmentId() == "" ||
		req.GetHubPodName() == "" || req.GetAdmissionId() == "" || req.GetAdmissionSeq() == 0 {
		// 一个 INVALID_ARG 收敛 6 个条件 → 拆回唯一枚举 reason(R2)。DS 侧只认 code==0
		// 才出队,非 0 按 1s 周期无限重试(2026-08-06 事故),必须能一眼分出是哪个字段。
		plog.With(ctx).Warnw("msg", "hub_admission_rejected",
			"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
			"ds_pod", req.GetHubPodName(), "admission_id", req.GetAdmissionId(),
			"admission_seq", req.GetAdmissionSeq(), "channel", channelModelB,
			"reason", modelBAdmissionArgReason(s.modelBAuthority, req.GetPlayerId(),
				req.GetAssignmentId(), req.GetHubPodName(), req.GetAdmissionId(), req.GetAdmissionSeq()),
			"code", int32(commonv1.ErrCode_ERR_INVALID_ARG))
		return &hubv1.AcknowledgeAdmissionResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	_, cred, err := s.dsGuard.CheckHubCredential(ctx, pmw.DSScope{
		Type: auth.DSTypeHub, Pod: req.GetHubPodName(), RequireToken: true,
	})
	if err != nil {
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "hub_admission_rejected",
			"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
			"ds_pod", req.GetHubPodName(), "admission_id", req.GetAdmissionId(),
			"admission_seq", req.GetAdmissionSeq(), "channel", channelModelB,
			"reason", reasonCredentialCheckFailed, "code", int32(code), "err", err)
		return &hubv1.AcknowledgeAdmissionResponse{Code: code}, nil
	}
	if cred == nil {
		plog.With(ctx).Warnw("msg", "hub_admission_rejected",
			"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
			"ds_pod", req.GetHubPodName(), "admission_id", req.GetAdmissionId(),
			"admission_seq", req.GetAdmissionSeq(), "channel", channelModelB,
			"reason", reasonNoModelBCredential,
			"code", int32(commonv1.ErrCode_ERR_UNAUTHORIZED))
		return &hubv1.AcknowledgeAdmissionResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	result, err := s.uc.AcknowledgeAdmission(ctx, req.GetPlayerId(), req.GetAssignmentId(),
		req.GetHubPodName(), req.GetAdmissionId(), req.GetAdmissionSeq(), req.GetSessionJti(),
		hubCredentialFromGuard(cred))
	if err != nil {
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "hub_admission_failed",
			"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
			"ds_pod", req.GetHubPodName(), "admission_id", req.GetAdmissionId(),
			"admission_seq", req.GetAdmissionSeq(), "channel", channelModelB,
			"instance_uid", cred.InstanceUID, "writer_epoch", cred.WriterEpoch,
			"has_session_jti", req.GetSessionJti() != "",
			"reason", bizRejectReason(err), "code", int32(code),
			"elapsed_ms", time.Since(startedAt).Milliseconds(), "err", err)
		return &hubv1.AcknowledgeAdmissionResponse{Code: code}, nil
	}
	logAdmissionOutcome(ctx, req.GetPlayerId(), req.GetAssignmentId(), req.GetHubPodName(),
		req.GetAdmissionId(), req.GetAdmissionSeq(), channelModelB, result.Admitted, startedAt)
	return &hubv1.AcknowledgeAdmissionResponse{Code: commonv1.ErrCode_OK, Admitted: result.Admitted}, nil
}

// logAdmissionOutcome 把准入结果分成两条稳定事件:admitted=true 是不可逆阶段推进(R1,Info);
// admitted=false 是“OK 包里的拒绝”——ledger 未能把 reservation 转成 connected owner,
// 它不带错误码、access log 完全看不见,但 DS 侧会据此拒开 spawn gate(R2,Warn)。
func logAdmissionOutcome(ctx context.Context, playerID uint64, assignmentID, pod,
	admissionID string, admissionSeq uint64, channel string, admitted bool, startedAt time.Time,
) {
	if !admitted {
		plog.With(ctx).Warnw("msg", "hub_admission_rejected",
			"player_id", playerID, "hub_assignment_id", assignmentID, "ds_pod", pod,
			"admission_id", admissionID, "admission_seq", admissionSeq, "channel", channel,
			"reason", reasonLedgerNotAdmitted,
			"elapsed_ms", time.Since(startedAt).Milliseconds())
		return
	}
	plog.With(ctx).Infow("msg", "hub_admitted",
		"player_id", playerID, "hub_assignment_id", assignmentID, "ds_pod", pod,
		"admission_id", admissionID, "admission_seq", admissionSeq, "channel", channel,
		"elapsed_ms", time.Since(startedAt).Milliseconds())
}

// AcknowledgeDeparture exact 删除当前 admission owner；Conflict 返回 OK+departed=false，
// 让旧连接停止重试且不影响已接管的新连接。
func (s *HubService) AcknowledgeDeparture(ctx context.Context, req *hubv1.AcknowledgeDepartureRequest) (*hubv1.AcknowledgeDepartureResponse, error) {
	startedAt := time.Now()
	// local-off-v1:与上面 AcknowledgeAdmission 同源的 legacy 离场通道。走下面 Model B 分支
	// 只会恒返回 INVALID_ARG(与请求内容无关),而 DS 侧 Departure 队列只认 code=0 才出队,
	// 结果是每秒一次的永久重试刷屏 + 队列项永不释放(2026-08-06 实测)。
	if !s.modelBAuthority && s.localAdmission {
		if req.GetPlayerId() == 0 || req.GetAssignmentId() == "" || req.GetHubPodName() == "" {
			plog.With(ctx).Warnw("msg", "hub_departure_rejected",
				"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
				"ds_pod", req.GetHubPodName(), "channel", channelLocal,
				"reason", localAdmissionArgReason(req.GetPlayerId(), req.GetAssignmentId(), req.GetHubPodName()),
				"code", int32(commonv1.ErrCode_ERR_INVALID_ARG))
			return &hubv1.AcknowledgeDepartureResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
		}
		result, err := s.uc.AcknowledgeLocalDeparture(ctx, req.GetPlayerId(),
			req.GetAssignmentId(), req.GetHubPodName())
		if err != nil {
			code := toProtoCode(err)
			plog.With(ctx).Warnw("msg", "hub_departure_failed",
				"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
				"ds_pod", req.GetHubPodName(), "channel", channelLocal,
				"reason", bizRejectReason(err), "code", int32(code),
				"elapsed_ms", time.Since(startedAt).Milliseconds(), "err", err)
			return &hubv1.AcknowledgeDepartureResponse{Code: code}, nil
		}
		plog.With(ctx).Infow("msg", "hub_departed",
			"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
			"ds_pod", req.GetHubPodName(), "channel", channelLocal,
			"departed", result.Departed, "conflict", false,
			"elapsed_ms", time.Since(startedAt).Milliseconds())
		return &hubv1.AcknowledgeDepartureResponse{Code: commonv1.ErrCode_OK, Departed: result.Departed}, nil
	}
	if !s.modelBAuthority || req.GetPlayerId() == 0 || req.GetAssignmentId() == "" ||
		req.GetHubPodName() == "" || req.GetAdmissionId() == "" || req.GetAdmissionSeq() == 0 {
		plog.With(ctx).Warnw("msg", "hub_departure_rejected",
			"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
			"ds_pod", req.GetHubPodName(), "admission_id", req.GetAdmissionId(),
			"admission_seq", req.GetAdmissionSeq(), "channel", channelModelB,
			"reason", modelBAdmissionArgReason(s.modelBAuthority, req.GetPlayerId(),
				req.GetAssignmentId(), req.GetHubPodName(), req.GetAdmissionId(), req.GetAdmissionSeq()),
			"code", int32(commonv1.ErrCode_ERR_INVALID_ARG))
		return &hubv1.AcknowledgeDepartureResponse{Code: commonv1.ErrCode_ERR_INVALID_ARG}, nil
	}
	_, cred, err := s.dsGuard.CheckHubCredential(ctx, pmw.DSScope{
		Type: auth.DSTypeHub, Pod: req.GetHubPodName(), RequireToken: true,
	})
	if err != nil {
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "hub_departure_rejected",
			"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
			"ds_pod", req.GetHubPodName(), "admission_id", req.GetAdmissionId(),
			"admission_seq", req.GetAdmissionSeq(), "channel", channelModelB,
			"reason", reasonCredentialCheckFailed, "code", int32(code), "err", err)
		return &hubv1.AcknowledgeDepartureResponse{Code: code}, nil
	}
	if cred == nil {
		plog.With(ctx).Warnw("msg", "hub_departure_rejected",
			"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
			"ds_pod", req.GetHubPodName(), "admission_id", req.GetAdmissionId(),
			"admission_seq", req.GetAdmissionSeq(), "channel", channelModelB,
			"reason", reasonNoModelBCredential,
			"code", int32(commonv1.ErrCode_ERR_UNAUTHORIZED))
		return &hubv1.AcknowledgeDepartureResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	result, err := s.uc.AcknowledgeDeparture(ctx, req.GetPlayerId(), req.GetAssignmentId(),
		req.GetHubPodName(), req.GetAdmissionId(), req.GetAdmissionSeq(), hubCredentialFromGuard(cred))
	if err != nil {
		code := toProtoCode(err)
		plog.With(ctx).Warnw("msg", "hub_departure_failed",
			"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
			"ds_pod", req.GetHubPodName(), "admission_id", req.GetAdmissionId(),
			"admission_seq", req.GetAdmissionSeq(), "channel", channelModelB,
			"instance_uid", cred.InstanceUID, "writer_epoch", cred.WriterEpoch,
			"reason", bizRejectReason(err), "code", int32(code),
			"elapsed_ms", time.Since(startedAt).Milliseconds(), "err", err)
		return &hubv1.AcknowledgeDepartureResponse{Code: code}, nil
	}
	// R1 阶段推进与 hub_admitted 成对。conflict=true 是旧连接晚到的 Logout(已被新
	// admission 接管),对旧连接是“停止重试”、对新连接零影响,带 reason 字段区分。
	plog.With(ctx).Infow("msg", "hub_departed",
		"player_id", req.GetPlayerId(), "hub_assignment_id", req.GetAssignmentId(),
		"ds_pod", req.GetHubPodName(), "admission_id", req.GetAdmissionId(),
		"admission_seq", req.GetAdmissionSeq(), "channel", channelModelB,
		"instance_uid", cred.InstanceUID,
		"departed", result.Departed, "conflict", result.Conflict,
		"elapsed_ms", time.Since(startedAt).Milliseconds())
	return &hubv1.AcknowledgeDepartureResponse{Code: commonv1.ErrCode_OK, Departed: result.Departed}, nil
}

// ListHubLines 列出玩家当前 region 可切换的大厅线路(玩家侧,player_id 取自 JWT sub)。
func (s *HubService) ListHubLines(ctx context.Context, req *hubv1.ListHubLinesRequest) (*hubv1.ListHubLinesResponse, error) {
	playerID := pmw.PlayerIDFromContext(ctx)
	if playerID == 0 {
		plog.With(ctx).Warnw("msg", "hub_list_lines_rejected",
			"region", req.GetRegion(),
			"reason", reasonNoSessionPlayerID, "code", int32(commonv1.ErrCode_ERR_UNAUTHORIZED))
		return &hubv1.ListHubLinesResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	views, err := s.uc.ListHubLinesForPlayer(ctx, playerID, req.GetRegion())
	if err != nil {
		code := toProtoCode(err)
		// 切线 UI 的数据源。它与 AssignHub 共用 routableShardViews:这里报错 = 玩家看到
		// 空线路列表,与 ErrHubNoAvailable 同根。错误码多为 ErrInvalidState(非 ServerFault)。
		plog.With(ctx).Warnw("msg", "hub_list_lines_failed",
			"player_id", playerID, "region", req.GetRegion(),
			"reason", bizRejectReason(err), "code", int32(code), "err", err)
		return &hubv1.ListHubLinesResponse{Code: code}, nil
	}
	lines := make([]*hubv1.HubLine, 0, len(views))
	for _, v := range views {
		lines = append(lines, &hubv1.HubLine{
			LineNo:      v.LineNo,
			ShardId:     v.ShardID,
			PlayerCount: v.PlayerCount,
			Capacity:    v.Capacity,
			IsFull:      v.IsFull,
			IsCurrent:   v.IsCurrent,
		})
	}
	plog.With(ctx).Debugw("msg", "hub_list_lines_ok",
		"player_id", playerID, "region", req.GetRegion(), "lines", len(lines))
	return &hubv1.ListHubLinesResponse{Code: commonv1.ErrCode_OK, Lines: lines}, nil
}

// TransferToLine 玩家主动切换到指定线路(换实例,player_id 取自 JWT sub)。
func (s *HubService) TransferToLine(ctx context.Context, req *hubv1.TransferToLineRequest) (*hubv1.TransferToLineResponse, error) {
	startedAt := time.Now()
	playerID := pmw.PlayerIDFromContext(ctx)
	if playerID == 0 {
		plog.With(ctx).Warnw("msg", "hub_line_transfer_rejected",
			"target_shard_id", req.GetTargetShardId(),
			"reason", reasonNoSessionPlayerID, "code", int32(commonv1.ErrCode_ERR_UNAUTHORIZED))
		return &hubv1.TransferToLineResponse{Code: commonv1.ErrCode_ERR_UNAUTHORIZED}, nil
	}
	res, err := s.uc.TransferToLineForPlayer(ctx, playerID, req.GetTargetShardId())
	if err != nil {
		code := toProtoCode(err)
		// 全部拒绝码(ErrHubLineFull / ErrHubTransferCooldown / ErrHubTransferNotInHub /
		// ErrInvalidState)都不是 IsServerFault → access log 只记 DEBUG,必须在这里打。
		plog.With(ctx).Warnw("msg", "hub_line_transfer_failed",
			"player_id", playerID, "target_shard_id", req.GetTargetShardId(),
			"reason", bizRejectReason(err), "code", int32(code),
			"elapsed_ms", time.Since(startedAt).Milliseconds(), "err", err)
		return &hubv1.TransferToLineResponse{Code: code}, nil
	}
	plog.With(ctx).Infow("msg", "hub_line_transferred",
		"player_id", playerID, "target_shard_id", req.GetTargetShardId(),
		"new_shard_id", res.NewShardID, "line_no", res.LineNo,
		"elapsed_ms", time.Since(startedAt).Milliseconds())
	return &hubv1.TransferToLineResponse{
		Code:         commonv1.ErrCode_OK,
		NewHubDsAddr: res.NewHubDSAddr,
		NewHubTicket: res.NewHubTicket,
		NewShardId:   res.NewShardID,
		LineNo:       res.LineNo,
	}, nil
}

// ── 辅助 ──────────────────────────────────────────────────────────────────────

// 日志 reason 枚举(infra.md §11.3 R2:一个 if 收敛了 N 个条件的必须拆成 N 个 reason;
// snake_case 常量,不是自由文本)。这里全部是**入参/凭据门**的拒绝原因 —— 它们返回的
// ERR_INVALID_ARG / ERR_UNAUTHORIZED 都不是 errcode.IsServerFault,pkg/middleware/logging.go
// 只会记成 rpc_ok(DEBUG),线上 info 级完全不可见,必须由本层自己打出来。
const (
	reasonNotModelBAuthority          = "not_model_b_authority"
	reasonMissingPlayerID             = "missing_player_id"
	reasonMissingAssignmentID         = "missing_assignment_id"
	reasonMissingPod                  = "missing_pod"
	reasonMissingAdmissionID          = "missing_admission_id"
	reasonMissingAdmissionSeq         = "missing_admission_seq"
	reasonNoModelBCredential          = "no_model_b_credential"
	reasonCredentialCheckFailed       = "credential_check_failed"
	reasonLegacyCredentialUnderModelB = "legacy_credential_under_model_b"
	reasonNoSessionPlayerID           = "no_session_player_id"
	reasonBizRejected                 = "biz_rejected"
	reasonUnknown                     = "unknown"
	// reasonLedgerNotAdmitted:ledger 未能把 reservation 转成 connected owner。
	// 它返回 code=OK + admitted=false,access log 记 rpc_ok(DEBUG),不打就彻底看不见。
	reasonLedgerNotAdmitted = "ledger_not_admitted"
	// reasonServiceDisabled:placement 路由体系已硬切删除,旧调用方残留。
	reasonServiceDisabled = "service_disabled"
)

// 准入 / 离场的两条通道标签(channel 字段):同一个 msg 下区分 Model B 与 local 旧面,
// 排障时不用反推部署形态。
const (
	channelModelB = "model_b"
	channelLocal  = "local"
)

// modelBAdmissionArgReason 把 AcknowledgeAdmission / AcknowledgeDeparture 里那个
// **一个 INVALID_ARG 收敛 6 个条件**的入参门拆回唯一枚举 reason(R2)。
//
// 判定顺序刻意与 if 的短路顺序逐字一致:调用方只在已经判定要拒绝时才调用它,
// 因此这里返回的就是"第一个不满足的条件",与拒绝原因严格同源。
//
// 为什么必须区分:modelBAuthority 配错(部署形态,要改配置重启)与 DS 没带
// admission_seq(客户端/DS 版本不对,要重打包)处置方案完全相反,而 DS 侧
// ShouldCompleteQueuedHubDeparture 只认 code==0 才出队,非 0 按 1s 周期无限重试
// (2026-08-06 事故),再犯时必须能从后端日志一眼分出是哪一个字段缺失。
func modelBAdmissionArgReason(modelBAuthority bool, playerID uint64,
	assignmentID, pod, admissionID string, admissionSeq uint64,
) string {
	switch {
	case !modelBAuthority:
		return reasonNotModelBAuthority
	case playerID == 0:
		return reasonMissingPlayerID
	case assignmentID == "":
		return reasonMissingAssignmentID
	case pod == "":
		return reasonMissingPod
	case admissionID == "":
		return reasonMissingAdmissionID
	case admissionSeq == 0:
		return reasonMissingAdmissionSeq
	}
	return reasonUnknown
}

// localAdmissionArgReason 是 mode=local 通道(只校验三元组)的同款拆分。
func localAdmissionArgReason(playerID uint64, assignmentID, pod string) string {
	switch {
	case playerID == 0:
		return reasonMissingPlayerID
	case assignmentID == "":
		return reasonMissingAssignmentID
	case pod == "":
		return reasonMissingPod
	}
	return reasonUnknown
}

// bizRejectReason 把 biz 上抛的 errcode 收敛成稳定的 snake_case reason,用于
// "同一个错误码两种截然不同的处置"场景下的日志分流(如 ErrHubNoAvailable 既可能是
// 真没容量、也可能是 CAS 并发耗尽 —— 后者在 biz 侧另有 hub_assign_cas_exhausted)。
func bizRejectReason(err error) string {
	switch errcode.As(err) {
	case errcode.ErrInvalidArg:
		return "invalid_arg"
	case errcode.ErrUnauthorized:
		return "unauthorized"
	case errcode.ErrInvalidState:
		return "invalid_state"
	case errcode.ErrUnavailable:
		return "upstream_unavailable"
	case errcode.ErrSessionSuperseded:
		return "session_superseded"
	case errcode.ErrLocatorConflict:
		return "locator_conflict"
	case errcode.ErrHubNoAvailable:
		return "hub_no_available"
	case errcode.ErrHubTransferFailed:
		return "hub_transfer_failed"
	case errcode.ErrHubLineFull:
		return "hub_line_full"
	case errcode.ErrHubTransferCooldown:
		return "hub_transfer_cooldown"
	case errcode.ErrHubTransferNotInHub:
		return "hub_transfer_not_in_hub"
	case errcode.ErrOwnerBarrierNotOpen:
		return "owner_barrier_not_open"
	case errcode.ErrOwnerEpochConflict:
		return "owner_epoch_conflict"
	case errcode.ErrOwnerIdentityMismatch:
		return "owner_identity_mismatch"
	}
	return reasonBizRejected
}

// claimsGen 只为日志取 legacy 令牌代际(nil-safe);claims==nil(off/permissive)时为 0。
func claimsGen(claims *auth.DSCallbackClaims) uint64 {
	if claims == nil {
		return 0
	}
	return claims.Gen()
}

// toProtoCode 把 pkg/errcode 1:1 映射成 proto enum(数值相同)。
func toProtoCode(err error) commonv1.ErrCode {
	return commonv1.ErrCode(errcode.As(err))
}

func hubCredentialFromGuard(cred *pmw.VerifiedCredential) *biz.HubCredential {
	if cred == nil {
		return nil
	}
	return &biz.HubCredential{
		InstanceUID: cred.InstanceUID, ProtocolEpoch: cred.ProtocolEpoch, Gen: cred.Gen,
		JTI: cred.JTI, TokenSHA256: cred.TokenSHA256, Kid: cred.Kid, WriterEpoch: cred.WriterEpoch,
	}
}
