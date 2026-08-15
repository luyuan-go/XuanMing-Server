// ticket.go — DSTicket 签发 / 校验用例(W3 ①,2026-06-05)。
//
// 不变量(CLAUDE.md §9):
//   - §3 DS 票据短时效:本用例签的 ticket 默认 exp 5min
//   - §4 DS 崩溃必有补偿:本用例不维护 ticket 状态(无状态),DS 崩溃由 player_locator + hub_allocator 补
//   - §6 MMR 计算在 battle_result(DS 不可信):本用例签的 ticket 只代表"准入",DS 内业务不能信任 ticket 之外的玩家数据
//
// W3 ②(2026-06-05)真实化:
//   - VerifyDSTicket 通过签名后,调 jtiRepo.MarkUsed(jti, ds_ticket_ttl) → SETNX 防重放
//   - SETNX 失败映射 ErrLoginTicketReplayed(同一票据被多个 DS 重复 Verify)
//   - IssueDSTicket 仍只签发(不预占 jti,节省一次 redis 写)
//
// 本用例只做"签 / 验",IssueDSTicket 的输入校验(session 是否在线、target_id 是否合法 DS pod)由调用方做。

package biz

import (
	"context"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"

	"github.com/luyuancpp/pandora/services/account/login/internal/data"
)

// DS 准入验票的拒绝 reason 枚举(§11.3 R2)。
//
// 这条路径上的拒绝码大多是 ErrInvalidArg / ErrUnauthorized,而 pkg/middleware/logging.go
// 只对 errcode.IsServerFault 升 ERROR,其余一律落 rpc_ok=DEBUG —— 也就是说线上默认级别下
// 「玩家进不去 DS」在后端一行日志都不会有。故这些分支必须由业务代码自己显式打。
const (
	admissionRejectRepoUnavailable       = "admission_repo_unavailable"
	admissionRejectTicketNoJTI           = "ticket_missing_jti"
	admissionRejectMarkerOwnerInvalid    = "admission_marker_owner_invalid"
	admissionRejectPeekFailed            = "admission_peek_failed"
	admissionRejectHubCheckerUnavailable = "hub_assignment_checker_unavailable"
)

// DSTicketResult 是 IssueDSTicket 的产出。
type DSTicketResult struct {
	Ticket      string
	JTI         string
	ExpiresAtMs int64
	PlayerID    uint64
	// BattleDSAddr 仅供 login reconnect 内部使用，来自 roster authorizer 的同一 Redis 快照。
	// 公共 IssueDSTicket(battle) 响应仍不返回地址（正常地址来自 matchmaker）。
	BattleDSAddr string
}

// DSTicketClaims 是 VerifyDSTicket 的产出(透传 auth.DSTicketClaims 的核心字段,
// service 层翻译成 proto LoginService.DSTicket message)。
type DSTicketClaims struct {
	Version     int
	PlayerID    uint64
	MatchID     uint64
	DSType      string
	JTI         string
	IssuedAtMs  int64
	ExpiresAtMs int64
	// RegionID / CellID 是票据绑定的玩家路由落点(scale-cellular-20m.md §3.3)。
	// 单 Cell / dev 票据为 0。DS 侧据此校验票据 Cell == 本 DS 所在 Cell,防跨单元串号。
	RegionID uint32
	CellID   uint32
	// RoleID 是票据携带的玩家已选角色(选角权威化 2026-07-08)。0 = 未携带。
	RoleID uint32
	// Hub DSTicket 的当前归属/DS active 凭据绑定。battle/legacy 票据为零值。
	DSPodName       string
	DSInstanceUID   string
	DSProtocolEpoch uint32
	DSCredentialGen uint64
	DSCredentialJTI string
	HubAssignmentID string
	DSWriterEpoch   uint32
	// v2(RS256/B1)稳定实例绑定。legacy 票据为零值。
	DSInstanceEpoch uint32
	AllocationID    string
	ReleaseTrack    string
	// SessJTI:签发本票的请求方会话 jti(R6 复审 P0-3)。VerifyDSTicket 核销时对非空值
	// 复核会话权威现行性;空 = 兼容窗(legacy 票/批签路径/滚动升级旧票)。
	SessJTI string
}

// verifiedDSTicket 是 legacy HS256 与 v2 RS256 在 Login 内部的统一、只读视图。
// Version 精确区分两条验证规则，绝不把 v2 缺失字段降级解释成 legacy。
type verifiedDSTicket struct {
	Version         int
	PlayerID        uint64
	MatchID         uint64
	DSType          string
	JTI             string
	IssuedAtMs      int64
	ExpiresAtMs     int64
	ReplayTTL       time.Duration
	RegionID        uint32
	CellID          uint32
	RoleID          uint32
	DSPodName       string
	DSInstanceUID   string
	DSInstanceEpoch uint32
	ReleaseTrack    string
	AllocationID    string
	HubAssignmentID string
	// SessJTI:v2 票内签发方会话 jti(R6 复审 P0-3;legacy 票恒空)。
	SessJTI string
	// legacy Model-B callback credential binding（v2 有意不携带）。
	DSProtocolEpoch uint32
	DSCredentialGen uint64
	DSCredentialJTI string
	DSWriterEpoch   uint32
}

// TicketUsecase 处理 DSTicket 的签发 / 校验。
//
// W3 ①:HS256 + 5min exp;jti 用 uuid v4。
// W3 ②:jtiRepo 非空时,VerifyDSTicket 通过后 SETNX,防止同一 jti 被多个 DS 重放。
type TicketUsecase struct {
	signer            *auth.Signer
	verifier          *auth.Verifier
	jtiRepo           data.TicketJTIRepo // 可空(dev 不接 redis 时):跳过防重放,只验签
	assignmentChecker data.HubAssignmentChecker
	battleAuthorizer  data.BattleTicketAuthorizer
	// requireHubAssignmentBinding 是滚动激活栅栏。false 接受旧的全空绑定 hub 票；true
	// 后只接受 hub_allocator 签发且与 Redis 当前 assignment 精确一致的完整绑定票。
	requireHubAssignmentBinding bool

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §3.3)。
	// 可为 nil:单 Cell / dev 部署不路由,签出票据 region/cell = 0。多 Cell 部署由 main
	// 经 SetCellRouter 注入。nil-safe,不阻断签票。
	router *cellroute.Router

	// v2Signer 非 nil 时启用 DSTicket v2(RS256,方案 B):battle 票改签实例绑定票
	// (绑定来自 roster 权威门同一 Redis 快照,缺失 fail-closed 拒签);hub 票一律拒签
	// (v2 hub 票必须带实例绑定,只能由 hub_allocator 签)。
	v2Signer *auth.DSTicketSigner
	// v2Verifier 供 Login VerifyDSTicket 在线端点使用；非 nil 同时机械激活玩家票
	// RS256-only。B1 DS 正常准入仍本地验签，不依赖此在线调用。
	v2Verifier *auth.DSTicketVerifier

	// sessionGate 票据兑换点会话现行性门(R7 复审 P2-1,由 LoginUsecase 实现)。
	// 非 nil 时 verifyDSTicket 在写 replay marker **之前**复核票内 sjti 仍是当前一代:
	// 已被顶旧票不再消耗 jti marker/短幂等窗名额,合法新登录链路不受旧票干扰。
	// service 层在响应写出前另有一道终检,两道共同覆盖验票期间被轮换的窗口。
	sessionGate TicketSessionGate
}

// TicketSessionGate 是票据兑换点的会话现行性复核接口(LoginUsecase.RequireTicketSessionCurrent)。
type TicketSessionGate interface {
	RequireTicketSessionCurrent(ctx context.Context, playerID uint64, ticketSessJTI string) error
}

// SetTicketSessionGate 注入会话现行性门(启动期、对外监听前调用;nil-safe)。
func (u *TicketUsecase) SetTicketSessionGate(g TicketSessionGate) {
	u.sessionGate = g
}

// NewTicketUsecase 构造用例。
func NewTicketUsecase(signer *auth.Signer, verifier *auth.Verifier, jtiRepo data.TicketJTIRepo) *TicketUsecase {
	return &TicketUsecase{signer: signer, verifier: verifier, jtiRepo: jtiRepo}
}

// SetHubAssignmentBindingPolicy 在服务启动、对外监听前注入 Hub 归属校验器与激活栅栏。
func (u *TicketUsecase) SetHubAssignmentBindingPolicy(require bool, checker data.HubAssignmentChecker) {
	u.requireHubAssignmentBinding = require
	u.assignmentChecker = checker
}

// SetBattleTicketAuthorizer 注入 battle 票签发前的 player↔match roster 权威门。
// 未注入时 battle 签票 fail-closed；Hub 签票不受此门影响。
func (u *TicketUsecase) SetBattleTicketAuthorizer(authorizer data.BattleTicketAuthorizer) {
	u.battleAuthorizer = authorizer
}

// SetDSTicketV2Signer 注入 DSTicket v2(RS256,方案 B)签发器(启动期、对外监听前调用)。
// 注入后 battle 签票全部走 v2,不再签 legacy HS256 票。
func (u *TicketUsecase) SetDSTicketV2Signer(s *auth.DSTicketSigner) {
	u.v2Signer = s
}

// SetDSTicketV2Verifier 注入严格 RS256 verifier。只要 v2 signer/verifier 任一启用，
// 玩家 DSTicket 验证就机械进入 RS256-only；legacy HS256 只留给完全未启用 v2 的 local/off。
func (u *TicketUsecase) SetDSTicketV2Verifier(v *auth.DSTicketVerifier) {
	u.v2Verifier = v
}

func (u *TicketUsecase) rs256DSTicketProfileEnabled() bool {
	return u != nil && (u.v2Signer != nil || u.v2Verifier != nil)
}

// SetCellRouter 注入确定性 region/cell 路由器(可选,多 Cell 部署用)。
//
// nil-safe:不调用 / 传 nil 时签出票据 region/cell = 0(单 Cell / dev 语义)。
// 用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 LoginUsecase.SetCellRouter 一致)。
func (u *TicketUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// routeRegionCell 算玩家落点;router 为 nil(单 Cell / dev)或 Route 报错时降级为 0/0,不阻断签票。
func (u *TicketUsecase) routeRegionCell(ctx context.Context, playerID uint64) (regionID, cellID uint32) {
	if u.router == nil {
		return 0, 0
	}
	loc, err := u.router.Route(playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "cellroute_failed", "err", err, "player_id", playerID)
		return 0, 0
	}
	return loc.RegionID, loc.CellID
}

// IssueDSTicket 给指定 player 签 hub / battle DS 票据。
//
// dsType: "hub" / "battle"
// targetID: hub 为 0;battle 必须填 match_id
// playerID: 已通过 session 校验(本用例不再二次解 session_token,只信调用方)
// sessJTI: 请求方会话 jti(R6 复审 P0-3,签进 v2 票 sjti claim;空 = dev/兼容窗)。
//
// 失败返回 *errcode.Error。
func (u *TicketUsecase) IssueDSTicket(ctx context.Context, playerID uint64, dsType string, targetID uint64, sessJTI string) (*DSTicketResult, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "playerID must be > 0")
	}
	var ds auth.DSType
	switch dsType {
	case string(auth.DSTypeHub):
		ds = auth.DSTypeHub
	case string(auth.DSTypeBattle):
		ds = auth.DSTypeBattle
	default:
		return nil, errcode.New(errcode.ErrInvalidArg, "dsType must be hub|battle, got %q", dsType)
	}
	if ds == auth.DSTypeBattle && targetID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "battle DSTicket requires match_id (targetID)")
	}
	if ds == auth.DSTypeBattle {
		regionID, cellID := u.routeRegionCell(ctx, playerID)
		return u.IssueBattleDSTicketAtCell(ctx, playerID, targetID, regionID, cellID, sessJTI)
	}
	if ds == auth.DSTypeHub && u.rs256DSTicketProfileEnabled() {
		// v2(方案 B):hub 票必须带完整实例绑定,只能由 hub_allocator 签;login 不再自签。
		return nil, errcode.New(errcode.ErrUnavailable,
			"hub DSTicket v2 must be issued by hub_allocator (instance binding required)")
	}
	if ds == auth.DSTypeHub && u.requireHubAssignmentBinding {
		return nil, errcode.New(errcode.ErrUnavailable,
			"hub DSTicket must be issued by hub_allocator while assignment binding is required")
	}

	// 算玩家路由落点并签进票据(§3.3 防跨单元串号);单 Cell / dev → 0/0。
	regionID, cellID := u.routeRegionCell(ctx, playerID)
	return u.issueDSTicketAtCell(ctx, playerID, ds, targetID, regionID, cellID)
}

// IssueBattleDSTicketAtCell 是所有 login 侧 Battle 签票路径的唯一入口。公共
// IssueDSTicket 与登录断线重连都必须先经过同一个 player↔match roster 权威门，
// 防止重连路径因只相信 locator 而重新引入“知道 match_id 即可拿票”的旁路。
func (u *TicketUsecase) IssueBattleDSTicketAtCell(
	ctx context.Context,
	playerID, matchID uint64,
	regionID, cellID uint32,
	sessJTI string,
) (*DSTicketResult, error) {
	if playerID == 0 || matchID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "battle ticket requires player and match")
	}
	if u == nil || u.battleAuthorizer == nil {
		return nil, errcode.New(errcode.ErrUnavailable, "battle ticket roster authority unavailable")
	}
	target, err := u.battleAuthorizer.AuthorizeBattleTicket(ctx, playerID, matchID)
	if err != nil {
		return nil, err
	}
	if target.DSAddr == "" {
		return nil, errcode.New(errcode.ErrUnavailable, "battle ticket target address unavailable")
	}
	if u.v2Signer != nil {
		// v2(方案 B):票据绑死到 roster 权威门同一 Redis 快照里的唯一 DS 实例。
		// 实例身份缺一即拒(旧记录/降级路径),绝不退回无绑定票。
		return u.issueBattleDSTicketV2(ctx, playerID, matchID, regionID, cellID, target, sessJTI)
	}
	if u.rs256DSTicketProfileEnabled() {
		return nil, errcode.New(errcode.ErrUnavailable,
			"RS256 DSTicket profile has no battle ticket signer")
	}
	result, err := u.issueDSTicketAtCell(ctx, playerID, auth.DSTypeBattle, matchID, regionID, cellID)
	if err != nil {
		return nil, err
	}
	result.BattleDSAddr = target.DSAddr
	return result, nil
}

// InspectBattleRoute 实现 BattleTicketIssuer 的显式三态判定入口(P0 修复 2026-07-15):
// 委托给 data.BattleRouteInspector(RedisBattleTicketAuthorizer 实现)。不签票、零副作用。
// authorizer 未接/不支持三态检查 → UNKNOWN fail-closed(绝不退回把 ErrPermissionDeny 当终态)。
func (u *TicketUsecase) InspectBattleRoute(ctx context.Context, playerID, matchID uint64) (data.BattleRouteState, error) {
	if playerID == 0 || matchID == 0 {
		return data.BattleRouteUnknown, errcode.New(errcode.ErrInvalidArg, "battle route check requires player and match")
	}
	if u == nil || u.battleAuthorizer == nil {
		return data.BattleRouteUnknown, errcode.New(errcode.ErrUnavailable, "battle route roster authority unavailable")
	}
	inspector, ok := u.battleAuthorizer.(data.BattleRouteInspector)
	if !ok {
		return data.BattleRouteUnknown, errcode.New(errcode.ErrUnavailable,
			"battle route authority does not support explicit terminal inspection")
	}
	return inspector.InspectBattleRoute(ctx, playerID, matchID)
}

// issueBattleDSTicketV2 用 v2 签发器签实例绑定 battle 票(RS256,方案 B)。
// sessJTI 签进 sjti claim(R6 复审 P0-3,VerifyDSTicket 核销时复核;空 = 兼容窗)。
func (u *TicketUsecase) issueBattleDSTicketV2(
	ctx context.Context,
	playerID, matchID uint64,
	regionID, cellID uint32,
	target data.BattleTicketTarget,
	sessJTI string,
) (*DSTicketResult, error) {
	h := plog.With(ctx)
	if target.PodName == "" || target.InstanceUID == "" || target.InstanceEpoch == 0 || target.AllocationID == "" ||
		(target.ReleaseTrack != auth.ReleaseTrackStable && target.ReleaseTrack != auth.ReleaseTrackCanary) {
		h.Warnw("msg", "battle_ticket_v2_target_incomplete",
			"player_id", playerID, "match_id", matchID, "pod", target.PodName,
			"uid", target.InstanceUID, "epoch", target.InstanceEpoch, "allocation_id", target.AllocationID,
			"release_track", target.ReleaseTrack)
		return nil, errcode.New(errcode.ErrUnavailable,
			"battle ticket v2 requires complete DS instance identity from roster authority")
	}
	jti := uuid.NewString()
	tok, expMs, err := u.v2Signer.SignBattleTicket(playerID, regionID, cellID, jti, auth.DSTicketTarget{
		DSPodName:       target.PodName,
		DSInstanceUID:   target.InstanceUID,
		DSInstanceEpoch: target.InstanceEpoch,
		ReleaseTrack:    target.ReleaseTrack,
		MatchID:         matchID,
		AllocationID:    target.AllocationID,
		SessionJTI:      sessJTI, // R6 P0-3:会话绑定,VerifyDSTicket 核销时复核
	})
	if err != nil {
		h.Errorw("msg", "sign_ds_ticket_v2_failed", "err", err, "player_id", playerID, "match_id", matchID)
		return nil, errcode.New(errcode.ErrInternal, "sign v2 battle ticket failed: %v", err)
	}
	// §9.3 五要件里 battle 侧的那一半必须落盘并且是 INFO:DS 侧报
	// 「battle v2 ticket no longer matches roster authority」时,需要对比
	// 「签发瞬间的实例身份」与「核销瞬间的 roster 权威」;此前签发侧只留了 pod + allocation_id,
	// 且是 Debug(线上 info 级一条都不出),两边日志根本对不上。
	h.Infow("msg", "ds_ticket_v2_issued",
		"player_id", playerID, "ds_type", "battle", "match_id", matchID,
		"jti", jti, "exp_ms", expMs, "region_id", regionID, "cell_id", cellID,
		"pod", target.PodName, "allocation_id", target.AllocationID,
		"ds_instance_uid", target.InstanceUID, "ds_instance_epoch", target.InstanceEpoch,
		"release_track", target.ReleaseTrack, "sess_jti", sessJTI, "ds_addr", target.DSAddr)
	return &DSTicketResult{
		Ticket:       tok,
		JTI:          jti,
		ExpiresAtMs:  expMs,
		PlayerID:     playerID,
		BattleDSAddr: target.DSAddr,
	}, nil
}

func (u *TicketUsecase) issueDSTicketAtCell(
	ctx context.Context,
	playerID uint64,
	ds auth.DSType,
	targetID uint64,
	regionID, cellID uint32,
) (*DSTicketResult, error) {
	if u.rs256DSTicketProfileEnabled() {
		return nil, errcode.New(errcode.ErrUnavailable,
			"legacy HS256 DSTicket signing is disabled by the RS256 profile")
	}
	h := plog.With(ctx)
	jti := uuid.NewString()
	tok, expMs, err := u.signer.SignDSTicketWithCell(playerID, ds, targetID, regionID, cellID, jti)
	if err != nil {
		h.Errorw("msg", "sign_ds_ticket_failed", "err", err, "player_id", playerID, "ds_type", string(ds))
		return nil, errcode.New(errcode.ErrInternal, "sign ds ticket failed: %v", err)
	}

	// §11.3 R1:签发票据是不可逆推进,也是 login 侧唯一能证明「我发出去的票绑的是谁」的记录。
	// DS 拒票时后端无感,两边日志就靠这条 + jti 对账;原为 Debug 在线上默认 info 级下一条不出,
	// 与 v2 路径的 ds_ticket_v2_issued(Info)不对称 —— 同一个动作两个级别会让「票到底发没发」在 legacy 档变成盲区。
	h.Infow("msg", "ds_ticket_issued",
		"player_id", playerID, "ds_type", string(ds), "target_id", targetID,
		"jti", jti, "exp_ms", expMs, "region_id", regionID, "cell_id", cellID)

	return &DSTicketResult{
		Ticket:      tok,
		JTI:         jti,
		ExpiresAtMs: expMs,
		PlayerID:    playerID,
	}, nil
}

// VerifyDSTicket 校验 ticket 签名 + exp + iss + aud,然后(W3 ②)SETNX jti 防重放。
//
// dsPodName 当前仅写日志,W3+ 接 DS 注册表后用于"票据 target_id == pod 自报 id" 二次校验。
func (u *TicketUsecase) VerifyDSTicket(ctx context.Context, ticket, dsPodName string) (*DSTicketClaims, error) {
	return u.verifyDSTicket(ctx, ticket, dsPodName, "", nil)
}

// VerifyDSTicketForAdmission 是 Redis authority 的在线入场入口。
// caller 必须已经由 service 依次完成 DSCallbackGuard 验签与 Redis active checker；本方法
// 再把玩家票据 claims 与 caller active binding 精确比对，最后才以 admission owner 幂等消费 jti。
func (u *TicketUsecase) VerifyDSTicketForAdmission(
	ctx context.Context,
	ticket, dsPodName, admissionID string,
	admission data.DSAdmissionBinding,
) (*DSTicketClaims, error) {
	if admissionID == "" {
		return nil, errcode.New(errcode.ErrInvalidArg, "admission_id is required")
	}
	if !admission.Complete() {
		return nil, errcode.New(errcode.ErrUnauthorized, "ds admission binding is incomplete")
	}
	return u.verifyDSTicket(ctx, ticket, dsPodName, admissionID, &admission)
}

func (u *TicketUsecase) verifyDSTicket(
	ctx context.Context,
	ticket, dsPodName, admissionID string,
	admission *data.DSAdmissionBinding,
) (*DSTicketClaims, error) {
	h := plog.With(ctx)

	claims, err := u.verifyDSTicketSignature(ticket)
	if err != nil {
		h.Warnw("msg", "verify_ds_ticket_failed", "err", err, "ds_pod", dsPodName)
		return nil, err
	}
	var (
		admissionRepo  data.AdmissionTicketJTIRepo
		markerStatus   data.AdmissionMarkerStatus
		attemptOwner   string
		credentialHash string
	)
	if admission != nil {
		var ok bool
		admissionRepo, ok = u.jtiRepo.(data.AdmissionTicketJTIRepo)
		if !ok || admissionRepo == nil || claims.JTI == "" {
			// §11.3 R2:三个条件合并在一个 if 里,拆开才能区分「部署没接 admission repo」
			// 和「票本身没 jti」—— 前者是配置缺口(恒不可进),后者是单张票的问题。
			reason := admissionRejectRepoUnavailable
			if ok && admissionRepo != nil && claims.JTI == "" {
				reason = admissionRejectTicketNoJTI
			}
			h.Errorw("msg", "ds_ticket_admission_rejected", "reason", reason,
				"player_id", claims.PlayerID, "ds_pod", dsPodName, "ds_type", claims.DSType,
				"admission_id", admissionID)
			return nil, errcode.New(errcode.ErrUnavailable, "ticket admission replay authority unavailable")
		}
		var ownerErr error
		attemptOwner, ownerErr = admission.AdmissionAttemptOwner(admissionID)
		if ownerErr == nil {
			credentialHash, ownerErr = admission.AcceptedCredentialHash()
		}
		if ownerErr != nil {
			// ErrInvalidArg 在 access log 里落 rpc_ok=Debug,线上完全不可见 —— 必须自己打。
			h.Warnw("msg", "ds_ticket_admission_rejected", "reason", admissionRejectMarkerOwnerInvalid,
				"err", ownerErr, "player_id", claims.PlayerID, "ds_pod", dsPodName,
				"ds_type", claims.DSType, "admission_id", admissionID)
			return nil, errcode.NewCause(errcode.ErrInvalidArg, ownerErr, "invalid admission marker owner")
		}
		markerStatus, err = admissionRepo.PeekAdmission(ctx, claims.JTI, attemptOwner)
		if err != nil {
			h.Warnw("msg", "ds_ticket_admission_rejected", "reason", admissionRejectPeekFailed,
				"err", err, "jti", claims.JTI, "player_id", claims.PlayerID,
				"ds_pod", dsPodName, "ds_type", claims.DSType, "admission_id", admissionID)
			return nil, err
		}
		if markerStatus == data.AdmissionMarkerConflict {
			// 票据已属于另一次 admission 的冲突拒绝 = replay / 双准入安全信号(ErrLoginTicketReplayed
			// 是业务码,access log 不当故障)→ 显式 WARN 带 jti/player/ds_pod 便于排查为何 DS 准入被拒。
			h.Warnw("msg", "ds_ticket_replayed", "jti", claims.JTI,
				"player_id", claims.PlayerID, "ds_pod", dsPodName, "ds_type", claims.DSType)
			return nil, errcode.New(errcode.ErrLoginTicketReplayed, "ticket already belongs to another admission")
		}
		if markerStatus == data.AdmissionMarkerMissing {
			err = validateTicketAdmissionStrict(claims, dsPodName, *admission)
		} else {
			err = validateTicketAdmissionRetry(claims, dsPodName, *admission)
		}
		if err != nil {
			h.Warnw("msg", "ds_ticket_admission_binding_rejected", "err", err,
				"player_id", claims.PlayerID, "ds_pod", dsPodName, "ds_type", claims.DSType)
			return nil, err
		}
		if claims.DSType == string(auth.DSTypeHub) {
			admissionChecker, ok := u.assignmentChecker.(data.AdmissionHubAssignmentChecker)
			if !ok || admissionChecker == nil {
				// 部署配置缺口:本部署恒不可进 Hub。玩家侧只看到「进不去」,
				// 没这条就只能去 hub_allocator 侧猜(而那边根本没收到请求)。
				h.Errorw("msg", "ds_ticket_admission_rejected", "reason", admissionRejectHubCheckerUnavailable,
					"player_id", claims.PlayerID, "ds_pod", dsPodName, "admission_id", admissionID)
				return nil, errcode.New(errcode.ErrUnavailable, "hub admission assignment checker unavailable")
			}
			stable := hubBindingFromClaims(claims)
			if claims.Version == auth.DSTicketVersion2 {
				// v2 不携带 callback credential；它只钉稳定实例/assignment，当前
				// credential 已由 admission checker 证明，借它构造 assignment 终态门。
				stable = hubBindingFromAdmission(*admission, claims.HubAssignmentID)
			}
			active := hubBindingFromAdmission(*admission, claims.HubAssignmentID)
			if err := admissionChecker.CheckCurrentAdmission(ctx, claims.PlayerID, stable, active,
				markerStatus == data.AdmissionMarkerMissing); err != nil {
				return nil, err
			}
		}
	} else if claims.Version == auth.DSTicketVersion2 && claims.DSType == string(auth.DSTypeBattle) {
		if u.battleAuthorizer == nil {
			return nil, errcode.New(errcode.ErrUnavailable, "battle ticket roster authority unavailable")
		}
		target, targetErr := u.battleAuthorizer.AuthorizeBattleTicket(ctx, claims.PlayerID, claims.MatchID)
		if targetErr != nil {
			return nil, targetErr
		}
		if field := battleV2BindingMismatchField(dsPodName, claims, target); field != "" {
			// 七个条件原本塌成一句话且**静默 return**:DS 侧只会说「准入被拒」,login 侧
			// 连哪个字段不一致都不记。换 DS 版本(release_track stable→canary)或
			// instance_epoch 递增导致的全服进不去副本,只能靠猜。
			h.Warnw("msg", "ds_ticket_binding_rejected",
				"player_id", claims.PlayerID, "ds_type", claims.DSType, "ds_pod", dsPodName,
				"jti", claims.JTI, "match_id", claims.MatchID, "mismatch_field", field,
				"ticket_pod", claims.DSPodName, "ticket_uid", claims.DSInstanceUID,
				"ticket_instance_epoch", claims.DSInstanceEpoch,
				"ticket_allocation_id", claims.AllocationID, "ticket_release_track", claims.ReleaseTrack,
				"authority_pod", target.PodName, "authority_uid", target.InstanceUID,
				"authority_instance_epoch", target.InstanceEpoch,
				"authority_allocation_id", target.AllocationID, "authority_release_track", target.ReleaseTrack)
			return nil, errcode.New(errcode.ErrLoginTicketInvalid, "battle v2 ticket no longer matches roster authority")
		}
	} else if claims.DSType == string(auth.DSTypeHub) {
		if claims.Version == auth.DSTicketVersion2 {
			checker, ok := u.assignmentChecker.(data.B1HubAssignmentChecker)
			if !ok || checker == nil {
				return nil, errcode.New(errcode.ErrUnavailable, "hub v2 assignment checker unavailable")
			}
			if dsPodName == "" || dsPodName != claims.DSPodName {
				field := "caller_ds_pod"
				if dsPodName == "" {
					field = "caller_ds_pod_empty"
				}
				h.Warnw("msg", "ds_ticket_binding_rejected",
					"player_id", claims.PlayerID, "ds_type", claims.DSType, "ds_pod", dsPodName,
					"jti", claims.JTI, "mismatch_field", field,
					"ticket_pod", claims.DSPodName, "ticket_uid", claims.DSInstanceUID,
					"ticket_instance_epoch", claims.DSInstanceEpoch,
					"ticket_hub_assignment_id", claims.HubAssignmentID,
					"ticket_release_track", claims.ReleaseTrack)
				return nil, errcode.New(errcode.ErrUnauthorized, "hub v2 ticket target pod mismatch")
			}
			if err := checker.CheckCurrentB1(ctx, claims.PlayerID, claims.DSPodName, claims.DSInstanceUID,
				claims.DSInstanceEpoch, claims.HubAssignmentID, claims.ReleaseTrack); err != nil {
				// 归属权威说这张票绑的 assignment/实例已不是当前的(Transfer / Release /
				// 同名 Pod 重建 / 灰度换轨)。err 里有权威侧口径,票内五要件在这里给出。
				h.Warnw("msg", "ds_ticket_binding_rejected", "err", err,
					"player_id", claims.PlayerID, "ds_type", claims.DSType, "ds_pod", dsPodName,
					"jti", claims.JTI, "mismatch_field", "hub_assignment_authority",
					"ticket_pod", claims.DSPodName, "ticket_uid", claims.DSInstanceUID,
					"ticket_instance_epoch", claims.DSInstanceEpoch,
					"ticket_hub_assignment_id", claims.HubAssignmentID,
					"ticket_release_track", claims.ReleaseTrack)
				return nil, err
			}
		} else {
			// off/legacy 保留原有票内 binding 兼容策略。
			binding := hubBindingFromClaims(claims)
			if binding.Complete() {
				if err := u.checkHubAssignment(ctx, claims.PlayerID, dsPodName, binding); err != nil {
					h.Warnw("msg", "ds_ticket_binding_rejected", "err", err,
						"player_id", claims.PlayerID, "ds_type", claims.DSType, "ds_pod", dsPodName,
						"jti", claims.JTI, "mismatch_field", "hub_assignment_authority_legacy",
						"ticket_pod", claims.DSPodName, "ticket_uid", claims.DSInstanceUID,
						"ticket_protocol_epoch", claims.DSProtocolEpoch,
						"ticket_credential_gen", claims.DSCredentialGen,
						"ticket_hub_assignment_id", claims.HubAssignmentID)
					return nil, err
				}
			} else if !binding.Empty() {
				// 部分字段有、部分没有 = 签发面半截升级,比完全没有更危险(不能当兼容旧票放过)。
				h.Warnw("msg", "ds_ticket_binding_rejected",
					"player_id", claims.PlayerID, "ds_type", claims.DSType, "ds_pod", dsPodName,
					"jti", claims.JTI, "mismatch_field", "binding_incomplete",
					"ticket_pod", claims.DSPodName, "ticket_uid", claims.DSInstanceUID,
					"ticket_protocol_epoch", claims.DSProtocolEpoch,
					"ticket_credential_gen", claims.DSCredentialGen,
					"ticket_hub_assignment_id", claims.HubAssignmentID)
				return nil, errcode.New(errcode.ErrLoginTicketInvalid, "hub ticket has incomplete assignment binding")
			} else if u.requireHubAssignmentBinding {
				// 空绑定旧票撞上已激活的 binding 栅栏:滚动窗口没排空干净的典型信号。
				h.Warnw("msg", "ds_ticket_binding_rejected",
					"player_id", claims.PlayerID, "ds_type", claims.DSType, "ds_pod", dsPodName,
					"jti", claims.JTI, "mismatch_field", "binding_missing",
					"hint", "require_hub_assignment_binding 已开,但收到无绑定的旧签发面票据")
				return nil, errcode.New(errcode.ErrLoginTicketInvalid, "hub ticket missing required assignment binding")
			}
		}
	}

	// R7 复审 P2-1:会话现行性复核前置到 replay marker 写入之前。顺序意义:
	// 已被新登录轮换的旧票在此即被拒,不消耗 jti 防重放名额,也不占 admission
	// 短幂等窗;空 sjti 由 login.require_ticket_sjti 门控制(R8 收口,P0-5 滚动兼容:
	// 默认兼容档告警放行,签发面排空 + 等满票据最大 TTL 后开门硬拒)。service 层
	// 响应写出前还有终检。
	if u.sessionGate != nil {
		if err := u.sessionGate.RequireTicketSessionCurrent(ctx, claims.PlayerID, claims.SessJTI); err != nil {
			h.Warnw("msg", "ds_ticket_session_gate_rejected_pre_marker",
				"player_id", claims.PlayerID, "ds_pod", dsPodName, "jti", claims.JTI, "err", err)
			return nil, err
		}
	}

	// W3 ②:防重放。legacy/off 保持原始单次 SETNX；Redis authority 每次都用 Lua
	// 原子确认短幂等窗：missing→marker，existing same-attempt 仅确认、不覆盖/不续 TTL；
	// 这也封住 Peek 后验证期间刚好跨出 replay_until 的竞态。
	if admission != nil {
		status, markErr := admissionRepo.MarkUsedByAdmission(
			ctx, claims.JTI, attemptOwner, credentialHash, claims.ReplayTTL)
		if markErr != nil {
			h.Warnw("msg", "ds_ticket_admission_replay_blocked",
				"jti", claims.JTI, "player_id", claims.PlayerID, "ds_pod", dsPodName, "err", markErr)
			return nil, markErr
		}
		if status != data.AdmissionMarkerCreated && status != data.AdmissionMarkerExisting {
			// 与 PeekAdmission 的 Conflict 同类(双准入 / 重放安全信号),但发生在原子
			// MarkUsedByAdmission 这一步:Peek 之后、Mark 之前有别的 admission 抢占了这张票。
			h.Warnw("msg", "ds_ticket_admission_marker_conflict",
				"jti", claims.JTI, "player_id", claims.PlayerID, "ds_pod", dsPodName,
				"ds_type", claims.DSType, "admission_id", admissionID, "marker_status", int(status))
			return nil, errcode.New(errcode.ErrLoginTicketReplayed, "ticket admission marker conflict")
		}
	} else if admission == nil && u.jtiRepo != nil && claims.JTI != "" {
		if err := u.jtiRepo.MarkUsed(ctx, claims.JTI, claims.ReplayTTL); err != nil {
			h.Warnw("msg", "ds_ticket_replay_blocked",
				"jti", claims.JTI, "player_id", claims.PlayerID, "ds_pod", dsPodName, "err", err)
			return nil, err
		}
	}

	h.Debugw("msg", "ds_ticket_verified",
		"player_id", claims.PlayerID,
		"ds_type", claims.DSType, "match_id", claims.MatchID,
		"jti", claims.JTI, "ds_pod", dsPodName)

	out := &DSTicketClaims{
		Version:         claims.Version,
		PlayerID:        claims.PlayerID,
		MatchID:         claims.MatchID,
		DSType:          claims.DSType,
		JTI:             claims.JTI,
		IssuedAtMs:      claims.IssuedAtMs,
		ExpiresAtMs:     claims.ExpiresAtMs,
		RegionID:        claims.RegionID,
		CellID:          claims.CellID,
		RoleID:          claims.RoleID,
		DSPodName:       claims.DSPodName,
		DSInstanceUID:   claims.DSInstanceUID,
		DSProtocolEpoch: claims.DSProtocolEpoch,
		DSCredentialGen: claims.DSCredentialGen,
		DSCredentialJTI: claims.DSCredentialJTI,
		HubAssignmentID: claims.HubAssignmentID,
		DSWriterEpoch:   claims.DSWriterEpoch,
		DSInstanceEpoch: claims.DSInstanceEpoch,
		AllocationID:    claims.AllocationID,
		ReleaseTrack:    claims.ReleaseTrack,
		SessJTI:         claims.SessJTI,
	}
	return out, nil
}

func (u *TicketUsecase) verifyDSTicketSignature(ticket string) (*verifiedDSTicket, error) {
	alg, err := auth.DSTicketAlgorithm(ticket)
	if err != nil {
		return nil, err
	}
	switch alg {
	case "HS256":
		// B1 不是按票据 alg 做迁移兼容：一旦本进程装了任一 v2 组件，玩家票就必须
		// 全部是 RS256。SessionToken 仍由独立的 VerifySession HS256 路径处理。
		if u.rs256DSTicketProfileEnabled() {
			return nil, errcode.New(errcode.ErrLoginTicketInvalid,
				"legacy HS256 DSTicket is disabled by the RS256 profile")
		}
		if u == nil || u.verifier == nil {
			return nil, errcode.New(errcode.ErrUnavailable, "legacy DSTicket verifier unavailable")
		}
		claims, err := u.verifier.VerifyDSTicket(ticket)
		if err != nil {
			return nil, err
		}
		out := &verifiedDSTicket{
			Version: 1, PlayerID: claims.PlayerID(), MatchID: claims.MatchID, DSType: claims.DSType,
			JTI: claims.ID, ReplayTTL: u.verifier.DSTicketTTL(), RegionID: claims.RegionID,
			CellID: claims.CellID, RoleID: claims.RoleID, DSPodName: claims.DSPodName,
			DSInstanceUID: claims.DSInstanceUID, HubAssignmentID: claims.HubAssignmentID,
			DSProtocolEpoch: claims.DSProtocolEpoch, DSCredentialGen: claims.DSCredentialGen,
			DSCredentialJTI: claims.DSCredentialJTI, DSWriterEpoch: claims.DSWriterEpoch,
		}
		if claims.IssuedAt != nil {
			out.IssuedAtMs = claims.IssuedAt.UnixMilli()
		}
		if claims.ExpiresAt != nil {
			out.ExpiresAtMs = claims.ExpiresAt.UnixMilli()
		}
		return out, nil
	case "RS256":
		if u == nil || u.v2Verifier == nil {
			return nil, errcode.New(errcode.ErrUnavailable, "DSTicket v2 verifier unavailable")
		}
		claims, err := u.v2Verifier.Verify(ticket)
		if err != nil {
			return nil, err
		}
		out := &verifiedDSTicket{
			Version: auth.DSTicketVersion2, PlayerID: claims.PlayerID(), MatchID: claims.MatchID,
			DSType: claims.DSType, JTI: claims.ID, ReplayTTL: auth.DSTicketMaxTTL,
			RegionID: claims.RegionID, CellID: claims.CellID, RoleID: claims.RoleID,
			DSPodName: claims.DSPodName, DSInstanceUID: claims.DSInstanceUID,
			DSInstanceEpoch: claims.DSInstanceEpoch, ReleaseTrack: claims.ReleaseTrack,
			AllocationID: claims.AllocationID, HubAssignmentID: claims.HubAssignmentID,
			SessJTI: claims.SessJTI, // R6 P0-3:兑换点会话复核用
		}
		if claims.IssuedAt != nil {
			out.IssuedAtMs = claims.IssuedAt.UnixMilli()
		}
		if claims.ExpiresAt != nil {
			out.ExpiresAtMs = claims.ExpiresAt.UnixMilli()
		}
		return out, nil
	default:
		return nil, errcode.New(errcode.ErrLoginTicketInvalid, "DSTicket algorithm unsupported")
	}
}

// validateTicketAdmissionStrict 用于 marker 不存在的首次准入，必须在 MarkUsed 前把
// 玩家票内完整 Hub credential 或 Battle match 与本次 caller active 精确绑定。
// Hub 票还需把票内 assignment/active tuple 与调用者 active 精确绑定；Battle 票没有
// assignment 字段，必须把 ticket.match_id 与 caller active.match_id 精确绑定。
func validateTicketAdmissionStrict(claims *verifiedDSTicket, dsPodName string, admission data.DSAdmissionBinding) error {
	if claims == nil || !admission.Complete() || dsPodName == "" || dsPodName != admission.PodName ||
		claims.DSType != string(admission.DSType) {
		return errcode.New(errcode.ErrLoginTicketInvalid, "ds ticket caller type or pod mismatch")
	}
	if claims.Version == auth.DSTicketVersion2 {
		if claims.DSPodName != admission.PodName || claims.DSInstanceUID != admission.InstanceUID ||
			claims.DSInstanceEpoch != admission.ProtocolEpoch || claims.ReleaseTrack == "" ||
			claims.ReleaseTrack != admission.ReleaseTrack {
			return errcode.New(errcode.ErrLoginTicketInvalid, "v2 ticket stable instance binding mismatch")
		}
		switch admission.DSType {
		case auth.DSTypeHub:
			if claims.MatchID != 0 || admission.MatchID != 0 || claims.HubAssignmentID == "" {
				return errcode.New(errcode.ErrLoginTicketInvalid, "hub v2 ticket assignment binding invalid")
			}
		case auth.DSTypeBattle:
			if claims.MatchID == 0 || claims.MatchID != admission.MatchID || claims.AllocationID == "" ||
				claims.AllocationID != admission.AllocationID || !slices.Contains(admission.PlayerIDs, claims.PlayerID) {
				return errcode.New(errcode.ErrLoginTicketInvalid, "battle v2 ticket authority binding mismatch")
			}
		default:
			return errcode.New(errcode.ErrLoginTicketInvalid, "ds ticket admission type invalid")
		}
		return nil
	}
	switch admission.DSType {
	case auth.DSTypeHub:
		if claims.MatchID != 0 || admission.MatchID != 0 ||
			claims.DSPodName != admission.PodName || claims.DSInstanceUID != admission.InstanceUID ||
			claims.DSProtocolEpoch != admission.ProtocolEpoch || claims.DSCredentialGen != admission.CredentialGen ||
			claims.DSCredentialJTI != admission.CredentialJTI || claims.DSWriterEpoch != admission.WriterEpoch {
			return errcode.New(errcode.ErrLoginTicketInvalid, "hub ticket does not match caller active credential")
		}
	case auth.DSTypeBattle:
		if claims.MatchID == 0 || claims.MatchID != admission.MatchID ||
			!slices.Contains(admission.PlayerIDs, claims.PlayerID) {
			return errcode.New(errcode.ErrLoginTicketInvalid, "battle ticket match does not match caller active credential")
		}
	default:
		return errcode.New(errcode.ErrLoginTicketInvalid, "ds ticket admission type invalid")
	}
	return nil
}

// validateTicketAdmissionRetry 仅在 Redis 已存在同 attempt_owner marker 时使用。
// 普通 token 轮换允许 gen/jti/exp/kid/hash 变化；稳定身份(type/match/pod/UID/instance
// epoch/writer)与 Hub assignment_id 仍必须一致。caller 当前 active/projection 已由 service 先验。
func validateTicketAdmissionRetry(claims *verifiedDSTicket, dsPodName string, admission data.DSAdmissionBinding) error {
	if claims == nil || !admission.Complete() || dsPodName == "" || dsPodName != admission.PodName ||
		claims.DSType != string(admission.DSType) {
		return errcode.New(errcode.ErrLoginTicketInvalid, "ds ticket retry caller type or pod mismatch")
	}
	if claims.Version == auth.DSTicketVersion2 {
		// B1 票据不绑定普通 callback credential，重试仍必须精确钉住稳定实例、
		// allocation/assignment 与实际 release track。
		return validateTicketAdmissionStrict(claims, dsPodName, admission)
	}
	switch admission.DSType {
	case auth.DSTypeHub:
		if claims.MatchID != 0 || admission.MatchID != 0 || claims.HubAssignmentID == "" ||
			claims.DSPodName != admission.PodName || claims.DSInstanceUID != admission.InstanceUID ||
			claims.DSProtocolEpoch != admission.ProtocolEpoch || claims.DSWriterEpoch != admission.WriterEpoch {
			return errcode.New(errcode.ErrLoginTicketInvalid, "hub ticket retry stable identity mismatch")
		}
	case auth.DSTypeBattle:
		if claims.MatchID == 0 || claims.MatchID != admission.MatchID ||
			!slices.Contains(admission.PlayerIDs, claims.PlayerID) {
			return errcode.New(errcode.ErrLoginTicketInvalid, "battle ticket retry match mismatch")
		}
	default:
		return errcode.New(errcode.ErrLoginTicketInvalid, "ds ticket retry admission type invalid")
	}
	return nil
}

func hubBindingFromClaims(claims *verifiedDSTicket) data.HubAssignmentBinding {
	if claims == nil {
		return data.HubAssignmentBinding{}
	}
	return data.HubAssignmentBinding{
		PodName: claims.DSPodName, InstanceUID: claims.DSInstanceUID,
		ProtocolEpoch: claims.DSProtocolEpoch, CredentialGen: claims.DSCredentialGen,
		CredentialJTI: claims.DSCredentialJTI, AssignmentID: claims.HubAssignmentID,
		WriterEpoch: claims.DSWriterEpoch, ReleaseTrack: claims.ReleaseTrack,
	}
}

func hubBindingFromAdmission(admission data.DSAdmissionBinding, assignmentID string) data.HubAssignmentBinding {
	return data.HubAssignmentBinding{
		PodName: admission.PodName, InstanceUID: admission.InstanceUID,
		ProtocolEpoch: admission.ProtocolEpoch, CredentialGen: admission.CredentialGen,
		CredentialJTI: admission.CredentialJTI, AssignmentID: assignmentID,
		WriterEpoch: admission.WriterEpoch, ExpMs: admission.ExpMs, Kid: admission.Kid,
		TokenSHA256: admission.TokenSHA256, ReleaseTrack: admission.ReleaseTrack,
	}
}

// battleV2BindingMismatchField 逐项核对 battle v2 票据与 roster 权威的绑定七要件,
// 返回**第一个**不一致的字段名;全部一致返回 ""。
//
// 七个条件与判定语义和历史版本(塌成一句 if 的写法)完全一致,拆开只为可观测:
// 调用方把返回的字段名连同票内/权威侧两份值一起打进 ds_ticket_binding_rejected,
// 否则换 DS 版本(release_track)或 instance_epoch 递增导致的全服拒入只能靠猜。
// 顺序有意义:先验调用方身份(caller pod),再验票据与权威的实例五要件——
// 前者不匹配时后者的对比值没有意义。
func battleV2BindingMismatchField(dsPodName string, claims *verifiedDSTicket, target data.BattleTicketTarget) string {
	switch {
	case dsPodName == "":
		return "caller_ds_pod_empty"
	case dsPodName != claims.DSPodName:
		return "caller_ds_pod"
	case target.PodName != claims.DSPodName:
		return "pod_name"
	case target.InstanceUID != claims.DSInstanceUID:
		return "instance_uid"
	case target.InstanceEpoch != claims.DSInstanceEpoch:
		return "instance_epoch"
	case target.AllocationID != claims.AllocationID:
		return "allocation_id"
	case target.ReleaseTrack != claims.ReleaseTrack:
		return "release_track"
	}
	return ""
}

func (u *TicketUsecase) checkHubAssignment(
	ctx context.Context,
	playerID uint64,
	dsPodName string,
	binding data.HubAssignmentBinding,
) error {
	if !binding.Complete() {
		return errcode.New(errcode.ErrLoginTicketInvalid, "hub ticket assignment binding incomplete")
	}
	if dsPodName == "" || dsPodName != binding.PodName {
		return errcode.New(errcode.ErrUnauthorized, "hub ticket target pod mismatch")
	}
	if u.assignmentChecker == nil {
		return errcode.New(errcode.ErrUnavailable, "hub assignment checker unavailable")
	}
	return u.assignmentChecker.CheckCurrent(ctx, playerID, binding)
}
