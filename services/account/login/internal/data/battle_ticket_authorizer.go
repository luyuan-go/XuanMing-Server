// battle_ticket_authorizer.go 在 login 签发 battle DSTicket 前证明 player 属于目标 match。
// 这是签发线性化门，不修改 Redis；local/off 也必须经过 roster，不能因关闭在线 admission
// 就退化成“知道 match_id 即可拿票”。
package data

import (
	"context"
	"slices"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/placement"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
)

// BattleTicketTarget 是本次 roster 授权读取同一 Redis 快照得到的可路由目标。
// Login reconnect 必须使用这里的 DSAddr，不能在证明当前 projection 后又回退使用可能陈旧的 locator 地址。
type BattleTicketTarget struct {
	DSAddr        string
	PodName       string
	InstanceUID   string
	InstanceEpoch uint32
	// AllocationID 是本局分配 ID(DSTicket v2 allocation_id claim;旧记录可能为空,
	// v2 签发侧对空值 fail-closed 拒签)。
	AllocationID string
	ReleaseTrack string
}

// BattleTicketAuthorizer 是 battle DSTicket 的签发前 player↔match 权威门。
type BattleTicketAuthorizer interface {
	AuthorizeBattleTicket(context.Context, uint64, uint64) (BattleTicketTarget, error)
}

// BattleRouteState 是 Hub 签票门的显式三态判定结果(P0 修复 2026-07-15):
// 通用 ErrPermissionDeny 不得再被当作“对局已终态”的证明——它同时覆盖
// roster 漂移/非成员/记录缺失/stale 心跳,那些都只能是 UNKNOWN。
type BattleRouteState int

const (
	// BattleRouteUnknown:无法权威判定(记录缺失/非成员漂移/stale 心跳/Redis 错误)。
	// 调用方在 locator 阳性 BATTLE 信号下必须 fail-closed。
	BattleRouteUnknown BattleRouteState = iota
	// BattleRouteActive:玩家确属 live 对局(ready/running + 成员 + 心跳新鲜)。
	BattleRouteActive
	// BattleRouteTerminal:权威记录显式终态(ended/abandoned)——唯一允许 Hub 的证明。
	BattleRouteTerminal
)

// BattleRouteInspector 是可选能力接口:Hub 签票门用它区分“仍在活局/已终局/不可判定”。
// 未实现本接口的 authorizer 一律按 UNKNOWN fail-closed。
type BattleRouteInspector interface {
	InspectBattleRoute(ctx context.Context, playerID, matchID uint64) (BattleRouteState, error)
}

// 战斗投影记录的显式终态(与 ds_allocator 状态机常量一致;TerminateExpected 写入)。
const (
	battleStateEnded     = "ended"
	battleStateAbandoned = "abandoned"
)

type RedisBattleTicketAuthorizer struct {
	rdb             redis.UniversalClient
	requireModelB   bool
	now             func() time.Time
	maxHeartbeatAge time.Duration
}

func NewRedisBattleTicketAuthorizer(
	rdb redis.UniversalClient,
	requireModelB bool,
	maxHeartbeatAge time.Duration,
) *RedisBattleTicketAuthorizer {
	if maxHeartbeatAge <= 0 {
		maxHeartbeatAge = 30 * time.Second
	}
	return &RedisBattleTicketAuthorizer{
		rdb: rdb, requireModelB: requireModelB, now: time.Now, maxHeartbeatAge: maxHeartbeatAge,
	}
}

// AuthorizeBattleTicket 的读取时刻是本次签发授权线性化点。Redis 不可判定返回 Unavailable；
// 非成员、空 roster、非 live/stale 或 Model-B 漂移返回 PermissionDeny，绝不签票。
func (c *RedisBattleTicketAuthorizer) AuthorizeBattleTicket(
	ctx context.Context,
	playerID, matchID uint64,
) (BattleTicketTarget, error) {
	if playerID == 0 || matchID == 0 {
		return BattleTicketTarget{}, errcode.New(errcode.ErrInvalidArg, "battle ticket authorization requires player and match")
	}
	if c == nil || c.rdb == nil || c.now == nil {
		return BattleTicketTarget{}, errcode.New(errcode.ErrUnavailable, "battle ticket roster authority unavailable")
	}
	if c.requireModelB {
		return c.authorizeModelB(ctx, playerID, matchID)
	}
	payload, err := c.rdb.Get(ctx, admissionBattleProjectionKey(matchID)).Bytes()
	if err == redis.Nil {
		return BattleTicketTarget{}, errcode.New(errcode.ErrPermissionDeny, "battle ticket target is not live")
	}
	if err != nil {
		return BattleTicketTarget{}, errcode.NewCause(errcode.ErrUnavailable, err, "read battle ticket roster failed")
	}
	battle := &dsv1.BattleStorageRecord{}
	if err := proto.Unmarshal(payload, battle); err != nil {
		return BattleTicketTarget{}, errcode.NewCause(errcode.ErrUnavailable, err, "decode battle ticket roster failed")
	}
	if reason := c.liveRosterDenyReason(battle, playerID, matchID); reason != "" {
		// reason 进错误串:调用方(login authorize_battle_reconnect_ticket_failed ERROR)带 err
		// 落盘,否则七个条件塌成一句话,「不在 roster/心跳 stale/状态不对」无法分诊。
		return BattleTicketTarget{}, errcode.New(errcode.ErrPermissionDeny,
			"player is not authorized for battle ticket target: %s", reason)
	}
	return battleTicketTarget(battle), nil
}

// InspectBattleRoute 是 Hub 签票门的显式三态权威判定(P0 修复 2026-07-15):
//
//	TERMINAL:投影记录存在、match_id 一致且 state ∈ {ended, abandoned}——唯一允许 Hub 的证明。
//	ACTIVE  :liveRosterAllows(ready/running + 成员 + 心跳新鲜)。
//	UNKNOWN :其余一切——记录缺失(redis.Nil,可能是终局清理也可能是 TTL 漂移,不可区分)、
//	         match_id 不匹配、running 但玩家非成员(roster 漂移)、stale 心跳(DS 可能崩溃)、
//	         Redis/解码错误。调用方必须 fail-closed。
//
// 注意与 AuthorizeBattleTicket 的区别:后者把上述 UNKNOWN 情形折叠进 ErrPermissionDeny
// (签票语义"不给票"正确),但作为 Hub 放行证明会把 roster 漂移误判成终局(Codex 复审 P0)。
func (c *RedisBattleTicketAuthorizer) InspectBattleRoute(
	ctx context.Context,
	playerID, matchID uint64,
) (BattleRouteState, error) {
	if playerID == 0 || matchID == 0 {
		return BattleRouteUnknown, errcode.New(errcode.ErrInvalidArg, "battle route inspection requires player and match")
	}
	if c == nil || c.rdb == nil || c.now == nil {
		return BattleRouteUnknown, errcode.New(errcode.ErrUnavailable, "battle route authority unavailable")
	}
	payload, err := c.rdb.Get(ctx, admissionBattleProjectionKey(matchID)).Bytes()
	if err == redis.Nil {
		// 记录缺失 ≠ 终态:可能是终局后清理,也可能是 DS 续期失败导致 TTL 漂移(活局仍在)。
		// 没有版本化 placement lease 前无法区分,一律 UNKNOWN。
		return BattleRouteUnknown, errcode.New(errcode.ErrUnavailable,
			"battle projection missing; cannot prove match %d is terminal", matchID)
	}
	if err != nil {
		return BattleRouteUnknown, errcode.NewCause(errcode.ErrUnavailable, err, "read battle route projection failed")
	}
	battle := &dsv1.BattleStorageRecord{}
	if err := proto.Unmarshal(payload, battle); err != nil {
		return BattleRouteUnknown, errcode.NewCause(errcode.ErrUnavailable, err, "decode battle route projection failed")
	}
	if battle.GetMatchId() != matchID {
		return BattleRouteUnknown, errcode.New(errcode.ErrUnavailable,
			"battle projection match mismatch (want %d got %d)", matchID, battle.GetMatchId())
	}
	if battle.GetState() == battleStateEnded {
		// ended = DS 自己上报的正常终局:DS 已按结算流程收尾,无脑裂窗口,立即放行。
		return BattleRouteTerminal, nil
	}
	if battle.GetState() == battleStateAbandoned {
		// abandoned = 心跳超时判死(补偿性终态),DS 可能只是与后端分区、其上玩家仍可玩。
		// 脑裂再入屏障(2026-07-16,pkg/placement 契约):必须等旧 DS 的授权租约上限 +
		// 偏差余量过去(它届时已对存量玩家自我 fencing),才能把玩家放去 Hub/新局。
		// LastHeartbeatMs==0 = 从未有过成功心跳:DS 从未取得授权租约,其准入门从未打开,
		// 不可能有玩家在其上,立即 Terminal 安全。
		if last := battle.GetLastHeartbeatMs(); last > 0 {
			if wait := last + placement.DSFenceReentryBarrier.Milliseconds() - c.now().UnixMilli(); wait > 0 {
				return BattleRouteUnknown, errcode.New(errcode.ErrUnavailable,
					"abandoned battle %d is inside the DS fence re-entry barrier (%dms left); retry", matchID, wait)
			}
		}
		return BattleRouteTerminal, nil
	}
	if c.liveRosterAllows(battle, playerID, matchID) {
		return BattleRouteActive, nil
	}
	// 非终态且非可证明 live:running 但非成员(漂移)/stale 心跳/warming 等中间态,全部不可判定。
	return BattleRouteUnknown, errcode.New(errcode.ErrUnavailable,
		"battle route not provably terminal (state=%q)", battle.GetState())
}

func (c *RedisBattleTicketAuthorizer) authorizeModelB(
	ctx context.Context,
	playerID, matchID uint64,
) (BattleTicketTarget, error) {
	values, err := c.rdb.MGet(ctx,
		admissionBattleAuthKey(matchID), admissionBattleProjectionKey(matchID)).Result()
	if err != nil {
		return BattleTicketTarget{}, errcode.NewCause(errcode.ErrUnavailable, err, "read battle ticket authority failed")
	}
	if len(values) != 2 || values[0] == nil || values[1] == nil {
		return BattleTicketTarget{}, errcode.New(errcode.ErrPermissionDeny, "battle ticket authority is not active")
	}
	authRaw, err := admissionRedisBytes(values[0])
	if err != nil {
		return BattleTicketTarget{}, errcode.NewCause(errcode.ErrUnavailable, err, "decode battle ticket auth value failed")
	}
	battleRaw, err := admissionRedisBytes(values[1])
	if err != nil {
		return BattleTicketTarget{}, errcode.NewCause(errcode.ErrUnavailable, err, "decode battle ticket projection value failed")
	}
	record := &dsv1.BattleDSAuthStorageRecord{}
	battle := &dsv1.BattleStorageRecord{}
	if err := proto.Unmarshal(authRaw, record); err != nil {
		return BattleTicketTarget{}, errcode.NewCause(errcode.ErrUnavailable, err, "decode battle ticket auth failed")
	}
	if err := proto.Unmarshal(battleRaw, battle); err != nil {
		return BattleTicketTarget{}, errcode.NewCause(errcode.ErrUnavailable, err, "decode battle ticket projection failed")
	}
	if reason := c.modelBDenyReason(record, battle, playerID, matchID); reason != "" {
		// 同 liveRosterDenyReason:约 25 个条件的合取,首个不满足的条件名必须可见,
		// 处置方向(查 matchmaker 数据 / 等心跳自愈 / 查 DS 凭据轮换)截然不同。
		return BattleTicketTarget{}, errcode.New(errcode.ErrPermissionDeny,
			"battle ticket authority or roster is not routable: %s", reason)
	}
	return battleTicketTarget(battle), nil
}

func (c *RedisBattleTicketAuthorizer) liveRosterAllows(
	battle *dsv1.BattleStorageRecord,
	playerID, matchID uint64,
) bool {
	return c.liveRosterDenyReason(battle, playerID, matchID) == ""
}

// liveRosterDenyReason 返回 live roster 门第一个不满足的条件名("" = 通过)。
// 判定语义与历史 liveRosterAllows 完全等价,只是把合取拆成可落盘的 reason 枚举
// (§11.3 R2:拒绝必须带固定枚举 reason,一句话塌缩的 ErrPermissionDeny 无法分诊)。
func (c *RedisBattleTicketAuthorizer) liveRosterDenyReason(
	battle *dsv1.BattleStorageRecord,
	playerID, matchID uint64,
) string {
	switch {
	case battle == nil:
		return "projection_missing"
	case battle.GetMatchId() != matchID:
		return "projection_match_id_mismatch"
	case battle.GetDsPodName() == "":
		return "projection_ds_pod_empty"
	case battle.GetDsAddr() == "":
		return "projection_ds_addr_empty"
	case battle.GetState() != "ready" && battle.GetState() != "running":
		return "state_not_live(" + battle.GetState() + ")"
	case len(battle.GetPlayerIds()) == 0:
		return "roster_empty"
	case !slices.Contains(battle.GetPlayerIds(), playerID):
		return "player_not_in_roster"
	case !ticketHeartbeatFresh(battle.GetLastHeartbeatMs(), c.now().UnixMilli(), c.heartbeatAgeLimit()):
		return "heartbeat_stale"
	}
	return ""
}

func battleTicketTarget(battle *dsv1.BattleStorageRecord) BattleTicketTarget {
	if battle == nil {
		return BattleTicketTarget{}
	}
	return BattleTicketTarget{
		DSAddr: battle.GetDsAddr(), PodName: battle.GetDsPodName(),
		InstanceUID: battle.GetGameserverUid(), InstanceEpoch: battle.GetInstanceEpoch(),
		AllocationID: battle.GetAllocationId(), ReleaseTrack: battle.GetReleaseTrack(),
	}
}

func (c *RedisBattleTicketAuthorizer) modelBAllows(
	record *dsv1.BattleDSAuthStorageRecord,
	battle *dsv1.BattleStorageRecord,
	playerID, matchID uint64,
) bool {
	return c.modelBDenyReason(record, battle, playerID, matchID) == ""
}

// modelBDenyReason 返回 Model-B 授权门第一个不满足的条件名("" = 通过)。
// 条件与历史 modelBAllows 的合取逐项等价、顺序不变;拆开的唯一目的是让约 25 个条件
// 的拒签可分诊(roster 数据 / 心跳 / DS 凭据轮换 / 实例漂移是完全不同的处置方向)。
func (c *RedisBattleTicketAuthorizer) modelBDenyReason(
	record *dsv1.BattleDSAuthStorageRecord,
	battle *dsv1.BattleStorageRecord,
	playerID, matchID uint64,
) string {
	if reason := c.liveRosterDenyReason(battle, playerID, matchID); reason != "" {
		return reason
	}
	if record == nil {
		return "auth_record_missing"
	}
	active := record.GetActive()
	nowMs := c.now().UnixMilli()
	switch {
	case active == nil:
		return "auth_active_credential_missing"
	case record.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE &&
		record.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING:
		return "auth_phase_not_active(" + record.GetPhase().String() + ")"
	case record.GetMatchId() != matchID:
		return "auth_match_id_mismatch"
	case record.GetAllocationId() == "":
		return "auth_allocation_id_empty"
	case record.GetAllocationId() != battle.GetAllocationId():
		return "allocation_id_mismatch"
	case record.GetInstanceUid() == "" || record.GetInstanceEpoch() == 0:
		return "auth_instance_identity_missing"
	case battle.GetGameserverUid() == "" || battle.GetInstanceEpoch() == 0:
		return "projection_instance_identity_missing"
	case record.GetDsPodName() != battle.GetDsPodName():
		return "instance_pod_mismatch"
	case record.GetInstanceUid() != battle.GetGameserverUid():
		return "instance_uid_mismatch"
	case record.GetInstanceEpoch() != battle.GetInstanceEpoch():
		return "instance_epoch_mismatch"
	case record.GetRequiredWriterEpoch() != auth.DSAuthWriterEpochV2:
		return "required_writer_epoch_not_v2"
	case record.GetPending() != nil && record.GetPending().GetWriterEpoch() != auth.DSAuthWriterEpochV2:
		return "pending_writer_epoch_not_v2"
	case record.GetHighWaterGen() < active.GetGen():
		return "high_water_below_active_gen"
	case !ticketHeartbeatFresh(record.GetLastActiveHeartbeatMs(), nowMs, c.heartbeatAgeLimit()):
		return "auth_heartbeat_stale"
	case battle.GetLastHeartbeatMs() != record.GetLastActiveHeartbeatMs():
		return "heartbeat_anchor_mismatch"
	case active.GetInstanceUid() == "" || active.GetInstanceEpoch() == 0:
		return "active_instance_identity_missing"
	case active.GetInstanceUid() != record.GetInstanceUid() || active.GetInstanceEpoch() != record.GetInstanceEpoch():
		return "active_instance_mismatch"
	case active.GetGen() == 0:
		return "active_gen_zero"
	case active.GetJti() == "":
		return "active_jti_empty"
	case active.GetExpMs() <= uint64(nowMs):
		return "active_credential_expired"
	case active.GetKid() == "":
		return "active_kid_empty"
	case active.GetTokenSha256() == "":
		return "active_token_hash_empty"
	case active.GetWriterEpoch() != auth.DSAuthWriterEpochV2:
		return "active_writer_epoch_not_v2"
	case battle.GetLastVerifiedGen() != active.GetGen():
		return "projection_verified_gen_stale"
	case battle.GetLastVerifiedJti() != active.GetJti():
		return "projection_verified_jti_stale"
	case battle.GetLastVerifiedWriterEpoch() != auth.DSAuthWriterEpochV2:
		return "projection_verified_writer_epoch_not_v2"
	}
	return ""
}

func (c *RedisBattleTicketAuthorizer) heartbeatAgeLimit() time.Duration {
	if c.maxHeartbeatAge <= 0 {
		return 30 * time.Second
	}
	return c.maxHeartbeatAge
}

func ticketHeartbeatFresh(value, nowMs int64, maxAge time.Duration) bool {
	return value > 0 && value <= nowMs && nowMs-value <= maxAge.Milliseconds()
}
