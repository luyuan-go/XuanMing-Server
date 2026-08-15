// diag.go — team 链路可诊断性的共用件(infra.md §11.3,2026-08-13)。
//
// # 为什么单独一个文件
//
// §11.3 的四条硬规则里有两条是「跨方法一致」才有意义的:
//   - R2 要求拒绝分支带**枚举** reason —— 枚举必须只有一份定义,散在各方法里写字面量
//     等于没有枚举(同一个原因迟早出现 team_full / full / TEAM_FULL 三种写法,看板聚合不了);
//   - 「状态机每次 from→to 都有日志」要求所有写路径用**同一个 msg**,否则查一支队伍的
//     状态轨迹得先知道它走过哪几个 RPC。
//
// 因此 reason 枚举与两个日志助手收在本文件,team.go / offline_leave.go 只负责在
// 各自的分支上引用它们。本文件不含任何业务判定,只是日志形状的单一出处。
package biz

import (
	"context"
	"strconv"
	"strings"

	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"

	plog "github.com/luyuancpp/pandora/pkg/log"
)

// ── R2 枚举 reason ────────────────────────────────────────────────────────────
//
// 取值一律 snake_case,且**一个 reason 只对应一个 if 条件**。
// 一个 if 里收敛了 N 个条件的(例如 `!found || disbanded || !hasMember`),
// 必须拆成 N 个 reason —— 否则"被拒了"查得到、"为什么被拒"查不到。
const (
	// 队伍存在性与状态。
	reasonTeamNotFound      = "team_not_found"
	reasonTeamDisbanded     = "team_disbanded"
	reasonTeamFull          = "team_full"
	reasonTeamNotRecruiting = "team_not_recruiting"
	reasonStateNotAllowed   = "team_state_not_allowed"

	// 成员与权限。
	reasonNotMember        = "player_not_in_team"
	reasonAlreadyInTeam    = "player_already_in_team"
	reasonNotCaptain       = "player_not_captain"
	reasonCaptainSelfKick  = "captain_cannot_kick_self"
	reasonInviterNotMember = "inviter_not_in_team"
	reasonIndexNonMember   = "player_index_points_to_non_member"

	// 邀请 / 申请令牌。
	reasonInviteNotFound         = "invite_not_found_or_expired"
	reasonInviteTargetMismatch   = "invite_target_mismatch"
	reasonInviteTeamMismatch     = "invite_team_mismatch"
	reasonInviteLookupFailed     = "invite_lookup_failed"
	reasonInviteStoreFailed      = "invite_store_failed"
	reasonApplicationNotFound    = "application_not_found_or_expired"
	reasonApplicationStoreFailed = "application_store_failed"
	// 写入侧总量上限拒绝(§9 不变量 18)。玩家侧表现是「加不进去 / 申请不了」,
	// 与「Redis 写失败」必须分开,否则查不出是真满了还是清理没跑。
	reasonInvitePendingLimit      = "invite_pending_limit_reached"
	reasonApplicationPendingLimit = "application_pending_limit_reached"
	// reasonJoinAfterApplicationConsumed:令牌已取走(不可放回)但入队失败。
	reasonJoinAfterApplicationConsumed = "join_failed_after_application_consumed"

	// 匹配闸门 / 组票租约(与 matchmaker 的共同线性化点)。
	reasonMatchCommitted          = "team_committed_to_match"
	reasonMatchCommitmentUnknown  = "match_commitment_unknown"
	reasonPlayerCommitted         = "player_committed_to_match"
	reasonPlayerCommitmentUnknown = "player_commitment_unknown"
	reasonRosterLocked            = "roster_locked_for_match"
	reasonTeamNotReady            = "team_not_ready"
	reasonReadyGenerationMismatch = "ready_generation_mismatch"
	reasonReadyAlreadyCleared     = "ready_already_cleared"
	reasonLegacyReadyRevoked      = "legacy_ready_revoked"
	// reasonMatchmakerCancelFailed:离队 / 踢人后撤票的跨服务 RPC 失败。
	reasonMatchmakerCancelFailed = "matchmaker_cancel_rpc_failed"
	// reasonRaceRecheckFailed / reasonRaceTicketFroze:摘人与组票之间 TOCTOU 窗口的两种结局。
	reasonRaceRecheckFailed = "offline_race_recheck_failed"
	reasonRaceTicketFroze   = "match_ticket_froze_removed_member"

	// 入参与配额。
	reasonMissingArg       = "missing_required_arg"
	reasonRateLimited      = "action_rate_limited"
	reasonRateQuotaUnknown = "rate_quota_check_failed"

	// 存储。
	reasonStoreReadFailed  = "store_read_failed"
	reasonStoreWriteFailed = "store_write_failed"
	// reasonOptimisticRetryExhausted 与 data 层 team_update_lock_exhausted 那条的
	// reason 取值必须一致(data 不反向依赖 biz,那边是同值字面量)。
	reasonOptimisticRetryExhausted = "optimistic_retry_exhausted"

	// 推送(加速器,不是权威;失败只影响到达延迟)。
	reasonPushMarshalFailed = "push_payload_marshal_failed"
	reasonPushProduceFailed = "push_produce_failed"

	// 离线判定(offline_leave.go)。
	reasonNoTeam              = "player_has_no_team"
	reasonSingleMemberTeam    = "single_member_team"
	reasonFeatureDisabled     = "offline_leave_disabled"
	reasonNoTeammateToKeep    = "no_teammate_to_keep"
	reasonPresenceObserveFail = "presence_observe_failed"
)

// ── 状态机轨迹 ────────────────────────────────────────────────────────────────

// logTeamStateChanged 打一条队伍状态机迁移日志(§11.3 R1:不可逆状态推进打 INFO)。
//
// msg 固定为 team_state_changed,**所有**写路径共用:查一支队伍这一小时的状态轨迹
// 只需 `msg=team_state_changed team_id=X`,不必先知道它走过哪几个 RPC。
// from == to 时不打 —— 那不是迁移,打了只会把真正的迁移淹掉。
//
// cause 用与触发它的 RPC 同名的 snake_case 词根(create / join / leave / kick /
// set_ready / match_begin / match_end / presence_lost / offline_leave),
// 与该 RPC 自己那条阶段日志成对,便于对齐。
func logTeamStateChanged(
	ctx context.Context,
	teamID uint64,
	from, to teamv1.TeamState,
	cause string,
	readyGeneration uint64,
	members int,
) {
	if from == to {
		return
	}
	plog.With(ctx).Infow("msg", "team_state_changed",
		"team_id", teamID,
		"from_state", from,
		"to_state", to,
		"cause", cause,
		"ready_generation", readyGeneration,
		"members", members)
}

// ── 开局那一刻的成员画像 ──────────────────────────────────────────────────────

// rosterDigest 把一份成员名单压成可读的一维数组,每项形如 `10001:ready=1,hero=7`。
//
// 为什么要它:"队伍人数对不上就开局了"这类故障,事后**唯一**能还原现场的就是
// 冻结名单那一刻每个成员的三元组。只打 member_count 查不出是谁缺席,
// 而每人一条日志会在 500 人 hub 下把同文件的 WARN 冲走(§11.3 R4)——
// 所以压成一条日志里的一个数组字段。
//
// 队伍最多 5 人,拼接开销可忽略;用 strconv 而非 fmt(§11.1 禁 printf 风格)。
//
// ⚠️ **online 不在这里**:玩家在不在线的权威在 player_locator,team 只持有
// 「成员表 + ready 位 + 队伍状态」。缺席排查要把本条与 locator / offlinewatch 侧
// 同 trace_id 的日志并起来看,team 侧不复制那份判定(§9.22 不建影子状态)。
func rosterDigest(members []*teamv1.TeamMemberStorageRecord) []string {
	out := make([]string, 0, len(members))
	for _, m := range members {
		var b strings.Builder
		b.WriteString(strconv.FormatUint(m.GetPlayerId(), 10))
		if m.GetReady() {
			b.WriteString(":ready=1")
		} else {
			b.WriteString(":ready=0")
		}
		b.WriteString(",hero=")
		b.WriteString(strconv.FormatUint(uint64(m.GetHeroId()), 10))
		out = append(out, b.String())
	}
	return out
}

// readyCount 统计名单里挂着 ready 的人数(与 rosterDigest 配套的聚合值,便于告警按数值比对)。
func readyCount(members []*teamv1.TeamMemberStorageRecord) int {
	n := 0
	for _, m := range members {
		if m.GetReady() {
			n++
		}
	}
	return n
}

// logMatchRosterFrozen 记录「这一局是拿哪份名单开的」。
//
// 这是 INC-20260813-001(3v3 打成 3v2)的核心取证点:BeginTeamMatch 在锁内冻结的
// 这份快照就是 matchmaker 建票的输入,票据之后再不会变。事后只要有这一条,就能回答
// "开局那一刻队伍里到底有谁、各自 ready 与否、队伍处于哪个状态、代际是多少"。
//
// reentry=true 表示本次是同一 attempt 的重试,名单来自收据(未再次消费 ready)。
func logMatchRosterFrozen(
	ctx context.Context,
	teamID, captainID uint64,
	attemptID string,
	snapshot *teamv1.TeamStorageRecord,
	reentry bool,
) {
	if snapshot == nil {
		return
	}
	members := snapshot.GetMembers()
	plog.With(ctx).Infow("msg", "team_match_roster_frozen",
		"team_id", teamID,
		"captain_id", captainID,
		"attempt_id", attemptID,
		"reentry", reentry,
		"frozen_state", snapshot.GetState(),
		"ready_generation", snapshot.GetReadyGeneration(),
		"member_count", len(members),
		"ready_count", readyCount(members),
		"roster", rosterDigest(members),
		"hint", "这份名单就是 matchmaker 建票的输入;成员在线与否见 locator 侧同 trace_id 日志")
}
