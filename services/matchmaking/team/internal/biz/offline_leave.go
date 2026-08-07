// offline_leave.go — 队员离线超时自动移出队伍(2026-08-06)。
//
// # 这条链长什么样
//
//	Hub DS Logout ──ReportDisconnect──▶ player_locator(记 last-seen + 发离场事件)
//	                                          │ kafka: pandora.player.presence
//	                                          ▼
//	                          pkg/offlinewatch(排到期 → 到点回查 locator 权威)
//	                                          │ 判定 offline 且已满 threshold
//	                                          ▼
//	                              TeamUsecase.OnPlayerOffline(本文件)
//
// # 三道闸,少一道都会出事
//
//  1. **此刻真的不在线**:由 offlinewatch 回查 locator 得出。locator 查不通一律不动作
//     (§9.22 不确定不得冒充 OFFLINE),本文件不再重复判。
//  2. **整支队伍没被一场对局占住**:自动摘人绝不能拆一支正在打的队伍 —— 那会让还在
//     正常游戏的队友一起受影响。判定走 matchmaker 权威,读不确定就 fail-closed 重试。
//  3. **这名玩家自己没被对局占住**:玩家 travel 去战斗时位置是 MATCHING/BATTLE,
//     第 1 道闸本就拦得住;这道是冗余的第二保险,防的是 locator 与 matchmaker 之间的
//     短暂不一致(位置已掉、票据还在)。
//
// # TOCTOU 已消除(2026-08-06)
//
// 闸 ②③ 读的是 matchmaker 权威、写的是 team 的 Redis,这两步之间曾有一个真实窗口:
// 闸门放行后、`UpdateWithLock` 提交前,队长若刚好点了开始匹配,matchmaker 会把含该
// 离线成员的 roster 冻进票据 —— 人在票据里却已不在队伍里,被拉进一场自己不在场的对局。
//
// 现在 matchmaker 组票改走 `TeamService.BeginTeamMatch`,在 **team 自己的乐观锁内**
// 冻结名单并留下一把秒级自净租约;摘人在**同一把锁内**看到租约就推迟(ErrDeferred)。
// 两个操作因此只能有一个赢,窗口不再存在 —— 不是「后果收敛」,是消除。
// compensateIfCommittedDuringRemoval 作为纵深防御保留(覆盖租约已过期、
// 而 claim 恰在此刻落地的极窄残留),正常路径不会触发。
//
// # 刻意不做的事
//
//   - **正常路径不联动 cancelMatchmaking**。LeaveTeam / Kick 会撤票是因为那是玩家的主动操作;
//     自动摘人只在「队伍没被对局占住」时发生,此时根本没有票可撤,调用它只会平添一次
//     无谓 RPC 和一条误导性日志。**唯一例外**是上面那个 TOCTOU 窗口被命中时的补偿撤票 ——
//     那时票据确实存在,且它里面那份成员快照已经不成立了。
//   - **不处理单人队**。一个人的队伍没有队友受影响,摘掉他等于解散,不如留给 active_ttl
//     自然回收 —— 玩家断线重连回来还能看到自己的队伍,少一次「队伍怎么没了」。
package biz

import (
	"context"
	"time"

	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/offlinewatch"
)

// PresenceInspector 是 team 对离线复查骨架的最小依赖面(*offlinewatch.Watcher 满足)。
//
// 只用于**读路径兜底**:kafka 事件会丢、Hub DS 整台挂掉时压根不发事件,
// 光靠事件会留下永远清不掉的残留成员。玩家一打开组队面板就顺手观察一次;
// ONLINE / OFFLINE / UNKNOWN 的分类与排期全部由 offlinewatch 封装。
type PresenceInspector interface {
	Observe(ctx context.Context, playerIDs []uint64) error
}

// SetPresenceInspector 注入读路径兜底所需的复查入口。
// nil-safe:不注入(功能关闭 / 未配 locator)时读路径行为与历史完全一致。
func (u *TeamUsecase) SetPresenceInspector(p PresenceInspector) {
	u.presence = p
}

// offlineLeaveEnabled 汇总「这条链是不是真的开着」。
// 配置开了但依赖没注入(装配漏了)时按关处理,不会走到半截逻辑。
func (u *TeamUsecase) offlineLeaveEnabled() bool {
	return u.cfg.OfflineLeave.Enabled && u.matchCommitment != nil
}

// OnPlayerOffline 实现 offlinewatch.Handler:某玩家已确认离线满阈值。
//
// 幂等:同一玩家被重复调用(事件重投、多副本各扫一遍、读路径兜底)是常态。
// 玩家不在任何队伍可直接完成；队伍已不存在 / 已解散 / 已不含该玩家时，仍须用
// compare-delete 收敛旧 player→team 索引，成功后才算处理完成。
//
// 返回 error 只留给「这次没判成,下轮再来」:Redis 读失败、matchmaker 读不确定、
// 写冲突或旧索引尚未清理成功。
func (u *TeamUsecase) OnPlayerOffline(ctx context.Context, playerID uint64, offlineSinceMs int64) error {
	if !u.offlineLeaveEnabled() || playerID == 0 {
		return nil
	}

	teamID, found, err := u.repo.GetPlayerTeamID(ctx, playerID)
	if err != nil {
		return err // 读不通 → 重试,绝不当成「他没队伍」
	}
	if !found || teamID == 0 {
		return nil // 不在任何队伍,正常路径
	}

	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		return err
	}
	if !found || team.State == stateDisbanded || !hasMember(team, playerID) {
		// 队伍主体已是终态，但 player→team 索引可能来自上一次「主体写成功、索引删除
		// 失败」的部分成功。必须精确删除仍指向旧 teamID 的索引；失败继续重试。
		return u.deleteOfflinePlayerIndex(ctx, playerID, teamID)
	}
	if len(team.Members) <= 1 {
		return nil // 单人队不动(见文件头「刻意不做的事」)
	}

	// 闸 ②:整支队伍被一场对局占住 → 整轮跳过。读不确定必须 fail-closed(返回 error 重试),
	// 绝不能因为 matchmaker 抖一下就把一支正在打的队伍拆了。
	committed, err := u.isTeamCommittedToMatch(ctx, team)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_offline_leave_commitment_unknown",
			"team_id", teamID, "player_id", playerID, "err", err)
		return err
	}
	if committed {
		plog.With(ctx).Debugw("msg", "team_offline_leave_skipped_match_committed",
			"team_id", teamID, "player_id", playerID)
		// 对局占用是暂态，不是处理终态。返回 ErrDeferred 让 offlinewatch 保留到期项，
		// 票据释放后自动重查；返回 nil 会永久删任务，只能碰运气等下一次事件。
		return offlinewatch.ErrDeferred
	}

	// 闸 ③:该玩家自己被对局占住(冗余保险,见文件头)。
	playerCommitted, err := u.matchCommitment.IsPlayerCommittedToMatch(ctx, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_offline_leave_player_commitment_unknown",
			"team_id", teamID, "player_id", playerID, "err", err)
		return err
	}
	if playerCommitted {
		return offlinewatch.ErrDeferred
	}

	return u.removeOfflineMember(ctx, teamID, playerID, offlineSinceMs)
}

// removeOfflineMember 把成员摘出队伍。核心与 LeaveTeam 同源(同一把乐观锁、同样的
// 队长转移 / READY 回退 / 索引清理),差别只有:推送原因不同、且不撤匹配票据。
func (u *TeamUsecase) removeOfflineMember(ctx context.Context, teamID, playerID uint64, offlineSinceMs int64) error {
	ttl := u.activeTTL()
	disbandedTTL := u.cfg.DisbandedRetention.Std()
	var result *teamv1.TeamStorageRecord
	var terminalNeedsIndexCleanup bool

	if err := u.repo.UpdateWithLock(ctx, teamID, u.cfg.OptimisticRetry, func(team *teamv1.TeamStorageRecord) error {
		// 锁内重查一遍:从上面的读到这里之间,他可能已经自己离队 / 被踢 / 队伍已解散。
		if team.State == stateDisbanded {
			terminalNeedsIndexCleanup = true
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if !hasMember(team, playerID) {
			terminalNeedsIndexCleanup = true
			return errcode.New(errcode.ErrTeamNotFound, "player %d not in team %d", playerID, teamID)
		}
		// 锁内再确认一次人数:并发摘人时不能把最后一个成员也摘掉(那等于自动解散队伍,
		// 超出了「移除离线队员」的授权范围)。
		if len(team.Members) <= 1 {
			return errcode.New(errcode.ErrTeamWrongState, "team %d has no teammate to keep", teamID)
		}
		// ★ 与 matchmaker 的共同线性化点:BeginTeamMatch 在**同一把锁**内上的租约。
		// 看到它就说明有一次组票已经(或正在)把这份名单冻进票据 —— 这时摘人就会造出
		// 「人在票据里、却不在队伍里」。两个操作现在只能有一个赢,窗口不再存在。
		// 租约是秒级且会自净,所以这里推迟重试即可,不需要任何补偿。
		if rosterLockedForMatch(team) {
			return errcode.New(errcode.ErrTeamConcurrent,
				"team %d roster locked for matchmaking until %d", teamID, team.GetMatchLockUntilMs())
		}

		team.Members = removeMember(team.Members, playerID)
		team.UpdatedAtMs = time.Now().UnixMilli()

		if team.CaptainId == playerID {
			// 队长离线被摘 → 按 LeaveTeam 的既有规则转给第一个成员,
			// 否则一支队伍会永远卡在「队长不在、没人能改图 / 审批申请」。
			team.CaptainId = team.Members[0].PlayerId
		}
		if team.State == stateReady {
			team.State = stateForming // 少了人就不再是「全员已准备」
		}
		result = cloneTeam(team)
		return nil
	}, ttl); err != nil {
		// 主体不存在 / 已解散 / 已不含玩家时，仍须收敛此前读到的旧归属索引。
		// 单人队则保留其正常归属；其余错误由骨架退避后重排。
		switch errcode.As(err) {
		case errcode.ErrTeamNotFound:
			return u.deleteOfflinePlayerIndex(ctx, playerID, teamID)
		case errcode.ErrTeamWrongState:
			if terminalNeedsIndexCleanup {
				return u.deleteOfflinePlayerIndex(ctx, playerID, teamID)
			}
			return nil
		case errcode.ErrTeamConcurrent:
			// 组票租约赢了这一轮(或乐观锁重试耗尽)。都是暂态,不是处理终态:
			// 保留到期项,租约自净后下轮重来。用 ErrDeferred 而非普通 error,
			// 免得每次正常竞争都刷一条 handler_failed 的 Warn。
			return offlinewatch.ErrDeferred
		default:
			return err
		}
	}

	// CAS 删索引:仅当索引仍指向本队才删,防误删他并发加入新队的归属。
	// 失败时仍完成下面的开放索引 / 推送等主体写后动作，但最终返回 error 保留复查任务；
	// 下轮会命中 OnPlayerOffline 的终态分支，继续精确清理这条旧索引。
	indexErr := u.deleteOfflinePlayerIndex(ctx, playerID, teamID)

	// 人数变了 → 同步开放招募索引(满员队摘掉一人后重新开放)。
	u.syncOpenIndex(ctx, result, result.MapId)

	if result.State == stateDisbanded {
		// 理论上到不了(锁内已挡住摘最后一人),留着是为了万一将来放开该限制时不漏处理。
		u.refreshDisbandedTTL(ctx, teamID, disbandedTTL)
	}

	// caller 传 0:所有人都收到,包括被摘的那位本人 —— 他若在推送到达前恰好重连回来,
	// 能立刻知道自己已经不在队里,而不是对着一个过期的队伍界面点半天。
	u.pushUpdate(ctx, 0, append(memberIDs(result), playerID), result,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_OFFLINE_LEFT, 0)

	plog.With(ctx).Infow("msg", "team_offline_leave",
		"team_id", teamID, "player_id", playerID,
		"offline_since_ms", offlineSinceMs,
		"threshold", u.cfg.OfflineLeave.Threshold.String(),
		"new_state", result.State, "remaining", len(result.Members))
	u.compensateIfCommittedDuringRemoval(ctx, teamID, playerID)
	if indexErr != nil {
		return indexErr
	}
	return nil
}

// deleteOfflinePlayerIndex 精确清理本次处理看到的旧 player→team 归属。
// compare-delete 在玩家已并发加入新队时是安全 no-op；存储失败必须向上传播，确保
// offlinewatch 不会把仍有残留索引的任务误判为完成。
func (u *TeamUsecase) deleteOfflinePlayerIndex(ctx context.Context, playerID, teamID uint64) error {
	if err := u.repo.DeletePlayerIndexIfMatches(ctx, playerID, teamID); err != nil {
		plog.With(ctx).Warnw("msg", "team_offline_leave_delete_player_index_failed",
			"player_id", playerID, "team_id", teamID, "err", err)
		return err
	}
	return nil
}

// inspectTeamPresence 是读路径兜底:把完整成员列表交给 offlinewatch 的统一观察入口。
//
// ONLINE / OFFLINE / UNKNOWN 的分类和排期都留在 offlinewatch 内；Team 不复制判定规则，
// 也不在读路径上直接摘人。实际动作仍由后台复查循环执行。
//
// 全程 best-effort:观察失败只记日志,绝不影响本次读返回 —— 组队面板打不开
// 比多留一个离线成员严重得多。
func (u *TeamUsecase) inspectTeamPresence(ctx context.Context, team *teamv1.TeamStorageRecord) {
	if !u.offlineLeaveEnabled() || u.presence == nil || team == nil {
		return
	}
	if team.State == stateDisbanded || len(team.Members) <= 1 {
		return
	}
	ids := memberIDs(team)
	if err := u.presence.Observe(ctx, ids); err != nil {
		plog.With(ctx).Warnw("msg", "team_presence_observe_failed",
			"team_id", team.GetTeamId(), "members", len(ids), "err", err)
	}
}

// compensateIfCommittedDuringRemoval 收口「检查对局占用 → 改队伍」之间的 TOCTOU。
//
// 这个窗口跨服务(matchmaker 权威 + team 的 Redis 乐观锁),没法做成一个原子操作:
// 闸门放行之后、`UpdateWithLock` 提交之前,队长完全可能刚好点了开始匹配,
// matchmaker 把包含这名离线成员的 roster 冻进票据。那样就会出现
// **人在票据里、却已经不在队伍里** —— 他被拉进一场自己不在场的对局(掉分),
// 回来还发现没了队伍。
//
// 窗口消不掉,但后果能收敛:摘人成功后复核一次,发现票据确实在窗口内成立了,
// 就走**与 LeaveTeam 完全相同的补偿** —— 撤销整张票据,全队退回队列重新匹配。
// 理由也和 LeaveTeam 一样:队伍人数已经变了,票据里那份成员快照不再成立。
// 结果从「带着一个离线的人开局」变成「重新匹配一次(且这次不含他)」。
//
// 为什么不在锁内复核:Redis 事务里发不了 gRPC。
// 为什么不回滚摘人:那时票据已冻结,把人加回去反而制造第二种不一致,还可能撞上其它并发写。
//
// 复核 RPC 失败时不重试也不回滚(人已经摘了),只记 Error + 计数 —— 这是本路径唯一
// 需要人工看一眼的残留,必须可观测,不能静默。
func (u *TeamUsecase) compensateIfCommittedDuringRemoval(ctx context.Context, teamID, playerID uint64) {
	if u.matchCommitment == nil {
		return
	}
	committed, err := u.matchCommitment.IsPlayerCommittedToMatch(ctx, playerID)
	if err != nil {
		OfflineLeaveRace.WithLabelValues("recheck_failed").Inc()
		plog.With(ctx).Errorw("msg", "team_offline_leave_race_recheck_failed",
			"team_id", teamID, "player_id", playerID, "err", err,
			"impact", "member already removed; cannot tell whether a ticket froze him in during the window")
		return
	}
	if !committed {
		return // 常态:窗口没被命中
	}

	// 窗口被命中了。撤票语义与 LeaveTeam 一致:排队中 → 全队退出队列;
	// 确认期 → 等价该玩家拒绝确认(match 失败,其余票据退回队列)。
	OfflineLeaveRace.WithLabelValues("compensated").Inc()
	plog.With(ctx).Warnw("msg", "team_offline_leave_race_compensated",
		"team_id", teamID, "player_id", playerID,
		"detail", "match ticket froze this member between the gate check and the team write; cancelling it so the team rematches without him")
	u.cancelMatchmaking(ctx, teamID, playerID)
}

// ── 组票 roster fence:与 matchmaker 的共同线性化点 ───────────────────────────

// 租约钳制范围。只需覆盖「matchmaker 拿到名单 → ClaimPlayer 落地」这一小段。
//
// 下限防误配成 0(锁瞬间失效 = 等于没上锁);上限防一次异常的 Begin 把摘人挡住太久 ——
// 租约到期即自净,所以上限也是「matchmaker 崩了最多拖多久」的上界。
const (
	matchLockMinLease = 2 * time.Second
	matchLockMaxLease = 15 * time.Second
)

// BeginTeamMatch 在 team 的乐观锁内原子完成「校验 + 上租约锁 + 返回快照」。
//
// 这是消除 TOCTOU 的关键:matchmaker 原先只读 GetTeam 取名单,与本服务的自动摘人
// 分属两把锁,凑不出共同线性化点。改成在这里上锁后,「冻结名单」与「移除离线成员」
// 落在同一把 team 乐观锁上,两者只能有一个赢 —— 窗口从「收敛后果」变成「不存在」。
//
// 同 operation_id 幂等续租:matchmaker 的重试(网络抖动 / 响应丢失)不得把自己的锁
// 判成冲突(§9.23 端到端幂等)。
func (u *TeamUsecase) BeginTeamMatch(
	ctx context.Context, teamID, captainID uint64, operationID string, leaseMs int64,
) (*teamv1.TeamStorageRecord, int64, error) {
	if teamID == 0 || captainID == 0 || operationID == "" {
		return nil, 0, errcode.New(errcode.ErrInvalidArg, "team_id, captain_id and operation_id required")
	}
	lease := time.Duration(leaseMs) * time.Millisecond
	if lease < matchLockMinLease {
		lease = matchLockMinLease
	}
	if lease > matchLockMaxLease {
		lease = matchLockMaxLease
	}

	var result *teamv1.TeamStorageRecord
	var expiresAtMs int64
	if err := u.repo.UpdateWithLock(ctx, teamID, u.cfg.OptimisticRetry, func(team *teamv1.TeamStorageRecord) error {
		if team.State == stateDisbanded {
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if team.CaptainId != captainID {
			return errcode.New(errcode.ErrTeamNotCaptain, "player %d is not captain of team %d", captainID, teamID)
		}
		// 与 matchmaker 原先在 resolveMembers 里做的校验保持一致,只是挪进了锁内。
		if team.State != stateReady {
			return errcode.New(errcode.ErrTeamWrongState, "team %d not ready (state=%d)", teamID, team.State)
		}
		now := time.Now().UnixMilli()
		if team.MatchLockUntilMs > now && team.MatchLockOperationId != operationID {
			// 另一次组票的租约还没到期。这是正常竞争(队长连点 / 并发重试),
			// 调用方退避重来即可,不是错误状态。
			return errcode.New(errcode.ErrTeamConcurrent,
				"team %d roster locked by operation %s until %d", teamID, team.MatchLockOperationId, team.MatchLockUntilMs)
		}
		expiresAtMs = now + lease.Milliseconds()
		team.MatchLockUntilMs = expiresAtMs
		team.MatchLockOperationId = operationID
		team.UpdatedAtMs = now
		result = cloneTeam(team)
		return nil
	}, u.activeTTL()); err != nil {
		return nil, 0, err
	}

	plog.With(ctx).Debugw("msg", "team_match_roster_locked",
		"team_id", teamID, "captain_id", captainID, "operation_id", operationID,
		"lease_ms", lease.Milliseconds(), "expires_at_ms", expiresAtMs, "members", len(result.Members))
	return result, expiresAtMs, nil
}

// rosterLockedForMatch 判断此刻是否有未过期的组票租约。只在 team 的乐观锁**内**调用。
func rosterLockedForMatch(team *teamv1.TeamStorageRecord) bool {
	return team.GetMatchLockUntilMs() > time.Now().UnixMilli()
}
