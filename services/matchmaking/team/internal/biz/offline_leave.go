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
// ⚠️ 上述读取闸门只能 fail-closed，不能与 StartMatch 形成共同线性化点。当前启动
// 配置因此禁止开启本功能（见 conf.ValidateOfflineLeave）；这里保留实现与防御分支，供后续
// roster fence 接入及回归测试使用。不得把这些前置读取解释成并发安全证明。
//
// # 刻意不做的事
//
//   - **不联动 cancelMatchmaking**。LeaveTeam / Kick 会撤票是因为那是玩家的主动操作;
//     自动摘人只在「队伍没被对局占住」时发生,此时根本没有票可撤,调用它只会平添一次
//     无谓 RPC 和一条误导性日志。
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
