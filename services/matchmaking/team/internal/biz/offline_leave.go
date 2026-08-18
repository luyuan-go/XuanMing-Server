// offline_leave.go — 队员离线超时自动移出队伍(2026-08-06)。
//
// # 这条链长什么样
//
//	Hub DS Logout ──ReportDisconnect──▶ player_locator(记 last-seen + 发离场事件)
//	                                          │ kafka: pandora.player.presence
//	                                          ▼
//	                          pkg/offlinewatch(排到期 → 到点回查 locator 权威)
//	                                          │
//	              ┌───────────────────────────┴───────────────────────────┐
//	              │ 不在线,未满 threshold                                  │ 不在线,已满 threshold
//	              ▼                                                       ▼
//	  TeamUsecase.OnPlayerPresenceLost                       TeamUsecase.OnPlayerOffline
//	  (取消他的准备,队伍掉出 READY,人还在队里)                (把人摘出队伍)
//
// # 为什么要两档(INC-20260813-001)
//
// 180s 的 threshold 是留给弱网 / 地铁 / 重连的余量,**不该缩**。但在这段时间里,
// 「他还留在队伍里」会被下游误读成「他有资格被拉进对局」:队长点开始匹配时,matchmaker
// 把一个已经不在大厅的人原样冻进票据,战斗 DS 拿到 N 人 roster 却只进来 N-1 个。
//
// 留人(为了重连)与可开局(需要人在场)本来就是两个判断,现在分开:软档只动准备状态,
// 玩家回来自己重新点一下即可;硬档才动队伍成员。
//
// ⚠️ **本档不是 INC-20260813-001 的第一根因**。那次事故的缺席者全程没有掉线 ——
// 他打完上一局先退出战斗,还在「结算 → 回大厅 → 重登」路上,队长 75 秒后就开了下一局。
// 真正的缺口是 team 侧没有 match-ended 复位路径(见下面的 EndTeamMatch);
// 本档是掉线场景的纵深防御,任何离线阈值都覆盖不到「正常玩家还没走回来」这一形态。
//
// ⚠️ 2026-08-17 起 ready 不再是开局门槛(LoL 式流程,见 BeginTeamMatch 注释),
// 软档清 ready 不再能挡住组票;「不在场者进不了票」由 matchmaker 的 StartMatch
// 在线闸(4011 点名)与撮合确认期(CONFIRM 超时判过错)承担。软档保留的价值:
// 存量客户端的「已准备」显示随掉线复位,且 READY→FORMING 让队伍回到招募列表。
//
// # 三道闸,少一道都会出事(硬档 OnPlayerOffline)
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
	"errors"
	"time"

	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/offlinewatch"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/data"
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
	// R3:offlinewatch 是定时任务面,ctx 里没有玩家 JWT —— player_id 不写进 ctx 的话,
	// 这一整条自动摘人链在日志里与该玩家的其它记录完全串不起来。
	ctx = plog.WithPlayerID(ctx, playerID)

	teamID, found, err := u.repo.GetPlayerTeamID(ctx, playerID)
	if err != nil {
		// R2:读不通就重试(绝不当成「他没队伍」)。不打日志的话,一个持续读失败的
		// 玩家会永远留在队伍里,而外部只看到"人一直没被摘掉"。
		plog.With(ctx).Warnw("msg", "team_offline_leave_rejected",
			"reason", reasonStoreReadFailed, "player_id", playerID,
			"stage", "player_index", "err", err)
		return err // 读不通 → 重试,绝不当成「他没队伍」
	}
	if !found || teamID == 0 {
		return nil // 不在任何队伍,正常路径
	}
	ctx = plog.WithTeamID(ctx, teamID)

	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_offline_leave_rejected",
			"reason", reasonStoreReadFailed, "player_id", playerID, "team_id", teamID,
			"stage", "team_record", "err", err)
		return err
	}
	if !found || team.State == stateDisbanded || !hasMember(team, playerID) {
		// 队伍主体已是终态，但 player→team 索引可能来自上一次「主体写成功、索引删除
		// 失败」的部分成功。必须精确删除仍指向旧 teamID 的索引；失败继续重试。
		return u.deleteOfflinePlayerIndex(ctx, playerID, teamID)
	}
	if len(team.Members) <= 1 {
		// R4:单人队每轮复查都会走到这里,只能 Debug。
		plog.With(ctx).Debugw("msg", "team_offline_leave_skipped",
			"reason", reasonSingleMemberTeam, "player_id", playerID, "team_id", teamID)
		return nil // 单人队不动(见文件头「刻意不做的事」)
	}

	// 闸 ②:整支队伍被一场对局占住 → 整轮跳过。读不确定必须 fail-closed(返回 error 重试),
	// 绝不能因为 matchmaker 抖一下就把一支正在打的队伍拆了。
	committed, err := u.isTeamCommittedToMatch(ctx, team)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_offline_leave_commitment_unknown",
			"reason", reasonMatchCommitmentUnknown,
			"team_id", teamID, "player_id", playerID,
			"captain_id", team.CaptainId, "fail_closed", true, "err", err)
		return err
	}
	if committed {
		plog.With(ctx).Debugw("msg", "team_offline_leave_skipped_match_committed",
			"reason", reasonMatchCommitted,
			"team_id", teamID, "player_id", playerID, "team_state", team.State)
		// 对局占用是暂态，不是处理终态。返回 ErrDeferred 让 offlinewatch 保留到期项，
		// 票据释放后自动重查；返回 nil 会永久删任务，只能碰运气等下一次事件。
		return offlinewatch.ErrDeferred
	}

	// 闸 ③:该玩家自己被对局占住(冗余保险,见文件头)。
	playerCommitted, err := u.matchCommitment.IsPlayerCommittedToMatch(ctx, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_offline_leave_player_commitment_unknown",
			"reason", reasonPlayerCommitmentUnknown,
			"team_id", teamID, "player_id", playerID, "fail_closed", true, "err", err)
		return err
	}
	if playerCommitted {
		plog.With(ctx).Debugw("msg", "team_offline_leave_skipped_match_committed",
			"reason", reasonPlayerCommitted,
			"team_id", teamID, "player_id", playerID)
		return offlinewatch.ErrDeferred
	}

	return u.removeOfflineMember(ctx, teamID, playerID, offlineSinceMs)
}

// errReadyNoChange 是锁内的「没什么可改的」哨兵:让 UpdateWithLock 放弃写回。
// 不是错误,外层统一映射成 nil —— 否则一个挂机离线的玩家会让他所在队伍每 15s
// 白写一次 Redis 并广播一次无意义的推送。
var errReadyNoChange = errors.New("team: ready state unchanged")

// OnPlayerPresenceLost 实现 offlinewatch.PresenceLostHandler:成员此刻不在线,
// 但还没满 offline_leave.threshold。
//
// # 它和 OnPlayerOffline 的分工
//
//	不在线,未满阈值(本函数)   → 取消他的准备,队伍掉出 READY。**人留在队伍里。**
//	不在线,已满阈值(OnPlayerOffline)→ 把人摘出队伍。
//
// 那 180s 阈值是留给弱网 / 地铁 / 重连的余量,**不该缩**。但在这段时间里,
// 「他还留在队伍里」会被下游误读成「他有资格被拉进对局」:队长点开始匹配时,
// 一个已经不在大厅的人被原样冻进票据,DS 拿到 N 人却只进来 N-1 个。
// 本函数把两件事分开:留人(为了重连)与可开局(需要人在场)不是同一个判断。
//
// 队长因此根本点不动「开始匹配」(队伍不是 READY),不需要先撞一个错误码才知道出了事;
// 玩家重连回来自己重新点准备即可 —— 与「队友取消准备」完全同一套语义,客户端零改动。
//
// # 幂等 / 无变化即无写
//
// 本回调在玩家离线期间每轮 Observe 都会来一次。锁外先廉价判一次、锁内再判一次,
// 没有实际变化就用哨兵放弃写回 —— 否则一个挂机离线的玩家会让他所在队伍每 15s 白写一次
// Redis 并广播一次无意义的推送。
func (u *TeamUsecase) OnPlayerPresenceLost(ctx context.Context, playerID uint64, sinceMs int64) error {
	if !u.offlineLeaveEnabled() || playerID == 0 {
		return nil
	}
	// R3:同 OnPlayerOffline —— 定时任务面没有玩家 JWT,join key 必须手写。
	ctx = plog.WithPlayerID(ctx, playerID)
	teamID, found, err := u.repo.GetPlayerTeamID(ctx, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_presence_lost_rejected",
			"reason", reasonStoreReadFailed, "player_id", playerID,
			"stage", "player_index", "err", err)
		return err // 读不通 → 下轮重来,绝不当成「他没队伍」
	}
	if !found || teamID == 0 {
		return nil
	}
	ctx = plog.WithTeamID(ctx, teamID)
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "team_presence_lost_rejected",
			"reason", reasonStoreReadFailed, "player_id", playerID, "team_id", teamID,
			"stage", "team_record", "err", err)
		return err
	}
	// 终态 / 已不含该成员 / 单人队一律不动。旧 player→team 索引的收敛是 OnPlayerOffline
	// 的职责(它有 due/claim 闭环能保证重试),本函数不重复那条链。
	if !found || team.State == stateDisbanded || !hasMember(team, playerID) || len(team.Members) <= 1 {
		// R4:玩家离线期间每轮 Observe 都会来一次,只能 Debug。四个条件拆开报,
		// 否则"他为什么没被取消准备"分不出是队伍没了还是他早已不在队里。
		reason := reasonTeamNotFound
		switch {
		case found && team.State == stateDisbanded:
			reason = reasonTeamDisbanded
		case found && !hasMember(team, playerID):
			reason = reasonNotMember
		case found:
			reason = reasonSingleMemberTeam
		}
		plog.With(ctx).Debugw("msg", "team_presence_lost_skipped",
			"reason", reason, "player_id", playerID, "team_id", teamID)
		return nil
	}

	result, err := u.clearMemberReady(ctx, teamID, readyClearOpts{
		targets: []uint64{playerID},
		cause:   "presence_lost",
	})
	if err != nil {
		return err
	}
	if result == nil {
		return nil // 并发路径已经改过了
	}
	u.publishReadyCleared(ctx, result)
	plog.With(ctx).Infow("msg", "team_presence_lost_unready",
		"team_id", teamID, "player_id", playerID, "since_ms", sinceMs,
		"new_state", result.State, "members", len(result.Members),
		"ready_count", readyCount(result.Members),
		"ready_generation", result.GetReadyGeneration())
	return nil
}

// EndTeamMatch 对局结束后复位队伍准备状态(matchmaker 在 ReleaseMatch 时调用)。
//
// # 历史与现状
//
// 这条路径为 INC-20260813-001 v2 的第一根因而生:当时 ready 是开局门槛,一局打完
// 队伍仍停在 READY,队长能在队友还没回大厅时用残留 ready 立刻再开一局(缺席者被
// 原样冻进票据,3v3 打成 3v2)。
//
// 2026-08-17 起 ready 不再是开局门槛(LoL 式流程,见 BeginTeamMatch 注释),
// 「带缺席者开局」由 matchmaker 在线闸 + 撮合确认期兜住。本方法保留两个职责:
//  1. 清掉本局成员的残留 ready 位 —— 存量客户端还在按 ready 渲染面板;
//  2. 滚动升级共存窗口:旧 team 记录 / 旧 matchmaker 组合下,这是唯一的赛后复位。
//
// 新链路下 Begin 已在开局时清过 ready,本方法的代际 CAS 几乎总是落空 → no-op。
//
// # 语义
//
// 打完一局回大厅,残留 ready 被清掉;队长**不必**等任何人重新点准备即可再开局。
//
// # 幂等与终态
//
// battle_result 的 outbox 会把 ReleaseMatch 重投到成功为止,所以本方法必然被重复调用:
// 已复位过再调是零写零推送。队伍已解散 / 已不存在 / 成员已不在队,一律**返回成功** ——
// 那些都是「本就没什么可复位的」,报错只会让 outbox 永远重试下去。
// # 跨代幂等靠 expectedGen,不靠 player_ids
//
// outbox 会重投,所以本方法必然被重复调用。光看「谁还挂着 ready」是**不够**的:
// ACK 丢失后玩家重新点了准备 / 离队重入 / 队长已开新局,重投照样把新意图抹平。
// expectedGen 是 BeginTeamMatch 冻结名单那一刻的 ready 代际,只有它仍等于当前代际
// 才复位 —— 任何 ready 意图变更都会推进代际(见 ready_generation.go),重投自然落空。
//
// expectedGen==0 = 代际未知(滚动升级窗口的旧 matchmaker / 旧 team 记录),
// 退化为「只在当前确实还挂着 ready 时复位一次」。不是跨代安全的,但严格优于完全不复位。
func (u *TeamUsecase) EndTeamMatch(ctx context.Context, teamID uint64, playerIDs []uint64, expectedGen uint64) error {
	started := time.Now()
	if teamID == 0 {
		plog.With(ctx).Warnw("msg", "team_match_end_rejected",
			"reason", reasonMissingArg,
			"players", len(playerIDs), "expected_ready_generation", expectedGen)
		return errcode.New(errcode.ErrInvalidArg, "team_id required")
	}
	if expectedGen == 0 {
		// R2:滚动升级共存窗口(旧 matchmaker / 旧 team 记录没有代际)。此时复位退化成
		// 「只要还挂着 ready 就清一次」,不是跨代安全的 —— 出现即说明还有旧副本在跑,
		// 必须可见,否则事后无法解释"为什么某次复位把新点的准备抹掉了"。
		plog.With(ctx).Warnw("msg", "team_match_end_legacy_generation",
			"reason", reasonLegacyReadyRevoked,
			"team_id", teamID, "players", len(playerIDs),
			"hint", "调用方未回传 ready 代际(旧副本共存窗口),复位退化为非跨代安全")
	}
	result, err := u.clearMemberReady(ctx, teamID, readyClearOpts{
		targets:     playerIDs,
		expectedGen: expectedGen,
		cause:       "match_end",
	})
	if err != nil {
		if errors.Is(err, offlinewatch.ErrDeferred) {
			// 组票租约在手 = 队长已经开了**下一局**。租约是秒级自净的,
			// 交给上游 outbox 下一轮重投即可(此时改 ready 会与那一局的冻结名单打架)。
			// clearMemberReady 已按分支打过带 reason 的 deferred 日志,这里只补收尾。
			plog.With(ctx).Warnw("msg", "team_match_end_rejected",
				"reason", reasonRosterLocked,
				"team_id", teamID, "players", len(playerIDs),
				"expected_ready_generation", expectedGen,
				"elapsed_ms", time.Since(started).Milliseconds(),
				"hint", "上游 outbox 会重投;返回 3007 属暂态")
			return errcode.New(errcode.ErrTeamConcurrent,
				"team %d roster locked by another match start", teamID)
		}
		return err
	}
	if result == nil {
		return nil // 已经复位过 / 队伍已终态:幂等成功
	}
	u.publishReadyCleared(ctx, result)
	plog.With(ctx).Infow("msg", "team_match_ended_unready",
		"team_id", teamID, "players", len(playerIDs),
		"expected_ready_generation", expectedGen,
		"new_state", result.State, "members", len(result.Members),
		"ready_count", readyCount(result.Members),
		"ready_generation", result.GetReadyGeneration(),
		"elapsed_ms", time.Since(started).Milliseconds())
	return nil
}

// clearMemberReady 是「取消部分成员的准备、必要时把队伍打回 FORMING」的共同实现。
//
// 两个调用方(掉线软化 / 对局结束复位)的差别只有触发原因与日志,**写路径必须完全一致** ——
// 一处漏了 syncOpenIndex 或推送,另一处的玩家就会对着一个不会刷新的面板点半天。
//
// targets 为空 = 全队。返回 (nil, nil) 表示「没什么可改的」,调用方按幂等成功处理。
// 返回 offlinewatch.ErrDeferred 表示组票租约在手,调用方自行决定怎么重试。
type readyClearOpts struct {
	// targets 要取消准备的成员;空 = 全队。
	targets []uint64
	// expectedGen 非 0 时做 CAS:当前 ready 代际必须等于它才动手(跨代幂等,见 EndTeamMatch)。
	expectedGen uint64
	// cause 只用于日志(§11.3 R1 的 cause 词根:presence_lost / match_end)。
	// 两个调用方共用同一段写路径,不带它就分不出"这次没复位"是掉线软化还是对局结束。
	// **不参与任何判定** —— 加它是为了可诊断性,不是为了分叉行为。
	cause string
}

func (u *TeamUsecase) clearMemberReady(
	ctx context.Context, teamID uint64, opts readyClearOpts,
) (*teamv1.TeamStorageRecord, error) {
	targets := opts.targets
	var result *teamv1.TeamStorageRecord
	// prevState 只用于日志:锁内看到的最后一次取值即本次提交的依据(乐观锁重试时
	// 会被覆盖成最后一轮的值,正是我们要记的那一轮)。放在闭包外,重试不会重复打日志。
	var prevState teamv1.TeamState
	if err := u.updateTeam(ctx, teamID, func(team *teamv1.TeamStorageRecord) error {
		prevState = team.State
		if team.State == stateDisbanded {
			// R4:队伍已解散是常态终态(outbox 迟到重投 / 离线玩家挂机),Debug。
			plog.With(ctx).Debugw("msg", "team_ready_clear_skipped",
				"reason", reasonTeamDisbanded, "cause", opts.cause,
				"team_id", teamID, "targets", len(targets))
			return errReadyNoChange
		}
		// 跨代 CAS:代际已经往前走了,说明这条复位对应的那一局早已翻篇
		// (玩家重新点了准备 / 离队重入 / 队长开了新局)。按幂等成功处理,绝不能抹掉新意图。
		if opts.expectedGen != 0 && team.GetReadyGeneration() != opts.expectedGen {
			// R2:这是**设计内**的幂等落空,但必须可见 —— INC-20260813-001 里
			// "打完一局还挂着 ready" 的对立面就是"复位被 CAS 挡了却没人知道"。
			// 只在 expectedGen != 0(EndTeamMatch)时可达,不会被离线软化路径刷屏。
			plog.With(ctx).Warnw("msg", "team_ready_clear_skipped",
				"reason", reasonReadyGenerationMismatch, "cause", opts.cause,
				"team_id", teamID,
				"expected_ready_generation", opts.expectedGen,
				"current_ready_generation", team.GetReadyGeneration(),
				"team_state", team.State, "members", len(team.Members),
				"ready_count", readyCount(team.Members),
				"hint", "代际已推进 = 这条复位对应的那一局早已翻篇,按幂等成功处理")
			return errReadyNoChange
		}
		// ★ 与 matchmaker 的共同线性化点,同 removeOfflineMember:BeginTeamMatch 已经
		// (或正在)把这份名单冻进票据,这时改 ready 会让票据里的快照与队伍打架。
		// 租约是秒级自净,推迟一轮即可。
		if rosterLockedForMatch(team) {
			plog.With(ctx).Warnw("msg", "team_ready_clear_deferred",
				"reason", reasonRosterLocked, "cause", opts.cause,
				"team_id", teamID,
				"lock_until_ms", team.GetMatchLockUntilMs(),
				"lock_operation_id", team.GetMatchLockOperationId(),
				"hint", "组票租约在手,秒级自净;调用方退避后重来")
			return offlinewatch.ErrDeferred
		}
		if !readyNeedsClearing(team, targets) {
			// R4:离线玩家每轮 Observe 都会走到这里,只能 Debug。
			plog.With(ctx).Debugw("msg", "team_ready_clear_skipped",
				"reason", reasonReadyAlreadyCleared, "cause", opts.cause,
				"team_id", teamID, "team_state", team.State,
				"targets", len(targets), "ready_count", readyCount(team.Members))
			return errReadyNoChange
		}
		for _, idx := range readyTargetIndexes(team, targets) {
			team.Members[idx].Ready = false
		}
		if team.State == stateReady {
			team.State = stateForming // 少一个人在场 / 刚打完一局,就不再是「全员已准备」
		}
		team.UpdatedAtMs = time.Now().UnixMilli()
		result = cloneTeam(team)
		return nil
	}, u.activeTTL()); err != nil {
		switch {
		case errors.Is(err, errReadyNoChange):
			return nil, nil
		case errors.Is(err, offlinewatch.ErrDeferred):
			return nil, err
		case errcode.As(err) == errcode.ErrTeamNotFound, errcode.As(err) == errcode.ErrTeamWrongState:
			// R4:队伍已没了 / 已终态是常态(outbox 迟到重投),Debug 即可。
			plog.With(ctx).Debugw("msg", "team_ready_clear_skipped",
				"reason", reasonTeamNotFound, "cause", opts.cause,
				"team_id", teamID, "err", err)
			return nil, nil // 队伍已没了 / 已终态:本就没什么可复位的
		case errcode.As(err) == errcode.ErrTeamConcurrent:
			// 乐观锁重试耗尽,暂态。用 ErrDeferred 而非普通 error,免得正常竞争刷 Warn。
			// data 层已打过 team_update_lock_exhausted,这里只补上"是哪条业务被挡了"。
			plog.With(ctx).Warnw("msg", "team_ready_clear_deferred",
				"reason", reasonOptimisticRetryExhausted, "cause", opts.cause,
				"team_id", teamID, "targets", len(targets),
				"expected_ready_generation", opts.expectedGen)
			return nil, offlinewatch.ErrDeferred
		default:
			plog.With(ctx).Warnw("msg", "team_ready_clear_failed",
				"reason", reasonStoreWriteFailed, "cause", opts.cause,
				"team_id", teamID, "targets", len(targets), "err", err)
			return nil, err
		}
	}
	// 状态机轨迹与其它写路径共用同一个 msg;cause 取调用方词根(presence_lost / match_end),
	// from == to 时 helper 自己会跳过,不会污染轨迹。
	logTeamStateChanged(ctx, teamID, prevState, result.State, opts.cause,
		result.GetReadyGeneration(), len(result.Members))
	return result, nil
}

// publishReadyCleared 收口 ready 复位后的对外动作,两个调用方共用。
//
// 复用 MEMBER_READY:这条推送表达的事实就是「成员的准备状态变了」,客户端既有的刷新
// 逻辑一字不用改。刻意不新增 reason 枚举 —— 客户端在本推送到达时的动作与队友手动取消
// 准备完全相同(§15.3:没有真实差异就不加协议面)。将来产品要区分文案时再按 §9.21 加法演进。
func (u *TeamUsecase) publishReadyCleared(ctx context.Context, result *teamv1.TeamStorageRecord) {
	u.pushUpdate(ctx, 0, memberIDs(result), result,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_READY, 0)
	// FORMING ↔ READY 改变「是否还在招募」→ 同步开放索引(同 SetReady)。
	u.syncOpenIndex(ctx, result, result.MapId)
}

// readyTargetIndexes 把 targets 映射成队伍内的下标;targets 为空 = 全队。
// 不在队伍里的 id 直接忽略(对局期间离队是正常的)。
func readyTargetIndexes(team *teamv1.TeamStorageRecord, targets []uint64) []int {
	if len(targets) == 0 {
		idxs := make([]int, 0, len(team.Members))
		for i := range team.Members {
			idxs = append(idxs, i)
		}
		return idxs
	}
	idxs := make([]int, 0, len(targets))
	for _, pid := range targets {
		if idx := memberIndex(team.Members, pid); idx >= 0 {
			idxs = append(idxs, idx)
		}
	}
	return idxs
}

// readyNeedsClearing 判断「还有没有东西要复位」。两件事任一成立就要动:
// 目标成员里还有人挂着 ready,或队伍还停在 READY(理论上后者由前者推出,分开判是为了
// 兜住历史脏数据 —— 一支 READY 却没人 ready 的队伍照样能让队长点开始匹配)。
func readyNeedsClearing(team *teamv1.TeamStorageRecord, targets []uint64) bool {
	if team.State == stateReady {
		return true
	}
	for _, idx := range readyTargetIndexes(team, targets) {
		if team.Members[idx].Ready {
			return true
		}
	}
	return false
}

// removeOfflineMember 把成员摘出队伍。核心与 LeaveTeam 同源(同一把乐观锁、同样的
// 队长转移 / READY 回退 / 索引清理),差别只有:推送原因不同、且不撤匹配票据。
func (u *TeamUsecase) removeOfflineMember(ctx context.Context, teamID, playerID uint64, offlineSinceMs int64) error {
	ttl := u.activeTTL()
	disbandedTTL := u.cfg.DisbandedRetention.Std()
	var result *teamv1.TeamStorageRecord
	var terminalNeedsIndexCleanup bool
	// 锁内判定依据只用于日志(同 team.go 各写路径)。
	var (
		rejectReason  string
		prevState     teamv1.TeamState
		prevCaptainID uint64
		prevMembers   int
		lockUntilMs   int64
	)

	if err := u.updateTeam(ctx, teamID, func(team *teamv1.TeamStorageRecord) error {
		prevState = team.State
		prevCaptainID = team.CaptainId
		prevMembers = len(team.Members)
		lockUntilMs = team.GetMatchLockUntilMs()
		// 锁内重查一遍:从上面的读到这里之间,他可能已经自己离队 / 被踢 / 队伍已解散。
		if team.State == stateDisbanded {
			terminalNeedsIndexCleanup = true
			rejectReason = reasonTeamDisbanded
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if !hasMember(team, playerID) {
			terminalNeedsIndexCleanup = true
			rejectReason = reasonNotMember
			return errcode.New(errcode.ErrTeamNotFound, "player %d not in team %d", playerID, teamID)
		}
		// 锁内再确认一次人数:并发摘人时不能把最后一个成员也摘掉(那等于自动解散队伍,
		// 超出了「移除离线队员」的授权范围)。
		if len(team.Members) <= 1 {
			rejectReason = reasonNoTeammateToKeep
			return errcode.New(errcode.ErrTeamWrongState, "team %d has no teammate to keep", teamID)
		}
		// ★ 与 matchmaker 的共同线性化点:BeginTeamMatch 在**同一把锁**内上的租约。
		// 看到它就说明有一次组票已经(或正在)把这份名单冻进票据 —— 这时摘人就会造出
		// 「人在票据里、却不在队伍里」。两个操作现在只能有一个赢,窗口不再存在。
		// 租约是秒级且会自净,所以这里推迟重试即可,不需要任何补偿。
		if rosterLockedForMatch(team) {
			rejectReason = reasonRosterLocked
			return errcode.New(errcode.ErrTeamConcurrent,
				"team %d roster locked for matchmaking until %d", teamID, team.GetMatchLockUntilMs())
		}
		rejectReason = ""

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
		if rejectReason == "" {
			rejectReason = reasonStoreWriteFailed
			if errcode.As(err) == errcode.ErrTeamConcurrent {
				rejectReason = reasonOptimisticRetryExhausted
			}
		}
		// R2:自动摘人被挡住是常态(租约竞争 / 玩家已自行离队),但"为什么没摘掉"
		// 必须查得出来 —— 否则只看到一个离线成员一直挂在队里。终态分支走 Debug
		// (每轮复查都会命中),暂态 / 故障分支走 Warn。
		if rejectReason == reasonTeamDisbanded || rejectReason == reasonNotMember ||
			rejectReason == reasonNoTeammateToKeep {
			plog.With(ctx).Debugw("msg", "team_offline_leave_skipped",
				"reason", rejectReason, "team_id", teamID, "player_id", playerID,
				"team_state", prevState, "members", prevMembers)
		} else {
			plog.With(ctx).Warnw("msg", "team_offline_leave_deferred",
				"reason", rejectReason, "team_id", teamID, "player_id", playerID,
				"team_state", prevState, "members", prevMembers,
				"lock_until_ms", lockUntilMs, "err", err)
		}
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
		"prev_state", prevState, "new_state", result.State,
		"remaining", len(result.Members),
		"captain_transferred", prevCaptainID == playerID,
		"new_captain_id", result.CaptainId,
		"ready_generation", result.GetReadyGeneration())
	logTeamStateChanged(ctx, teamID, prevState, result.State, "offline_leave",
		result.GetReadyGeneration(), len(result.Members))
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
			"reason", reasonStoreWriteFailed,
			"player_id", playerID, "team_id", teamID, "err", err,
			"hint", "旧归属索引未收敛,玩家会被 3004 挡住建不了新队;复查任务保留,下轮重试")
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
			"reason", reasonPresenceObserveFail,
			"team_id", team.GetTeamId(), "members", len(ids), "err", err,
			"hint", "只影响离线兜底提名;本次读返回不受影响")
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
			"reason", reasonRaceRecheckFailed,
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
		"reason", reasonRaceTicketFroze,
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

	// matchStartReceiptWindow 是收据的重入窗:超过这么久之后,即使 attempt_id 与
	// post_ready_generation 都还对得上,也不再把请求当成「同一次尝试的重试」。
	//
	// 为什么必须有上界:冻结之后若名单/状态都没再变,代际就停在收据记的值上。
	// 名单不变时收据名单与当前名单逐字节相同,拿回收据本身无害;窗口兜的是
	// 「重入路径不重复冻结、不推送」这层语义 —— 一次很久之后的真实点击应当走
	// 正常冻结路径(重清残留 ready、重新盖收据),而不是永远命中重入。
	//
	// 取 60s:明显大于客户端最长重试链(RPC 超时 + 断线重发,秒级),又远小于
	// 「看完结算回大厅再开一局」的尺度。失败方向安全(多走一次正常冻结,不会多开一局)。
	matchStartReceiptWindow = 60 * time.Second
)

// BeginTeamMatch 在 team 的乐观锁内原子完成
// 「校验 + 冻结名单 + 清残留 ready + 上租约锁 + 返回冻结快照」。
//
// # ready 是不是开局门槛,由**关卡表**决定(2026-08-18 拍板:两模式按图二选一)
//
// requireReady 来自关卡表 ready_mode 列(matchmaker 按本次 StartMatch 的 map_id 解析后传入):
//
//	requireReady=true (PRE_READY)     队伍必须 State==READY 才放行 —— 面板上全员点过准备,
//	                                  准备态本身就是「此刻都在」的证明;撮合成功直接进场。
//	requireReady=false(POST_CONFIRM)  FORMING 也放行,新鲜度改由撮合确认期承担。
//
// 这一列存在之前,这件事是全服一刀切的:2026-08-13 方案 A 让 ready 成为门槛,
// 2026-08-17 又整体取消。两次都是改代码全图同时变。而固定队副本要不要先准备、
// 排位要不要接受框,本就是**每张图各自的产品决定**(§17.1 差异进表)。
//
// 「带着不在场的人开局」(INC-20260813-001)在两种模式下都有防线:
//   - 两种模式共有:StartMatch 在线闸(matchmaker ensureAllPresent),离开大厅超过宽限窗
//     (默认 30s)的成员直接 4011 点名拒绝 —— 事故里那位退场 75s 的缺席者在这里就进不了票;
//   - PRE_READY:队伍必须 READY;任何人掉线 / 离队 / 入队都会把队伍打回 FORMING(见
//     OnPlayerPresenceLost 与入队重算),门槛自动失效,队长开不了局;
//   - POST_CONFIRM:撮合确认期(MATCH_STAGE_CONFIRM,confirm_timeout 默认 15s)全员点
//     「接受」才拉 DS;缺席者超时 / 拒绝 → match FAILED,含缺席者的票据判过错删除,
//     其余票据保排队时长退回队列。此时 DS 尚未分配,没有任何人被拉进对局。
//
// 方案 A 的另一半 —— 锁内冻结名单、秒级租约、收据幂等重入 —— **两种模式下都原样保留**:
// 它们消除的是「组票 vs 自动摘人」的 TOCTOU 与「响应丢失重试」的重复消费,与 ready 无关。
// EndTeamMatch 同样保留(滚动升级共存窗口 + 清残留 ready 的兼容路径)。
//
// Begin 在两种模式下都清掉 ready 位并把队伍转回 FORMING。PRE_READY 下这是**必需**的:
// 一次准备只授权一次开局,不清就等于队长能拿同一次准备连开两局(正是方案 A 要堵的形状);
// POST_CONFIRM 下则是为了存量客户端 —— 它们还在发 SetReady,残留 ready 不清,面板会在
// 开局后继续显示「已准备」。
//
// # 两条路径
//
//  1. **重入**:同一 attempt 的重试(响应丢失)。返回收据里那份冻结名单,不再重复冻结。
//     判定同时看 attempt_id 与冻结后代际,见 MatchStartReceipt(proto)与 receiptReentry。
//     注:POST_CONFIRM 下 ready 不是门槛,名单未变的连续两次真实开局(同 (team,captain)
//     派生的 attempt_id)在重入窗内也会命中此路径 —— 拿回的收据名单与当前名单**逐字节
//     相同**,方向安全;名单一变代际必前进,不可能拿回旧名单。PRE_READY 下第二次开局
//     必然先经过一次 SetReady(否则过不了门槛),代际已前进,不会命中重入。
//  2. **正常冻结**:校验通过(PRE_READY 时含 READY 闸)后留快照 → 清 ready → 转 FORMING
//     → 上租约 → 开收据。
//     (原「legacy 零代际 READY 作废」路径已删:那是 2026-08-13 一次性迁移的产物,
//     升级窗口早已过去;本次恢复门槛不需要它 —— 门槛只看当前 State,不追溯旧 ready 用过没。)
//
// # 租约仍然要上(消除 TOCTOU 的那一半不变)
//
// matchmaker 原先只读 GetTeam 取名单,与本服务的自动摘人分属两把锁,凑不出共同线性化点。
// 在这里上锁后,「冻结名单」与「移除离线成员」落在同一把 team 乐观锁上,两者只能有一个赢。
func (u *TeamUsecase) BeginTeamMatch(
	ctx context.Context, teamID, captainID uint64, operationID string, leaseMs int64, requireReady bool,
) (*teamv1.TeamStorageRecord, int64, error) {
	started := time.Now()
	if teamID == 0 || captainID == 0 || operationID == "" {
		// 一个 if 三个条件 → 三个 reason 分不开时,至少把三项原值一起打出来:
		// matchmaker 传空 operation_id 与传空 captain_id 的排查方向完全不同。
		plog.With(ctx).Warnw("msg", "team_match_start_rejected",
			"reason", reasonMissingArg,
			"team_id", teamID, "captain_id", captainID,
			"operation_id", operationID, "lease_ms", leaseMs)
		return nil, 0, errcode.New(errcode.ErrInvalidArg, "team_id, captain_id and operation_id required")
	}
	lease := time.Duration(leaseMs) * time.Millisecond
	if lease < matchLockMinLease {
		lease = matchLockMinLease
	}
	if lease > matchLockMaxLease {
		lease = matchLockMaxLease
	}

	var (
		// snapshot 是**冻结时刻**的名单,返回给 matchmaker 建票;重入路径下来自收据。
		snapshot *teamv1.TeamStorageRecord
		// committed 是本次提交落定后的队伍(含推进后的代际),用于推送与状态机日志。
		committed   *teamv1.TeamStorageRecord
		expiresAtMs int64
		reentered   bool
	)
	// 锁内看到的判定依据只用于日志:乐观锁重试时会被覆盖成最后一轮的取值,
	// 那正是本次提交真正依据的那一轮(与 team.go 各写路径同一套写法)。
	var (
		rejectReason  string
		prevState     teamv1.TeamState
		prevCaptainID uint64
		prevMembers   int
		prevReady     int
		lockUntilMs   int64
		lockOwnerOp   string
	)
	if err := u.updateTeamThenStamp(ctx, teamID, func(team *teamv1.TeamStorageRecord) error {
		// 乐观锁重试会重跑整个闭包 → 上一轮的输出必须清干净,否则「前一轮判成重入、
		// 这一轮正常冻结」会带着上一轮的 snapshot/标志位污染本轮结果。
		snapshot, reentered = nil, false
		prevState = team.State
		prevCaptainID = team.CaptainId
		prevMembers = len(team.Members)
		prevReady = readyCount(team.Members)
		lockUntilMs = team.GetMatchLockUntilMs()
		lockOwnerOp = team.GetMatchLockOperationId()
		if team.State == stateDisbanded {
			rejectReason = reasonTeamDisbanded
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		now := time.Now().UnixMilli()

		// ── 路径 1:重入(同一 attempt 的重试) ────────────────────────────────
		//
		// 必须**先于**队长与 READY 校验:此刻队伍已因上一次消费转成 FORMING,任何按
		// READY 判定的门都会把自己的重试拒掉;队长若恰在重试间隙转移,再验队长同样会拒。
		// 对一次**已提交成功**的操作重新做授权判定,正是端到端幂等被打破的经典形状(§9.23)。
		// 收据本身就是「这次 attempt 已被授权并提交过」的证据;verifyMatchCall 已把住调用方。
		if rec := team.GetMatchStartReceipt(); receiptReentry(rec, operationID, team.GetReadyGeneration(), now) {
			if team.MatchLockUntilMs > now && team.MatchLockOperationId != operationID {
				// 收据还有效但租约已被另一次组票拿走 —— 理论上只在收据窗内换队长再开局
				// 时可达。不抢锁,按正常竞争退避。
				rejectReason = reasonRosterLocked
				return errcode.New(errcode.ErrTeamConcurrent,
					"team %d roster locked by operation %s until %d",
					teamID, team.MatchLockOperationId, team.MatchLockUntilMs)
			}
			rejectReason = ""
			expiresAtMs = now + lease.Milliseconds()
			team.MatchLockUntilMs = expiresAtMs
			team.MatchLockOperationId = operationID
			team.UpdatedAtMs = now
			// 用收据重建「冻结那一刻」的队伍:当前记录的 ready 已被清空、状态已转 FORMING,
			// 直接返回当前记录等于把一份空 ready 的名单交给 matchmaker 建票。
			// State 置 READY 只为与首次返回的快照形状一致 —— matchmaker 不读 State。
			snapshot = cloneTeam(team)
			snapshot.Members = cloneMembers(rec.GetRoster())
			snapshot.State = stateReady
			snapshot.ReadyGeneration = rec.GetConsumedReadyGeneration()
			snapshot.MatchStartReceipt = nil
			reentered = true
			return nil
		}

		if team.CaptainId != captainID {
			rejectReason = reasonNotCaptain
			return errcode.New(errcode.ErrTeamNotCaptain, "player %d is not captain of team %d", captainID, teamID)
		}
		if team.MatchLockUntilMs > now && team.MatchLockOperationId != operationID {
			// 另一次组票的租约还没到期。这是正常竞争(并发重试 / 罕见的换 attempt),
			// 返回可重试错误 —— 几秒后租约自净,客户端重试即可。
			rejectReason = reasonRosterLocked
			return errcode.New(errcode.ErrTeamConcurrent,
				"team %d roster locked by operation %s until %d", teamID, team.MatchLockOperationId, team.MatchLockUntilMs)
		}

		// ── PRE_READY 图的准备门槛(关卡表 ready_mode=1) ──────────────────────
		//
		// 必须放在**租约冲突判定之后**:走到这里队伍可能刚被上一次冻结转成 FORMING,
		// 先判 READY 会把一个「稍后重试即可」的暂态说成「队伍未准备」这种终态 ——
		// 客户端会据此引导玩家去重新点准备,而其实几秒后租约一过什么都不用做。
		//
		// POST_CONFIRM 图(requireReady=false,含关卡表留空与旧 matchmaker 不发该字段)
		// 整道跳过,行为与本字段上线前逐字节一致。
		if requireReady && team.State != stateReady {
			rejectReason = reasonTeamNotReady
			return errcode.New(errcode.ErrTeamWrongState,
				"team %d not ready (state=%d, map requires pre-match ready)", teamID, team.State)
		}

		// ── 路径 2:正常冻结 ──────────────────────────────────────────────────
		//
		// 先留冻结快照(matchmaker 建票的输入),再清 ready —— 顺序不能反,
		// 反了快照里的 ready 位就全是假的(PRE_READY 图靠它对账,存量客户端靠它显示)。
		rejectReason = ""
		snapshot = cloneTeam(team)
		for i := range team.Members {
			team.Members[i].Ready = false
		}
		team.State = stateForming
		expiresAtMs = now + lease.Milliseconds()
		team.MatchLockUntilMs = expiresAtMs
		team.MatchLockOperationId = operationID
		team.UpdatedAtMs = now
		return nil
	}, func(team *teamv1.TeamStorageRecord) {
		// 盖章在代际推进**之后**:收据要记的是冻结后代际(receiptReentry 的 CAS 依据)。
		// 手动去猜「当前值 + 1」等于复刻 updateTeam 内部实现,推进规则一变就静默劈叉。
		if snapshot != nil && !reentered {
			team.MatchStartReceipt = &teamv1.MatchStartReceipt{
				AttemptId:               operationID,
				Roster:                  cloneMembers(snapshot.GetMembers()),
				ConsumedReadyGeneration: snapshot.GetReadyGeneration(),
				PostReadyGeneration:     team.GetReadyGeneration(),
				CreatedAtMs:             team.GetUpdatedAtMs(),
			}
		}
		committed = cloneTeam(team)
	}, u.activeTTL()); err != nil {
		// 锁内没给出 reason 的只剩「队伍不存在 / 乐观锁重试耗尽 / Redis 故障」三类。
		if rejectReason == "" {
			switch errcode.As(err) {
			case errcode.ErrTeamNotFound:
				rejectReason = reasonTeamNotFound
			case errcode.ErrTeamConcurrent:
				rejectReason = reasonOptimisticRetryExhausted
			default:
				rejectReason = reasonStoreWriteFailed
			}
		}
		// R2:这是「队长点了开始匹配却没进匹配」的**唯一**服务端证据。matchmaker 侧
		// 只看得到一个错误码,team 侧不打就两边都断线。依据字段(队伍状态 / 人数 /
		// ready 数 / 当前租约持有者)必须同框,否则查不出到底是没准备还是被别人占着。
		plog.With(ctx).Warnw("msg", "team_match_start_rejected",
			"reason", rejectReason,
			"team_id", teamID, "captain_id", captainID,
			"operation_id", operationID,
			"team_state", prevState, "actual_captain_id", prevCaptainID,
			"members", prevMembers, "ready_count", prevReady,
			"lock_until_ms", lockUntilMs, "lock_operation_id", lockOwnerOp,
			"elapsed_ms", time.Since(started).Milliseconds(), "err", err)
		return nil, 0, err
	}

	if !reentered {
		// 残留 ready 已被清掉 → 队伍已转 FORMING。必须推送 + 同步招募索引,否则存量
		// 客户端面板一直显示全员已准备,且 FORMING 队伍不回到招募列表。
		u.publishReadyCleared(ctx, committed)
		logTeamStateChanged(ctx, teamID, prevState, committed.State, "match_start_consume",
			committed.GetReadyGeneration(), len(committed.Members))
	}

	// R1:上租约锁 = 这一局的名单从此刻起被冻结(不可逆推进,且是 matchmaker 建票的输入)。
	plog.With(ctx).Infow("msg", "team_match_roster_locked",
		"team_id", teamID, "captain_id", captainID, "operation_id", operationID,
		"reentry", reentered,
		"lease_ms", lease.Milliseconds(), "expires_at_ms", expiresAtMs,
		"members", len(snapshot.Members), "ready_count", readyCount(snapshot.Members),
		"consumed_ready_generation", snapshot.GetReadyGeneration(),
		"post_ready_generation", committed.GetReadyGeneration(),
		"elapsed_ms", time.Since(started).Milliseconds())
	// 冻结那一刻每个成员的 (id, ready, hero) 三元组。INC-20260813-001(3v3 打成 3v2)
	// 事后唯一能还原"到底是谁没进去"的取证点。
	// reentry:同 attempt 重试,名单来自收据,未再次消费 ready。
	logMatchRosterFrozen(ctx, teamID, captainID, operationID, snapshot, reentered)
	return snapshot, expiresAtMs, nil
}

// receiptReentry 判断本次请求是不是「同一次尝试的重试」。
//
// 三个条件缺一不可:
//  1. attempt_id 相同 —— 是同一次尝试;
//  2. 当前代际仍等于收据记下的**消费后**代际 —— 期间没有任何人动过 ready。
//     动过就说明这是一次**新的**开局意图,必须重新消费,而不是拿回旧名单。
//     这一条同时让 matchmaker 那个跨局复用的 operation_id((team,captain) 派生)安全:
//     打完一局全队重新点准备后再开,代际已前进,不会被误判成上一局的重试;
//  3. 还在重入窗内 —— 见 matchStartReceiptWindow,防「很久以后的真实点击被当成重试」。
//
// 时钟回拨会让 now-created 变负,按「在窗内」处理(那只是把重试窗放宽,方向仍安全)。
func receiptReentry(rec *teamv1.MatchStartReceipt, attemptID string, currentGen uint64, nowMs int64) bool {
	if rec == nil || rec.GetAttemptId() == "" || rec.GetAttemptId() != attemptID {
		return false
	}
	if rec.GetPostReadyGeneration() != currentGen {
		return false
	}
	return nowMs-rec.GetCreatedAtMs() <= matchStartReceiptWindow.Milliseconds()
}

// rosterLockedForMatch 判断此刻是否有未过期的组票租约。只在 team 的乐观锁**内**调用。
func rosterLockedForMatch(team *teamv1.TeamStorageRecord) bool {
	return team.GetMatchLockUntilMs() > time.Now().UnixMilli()
}

// ── 兜底候选源:整队一起掉线时唯一能发现残留的路径 ──────────────────────────

// teamRosterSource 实现 offlinewatch.RosterSource:增量提名「有队伍的玩家」。
//
// # 为什么必须有它(2026-08-12 实测发现)
//
// 排期原本只有两条触发链,它们在同一种情况下**同时失效**:
//   - kafka 离场事件:依赖 Hub DS 走完 Logout 调 ReportDisconnect —— Hub DS 整机崩溃时不会调;
//   - GetMyTeam 读路径:依赖有活人打开组队面板 —— 整队都掉线时没人打开。
//
// 现场证据:两名玩家的 hubmeta 只有 last_alive_ms、没有 left_at_ms(印证 Hub 侧没走 Logout),
// 他们仍挂在队伍里,而调度队列是空的 —— 永久残留。
//
// last_alive_ms 解决的是「**能不能判**」,本类型解决的是「**谁来触发判**」,缺一不可。
// 注意本类型只负责**提名**,判定与排期全部由 Observe 走 locator 权威完成:
// 它不看时间、不做任何判断,因此不存在「用本地时钟猜离线」的风险。
type teamRosterSource struct {
	repo   data.TeamRepo
	cursor uint64

	// 以下三项只用于**观测一轮完整遍历要多久** —— 那是判断 RosterScanBatch 够不够大的
	// 唯一现场依据(见 offlinewatch.Options.RosterScanBatch 的公式)。纯进程内,
	// 重启即清空,不写任何存储(§16.10 调度状态不得寄生到业务数据上)。
	sweepStartedAt time.Time
	scannedInRound int
	rounds         int
}

// NextBatch 每轮取一小段游标。游标只存进程内:它是纯调度提示,重启从头扫一遍即可,
// 不写进任何权威或派生存储(§16.10)。
//
// 游标回到 0 表示「本次完整遍历结束」,此时打一条 Info 记录这一轮的耗时与候选数 ——
// 运维据此核对「全量扫描周期 <= threshold」是否成立;不成立就得调大 RosterScanBatch,
// 否则残留成员会等远超阈值的时间才被复查。
func (s *teamRosterSource) NextBatch(ctx context.Context, limit int) ([]uint64, error) {
	if s.sweepStartedAt.IsZero() {
		s.sweepStartedAt = time.Now()
	}
	ids, next, err := s.repo.ScanPlayerIndex(ctx, s.cursor, int64(limit))
	if err != nil {
		// R2:扫描是「整队一起掉线」时唯一的候选来源。它持续失败 = 残留成员永远
		// 没人提名,而调用方只看到一个 error 往上抛。带上游标位置,便于判断是不是
		// 卡在同一段 keyspace(而不是偶发抖动)。
		plog.With(ctx).Warnw("msg", "team_roster_scan_failed",
			"reason", reasonStoreReadFailed,
			"cursor", s.cursor, "limit", limit, "round", s.rounds,
			"scanned_in_round", s.scannedInRound, "err", err)
		return nil, err
	}
	s.cursor = next
	s.scannedInRound += len(ids)

	if next == 0 { // 一轮遍历结束
		s.rounds++
		elapsed := time.Since(s.sweepStartedAt)
		plog.With(ctx).Infow("msg", "team_roster_full_scan_completed",
			"round", s.rounds, "scanned", s.scannedInRound, "elapsed", elapsed.String(),
			"hint", "elapsed 必须明显小于 offline_leave.threshold;否则调大 offlinewatch RosterScanBatch")
		s.sweepStartedAt = time.Time{}
		s.scannedInRound = 0
	}
	return ids, nil
}

// NewTeamRosterSource 构造兜底候选源。
func NewTeamRosterSource(repo data.TeamRepo) offlinewatch.RosterSource {
	return &teamRosterSource{repo: repo}
}
