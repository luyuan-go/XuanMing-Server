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
	teamID, found, err := u.repo.GetPlayerTeamID(ctx, playerID)
	if err != nil {
		return err // 读不通 → 下轮重来,绝不当成「他没队伍」
	}
	if !found || teamID == 0 {
		return nil
	}
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		return err
	}
	// 终态 / 已不含该成员 / 单人队一律不动。旧 player→team 索引的收敛是 OnPlayerOffline
	// 的职责(它有 due/claim 闭环能保证重试),本函数不重复那条链。
	if !found || team.State == stateDisbanded || !hasMember(team, playerID) || len(team.Members) <= 1 {
		return nil
	}

	result, err := u.clearMemberReady(ctx, teamID, readyClearOpts{targets: []uint64{playerID}})
	if err != nil {
		return err
	}
	if result == nil {
		return nil // 并发路径已经改过了
	}
	u.publishReadyCleared(ctx, result)
	plog.With(ctx).Infow("msg", "team_presence_lost_unready",
		"team_id", teamID, "player_id", playerID, "since_ms", sinceMs,
		"new_state", result.State, "members", len(result.Members))
	return nil
}

// EndTeamMatch 对局结束后复位队伍准备状态(matchmaker 在 ReleaseMatch 时调用)。
//
// # 这是本事故的**第一根因**(INC-20260813-001 v2)
//
// 在此之前 team 侧**没有任何一条 match-ended 路径** —— 全仓只有 LeaveTeam / Kick /
// 离线摘人会把 State 打回 FORMING。于是一局打完,队伍仍停在 TEAM_STATE_READY、
// 全员 ready 标记原样保留,队长可以在队友还卡在结算界面 / 回大厅路上的时候立刻再开一局。
// 事故当天队员阵亡后先退了战斗(比结算还早 13 秒),队长距他退出仅 **75 秒**就开了下一局,
// 而他此时才刚 login 回来、停在选角界面 —— 于是被原样冻进新票据,DS 拿到 6 人只进来 5 个。
//
// 注意这跟「掉线」无关:他全程没有任何异常,只是**连打第二局的正常窗口**。
// 所以光靠离线判定(不论阈值多短)都堵不住,必须有这条 match-ended 复位。
//
// # 语义
//
// 打完一局回大厅 = 必须重新点准备。除了堵住上述窗口,它也符合玩家预期(避免"手滑连开")。
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
	if teamID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "team_id required")
	}
	result, err := u.clearMemberReady(ctx, teamID, readyClearOpts{
		targets:     playerIDs,
		expectedGen: expectedGen,
	})
	if err != nil {
		if errors.Is(err, offlinewatch.ErrDeferred) {
			// 组票租约在手 = 队长已经开了**下一局**。租约是秒级自净的,
			// 交给上游 outbox 下一轮重投即可(此时改 ready 会与那一局的冻结名单打架)。
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
		"new_state", result.State, "members", len(result.Members))
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
}

func (u *TeamUsecase) clearMemberReady(
	ctx context.Context, teamID uint64, opts readyClearOpts,
) (*teamv1.TeamStorageRecord, error) {
	targets := opts.targets
	var result *teamv1.TeamStorageRecord
	if err := u.updateTeam(ctx, teamID, func(team *teamv1.TeamStorageRecord) error {
		if team.State == stateDisbanded {
			return errReadyNoChange
		}
		// 跨代 CAS:代际已经往前走了,说明这条复位对应的那一局早已翻篇
		// (玩家重新点了准备 / 离队重入 / 队长开了新局)。按幂等成功处理,绝不能抹掉新意图。
		if opts.expectedGen != 0 && team.GetReadyGeneration() != opts.expectedGen {
			return errReadyNoChange
		}
		// ★ 与 matchmaker 的共同线性化点,同 removeOfflineMember:BeginTeamMatch 已经
		// (或正在)把这份名单冻进票据,这时改 ready 会让票据里的快照与队伍打架。
		// 租约是秒级自净,推迟一轮即可。
		if rosterLockedForMatch(team) {
			return offlinewatch.ErrDeferred
		}
		if !readyNeedsClearing(team, targets) {
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
			return nil, nil // 队伍已没了 / 已终态:本就没什么可复位的
		case errcode.As(err) == errcode.ErrTeamConcurrent:
			// 乐观锁重试耗尽,暂态。用 ErrDeferred 而非普通 error,免得正常竞争刷 Warn。
			return nil, offlinewatch.ErrDeferred
		default:
			return nil, err
		}
	}
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

	if err := u.updateTeam(ctx, teamID, func(team *teamv1.TeamStorageRecord) error {
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
	if err := u.updateTeam(ctx, teamID, func(team *teamv1.TeamStorageRecord) error {
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
