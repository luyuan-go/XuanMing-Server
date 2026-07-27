// progress.go — 战斗中实时进度通道业务逻辑(实时成长,docs/design/realtime-progression.md)。
//
// 链路:DS 异步批量 ReportProgress(事实事件,seq 幂等)
//
//	→ 本文件:校验(roster / 上限 / 升序)→ 换算(怪物经验表 / 掉落白名单,DS 不可信)
//	→ repo.ApplyProgress(水位 CAS + 进度出箱同事务)
//	→ RunProgressPublisher:出箱 worker 幂等调 player.AddExperience / inventory.GrantInstances。
//
// 错误语义(DS 侧行为契约,battle.proto ReportProgress 注释):
//   - ErrUnavailable(水位竞争 / DB 瞬时)→ DS 原批重试;
//   - ErrInvalidArg / ErrUnauthorized(坏批 / 越权)→ DS 丢批告警,继续后续批;
//   - ErrInvalidState(对局已结算 / 通道关闭 / 未知事实类型)→ DS 停流,不得无限重试
//     (未知事实 = 新 DS 对旧 Go,能力不匹配是整场性质的,丢批语义会造成逐批永久丢失)。
//     停流后果:已 ACK 部分保持有效;水位>0 时结算掉落发放保持抑制(单一权威路径),
//     停流之后的拾取 / 经验不结算兜底,本场剩余实时奖励永久丢失,错误日志告警留证
//     (该场景只该出现在违反 Go 先行发布纪律时,不为违纪场景做兜底)。
package biz

import (
	"context"
	"sort"
	"strconv"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"

	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/data"
)

// ExperienceGranter 把击杀经验幂等入账到 player(AddExperience,系统 RPC)。
//
// 由 RunProgressPublisher 调用:失败 → 返回 error → 进度出箱行保留下轮重试
// (at-least-once,配合 player exp_history uk 去重)。实现可为 nil:player_addr 未配
// → 经验出箱行积压不丢(地址配好重启后补发,与掉落 granter 同语义)。
type ExperienceGranter interface {
	AddExperience(ctx context.Context, playerID uint64, expDelta uint64, reason, idempotencyKey string) error
}

// SetExperienceGranter 注入 player 经验入账器(nil-safe,风格同 SetInstanceGranter)。
func (u *BattleResultUsecase) SetExperienceGranter(g ExperienceGranter) {
	u.expGranter = g
}

// ReportProgress 处理 DS 的一批进度事实事件,返回已应用水位 acked_seq。
//
// roster 是凭据检查器从权威 BattleStorageRecord 取的本场玩家名单(service 层注入);
// nil = dev / guard off 模式,跳过成员校验(生产 authority_mode=redis 恒非 nil)。
func (u *BattleResultUsecase) ReportProgress(ctx context.Context, matchID uint64, roster []uint64, events []*battlev1.BattleProgressEvent) (uint64, error) {
	if matchID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	if len(events) == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "events required")
	}
	if maxBatch := u.cfg.MaxProgressBatchOrDefault(); len(events) > maxBatch {
		return 0, errcode.New(errcode.ErrInvalidArg, "batch size %d exceeds max %d", len(events), maxBatch)
	}
	var rosterSet map[uint64]struct{}
	if roster != nil {
		rosterSet = make(map[uint64]struct{}, len(roster))
		for _, pid := range roster {
			rosterSet[pid] = struct{}{}
		}
	}

	wm, err := u.repo.GetProgressWatermark(ctx, matchID)
	if err != nil {
		return 0, err
	}
	if wm.Settled {
		// 结算后仍收到进度 = 僵尸 / 分区恢复 DS 迟到上报(水位表终局标记挡住,§9.22 / §9.4 fencing);
		// ErrInvalidState 是业务码不被 access log 当故障 → 显式 WARN 留证。
		plog.With(ctx).Warnw("msg", "progress_rejected_settled", "match_id", matchID, "events", len(events))
		return 0, errcode.New(errcode.ErrInvalidState, "match %d already settled, progress rejected", matchID)
	}
	if wm.Stopped {
		// 停流标记持久化(审计 P1):未知事实停流后,违纪 DS 再发只含已知事实的批
		// 也一律拒,禁止重新开流(契约:整场停流、剩余实时奖励永久丢失)。
		plog.With(ctx).Warnw("msg", "progress_rejected_stream_stopped", "match_id", matchID, "events", len(events))
		return 0, errcode.New(errcode.ErrInvalidState,
			"match %d progress stream permanently stopped, rejected", matchID)
	}
	// 每场模式以水位行存在性固化(§22 单一权威):行已存在 = 本场发放权已归实时通道,
	// killswitch 中途关闭不影响进行中对局(否则"部分实时 + 结算掉落被整体抑制"会丢奖);
	// 行不存在时由开关决定能否开流(默认关,混版发布纪律见 conf.ProgressEnabled)。
	if !wm.Existed && !u.cfg.ProgressEnabled {
		// 固化"本场 legacy 结算模式"权威事实(审计 P1:只返回错误不落标记时,对局
		// 中途重新开启配置会让同一对局**晚开流**,与已按 legacy 进行的前半场混用)。
		// 停流标记行 lastSeq=0 → 结算路径正常发放全部产出(零实时入账,无双发面)。
		// 认领必须是"无行才创建"(审计 R4 #11):滚动混版下开启副本可能在本副本读
		// 水位之后、认领之前已创建行开流,upsert 会把那条**合法已开的流**停掉——
		// 认领失败(claimed=false)= 输给并发写者,重读水位按现行状态继续。
		// 认领错误保持可重试(与未知事实路径同纪律)。
		claimed, merr := u.repo.ClaimProgressLegacy(ctx, matchID)
		if merr != nil {
			plog.With(ctx).Errorw("msg", "progress_claim_legacy_failed_retryable", "match_id", matchID, "err", merr)
			return 0, errcode.New(errcode.ErrUnavailable,
				"progress channel disabled and legacy claim persist failed, retry: %v", merr)
		}
		if claimed {
			return 0, errcode.New(errcode.ErrInvalidState, "realtime progress channel disabled")
		}
		// 竞态输给已存在的行:可能是开启副本已开流(继续在途流程入账)、已认领
		// legacy、或已结算——重读后按与首读相同的裁决顺序处理。
		wm, err = u.repo.GetProgressWatermark(ctx, matchID)
		if err != nil {
			return 0, err
		}
		if wm.Settled {
			return 0, errcode.New(errcode.ErrInvalidState, "match %d already settled, progress rejected", matchID)
		}
		if wm.Stopped {
			return 0, errcode.New(errcode.ErrInvalidState,
				"match %d progress stream permanently stopped, rejected", matchID)
		}
		if !wm.Existed {
			// INSERT IGNORE 没生效行却不存在(并发删除/复制异常):按瞬时态重试收敛。
			return 0, errcode.New(errcode.ErrUnavailable, "progress legacy claim raced, retry")
		}
		plog.With(ctx).Warnw("msg", "progress_disabled_replica_joins_open_stream",
			"match_id", matchID, "last_seq", wm.LastAppliedSeq,
			"hint", "通道关闭副本遇到已开流对局(滚动混版/killswitch 中途关闭),按已开流继续入账")
	}
	lastSeq := wm.LastAppliedSeq

	// 事实换算(DS 不可信):怪物经验查配置表,拾取过白名单;未知怪 / 非白名单物品跳过并告警
	// (只丢该事实的发放,水位照常推进 —— 坏配置不能把整条流卡死)。
	maxSeqCap := u.cfg.MaxProgressSeqPerMatchOrDefault()
	maxKill := u.cfg.MaxKillCountPerFactOrDefault()
	maxPickup := u.cfg.MaxPickupCountPerFactOrDefault()
	expByPlayer := make(map[uint64]uint64)
	itemsByPlayer := make(map[uint64]uint32)
	killsByPlayer := make(map[uint64]uint32)
	var (
		itemRows       []data.ProgressOutboxRecord
		prevSeq        uint64
		newSeq         = lastSeq
		prevAppliedSeq = lastSeq
		skippedFact    int
		batchExp       uint64
		batchItems     uint32
	)
	// 模式 C:漏配 / 非白名单事实原先在事件循环内**逐条**打 Warn——一张漏配的怪物经验表
	// 会让「该怪每次击杀 × 每个玩家 × 每场对局」都刷一行。改为按 config_id 去重收集
	// (distinct 计数 + 样例 ID),批末汇总一条:既能定位是哪个 ID 漏配,又不随击杀数刷屏。
	unconfiguredMonsters := map[uint32]struct{}{}
	notWhitelistedItems := map[uint32]struct{}{}
	var sampleMonsterID, sampleItemID uint32
	for _, e := range events {
		seq := e.GetSeq()
		if seq == 0 || seq <= prevSeq {
			return 0, errcode.New(errcode.ErrInvalidArg, "event seq must be ascending (seq=%d prev=%d)", seq, prevSeq)
		}
		prevSeq = seq
		if seq > maxSeqCap {
			// seq 硬上限拒收也要告警(契约:拒收并告警;与累计 cap 的 progress_cap_rejected
			// 同名同监控面——失陷 DS 刷 seq 的第一现场信号)。
			plog.With(ctx).Errorw("msg", "progress_cap_rejected",
				"match_id", matchID, "kind", "seq", "seq", seq, "cap", maxSeqCap)
			return 0, errcode.New(errcode.ErrInvalidArg, "event seq %d exceeds per-match cap %d", seq, maxSeqCap)
		}
		if seq <= lastSeq {
			continue // 旧事件重放(at-least-once),已入账,跳过
		}
		if seq > prevAppliedSeq+1 {
			// seq 跳号合法(DS 有界缓冲满载丢最老事件,realtime-progression.md §3/§9),
			// 但必须留痕(批首与批内统一检测,批内 [1,3] 同样告警):跳过的 seq 永不再来,
			// 结算对账 gap 告警的先导信号。
			plog.With(ctx).Warnw("msg", "progress_seq_gap",
				"match_id", matchID, "prev_applied", prevAppliedSeq, "next", seq)
		}
		prevAppliedSeq = seq
		newSeq = seq
		playerID := e.GetPlayerId()
		if playerID == 0 {
			return 0, errcode.New(errcode.ErrInvalidArg, "event %d missing player_id", seq)
		}
		if rosterSet != nil {
			if _, ok := rosterSet[playerID]; !ok {
				// DS 为非本场玩家上报进度事实 = 越权 / 失陷 DS 的直接信号(§9.6 owner 授权 / roster 门)。
				plog.With(ctx).Warnw("msg", "progress_roster_reject",
					"match_id", matchID, "player_id", playerID, "seq", seq)
				return 0, errcode.New(errcode.ErrUnauthorized, "player %d not in match %d roster", playerID, matchID)
			}
		}
		switch fact := e.GetFact().(type) {
		case *battlev1.BattleProgressEvent_MonsterKill:
			cnt := fact.MonsterKill.GetCount()
			if cnt == 0 || cnt > maxKill {
				return 0, errcode.New(errcode.ErrInvalidArg, "kill count %d out of range (max %d)", cnt, maxKill)
			}
			// 击杀计数在经验换算前累计:未配置经验的怪也计入单玩家击杀上限,
			// 失陷 DS 不能靠刷未知怪 ID 绕过反作弊额度。
			killsByPlayer[playerID] += cnt
			expPer, ok := u.cfg.MonsterExpOf(fact.MonsterKill.GetMonsterConfigId())
			if !ok {
				skippedFact++
				id := fact.MonsterKill.GetMonsterConfigId()
				if _, seen := unconfiguredMonsters[id]; !seen {
					unconfiguredMonsters[id] = struct{}{}
					if sampleMonsterID == 0 {
						sampleMonsterID = id
					}
				}
				continue
			}
			expByPlayer[playerID] += expPer * uint64(cnt)
			batchExp += expPer * uint64(cnt)
		case *battlev1.BattleProgressEvent_ItemPickup:
			cnt := fact.ItemPickup.GetCount()
			if cnt == 0 || cnt > maxPickup {
				return 0, errcode.New(errcode.ErrInvalidArg, "pickup count %d out of range (max %d)", cnt, maxPickup)
			}
			itemID := fact.ItemPickup.GetItemConfigId()
			if itemID == 0 || !u.cfg.IsDroppable(itemID) {
				skippedFact++
				if _, seen := notWhitelistedItems[itemID]; !seen {
					notWhitelistedItems[itemID] = struct{}{}
					if sampleItemID == 0 {
						sampleItemID = itemID
					}
				}
				continue
			}
			// 每拾取事实一行出箱(Seq=事实自身 seq,uk 天然唯一):单事实 count 已被
			// 夹紧到 CSV 列宽内,合法掉落永不截断(审计 P1;拾取低频,行数有界)。
			items := make([]uint32, cnt)
			for i := range items {
				items[i] = itemID
			}
			itemRows = append(itemRows, data.ProgressOutboxRecord{
				MatchID: matchID, Seq: seq, PlayerID: playerID,
				Kind: data.ProgressGrantItem, ItemConfigIDs: items,
			})
			batchItems += cnt
			itemsByPlayer[playerID] += cnt
		default:
			// 未知事实类型 = 能力不匹配(新 DS 对旧 Go),是整场性质而非单批坏数据:
			// ErrInvalidArg 的"丢批继续"语义会让 DS 逐批丢弃所有含新事实的批(永久丢失,
			// 审计 P1)。改用 ErrInvalidState → DS 停流。停流后果(审计明示,proto 同步):
			// 水位>0 时结算掉落发放保持抑制,停流之后的拾取 / 经验不结算兜底,本场剩余
			// 实时奖励永久丢失——该场景只该出现在违反发布纪律时,不为违纪场景做兜底。
			// 新 DS 携带新事实类型必须先升级 battle_result 全 fleet 再放量
			// (与 conf.ProgressEnabled 混版纪律同向:Go 先行,DS 后行)。
			// 停流标记持久化:防违纪 DS 用后续"只含已知事实"的批重新开流(审计 P1)。
			// 标记失败必须**保持可重试**(审计 P1:吞掉失败直接返回终态 InvalidState 会让
			// DS 永久停流而库里没有标记,后续已知批仍可能被接受)——返回 ErrUnavailable,
			// DS 原批重试 → 再次命中未知事实 → 重试落标记,收敛后才返回停流终态。
			// "已停流"日志只在标记成功后打(审计 R4 P2:先打日志再落标记,标记失败时
			// 日志与库状态矛盾,排障会按已停流处理)。
			if merr := u.repo.MarkProgressStopped(ctx, matchID); merr != nil {
				plog.With(ctx).Errorw("msg", "progress_mark_stopped_failed_retryable", "match_id", matchID, "err", merr)
				return 0, errcode.New(errcode.ErrUnavailable,
					"unknown progress fact seq=%d and stop marker persist failed, retry batch: %v", seq, merr)
			}
			plog.With(ctx).Errorw("msg", "progress_unknown_fact_stream_stopped",
				"match_id", matchID, "seq", seq,
				"hint", "新 DS 事实类型早于 battle_result 升级放量(违反 Go 先行纪律),本场已停流,停流后实时奖励永久丢失")
			return 0, errcode.New(errcode.ErrInvalidState, "unknown progress fact seq=%d: upgrade battle_result fleet before enabling new DS fact types (stream stopped; remaining realtime rewards for this match are permanently lost)", seq)
		}
	}

	// 漏配 / 非白名单汇总(模式 C):按 config_id 去重后每批至多一条。distinct 计数暴露
	// 「漏了几个 ID」,样例 ID 给出排查落点;非白名单还可能是失陷 DS 在刷拾取,故保持 Warn。
	if len(unconfiguredMonsters) > 0 || len(notWhitelistedItems) > 0 {
		plog.With(ctx).Warnw("msg", "progress_facts_skipped",
			"match_id", matchID, "skipped_facts", skippedFact,
			"unconfigured_monster_ids", len(unconfiguredMonsters), "sample_monster_config_id", sampleMonsterID,
			"not_whitelisted_item_ids", len(notWhitelistedItems), "sample_item_config_id", sampleItemID,
			"hint", "怪物经验表漏配 / 掉落白名单漏项,或失陷 DS 上报未授权事实")
	}

	if newSeq == lastSeq {
		// 整批都是旧事件(原批重发)→ 纯重放 ACK,零副作用。
		return lastSeq, nil
	}

	// 单场 / 单玩家累计上限统一在 ApplyProgress 事务内的一致快照上判定(审计 P1:
	// 此处若按事务外读到的 wm / player totals 先判,与水位 CAS 分属不同快照——重试
	// 请求可能读到旧水位 + 首请求已提交的新累计,把同批 delta 重复计入后永久误拒,
	// DS 据契约丢批并释放拾取认领,而首请求出箱已提交 → 重新拾取可重复发放)。
	// 这里只聚合本批 delta:expByPlayer 键集 ⊆ killsByPlayer 键集(经验只源自击杀),
	// 触达玩家 = 击杀 ∪ 拾取。
	touched := make(map[uint64]struct{}, len(killsByPlayer)+len(itemsByPlayer))
	for pid := range killsByPlayer {
		touched[pid] = struct{}{}
	}
	for pid := range itemsByPlayer {
		touched[pid] = struct{}{}
	}
	playerDeltas := make([]data.ProgressPlayerDelta, 0, len(touched))
	for pid := range touched {
		playerDeltas = append(playerDeltas, data.ProgressPlayerDelta{
			PlayerID: pid, Exp: expByPlayer[pid], Items: itemsByPlayer[pid], Kills: killsByPlayer[pid],
		})
	}
	sort.Slice(playerDeltas, func(i, j int) bool { return playerDeltas[i].PlayerID < playerDeltas[j].PlayerID })
	caps := data.ProgressCaps{
		MatchExp:    u.cfg.MaxProgressExpPerMatchOrDefault(),
		MatchItems:  u.cfg.MaxProgressItemsPerMatchOrDefault(),
		PlayerExp:   u.cfg.MaxProgressExpPerPlayerOrDefault(),
		PlayerItems: u.cfg.MaxProgressItemsPerPlayerOrDefault(),
		PlayerKills: u.cfg.MaxProgressKillsPerPlayerOrDefault(),
	}

	rows := make([]data.ProgressOutboxRecord, 0, len(expByPlayer)+len(itemRows))
	for playerID, exp := range expByPlayer {
		if exp == 0 {
			continue // monster_exp 显式配 0(无经验怪):不产生 0 额度出箱行(player 拒收会永久重试)
		}
		rows = append(rows, data.ProgressOutboxRecord{
			MatchID: matchID, Seq: newSeq, PlayerID: playerID,
			Kind: data.ProgressGrantExp, ExpDelta: exp,
		})
	}
	rows = append(rows, itemRows...)

	if err := u.repo.ApplyProgress(ctx, matchID, lastSeq, newSeq, batchExp, batchItems, playerDeltas, rows, caps); err != nil {
		if errcode.As(err) == errcode.ErrInvalidArg {
			// 累计上限拒收告警(契约:拒收**并告警**,审计 P2):这是失陷 DS 刷产出的
			// 第一现场信号,不能只静默返回业务错误。biz 层 InvalidArg 前置校验都在
			// ApplyProgress 之前,此处 InvalidArg 只来自事务内单场/单玩家累计上限。
			plog.With(ctx).Errorw("msg", "progress_cap_rejected",
				"match_id", matchID, "batch_exp", batchExp, "batch_items", batchItems,
				"players", len(playerDeltas), "err", err)
		}
		return 0, err
	}
	plog.With(ctx).Debugw("msg", "battle_progress_applied",
		"match_id", matchID, "acked_seq", newSeq, "events", len(events),
		"grant_rows", len(rows), "skipped_facts", skippedFact,
		"batch_exp", batchExp, "batch_items", batchItems)
	return newSeq, nil
}

// ReconcileProgress 结算后对账:DS 上报的 final_progress_seq vs 服务端已应用水位。
// 缺口只告警不自动补(尾窗丢失,realtime-progression.md §9 明示的残余风险)。
func reconcileProgress(ctx context.Context, matchID, finalSeq uint64, info data.ProgressSettleInfo) {
	switch {
	case finalSeq == 0 && !info.StreamExisted:
		return // 本场未走实时通道(旧 DS / 通道关),无需对账
	case finalSeq > 0 && !info.StreamExisted:
		plog.With(ctx).Warnw("msg", "progress_reconcile_stream_missing",
			"match_id", matchID, "final_seq", finalSeq,
			"hint", "DS 声称走了实时通道但服务端无水位(全部批次丢失或伪造 final_seq)")
	case finalSeq != info.LastAppliedSeq:
		plog.With(ctx).Warnw("msg", "progress_reconcile_gap",
			"match_id", matchID, "final_seq", finalSeq, "applied_seq", info.LastAppliedSeq,
			"hint", "尾窗事件丢失(DS 崩溃/网络),只告警不自动补(realtime-progression.md §9)")
	default:
		plog.With(ctx).Debugw("msg", "progress_reconcile_ok",
			"match_id", matchID, "applied_seq", info.LastAppliedSeq)
	}
}

// ── 进度发放事务出箱发布器(at-least-once + 下游幂等)──────────────────────────

// RunProgressPublisher 启动后台进度出箱发放循环,直到 ctx 取消。
//
// 纪律同 RunDropPublisher:单行失败仅记录并 continue(保留出箱行下轮重试),不阻塞其他行;
// 对应下游客户端未注入(player_addr / inventory_addr 未配)的行原样跳过积压不丢。
// 背包满 + 已配 mail → 溢出转个人邮件(同键去重,直发链与邮件链至多一次)。
func (u *BattleResultUsecase) RunProgressPublisher(ctx context.Context) {
	if u.expGranter == nil && u.granter == nil {
		plog.With(ctx).Infow("msg", "progress_publisher_disabled",
			"hint", "player_addr / inventory_addr 均未配置 → 进度出箱积压不丢,配置后重启补发")
		return
	}
	interval := u.cfg.ProgressPublishIntervalOrDefault()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "progress_publisher_started",
		"interval", interval.String(), "batch", u.cfg.ProgressBatchSizeOrDefault())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "progress_publisher_stopped")
			return
		case <-ticker.C:
			safego.Run(ctx, "battle_progress_publisher", func() {
				if n, err := u.publishProgressBatch(ctx); err != nil {
					plog.With(ctx).Warnw("msg", "progress_publish_batch_failed", "granted", n, "err", err)
				}
			})
		}
	}
}

// publishProgressBatch 取一批进度出箱行发放,返回本轮成功发放并删除的条数。
// 单行失败 → deferRow 指数退避推迟(行不丢,坏行不会长期占满首批饿死后续正常行)。
func (u *BattleResultUsecase) publishProgressBatch(ctx context.Context) (int, error) {
	recs, err := u.repo.FetchProgressOutbox(ctx, u.cfg.ProgressBatchSizeOrDefault())
	if err != nil {
		return 0, err
	}
	granted := 0
	for _, r := range recs {
		switch r.Kind {
		case data.ProgressGrantExp:
			if u.expGranter == nil {
				u.deferProgressRow(ctx, r.ID) // player_addr 未配:积压不丢,退避防饿死 item 行
				continue
			}
			key := progressIdempotencyKey(r.MatchID, r.Seq, r.PlayerID, "exp")
			if gerr := u.expGranter.AddExperience(ctx, r.PlayerID, r.ExpDelta, "monster_kill", key); gerr != nil {
				plog.With(ctx).Warnw("msg", "progress_exp_grant_failed",
					"player_id", r.PlayerID, "exp", r.ExpDelta, "err", gerr)
				u.deferProgressRow(ctx, r.ID)
				continue
			}
		case data.ProgressGrantItem:
			if u.granter == nil {
				u.deferProgressRow(ctx, r.ID) // inventory_addr 未配:积压不丢
				continue
			}
			key := progressIdempotencyKey(r.MatchID, r.Seq, r.PlayerID, "item")
			if gerr := u.granter.GrantInstances(ctx, r.PlayerID, r.ItemConfigIDs, key); gerr != nil {
				// 背包满且已配 mail → 转个人邮件(同键,直发链与邮件链至多一次),成功后删行。
				if u.mailSender != nil && errcode.As(gerr) == errcode.ErrInventoryCapacityFull {
					if merr := u.mailSender.SendOverflowMail(ctx, r.PlayerID, r.ItemConfigIDs, key); merr != nil {
						plog.With(ctx).Warnw("msg", "progress_overflow_mail_failed",
							"player_id", r.PlayerID, "items", len(r.ItemConfigIDs), "err", merr)
						u.deferProgressRow(ctx, r.ID)
						continue
					}
					plog.With(ctx).Infow("msg", "progress_overflow_mailed",
						"player_id", r.PlayerID, "items", len(r.ItemConfigIDs))
				} else {
					plog.With(ctx).Warnw("msg", "progress_item_grant_failed",
						"player_id", r.PlayerID, "items", len(r.ItemConfigIDs), "err", gerr)
					u.deferProgressRow(ctx, r.ID)
					continue
				}
			}
		default:
			// 未知类型行(未来扩展 / 脏数据):告警并退避推迟,不删(人工介入)。
			plog.With(ctx).Warnw("msg", "progress_outbox_unknown_kind", "id", r.ID, "kind", r.Kind)
			u.deferProgressRow(ctx, r.ID)
			continue
		}
		if derr := u.repo.DeleteProgressOutbox(ctx, r.ID); derr != nil {
			return granted, derr
		}
		granted++
	}
	if granted > 0 {
		plog.With(ctx).Debugw("msg", "progress_outbox_granted", "count", granted)
	}
	return granted, nil
}

// deferProgressRow 推迟一条发放失败的出箱行(失败本身只告警:推迟失败下轮 Fetch 仍会
// 取到该行重试,不影响 at-least-once)。
func (u *BattleResultUsecase) deferProgressRow(ctx context.Context, id int64) {
	if err := u.repo.DeferProgressOutbox(ctx, id); err != nil {
		plog.With(ctx).Warnw("msg", "progress_outbox_defer_failed", "id", id, "err", err)
	}
}

// progressIdempotencyKey 组装进度发放幂等键:progress:{match_id}:{seq}:{player_id}:{kind}。
// 与 realtime-progression.md §3 / player.proto AddExperienceRequest 注释保持同一口径。
func progressIdempotencyKey(matchID, seq, playerID uint64, kind string) string {
	return "progress:" + strconv.FormatUint(matchID, 10) +
		":" + strconv.FormatUint(seq, 10) +
		":" + strconv.FormatUint(playerID, 10) +
		":" + kind
}
