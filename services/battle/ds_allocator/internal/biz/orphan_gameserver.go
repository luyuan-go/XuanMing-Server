// orphan_gameserver.go — 孤儿 Allocated GameServer 对账清扫(2026-08-03)。
//
// 问题:Agones 生命周期不回收 Allocated GameServer。一台 GS 若处于 Allocated 却在
// 权威存储里没有任何分配记录引用(记录已释放但外部删除失败且响应丢失、手工 GSA、
// 历史残留),它会**永久占位**锁死 Fleet 容量(实例:泄漏 18h 把 min=max=2 的匹配池
// 锁死)。而"人工确认无人后删除"已两次误删载人 DS(日志窗口/轮转/级别静默三重失真,
// 见工作区记忆 never-delete-allocated-gameserver-20260803)——人肉路径必须从流程上废除。
//
// 判定链(为什么"无记录 ⇒ 不可能有玩家",两个半边的依据**不同**,缺一不可):
//   ①「不可能再进来」由时长数学保证:玩家进入战斗 DS 的唯一通道是 DSTicket(§9.3),
//     票据只在持有分配记录时签发;记录先于 GSA POST 落库(Model B:
//     FenceBattleAllocation 成功才允许 POST)。故连续 orphanGSReclaimAfter
//     (默认 10min,≫ 票据硬上限 180s + ready_wait 120s)每轮都查无记录的 GS,
//     期间签不出、也不存在仍有效的进场票据。
//   ②「不可能已在内」**不由**时长数学保证,而由心跳停机契约闭合:记录消失
//     (如 BattleTTL 到期、Redis failover 丢 key)后,DS 下一拍心跳命中
//     ErrDSPodNotFound → 应答 commandStop,UE DS 收到即停心跳并 Agones Shutdown;
//     心跳打不通时 DS 按 §9.22 自我 fencing(失租即停玩)。也就是说本清扫删到
//     "载人却无记录"的 GS,前提是 DS 侧 stop/自我 fencing 契约同时失效——
//     谁要放松 UE DS 的「心跳被拒/失联即停机」行为,等于拆掉本清扫②的安全依据,
//     必须连本文件一起重新论证。
//   ③ ①②都还有一个隐含前提:本进程读的 Redis 权威与签票方/心跳处理方是**同一份**。
//     该前提按「危害向量」各有一套机制强制,不靠配置纪律:
//       - **清扫删除向量**:防误删④的 allocation 台账(见下)——空/错配 Redis 的
//         进程台账必空,一台都删不掉;
//       - **心跳指令向量**(读错权威的副本能否叫 DS 停机):由逐拍凭据 ACK 绑定承重
//         ——UE DS 只消费「响应携带与本次请求/当前 active 凭据五元组(InstanceUID/
//         InstanceEpoch/Gen/JTI/WriterEpoch)精确匹配的 ACK」的指令,而该五元组只能
//         从**真权威**的 auth 记录填充(读错 Redis 的副本原理上无法伪造),空/错配
//         Redis 下 Model B 心跳只回 ERR_UNAUTHORIZED 且无指令(errBattleAuthStale),
//         legacy 的 ErrDSPodNotFound→commandStop 分支在生产 Model B 门禁下不可达。
//         残余行为:DS 心跳连接若被钉死在坏副本上超过授权租约窗(~20s),按 §9.22
//         自我 fencing 停玩——这是「无法证明仍被授权」的既定 fail-closed,不是本清扫
//         引入的向量。(2026-08-03 闭环审查第三轮攻防结论,双反驳者代码证据在案。)
//   有记录 / 有心跳的 GS 与本清扫无关,由既有判弃链负责。
//
// 四重防误删(§16.1 TOCTOU / §9.22 fail-closed;④ 为对抗审查确认 P0 后补):
//   ① 证据不可得 = 不删:GS 清单或权威记录任何一步读失败,整轮放弃(宁可占位);
//     且权威记录读失败的轮次会把全部候选的观察起点**重置为当前时刻**——证据中断
//     即重新起算,保证「连续每轮都有完整证据核验」严格成立,而不是墙钟静默推进。
//   ② 候选期跨轮观察:首见只登记不删,连续超过阈值且**每轮**都无引用才进入删除;
//     期间任一轮出现引用立即出候选;
//   ③ exact 复核删除:DeleteAllocatedGameServerExact 服务端 GET 复核(UID/状态/
//     allocation-id)后携带 UID+resourceVersion 双 precondition DELETE,GET→DELETE
//     窗口内对象任何变更(含新 GSA 分配)都 409 失效,机制上无竞态窗口。
//   ④ 权威出身台账(P0「权威视图与被清扫集群零绑定」的整改):被删 GS 必须携带
//     pandora.dev/allocation-id label,且该值能在**本权威**的 allocation 台账
//     (data/allocation_ledger.go,ClaimBattle 赢家在 GSA POST 前写入)中查到。
//     一个读到空/错配 Redis 的进程(第二套部署、宿主残留、failover 到空实例)台账
//     必然为空 ⇒ 一台都删不掉;真正由本权威分配后泄漏的 GS 台账必然有记录 ⇒ 照常
//     回收。台账查无以 WARN/ERROR 暴露(它同时是「疑似权威视图分裂」的告警信号)。
//     bootstrap 限制:台账上线前已泄漏的存量 GS 与手工 GSA(无 label)不会被自动
//     回收,只告警,由人按 never-delete-allocated 纪律处置(拿不到证据=不删)。
//
// 资源占用防护(对抗审查 P2「孤儿轮饿死 §9.4 判弃链」的整改):对账轮复用判弃链的
// sweepRoundBudget 作墙钟预算,且单轮删除尝试数封顶 orphanGSMaxReclaimPerRound;
// 超限的候选留在首见表下轮继续(表跨轮持久,只推迟不丢失)。
//
// 调度:挂在既有 sweep 循环末尾(sweepOnce),复用其 leader 门与单协程并发域,
// 自带 orphanGSReconcileInterval 节流(§16.10:不新建第二套 timer 状态机)。
// firstSeen 表是进程内非权威调度提示(同 sweepDeferUntil 语义):重启即清空,
// 效果只是把回收推迟一个观察期,方向安全;每轮按当前候选集修剪,容量有界。
package biz

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/metrics"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// OrphanGameServerReconciler 是对账清扫需要的编排层能力(data.AgonesGameServerAllocator
// 实现);local / mock 分配器不实现,清扫自动禁用(它们没有 Agones Allocated 概念)。
type OrphanGameServerReconciler interface {
	ListAllocatedGameServers(ctx context.Context) ([]data.AllocatedGameServerInfo, error)
	DeleteAllocatedGameServerExact(
		ctx context.Context, name, uid, expectedAllocationID string) (bool, error)
}

// BattleAllocationLedger 是防误删④的权威台账能力(data.RedisBattleRepo 实现)。
// 未实现该接口的 repo 下清扫整体禁用——没有出身证明就没有删除权。
type BattleAllocationLedger interface {
	RecordAllocationLedger(ctx context.Context, allocationID string, atMs int64) error
	AllocationLedgerContains(ctx context.Context, allocationID string) (bool, error)
	PruneAllocationLedger(ctx context.Context, beforeMs int64) (int64, error)
}

// orphanGSReconcileInterval 是对账节流间隔:sweep 每 SweepInterval(默认 5s)tick,
// 对账每分钟至多一轮(一次 GS LIST + 全量记录读)。回收时效要求以小时计(占位泄漏),
// 1min 远够;更密只是白耗控制面。
const orphanGSReconcileInterval = time.Minute

// orphanGSMaxReclaimPerRound 单轮删除尝试上限:限制单轮外部调用成本(每次尝试
// = GET 复核 + DELETE,各 ≤ allocate_timeout),也把「万一还有未知误删路径」的
// 爆炸半径压到每分钟最多 N 台。泄漏回收时效以小时计,封顶只推迟不丢失。
const orphanGSMaxReclaimPerRound = 3

// orphanGSLedgerRetention 是 allocation 台账的保留期(§9.24 有界性):功能上只需
// ≫ 观察阈值(10min),取 7 天给「泄漏很久才被注意到」的场景留余量;超期条目每轮
// 对账顺带 ZREMRANGEBYSCORE 清除,台账容量 = 7 天内的分配次数,有界。
const orphanGSLedgerRetention = 7 * 24 * time.Hour

// orphanGSReclaimAfter 的默认值与下限。
// 推导(不是拍脑袋):候选期间每轮都重新核验"无任何权威引用",阈值只需覆盖
//   - 分配在途窗口:记录写入先于 GSA POST,GS 变 Allocated 时记录必已存在,窗口 ≈ 0;
//   - 票据可用窗口:DSTicket 硬上限 180s(pkg/auth/dsticket.go 签发/验签双向强制);
//   - warming 就绪窗口:ready_wait_timeout 生产档 120s;
//   - 时钟与控制面观察余量。
// 默认 10min ≫ 上述总和;下限 5min 防误配出危险的短阈值(配 0/负值走默认)。
// ⚠️ 该推导只覆盖文件头判定链的①「不可能再进来」;②「不可能已在内」靠心跳
// stop/自我 fencing 契约,与阈值无关——把阈值压到 300s 附近并不能靠"仍 ≫ 180s+120s"
// 论证安全,候选观察还需要跨多轮对账与控制面观察余量,这正是下限取 5min 的原因。
const (
	orphanGSReclaimAfterDefault = 10 * time.Minute
	orphanGSReclaimAfterFloor   = 5 * time.Minute
)

var orphanGSReclaimCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "pandora_ds_allocator_orphan_gameserver_reclaims_total",
	Help: "孤儿 Allocated GameServer 对账处置计数(result=reclaimed 回收成功 / skipped 复核失效 / failed 删除失败 / unprovable 台账查无保留)",
}, []string{"result"})

func init() {
	metrics.Register(orphanGSReclaimCounter)
}

// orphanGSReclaimAfter 返回生效的回收观察阈值(默认 + 下限钳制,见常量注释)。
func (u *AllocatorUsecase) orphanGSReclaimAfter() time.Duration {
	d := u.cfg.OrphanGsReclaimAfter.Std()
	if d <= 0 {
		return orphanGSReclaimAfterDefault
	}
	if d < orphanGSReclaimAfterFloor {
		return orphanGSReclaimAfterFloor
	}
	return d
}

// battleGameServerRefs 是一轮对账中权威分配记录对 GameServer 的全部引用。
type battleGameServerRefs struct {
	pods   map[string]bool // DsPodName(GameServer 名即 pod 名)
	uids   map[string]bool // GameserverUid
	allocs map[string]bool // AllocationId
}

func (r battleGameServerRefs) references(gs data.AllocatedGameServerInfo) bool {
	if gs.Name != "" && r.pods[gs.Name] {
		return true
	}
	if gs.UID != "" && r.uids[gs.UID] {
		return true
	}
	if gs.AllocationID != "" && r.allocs[gs.AllocationID] {
		return true
	}
	return false
}

// collectBattleGameServerRefs 从权威存储收集全部分配记录的 GameServer 引用。
// fail-closed:任何一条记录读失败都整轮报错——部分引用集会把仍被引用的 GS 误判为
// 孤儿,这是本清扫唯一不可接受的错误方向。Range 与 Get 之间记录被正常释放(!found)
// 不是错误:释放路径本就会回收 GS,继续观察即可。
func (u *AllocatorUsecase) collectBattleGameServerRefs(
	ctx context.Context,
) (battleGameServerRefs, error) {
	refs := battleGameServerRefs{
		pods:   make(map[string]bool),
		uids:   make(map[string]bool),
		allocs: make(map[string]bool),
	}
	matchIDs, err := u.repo.RangeActiveBattles(ctx)
	if err != nil {
		return refs, err
	}
	for _, mid := range matchIDs {
		b, found, gerr := u.repo.GetBattle(ctx, mid)
		if gerr != nil {
			return refs, gerr
		}
		if !found {
			continue
		}
		if pod := b.GetDsPodName(); pod != "" {
			refs.pods[pod] = true
		}
		if uid := b.GetGameserverUid(); uid != "" {
			refs.uids[uid] = true
		}
		if alloc := b.GetAllocationId(); alloc != "" {
			refs.allocs[alloc] = true
		}
	}
	return refs, nil
}

// reconcileOrphanGameServersIfDue 在 sweepOnce 末尾被调用(leader 门与单协程并发域
// 由 sweep 继承),按 orphanGSReconcileInterval 节流。
func (u *AllocatorUsecase) reconcileOrphanGameServersIfDue(ctx context.Context, now time.Time) {
	if u.orphanGSReconciler == nil || u.allocationLedger == nil {
		return
	}
	if !u.lastOrphanGSReconcile.IsZero() &&
		now.Sub(u.lastOrphanGSReconcile) < orphanGSReconcileInterval {
		return
	}
	u.lastOrphanGSReconcile = now
	u.reconcileOrphanGameServers(ctx, now)
}

// reconcileOrphanGameServers 执行一轮对账(见文件头注释的四重防误删)。
func (u *AllocatorUsecase) reconcileOrphanGameServers(ctx context.Context, now time.Time) {
	gsList, err := u.orphanGSReconciler.ListAllocatedGameServers(ctx)
	if err != nil {
		plog.With(ctx).Warnw("msg", "orphan_gs_list_failed", "err", err)
		return
	}
	if u.orphanGSFirstSeen == nil {
		u.orphanGSFirstSeen = make(map[string]time.Time)
	}
	if len(gsList) == 0 {
		clear(u.orphanGSFirstSeen)
		return
	}
	refs, err := u.collectBattleGameServerRefs(ctx)
	if err != nil {
		// 防误删①:权威引用不可得 = 本轮不删,且把全部已有候选的观察起点重置为
		// 当前时刻——「连续每轮都有证据核验」是删除的前提,证据中断就重新起算。
		// 方向安全:权威抖动只会推迟回收,绝不会让未经核验的墙钟时间计入阈值。
		for key := range u.orphanGSFirstSeen {
			u.orphanGSFirstSeen[key] = now
		}
		plog.With(ctx).Warnw("msg", "orphan_gs_authority_refs_unavailable",
			"candidates_reset", len(u.orphanGSFirstSeen), "err", err)
		return
	}
	// 台账保留期修剪(§9.24 有界性;放在成功拿到权威证据的轮次里顺带做)。
	if _, perr := u.allocationLedger.PruneAllocationLedger(
		ctx, now.Add(-orphanGSLedgerRetention).UnixMilli()); perr != nil {
		plog.With(ctx).Warnw("msg", "orphan_gs_ledger_prune_failed", "err", perr)
	}
	threshold := u.orphanGSReclaimAfter()
	// 墙钟预算与单轮删除封顶(对抗审查 P2:不得饿死同协程的 §9.4 判弃链)。
	// 预算口径与判弃链一致(sweepRoundBudget);预算只在开始下一次删除尝试前检查,
	// 单轮实际上界 = 预算 + 单次尝试最坏耗时(GET+DELETE,各 ≤ allocate_timeout)。
	roundStart := time.Now()
	budget := u.sweepRoundBudget()
	reclaimAttempts := 0
	unprovable := 0
	candidates := make(map[string]bool, len(gsList))
	for _, gs := range gsList {
		if gs.Name == "" || gs.UID == "" || gs.Deleting {
			continue
		}
		key := gs.Name + "/" + gs.UID
		if refs.references(gs) {
			// 有权威引用:健康 Allocated,出候选(若曾入选)。
			delete(u.orphanGSFirstSeen, key)
			continue
		}
		candidates[key] = true
		first, seen := u.orphanGSFirstSeen[key]
		if !seen {
			u.orphanGSFirstSeen[key] = now
			plog.With(ctx).Warnw("msg", "orphan_allocated_gs_candidate",
				"gameserver", gs.Name, "uid", gs.UID, "fleet", gs.Fleet,
				"allocation_id", gs.AllocationID, "reclaim_after", threshold.String())
			continue
		}
		if now.Sub(first) < threshold {
			continue
		}
		// 防误删④:出身证明。无 allocation-id label(手工 GSA / 非本系统分配)或
		// 台账查无(空/错配权威、台账上线前的存量泄漏)一律不删,只告警保留。
		if gs.AllocationID == "" {
			unprovable++
			orphanGSReclaimCounter.WithLabelValues("unprovable").Inc()
			plog.With(ctx).Warnw("msg", "orphan_allocated_gs_unprovable_no_label",
				"gameserver", gs.Name, "uid", gs.UID, "fleet", gs.Fleet)
			continue
		}
		known, kerr := u.allocationLedger.AllocationLedgerContains(ctx, gs.AllocationID)
		if kerr != nil {
			plog.With(ctx).Warnw("msg", "orphan_allocated_gs_ledger_check_failed",
				"gameserver", gs.Name, "uid", gs.UID, "err", kerr)
			continue // fail-closed:证据不可得不删,候选保留
		}
		if !known {
			unprovable++
			orphanGSReclaimCounter.WithLabelValues("unprovable").Inc()
			plog.With(ctx).Warnw("msg", "orphan_allocated_gs_unprovable_ledger_miss",
				"gameserver", gs.Name, "uid", gs.UID, "fleet", gs.Fleet,
				"allocation_id", gs.AllocationID)
			continue
		}
		if reclaimAttempts >= orphanGSMaxReclaimPerRound ||
			(reclaimAttempts > 0 && time.Since(roundStart) >= budget) {
			plog.With(ctx).Infow("msg", "orphan_gs_round_budget_exhausted",
				"attempts", reclaimAttempts, "budget", budget.String())
			continue // 剩余候选保留首见时间,下轮继续;只统计不再发外部调用
		}
		reclaimAttempts++
		deleted, derr := u.orphanGSReconciler.DeleteAllocatedGameServerExact(
			ctx, gs.Name, gs.UID, gs.AllocationID)
		switch {
		case derr != nil:
			// 保留候选(首见时间不变),下一轮重试;删除幂等。
			orphanGSReclaimCounter.WithLabelValues("failed").Inc()
			plog.With(ctx).Warnw("msg", "orphan_allocated_gs_reclaim_failed",
				"gameserver", gs.Name, "uid", gs.UID, "fleet", gs.Fleet, "err", derr)
		case !deleted:
			// 复核失效(对象变过/已消失/离开 Allocated):候选作废,重新观察。
			delete(u.orphanGSFirstSeen, key)
			orphanGSReclaimCounter.WithLabelValues("skipped").Inc()
			plog.With(ctx).Infow("msg", "orphan_allocated_gs_reclaim_skipped_recheck",
				"gameserver", gs.Name, "uid", gs.UID, "fleet", gs.Fleet)
		default:
			delete(u.orphanGSFirstSeen, key)
			orphanGSReclaimCounter.WithLabelValues("reclaimed").Inc()
			// ERROR 级(运维语义的破坏性动作必须显眼):正常运行不该出现孤儿,
			// 出现即说明某次外部释放没有闭环,值得追查来源。
			plog.With(ctx).Errorw("msg", "orphan_allocated_gs_reclaimed",
				"gameserver", gs.Name, "uid", gs.UID, "fleet", gs.Fleet,
				"allocation_id", gs.AllocationID,
				"candidate_since", first.Format(time.RFC3339),
				"observed", now.Sub(first).String(),
				"hint", "Allocated 且连续无任何权威分配记录引用超过阈值,台账确认出身本权威,已按 UID+resourceVersion 精确回收;请追查该 GS 当初为何未被正常释放")
		}
	}
	if unprovable > 0 {
		// 台账查无的候选 ≥1 即打 ERROR:它既可能是台账上线前的存量泄漏/手工 GSA,
		// 也可能是「本进程读错了权威」(防误删④正在拦下一次全量误删)——两者都必须有人看。
		plog.With(ctx).Errorw("msg", "orphan_allocated_gs_unprovable_present",
			"count", unprovable,
			"hint", "存在无法证明出身本权威的孤儿候选,已全部保留不删;若数量≈全部 Allocated,优先排查本进程 Redis 配置是否指向了错误/空的权威(权威视图分裂)")
	}
	// 修剪:不再是候选(GS 消失/转态/获得引用/进入删除宽限)的项移出首见表,
	// 表容量上界 = 当前候选数(§9.18 进程内容器有界纪律)。
	for key := range u.orphanGSFirstSeen {
		if !candidates[key] {
			delete(u.orphanGSFirstSeen, key)
		}
	}
}
