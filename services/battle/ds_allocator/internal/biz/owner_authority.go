// owner_authority.go — owner 迁移弱依赖接线(owner-authority.md migrate ②/③,2026-07-22)。
//
// migrate 语义(全部弱依赖,路由决策不变,§9.23 行为切换属 contract 阶段):
//   - ② READY 交付前逐玩家 BeginTransition(BATTLE):把"这批玩家将由该 Battle 实例 own"
//     写进 owner 权威(E+1/PENDING/屏障);失败仅告警,分配照常;
//   - ③ 授权心跳 census 首见玩家代提交 Admit(migrate 近似:census 来自绑定 exact 实例
//     身份的授权心跳,是"该实例正在服务该玩家"的证据;contract 阶段 Admit 移交 DS
//     Admission 链原生提交,本近似退役)。
package biz

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	plog "github.com/luyuancpp/pandora/pkg/log"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// owner 类型常量(对齐 owner.proto OwnerType;biz 不依赖生成代码)。
const (
	ownerTypeHub    int8 = 1
	ownerTypeBattle int8 = 2
)

// owner 阶段常量(对齐 owner.proto OwnerPhase)。
const (
	ownerPhasePending  int8 = 1
	ownerPhaseAdmitted int8 = 2
)

// ownerAdmittedStaleTTL 是 census 已准入缓存项(ownerAdmitted,key=instanceUID|playerID)的
// 最大保鲜期。活实例每次心跳 census 对在场玩家续期 last-touch;超过本值未续期 = 其所属 Battle
// 实例已销毁(UID 不再心跳),项由 sweepStaleOwnerAdmitted 清除,防缓存随历史实例 UID 无界增长
// (压测前审核 P1;§9.18 客户端触发型内存容器有界)。取值远大于心跳/census 周期,活实例项绝不误清。
//
// 与 hub_allocator 同名机制一致(hub 复审 P1-5):Battle DS 打完即销毁、InstanceUID 永不复用,
// admitted 项若不老化回收会随累计对局数单调增长,长压测下 OOM。
const ownerAdmittedStaleTTL = 5 * time.Minute

// sweepStaleOwnerAdmitted 删除 last-touch 早于 cutoff 的 census 准入缓存项。
// 值意外非 time.Time(理论不达,所有写入点均写 time.Time)也删除,保证 fail-safe 有界。
// best-effort:被清项若玩家仍在场,下一轮 census 会重新 Query→Admit 补回(至多一次多余往返)。
func sweepStaleOwnerAdmitted(admitted *sync.Map, cutoff time.Time) {
	admitted.Range(func(k, v any) bool {
		if t, ok := v.(time.Time); !ok || t.Before(cutoff) {
			admitted.Delete(k)
		}
		return true
	})
}

// OwnerAuthority 是 owner 权威的 migrate 调用面(Query/Begin/Admit/Release;弱依赖)。
// 由 data.GrpcOwnerLeaseRenewer 实现(与租约续写共用连接);可为 nil(未启用)。
type OwnerAuthority interface {
	QueryOwner(ctx context.Context, playerID uint64) (data.OwnerRecordView, error)
	BeginTransition(ctx context.Context, playerID, expectEpoch uint64, operationID string, ownerType int8, target data.OwnerTargetView) error
	Admit(ctx context.Context, playerID, ownerEpoch uint64, operationID string, target data.OwnerTargetView) (int64, error)
	ReleaseOwner(ctx context.Context, playerID, ownerEpoch uint64, operationID string) error
}

// SetOwnerAuthority 注入 owner 权威调用面(nil-safe)。
func (u *AllocatorUsecase) SetOwnerAuthority(a OwnerAuthority) {
	u.ownerAuth = a
}

// decideOwnerBegin 判定是否需要发起迁移(§9.23 幂等 no-op 规则):
// 记录已指向同一实例(类型同 && pod+uid 同)且处于 PENDING/ADMITTED → 跳过
// (同目标重连/重复交付不再推进 epoch);否则以当前 epoch 为 CAS 期望发起。
func decideOwnerBegin(rec data.OwnerRecordView, ownerType int8, target data.OwnerTargetView) (skip bool, expectEpoch uint64) {
	if rec.OwnerType == ownerType && rec.PodName == target.PodName && rec.InstanceUID == target.InstanceUID &&
		(rec.Phase == ownerPhasePending || rec.Phase == ownerPhaseAdmitted) {
		return true, 0
	}
	return false, rec.OwnerEpoch
}

// ownerBeginPlayersWeak 批量弱 Begin:整批共享预算 ctx(防 owner 卡顿拖慢调用链),
// 每玩家 Query→decide→Begin;任何失败仅告警(migrate 弱依赖,旧路由门照跑)。
func ownerBeginPlayersWeak(ctx context.Context, auth OwnerAuthority, players []uint64,
	ownerType int8, target data.OwnerTargetView, budget time.Duration) {
	if auth == nil || len(players) == 0 {
		return
	}
	budgetCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	// 模式 C:owner 是显式弱依赖(失败时旧路由门照跑,不影响正确性),但本循环按玩家扇出
	// 且由 AllocateBattle / 心跳驱动——owner 抖动时逐玩家打 Warn 会按「批人数 × 调用频率」
	// 刷屏。改为批内累加、批末汇总一条:既保留降级信号,又能直接回答「这批 N 个里坏了几个」。
	var queryFailed, beginFailed int
	var firstErr error
	var samplePlayer uint64
	noteFail := func(playerID uint64, err error) {
		if firstErr == nil {
			firstErr, samplePlayer = err, playerID
		}
	}
	defer func() {
		if queryFailed+beginFailed == 0 {
			return
		}
		plog.With(ctx).Warnw("msg", "owner_begin_weak_failed",
			"players", len(players), "query_failed", queryFailed, "begin_failed", beginFailed,
			"sample_player_id", samplePlayer, "first_err", firstErr,
			"hint", "migrate 弱依赖,旧路由门照跑")
	}()
	for i, playerID := range players {
		if budgetCtx.Err() != nil {
			plog.With(ctx).Warnw("msg", "owner_begin_budget_exhausted",
				"players", len(players), "done", i, "remaining_players", len(players)-i,
				"hint", "migrate 弱依赖,跳过剩余玩家")
			return
		}
		rec, err := auth.QueryOwner(budgetCtx, playerID)
		if err != nil {
			queryFailed++
			noteFail(playerID, err)
			continue
		}
		skip, expectEpoch := decideOwnerBegin(rec, ownerType, target)
		if skip {
			continue
		}
		if err := auth.BeginTransition(budgetCtx, playerID, expectEpoch, uuid.NewString(), ownerType, target); err != nil {
			beginFailed++
			noteFail(playerID, err)
		}
	}
}

// ownerReleaseAbandonedPlayersWeak 判弃对局后释放仍指向该实例的 owner 记录
// (INC-20260729-002 P0-B1;弱依赖,同 Begin/Admit)。
//
// 为什么必须有:§9.4 的 abandoned 补偿链原本被当作「玩家解放出口」(UE 侧
// PandoraAgonesHealthPinger.h 的设计注释明写这一点),但它只做了 lifecycle 事件 +
// battle_result 记账 + match 释放,**从没动过 owner 权威**。全仓 ReleaseOwner 的唯一
// 调用点是 login 登出。后果:一旦 login 的 owner_query_first 打开,判弃后的恢复查询会
// 一直返回 TARGET(已删除的 battle Pod),客户端按 §9.23 反复 Travel 到不存在的实例,
// 比不接 owner 更糟。释放后恢复查询才会落到「无归属 → 首次进场链 → Hub」。
//
// 安全边界(三条,缺一不可):
//  ① **时序**:只能在被判弃实例的 GameServer 回收已确认之后调用(与 deliverAbandoned
//     同一门控)。提前释放会在旧 DS 可能仍在跑时放行新归属 = 双 DS(§9.22)。
//  ② **exact 身份**:只释放「记录仍指向本次被判弃的 pod+uid 且类型为 BATTLE」的玩家。
//     玩家已被迁到新 DS(epoch 已推进、pod/uid 已变)时必须跳过,否则误删活归属。
//  ③ **compare-delete**:带 Query 读到的 owner_epoch + operation_id 调用,owner 侧按
//     epoch 比对拒绝陈旧释放(同 §9.23「迟到 Logout 只能删自己」)。
//
// 失败只告警:owner 未启用 / 抖动时,login 侧 InspectBattleRoute 的
// abandoned→(过再入屏障)→Terminal→Hub 旧门仍能让玩家收敛,不影响正确性。
func ownerReleaseAbandonedPlayersWeak(ctx context.Context, auth OwnerAuthority, players []uint64,
	selfPod, selfUID string, budget time.Duration) {
	if auth == nil || len(players) == 0 || selfPod == "" || selfUID == "" {
		return
	}
	budgetCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	var queryFailed, releaseFailed, released, skipped int
	var firstErr error
	var samplePlayer uint64
	noteFail := func(playerID uint64, err error) {
		if firstErr == nil {
			firstErr, samplePlayer = err, playerID
		}
	}
	defer func() {
		// 成功条数也要可观测:判弃是低频事件,一条汇总不会刷屏,而「释放了几个/跳过几个」
		// 正是排查「玩家为什么还回不去 Hub」时第一眼要看的数。
		plog.With(ctx).Infow("msg", "owner_release_abandoned_weak",
			"players", len(players), "released", released, "skipped_not_self", skipped,
			"query_failed", queryFailed, "release_failed", releaseFailed,
			"pod", selfPod, "sample_player_id", samplePlayer, "first_err", firstErr)
	}()
	for i, playerID := range players {
		if budgetCtx.Err() != nil {
			plog.With(ctx).Warnw("msg", "owner_release_abandoned_budget_exhausted",
				"players", len(players), "done", i, "remaining_players", len(players)-i,
				"hint", "migrate 弱依赖;login InspectBattleRoute 旧门仍能收敛")
			return
		}
		rec, err := auth.QueryOwner(budgetCtx, playerID)
		if err != nil {
			queryFailed++
			noteFail(playerID, err)
			continue
		}
		// ② exact 身份门:只有记录仍指向本次被判弃的实例才允许释放。
		if rec.OwnerType != ownerTypeBattle || rec.PodName != selfPod || rec.InstanceUID != selfUID {
			skipped++
			continue
		}
		// ③ compare-delete:epoch/operation 取自刚读到的记录,owner 侧再校验一次。
		if rerr := auth.ReleaseOwner(budgetCtx, playerID, rec.OwnerEpoch, rec.OperationID); rerr != nil {
			releaseFailed++
			noteFail(playerID, rerr)
			continue
		}
		released++
	}
}

// ownerAdmitCensusWeak census 首见玩家代提交 Admit(migrate 近似;弱依赖)。
//
// admitted 缓存 key = instanceUID|playerID(进程内 best-effort:重启后重查一轮即收敛);
// 仅当记录确实指向本实例(pod+uid 同 && 类型同 && PENDING)才 Admit,目标取记录自身字段
// (Admit 的 exact 全等校验由 owner 侧执行;pod/uid 是本调用方独立断言的部分)。
// 屏障未开(retryAfter>0)→ 本轮跳过,下次心跳重试;其余失败告警跳过。
//
// 缓存有界(压测前审核 P1,对齐 hub_allocator):
//   - 值存 time.Time(last-touch):命中即续期(接近过期才写,降 sync.Map 写争用),活实例
//     项恒新鲜;仅已销毁 Battle 实例(UID 不再心跳续期)的项会老化超 TTL,由后台
//     sweepStaleOwnerAdmitted 清除,防缓存随累计对局的历史 InstanceUID 无界增长导致 OOM。
//   - 按本实例 census 剪枝:玩家离开本实例(不再出现在 census)即删除其缓存项,与 TTL 兜底
//     互补(TTL 清死实例项,剪枝清活实例上已离场玩家项)。
func ownerAdmitCensusWeak(ctx context.Context, auth OwnerAuthority, admitted *sync.Map,
	players []uint64, ownerType int8, selfPod, selfUID string, budget time.Duration) {
	if auth == nil {
		return
	}
	// 先按本实例 census 剪枝:present 为本轮 census 的本实例 key 集合;删除带 selfUID| 前缀
	// 但已不在 present 的项(该玩家已离开本实例)。只触本实例前缀,死实例项交给 TTL sweep。
	present := make(map[string]struct{}, len(players))
	for _, playerID := range players {
		present[selfUID+"|"+fmt.Sprintf("%d", playerID)] = struct{}{}
	}
	admitted.Range(func(k, _ any) bool {
		key, ok := k.(string)
		if ok && strings.HasPrefix(key, selfUID+"|") {
			if _, in := present[key]; !in {
				admitted.Delete(key)
			}
		}
		return true
	})
	if len(players) == 0 {
		return // 剪枝已完成;本轮无玩家可代提交 Admit。
	}
	budgetCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	// 模式 C:本函数由**每次心跳**(每 ~5s/对局)对全部在场玩家调用,owner 抖动时逐玩家
	// 打 Warn = 并发对局数 × 玩家数 / 5s 条刷屏。批末汇总一条(弱依赖,双门兜底)。
	var queryFailed, admitFailed int
	var firstErr error
	var samplePlayer uint64
	noteFail := func(playerID uint64, err error) {
		if firstErr == nil {
			firstErr, samplePlayer = err, playerID
		}
	}
	defer func() {
		if queryFailed+admitFailed == 0 {
			return
		}
		plog.With(ctx).Warnw("msg", "owner_admit_census_weak_failed",
			"players", len(players), "query_failed", queryFailed, "admit_failed", admitFailed,
			"sample_player_id", samplePlayer, "first_err", firstErr,
			"hint", "migrate 弱依赖,再入屏障双门兜底")
	}()
	now := time.Now()
	for _, playerID := range players {
		key := selfUID + "|" + fmt.Sprintf("%d", playerID)
		if v, ok := admitted.Load(key); ok {
			// 命中即续期(接近过期才写):活实例项恒新鲜,仅死实例项会老化被 sweep 清。
			if t, isTime := v.(time.Time); !isTime || now.Sub(t) > ownerAdmittedStaleTTL/2 {
				admitted.Store(key, now)
			}
			continue
		}
		if budgetCtx.Err() != nil {
			return // 预算耗尽:剩余玩家下次心跳继续(census 每 ~5s 一轮,自然收敛)。
		}
		rec, err := auth.QueryOwner(budgetCtx, playerID)
		if err != nil {
			queryFailed++
			noteFail(playerID, err)
			continue
		}
		if rec.OwnerType != ownerType || rec.PodName != selfPod || rec.InstanceUID != selfUID {
			continue // 记录不指向本实例(迁移中/漂移),不是本实例可断言的准入。
		}
		if rec.Phase == ownerPhaseAdmitted {
			admitted.Store(key, now)
			continue
		}
		if rec.Phase != ownerPhasePending {
			continue
		}
		target := data.OwnerTargetView{
			PodName: rec.PodName, InstanceUID: rec.InstanceUID, InstanceEpoch: rec.InstanceEpoch,
			AssignmentOrAllocationID: rec.AssignmentOrAllocationID, ReleaseTrack: rec.ReleaseTrack,
		}
		retryAfter, aerr := auth.Admit(budgetCtx, playerID, rec.OwnerEpoch, rec.OperationID, target)
		switch {
		case aerr == nil:
			admitted.Store(key, now)
		case retryAfter > 0:
			// 屏障未开:预期中的 WAIT,下次心跳重试,不告警刷屏。
		default:
			admitFailed++
			noteFail(playerID, aerr)
		}
	}
}
