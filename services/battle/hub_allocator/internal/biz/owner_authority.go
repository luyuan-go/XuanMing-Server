// owner_authority.go — owner 迁移弱依赖接线(owner-authority.md migrate ①/③④,2026-07-22)。
//
// migrate 语义(全部弱依赖,路由决策不变,§9.23 行为切换属 contract 阶段):
//   - ① hub 归属定案统一出口(签票点)弱 Begin(HUB):分配/恢复/转移/Battle 回流全路径
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

	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/data"
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
// 最大保鲜期。活实例每次心跳 census 对在场玩家续期 last-touch;超过本值未续期 = 其所属实例
// 已销毁(UID 不再心跳),项由 sweepStaleOwnerAdmitted 清除,防缓存随历史实例 UID 无界增长
// (复审 P1-5;§9.18 客户端触发型内存容器有界)。取值远大于心跳/census 周期,活实例项绝不误清。
const ownerAdmittedStaleTTL = 5 * time.Minute

// sweepStaleOwnerAdmitted 删除 last-touch 早于 cutoff 的 census 准入缓存项(复审 P1-5)。
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

// OwnerAuthority 是 owner 权威的 migrate 调用面(Query/Begin/Admit;弱依赖)。
// 由 data.GrpcOwnerLeaseRenewer 实现(与租约续写共用连接);可为 nil(未启用)。
type OwnerAuthority interface {
	QueryOwner(ctx context.Context, playerID uint64) (data.OwnerRecordView, error)
	BeginTransition(ctx context.Context, playerID, expectEpoch uint64, operationID string, ownerType int8, target data.OwnerTargetView) error
	Admit(ctx context.Context, playerID, ownerEpoch uint64, operationID string, target data.OwnerTargetView) (int64, error)
}

// SetOwnerAuthority 注入 owner 权威调用面(nil-safe)。
func (u *HubUsecase) SetOwnerAuthority(a OwnerAuthority) {
	u.ownerAuth = a
}

// decideOwnerBegin 判定是否需要发起迁移(§9.23 幂等 no-op 规则):
// 记录已指向**同一 exact owner identity**且处于 PENDING/ADMITTED → 跳过(同目标重连/
// 重复交付不再推进 epoch);否则以当前 epoch 为 CAS 期望发起。
//
// 复审 P1-3:同目标判定必须包含 InstanceEpoch。§9.22 明确"Pod UID / instance epoch 变化
// 或灾备接管都必须递增 owner_epoch"——只比 (type,pod,uid) 会把"同 pod+uid 但 instance
// epoch 已推进(灾备接管 / 实例代次翻转)"误判为同目标而跳过,漏掉本应发生的 owner 迁移。
// 故加入 rec.InstanceEpoch == target.InstanceEpoch。
// 有意不纳入 AssignmentOrAllocationID / ReleaseTrack:同一 exact 实例上的 assignment 刷新
// (seat 续租)是同一 owner 的重复交付,纳入会造成 epoch 无谓翻动(churn);release track
// (stable/canary)是独立 Fleet,track 变化必伴随 pod/uid 变化,已被上面捕获。
func decideOwnerBegin(rec data.OwnerRecordView, ownerType int8, target data.OwnerTargetView) (skip bool, expectEpoch uint64) {
	if rec.OwnerType == ownerType && rec.PodName == target.PodName && rec.InstanceUID == target.InstanceUID &&
		rec.InstanceEpoch == target.InstanceEpoch &&
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
	// 模式 C:owner 是显式弱依赖(失败时旧路由门照跑),但本循环按玩家扇出,
	// owner 抖动时逐玩家打 Warn 会按批人数刷屏。改为批内累加、批末汇总一条。
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

// ownerAdmitCensusWeak census 首见玩家代提交 Admit(migrate 近似;弱依赖)。
//
// admitted 缓存 key = instanceUID|playerID(进程内 best-effort:重启后重查一轮即收敛);
// 仅当记录确实指向本实例(pod+uid 同 && 类型同 && PENDING)才 Admit,目标取记录自身字段
// (Admit 的 exact 全等校验由 owner 侧执行;pod/uid 是本调用方独立断言的部分)。
// 屏障未开(retryAfter>0)→ 本轮跳过,下次心跳重试;其余失败告警跳过。
//
// 复审 P1-2:缓存按 census 轮剪枝——玩家离开本实例(不再出现在 census)即删除其
// 缓存项;其回流本实例时(owner epoch 已推进、新 PENDING)缓存必然 miss,重新
// Query→Admit,不会被上一纪元的 admitted 缓存误吞。持续在场的玩家缓存命中跳过
// Query(原优化保留;在场期间同实例重连是 decideOwnerBegin 幂等跳过,epoch 不推进)。
//
// 复审 P1-3:resolveTarget 非 nil 时,对「记录不指向本实例但玩家确在本实例 census」
// 的玩家做自愈弱 Begin——签票点弱 Begin 失败后无人重试,owner 记录会长期漂移;
// census 是周期性重试点(归属镜像同样指向本实例才补 Begin,不与真实迁移打架)。
func ownerAdmitCensusWeak(ctx context.Context, auth OwnerAuthority, admitted *sync.Map,
	players []uint64, ownerType int8, selfPod, selfUID string, budget time.Duration,
	resolveTarget func(context.Context, uint64) (data.OwnerTargetView, bool)) {
	if auth == nil {
		return
	}
	// 复审 P1-4:先按本实例 census 剪枝——即使本轮 census 为空(最后一名玩家离场)也必须执行,
	// 否则该玩家的 admitted 项残留;其回流同实例(owner epoch 已推进、新 PENDING)时会被下方
	// 缓存命中(admitted.Load)误吞、跳过 Query→Admit。故剪枝前置到「无玩家早退」之前。
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
	// 模式 C:本函数由**每次心跳**(每 ~5s/pod)对 pod 上全部在场玩家(Hub 可达 ~500 人)
	// 调用,owner 抖动时逐玩家打 Warn = pod 数 × 500 / 5s 条刷屏。批末汇总一条。
	var queryFailed, admitFailed, healFailed int
	var firstErr error
	var samplePlayer uint64
	noteFail := func(playerID uint64, err error) {
		if firstErr == nil {
			firstErr, samplePlayer = err, playerID
		}
	}
	defer func() {
		if queryFailed+admitFailed+healFailed == 0 {
			return
		}
		plog.With(ctx).Warnw("msg", "owner_admit_census_weak_failed",
			"players", len(players), "query_failed", queryFailed, "admit_failed", admitFailed,
			"heal_begin_failed", healFailed,
			"sample_player_id", samplePlayer, "first_err", firstErr,
			"hint", "migrate 弱依赖,再入屏障双门兜底")
	}()
	now := time.Now()
	for _, playerID := range players {
		key := selfUID + "|" + fmt.Sprintf("%d", playerID)
		if v, ok := admitted.Load(key); ok {
			// 复审 P1-5:命中即续期(接近过期才写,降 sync.Map 写争用);活实例项恒新鲜,
			// 仅已销毁实例(UID 不再心跳续期)的项会老化超 TTL 被 sweepStaleOwnerAdmitted 清除。
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
			// 记录不指向本实例(迁移中/漂移)。复审 P1-3:若归属镜像仍指向本实例
			// (resolveTarget 确认),说明是签票点弱 Begin 失败留下的漂移,补一次弱
			// Begin 自愈;否则是真实迁移,不干预。
			if resolveTarget != nil {
				if tgt, ok := resolveTarget(budgetCtx, playerID); ok && tgt.PodName == selfPod && tgt.InstanceUID == selfUID {
					if skip, expectEpoch := decideOwnerBegin(rec, ownerType, tgt); !skip {
						if berr := auth.BeginTransition(budgetCtx, playerID, expectEpoch, uuid.NewString(), ownerType, tgt); berr != nil {
							healFailed++
							noteFail(playerID, berr)
						}
					}
				}
			}
			continue
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
