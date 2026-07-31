// owner_authority.go — owner 归属接线(owner-authority.md;contract 阶段,2026-07-29)。
//
//   - ① hub 归属定案统一出口(签票点)**强** Begin(HUB):分配/恢复/转移/Battle 回流全路径
//     写进 owner 权威(E+1/PENDING/屏障);**写不进即拒绝本次签票**(§9.22 fail-closed),
//     调用方上抛,客户端按 §9.23 退避重查;
//   - ③ 授权心跳 census 首见玩家代提交 Admit(仍是近似:census 来自绑定 exact 实例身份的
//     授权心跳,是"该实例正在服务该玩家"的证据;DS Admission 链原生提交后本近似退役)。
//     census 侧刻意保持弱:它是周期性重试点,不该让一个玩家的自愈失败打挂整台 DS 的心跳。
//
// 同实例收敛与 operation_id 铸造已下沉到 owner 权威的行锁事务(见 owner 服务
// BeginTransition),本文件不再做本地 Query→比对→Begin 的先查再存(§9.22 禁止)。
package biz

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
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

// beginOnePlayer 为单个玩家把 owner 权威推进到 target(contract 阶段:强依赖)。
//
// 与 migrate 阶段的差别有两处,都不在本函数里"多做",而是"少做":
//   - **不再本地判定同目标跳过**。那份 Query → 比对 → Begin 是先查再存(§9.22 明令禁止),
//     判定与写入不在同一线性化点;现在同 exact 实例的收敛由 owner 在行锁事务内做,
//     调用方直接发起即可,权威会原样返回既有记录(不推进 epoch、不覆盖 operation_id)。
//   - **不再自铸 operation_id**。传空 = 由权威铸造,真实迁移才产生新值,同目标重复投递
//     沿用既有 operation——这正是 §9.23「一次真实进场用一个稳定 operation_id」的落点。
//     调用方现铸 UUID 恰恰破坏它(每次投递一个新 operation,幂等键形同虚设)。
//
// Query 仍然要发:它取的是 CAS 期望值 expect_epoch。Query 与 Begin 之间记录可能已被
// 另一个写者推进,那是 CAS 的设计内竞争,权威以 EPOCH_CONFLICT 告知——重查一次再试;
// 仍冲突说明有写者在持续推进,fail-closed 交由调用方整体重试,不盲目循环抢。
func beginOnePlayer(ctx context.Context, auth OwnerAuthority, playerID uint64,
	ownerType int8, target data.OwnerTargetView) error {
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		rec, qerr := auth.QueryOwner(ctx, playerID)
		if qerr != nil {
			// 查询不可判定 → UNKNOWN,绝不当作"无归属"继续(§9.22 禁冒充 OFFLINE/空闲)。
			return qerr
		}
		berr := auth.BeginTransition(ctx, playerID, rec.OwnerEpoch, "", ownerType, target)
		if berr == nil {
			return nil
		}
		if errcode.As(berr) != errcode.ErrOwnerEpochConflict {
			return berr
		}
		lastErr = berr
	}
	return lastErr
}

// ownerBeginPlayers 批量强 Begin(contract 阶段):**任一玩家写不进 owner 权威即整体失败**。
//
// 为什么从"告警放行"改成 fail-closed:owner 是归属的唯一权威(§9.22),写不进去就无法证明
// "这台 DS 有权控制该玩家"。此时放行 = 玩家可能同时被两台 DS 认领,直接踩验收底线第 3 条
// (宁可 fail-closed 拒绝一次操作,也不要写出一份不自洽的数据)。
//
// 拒绝不会把玩家卡死(底线第 1 条):调用方把错误上抛后,客户端按 §9.23 退避重查,
// owner 恢复即自动继续——属于底线明确允许的"短暂不可用后自动恢复"。
//
// auth == nil = owner_addr 未配置(owner 服务未部署),属部署形态问题,不在本函数收敛。
//
// 超预算即失败,而不是 migrate 阶段的"跳过剩余玩家":部分写入 + 部分跳过会留下一批
// 无归属记录的在场玩家,比整体失败重试更难收敛。
func ownerBeginPlayers(ctx context.Context, auth OwnerAuthority, players []uint64,
	ownerType int8, target data.OwnerTargetView, budget time.Duration) error {
	if auth == nil || len(players) == 0 {
		return nil
	}
	budgetCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	for i, playerID := range players {
		if err := beginOnePlayer(budgetCtx, auth, playerID, ownerType, target); err != nil {
			plog.With(ctx).Warnw("msg", "owner_begin_failed",
				"players", len(players), "failed_at", i, "player_id", playerID, "err", err,
				"pod", target.PodName, "instance_uid", target.InstanceUID,
				"hint", "contract 强依赖:归属写不进权威即拒绝本次交付,调用方重试")
			return err
		}
	}
	return nil
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
			// (resolveTarget 确认),说明是签票点 Begin 失败留下的漂移,补一次自愈;
			// 否则是真实迁移,不干预。
			//
			// 自愈仍是**弱**的:census 是周期性重试点,补不上下一轮再补,不该让一个
			// 漂移玩家的自愈失败把整台 DS 的心跳打挂。真正的强门在签票/交付点。
			// 同实例判定与 operation 铸造都已下沉到权威,这里直接发起即可。
			if resolveTarget != nil {
				if tgt, ok := resolveTarget(budgetCtx, playerID); ok && tgt.PodName == selfPod && tgt.InstanceUID == selfUID {
					if berr := beginOnePlayer(budgetCtx, auth, playerID, ownerType, tgt); berr != nil {
						healFailed++
						noteFail(playerID, berr)
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
