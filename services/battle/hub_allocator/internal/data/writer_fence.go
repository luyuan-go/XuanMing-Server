// writer_fence.go — hub_allocator 写者继任 fencing(INC-20260722-004 R9 P0-7 收口;
// docs/design/session-generation-rollout.md §5)。
//
// 语义:每届 hub_allocator 写者持有一个严格单调递增的 fencing token(etcd 继任租约,
// pkg/dsauthfence/writerlease)。所有 per-{pod} 授权/容量账本事务在同一 WATCH/MULTI/EXEC
// 内比较并推进与业务键同 slot 的 fence key:
//
//	pandora:hub:wfence:{<pod>} → 已见最大写者 token(十进制字符串,持久键)
//
//	cur > mine → 拒绝(ErrWriterSuperseded,零写入):继任者已触达此 slot,前任的迟到
//	             写永久出局(即使前任进程尚未察觉失主);
//	cur < mine → 本事务顺带把 fence 推进到 mine(SET 进同一 EXEC);
//	cur == mine → 直接放行。
//
// 逐 slot 懒推进的正确性:继任者第一次写某 {pod} slot 起,前任在该 slot 永久被拒;
// 继任者尚未触达的 slot 上,前任写在语义上线性化于交接之前(继任者随后读到并接续),
// 不构成账本冲突。fence key 故意**不设 TTL、RemoveShard 也不删**:fencing 水位必须
// 比业务记录长寿,否则删除即复位,迟到旧写者可借尸还魂。
//
// per-player assignment key(pandora:hub:player:<id>)无 hashtag、与任何 {pod} slot 不可
// 同事务,用不了上面的 {pod} 水位键。该路径由五层组合收口(R10 复审 P0-4 补 ③ 的硬门
// 语义与 ⑤ 的持久水位;此前只有 ①②④ + 懒执行的 ③,存在"已接流但继任未完成"窗口):
//
//	① biz 入口 writer gate(失主副本快速拒写);
//	② 既有 CompareAndSwapAssignment 精确前置快照 CAS;
//	③ 继任者水位推扫(AdvanceWriterFencesForToken)是**接流前硬门**:新写者当选后、
//	   对外宣告持有领导权**之前**必须先把**全部已知 pod** 的 fence 推进到本届 token
//	   (writerlease.Config.OnElected)。推扫失败即让位重选,本副本 Current() 恒不持有,
//	   写请求继续被拒。推扫完成后前任在任何 {pod} slot 上的席位预留/账本写全部被拒,
//	   其签出的票在 Admission 点必然找不到席位;
//	④ biz 出票前写者复核(票据只在「入口到返回全程持有租约」时交付,失主瞬间在途
//	   的请求不返回票,ErrUnavailable 引导重试路由到新写者);
//	⑤ **每玩家持久水位**:归属记录自身携带 HubAssignmentStorageRecord.writer_token
//	   (allocator.proto 31)。同一 key 的 WATCH/MULTI/EXEC 天然原子,故比较与写入在同
//	   一线性化点;current.writer_token > 本届 token → 零写入 ErrWriterSuperseded。
//	   旧记录 / 未启用 fencing 时该字段为 0,按"尚无水位"放行(滚动升级双向兼容)。
//	   R11 复审补两处交错(此前 ⑤ 只在"继任者的记录仍存在"时成立):
//	   ⑤a **删除写墓碑,不裸 DEL**:裸 DEL 把水位随业务记录一起抹掉,于继任者
//	      「创建 → 合法删除」之后,失主旧写者看到键不存在即可用旧 token 重建归属。
//	      改写只带 (player_id, writer_token) 的墓碑:GetAssignment 视为不存在,水位却
//	      比业务记录长寿(assignmentFenceTombstoneTTL)。
//	   ⑤b **租约在事务内读**:Current() 此前读在重试循环之外,旧写者暂停任意长后仍
//	      带陈旧 token 走完事务。放进 Watch 回调后失主至迟在下一 attempt 被发现,
//	      陈旧上界收敛为 etcd 租约 TTL——正是 ⑤a 墓碑 TTL 的取值依据。
package data

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/errcode"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"
)

// WriterFence 提供当前写者的 fencing token(dsauthfence/writerlease.Lease 满足此接口;
// nil = 未启用继任租约,dev/mock/单副本 Recreate 部署保持原行为)。
type WriterFence interface {
	// Current 返回 (token, 是否持有写者租约)。token 历届严格单调递增。
	Current() (uint64, bool)
}

// wfenceKey 与 shardKey/authKey/capacityLedgerKeys 同 hashtag {pod} 同 slot,
// 可捆进同一 Redis Cluster 事务(decision-revisit-hub-crossslot.md 单 slot 铁律)。
func wfenceKey(pod string) string { return fmt.Sprintf("pandora:hub:wfence:{%s}", pod) }

// ErrWriterSuperseded:本副本的写者租约已失效/被更新写者继任 → fail-closed 零写入。
// ErrUnavailable 语义:对调用方可重试(重试会被路由到新写者副本),对本副本是终态拒绝。
var ErrWriterSuperseded = errcode.New(errcode.ErrUnavailable,
	"hub allocator writer lease superseded; retry against current writer")

// assignmentFenceTombstoneTTL 是每玩家 fencing 墓碑的存活时间(R11 复审 P0-4 问题 A)。
//
// 墓碑只需活过「旧写者仍可能以为自己持有租约」的最长时间。CompareAndSwapAssignment /
// DeleteAssignmentIfPodMatches 在**事务内**读 fence.Current(),因此旧写者带进 EXEC 的
// token 至多陈旧 = etcd 租约剩余寿命(writerlease.DefaultLeaseTTLSec=15s)+ 一次 EXEC 往返。
// 5 分钟 ≈ 20× 租约 TTL,给足时钟偏移与 GC/暂停余量;墓碑自然过期后旧写者的租约必然
// 早已失效,in-tx Current() 返回不持有,水位不再需要。
//
// 取有限 TTL 而非持久键:墓碑数量 = 曾有归属的玩家数,持久化会无界增长(§9.24)。
const assignmentFenceTombstoneTTL = 5 * time.Minute

// isAssignmentFenceTombstone 判定归属记录是否只是 fencing 墓碑。真实归属必有
// hub_pod_name(分片身份是归属的本体);墓碑只带 player_id + writer_token。
// 故无需新增 proto 字段即可与真实归属区分,滚动升级双向兼容(旧副本读到墓碑时
// hub_pod_name 为空,同样不会把它当成可用归属)。
func isAssignmentFenceTombstone(rec *hubv1.HubAssignmentStorageRecord) bool {
	return rec != nil && rec.GetHubPodName() == ""
}

// newAssignmentFenceTombstone 构造墓碑:除水位与 player_id 外一律零值。
func newAssignmentFenceTombstone(playerID, token uint64) *hubv1.HubAssignmentStorageRecord {
	return &hubv1.HubAssignmentStorageRecord{PlayerId: playerID, WriterToken: token}
}

// noopAdvance 供未启用 fence / 无需推进时占位。
func noopAdvance(redis.Pipeliner) {}

// fencedPodTx 在 {pod} slot 上跑一个受写者水位保护的 WATCH/MULTI/EXEC(R11 复审 P0-4 续:
// 收口 assignment 之外的同类写入口)。水位比较、业务写与水位推进落在同一个 EXEC,
// 失主副本或落后 token 一律零写入 ErrWriterSuperseded。
//
// keys 必须全部与 pod 同 hashtag(shardKey / membersKey / transferCleanupKey 均满足),
// 否则 Redis Cluster 会 CROSSSLOT 拒绝整个事务。
//
// 未启用 fence(dev / 单副本 Recreate)时退化为原来的无 WATCH TxPipelined,行为逐字不变——
// 不引入新的乐观锁冲突重试。
func (r *RedisHubRepo) fencedPodTx(ctx context.Context, pod string, keys []string,
	mutate func(pipe redis.Pipeliner)) error {
	if r.fence == nil {
		_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			mutate(pipe)
			return nil
		})
		return err
	}
	watch := fencedWatchKeys(keys, pod, r.fence)
	const casMaxRetry = 8
	for attempt := 0; attempt < casMaxRetry; attempt++ {
		err := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			advance, gerr := guardWriterFence(ctx, tx, pod, r.fence)
			if gerr != nil {
				return gerr
			}
			_, perr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				mutate(pipe)
				advance(pipe)
				return nil
			})
			return perr
		}, watch...)
		if err == redis.TxFailedErr {
			casConflictBackoff(ctx, attempt)
			continue
		}
		return err
	}
	return errcode.New(errcode.ErrUnavailable, "hub fenced pod tx contention on pod %s", pod)
}

// requireWriterHeld 是**入口级**写者校验,用于键无法与任何 {pod} 水位同事务的写路径
// (无 hashtag 的 per-team 提示键、per-player 冷却键、以及跨 slot 的全局索引)。
//
// 它严格弱于 guardWriterFence:只挡"本副本已知失主",挡不住"检查通过后才失租"。
// 因此只允许用在**丢失即自愈、且不参与准入/归属判定**的键上,每个调用点必须注释写明
// 为什么做不成原子 fencing。归属、席位、容量账本一律不得降级到本函数。
func (r *RedisHubRepo) requireWriterHeld() error {
	if r.fence == nil {
		return nil
	}
	if _, held := r.fence.Current(); !held {
		return ErrWriterSuperseded
	}
	return nil
}

// guardWriterFence 在 Watch 回调内做 fence 比较,返回「推进闭包」供写事务在同一
// EXEC 内推进水位。调用方必须把 wfenceKey(pod) 加入 WATCH 集(见 fencedWatchKeys),
// 否则比较与推进之间的并发继任会绕过乐观锁。
//
// 只读事务可只调本函数校验、不执行 advance(检查恒保守安全)。
func guardWriterFence(ctx context.Context, tx *redis.Tx, pod string, fence WriterFence) (func(redis.Pipeliner), error) {
	if fence == nil {
		return noopAdvance, nil
	}
	mine, held := fence.Current()
	if !held {
		return nil, ErrWriterSuperseded
	}
	raw, err := tx.Get(ctx, wfenceKey(pod)).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	var cur uint64
	if err == nil {
		cur, err = strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("hub writer fence %s corrupt value %q: %w", pod, raw, err)
		}
	}
	if cur > mine {
		return nil, ErrWriterSuperseded
	}
	if cur == mine {
		return noopAdvance, nil
	}
	key, val := wfenceKey(pod), strconv.FormatUint(mine, 10)
	return func(pipe redis.Pipeliner) {
		pipe.Set(ctx, key, val, 0) // 持久:fencing 水位必须比业务记录长寿
	}, nil
}

// fencedWatchKeys 在启用 fence 时把 wfenceKey(pod) 并入 WATCH 集(同 slot)。
func fencedWatchKeys(keys []string, pod string, fence WriterFence) []string {
	if fence == nil {
		return keys
	}
	return append(keys, wfenceKey(pod))
}

// AdvanceWriterFences 是继任者水位推扫(见文件头覆盖边界 ③):把**全部已知 pod**
// (分片 SET ∪ 待清理 saga 源 pod)的 fence 主动推进到本届 token。逐 slot 懒推进只在
// 继任者写过的 slot 生效;推扫消灭「继任者尚未触碰的 pod」盲区——完成后前任写者在
// 任何 {pod} slot 上的席位/账本写永久出局。幂等,可在同一届内重复调用(cur==mine
// 直接跳过)。任一 pod 推扫遇到更大 token(自己已被继任)立即返回 ErrWriterSuperseded。
// fence 未注入时是 no-op。
func (r *RedisHubRepo) AdvanceWriterFences(ctx context.Context) error {
	if r.fence == nil {
		return nil
	}
	mine, held := r.fence.Current()
	if !held {
		return ErrWriterSuperseded
	}
	return r.advanceWriterFencesTo(ctx, mine)
}

// AdvanceWriterFencesForToken 用显式 token 推扫(R10 复审 P0-4 接流前硬门):写者租约的
// 激活钩子在「已当选、尚未宣告持有」的窗口里调用,此时 fence.Current() 故意还不返回
// held——推扫成功才是获得写权的前置条件,不能反过来依赖写权。token 必须 >0。
// 与 AdvanceWriterFences 同为幂等只进不退,可安全重试。
func (r *RedisHubRepo) AdvanceWriterFencesForToken(ctx context.Context, token uint64) error {
	if token == 0 {
		return errcode.New(errcode.ErrInvalidArg, "hub writer fence advance requires a non-zero token")
	}
	return r.advanceWriterFencesTo(ctx, token)
}

// advanceWriterFencesTo 是两个入口的公共实现:枚举全部已知 pod(分片 SET ∪ 待清理
// saga 源 pod)并逐 slot 只进不退推进水位。
func (r *RedisHubRepo) advanceWriterFencesTo(ctx context.Context, mine uint64) error {
	pods := map[string]struct{}{}
	shardPods, err := r.rdb.SMembers(ctx, shardsSetKey).Result()
	if err != nil {
		return err
	}
	for _, p := range shardPods {
		pods[p] = struct{}{}
	}
	cleanupPods, err := r.ListTransferCleanupPods(ctx)
	if err != nil {
		return err
	}
	for _, p := range cleanupPods {
		pods[p] = struct{}{}
	}
	for pod := range pods {
		if err := r.advanceWriterFencePod(ctx, pod, mine); err != nil {
			return err
		}
	}
	return nil
}

// advanceWriterFencePod 单 pod 水位推进:WATCH/MULTI/EXEC 只进不退。
func (r *RedisHubRepo) advanceWriterFencePod(ctx context.Context, pod string, mine uint64) error {
	key := wfenceKey(pod)
	const casMaxRetry = 8
	for attempt := 0; attempt < casMaxRetry; attempt++ {
		err := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			raw, gerr := tx.Get(ctx, key).Result()
			if gerr != nil && gerr != redis.Nil {
				return gerr
			}
			var cur uint64
			if gerr == nil {
				cur, gerr = strconv.ParseUint(raw, 10, 64)
				if gerr != nil {
					return fmt.Errorf("hub writer fence %s corrupt value %q: %w", pod, raw, gerr)
				}
			}
			if cur > mine {
				return ErrWriterSuperseded
			}
			if cur == mine {
				return nil
			}
			_, perr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, key, strconv.FormatUint(mine, 10), 0) // 持久:水位比业务记录长寿
				return nil
			})
			return perr
		}, key)
		if err == redis.TxFailedErr {
			continue
		}
		return err
	}
	return errcode.New(errcode.ErrUnavailable, "hub writer fence advance contention on pod %s", pod)
}
