// Package data 是 hub_allocator 服务的数据层(Redis 分片镜像 + 玩家归属)。
//
// Redis key 模板:
//
//	pandora:hub:shard:{<hub_pod_name>}  → HubShardStorageRecord proto bytes(hashtag 锁 slot),TTL=ShardTTL
//	pandora:hub:shards                  → SET(成员=hub_pod_name),ListHubs / 候选分片遍历
//	pandora:hub:active                  → ZSET(score=last_heartbeat_ms,member=hub_pod_name),心跳超时扫描
//	pandora:hub:player:<player_id>      → HubAssignmentStorageRecord proto bytes(不变量 §1 一人一 hub),strict admitted owner 无 TTL
//	pandora:hub:team:<team_id>          → string(hub_pod_name),队友同分片提示,TTL=AssignmentTTL
//
// 分片 player_count 写用 WATCH/MULTI/EXEC 乐观锁,冲突重试耗尽返 ErrHubNoAvailable。
package data

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"
)

// ── key 模板 ─────────────────────────────────────────────────────────────────

const (
	shardsSetKey           = "pandora:hub:shards"
	activeKey              = "pandora:hub:active"
	transferCleanupPodsKey = "pandora:hub:transfer_cleanup:pods"
)

func shardKey(pod string) string       { return fmt.Sprintf("pandora:hub:shard:{%s}", pod) }
func assignKey(playerID uint64) string { return fmt.Sprintf("pandora:hub:player:%d", playerID) }
func teamKey(teamID uint64) string     { return fmt.Sprintf("pandora:hub:team:%d", teamID) }

// transferCooldownKey 是玩家主动切线冷却占坑键(string,SET NX EX,TTL=cooldown)。
// 防止玩家高频刷线切换;冷却窗口内再切被拒(ErrHubTransferCooldown)。
func transferCooldownKey(playerID uint64) string {
	return fmt.Sprintf("pandora:hub:transfer_cd:%d", playerID)
}

// membersKey 是分片成员反向索引(SET,成员=player_id 十进制字符串)。
// hashtag {pod} 与 shardKey 同 slot,强制整合时按分片枚举玩家做服务端权威搬迁。
// best-effort:漂移不影响正确性(双通道中 Hub DS drain 心跳指令兼底漏听的玩家)。
func membersKey(pod string) string { return fmt.Sprintf("pandora:hub:shard:members:{%s}", pod) }

// transferCleanupKey 与 source shard 同 hashtag。注册顺序固定为全局 pod 索引 →
// per-pod exact ref → assignment CAS；因此 CAS 成功前崩溃至多留下可安全识别的 orphan，
// CAS 成功后绝不会缺 reconciler 索引。pod 索引是持久 superset，不因空集合删除，避免
// 并发 register 与“最后一项删除”竞态造成漏扫。
func transferCleanupKey(pod string) string {
	return fmt.Sprintf("pandora:hub:transfer_cleanup:{%s}", pod)
}

type TransferCleanupRef struct {
	PlayerID           uint64
	TargetAssignmentID string
}

func (r TransferCleanupRef) valid() bool {
	return r.PlayerID != 0 && strings.TrimSpace(r.TargetAssignmentID) != "" &&
		!strings.Contains(r.TargetAssignmentID, ":")
}

func encodeTransferCleanupRef(r TransferCleanupRef) string {
	return strconv.FormatUint(r.PlayerID, 10) + ":" + r.TargetAssignmentID
}

func decodeTransferCleanupRef(raw string) (TransferCleanupRef, bool) {
	player, assignmentID, ok := strings.Cut(raw, ":")
	if !ok || assignmentID == "" {
		return TransferCleanupRef{}, false
	}
	playerID, err := strconv.ParseUint(player, 10, 64)
	if err != nil {
		return TransferCleanupRef{}, false
	}
	ref := TransferCleanupRef{PlayerID: playerID, TargetAssignmentID: assignmentID}
	return ref, ref.valid()
}

// ── 接口 ──────────────────────────────────────────────────────────────────────

// HubRepo 是 hub_allocator 数据层抽象。biz 层只依赖此接口,不依赖 redis。
type HubRepo interface {
	// GetShard 读分片镜像。not found 返 (nil, false, nil)。
	GetShard(ctx context.Context, pod string) (*hubv1.HubShardStorageRecord, bool, error)
	// ListShards 列出全部已登记分片(ListHubs / 候选遍历用)。
	ListShards(ctx context.Context) ([]*hubv1.HubShardStorageRecord, error)
	// CreateShard 写分片镜像(TTL=shardTTL)并加入 shards SET(不进 active,等首次 Heartbeat)。
	CreateShard(ctx context.Context, rec *hubv1.HubShardStorageRecord, shardTTL time.Duration) error
	// UpdateShardWithLock WATCH/MULTI/EXEC 读-改-写分片;CAS 失败重试 maxRetry 次,耗尽返 ErrHubNoAvailable。
	UpdateShardWithLock(ctx context.Context, pod string, maxRetry int, fn func(*hubv1.HubShardStorageRecord) error, shardTTL time.Duration) error
	// HeartbeatShard Hub DS 心跳上报:仅刷新已存在分片(player_count/state/last_heartbeat_ms)并 ZADD active。
	// 分片不存在(孤儿 DS)返 (false, nil),由 biz 下发 stop 指令。HeartbeatRequest 不含 addr/region,
	// 故不在心跳路径建档(分片拓扑由 Fleet provider 登记)。
	// tokenGen:本次心跳携带的已验签 DS 回调令牌代际(Redis INCR 单调值;0=无)。genRequired:enforce
	// 代际门是否开启(= biz.dsTokenGeneration)。代际校验在**任何镜像变更之前**做,过期/缺失代际一律
	// fail-closed(返回 ErrShardTokenStale,镜像零变更:不刷 player_count/state/last_heartbeat_ms/TTL,
	// 不进 active 索引),旧代际/无令牌 DS 不能借心跳保活/占位/伪造在场(审核 P1)。stale 两种情形:
	// ① 镜像已绑定代际(CurrentTokenGen!=0)但心跳代际不等(含 0);② genRequired 但心跳无代际(tokenGen==0)。
	HeartbeatShard(ctx context.Context, pod string, playerCount int32, state string, tsMs int64, tokenGen uint64, genRequired bool, shardTTL time.Duration) (bool, error)
	// RemoveShard 删分片镜像 + 移出 shards SET + 移出 active ZSET。
	RemoveShard(ctx context.Context, pod string) error
	// RangeStaleShards 返回 active ZSET 中 last_heartbeat_ms ≤ thresholdMs(且 >0)的 pod(心跳超时)。
	RangeStaleShards(ctx context.Context, thresholdMs int64) ([]string, error)
	// RemoveActive 把 pod 移出 active ZSET(不再心跳扫描)。
	RemoveActive(ctx context.Context, pod string) error

	// GetAssignment 读玩家归属。not found 返 (nil, false, nil)。
	GetAssignment(ctx context.Context, playerID uint64) (*hubv1.HubAssignmentStorageRecord, bool, error)
	// SetAssignment 写玩家归属(TTL=assignmentTTL)。
	SetAssignment(ctx context.Context, rec *hubv1.HubAssignmentStorageRecord, assignmentTTL time.Duration) error
	// CompareAndSwapAssignment 以玩家单键为线性化点精确 CAS 归属。
	// expected=nil 表示仅当键不存在时创建；next=nil 表示仅当当前值完整等于 expected 时删除。
	// 比较覆盖 unknown fields，滚动更新期间不会把新副本字段静默当成相同。返回 false 表示前置快照已变化，零写入。
	CompareAndSwapAssignment(ctx context.Context, playerID uint64, expected, next *hubv1.HubAssignmentStorageRecord, assignmentTTL time.Duration) (bool, error)
	// DeleteAssignmentIfPodMatches CAS 删玩家归属:仅当当前归属仍指向 pod 才删。
	// 防止 ReleaseHub 读到旧归属后、并发 Assign/Transfer 已写入新归属时无条件 DEL 误删新归属
	// (同 team 孤儿索引修复的写序铁律:删除必须带前置校验)。
	// 已不存在或已指向其它分片 → (false, nil) 不删;删成功 → (true, nil)。
	DeleteAssignmentIfPodMatches(ctx context.Context, playerID uint64, pod string) (bool, error)

	// GetTeamShard 读队伍同分片提示。not found 返 ("", false, nil)。
	GetTeamShard(ctx context.Context, teamID uint64) (string, bool, error)
	// SetTeamShard 写队伍同分片提示(TTL=assignmentTTL)。
	SetTeamShard(ctx context.Context, teamID uint64, pod string, assignmentTTL time.Duration) error

	// AddShardMember 把 player_id 记入分片成员反向索引(强制整合枚举玩家用)。ttl<=0
	// 表示持久化到 explicit Departure/assignment cleanup，避免长连玩家从 drain 枚举消失。
	AddShardMember(ctx context.Context, pod string, playerID uint64, assignmentTTL time.Duration) error
	// RemoveShardMember 把 player_id 移出分片成员反向索引。
	RemoveShardMember(ctx context.Context, pod string, playerID uint64) error
	// ListShardMembers 列出分片成员反向索引中的 player_id(强制整合时遍历待迁玩家)。
	ListShardMembers(ctx context.Context, pod string) ([]uint64, error)

	// RegisterTransferCleanup 必须在切换 assignment CAS 前成功。ref 绑定目标 assignment_id，
	// CAS loser 只能删除自己的 ref，不能误删并发 winner。记录无 TTL；route/cleanup 不依赖过期。
	RegisterTransferCleanup(ctx context.Context, sourcePod string, ref TransferCleanupRef) error
	RemoveTransferCleanup(ctx context.Context, sourcePod string, ref TransferCleanupRef) error
	ListTransferCleanupPods(ctx context.Context) ([]string, error)
	ListTransferCleanups(ctx context.Context, sourcePod string) ([]TransferCleanupRef, error)

	// TryTransferCooldown 玩家主动切线防刷占坑(SET NX EX,TTL=cooldown)。
	// 冷却窗口内首次切线返 (true, nil) 并占坑;窗口内再切返 (false, nil)(应拒绝)。
	// cooldown<=0 视为不限流,恒返 (true, nil)。
	TryTransferCooldown(ctx context.Context, playerID uint64, cooldown time.Duration) (bool, error)
	// ClearTransferCooldown 清除切线冷却占坑(切线失败时释放,让玩家可立即重试)。best-effort。
	ClearTransferCooldown(ctx context.Context, playerID uint64) error

	// AdvanceWriterFences 继任者水位推扫:把全部已知 pod 的写者 fence 推进到本届 token
	// (writer_fence.go 覆盖边界 ③)。fence 未启用时 no-op;失主/被继任返 ErrWriterSuperseded。
	AdvanceWriterFences(ctx context.Context) error
	// AdvanceWriterFencesForToken 同上,但用**显式 token**推扫:供写者租约的「接流前
	// 激活钩子」在 Current() 尚未宣告持有时调用(R10 P0-4 硬门,见 writer_fence.go)。
	AdvanceWriterFencesForToken(ctx context.Context, token uint64) error
}

// ── Redis 实现 ────────────────────────────────────────────────────────────────

// RedisHubRepo 是基于 go-redis/v9 的 HubRepo 实现。
type RedisHubRepo struct {
	rdb redis.UniversalClient
	// fence:写者继任 fencing token 源(writer_fence.go;nil = 未启用,保持原行为)。
	fence WriterFence
}

// NewRedisHubRepo 构造 RedisHubRepo。
func NewRedisHubRepo(rdb redis.UniversalClient) *RedisHubRepo {
	return &RedisHubRepo{rdb: rdb}
}

// SetWriterFence 注入写者继任 fencing(Model B 生产由 main 注入;见 writer_fence.go)。
func (r *RedisHubRepo) SetWriterFence(f WriterFence) { r.fence = f }

func (r *RedisHubRepo) GetShard(ctx context.Context, pod string) (*hubv1.HubShardStorageRecord, bool, error) {
	b, err := r.rdb.Get(ctx, shardKey(pod)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	rec, err := unmarshalShard(pod, b)
	if err != nil {
		return nil, false, err
	}
	return rec, true, nil
}

func (r *RedisHubRepo) ListShards(ctx context.Context) ([]*hubv1.HubShardStorageRecord, error) {
	pods, err := r.rdb.SMembers(ctx, shardsSetKey).Result()
	if err != nil {
		return nil, err
	}
	out := make([]*hubv1.HubShardStorageRecord, 0, len(pods))
	for _, pod := range pods {
		rec, found, gerr := r.GetShard(ctx, pod)
		if gerr != nil {
			return nil, gerr
		}
		if !found {
			// 镜像已过期但 SET 残留 → 顺手清理
			_ = r.rdb.SRem(ctx, shardsSetKey, pod).Err()
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// CreateShard 写分片镜像(权威)并登记到全局 shards SET。
//
// **init-only 语义(审核二轮 CE7)**:用 SET NX 只在分片键不存在时初始化,已存在则**绝不覆盖**
// —— 两个并发 GetShard-miss 的种子调用(ensureShards / reconcile 新 pod 分支)不会互相把对方
// 刚写入的心跳 / last_verified / 状态清回初始值。已存在分片的地址 / 容量刷新由 reconcile 的
// UpdateShardWithLock 单调合并负责,不走本路径覆盖。
//
// Redis Cluster 兼容(decision-revisit-hub-crossslot.md):shardKey{pod} 与全局 shardsSetKey
// 分属不同 slot,不能捆同一事务。① shardKey 单键 SET NX 初始化;② shardsSetKey 独立 SADD
// 登记 membership(必须成功,否则 ListShards 漏这个分片)。两步幂等,失败重试可重入。
func (r *RedisHubRepo) CreateShard(ctx context.Context, rec *hubv1.HubShardStorageRecord, shardTTL time.Duration) error {
	payload, err := marshalShard(rec)
	if err != nil {
		return err
	}
	// SET NX:仅初始化,不覆盖既有镜像(CE7 防并发种子互相清心跳/last_verified)。
	if err := r.rdb.SetNX(ctx, shardKey(rec.HubPodName), payload, shardTTL).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, shardsSetKey, rec.HubPodName).Err()
}

func (r *RedisHubRepo) UpdateShardWithLock(
	ctx context.Context,
	pod string,
	maxRetry int,
	fn func(*hubv1.HubShardStorageRecord) error,
	shardTTL time.Duration,
) error {
	key := shardKey(pod)
	watchKeys := fencedWatchKeys([]string{key}, pod, r.fence)

	for attempt := 0; attempt <= maxRetry; attempt++ {
		var fnErr error

		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			// 写者继任 fence(writer_fence.go):迟到旧写者 fail-closed 零写入。
			advanceFence, ferr := guardWriterFence(ctx, tx, pod, r.fence)
			if ferr != nil {
				return ferr
			}
			b, err := tx.Get(ctx, key).Bytes()
			if err == redis.Nil {
				return errcode.New(errcode.ErrHubNoAvailable, "hub shard %s not found", pod)
			}
			if err != nil {
				return err
			}
			rec, err := unmarshalShard(pod, b)
			if err != nil {
				return err
			}
			if fnErr = fn(rec); fnErr != nil {
				return fnErr
			}
			payload, err := marshalShard(rec)
			if err != nil {
				return err
			}
			// Cluster 兼容:WATCH/SET 只围 {pod} 单 slot;全局 shardsSetKey 移出事务。
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				advanceFence(pipe)
				pipe.Set(ctx, key, payload, shardTTL)
				return nil
			})
			return err
		}, watchKeys...)

		if txErr == nil {
			// shards membership re-ensure(独立命令,幂等;membership 已在 CreateShard 建立,
			// best-effort:失败不影响权威镜像,ListShards 自愈 + 下次心跳补)。
			_ = r.rdb.SAdd(ctx, shardsSetKey, pod).Err()
			return nil
		}
		if txErr == fnErr && fnErr != nil {
			return fnErr // fn 业务错误,不重试
		}
		if txErr == redis.TxFailedErr {
			casConflictBackoff(ctx, attempt)
			continue // CAS 冲突,重试
		}
		return txErr
	}
	return errcode.New(errcode.ErrHubNoAvailable, "hub shard %s update concurrent retry exhausted", pod)
}

// errShardTokenStale:enforce 代际门下心跳令牌代际过期/缺失 → fail-closed。
// 返回前对镜像**零变更**(不刷 player_count/state/last_heartbeat_ms/TTL,不进 active 索引),
// biz 透传该错误 → service 层据此**不刷 presence**(审核 P1:旧代际心跳不得保活/占位/伪造在场)。
// 用 ErrUnauthorized(=8)对客户端/DS 呈现明确的鉴权拒绝码。
var errShardTokenStale = errcode.New(errcode.ErrUnauthorized, "hub heartbeat token generation stale")

func (r *RedisHubRepo) HeartbeatShard(ctx context.Context, pod string, playerCount int32, state string, tsMs int64, tokenGen uint64, genRequired bool, shardTTL time.Duration) (bool, error) {
	key := shardKey(pod)
	found := false
	err := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
		// 写者继任 fence:与令牌代际门同理,必须在任何镜像变更之前 fail-closed。
		advanceFence, ferr := guardWriterFence(ctx, tx, pod, r.fence)
		if ferr != nil {
			return ferr
		}
		b, gerr := tx.Get(ctx, key).Bytes()
		if gerr == redis.Nil {
			found = false
			return nil // 孤儿 DS:不建档,由 biz 回 stop
		}
		if gerr != nil {
			return gerr
		}
		rec, uerr := unmarshalShard(pod, b)
		if uerr != nil {
			return uerr
		}
		found = true
		// 令牌代际校验(审核 P1):代际门下过期/缺失代际的心跳一律 fail-closed,且必须在**任何镜像变更之前**
		// 拒绝 —— 旧代际/无令牌心跳不得刷 player_count/state/last_heartbeat_ms/TTL,也不得进 active 索引
		// (旧代际 DS 不能借心跳保活/占位;presence 由上层据本错误跳过)。
		//   ① 镜像已绑定代际(CurrentTokenGen!=0)但心跳代际不等(含 0)→ stale;
		//   ② genRequired(enforce 代际门开)但心跳无代际(tokenGen==0)→ stale(挡 legacy gen0 关闭代际门)。
		// gen 来自 Redis INCR 单调值,同秒多次重签不碰撞;精确相等才算「当前代际」(替代旧 exp 秒级比较)。
		if (rec.CurrentTokenGen != 0 && tokenGen != rec.CurrentTokenGen) || (genRequired && tokenGen == 0) {
			return errShardTokenStale // 零变更返回:WATCH fn 报错 → 不 EXEC,镜像/索引/TTL 全不动
		}
		// —— 代际校验通过,方可变更镜像 ——
		applyHeartbeatToShard(rec, playerCount, state, tsMs)
		payload, merr := marshalShard(rec)
		if merr != nil {
			return merr
		}
		// Cluster 兼容:WATCH/SET 只围 {pod} 单 slot;全局 shards/active 索引移出事务(见下)。
		_, perr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			advanceFence(pipe)
			pipe.Set(ctx, key, payload, shardTTL)
			return nil
		})
		return perr
	}, fencedWatchKeys([]string{key}, pod, r.fence)...)
	if err != nil {
		return false, err
	}
	if found {
		// 全局索引:与 shardKey 不同 slot,各自独立命令。幂等;心跳高频,失败下次即补。
		if serr := r.rdb.SAdd(ctx, shardsSetKey, pod).Err(); serr != nil {
			return true, serr
		}
		if zerr := r.rdb.ZAdd(ctx, activeKey, redis.Z{Score: float64(tsMs), Member: pod}).Err(); zerr != nil {
			return true, zerr
		}
	}
	return found, nil
}

// RemoveShard 删分片镜像 + 成员索引 + 全局 shards/active 登记。
//
// Redis Cluster 兼容(decision-revisit-hub-crossslot.md):shardKey/membersKey 同 hashtag {pod}
// 同 slot,一个 mini-tx;全局 shards/active 不同 slot,拆为独立命令。全部幂等,残留由 ListShards
// 自愈 + active 扫到已删镜像跳过兜底。
func (r *RedisHubRepo) RemoveShard(ctx context.Context, pod string) error {
	// per-pod 同 slot:镜像 + 成员索引一起删。
	if _, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, shardKey(pod))
		pipe.Del(ctx, membersKey(pod))
		return nil
	}); err != nil {
		return err
	}
	// 全局索引:独立命令(不同 slot)。
	if err := r.rdb.SRem(ctx, shardsSetKey, pod).Err(); err != nil {
		return err
	}
	return r.rdb.ZRem(ctx, activeKey, pod).Err()
}

func (r *RedisHubRepo) RangeStaleShards(ctx context.Context, thresholdMs int64) ([]string, error) {
	// Min "(0" 排除从未心跳的 Mock 种子(score=0);Max=threshold 含等于。
	return r.rdb.ZRangeByScore(ctx, activeKey, &redis.ZRangeBy{
		Min: "(0",
		Max: strconv.FormatInt(thresholdMs, 10),
	}).Result()
}

func (r *RedisHubRepo) RemoveActive(ctx context.Context, pod string) error {
	return r.rdb.ZRem(ctx, activeKey, pod).Err()
}

func (r *RedisHubRepo) GetAssignment(ctx context.Context, playerID uint64) (*hubv1.HubAssignmentStorageRecord, bool, error) {
	b, err := r.rdb.Get(ctx, assignKey(playerID)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	rec := &hubv1.HubAssignmentStorageRecord{}
	if uerr := proto.Unmarshal(b, rec); uerr != nil {
		return nil, false, fmt.Errorf("assignment %d bad proto: %w", playerID, uerr)
	}
	// fencing 墓碑对业务不可见:玩家确实无归属,只是水位还在(见 writer_fence.go ⑤)。
	if isAssignmentFenceTombstone(rec) {
		return nil, false, nil
	}
	return rec, true, nil
}

// SetAssignment 无条件写(旧接口,仅 dev/测试路径)。启用写者 fencing 后它是纯粹的
// fencing 旁路——无 WATCH 无比较,失主旧写者可借它覆盖继任者的归属——故 fail-closed
// 拒绝,归属写一律走 CompareAndSwapAssignment(R11 复审 P0-4)。
func (r *RedisHubRepo) SetAssignment(ctx context.Context, rec *hubv1.HubAssignmentStorageRecord, assignmentTTL time.Duration) error {
	if r.fence != nil {
		return errcode.New(errcode.ErrInternal,
			"assignment unconditional Set is fencing-unsafe; use CompareAndSwapAssignment")
	}
	payload, err := proto.Marshal(rec)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, assignKey(rec.PlayerId), payload, assignmentTTL).Err()
}

func (r *RedisHubRepo) CompareAndSwapAssignment(
	ctx context.Context,
	playerID uint64,
	expected, next *hubv1.HubAssignmentStorageRecord,
	assignmentTTL time.Duration,
) (bool, error) {
	if playerID == 0 || (expected != nil && expected.PlayerId != playerID) || (next != nil && next.PlayerId != playerID) {
		return false, errcode.New(errcode.ErrInvalidArg, "assignment CAS player_id mismatch")
	}
	key := assignKey(playerID)
	const casMaxRetry = 8
	for attempt := 0; attempt < casMaxRetry; attempt++ {
		matched := false
		err := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			// 每玩家持久写者水位(R10 复审 P0-4;R11 复审收口两处交错):assignment 键无
			// hashtag,进不了 {pod} slot 事务,用不了 pandora:hub:wfence:{pod}。水位记在归属
			// 记录**自身**里——同一 key 的 WATCH/MULTI/EXEC 天然原子,比较与写入同线性化点。
			//
			// R11 复审 P0-4 问题 B(检查后执行):租约必须在**事务内**读。此前 Current() 读在
			// 重试循环之外,旧写者暂停任意长后仍带着陈旧的 mine 走完事务。放进回调后,失主
			// 至迟在下一次 attempt 被发现;窗口收敛到「本次 EXEC」,其上界即 etcd 租约 TTL。
			mine, held := uint64(0), true
			if r.fence != nil {
				if mine, held = r.fence.Current(); !held {
					return ErrWriterSuperseded
				}
			}
			var current *hubv1.HubAssignmentStorageRecord
			b, gerr := tx.Get(ctx, key).Bytes()
			switch {
			case gerr == redis.Nil:
				if expected != nil {
					return nil
				}
			case gerr != nil:
				return gerr
			default:
				current = &hubv1.HubAssignmentStorageRecord{}
				if uerr := proto.Unmarshal(b, current); uerr != nil {
					return fmt.Errorf("assignment %d bad proto: %w", playerID, uerr)
				}
				// 水位只进不退:记录已被更高代写者触碰 → 本副本永久出局(零写入)。
				// 0 = 本字段上线前的旧记录 / 未启用 fencing,按"尚无水位"放行。
				if r.fence != nil && current.GetWriterToken() > mine {
					return ErrWriterSuperseded
				}
				// 墓碑 = 业务上无归属、水位仍在。比较按"键不存在"处理,水位比较照旧。
				if isAssignmentFenceTombstone(current) {
					if expected != nil {
						return nil
					}
				} else if expected == nil || !proto.Equal(current, expected) {
					return nil
				}
			}

			var payload []byte
			write := next
			if next == nil && r.fence != nil {
				// R11 复审 P0-4 问题 A(删除即复位):裸 DEL 会把水位随业务记录一起抹掉,
				// 于继任者「创建→合法删除」之后,失主旧写者看到键不存在便能以旧 token 重建
				// 归属(借尸还魂)。改写 fencing 墓碑:业务上等于无归属(GetAssignment 返回
				// 不存在),但水位活得比业务记录长,旧写者一定读到更高 token 而零写入。
				write = newAssignmentFenceTombstone(playerID, mine)
			}
			if write != nil {
				if r.fence != nil && write.GetWriterToken() != mine {
					// 克隆后盖水位:调用方(biz)可能复用同一 message 做后续比较,
					// 不能就地改写它的字段。
					write = proto.Clone(write).(*hubv1.HubAssignmentStorageRecord)
					write.WriterToken = mine
				}
				var merr error
				payload, merr = proto.Marshal(write)
				if merr != nil {
					return merr
				}
			}
			_, perr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				switch {
				case write == nil:
					pipe.Del(ctx, key) // 未启用 fencing:保持旧行为
				case next == nil:
					pipe.Set(ctx, key, payload, assignmentFenceTombstoneTTL)
				default:
					pipe.Set(ctx, key, payload, assignmentTTL)
				}
				return nil
			})
			if perr == nil {
				matched = true
			}
			return perr
		}, key)
		if err == redis.TxFailedErr {
			casConflictBackoff(ctx, attempt)
			continue
		}
		if err != nil {
			return false, err
		}
		return matched, nil
	}
	// 高并发下 WATCH 连续冲突只表示 expected 已不再稳定；交给上层重读最新归属重试，零写入。
	return false, nil
}

func (r *RedisHubRepo) DeleteAssignmentIfPodMatches(ctx context.Context, playerID uint64, pod string) (bool, error) {
	key := assignKey(playerID)
	const casMaxRetry = 3
	for i := 0; i < casMaxRetry; i++ {
		deleted := false
		err := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			// 与 CompareAndSwapAssignment 同契约(R11 复审 P0-4):事务内读租约,
			// 删除写墓碑而非裸 DEL,否则本路径同样是「删除即复位」的水位抹除口。
			mine, held := uint64(0), true
			if r.fence != nil {
				if mine, held = r.fence.Current(); !held {
					return ErrWriterSuperseded
				}
			}
			b, gerr := tx.Get(ctx, key).Bytes()
			if gerr == redis.Nil {
				return nil // 已不存在,幂等视为无需删
			}
			if gerr != nil {
				return gerr
			}
			rec := &hubv1.HubAssignmentStorageRecord{}
			if uerr := proto.Unmarshal(b, rec); uerr != nil {
				return fmt.Errorf("assignment %d bad proto: %w", playerID, uerr)
			}
			if r.fence != nil && rec.GetWriterToken() > mine {
				return ErrWriterSuperseded
			}
			if isAssignmentFenceTombstone(rec) {
				return nil // 已是墓碑:业务上无归属,幂等无需删
			}
			if rec.HubPodName != pod {
				return nil // 并发 Assign/Transfer 已指向新分片,不能删
			}
			var payload []byte
			if r.fence != nil {
				var merr error
				payload, merr = proto.Marshal(newAssignmentFenceTombstone(playerID, mine))
				if merr != nil {
					return merr
				}
			}
			_, perr := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				if payload == nil {
					pipe.Del(ctx, key) // 未启用 fencing:保持旧行为
				} else {
					pipe.Set(ctx, key, payload, assignmentFenceTombstoneTTL)
				}
				return nil
			})
			if perr == nil {
				deleted = true
			}
			return perr
		}, key)
		if err == redis.TxFailedErr {
			casConflictBackoff(ctx, i)
			continue // WATCH 期间归属被改写,重读再判
		}
		if err != nil {
			return false, err
		}
		return deleted, nil
	}
	// 重试耗尽:归属正被并发频繁改写,安全侧不删(新归属为准)。
	return false, nil
}

func (r *RedisHubRepo) GetTeamShard(ctx context.Context, teamID uint64) (string, bool, error) {
	pod, err := r.rdb.Get(ctx, teamKey(teamID)).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return pod, true, nil
}

func (r *RedisHubRepo) SetTeamShard(ctx context.Context, teamID uint64, pod string, assignmentTTL time.Duration) error {
	return r.rdb.Set(ctx, teamKey(teamID), pod, assignmentTTL).Err()
}

func (r *RedisHubRepo) AddShardMember(ctx context.Context, pod string, playerID uint64, assignmentTTL time.Duration) error {
	key := membersKey(pod)
	_, err := r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.SAdd(ctx, key, strconv.FormatUint(playerID, 10))
		if assignmentTTL > 0 {
			pipe.Expire(ctx, key, assignmentTTL)
		} else {
			pipe.Persist(ctx, key)
		}
		return nil
	})
	return err
}

func (r *RedisHubRepo) RemoveShardMember(ctx context.Context, pod string, playerID uint64) error {
	return r.rdb.SRem(ctx, membersKey(pod), strconv.FormatUint(playerID, 10)).Err()
}

func (r *RedisHubRepo) ListShardMembers(ctx context.Context, pod string) ([]uint64, error) {
	members, err := r.rdb.SMembers(ctx, membersKey(pod)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]uint64, 0, len(members))
	for _, m := range members {
		pid, perr := strconv.ParseUint(m, 10, 64)
		if perr != nil {
			continue // 脏成员,跳过
		}
		out = append(out, pid)
	}
	return out, nil
}

func (r *RedisHubRepo) RegisterTransferCleanup(ctx context.Context, sourcePod string, ref TransferCleanupRef) error {
	if strings.TrimSpace(sourcePod) == "" || !ref.valid() {
		return errcode.New(errcode.ErrInvalidArg, "complete Hub transfer cleanup ref required")
	}
	// Global index first. If either command has an unknown result, callers must
	// not publish assignment; any orphan is removed by the reconciler.
	if err := r.rdb.SAdd(ctx, transferCleanupPodsKey, sourcePod).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, transferCleanupKey(sourcePod), encodeTransferCleanupRef(ref)).Err()
}

func (r *RedisHubRepo) RemoveTransferCleanup(ctx context.Context, sourcePod string, ref TransferCleanupRef) error {
	if strings.TrimSpace(sourcePod) == "" || !ref.valid() {
		return errcode.New(errcode.ErrInvalidArg, "complete Hub transfer cleanup ref required")
	}
	return r.rdb.SRem(ctx, transferCleanupKey(sourcePod), encodeTransferCleanupRef(ref)).Err()
}

func (r *RedisHubRepo) ListTransferCleanupPods(ctx context.Context) ([]string, error) {
	return r.rdb.SMembers(ctx, transferCleanupPodsKey).Result()
}

func (r *RedisHubRepo) ListTransferCleanups(ctx context.Context, sourcePod string) ([]TransferCleanupRef, error) {
	if strings.TrimSpace(sourcePod) == "" {
		return nil, errcode.New(errcode.ErrInvalidArg, "source Hub pod required")
	}
	raw, err := r.rdb.SMembers(ctx, transferCleanupKey(sourcePod)).Result()
	if err != nil {
		return nil, err
	}
	out := make([]TransferCleanupRef, 0, len(raw))
	for _, item := range raw {
		if ref, ok := decodeTransferCleanupRef(item); ok {
			out = append(out, ref)
		}
	}
	return out, nil
}

func (r *RedisHubRepo) TryTransferCooldown(ctx context.Context, playerID uint64, cooldown time.Duration) (bool, error) {
	if cooldown <= 0 {
		return true, nil // 不限流
	}
	// SET key 1 NX EX cooldown:占坑成功=首次切线;已存在=冷却中。
	ok, err := r.rdb.SetNX(ctx, transferCooldownKey(playerID), "1", cooldown).Result()
	if err != nil {
		return false, err
	}
	return ok, nil
}

func (r *RedisHubRepo) ClearTransferCooldown(ctx context.Context, playerID uint64) error {
	return r.rdb.Del(ctx, transferCooldownKey(playerID)).Err()
}

// ── 序列化辅助 ────────────────────────────────────────────────────────────────

// applyHeartbeatToShard 把一次(已鉴权/已授权的)心跳上报应用到分片镜像:对账在线数、
// 推进状态机、刷新 last_heartbeat。抽成纯函数供 HeartbeatShard(legacy 代际门路径)与
// ActivateHeartbeat(Model B 授权原子路径)共用**同一套状态机语义**,杜绝两条路径漂移。
//
// 状态机:允许 DS 上报升级 drain 等级(ready→draining→stopping),但禁止把 allocator 强制整合
// 标记的 draining/stopping 被 DS 上报的 ready 冲掉。存活恢复例外:心跳超时误标的 draining
// (draining_since_ms==0)不是 allocator 主动意图,只是「DS 可能已死」的推断;一个健康心跳即推断
// 失效的直接证据,允许它复位 ready,打断「活着的 DS 被误判超时后永久卡 draining」的死锁。强制整合
// 排空的 draining(draining_since_ms>0)仍 sticky。调用方须保证心跳已通过代际/授权校验(stale
// 心跳必须在调用本函数前 fail-closed 返回,零变更)。
func applyHeartbeatToShard(rec *hubv1.HubShardStorageRecord, playerCount int32, state string, tsMs int64) {
	rec.PlayerCount = playerCount
	applyHeartbeatStateToShard(rec, state, tsMs)
}

// applyHeartbeatStateToShard 是 Model B 专用状态更新：容量 player_count 由 reservation+
// connected ownership ledger 派生，绝不接受 DS reported count 覆盖。
func applyHeartbeatStateToShard(rec *hubv1.HubShardStorageRecord, state string, tsMs int64) {
	switch {
	case rec.State == "warming":
		// 首个通过 Guard/授权的心跳即「DS 已就绪且可信」的直接证据:warming → ready。
		// 若 DS 首跳已上报更高 drain 等级(draining/stopping),则采纳其上报,不强行 ready。
		if drainRank(state) > 0 {
			rec.State = state
		} else {
			rec.State = "ready"
		}
	case state == "":
		// 空上报:不动状态。
	case drainRank(state) >= drainRank(rec.State):
		rec.State = state // 升级或同级 drain → 采用 DS 上报
	case state == "ready" && rec.State == "draining" && rec.DrainingSinceMs == 0:
		rec.State = "ready" // 存活恢复:心跳超时误标的 draining 被健康心跳复位
	default:
		// 其余降级(强制整合 draining 被 ready 冲)→ 保持 rec.State 不变。
	}
	rec.LastHeartbeatMs = tsMs
}

// drainRank 把分片状态映射成排空等级(ready<draining<stopping),
// 心跳路径用它防止 allocator 标记的 draining/stopping 被 DS 上报的 ready 降级。
func drainRank(state string) int {
	switch state {
	case "draining":
		return 1
	case "stopping":
		return 2
	default:
		return 0 // "ready" / "" / 未知
	}
}

func marshalShard(rec *hubv1.HubShardStorageRecord) ([]byte, error) {
	if rec == nil {
		return nil, fmt.Errorf("nil hub shard")
	}
	return proto.Marshal(rec)
}

func unmarshalShard(pod string, payload []byte) (*hubv1.HubShardStorageRecord, error) {
	rec := &hubv1.HubShardStorageRecord{}
	if err := proto.Unmarshal(payload, rec); err != nil {
		return nil, fmt.Errorf("hub shard %s bad proto: %w", pod, err)
	}
	if rec.HubPodName == "" {
		rec.HubPodName = pod
	}
	if rec.HubPodName != pod {
		return nil, fmt.Errorf("hub shard %s pod mismatch: %s", pod, rec.HubPodName)
	}
	return rec, nil
}
