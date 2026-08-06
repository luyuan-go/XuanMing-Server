// Package data 是 player_locator 服务的数据层(redis-only)。
//
// W3 ⑤(2026-06-05):
//   - Redis hash: pandora:locator:<player_id>
//   - TTL 30s,SetLocation 每次刷新
//   - 不接 MySQL(locator 是临时态,玩家离线 → 30s 后自动消失)
//
// W4 ⑩(2026-06-06):
//   - 覆盖式 Set 升级为 SetGuarded:WATCH/MULTI/EXEC 原子读-判-写,
//     先把当前记录交给 biz guard 决策(不变量 §1 状态机守卫),通过才覆盖写。
package data

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// LocationRecord 是写入 / 读出 redis 的中间结构(避免 data 层依赖 proto)。
//
// state 用 int32 保存(直接对应 pandora.locator.v1.LocationState 枚举值),
// service 层负责跟 proto enum 互转。
type LocationRecord struct {
	State       int32
	HubPod      string
	ShardID     uint32
	MatchID     uint64
	BattlePod   string
	UpdatedAtMs int64
}

// LocationRepo 玩家位置仓储接口。
type LocationRepo interface {
	// SetGuarded WATCH/MULTI/EXEC 原子读-判-写:先读当前记录交给 guard 决策,
	// guard 返回非 nil 则中止写并原样返回该错误(用于不变量 §1 状态机守卫);
	// guard 通过则 DEL+HSET+EXPIRE 覆盖式写入。CAS 冲突重试 maxRetry 次。
	SetGuarded(ctx context.Context, playerID uint64, rec LocationRecord, ttl time.Duration, maxRetry int, guard func(cur LocationRecord, found bool) error) error
	Get(ctx context.Context, playerID uint64) (LocationRecord, bool, error)
	// BatchGet 一次读多个玩家的位置(好友列表在线态批量拉,见
	// docs/design/friend-distributed-scaling.md §13.3)。用 Redis pipeline 一次往返,
	// 替代逐个 Get 的 N 次网络往返。返回 map 只含命中的玩家;
	// key 不存在(离线 / TTL 过期)的 player_id 不出现在 map 里(调用方按缺席判离线)。
	// playerID==0 与重复 id 自动跳过 / 去重。
	BatchGet(ctx context.Context, playerIDs []uint64) (map[uint64]LocationRecord, error)
	// RefreshHubLocations 批量续期 HUB 位置 TTL(在线保活,Hub DS 心跳捎带链路)。
	// 逐个校验「state==HUB 且 hub_pod 匹配」才 EXPIRE;MATCHING/BATTLE/其它 pod
	// 的记录不动(不变量 §1)。返实际续期成功条数。
	// 非事务(校验→EXPIRE 两次 pipeline 往返):竞争窗口内状态若切到 MATCHING/BATTLE,
	// 多续一次 30s TTL 无害(对局态由战斗链路自己刷 TTL,且后续写会重置)。
	RefreshHubLocations(ctx context.Context, hubPod string, playerIDs []uint64, ttl time.Duration) (int, error)
	// ShrinkHubTTL 快速断线上报:把玩家 HUB 位置的剩余 TTL 缩短到 grace(只缩不涨,
	// PEXPIRE LT)。守卫同 RefreshHubLocations:仅「state==HUB 且 hub_pod 匹配」才缩;
	// MATCHING/BATTLE/其它 pod/剩余 TTL 已更短 → 不动,返 false(均属正常路径)。
	// Lua 原子(单 key):校验与缩 TTL 同脚本执行,不存在「读到旧 HUB → 并发写成
	// MATCHING/BATTLE → 误缩新状态 TTL」的窗口(Codex 复审 2026-07-06)。
	ShrinkHubTTL(ctx context.Context, hubPod string, playerID uint64, grace time.Duration) (bool, error)
	// SetLastSeen 记录「Hub DS 观测到该玩家离开的时刻」,写在**独立 key**
	// `pandora:locator:lastseen:<player_id>` 上,retention 远长于位置 TTL。
	//
	// 为什么必须独立 key:位置记录是整 key 带 TTL 的 hash,key 一过期 updated_at_ms
	// 跟着消失,于是「已经离线多久」在权威侧无处可查。把时间戳放进同一个 hash 解决不了
	// 问题(一起被删),延长位置 TTL 更不行(会破坏 key miss = 离线这个全链依赖的判据,
	// 以及 §9.22 的 27s 再入屏障推导)。
	//
	// 只在 ShrinkHubTTL 守卫通过后调用(见 biz.ReportDisconnect 的顺序说明),
	// 因此语义严格是「确实离开了 Hub」,而不是「某台 DS 声称他走了」。
	SetLastSeen(ctx context.Context, playerID uint64, atMs int64, retention time.Duration) error
	// ClearLastSeen 删掉 last-seen 时刻(玩家回到 Hub 时调用)。
	//
	// 维持不变量:**last-seen 存在 ⟺ 该玩家离开 Hub 后还没回来过**。
	// 不清的话,一次「断线→秒重连」会在权威侧留下一个陈旧时刻;等到下一次掉线
	// 恰好没能写新时刻时(Hub DS 整台挂掉 → 压根不会调 ReportDisconnect),
	// 消费方就会拿那个旧时刻算出「已离线几十分钟」,把刚掉线 10 秒的玩家立刻踢掉。
	ClearLastSeen(ctx context.Context, playerID uint64) error
	// BatchGetLastSeen 批量读 last-seen 时刻。用 pipeline 一次往返。
	// 返回 map 只含有记录的玩家;缺席 = UNKNOWN(从未记录 / 已超 retention),
	// 调用方不得当成 0 或「刚离开」(§9.22 不确定不得冒充默认值)。
	BatchGetLastSeen(ctx context.Context, playerIDs []uint64) (map[uint64]int64, error)
	Delete(ctx context.Context, playerID uint64) error
}

// RedisLocationRepo 基于 go-redis/v9 的实现。
type RedisLocationRepo struct {
	rdb redis.UniversalClient
}

// NewRedisLocationRepo 构造。
func NewRedisLocationRepo(rdb redis.UniversalClient) *RedisLocationRepo {
	return &RedisLocationRepo{rdb: rdb}
}

func locKey(playerID uint64) string {
	return fmt.Sprintf("pandora:locator:%d", playerID)
}

// lastSeenKey returns "pandora:locator:lastseen:<playerID>" —— 与位置 key 分开,
// 好让它在位置 key 过期之后依然存在(见 LocationRepo.SetLastSeen 注释)。
//
// ⚠️ 刻意**不**与 locKey 做同 slot 的 hash tag:两者永远不在同一条 Lua / 事务里写
// (biz 按「先守卫缩 TTL、成功后再记时刻」的顺序分两步做,见 ReportDisconnect),
// 因此不存在 CROSSSLOT 问题;而给 locKey 补 hash tag 会改动全链已在用的 key 形态。
func lastSeenKey(playerID uint64) string {
	return fmt.Sprintf("pandora:locator:lastseen:%d", playerID)
}

// SetGuarded WATCH/MULTI/EXEC 原子读-判-写。
//
// 流程(每次重试一轮 WATCH):
//  1. WATCH key 并读当前记录
//  2. guard(cur, found):返回非 nil → 中止写,原样返回该错误(业务守卫拒绝,不重试)
//  3. MULTI:DEL + HSET 覆盖 + EXPIRE 刷新 TTL
//
// 先 DEL 再 HSET,保证不同 state 切换时不残留旧字段(BATTLE → HUB 时 match_id 不清除会误读)。
// CAS 冲突(EXEC 期间 key 被并发改)返回 TxFailedErr → 重试;耗尽 maxRetry 返 ErrLocatorConflict。
func (r *RedisLocationRepo) SetGuarded(
	ctx context.Context,
	playerID uint64,
	rec LocationRecord,
	ttl time.Duration,
	maxRetry int,
	guard func(cur LocationRecord, found bool) error,
) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "playerID must > 0")
	}
	key := locKey(playerID)
	if rec.UpdatedAtMs == 0 {
		rec.UpdatedAtMs = time.Now().UnixMilli()
	}

	for attempt := 0; attempt <= maxRetry; attempt++ {
		var guardErr error
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			cur, found, err := readLocation(ctx, tx, key)
			if err != nil {
				return err
			}
			if guard != nil {
				if guardErr = guard(cur, found); guardErr != nil {
					return guardErr
				}
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Del(ctx, key)
				pipe.HSet(ctx, key,
					"state", rec.State,
					"hub_pod", rec.HubPod,
					"shard_id", rec.ShardID,
					"match_id", rec.MatchID,
					"battle_pod", rec.BattlePod,
					"updated_at_ms", rec.UpdatedAtMs,
				)
				pipe.Expire(ctx, key, ttl)
				return nil
			})
			return err
		}, key)

		if txErr == nil {
			return nil
		}
		if guardErr != nil && txErr == guardErr {
			return guardErr // 业务守卫拒绝,不重试
		}
		if txErr == redis.TxFailedErr {
			continue // CAS 冲突,重试
		}
		return errcode.New(errcode.ErrInternal, "redis location set: %v", txErr)
	}
	return errcode.New(errcode.ErrLocatorConflict, "player %d location set concurrent retry exhausted", playerID)
}

// Get 返回 (record, found, err)。key 不存在 → found=false。
func (r *RedisLocationRepo) Get(ctx context.Context, playerID uint64) (LocationRecord, bool, error) {
	if playerID == 0 {
		return LocationRecord{}, false, errcode.New(errcode.ErrInvalidArg, "playerID must > 0")
	}
	rec, found, err := readLocation(ctx, r.rdb, locKey(playerID))
	if err != nil {
		return LocationRecord{}, false, errcode.New(errcode.ErrInternal, "redis location get: %v", err)
	}
	return rec, found, nil
}

// BatchGet 用 Redis pipeline 一次往返批量 HGETALL 多个玩家位置。
//
// HGETALL 对不存在的 key 返回空 map(不是 redis.Nil),故 pipeline Exec 不会因 miss 报错;
// 单个命令失败按缺席跳过(map 里没有 → 调用方判离线),不让整批失败。
// playerID==0 跳过;重复 id 经 cmds map 天然去重。
func (r *RedisLocationRepo) BatchGet(ctx context.Context, playerIDs []uint64) (map[uint64]LocationRecord, error) {
	out := make(map[uint64]LocationRecord, len(playerIDs))
	if len(playerIDs) == 0 {
		return out, nil
	}
	pipe := r.rdb.Pipeline()
	cmds := make(map[uint64]*redis.MapStringStringCmd, len(playerIDs))
	for _, pid := range playerIDs {
		if pid == 0 {
			continue
		}
		if _, dup := cmds[pid]; dup {
			continue
		}
		cmds[pid] = pipe.HGetAll(ctx, locKey(pid))
	}
	if len(cmds) == 0 {
		return out, nil
	}
	// Exec 在任一命令出错时返回该错误;HGETALL 不会产生 redis.Nil。
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, errcode.New(errcode.ErrInternal, "redis location batch get: %v", err)
	}
	for pid, cmd := range cmds {
		m, err := cmd.Result()
		if err != nil || len(m) == 0 {
			continue // 单命令失败 / key 不存在 → 缺席判离线
		}
		out[pid] = parseLocationMap(m)
	}
	return out, nil
}

// RefreshHubLocations 批量续期 HUB 位置 TTL。
//
// 两轮 pipeline:
//  1. HMGET state,hub_pod 批量读(一次往返)
//  2. 对「state==HUB 且 hub_pod 匹配」的 key 批量 EXPIRE(一次往返)
//
// 非事务:步骤 1→2 之间状态若被并发写成 MATCHING/BATTLE,EXPIRE 只多续一次
// 30s TTL(无害:对局态由战斗链路持续刷新,且下次写会重置 TTL),不值得上 WATCH。
// 单 key miss / 解析失败直接跳过(玩家刚离线属正常路径),不让整批失败。
func (r *RedisLocationRepo) RefreshHubLocations(ctx context.Context, hubPod string, playerIDs []uint64, ttl time.Duration) (int, error) {
	if hubPod == "" || len(playerIDs) == 0 {
		return 0, nil
	}
	readPipe := r.rdb.Pipeline()
	cmds := make(map[uint64]*redis.SliceCmd, len(playerIDs))
	for _, pid := range playerIDs {
		if pid == 0 {
			continue
		}
		if _, dup := cmds[pid]; dup {
			continue
		}
		cmds[pid] = readPipe.HMGet(ctx, locKey(pid), "state", "hub_pod")
	}
	if len(cmds) == 0 {
		return 0, nil
	}
	if _, err := readPipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return 0, errcode.New(errcode.ErrInternal, "redis hub refresh read: %v", err)
	}

	expirePipe := r.rdb.Pipeline()
	refreshed := 0
	for pid, cmd := range cmds {
		vals, err := cmd.Result()
		if err != nil || len(vals) != 2 {
			continue
		}
		stateStr, ok1 := vals[0].(string)
		podStr, ok2 := vals[1].(string)
		if !ok1 || !ok2 {
			continue // key 不存在(HMGET 回 nil)/ 字段缺失 → 跳过
		}
		state, err := strconv.ParseInt(stateStr, 10, 32)
		if err != nil || int32(state) != 3 /* LOCATION_STATE_HUB */ || podStr != hubPod {
			continue // 非 HUB 态 / 别的 pod 的记录不动(不变量 §1)
		}
		expirePipe.Expire(ctx, locKey(pid), ttl)
		refreshed++
	}
	if refreshed == 0 {
		return 0, nil
	}
	if _, err := expirePipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return 0, errcode.New(errcode.ErrInternal, "redis hub refresh expire: %v", err)
	}
	return refreshed, nil
}

// shrinkHubTTLScript 原子完成「守卫校验 + 缩 TTL」(单 key,Lua 内无并发写插入的窗口):
// state==HUB('3') 且 hub_pod 匹配才 PEXPIRE LT;否则返 0 不动。
// 若非原子(先 HMGET 再 EXPIRE),窗口内状态被并发写成 MATCHING/BATTLE 会误缩新状态
// 的 TTL 到 grace,与「不误伤对局态」的设计目标冲突(Codex 复审 2026-07-06)。
var shrinkHubTTLScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'state') ~= '3' then return 0 end
if redis.call('HGET', KEYS[1], 'hub_pod') ~= ARGV[1] then return 0 end
return redis.call('PEXPIRE', KEYS[1], ARGV[2], 'LT')`)

// ShrinkHubTTL 快速断线上报:守卫通过后把剩余 TTL 缩到 grace。
//
// PEXPIRE LT 语义(Redis 7):仅当新 TTL 小于当前剩余 TTL 才生效——只缩不涨,
// 重复上报/迟到报文天然幂等。守卫失败(非 HUB / pod 不匹配 / key 已过期)返
// (false, nil):玩家 travel 去战斗、切线后旧 pod 迟到报文等均属正常路径,不是错误。
func (r *RedisLocationRepo) ShrinkHubTTL(ctx context.Context, hubPod string, playerID uint64, grace time.Duration) (bool, error) {
	if hubPod == "" || playerID == 0 {
		return false, errcode.New(errcode.ErrInvalidArg, "hub_pod and player_id required")
	}
	shrunk, err := shrinkHubTTLScript.Run(ctx, r.rdb,
		[]string{locKey(playerID)},
		hubPod, grace.Milliseconds()).Int()
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "redis shrink hub ttl: %v", err)
	}
	return shrunk == 1, nil
}

// SetLastSeen 写 last-seen 时刻(SET + PX,覆盖式)。
//
// 覆盖而非「只增不减」:同一玩家的多次离开,最后一次才是有效的「离开多久」基线。
// 重复 / 迟到的 ReportDisconnect 只会把时刻刷新到更晚,后果是**晚一点**才判定超时,
// 偏保守的方向(宁可多留他一会儿,也不要把在线的人误算成离线很久)。
func (r *RedisLocationRepo) SetLastSeen(ctx context.Context, playerID uint64, atMs int64, retention time.Duration) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "playerID must > 0")
	}
	if retention <= 0 {
		return errcode.New(errcode.ErrInvalidArg, "retention must > 0")
	}
	if err := r.rdb.Set(ctx, lastSeenKey(playerID), strconv.FormatInt(atMs, 10), retention).Err(); err != nil {
		return errcode.New(errcode.ErrInternal, "redis set last seen: %v", err)
	}
	return nil
}

// ClearLastSeen UNLINK(异步删)。key 不存在不是错误(玩家本来就没离开过)。
func (r *RedisLocationRepo) ClearLastSeen(ctx context.Context, playerID uint64) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "playerID must > 0")
	}
	if err := r.rdb.Unlink(ctx, lastSeenKey(playerID)).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return errcode.New(errcode.ErrInternal, "redis clear last seen: %v", err)
	}
	return nil
}

// BatchGetLastSeen 用 pipeline 一次往返批量读。
//
// 单 key miss / 解析失败一律跳过(缺席 = UNKNOWN),不让整批失败:调用方对 UNKNOWN
// 的处理必须是「不动作」,所以把坏值当缺席比返回一个可能被误用的 0 更安全。
func (r *RedisLocationRepo) BatchGetLastSeen(ctx context.Context, playerIDs []uint64) (map[uint64]int64, error) {
	out := make(map[uint64]int64, len(playerIDs))
	if len(playerIDs) == 0 {
		return out, nil
	}
	pipe := r.rdb.Pipeline()
	cmds := make(map[uint64]*redis.StringCmd, len(playerIDs))
	for _, pid := range playerIDs {
		if pid == 0 {
			continue
		}
		if _, dup := cmds[pid]; dup {
			continue
		}
		cmds[pid] = pipe.Get(ctx, lastSeenKey(pid))
	}
	if len(cmds) == 0 {
		return out, nil
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, errcode.New(errcode.ErrInternal, "redis batch get last seen: %v", err)
	}
	for pid, cmd := range cmds {
		v, err := cmd.Result()
		if err != nil {
			continue // redis.Nil(无记录)/ 单命令失败 → 缺席判 UNKNOWN
		}
		ms, perr := strconv.ParseInt(v, 10, 64)
		if perr != nil {
			continue // 坏值当缺席,不喂给调用方
		}
		out[pid] = ms
	}
	return out, nil
}

// readLocation HGETALL 并解析为 LocationRecord。c 可以是 *redis.Client 或 WATCH 内的 *redis.Tx。
func readLocation(ctx context.Context, c redis.Cmdable, key string) (LocationRecord, bool, error) {
	m, err := c.HGetAll(ctx, key).Result()
	if err != nil {
		return LocationRecord{}, false, err
	}
	if len(m) == 0 {
		return LocationRecord{}, false, nil
	}
	return parseLocationMap(m), true, nil
}

// parseLocationMap 把 redis hash 字段解析成 LocationRecord(容错:解析失败的字段留零值)。
func parseLocationMap(m map[string]string) LocationRecord {
	rec := LocationRecord{
		HubPod:    m["hub_pod"],
		BattlePod: m["battle_pod"],
	}
	if v, ok := m["state"]; ok {
		if x, e := strconv.ParseInt(v, 10, 32); e == nil {
			rec.State = int32(x)
		}
	}
	if v, ok := m["shard_id"]; ok {
		if x, e := strconv.ParseUint(v, 10, 32); e == nil {
			rec.ShardID = uint32(x)
		}
	}
	if v, ok := m["match_id"]; ok {
		if x, e := strconv.ParseUint(v, 10, 64); e == nil {
			rec.MatchID = x
		}
	}
	if v, ok := m["updated_at_ms"]; ok {
		if x, e := strconv.ParseInt(v, 10, 64); e == nil {
			rec.UpdatedAtMs = x
		}
	}
	return rec
}

// Delete UNLINK(异步删,避免大 key 阻塞);TTL 已经在 set 时挂了,Delete 失败不致命。
func (r *RedisLocationRepo) Delete(ctx context.Context, playerID uint64) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "playerID must > 0")
	}
	if err := r.rdb.Unlink(ctx, locKey(playerID)).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return errcode.New(errcode.ErrInternal, "redis location del: %v", err)
	}
	return nil
}
