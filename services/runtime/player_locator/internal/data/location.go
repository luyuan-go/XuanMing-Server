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
	State            int32
	HubPod           string
	ShardID          uint32
	MatchID          uint64
	BattlePod        string
	UpdatedAtMs      int64
	HubPresenceFence HubPresenceFence
}

// HubPresenceFence 是一次 Hub 物理连接的精确 identity —— 与 Hub DS 在
// SetLocation / ReportDisconnect 里携带的 proto 字段 1:1,不多不少。
//
// 职责边界(CLAUDE.md §9.22):locator 是 **presence 投影**,不是 owner 权威。
// 因此这里只解决投影自己的问题:「同一个 Pod 上,哪条物理连接是当前这条」——
// 玩家断线秒重连落回同一 assignment 时,旧连接的迟到 Logout 不能误伤新连接。
// 这在 **同 assignment 内** 用 admission_seq 单调 + admission_id 防同序 ABA 即可判定。
//
// **跨 assignment 的归属顺序刻意不在这里判**:玩家该属于哪台 Hub 是 hub_allocator 的
// assignment / placement 权威决定的。一个新 assignment 能拿着有效 DS 令牌调进来,
// 就说明权威已经签发过它,locator 没有立场推翻;真要在这里反向定序,就得实时查
// owner 服务,等于让「玩家进大厅写位置」强依赖另一个服务(它不可用时 fail-closed,
// 玩家进不去大厅)—— 用一条关键路径的可用性,换一个本就不归自己管的判定。
type HubPresenceFence struct {
	AssignmentID string
	AdmissionID  string
	AdmissionSeq uint64
}

// IsZero 表示滚动升级中的旧 Hub DS 没有携带任何连接级 fence。
func (f HubPresenceFence) IsZero() bool {
	return f.AssignmentID == "" && f.AdmissionID == "" && f.AdmissionSeq == 0
}

// IsComplete 表示三个 fence 字段全部存在。
func (f HubPresenceFence) IsComplete() bool {
	return f.AssignmentID != "" && f.AdmissionID != "" && f.AdmissionSeq > 0
}

// Equal 做 exact identity 比较。
func (f HubPresenceFence) Equal(other HubPresenceFence) bool {
	return f.AssignmentID == other.AssignmentID &&
		f.AdmissionID == other.AdmissionID &&
		f.AdmissionSeq == other.AdmissionSeq
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
	RefreshHubLocations(ctx context.Context, hubPod string, playerIDs []uint64, ttl, metaTTL time.Duration) (int, error)
	// ValidateHubPresence 只读校验长 TTL meta：false 表示同 assignment 内的旧 admission、
	// 同序号 ABA、已离开的 admission 重放，或 fenced 当前代下的 legacy 降级写。
	// 它不改变 meta，业务状态守卫拒绝时不得留下半步副作用。
	ValidateHubPresence(ctx context.Context, playerID uint64, fence HubPresenceFence) (bool, error)
	// ActivateHubPresence 必须在 location CAS 写成功后调用。它重复同一套单 key 校验并
	// commit 当前 fence、清除旧 last-seen；并发更新若已推进到更新代则返回 false。
	// legacy(全零 fence)只在尚无 fenced 当前代时接受，用于滚动升级安全降级。
	ActivateHubPresence(ctx context.Context, playerID uint64, fence HubPresenceFence, retention time.Duration) (bool, error)
	// ShrinkHubTTL 快速断线上报:把玩家 HUB 位置的剩余 TTL 缩短到 grace(只缩不涨,
	// PEXPIRE LT)。守卫要求「state==HUB、hub_pod 与 exact fence 全匹配」；
	// accepted 区分身份匹配但 TTL 已更短的幂等重试，shrunk 只表示本次实际缩短。
	// Lua 原子(单 key):校验与缩 TTL 同脚本执行,不存在「读到旧 HUB → 并发写成
	// MATCHING/BATTLE → 误缩新状态 TTL」的窗口(Codex 复审 2026-07-06)。
	ShrinkHubTTL(ctx context.Context, hubPod string, playerID uint64, fence HubPresenceFence, grace time.Duration) (accepted, shrunk bool, err error)
	// RecordLastSeen 仅在 meta 当前 fence exact 匹配时记录首次离开时刻；重复请求返回
	// 原时刻而不后移。meta 若尚不存在，可由已经通过 location exact 守卫的调用重建。
	RecordLastSeen(ctx context.Context, playerID uint64, fence HubPresenceFence, atMs int64, retention time.Duration) (recorded bool, effectiveAtMs int64, err error)
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

// lastSeenKey 是旧协议留下的 string key。新协议只在 hubMetaKey 完全不存在时
// 兼容读取；一旦新 locator 见过一次 HUB Set，meta marker 就会屏蔽这份旧时刻。
func lastSeenKey(playerID uint64) string {
	return fmt.Sprintf("pandora:locator:lastseen:%d", playerID)
}

// hubMetaKey 保存当前 Hub 连接 fence 与可选 left_at_ms。它必须独立于带 presence TTL
// 的 location key，才能在 location 过期后继续回答离线时长；同时所有代际推进 / 清理 /
// 记录离开都只在本单 key Lua 内完成，避免 Redis Cluster 的 CROSSSLOT。
func hubMetaKey(playerID uint64) string {
	return fmt.Sprintf("pandora:locator:hubmeta:%d", playerID)
}

// hubPresenceScript 以同一套规则完成 validate/commit：
//   - 同 assignment 内按 admission_seq 排序，admission_id 防同序 ABA；
//   - 跨 assignment 一律接受(归属是 hub_allocator 的权威，本投影不反向定序，见
//     HubPresenceFence 注释)；
//   - 已记录 left_at 的 exact admission 不得被迟到 Set 复活。
//
// action=validate 只读，action=commit 才推进 meta 并清理旧 left_at_ms。调用方严格按
// validate → location CAS → commit 排序，既能用长 TTL meta 挡 location 过期后的旧写，
// 又不会在 MATCHING/BATTLE guard 拒绝时污染 meta。
var hubPresenceScript = redis.NewScript(`
local function compare_uint_decimal(left, right)
  left = string.gsub(left or '0', '^0+', '')
  right = string.gsub(right or '0', '^0+', '')
  if left == '' then left = '0' end
  if right == '' then right = '0' end
  if string.len(left) < string.len(right) then return -1 end
  if string.len(left) > string.len(right) then return 1 end
  if left < right then return -1 end
  if left > right then return 1 end
  return 0
end

local action = ARGV[1]
local incoming_mode = ARGV[2]
local current_mode = redis.call('HGET', KEYS[1], 'mode')
if action ~= 'validate' and action ~= 'commit' then return 0 end
if not current_mode and redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
if current_mode and current_mode ~= 'legacy' and current_mode ~= 'fenced' then return 0 end

if incoming_mode == 'legacy' then
  if current_mode == 'fenced' then return 0 end
  if action == 'validate' then return 1 end
  redis.call('HSET', KEYS[1], 'mode', 'legacy')
  redis.call('HDEL', KEYS[1], 'assignment_id', 'admission_id', 'admission_seq', 'left_at_ms', 'last_alive_ms')
  redis.call('PEXPIRE', KEYS[1], ARGV[6])
  return 1
end

if current_mode == 'fenced' then
  -- 只在**同 assignment 内**定序:那正是「同一个 Pod 上哪条连接是当前这条」的问题,
  -- 也正是本投影该管的范围。跨 assignment 一律接受 —— 玩家归属由 hub_allocator 的
  -- assignment / placement 权威决定,locator 不反向充当 owner authority(§9.22)。
  if (redis.call('HGET', KEYS[1], 'assignment_id') or '') == ARGV[3] then
    local seq_cmp = compare_uint_decimal(redis.call('HGET', KEYS[1], 'admission_seq'), ARGV[5])
    if seq_cmp > 0 then return 0 end
    if seq_cmp == 0 and (redis.call('HGET', KEYS[1], 'admission_id') or '') ~= ARGV[4] then return 0 end
    -- 同一 admission 已经离开后，迟到/重放的 SetLocation 不能清掉 left_at 复活。
    if seq_cmp == 0 and redis.call('HEXISTS', KEYS[1], 'left_at_ms') == 1 then return 0 end
  end
end

if action == 'validate' then return 1 end
redis.call('HSET', KEYS[1],
  'mode', 'fenced',
  'assignment_id', ARGV[3],
  'admission_id', ARGV[4],
  'admission_seq', ARGV[5])
redis.call('HDEL', KEYS[1], 'left_at_ms')
redis.call('PEXPIRE', KEYS[1], ARGV[6])
return 1`)

// ValidateHubPresence 只读校验，不改变 meta。
func (r *RedisLocationRepo) ValidateHubPresence(ctx context.Context, playerID uint64, fence HubPresenceFence) (bool, error) {
	if playerID == 0 {
		return false, errcode.New(errcode.ErrInvalidArg, "playerID must be valid")
	}
	if !fence.IsZero() && !fence.IsComplete() {
		return false, errcode.New(errcode.ErrInvalidArg, "hub presence fence must be complete or empty")
	}
	mode := "fenced"
	if fence.IsZero() {
		mode = "legacy"
	}
	accepted, err := hubPresenceScript.Run(ctx, r.rdb,
		[]string{hubMetaKey(playerID)},
		"validate", mode, fence.AssignmentID, fence.AdmissionID, fence.AdmissionSeq, 0).Int()
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "redis validate hub presence: %v", err)
	}
	return accepted == 1, nil
}

// ActivateHubPresence 在 location CAS 写成功后 commit meta。
func (r *RedisLocationRepo) ActivateHubPresence(ctx context.Context, playerID uint64, fence HubPresenceFence, retention time.Duration) (bool, error) {
	if playerID == 0 || retention <= 0 {
		return false, errcode.New(errcode.ErrInvalidArg, "playerID and retention must be valid")
	}
	if !fence.IsZero() && !fence.IsComplete() {
		return false, errcode.New(errcode.ErrInvalidArg, "hub presence fence must be complete or empty")
	}
	mode := "fenced"
	if fence.IsZero() {
		mode = "legacy"
	}
	accepted, err := hubPresenceScript.Run(ctx, r.rdb,
		[]string{hubMetaKey(playerID)},
		"commit", mode, fence.AssignmentID, fence.AdmissionID, fence.AdmissionSeq, retention.Milliseconds()).Int()
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "redis activate hub presence: %v", err)
	}
	return accepted == 1, nil
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
					"hub_assignment_id", rec.HubPresenceFence.AssignmentID,
					"hub_admission_id", rec.HubPresenceFence.AdmissionID,
					"hub_admission_seq", rec.HubPresenceFence.AdmissionSeq,
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
func (r *RedisLocationRepo) RefreshHubLocations(ctx context.Context, hubPod string, playerIDs []uint64, ttl, metaTTL time.Duration) (int, error) {
	if hubPod == "" || len(playerIDs) == 0 {
		return 0, nil
	}
	nowMs := time.Now().UnixMilli()
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
		// Hub meta 承载连接 fence；在线数小时也不能让它先于 location 丢失，否则
		// 后续 exact Disconnect 只能退化为普通 TTL。
		//
		// 顺带把 last_alive_ms 推到本次心跳时刻 —— 这是「Hub DS 整台崩溃」唯一能留下的
		// 离开时间线索:那种情况下不会有任何 ReportDisconnect，写不出 left_at_ms，
		// 于是消费方只能查到 UNKNOWN、永远不动作(离线成员一直挂在队伍里)。
		// 有了它，最后一次心跳时刻就是「最后一次被观测在线」，精度 ±一个心跳周期(5s)，
		// 对 180s 级别的阈值绰绰有余。
		if metaTTL > 0 {
			// 用 Eval(全文)而不是 Run:Run 在 pipeline 里只发 EVALSHA，脚本没被本连接
			// 加载过就整批 NOSCRIPT 失败，而 pipeline 内拿不到单命令的自动 fallback。
			// 脚本仅 ~120 字节，相对一次心跳的 player_ids 可忽略。
			touchHubAliveScript.Eval(ctx, expirePipe,
				[]string{hubMetaKey(pid)}, nowMs, metaTTL.Milliseconds())
		}
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

// touchHubAliveScript 在心跳续期时把 meta 的 last_alive_ms 推到当前时刻。
//
// **必须只在 meta 已存在时写**:HSET 会凭空建 key，而一个「有内容但没有 mode 字段」的
// meta 会被 hubPresenceScript 判为损坏数据并 fail-closed(那条判定是对的，不能放松)，
// 结果就是给 legacy 玩家造出一个永远无法接受 HUB 写的毒 key。EXISTS 守卫挡住这一点。
//
// 同理只续期不新建:key 不存在说明这个玩家从没走过带 fence 的路径，没有 meta 可维护。
var touchHubAliveScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) ~= 1 then return 0 end
redis.call('HSET', KEYS[1], 'last_alive_ms', ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1`)

// shrinkHubTTLScript 原子完成「守卫校验 + 缩 TTL」(单 key,Lua 内无并发写插入的窗口):
// state==HUB('3')、hub_pod 与 exact connection fence 全匹配才 PEXPIRE LT。
// 返回 0=身份不匹配，1=实际缩短，2=身份匹配但 TTL 已经更短（幂等接受）。
// 若非原子(先 HMGET 再 EXPIRE),窗口内状态被并发写成 MATCHING/BATTLE 会误缩新状态
// 的 TTL 到 grace,与「不误伤对局态」的设计目标冲突(Codex 复审 2026-07-06)。
var shrinkHubTTLScript = redis.NewScript(`
if redis.call('HGET', KEYS[1], 'state') ~= '3' then return 0 end
if redis.call('HGET', KEYS[1], 'hub_pod') ~= ARGV[1] then return 0 end
if redis.call('HGET', KEYS[1], 'hub_assignment_id') ~= ARGV[2] then return 0 end
if redis.call('HGET', KEYS[1], 'hub_admission_id') ~= ARGV[3] then return 0 end
if redis.call('HGET', KEYS[1], 'hub_admission_seq') ~= ARGV[4] then return 0 end
local changed = redis.call('PEXPIRE', KEYS[1], ARGV[5], 'LT')
if changed == 1 then return 1 end
return 2`)

// ShrinkHubTTL 快速断线上报:守卫通过后把剩余 TTL 缩到 grace。
//
// PEXPIRE LT 语义(Redis 7):仅当新 TTL 小于当前剩余 TTL 才生效——只缩不涨,
// 重复上报天然幂等。守卫失败(非 HUB / pod 或连接 fence 不匹配 / key 已过期)返
// accepted=false；身份匹配但 TTL 已更短返 accepted=true、shrunk=false，仍允许补写 meta。
func (r *RedisLocationRepo) ShrinkHubTTL(ctx context.Context, hubPod string, playerID uint64, fence HubPresenceFence, grace time.Duration) (bool, bool, error) {
	if hubPod == "" || playerID == 0 || !fence.IsComplete() {
		return false, false, errcode.New(errcode.ErrInvalidArg, "hub_pod, player_id and complete fence required")
	}
	result, err := shrinkHubTTLScript.Run(ctx, r.rdb,
		[]string{locKey(playerID)},
		hubPod, fence.AssignmentID, fence.AdmissionID, fence.AdmissionSeq, grace.Milliseconds()).Int()
	if err != nil {
		return false, false, errcode.New(errcode.ErrInternal, "redis shrink hub ttl: %v", err)
	}
	return result == 1 || result == 2, result == 1, nil
}

// recordLastSeenScript 只给当前 exact fence 写首次离开时刻。location 守卫已经通过而
// meta 恰好丢失时允许重建；若重连已推进 meta，则旧请求严格返回 0。
var recordLastSeenScript = redis.NewScript(`
local current_mode = redis.call('HGET', KEYS[1], 'mode')
if not current_mode and redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
if current_mode then
  if current_mode ~= 'fenced' then return 0 end
  if redis.call('HGET', KEYS[1], 'assignment_id') ~= ARGV[1] then return 0 end
  if redis.call('HGET', KEYS[1], 'admission_id') ~= ARGV[2] then return 0 end
  if redis.call('HGET', KEYS[1], 'admission_seq') ~= ARGV[3] then return 0 end
else
  redis.call('HSET', KEYS[1],
    'mode', 'fenced',
    'assignment_id', ARGV[1],
    'admission_id', ARGV[2],
    'admission_seq', ARGV[3])
end
local existing = redis.call('HGET', KEYS[1], 'left_at_ms')
if existing then
  redis.call('PEXPIRE', KEYS[1], ARGV[5])
  return tonumber(existing) or 0
end
redis.call('HSET', KEYS[1], 'left_at_ms', ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return tonumber(ARGV[4])`)

// RecordLastSeen 精确记录该连接首次离开时刻；重复调用返回第一次的时刻。
func (r *RedisLocationRepo) RecordLastSeen(ctx context.Context, playerID uint64, fence HubPresenceFence, atMs int64, retention time.Duration) (bool, int64, error) {
	if playerID == 0 || !fence.IsComplete() || atMs <= 0 || retention <= 0 {
		return false, 0, errcode.New(errcode.ErrInvalidArg, "valid player, fence, timestamp and retention required")
	}
	effectiveAtMs, err := recordLastSeenScript.Run(ctx, r.rdb,
		[]string{hubMetaKey(playerID)},
		fence.AssignmentID, fence.AdmissionID, fence.AdmissionSeq, atMs, retention.Milliseconds()).Int64()
	if err != nil {
		return false, 0, errcode.New(errcode.ErrInternal, "redis record last seen: %v", err)
	}
	return effectiveAtMs > 0, effectiveAtMs, nil
}

// BatchGetLastSeen 用 pipeline 同时读新 meta 与旧 string key。meta 只要存在就拥有优先级：
// 没有 left_at_ms 明确表示当前连接未离开，绝不能再回退到重连前的旧 string 时刻。
// 只有 meta 完全不存在时才读旧 key，供滚动升级窗口兼容。
func (r *RedisLocationRepo) BatchGetLastSeen(ctx context.Context, playerIDs []uint64) (map[uint64]int64, error) {
	out := make(map[uint64]int64, len(playerIDs))
	if len(playerIDs) == 0 {
		return out, nil
	}
	pipe := r.rdb.Pipeline()
	metaCmds := make(map[uint64]*redis.MapStringStringCmd, len(playerIDs))
	legacyCmds := make(map[uint64]*redis.StringCmd, len(playerIDs))
	for _, pid := range playerIDs {
		if pid == 0 {
			continue
		}
		if _, dup := metaCmds[pid]; dup {
			continue
		}
		metaCmds[pid] = pipe.HGetAll(ctx, hubMetaKey(pid))
		legacyCmds[pid] = pipe.Get(ctx, lastSeenKey(pid))
	}
	if len(metaCmds) == 0 {
		return out, nil
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, errcode.New(errcode.ErrInternal, "redis batch get last seen: %v", err)
	}
	for pid, metaCmd := range metaCmds {
		meta, metaErr := metaCmd.Result()
		if metaErr == nil && len(meta) > 0 {
			if meta["mode"] == "fenced" {
				// 两级来源,精确的优先:
				//  ① left_at_ms —— Hub DS 显式上报的离开时刻(玩家正常退出 / 连接超时);
				//  ② last_alive_ms —— 最后一次心跳把该玩家报为在场的时刻。
				//
				// ② 存在的意义是 **Hub DS 整台崩溃**:那种情况下没有任何 ReportDisconnect,
				// 写不出 ①,此前只能返回 UNKNOWN → 消费方一律不动作 → 那批玩家永远挂在
				// 队伍里。用「最后一次被观测在线」当离开时刻,误差 ≤ 一个心跳周期(5s)。
				//
				// 玩家**在线时** ② 也一直在被刷新,看起来像「刚离开 5 秒」——这不会误伤:
				// 调用方(pkg/offlinewatch.classify)永远先判「此刻是否在线」,在线直接放行,
				// 只有确认查不到位置时才会用到这里的时刻。
				v, ok := meta["left_at_ms"]
				if !ok {
					v, ok = meta["last_alive_ms"]
				}
				if !ok {
					continue
				}
				if ms, perr := strconv.ParseInt(v, 10, 64); perr == nil && ms > 0 {
					out[pid] = ms
				}
			}
			continue // meta marker 存在即禁止读旧 key
		}
		v, err := legacyCmds[pid].Result()
		if err != nil {
			continue // redis.Nil(无记录)/ 单命令失败 → 缺席判 UNKNOWN
		}
		ms, perr := strconv.ParseInt(v, 10, 64)
		if perr != nil || ms <= 0 {
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
		HubPresenceFence: HubPresenceFence{
			AssignmentID: m["hub_assignment_id"],
			AdmissionID:  m["hub_admission_id"],
		},
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
	if v, ok := m["hub_admission_seq"]; ok {
		if x, e := strconv.ParseUint(v, 10, 64); e == nil {
			rec.HubPresenceFence.AdmissionSeq = x
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
