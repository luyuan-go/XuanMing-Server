// Package data 是 team 服务的数据层。
//
// Redis key 模板(所有业务 ID 用 uint64,%d 格式化):
//
//	pandora:team:{%d}        → protobuf bytes(TeamStorageRecord)
//	                           hashtag {} 确保同 team 的所有 key 落同一 redis cluster slot(兜底)
//	pandora:team:player:%d   → string(team_id,uint64),TTL 跟随队伍生命周期
//	pandora:team:invite:%d   → hash(team_id/target_player_id/inviter_id/expires_at_ms),TTL=InviteTTL(60s)
//	pandora:team:invite:target:%d → zset(member=invite_id,score=expires_at_ms),被邀请人维度的
//	                           pending 邀请索引:写入侧限流(不变量 §9-18)+ 拉取兜底查询。
//	                           TTL=InviteTTL(每次写入刷新);过期成员由 score 惰性清理。
//	pandora:team:apply:{%d}  → zset(member=applicant_id,score=expires_at_ms),队伍维度的
//	                           pending 入队申请索引。hashtag 与 pandora:team:{%d} 同源,
//	                           保证申请与队伍主体落同一 cluster slot。写入侧限流
//	                           (不变量 §9-18,上限 max_applications_per_team)+ 队长拉取查询。
//	                           TTL=ApplyTTL(每次写入刷新);过期成员由 score 惰性清理。
//	pandora:team:open:all    → zset(member=team_id,score=索引项过期时刻 ms),开放招募队伍索引
//	pandora:team:open:map:%d → zset(同上),按目标关卡分桶,供 ListOpenTeams 的 map_id 筛选
//
// ⚠️ open 索引是**非权威投影**(不变量 §9.22):唯一权威永远是 pandora:team:{id} 记录本身。
// 索引只用来"找候选",biz 逐条回权威记录复核后才返回给客户端;索引脏了只会少返几条或多读
// 几次,永远不会返回一支实际已满/已解散/已开打的队伍。因此索引写失败一律 best-effort 告警,
// 不回滚已提交的队伍状态机迁移。悬挂成员由两道机制收敛:①score = 队伍 key 的最晚存活时刻,
// ZREMRANGEBYSCORE 惰性清掉;②复核不通过时顺手 ZREM。
//
// 状态机写用 WATCH/MULTI/EXEC 乐观锁:
//
//	GET(proto bytes) → fn(modify) → MULTI/SET/EXEC
//	EXEC 返回 nil(key 被并发修改) → 重试至 maxRetry 次 → 返 ErrTeamConcurrent(3007)
//
// 队伍主体序列化为 protobuf bytes 存入 Redis value。
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
	plog "github.com/luyuancpp/pandora/pkg/log"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// ── 常量 ─────────────────────────────────────────────────────────────────────

const (
	// fieldTeamID / fieldTargetPlayerID / fieldInviterID / fieldExpiresAtMs — invite hash 字段
	fieldTeamID         = "team_id"
	fieldTargetPlayerID = "target_player_id"
	fieldInviterID      = "inviter_id"
	fieldExpiresAtMs    = "expires_at_ms"
)

// teamKey returns "pandora:team:{teamID}" — hashtag 括住 teamID 保 cluster slot 一致性。
func teamKey(teamID uint64) string {
	return fmt.Sprintf("pandora:team:{%d}", teamID)
}

// playerKey returns "pandora:team:player:playerID".
func playerKey(playerID uint64) string {
	return fmt.Sprintf("pandora:team:player:%d", playerID)
}

// inviteKey returns "pandora:team:invite:inviteID".
func inviteKey(inviteID uint64) string {
	return fmt.Sprintf("pandora:team:invite:%d", inviteID)
}

// inviteTargetKey returns "pandora:team:invite:target:playerID" — 被邀请人维度的
// pending 邀请索引(zset,member=invite_id,score=expires_at_ms)。
func inviteTargetKey(playerID uint64) string {
	return fmt.Sprintf("pandora:team:invite:target:%d", playerID)
}

// applyKey returns "pandora:team:apply:{teamID}" — 队伍维度的 pending 入队申请索引
// (zset,member=applicant_id,score=expires_at_ms)。
// hashtag 内容与 teamKey 一致(都是 teamID),保证申请索引与队伍主体同 cluster slot。
func applyKey(teamID uint64) string {
	return fmt.Sprintf("pandora:team:apply:{%d}", teamID)
}

// openAllKey 是"全部开放招募队伍"索引(ListOpenTeams 的 map_id=0 路径)。
const openAllKey = "pandora:team:open:all"

// openMapKey returns "pandora:team:open:map:mapID" — 按目标关卡分桶的开放队伍索引
// (ListOpenTeams 的 map_id>0 路径)。mapID=0 也有自己的桶(队长未选图的队伍)。
func openMapKey(mapID uint32) string {
	return fmt.Sprintf("pandora:team:open:map:%d", mapID)
}

// ── 数据模型 ──────────────────────────────────────────────────────────────────
//
// 队伍主体直接使用 proto 存储类型 teamv1.TeamStorageRecord /
// teamv1.TeamMemberStorageRecord，不再起本地别名，保证存储结构全局只有一个权威
// 命名（CLAUDE.md §5.10：存储字段命名以 <Domain>StorageRecord 为准）。

// InviteRecord 是邀请令牌的内存表示，对应 Redis hash pandora:team:invite:{inviteID}。
// 邀请是短 TTL 小令牌，按 CLAUDE.md §5.9 保留 hash 不升级为 proto bytes，
// 因此用本地 struct（它不是 proto 存储记录，不叫 StorageRecord）。
type InviteRecord struct {
	InviteID       uint64
	TeamID         uint64
	TargetPlayerID uint64
	// InviterID / ExpiresAtMs 供 ListPendingInvites 拉取兜底展示。
	// 历史记录(旧版本写入、无这两个字段)解析为 0,不报错(记录只活 60s,自然换代)。
	InviterID   uint64
	ExpiresAtMs int64
}

// ApplicationRecord 是一份 pending 入队申请的内存表示,对应 zset
// pandora:team:apply:{teamID} 里的一个成员(member=PlayerID,score=ExpiresAtMs)。
//
// 为什么不像邀请那样再配一个 hash:申请只需要"谁、到期时刻"两项,zset 成员本身就装得下,
// 多一个 hash 只会多一个可漂移的存储(§15.2 能一份就不拆两份)。
type ApplicationRecord struct {
	PlayerID    uint64
	ExpiresAtMs int64
}

// ── 接口 ──────────────────────────────────────────────────────────────────────

// TeamRepo 是 team 数据层抽象。biz 层只依赖此接口,不依赖 redis。
type TeamRepo interface {
	// ScanPlayerIndex 增量扫描「有队伍的玩家」全集,供离线兜底提名(见实现注释)。
	ScanPlayerIndex(ctx context.Context, cursor uint64, count int64) ([]uint64, uint64, error)
	// Get 读取队伍。not found 时返回 false(不报错)。
	Get(ctx context.Context, teamID uint64) (*teamv1.TeamStorageRecord, bool, error)

	// Create 创建队伍：仅写 team protobuf value + TTL=teamTTL。
	// player 归属由上层 ClaimPlayer(SETNX) 独立保证（不变量 §1），不在此处写 player index。
	Create(ctx context.Context, team *teamv1.TeamStorageRecord, teamTTL time.Duration) error

	// UpdateWithLock 使用 WATCH/MULTI/EXEC 读-改-写 team protobuf value。
	//   1. WATCH team key
	//   2. GET → proto 反序列化
	//   3. 调 fn(team) — fn 可返错误，返错则 UNWATCH 并透传
	//   4. MULTI → SET(value+TTL) → EXEC
	//   5. EXEC=nil（CAS 失败）→ 重试，耗尽返 ErrTeamConcurrent(3007)
	UpdateWithLock(ctx context.Context, teamID uint64, maxRetry int, fn func(*teamv1.TeamStorageRecord) error, teamTTL time.Duration) error

	// GetPlayerTeamID 查玩家当前所在队伍 ID。not found 返 (0, false, nil)。
	GetPlayerTeamID(ctx context.Context, playerID uint64) (uint64, bool, error)

	// ClaimPlayer 原子声明 player→teamID 归属(SETNX),保证不变量 §1(一人只能在一个队)。
	// 声明成功返回 (teamID, true, nil);玩家已属其他队伍返回 (existingTeamID, false, nil)。
	ClaimPlayer(ctx context.Context, playerID, teamID uint64, ttl time.Duration) (uint64, bool, error)

	// SetPlayerIndex 设置或覆盖 player→teamID 映射。
	SetPlayerIndex(ctx context.Context, playerID, teamID uint64, ttl time.Duration) error

	// DeletePlayerIndexIfMatches 仅当索引当前值仍指向 teamID 时才删除(原子 CAS)。
	// biz 层所有索引清理路径一律用它:无条件 DEL 存在「读旧索引 → 玩家并发声明新归属
	// → 误删新 claim」的窗口(镜像 matchmaker DeletePlayerIndexIfMatches 的结论)。
	DeletePlayerIndexIfMatches(ctx context.Context, playerID, teamID uint64) error

	// DeleteTeam 删除队伍主体 key。仅供 CreateTeam 声明失败时回滚自己刚写的主体:
	// teamID 是 Snowflake 新发、返回前仅创建者可见,无条件 DEL 安全。
	DeleteTeam(ctx context.Context, teamID uint64) error

	// ExpireTeam 单独刷新 team key 的 TTL(不读改写 value),供解散后改短 TTL 用。
	ExpireTeam(ctx context.Context, teamID uint64, ttl time.Duration) error

	// TouchTeam 同时续期队伍 key 与玩家索引 key 的 TTL(不动 value)。
	// 在线心跳保活:玩家仍在轮询 GetMyTeam → 队伍不因 active_ttl 被误回收;
	// 停止轮询(退出/掉线)后 TTL 自然倒计时,保留僵尸队伍 GC 语义。best-effort。
	TouchTeam(ctx context.Context, teamID, playerID uint64, ttl time.Duration) error

	// SetInvite 存储邀请令牌,TTL=inviteTTL,并把 invite_id 记入被邀请人的 pending 索引。
	// 写入侧上限(不变量 §9-18):同一 targetPlayerID 的未过期 pending 邀请数 ≥ maxPending
	// 时拒绝写入,返回 ErrTeamInvitePendingLimit(3008)。上限校验与占位在同一段 Lua
	// 内原子完成(单 key,cluster 安全),无 TOCTOU。
	SetInvite(ctx context.Context, inviteID, teamID, inviterID, targetPlayerID uint64, ttl time.Duration, maxPending int) error

	// GetInvite 读取邀请令牌。已过期或不存在时返回 (nil, false, nil)。
	GetInvite(ctx context.Context, inviteID uint64) (*InviteRecord, bool, error)

	// DeleteInvite 删除邀请令牌(AcceptInvite 或取消时调用),同时从被邀请人的
	// pending 索引中移除,释放其配额。
	DeleteInvite(ctx context.Context, inviteID, targetPlayerID uint64) error

	// ListPendingInvites 列出发给 targetPlayerID 的未过期 pending 邀请(拉取兜底,
	// 不变量 §9-22:推送只是投影,这里才是唯一权威查询)。按过期时间升序,
	// 至多返回 limit 条(读取侧上限)。顺手惰性清理已过期/已接受的残留索引成员。
	ListPendingInvites(ctx context.Context, targetPlayerID uint64, limit int) ([]*InviteRecord, error)

	// ── 入队申请(找队伍) ────────────────────────────────────────────────────

	// ClaimApplication 写入或刷新一份 pending 入队申请,返回该申请的过期时刻(unix ms)。
	//
	// 写入侧上限(不变量 §9-18):同一队伍未过期 pending 申请数 ≥ maxPending 且
	// applicantID 尚未在列表内时拒绝,返回 ErrTeamApplyPendingLimit(3009)。
	// 清理过期成员 → 校验上限 → 写入/刷新 在同一段 Lua 内原子完成(单 key,cluster 安全),
	// 无 TOCTOU。重复申请同一队伍幂等:成员已存在时只刷新 score,不再占一个新名额。
	ClaimApplication(ctx context.Context, teamID, applicantID uint64, ttl time.Duration, maxPending int) (int64, error)

	// TakeApplication 原子取走一份申请(校验未过期 + 删除),返回是否取到有效申请。
	// 已过期的成员也会被顺手删掉,但返回 false(按"申请已失效"处理)。
	// 队长审批(同意/拒绝)与申请人重复提交之间靠本操作的原子性定序:先取到的那次才算数。
	TakeApplication(ctx context.Context, teamID, applicantID uint64) (bool, error)

	// ListApplications 列出 teamID 的未过期 pending 申请(拉取兜底,权威查询)。
	// 按过期时间升序,至多返回 limit 条(读取侧上限)。顺手惰性清理已过期成员。
	ListApplications(ctx context.Context, teamID uint64, limit int) ([]*ApplicationRecord, error)

	// DeleteApplications 删除整支队伍的申请索引(解散时清理,避免残留占 Redis)。
	// best-effort:失败无碍,索引自带 TTL。
	DeleteApplications(ctx context.Context, teamID uint64) error

	// ── 开放招募队伍索引(非权威投影,见文件头注释) ──────────────────────────

	// SyncOpenTeam 同步队伍在"开放招募"索引中的存在性。
	//   open=true  → 写入 pandora:team:open:all 与 open:map:{mapID},score=expiresAtMs,并刷新索引 key 的 TTL;
	//   open=false → 从上述两个 key 移除。
	// prevMapID != mapID 时(队长换了目标关卡),无论 open 与否都先从 open:map:{prevMapID} 移除。
	// 跨 slot 多 key,不做原子保证:索引是非权威投影,部分失败最坏只是多/少几条候选,
	// 由 score 过期与读取侧复核收敛。
	SyncOpenTeam(ctx context.Context, teamID uint64, mapID, prevMapID uint32, open bool, expiresAtMs int64, ttl time.Duration) error

	// ListOpenTeamIDs 惰性清理过期成员后,按最近活跃(score 降序)返回至多 limit 个候选 team_id。
	// mapID=0 读 open:all(不限关卡);mapID>0 读对应分桶。
	// 返回的只是**候选**,调用方必须逐条回权威队伍记录复核(见文件头注释)。
	ListOpenTeamIDs(ctx context.Context, mapID uint32, limit int) ([]uint64, error)

	// RemoveOpenTeamCandidate 从索引里剔除一个复核不通过的候选(已满/已解散/已开打/已不存在)。
	// best-effort 自愈,失败无碍(score 过期兜底)。
	RemoveOpenTeamCandidate(ctx context.Context, teamID uint64, mapID uint32) error
}

// ── Redis 实现 ────────────────────────────────────────────────────────────────

// RedisTeamRepo 是基于 go-redis/v9 的 TeamRepo 实现。
type RedisTeamRepo struct {
	rdb redis.UniversalClient
}

// NewRedisTeamRepo 构造 RedisTeamRepo。
func NewRedisTeamRepo(rdb redis.UniversalClient) *RedisTeamRepo {
	return &RedisTeamRepo{rdb: rdb}
}

// --- Get ---

func (r *RedisTeamRepo) Get(ctx context.Context, teamID uint64) (*teamv1.TeamStorageRecord, bool, error) {
	b, err := r.rdb.Get(ctx, teamKey(teamID)).Bytes()
	if err == redis.Nil {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	rec, err := unmarshalTeam(teamID, b)
	if err != nil {
		return nil, false, err
	}
	return rec, true, nil
}

// --- Create ---

func (r *RedisTeamRepo) Create(ctx context.Context, team *teamv1.TeamStorageRecord, teamTTL time.Duration) error {
	payload, err := marshalTeam(team)
	if err != nil {
		return err
	}
	key := teamKey(team.TeamId)

	// 仅写 team protobuf value + TTL。player 归属由上层 ClaimPlayer(SETNX) 独立保证(不变量 §1),
	// 不在此处写 player index,避免覆盖已声明的 claim。
	return r.rdb.Set(ctx, key, payload, teamTTL).Err()
}

// --- UpdateWithLock ---

func (r *RedisTeamRepo) UpdateWithLock(
	ctx context.Context,
	teamID uint64,
	maxRetry int,
	fn func(*teamv1.TeamStorageRecord) error,
	teamTTL time.Duration,
) error {
	key := teamKey(teamID)
	started := time.Now()
	conflicts := 0

	for attempt := 0; attempt <= maxRetry; attempt++ {
		var team *teamv1.TeamStorageRecord
		var fnErr error

		// TxPipelined with WATCH
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			// 1. 读取当前 team
			b, err := tx.Get(ctx, key).Bytes()
			if err == redis.Nil {
				return errcode.New(errcode.ErrTeamNotFound, "team %d not found", teamID)
			}
			if err != nil {
				return err
			}
			team, err = unmarshalTeam(teamID, b)
			if err != nil {
				return err
			}

			// 2. 调用 fn 修改 team
			if fnErr = fn(team); fnErr != nil {
				return fnErr
			}

			// 3. MULTI → 写回 → EXEC
			payload, err := marshalTeam(team)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, key, payload, teamTTL)
				return nil
			})
			return err
		}, key)

		if txErr == nil {
			if conflicts > 0 {
				// R4:并发重试后成功是常态(热点队伍),Debug 即可;
				// 但它是"写变慢了"与后面那条耗尽 WARN 之间的唯一中间证据。
				plog.With(ctx).Debugw("msg", "team_update_lock_retried",
					"team_id", teamID, "conflicts", conflicts, "max_retry", maxRetry,
					"elapsed_ms", time.Since(started).Milliseconds())
			}
			return nil
		}
		// fn 自身返回的业务错误 — 不重试,直接透传
		if txErr == fnErr && fnErr != nil {
			return fnErr
		}
		// WATCH 冲突(redis.TxFailedErr) — 重试
		if txErr == redis.TxFailedErr {
			conflicts++
			continue
		}
		// 其他 redis 错误 — 不重试
		// R2:这里透传的是裸 redis 错误(非 errcode),上层只能把它归到
		// store_write_failed 一大类。不在这里留一条带 team_id 的痕,就完全分不出
		// 是连不上 redis、proto 坏了、还是 MULTI 写失败。
		plog.With(ctx).Warnw("msg", "team_update_store_failed",
			"reason", "store_write_failed",
			"team_id", teamID, "attempt", attempt, "conflicts", conflicts,
			"elapsed_ms", time.Since(started).Milliseconds(), "err", txErr)
		return txErr
	}
	// WATCH/MULTI/EXEC 乐观锁重试耗尽(§16 TOCTOU / 重试耗尽盲点):某热点队伍被并发写打爆;
	// 经 in-band 码返回被 access log 记成泛化失败,无 team_id → WARN 留证以识别热点争用。
	// reason 与 biz 层同一套枚举取值(§11.3 R2:同一个失败在全服务只有一个 reason 名);
	// 常量定义在 biz 包,data 不反向依赖 biz,故此处写字面量并在两侧注释里互指。
	plog.With(ctx).Warnw("msg", "team_update_lock_exhausted",
		"reason", "optimistic_retry_exhausted",
		"team_id", teamID, "max_retry", maxRetry, "conflicts", conflicts,
		"elapsed_ms", time.Since(started).Milliseconds(),
		"hint", "热点队伍被并发写打爆;调用方会收到 3007,属暂态")
	return errcode.New(errcode.ErrTeamConcurrent, "team %d update concurrent retry exhausted", teamID)
}

// --- Player index ---

func (r *RedisTeamRepo) GetPlayerTeamID(ctx context.Context, playerID uint64) (uint64, bool, error) {
	val, err := r.rdb.Get(ctx, playerKey(playerID)).Result()
	if err == redis.Nil {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	teamID, err := strconv.ParseUint(val, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return teamID, true, nil
}

func (r *RedisTeamRepo) SetPlayerIndex(ctx context.Context, playerID, teamID uint64, ttl time.Duration) error {
	return r.rdb.Set(ctx, playerKey(playerID), strconv.FormatUint(teamID, 10), ttl).Err()
}

// ClaimPlayer 用 SETNX 原子声明 player→teamID 归属(不变量 §1)。
func (r *RedisTeamRepo) ClaimPlayer(ctx context.Context, playerID, teamID uint64, ttl time.Duration) (uint64, bool, error) {
	key := playerKey(playerID)
	val := strconv.FormatUint(teamID, 10)
	// 最多两次:首次 SETNX 失败后若发现刚好过期(redis.Nil)再抢一次。
	for attempt := 0; attempt < 2; attempt++ {
		ok, err := r.rdb.SetNX(ctx, key, val, ttl).Result()
		if err != nil {
			return 0, false, err
		}
		if ok {
			return teamID, true, nil
		}
		cur, err := r.rdb.Get(ctx, key).Result()
		if err == redis.Nil {
			// 占用者刚好过期,重试一次 SETNX
			continue
		}
		if err != nil {
			return 0, false, err
		}
		existing, err := strconv.ParseUint(cur, 10, 64)
		if err != nil {
			return 0, false, err
		}
		return existing, false, nil
	}
	return 0, false, errcode.New(errcode.ErrTeamConcurrent, "claim player %d concurrent", playerID)
}

// DeletePlayerIndex 无条件删除 player→teamID 映射。不在 TeamRepo 接口内:biz 清理路径
// 一律用 DeletePlayerIndexIfMatches(CAS),无条件删存在误删并发新 claim 的窗口;
// 仅供测试造数据用(镜像 matchmaker RedisMatchRepo.DeletePlayerIndex)。
func (r *RedisTeamRepo) DeletePlayerIndex(ctx context.Context, playerID uint64) error {
	return r.rdb.Del(ctx, playerKey(playerID)).Err()
}

// DeleteTeam 见接口注释。删队伍主体 key(CreateTeam 声明失败回滚专用)。
func (r *RedisTeamRepo) DeleteTeam(ctx context.Context, teamID uint64) error {
	return r.rdb.Del(ctx, teamKey(teamID)).Err()
}

// deletePlayerIndexScript 仅当索引当前值仍是待清理的旧 teamID 时才 DEL(原子比较,
// 防「读旧索引 → 队伍已没 → 玩家新 SETNX 写入 → 误删新索引」的并发窗口)。
var deletePlayerIndexScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0`)

// DeletePlayerIndexIfMatches 见接口注释。CAS 删除孤儿索引。
func (r *RedisTeamRepo) DeletePlayerIndexIfMatches(ctx context.Context, playerID, teamID uint64) error {
	return deletePlayerIndexScript.Run(ctx, r.rdb,
		[]string{playerKey(playerID)},
		strconv.FormatUint(teamID, 10)).Err()
}

// ExpireTeam 单独刷新 team key 的 TTL(单条 EXPIRE,不读改写 value)。
func (r *RedisTeamRepo) ExpireTeam(ctx context.Context, teamID uint64, ttl time.Duration) error {
	return r.rdb.Expire(ctx, teamKey(teamID), ttl).Err()
}

// TouchTeam 一次 pipeline 同时 EXPIRE 队伍 key + 玩家索引 key(在线心跳保活)。
func (r *RedisTeamRepo) TouchTeam(ctx context.Context, teamID, playerID uint64, ttl time.Duration) error {
	pipe := r.rdb.Pipeline()
	pipe.Expire(ctx, teamKey(teamID), ttl)
	pipe.Expire(ctx, playerKey(playerID), ttl)
	_, err := pipe.Exec(ctx)
	return err
}

// --- Invite ---

// claimInviteSlotScript 在被邀请人 pending 索引(单 key,cluster 安全)上原子完成:
// 清理已过期成员 → 校验上限 → 占位 + 刷新 TTL。返回 1=占位成功,0=已达上限。
// KEYS[1]=inviteTargetKey ARGV[1]=now_ms ARGV[2]=invite_id ARGV[3]=expires_at_ms
// ARGV[4]=max_pending ARGV[5]=ttl_ms
var claimInviteSlotScript = redis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[4]) then
	return 0
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1`)

func (r *RedisTeamRepo) SetInvite(ctx context.Context, inviteID, teamID, inviterID, targetPlayerID uint64, ttl time.Duration, maxPending int) error {
	now := time.Now().UnixMilli()
	expiresAtMs := now + ttl.Milliseconds()

	// 1. 先在 pending 索引上原子「限流 + 占位」(不变量 §9-18 写入侧上限)。
	//    索引与令牌 hash 分属不同 cluster slot,不能跨 key 原子;先占位后写 hash:
	//    hash 写失败时占位残留至多 TTL(60s)且 List/Get 都会跳过,自愈,方向安全
	//    (反过来先写 hash 会出现「令牌可 Accept 但不受限流管控」的超限窗口)。
	ok, err := claimInviteSlotScript.Run(ctx, r.rdb,
		[]string{inviteTargetKey(targetPlayerID)},
		now, strconv.FormatUint(inviteID, 10), expiresAtMs, maxPending, ttl.Milliseconds()).Int()
	if err != nil {
		return err
	}
	if ok == 0 {
		// R2 + §9.18:这是写入侧总量上限拒绝。必须带**当前 pending 数与上限**，
		// 否则玩家报“收不到邀请”时分不出是真满了还是过期成员没被清。
		// 计数只走拒绝路径且 best-effort（失败记 -1），不影响返回值与控制流。
		plog.With(ctx).Warnw("msg", "team_invite_slot_rejected",
			"reason", "invite_pending_limit_reached",
			"team_id", teamID, "inviter_id", inviterID,
			"target_player_id", targetPlayerID, "invite_id", inviteID,
			"pending", r.pendingCount(ctx, inviteTargetKey(targetPlayerID)),
			"max_pending", maxPending,
			"hint", "计数不含已过期成员（写入前已 ZREMRANGEBYSCORE 清过）")
		return errcode.New(errcode.ErrTeamInvitePendingLimit,
			"player %d has too many pending invites (max %d)", targetPlayerID, maxPending)
	}

	// 2. 写令牌 hash(权威)。失败则 best-effort 回收占位,避免白占配额 60s。
	key := inviteKey(inviteID)
	_, err = r.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, key,
			fieldTeamID, strconv.FormatUint(teamID, 10),
			fieldTargetPlayerID, strconv.FormatUint(targetPlayerID, 10),
			fieldInviterID, strconv.FormatUint(inviterID, 10),
			fieldExpiresAtMs, strconv.FormatInt(expiresAtMs, 10),
		)
		pipe.Expire(ctx, key, ttl)
		return nil
	})
	if err != nil {
		_ = r.rdb.ZRem(ctx, inviteTargetKey(targetPlayerID), strconv.FormatUint(inviteID, 10)).Err()
		return err
	}
	return nil
}

func (r *RedisTeamRepo) GetInvite(ctx context.Context, inviteID uint64) (*InviteRecord, bool, error) {
	fields, err := r.rdb.HGetAll(ctx, inviteKey(inviteID)).Result()
	if err != nil {
		return nil, false, err
	}
	if len(fields) == 0 {
		return nil, false, nil
	}
	rec, err := inviteFromHash(inviteID, fields)
	if err != nil {
		return nil, false, err
	}
	return rec, true, nil
}

func (r *RedisTeamRepo) DeleteInvite(ctx context.Context, inviteID, targetPlayerID uint64) error {
	// 两个 key 分属不同 cluster slot,顺序两条命令(非原子):hash 先删保证令牌立即失效;
	// 索引 ZREM 失败只影响配额释放,由 List/SetInvite 的过期清理兜底。
	if err := r.rdb.Del(ctx, inviteKey(inviteID)).Err(); err != nil {
		return err
	}
	return r.rdb.ZRem(ctx, inviteTargetKey(targetPlayerID), strconv.FormatUint(inviteID, 10)).Err()
}

func (r *RedisTeamRepo) ListPendingInvites(ctx context.Context, targetPlayerID uint64, limit int) ([]*InviteRecord, error) {
	if limit <= 0 {
		return nil, nil
	}
	idxKey := inviteTargetKey(targetPlayerID)
	now := time.Now().UnixMilli()

	// 1. 惰性清理已过期成员,再按过期时间升序取前 limit 个 invite_id(读取侧上限)。
	if err := r.rdb.ZRemRangeByScore(ctx, idxKey, "-inf", strconv.FormatInt(now, 10)).Err(); err != nil {
		return nil, err
	}
	ids, err := r.rdb.ZRangeByScore(ctx, idxKey, &redis.ZRangeBy{
		Min: "-inf", Max: "+inf", Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}

	// 2. 逐条读令牌 hash(权威)。hash 已没(已接受/TTL 竞态)→ 跳过并顺手清索引残留。
	pipe := r.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		inviteID, perr := strconv.ParseUint(id, 10, 64)
		if perr != nil {
			return nil, fmt.Errorf("invite index of player %d bad member %q: %w", targetPlayerID, id, perr)
		}
		cmds[i] = pipe.HGetAll(ctx, inviteKey(inviteID))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}

	invites := make([]*InviteRecord, 0, len(ids))
	var stale []interface{}
	for i, id := range ids {
		fields, cerr := cmds[i].Result()
		if cerr != nil {
			return nil, cerr
		}
		if len(fields) == 0 {
			stale = append(stale, id)
			continue
		}
		inviteID, _ := strconv.ParseUint(id, 10, 64)
		rec, perr := inviteFromHash(inviteID, fields)
		if perr != nil {
			return nil, perr
		}
		invites = append(invites, rec)
	}
	if len(stale) > 0 {
		// best-effort:残留成员本来也会随 score 过期被清,失败无碍。
		_ = r.rdb.ZRem(ctx, idxKey, stale...).Err()
	}
	return invites, nil
}

// --- Application(入队申请) ---

// claimApplicationScript 在队伍申请索引(单 key,cluster 安全)上原子完成:
// 清理已过期成员 → 仅当申请人不在列表内时才校验上限 → 写入/刷新 + 刷新 TTL。
// 返回 1=写入成功(含幂等刷新),0=已达上限。
// KEYS[1]=applyKey ARGV[1]=now_ms ARGV[2]=applicant_id ARGV[3]=expires_at_ms
// ARGV[4]=max_pending ARGV[5]=ttl_ms
//
// 「已存在则不查上限」是幂等的关键:申请人重复点申请只是刷新自己那条的过期时间,
// 不应该因为队伍恰好满 10 条(其中一条就是他自己)而被自己挤掉。
var claimApplicationScript = redis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if redis.call('ZSCORE', KEYS[1], ARGV[2]) == false then
	if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[4]) then
		return 0
	end
end
redis.call('ZADD', KEYS[1], ARGV[3], ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[5])
return 1`)

func (r *RedisTeamRepo) ClaimApplication(ctx context.Context, teamID, applicantID uint64, ttl time.Duration, maxPending int) (int64, error) {
	now := time.Now().UnixMilli()
	expiresAtMs := now + ttl.Milliseconds()

	ok, err := claimApplicationScript.Run(ctx, r.rdb,
		[]string{applyKey(teamID)},
		now, strconv.FormatUint(applicantID, 10), expiresAtMs, maxPending, ttl.Milliseconds()).Int()
	if err != nil {
		return 0, err
	}
	if ok == 0 {
		// R2 + §9.18：max_applications_per_team（默认 10）上限拒绝，错误码 3009。
		// 3009 不是 IsServerFault，access log 只会记成 rpc_ok=DEBUG，线上不可见；
		// 玩家报“申请不了”时，只有 pending/max 同框才分得出是真满了还是清理没跑。
		plog.With(ctx).Warnw("msg", "team_application_slot_rejected",
			"reason", "application_pending_limit_reached",
			"team_id", teamID, "player_id", applicantID,
			"pending", r.pendingCount(ctx, applyKey(teamID)),
			"max_pending", maxPending,
			"ttl_ms", ttl.Milliseconds(),
			"hint", "重复申请同一队伍不占新名额；计数不含已过期成员")
		return 0, errcode.New(errcode.ErrTeamApplyPendingLimit,
			"team %d has too many pending applications (max %d)", teamID, maxPending)
	}
	return expiresAtMs, nil
}

// takeApplicationScript 原子取走一份申请:存在即删,并按 score 判定是否仍有效。
// 返回 1=取到有效申请,0=不存在或已过期(过期成员同样被删掉,顺手清理)。
// KEYS[1]=applyKey ARGV[1]=now_ms ARGV[2]=applicant_id
var takeApplicationScript = redis.NewScript(`
local score = redis.call('ZSCORE', KEYS[1], ARGV[2])
if score == false then
	return 0
end
redis.call('ZREM', KEYS[1], ARGV[2])
if tonumber(score) <= tonumber(ARGV[1]) then
	return 0
end
return 1`)

func (r *RedisTeamRepo) TakeApplication(ctx context.Context, teamID, applicantID uint64) (bool, error) {
	taken, err := takeApplicationScript.Run(ctx, r.rdb,
		[]string{applyKey(teamID)},
		time.Now().UnixMilli(), strconv.FormatUint(applicantID, 10)).Int()
	if err != nil {
		return false, err
	}
	return taken == 1, nil
}

func (r *RedisTeamRepo) ListApplications(ctx context.Context, teamID uint64, limit int) ([]*ApplicationRecord, error) {
	if limit <= 0 {
		return nil, nil
	}
	key := applyKey(teamID)
	now := time.Now().UnixMilli()

	// 惰性清理已过期成员,再按过期时间升序取前 limit 条(读取侧上限)。
	if err := r.rdb.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(now, 10)).Err(); err != nil {
		return nil, err
	}
	items, err := r.rdb.ZRangeArgsWithScores(ctx, redis.ZRangeArgs{
		Key: key, Start: "-inf", Stop: "+inf", ByScore: true, Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]*ApplicationRecord, 0, len(items))
	for _, it := range items {
		member, _ := it.Member.(string)
		playerID, perr := strconv.ParseUint(member, 10, 64)
		if perr != nil {
			return nil, fmt.Errorf("team %d application index bad member %q: %w", teamID, member, perr)
		}
		out = append(out, &ApplicationRecord{PlayerID: playerID, ExpiresAtMs: int64(it.Score)})
	}
	return out, nil
}

func (r *RedisTeamRepo) DeleteApplications(ctx context.Context, teamID uint64) error {
	return r.rdb.Del(ctx, applyKey(teamID)).Err()
}

// --- Open team index(开放招募队伍索引,非权威投影) ---

func (r *RedisTeamRepo) SyncOpenTeam(ctx context.Context, teamID uint64, mapID, prevMapID uint32, open bool, expiresAtMs int64, ttl time.Duration) error {
	member := strconv.FormatUint(teamID, 10)

	// 换图:先把旧分桶里的残留摘掉,否则同一支队伍会同时挂在两个 map 桶下。
	// 与下面的写入不在同一 slot,不做原子保证(索引非权威,读取侧会复核 map_id)。
	if prevMapID != mapID {
		if err := r.rdb.ZRem(ctx, openMapKey(prevMapID), member).Err(); err != nil {
			return err
		}
	}

	if !open {
		if err := r.rdb.ZRem(ctx, openAllKey, member).Err(); err != nil {
			return err
		}
		return r.rdb.ZRem(ctx, openMapKey(mapID), member).Err()
	}

	z := redis.Z{Score: float64(expiresAtMs), Member: member}
	if err := r.rdb.ZAdd(ctx, openAllKey, z).Err(); err != nil {
		return err
	}
	if err := r.rdb.Expire(ctx, openAllKey, ttl).Err(); err != nil {
		return err
	}
	if err := r.rdb.ZAdd(ctx, openMapKey(mapID), z).Err(); err != nil {
		return err
	}
	return r.rdb.Expire(ctx, openMapKey(mapID), ttl).Err()
}

func (r *RedisTeamRepo) ListOpenTeamIDs(ctx context.Context, mapID uint32, limit int) ([]uint64, error) {
	if limit <= 0 {
		return nil, nil
	}
	key := openAllKey
	if mapID > 0 {
		key = openMapKey(mapID)
	}
	now := time.Now().UnixMilli()

	// score = 索引项最晚存活时刻;已过期 = 对应队伍 key 也早该没了,直接清掉。
	if err := r.rdb.ZRemRangeByScore(ctx, key, "-inf", strconv.FormatInt(now, 10)).Err(); err != nil {
		return nil, err
	}
	// score 降序 = 最近一次写入(最近活跃)在前,让玩家优先看到还在动的队伍。
	members, err := r.rdb.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
		Min: "-inf", Max: "+inf", Offset: 0, Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}
	out := make([]uint64, 0, len(members))
	for _, m := range members {
		teamID, perr := strconv.ParseUint(m, 10, 64)
		if perr != nil {
			return nil, fmt.Errorf("open team index %q bad member %q: %w", key, m, perr)
		}
		out = append(out, teamID)
	}
	return out, nil
}

func (r *RedisTeamRepo) RemoveOpenTeamCandidate(ctx context.Context, teamID uint64, mapID uint32) error {
	member := strconv.FormatUint(teamID, 10)
	if err := r.rdb.ZRem(ctx, openAllKey, member).Err(); err != nil {
		return err
	}
	return r.rdb.ZRem(ctx, openMapKey(mapID), member).Err()
}

// inviteFromHash 把 invite hash 字段解析成 InviteRecord。
// inviter_id / expires_at_ms 缺失(旧版本写入的记录)按 0 处理不报错:记录只活 60s,自然换代。
func inviteFromHash(inviteID uint64, fields map[string]string) (*InviteRecord, error) {
	teamID, err := strconv.ParseUint(fields[fieldTeamID], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invite %d bad team_id: %w", inviteID, err)
	}
	targetPlayerID, err := strconv.ParseUint(fields[fieldTargetPlayerID], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invite %d bad target_player_id: %w", inviteID, err)
	}
	inviterID, _ := strconv.ParseUint(fields[fieldInviterID], 10, 64)
	expiresAtMs, _ := strconv.ParseInt(fields[fieldExpiresAtMs], 10, 64)
	return &InviteRecord{
		InviteID:       inviteID,
		TeamID:         teamID,
		TargetPlayerID: targetPlayerID,
		InviterID:      inviterID,
		ExpiresAtMs:    expiresAtMs,
	}, nil
}

// ── 诊断辅助 ──────────────────────────────────────────────────────────────────

// pendingCount 读一个 pending 索引 zset 的当前成员数,**只用于上限拒绝那一条日志**。
//
// 为什么不让 Lua 顺带返回:改脚本返回值会动到已被测试与调用方依赖的契约(1/0),
// 而这里只在**已经确定要拒**的分支上多读一次 ZCARD —— 不改控制流、不改返回值、
// 不改错误码,失败也只是把日志里的数字记成 -1。
//
// 计数口径:Lua 已在同一次调用里 ZREMRANGEBYSCORE 清过期成员,所以这个数就是
// "此刻真正占着名额的条数";与拒绝时用的判据存在毫秒级偏差(期间可能有人被删),
// 排查量级足够,不用于任何判定。
func (r *RedisTeamRepo) pendingCount(ctx context.Context, key string) int64 {
	n, err := r.rdb.ZCard(ctx, key).Result()
	if err != nil {
		return -1
	}
	return n
}

// ── 序列化辅助 ────────────────────────────────────────────────────────────────

func marshalTeam(team *teamv1.TeamStorageRecord) ([]byte, error) {
	if team == nil {
		return nil, fmt.Errorf("nil team")
	}
	return proto.Marshal(team)
}

// unmarshalTeam 从 Redis value 反序列化成 teamv1.TeamStorageRecord。
func unmarshalTeam(teamID uint64, payload []byte) (*teamv1.TeamStorageRecord, error) {
	rec := &teamv1.TeamStorageRecord{}
	if err := proto.Unmarshal(payload, rec); err != nil {
		return nil, fmt.Errorf("team %d bad proto: %w", teamID, err)
	}
	if rec.TeamId == 0 {
		rec.TeamId = teamID
	}
	if rec.TeamId != teamID {
		return nil, fmt.Errorf("team %d id mismatch: %d", teamID, rec.TeamId)
	}
	return rec, nil
}

// ScanPlayerIndex 增量扫描 player→team 归属索引,返回一批**有队伍的玩家**。
//
// 用途:offlinewatch 的兜底候选源(见 biz/offline_leave.go 的 teamRosterSource)。
// 事件链与读路径兜底在「整支队伍一起掉线」时会同时失效,那时只有主动扫描能发现残留。
//
// 用 SCAN 而不是另建一个活跃玩家集合:这份索引**本来就是**「有队伍的玩家」全集,
// 再维护一个平行集合就是 §9.22 的重复影子状态(还要在建队/离队/解散/TTL 过期四处
// 保持一致,漏一处就漂移)。SCAN 是增量游标、不阻塞,配合 count 上限足够。
//
// cursor 由调用方持有并在轮次间传递;返回 0 表示一轮遍历结束(下轮从头开始)。
func (r *RedisTeamRepo) ScanPlayerIndex(ctx context.Context, cursor uint64, count int64) ([]uint64, uint64, error) {
	keys, next, err := r.rdb.Scan(ctx, cursor, "pandora:team:player:*", count).Result()
	if err != nil {
		return nil, 0, errcode.New(errcode.ErrInternal, "redis scan player index: %v", err)
	}
	ids := make([]uint64, 0, len(keys))
	for _, k := range keys {
		// key 形如 pandora:team:player:<player_id>
		idx := strings.LastIndexByte(k, ':')
		if idx < 0 || idx+1 >= len(k) {
			continue
		}
		pid, perr := strconv.ParseUint(k[idx+1:], 10, 64)
		if perr != nil || pid == 0 {
			continue // 脏 key 跳过,不让一条坏数据卡住整轮兜底
		}
		ids = append(ids, pid)
	}
	return ids, next, nil
}
