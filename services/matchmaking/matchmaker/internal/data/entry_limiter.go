// entry_limiter.go —— 进场侧限流适配(anti-abuse-scene-entry.md §6 第 2/3/7/8 项)。
//
// 全部委托 pkg/redisx 限流原语;语义边界 = 背压非权威门:Redis 故障一律 fail-open
// (原语已内建 allow=true + err 上抛,biz 层 Warn 留证),一人一票的正确性仍由
// durable operation SETNX / claim / locator BATTLE 门兜底。
//
// key 契约(登记于 docs/design/infra.md §3.2「RateLimit」;跨服务共享的两个 noshow
// key 由 ds_allocator 写、本服务读,两端都经 redisx.RLKey 构造,不得各自拼字符串):
//
//	pandora:rl:match:start:<captain_id>      StartMatch per-队长冷却
//	pandora:rl:match:startteam:<team_id>     StartMatch per-队伍冷却
//	pandora:rl:match:form:<ticket_id>        成局级冷却(含容量耗尽静默窗)
//	pandora:rl:match:noshowcd:<player_id>    no-show 进入侧退避(写者 ds_allocator)
package data

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/redis/go-redis/v9"
)

// RedisEntryLimiter 实现 biz.EntryRateLimiter。
type RedisEntryLimiter struct {
	rdb redis.UniversalClient
}

func NewRedisEntryLimiter(rdb redis.UniversalClient) *RedisEntryLimiter {
	return &RedisEntryLimiter{rdb: rdb}
}

// TryStartCooldown 依次占用队长与队伍两个冷却窗。任一在窗内 → 拒绝;
// 队长占成而队伍被拒时回收队长窗,避免半占状态双重惩罚下一次合法重试。
// teamID 为 0 时只按队长限(防御:调用方契约上 teamID 恒非 0)。
func (l *RedisEntryLimiter) TryStartCooldown(ctx context.Context, captainID, teamID uint64, window time.Duration) (bool, error) {
	capKey := redisx.RLKey("match", "start", captainID)
	ok, err := redisx.Cooldown(ctx, l.rdb, capKey, window)
	if err != nil || !ok {
		return ok, err // 原语契约:err 时 ok=true(fail-open)
	}
	if teamID == 0 {
		return true, nil
	}
	teamOK, err := redisx.Cooldown(ctx, l.rdb, redisx.RLKey("match", "startteam", teamID), window)
	if err != nil {
		return true, err
	}
	if !teamOK {
		_ = redisx.ClearCooldown(ctx, l.rdb, capKey)
		return false, nil
	}
	return true, nil
}

// ClearStartCooldown 释放两个冷却窗(StartMatch 占窗后业务失败时调用,§9.20 立即可重试)。
func (l *RedisEntryLimiter) ClearStartCooldown(ctx context.Context, captainID, teamID uint64) error {
	err := redisx.ClearCooldown(ctx, l.rdb, redisx.RLKey("match", "start", captainID))
	if teamID != 0 {
		if terr := redisx.ClearCooldown(ctx, l.rdb, redisx.RLKey("match", "startteam", teamID)); err == nil {
			err = terr
		}
	}
	return err
}

// TryFormCooldown 成局提交前占用本票据的成局冷却窗(首次零延迟,窗内重成局拒绝)。
func (l *RedisEntryLimiter) TryFormCooldown(ctx context.Context, ticketID uint64, window time.Duration) (bool, error) {
	return redisx.Cooldown(ctx, l.rdb, redisx.RLKey("match", "form", ticketID), window)
}

// InFormCooldown 只读探测(撮合组队路径:组内任一票据在窗内则本轮跳过该组合)。
func (l *RedisEntryLimiter) InFormCooldown(ctx context.Context, ticketID uint64) (bool, error) {
	d, err := redisx.PenaltyRemaining(ctx, l.rdb, redisx.RLKey("match", "form", ticketID))
	if err != nil {
		return false, err // fail-open:探测失败按不在窗内
	}
	return d > 0, nil
}

// ArmFormCooldown 无条件布设成局冷却窗(容量耗尽退票时用更长的静默窗覆盖)。
func (l *RedisEntryLimiter) ArmFormCooldown(ctx context.Context, ticketID uint64, window time.Duration) error {
	return redisx.ArmPenalty(ctx, l.rdb, redisx.RLKey("match", "form", ticketID), window)
}

// NoShowPenaltyRemaining 读 no-show 进入侧退避剩余(0 = 无惩罚;写者是 ds_allocator)。
func (l *RedisEntryLimiter) NoShowPenaltyRemaining(ctx context.Context, playerID uint64) (time.Duration, error) {
	return redisx.PenaltyRemaining(ctx, l.rdb, redisx.RLKey("match", "noshowcd", playerID))
}
