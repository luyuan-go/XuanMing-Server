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
//	pandora:rl:match:form:<ticket_id>        成局级冷却(含容量耗尽静默窗)
//	pandora:rl:match:noshowcd:<player_id>    no-show 进入侧退避(写者 ds_allocator)
//
// 刻意只按队长(captain_id)计,不按 team_id:captain_id 来自 JWT(service 层 callerID),
// 攻击者只能占用自己作为队长的键,天然自限;而 team_id 来自请求体、未经校验,若按它占坑
// 会变成「刷任意 team_id 压制他人队伍进场」的定向骚扰原语(冷却门在成员校验之前),得不偿失。
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

// TryStartCooldown 占用队长冷却窗(按 JWT 身份的 captain_id,自限、不可用于骚扰他人)。
// teamID 参数保留在签名里仅为接口稳定,当前不参与占坑(见文件头说明)。
func (l *RedisEntryLimiter) TryStartCooldown(ctx context.Context, captainID, teamID uint64, window time.Duration) (bool, error) {
	return redisx.Cooldown(ctx, l.rdb, redisx.RLKey("match", "start", captainID), window)
}

// ClearStartCooldown 释放队长冷却窗(StartMatch 占窗后业务失败时调用,§9.20 立即可重试)。
func (l *RedisEntryLimiter) ClearStartCooldown(ctx context.Context, captainID, teamID uint64) error {
	return redisx.ClearCooldown(ctx, l.rdb, redisx.RLKey("match", "start", captainID))
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
