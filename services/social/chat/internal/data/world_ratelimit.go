// world_ratelimit.go —— 世界频道 per-player 发送冷却(压测审核【必修-5】,2026-07-26)。
//
// 实现:SET pandora:chat:world:cd:{player_id} 1 NX PX <cooldown> —— 单命令原子占窗,
// 跨副本一致(chat 可水平扩展,进程内限流会被副本数摊薄)。占窗成功 = 允许发送;
// key 已存在 = 冷却期内拒绝。无后台清理:key 自带 PX 过期,内存增长有界
// (≤ 冷却窗口内发过言的玩家数)。
//
// 语义边界:这是背压限流,不是权威门 —— Redis 故障时由 biz 层 fail-open 放行
// (牺牲限流保聊天可用),不影响任何数据正确性。
package data

import (
	"context"
	"fmt"
	"time"

	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/redis/go-redis/v9"
)

// worldCooldownKey returns "pandora:chat:world:cd:{playerID}"。
// (历史 key,先于 pandora:rl:* 规范存在;保留原样避免滚动升级窗口双键并行,
// 新增的非世界频道 key 一律走 redisx.RLKey 规范。)
func worldCooldownKey(playerID uint64) string {
	return fmt.Sprintf("pandora:chat:world:cd:%d", playerID)
}

// RedisWorldRateLimiter 是基于 go-redis 的 biz.WorldRateLimiter + biz.ChannelRateLimiter
// 实现(同一结构体,main 一次构造、两处注入)。
type RedisWorldRateLimiter struct {
	rdb redis.UniversalClient
}

// NewRedisWorldRateLimiter 构造。
func NewRedisWorldRateLimiter(rdb redis.UniversalClient) *RedisWorldRateLimiter {
	return &RedisWorldRateLimiter{rdb: rdb}
}

// AllowWorld 尝试占用 playerID 的世界频道冷却窗口(委托 pkg/redisx.Cooldown,
// 2026-08-10 起与 hub 切线冷却共用同一原语,不再各写一份 SETNX)。
// 注意 redisx 契约:Redis 故障返回 (true, err) —— biz 层本就按 err fail-open,语义不变。
func (l *RedisWorldRateLimiter) AllowWorld(ctx context.Context, playerID uint64, cooldown time.Duration) (bool, error) {
	return redisx.Cooldown(ctx, l.rdb, worldCooldownKey(playerID), cooldown)
}

// AllowChannel 非世界频道(private/team/guild/group)按频道独立占窗
// (anti-abuse §6 第 6 项;key = pandora:rl:chat:<channel>:<player_id>)。
func (l *RedisWorldRateLimiter) AllowChannel(ctx context.Context, channel string, playerID uint64, cooldown time.Duration) (bool, error) {
	return redisx.Cooldown(ctx, l.rdb, redisx.RLKey("chat", channel, playerID), cooldown)
}
