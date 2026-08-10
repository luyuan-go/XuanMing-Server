// noshow_recorder.go —— no-show 记账 → 进入侧退避的写者侧(anti-abuse §6 第 8 项)。
//
// key 契约(登记于 docs/design/infra.md §3.2「RateLimit」;读者是 matchmaker
// StartMatch 的 NoShowPenaltyRemaining,两端都经 redisx.RLKey 构造):
//
//	pandora:rl:match:noshow:<player_id>    记账计数器(窗口 no_show_ledger_window)
//	pandora:rl:match:noshowcd:<player_id>  退避惩罚窗(matchmaker 执行拒绝)
package data

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/redis/go-redis/v9"
)

// RedisNoShowRecorder 实现 biz.NoShowRecorder。
type RedisNoShowRecorder struct {
	rdb redis.UniversalClient
}

func NewRedisNoShowRecorder(rdb redis.UniversalClient) *RedisNoShowRecorder {
	return &RedisNoShowRecorder{rdb: rdb}
}

// RecordNoShow 记一次 no-show,返回窗口内累计次数(含本次)。
func (r *RedisNoShowRecorder) RecordNoShow(ctx context.Context, playerID uint64, window time.Duration) (int64, error) {
	return redisx.IncrWindow(ctx, r.rdb, redisx.RLKey("match", "noshow", playerID), window)
}

// ArmPenalty 布设进入侧退避窗(新罚覆盖旧罚剩余)。
func (r *RedisNoShowRecorder) ArmPenalty(ctx context.Context, playerID uint64, d time.Duration) error {
	return redisx.ArmPenalty(ctx, r.rdb, redisx.RLKey("match", "noshowcd", playerID), d)
}
