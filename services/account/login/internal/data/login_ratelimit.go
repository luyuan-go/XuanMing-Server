// login_ratelimit.go —— 登录失败 Quota(账号 + IP)的存储侧(anti-abuse §6 第 4 项)。
//
// key 契约(登记于 docs/design/infra.md §3.2「RateLimit」,全部 PX 自过期):
//
//	pandora:rl:login:failacct:<sha256_16>  账号维度失败计数(窗口 login_fail_window)
//	pandora:rl:login:failip:<ip>           IP 维度失败计数
//	pandora:rl:login:lockacct:<sha256_16>  账号锁(达限布设,PX=login_fail_lock)
//	pandora:rl:login:lockip:<ip>           IP 锁
//
// 账号名是客户端原文,不消毒直接拼 key 会撞 key 规范(长度/字符集),故取
// sha256 前 16 hex——只用作限流键,无逆向需求,16 hex(64bit)对在线撞库场景零碰撞压力。
// IP 由 Envoy 注入的受信头提供(字面量安全);空 IP = 未经 Envoy(dev 直连),跳过该维度。
package data

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/redis/go-redis/v9"
)

// RedisLoginRateLimiter 实现 biz.LoginRateLimiter。
type RedisLoginRateLimiter struct {
	rdb    redis.UniversalClient
	limit  int64
	window time.Duration
	lock   time.Duration
}

func NewRedisLoginRateLimiter(rdb redis.UniversalClient, limit int, window, lock time.Duration) *RedisLoginRateLimiter {
	return &RedisLoginRateLimiter{rdb: rdb, limit: int64(limit), window: window, lock: lock}
}

func hashAccount(account string) string {
	sum := sha256.Sum256([]byte(account))
	return hex.EncodeToString(sum[:8])
}

// LockRemaining 返回账号 / IP 任一维度的锁定剩余(取更长者;0 = 未锁)。
// Redis 故障返回 (0, err),调用方 Warn 后 fail-open 放行。
func (l *RedisLoginRateLimiter) LockRemaining(ctx context.Context, account, clientIP string) (time.Duration, error) {
	if l.limit <= 0 {
		return 0, nil
	}
	remain, err := redisx.PenaltyRemaining(ctx, l.rdb, redisx.RLKeyString("login", "lockacct", hashAccount(account)))
	if err != nil {
		return 0, err
	}
	if clientIP != "" {
		ipRemain, ipErr := redisx.PenaltyRemaining(ctx, l.rdb, redisx.RLKeyString("login", "lockip", clientIP))
		if ipErr != nil {
			return remain, ipErr
		}
		if ipRemain > remain {
			remain = ipRemain
		}
	}
	return remain, nil
}

// RecordFailure 记一次凭据失败:账号 + IP 双维度计数,任一维度达限即对该维度布锁。
// 锁窗内 Login 在入口被拒 → 不再产生新失败计数 → 锁到期自动恢复,无需人工干预。
func (l *RedisLoginRateLimiter) RecordFailure(ctx context.Context, account, clientIP string) error {
	if l.limit <= 0 || l.window <= 0 {
		return nil
	}
	n, err := redisx.IncrWindow(ctx, l.rdb, redisx.RLKeyString("login", "failacct", hashAccount(account)), l.window)
	if err != nil {
		return err
	}
	if n >= l.limit {
		if err := redisx.ArmPenalty(ctx, l.rdb, redisx.RLKeyString("login", "lockacct", hashAccount(account)), l.lock); err != nil {
			return err
		}
	}
	if clientIP == "" {
		return nil
	}
	ipN, err := redisx.IncrWindow(ctx, l.rdb, redisx.RLKeyString("login", "failip", clientIP), l.window)
	if err != nil {
		return err
	}
	if ipN >= l.limit {
		return redisx.ArmPenalty(ctx, l.rdb, redisx.RLKeyString("login", "lockip", clientIP), l.lock)
	}
	return nil
}
