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
	"strings"
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

// hashAccount 先按认证后端的账号等价语义归一化,再哈希——否则失败配额可被
// 大小写 / 尾空格变体绕过(账号权威列是 utf8mb4_0900_ai_ci 大小写不敏感 + NO PAD,
// FindByAccount 裸 WHERE account=? 把 alice/Alice/ALICE/"alice " 解析为同一 player,
// 若各自独立计数,针对单一真实账号的失败预算被放大 N 倍,账号维度闸形同虚设)。
// 归一化口径:小写 + 去首尾空白,与 ai_ci + NO PAD 对齐。
func hashAccount(account string) string {
	norm := strings.ToLower(strings.TrimSpace(account))
	sum := sha256.Sum256([]byte(norm))
	return hex.EncodeToString(sum[:8])
}

// LockRemaining 返回账号 / IP 两维度锁定剩余的较长者(0 = 未锁)。
// 两维度**独立读、各自 fail-open**:一个维度读失败绝不短路掉另一个维度已读到的锁
// ——撞库场景下 IP 维度锁常是唯一有效防线(攻击者轮换账号名使账号锁失效),
// 若账号键读失败就提前返回会把 IP 锁静默绕过。返回的 err 是聚合的部分故障信号
// (调用方对**未读到**的部分 fail-open,对**已读到**的锁照常生效)。
func (l *RedisLoginRateLimiter) LockRemaining(ctx context.Context, account, clientIP string) (time.Duration, error) {
	if l.limit <= 0 {
		return 0, nil
	}
	var remain time.Duration
	var firstErr error
	if d, err := redisx.PenaltyRemaining(ctx, l.rdb, redisx.RLKeyString("login", "lockacct", hashAccount(account))); err != nil {
		firstErr = err
	} else if d > remain {
		remain = d
	}
	if clientIP != "" {
		if d, err := redisx.PenaltyRemaining(ctx, l.rdb, redisx.RLKeyString("login", "lockip", clientIP)); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		} else if d > remain {
			remain = d
		}
	}
	return remain, firstErr
}

// RecordFailure 记一次凭据失败:账号 + IP 双维度计数,任一维度达限即对该维度布锁,
// 并**清零该维度的计数**——否则计数窗(默认 15m)长于锁窗(默认 5m)时,锁到期后
// 残留的满计数会被单次失败 INC 回 ≥limit 重新布满锁,攻击者以「每 lock 一次失败」把
// 目标账号 / IP 长锁到整个计数窗,且共享 NAT 出口下会连坐锁死同 IP 正常玩家(违反 §9.20)。
// 清零后锁到期即真正恢复,再锁必须重新攒满 limit 次(与 TestLogin...AutoRecovers 断言一致)。
func (l *RedisLoginRateLimiter) RecordFailure(ctx context.Context, account, clientIP string) error {
	if l.limit <= 0 || l.window <= 0 {
		return nil
	}
	acctFailKey := redisx.RLKeyString("login", "failacct", hashAccount(account))
	n, err := redisx.IncrWindow(ctx, l.rdb, acctFailKey, l.window)
	if err != nil {
		return err
	}
	if n >= l.limit {
		if err := redisx.ArmPenalty(ctx, l.rdb, redisx.RLKeyString("login", "lockacct", hashAccount(account)), l.lock); err != nil {
			return err
		}
		_ = redisx.ClearCooldown(ctx, l.rdb, acctFailKey) // 清计数:锁到期后需重新攒满
	}
	if clientIP == "" {
		return nil
	}
	ipFailKey := redisx.RLKeyString("login", "failip", clientIP)
	ipN, err := redisx.IncrWindow(ctx, l.rdb, ipFailKey, l.window)
	if err != nil {
		return err
	}
	if ipN >= l.limit {
		if err := redisx.ArmPenalty(ctx, l.rdb, redisx.RLKeyString("login", "lockip", clientIP), l.lock); err != nil {
			return err
		}
		_ = redisx.ClearCooldown(ctx, l.rdb, ipFailKey)
	}
	return nil
}
