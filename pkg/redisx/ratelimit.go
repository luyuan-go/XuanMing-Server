// ratelimit.go —— per-player 限流两原语(anti-abuse-scene-entry.md §4.2 第 1 层)。
//
// 定位:**背压,不是权威门**(同文档 §2 铁律)。正确性由各自的权威门兜底
// (ensureNoneInBattle / owner lease / Admission CAS…),限流只压成本;因此:
//   - 所有判定 error 时一律 fail-open(返回 allow=true 并把 err 交给调用方 Warn 留证),
//     限流器故障绝不能阻断玩家(§9.20 反向红线:限流自身不得成为卡玩家源头);
//   - key 一律自带 PX 过期,无后台清理任务,内存有界 = 窗口内活跃主体数;
//   - 拒绝时调用方返回 errcode.ErrRateLimited(9,已归入可重试类,gRPC 映射
//     ResourceExhausted),客户端按可重试处理。
//
// 刻意不做滑动窗口 / 令牌桶:固定窗口的边界双倍对「防外挂刷量」无实质影响
// (2 倍仍远小于外挂想要的 100 倍),滑动窗口要存有序集合、要清理,属 §15.3
// 预设性复杂化。本文件是 hub TryTransferCooldown 与 chat AllowWorld 两份同型
// SETNX 的收敛点:新增限流点一律用这里的原语,不再各写一份。
//
// key 规范(登记于 docs/design/infra.md §3.2「RateLimit」):
//
//	pandora:rl:<域>:<动作>:<主体id>   例 pandora:rl:match:start:1234567
package redisx

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RLKey 按统一规范构造限流 key(主体是 uint64 业务 ID 的常规形态)。
func RLKey(domain, action string, subject uint64) string {
	return fmt.Sprintf("pandora:rl:%s:%s:%d", domain, action, subject)
}

// RLKeyString 主体不是数字 ID 时(如账号名哈希、IP)的变体。
// 调用方负责保证 subject 已消毒(定长哈希 / IP 字面量),不得直接拼客户端原文。
func RLKeyString(domain, action, subject string) string {
	return fmt.Sprintf("pandora:rl:%s:%s:%s", domain, action, subject)
}

// Cooldown 占用一次冷却窗:SET key 1 NX PX window,单命令原子、跨副本一致。
// 返回 true = 占窗成功(放行),false = 窗口内已有一次(拒绝)。
// window <= 0 视为不限流(与 hub.transfer_cooldown 的既有语义一致)。
// Redis 故障 fail-open:返回 (true, err),调用方必须 Warn 留证后放行。
func Cooldown(ctx context.Context, rdb redis.UniversalClient, key string, window time.Duration) (bool, error) {
	if window <= 0 {
		return true, nil
	}
	ok, err := rdb.SetNX(ctx, key, 1, window).Result()
	if err != nil {
		return true, err
	}
	return ok, nil
}

// ClearCooldown 释放冷却窗(DEL,幂等)。
// 用于「先占冷却 → 干活 → 失败释放」模板(hub transferToLineInner 的既有形状):
// 业务失败时立即释放,让玩家可重试,冷却只约束成功路径的频率(§9.20)。
func ClearCooldown(ctx context.Context, rdb redis.UniversalClient, key string) error {
	return rdb.Del(ctx, key).Err()
}

// quotaScript:INCR + 仅首次设 PEXPIRE。写在一个 Lua 里保证「计数与窗口起点」原子,
// 避免 INCR 成功后进程死掉留下无 TTL 的永久计数键。
var quotaScript = redis.NewScript(`
local n = redis.call('INCR', KEYS[1])
if n == 1 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return n`)

// IncrWindow 固定窗口计数:返回本次递增后的计数值。窗口从首次计数起算,PX 自过期。
// Redis 故障返回 (0, err),调用方自行决定 fail-open 语义(通常按 0 处理)。
func IncrWindow(ctx context.Context, rdb redis.UniversalClient, key string, window time.Duration) (int64, error) {
	if window <= 0 {
		return 0, nil
	}
	n, err := quotaScript.Run(ctx, rdb, []string{key}, window.Milliseconds()).Int64()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// Quota 固定窗口配额:窗口内放行 limit 次,超出拒绝。
// limit <= 0 或 window <= 0 视为不限流。Redis 故障 fail-open 返回 (true, err)。
func Quota(ctx context.Context, rdb redis.UniversalClient, key string, limit int64, window time.Duration) (bool, error) {
	if limit <= 0 || window <= 0 {
		return true, nil
	}
	n, err := IncrWindow(ctx, rdb, key, window)
	if err != nil {
		return true, err
	}
	return n <= limit, nil
}

// ActionQuota 是「per-player 动作频率配额」的可复用件(anti-abuse §6 第 6 项):
// 申请/邀请/下单/撤单这类有总量闸但无频率闸的写入面,统一按
// pandora:rl:<Domain>:<action>:<player_id> 记固定窗口配额。
// 各服务 biz 定义自己的小接口,main 用本结构体一行装配,不再逐服务手写 data 适配。
// 语义同包内原语:Limit/Window <=0 不限流;Redis 故障 fail-open 返回 (true, err)。
type ActionQuota struct {
	RDB    redis.UniversalClient
	Domain string
	Limit  int64
	Window time.Duration
}

// Allow 记一次动作并判定是否在配额内。
func (q *ActionQuota) Allow(ctx context.Context, action string, subject uint64) (bool, error) {
	return Quota(ctx, q.RDB, RLKey(q.Domain, action, subject), q.Limit, q.Window)
}

// ArmPenalty 布设惩罚窗(SET PX,无条件覆盖 = 新惩罚顶替旧惩罚的剩余时长)。
// 与 Cooldown 的区别:Cooldown 是「先到先占」的准入判定,ArmPenalty 是
// 「事实发生后记罚」的写入(如 no-show 判弃后的进入侧退避)。
func ArmPenalty(ctx context.Context, rdb redis.UniversalClient, key string, window time.Duration) error {
	if window <= 0 {
		return nil
	}
	return rdb.Set(ctx, key, 1, window).Err()
}

// PenaltyRemaining 读惩罚窗剩余时长;key 不存在返回 0。
// Redis 故障返回 (0, err):调用方 Warn 后按 0(无惩罚)放行,同属 fail-open。
func PenaltyRemaining(ctx context.Context, rdb redis.UniversalClient, key string) (time.Duration, error) {
	d, err := rdb.PTTL(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	// PTTL 契约:-2 = key 不存在,-1 = 无过期(本包永不产生,防御性归零)。
	if d < 0 {
		return 0, nil
	}
	return d, nil
}
