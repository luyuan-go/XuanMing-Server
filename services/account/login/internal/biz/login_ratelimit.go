// login_ratelimit.go —— 登录失败 Quota(账号 + IP)的业务侧(anti-abuse §6 第 4 项)。
//
// 语义边界 = 背压非权威门(anti-abuse §2 铁律):
//   - 只对**凭据失败**计数:密码错、自动注册关闭下的账号不存在。成功登录不计数,
//     封禁不计数(已是终态),DB/Redis 故障不计数(否则故障期间会把正常玩家锁死);
//   - 判定 error 一律 fail-open + Warn,限流器故障绝不能挡住正常登录(§9.20);
//   - 锁窗内拒绝返回 ErrRateLimited + 可见剩余秒数,PX 自过期,无需人工解锁。
package biz

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
)

// LoginRateLimiter 登录失败配额(实现:data.RedisLoginRateLimiter,阈值在构造时注入)。
type LoginRateLimiter interface {
	// LockRemaining 返回账号 / IP 任一维度的锁定剩余(0 = 未锁)。
	LockRemaining(ctx context.Context, account, clientIP string) (time.Duration, error)
	// RecordFailure 记一次凭据失败(内部计数,达限布锁)。
	RecordFailure(ctx context.Context, account, clientIP string) error
}

// SetLoginRateLimiter 注入登录失败配额(可选;不注入 = 不限,dev 无 Redis 联调兼容)。
func (u *LoginUsecase) SetLoginRateLimiter(l LoginRateLimiter) {
	u.loginLimiter = l
}

// clientIPKey 请求级受信客户端 IP 的 ctx 键。只在单次 RPC 生命周期内由 service 层写入、
// Login 内读取,不逃逸到异步边界(§16.7);用 ctx 而非改 Login 签名,与 auth middleware
// 「身份经请求上下文传递」的既有模式一致。
type clientIPKey struct{}

// WithClientIP 由 service 层把 Envoy 注入的受信客户端 IP(x-pandora-client-ip,入站同名
// 头已被 Envoy 剥离防伪造)挂到请求 ctx。空串 = 未经 Envoy(本机直连),IP 维度自动关闭。
func WithClientIP(ctx context.Context, ip string) context.Context {
	if ip == "" {
		return ctx
	}
	return context.WithValue(ctx, clientIPKey{}, ip)
}

func clientIPFrom(ctx context.Context) string {
	ip, _ := ctx.Value(clientIPKey{}).(string)
	return ip
}

// checkLoginFailLock Login 入口的锁窗检查。锁中返回 ErrRateLimited(带剩余秒数)。
func (u *LoginUsecase) checkLoginFailLock(ctx context.Context, account, clientIP string) error {
	if u.loginLimiter == nil {
		return nil
	}
	remain, err := u.loginLimiter.LockRemaining(ctx, account, clientIP)
	if err != nil {
		// fail-open 只对「没读到」的维度:LockRemaining 返回的是已成功读到的锁剩余
		// (如账号维度读到、IP 维度读失败),已知的锁必须继续生效——部分故障不得
		// 放大成整体放行(否则打挂 IP 键所在分片就能绕过账号锁)。
		plog.With(ctx).Warnw("msg", "login_fail_lock_check_failed", "err", err)
	}
	if remain <= 0 {
		return nil
	}
	retrySec := int64((remain + time.Second - 1) / time.Second)
	// 定位到主体走日志不走 metrics(§4.4);账号原文只进日志供客服排查。
	plog.With(ctx).Warnw("msg", "login_fail_locked",
		"account", account, "client_ip", clientIP, "retry_after_sec", retrySec)
	return errcode.New(errcode.ErrRateLimited, "too many failed logins, retry after %ds", retrySec)
}

// recordLoginFailure 记一次凭据失败(fail-open:错误只 Warn)。
func (u *LoginUsecase) recordLoginFailure(ctx context.Context, account, clientIP string) {
	if u.loginLimiter == nil {
		return
	}
	if err := u.loginLimiter.RecordFailure(ctx, account, clientIP); err != nil {
		plog.With(ctx).Warnw("msg", "login_fail_record_failed", "err", err)
	}
}
