// Package safego —— 后台 goroutine / server-stream 的统一 panic 兜底(压测审核【必修-6】,2026-07-26)。
//
// 背景:pkg/middleware.Recovery() 只覆盖 gRPC unary 中间件链;server-stream RPC
// (push.Subscribe)与所有长生命周期后台 goroutine(matchmaker RunMatchLoop、push 看门狗、
// locator PresenceHub tick、hub RunHeartbeatSweep 等)此前无任何 recover —— 任一 latent
// panic(nil 解引用 / 越界 / 类型断言 / close 已关 channel)直接崩整进程:push 崩 = 该 Pod
// 全部长连瞬断 → 重连风暴;RunMatchLoop 崩 = 撮合停摆,玩家匹配卡死(违反 §9.19/§9.20)。
//
// 边界(重要):并发读写 map 触发的是 runtime fatal throw,recover 兜不住 —— 那类必须靠
// 正确同步,本包不提供任何此类兜底假象。recover 只覆盖可恢复 panic。
//
// 提供两个原语,按 goroutine 生命周期选用:
//
//   - Go(name, fn):一次性 goroutine(fire-and-forget / 看门狗单次驱动)。panic 只终止
//     该 goroutine,打完整 stack + 计数后返回,不重启(单次任务无"续跑"语义)。
//   - Loop(ctx, name, interval, fn):周期循环(撮合 tick / 心跳清扫 / presence tick)。
//     panic 终止**本轮**,打 stack 后等下个 tick 继续 —— 循环整体存活,单轮故障不停摆。
//     ctx 取消即退出,复用调用方已有的生命周期,不新建第二套 timer 状态机(§16.10)。
package safego

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	plog "github.com/luyuancpp/pandora/pkg/log"
)

// PanicRecovered 统计各命名点位被 recover 的 panic 次数(压测期告警断言:恒 0 才健康)。
// name 是代码点位名(有界枚举),不是 player_id 等高基数值。
var PanicRecovered = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "pandora_safego_panic_recovered_total",
	Help: "panics recovered by safego, by goroutine/callsite name",
}, []string{"name"})

// Recover 供无法用 Go/Loop 包装的入口直接 defer:
//
//	defer safego.Recover(ctx, "push_session_watchdog")
//
// 吞掉 panic 并打 stack;调用方函数按正常 return 路径退出。
// 必须以 `defer safego.Recover(...)` 形式直接调用才有效。
func Recover(ctx context.Context, name string) {
	Recovered(ctx, name, recover())
}

// Recovered 处理调用方已捕获的 panic 值,供需要自定义错误返回的 defer 闭包用
// (如 server-stream handler 要把 panic 转成 gRPC 错误):
//
//	defer func() {
//	    if safego.Recovered(ctx, "push_subscribe", recover()) {
//	        err = <转换后的错误>
//	    }
//	}()
//
// r == nil 返回 false(无 panic);否则打 stack + 计数并返回 true。
func Recovered(ctx context.Context, name string, r any) bool {
	if r == nil {
		return false
	}
	PanicRecovered.WithLabelValues(name).Inc()
	plog.With(ctx).Errorw(
		"msg", "panic_recovered",
		"name", name,
		"panic", r,
		"stack", string(debug.Stack()),
	)
	return true
}

// Go 启动一次性后台 goroutine,panic 只终止该 goroutine(打 stack + 计数,不重启)。
func Go(ctx context.Context, name string, fn func()) {
	go func() {
		defer Recover(ctx, name)
		fn()
	}()
}

// Run 同步执行 fn 并兜住其中的 panic(打 stack + 计数后返回)。
// 供既有 ticker 循环就地包装单轮 tick 体:
//
//	case <-t.C:
//	    safego.Run(ctx, "mail_sweep", func() { u.sweepOnce(ctx) })
//
// 单轮 panic 只丢本轮,循环与进程存活。fn 内持锁必须用 defer 解锁,
// 否则 panic 展开后锁悬挂(比崩进程更糟)。
func Run(ctx context.Context, name string, fn func()) {
	defer Recover(ctx, name)
	fn()
}

// Loop 以 interval 周期驱动 fn,直到 ctx 取消;每轮独立 recover,单轮 panic 不终止循环。
// fn 返回后等下个 tick(不 drift 累积;Ticker 语义)。interval<=0 时立即返回(防误配自旋)。
// 首轮在第一个 tick 到达后执行(不立即执行,与 time.Ticker 语义一致)。
func Loop(ctx context.Context, name string, interval time.Duration, fn func(ctx context.Context)) {
	if interval <= 0 {
		plog.With(ctx).Errorw("msg", "safego_loop_invalid_interval", "name", name, "interval", interval.String())
		return
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				runOnce(ctx, name, fn)
			}
		}
	}()
}

// runOnce 单轮执行(独立函数,让 defer recover 的作用域恰为一轮)。
func runOnce(ctx context.Context, name string, fn func(ctx context.Context)) {
	defer Recover(ctx, name)
	fn(ctx)
}
