package log

import "sync/atomic"

// Window 是依赖降级日志的「首错 + 每窗口一条 + 期间累计 + 恢复一条」限流器。
//
// 用途(模式 C):热路径(每请求 / 每条消息 / 每次心跳)上的**依赖降级**告警,依赖一挂
// 就会按请求量刷屏,把真信号淹掉。典型:Redis 缓存读写失败、pub/sub 发布失败、
// owner 权威弱依赖调用失败、locator 批量查询失败。
//
// 语义:
//   - 第一次失败必打(故障要立刻可见,不能被窗口吞掉);
//   - 同一窗口内的后续失败只累加不打;
//   - 窗口到期再打一条,并带上自上次成功以来的累计失败次数(streak);
//   - 成功路径调用 Recovered:返回本轮累计失败数并归零,>0 时应打一条「已恢复」。
//
// **不按业务 key 分桶**:业务 key(player_id / guild_id 等)是高基数,分桶会让内存随
// 玩家数无界增长(§16.5 容量边界、§9.18 有界纪律),还得配 TTL 清理;而运维要的信息是
// 「这个依赖挂了多久、期间失败多少次」,不是「每个玩家各自失败过」——后者由 prometheus
// 计数器承担。因此本类型故意无 key:零值可用,单实例常量内存。
//
// 并发安全:全部字段用 atomic,可被多 goroutine(如按 partition 并发的消费循环、
// 并发 RPC handler)共享。零值即可用,通常作为 usecase / consumer 结构体的**值字段**
// (不要拷贝已使用过的 Window,和 sync 类型同理——按值拷贝会连带复制计数)。
//
// 注意:不要用它包裹「一次扇出循环内逐元素失败」的日志——那种情况应在循环内累加、
// 循环后汇总一条(带 total/failed 分布),信息比限流更完整,且零状态。
//
// 用法:
//
//	if err := dep.Do(ctx); err != nil {
//	    if ok, streak := u.depLog.Admit(time.Now().UnixMilli(), 5000); ok {
//	        plog.With(ctx).Warnw("msg", "dep_failed", "streak", streak, "err", err)
//	    }
//	} else if n, _ := u.depLog.Recovered(); n > 0 {
//	    plog.With(ctx).Infow("msg", "dep_recovered", "failed_total", n)
//	}
type Window struct {
	streak   atomic.Int64 // 自上次成功以来的累计失败次数
	extra    atomic.Int64 // 附加累计量(如累计丢弃帧数);不用时恒 0
	loggedMs atomic.Int64 // 上次实际打印时刻(UnixMilli)
}

// Admit 记一次失败,返回本次是否应打印、以及自上次成功以来的累计失败次数。
// windowMs <= 0 时退化为「每次都打」(等价于不限流)。
func (w *Window) Admit(nowMs, windowMs int64) (shouldLog bool, streak int64) {
	n := w.streak.Add(1)
	if n == 1 {
		// 首错必打:Add 是原子的,并发下只有一个调用方拿到 1,不会重复打。
		w.loggedMs.Store(nowMs)
		return true, n
	}
	if windowMs <= 0 {
		return true, n
	}
	last := w.loggedMs.Load()
	// CAS 保证同一窗口只有一个调用方打印(并发失败不会在窗口边界打出多条)。
	if nowMs-last >= windowMs && w.loggedMs.CompareAndSwap(last, nowMs) {
		return true, n
	}
	return false, n
}

// AddExtra 累加附加计量(如本次丢弃的帧数),返回累计值。
func (w *Window) AddExtra(n int64) int64 { return w.extra.Add(n) }

// Recovered 在成功路径调用:归零并返回本轮累计失败次数与附加计量。
// failed > 0 表示此前处于降级状态,调用方应打一条「已恢复」日志。
func (w *Window) Recovered() (failed, extra int64) {
	failed = w.streak.Swap(0)
	if failed > 0 {
		extra = w.extra.Swap(0)
	}
	return failed, extra
}
