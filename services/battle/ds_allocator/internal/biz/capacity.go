// capacity.go — 战斗 DS Fleet 容量巡检 + 快到上限预警(2026-07-10)。
//
// 背景:Agones Fleet 副本数是战斗 DS 并发容量的硬上限,打满后 GameServerAllocation
// 直接返回 UnAllocated(玩家匹配成功却进不了局)。本巡检让运维在打满**之前**就看到信号:
//   - 每 capacity_watch_interval(默认 30s)GET 通用 Fleet + 各 map_fleets 专属 Fleet 的 status;
//   - 暴露 Prometheus 指标 pandora_ds_allocator_fleet_{replicas,ready,allocated,usage_ratio};
//   - allocated/replicas ≥ capacity_warn_ratio(默认 0.8)→ Warn 日志 ds_fleet_capacity_near_limit;
//   - ready==0(完全打满 / Fleet 缩到 0)→ Error 日志 ds_fleet_capacity_exhausted;
//   - 回落阈值以下 → Info 日志 ds_fleet_capacity_recovered。
//
// 降噪(infra.md §11 周期任务只在有事发生时打):只在状态**变化**时打日志;持续超限时
// 每 rewarnInterval(5m)重打一次,避免 30s 一条刷屏,又不至于让长时间高水位静默。
// 持续性的水位信号看 Prometheus gauge(Grafana / 告警规则消费),日志只做事件预警。
//
// 仅 mode=agones 装配(main.go);local / mock 无 Fleet 概念不巡检。
package biz

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/metrics"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// ── Prometheus 指标(命名规范 infra.md §10:pandora_<service>_<metric>)────────
//
// label 只有 fleet(集群里 Fleet 数量级 = 个位数,无高基数风险)。

var (
	fleetReplicasGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pandora_ds_allocator_fleet_replicas",
		Help: "战斗 DS Fleet 当前总副本数(容量上限,Fleet status.replicas)",
	}, []string{"fleet"})

	fleetReadyGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pandora_ds_allocator_fleet_ready",
		Help: "战斗 DS Fleet 空闲可分配副本数(Fleet status.readyReplicas)",
	}, []string{"fleet"})

	fleetAllocatedGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pandora_ds_allocator_fleet_allocated",
		Help: "战斗 DS Fleet 已被对局占用副本数(Fleet status.allocatedReplicas)",
	}, []string{"fleet"})

	fleetUsageRatioGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pandora_ds_allocator_fleet_usage_ratio",
		Help: "战斗 DS Fleet 容量占用比 allocated/replicas(0~1;replicas=0 时置 1,有意不配容量的 canary 置 0)",
	}, []string{"fleet"})

	// fleetDesiredGauge 暴露 spec.replicas(期望副本数),供 Grafana 把「有意缩到 0 的 canary」
	// 从 ready==0 的 critical 规则里排除(INC-20260724-001 降噪)。
	// 只在解码到 spec.replicas 时才 Set —— 没解码到就不写,让告警规则退化为原判据继续告警,
	// 而不是被一个默认 0 值静默(§9.22:不得把 UNKNOWN 冒充成确定值)。
	fleetDesiredGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pandora_ds_allocator_fleet_desired_replicas",
		Help: "战斗 DS Fleet 期望副本数(Fleet spec.replicas;未解码到时不上报该序列)",
	}, []string{"fleet"})
)

func init() {
	metrics.Register(fleetReplicasGauge)
	metrics.Register(fleetReadyGauge)
	metrics.Register(fleetAllocatedGauge)
	metrics.Register(fleetUsageRatioGauge)
	metrics.Register(fleetDesiredGauge)
}

// FleetCapacityLister 由 data.AgonesGameServerAllocator 实现(GET Fleet status)。
type FleetCapacityLister interface {
	ListFleetCapacities(ctx context.Context) ([]data.FleetCapacity, error)
}

// capacityLevel 是单个 Fleet 的容量水位档。
type capacityLevel int

const (
	capacityOK        capacityLevel = iota // 水位正常
	capacityWarn                           // allocated/replicas ≥ warnRatio,接近上限
	capacityExhausted                      // ready==0,完全打满(或 Fleet 缩到 0),分配必失败
)

// rewarnInterval 是持续超限时的重复告警间隔:状态不变时最多每 5m 重打一条,防刷屏。
const rewarnInterval = 5 * time.Minute

// CapacityWatcher 周期巡检 Fleet 容量,更新指标 + 状态变化时打预警日志。
type CapacityWatcher struct {
	lister    FleetCapacityLister
	interval  time.Duration
	warnRatio float64

	// 每 Fleet 的上次水位档 + 上次告警时刻(仅巡检 goroutine 单线程读写,无需锁)。
	levels     map[string]capacityLevel
	lastWarnAt map[string]time.Time

	now func() time.Time // 可注入,单测控制时钟
}

// NewCapacityWatcher 构造容量巡检器;capacity_watch_interval 为负 = 显式禁用,返回 nil
// (调用方跳过启动)。cfg 经 Defaults() 归一化:interval 默认 30s,warnRatio 默认 0.8。
func NewCapacityWatcher(lister FleetCapacityLister, cfg conf.AgonesConf) *CapacityWatcher {
	interval := cfg.CapacityWatchInterval.Std()
	if interval < 0 {
		return nil
	}
	if interval == 0 {
		interval = 30 * time.Second
	}
	warnRatio := cfg.CapacityWarnRatio
	if warnRatio <= 0 || warnRatio > 1 {
		warnRatio = 0.8
	}
	return &CapacityWatcher{
		lister:     lister,
		interval:   interval,
		warnRatio:  warnRatio,
		levels:     make(map[string]capacityLevel),
		lastWarnAt: make(map[string]time.Time),
		now:        time.Now,
	}
}

// Run 启动巡检循环,直到 ctx 取消。与 RunHeartbeatSweep 同样由 main 用 go 启动。
func (w *CapacityWatcher) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "fleet_capacity_watch_started",
		"interval", w.interval.String(), "warn_ratio", w.warnRatio)
	// 启动先巡一次,不等首个 tick(服务刚起时就把水位摸清)。
	w.pollOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "fleet_capacity_watch_stopped")
			return
		case <-ticker.C:
			w.pollOnce(ctx)
		}
	}
}

// pollOnce 巡检一轮:拉容量快照 → 刷指标 → 按水位状态机打事件日志。
// 部分 Fleet 查询失败不影响其余(data 层部分成功也返回),失败汇总打一条 Warn。
func (w *CapacityWatcher) pollOnce(ctx context.Context) {
	caps, err := w.lister.ListFleetCapacities(ctx)
	if err != nil {
		plog.With(ctx).Warnw("msg", "fleet_capacity_query_failed", "err", err)
	}
	for _, c := range caps {
		ratio := usageRatio(c)
		fleetReplicasGauge.WithLabelValues(c.Fleet).Set(float64(c.Replicas))
		fleetReadyGauge.WithLabelValues(c.Fleet).Set(float64(c.Ready))
		fleetAllocatedGauge.WithLabelValues(c.Fleet).Set(float64(c.Allocated))
		fleetUsageRatioGauge.WithLabelValues(c.Fleet).Set(ratio)
		if c.DesiredKnown {
			fleetDesiredGauge.WithLabelValues(c.Fleet).Set(float64(c.Desired))
		}

		kv := []any{
			"fleet", c.Fleet, "replicas", c.Replicas, "ready", c.Ready,
			"allocated", c.Allocated, "usage_ratio", ratio, "warn_ratio", w.warnRatio,
			"desired", c.Desired, "desired_known", c.DesiredKnown, "canary", c.Canary,
		}
		switch w.observe(c) {
		case eventExhausted:
			plog.With(ctx).Errorw(append([]any{"msg", "ds_fleet_capacity_exhausted",
				"hint", "战斗 DS 无空闲副本,新对局分配必失败;立即扩 Fleet replicas"}, kv...)...)
		case eventNearLimit:
			plog.With(ctx).Warnw(append([]any{"msg", "ds_fleet_capacity_near_limit",
				"hint", "战斗 DS 并发接近容量上限,考虑扩 Fleet replicas"}, kv...)...)
		case eventRecovered:
			plog.With(ctx).Infow(append([]any{"msg", "ds_fleet_capacity_recovered"}, kv...)...)
		}
	}
}

// 巡检事件:pollOnce 据此决定打哪条日志(空串 = 不打)。
const (
	eventNone      = ""
	eventNearLimit = "near_limit"
	eventExhausted = "exhausted"
	eventRecovered = "recovered"
)

// observe 更新该 Fleet 的水位状态机,返回本轮要上报的事件:
//   - 升档(ok→warn / *→exhausted)立即上报;
//   - 同档持续超限,距上次告警 ≥ rewarnInterval 才再报(防刷屏);
//   - 从超限回落到 ok 上报 recovered;首轮即 ok 不上报。
func (w *CapacityWatcher) observe(c data.FleetCapacity) string {
	level := levelFor(c, w.warnRatio)
	prev, seen := w.levels[c.Fleet]
	w.levels[c.Fleet] = level
	now := w.now()

	switch {
	case level == capacityOK:
		if seen && prev != capacityOK {
			delete(w.lastWarnAt, c.Fleet)
			return eventRecovered
		}
		return eventNone
	case level > prev || !seen: // 升档(或首轮即超限)
		w.lastWarnAt[c.Fleet] = now
	case now.Sub(w.lastWarnAt[c.Fleet]) < rewarnInterval: // 同档/降档持续超限,未到重报间隔
		return eventNone
	default:
		w.lastWarnAt[c.Fleet] = now
	}
	if level == capacityExhausted {
		return eventExhausted
	}
	return eventNearLimit
}

// levelFor 计算水位档:ready==0 = 打满(exhausted);占用比 ≥ warnRatio = 接近上限(warn)。
//
// INC-20260724-001 降噪:**未做金丝雀发布时 canary Fleet 常态 desired=0**,
// 旧逻辑 ready==0 先于一切判定 ⇒ 恒判 exhausted,每 5m 一条 Error + Grafana critical 长期
// firing,把真实的 stable ready=0 信号淹没(事故当天 13:12:38 那条极可能就被当噪音略过)。
// 故:仅当**确定**知道 desired==0 且该 Fleet 属 canary 轨时,才认定"本就没配容量"而静默。
//
// 两条刻意的保守边界(防止把真问题也静音):
//   - DesiredKnown==false(没解码到 spec.replicas)→ 维持旧行为照常判 exhausted。
//     不确定不得冒充"已知为 0"(§9.22)。
//   - stable 轨 desired==0(运维/脚本误把 stable 缩到 0)→ **照常 exhausted 告警**。
//     那确实会让新对局分配必失败,是真问题,不能因为"是故意缩的"就不报。
func levelFor(c data.FleetCapacity, warnRatio float64) capacityLevel {
	if deliberatelyUnprovisioned(c) {
		return capacityOK
	}
	if c.Ready == 0 {
		return capacityExhausted
	}
	if usageRatio(c) >= warnRatio {
		return capacityWarn
	}
	return capacityOK
}

// deliberatelyUnprovisioned:该 Fleet 是「有意不配容量」而非「被负载打满」。
// 当前只认 canary 轨 desired==0 这一种(见 levelFor 注释里的两条保守边界)。
func deliberatelyUnprovisioned(c data.FleetCapacity) bool {
	return c.Canary && c.DesiredKnown && c.Desired == 0
}

// usageRatio = allocated/replicas;replicas==0(Fleet 缩到 0)按 1.0 计(零容量即满)。
// 例外:有意不配容量的 canary(desired==0)按 0 计 —— 它没有"占用比"可言,
// 记 1.0 会让 Grafana 面板上一条常态空跑的 canary 永远顶在 100%。
func usageRatio(c data.FleetCapacity) float64 {
	if c.Replicas == 0 {
		if deliberatelyUnprovisioned(c) {
			return 0
		}
		return 1.0
	}
	return float64(c.Allocated) / float64(c.Replicas)
}
