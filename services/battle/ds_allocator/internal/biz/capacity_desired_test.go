// capacity_desired_test.go — INC-20260724-001:按 spec.replicas(desired)区分
// 「有意不配容量」与「被负载打满」,给容量告警去噪。
//
// 事故背景:未做金丝雀发布时 canary Fleet 常态 desired=0/ready=0,旧逻辑 ready==0 先于一切
// 判定 ⇒ 恒判 exhausted,每 5m 一条 Error + Grafana critical 长期 firing,把真实的
// stable ready=0 信号淹没(事故当天 13:12:38 那条极可能就被当噪音略过)。
//
// 修复前:levelFor 只看 Ready,TestLevelForCanaryDesiredZeroIsQuiet 必失败。
package biz

import (
	"testing"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// 有意缩到 0 的 canary:不再判 exhausted,占用比按 0 计(不再在面板上常驻 100%)。
func TestLevelForCanaryDesiredZeroIsQuiet(t *testing.T) {
	c := data.FleetCapacity{
		Fleet: "pandora-battle-canary",
		// 事故当天实测:replicas=0 ready=0 allocated=0
		Replicas: 0, Ready: 0, Allocated: 0,
		Desired: 0, DesiredKnown: true, Canary: true,
	}
	if got := levelFor(c, 0.8); got != capacityOK {
		t.Fatalf("canary desired=0 level = %v, want capacityOK(不该告警)", got)
	}
	if got := usageRatio(c); got != 0 {
		t.Fatalf("canary desired=0 usage_ratio = %v, want 0", got)
	}
}

// 保守边界一:没解码到 spec.replicas(DesiredKnown=false)时维持旧行为照常告警。
// 不确定不得冒充"已知为 0"(§9.22)。
func TestLevelForDesiredUnknownStillExhausted(t *testing.T) {
	c := data.FleetCapacity{
		Fleet: "pandora-battle-canary",
		Ready: 0, Canary: true,
		Desired: 0, DesiredKnown: false,
	}
	if got := levelFor(c, 0.8); got != capacityExhausted {
		t.Fatalf("desired 未知时 level = %v, want capacityExhausted(保守告警)", got)
	}
	if got := usageRatio(c); got != 1.0 {
		t.Fatalf("desired 未知时 usage_ratio = %v, want 1.0", got)
	}
}

// 保守边界二:stable 轨被缩到 0 仍照常 exhausted —— 运维/脚本误缩零会让新对局分配必失败,
// 是真问题,不能因为"是故意缩的"就静音(本次事故的直接根因之一正是 stable 被 churn 到 0)。
func TestLevelForStableDesiredZeroStillExhausted(t *testing.T) {
	c := data.FleetCapacity{
		Fleet: "pandora-battle-stable",
		Ready: 0, Canary: false,
		Desired: 0, DesiredKnown: true,
	}
	if got := levelFor(c, 0.8); got != capacityExhausted {
		t.Fatalf("stable 缩到 0 时 level = %v, want capacityExhausted(必须照常告警)", got)
	}
}

// canary 真的在跑金丝雀(desired>0)时,打满照常告警 —— 去噪不得把 canary 的真实容量问题也吃掉。
func TestLevelForCanaryWithDesiredStillReportsExhausted(t *testing.T) {
	c := data.FleetCapacity{
		Fleet:    "pandora-battle-canary",
		Replicas: 3, Ready: 0, Allocated: 3,
		Desired: 3, DesiredKnown: true, Canary: true,
	}
	if got := levelFor(c, 0.8); got != capacityExhausted {
		t.Fatalf("canary desired=3 打满时 level = %v, want capacityExhausted", got)
	}
}

// 去噪必须体现在事件流上:常态空跑的 canary 连续多轮巡检都不产生任何事件日志。
func TestObserveCanaryDesiredZeroEmitsNoEvent(t *testing.T) {
	w := newWatcher(0.8)
	c := data.FleetCapacity{
		Fleet:   "pandora-battle-canary",
		Desired: 0, DesiredKnown: true, Canary: true,
	}
	for i := 0; i < 3; i++ {
		if ev := w.observe(c); ev != eventNone {
			t.Fatalf("第 %d 轮 canary desired=0 产生事件 %q, want 静默", i+1, ev)
		}
	}
}
