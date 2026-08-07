package conf

import (
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
)

// TestResolveNoShowTimeout 锁定「从未连入」局的空场回收阈值解析。
//
// 这个函数决定的是「一台 14Gi 的 Battle DS 白押多久才回收」,配错的两个方向都危险:
// 配太长 → 刷进出副本能把 Fleet 押死(anti-abuse-scene-entry.md §3.2.1);
// 配太短 → 正在 travel / 加载地图的正常玩家被判 no-show,进不去场景(§9.20 红线)。
// 因此下限有护栏、上限跟随 empty,且**任何情况下都不得返回 0**(0 = 永不回收,比不改还糟)。
func TestResolveNoShowTimeout(t *testing.T) {
	const empty = 5 * time.Minute

	cases := []struct {
		name   string
		empty  time.Duration
		noShow time.Duration
		want   time.Duration
	}{
		{
			name: "未配置取默认 150s(DSTicket TTL 120s + 30s 余量)",
			empty: empty, noShow: 0, want: DefaultNoShowBattleTimeout,
		},
		{
			name: "显式配置按原值生效",
			empty: empty, noShow: 90 * time.Second, want: 90 * time.Second,
		},
		{
			name: "低于下限被钳到 60s —— 手滑配 1s 不能让玩家进不去",
			empty: empty, noShow: time.Second, want: NoShowTimeoutFloor,
		},
		{
			name: "高于 empty 被钳到 empty —— no-show 不该比普通空场还晚回收",
			empty: empty, noShow: 10 * time.Minute, want: empty,
		},
		{
			name: "负值 = 显式禁用差异化,退回单阈值(改动前行为)",
			empty: empty, noShow: -1, want: empty,
		},
		{
			name: "empty 本身禁用时跟随禁用,不自作主张开启回收",
			empty: -1, noShow: 90 * time.Second, want: -1,
		},
		{
			name: "empty 为 0(未配)时跟随,交由 Defaults 填默认值",
			empty: 0, noShow: 90 * time.Second, want: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := AllocatorConf{
				EmptyBattleTimeout:  config.Duration(tc.empty),
				NoShowBattleTimeout: config.Duration(tc.noShow),
			}.ResolveNoShowTimeout()
			if got != tc.want {
				t.Fatalf("ResolveNoShowTimeout() = %s, want %s", got, tc.want)
			}
		})
	}
}

// TestResolveNoShowTimeoutNeverSilentlyZero:empty 已启用时,无论 no-show 怎么配,
// 解析结果都必须是正数。返回 0 会让 `timeout > 0` 判定永假 ⇒ no-show 局**永不回收**,
// 那比改动前更糟(fail-safe 方向:宁可回收得晚,不可不回收)。
func TestResolveNoShowTimeoutNeverSilentlyZero(t *testing.T) {
	const empty = 5 * time.Minute
	for _, noShow := range []time.Duration{
		-time.Hour, -1, 0, time.Nanosecond, time.Second, 90 * time.Second, time.Hour,
	} {
		got := AllocatorConf{
			EmptyBattleTimeout:  config.Duration(empty),
			NoShowBattleTimeout: config.Duration(noShow),
		}.ResolveNoShowTimeout()
		if got <= 0 {
			t.Fatalf("ResolveNoShowTimeout() = %s for no_show=%s; 启用 empty 时必须为正(0 = 永不回收)", got, noShow)
		}
		if got > empty {
			t.Fatalf("ResolveNoShowTimeout() = %s for no_show=%s; 不得超过 empty_battle_timeout", got, noShow)
		}
	}
}
