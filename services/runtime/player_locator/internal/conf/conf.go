// Package conf 是 player_locator 服务的私有配置结构。
package conf

import (
	"fmt"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
)

// Config 是 player_locator 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Locator  LocatorConf  `yaml:"locator" json:"locator"`
	Presence PresenceConf `yaml:"presence" json:"presence"`

	// DSAuth DS 回调服务令牌校验(审核 P1 #1)。player_locator 只做校验(verify-only):
	// Hub DS 经 :8444 调 SetLocation(HUB)/ReportDisconnect 须带 hub 令牌(绑 pod)。
	// mode 默认 off;authority_mode 默认 legacy。Model B 激活时部署层须与 hub_allocator 同步
	// 切 authority_mode=redis，才会启用 Redis active credential 终态门。
	DSAuth config.DSAuthConf `yaml:"ds_auth,omitempty" json:"ds_auth,omitempty"`
}

// ValidateDSAuthAuthorityMode 拒绝拼写错误的授权权威模式。若把预期的 redis 误写成
// 其它值却静默退化为 legacy，SetLocation/ReportDisconnect 会绕过 active credential 门。
func (c *Config) ValidateDSAuthAuthorityMode() error {
	switch c.DSAuth.AuthorityMode {
	case "legacy", "redis":
		return nil
	default:
		return fmt.Errorf("ds_auth.authority_mode invalid: %q (want legacy|redis)", c.DSAuth.AuthorityMode)
	}
}

// LocatorConf 是 player_locator 私有配置。
type LocatorConf struct {
	// LocationTTL Redis hash 的 TTL。默认 30s,对齐 infra.md §3.2 表中的 30s heartbeat。
	// W3 ⑥(2026-06-05):字段改用 config.Duration,etc yaml 可写 "30s" 字符串。
	LocationTTL config.Duration `yaml:"location_ttl,omitempty" json:"location_ttl,omitempty"`
}

// PresenceConf 是好友在线态订阅推送 fan-out 的配置
// (docs/design/friend-distributed-scaling.md §13.4 / §13.5)。
//
// 默认 Enabled=false:按 §13.7 顺序「先拉后推」,订阅推送是后续可选增强。
// 开启时需配 cfg.Kafka.Brokers(往 pandora.presence.update 生产)+(可选)killswitch。
type PresenceConf struct {
	// Enabled 是否开启订阅推送;false = 纯拉模式(SubscribePresence 变 no-op,不起 worker)。
	Enabled bool `yaml:"enabled,omitempty" json:"enabled,omitempty"`

	// DebounceWindow 上线去抖窗口(§13.4.2),默认 8s;窗口内回退到原状态判为抖动不推。
	DebounceWindow config.Duration `yaml:"debounce_window,omitempty" json:"debounce_window,omitempty"`

	// CoalesceTick 合并/flush tick 间隔(§13.4.3),默认 1s;同订阅者同 tick 内变更攒一批。
	CoalesceTick config.Duration `yaml:"coalesce_tick,omitempty" json:"coalesce_tick,omitempty"`

	// KillSwitchKey 洪峰降级开关 key(§13.5);ops 把该 key 写进 killswitch 规则即降为纯拉。
	// 默认 "presence/fanout"。
	KillSwitchKey string `yaml:"kill_switch_key,omitempty" json:"kill_switch_key,omitempty"`
}

// Defaults 填默认值。
func (c *Config) Defaults() {
	if c.Locator.LocationTTL == 0 {
		c.Locator.LocationTTL = config.Duration(30 * time.Second)
	}
	if c.Presence.DebounceWindow == 0 {
		c.Presence.DebounceWindow = config.Duration(8 * time.Second)
	}
	if c.Presence.CoalesceTick == 0 {
		c.Presence.CoalesceTick = config.Duration(1 * time.Second)
	}
	if c.Presence.KillSwitchKey == "" {
		c.Presence.KillSwitchKey = "presence/fanout"
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20006"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21006"
	}
	c.DSAuth.Defaults()
	if c.DSAuth.AuthorityMode == "" {
		c.DSAuth.AuthorityMode = "legacy"
	}
}
