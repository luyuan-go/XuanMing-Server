// Package conf 是 player 服务的私有配置结构(W4 ④,2026-06-06)。
package conf

import (
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/kafkax"
)

// Config 是 player 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Player PlayerConf `yaml:"player" json:"player"`
}

// PlayerConf 是 player 服务私有配置。
type PlayerConf struct {
	// BaseMMR 新玩家缺省 MMR(EnsureProfile / GetMMR 未建档兜底,默认 1500,与 battle_result 对齐)。
	BaseMMR int `yaml:"base_mmr,omitempty" json:"base_mmr,omitempty"`

	// MMRFloor MMR 下限(UpdateMMR 后 clamp,默认 0)。
	MMRFloor int `yaml:"mmr_floor,omitempty" json:"mmr_floor,omitempty"`

	// DefaultNicknamePrefix 默认昵称前缀(EnsureProfile 建档时 nickname=prefix+player_id,保证 uk 唯一,默认 "Player_")。
	DefaultNicknamePrefix string `yaml:"default_nickname_prefix,omitempty" json:"default_nickname_prefix,omitempty"`

	// MaxNicknameLen 昵称最大长度(UpdateNickname 校验,默认 32)。
	MaxNicknameLen int `yaml:"max_nickname_len,omitempty" json:"max_nickname_len,omitempty"`

	// HeroSelectionEnabled 出战英雄选择功能开关(默认 false,demo 阶段跳过选英雄,
	// 与 login demo-skip 风格一致;关闭时 SelectHero 返回 ERR_PLAYER_FEATURE_DISABLED)。
	HeroSelectionEnabled bool `yaml:"hero_selection_enabled,omitempty" json:"hero_selection_enabled,omitempty"`

	// LoadoutCustomizeEnabled 出战装备预设 / 天赋树自定义功能开关(默认 false;
	// 关闭时 SetEquipment / SetTalents / ResetTalents 返回 ERR_PLAYER_FEATURE_DISABLED;
	// 授予类 GrantTalentPoints 由系统驱动不受此开关影响)。
	//
	// 2026-07-25:2026-06-17 审查提出的三项校验已补齐,本开关不再是唯一防线——
	//   - isEquip / slotMatch:读 configtable 道具表(d_道具.xlsx「装备部位」列),零 RPC;
	//   - ownEquipment:经 inventory.CheckItemsOwned 系统 RPC 确认持有(见 InventoryAddr);
	//   - SetTalents:读 configtable 专精表校验等级上限 / 前置 / 总消耗。
	// 三条依赖任一缺失(表未加载 / InventoryAddr 未配)一律 fail-closed 拒绝,不会退化成放行。
	// 因此打开本开关的前提是 **config_table.dir 已就绪 + inventory_addr 已配置**。
	LoadoutCustomizeEnabled bool `yaml:"loadout_customize_enabled,omitempty" json:"loadout_customize_enabled,omitempty"`

	// InventoryAddr inventory 服务 gRPC 端点(host:port,内网直连无 JWT)。
	// SetEquipment 的拥有权校验依赖它;留空 = 拥有权校验器不接线,SetEquipment 直接
	// fail-closed 返回内部错误(不会静默跳过校验)。LoadoutCustomizeEnabled=true 时必配。
	InventoryAddr string `yaml:"inventory_addr,omitempty" json:"inventory_addr,omitempty"`

	// ConsumeTopics 本服订阅的 kafka topic(默认 [player.update])。
	ConsumeTopics []string `yaml:"consume_topics,omitempty" json:"consume_topics,omitempty"`

	// ExperienceEnabled 玩家经验入账开关。曲线始终来自 configtable；本开关只控制功能放行，
	// 不再兼任数值载体。策划正式数值确认前生产保持 false。
	ExperienceEnabled bool `yaml:"experience_enabled,omitempty" json:"experience_enabled,omitempty"`

	// MaxExpPerGrant 单次 AddExperience 入账上限(默认 1000000)。
	// 防异常 / 越权调用方一次灌满等级(DS 不可信纵深:battle_result 已按怪物表换算,
	// 这里是 player 侧最后一道兜底)。
	MaxExpPerGrant uint64 `yaml:"max_exp_per_grant,omitempty" json:"max_exp_per_grant,omitempty"`

	// PushOutboxInterval 经验推送出箱发布轮询间隔(默认 1s;经验条刷新体感由它决定上界)。
	PushOutboxInterval config.Duration `yaml:"push_outbox_interval,omitempty" json:"push_outbox_interval,omitempty"`

	// PushOutboxBatch 每轮发布取多少条推送出箱记录(默认 128)。
	PushOutboxBatch int `yaml:"push_outbox_batch,omitempty" json:"push_outbox_batch,omitempty"`

	// ExpHistoryCleanupEnabled 经验幂等收据(exp_history)后台清理开关(**默认 false=不清理**,
	// 审计 P1:battle_result progress 出箱只有退避上限(5min)没有总重试期限——入账成功但
	// 响应/删行持续失败超过留存期后,同一事件会被再次入账(双发)。开启前置条件:上游出箱
	// 必须先有小于留存期的有界重试/隔离期限,否则收据表宁可增长也不能破坏幂等,§9.2)。
	ExpHistoryCleanupEnabled bool `yaml:"exp_history_cleanup_enabled,omitempty" json:"exp_history_cleanup_enabled,omitempty"`

	// ExpHistoryRetention 经验幂等收据(exp_history)留存期(默认 7 天,下限 7 天:
	// 必须严格覆盖 battle_result progress 出箱最长重试窗;仅在 cleanup 开关开启时生效)。
	ExpHistoryRetention config.Duration `yaml:"exp_history_retention,omitempty" json:"exp_history_retention,omitempty"`

	// HistoryCleanupEnabled mmr_history / attr_point_grants / talent_point_grants 幂等
	// 历史行后台清理开关(**默认 false=不清理**,与 exp_history 同理由:上游 kafka
	// player.update 消费与授予补扫是 at-least-once,清掉幂等行后同一事件重放 = 双发
	// (重复加段位分/重复加点)。开启前置条件:上游重放期限(kafka retention / 补扫窗口)
	// 必须小于留存期,由运维确认后配置。CLAUDE.md §9 不变量 24)。
	HistoryCleanupEnabled bool `yaml:"history_cleanup_enabled,omitempty" json:"history_cleanup_enabled,omitempty"`

	// HistoryRetentionDays mmr_history / 点数授予幂等表留存天数(默认 90,下限 30:
	// 必须远大于 kafka retention 与一切授予补扫窗口;仅在 cleanup 开关开启时生效)。
	HistoryRetentionDays int `yaml:"history_retention_days,omitempty" json:"history_retention_days,omitempty"`
}

// Defaults 填默认值。
func (c *Config) Defaults() {
	if c.Player.BaseMMR <= 0 {
		c.Player.BaseMMR = 1500
	}
	if c.Player.MMRFloor < 0 {
		c.Player.MMRFloor = 0
	}
	if c.Player.DefaultNicknamePrefix == "" {
		c.Player.DefaultNicknamePrefix = "Player_"
	}
	if c.Player.MaxNicknameLen <= 0 {
		c.Player.MaxNicknameLen = 32
	}
	if len(c.Player.ConsumeTopics) == 0 {
		c.Player.ConsumeTopics = []string{kafkax.TopicPlayerUpdate}
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":50002"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":51002"
	}
}

// MaxExpPerGrantOrDefault 返回生效的单次入账上限(未配置 → 1000000)。
func (p *PlayerConf) MaxExpPerGrantOrDefault() uint64 {
	if p.MaxExpPerGrant > 0 {
		return p.MaxExpPerGrant
	}
	return 1_000_000
}

// PushOutboxIntervalOrDefault 返回生效的推送出箱轮询间隔(未配置 → 1s)。
func (p *PlayerConf) PushOutboxIntervalOrDefault() time.Duration {
	if d := p.PushOutboxInterval.Std(); d > 0 {
		return d
	}
	return time.Second
}

// PushOutboxBatchOrDefault 返回生效的推送出箱批大小(未配置 → 128)。
func (p *PlayerConf) PushOutboxBatchOrDefault() int {
	if p.PushOutboxBatch > 0 {
		return p.PushOutboxBatch
	}
	return 128
}

// ExpHistoryRetentionOrDefault 返回生效的 exp_history 留存期(未配置 → 7 天;
// 配置低于 7 天按 7 天,防手滑把幂等窗清穿)。
func (p *PlayerConf) ExpHistoryRetentionOrDefault() time.Duration {
	const (
		min = 7 * 24 * time.Hour
		cap = 90 * 24 * time.Hour // §9.24 硬上限:失效数据最多保留 90 天(审计 P1:上限必须钳制,不能只信配置)
	)
	d := p.ExpHistoryRetention.Std()
	if d < min {
		return min
	}
	if d > cap {
		return cap
	}
	return d
}

// HistoryRetentionOrDefault 返回生效的 mmr/点数授予幂等历史留存期(未配置 → 90 天;
// 低于 30 天按 30 天,防手滑把幂等窗清穿;高于 90 天按 90 天,§9.24 硬上限)。
// 先钳**天数整数**再乘 Duration(审计 P1:先乘后判时,极大天数乘 24h 溢出为负,
// 会误落 floor 分支返回 30 天,清理开启时提前删幂等收据)。
func (p *PlayerConf) HistoryRetentionOrDefault() time.Duration {
	days := p.HistoryRetentionDays
	if days <= 0 {
		days = 90
	}
	if days < 30 {
		days = 30
	}
	if days > 90 {
		days = 90
	}
	return time.Duration(days) * 24 * time.Hour
}
