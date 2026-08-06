// Package conf 是 leaderboard 服务的私有配置结构(2026-06-27)。
package conf

import (
	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/dbguard"
)

// Config 是 leaderboard 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Leaderboard LeaderboardConf `yaml:"leaderboard" json:"leaderboard"`
}

// LeaderboardConf 是 leaderboard 服务私有配置。
type LeaderboardConf struct {
	// DefaultListLimit GetRange 默认返回条数(默认 50)。
	DefaultListLimit int `yaml:"default_list_limit,omitempty" json:"default_list_limit,omitempty"`

	// MaxListLimit GetRange / GetAround 单次返回上限(默认 200)。
	MaxListLimit int `yaml:"max_list_limit,omitempty" json:"max_list_limit,omitempty"`

	// DefaultAroundRadius GetAround 默认上下名数(默认 10)。
	DefaultAroundRadius int `yaml:"default_around_radius,omitempty" json:"default_around_radius,omitempty"`

	// DefaultSettleTopN SettleBoard 未指定 top_n 时默认结算前 N 名(默认 100)。
	DefaultSettleTopN int `yaml:"default_settle_top_n,omitempty" json:"default_settle_top_n,omitempty"`

	// DefaultEstimateBucketWidth 建榜未指定 estimate_bucket_width 时的直方图桶宽默认值
	// (默认 25,MMR 量纲;榜外名次区间估算用,建榜后不可变)。
	DefaultEstimateBucketWidth int64 `yaml:"default_estimate_bucket_width,omitempty" json:"default_estimate_bucket_width,omitempty"`

	// InventoryAddr 是 inventory 服务的内网 gRPC 地址(host:port,如 127.0.0.1:20015)。
	// 配了 → 结算发奖走真实 GrantItems(幂等键 lb:<settlement_id>:<entity_id>);
	// 留空 → 退回 NoopRewardGranter(占位,不真实发奖),仅供无背包联调 / 单测环境用。
	InventoryAddr string `yaml:"inventory_addr,omitempty" json:"inventory_addr,omitempty"`

	// AllowNoopReward 显式允许在 InventoryAddr 为空时退回 NoopRewardGranter(不真实发奖)。
	// 默认 false:InventoryAddr 缺失即 fail-fast,防生产漏配后静默以「结算不发奖」启动。
	AllowNoopReward bool `yaml:"allow_noop_reward,omitempty" json:"allow_noop_reward,omitempty"`

	// ── 保留期清理(CLAUDE.md §9 不变量 24:只增表必须有界)──

	// RetentionDays 名次快照(leaderboard_snapshot)与已发放发奖记录(reward_log GRANTED)
	// 保留天数(默认 90)。leaderboard_settlement 故意不清:settle uk 是防重复结算的永久闸,
	// 每批次仅 1 行,慢增长登记豁免(§9.24 清单)。
	RetentionDays int `yaml:"retention_days,omitempty" json:"retention_days,omitempty"`

	// RetentionSweepBatch 每轮每表清理行数上限(默认 500)。清理与发奖补扫共用扫描节拍。
	RetentionSweepBatch int `yaml:"retention_sweep_batch,omitempty" json:"retention_sweep_batch,omitempty"`

	// RetentionModeRaw 保留期清理模式:留空 / "report_only" = 默认只报告不删;"delete" = 真删。
	RetentionModeRaw string `yaml:"retention_mode,omitempty" json:"retention_mode,omitempty"`
}

// Defaults 填默认值,防止 yaml 缺字段时零值引发非预期行为。
func (c *Config) Defaults() {
	if c.Leaderboard.DefaultListLimit <= 0 {
		c.Leaderboard.DefaultListLimit = 50
	}
	if c.Leaderboard.MaxListLimit <= 0 {
		c.Leaderboard.MaxListLimit = 200
	}
	if c.Leaderboard.DefaultAroundRadius <= 0 {
		c.Leaderboard.DefaultAroundRadius = 10
	}
	if c.Leaderboard.DefaultSettleTopN <= 0 {
		c.Leaderboard.DefaultSettleTopN = 100
	}
	if c.Leaderboard.DefaultEstimateBucketWidth <= 0 {
		c.Leaderboard.DefaultEstimateBucketWidth = 25
	}
	if c.Leaderboard.RetentionDays <= 0 {
		c.Leaderboard.RetentionDays = 90
	}
	if c.Leaderboard.RetentionSweepBatch <= 0 {
		c.Leaderboard.RetentionSweepBatch = 500
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20007"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21007"
	}
}

// RetentionMode 返回生效的保留期清理模式(默认 ModeReportOnly = 只报告不删)。
func (l *LeaderboardConf) RetentionMode() dbguard.Mode {
	m, err := dbguard.ParseMode(l.RetentionModeRaw)
	if err != nil {
		return dbguard.ModeReportOnly
	}
	return m
}

// ValidateRetentionMode 供启动 fail-fast(写了无法识别的模式必须拒启)。
func (l *LeaderboardConf) ValidateRetentionMode() error {
	_, err := dbguard.ParseMode(l.RetentionModeRaw)
	return err
}
