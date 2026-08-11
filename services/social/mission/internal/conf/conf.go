// Package conf 是 mission 服务的私有配置结构(2026-08-11)。
package conf

import (
	"fmt"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/dbguard"
)

// Config 是 mission 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	ConfigTable ConfigTableConf `yaml:"config_table" json:"config_table"`
	Mission     MissionConf     `yaml:"mission" json:"mission"`
}

// ConfigTableConf 配置表加载(不变量 §9.15;对齐 matchmaker)。
type ConfigTableConf struct {
	// Dir 配置表 active 目录。mission 的接取校验 / 进度判定 / 发奖内容全部读表,
	// 表是本服务的启动强依赖:必配,加载失败 fail-closed 拒启。
	Dir string `yaml:"dir,omitempty" json:"dir,omitempty"`
}

// MissionConf 是 mission 服务私有配置。
type MissionConf struct {
	// MaxActiveMissions 单玩家活跃任务数上限(默认 50,不变量 §9.18 写入侧上限)。
	// AcceptMission 与完成扇出自动接链在同一事务内校验;自动接链超限跳过该条不阻断。
	MaxActiveMissions int `yaml:"max_active_missions,omitempty" json:"max_active_missions,omitempty"`

	// MaxFactsPerReport 单次 ReportMissionFacts 的事实条数上限(默认 64)。
	// 上游 battle_result 按批聚合,正常远小于此;超限 ERR_INVALID_ARG(§9.18 读写边界)。
	MaxFactsPerReport int `yaml:"max_facts_per_report,omitempty" json:"max_facts_per_report,omitempty"`

	// ── 发奖下游(docs/design/mission.md §6;对齐 leaderboard granter)──

	// InventoryAddr inventory 服务 gRPC 地址(道具/装备发放,内网 insecure 直连)。
	InventoryAddr string `yaml:"inventory_addr,omitempty" json:"inventory_addr,omitempty"`
	// PlayerAddr player 服务 gRPC 地址(经验发放 AddExperience)。
	PlayerAddr string `yaml:"player_addr,omitempty" json:"player_addr,omitempty"`
	// MailAddr mail 服务 gRPC 地址(装备发放满包时溢出转邮件;空 = 满包发放失败留补扫)。
	MailAddr string `yaml:"mail_addr,omitempty" json:"mail_addr,omitempty"`
	// AllowNoopReward 只允许 dev 骨架联调置 true:发奖下游地址缺失时退化为 no-op granter
	// (奖励只落流水不发内容,补扫恒失败)。默认 false:InventoryAddr / PlayerAddr 缺失拒启
	// (发奖链是任务域的交付承诺,静默 no-op 比起不来更糟 —— 对齐 leaderboard allow_noop_reward)。
	AllowNoopReward bool `yaml:"allow_noop_reward,omitempty" json:"allow_noop_reward,omitempty"`

	// RewardRetryInterval 发奖补扫轮询间隔(默认 1m;对齐 leaderboard RetryUngrantedRewards)。
	RewardRetryInterval config.Duration `yaml:"reward_retry_interval,omitempty" json:"reward_retry_interval,omitempty"`
	// RewardRetryGrace 补扫只处理更新时间早于本时长的行(默认 2m,挡住刚创建还在同步发的批次)。
	RewardRetryGrace config.Duration `yaml:"reward_retry_grace,omitempty" json:"reward_retry_grace,omitempty"`
	// RewardRetryBatch 补扫单轮行数上限(默认 200)。
	RewardRetryBatch int `yaml:"reward_retry_batch,omitempty" json:"reward_retry_batch,omitempty"`

	// ── 推送出箱(kafka pandora.mission.update → push)──

	// PushPublishInterval 推送出箱发布轮询间隔(默认 1s)。
	PushPublishInterval config.Duration `yaml:"push_publish_interval,omitempty" json:"push_publish_interval,omitempty"`
	// PushPublishBatch 单轮发布行数上限(默认 128;FIFO,失败中断本轮保序)。
	PushPublishBatch int `yaml:"push_publish_batch,omitempty" json:"push_publish_batch,omitempty"`

	// ── 保留期清理(CLAUDE.md §9 不变量 24)──

	// RewardLogRetentionDays mission_reward_log 已发放(GRANTED)行保留天数(默认 90)。
	// PENDING/FAILED 是补发工作集,永不清理。
	RewardLogRetentionDays int `yaml:"reward_log_retention_days,omitempty" json:"reward_log_retention_days,omitempty"`

	// ReceiptRetentionDays mission_fact_receipts 保留天数(默认 90)。
	ReceiptRetentionDays int `yaml:"receipt_retention_days,omitempty" json:"receipt_retention_days,omitempty"`
	// ReceiptCleanupEnabled 收据清理组级闸(默认 false = 连报告都只按 reward_log 走)。
	// 默认关的原因(同 player exp_history):上游 battle_progress_outbox 的重试没有总期限,
	// 删收据后迟到重放会把同一批事实**双计**进任务进度;上游重试有界之前不得开启。
	// 开启后仍受 retention_mode 约束(report_only 只报告)。
	ReceiptCleanupEnabled bool `yaml:"receipt_cleanup_enabled,omitempty" json:"receipt_cleanup_enabled,omitempty"`

	// SweepInterval 保留期清理轮询间隔(默认 5m)。多副本各自跑,DELETE 幂等无需锁。
	SweepInterval config.Duration `yaml:"sweep_interval,omitempty" json:"sweep_interval,omitempty"`
	// SweepBatch 每轮清理行数上限(默认 500)。
	SweepBatch int `yaml:"sweep_batch,omitempty" json:"sweep_batch,omitempty"`
	// RetentionModeRaw 保留期清理模式:留空 / "report_only" = 默认只报告不删;"delete" = 真删。
	RetentionModeRaw string `yaml:"retention_mode,omitempty" json:"retention_mode,omitempty"`
}

// Defaults 填默认值,防止 yaml 缺字段时零值引发非预期行为。
func (c *Config) Defaults() {
	if c.Mission.MaxActiveMissions <= 0 {
		c.Mission.MaxActiveMissions = 50
	}
	if c.Mission.MaxFactsPerReport <= 0 {
		c.Mission.MaxFactsPerReport = 64
	}
	if c.Mission.RewardRetryInterval <= 0 {
		c.Mission.RewardRetryInterval = config.Duration(time.Minute)
	}
	if c.Mission.RewardRetryGrace <= 0 {
		c.Mission.RewardRetryGrace = config.Duration(2 * time.Minute)
	}
	if c.Mission.RewardRetryBatch <= 0 {
		c.Mission.RewardRetryBatch = 200
	}
	if c.Mission.PushPublishInterval <= 0 {
		c.Mission.PushPublishInterval = config.Duration(time.Second)
	}
	if c.Mission.PushPublishBatch <= 0 {
		c.Mission.PushPublishBatch = 128
	}
	if c.Mission.RewardLogRetentionDays <= 0 {
		c.Mission.RewardLogRetentionDays = 90
	}
	if c.Mission.ReceiptRetentionDays <= 0 {
		c.Mission.ReceiptRetentionDays = 90
	}
	if c.Mission.SweepInterval <= 0 {
		c.Mission.SweepInterval = config.Duration(5 * time.Minute)
	}
	if c.Mission.SweepBatch <= 0 {
		c.Mission.SweepBatch = 500
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20019"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21019"
	}
}

// Validate 启动期硬校验(fail-fast;缺强依赖起来了也只是慢性故障)。
func (c *Config) Validate() error {
	if c.ConfigTable.Dir == "" {
		return fmt.Errorf("config_table.dir 必配:任务校验/进度判定/发奖内容全部读表(§9.15)")
	}
	if !c.Mission.AllowNoopReward {
		if c.Mission.InventoryAddr == "" {
			return fmt.Errorf("mission.inventory_addr 必配(发奖链交付承诺);dev 骨架联调可置 allow_noop_reward=true")
		}
		if c.Mission.PlayerAddr == "" {
			return fmt.Errorf("mission.player_addr 必配(经验发放);dev 骨架联调可置 allow_noop_reward=true")
		}
	}
	return nil
}

// RetentionMode 返回生效的保留期清理模式(默认 ModeReportOnly = 只报告不删)。
func (c *MissionConf) RetentionMode() dbguard.Mode {
	m, err := dbguard.ParseMode(c.RetentionModeRaw)
	if err != nil {
		return dbguard.ModeReportOnly
	}
	return m
}

// ValidateRetentionMode 供 main 启动 fail-fast(写了无法识别的模式必须拒启)。
func (c *MissionConf) ValidateRetentionMode() error {
	_, err := dbguard.ParseMode(c.RetentionModeRaw)
	return err
}
