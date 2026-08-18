// Package conf — owner 服务私有配置(owner-authority.md)。
package conf

import (
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
)

// Config 是 owner 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Owner OwnerConf `yaml:"owner" json:"owner"`
}

// OwnerConf owner 权威私有配置。
//
// 注意:fence/lease 协议常量不在配置里(单一来源 pkg/placement,正确性常量禁调优);
// 本配置只管审计保留与清理节奏。
type OwnerConf struct {
	// RequireTiDB 启动时强校验权威库确为 TiDB(§9.22:MySQL 异步复制切换会回滚已确认写,
	// owner CAS 回滚即可能双 owner,生产禁用)。-Prod 产物由 gen_cluster_config.ps1 机械
	// 注入 true;dev 模板保持 false(单机 MySQL 无复制,天然线性一致)。
	RequireTiDB bool `yaml:"require_tidb,omitempty" json:"require_tidb,omitempty"`

	// SweepInterval 审计流水清理轮询间隔(默认 5m;多副本各自跑,DELETE 幂等)。
	SweepInterval config.Duration `yaml:"sweep_interval,omitempty" json:"sweep_interval,omitempty"`

	// SweepBatch 每轮清理行数上限(默认 500)。
	SweepBatch int `yaml:"sweep_batch,omitempty" json:"sweep_batch,omitempty"`

	// LogRetentionDays owner_transition_log 保留天数(默认 90,§9.24)。
	LogRetentionDays int `yaml:"log_retention_days,omitempty" json:"log_retention_days,omitempty"`

	// RejectLegacySourceRevision 打开**全局** legacy(hub source revision=0)拒绝门
	// (INC-20260818-003 §3 分阶段发布的最后一步)。
	//
	// 默认 false = 兼容窗:允许尚未滚上本协议的 hub_allocator 不带来源版本写入。
	// **只有在证明旧 hub_allocator 副本已完全排空之后**才可置 true;提前打开会让仍在跑的
	// 旧副本全部写失败 = 大厅分配停摆。
	//
	// 逐玩家的那条规则(见过非零版本就永久拒 legacy)不受本开关控制,它从第一个新写者
	// 写下第一个非零版本起自动生效 —— 那条不需要人来拍时机,所以不做成开关。
	RejectLegacySourceRevision bool `yaml:"reject_legacy_source_revision,omitempty" json:"reject_legacy_source_revision,omitempty"`
}

// Defaults 填默认值。
func (c *Config) Defaults() {
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20017"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21017"
	}
	if c.Owner.SweepInterval <= 0 {
		c.Owner.SweepInterval = config.Duration(5 * time.Minute)
	}
	if c.Owner.SweepBatch <= 0 {
		c.Owner.SweepBatch = 500
	}
	if c.Owner.LogRetentionDays <= 0 {
		c.Owner.LogRetentionDays = 90
	}
}
