// Package conf 是 chat 服务的私有配置结构(2026-06-16)。
package conf

import (
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/dbguard"
)

// Config 是 chat 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Chat ChatConf `yaml:"chat" json:"chat"`
}

// ChatConf 是 chat 服务私有配置。
type ChatConf struct {
	// MaxContentLen 单条消息最大字符数(按 utf8 rune 计,默认 256)。
	// 超长 → ErrChatMessageTooLong。
	MaxContentLen int `yaml:"max_content_len,omitempty" json:"max_content_len,omitempty"`

	// HistoryLimit PullHistory 单次返回上限(默认 50)。请求 limit 超过此值时按此值截断。
	HistoryLimit int `yaml:"history_limit,omitempty" json:"history_limit,omitempty"`

	// TeamAddr team 服务 gRPC 地址(host:port)。
	// 空 → 队伍频道无法解析成员,TEAM 消息静默降级(弱依赖)。
	TeamAddr string `yaml:"team_addr,omitempty" json:"team_addr,omitempty"`

	// GuildAddr guild 服务 gRPC 地址(host:port)。GuildService + GroupService 同进程共用此地址。
	// 空 → 公会 / 群频道无法解析成员,GUILD / GROUP 消息静默降级(弱依赖)。
	GuildAddr string `yaml:"guild_addr,omitempty" json:"guild_addr,omitempty"`

	// SensitiveWords 敏感词列表(命中后整词替换为等长 *,大小写不敏感)。
	// 默认空 → 不过滤。仅做最小化屏蔽,真正风控由独立服务接管(后续)。
	SensitiveWords []string `yaml:"sensitive_words,omitempty" json:"sensitive_words,omitempty"`

	// WorldCooldown 世界频道单玩家发送冷却(默认 3s;<=0 用默认,显式关闭配 -1ns 无意义,
	// 生产不应关)。压测审核【必修-5】:世界频道每条消息经 kafka 广播 topic 被每个 push Pod
	// Broadcast 一次,集群级成本 ≈ 发送速率 × 全服在线数;无冷却时单客户端刷屏即可拖垮
	// push 层的定向推送(READY/私聊)。冷却在 chat 生产侧统一做(push 每 Pod 独立 consumer
	// group,单 Pod 自限无效)。3s 取自常见世界频道口径,待实测复核。
	WorldCooldown config.Duration `yaml:"world_cooldown,omitempty" json:"world_cooldown,omitempty"`

	// ── 保留期清理(CLAUDE.md §9 不变量 24:只增表必须有界)──

	// HistoryRetentionDays 私聊历史(chat_private_messages)保留天数(默认 90)。
	// message_id 是雪花 ID(时间段单调),按 snowflake.MinIDAt(cutoff) 走主键范围删,无需新索引。
	HistoryRetentionDays int `yaml:"history_retention_days,omitempty" json:"history_retention_days,omitempty"`

	// SweepInterval 保留期清理轮询间隔(默认 5m)。多副本各自跑,DELETE 幂等无需锁。
	SweepInterval config.Duration `yaml:"sweep_interval,omitempty" json:"sweep_interval,omitempty"`

	// SweepBatch 每轮清理行数上限(默认 500,小批量防长事务锁表)。
	SweepBatch int `yaml:"sweep_batch,omitempty" json:"sweep_batch,omitempty"`

	// RetentionModeRaw 是保留期清理的执行模式(§9.24)。
	//
	// **留空 / "report_only" = 默认:只统计待清理量并 WARN 告警,一行都不删。**
	// "delete" = 真删。自动删除生产数据不可逆,清理条件/保留期/幂等窗口任一处配错都会
	// 静默删掉不该删的玩家数据且删完才发现,所以默认不删,由人确认后显式开启。
	// 不认识的值在 Validate 阶段 fail-fast(绝不猜成 delete)。
	RetentionModeRaw string `yaml:"retention_mode,omitempty" json:"retention_mode,omitempty"`
}

// RetentionMode 返回生效的清理模式(已在启动 Validate 校验过合法性;此处解析失败回落
// ModeReportOnly —— 任何不确定都不删)。
func (c *ChatConf) RetentionMode() dbguard.Mode {
	m, err := dbguard.ParseMode(c.RetentionModeRaw)
	if err != nil {
		return dbguard.ModeReportOnly
	}
	return m
}

// ValidateRetentionMode 供 main 启动时 fail-fast:配置写了无法识别的模式必须拒启,
// 而不是静默按 report_only 跑(运维以为开了删除,实际没开,库继续涨)。
func (c *ChatConf) ValidateRetentionMode() error {
	_, err := dbguard.ParseMode(c.RetentionModeRaw)
	return err
}

// Defaults 填默认值,防止 yaml 缺字段时零值引发非预期行为。
func (c *Config) Defaults() {
	if c.Chat.MaxContentLen <= 0 {
		c.Chat.MaxContentLen = 256
	}
	if c.Chat.HistoryLimit <= 0 {
		c.Chat.HistoryLimit = 50
	}
	if c.Chat.WorldCooldown <= 0 {
		c.Chat.WorldCooldown = config.Duration(3 * time.Second)
	}
	if c.Chat.HistoryRetentionDays <= 0 {
		c.Chat.HistoryRetentionDays = 90
	}
	if c.Chat.SweepInterval <= 0 {
		c.Chat.SweepInterval = config.Duration(5 * time.Minute)
	}
	if c.Chat.SweepBatch <= 0 {
		c.Chat.SweepBatch = 500
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20005"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21005"
	}
}
