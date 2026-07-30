// Package conf 是 auction 服务的私有配置结构(2026-06-19)。
package conf

import (
	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/dbguard"
)

// Config 是 auction 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Auction AuctionConf `yaml:"auction" json:"auction"`
}

// AuctionConf 是 auction 服务私有配置。
type AuctionConf struct {
	// MaxQuantityPerOrder 单挂单 / 出价最大数量(默认 1_000_000)。防一次挂天量。
	MaxQuantityPerOrder int64 `yaml:"max_quantity_per_order,omitempty" json:"max_quantity_per_order,omitempty"`

	// MaxPrice 单价上限(默认 1_000_000_000)。防溢出 / 异常价。
	MaxPrice int64 `yaml:"max_price,omitempty" json:"max_price,omitempty"`

	// MaxActiveOrdersPerPlayer 单玩家 PENDING+OPEN+PARTIALLY_FILLED 订单硬上限(默认 200)。
	// 跨 market/分片由 Redis Lua SCARD+SADD 原子预留。
	MaxActiveOrdersPerPlayer int `yaml:"max_active_orders_per_player,omitempty" json:"max_active_orders_per_player,omitempty"`

	// DefaultListLimit ListMarket 默认返回条数(默认 50)。
	DefaultListLimit int `yaml:"default_list_limit,omitempty" json:"default_list_limit,omitempty"`

	// MaxListLimit ListMarket 单次返回上限(默认 200)。
	MaxListLimit int `yaml:"max_list_limit,omitempty" json:"max_list_limit,omitempty"`

	// InventoryAddr 是 inventory 服务的内网 gRPC 地址(host:port,如 127.0.0.1:50015)。
	// 配了 → 成交走真实结算(SettleAuctionMatch:卖↔买资产原子对转 + match_id 幂等);
	// 留空 → 退回 NoopSettlementLedger(占位,总成功),仅供无交易联调 / 单测环境用。
	InventoryAddr string `yaml:"inventory_addr,omitempty" json:"inventory_addr,omitempty"`

	// AllowNoopSettlement 显式允许在 InventoryAddr 为空时退回 NoopSettlementLedger(占位,不真实扣转资产)。
	// 默认 false:InventoryAddr 缺失即 fail-fast,防止生产漏配 inventory 地址后仍静默以「成交不结算」启动。
	// 仅无交易联调 / 单测环境显式置 true。
	AllowNoopSettlement bool `yaml:"allow_noop_settlement,omitempty" json:"allow_noop_settlement,omitempty"`

	// AllowNoopMatchEvents 仅供明确不接 Kafka 的本地联调。默认 false：brokers 为空时启动失败，
	// 防止成交 outbox 在“事件被静默禁用”时仍被清除。配置了 brokers 但暂时不可用时不退出，
	// marker 保持 pending，由可重连 producer 后台重试。
	AllowNoopMatchEvents bool `yaml:"allow_noop_match_events,omitempty" json:"allow_noop_match_events,omitempty"`

	// PassiveWarmup 是蓝绿 R3 的只读预热门禁。true 时拒绝挂单/出价/撤单，并禁用
	// legacy verifier、持久副作用补偿和过期清扫；必须等所有旧 auction 完全下线后再以 false 重启。
	PassiveWarmup bool `yaml:"passive_warmup,omitempty" json:"passive_warmup,omitempty"`

	// ShardTopologyGeneration 与有序 DSN 身份共同写入每个物理 shard，默认 auction-v1。
	// 任何片数、顺序或目标库漂移都必须 fail-fast，不能靠修改本字段覆盖既有 marker。
	ShardTopologyGeneration string `yaml:"shard_topology_generation,omitempty" json:"shard_topology_generation,omitempty"`

	// AllowShardTopologyBootstrap 只授权“所有 marker 均不存在”的首次双分片登记。
	// 首次成功后必须恢复 false；已有 marker 不一致时该开关也绝不允许覆盖。
	AllowShardTopologyBootstrap bool `yaml:"allow_shard_topology_bootstrap,omitempty" json:"allow_shard_topology_bootstrap,omitempty"`

	// OrderTTLSeconds 挂单存活时长(秒)。> 0 → 启用过期清扫:创建超过 TTL 仍未成交的挂单
	// 自动置 EXPIRED、移出订单簿并退还 escrow(限制#1 补偿)。<= 0 → 不过期(永久挂单)。
	OrderTTLSeconds int64 `yaml:"order_ttl_seconds,omitempty" json:"order_ttl_seconds,omitempty"`

	// ExpirySweepIntervalSeconds 过期清扫扫描间隔(秒,默认 60)。仅 OrderTTLSeconds > 0 时生效。
	ExpirySweepIntervalSeconds int64 `yaml:"expiry_sweep_interval_seconds,omitempty" json:"expiry_sweep_interval_seconds,omitempty"`

	// ExpirySweepBatch 单次清扫最多处理的过期订单数(默认 200)。防一次扫太多阻塞。
	ExpirySweepBatch int `yaml:"expiry_sweep_batch,omitempty" json:"expiry_sweep_batch,omitempty"`

	// SideEffectReconcileIntervalSeconds 成交结算 / escrow 释放补偿扫描间隔(默认 5 秒)。
	// 外部账本调用都以 MySQL 持久意图为准，失败或进程退出后由该循环幂等重试。
	SideEffectReconcileIntervalSeconds int64 `yaml:"side_effect_reconcile_interval_seconds,omitempty" json:"side_effect_reconcile_interval_seconds,omitempty"`

	// SideEffectReconcileBatch 单轮每片最多处理的待结算、待释放和待事件记录数(各自默认 100)。
	SideEffectReconcileBatch int `yaml:"side_effect_reconcile_batch,omitempty" json:"side_effect_reconcile_batch,omitempty"`

	// AuditQueueCapacity 弱依赖订单审计的进程内异步队列上限(默认 1024)。
	// 队列满时只告警丢弃 audit，绝不能反压 market 锁或资产补偿主路径。
	AuditQueueCapacity int `yaml:"audit_queue_capacity,omitempty" json:"audit_queue_capacity,omitempty"`

	// CrossInstanceLock 保留给旧二进制读取；新二进制因 Redis 已是强依赖而始终启用跨实例锁。
	CrossInstanceLock bool `yaml:"cross_instance_lock,omitempty" json:"cross_instance_lock,omitempty"`

	// MarketLockTTLSeconds 跨实例 market 锁 TTL(秒,默认 30,上限 30,不变量 §10)。
	MarketLockTTLSeconds int64 `yaml:"market_lock_ttl_seconds,omitempty" json:"market_lock_ttl_seconds,omitempty"`

	// MarketLockMaxWaitMs 抢 market 锁最大等待(毫秒,默认 3000);超时返回 ERR_AUCTION_MARKET_BUSY。
	MarketLockMaxWaitMs int64 `yaml:"market_lock_max_wait_ms,omitempty" json:"market_lock_max_wait_ms,omitempty"`

	// ── 保留期清理(CLAUDE.md §9 不变量 24:只增表必须有界)──

	// RetentionDays 终态挂单(FILLED/CANCELED/EXPIRED 且 escrow 已释放)、已结算成交流水
	// (settlement/event 均完成)与超期幂等键映射(auction_idempotency_keys)的保留天数(默认 90)。
	// 远大于挂单/结算重试窗口(分钟级);ListMyOrders 历史窗口同步受此限。
	RetentionDays int `yaml:"retention_days,omitempty" json:"retention_days,omitempty"`

	// RetentionSweepIntervalSeconds 保留期清理扫描间隔(秒,默认 3600)。逐分片跑,DELETE 幂等无需锁。
	RetentionSweepIntervalSeconds int64 `yaml:"retention_sweep_interval_seconds,omitempty" json:"retention_sweep_interval_seconds,omitempty"`

	// RetentionSweepBatch 每轮每分片每表清理行数上限(默认 500)。
	RetentionSweepBatch int `yaml:"retention_sweep_batch,omitempty" json:"retention_sweep_batch,omitempty"`

	// RetentionModeRaw 保留期清理模式:留空 / "report_only" = 默认只报告不删;"delete" = 真删。
	// 「因为数据大了就自动删」不可接受(§9.24 + 2026-07-22 用户指令);挂单过期置 EXPIRED
	// 那类**业务语义**流转不受此约束(那是订单本来就该失效,不是"数据太多要删")。
	RetentionModeRaw string `yaml:"retention_mode,omitempty" json:"retention_mode,omitempty"`
}

// Defaults 填默认值,防止 yaml 缺字段时零值引发非预期行为。
func (c *Config) Defaults() {
	if c.Auction.ShardTopologyGeneration == "" {
		c.Auction.ShardTopologyGeneration = "auction-v1"
	}
	if c.Auction.MaxQuantityPerOrder <= 0 {
		c.Auction.MaxQuantityPerOrder = 1_000_000
	}
	if c.Auction.MaxPrice <= 0 {
		c.Auction.MaxPrice = 1_000_000_000
	}
	if c.Auction.MaxActiveOrdersPerPlayer <= 0 {
		c.Auction.MaxActiveOrdersPerPlayer = 200
	}
	if c.Auction.DefaultListLimit <= 0 {
		c.Auction.DefaultListLimit = 50
	}
	if c.Auction.MaxListLimit <= 0 {
		c.Auction.MaxListLimit = 200
	}
	if c.Auction.ExpirySweepIntervalSeconds <= 0 {
		c.Auction.ExpirySweepIntervalSeconds = 60
	}
	if c.Auction.ExpirySweepBatch <= 0 {
		c.Auction.ExpirySweepBatch = 200
	}
	if c.Auction.SideEffectReconcileIntervalSeconds <= 0 {
		c.Auction.SideEffectReconcileIntervalSeconds = 5
	}
	if c.Auction.SideEffectReconcileBatch <= 0 {
		c.Auction.SideEffectReconcileBatch = 100
	}
	if c.Auction.AuditQueueCapacity <= 0 {
		c.Auction.AuditQueueCapacity = 1024
	}
	if c.Auction.MarketLockTTLSeconds <= 0 || c.Auction.MarketLockTTLSeconds > 30 {
		c.Auction.MarketLockTTLSeconds = 30 // 不变量 §10:Redis lock TTL ≤ 30s
	}
	if c.Auction.MarketLockMaxWaitMs <= 0 {
		c.Auction.MarketLockMaxWaitMs = 3000
	}
	if c.Auction.RetentionDays <= 0 {
		c.Auction.RetentionDays = 90
	}
	if c.Auction.RetentionSweepIntervalSeconds <= 0 {
		c.Auction.RetentionSweepIntervalSeconds = 3600
	}
	if c.Auction.RetentionSweepBatch <= 0 {
		c.Auction.RetentionSweepBatch = 500
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":50016"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":51016"
	}
}

// RetentionMode 返回生效的保留期清理模式(默认 ModeReportOnly = 只报告不删)。
func (a *AuctionConf) RetentionMode() dbguard.Mode {
	m, err := dbguard.ParseMode(a.RetentionModeRaw)
	if err != nil {
		return dbguard.ModeReportOnly
	}
	return m
}

// ValidateRetentionMode 供启动 fail-fast(写了无法识别的模式必须拒启)。
func (a *AuctionConf) ValidateRetentionMode() error {
	_, err := dbguard.ParseMode(a.RetentionModeRaw)
	return err
}
