// Package conf 是 battle_result 服务的私有配置结构(W4 ③,2026-06-06)。
package conf

import (
	"fmt"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/kafkax"
)

// Config 是 battle_result 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Battle BattleConf `yaml:"battle" json:"battle"`

	// DSAuth DS 回调服务令牌校验(审核 P1 #1)。battle_result 只做校验(verify-only):
	// Battle DS 经 :8444 调 ReportResult 须带 battle 令牌(绑 match_id,ds_allocator 签发)。
	// mode 默认 off;UE DS 带令牌后先 permissive 观察再 enforce。
	DSAuth config.DSAuthConf `yaml:"ds_auth,omitempty" json:"ds_auth,omitempty"`
}

// BattleConf 是 battle_result 服务私有配置。
type BattleConf struct {
	// EloKFactor Elo K 系数(默认 32)。胜负 MMR 变化幅度上限 ≈ K。
	EloKFactor int `yaml:"elo_k_factor,omitempty" json:"elo_k_factor,omitempty"`

	// BaseMMR 玩家缺省 MMR(W4 ③ player 服务未上线 → StaticMMRReader 全返此值,默认 1500)。
	BaseMMR int `yaml:"base_mmr,omitempty" json:"base_mmr,omitempty"`

	// ConsumeTopics 本服订阅的 kafka topic(legacy 默认 [battle.result, ds.lifecycle])。
	// Redis authority 只能显式配置 [ds.lifecycle]；battle.result 无凭据，会被启动校验拒绝。
	ConsumeTopics []string `yaml:"consume_topics,omitempty" json:"consume_topics,omitempty"`

	// PlayerAddr player 服务 gRPC 地址(弱依赖:空 → 用 BaseMMR 静态 reader)。
	// W4 ③ player 未上线,留空;player 上线后填地址接真实当前 MMR。
	PlayerAddr string `yaml:"player_addr,omitempty" json:"player_addr,omitempty"`

	// MatchmakerAddr matchmaker 服务 gRPC 地址(弱依赖:空 → 不通知 matchmaker 释放撮合状态)。
	// 用于结算/废弃落库后调 matchmaker.ReleaseMatch,释放残留 player→ticket claim + 票据 +
	// match 镜像,修复"结算返回 Hub 后玩家无法再次匹配(StartMatch 4002)"。
	MatchmakerAddr string `yaml:"matchmaker_addr,omitempty" json:"matchmaker_addr,omitempty"`

	// OutboxPublishInterval player.update 出箱发布轮询间隔(W4 ⑨,默认 2s)。
	OutboxPublishInterval config.Duration `yaml:"outbox_publish_interval,omitempty" json:"outbox_publish_interval,omitempty"`

	// OutboxBatchSize 每轮发布取多少条出箱记录(默认 128)。
	OutboxBatchSize int `yaml:"outbox_batch_size,omitempty" json:"outbox_batch_size,omitempty"`

	// DSAllocatorAddr 终态回收 relay 的内部 gRPC 地址。authority_mode=redis 时为强依赖；
	// worker 只向不暴露在 :8444 的 ReleaseBattle 发送 MySQL 持久证明。
	DSAllocatorAddr string `yaml:"ds_allocator_addr,omitempty" json:"ds_allocator_addr,omitempty"`

	// TerminalReleaseInterval 持久终态回收 outbox 的轮询间隔(默认 2s)。
	TerminalReleaseInterval config.Duration `yaml:"terminal_release_interval,omitempty" json:"terminal_release_interval,omitempty"`

	// TerminalReleaseBatchSize 每轮最多处理的终态回收行数(默认 128)。
	TerminalReleaseBatchSize int `yaml:"terminal_release_batch_size,omitempty" json:"terminal_release_batch_size,omitempty"`

	// TerminalReleaseGrace ReportResult 事务提交后，强制 UID 回收前留给 DS 收响应、
	// 通知客户端回大厅的宽限窗(默认 15s)。资源安全不依赖 DS ended ACK；响应丢失时
	// 宽限到期仍由 worker 回收。
	TerminalReleaseGrace config.Duration `yaml:"terminal_release_grace,omitempty" json:"terminal_release_grace,omitempty"`

	// ── 战斗装备掉落回写 W5 ④ ──

	// InventoryAddr inventory 服务 gRPC 地址(弱依赖:空 → 关闭掉落回写,不发放战斗装备掉落)。
	// 内网 insecure 直连(系统接口,无 JWT)。RunDropPublisher 用它调 GrantInstances。
	InventoryAddr string `yaml:"inventory_addr,omitempty" json:"inventory_addr,omitempty"`

	// DropWhitelist 允许作为战斗掉落落库的装备 item_config_id 白名单(DS 不可信,§12)。
	// 空 = 不放行任何掉落(安全默认:DS 上报的 dropped_item_config_ids 全被过滤掉,不发放)。
	// battle_result 写 drop 出箱前按此过滤,DS 只能触发白名单内装备落库。
	DropWhitelist []uint32 `yaml:"drop_whitelist,omitempty" json:"drop_whitelist,omitempty"`

	// MaxDropPerPlayer 单场结算里单个玩家最多入库的掉落条数(DS 不可信:防异常/恶意 DS
	// 重复上报海量白名单 ID 撑爆 battle_drop_outbox.item_config_ids VARCHAR(512),导致整场
	// 结算 insert 回滚)。默认 32;硬上限 46(46 个 10 位 uint32 + 逗号 = 505 字符,再多必超列宽)。
	// 超限部分被截断丢弃并记 Warn(battle_drop_truncated),不影响结算落库。
	MaxDropPerPlayer int `yaml:"max_drop_per_player,omitempty" json:"max_drop_per_player,omitempty"`

	// DropPublishInterval 战斗掉落出箱发布轮询间隔(默认 2s)。
	DropPublishInterval config.Duration `yaml:"drop_publish_interval,omitempty" json:"drop_publish_interval,omitempty"`

	// DropBatchSize 每轮发布取多少条掉落出箱记录(默认 128)。
	DropBatchSize int `yaml:"drop_batch_size,omitempty" json:"drop_batch_size,omitempty"`

	// MailAddr mail 服务 gRPC 地址(弱依赖:空 → 背包满掉落留在出箱轮询重试,不转邮件)。
	// 内网 insecure 直连(系统接口,无 JWT)。发放遇 ErrInventoryCapacityFull(背包满)时,
	// RunDropPublisher 用它调 SendPersonalMail 把溢出装备转个人邮件(幂等键防重发),再删出箱行。
	MailAddr string `yaml:"mail_addr,omitempty" json:"mail_addr,omitempty"`

	// ── 战斗中实时进度通道(实时成长,docs/design/realtime-progression.md)──

	// ProgressEnabled 实时进度通道开关(**默认 false=关闭**,§14.2 默认不改变现有行为)。
	//
	// 关闭时新对局的 ReportProgress 一律拒(ERR_INVALID_STATE,DS 收到即停流,回退局后
	// 结算路径);**已有水位的进行中对局不受开关影响,继续收流直到结算**(每场模式以水位行
	// 存在性为权威固化,防中途切换造成"部分实时+结算掉落被抑制"的丢奖,§22 单一权威)。
	//
	// ⚠️ 混版发布纪律(P0 级,不变量 §21):只有当 battle_result 全部副本都已升级到含
	// 实时进度收口逻辑的版本(≥2026-07-21)后才允许置 true。旧代码副本结算时不感知
	// 水位表、不抑制结算掉落 —— 开关提前打开会在混版窗口双发掉落。
	ProgressEnabled bool `yaml:"progress_enabled,omitempty" json:"progress_enabled,omitempty"`

	// MonsterExp 怪物击杀经验表:monster_config_id → 单只经验(DS 不可信,换算唯一权威)。
	// 空 = 击杀事实不折算经验(跳过并告警,水位照常推进);与导表管线接通前先在此配置。
	MonsterExp map[uint32]uint64 `yaml:"monster_exp,omitempty" json:"monster_exp,omitempty"`

	// MaxProgressBatch 单次 ReportProgress 最多接受的事件条数(默认 256,超限拒)。
	MaxProgressBatch int `yaml:"max_progress_batch,omitempty" json:"max_progress_batch,omitempty"`

	// MaxProgressSeqPerMatch 单场事件 seq 硬上限(默认 100000)。超上限整批拒:
	// 正常对局到不了这个量级,到了必是异常 / 恶意 DS(有界性兜底,§9.18 同类纪律)。
	MaxProgressSeqPerMatch uint64 `yaml:"max_progress_seq_per_match,omitempty" json:"max_progress_seq_per_match,omitempty"`

	// MaxKillCountPerFact 单条击杀事实的 count 上限(默认 100;DS 侧批内聚合后仍超必异常)。
	MaxKillCountPerFact uint32 `yaml:"max_kill_count_per_fact,omitempty" json:"max_kill_count_per_fact,omitempty"`

	// MaxPickupCountPerFact 单条拾取事实的 count 上限(默认 10;硬上限 46:单事实必须能
	// 装进一行出箱 CSV,见 maxDropPerPlayerHardCap)。
	MaxPickupCountPerFact uint32 `yaml:"max_pickup_count_per_fact,omitempty" json:"max_pickup_count_per_fact,omitempty"`

	// MaxProgressExpPerMatch 单场累计经验硬上限(默认 1000000)。事实换算后按场累计,
	// 超限整批拒 + 告警:失陷 DS 只有单事实/单批上限时仍可跨大量 seq 累计巨额经验,
	// 必须在事务权威侧封顶(审计 P1;正常对局全员满级也远到不了该量级)。
	MaxProgressExpPerMatch uint64 `yaml:"max_progress_exp_per_match,omitempty" json:"max_progress_exp_per_match,omitempty"`

	// MaxProgressItemsPerMatch 单场累计掉落件数硬上限(默认 500),语义同上。
	MaxProgressItemsPerMatch uint32 `yaml:"max_progress_items_per_match,omitempty" json:"max_progress_items_per_match,omitempty"`

	// MaxProgressExpPerPlayer 单场单玩家累计经验硬上限(默认 200000)。只有按场累计时,
	// 失陷 DS 仍可把全场额度灌给一人;设计要求的反作弊上限是单场单玩家粒度
	// (realtime-progression.md 反作弊上限),按玩家在事务权威侧封顶。
	MaxProgressExpPerPlayer uint64 `yaml:"max_progress_exp_per_player,omitempty" json:"max_progress_exp_per_player,omitempty"`

	// MaxProgressItemsPerPlayer 单场单玩家累计掉落件数硬上限(默认 100),语义同上。
	MaxProgressItemsPerPlayer uint32 `yaml:"max_progress_items_per_player,omitempty" json:"max_progress_items_per_player,omitempty"`

	// MaxProgressKillsPerPlayer 单场单玩家累计击杀数硬上限(默认 1000),语义同上。
	MaxProgressKillsPerPlayer uint32 `yaml:"max_progress_kills_per_player,omitempty" json:"max_progress_kills_per_player,omitempty"`

	// ProgressPublishInterval 进度出箱发布轮询间隔(默认 1s;经验/掉落到账体感由它决定上界)。
	ProgressPublishInterval config.Duration `yaml:"progress_publish_interval,omitempty" json:"progress_publish_interval,omitempty"`

	// ProgressBatchSize 每轮发布取多少条进度出箱记录(默认 128)。
	ProgressBatchSize int `yaml:"progress_batch_size,omitempty" json:"progress_batch_size,omitempty"`

	// ── 保留期清理(CLAUDE.md §9 不变量 24:只增表必须有界)──

	// HistoryRetentionDays 对局结算(battles+stats)与已结算进度水位(stream+player)
	// 保留天数(默认 90)。远大于结算重试窗口(小时级);对局历史查询窗口同步受此限。
	HistoryRetentionDays int `yaml:"history_retention_days,omitempty" json:"history_retention_days,omitempty"`

	// RetentionSweepInterval 保留期清理轮询间隔(默认 1h)。多副本各自跑,批删幂等无需锁。
	RetentionSweepInterval config.Duration `yaml:"retention_sweep_interval,omitempty" json:"retention_sweep_interval,omitempty"`

	// RetentionSweepBatch 每轮每类清理的对局数上限(默认 200 场;stats 行数 ≈ 场数×人数)。
	RetentionSweepBatch int `yaml:"retention_sweep_batch,omitempty" json:"retention_sweep_batch,omitempty"`

	// RetentionModeRaw 保留期清理模式:留空 / "report_only" = 默认只报告不删;"delete" = 真删。
	// 「因为数据大了就自动删」不可接受(§9.24 + 2026-07-22 用户指令)。
	RetentionModeRaw string `yaml:"retention_mode,omitempty" json:"retention_mode,omitempty"`
}

// Defaults 填默认值。
func (c *Config) Defaults() {
	if c.Battle.EloKFactor <= 0 {
		c.Battle.EloKFactor = 32
	}
	if c.Battle.BaseMMR <= 0 {
		c.Battle.BaseMMR = 1500
	}
	if len(c.Battle.ConsumeTopics) == 0 {
		c.Battle.ConsumeTopics = []string{kafkax.TopicBattleResult, kafkax.TopicDSLifecycle}
	}
	if c.Battle.OutboxPublishInterval.Std() <= 0 {
		c.Battle.OutboxPublishInterval = config.Duration(2 * time.Second)
	}
	if c.Battle.OutboxBatchSize <= 0 {
		c.Battle.OutboxBatchSize = 128
	}
	if c.Battle.TerminalReleaseInterval.Std() <= 0 {
		c.Battle.TerminalReleaseInterval = config.Duration(2 * time.Second)
	}
	if c.Battle.TerminalReleaseBatchSize <= 0 {
		c.Battle.TerminalReleaseBatchSize = 128
	}
	if c.Battle.TerminalReleaseGrace.Std() <= 0 {
		c.Battle.TerminalReleaseGrace = config.Duration(15 * time.Second)
	}
	if c.Battle.DropPublishInterval.Std() <= 0 {
		c.Battle.DropPublishInterval = config.Duration(2 * time.Second)
	}
	if c.Battle.DropBatchSize <= 0 {
		c.Battle.DropBatchSize = 128
	}
	if c.Battle.HistoryRetentionDays <= 0 {
		c.Battle.HistoryRetentionDays = 90
	}
	if c.Battle.HistoryRetentionDays > 90 {
		// §9.24 硬上限:失效数据最多保留 90 天(审计 P1:配置 365 不得原样生效;
		// 确需更长必须走 §9.24 登记例外,不允许静默配置突破)。
		c.Battle.HistoryRetentionDays = 90
	}
	if c.Battle.RetentionSweepInterval.Std() <= 0 {
		c.Battle.RetentionSweepInterval = config.Duration(time.Hour)
	}
	if c.Battle.RetentionSweepBatch <= 0 {
		c.Battle.RetentionSweepBatch = 200
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":50022"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":51022"
	}
	c.DSAuth.Defaults()
}

// ValidateRedisAuthorityIngress 保证 Model-B 结算只有经过 :8444 Guard + Redis active
// checker + receipt 的同步 RPC 入口。旧 pandora.battle.result 消息没有可核验的完整凭据，
// 继续订阅会直接调用 usecase 落 MySQL，形成绕过授权与 receipt 的第二入口。
func (c *Config) ValidateRedisAuthorityIngress() error {
	if !c.DSAuth.AuthorityModeRedis() {
		return nil
	}
	if c.Battle.DSAllocatorAddr == "" {
		return fmt.Errorf("battle_result: authority_mode=redis requires battle.ds_allocator_addr for terminal release outbox relay")
	}
	if c.Battle.TerminalReleaseInterval.Std() <= 0 || c.Battle.TerminalReleaseBatchSize <= 0 {
		return fmt.Errorf("battle_result: terminal release worker interval/batch must be positive")
	}
	grace := c.Battle.TerminalReleaseGrace.Std()
	if grace < 5*time.Second || grace > 2*time.Minute {
		return fmt.Errorf("battle_result: terminal_release_grace must be within [5s,2m]")
	}
	for _, topic := range c.Battle.ConsumeTopics {
		if topic == kafkax.TopicBattleResult {
			return fmt.Errorf("battle_result: authority_mode=redis forbids unauthenticated topic %q; consume only %q and accept results via guarded ReportResult RPC",
				kafkax.TopicBattleResult, kafkax.TopicDSLifecycle)
		}
	}
	return nil
}

// IsDroppable 判断某 item_config_id 是否在战斗掉落白名单内(DS 不可信过滤,W5 ④)。
// 白名单为空 → 恒 false(安全默认:不放行任何掉落)。
func (b *BattleConf) IsDroppable(itemConfigID uint32) bool {
	for _, id := range b.DropWhitelist {
		if id == itemConfigID {
			return true
		}
	}
	return false
}

// maxDropPerPlayerHardCap 每玩家掉落条数硬上限:battle_drop_outbox.item_config_ids 为
// VARCHAR(512),46 个 10 位 uint32 + 45 个逗号 = 505 字符,是不超列宽的最大条数。
const maxDropPerPlayerHardCap = 46

// MaxDropsPerPlayer 返回生效的每玩家最大掉落条数(未配置/非法 → 默认 32;超硬上限 → 46)。
// 放访问器而非 Defaults,保证任何构造路径(含测试直建 BattleConf)都有安全上限。
func (b *BattleConf) MaxDropsPerPlayer() int {
	n := b.MaxDropPerPlayer
	if n <= 0 {
		n = 32
	}
	if n > maxDropPerPlayerHardCap {
		n = maxDropPerPlayerHardCap
	}
	return n
}

// ── 实时进度通道访问器(任何构造路径都有安全默认,风格同 MaxDropsPerPlayer)──

// MonsterExpOf 查怪物单只经验。未配置该怪物 → (0, false),调用方跳过并告警。
func (b *BattleConf) MonsterExpOf(monsterConfigID uint32) (uint64, bool) {
	exp, ok := b.MonsterExp[monsterConfigID]
	return exp, ok
}

// MaxProgressBatchOrDefault 单批事件条数上限(默认 256)。
func (b *BattleConf) MaxProgressBatchOrDefault() int {
	if b.MaxProgressBatch > 0 {
		return b.MaxProgressBatch
	}
	return 256
}

// MaxProgressSeqPerMatchOrDefault 单场 seq 硬上限(默认 100000)。
func (b *BattleConf) MaxProgressSeqPerMatchOrDefault() uint64 {
	if b.MaxProgressSeqPerMatch > 0 {
		return b.MaxProgressSeqPerMatch
	}
	return 100_000
}

// MaxKillCountPerFactOrDefault 单条击杀事实 count 上限(默认 100)。
func (b *BattleConf) MaxKillCountPerFactOrDefault() uint32 {
	if b.MaxKillCountPerFact > 0 {
		return b.MaxKillCountPerFact
	}
	return 100
}

// MaxPickupCountPerFactOrDefault 单条拾取事实 count 上限(默认 10;夹紧到 46:
// 出箱按事实成行,单事实必须能装进一行 CSV 列宽,见 maxDropPerPlayerHardCap)。
func (b *BattleConf) MaxPickupCountPerFactOrDefault() uint32 {
	n := b.MaxPickupCountPerFact
	if n == 0 {
		n = 10
	}
	if n > maxDropPerPlayerHardCap {
		n = maxDropPerPlayerHardCap
	}
	return n
}

// MaxProgressExpPerMatchOrDefault 单场累计经验硬上限(默认 1000000)。
func (b *BattleConf) MaxProgressExpPerMatchOrDefault() uint64 {
	if b.MaxProgressExpPerMatch > 0 {
		return b.MaxProgressExpPerMatch
	}
	return 1_000_000
}

// MaxProgressItemsPerMatchOrDefault 单场累计掉落件数硬上限(默认 500)。
func (b *BattleConf) MaxProgressItemsPerMatchOrDefault() uint32 {
	if b.MaxProgressItemsPerMatch > 0 {
		return b.MaxProgressItemsPerMatch
	}
	return 500
}

// MaxProgressExpPerPlayerOrDefault 单场单玩家累计经验硬上限(默认 200000)。
func (b *BattleConf) MaxProgressExpPerPlayerOrDefault() uint64 {
	if b.MaxProgressExpPerPlayer > 0 {
		return b.MaxProgressExpPerPlayer
	}
	return 200_000
}

// MaxProgressItemsPerPlayerOrDefault 单场单玩家累计掉落件数硬上限(默认 100)。
func (b *BattleConf) MaxProgressItemsPerPlayerOrDefault() uint32 {
	if b.MaxProgressItemsPerPlayer > 0 {
		return b.MaxProgressItemsPerPlayer
	}
	return 100
}

// MaxProgressKillsPerPlayerOrDefault 单场单玩家累计击杀数硬上限(默认 1000)。
func (b *BattleConf) MaxProgressKillsPerPlayerOrDefault() uint32 {
	if b.MaxProgressKillsPerPlayer > 0 {
		return b.MaxProgressKillsPerPlayer
	}
	return 1000
}

// ProgressPublishIntervalOrDefault 进度出箱发布轮询间隔(默认 1s)。
func (b *BattleConf) ProgressPublishIntervalOrDefault() time.Duration {
	if d := b.ProgressPublishInterval.Std(); d > 0 {
		return d
	}
	return time.Second
}

// ProgressBatchSizeOrDefault 进度出箱发布批大小(默认 128)。
func (b *BattleConf) ProgressBatchSizeOrDefault() int {
	if b.ProgressBatchSize > 0 {
		return b.ProgressBatchSize
	}
	return 128
}

// RetentionMode 返回生效的保留期清理模式(默认 ModeReportOnly = 只报告不删)。
func (b *BattleConf) RetentionMode() dbguard.Mode {
	m, err := dbguard.ParseMode(b.RetentionModeRaw)
	if err != nil {
		return dbguard.ModeReportOnly
	}
	return m
}

// ValidateRetentionMode 供启动 fail-fast(写了无法识别的模式必须拒启)。
func (b *BattleConf) ValidateRetentionMode() error {
	_, err := dbguard.ParseMode(b.RetentionModeRaw)
	return err
}
