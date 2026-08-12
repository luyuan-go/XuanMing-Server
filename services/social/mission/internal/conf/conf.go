// Package conf 是 mission 服务的私有配置结构(2026-08-11)。
package conf

import (
	"fmt"
	"strings"
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

	// PushWriterLease 推送发布器的写者继任租约(单写者选举)。
	PushWriterLease PushWriterLeaseConf `yaml:"push_writer_lease,omitempty" json:"push_writer_lease,omitempty"`

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

// 推送发布器写者租约档位。
const (
	// PushWriterLeaseOff 不选举:本副本无条件跑发布器。
	// 只在**能保证单发布者**时合法(单进程 dev / 单副本 Recreate)。
	PushWriterLeaseOff = "off"
	// PushWriterLeaseEnforce 只有当选副本跑发布器,其余副本热备。
	PushWriterLeaseEnforce = "enforce"
)

// PushWriterLeaseConf 是推送出箱发布器的单写者选举配置。
//
// 为什么发布器必须单写者(而补扫 / 清理不需要):
//
//	mission_push_outbox 是**全局未分区**的一张表,发布器按 id 序整表 FIFO 取行 ——
//	正是 §9.21 点名要串行化的「作用于同一未分区权威的单写者循环」。两个副本同时跑时,
//	各自持有一份内存快照,投递顺序会交错:副本 B 手上的旧行可能在副本 A 投完新行之后
//	才发出去。而 MissionUpdateEvent.progressed 是**逐任务全量快照**(不是增量),后到即
//	覆盖 —— 玩家 UI 上进度条会从 7/10 退回 3/10,直到下次 ListMissions / push.resync
//	才恢复。事件里没有任何 revision 可供客户端判旧(ts_ms 是 event 级、跨副本各自墙钟,
//	protocol-ordering-rules §5-B 明令不得只靠它判重)。
//
//	对照:RunRewardRetry 与 RunRetentionSweep **刻意不选举** —— 前者的正确性由下游三个
//	幂等键保证(重复重放被吸收),后者的 DELETE 天然幂等。§9.21 明确「可并行 worker
//	不得为金丝雀强行全局串行化」,把它们一起包进选举是错的。
//
// 为什么不用出箱 claim 列:那需要建表迁移 + 每批多一次 UPDATE 往返 + 一套租约过期回收
// 语义(还得处理"claim 后进程被 SIGKILL,行卡到租约到期"),而 writerlease 是零 schema、
// 零额外往返、且已在 ds_allocator / hub_allocator 生产跑着的同形状件(§15.2 最少复杂度)。
// 也不能改成"先 DELETE 拿 RowsAffected==1 再投递":那把 at-least-once 降成 at-most-once
// (投递失败行已删,永不重放),用丢推送换保序,是更差的交易。
type PushWriterLeaseConf struct {
	// Mode:"off"(留空即 off)| "enforce"。
	//
	// 默认 off 而不是 enforce:mission 的 dev / 一键启动是单进程,没有 etcd 也必须起得来
	// (§14.2 默认值必须保证现有行为不变)。**生产的安全网不是这个默认值,而是 main.go
	// 里的机械门禁**:受管 k8s 内检测到 Deployment 是 RollingUpdate 而 mode != enforce
	// 时 fail-closed 退出 —— 与 ds_allocator / hub_allocator 同款,不留"滚动重叠期两个
	// 发布器并发"的无保护组合。
	Mode string `yaml:"mode,omitempty" json:"mode,omitempty"`
	// EtcdEndpoints etcd 地址(mode=enforce 必填)。
	EtcdEndpoints []string `yaml:"etcd_endpoints,omitempty" json:"etcd_endpoints,omitempty"`
	// LeaseTTLSec etcd session lease TTL(秒);留空用 writerlease 默认值。
	LeaseTTLSec int `yaml:"lease_ttl_sec,omitempty" json:"lease_ttl_sec,omitempty"`
	// DialTimeout etcd 连接超时;留空用 writerlease 默认值。
	DialTimeout config.Duration `yaml:"dial_timeout,omitempty" json:"dial_timeout,omitempty"`
}

// ResolveMode 归一化档位,取值不认识**报错而非猜** —— 拼错一个字母就静默退回无保护
// 并发发布是不可接受的失败模式(同 dbguard.ParseMode 的纪律)。
func (c PushWriterLeaseConf) ResolveMode() (string, error) {
	switch strings.ToLower(strings.TrimSpace(c.Mode)) {
	case "", PushWriterLeaseOff:
		return PushWriterLeaseOff, nil
	case PushWriterLeaseEnforce:
		return PushWriterLeaseEnforce, nil
	default:
		return "", fmt.Errorf("mission.push_writer_lease.mode=%q 不认识(只允许 %q / %q,留空=%q)",
			c.Mode, PushWriterLeaseOff, PushWriterLeaseEnforce, PushWriterLeaseOff)
	}
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
