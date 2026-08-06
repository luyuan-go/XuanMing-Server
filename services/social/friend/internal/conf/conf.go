// Package conf 是 friend 服务的私有配置结构(2026-06-15)。
package conf

import (
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/dbguard"
)

// Config 是 friend 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Friend FriendConf `yaml:"friend" json:"friend"`
}

// FriendConf 是 friend 服务私有配置。
type FriendConf struct {
	// MaxFriends 单玩家好友数量上限(默认 200)。
	// AddFriend 时对 requester 提前失败;权威校验在 AcceptFriend 事务内对双方原子执行。
	MaxFriends int `yaml:"max_friends,omitempty" json:"max_friends,omitempty"`

	// MaxIncomingRequests 单玩家「收到的待处理好友申请」上限(默认 200,不变量 §9.18)。
	// AddFriend 时在 CreateRequest 事务内校验 target 的 pending 收件箱数量,超限回 ErrFriendRequestLimit,
	// 防止被恶意刷爆好友申请收件箱(客户端可写入的累积列表须有写入侧总量上限)。
	MaxIncomingRequests int `yaml:"max_incoming_requests,omitempty" json:"max_incoming_requests,omitempty"`

	// MaxBlocks 单玩家黑名单上限(默认 200,不变量 §9.18)。
	// Block 时在事务内校验拉黑数量,超限回 ErrFriendBlockLimit。
	MaxBlocks int `yaml:"max_blocks,omitempty" json:"max_blocks,omitempty"`

	// LocatorAddr player_locator gRPC 地址(host:port)。
	// 空 → ListFriends 不查在线状态(is_online 全 false,弱依赖)。
	LocatorAddr string `yaml:"locator_addr,omitempty" json:"locator_addr,omitempty"`

	// RecommendLimit 单次推荐好友数量(默认 10,硬上限 20,超界收敛到 20)。
	RecommendLimit int `yaml:"recommend_limit,omitempty" json:"recommend_limit,omitempty"`

	// RecommendStrategies 推荐策略链(按序召回直到凑够 limit)。空 → ["mutual","random"]。
	// 已支持:mutual(熟人,共同好友数)/ random(随机兜底)。
	// 未来扩展:similar_power(实力相当)/ same_region(同区域),待 player 服务接画像后接。
	RecommendStrategies []string `yaml:"recommend_strategies,omitempty" json:"recommend_strategies,omitempty"`

	// ── 保留期清理(CLAUDE.md §9 不变量 24:只增表必须有界)──

	// RequestRetentionDays 终态好友请求(accepted/rejected/expired)保留天数(默认 90)。
	// friend_requests 每对玩家至多一行,但终态行随社交图对数累积;删后再次发起 = 重新
	// INSERT pending,行为等价(好友关系权威在 friendships,请求行无资产语义)。pending 永不清。
	RequestRetentionDays int `yaml:"request_retention_days,omitempty" json:"request_retention_days,omitempty"`

	// SweepInterval 保留期清理轮询间隔(默认 5m)。多副本各自跑,DELETE 幂等无需锁。
	SweepInterval config.Duration `yaml:"sweep_interval,omitempty" json:"sweep_interval,omitempty"`

	// SweepBatch 每轮清理行数上限(默认 500)。
	SweepBatch int `yaml:"sweep_batch,omitempty" json:"sweep_batch,omitempty"`

	// RetentionModeRaw 保留期清理模式:留空 / "report_only" = 默认只报告不删;"delete" = 真删。
	// 「因为数据大了就自动删」不可接受(§9.24 + 2026-07-22 用户指令);业务性过期删除不受此约束。
	RetentionModeRaw string `yaml:"retention_mode,omitempty" json:"retention_mode,omitempty"`

	// PairGuardRetentionDays 关系对守卫行(friend_pair_guards)保留天数(默认 30)。
	// R9 复审 P1:pair 守卫每关系对 1 行,随社交图 O(n²) 累积无上界。守卫行仅是
	// 锁载体无业务数据,任意时刻删除都安全(正被持有的行锁会阻塞 DELETE 到事务
	// 提交;下次 acquire 重新 INSERT),保留期只为限制清理频率/表规模。
	PairGuardRetentionDays int `yaml:"pair_guard_retention_days,omitempty" json:"pair_guard_retention_days,omitempty"`
}

// Defaults 填默认值,防止 yaml 缺字段时零值引发非预期行为。
func (c *Config) Defaults() {
	if c.Friend.MaxFriends <= 0 {
		c.Friend.MaxFriends = 200
	}
	if c.Friend.MaxIncomingRequests <= 0 {
		c.Friend.MaxIncomingRequests = 200
	}
	if c.Friend.MaxBlocks <= 0 {
		c.Friend.MaxBlocks = 200
	}
	if c.Friend.RecommendLimit <= 0 {
		c.Friend.RecommendLimit = 10
	}
	if c.Friend.RecommendLimit > 20 {
		c.Friend.RecommendLimit = 20
	}
	if c.Friend.RequestRetentionDays <= 0 {
		c.Friend.RequestRetentionDays = 90
	}
	if c.Friend.SweepInterval <= 0 {
		c.Friend.SweepInterval = config.Duration(5 * time.Minute)
	}
	if c.Friend.SweepBatch <= 0 {
		c.Friend.SweepBatch = 500
	}
	if c.Friend.PairGuardRetentionDays <= 0 {
		c.Friend.PairGuardRetentionDays = 30
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20004"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21004"
	}
}

// RetentionMode 返回生效的保留期清理模式(默认 ModeReportOnly = 只报告不删)。
func (c *FriendConf) RetentionMode() dbguard.Mode {
	m, err := dbguard.ParseMode(c.RetentionModeRaw)
	if err != nil {
		return dbguard.ModeReportOnly
	}
	return m
}

// ValidateRetentionMode 供 main 启动 fail-fast(写了无法识别的模式必须拒启)。
func (c *FriendConf) ValidateRetentionMode() error {
	_, err := dbguard.ParseMode(c.RetentionModeRaw)
	return err
}
