// Package conf 是 push 服务的私有配置结构。
//
// 内嵌 pkg/config.Base 拿公共字段,再加 push 自有字段。
//
// 加载方式(见 cmd/push/main.go):
//
//	c := kconfig.New(kconfig.WithSource(file.NewSource("./etc/push-dev.yaml")))
//	c.Load()
//	var cfg conf.Config
//	c.Scan(&cfg)
package conf

import (
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/kafkax"
)

// Config 是 push 服务的完整配置。
type Config struct {
	// Base 公共字段(Server/Node/Snowflake/Locker/Registry/Timeouts/Kafka)。
	config.Base `yaml:",inline" mapstructure:",squash"`

	// Push 业务字段。
	Push PushConf `yaml:"push" json:"push"`
}

// PushConf 是 push 服务私有配置。
//
// W3 ④(2026-06-05)真实化:
//   - 删除 mock_* 字段(mock tick 退役),改为 kafka consumer 真实推送
//   - 新增 Topics:订阅的 push topic 列表,默认 kafkax.PushTopics(3 个 W3 ④ 启用)
//   - 保留 OfflineCacheTTL:在线离线切换补推用 redis ZSET TTL,默认 5min
type PushConf struct {
	// Topics 订阅的 kafka topic 列表;空时用 kafkax.PushTopics 默认值。
	//
	// 每个 topic 一个独立的 KafkaConsumer,共享 cfg.Kafka.GroupID。
	// 业务侧 producer 用 kafkax.PushToPlayers helper 发送,key=player_id。
	Topics []string `yaml:"topics,omitempty" json:"topics,omitempty"`

	// OfflineCacheTTL 投递缓冲的帧保留窗口,默认 5min(2026-07-22 v2:**所有**定向帧
	// 先入 pandora:push:offline:<player_id> ZSET 再投递,score = 服务端投递游标;
	// 在线实时投递 + 离线/跨 Pod 由连接写者按游标拉取,见 data/offline.go)。
	// 整 key TTL 另有 7 天游标基线保活(哨兵 member),本值只控帧 member 修剪窗口。
	OfflineCacheTTL config.Duration `yaml:"offline_cache_ttl,omitempty" json:"offline_cache_ttl,omitempty"`

	// OfflineCacheMaxFrames 单玩家投递缓冲条数硬上限(§9.18 有界纪律),默认 512。
	// 写入侧保留最新 N 条 + 按 TTL 窗口修剪(持续有消息时整 key TTL 一直被刷新,
	// 不修剪 member 会让旧帧永久保留);读取侧同值兜底 LIMIT。
	OfflineCacheMaxFrames int `yaml:"offline_cache_max_frames,omitempty" json:"offline_cache_max_frames,omitempty"`

	// RequireSessionGate 会话现行性门强制档(P0,INC-20260722-004;prod 生成器机械置 true)。
	// true:建流必须携带 Envoy 验签 jti 且为当前一代会话,权威不可达 fail-closed 拒;
	// false(缺省,§14.2 dev 直连联调不变):有 jti 仍校验,无 jti 放行。
	RequireSessionGate bool `yaml:"require_session_gate,omitempty" json:"require_session_gate,omitempty"`

	// AllowUnverifiedEvictionPolicy 托管 Redis 禁用 CONFIG 导致 maxmemory-policy 无法
	// 核验时,是否放行启动(R4 复审:核验失败缺省 fail-closed 拒启动——「查不了」不等于
	// 「配置正确」,驱逐策略错配会静默丢帧/放行旧会话)。只允许在**人工确认**目标
	// Redis 全拓扑 maxmemory-policy=noeviction 的托管环境显式置 true,并把该确认列入
	// 部署核对清单;自建 Redis(仓内 compose/k8s 基线)一律保持 false。
	AllowUnverifiedEvictionPolicy bool `yaml:"allow_unverified_eviction_policy,omitempty" json:"allow_unverified_eviction_policy,omitempty"`
}

// Defaults 把零值填成 Pandora 标准默认值。
func (c *Config) Defaults() {
	if len(c.Push.Topics) == 0 {
		c.Push.Topics = append([]string(nil), kafkax.PushTopics...)
	}
	if c.Push.OfflineCacheTTL == 0 {
		c.Push.OfflineCacheTTL = config.Duration(5 * time.Minute)
	}
	if c.Push.OfflineCacheMaxFrames <= 0 {
		c.Push.OfflineCacheMaxFrames = 512
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20014"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21014"
	}
	if c.Kafka.GroupID == "" {
		c.Kafka.GroupID = "pandora-push"
	}
}
