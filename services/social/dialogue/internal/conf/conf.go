// Package conf 是 dialogue 服务的私有配置结构(2026-06-16)。
package conf

import (
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
)

// Config 是 dialogue 服务的完整配置。
type Config struct {
	config.Base `yaml:",inline" mapstructure:",squash"`

	Dialogue DialogueConf `yaml:"dialogue" json:"dialogue"`
}

// DialogueConf 是 dialogue 服务私有配置。
//
// 对话树**不在这里** —— 它是策划数值,唯一权威是与 UE 同源的配置表
// `configtable/dist/dialogue.json`(源表 对话/d_对话.xlsx),经 config_table.dir 加载。
// 本结构只保留服务自己的运行参数。
//
// 历史:对话树曾以 trees/nodes/options 三层内联在 dialogue-dev.yaml,属 W2 骨架期
// 的 demo 数据;配置表管线接入后整块删除,不要再加回来(YAML 双数据源必然漂移)。
type DialogueConf struct {
	// SessionTTL 单次对话会话存活时间。空闲超过此时长的会话会被回收(默认 5m)。
	SessionTTL config.Duration `yaml:"session_ttl,omitempty" json:"session_ttl,omitempty"`
}

// DefaultSessionTTL 是会话默认存活时间。
const DefaultSessionTTL = 5 * time.Minute

// Defaults 填默认值,防止 yaml 缺字段时零值引发非预期行为。
func (c *Config) Defaults() {
	if c.Dialogue.SessionTTL.Std() <= 0 {
		c.Dialogue.SessionTTL = config.Duration(DefaultSessionTTL)
	}
	if c.Server.Grpc.Addr == "" {
		c.Server.Grpc.Addr = ":20013"
	}
	if c.Server.Http.Addr == "" {
		c.Server.Http.Addr = ":21013"
	}
}
