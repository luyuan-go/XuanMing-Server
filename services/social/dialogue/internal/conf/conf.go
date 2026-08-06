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
// 对话树从配置加载(docs/design/go-services.md §2.10:配置中心 / mysql dialogue_trees
// json blob;MOBA 早期简单 if-else)。当前最小版本直接在 yaml 里内联对话树。
type DialogueConf struct {
	// SessionTTL 单次对话会话存活时间。空闲超过此时长的会话会被回收(默认 5m)。
	SessionTTL config.Duration `yaml:"session_ttl,omitempty" json:"session_ttl,omitempty"`

	// Trees 对话树定义(按 npc_id 索引)。
	Trees []TreeConf `yaml:"trees,omitempty" json:"trees,omitempty"`
}

// TreeConf 单个 NPC 的对话树。
type TreeConf struct {
	NpcID     uint32     `yaml:"npc_id" json:"npc_id"`
	Speaker   string     `yaml:"speaker" json:"speaker"`       // NPC 显示名
	StartNode string     `yaml:"start_node" json:"start_node"` // 起始节点 ID
	Nodes     []NodeConf `yaml:"nodes" json:"nodes"`
}

// NodeConf 对话树的一个节点。
type NodeConf struct {
	NodeID  string       `yaml:"node_id" json:"node_id"`
	Text    string       `yaml:"text" json:"text"`
	Options []OptionConf `yaml:"options,omitempty" json:"options,omitempty"` // 空 = 终止节点(对话结束)
}

// OptionConf 一个对话选项。
type OptionConf struct {
	OptionID string `yaml:"option_id" json:"option_id"`
	Text     string `yaml:"text" json:"text"`
	// Visible 是否对客户端可见(指针:省略时默认 true)。
	// 当前最小版本是静态可见性;基于玩家数据的前置条件判定(等级 / 任务进度等)留后续接 player 服务。
	Visible *bool `yaml:"visible,omitempty" json:"visible,omitempty"`
	// NextNode 选择此项后跳转的节点 ID;空或指向不存在的节点 = 结束对话。
	NextNode string `yaml:"next_node,omitempty" json:"next_node,omitempty"`
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
