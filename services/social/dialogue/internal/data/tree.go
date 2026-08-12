// Package data 是 dialogue 服务的数据层(2026-06-16)。
//
// 当前最小版本:
//   - 对话树:从配置加载,ConfigTreeProvider 内存只读(MOBA 早期简单 if-else)。
//   - 会话:MemorySessionStore 单实例内存会话(见 session.go)。
//
// 后续(留 hook):对话树接 mysql `dialogue_trees`(json blob)/ 配置中心热更;
// 会话若需水平扩展则接 Redis(改 SessionStore 实现,biz/service 不动)。
package data

// DialogueOption 是对话节点上的一个选项(领域类型,非 proto)。
type DialogueOption struct {
	OptionID string
	Text     string
	Visible  bool
	// NextNode 选择后跳转的节点 ID;空或指向不存在的节点 = 结束对话。
	NextNode string
}

// DialogueNode 是对话树的一个节点。Options 为空 = 终止节点。
type DialogueNode struct {
	NodeID string
	// Speaker 本节点说话人;空 = 沿用 DialogueTree.Speaker。
	// 源表逐行有「说话人」列(分身 / 旁白可与 NPC 主名不同),这里如实承载,
	// 不在组树时压成一个值。
	Speaker string
	Text    string
	Options []DialogueOption
}

// DialogueTree 是单个 NPC 的完整对话树。
type DialogueTree struct {
	NpcID     uint32
	Speaker   string
	StartNode string
	Nodes     map[string]*DialogueNode
}

// TreeProvider 提供按 npc_id 查对话树的能力。
type TreeProvider interface {
	// GetTree 返回 npcID 对应的对话树;不存在返回 (nil, false)。
	GetTree(npcID uint32) (*DialogueTree, bool)
}

// ConfigTreeProvider 是配置驱动的只读对话树提供者。
// 构造后 trees 不再变更,GetTree 并发安全(纯读)。
type ConfigTreeProvider struct {
	trees map[uint32]*DialogueTree
}

// NewConfigTreeProvider 用预构建好的对话树表构造 provider。
// main.go 负责把 conf.TreeConf 转成 *DialogueTree 后传入。
func NewConfigTreeProvider(trees map[uint32]*DialogueTree) *ConfigTreeProvider {
	if trees == nil {
		trees = map[uint32]*DialogueTree{}
	}
	return &ConfigTreeProvider{trees: trees}
}

// GetTree 实现 TreeProvider。
func (p *ConfigTreeProvider) GetTree(npcID uint32) (*DialogueTree, bool) {
	t, ok := p.trees[npcID]
	return t, ok
}
