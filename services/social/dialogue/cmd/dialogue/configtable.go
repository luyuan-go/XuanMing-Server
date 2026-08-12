package main

import (
	"strconv"

	"github.com/luyuancpp/pandora/pkg/configtable"

	"github.com/luyuancpp/pandora/services/social/dialogue/internal/data"
)

// dialogueTreesFromStore 用配置表 dialogue 表实现 data.TreeProvider。
//
// 每次 GetTree 现取 Store 当前批次并就地组树:表是整批不可变快照原子切换的,
// 单次调用内不会跨版本;热更后下一次 GetTree 自然拿到新树,不需要重启,也不需要
// 在本服再缓存一份(§9.22 不重复存储影子状态)。
//
// 树的规模由源表行数决定(当前 4 行),组装成本可忽略;真涨到需要缓存时再按
// 批次 version 做记忆化,不预先复杂化(§15.3)。
type dialogueTreesFromStore struct{ store *configtable.Store }

// dialogueNodeKey 把表内 uint32 节点 ID 映射成协议里的 node_id 字符串。
// 协议 DialogueState.node_id / ChooseOptionRequest.option_id 都是 string(已上线字段,
// 按 §9.21 不改类型),这里只做十进制文本化,不引入第二套 ID 语义。
func dialogueNodeKey(id uint32) string { return strconv.FormatUint(uint64(id), 10) }

// GetTree 实现 data.TreeProvider。表里没有该 NPC、或没有起始节点(理论上已被
// ValidateDialogueTable 在加载期拒批次)一律返回 false,由 biz fail-closed。
func (p dialogueTreesFromStore) GetTree(npcID uint32) (*data.DialogueTree, bool) {
	tables := p.store.Tables()
	if tables == nil || tables.Dialogue == nil {
		return nil, false
	}
	rows := tables.Dialogue.ListByNpcId(npcID)
	if len(rows) == 0 {
		return nil, false
	}
	start, ok := tables.Dialogue.StartNodeOf(npcID)
	if !ok {
		return nil, false
	}
	nodes := make(map[string]*data.DialogueNode, len(rows))
	for _, row := range rows {
		opts := make([]data.DialogueOption, 0, configtable.DialogueMaxOptions)
		for _, o := range configtable.DialogueOptions(row) {
			next := ""
			if o.NextNodeID != 0 {
				next = dialogueNodeKey(o.NextNodeID)
			}
			opts = append(opts, data.DialogueOption{
				// option_id = 选项在源表里的列序号(1 基):随表稳定,改文案不影响客户端回传。
				OptionID: strconv.Itoa(o.Index),
				Text:     o.Text,
				// 表暂无「可见条件」列。可见性一旦要按玩家数据判定,是加表列 + 服务端判定,
				// 不是让客户端自己决定(§17.3),因此这里恒 true 而非读客户端。
				Visible:  true,
				NextNode: next,
			})
		}
		nodes[dialogueNodeKey(row.GetId())] = &data.DialogueNode{
			NodeID:  dialogueNodeKey(row.GetId()),
			Speaker: row.GetSpeaker(),
			Text:    row.GetText(),
			Options: opts,
		}
	}
	return &data.DialogueTree{
		NpcID:     npcID,
		Speaker:   start.GetSpeaker(),
		StartNode: dialogueNodeKey(start.GetId()),
		Nodes:     nodes,
	}, true
}
