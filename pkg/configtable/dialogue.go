package configtable

import (
	"fmt"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// dialogue.go — DialogueTable 手写伴生文件。
//
// 一行 = 一个对话节点;同一 npc_id 的全部行组成该 NPC 的一棵对话树。选项以「文本 + 后继节点」
// 成对列承载(最多 DialogueMaxOptions 个)。
//
// 后继节点(option*_next)引用的是**本表自己的 id**,而 (excel_fk) 明确不支持自引用
// (tools/configtable-gen/internal/tablegen/discover.go)。因此这里手写等强度的两道闸:
//   - validateDialogueRow:逐行形状(选项连续、空选项不得带后继);生成期与加载期各跑一次;
//   - ValidateDialogueTable:整表引用完整性 + 每 NPC 恰好一个起始节点,由消费服务
//     经 Store.AddValidator 注册(与 ValidateMissionCrossTables 同模式)。
//
// 域方法只回配置事实,不含会话状态 —— 会话是 dialogue 服务的运行时,不进配置层。

// DialogueMaxOptions 是单个对话节点的选项上限 = 源表成对列的组数。
// 加选项 = 加表列 + 加 proto 字段 + 改本常量,不动任何 RPC(CLAUDE.md §17.1)。
const DialogueMaxOptions = 3

// DialogueOptionView 是某节点上一个已填写的选项。
// Index 是选项在源表里的序号(1 基),同时充当协议层 option_id —— 它随表稳定,
// 不受行序 / 文案改动影响,客户端回传它即可定位。
type DialogueOptionView struct {
	Index int
	Text  string
	// NextNodeID 选完跳转的节点 id;0 = 选完即结束对话。
	NextNodeID uint32
}

// dialogueOptionCols 把成对列摊平成可遍历的形状,避免逐个 option1/2/3 复制粘贴。
func dialogueOptionCols(row *configpb.DialogueRow) [DialogueMaxOptions]struct {
	text string
	next uint32
} {
	return [DialogueMaxOptions]struct {
		text string
		next uint32
	}{
		{row.GetOption1Text(), row.GetOption1Next()},
		{row.GetOption2Text(), row.GetOption2Next()},
		{row.GetOption3Text(), row.GetOption3Next()},
	}
}

// DialogueOptions 返回该节点已填写的选项(按表内顺序)。空切片 = 终止节点。
func DialogueOptions(row *configpb.DialogueRow) []DialogueOptionView {
	cols := dialogueOptionCols(row)
	out := make([]DialogueOptionView, 0, DialogueMaxOptions)
	for i, c := range cols {
		if c.text == "" {
			break // validateDialogueRow 已保证选项从 1 起连续,遇空即到尾
		}
		out = append(out, DialogueOptionView{Index: i + 1, Text: c.text, NextNodeID: c.next})
	}
	return out
}

// validateDialogueRow 逐行业务校验(生成的 newDialogueTable 调用)。
//
// 两条形状约束都是为了让「空选项」只有一种表达,否则策划漏填一格就会得到一个
// 点不动的死选项,而表面上表是合法的:
//   - 选项必须从 1 起连续填(不得空 1 填 2);
//   - 选项文本为空时后继必须为 0(有后继没文案 = 玩家永远看不到、也点不到的分支)。
func validateDialogueRow(row *configpb.DialogueRow) error {
	cols := dialogueOptionCols(row)
	ended := false
	for i, c := range cols {
		if c.text == "" {
			ended = true
			if c.next != 0 {
				return fmt.Errorf("节点 %d:选项%d 文本为空但填了后继 %d(空选项的后继必须为 0)",
					row.GetId(), i+1, c.next)
			}
			continue
		}
		if ended {
			return fmt.Errorf("节点 %d:选项%d 有文本但前面的选项是空的(选项须从 1 起连续填)",
				row.GetId(), i+1)
		}
	}
	return nil
}

// ValidateDialogueTable 对话表批次级校验(消费服务 AddValidator 注册)。
//
// 补齐 (excel_fk) 因禁止自引用而覆盖不到的两件事:
//   - 每个 npc_id 恰好一个起始节点(0 个 → StartDialogue 永远进不去;多个 → 入口不确定);
//   - 每个非 0 后继必须存在,且与来源节点同属一个 NPC(跨 NPC 跳转会让会话的 npc_id
//     与实际节点分叉,展示的说话人和内容对不上)。
func ValidateDialogueTable(d *DialogueTable) error {
	if d == nil {
		return fmt.Errorf("缺少 dialogue 配置表")
	}
	starts := make(map[uint32]uint32, d.Count()) // npc_id → 已见起始节点 id
	for _, row := range d.All() {
		if !row.GetIsStart() {
			continue
		}
		if prev, dup := starts[row.GetNpcId()]; dup {
			return fmt.Errorf("NPC %d 有多个起始节点(%d 与 %d);每个 NPC 必须恰好一个",
				row.GetNpcId(), prev, row.GetId())
		}
		starts[row.GetNpcId()] = row.GetId()
	}
	for _, row := range d.All() {
		if _, ok := starts[row.GetNpcId()]; !ok {
			return fmt.Errorf("NPC %d 没有起始节点(需要有一行「起始节点」填 1)", row.GetNpcId())
		}
		for _, opt := range DialogueOptions(row) {
			if opt.NextNodeID == 0 {
				continue
			}
			next, ok := d.ByID(opt.NextNodeID)
			if !ok {
				return fmt.Errorf("节点 %d:选项%d 的后继 %d 不存在于对话表",
					row.GetId(), opt.Index, opt.NextNodeID)
			}
			if next.GetNpcId() != row.GetNpcId() {
				return fmt.Errorf("节点 %d(NPC %d):选项%d 的后继 %d 属于 NPC %d(不允许跨 NPC 跳转)",
					row.GetId(), row.GetNpcId(), opt.Index, opt.NextNodeID, next.GetNpcId())
			}
		}
	}
	return nil
}

// StartNodeOf 返回某 NPC 的起始节点。不存在 → (nil, false),调用方 fail-closed。
// 唯一性由 ValidateDialogueTable 在加载期保证,这里取首个命中即可。
func (t *DialogueTable) StartNodeOf(npcID uint32) (*configpb.DialogueRow, bool) {
	for _, row := range t.ListByNpcId(npcID) {
		if row.GetIsStart() {
			return row, true
		}
	}
	return nil, false
}

// HasNpc 报告该 NPC 是否配了对话树。
func (t *DialogueTable) HasNpc(npcID uint32) bool {
	return len(t.ListByNpcId(npcID)) > 0
}
