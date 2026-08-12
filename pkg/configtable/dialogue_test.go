// dialogue_test.go — 对话表逐行形状(validateDialogueRow)与批次级引用完整性
// (ValidateDialogueTable)的加载期门禁回归。
//
// 这两道闸补的是 (excel_fk) 覆盖不到的洞:后继节点引用的是本表自己的 id,而生成器
// 明确禁止自引用外键,所以「后继悬空 / 跨 NPC 跳转 / 起始节点缺失重复」全靠这里拦。
package configtable

import (
	"strings"
	"testing"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// dialogueNode 造一个最小合法节点;next != 0 时带一个指向它的选项。
func dialogueNode(id, npc uint32, start bool, next uint32) *configpb.DialogueRow {
	r := &configpb.DialogueRow{Id: id, NpcId: npc, Speaker: "甲", Text: "文", IsStart: start}
	if next != 0 {
		r.Option1Text = "选项"
		r.Option1Next = next
	}
	return r
}

func newDialogueTableForTest(t *testing.T, rows ...*configpb.DialogueRow) *DialogueTable {
	t.Helper()
	tb, err := newDialogueTable(&configpb.DialogueTableData{Rows: rows})
	if err != nil {
		t.Fatalf("建对话表: %v", err)
	}
	return tb
}

func TestValidateDialogueTableRejectsBadConfig(t *testing.T) {
	cases := []struct {
		name string
		rows []*configpb.DialogueRow
		want string
	}{
		{
			// 0 个起始节点 → StartDialogue 永远进不去,是「服务起来了但功能全废」的典型静默故障。
			name: "没有起始节点",
			rows: []*configpb.DialogueRow{dialogueNode(1, 100, false, 0)},
			want: "没有起始节点",
		},
		{
			// 多个起始节点 → 入口取决于行序,不确定。
			name: "起始节点重复",
			rows: []*configpb.DialogueRow{dialogueNode(1, 100, true, 0), dialogueNode(2, 100, true, 0)},
			want: "多个起始节点",
		},
		{
			name: "后继悬空",
			rows: []*configpb.DialogueRow{dialogueNode(1, 100, true, 777)},
			want: "不存在于对话表",
		},
		{
			// 跨 NPC 跳转会让会话的 npc_id 与实际节点分叉,展示的说话人和内容对不上。
			name: "跨 NPC 跳转",
			rows: []*configpb.DialogueRow{dialogueNode(1, 100, true, 2), dialogueNode(2, 200, true, 0)},
			want: "不允许跨 NPC 跳转",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateDialogueTable(newDialogueTableForTest(t, tc.rows...))
			if err == nil {
				t.Fatal("应被校验器拒绝,实际放行")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("错误信息 = %q, 应含 %q", err.Error(), tc.want)
			}
		})
	}
}

// TestValidateDialogueTableAcceptsValid 成环 + 终止节点 + 多 NPC 并存必须放行 ——
// 对话树本来就允许「返回上一层」,别把合法环判成错。
func TestValidateDialogueTableAcceptsValid(t *testing.T) {
	tb := newDialogueTableForTest(t,
		dialogueNode(1, 100, true, 2),
		dialogueNode(2, 100, false, 1), // 跳回 1,成环
		dialogueNode(3, 200, true, 0),  // 另一个 NPC 的终止节点
	)
	if err := ValidateDialogueTable(tb); err != nil {
		t.Fatalf("合法配置被拒: %v", err)
	}
	start, ok := tb.StartNodeOf(100)
	if !ok || start.GetId() != 1 {
		t.Fatalf("StartNodeOf(100) = %v/%v, want 节点 1", start, ok)
	}
	if tb.HasNpc(300) {
		t.Fatal("未配置的 NPC 不应报告有树")
	}
}

// TestValidateDialogueTableNilFailsClosed 表缺失必须拒批次,不能当成「没有对话」放行。
func TestValidateDialogueTableNilFailsClosed(t *testing.T) {
	if err := ValidateDialogueTable(nil); err == nil {
		t.Fatal("缺表应拒批次")
	}
}

// TestValidateDialogueRowShape 逐行形状闸:空选项不得带后继、选项须从 1 起连续。
// 两条都是为了让「没有这个选项」只有一种表达 —— 否则策划漏填一格就得到一个
// 玩家永远点不到、而表面上又合法的死分支。
func TestValidateDialogueRowShape(t *testing.T) {
	t.Run("空选项带后继", func(t *testing.T) {
		row := &configpb.DialogueRow{Id: 1, NpcId: 100, Text: "文", IsStart: true, Option1Next: 1}
		if err := validateDialogueRow(row); err == nil {
			t.Fatal("空文本却填了后继,应拒")
		}
	})
	t.Run("选项跳空", func(t *testing.T) {
		row := &configpb.DialogueRow{Id: 1, NpcId: 100, Text: "文", IsStart: true, Option2Text: "第二个"}
		if err := validateDialogueRow(row); err == nil {
			t.Fatal("空 1 填 2,应拒")
		}
	})
	t.Run("连续填合法", func(t *testing.T) {
		row := &configpb.DialogueRow{
			Id: 1, NpcId: 100, Text: "文", IsStart: true,
			Option1Text: "一", Option1Next: 2, Option2Text: "二",
		}
		if err := validateDialogueRow(row); err != nil {
			t.Fatalf("合法行被拒: %v", err)
		}
		opts := DialogueOptions(row)
		if len(opts) != 2 || opts[0].Index != 1 || opts[0].NextNodeID != 2 || opts[1].NextNodeID != 0 {
			t.Fatalf("DialogueOptions = %+v", opts)
		}
	})
}
