package main

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/luyuancpp/pandora/pkg/configtable"

	"github.com/luyuancpp/pandora/services/social/dialogue/internal/data"
)

// distDir 定位仓库内真实发布批次(configtable/dist),让本测试跑在真表上而非夹具:
// 对话树接表的价值就是「和 UE 同一份数据」,拿夹具测等于没测这条契约。
func distDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "..", "configtable", "dist"))
}

func loadRealStore(t *testing.T) *configtable.Store {
	t.Helper()
	store := configtable.NewStore()
	store.AddValidator(func(tb *configtable.Tables) error {
		return configtable.ValidateDialogueTable(tb.Dialogue)
	})
	if _, err := store.Load(distDir(), 0); err != nil {
		t.Fatalf("加载真实批次失败: %v", err)
	}
	return store
}

// TestRealDistPassesDialogueValidator 守住「当前发布批次能起服务」:
// 起始节点唯一、后继不悬空、不跨 NPC —— 任一条被策划改坏,这里先红。
func TestRealDistPassesDialogueValidator(t *testing.T) {
	store := loadRealStore(t)
	if store.Tables().Dialogue.Count() == 0 {
		t.Fatal("dialogue 表为空:对话服务起来也没有任何 NPC 可对话")
	}
}

// TestGetTreeMatchesLegacyShape 钉死迁表前后行为等价。
// 期望值直接抄自被删除的 dialogue-dev.yaml 内联树(2026-08-11 前),
// 因此这个测试同时是「迁表没改动策划内容」的证据。
func TestGetTreeMatchesLegacyShape(t *testing.T) {
	p := dialogueTreesFromStore{store: loadRealStore(t)}

	tree, ok := p.GetTree(1001)
	if !ok {
		t.Fatal("NPC 1001 应有对话树")
	}
	if tree.Speaker != "商店老板" {
		t.Fatalf("speaker = %q, want 商店老板", tree.Speaker)
	}
	if len(tree.Nodes) != 3 {
		t.Fatalf("NPC 1001 应有 3 个节点,实为 %d", len(tree.Nodes))
	}

	start := tree.Nodes[tree.StartNode]
	if start == nil {
		t.Fatalf("起始节点 %q 不在 Nodes 里", tree.StartNode)
	}
	if start.Text != "欢迎光临,冒险者!需要点什么?" {
		t.Fatalf("起始节点文案 = %q", start.Text)
	}
	if len(start.Options) != 3 {
		t.Fatalf("起始节点应有 3 个选项,实为 %d", len(start.Options))
	}
	// 第三个选项(「没事,告辞」)后继为空 = 选完结束对话,对应旧 yaml 省略 next_node。
	if last := start.Options[2]; last.Text != "没事,告辞" || last.NextNode != "" {
		t.Fatalf("末选项 = %+v, want 文本「没事,告辞」且无后继", last)
	}
	// 选项均可见:表暂无可见条件列,provider 恒置 true。
	for i, o := range start.Options {
		if !o.Visible {
			t.Fatalf("选项 %d 应可见", i)
		}
	}

	// 「看看你的货品」→ shop 节点;shop 的「我再想想」应能跳回起始节点(成环合法)。
	shop := tree.Nodes[start.Options[0].NextNode]
	if shop == nil || shop.Text != "这把钢剑削铁如泥,要不要试试?" {
		t.Fatalf("选项1 后继节点不对: %+v", shop)
	}
	if shop.Options[0].NextNode != tree.StartNode {
		t.Fatalf("shop 的「我再想想」应跳回起始节点,实为 %q", shop.Options[0].NextNode)
	}

	// 「最近有什么新鲜事?」→ rumor,无选项 = 终止节点。
	rumor := tree.Nodes[start.Options[1].NextNode]
	if rumor == nil || len(rumor.Options) != 0 {
		t.Fatalf("rumor 应是终止节点: %+v", rumor)
	}

	// NPC 1002 单节点单选项。
	guard, ok := p.GetTree(1002)
	if !ok || guard.Speaker != "守卫" || len(guard.Nodes) != 1 {
		t.Fatalf("NPC 1002 对话树不对: ok=%v tree=%+v", ok, guard)
	}

	// 未配置的 NPC 必须 fail-closed,不能返回空树让玩家进入一个不存在的对话。
	if _, ok := p.GetTree(999999); ok {
		t.Fatal("未配置的 NPC 应返回 false")
	}
}

// 校验器本身的正负用例在 pkg/configtable/dialogue_test.go(那边能用未导出的建表构造器);
// 本文件只覆盖「服务侧 provider + 真实发布批次」这一段。

// 编译期确认 provider 满足 biz 依赖的接口(换实现时先在这里红)。
var _ data.TreeProvider = dialogueTreesFromStore{}
