package configtable

import (
	"os"
	"path/filepath"
	"testing"
)

// TestLoadRealDistIfPresent 端到端冒烟:直接加载仓库 configtable/dist 的真实产物
// (tools/configtable-gen 生成)。产物尚未生成的环境跳过。
func TestLoadRealDistIfPresent(t *testing.T) {
	dist := filepath.Join("..", "..", "configtable", "dist")
	if _, err := os.Stat(filepath.Join(dist, ManifestFileName)); err != nil {
		t.Skipf("真实 dist 不存在,跳过: %v", err)
	}
	s := NewStore()
	res, err := s.Load(dist, 0)
	if err != nil {
		t.Fatalf("加载真实 dist 失败: %v", err)
	}
	for _, w := range res.Warnings {
		t.Logf("告警: %s", w)
	}
	tb := s.Tables()
	if tb.Level.Count() == 0 {
		t.Fatal("关卡表为空")
	}
	// 与 g_关卡.xlsx 的稳定事实对齐:6=MOBA战斗、7=松林镇副本均为战斗类;1=登录不是。
	if !tb.Level.IsBattleLevel(6) || !tb.Level.IsBattleLevel(7) {
		t.Fatal("6/7 应为战斗关卡")
	}
	if tb.Level.IsBattleLevel(1) {
		t.Fatal("1(登录)不应为战斗关卡")
	}
	if err := tb.PlayerLevelExp.ValidateCurve(); err != nil {
		t.Fatalf("真实玩家等级经验表不合法: %v", err)
	}
	if tb.PlayerLevelExp.Count() != 15 || tb.PlayerLevelExp.MaxLevel() != 15 {
		t.Fatalf("玩家等级经验表等级数=%d max=%d, want 15/15",
			tb.PlayerLevelExp.Count(), tb.PlayerLevelExp.MaxLevel())
	}
	curve := tb.PlayerLevelExp.ExperienceCurve()
	if len(curve) != 14 || curve[0] != 1000 || curve[7] != 6600 || curve[13] != 11400 {
		t.Fatalf("真实曲线关键值错误: %v", curve)
	}
	last, ok := tb.PlayerLevelExp.ByID(15)
	if !ok || last.GetUpgradeExp() != 0 || last.GetCumulativeExp() != 86800 {
		t.Fatalf("Lv15 终点错误: row=%+v ok=%v", last, ok)
	}

	// 道具表(d_道具.xlsx):10003 是测试装备(部位 1、不可堆叠),10001 是消耗品(不可穿戴)。
	// 这两条是 player.SetEquipment 的 isEquip / slotMatch 校验直接依赖的事实。
	if tb.Item == nil || tb.Item.Count() == 0 {
		t.Fatal("道具表为空")
	}
	if !tb.Item.IsEquipment(10003) || tb.Item.EquipSlotOf(10003) != 1 {
		t.Fatalf("10003 应为部位 1 的装备, IsEquipment=%v slot=%d",
			tb.Item.IsEquipment(10003), tb.Item.EquipSlotOf(10003))
	}
	if !tb.Item.MatchesSlot(10003, 1) {
		t.Fatal("10003 应能装进部位 1")
	}
	if tb.Item.MatchesSlot(10003, 2) {
		t.Fatal("10003 不应能装进部位 2")
	}
	if tb.Item.IsEquipment(10001) || tb.Item.MatchesSlot(10001, 1) {
		t.Fatal("10001(消耗品)不应可穿戴")
	}
	if tb.Item.MatchesSlot(999999, 1) {
		t.Fatal("表里不存在的道具必须 fail-closed 判为不可装备")
	}

	// 专精表(z_专精.xlsx):整树校验必须过,且前置关系可用。
	if tb.Talent == nil || tb.Talent.Count() == 0 {
		t.Fatal("专精表为空")
	}
	if err := tb.Talent.ValidateTree(); err != nil {
		t.Fatalf("真实专精表整树校验不过: %v", err)
	}
	if got := tb.Talent.MaxLevelOf(1); got != 5 {
		t.Fatalf("专精 1 等级上限应为 5, got %d", got)
	}
	// 专精 4(聚能)前置是专精 1 达 3 级:只点 4 必须被拒。
	if _, err := tb.Talent.ValidateAllocation(map[uint32]uint32{4: 1}); err == nil {
		t.Fatal("缺前置的分配应被拒")
	}
	cost, err := tb.Talent.ValidateAllocation(map[uint32]uint32{1: 3, 4: 1})
	if err != nil {
		t.Fatalf("满足前置的分配应通过: %v", err)
	}
	// 当前表每级消耗均为 1,总消耗 = 等级和 = 4。
	if cost != 4 {
		t.Fatalf("总消耗应为 4, got %d", cost)
	}
	if _, err := tb.Talent.ValidateAllocation(map[uint32]uint32{1: 99}); err == nil {
		t.Fatal("超等级上限的分配应被拒")
	}
}
