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
	// 与 g_关卡.xlsx 的稳定事实对齐:6=MOBA战斗、7=松林镇副本、8=Artic01
	// 均为战斗类;1=登录不是。ID 8 是 PVE 匹配入口的线上准入门，必须由真实 dist 冒烟钉住。
	if !tb.Level.IsBattleLevel(6) || !tb.Level.IsBattleLevel(7) || !tb.Level.IsBattleLevel(8) {
		t.Fatal("6/7/8 应为战斗关卡")
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
	if _, _, err := tb.Talent.ValidateAllocation(map[uint32]uint32{4: 1}); err == nil {
		t.Fatal("缺前置的分配应被拒")
	}
	costs, cost, err := tb.Talent.ValidateAllocation(map[uint32]uint32{1: 3, 4: 1})
	if err != nil {
		t.Fatalf("满足前置的分配应通过: %v", err)
	}
	// 当前表每级消耗均为 1,总消耗 = 等级和 = 4。
	if cost != 4 {
		t.Fatalf("总消耗应为 4, got %d", cost)
	}
	// 每级消耗全为 1 正是"读按等级和反推"能长期蒙混过关的原因,这里把它钉成显式事实:
	// 逐节点消耗此刻恰好等于等级,一旦策划调表,本断言会随真实 dist 一起变。
	if costs[1] != 3 || costs[4] != 1 {
		t.Fatalf("逐节点消耗应为 {1:3, 4:1}, got %v", costs)
	}
	if _, _, err := tb.Talent.ValidateAllocation(map[uint32]uint32{1: 99}); err == nil {
		t.Fatal("超等级上限的分配应被拒")
	}

	// 专精效果表(z_专精_效果.xlsx):跨行校验必须过,且分配能换算出加成。
	// 效果表缺失会让天赋"点了没数值",是静默失败,必须在真实产物上钉住。
	if tb.TalentEffect == nil || tb.TalentEffect.Count() == 0 {
		t.Fatal("专精效果表为空:天赋将不产生任何战斗加成")
	}
	if err := tb.TalentEffect.ValidateEffects(); err != nil {
		t.Fatalf("真实专精效果表校验不过: %v", err)
	}
	// 每个有效果行的专精都必须真的在专精表里(外键由生成器校验,这里确认方向反过来也成立:
	// 专精表里的节点不一定都有效果行——纯解锁型节点合法,所以只断言"有加成能算出来")。
	if bonuses := tb.TalentEffect.ResolveBonuses(map[uint32]uint32{1: 3}); len(bonuses) == 0 {
		t.Fatal("专精 1 点 3 级应能换算出属性加成")
	}

	// 技能卡两张表(j_技能卡.xlsx / j_技能卡升级.xlsx):曲线断档会让卡升到某级后
	// 按钮没反应且不报错,必须在真实产物上整表校验。
	if tb.SkillCard == nil || tb.SkillCard.Count() == 0 {
		t.Fatal("技能卡表为空")
	}
	if tb.SkillCardUpgrade == nil || tb.SkillCardUpgrade.Count() == 0 {
		t.Fatal("技能卡升级表为空")
	}
	if err := tb.SkillCardUpgrade.ValidateCurves(tb.SkillCard); err != nil {
		t.Fatalf("真实技能卡升级曲线校验不过: %v", err)
	}
	// 每张卡都必须能一路升到自己的上限:逐级查一遍,缺任何一级都算断档。
	for _, card := range tb.SkillCard.All() {
		for level := uint32(2); level <= card.GetMaxLevel(); level++ {
			if _, ok := tb.SkillCardUpgrade.ShardCost(card.GetRarity(), level); !ok {
				t.Fatalf("卡 %d(稀有度 %d)升到 %d 级缺消耗配置", card.GetId(), card.GetRarity(), level)
			}
		}
	}
}

// TestRealDistMonsterKillExp 用真实 dist 产物钉住怪物击杀经验。
//
// 存在理由:「怪物有行但击杀经验为 0」的失败模式是**静默的** —— battle_result 对 exp==0
// 直接跳过出箱,一条日志都不打,水位照常推进。玩家表现是"杀怪没经验",而服务端毫无异常。
// 历史上这份数值住在服务 yaml 里时就漏配过 2007/2008 两只怪且长期无人发现(2026-08-04
// 迁到 role_level 表时才查出来),所以把"每只怪都有正经验"变成可机械复现的断言。
//
// 玩家英雄行(1001~1010)显式配 0,表示"有意不给击杀经验",不在本断言范围内。
func TestRealDistMonsterKillExp(t *testing.T) {
	dist := filepath.Join("..", "..", "configtable", "dist")
	if _, err := os.Stat(filepath.Join(dist, ManifestFileName)); err != nil {
		t.Skipf("真实 dist 不存在,跳过: %v", err)
	}
	s := NewStore()
	if _, err := s.Load(dist, 0); err != nil {
		t.Fatalf("加载真实 dist 失败: %v", err)
	}
	tb := s.Tables()
	if tb.RoleLevel == nil || tb.RoleLevel.Count() == 0 {
		t.Fatal("角色等级表为空")
	}

	// 怪物角色 ID 段:2xxx 起(1001~1010 是玩家英雄)。逐行断言经验为正。
	const firstMonsterRoleID = 2000
	monsters := 0
	for _, row := range tb.RoleLevel.All() {
		roleID := row.GetRoleId()
		if roleID < firstMonsterRoleID && roleID > 10 {
			continue // 玩家英雄行:显式 0 是有意的
		}
		monsters++
		if row.GetKillExp() == 0 {
			t.Errorf("怪物 role_id=%d level=%d 的击杀经验为 0(杀它零收益且不打任何日志;"+
				"若确实不该给经验请在此用例登记豁免)", roleID, row.GetLevel())
		}
	}
	if monsters == 0 {
		t.Fatal("角色等级表里一只怪都没有,断言失效")
	}

	// 定点核对历史漏配的两只(2007 蛇怪 / 2008 剧毒蛇怪):它们在 yaml 时代被漏掉过。
	for _, roleID := range []uint32{2007, 2008} {
		exp, ok := tb.RoleLevel.KillExpOf(roleID, 1)
		if !ok {
			t.Errorf("怪物 %d 在角色等级表里没有 1 级行", roleID)
			continue
		}
		if exp == 0 {
			t.Errorf("怪物 %d 的 1 级击杀经验为 0", roleID)
		}
	}

	// 等级维度:2001 松林近战腐蚀体有 1 级与 11 级两档,经验必须随等级抬高
	// (11 级血量是 1 级的 4 倍)。这条同时验证复合主键 角色ID×1000+等级 的查表口径。
	lv1, ok1 := tb.RoleLevel.KillExpOf(2001, 1)
	lv11, ok11 := tb.RoleLevel.KillExpOf(2001, 11)
	if !ok1 || !ok11 {
		t.Fatalf("2001 应同时有 1 级与 11 级行(实得 ok1=%v ok11=%v)", ok1, ok11)
	}
	if lv11 <= lv1 {
		t.Errorf("2001 的 11 级击杀经验 %d 应高于 1 级 %d", lv11, lv1)
	}
	// 等级 0 必须按 1 级处理(旧刷怪点表没有「怪物等级」列,导表后取到 0)。
	if lv0, ok := tb.RoleLevel.KillExpOf(2001, 0); !ok || lv0 != lv1 {
		t.Errorf("等级 0 应按 1 级处理,实得 exp=%d ok=%v(期望 %d)", lv0, ok, lv1)
	}
}

// TestRealDistBattleLaunchURLs 用真实 dist 钉死「关卡表 → DS 启动 URL」的换算结果。
//
// 这条替代了 ds_allocator-dev.yaml 里那张已删除的 local_ds.maps 手抄映射(§9.22 影子配置):
// 现在 allocator 起战斗 DS 时现查本表,所以本用例既是换算回归,也是"表里 8 张战斗图都能开局"
// 的机械核对——新增副本只需改 g_关卡.xlsx,这里跟着表走即可,不必再同步第二份清单。
func TestRealDistBattleLaunchURLs(t *testing.T) {
	dist := filepath.Join("..", "..", "configtable", "dist")
	if _, err := os.Stat(filepath.Join(dist, ManifestFileName)); err != nil {
		t.Skipf("真实 dist 不存在,跳过: %v", err)
	}
	s := NewStore()
	if _, err := s.Load(dist, 0); err != nil {
		t.Fatalf("加载真实 dist 失败: %v", err)
	}
	lv := s.Tables().Level

	// 战斗类关卡整表可开局(与服务启动期批次校验器同一断言)。
	if err := lv.ValidateBattleLaunchURLs(); err != nil {
		t.Fatalf("真实关卡表存在开不了局的战斗关卡: %v", err)
	}

	// 逐张比对(值取自 g_关卡.xlsx;7/8 等历史图的 URL 与旧 yaml 手抄值逐字一致)。
	// id=5「测试场景」的 GameMode类 列为空 → 不拼 ?game=,沿用关卡自带 GameMode。
	want := map[uint32]string{
		4:  "/Game/Test/Level/Fight/Lvl_Test_Fight?game=/Script/Pandora.PandoraBattleGameMode",
		5:  "/Game/Test/Level/_Test/Level_TopDown_Test02",
		6:  "/Game/Test/Level/MobaLevel?game=/Script/Pandora.PandoraBattleGameMode",
		7:  "/Game/Test/Level/SonglinTown?game=/Script/Pandora.PandoraPveGameMode",
		8:  "/Game/ScifiArctic/Maps/ExampleLevel_Artic01?game=/Script/Pandora.PandoraPveGameMode",
		9:  "/Game/Fantastic_Dungeon_Pack/maps/map_dungeon_level_1_dungeon?game=/Script/Pandora.PandoraBattleGameMode",
		10: "/Game/Fantastic_Dungeon_Pack/maps/map_dungeon_level_1_dungeon_Pve?game=/Script/Pandora.PandoraPveGameMode",
		11: "/Game/StylizedCyberpunk/Levels/StylizedCyberpunk?game=/Script/Pandora.PandoraPveGameMode",
	}
	for id, wantURL := range want {
		got, err := lv.BattleLaunchURL(id)
		if err != nil {
			t.Fatalf("map_id=%d 拼不出启动 URL: %v", id, err)
		}
		if got != wantURL {
			t.Fatalf("map_id=%d URL=%q, want %q", id, got, wantURL)
		}
	}
	// 登录 / 选角 / 主城不可开局(map_id 走错也进不去战斗)。
	for _, id := range []uint32{1, 2, 3} {
		if _, err := lv.BattleLaunchURL(id); err == nil {
			t.Fatalf("非战斗类关卡 %d 不应能拼出启动 URL", id)
		}
	}
}
