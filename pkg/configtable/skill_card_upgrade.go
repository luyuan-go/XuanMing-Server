package configtable

import (
	"fmt"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// skill_card_upgrade.go — SkillCardUpgradeTable 手写伴生文件。
// 首次由 configtable-gen 创建(仅当文件不存在),此后归人维护,生成器不再覆盖。
//
// 视图结构与通用访问 API 在 skill_card_upgrade_table.gen.go(生成,勿手改)。

// validateSkillCardUpgradeRow 逐行业务校验。
//
// 跨行约束(同稀有度同等级不得重复、曲线不得断档)看不到其它行,放在 ValidateCurves。
func validateSkillCardUpgradeRow(row *configpb.SkillCardUpgradeRow) error {
	switch row.GetRarity() {
	case SkillCardRarityCommon, SkillCardRarityRare, SkillCardRarityEpic, SkillCardRarityLegendary:
	default:
		return fmt.Errorf("稀有度(rarity=%d)不是合法取值(1=普通 2=稀有 3=史诗 4=传说)", row.GetRarity())
	}
	if row.GetLevel() < 2 {
		return fmt.Errorf("目标等级(level=%d)必须 >= 2:1 级是获得卡时的初始等级,不存在升到 1 级这回事",
			row.GetLevel())
	}
	if row.GetLevel() > MaxSkillCardLevel {
		return fmt.Errorf("目标等级(level=%d)超过 %d", row.GetLevel(), MaxSkillCardLevel)
	}
	if row.GetShardCost() == 0 {
		return fmt.Errorf("碎片消耗(shard_cost)为 0,等于免费升级;要做免费升级请显式改需求而不是填 0")
	}
	return nil
}

// ShardCost 返回某稀有度的卡升到 level 级所需碎片数。
//
// 第二个返回值为 false 表示曲线没铺到这一级——调用方必须据此拒绝升级,
// 绝不能当 0 处理(那就成了免费升级)。
func (t *SkillCardUpgradeTable) ShardCost(rarity, level uint32) (uint32, bool) {
	for _, row := range t.rows {
		if row.GetRarity() == rarity && row.GetLevel() == level {
			return row.GetShardCost(), true
		}
	}
	return 0, false
}

// ValidateCurves 整表跨行校验 + 与技能卡表的交叉校验。
//
// 三条不变量:
//  1. (稀有度, 目标等级) 唯一 —— 重复行会让 ShardCost 的结果取决于表内顺序;
//  2. 每个在用稀有度的曲线从 2 级起连续铺到该稀有度的最高等级上限 —— 断档表现为
//     "升到某级之后按钮没反应",不报错,是最难查的一类配置事故;
//  3. 消耗随等级单调不减 —— 越升越便宜几乎总是填错(填对了也该显式改这条校验)。
//
// 由各服务的 configtable 加载校验器调用(做法对齐 TalentTable.ValidateTree)。
func (t *SkillCardUpgradeTable) ValidateCurves(cards *SkillCardTable) error {
	type key struct {
		rarity uint32
		level  uint32
	}
	costByKey := make(map[key]uint32, len(t.rows))
	for _, row := range t.rows {
		k := key{rarity: row.GetRarity(), level: row.GetLevel()}
		if _, dup := costByKey[k]; dup {
			return fmt.Errorf("稀有度 %d 的 %d 级升级消耗配了多行,取值将取决于表内顺序",
				k.rarity, k.level)
		}
		costByKey[k] = row.GetShardCost()
	}

	if cards == nil {
		// 没有技能卡表就无从判断"要铺到几级",只能做到唯一性校验。
		// 调用方(加载校验器)应保证两张表同时存在;这里不 panic,让缺表由调用方报得更准。
		return nil
	}

	for rarity, maxLevel := range cards.MaxLevelByRarity() {
		var prev uint32
		for level := uint32(2); level <= maxLevel; level++ {
			cost, ok := costByKey[key{rarity: rarity, level: level}]
			if !ok {
				return fmt.Errorf("稀有度 %d 的升级曲线缺 %d 级(该稀有度有卡的等级上限是 %d);"+
					"缺档会让卡升到 %d 级后无法继续升且不报错", rarity, level, maxLevel, level-1)
			}
			if cost < prev {
				return fmt.Errorf("稀有度 %d 的升级消耗在 %d 级下降(%d → %d),疑似填错",
					rarity, level, prev, cost)
			}
			prev = cost
		}
	}
	return nil
}
