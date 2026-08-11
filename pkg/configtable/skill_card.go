package configtable

import (
	"fmt"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// skill_card.go — SkillCardTable 手写伴生文件。
// 首次由 configtable-gen 创建(仅当文件不存在),此后归人维护,生成器不再覆盖。
//
// 视图结构与通用访问 API(All/ByID/Exists/Count/ByIDs/RandOne/Where/First/ListBySkillId)在
// skill_card_table.gen.go(生成,勿手改)。

// 技能卡稀有度。决定走哪条升级消耗曲线(j_技能卡升级.xlsx 按稀有度分档)。
const (
	SkillCardRarityCommon    uint32 = 1 // 普通
	SkillCardRarityRare      uint32 = 2 // 稀有
	SkillCardRarityEpic      uint32 = 3 // 史诗
	SkillCardRarityLegendary uint32 = 4 // 传说
)

// MaxSkillCardLevel 是等级上限的上限。
//
// 与升级消耗表的行数直接挂钩:每一级都要有一行消耗,上限填大了会让 ValidateCurves
// 报出一堆"缺消耗行"。取一个策划正常填不到的量级,只挡手滑(§16.5 容量边界)。
const MaxSkillCardLevel uint32 = 30

// validateSkillCardRow 逐行业务校验(生成的 newSkillCardTable 调用;
// 主键非零/唯一已由生成代码兜住,类型/必填/外键已由生成器在导表阶段校验)。
func validateSkillCardRow(row *configpb.SkillCardRow) error {
	switch row.GetRarity() {
	case SkillCardRarityCommon, SkillCardRarityRare, SkillCardRarityEpic, SkillCardRarityLegendary:
	default:
		return fmt.Errorf("稀有度(rarity=%d)不是合法取值(1=普通 2=稀有 3=史诗 4=传说);"+
			"非法稀有度查不到升级曲线,该卡将永远无法升级", row.GetRarity())
	}
	if row.GetMaxLevel() == 0 {
		return fmt.Errorf("等级上限(max_level)为 0;初始等级就是 1,上限至少为 1")
	}
	if row.GetMaxLevel() > MaxSkillCardLevel {
		return fmt.Errorf("等级上限(max_level=%d)超过 %d,疑似手滑", row.GetMaxLevel(), MaxSkillCardLevel)
	}
	return nil
}

// RaritiesInUse 返回本表实际用到的稀有度集合。
//
// 供升级消耗表做交叉校验:只有卡在用的稀有度才要求有完整曲线——
// 反过来要求"曲线表里每个稀有度都得有卡"是错的(策划可以先铺曲线再加卡)。
func (t *SkillCardTable) RaritiesInUse() map[uint32]struct{} {
	out := make(map[uint32]struct{}, 4)
	for _, row := range t.rows {
		out[row.GetRarity()] = struct{}{}
	}
	return out
}

// MaxLevelByRarity 返回每个稀有度下所有卡的最高等级上限。
//
// 升级曲线必须覆盖到这个等级为止:某张传说卡上限 5,而曲线只铺到 4,
// 表现为"卡升到 4 级之后按钮没反应",不报错——必须在加载期挡住。
func (t *SkillCardTable) MaxLevelByRarity() map[uint32]uint32 {
	out := make(map[uint32]uint32, 4)
	for _, row := range t.rows {
		if row.GetMaxLevel() > out[row.GetRarity()] {
			out[row.GetRarity()] = row.GetMaxLevel()
		}
	}
	return out
}
