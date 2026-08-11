package configtable

import (
	"fmt"
	"sort"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// talent_effect.go — TalentEffectTable 手写伴生文件。
// 首次由 configtable-gen 创建(仅当文件不存在),此后归人维护,生成器不再覆盖。
// 表私有的逐行业务校验写在 validateTalentEffectRow;域方法(业务语义查询)也加在本文件。
//
// 视图结构与通用访问 API(All/ByID/Exists/Count/ByIDs/RandOne/Where/First/ListByTalentId)在
// talent_effect_table.gen.go(tools/configtable-gen 生成,勿手改)。

// allowedTalentAttrKeys 是专精效果可以作用的 GAS 属性名白名单。
//
// **权威在客户端**:Pandora-Client-SVN/Pandora/Source/Pandora/Public/GAS/MyEntityAttrSet.h
// 的 UMyEntityAttrSet。这里是校验用副本,不是第二份权威(§9.22)——服务端不消费这些数值,
// 但必须在加载期挡住拼错的键:attr_key 写错在 DS 上的表现是"这个天赋点了完全没反应",
// 既不报错也不崩,是最难定位的一类配置事故。宁可在导表/加载边界整批拒绝。
//
// ⚠️ UMyEntityAttrSet 增删属性时必须同步本表(以及 TestTalentEffectAttrKeyWhitelist 的注释)。
var allowedTalentAttrKeys = map[string]struct{}{
	"Hp":            {}, // 血量
	"Atk":           {}, // 攻击力
	"Defense":       {}, // 防御
	"Shield":        {}, // 护盾
	"CritChance":    {}, // 暴击率
	"CritDamage":    {}, // 暴击伤害
	"HitChance":     {}, // 命中率
	"DodgeChance":   {}, // 闪避率
	"MoveSpeedRate": {}, // 移动速度倍率
	"AtkSpeedRate":  {}, // 攻击速度倍率
}

// MaxTalentEffectValuePerLevel 是单级加成绝对值上限。
//
// 与 MaxTalentLevel(100)相乘把单节点加成钳在 100 万以内:配置是热更进来的,
// 一个多打几个零的数值会让 DS 上出现"一个天赋点满血量加十亿"的不可玩局面。
// 取一个策划正常填不到的量级,只挡明显手滑,不干涉数值设计(§16.5 容量边界)。
const MaxTalentEffectValuePerLevel = 10000

// validateTalentEffectRow 逐行业务校验(生成的 newTalentEffectTable 调用;
// 主键非零/唯一已由生成代码兜住,类型/必填/外键已由生成器在导表阶段校验)。
//
// 跨行约束(同一专精不得对同一属性重复加成)看不到其它行,放在 ValidateEffects。
func validateTalentEffectRow(row *configpb.TalentEffectRow) error {
	if _, ok := allowedTalentAttrKeys[row.GetAttrKey()]; !ok {
		return fmt.Errorf("属性键(attr_key=%q)不是 UE GAS 属性名,该效果在 DS 上永远不会生效;"+
			"合法取值见 MyEntityAttrSet.h", row.GetAttrKey())
	}
	if row.GetValuePerLevel() == 0 {
		return fmt.Errorf("每级数值(value_per_level)为 0,该效果行没有任何作用")
	}
	if v := row.GetValuePerLevel(); v > MaxTalentEffectValuePerLevel || v < -MaxTalentEffectValuePerLevel {
		return fmt.Errorf("每级数值(value_per_level=%v)超出 ±%d,疑似多打了零",
			v, MaxTalentEffectValuePerLevel)
	}
	return nil
}

// ValidateEffects 整表跨行校验:同一专精不得对同一属性配置多行。
//
// 重复行不会报错,只会让加成翻倍——是"数值莫名其妙偏高"这类问题里最难查的来源,
// 必须在加载边界整批拒绝。由各服务的 configtable 加载校验器调用
// (做法对齐 TalentTable.ValidateTree / PlayerLevelExpTable.ValidateCurve)。
func (t *TalentEffectTable) ValidateEffects() error {
	type key struct {
		talentID uint32
		attrKey  string
	}
	seen := make(map[key]uint32, len(t.rows))
	for _, row := range t.rows {
		k := key{talentID: row.GetTalentId(), attrKey: row.GetAttrKey()}
		if firstID, dup := seen[k]; dup {
			return fmt.Errorf("专精 %d 对属性 %s 配了多行效果(行 %d 与行 %d),加成会翻倍",
				k.talentID, k.attrKey, firstID, row.GetId())
		}
		seen[k] = row.GetId()
	}
	return nil
}

// TalentAttrBonus 是一份专精分配换算出来的单条属性加成(展示 / 下发用聚合结果)。
type TalentAttrBonus struct {
	AttrKey string
	Value   float32
}

// ResolveBonuses 把一份专精分配(节点 ID → 等级)换算成属性加成合计。
//
// 每个节点的加成 = 等级 × 该节点每条效果的每级数值;同一属性跨节点累加
// (如专精 8 与专精 1 都加 Atk)。结果按属性名定序,保证同一份分配每次输出一致
// (map 遍历顺序不稳定会让下发内容抖动、测试随机失败)。
//
// 分配里出现本表没有效果行的节点是合法的(纯解锁型节点),跳过即可,不报错。
func (t *TalentEffectTable) ResolveBonuses(levels map[uint32]uint32) []TalentAttrBonus {
	sum := make(map[string]float32)
	for talentID, level := range levels {
		if level == 0 {
			continue
		}
		for _, row := range t.ListByTalentId(talentID) {
			sum[row.GetAttrKey()] += float32(level) * row.GetValuePerLevel()
		}
	}
	if len(sum) == 0 {
		return nil
	}

	keys := make([]string, 0, len(sum))
	for k := range sum {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	out := make([]TalentAttrBonus, 0, len(keys))
	for _, k := range keys {
		out = append(out, TalentAttrBonus{AttrKey: k, Value: sum[k]})
	}
	return out
}
