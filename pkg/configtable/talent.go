package configtable

import (
	"fmt"
	"math"
	"sort"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// talent.go — TalentTable 手写伴生文件。
// 首次由 configtable-gen 创建(仅当文件不存在),此后归人维护,生成器不再覆盖。
// 表私有的逐行业务校验写在 validateTalentRow;域方法(业务语义查询)也加在本文件。
//
// 视图结构与通用访问 API(All/ByID/Exists/Count/ByIDs/RandOne/Where/First)在
// talent_table.gen.go(tools/configtable-gen 生成,勿手改)。

// MaxTalentLevel 是单个专精节点等级上限的硬约束。
//
// max_level 与 cost_per_level 都是裸 uint32 且支持热更,总消耗按 Σ 等级 × 每级消耗 计算;
// 若允许任意大值,一张热更进来的超大表能让总消耗在 uint32 上回绕(回绕后反而"点得起"),
// 或让界面生成天量列表行。取 100 给策划留足余量,同时把乘法钳在可控量级
// (§16.5 容量/溢出边界)。
const MaxTalentLevel = 100

// MaxTalentCostPerLevel 是单级消耗上限,与 MaxTalentLevel 一起把单节点总消耗钳在
// 100 × 1000 = 10 万以内,远离 uint32 上界。
const MaxTalentCostPerLevel = 1000

// validateTalentRow 逐行业务校验(生成的 newTalentTable 调用;
// 主键非零/唯一已由生成代码兜住,类型/必填已由生成器在导表阶段校验)。
//
// 跨行约束(前置存在 / 前置等级合法 / 依赖无环)不能在这里做——本钩子看不到其它行,
// 统一放在 ValidateTree,由服务端加载校验器调用。
func validateTalentRow(row *configpb.TalentRow) error {
	if row.GetName() == "" {
		return fmt.Errorf("名称(name)为空")
	}
	if row.GetMaxLevel() == 0 {
		return fmt.Errorf("等级上限(max_level)为 0,该专精永远点不出来")
	}
	if row.GetMaxLevel() > MaxTalentLevel {
		return fmt.Errorf("等级上限(max_level=%d)超过上限 %d(防总消耗溢出)", row.GetMaxLevel(), MaxTalentLevel)
	}
	if row.GetCostPerLevel() == 0 {
		return fmt.Errorf("每级消耗(cost_per_level)为 0,该专精可无限点满")
	}
	if row.GetCostPerLevel() > MaxTalentCostPerLevel {
		return fmt.Errorf("每级消耗(cost_per_level=%d)超过上限 %d(防总消耗溢出)",
			row.GetCostPerLevel(), MaxTalentCostPerLevel)
	}
	// 自己依赖自己必然无解,单行即可判定,不必等 ValidateTree 的环检测。
	if row.GetRequireTalentId() == row.GetId() {
		return fmt.Errorf("前置专精(require_talent_id)指向自身")
	}
	if row.GetRequireTalentId() == 0 && row.GetRequireTalentLevel() != 0 {
		return fmt.Errorf("无前置专精却填了前置等级(require_talent_level=%d)", row.GetRequireTalentLevel())
	}
	if row.GetRequireTalentId() != 0 && row.GetRequireTalentLevel() == 0 {
		return fmt.Errorf("填了前置专精(%d)却未填前置等级", row.GetRequireTalentId())
	}
	return nil
}

// ValidateTree 整表跨行校验:前置存在、前置等级不超过前置节点上限、依赖无环。
//
// 前置关系是自引用外键,而生成器明确拒绝 (excel_fk) 自引用,因此这项校验只能落在这里;
// 由各服务的 configtable 加载校验器调用(做法对齐 PlayerLevelExpTable.ValidateCurve),
// 任一条不过则整批不切换,坏表挡在加载边界而不是等玩家点专精时才炸。
func (t *TalentTable) ValidateTree() error {
	for _, row := range t.rows {
		reqID := row.GetRequireTalentId()
		if reqID == 0 {
			continue
		}
		req, ok := t.byID[reqID]
		if !ok {
			return fmt.Errorf("专精 %d 的前置专精 %d 不存在", row.GetId(), reqID)
		}
		if lv := row.GetRequireTalentLevel(); lv > req.GetMaxLevel() {
			return fmt.Errorf("专精 %d 要求前置 %d 达到 %d 级,但其等级上限只有 %d(该专精永远点不出来)",
				row.GetId(), reqID, lv, req.GetMaxLevel())
		}
	}

	// 环检测:每个节点至多一条出边(require_talent_id),沿链走一定终止于 0、已判定安全的节点,
	// 或回到本次路径中的节点(成环)。访问过的节点缓存到 safe,整表 O(N)。
	safe := make(map[uint32]bool, len(t.rows))
	for _, row := range t.rows {
		if safe[row.GetId()] {
			continue
		}
		path := make(map[uint32]bool)
		var chain []uint32
		cur := row.GetId()
		for cur != 0 && !safe[cur] {
			if path[cur] {
				return fmt.Errorf("专精前置关系成环: %v", append(chain, cur))
			}
			path[cur] = true
			chain = append(chain, cur)
			node, ok := t.byID[cur]
			if !ok {
				// 前置缺失已在上面的循环报过;这里只做保守终止,不重复报错。
				break
			}
			cur = node.GetRequireTalentId()
		}
		for _, id := range chain {
			safe[id] = true
		}
	}
	return nil
}

// MaxLevelOf 取专精等级上限;节点不存在返回 0(调用方据此判定"未知专精")。
func (t *TalentTable) MaxLevelOf(talentID uint32) uint32 {
	row, ok := t.byID[talentID]
	if !ok {
		return 0
	}
	return row.GetMaxLevel()
}

// ValidateAllocation 校验一份完整专精分配,返回逐节点消耗与总消耗点数。
//
// levels 是「节点 ID → 等级」的完整分配(等级 0 的节点不应出现,由调用方剔除),
// 对齐服务端 SetTalents 的全量替换语义:每次提交都是一份完整方案,因此前置关系
// 只看本次方案自身,不看库里旧数据——否则"先点满前置、再单独洗掉前置"就能留下悬空节点。
//
// 返回的总消耗供调用方与玩家可用专精点比对;逐节点消耗(节点 ID → 等级 × 每级消耗)
// 供调用方落库,让读取侧不必再按 Σ 等级 反推——反推在 cost_per_level≠1 时会把
// 已花掉的点算少,读出来的可点数比实际多。**一次调用取一份表快照**,校验与逐节点
// 消耗必然同版本;分成两次查表则可能跨热更边界混算(§9.15)。
// 任一条不满足返回 error。
func (t *TalentTable) ValidateAllocation(levels map[uint32]uint32) (map[uint32]uint32, uint32, error) {
	// map 遍历顺序不稳定,先定序,保证同一份非法分配每次报同一条错误(便于定位与测试)。
	ids := make([]uint32, 0, len(levels))
	for id := range levels {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var total uint64
	costs := make(map[uint32]uint32, len(levels))
	for _, id := range ids {
		level := levels[id]
		row, ok := t.byID[id]
		if !ok {
			return nil, 0, fmt.Errorf("专精 %d 不在配置表中", id)
		}
		if level == 0 {
			return nil, 0, fmt.Errorf("专精 %d 等级为 0(等级 0 应从分配中移除)", id)
		}
		if level > row.GetMaxLevel() {
			return nil, 0, fmt.Errorf("专精 %d 等级 %d 超过上限 %d", id, level, row.GetMaxLevel())
		}
		if reqID := row.GetRequireTalentId(); reqID != 0 {
			// 前置必须在**本次方案**里达标,不看历史分配。
			if got := levels[reqID]; got < row.GetRequireTalentLevel() {
				return nil, 0, fmt.Errorf("专精 %d 需要前置专精 %d 达到 %d 级,本次方案只有 %d 级",
					id, reqID, row.GetRequireTalentLevel(), got)
			}
		}
		// 单节点消耗已由 validateTalentRow 的上限钳在 10 万内(level ≤ 100 × cost ≤ 1000),
		// 单独放进 uint32 不会溢出;这里再用 uint64 累加并逐步比对 uint32 上界,
		// 保证任意行数下总消耗都不回绕。
		nodeCost := uint64(level) * uint64(row.GetCostPerLevel())
		costs[id] = uint32(nodeCost)
		total += nodeCost
		if total > math.MaxUint32 {
			return nil, 0, fmt.Errorf("专精总消耗超出上限(累加到节点 %d 时已达 %d)", id, total)
		}
	}
	return costs, uint32(total), nil
}
