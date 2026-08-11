package configtable

import (
	"fmt"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// mission.go — MissionTable 手写伴生文件(任务域,docs/design/mission.md §2)。
// 首次由人创建(先于 configtable-gen 落仓),生成器发现已存在即不再覆盖。
//
// 视图结构与通用访问 API 在 mission_table.gen.go(生成,勿手改)。
// 跨表校验(条件/后续任务/奖励道具存在性 + 任务链环)在 ValidateMissionCrossTables,
// 由消费服务经 Store.AddValidator 注册——启动首载与每次热 reload 同一门禁。

// MaxMissionConditionSlots 单任务条件槽上限。
//
// 与存储直接挂钩:player_mission_active.progress 是 MissionProgressStorageRecord pb
// (VARBINARY(256),写入过 dbguard.CheckPayload),槽数上限是该列「集合条目上限」闸
// (§9.24 深度三上限之一)。8 远超正常任务设计(D 版实际用 1~3),只挡手滑。
const MaxMissionConditionSlots = 8

// validateMissionRow 逐行业务校验(生成的 newMissionTable 调用;
// 主键非零/唯一由生成代码兜住,reward_id 外键由生成器 fk 校验 + validateCrossTables 兜底)。
func validateMissionRow(row *configpb.MissionRow) error {
	condIDs, err := parseUint32CSV(row.GetConditionIds())
	if err != nil {
		return fmt.Errorf("条件ID数组格式非法: %w", err)
	}
	if len(condIDs) == 0 {
		return fmt.Errorf("条件ID为空;无条件的任务永远无法完成")
	}
	if len(condIDs) > MaxMissionConditionSlots {
		return fmt.Errorf("条件数 %d 超上限 %d(进度列存储闸,疑似手滑)", len(condIDs), MaxMissionConditionSlots)
	}
	for i, id := range condIDs {
		if id == 0 {
			return fmt.Errorf("条件ID第 %d 个元素为 0", i+1)
		}
	}

	targets, err := parseUint32CSV(row.GetTargetCounts())
	if err != nil {
		return fmt.Errorf("条件目标数组格式非法: %w", err)
	}
	if len(targets) > 0 && len(targets) != len(condIDs) {
		return fmt.Errorf("条件目标数组长度 %d 与条件ID数组长度 %d 不等(空 = 全用条件行目标;非空必须等长)",
			len(targets), len(condIDs))
	}

	nextIDs, err := parseUint32CSV(row.GetNextMissionIds())
	if err != nil {
		return fmt.Errorf("后续任务数组格式非法: %w", err)
	}
	for i, id := range nextIDs {
		if id == 0 {
			return fmt.Errorf("后续任务第 %d 个元素为 0", i+1)
		}
		if id == row.GetId() {
			return fmt.Errorf("后续任务包含自身(id=%d),直接自环", id)
		}
	}

	if row.GetAutoReward() > 0 && row.GetRewardId() == 0 {
		return fmt.Errorf("自动发奖开启但奖励ID为 0;自动发放无内容必是配置错")
	}
	return nil
}

// MissionConditionIDs 条件槽数组(与 progress 槽一一对应)。加载期已校验格式。
func MissionConditionIDs(row *configpb.MissionRow) []uint32 {
	return mustUint32CSV(row.GetConditionIds())
}

// MissionTargetCounts 条件目标覆盖数组(可能为空 = 全用条件行目标;元素 0 = 该槽不覆盖)。
func MissionTargetCounts(row *configpb.MissionRow) []uint32 {
	return mustUint32CSV(row.GetTargetCounts())
}

// MissionNextIDs 完成后自动接取的后续任务数组。
func MissionNextIDs(row *configpb.MissionRow) []uint32 {
	return mustUint32CSV(row.GetNextMissionIds())
}

// MissionSlotTarget 第 i 个条件槽的目标覆盖值(越界/未覆盖返回 0 = 用条件行目标)。
func MissionSlotTarget(row *configpb.MissionRow, i int) uint32 {
	targets := MissionTargetCounts(row)
	if i < 0 || i >= len(targets) {
		return 0
	}
	return targets[i]
}

// ValidateMissionCrossTables 任务域批次级跨表校验(消费服务 AddValidator 注册):
//   - mission.condition_ids 每元素存在于条件表;
//   - mission.next_mission_ids 每元素存在于任务表,且全表无链环(DFS);
//   - reward.item_ids 每元素存在于道具表。
//
// fk 注解只支持单值 uint32 列(reward_id 已用),数组列的引用完整性只能在这里兜。
// 链环必须加载期拒绝:运行期完成扇出的 16 轮迭代上限只是纵深兜底,不是许可
// (docs/design/mission.md §5.4)。
func ValidateMissionCrossTables(missions *MissionTable, conditions *ConditionTable, rewards *RewardTable, items *ItemTable) error {
	for _, row := range missions.All() {
		for _, cid := range MissionConditionIDs(row) {
			if !conditions.Exists(cid) {
				return fmt.Errorf("mission %d 引用不存在的条件 %d", row.GetId(), cid)
			}
		}
		for _, nid := range MissionNextIDs(row) {
			if !missions.Exists(nid) {
				return fmt.Errorf("mission %d 的后续任务 %d 不存在", row.GetId(), nid)
			}
		}
	}

	// next_mission_ids 链环检测(三色 DFS;含间接环)。
	const (
		white = 0 // 未访问
		gray  = 1 // 在栈上
		black = 2 // 已完成
	)
	color := make(map[uint32]uint8, missions.Count())
	var visit func(id uint32, path []uint32) error
	visit = func(id uint32, path []uint32) error {
		switch color[id] {
		case gray:
			return fmt.Errorf("后续任务链成环: %v → %d", path, id)
		case black:
			return nil
		}
		color[id] = gray
		row, _ := missions.ByID(id)
		for _, nid := range MissionNextIDs(row) {
			if err := visit(nid, append(path, id)); err != nil {
				return err
			}
		}
		color[id] = black
		return nil
	}
	for _, row := range missions.All() {
		if err := visit(row.GetId(), nil); err != nil {
			return err
		}
	}

	for _, row := range rewards.All() {
		for _, entry := range RewardItems(row) {
			if !items.Exists(entry.ItemConfigID) {
				return fmt.Errorf("reward %d 引用不存在的道具 %d", row.GetId(), entry.ItemConfigID)
			}
			// 装备数量闸只能在这里做:validateRewardRow 是单表逐行校验,拿不到道具表,
			// 判不出这一条是装备(按件展开成实例)还是堆叠(数量只是一个字段)。
			if items.IsEquipment(entry.ItemConfigID) && entry.Count > MaxRewardEquipmentInstances {
				return fmt.Errorf("reward %d 的装备 %d 数量 %d 超上限 %d(装备按件展开成实例,数量即切片长度,大数会打爆发放侧内存)",
					row.GetId(), entry.ItemConfigID, entry.Count, MaxRewardEquipmentInstances)
			}
		}
	}
	return nil
}
