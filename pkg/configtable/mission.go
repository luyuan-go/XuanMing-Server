package configtable

import (
	"fmt"
	"math"

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

// MaxMissionRows 任务表行数上限。
//
// 这不是"手滑护栏",是 §9.18 读取侧上限的**前提**:player_mission_done 每玩家每任务
// 至多一行,所以完成集的规模 = 任务表行数。CLAUDE.md §9.18 允许「单次全量返回 +
// 服务端 SQL LIMIT 兜底」达标,但那句话的前提是「列表被写入侧硬上限兜住到几百内」——
// 在有这个常量之前,"完成集被任务表行数有界"只是一句描述,没有任何代码拒过批次。
// 完成集同时还在**写路径**上:MutatePlayer / ApplyFactsTx 每次都 FOR UPDATE 全量载入,
// 行锁数与事务时长随它线性增长。
//
// 2000 与 internal/data/budgets.go 的行数预算同阶;真要上日常/成就任务突破这个量级,
// 应当改成 ListMissions 游标分页(proto 加 cursor/next_cursor),而不是调大本常量。
const MaxMissionRows = 2000

// MaxMissionNextIDs 单任务后续链条数上限(完成扇出会逐条 acceptInto,每条失败一条 WARN)。
const MaxMissionNextIDs = 16

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
	if len(nextIDs) > MaxMissionNextIDs {
		return fmt.Errorf("后续任务数 %d 超上限 %d(完成扇出逐条接取,超 max_active_missions 后每条一条 WARN)",
			len(nextIDs), MaxMissionNextIDs)
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
	if missions.Count() > MaxMissionRows {
		return fmt.Errorf("任务表行数 %d 超上限 %d;完成集 player_mission_done 每玩家每任务一行,"+
			"§9.18「读取侧上限」靠的正是这条写入侧硬上限,没有它 ListMissions 与事务内 loadState 都会无界增长",
			missions.Count(), MaxMissionRows)
	}

	for _, row := range missions.All() {
		for i, cid := range MissionConditionIDs(row) {
			if !conditions.Exists(cid) {
				return fmt.Errorf("mission %d 引用不存在的条件 %d", row.GetId(), cid)
			}
			cond, _ := conditions.ByID(cid)
			target := ConditionEffectiveTarget(cond, MissionSlotTarget(row, i))
			if _, ok := ConditionMinFulfillingProgress(cond.GetComparisonOp(), target); !ok {
				// 为什么这条闸在任务域而不是 validateConditionRow:条件件声明为跨系统通用
				// 判定件,「达标集合必须向上闭合」是**任务进度是单调累加计数器**才有的要求。
				// 将来若有快照型消费者(如「等级 <= 10」),LE/LT 在那里是合法的。
				return fmt.Errorf(
					"mission %d 第 %d 个条件 %d 的比较符=%d 不能用作任务条件(目标=%d):"+
						"任务进度是单调不减的累加器且达标槽不再累加,达标集合必须向上闭合。"+
						"LE/LT 在进度=0 时即为真 → 该槽永不累加、恒定达标(白送完成);"+
						"EQ 是单点集合,单次事实 amount>1 会一步跨过目标后永远不再相等(任务永久完不成);"+
						"GT 目标=MaxUint32 无可达值。当前只有 GE(=%d)与 GT(=%d)可用",
					row.GetId(), i+1, cid, cond.GetComparisonOp(), target,
					ConditionCompareGE, ConditionCompareGT)
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
		// 累计而不是逐条判:发放侧 deliver 闸的是**整条奖励展开出的 instance 切片总长**,
		// 加载期若只判单条,「10 个不同装备各 64 件」= 640 件可以整批过审、落进 reward_pb
		// 快照、任务同事务置 CLAIMED,然后在 deliver 的累计闸上**永远发不出去**:快照是
		// 发放唯一入参不回读配置表,改表也救不回在途行 → 玩家永久损失该任务全部奖励,
		// 且补扫每轮重试一次、FAILED 行不被保留期清理(SweepRewardLog 只清 GRANTED)。
		// 两侧必须同口径。
		var equipTotal uint32
		for _, entry := range RewardItems(row) {
			if !items.Exists(entry.ItemConfigID) {
				return fmt.Errorf("reward %d 引用不存在的道具 %d", row.GetId(), entry.ItemConfigID)
			}
			// 装备数量闸只能在这里做:validateRewardRow 是单表逐行校验,拿不到道具表,
			// 判不出这一条是装备(按件展开成实例)还是堆叠(数量只是一个字段)。
			if !items.IsEquipment(entry.ItemConfigID) {
				continue
			}
			equipTotal = saturatingAddU32(equipTotal, entry.Count)
			if equipTotal > MaxRewardEquipmentInstances {
				return fmt.Errorf("reward %d 的装备累计件数 %d 超上限 %d(累计到道具 %d 时越界;"+
					"装备按件展开成实例,总件数即切片长度,大数会打爆发放侧内存)",
					row.GetId(), equipTotal, MaxRewardEquipmentInstances, entry.ItemConfigID)
			}
		}
	}
	return nil
}

// saturatingAddU32 防止装备件数累加在校验途中回绕(回绕会让越界配置反而过审)。
func saturatingAddU32(a, b uint32) uint32 {
	if a > math.MaxUint32-b {
		return math.MaxUint32
	}
	return a + b
}
