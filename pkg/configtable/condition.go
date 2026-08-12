package configtable

import (
	"fmt"
	"math"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// condition.go — ConditionTable 手写伴生文件(任务域,docs/design/mission.md §2)。
// 首次由人创建(先于 configtable-gen 落仓),生成器发现已存在即不再覆盖。
//
// 除逐行校验外,本文件还承载 D 版 condition_util 的移植(五比较器 / 槽位过滤 / clamp):
// 条件是跨系统通用判定件(任务首用,成就/活动将来复用),判定语义与表同源,故放本包
// 而不是散在各服务 biz 里。
//
// 视图结构与通用访问 API 在 condition_table.gen.go(生成,勿手改)。

// 条件类别(取值与 pandora.mission.v1.MissionConditionCategory 一一对应;
// 1-8 继承 luyuan/mmorpg eConditionType,9 起为本仓扩展)。
const (
	ConditionCategoryKillMonster       uint32 = 1 // 杀怪;槽位1=怪物配置ID
	ConditionCategoryTalkNpc           uint32 = 2 // 与 NPC 对话;槽位1=npc_id
	ConditionCategoryCompleteCondition uint32 = 3 // 完成指定条件;槽位1=condition_id
	ConditionCategoryUseItem           uint32 = 4 // 使用道具;槽位1=item_config_id
	ConditionCategoryInteract          uint32 = 5 // 交互;槽位1=interact_id
	ConditionCategoryLevelUp           uint32 = 6 // 升级;槽位1=达到的等级
	ConditionCategoryCustom            uint32 = 7 // 自定义
	ConditionCategoryCompleteMission   uint32 = 8 // 完成任务;槽位1=mission_config_id
	ConditionCategoryPickupItem        uint32 = 9 // 拾取物品;槽位1=item_config_id

	conditionCategoryMax uint32 = 9
)

// 比较符(「比较符」列取值;继承 D 版 kComparisonFunctions 的下标语义)。
const (
	ConditionCompareGE uint32 = 0 // 进度 >= 目标
	ConditionCompareGT uint32 = 1 // 进度 >  目标
	ConditionCompareLE uint32 = 2 // 进度 <= 目标
	ConditionCompareLT uint32 = 3 // 进度 <  目标
	ConditionCompareEQ uint32 = 4 // 进度 == 目标

	conditionCompareMax uint32 = 4
)

// MaxConditionSlotValues 单个槽位过滤集合的取值条数上限(§9.24 深度②集合条目上限)。
// ConditionMatchesEventSlots 是线性扫描:单次上报最多 max_facts_per_report 条事实 ×
// 每玩家最多 max_active_missions 个活跃任务 × 8 个槽 × 本上限次比较,填大了纯烧 CPU。
// 32 远超正常设计(实际 1~5 个 ID),只挡手滑。
const MaxConditionSlotValues = 32

// validateConditionRow 逐行业务校验(生成的 newConditionTable 调用)。
func validateConditionRow(row *configpb.ConditionRow) error {
	cat := row.GetConditionCategory()
	if cat == 0 || cat > conditionCategoryMax {
		return fmt.Errorf("条件类别(condition_category=%d)不在 [1,%d];类别不认识的条件永远不会被任何事实命中",
			cat, conditionCategoryMax)
	}
	if row.GetComparisonOp() > conditionCompareMax {
		return fmt.Errorf("比较符(comparison_op=%d)不在 [0,%d]", row.GetComparisonOp(), conditionCompareMax)
	}
	for i, slot := range []string{row.GetSlot1(), row.GetSlot2(), row.GetSlot3(), row.GetSlot4()} {
		vals, err := parseUint32CSV(slot)
		if err != nil {
			return fmt.Errorf("槽位%d 数组格式非法: %w", i+1, err)
		}
		if len(vals) > MaxConditionSlotValues {
			return fmt.Errorf("槽位%d 取值条数 %d 超上限 %d(槽位匹配是线性扫描,填大了纯烧 CPU)",
				i+1, len(vals), MaxConditionSlotValues)
		}
		for _, v := range vals {
			if v == 0 {
				return fmt.Errorf("槽位%d 含元素 0;槽位过滤值是配置 ID/量值,0 必是手滑", i+1)
			}
		}
	}
	return nil
}

// ConditionSlotFilters 返回四个槽位过滤集合(空槽 = nil = 不过滤)。加载期已校验格式。
func ConditionSlotFilters(row *configpb.ConditionRow) [4][]uint32 {
	return [4][]uint32{
		mustUint32CSV(row.GetSlot1()),
		mustUint32CSV(row.GetSlot2()),
		mustUint32CSV(row.GetSlot3()),
		mustUint32CSV(row.GetSlot4()),
	}
}

// ConditionEffectiveTarget 有效目标值:override>0(任务行「条件目标」)优先,否则条件行「目标值」。
// 继承 D 版 targetCountOverride 语义。
func ConditionEffectiveTarget(row *configpb.ConditionRow, targetOverride uint32) uint32 {
	if targetOverride > 0 {
		return targetOverride
	}
	return row.GetTargetCount()
}

// ConditionIsFulfilled 判断进度是否满足条件(D 版 condition_util::IsFulfilled)。
// 非法比较符返回 false(加载期已挡,防御)。
func ConditionIsFulfilled(row *configpb.ConditionRow, progress, targetOverride uint32) bool {
	target := ConditionEffectiveTarget(row, targetOverride)
	switch row.GetComparisonOp() {
	case ConditionCompareGE:
		return progress >= target
	case ConditionCompareGT:
		return progress > target
	case ConditionCompareLE:
		return progress <= target
	case ConditionCompareLT:
		return progress < target
	case ConditionCompareEQ:
		return progress == target
	default:
		return false
	}
}

// ConditionMinFulfillingProgress 返回「刚好达标」的最小进度值,以及**该比较符能否用在
// 单调累加计数器上**(第二个返回值)。
//
// 任务进度是单调不减的累加器(saturatingAdd + 达标槽不再累加),所以一个比较符要能用,
// 它的达标集合必须**向上闭合**:一旦为真,更大的进度也必须为真。逐个检查:
//
//	GE  progress >= target   向上闭合 ✓   最小达标值 = target
//	GT  progress >  target   向上闭合 ✓   最小达标值 = target+1
//	LE  progress <= target   ✗ 在 progress=0 就为真,且越推进越假
//	LT  progress <  target   ✗ 同上
//	EQ  progress == target   ✗ 单点集合,amount>1 会一步跨过去后永不再等
//
// 只有 GE/GT 返回 ok=true。LE/LT/EQ 用作任务条件的后果分别是「白送完成」与「永远
// 完不成」,由 ValidateMissionCrossTables 在加载期整批拒绝(mission.go)。
func ConditionMinFulfillingProgress(comparisonOp, target uint32) (uint32, bool) {
	switch comparisonOp {
	case ConditionCompareGE:
		return target, true
	case ConditionCompareGT:
		if target == math.MaxUint32 {
			// GT MaxUint32 无解:没有任何 uint32 大于它,配置错而非可 clamp 的形态。
			return math.MaxUint32, false
		}
		return target + 1, true
	default:
		return 0, false
	}
}

// ConditionClampIfFulfilled 达标后把进度 clamp 到**最小达标值**,未达标原样返回
// (D 版 condition_util::ClampIfFulfilled:进度条不超冲,存储不膨胀)。
//
// 必须 clamp 到最小达标值而不是 target 本身 —— 否则 GT 会被 clamp 打回未达标:
// target=5 的 GT 条件,进度 6 达标 → clamp 到 5 → 再判定 `5 > 5` 为假 → 任务判定
// 回到未完成,而进度已被写死在 5;下一条事实推到 6 又被 clamp 回 5,**永久活锁,
// 任务再也完不成**。clamp 的不变量是「clamp 不得改变达标与否」,GT 的最小达标值
// 是 target+1。
//
// 非单调可用的比较符(LE/LT/EQ)不 clamp:它们达标时 progress > target 本就不可能
// 成立(旧实现同样不会 clamp),行为不变;这类比较符用作任务条件已在加载期被拒。
func ConditionClampIfFulfilled(row *configpb.ConditionRow, progress, targetOverride uint32) uint32 {
	if !ConditionIsFulfilled(row, progress, targetOverride) {
		return progress
	}
	target := ConditionEffectiveTarget(row, targetOverride)
	floor, ok := ConditionMinFulfillingProgress(row.GetComparisonOp(), target)
	if !ok {
		return progress
	}
	if progress > floor {
		return floor
	}
	return progress
}

// ConditionMatchesEventSlots 判断事实的槽位值是否命中本条件的槽位过滤
// (D 版 condition_util::MatchesEventSlots):
//   - 配置了取值集合的槽才参与判定(空槽跳过);
//   - 事实第 N 槽的值必须落在条件第 N 槽集合内;
//   - 全部非空槽命中才算匹配;一个非空槽都没有 = 匹配任意同类事实。
//
// 事实槽位数不足时,缺位的非空槽判不命中(fail-closed,与 D 版一致)。
func ConditionMatchesEventSlots(row *configpb.ConditionRow, eventSlotValues []uint32) bool {
	filters := ConditionSlotFilters(row)
	configured, matched := 0, 0
	for i, filter := range filters {
		if len(filter) == 0 {
			continue
		}
		configured++
		if i >= len(eventSlotValues) {
			continue
		}
		v := eventSlotValues[i]
		for _, want := range filter {
			if want == v {
				matched++
				break
			}
		}
	}
	return configured == 0 || matched == configured
}
