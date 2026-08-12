package configtable

import (
	"fmt"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// reward.go — RewardTable 手写伴生文件(任务域,docs/design/mission.md §2)。
// 首次由人创建(先于 configtable-gen 落仓),生成器发现已存在即不再覆盖。
//
// 本表是纯数据引用(D 版 reward 模块归属原则:「领取状态归业务系统,本表只回答给什么」)。
// item_ids 元素与道具表的存在性校验在 ValidateMissionCrossTables(mission.go)。
// 视图结构与通用访问 API 在 reward_table.gen.go(生成,勿手改)。

// RewardEntry 一条道具奖励(id + 数量),D 版嵌套 Rewardreward 的移植形态。
type RewardEntry struct {
	ItemConfigID uint32
	Count        uint32
}

// MaxRewardEquipmentInstances 单条奖励里**装备**类道具的数量上限。
//
// 只针对装备:装备没有堆叠,发放前要按件展开成 instance 列表
// (mission deliver → inventory.GrantInstances),数量**直接等于切片长度**——
// 策划把数量列手滑成 100000000 时,分配的不是一个大数字段而是一个上亿元素的切片,
// 发放侧当场 OOM,且快照落库后每轮补扫都会再炸一次(§16.5 容量边界)。
// 堆叠道具 / 货币不受此限(数量只是 pb 里的一个 uint32 字段,金币奖励几十万很正常)。
//
// 64 远超正常任务奖励设计(实际 1~5 件),只挡手滑;加载期在
// ValidateMissionCrossTables 拒批次,运行期 deliver 另有同值 fail-closed 闸
// (快照可能来自早于本上限的历史批次,或道具在热更里由堆叠改成了装备)。
const MaxRewardEquipmentInstances = 64

// MaxRewardItemEntries 单条奖励的道具条目数上限(§9.24 深度②集合条目上限)。
//
// mission_reward_log.reward_pb 是 VARBINARY(2048),单条 MissionRewardItem 约 9 字节,
// 光靠字节闸要到 ~200 条才拦得住 —— 那等于"按列类型上限设限",§9.24 明令不算数。
// 32 条按设计期望定(实际任务奖励 1~5 条),超了必是手滑或表列错位。
const MaxRewardItemEntries = 32

// validateRewardRow 逐行业务校验(生成的 newRewardTable 调用)。
func validateRewardRow(row *configpb.RewardRow) error {
	ids, err := parseUint32CSV(row.GetItemIds())
	if err != nil {
		return fmt.Errorf("道具ID数组格式非法: %w", err)
	}
	counts, err := parseUint32CSV(row.GetItemCounts())
	if err != nil {
		return fmt.Errorf("道具数量数组格式非法: %w", err)
	}
	if len(ids) != len(counts) {
		return fmt.Errorf("道具ID数组长度 %d 与道具数量数组长度 %d 不等", len(ids), len(counts))
	}
	if len(ids) > MaxRewardItemEntries {
		return fmt.Errorf("道具条目数 %d 超上限 %d(reward_pb 落库列 2048 字节,按设计期望而非列容量设限)",
			len(ids), MaxRewardItemEntries)
	}
	seen := make(map[uint32]struct{}, len(ids))
	for i, id := range ids {
		if id == 0 {
			return fmt.Errorf("道具ID第 %d 个元素为 0", i+1)
		}
		if _, dup := seen[id]; dup {
			return fmt.Errorf("道具ID %d 重复;同一奖励里重复道具应合并数量,重复行为发放语义不明", id)
		}
		seen[id] = struct{}{}
		if counts[i] == 0 {
			return fmt.Errorf("道具 %d 的数量为 0;发 0 个必是手滑", id)
		}
	}
	if len(ids) == 0 && row.GetExp() == 0 {
		return fmt.Errorf("空奖励行(无道具且经验为 0);任务表想表达无奖励应把奖励ID填 0,而不是指向空行")
	}
	return nil
}

// RewardItems 道具奖励列表(可能为空 = 纯经验奖励)。加载期已校验格式与等长。
func RewardItems(row *configpb.RewardRow) []RewardEntry {
	ids := mustUint32CSV(row.GetItemIds())
	counts := mustUint32CSV(row.GetItemCounts())
	if len(ids) != len(counts) {
		return nil // 防御:加载期已保证等长
	}
	out := make([]RewardEntry, 0, len(ids))
	for i, id := range ids {
		out = append(out, RewardEntry{ItemConfigID: id, Count: counts[i]})
	}
	return out
}
