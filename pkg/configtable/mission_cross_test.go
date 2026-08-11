// mission_cross_test.go — 任务域跨表校验(ValidateMissionCrossTables)的加载期门禁回归。
package configtable

import (
	"strings"
	"testing"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

func newMissionTablesForTest(t *testing.T, missions []*configpb.MissionRow,
	conditions []*configpb.ConditionRow, rewards []*configpb.RewardRow,
	items []*configpb.ItemRow) (*MissionTable, *ConditionTable, *RewardTable, *ItemTable) {
	t.Helper()
	mt, err := newMissionTable(&configpb.MissionTableData{Rows: missions})
	if err != nil {
		t.Fatalf("建任务表: %v", err)
	}
	ct, err := newConditionTable(&configpb.ConditionTableData{Rows: conditions})
	if err != nil {
		t.Fatalf("建条件表: %v", err)
	}
	rt, err := newRewardTable(&configpb.RewardTableData{Rows: rewards})
	if err != nil {
		t.Fatalf("建奖励表: %v", err)
	}
	it, err := newItemTable(&configpb.ItemTableData{Rows: items})
	if err != nil {
		t.Fatalf("建道具表: %v", err)
	}
	return mt, ct, rt, it
}

// 装备类奖励的数量上限必须在加载期拒批次。
//
// 装备没有堆叠,发放前按件展开成 instance 列表,**数量直接等于切片长度** ——
// 策划把数量列手滑成一个大数,发放侧当场按该数量分配内存;快照落库后补扫每轮再炸一次。
// 堆叠道具不受此限(数量只是 pb 里的一个字段,金币奖励几十万很正常)。
func TestValidateMissionCrossTables_EquipmentRewardCountBound(t *testing.T) {
	baseMissions := []*configpb.MissionRow{
		{Id: 1, Name: "m1", MissionType: 1, ConditionIds: "10", RewardId: 60},
	}
	baseConditions := []*configpb.ConditionRow{{Id: 10, Name: "c10", ConditionCategory: 1, TargetCount: 1}}
	items := []*configpb.ItemRow{
		// 9001 是装备(equip_slot>0 → IsEquipment);9002 是可堆叠消耗品。
		{Id: 9001, Name: "剑", Type: configpb.ItemType_ITEM_TYPE_EQUIPMENT, MaxStackSize: 1, EquipSlot: 1},
		{Id: 9002, Name: "金币", Type: configpb.ItemType_ITEM_TYPE_MATERIAL, MaxStackSize: 9999},
	}

	rewardWith := func(itemID, count uint32) []*configpb.RewardRow {
		return []*configpb.RewardRow{{
			Id: 60, Name: "r60",
			ItemIds:    itoa(itemID),
			ItemCounts: itoa(count),
		}}
	}

	t.Run("装备超上限拒批次", func(t *testing.T) {
		mt, ct, rt, it := newMissionTablesForTest(t, baseMissions, baseConditions,
			rewardWith(9001, MaxRewardEquipmentInstances+1), items)
		err := ValidateMissionCrossTables(mt, ct, rt, it)
		if err == nil {
			t.Fatal("装备数量超上限必须拒批次(否则发放侧按数量展开切片 → OOM)")
		}
		if !strings.Contains(err.Error(), "装备") {
			t.Fatalf("错误信息未点明装备数量闸: %v", err)
		}
	})

	t.Run("装备恰好等于上限放行", func(t *testing.T) {
		mt, ct, rt, it := newMissionTablesForTest(t, baseMissions, baseConditions,
			rewardWith(9001, MaxRewardEquipmentInstances), items)
		if err := ValidateMissionCrossTables(mt, ct, rt, it); err != nil {
			t.Fatalf("边界值应放行: %v", err)
		}
	})

	t.Run("堆叠道具大数量不受限", func(t *testing.T) {
		mt, ct, rt, it := newMissionTablesForTest(t, baseMissions, baseConditions,
			rewardWith(9002, 500_000), items)
		if err := ValidateMissionCrossTables(mt, ct, rt, it); err != nil {
			t.Fatalf("堆叠道具数量不该被装备闸误伤: %v", err)
		}
	})
}

func itoa(v uint32) string {
	if v == 0 {
		return "0"
	}
	var buf [10]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
