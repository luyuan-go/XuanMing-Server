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
		{Id: 9001, Name: "剑", Type: configpb.ItemType_ITEM_TYPE_EQUIPMENT, MaxStackSize: 1, EquipSlot: 1, IdentifyPoolId: 1, EquipScaleX: 1, EquipScaleY: 1, EquipScaleZ: 1},
		{Id: 9002, Name: "金币", Type: configpb.ItemType_ITEM_TYPE_MATERIAL, MaxStackSize: 9999, EquipScaleX: 1, EquipScaleY: 1, EquipScaleZ: 1},
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

// ── 比较符可用性(本次修复的回归判据)────────────────────────────────────────

// 任务进度是单调不减的累加器且达标槽不再累加,所以条件的达标集合必须**向上闭合**。
// 只有 GE / GT 满足。LE/LT 在 progress=0 时即为真 → 该槽永不累加、恒定达标(白送完成);
// EQ 是单点集合,单次事实 amount>1 一步跨过目标后永远不再相等(任务永久完不成)。
// 这三档必须挡在加载边界,而不是等玩家发现任务做不完或白送。
//
// 退掉 mission.go 里那段 ConditionMinFulfillingProgress 判据,本用例必红。
func TestValidateMissionCrossTables_RejectsNonUpwardClosedComparator(t *testing.T) {
	missions := []*configpb.MissionRow{
		{Id: 1, Name: "m1", MissionType: 1, ConditionIds: "10"},
	}
	items := []*configpb.ItemRow{{Id: 9001, Name: "剑", Type: configpb.ItemType_ITEM_TYPE_EQUIPMENT, MaxStackSize: 1, EquipSlot: 1, IdentifyPoolId: 1, EquipScaleX: 1, EquipScaleY: 1, EquipScaleZ: 1}}

	for _, tc := range []struct {
		name    string
		op      uint32
		wantErr bool
	}{
		{"GE 可用", ConditionCompareGE, false},
		{"GT 可用", ConditionCompareGT, false},
		{"LE 拒批次", ConditionCompareLE, true},
		{"LT 拒批次", ConditionCompareLT, true},
		{"EQ 拒批次", ConditionCompareEQ, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conditions := []*configpb.ConditionRow{
				{Id: 10, Name: "c10", ConditionCategory: 1, TargetCount: 5, ComparisonOp: tc.op},
			}
			mt, ct, rt, it := newMissionTablesForTest(t, missions, conditions, nil, items)
			err := ValidateMissionCrossTables(mt, ct, rt, it)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("比较符 %d 必须被加载期拒绝(否则白送完成或永久做不完)", tc.op)
				}
				if !strings.Contains(err.Error(), "比较符") {
					t.Fatalf("错误信息应点名比较符,got: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("比较符 %d 应放行,got: %v", tc.op, err)
			}
		})
	}
}

// 装备件数上限必须按**整条奖励累计**判,不是逐条判。
// 逐条判时「10 个不同装备各 64 件」= 640 件可以整批过审、落进 reward_pb 快照、
// 任务同事务置 CLAIMED,然后在发放侧的累计闸上永远发不出去 —— 快照是发放唯一入参,
// 不回读配置表,改表也救不回在途行,玩家永久损失该任务全部奖励。
//
// 退回逐条判(entry.Count > Max),本用例必红。
func TestValidateMissionCrossTables_EquipmentTotalAcrossEntries(t *testing.T) {
	missions := []*configpb.MissionRow{
		{Id: 1, Name: "m1", MissionType: 1, ConditionIds: "10", RewardId: 60},
	}
	conditions := []*configpb.ConditionRow{{Id: 10, Name: "c10", ConditionCategory: 1, TargetCount: 1}}
	items := []*configpb.ItemRow{
		{Id: 9001, Name: "剑", Type: configpb.ItemType_ITEM_TYPE_EQUIPMENT, MaxStackSize: 1, EquipSlot: 1, IdentifyPoolId: 1, EquipScaleX: 1, EquipScaleY: 1, EquipScaleZ: 1},
		{Id: 9002, Name: "盾", Type: configpb.ItemType_ITEM_TYPE_EQUIPMENT, MaxStackSize: 1, EquipSlot: 2, IdentifyPoolId: 1, EquipScaleX: 1, EquipScaleY: 1, EquipScaleZ: 1},
		{Id: 9003, Name: "金币", Type: configpb.ItemType_ITEM_TYPE_MATERIAL, MaxStackSize: 9999, EquipScaleX: 1, EquipScaleY: 1, EquipScaleZ: 1},
	}
	half := uint32(MaxRewardEquipmentInstances/2 + 1) // 两条各自合规,合计越界

	t.Run("多条装备合计超上限拒批次", func(t *testing.T) {
		rewards := []*configpb.RewardRow{{
			Id: 60, Name: "r60",
			ItemIds:    itoa(9001) + "," + itoa(9002),
			ItemCounts: itoa(half) + "," + itoa(half),
		}}
		mt, ct, rt, it := newMissionTablesForTest(t, missions, conditions, rewards, items)
		err := ValidateMissionCrossTables(mt, ct, rt, it)
		if err == nil {
			t.Fatalf("装备累计 %d 件超上限 %d,必须拒批次", 2*half, MaxRewardEquipmentInstances)
		}
		if !strings.Contains(err.Error(), "累计") {
			t.Fatalf("错误信息应点名累计件数,got: %v", err)
		}
	})

	t.Run("堆叠道具不计入装备累计", func(t *testing.T) {
		rewards := []*configpb.RewardRow{{
			Id: 60, Name: "r60",
			ItemIds:    itoa(9001) + "," + itoa(9003),
			ItemCounts: itoa(MaxRewardEquipmentInstances) + "," + itoa(999999),
		}}
		mt, ct, rt, it := newMissionTablesForTest(t, missions, conditions, rewards, items)
		if err := ValidateMissionCrossTables(mt, ct, rt, it); err != nil {
			t.Fatalf("装备恰好等于上限 + 大额堆叠货币应放行,got: %v", err)
		}
	})
}

// ── 本轮新增的三条数组/行数上限(§9.24 深度②集合条目上限)────────────────────

// validItemRow 造一条能过 validateItemRow 的道具行(该校验在 2026-08-11 被并发编辑者
// 加严:装备须 identify_pool_id>0,且三个缩放对**所有行**都必须 >0)。
func validItemRow(id uint32, equip bool) *configpb.ItemRow {
	row := &configpb.ItemRow{
		Id: id, Name: "it", MaxStackSize: 9999,
		EquipScaleX: 1, EquipScaleY: 1, EquipScaleZ: 1,
	}
	if equip {
		row.Type = configpb.ItemType_ITEM_TYPE_EQUIPMENT
		row.MaxStackSize = 1
		row.EquipSlot = 1
		row.IdentifyPoolId = 1
	} else {
		row.Type = configpb.ItemType_ITEM_TYPE_MATERIAL
	}
	return row
}

// 任务表行数上限是 §9.18「读取侧上限」的**前提**:完成集 player_mission_done 每玩家
// 每任务一行,规模就是任务表行数。没有这条闸,"完成集被任务表行数有界"只是一句描述,
// 而 loadState 在事务路径是全量 FOR UPDATE 载入(行锁数与事务时长随之线性增长)。
func TestValidateMissionCrossTables_RejectsTooManyMissionRows(t *testing.T) {
	conditions := []*configpb.ConditionRow{{Id: 10, Name: "c10", ConditionCategory: 1, TargetCount: 1}}
	items := []*configpb.ItemRow{validItemRow(9001, true)}

	build := func(n int) []*configpb.MissionRow {
		out := make([]*configpb.MissionRow, 0, n)
		for i := 1; i <= n; i++ {
			out = append(out, &configpb.MissionRow{
				Id: uint32(i), Name: "m", MissionType: 1, ConditionIds: "10",
			})
		}
		return out
	}

	t.Run("恰好等于上限放行", func(t *testing.T) {
		mt, ct, rt, it := newMissionTablesForTest(t, build(MaxMissionRows), conditions, nil, items)
		if err := ValidateMissionCrossTables(mt, ct, rt, it); err != nil {
			t.Fatalf("边界值应放行: %v", err)
		}
	})
	t.Run("超上限拒批次", func(t *testing.T) {
		mt, ct, rt, it := newMissionTablesForTest(t, build(MaxMissionRows+1), conditions, nil, items)
		err := ValidateMissionCrossTables(mt, ct, rt, it)
		if err == nil {
			t.Fatalf("任务表 %d 行超上限 %d,必须拒批次", MaxMissionRows+1, MaxMissionRows)
		}
		if !strings.Contains(err.Error(), "行数") {
			t.Fatalf("错误信息应点名行数,got: %v", err)
		}
	})
}

// 后续任务链条数上限:完成扇出会逐条 acceptInto,超 max_active_missions 后每条一条 WARN。
func TestValidateMissionRow_RejectsTooManyNextIDs(t *testing.T) {
	ids := make([]string, 0, MaxMissionNextIDs+1)
	for i := 2; i <= MaxMissionNextIDs+2; i++ {
		ids = append(ids, itoa(uint32(i)))
	}
	row := &configpb.MissionRow{
		Id: 1, Name: "m1", MissionType: 1, ConditionIds: "10",
		NextMissionIds: strings.Join(ids, ","),
	}
	err := validateMissionRow(row)
	if err == nil {
		t.Fatalf("后续任务 %d 条超上限 %d,必须拒行", len(ids), MaxMissionNextIDs)
	}
	if !strings.Contains(err.Error(), "后续任务数") {
		t.Fatalf("错误信息应点名后续任务数,got: %v", err)
	}
}

// 槽位取值条数上限:ConditionMatchesEventSlots 是线性扫描,
// 单次上报 = 事实数 × 活跃任务数 × 槽数 × 本上限次比较。
func TestValidateConditionRow_RejectsTooManySlotValues(t *testing.T) {
	vals := make([]string, 0, MaxConditionSlotValues+1)
	for i := 1; i <= MaxConditionSlotValues+1; i++ {
		vals = append(vals, itoa(uint32(i)))
	}
	row := &configpb.ConditionRow{
		Id: 10, Name: "c10", ConditionCategory: 1, TargetCount: 1,
		Slot1: strings.Join(vals, ","),
	}
	err := validateConditionRow(row)
	if err == nil {
		t.Fatalf("槽位取值 %d 条超上限 %d,必须拒行", len(vals), MaxConditionSlotValues)
	}
	if !strings.Contains(err.Error(), "取值条数") {
		t.Fatalf("错误信息应点名取值条数,got: %v", err)
	}
}

// 奖励道具条目数上限:按设计期望定(实际 1~5 条),不按 reward_pb 列容量定
// —— §9.24 明令「上限值按设计期望定,不按列类型上限定」,否则等于没设。
func TestValidateRewardRow_RejectsTooManyItemEntries(t *testing.T) {
	n := MaxRewardItemEntries + 1
	ids := make([]string, 0, n)
	counts := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		ids = append(ids, itoa(uint32(9000+i)))
		counts = append(counts, "1")
	}
	row := &configpb.RewardRow{
		Id: 60, Name: "r60",
		ItemIds:    strings.Join(ids, ","),
		ItemCounts: strings.Join(counts, ","),
	}
	err := validateRewardRow(row)
	if err == nil {
		t.Fatalf("道具条目 %d 条超上限 %d,必须拒行", n, MaxRewardItemEntries)
	}
	if !strings.Contains(err.Error(), "道具条目数") {
		t.Fatalf("错误信息应点名道具条目数,got: %v", err)
	}
}
