package configtable

import (
	"slices"
	"testing"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

// query_test.go — 通用投影查询(query.go)测试。重点钉三件事:
// 顺序契约(IDs/Values 加载序、DistinctValues 升序)、返回值是调用方独占副本、空表零值安全。

// TestIDsLoadOrder IDs 必须按加载序,而不是主键序、更不是 map 迭代序。
// (从 byID map 迭代出来每次顺序都不同 → 下发客户端的列表抖动、测试 flaky。)
func TestIDsLoadOrder(t *testing.T) {
	tbl := mustLevelTable(t, battleRow(9, "c"), battleRow(6, "a"), battleRow(7, "b"))
	if got := IDs(tbl.All()); !slices.Equal(got, []uint32{9, 6, 7}) {
		t.Fatalf("IDs=%v,应按加载序 [9 6 7]", got)
	}
	// 反复调用必须完全一致(若实现改成 map 迭代,这里会随机红)
	for i := 0; i < 20; i++ {
		if got := IDs(tbl.All()); !slices.Equal(got, []uint32{9, 6, 7}) {
			t.Fatalf("第 %d 次 IDs=%v,顺序不稳定", i, got)
		}
	}
}

// TestIDsReturnsCallerOwnedCopy 返回值必须是调用方独占副本:
// 调用方 sort / 改写不得影响表内部状态,也不得影响下一次调用。
func TestIDsReturnsCallerOwnedCopy(t *testing.T) {
	tbl := mustLevelTable(t, battleRow(9, "c"), battleRow(6, "a"), battleRow(7, "b"))

	got := IDs(tbl.All())
	slices.Sort(got) // 调用方随意 sort
	got[0] = 12345   // 调用方随意改写

	if again := IDs(tbl.All()); !slices.Equal(again, []uint32{9, 6, 7}) {
		t.Fatalf("调用方改写污染了表内部状态,再次 IDs=%v", again)
	}
	// 行本身也不能被动过(投影只读行,不碰行)
	if tbl.All()[0].GetId() != 9 {
		t.Fatalf("行被改动,All()[0].Id=%d", tbl.All()[0].GetId())
	}
}

// TestValuesProjection Values 是纯投影:不去重、长度与行数一致、保持加载序。
func TestValuesProjection(t *testing.T) {
	tbl := mustLevelTable(t, battleRow(6, "a"), battleRow(7, "b"), battleRow(9, "a"))

	names := Values(tbl.All(), (*configpb.LevelRow).GetName)
	if !slices.Equal(names, []string{"a", "b", "a"}) {
		t.Fatalf("Values(Name)=%v,应保留重复且按加载序", names)
	}
	if len(names) != tbl.Count() {
		t.Fatalf("Values 长度 %d 应等于行数 %d", len(names), tbl.Count())
	}
}

// TestDistinctValues 去重 + 升序;重复值只留一个,且与行的排列无关。
func TestDistinctValues(t *testing.T) {
	tbl := mustLevelTable(t, battleRow(9, "b"), battleRow(6, "a"), battleRow(7, "b"), battleRow(8, "a"))

	names := DistinctValues(tbl.All(), (*configpb.LevelRow).GetName)
	if !slices.Equal(names, []string{"a", "b"}) {
		t.Fatalf("DistinctValues(Name)=%v,应为升序去重 [a b]", names)
	}
	// 换个行序,键集合必须完全一致(升序的意义)
	shuffled := mustLevelTable(t, battleRow(6, "a"), battleRow(9, "b"), battleRow(8, "a"), battleRow(7, "b"))
	if other := DistinctValues(shuffled.All(), (*configpb.LevelRow).GetName); !slices.Equal(names, other) {
		t.Fatalf("键集合应与行序无关:%v vs %v", names, other)
	}
	// 主键列本来就唯一,去重后 = 全部主键的升序
	if got := DistinctValues(tbl.All(), (*configpb.LevelRow).GetId); !slices.Equal(got, []uint32{6, 7, 8, 9}) {
		t.Fatalf("DistinctValues(Id)=%v", got)
	}
}

// TestDistinctValuesOnEnumColumn 枚举列(底层 int32)满足 cmp.Ordered,可直接当键集合用。
func TestDistinctValuesOnEnumColumn(t *testing.T) {
	login := &configpb.LevelRow{Id: 1, Name: "登录", AssetPath: "/Game/L/x.x",
		Category: configpb.LevelCategory_LEVEL_CATEGORY_LOGIN}
	tbl := mustLevelTable(t, battleRow(6, "a"), login, battleRow(7, "b"))

	cats := DistinctValues(tbl.All(), (*configpb.LevelRow).GetCategory)
	if len(cats) != 2 {
		t.Fatalf("DistinctValues(Category)=%v,应有 2 个类别", cats)
	}
	if !slices.Contains(cats, configpb.LevelCategory_LEVEL_CATEGORY_BATTLE) ||
		!slices.Contains(cats, configpb.LevelCategory_LEVEL_CATEGORY_LOGIN) {
		t.Fatalf("DistinctValues(Category)=%v", cats)
	}
}

// TestQueryEmptyTable 空表零值安全:一律返回空切片而非 nil panic。
func TestQueryEmptyTable(t *testing.T) {
	empty := mustLevelTable(t)
	if got := IDs(empty.All()); len(got) != 0 {
		t.Fatalf("空表 IDs=%v", got)
	}
	if got := Values(empty.All(), (*configpb.LevelRow).GetName); len(got) != 0 {
		t.Fatalf("空表 Values=%v", got)
	}
	if got := DistinctValues(empty.All(), (*configpb.LevelRow).GetId); len(got) != 0 {
		t.Fatalf("空表 DistinctValues=%v", got)
	}
}

// TestQueryAcrossTables 泛型对不同表的行类型都成立(加新表零成本的实证)。
func TestQueryAcrossTables(t *testing.T) {
	items, err := newItemTable(&configpb.ItemTableData{Rows: []*configpb.ItemRow{
		{Id: 10001, Name: "药水", Type: configpb.ItemType_ITEM_TYPE_CONSUMABLE, MaxStackSize: 99},
		{Id: 10002, Name: "长剑", Type: configpb.ItemType_ITEM_TYPE_EQUIPMENT, MaxStackSize: 1, EquipSlot: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if got := IDs(items.All()); !slices.Equal(got, []uint32{10001, 10002}) {
		t.Fatalf("IDs(item)=%v", got)
	}

	spawns, err := newSpawnPointTable(&configpb.SpawnPointTableData{Rows: []*configpb.SpawnPointRow{
		{Id: 1, LevelId: 7, SpawnGroupId: 1}, {Id: 2, LevelId: 7, SpawnGroupId: 1},
		{Id: 3, LevelId: 6, SpawnGroupId: 2},
	}})
	if err != nil {
		t.Fatal(err)
	}
	// 「哪些关卡有刷怪点」——本文件要解决的原始问题
	if got := DistinctValues(spawns.All(), (*configpb.SpawnPointRow).GetLevelId); !slices.Equal(got, []uint32{6, 7}) {
		t.Fatalf("DistinctValues(spawn_point.LevelId)=%v,应为 [6 7]", got)
	}
}
