package configtable

import (
	"testing"

	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
)

func newSkillCardTableForTest(t *testing.T, rows ...*configpb.SkillCardRow) *SkillCardTable {
	t.Helper()
	tbl, err := newSkillCardTable(&configpb.SkillCardTableData{Rows: rows})
	if err != nil {
		t.Fatalf("建卡表失败: %v", err)
	}
	return tbl
}

func newSkillCardUpgradeTableForTest(t *testing.T, rows ...*configpb.SkillCardUpgradeRow) *SkillCardUpgradeTable {
	t.Helper()
	tbl, err := newSkillCardUpgradeTable(&configpb.SkillCardUpgradeTableData{Rows: rows})
	if err != nil {
		t.Fatalf("建升级表失败: %v", err)
	}
	return tbl
}

func TestSkillCardRowValidation(t *testing.T) {
	// 非法稀有度查不到升级曲线,该卡永远升不了级。
	if _, err := newSkillCardTable(&configpb.SkillCardTableData{Rows: []*configpb.SkillCardRow{
		{Id: 1, Name: "A", SkillId: 100111, Rarity: 9, MaxLevel: 5},
	}}); err == nil {
		t.Fatal("非法稀有度应被拒")
	}
	// 等级上限 0:初始等级就是 1,上限 0 自相矛盾。
	if _, err := newSkillCardTable(&configpb.SkillCardTableData{Rows: []*configpb.SkillCardRow{
		{Id: 1, Name: "A", SkillId: 100111, Rarity: 1, MaxLevel: 0},
	}}); err == nil {
		t.Fatal("等级上限 0 应被拒")
	}
	if _, err := newSkillCardTable(&configpb.SkillCardTableData{Rows: []*configpb.SkillCardRow{
		{Id: 1, Name: "A", SkillId: 100111, Rarity: 1, MaxLevel: MaxSkillCardLevel + 1},
	}}); err == nil {
		t.Fatal("超上限的等级上限应被拒")
	}
	// max_level=1 是合法的"不可升级卡"。
	if _, err := newSkillCardTable(&configpb.SkillCardTableData{Rows: []*configpb.SkillCardRow{
		{Id: 1, Name: "A", SkillId: 100111, Rarity: 1, MaxLevel: 1},
	}}); err != nil {
		t.Fatalf("不可升级卡(上限 1)应放行: %v", err)
	}
}

func TestSkillCardUpgradeRowValidation(t *testing.T) {
	// 目标等级 1:1 级是获得卡时的初始等级,不存在"升到 1 级"。
	if _, err := newSkillCardUpgradeTable(&configpb.SkillCardUpgradeTableData{Rows: []*configpb.SkillCardUpgradeRow{
		{Id: 1, Rarity: 1, Level: 1, ShardCost: 5},
	}}); err == nil {
		t.Fatal("目标等级 1 应被拒")
	}
	// 碎片消耗 0 = 免费升级,必须显式改需求而不是填 0。
	if _, err := newSkillCardUpgradeTable(&configpb.SkillCardUpgradeTableData{Rows: []*configpb.SkillCardUpgradeRow{
		{Id: 1, Rarity: 1, Level: 2, ShardCost: 0},
	}}); err == nil {
		t.Fatal("碎片消耗 0 应被拒")
	}
}

// TestSkillCardUpgradeValidateCurves_RejectsGap 钉住本表最关键的一条:曲线断档。
// 断档不报错,表现为"卡升到某级之后按钮没反应",必须在加载期整表挡住。
func TestSkillCardUpgradeValidateCurves_RejectsGap(t *testing.T) {
	cards := newSkillCardTableForTest(t,
		&configpb.SkillCardRow{Id: 1, Name: "A", SkillId: 100111, Rarity: 1, MaxLevel: 4},
	)
	// 缺 4 级。
	gapped := newSkillCardUpgradeTableForTest(t,
		&configpb.SkillCardUpgradeRow{Id: 1, Rarity: 1, Level: 2, ShardCost: 5},
		&configpb.SkillCardUpgradeRow{Id: 2, Rarity: 1, Level: 3, ShardCost: 10},
	)
	if err := gapped.ValidateCurves(cards); err == nil {
		t.Fatal("曲线缺 4 级应被拒(卡上限是 4)")
	}

	full := newSkillCardUpgradeTableForTest(t,
		&configpb.SkillCardUpgradeRow{Id: 1, Rarity: 1, Level: 2, ShardCost: 5},
		&configpb.SkillCardUpgradeRow{Id: 2, Rarity: 1, Level: 3, ShardCost: 10},
		&configpb.SkillCardUpgradeRow{Id: 3, Rarity: 1, Level: 4, ShardCost: 20},
	)
	if err := full.ValidateCurves(cards); err != nil {
		t.Fatalf("完整曲线应放行: %v", err)
	}
}

func TestSkillCardUpgradeValidateCurves_RejectsDuplicateAndDecreasing(t *testing.T) {
	cards := newSkillCardTableForTest(t,
		&configpb.SkillCardRow{Id: 1, Name: "A", SkillId: 100111, Rarity: 1, MaxLevel: 3},
	)

	// 同稀有度同等级两行:取值会取决于表内顺序。
	dup := newSkillCardUpgradeTableForTest(t,
		&configpb.SkillCardUpgradeRow{Id: 1, Rarity: 1, Level: 2, ShardCost: 5},
		&configpb.SkillCardUpgradeRow{Id: 2, Rarity: 1, Level: 2, ShardCost: 8},
	)
	if err := dup.ValidateCurves(cards); err == nil {
		t.Fatal("同稀有度同等级重复行应被拒")
	}

	// 越升越便宜:几乎总是填错。
	desc := newSkillCardUpgradeTableForTest(t,
		&configpb.SkillCardUpgradeRow{Id: 1, Rarity: 1, Level: 2, ShardCost: 20},
		&configpb.SkillCardUpgradeRow{Id: 2, Rarity: 1, Level: 3, ShardCost: 5},
	)
	if err := desc.ValidateCurves(cards); err == nil {
		t.Fatal("消耗随等级下降应被拒")
	}
}

// TestSkillCardUpgradeValidateCurves_IgnoresUnusedRarity 钉住方向:
// 只要求"卡在用的稀有度"有完整曲线,不要求"曲线表里每个稀有度都得有卡"
// (策划可以先铺曲线再加卡)。
func TestSkillCardUpgradeValidateCurves_IgnoresUnusedRarity(t *testing.T) {
	cards := newSkillCardTableForTest(t,
		&configpb.SkillCardRow{Id: 1, Name: "A", SkillId: 100111, Rarity: 1, MaxLevel: 2},
	)
	tbl := newSkillCardUpgradeTableForTest(t,
		&configpb.SkillCardUpgradeRow{Id: 1, Rarity: 1, Level: 2, ShardCost: 5},
		// 传说曲线已铺,但还没有传说卡——不该因此报错。
		&configpb.SkillCardUpgradeRow{Id: 2, Rarity: 4, Level: 2, ShardCost: 20},
	)
	if err := tbl.ValidateCurves(cards); err != nil {
		t.Fatalf("尚无卡在用的稀有度不该要求完整曲线: %v", err)
	}
}

func TestSkillCardShardCost(t *testing.T) {
	tbl := newSkillCardUpgradeTableForTest(t,
		&configpb.SkillCardUpgradeRow{Id: 1, Rarity: 2, Level: 3, ShardCost: 16},
	)
	if cost, ok := tbl.ShardCost(2, 3); !ok || cost != 16 {
		t.Fatalf("稀有度 2 升到 3 级应为 16, got (%d, %v)", cost, ok)
	}
	// 查不到必须是 (0,false) 而不是 (0,true):调用方据此拒绝升级,不能当免费。
	if _, ok := tbl.ShardCost(2, 9); ok {
		t.Fatal("曲线没铺到的等级必须返回 false")
	}
}

func TestSkillCardMaxLevelByRarity(t *testing.T) {
	cards := newSkillCardTableForTest(t,
		&configpb.SkillCardRow{Id: 1, Name: "A", SkillId: 100111, Rarity: 1, MaxLevel: 3},
		&configpb.SkillCardRow{Id: 2, Name: "B", SkillId: 100121, Rarity: 1, MaxLevel: 5},
		&configpb.SkillCardRow{Id: 3, Name: "C", SkillId: 100131, Rarity: 3, MaxLevel: 2},
	)
	got := cards.MaxLevelByRarity()
	// 同稀有度取最高上限:曲线必须铺到该稀有度最能升的那张卡。
	if got[1] != 5 {
		t.Fatalf("稀有度 1 的最高上限应为 5, got %d", got[1])
	}
	if got[3] != 2 {
		t.Fatalf("稀有度 3 的最高上限应为 2, got %d", got[3])
	}
	if _, ok := cards.RaritiesInUse()[2]; ok {
		t.Fatal("没有卡在用稀有度 2,不该出现在在用集合里")
	}
}
