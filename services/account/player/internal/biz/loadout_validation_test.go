// loadout_validation_test.go — 出战装备预设 / 专精分配的权威校验单测(2026-07-25)。
//
// 覆盖装备校验(isEquip / slotMatch / exact instance ownership)
// 与专精表校验(节点存在 / 等级上限 / 前置 / 按每级消耗算总点数),以及各依赖缺失时的
// fail-closed 行为——这些路径在补齐前是直接放行的,回归价值全在"必须拒"的分支上。
package biz

import (
	"context"
	"errors"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/account/player/internal/conf"
	"github.com/luyuancpp/pandora/services/account/player/internal/data"
)

// ── SetEquipment ─────────────────────────────────────────────────────────────

func TestSetEquipment_RejectsNonEquipItem(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	// 9999 不在道具表的可穿戴集合里 → isEquip 不通过。
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 9999, InstanceID: 9001}})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("非装备道具应拒: %v", err)
	}
}

func TestSetEquipment_RejectsSlotMismatch(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	// 1001 的装备部位是 1,提交到部位 2 → slotMatch 不通过。
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 2, ItemConfigID: 1001, InstanceID: 9001}})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("部位不匹配应拒: %v", err)
	}
}

func TestSetEquipment_RejectsZeroSlot(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	// 道具表约定 equip_slot=0 是"不可穿戴",预设里的 slot 0 永远匹配不到任何装备。
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 0, ItemConfigID: 1001, InstanceID: 9001}})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("slot 0 应拒: %v", err)
	}
}

func TestSetEquipment_RejectsNotOwned(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	// 只持有 instance 9002/config 1002,却想装 instance 9001/config 1001。
	uc.SetInstanceOwnershipChecker(stubOwnership{owned: map[uint64]uint32{9002: 1002}})
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}})
	if errcode.As(err) != errcode.ErrPermissionDeny {
		t.Fatalf("未持有应拒(ErrPermissionDeny): %v", err)
	}
}

func TestSetEquipment_OwnershipQueryFailurePropagates(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	wantErr := errors.New("inventory unavailable")
	uc.SetInstanceOwnershipChecker(stubOwnership{err: wantErr})
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}})
	if !errors.Is(err, wantErr) {
		// 查询失败绝不能被当成"持有"放行,也不能被吞成成功。
		t.Fatalf("拥有权查询失败应原样传播: %v", err)
	}
}

func TestSetEquipment_FailsClosedWithoutItemTable(t *testing.T) {
	uc := NewPlayerUsecase(newFakeRepo(), conf.PlayerConf{
		BaseMMR: 1500, DefaultNicknamePrefix: "Player_", MaxNicknameLen: 32, LoadoutCustomizeEnabled: true,
	})
	uc.SetInstanceOwnershipChecker(stubOwnership{})
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}})
	if errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("道具表未加载应 fail-closed: %v", err)
	}
}

func TestSetEquipment_FailsClosedWithoutOwnershipChecker(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	uc.SetInstanceOwnershipChecker(nil)
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}})
	if errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("拥有权校验器未接线应 fail-closed: %v", err)
	}
}

// TestSetEquipment_EmptyClearsPreset 空预设是合法的"全部卸下",不该被校验链误拒
// (没有任何 item 要查表 / 查持有)。
func TestSetEquipment_EmptyClearsPreset(t *testing.T) {
	repo := newFakeRepo()
	uc := newUCLoadout(repo)
	if err := uc.SetEquipment(context.Background(), 100, nil); err != nil {
		t.Fatalf("空预设应放行: %v", err)
	}
	slots, err := uc.GetEquipment(context.Background(), 100)
	if err != nil {
		t.Fatalf("get equipment: %v", err)
	}
	if len(slots) != 0 {
		t.Fatalf("空预设应清空,实为 %d 条", len(slots))
	}
}

func TestSetEquipment_RequiresExactInstanceID(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{
		{Slot: 1, ItemConfigID: 1001},
	})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("新写缺 instance_id 应拒: %v", err)
	}
}

func TestSetEquipment_RejectsDuplicateInstanceID(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{
		{Slot: 1, ItemConfigID: 1001, InstanceID: 9001},
		{Slot: 2, ItemConfigID: 1002, InstanceID: 9001},
	})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("同一实例占两个槽应拒: %v", err)
	}
}

func TestSetEquipment_RejectsInstanceConfigMismatch(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	// 9001 确实归玩家，但权威实例行是 config 1002；客户端伪报成 1001 必须拒。
	uc.SetInstanceOwnershipChecker(stubOwnership{owned: map[uint64]uint32{9001: 1002}})
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{
		{Slot: 1, ItemConfigID: 1001, InstanceID: 9001},
	})
	if errcode.As(err) != errcode.ErrPermissionDeny {
		t.Fatalf("instance/config 非 exact pair 应拒: %v", err)
	}
}

func TestSetEquipment_AcceptsExactIDsFromOldInventoryDuringRollout(t *testing.T) {
	repo := newFakeRepo()
	uc := newUCLoadout(repo)
	uc.SetInstanceOwnershipChecker(stubOwnership{idsOnly: true})
	want := []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}}
	if err := uc.SetEquipment(context.Background(), 100, want); err != nil {
		t.Fatalf("旧 inventory 已能 exact pair 核权但尚未回详情时，SetEquipment 应兼容: %v", err)
	}
	got, err := uc.GetEquipment(context.Background(), 100)
	if err != nil || len(got) != 1 || got[0] != want[0] {
		t.Fatalf("精确实例未持久化: got=%+v err=%v", got, err)
	}
}

func TestGetLoadout_RejectsLegacyConfigOnlyEquipment(t *testing.T) {
	repo := newFakeRepo()
	repo.equipment[100] = []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001}}
	uc := newUCLoadout(repo)
	if _, err := uc.GetLoadout(context.Background(), 100); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("旧配置级预设不得进入战斗快照: %v", err)
	}
	// 展示读取仍保留旧行，让客户端能看见并引导重选，不静默删数据。
	got, err := uc.GetEquipment(context.Background(), 100)
	if err != nil || len(got) != 1 || got[0].InstanceID != 0 {
		t.Fatalf("旧行只读兼容丢失: got=%+v err=%v", got, err)
	}
}

func TestGetLoadout_RechecksCurrentExactOwnership(t *testing.T) {
	repo := newFakeRepo()
	repo.equipment[100] = []data.EquipmentSlot{
		{Slot: 1, ItemConfigID: 1001, InstanceID: 9001},
	}
	uc := newUCLoadout(repo)
	uc.SetInstanceOwnershipChecker(stubOwnership{owned: map[uint64]uint32{}})
	if _, err := uc.GetLoadout(context.Background(), 100); errcode.As(err) != errcode.ErrPermissionDeny {
		t.Fatalf("实例已转移/丢失后 GetLoadout 应 fail-closed: %v", err)
	}
}

func TestGetLoadout_CarriesExactInstanceID(t *testing.T) {
	repo := newFakeRepo()
	repo.equipment[100] = []data.EquipmentSlot{
		{Slot: 1, ItemConfigID: 1001, InstanceID: 9001},
	}
	uc := newUCLoadout(repo)
	loadout, err := uc.GetLoadout(context.Background(), 100)
	if err != nil {
		t.Fatalf("GetLoadout: %v", err)
	}
	if len(loadout.GetEquipment()) != 1 || loadout.GetEquipment()[0].GetInstanceId() != 9001 {
		t.Fatalf("战斗快照丢失 instance_id: %+v", loadout.GetEquipment())
	}
	if loadout.GetEquipment()[0].GetIdentified() || len(loadout.GetEquipment()[0].GetAttributes()) != 0 {
		t.Fatalf("未鉴定实例必须保持 identified=false 且无词条: %+v", loadout.GetEquipment()[0])
	}
}

func TestGetLoadout_PreservesIdentifiedAttributes(t *testing.T) {
	repo := newFakeRepo()
	repo.equipment[100] = []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}}
	uc := newUCLoadout(repo)
	uc.SetInstanceOwnershipChecker(stubOwnership{details: map[uint64]data.OwnedEquipmentInstance{
		9001: {
			InstanceID: 9001, ItemConfigID: 1001, Identified: true,
			Attributes: []data.EquipmentAttributeSnapshot{
				{AttrID: 3, Value: 1_000_000},
				{AttrID: 9, Value: 1_000_000},
				{AttrID: 7, Value: 10_000},
			},
		},
	}})
	loadout, err := uc.GetLoadout(context.Background(), 100)
	if err != nil {
		t.Fatalf("GetLoadout: %v", err)
	}
	got := loadout.GetEquipment()[0]
	if !got.GetIdentified() || len(got.GetAttributes()) != 3 ||
		got.GetAttributes()[0].GetAttrId() != 3 || got.GetAttributes()[0].GetValue() != 1_000_000 ||
		got.GetAttributes()[1].GetAttrId() != 9 || got.GetAttributes()[1].GetValue() != 1_000_000 ||
		got.GetAttributes()[2].GetAttrId() != 7 || got.GetAttributes()[2].GetValue() != 10_000 {
		t.Fatalf("鉴定词条顺序/数值必须保真进入战斗快照: %+v", got)
	}
}

func TestGetLoadout_RejectsUnsafeIdentifiedAttributes(t *testing.T) {
	tests := []struct {
		name  string
		attrs []data.EquipmentAttributeSnapshot
	}{
		{name: "unknown attr", attrs: []data.EquipmentAttributeSnapshot{{AttrID: 8, Value: 1}}},
		{name: "zero attr id", attrs: []data.EquipmentAttributeSnapshot{{AttrID: 0, Value: 1}}},
		{name: "duplicate attr", attrs: []data.EquipmentAttributeSnapshot{{AttrID: 3, Value: 1}, {AttrID: 3, Value: 2}}},
		{name: "zero attack", attrs: []data.EquipmentAttributeSnapshot{{AttrID: 3, Value: 0}}},
		{name: "negative defense", attrs: []data.EquipmentAttributeSnapshot{{AttrID: 9, Value: -1}}},
		{name: "attack above cap", attrs: []data.EquipmentAttributeSnapshot{{AttrID: 3, Value: 1_000_001}}},
		{name: "defense above cap", attrs: []data.EquipmentAttributeSnapshot{{AttrID: 9, Value: 1_000_001}}},
		{name: "move speed rate above cap", attrs: []data.EquipmentAttributeSnapshot{{AttrID: 7, Value: 10_001}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newFakeRepo()
			repo.equipment[100] = []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}}
			uc := newUCLoadout(repo)
			uc.SetInstanceOwnershipChecker(stubOwnership{details: map[uint64]data.OwnedEquipmentInstance{
				9001: {
					InstanceID: 9001, ItemConfigID: 1001, Identified: true,
					Attributes: tt.attrs,
				},
			}})
			if _, err := uc.GetLoadout(context.Background(), 100); errcode.As(err) != errcode.ErrInternal {
				t.Fatalf("不安全词条必须 fail-closed: attrs=%+v err=%v", tt.attrs, err)
			}
		})
	}
}

func TestGetLoadout_RejectsIdentifiedInstanceWithoutAttributes(t *testing.T) {
	repo := newFakeRepo()
	repo.equipment[100] = []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}}
	uc := newUCLoadout(repo)
	uc.SetInstanceOwnershipChecker(stubOwnership{details: map[uint64]data.OwnedEquipmentInstance{
		9001: {InstanceID: 9001, ItemConfigID: 1001, Identified: true},
	}})
	if _, err := uc.GetLoadout(context.Background(), 100); errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("已鉴定实例零词条必须 fail-closed: %v", err)
	}
}

func TestGetLoadout_FailsClosedWhenOldInventoryReturnsIDsOnly(t *testing.T) {
	repo := newFakeRepo()
	repo.equipment[100] = []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}}
	uc := newUCLoadout(repo)
	uc.SetInstanceOwnershipChecker(stubOwnership{idsOnly: true})
	if _, err := uc.GetLoadout(context.Background(), 100); errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("旧 inventory 仅回 owned ids 时不得生成缺词条战斗快照: %v", err)
	}
}

func TestGetLoadout_RejectsNonExactInstanceDetail(t *testing.T) {
	repo := newFakeRepo()
	repo.equipment[100] = []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}}
	uc := newUCLoadout(repo)
	uc.SetInstanceOwnershipChecker(stubOwnership{details: map[uint64]data.OwnedEquipmentInstance{
		9001: {InstanceID: 9001, ItemConfigID: 1002},
	}})
	if _, err := uc.GetLoadout(context.Background(), 100); errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("详情的 instance/config 非 exact pair 必须 fail-closed: %v", err)
	}
}

func TestGetLoadout_RejectsAttributesOnUnidentifiedInstance(t *testing.T) {
	repo := newFakeRepo()
	repo.equipment[100] = []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001, InstanceID: 9001}}
	uc := newUCLoadout(repo)
	uc.SetInstanceOwnershipChecker(stubOwnership{details: map[uint64]data.OwnedEquipmentInstance{
		9001: {
			InstanceID: 9001, ItemConfigID: 1001, Identified: false,
			Attributes: []data.EquipmentAttributeSnapshot{{AttrID: 3, Value: 37}},
		},
	}})
	if _, err := uc.GetLoadout(context.Background(), 100); errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("未鉴定实例携词条违反权威不变量，必须 fail-closed: %v", err)
	}
}

// ── SetTalents ───────────────────────────────────────────────────────────────

func TestSetTalents_RejectsUnknownTalent(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	if _, err := uc.GrantTalentPoints(context.Background(), 100, 10, "g1"); err != nil {
		t.Fatalf("grant: %v", err)
	}
	_, err := uc.SetTalents(context.Background(), 100, []data.TalentLevel{{TalentID: 8888, Level: 1}})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("未知专精应拒: %v", err)
	}
}

func TestSetTalents_RejectsOverMaxLevel(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	if _, err := uc.GrantTalentPoints(context.Background(), 100, 100, "g1"); err != nil {
		t.Fatalf("grant: %v", err)
	}
	// 5001 上限 5;点数给足 100,证明拒绝来自等级上限而不是点数不足。
	_, err := uc.SetTalents(context.Background(), 100, []data.TalentLevel{{TalentID: 5001, Level: 6}})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("超等级上限应拒: %v", err)
	}
}

func TestSetTalents_RejectsUnmetPrerequisite(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	if _, err := uc.GrantTalentPoints(context.Background(), 100, 100, "g1"); err != nil {
		t.Fatalf("grant: %v", err)
	}
	// 5002 要求 5001 达到 2 级,本次方案里 5001 只有 1 级。
	_, err := uc.SetTalents(context.Background(), 100, []data.TalentLevel{
		{TalentID: 5001, Level: 1},
		{TalentID: 5002, Level: 1},
	})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("前置未达标应拒: %v", err)
	}
}

// TestSetTalents_CostPerLevelCountsTowardBudget 是本次改动的核心回归:
// 扣点必须按 Σ 等级 × 每级消耗,而不是旧的 Σ 等级。5002 每级消耗 2,
// 方案 5001×2 + 5002×2 的总消耗 = 2×1 + 2×2 = 6,若仍按等级和只会算 4。
func TestSetTalents_CostPerLevelCountsTowardBudget(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	if _, err := uc.GrantTalentPoints(context.Background(), 100, 6, "g1"); err != nil {
		t.Fatalf("grant: %v", err)
	}
	unspent, err := uc.SetTalents(context.Background(), 100, []data.TalentLevel{
		{TalentID: 5001, Level: 2},
		{TalentID: 5002, Level: 2},
	})
	if err != nil {
		t.Fatalf("恰好花光应成功: %v", err)
	}
	if unspent != 0 {
		t.Fatalf("总消耗应为 6,余点应为 0,实为 %d", unspent)
	}
}

func TestSetTalents_CostPerLevelCanExhaustBudget(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	// 只给 5 点,同一方案总消耗 6 → 必须判点数不足。旧口径按等级和算 4,会错误放行。
	if _, err := uc.GrantTalentPoints(context.Background(), 100, 5, "g1"); err != nil {
		t.Fatalf("grant: %v", err)
	}
	_, err := uc.SetTalents(context.Background(), 100, []data.TalentLevel{
		{TalentID: 5001, Level: 2},
		{TalentID: 5002, Level: 2},
	})
	if errcode.As(err) != errcode.ErrPlayerInsufficientPoints {
		t.Fatalf("按每级消耗应判点数不足: %v", err)
	}
}

// TestGetTalents_UnspentUsesCostPerLevel 是本次修复的核心回归:读可点数必须按
// 每节点实际消耗算,而不是 Σ 等级。此前写按 Σ 等级×每级消耗 扣、读按 Σ 等级 算,
// 两个口径只在全表 cost_per_level=1 时才碰巧一致;5002 每级消耗 2,
// 方案 5001×2 + 5002×2 实扣 6 点,旧读取口径只会算 4 点,界面凭空多出 2 点可点数。
func TestGetTalents_UnspentUsesCostPerLevel(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	if _, err := uc.GrantTalentPoints(context.Background(), 100, 10, "g1"); err != nil {
		t.Fatalf("grant: %v", err)
	}
	setUnspent, err := uc.SetTalents(context.Background(), 100, []data.TalentLevel{
		{TalentID: 5001, Level: 2},
		{TalentID: 5002, Level: 2},
	})
	if err != nil {
		t.Fatalf("set talents: %v", err)
	}
	if setUnspent != 4 {
		t.Fatalf("写侧总消耗应为 6,余点应为 4,实为 %d", setUnspent)
	}

	_, getUnspent, err := uc.GetTalents(context.Background(), 100)
	if err != nil {
		t.Fatalf("get talents: %v", err)
	}
	// 读写必须报同一个数;不等就说明读取侧又在按等级和反推。
	if getUnspent != setUnspent {
		t.Fatalf("读侧可点数应与写侧一致(%d),实为 %d", setUnspent, getUnspent)
	}
}

// TestGrantTalentPoints_UnspentUsesCostPerLevel 覆盖同一口径分裂的另一个出口:
// 授予点数后回读的可点数也走 talentUnspent,同样不能按 Σ 等级 反推。
func TestGrantTalentPoints_UnspentUsesCostPerLevel(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	if _, err := uc.GrantTalentPoints(context.Background(), 100, 6, "g1"); err != nil {
		t.Fatalf("grant: %v", err)
	}
	if _, err := uc.SetTalents(context.Background(), 100, []data.TalentLevel{
		{TalentID: 5001, Level: 2},
		{TalentID: 5002, Level: 2},
	}); err != nil {
		t.Fatalf("set talents: %v", err)
	}
	// 再授 3 点:已花 6 点,总授予 9 点 → 可点 3 点。按等级和反推会得到 5。
	unspent, err := uc.GrantTalentPoints(context.Background(), 100, 3, "g2")
	if err != nil {
		t.Fatalf("grant 2: %v", err)
	}
	if unspent != 3 {
		t.Fatalf("授予后可点数应为 3,实为 %d", unspent)
	}
}

func TestSetTalents_FailsClosedWithoutTalentTable(t *testing.T) {
	uc := NewPlayerUsecase(newFakeRepo(), conf.PlayerConf{
		BaseMMR: 1500, DefaultNicknamePrefix: "Player_", MaxNicknameLen: 32, LoadoutCustomizeEnabled: true,
	})
	_, err := uc.SetTalents(context.Background(), 100, []data.TalentLevel{{TalentID: 5001, Level: 1}})
	if errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("专精表未加载应 fail-closed: %v", err)
	}
}

// TestSetTalents_EmptyAllocationAllowed 空分配等价于清空,不需要专精表也不该被拒
// (真正的清空入口是 ResetTalents,但这条路径不能因为校验链而报内部错)。
func TestSetTalents_EmptyAllocationAllowed(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	if _, err := uc.GrantTalentPoints(context.Background(), 100, 5, "g1"); err != nil {
		t.Fatalf("grant: %v", err)
	}
	unspent, err := uc.SetTalents(context.Background(), 100, nil)
	if err != nil {
		t.Fatalf("空分配应放行: %v", err)
	}
	if unspent != 5 {
		t.Fatalf("空分配后余点应为 5,实为 %d", unspent)
	}
}
