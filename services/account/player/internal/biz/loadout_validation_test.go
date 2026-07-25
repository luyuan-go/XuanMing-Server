// loadout_validation_test.go — 出战装备预设 / 专精分配的权威校验单测(2026-07-25)。
//
// 覆盖 2026-06-17 审查提出、本次补齐的三项装备校验(isEquip / slotMatch / ownEquipment)
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
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 9999}})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("非装备道具应拒: %v", err)
	}
}

func TestSetEquipment_RejectsSlotMismatch(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	// 1001 的装备部位是 1,提交到部位 2 → slotMatch 不通过。
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 2, ItemConfigID: 1001}})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("部位不匹配应拒: %v", err)
	}
}

func TestSetEquipment_RejectsZeroSlot(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	// 道具表约定 equip_slot=0 是"不可穿戴",预设里的 slot 0 永远匹配不到任何装备。
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 0, ItemConfigID: 1001}})
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("slot 0 应拒: %v", err)
	}
}

func TestSetEquipment_RejectsNotOwned(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	// 只持有 1002,却想装 1001。
	uc.SetItemOwnershipChecker(stubOwnership{owned: map[uint32]bool{1002: true}})
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001}})
	if errcode.As(err) != errcode.ErrPermissionDeny {
		t.Fatalf("未持有应拒(ErrPermissionDeny): %v", err)
	}
}

func TestSetEquipment_OwnershipQueryFailurePropagates(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	wantErr := errors.New("inventory unavailable")
	uc.SetItemOwnershipChecker(stubOwnership{err: wantErr})
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001}})
	if !errors.Is(err, wantErr) {
		// 查询失败绝不能被当成"持有"放行,也不能被吞成成功。
		t.Fatalf("拥有权查询失败应原样传播: %v", err)
	}
}

func TestSetEquipment_FailsClosedWithoutItemTable(t *testing.T) {
	uc := NewPlayerUsecase(newFakeRepo(), conf.PlayerConf{
		BaseMMR: 1500, DefaultNicknamePrefix: "Player_", MaxNicknameLen: 32, LoadoutCustomizeEnabled: true,
	})
	uc.SetItemOwnershipChecker(stubOwnership{})
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001}})
	if errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("道具表未加载应 fail-closed: %v", err)
	}
}

func TestSetEquipment_FailsClosedWithoutOwnershipChecker(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	uc.SetItemOwnershipChecker(nil)
	err := uc.SetEquipment(context.Background(), 100, []data.EquipmentSlot{{Slot: 1, ItemConfigID: 1001}})
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
