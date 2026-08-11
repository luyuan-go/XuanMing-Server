package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/account/player/internal/conf"
	"github.com/luyuancpp/pandora/services/account/player/internal/data"
)

func grantCards(t *testing.T, uc *PlayerUsecase, playerID uint64, key string, grants ...data.SkillCardGrant) {
	t.Helper()
	if _, _, err := uc.GrantSkillCards(context.Background(), playerID, grants, key); err != nil {
		t.Fatalf("grant skill cards: %v", err)
	}
}

// ── 发放 ─────────────────────────────────────────────────────────────────────

func TestGrantSkillCards_IdempotentAndShardAccumulate(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	ctx := context.Background()

	cards, already, err := uc.GrantSkillCards(ctx, 100, []data.SkillCardGrant{{CardID: 7001, Shards: 3}}, "g1")
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if already {
		t.Fatal("首次发放不应命中幂等")
	}
	if len(cards) != 1 || cards[0].Level != 1 || cards[0].Shards != 3 {
		t.Fatalf("首次获得应是 1 级 3 碎片, got %+v", cards)
	}

	// 同一幂等键重放:一片碎片都不能多加。
	cards, already, err = uc.GrantSkillCards(ctx, 100, []data.SkillCardGrant{{CardID: 7001, Shards: 3}}, "g1")
	if err != nil {
		t.Fatalf("regrant: %v", err)
	}
	if !already {
		t.Fatal("同一幂等键重放应命中幂等")
	}
	if cards[0].Shards != 3 {
		t.Fatalf("幂等命中不得重复加碎片,应仍为 3, got %d", cards[0].Shards)
	}

	// 换幂等键 = 真的又发了一次:重复获得同名卡转碎片,等级不动。
	cards, _, err = uc.GrantSkillCards(ctx, 100, []data.SkillCardGrant{{CardID: 7001, Shards: 4}}, "g2")
	if err != nil {
		t.Fatalf("grant 2: %v", err)
	}
	if cards[0].Shards != 7 {
		t.Fatalf("重复获得应累加碎片到 7, got %d", cards[0].Shards)
	}
	if cards[0].Level != 1 {
		t.Fatalf("发放不得改变已培养等级,应仍为 1 级, got %d", cards[0].Level)
	}
}

func TestGrantSkillCards_RejectsUnknownAndDuplicateCard(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	ctx := context.Background()

	// 表里没有的卡:发出去会变成永远打不开、升不了、装不上的幽灵卡。
	if _, _, err := uc.GrantSkillCards(ctx, 100, []data.SkillCardGrant{{CardID: 9999, Shards: 1}}, "g1"); err == nil {
		t.Fatal("发放配置表里不存在的卡应被拒")
	}
	// 同一批里重复:要求调用方自己合并,否则发放意图对不清账。
	_, _, err := uc.GrantSkillCards(ctx, 100, []data.SkillCardGrant{
		{CardID: 7001, Shards: 1},
		{CardID: 7001, Shards: 2},
	}, "g2")
	if err == nil {
		t.Fatal("同一批里重复的 card_id 应被拒")
	}
}

// ── 培养 ─────────────────────────────────────────────────────────────────────

func TestUpgradeSkillCard_ConsumesShardsAndLevels(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	ctx := context.Background()
	grantCards(t, uc, 100, "g1", data.SkillCardGrant{CardID: 7001, Shards: 20})

	// 1 → 2 级消耗 5。
	card, cost, err := uc.UpgradeSkillCard(ctx, 100, 7001)
	if err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	if card.Level != 2 || cost != 5 || card.Shards != 15 {
		t.Fatalf("升到 2 级应扣 5 碎片余 15, got level=%d cost=%d shards=%d", card.Level, cost, card.Shards)
	}

	// 2 → 3 级消耗 10(曲线随等级走,不是每级同价)。
	card, cost, err = uc.UpgradeSkillCard(ctx, 100, 7001)
	if err != nil {
		t.Fatalf("upgrade 2: %v", err)
	}
	if card.Level != 3 || cost != 10 || card.Shards != 5 {
		t.Fatalf("升到 3 级应扣 10 碎片余 5, got level=%d cost=%d shards=%d", card.Level, cost, card.Shards)
	}

	// 已达上限:即使碎片还够也不能再升。
	if _, _, err := uc.UpgradeSkillCard(ctx, 100, 7001); errcode.As(err) != errcode.ErrSkillCardMaxLevel {
		t.Fatalf("满级应返回 ErrSkillCardMaxLevel, got %v", err)
	}
}

func TestUpgradeSkillCard_RejectsNotOwnedAndInsufficientShards(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	ctx := context.Background()

	if _, _, err := uc.UpgradeSkillCard(ctx, 100, 7001); errcode.As(err) != errcode.ErrSkillCardNotOwned {
		t.Fatalf("未持有应返回 ErrSkillCardNotOwned, got %v", err)
	}

	grantCards(t, uc, 100, "g1", data.SkillCardGrant{CardID: 7002, Shards: 19}) // 传说卡升级要 20
	if _, _, err := uc.UpgradeSkillCard(ctx, 100, 7002); errcode.As(err) != errcode.ErrSkillCardInsufficientShards {
		t.Fatalf("碎片不足应返回 ErrSkillCardInsufficientShards, got %v", err)
	}
}

// TestUpgradeSkillCard_CurveGapIsNotFreeUpgrade 钉住"曲线断档必须拒绝,不得当免费升级"。
// 7003 上限 3 级但曲线只铺到 2 级:升到 2 级后再升必须报错,而不是白升一级。
func TestUpgradeSkillCard_CurveGapIsNotFreeUpgrade(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	ctx := context.Background()
	grantCards(t, uc, 100, "g1", data.SkillCardGrant{CardID: 7003, Shards: 100})

	if _, _, err := uc.UpgradeSkillCard(ctx, 100, 7003); err != nil {
		t.Fatalf("升到 2 级应成功: %v", err)
	}
	_, _, err := uc.UpgradeSkillCard(ctx, 100, 7003)
	if err == nil {
		t.Fatal("曲线缺 3 级时不得放行(白升一级)")
	}
	if errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("曲线断档属配置事故,应报 ErrInternal 而不是业务码, got %v", err)
	}
}

// ── 更换 ─────────────────────────────────────────────────────────────────────

func TestSetSkillSlots_ReplacesAndClears(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	ctx := context.Background()
	grantCards(t, uc, 100, "g1",
		data.SkillCardGrant{CardID: 7001, Shards: 0},
		data.SkillCardGrant{CardID: 7002, Shards: 0})

	applied, err := uc.SetSkillSlots(ctx, 100, []data.SkillSlot{
		{Slot: 0, CardID: 7001},
		{Slot: 1, CardID: 7002},
	})
	if err != nil {
		t.Fatalf("set slots: %v", err)
	}
	if len(applied) != 2 {
		t.Fatalf("应装配 2 个槽, got %d", len(applied))
	}

	// card_id=0 是显式清空:该槽不落行。
	applied, err = uc.SetSkillSlots(ctx, 100, []data.SkillSlot{
		{Slot: 0, CardID: 7002},
		{Slot: 1, CardID: 0},
	})
	if err != nil {
		t.Fatalf("clear slot: %v", err)
	}
	if len(applied) != 1 || applied[0].Slot != 0 || applied[0].CardID != 7002 {
		t.Fatalf("清空后应只剩槽 0 装 7002, got %+v", applied)
	}

	_, slots, err := uc.GetSkillCards(ctx, 100)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(slots) != 1 || slots[0].CardID != 7002 {
		t.Fatalf("权威装配应与设置一致, got %+v", slots)
	}
}

func TestSetSkillSlots_RejectsInvalidAssignments(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	ctx := context.Background()
	grantCards(t, uc, 100, "g1", data.SkillCardGrant{CardID: 7001, Shards: 0})

	// 槽位越界。
	_, err := uc.SetSkillSlots(ctx, 100, []data.SkillSlot{{Slot: SkillSlotCount, CardID: 7001}})
	if errcode.As(err) != errcode.ErrSkillCardSlotInvalid {
		t.Fatalf("槽位越界应返回 ErrSkillCardSlotInvalid, got %v", err)
	}
	// 同一张卡占两个槽。
	_, err = uc.SetSkillSlots(ctx, 100, []data.SkillSlot{
		{Slot: 0, CardID: 7001},
		{Slot: 1, CardID: 7001},
	})
	if errcode.As(err) != errcode.ErrSkillCardSlotInvalid {
		t.Fatalf("同卡占两槽应返回 ErrSkillCardSlotInvalid, got %v", err)
	}
	// 同一个槽出现两次。
	_, err = uc.SetSkillSlots(ctx, 100, []data.SkillSlot{
		{Slot: 0, CardID: 7001},
		{Slot: 0, CardID: 7002},
	})
	if errcode.As(err) != errcode.ErrSkillCardSlotInvalid {
		t.Fatalf("重复槽位应返回 ErrSkillCardSlotInvalid, got %v", err)
	}
	// 装一张没持有的卡:配置表里有,但玩家没有。
	_, err = uc.SetSkillSlots(ctx, 100, []data.SkillSlot{{Slot: 0, CardID: 7002}})
	if errcode.As(err) != errcode.ErrSkillCardNotOwned {
		t.Fatalf("装未持有的卡应返回 ErrSkillCardNotOwned, got %v", err)
	}
}

// ── 开关与 fail-closed ───────────────────────────────────────────────────────

// TestSkillCard_FeatureDisabledBlocksPlayerWrites 钉住开关语义:
// 玩家自助的培养 / 更换受 LoadoutCustomizeEnabled 约束,系统发放不受
// (与 GrantTalentPoints 一致——开关管的是玩家改配装,不是系统行为)。
func TestSkillCard_FeatureDisabledBlocksPlayerWrites(t *testing.T) {
	uc := NewPlayerUsecase(newFakeRepo(), conf.PlayerConf{
		BaseMMR: 1500, DefaultNicknamePrefix: "Player_", MaxNicknameLen: 32,
		LoadoutCustomizeEnabled: false,
	})
	uc.skillCardRules = stubSkillCardRules{cards: map[uint32]stubSkillCard{
		7001: {maxLevel: 3, curve: map[uint32]uint32{2: 5, 3: 10}},
	}}
	ctx := context.Background()

	if _, _, err := uc.GrantSkillCards(ctx, 100, []data.SkillCardGrant{{CardID: 7001, Shards: 9}}, "g1"); err != nil {
		t.Fatalf("系统发放不应受出战养成开关影响: %v", err)
	}
	if _, _, err := uc.UpgradeSkillCard(ctx, 100, 7001); errcode.As(err) != errcode.ErrPlayerFeatureDisabled {
		t.Fatalf("开关关闭时升级应返回 ErrPlayerFeatureDisabled, got %v", err)
	}
	if _, err := uc.SetSkillSlots(ctx, 100, []data.SkillSlot{{Slot: 0, CardID: 7001}}); errcode.As(err) != errcode.ErrPlayerFeatureDisabled {
		t.Fatalf("开关关闭时换卡应返回 ErrPlayerFeatureDisabled, got %v", err)
	}
}

// TestSkillCard_FailsClosedWithoutConfigTable 钉住配置表缺失时不得静默放行。
func TestSkillCard_FailsClosedWithoutConfigTable(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	grantCards(t, uc, 100, "g1", data.SkillCardGrant{CardID: 7001, Shards: 50})
	uc.skillCardRules = nil // 模拟技能卡表未加载
	ctx := context.Background()

	if _, _, err := uc.GrantSkillCards(ctx, 100, []data.SkillCardGrant{{CardID: 7001, Shards: 1}}, "g2"); err == nil {
		t.Fatal("表未加载时发放应 fail-closed")
	}
	if _, _, err := uc.UpgradeSkillCard(ctx, 100, 7001); err == nil {
		t.Fatal("表未加载时升级应 fail-closed(否则等于无上限无消耗)")
	}
	if _, err := uc.SetSkillSlots(ctx, 100, []data.SkillSlot{{Slot: 0, CardID: 7001}}); err == nil {
		t.Fatal("表未加载时换卡应 fail-closed")
	}
}

// ── 出战快照 ─────────────────────────────────────────────────────────────────

// TestGetLoadout_CarriesSkillCardLevels 钉住 DS 拿到的快照里卡等级随槽位带出,
// 免得 DS 拿着 card_id 再查一次持有表(多一次读,且中间可能被改)。
func TestGetLoadout_CarriesSkillCardLevels(t *testing.T) {
	uc := newUCLoadout(newFakeRepo())
	ctx := context.Background()
	grantCards(t, uc, 100, "g1", data.SkillCardGrant{CardID: 7001, Shards: 20})
	if _, _, err := uc.UpgradeSkillCard(ctx, 100, 7001); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	if _, err := uc.SetSkillSlots(ctx, 100, []data.SkillSlot{{Slot: 2, CardID: 7001}}); err != nil {
		t.Fatalf("set slots: %v", err)
	}

	loadout, err := uc.GetLoadout(ctx, 100)
	if err != nil {
		t.Fatalf("get loadout: %v", err)
	}
	if len(loadout.GetSkillCards()) != 1 {
		t.Fatalf("快照应含 1 个已装配卡槽, got %d", len(loadout.GetSkillCards()))
	}
	got := loadout.GetSkillCards()[0]
	if got.GetSlot() != 2 || got.GetCardId() != 7001 || got.GetLevel() != 2 {
		t.Fatalf("快照应为 slot=2 card=7001 level=2, got %+v", got)
	}
}
