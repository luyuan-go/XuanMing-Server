package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
)

type mapItemCatalog map[uint32]ItemDefinition

func (c mapItemCatalog) Lookup(id uint32) (ItemDefinition, bool) {
	d, ok := c[id]
	return d, ok
}

func realItemTestUC(repo *fakeRepo) *InventoryUsecase {
	uc := NewInventoryUsecase(repo, conf.InventoryConf{
		Capacity: 8,
		DefaultIdentifyRule: &conf.IdentifyRule{
			AttrCount: 1,
			Pool:      []conf.IdentifyAttrRoll{{AttrID: 3, Min: 1, Max: 1}},
		},
	})
	uc.SetItemCatalog(mapItemCatalog{
		// 真实契约 ID：10001 是 item.usable=true 的可堆叠局内消耗品；大厅无效果处理器。
		10001: {BattleUsable: true, SellUnitPrice: 15, MaxStack: 20},
		// 真实契约 ID：10003 是可出售装备实例。
		10003: {Equipment: true, SellUnitPrice: 180, MaxStack: 1},
		10002: {SellUnitPrice: 25, MaxStack: 99},
		10027: {Equipment: true, SellUnitPrice: 220, MaxStack: 1},
	})
	uc.SetSnowflake(&seqGen{})
	return uc
}

func TestRealUsableItemLobbyUseFailsClosedWithoutDeduction(t *testing.T) {
	repo := newFakeRepo()
	repo.items[7] = map[uint32]int64{10001: 3}
	uc := realItemTestUC(repo)

	if _, err := uc.UseItem(context.Background(), 7, 10001, 1, "lobby-use"); errcode.As(err) != errcode.ErrInventoryItemNotUsable {
		t.Fatalf("大厅 UseItem 必须 fail-closed, got %v", err)
	}
	if got := repo.items[7][10001]; got != 3 {
		t.Fatalf("拒绝时不得扣物: got=%d want=3", got)
	}
}

func TestConsumeBattleItemIsInternalRuleAndIdempotent(t *testing.T) {
	repo := newFakeRepo()
	repo.items[7] = map[uint32]int64{10001: 3, 10002: 2}
	uc := realItemTestUC(repo)

	remaining, err := uc.ConsumeBattleItem(context.Background(), 7, 10001, 1, "progress:9:2:7:consume")
	if err != nil || remaining != 2 {
		t.Fatalf("battle consume remaining=%d err=%v", remaining, err)
	}
	remaining, err = uc.ConsumeBattleItem(context.Background(), 7, 10001, 1, "progress:9:2:7:consume")
	if err != nil || remaining != 2 || repo.items[7][10001] != 2 {
		t.Fatalf("idempotent replay remaining=%d stored=%d err=%v", remaining, repo.items[7][10001], err)
	}
	if _, err := uc.ConsumeBattleItem(context.Background(), 7, 10002, 1, "not-usable"); errcode.As(err) != errcode.ErrInventoryItemNotUsable {
		t.Fatalf("非局内消耗品必须拒绝, got %v", err)
	}
	if repo.items[7][10002] != 2 {
		t.Fatalf("规则拒绝不得扣物")
	}
}

func TestDiscardBattleItemStackOnlyAndIdempotent(t *testing.T) {
	repo := newFakeRepo()
	repo.items[7] = map[uint32]int64{10002: 3}
	uc := realItemTestUC(repo)
	remaining, err := uc.DiscardBattleItem(context.Background(), 7, 10002, 2, "progress:9:3:7:discard")
	if err != nil || remaining != 1 {
		t.Fatalf("battle discard remaining=%d err=%v", remaining, err)
	}
	remaining, err = uc.DiscardBattleItem(context.Background(), 7, 10002, 2, "progress:9:3:7:discard")
	if err != nil || remaining != 1 || repo.items[7][10002] != 1 {
		t.Fatalf("battle discard replay remaining=%d stored=%d err=%v", remaining, repo.items[7][10002], err)
	}
	if _, err := uc.DiscardBattleItem(context.Background(), 7, 10003, 1, "bad-instance-guid"); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("副本装备丢弃必须 fail-closed, got %v", err)
	}
}

func TestDiscardItemAndSellInstanceClosure(t *testing.T) {
	repo := newFakeRepo()
	repo.items[7] = map[uint32]int64{10002: 4}
	uc := realItemTestUC(repo)

	remaining, err := uc.DiscardItem(context.Background(), 7, 10002, 2, "discard-1")
	if err != nil || remaining != 2 {
		t.Fatalf("discard remaining=%d err=%v", remaining, err)
	}
	remaining, err = uc.DiscardItem(context.Background(), 7, 10002, 2, "discard-1")
	if err != nil || remaining != 2 || repo.items[7][10002] != 2 {
		t.Fatalf("discard replay remaining=%d stored=%d err=%v", remaining, repo.items[7][10002], err)
	}
	if _, err := uc.DiscardItem(context.Background(), 7, 10003, 1, "bad-discard"); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("装备不得按配置丢弃, got %v", err)
	}

	insts, err := uc.GrantInstances(context.Background(), 7, []uint32{10003}, "grant-inst")
	if err != nil || len(insts) != 1 {
		t.Fatalf("grant equipment: %+v err=%v", insts, err)
	}
	instanceID := insts[0].InstanceID
	if _, err := uc.SellInstance(context.Background(), 7, instanceID, 10002, "sell-inst-wrong"); errcode.As(err) != errcode.ErrInventoryNotSellable {
		t.Fatalf("陈旧 config_id 必须拒绝, got %v", err)
	}
	gold, err := uc.SellInstance(context.Background(), 7, instanceID, 10003, "sell-inst")
	if err != nil || gold != 180 {
		t.Fatalf("sell instance gold=%d err=%v", gold, err)
	}
	// 实例已删仍由 ledger 回放，不能在 biz 先查实例导致幂等失效。
	gold, err = uc.SellInstance(context.Background(), 7, instanceID, 10003, "sell-inst")
	if err != nil || gold != 180 {
		t.Fatalf("sell instance replay gold=%d err=%v", gold, err)
	}
}

func TestSaleIdempotencySurvivesPriceReloadAndConfigRemoval(t *testing.T) {
	repo := newFakeRepo()
	repo.items[7] = map[uint32]int64{10002: 5}
	catalog := mapItemCatalog{
		10002: {SellUnitPrice: 25, MaxStack: 99},
		10003: {Equipment: true, SellUnitPrice: 180, MaxStack: 1},
	}
	uc := NewInventoryUsecase(repo, conf.InventoryConf{Capacity: 8})
	uc.SetItemCatalog(catalog)
	uc.SetSnowflake(&seqGen{})

	remaining, gold, err := uc.SellItem(context.Background(), 7, 10002, 2, "hot-stack")
	if err != nil || remaining != 3 || gold != 50 {
		t.Fatalf("first stack sale remaining=%d gold=%d err=%v", remaining, gold, err)
	}
	catalog[10002] = ItemDefinition{SellUnitPrice: 100, MaxStack: 99}
	remaining, gold, err = uc.SellItem(context.Background(), 7, 10002, 2, "hot-stack")
	if err != nil || remaining != 3 || gold != 50 || repo.items[7][10002] != 3 {
		t.Fatalf("price reload stack replay remaining=%d gold=%d stored=%d err=%v",
			remaining, gold, repo.items[7][10002], err)
	}
	delete(catalog, 10002)
	remaining, gold, err = uc.SellItem(context.Background(), 7, 10002, 2, "hot-stack")
	if err != nil || remaining != 3 || gold != 50 || repo.items[7][10002] != 3 {
		t.Fatalf("removed config stack replay remaining=%d gold=%d stored=%d err=%v",
			remaining, gold, repo.items[7][10002], err)
	}

	insts, err := uc.GrantInstances(context.Background(), 7, []uint32{10003}, "hot-grant-inst")
	if err != nil || len(insts) != 1 {
		t.Fatalf("grant instance=%+v err=%v", insts, err)
	}
	instanceID := insts[0].InstanceID
	gold, err = uc.SellInstance(context.Background(), 7, instanceID, 10003, "hot-instance")
	if err != nil || gold != 230 { // 先前 stack +50，再 instance +180。
		t.Fatalf("first instance sale gold=%d err=%v", gold, err)
	}
	delete(catalog, 10003)
	gold, err = uc.SellInstance(context.Background(), 7, instanceID, 10003, "hot-instance")
	if err != nil || gold != 230 || repo.gold[7] != 230 {
		t.Fatalf("removed config instance replay gold=%d stored=%d err=%v", gold, repo.gold[7], err)
	}
}

func TestBoundInstanceCannotDiscardOrSell(t *testing.T) {
	repo := newFakeRepo()
	uc := realItemTestUC(repo)
	insts, err := uc.GrantInstances(context.Background(), 7, []uint32{10003}, "grant-bound")
	if err != nil {
		t.Fatal(err)
	}
	id := insts[0].InstanceID
	repo.instances[7][id].Bound = true
	if err := uc.DiscardInstance(context.Background(), 7, id); errcode.As(err) != errcode.ErrInventoryInstanceBound {
		t.Fatalf("bound discard got %v", err)
	}
	if _, err := uc.SellInstance(context.Background(), 7, id, 10003, "sell-bound"); errcode.As(err) != errcode.ErrInventoryInstanceBound {
		t.Fatalf("bound sell got %v", err)
	}
	if repo.instances[7][id] == nil || repo.gold[7] != 0 {
		t.Fatalf("bound reject must preserve instance and gold")
	}
}

func TestCheckInstancesOwnedRequiresExactPair(t *testing.T) {
	repo := newFakeRepo()
	uc := realItemTestUC(repo)
	insts, err := uc.GrantInstances(context.Background(), 7, []uint32{10003, 10003}, "grant-two")
	if err != nil {
		t.Fatal(err)
	}
	queries := []data.InstanceOwnershipQuery{
		{InstanceID: insts[0].InstanceID, ItemConfigID: 10003},
		{InstanceID: insts[1].InstanceID, ItemConfigID: 10002},
	}
	// 第二项在 catalog 层先被拒为非装备，防调用方拿材料配置探测实例。
	if _, err := uc.CheckInstancesOwned(context.Background(), 7, queries); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("non-equipment query got %v", err)
	}
	queries[1].ItemConfigID = 10027 // 也是装备，但与实例真实 config 不一致。
	owned, err := uc.CheckInstancesOwned(context.Background(), 7, queries)
	if err != nil || len(owned) != 1 || owned[0].InstanceID != insts[0].InstanceID ||
		owned[0].ItemConfigID != 10003 {
		t.Fatalf("exact pair subset owned=%v err=%v", owned, err)
	}
	if _, err := uc.CheckInstancesOwned(context.Background(), 7, []data.InstanceOwnershipQuery{
		{InstanceID: insts[0].InstanceID, ItemConfigID: 10003},
		{InstanceID: insts[0].InstanceID, ItemConfigID: 10003},
	}); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("duplicate instance_id got %v", err)
	}
}
