package main

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/luyuancpp/pandora/pkg/configtable"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/biz"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
)

func realDistDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "..", "configtable", "dist"))
}

func defaultIdentifyConf() conf.InventoryConf {
	return conf.InventoryConf{}
}

func TestRealItemTableInventoryContract(t *testing.T) {
	store := configtable.NewStore()
	store.AddValidator(validateInventoryTables(defaultIdentifyConf()))
	if _, err := store.Load(realDistDir(t), 0); err != nil {
		t.Fatalf("load real dist: %v", err)
	}
	tables := store.Tables()
	if got := tables.Item.Count(); got != 65 {
		t.Fatalf("item row count drifted: got=%d want=65", got)
	}
	if got := tables.EquipmentAffix.Count(); got != 18 {
		t.Fatalf("equipment affix row count drifted: got=%d want=18", got)
	}
	if got := tables.RoleAttrMap.Count(); got != 5 {
		t.Fatalf("role attr row count drifted: got=%d want=5", got)
	}
	var equipment, sellable, battleUsable int
	for _, row := range tables.Item.All() {
		if row.GetType() == configpb.ItemType_ITEM_TYPE_EQUIPMENT {
			equipment++
		}
		if row.GetSellPrice() > 0 {
			sellable++
		}
		if row.GetUsable() {
			battleUsable++
		}
	}
	if equipment != 31 || sellable != 60 || battleUsable != 5 {
		t.Fatalf("real item contract drift: equipment=%d sellable=%d battle_usable=%d", equipment, sellable, battleUsable)
	}

	catalog := inventoryCatalogFromStore{store: store}
	consumable, ok := catalog.Lookup(10001)
	if !ok || consumable.Equipment || consumable.LobbyUsable || !consumable.BattleUsable || consumable.SellUnitPrice != 15 || consumable.MaxStack != 20 {
		t.Fatalf("10001 contract mismatch: %+v ok=%v", consumable, ok)
	}
	equipmentDef, ok := catalog.Lookup(10003)
	if !ok || !equipmentDef.Equipment || equipmentDef.BattleUsable || equipmentDef.SellUnitPrice != 180 || equipmentDef.MaxStack != 1 {
		t.Fatalf("10003 contract mismatch: %+v ok=%v", equipmentDef, ok)
	}
	if _, ok := catalog.Lookup(2001); ok {
		t.Fatal("removed demo id 2001 must not exist in real catalog")
	}
	if _, ok := catalog.Lookup(3001); ok {
		t.Fatal("removed demo id 3001 must not exist in real catalog")
	}
	affixRule, ok := catalog.IdentifyRule(10003) // 品质 2 → 池 3
	if !ok || affixRule.AttrCount != 2 || len(affixRule.Pool) != 3 {
		t.Fatalf("10003 affix rule mismatch: %+v ok=%v", affixRule, ok)
	}
	if _, ok := catalog.IdentifyRule(10001); ok {
		t.Fatal("non-equipment must not expose an identify rule")
	}
	if got := len(itemMaxStacksFromTables(tables)); got != 65 {
		t.Fatalf("max-stack projection count=%d want=65", got)
	}

	// Compile-time guard: catalog remains the biz-facing seam, not a command-only helper.
	var _ biz.ItemCatalog = catalog
	var _ biz.IdentifyCatalog = catalog
}

func TestFormalAffixValidatorRejectsUnsupportedGameplayAttribute(t *testing.T) {
	store := configtable.NewStore()
	store.AddValidator(validateInventoryTables(defaultIdentifyConf()))
	if _, err := store.Load(realDistDir(t), 0); err != nil {
		t.Fatalf("formal affix tables must validate: %v", err)
	}
	tables := store.Tables()
	row := tables.EquipmentAffix.All()[0]
	original := row.AttrId
	row.AttrId = 1 // Hp 存在于 role_attr_map，但当前只展示，不能偷偷进入战斗池。
	defer func() { row.AttrId = original }()
	if err := validateInventoryTables(defaultIdentifyConf())(tables); err == nil {
		t.Fatal("attribute without gameplay apply/reconcile semantics must fail table validation")
	}
}
