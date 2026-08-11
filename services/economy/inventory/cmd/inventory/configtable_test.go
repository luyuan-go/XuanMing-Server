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
	return conf.InventoryConf{DefaultIdentifyRule: &conf.IdentifyRule{
		AttrCount: 1,
		Pool: []conf.IdentifyAttrRoll{
			{AttrID: 3, Min: 1, Max: 1},
		},
	}}
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
	if got := len(itemMaxStacksFromTables(tables)); got != 65 {
		t.Fatalf("max-stack projection count=%d want=65", got)
	}

	// Compile-time guard: catalog remains the biz-facing seam, not a command-only helper.
	var _ biz.ItemCatalog = catalog
}

func TestIdentifyDefaultOnlyReferencesRealRoleAttributes(t *testing.T) {
	store := configtable.NewStore()
	store.AddValidator(validateInventoryTables(defaultIdentifyConf()))
	if _, err := store.Load(realDistDir(t), 0); err != nil {
		t.Fatalf("approved Atk pool must validate: %v", err)
	}
	bad := defaultIdentifyConf()
	bad.DefaultIdentifyRule.Pool[0].AttrID = 999999
	store = configtable.NewStore()
	store.AddValidator(validateInventoryTables(bad))
	if _, err := store.Load(realDistDir(t), 0); err == nil {
		t.Fatal("unknown role_attr_map id must fail table load")
	}
}
