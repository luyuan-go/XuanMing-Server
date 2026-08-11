package main

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/luyuancpp/pandora/pkg/configtable"
)

func battleRealDistDir(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "..", "configtable", "dist"))
}

func TestRealDropTableRoutesAllItems(t *testing.T) {
	store := configtable.NewStore()
	if _, err := store.Load(battleRealDistDir(t), 0); err != nil {
		t.Fatalf("load real dist: %v", err)
	}
	catalog := battleItemCatalogFromStore{store: store}
	unique := make(map[uint32]struct{})
	for _, row := range store.Tables().Drop.All() {
		unique[row.GetItemConfigId()] = struct{}{}
	}
	var stackable, equipment int
	for id := range unique {
		def, ok := catalog.Lookup(id)
		if !ok || !def.Droppable {
			t.Fatalf("drop item %d missing catalog route: %+v ok=%v", id, def, ok)
		}
		if def.Equipment {
			equipment++
		} else {
			stackable++
		}
	}
	if len(unique) != 58 || stackable != 33 || equipment != 25 {
		t.Fatalf("real drop contract drift: unique=%d stackable=%d equipment=%d",
			len(unique), stackable, equipment)
	}
	food, ok := catalog.Lookup(10001)
	if !ok || !food.Droppable || food.Equipment || !food.BattleUsable || food.MaxStack != 20 {
		t.Fatalf("10001 route mismatch: %+v ok=%v", food, ok)
	}
	blade, ok := catalog.Lookup(10003)
	if !ok || !blade.Droppable || !blade.Equipment || blade.BattleUsable || blade.MaxStack != 1 {
		t.Fatalf("10003 route mismatch: %+v ok=%v", blade, ok)
	}
}
