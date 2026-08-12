package biz

import (
	"testing"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
)

type affixTestCatalog struct {
	items map[uint32]ItemDefinition
	rules map[uint32]IdentifyDefinition
}

func (c *affixTestCatalog) Lookup(id uint32) (ItemDefinition, bool) {
	def, ok := c.items[id]
	return def, ok
}

func (c *affixTestCatalog) IdentifyRule(id uint32) (IdentifyDefinition, bool) {
	rule, ok := c.rules[id]
	return rule, ok
}

func TestRollIdentifyAttrsWeightedWithoutReplacement(t *testing.T) {
	catalog := &affixTestCatalog{
		items: map[uint32]ItemDefinition{10003: {Equipment: true}},
		rules: map[uint32]IdentifyDefinition{10003: {
			AttrCount: 2,
			Pool: []IdentifyAttrDefinition{
				{AttrID: 3, Weight: 10, Min: 1, Max: 3},
				{AttrID: 9, Weight: 20, Min: 4, Max: 6},
				{AttrID: 7, Weight: 30, Min: 10, Max: 12},
			},
		}},
	}
	uc := NewInventoryUsecase(nil, conf.InventoryConf{})
	uc.SetItemCatalog(catalog)
	// 调用次序:权重抽取、数值抽取、权重抽取、数值抽取。
	draws := []int64{10, 2, 0, 1}
	uc.SetRandSource(func(n int64) int64 {
		if len(draws) == 0 {
			t.Fatal("unexpected random draw")
		}
		v := draws[0]
		draws = draws[1:]
		if v < 0 || v >= n {
			t.Fatalf("draw %d outside [0,%d)", v, n)
		}
		return v
	})

	got := uc.rollIdentifyAttrs(10003)
	if len(got) != 2 {
		t.Fatalf("attribute count=%d want=2", len(got))
	}
	if got[0].AttrID != 9 || got[0].Value != 6 {
		t.Fatalf("first weighted attribute=%+v want attr=9 value=6", got[0])
	}
	if got[1].AttrID != 3 || got[1].Value != 2 {
		t.Fatalf("second weighted attribute=%+v want attr=3 value=2", got[1])
	}
	if got[0].AttrID == got[1].AttrID {
		t.Fatal("weighted selection must be without replacement")
	}
}

func TestRollIdentifyAttrsReadsCatalogOnEveryRequest(t *testing.T) {
	catalog := &affixTestCatalog{
		items: map[uint32]ItemDefinition{10003: {Equipment: true}},
		rules: map[uint32]IdentifyDefinition{10003: {
			AttrCount: 1,
			Pool:      []IdentifyAttrDefinition{{AttrID: 3, Weight: 1, Min: 1, Max: 1}},
		}},
	}
	uc := NewInventoryUsecase(nil, conf.InventoryConf{})
	uc.SetItemCatalog(catalog)
	uc.SetRandSource(func(int64) int64 { return 0 })
	if got := uc.rollIdentifyAttrs(10003); len(got) != 1 || got[0].AttrID != 3 {
		t.Fatalf("initial rule=%+v", got)
	}
	catalog.rules[10003] = IdentifyDefinition{
		AttrCount: 1,
		Pool:      []IdentifyAttrDefinition{{AttrID: 9, Weight: 1, Min: 5, Max: 5}},
	}
	if got := uc.rollIdentifyAttrs(10003); len(got) != 1 || got[0].AttrID != 9 || got[0].Value != 5 {
		t.Fatalf("hot-reloaded rule=%+v", got)
	}
}

func TestRollIdentifyAttrsFailsClosedForInvalidCatalogRule(t *testing.T) {
	catalog := &affixTestCatalog{
		items: map[uint32]ItemDefinition{10003: {Equipment: true}},
		rules: map[uint32]IdentifyDefinition{10003: {
			AttrCount: 2,
			Pool:      []IdentifyAttrDefinition{{AttrID: 3, Weight: 1, Min: 1, Max: 1}},
		}},
	}
	uc := NewInventoryUsecase(nil, conf.InventoryConf{})
	uc.SetItemCatalog(catalog)
	if got := uc.rollIdentifyAttrs(10003); got != nil {
		t.Fatalf("invalid attr_count must fail closed, got=%+v", got)
	}
	catalog.rules[10003] = IdentifyDefinition{
		AttrCount: 1,
		Pool:      []IdentifyAttrDefinition{{AttrID: 3, Weight: 0, Min: 1, Max: 1}},
	}
	if got := uc.rollIdentifyAttrs(10003); got != nil {
		t.Fatalf("zero weight must fail closed, got=%+v", got)
	}
}
