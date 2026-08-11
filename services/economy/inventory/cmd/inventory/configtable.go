package main

import (
	"fmt"

	"github.com/luyuancpp/pandora/pkg/configtable"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/biz"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
)

// inventoryCatalogFromStore 每次读取 Store 当前批次，Use/Sell/Grant/Identify 与热更后的
// item 表保持同源。未知 ID 一律返回 ok=false，由 biz fail-closed。
type inventoryCatalogFromStore struct{ store *configtable.Store }

func (c inventoryCatalogFromStore) Lookup(itemConfigID uint32) (biz.ItemDefinition, bool) {
	tables := c.store.Tables()
	if tables == nil || tables.Item == nil {
		return biz.ItemDefinition{}, false
	}
	row, ok := tables.Item.ByID(itemConfigID)
	if !ok {
		return biz.ItemDefinition{}, false
	}
	return biz.ItemDefinition{
		Equipment: row.GetType() == configpb.ItemType_ITEM_TYPE_EQUIPMENT,
		// item.usable 的真实语义是局内 UE GAS 可消费。大厅没有效果派发器，必须
		// fail-closed；BattleUsable 只授权可信战斗事实走内部持久扣减 RPC。
		LobbyUsable:   false,
		BattleUsable:  row.GetUsable(),
		SellUnitPrice: int64(row.GetSellPrice()),
		MaxStack:      row.GetMaxStackSize(),
	}, true
}

// validateInventoryTables 是启动和热更共用的整批门禁。默认鉴定池只能引用
// role_attr_map 的真实属性；专属规则只能挂真实装备，防配置把材料鉴定成实例。
func validateInventoryTables(ic conf.InventoryConf) func(*configtable.Tables) error {
	return func(t *configtable.Tables) error {
		if t == nil || t.Item == nil || t.RoleAttrMap == nil {
			return fmt.Errorf("item / role_attr_map tables required")
		}
		if ic.DefaultIdentifyRule == nil {
			return fmt.Errorf("inventory.default_identify_rule required when config_table is enabled")
		}
		validatePool := func(path string, rule *conf.IdentifyRule) error {
			for i, p := range rule.Pool {
				if !t.RoleAttrMap.Exists(p.AttrID) {
					return fmt.Errorf("%s.pool[%d] references unknown role_attr_map attr_id %d", path, i, p.AttrID)
				}
			}
			return nil
		}
		if err := validatePool("inventory.default_identify_rule", ic.DefaultIdentifyRule); err != nil {
			return err
		}
		for i := range ic.IdentifyRules {
			rule := &ic.IdentifyRules[i]
			row, ok := t.Item.ByID(rule.ItemConfigID)
			if !ok || row.GetType() != configpb.ItemType_ITEM_TYPE_EQUIPMENT {
				return fmt.Errorf("inventory.identify_rules[%d] item %d is not configured equipment", i, rule.ItemConfigID)
			}
			if err := validatePool(fmt.Sprintf("inventory.identify_rules[%d]", i), rule); err != nil {
				return err
			}
		}
		return nil
	}
}

// itemMaxStacksFromTables 把同源 item.max_stack_size 投影给后端驻留背包段。
// BagConf 目前是启动快照，因此 reload 后改变堆叠上限需滚动重启 inventory；规则值本身
// 不再手抄 YAML，启动时始终取当前发布批次。
func itemMaxStacksFromTables(t *configtable.Tables) []conf.BagItemStackRule {
	out := make([]conf.BagItemStackRule, 0, t.Item.Count())
	for _, row := range t.Item.All() {
		out = append(out, conf.BagItemStackRule{ItemConfigID: row.GetId(), MaxStack: row.GetMaxStackSize()})
	}
	return out
}
