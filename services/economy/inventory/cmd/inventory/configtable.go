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

// IdentifyRule 每次从 Store 当前原子批次读取 item→pool→候选行，热更后下一次鉴定立即
// 使用新规则；已经鉴定并落库的实例不会重 roll。
func (c inventoryCatalogFromStore) IdentifyRule(itemConfigID uint32) (biz.IdentifyDefinition, bool) {
	tables := c.store.Tables()
	if tables == nil || tables.Item == nil || tables.EquipmentAffix == nil {
		return biz.IdentifyDefinition{}, false
	}
	item, ok := tables.Item.ByID(itemConfigID)
	if !ok || item.GetType() != configpb.ItemType_ITEM_TYPE_EQUIPMENT || item.GetIdentifyPoolId() == 0 {
		return biz.IdentifyDefinition{}, false
	}
	rows := tables.EquipmentAffix.ListByPoolId(item.GetIdentifyPoolId())
	if len(rows) == 0 {
		return biz.IdentifyDefinition{}, false
	}
	rule := biz.IdentifyDefinition{AttrCount: int(rows[0].GetAttrCount()), Pool: make([]biz.IdentifyAttrDefinition, 0, len(rows))}
	for _, row := range rows {
		rule.Pool = append(rule.Pool, biz.IdentifyAttrDefinition{
			AttrID: row.GetAttrId(), Weight: int64(row.GetWeight()),
			Min: row.GetMinValue(), Max: row.GetMaxValue(),
		})
	}
	return rule, true
}

// validateInventoryTables 是启动和热更共用的整批门禁。鉴定池和 item 同源发布，
// 不再依赖 YAML 默认池；任何装备缺池、池内语义漂移或未知玩法属性都会拒绝整批切换。
func validateInventoryTables(_ conf.InventoryConf) func(*configtable.Tables) error {
	return func(t *configtable.Tables) error {
		if t == nil || t.Item == nil || t.RoleAttrMap == nil || t.EquipmentAffix == nil {
			return fmt.Errorf("item / role_attr_map / equipment_affix tables required")
		}

		// 当前战斗属性映射只为这三类定义了权威单位。新增属性必须先实现 UE 应用/
		// 卸载对账，再改这里放行，不能让配表悄悄造出“只显示不生效”的词条。
		allowedAttrs := map[uint32]string{3: "Atk", 7: "MoveSpeedRate", 9: "Defense"}
		for id, codeName := range allowedAttrs {
			row, ok := t.RoleAttrMap.ByID(id)
			if !ok || row.GetCodeName() != codeName {
				return fmt.Errorf("role_attr_map id %d must be %q for equipment gameplay semantics", id, codeName)
			}
		}

		type poolStats struct {
			attrCount uint32
			attrs     map[uint32]struct{}
			total     int64
		}
		pools := make(map[uint32]*poolStats)
		for _, row := range t.EquipmentAffix.All() {
			if _, ok := allowedAttrs[row.GetAttrId()]; !ok {
				return fmt.Errorf("equipment_affix row %d attr_id %d has no gameplay apply/reconcile semantics",
					row.GetId(), row.GetAttrId())
			}
			stats := pools[row.GetPoolId()]
			if stats == nil {
				stats = &poolStats{attrCount: row.GetAttrCount(), attrs: make(map[uint32]struct{})}
				pools[row.GetPoolId()] = stats
			}
			if stats.attrCount != row.GetAttrCount() {
				return fmt.Errorf("equipment_affix pool %d has inconsistent attr_count %d/%d",
					row.GetPoolId(), stats.attrCount, row.GetAttrCount())
			}
			if _, duplicate := stats.attrs[row.GetAttrId()]; duplicate {
				return fmt.Errorf("equipment_affix pool %d duplicates attr_id %d", row.GetPoolId(), row.GetAttrId())
			}
			stats.attrs[row.GetAttrId()] = struct{}{}
			stats.total += int64(row.GetWeight())
			if stats.total > 1_000_000 {
				return fmt.Errorf("equipment_affix pool %d total weight exceeds 1000000", row.GetPoolId())
			}
		}
		for poolID, stats := range pools {
			if stats.attrCount == 0 || int(stats.attrCount) > len(stats.attrs) {
				return fmt.Errorf("equipment_affix pool %d attr_count %d exceeds unique candidates %d",
					poolID, stats.attrCount, len(stats.attrs))
			}
		}

		referenced := make(map[uint32]struct{})
		for _, item := range t.Item.All() {
			if item.GetType() != configpb.ItemType_ITEM_TYPE_EQUIPMENT {
				continue
			}
			poolID := item.GetIdentifyPoolId()
			if _, ok := pools[poolID]; !ok {
				return fmt.Errorf("equipment item %d references missing identify pool %d", item.GetId(), poolID)
			}
			referenced[poolID] = struct{}{}
		}
		for poolID := range pools {
			if _, ok := referenced[poolID]; !ok {
				return fmt.Errorf("equipment_affix pool %d is orphaned (no item references it)", poolID)
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
