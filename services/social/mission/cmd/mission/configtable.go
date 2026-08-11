// configtable.go — 配置表适配(对齐 inventory cmd/inventory/configtable.go)。
//
// catalogFromStore 每次调用都取 Store.Tables() 当前批次指针,与热更同源:
// reload 原子切换后,进行中的单次判定仍用旧批次快照(引擎在一个事务回调内多次
// 取行,跨批次撕裂由「每次方法调用单独取批次」的粒度天然避免——单方法内一致)。
package main

import (
	"fmt"

	"github.com/luyuancpp/pandora/pkg/configtable"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"

	"github.com/luyuancpp/pandora/services/social/mission/internal/biz"
)

// catalogFromStore 实现 biz.Catalog。
type catalogFromStore struct {
	store *configtable.Store
}

var _ biz.Catalog = catalogFromStore{}

func (c catalogFromStore) MissionByID(id uint32) (*configpb.MissionRow, bool) {
	return c.store.Tables().Mission.ByID(id)
}

func (c catalogFromStore) ConditionByID(id uint32) (*configpb.ConditionRow, bool) {
	return c.store.Tables().Condition.ByID(id)
}

func (c catalogFromStore) RewardByID(id uint32) (*configpb.RewardRow, bool) {
	return c.store.Tables().Reward.ByID(id)
}

func (c catalogFromStore) IsEquipment(itemConfigID uint32) bool {
	return c.store.Tables().Item.IsEquipment(itemConfigID)
}

// validateMissionTables 批次级门禁(启动首载与每次热 reload 同一门禁,失败整批不切换):
// 数组列跨表存在性 + next_mission_ids 链环(fk 注解只覆盖单值 reward_id 列)。
func validateMissionTables(tb *configtable.Tables) error {
	if err := configtable.ValidateMissionCrossTables(tb.Mission, tb.Condition, tb.Reward, tb.Item); err != nil {
		return fmt.Errorf("任务域跨表校验失败: %w", err)
	}
	return nil
}
