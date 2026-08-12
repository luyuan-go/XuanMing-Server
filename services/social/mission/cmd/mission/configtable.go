// configtable.go — 配置表适配。
//
// **批次快照粒度 = 一次领域操作**,不是一次方法调用。
//
// inventory 的 Lookup 是单方法自足(一次调用取完一个道具需要的全部字段),所以"每次
// 调用取一次 Tables()"对它就是原子的。任务域不是:一次 ApplyFactsTx 事务回调里
// MissionByID / ConditionByID / RewardByID / IsEquipment 会被调用几十次(每活跃任务 ×
// 每条件槽 × 每事实),中间只要发生一次 reload 原子切换,同一个事务就会读到**两个批次的
// 混合数据**。最坏的一条:buildRewardLog 用批次 A 的 RewardByID 拿奖励条目、用批次 B 的
// IsEquipment 决定装备/堆叠冻结位 —— 冻结位与奖励内容出身不同批次,而冻结位的整个存在
// 意义就是"路由必须与快照同源",撕裂之后它守的东西恰好被绕过。
//
// 所以 storeCatalogSource.Snapshot() 在操作入口取一次 *Tables 并钉住,整个事务回调复用
// 同一指针;reload 只影响下一次操作,不会在半途换批次。
package main

import (
	"fmt"

	"github.com/luyuancpp/pandora/pkg/configtable"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"

	"github.com/luyuancpp/pandora/services/social/mission/internal/biz"
)

// storeCatalogSource 实现 biz.CatalogSource:每次 Snapshot() 钉住当前批次指针。
type storeCatalogSource struct {
	store *configtable.Store
}

var _ biz.CatalogSource = storeCatalogSource{}

func (s storeCatalogSource) Snapshot() biz.Catalog {
	return batchCatalog{tables: s.store.Tables()}
}

// batchCatalog 是**单一批次**上的只读视图,构造后不再回读 Store。
type batchCatalog struct {
	tables *configtable.Tables
}

var _ biz.Catalog = batchCatalog{}

// 未加载完成时四个方法一律 fail-closed(照 inventory 的 nil 守卫;
// 原实现直接 c.store.Tables().Mission.ByID 会在启动竞态里 nil panic)。
func (c batchCatalog) MissionByID(id uint32) (*configpb.MissionRow, bool) {
	if c.tables == nil || c.tables.Mission == nil {
		return nil, false
	}
	return c.tables.Mission.ByID(id)
}

func (c batchCatalog) ConditionByID(id uint32) (*configpb.ConditionRow, bool) {
	if c.tables == nil || c.tables.Condition == nil {
		return nil, false
	}
	return c.tables.Condition.ByID(id)
}

func (c batchCatalog) RewardByID(id uint32) (*configpb.RewardRow, bool) {
	if c.tables == nil || c.tables.Reward == nil {
		return nil, false
	}
	return c.tables.Reward.ByID(id)
}

func (c batchCatalog) IsEquipment(itemConfigID uint32) bool {
	if c.tables == nil || c.tables.Item == nil {
		return false
	}
	return c.tables.Item.IsEquipment(itemConfigID)
}

// validateMissionTables 批次级门禁(启动首载与每次热 reload 同一门禁,失败整批不切换):
// 数组列跨表存在性 + next_mission_ids 链环(fk 注解只覆盖单值 reward_id 列)。
func validateMissionTables(tb *configtable.Tables) error {
	if err := configtable.ValidateMissionCrossTables(tb.Mission, tb.Condition, tb.Reward, tb.Item); err != nil {
		return fmt.Errorf("任务域跨表校验失败: %w", err)
	}
	return nil
}
