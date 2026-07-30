// budgets.go — auction 服务的库容量预算(CLAUDE.md §9.24,2026-07-22)。
// 口径见 inventory/internal/data/budgets.go 头注释。
package data

import "github.com/luyuancpp/pandora/pkg/dbguard"

// 容量规划:日活 1 万 × 每人每天 5 单;分片时预算按**单分片**给
// (main 逐分片建 Guard,各分片各自比对,不把总量摊在一个分片上)。
const planDailyOrders = 50_000

// Budgets 是 pandora_auction 单个分片的容量预算。
func Budgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{
			// 终态行保留 90 天:5 万单/天 × 90 × 3。
			Table: "auction_orders", MaxRows: planDailyOrders * 90 * 3, MaxAvgRowBytes: 384,
			Note: "挂单/出价;终态行保留 90 天,超限先查 auction 保留期清理是否在跑(日志 auction_retention_swept)",
		},
		{
			Table: "auction_matches", MaxRows: planDailyOrders * 90 * 3, MaxAvgRowBytes: 256,
			Note: "成交流水;已结算行保留 90 天。堆积也可能是结算/事件补偿链堵塞(PENDING 行不清理)",
		},
		{
			Table: "auction_idempotency_keys", MaxRows: planDailyOrders * 90 * 3, MaxAvgRowBytes: 192,
			Note: "owner+key canonical 映射,保留 90 天",
		},
		{Table: "auction_owner_guards", MaxRows: 100_000 * 3, MaxAvgRowBytes: 128,
			Note: "§9.24 登记豁免:每 owner 一行,被玩家数有界"},
		{Table: "auction_shard_topology", MaxRows: 10, MaxAvgRowBytes: 512,
			Note: "单行拓扑 marker;多于 1 行说明拓扑被污染"},
	}
}
