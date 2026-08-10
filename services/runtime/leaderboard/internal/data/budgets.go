// budgets.go — leaderboard 服务的库容量预算(CLAUDE.md §9.24,2026-07-22)。
// 口径见 inventory/internal/data/budgets.go 头注释。
package data

import "github.com/luyuancpp/pandora/pkg/dbguard"

// Budgets 是 pandora_leaderboard 库的容量预算。
//
// 结算批次驱动:每天若干榜 × 若干周期,量级远小于业务流水表;
// snapshot / reward_log 行数 = 批次数 × Top-N,是这里的主要增长源。
func Budgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{
			// settlement 故意不清理(settle uk 是防重复结算的永久闸,每批次 1 行慢增长豁免),
			// 所以这里给一个"慢增长仍需可见"的告警线:每天 100 批 × 3 年。
			Table: "leaderboard_settlement", MaxRows: 100 * 365 * 3, MaxAvgRowBytes: 256,
			Note: "§9.24 登记豁免(永久闸,不清理);超限说明结算批次量远超预期,需人工评估是否要归档",
		},
		{
			// 保留 90 天:每天 100 批 × Top-100 × 90 × 3。
			Table: "leaderboard_snapshot", MaxRows: 100 * 100 * 90 * 3, MaxAvgRowBytes: 128,
			Note: "名次快照,保留 90 天;超限先查 leaderboard 保留期清理是否在跑",
		},
		{
			Table: "leaderboard_reward_log", MaxRows: 100 * 100 * 90 * 3, MaxAvgRowBytes: 1024,
			Note: "GRANTED 行保留 90 天,PENDING/FAILED 永不清(补发工作集);" +
				"avg_row 超 1KB 查 reward_pb 的 items 条数(列 VARBINARY(2048),无条数上限)",
		},
	}
}

// BigFields 是列级字节预算。
func BigFields() []dbguard.ColumnBudget {
	return []dbguard.ColumnBudget{
		{Table: "leaderboard_reward_log", Column: "reward_pb", MaxBytes: 1536,
			Note: "列是 VARBINARY(2048) 存 pb RewardGrantStorageRecord;1536=75% 预警线且是写入侧硬阀。超限说明 RewardTier.items 条数失控"},
	}
}
