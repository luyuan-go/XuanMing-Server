// budgets.go — owner 服务的库容量预算(CLAUDE.md §9.24,2026-07-22)。
// 口径见 inventory/internal/data/budgets.go 头注释。
package data

import "github.com/luyuancpp/pandora/pkg/dbguard"

const (
	planPlayers = 100_000
	planDAU     = 10_000
)

// Budgets 是 pandora_owner 库的容量预算。
//
// owner_record / ds_instance_lease 按玩家 / DS 实例有界;
// owner_transition_log 是唯一只增流水(有 90 天保留期清理)。
func Budgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{Table: "owner_record", MaxRows: planPlayers * 3, MaxAvgRowBytes: 512,
			Note: "每玩家一行 owner 权威(§9.22);超限说明玩家量级超规划"},
		{Table: "ds_instance_lease", MaxRows: 100_000, MaxAvgRowBytes: 384,
			Note: "每 DS 实例一行;超限说明失效实例行未回收"},
		{
			// 保留 90 天:日活 1 万 × 每人每天 20 次归属迁移(登录/进场/换线/回大厅)× 90 × 3。
			Table: "owner_transition_log", MaxRows: planDAU * 20 * 90 * 3, MaxAvgRowBytes: 768,
			Note: "归属迁移审计流水,保留 90 天;超限先查 owner sweep 是否在跑(日志 owner_transition_log_swept)," +
				"再查是否有玩家在异常高频迁移(进场链抖动)",
		},
	}
}
