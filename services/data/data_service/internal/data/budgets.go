// budgets.go — data_service 的库容量预算(CLAUDE.md §9.24,2026-07-22)。
// 口径见 inventory/internal/data/budgets.go 头注释。
package data

import "github.com/luyuancpp/pandora/pkg/dbguard"

const planPlayers = 100_000

// Budgets 是 player_data 表的容量预算。
//
// ⚠️ player_data 的 schema 由 proto2mysql 按 PlayerData proto **自动建表**:
// string 字段生成 MEDIUMTEXT(16MB)、bytes/message 生成 MEDIUMBLOB,
// 即 **DB 层几乎不设防**。写入侧目前也无长度校验(只校验 update_mask),
// 所以 MaxAvgRowBytes 是这里唯一的自动告警手段:
// avg_row 突增 = 某个字符串/blob 字段在无界增长,查 WritePlayer 的调用方。
func Budgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{
			Table: "player_data", MaxRows: planPlayers * 3, MaxAvgRowBytes: 4 * 1024,
			Note: "proto2mysql 自动建表(string→MEDIUMTEXT 16MB,DB 层不设防);" +
				"avg_row 超 4KB 说明某字段无界增长,用 dbcheck -size-check 定位到列再查 WritePlayer 调用方",
		},
	}
}
