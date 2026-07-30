// budgets.go — guild 服务的库容量预算(CLAUDE.md §9.24,2026-07-22)。
// 只声明本服务负责的表(pandora_social 共用库);口径见 inventory 同名文件头注释。
package data

import "github.com/luyuancpp/pandora/pkg/dbguard"

const planPlayers = 100_000

// Budgets 是公会 / 临时群表的容量预算(上限均由 §9.18 写入侧校验保护)。
func Budgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{Table: "guilds", MaxRows: planPlayers / 10 * 3, MaxAvgRowBytes: 256,
			Note: "公会数 ≈ 玩家数/最小成员数;超限查建会是否被刷"},
		{Table: "guild_members", MaxRows: planPlayers * 3, MaxAvgRowBytes: 128,
			Note: "单归属:每玩家至多一行"},
		{Table: "guild_join_requests", MaxRows: planPlayers * 20 * 3, MaxAvgRowBytes: 192,
			Note: "终态行保留 90 天;超限先查 guild sweep 是否在跑"},
		{Table: "chat_groups", MaxRows: planPlayers * 50 * 3, MaxAvgRowBytes: 256},
		{Table: "chat_group_members", MaxRows: planPlayers * 50 * 3, MaxAvgRowBytes: 128,
			Note: "受 max_group_members=50 / max_groups_per_player=50 双向约束(§9.18)"},
		{Table: "player_group_counts", MaxRows: planPlayers * 3, MaxAvgRowBytes: 128},
	}
}
