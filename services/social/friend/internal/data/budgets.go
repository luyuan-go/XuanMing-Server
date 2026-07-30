// budgets.go — friend 服务的库容量预算(CLAUDE.md §9.24,2026-07-22)。
// 只声明本服务负责的表(pandora_social 共用库);口径见 inventory 同名文件头注释。
package data

import "github.com/luyuancpp/pandora/pkg/dbguard"

const planPlayers = 100_000

// Budgets 是好友域表的容量预算。
//
// 三张受 §9.18 写入侧上限保护的表(好友 200 / 申请 200 / 黑名单 200),
// 行数上限 = 玩家数 × 每玩家上限 × 3;超限说明上限校验失效或玩家量级超规划。
func Budgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{Table: "friendships", MaxRows: planPlayers * 200 * 3, MaxAvgRowBytes: 128,
			Note: "双向各一行;超限查 max_friends 上限校验是否失效(§9.18)"},
		{Table: "friend_requests", MaxRows: planPlayers * 200 * 3, MaxAvgRowBytes: 192,
			Note: "终态行保留 90 天;超限先查 friend sweep 是否在跑"},
		{Table: "blocks", MaxRows: planPlayers * 200 * 3, MaxAvgRowBytes: 128},
		{Table: "friend_player_guards", MaxRows: planPlayers * 3, MaxAvgRowBytes: 128,
			Note: "每玩家一行守卫"},
		{Table: "friend_pair_guards", MaxRows: planPlayers * 200 * 3, MaxAvgRowBytes: 128,
			Note: "关系对守卫随社交图 O(n²) 累积,有保留期清理;超限查 pair guard sweep"},
	}
}
