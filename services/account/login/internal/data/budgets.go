// budgets.go — login 服务的库容量预算(CLAUDE.md §9.24,2026-07-22)。
// 口径见 inventory/internal/data/budgets.go 头注释。
package data

import "github.com/luyuancpp/pandora/pkg/dbguard"

const planPlayers = 100_000

// Budgets 是 pandora_account 库的容量预算。
func Budgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{Table: "accounts", MaxRows: planPlayers * 3, MaxAvgRowBytes: 256,
			Note: "每账号一行;超限说明注册量超容量规划(或被刷注册)"},
		{Table: "player_roles", MaxRows: planPlayers * 3, MaxAvgRowBytes: 128},
		{Table: "player_session_generations", MaxRows: planPlayers * 3, MaxAvgRowBytes: 128,
			Note: "每玩家一行会话代际"},
		{
			// device_id 由客户端上报、单账号可堆多设备;保留 90 天兜底。
			Table: "account_devices", MaxRows: planPlayers * 10 * 3, MaxAvgRowBytes: 256,
			Note: "行数 ≈ 玩家数 × 设备数;超限查是否有人刷任意 device_id(保留期清理已落地,日志 stale_devices_purged)",
		},
		{Table: "account_bans", MaxRows: 1_000_000, MaxAvgRowBytes: 512,
			Note: "§9.24 登记豁免(运营合规审计,不清理);超限说明封禁量异常,需人工评估归档"},
		{Table: "register_no_counter", MaxRows: 8, MaxAvgRowBytes: 64,
			Note: "注册编号全局发号计数器,恒 1 行(§9.24 登记豁免:发号权威闸,不清理);上限留余量防 information_schema 估算抖动误报"},
	}
}
