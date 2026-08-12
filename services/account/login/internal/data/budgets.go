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
		{Table: "player_no_counter", MaxRows: 8,
			// MaxAvgRowBytes 刻意留 0(=不检查):单行表的 avg_row_length 由
			// information_schema 按 data_length/rows 估算,InnoDB 最小分配一个 16KB 页
			// → 恒报 16384,与真实行长(≈9 字节)无关。设任何"按 schema 推算"的值都必然
			// 误报(实测 2026-08-10:budget=64 每轮巡检刷一条 ERROR)。行数由 MaxRows=8 兜住,
			// 单行表也不存在深度增长(无 blob/repeated 字段),缺这一项无覆盖损失。
			Note: "角色编号全局发号计数器,恒 1 行(§9.24 登记豁免:发号权威闸,不清理);行长不设限=单行表 avg 估算恒为页大小"},
	}
}
