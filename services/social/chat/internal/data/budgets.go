// budgets.go — chat 服务的库容量预算(CLAUDE.md §9.24,2026-07-22)。
// 只声明本服务负责的表(pandora_social 共用库);口径见 inventory 同名文件头注释。
package data

import "github.com/luyuancpp/pandora/pkg/dbguard"

// Budgets 是私聊历史表的容量预算。
//
// 私聊是全服写入最密集的社交表之一:日活 1 万 × 每人每天 50 条 × 保留 90 天 × 3。
func Budgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{
			Table: "chat_private_messages", MaxRows: 10_000 * 50 * 90 * 3, MaxAvgRowBytes: 768,
			Note: "私聊历史,保留 90 天;行数超限查 chat sweep 是否在跑(日志 chat_history_swept)" +
				"与发言速率;avg_row 超限查 MaxContentLen 是否生效(content 列 VARCHAR(512))",
		},
	}
}
