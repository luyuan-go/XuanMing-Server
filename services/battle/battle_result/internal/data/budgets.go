// budgets.go — battle_result 服务的库容量预算(CLAUDE.md §9.24,2026-07-22)。
// 口径与 fail-open 语义见 inventory/internal/data/budgets.go 头注释。
package data

import (
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/conf"
)

// 容量规划基数:日活 1 万,每人每天 10 局,每局 10 人 → 每天约 1 万局。
const (
	planDailyMatches = 10_000
	planPerMatch     = 10 // 单局人数(stats / progress_player 的行放大系数)
	// planRetentionDays 直接取保留期上限(六个月),不另抄一份常量:
	// 预算 = 稳态最大行数 × 3 倍余量,保留期一改预算必须同步,否则告警要么恒响要么形同虚设。
	planRetentionDays = conf.HistoryRetentionMaxDays
)

// Budgets 是 pandora_battle 库的容量预算。
//
// 出箱表给的是**积压告警线**而非容量上限:它们投递成功即删,稳态应接近空,
// 堆积 = 投递链堵塞(kafka / 下游服务不可用),属告警问题不是增长问题。
func Budgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{
			// 保留 180 天(六个月):1 万局/天 × 180 × 3。
			Table: "battles", MaxRows: planDailyMatches * planRetentionDays * 3, MaxAvgRowBytes: 256,
			Note: "对局结算头,保留 180 天;超限先查 battle 保留期清理是否在跑(日志 battle_retention_battles_purged)",
		},
		{
			Table: "battle_player_stats", MaxRows: planDailyMatches * planPerMatch * planRetentionDays * 3, MaxAvgRowBytes: 192,
			Note: "随 battles 同事务批删;行数 ≈ 对局数 × 单局人数",
		},
		{Table: "battle_progress_stream", MaxRows: planDailyMatches * planRetentionDays * 3, MaxAvgRowBytes: 128,
			Note: "已结算行保留 180 天;未结算陈年行永不清但有 ERROR 告警(battle_retention_stale_unsettled_progress)"},
		{Table: "battle_progress_player", MaxRows: planDailyMatches * planPerMatch * planRetentionDays * 3, MaxAvgRowBytes: 128},
		{Table: "battle_progress_item_balance", MaxRows: planDailyMatches * planPerMatch * planRetentionDays * 3 * 8, MaxAvgRowBytes: 96,
			Note: "phase0 每场玩家同 item pickup/支出余额;随 settled progress 成组清理"},
		{Table: "battle_progress_action", MaxRows: planDailyMatches * planPerMatch * planRetentionDays * 3 * 8, MaxAvgRowBytes: 128,
			Note: "consume/discard durable outcome;随 settled progress 成组清理,响应丢失回放依据"},
		// ── 出箱表:积压告警线 ──
		{Table: "player_update_outbox", MaxRows: 200_000, MaxAvgRowBytes: 768,
			Note: "投递成功即删;堆积 = kafka 投递链堵塞,查 RunOutboxPublisher 日志"},
		{Table: "battle_drop_outbox", MaxRows: 200_000, MaxAvgRowBytes: 1536,
			Note: "冻结完整/stack/instance 三份 CSV;堆积 = inventory GrantItems/GrantInstances 长期失败"},
		{Table: "battle_progress_outbox", MaxRows: 500_000, MaxAvgRowBytes: 768,
			Note: "实时进度出箱,写入最密集;堆积 = player/inventory 发放链堵塞"},
		{Table: "match_release_outbox", MaxRows: 200_000, MaxAvgRowBytes: 1024,
			Note: "payload 含 repeated player_ids(VARBINARY(1024));队伍规模变大会逼近列上限"},
		{Table: "terminal_release_outbox", MaxRows: 200_000, MaxAvgRowBytes: 1024},
		{Table: "battle_exit_proof_outbox", MaxRows: 200_000, MaxAvgRowBytes: 2048},
	}
}

// BigFields 是列级字节预算(全表扫描,仅人工 / 低频触发)。
func BigFields() []dbguard.ColumnBudget {
	return []dbguard.ColumnBudget{
		{Table: "match_release_outbox", Column: "payload", MaxBytes: 768,
			Note: "列是 VARBINARY(1024);768=75% 预警线。超限说明 player_ids 随队伍规模胀大,再涨会让整场结算的释放出箱写失败"},
		{Table: "battle_exit_proof_outbox", Column: "payload", MaxBytes: 1536,
			Note: "列是 VARBINARY(2048);1536=75% 预警线"},
	}
}
