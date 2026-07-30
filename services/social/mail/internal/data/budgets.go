// budgets.go — mail 服务的库容量预算(CLAUDE.md §9.24,2026-07-22)。
//
// 只声明**本服务负责的表**(pandora_social 是 chat/friend/guild/mail 共用库,
// 各服务各管自己那几张,避免四份重复巡检)。
// 预算口径与 fail-open 语义见 inventory/internal/data/budgets.go 头注释。
package data

import "github.com/luyuancpp/pandora/pkg/dbguard"

const (
	planPlayers = 100_000
	planDAU     = 10_000
)

// Budgets 是邮件相关表的容量预算。
//
// payload 是 BLOB(集合序列化:标题+正文+附件),所以 MaxAvgRowBytes 是这里的关键信号:
// 行数正常但 avg_row 涨 = 某个发送方绕过了 MaxTitleLen/MaxBodyLen/MaxAttachments。
func Budgets() []dbguard.TableBudget {
	return []dbguard.TableBudget{
		{
			// 写扩散:日活 × 每人每天 5 封 × 过期缓冲 37 天 × 3。
			Table: "player_mail", MaxRows: planDAU * 5 * 37 * 3, MaxAvgRowBytes: 4 * 1024,
			Note: "写扩散收件箱;行数超限查 mail sweep 是否在跑 + MaxInboxSize 是否生效;" +
				"avg_row 超 4KB 查 payload 内标题/正文/附件是否绕过逐项上限",
		},
		{Table: "sys_mail", MaxRows: 100_000, MaxAvgRowBytes: 4 * 1024,
			Note: "全服一份,运营发送驱动;行数超限说明运营发信量异常或失效邮件未清"},
		{Table: "guild_mail", MaxRows: 500_000, MaxAvgRowBytes: 4 * 1024},
		{Table: "player_mail_cursor", MaxRows: planPlayers * 3, MaxAvgRowBytes: 128,
			Note: "每玩家一行游标"},
		{
			// 领取记录保留 180 天(登记例外:必须盖过邮件最长可领窗口)。
			Table: "player_mail_claim", MaxRows: planDAU * 5 * 180 * 3, MaxAvgRowBytes: 512,
			Note: "领取幂等记录,保留 180 天(§9.24 登记例外);intent_payload 为 DS 领取意图 blob",
		},
		{Table: "player_mail_archive", MaxRows: planDAU * 5 * 90 * 3, MaxAvgRowBytes: 4 * 1024,
			Note: "过期未领附件归档,保留 90 天"},
	}
}

// BigFields 是列级字节预算(全表扫描,仅 dbcheck -size-check / 人工触发)。
func BigFields() []dbguard.ColumnBudget {
	return []dbguard.ColumnBudget{
		{Table: "player_mail", Column: "payload", MaxBytes: 16 * 1024,
			Note: "设计上界约 10KB(标题 64rune + 正文 2048rune + 附件 16 条);超限用 -top-rows 定位 mail_id 再反序列化"},
		{Table: "sys_mail", Column: "payload", MaxBytes: 16 * 1024},
		{Table: "guild_mail", Column: "payload", MaxBytes: 16 * 1024},
		{Table: "player_mail_archive", Column: "payload", MaxBytes: 16 * 1024},
	}
}
