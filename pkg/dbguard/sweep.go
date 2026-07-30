// sweep.go — 保留期清理的执行模式(2026-07-22,用户指令:"不能清理我的数据,只能打印日志")。
//
// # 为什么默认不删
//
// 自动删除生产数据是不可逆操作:清理条件写错、保留期配错、幂等窗口算错,任何一处出问题
// 都会静默删掉不该删的玩家数据,而且**删完才发现**。所以本包把执行模式的**零值定为
// ModeReportOnly**——服务默认只统计"有多少行满足清理条件"并打日志告警,一行都不删;
// 真删必须由配置显式写 `retention_mode: delete` 打开。
//
// 这不违反 §9.24(数据增长必须有界):§9.24 要解决的是"无界增长没人知道",
// report-only 模式同样把增长暴露出来(日志 + metric + dbcheck 门禁),
// 只是把"什么时候真删"的决定权交回人手里。**代价是库会继续增长**,
// 所以 report-only 的日志必须持续可见、必须给出待清理量,让人能判断何时开删。
//
// # 条件只写一遍
//
// 最危险的实现方式是"COUNT 用一个 WHERE、DELETE 用另一个 WHERE"——两份条件迟早漂移,
// 于是报告说"待清理 0 行",实际删了 10 万行。SweepTable 强制 Count 与 Delete
// **共用同一个 where 字符串与同一组参数**,从机制上排除这种漂移。
package dbguard

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"

	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/metrics"
)

// Mode 是保留期清理的执行模式。**零值 = ModeReportOnly**(默认不删)。
type Mode int32

const (
	// ModeReportOnly 只统计满足清理条件的行数并告警,不删任何数据。默认值。
	ModeReportOnly Mode = 0
	// ModeDelete 真实执行删除。必须由配置显式开启(retention_mode: delete)。
	ModeDelete Mode = 1
)

// String 供日志与 metric label 使用。
func (m Mode) String() string {
	if m == ModeDelete {
		return "delete"
	}
	return "report_only"
}

// ParseMode 解析配置里的保留期模式。
//
// 空串 / "report_only" / "report" → ModeReportOnly(默认,不删);
// "delete" → ModeDelete。其他值返回错误 —— **不认识的值绝不能猜成 delete**,
// 拼错一个字母就开始删生产数据是不可接受的失败模式;调用方应 fail-fast 拒启。
func ParseMode(s string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "report_only", "report", "report-only":
		return ModeReportOnly, nil
	case "delete":
		return ModeDelete, nil
	default:
		return ModeReportOnly, fmt.Errorf(
			"dbguard: 无法识别的 retention_mode %q(只接受 \"report_only\"(默认,不删) 或 \"delete\")", s)
	}
}

// Outcome 是一次清理的结果。
type Outcome struct {
	Mode Mode
	// Matched 是满足清理条件的行数。
	//   ModeReportOnly:本次统计到的待清理总量(不受 limit 截断,给的是真实积压规模);
	//   ModeDelete:等于 Deleted。
	Matched int64
	// Deleted 是实际删除的行数(ModeReportOnly 恒为 0)。
	Deleted int64
	// Truncated 表示 ModeDelete 下本批打满了 limit(可能还有积压,调用方可继续下一批)。
	Truncated bool
}

// Cleaned 返回本次是否真的删了数据(供调用方决定日志措辞)。
func (o Outcome) Cleaned() bool { return o.Deleted > 0 }

var (
	retentionPending = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pandora_db_retention_pending_rows",
		Help: "满足保留期清理条件但尚未删除的行数(report_only 模式下即为持续积压量)。持续上涨 = 需要人决定开删或调保留期。",
	}, []string{"db", "table"})

	retentionDeleted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pandora_db_retention_deleted_rows_total",
		Help: "保留期清理实际删除的行数(仅 delete 模式非零)。",
	}, []string{"db", "table"})
)

func init() {
	metrics.Register(retentionPending)
	metrics.Register(retentionDeleted)
}

// ReportPending 供**多表事务清理**场景复用 report-only 的日志与 metric
// (那类清理不是单条 DELETE,用不了 SweepTable,但告警口径必须与之完全一致,
// 否则同一件事在不同服务里长得不一样,排查时对不上)。
//
// 调用方自己算出 pending(满足清理条件的实体数),本函数只负责统一"怎么报"。
// unit 说明 pending 的计量单位(如 "matches"),因为多表清理删的是"一组行"不是"一行"。
func ReportPending(ctx context.Context, dbLabel, table, unit string, pending int64) {
	retentionPending.WithLabelValues(dbLabel, table).Set(float64(pending))
	if pending <= 0 {
		return
	}
	plog.With(ctx).Warnw(
		"msg", "db_retention_pending_not_deleted",
		"db", dbLabel, "table", table, "pending_rows", pending, "unit", unit, "mode", ModeReportOnly.String(),
		"hint", "按当前配置只报告不删除(retention_mode=report_only);"+
			"库会继续增长,确认清理条件无误后设 retention_mode=delete 开启实删",
	)
}

// ReportDeleted 供多表事务清理场景复用 delete 模式的日志与 metric。
func ReportDeleted(ctx context.Context, dbLabel, table, unit string, deleted int64) {
	if deleted <= 0 {
		return
	}
	retentionDeleted.WithLabelValues(dbLabel, table).Add(float64(deleted))
	plog.With(ctx).Infow(
		"msg", "db_retention_deleted",
		"db", dbLabel, "table", table, "deleted", deleted, "unit", unit,
	)
}

// execer 是 SweepTable 需要的最小数据库能力(*sql.DB 与 *sql.Tx 都满足)。
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// SweepTable 按 mode 执行一张表的保留期清理,并统一打日志 + 更新 metric。
//
// where 不含 "WHERE" 关键字;args 是 where 里占位符的参数。
// **Count 与 Delete 共用同一 where + args**(条件只写一遍,杜绝报告与实删漂移)。
//
//	ModeReportOnly:执行 SELECT COUNT(*) ... WHERE <where>,打 WARN 日志(有待清理时),不删。
//	ModeDelete:    执行 DELETE ... WHERE <where> LIMIT ?,打 INFO 日志。
//
// dbLabel / table 只用于日志与 metric label(低基数)。table 与 where 必须是调用方
// 硬编码的字面量,不接受外部输入(SQL 标识符位置无法参数化)。
func SweepTable(ctx context.Context, db execer, mode Mode, dbLabel, table, where string, limit int, args ...any) (Outcome, error) {
	out := Outcome{Mode: mode}

	if mode != ModeDelete {
		// report-only:只数,不动。
		var n int64
		q := "SELECT COUNT(*) FROM `" + table + "` WHERE " + where
		if err := db.QueryRowContext(ctx, q, args...).Scan(&n); err != nil {
			return out, fmt.Errorf("dbguard: 统计 %s 待清理行数: %w", table, err)
		}
		out.Matched = n
		retentionPending.WithLabelValues(dbLabel, table).Set(float64(n))
		if n > 0 {
			// WARN 而非 INFO:这是"有数据该清但按配置没清"的持续状态,需要人看见并决定,
			// 不是可忽略的例行信息。
			plog.With(ctx).Warnw(
				"msg", "db_retention_pending_not_deleted",
				"db", dbLabel, "table", table, "pending_rows", n, "mode", mode.String(),
				"hint", "按当前配置只报告不删除(retention_mode=report_only);"+
					"库会继续增长,确认清理条件无误后设 retention_mode=delete 开启实删",
			)
		}
		return out, nil
	}

	// delete:真删。
	q := "DELETE FROM `" + table + "` WHERE " + where + " LIMIT ?"
	res, err := db.ExecContext(ctx, q, append(append([]any(nil), args...), limit)...)
	if err != nil {
		return out, fmt.Errorf("dbguard: 清理 %s: %w", table, err)
	}
	n, _ := res.RowsAffected()
	out.Matched, out.Deleted = n, n
	out.Truncated = limit > 0 && n >= int64(limit)
	if n > 0 {
		retentionDeleted.WithLabelValues(dbLabel, table).Add(float64(n))
		plog.With(ctx).Infow(
			"msg", "db_retention_deleted",
			"db", dbLabel, "table", table, "deleted", n, "batch_limit", limit, "truncated", out.Truncated,
		)
	}
	// delete 模式下 pending gauge 归零语义不准(本批之后可能还有积压),
	// 交由 truncated 与下一轮 sweep 体现,这里不写 pending。
	return out, nil
}
