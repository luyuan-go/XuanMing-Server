// Package dbguard 是数据库容量与大字段守护(CLAUDE.md §9 不变量 24 的运行时侧,2026-07-22)。
//
// 解决两类问题,对应两种完全不同的处置策略——**别搞混**:
//
//	① sql_mode 非严格 → **fail-fast 拒启动**(AssertStrictMode)
//	   非严格模式下超长写入会被 MySQL **静默截断**(err=nil,数据被砍掉一半,无任何错误与日志)。
//	   实测(MySQL 8.4,2026-07-22):往 VARBINARY(16) 写 100 字节,
//	     STRICT_TRANS_TABLES → Error 1406 Data too long(安全,写入失败);
//	     sql_mode=''        → err=nil 且 LENGTH()=16(静默丢 84 字节)。
//	   静默数据损坏比服务起不来严重得多,所以这里是唯一允许 fail-fast 的检查。
//
//	② 表行数 / 表字节 / 平均行长 / 单列字节超预算 → **ERROR 日志 + metric,不阻止启动**
//	   容量超限是"要去查的问题",不是"服务不能跑的理由"。拒绝启动会把容量问题升级成
//	   可用性事故(违反验收底线第 1 条:不卡玩家)。因此 Check 只观测与告警,永不返回致命错误。
//
// 为什么用 information_schema 而不是 COUNT(*):
//
//	TABLE_ROWS / DATA_LENGTH / AVG_ROW_LENGTH 是 InnoDB 采样估算,毫秒级、不锁表、不扫数据,
//	适合启动时与周期巡检。COUNT(*) 在千万行表上要几十秒,放启动路径会拖垮滚动更新
//	(§9.16 零停机)。估算值有 ±10% 误差,但容量告警看的是数量级,误差无关紧要。
//	需要精确值时用 dbcheck -exact(离线/发布门禁),不放服务启动路径。
//
// 三个信号的诊断价值(按灵敏度排序,排查时从上往下看):
//
//	AvgRowBytes 突增 → **单行变胖**,几乎必然是 bug(blob 里某个 repeated 字段无界增长)。
//	                    最灵敏:总量增长可能只是行数增长(正常),平均行长增长一定异常。
//	Bytes 突增      → 可能是行数正常增长,也可能是行变胖;要结合 Rows 一起看。
//	Rows 超预算     → 清理没追上写入 / 上限校验失效 / 业务量超容量规划。
//
// 列级 MAX(LENGTH(col)) 是全表扫描,**不放周期巡检**(会打爆 IO);只在表级告警后由人工用
// dbcheck -size-check 触发,或按天级低频跑。定位到具体行用 TopLargeRows。
package dbguard

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/metrics"
)

// ── Prometheus 指标(告警的权威来源;日志只是给人看的旁证)────────────────────
//
// 命名遵循 docs/design/infra.md §10:pandora_<subsystem>_<metric>。
// label 只有 db/table/column(低基数),绝不放 player_id(§12 禁止高基数 label)。
var (
	tableRows = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pandora_db_table_rows",
		Help: "表行数(information_schema 估算)。配合 pandora_db_table_rows_budget 做超限告警。",
	}, []string{"db", "table"})

	tableRowsBudget = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pandora_db_table_rows_budget",
		Help: "表行数预算上限(0=未设预算)。告警规则:rows > budget > 0。",
	}, []string{"db", "table"})

	tableBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pandora_db_table_bytes",
		Help: "表数据字节数(information_schema DATA_LENGTH,不含索引)。",
	}, []string{"db", "table"})

	tableAvgRowBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pandora_db_avg_row_bytes",
		Help: "表平均行字节数。**排查大字段最灵敏的信号**:突增 = 单行变胖 = blob 内部无界增长。",
	}, []string{"db", "table"})

	budgetViolations = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pandora_db_budget_violations_total",
		Help: "容量预算超限次数(按维度)。kind=rows|bytes|avg_row_bytes|column_bytes。",
	}, []string{"db", "table", "kind"})

	columnMaxBytes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pandora_db_column_max_bytes",
		Help: "单列最大字节数(全表扫描,仅低频/人工触发时更新)。",
	}, []string{"db", "table", "column"})
)

func init() {
	metrics.Register(tableRows)
	metrics.Register(tableRowsBudget)
	metrics.Register(tableBytes)
	metrics.Register(tableAvgRowBytes)
	metrics.Register(budgetViolations)
	metrics.Register(columnMaxBytes)
}

// TableBudget 是一张表的容量预算。任一项为 0 = 该维度不检查。
//
// 定预算的口径(必须能说明推导依据,不准拍脑袋):
//   - MaxRows:按"容量规划量级 × 安全系数"推,例如 500 人/实例 × 预期实例数 × 每人行数 × 3;
//     或对有保留期的表:峰值写入速率 × 保留期 × 3。超了说明清理没追上或上限校验失效。
//   - MaxAvgRowBytes:按 schema 定长列之和 + blob 的**设计期望大小**推(不是列类型上限)。
//     例如 blob 设计装 20 个道具 ≈ 400 字节,就写 600,而不是写 BLOB 的 65535。
//     写成列类型上限等于没设:那样只有 MySQL 快报错时才告警,失去预警意义。
//   - MaxBytes:一般不用单独设(≈ MaxRows × MaxAvgRowBytes),只在有独立磁盘配额时设。
type TableBudget struct {
	Table          string
	MaxRows        int64
	MaxBytes       int64
	MaxAvgRowBytes int64
	// Note 是超限时打进日志的排查提示(例如"检查 xxx sweep 是否在跑 / 检查 yyy 上限校验")。
	Note string
}

// ColumnBudget 是单个大字段的字节预算(列级检查,全表扫描,低频用)。
type ColumnBudget struct {
	Table    string
	Column   string
	MaxBytes int64
	Note     string
}

// Guard 是某个库(单个 *sql.DB 连接对应的 database)的容量守护。
type Guard struct {
	db      *sql.DB
	dbName  string
	tables  []TableBudget
	columns []ColumnBudget
}

// New 构造 Guard。dbName 必须是该连接实际使用的 database 名(用于 information_schema 过滤
// 与 metric label);传空时由 Check 首次调用现查 DATABASE() 补上。
func New(db *sql.DB, dbName string, tables []TableBudget, columns []ColumnBudget) *Guard {
	return &Guard{db: db, dbName: dbName, tables: tables, columns: columns}
}

// AssertStrictMode 断言当前连接的 sql_mode 含 STRICT_TRANS_TABLES。
//
// **调用方应在启动时调用并在失败时退出进程**:非严格模式下超长写入静默截断,
// 玩家数据被无声砍断且无任何错误可观测,继续运行只会持续产生损坏数据。
// 这是本包唯一允许 fail-fast 的检查(对比 Check 的"只告警不阻断")。
//
// 注意查的是 @@session.sql_mode 而非 @@global:DSN 参数、连接初始化语句都可能让 session
// 与 global 不一致,而真正决定写入行为的是 session。
func AssertStrictMode(ctx context.Context, db *sql.DB) error {
	var mode string
	if err := db.QueryRowContext(ctx, "SELECT @@session.sql_mode").Scan(&mode); err != nil {
		return fmt.Errorf("dbguard: 读取 session sql_mode 失败: %w", err)
	}
	for _, m := range strings.Split(mode, ",") {
		if strings.TrimSpace(m) == "STRICT_TRANS_TABLES" {
			return nil
		}
	}
	return fmt.Errorf(
		"dbguard: session sql_mode 缺 STRICT_TRANS_TABLES(当前=%q)。"+
			"非严格模式下超长写入会被静默截断(err=nil 但数据被砍断),等于无声的数据损坏。"+
			"修法:MySQL 服务端 --sql-mode 保留默认值,或从 DSN 中移除覆盖 sql_mode 的参数", mode)
}

// strictModeProbeTimeout 是启动期断言的探测超时。取 5s:够慢网络下的一次往返,
// 又不会让 MySQL 不可达时把启动挂死(不可达本身会被 mysqlx 的 Ping 先拦掉)。
const strictModeProbeTimeout = 5 * time.Second

// AssertStrictModeStartup 是 AssertStrictMode 的启动期便捷版:自带超时,
// 免得每个服务 main 重复写 context.WithTimeout + cancel 三行。
//
// 用法(各服务 main 装配完 *sql.DB 后立刻调):
//
//	if serr := dbguard.AssertStrictModeStartup(db); serr != nil {
//		helper.Errorw("msg", "mysql_strict_mode_required", "err", serr)
//		os.Exit(1)
//	}
//
// 为什么不在包内直接 os.Exit / panic:各服务 main 的日志 helper 与退出约定不同
// (有的 Errorw+Exit,有的 panic),把决定权留给调用方,只统一"怎么探测"。
func AssertStrictModeStartup(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), strictModeProbeTimeout)
	defer cancel()
	return AssertStrictMode(ctx, db)
}

// Result 是一轮巡检的结果。Violations 非空 = 有超预算项(已打 ERROR 日志 + 计数 metric)。
type Result struct {
	Checked    int
	Violations []Violation
}

// Violation 是一条超预算记录。
type Violation struct {
	Table  string
	Column string // 列级检查才有
	Kind   string // rows | bytes | avg_row_bytes | column_bytes
	Actual int64
	Budget int64
	Note   string
}

// Check 跑一轮表级巡检(information_schema 估算,毫秒级,不锁表,不扫数据)。
//
// **永不返回致命错误、永不阻止调用方继续**:查询失败只记 Warn(数据库抖动不该影响业务);
// 超预算记 ERROR 日志 + metric 并放进 Result 供调用方决定是否额外处理。
//
// 调用时机:①服务启动装配完成后跑一次(拿到基线,启动即暴露既有超限);
// ②挂已有的 sweep / 巡检 ticker 周期跑(**不新建 timer 状态机**,遵守 CLAUDE.md 定时器纪律)。
func (g *Guard) Check(ctx context.Context) Result {
	log := plog.With(ctx)
	res := Result{}
	if len(g.tables) == 0 {
		return res
	}
	if g.dbName == "" {
		if err := g.db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&g.dbName); err != nil || g.dbName == "" {
			log.Warnw("msg", "dbguard_resolve_database_failed", "err", err)
			return res
		}
	}

	stats, err := g.tableStats(ctx)
	if err != nil {
		// 巡检失败不影响业务:下一轮自然重试。
		log.Warnw("msg", "dbguard_table_stats_failed", "db", g.dbName, "err", err)
		return res
	}

	for _, b := range g.tables {
		s, ok := stats[b.Table]
		if !ok {
			// 表不存在 = 该功能未部署 / 未迁移,不是容量问题,交给 mysqlx.CheckTables 管。
			continue
		}
		res.Checked++
		tableRows.WithLabelValues(g.dbName, b.Table).Set(float64(s.rows))
		tableBytes.WithLabelValues(g.dbName, b.Table).Set(float64(s.bytes))
		tableAvgRowBytes.WithLabelValues(g.dbName, b.Table).Set(float64(s.avgRow))
		tableRowsBudget.WithLabelValues(g.dbName, b.Table).Set(float64(b.MaxRows))

		check := func(kind string, actual, budget int64) {
			if budget <= 0 || actual <= budget {
				return
			}
			budgetViolations.WithLabelValues(g.dbName, b.Table, kind).Inc()
			res.Violations = append(res.Violations, Violation{
				Table: b.Table, Kind: kind, Actual: actual, Budget: budget, Note: b.Note,
			})
			// ERROR 级:这是"需要人去查"的信号(§9.24 有界承诺已被突破),不是可忽略的 Warn。
			log.Errorw(
				"msg", "db_capacity_budget_exceeded",
				"db", g.dbName, "table", b.Table, "kind", kind,
				"actual", actual, "budget", budget,
				"note", b.Note,
				"hint", "见 docs/design/db-capacity-guard.md 排查手册;行数超限先查清理任务是否在跑,平均行长超限查 blob 内 repeated 字段是否无界",
			)
		}
		check("rows", s.rows, b.MaxRows)
		check("bytes", s.bytes, b.MaxBytes)
		check("avg_row_bytes", s.avgRow, b.MaxAvgRowBytes)
	}
	return res
}

type tableStat struct {
	rows   int64
	bytes  int64
	avgRow int64
}

// tableStats 一次查出本库全部表的估算统计(单次查询,不按表 N 次往返)。
func (g *Guard) tableStats(ctx context.Context) (map[string]tableStat, error) {
	rows, err := g.db.QueryContext(ctx,
		`SELECT TABLE_NAME, COALESCE(TABLE_ROWS,0), COALESCE(DATA_LENGTH,0), COALESCE(AVG_ROW_LENGTH,0)
		 FROM information_schema.TABLES
		 WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'`, g.dbName)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]tableStat{}
	for rows.Next() {
		var name string
		var s tableStat
		if serr := rows.Scan(&name, &s.rows, &s.bytes, &s.avgRow); serr != nil {
			return nil, serr
		}
		out[name] = s
	}
	return out, rows.Err()
}

// CheckColumns 跑列级字节巡检(**全表扫描 MAX(LENGTH(col))**,成本高)。
//
// 不要放进 Check 的周期路径。用法:①表级 avg_row_bytes 告警后由人工/工具触发定位;
// ②天级低频巡检(如每天凌晨一次)。超预算同样只告警不阻断。
func (g *Guard) CheckColumns(ctx context.Context) Result {
	log := plog.With(ctx)
	res := Result{}
	for _, c := range g.columns {
		var maxLen, avgLen sql.NullInt64
		err := g.db.QueryRowContext(ctx,
			"SELECT MAX(LENGTH(`"+c.Column+"`)), AVG(LENGTH(`"+c.Column+"`)) FROM `"+c.Table+"`").
			Scan(&maxLen, &avgLen)
		if err != nil {
			log.Warnw("msg", "dbguard_column_scan_failed", "db", g.dbName, "table", c.Table, "column", c.Column, "err", err)
			continue
		}
		res.Checked++
		columnMaxBytes.WithLabelValues(g.dbName, c.Table, c.Column).Set(float64(maxLen.Int64))
		if c.MaxBytes <= 0 || maxLen.Int64 <= c.MaxBytes {
			continue
		}
		budgetViolations.WithLabelValues(g.dbName, c.Table, "column_bytes").Inc()
		res.Violations = append(res.Violations, Violation{
			Table: c.Table, Column: c.Column, Kind: "column_bytes",
			Actual: maxLen.Int64, Budget: c.MaxBytes, Note: c.Note,
		})
		log.Errorw(
			"msg", "db_column_size_budget_exceeded",
			"db", g.dbName, "table", c.Table, "column", c.Column,
			"max_bytes", maxLen.Int64, "avg_bytes", avgLen.Int64, "budget", c.MaxBytes,
			"note", c.Note,
			"hint", "用 dbguard.TopLargeRows 或 dbcheck -top-rows 定位到具体主键,再反序列化看是哪个字段爆了",
		)
	}
	return res
}

// LargeRow 是一条大行定位结果(主键 + 该列字节数)。
type LargeRow struct {
	PK    string
	Bytes int64
}

// TopLargeRows 定位某列最大的 N 行,返回主键与字节数——**排查大字段的第一步落点**。
//
// 拿到主键后的标准下一步:把该行的 blob dump 出来反序列化(proto/JSON),看是哪个
// repeated 字段元素数异常,再回到写入路径找为什么没有上限。
//
// pkCol 必须是调用方硬编码的主键列名(不接受外部输入):本函数用字符串拼接列名/表名,
// 参数化占位符在标识符位置不可用。limit 有界防止一次拉太多。
func TopLargeRows(ctx context.Context, db *sql.DB, table, pkCol, col string, limit int) ([]LargeRow, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	rows, err := db.QueryContext(ctx,
		"SELECT `"+pkCol+"`, LENGTH(`"+col+"`) AS n FROM `"+table+"` ORDER BY n DESC LIMIT ?", limit)
	if err != nil {
		return nil, fmt.Errorf("dbguard: top large rows %s.%s: %w", table, col, err)
	}
	defer func() { _ = rows.Close() }()
	var out []LargeRow
	for rows.Next() {
		var r LargeRow
		var n sql.NullInt64
		if serr := rows.Scan(&r.PK, &n); serr != nil {
			return nil, serr
		}
		r.Bytes = n.Int64
		out = append(out, r)
	}
	return out, rows.Err()
}
