// backend_check.go —— 权威库后端强校验(共享实现)。
//
// 为什么在 pkg 而不在某个服务的 internal:同一套校验已有两个使用方,而各服务是独立 go module
// (services/*/go.mod),owner 的 internal/data 无法被 login import,不下沉就只能复制一份:
//   - owner(§9.22):owner CAS 依赖「线性一致 + 确认写不回滚」,MySQL 异步复制主从切换会
//     回滚已确认写,一次回滚就可能让两台 DS 同时拿到同一玩家的 owner(双 owner 脑裂);
//   - login(全服单点扩容,2026-07-27):pandora_account 迁 TiDB 后,-Prod 产物必须确保真的
//     连在 TiDB 上,且**字符串比较语义**与原单机 MySQL 逐字节等价。
// 两份各自实现的安全校验必然漂移(§15.5),故下沉共享。
//
// 判定一律 fail-closed:证不了就返回错误,由调用方启动期 os.Exit,不允许"看着像对的"上线。
package mysqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// AssertTiDBBackend 校验连接的权威库确为 TiDB,不是则返回错误(调用方 fail-fast 退出)。
// TiDB 的 VERSION() 形如 "8.0.11-TiDB-v8.5.1",以 "-TiDB-" 特征串判定。
func AssertTiDBBackend(ctx context.Context, db *sql.DB) error {
	_, err := queryTiDBVersion(ctx, db)
	return err
}

// IsTiDBVersion 判定 VERSION() 返回串是否 TiDB。
func IsTiDBVersion(version string) bool {
	return strings.Contains(version, "-TiDB-")
}

// tidbVersionRe 从 "8.0.11-TiDB-v8.5.1" 里取出 TiDB 自身版本号(8,5,1)。
var tidbVersionRe = regexp.MustCompile(`-TiDB-v(\d+)\.(\d+)\.(\d+)`)

// queryTiDBVersion 查 VERSION() 并断言是 TiDB,返回原始版本串。
func queryTiDBVersion(ctx context.Context, db *sql.DB) (string, error) {
	var version string
	if err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		return "", fmt.Errorf("权威库 VERSION() 查询失败: %w", err)
	}
	if !IsTiDBVersion(version) {
		return version, fmt.Errorf("权威库要求 TiDB(require_tidb=true),实际 VERSION()=%q:"+
			"MySQL 异步复制切换会回滚已确认写,拒绝启动", version)
	}
	return version, nil
}

// AssertTiDBVersionAtLeast 断言后端 TiDB 版本不低于 major.minor。
//
// 为什么需要:本仓 TiDB 版 DDL 用到的特性各有版本门槛 —— AUTO_RANDOM(v3.1+)、
// /*T![clustered_index] NONCLUSTERED */(v5.0+)、golang-migrate 依赖的 GET_LOCK(v6.2+)、
// utf8mb4_0900_ai_ci(v7.4+)。只判 "-TiDB-" 特征串挡不住"是 TiDB 但太老":
// 老版本对 utf8mb4_0900_ai_ci 是**静默回退**到 utf8mb4_bin,不报错,正是最危险的形态。
// 版本号解析不出来时按 fail-closed 拒绝(不假设它足够新)。
func AssertTiDBVersionAtLeast(ctx context.Context, db *sql.DB, major, minor int) error {
	version, err := queryTiDBVersion(ctx, db)
	if err != nil {
		return err
	}
	return tidbVersionAtLeast(version, major, minor)
}

// tidbVersionAtLeast 是 AssertTiDBVersionAtLeast 的纯函数部分(便于不依赖真实库做表驱动测试)。
func tidbVersionAtLeast(version string, major, minor int) error {
	m := tidbVersionRe.FindStringSubmatch(version)
	if m == nil {
		return fmt.Errorf("无法从 VERSION()=%q 解析 TiDB 版本号,拒绝按「版本足够新」假设启动", version)
	}
	gotMajor, _ := strconv.Atoi(m[1])
	gotMinor, _ := strconv.Atoi(m[2])
	if gotMajor < major || (gotMajor == major && gotMinor < minor) {
		return fmt.Errorf("后端 TiDB 版本 v%d.%d 低于要求的 v%d.%d(VERSION()=%q):"+
			"本库 DDL 依赖的排序规则 / 索引特性在更低版本上会静默降级,拒绝启动",
			gotMajor, gotMinor, major, minor, version)
	}
	return nil
}

// safeCollationName 限制可插入 SQL 的 collation 标识符字符集。collation 名不能用占位符传参
// (它是标识符不是值),故这里做白名单校验;来源本就是 information_schema 而非用户输入,
// 此处是纵深防御。
var safeCollationName = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// AssertColumnCollationSemantics 断言 table.column 的排序规则在**当前后端上的实际行为**符合预期,
// 而不是比对 collation 名字。
//
// 为什么必须行为探针(TiDB 特有的静默陷阱):
//   - utf8mb4_0900_ai_ci 自 TiDB v7.4.0 起才支持,更早版本**静默回退**到 utf8mb4_bin;
//   - 即便版本够,大小写不敏感真正生效还要求集群启用「新 collation 框架」
//     (new_collations_enabled_on_first_bootstrap,自 v6.0.0 起默认 true)。关闭时 TiDB
//     只在**语法上**接受 _ci,**语义上按 binary 比较**;
//   - 该配置只在集群首次 bootstrap 生效且**之后不可更改**,老集群 / 显式关过的集群改不回来,
//     只能重建集群。
//
// 两个维度都要查,因为退化路径不同:
//   - wantCaseInsensitive:退化成 _bin 时 'A' != 'a' —— 老玩家换个大小写就登不进,
//     且能用大小写变体抢注同名账号;
//   - wantNoPad:utf8mb4_0900_ai_ci 是 NO PAD,而 utf8mb4_bin / utf8mb4_general_ci 都是
//     PAD SPACE。退化成 PAD SPACE 会让 "abc " 与 "abc" 塌成同一个值 —— 存量数据导入时
//     唯一键直接冲突,或静默把两个账号并成一个。
//
// 探针取的是该列**实际生效**的 collation 名(而不是假设 DDL 已生效),再用它做真实比较。
func AssertColumnCollationSemantics(
	ctx context.Context, db *sql.DB, table, column string, wantCaseInsensitive, wantNoPad bool,
) error {
	const q = `SELECT COLLATION_NAME FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?`
	var collation sql.NullString
	if err := db.QueryRowContext(ctx, q, table, column).Scan(&collation); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("列 %s.%s 不存在,无法校验排序规则(schema 未就绪?)", table, column)
		}
		return fmt.Errorf("查询 %s.%s 排序规则失败: %w", table, column, err)
	}
	if !collation.Valid || collation.String == "" {
		return fmt.Errorf("列 %s.%s 无排序规则(非字符串列?),拒绝按预期比较语义启动", table, column)
	}
	name := collation.String
	if !safeCollationName.MatchString(name) {
		return fmt.Errorf("列 %s.%s 的排序规则名 %q 含非法字符,拒绝拼接探针 SQL", table, column, name)
	}

	// 显式 COLLATE 的一侧决定比较规则。collation 不被后端支持时本语句直接报错,同样 fail-closed。
	var gotCaseInsensitive, gotPadSpace bool
	probe := fmt.Sprintf("SELECT 'A' COLLATE %s = 'a', 'a ' COLLATE %s = 'a'", name, name)
	if err := db.QueryRowContext(ctx, probe).Scan(&gotCaseInsensitive, &gotPadSpace); err != nil {
		return fmt.Errorf("排序规则 %s 行为探针执行失败(后端可能不支持该 collation): %w", name, err)
	}

	if wantCaseInsensitive && !gotCaseInsensitive {
		return fmt.Errorf("列 %s.%s 声明排序规则 %s,但后端实际按**大小写敏感**比较:"+
			"TiDB 需 v7.4.0+ 且集群首次 bootstrap 时启用新 collation 框架"+
			"(new_collations_enabled_on_first_bootstrap,该配置事后不可更改);"+
			"继续启动会让唯一键语义与单机 MySQL 不一致,拒绝启动", table, column, name)
	}
	if wantNoPad && gotPadSpace {
		return fmt.Errorf("列 %s.%s 声明排序规则 %s,但后端实际是 PAD SPACE(尾随空格被忽略),"+
			"而单机 MySQL 的 utf8mb4_0900_ai_ci 是 NO PAD:两者会把 %q 与 %q 判成不同/相同两种结果,"+
			"存量数据迁移时唯一键冲突或静默并号,拒绝启动", table, column, name, "abc ", "abc")
	}
	return nil
}
