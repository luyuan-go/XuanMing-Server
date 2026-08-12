// player_schema_mysql_test.go — 直接重放发布 schema 的真 MySQL 测试底座(2026-08-11)。
//
// 与同包 attribute_repo_mysql_test.go 的 openAttributeTestDB 并存,但分工不同:
// 那份为隔离 AllocateAttributePoints 的锁语义,在测试里另抄了三张表的最小 DDL;
// 本底座**重放 deploy/mysql-init 的发布 SQL 原文**(mission 域 newMissionTestDB 同款做法),
// 于是 players / mmr_history / player_skill_cards / player_reward_claims 的列类型、默认值、
// 唯一键与 UNSIGNED 属性都与线上逐字一致。这一点对本轮新增的三组用例是必要条件而非偏好:
// 它们断言的正是这些约束本身(uk 命中即幂等、UNSIGNED 碎片不得下溢、version 乐观锁),
// 抄一份 DDL 等于把断言建在副本上,副本与发布 SQL 漂移时测试照样绿。
//
// 门控沿用仓库既有约定:PANDORA_TEST_MYSQL_DSN 必须不带库名(测试自建随机临时库并在
// 结束时删掉);未设置 → 明确 Skip;已设置但不可达 / 建库失败 → 硬失败,绝不 false-green。
//
//	$env:PANDORA_TEST_MYSQL_DSN='root:<pw>@tcp(127.0.0.1:3307)/?parseTime=true&loc=UTC&charset=utf8mb4'
//	go test ./internal/data/ -count=1 -run '_MySQL$' -v
package data

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

const playerSchemaSetupTimeout = 90 * time.Second

var playerSchemaTestDBPattern = regexp.MustCompile(`^pandora_player_schema_it_[0-9]+_[0-9a-f]{12}$`)

// playerSchemaFiles 是本底座重放的发布 SQL(顺序即依赖顺序)。
// 两份都以 `USE \`pandora_player\`;` 开头,重放时被替换成随机临时库。
var playerSchemaFiles = []string{
	"04-player-tables.sql",
	"13-reward-claim-tables.sql",
}

// newPlayerSchemaDB 建随机临时库、重放发布 schema,并返回指向该库的连接。
func newPlayerSchemaDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("PANDORA_TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("PANDORA_TEST_MYSQL_DSN 未设置,跳过真实 MySQL 集成测试")
	}
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("解析 PANDORA_TEST_MYSQL_DSN: %v", err)
	}
	if strings.TrimSpace(cfg.DBName) != "" {
		t.Fatalf("PANDORA_TEST_MYSQL_DSN 禁止携带库名(拿到 %q):测试要自建临时库,不能碰业务库", cfg.DBName)
	}
	cfg.MultiStatements = true // 发布 SQL 是多语句文件,必须整份重放
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second

	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开 MySQL 管理连接: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), playerSchemaSetupTimeout)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("已设 PANDORA_TEST_MYSQL_DSN 但 MySQL 不可达(不允许静默 PASS): %v", err)
	}

	dbName := newPlayerSchemaDBName(t)
	if !playerSchemaTestDBPattern.MatchString(dbName) { // 二次校验,永不误删业务库
		t.Fatalf("内部错误:随机临时库名未通过安全校验: %q", dbName)
	}
	if _, err := admin.ExecContext(ctx,
		"CREATE DATABASE `"+dbName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		_ = admin.Close()
		t.Fatalf("创建随机临时库 %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dcancel()
		// 删库前再独立校验一次库名;异常时宁可泄漏临时库也绝不 DROP。
		if playerSchemaTestDBPattern.MatchString(dbName) {
			if _, err := admin.ExecContext(dctx, "DROP DATABASE IF EXISTS `"+dbName+"`"); err != nil {
				t.Errorf("删除随机临时库 %s: %v", dbName, err)
			}
		} else {
			t.Errorf("拒绝删除未通过安全校验的数据库 %q", dbName)
		}
		if err := admin.Close(); err != nil {
			t.Errorf("关闭 MySQL 管理连接: %v", err)
		}
	})

	for _, file := range playerSchemaFiles {
		if _, err := admin.ExecContext(ctx, readPlayerSchema(t, file, dbName)); err != nil {
			t.Fatalf("重放 %s: %v", file, err)
		}
	}

	testCfg := cfg.Clone()
	testCfg.DBName = dbName
	db, err := sql.Open("mysql", testCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开随机临时库 %s: %v", dbName, err)
	}
	// 并发用例要同时持有多条连接(N 个 writer + 断言读),池太小会把并发退化成串行,
	// 让"锁语义"这类断言失去意义。
	db.SetMaxOpenConns(24)
	db.SetMaxIdleConns(24)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("连接随机临时库 %s: %v", dbName, err)
	}
	var selected string
	if err := db.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&selected); err != nil {
		t.Fatalf("校验当前数据库: %v", err)
	}
	if selected != dbName {
		t.Fatalf("连接落到非预期库 %q,期望随机临时库 %q", selected, dbName)
	}
	return db
}

func newPlayerSchemaDBName(t *testing.T) string {
	t.Helper()
	seed := make([]byte, 6)
	if _, err := rand.Read(seed); err != nil {
		t.Fatalf("生成临时库随机后缀: %v", err)
	}
	return fmt.Sprintf("pandora_player_schema_it_%d_%s", time.Now().UnixNano(), hex.EncodeToString(seed))
}

// readPlayerSchema 读发布 SQL 原文并把 USE 目标换成临时库。
// USE 锚点数量必须恰为 1:发布文件若改成多库或删掉 USE,这里宁可硬失败,
// 也不能悄悄把表建到默认库(那会污染业务库)。
func readPlayerSchema(t *testing.T, file, dbName string) string {
	t.Helper()
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("无法定位测试文件路径")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(self), "..", "..", "..", "..", "..",
		"deploy", "mysql-init", file))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取发布 schema %s: %v", path, err)
	}
	schema := string(raw)
	const needle = "USE `pandora_player`;"
	if got := strings.Count(schema, needle); got != 1 {
		t.Fatalf("%s 的 USE 锚点数量异常: %d(期望 1)", file, got)
	}
	return strings.Replace(schema, needle, "USE `"+dbName+"`;", 1)
}

// seedPlayerProfile 用生产路径 EnsureProfile 建档(而不是测试自拼 INSERT):
// 建档语义本身也在被测面内,自拼 INSERT 会绕开默认值与 uk_nickname 约束。
func seedPlayerProfile(t *testing.T, repo *MySQLPlayerRepo, playerID uint64, baseMMR int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := repo.EnsureProfile(ctx, playerID, fmt.Sprintf("it_player_%d", playerID), baseMMR); err != nil {
		t.Fatalf("建档玩家 %d: %v", playerID, err)
	}
}

// playerCounters 读 players 行上的三个累加计数,供幂等断言使用。
func playerCounters(t *testing.T, db *sql.DB, playerID uint64) (mmr, battles, wins int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.QueryRowContext(ctx,
		`SELECT mmr, total_battles, total_wins FROM players WHERE player_id = ?`,
		playerID).Scan(&mmr, &battles, &wins); err != nil {
		t.Fatalf("读取玩家 %d 计数: %v", playerID, err)
	}
	return mmr, battles, wins
}

// countRows 是断言用的通用计数(临时库内,表名由调用方以字面量传入)。
func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := db.QueryRowContext(ctx, query, args...).Scan(&n); err != nil {
		t.Fatalf("计数查询失败(%s): %v", query, err)
	}
	return n
}
