package main

// account_player_no_migration_test.go 钉住 pandora_account 000006 的收敛契约。
//
// 000005 已经对 origin 暴露，必须按可能执行过的 immutable 迁移处理。000006 因而不能
// 假设库只处于单一版本：它既要接住普通的 register-only 存量库，也要接住
// mysql-init(target) 先建表、随后 000004 又补出 legacy 对象的双列/双索引/双计数器库。

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	accountV6UpPath   = "migrations/pandora_account/000006_reconcile_player_no.up.sql"
	accountV6DownPath = "migrations/pandora_account/000006_reconcile_player_no.down.sql"
	accountV7UpPath   = "migrations/pandora_account/000007_player_no_expand_compat.up.sql"
	accountV7DownPath = "migrations/pandora_account/000007_player_no_expand_compat.down.sql"

	canonicalPlayerNoComment = "角色编号(展示专用,禁作身份键/外键/幂等键;绑定角色实体——今 player_id 即角色身份,卖角色过户时随角色走、值不变,故一账号建 N 角色 = N 个编号;NULL=待补号,login 补号任务按 created_at+player_id 序异步分配,player-no-and-login-surge.md §3.3/§3.6.1)"
	canonicalNextNoComment   = "下一个待发角色编号"
	canonicalCounterComment  = "Pandora 角色编号全局发号计数器(单行 id=1;发号权威闸)"
)

func TestPandoraAccountV6PlayerNoReconcileContract(t *testing.T) {
	version, err := latestMigrationVersion("pandora_account")
	if err != nil {
		t.Fatalf("latestMigrationVersion: %v", err)
	}
	if version < 6 {
		t.Fatalf("pandora_account latest version=%d,期望至少包含 v6", version)
	}

	up := readEmbeddedMigration(t, accountV6UpPath)
	for _, fragment := range []string{
		"__pandora_player_no_reconcile_data_conflict__",
		"__pandora_player_no_reconcile_column_shape_invalid__",
		"__pandora_player_no_reconcile_counter_shape_invalid__",
		"__pandora_player_no_reconcile_legacy_index_invalid__",
		"DATA_TYPE = 'bigint'",
		"LOWER(COLUMN_TYPE) = 'bigint unsigned'",
		"IS_NULLABLE = 'YES'",
		"COLUMN_DEFAULT IS NULL",
		"a.`register_no` <> a.`player_no`",
		"b.`register_no` = a.`register_no`",
		"b.`player_no` = a.`register_no`",
		"UPDATE `accounts` SET `player_no` = `register_no`",
		"DROP INDEX `uk_register_no`",
		"DROP COLUMN `register_no`",
		"GREATEST(p.`next_no`, r.`next_no`)",
		"DROP TABLE `register_no_counter`",
		canonicalPlayerNoComment,
		canonicalNextNoComment,
		canonicalCounterComment,
	} {
		if !strings.Contains(up, fragment) {
			t.Errorf("000006 up 缺少收敛契约片段 %q", fragment)
		}
	}
	preflightAt := strings.Index(up, "PREPARE stmt_guard_player_no_column_shape")
	conflictAt := strings.Index(up, "PREPARE stmt_count_player_no_conflicts")
	firstWriteAt := strings.Index(up, "UPDATE `accounts` SET `player_no` = `register_no`")
	if preflightAt < 0 || conflictAt < 0 || firstWriteAt < 0 || preflightAt > conflictAt || conflictAt > firstWriteAt {
		t.Fatalf("000006 必须按 schema preflight → data conflict guard → first write 排序: preflight=%d conflict=%d write=%d", preflightAt, conflictAt, firstWriteAt)
	}

	if statements := executableStatements(readEmbeddedMigration(t, accountV6DownPath)); len(statements) != 0 {
		t.Fatalf("000006 down 必须是有解释的 no-op,不能重建已合并的 legacy 对象: %v", statements)
	}

	for path, content := range map[string]string{
		"deploy/mysql-init/02-account-tables.sql": readRepoFile(t, "../../deploy/mysql-init/02-account-tables.sql"),
		"deploy/tidb-init/03-account-tidb.sql":    readRepoFile(t, "../../deploy/tidb-init/03-account-tidb.sql"),
	} {
		for _, fragment := range []string{canonicalNextNoComment, canonicalCounterComment} {
			if !strings.Contains(content, fragment) {
				t.Errorf("%s 与 000006 canonical schema 漂移,缺少 %q", path, fragment)
			}
		}
	}
}

func TestPandoraAccountV7PlayerNoExpandCompatibilityContract(t *testing.T) {
	version, err := latestMigrationVersion("pandora_account")
	if err != nil {
		t.Fatalf("latestMigrationVersion: %v", err)
	}
	if version < 7 {
		t.Fatalf("pandora_account latest version=%d,期望至少包含 v7", version)
	}

	up := readEmbeddedMigration(t, accountV7UpPath)
	for _, fragment := range []string{
		"ADD COLUMN `register_no` BIGINT UNSIGNED",
		"ADD UNIQUE KEY `uk_register_no` (`register_no`)",
		"CREATE TABLE IF NOT EXISTS `register_no_counter`",
		"GREATEST(p.`next_no`,r.`next_no`)",
		"SET `register_no` = `player_no`",
		"SET `player_no` = `register_no`",
		"__pandora_player_no_expand_data_conflict__",
		"__pandora_player_no_expand_counter_conflict__",
	} {
		if !strings.Contains(up, fragment) {
			t.Errorf("000007 up 缺少不停服 expand 契约片段 %q", fragment)
		}
	}
	for _, forbidden := range []string{
		"ALTER TABLE `ACCOUNTS` DROP COLUMN", "ALTER TABLE `ACCOUNTS` DROP INDEX",
		"DROP TABLE `REGISTER_NO_COUNTER`", "RENAME COLUMN", "RENAME INDEX", "RENAME TABLE",
	} {
		if strings.Contains(strings.ToUpper(up), forbidden) {
			t.Errorf("000007 expand 禁止破坏 legacy schema,发现 %q", forbidden)
		}
	}
	if statements := executableStatements(readEmbeddedMigration(t, accountV7DownPath)); len(statements) != 0 {
		t.Fatalf("000007 down 必须 no-op,禁止回退时删除任一代对象: %v", statements)
	}

	for path, content := range map[string]string{
		"deploy/mysql-init/02-account-tables.sql": readRepoFile(t, "../../deploy/mysql-init/02-account-tables.sql"),
		"deploy/tidb-init/03-account-tidb.sql":    readRepoFile(t, "../../deploy/tidb-init/03-account-tidb.sql"),
	} {
		for _, fragment := range []string{
			"`player_no`", "`register_no`", "uk_player_no", "uk_register_no",
			"CREATE TABLE IF NOT EXISTS `player_no_counter`", "CREATE TABLE IF NOT EXISTS `register_no_counter`",
		} {
			if !strings.Contains(content, fragment) {
				t.Errorf("%s 缺少 Stable/Canary 共存对象 %q", path, fragment)
			}
		}
	}
}

type accountPlayerNoScenario struct {
	name                   string
	columns                string
	indexes                string
	seed                   string
	counters               []string
	wantError              bool
	wantConflictPlayerID   uint64
	wantConflictPlayerNo   sql.NullInt64
	wantConflictRegisterNo sql.NullInt64
	wantNumbers            map[uint64]sql.NullInt64
	wantNextNo             uint64
}

func TestPandoraAccountV6PlayerNoReconcileAcrossBackends(t *testing.T) {
	scenarios := []accountPlayerNoScenario{
		{
			name:        "old_only",
			columns:     "`register_no` BIGINT UNSIGNED NULL COMMENT 'legacy'",
			indexes:     "UNIQUE KEY `uk_register_no` (`register_no`)",
			seed:        "INSERT INTO accounts (player_id, account, register_no) VALUES (1, 'old-1', 7), (2, 'old-2', NULL)",
			counters:    []string{legacyCounterDDL, "INSERT INTO register_no_counter (id, next_no) VALUES (1, 20)"},
			wantNumbers: map[uint64]sql.NullInt64{1: {Int64: 7, Valid: true}, 2: {}},
			wantNextNo:  20,
		},
		{
			name:        "target_only",
			columns:     "`player_no` BIGINT UNSIGNED NULL COMMENT 'stale target comment'",
			indexes:     "UNIQUE KEY `uk_player_no` (`player_no`)",
			seed:        "INSERT INTO accounts (player_id, account, player_no) VALUES (1, 'target-1', 7), (2, 'target-2', NULL)",
			counters:    []string{targetCounterDDL, "INSERT INTO player_no_counter (id, next_no) VALUES (1, 20)"},
			wantNumbers: map[uint64]sql.NullInt64{1: {Int64: 7, Valid: true}, 2: {}},
			wantNextNo:  20,
		},
		{
			name:        "both_empty_legacy",
			columns:     "`player_no` BIGINT UNSIGNED NULL COMMENT 'target', `register_no` BIGINT UNSIGNED NULL COMMENT 'legacy'",
			indexes:     "UNIQUE KEY `uk_player_no` (`player_no`), UNIQUE KEY `uk_register_no` (`register_no`)",
			seed:        "INSERT INTO accounts (player_id, account, player_no, register_no) VALUES (1, 'both-1', 7, NULL), (2, 'both-2', NULL, NULL)",
			counters:    []string{targetCounterDDL, legacyCounterDDL, "INSERT INTO player_no_counter (id, next_no) VALUES (1, 20)", "INSERT INTO register_no_counter (id, next_no) VALUES (1, 25)"},
			wantNumbers: map[uint64]sql.NullInt64{1: {Int64: 7, Valid: true}, 2: {}},
			wantNextNo:  25,
		},
		{
			name:        "both_compatible_values",
			columns:     "`player_no` BIGINT UNSIGNED NULL COMMENT 'target', `register_no` BIGINT UNSIGNED NULL COMMENT 'legacy'",
			indexes:     "UNIQUE KEY `uk_player_no` (`player_no`), UNIQUE KEY `uk_register_no` (`register_no`)",
			seed:        "INSERT INTO accounts (player_id, account, player_no, register_no) VALUES (1, 'merge-1', 7, 7), (2, 'merge-2', NULL, 8)",
			counters:    []string{targetCounterDDL, legacyCounterDDL, "INSERT INTO player_no_counter (id, next_no) VALUES (1, 9)", "INSERT INTO register_no_counter (id, next_no) VALUES (1, 12)"},
			wantNumbers: map[uint64]sql.NullInt64{1: {Int64: 7, Valid: true}, 2: {Int64: 8, Valid: true}},
			wantNextNo:  12,
		},
		{
			name:                   "both_cross_player_conflict_fail_closed",
			columns:                "`player_no` BIGINT UNSIGNED NULL COMMENT 'target', `register_no` BIGINT UNSIGNED NULL COMMENT 'legacy'",
			indexes:                "UNIQUE KEY `uk_player_no` (`player_no`), UNIQUE KEY `uk_register_no` (`register_no`)",
			seed:                   "INSERT INTO accounts (player_id, account, player_no, register_no) VALUES (1, 'cross-1', 101, NULL), (2, 'cross-2', NULL, 101)",
			counters:               []string{targetCounterDDL, legacyCounterDDL, "INSERT INTO player_no_counter (id, next_no) VALUES (1, 400)", "INSERT INTO register_no_counter (id, next_no) VALUES (1, 500)"},
			wantError:              true,
			wantConflictPlayerID:   2,
			wantConflictPlayerNo:   sql.NullInt64{},
			wantConflictRegisterNo: sql.NullInt64{Int64: 101, Valid: true},
		},
		{
			name:                   "both_same_row_conflict_fail_closed",
			columns:                "`player_no` BIGINT UNSIGNED NULL COMMENT 'target', `register_no` BIGINT UNSIGNED NULL COMMENT 'legacy'",
			indexes:                "UNIQUE KEY `uk_player_no` (`player_no`), UNIQUE KEY `uk_register_no` (`register_no`)",
			seed:                   "INSERT INTO accounts (player_id, account, player_no, register_no) VALUES (1, 'same-row-1', 303, 304), (2, 'same-row-2', NULL, NULL)",
			counters:               []string{targetCounterDDL, legacyCounterDDL, "INSERT INTO player_no_counter (id, next_no) VALUES (1, 400)", "INSERT INTO register_no_counter (id, next_no) VALUES (1, 500)"},
			wantError:              true,
			wantConflictPlayerID:   1,
			wantConflictPlayerNo:   sql.NullInt64{Int64: 303, Valid: true},
			wantConflictRegisterNo: sql.NullInt64{Int64: 304, Valid: true},
		},
	}

	for _, backend := range []struct {
		name string
		env  string
	}{
		{name: "mysql", env: "PANDORA_TEST_MYSQL_DSN"},
		{name: "tidb", env: "PANDORA_TEST_TIDB_DSN"},
	} {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			dsn := strings.TrimSpace(os.Getenv(backend.env))
			if dsn == "" {
				t.Skipf("跳过真实 %s 迁移矩阵:未设 %s", backend.name, backend.env)
			}
			for _, scenario := range scenarios {
				scenario := scenario
				t.Run(scenario.name, func(t *testing.T) {
					db := setupAccountPlayerNoTestDB(t, dsn)
					prepareAccountPlayerNoScenario(t, db, scenario)
					up := readEmbeddedMigration(t, accountV6UpPath)
					_, err := db.ExecContext(context.Background(), up)
					if scenario.wantError {
						if err == nil || !strings.Contains(err.Error(), "__pandora_player_no_reconcile_data_conflict__") {
							t.Fatalf("冲突库必须由语义化 guard fail-closed,err=%v", err)
						}
						t.Logf("fail-closed error: %v", err)
						assertAccountPlayerNoConflictUntouched(t, db, scenario)
						return
					}
					if err != nil {
						t.Fatalf("执行 000006: %v", err)
					}
					assertAccountPlayerNoCanonical(t, db, scenario)
					if _, err := db.ExecContext(context.Background(), up); err != nil {
						t.Fatalf("重复执行 000006 必须幂等: %v", err)
					}
					assertAccountPlayerNoCanonical(t, db, scenario)
				})
			}
		})
	}
}

func TestPandoraAccountV6PlayerNoPreflightRejectsShapeDriftAcrossBackends(t *testing.T) {
	testCases := []struct {
		name      string
		columns   string
		indexes   string
		seed      string
		counters  []string
		dataQuery string
		guard     string
	}{
		{
			name:      "signed_target_column",
			columns:   "`player_no` BIGINT NULL COMMENT 'must not be silently fixed'",
			indexes:   "UNIQUE KEY `uk_player_no` (`player_no`)",
			seed:      "INSERT INTO accounts (player_id, account, player_no) VALUES (1, 'signed-target', 7)",
			counters:  []string{targetCounterDDL, "INSERT INTO player_no_counter (id, next_no) VALUES (1, 20)"},
			dataQuery: "SELECT CONCAT(player_id, '|', account, '|', player_no) FROM accounts ORDER BY player_id",
			guard:     "__pandora_player_no_reconcile_column_shape_invalid__",
		},
		{
			name:      "non_unique_legacy_index",
			columns:   "`register_no` BIGINT UNSIGNED NULL COMMENT 'legacy'",
			indexes:   "KEY `uk_register_no` (`register_no`)",
			seed:      "INSERT INTO accounts (player_id, account, register_no) VALUES (1, 'bad-legacy-index', 7)",
			counters:  []string{legacyCounterDDL, "INSERT INTO register_no_counter (id, next_no) VALUES (1, 20)"},
			dataQuery: "SELECT CONCAT(player_id, '|', account, '|', register_no) FROM accounts ORDER BY player_id",
			guard:     "__pandora_player_no_reconcile_legacy_index_invalid__",
		},
		{
			name:      "nullable_target_counter",
			columns:   "`player_no` BIGINT UNSIGNED NULL COMMENT 'target'",
			indexes:   "UNIQUE KEY `uk_player_no` (`player_no`)",
			seed:      "INSERT INTO accounts (player_id, account, player_no) VALUES (1, 'bad-counter', 7)",
			counters:  []string{"CREATE TABLE player_no_counter (id TINYINT UNSIGNED NOT NULL, next_no BIGINT UNSIGNED NULL COMMENT 'bad', PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4", "INSERT INTO player_no_counter (id, next_no) VALUES (1, 20)"},
			dataQuery: "SELECT CONCAT(player_id, '|', account, '|', player_no) FROM accounts ORDER BY player_id",
			guard:     "__pandora_player_no_reconcile_counter_shape_invalid__",
		},
	}

	for _, backend := range []struct {
		name string
		env  string
	}{
		{name: "mysql", env: "PANDORA_TEST_MYSQL_DSN"},
		{name: "tidb", env: "PANDORA_TEST_TIDB_DSN"},
	} {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			dsn := strings.TrimSpace(os.Getenv(backend.env))
			if dsn == "" {
				t.Skipf("跳过真实 %s preflight 矩阵:未设 %s", backend.name, backend.env)
			}
			for _, testCase := range testCases {
				testCase := testCase
				t.Run(testCase.name, func(t *testing.T) {
					db := setupAccountPlayerNoTestDB(t, dsn)
					prepareAccountPlayerNoScenario(t, db, accountPlayerNoScenario{
						name: testCase.name, columns: testCase.columns, indexes: testCase.indexes,
						seed: testCase.seed, counters: testCase.counters,
					})
					before := snapshotAccountPlayerNoTestState(t, db, testCase.dataQuery)
					_, err := db.ExecContext(context.Background(), readEmbeddedMigration(t, accountV6UpPath))
					if err == nil || !strings.Contains(err.Error(), testCase.guard) {
						t.Fatalf("shape drift 必须由 %s fail-closed,err=%v", testCase.guard, err)
					}
					after := snapshotAccountPlayerNoTestState(t, db, testCase.dataQuery)
					if after != before {
						t.Fatalf("preflight guard 必须零写入\nbefore=%s\nafter =%s", before, after)
					}
				})
			}
		})
	}
}

const (
	targetCounterDDL = "CREATE TABLE player_no_counter (id TINYINT UNSIGNED NOT NULL, next_no BIGINT UNSIGNED NOT NULL COMMENT 'target', PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
	legacyCounterDDL = "CREATE TABLE register_no_counter (id TINYINT UNSIGNED NOT NULL, next_no BIGINT UNSIGNED NOT NULL COMMENT 'legacy', PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
)

func setupAccountPlayerNoTestDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("解析测试 DSN: %v", err)
	}
	adminCfg := *cfg
	adminCfg.DBName = ""
	admin, err := sql.Open("mysql", adminCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开管理连接: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("已提供测试 DSN 但数据库不可达: %v", err)
	}

	dbName := fmt.Sprintf("pandora_account_player_no_it_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+dbName+"` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		_ = admin.Close()
		t.Fatalf("创建迁移测试库: %v", err)
	}
	testCfg := *cfg
	testCfg.DBName = dbName
	testCfg.MultiStatements = true
	db, err := sql.Open("mysql", testCfg.FormatDSN())
	if err != nil {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + dbName + "`")
		_ = admin.Close()
		t.Fatalf("打开测试库: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
		_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + dbName + "`")
		_ = admin.Close()
	})
	return db
}

func prepareAccountPlayerNoScenario(t *testing.T, db *sql.DB, scenario accountPlayerNoScenario) {
	t.Helper()
	ddl := fmt.Sprintf(`CREATE TABLE accounts (
player_id BIGINT UNSIGNED NOT NULL,
account VARCHAR(64) NOT NULL,
status TINYINT UNSIGNED NOT NULL DEFAULT 0,
%s,
created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
PRIMARY KEY (player_id),
UNIQUE KEY uk_account (account),
%s
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, scenario.columns, scenario.indexes)
	statements := append([]string{ddl, scenario.seed}, scenario.counters...)
	for _, statement := range statements {
		if _, err := db.ExecContext(context.Background(), statement); err != nil {
			t.Fatalf("准备 %s: %v\nSQL: %s", scenario.name, err, statement)
		}
	}
}

func assertAccountPlayerNoCanonical(t *testing.T, db *sql.DB, scenario accountPlayerNoScenario) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for playerID, want := range scenario.wantNumbers {
		var got sql.NullInt64
		if err := db.QueryRowContext(ctx, "SELECT player_no FROM accounts WHERE player_id = ?", playerID).Scan(&got); err != nil {
			t.Fatalf("读取 player_no(%d): %v", playerID, err)
		}
		if got != want {
			t.Errorf("player_id=%d player_no=%v,期望=%v", playerID, got, want)
		}
	}
	var nextNo uint64
	if err := db.QueryRowContext(ctx, "SELECT next_no FROM player_no_counter WHERE id=1").Scan(&nextNo); err != nil {
		t.Fatalf("读取 player_no_counter: %v", err)
	}
	if nextNo != scenario.wantNextNo {
		t.Errorf("next_no=%d,期望=%d", nextNo, scenario.wantNextNo)
	}

	var playerComment, nextComment, tableComment string
	if err := db.QueryRowContext(ctx, `SELECT COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='accounts' AND COLUMN_NAME='player_no'`).Scan(&playerComment); err != nil {
		t.Fatalf("读取 player_no comment: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COLUMN_COMMENT FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='player_no_counter' AND COLUMN_NAME='next_no'`).Scan(&nextComment); err != nil {
		t.Fatalf("读取 next_no comment: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='player_no_counter'`).Scan(&tableComment); err != nil {
		t.Fatalf("读取 counter table comment: %v", err)
	}
	if playerComment != canonicalPlayerNoComment || nextComment != canonicalNextNoComment || tableComment != canonicalCounterComment {
		t.Errorf("canonical comments 漂移: player=%q next=%q table=%q", playerComment, nextComment, tableComment)
	}

	var oldCols, oldIndexes, oldTables, targetUnique int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='accounts' AND COLUMN_NAME='register_no'`).Scan(&oldCols); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='accounts' AND INDEX_NAME='uk_register_no'`).Scan(&oldIndexes); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='register_no_counter'`).Scan(&oldTables); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='accounts' AND INDEX_NAME='uk_player_no' AND NON_UNIQUE=0 AND COLUMN_NAME='player_no'`).Scan(&targetUnique); err != nil {
		t.Fatal(err)
	}
	if oldCols != 0 || oldIndexes != 0 || oldTables != 0 || targetUnique != 1 {
		t.Errorf("未完全收敛: oldCols=%d oldIndexes=%d oldTables=%d targetUnique=%d", oldCols, oldIndexes, oldTables, targetUnique)
	}
}

func assertAccountPlayerNoConflictUntouched(t *testing.T, db *sql.DB, scenario accountPlayerNoScenario) {
	t.Helper()
	var columns, indexes, counters int
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='accounts' AND COLUMN_NAME IN ('player_no','register_no')`).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='accounts' AND INDEX_NAME IN ('uk_player_no','uk_register_no')`).Scan(&indexes); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME IN ('player_no_counter','register_no_counter')`).Scan(&counters); err != nil {
		t.Fatal(err)
	}
	if columns != 2 || indexes != 2 || counters != 2 {
		t.Fatalf("冲突 guard 后不应删除 schema 对象,columns=%d indexes=%d counters=%d", columns, indexes, counters)
	}
	var playerNo, registerNo sql.NullInt64
	if err := db.QueryRow("SELECT player_no, register_no FROM accounts WHERE player_id=?", scenario.wantConflictPlayerID).Scan(&playerNo, &registerNo); err != nil {
		t.Fatal(err)
	}
	if playerNo != scenario.wantConflictPlayerNo || registerNo != scenario.wantConflictRegisterNo {
		t.Fatalf("冲突 guard 必须先于 merge,player_id=%d got=(%v,%v) want=(%v,%v)", scenario.wantConflictPlayerID, playerNo, registerNo, scenario.wantConflictPlayerNo, scenario.wantConflictRegisterNo)
	}
	var playerNext, registerNext uint64
	if err := db.QueryRow("SELECT next_no FROM player_no_counter WHERE id=1").Scan(&playerNext); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT next_no FROM register_no_counter WHERE id=1").Scan(&registerNext); err != nil {
		t.Fatal(err)
	}
	if playerNext != 400 || registerNext != 500 {
		t.Fatalf("冲突 guard 后不应合并计数器,player=%d register=%d", playerNext, registerNext)
	}
	t.Logf("guard 后 schema/data 未改: columns=%d indexes=%d counters=%d player_no=%v register_no=%v counter=(%d,%d)",
		columns, indexes, counters, playerNo, registerNo, playerNext, registerNext)
}

func snapshotAccountPlayerNoTestState(t *testing.T, db *sql.DB, dataQuery string) string {
	t.Helper()
	var parts []string
	existing := make(map[string]bool)
	for _, table := range []string{"accounts", "player_no_counter", "register_no_counter"} {
		var exists int
		if err := db.QueryRow(`SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?`, table).Scan(&exists); err != nil {
			t.Fatal(err)
		}
		if exists == 0 {
			continue
		}
		existing[table] = true
		var tableName, ddl string
		if err := db.QueryRow("SHOW CREATE TABLE `"+table+"`").Scan(&tableName, &ddl); err != nil {
			t.Fatalf("SHOW CREATE TABLE %s: %v", table, err)
		}
		parts = append(parts, tableName+"="+ddl)
	}
	rows, err := db.Query(dataQuery)
	if err != nil {
		t.Fatalf("snapshot data: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatal(err)
		}
		parts = append(parts, "row="+row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"player_no_counter", "register_no_counter"} {
		if !existing[table] {
			continue
		}
		var next sql.NullInt64
		if err := db.QueryRow("SELECT next_no FROM `" + table + "` WHERE id=1").Scan(&next); err != nil {
			t.Fatalf("snapshot %s rows: %v", table, err)
		}
		parts = append(parts, fmt.Sprintf("%s.next=%v", table, next))
	}
	return strings.Join(parts, "\n")
}
