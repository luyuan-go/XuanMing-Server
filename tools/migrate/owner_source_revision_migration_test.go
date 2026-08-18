package main

// owner_source_revision_migration_test.go — pandora_owner 迁移集的契约与真库验收。
//
// 这套迁移集是 2026-08-18 补的:pandora_owner 一直没有迁移集,而 owner 新版把
// `owner_record.hub_source_revision` 写进了 SELECT(INC-20260818-003)。存量库拿不到该列,
// owner 启动期 fail-fast 拒启(AssertSourceRevisionColumn)——「一键启动 owner exit 1」。
// dev_migrate.ps1 第 0 步重放 mysql-init 只能补新表(IF NOT EXISTS),补不了加列,所以
// 没有迁移集 = 这个库的列改动在存量库上**永远**不会自动进库。
//
// 本文件把这条链上「改一个字就再犯一次」的地方机械化。

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

const (
	ownerV1UpPath   = "migrations/pandora_owner/000001_baseline.up.sql"
	ownerV2UpPath   = "migrations/pandora_owner/000002_hub_source_revision.up.sql"
	ownerV2DownPath = "migrations/pandora_owner/000002_hub_source_revision.down.sql"

	// fresh-init 权威建表(容器 entrypoint 装载),不在 embed FS 里,按相对路径读。
	ownerMySQLInitPath = "../../deploy/mysql-init/15-owner-tables.sql"
	ownerTiDBInitPath  = "../../deploy/tidb-init/02-owner-tidb.sql"

	ownerSourceRevisionColumn = "hub_source_revision"
)

// ownerBaselineTables 是 000001 建的三张表。
var ownerBaselineTables = []string{"owner_record", "ds_instance_lease", "owner_transition_log"}

func TestPandoraOwnerV2SourceRevisionContract(t *testing.T) {
	version, err := latestMigrationVersion("pandora_owner")
	if err != nil {
		t.Fatalf("latestMigrationVersion: %v", err)
	}
	// 精确钉住 latest:本套迁移的「最新版契约」由本用例持有。加 v3 的人必须先来这里,
	// 把这条 pin 让给新用例(本用例降级成 `version < 3` 的下限),顺手复核下面每一条。
	if version != 2 {
		t.Fatalf("pandora_owner latest version=%d,期望=2", version)
	}

	// 一律先剥 `-- ` 行注释:注释里成段解释这些规则本身(含被禁字样)属正常。
	up := stripLineComments(readEmbeddedMigration(t, ownerV2UpPath))
	for _, fragment := range []string{
		// 条件加列:fresh-init(mysql-init / tidb-init)已建出该列时必须跳过,
		// 否则那种库上 duplicate column 直接把迁移打成 dirty。
		"information_schema.COLUMNS",
		"TABLE_NAME = 'owner_record'",
		"COLUMN_NAME = '" + ownerSourceRevisionColumn + "'",
		"PREPARE",
		"ADD COLUMN `hub_source_revision` BIGINT UNSIGNED NOT NULL DEFAULT 0",
		// 列位置与 fresh-init 一致,免得两条路径建出的表列序不同。
		"AFTER `admit_not_before_ms`",
	} {
		if !strings.Contains(up, fragment) {
			t.Errorf("000002 up 缺少来源版本 expand 契约片段 %q", fragment)
		}
	}

	// expand 纪律:本版只加不减(§9.16/§9.21)。
	upper := strings.ToUpper(up)
	for _, forbidden := range []string{"DROP COLUMN", "DROP TABLE", "DROP INDEX", "RENAME"} {
		if strings.Contains(upper, forbidden) {
			t.Errorf("000002 是 expand,不许出现 %q", forbidden)
		}
	}

	// 生产 owner 权威库在 TiDB(§9.22),TiDB 走自己的 online DDL,不吃 MySQL 的
	// ALGORITHM 语义;同在 TiDB 的 pandora_account 加列迁移也一律不带该子句。
	if strings.Contains(upper, "ALGORITHM") {
		t.Error("000002 不该带 ALGORITHM 子句:生产 owner 在 TiDB,该子句只对 MySQL 有意义")
	}

	// 绝不回填。DEFAULT 0 就是 legacy 哨兵(该玩家还没被带版本的写者服务过);
	// 一旦回填成非 0,「见过非零版本就永久拒 legacy」当场对全部存量玩家生效,
	// 仍在跑的旧 hub_allocator 会被整体拒掉 = 大厅分配停摆。
	if strings.Contains(upper, "UPDATE `OWNER_RECORD`") {
		t.Error("000002 不许回填 hub_source_revision:非 0 水位会把兼容窗直接关死")
	}

	// down 只碰本次这一列。多删一个对象,「回滚」就变成 owner 权威数据事故。
	down := stripLineComments(readEmbeddedMigration(t, ownerV2DownPath))
	if !strings.Contains(down, "DROP COLUMN `hub_source_revision`") {
		t.Error("000002 down 必须删掉本次新增的列")
	}
	for _, forbidden := range []string{"owner_epoch", "ds_instance_lease", "owner_transition_log", "DROP TABLE"} {
		if strings.Contains(down, forbidden) {
			t.Errorf("000002 down 越界碰到 %q:回滚只允许删 hub_source_revision 一列", forbidden)
		}
	}
	// 回滚也必须条件化,重复执行不炸。
	if !strings.Contains(down, "information_schema.COLUMNS") || !strings.Contains(down, "PREPARE") {
		t.Error("000002 down 必须条件化(列不存在时空跑),否则回滚不可重跑")
	}
}

// TestPandoraOwnerBaselineIsPreExpandShape 钉住 baseline 与 000002 的分工。
//
// baseline 是**建立迁移集那一刻的历史形态**,不含 hub_source_revision —— 存量库(卷早就
// 建好、mysql-init 不会重放)正是靠 000002 才拿到该列。要是有人把该列塞进 baseline 再把
// 000002 删掉,fresh 库看着一切正常,存量库则永远缺列:owner 启动即 exit 1 的老毛病原样复发。
func TestPandoraOwnerBaselineIsPreExpandShape(t *testing.T) {
	baseline := stripLineComments(readEmbeddedMigration(t, ownerV1UpPath))
	if strings.Contains(baseline, ownerSourceRevisionColumn) {
		t.Error("000001 baseline 不该含 hub_source_revision:该列的唯一来源是 000002 的条件加列")
	}
	for _, table := range ownerBaselineTables {
		if !strings.Contains(baseline, "CREATE TABLE IF NOT EXISTS `"+table+"`") {
			t.Errorf("000001 baseline 缺表 %q", table)
		}
	}
	// baseline 必须整套 IF NOT EXISTS:mysql-init / tidb-init 已建好的库跑到这里要空跑。
	if strings.Count(baseline, "CREATE TABLE IF NOT EXISTS") != len(ownerBaselineTables) {
		t.Error("000001 baseline 的建表必须全部是 CREATE TABLE IF NOT EXISTS")
	}
	// 迁移器连的就是目标库;baseline 里出现 USE 会让它写到别的库去。
	if strings.Contains(strings.ToUpper(baseline), "\nUSE ") {
		t.Error("000001 baseline 不许含 USE 语句(库由 targets 清单锁定)")
	}
}

// TestPandoraOwnerFreshInitMatchesV2 钉住 fresh-init 与迁移产物的一致性。
//
// 两条路径必须落到同一套对象:全新集群跑 deploy/*-init,存量库跑 000002。任一边漏掉该列,
// owner 启动期的 AssertSourceRevisionColumn 就 fail-fast 拒启。
//
// ⚠️ tidb-init 尤其容易漏:生产 owner 库在 TiDB(§9.22),日常联调只装 mysql-init,
// 本地全绿也发现不了 TiDB 那份的漂移(2026-08-18 account 就漏过整张 account_roles)。
func TestPandoraOwnerFreshInitMatchesV2(t *testing.T) {
	for path, content := range map[string]string{
		"deploy/mysql-init/15-owner-tables.sql": readRepoFile(t, ownerMySQLInitPath),
		"deploy/tidb-init/02-owner-tidb.sql":    readRepoFile(t, ownerTiDBInitPath),
	} {
		collapsed := collapseSpaces(content)
		if !strings.Contains(collapsed, "`hub_source_revision` BIGINT UNSIGNED NOT NULL DEFAULT 0") {
			t.Errorf("%s 与 000002 canonical schema 漂移:缺 hub_source_revision 列定义", path)
		}
		// 列必须 NOT NULL DEFAULT 0:0 是 legacy 哨兵,NULL 会让判定矩阵多出一种未定义态。
		if strings.Contains(collapsed, "`hub_source_revision` BIGINT UNSIGNED NULL") {
			t.Errorf("%s 把 hub_source_revision 写成可空:0 是 legacy 哨兵,不能有 NULL 态", path)
		}
	}
}

// TestPandoraOwnerMigratesToLatestAcrossBackends 覆盖三条真实路径(默认 skip,
// 设 PANDORA_TEST_MYSQL_DSN / PANDORA_TEST_TIDB_DSN 才跑):
//
//	fresh_empty       空库直接迁移(全新环境)
//	fresh_init_schema 先跑 deploy/mysql-init/15 建表再迁移(docker-init 形态,000002 必须跳过)
//	legacy_v1         存量库停在 baseline 形态且有 owner 记录,增量升到 latest 且数据不动
//
// 每条路径都跑两遍:第二遍必须仍是 clean latest=2(重复迁移安全)。
func TestPandoraOwnerMigratesToLatestAcrossBackends(t *testing.T) {
	scenarios := []struct {
		name    string
		prepare func(t *testing.T, ctx context.Context, db *sql.DB, target migrationTarget) func(*testing.T)
	}{
		{
			name: "fresh_empty",
			prepare: func(*testing.T, context.Context, *sql.DB, migrationTarget) func(*testing.T) {
				return nil
			},
		},
		{
			name: "fresh_init_schema",
			prepare: func(t *testing.T, ctx context.Context, db *sql.DB, _ migrationTarget) func(*testing.T) {
				t.Helper()
				if _, err := db.ExecContext(ctx, stripUseStatements(readRepoFile(t, ownerMySQLInitPath))); err != nil {
					t.Fatalf("执行 fresh-init: %v", err)
				}
				return nil
			},
		},
		{
			name:    "legacy_v1",
			prepare: prepareOwnerLegacyV1,
		},
	}
	forEachPlayerBackend(t, func(t *testing.T, dsn string, isTiDB bool) {
		for _, scenario := range scenarios {
			scenario := scenario
			t.Run(scenario.name, func(t *testing.T) {
				target, db := setupOwnerMigrationDB(t, dsn, isTiDB)
				ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
				defer cancel()
				verify := scenario.prepare(t, ctx, db, target)

				for _, pass := range []string{"首跑", "重跑"} {
					if err := migrateTarget(target, false); err != nil {
						t.Fatalf("migrateTarget %s: %v", pass, err)
					}
					assertPlayerMigrationVersion(t, ctx, db, 2)
					assertOwnerSourceRevisionColumn(t, ctx, db)
					if verify != nil {
						verify(t)
					}
				}
			})
		}
	})
}

// prepareOwnerLegacyV1 复现存量库:停在 baseline 形态(没有 hub_source_revision)且已有
// owner 记录。返回的断言保证增量升级只加列、一行业务数据都不动,新列取哨兵值 0。
func prepareOwnerLegacyV1(t *testing.T, ctx context.Context, db *sql.DB, target migrationTarget) func(*testing.T) {
	t.Helper()
	// newPlayerMigrator 与 migration set 无关(按 target.MigrationSet 加载),这里复用。
	migrator := newPlayerMigrator(t, target)
	if err := migrator.Migrate(1); err != nil {
		t.Fatalf("预置存量库到 v1: %v", err)
	}
	if err := closeMigration(migrator); err != nil {
		t.Fatalf("关闭 v1 预置迁移器: %v", err)
	}
	assertPlayerMigrationVersion(t, ctx, db, 1)

	var columns int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.COLUMNS
		  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'owner_record' AND COLUMN_NAME = ?`,
		ownerSourceRevisionColumn).Scan(&columns); err != nil {
		t.Fatalf("探测 v1 形态: %v", err)
	}
	if columns != 0 {
		t.Fatal("baseline 形态不该有 hub_source_revision,否则本场景验证不到加列路径")
	}

	if _, err := db.ExecContext(ctx,
		"INSERT INTO `owner_record` (`player_id`, `owner_epoch`, `owner_type`, `phase`, `pod_name`,"+
			" `instance_uid`, `instance_epoch`, `assignment_or_allocation_id`, `release_track`,"+
			" `operation_id`, `admit_not_before_ms`, `updated_at_ms`)"+
			" VALUES (10001, 7, 1, 2, 'hub-0', 'uid-abc', 3, 'assign-1', 'stable', 'op-1', 1755500000000, 1755500000001)",
	); err != nil {
		t.Fatalf("写入存量 owner 记录: %v", err)
	}
	if _, err := db.ExecContext(ctx,
		"INSERT INTO `ds_instance_lease` (`instance_uid`, `pod_name`, `instance_epoch`, `release_track`,"+
			" `lease_deadline_ms`, `updated_at_ms`) VALUES ('uid-abc', 'hub-0', 3, 'stable', 1755500020000, 1755500000001)",
	); err != nil {
		t.Fatalf("写入存量实例租约: %v", err)
	}

	return func(t *testing.T) {
		t.Helper()
		var epoch, revision, deadline uint64
		var ownerType, phase int
		var assignment string
		if err := db.QueryRowContext(ctx,
			"SELECT `owner_epoch`, `owner_type`, `phase`, `assignment_or_allocation_id`, `hub_source_revision`"+
				" FROM `owner_record` WHERE `player_id` = 10001",
		).Scan(&epoch, &ownerType, &phase, &assignment, &revision); err != nil {
			t.Fatalf("回读存量 owner 记录: %v", err)
		}
		if epoch != 7 || ownerType != 1 || phase != 2 || assignment != "assign-1" {
			t.Fatalf("存量 owner 记录被改动: epoch=%d type=%d phase=%d assignment=%q",
				epoch, ownerType, phase, assignment)
		}
		// 存量玩家必须落在 legacy 哨兵 0 上:兼容窗靠它放行旧 hub_allocator。
		if revision != 0 {
			t.Fatalf("存量行的 hub_source_revision=%d,期望 0(legacy 哨兵)", revision)
		}
		if err := db.QueryRowContext(ctx,
			"SELECT `lease_deadline_ms` FROM `ds_instance_lease` WHERE `instance_uid` = 'uid-abc'",
		).Scan(&deadline); err != nil {
			t.Fatalf("回读存量实例租约: %v", err)
		}
		if deadline != 1755500020000 {
			t.Fatalf("存量实例租约被改动: deadline=%d", deadline)
		}
	}
}

// assertOwnerSourceRevisionColumn 用 owner 启动期同一条判据(information_schema 探测)
// 验收迁移产物 —— 迁移跑完 = owner 能起来。
func assertOwnerSourceRevisionColumn(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var dataType, isNullable, columnDefault string
	err := db.QueryRowContext(ctx,
		`SELECT COLUMN_TYPE, IS_NULLABLE, COALESCE(COLUMN_DEFAULT, '')
		   FROM information_schema.COLUMNS
		  WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'owner_record' AND COLUMN_NAME = ?`,
		ownerSourceRevisionColumn).Scan(&dataType, &isNullable, &columnDefault)
	if err == sql.ErrNoRows {
		t.Fatal("迁移到 latest 后 owner_record 仍缺 hub_source_revision:owner 会启动即 exit 1")
	}
	if err != nil {
		t.Fatalf("探测 hub_source_revision: %v", err)
	}
	if !strings.EqualFold(dataType, "bigint unsigned") && !strings.EqualFold(dataType, "bigint(20) unsigned") {
		t.Errorf("hub_source_revision 类型=%q,期望 bigint unsigned", dataType)
	}
	if !strings.EqualFold(isNullable, "NO") {
		t.Errorf("hub_source_revision 必须 NOT NULL,实际 IS_NULLABLE=%q", isNullable)
	}
	if columnDefault != "0" {
		t.Errorf("hub_source_revision 默认值=%q,期望 0(legacy 哨兵)", columnDefault)
	}
}

// setupOwnerMigrationDB 建一个一次性库并返回指向它的迁移目标(t.Cleanup 负责删库)。
func setupOwnerMigrationDB(t *testing.T, dsn string, isTiDB bool) (migrationTarget, *sql.DB) {
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("已提供测试 DSN 但无法连接: %v", err)
	}

	dbName := fmt.Sprintf("pandora_owner_mig_it_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx,
		"CREATE DATABASE `"+dbName+"` DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		_ = admin.Close()
		t.Fatalf("创建迁移测试库: %v", err)
	}
	var db *sql.DB
	t.Cleanup(func() {
		if db != nil {
			_ = db.Close()
		}
		_, _ = admin.Exec("DROP DATABASE IF EXISTS `" + dbName + "`")
		_ = admin.Close()
	})

	testCfg := *cfg
	testCfg.DBName = dbName
	testCfg.MultiStatements = true
	if isTiDB {
		if testCfg.Params == nil {
			testCfg.Params = make(map[string]string)
		}
		// golang-migrate/mysql 以 SERIALIZABLE 开事务;TiDB 需显式接受并降级为其支持的
		// 悲观隔离语义,生产 owner 的 migration DSN 也必须带同一 session 参数。
		testCfg.Params["tidb_skip_isolation_level_check"] = "1"
	}
	db, err = sql.Open("mysql", testCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开迁移测试库: %v", err)
	}

	dsnPath := filepath.Join(t.TempDir(), "owner.dsn")
	if err := os.WriteFile(dsnPath, []byte(testCfg.FormatDSN()), 0o600); err != nil {
		t.Fatalf("写测试 DSN 文件: %v", err)
	}
	version, err := latestMigrationVersion("pandora_owner")
	if err != nil {
		t.Fatalf("读取 pandora_owner latest version: %v", err)
	}
	return migrationTarget{
		Name:                     "owner-it",
		MigrationSet:             "pandora_owner",
		Database:                 dbName,
		DSNFile:                  dsnPath,
		TimeoutSeconds:           120,
		LockWaitTimeoutSeconds:   10,
		expectedMigrationVersion: version,
	}, db
}
