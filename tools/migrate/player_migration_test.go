package main

// player_migration_test.go — pandora_player 000005/000006 的迁移契约与真库验收。
//
// 分工:000002..000006 的**静态片段断言**历史上落在
// battle_recovery_migration_test.go 的 TestPandoraPlayerExperienceMigrationIsInitSafe
// (含 latest=6 版本门禁),这里主要补真库路径与 fresh-init 漂移断言。
//
//	① 000005 独有的两条静态契约:清理索引的条件补齐守卫、down 只碰本次新增的三张表;
//	② **真库**验收 —— fresh-init / 存量升级 / 重复迁移 / 回滚,四条路径各自跑通。
//
// 真库用例默认 skip,设 PANDORA_TEST_MYSQL_DSN(和可选 PANDORA_TEST_TIDB_DSN)才跑。
// 静态用例永远跑,是 CI 里能拦住漂移的那一层。

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
	"github.com/golang-migrate/migrate/v4"
	migratemysql "github.com/golang-migrate/migrate/v4/database/mysql"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

const (
	playerV5UpPath   = "migrations/pandora_player/000005_skill_cards.up.sql"
	playerV5DownPath = "migrations/pandora_player/000005_skill_cards.down.sql"

	// fresh-init 权威建表(容器 entrypoint 装载),不在 embed FS 里,按相对路径读。
	playerFreshInitPath  = "../../deploy/mysql-init/04-player-tables.sql"
	playerRewardInitPath = "../../deploy/mysql-init/13-reward-claim-tables.sql"
)

// skillCardTables 是 000005 新增的三张表(顺序 = down 里的删除顺序)。
var skillCardTables = []string{"skill_card_grants", "player_skill_slots", "player_skill_cards"}

// TestPandoraPlayerV5CleanupIndexIsGuaranteed 钉住 §9.24 的清理索引不能只靠"建表那次建对了"。
//
// 建表用的是 `CREATE TABLE IF NOT EXISTS`,对"表已存在但形态不同"是**静默 no-op**:
// 一个先前从别处拿到过 skill_card_grants(缺 idx_created)的库,会带着缺索引的表进 v5,
// 而 dbcheck 把这条索引当发布门禁项。所以 up 必须另有一段条件补齐,且 fresh-init 权威定义
// 也必须自带同一条索引 —— 两边任一处漂移,这里就红。
func TestPandoraPlayerV5CleanupIndexIsGuaranteed(t *testing.T) {
	up := readEmbeddedMigration(t, playerV5UpPath)
	for _, fragment := range []string{
		"information_schema.STATISTICS",    // 现查索引是否存在
		"TABLE_NAME = 'skill_card_grants'", // 查的是这张表
		"INDEX_NAME = 'idx_created'",       // 查的是这条索引
		"ALTER TABLE `skill_card_grants` ADD KEY `idx_created` (`created_at`), ALGORITHM=INPLACE",
		"PREPARE",
	} {
		if !strings.Contains(up, fragment) {
			t.Errorf("000005 up 缺少清理索引条件补齐片段 %q", fragment)
		}
	}

	// fresh-init 权威定义必须与迁移建出同样的结构。两边漂移是本仓踩过的坑
	// (同一张表在 init 和 migration 里长得不一样,只有其中一条路径的库会被 dbcheck 拦下)。
	freshInit := readRepoFile(t, playerFreshInitPath)
	for _, fragment := range []string{
		"`instance_id`     BIGINT UNSIGNED  NULL",
		"UNIQUE KEY `uk_player_instance` (`player_id`, `instance_id`)",
		"CREATE TABLE IF NOT EXISTS `player_skill_cards`",
		"CREATE TABLE IF NOT EXISTS `player_skill_slots`",
		"CREATE TABLE IF NOT EXISTS `skill_card_grants`",
		"UNIQUE KEY `uk_player_card` (`player_id`, `card_id`)",
		"UNIQUE KEY `uk_player_card_once` (`player_id`, `card_id`)",
		"UNIQUE KEY `uk_player_key` (`player_id`, `idempotency_key`)",
		"KEY `idx_created` (`created_at`)",
	} {
		if !strings.Contains(freshInit, fragment) {
			t.Errorf("deploy/mysql-init/04-player-tables.sql 缺少片段 %q(与 000005 漂移)", fragment)
		}
	}
}

// TestPandoraPlayerV5DownTouchesOnlyNewTables 是回滚风险的机械闸。
//
// 000005 的 down **不是** no-op(它建的是全新表,回滚就该收走,与 000002 同类;
// 000003 那种给既有表补索引的才必须 no-op)。既然允许它删东西,就必须逐条钉死
// 它只能删本次新增的三张表:任何 ALTER / DROP COLUMN / DROP INDEX / DELETE / UPDATE /
// TRUNCATE,或者删到 players、exp_history 这类既有表头上,都会让"回滚"变成数据事故。
func TestPandoraPlayerV5DownTouchesOnlyNewTables(t *testing.T) {
	statements := executableStatements(readEmbeddedMigration(t, playerV5DownPath))
	if len(statements) != len(skillCardTables) {
		t.Fatalf("000005 down 有 %d 条可执行语句,期望恰好 %d 条 DROP TABLE:%v",
			len(statements), len(skillCardTables), statements)
	}
	allowed := make(map[string]bool, len(skillCardTables))
	for _, table := range skillCardTables {
		allowed["DROP TABLE IF EXISTS `"+table+"`"] = true
	}
	for _, statement := range statements {
		if !allowed[statement] {
			t.Errorf("000005 down 出现越界语句 %q:回滚只允许删本次新增的三张表 %v",
				statement, skillCardTables)
		}
	}
}

func TestPandoraPlayerV6DownTouchesOnlyInstanceProjection(t *testing.T) {
	statements := executableStatements(readEmbeddedMigration(t,
		"migrations/pandora_player/000006_equipment_instance_id.down.sql"))
	want := map[string]bool{
		"ALTER TABLE `player_equipment` DROP INDEX `uk_player_instance`, ALGORITHM=INPLACE": true,
		"ALTER TABLE `player_equipment` DROP COLUMN `instance_id`, ALGORITHM=INSTANT":       true,
	}
	if len(statements) != len(want) {
		t.Fatalf("000006 down 语句=%v,期望恰好删除本次索引与列", statements)
	}
	for _, statement := range statements {
		if !want[statement] {
			t.Errorf("000006 down 出现越界语句 %q", statement)
		}
	}
}

// executableStatements 去掉注释与空行后按分号切出语句(只用于静态断言,不求 SQL 解析完备)。
func executableStatements(sqlText string) []string {
	var out []string
	for _, statement := range strings.Split(stripLineComments(sqlText), ";") {
		if collapsed := collapseSpaces(statement); collapsed != "" {
			out = append(out, collapsed)
		}
	}
	return out
}

func readRepoFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(path))
	if err != nil {
		t.Fatalf("读取 %s: %v", path, err)
	}
	return string(b)
}

// ── 真库验收 ────────────────────────────────────────────────────────────────

// playerMigrationScenario 是一种"迁移前的库长什么样"。
//
// prepare 把库摆成该形态,并返回"迁移后必须仍然成立"的断言(没有额外断言时返回 nil)。
// 用返回值而不是 t.Cleanup:断言要在迁移跑完的当场执行,不该藏在清理阶段。
type playerMigrationScenario struct {
	name    string
	prepare func(t *testing.T, ctx context.Context, db *sql.DB, target migrationTarget) func(*testing.T)
}

// TestPandoraPlayerMigratesToLatestAcrossBackends 覆盖四条真实路径:
//
//	fresh_empty          空库直接迁移(全新环境)
//	fresh_init_schema    先跑 deploy/mysql-init 建表再迁移(docker-init 形态,迁移必须幂等)
//	legacy_v4            存量库停在 v4 且有业务数据,增量升到 latest(数据一行不能动)
//	legacy_missing_index 存量库已有 skill_card_grants 但缺 idx_created(条件补齐守卫必须补上)
//
// 每条路径都跑**两遍** migrateTarget:第二遍必须仍是 clean latest=6(重复迁移安全)。
func TestPandoraPlayerMigratesToLatestAcrossBackends(t *testing.T) {
	scenarios := []playerMigrationScenario{
		{
			name: "fresh_empty",
			prepare: func(*testing.T, context.Context, *sql.DB, migrationTarget) func(*testing.T) {
				return nil
			},
		},
		{name: "fresh_init_schema", prepare: preparePlayerFreshInitSchema},
		{name: "legacy_v4", prepare: preparePlayerLegacyV4},
		{name: "legacy_missing_index", prepare: preparePlayerLegacyMissingIndex},
	}
	forEachPlayerBackend(t, func(t *testing.T, dsn string, isTiDB bool) {
		for _, scenario := range scenarios {
			scenario := scenario
			t.Run(scenario.name, func(t *testing.T) {
				target, db := setupPlayerMigrationDB(t, dsn, isTiDB)
				ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
				defer cancel()
				verify := scenario.prepare(t, ctx, db, target)

				if err := migrateTarget(target, false); err != nil {
					t.Fatalf("migrateTarget 首跑: %v", err)
				}
				assertPlayerSkillCardSchema(t, ctx, db)
				assertPlayerMigrationVersion(t, ctx, db, 6)
				assertPlayerEquipmentInstanceSchema(t, ctx, db)
				if verify != nil {
					verify(t)
				}

				// 重复迁移:已到终版的库必须原地 no-op,不得把 backfill 再做一遍、
				// 也不得留下 dirty。
				if err := migrateTarget(target, false); err != nil {
					t.Fatalf("migrateTarget 重跑: %v", err)
				}
				assertPlayerSkillCardSchema(t, ctx, db)
				assertPlayerMigrationVersion(t, ctx, db, 6)
				assertPlayerEquipmentInstanceSchema(t, ctx, db)
				if verify != nil {
					verify(t)
				}
			})
		}
	})
}

// TestPandoraPlayerV5RollbackKeepsLegacyData 验收回滚风险的实际后果:
// down 之后三张技能卡表消失(数据不可恢复,这是设计上接受的代价),
// 但**既有表的数据必须一行不少**;重新 up 结构完整回来(数据不会回来)。
func TestPandoraPlayerV5RollbackKeepsLegacyData(t *testing.T) {
	forEachPlayerBackend(t, func(t *testing.T, dsn string, isTiDB bool) {
		target, db := setupPlayerMigrationDB(t, dsn, isTiDB)
		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		upToV5 := newPlayerMigrator(t, target)
		if err := upToV5.Migrate(5); err != nil {
			t.Fatalf("迁移到 000005: %v", err)
		}
		if err := closeMigration(upToV5); err != nil {
			t.Fatalf("关闭 v5 迁移器: %v", err)
		}
		seedPlayerLegacyRows(t, ctx, db)
		if _, err := db.ExecContext(ctx,
			"INSERT INTO player_skill_cards (player_id, card_id, level, shards) VALUES (7001, 3001, 2, 15)"); err != nil {
			t.Fatalf("写入技能卡持有行: %v", err)
		}

		migrator := newPlayerMigrator(t, target)
		if err := migrator.Steps(-1); err != nil {
			t.Fatalf("回滚 000005: %v", err)
		}
		if err := closeMigration(migrator); err != nil {
			t.Fatalf("关闭回滚迁移器: %v", err)
		}

		assertPlayerMigrationVersion(t, ctx, db, 4)
		for _, table := range skillCardTables {
			if tableExists(t, ctx, db, table) {
				t.Errorf("回滚后 %s 仍存在,down 没生效", table)
			}
		}
		assertPlayerLegacyRowsIntact(t, ctx, db)

		// 再 up 回去:结构必须完整重建(dbcheck 的"登记表缺失"才能重新变绿)。
		reupV5 := newPlayerMigrator(t, target)
		if err := reupV5.Migrate(5); err != nil {
			t.Fatalf("回滚后重新 up 到 v5: %v", err)
		}
		if err := closeMigration(reupV5); err != nil {
			t.Fatalf("关闭重建 v5 迁移器: %v", err)
		}
		assertPlayerSkillCardSchema(t, ctx, db)
		assertPlayerMigrationVersion(t, ctx, db, 5)
		assertPlayerLegacyRowsIntact(t, ctx, db)

		var cards int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM player_skill_cards").Scan(&cards); err != nil {
			t.Fatalf("读取回滚后重建的持卡表: %v", err)
		}
		if cards != 0 {
			t.Fatalf("回滚重建后 player_skill_cards 有 %d 行,期望 0(回滚丢数据是已知代价,不该凭空回来)", cards)
		}
	})
}

func forEachPlayerBackend(t *testing.T, run func(t *testing.T, dsn string, isTiDB bool)) {
	t.Helper()
	backends := []struct {
		name string
		env  string
		tidb bool
	}{
		{name: "mysql", env: "PANDORA_TEST_MYSQL_DSN"},
		{name: "tidb", env: "PANDORA_TEST_TIDB_DSN", tidb: true},
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			dsn := os.Getenv(backend.env)
			if dsn == "" {
				t.Skipf("跳过真实 %s 迁移测试:未设 %s", backend.name, backend.env)
			}
			run(t, dsn, backend.tidb)
		})
	}
}

// setupPlayerMigrationDB 建一个一次性库并返回指向它的迁移目标(t.Cleanup 负责删库)。
func setupPlayerMigrationDB(t *testing.T, dsn string, isTiDB bool) (migrationTarget, *sql.DB) {
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

	dbName := fmt.Sprintf("pandora_player_mig_it_%d", time.Now().UnixNano())
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
		// 悲观隔离语义,生产 TiDB migration DSN 也必须带同一 session 参数。
		testCfg.Params["tidb_skip_isolation_level_check"] = "1"
	}
	db, err = sql.Open("mysql", testCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开迁移测试库: %v", err)
	}

	dsnPath := filepath.Join(t.TempDir(), "player.dsn")
	if err := os.WriteFile(dsnPath, []byte(testCfg.FormatDSN()), 0o600); err != nil {
		t.Fatalf("写测试 DSN 文件: %v", err)
	}
	version, err := latestMigrationVersion("pandora_player")
	if err != nil {
		t.Fatalf("读取 pandora_player latest version: %v", err)
	}
	return migrationTarget{
		Name:                     "player-it",
		MigrationSet:             "pandora_player",
		Database:                 dbName,
		DSNFile:                  dsnPath,
		TimeoutSeconds:           120,
		LockWaitTimeoutSeconds:   10,
		expectedMigrationVersion: version,
	}, db
}

// newPlayerMigrator 按 migrateTarget 的同一套参数构造迁移器,供需要 Down/Steps 的用例使用
// (migrateTarget 自身只做 Up,且刻意不暴露回滚 —— 发布链路不该有回滚按钮)。
func newPlayerMigrator(t *testing.T, target migrationTarget) *migrate.Migrate {
	t.Helper()
	cfg, err := readAndHardenDSN(target.DSNFile, target, false)
	if err != nil {
		t.Fatalf("读取测试 DSN: %v", err)
	}
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开回滚连接: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	driver, err := migratemysql.WithInstance(db, &migratemysql.Config{
		MigrationsTable:  schemaMigrationsTable,
		DatabaseName:     target.Database,
		StatementTimeout: statementTimeout(target),
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("构造回滚 driver: %v", err)
	}
	source, err := iofs.New(migrationsFS, "migrations/"+target.MigrationSet)
	if err != nil {
		_ = db.Close()
		t.Fatalf("加载迁移集: %v", err)
	}
	m, err := migrate.NewWithInstance("iofs", source, target.Name, driver)
	if err != nil {
		_ = db.Close()
		t.Fatalf("构造回滚迁移器: %v", err)
	}
	m.LockTimeout = advisoryLockTimeout(target)
	return m
}

// preparePlayerFreshInitSchema 复现 docker-init 形态:容器 entrypoint 已经把
// deploy/mysql-init 的建表脚本跑完(表全在、列全在、索引全在),迁移随后才纳管该库。
// 这条路径上 000002/000004 的条件加列必须跳过、000001/000005 的建表必须 no-op。
func preparePlayerFreshInitSchema(t *testing.T, ctx context.Context, db *sql.DB, _ migrationTarget) func(*testing.T) {
	t.Helper()
	for _, path := range []string{playerFreshInitPath, playerRewardInitPath} {
		if _, err := db.ExecContext(ctx, stripUseStatements(readRepoFile(t, path))); err != nil {
			t.Fatalf("执行 fresh-init %s: %v", path, err)
		}
	}
	return nil
}

// preparePlayerLegacyV4 复现存量库:停在 v4(golang-migrate 记账也停在 4)且有业务数据。
// 返回的断言保证增量升级与重复迁移都没动这些行。
func preparePlayerLegacyV4(t *testing.T, ctx context.Context, db *sql.DB, target migrationTarget) func(*testing.T) {
	t.Helper()
	migrator := newPlayerMigrator(t, target)
	if err := migrator.Migrate(4); err != nil {
		t.Fatalf("预置存量库到 v4: %v", err)
	}
	if err := closeMigration(migrator); err != nil {
		t.Fatalf("关闭 v4 预置迁移器: %v", err)
	}
	assertPlayerMigrationVersion(t, ctx, db, 4)
	seedPlayerLegacyRows(t, ctx, db)
	return func(t *testing.T) { assertPlayerLegacyRowsIntact(t, ctx, db) }
}

// preparePlayerLegacyMissingIndex 复现"表已存在但缺清理索引"的库:
// `CREATE TABLE IF NOT EXISTS` 会静默跳过这张表,只有 up 里的条件补齐能救它。
func preparePlayerLegacyMissingIndex(t *testing.T, ctx context.Context, db *sql.DB, target migrationTarget) func(*testing.T) {
	t.Helper()
	verify := preparePlayerLegacyV4(t, ctx, db, target)
	if _, err := db.ExecContext(ctx, "CREATE TABLE `skill_card_grants` ("+
		"`id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"+
		"`player_id` BIGINT UNSIGNED NOT NULL,"+
		"`idempotency_key` VARCHAR(128) NOT NULL,"+
		"`created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"+
		"PRIMARY KEY (`id`),"+
		"UNIQUE KEY `uk_player_key` (`player_id`, `idempotency_key`),"+
		"KEY `idx_player` (`player_id`)"+
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci"); err != nil {
		t.Fatalf("预置缺索引的 skill_card_grants: %v", err)
	}
	if indexColumns(t, ctx, db, "skill_card_grants", "idx_created") != nil {
		t.Fatal("预置失败:这张表本该缺 idx_created")
	}
	return verify
}

// stripUseStatements 去掉 mysql-init 脚本里的 `USE \`db\`;`(测试 DSN 已经指定了库,
// 而且那个库名是一次性的,不能真的切过去)。
func stripUseStatements(sqlText string) string {
	lines := strings.Split(sqlText, "\n")
	kept := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(line)), "USE ") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.Join(kept, "\n")
}

func seedPlayerLegacyRows(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(ctx, `
INSERT INTO players (player_id, nickname, level, mmr, total_talent_points) VALUES
  (7001, 'Player_7001', 12, 1620, 11),
  (7002, 'Player_7002', 3, 1480, 2);
INSERT INTO player_talents (player_id, talent_id, level, spent_points) VALUES
  (7001, 101, 3, 3),
  (7001, 102, 2, 2);
INSERT INTO talent_point_grants (player_id, idempotency_key, points) VALUES
  (7001, 'level_up:12', 1);
INSERT INTO player_equipment (player_id, slot, item_config_id) VALUES
  (7001, 1, 10003);`); err != nil {
		t.Fatalf("写入存量业务数据: %v", err)
	}
}

// assertPlayerLegacyRowsIntact 断言升级 / 回滚都没动既有业务数据。
// 特别盯住 spent_points:000004 的回填是 `WHERE spent_points = 0` 的条件更新,
// 重复迁移时若条件写漏就会把已回填的行再改一次。
func assertPlayerLegacyRowsIntact(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var players int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM players").Scan(&players); err != nil {
		t.Fatalf("读取 players: %v", err)
	}
	if players != 2 {
		t.Errorf("players 行数=%d, 期望 2", players)
	}
	want := map[uint32][2]int{101: {3, 3}, 102: {2, 2}} // talent_id → (level, spent_points)
	rows, err := db.QueryContext(ctx,
		"SELECT talent_id, level, spent_points FROM player_talents WHERE player_id = 7001 ORDER BY talent_id")
	if err != nil {
		t.Fatalf("读取 player_talents: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var talentID uint32
		var level, spent int
		if err := rows.Scan(&talentID, &level, &spent); err != nil {
			t.Fatalf("扫描 player_talents: %v", err)
		}
		expect, ok := want[talentID]
		if !ok {
			t.Errorf("player_talents 出现意外行 talent_id=%d", talentID)
			continue
		}
		if level != expect[0] || spent != expect[1] {
			t.Errorf("talent_id=%d level=%d spent_points=%d, 期望 %d/%d", talentID, level, spent, expect[0], expect[1])
		}
		delete(want, talentID)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历 player_talents: %v", err)
	}
	if len(want) != 0 {
		t.Errorf("player_talents 缺行: %v", want)
	}
	var slot, itemConfigID uint32
	if err := db.QueryRowContext(ctx,
		"SELECT slot, item_config_id FROM player_equipment WHERE player_id = 7001").Scan(&slot, &itemConfigID); err != nil {
		t.Fatalf("读取 player_equipment 存量行: %v", err)
	}
	if slot != 1 || itemConfigID != 10003 {
		t.Errorf("player_equipment 存量行漂移: slot=%d item=%d", slot, itemConfigID)
	}
	var hasInstanceColumn int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_equipment' AND COLUMN_NAME = 'instance_id'`).
		Scan(&hasInstanceColumn); err != nil {
		t.Fatalf("探测 player_equipment.instance_id: %v", err)
	}
	if hasInstanceColumn > 0 {
		var instanceID sql.NullInt64
		if err := db.QueryRowContext(ctx,
			"SELECT instance_id FROM player_equipment WHERE player_id = 7001").Scan(&instanceID); err != nil {
			t.Fatalf("读取 player_equipment.instance_id: %v", err)
		}
		if instanceID.Valid {
			t.Errorf("v6 不得猜测回填旧预设 instance_id,实际=%d", instanceID.Int64)
		}
	}
}

// assertPlayerSkillCardSchema 断言 v5 结构齐备:三张表 + 每条业务/清理索引。
// 清理索引单列出来核对,因为它是 dbcheck 的发布门禁项(§9.24)。
func assertPlayerSkillCardSchema(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	for _, table := range skillCardTables {
		if !tableExists(t, ctx, db, table) {
			t.Errorf("迁移后缺表 %s", table)
		}
	}
	wantIndexes := []struct {
		table   string
		index   string
		columns []string
	}{
		{"player_skill_cards", "uk_player_card", []string{"player_id", "card_id"}},
		{"player_skill_slots", "uk_player_slot", []string{"player_id", "slot"}},
		{"player_skill_slots", "uk_player_card_once", []string{"player_id", "card_id"}},
		{"skill_card_grants", "uk_player_key", []string{"player_id", "idempotency_key"}},
		{"skill_card_grants", "idx_created", []string{"created_at"}}, // §9.24 清理索引
	}
	for _, want := range wantIndexes {
		got := indexColumns(t, ctx, db, want.table, want.index)
		if got == nil {
			t.Errorf("迁移后缺索引 %s.%s", want.table, want.index)
			continue
		}
		if strings.Join(got, ",") != strings.Join(want.columns, ",") {
			t.Errorf("索引 %s.%s 列=%v, 期望=%v", want.table, want.index, got, want.columns)
		}
	}
}

// assertPlayerEquipmentInstanceSchema 断言 v6 的 nullable expand 列与单玩家实例唯一键齐备。
func assertPlayerEquipmentInstanceSchema(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var dataType, columnType, nullable string
	if err := db.QueryRowContext(ctx,
		`SELECT DATA_TYPE, COLUMN_TYPE, IS_NULLABLE FROM information_schema.COLUMNS
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_equipment' AND COLUMN_NAME = 'instance_id'`).
		Scan(&dataType, &columnType, &nullable); err != nil {
		t.Fatalf("迁移后缺 player_equipment.instance_id: %v", err)
	}
	if !strings.EqualFold(dataType, "bigint") || !strings.Contains(strings.ToLower(columnType), "unsigned") || nullable != "YES" {
		t.Errorf("player_equipment.instance_id 形态=%s/%s nullable=%s,期望 bigint unsigned/YES",
			dataType, columnType, nullable)
	}
	got := indexColumns(t, ctx, db, "player_equipment", "uk_player_instance")
	want := []string{"player_id", "instance_id"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("索引 player_equipment.uk_player_instance 列=%v,期望=%v", got, want)
	}
}

func assertPlayerMigrationVersion(t *testing.T, ctx context.Context, db *sql.DB, want uint) {
	t.Helper()
	var version uint
	var dirty bool
	if err := db.QueryRowContext(ctx, "SELECT version, dirty FROM schema_migrations LIMIT 1").Scan(&version, &dirty); err != nil {
		t.Fatalf("读取 schema_migrations: %v", err)
	}
	if version != want || dirty {
		t.Fatalf("schema_migrations version=%d dirty=%v, 期望 version=%d dirty=false", version, dirty, want)
	}
}

func tableExists(t *testing.T, ctx context.Context, db *sql.DB, table string) bool {
	t.Helper()
	var n int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
		table).Scan(&n); err != nil {
		t.Fatalf("查询表 %s: %v", table, err)
	}
	return n > 0
}

// indexColumns 返回索引的列(按 SEQ_IN_INDEX 顺序);索引不存在返回 nil。
func indexColumns(t *testing.T, ctx context.Context, db *sql.DB, table, index string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx,
		`SELECT column_name FROM information_schema.statistics
		 WHERE table_schema = DATABASE() AND table_name = ? AND index_name = ?
		 ORDER BY seq_in_index`, table, index)
	if err != nil {
		t.Fatalf("查询索引 %s.%s: %v", table, index, err)
	}
	defer func() { _ = rows.Close() }()
	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			t.Fatalf("扫描索引列 %s.%s: %v", table, index, err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("遍历索引列 %s.%s: %v", table, index, err)
	}
	return columns
}
