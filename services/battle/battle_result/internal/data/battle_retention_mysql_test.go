// battle_retention_mysql_test.go — 保留期清理的真实 MySQL 集成测试(2026-07-21,§9.24)。
//
// 自建随机临时库并重放 03-battle-tables.sql + 05-battle-outbox.sql(对齐 inventory 夹具模式;
// PANDORA_TEST_MYSQL_DSN 未设置时 Skip,DSN 必须不带 database)。
// 覆盖:battles+stats 同事务批删(超期删/未超期留/ended=0 按 created_at 兜底)、
// progress stream+player 只删已结算超期行(未结算陈年行保留)。
package data

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
)

var battleRetentionTestDBPattern = regexp.MustCompile(`^pandora_battle_it_[0-9]+_[0-9a-f]{12}$`)

func openBattleRetentionDB(t *testing.T) *sql.DB {
	t.Helper()
	serverDSN := strings.TrimSpace(os.Getenv("PANDORA_TEST_MYSQL_DSN"))
	if serverDSN == "" {
		t.Skip("未设置 PANDORA_TEST_MYSQL_DSN,跳过 battle_result 真 MySQL 集成测试")
	}
	cfg, err := mysql.ParseDSN(serverDSN)
	if err != nil {
		t.Fatalf("解析 PANDORA_TEST_MYSQL_DSN: %v", err)
	}
	if cfg.DBName != "" {
		t.Fatalf("PANDORA_TEST_MYSQL_DSN 禁止带 database,got=%q", cfg.DBName)
	}
	cfg.MultiStatements = true
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second

	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开 MySQL 管理连接: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("已设置测试 DSN 但 MySQL 不可达: %v", err)
	}

	b := make([]byte, 6)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("生成随机测试库后缀: %v", err)
	}
	dbName := fmt.Sprintf("pandora_battle_it_%d_%s", time.Now().UnixMilli(), hex.EncodeToString(b))
	if !battleRetentionTestDBPattern.MatchString(dbName) {
		t.Fatalf("随机测试库名未通过安全校验: %q", dbName)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+dbName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		t.Fatalf("创建随机测试库 %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer dcancel()
		if _, err := admin.ExecContext(dctx, "DROP DATABASE `"+dbName+"`"); err != nil {
			t.Errorf("删除随机测试库 %s: %v", dbName, err)
		}
		_ = admin.Close()
	})

	for _, file := range []string{"03-battle-tables.sql", "05-battle-outbox.sql"} {
		if _, err := admin.ExecContext(ctx, readBattleSchema(t, file, dbName)); err != nil {
			t.Fatalf("初始化 %s: %v", file, err)
		}
	}

	testCfg := cfg.Clone()
	testCfg.DBName = dbName
	db, err := sql.Open("mysql", testCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开随机测试库: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("连接随机测试库: %v", err)
	}
	return db
}

func readBattleSchema(t *testing.T, name, dbName string) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("无法定位测试文件")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "..", "..",
		"deploy", "mysql-init", name))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 schema %s: %v", path, err)
	}
	schema := string(b)
	const needle = "USE `pandora_battle`;"
	if strings.Count(schema, needle) != 1 {
		t.Fatalf("%s USE 锚点数量异常: %d", name, strings.Count(schema, needle))
	}
	return strings.Replace(schema, needle, "USE `"+dbName+"`;", 1)
}

func TestBattleRetentionPurge_MySQL(t *testing.T) {
	db := openBattleRetentionDB(t)
	repo := NewMySQLBattleRepo(db)
	ctx := context.Background()

	nowMs := time.Now().UnixMilli()
	const day = int64(86400_000)
	cutoffMs := nowMs - 90*day

	t.Run("BattlesAndStatsPurgedTogether", func(t *testing.T) {
		// 清理依据 = 服务端落库时间 created_at(§9.6 数值不信 DS,ended_at_ms 不参与判定):
		// match 1: created_at 91 天前 → 删;
		// match 2: created_at 89 天前 → 留;
		// match 3: DS 伪造 ended_at_ms 很古老但 created_at 是现在 → 留(不信 DS 时间,不可提前删);
		// match 4: DS 伪造 ended_at_ms 在未来但 created_at 100 天前 → 删(伪造未来时间挡不住清理)。
		mustExecBattle(t, db, `INSERT INTO battles(match_id, started_at_ms, ended_at_ms, created_at) VALUES
			(1, ?, ?, DATE_SUB(NOW(), INTERVAL 91 DAY)),
			(2, ?, ?, DATE_SUB(NOW(), INTERVAL 89 DAY)),
			(3, ?, ?, NOW()),
			(4, ?, ?, DATE_SUB(NOW(), INTERVAL 100 DAY))`,
			nowMs-92*day, nowMs-91*day,
			nowMs-90*day, nowMs-89*day,
			nowMs-200*day, nowMs-199*day,
			nowMs-100*day, nowMs+365*day)
		mustExecBattle(t, db, `INSERT INTO battle_player_stats(match_id, player_id) VALUES
			(1, 11), (1, 12), (2, 21), (3, 31), (4, 41)`)

		out, err := repo.SweepExpiredBattles(ctx, dbguard.ModeDelete, cutoffMs, 100)
		if err != nil || out.Deleted != 2 {
			t.Fatalf("SweepExpiredBattles(delete): deleted=%d err=%v, want 2(match 1+4)", out.Deleted, err)
		}
		assertCount(t, db, `SELECT COUNT(*) FROM battles`, 2)
		assertCount(t, db, `SELECT COUNT(*) FROM battle_player_stats`, 2)
		assertCount(t, db, `SELECT COUNT(*) FROM battle_player_stats WHERE match_id = 2`, 1)
		assertCount(t, db, `SELECT COUNT(*) FROM battle_player_stats WHERE match_id = 3`, 1)

		// 小批量排空:剩余 2 行按 batch=1 分两批删光(drain 循环的 SQL 侧配合面)。
		mustExecBattle(t, db, `UPDATE battles SET created_at = DATE_SUB(NOW(), INTERVAL 95 DAY)`)
		if out, err := repo.SweepExpiredBattles(ctx, dbguard.ModeDelete, cutoffMs, 1); err != nil || out.Deleted != 1 {
			t.Fatalf("batch=1 first purge: deleted=%d err=%v", out.Deleted, err)
		}
		if out, err := repo.SweepExpiredBattles(ctx, dbguard.ModeDelete, cutoffMs, 1); err != nil || out.Deleted != 1 {
			t.Fatalf("batch=1 second purge: deleted=%d err=%v", out.Deleted, err)
		}
		assertCount(t, db, `SELECT COUNT(*) FROM battles`, 0)
		assertCount(t, db, `SELECT COUNT(*) FROM battle_player_stats`, 0)
	})

	t.Run("SettledProgressPurgedUnsettledKept", func(t *testing.T) {
		// match 101: 结算于 91 天前 → 删(含 player 行);match 102: 结算于 10 天前 → 留;
		// match 103: 未结算(settled=0)但很老 → 永不清(补偿链 bug 证据)。
		mustExecBattle(t, db, `INSERT INTO battle_progress_stream(match_id, last_applied_seq, settled_at_ms, updated_at_ms) VALUES
			(101, 5, ?, ?), (102, 5, ?, ?), (103, 5, 0, ?)`,
			nowMs-91*day, nowMs-91*day,
			nowMs-10*day, nowMs-10*day,
			nowMs-200*day)
		mustExecBattle(t, db, `INSERT INTO battle_progress_player(match_id, player_id, total_exp) VALUES
			(101, 11, 100), (101, 12, 200), (102, 21, 300), (103, 31, 400)`)

		out, err := repo.SweepSettledProgress(ctx, dbguard.ModeDelete, cutoffMs, 100)
		if err != nil || out.Deleted != 1 {
			t.Fatalf("SweepSettledProgress(delete): deleted=%d err=%v, want 1(match 101)", out.Deleted, err)
		}
		assertCount(t, db, `SELECT COUNT(*) FROM battle_progress_stream`, 2)
		assertCount(t, db, `SELECT COUNT(*) FROM battle_progress_player WHERE match_id = 101`, 0)
		assertCount(t, db, `SELECT COUNT(*) FROM battle_progress_stream WHERE match_id = 103`, 1)

		// 陈年未结算行(103)永不清理,但必须被告警探测数出来(updated_at_ms 超期)。
		stale, serr := repo.CountStaleUnsettledProgress(ctx, cutoffMs)
		if serr != nil || stale != 1 {
			t.Fatalf("CountStaleUnsettledProgress: n=%d err=%v, want n=1(match 103)", stale, serr)
		}
	})
}

func mustExecBattle(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %s: %v", q, err)
	}
}

func assertCount(t *testing.T, db *sql.DB, q string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(q).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", q, err)
	}
	if got != want {
		t.Fatalf("%s = %d, want %d", q, got, want)
	}
}
