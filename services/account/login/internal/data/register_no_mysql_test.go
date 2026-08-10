// register_no_mysql_test.go — 注册编号补号的**真 MySQL / 真 TiDB** 双后端语义验证
// (docs/design/register-no-and-login-surge.md §3.3,2026-08-10 落码)。
//
// 为什么必须有这一层、且必须两个后端都跑:方案的四条承诺全部押在真实引擎语义上,
// fake 无法替代,且两家引擎的语义差异恰好落在承诺①的要害上——
//
//	① 计数器行 `SELECT ... FOR UPDATE` + READ COMMITTED 真的把并发补号串行化(多副本
//	   无 leader 安全的全部依据)。TiDB 悲观事务 RR 用 BeginTx 时刻的 start_ts 快照服务
//	   普通 SELECT,取批扫描看不到锁前驱批次的提交,曾把正常并发误判成"第二写者"
//	   (2026-08-10 真 TiDB 并发测试抓获,InnoDB RR 因 read view 迟建而恰好无症状);
//	   修法 = 补号事务 READ COMMITTED,测试③是该缺陷的针对性回归:修复前 TiDB 必炸、
//	   修复后两端全绿;
//	② 编号全序 = (created_at, player_id) 全序,跨多批仍严格连续无空洞;
//	③ 水位安全滞后真的把窗口内的行留到下一轮,且不影响窗口外行的顺序;
//	④ 起始号只在计数器首次初始化生效(INSERT IGNORE 幂等,改配置不改已有计数器)。
//
// 门控(沿用仓库 friend/guild 双后端约定):DSN **必须不带库名**,测试自建随机临时库
// 并在结束时删掉;对应变量未设置时该后端明确 Skip,已设置但不可达则硬失败。
//
//	kubectl -n pandora port-forward svc/mysql 13306:3306
//	$env:PANDORA_TEST_MYSQL_DSN='root:<pw>@tcp(127.0.0.1:13306)/?parseTime=true&loc=UTC&charset=utf8mb4'
//	$env:PANDORA_TEST_TIDB_DSN='root:@tcp(127.0.0.1:4000)/?parseTime=true&loc=UTC&charset=utf8mb4'
//	go test ./services/account/login/internal/data/ -count=1 -run RegisterNo_MySQLAndTiDB -v
package data

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

const registerNoTestDBPrefix = "pandora_account_regno_it_"

var registerNoTestDBPattern = regexp.MustCompile(`^pandora_account_regno_it_[0-9]+_[0-9a-f]{16}$`)

// registerNoTestDDL 与 deploy/mysql-init/02-account-tables.sql / deploy/tidb-init/
// 03-account-tidb.sql / migrations 000004 的 accounts + register_no_counter 定义一致
// (测试只需要参与补号的列;ENGINE 子句 TiDB 接受并忽略)。
var registerNoTestDDL = []string{
	"CREATE TABLE `accounts` (" +
		"`player_id`     BIGINT UNSIGNED  NOT NULL," +
		"`account`       VARCHAR(64)      NOT NULL," +
		"`password_hash` VARCHAR(80)      NOT NULL DEFAULT ''," +
		"`status`        TINYINT UNSIGNED NOT NULL DEFAULT 0," +
		"`register_no`   BIGINT UNSIGNED       NULL," +
		"`created_at`    DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP," +
		"`updated_at`    DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
		"PRIMARY KEY (`player_id`)," +
		"UNIQUE KEY `uk_account` (`account`)," +
		"UNIQUE KEY `uk_register_no` (`register_no`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
	"CREATE TABLE `register_no_counter` (" +
		"`id`      TINYINT UNSIGNED NOT NULL," +
		"`next_no` BIGINT UNSIGNED  NOT NULL," +
		"PRIMARY KEY (`id`)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
}

type registerNoBackend struct {
	name string
	env  string
}

// forEachRegisterNoBackend 对 MySQL / TiDB 各跑一遍同一组断言(friend/guild 套件同款):
// 未设置对应 DSN 的后端明确 Skip,两个都没设时整组 Skip 而非静默全绿。
func forEachRegisterNoBackend(t *testing.T, fn func(t *testing.T, db *sql.DB)) {
	t.Helper()
	backends := [...]registerNoBackend{
		{name: "mysql", env: "PANDORA_TEST_MYSQL_DSN"},
		{name: "tidb", env: "PANDORA_TEST_TIDB_DSN"},
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) {
			dsn := strings.TrimSpace(os.Getenv(backend.env))
			if dsn == "" {
				t.Skipf("跳过 %s register_no 集成测试:未设置 %s", backend.name, backend.env)
			}
			fn(t, newRegisterNoTestDB(t, backend.name, dsn))
		})
	}
}

// newRegisterNoTestDB 建临时库 + 建表,返回 *sql.DB;DSN 已设置但后端不可达则硬失败。
func newRegisterNoTestDB(t *testing.T, backend, dsn string) *sql.DB {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse %s 测试 DSN: %v", backend, err)
	}
	if strings.TrimSpace(cfg.DBName) != "" {
		t.Fatalf("%s 测试 DSN 必须不带库名(拿到 %q):测试要自建临时库,不能碰业务库", backend, cfg.DBName)
	}
	var seed [8]byte
	if _, rerr := rand.Read(seed[:]); rerr != nil {
		t.Fatalf("rand: %v", rerr)
	}
	dbName := fmt.Sprintf("%s%d_%s", registerNoTestDBPrefix, time.Now().UnixNano(), hex.EncodeToString(seed[:]))
	if !registerNoTestDBPattern.MatchString(dbName) {
		t.Fatalf("生成的临时库名不合规:%s", dbName)
	}

	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open %s admin conn: %v", backend, err)
	}
	// DDL 超时放宽:一次性建库建表,环境慢不应误报逻辑失败(同 session_generation 套件)。
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("已设置 %s 测试 DSN 但后端不可达:%v", backend, err)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+dbName+"`"); err != nil {
		_ = admin.Close()
		t.Fatalf("create temp db: %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer dcancel()
		if !registerNoTestDBPattern.MatchString(dbName) { // 二次校验,永不误删
			return
		}
		_, _ = admin.ExecContext(dctx, "DROP DATABASE IF EXISTS `"+dbName+"`")
		_ = admin.Close()
	})

	cfg.DBName = dbName
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("open temp db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	for _, ddl := range registerNoTestDDL {
		if _, err := db.ExecContext(ctx, ddl); err != nil {
			t.Fatalf("create table: %v", err)
		}
	}
	return db
}

// insertAccountAt 插入一条测试账号,created_at 显式指定(相对 NOW() 的秒偏移,负数=过去)。
// 用显式时间戳而不是 sleep 等真实时间,让水位滞后可测且测试不慢。
func insertAccountAt(t *testing.T, db *sql.DB, playerID uint64, offsetSec int) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		"INSERT INTO accounts (player_id, account, created_at) VALUES (?, ?, NOW() + INTERVAL ? SECOND)",
		playerID, fmt.Sprintf("acct_%d", playerID), offsetSec); err != nil {
		t.Fatalf("insert account %d: %v", playerID, err)
	}
}

func fetchRegisterNo(t *testing.T, db *sql.DB, playerID uint64) (uint64, bool) {
	t.Helper()
	var no sql.NullInt64
	if err := db.QueryRowContext(context.Background(),
		"SELECT register_no FROM accounts WHERE player_id = ?", playerID).Scan(&no); err != nil {
		t.Fatalf("fetch register_no %d: %v", playerID, err)
	}
	if !no.Valid {
		return 0, false
	}
	return uint64(no.Int64), true
}

func fetchCounter(t *testing.T, db *sql.DB) uint64 {
	t.Helper()
	var next uint64
	if err := db.QueryRowContext(context.Background(),
		"SELECT next_no FROM register_no_counter WHERE id = 1").Scan(&next); err != nil {
		t.Fatalf("fetch counter: %v", err)
	}
	return next
}

// drainRegisterNo 反复 SweepRegisterNo 到无待编号行,返回总编号数。
func drainRegisterNo(t *testing.T, db *sql.DB, batch int) int {
	t.Helper()
	total := 0
	for i := 0; i < 100; i++ { // 上限护栏,防实现 bug 死循环
		n, err := SweepRegisterNo(context.Background(), db, batch)
		if err != nil {
			t.Fatalf("sweep: %v", err)
		}
		total += n
		if n < batch {
			return total
		}
	}
	t.Fatalf("drain 未收敛(100 轮仍有整批产出)")
	return total
}

// ① 编号全序 = (created_at, player_id) 全序,跨多批严格连续无空洞;计数器精确推进。
func TestRegisterNo_MySQLAndTiDB_OrderAndContinuityAcrossBatches(t *testing.T) {
	forEachRegisterNoBackend(t, func(t *testing.T, db *sql.DB) {
		if err := EnsureRegisterNoCounter(context.Background(), db, 1); err != nil {
			t.Fatalf("ensure counter: %v", err)
		}

		// 7 行,created_at 三个时刻(远早于 10s 水位),同刻内靠 player_id 决胜;
		// 插入顺序故意乱序,验证编号只认 (created_at, player_id) 不认插入序。
		insertAccountAt(t, db, 505, -120) // 第 3 名(t-120 组内 player_id 最大)
		insertAccountAt(t, db, 501, -120) // 第 1 名
		insertAccountAt(t, db, 900, -60)  // 第 5 名
		insertAccountAt(t, db, 503, -120) // 第 2 名
		insertAccountAt(t, db, 700, -90)  // 第 4 名
		insertAccountAt(t, db, 902, -60)  // 第 6 名
		insertAccountAt(t, db, 999, -30)  // 第 7 名

		// batch=3 强制跨 3 批,验证批间连续。
		if got := drainRegisterNo(t, db, 3); got != 7 {
			t.Fatalf("drain assigned=%d want 7", got)
		}
		want := []struct {
			playerID uint64
			no       uint64
		}{{501, 1}, {503, 2}, {505, 3}, {700, 4}, {900, 5}, {902, 6}, {999, 7}}
		for _, w := range want {
			no, ok := fetchRegisterNo(t, db, w.playerID)
			if !ok || no != w.no {
				t.Fatalf("player %d register_no=%d(assigned=%v) want %d", w.playerID, no, ok, w.no)
			}
		}
		if next := fetchCounter(t, db); next != 8 {
			t.Fatalf("counter next_no=%d want 8", next)
		}
	})
}

// ② 水位滞后:窗口内的行本轮不编号;窗口外的行照常编,且后补的行接续编号不乱序。
func TestRegisterNo_MySQLAndTiDB_WatermarkLagHoldsFreshRows(t *testing.T) {
	forEachRegisterNoBackend(t, func(t *testing.T, db *sql.DB) {
		if err := EnsureRegisterNoCounter(context.Background(), db, 1); err != nil {
			t.Fatalf("ensure counter: %v", err)
		}

		insertAccountAt(t, db, 1001, -60) // 窗口外 → 本轮编 1
		insertAccountAt(t, db, 1002, 0)   // 刚打戳,窗口内 → 本轮必须跳过
		if got := drainRegisterNo(t, db, RegisterNoBatchSize); got != 1 {
			t.Fatalf("drain assigned=%d want 1(窗口内行被提前编号 = 迟可见封窗失效)", got)
		}
		if _, ok := fetchRegisterNo(t, db, 1002); ok {
			t.Fatalf("player 1002 仍在水位窗口内却已编号")
		}

		// 把 1002 的时间戳改老(等价于窗口过期,不真等 10s),下一轮应编到 2。
		if _, err := db.ExecContext(context.Background(),
			"UPDATE accounts SET created_at = NOW() - INTERVAL 30 SECOND WHERE player_id = 1002"); err != nil {
			t.Fatalf("age row: %v", err)
		}
		if got := drainRegisterNo(t, db, RegisterNoBatchSize); got != 1 {
			t.Fatalf("second drain assigned=%d want 1", got)
		}
		if no, ok := fetchRegisterNo(t, db, 1002); !ok || no != 2 {
			t.Fatalf("player 1002 register_no=%d want 2", no)
		}
	})
}

// ③ 并发补号被计数器行锁 + READ COMMITTED 串行化:两个 goroutine 同时 drain,
// 总量精确、无重号、无空洞。这是"多副本各自跑、无需 leader election"的全部依据,
// 也是 2026-08-10 TiDB start_ts 快照缺陷的针对性回归(修复前 TiDB 端必现
// "affected=0 第二写者"误报,修复后两端全绿)。
func TestRegisterNo_MySQLAndTiDB_ConcurrentSweepersSerialize(t *testing.T) {
	forEachRegisterNoBackend(t, func(t *testing.T, db *sql.DB) {
		if err := EnsureRegisterNoCounter(context.Background(), db, 1); err != nil {
			t.Fatalf("ensure counter: %v", err)
		}
		const rows = 200
		for i := 0; i < rows; i++ {
			insertAccountAt(t, db, uint64(2000+i), -3600+i) // 时间戳各不相同,全在窗口外
		}

		var wg sync.WaitGroup
		totals := make([]int, 2)
		errs := make([]error, 2)
		for w := 0; w < 2; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for {
					n, err := SweepRegisterNo(context.Background(), db, 50)
					if err != nil {
						errs[w] = err
						return
					}
					totals[w] += n
					if n < 50 {
						return
					}
				}
			}(w)
		}
		wg.Wait()
		for w, err := range errs {
			if err != nil {
				t.Fatalf("sweeper %d: %v", w, err)
			}
		}
		if got := totals[0] + totals[1]; got != rows {
			t.Fatalf("并发 drain 总量=%d want %d", got, rows)
		}
		// 全序断言:编号 1..rows 恰好各出现一次,且顺序 = created_at 序(即 player_id 序,时间戳递增构造)。
		var cnt int
		if err := db.QueryRowContext(context.Background(),
			"SELECT COUNT(DISTINCT register_no) FROM accounts WHERE register_no BETWEEN 1 AND ?", rows).Scan(&cnt); err != nil {
			t.Fatalf("count distinct: %v", err)
		}
		if cnt != rows {
			t.Fatalf("distinct 编号=%d want %d(有重号或空洞)", cnt, rows)
		}
		firstNo, ok := fetchRegisterNo(t, db, 2000)
		lastNo, ok2 := fetchRegisterNo(t, db, uint64(2000+rows-1))
		if !ok || !ok2 || firstNo != 1 || lastNo != uint64(rows) {
			t.Fatalf("边界错序:earliest=%d(want 1) latest=%d(want %d)", firstNo, lastNo, rows)
		}
		if next := fetchCounter(t, db); next != uint64(rows)+1 {
			t.Fatalf("counter next_no=%d want %d", next, rows+1)
		}
	})
}

// ④ 起始号只在计数器首次初始化生效:INSERT IGNORE 幂等,二次 Ensure 换起始号不生效。
func TestRegisterNo_MySQLAndTiDB_EnsureCounterIdempotentStart(t *testing.T) {
	forEachRegisterNoBackend(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		if err := EnsureRegisterNoCounter(ctx, db, 100001); err != nil {
			t.Fatalf("ensure counter: %v", err)
		}
		if err := EnsureRegisterNoCounter(ctx, db, 777); err != nil { // 改配置重启,不得覆盖
			t.Fatalf("re-ensure counter: %v", err)
		}
		if next := fetchCounter(t, db); next != 100001 {
			t.Fatalf("counter next_no=%d want 100001(起始号被二次初始化覆盖)", next)
		}
		insertAccountAt(t, db, 3001, -60)
		if got := drainRegisterNo(t, db, RegisterNoBatchSize); got != 1 {
			t.Fatalf("drain assigned=%d want 1", got)
		}
		if no, ok := fetchRegisterNo(t, db, 3001); !ok || no != 100001 {
			t.Fatalf("player 3001 register_no=%d want 100001", no)
		}
	})
}

// ⑤ 迁移未跑(缺列)时 Ensure 必须失败(启动探针 fail-soft 的依据),且不产生半初始化状态。
func TestRegisterNo_MySQLAndTiDB_EnsureFailsWithoutMigration(t *testing.T) {
	forEachRegisterNoBackend(t, func(t *testing.T, db *sql.DB) {
		ctx := context.Background()
		// 模拟 000004 未跑:删掉列与计数器表。
		if _, err := db.ExecContext(ctx, "ALTER TABLE accounts DROP INDEX uk_register_no"); err != nil {
			t.Fatalf("drop index: %v", err)
		}
		if _, err := db.ExecContext(ctx, "ALTER TABLE accounts DROP COLUMN register_no"); err != nil {
			t.Fatalf("drop column: %v", err)
		}
		if _, err := db.ExecContext(ctx, "DROP TABLE register_no_counter"); err != nil {
			t.Fatalf("drop counter: %v", err)
		}
		if err := EnsureRegisterNoCounter(ctx, db, 1); err == nil {
			t.Fatalf("缺列时 Ensure 应失败(探针失效会让补号任务在未迁移库上每 5s 报错)")
		}
	})
}
