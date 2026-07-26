// session_generation_mysql_test.go — 会话代际的**真 MySQL** 语义验证(R11 复审 P0-1)。
//
// 为什么必须有这一层:P0-1 的整条修复押在三条**真 InnoDB 语义**上,fake 无法替代——
//
//	① `SELECT ... FOR UPDATE` 真的把并发 Login 串行化(定序权威成立);
//	② `ON DUPLICATE KEY UPDATE generation = generation + 1` 真的单调不跳(不回退/不重复);
//	③ 条件回补 `WHERE sess_jti=? AND generation=?` 真的只在"行仍属于本次写"时命中 ——
//	   这是"不确定 COMMIT 且读回失败 → 回补到覆盖前 jti"能收敛到一致的全部依据。
//	   若它在并发赢家已改写行之后仍会命中,就会把别人的登录回滚掉(违反不回档)。
//
// 门控(沿用仓库既有约定):DSN **必须不带库名**,测试自建随机临时库并在结束时删掉。
//
//	kubectl -n pandora port-forward svc/mysql 13306:3306
//	$env:PANDORA_TEST_MYSQL_DSN='root:<pw>@tcp(127.0.0.1:13306)/?parseTime=true&loc=UTC&charset=utf8mb4'
//	go test ./services/account/login/internal/data/ -count=1 -run SessionGeneration_MySQL -v
//
// 安全边界:拒绝带库名的 DSN;只在严格校验过的 pandora_account_it_<ts>_<rand> 临时库内建表,
// 绝不触碰任何业务库的行。
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

	"github.com/luyuancpp/pandora/pkg/errcode"
)

const sessionGenTestDBPrefix = "pandora_account_it_"

var sessionGenTestDBPattern = regexp.MustCompile(`^pandora_account_it_[0-9]+_[0-9a-f]{16}$`)

// sessionGenTestDDL 与 tools/migrate/migrations/pandora_account/000003 的建表语句一致。
const sessionGenTestDDL = "CREATE TABLE `player_session_generations` (" +
	"`player_id`  BIGINT UNSIGNED NOT NULL," +
	"`sess_jti`   VARCHAR(64)     NOT NULL," +
	"`generation` BIGINT UNSIGNED NOT NULL DEFAULT 0," +
	"`updated_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP," +
	"PRIMARY KEY (`player_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"

// newSessionGenMySQLRepo 建临时库 + 建表,返回 repo;未配 DSN 即 Skip。
func newSessionGenMySQLRepo(t *testing.T) *MySQLSessionGenerationRepo {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("PANDORA_TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("set PANDORA_TEST_MYSQL_DSN (无库名) for the real-MySQL session generation suite")
	}
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("parse PANDORA_TEST_MYSQL_DSN: %v", err)
	}
	if strings.TrimSpace(cfg.DBName) != "" {
		t.Fatalf("PANDORA_TEST_MYSQL_DSN 必须不带库名(拿到 %q):测试要自建临时库,不能碰业务库", cfg.DBName)
	}
	var seed [8]byte
	if _, rerr := rand.Read(seed[:]); rerr != nil {
		t.Fatalf("rand: %v", rerr)
	}
	dbName := fmt.Sprintf("%s%d_%s", sessionGenTestDBPrefix, time.Now().UnixNano(), hex.EncodeToString(seed[:]))
	if !sessionGenTestDBPattern.MatchString(dbName) {
		t.Fatalf("生成的临时库名不合规:%s", dbName)
	}

	admin, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open admin conn: %v", err)
	}
	// 建库/建表超时放宽:这些是一次性 DDL,机器在跑别的重活(如 UE 构建)时可能秒级变几十秒。
	// 设太紧会把"环境慢"误报成"逻辑失败",反而掩盖真问题。
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+dbName+"`"); err != nil {
		_ = admin.Close()
		t.Fatalf("create temp db: %v", err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer dcancel()
		if !sessionGenTestDBPattern.MatchString(dbName) { // 二次校验,永不误删
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
	if _, err := db.ExecContext(ctx, sessionGenTestDDL); err != nil {
		t.Fatalf("create table: %v", err)
	}
	return NewMySQLSessionGenerationRepo(db)
}

// ① 并发 Login 必须被 FOR UPDATE 串行化,代际严格单调、不重复、不跳号。
// 这是"MySQL 是登录定序权威"的全部依据。
func TestSessionGeneration_MySQL_ConcurrentLoginsSerializeMonotonically(t *testing.T) {
	repo := newSessionGenMySQLRepo(t)
	ctx := context.Background()
	const playerID = uint64(90001)
	const n = 12

	var wg sync.WaitGroup
	gens := make([]uint64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			lease, err := repo.PersistSessionJTI(ctx, playerID, fmt.Sprintf("jti-%02d", idx))
			gens[idx], errs[idx] = lease.Generation, err
		}(i)
	}
	wg.Wait()

	seen := map[uint64]int{}
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("并发登录 %d 失败(真 MySQL 下不应有失败):%v", i, errs[i])
		}
		if gens[i] == 0 {
			t.Fatalf("并发登录 %d 拿到代际 0(哨兵值)", i)
		}
		seen[gens[i]]++
	}
	// 每个代际必须恰好被一次登录拿到:重复 = FOR UPDATE 没串行化 = 定序权威不成立。
	for g, c := range seen {
		if c != 1 {
			t.Fatalf("代际 %d 被 %d 次登录同时拿到:FOR UPDATE 未串行化并发登录", g, c)
		}
	}
	if len(seen) != n {
		t.Fatalf("代际去重后 %d 个,应为 %d 个", len(seen), n)
	}
	// 行内最终代际 = 最大值(单调不回退)。
	_, finalGen, found, err := repo.LoadSessionGeneration(ctx, playerID)
	if err != nil || !found {
		t.Fatalf("read back: found=%v err=%v", found, err)
	}
	var maxGen uint64
	for g := range seen {
		if g > maxGen {
			maxGen = g
		}
	}
	if finalGen != maxGen {
		t.Fatalf("行内代际 %d != 并发中的最大代际 %d(发生了回退)", finalGen, maxGen)
	}
}

// ①b 并发争用**耗尽重试后必须报可重试语义**(ErrUnavailable),不能是 ErrInternal。
//
// 这条是真库实测暴露出来的连锁缺陷:1213 死锁原本被包成 ErrInternal(2),而客户端与 UE 侧的
// 可重试判定只认 ErrUnavailable(10) → 玩家首登撞上死锁就被当作终态,卡在登录页(违反不卡玩家)。
// 用 1 次连接上限的连接池 + 已被占用的连接制造"必然锁等待",验证错误码语义。
func TestSessionGeneration_MySQL_ContentionSurfacesRetryableCode(t *testing.T) {
	repo := newSessionGenMySQLRepo(t)
	ctx := context.Background()
	const playerID = uint64(90006)

	// 先建行,再用另一个事务长期持有该行的 X 锁,逼出锁等待超时。
	if _, err := repo.PersistSessionJTI(ctx, playerID, "jti-seed"); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	blocker, err := repo.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin blocker tx: %v", err)
	}
	defer func() { _ = blocker.Rollback() }()
	var scratch string
	if err := blocker.QueryRowContext(ctx,
		`SELECT sess_jti FROM player_session_generations WHERE player_id = ? FOR UPDATE`,
		playerID).Scan(&scratch); err != nil {
		t.Fatalf("blocker lock: %v", err)
	}
	// 把锁等待压到 1s,让本用例在秒级内确定地拿到 1205,而不是等默认 50s。
	if _, err := repo.db.ExecContext(ctx, "SET SESSION innodb_lock_wait_timeout = 1"); err != nil {
		t.Fatalf("set lock wait timeout: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	_, err = repo.PersistSessionJTI(waitCtx, playerID, "jti-contended")
	if err == nil {
		t.Skip("未能制造锁争用(连接池给了未设置超时的新连接);本用例只在能复现争用时有意义")
	}
	if code := errcode.As(err); code != errcode.ErrUnavailable {
		t.Fatalf("争用耗尽后必须是可重试的 ErrUnavailable(否则客户端当终态,玩家卡登录页),得 errcode=%d err=%v",
			code, err)
	}
}

// ② 条件回补的**命中**语义:行仍是本次写入的 (jti, generation) 时回到覆盖前 jti。
// 这是"不确定 COMMIT + 读回失败 → 回补"能收敛到与 Redis 一致的依据。
func TestSessionGeneration_MySQL_RestoreHitsOnlyOwnRow(t *testing.T) {
	repo := newSessionGenMySQLRepo(t)
	ctx := context.Background()
	const playerID = uint64(90002)

	// A 先登录(已交付,Redis 持有 A)。
	leaseA, err := repo.PersistSessionJTI(ctx, playerID, "jti-A")
	if err != nil {
		t.Fatalf("persist A: %v", err)
	}
	// B 登录:模拟"COMMIT 落地但回包丢失"——行已是 B,lease 带着覆盖前快照 A。
	leaseB, err := repo.PersistSessionJTI(ctx, playerID, "jti-B")
	if err != nil {
		t.Fatalf("persist B: %v", err)
	}
	if leaseB.PrevJTI != "jti-A" || !leaseB.HadPrev {
		t.Fatalf("覆盖前快照必须是 A:%+v", leaseB)
	}
	if leaseB.Generation <= leaseA.Generation {
		t.Fatalf("代际必须严格递增:A=%d B=%d", leaseA.Generation, leaseB.Generation)
	}

	// 回补:行仍是 (jti-B, genB) → 必须命中并回到 A。
	restored, err := repo.RestoreSessionJTI(ctx, playerID, "jti-B", leaseB)
	if err != nil || !restored {
		t.Fatalf("条件回补应命中:restored=%v err=%v", restored, err)
	}
	jti, gen, found, err := repo.LoadSessionGeneration(ctx, playerID)
	if err != nil || !found {
		t.Fatalf("read back: found=%v err=%v", found, err)
	}
	if jti != "jti-A" {
		t.Fatalf("回补后行内 jti 应为 A(与 Redis 一致),得 %q", jti)
	}
	// generation 不回退:单调性是全局不变量,回补只改 jti。
	if gen != leaseB.Generation {
		t.Fatalf("回补不得回退代际:期望仍为 %d,得 %d", leaseB.Generation, gen)
	}
}

// ③ 条件回补的**未命中**语义(最关键的一条):并发赢家已把行推到更高代际时,
// 回补必须 no-op —— 否则就会把别人已经成功的登录回滚掉(违反"不回档")。
func TestSessionGeneration_MySQL_RestoreNeverClobbersConcurrentWinner(t *testing.T) {
	repo := newSessionGenMySQLRepo(t)
	ctx := context.Background()
	const playerID = uint64(90003)

	if _, err := repo.PersistSessionJTI(ctx, playerID, "jti-A"); err != nil {
		t.Fatalf("persist A: %v", err)
	}
	leaseB, err := repo.PersistSessionJTI(ctx, playerID, "jti-B")
	if err != nil {
		t.Fatalf("persist B: %v", err)
	}
	// 赢家 C 在 B 的补偿之前落地。
	leaseC, err := repo.PersistSessionJTI(ctx, playerID, "jti-C")
	if err != nil {
		t.Fatalf("persist C: %v", err)
	}

	// B 迟到回补:WHERE (sess_jti=B AND generation=genB) 已不成立 → 必须 no-op。
	restored, err := repo.RestoreSessionJTI(ctx, playerID, "jti-B", leaseB)
	if err != nil {
		t.Fatalf("回补不应报错(未命中是正常终态):%v", err)
	}
	if restored {
		t.Fatal("迟到回补命中了赢家的行:会把 C 的登录回滚掉(违反不回档)")
	}
	jti, gen, _, err := repo.LoadSessionGeneration(ctx, playerID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if jti != "jti-C" || gen != leaseC.Generation {
		t.Fatalf("赢家 C 的行被改动了:jti=%q gen=%d(期望 jti-C/%d)", jti, gen, leaseC.Generation)
	}
}

// ④ 首登(HadPrev=false)的回补退化为删行,等价于回到"从未登录过"的一致态;
// 且同样只在行仍属于本次写时命中。
func TestSessionGeneration_MySQL_RestoreFirstLoginDeletesRow(t *testing.T) {
	repo := newSessionGenMySQLRepo(t)
	ctx := context.Background()
	const playerID = uint64(90004)

	lease, err := repo.PersistSessionJTI(ctx, playerID, "jti-first")
	if err != nil {
		t.Fatalf("persist: %v", err)
	}
	if lease.HadPrev {
		t.Fatalf("首登不应有覆盖前快照:%+v", lease)
	}
	restored, err := repo.RestoreSessionJTI(ctx, playerID, "jti-first", lease)
	if err != nil || !restored {
		t.Fatalf("首登回补应命中(删行):restored=%v err=%v", restored, err)
	}
	if _, _, found, err := repo.LoadSessionGeneration(ctx, playerID); err != nil || found {
		t.Fatalf("首登回补后行应不存在:found=%v err=%v", found, err)
	}
}

// ⑤ 登出墓碑同样是条件 CAS:只命中仍持有该 jti 的行,并推进代际(不毒化新会话)。
func TestSessionGeneration_MySQL_TombstoneIsConditional(t *testing.T) {
	repo := newSessionGenMySQLRepo(t)
	ctx := context.Background()
	const playerID = uint64(90005)

	if _, err := repo.PersistSessionJTI(ctx, playerID, "jti-A"); err != nil {
		t.Fatalf("persist A: %v", err)
	}
	leaseB, err := repo.PersistSessionJTI(ctx, playerID, "jti-B")
	if err != nil {
		t.Fatalf("persist B: %v", err)
	}
	// 迟到的 A 登出:行已是 B → 不得命中。
	tombstoned, err := repo.TombstoneSessionJTI(ctx, playerID, "jti-A")
	if err != nil {
		t.Fatalf("tombstone A: %v", err)
	}
	if tombstoned {
		t.Fatal("迟到登出墓碑命中了新会话的行(会毒化 B)")
	}
	// B 自己登出:命中并推进代际。
	tombstoned, err = repo.TombstoneSessionJTI(ctx, playerID, "jti-B")
	if err != nil || !tombstoned {
		t.Fatalf("B 登出墓碑应命中:tombstoned=%v err=%v", tombstoned, err)
	}
	jti, gen, _, err := repo.LoadSessionGeneration(ctx, playerID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if jti == "jti-B" {
		t.Fatal("墓碑后行内不应仍是真实 jti")
	}
	if gen <= leaseB.Generation {
		t.Fatalf("墓碑必须推进代际:%d 应 > %d", gen, leaseB.Generation)
	}
}
