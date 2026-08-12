// rr_gap_lock_deadlock_survey_test.go — 全仓「RR 间隙锁 → insert intention」死锁面普查
// (2026-08-11,INC-20260811-002 §6 同类扫描的执行体)。
//
// 缘起:friend 与 mission 两个域被证实在真 MySQL 上确定性 1213 死锁,形状是
//
//	BEGIN(REPEATABLE READ)
//	SELECT ... WHERE <key> = ? FOR UPDATE   -- **查不到行** → 锁的是键所在的间隙,不是行
//	INSERT INTO <同一张表> ...               -- insert intention 被别人的间隙锁挡住
//	COMMIT
//
// 间隙锁彼此相容,N 个事务都能拿到;冲突全部堆在随后的插入意向上,于是**互不相干的实体**
// (不同玩家 / 不同订单 / 不同公会)也会互相打死。TiDB 没有 gap 锁,所以这类缺陷在双后端
// 测试里只在 MySQL 侧现形。
//
// 静态扫描(见事故文档 §6)在 14 个仍用默认 RR 的文件里发现了「同一张表既 FOR UPDATE 又
// INSERT」。但静态扫描**会漏**:mission 的 `loadState` 把 " FOR UPDATE" 拼成变量,正则看不见,
// 而它恰恰是确诊病例之一。所以这里不靠读代码定罪,直接对**发布 schema 的真实表**跑该形状,
// 逐表给出「RR 下是否成环 / RC 下是否消解」的实测结论。
//
// 本文件测的是**引擎行为在这些表上的表现**,不是各服务的业务路径 —— 它回答的是
// 「这张表上,这个写法安不安全」。业务路径是否真的走了这个写法由静态扫描定位,两者合起来
// 才是定罪证据;单独任何一个都不够。
//
// 运行:
//
//	$env:PANDORA_TEST_MYSQL_DSN='root:<pw>@tcp(127.0.0.1:3307)/'
//	go test ./tools/migrate/ -count=1 -run TestRRGapLockDeadlockSurvey -v
package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

// surveyVerdict 是一张表的普查结论。
type surveyVerdict struct {
	table, owner string
	rrDeadlock   bool
	rcDeadlock   bool
	rrErr, rcErr string
}

var surveyDBPattern = regexp.MustCompile(`^pandora_gaplock_survey_[0-9]+_[0-9a-f]{12}$`)

// surveyCase 是一张被普查的表:静态扫描判定「同一张表既 FOR UPDATE 又 INSERT」的那些。
//
// probeKey/probeInsert 复刻该表在生产代码里的语句形状(按主键或唯一键定位 → 未命中 → 插入),
// 参数 i 由并发 goroutine 各自取不同值,保证**实体互不相干**——这正是 friend/mission 那条
// 「不同玩家也互相打死」的形状。
type surveyCase struct {
	schemaFile string // deploy/mysql-init/ 下的发布 SQL
	useDB      string // 该文件里的 USE 目标库名
	table      string
	owner      string // 代码位置,便于按图索骥
	// selectSQL 按 i 定位一行(必然查不到),insertSQL 插入该行。
	selectSQL string
	insertSQL string
}

func surveyCases() []surveyCase {
	return []surveyCase{
		// ── 阳性对照(必须在 RR 下死锁)────────────────────────────────────────
		//
		// friend_requests 是**已确诊病例**:2026-08-11 在真 MySQL 8.4 上由
		// TestFriendCreateRequestGuardAcrossDistinctTargets 抓到确定性 1213,InnoDB 日志两侧
		// 均为 `uk_requester_target ... lock_mode X` + `insert intention waiting`。
		//
		// 它在这里的作用是**证伪本普查本身**:如果连它都报"RR 安全",说明探针没能复现该形状
		// (窗口太短 / 语句形状不对 / 并发度不够),那么其余各表的"安全"结论一律作废,不得采信。
		// 阴性结果只有在阳性对照亮起时才有意义。
		{
			schemaFile: "06-social-tables.sql", useDB: "pandora_social",
			table: "friend_requests(阳性对照)", owner: "friend/internal/data/friend_repo.go(已修:RC)",
			selectSQL: "SELECT request_id FROM friend_requests WHERE requester_id = ? AND target_id = ? FOR UPDATE",
			insertSQL: "INSERT INTO friend_requests (request_id, requester_id, target_id, status) VALUES (?, ?, ?, 1)",
		},
		// ── 待判定的各表 ──────────────────────────────────────────────────────
		{
			schemaFile: "02-account-tables.sql", useDB: "pandora_account",
			table: "player_session_generations", owner: "login/internal/data/session_generation.go",
			selectSQL: "SELECT generation FROM player_session_generations WHERE player_id = ? FOR UPDATE",
			insertSQL: "INSERT INTO player_session_generations (player_id, generation) VALUES (?, 1)",
		},
		{
			schemaFile: "04-player-tables.sql", useDB: "pandora_player",
			table: "player_attributes", owner: "player/internal/data/attribute_repo.go",
			selectSQL: "SELECT points FROM player_attributes WHERE player_id = ? AND attr_key = 'str' FOR UPDATE",
			insertSQL: "INSERT INTO player_attributes (player_id, attr_key, points) VALUES (?, 'str', 1)",
		},
		{
			schemaFile: "04-player-tables.sql", useDB: "pandora_player",
			table: "player_skill_cards", owner: "player/internal/data/skill_card_repo.go",
			selectSQL: "SELECT level FROM player_skill_cards WHERE player_id = ? AND card_id = 1 FOR UPDATE",
			insertSQL: "INSERT INTO player_skill_cards (player_id, card_id, level, shards) VALUES (?, 1, 1, 0)",
		},
		{
			schemaFile: "06-social-tables.sql", useDB: "pandora_social",
			table: "guild_members", owner: "guild/internal/data/guild_repo.go",
			selectSQL: "SELECT guild_id FROM guild_members WHERE player_id = ? FOR UPDATE",
			insertSQL: "INSERT INTO guild_members (guild_id, player_id, role) VALUES (?, ?, 3)",
		},
		{
			schemaFile: "06-social-tables.sql", useDB: "pandora_social",
			table: "chat_group_members", owner: "guild/internal/data/group_repo.go",
			selectSQL: "SELECT group_id FROM chat_group_members WHERE group_id = ? AND player_id = ? FOR UPDATE",
			insertSQL: "INSERT INTO chat_group_members (group_id, player_id) VALUES (?, ?)",
		},
		{
			schemaFile: "15-owner-tables.sql", useDB: "pandora_owner",
			table: "ds_instance_lease", owner: "owner/internal/data/owner_repo.go",
			selectSQL: "SELECT owner_epoch FROM ds_instance_lease WHERE ds_pod = ? FOR UPDATE",
			insertSQL: "INSERT INTO ds_instance_lease (ds_pod, ds_uid, owner_epoch) VALUES (?, ?, 1)",
		},
	}
}

// TestRRGapLockDeadlockSurvey 对每张表跑两遍同一形状:默认 RR 与显式 RC。
// 断言只有一条:**RC 下不得死锁**。RR 下的结果只记录不断言 —— 它是普查数据
// (哪些表真的会炸),不是回归判据;把 RR 会炸写成断言会让这个文件在别人修好之后变红。
func TestRRGapLockDeadlockSurvey(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("PANDORA_TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("PANDORA_TEST_MYSQL_DSN 未设置,跳过 RR 间隙锁死锁普查")
	}

	var verdicts []surveyVerdict

	for _, c := range surveyCases() {
		c := c
		t.Run(c.table, func(t *testing.T) {
			db := newSurveyDB(t, dsn, c.schemaFile, c.useDB)
			rrDead, rrErr := probeShape(t, db, c, sql.LevelDefault)
			rcDead, rcErr := probeShape(t, db, c, sql.LevelReadCommitted)
			verdicts = append(verdicts, surveyVerdict{c.table, c.owner, rrDead, rcDead, rrErr, rcErr})
			if rcDead {
				t.Errorf("%s 在 READ COMMITTED 下仍然死锁,RC 不足以修复本表(需另行分析):%s", c.table, rcErr)
			}
		})
	}

	// ── 阳性对照校验:对照不亮 = 本次普查无结论,绝不能输出"安全"清单 ──────────
	//
	// 2026-08-11 实测:本探针**尚未能复现**该形状 —— 连已确诊的 friend_requests 都报"RR 安全"。
	// 观察到的原因是屏障不够强:5s 超时会让先到的事务提前插入并提交,后到者的 FOR UPDATE
	// 转而撞上已提交行的记录锁,整批被串行化而不是成环(该子测试耗时 42s = 多次超时叠加)。
	// 也就是说"没测出死锁"只说明**探针没造出并发形态**,不构成任何一张表安全的证据。
	//
	// 因此这里 fail-closed:对照不亮就整体 Skip。宁可输出"无结论",也不能输出会被当成
	// 结论引用的假阴性 —— 这批表的安全性正是要靠它来判定的。
	var control *surveyVerdict
	for i := range verdicts {
		if strings.HasPrefix(verdicts[i].table, "friend_requests") {
			control = &verdicts[i]
		}
	}
	if control == nil {
		t.Fatal("阳性对照用例缺失:普查必须带一个已确诊会死锁的表,否则阴性结果不可采信")
	}
	if !control.rrDeadlock {
		t.Skipf("阳性对照(%s)在 RR 下未复现死锁 → 本探针未能造出目标并发形态,"+
			"本次普查**无结论**,其余各表既不判安全也不判有问题。"+
			"已确证有效的方法是按域走真实 repo API 的端到端并发用例"+
			"(friend/mission 两例即由此抓获)。", control.table)
	}

	t.Log("── RR 间隙锁死锁普查结果 ───────────────────────────────────────────")
	for _, v := range verdicts {
		state := "RR 安全"
		if v.rrDeadlock {
			state = "**RR 死锁**"
		}
		rc := "RC 安全"
		if v.rcDeadlock {
			rc = "**RC 仍死锁**"
		}
		t.Logf("  %-28s %-12s %-12s  %s", v.table, state, rc, v.owner)
		if v.rrDeadlock {
			t.Logf("      RR 报错:%s", v.rrErr)
		}
	}
}

// probeShape 起 N 个并发事务,每个用**互不相同**的 key 走「未命中 FOR UPDATE → INSERT」。
// 返回是否观察到 1213。
//
// **SELECT 与 INSERT 之间必须有屏障**,否则这个普查会给出假阴性:
// 第一版没有屏障,每个事务 SELECT 完立刻 INSERT 再提交,重叠窗口只有几百微秒,
// 六张表全报"RR 安全"——而 friend/mission 在同样的引擎上是确定性死锁。差别就在窗口:
// 真实业务事务在取得间隙锁之后还要跑 5~10 条语句(探针、COUNT、其它表的读写)才轮到 INSERT,
// 那段时间里所有并发事务都攥着同一个间隙。屏障把这个窗口显式化:**所有事务都拿到间隙锁之后**
// 才开始插入,于是"这张表上这个写法会不会成环"得到的是能力判定,而不是一次赛跑的运气。
func probeShape(t *testing.T, db *sql.DB, c surveyCase, level sql.IsolationLevel) (bool, string) {
	t.Helper()
	const concurrent = 16
	// 每轮换一段 key 空间,避免前一轮(RR)插入的行让后一轮(RC)命中而改变形状。
	base := uint64(time.Now().UnixNano() % 1_000_000 * 1000)
	if level == sql.LevelReadCommitted {
		base += 500_000_000
	}

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		deadlock bool
		firstErr string
		arrived  int32
	)
	start := make(chan struct{})
	gate := make(chan struct{})
	var gateOnce sync.Once
	// releaseGate 在「全员都已取得间隙锁」或超时后放行。超时是必须的:一旦引擎判死并回滚了
	// 某些事务,它们不会再到达屏障,剩下的不能永远等下去(那会把死锁伪装成测试挂起)。
	releaseGate := func() { gateOnce.Do(func() { close(gate) }) }

	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			reported := false
			barrier := func() {
				if reported {
					return
				}
				reported = true
				if atomic.AddInt32(&arrived, 1) == concurrent {
					releaseGate()
				}
				select {
				case <-gate:
				case <-time.After(5 * time.Second):
					releaseGate()
				}
			}
			defer barrier() // 兜底:SELECT 阶段就出错的事务也要报到,别让同伴白等

			key := base + uint64(i)
			err := runShapeTx(context.Background(), db, level, c, key, barrier)
			if err == nil {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if strings.Contains(err.Error(), "Error 1213") || strings.Contains(err.Error(), "Deadlock found") {
				deadlock = true
			}
			if firstErr == "" {
				firstErr = err.Error()
			}
		}(i)
	}
	close(start)
	wg.Wait()
	return deadlock, firstErr
}

// runShapeTx 走一遍被测形状。barrier 在 SELECT 之后、INSERT 之前调用一次,
// 由调用方保证幂等(见 probeShape)。
func runShapeTx(ctx context.Context, db *sql.DB, level sql.IsolationLevel, c surveyCase, key uint64, barrier func()) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: level})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	selArgs := argsFor(c.selectSQL, key)
	var scrap sql.RawBytes
	row := tx.QueryRowContext(ctx, c.selectSQL, selArgs...)
	selErr := row.Scan(&scrap)

	barrier() // 无论 SELECT 命中与否都要报到:间隙锁在未命中时才产生,而那正是被测场景

	if selErr != nil && selErr != sql.ErrNoRows {
		return fmt.Errorf("select: %w", selErr)
	}
	insArgs := argsFor(c.insertSQL, key)
	if _, ierr := tx.ExecContext(ctx, c.insertSQL, insArgs...); ierr != nil {
		return fmt.Errorf("insert: %w", ierr)
	}
	return tx.Commit()
}

// argsFor 按语句里的 ? 个数把同一个 key 填进去(本普查的所有语句都只用一个实体标识)。
func argsFor(query string, key uint64) []any {
	n := strings.Count(query, "?")
	args := make([]any, n)
	for i := range args {
		args[i] = key
	}
	return args
}

func newSurveyDB(t *testing.T, dsn, schemaFile, useDB string) *sql.DB {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("解析 DSN: %v", err)
	}
	if strings.TrimSpace(cfg.DBName) != "" {
		t.Fatalf("PANDORA_TEST_MYSQL_DSN 禁止携带库名(拿到 %q)", cfg.DBName)
	}
	cfg.MultiStatements = true
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second

	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开管理连接: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("已设 PANDORA_TEST_MYSQL_DSN 但 MySQL 不可达(不允许静默 PASS): %v", err)
	}

	seed := make([]byte, 6)
	if _, rerr := rand.Read(seed); rerr != nil {
		t.Fatalf("随机后缀: %v", rerr)
	}
	dbName := fmt.Sprintf("pandora_gaplock_survey_%d_%s", time.Now().UnixNano(), hex.EncodeToString(seed))
	if !surveyDBPattern.MatchString(dbName) {
		t.Fatalf("临时库名未通过安全校验: %q", dbName)
	}
	if _, err := admin.ExecContext(ctx,
		"CREATE DATABASE `"+dbName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		_ = admin.Close()
		t.Fatalf("创建临时库 %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dcancel()
		if surveyDBPattern.MatchString(dbName) {
			if _, err := admin.ExecContext(dctx, "DROP DATABASE IF EXISTS `"+dbName+"`"); err != nil {
				t.Errorf("删除临时库 %s: %v", dbName, err)
			}
		}
		_ = admin.Close()
	})
	if _, err := admin.ExecContext(ctx, readSurveySchema(t, schemaFile, useDB, dbName)); err != nil {
		t.Fatalf("重放 %s: %v", schemaFile, err)
	}

	testCfg := cfg.Clone()
	testCfg.DBName = dbName
	db, err := sql.Open("mysql", testCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开临时库: %v", err)
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(32)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("连接临时库: %v", err)
	}
	return db
}

func readSurveySchema(t *testing.T, file, useDB, dbName string) string {
	t.Helper()
	path := filepath.Clean(filepath.Join("..", "..", "deploy", "mysql-init", file))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取发布 schema %s: %v", path, err)
	}
	schema := string(raw)
	needle := "USE `" + useDB + "`;"
	if got := strings.Count(schema, needle); got != 1 {
		t.Fatalf("%s 的 USE 锚点数量异常: %d(期望 1)", file, got)
	}
	return strings.Replace(schema, needle, "USE `"+dbName+"`;", 1)
}
