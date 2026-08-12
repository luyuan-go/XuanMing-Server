// leaderboard_reward_mysql_test.go — 发奖状态机的真实 MySQL 集成回归(2026-08-11 补)。
//
// 本文件锁的是 INC-20260811-001 §6 同型扫描修掉的那条缺陷:`MarkReward` 的**失败标记**
// 必须带 `status <> GRANTED` 守卫。此前同包只有 buildMarkRewardSQL 的**字符串断言** ——
// 它能证明"SQL 里有那个条件",证明不了"引擎执行后 GRANTED 行没被打回 FAILED"。
// 两者的差距正是这条缺陷的藏身处:改错任一处(条件写反、参数顺序错、状态常量传错)
// 字符串断言都可能照样绿。
//
// 为什么这条必须守住(缺陷原文):多副本补扫是刻意允许的,正确性靠下游幂等键;
// 但无条件 UPDATE 会让 A 副本发放成功写 GRANTED 的同时,B 副本因下游瞬时不可用把同一行
// 打回 FAILED —— 已发放的行重回补发工作集每轮重放,"陈年 FAILED = 发放链有 bug"的审计
// 信号被淹没,且下游幂等记录过 90 天保留期后重放就从"幂等吸收"变成**真重复发放**。
//
// 门控沿用仓库约定:PANDORA_TEST_MYSQL_DSN 必须不带库名,测试自建随机临时库并重放
// deploy/mysql-init/10-leaderboard-tables.sql 原文;未设置 → Skip,不可达 → 硬失败。
//
//	$env:PANDORA_TEST_MYSQL_DSN='root:<pw>@tcp(127.0.0.1:3307)/?parseTime=true&loc=UTC&charset=utf8mb4'
//	go test ./internal/data/ -count=1 -run MarkReward -v
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
	"sync"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"
)

const leaderboardITTimeout = 30 * time.Second

var leaderboardTestDBPattern = regexp.MustCompile(`^pandora_leaderboard_it_[0-9]+_[0-9a-f]{12}$`)

// newLeaderboardTestDB 建随机临时库并重放发布 schema(不在测试里另抄一份 DDL:
// 被断言的 uk_grant_idem / status 默认值就来自那份文件,抄一份等于把断言建在副本上)。
func newLeaderboardTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := strings.TrimSpace(os.Getenv("PANDORA_TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("PANDORA_TEST_MYSQL_DSN 未设置,跳过 leaderboard 真实 MySQL 集成测试")
	}
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("解析 PANDORA_TEST_MYSQL_DSN: %v", err)
	}
	if strings.TrimSpace(cfg.DBName) != "" {
		t.Fatalf("PANDORA_TEST_MYSQL_DSN 禁止携带库名(拿到 %q):测试要自建临时库,不能碰业务库", cfg.DBName)
	}
	cfg.MultiStatements = true
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second

	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开 MySQL 管理连接: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), leaderboardITTimeout)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		_ = admin.Close()
		t.Fatalf("已设 PANDORA_TEST_MYSQL_DSN 但 MySQL 不可达(不允许静默 PASS): %v", err)
	}

	seed := make([]byte, 6)
	if _, rerr := rand.Read(seed); rerr != nil {
		t.Fatalf("生成临时库随机后缀: %v", rerr)
	}
	dbName := fmt.Sprintf("pandora_leaderboard_it_%d_%s", time.Now().UnixNano(), hex.EncodeToString(seed))
	if !leaderboardTestDBPattern.MatchString(dbName) { // 二次校验,永不误删业务库
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
		if leaderboardTestDBPattern.MatchString(dbName) {
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
	if _, err := admin.ExecContext(ctx, readLeaderboardSchema(t, dbName)); err != nil {
		t.Fatalf("重放 10-leaderboard-tables.sql: %v", err)
	}

	testCfg := cfg.Clone()
	testCfg.DBName = dbName
	db, err := sql.Open("mysql", testCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开随机临时库 %s: %v", dbName, err)
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(16)
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("连接随机临时库 %s: %v", dbName, err)
	}
	return db
}

func readLeaderboardSchema(t *testing.T, dbName string) string {
	t.Helper()
	_, self, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("无法定位测试文件路径")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(self), "..", "..", "..", "..", "..",
		"deploy", "mysql-init", "10-leaderboard-tables.sql"))
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取发布 schema %s: %v", path, err)
	}
	schema := string(raw)
	const needle = "USE `pandora_leaderboard`;"
	if got := strings.Count(schema, needle); got != 1 {
		t.Fatalf("10-leaderboard-tables.sql 的 USE 锚点数量异常: %d(期望 1)", got)
	}
	return strings.Replace(schema, needle, "USE `"+dbName+"`;", 1)
}

func seedRewardLog(t *testing.T, repo *MySQLLeaderboardRepo, key string, nowMs int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), leaderboardITTimeout)
	defer cancel()
	already, err := repo.ClaimReward(ctx, &RewardLogRecord{
		SettlementID:  9001,
		EntityID:      7001,
		Rank:          1,
		GrantIdemKey:  key,
		Status:        RewardPending,
		RewardPayload: []byte{0x0a, 0x02, 0x10, 0x01},
		CreatedAtMs:   nowMs,
		UpdatedAtMs:   nowMs,
	})
	if err != nil {
		t.Fatalf("登记发奖行 %s: %v", key, err)
	}
	if already {
		t.Fatalf("首次登记发奖行 %s 不应命中幂等", key)
	}
}

func readRewardStatus(t *testing.T, db *sql.DB, key string) (int8, int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), leaderboardITTimeout)
	defer cancel()
	var (
		status    int8
		updatedMs int64
	)
	if err := db.QueryRowContext(ctx,
		`SELECT status, updated_at_ms FROM leaderboard_reward_log WHERE grant_idempotency_key = ?`,
		key).Scan(&status, &updatedMs); err != nil {
		t.Fatalf("读取发奖行 %s: %v", key, err)
	}
	return status, updatedMs
}

// TestMarkRewardFailedNeverOverwritesGranted_MySQL —— 终态守卫:GRANTED 行不可被打回 FAILED。
func TestMarkRewardFailedNeverOverwritesGranted_MySQL(t *testing.T) {
	db := newLeaderboardTestDB(t)
	repo := NewMySQLLeaderboardRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), leaderboardITTimeout)
	defer cancel()

	const key = "lb:9001:7001"
	const baseMs = int64(1_786_000_000_000)
	seedRewardLog(t, repo, key, baseMs)

	if err := repo.MarkReward(ctx, key, RewardGranted, baseMs+1_000); err != nil {
		t.Fatalf("标记 GRANTED: %v", err)
	}
	if status, updated := readRewardStatus(t, db, key); status != RewardGranted || updated != baseMs+1_000 {
		t.Fatalf("GRANTED 后 status=%d updated=%d, want %d/%d", status, updated, RewardGranted, baseMs+1_000)
	}

	// 另一副本因下游瞬时不可用把同一行打回 FAILED —— 必须被守卫挡掉,且连 updated_at_ms
	// 都不能动(动了会把这行推进补发扫描窗口,即使 status 没变也会被重放)。
	if err := repo.MarkReward(ctx, key, RewardFailed, baseMs+2_000); err != nil {
		t.Fatalf("标记 FAILED(应被守卫静默挡掉,不报错): %v", err)
	}
	status, updated := readRewardStatus(t, db, key)
	if status != RewardGranted {
		t.Fatalf("GRANTED 被打回 status=%d(失败标记缺少 `status <> GRANTED` 守卫)", status)
	}
	if updated != baseMs+1_000 {
		t.Fatalf("被守卫挡掉的更新改了 updated_at_ms=%d, want %d", updated, baseMs+1_000)
	}

	// 已发放的行不得再出现在补发工作集里。
	pending, err := repo.ListUngrantedRewards(ctx, baseMs+10_000, 100)
	if err != nil {
		t.Fatalf("列出待补发: %v", err)
	}
	for _, rec := range pending {
		if rec.GrantIdemKey == key {
			t.Fatalf("已 GRANTED 的 %s 仍在补发工作集里", key)
		}
	}
}

// TestMarkRewardPendingToFailedThenGranted_MySQL —— 守卫只挡"从 GRANTED 退回",
// 不得挡住正常的 PENDING → FAILED → 补发成功 → GRANTED 推进。
func TestMarkRewardPendingToFailedThenGranted_MySQL(t *testing.T) {
	db := newLeaderboardTestDB(t)
	repo := NewMySQLLeaderboardRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), leaderboardITTimeout)
	defer cancel()

	const key = "lb:9001:7002"
	const baseMs = int64(1_786_000_000_000)
	seedRewardLog(t, repo, key, baseMs)

	if err := repo.MarkReward(ctx, key, RewardFailed, baseMs+1_000); err != nil {
		t.Fatalf("标记 FAILED: %v", err)
	}
	if status, _ := readRewardStatus(t, db, key); status != RewardFailed {
		t.Fatalf("PENDING → FAILED 被误挡, status=%d", status)
	}

	pending, err := repo.ListUngrantedRewards(ctx, baseMs+10_000, 100)
	if err != nil {
		t.Fatalf("列出待补发: %v", err)
	}
	found := false
	for _, rec := range pending {
		if rec.GrantIdemKey == key {
			found = true
			if len(rec.RewardPayload) == 0 {
				t.Fatal("补发工作集里的 reward_pb 为空:重放路径拿不到发奖入参")
			}
		}
	}
	if !found {
		t.Fatalf("FAILED 的 %s 不在补发工作集里", key)
	}

	if err := repo.MarkReward(ctx, key, RewardGranted, baseMs+2_000); err != nil {
		t.Fatalf("补发成功后标记 GRANTED: %v", err)
	}
	if status, updated := readRewardStatus(t, db, key); status != RewardGranted || updated != baseMs+2_000 {
		t.Fatalf("补发成功后 status=%d updated=%d, want %d/%d", status, updated, RewardGranted, baseMs+2_000)
	}
}

// TestMarkRewardConcurrentGrantAndFailKeepsGranted_MySQL —— 两个副本同时标记(一成一败),
// 无论交错顺序,终态必须是 GRANTED:先 GRANTED 则 FAILED 被守卫挡掉;先 FAILED 则随后
// GRANTED 覆盖(成功标记是无条件的,终态推进幂等)。
func TestMarkRewardConcurrentGrantAndFailKeepsGranted_MySQL(t *testing.T) {
	db := newLeaderboardTestDB(t)
	repo := NewMySQLLeaderboardRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), leaderboardITTimeout)
	defer cancel()

	const baseMs = int64(1_786_000_000_000)
	// 多轮以覆盖两种交错顺序(单轮可能每次都落到同一侧)。
	for round := 0; round < 8; round++ {
		key := fmt.Sprintf("lb:9002:%d", round)
		seedRewardLog(t, repo, key, baseMs)

		var wg sync.WaitGroup
		errs := make([]error, 2)
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			errs[0] = repo.MarkReward(ctx, key, RewardGranted, baseMs+1_000)
		}()
		go func() {
			defer wg.Done()
			<-start
			errs[1] = repo.MarkReward(ctx, key, RewardFailed, baseMs+1_001)
		}()
		close(start)
		wg.Wait()

		for i, err := range errs {
			if err != nil {
				t.Fatalf("round=%d 标记 %d 出错: %v", round, i, err)
			}
		}
		if status, _ := readRewardStatus(t, db, key); status != RewardGranted {
			t.Fatalf("round=%d 并发标记终态 status=%d, want GRANTED(%d)", round, status, RewardGranted)
		}
	}
}
