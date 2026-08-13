// guild_repo_mysql_test.go — guild / group 数据层的真实 MySQL 双连接并发集成测试。
//
// 执行路径:本文件**不带 build tag**,随默认 `go test ./...` 一起编译执行,只由环境变量
// PANDORA_TEST_MYSQL_DSN 门控:
//   - 未设该变量 → 每个用例 t.Skip(本地 / 无 DB 的 CI 不失败,但 internal/data 不再是 [no test files])。
//   - 已设该变量但库不可达 / 建库失败 → t.Fatal(硬失败,绝不 false-green;六审 P1:提供 DSN 却连不上仍 PASS)。
//
// 因此只要 CI 注入 PANDORA_TEST_MYSQL_DSN,这些真实并发断言就进入强制执行路径。
//
// 本地运行(需已起 deploy/docker-compose.dev.yml 的 mysql,或任意 MySQL 8):
//
//	$env:PANDORA_TEST_MYSQL_DSN = 'root:pandora@tcp(127.0.0.1:3306)/'
//	go test -run TestMySQL ./internal/data/...
//
// DSN 末尾不带库名:测试自建独立临时库 pandora_guild_it_<rand>,跑完 DROP,绝不碰业务库。
//
// 覆盖 reviewer「验证 P1」四项,且刻意做成能区分「旧漏洞实现 vs 现修复」的确定性交错:
//   - 权限撤销 RR 快照(五审 P1):用外部连接持有 guilds 父行锁,先让 ApproveJoin 建立旧 RR 快照
//     并阻塞在父锁上,再降级审批人并提交 —— 普通读实现会读快照旧角色误放行,FOR UPDATE 实现读到
//     最新已提交角色而拒绝。旧实现必挂,新实现必过。
//   - Approve × Disband(四审 P1):同样用外部锁制造「Approve 已建快照、公会随后被解散」的确定性
//     交错,断言 ApproveJoin 在父锁下复读公会已消失(ErrGuildNotFound),不复活成员到已删公会;
//     另有高频真实并发版,严格断言错误码不得泄漏 ErrInternal(死锁)。
//   - 死锁重试:反序成员集并发建群,断言两群都成功(排序加锁序 + 1213/1205 重试)。
//   - 最终一致性:并发审批 + 退会后,断言每步都成功且 guilds.member_count == 实际行数、恰 1 会长。
package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	drivermysql "github.com/go-sql-driver/mysql"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// ddlStatements 是公会 / 群相关表的建表语句(取自 deploy/mysql-init/11-guild-tables.sql,去掉 USE)。
var ddlStatements = []string{
	`CREATE TABLE guilds (
		guild_id     BIGINT UNSIGNED NOT NULL,
		name         VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
		leader_id    BIGINT UNSIGNED NOT NULL,
		member_count INT             NOT NULL DEFAULT 1,
		pending_request_count INT    NOT NULL DEFAULT 0,
		max_members  INT             NOT NULL DEFAULT 100,
		created_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (guild_id),
		UNIQUE KEY uk_name (name)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	`CREATE TABLE guild_members (
		player_id BIGINT UNSIGNED NOT NULL,
		guild_id  BIGINT UNSIGNED NOT NULL,
		role      TINYINT         NOT NULL DEFAULT 3,
		joined_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (player_id),
		KEY idx_guild_role (guild_id, role)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	`CREATE TABLE guild_join_requests (
		request_id BIGINT UNSIGNED NOT NULL,
		guild_id   BIGINT UNSIGNED NOT NULL,
		player_id  BIGINT UNSIGNED NOT NULL,
		status     TINYINT         NOT NULL DEFAULT 1,
		created_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		PRIMARY KEY (request_id),
		UNIQUE KEY uk_guild_player (guild_id, player_id),
		KEY idx_guild_status (guild_id, status)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	`CREATE TABLE chat_groups (
		group_id     BIGINT UNSIGNED NOT NULL,
		name         VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
		owner_id     BIGINT UNSIGNED NOT NULL,
		member_count INT             NOT NULL DEFAULT 1,
		max_members  INT             NOT NULL DEFAULT 50,
		created_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (group_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	`CREATE TABLE chat_group_members (
		group_id  BIGINT UNSIGNED NOT NULL,
		player_id BIGINT UNSIGNED NOT NULL,
		role      TINYINT         NOT NULL DEFAULT 2,
		joined_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (group_id, player_id),
		KEY idx_player (player_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
	`CREATE TABLE player_group_counts (
		player_id   BIGINT UNSIGNED NOT NULL,
		group_count INT             NOT NULL DEFAULT 0,
		PRIMARY KEY (player_id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
}

// ddlTimeout 是建库 / 建表 / ALTER 这类 DDL 的兜底超时(只防挂死,不做延迟断言)。
// 给得比读写用例宽,是因为 DDL 天然是秒级:TiDB 的 online DDL 要等 schema lease
// 全网重载,而 CI 把 MySQL/TiDB/TiKV/Kafka 挤在同一台机器上跑,整机争用时实测能
// 把一条 DROP COLUMN 拖到 5s 以上(backend-dev #28 即因此误报红灯)。
const ddlTimeout = 30 * time.Second

// idGen 生成测试用递增 ID(避免与真实 snowflake 冲突;测试库独立,无所谓)。
var idGen uint64 = 1

func nextID() uint64 { return atomic.AddUint64(&idGen, 1) }

// setupMySQL 打开 DSN,创建独立临时库并建表,返回连该库的 *sql.DB 与清理函数。
// 门控语义(六审 P1 修正):
//   - 未设 PANDORA_TEST_MYSQL_DSN → Skip(无 DB 环境合法跳过)。
//   - 已设 DSN 但连不上 / 建库失败 → Fatal(硬失败,杜绝「给了 DSN 却连不上也 PASS」的 false-green)。
func setupMySQL(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	dsn := os.Getenv("PANDORA_TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("跳过真实 MySQL 集成测试:未设 PANDORA_TEST_MYSQL_DSN(例如 root:pandora@tcp(127.0.0.1:3306)/)")
	}
	return openTempDB(t, dsn)
}

// openTempDB 用给定 DSN 建独立临时库、建表,返回连该库的 *sql.DB 与清理函数。
// DSN 已给出却连不上 / 建库失败 → Fatal(硬失败,杜绝「给了 DSN 却连不上也 PASS」的 false-green)。
// MySQL 与 TiDB 均走 MySQL 协议,同一套 DDL / 建库流程通用。
func openTempDB(t *testing.T, dsn string) (*sql.DB, func()) {
	t.Helper()
	cfg, err := drivermysql.ParseDSN(dsn)
	if err != nil {
		t.Fatalf("解析 DSN 失败:%v", err)
	}

	// 1. 用无库名连接建临时库。DSN 已给出却连不上 → 硬失败。
	adminCfg := *cfg
	adminCfg.DBName = ""
	admin, err := sql.Open("mysql", adminCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开管理连接失败:%v", err)
	}
	defer func() { _ = admin.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("已给出测试 DSN 但无法连接(不允许静默 PASS):%v", err)
	}
	// 建库 / 建表走 ddlTimeout,与连通性检查分开计时:DDL 是秒级操作(TiDB 还要等
	// schema lease 重载),和「连不上」不是同一类故障,共用一个紧预算只会把整机
	// 争用误报成建表失败。
	ddlCtx, ddlCancel := context.WithTimeout(context.Background(), ddlTimeout)
	defer ddlCancel()
	dbName := fmt.Sprintf("pandora_guild_it_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ddlCtx, "CREATE DATABASE `"+dbName+"` DEFAULT CHARSET utf8mb4"); err != nil {
		t.Fatalf("建临时库失败:%v", err)
	}

	// 2. 连到临时库建表。
	testCfg := *cfg
	testCfg.DBName = dbName
	db, err := sql.Open("mysql", testCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开测试库连接失败:%v", err)
	}
	db.SetMaxOpenConns(16) // 需要多连接才能真正并发出行锁竞争
	for _, stmt := range ddlStatements {
		if _, err := db.ExecContext(ddlCtx, stmt); err != nil {
			_ = db.Close()
			t.Fatalf("建表失败:%v\nSQL: %s", err, stmt)
		}
	}

	cleanup := func() {
		_ = db.Close()
		a, err := sql.Open("mysql", adminCfg.FormatDSN())
		if err == nil {
			_, _ = a.Exec("DROP DATABASE IF EXISTS `" + dbName + "`")
			_ = a.Close()
		}
	}
	return db, cleanup
}

// backendCase 描述一个待测存储后端(MySQL 或 TiDB)。
type backendCase struct {
	name string
	db   *sql.DB
}

// forEachBackend 对每个已配置的后端各跑一遍 fn(以子测试隔离):
//   - MySQL:PANDORA_TEST_MYSQL_DSN(未设则该子测试 Skip);
//   - TiDB :PANDORA_TEST_TIDB_DSN(未设则该子测试 Skip)。
//
// 上限并发测试(pending 申请 / 所在群)必须在 MySQL 与 TiDB 双后端各验一遍:计数列 / 计数表方案
// 取代了依赖间隙锁的 COUNT(*)...FOR UPDATE,须证明在 TiDB(无间隙锁)下上限同样拦得住并发幻读
// (decision-revisit-guild-scaling.md §3.5)。CI 注入哪个 DSN 就强制跑哪个后端,不给静默漏测。
func forEachBackend(t *testing.T, fn func(t *testing.T, bc backendCase)) {
	t.Helper()
	backends := []struct {
		name string
		env  string
	}{
		{"mysql", "PANDORA_TEST_MYSQL_DSN"},
		{"tidb", "PANDORA_TEST_TIDB_DSN"},
	}
	for _, b := range backends {
		b := b
		t.Run(b.name, func(t *testing.T) {
			dsn := os.Getenv(b.env)
			if dsn == "" {
				t.Skipf("跳过 %s 后端:未设 %s", b.name, b.env)
			}
			db, cleanup := openTempDB(t, dsn)
			defer cleanup()
			fn(t, backendCase{name: b.name, db: db})
		})
	}
}

// waitForLockWaiters 轮询锁视图,直到出现 want 个被阻塞的锁请求(即目标 goroutine 已阻塞在
// 父行锁上、旧 RR 快照已锚定)。用于制造确定性交错,取代脆弱的固定 sleep。同时查
// performance_schema.data_lock_waits(直接计被阻塞的锁请求)与 information_schema.INNODB_TRX
// 的 LOCK WAIT 事务,任一达到阈值即返回,增强跨版本 / 权限差异下的鲁棒性;两个视图都读不到
// (权限不足)才退化为一次性 sleep 兜底,不让测试卡死。
func waitForLockWaiters(t *testing.T, db *sql.DB, want int) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		var dlw, trx int
		errDlw := db.QueryRow(
			`SELECT COUNT(*) FROM performance_schema.data_lock_waits`).Scan(&dlw)
		errTrx := db.QueryRow(
			`SELECT COUNT(*) FROM information_schema.INNODB_TRX WHERE trx_state = 'LOCK WAIT'`).Scan(&trx)
		if errDlw != nil && errTrx != nil {
			// 两个锁视图都读不到(权限不足):退化为固定等待,不让测试卡死。
			time.Sleep(800 * time.Millisecond)
			return
		}
		if dlw >= want || trx >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("等待 %d 个被阻塞锁请求超时(交错未按预期建立)", want)
}

// createGuildWithLeader 建公会并返回 guildID / leaderID。
func createGuildWithLeader(t *testing.T, ctx context.Context, r *MySQLGuildRepo, maxMembers int) (uint64, uint64) {
	t.Helper()
	guildID, leaderID := nextID(), nextID()
	name := fmt.Sprintf("g-%d", guildID)
	if err := r.CreateGuild(ctx, guildID, leaderID, name, maxMembers); err != nil {
		t.Fatalf("CreateGuild: %v", err)
	}
	return guildID, leaderID
}

// addMemberViaApprove 让 applicant 申请并由 approver 通过,成为成员。
func addMemberViaApprove(t *testing.T, ctx context.Context, r *MySQLGuildRepo, guildID, approverID, applicantID uint64, maxMembers int) {
	t.Helper()
	reqID := nextID()
	if _, _, err := r.CreateJoinRequest(ctx, reqID, guildID, applicantID, 0); err != nil {
		t.Fatalf("CreateJoinRequest: %v", err)
	}
	approved, err := r.ApproveJoin(ctx, reqID, approverID, maxMembers)
	if err != nil {
		t.Fatalf("ApproveJoin: %v", err)
	}
	if !approved {
		t.Fatalf("期望 approved=true")
	}
}

// TestMySQLApproveRRSnapshotRevokedOfficerDeterministic 确定性复现「五审 P1」RR 快照越权:
//
//	外部事务 holdTx 持有 guilds 父行锁 → ApproveJoin(officer) 先未锁读 request 建立 RR 快照
//	(此刻 officer 仍是 officer)→ 阻塞在 guilds FOR UPDATE → holdTx 把 officer 降为 member 并提交、
//	释放父锁 → ApproveJoin 拿到父锁后复读审批人角色。
//
// 旧实现(普通 SELECT 读 approver 角色)会命中 RR 快照里的旧 officer 角色 → 误放行(测试挂);
// 现实现(FOR UPDATE 锁定读)绕过快照读到最新已提交的 member 角色 → ErrGuildNoPermission(测试过)。
func TestMySQLApproveRRSnapshotRevokedOfficerDeterministic(t *testing.T) {
	db, cleanup := setupMySQL(t)
	defer cleanup()
	ctx := context.Background()
	r := NewMySQLGuildRepo(db)

	const maxMembers = 100
	guildID, leaderID := createGuildWithLeader(t, ctx, r, maxMembers)

	// 官员 O 与挂起申请人 A。
	officerID := nextID()
	addMemberViaApprove(t, ctx, r, guildID, leaderID, officerID, maxMembers)
	if err := r.SetRole(ctx, guildID, leaderID, officerID, GuildRoleOfficer); err != nil {
		t.Fatalf("SetRole officer: %v", err)
	}
	applicantID := nextID()
	reqID := nextID()
	if _, _, err := r.CreateJoinRequest(ctx, reqID, guildID, applicantID, 0); err != nil {
		t.Fatalf("CreateJoinRequest: %v", err)
	}

	// holdTx 持有 guilds 父行锁,逼停后续 ApproveJoin,使其在降级前锚定 RR 快照。
	holdTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin holdTx: %v", err)
	}
	var lockedGuild uint64
	if err := holdTx.QueryRowContext(ctx,
		`SELECT guild_id FROM guilds WHERE guild_id = ? FOR UPDATE`, guildID).Scan(&lockedGuild); err != nil {
		_ = holdTx.Rollback()
		t.Fatalf("holdTx 锁 guilds: %v", err)
	}

	type approveRes struct {
		approved bool
		err      error
	}
	resCh := make(chan approveRes, 1)
	go func() {
		approved, aerr := r.ApproveJoin(ctx, reqID, officerID, maxMembers)
		resCh <- approveRes{approved, aerr}
	}()

	// 等 ApproveJoin 阻塞在 guilds 父锁(此时它的 RR 快照已锚定,officer 仍是 officer)。
	waitForLockWaiters(t, db, 1)

	// 在持锁事务里把 officer 降级为普通成员并提交,释放父锁。
	if _, err := holdTx.ExecContext(ctx,
		`UPDATE guild_members SET role = ? WHERE guild_id = ? AND player_id = ?`,
		GuildRoleMember, guildID, officerID); err != nil {
		_ = holdTx.Rollback()
		t.Fatalf("holdTx 降级 officer: %v", err)
	}
	if err := holdTx.Commit(); err != nil {
		t.Fatalf("holdTx commit: %v", err)
	}

	res := <-resCh
	if code := errcode.As(res.err); code != errcode.ErrGuildNoPermission {
		t.Fatalf("越权审批必须被拒:期望 ErrGuildNoPermission(9405),得到 code=%d err=%v approved=%v",
			code, res.err, res.approved)
	}
	if res.approved {
		t.Fatalf("被降级官员在 RR 快照下不得放行审批(false-green 说明读了旧快照)")
	}
	if _, ok, _ := r.GetMember(ctx, applicantID); ok {
		t.Fatalf("申请人不应因越权审批入会")
	}
}

// TestMySQLApproveAfterDisbandCommittedDeterministic 确定性复现「Approve 已建快照、公会随后被解散」:
//
//	holdTx 持有 guilds 父锁 → ApproveJoin 建立 RR 快照(公会存在、申请 pending)→ 阻塞在父锁 →
//	holdTx 执行 Disband 的删表效果(删成员/申请/公会)并提交 → ApproveJoin 拿锁后在父锁下复读公会。
//
// 现实现用 FOR UPDATE 复读 guilds.member_count → 命中已删除 → ErrGuildNotFound、不插成员;
// 若改回读快照 member_count 就会把成员复活进已删公会(资源泄漏 / 悬空)。
func TestMySQLApproveAfterDisbandCommittedDeterministic(t *testing.T) {
	db, cleanup := setupMySQL(t)
	defer cleanup()
	ctx := context.Background()
	r := NewMySQLGuildRepo(db)

	const maxMembers = 100
	guildID, leaderID := createGuildWithLeader(t, ctx, r, maxMembers)
	applicantID := nextID()
	reqID := nextID()
	if _, _, err := r.CreateJoinRequest(ctx, reqID, guildID, applicantID, 0); err != nil {
		t.Fatalf("CreateJoinRequest: %v", err)
	}

	holdTx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin holdTx: %v", err)
	}
	var lockedGuild uint64
	if err := holdTx.QueryRowContext(ctx,
		`SELECT guild_id FROM guilds WHERE guild_id = ? FOR UPDATE`, guildID).Scan(&lockedGuild); err != nil {
		_ = holdTx.Rollback()
		t.Fatalf("holdTx 锁 guilds: %v", err)
	}

	type approveRes struct {
		approved bool
		err      error
	}
	resCh := make(chan approveRes, 1)
	go func() {
		approved, aerr := r.ApproveJoin(ctx, reqID, leaderID, maxMembers)
		resCh <- approveRes{approved, aerr}
	}()

	waitForLockWaiters(t, db, 1)

	// 在持锁事务里执行 DisbandGuild 的等价删表效果并提交(忠实还原解散胜出的落库结果)。
	for _, stmt := range []string{
		`DELETE FROM guild_members WHERE guild_id = ?`,
		`DELETE FROM guild_join_requests WHERE guild_id = ?`,
		`DELETE FROM guilds WHERE guild_id = ?`,
	} {
		if _, err := holdTx.ExecContext(ctx, stmt, guildID); err != nil {
			_ = holdTx.Rollback()
			t.Fatalf("holdTx 解散删表 %q: %v", stmt, err)
		}
	}
	if err := holdTx.Commit(); err != nil {
		t.Fatalf("holdTx commit: %v", err)
	}

	res := <-resCh
	if code := errcode.As(res.err); code != errcode.ErrGuildNotFound {
		t.Fatalf("公会已解散后审批必须拒:期望 ErrGuildNotFound(9401),得到 code=%d err=%v approved=%v",
			code, res.err, res.approved)
	}
	if res.approved {
		t.Fatalf("公会已删,审批不得放行(否则成员被复活进已删公会)")
	}
	if _, ok, _ := r.GetMember(ctx, applicantID); ok {
		t.Fatalf("公会已解散,申请人不得被复活为成员")
	}
	if _, ok, _ := r.GetGuild(ctx, guildID); ok {
		t.Fatalf("公会应已被解散")
	}
}

// TestMySQLConcurrentApproveDisbandInvariants 真实并发跑 ApproveJoin × DisbandGuild(非外部锁模拟),
// 严格断言:①DisbandGuild(会长解散自己公会)必成功;②ApproveJoin 只能是成功、或明确业务错误
// (ErrGuildNotFound / ErrGuildRequestInvalid),绝不泄漏 ErrInternal(死锁未被重试消化);③终态不变量:
// 公会已删、申请人不残留为成员、无孤儿申请。
func TestMySQLConcurrentApproveDisbandInvariants(t *testing.T) {
	db, cleanup := setupMySQL(t)
	defer cleanup()
	ctx := context.Background()
	r := NewMySQLGuildRepo(db)

	const maxMembers = 100
	const iterations = 80
	for i := 0; i < iterations; i++ {
		guildID, leaderID := createGuildWithLeader(t, ctx, r, maxMembers)
		applicantID := nextID()
		reqID := nextID()
		if _, _, err := r.CreateJoinRequest(ctx, reqID, guildID, applicantID, 0); err != nil {
			t.Fatalf("iter %d CreateJoinRequest: %v", i, err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		var approveErr, disbandErr error
		var approved bool
		go func() {
			defer wg.Done()
			approved, approveErr = r.ApproveJoin(ctx, reqID, leaderID, maxMembers)
		}()
		go func() {
			defer wg.Done()
			_, disbandErr = r.DisbandGuild(ctx, guildID, leaderID)
		}()
		wg.Wait()

		// ① 会长解散自己的公会,无并发者改 leader → 必成功。
		if disbandErr != nil {
			t.Fatalf("iter %d DisbandGuild 应成功:err=%v", i, disbandErr)
		}
		// ② ApproveJoin 结果必须是可解释的:要么成功(抢在解散前入会,随后被解散一并删除),
		//    要么明确业务错误;绝不能是 ErrInternal(死锁/锁超时泄漏)或其它未知码。
		if approveErr != nil {
			switch code := errcode.As(approveErr); code {
			case errcode.ErrGuildNotFound, errcode.ErrGuildRequestInvalid:
				// 解散先赢,合法。
			default:
				t.Fatalf("iter %d ApproveJoin 非预期错误码 %d(疑似死锁泄漏):%v", i, code, approveErr)
			}
		}
		// ③ 终态:公会已删、申请人不得残留为成员、无孤儿申请。
		if _, ok, err := r.GetGuild(ctx, guildID); err != nil || ok {
			t.Fatalf("iter %d 公会应已解散:ok=%v err=%v", i, ok, err)
		}
		if _, ok, err := r.GetMember(ctx, applicantID); err != nil || ok {
			t.Fatalf("iter %d 申请人不得残留为成员(approved=%v):ok=%v err=%v", i, approved, ok, err)
		}
		if _, ok, err := r.GetRequest(ctx, reqID); err != nil || ok {
			t.Fatalf("iter %d 不得残留孤儿申请:ok=%v err=%v", i, ok, err)
		}
	}
}

// TestMySQLDisbandVsCreateRequestNoOrphan 并发解散 + 建申请:无论交错如何,都不得留下指向
// 已删除公会的孤儿 pending 申请(CreateJoinRequest 首步锁 guilds 父行保证)。
// 六审 P1 修正:显式断言 DisbandGuild 错误(会长解散自己公会必成功),不再吞掉。
func TestMySQLDisbandVsCreateRequestNoOrphan(t *testing.T) {
	db, cleanup := setupMySQL(t)
	defer cleanup()
	ctx := context.Background()
	r := NewMySQLGuildRepo(db)

	const iterations = 80
	for i := 0; i < iterations; i++ {
		guildID, leaderID := createGuildWithLeader(t, ctx, r, 100)
		applicantID := nextID()
		reqID := nextID()

		var wg sync.WaitGroup
		wg.Add(2)
		var reqErr, disbandErr error
		go func() {
			defer wg.Done()
			_, _, reqErr = r.CreateJoinRequest(ctx, reqID, guildID, applicantID, 0)
		}()
		go func() {
			defer wg.Done()
			_, disbandErr = r.DisbandGuild(ctx, guildID, leaderID)
		}()
		wg.Wait()

		// 会长解散自己公会必成功(不吞错)。
		if disbandErr != nil {
			t.Fatalf("iter %d DisbandGuild 应成功:err=%v", i, disbandErr)
		}
		// CreateJoinRequest 要么成功,要么明确 ErrGuildNotFound(公会已消失),不得泄漏 ErrInternal。
		if reqErr != nil {
			if code := errcode.As(reqErr); code != errcode.ErrGuildNotFound {
				t.Fatalf("iter %d CreateJoinRequest 非预期错误码 %d:%v", i, code, reqErr)
			}
		}
		// 不变量:公会已删 → 不得存在该 request(孤儿)。
		_, guildExists, err := r.GetGuild(ctx, guildID)
		if err != nil {
			t.Fatalf("iter %d GetGuild: %v", i, err)
		}
		_, reqExists, err := r.GetRequest(ctx, reqID)
		if err != nil {
			t.Fatalf("iter %d GetRequest: %v", i, err)
		}
		if !guildExists && reqExists {
			t.Fatalf("iter %d 出现孤儿申请:guild 已删但 request %d 仍存在(reqErr=%v)", i, reqID, reqErr)
		}
	}
}

// TestMySQLConcurrentCreateGroupReversedNoDeadlock 反序成员集并发建群:排序加锁序 + 1213/1205
// 重试后,调用方不得再收到不可重试的 ErrInternal(死锁泄漏),两个群都应建成。
func TestMySQLConcurrentCreateGroupReversedNoDeadlock(t *testing.T) {
	db, cleanup := setupMySQL(t)
	defer cleanup()
	ctx := context.Background()
	gr := NewMySQLGroupRepo(db)

	// 两个固定玩家,反序作为成员;maxGroups 取大值以强制 reservePlayerGroupSlot 对
	// player_group_counts 计数行的 FOR UPDATE 加锁(仍会 reserve 并锁行,只是不触发上限拒绝),
	// 从而制造锁竞争验证排序加锁序消除 ABBA。
	pa, pb := nextID(), nextID()
	const maxMembers = 50
	const maxGroups = 100000
	const iterations = 80

	for i := 0; i < iterations; i++ {
		g1, g2 := nextID(), nextID()
		var wg sync.WaitGroup
		wg.Add(2)
		var e1, e2 error
		go func() {
			defer wg.Done()
			// owner=pa,成员 [pb] → 锁 {pa, pb}
			e1 = gr.CreateGroup(ctx, g1, pa, fmt.Sprintf("g1-%d", g1), []uint64{pb}, maxMembers, maxGroups)
		}()
		go func() {
			defer wg.Done()
			// owner=pb,成员 [pa] → 反序锁 {pb, pa};排序后与上者同序,无 ABBA
			e2 = gr.CreateGroup(ctx, g2, pb, fmt.Sprintf("g2-%d", g2), []uint64{pa}, maxMembers, maxGroups)
		}()
		wg.Wait()
		if e1 != nil {
			t.Fatalf("iter %d 建群1 失败(疑似死锁泄漏):code=%d err=%v", i, errcode.As(e1), e1)
		}
		if e2 != nil {
			t.Fatalf("iter %d 建群2 失败(疑似死锁泄漏):code=%d err=%v", i, errcode.As(e2), e2)
		}
	}
}

// TestMySQLConcurrentMembershipConsistency 并发审批 + 退会后,guilds.member_count 必须与实际
// guild_members 行数一致,且恰有一个会长。六审 P1 修正:严格断言每步审批/退会都成功(收集错误、
// 统计成功数),操作静默失败则测试挂,而非「全失败也通过」。
func TestMySQLConcurrentMembershipConsistency(t *testing.T) {
	db, cleanup := setupMySQL(t)
	defer cleanup()
	ctx := context.Background()
	r := NewMySQLGuildRepo(db)

	const maxMembers = 500
	guildID, leaderID := createGuildWithLeader(t, ctx, r, maxMembers)

	const n = 40
	applicants := make([]uint64, n)
	reqs := make([]uint64, n)
	for i := 0; i < n; i++ {
		applicants[i] = nextID()
		reqs[i] = nextID()
		if _, _, err := r.CreateJoinRequest(ctx, reqs[i], guildID, applicants[i], 0); err != nil {
			t.Fatalf("CreateJoinRequest %d: %v", i, err)
		}
	}

	// 并发审批全部申请(会长审批),逐个收集结果,要求全部成功。
	var wg sync.WaitGroup
	wg.Add(n)
	approveErrs := make([]error, n)
	approvedFlags := make([]bool, n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			approvedFlags[idx], approveErrs[idx] = r.ApproveJoin(ctx, reqs[idx], leaderID, maxMembers)
		}(i)
	}
	wg.Wait()
	for i := 0; i < n; i++ {
		if approveErrs[i] != nil {
			t.Fatalf("审批 %d 应成功:err=%v", i, approveErrs[i])
		}
		if !approvedFlags[i] {
			t.Fatalf("审批 %d 应 approved=true", i)
		}
	}

	// 并发退会前一半,收集结果,要求全部成功。
	half := n / 2
	wg.Add(half)
	removeErrs := make([]error, half)
	for i := 0; i < half; i++ {
		go func(idx int) {
			defer wg.Done()
			removeErrs[idx] = r.RemoveMember(ctx, guildID, applicants[idx])
		}(i)
	}
	wg.Wait()
	for i := 0; i < half; i++ {
		if removeErrs[i] != nil {
			t.Fatalf("退会 %d 应成功:err=%v", i, removeErrs[i])
		}
	}

	// 期望成员数 = 1 会长 + (n - half) 名剩余成员。
	wantCount := 1 + (n - half)
	g, ok, err := r.GetGuild(ctx, guildID)
	if err != nil || !ok {
		t.Fatalf("GetGuild: ok=%v err=%v", ok, err)
	}
	var actual int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM guild_members WHERE guild_id = ?`, guildID).Scan(&actual); err != nil {
		t.Fatalf("count members: %v", err)
	}
	if actual != wantCount {
		t.Fatalf("实际成员行数=%d,期望=%d", actual, wantCount)
	}
	if int(g.MemberCount) != actual {
		t.Fatalf("member_count 漂移:guilds.member_count=%d, 实际行数=%d", g.MemberCount, actual)
	}
	// 恰有一个会长,且就是 leaderID。
	var leaders int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM guild_members WHERE guild_id = ? AND role = ?`, guildID, GuildRoleLeader).Scan(&leaders); err != nil {
		t.Fatalf("count leaders: %v", err)
	}
	if leaders != 1 {
		t.Fatalf("会长数应为 1,实际 %d", leaders)
	}
	if _, ok, _ := r.GetMember(ctx, leaderID); !ok {
		t.Fatalf("会长成员行应存在")
	}
}

// TestConcurrentPendingRequestLimitEnforced 双后端(MySQL / TiDB)验证公会 pending 申请上限:
// 大量不同申请人并发对同一公会发起加入申请,maxPending 取小值;计数列(guilds.pending_request_count
// 在 guilds 父行 FOR UPDATE 下维护)必须精确拦停——成功数恰为 maxPending,超出的全部 ErrGuildRequestLimit,
// 且落库 pending 行数与计数列都恰等于 maxPending(§3.5:TiDB 无间隙锁,COUNT+FOR UPDATE 会漏,故改计数列)。
func TestConcurrentPendingRequestLimitEnforced(t *testing.T) {
	forEachBackend(t, func(t *testing.T, bc backendCase) {
		ctx := context.Background()
		r := NewMySQLGuildRepo(bc.db)

		const maxMembers = 500
		const maxPending = 8
		const applicants = 40
		guildID, _ := createGuildWithLeader(t, ctx, r, maxMembers)

		var wg sync.WaitGroup
		wg.Add(applicants)
		errs := make([]error, applicants)
		for i := 0; i < applicants; i++ {
			go func(idx int) {
				defer wg.Done()
				_, _, errs[idx] = r.CreateJoinRequest(ctx, nextID(), guildID, nextID(), maxPending)
			}(i)
		}
		wg.Wait()

		var ok, limited int
		for i := 0; i < applicants; i++ {
			switch {
			case errs[i] == nil:
				ok++
			case errcode.As(errs[i]) == errcode.ErrGuildRequestLimit:
				limited++
			default:
				t.Fatalf("[%s] 申请 %d 非预期错误码 %d:%v", bc.name, i, errcode.As(errs[i]), errs[i])
			}
		}
		if ok != maxPending {
			t.Fatalf("[%s] 成功申请数=%d,期望恰为 maxPending=%d(上限被击穿或误拦)", bc.name, ok, maxPending)
		}
		if limited != applicants-maxPending {
			t.Fatalf("[%s] 被拒申请数=%d,期望=%d", bc.name, limited, applicants-maxPending)
		}

		// 落库 pending 行数与计数列都必须恰等于 maxPending。
		var rows int
		if err := bc.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM guild_join_requests WHERE guild_id = ? AND status = 1`, guildID).Scan(&rows); err != nil {
			t.Fatalf("[%s] count pending rows: %v", bc.name, err)
		}
		if rows != maxPending {
			t.Fatalf("[%s] 实际 pending 行数=%d,期望=%d", bc.name, rows, maxPending)
		}
		var counter int
		if err := bc.db.QueryRowContext(ctx,
			`SELECT pending_request_count FROM guilds WHERE guild_id = ?`, guildID).Scan(&counter); err != nil {
			t.Fatalf("[%s] read pending_request_count: %v", bc.name, err)
		}
		if counter != maxPending {
			t.Fatalf("[%s] pending_request_count=%d,期望=%d(计数列与实际行数漂移)", bc.name, counter, maxPending)
		}
	})
}

// TestConcurrentPlayerGroupLimitEnforced 双后端(MySQL / TiDB)验证「我所在的群」上限:同一玩家 P
// 被大量不同群主并发拉入群(CreateGroup owner + [P]),maxGroups 取小值;计数表 player_group_counts
// (per-player 计数行 FOR UPDATE)必须精确拦停——P 成功入群数恰为 maxGroups,超出全部 ErrGroupJoinLimit,
// 且 P 的实际成员行数与计数行都恰等于 maxGroups(§3.5:取代 COUNT(*)...FOR UPDATE,TiDB 安全)。
func TestConcurrentPlayerGroupLimitEnforced(t *testing.T) {
	forEachBackend(t, func(t *testing.T, bc backendCase) {
		ctx := context.Background()
		gr := NewMySQLGroupRepo(bc.db)

		const maxMembers = 50
		const maxGroups = 6
		const attempts = 40
		player := nextID() // 被争抢入群的固定玩家

		var wg sync.WaitGroup
		wg.Add(attempts)
		errs := make([]error, attempts)
		for i := 0; i < attempts; i++ {
			go func(idx int) {
				defer wg.Done()
				gid := nextID()
				owner := nextID() // 每个群不同群主,只有 player 是争抢点
				errs[idx] = gr.CreateGroup(ctx, gid, owner, fmt.Sprintf("g-%d", gid), []uint64{player}, maxMembers, maxGroups)
			}(i)
		}
		wg.Wait()

		var ok, limited int
		for i := 0; i < attempts; i++ {
			switch {
			case errs[i] == nil:
				ok++
			case errcode.As(errs[i]) == errcode.ErrGroupJoinLimit:
				limited++
			default:
				t.Fatalf("[%s] 建群 %d 非预期错误码 %d:%v", bc.name, i, errcode.As(errs[i]), errs[i])
			}
		}
		if ok != maxGroups {
			t.Fatalf("[%s] 成功入群数=%d,期望恰为 maxGroups=%d(上限被击穿或误拦)", bc.name, ok, maxGroups)
		}
		if limited != attempts-maxGroups {
			t.Fatalf("[%s] 被拒建群数=%d,期望=%d", bc.name, limited, attempts-maxGroups)
		}

		// P 的实际所在群成员行数与计数行都必须恰等于 maxGroups。
		var rows int
		if err := bc.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM chat_group_members WHERE player_id = ?`, player).Scan(&rows); err != nil {
			t.Fatalf("[%s] count player group rows: %v", bc.name, err)
		}
		if rows != maxGroups {
			t.Fatalf("[%s] P 实际所在群行数=%d,期望=%d", bc.name, rows, maxGroups)
		}
		var counter int
		if err := bc.db.QueryRowContext(ctx,
			`SELECT group_count FROM player_group_counts WHERE player_id = ?`, player).Scan(&counter); err != nil {
			t.Fatalf("[%s] read group_count: %v", bc.name, err)
		}
		if counter != maxGroups {
			t.Fatalf("[%s] group_count=%d,期望=%d(计数表与实际行数漂移)", bc.name, counter, maxGroups)
		}
	})
}

// TestGroupCountReleasedOnLeave 双后端验证退群 / 被踢 / 解散后计数表正确回收:玩家满额入群后
// 退出若干个,应能重新入群(名额被 releasePlayerGroupSlot 归还,计数行不漂移)。
func TestGroupCountReleasedOnLeave(t *testing.T) {
	forEachBackend(t, func(t *testing.T, bc backendCase) {
		ctx := context.Background()
		gr := NewMySQLGroupRepo(bc.db)

		const maxMembers = 50
		const maxGroups = 3
		player := nextID()

		// 先把 player 塞满 maxGroups 个群,记下群 ID。
		gids := make([]uint64, 0, maxGroups)
		for i := 0; i < maxGroups; i++ {
			gid := nextID()
			if err := gr.CreateGroup(ctx, gid, nextID(), fmt.Sprintf("g-%d", gid), []uint64{player}, maxMembers, maxGroups); err != nil {
				t.Fatalf("[%s] 建群 %d: %v", bc.name, i, err)
			}
			gids = append(gids, gid)
		}
		// 满额后再入群必须被拒。
		if err := gr.CreateGroup(ctx, nextID(), nextID(), "overflow", []uint64{player}, maxMembers, maxGroups); errcode.As(err) != errcode.ErrGroupJoinLimit {
			t.Fatalf("[%s] 满额入群应回 ErrGroupJoinLimit,得到 %v", bc.name, err)
		}

		// 退出一个群(player 是成员而非群主,可直接退群),名额应归还。
		if err := gr.RemoveMember(ctx, gids[0], player); err != nil {
			t.Fatalf("[%s] 退群: %v", bc.name, err)
		}
		var counter int
		if err := bc.db.QueryRowContext(ctx,
			`SELECT group_count FROM player_group_counts WHERE player_id = ?`, player).Scan(&counter); err != nil {
			t.Fatalf("[%s] read group_count: %v", bc.name, err)
		}
		if counter != maxGroups-1 {
			t.Fatalf("[%s] 退群后 group_count=%d,期望=%d(名额未归还)", bc.name, counter, maxGroups-1)
		}
		// 归还后应能重新入一个新群。
		if err := gr.CreateGroup(ctx, nextID(), nextID(), "refill", []uint64{player}, maxMembers, maxGroups); err != nil {
			t.Fatalf("[%s] 归还名额后重新入群应成功,得到 %v", bc.name, err)
		}
	})
}

// TestLegacyGuildWriterCounterDriftSelfHeals 模拟滚动窗口中的旧 Pod：它仍按 guilds 父行 +
// pending 明细范围锁判限，但只写 guild_join_requests，不维护新计数列。随后兼容新版必须以
// 明细为权威自愈，且旧写把实际行数推到上限后不能再放入第 N+1 条。
func TestLegacyGuildWriterCounterDriftSelfHeals(t *testing.T) {
	forEachBackend(t, func(t *testing.T, bc backendCase) {
		ctx := context.Background()
		r := NewMySQLGuildRepo(bc.db)
		guildID, leaderID := createGuildWithLeader(t, ctx, r, 100)
		const maxPending = 3

		legacyPlayer1, legacyRequest1 := nextID(), nextID()
		if err := legacyCreateJoinRequestForTest(ctx, r, legacyRequest1, guildID, legacyPlayer1, maxPending); err != nil {
			t.Fatalf("[%s] legacy request 1: %v", bc.name, err)
		}
		if _, err := bc.db.ExecContext(ctx,
			`UPDATE guilds SET pending_request_count = 99 WHERE guild_id = ?`, guildID); err != nil {
			t.Fatalf("[%s] poison pending counter: %v", bc.name, err)
		}

		// 实际只有 1 条，错误高计数不能误拒；成功后应绝对值自愈为 2。
		if _, _, err := r.CreateJoinRequest(ctx, nextID(), guildID, nextID(), maxPending); err != nil {
			t.Fatalf("[%s] compatible writer should heal high counter: %v", bc.name, err)
		}
		assertGuildPendingCounts(t, ctx, bc, guildID, 2, 2)

		legacyPlayer3, legacyRequest3 := nextID(), nextID()
		if err := legacyCreateJoinRequestForTest(ctx, r, legacyRequest3, guildID, legacyPlayer3, maxPending); err != nil {
			t.Fatalf("[%s] legacy request 3: %v", bc.name, err)
		}
		// 旧写后 counter 仍为 2、实际已满 3；新版必须拒绝第 4 条，不能信任低计数突破上限。
		if _, _, err := r.CreateJoinRequest(ctx, nextID(), guildID, nextID(), maxPending); errcode.As(err) != errcode.ErrGuildRequestLimit {
			t.Fatalf("[%s] stale low counter must not admit request 4: %v", bc.name, err)
		}
		var actual int
		if err := bc.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM guild_join_requests WHERE guild_id = ? AND status = ?`,
			guildID, joinStatusPending).Scan(&actual); err != nil {
			t.Fatalf("[%s] count pending after limit: %v", bc.name, err)
		}
		if actual != maxPending {
			t.Fatalf("[%s] actual pending=%d,期望=%d", bc.name, actual, maxPending)
		}

		// 对旧写已有 pending 的幂等调用也会提交 self-heal，不要求新建一行才修正。
		gotID, already, err := r.CreateJoinRequest(ctx, nextID(), guildID, legacyPlayer3, maxPending)
		if err != nil || !already || gotID != legacyRequest3 {
			t.Fatalf("[%s] idempotent self-heal got id=%d already=%v err=%v", bc.name, gotID, already, err)
		}
		assertGuildPendingCounts(t, ctx, bc, guildID, maxPending, maxPending)

		if _, err := bc.db.ExecContext(ctx,
			`UPDATE guilds SET pending_request_count = 99 WHERE guild_id = ?`, guildID); err != nil {
			t.Fatalf("[%s] poison pending counter before reject: %v", bc.name, err)
		}
		rejected, err := r.RejectJoin(ctx, legacyRequest1, leaderID)
		if err != nil || !rejected {
			t.Fatalf("[%s] compatible reject should reconcile detail count, rejected=%v err=%v", bc.name, rejected, err)
		}
		assertGuildPendingCounts(t, ctx, bc, guildID, 2, 2)
	})
}

// TestLegacyGroupWriterCounterDriftSelfHeals 模拟旧 Pod 继续用 chat_group_members(player_id)
// COUNT...FOR UPDATE 判限且不维护 player_group_counts。兼容新版必须按明细修复高/低错误值，
// 第 N+1 次入群仍被拒；删明细后计数也必须按剩余明细绝对值校正而不是盲目 -1。
func TestLegacyGroupWriterCounterDriftSelfHeals(t *testing.T) {
	forEachBackend(t, func(t *testing.T, bc backendCase) {
		ctx := context.Background()
		r := NewMySQLGroupRepo(bc.db)
		const maxGroups = 3
		const maxMembers = 50
		playerID := nextID()
		groupIDs := []uint64{nextID(), nextID(), nextID(), nextID()}
		for _, groupID := range groupIDs {
			insertLegacyGroupForTest(t, ctx, bc, groupID, nextID())
		}

		if err := legacyAddGroupMemberForTest(ctx, r, groupIDs[0], playerID, maxGroups); err != nil {
			t.Fatalf("[%s] legacy add group 1: %v", bc.name, err)
		}
		if _, err := bc.db.ExecContext(ctx,
			`INSERT INTO player_group_counts (player_id, group_count) VALUES (?, 99)
			 ON DUPLICATE KEY UPDATE group_count = VALUES(group_count)`, playerID); err != nil {
			t.Fatalf("[%s] poison group counter: %v", bc.name, err)
		}

		if already, err := r.AddMember(ctx, groupIDs[1], 0, playerID, maxMembers, maxGroups); err != nil || already {
			t.Fatalf("[%s] compatible add should heal high counter, already=%v err=%v", bc.name, already, err)
		}
		assertPlayerGroupCounts(t, ctx, bc, playerID, 2, 2)

		if err := legacyAddGroupMemberForTest(ctx, r, groupIDs[2], playerID, maxGroups); err != nil {
			t.Fatalf("[%s] legacy add group 3: %v", bc.name, err)
		}
		if _, err := r.AddMember(ctx, groupIDs[3], 0, playerID, maxMembers, maxGroups); errcode.As(err) != errcode.ErrGroupJoinLimit {
			t.Fatalf("[%s] stale low counter must not admit group 4: %v", bc.name, err)
		}
		var actual int
		if err := bc.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM chat_group_members WHERE player_id = ?`, playerID).Scan(&actual); err != nil {
			t.Fatalf("[%s] count groups after limit: %v", bc.name, err)
		}
		if actual != maxGroups {
			t.Fatalf("[%s] actual groups=%d,期望=%d", bc.name, actual, maxGroups)
		}

		if already, err := r.AddMember(ctx, groupIDs[2], 0, playerID, maxMembers, maxGroups); err != nil || !already {
			t.Fatalf("[%s] idempotent group self-heal already=%v err=%v", bc.name, already, err)
		}
		assertPlayerGroupCounts(t, ctx, bc, playerID, maxGroups, maxGroups)

		if _, err := bc.db.ExecContext(ctx,
			`UPDATE player_group_counts SET group_count = 99 WHERE player_id = ?`, playerID); err != nil {
			t.Fatalf("[%s] poison group counter before release: %v", bc.name, err)
		}
		if err := r.RemoveMember(ctx, groupIDs[0], playerID); err != nil {
			t.Fatalf("[%s] compatible release: %v", bc.name, err)
		}
		assertPlayerGroupCounts(t, ctx, bc, playerID, 2, 2)
	})
}

func legacyCreateJoinRequestForTest(
	ctx context.Context,
	r *MySQLGuildRepo,
	requestID, guildID, playerID uint64,
	maxPending int,
) error {
	return r.tx(ctx, func(tx *sql.Tx) error {
		var lockedGuildID uint64
		if err := tx.QueryRowContext(ctx,
			`SELECT guild_id FROM guilds WHERE guild_id = ? FOR UPDATE`, guildID).Scan(&lockedGuildID); err != nil {
			return dbErr(err, "legacy lock guild %d", guildID)
		}
		var existingID uint64
		err := tx.QueryRowContext(ctx,
			`SELECT request_id FROM guild_join_requests WHERE guild_id = ? AND player_id = ? FOR UPDATE`,
			guildID, playerID).Scan(&existingID)
		if err == nil {
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return dbErr(err, "legacy query join request")
		}
		var count int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM guild_join_requests WHERE guild_id = ? AND status = ? FOR UPDATE`,
			guildID, joinStatusPending).Scan(&count); err != nil {
			return dbErr(err, "legacy count pending")
		}
		if maxPending > 0 && count >= maxPending {
			return errcode.New(errcode.ErrGuildRequestLimit, "legacy pending limit")
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO guild_join_requests (request_id, guild_id, player_id, status) VALUES (?, ?, ?, ?)`,
			requestID, guildID, playerID, joinStatusPending); err != nil {
			return dbErr(err, "legacy insert join request")
		}
		return nil
	})
}

func legacyAddGroupMemberForTest(
	ctx context.Context,
	r *MySQLGroupRepo,
	groupID, playerID uint64,
	maxGroups int,
) error {
	return r.tx(ctx, func(tx *sql.Tx) error {
		var memberCount int
		if err := tx.QueryRowContext(ctx,
			`SELECT member_count FROM chat_groups WHERE group_id = ? FOR UPDATE`, groupID).Scan(&memberCount); err != nil {
			return dbErr(err, "legacy lock group %d", groupID)
		}
		var count int
		if err := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM chat_group_members WHERE player_id = ? FOR UPDATE`, playerID).Scan(&count); err != nil {
			return dbErr(err, "legacy count player groups")
		}
		if maxGroups > 0 && count >= maxGroups {
			return errcode.New(errcode.ErrGroupJoinLimit, "legacy group limit")
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO chat_group_members (group_id, player_id, role) VALUES (?, ?, ?)`,
			groupID, playerID, GroupRoleMember); err != nil {
			return dbErr(err, "legacy insert group member")
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE chat_groups SET member_count = member_count + 1 WHERE group_id = ?`, groupID); err != nil {
			return dbErr(err, "legacy inc group member_count")
		}
		return nil
	})
}

func insertLegacyGroupForTest(t *testing.T, ctx context.Context, bc backendCase, groupID, ownerID uint64) {
	t.Helper()
	if _, err := bc.db.ExecContext(ctx,
		`INSERT INTO chat_groups (group_id, name, owner_id, member_count, max_members) VALUES (?, ?, ?, 1, 50)`,
		groupID, fmt.Sprintf("legacy-%d", groupID), ownerID); err != nil {
		t.Fatalf("[%s] insert legacy group: %v", bc.name, err)
	}
	if _, err := bc.db.ExecContext(ctx,
		`INSERT INTO chat_group_members (group_id, player_id, role) VALUES (?, ?, ?)`,
		groupID, ownerID, GroupRoleOwner); err != nil {
		t.Fatalf("[%s] insert legacy owner: %v", bc.name, err)
	}
}

func assertGuildPendingCounts(t *testing.T, ctx context.Context, bc backendCase, guildID uint64, wantActual, wantCounter int) {
	t.Helper()
	var actual, counter int
	if err := bc.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM guild_join_requests WHERE guild_id = ? AND status = ?`,
		guildID, joinStatusPending).Scan(&actual); err != nil {
		t.Fatalf("[%s] count pending details: %v", bc.name, err)
	}
	if err := bc.db.QueryRowContext(ctx,
		`SELECT pending_request_count FROM guilds WHERE guild_id = ?`, guildID).Scan(&counter); err != nil {
		t.Fatalf("[%s] read pending counter: %v", bc.name, err)
	}
	if actual != wantActual || counter != wantCounter {
		t.Fatalf("[%s] pending actual=%d counter=%d,期望 actual=%d counter=%d",
			bc.name, actual, counter, wantActual, wantCounter)
	}
}

func assertPlayerGroupCounts(t *testing.T, ctx context.Context, bc backendCase, playerID uint64, wantActual, wantCounter int) {
	t.Helper()
	var actual, counter int
	if err := bc.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM chat_group_members WHERE player_id = ?`, playerID).Scan(&actual); err != nil {
		t.Fatalf("[%s] count player group details: %v", bc.name, err)
	}
	if err := bc.db.QueryRowContext(ctx,
		`SELECT group_count FROM player_group_counts WHERE player_id = ?`, playerID).Scan(&counter); err != nil {
		t.Fatalf("[%s] read player group counter: %v", bc.name, err)
	}
	if actual != wantActual || counter != wantCounter {
		t.Fatalf("[%s] groups actual=%d counter=%d,期望 actual=%d counter=%d",
			bc.name, actual, counter, wantActual, wantCounter)
	}
}

// TestGuildNameCaseInsensitiveDedup 双后端验证公会名唯一键的 collation 语义:name 列显式
// utf8mb4_0900_ai_ci → 大小写 / 口音不敏感,"GuildAlpha" 与 "guildalpha" 视为同名冲突。
// 关键防回归点(§5.1):TiDB 默认 collation 是 utf8mb4_bin(大小写敏感),若 name 列不显式声明
// 0900_ai_ci,重名判定会从"冲突"变成"不冲突",与现网 MySQL 行为漂移。此测试锁死该语义。
func TestGuildNameCaseInsensitiveDedup(t *testing.T) {
	forEachBackend(t, func(t *testing.T, bc backendCase) {
		ctx := context.Background()
		r := NewMySQLGuildRepo(bc.db)

		if err := r.CreateGuild(ctx, nextID(), nextID(), "GuildAlpha", 100); err != nil {
			t.Fatalf("[%s] 首次建公会应成功:%v", bc.name, err)
		}
		// 仅大小写不同的重名必须被拒(大小写不敏感 uk)。
		err := r.CreateGuild(ctx, nextID(), nextID(), "guildalpha", 100)
		if code := errcode.As(err); code != errcode.ErrGuildNameTaken {
			t.Fatalf("[%s] 大小写变体重名必须回 ErrGuildNameTaken(collation 漂移风险),得到 code=%d err=%v",
				bc.name, code, err)
		}
	})
}
