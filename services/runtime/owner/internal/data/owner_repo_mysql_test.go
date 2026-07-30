// owner_repo_mysql_test.go — owner 权威状态机真实 MySQL 集成测试(owner-authority.md §5)。
//
// PANDORA_TEST_MYSQL_DSN 必须是不带 database 的专用测试实例 DSN(同 inventory/bag 模式)。
// 测试只创建/删除名称严格匹配 pandora_owner_it_* 的随机临时库;未设置时明确 Skip。
//
// 覆盖(§16 风险对应验证):
//   - 首迁移无屏障 + Begin/Admit 幂等重放(响应丢失安全);
//   - epoch CAS 冲突(并发双迁移一胜一败);
//   - admit_not_before = CAS 时点旧租约观察值 + 余量;早到 Admit 拒;Begin 后旧实例续租
//     不回写已算屏障;
//   - Admit exact 身份全等(旧 epoch / 换实例 UID 拒);
//   - Release 幂等(迟到旧 operation no-op);
//   - 实例租约只前进 + 实例纪元不符拒。
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

	mysql "github.com/go-sql-driver/mysql"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

const ownerMySQLTestTimeout = 15 * time.Second

// ownerMySQLSetupTimeout 是 fixture 建库 + 建表(DDL)与清理的预算,独立于断言用的
// ownerMySQLTestTimeout。DDL 比测试体里的小 DML 重一个量级(CREATE DATABASE 要落目录),
// 而测试可能跑在被代理的链路上(kubectl port-forward / 远端联调库),实测单条 CREATE
// DATABASE 可达十余秒——原实现用**同一个 15s ctx** 覆盖 ping + 建库 + 建表三步,任一步
// 稍慢就把后面两步的预算吃光,失败还表现为「schema 初始化超时」这种误导性信息。
// 放宽只影响等待上限,不改变任何断言:逻辑错依旧失败。
const ownerMySQLSetupTimeout = 120 * time.Second

var ownerMySQLTestDBPattern = regexp.MustCompile(`^pandora_owner_it_[0-9]+_[0-9a-f]{12}$`)

type ownerMySQLFixture struct {
	admin   *sql.DB
	db      *sql.DB
	dbName  string
	created bool
}

func openOwnerMySQLFixture(t *testing.T) *ownerMySQLFixture {
	t.Helper()
	serverDSN := strings.TrimSpace(os.Getenv("PANDORA_TEST_MYSQL_DSN"))
	if serverDSN == "" {
		t.Skip("未设置 PANDORA_TEST_MYSQL_DSN,跳过 owner 权威真 MySQL 集成测试")
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
	// 驱动级读写上限按 DDL 预算取(建库/建表是本套最重的语句);断言用的小 DML 远低于此,
	// 放宽只影响卡死时的等待上限,不改变任何判定。
	cfg.ReadTimeout = ownerMySQLSetupTimeout
	cfg.WriteTimeout = ownerMySQLSetupTimeout

	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开 MySQL 管理连接: %v", err)
	}
	f := &ownerMySQLFixture{admin: admin}
	t.Cleanup(func() { f.cleanup(t) })
	ctx, cancel := context.WithTimeout(context.Background(), ownerMySQLSetupTimeout)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("已设置测试 DSN 但 MySQL 不可达: %v", err)
	}

	var randBytes [6]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		t.Fatalf("生成随机库名: %v", err)
	}
	f.dbName = fmt.Sprintf("pandora_owner_it_%d_%s", time.Now().UnixNano(), hex.EncodeToString(randBytes[:]))
	if !ownerMySQLTestDBPattern.MatchString(f.dbName) {
		t.Fatalf("随机测试库名未通过安全校验: %q", f.dbName)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+f.dbName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		t.Fatalf("创建随机测试库 %s: %v", f.dbName, err)
	}
	f.created = true

	if _, err := admin.ExecContext(ctx, readOwnerMySQLSchema(t, f.dbName)); err != nil {
		t.Fatalf("初始化 owner schema: %v", err)
	}
	testCfg := cfg.Clone()
	testCfg.DBName = f.dbName
	db, err := sql.Open("mysql", testCfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开测试库连接: %v", err)
	}
	f.db = db
	return f
}

func (f *ownerMySQLFixture) cleanup(t *testing.T) {
	t.Helper()
	// DROP DATABASE 与建库同级重,用 setup 预算(原来复用 15s 断言预算,慢链路上必然误报)。
	ctx, cancel := context.WithTimeout(context.Background(), ownerMySQLSetupTimeout)
	defer cancel()
	if f.db != nil {
		_ = f.db.Close()
	}
	if f.created && ownerMySQLTestDBPattern.MatchString(f.dbName) {
		if _, err := f.admin.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+f.dbName+"`"); err != nil {
			t.Errorf("删除随机测试库 %s: %v", f.dbName, err)
		}
	}
	_ = f.admin.Close()
}

func readOwnerMySQLSchema(t *testing.T, dbName string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("定位测试文件路径失败")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(thisFile),
		"..", "..", "..", "..", "..", "deploy", "mysql-init", "15-owner-tables.sql"))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 owner schema %s: %v", path, err)
	}
	schema := string(b)
	const needle = "USE `pandora_owner`;"
	if strings.Count(schema, needle) != 1 {
		t.Fatalf("owner schema USE 锚点数量异常: %d", strings.Count(schema, needle))
	}
	return strings.Replace(schema, needle, "USE `"+dbName+"`;", 1)
}

func testTarget(uid string) OwnerTarget {
	return OwnerTarget{
		PodName: "hub-" + uid, InstanceUID: uid, InstanceEpoch: 1,
		AssignmentOrAllocationID: "assign-" + uid, ReleaseTrack: "stable",
	}
}

const testOpA = "6f9619ff-8b86-4d01-b42d-00cf4fc964ff"
const testOpB = "7f9619ff-8b86-4d01-b42d-00cf4fc964aa"
const testOpC = "8f9619ff-8b86-4d01-b42d-00cf4fc964bb"

func TestOwnerRepoMySQL(t *testing.T) {
	f := openOwnerMySQLFixture(t)
	repo := NewMySQLOwnerRepo(f.db)
	ctx := context.Background()

	t.Run("FirstTransitionNoBarrierAndIdempotentReplay", func(t *testing.T) {
		const player = 301
		target := testTarget("uid-a")
		rec, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, target, 5*time.Second)
		if err != nil || rec.OwnerEpoch != 1 || rec.Phase != OwnerPhasePending {
			t.Fatalf("首迁移: %+v err=%v", rec, err)
		}
		if rec.AdmitNotBeforeMs > time.Now().UnixMilli() {
			t.Fatalf("首迁移(无旧 owner)不应有屏障: admit=%d", rec.AdmitNotBeforeMs)
		}
		// Begin 幂等重放:同 operation 原样返回,epoch 不再推进。
		replay, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, target, 5*time.Second)
		if err != nil || replay.OwnerEpoch != 1 {
			t.Fatalf("Begin 重放: %+v err=%v", replay, err)
		}
		// Admit 成功 + 幂等重放。
		admitted, retry, err := repo.Admit(ctx, player, 1, testOpA, target)
		if err != nil || retry != 0 || admitted.Phase != OwnerPhaseAdmitted {
			t.Fatalf("Admit: %+v retry=%d err=%v", admitted, retry, err)
		}
		again, _, err := repo.Admit(ctx, player, 1, testOpA, target)
		if err != nil || again.Phase != OwnerPhaseAdmitted {
			t.Fatalf("Admit 重放: %+v err=%v", again, err)
		}
	})

	// 同 exact 实例的重复投递必须收敛为 no-op,且 **operation_id 必须保持不变**。
	// 这是 §9.23「一次真实进场用一个稳定 operation_id」的权威侧落点:调用方过去每次
	// 投递现铸 UUID,同一次进场的重连/重复交付/心跳自愈会写出不同 operation,幂等键失效。
	t.Run("SameInstanceRedeliveryKeepsOperationAndEpoch", func(t *testing.T) {
		const player = 310
		target := testTarget("uid-same")
		first, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, target, 5*time.Second)
		if err != nil || first.OwnerEpoch != 1 || first.OperationID != testOpA {
			t.Fatalf("首迁移: %+v err=%v", first, err)
		}

		// ① 换一个 operation 重复投递同一实例 → 原样返回,epoch 不动,operation 不被覆盖。
		again, err := repo.BeginTransition(ctx, player, 0, testOpB, OwnerTypeHub, target, 5*time.Second)
		if err != nil {
			t.Fatalf("同实例重复投递不应报错: %v", err)
		}
		if again.OwnerEpoch != 1 {
			t.Fatalf("同实例重复投递不得推进 epoch: got=%d want=1", again.OwnerEpoch)
		}
		if again.OperationID != testOpA {
			t.Fatalf("operation_id 必须稳定: got=%q want=%q(调用方传的 %q 不得覆盖)",
				again.OperationID, testOpA, testOpB)
		}

		// ② 过期 expectEpoch + 同实例 → 仍是 no-op,不得报 EPOCH_CONFLICT。
		//    「目标已经是它」本就该幂等,不该先冲突再让调用方重查。
		stale, err := repo.BeginTransition(ctx, player, 0, testOpC, OwnerTypeHub, target, 5*time.Second)
		if err != nil || stale.OwnerEpoch != 1 || stale.OperationID != testOpA {
			t.Fatalf("过期 expect + 同实例应幂等 no-op: %+v err=%v", stale, err)
		}

		// ③ 同实例但 assignment 刷新(seat 续租)→ 仍不得推进 epoch(churn 防护)。
		refreshed := target
		refreshed.AssignmentOrAllocationID = "assign-uid-same-v2"
		seat, err := repo.BeginTransition(ctx, player, 1, testOpB, OwnerTypeHub, refreshed, 5*time.Second)
		if err != nil || seat.OwnerEpoch != 1 || seat.OperationID != testOpA {
			t.Fatalf("assignment 刷新不得推进 epoch: %+v err=%v", seat, err)
		}

		// ④ 已 ADMITTED 后重复投递 → 不得被打回 PENDING。
		if _, _, aerr := repo.Admit(ctx, player, 1, testOpA, target); aerr != nil {
			t.Fatalf("Admit: %v", aerr)
		}
		afterAdmit, err := repo.BeginTransition(ctx, player, 1, testOpC, OwnerTypeHub, target, 5*time.Second)
		if err != nil || afterAdmit.Phase != OwnerPhaseAdmitted || afterAdmit.OwnerEpoch != 1 {
			t.Fatalf("ADMITTED 后重复投递不得回退 phase: %+v err=%v", afterAdmit, err)
		}

		// ⑤ 真换实例 = 真实迁移:epoch 推进,operation 换成本次的。
		moved, err := repo.BeginTransition(ctx, player, 1, testOpB, OwnerTypeHub, testTarget("uid-moved"), 0)
		if err != nil || moved.OwnerEpoch != 2 || moved.OperationID != testOpB {
			t.Fatalf("换实例应真实迁移: %+v err=%v", moved, err)
		}
	})

	// 真实迁移路径不接受空 operation_id(绕过 biz 的内部调用兜底;空 operation 会让
	// Admit exact 校验、客户端续用与审计流水同时失去锚点)。
	t.Run("RealTransitionRejectsEmptyOperation", func(t *testing.T) {
		const player = 311
		_, err := repo.BeginTransition(ctx, player, 0, "", OwnerTypeHub, testTarget("uid-empty"), 0)
		if errcode.As(err) != errcode.ErrOwnerInvalidOperation {
			t.Fatalf("空 operation 的真实迁移必须拒绝,得到 err=%v", err)
		}
	})

	t.Run("EpochConflictAndConcurrentBegin", func(t *testing.T) {
		const player = 302
		target := testTarget("uid-b")
		if _, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, target, 0); err != nil {
			t.Fatalf("首迁移: %v", err)
		}
		// 过期期望 → 冲突并附当前记录。
		cur, err := repo.BeginTransition(ctx, player, 0, testOpB, OwnerTypeHub, testTarget("uid-b2"), 0)
		if errcode.As(err) != errcode.ErrOwnerEpochConflict || cur.OwnerEpoch != 1 {
			t.Fatalf("epoch 冲突: %+v err=%v", cur, err)
		}
		// 并发双迁移(同 expect):恰好一胜一冲突。
		var wg sync.WaitGroup
		errs := make([]error, 2)
		ops := []string{testOpB, testOpC}
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = repo.BeginTransition(ctx, player, 1, ops[idx], OwnerTypeBattle,
					testTarget(fmt.Sprintf("uid-b-race-%d", idx)), 0)
			}(i)
		}
		wg.Wait()
		wins, conflicts := 0, 0
		for _, e := range errs {
			switch {
			case e == nil:
				wins++
			case errcode.As(e) == errcode.ErrOwnerEpochConflict:
				conflicts++
			default:
				t.Fatalf("并发 Begin 意外错误: %v", e)
			}
		}
		if wins != 1 || conflicts != 1 {
			t.Fatalf("并发双迁移应恰好一胜一冲突: wins=%d conflicts=%d", wins, conflicts)
		}
	})

	t.Run("BarrierFromOldLeaseObservedAtCAS", func(t *testing.T) {
		const player = 303
		oldTarget := testTarget("uid-c-old")
		newTarget := testTarget("uid-c-new")
		if _, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, oldTarget, 0); err != nil {
			t.Fatalf("旧 owner 建立: %v", err)
		}
		if _, _, err := repo.Admit(ctx, player, 1, testOpA, oldTarget); err != nil {
			t.Fatalf("旧 owner admit: %v", err)
		}
		// 旧实例续租 10s → 屏障应 ≥ 旧租约截止 + 余量。
		oldDeadline, err := repo.RenewInstanceLease(ctx, oldTarget, 10*time.Second)
		if err != nil {
			t.Fatalf("旧实例续租: %v", err)
		}
		const margin = 5 * time.Second
		rec, err := repo.BeginTransition(ctx, player, 1, testOpB, OwnerTypeBattle, newTarget, margin)
		if err != nil {
			t.Fatalf("迁移到新 owner: %v", err)
		}
		wantMin := oldDeadline + margin.Milliseconds()
		if rec.AdmitNotBeforeMs < wantMin {
			t.Fatalf("屏障应 ≥ 旧租约+余量: admit=%d want>=%d", rec.AdmitNotBeforeMs, wantMin)
		}
		// 早到 Admit 拒(带 retry_after)。
		_, retry, err := repo.Admit(ctx, player, 2, testOpB, newTarget)
		if errcode.As(err) != errcode.ErrOwnerBarrierNotOpen || retry <= 0 {
			t.Fatalf("早到 Admit 应拒: retry=%d err=%v", retry, err)
		}
		// Begin 后旧实例继续续租,不回写已算屏障(取 CAS 时点观察值)。
		if _, err := repo.RenewInstanceLease(ctx, oldTarget, 20*time.Second); err != nil {
			t.Fatalf("Begin 后旧实例续租: %v", err)
		}
		after, err := repo.Query(ctx, player)
		if err != nil || after.AdmitNotBeforeMs != rec.AdmitNotBeforeMs {
			t.Fatalf("屏障被后续续租改写: before=%d after=%d err=%v", rec.AdmitNotBeforeMs, after.AdmitNotBeforeMs, err)
		}
		// 身份不匹配拒:旧 epoch / 换实例 UID。
		if _, _, err := repo.Admit(ctx, player, 1, testOpA, oldTarget); errcode.As(err) != errcode.ErrOwnerIdentityMismatch {
			t.Fatalf("旧 epoch Admit 应拒: %v", err)
		}
		forged := newTarget
		forged.InstanceUID = "uid-c-forged"
		if _, _, err := repo.Admit(ctx, player, 2, testOpB, forged); errcode.As(err) != errcode.ErrOwnerIdentityMismatch {
			t.Fatalf("换实例 Admit 应拒: %v", err)
		}
	})

	t.Run("ReleaseIdempotentLateNoop", func(t *testing.T) {
		const player = 304
		target := testTarget("uid-d")
		if _, err := repo.BeginTransition(ctx, player, 0, testOpA, OwnerTypeHub, target, 0); err != nil {
			t.Fatalf("建立: %v", err)
		}
		if _, _, err := repo.Admit(ctx, player, 1, testOpA, target); err != nil {
			t.Fatalf("admit: %v", err)
		}
		rec, err := repo.Release(ctx, player, 1, testOpA)
		if err != nil || rec.OwnerType != OwnerTypeNone || rec.OwnerEpoch != 1 {
			t.Fatalf("释放: %+v err=%v", rec, err)
		}
		// 迟到 Release(旧 operation)幂等 no-op。
		late, err := repo.Release(ctx, player, 1, testOpB)
		if err != nil || late.OwnerType != OwnerTypeNone {
			t.Fatalf("迟到释放应 no-op: %+v err=%v", late, err)
		}
	})

	t.Run("LeaseMonotonicAndEpochGuard", func(t *testing.T) {
		target := testTarget("uid-e")
		first, err := repo.RenewInstanceLease(ctx, target, 10*time.Second)
		if err != nil {
			t.Fatalf("首续: %v", err)
		}
		shorter, err := repo.RenewInstanceLease(ctx, target, 1*time.Second)
		if err != nil || shorter < first {
			t.Fatalf("deadline 不得回退: first=%d shorter=%d err=%v", first, shorter, err)
		}
		replaced := target
		replaced.InstanceEpoch = 2
		if _, err := repo.RenewInstanceLease(ctx, replaced, 5*time.Second); errcode.As(err) != errcode.ErrOwnerLeaseRegressed {
			t.Fatalf("实例纪元不符续租应拒: %v", err)
		}
	})
}
