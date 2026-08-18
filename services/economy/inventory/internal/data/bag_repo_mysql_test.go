// bag_repo_mysql_test.go — 背包域 repo 真实 MySQL 集成测试(bag-domain.md §2/§4/§6)。
//
// PANDORA_TEST_MYSQL_DSN 必须是不带 database 的专用测试实例 DSN(同 inventory_repo_mysql_test)。
// 测试只创建/删除名称严格匹配 pandora_bag_it_* 的随机临时库;未设置时明确 Skip。
//
// 覆盖(§16 风险对应验证):
//   - epoch fencing:旧 epoch 整批拒,新 epoch 推进(脑裂旧写封死);
//   - journal 前缀确认 + 纯重放安全 + 幂等键复用不同内容冲突;
//   - 后端驻留段与 journal 同事务(失败整批回滚,零部分应用);
//   - 活动段代际:不符拒、切代后读过滤逻辑清空 + 迟到旧代写拒;
//   - checkpoint covered_seq 单调与不超水位;LoadBag = 快照 + 尾部;
//   - 保留期 sweep 只删 checkpoint 已覆盖行,未覆盖尾部/无 checkpoint 玩家永不删
//     (INC-20260722-003);LoadBag 尾部缺口/截断 fail-closed;
//   - 额度滑窗封顶。
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
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
)

const bagMySQLTestTimeout = 15 * time.Second

var bagMySQLTestDBPattern = regexp.MustCompile(`^pandora_bag_it_[0-9]+_[0-9a-f]{12}$`)

type bagMySQLFixture struct {
	admin   *sql.DB
	db      *sql.DB
	dbName  string
	created bool
}

func openBagMySQLFixture(t *testing.T) *bagMySQLFixture {
	t.Helper()
	serverDSN := strings.TrimSpace(os.Getenv("PANDORA_TEST_MYSQL_DSN"))
	if serverDSN == "" {
		t.Skip("未设置 PANDORA_TEST_MYSQL_DSN,跳过背包域真 MySQL 集成测试")
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
	cfg.ReadTimeout = bagMySQLTestTimeout
	cfg.WriteTimeout = bagMySQLTestTimeout

	admin, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开 MySQL 管理连接: %v", err)
	}
	f := &bagMySQLFixture{admin: admin}
	t.Cleanup(func() { f.cleanup(t) })
	ctx, cancel := context.WithTimeout(context.Background(), bagMySQLTestTimeout)
	defer cancel()
	if err := admin.PingContext(ctx); err != nil {
		t.Fatalf("已设置测试 DSN 但 MySQL 不可达: %v", err)
	}

	var randBytes [6]byte
	if _, err := rand.Read(randBytes[:]); err != nil {
		t.Fatalf("生成随机库名: %v", err)
	}
	f.dbName = fmt.Sprintf("pandora_bag_it_%d_%s", time.Now().UnixNano(), hex.EncodeToString(randBytes[:]))
	if !bagMySQLTestDBPattern.MatchString(f.dbName) {
		t.Fatalf("随机测试库名未通过安全校验: %q", f.dbName)
	}
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE `"+f.dbName+"` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci"); err != nil {
		t.Fatalf("创建随机测试库 %s: %v", f.dbName, err)
	}
	f.created = true

	if _, err := admin.ExecContext(ctx, readBagMySQLSchema(t, f.dbName)); err != nil {
		t.Fatalf("初始化 bag schema: %v", err)
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

func (f *bagMySQLFixture) cleanup(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), bagMySQLTestTimeout)
	defer cancel()
	if f.db != nil {
		_ = f.db.Close()
	}
	if f.created && bagMySQLTestDBPattern.MatchString(f.dbName) {
		if _, err := f.admin.ExecContext(ctx, "DROP DATABASE IF EXISTS `"+f.dbName+"`"); err != nil {
			t.Errorf("删除随机测试库 %s: %v", f.dbName, err)
		}
	}
	_ = f.admin.Close()
}

func readBagMySQLSchema(t *testing.T, dbName string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("定位测试文件路径失败")
	}
	path := filepath.Clean(filepath.Join(filepath.Dir(thisFile),
		"..", "..", "..", "..", "..", "deploy", "mysql-init", "14-bag-tables.sql"))
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取 bag schema %s: %v", path, err)
	}
	schema := string(b)
	const needle = "USE `pandora_bag`;"
	if strings.Count(schema, needle) != 1 {
		t.Fatalf("bag schema USE 锚点数量异常: %d", strings.Count(schema, needle))
	}
	return strings.Replace(schema, needle, "USE `"+dbName+"`;", 1)
}

// bagTestCaps 集成测试统一容量表:仓库 10 / 活动段 100 = 10。
func bagTestCaps(bagType uint32) uint32 {
	switch bagType {
	case BagWarehouseType, BagActivityTypeBase:
		return 10
	}
	return 0
}

// bagTestMaxStack 集成测试统一堆叠上限:所有 config 单格 99(与 conf 默认值一致)。
func bagTestMaxStack(uint32) uint32 { return 99 }

func mkClaimEntry(seq uint64, key string, bagType uint32, generation uint64, items ...*bagv1.BagItem) *bagv1.BagJournalEntry {
	return &bagv1.BagJournalEntry{
		JournalSeq: seq, BagType: bagType, Generation: generation, IdempotencyKey: key,
		Op: &bagv1.BagJournalEntry_MailClaim{MailClaim: &bagv1.MailClaimOp{
			MailId: 1, ClaimKey: key, Items: items,
		}},
	}
}

func queryWarehouseCount(t *testing.T, repo *MySQLBagRepo, playerID uint64, config uint32) uint32 {
	t.Helper()
	secs, err := repo.GetSections(context.Background(), playerID, []uint32{BagWarehouseType}, bagTestCaps)
	if err != nil {
		t.Fatalf("GetSections: %v", err)
	}
	var total uint32
	for _, it := range secs[0].GetItems() {
		if it.GetItemConfigId() == config {
			total += it.GetCount()
		}
	}
	return total
}

func TestBagRepoMySQL(t *testing.T) {
	f := openBagMySQLFixture(t)
	repo := NewMySQLBagRepo(f.db)
	ctx := context.Background()

	t.Run("AppendReplayAndSectionTx", func(t *testing.T) {
		const player = 201
		batch := []*bagv1.BagJournalEntry{
			mkClaimEntry(1, "k1", BagWarehouseType, 0, stackItem(10001, 5)),
			mkClaimEntry(2, "k2", BagWarehouseType, 0, stackItem(10001, 2)),
		}
		acked, err := repo.AppendJournal(ctx, player, 1, batch, bagTestCaps, bagTestMaxStack, 0)
		if err != nil || acked != 2 {
			t.Fatalf("首批 append: acked=%d err=%v", acked, err)
		}
		if got := queryWarehouseCount(t, repo, player, 10001); got != 7 {
			t.Fatalf("仓库应有 7,实际 %d", got)
		}
		// 纯重放:同批重发 → 返回当前水位,不重复应用。
		acked, err = repo.AppendJournal(ctx, player, 1, batch, bagTestCaps, bagTestMaxStack, 0)
		if err != nil || acked != 2 {
			t.Fatalf("重放 append: acked=%d err=%v", acked, err)
		}
		if got := queryWarehouseCount(t, repo, player, 10001); got != 7 {
			t.Fatalf("重放不得二次入账: %d", got)
		}
		// 失败整批回滚:seq3 合法扣除 + seq4 数量不足 → 整批拒,仓库不变、水位不动。
		bad := []*bagv1.BagJournalEntry{
			mkClaimEntry(3, "k3", BagWarehouseType, 0, stackItem(10002, 1)),
			{
				JournalSeq: 4, BagType: BagWarehouseType, IdempotencyKey: "k4",
				Op: &bagv1.BagJournalEntry_Transfer{Transfer: &bagv1.TransferOp{
					ToBagType: 0, Items: []*bagv1.BagItem{stackItem(10001, 99)},
				}},
			},
		}
		if _, err := repo.AppendJournal(ctx, player, 1, bad, bagTestCaps, bagTestMaxStack, 0); errcode.As(err) != errcode.ErrBagItemNotFound {
			t.Fatalf("数量不足应整批拒: %v", err)
		}
		if got := queryWarehouseCount(t, repo, player, 10002); got != 0 {
			t.Fatalf("整批拒后 seq3 不得残留: %d", got)
		}
		if _, _, lastSeq, err := repo.LoadBag(ctx, player, 1); err != nil || lastSeq != 2 {
			t.Fatalf("整批拒后水位应保持 2: last=%d err=%v", lastSeq, err)
		}
	})

	t.Run("EpochFencing", func(t *testing.T) {
		const player = 202
		if _, err := repo.AppendJournal(ctx, player, 5,
			[]*bagv1.BagJournalEntry{mkClaimEntry(1, "k1", BagWarehouseType, 0, stackItem(10001, 1))},
			bagTestCaps, bagTestMaxStack, 0); err != nil {
			t.Fatalf("epoch5 首写: %v", err)
		}
		_, err := repo.AppendJournal(ctx, player, 4,
			[]*bagv1.BagJournalEntry{mkClaimEntry(2, "k2", BagWarehouseType, 0, stackItem(10001, 1))},
			bagTestCaps, bagTestMaxStack, 0)
		if errcode.As(err) != errcode.ErrBagEpochFenced {
			t.Fatalf("旧 epoch 应被 fence: %v", err)
		}
		if _, err := repo.AppendJournal(ctx, player, 6,
			[]*bagv1.BagJournalEntry{mkClaimEntry(2, "k2", BagWarehouseType, 0, stackItem(10001, 1))},
			bagTestCaps, bagTestMaxStack, 0); err != nil {
			t.Fatalf("新 epoch 推进后应可写: %v", err)
		}
		// 推进后旧 epoch 读也拒(LoadBag 同为授权入口)。
		if _, _, _, err := repo.LoadBag(ctx, player, 5); errcode.As(err) != errcode.ErrBagEpochFenced {
			t.Fatalf("旧 epoch LoadBag 应被 fence: %v", err)
		}
	})

	t.Run("ActivityGeneration", func(t *testing.T) {
		const player = 203
		const activity = BagActivityTypeBase
		if _, err := f.db.ExecContext(ctx,
			`INSERT INTO bag_generation (bag_type, current_generation, salvage_mode) VALUES (?, 5, 0)`,
			activity); err != nil {
			t.Fatalf("登记活动代际: %v", err)
		}
		// 代际不符拒。
		_, err := repo.AppendJournal(ctx, player, 1,
			[]*bagv1.BagJournalEntry{mkClaimEntry(1, "k1", activity, 4, stackItem(20001, 1))}, bagTestCaps, bagTestMaxStack, 0)
		if errcode.As(err) != errcode.ErrBagGenerationMismatch {
			t.Fatalf("旧代写应拒: %v", err)
		}
		// 当前代写入成功。
		if _, err := repo.AppendJournal(ctx, player, 1,
			[]*bagv1.BagJournalEntry{mkClaimEntry(1, "k1", activity, 5, stackItem(20001, 3))}, bagTestCaps, bagTestMaxStack, 0); err != nil {
			t.Fatalf("当前代写入: %v", err)
		}
		secs, err := repo.GetSections(ctx, player, []uint32{activity}, bagTestCaps)
		if err != nil || len(secs[0].GetItems()) != 1 {
			t.Fatalf("活动段应有 1 格: %v err=%v", secs, err)
		}
		// 切代:读过滤即逻辑清空;迟到旧代写拒。
		if _, err := f.db.ExecContext(ctx,
			`UPDATE bag_generation SET current_generation = 6 WHERE bag_type = ?`, activity); err != nil {
			t.Fatalf("切代: %v", err)
		}
		secs, err = repo.GetSections(ctx, player, []uint32{activity}, bagTestCaps)
		if err != nil || len(secs[0].GetItems()) != 0 || secs[0].GetGeneration() != 6 {
			t.Fatalf("切代后应读到空段 gen=6: %+v err=%v", secs[0], err)
		}
		_, err = repo.AppendJournal(ctx, player, 1,
			[]*bagv1.BagJournalEntry{mkClaimEntry(2, "k2", activity, 5, stackItem(20001, 1))}, bagTestCaps, bagTestMaxStack, 0)
		if errcode.As(err) != errcode.ErrBagGenerationMismatch {
			t.Fatalf("切代后迟到旧代写应拒: %v", err)
		}
		// 新代从空段开始(类型重用安全)。
		if _, err := repo.AppendJournal(ctx, player, 1,
			[]*bagv1.BagJournalEntry{mkClaimEntry(3, "k3", activity, 6, stackItem(20002, 1))}, bagTestCaps, bagTestMaxStack, 0); err != nil {
			t.Fatalf("新代写入: %v", err)
		}
		secs, _ = repo.GetSections(ctx, player, []uint32{activity}, bagTestCaps)
		if len(secs[0].GetItems()) != 1 || secs[0].GetItems()[0].GetItemConfigId() != 20002 {
			t.Fatalf("新代应只有新物品: %+v", secs[0])
		}
	})

	t.Run("IdempotencyConflict", func(t *testing.T) {
		const player = 204
		if _, err := repo.AppendJournal(ctx, player, 1,
			[]*bagv1.BagJournalEntry{mkClaimEntry(1, "dup", BagWarehouseType, 0, stackItem(10001, 1))},
			bagTestCaps, bagTestMaxStack, 0); err != nil {
			t.Fatalf("首写: %v", err)
		}
		// 同 key 不同内容 + 新 seq → 指纹冲突。
		_, err := repo.AppendJournal(ctx, player, 1,
			[]*bagv1.BagJournalEntry{mkClaimEntry(2, "dup", BagWarehouseType, 0, stackItem(10001, 9))},
			bagTestCaps, bagTestMaxStack, 0)
		if errcode.As(err) != errcode.ErrBagIdempotencyConflict {
			t.Fatalf("key 复用不同内容应冲突: %v", err)
		}
	})

	// CarrySectionConsumeJournalOnly:随身段丢弃扣减的端到端契约(§5.1.1)——
	// 事务内写 bag_journal(op_type=consume)但**绝不建/改 bag_section**;带产出 fail-closed
	// 且整事务回滚(不留半条流水)。数据层单测只验内存变换,这里验真落库的那一半。
	t.Run("CarrySectionConsumeJournalOnly", func(t *testing.T) {
		const player = 216
		carry := &bagv1.BagJournalEntry{
			JournalSeq: 1, BagType: 0, IdempotencyKey: "discard-1",
			Op: &bagv1.BagJournalEntry_Consume{Consume: &bagv1.ConsumeOp{
				ConsumeItems: []*bagv1.BagItem{stackItem(10001, 2)},
			}},
		}
		acked, err := repo.AppendJournal(ctx, player, 1, []*bagv1.BagJournalEntry{carry},
			bagTestCaps, bagTestMaxStack, 0)
		if err != nil || acked != 1 {
			t.Fatalf("随身段 consume 应落库: acked=%d err=%v", acked, err)
		}
		var opType int8
		if qerr := f.db.QueryRowContext(ctx,
			`SELECT op_type FROM bag_journal WHERE player_id = ? AND journal_seq = 1`, player).Scan(&opType); qerr != nil {
			t.Fatalf("读 bag_journal: %v", qerr)
		}
		if opType != BagOpConsume {
			t.Fatalf("op_type 应为 consume(%d),实际 %d", BagOpConsume, opType)
		}
		var sections int
		if qerr := f.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM bag_section WHERE player_id = ?`, player).Scan(&sections); qerr != nil {
			t.Fatalf("读 bag_section: %v", qerr)
		}
		if sections != 0 {
			t.Fatalf("随身段 consume 不得写 bag_section,实际 %d 行", sections)
		}

		// 带产出:整批拒 + 事务回滚,不得留下 seq=2 的流水。
		withProduce := &bagv1.BagJournalEntry{
			JournalSeq: 2, BagType: 0, IdempotencyKey: "discard-produce",
			Op: &bagv1.BagJournalEntry_Consume{Consume: &bagv1.ConsumeOp{
				ConsumeItems:   []*bagv1.BagItem{stackItem(10001, 1)},
				ProduceBagType: BagWarehouseType,
				ProduceItems:   []*bagv1.BagItem{stackItem(20001, 1)},
			}},
		}
		if _, perr := repo.AppendJournal(ctx, player, 1, []*bagv1.BagJournalEntry{withProduce},
			bagTestCaps, bagTestMaxStack, 0); errcode.As(perr) != errcode.ErrBagSectionNotAllowed {
			t.Fatalf("随身段 consume 带产出应拒: %v", perr)
		}
		var rows int
		if qerr := f.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM bag_journal WHERE player_id = ?`, player).Scan(&rows); qerr != nil {
			t.Fatalf("读 bag_journal 计数: %v", qerr)
		}
		if rows != 1 {
			t.Fatalf("拒批必须整事务回滚,bag_journal 应只有 1 行,实际 %d", rows)
		}
	})

	t.Run("CheckpointAndLoad", func(t *testing.T) {
		const player = 205
		batch := []*bagv1.BagJournalEntry{
			mkClaimEntry(1, "k1", BagWarehouseType, 0, stackItem(10001, 1)),
			mkClaimEntry(2, "k2", BagWarehouseType, 0, stackItem(10001, 1)),
			mkClaimEntry(3, "k3", BagWarehouseType, 0, stackItem(10001, 1)),
		}
		if _, err := repo.AppendJournal(ctx, player, 1, batch, bagTestCaps, bagTestMaxStack, 0); err != nil {
			t.Fatalf("append: %v", err)
		}
		record := &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{{BagType: 0, Capacity: 100}}}
		blob, _ := proto.Marshal(record)
		if err := repo.SaveCheckpoint(ctx, player, 1, blob, 2); err != nil {
			t.Fatalf("checkpoint covered=2: %v", err)
		}
		if err := repo.SaveCheckpoint(ctx, player, 1, blob, 1); errcode.As(err) != errcode.ErrBagCheckpointStale {
			t.Fatalf("covered 回退应拒: %v", err)
		}
		if err := repo.SaveCheckpoint(ctx, player, 1, blob, 9); errcode.As(err) != errcode.ErrBagCheckpointStale {
			t.Fatalf("covered 超水位应拒: %v", err)
		}
		snapshot, tail, lastSeq, err := repo.LoadBag(ctx, player, 1)
		if err != nil || lastSeq != 3 {
			t.Fatalf("load: last=%d err=%v", lastSeq, err)
		}
		if len(snapshot) == 0 {
			t.Fatal("load 应带 checkpoint 快照")
		}
		if len(tail) != 1 || tail[0].JournalSeq != 3 {
			t.Fatalf("尾部应只含 covered 之后的 seq3: %+v", tail)
		}
	})

	t.Run("SweepRespectsCheckpointCoverage", func(t *testing.T) {
		// INC-20260722-003 回归:删除资格 = 超保留期 **且** journal_seq <= covered_journal_seq。
		// 未覆盖尾部是唯一恢复数据,时间到期也绝不删;无 checkpoint 的玩家任何流水都不删。
		const covered, uncovered = 207, 208
		record := &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{{BagType: 0, Capacity: 100}}}
		blob, _ := proto.Marshal(record)

		for _, player := range []uint64{covered, uncovered} {
			batch := []*bagv1.BagJournalEntry{
				mkClaimEntry(1, "k1", BagWarehouseType, 0, stackItem(10001, 1)),
				mkClaimEntry(2, "k2", BagWarehouseType, 0, stackItem(10001, 1)),
				mkClaimEntry(3, "k3", BagWarehouseType, 0, stackItem(10001, 1)),
			}
			if _, err := repo.AppendJournal(ctx, player, 1, batch, bagTestCaps, bagTestMaxStack, 0); err != nil {
				t.Fatalf("append player=%d: %v", player, err)
			}
		}
		// 玩家 207 checkpoint 覆盖到 seq2;玩家 208 从未 checkpoint。
		if err := repo.SaveCheckpoint(ctx, covered, 1, blob, 2); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		// 两名玩家的全部流水都超保留期(100 天前)。
		if _, err := f.db.Exec(
			`UPDATE bag_journal SET created_at = DATE_SUB(NOW(), INTERVAL 100 DAY) WHERE player_id IN (?, ?)`,
			covered, uncovered); err != nil {
			t.Fatalf("backdate journal: %v", err)
		}

		n, err := repo.SweepJournal(ctx, 90*24*time.Hour, 100)
		if err != nil {
			t.Fatalf("sweep: %v", err)
		}
		if n != 2 {
			t.Fatalf("sweep 应只删玩家 %d 的 seq1/seq2(已覆盖且超期),实际删了 %d 行", covered, n)
		}
		assertBagCount(t, f.db, `SELECT COUNT(*) FROM bag_journal WHERE player_id = ?`, covered, 1)
		assertBagCount(t, f.db, `SELECT COUNT(*) FROM bag_journal WHERE player_id = ? AND journal_seq = 3`, covered, 1)
		assertBagCount(t, f.db, `SELECT COUNT(*) FROM bag_journal WHERE player_id = ?`, uncovered, 3)

		// 清理后两名玩家都必须仍可无损加载(尾部连续性成立)。
		if _, tail, lastSeq, err := repo.LoadBag(ctx, covered, 1); err != nil || lastSeq != 3 || len(tail) != 1 {
			t.Fatalf("covered player load: tail=%d last=%d err=%v", len(tail), lastSeq, err)
		}
		if _, tail, lastSeq, err := repo.LoadBag(ctx, uncovered, 1); err != nil || lastSeq != 3 || len(tail) != 3 {
			t.Fatalf("uncovered player load: tail=%d last=%d err=%v", len(tail), lastSeq, err)
		}

		// 小批量排空:再补一名已覆盖玩家,batch=1 分两次删光(sweep 循环的 SQL 配合面)。
		if _, err := f.db.Exec(
			`UPDATE bag_checkpoint SET covered_journal_seq = 3 WHERE player_id = ?`, covered); err != nil {
			t.Fatalf("advance coverage: %v", err)
		}
		if n, err := repo.SweepJournal(ctx, 90*24*time.Hour, 1); err != nil || n != 1 {
			t.Fatalf("batch=1 sweep: n=%d err=%v", n, err)
		}
		assertBagCount(t, f.db, `SELECT COUNT(*) FROM bag_journal WHERE player_id = ?`, covered, 0)
	})

	t.Run("LoadBagTailGapFailsClosed", func(t *testing.T) {
		// INC-20260722-003 回归:(covered, last] 尾部出现缺口(误删/损坏)时,LoadBag 必须
		// 拒绝加载(静默继续 = 玩家资产静默丢失)。
		const player = 209
		batch := []*bagv1.BagJournalEntry{
			mkClaimEntry(1, "k1", BagWarehouseType, 0, stackItem(10001, 1)),
			mkClaimEntry(2, "k2", BagWarehouseType, 0, stackItem(10001, 1)),
			mkClaimEntry(3, "k3", BagWarehouseType, 0, stackItem(10001, 1)),
		}
		if _, err := repo.AppendJournal(ctx, player, 1, batch, bagTestCaps, bagTestMaxStack, 0); err != nil {
			t.Fatalf("append: %v", err)
		}
		// 模拟越权清理/损坏:挖掉中间的 seq2。
		if _, err := f.db.Exec(
			`DELETE FROM bag_journal WHERE player_id = ? AND journal_seq = 2`, player); err != nil {
			t.Fatalf("simulate gap: %v", err)
		}
		if _, _, _, err := repo.LoadBag(ctx, player, 1); err == nil {
			t.Fatal("tail gap 必须 fail-closed 拒绝加载,实际成功返回(静默丢资产)")
		}
		// 挖掉末尾 seq3(尾部截断,水位 3 但尾部只到 1)同样拒。
		if _, err := f.db.Exec(
			`DELETE FROM bag_journal WHERE player_id = ? AND journal_seq = 3`, player); err != nil {
			t.Fatalf("simulate truncation: %v", err)
		}
		if _, _, _, err := repo.LoadBag(ctx, player, 1); err == nil {
			t.Fatal("tail truncation 必须 fail-closed 拒绝加载")
		}
	})

	t.Run("HourlyQuota", func(t *testing.T) {
		const player = 206
		if _, err := repo.AppendJournal(ctx, player, 1,
			[]*bagv1.BagJournalEntry{
				mkClaimEntry(1, "k1", BagWarehouseType, 0, stackItem(10001, 1)),
				mkClaimEntry(2, "k2", BagWarehouseType, 0, stackItem(10001, 1)),
			}, bagTestCaps, bagTestMaxStack, 3); err != nil {
			t.Fatalf("额度内写入: %v", err)
		}
		_, err := repo.AppendJournal(ctx, player, 1,
			[]*bagv1.BagJournalEntry{
				mkClaimEntry(3, "k3", BagWarehouseType, 0, stackItem(10001, 1)),
				mkClaimEntry(4, "k4", BagWarehouseType, 0, stackItem(10001, 1)),
			}, bagTestCaps, bagTestMaxStack, 3)
		if errcode.As(err) != errcode.ErrBagQuotaExceeded {
			t.Fatalf("超额应拒: %v", err)
		}
	})
}

// assertBagCount 断言带单个 player 参数的计数查询结果。
func assertBagCount(t *testing.T, db *sql.DB, q string, player uint64, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(q, player).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", q, err)
	}
	if got != want {
		t.Fatalf("%s player=%d = %d, want %d", q, player, got, want)
	}
}
