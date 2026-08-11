// progress_repo_mysql_test.go — 实时进度累计上限与水位 CAS 的真实 MySQL 回归。
package data

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// openBattleProgressPeerDB 复用 openBattleRetentionDB 创建的严格随机临时库，再建立
// 一个独立、单物理连接的连接池。连接 ID 必须不同，确保测试确实模拟两个服务副本，
// 而不是同一连接上的顺序调用。
func openBattleProgressPeerDB(t *testing.T, primary *sql.DB) *sql.DB {
	t.Helper()
	primary.SetMaxOpenConns(1)
	primary.SetMaxIdleConns(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var dbName string
	if err := primary.QueryRowContext(ctx, `SELECT DATABASE()`).Scan(&dbName); err != nil {
		t.Fatalf("读取随机测试库名: %v", err)
	}
	if !battleRetentionTestDBPattern.MatchString(dbName) {
		t.Fatalf("主连接不在受保护的随机测试库: %q", dbName)
	}

	cfg, err := mysql.ParseDSN(os.Getenv("PANDORA_TEST_MYSQL_DSN"))
	if err != nil {
		t.Fatalf("解析第二连接 DSN: %v", err)
	}
	if cfg.DBName != "" {
		t.Fatalf("PANDORA_TEST_MYSQL_DSN 禁止带 database, got=%q", cfg.DBName)
	}
	cfg.DBName = dbName
	cfg.ParseTime = true
	cfg.Timeout = 5 * time.Second
	peer, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatalf("打开第二连接: %v", err)
	}
	peer.SetMaxOpenConns(1)
	peer.SetMaxIdleConns(1)
	t.Cleanup(func() { _ = peer.Close() })
	if err := peer.PingContext(ctx); err != nil {
		t.Fatalf("连接随机测试库的第二连接: %v", err)
	}

	var primaryID, peerID uint64
	if err := primary.QueryRowContext(ctx, `SELECT CONNECTION_ID()`).Scan(&primaryID); err != nil {
		t.Fatalf("读取主连接 ID: %v", err)
	}
	if err := peer.QueryRowContext(ctx, `SELECT CONNECTION_ID()`).Scan(&peerID); err != nil {
		t.Fatalf("读取第二连接 ID: %v", err)
	}
	if primaryID == peerID {
		t.Fatalf("测试未建立两个独立 MySQL 连接: connection_id=%d", primaryID)
	}
	return peer
}

// TestApplyProgressPlayerCapAndStaleCAS_MySQL 控制交错如下：连接 A 读取 seq=1 的
// 水位；连接 B 把同一玩家累计推进到上限并提交 seq=2；连接 A 再携带旧水位重放。
// 旧请求必须先在 CAS 处得到 UNAVAILABLE，不能把竞争误判为永久超限；真正基于最新
// 水位的超限批次则必须返回 INVALID_ARG，并整体回滚水位、玩家累计和 outbox。
func TestApplyProgressPlayerCapAndStaleCAS_MySQL(t *testing.T) {
	primary := openBattleRetentionDB(t)
	peer := openBattleProgressPeerDB(t, primary)
	repoA := NewMySQLBattleRepo(primary)
	repoB := NewMySQLBattleRepo(peer)
	ctx := context.Background()

	const (
		matchID  = uint64(88001)
		playerID = uint64(99001)
	)
	caps := ProgressCaps{
		MatchExp: 1000, MatchItems: 100,
		PlayerExp: 100, PlayerItems: 100, PlayerKills: 100,
	}
	row := func(seq, exp uint64) []ProgressOutboxRecord {
		return []ProgressOutboxRecord{{
			Seq: seq, PlayerID: playerID, Kind: ProgressGrantExp, ExpDelta: exp,
		}}
	}

	if err := repoA.ApplyProgress(ctx, matchID, 0, 1, 90, 0,
		[]ProgressPlayerDelta{{PlayerID: playerID, Exp: 90}}, row(1, 90), caps); err != nil {
		t.Fatalf("建立 seq=1 初始进度: %v", err)
	}
	stale, err := repoA.GetProgressWatermark(ctx, matchID)
	if err != nil || !stale.Existed || stale.LastAppliedSeq != 1 || stale.TotalExp != 90 {
		t.Fatalf("连接 A 读取旧水位: %+v err=%v", stale, err)
	}

	// 连接 B 在 A 保留旧快照期间提交，把单玩家累计精确推到上限。
	if err := repoB.ApplyProgress(ctx, matchID, stale.LastAppliedSeq, 2, 10, 0,
		[]ProgressPlayerDelta{{PlayerID: playerID, Exp: 10}}, row(2, 10), caps); err != nil {
		t.Fatalf("连接 B 推进 seq=2: %v", err)
	}

	// A 重放同一批；正确分类是水位竞争，而不是把已由 B 计入的 delta 再算一次后报超限。
	err = repoA.ApplyProgress(ctx, matchID, stale.LastAppliedSeq, 2, 10, 0,
		[]ProgressPlayerDelta{{PlayerID: playerID, Exp: 10}}, row(2, 10), caps)
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("旧水位重放 code=%d err=%v, want ErrUnavailable", errcode.As(err), err)
	}
	assertProgressMySQLState(t, primary, repoA, matchID, playerID, 2, 100, 2)

	// 重新读取最新水位后提交真实超限的新批，必须永久拒绝且事务零副作用。
	fresh, err := repoA.GetProgressWatermark(ctx, matchID)
	if err != nil || fresh.LastAppliedSeq != 2 {
		t.Fatalf("读取最新水位: %+v err=%v", fresh, err)
	}
	err = repoA.ApplyProgress(ctx, matchID, fresh.LastAppliedSeq, 3, 1, 0,
		[]ProgressPlayerDelta{{PlayerID: playerID, Exp: 1}}, row(3, 1), caps)
	if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("真实单玩家超限 code=%d err=%v, want ErrInvalidArg", errcode.As(err), err)
	}
	assertProgressMySQLState(t, primary, repoA, matchID, playerID, 2, 100, 2)
}

// TestFetchProgressOutboxStrictPlayerOrder_MySQL 锁住真实 SQL 的同玩家严格顺序：
// 前序拾取发放即使已退避到未来，后序消费/丢弃也不得越过；其它玩家不受阻塞。
// 前序行成功删除后，后序才变为可取。
func TestFetchProgressOutboxStrictPlayerOrder_MySQL(t *testing.T) {
	db := openBattleRetentionDB(t)
	repo := NewMySQLBattleRepo(db)
	ctx := context.Background()

	const (
		matchID       = uint64(88002)
		consumePlayer = uint64(99101)
		discardPlayer = uint64(99102)
		otherPlayer   = uint64(99103)
	)
	consumeGrantID := insertProgressOutbox(t, db, matchID, 1, consumePlayer, ProgressGrantStack, "10001")
	insertProgressOutbox(t, db, matchID, 2, consumePlayer, ProgressConsumeStack, "10001")
	discardGrantID := insertProgressOutbox(t, db, matchID, 1, discardPlayer, ProgressGrantStack, "10006")
	insertProgressOutbox(t, db, matchID, 2, discardPlayer, ProgressDiscardStack, "10006")
	otherID := insertProgressOutbox(t, db, matchID, 1, otherPlayer, ProgressGrantStack, "10007")

	// 模拟两个玩家的 seq1 Grant 下游失败并进入退避；seq2 虽然到期，也必须被 seq1 挡住。
	if err := repo.DeferProgressOutbox(ctx, consumeGrantID); err != nil {
		t.Fatalf("defer consume player's grant: %v", err)
	}
	if err := repo.DeferProgressOutbox(ctx, discardGrantID); err != nil {
		t.Fatalf("defer discard player's grant: %v", err)
	}
	got, err := repo.FetchProgressOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("FetchProgressOutbox with deferred heads: %v", err)
	}
	if len(got) != 1 || got[0].ID != otherID || got[0].PlayerID != otherPlayer {
		t.Fatalf("退避头行只能阻塞同玩家，got=%+v want other player id=%d", got, otherID)
	}
	if err := repo.DeleteProgressOutbox(ctx, otherID); err != nil {
		t.Fatalf("delete other player's due row: %v", err)
	}
	got, err = repo.FetchProgressOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("FetchProgressOutbox while both heads deferred: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("前序退避时后序不得越过，got=%+v", got)
	}

	// 模拟两个 seq1 Grant 已成功投递并删除；对应 seq2 Consume/Discard 立即解锁。
	if err := repo.DeleteProgressOutbox(ctx, consumeGrantID); err != nil {
		t.Fatalf("delete consume player's grant: %v", err)
	}
	if err := repo.DeleteProgressOutbox(ctx, discardGrantID); err != nil {
		t.Fatalf("delete discard player's grant: %v", err)
	}
	got, err = repo.FetchProgressOutbox(ctx, 10)
	if err != nil {
		t.Fatalf("FetchProgressOutbox after deleting heads: %v", err)
	}
	if len(got) != 2 || got[0].PlayerID != consumePlayer || got[0].Seq != 2 || got[0].Kind != ProgressConsumeStack ||
		got[1].PlayerID != discardPlayer || got[1].Seq != 2 || got[1].Kind != ProgressDiscardStack {
		t.Fatalf("删除前序后应解锁 consume+discard，got=%+v", got)
	}
}

func TestProgressItemActionAuthorityAndOutcome_MySQL(t *testing.T) {
	db := openBattleRetentionDB(t)
	repo := NewMySQLBattleRepo(db)
	ctx := context.Background()
	const (
		matchID  = uint64(88003)
		playerID = uint64(99201)
		itemID   = uint32(10001)
	)
	caps := ProgressCaps{MatchExp: 1000, MatchItems: 100, PlayerExp: 1000, PlayerItems: 100, PlayerKills: 100}
	pickup := ProgressOutboxRecord{Seq: 1, PlayerID: playerID, Kind: ProgressGrantStack, ItemConfigIDs: []uint32{itemID, itemID}}
	if err := repo.ApplyProgress(ctx, matchID, 0, 1, 0, 2,
		[]ProgressPlayerDelta{{PlayerID: playerID, Items: 2}}, []ProgressOutboxRecord{pickup}, caps); err != nil {
		t.Fatalf("apply pickup: %v", err)
	}

	// 跨 item 和超余额都必须整体回滚水位、action、outbox。
	cross := ProgressOutboxRecord{Seq: 2, PlayerID: playerID, Kind: ProgressDiscardStack, ItemConfigIDs: []uint32{10002}}
	if err := repo.ApplyProgress(ctx, matchID, 1, 2, 0, 0, nil, []ProgressOutboxRecord{cross}, caps); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("cross-item action err=%v", err)
	}
	assertActionBalanceMySQL(t, db, matchID, playerID, itemID, 2, 0, 0)

	action := ProgressOutboxRecord{Seq: 2, PlayerID: playerID, Kind: ProgressConsumeStack, ItemConfigIDs: []uint32{itemID, itemID}}
	if err := repo.ApplyProgress(ctx, matchID, 1, 2, 0, 0, nil, []ProgressOutboxRecord{action}, caps); err != nil {
		t.Fatalf("reserve action: %v", err)
	}
	assertActionBalanceMySQL(t, db, matchID, playerID, itemID, 2, 2, 1)

	// 同步路径先清前序 grant，再处理 action。终态失败必须同事务持久结果、删 action
	// outbox 并释放预留；重读稳定回放原始业务码。
	head, ok, err := repo.FetchProgressOutboxForPlayer(ctx, matchID, playerID, 2)
	if err != nil || !ok || head.Seq != 1 || head.Kind != ProgressGrantStack {
		t.Fatalf("first player row=%+v ok=%v err=%v", head, ok, err)
	}
	if err := repo.DeleteProgressOutbox(ctx, head.ID); err != nil {
		t.Fatalf("delete simulated granted pickup: %v", err)
	}
	actionRow, ok, err := repo.FetchProgressOutboxForPlayer(ctx, matchID, playerID, 2)
	if err != nil || !ok || actionRow.Kind != ProgressConsumeStack {
		t.Fatalf("action row=%+v ok=%v err=%v", actionRow, ok, err)
	}
	resolved, err := repo.ResolveProgressAction(ctx, actionRow, errcode.ErrInventoryInsufficient)
	if err != nil || resolved.Status != ProgressActionFailed || resolved.ResultCode != errcode.ErrInventoryInsufficient {
		t.Fatalf("resolve failure=%+v err=%v", resolved, err)
	}
	assertActionBalanceMySQL(t, db, matchID, playerID, itemID, 2, 0, 0)
	replayed, found, err := repo.GetProgressAction(ctx, matchID, 2, playerID, ProgressConsumeStack)
	if err != nil || !found || replayed.Status != ProgressActionFailed || replayed.ResultCode != errcode.ErrInventoryInsufficient {
		t.Fatalf("replay failure=%+v found=%v err=%v", replayed, found, err)
	}

	// 失败释放后下一 seq 可重新预留；成功则不释放，后续再次支出被拒。
	action.Seq = 3
	if err := repo.ApplyProgress(ctx, matchID, 2, 3, 0, 0, nil, []ProgressOutboxRecord{action}, caps); err != nil {
		t.Fatalf("reserve action after failure: %v", err)
	}
	actionRow, ok, err = repo.FetchProgressOutboxForPlayer(ctx, matchID, playerID, 3)
	if err != nil || !ok || actionRow.Seq != 3 {
		t.Fatalf("next action row=%+v ok=%v err=%v", actionRow, ok, err)
	}
	resolved, err = repo.ResolveProgressAction(ctx, actionRow, errcode.OK)
	if err != nil || resolved.Status != ProgressActionSucceeded {
		t.Fatalf("resolve success=%+v err=%v", resolved, err)
	}
	assertActionBalanceMySQL(t, db, matchID, playerID, itemID, 2, 2, 0)
	action.Seq = 4
	action.ItemConfigIDs = []uint32{itemID}
	if err := repo.ApplyProgress(ctx, matchID, 3, 4, 0, 0, nil, []ProgressOutboxRecord{action}, caps); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("successful reservation reused err=%v", err)
	}
}

func TestProgressItemActionConcurrentReservation_MySQL(t *testing.T) {
	primary := openBattleRetentionDB(t)
	peer := openBattleProgressPeerDB(t, primary)
	repoA, repoB := NewMySQLBattleRepo(primary), NewMySQLBattleRepo(peer)
	ctx := context.Background()
	const (
		matchID  = uint64(88004)
		playerID = uint64(99202)
		itemID   = uint32(10001)
	)
	caps := ProgressCaps{MatchExp: 1000, MatchItems: 100, PlayerExp: 1000, PlayerItems: 100, PlayerKills: 100}
	pickup := ProgressOutboxRecord{Seq: 1, PlayerID: playerID, Kind: ProgressGrantStack, ItemConfigIDs: []uint32{itemID}}
	if err := repoA.ApplyProgress(ctx, matchID, 0, 1, 0, 1,
		[]ProgressPlayerDelta{{PlayerID: playerID, Items: 1}}, []ProgressOutboxRecord{pickup}, caps); err != nil {
		t.Fatalf("apply pickup: %v", err)
	}
	action := []ProgressOutboxRecord{{Seq: 2, PlayerID: playerID, Kind: ProgressDiscardStack, ItemConfigIDs: []uint32{itemID}}}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, repo := range []*MySQLBattleRepo{repoA, repoB} {
		go func(repo *MySQLBattleRepo) {
			<-start
			errs <- repo.ApplyProgress(ctx, matchID, 1, 2, 0, 0, nil, action, caps)
		}(repo)
	}
	close(start)
	var success, unavailable int
	for range 2 {
		err := <-errs
		switch errcode.As(err) {
		case errcode.OK:
			success++
		case errcode.ErrUnavailable:
			unavailable++
		default:
			t.Fatalf("concurrent action unexpected err=%v", err)
		}
	}
	if success != 1 || unavailable != 1 {
		t.Fatalf("concurrent results success=%d unavailable=%d", success, unavailable)
	}
	assertActionBalanceMySQL(t, primary, matchID, playerID, itemID, 1, 1, 1)
}

func assertActionBalanceMySQL(t *testing.T, db *sql.DB, matchID, playerID uint64, itemID uint32, wantPicked, wantSpent uint64, wantPendingOutbox int) {
	t.Helper()
	var picked, spent uint64
	if err := db.QueryRow(`SELECT picked_count,spent_count FROM battle_progress_item_balance
WHERE match_id=? AND player_id=? AND item_config_id=?`, matchID, playerID, itemID).Scan(&picked, &spent); err != nil {
		t.Fatalf("read action balance: %v", err)
	}
	if picked != wantPicked || spent != wantSpent {
		t.Fatalf("balance picked/spent=%d/%d want=%d/%d", picked, spent, wantPicked, wantSpent)
	}
	var pending int
	if err := db.QueryRow(`SELECT COUNT(*) FROM battle_progress_outbox
WHERE match_id=? AND player_id=? AND kind IN (?,?)`, matchID, playerID,
		uint8(ProgressConsumeStack), uint8(ProgressDiscardStack)).Scan(&pending); err != nil {
		t.Fatalf("count action outbox: %v", err)
	}
	if pending != wantPendingOutbox {
		t.Fatalf("action outbox=%d want=%d", pending, wantPendingOutbox)
	}
}

func insertProgressOutbox(t *testing.T, db *sql.DB, matchID, seq, playerID uint64, kind ProgressGrantKind, itemIDs string) int64 {
	t.Helper()
	result, err := db.Exec(`INSERT INTO battle_progress_outbox
(match_id,seq,player_id,kind,item_config_ids,next_attempt_at_ms,created_at_ms)
VALUES(?,?,?,?,?,0,?)`, matchID, seq, playerID, uint8(kind), itemIDs, time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("insert progress outbox match=%d seq=%d player=%d kind=%d: %v", matchID, seq, playerID, kind, err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read inserted progress outbox id: %v", err)
	}
	return id
}

func assertProgressMySQLState(t *testing.T, db *sql.DB, repo *MySQLBattleRepo,
	matchID, playerID, wantSeq, wantPlayerExp uint64, wantOutbox int,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wm, err := repo.GetProgressWatermark(ctx, matchID)
	if err != nil || wm.LastAppliedSeq != wantSeq || wm.TotalExp != wantPlayerExp {
		t.Fatalf("水位状态 %+v err=%v, want seq=%d total_exp=%d", wm, err, wantSeq, wantPlayerExp)
	}
	var playerExp uint64
	if err := db.QueryRowContext(ctx,
		`SELECT total_exp FROM battle_progress_player WHERE match_id = ? AND player_id = ?`,
		matchID, playerID).Scan(&playerExp); err != nil {
		t.Fatalf("读取单玩家累计: %v", err)
	}
	if playerExp != wantPlayerExp {
		t.Fatalf("player total_exp=%d, want %d", playerExp, wantPlayerExp)
	}
	var outbox int
	if err := db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM battle_progress_outbox WHERE match_id = ?`, matchID).Scan(&outbox); err != nil {
		t.Fatalf("统计进度 outbox: %v", err)
	}
	if outbox != wantOutbox {
		t.Fatalf("progress outbox rows=%d, want %d", outbox, wantOutbox)
	}
}
