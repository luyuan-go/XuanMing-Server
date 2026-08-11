// mission_outbox_mysql_test.go — 任务事实出箱「每玩家 FIFO + USE_ITEM 扣除闸」的真实
// MySQL 回归(2026-08-11)。
//
// 两条被验证的性质都在 SQL 谓词里,fake repo 复制不了:
//
//	① 每玩家严格 FIFO:FetchMissionOutbox 的 NOT EXISTS 前驱谓词。修复前本表按 id 序
//	   平摊投递,DeferMissionOutbox 一退避,同玩家后续事实就越过队首先投 —— 任务链前后
//	   两环条件类别不同时(「杀 5 只狼」→「收集 3 张狼皮」),后环事实提前到达会匹配不上
//	   任何活跃任务,被 mission 侧收据吸收后**静默丢失且永不重放**;
//	② USE_ITEM 的 pending_action 闸:局内消费扣除以业务失败终态收场时(道具不足),
//	   inventory 一件没扣,任务事实必须**跟着删掉**;扣成功才放行。修复前事实与扣除
//	   各走各的,「使用 N 个 X」型任务能靠上报根本没发生的消耗刷完(§9.6 不信 DS)。
//
// 门控:PANDORA_TEST_MYSQL_DSN(不带库名),复用 openBattleRetentionDB 的随机临时库夹具
// (直接重放 deploy/mysql-init/05-battle-outbox.sql,杜绝测试内另抄 DDL 的漂移)。
//
//	go test ./services/battle/battle_result/internal/data/ -count=1 -run MissionOutbox -v
package data

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// insertMissionFacts 走生产写入路径(ApplyProgress 事务内的同一个函数)落行。
func insertMissionFacts(t *testing.T, r *MySQLBattleRepo, rows []MissionFactRecord) {
	t.Helper()
	ctx := context.Background()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := insertMissionFactsTx(ctx, tx, rows, 0); err != nil {
		_ = tx.Rollback()
		t.Fatalf("insert mission facts: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func fetchedSeqs(t *testing.T, r *MySQLBattleRepo, limit int) []uint64 {
	t.Helper()
	recs, err := r.FetchMissionOutbox(context.Background(), limit)
	if err != nil {
		t.Fatalf("fetch mission outbox: %v", err)
	}
	out := make([]uint64, 0, len(recs))
	for _, rec := range recs {
		out = append(out, rec.Seq)
	}
	return out
}

// TestMissionOutbox_PerPlayerFIFOBlocksOnDeferredHead —— 队首退避时,同玩家后续事实
// 必须一起等;其它玩家不受影响。
func TestMissionOutbox_PerPlayerFIFOBlocksOnDeferredHead(t *testing.T) {
	db := openBattleRetentionDB(t)
	r := NewMySQLBattleRepo(db)
	const matchID = uint64(7001)

	// 同一玩家 3 条事实(seq 1/2/3)+ 另一玩家 1 条(seq 4)。
	insertMissionFacts(t, r, []MissionFactRecord{
		{MatchID: matchID, Seq: 1, PlayerID: 11, Category: 1, SlotValue: 101, Amount: 1},
		{MatchID: matchID, Seq: 2, PlayerID: 11, Category: 9, SlotValue: 201, Amount: 1},
		{MatchID: matchID, Seq: 3, PlayerID: 11, Category: 1, SlotValue: 102, Amount: 1},
		{MatchID: matchID, Seq: 4, PlayerID: 22, Category: 1, SlotValue: 103, Amount: 1},
	})

	// 每玩家只出队首:玩家 11 的 seq=1、玩家 22 的 seq=4。
	if got := fetchedSeqs(t, r, 64); len(got) != 2 || got[0] != 1 || got[1] != 4 {
		t.Fatalf("首轮取到 seq=%v, want [1 4](每玩家只出队首)", got)
	}

	// 队首投递失败退避 → 同玩家 seq=2/3 **不得**越过它先投。
	head, err := r.FetchMissionOutbox(context.Background(), 64)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	var headID int64
	for _, rec := range head {
		if rec.PlayerID == 11 {
			headID = rec.ID
		}
	}
	if headID == 0 {
		t.Fatal("未取到玩家 11 的队首行")
	}
	if err := r.DeferMissionOutbox(context.Background(), headID); err != nil {
		t.Fatalf("defer: %v", err)
	}
	got := fetchedSeqs(t, r, 64)
	if len(got) != 1 || got[0] != 4 {
		t.Fatalf("队首退避后取到 seq=%v, want [4](同玩家后续事实必须一起等,其它玩家照常)", got)
	}

	// 队首投递成功删行后,seq=2 才成为新队首(顺序不被破坏)。
	if err := r.DeleteMissionOutbox(context.Background(), headID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got := fetchedSeqs(t, r, 64); len(got) != 2 || got[0] != 2 || got[1] != 4 {
		t.Fatalf("队首出队后取到 seq=%v, want [2 4]", got)
	}
}

// TestMissionOutbox_UseItemPendingUntilConsumeResolved —— USE_ITEM 事实在扣除落定前
// 既不可投递、也挡住同玩家后续事实;扣除成功放行、失败删行。
func TestMissionOutbox_UseItemPendingUntilConsumeResolved(t *testing.T) {
	ctx := context.Background()
	db := openBattleRetentionDB(t)
	r := NewMySQLBattleRepo(db)
	const matchID = uint64(7002)

	insertMissionFacts(t, r, []MissionFactRecord{
		{MatchID: matchID, Seq: 1, PlayerID: 33, Category: 4, SlotValue: 5001, Amount: 1, PendingAction: true},
		{MatchID: matchID, Seq: 2, PlayerID: 33, Category: 1, SlotValue: 101, Amount: 1},
		{MatchID: matchID, Seq: 1, PlayerID: 44, Category: 4, SlotValue: 5001, Amount: 2, PendingAction: true},
	})

	// pending 行不投递,且占队首挡住同玩家 seq=2 —— 本轮一条都取不到。
	if got := fetchedSeqs(t, r, 64); len(got) != 0 {
		t.Fatalf("扣除未落定就取到 seq=%v, want 空(pending 闸 + 队首阻塞)", got)
	}

	// 扣除成功 → 放行(玩家 33)。
	if err := settleMissionFactPending(ctx, r, matchID, 1, 33, true); err != nil {
		t.Fatalf("settle granted: %v", err)
	}
	if got := fetchedSeqs(t, r, 64); len(got) != 1 || got[0] != 1 {
		t.Fatalf("扣除成功后取到 seq=%v, want [1]", got)
	}

	// 扣除以业务失败终态收场 → 事实删行(玩家 44),「使用道具」任务不得推进。
	if err := settleMissionFactPending(ctx, r, matchID, 1, 44, false); err != nil {
		t.Fatalf("settle failed: %v", err)
	}
	var remain int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM battle_mission_outbox WHERE match_id=? AND player_id=?",
		matchID, 44).Scan(&remain); err != nil {
		t.Fatalf("count player 44 rows: %v", err)
	}
	if remain != 0 {
		t.Fatalf("扣除失败后仍留 %d 行任务事实:没扣到东西却能刷「使用 N 个 X」", remain)
	}
}

// settleMissionFactPending 是 settleMissionFactPendingTx 的独立事务包装,只给测试用
// (生产路径必须与扣除结果同事务,见 ResolveProgressAction)。
func settleMissionFactPending(ctx context.Context, r *MySQLBattleRepo, matchID, seq, playerID uint64, granted bool) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "begin settle: %v", err)
	}
	if err := settleMissionFactPendingTx(ctx, tx, matchID, seq, playerID, granted); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}
