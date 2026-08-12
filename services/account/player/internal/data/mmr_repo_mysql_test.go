// mmr_repo_mysql_test.go — ApplyMMRChange 的真实 MySQL 集成回归(2026-08-11 补)。
//
// 为什么必须跑真库:ApplyMMRChange 的正确性完全建立在**引擎行为**上 ——
//   - `uk_player_idem` 撞键才走幂等分支(fake 里"我记得见过这个 key"是另一回事);
//   - `SELECT ... FOR UPDATE` 把同玩家的并发结算串行化,否则 old_mmr 会被两侧读成同一值;
//   - players.total_battles / total_wins 是**累加列**,幂等分支必须整段跳过 UPDATE,
//     少跳一次战绩就永久多一场(不变量 §2,且事后无法从数据里反推该修正几次)。
//
// 这三条 sqlmock 都只能验"发出了哪条 SQL",验不出"引擎收到后发生了什么"。
package data

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

const mmrTestTimeout = 20 * time.Second

// TestApplyMMRChangeIdempotency_MySQL —— 同一 idempotency_key 重放不得二次记账。
func TestApplyMMRChangeIdempotency_MySQL(t *testing.T) {
	db := newPlayerSchemaDB(t)
	repo := NewMySQLPlayerRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), mmrTestTimeout)
	defer cancel()

	const playerID = uint64(70_001)
	seedPlayerProfile(t, repo, playerID, 1500)

	change := MMRChange{
		PlayerID:       playerID,
		IdempotencyKey: "match-70001-a",
		Delta:          25,
		Reason:         "win",
		Floor:          0,
		IncBattle:      true,
		IncWin:         true,
	}
	newMMR, already, err := repo.ApplyMMRChange(ctx, change)
	if err != nil {
		t.Fatalf("首次结算: %v", err)
	}
	if already {
		t.Fatal("首次结算不应命中幂等")
	}
	if newMMR != 1525 {
		t.Fatalf("首次结算 mmr=%d, want 1525", newMMR)
	}

	// 重放:同 key、同内容。第二次必须什么都不改,并回放已记录的 new_mmr。
	replayMMR, replayAlready, err := repo.ApplyMMRChange(ctx, change)
	if err != nil {
		t.Fatalf("重放结算: %v", err)
	}
	if !replayAlready {
		t.Fatal("重放同一 idempotency_key 必须命中幂等(uk_player_idem 没起作用)")
	}
	if replayMMR != newMMR {
		t.Fatalf("重放回放 mmr=%d, want %d(幂等分支必须回放已记录值,不得重算)", replayMMR, newMMR)
	}

	mmr, battles, wins := playerCounters(t, db, playerID)
	if mmr != 1525 || battles != 1 || wins != 1 {
		t.Fatalf("重放后 mmr=%d battles=%d wins=%d, want 1525/1/1(累加列被重复记账)", mmr, battles, wins)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM mmr_history WHERE player_id = ?`, playerID); got != 1 {
		t.Fatalf("mmr_history 行数=%d, want 1", got)
	}
}

// TestApplyMMRChangeFloorClamp_MySQL —— 负 delta 越过下限时钳到 floor,
// 且**落进 mmr_history 的 new_mmr 必须是钳位后的值**:补发 / 对账都以历史行为准,
// 历史里写未钳位值会让后续回放把玩家推到负分。
func TestApplyMMRChangeFloorClamp_MySQL(t *testing.T) {
	db := newPlayerSchemaDB(t)
	repo := NewMySQLPlayerRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), mmrTestTimeout)
	defer cancel()

	const playerID = uint64(70_002)
	seedPlayerProfile(t, repo, playerID, 1500)

	newMMR, _, err := repo.ApplyMMRChange(ctx, MMRChange{
		PlayerID:       playerID,
		IdempotencyKey: "match-70002-crash",
		Delta:          -5000,
		Reason:         "lose",
		Floor:          0,
		IncBattle:      true,
	})
	if err != nil {
		t.Fatalf("负分结算: %v", err)
	}
	if newMMR != 0 {
		t.Fatalf("钳位后 mmr=%d, want 0", newMMR)
	}
	mmr, battles, wins := playerCounters(t, db, playerID)
	if mmr != 0 || battles != 1 || wins != 0 {
		t.Fatalf("落库 mmr=%d battles=%d wins=%d, want 0/1/0", mmr, battles, wins)
	}
	var recordedNew, recordedOld int
	if err := db.QueryRowContext(ctx,
		`SELECT old_mmr, new_mmr FROM mmr_history WHERE player_id = ? AND idempotency_key = ?`,
		playerID, "match-70002-crash").Scan(&recordedOld, &recordedNew); err != nil {
		t.Fatalf("读取 mmr_history: %v", err)
	}
	if recordedOld != 1500 || recordedNew != 0 {
		t.Fatalf("历史行 old=%d new=%d, want 1500/0(历史必须记钳位后的值)", recordedOld, recordedNew)
	}
}

// TestApplyMMRChangeConcurrentSameKey_MySQL —— 同一场对局被多个副本同时结算
// (battle_result 补扫与同步路径重叠是刻意允许的),只允许恰好一次真正入账。
func TestApplyMMRChangeConcurrentSameKey_MySQL(t *testing.T) {
	db := newPlayerSchemaDB(t)
	repo := NewMySQLPlayerRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), mmrTestTimeout)
	defer cancel()

	const (
		playerID   = uint64(70_003)
		concurrent = 8
	)
	seedPlayerProfile(t, repo, playerID, 1500)

	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		applied  int
		results  []int
		firstErr error
	)
	start := make(chan struct{})
	for i := 0; i < concurrent; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // 尽量把并发挤进同一时刻
			got, already, err := repo.ApplyMMRChange(ctx, MMRChange{
				PlayerID:       playerID,
				IdempotencyKey: "match-70003-shared",
				Delta:          30,
				Reason:         "win",
				Floor:          0,
				IncBattle:      true,
				IncWin:         true,
			})
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			if !already {
				applied++
			}
			results = append(results, got)
		}()
	}
	close(start)
	wg.Wait()

	if firstErr != nil {
		t.Fatalf("并发结算出现错误(同玩家行锁应把它们串行化,不应报错): %v", firstErr)
	}
	if applied != 1 {
		t.Fatalf("真正入账 %d 次, want 1(uk 幂等被并发穿透)", applied)
	}
	for _, got := range results {
		if got != 1530 {
			t.Fatalf("并发返回 mmr=%d, want 1530(所有副本必须看到同一权威值)", got)
		}
	}
	mmr, battles, wins := playerCounters(t, db, playerID)
	if mmr != 1530 || battles != 1 || wins != 1 {
		t.Fatalf("并发后 mmr=%d battles=%d wins=%d, want 1530/1/1", mmr, battles, wins)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM mmr_history WHERE player_id = ?`, playerID); got != 1 {
		t.Fatalf("mmr_history 行数=%d, want 1", got)
	}
}

// TestApplyMMRChangeUnknownPlayer_MySQL —— 未建档玩家必须拒绝而不是隐式建行:
// players 行是属性 / 天赋 / 战绩的共同载体,结算路径悄悄建档会绕开 login 的发号与默认档。
func TestApplyMMRChangeUnknownPlayer_MySQL(t *testing.T) {
	db := newPlayerSchemaDB(t)
	repo := NewMySQLPlayerRepo(db)
	ctx, cancel := context.WithTimeout(context.Background(), mmrTestTimeout)
	defer cancel()

	_, _, err := repo.ApplyMMRChange(ctx, MMRChange{
		PlayerID:       uint64(70_004),
		IdempotencyKey: "match-70004",
		Delta:          10,
		Reason:         "win",
	})
	if errcode.As(err) != errcode.ErrPlayerNotFound {
		t.Fatalf("未建档玩家结算 err=%v, want ErrPlayerNotFound", err)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM mmr_history`); got != 0 {
		t.Fatalf("拒绝路径写了 %d 行 mmr_history, want 0", got)
	}
	if got := countRows(t, db, `SELECT COUNT(*) FROM players`); got != 0 {
		t.Fatalf("拒绝路径隐式建了 %d 行 players, want 0", got)
	}
}
