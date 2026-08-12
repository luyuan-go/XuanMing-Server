package data

import (
	"context"
	"database/sql"
	"errors"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// GetMMR 读某玩家在某段位池下的分。
//
// found=false 表示该玩家在该池**没有任何记录**(没打过),不是"分是 0"——调用方必须
// 自己决定呈现方式(通常用基线分展示但标注未定级)。这里刻意不在读路径插占位行:
// 读操作产生写会让只读副本 / 只读事务失败,而且"查过一次就算定级了"是错的语义。
func (r *MySQLPlayerRepo) GetMMR(ctx context.Context, playerID uint64, ratingPool string) (int, bool, error) {
	const q = `SELECT mmr FROM player_mmr WHERE player_id = ? AND rating_pool = ? LIMIT 1`
	var mmr int
	err := r.db.QueryRowContext(ctx, q, playerID, ratingPool).Scan(&mmr)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "query mmr player=%d pool=%s: %v", playerID, ratingPool, err)
	}
	return mmr, true, nil
}

// ListRatings 列出某玩家**已有记录**的全部池段位分,按 rating_pool 字典序。
//
// 排序保证同一份档案多次查询顺序稳定(客户端可直接渲染,不必自己排)。
// 行数被"每玩家每池至多 1 行 + 池数由关卡表行数有界"兜住(§9.24 登记豁免),
// 故单次全量返回,不分页。
func (r *MySQLPlayerRepo) ListRatings(ctx context.Context, playerID uint64) ([]PlayerRating, error) {
	const q = `SELECT rating_pool, mmr FROM player_mmr WHERE player_id = ? ORDER BY rating_pool`
	rows, err := r.db.QueryContext(ctx, q, playerID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "list ratings player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()
	var out []PlayerRating
	for rows.Next() {
		var pr PlayerRating
		if serr := rows.Scan(&pr.RatingPool, &pr.MMR); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan rating player=%d: %v", playerID, serr)
		}
		out = append(out, pr)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate ratings player=%d: %v", playerID, rerr)
	}
	return out, nil
}

// ApplyMMRChange 在一个事务里幂等改**某个池**的 MMR(不变量 §2)。
//
// 流程:
//  1. SELECT players FOR UPDATE —— 锁玩家档案行,它是本玩家段位写入的**守卫行**
//  2. 读该池当前分(无行 = 该池首战,以 Baseline 起算)
//  3. INSERT mmr_history(命中 uk → 幂等:读回已记录 new_mmr 返回,不重复改任何分)
//  4. upsert player_mmr 到 clamp 后的新分
//  5. UPDATE players 的 total_battles / total_wins 计数
//
// ⚠️ 为什么锁 players 而不是锁 player_mmr:该池首战时 player_mmr **一行都没有**,而
// TiDB 没有 gap 锁 —— 零行上的 `FOR UPDATE` 一把锁都不加(与 friend/mission 域
// `*_player_guards` 同一个坑,那两处为此专门建了守卫表)。这里不必新建守卫表:
// players 行由 EnsureProfile 保证必然存在,而本事务本来就要 UPDATE 它的战绩计数,
// 锁它是自然且免费的。代价是同一玩家的跨池结算串行化 —— 一个玩家同时结算多局
// 本就极罕见,换来的是"两局并发都判自己是首战、各自 INSERT 覆盖"的彻底消除。
//
// 加锁顺序固定 players → player_mmr → mmr_history,全仓无反向路径,不成环。
func (r *MySQLPlayerRepo) ApplyMMRChange(ctx context.Context, change MMRChange) (int, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// ① 守卫行:存在性校验 + 该玩家段位写入的互斥点(见上方注释)。
	var exists uint64
	err = tx.QueryRowContext(ctx, `SELECT player_id FROM players WHERE player_id = ? FOR UPDATE`, change.PlayerID).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, errcode.New(errcode.ErrPlayerNotFound, "player not found: %d", change.PlayerID)
	}
	if err != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "lock player=%d: %v", change.PlayerID, err)
	}

	// ② 该池当前分。无行 = 该池首战,从基线起算(不预插占位行,见 GetMMR 注释)。
	//    已被守卫行锁保护,故这里无需再 FOR UPDATE。
	oldMMR := change.Baseline
	var stored int
	err = tx.QueryRowContext(ctx,
		`SELECT mmr FROM player_mmr WHERE player_id = ? AND rating_pool = ?`,
		change.PlayerID, change.RatingPool).Scan(&stored)
	switch {
	case err == nil:
		oldMMR = stored
	case errors.Is(err, sql.ErrNoRows):
		// 保持 baseline
	default:
		return 0, false, errcode.New(errcode.ErrInternal, "read pool mmr player=%d pool=%s: %v",
			change.PlayerID, change.RatingPool, err)
	}

	newMMR := oldMMR + int(change.Delta)
	if newMMR < change.Floor {
		newMMR = change.Floor
	}

	// ③ 幂等闸:uk (player_id, idempotency_key)。**刻意不把 rating_pool 混进幂等键** ——
	//    一场对局只属于一个池,若把池并入键,同一 match 换个池名就能重复入账(§16.2)。
	const insHist = `INSERT INTO mmr_history (player_id, idempotency_key, rating_pool, delta, reason, old_mmr, new_mmr)
VALUES (?, ?, ?, ?, ?, ?, ?)`
	if _, herr := tx.ExecContext(ctx, insHist,
		change.PlayerID, change.IdempotencyKey, change.RatingPool,
		change.Delta, change.Reason, oldMMR, newMMR); herr != nil {
		if isDupErr(herr) {
			// 幂等命中:读回该 idempotency_key 已记录的 new_mmr(不重复改任何池的分)
			var recordedNew int
			qerr := tx.QueryRowContext(ctx,
				`SELECT new_mmr FROM mmr_history WHERE player_id = ? AND idempotency_key = ? LIMIT 1`,
				change.PlayerID, change.IdempotencyKey).Scan(&recordedNew)
			if qerr != nil {
				return 0, false, errcode.New(errcode.ErrInternal, "read idem mmr player=%d key=%s: %v",
					change.PlayerID, change.IdempotencyKey, qerr)
			}
			return recordedNew, true, nil
		}
		return 0, false, errcode.New(errcode.ErrInternal, "insert mmr_history player=%d: %v", change.PlayerID, herr)
	}

	// ④ 分池落分。首战 INSERT,后续 UPDATE;两条路径同一条语句,不做"先查再决定"。
	const upsertMMR = `INSERT INTO player_mmr (player_id, rating_pool, mmr) VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE mmr = VALUES(mmr)`
	if _, merr := tx.ExecContext(ctx, upsertMMR, change.PlayerID, change.RatingPool, newMMR); merr != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "upsert player=%d pool=%s mmr: %v",
			change.PlayerID, change.RatingPool, merr)
	}

	// ⑤ 战绩计数是**跨池的账号级累计**(打了多少场 / 赢了多少场),不随段位分区。
	battleInc := 0
	if change.IncBattle {
		battleInc = 1
	}
	winInc := 0
	if change.IncWin {
		winInc = 1
	}
	const updPlayer = `UPDATE players SET total_battles = total_battles + ?, total_wins = total_wins + ?
WHERE player_id = ?`
	if _, uerr := tx.ExecContext(ctx, updPlayer, battleInc, winInc, change.PlayerID); uerr != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "update player=%d counters: %v", change.PlayerID, uerr)
	}

	if cerr := tx.Commit(); cerr != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "commit player=%d: %v", change.PlayerID, cerr)
	}
	return newMMR, false, nil
}
