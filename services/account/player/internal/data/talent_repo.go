package data

import (
	"context"
	"database/sql"
	"errors"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// rowQueryer 抽象 *sql.DB / *sql.Tx 的 QueryRowContext,供 talentUnspent 复用。
type rowQueryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// talentUnspent 读可点天赋点 = total_talent_points - SUM(player_talents.level)。
// 玩家未建档 → ErrPlayerNotFound(调用方须先 EnsureProfile)。
func talentUnspent(ctx context.Context, q rowQueryer, playerID uint64) (int, error) {
	var total int
	if err := q.QueryRowContext(ctx, `SELECT total_talent_points FROM players WHERE player_id = ? LIMIT 1`, playerID).Scan(&total); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, errcode.New(errcode.ErrPlayerNotFound, "player not found: %d", playerID)
		}
		return 0, errcode.New(errcode.ErrInternal, "read total talent player=%d: %v", playerID, err)
	}
	var used int
	if err := q.QueryRowContext(ctx, `SELECT COALESCE(SUM(level), 0) FROM player_talents WHERE player_id = ?`, playerID).Scan(&used); err != nil {
		return 0, errcode.New(errcode.ErrInternal, "sum talent player=%d: %v", playerID, err)
	}
	return total - used, nil
}

// GrantTalentPoints 幂等授予天赋点(命中 uk → 读回当前可点,不重复授予)。
func (r *MySQLPlayerRepo) GrantTalentPoints(ctx context.Context, playerID uint64, points int32, idempotencyKey string) (int, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insGrant = `INSERT INTO talent_point_grants (player_id, idempotency_key, points) VALUES (?, ?, ?)`
	if _, gerr := tx.ExecContext(ctx, insGrant, playerID, idempotencyKey, points); gerr != nil {
		if isDupErr(gerr) {
			unspent, uerr := talentUnspent(ctx, tx, playerID)
			if uerr != nil {
				return 0, false, uerr
			}
			return unspent, true, nil
		}
		return 0, false, errcode.New(errcode.ErrInternal, "insert talent grant player=%d: %v", playerID, gerr)
	}

	res, uerr := tx.ExecContext(ctx, `UPDATE players SET total_talent_points = total_talent_points + ? WHERE player_id = ?`, points, playerID)
	if uerr != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "grant talent player=%d: %v", playerID, uerr)
	}
	if affected, _ := res.RowsAffected(); affected == 0 {
		return 0, false, errcode.New(errcode.ErrPlayerNotFound, "player not found: %d", playerID)
	}

	unspent, terr := talentUnspent(ctx, tx, playerID)
	if terr != nil {
		return 0, false, terr
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "commit talent grant player=%d: %v", playerID, cerr)
	}
	return unspent, false, nil
}

// SetTalents 全量重置天赋(事务:锁 players 行,校验 totalCost<=total,替换 player_talents)。
//
// totalCost 是 biz 按专精表算好的总消耗(Σ 等级 × cost_per_level),这里不再按 sum(level) 推算:
// 每级消耗是配置表列,repo 看不到配置,自行推算会在 cost_per_level≠1 时算少扣。
func (r *MySQLPlayerRepo) SetTalents(ctx context.Context, playerID uint64, talents []TalentLevel, totalCost uint32) (int, error) {
	sum := int64(totalCost)

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var total int
	err = tx.QueryRowContext(ctx, `SELECT total_talent_points FROM players WHERE player_id = ? FOR UPDATE`, playerID).Scan(&total)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, errcode.New(errcode.ErrPlayerNotFound, "player not found: %d", playerID)
	}
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "lock player=%d: %v", playerID, err)
	}
	// 用 int64 比较:totalCost 是 uint32,32 位平台上 int(sum) 会截断成负数而"通过"校验。
	if sum > int64(total) {
		return 0, errcode.New(errcode.ErrPlayerInsufficientPoints, "insufficient talent points player=%d need=%d have=%d", playerID, sum, total)
	}

	if _, derr := tx.ExecContext(ctx, `DELETE FROM player_talents WHERE player_id = ?`, playerID); derr != nil {
		return 0, errcode.New(errcode.ErrInternal, "clear talents player=%d: %v", playerID, derr)
	}
	const ins = `INSERT INTO player_talents (player_id, talent_id, level) VALUES (?, ?, ?)`
	for _, t := range talents {
		if _, ierr := tx.ExecContext(ctx, ins, playerID, t.TalentID, t.Level); ierr != nil {
			if isDupErr(ierr) {
				return 0, errcode.New(errcode.ErrInvalidArg, "duplicate talent_id player=%d talent=%d", playerID, t.TalentID)
			}
			return 0, errcode.New(errcode.ErrInternal, "insert talent player=%d talent=%d: %v", playerID, t.TalentID, ierr)
		}
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, errcode.New(errcode.ErrInternal, "commit talents player=%d: %v", playerID, cerr)
	}
	return total - int(sum), nil
}

// ResetTalents 清空天赋(事务:锁 players 行,删 player_talents,可点恢复为 total)。
func (r *MySQLPlayerRepo) ResetTalents(ctx context.Context, playerID uint64) (int, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var total int
	err = tx.QueryRowContext(ctx, `SELECT total_talent_points FROM players WHERE player_id = ? FOR UPDATE`, playerID).Scan(&total)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, errcode.New(errcode.ErrPlayerNotFound, "player not found: %d", playerID)
	}
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "lock player=%d: %v", playerID, err)
	}
	if _, derr := tx.ExecContext(ctx, `DELETE FROM player_talents WHERE player_id = ?`, playerID); derr != nil {
		return 0, errcode.New(errcode.ErrInternal, "clear talents player=%d: %v", playerID, derr)
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, errcode.New(errcode.ErrInternal, "commit reset talents player=%d: %v", playerID, cerr)
	}
	return total, nil
}

func (r *MySQLPlayerRepo) GetTalents(ctx context.Context, playerID uint64) ([]TalentLevel, int, error) {
	const q = `SELECT talent_id, level FROM player_talents WHERE player_id = ? ORDER BY talent_id`
	rows, err := r.db.QueryContext(ctx, q, playerID)
	if err != nil {
		return nil, 0, errcode.New(errcode.ErrInternal, "query talents player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()

	var talents []TalentLevel
	var used int32
	for rows.Next() {
		var t TalentLevel
		if serr := rows.Scan(&t.TalentID, &t.Level); serr != nil {
			return nil, 0, errcode.New(errcode.ErrInternal, "scan talent player=%d: %v", playerID, serr)
		}
		talents = append(talents, t)
		used += t.Level
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, 0, errcode.New(errcode.ErrInternal, "iterate talents player=%d: %v", playerID, rerr)
	}

	var total int
	terr := r.db.QueryRowContext(ctx, `SELECT total_talent_points FROM players WHERE player_id = ? LIMIT 1`, playerID).Scan(&total)
	if errors.Is(terr, sql.ErrNoRows) {
		return talents, 0, nil
	}
	if terr != nil {
		return nil, 0, errcode.New(errcode.ErrInternal, "read total talent player=%d: %v", playerID, terr)
	}
	return talents, total - int(used), nil
}
