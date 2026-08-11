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

// talentSpentExpr 是「该行实际花掉多少天赋点」的 SQL 口径,读取侧统一用它,
// 不再按 SUM(level) 反推(cost_per_level≠1 时反推算少,可点数会虚高)。
//
// spent_points 为 0 时回退到 level,是 §9.21 滚动升级共存窗口的桥:
// 老副本的 INSERT 不带 spent_points,新列取默认 0,新副本读到 0 会把这份分配当"没花点"。
// 回退到 level 等同当前线上口径(全部节点 cost_per_level=1),等所有副本换新后
// 所有行都会带上真实消耗,该分支自然不再命中。cost_per_level≥1 且 level≥1,
// 真实消耗恒 >0,所以 0 只可能来自老副本写入,不会误判正常行。
const talentSpentExpr = `IF(spent_points > 0, spent_points, level)`

// talentUnspent 读可点天赋点 = total_talent_points - SUM(已花点数)。
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
	const sumQ = `SELECT COALESCE(SUM(` + talentSpentExpr + `), 0) FROM player_talents WHERE player_id = ?`
	if err := q.QueryRowContext(ctx, sumQ, playerID).Scan(&used); err != nil {
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

// SetTalents 全量重置天赋(事务:锁 players 行,校验总消耗<=total,替换 player_talents)。
//
// 每条 TalentLevel.SpentPoints 是 biz 按专精表算好的该节点消耗(等级 × cost_per_level),
// 这里不按 sum(level) 推算:每级消耗是配置表列,repo 看不到配置,自行推算会在
// cost_per_level≠1 时算少扣。总消耗 = Σ SpentPoints,与落库的每行消耗同源,不会漂移。
func (r *MySQLPlayerRepo) SetTalents(ctx context.Context, playerID uint64, talents []TalentLevel) (int, error) {
	// biz 侧 ValidateAllocation 已把总消耗钳在 uint32 内,这里用 int64 累加只为不依赖上游钳位。
	var sum int64
	for _, t := range talents {
		if t.SpentPoints <= 0 {
			// 消耗必须由 biz 按表填;缺失说明调用方绕过了专精表校验,不能按"免费"落库。
			return 0, errcode.New(errcode.ErrInvalidArg,
				"talent %d missing spent points player=%d", t.TalentID, playerID)
		}
		sum += int64(t.SpentPoints)
	}

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
	// 用 int64 比较:总消耗可超 int32,32 位平台上 int(sum) 会截断成负数而"通过"校验。
	if sum > int64(total) {
		return 0, errcode.New(errcode.ErrPlayerInsufficientPoints, "insufficient talent points player=%d need=%d have=%d", playerID, sum, total)
	}

	if _, derr := tx.ExecContext(ctx, `DELETE FROM player_talents WHERE player_id = ?`, playerID); derr != nil {
		return 0, errcode.New(errcode.ErrInternal, "clear talents player=%d: %v", playerID, derr)
	}
	const ins = `INSERT INTO player_talents (player_id, talent_id, level, spent_points) VALUES (?, ?, ?, ?)`
	for _, t := range talents {
		if _, ierr := tx.ExecContext(ctx, ins, playerID, t.TalentID, t.Level, t.SpentPoints); ierr != nil {
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
	// 已花点数取 talentSpentExpr 而非裸 spent_points:与 talentUnspent 同一口径,
	// 共存窗口里老副本写的行(spent_points=0)回退按 level 计。
	const q = `SELECT talent_id, level, ` + talentSpentExpr + ` FROM player_talents WHERE player_id = ? ORDER BY talent_id`
	rows, err := r.db.QueryContext(ctx, q, playerID)
	if err != nil {
		return nil, 0, errcode.New(errcode.ErrInternal, "query talents player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()

	var talents []TalentLevel
	var used int64
	for rows.Next() {
		var t TalentLevel
		if serr := rows.Scan(&t.TalentID, &t.Level, &t.SpentPoints); serr != nil {
			return nil, 0, errcode.New(errcode.ErrInternal, "scan talent player=%d: %v", playerID, serr)
		}
		talents = append(talents, t)
		used += int64(t.SpentPoints)
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
