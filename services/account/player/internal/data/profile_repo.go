package data

import (
	"context"
	"database/sql"
	"errors"

	"github.com/luyuancpp/pandora/pkg/errcode"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
)

// EnsureProfile 懒建档。INSERT IGNORE 语义:行已存在则**完全不动**(昵称不覆盖)。
//
// created 报告本次是否真的建了档。login 播种角色名(账号 / 角色分离 2026-08-18)靠它
// 判断「名字有没有真的落下去」——不能靠 err==nil 推断,建档与"早就存在"两种情况
// 都是 nil。
//
// ⚠️ 昵称冲突(uk_nickname 被别的玩家占了)在 INSERT IGNORE 下**不会报错**,而是
// 静默不插入 → created=false 且该玩家至此仍无档案。调用方必须按 created=false
// 复查,不能默认"没建就是已经有了"。
func (r *MySQLPlayerRepo) EnsureProfile(ctx context.Context, playerID uint64, defaultNickname string, baseMMR int) (bool, error) {
	// expand 期仍写 players.mmr：旧副本以它作为 default 池，fresh-init 与 000008
	// 都保留此兼容列。独立 contract 删除旧列后才可同步移除这里的 dual-schema 写法。
	const q = `INSERT IGNORE INTO players (player_id, nickname, level, mmr, avatar, total_battles, total_wins)
VALUES (?, ?, 1, ?, '', 0, 0)`
	res, err := r.db.ExecContext(ctx, q, playerID, defaultNickname, baseMMR)
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "ensure profile player=%d: %v", playerID, err)
	}
	n, aerr := res.RowsAffected()
	if aerr != nil {
		// 驱动不支持 RowsAffected:不猜。建档本身已成功,只是"是不是这次建的"不确定,
		// 报 false 让调用方走复查路径(比谎报 true 安全)。
		return false, nil
	}
	return n > 0, nil
}

// ListNicknames 批量反查角色显示名(Hub DS 铭牌用,2026-08-18)。
//
// 只返回**查到的**行:请求里有、结果里没有 = 该角色无档案。调用方据此区分
// 「查不到」与「名字是空串」—— 用零值占位会让 DS 把一个真名字覆盖成空。
//
// 刻意不在这里建档:本方法是纯只读旁路,给它加写副作用会让「看一眼名字」变成
// 能给任意 player_id 造档案的入口。
func (r *MySQLPlayerRepo) ListNicknames(ctx context.Context, playerIDs []uint64) (map[uint64]string, error) {
	if len(playerIDs) == 0 {
		return map[uint64]string{}, nil
	}
	// 手拼 IN 占位符:database/sql 不支持切片展开,且 playerIDs 是 uint64(非字符串,
	// 无注入面);值仍走参数绑定,不进 SQL 文本。
	placeholders := make([]byte, 0, len(playerIDs)*2)
	args := make([]any, 0, len(playerIDs))
	for i, id := range playerIDs {
		if i > 0 {
			placeholders = append(placeholders, ',')
		}
		placeholders = append(placeholders, '?')
		args = append(args, id)
	}
	q := `SELECT player_id, nickname FROM players WHERE player_id IN (` + string(placeholders) + `)`
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "list nicknames: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := make(map[uint64]string, len(playerIDs))
	for rows.Next() {
		var (
			id       uint64
			nickname string
		)
		if serr := rows.Scan(&id, &nickname); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan nickname: %v", serr)
		}
		out[id] = nickname
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate nicknames: %v", rerr)
	}
	return out, nil
}

// GetProfile 只读 players 表(账号级档案)。**分池段位不在这里** —— 它按 rating_pool
// 存 player_mmr；players.mmr 仅是滚动升级 default 投影。biz 层另调 ListRatings
// 组装进 PlayerProfile.ratings。
// 刻意不在本查询里 JOIN:段位是一对多,JOIN 会让单行扫描变成需要去重的多行结果,
// 而档案本身(昵称/等级/战绩)与打了几个池无关。
func (r *MySQLPlayerRepo) GetProfile(ctx context.Context, playerID uint64) (*playerv1.PlayerProfile, bool, error) {
	const q = `SELECT nickname, level, mmr, avatar,
UNIX_TIMESTAMP(created_at)*1000, UNIX_TIMESTAMP(last_seen_at)*1000, total_battles, total_wins, exp
FROM players WHERE player_id = ? LIMIT 1`
	p := &playerv1.PlayerProfile{PlayerId: playerID}
	err := r.db.QueryRowContext(ctx, q, playerID).Scan(
		&p.Nickname, &p.Level, &p.Mmr, &p.Avatar,
		&p.CreatedAtMs, &p.LastSeenMs, &p.TotalBattles, &p.TotalWins, &p.ExpInLevel)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "query profile player=%d: %v", playerID, err)
	}
	return p, true, nil
}

func (r *MySQLPlayerRepo) UpdateNickname(ctx context.Context, playerID uint64, nickname string) error {
	const q = `UPDATE players SET nickname = ? WHERE player_id = ?`
	res, err := r.db.ExecContext(ctx, q, nickname, playerID)
	if err != nil {
		if isDupErr(err) {
			return errcode.New(errcode.ErrPlayerNicknameTaken, "nickname taken: %s", nickname)
		}
		return errcode.New(errcode.ErrInternal, "update nickname player=%d: %v", playerID, err)
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		// 0 行受影响有两种可能:玩家不存在,或昵称未变。确认玩家是否存在以区分。
		var exists int
		qerr := r.db.QueryRowContext(ctx, `SELECT 1 FROM players WHERE player_id = ? LIMIT 1`, playerID).Scan(&exists)
		if errors.Is(qerr, sql.ErrNoRows) {
			return errcode.New(errcode.ErrPlayerNotFound, "player not found: %d", playerID)
		}
		if qerr != nil {
			return errcode.New(errcode.ErrInternal, "check player exists %d: %v", playerID, qerr)
		}
		// 玩家存在但昵称未变 → 幂等成功
	}
	return nil
}
