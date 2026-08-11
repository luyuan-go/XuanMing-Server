// mission_outbox_repo.go — 任务事实转发出箱仓储(docs/design/mission.md §5.1)。
//
// 与 battle_progress_outbox 分表的理由见迁移 000010 文件头:**故障域隔离**——任务行混入
// 进度出箱会让 mission 不可用卡住队首、连带阻塞该玩家的经验 / 掉落投递。两张表各自
// 都是每玩家严格 FIFO,分表只是把"谁堵谁"切开,不是"任务事实可以乱序"。
//
// 行的写入在 ApplyProgress 的**同一个事务**里(水位 CAS + 进度出箱 + 任务出箱):
// 分开写会在两次提交之间留下丢事实(seq 已推进,DS 不会重发)或重复计入的窗口。
package data

import (
	"context"
	"database/sql"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// MissionFactRecord 是一条待转发的任务事实(一事件一行)。
//
// Category / SlotValue 对应 pandora.mission.v1 的 MissionConditionCategory 与该类别的
// 槽位1语义(杀怪=怪物配置 ID,拾取 / 使用道具=道具配置 ID);Amount 是进度增量。
// 幂等键 = progress:{MatchID}:{Seq}:{PlayerID}:mission,由 mission 侧收据表吸收重放。
type MissionFactRecord struct {
	ID        int64
	MatchID   uint64
	Seq       uint64
	PlayerID  uint64
	Category  uint32
	SlotValue uint32
	Amount    uint32
	// PendingAction 为 true 时行落库即不可投递,等局内消费扣除落定
	// (ResolveProgressAction 置 0 或删行)。只有 USE_ITEM 类事实用得上:
	// 扣除可能以业务失败终态收场(道具不足),照发就等于让"上报根本没发生的消耗"
	// 刷「使用 N 个 X」型任务(§9.6 派生数值不信 DS)。
	PendingAction bool
}

// insertMissionFactsTx 在 ApplyProgress 事务内写任务出箱行。
//
// 撞 uk(match_id, seq, player_id) 说明同一事件被重复展开(理论不可达:seq 去重在前),
// 按幂等忽略而不是让整批失败 —— 重复插入不该把一批合法进度打回。
func insertMissionFactsTx(ctx context.Context, tx *sql.Tx, rows []MissionFactRecord, nowMs int64) error {
	if len(rows) == 0 {
		return nil
	}
	const ins = `INSERT IGNORE INTO battle_mission_outbox
(match_id, seq, player_id, category, slot_value, amount, pending_action, created_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	for _, row := range rows {
		pending := 0
		if row.PendingAction {
			pending = 1
		}
		if _, err := tx.ExecContext(ctx, ins,
			row.MatchID, row.Seq, row.PlayerID, row.Category, row.SlotValue, row.Amount, pending, nowMs,
		); err != nil {
			return errcode.New(errcode.ErrUnavailable, "insert mission outbox match=%d player=%d seq=%d: %v",
				row.MatchID, row.PlayerID, row.Seq, err)
		}
	}
	return nil
}

// FetchMissionOutbox 取一批到期的任务事实出箱行,**同一 player_id 只返回 id 最小的一条**
// —— 与进度出箱同款 NOT EXISTS 谓词,每玩家严格 FIFO。
//
// 为什么必须 FIFO(推翻早期"任务事实顺序无关"的说法):任务链前后两环的条件类别通常
// 不同(「杀 5 只狼」→「收集 3 张狼皮」),后环任务在前环完成时才被自动接取。若"狼皮"
// 事实先于"杀狼"事实投递,mission 侧匹配不上任何活跃任务,进度**静默丢失**且事实已被
// 收据吸收、永不重放。乱序的来源就是 DeferMissionOutbox:失败行退避后,同玩家后续行
// 会越过它先投。改为队首阻塞后,退避的行会把同玩家后续事实一起挡住,顺序不再被破坏。
//
// **按 player_id 而不是 (match_id, player_id) 分组**:任务链是玩家维度的,不是对局维度的。
// 按对局分组时,A 局卡住的队首挡不住 B 局的行,玩家打完 A 再打 B,B 的事实照样能抢在
// A 的前面落地 —— 同一个丢进度的洞,只是要多打一局才踩到。同理排序键用 **id 而不是 seq**:
// seq 是每对局自增的,跨对局不可比;id 是插入序,而 ApplyProgress 一个事务一批、批内按 seq
// 升序插入,所以 id 序在对局内等价于 seq 序,跨对局等价于对局发生序(§9.1 保证玩家同一
// 时刻只在一个可操作 DS,不存在两局并行产事实)。
//
// pending_action=1 的行同样占队首但不投递(等局内消费扣除落定),天然实现"扣除未落定
// 就不推进使用类任务、也不让后续事实抢跑"。
func (r *MySQLBattleRepo) FetchMissionOutbox(ctx context.Context, limit int) ([]MissionFactRecord, error) {
	if limit <= 0 {
		limit = 128
	}
	const q = `SELECT cur.id, cur.match_id, cur.seq, cur.player_id, cur.category, cur.slot_value, cur.amount
FROM battle_mission_outbox cur
WHERE cur.next_attempt_at_ms <= ?
  AND cur.pending_action = 0
  AND NOT EXISTS (
    SELECT 1 FROM battle_mission_outbox prev
    WHERE prev.player_id = cur.player_id AND prev.id < cur.id
  )
ORDER BY cur.id ASC LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, time.Now().UnixMilli(), limit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query mission outbox: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]MissionFactRecord, 0, limit)
	for rows.Next() {
		var rec MissionFactRecord
		if serr := rows.Scan(&rec.ID, &rec.MatchID, &rec.Seq, &rec.PlayerID,
			&rec.Category, &rec.SlotValue, &rec.Amount); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan mission outbox: %v", serr)
		}
		out = append(out, rec)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate mission outbox: %v", rerr)
	}
	return out, nil
}

// settleMissionFactPendingTx 在局内消费**结果落定的同一事务**里解开 USE_ITEM 事实的
// pending 闸:granted=true 置 0(允许投递),false 直接删行(扣除没发生 = 事实不存在)。
//
// 必须同事务:分两次提交会留下"扣除已失败但任务事实已可投递"的窗口,足够让补扫把
// 一条根本没发生的消耗推进任务进度。命中 0 行是正常的(丢弃类事实、mission_addr 未配
// 时压根没写行、或重复 resolve),不构成错误。
func settleMissionFactPendingTx(ctx context.Context, tx *sql.Tx, matchID, seq, playerID uint64, granted bool) error {
	const (
		release = `UPDATE battle_mission_outbox SET pending_action = 0
WHERE match_id = ? AND seq = ? AND player_id = ? AND pending_action = 1`
		drop = `DELETE FROM battle_mission_outbox
WHERE match_id = ? AND seq = ? AND player_id = ? AND pending_action = 1`
	)
	stmt := drop
	if granted {
		stmt = release
	}
	if _, err := tx.ExecContext(ctx, stmt, matchID, seq, playerID); err != nil {
		return errcode.New(errcode.ErrInternal,
			"settle mission fact pending match=%d seq=%d player=%d granted=%v: %v",
			matchID, seq, playerID, granted, err)
	}
	return nil
}

// DeleteMissionOutbox 投递成功后删行。
func (r *MySQLBattleRepo) DeleteMissionOutbox(ctx context.Context, id int64) error {
	if _, err := r.db.ExecContext(ctx, `DELETE FROM battle_mission_outbox WHERE id = ?`, id); err != nil {
		return errcode.New(errcode.ErrInternal, "delete mission outbox id=%d: %v", id, err)
	}
	return nil
}

// ValidateMissionOutboxSchema 启动时探测任务出箱表(000010 迁移产物)。
//
// 只在**任务转发已启用**(mission_addr 已配)时由 main 调用:未启用时不写该表,
// 缺表不该拖垮启动;已启用而缺表则每次 ReportProgress 都会在事务里炸,必须 fail-fast。
func (r *MySQLBattleRepo) ValidateMissionOutboxSchema(ctx context.Context) error {
	const probe = `SELECT id, match_id, seq, player_id, category, slot_value, amount,
pending_action, next_attempt_at_ms, attempt_count, created_at_ms FROM battle_mission_outbox LIMIT 0`
	rows, err := r.db.QueryContext(ctx, probe)
	if err != nil {
		return errcode.New(errcode.ErrInternal,
			"battle_mission_outbox schema 探测失败(缺 tools/migrate/migrations/pandora_battle/000010_battle_mission_outbox.up.sql?): %v", err)
	}
	return rows.Close()
}

// DeferMissionOutbox 投递失败后指数退避(2s·2^n 封顶 5min;行永不丢弃,与进度出箱同纪律)。
func (r *MySQLBattleRepo) DeferMissionOutbox(ctx context.Context, id int64) error {
	if _, err := r.db.ExecContext(ctx,
		`UPDATE battle_mission_outbox
SET attempt_count = attempt_count + 1,
    next_attempt_at_ms = ? + LEAST(2000 * POW(2, LEAST(attempt_count, 7)), 300000)
WHERE id = ?`, time.Now().UnixMilli(), id); err != nil {
		return errcode.New(errcode.ErrInternal, "defer mission outbox id=%d: %v", id, err)
	}
	return nil
}
