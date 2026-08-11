// progress_repo.go — 战斗中实时进度水位去重 + 进度发放事务出箱(实时成长,2026-07-20)。
//
// 库表(deploy/mysql-init/05-battle-outbox.sql / tools/migrate pandora_battle 000005+000006):
//
//	pandora_battle.battle_progress_stream 每场进度水位(PK match_id;last_applied_seq 单调推进,
//	                                      settled_at_ms>0 = 对局已结算,后续进度一律拒 = 僵尸 DS fencing)
//	pandora_battle.battle_progress_outbox 进度事实事务出箱(uk match+seq+player+kind;
//	                                      exp / stack+instance grant / stack consume+discard)
//
// 幂等 / 原子:水位推进(乐观 CAS:WHERE last_applied_seq=expected AND settled_at_ms=0)、
// 单场/单玩家累计上限判定与出箱行写入同一 MySQL 事务;CAS 失败(并发写者 / 已结算)或
// 超限整批回滚,DS 按错误语义重试、丢批或停流。
package data

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// ProgressGrantKind 是进度出箱行的发放类型。
type ProgressGrantKind uint8

const (
	// ProgressGrantExp 经验入账(player.AddExperience)。
	ProgressGrantExp ProgressGrantKind = 1
	// ProgressGrantInstance 装备掉落发放(inventory.GrantInstances)。值 2 保持旧出箱兼容。
	ProgressGrantInstance ProgressGrantKind = 2
	// ProgressGrantItem 是历史名称兼容别名。
	ProgressGrantItem = ProgressGrantInstance
	// ProgressGrantStack 可堆叠掉落发放(inventory.GrantItems)。
	ProgressGrantStack ProgressGrantKind = 3
	// ProgressConsumeStack 局内消费持久扣减(inventory.ConsumeBattleItem)。
	ProgressConsumeStack ProgressGrantKind = 4
	// ProgressDiscardStack 副本丢弃持久扣减(inventory.DiscardBattleItem)。
	ProgressDiscardStack ProgressGrantKind = 5
)

// ProgressActionStatus 是 consume/discard 的 durable completion outcome。
// Pending 只表示事实和 outbox 已接受；只有 Succeeded 才允许 ReportProgress 返回 OK。
type ProgressActionStatus uint8

const (
	ProgressActionPending ProgressActionStatus = iota
	ProgressActionSucceeded
	ProgressActionFailed
)

// ProgressAction 固化一个独立 action 事实及其最终结果。Item/Count 同时充当请求指纹，
// 同 seq 改 payload 必须拒绝；ResultCode 只在 Failed 时非 OK。
type ProgressAction struct {
	MatchID      uint64
	Seq          uint64
	PlayerID     uint64
	Kind         ProgressGrantKind
	ItemConfigID uint32
	Count        uint32
	Status       ProgressActionStatus
	ResultCode   errcode.Code
}

// ProgressOutboxRecord 是一条待发放的进度出箱记录。
// 幂等键 = progress:{match_id}:{seq}:{player_id}:{kind 名}。
// Seq:exp 行 = 批末事件 seq(批内按玩家聚合);item 行 = 该拾取事实自身的 seq
// (每事实一行,天然不超 CSV 列宽,合法掉落永不截断 —— 审计 P1)。
type ProgressOutboxRecord struct {
	ID            int64
	MatchID       uint64
	Seq           uint64
	PlayerID      uint64
	Kind          ProgressGrantKind
	ExpDelta      uint64   // Kind=Exp 时有效
	ItemConfigIDs []uint32 // 掉落/消费时有效(CSV 存储；同一事实内为同配置 ID)
	ItemCount     uint32   // consume/discard 紧凑计数；0 表示兼容旧重复 CSV
}

// ProgressWatermark 是一场对局的进度水位快照(单场模式 / 去重 / 累计上限的权威依据)。
type ProgressWatermark struct {
	// LastAppliedSeq 已应用批末 seq(0 = 尚未入账任何批)。
	LastAppliedSeq uint64
	// TotalExp 本场已累计入账经验(事实换算后,累计上限依据,审计 P1)。
	TotalExp uint64
	// TotalItems 本场已累计入账掉落件数。
	TotalItems uint32
	// Settled 对局已结算(终局标记,迟到进度一律拒)。
	Settled bool
	// Stopped 实时通道已停流(未知事实/违纪混版持久标记,后续进度一律拒;
	// 审计 P1:无持久标记时,违纪 DS 停流后再发只含已知事实的批会被重新接受)。
	Stopped bool
	// Existed 水位行是否存在(= 本场已走实时通道;killswitch 中途关闭不影响已开流对局)。
	Existed bool
}

// ProgressPlayerTotals 是本场单个玩家的累计入账快照(单玩家上限封顶依据,审计 P1:
// 只按场累计时失陷 DS 可把全场额度灌给一人)。
type ProgressPlayerTotals struct {
	TotalExp   uint64
	TotalItems uint32
	TotalKills uint32
}

// ProgressPlayerDelta 是本批某玩家的新增累计(与水位 CAS 同事务 upsert 到 battle_progress_player)。
type ProgressPlayerDelta struct {
	PlayerID uint64
	Exp      uint64
	Items    uint32
	Kills    uint32
}

// ProgressCaps 单场 / 单场单玩家累计上限(biz 从配置注入;各项必须 >0,由
// conf *OrDefault 取值保证)。判定在 ApplyProgress 事务内的一致快照上进行:
// 事务外"先读累计再判上限"与水位 CAS 分属不同快照,重试请求可能读到旧水位 +
// 新累计,把同批 delta 重复计入后返回永久 ErrInvalidArg(审计 P1:DS 据契约
// 丢批并释放拾取认领,而首请求出箱已提交 → 重新拾取可重复发放)。
type ProgressCaps struct {
	MatchExp    uint64
	MatchItems  uint32
	PlayerExp   uint64
	PlayerItems uint32
	PlayerKills uint32
}

// GetProgressWatermark 读一场对局的进度水位。行不存在 → 零值(Existed=false)。
func (r *MySQLBattleRepo) GetProgressWatermark(ctx context.Context, matchID uint64) (ProgressWatermark, error) {
	var (
		wm        ProgressWatermark
		settledMs int64
		stoppedMs int64
	)
	err := r.db.QueryRowContext(ctx,
		`SELECT last_applied_seq, total_exp, total_items, settled_at_ms, stopped_at_ms FROM battle_progress_stream WHERE match_id = ? LIMIT 1`,
		matchID,
	).Scan(&wm.LastAppliedSeq, &wm.TotalExp, &wm.TotalItems, &settledMs, &stoppedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return ProgressWatermark{}, nil
	}
	if err != nil {
		return ProgressWatermark{}, errcode.New(errcode.ErrUnavailable, "query progress watermark match=%d: %v", matchID, err)
	}
	wm.Settled = settledMs > 0
	wm.Stopped = stoppedMs > 0
	wm.Existed = true
	return wm, nil
}

// ClaimProgressLegacy 实现 BattleRepo.ClaimProgressLegacy:行不存在才创建停流标记
// (固化"本场 legacy 结算模式");行已存在时零修改(INSERT IGNORE 撞 PK 即输掉认领,
// 审计 R4 #11:不得用 upsert 把开启副本并发刚开的流停掉)。
func (r *MySQLBattleRepo) ClaimProgressLegacy(ctx context.Context, matchID uint64) (bool, error) {
	nowMs := time.Now().UnixMilli()
	res, err := r.db.ExecContext(ctx, `
INSERT IGNORE INTO battle_progress_stream (match_id, last_applied_seq, total_exp, total_items, final_seq, settled_at_ms, stopped_at_ms, updated_at_ms)
VALUES (?, 0, 0, 0, 0, 0, ?, ?)`,
		matchID, nowMs, nowMs)
	if err != nil {
		return false, errcode.New(errcode.ErrUnavailable, "claim progress legacy match=%d: %v", matchID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, errcode.New(errcode.ErrUnavailable, "claim progress legacy rows match=%d: %v", matchID, err)
	}
	return n == 1, nil
}

// MarkProgressStopped 持久化停流标记(幂等:只记录首次停流时间)。行不存在时创建
// (首批就含未知事实的场景也必须留标记,否则后续已知批会开流)。
// ⚠️ upsert 语义,仅供"流内确定停流"(未知事实)使用;通道关闭固化走 ClaimProgressLegacy。
func (r *MySQLBattleRepo) MarkProgressStopped(ctx context.Context, matchID uint64) error {
	nowMs := time.Now().UnixMilli()
	if _, err := r.db.ExecContext(ctx, `
INSERT INTO battle_progress_stream (match_id, last_applied_seq, total_exp, total_items, final_seq, settled_at_ms, stopped_at_ms, updated_at_ms)
VALUES (?, 0, 0, 0, 0, 0, ?, ?)
ON DUPLICATE KEY UPDATE stopped_at_ms = IF(stopped_at_ms = 0, VALUES(stopped_at_ms), stopped_at_ms),
updated_at_ms = VALUES(updated_at_ms)`,
		matchID, nowMs, nowMs); err != nil {
		return errcode.New(errcode.ErrUnavailable, "mark progress stopped match=%d: %v", matchID, err)
	}
	return nil
}

// ApplyProgress 原子推进水位、判定累计上限并写进度出箱(同一事务)。
//
//	expectedSeq 是调用方读到的水位(乐观 CAS 期望值);newSeq 是本批批末 seq(> expectedSeq)。
//	addExp / addItems 是本批新入账的经验 / 掉落件数,与水位同一 CAS 行累计。
//	caps 是单场 / 单玩家累计上限:入账后在**本事务一致快照**上判定(水位行已被本事务
//	写锁定,并发批次被 CAS 串行化),超限返回 ErrInvalidArg 并整体回滚(零副作用)。
//	上限判定不得放在事务外(§16.1 TOCTOU;混合快照误判会把可重试竞争放大成永久拒,审计 P1)。
//	CAS 失败(并发写者抢先 / 对局已结算)→ ErrUnavailable(瞬时,DS 单飞行批下几乎不会发生,
//	重试后按新水位去重收敛);行不存在时首批 INSERT,撞 PK 同样按 ErrUnavailable 重试收敛。
func (r *MySQLBattleRepo) ApplyProgress(ctx context.Context, matchID, expectedSeq, newSeq uint64, addExp uint64, addItems uint32, playerDeltas []ProgressPlayerDelta, rows []ProgressOutboxRecord, missionRows []MissionFactRecord, caps ProgressCaps) error {
	if matchID == 0 || newSeq <= expectedSeq {
		return errcode.New(errcode.ErrInvalidArg, "apply progress requires match/seq advance")
	}
	actionRows := 0
	for _, row := range rows {
		if row.Kind == ProgressConsumeStack || row.Kind == ProgressDiscardStack {
			actionRows++
		}
	}
	if actionRows > 0 && (actionRows != 1 || len(rows) != 1 || addExp != 0 || addItems != 0 || len(playerDeltas) != 0) {
		return errcode.New(errcode.ErrInvalidArg,
			"consume/discard progress action must be one isolated outbox row")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errcode.New(errcode.ErrUnavailable, "begin progress tx match=%d: %v", matchID, err)
	}
	defer func() { _ = tx.Rollback() }()

	nowMs := time.Now().UnixMilli()
	if expectedSeq == 0 {
		// 首批:INSERT 创建水位行(已存在 → 并发写者已推进 → 重试收敛)。
		// 已结算 / 已停流对局的行永远存在(SaveResult 落终局标记 / MarkProgressStopped
		// 落停流标记),INSERT 撞 PK 即被拒 → 调用方重读水位看到 Settled/Stopped,
		// 天然 fail-closed;非首批 UPDATE 由 settled_at_ms=0 AND stopped_at_ms=0 条件
		// fencing(审计 P1:停流与正常批的 CAS 竞态——正常批读到停流前旧快照后,
		// 不得再推进水位写出箱)。
		if _, ierr := tx.ExecContext(ctx,
			`INSERT INTO battle_progress_stream (match_id, last_applied_seq, total_exp, total_items, final_seq, settled_at_ms, updated_at_ms)
VALUES (?, ?, ?, ?, 0, 0, ?)`, matchID, newSeq, addExp, addItems, nowMs); ierr != nil {
			if isDupErr(ierr) {
				return errcode.New(errcode.ErrUnavailable, "progress watermark contended match=%d", matchID)
			}
			return errcode.New(errcode.ErrUnavailable, "insert progress watermark match=%d: %v", matchID, ierr)
		}
	} else {
		res, uerr := tx.ExecContext(ctx,
			`UPDATE battle_progress_stream SET last_applied_seq = ?, total_exp = total_exp + ?, total_items = total_items + ?, updated_at_ms = ?
WHERE match_id = ? AND last_applied_seq = ? AND settled_at_ms = 0 AND stopped_at_ms = 0`,
			newSeq, addExp, addItems, nowMs, matchID, expectedSeq)
		if uerr != nil {
			return errcode.New(errcode.ErrUnavailable, "advance progress watermark match=%d: %v", matchID, uerr)
		}
		if affected, _ := res.RowsAffected(); affected == 0 {
			// 期望水位不匹配或已结算:让调用方重读水位。已结算场景重读后会拿到明确的
			// ErrInvalidState(biz 判 settled),不会无限重试。
			return errcode.New(errcode.ErrUnavailable, "progress watermark moved or settled match=%d", matchID)
		}
	}

	// 单场累计上限判定:水位行已被本事务写锁定,读回的是入账后的权威累计(一致快照)。
	// 超限 → ErrInvalidArg + 整体回滚(零副作用);永久拒只可能来自真实超限的新批,
	// 重放批在水位 CAS 处就以 ErrUnavailable 收敛,不会走到这里。
	var (
		curExp   uint64
		curItems uint32
	)
	if qerr := tx.QueryRowContext(ctx,
		`SELECT total_exp, total_items FROM battle_progress_stream WHERE match_id = ?`,
		matchID).Scan(&curExp, &curItems); qerr != nil {
		return errcode.New(errcode.ErrUnavailable, "read progress totals match=%d: %v", matchID, qerr)
	}
	if curExp > caps.MatchExp {
		return errcode.New(errcode.ErrInvalidArg,
			"match %d cumulative exp %d exceeds per-match cap %d (batch +%d)", matchID, curExp, caps.MatchExp, addExp)
	}
	if curItems > caps.MatchItems {
		return errcode.New(errcode.ErrInvalidArg,
			"match %d cumulative items %d exceeds per-match cap %d (batch +%d)", matchID, curItems, caps.MatchItems, addItems)
	}

	// 单玩家累计与水位同事务推进(CAS 保护下 upsert 累加无竞态,单玩家上限判定依据)。
	const upsertPlayer = `INSERT INTO battle_progress_player
(match_id, player_id, total_exp, total_items, total_kills, updated_at_ms)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE total_exp = total_exp + VALUES(total_exp),
total_items = total_items + VALUES(total_items),
total_kills = total_kills + VALUES(total_kills),
updated_at_ms = VALUES(updated_at_ms)`
	for _, d := range playerDeltas {
		if _, perr := tx.ExecContext(ctx, upsertPlayer,
			matchID, d.PlayerID, d.Exp, d.Items, d.Kills, nowMs); perr != nil {
			return errcode.New(errcode.ErrUnavailable, "upsert progress player totals match=%d player=%d: %v",
				matchID, d.PlayerID, perr)
		}
	}

	// 单玩家累计上限判定(同一事务一致快照,理由同上)。
	if len(playerDeltas) > 0 {
		query := `SELECT player_id, total_exp, total_items, total_kills FROM battle_progress_player
WHERE match_id = ? AND player_id IN (?` + strings.Repeat(",?", len(playerDeltas)-1) + `)`
		args := make([]any, 0, len(playerDeltas)+1)
		args = append(args, matchID)
		for _, d := range playerDeltas {
			args = append(args, d.PlayerID)
		}
		prows, perr := tx.QueryContext(ctx, query, args...)
		if perr != nil {
			return errcode.New(errcode.ErrUnavailable, "query progress player totals match=%d: %v", matchID, perr)
		}
		var capErr error
		for prows.Next() {
			var (
				pid uint64
				t   ProgressPlayerTotals
			)
			if serr := prows.Scan(&pid, &t.TotalExp, &t.TotalItems, &t.TotalKills); serr != nil {
				_ = prows.Close()
				return errcode.New(errcode.ErrUnavailable, "scan progress player totals match=%d: %v", matchID, serr)
			}
			switch {
			case t.TotalExp > caps.PlayerExp:
				capErr = errcode.New(errcode.ErrInvalidArg,
					"match %d player %d cumulative exp %d exceeds per-player cap %d", matchID, pid, t.TotalExp, caps.PlayerExp)
			case t.TotalItems > caps.PlayerItems:
				capErr = errcode.New(errcode.ErrInvalidArg,
					"match %d player %d cumulative items %d exceeds per-player cap %d", matchID, pid, t.TotalItems, caps.PlayerItems)
			case t.TotalKills > caps.PlayerKills:
				capErr = errcode.New(errcode.ErrInvalidArg,
					"match %d player %d cumulative kills %d exceeds per-player cap %d", matchID, pid, t.TotalKills, caps.PlayerKills)
			}
			if capErr != nil {
				break
			}
		}
		if cerr := prows.Close(); cerr != nil && capErr == nil {
			return errcode.New(errcode.ErrUnavailable, "close progress player totals match=%d: %v", matchID, cerr)
		}
		if capErr != nil {
			return capErr
		}
		if rerr := prows.Err(); rerr != nil {
			return errcode.New(errcode.ErrUnavailable, "iterate progress player totals match=%d: %v", matchID, rerr)
		}
	}

	const insRow = `INSERT INTO battle_progress_outbox
(match_id, seq, player_id, kind, exp_delta, item_config_ids, item_count, created_at_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	for _, row := range rows {
		if aerr := applyProgressItemAuthorityTx(ctx, tx, matchID, row, nowMs); aerr != nil {
			return aerr
		}
		if _, rerr := tx.ExecContext(ctx, insRow,
			matchID, row.Seq, row.PlayerID, uint8(row.Kind),
			row.ExpDelta, encodeConfigIDs(row.ItemConfigIDs), row.ItemCount, nowMs,
		); rerr != nil {
			return errcode.New(errcode.ErrUnavailable, "insert progress outbox match=%d player=%d: %v",
				matchID, row.PlayerID, rerr)
		}
	}

	// 任务事实出箱与水位 CAS 同事务(docs/design/mission.md §5.1):分开写会在两次提交
	// 之间留下"seq 已推进但事实未落箱"的窗口 —— DS 不会重发,事实永久丢失。
	if merr := insertMissionFactsTx(ctx, tx, missionRows, nowMs); merr != nil {
		return merr
	}

	if cerr := tx.Commit(); cerr != nil {
		return errcode.New(errcode.ErrUnavailable, "commit progress match=%d: %v", matchID, cerr)
	}
	return nil
}

// applyProgressItemAuthorityTx 把 phase0 stack pickup 额度与 action 支出预留收进
// ApplyProgress 的同一个水位事务。只有本场、同玩家、同 item 已接受的 stack pickup
// 能提供额度；进入本场前的主库存永远不会增加这里的 picked_count。
func applyProgressItemAuthorityTx(ctx context.Context, tx *sql.Tx, matchID uint64, row ProgressOutboxRecord, nowMs int64) error {
	switch row.Kind {
	case ProgressGrantStack:
		counts := make(map[uint32]uint32)
		for _, itemID := range row.ItemConfigIDs {
			if itemID == 0 {
				return errcode.New(errcode.ErrInvalidArg, "stack pickup contains zero item_config_id")
			}
			counts[itemID]++
		}
		if len(counts) == 0 {
			return errcode.New(errcode.ErrInvalidArg, "stack pickup row is empty")
		}
		const upsertBalance = `INSERT INTO battle_progress_item_balance
(match_id,player_id,item_config_id,picked_count,spent_count,updated_at_ms)
VALUES(?,?,?,?,0,?)
ON DUPLICATE KEY UPDATE picked_count=picked_count+VALUES(picked_count),updated_at_ms=VALUES(updated_at_ms)`
		for itemID, count := range counts {
			if _, err := tx.ExecContext(ctx, upsertBalance,
				matchID, row.PlayerID, itemID, count, nowMs); err != nil {
				return errcode.New(errcode.ErrUnavailable,
					"credit battle pickup balance match=%d player=%d item=%d: %v",
					matchID, row.PlayerID, itemID, err)
			}
		}
		return nil
	case ProgressConsumeStack, ProgressDiscardStack:
		itemID, count, err := progressSingleStackFact(row)
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(ctx, `UPDATE battle_progress_item_balance
SET spent_count=spent_count+?,updated_at_ms=?
WHERE match_id=? AND player_id=? AND item_config_id=?
  AND spent_count<=picked_count AND picked_count-spent_count>=?`,
			count, nowMs, matchID, row.PlayerID, itemID, count)
		if err != nil {
			return errcode.New(errcode.ErrUnavailable,
				"reserve battle item spend match=%d player=%d item=%d: %v",
				matchID, row.PlayerID, itemID, err)
		}
		affected, err := res.RowsAffected()
		if err != nil {
			return errcode.New(errcode.ErrUnavailable,
				"read battle item spend result match=%d player=%d item=%d: %v",
				matchID, row.PlayerID, itemID, err)
		}
		if affected != 1 {
			return errcode.New(errcode.ErrInvalidArg,
				"battle item action exceeds same-match accepted pickup balance match=%d player=%d item=%d count=%d; phase0 actions may only spend this match's stack pickups",
				matchID, row.PlayerID, itemID, count)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO battle_progress_action
(match_id,seq,player_id,kind,item_config_id,count,status,result_code,created_at_ms,updated_at_ms)
VALUES(?,?,?,?,?,?,0,0,?,?)`,
			matchID, row.Seq, row.PlayerID, uint8(row.Kind), itemID, count, nowMs, nowMs); err != nil {
			return errcode.New(errcode.ErrUnavailable,
				"insert battle progress action match=%d seq=%d player=%d: %v",
				matchID, row.Seq, row.PlayerID, err)
		}
		return nil
	default:
		return nil
	}
}

func progressSingleStackFact(row ProgressOutboxRecord) (uint32, uint32, error) {
	itemConfigIDs := row.ItemConfigIDs
	if len(itemConfigIDs) == 0 || itemConfigIDs[0] == 0 {
		return 0, 0, errcode.New(errcode.ErrInvalidArg, "empty battle stack action")
	}
	if row.ItemCount > 0 {
		if len(itemConfigIDs) != 1 {
			return 0, 0, errcode.New(errcode.ErrInvalidArg,
				"compact battle stack action must contain exactly one item_config_id")
		}
		return itemConfigIDs[0], row.ItemCount, nil
	}
	if uint64(len(itemConfigIDs)) > uint64(^uint32(0)) {
		return 0, 0, errcode.New(errcode.ErrInvalidArg, "battle stack action count overflows uint32")
	}
	itemID := itemConfigIDs[0]
	for _, got := range itemConfigIDs[1:] {
		if got != itemID {
			return 0, 0, errcode.New(errcode.ErrInvalidArg,
				"battle stack action mixes item_config_ids %d and %d", itemID, got)
		}
	}
	return itemID, uint32(len(itemConfigIDs)), nil
}

// FetchProgressOutbox 按 id 升序取最多 limit 条**已到重试时点**的待发放进度出箱记录。
// 同 (match,player) 只返回 seq/id 最早的一条：即使前序失败已被退避，后序也不能
// 越过它先发，保证 ItemPickup 的 Grant 一定先于后续 ItemConsume。
// next_attempt_at_ms 过滤 + DeferProgressOutbox 退避,保证个别永久失败行(坏数据 /
// granter 未配)不会长期占满首批饿死后续正常行(审计 P1 队首阻塞)。
func (r *MySQLBattleRepo) FetchProgressOutbox(ctx context.Context, limit int) ([]ProgressOutboxRecord, error) {
	if limit <= 0 {
		limit = 128
	}
	const q = `SELECT cur.id, cur.match_id, cur.seq, cur.player_id, cur.kind, cur.exp_delta, cur.item_config_ids, cur.item_count
FROM battle_progress_outbox cur
WHERE cur.next_attempt_at_ms <= ?
  AND NOT EXISTS (
    SELECT 1 FROM battle_progress_outbox prev
    WHERE prev.match_id = cur.match_id AND prev.player_id = cur.player_id
      AND (prev.seq < cur.seq OR (prev.seq = cur.seq AND prev.id < cur.id))
  )
ORDER BY cur.id ASC LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, time.Now().UnixMilli(), limit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query progress outbox: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]ProgressOutboxRecord, 0, limit)
	for rows.Next() {
		var (
			rec  ProgressOutboxRecord
			kind uint8
			csv  string
		)
		if serr := rows.Scan(&rec.ID, &rec.MatchID, &rec.Seq, &rec.PlayerID, &kind, &rec.ExpDelta, &csv, &rec.ItemCount); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan progress outbox: %v", serr)
		}
		rec.Kind = ProgressGrantKind(kind)
		rec.ItemConfigIDs = decodeConfigIDs(csv)
		out = append(out, rec)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate progress outbox: %v", rerr)
	}
	return out, nil
}

// FetchProgressOutboxForPlayer 忽略 next_attempt_at_ms，供同步 action 路径主动驱动
// 该玩家不超过 target seq 的最早出箱。其它玩家完全不受影响。
func (r *MySQLBattleRepo) FetchProgressOutboxForPlayer(ctx context.Context, matchID, playerID, maxSeq uint64) (ProgressOutboxRecord, bool, error) {
	var (
		rec  ProgressOutboxRecord
		kind uint8
		csv  string
	)
	err := r.db.QueryRowContext(ctx, `SELECT id,match_id,seq,player_id,kind,exp_delta,item_config_ids,item_count
FROM battle_progress_outbox
WHERE match_id=? AND player_id=? AND seq<=?
ORDER BY seq ASC,id ASC LIMIT 1`, matchID, playerID, maxSeq).
		Scan(&rec.ID, &rec.MatchID, &rec.Seq, &rec.PlayerID, &kind, &rec.ExpDelta, &csv, &rec.ItemCount)
	if errors.Is(err, sql.ErrNoRows) {
		return ProgressOutboxRecord{}, false, nil
	}
	if err != nil {
		return ProgressOutboxRecord{}, false, errcode.New(errcode.ErrInternal,
			"query player progress outbox match=%d player=%d max_seq=%d: %v", matchID, playerID, maxSeq, err)
	}
	rec.Kind = ProgressGrantKind(kind)
	rec.ItemConfigIDs = decodeConfigIDs(csv)
	return rec, true, nil
}

// GetProgressAction 返回 consume/discard 的持久事实与权威结果；不存在不能解释为成功。
func (r *MySQLBattleRepo) GetProgressAction(ctx context.Context, matchID, seq, playerID uint64, kind ProgressGrantKind) (ProgressAction, bool, error) {
	var (
		a          ProgressAction
		storedKind uint8
		status     uint8
		code       int32
	)
	err := r.db.QueryRowContext(ctx, `SELECT match_id,seq,player_id,kind,item_config_id,count,status,result_code
FROM battle_progress_action WHERE match_id=? AND seq=? AND player_id=? AND kind=? LIMIT 1`,
		matchID, seq, playerID, uint8(kind)).
		Scan(&a.MatchID, &a.Seq, &a.PlayerID, &storedKind, &a.ItemConfigID, &a.Count, &status, &code)
	if errors.Is(err, sql.ErrNoRows) {
		return ProgressAction{}, false, nil
	}
	if err != nil {
		return ProgressAction{}, false, errcode.New(errcode.ErrInternal,
			"query progress action match=%d seq=%d player=%d kind=%d: %v",
			matchID, seq, playerID, kind, err)
	}
	a.Kind = ProgressGrantKind(storedKind)
	a.Status = ProgressActionStatus(status)
	a.ResultCode = errcode.Code(code)
	return a, true, nil
}

// ResolveProgressAction 原子落 action outcome 并删除对应 outbox。inventory RPC 与
// battle MySQL 之间的崩溃窗由 inventory 幂等键收敛：RPC 成功后若本事务未提交，重试
// 会得到同一 RPC 结果，再完成本事务。并发执行以首个已提交 outcome 为权威。
func (r *MySQLBattleRepo) ResolveProgressAction(ctx context.Context, row ProgressOutboxRecord, resultCode errcode.Code) (ProgressAction, error) {
	itemID, count, err := progressSingleStackFact(row)
	if err != nil {
		return ProgressAction{}, err
	}
	if row.ID == 0 || row.MatchID == 0 || row.Seq == 0 || row.PlayerID == 0 ||
		(row.Kind != ProgressConsumeStack && row.Kind != ProgressDiscardStack) {
		return ProgressAction{}, errcode.New(errcode.ErrInvalidArg, "resolve progress action requires persisted action outbox row")
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return ProgressAction{}, errcode.New(errcode.ErrInternal, "begin resolve progress action: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		a          ProgressAction
		storedKind uint8
		status     uint8
		code       int32
	)
	err = tx.QueryRowContext(ctx, `SELECT match_id,seq,player_id,kind,item_config_id,count,status,result_code
FROM battle_progress_action
WHERE match_id=? AND seq=? AND player_id=? AND kind=? FOR UPDATE`,
		row.MatchID, row.Seq, row.PlayerID, uint8(row.Kind)).
		Scan(&a.MatchID, &a.Seq, &a.PlayerID, &storedKind, &a.ItemConfigID, &a.Count, &status, &code)
	if errors.Is(err, sql.ErrNoRows) {
		return ProgressAction{}, errcode.New(errcode.ErrInvalidState,
			"progress action outcome missing match=%d seq=%d player=%d kind=%d",
			row.MatchID, row.Seq, row.PlayerID, row.Kind)
	}
	if err != nil {
		return ProgressAction{}, errcode.New(errcode.ErrInternal, "lock progress action: %v", err)
	}
	a.Kind = ProgressGrantKind(storedKind)
	a.Status = ProgressActionStatus(status)
	a.ResultCode = errcode.Code(code)
	if a.ItemConfigID != itemID || a.Count != count {
		return ProgressAction{}, errcode.New(errcode.ErrInvalidState,
			"progress action/outbox mismatch match=%d seq=%d stored=%d:%d row=%d:%d",
			row.MatchID, row.Seq, a.ItemConfigID, a.Count, itemID, count)
	}
	if a.Status == ProgressActionPending {
		if resultCode == errcode.OK {
			a.Status = ProgressActionSucceeded
			a.ResultCode = errcode.OK
		} else {
			// 预留发生在 ApplyProgress。终态业务失败意味着 inventory 没有扣物、UE
			// 也会保留本地物品，因此必须在 outcome 同一事务释放 spent 额度；否则
			// 玩家用新 seq 重试同一意图会被假余额永久拒绝。
			res, releaseErr := tx.ExecContext(ctx, `UPDATE battle_progress_item_balance
SET spent_count=spent_count-?,updated_at_ms=?
WHERE match_id=? AND player_id=? AND item_config_id=? AND spent_count>=?`,
				count, time.Now().UnixMilli(), a.MatchID, a.PlayerID, a.ItemConfigID, count)
			if releaseErr != nil {
				return ProgressAction{}, errcode.New(errcode.ErrInternal,
					"release failed progress action balance: %v", releaseErr)
			}
			affected, releaseErr := res.RowsAffected()
			if releaseErr != nil || affected != 1 {
				return ProgressAction{}, errcode.New(errcode.ErrInvalidState,
					"release failed progress action balance match=%d seq=%d affected=%d err=%v",
					a.MatchID, a.Seq, affected, releaseErr)
			}
			a.Status = ProgressActionFailed
			a.ResultCode = resultCode
		}
		if _, err := tx.ExecContext(ctx, `UPDATE battle_progress_action
SET status=?,result_code=?,updated_at_ms=?
WHERE match_id=? AND seq=? AND player_id=? AND kind=? AND status=0`,
			uint8(a.Status), int32(a.ResultCode), time.Now().UnixMilli(),
			a.MatchID, a.Seq, a.PlayerID, uint8(a.Kind)); err != nil {
			return ProgressAction{}, errcode.New(errcode.ErrInternal, "update progress action outcome: %v", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM battle_progress_outbox
WHERE id=? AND match_id=? AND seq=? AND player_id=? AND kind=?`,
		row.ID, row.MatchID, row.Seq, row.PlayerID, uint8(row.Kind)); err != nil {
		return ProgressAction{}, errcode.New(errcode.ErrInternal, "delete resolved progress action outbox id=%d: %v", row.ID, err)
	}
	// 「使用道具」任务事实与扣除结果同事务落定:成功放行、失败删行(见
	// settleMissionFactPendingTx)。丢弃(discard)不产任务事实,命中 0 行即可。
	if row.Kind == ProgressConsumeStack {
		if err := settleMissionFactPendingTx(ctx, tx, a.MatchID, a.Seq, a.PlayerID,
			a.Status == ProgressActionSucceeded); err != nil {
			return ProgressAction{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return ProgressAction{}, errcode.New(errcode.ErrInternal, "commit progress action outcome: %v", err)
	}
	return a, nil
}

// DeleteProgressOutbox 删除已成功发放的进度出箱行。
func (r *MySQLBattleRepo) DeleteProgressOutbox(ctx context.Context, id int64) error {
	if _, err := r.db.ExecContext(ctx, `DELETE FROM battle_progress_outbox WHERE id = ?`, id); err != nil {
		return errcode.New(errcode.ErrInternal, "delete progress outbox id=%d: %v", id, err)
	}
	return nil
}

// DeferProgressOutbox 发放失败后把出箱行推迟到指数退避后的下一次尝试
// (2s·2^n,封顶 5min;行永不丢弃 —— 上限封顶后持续告警由人工介入,at-least-once 不变)。
func (r *MySQLBattleRepo) DeferProgressOutbox(ctx context.Context, id int64) error {
	if _, err := r.db.ExecContext(ctx,
		`UPDATE battle_progress_outbox
SET attempt_count = attempt_count + 1,
    next_attempt_at_ms = ? + LEAST(2000 * POW(2, LEAST(attempt_count, 7)), 300000)
WHERE id = ?`, time.Now().UnixMilli(), id); err != nil {
		return errcode.New(errcode.ErrInternal, "defer progress outbox id=%d: %v", id, err)
	}
	return nil
}

// ValidateProgressSchema 启动时探测实时进度五表(stream/outbox/player/item balance/action,
// 含累计上限/退避/outcome 列;000005+000006+000009 迁移产物),缺失即失败:
// settleProgressStreamTx 在**每次结算**都会无条件访问水位表,不能等首个结算才炸(§16.4)。
func (r *MySQLBattleRepo) ValidateProgressSchema(ctx context.Context) error {
	checks := []string{
		`SELECT match_id, last_applied_seq, total_exp, total_items, final_seq, settled_at_ms, stopped_at_ms, updated_at_ms FROM battle_progress_stream LIMIT 0`,
		`SELECT id, match_id, seq, player_id, kind, exp_delta, item_config_ids, item_count, next_attempt_at_ms, attempt_count, created_at_ms FROM battle_progress_outbox LIMIT 0`,
		`SELECT match_id, player_id, total_exp, total_items, total_kills, updated_at_ms FROM battle_progress_player LIMIT 0`,
		`SELECT match_id, player_id, item_config_id, picked_count, spent_count, updated_at_ms FROM battle_progress_item_balance LIMIT 0`,
		`SELECT match_id, seq, player_id, kind, item_config_id, count, status, result_code, created_at_ms, updated_at_ms FROM battle_progress_action LIMIT 0`,
	}
	for _, query := range checks {
		rows, err := r.db.QueryContext(ctx, query)
		if err != nil {
			return errcode.New(errcode.ErrInternal, "battle progress schema invalid: %v", err)
		}
		if err := rows.Close(); err != nil {
			return errcode.New(errcode.ErrInternal, "close battle progress schema probe: %v", err)
		}
	}
	// stopped_at_ms 列级契约(审计 P2:只探测列存在时,错类型/可空/坏默认值的手工漂移
	// 列也会通过——停流 fencing 依赖 "缺省 0 = 未停流" 的语义)。
	var (
		dataType   string
		isNullable string
		colDefault sql.NullString
	)
	if err := r.db.QueryRowContext(ctx,
		`SELECT DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'battle_progress_stream' AND COLUMN_NAME = 'stopped_at_ms'`,
	).Scan(&dataType, &isNullable, &colDefault); err != nil {
		return errcode.New(errcode.ErrInternal, "probe stopped_at_ms column contract: %v", err)
	}
	if dataType != "bigint" || isNullable != "NO" || !colDefault.Valid || colDefault.String != "0" {
		return errcode.New(errcode.ErrInternal,
			"battle_progress_stream.stopped_at_ms contract violated (type=%s nullable=%s default=%v, want bigint/NO/0): stop fencing semantics broken",
			dataType, isNullable, colDefault.String)
	}
	return nil
}

// ProgressSettleInfo 是 SaveResult 事务内对实时进度通道的结算收口结果。
type ProgressSettleInfo struct {
	// StreamExisted 结算时水位行是否已存在(= 本场走过实时通道)。
	StreamExisted bool
	// LastAppliedSeq 结算时的已应用水位(与 DS 上报 final_progress_seq 对账)。
	LastAppliedSeq uint64
	// DropsSuppressed true = 掉落发放权已归实时通道,结算路径的 dropped_item_config_ids
	// 只作对账不再发放(单一权威路径,realtime-progression.md §5)。
	DropsSuppressed bool
}

// settleProgressStreamTx 在结算事务内收口实时进度通道:
//  1. 锁定(或创建)水位行并打终局标记(settled_at_ms>0)→ 之后任何 ReportProgress 一律拒
//     (僵尸 / 分区恢复 DS fencing;ABANDONED 同样收口);
//  2. 返回水位信息,SaveResult 据 last_applied_seq>0 决定是否抑制结算路径掉落发放。
//
// 判定依据是服务端自己的水位表,不信 DS 声明:恶意 DS 两头上报也只有一条路径发放。
// 幂等:已打过终局标记的行原样返回不再改写(重复结算 / battles 幂等重放分支也会调本函数,
// 首次结算的 settled_at_ms / final_seq 是权威审计值,不被重放覆盖)。
func settleProgressStreamTx(ctx context.Context, tx *sql.Tx, matchID uint64, finalSeq uint64, nowMs int64) (ProgressSettleInfo, error) {
	var (
		lastSeq   uint64
		settledMs int64
	)
	err := tx.QueryRowContext(ctx,
		`SELECT last_applied_seq, settled_at_ms FROM battle_progress_stream WHERE match_id = ? FOR UPDATE`,
		matchID,
	).Scan(&lastSeq, &settledMs)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		// 本场未走实时通道:插入终局标记行,封死迟到进度(水位 0,掉落走结算路径)。
		if _, ierr := tx.ExecContext(ctx,
			`INSERT INTO battle_progress_stream (match_id, last_applied_seq, final_seq, settled_at_ms, updated_at_ms)
VALUES (?, 0, ?, ?, ?)`, matchID, finalSeq, nowMs, nowMs); ierr != nil {
			return ProgressSettleInfo{}, ierr
		}
		return ProgressSettleInfo{}, nil
	case err != nil:
		return ProgressSettleInfo{}, err
	}
	info := ProgressSettleInfo{
		StreamExisted:   true,
		LastAppliedSeq:  lastSeq,
		DropsSuppressed: lastSeq > 0,
	}
	if settledMs > 0 {
		return info, nil // 已收口(幂等重放),不改写首次结算标记
	}
	if _, uerr := tx.ExecContext(ctx,
		`UPDATE battle_progress_stream SET settled_at_ms = ?, final_seq = ?, updated_at_ms = ? WHERE match_id = ?`,
		nowMs, finalSeq, nowMs, matchID); uerr != nil {
		return ProgressSettleInfo{}, uerr
	}
	return info, nil
}
