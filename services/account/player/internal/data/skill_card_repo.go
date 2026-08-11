package data

import (
	"context"
	"database/sql"
	"errors"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// skill_card_repo.go — 技能卡持有 / 培养 / 更换的数据层。
//
// 表:player_skill_cards(持卡:等级 + 碎片)、player_skill_slots(卡槽装配)、
//     skill_card_grants(发放幂等收据)。
//
// 配置相关的判定(每级消耗、等级上限、卡是否存在、槽位数)全部由 biz 按配置表算好后传入,
// repo 层看不到配置表(同 SetTalents 的 SpentPoints 处理)。这样"改配置表"永远不需要动 SQL。

// GrantSkillCards 幂等发放技能卡 / 碎片。
//
// 已持有 → 碎片累加(等级不动:发放不改变培养进度);未持有 → 以 1 级建卡,碎片记为初始余量。
// 命中幂等键 → 读回当前持卡并返回 already=true,不重复入账。
func (r *MySQLPlayerRepo) GrantSkillCards(ctx context.Context, playerID uint64, grants []SkillCardGrant, idempotencyKey string) ([]SkillCard, bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	const insGrant = `INSERT INTO skill_card_grants (player_id, idempotency_key) VALUES (?, ?)`
	if _, gerr := tx.ExecContext(ctx, insGrant, playerID, idempotencyKey); gerr != nil {
		if isDupErr(gerr) {
			// 幂等命中:本次一张卡一片碎片都不加,只把当前持卡读回去。
			cards, cerr := skillCardsOf(ctx, tx, playerID)
			if cerr != nil {
				return nil, false, cerr
			}
			return cards, true, nil
		}
		return nil, false, errcode.New(errcode.ErrInternal, "insert skill card grant player=%d: %v", playerID, gerr)
	}

	// ON DUPLICATE KEY UPDATE 让"首次获得"与"重复获得转碎片"走同一条语句:
	// 先查再决定 insert/update 会在并发发放下丢更新(两个请求都查到"没有",都去 insert,
	// 一个失败回滚整批)。level 刻意不写进 UPDATE 子句——发放不该重置已培养的等级。
	const ins = `INSERT INTO player_skill_cards (player_id, card_id, level, shards) VALUES (?, ?, 1, ?)
	             ON DUPLICATE KEY UPDATE shards = shards + VALUES(shards)`
	for _, g := range grants {
		if _, ierr := tx.ExecContext(ctx, ins, playerID, g.CardID, g.Shards); ierr != nil {
			return nil, false, errcode.New(errcode.ErrInternal,
				"grant skill card player=%d card=%d: %v", playerID, g.CardID, ierr)
		}
	}

	cards, cerr := skillCardsOf(ctx, tx, playerID)
	if cerr != nil {
		return nil, false, cerr
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "commit skill card grant player=%d: %v", playerID, commitErr)
	}
	return cards, false, nil
}

// UpgradeSkillCard 消耗碎片把一张卡升一级。
//
// costByLevel(目标等级 → 碎片消耗)与 maxLevel 由 biz 按配置表算好传入。
// 事务内 FOR UPDATE 锁住卡行:"读余量 → 判够不够 → 扣"三步之间若不加锁,
// 并发两次升级能用同一批碎片升两级(§16.1 TOCTOU)。
// 价钱也在锁内按读到的等级查曲线,不由调用方预先算好——否则并发两次都会按同一级的价钱扣。
func (r *MySQLPlayerRepo) UpgradeSkillCard(ctx context.Context, playerID uint64, cardID uint32, costByLevel map[uint32]uint32, maxLevel uint32) (SkillCard, uint32, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return SkillCard{}, 0, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	card := SkillCard{CardID: cardID}
	const sel = `SELECT level, shards FROM player_skill_cards WHERE player_id = ? AND card_id = ? FOR UPDATE`
	serr := tx.QueryRowContext(ctx, sel, playerID, cardID).Scan(&card.Level, &card.Shards)
	if errors.Is(serr, sql.ErrNoRows) {
		return SkillCard{}, 0, errcode.New(errcode.ErrSkillCardNotOwned,
			"skill card not owned player=%d card=%d", playerID, cardID)
	}
	if serr != nil {
		return SkillCard{}, 0, errcode.New(errcode.ErrInternal,
			"lock skill card player=%d card=%d: %v", playerID, cardID, serr)
	}

	if card.Level >= maxLevel {
		return SkillCard{}, 0, errcode.New(errcode.ErrSkillCardMaxLevel,
			"skill card at max level player=%d card=%d level=%d max=%d", playerID, cardID, card.Level, maxLevel)
	}
	targetLevel := card.Level + 1
	shardCost, ok := costByLevel[targetLevel]
	if !ok {
		// 曲线断档。绝不能当免费升级放行——加载期 ValidateCurves 已挡过一道,
		// 走到这里说明表和上限对不上,宁可拒绝也不给玩家白升。
		return SkillCard{}, 0, errcode.New(errcode.ErrInternal,
			"upgrade curve missing level player=%d card=%d level=%d", playerID, cardID, targetLevel)
	}
	if card.Shards < shardCost {
		return SkillCard{}, 0, errcode.New(errcode.ErrSkillCardInsufficientShards,
			"insufficient shards player=%d card=%d need=%d have=%d", playerID, cardID, shardCost, card.Shards)
	}

	const upd = `UPDATE player_skill_cards SET level = level + 1, shards = shards - ? WHERE player_id = ? AND card_id = ?`
	if _, uerr := tx.ExecContext(ctx, upd, shardCost, playerID, cardID); uerr != nil {
		return SkillCard{}, 0, errcode.New(errcode.ErrInternal,
			"upgrade skill card player=%d card=%d: %v", playerID, cardID, uerr)
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return SkillCard{}, 0, errcode.New(errcode.ErrInternal,
			"commit skill card upgrade player=%d card=%d: %v", playerID, cardID, commitErr)
	}

	card.Level = targetLevel
	card.Shards -= shardCost
	return card, shardCost, nil
}

// SetSkillSlots 全量替换卡槽装配(未列出的槽视为清空)。
//
// 先校验每张要装的卡都真的持有:装一张没有的卡在库层面不会报错(卡槽表不带外键),
// 结果是开局给不出技能且无从排查。
func (r *MySQLPlayerRepo) SetSkillSlots(ctx context.Context, playerID uint64, slots []SkillSlot) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	const own = `SELECT 1 FROM player_skill_cards WHERE player_id = ? AND card_id = ?`
	for _, s := range slots {
		var one int
		oerr := tx.QueryRowContext(ctx, own, playerID, s.CardID).Scan(&one)
		if errors.Is(oerr, sql.ErrNoRows) {
			return errcode.New(errcode.ErrSkillCardNotOwned,
				"skill card not owned player=%d card=%d slot=%d", playerID, s.CardID, s.Slot)
		}
		if oerr != nil {
			return errcode.New(errcode.ErrInternal,
				"check skill card owned player=%d card=%d: %v", playerID, s.CardID, oerr)
		}
	}

	if _, derr := tx.ExecContext(ctx, `DELETE FROM player_skill_slots WHERE player_id = ?`, playerID); derr != nil {
		return errcode.New(errcode.ErrInternal, "clear skill slots player=%d: %v", playerID, derr)
	}
	const ins = `INSERT INTO player_skill_slots (player_id, slot, card_id) VALUES (?, ?, ?)`
	for _, s := range slots {
		if _, ierr := tx.ExecContext(ctx, ins, playerID, s.Slot, s.CardID); ierr != nil {
			if isDupErr(ierr) {
				// uk_player_slot 或 uk_player_card_once 撞了:同槽两张卡,或同一张卡占两个槽。
				// biz 侧已校验过一次,能走到这里说明是并发两次 SetSkillSlots 交错——库是最后一道。
				return errcode.New(errcode.ErrSkillCardSlotInvalid,
					"duplicate slot or card player=%d slot=%d card=%d", playerID, s.Slot, s.CardID)
			}
			return errcode.New(errcode.ErrInternal,
				"insert skill slot player=%d slot=%d: %v", playerID, s.Slot, ierr)
		}
	}
	if commitErr := tx.Commit(); commitErr != nil {
		return errcode.New(errcode.ErrInternal, "commit skill slots player=%d: %v", playerID, commitErr)
	}
	return nil
}

func (r *MySQLPlayerRepo) GetSkillCards(ctx context.Context, playerID uint64) ([]SkillCard, error) {
	return skillCardsOf(ctx, r.db, playerID)
}

func (r *MySQLPlayerRepo) GetSkillSlots(ctx context.Context, playerID uint64) ([]SkillSlot, error) {
	const q = `SELECT slot, card_id FROM player_skill_slots WHERE player_id = ? ORDER BY slot`
	rows, err := r.db.QueryContext(ctx, q, playerID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query skill slots player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()

	var slots []SkillSlot
	for rows.Next() {
		var s SkillSlot
		if serr := rows.Scan(&s.Slot, &s.CardID); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan skill slot player=%d: %v", playerID, serr)
		}
		slots = append(slots, s)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate skill slots player=%d: %v", playerID, rerr)
	}
	return slots, nil
}

// queryer 抽象 *sql.DB / *sql.Tx 的 QueryContext,让持卡读取在事务内外共用一份实现。
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func skillCardsOf(ctx context.Context, q queryer, playerID uint64) ([]SkillCard, error) {
	const sel = `SELECT card_id, level, shards FROM player_skill_cards WHERE player_id = ? ORDER BY card_id`
	rows, err := q.QueryContext(ctx, sel, playerID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query skill cards player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()

	var cards []SkillCard
	for rows.Next() {
		var c SkillCard
		if serr := rows.Scan(&c.CardID, &c.Level, &c.Shards); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan skill card player=%d: %v", playerID, serr)
		}
		cards = append(cards, c)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate skill cards player=%d: %v", playerID, rerr)
	}
	return cards, nil
}
