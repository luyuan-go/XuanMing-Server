// experience_repo.go — 玩家等级经验幂等入账 + 经验推送事务出箱(实时成长,2026-07-20)。
//
// 库表(deploy/mysql-init/04-player-tables.sql / tools/migrate pandora_player 000002):
//
//	pandora_player.players.exp        级内经验列(满级恒 0)
//	pandora_player.exp_history        经验入账历史 + 幂等键(uk player_id+idempotency_key,不变量 §2)
//	pandora_player.player_push_outbox 经验推送事务出箱(与入账同事务原子提交,不变量 §4)
//
// 幂等:ApplyExperience 在一个事务里 INSERT exp_history;命中 1062 唯一键冲突 → 视为已入账
// (already=true),回读当前权威快照返回,不重复加经验、不重复出箱。
// 等级结算(连升多级 / 封顶 / 满级不累加)是纯函数 AdvanceExperience,单测覆盖。
package data

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
)

// ExpApply 是一次经验入账请求(biz 校验合法性后传入)。
// Curve 是等级经验曲线:第 i 项(0 基)= 从 Lv(i+1) 升到 Lv(i+2) 所需级内经验(>0);
// 最高等级 = len(Curve)+1(与策划 j_玩家等级经验.xlsx / 客户端 CfgPlayerLevelExp 同源)。
type ExpApply struct {
	PlayerID       uint64
	Delta          uint64
	Reason         string
	IdempotencyKey string
	Curve          []uint64
}

// ExpState 是入账后(或幂等命中时当前)的权威经验快照。
type ExpState struct {
	Level        int32
	ExpInLevel   uint64
	IsMaxLevel   bool
	LevelsGained uint32
}

// AdvanceExperience 纯函数:级内经验加 delta 后按曲线循环进位(天然支持一次连升多级)。
//
//	返回 (新等级, 新级内经验, 升级数)。
//	- 满级(level >= len(curve)+1)→ 原样返回且级内经验恒 0(不再累加,需求「满级 MAX」);
//	- 升到满级的瞬间级内经验清 0;
//	- 曲线项为 0(非法配置,防御)→ 视为不可升级,停止进位;
//	- 加法回绕(理论上 delta 有上限不会发生,防御)→ 按「足够升满」处理。
func AdvanceExperience(level int32, expInLevel uint64, delta uint64, curve []uint64) (int32, uint64, uint32) {
	maxLevel := int32(len(curve)) + 1
	if level < 1 {
		level = 1
	}
	if level >= maxLevel {
		return maxLevel, 0, 0
	}
	exp := expInLevel + delta
	if exp < expInLevel {
		// uint64 回绕(防御):按可升满处理。
		exp = ^uint64(0)
	}
	var gained uint32
	for level < maxLevel {
		need := curve[level-1]
		if need == 0 || exp < need {
			break
		}
		exp -= need
		level++
		gained++
		if level >= maxLevel {
			exp = 0 // 满级不累加(需求:满级后显示 MAX,不再增加经验)
			break
		}
	}
	return level, exp, gained
}

// ApplyExperience 幂等入账经验并结算等级,与经验推送出箱同一事务原子提交。
//
// 事务顺序(锁序固定,防死锁):先锁 players 行 → 满级判定 → INSERT exp_history(幂等)
// → UPDATE players → INSERT player_push_outbox。
// 满级 no-op:不加经验、不出箱,但仍消费幂等键落 no-op 收据(见分支内注释);
// 重放命中收据按契约返回 already=true(proto:true = 幂等命中,本次未重复入账)。
func (r *MySQLPlayerRepo) ApplyExperience(ctx context.Context, apply ExpApply) (ExpState, bool, error) {
	maxLevel := int32(len(apply.Curve)) + 1

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return ExpState{}, false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		level int32
		exp   uint64
	)
	err = tx.QueryRowContext(ctx,
		`SELECT level, exp FROM players WHERE player_id = ? FOR UPDATE`, apply.PlayerID,
	).Scan(&level, &exp)
	if errors.Is(err, sql.ErrNoRows) {
		return ExpState{}, false, errcode.New(errcode.ErrPlayerNotFound, "player not found: %d", apply.PlayerID)
	}
	if err != nil {
		return ExpState{}, false, errcode.New(errcode.ErrInternal, "lock player=%d: %v", apply.PlayerID, err)
	}

	// 满级 no-op:不加经验、不出箱,但**仍消费幂等键**(审计 P2):事件语义是"已消费并
	// 丢弃"。若不落收据,成功响应丢失 + 未来曲线扩容(上限提升)后,滞留在上游出箱的
	// 同一事件重试会被重新入账,破坏 exactly-once。
	if level >= maxLevel {
		alreadyConsumed := false
		const insNoopHistory = `INSERT INTO exp_history
(player_id, idempotency_key, exp_delta, reason, old_level, old_exp, new_level, new_exp)
VALUES (?, ?, ?, ?, ?, 0, ?, 0)`
		if _, herr := tx.ExecContext(ctx, insNoopHistory,
			apply.PlayerID, apply.IdempotencyKey, apply.Delta, apply.Reason,
			maxLevel, maxLevel,
		); herr != nil {
			if !isDupErr(herr) {
				return ExpState{}, false, errcode.New(errcode.ErrInternal, "insert max-level noop receipt player=%d: %v", apply.PlayerID, herr)
			}
			// 幂等命中(收据已在):契约 already=true(审计 P2:重放返回 false 违反
			// proto "true = 幂等命中" 语义,上游据 already 区分首次/重放时会误判)。
			alreadyConsumed = true
		}
		if cerr := tx.Commit(); cerr != nil {
			return ExpState{}, false, errcode.New(errcode.ErrInternal, "commit max-level noop player=%d: %v", apply.PlayerID, cerr)
		}
		return ExpState{Level: maxLevel, ExpInLevel: 0, IsMaxLevel: true}, alreadyConsumed, nil
	}

	newLevel, newExp, gained := AdvanceExperience(level, exp, apply.Delta, apply.Curve)

	const insHistory = `INSERT INTO exp_history
(player_id, idempotency_key, exp_delta, reason, old_level, old_exp, new_level, new_exp)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	if _, herr := tx.ExecContext(ctx, insHistory,
		apply.PlayerID, apply.IdempotencyKey, apply.Delta, apply.Reason,
		level, exp, newLevel, newExp,
	); herr != nil {
		if isDupErr(herr) {
			// 幂等命中:回读锁内当前权威快照(原次入账已生效),不重复加、不重复出箱。
			if cerr := tx.Commit(); cerr != nil {
				return ExpState{}, false, errcode.New(errcode.ErrInternal, "commit idempotent hit player=%d: %v", apply.PlayerID, cerr)
			}
			return ExpState{
				Level:      level,
				ExpInLevel: exp,
				IsMaxLevel: level >= maxLevel,
			}, true, nil
		}
		return ExpState{}, false, errcode.New(errcode.ErrInternal, "insert exp history player=%d: %v", apply.PlayerID, herr)
	}

	if _, uerr := tx.ExecContext(ctx,
		`UPDATE players SET level = ?, exp = ? WHERE player_id = ?`,
		newLevel, newExp, apply.PlayerID,
	); uerr != nil {
		return ExpState{}, false, errcode.New(errcode.ErrInternal, "update exp player=%d: %v", apply.PlayerID, uerr)
	}

	// 经验推送出箱:与入账同事务(不变量 §4)。payload 是入账后的权威快照,
	// push 经 pandora.player.experience(event_type=EXPERIENCE)透传给客户端。
	evt := &playerv1.PlayerExperienceEvent{
		PlayerId:     apply.PlayerID,
		Level:        newLevel,
		ExpInLevel:   newExp,
		IsMaxLevel:   newLevel >= maxLevel,
		LevelsGained: gained,
		TsMs:         time.Now().UnixMilli(),
	}
	payload, merr := proto.Marshal(evt)
	if merr != nil {
		return ExpState{}, false, errcode.New(errcode.ErrInternal, "marshal experience event player=%d: %v", apply.PlayerID, merr)
	}
	const insOutbox = `INSERT INTO player_push_outbox (player_id, event_type, payload, created_at_ms)
VALUES (?, ?, ?, ?)`
	if _, oerr := tx.ExecContext(ctx, insOutbox,
		apply.PlayerID, uint32(playerv1.PlayerPushEventType_PLAYER_PUSH_EVENT_TYPE_EXPERIENCE),
		payload, time.Now().UnixMilli(),
	); oerr != nil {
		return ExpState{}, false, errcode.New(errcode.ErrInternal, "insert push outbox player=%d: %v", apply.PlayerID, oerr)
	}

	if cerr := tx.Commit(); cerr != nil {
		return ExpState{}, false, errcode.New(errcode.ErrInternal, "commit exp player=%d: %v", apply.PlayerID, cerr)
	}
	return ExpState{
		Level:        newLevel,
		ExpInLevel:   newExp,
		IsMaxLevel:   newLevel >= maxLevel,
		LevelsGained: gained,
	}, false, nil
}

// PushOutboxRecord 是一条待发布的玩家推送事务出箱记录(FIFO 按 id)。
type PushOutboxRecord struct {
	ID        int64
	PlayerID  uint64
	EventType uint32
	Payload   []byte
}

// FetchPushOutbox 按 id 升序取最多 limit 条待发布推送出箱记录(FIFO 保序,同玩家事件有序)。
func (r *MySQLPlayerRepo) FetchPushOutbox(ctx context.Context, limit int) ([]PushOutboxRecord, error) {
	if limit <= 0 {
		limit = 128
	}
	const q = `SELECT id, player_id, event_type, payload FROM player_push_outbox ORDER BY id ASC LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, limit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query push outbox: %v", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]PushOutboxRecord, 0, limit)
	for rows.Next() {
		var rec PushOutboxRecord
		if serr := rows.Scan(&rec.ID, &rec.PlayerID, &rec.EventType, &rec.Payload); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan push outbox: %v", serr)
		}
		out = append(out, rec)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate push outbox: %v", rerr)
	}
	return out, nil
}

// DeletePushOutbox 删除已成功投递的推送出箱行。
func (r *MySQLPlayerRepo) DeletePushOutbox(ctx context.Context, id int64) error {
	if _, err := r.db.ExecContext(ctx, `DELETE FROM player_push_outbox WHERE id = ?`, id); err != nil {
		return errcode.New(errcode.ErrInternal, "delete push outbox id=%d: %v", id, err)
	}
	return nil
}

// SweepExpHistory 按保留期处理经验幂等收据(mode=report_only 只数不删,delete 才批删)。
// 走 idx_created 前导索引(无索引时收据全部未到期的稳态下会每小时全表扫,审计 P2);
// 多副本并发调用安全(各删各的行)。
func (r *MySQLPlayerRepo) SweepExpHistory(ctx context.Context, mode dbguard.Mode, cutoff time.Time, limit int) (dbguard.Outcome, error) {
	return r.sweepByCreatedAt(ctx, mode, "exp_history", cutoff, limit)
}

// SweepMMRHistory / SweepAttrPointGrants / SweepTalentPointGrants / SweepSkillCardGrants:
// 保留期清理(CLAUDE.md §9 不变量 24)。四表均按 created_at 处理(需 idx_created,
// 见 04-player-tables.sql 与 tools/migrate 000003 / 000005);多副本并发调用安全。
func (r *MySQLPlayerRepo) SweepMMRHistory(ctx context.Context, mode dbguard.Mode, cutoff time.Time, limit int) (dbguard.Outcome, error) {
	return r.sweepByCreatedAt(ctx, mode, "mmr_history", cutoff, limit)
}

func (r *MySQLPlayerRepo) SweepAttrPointGrants(ctx context.Context, mode dbguard.Mode, cutoff time.Time, limit int) (dbguard.Outcome, error) {
	return r.sweepByCreatedAt(ctx, mode, "attr_point_grants", cutoff, limit)
}

func (r *MySQLPlayerRepo) SweepTalentPointGrants(ctx context.Context, mode dbguard.Mode, cutoff time.Time, limit int) (dbguard.Outcome, error) {
	return r.sweepByCreatedAt(ctx, mode, "talent_point_grants", cutoff, limit)
}

func (r *MySQLPlayerRepo) SweepSkillCardGrants(ctx context.Context, mode dbguard.Mode, cutoff time.Time, limit int) (dbguard.Outcome, error) {
	return r.sweepByCreatedAt(ctx, mode, "skill_card_grants", cutoff, limit)
}

// sweepByCreatedAt 按 created_at < cutoff 处理指定表(表名只来自本文件内固定调用点,非外部输入)。
//
// 走 dbguard.SweepTable 而不是自己写 COUNT / DELETE 两条 SQL:它强制"报告"与"实删"
// 共用同一个 where + 同一组参数,从机制上排除"报告说 0 行、实际删了 10 万行"的条件漂移
// (§9.24);WARN 日志与 pandora_db_retention_pending_rows gauge 也由它统一打,
// 全服清理告警长一个样,排查时对得上。
func (r *MySQLPlayerRepo) sweepByCreatedAt(ctx context.Context, mode dbguard.Mode, table string, cutoff time.Time, limit int) (dbguard.Outcome, error) {
	if limit <= 0 {
		limit = 1000
	}
	out, err := dbguard.SweepTable(ctx, r.db, mode, "pandora_player", table, "created_at < ?", limit, cutoff)
	if err != nil {
		return out, errcode.New(errcode.ErrInternal, "sweep %s: %v", table, err)
	}
	return out, nil
}

// ValidateExperienceSchema 启动时探测经验相关表列,缺失即失败(fail-fast,§16.4):
// 副本不能先 Ready 再在首个 GetProfile / AddExperience 上大面积报错。
// 纪律对齐 battle_result.ValidateRecoveryOutboxSchema。
func (r *MySQLPlayerRepo) ValidateExperienceSchema(ctx context.Context) error {
	checks := []string{
		`SELECT player_id, level, exp FROM players LIMIT 0`,
		`SELECT id, player_id, idempotency_key, exp_delta, reason, old_level, old_exp, new_level, new_exp, created_at FROM exp_history LIMIT 0`,
		`SELECT id, player_id, event_type, payload, created_at_ms FROM player_push_outbox LIMIT 0`,
	}
	for _, query := range checks {
		rows, err := r.db.QueryContext(ctx, query)
		if err != nil {
			return errcode.New(errcode.ErrInternal, "player experience schema invalid: %v", err)
		}
		if err := rows.Close(); err != nil {
			return errcode.New(errcode.ErrInternal, "close experience schema probe: %v", err)
		}
	}
	// 幂等唯一索引是 exactly-once 的权威机制(不变量 §2),不是可选优化:列探测通过但
	// uk 缺失(手工建表漂移)时 isDupErr 永不触发,重试直接双发。启动即失败(审计 P2)。
	// 必须核对**列名、顺序与 SUB_PART**(审计 P1:同名错列的 UNIQUE(id,idempotency_key)
	// 有两列也得拦;前缀唯一索引 UNIQUE(player_id, idempotency_key(1)) 列名顺序全对,
	// 却会把首字符相同的不同幂等键判成 duplicate,静默少发经验——SUB_PART 必须为 NULL)。
	rows, err := r.db.QueryContext(ctx,
		`SELECT SEQ_IN_INDEX, COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'exp_history' AND INDEX_NAME = 'uk_player_idem' AND NON_UNIQUE = 0
ORDER BY SEQ_IN_INDEX`)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "probe exp_history unique index: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var cols []string
	for rows.Next() {
		var (
			seq     int
			name    string
			subPart sql.NullInt64
		)
		if serr := rows.Scan(&seq, &name, &subPart); serr != nil {
			return errcode.New(errcode.ErrInternal, "scan exp_history unique index: %v", serr)
		}
		if seq != len(cols)+1 {
			return errcode.New(errcode.ErrInternal,
				"exp_history uk_player_idem column sequence broken at %d (got seq %d)", len(cols)+1, seq)
		}
		if subPart.Valid {
			return errcode.New(errcode.ErrInternal,
				"exp_history uk_player_idem column %s is a prefix index (SUB_PART=%d): full-column uniqueness required, idempotency broken",
				name, subPart.Int64)
		}
		cols = append(cols, name)
	}
	if rerr := rows.Err(); rerr != nil {
		return errcode.New(errcode.ErrInternal, "iterate exp_history unique index: %v", rerr)
	}
	if len(cols) != 2 || cols[0] != "player_id" || cols[1] != "idempotency_key" {
		return errcode.New(errcode.ErrInternal,
			"exp_history uk_player_idem missing or malformed (columns=%v, want [player_id idempotency_key]): idempotency broken", cols)
	}
	return nil
}

// ValidateExperienceLevels 在副本 Ready 前确认持久化等级落在当前策划表范围内。
// 热更另由 Store validator 禁止降低最高等级，因此通过启动门后不会因换表把玩家降级。
func (r *MySQLPlayerRepo) ValidateExperienceLevels(ctx context.Context, maxLevel int32) error {
	if maxLevel < 1 {
		return errcode.New(errcode.ErrInvalidState, "player level table max_level invalid: %d", maxLevel)
	}
	var minLevel, storedMax int32
	if err := r.db.QueryRowContext(ctx,
		`SELECT COALESCE(MIN(level), 1), COALESCE(MAX(level), 1) FROM players`,
	).Scan(&minLevel, &storedMax); err != nil {
		return errcode.New(errcode.ErrInternal, "validate player level range: %v", err)
	}
	if minLevel < 1 || storedMax > maxLevel {
		return errcode.New(errcode.ErrInvalidState,
			"players.level range [%d,%d] outside config range [1,%d]", minLevel, storedMax, maxLevel)
	}
	return nil
}
