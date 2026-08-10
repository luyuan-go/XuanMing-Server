// Package data 是 leaderboard 服务的数据层(Redis ZSET 实时排名 + MySQL 结算归档,2026-06-27)。
//
// 库表(deploy/mysql-init/10-leaderboard-tables.sql,pandora_leaderboard 库):
//
//	leaderboard_settlement  结算批次头(uk settle_idempotency_key 防重复结算,不变量 §9.2)
//	leaderboard_snapshot    结算 Top-N 名次快照(归档 / 对账)
//	leaderboard_reward_log  逐名次发奖记录(uk grant_idempotency_key 防重复发奖,不变量 §9.7)
//
// 进行中的实时排名 / 临时榜只在 Redis(board_store.go),不落库;MySQL 只兜结算结果 + 发奖凭证。
package data

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
)

// 发奖状态(leaderboard_reward_log.status)。
const (
	RewardPending int8 = 0
	RewardGranted int8 = 1
	RewardFailed  int8 = 2
)

// SettlementRecord 是 leaderboard_settlement 一行的存储视图。
type SettlementRecord struct {
	SettlementID  uint64
	BoardType     uint32
	Scope         int32
	ScopeID       uint64
	Period        string
	TopN          int32
	SettledCount  int32
	SettleIdemKey string
	ResetAfter    bool
	CreatedAtMs   int64
}

// SnapshotRow 是 leaderboard_snapshot 一行。
type SnapshotRow struct {
	Rank        int64
	EntityID    uint64
	Score       int64
	CreatedAtMs int64
}

// RewardLogRecord 是 leaderboard_reward_log 一行的存储视图。
//
// RewardPayload 是 pb `RewardGrantStorageRecord` 二进制(列 reward_pb VARBINARY)。
// 它既是审计明细,也是 RetryUngrantedRewards 补发时重放的权威入参——重放路径不得
// 因编码漂移解不出奖励,故用 proto 二进制而非 JSON(§5.8/§9.17)。
type RewardLogRecord struct {
	SettlementID  uint64
	EntityID      uint64
	Rank          int64
	GrantIdemKey  string
	Status        int8
	RewardPayload []byte
	CreatedAtMs   int64
	UpdatedAtMs   int64
}

// LeaderboardRepo 是结算归档库抽象。biz 只依赖此接口,不依赖 *sql.DB。
type LeaderboardRepo interface {
	// ClaimSettlement 幂等插入结算批次。命中 uk(settle_idempotency_key)→ already=true,返回已存批次(回放)。
	ClaimSettlement(ctx context.Context, rec *SettlementRecord) (existing *SettlementRecord, already bool, err error)
	// SaveSnapshot 批量落 Top-N 名次快照(已存在的 (settlement_id,rank) 忽略,幂等回放)。
	SaveSnapshot(ctx context.Context, settlementID uint64, rows []SnapshotRow) error
	// LoadSnapshot 按 settlement_id 读 Top-N 名次快照(rank 升序),供幂等命中后回放 winners
	//(首次结算 reset_after=true 已清空 Redis 榜,回放只能取 MySQL 快照)。
	LoadSnapshot(ctx context.Context, settlementID uint64) ([]SnapshotRow, error)
	// ClaimReward 幂等插入发奖记录。命中 uk(grant_idempotency_key)→ already=true(本名次已发过)。
	ClaimReward(ctx context.Context, rec *RewardLogRecord) (already bool, err error)
	// MarkReward 更新发奖状态(GRANTED / FAILED)。
	MarkReward(ctx context.Context, grantIdemKey string, status int8, updatedAtMs int64) error
	// ListUngrantedRewards 列出未发成的奖励(PENDING / FAILED)且 updated_at_ms < olderThanMs,
	// 供后台补发扫描重试(PENDING = 发奖中进程崩残留下;FAILED = inventory 拒绝/不可达)。
	// olderThanMs 把刚结算还在发的批次挡在扫描外,避免与同步发奖路径双重发起(Grant 幂等,重叠也不双发)。
	ListUngrantedRewards(ctx context.Context, olderThanMs int64, limit int) ([]RewardLogRecord, error)
}

// MySQLLeaderboardRepo 是基于 database/sql 的 LeaderboardRepo 实现(单库)。
type MySQLLeaderboardRepo struct {
	db *sql.DB
}

// NewMySQLLeaderboardRepo 构造。
func NewMySQLLeaderboardRepo(db *sql.DB) *MySQLLeaderboardRepo { return &MySQLLeaderboardRepo{db: db} }

const settlementCols = `settlement_id, board_type, scope, scope_id, period, top_n, settled_count, settle_idempotency_key, reset_after, created_at_ms`

func scanSettlement(row interface{ Scan(...any) error }) (*SettlementRecord, error) {
	var s SettlementRecord
	var reset int8
	if err := row.Scan(&s.SettlementID, &s.BoardType, &s.Scope, &s.ScopeID, &s.Period,
		&s.TopN, &s.SettledCount, &s.SettleIdemKey, &reset, &s.CreatedAtMs); err != nil {
		return nil, err
	}
	s.ResetAfter = reset != 0
	return &s, nil
}

func (r *MySQLLeaderboardRepo) ClaimSettlement(ctx context.Context, rec *SettlementRecord) (*SettlementRecord, bool, error) {
	reset := int8(0)
	if rec.ResetAfter {
		reset = 1
	}
	const ins = `INSERT INTO leaderboard_settlement (` + settlementCols + `) VALUES (?,?,?,?,?,?,?,?,?,?)`
	_, err := r.db.ExecContext(ctx, ins,
		rec.SettlementID, rec.BoardType, rec.Scope, rec.ScopeID, rec.Period,
		rec.TopN, rec.SettledCount, rec.SettleIdemKey, reset, rec.CreatedAtMs)
	if err == nil {
		return rec, false, nil
	}
	if !isDupErr(err) {
		return nil, false, errcode.New(errcode.ErrInternal, "insert settlement key=%s: %v", rec.SettleIdemKey, err)
	}
	const q = `SELECT ` + settlementCols + ` FROM leaderboard_settlement WHERE settle_idempotency_key = ? LIMIT 1`
	existing, serr := scanSettlement(r.db.QueryRowContext(ctx, q, rec.SettleIdemKey))
	if serr != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "read settlement key=%s: %v", rec.SettleIdemKey, serr)
	}
	return existing, true, nil
}

func (r *MySQLLeaderboardRepo) SaveSnapshot(ctx context.Context, settlementID uint64, rows []SnapshotRow) error {
	if len(rows) == 0 {
		return nil
	}
	query, args := buildSaveSnapshotSQL(settlementID, rows)
	if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
		return errcode.New(errcode.ErrInternal, "save snapshot settlement=%d: %v", settlementID, err)
	}
	return nil
}

func buildSaveSnapshotSQL(settlementID uint64, rows []SnapshotRow) (string, []any) {
	var sb strings.Builder
	sb.WriteString("INSERT IGNORE INTO leaderboard_snapshot (settlement_id, `rank`, entity_id, score, created_at_ms) VALUES ")
	args := make([]any, 0, len(rows)*5)
	for i, row := range rows {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?)")
		args = append(args, settlementID, row.Rank, row.EntityID, row.Score, row.CreatedAtMs)
	}
	return sb.String(), args
}

// LoadSnapshot 按 settlement_id 读快照,rank 升序。`rank` 是 MySQL 8 关键字,须反引号。
func (r *MySQLLeaderboardRepo) LoadSnapshot(ctx context.Context, settlementID uint64) ([]SnapshotRow, error) {
	const q = "SELECT `rank`, entity_id, score, created_at_ms FROM leaderboard_snapshot WHERE settlement_id = ? ORDER BY `rank` ASC"
	rows, err := r.db.QueryContext(ctx, q, settlementID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "load snapshot settlement=%d: %v", settlementID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []SnapshotRow
	for rows.Next() {
		var row SnapshotRow
		if err := rows.Scan(&row.Rank, &row.EntityID, &row.Score, &row.CreatedAtMs); err != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan snapshot settlement=%d: %v", settlementID, err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, errcode.New(errcode.ErrInternal, "iter snapshot settlement=%d: %v", settlementID, err)
	}
	return out, nil
}

func (r *MySQLLeaderboardRepo) ClaimReward(ctx context.Context, rec *RewardLogRecord) (bool, error) {
	const ins = "INSERT INTO leaderboard_reward_log (settlement_id, entity_id, `rank`, grant_idempotency_key, status, reward_pb, created_at_ms, updated_at_ms) VALUES (?,?,?,?,?,?,?,?)"
	_, err := r.db.ExecContext(ctx, ins,
		rec.SettlementID, rec.EntityID, rec.Rank, rec.GrantIdemKey, rec.Status, rec.RewardPayload, rec.CreatedAtMs, rec.UpdatedAtMs)
	if err == nil {
		return false, nil
	}
	if isDupErr(err) {
		return true, nil
	}
	return false, errcode.New(errcode.ErrInternal, "insert reward_log key=%s: %v", rec.GrantIdemKey, err)
}

func (r *MySQLLeaderboardRepo) MarkReward(ctx context.Context, grantIdemKey string, status int8, updatedAtMs int64) error {
	const upd = `UPDATE leaderboard_reward_log SET status = ?, updated_at_ms = ? WHERE grant_idempotency_key = ?`
	if _, err := r.db.ExecContext(ctx, upd, status, updatedAtMs, grantIdemKey); err != nil {
		return errcode.New(errcode.ErrInternal, "mark reward key=%s: %v", grantIdemKey, err)
	}
	return nil
}

func (r *MySQLLeaderboardRepo) ListUngrantedRewards(ctx context.Context, olderThanMs int64, limit int) ([]RewardLogRecord, error) {
	if limit <= 0 {
		limit = 100
	}
	const q = "SELECT settlement_id, entity_id, `rank`, grant_idempotency_key, status, reward_pb, created_at_ms, updated_at_ms" +
		" FROM leaderboard_reward_log WHERE status <> ? AND updated_at_ms < ? ORDER BY updated_at_ms ASC LIMIT ?"
	rows, err := r.db.QueryContext(ctx, q, RewardGranted, olderThanMs, limit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "list ungranted rewards: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var out []RewardLogRecord
	for rows.Next() {
		var rec RewardLogRecord
		if err := rows.Scan(&rec.SettlementID, &rec.EntityID, &rec.Rank, &rec.GrantIdemKey,
			&rec.Status, &rec.RewardPayload, &rec.CreatedAtMs, &rec.UpdatedAtMs); err != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan reward_log: %v", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, errcode.New(errcode.ErrInternal, "iter reward_log: %v", err)
	}
	return out, nil
}

// isDupErr 判断是否 MySQL 唯一键冲突(Error 1062)。
func isDupErr(err error) bool {
	if err == nil {
		return false
	}
	var me interface{ Error() string }
	if errors.As(err, &me) {
		return strings.Contains(me.Error(), "1062") || strings.Contains(me.Error(), "Duplicate entry")
	}
	return false
}

// ── 保留期清理(CLAUDE.md §9 不变量 24)────────────────────────────────────────
//
// leaderboard_snapshot / leaderboard_reward_log(GRANTED)随结算批次×名次数线性增长,
// 超保留期(默认 90 天)批删。leaderboard_settlement **故意不清**(每批次 1 行,慢增长):
// settle_idempotency_key 的 uk 是防重复结算的永久闸——若删除,超期后同 key 重放会被当
// 新结算重新发奖;保留它则重放 already=true + 快照已清 → 回放空 winners,fail-safe。
// reward_log 只清 GRANTED:PENDING/FAILED 是补发扫描工作集,陈年残留属告警问题不是增长问题。
// 多副本各自跑,DELETE 幂等无需锁;单批 limit 有界。

// SweepSnapshotsBefore 处理 created_at_ms 超保留期的名次快照。
// **mode 默认 ModeReportOnly:只统计待清理量并 WARN 告警,一行都不删**(用户指令)。
func (r *MySQLLeaderboardRepo) SweepSnapshotsBefore(ctx context.Context, mode dbguard.Mode, cutoffMs int64, limit int) (dbguard.Outcome, error) {
	out, err := dbguard.SweepTable(ctx, r.db, mode, "pandora_leaderboard", "leaderboard_snapshot",
		"created_at_ms < ?", limit, cutoffMs)
	if err != nil {
		return out, errcode.New(errcode.ErrInternal, "sweep snapshots: %v", err)
	}
	return out, nil
}

// SweepGrantedRewardsBefore 处理已发放(GRANTED)且 updated_at_ms 超保留期的发奖记录。
// **mode 默认 ModeReportOnly(只报告不删)**;PENDING/FAILED 是补发工作集,永不进入处理范围。
func (r *MySQLLeaderboardRepo) SweepGrantedRewardsBefore(ctx context.Context, mode dbguard.Mode, cutoffMs int64, limit int) (dbguard.Outcome, error) {
	out, err := dbguard.SweepTable(ctx, r.db, mode, "pandora_leaderboard", "leaderboard_reward_log",
		"status = ? AND updated_at_ms < ?", limit, RewardGranted, cutoffMs)
	if err != nil {
		return out, errcode.New(errcode.ErrInternal, "sweep granted rewards: %v", err)
	}
	return out, nil
}
