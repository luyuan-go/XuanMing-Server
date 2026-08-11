// mission_repo.go — 任务域 MySQL 仓储(pandora_mission;docs/design/mission.md §3)。
//
// 事务边界:MutatePlayer / ApplyFactsTx 一次领域操作一个事务——
// 先取该玩家守卫行点锁(玩家内串行)→ FOR UPDATE 载入该玩家全部活跃/完成行 → biz
// 引擎回调(纯函数)→ 突变 + 发奖流水 + 推送出箱同事务持久化。repo 不读配置表(规则全在 biz)。
//
// 为什么必须有守卫行:TiDB 悲观事务没有 gap/next-key 锁,
// `SELECT ... WHERE player_id=? FOR UPDATE` 只锁**已存在**的行 —— 玩家一条活跃任务
// 都没有时该语句一把锁都不加,两个并发 AcceptMission 各自读到空活跃集,双双通过
// max_active_missions 上限(§9.18)与 (type,sub_type) 类型互斥校验,然后各插一行。
// 已存在行的点锁两库语义一致,所以照 friend 域(R5 P1-2)的做法:临界区入口先
// `INSERT ... ON DUPLICATE KEY UPDATE pk=pk` 建/锁守卫行,锁持有到事务结束。
//
// 幂等:
//
//	mission_fact_receipts uk(player_id, idempotency_key) + 指纹比对(同键不同内容
//	  fail-closed,对齐 inventory claimLedger);
//	mission_reward_log uk(grant_idempotency_key) 撞键回填既有行 ID(重放安全)。
package data

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
	missionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/mission/v1"

	"github.com/luyuancpp/pandora/services/social/mission/internal/biz"
)

const dbLabel = "pandora_mission"

// progressPayloadLimit 进度 blob 写入侧字节闸(列 VARBINARY(256),§9.24 深度三上限之③;
// ①单槽 uint32 ②槽数 ≤8 由配置表校验兜)。
var progressPayloadLimit = dbguard.PayloadLimit{
	DB: dbLabel, Table: "player_mission_active", Column: "progress", Max: 256,
	Hint: "MissionProgressStorageRecord 超限:检查 MaxMissionConditionSlots 上限是否被绕过",
}

// rewardPayloadLimit 发奖快照写入侧字节闸(列 VARBINARY(2048))。
var rewardPayloadLimit = dbguard.PayloadLimit{
	DB: dbLabel, Table: "mission_reward_log", Column: "reward_pb", Max: 2048,
	Hint: "MissionRewardStorageRecord 超限:奖励表单行道具条目过多",
}

// pushPayloadLimit 推送出箱写入侧字节闸(列 VARBINARY(2048);biz 侧已按软上限分片)。
var pushPayloadLimit = dbguard.PayloadLimit{
	DB: dbLabel, Table: "mission_push_outbox", Column: "payload", Max: 2048,
	Hint: "MissionUpdateEvent 分片超限:检查 marshalEventChunks 分片粒度",
}

// MySQLMissionRepo 实现 biz.MissionRepo。
type MySQLMissionRepo struct {
	db *sql.DB
}

// NewMySQLMissionRepo 构造。
func NewMySQLMissionRepo(db *sql.DB) *MySQLMissionRepo { return &MySQLMissionRepo{db: db} }

var _ biz.MissionRepo = (*MySQLMissionRepo)(nil)

// MutatePlayer 见 biz.MissionRepo。
func (r *MySQLMissionRepo) MutatePlayer(ctx context.Context, playerID uint64, fn func(st *biz.PlayerState) (*biz.Mutation, error)) error {
	return r.inTx(ctx, func(tx *sql.Tx) error {
		if err := acquirePlayerGuard(ctx, tx, playerID); err != nil {
			return err
		}
		st, err := r.loadState(ctx, tx, playerID, true)
		if err != nil {
			return err
		}
		mut, err := fn(st)
		if err != nil {
			return err
		}
		return r.persist(ctx, tx, playerID, mut)
	})
}

// ApplyFactsTx 见 biz.MissionRepo。
func (r *MySQLMissionRepo) ApplyFactsTx(ctx context.Context, playerID uint64, idemKey string, fingerprint []byte, fn func(st *biz.PlayerState) (*biz.Mutation, error)) (bool, error) {
	already := false
	err := r.inTx(ctx, func(tx *sql.Tx) error {
		// 锁序纪律:守卫行**恒为第一把锁**(与 MutatePlayer 一致),两条路径不会互相等成环。
		if err := acquirePlayerGuard(ctx, tx, playerID); err != nil {
			return err
		}
		// 收据先行(同事务):撞 uk → 指纹比对——一致 = 纯重放幂等吸收,不一致 = 同键
		// 串改账 fail-closed(inventory claimLedger 同款,§16.2)。
		_, ierr := tx.ExecContext(ctx,
			"INSERT INTO mission_fact_receipts (player_id, idempotency_key, request_fingerprint) VALUES (?, ?, ?)",
			playerID, idemKey, fingerprint)
		if ierr != nil {
			if !isDupErr(ierr) {
				return fmt.Errorf("insert fact receipt: %w", ierr)
			}
			var existing []byte
			if serr := tx.QueryRowContext(ctx,
				"SELECT request_fingerprint FROM mission_fact_receipts WHERE player_id = ? AND idempotency_key = ?",
				playerID, idemKey).Scan(&existing); serr != nil {
				return fmt.Errorf("load fact receipt: %w", serr)
			}
			if !bytesEqual(existing, fingerprint) {
				return errcode.New(errcode.ErrMissionFactsConflict,
					"fact key reused with different content player=%d key=%s", playerID, idemKey)
			}
			already = true
			return nil // 空事务提交:无副作用
		}
		st, err := r.loadState(ctx, tx, playerID, true)
		if err != nil {
			return err
		}
		mut, err := fn(st)
		if err != nil {
			return err
		}
		return r.persist(ctx, tx, playerID, mut)
	})
	return already, err
}

// LoadPlayer 见 biz.MissionRepo(无锁读)。
func (r *MySQLMissionRepo) LoadPlayer(ctx context.Context, playerID uint64) (*biz.PlayerState, error) {
	return r.loadState(ctx, r.db, playerID, false)
}

// acquirePlayerGuard 取该玩家的守卫行悲观点锁,持有到事务结束。
//
// `INSERT ... ON DUPLICATE KEY UPDATE player_id = player_id` 一条语句同时完成
// 「不存在则建行、存在则锁行」(friend 域 acquirePlayerGuard 同款)。守卫行无业务
// 数据,只是锁载体;每玩家至多 1 行,被玩家数有界(§9.24 登记豁免)。
func acquirePlayerGuard(ctx context.Context, tx *sql.Tx, playerID uint64) error {
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO mission_player_guards (player_id) VALUES (?)
ON DUPLICATE KEY UPDATE player_id = player_id`, playerID); err != nil {
		return errcode.NewCause(errcode.ErrInternal, err, "acquire mission player guard %d", playerID)
	}
	return nil
}

// ── 状态载入 / 持久化 ───────────────────────────────────────────────────────

// queryer 兼容 *sql.DB 与 *sql.Tx。
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (r *MySQLMissionRepo) loadState(ctx context.Context, q queryer, playerID uint64, forUpdate bool) (*biz.PlayerState, error) {
	lock := ""
	if forUpdate {
		lock = " FOR UPDATE"
	}
	st := &biz.PlayerState{
		PlayerID: playerID,
		Active:   make(map[uint32]*biz.ActiveMission),
		Done:     make(map[uint32]*biz.DoneMission),
	}

	rows, err := q.QueryContext(ctx,
		"SELECT mission_config_id, progress, accepted_at_ms FROM player_mission_active WHERE player_id = ?"+lock,
		playerID)
	if err != nil {
		return nil, fmt.Errorf("load active: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			mid      uint32
			blob     []byte
			accepted int64
		)
		if err := rows.Scan(&mid, &blob, &accepted); err != nil {
			return nil, fmt.Errorf("scan active: %w", err)
		}
		record := &missionv1.MissionProgressStorageRecord{}
		if err := proto.Unmarshal(blob, record); err != nil {
			// 坏行 fail-closed:静默清零会让玩家进度凭空回退(§16 不吞错)。
			return nil, fmt.Errorf("progress blob 解码失败 player=%d mission=%d: %w", playerID, mid, err)
		}
		st.Active[mid] = &biz.ActiveMission{
			MissionConfigID: mid,
			Progress:        record.GetProgress(),
			AcceptedAtMs:    accepted,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iter active: %w", err)
	}

	drows, err := q.QueryContext(ctx,
		"SELECT mission_config_id, reward_state, completed_at_ms FROM player_mission_done WHERE player_id = ?"+lock,
		playerID)
	if err != nil {
		return nil, fmt.Errorf("load done: %w", err)
	}
	defer drows.Close()
	for drows.Next() {
		var (
			mid       uint32
			state     uint32
			completed int64
		)
		if err := drows.Scan(&mid, &state, &completed); err != nil {
			return nil, fmt.Errorf("scan done: %w", err)
		}
		st.Done[mid] = &biz.DoneMission{MissionConfigID: mid, RewardState: state, CompletedAtMs: completed}
	}
	if err := drows.Err(); err != nil {
		return nil, fmt.Errorf("iter done: %w", err)
	}
	return st, nil
}

func (r *MySQLMissionRepo) persist(ctx context.Context, tx *sql.Tx, playerID uint64, mut *biz.Mutation) error {
	if mut == nil {
		return nil
	}
	nowMs := time.Now().UnixMilli()

	for _, am := range mut.UpsertActive {
		blob, err := proto.Marshal(&missionv1.MissionProgressStorageRecord{Progress: am.Progress})
		if err != nil {
			return errcode.NewCause(errcode.ErrInternal, err, "marshal progress mission=%d", am.MissionConfigID)
		}
		if err := dbguard.CheckPayload(ctx, progressPayloadLimit, blob); err != nil {
			return errcode.NewCause(errcode.ErrInternal, err, "progress payload mission=%d", am.MissionConfigID)
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO player_mission_active (player_id, mission_config_id, progress, accepted_at_ms) VALUES (?, ?, ?, ?) "+
				"ON DUPLICATE KEY UPDATE progress = VALUES(progress)",
			playerID, am.MissionConfigID, blob, am.AcceptedAtMs); err != nil {
			return fmt.Errorf("upsert active mission=%d: %w", am.MissionConfigID, err)
		}
	}

	for _, mid := range mut.DeleteActive {
		if _, err := tx.ExecContext(ctx,
			"DELETE FROM player_mission_active WHERE player_id = ? AND mission_config_id = ?",
			playerID, mid); err != nil {
			return fmt.Errorf("delete active mission=%d: %w", mid, err)
		}
	}

	for _, dm := range mut.InsertDone {
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO player_mission_done (player_id, mission_config_id, reward_state, completed_at_ms) VALUES (?, ?, ?, ?)",
			playerID, dm.MissionConfigID, dm.RewardState, dm.CompletedAtMs); err != nil {
			// uk 撞行 = 引擎不变量被破坏(接取校验挡了已完成任务),fail-closed 整事务回滚。
			return fmt.Errorf("insert done mission=%d: %w", dm.MissionConfigID, err)
		}
	}

	for _, mid := range mut.ClaimDone {
		res, err := tx.ExecContext(ctx,
			"UPDATE player_mission_done SET reward_state = ? WHERE player_id = ? AND mission_config_id = ? AND reward_state = ?",
			biz.RewardStateClaimed, playerID, mid, biz.RewardStateClaimable)
		if err != nil {
			return fmt.Errorf("claim done mission=%d: %w", mid, err)
		}
		// FOR UPDATE 下不该出现;条件更新兜底(§16.1 TOCTOU 双保险)。
		if n, _ := res.RowsAffected(); n == 0 {
			return errcode.New(errcode.ErrMissionNotClaimable, "claim cas miss mission=%d player=%d", mid, playerID)
		}
	}

	for _, entry := range mut.RewardLogs {
		if err := dbguard.CheckPayload(ctx, rewardPayloadLimit, entry.RewardPB); err != nil {
			return errcode.NewCause(errcode.ErrInternal, err, "reward payload mission=%d", entry.MissionConfigID)
		}
		res, err := tx.ExecContext(ctx,
			"INSERT INTO mission_reward_log (player_id, mission_config_id, grant_idempotency_key, status, reward_pb, created_at_ms, updated_at_ms) "+
				"VALUES (?, ?, ?, 0, ?, ?, ?)",
			playerID, entry.MissionConfigID, entry.Key, entry.RewardPB, nowMs, nowMs)
		if err != nil {
			if !isDupErr(err) {
				return fmt.Errorf("insert reward log mission=%d: %w", entry.MissionConfigID, err)
			}
			// 撞 uk_grant_idem = 该任务的发奖流水已存在(历史重放),回填既有行 ID,
			// 由补扫按其真实状态处置(GRANTED 行不会被重发)。
			if serr := tx.QueryRowContext(ctx,
				"SELECT id FROM mission_reward_log WHERE grant_idempotency_key = ?", entry.Key).Scan(&entry.ID); serr != nil {
				return fmt.Errorf("load reward log by key: %w", serr)
			}
			continue
		}
		id, lerr := res.LastInsertId()
		if lerr != nil {
			return fmt.Errorf("reward log insert id: %w", lerr)
		}
		entry.ID = uint64(id)
	}

	for _, payload := range mut.PushPayloads {
		if err := dbguard.CheckPayload(ctx, pushPayloadLimit, payload); err != nil {
			return errcode.NewCause(errcode.ErrInternal, err, "push payload")
		}
		if _, err := tx.ExecContext(ctx,
			"INSERT INTO mission_push_outbox (player_id, payload, created_at_ms) VALUES (?, ?, ?)",
			playerID, payload, nowMs); err != nil {
			return fmt.Errorf("insert push outbox: %w", err)
		}
	}
	return nil
}

// ── 发奖补扫工作集 ─────────────────────────────────────────────────────────

// ListUngrantedRewards 见 biz.MissionRepo(status<>GRANTED 且早于 grace;按 id 序)。
func (r *MySQLMissionRepo) ListUngrantedRewards(ctx context.Context, olderThanMs int64, limit int) ([]*biz.RewardLogRow, error) {
	rows, err := r.db.QueryContext(ctx,
		"SELECT id, player_id, mission_config_id, grant_idempotency_key, reward_pb FROM mission_reward_log "+
			"WHERE status <> 1 AND updated_at_ms < ? ORDER BY id LIMIT ?",
		olderThanMs, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*biz.RewardLogRow
	for rows.Next() {
		row := &biz.RewardLogRow{}
		if err := rows.Scan(&row.ID, &row.PlayerID, &row.MissionConfigID, &row.Key, &row.RewardPB); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// MarkReward 见 biz.MissionRepo(1 GRANTED / 2 FAILED)。
func (r *MySQLMissionRepo) MarkReward(ctx context.Context, id uint64, granted bool, nowMs int64) error {
	status := 2
	if granted {
		status = 1
	}
	_, err := r.db.ExecContext(ctx,
		"UPDATE mission_reward_log SET status = ?, updated_at_ms = ? WHERE id = ?",
		status, nowMs, id)
	return err
}

// ── 推送出箱 ───────────────────────────────────────────────────────────────

// FetchPushOutbox 见 biz.MissionRepo(FIFO 按 id 序)。
func (r *MySQLMissionRepo) FetchPushOutbox(ctx context.Context, limit int) ([]*biz.PushOutboxRow, error) {
	rows, err := r.db.QueryContext(ctx,
		"SELECT id, player_id, payload FROM mission_push_outbox ORDER BY id LIMIT ?", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*biz.PushOutboxRow
	for rows.Next() {
		row := &biz.PushOutboxRow{}
		if err := rows.Scan(&row.ID, &row.PlayerID, &row.Payload); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// DeletePushOutbox 见 biz.MissionRepo。
func (r *MySQLMissionRepo) DeletePushOutbox(ctx context.Context, id uint64) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM mission_push_outbox WHERE id = ?", id)
	return err
}

// ── 保留期清理(§9.24)──────────────────────────────────────────────────────

// SweepRewardLog 见 biz.MissionRepo:只清 GRANTED 且超期;PENDING/FAILED 永不清。
func (r *MySQLMissionRepo) SweepRewardLog(ctx context.Context, modeRaw string, retentionDays, batch int) error {
	cutoff := time.Now().AddDate(0, 0, -retentionDays).UnixMilli()
	_, err := dbguard.SweepTable(ctx, r.db, parseMode(modeRaw), dbLabel, "mission_reward_log",
		"status = 1 AND updated_at_ms < ?", batch, cutoff)
	return err
}

// SweepReceipts 见 biz.MissionRepo(组级闸在 biz;这里只按模式执行)。
func (r *MySQLMissionRepo) SweepReceipts(ctx context.Context, modeRaw string, retentionDays, batch int) error {
	_, err := dbguard.SweepTable(ctx, r.db, parseMode(modeRaw), dbLabel, "mission_fact_receipts",
		"created_at < DATE_SUB(NOW(), INTERVAL ? DAY)", batch, retentionDays)
	return err
}

// ── 事务与杂项 ─────────────────────────────────────────────────────────────

func (r *MySQLMissionRepo) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// parseMode 启动期已 fail-fast 校验过;这里兜底回 report_only(绝不猜成 delete)。
func parseMode(raw string) dbguard.Mode {
	m, err := dbguard.ParseMode(raw)
	if err != nil {
		return dbguard.ModeReportOnly
	}
	return m
}

// isDupErr 判定 MySQL 1062 唯一键冲突(对齐 inventory_repo)。
func isDupErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Error 1062")
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
