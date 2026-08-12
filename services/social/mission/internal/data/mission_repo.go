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
	"errors"
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

// errPushOutboxRaced 见 DeletePushOutbox:删行命中 0 行 = 另一副本已抢先投递并删除。
var errPushOutboxRaced = errors.New("mission_push_outbox 行已被其它副本删除(多副本并发发布,推送可能乱序)")

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
	// doneReadLimit 只读路径完成集截断行数;0 = 用包级默认 doneReadLimit。
	// 只为测试可注入(真造 2000 行太慢),生产恒走默认值。
	doneReadLimitOverride int
}

// NewMySQLMissionRepo 构造。
func NewMySQLMissionRepo(db *sql.DB) *MySQLMissionRepo { return &MySQLMissionRepo{db: db} }

// effectiveDoneReadLimit 返回生效的只读截断行数。
func (r *MySQLMissionRepo) effectiveDoneReadLimit() int {
	if r.doneReadLimitOverride > 0 {
		return r.doneReadLimitOverride
	}
	return doneReadLimit
}

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

// doneReadLimit 是**只读路径**(LoadPlayer → ListMissions)对完成集的单次返回上限
// (§9.18 读取侧上限)。
//
// 为什么只加在只读路径:事务路径(MutatePlayer / ApplyFactsTx)用 st.Done 判「已完成
// 不可重复接取」与领奖 CAS —— 那里一旦截断,超出截断窗口的已完成任务会被判成可重新
// 接取,**把一个展示问题升级成重复发奖**。所以事务路径刻意保持全量,由
// configtable.MaxMissionRows 的写入侧硬上限兜住规模(§9.18 要求写入侧上限与读取侧
// 上限同时存在,不是二选一)。
const doneReadLimit = 2000

func (r *MySQLMissionRepo) loadState(ctx context.Context, q queryer, playerID uint64, forUpdate bool) (*biz.PlayerState, error) {
	lock := ""
	doneLimit := ""
	if forUpdate {
		lock = " FOR UPDATE"
	} else {
		// 只读路径按 mission_config_id 稳定序截断,与 biz 侧 sortedDone 同序。
		doneLimit = fmt.Sprintf(" ORDER BY mission_config_id LIMIT %d", r.effectiveDoneReadLimit())
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
		"SELECT mission_config_id, reward_state, completed_at_ms FROM player_mission_done WHERE player_id = ?"+doneLimit+lock,
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
//
// **GRANTED 是终态,任何副本都不得把它改回 FAILED**(§16.1/§16.4 多副本)。多副本补扫是
// 刻意允许的(正确性由下游幂等键保证,不引入 claim/lease —— §15.3 不为此加一套锁),
// 但那意味着两个副本可能同时处理同一行:A 发放成功正要写 GRANTED,B 因下游瞬时不可用
// 写 FAILED。若无条件覆盖,已发放的行会被打回补发工作集,然后:
//   - 每轮补扫都重放它(下游幂等键吸收,但白烧配额与日志);
//   - `status <> 1 AND updated_at_ms 超期` 的行永不收敛,"陈年 FAILED = 发放链有 bug"
//     这个审计信号被噪声淹没;
//   - 保留期把下游幂等记录清掉之后(90 天),再一次重放就是**真的重复发放**。
//
// 因此失败标记带 `status <> 1` 条件更新;成功标记无条件(终态推进,重复写同值幂等)。
// 命中 0 行是正常并发结果(行已被别的副本置 GRANTED),不算错误。
func (r *MySQLMissionRepo) MarkReward(ctx context.Context, id uint64, granted bool, nowMs int64) error {
	if granted {
		_, err := r.db.ExecContext(ctx,
			"UPDATE mission_reward_log SET status = 1, updated_at_ms = ? WHERE id = ?",
			nowMs, id)
		return err
	}
	_, err := r.db.ExecContext(ctx,
		"UPDATE mission_reward_log SET status = 2, updated_at_ms = ? WHERE id = ? AND status <> 1",
		nowMs, id)
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
//
// 命中 0 行 = **另一个副本已经投过并删掉了这一行**,也就是两个 RunPushPublisher 正在
// 同一张出箱表上打架:两边各自持有一份内存快照,投递顺序会交错,同玩家的旧进度快照
// 可能在新快照之后到达客户端(progressed 是逐任务全量快照,后到即覆盖)。
// 旧实现丢弃 Result,这件事在日志与 metric 里完全不可见 —— 先让它可见,
// 这也是验证「单写者是否真的生效」的唯一手段。
func (r *MySQLMissionRepo) DeletePushOutbox(ctx context.Context, id uint64) error {
	res, err := r.db.ExecContext(ctx, "DELETE FROM mission_push_outbox WHERE id = ?", id)
	if err != nil {
		return err
	}
	if n, aerr := res.RowsAffected(); aerr == nil && n == 0 {
		return errPushOutboxRaced
	}
	return nil
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

// inTx 是任务域所有写事务的唯一入口。
//
// **显式 READ COMMITTED 是正确性要求,不是调优**(2026-08-11 真 MySQL 8.4 实测抓获;
// friend 域同因同批修,login/register_no.go 有更早的同款先例)。
//
// 症状:24 个**不同玩家**并发 Accept(彼此不共享任何守卫行、任何业务行)必炸 1213,
// 报在 `upsert active mission=...`。根因不是锁序 —— 守卫行确实是本事务第一把锁,
// 而是 RR 下 `loadState(forUpdate=true)` 对**零行**取的是「键所在的间隙」而非某一行:
// 表空时全部 player_id 落进 player_mission_active 主键的同一个 supremum 间隙,
// N 个事务各自拿到相容的间隙锁,随后各自的 INSERT 都要 insert intention → 互相挡成环。
// 玩家彼此无关却互相打死,且并发越高越必然。
//
// 为什么降到 RC 安全:本域的并发正确性**从设计之初就不依赖 gap 锁** —— 守卫行
// (mission_player_guards)存在的理由正是「TiDB 没有 gap 锁,零行 FOR UPDATE 一把锁都不加」
// (见 acquirePlayerGuard 注释)。限额与类型互斥的权威性来自守卫行 + 守卫锁内的锁定读,
// 幂等来自 mission_fact_receipts 的唯一键,这三者在 RC 下全部成立。RR 的 gap 锁在 MySQL
// 侧是纯副作用,只贡献死锁;RC 的锁定读还更"当前"(总读最新已提交)。
//
// 回归钉在 mission_guard_lock_order_mysql_test.go(改回 nil 即必红)。
// 前置:binlog_format=ROW(MySQL 8.4 默认)。
func (r *MySQLMissionRepo) inTx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
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
