// Package data — 背包域数据层(pandora_bag 库,bag-domain.md §4;phase 1 由 inventory 进程承载)。
//
// 库表(deploy/mysql-init/14-bag-tables.sql):
//
//	bag_meta        每玩家 fencing 锚点(owner_epoch 单调 CAS + last_journal_seq 水位)
//	bag_checkpoint  随身组快照(pb BagStorageRecord blob)
//	bag_section     后端驻留段本体(仓库/活动段;pb BagSection blob,与 journal 同事务变更)
//	bag_journal     背包流水(uk player+seq / uk player+idem 双去重;fingerprint 防 key 复用)
//	bag_generation  活动段代际权威
//
// 一致性(五要件,CLAUDE.md §9.6):
//   - 每个写事务先 SELECT ... FOR UPDATE 锁 bag_meta 行:owner_epoch 单调 CAS(旧 epoch 拒,
//     新 epoch 推进),同时天然串行化同一玩家的全部背包写(§9.22 禁"先查再存");
//   - 活动段写校验 bag_generation.current_generation,不符 fail-closed 整批拒;
//   - journal 前缀确认:批内 seq 升序,<= 水位的条目视为重放跳过,应用后推进水位;
//   - 涉及后端驻留段的 op 在同一事务里改 bag_section(转移/领取/使用零撕裂)。
package data

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
)

// BagOpType 是 bag_journal.op_type 的取值(对齐 bag.proto oneof 分支)。
const (
	BagOpPickupGrant int8 = 1
	BagOpMailClaim   int8 = 2
	BagOpTransfer    int8 = 3
	BagOpConsume     int8 = 4
)

// BagWarehouseType 仓库段类型;>= BagActivityTypeBase 为活动段(bag-domain.md §5)。
const (
	BagWarehouseType    uint32 = 1
	BagActivityTypeBase uint32 = 100
)

// IsBackendResidentBagType 判断后端驻留段(仓库/活动段;随身组 0/2/3 不落 bag_section)。
func IsBackendResidentBagType(bagType uint32) bool {
	return bagType == BagWarehouseType || bagType >= BagActivityTypeBase
}

// IsActivityBagType 判断活动段(代际语义仅活动段生效)。
func IsActivityBagType(bagType uint32) bool { return bagType >= BagActivityTypeBase }

// BagJournalRow 是一条已落库流水(LoadBag 尾部重放用;payload 为 BagJournalEntry 原文)。
type BagJournalRow struct {
	JournalSeq uint64
	Payload    []byte
}

// BagSectionCapacity 后端驻留段容量查询回调(biz 注入配置;0 = 未配置,fail-closed 拒写)。
type BagSectionCapacity func(bagType uint32) uint32

// BagMaxStack 可堆叠道具单格堆叠上限查询回调(biz 注入配置;0 = 未配置,fail-closed 拒写)。
// 后端驻留段由服务端权威拆堆(2026-07-22 拍板,bag-domain.md §5.2):同 config 按上限分格,
// 容量按拆堆后的格子数校验;客户端对后端段只读渲染,不得本地重算堆叠。
// 随身组(0/2/3)的堆叠权威在 owner DS(FMyBag),本回调不涉及。
type BagMaxStack func(itemConfigID uint32) uint32

// BagRepo 是背包域数据层抽象。biz 只依赖此接口。
type BagRepo interface {
	// LoadBag 加载随身组:epoch CAS(checkout 推进)+ checkpoint 快照 + covered 之后的
	// journal 尾部 + 权威水位。新玩家返回空快照、水位 0。
	LoadBag(ctx context.Context, playerID, ownerEpoch uint64) (snapshot []byte, tail []BagJournalRow, lastSeq uint64, err error)

	// AppendJournal 追加一批流水(单事务):epoch CAS + generation 校验 + 幂等去重 +
	// 后端驻留段同事务变更 + 水位推进。返回已应用水位(含本批;纯重放返回当前水位)。
	AppendJournal(ctx context.Context, playerID, ownerEpoch uint64, entries []*bagv1.BagJournalEntry, capacity BagSectionCapacity, maxStack BagMaxStack, hourlyQuota int64) (ackedSeq uint64, err error)

	// SaveCheckpoint 保存随身组快照:epoch CAS;coveredSeq 不得回退、不得超已确认水位。
	SaveCheckpoint(ctx context.Context, playerID, ownerEpoch uint64, snapshot []byte, coveredSeq uint64) error

	// GetSections 读后端驻留段(活动段按 current generation 过滤;无行返回空段)。
	// 返回的 Capacity 为有效容量(base + 已购增量,§5.3)。
	GetSections(ctx context.Context, playerID uint64, bagTypes []uint32, capacity BagSectionCapacity) ([]*bagv1.BagSection, error)

	// SweepJournal 删除超过保留期的流水(有界批量;返回删除行数)。
	SweepJournal(ctx context.Context, retention time.Duration, batch int) (int64, error)

	// ApplyCapacityPurchase 容量购买落位(§5.3 两步 saga 第②步;档数 CAS 幂等)。
	ApplyCapacityPurchase(ctx context.Context, playerID uint64, bagType, tier, slots, maxExtra uint32) (extra, purchases uint32, applied bool, err error)

	// GetCapacityState 读某段已购状态(定档 / 展示;无行 = 0/0)。
	GetCapacityState(ctx context.Context, playerID uint64, bagType uint32) (extra, purchases uint32, err error)
}

// MySQLBagRepo 是基于 database/sql 的 BagRepo 实现(连 pandora_bag 库)。
type MySQLBagRepo struct {
	db *sql.DB
}

// NewMySQLBagRepo 构造。db 由 pkg/mysqlx.MustNewClient 提供(连 pandora_bag 库)。
func NewMySQLBagRepo(db *sql.DB) *MySQLBagRepo {
	return &MySQLBagRepo{db: db}
}

// lockBagMetaTx 在事务里确保并锁定 bag_meta 行,执行 owner_epoch 单调 CAS。
//   - reqEpoch < 存量 → ErrBagEpochFenced(失租旧写)
//   - reqEpoch > 存量 → 推进(新 owner checkout / 首写)
//
// 返回当前 last_journal_seq 水位。锁行同时串行化同一玩家的全部背包写。
func lockBagMetaTx(ctx context.Context, tx *sql.Tx, playerID, reqEpoch uint64) (lastSeq uint64, err error) {
	const ins = `INSERT IGNORE INTO bag_meta (player_id, owner_epoch, last_journal_seq) VALUES (?, 0, 0)`
	if _, ierr := tx.ExecContext(ctx, ins, playerID); ierr != nil {
		return 0, errcode.New(errcode.ErrInternal, "ensure bag_meta player=%d: %v", playerID, ierr)
	}
	var storedEpoch uint64
	qerr := tx.QueryRowContext(ctx,
		`SELECT owner_epoch, last_journal_seq FROM bag_meta WHERE player_id = ? FOR UPDATE`,
		playerID).Scan(&storedEpoch, &lastSeq)
	if qerr != nil {
		return 0, errcode.New(errcode.ErrInternal, "lock bag_meta player=%d: %v", playerID, qerr)
	}
	if reqEpoch < storedEpoch {
		// 失租旧 DS 的迟到写在存储侧 owner_epoch 单调 CAS 被拒(§9.22 fencing / 脑裂防线)。
		// ErrBagEpochFenced 是业务码,不被 access log 中间件当故障 → 在此显式 WARN 留证。
		plog.With(ctx).Warnw("msg", "bag_owner_epoch_fenced",
			"player_id", playerID, "req_epoch", reqEpoch, "current_epoch", storedEpoch)
		return 0, errcode.New(errcode.ErrBagEpochFenced,
			"stale owner epoch player=%d req=%d current=%d", playerID, reqEpoch, storedEpoch)
	}
	if reqEpoch > storedEpoch {
		if _, uerr := tx.ExecContext(ctx,
			`UPDATE bag_meta SET owner_epoch = ? WHERE player_id = ?`, reqEpoch, playerID); uerr != nil {
			return 0, errcode.New(errcode.ErrInternal, "advance bag epoch player=%d: %v", playerID, uerr)
		}
	}
	return lastSeq, nil
}

func (r *MySQLBagRepo) LoadBag(ctx context.Context, playerID, ownerEpoch uint64) ([]byte, []BagJournalRow, uint64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, 0, errcode.New(errcode.ErrInternal, "begin load tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	lastSeq, lerr := lockBagMetaTx(ctx, tx, playerID, ownerEpoch)
	if lerr != nil {
		return nil, nil, 0, lerr
	}

	var (
		snapshot   []byte
		coveredSeq uint64
	)
	cerr := tx.QueryRowContext(ctx,
		`SELECT snapshot, covered_journal_seq FROM bag_checkpoint WHERE player_id = ?`,
		playerID).Scan(&snapshot, &coveredSeq)
	if cerr != nil && !errors.Is(cerr, sql.ErrNoRows) {
		return nil, nil, 0, errcode.New(errcode.ErrInternal, "read checkpoint player=%d: %v", playerID, cerr)
	}

	rows, qerr := tx.QueryContext(ctx,
		`SELECT journal_seq, payload FROM bag_journal WHERE player_id = ? AND journal_seq > ? ORDER BY journal_seq`,
		playerID, coveredSeq)
	if qerr != nil {
		return nil, nil, 0, errcode.New(errcode.ErrInternal, "query journal tail player=%d: %v", playerID, qerr)
	}
	defer func() { _ = rows.Close() }()
	var tail []BagJournalRow
	for rows.Next() {
		var row BagJournalRow
		if serr := rows.Scan(&row.JournalSeq, &row.Payload); serr != nil {
			return nil, nil, 0, errcode.New(errcode.ErrInternal, "scan journal tail player=%d: %v", playerID, serr)
		}
		tail = append(tail, row)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, nil, 0, errcode.New(errcode.ErrInternal, "iterate journal tail player=%d: %v", playerID, rerr)
	}

	// 尾部连续性校验(INC-20260722-003 fail-closed):恢复 = 快照 + (covered, last] 连续
	// 重放,journal_seq 每玩家单调连续。任何缺口(误删 / 损坏 / 越权清理)都意味着
	// 加载会静默少资产:必须拒绝加载并把缺口暴露给告警排查,绝不静默继续。
	expect := coveredSeq
	for _, row := range tail {
		expect++
		if row.JournalSeq != expect {
			// journal 缺口 = 加载会静默少资产(误删 / 损坏 / 越权清理),事故级(INC-20260722-003)。
			// 这里已 fail-closed 拒绝有损加载;独立 ERROR 带完整 seq 便于告警与取证(§16.9)。
			plog.With(ctx).Errorw("msg", "bag_journal_gap",
				"player_id", playerID, "expect_seq", expect, "got_seq", row.JournalSeq,
				"covered_seq", coveredSeq, "last_seq", lastSeq)
			return nil, nil, 0, errcode.New(errcode.ErrInternal,
				"bag journal tail gap player=%d: expect seq %d got %d (covered=%d last=%d), refusing lossy load",
				playerID, expect, row.JournalSeq, coveredSeq, lastSeq)
		}
	}
	if expect != lastSeq {
		plog.With(ctx).Errorw("msg", "bag_journal_truncated",
			"player_id", playerID, "tail_end_seq", expect, "watermark_seq", lastSeq, "covered_seq", coveredSeq)
		return nil, nil, 0, errcode.New(errcode.ErrInternal,
			"bag journal tail truncated player=%d: tail ends at %d but watermark %d (covered=%d), refusing lossy load",
			playerID, expect, lastSeq, coveredSeq)
	}

	if cerr := tx.Commit(); cerr != nil {
		return nil, nil, 0, errcode.New(errcode.ErrInternal, "commit load player=%d: %v", playerID, cerr)
	}
	return snapshot, tail, lastSeq, nil
}

// bagEntryFingerprint 计算流水内容指纹(payload 原文 sha256;同 key 不同内容 → 幂等冲突)。
func bagEntryFingerprint(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (r *MySQLBagRepo) AppendJournal(ctx context.Context, playerID, ownerEpoch uint64, entries []*bagv1.BagJournalEntry, capacity BagSectionCapacity, maxStack BagMaxStack, hourlyQuota int64) (uint64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "begin append tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	lastSeq, lerr := lockBagMetaTx(ctx, tx, playerID, ownerEpoch)
	if lerr != nil {
		return 0, lerr
	}

	// 有效容量(§5.3):事务内预取本批触及后端段的已购增量,base + extra 作判定容量
	// (判定与权威同址;bag_capacity 写路径同样先锁 bag_meta 行,天然串行无脏读)。
	extras := map[uint32]uint32{}
	for _, bagType := range collectBackendBagTypes(entries) {
		extra, xerr := readCapacityExtraTx(ctx, tx, playerID, bagType)
		if xerr != nil {
			return 0, xerr
		}
		if extra > 0 {
			extras[bagType] = extra
		}
	}
	capacity = effectiveCapacityFn(capacity, extras)

	// 额度(五要件④):单玩家滑窗流水条数封顶,压缩单实例被攻破/出 bug 的爆炸半径。
	if hourlyQuota > 0 {
		var recent int64
		if qerr := tx.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM bag_journal WHERE player_id = ? AND created_at > (NOW() - INTERVAL 1 HOUR)`,
			playerID).Scan(&recent); qerr != nil {
			return 0, errcode.New(errcode.ErrInternal, "count journal quota player=%d: %v", playerID, qerr)
		}
		if recent+int64(len(entries)) > hourlyQuota {
			// 额度封顶触发(五要件④限流):某玩家 / DS 在猛刷背包写路径,应能主动发现。
			plog.With(ctx).Warnw("msg", "bag_journal_quota_exceeded",
				"player_id", playerID, "recent", recent, "batch", len(entries), "quota", hourlyQuota)
			return 0, errcode.New(errcode.ErrBagQuotaExceeded,
				"journal hourly quota exceeded player=%d recent=%d batch=%d quota=%d",
				playerID, recent, len(entries), hourlyQuota)
		}
	}

	// 代际权威一次性读取(事务内一致视图);仅活动段需要。
	generations := map[uint32]uint64{}
	loadGeneration := func(bagType uint32) (uint64, error) {
		if g, ok := generations[bagType]; ok {
			return g, nil
		}
		var current uint64
		gerr := tx.QueryRowContext(ctx,
			`SELECT current_generation FROM bag_generation WHERE bag_type = ?`, bagType).Scan(&current)
		if errors.Is(gerr, sql.ErrNoRows) {
			current = 0 // 未登记的活动段:代际 0(活动开启前运营必须先写 bag_generation)
		} else if gerr != nil {
			return 0, errcode.New(errcode.ErrInternal, "read generation bag_type=%d: %v", bagType, gerr)
		}
		generations[bagType] = current
		return current, nil
	}

	// 后端驻留段在事务内的工作副本(同一批多条 op 触同段时读改写复用,提交前统一落库)。
	sections := map[uint32]*bagv1.BagSection{}
	dirty := map[uint32]bool{}
	loadSection := func(bagType uint32) (*bagv1.BagSection, error) {
		if sec, ok := sections[bagType]; ok {
			return sec, nil
		}
		gen, gerr := loadGeneration(bagType)
		if gerr != nil {
			return nil, gerr
		}
		sec := &bagv1.BagSection{BagType: bagType, Generation: gen}
		var (
			blob   []byte
			rowGen uint64
		)
		serr := tx.QueryRowContext(ctx,
			`SELECT generation, section FROM bag_section WHERE player_id = ? AND bag_type = ? FOR UPDATE`,
			playerID, bagType).Scan(&rowGen, &blob)
		if serr != nil && !errors.Is(serr, sql.ErrNoRows) {
			return nil, errcode.New(errcode.ErrInternal, "lock section player=%d bag=%d: %v", playerID, bagType, serr)
		}
		// 旧代行按"已逻辑清空"处理:从空段开始(物理回收交给后台 sweep,读写都不认旧代)。
		if serr == nil && (!IsActivityBagType(bagType) || rowGen == gen) {
			if uerr := proto.Unmarshal(blob, sec); uerr != nil {
				return nil, errcode.New(errcode.ErrInternal, "decode section player=%d bag=%d: %v", playerID, bagType, uerr)
			}
			sec.BagType = bagType
			sec.Generation = gen
		}
		sections[bagType] = sec
		return sec, nil
	}

	var (
		newSeq  = lastSeq
		prevSeq uint64
		applied int
	)
	for _, entry := range entries {
		seq := entry.GetJournalSeq()
		if seq == 0 || seq <= prevSeq {
			return 0, errcode.New(errcode.ErrBagSeqConflict,
				"journal seq must be ascending player=%d seq=%d prev=%d", playerID, seq, prevSeq)
		}
		prevSeq = seq
		if seq <= lastSeq {
			continue // 旧条目重放(at-least-once),已应用,跳过。
		}

		payload, merr := proto.Marshal(entry)
		if merr != nil {
			return 0, errcode.New(errcode.ErrInternal, "marshal journal entry player=%d seq=%d: %v", playerID, seq, merr)
		}
		fp := bagEntryFingerprint(payload)

		// 活动段代际校验(fail-closed):切代后迟到写整批拒,旧物品不可能漏进新代。
		if IsActivityBagType(entry.GetBagType()) {
			current, gerr := loadGeneration(entry.GetBagType())
			if gerr != nil {
				return 0, gerr
			}
			if entry.GetGeneration() != current {
				return 0, errcode.New(errcode.ErrBagGenerationMismatch,
					"generation mismatch player=%d bag=%d entry_gen=%d current=%d",
					playerID, entry.GetBagType(), entry.GetGeneration(), current)
			}
		}

		opType, aerr := applyBagOpTx(entry, loadSection, dirty, capacity, maxStack)
		if aerr != nil {
			return 0, aerr
		}

		const ins = `INSERT INTO bag_journal
    (player_id, journal_seq, owner_epoch, op_type, bag_type, generation, payload, idempotency_key, fingerprint)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
		if _, ierr := tx.ExecContext(ctx, ins, playerID, seq, ownerEpoch, opType,
			entry.GetBagType(), entry.GetGeneration(), payload, entry.GetIdempotencyKey(), fp); ierr != nil {
			if !isDupErr(ierr) {
				return 0, errcode.New(errcode.ErrInternal, "insert journal player=%d seq=%d: %v", playerID, seq, ierr)
			}
			// uk 命中:seq 冲突在 meta 行锁下不可能来自并发,只可能是 idem key 复用。
			var storedFP string
			ferr := tx.QueryRowContext(ctx,
				`SELECT fingerprint FROM bag_journal WHERE player_id = ? AND idempotency_key = ?`,
				playerID, entry.GetIdempotencyKey()).Scan(&storedFP)
			if ferr != nil {
				return 0, errcode.New(errcode.ErrInternal, "read journal idem player=%d key=%s: %v",
					playerID, entry.GetIdempotencyKey(), ferr)
			}
			if storedFP != fp {
				// 同一幂等键被用于不同内容:客户端 bug / 重放攻击 / key 冲突。fail-closed 拒绝,
				// 但必须留证——这是完整性信号,能发现异常写入方(业务码不被 access log 当故障)。
				plog.With(ctx).Warnw("msg", "bag_idempotency_conflict",
					"player_id", playerID, "idempotency_key", entry.GetIdempotencyKey())
				return 0, errcode.New(errcode.ErrBagIdempotencyConflict,
					"idempotency_key reused for different content player=%d key=%s", playerID, entry.GetIdempotencyKey())
			}
			return 0, errcode.New(errcode.ErrBagSeqConflict,
				"idempotency_key already applied under another seq player=%d key=%s seq=%d",
				playerID, entry.GetIdempotencyKey(), seq)
		}
		newSeq = seq
		applied++
	}

	if applied == 0 {
		// 整批旧条目重放:返回当前水位,安全可清(语义同 ReportProgress)。
		if cerr := tx.Commit(); cerr != nil {
			return 0, errcode.New(errcode.ErrInternal, "commit replay player=%d: %v", playerID, cerr)
		}
		return lastSeq, nil
	}

	// 落脏段(与 journal 同事务:转移/领取/使用零撕裂)。
	for bagType, isDirty := range dirty {
		if !isDirty {
			continue
		}
		sec := sections[bagType]
		blob, merr := proto.Marshal(sec)
		if merr != nil {
			return 0, errcode.New(errcode.ErrInternal, "marshal section player=%d bag=%d: %v", playerID, bagType, merr)
		}
		const up = `INSERT INTO bag_section (player_id, bag_type, generation, section) VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE generation = VALUES(generation), section = VALUES(section)`
		if _, uerr := tx.ExecContext(ctx, up, playerID, bagType, sec.GetGeneration(), blob); uerr != nil {
			return 0, errcode.New(errcode.ErrInternal, "upsert section player=%d bag=%d: %v", playerID, bagType, uerr)
		}
	}

	if _, uerr := tx.ExecContext(ctx,
		`UPDATE bag_meta SET last_journal_seq = ? WHERE player_id = ?`, newSeq, playerID); uerr != nil {
		return 0, errcode.New(errcode.ErrInternal, "advance watermark player=%d: %v", playerID, uerr)
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, errcode.New(errcode.ErrInternal, "commit append player=%d: %v", playerID, cerr)
	}
	return newSeq, nil
}

func (r *MySQLBagRepo) SaveCheckpoint(ctx context.Context, playerID, ownerEpoch uint64, snapshot []byte, coveredSeq uint64) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "begin checkpoint tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	lastSeq, lerr := lockBagMetaTx(ctx, tx, playerID, ownerEpoch)
	if lerr != nil {
		return lerr
	}
	if coveredSeq > lastSeq {
		return errcode.New(errcode.ErrBagCheckpointStale,
			"covered_seq beyond watermark player=%d covered=%d last=%d", playerID, coveredSeq, lastSeq)
	}
	var existing uint64
	qerr := tx.QueryRowContext(ctx,
		`SELECT covered_journal_seq FROM bag_checkpoint WHERE player_id = ? FOR UPDATE`, playerID).Scan(&existing)
	if qerr != nil && !errors.Is(qerr, sql.ErrNoRows) {
		return errcode.New(errcode.ErrInternal, "read checkpoint covered player=%d: %v", playerID, qerr)
	}
	if qerr == nil && coveredSeq < existing {
		return errcode.New(errcode.ErrBagCheckpointStale,
			"covered_seq regressed player=%d covered=%d existing=%d", playerID, coveredSeq, existing)
	}

	const up = `INSERT INTO bag_checkpoint (player_id, snapshot, covered_journal_seq) VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE snapshot = VALUES(snapshot), covered_journal_seq = VALUES(covered_journal_seq)`
	if _, uerr := tx.ExecContext(ctx, up, playerID, snapshot, coveredSeq); uerr != nil {
		return errcode.New(errcode.ErrInternal, "upsert checkpoint player=%d: %v", playerID, uerr)
	}
	if cerr := tx.Commit(); cerr != nil {
		return errcode.New(errcode.ErrInternal, "commit checkpoint player=%d: %v", playerID, cerr)
	}
	return nil
}

func (r *MySQLBagRepo) GetSections(ctx context.Context, playerID uint64, bagTypes []uint32, capacity BagSectionCapacity) ([]*bagv1.BagSection, error) {
	out := make([]*bagv1.BagSection, 0, len(bagTypes))
	for _, bagType := range bagTypes {
		var current uint64
		if IsActivityBagType(bagType) {
			gerr := r.db.QueryRowContext(ctx,
				`SELECT current_generation FROM bag_generation WHERE bag_type = ?`, bagType).Scan(&current)
			if gerr != nil && !errors.Is(gerr, sql.ErrNoRows) {
				return nil, errcode.New(errcode.ErrInternal, "read generation bag_type=%d: %v", bagType, gerr)
			}
		}
		// 有效容量(§5.3):base + 已购增量。
		extra, _, xerr := r.GetCapacityState(ctx, playerID, bagType)
		if xerr != nil {
			return nil, xerr
		}
		effCap := effectiveCapacityFn(capacity, map[uint32]uint32{bagType: extra})
		sec := &bagv1.BagSection{BagType: bagType, Generation: current, Capacity: effCap(bagType)}
		var (
			rowGen uint64
			blob   []byte
		)
		serr := r.db.QueryRowContext(ctx,
			`SELECT generation, section FROM bag_section WHERE player_id = ? AND bag_type = ?`,
			playerID, bagType).Scan(&rowGen, &blob)
		if serr != nil && !errors.Is(serr, sql.ErrNoRows) {
			return nil, errcode.New(errcode.ErrInternal, "read section player=%d bag=%d: %v", playerID, bagType, serr)
		}
		// 读过滤:活动段只认 current generation(切代即逻辑清空,旧代行返回空段)。
		if serr == nil && (!IsActivityBagType(bagType) || rowGen == current) {
			if uerr := proto.Unmarshal(blob, sec); uerr != nil {
				return nil, errcode.New(errcode.ErrInternal, "decode section player=%d bag=%d: %v", playerID, bagType, uerr)
			}
			sec.BagType = bagType
			sec.Generation = current
			sec.Capacity = effCap(bagType)
		}
		out = append(out, sec)
	}
	return out, nil
}

func (r *MySQLBagRepo) SweepJournal(ctx context.Context, retention time.Duration, batch int) (int64, error) {
	if batch <= 0 || retention <= 0 {
		return 0, nil
	}
	// 删除资格 = 超保留期 **且** 已被该玩家 checkpoint 覆盖(INC-20260722-003:
	// 恢复 = 快照 + (covered, last] 尾部重放,未覆盖尾部是唯一恢复数据,时间到期也绝不删;
	// 时间阈值只是附加条件,覆盖水位才是删除资格)。无 checkpoint 行的玩家 INNER JOIN
	// 不命中,任何流水都不删。covered_journal_seq 只单调前进(SaveCheckpoint 拒回退),
	// 子查询一致性读读到旧值只会少删(安全方向),与 SaveCheckpoint 并发无需额外锁。
	// 多表 DELETE 不支持 LIMIT → 派生表选主键再删(短事务小批量,§9.24)。
	res, err := r.db.ExecContext(ctx, `
DELETE FROM bag_journal WHERE id IN (
  SELECT id FROM (
    SELECT j.id
    FROM bag_journal j
    JOIN bag_checkpoint c ON c.player_id = j.player_id
    WHERE j.created_at < (NOW() - INTERVAL ? SECOND)
      AND j.journal_seq <= c.covered_journal_seq
    ORDER BY j.id
    LIMIT ?
  ) pick)`,
		int64(retention.Seconds()), batch)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "sweep bag_journal: %v", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}
