// Package data — owner 权威数据层(pandora_owner 库,owner-authority.md §2/§3)。
//
// 一致性核心(§9.22):owner_record 行是每玩家的串行化锚点——所有 transition 先
// SELECT ... FOR UPDATE 锁该行,epoch 单调 CAS、admit_not_before 计算(同事务 FOR UPDATE
// 读旧实例租约行,取 CAS 线性化点观察值)、PENDING→ADMITTED 推进全部在同一事务内完成。
// 锁序固定 owner_record → ds_instance_lease,Renew 只锁 lease 行,无环无死锁。
//
// SQL 写法 TiDB 安全:只锁存在行 + 条件更新,不依赖间隙锁(生产 TiDB / dev 单机 MySQL 同构)。
package data

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
)

// OwnerType 取值(对齐 owner.proto OwnerType)。
const (
	OwnerTypeNone   int8 = 0
	OwnerTypeHub    int8 = 1
	OwnerTypeBattle int8 = 2
)

// OwnerPhase 取值(对齐 owner.proto OwnerPhase)。
const (
	OwnerPhaseNone     int8 = 0
	OwnerPhasePending  int8 = 1
	OwnerPhaseAdmitted int8 = 2
)

// transition 审计 op 取值。
const (
	transitionOpBegin   int8 = 1
	transitionOpAdmit   int8 = 2
	transitionOpRelease int8 = 3
)

// OwnerTarget exact DS 实例身份(对齐 pkg/placement.Target 语义;同名换实例不相等)。
type OwnerTarget struct {
	PodName                  string
	InstanceUID              string
	InstanceEpoch            uint32
	AssignmentOrAllocationID string
	ReleaseTrack             string
}

// Equal 四元组 + 分配 ID + 轨道全等(§9.22 exact 匹配)。
func (t OwnerTarget) Equal(o OwnerTarget) bool {
	return t.PodName == o.PodName && t.InstanceUID == o.InstanceUID &&
		t.InstanceEpoch == o.InstanceEpoch &&
		t.AssignmentOrAllocationID == o.AssignmentOrAllocationID &&
		t.ReleaseTrack == o.ReleaseTrack
}

// Complete 实例身份完整性(pod/uid/epoch/track/分配 ID 全非空)。
func (t OwnerTarget) Complete() bool {
	return strings.TrimSpace(t.PodName) != "" && strings.TrimSpace(t.InstanceUID) != "" &&
		t.InstanceEpoch > 0 && strings.TrimSpace(t.AssignmentOrAllocationID) != "" &&
		strings.TrimSpace(t.ReleaseTrack) != ""
}

// OwnerRecord 每玩家 owner 权威记录(LeaseDeadlineMs 为派生字段:同事务读实例租约)。
type OwnerRecord struct {
	PlayerID         uint64
	OwnerEpoch       uint64
	OwnerType        int8
	Phase            int8
	Target           OwnerTarget
	OperationID      string
	AdmitNotBeforeMs int64
	LeaseDeadlineMs  int64
	UpdatedAtMs      int64
}

// OwnerRepo 是 owner 权威数据层抽象。
type OwnerRepo interface {
	// Query 读当前记录(无行返回 epoch=0/none;附带派生 lease 截止)。
	Query(ctx context.Context, playerID uint64) (OwnerRecord, error)

	// BeginTransition CAS expect_epoch → epoch+1/PENDING/newTarget;admit_not_before 按旧
	// owner 类型分流:旧 owner=BATTLE → 同事务读旧实例租约,= max(now, 旧 deadline)+skewMargin
	// (失联对局 DS 的双可玩/迟到写风险);旧 owner=HUB 或无 → now(协作迁移,双写由 epoch
	// fencing 拦,双可玩由客户端单连接拆链拦;详见实现处举证)。
	// 同 (player, operationID) 幂等重放。expect 不符 → ErrOwnerEpochConflict(附当前记录)。
	BeginTransition(ctx context.Context, playerID, expectEpoch uint64, operationID string, ownerType int8, target OwnerTarget, skewMargin time.Duration) (OwnerRecord, error)

	// Admit 屏障开 + epoch/operation/实例全等 → PENDING→ADMITTED;已 ADMITTED 幂等重放。
	// 屏障未开 → ErrOwnerBarrierNotOpen(retryAfterMs>0)。
	Admit(ctx context.Context, playerID, ownerEpoch uint64, operationID string, target OwnerTarget) (rec OwnerRecord, retryAfterMs int64, err error)

	// RenewInstanceLease 实例租约续期(deadline 只前进;实例纪元不符拒)。返回生效截止。
	RenewInstanceLease(ctx context.Context, target OwnerTarget, lease time.Duration) (int64, error)

	// Release epoch+operation 匹配 → 置 none(epoch 保留);不匹配(迟到)幂等 no-op 返回当前。
	Release(ctx context.Context, playerID, ownerEpoch uint64, operationID string) (OwnerRecord, error)

	// SweepTransitionLog 删除超保留期审计行(有界批量)。
	SweepTransitionLog(ctx context.Context, retention time.Duration, batch int) (int64, error)
}

// MySQLOwnerRepo 基于 database/sql 的实现(生产连 TiDB,dev 连单机 MySQL;DDL 同构)。
type MySQLOwnerRepo struct {
	db *sql.DB
}

// NewMySQLOwnerRepo 构造。
func NewMySQLOwnerRepo(db *sql.DB) *MySQLOwnerRepo {
	return &MySQLOwnerRepo{db: db}
}

func nowUnixMs() int64 { return time.Now().UnixMilli() }

// scanRecordRow 读 owner_record 行(锁定与否由调用 SQL 决定)。无行 → zero 记录 + false。
func scanRecordRow(row *sql.Row, playerID uint64) (OwnerRecord, bool, error) {
	rec := OwnerRecord{PlayerID: playerID}
	err := row.Scan(&rec.OwnerEpoch, &rec.OwnerType, &rec.Phase,
		&rec.Target.PodName, &rec.Target.InstanceUID, &rec.Target.InstanceEpoch,
		&rec.Target.AssignmentOrAllocationID, &rec.Target.ReleaseTrack,
		&rec.OperationID, &rec.AdmitNotBeforeMs, &rec.UpdatedAtMs)
	if errors.Is(err, sql.ErrNoRows) {
		return rec, false, nil
	}
	if err != nil {
		return rec, false, errcode.New(errcode.ErrInternal, "scan owner_record player=%d: %v", playerID, err)
	}
	return rec, true, nil
}

const selectRecordCols = `SELECT owner_epoch, owner_type, phase, pod_name, instance_uid, instance_epoch,
 assignment_or_allocation_id, release_track, operation_id, admit_not_before_ms, updated_at_ms
 FROM owner_record WHERE player_id = ?`

// lockRecordTx 确保并锁定 owner_record 行(无行则建 epoch=0/none 再锁)。
func lockRecordTx(ctx context.Context, tx *sql.Tx, playerID uint64) (OwnerRecord, error) {
	const ins = `INSERT IGNORE INTO owner_record (player_id, updated_at_ms) VALUES (?, ?)`
	if _, ierr := tx.ExecContext(ctx, ins, playerID, nowUnixMs()); ierr != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "ensure owner_record player=%d: %v", playerID, ierr)
	}
	rec, found, err := scanRecordRow(tx.QueryRowContext(ctx, selectRecordCols+` FOR UPDATE`, playerID), playerID)
	if err != nil {
		return OwnerRecord{}, err
	}
	if !found {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "owner_record vanished after ensure player=%d", playerID)
	}
	return rec, nil
}

// readLeaseDeadline 读实例租约截止(forUpdate 决定是否锁行;无行返回 0)。
func readLeaseDeadline(ctx context.Context, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}, instanceUID string, forUpdate bool) (int64, error) {
	if strings.TrimSpace(instanceUID) == "" {
		return 0, nil
	}
	query := `SELECT lease_deadline_ms FROM ds_instance_lease WHERE instance_uid = ?`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var deadline int64
	err := q.QueryRowContext(ctx, query, instanceUID).Scan(&deadline)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "read lease uid=%s: %v", instanceUID, err)
	}
	return deadline, nil
}

func appendTransitionLog(ctx context.Context, tx *sql.Tx, playerID, fromEpoch, toEpoch uint64, op int8, operationID, detail string) error {
	const ins = `INSERT INTO owner_transition_log (player_id, from_epoch, to_epoch, op, operation_id, detail)
VALUES (?, ?, ?, ?, ?, ?)`
	if _, err := tx.ExecContext(ctx, ins, playerID, fromEpoch, toEpoch, op, operationID, detail); err != nil {
		return errcode.New(errcode.ErrInternal, "append transition log player=%d: %v", playerID, err)
	}
	return nil
}

func (r *MySQLOwnerRepo) Query(ctx context.Context, playerID uint64) (OwnerRecord, error) {
	// 读事务:record + 派生 lease 两读同快照(§9.22 状态按语义拆开,查询不落缓存)。
	//
	// ⚠️ 不能用 sql.TxOptions{ReadOnly: true}(2026-07-27 审计,实测):go-sql-driver 会把它
	// 翻成 `START TRANSACTION READ ONLY`,而 TiDB 在默认 tidb_enable_noop_functions=OFF 下
	// 直接返回 `Error 1235: function READ ONLY has only noop implementation in tidb now`。
	// owner 生产被 -Prod 机械注入 require_tidb: true 强制连 TiDB,而 Query 是 owner 唯一读路径
	// —— 保留 ReadOnly 等于 owner 一上生产就 100% 读失败(dev 走单机 MySQL 所以永远测不出来)。
	// 普通事务同样满足这里需要的「两读同快照」:TiDB 按 start_ts 取快照,InnoDB 按 RR 读视图。
	// 不要改用 DSN 打开 tidb_enable_noop_functions 绕过 —— 那是让 TiDB 假装接受语义它并不实现。
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "begin query tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rec, found, serr := scanRecordRow(tx.QueryRowContext(ctx, selectRecordCols, playerID), playerID)
	if serr != nil {
		return OwnerRecord{}, serr
	}
	if !found {
		return OwnerRecord{PlayerID: playerID}, nil // epoch=0 / none(从未有 owner)
	}
	if rec.OwnerType != OwnerTypeNone {
		deadline, derr := readLeaseDeadline(ctx, tx, rec.Target.InstanceUID, false)
		if derr != nil {
			return OwnerRecord{}, derr
		}
		rec.LeaseDeadlineMs = deadline
	}
	if cerr := tx.Commit(); cerr != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "commit query player=%d: %v", playerID, cerr)
	}
	return rec, nil
}

func (r *MySQLOwnerRepo) BeginTransition(ctx context.Context, playerID, expectEpoch uint64, operationID string, ownerType int8, target OwnerTarget, skewMargin time.Duration) (OwnerRecord, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "begin transition tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rec, lerr := lockRecordTx(ctx, tx, playerID)
	if lerr != nil {
		return OwnerRecord{}, lerr
	}

	// 幂等重放:同 operation 且记录就是本次 Begin 的结果(epoch=expect+1 / 目标全等)。
	// 响应丢失后的原样重试拿回同一结果,不再推进 epoch(§9.23 端到端幂等)。
	// operationID 为空时本分支不适用(空 = 调用方未持显式幂等键,交由下面的同实例收敛)。
	if operationID != "" && rec.OperationID == operationID && rec.OwnerEpoch == expectEpoch+1 &&
		rec.OwnerType == ownerType && rec.Target.Equal(target) {
		deadline, derr := readLeaseDeadline(ctx, tx, rec.Target.InstanceUID, false)
		if derr != nil {
			return OwnerRecord{}, derr
		}
		rec.LeaseDeadlineMs = deadline
		if cerr := tx.Commit(); cerr != nil {
			return OwnerRecord{}, errcode.New(errcode.ErrInternal, "commit replay begin player=%d: %v", playerID, cerr)
		}
		return rec, nil
	}

	// 同 exact 实例的重复投递:在本行锁事务内收敛为 no-op,原样返回既有记录
	// (不推进 epoch、不改 phase、不覆盖 operation_id)。
	//
	// 这段判定原先在调用方(两个 allocator 的 decideOwnerBegin:Query → 本地比对 → Begin)。
	// 移进事务的理由:
	//   ① **operation_id 必须稳定**(§9.23「一次真实进场 / owner 迁移使用一个稳定
	//      operation_id」)。调用方每次 Begin 现铸 uuid.NewString(),同一次进场的重复
	//      投递(重连、重复交付、心跳自愈)会写出不同 operation,幂等键形同虚设。改由权威
	//      在同目标时原样返回既有记录后,operation_id 天然贯穿整条链,客户端也能从
	//      ResumeContext 拿到同一个值续用。
	//   ② **判定与写入落在同一线性化点**。调用方那份判定是建议性的:Query 与 Begin 之间
	//      记录可能已变,expectEpoch 随之作废,只能靠 EPOCH_CONFLICT 兜底重查——而"目标
	//      已经是它"本就该是 no-op,不该先冲突再重来。
	//
	// 比对刻意只到**实例**粒度(type + pod + uid + instance_epoch),不含
	// assignment_or_allocation_id 与 release_track,沿用调用方原语义:同一实例上的
	// assignment 刷新(seat 续租)是同一 owner 的重复交付,纳入会让 epoch 无谓翻动(churn);
	// release track 是独立 Fleet,其变化必然伴随 pod/uid 变化,已被实例粒度捕获。
	if rec.OwnerType == ownerType && rec.Target.PodName == target.PodName &&
		rec.Target.InstanceUID == target.InstanceUID &&
		rec.Target.InstanceEpoch == target.InstanceEpoch &&
		(rec.Phase == OwnerPhasePending || rec.Phase == OwnerPhaseAdmitted) {
		deadline, derr := readLeaseDeadline(ctx, tx, rec.Target.InstanceUID, false)
		if derr != nil {
			return OwnerRecord{}, derr
		}
		rec.LeaseDeadlineMs = deadline
		if cerr := tx.Commit(); cerr != nil {
			return OwnerRecord{}, errcode.New(errcode.ErrInternal, "commit same-instance begin player=%d: %v", playerID, cerr)
		}
		return rec, nil
	}

	if rec.OwnerEpoch != expectEpoch {
		// CAS 期望不符:附当前记录返回,调用方重查再决策(禁盲重试推进 epoch)。
		// 单次冲突是 §9.23 query-first 正常竞争(故 INFO);同一 player 高频冲突 = 两个调用方
		// 在抢 owner 迁移,靠此可观测频率与双方 epoch(否则 in-band 业务码被 access log 记 DEBUG)。
		plog.With(ctx).Infow("msg", "owner_epoch_conflict",
			"player_id", playerID, "expect_epoch", expectEpoch, "current_epoch", rec.OwnerEpoch,
			"operation_id", operationID)
		return rec, errcode.New(errcode.ErrOwnerEpochConflict,
			"epoch conflict player=%d expect=%d current=%d", playerID, expectEpoch, rec.OwnerEpoch)
	}

	// 到这里必定是**真实迁移**(要写新记录),operation_id 不能为空:空 operation 会让后续
	// Admit 的 exact 校验、客户端续用与审计流水同时失去锚点。biz 层保证非空(空则铸新),
	// 本行是防止绕过 biz 的内部调用写出无锚点记录的兜底。
	if operationID == "" {
		return OwnerRecord{}, errcode.New(errcode.ErrOwnerInvalidOperation,
			"operation_id required for a real transition player=%d", playerID)
	}

	// admit_not_before:按旧 owner 类型分流(2026-08-03,实测事故:大厅每次进战斗都要干等
	// ~27s 屏障,客户端 30s 权威等待窗口随之耗尽,玩家看到的是"匹配没反应")。
	//
	// ① 旧 owner 是 **BATTLE**:保守屏障 = max(now, 旧实例租约截止) + 时钟/网络余量。
	//    失联战斗 DS 上该玩家的 Pawn 仍可能被模拟(AI/其他玩家仍与之交互),且 DS 是受信
	//    写者(§9.6)可能有 journal 迟到写在途——"双可玩 + 迟到写"风险真实存在,必须等旧
	//    实例本地自 fencing 的最晚时刻(其租约截止)过去后才许新 DS 接管。
	//
	// ② 旧 owner 是 **HUB**:屏障 = now,不等实例租约。举证(§15.4):
	//    - 双写防护不靠屏障:hub 对该玩家的权威写全部走 §9.6 五要件,本 CAS 提交后
	//      owner_epoch 已 +1,旧 epoch 的写与时间无关地被下游拒(五要件③ fencing);
	//    - "双可玩"防护不靠屏障:玩家只有一个客户端,travel 去新 DS 时旧 hub 连接被
	//      客户端协调器主动拆除,hub 侧 Pawn 随连接断开清退;大厅 Pawn 无对局语义,
	//      不存在"残留 Pawn 继续演化影响权威态"(对局内那种风险正是 ① 保守的原因);
	//    - hub 与后端分区也不改变上述两条(fencing 在写入侧、连接归属在客户端侧)。
	//    等实例租约唯一"防住"的是——该实例上其它逻辑仍自认持有玩家——而那正是 epoch
	//    fencing 的职责。hub 实例租约被 allocator 持续代续(整机级,承载数百玩家,永不
	//    过期),对 HUB 旧 owner 等它 = 恒定 ~27s 纯延迟、零安全收益,且违反验收底线
	//    第 1 条(无收益的强制等待)。margin 也一并省去:本分支不依赖旧 DS 本地自
	//    fencing 时钟,Admit 的屏障判定与本处写入同库同钟,无跨机偏移可补。
	//
	// 无旧 owner → 无需屏障(没有要围栏的旧 DS)。
	now := nowUnixMs()
	admitNotBefore := now
	if rec.OwnerType == OwnerTypeBattle && rec.Target.InstanceUID != "" {
		oldDeadline, derr := readLeaseDeadline(ctx, tx, rec.Target.InstanceUID, true)
		if derr != nil {
			return OwnerRecord{}, derr
		}
		base := now
		if oldDeadline > base {
			base = oldDeadline
		}
		admitNotBefore = base + skewMargin.Milliseconds()
	}

	newRec := OwnerRecord{
		PlayerID:         playerID,
		OwnerEpoch:       rec.OwnerEpoch + 1,
		OwnerType:        ownerType,
		Phase:            OwnerPhasePending,
		Target:           target,
		OperationID:      operationID,
		AdmitNotBeforeMs: admitNotBefore,
		UpdatedAtMs:      now,
	}
	const upd = `UPDATE owner_record SET owner_epoch = ?, owner_type = ?, phase = ?, pod_name = ?,
 instance_uid = ?, instance_epoch = ?, assignment_or_allocation_id = ?, release_track = ?,
 operation_id = ?, admit_not_before_ms = ?, updated_at_ms = ? WHERE player_id = ?`
	if _, uerr := tx.ExecContext(ctx, upd, newRec.OwnerEpoch, newRec.OwnerType, newRec.Phase,
		newRec.Target.PodName, newRec.Target.InstanceUID, newRec.Target.InstanceEpoch,
		newRec.Target.AssignmentOrAllocationID, newRec.Target.ReleaseTrack,
		newRec.OperationID, newRec.AdmitNotBeforeMs, newRec.UpdatedAtMs, playerID); uerr != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "cas owner_record player=%d: %v", playerID, uerr)
	}
	if aerr := appendTransitionLog(ctx, tx, playerID, rec.OwnerEpoch, newRec.OwnerEpoch,
		transitionOpBegin, operationID, target.PodName); aerr != nil {
		return OwnerRecord{}, aerr
	}
	newDeadline, derr := readLeaseDeadline(ctx, tx, target.InstanceUID, false)
	if derr != nil {
		return OwnerRecord{}, derr
	}
	newRec.LeaseDeadlineMs = newDeadline
	if cerr := tx.Commit(); cerr != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "commit begin player=%d: %v", playerID, cerr)
	}
	return newRec, nil
}

func (r *MySQLOwnerRepo) Admit(ctx context.Context, playerID, ownerEpoch uint64, operationID string, target OwnerTarget) (OwnerRecord, int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return OwnerRecord{}, 0, errcode.New(errcode.ErrInternal, "begin admit tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rec, found, serr := scanRecordRow(tx.QueryRowContext(ctx, selectRecordCols+` FOR UPDATE`, playerID), playerID)
	if serr != nil {
		return OwnerRecord{}, 0, serr
	}
	if !found || rec.OwnerEpoch != ownerEpoch || rec.OperationID != operationID ||
		rec.OwnerType == OwnerTypeNone || !rec.Target.Equal(target) {
		// fail-closed:任何一项不匹配都拒(旧 epoch / 换代实例 / 伪造 operation 都进不来)。
		// owner fencing 的核心拒绝点(§9.22),恰是要能查到的脑裂 / stale writer 信号 → WARN。
		plog.With(ctx).Warnw("msg", "owner_admit_identity_mismatch",
			"player_id", playerID, "found", found,
			"req_epoch", ownerEpoch, "current_epoch", rec.OwnerEpoch,
			"req_op", operationID, "current_op", rec.OperationID,
			"target_match", rec.Target.Equal(target), "owner_type", rec.OwnerType)
		return rec, 0, errcode.New(errcode.ErrOwnerIdentityMismatch,
			"admit identity mismatch player=%d epoch=%d op=%s", playerID, ownerEpoch, operationID)
	}
	if rec.Phase == OwnerPhaseAdmitted {
		// 幂等重放:Admission 回包丢失 → 原样返回,不再分配、不创建第二 owner(§9.23)。
		if cerr := tx.Commit(); cerr != nil {
			return OwnerRecord{}, 0, errcode.New(errcode.ErrInternal, "commit replay admit player=%d: %v", playerID, cerr)
		}
		return rec, 0, nil
	}
	now := nowUnixMs()
	if now < rec.AdmitNotBeforeMs {
		// 屏障未开:WAIT 语义(§9.23),带剩余毫秒退避重试;安全优先但不永久卡(watchdog 驱动)。
		// 调用方按 wait_ms 轮询,故 DEBUG 避免刷屏;LOG_LEVEL=debug 时可观测迁移是否卡在屏障。
		plog.With(ctx).Debugw("msg", "owner_admit_barrier_wait",
			"player_id", playerID, "owner_epoch", ownerEpoch, "wait_ms", rec.AdmitNotBeforeMs-now)
		return rec, rec.AdmitNotBeforeMs - now, errcode.New(errcode.ErrOwnerBarrierNotOpen,
			"admit barrier not open player=%d wait_ms=%d", playerID, rec.AdmitNotBeforeMs-now)
	}
	const upd = `UPDATE owner_record SET phase = ?, updated_at_ms = ? WHERE player_id = ? AND owner_epoch = ?`
	if _, uerr := tx.ExecContext(ctx, upd, OwnerPhaseAdmitted, now, playerID, ownerEpoch); uerr != nil {
		return OwnerRecord{}, 0, errcode.New(errcode.ErrInternal, "admit update player=%d: %v", playerID, uerr)
	}
	if aerr := appendTransitionLog(ctx, tx, playerID, rec.OwnerEpoch, rec.OwnerEpoch,
		transitionOpAdmit, operationID, target.PodName); aerr != nil {
		return OwnerRecord{}, 0, aerr
	}
	rec.Phase = OwnerPhaseAdmitted
	rec.UpdatedAtMs = now
	deadline, derr := readLeaseDeadline(ctx, tx, rec.Target.InstanceUID, false)
	if derr != nil {
		return OwnerRecord{}, 0, derr
	}
	rec.LeaseDeadlineMs = deadline
	if cerr := tx.Commit(); cerr != nil {
		return OwnerRecord{}, 0, errcode.New(errcode.ErrInternal, "commit admit player=%d: %v", playerID, cerr)
	}
	return rec, 0, nil
}

func (r *MySQLOwnerRepo) RenewInstanceLease(ctx context.Context, target OwnerTarget, lease time.Duration) (int64, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "begin renew tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	now := nowUnixMs()
	newDeadline := now + lease.Milliseconds()

	var (
		storedEpoch    uint32
		storedDeadline int64
	)
	qerr := tx.QueryRowContext(ctx,
		`SELECT instance_epoch, lease_deadline_ms FROM ds_instance_lease WHERE instance_uid = ? FOR UPDATE`,
		target.InstanceUID).Scan(&storedEpoch, &storedDeadline)
	switch {
	case errors.Is(qerr, sql.ErrNoRows):
		const ins = `INSERT INTO ds_instance_lease (instance_uid, pod_name, instance_epoch, release_track, lease_deadline_ms, updated_at_ms)
VALUES (?, ?, ?, ?, ?, ?)`
		if _, ierr := tx.ExecContext(ctx, ins, target.InstanceUID, target.PodName, target.InstanceEpoch,
			target.ReleaseTrack, newDeadline, now); ierr != nil {
			return 0, errcode.New(errcode.ErrInternal, "insert lease uid=%s: %v", target.InstanceUID, ierr)
		}
	case qerr != nil:
		return 0, errcode.New(errcode.ErrInternal, "lock lease uid=%s: %v", target.InstanceUID, qerr)
	default:
		// 实例纪元守卫:双方都带纪元且不同 → 换代实例不得续旧行(fail-closed)。
		// 请求 0 = 调用方无纪元语义(hub 凭据不携带实例纪元,uid 全局唯一已足);
		// 存量 0 且请求非零 → 首次补齐纪元(battle 侧续租升级旧行)。
		if storedEpoch != 0 && target.InstanceEpoch != 0 && storedEpoch != target.InstanceEpoch {
			// 换代实例试图续旧租约行的 fencing 拒绝(§8):可能是 Pod 复用 uid 或 epoch 回退。
			// 续租链断裂本只能靠玩家掉线反推,这里显式 WARN 留证。
			plog.With(ctx).Warnw("msg", "owner_lease_epoch_regressed",
				"instance_uid", target.InstanceUID, "pod", target.PodName,
				"stored_epoch", storedEpoch, "req_epoch", target.InstanceEpoch)
			return 0, errcode.New(errcode.ErrOwnerLeaseRegressed,
				"instance epoch mismatch uid=%s stored=%d req=%d", target.InstanceUID, storedEpoch, target.InstanceEpoch)
		}
		if storedEpoch == 0 && target.InstanceEpoch != 0 {
			if _, uerr := tx.ExecContext(ctx,
				`UPDATE ds_instance_lease SET instance_epoch = ? WHERE instance_uid = ?`,
				target.InstanceEpoch, target.InstanceUID); uerr != nil {
				return 0, errcode.New(errcode.ErrInternal, "backfill lease epoch uid=%s: %v", target.InstanceUID, uerr)
			}
		}
		if newDeadline <= storedDeadline {
			// deadline 只前进:乱序/迟到续租幂等返回现值,不回退。
			newDeadline = storedDeadline
		} else {
			const upd = `UPDATE ds_instance_lease SET lease_deadline_ms = ?, updated_at_ms = ? WHERE instance_uid = ?`
			if _, uerr := tx.ExecContext(ctx, upd, newDeadline, now, target.InstanceUID); uerr != nil {
				return 0, errcode.New(errcode.ErrInternal, "renew lease uid=%s: %v", target.InstanceUID, uerr)
			}
		}
	}
	if cerr := tx.Commit(); cerr != nil {
		return 0, errcode.New(errcode.ErrInternal, "commit renew uid=%s: %v", target.InstanceUID, cerr)
	}
	return newDeadline, nil
}

func (r *MySQLOwnerRepo) Release(ctx context.Context, playerID, ownerEpoch uint64, operationID string) (OwnerRecord, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "begin release tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	rec, found, serr := scanRecordRow(tx.QueryRowContext(ctx, selectRecordCols+` FOR UPDATE`, playerID), playerID)
	if serr != nil {
		return OwnerRecord{}, serr
	}
	if !found || rec.OwnerEpoch != ownerEpoch || rec.OperationID != operationID || rec.OwnerType == OwnerTypeNone {
		// 迟到 Release(旧 epoch / 旧 operation / 已释放):幂等 no-op,只能"compare-delete 自己"。
		if cerr := tx.Commit(); cerr != nil {
			return OwnerRecord{}, errcode.New(errcode.ErrInternal, "commit noop release player=%d: %v", playerID, cerr)
		}
		return rec, nil
	}
	now := nowUnixMs()
	const upd = `UPDATE owner_record SET owner_type = ?, phase = ?, pod_name = '', instance_uid = '',
 instance_epoch = 0, assignment_or_allocation_id = '', release_track = '', updated_at_ms = ?
 WHERE player_id = ? AND owner_epoch = ?`
	if _, uerr := tx.ExecContext(ctx, upd, OwnerTypeNone, OwnerPhaseNone, now, playerID, ownerEpoch); uerr != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "release update player=%d: %v", playerID, uerr)
	}
	if aerr := appendTransitionLog(ctx, tx, playerID, rec.OwnerEpoch, rec.OwnerEpoch,
		transitionOpRelease, operationID, ""); aerr != nil {
		return OwnerRecord{}, aerr
	}
	rec.OwnerType = OwnerTypeNone
	rec.Phase = OwnerPhaseNone
	rec.Target = OwnerTarget{}
	rec.LeaseDeadlineMs = 0
	rec.UpdatedAtMs = now
	if cerr := tx.Commit(); cerr != nil {
		return OwnerRecord{}, errcode.New(errcode.ErrInternal, "commit release player=%d: %v", playerID, cerr)
	}
	return rec, nil
}

func (r *MySQLOwnerRepo) SweepTransitionLog(ctx context.Context, retention time.Duration, batch int) (int64, error) {
	if batch <= 0 || retention <= 0 {
		return 0, nil
	}
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM owner_transition_log WHERE created_at < (NOW() - INTERVAL ? SECOND) LIMIT ?`,
		int64(retention.Seconds()), batch)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "sweep transition log: %v", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}
