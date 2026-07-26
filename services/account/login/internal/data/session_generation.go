// session_generation.go — 会话代际 MySQL 持久化(R7 复审 P0-4,2026-07-23;R7 收口改单调代际)。
//
// 背景:角色写 fencing 此前依赖「MySQL 事务内 precommit 读 Redis 会话权威」,precommit
// 通过与 COMMIT 之间存在跨存储窗口——A 的 Redis 检查通过 → B 登录轮换 session → A COMMIT,
// 旧会话的角色写仍成为新会话看到的权威数据。
//
// R7 收口(并发 Login 定序):首版实现是「无条件覆盖 upsert」,并发登录 A/B 各自先写
// Redis 再写 MySQL 时,迟到的 A 会把 MySQL 回写成旧 jti(Redis=B、MySQL=A 撕裂),合法的
// B 反而被 SetRole 代际复核拒绝。现在 MySQL 是登录定序权威:本表增加单调 `generation`
// 列,PersistSessionJTI 在事务内原子 +1 并返回新代际;Login 拿到代际后再对 Redis 做
// 「仅更高代际可覆盖」的条件写(见 account.go RedisSessionRepo.Set)。任意交错下两个
// 存储最终都收敛到最高代际那次登录,输掉定序的登录直接失败,不交付凭据。
//
// SetRole 侧契约不变:同一 MySQL 事务内 SELECT ... FOR UPDATE 复核 sess_jti 后再 COMMIT,
// InnoDB 行锁保证与本表的代际写串行化(见 role.go)。
package data

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// MySQL 事务并发错误码:1213 死锁、1205 锁等待超时。二者发生时 InnoDB 已回滚本事务,
// 整段事务重放是安全且标准的做法(与 services/social/guild 的 isRetryableTxErr 同口径)。
const (
	mysqlErrLockWaitTimeout = 1205
	mysqlErrDeadlock        = 1213
)

// persistMaxAttempts 是 PersistSessionJTI 遇并发错误时的有界重试次数。
//
// 为什么需要它(R11 复审 P0-1,真 MySQL 实测发现):`SELECT ... FOR UPDATE` 命中**不存在的
// 行**时 InnoDB 加的是 gap/next-key 锁;多个首登事务先各自拿到同一主键的 gap 锁,再各自要
// INSERT 的 insert intention 锁 → 互等 → 1213 死锁。这是"先 FOR UPDATE 再 INSERT"的经典
// 死锁模式,只在**真并发首登**下出现(同玩家多设备同时首登、或客户端重试),fake 测不出来。
//
// 3 次足够:死锁的一方必然被 InnoDB 选中回滚,重放时对手已提交、行已存在,
// FOR UPDATE 退化为普通行锁,不再有 gap 锁竞争。
const persistMaxAttempts = 3

// isRetryableTxErr 判断是否为可重试的事务并发错误。依赖调用方用 errcode.NewCause 保留
// 底层 *mysql.MySQLError,errors.As 才能沿 Unwrap 链检出。
func isRetryableTxErr(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) &&
		(me.Number == mysqlErrDeadlock || me.Number == mysqlErrLockWaitTimeout)
}

// ErrCommitAmbiguous 标记「COMMIT 结果不确定」:MySQL 可能已经提交,但 Commit() 因连接
// 中断 / 代理断链 / deadline 返回错误(R11 复审 P0-1 问题 A)。这是唯一必须与「确定没提交」
// 区分开的失败:确定没提交时直接失败即一致,而不确定时若直接失败,MySQL 可能已经带着
// 一个**从未交付给客户端的 jti** 推进了代际,把仍在线的上一代会话错误地 fence 出去。
//
// 调用方(biz)见到它必须用本次 jti 作唯一标记读回权威(LoadSessionGeneration)把不确定
// 态**判定**掉,而不是猜。用 errors.Is 检出(errcode.NewCause 保留 Unwrap 链)。
var ErrCommitAmbiguous = errors.New("session generation commit result unknown")

// IsCommitAmbiguous 判定错误是否为「COMMIT 结果不确定」。
func IsCommitAmbiguous(err error) bool { return errors.Is(err, ErrCommitAmbiguous) }

// SessionGenerationLease 一次 PersistSessionJTI 的分配结果:本次代际 + 覆盖前的行
// 状态快照。Redis 条件写失败(基础设施错误)时,Login 用它做条件回补
// (RestoreSessionJTI),消除「MySQL 已轮换、Redis 仍是上一代」的撕裂窗口
// (R9 复审 P0-2):撕裂期间上一代合法会话会被 SetRole 代际强制门误拒。
type SessionGenerationLease struct {
	// Generation 本次登录分配到的单调代际(首登=1)。
	Generation uint64
	// PrevJTI 覆盖前行内的 sess_jti;HadPrev=false 时无意义。
	PrevJTI string
	// HadPrev 覆盖前该玩家是否已有代际行(false=本次为首登插入)。
	HadPrev bool
	// SnapshotUnknown 表示本次代际是从「不确定的 COMMIT」读回确认的(R11 复审 P0-1):
	// 代际与 jti 可信,但**覆盖前的行状态无从得知**(快照随失败的事务一起丢了)。
	// 此时禁止条件回补(RestoreSessionJTI):回补要么错误复活上一代 jti、要么误删行让
	// SetRole 走缺行兼容路径——两者都是审核明令禁止的「旧凭据错误复活」。
	// 需要补偿时只能条件墓碑(TombstoneSessionJTI):两侧一致拒绝所有旧凭据,fail-closed。
	SnapshotUnknown bool
}

// SessionGenerationRepo 持久化玩家当前会话代际(jti + 单调 generation)到 MySQL,
// 供 Login 定序与业务写事务同事务域 fencing。
type SessionGenerationRepo interface {
	// PersistSessionJTI 原子推进玩家会话代际并落当前 jti,返回本次登录分配到的
	// 单调 generation(首登=1)与覆盖前状态。Login 必须在 Redis 会话写入之前调用;
	// 失败必须使登录失败(fail-closed),否则定序权威落后于 Redis。
	PersistSessionJTI(ctx context.Context, playerID uint64, jti string) (SessionGenerationLease, error)

	// RestoreSessionJTI 条件回补(R9 复审 P0-2):仅当行仍是本次登录写入的
	// (failedJTI, lease.Generation) 时,把 sess_jti 回写为覆盖前的值(HadPrev=false
	// 则删除整行,回到首登前状态)。generation 保持已推进值不回退,单调性不破坏。
	// 并发新登录已把行推到更高代际时 WHERE 不命中 = no-op,绝不回滚别人的登录。
	// 返回是否实际回补。仅用于「MySQL 已提交、Redis 条件写基础设施失败」的补偿,
	// 失败只记日志(下一次成功登录仍会原子推进两个存储自愈)。
	RestoreSessionJTI(ctx context.Context, playerID uint64, failedJTI string, lease SessionGenerationLease) (bool, error)

	// LoadSessionGeneration 读回玩家当前代际行(R11 复审 P0-1 问题 A 的判定读):
	// PersistSessionJTI 返回 ErrCommitAmbiguous 时,调用方用本次 jti 作唯一标记读回,
	// 把「到底提交没有」变成可判定事实。found=false = 无行(必然没提交,或已被并发登录
	// 清理);jti != 本次 jti = 本次写没落地、或已被更高代际的登录取代。
	LoadSessionGeneration(ctx context.Context, playerID uint64) (jti string, generation uint64, found bool, err error)

	// TombstoneSessionJTI 登出墓碑(R8 收口,P2):仅当行内 sess_jti 仍等于本次登出
	// 的 jti 时,把它推进一代并改写为哨兵值(永不命中真 jti)——登出后 MySQL 行
	// 不再与已失效的旧 jti 匹配。条件写(CAS)而非无条件覆盖:并发新登录可能已
	// 把行推到更新代际,无条件墓碑会毒化新会话。返回是否实际写入。
	TombstoneSessionJTI(ctx context.Context, playerID uint64, jti string) (bool, error)
}

// MySQLSessionGenerationRepo 基于 *sql.DB 的实现(pandora_account.player_session_generations)。
type MySQLSessionGenerationRepo struct {
	db *sql.DB
}

// NewMySQLSessionGenerationRepo 构造。db 与 AccountRepo/PlayerRoleRepo 共用连接池。
func NewMySQLSessionGenerationRepo(db *sql.DB) *MySQLSessionGenerationRepo {
	return &MySQLSessionGenerationRepo{db: db}
}

// PersistSessionJTI 事务内「SELECT ... FOR UPDATE 快照旧值 → upsert(generation+1) →
// 读回本行代际 → COMMIT」。行 X 锁持有到 COMMIT,同事务读回的必然是本次分配的代际,
// 并发登录在此串行化。旧值快照供 Redis 写失败时条件回补(RestoreSessionJTI)。
// 首登竞态(两事务都看到无行)下输家的快照可能缺失刚提交的对手行——该窗口极窄且
// 回补是条件 CAS,最坏退化为删行(= SetRole 缺行兼容路径),下一次成功登录自愈。
// R11 复审 P0-1(真 MySQL 实测):并发首登会因"先 FOR UPDATE 再 INSERT"的 gap 锁竞争
// 触发 1213 死锁,故整段事务外套有界重试(见 persistMaxAttempts)。重试耗尽后返回
// **ErrUnavailable**(可重试语义)而不是 ErrInternal —— 客户端与 UE 侧的
// IsRetryableLoginFailure 只对可重试语义做自动退避恢复;报成 ErrInternal 会让玩家
// 卡在登录页(违反不卡玩家)。
func (r *MySQLSessionGenerationRepo) PersistSessionJTI(ctx context.Context, playerID uint64, jti string) (SessionGenerationLease, error) {
	var lastErr error
	for attempt := 0; attempt < persistMaxAttempts; attempt++ {
		lease, err := r.persistSessionJTIOnce(ctx, playerID, jti)
		if err == nil {
			return lease, nil
		}
		// COMMIT 结果不确定必须原样上抛:它不是"事务已回滚"的并发错误,
		// 重放会造成第二次代际推进(把不确定变成确定的多推一代)。
		if IsCommitAmbiguous(err) || !isRetryableTxErr(err) {
			return lease, err
		}
		lastErr = err
		if ctx.Err() != nil {
			break
		}
	}
	return SessionGenerationLease{}, errcode.NewCause(errcode.ErrUnavailable, lastErr,
		"session generation contended (deadlock/lock timeout) after %d attempts; retry login",
		persistMaxAttempts)
}

// persistSessionJTIOnce 执行一次事务:「SELECT ... FOR UPDATE 快照旧值 → upsert(generation+1) →
// 读回本行代际 → COMMIT」。行 X 锁持有到 COMMIT,同事务读回的必然是本次分配的代际,
// 并发登录在此串行化。旧值快照供 Redis 写失败时条件回补(RestoreSessionJTI)。
// 首登竞态(两事务都看到无行)下输家的快照可能缺失刚提交的对手行——该窗口极窄且
// 回补是条件 CAS,最坏退化为删行(= SetRole 缺行兼容路径),下一次成功登录自愈。
//
// 所有加锁/写库语句都用 errcode.NewCause 保留底层 *mysql.MySQLError,
// 否则外层 isRetryableTxErr 沿 Unwrap 链检不出 1213/1205。
func (r *MySQLSessionGenerationRepo) persistSessionJTIOnce(ctx context.Context, playerID uint64, jti string) (SessionGenerationLease, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return SessionGenerationLease{}, errcode.NewCause(errcode.ErrInternal, err,
			"begin session generation tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	lease := SessionGenerationLease{}
	switch err := tx.QueryRowContext(ctx,
		`SELECT sess_jti FROM player_session_generations WHERE player_id = ? FOR UPDATE`,
		playerID).Scan(&lease.PrevJTI); err {
	case nil:
		lease.HadPrev = true
	case sql.ErrNoRows:
		// 首登:无旧行可快照。
	default:
		return SessionGenerationLease{}, errcode.NewCause(errcode.ErrInternal, err,
			"mysql snapshot session jti: %v", err)
	}
	const upsert = `INSERT INTO player_session_generations(player_id, sess_jti, generation) VALUES (?, ?, 1)
ON DUPLICATE KEY UPDATE generation = generation + 1, sess_jti = VALUES(sess_jti)`
	if _, err := tx.ExecContext(ctx, upsert, playerID, jti); err != nil {
		return SessionGenerationLease{}, errcode.NewCause(errcode.ErrInternal, err,
			"mysql persist session jti: %v", err)
	}
	if err := tx.QueryRowContext(ctx,
		`SELECT generation FROM player_session_generations WHERE player_id = ?`,
		playerID).Scan(&lease.Generation); err != nil {
		return SessionGenerationLease{}, errcode.NewCause(errcode.ErrInternal, err,
			"mysql read session generation: %v", err)
	}
	if err := tx.Commit(); err != nil {
		// R11 复审 P0-1 问题 A:COMMIT 可能**已经生效**而只是回包丢了。不猜、不当成
		// 「没提交」——标记为不确定交给 biz 用 jti 读回判定(见 ErrCommitAmbiguous)。
		// 快照(PrevJTI/HadPrev)一并返回:若判定为已提交,后续补偿仍需要它。
		return lease, errcode.NewCause(errcode.ErrInternal,
			fmt.Errorf("%w: %v", ErrCommitAmbiguous, err), "commit session generation: %v", err)
	}
	return lease, nil
}

// LoadSessionGeneration 单语句读回当前行。用于把不确定的 COMMIT 判定成事实。
func (r *MySQLSessionGenerationRepo) LoadSessionGeneration(ctx context.Context, playerID uint64) (string, uint64, bool, error) {
	var jti string
	var generation uint64
	switch err := r.db.QueryRowContext(ctx,
		`SELECT sess_jti, generation FROM player_session_generations WHERE player_id = ?`,
		playerID).Scan(&jti, &generation); err {
	case nil:
		return jti, generation, true, nil
	case sql.ErrNoRows:
		return "", 0, false, nil
	default:
		return "", 0, false, errcode.New(errcode.ErrInternal, "mysql load session generation: %v", err)
	}
}

// RestoreSessionJTI 单语句条件回补:行仍是本次登录写入的 (failedJTI, generation)
// 才把 sess_jti 改回覆盖前的值(无旧行则删行);generation 不回退,单调性不破坏。
// 并发新登录已推进代际时 WHERE 不命中 = no-op。
func (r *MySQLSessionGenerationRepo) RestoreSessionJTI(ctx context.Context, playerID uint64, failedJTI string, lease SessionGenerationLease) (bool, error) {
	if lease.SnapshotUnknown {
		// R11 复审 P0-1:快照不可信时回补两条路都是错的(错误复活上一代 jti / 误删行)。
		// 由调用方改走条件墓碑;这里 fail-closed 拒绝,防止将来有人误接。
		return false, errcode.New(errcode.ErrInternal,
			"session generation restore refused: pre-write snapshot unknown; tombstone instead")
	}
	var res sql.Result
	var err error
	if lease.HadPrev {
		res, err = r.db.ExecContext(ctx,
			`UPDATE player_session_generations SET sess_jti = ? WHERE player_id = ? AND sess_jti = ? AND generation = ?`,
			lease.PrevJTI, playerID, failedJTI, lease.Generation)
	} else {
		res, err = r.db.ExecContext(ctx,
			`DELETE FROM player_session_generations WHERE player_id = ? AND sess_jti = ? AND generation = ?`,
			playerID, failedJTI, lease.Generation)
	}
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "mysql restore session jti: %v", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "mysql restore rows affected: %v", err)
	}
	return n > 0, nil
}

// sessionTombstoneJTI 登出墓碑哨兵值:非 uuid 格式,永不与真实 jti 碰撞。
const sessionTombstoneJTI = "logged-out"

// TombstoneSessionJTI 单语句条件 UPDATE:行仍持有本次登出的 jti 才推代际改哨兵,
// 并发新登录已轮换时 WHERE 不命中 = no-op(不毒化新会话)。
func (r *MySQLSessionGenerationRepo) TombstoneSessionJTI(ctx context.Context, playerID uint64, jti string) (bool, error) {
	res, err := r.db.ExecContext(ctx,
		`UPDATE player_session_generations SET generation = generation + 1, sess_jti = ? WHERE player_id = ? AND sess_jti = ?`,
		sessionTombstoneJTI, playerID, jti)
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "mysql tombstone session jti: %v", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "mysql tombstone rows affected: %v", err)
	}
	return n > 0, nil
}
