// Package data 是 friend 服务的数据层(MySQL 好友图 / 好友请求 / 黑名单,2026-06-15)。
//
// 库表(deploy/mysql-init/06-social-tables.sql,pandora_social 库):
//
//	friendships      双向好友边(每对好友落两行,player_id↔friend_id 各一行,便于 ListFriends)
//	friend_requests  好友请求(PK request_id snowflake,uk requester_id+target_id)
//	blocks           黑名单(uk player_id+blocked_id)
//
// 三张表都是结构化列,直接映射(CLAUDE.md §5.9 关系型表不强制 proto bytes blob)。
// FriendRequestStatus 取值与 proto pandora.friend.v1.FriendRequestStatus 对齐:
// 1=pending / 2=accepted / 3=rejected / 4=expired。
package data

import (
	"context"
	"database/sql"
	"errors"
	"math/rand"
	"strings"

	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
)

// 好友请求状态(与 proto FriendRequestStatus 数值一致)。
const (
	requestStatusPending  = 1
	requestStatusAccepted = 2
	requestStatusRejected = 3
)

// listReadHardLimit 是好友 / 好友申请 / 黑名单列表单次返回的防御性 SQL 上限(不变量 §9.18
// 读取侧「单次返回上限」兜底)。写入侧上限默认 200,正常列表远低于此值;此处取更宽松的硬上限,
// 仅防历史脏数据 / 极端场景下的无界扫描与返回。
const listReadHardLimit = 1000

// friendWriteTxIsolation 说明本文件四条写事务为什么显式用 READ COMMITTED
// (2026-08-11;register_no.go 已有同款先例)。
//
// **这是正确性要求,不是调优。** 本域的所有权威判定读都是 `SELECT ... FOR UPDATE`
// (R9 复审 P1:RR 的一致读快照在第一条普通 SELECT 时固定,守卫锁不刷新它),而这些探针
// 绝大多数**查不到行**(首次申请 / 首次拉黑)。RR 下未命中的锁定读锁的不是"某一行"而是
// **该键所在的间隙**,于是:
//
//	TRX A 持 uk_requester_target 某间隙的 X 锁,等在同一间隙插入;
//	TRX B 持同一间隙的 X 锁,也等在同一间隙插入;   → 1213 死锁
//
// 间隙锁彼此相容,所以 N 个事务都能拿到;冲突发生在随后的 insert intention。真 MySQL 8.4
// 实测:16 个并发申请(**互不相同的 requester 与 target**,没有任何共享行)必炸,
// InnoDB 死锁日志两侧都是 `lock_mode X` + `insert intention waiting`。这一形状**不是锁序
// 问题**,重排守卫顺序解决不了 —— 它们根本没有共享的守卫行。
//
// 为什么降到 RC 是安全的:本域的并发正确性从设计之初就**不依赖 gap 锁**。守卫行
// (`friend_player_guards` / `friend_pair_guards`)之所以存在,正是因为 TiDB 没有 gap 锁、
// `COUNT ... FOR UPDATE` 在零行时一把锁都不加(R5 复审 P1-2)。也就是说:限额的权威性
// 来自守卫行 + 守卫锁内的锁定读,唯一性来自唯一键,两者在 RC 下都成立;RR 的 gap 锁在
// MySQL 侧是**纯多余的副作用**,只贡献死锁。RC 还让锁定读读到最新已提交(比 RR 更"当前"),
// R9 要修的陈旧快照问题只会更稳。
//
// 前置:binlog_format=ROW(MySQL 8.4 默认)。
var _ = friendWriteTxIsolation // 仅作文档锚点,供上面四处 BeginTx 注释指向

const friendWriteTxIsolation = "READ COMMITTED"

// FriendRequestRow 是一行好友请求(data → biz 内部结构,不外泄客户端)。
type FriendRequestRow struct {
	RequestID   uint64
	RequesterID uint64
	TargetID    uint64
	Status      int32
}

// FriendRow 是一条好友关系(friend_id + 成为好友时间,供 biz 组装 FriendInfo)。
type FriendRow struct {
	FriendID uint64
	SinceMs  int64
}

// IncomingRequestRow 是一条「发给本人且仍 pending」的好友请求(供 biz 组装 FriendRequestInfo)。
type IncomingRequestRow struct {
	RequestID   uint64
	RequesterID uint64
	CreatedMs   int64
}

// BlockRow 是一条黑名单条目(被拉黑玩家 + 拉黑时间,供 biz 组装 BlockInfo)。
type BlockRow struct {
	BlockedID uint64
	SinceMs   int64
}

// RecommendRow 是一条推荐好友候选(供 biz 组装 RecommendedFriendInfo)。
// Mutual 是与查询者的共同好友数(FOF 候选 >0;随机兜底候选为 0)。
type RecommendRow struct {
	CandidateID uint64
	Mutual      int
}

// FriendRepo 是 friend 数据层抽象。biz 层只依赖此接口,不依赖 *sql.DB。
type FriendRepo interface {
	// AreFriends 判断 a / b 是否已是好友(查一行即可,双向落库)。
	AreFriends(ctx context.Context, a, b uint64) (bool, error)
	// IsBlocked 判断 a / b 之间是否存在任一方向的拉黑。
	IsBlocked(ctx context.Context, a, b uint64) (bool, error)
	// CountFriends 统计玩家当前好友数(AddFriend 提前失败用,非权威)。
	CountFriends(ctx context.Context, playerID uint64) (int, error)
	// CreateRequest 创建 / 复用好友请求(R5 复审 P1-3:事务内先取 pair 守卫锁并权威复核
	// 拉黑/已好友——biz 预检只是 fail-fast;守卫机制见实现处注释)。
	//   - 无历史 → 用 newRequestID 插入 pending,返回 (newRequestID, false)
	//   - 已有 pending → 复用,返回 (已存在 request_id, true)
	//   - 已有 rejected/expired → 换新 ID 重置为 pending 并刷新 created_at,返回 (newRequestID, false)
	// maxIncoming>0 时:在 target 守卫锁内校验 pending 收件箱数量,新增一条 pending 会超限则
	// 回 ErrFriendRequestLimit(不变量 §9.18;TiDB 无 gap 锁,守卫行替代 COUNT..FOR UPDATE)。
	CreateRequest(ctx context.Context, newRequestID, requesterID, targetID uint64, maxIncoming int) (requestID uint64, reused bool, err error)
	// GetRequest 读好友请求;not found → (nil, false, nil)。
	GetRequest(ctx context.Context, requestID uint64) (*FriendRequestRow, bool, error)
	// AcceptRequest 在一个事务里完成「接受好友请求」的全部权威校验与写入
	// (R5 复审 P1-2/4:锁序 = pair 守卫 → player 守卫升序 → 请求行,与同对 Block/
	// AddFriend 全序;上限 COUNT 在守卫锁内成为权威,TiDB 无 gap 锁也不可穿透):
	//   1. pair/player 守卫锁 → 锁请求行复核仍是 pending;
	//   2. R5 校验:只有请求的 target 本人(accepterID)能接受;
	//   3. block 校验:双方任一方向已拉黑则拒绝;
	//   4. maxFriends > 0 时对 requester / target 双方做好友上限校验;
	//   5. 标记 accepted + 写双向好友边(幂等 INSERT IGNORE)+ 反向 pending 收敛 accepted(P2-8)。
	// 返回 accepted 表示本次调用是否真正把 pending→accepted 并建边:
	//   - accepted=true:本次完成,biz 应推送 REQUEST_ACCEPTED;
	//   - accepted=false, err=nil:请求已被并发处理(Block 改 rejected / 另一次 accept),
	//     biz 不得推送、不得报"成功"(避免假成功)。
	AcceptRequest(ctx context.Context, requestID, accepterID uint64, maxFriends int) (accepted bool, err error)
	// RejectRequest 在事务里拒绝好友请求:锁请求行(FOR UPDATE)→ 校验 target 本人 →
	// 确认仍 pending → 置 rejected。返回 rejected 表示本次是否真正把 pending→rejected:
	//   - rejected=true:本次完成;
	//   - rejected=false, err=nil:请求已被并发处理(已 accept / Block 改 rejected),biz 报找不到。
	RejectRequest(ctx context.Context, requestID, rejecterID uint64) (rejected bool, err error)
	// ListIncomingRequests 列出「发给 playerID 且仍 pending」的好友请求(离线补拉用)。
	ListIncomingRequests(ctx context.Context, playerID uint64) ([]IncomingRequestRow, error)
	// ListFriends 列出玩家的好友(friend_id + since_ms)。
	ListFriends(ctx context.Context, playerID uint64) ([]FriendRow, error)
	// RemoveFriend 删双向好友边(幂等:不存在也不报错)。不动黑名单 / 请求。
	RemoveFriend(ctx context.Context, playerID, targetID uint64) error
	// Block 在一个事务里:取 pair 守卫锁(与 Accept/AddFriend 同对串行化,R5 复审 P1-4)
	// → 写黑名单 + 删双向好友边 + 取消两人之间的 pending 请求。
	// maxBlocks>0 时:playerID 守卫锁内校验黑名单数量,新增会超限则回 ErrFriendBlockLimit
	// (不变量 §9.18;TiDB 无 gap 锁,守卫行替代 COUNT..FOR UPDATE)。
	Block(ctx context.Context, playerID, targetID uint64, maxBlocks int) error
	// Unblock 从黑名单移除(幂等:不存在也不报错)。不自动恢复好友关系。
	Unblock(ctx context.Context, playerID, targetID uint64) error
	// ListBlocks 列出玩家拉黑的人(blocked_id + since_ms)。
	ListBlocks(ctx context.Context, playerID uint64) ([]BlockRow, error)
	// RecommendByMutual 返回「好友的好友」候选,按共同好友数降序、同数随机,取 limit 个。
	// 已排除:自己 / 已是好友 / 任一方向拉黑 / 双方任一方向 pending 请求 / exclude 列表。
	RecommendByMutual(ctx context.Context, playerID uint64, exclude []uint64, limit int) ([]RecommendRow, error)
	// RecommendRandom 从好友图里的玩家随机兜底,返回至多 limit 个候选;尾部不足时可少于 limit。
	// 排除条件同上。
	RecommendRandom(ctx context.Context, playerID uint64, exclude []uint64, limit int) ([]RecommendRow, error)
	// SweepTerminalRequestsBefore 处理终态(accepted/rejected/expired)且 updated_at 超保留期的
	// 好友请求行(保留期清理,§9.24)。
	// **mode 默认 ModeReportOnly:只统计待清理量并 WARN 告警,一行都不删**(用户指令);
	// pending 无论如何都不在处理范围。真删语义:删后再次发起等价于全新请求
	// (好友关系权威在 friendships,请求行无资产语义)。
	SweepTerminalRequestsBefore(ctx context.Context, mode dbguard.Mode, retentionDays, limit int) (dbguard.Outcome, error)
	// DeletePairGuardsBefore 删 created_at 超保留期的关系对守卫行(R9 复审 P1:pair 守卫
	// 随社交图 O(n²) 累积无上界,§9.24 不能豁免)。守卫行仅锁载体,任意时刻删除安全:
	// 正被事务持有的行锁会阻塞 DELETE 到提交,下次 acquirePairGuard 重新 INSERT。
	DeletePairGuardsBefore(ctx context.Context, retentionDays, limit int) (int64, error)
}

// MySQLFriendRepo 是基于 database/sql 的 FriendRepo 实现。
type MySQLFriendRepo struct {
	db *sql.DB
}

// NewMySQLFriendRepo 构造。db 由 pkg/mysqlx.MustNewClient 提供(连 pandora_social 库)。
func NewMySQLFriendRepo(db *sql.DB) *MySQLFriendRepo {
	return &MySQLFriendRepo{db: db}
}

func (r *MySQLFriendRepo) AreFriends(ctx context.Context, a, b uint64) (bool, error) {
	const q = `SELECT 1 FROM friendships WHERE player_id = ? AND friend_id = ? LIMIT 1`
	var x int
	err := r.db.QueryRowContext(ctx, q, a, b).Scan(&x)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "query friendship %d-%d: %v", a, b, err)
	}
	return true, nil
}

func (r *MySQLFriendRepo) IsBlocked(ctx context.Context, a, b uint64) (bool, error) {
	const q = `SELECT 1 FROM blocks
WHERE (player_id = ? AND blocked_id = ?) OR (player_id = ? AND blocked_id = ?) LIMIT 1`
	var x int
	err := r.db.QueryRowContext(ctx, q, a, b, b, a).Scan(&x)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "query block %d-%d: %v", a, b, err)
	}
	return true, nil
}

func (r *MySQLFriendRepo) CountFriends(ctx context.Context, playerID uint64) (int, error) {
	const q = `SELECT COUNT(*) FROM friendships WHERE player_id = ?`
	var n int
	if err := r.db.QueryRowContext(ctx, q, playerID).Scan(&n); err != nil {
		return 0, errcode.New(errcode.ErrInternal, "count friends %d: %v", playerID, err)
	}
	return n, nil
}

func (r *MySQLFriendRepo) CreateRequest(ctx context.Context, newRequestID, requesterID, targetID uint64, maxIncoming int) (uint64, bool, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}) // RC 而非默认 RR,理由见 friendWriteTxIsolation
	if err != nil {
		return 0, false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// pair 守卫(R5 复审 P1-3):与同对的 Block/Accept 串行化。biz 层的拉黑/已好友预检
	// 在事务外只是 fail-fast,并发 Block/Accept 可在预检后落库;守卫锁内的复核才是权威,
	// 消除「已拉黑+pending」「已好友+pending」交错。
	//
	// R9 复审 P1(快照隔离):InnoDB RR 下事务的一致读快照在**第一条普通 SELECT**时
	// 固定,守卫锁(INSERT/FOR UPDATE 是当前读)不会刷新它——守卫锁内的普通 SELECT 仍
	// 可能读到守卫获取前的陈旧快照,串行化被架空。因此本事务内所有权威判定读
	//(block/好友存在性探针、限额 COUNT)一律用锁定读(FOR UPDATE = 当前读,InnoDB
	// 与 TiDB 悲观事务都读最新已提交);写者已被守卫串行化,锁冲突面可控。
	if gerr := acquirePairGuard(ctx, tx, requesterID, targetID); gerr != nil {
		return 0, false, gerr
	}
	// player 守卫必须在**任何锁定读之前**取(2026-08-11,真 MySQL 8.4 实测 1213 死锁)。
	//
	// 原实现把它放在 checkIncomingLimit 里(即三条 FOR UPDATE 探针之后),依据是
	// 「本事务持有的行锁只属于本 pair,与只共享单个玩家的其它事务无共同行」——**这条前提
	// 不成立**:探针查的行绝大多数不存在(首次申请),InnoDB RR 下 `FOR UPDATE` 未命中
	// 记录时锁的不是"本 pair 的行"而是**该键所在的间隙**,N 个不同 requester 指向同一
	// target 时全部落在 `uk_requester_target` 的同一个 supremum 间隙里,于是"只属于本 pair"
	// 的假设被打穿。InnoDB 死锁日志逐字印证了这个环:
	//
	//   TRX A 持 friend_requests.uk_requester_target supremum 的 X 间隙锁,等 guards 主键行;
	//   TRX B 持 guards 主键行,等 friend_requests 同一间隙的 insert intention。
	//
	// 间隙锁彼此相容(N 个事务都能拿到),真正的排他点是 guards 行——谁先拿到谁就要去插
	// friend_requests,而插入意向被其余事务尚未释放的间隙锁挡住,形成环。8 个并发申请必炸,
	// 且**只在 MySQL 上炸**(TiDB 无 gap 锁,所以 TiDB 侧一直是绿的,双后端跑才看得见)。
	//
	// 修法是把守卫提到探针之前:所有间隙锁都在守卫的串行化**内部**取得,同一 target 上
	// 同时最多一个事务持有这些间隙锁,环不成立。守卫锁序(pair → player)保持不变,
	// AcceptRequest / Block 也是这个顺序,不引入新的跨路径反序。
	//
	// 无条件取(不再受 maxIncoming>0 门控):间隙锁的暴露与限额是否开启无关,
	// 关掉限额不该把锁序纪律一起关掉。
	if gerr := acquirePlayerGuard(ctx, tx, targetID); gerr != nil {
		return 0, false, gerr
	}
	var probeX int
	berr := tx.QueryRowContext(ctx,
		`SELECT 1 FROM blocks
WHERE (player_id = ? AND blocked_id = ?) OR (player_id = ? AND blocked_id = ?) LIMIT 1 FOR UPDATE`,
		requesterID, targetID, targetID, requesterID).Scan(&probeX)
	if berr == nil {
		return 0, false, errcode.New(errcode.ErrFriendBlocked, "blocked between %d and %d", requesterID, targetID)
	}
	if !errors.Is(berr, sql.ErrNoRows) {
		return 0, false, errcode.New(errcode.ErrInternal, "check block %d-%d: %v", requesterID, targetID, berr)
	}
	ferr := tx.QueryRowContext(ctx,
		`SELECT 1 FROM friendships WHERE player_id = ? AND friend_id = ? LIMIT 1 FOR UPDATE`,
		requesterID, targetID).Scan(&probeX)
	if ferr == nil {
		return 0, false, errcode.New(errcode.ErrFriendAlreadyAdded, "already friends: %d-%d", requesterID, targetID)
	}
	if !errors.Is(ferr, sql.ErrNoRows) {
		return 0, false, errcode.New(errcode.ErrInternal, "check friendship %d-%d: %v", requesterID, targetID, ferr)
	}

	// 请求行锁在两把守卫之后取。**这里曾经写着"行锁只属于本 pair 故锁序安全"并把
	// player 守卫排在后面 —— 那是本文件 2026-08-11 修掉的死锁根因**,原因见上方守卫段:
	// 未命中的 FOR UPDATE 锁的是间隙不是行,间隙是跨 pair 共享的。
	var existingID uint64
	var status int32
	err = tx.QueryRowContext(ctx,
		`SELECT request_id, status FROM friend_requests
WHERE requester_id = ? AND target_id = ? FOR UPDATE`, requesterID, targetID).Scan(&existingID, &status)

	switch {
	case errors.Is(err, sql.ErrNoRows):
		// 无历史请求 → 新增一条 pending 前,先校验 target 收件箱未满(不变量 §9.18)。
		if ierr := checkIncomingLimit(ctx, tx, targetID, maxIncoming); ierr != nil {
			return 0, false, ierr
		}
		if _, ierr := tx.ExecContext(ctx,
			`INSERT INTO friend_requests (request_id, requester_id, target_id, status)
VALUES (?, ?, ?, ?)`, newRequestID, requesterID, targetID, requestStatusPending); ierr != nil {
			return 0, false, errcode.New(errcode.ErrInternal, "insert request %d->%d: %v", requesterID, targetID, ierr)
		}
		if cerr := tx.Commit(); cerr != nil {
			return 0, false, errcode.New(errcode.ErrInternal, "commit: %v", cerr)
		}
		return newRequestID, false, nil

	case err != nil:
		return 0, false, errcode.New(errcode.ErrInternal, "lock request %d->%d: %v", requesterID, targetID, err)

	default:
		// 已有历史请求
		if status == requestStatusPending {
			// 复用现有 pending,不改库
			if cerr := tx.Commit(); cerr != nil {
				return 0, false, errcode.New(errcode.ErrInternal, "commit: %v", cerr)
			}
			return existingID, true, nil
		}
		// rejected/expired/accepted → 复用旧行重置为 pending,但换**新 request_id**
		// (R4 复审 P1-4:推送为 at-least-once,客户端按 (request_id, reason) 判重;
		// 复用旧 ID 会让「申请→拒绝→再次申请」的新推送被当成重投丢弃。新一次申请
		// 是新事件,必须携带新 ID;旧 ID 随之失效,迟到的旧 Accept 自然查无此请求)。
		// 从非 pending 转 pending 也会占用 target 收件箱一格,故同样校验上限(§9.18)。
		if ierr := checkIncomingLimit(ctx, tx, targetID, maxIncoming); ierr != nil {
			return 0, false, ierr
		}
		// created_at 一并刷新(R5 复审 P2-7):这是**新一次申请**(新 request_id/新事件),
		// 列表按 created_at 展示与排序,沿用首次申请时间会把重新申请排到陈旧位置。
		if _, uerr := tx.ExecContext(ctx,
			`UPDATE friend_requests SET request_id = ?, status = ?, created_at = NOW(), updated_at = NOW() WHERE request_id = ?`,
			newRequestID, requestStatusPending, existingID); uerr != nil {
			return 0, false, errcode.New(errcode.ErrInternal, "reset request %d: %v", existingID, uerr)
		}
		if cerr := tx.Commit(); cerr != nil {
			return 0, false, errcode.New(errcode.ErrInternal, "commit: %v", cerr)
		}
		return newRequestID, false, nil
	}
}

func (r *MySQLFriendRepo) GetRequest(ctx context.Context, requestID uint64) (*FriendRequestRow, bool, error) {
	const q = `SELECT request_id, requester_id, target_id, status
FROM friend_requests WHERE request_id = ? LIMIT 1`
	row := &FriendRequestRow{}
	err := r.db.QueryRowContext(ctx, q, requestID).Scan(
		&row.RequestID, &row.RequesterID, &row.TargetID, &row.Status)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "query request %d: %v", requestID, err)
	}
	return row, true, nil
}

func (r *MySQLFriendRepo) AcceptRequest(ctx context.Context, requestID, accepterID uint64, maxFriends int) (bool, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}) // RC 而非默认 RR,理由见 friendWriteTxIsolation
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 0. 预读请求行(不加锁)只为得到 pair 身份 —— 守卫锁必须先于业务行锁(锁序纪律),
	//    而 pair 守卫的 key 需要 requester/target。行内容随后在守卫锁内重读复核。
	var requesterID, targetID uint64
	var status int32
	err = tx.QueryRowContext(ctx,
		`SELECT requester_id, target_id, status FROM friend_requests
WHERE request_id = ?`, requestID).Scan(&requesterID, &targetID, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, errcode.New(errcode.ErrFriendNotFound, "request not found: %d", requestID)
	}
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "probe request %d: %v", requestID, err)
	}
	if targetID != accepterID {
		return false, errcode.New(errcode.ErrFriendNotFound, "request %d not for %d", requestID, accepterID)
	}

	// 1. pair 守卫(R5 复审 P1-4):与同对 Block/AddFriend 串行化。旧实现 Accept 只锁
	//    请求行,Block 的「插黑名单+删好友边」可与 Accept 的「查无 block → 插好友边」
	//    交错(Block 的删边跑在 Accept 插边之前、其 pending 更新在请求行锁上等待),
	//    两笔都提交后形成「既好友又拉黑」。pair 守卫下两者全序,任一先行另一必见其果。
	if gerr := acquirePairGuard(ctx, tx, requesterID, targetID); gerr != nil {
		return false, gerr
	}
	// 2. player 守卫升序(R5 复审 P1-2):TiDB 无 gap 锁,原 COUNT ... FOR UPDATE 挡不住
	//    并发 accept 对同一玩家的建边插入;守卫锁内的 COUNT 才是权威上限判定。
	if maxFriends > 0 {
		loID, hiID := requesterID, targetID
		if hiID < loID {
			loID, hiID = hiID, loID
		}
		for _, pid := range [...]uint64{loID, hiID} {
			if gerr := acquirePlayerGuard(ctx, tx, pid); gerr != nil {
				return false, gerr
			}
		}
	}

	// 3. 守卫锁内锁请求行并复核:预读与取锁之间行可能已被并发处理
	//    (Block 置 rejected / 另一次 accept / 重新申请轮换 request_id → 查无此行)。
	err = tx.QueryRowContext(ctx,
		`SELECT requester_id, target_id, status FROM friend_requests
WHERE request_id = ? FOR UPDATE`, requestID).Scan(&requesterID, &targetID, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil // request_id 已被重新申请轮换:本次未完成 pending→accepted
	}
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "lock request %d: %v", requestID, err)
	}
	if targetID != accepterID {
		return false, errcode.New(errcode.ErrFriendNotFound, "request %d not for %d", requestID, accepterID)
	}
	if status != requestStatusPending {
		return false, nil
	}

	// 4. block 权威校验。pair 守卫串行化了写者;读侧必须用锁定读(R9 复审 P1):
	// 步骤 0 的普通预读已把 RR 快照固定在守卫获取之前,普通 SELECT 会读陈旧快照
	//(看不到守卫等待期间提交的 Block);FOR UPDATE 是当前读,两库都读最新已提交。
	var blockedX int
	berr := tx.QueryRowContext(ctx,
		`SELECT 1 FROM blocks
WHERE (player_id = ? AND blocked_id = ?) OR (player_id = ? AND blocked_id = ?) LIMIT 1 FOR UPDATE`,
		accepterID, requesterID, requesterID, accepterID).Scan(&blockedX)
	if berr == nil {
		return false, errcode.New(errcode.ErrFriendBlocked, "blocked between %d and %d", accepterID, requesterID)
	}
	if !errors.Is(berr, sql.ErrNoRows) {
		return false, errcode.New(errcode.ErrInternal, "query block %d-%d: %v", accepterID, requesterID, berr)
	}

	// 5. 好友上限权威校验。player 守卫串行化写者;COUNT 用锁定读拿当前读
	//(R9 复审 P1:普通 COUNT 在 RR 陈旧快照下会漏计守卫等待期间提交的建边)。
	if maxFriends > 0 {
		for _, pid := range [...]uint64{requesterID, targetID} {
			var cnt int
			if cerr := tx.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM friendships WHERE player_id = ? FOR UPDATE`, pid).Scan(&cnt); cerr != nil {
				return false, errcode.New(errcode.ErrInternal, "count friends %d: %v", pid, cerr)
			}
			if cnt >= maxFriends {
				return false, errcode.New(errcode.ErrFriendLimit,
					"friend limit reached for %d (max %d)", pid, maxFriends)
			}
		}
	}

	if _, uerr := tx.ExecContext(ctx,
		`UPDATE friend_requests SET status = ?, updated_at = NOW() WHERE request_id = ?`,
		requestStatusAccepted, requestID); uerr != nil {
		return false, errcode.New(errcode.ErrInternal, "accept request %d: %v", requestID, uerr)
	}

	// 写双向好友边(幂等:重复 accept 不报错)
	const insFriend = `INSERT IGNORE INTO friendships (player_id, friend_id) VALUES (?, ?)`
	if _, ferr := tx.ExecContext(ctx, insFriend, requesterID, targetID); ferr != nil {
		return false, errcode.New(errcode.ErrInternal, "insert friendship %d->%d: %v", requesterID, targetID, ferr)
	}
	if _, ferr := tx.ExecContext(ctx, insFriend, targetID, requesterID); ferr != nil {
		return false, errcode.New(errcode.ErrInternal, "insert friendship %d->%d: %v", targetID, requesterID, ferr)
	}

	// 6. 反向 pending 一并终结(R5 复审 P2-8):A→B 与 B→A 可各自 pending;本次接受已让
	//    双方成为好友,反向申请的结果同样是"好友已建立",按 accepted 收敛(同一 pair
	//    守卫内原子)。否则残留的反向 pending 被接受时会对已好友重复走建边流程。
	if _, uerr := tx.ExecContext(ctx,
		`UPDATE friend_requests SET status = ?, updated_at = NOW()
WHERE requester_id = ? AND target_id = ? AND status = ?`,
		requestStatusAccepted, targetID, requesterID, requestStatusPending); uerr != nil {
		return false, errcode.New(errcode.ErrInternal, "resolve reverse pending %d->%d: %v", targetID, requesterID, uerr)
	}

	if cerr := tx.Commit(); cerr != nil {
		return false, errcode.New(errcode.ErrInternal, "commit request %d: %v", requestID, cerr)
	}
	return true, nil
}

func (r *MySQLFriendRepo) RejectRequest(ctx context.Context, requestID, rejecterID uint64) (bool, error) {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}) // RC 而非默认 RR,理由见 friendWriteTxIsolation
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 锁请求行,确认仍是 pending(防并发:accept / 另一次 reject / Block 改状态)
	var targetID uint64
	var status int32
	err = tx.QueryRowContext(ctx,
		`SELECT target_id, status FROM friend_requests
WHERE request_id = ? FOR UPDATE`, requestID).Scan(&targetID, &status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, errcode.New(errcode.ErrFriendNotFound, "request not found: %d", requestID)
	}
	if err != nil {
		return false, errcode.New(errcode.ErrInternal, "lock request %d: %v", requestID, err)
	}
	// R5 权威校验:只有请求的 target 本人能拒绝(放进事务,杜绝 TOCTOU)
	if targetID != rejecterID {
		return false, errcode.New(errcode.ErrFriendNotFound, "request %d not for %d", requestID, rejecterID)
	}
	// 并发下请求已被处理(accept / Block)→ 本次未真正完成 pending→rejected
	if status != requestStatusPending {
		return false, nil
	}

	if _, uerr := tx.ExecContext(ctx,
		`UPDATE friend_requests SET status = ?, updated_at = NOW() WHERE request_id = ?`,
		requestStatusRejected, requestID); uerr != nil {
		return false, errcode.New(errcode.ErrInternal, "reject request %d: %v", requestID, uerr)
	}
	if cerr := tx.Commit(); cerr != nil {
		return false, errcode.New(errcode.ErrInternal, "commit reject %d: %v", requestID, cerr)
	}
	return true, nil
}

func (r *MySQLFriendRepo) ListIncomingRequests(ctx context.Context, playerID uint64) ([]IncomingRequestRow, error) {
	const q = `SELECT request_id, requester_id, UNIX_TIMESTAMP(created_at)*1000
FROM friend_requests WHERE target_id = ? AND status = ? ORDER BY created_at DESC LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, playerID, requestStatusPending, listReadHardLimit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query incoming requests player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []IncomingRequestRow
	for rows.Next() {
		var rr IncomingRequestRow
		if serr := rows.Scan(&rr.RequestID, &rr.RequesterID, &rr.CreatedMs); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan incoming request player=%d: %v", playerID, serr)
		}
		out = append(out, rr)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate incoming requests player=%d: %v", playerID, rerr)
	}
	return out, nil
}

func (r *MySQLFriendRepo) ListFriends(ctx context.Context, playerID uint64) ([]FriendRow, error) {
	const q = `SELECT friend_id, UNIX_TIMESTAMP(created_at)*1000
FROM friendships WHERE player_id = ? ORDER BY created_at DESC LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, playerID, listReadHardLimit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query friends player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []FriendRow
	for rows.Next() {
		var fr FriendRow
		if serr := rows.Scan(&fr.FriendID, &fr.SinceMs); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan friend player=%d: %v", playerID, serr)
		}
		out = append(out, fr)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate friends player=%d: %v", playerID, rerr)
	}
	return out, nil
}

func (r *MySQLFriendRepo) Block(ctx context.Context, playerID, targetID uint64, maxBlocks int) error {
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted}) // RC 而非默认 RR,理由见 friendWriteTxIsolation
	if err != nil {
		return errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	defer func() { _ = tx.Rollback() }()

	// pair 守卫(R5 复审 P1-4):与同对 Accept/AddFriend 串行化 —— 消除「Accept 查无
	// block 后、本事务插 block/删空边、Accept 再插边」交错出的「既好友又拉黑」。
	if gerr := acquirePairGuard(ctx, tx, playerID, targetID); gerr != nil {
		return gerr
	}

	// 0. 黑名单上限校验(不变量 §9.18):先确认未重复拉黑(幂等命中不占新名额)再校量。
	// 探针/COUNT 用锁定读(R9 复审 P1:普通 SELECT 在 RR 陈旧快照下会漏看守卫等待期间提交的写)。
	//
	// player 守卫**必须在存在性探针之前**取(2026-08-11,与 CreateRequest 同一根因):
	// 探针未命中时锁的是 `blocks` 的间隙而非某一行,同一玩家并发拉黑多个目标时,16 个事务
	// 共享同一间隙、又都要抢同一把守卫 → 谁抢到守卫谁去 INSERT,插入意向被其余事务的
	// 间隙锁挡住成环。原实现只在"新拉黑"分支里取守卫,那时间隙锁已在手,太晚了。
	// 无条件取(不再受 maxBlocks>0 与"是否新拉黑"门控):锁序纪律与限额开关无关。
	if gerr := acquirePlayerGuard(ctx, tx, playerID); gerr != nil {
		return gerr
	}
	if maxBlocks > 0 {
		var existsX int
		eerr := tx.QueryRowContext(ctx,
			`SELECT 1 FROM blocks WHERE player_id = ? AND blocked_id = ? LIMIT 1 FOR UPDATE`, playerID, targetID).Scan(&existsX)
		if eerr != nil && !errors.Is(eerr, sql.ErrNoRows) {
			return errcode.New(errcode.ErrInternal, "check block %d->%d: %v", playerID, targetID, eerr)
		}
		if errors.Is(eerr, sql.ErrNoRows) {
			// 新拉黑 → 守卫锁内的**锁定读** COUNT 即权威,防并发超限
			// (R5 复审 P1-2:TiDB 无 gap 锁,原 COUNT ... FOR UPDATE 挡不住并发插入)。
			var cnt int
			if cerr := tx.QueryRowContext(ctx,
				`SELECT COUNT(*) FROM blocks WHERE player_id = ? FOR UPDATE`, playerID).Scan(&cnt); cerr != nil {
				return errcode.New(errcode.ErrInternal, "count blocks %d: %v", playerID, cerr)
			}
			if cnt >= maxBlocks {
				return errcode.New(errcode.ErrFriendBlockLimit,
					"block limit reached for %d (max %d)", playerID, maxBlocks)
			}
		}
	}

	// 1. 写黑名单(幂等)
	if _, berr := tx.ExecContext(ctx,
		`INSERT IGNORE INTO blocks (player_id, blocked_id) VALUES (?, ?)`, playerID, targetID); berr != nil {
		return errcode.New(errcode.ErrInternal, "insert block %d->%d: %v", playerID, targetID, berr)
	}

	// 2. 删双向好友边
	if _, derr := tx.ExecContext(ctx,
		`DELETE FROM friendships WHERE (player_id = ? AND friend_id = ?) OR (player_id = ? AND friend_id = ?)`,
		playerID, targetID, targetID, playerID); derr != nil {
		return errcode.New(errcode.ErrInternal, "delete friendship %d-%d: %v", playerID, targetID, derr)
	}

	// 3. 取消两人之间的 pending 请求(任一方向)
	if _, rerr := tx.ExecContext(ctx,
		`UPDATE friend_requests SET status = ?, updated_at = NOW()
WHERE status = ? AND ((requester_id = ? AND target_id = ?) OR (requester_id = ? AND target_id = ?))`,
		requestStatusRejected, requestStatusPending, playerID, targetID, targetID, playerID); rerr != nil {
		return errcode.New(errcode.ErrInternal, "cancel pending requests %d-%d: %v", playerID, targetID, rerr)
	}

	if cerr := tx.Commit(); cerr != nil {
		return errcode.New(errcode.ErrInternal, "commit block %d->%d: %v", playerID, targetID, cerr)
	}
	return nil
}

func (r *MySQLFriendRepo) RemoveFriend(ctx context.Context, playerID, targetID uint64) error {
	// 删双向好友边(幂等:删不到行不报错)。单条 DELETE 覆盖两个方向,天然原子。
	if _, derr := r.db.ExecContext(ctx,
		`DELETE FROM friendships WHERE (player_id = ? AND friend_id = ?) OR (player_id = ? AND friend_id = ?)`,
		playerID, targetID, targetID, playerID); derr != nil {
		return errcode.New(errcode.ErrInternal, "remove friendship %d-%d: %v", playerID, targetID, derr)
	}
	return nil
}

func (r *MySQLFriendRepo) Unblock(ctx context.Context, playerID, targetID uint64) error {
	// 从黑名单移除(幂等:删不到行不报错)。不自动恢复好友关系。
	if _, derr := r.db.ExecContext(ctx,
		`DELETE FROM blocks WHERE player_id = ? AND blocked_id = ?`, playerID, targetID); derr != nil {
		return errcode.New(errcode.ErrInternal, "unblock %d->%d: %v", playerID, targetID, derr)
	}
	return nil
}

func (r *MySQLFriendRepo) ListBlocks(ctx context.Context, playerID uint64) ([]BlockRow, error) {
	const q = `SELECT blocked_id, UNIX_TIMESTAMP(created_at)*1000
FROM blocks WHERE player_id = ? ORDER BY created_at DESC LIMIT ?`
	rows, err := r.db.QueryContext(ctx, q, playerID, listReadHardLimit)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "query blocks player=%d: %v", playerID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []BlockRow
	for rows.Next() {
		var br BlockRow
		if serr := rows.Scan(&br.BlockedID, &br.SinceMs); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan block player=%d: %v", playerID, serr)
		}
		out = append(out, br)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate blocks player=%d: %v", playerID, rerr)
	}
	return out, nil
}

// ── 守卫行(R5 复审 P1-2/3/4,2026-07-22)────────────────────────────────────────
//
// TiDB 悲观事务没有 gap/next-key 锁(deploy/tidb-init/01-social-tidb.sql §3.5):
// 原 `COUNT(*) ... FOR UPDATE` 只锁存在的行,挡不住并发 INSERT 幻读,好友数/黑名单数/
// 申请收件箱三类上限都可被并发穿透;Accept/Block/AddFriend 之间也缺 pair 级串行化
// (可并发形成「既好友又拉黑」「已拉黑+pending」「已好友+pending」)。
//
// 修复:所有限额校验与关系变更先取守卫行悲观锁,再在串行化临界区内做一致性 COUNT
// 与检查/写入。`INSERT ... ON DUPLICATE KEY UPDATE <pk>=<pk>` 一条语句完成「不存在则
// 建行、存在则锁行」,锁持有到事务结束;对已存在行的点锁在 MySQL InnoDB 与 TiDB 悲观
// 事务下语义一致(TiDB 缺的是范围锁,点锁不缺)。
//
// 锁序纪律(防死锁):pair 守卫 → player 守卫(升序) → 业务行(请求行等)。
// 单事务至多持有一个 pair 守卫;player 守卫恒按 player_id 升序获取。

// acquirePairGuard 取关系对守卫行锁(lo/hi 规范化,双向同一把锁)。
func acquirePairGuard(ctx context.Context, tx *sql.Tx, a, b uint64) error {
	lo, hi := a, b
	if hi < lo {
		lo, hi = hi, lo
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO friend_pair_guards (lo_id, hi_id) VALUES (?, ?)
ON DUPLICATE KEY UPDATE lo_id = lo_id`, lo, hi); err != nil {
		return errcode.New(errcode.ErrInternal, "acquire pair guard %d-%d: %v", lo, hi, err)
	}
	return nil
}

// acquirePlayerGuard 取单玩家守卫行锁(该玩家限额域的写串行化)。
func acquirePlayerGuard(ctx context.Context, tx *sql.Tx, playerID uint64) error {
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO friend_player_guards (player_id) VALUES (?)
ON DUPLICATE KEY UPDATE player_id = player_id`, playerID); err != nil {
		return errcode.New(errcode.ErrInternal, "acquire player guard %d: %v", playerID, err)
	}
	return nil
}

// checkIncomingLimit 在 CreateRequest 事务内校验 target 的「收到的待处理好友申请」数量上限
// (不变量 §9.18)。maxIncoming<=0 关闭校验;否则先锁 target 守卫行(R5 复审 P1-2:
// TiDB 无 gap 锁,原 COUNT ... FOR UPDATE 挡不住并发插入),守卫锁内的 COUNT 即权威,
// 达到上限回 ErrFriendRequestLimit。COUNT 用锁定读拿当前读(R9 复审 P1:普通 COUNT
// 在 RR 陈旧快照下会漏计守卫等待期间提交的 pending)。
func checkIncomingLimit(ctx context.Context, tx *sql.Tx, targetID uint64, maxIncoming int) error {
	if maxIncoming <= 0 {
		return nil
	}
	// 前置条件:调用方**已在任何锁定读之前**取得 target 的 player 守卫。
	// 这里刻意不再自取——2026-08-11 的 1213 死锁根因正是"守卫在此处才取",
	// 那时三条 FOR UPDATE 探针的间隙锁已经拿在手里,取得再早也来不及(见 CreateRequest 注释)。
	var cnt int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM friend_requests WHERE target_id = ? AND status = ? FOR UPDATE`,
		targetID, requestStatusPending).Scan(&cnt); err != nil {
		return errcode.New(errcode.ErrInternal, "count incoming requests %d: %v", targetID, err)
	}
	if cnt >= maxIncoming {
		return errcode.New(errcode.ErrFriendRequestLimit,
			"incoming friend request limit reached for %d (max %d)", targetID, maxIncoming)
	}
	return nil
}

// excludeClause 把 exclude 列表拼成 ` AND <col> NOT IN (?,?,...)`,并返回对应参数。
// exclude 为空时返回空字符串 + nil,调用方原样拼到 SQL 即可。
func excludeClause(col string, exclude []uint64) (string, []any) {
	if len(exclude) == 0 {
		return "", nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(exclude)), ",")
	args := make([]any, 0, len(exclude))
	for _, id := range exclude {
		args = append(args, id)
	}
	return " AND " + col + " NOT IN (" + ph + ")", args
}

func (r *MySQLFriendRepo) RecommendByMutual(ctx context.Context, playerID uint64, exclude []uint64, limit int) ([]RecommendRow, error) {
	// 好友的好友(FOF):f1 是我的好友,f2.friend_id 是我好友的好友。
	// 排除:我自己 / 已是我好友 / 双向拉黑 / 双向 pending 请求 / exclude 列表。
	// 按共同好友数降序、同数随机,取 limit 个。
	exCl, exArgs := excludeClause("f2.friend_id", exclude)
	q := `SELECT f2.friend_id, COUNT(*) AS mutual
FROM friendships f1
JOIN friendships f2 ON f1.friend_id = f2.player_id
WHERE f1.player_id = ?
  AND f2.friend_id <> ?
  AND f2.friend_id NOT IN (SELECT friend_id FROM friendships WHERE player_id = ?)
  AND NOT EXISTS (SELECT 1 FROM blocks b
        WHERE (b.player_id = ? AND b.blocked_id = f2.friend_id)
           OR (b.player_id = f2.friend_id AND b.blocked_id = ?))
  AND NOT EXISTS (SELECT 1 FROM friend_requests r
        WHERE r.status = 1
          AND ((r.requester_id = ? AND r.target_id = f2.friend_id)
            OR (r.requester_id = f2.friend_id AND r.target_id = ?)))` + exCl + `
GROUP BY f2.friend_id
ORDER BY mutual DESC, RAND()
LIMIT ?`
	args := []any{playerID, playerID, playerID, playerID, playerID, playerID, playerID}
	args = append(args, exArgs...)
	args = append(args, limit)
	return r.scanRecommend(ctx, "fof", playerID, q, args)
}

func (r *MySQLFriendRepo) RecommendRandom(ctx context.Context, playerID uint64, exclude []uint64, limit int) ([]RecommendRow, error) {
	// 兜底:FOF 不足时尽量补候选。绝不全表扫,且 pivot 必须落进真实 id 区间。
	// player_id 是雪花 ID(集中在当前高位窗口),直接 rand.Int63() 大概率落在现有最大 id 之后;
	// 先用索引 MIN/MAX(MySQL 直接取 idx_player 两端,O(1))取真实区间,
	// pivot ∈ [min,max] 保证 player_id>=pivot 必有行;尾部不满返回偏少可接受。
	var minID, maxID sql.Null[uint64]
	err := r.db.QueryRowContext(ctx,
		`SELECT MIN(player_id), MAX(player_id) FROM friendships`).Scan(&minID, &maxID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "recommend range player=%d: %v", playerID, err)
	}
	if !maxID.Valid || maxID.V == 0 {
		return nil, nil // 空表,无兜底候选
	}
	lo, hi := minID.V, maxID.V
	pivot := lo
	if hi > lo {
		pivot = lo + uint64(rand.Int63n(int64(hi-lo+1)))
	}
	return r.recommendAnchor(ctx, playerID, exclude, pivot, limit)
}

// recommendAnchor 沿 player_id 索引从 pivot 正向扫,LIMIT limit 命中即止(有界索引区间,绝不全表)。
// 排除:自己 / 已是好友 / 双向拉黑 / 双向 pending / exclude;mutual 恒 0。
func (r *MySQLFriendRepo) recommendAnchor(ctx context.Context, playerID uint64, exclude []uint64, pivot uint64, limit int) ([]RecommendRow, error) {
	exCl, exArgs := excludeClause("player_id", exclude)
	q := `SELECT player_id, 0 AS mutual
FROM friendships
WHERE player_id >= ?
  AND player_id <> ?
  AND player_id NOT IN (SELECT friend_id FROM friendships WHERE player_id = ?)
  AND NOT EXISTS (SELECT 1 FROM blocks b
        WHERE (b.player_id = ? AND b.blocked_id = friendships.player_id)
           OR (b.player_id = friendships.player_id AND b.blocked_id = ?))
  AND NOT EXISTS (SELECT 1 FROM friend_requests r
        WHERE r.status = 1
          AND ((r.requester_id = ? AND r.target_id = friendships.player_id)
            OR (r.requester_id = friendships.player_id AND r.target_id = ?)))` + exCl + `
GROUP BY player_id
ORDER BY player_id
LIMIT ?`
	args := []any{pivot, playerID, playerID, playerID, playerID, playerID, playerID}
	args = append(args, exArgs...)
	args = append(args, limit)
	return r.scanRecommend(ctx, "random", playerID, q, args)
}

func (r *MySQLFriendRepo) scanRecommend(ctx context.Context, kind string, playerID uint64, q string, args []any) ([]RecommendRow, error) {
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "recommend %s player=%d: %v", kind, playerID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []RecommendRow
	for rows.Next() {
		var rr RecommendRow
		if serr := rows.Scan(&rr.CandidateID, &rr.Mutual); serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan recommend %s player=%d: %v", kind, playerID, serr)
		}
		out = append(out, rr)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate recommend %s player=%d: %v", kind, playerID, rerr)
	}
	return out, nil
}

// DeleteTerminalRequestsBefore 删终态且超保留期的好友请求行(保留期清理,§9.24)。
// 条件走 idx_status_updated(status, updated_at);pending(=1)永不匹配。
// 多副本并发调用安全(各删各的行)。
func (r *MySQLFriendRepo) SweepTerminalRequestsBefore(ctx context.Context, mode dbguard.Mode, retentionDays, limit int) (dbguard.Outcome, error) {
	out, err := dbguard.SweepTable(ctx, r.db, mode, "pandora_social", "friend_requests",
		"status <> ? AND updated_at < DATE_SUB(NOW(), INTERVAL ? DAY)", limit, requestStatusPending, retentionDays)
	if err != nil {
		return out, errcode.New(errcode.ErrInternal, "sweep terminal requests: %v", err)
	}
	return out, nil
}

// DeletePairGuardsBefore 删超保留期的关系对守卫行(§9.24,R9 复审 P1)。
// 条件走 idx_created(created_at);多副本并发调用安全(各删各的行)。
// 正被事务持有的守卫行:DELETE 阻塞到其提交后再删,不破坏串行化语义。
func (r *MySQLFriendRepo) DeletePairGuardsBefore(ctx context.Context, retentionDays, limit int) (int64, error) {
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM friend_pair_guards WHERE created_at < DATE_SUB(NOW(), INTERVAL ? DAY) LIMIT ?`,
		retentionDays, limit)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "delete pair guards: %v", err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}
