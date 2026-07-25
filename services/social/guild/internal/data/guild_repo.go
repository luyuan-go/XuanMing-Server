// Package data 是 guild 服务的数据层(MySQL 公会 / 群成员关系,2026-06-27)。
//
// 库表(deploy/mysql-init/11-guild-tables.sql,pandora_social 库):
//
//	guilds              公会(PK guild_id snowflake,uk name)
//	guild_members       公会成员(PK player_id = 单归属:玩家只属一个公会)
//	guild_join_requests 加入申请(PK request_id snowflake,uk guild_id+player_id)
//
// 角色 / 状态取值与 proto 对齐:
//
//	role:   1 leader / 2 officer / 3 member(GuildRole)
//	status: 1 pending / 2 approved / 3 rejected(GuildJoinStatus)
//
// 成员关系是结构化列,直接映射(CLAUDE.md §5.9 关系型表不强制 proto bytes blob)。
// 复合一致性操作(审批 / 退会 / 踢人 / 转让 / 解散)在单 MySQL 事务内完成;
// 公会成员是 owner(guild_id)单键操作,无跨人事务(不撞 friend 跨人强一致难题)。
package data

import (
	"context"
	"database/sql"
	"errors"

	"github.com/go-sql-driver/mysql"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
)

// 公会职位(与 proto GuildRole 数值一致)。
const (
	GuildRoleLeader  = 1
	GuildRoleOfficer = 2
	GuildRoleMember  = 3
)

// 加入申请状态(与 proto GuildJoinStatus 数值一致)。
const (
	joinStatusPending  = 1
	joinStatusApproved = 2
	joinStatusRejected = 3
)

// mysqlErrDupEntry 是 MySQL 唯一键冲突错误码(用于把 uk_name 冲突翻译成 ErrGuildNameTaken)。
const mysqlErrDupEntry = 1062

// MySQL 事务并发错误码:1213 死锁、1205 锁等待超时。二者发生时 InnoDB 已回滚本事务,
// 整段事务重放是安全且标准的做法。虽然本包已把 guilds 父行作为「所有公会操作的第一把锁」
// (统一 guilds → 子表 加锁序,消除确定性 ABBA 死锁,见 tx 注释),仍保留有界重试兜底
// 二级索引间隙锁等偶发死锁。
const (
	mysqlErrLockWaitTimeout = 1205
	mysqlErrDeadlock        = 1213
)

// GuildRow 是一行公会(data → biz 内部结构)。
type GuildRow struct {
	GuildID     uint64
	Name        string
	LeaderID    uint64
	MemberCount int32
	MaxMembers  int32
	CreatedMs   int64
}

// GuildMemberRow 是一行公会成员(含所属公会 + 职位 + 加入时间)。
type GuildMemberRow struct {
	PlayerID uint64
	GuildID  uint64
	Role     int32
	JoinedMs int64
}

// GuildJoinRequestRow 是一行加入申请。
type GuildJoinRequestRow struct {
	RequestID uint64
	GuildID   uint64
	PlayerID  uint64
	Status    int32
	CreatedMs int64
}

// GuildRepo 是公会数据层抽象。biz 只依赖此接口,不依赖 *sql.DB。
type GuildRepo interface {
	// CreateGuild 在事务里建公会:校验创建者未在任何公会(单归属)→ 插 guilds → 插 leader 成员。
	//   - 创建者已在公会 → ErrGuildAlreadyInGuild
	//   - 公会名已占用 → ErrGuildNameTaken
	CreateGuild(ctx context.Context, newGuildID, leaderID uint64, name string, maxMembers int) error
	// GetGuild 读公会;not found → (nil, false, nil)。
	GetGuild(ctx context.Context, guildID uint64) (*GuildRow, bool, error)
	// GetMyGuild 读玩家所在公会;不在任何公会 → (nil, false, nil)。
	GetMyGuild(ctx context.Context, playerID uint64) (*GuildRow, bool, error)
	// GetMember 读玩家的成员行(含 guild_id / role);不在任何公会 → (nil, false, nil)。
	GetMember(ctx context.Context, playerID uint64) (*GuildMemberRow, bool, error)
	// ListMembers 列公会成员(按 player_id 升序游标分页;cursor=0 首页,limit>0 限量)。
	ListMembers(ctx context.Context, guildID, cursor uint64, limit int) ([]GuildMemberRow, error)
	// CreateJoinRequest 创建 / 复用加入申请(pending);已 pending → 复用既有 request_id。
	// maxPending>0 时:事务内校验该公会 pending 申请数,新增 / 复开一条会超限则回
	// ErrGuildRequestLimit(不变量 §9.18)。
	CreateJoinRequest(ctx context.Context, newRequestID, guildID, playerID uint64, maxPending int) (requestID uint64, reused bool, err error)
	// GetRequest 读申请;not found → (nil, false, nil)。
	GetRequest(ctx context.Context, requestID uint64) (*GuildJoinRequestRow, bool, error)
	// ApproveJoin 在事务里审批通过:读取申请的不可变 guild_id → 锁 guilds 父行 → 锁申请行 →
	// 校验审批人在该公会且为 leader/officer、申请仍 pending、申请人未在公会、未超员 →
	// 插成员 + 申请置 approved + member_count++。
	// 返回 approved=false,err=nil 表示申请已被并发处理(非 pending),biz 不报成功。
	ApproveJoin(ctx context.Context, requestID, approverID uint64, maxMembers int) (approved bool, err error)
	// RejectJoin 在事务里拒绝:读取申请的不可变 guild_id → 锁 guilds 父行 → 锁申请行 →
	// 校验审批人 leader/officer → 仍 pending → 置 rejected。
	RejectJoin(ctx context.Context, requestID, approverID uint64) (rejected bool, err error)
	// RemoveMember 在事务里删成员 + member_count--(玩家本人退会用;幂等:不存在不报错)。
	// 先锁 guilds 父行并禁止移除现任会长(会长须先转让 / 解散)。
	RemoveMember(ctx context.Context, guildID, playerID uint64) error
	// KickMember 在事务里踢人:锁 guilds 父行 → 持锁复核操作者仍具权限(leader 可踢非会长成员,
	// officer 只可踢 member)且目标非会长 → 删成员 + member_count--(幂等)。operatorID 权限在事务内
	// 权威复核,消除「检查后被并发降级 / 退会仍能踢人」的 TOCTOU(三审 P1-9)。
	KickMember(ctx context.Context, guildID, operatorID, targetID uint64) error
	// DisbandGuild 在事务里删公会:锁 guilds 父行 → 持锁复核 operatorID 仍是现任会长 →
	// 持锁读全部成员 player_id(与删除原子,消除「快照后并发批准新成员被删却漏失效 / 漏通知」
	// 的 TOCTOU)→ 删全部成员 + 删全部申请 + 删 guild 行 → 返回实际删除的成员 player_id 集合。
	// 防旧会长转让后仍解散公会的 TOCTOU(三审 P1-9)。
	DisbandGuild(ctx context.Context, guildID, operatorID uint64) (deletedMembers []uint64, err error)
	// SetRole 设成员职位(任命 / 撤销官员):锁 guilds 父行 → 持锁复核 operatorID 为现任会长、
	// 目标是本公会成员且非现任会长 → 改职位。防并发转让后旧请求把新会长降级致 leader_id 与职位不一致
	// 的 TOCTOU(三审 P1-9)。
	SetRole(ctx context.Context, guildID, operatorID, targetID uint64, role int32) error
	// TransferLeader 在事务里转让会长:旧会长降 member,新会长升 leader,更新 guilds.leader_id。
	TransferLeader(ctx context.Context, guildID, oldLeaderID, newLeaderID uint64) error
	// ListPendingRequests 列公会的挂起申请(按 request_id 升序游标分页)。
	ListPendingRequests(ctx context.Context, guildID, cursor uint64, limit int) ([]GuildJoinRequestRow, error)
	// DeleteTerminalJoinRequestsBefore 删终态(approved/rejected)且 updated_at 超保留期的
	// 入会申请行(保留期清理,§9.24;单批 limit 行)。pending 永不清;删后再次申请等价于
	// 全新申请(成员权威在 guild_members,申请行无资产语义)。返回删除行数。
	DeleteTerminalJoinRequestsBefore(ctx context.Context, retentionDays, limit int) (int64, error)
}

// MySQLGuildRepo 是基于 database/sql 的 GuildRepo 实现。
type MySQLGuildRepo struct {
	db *sql.DB
}

// NewMySQLGuildRepo 构造。db 由 pkg/mysqlx.MustNewClient 提供(连 pandora_social 库)。
func NewMySQLGuildRepo(db *sql.DB) *MySQLGuildRepo {
	return &MySQLGuildRepo{db: db}
}

func isDupEntry(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) && me.Number == mysqlErrDupEntry
}

// isRetryableTxErr 判断是否是可重试的事务并发错误(1213 死锁 / 1205 锁等待超时)。
// 依赖 dbErr 用 errcode.NewCause 保留底层 *mysql.MySQLError,errors.As 才能沿链检出。
func isRetryableTxErr(err error) bool {
	var me *mysql.MySQLError
	return errors.As(err, &me) &&
		(me.Number == mysqlErrDeadlock || me.Number == mysqlErrLockWaitTimeout)
}

// dbErr 包装底层 DB 错误:对外仍是 ErrInternal(客户端错误码不变),同时用 NewCause 保留
// 原始 *mysql.MySQLError 供 tx 判定死锁重试。所有会加锁 / 写库的语句失败都应走此封装。
func dbErr(cause error, format string, args ...any) error {
	return errcode.NewCause(errcode.ErrInternal, cause, format+": %v", append(args, cause)...)
}

func (r *MySQLGuildRepo) CreateGuild(ctx context.Context, newGuildID, leaderID uint64, name string, maxMembers int) error {
	return r.tx(ctx, func(tx *sql.Tx) error {
		// 单归属:创建者不能已在任何公会。
		var x int
		err := tx.QueryRowContext(ctx, `SELECT 1 FROM guild_members WHERE player_id = ? LIMIT 1`, leaderID).Scan(&x)
		if err == nil {
			return errcode.New(errcode.ErrGuildAlreadyInGuild, "player %d already in a guild", leaderID)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return dbErr(err, "check member %d", leaderID)
		}

		if _, err := tx.ExecContext(ctx,
			`INSERT INTO guilds (guild_id, name, leader_id, member_count, max_members) VALUES (?, ?, ?, 1, ?)`,
			newGuildID, name, leaderID, maxMembers); err != nil {
			if isDupEntry(err) {
				return errcode.New(errcode.ErrGuildNameTaken, "guild name %q taken", name)
			}
			return dbErr(err, "insert guild %d", newGuildID)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO guild_members (player_id, guild_id, role) VALUES (?, ?, ?)`,
			leaderID, newGuildID, GuildRoleLeader); err != nil {
			return dbErr(err, "insert leader member %d", leaderID)
		}
		return nil
	})
}

func (r *MySQLGuildRepo) GetGuild(ctx context.Context, guildID uint64) (*GuildRow, bool, error) {
	return r.scanGuild(ctx, r.db.QueryRowContext(ctx,
		`SELECT guild_id, name, leader_id, member_count, max_members,
		        CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED)
		 FROM guilds WHERE guild_id = ?`, guildID))
}

func (r *MySQLGuildRepo) GetMyGuild(ctx context.Context, playerID uint64) (*GuildRow, bool, error) {
	return r.scanGuild(ctx, r.db.QueryRowContext(ctx,
		`SELECT g.guild_id, g.name, g.leader_id, g.member_count, g.max_members,
		        CAST(UNIX_TIMESTAMP(g.created_at) * 1000 AS SIGNED)
		 FROM guilds g JOIN guild_members m ON m.guild_id = g.guild_id
		 WHERE m.player_id = ?`, playerID))
}

func (r *MySQLGuildRepo) scanGuild(_ context.Context, row *sql.Row) (*GuildRow, bool, error) {
	var g GuildRow
	err := row.Scan(&g.GuildID, &g.Name, &g.LeaderID, &g.MemberCount, &g.MaxMembers, &g.CreatedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "scan guild: %v", err)
	}
	return &g, true, nil
}

func (r *MySQLGuildRepo) GetMember(ctx context.Context, playerID uint64) (*GuildMemberRow, bool, error) {
	var m GuildMemberRow
	err := r.db.QueryRowContext(ctx,
		`SELECT player_id, guild_id, role, CAST(UNIX_TIMESTAMP(joined_at) * 1000 AS SIGNED)
		 FROM guild_members WHERE player_id = ?`, playerID).
		Scan(&m.PlayerID, &m.GuildID, &m.Role, &m.JoinedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "get member %d: %v", playerID, err)
	}
	return &m, true, nil
}

func (r *MySQLGuildRepo) ListMembers(ctx context.Context, guildID, cursor uint64, limit int) ([]GuildMemberRow, error) {
	q := `SELECT player_id, guild_id, role, CAST(UNIX_TIMESTAMP(joined_at) * 1000 AS SIGNED)
		 FROM guild_members WHERE guild_id = ? AND (? = 0 OR player_id > ?)
		 ORDER BY player_id ASC`
	args := []any{guildID, cursor, cursor}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "list members %d: %v", guildID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []GuildMemberRow
	for rows.Next() {
		var m GuildMemberRow
		if err := rows.Scan(&m.PlayerID, &m.GuildID, &m.Role, &m.JoinedMs); err != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan member: %v", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (r *MySQLGuildRepo) CreateJoinRequest(ctx context.Context, newRequestID, guildID, playerID uint64, maxPending int) (uint64, bool, error) {
	var resultID uint64
	var already bool
	err := r.tx(ctx, func(tx *sql.Tx) error {
		// 1. 先锁 guilds 父行(全局统一 guilds → 子表 加锁序,兼作与 DisbandGuild 的串行化闸门)。
		//    公会不存在 / 已被并发解散 → ErrGuildNotFound,避免写出指向已删公会的孤儿申请
		//    (四审 P1:CreateJoinRequest 不锁公会父行,可与解散并发产生孤儿 pending)。
		//    新旧二进制都先锁同一父行；新版随后以明细 COUNT 为权威自愈计数列，兼容旧版只写明细。
		var lockedGuildID uint64
		if err := tx.QueryRowContext(ctx,
			`SELECT guild_id FROM guilds WHERE guild_id = ? FOR UPDATE`,
			guildID).Scan(&lockedGuildID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errcode.New(errcode.ErrGuildNotFound, "guild %d not found", guildID)
			}
			return dbErr(err, "lock guild %d", guildID)
		}

		// 2. 锁该玩家对该公会的申请行(唯一键 guild_id+player_id)。
		var existingID uint64
		var status int32
		qerr := tx.QueryRowContext(ctx,
			`SELECT request_id, status FROM guild_join_requests WHERE guild_id = ? AND player_id = ? FOR UPDATE`,
			guildID, playerID).Scan(&existingID, &status)
		if qerr != nil && !errors.Is(qerr, sql.ErrNoRows) {
			return dbErr(qerr, "query join request")
		}

		// 3. 以 pending 明细为权威并校正计数列。FOR UPDATE 保证 MySQL RR 下读到最新已提交值，
		// 同时沿用旧版的 idx_guild_status 锁语义；TiDB 下同公会新写由上面的 guilds 父行串行。
		pendingCount, err := reconcileGuildPendingCount(ctx, tx, guildID)
		if err != nil {
			return err
		}
		switch {
		case errors.Is(qerr, sql.ErrNoRows):
			// 新增一条 pending 前,先校验该公会 pending 申请未满(不变量 §9.18)。
			// guilds 父行已 FOR UPDATE,pendingCount 与后续 +1 在同一临界区,无幻读。
			if maxPending > 0 && pendingCount >= maxPending {
				return errcode.New(errcode.ErrGuildRequestLimit,
					"pending join request limit reached for guild %d (max %d)", guildID, maxPending)
			}
			if _, ierr := tx.ExecContext(ctx,
				`INSERT INTO guild_join_requests (request_id, guild_id, player_id, status) VALUES (?, ?, ?, ?)`,
				newRequestID, guildID, playerID, joinStatusPending); ierr != nil {
				return dbErr(ierr, "insert join request")
			}
			if _, uerr := tx.ExecContext(ctx,
				`UPDATE guilds SET pending_request_count = ? WHERE guild_id = ?`,
				pendingCount+1, guildID); uerr != nil {
				return dbErr(uerr, "set pending count guild=%d", guildID)
			}
			resultID, already = newRequestID, false
			return nil
		}

		if status == joinStatusPending {
			resultID, already = existingID, true
			return nil
		}
		// 历史 rejected → 复位 pending,复用 request_id;从非 pending 转 pending 同样占用一格,先校验上限。
		if maxPending > 0 && pendingCount >= maxPending {
			return errcode.New(errcode.ErrGuildRequestLimit,
				"pending join request limit reached for guild %d (max %d)", guildID, maxPending)
		}
		if _, uerr := tx.ExecContext(ctx,
			`UPDATE guild_join_requests SET status = ? WHERE request_id = ?`,
			joinStatusPending, existingID); uerr != nil {
			return dbErr(uerr, "reopen join request")
		}
		if _, uerr := tx.ExecContext(ctx,
			`UPDATE guilds SET pending_request_count = ? WHERE guild_id = ?`,
			pendingCount+1, guildID); uerr != nil {
			return dbErr(uerr, "set pending count guild=%d", guildID)
		}
		resultID, already = existingID, false
		return nil
	})
	if err != nil {
		return 0, false, err
	}
	return resultID, already, nil
}

// reconcileGuildPendingCount 必须在持有 guilds(guildID)父行锁后调用。它复用旧版
// COUNT(*)...FOR UPDATE 的 MySQL 索引锁，同时把明细权威值写回计数列：旧 Pod 在滚动窗口只改
// guild_join_requests 也不会让新版信任陈旧计数。TiDB 没有间隙锁，但所有新版写已由父行串行。
func reconcileGuildPendingCount(ctx context.Context, tx *sql.Tx, guildID uint64) (int, error) {
	var count int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM guild_join_requests WHERE guild_id = ? AND status = ? FOR UPDATE`,
		guildID, joinStatusPending).Scan(&count); err != nil {
		return 0, dbErr(err, "count pending requests guild=%d", guildID)
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE guilds SET pending_request_count = ? WHERE guild_id = ? AND pending_request_count <> ?`,
		count, guildID, count); err != nil {
		return 0, dbErr(err, "reconcile pending count guild=%d", guildID)
	}
	return count, nil
}

func (r *MySQLGuildRepo) GetRequest(ctx context.Context, requestID uint64) (*GuildJoinRequestRow, bool, error) {
	var rq GuildJoinRequestRow
	err := r.db.QueryRowContext(ctx,
		`SELECT request_id, guild_id, player_id, status, CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED)
		 FROM guild_join_requests WHERE request_id = ?`, requestID).
		Scan(&rq.RequestID, &rq.GuildID, &rq.PlayerID, &rq.Status, &rq.CreatedMs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errcode.New(errcode.ErrInternal, "get request %d: %v", requestID, err)
	}
	return &rq, true, nil
}

func (r *MySQLGuildRepo) ApproveJoin(ctx context.Context, requestID, approverID uint64, maxMembers int) (bool, error) {
	approved := false
	err := r.tx(ctx, func(tx *sql.Tx) error {
		// 全局统一加锁序 guilds → guild_join_requests / guild_members:先取申请所属公会(guild_id
		// 是申请行的不可变列,未锁读安全),再锁 guilds 父行,最后锁申请行。与 DisbandGuild 的
		// guilds → 子表 同序,消除 Approve/Reject(旧 request→guild)与 Disband(guild→request)
		// 交叉的确定性 ABBA 死锁(四审 P1)。
		var guildID uint64
		gerr := tx.QueryRowContext(ctx,
			`SELECT guild_id FROM guild_join_requests WHERE request_id = ?`, requestID).Scan(&guildID)
		if errors.Is(gerr, sql.ErrNoRows) {
			return errcode.New(errcode.ErrGuildRequestInvalid, "request %d not found", requestID)
		}
		if gerr != nil {
			return dbErr(gerr, "read request %d guild", requestID)
		}

		// 1. 锁公会父行(所有职位变更的串行化点)读 member_count:先于审批人职位读取上锁,
		//    确保审批人职位快照在本事务提交前不被并发转让 / 降级 / 踢人改变(三审 P1-9 TOCTOU)。
		var memberCount int32
		if err := tx.QueryRowContext(ctx,
			`SELECT member_count FROM guilds WHERE guild_id = ? FOR UPDATE`, guildID).Scan(&memberCount); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errcode.New(errcode.ErrGuildNotFound, "guild %d not found", guildID)
			}
			return dbErr(err, "lock guild %d", guildID)
		}

		// 2. 锁申请行并复读权威状态 / 申请人(锁序在 guilds 之后)。
		var applicantID uint64
		var status int32
		err := tx.QueryRowContext(ctx,
			`SELECT player_id, status FROM guild_join_requests WHERE request_id = ? FOR UPDATE`,
			requestID).Scan(&applicantID, &status)
		if errors.Is(err, sql.ErrNoRows) {
			return errcode.New(errcode.ErrGuildRequestInvalid, "request %d not found", requestID)
		}
		if err != nil {
			return dbErr(err, "lock request %d", requestID)
		}
		if status != joinStatusPending {
			_, err := reconcileGuildPendingCount(ctx, tx, guildID)
			return err // 已被并发处理；仍提交一次计数自愈
		}

		// 3. 审批人须在该公会且为 leader/officer(持父行锁复核,权威)。
		// 用 FOR UPDATE 锁定读而非普通读:本事务首条语句是未锁读 guild_id(为定加锁序),已在
		// 取父行锁前建立 REPEATABLE READ 快照;若这里用普通 SELECT,会读到父行锁获取之前的旧
		// 快照角色——审批人在等锁期间被并发降级 / 踢出仍读到旧 leader/officer(五审 P1 TOCTOU
		// 被 RR 快照重开)。FOR UPDATE 同时兼容 TiDB（FOR SHARE 当前仅 noop 且默认拒绝）；父行 X 锁
		// 已串行化同公会成员写，强化成员行锁不会新增同公会锁序。
		var approverRole int32
		err = tx.QueryRowContext(ctx,
			`SELECT role FROM guild_members WHERE player_id = ? AND guild_id = ? FOR UPDATE`,
			approverID, guildID).Scan(&approverRole)
		if errors.Is(err, sql.ErrNoRows) {
			return errcode.New(errcode.ErrGuildNoPermission, "approver %d not in guild %d", approverID, guildID)
		}
		if err != nil {
			return dbErr(err, "check approver")
		}
		if approverRole != GuildRoleLeader && approverRole != GuildRoleOfficer {
			return errcode.New(errcode.ErrGuildNoPermission, "approver %d not leader/officer", approverID)
		}

		// 4. 申请人不能已在任何公会(单归属)。
		var x int
		err = tx.QueryRowContext(ctx, `SELECT 1 FROM guild_members WHERE player_id = ? LIMIT 1`, applicantID).Scan(&x)
		if err == nil {
			return errcode.New(errcode.ErrGuildAlreadyInGuild, "applicant %d already in a guild", applicantID)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return dbErr(err, "check applicant")
		}

		// 5. 不超员(member_count 已在步骤 1 持父行锁读取)。
		if int(memberCount) >= maxMembers {
			return errcode.New(errcode.ErrGuildFull, "guild %d full (%d/%d)", guildID, memberCount, maxMembers)
		}

		// 6. 插成员 + 申请 approved + member_count++。
		// player_id 是主键(单归属硬约束):并发被另一个公会先批时 dup → 翻译成
		// 「已在公会」业务错误(步骤 3 的快照读拦不住这种竞态,靠 PK 兑底)。
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO guild_members (player_id, guild_id, role) VALUES (?, ?, ?)`,
			applicantID, guildID, GuildRoleMember); err != nil {
			if isDupEntry(err) {
				return errcode.New(errcode.ErrGuildAlreadyInGuild, "applicant %d already in a guild", applicantID)
			}
			return dbErr(err, "insert member %d", applicantID)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE guild_join_requests SET status = ? WHERE request_id = ?`,
			joinStatusApproved, requestID); err != nil {
			return dbErr(err, "mark approved")
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE guilds SET member_count = member_count + 1 WHERE guild_id = ?`, guildID); err != nil {
			return dbErr(err, "inc member_count")
		}
		// pending 转 approved 后按明细重算，修复滚动窗口内旧 Pod 留下的任意计数漂移。
		if _, err := reconcileGuildPendingCount(ctx, tx, guildID); err != nil {
			return err
		}
		approved = true
		return nil
	})
	return approved, err
}

func (r *MySQLGuildRepo) RejectJoin(ctx context.Context, requestID, approverID uint64) (bool, error) {
	rejected := false
	err := r.tx(ctx, func(tx *sql.Tx) error {
		// 加锁序同 ApproveJoin:先未锁读申请所属公会(guild_id 不可变),再锁 guilds 父行,
		// 最后锁申请行,保持全局 guilds → 子表 单一顺序,消除与 Disband 的 ABBA 死锁(四审 P1),
		// 并防 approver 通过职位检查后被并发降级仍能拒绝申请的 TOCTOU(三审 P1-9)。
		var guildID uint64
		gerr := tx.QueryRowContext(ctx,
			`SELECT guild_id FROM guild_join_requests WHERE request_id = ?`, requestID).Scan(&guildID)
		if errors.Is(gerr, sql.ErrNoRows) {
			return errcode.New(errcode.ErrGuildRequestInvalid, "request %d not found", requestID)
		}
		if gerr != nil {
			return dbErr(gerr, "read request %d guild", requestID)
		}
		var dummyCount int32
		if err := tx.QueryRowContext(ctx,
			`SELECT member_count FROM guilds WHERE guild_id = ? FOR UPDATE`, guildID).Scan(&dummyCount); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errcode.New(errcode.ErrGuildNotFound, "guild %d not found", guildID)
			}
			return dbErr(err, "lock guild %d", guildID)
		}
		var status int32
		err := tx.QueryRowContext(ctx,
			`SELECT status FROM guild_join_requests WHERE request_id = ? FOR UPDATE`,
			requestID).Scan(&status)
		if errors.Is(err, sql.ErrNoRows) {
			return errcode.New(errcode.ErrGuildRequestInvalid, "request %d not found", requestID)
		}
		if err != nil {
			return dbErr(err, "lock request %d", requestID)
		}
		if status != joinStatusPending {
			_, err := reconcileGuildPendingCount(ctx, tx, guildID)
			return err
		}
		// FOR UPDATE 锁定读绕过本事务首条未锁读 guild_id 建立的 RR 快照,取审批人最新已提交角色,
		// 防等父行锁期间被并发降级仍读到旧 leader/officer(五审 P1 TOCTOU 被 RR 快照重开)。
		var approverRole int32
		err = tx.QueryRowContext(ctx,
			`SELECT role FROM guild_members WHERE player_id = ? AND guild_id = ? FOR UPDATE`,
			approverID, guildID).Scan(&approverRole)
		if errors.Is(err, sql.ErrNoRows) {
			return errcode.New(errcode.ErrGuildNoPermission, "approver %d not in guild %d", approverID, guildID)
		}
		if err != nil {
			return dbErr(err, "check approver")
		}
		if approverRole != GuildRoleLeader && approverRole != GuildRoleOfficer {
			return errcode.New(errcode.ErrGuildNoPermission, "approver %d not leader/officer", approverID)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE guild_join_requests SET status = ? WHERE request_id = ?`,
			joinStatusRejected, requestID); err != nil {
			return dbErr(err, "mark rejected")
		}
		// pending 转 rejected 后按明细重算，不依赖可能被旧 Pod 留脏的计数列。
		if _, err := reconcileGuildPendingCount(ctx, tx, guildID); err != nil {
			return err
		}
		rejected = true
		return nil
	})
	return rejected, err
}

func (r *MySQLGuildRepo) RemoveMember(ctx context.Context, guildID, playerID uint64) error {
	return r.tx(ctx, func(tx *sql.Tx) error {
		// 先锁公会行,与 TransferLeader 共用同一串行化点,并禁止移除现任会长:否则退会/踢人
		// 与转让交错时,会删掉刚晋升的新会长并留下悬空 leader_id(三审 P1-9 TOCTOU)。
		// 会长须先 TransferLeader 或 DisbandGuild;此处为数据层兜底,不依赖上层检查。
		var curLeader uint64
		err := tx.QueryRowContext(ctx,
			`SELECT leader_id FROM guilds WHERE guild_id = ? FOR UPDATE`, guildID).Scan(&curLeader)
		if errors.Is(err, sql.ErrNoRows) {
			return nil // 公会已解散,退会/踢人幂等成功
		}
		if err != nil {
			return dbErr(err, "lock guild %d", guildID)
		}
		if curLeader == playerID {
			return errcode.New(errcode.ErrGuildNotLeader,
				"leader %d must transfer or disband before leaving guild %d", playerID, guildID)
		}
		res, err := tx.ExecContext(ctx,
			`DELETE FROM guild_members WHERE guild_id = ? AND player_id = ?`, guildID, playerID)
		if err != nil {
			return dbErr(err, "delete member %d", playerID)
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return nil // 幂等:本就不在
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE guilds SET member_count = member_count - 1 WHERE guild_id = ? AND member_count > 0`, guildID); err != nil {
			return dbErr(err, "dec member_count")
		}
		return nil
	})
}

func (r *MySQLGuildRepo) KickMember(ctx context.Context, guildID, operatorID, targetID uint64) error {
	return r.tx(ctx, func(tx *sql.Tx) error {
		// 先锁 guilds 父行(与 Transfer/Remove/SetRole 共用串行化点),再持锁复核操作者与目标职位:
		// 消除「biz 检查通过后操作者被并发降级 / 目标被并发转成会长仍被踢」的 TOCTOU(三审 P1-9)。
		var lockedGuildID uint64
		if err := tx.QueryRowContext(ctx,
			`SELECT guild_id FROM guilds WHERE guild_id = ? FOR UPDATE`, guildID).Scan(&lockedGuildID); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return errcode.New(errcode.ErrGuildNotFound, "guild %d not found", guildID)
			}
			return dbErr(err, "lock guild %d", guildID)
		}
		var opRole int32
		err := tx.QueryRowContext(ctx,
			`SELECT role FROM guild_members WHERE player_id = ? AND guild_id = ?`,
			operatorID, guildID).Scan(&opRole)
		if errors.Is(err, sql.ErrNoRows) {
			return errcode.New(errcode.ErrGuildNoPermission, "operator %d not in guild %d", operatorID, guildID)
		}
		if err != nil {
			return dbErr(err, "check operator %d", operatorID)
		}
		var targetRole int32
		err = tx.QueryRowContext(ctx,
			`SELECT role FROM guild_members WHERE player_id = ? AND guild_id = ?`,
			targetID, guildID).Scan(&targetRole)
		if errors.Is(err, sql.ErrNoRows) {
			return errcode.New(errcode.ErrGuildNotMember, "target %d not in guild %d", targetID, guildID)
		}
		if err != nil {
			return dbErr(err, "check target %d", targetID)
		}
		if targetRole == GuildRoleLeader {
			return errcode.New(errcode.ErrGuildNoPermission, "cannot kick the leader")
		}
		switch opRole {
		case GuildRoleLeader:
			// leader 可踢 officer / member
		case GuildRoleOfficer:
			if targetRole != GuildRoleMember {
				return errcode.New(errcode.ErrGuildNoPermission, "officer can only kick members")
			}
		default:
			return errcode.New(errcode.ErrGuildNoPermission, "member cannot kick")
		}
		res, err := tx.ExecContext(ctx,
			`DELETE FROM guild_members WHERE guild_id = ? AND player_id = ?`, guildID, targetID)
		if err != nil {
			return dbErr(err, "delete member %d", targetID)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return nil
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE guilds SET member_count = member_count - 1 WHERE guild_id = ? AND member_count > 0`, guildID); err != nil {
			return dbErr(err, "dec member_count")
		}
		return nil
	})
}

func (r *MySQLGuildRepo) DisbandGuild(ctx context.Context, guildID, operatorID uint64) ([]uint64, error) {
	var deletedMembers []uint64
	err := r.tx(ctx, func(tx *sql.Tx) error {
		deletedMembers = nil // 事务重试时重置,避免累计重复 player_id。
		// 先锁 guilds 父行并复核 operatorID 仍是现任会长:防旧会长转让后仍解散公会(三审 P1-9)。
		var curLeader uint64
		err := tx.QueryRowContext(ctx,
			`SELECT leader_id FROM guilds WHERE guild_id = ? FOR UPDATE`, guildID).Scan(&curLeader)
		if errors.Is(err, sql.ErrNoRows) {
			return errcode.New(errcode.ErrGuildNotFound, "guild %d not found", guildID)
		}
		if err != nil {
			return dbErr(err, "lock guild %d", guildID)
		}
		if curLeader != operatorID {
			return errcode.New(errcode.ErrGuildNotLeader,
				"player %d is not current leader of guild %d (concurrent transfer?)", operatorID, guildID)
		}
		// 持锁读全部成员 player_id:父行 FOR UPDATE 已串行化 ApproveJoin(其审批也锁同一父行),
		// 故此处读到的成员集合与随后的 DELETE 原子一致,不会漏掉并发新批准的成员(TOCTOU 收口)。
		rows, err := tx.QueryContext(ctx, `SELECT player_id FROM guild_members WHERE guild_id = ?`, guildID)
		if err != nil {
			return dbErr(err, "list members of %d", guildID)
		}
		for rows.Next() {
			var pid uint64
			if err := rows.Scan(&pid); err != nil {
				rows.Close()
				return dbErr(err, "scan member of %d", guildID)
			}
			deletedMembers = append(deletedMembers, pid)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return dbErr(err, "iterate members of %d", guildID)
		}
		rows.Close()
		if _, err := tx.ExecContext(ctx, `DELETE FROM guild_members WHERE guild_id = ?`, guildID); err != nil {
			return dbErr(err, "delete members of %d", guildID)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM guild_join_requests WHERE guild_id = ?`, guildID); err != nil {
			return dbErr(err, "delete requests of %d", guildID)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM guilds WHERE guild_id = ?`, guildID); err != nil {
			return dbErr(err, "delete guild %d", guildID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return deletedMembers, nil
}

func (r *MySQLGuildRepo) SetRole(ctx context.Context, guildID, operatorID, targetID uint64, role int32) error {
	return r.tx(ctx, func(tx *sql.Tx) error {
		// 先锁 guilds 父行(串行化点)并复核 operatorID 仍是现任会长:防旧会长转让后仍改职位,
		// 或并发转让后把新会长降成 member/officer 致 leader_id 与角色不一致(三审 P1-9)。
		var curLeader uint64
		err := tx.QueryRowContext(ctx,
			`SELECT leader_id FROM guilds WHERE guild_id = ? FOR UPDATE`, guildID).Scan(&curLeader)
		if errors.Is(err, sql.ErrNoRows) {
			return errcode.New(errcode.ErrGuildNotFound, "guild %d not found", guildID)
		}
		if err != nil {
			return dbErr(err, "lock guild %d", guildID)
		}
		if curLeader != operatorID {
			return errcode.New(errcode.ErrGuildNotLeader,
				"player %d is not current leader of guild %d (concurrent transfer?)", operatorID, guildID)
		}
		// 不能通过 SetRole 改现任会长职位(会长变更只能走 TransferLeader,保 leader_id 一致)。
		if targetID == curLeader {
			return errcode.New(errcode.ErrGuildNoPermission, "cannot change role of current leader %d", targetID)
		}
		res, err := tx.ExecContext(ctx,
			`UPDATE guild_members SET role = ? WHERE guild_id = ? AND player_id = ?`, role, guildID, targetID)
		if err != nil {
			return dbErr(err, "set role %d", targetID)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return errcode.New(errcode.ErrGuildNotMember, "player %d not in guild %d", targetID, guildID)
		}
		return nil
	})
}

func (r *MySQLGuildRepo) TransferLeader(ctx context.Context, guildID, oldLeaderID, newLeaderID uint64) error {
	return r.tx(ctx, func(tx *sql.Tx) error {
		// 先锁公会行(所有会长/成员变更的串行化点)并确认旧会长仍是现任会长。缺这步会让
		// 并发两次 TransferLeader 各自降旧会长、升不同目标 → 双 LEADER 且 leader_id 只留最后一个
		// (三审 P1-9 TOCTOU)。lock 顺序统一为 guilds → guild_members,与 RemoveMember 一致,避免死锁。
		var curLeader uint64
		err := tx.QueryRowContext(ctx,
			`SELECT leader_id FROM guilds WHERE guild_id = ? FOR UPDATE`, guildID).Scan(&curLeader)
		if errors.Is(err, sql.ErrNoRows) {
			return errcode.New(errcode.ErrGuildNotFound, "guild %d not found", guildID)
		}
		if err != nil {
			return dbErr(err, "lock guild %d", guildID)
		}
		if curLeader != oldLeaderID {
			return errcode.New(errcode.ErrGuildNotLeader,
				"player %d is not current leader of guild %d (concurrent transfer?)", oldLeaderID, guildID)
		}
		// 新会长须为本公会成员。
		var role int32
		err = tx.QueryRowContext(ctx,
			`SELECT role FROM guild_members WHERE player_id = ? AND guild_id = ? FOR UPDATE`,
			newLeaderID, guildID).Scan(&role)
		if errors.Is(err, sql.ErrNoRows) {
			return errcode.New(errcode.ErrGuildNotMember, "target %d not in guild %d", newLeaderID, guildID)
		}
		if err != nil {
			return dbErr(err, "lock target %d", newLeaderID)
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE guild_members SET role = ? WHERE guild_id = ? AND player_id = ?`,
			GuildRoleMember, guildID, oldLeaderID); err != nil {
			return dbErr(err, "demote old leader")
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE guild_members SET role = ? WHERE guild_id = ? AND player_id = ?`,
			GuildRoleLeader, guildID, newLeaderID); err != nil {
			return dbErr(err, "promote new leader")
		}
		if _, err := tx.ExecContext(ctx,
			`UPDATE guilds SET leader_id = ? WHERE guild_id = ?`, newLeaderID, guildID); err != nil {
			return dbErr(err, "update guild leader")
		}
		return nil
	})
}

func (r *MySQLGuildRepo) ListPendingRequests(ctx context.Context, guildID, cursor uint64, limit int) ([]GuildJoinRequestRow, error) {
	q := `SELECT request_id, guild_id, player_id, status, CAST(UNIX_TIMESTAMP(created_at) * 1000 AS SIGNED)
		 FROM guild_join_requests WHERE guild_id = ? AND status = ? AND (? = 0 OR request_id > ?)
		 ORDER BY request_id ASC`
	args := []any{guildID, joinStatusPending, cursor, cursor}
	if limit > 0 {
		q += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := r.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "list requests %d: %v", guildID, err)
	}
	defer func() { _ = rows.Close() }()

	var out []GuildJoinRequestRow
	for rows.Next() {
		var rq GuildJoinRequestRow
		if err := rows.Scan(&rq.RequestID, &rq.GuildID, &rq.PlayerID, &rq.Status, &rq.CreatedMs); err != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan request: %v", err)
		}
		out = append(out, rq)
	}
	return out, rows.Err()
}

// txMaxRetries 是事务遇死锁 / 锁等待超时后的额外重试次数(共 1+N 次尝试)。
const txMaxRetries = 3

// tx 是带死锁重试的事务封装。加锁序在本包内已全局统一为 guilds 父行 → 子表(guild_members /
// guild_join_requests):任一公会操作都先锁该公会的 guilds 行,使 guilds 行成为「单公会唯一
// 串行化闸门」——两个事务在拿到任何子表锁之前就会在 guilds 行相互阻塞,无法形成 hold-and-wait
// 环,确定性 ABBA 死锁(Approve/Reject 的 request→guild 与 Disband 的 guild→request 交叉)被消除。
// 此重试仅兜底 InnoDB 二级索引间隙锁等偶发死锁;fn 必须可安全重放(只读锁 + 幂等写,不得依赖
// 首次副作用)。
func (r *MySQLGuildRepo) tx(ctx context.Context, fn func(tx *sql.Tx) error) error {
	var lastErr error
	for attempt := 0; attempt <= txMaxRetries; attempt++ {
		lastErr = r.txOnce(ctx, fn)
		if lastErr == nil || !isRetryableTxErr(lastErr) || ctx.Err() != nil {
			if attempt > 0 && lastErr == nil {
				// 重试后成功:说明确实发生过死锁/锁等待超时并自愈。
				plog.With(ctx).Debugw("msg", "guild_tx_retry_succeeded", "attempts", attempt+1)
			}
			return lastErr
		}
		// 本包把 guilds 父行作为所有公会写的唯一串行化闸门,高并发下父行竞争会触发
		// 1213 死锁 / 1205 锁等待超时并静默重放。此前整个重试链零日志:父行热点导致的
		// 死锁风暴、重试放大延迟完全查不到(且耗尽后的 ErrInternal 经 in-band 码返回
		// 也不会被 access log 记 ERROR)。
		plog.With(ctx).Warnw("msg", "guild_tx_retryable_conflict",
			"attempt", attempt+1, "max_retries", txMaxRetries, "err", lastErr)
	}
	// 重试耗尽:父行竞争持续存在,调用方将拿到 ErrInternal。
	plog.With(ctx).Warnw("msg", "guild_tx_retries_exhausted",
		"attempts", txMaxRetries+1, "err", lastErr)
	return lastErr
}

// txOnce 执行单次事务:fn 返回 error 则回滚,nil 则提交。
func (r *MySQLGuildRepo) txOnce(ctx context.Context, fn func(tx *sql.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return errcode.New(errcode.ErrInternal, "begin tx: %v", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if err := tx.Commit(); err != nil {
		return dbErr(err, "commit tx")
	}
	return nil
}

// DeleteTerminalJoinRequestsBefore 删终态且超保留期的入会申请行(保留期清理,§9.24)。
// 条件走 idx_status_updated(status, updated_at);pending(=1)永不匹配。
// 多副本并发调用安全(各删各的行)。
func (r *MySQLGuildRepo) DeleteTerminalJoinRequestsBefore(ctx context.Context, retentionDays, limit int) (int64, error) {
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM guild_join_requests WHERE status <> ? AND updated_at < DATE_SUB(NOW(), INTERVAL ? DAY) LIMIT ?`,
		joinStatusPending, retentionDays, limit)
	if err != nil {
		return 0, dbErr(err, "delete terminal join requests")
	}
	n, _ := res.RowsAffected()
	return n, nil
}
