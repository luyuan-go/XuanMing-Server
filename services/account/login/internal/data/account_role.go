// account_role.go — 角色归属台账仓储(账号身份 / 角色身份分离,2026-08-18)。
//
// 模型:
//   - account_id 是**账号身份**,player_id 是**角色实体身份**。
//   - account_roles 一行 = 一个角色,account_id 列记录它当前挂在谁名下。
//     卖角色 / 角色过户 = 原子改这一列,全仓以 player_id 为键的业务数据零迁移。
//
// 权威边界(与既有 role.go 的 player_roles 别混):
//   - 本文件的 account_roles 管「有哪些角色、归谁」;
//   - role.go 的 player_roles 管「某个角色选了哪个 CfgRole 职业外观」。
//     两者正交,player_roles.role_id 是配置表 ID,不是角色实体。
//
// role_name 不是显示名权威:显示名权威在 pandora_player.players.nickname
// (uk_nickname 全服唯一)。本列只作播种源 + player 服务不可达时的兜底名,
// 故意不加唯一键(唯一性只能有一个权威点,两库各加一个必然分叉且无法自动收敛)。
package data

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// AccountRole 是角色归属台账的一行。
type AccountRole struct {
	PlayerID  uint64
	AccountID uint64
	Slot      uint32
	RoleName  string
	// Status 0=normal,1=deleted(软删)。ListByAccount 只返回 0。
	Status uint8
	// LastLoginAt 该角色最近一次真正进入游戏;零值 = 从未进过。
	LastLoginAt time.Time
	CreatedAt   time.Time
}

// AccountRoleRepo 是角色归属台账数据访问接口。biz 依赖接口,便于单测 fake。
type AccountRoleRepo interface {
	// ListByAccount 列出账号下**可用**角色(status=0)。
	//
	// 排序 = 选角界面的默认选中顺序:最近登录的在最前,从未登录过的按 slot 升序垫后。
	// 排序放在 SQL 里而不是 Go 里,是为了让「默认选中第一个」这条约定在所有调用方
	// (Login 响应 / ListAccountRoles / 未来的 GM 查询)都恒等,不会各写各的。
	//
	// 账号存在但一个角色都没有 → 返回空切片 + nil(不是错误:创建角色功能上线后
	// 「删光了所有角色」是合法状态)。
	ListByAccount(ctx context.Context, accountID uint64) ([]AccountRole, error)

	// GetByPlayer 按角色实体 ID 查台账行(EnterRole 的归属校验用)。
	// 不存在返回 ErrLoginRoleNotFound。
	GetByPlayer(ctx context.Context, playerID uint64) (AccountRole, error)

	// Create 建一行角色。同 (account_id, slot) 已存在返回 ErrAlreadyExists。
	Create(ctx context.Context, r AccountRole) error

	// TouchLogin 记录该角色最近一次进入游戏(纯记账,失败只记日志不阻断进场)。
	TouchLogin(ctx context.Context, playerID uint64) error

	// NextSlot 返回该账号下一个可用槽位(= 当前最大 slot + 1;无角色时为 0)。
	// 创建角色功能上线前只在「补建 slot 0」时用到。
	NextSlot(ctx context.Context, accountID uint64) (uint32, error)
}

// MySQLAccountRoleRepo 基于 *sql.DB 的实现(pandora_account.account_roles)。
type MySQLAccountRoleRepo struct {
	db *sql.DB
}

// NewMySQLAccountRoleRepo 构造。db 与 AccountRepo 共用连接池(同库)。
func NewMySQLAccountRoleRepo(db *sql.DB) *MySQLAccountRoleRepo {
	return &MySQLAccountRoleRepo{db: db}
}

// roleSelectColumns 统一列顺序,避免三处查询各写各的 Scan 顺序错位。
const roleSelectColumns = "player_id, account_id, slot, role_name, status, last_login_at, created_at"

func scanAccountRole(sc interface{ Scan(...any) error }) (AccountRole, error) {
	var (
		r           AccountRole
		lastLoginAt sql.NullTime
		createdAt   sql.NullTime
	)
	if err := sc.Scan(&r.PlayerID, &r.AccountID, &r.Slot, &r.RoleName, &r.Status, &lastLoginAt, &createdAt); err != nil {
		return AccountRole{}, err
	}
	if lastLoginAt.Valid {
		r.LastLoginAt = lastLoginAt.Time
	}
	if createdAt.Valid {
		r.CreatedAt = createdAt.Time
	}
	return r, nil
}

func (r *MySQLAccountRoleRepo) ListByAccount(ctx context.Context, accountID uint64) ([]AccountRole, error) {
	if accountID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "list account roles: accountID must be > 0")
	}
	// last_login_at IS NULL 排最后:MySQL 里 NULL 在 DESC 排序中会排到最前,
	// 直接 ORDER BY last_login_at DESC 会让「从未登录过的新角色」抢占默认选中位。
	const q = `SELECT ` + roleSelectColumns + ` FROM account_roles
WHERE account_id = ? AND status = 0
ORDER BY (last_login_at IS NULL) ASC, last_login_at DESC, slot ASC`
	rows, err := r.db.QueryContext(ctx, q, accountID)
	if err != nil {
		return nil, errcode.New(errcode.ErrInternal, "mysql list account roles: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var out []AccountRole
	for rows.Next() {
		role, serr := scanAccountRole(rows)
		if serr != nil {
			return nil, errcode.New(errcode.ErrInternal, "scan account role: %v", serr)
		}
		out = append(out, role)
	}
	if rerr := rows.Err(); rerr != nil {
		return nil, errcode.New(errcode.ErrInternal, "iterate account roles: %v", rerr)
	}
	return out, nil
}

func (r *MySQLAccountRoleRepo) GetByPlayer(ctx context.Context, playerID uint64) (AccountRole, error) {
	if playerID == 0 {
		return AccountRole{}, errcode.New(errcode.ErrInvalidArg, "get account role: playerID must be > 0")
	}
	const q = `SELECT ` + roleSelectColumns + ` FROM account_roles WHERE player_id = ? LIMIT 1`
	role, err := scanAccountRole(r.db.QueryRowContext(ctx, q, playerID))
	if errors.Is(err, sql.ErrNoRows) {
		return AccountRole{}, errcode.New(errcode.ErrLoginRoleNotFound, "role player_id=%d not found", playerID)
	}
	if err != nil {
		return AccountRole{}, errcode.New(errcode.ErrInternal, "mysql get account role: %v", err)
	}
	return role, nil
}

func (r *MySQLAccountRoleRepo) Create(ctx context.Context, role AccountRole) error {
	if role.PlayerID == 0 || role.AccountID == 0 {
		return errcode.New(errcode.ErrInvalidArg,
			"create account role: playerID/accountID must be > 0 (got %d/%d)", role.PlayerID, role.AccountID)
	}
	const q = `INSERT INTO account_roles(player_id, account_id, slot, role_name, status)
VALUES (?, ?, ?, ?, 0)`
	if _, err := r.db.ExecContext(ctx, q, role.PlayerID, role.AccountID, role.Slot, role.RoleName); err != nil {
		if isDupErr(err) {
			return errcode.New(errcode.ErrAlreadyExists,
				"account role already exists (player_id=%d account_id=%d slot=%d)",
				role.PlayerID, role.AccountID, role.Slot)
		}
		return errcode.New(errcode.ErrInternal, "mysql create account role: %v", err)
	}
	return nil
}

func (r *MySQLAccountRoleRepo) TouchLogin(ctx context.Context, playerID uint64) error {
	if playerID == 0 {
		return nil
	}
	const q = `UPDATE account_roles SET last_login_at = UTC_TIMESTAMP() WHERE player_id = ?`
	if _, err := r.db.ExecContext(ctx, q, playerID); err != nil {
		return errcode.New(errcode.ErrInternal, "mysql touch role login: %v", err)
	}
	return nil
}

func (r *MySQLAccountRoleRepo) NextSlot(ctx context.Context, accountID uint64) (uint32, error) {
	if accountID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "next slot: accountID must be > 0")
	}
	// 含软删行一起算最大值:槽位不回收。回收会让「刚删的角色」和「新建的角色」共用
	// 同一个 (account_id, slot),历史台账无法区分,过户 / 找回时对不上账。
	const q = `SELECT COALESCE(MAX(slot) + 1, 0) FROM account_roles WHERE account_id = ?`
	var next uint32
	if err := r.db.QueryRowContext(ctx, q, accountID).Scan(&next); err != nil {
		return 0, errcode.New(errcode.ErrInternal, "mysql next role slot: %v", err)
	}
	return next, nil
}
