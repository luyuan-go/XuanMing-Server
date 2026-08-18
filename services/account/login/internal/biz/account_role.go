// account_role.go — 账号身份 / 角色身份分离与两步登录(2026-08-18)。
//
// 模型:
//
//	账号(account_id)  ──1:N──▶  角色(player_id)
//	   认证的对象                  游戏数据的主键、进场的对象
//
// 登录因此是两步:
//  1. Login(账号 + 密码)      → 账号态 token + 角色列表
//  2. EnterRole(选中的角色)   → 该角色的 SessionToken + Hub 地址 + Hub 票据
//
// 兼容窗(§9.21):已发布客户端只会调 Login 且期待响应里直接有 hub 地址与票据,
// 所以 Login 默认仍**自动进入默认角色**;新客户端置 defer_role_entry=true 跳过。
//
// 角色名:创建角色功能上线前固定取账号名。全服显示名权威仍是 player 服务的
// players.nickname(uk_nickname 全局唯一),login 只负责**播种**(见 seedRoleProfile)。
package biz

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/safego"
	"github.com/luyuancpp/pandora/services/account/login/internal/data"
)

// profileSeedTimeout 是「播种 + 取当前显示名」的单角色超时。
//
// 与 playerNoReadTimeout(250ms)同一纪律:它挂在登录热路径上,必须有界且可降级,
// 不进 5s 登录预算的服务扇出账(压测审核【必修-1】口径)。超时 = 用台账里的名字兜底,
// 不阻断登录 —— 名字错了是可修的展示问题,登不进去不是。
const profileSeedTimeout = 300 * time.Millisecond

// ProfileSeeder 是 login → player 服务的出口(弱依赖)。
//
// 为什么 login 要依赖 player:「角色名 = 账号名」要对**全服**生效(头顶铭牌、队伍面板、
// 聊天、好友、公会、排行榜),就必须落到全服唯一的显示名权威 players.nickname 上。
// 只在 LoginResponse 里下发一个名字,只有本人看得见,别人看到的还是配置职业名。
//
// §9.21 弱依赖纪律:player 不可达 / 尚未滚上带 EnsureProfile 的版本(ErrNotImplemented)
// 时必须降级,绝不阻断登录。降级后果仅仅是角色名回落成 player 服务的默认前缀名,
// 下次登录会再试一次。
type ProfileSeeder interface {
	// EnsureProfile 建档并播种角色名,INSERT IGNORE 语义(已存在则原样返回既有档案)。
	EnsureProfile(ctx context.Context, playerID uint64, nickname string) (data.SeededProfile, error)
}

// SetRoleLedger 注入角色归属台账仓储(account_roles)。
//
// nil = 未启用账号 / 角色分离:登录降级为「一账号一角色」,行为与本次改造之前完全一致。
// 这条降级是滚动升级期的必需品(新二进制先上、000008 迁移还没跑完的窗口)。
func (u *LoginUsecase) SetRoleLedger(repo data.AccountRoleRepo) { u.roleLedger = repo }

// SetProfileSeeder 注入 player 服务出口。nil = 不播种角色名(名字回落 player 默认前缀名)。
func (u *LoginUsecase) SetProfileSeeder(s ProfileSeeder) { u.profileSeeder = s }

// accountView 是「认证完账号之后、进入角色之前」手上掌握的全部账号层信息。
type accountView struct {
	// Enabled 本部署是否启用了账号 / 角色分离(= 是否注入了角色台账)。
	//
	// 它区分的是「配置态」与「故障态」,这个区分是本结构最重要的一条不变式:
	// false = 本部署根本没有台账(dev 裸跑 / 000008 迁移未跑完的滚动窗口),其余字段
	// 全为零值,调用方走「一账号一角色」兼容档;台账**存在但读不出来**时调用方拿到的
	// 是 error 而不是 Enabled=false 的视图,绝不会误当成兼容档。
	Enabled    bool
	AccountID  uint64
	Token      string
	TokenExpMs int64
	Roles      []AccountRoleView
	// DefaultPlayerID 默认进入哪个角色(= Roles[0],最近登录过的那个)。
	// Enabled=true 时保证 > 0;Enabled=false 时恒为 0,调用方回落 accounts.player_id。
	DefaultPlayerID uint64
}

// resolveAccountView 补齐账号身份、角色台账,并签账号态 token。
//
// fail-closed(2026-08-18 用户拍板,推翻本函数原先的整体 fail-soft):台账**已启用**时
// 这里任何一步失败都直接让登录失败,不降级、不回落。
//
// 为什么不能 fail-soft:降级回落的目标是 accounts.player_id,而那恰恰是账号 / 角色分离
// 要管住的东西 —— 角色被软删(status!=0)或过户到别的账号之后,accounts.player_id 可能
// 还指着它。台账一抖就回落,等于给「进一个已经不属于自己的角色」开了一条旁路,而且它
// 恰好在最查不清的时候(DB 抖动)打开。归属校验必须是硬门,与封禁闸门同一纪律:
// 查不出结论 = 不放行。
//
// 代价是明确接受的:account_roles 读不出或 SignAccount 签不出时全服登不进去。
// 登不进去是可见、可告警、可回滚的故障;悄悄绕过归属校验不是。
//
// 唯一不算失败的分支是 u.roleLedger == nil —— 那是配置态不是故障,仍降级为单角色档。
func (u *LoginUsecase) resolveAccountView(
	ctx context.Context, account string, identity data.AccountIdentity, deviceID string,
) (accountView, error) {
	h := plog.With(ctx)
	if u.roleLedger == nil {
		// 未启用台账:静默降级(dev 裸跑 / 迁移未跑完),不打噪音日志。
		return accountView{}, nil
	}

	accountID, err := u.ensureAccountID(ctx, identity)
	if err != nil {
		h.Errorw("msg", "account_id_resolve_failed", "err", err, "account", account,
			"player_id", identity.PlayerID, "device_id", deviceID,
			"hint", "解不出账号身份 → 无从校验角色归属;fail-closed 拒绝登录")
		return accountView{}, err
	}

	roles, err := u.ensureAccountRoles(ctx, accountID, account, identity.PlayerID)
	if err != nil {
		h.Errorw("msg", "account_roles_resolve_failed", "err", err, "account", account,
			"account_id", accountID, "device_id", deviceID,
			"hint", "读不到角色台账 → 无从校验角色归属;fail-closed 拒绝登录")
		return accountView{}, err
	}
	if len(roles) == 0 {
		// 账号存在但一个可用角色都没有。创建角色功能上线前这不可能自然发生
		// (注册即建 slot 0),真出现说明角色被软删或过户走了 —— 既不能静默当成「新号」
		// 再建一个(会给同一个账号无限造角色),更不能回落 accounts.player_id 进那个
		// 已经不属于它的角色。
		h.Errorw("msg", "account_has_no_role", "account", account, "account_id", accountID,
			"hint", "台账里该账号没有 status=0 的角色;fail-closed 拒绝登录,请人工核查")
		return accountView{}, errcode.New(errcode.ErrLoginNoRole,
			"account_id=%d has no available role", accountID)
	}

	views := u.decorateRoles(ctx, account, roles)

	token, expMs, terr := u.signer.SignAccount(accountID, uuid.NewString())
	if terr != nil {
		h.Errorw("msg", "sign_account_token_failed", "err", terr,
			"account", account, "account_id", accountID,
			"hint", "签不出账号态 token → 新客户端选不了角;fail-closed 拒绝登录")
		return accountView{}, terr
	}

	if views[0].PlayerID == 0 {
		// player_id 是 account_roles 的主键,不可能为 0。真为 0 说明读回来的行是脏的,
		// 而拿它当默认进入角色会退化成「进 player_id=0」—— 必须当故障拦下,不能放行。
		h.Errorw("msg", "account_default_role_invalid", "account", account,
			"account_id", accountID,
			"hint", "台账首行 player_id=0;fail-closed 拒绝登录,请人工核查 account_roles")
		return accountView{}, errcode.New(errcode.ErrInternal,
			"account_id=%d default role has zero player_id", accountID)
	}

	return accountView{
		Enabled:         true,
		AccountID:       accountID,
		Token:           token,
		TokenExpMs:      expMs,
		Roles:           views,
		DefaultPlayerID: views[0].PlayerID,
	}, nil
}

// ensureAccountID 拿到账号身份;存量行 account_id 仍为 NULL 时就地补铸。
func (u *LoginUsecase) ensureAccountID(ctx context.Context, identity data.AccountIdentity) (uint64, error) {
	if identity.AccountID != 0 {
		return identity.AccountID, nil
	}
	if identity.PlayerID == 0 {
		return 0, errcode.New(errcode.ErrInternal, "cannot backfill account_id without player_id")
	}
	// 只可能是「新二进制上线后、旧二进制又注册了新账号」这一个窗口(000008 已把迁移
	// 那一刻的存量全部回填)。BackfillAccountID 是条件写,并发下返回赢家的值。
	return u.repo.BackfillAccountID(ctx, identity.PlayerID, u.sf.Generate())
}

// ensureAccountRoles 列出账号下的角色;台账里一个都没有时补建 slot 0。
//
// 什么时候需要补建:旧 login 二进制在滚动窗口内注册的账号 —— accounts 行有了,
// account_roles 没有(它不认识这张表)。此时把 accounts.player_id 当作既有角色补登记,
// **不铸新 player_id**:那个 ID 下可能已经有玩家数据了,另铸一个等于把存档丢掉。
func (u *LoginUsecase) ensureAccountRoles(
	ctx context.Context, accountID uint64, account string, legacyPlayerID uint64,
) ([]data.AccountRole, error) {
	roles, err := u.roleLedger.ListByAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if len(roles) > 0 {
		return roles, nil
	}
	if legacyPlayerID == 0 {
		return nil, nil
	}
	slot, serr := u.roleLedger.NextSlot(ctx, accountID)
	if serr != nil {
		return nil, serr
	}
	create := data.AccountRole{
		PlayerID:  legacyPlayerID,
		AccountID: accountID,
		Slot:      slot,
		// 创建角色功能上线前:角色名 = 账号名。
		RoleName: account,
	}
	if cerr := u.roleLedger.Create(ctx, create); cerr != nil {
		if errcode.As(cerr) != errcode.ErrAlreadyExists {
			return nil, cerr
		}
		// 并发补建:别人先写好了,回读拿最终结果。
	}
	return u.roleLedger.ListByAccount(ctx, accountID)
}

// decorateRoles 把台账行翻成选角界面视图,并顺带**播种角色名**。
//
// 名字口径:显示名权威是 player 服务的 players.nickname。EnsureProfile 一次调用同时
// 完成两件事 —— 档案不存在就用台账里的名字建档(播种),存在就原样返回既有名字
// (对账)。所以 effective_nickname 永远是「别人实际看到的那个名字」,拿它渲染选角
// 列表就不会出现「我这边显示张三、别人看到 Player_123」的分叉。
//
// player 不可达时降级用台账名:此时列表可能与全服显示名不一致,但选角功能仍可用。
func (u *LoginUsecase) decorateRoles(
	ctx context.Context, account string, roles []data.AccountRole,
) []AccountRoleView {
	views := make([]AccountRoleView, 0, len(roles))
	for _, r := range roles {
		v := AccountRoleView{
			PlayerID: r.PlayerID,
			RoleName: r.RoleName,
			Slot:     r.Slot,
		}
		if !r.LastLoginAt.IsZero() {
			v.LastLoginAtMs = uint64(r.LastLoginAt.UnixMilli())
		}
		// 已选职业外观:弱依赖,查不到按 0(未选过)。
		if u.roleRepo != nil {
			if roleID, rerr := u.roleRepo.GetRole(ctx, r.PlayerID); rerr == nil {
				v.RoleID = roleID
			}
		}
		// 角色编号:展示专用,fail-soft 置 0(客户端显示「生成中」)。
		if no, nerr := u.repo.GetPlayerNo(ctx, r.PlayerID); nerr == nil {
			v.PlayerNo = no
		}
		if profile, ok := u.seedRoleProfile(ctx, account, r); ok {
			v.RoleName = profile.Nickname
			v.Level = profile.Level
		}
		views = append(views, v)
	}
	return views
}

// seedRoleProfile 调 player 服务建档 + 播种角色名,返回该角色的权威显示名。
//
// ok=false 表示这次没拿到权威值(未配 seeder / 不可达 / 对端版本还没这个 RPC / 重名),
// 调用方应继续用台账里的名字。三类失败刻意用不同日志级别:
//   - ErrNotImplemented:对端版本还没滚上,滚动升级期的**预期状态**,Info 即可;
//   - ErrPlayerNicknameTaken:账号名被别的角色的昵称占了,是数据层面的真冲突,Warn;
//   - 其余(不可达 / 超时):基础设施问题,Warn。
func (u *LoginUsecase) seedRoleProfile(
	ctx context.Context, account string, r data.AccountRole,
) (data.SeededProfile, bool) {
	if u.profileSeeder == nil {
		return data.SeededProfile{}, false
	}
	seedCtx, cancel := context.WithTimeout(ctx, profileSeedTimeout)
	defer cancel()

	profile, err := u.profileSeeder.EnsureProfile(seedCtx, r.PlayerID, r.RoleName)
	if err == nil {
		if profile.Created {
			plog.With(ctx).Infow("msg", "role_profile_seeded", "account", account,
				"player_id", r.PlayerID, "role_name", profile.Nickname)
		}
		return profile, true
	}

	h := plog.With(ctx)
	switch errcode.As(err) {
	case errcode.ErrNotImplemented:
		// §9.21:对端还没滚上这个 RPC。重试永远不会成功,只能等它上线,不是故障。
		h.Infow("msg", "role_profile_seed_unimplemented", "player_id", r.PlayerID,
			"hint", "player 服务尚未滚上 EnsureProfile;角色名暂用台账名,对端上线后自动收敛")
	case errcode.ErrPlayerNicknameTaken:
		// 账号名与某个既有昵称撞了。创建角色功能上线前极罕见(账号名唯一、昵称默认是
		// Player_<id>),真撞上说明有人的昵称正好长成另一个账号名的样子。
		h.Warnw("msg", "role_profile_seed_name_taken", "player_id", r.PlayerID,
			"role_name", r.RoleName,
			"hint", "账号名已被其它角色昵称占用;该角色沿用 player 默认名,需要人工改名")
	default:
		h.Warnw("msg", "role_profile_seed_failed", "err", err, "player_id", r.PlayerID,
			"hint", "player 服务不可达/超时;角色名暂用台账名,下次登录重试")
	}
	return data.SeededProfile{}, false
}

// ListAccountRoles 列出账号下全部可用角色(两步登录,选角界面刷新用)。
//
// accountID 由调用方从**账号态 JWT** 的 sub 取(Envoy 注入 x-pandora-account-id),
// 不接受请求体自报 —— 否则任何人都能列别人账号下的角色。
func (u *LoginUsecase) ListAccountRoles(ctx context.Context, accountID uint64) ([]AccountRoleView, error) {
	if accountID == 0 {
		return nil, errcode.New(errcode.ErrUnauthorized, "list account roles: missing account identity")
	}
	if u.roleLedger == nil {
		return nil, errcode.New(errcode.ErrNotImplemented,
			"account role ledger not configured on this deployment")
	}
	roles, err := u.roleLedger.ListByAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	return u.decorateRoles(ctx, "", roles), nil
}

// EnterRole 是两步登录的第二步:选定角色进入游戏。
//
// 安全要点(这是本次改造最容易写错的一处):playerID 来自请求体,但**绝不当作身份**。
// 服务端按 account_roles 台账回查「这个角色是不是挂在 accountID 名下」,不属于就拒。
// 少了这一步,任何拿到自己账号 token 的人都能填别人的 player_id 直接进别人的号。
func (u *LoginUsecase) EnterRole(
	ctx context.Context, accountID, playerID uint64, deviceID string,
) (*LoginResult, error) {
	startedAt := time.Now()
	h := plog.With(ctx)

	if accountID == 0 {
		return nil, errcode.New(errcode.ErrUnauthorized, "enter role: missing account identity")
	}
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "enter role: player_id must be > 0")
	}
	if u.roleLedger == nil {
		return nil, errcode.New(errcode.ErrNotImplemented,
			"account role ledger not configured on this deployment")
	}

	role, err := u.roleLedger.GetByPlayer(ctx, playerID)
	if err != nil {
		// ErrLoginRoleNotFound 原样透出:客户端据此刷新角色列表重选,而不是重登。
		return nil, err
	}
	if role.AccountID != accountID {
		// 越权尝试:必须留痕。两个 ID 都记下来,便于事后判断是改包攻击还是客户端 bug。
		h.Warnw("msg", "enter_role_not_owned", "account_id", accountID,
			"role_account_id", role.AccountID, "player_id", playerID, "device_id", deviceID,
			"hint", "请求方账号与角色归属不符;拒绝进入")
		return nil, errcode.New(errcode.ErrLoginRoleNotOwned,
			"role player_id=%d does not belong to account_id=%d", playerID, accountID)
	}
	if role.Status != 0 {
		return nil, errcode.New(errcode.ErrLoginRoleNotFound,
			"role player_id=%d is not available (status=%d)", playerID, role.Status)
	}

	// 封禁闸门:封禁是**账号级**的,挂在 accounts.player_id(该账号的主角色指针)上,
	// 不是挂在本次要进入的角色上。所以必须按账号回查,不能拿 role.PlayerID 去查 ——
	// 那样账号下的其它角色就绕过了封禁。
	identity, aerr := u.repo.FindByAccountID(ctx, accountID)
	if aerr != nil {
		h.Errorw("msg", "enter_role_account_lookup_failed", "err", aerr,
			"account_id", accountID, "player_id", playerID)
		return nil, aerr
	}
	banned, berr := u.repo.CheckBanned(ctx, identity.PlayerID, deviceID)
	if berr != nil {
		// 与 Login 一致:封禁闸门 fail-closed,查不出来就不放行。
		h.Errorw("msg", "ban_check_failed", "err", berr,
			"account_id", accountID, "player_id", playerID, "device_id", deviceID)
		return nil, berr
	}
	if banned {
		h.Warnw("msg", "login_account_banned", "account_id", accountID,
			"player_id", playerID, "device_id", deviceID)
		return nil, errcode.New(errcode.ErrLoginAccountBanned,
			"account banned account_id=%d", accountID)
	}

	res, err := u.enterRoleSession(ctx, roleEntry{
		Account:   role.RoleName,
		AccountID: accountID,
		PlayerID:  playerID,
		DeviceID:  deviceID,
		StartedAt: startedAt,
	})
	if err != nil {
		return nil, err
	}
	res.AccountID = accountID
	return res, nil
}

// touchRoleLoginAsync 记录该角色最近一次进入游戏(决定下次选角默认选中谁)。
//
// 与 touchDeviceAsync 同纪律与同实现套路:纯记账副作用,不进登录关键路径,失败只记日志,
// 走 plog.Detach 脱离请求 ctx(响应返回后请求 ctx 即被取消,挂在上面必然写不进去)。
func (u *LoginUsecase) touchRoleLoginAsync(ctx context.Context, playerID uint64) {
	if u.roleLedger == nil || playerID == 0 {
		return
	}
	dctx, cancel := context.WithTimeout(plog.Detach(ctx), touchDeviceTimeout)
	safego.Go(dctx, "login_touch_role", func() {
		defer cancel()
		if err := u.roleLedger.TouchLogin(dctx, playerID); err != nil {
			plog.With(dctx).Warnw("msg", "touch_role_login_failed", "err", err,
				"player_id", playerID,
				"hint", "只影响选角界面的默认选中项,不影响本次进入")
		}
	})
}
