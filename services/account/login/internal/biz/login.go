// Package biz 是 login 服务的业务逻辑层(usecase)。
//
// 职责分层(Kratos 风格 + 大厂惯例):
//
//	service/  RPC 入口,只做 proto 与 biz 类型互转、错误码映射
//	biz/      用例,纯业务逻辑(不依赖 redis/mysql/grpc 直接 API)
//	data/     仓储,提供 mysql/redis/外部 grpc 访问的接口实现
//
// W3 ①(2026-06-05):session_token 从 uuid 改为由 pkg/auth.Signer 签发的 HS256 JWT。
// Envoy jwt_authn filter 会验证该 JWT 并把 sub 提到 x-pandora-player-id 头。
//
// W3 ②(2026-06-05):
//   - 密码改 bcrypt 校验(pkg/passwd)
//   - 登录成功写 redis session(覆盖式,顶号靠 push.ConnectionManager + 新 session 覆盖)
//   - TouchDevice 写 account_devices(失败只日志,不阻塞登录)
//   - Logout 真实 DEL pandora:sess:<player_id>
package biz

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/passwd"
	"github.com/luyuancpp/pandora/pkg/snowflake"
	locatorv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/locator/v1"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	"github.com/luyuancpp/pandora/services/account/login/internal/data"
)

// LoginResult 是 LoginUsecase.Login 的产出。service 层再翻译成 proto。
type LoginResult struct {
	PlayerID       uint64
	SessionToken   string // JWT(W3 ①)
	SessionExpMs   int64  // session_token exp(unix ms),客户端展示 / 提前别未过期
	HubDSAddr      string
	HubTicket      string // hub DS JWT(W3 ①)
	HubTicketExpMs int64

	// 断线重连(docs/design/battle-reconnect.md §2.1):玩家在 battle DS 掉线重登时,
	// login 查 player_locator 发现其处于 BATTLE 态,直接下发原对局 battle DS 直连信息。
	// 三字段"要么全空、要么全填":非空时客户端直连 battle DS 重连;为空则走 hub 进大厅。
	BattleDSAddr      string
	BattleTicket      string // battle DS JWT(新 jti)
	BattleTicketExpMs int64
	MatchID           uint64 // 重连对局 ID(Snowflake uint64)

	// RegionID / CellID 是玩家的确定性路由落点(docs/design/scale-cellular-20m.md §3.2/§4.2)。
	// 由 cellroute.Router 按 player_id 算出;未配 Router(单 Cell / dev)时为 0。
	// 客户端 / 边缘网关据此连到正确 Region 的正确 Cell 接入入口。
	RegionID uint32
	CellID   uint32

	// SelectedRoleID 是玩家当前已选角色(player_roles 表,选角权威化 2026-07-08)。
	// 0 = 从未选过角。客户端登录后进选角界面用此值预选中;确认后调 SelectRole。
	SelectedRoleID uint32
	Resume         ResumeContextResult
}

type ResumeContextResult struct {
	Route      loginv1.ResumeRoute
	MatchID    uint64
	MatchStage loginv1.ResumeMatchStage
	GameMode   string
	// MapID 本局副本编号(透传 matchmaker 权威,0=未指定/默认;语义见 login.proto ResumeContext.map_id)。
	MapID uint32
	// ── owner 权威 placement 叠加(§9.23 query-first,owner-authority.md migrate ①;默认关)──
	// 仅在 owner 查询成功且 owner 路由与既有解析一致时叠加,**不改变 Route 决策**(migrate 阶段
	// 新旧双门并行,不会误路由);客户端据此做 §9.23 的 TARGET/ADMITTED 幂等 no-op。
	// §9.23 全量还缺 owner_epoch / retry_after / 统一状态枚举——需 proto 扩展(Codex),属 contract 阶段。
	PlacementState  loginv1.ResumePlacementState
	OperationID     string
	DSPodName       string
	DSInstanceUID   string
	DSInstanceEpoch uint32
	HubAssignmentID string
	AllocationID    string
	ReleaseTrack    string
}

// BattleTicketIssuer 把所有 login 侧 Battle 票据签发统一到带 roster 权威门的入口。
// TicketUsecase 实现此接口；测试可注入严格 fake 验证 fail-closed 行为。
type BattleTicketIssuer interface {
	// 末位 sessionJTI(R6 复审 P0-3):请求方登录会话 jti,签进 battle 票 sjti claim
	// (VerifyDSTicket 在线核销时复核现行性);空 = 无网关证据(dev/兼容窗)。
	IssueBattleDSTicketAtCell(context.Context, uint64, uint64, uint32, uint32, string) (*DSTicketResult, error)
	// InspectBattleRoute 是 Hub 签票门的显式三态权威判定(零副作用,不签票):
	//   data.BattleRouteActive   = 玩家确属 live 对局 → 拒绝 Hub;
	//   data.BattleRouteTerminal = 权威记录显式终态(ended/abandoned) → 唯一允许 Hub 的证明;
	//   data.BattleRouteUnknown  = 其余一切(roster 漂移/非成员/记录缺失/stale/错误) → fail-closed。
	// P0 修复(2026-07-15,Codex 复审):不得用 AuthorizeBattleTicket 的通用 ErrPermissionDeny 充当终态证明。
	InspectBattleRoute(ctx context.Context, playerID, matchID uint64) (data.BattleRouteState, error)
}

// LoginUsecase 是 Login / Logout 用例。
type LoginUsecase struct {
	repo        data.AccountRepo
	sessions    data.SessionRepo
	notifier    data.LocationNotifier
	hubAssigner data.HubAssigner    // W4 ⑥:hub_allocator 客户端,可为 nil(回退自签)
	roleRepo    data.PlayerRoleRepo // 选角权威化(2026-07-08):player_roles 仓储,可为 nil(降级无选角)
	// ownerReleaser:owner 迁移登出释放(owner-authority.md migrate ⑤;弱依赖,nil=未启用)。
	ownerReleaser OwnerReleaser
	// ownerPlacement / ownerQueryFirst:§9.23 query-first placement 叠加(migrate ①;
	// nil 或 ownerQueryFirst=false 时完全不查 owner,恢复上下文行为与现状一致)。
	ownerPlacement  OwnerPlacementQuerier
	ownerQueryFirst bool
	sf            *snowflake.Node
	hubDSAddr     string // 回退用静态 hub DS 地址(hub_allocator 未配 / 调用失败时)
	hubRegion     string // 传给 hub_allocator.AssignHub 的 region(空=allocator 选最空分片)
	signer        *auth.Signer
	verifier      *auth.Verifier
	// v2Verifier 独立验证 Hub allocator 返回的 DSTicket v2(RS256)。非 nil 也机械激活
	// 玩家 DSTicket 的 RS256-only profile；玩家 Session 仍走独立 HS256 verifier。
	v2Verifier *auth.DSTicketVerifier
	// battleTicketIssuer 必须在监听前注入。nil 或 roster 权威失败时不签重连票，
	// locator 已明确 InBattle 时返回 Unavailable；绝不回退到 signer 直签或继续 Hub 链。
	battleTicketIssuer BattleTicketIssuer
	// requireHubAssignmentBinding 激活后禁止 login 自签无归属绑定的 hub 票，也禁止在
	// hub_allocator 故障/旧版本返回无绑定票时回退；所有 hub 入场票必须由 allocator 权威签发。
	requireHubAssignmentBinding bool

	// sessionGen 会话代际 MySQL 仓储(R7 复审 P0-4,SetSessionGenerationRepo 注入)。
	// Login 先在 MySQL 原子分配单调代际(fail-closed,定序权威),再条件写 Redis;
	// 业务写事务(SetRole)在同一 MySQL 事务域内复核代际即可确定性挡掉旧会话。
	sessionGen data.SessionGenerationRepo

	// sessionGenEnforce 是 SetRole 会话代际强制门(R7 收口,滚动发布分阶段激活)。
	// false(默认):Login 照常双写代际(emit),但 SetRole 不做 MySQL 代际复核——滚动
	// 窗口内旧 Login Pod 不写代际,MySQL 行陈旧会误拒合法会话,必须等全 fleet emit 且
	// 旧版本排空后才能开;true:SetRole 同事务 FOR UPDATE 复核代际,确定性挡旧会话。
	// 关闭期间纵深仍在:precommit(Redis 现行性复核)不受本门控制。
	sessionGenEnforce bool

	// requireTicketSJTI 是票据兑换点空 sjti 强制门(R8 收口,P0-5 滚动兼容;与
	// hub_allocator 的 session_gate.require_ticket_sjti 同语义、独立开关)。
	// false(默认兼容档):VerifyDSTicket 收到不带 sjti 的票据时告警放行——滚动窗口内
	// 旧签发面(旧 matchmaker/旧 hub_allocator)仍持续签空 sjti 票,硬拒会让混版期战斗
	// 准入整体不可用;非空 sjti 始终强制复核现行性(fail-closed),不受本门影响。
	// true:空 sjti 硬拒 ErrUnauthorized。激活前提(顺序硬约束,见
	// docs/design/session-generation-rollout.md):全 fleet 签发面已升级为必带 sjti、
	// 旧版本 Pod 已排空、再等满一个票据最大 TTL(部署内实际启用签发器的最大值,
	// v2 RS256 上限 180s;若 legacy HS256 仍在用则为其 ds_ticket_ttl,默认 5min)。
	requireTicketSJTI bool

	// matchResolver 是 matchmaker 只读权威兜底(P0 修复 2026-07-15,codex P0-2/P0-3/P0-4)。
	// locator 是 30s TTL presence 投影,不能当"玩家不在对局"的证明;matchmaker 的
	// player claim + match 记录才是耐久事实。nil = presence-only(dev/local 兼容)。
	matchResolver data.MatchContextResolver

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md 三层化地基)。
	// 可为 nil:单 Cell / dev 部署不路由,登录返回 region/cell = 0。多 Cell 部署由 main
	// 经 SetCellRouter 注入(配静态表或 etcdtable 热更新表)。nil-safe,不阻断登录。
	router *cellroute.Router

	// devSkipPassword 开发期免密登录(conf.LoginConf.DevSkipPassword)。
	// 为 true 时跳过密码校验。
	devSkipPassword bool

	// devAutoRegister 开发期“假注册”(conf.LoginConf.DevAutoRegister)。
	// 为 true 时账号不存在则首登自动注册(存入本次密码 bcrypt 哈希)。
	devAutoRegister bool

	// allowedRoleIDs 是选角白名单(conf.LoginConf.AllowedRoleIDs,对齐客户端 CfgMisc.DefaultRoleIDs)。
	// 非空 = 严格白名单;空 = fail-closed 拒绝 SelectRole(除非 devAllowAnyRole=true)。
	allowedRoleIDs map[uint32]struct{}

	// devAllowAnyRole 开发期选角宽松开关(conf.LoginConf.DevAllowAnyRole)。
	// 为 true 且白名单为空时,SelectRole 只校验 roleID 非 0(配合客户端配置表快速迭代)。
	// 默认 false:白名单为空 → SelectRole 一律拒绝(fail-closed,防改包客户端签任意 role_id 进 hub 票据)。
	devAllowAnyRole bool
}

// SetBattleTicketIssuer 在服务启动、对外监听前注入统一的 Battle 票据签发入口。
func (u *LoginUsecase) SetBattleTicketIssuer(issuer BattleTicketIssuer) {
	u.battleTicketIssuer = issuer
}

// SetSessionGenerationRepo 注入会话代际 MySQL 仓储(R7 复审 P0-4)。非 nil 时 Login 在
// Redis 会话写入之前先原子推进 player_session_generations 单调代际(fail-closed 定序
// 权威),供 SetRole 等业务写在同一 MySQL 事务域内做 fencing。nil = dev 裸跑降级。
func (u *LoginUsecase) SetSessionGenerationRepo(repo data.SessionGenerationRepo) {
	u.sessionGen = repo
}

// SetSessionGenerationEnforce 设置 SetRole 会话代际强制门(默认 false=只 emit 不强制)。
// 激活前提:全 fleet Login 已升级到会写代际的版本且旧版本已排空(发布顺序见
// docs/design/session-generation-rollout.md);提前开启会误拒经旧 Pod 登录的合法会话。
func (u *LoginUsecase) SetSessionGenerationEnforce(enforce bool) {
	u.sessionGenEnforce = enforce
}

// SetRequireTicketSJTI 设置票据兑换点空 sjti 强制门(默认 false=兼容档告警放行)。
// 激活前提:全 fleet 签发面必带 sjti、旧版本排空、等满一个票据最大 TTL(发布顺序见
// docs/design/session-generation-rollout.md);提前开启会硬拒旧签发面的存量合法票。
func (u *LoginUsecase) SetRequireTicketSJTI(require bool) {
	u.requireTicketSJTI = require
}

// SetMatchContextResolver 注入 matchmaker 只读权威客户端(可 nil,presence-only 降级)。
func (u *LoginUsecase) SetMatchContextResolver(r data.MatchContextResolver) {
	u.matchResolver = r
}

// NewLoginUsecase 构造 LoginUsecase。
//
// repo / sessions 必填;notifier / hubAssigner 可为 nil(弱依赖,nil 时降级)。
// sf 用 svc.BaseContext.Snowflake;hubDSAddr / hubRegion 从 conf 读;signer / legacy verifier /
// v2 verifier 由 main 层按独立信任域构造后传进来。
//
// W4 ⑥:新增 hubAssigner + hubRegion。hubAssigner 非 nil 时,Login 调 hub_allocator.AssignHub
// 拿真实 hub_ds_addr + hub_ticket;nil 或调用失败时回退到自签票据 + 静态 hubDSAddr。
//
// 选角权威化(2026-07-08):新增 roleRepo(可 nil,降级无选角) + allowedRoleIDs(选角白名单)
// + devAllowAnyRole(dev 宽松开关)。白名单空且未开 devAllowAnyRole → SelectRole fail-closed 拒绝。
// Login 读已选角透传给 AssignHub / 自签票;SelectRole 落库后重发 hub 票。
func NewLoginUsecase(
	repo data.AccountRepo,
	sessions data.SessionRepo,
	notifier data.LocationNotifier,
	hubAssigner data.HubAssigner,
	roleRepo data.PlayerRoleRepo,
	sf *snowflake.Node,
	hubDSAddr string,
	hubRegion string,
	signer *auth.Signer,
	verifier *auth.Verifier,
	v2Verifier *auth.DSTicketVerifier,
	devSkipPassword bool,
	devAutoRegister bool,
	allowedRoleIDs []uint32,
	devAllowAnyRole bool,
) *LoginUsecase {
	var allowed map[uint32]struct{}
	if len(allowedRoleIDs) > 0 {
		allowed = make(map[uint32]struct{}, len(allowedRoleIDs))
		for _, id := range allowedRoleIDs {
			allowed[id] = struct{}{}
		}
	}
	return &LoginUsecase{
		repo:            repo,
		sessions:        sessions,
		notifier:        notifier,
		hubAssigner:     hubAssigner,
		roleRepo:        roleRepo,
		sf:              sf,
		hubDSAddr:       hubDSAddr,
		hubRegion:       hubRegion,
		signer:          signer,
		verifier:        verifier,
		v2Verifier:      v2Verifier,
		devSkipPassword: devSkipPassword,
		devAutoRegister: devAutoRegister,
		allowedRoleIDs:  allowed,
		devAllowAnyRole: devAllowAnyRole,
	}
}

// SetCellRouter 注入确定性 region/cell 路由器(可选,多 Cell 部署用)。
//
// nil-safe:不调用 / 传 nil 时,Login 返回的 RegionID/CellID 为 0(单 Cell / dev 语义)。
// 用 setter 而非构造参数,避免单 Cell 阶段所有调用点被迫改签名;多 Cell 部署在 main
// 装配阶段调一次即可。Router 内部读路径无锁(AtomicTable),并发安全。
func (u *LoginUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// SetRequireHubAssignmentBinding 在服务监听前设置 Hub DSTicket 归属绑定激活栅栏。
func (u *LoginUsecase) SetRequireHubAssignmentBinding(require bool) {
	u.requireHubAssignmentBinding = require
}

func (u *LoginUsecase) rs256DSTicketProfileEnabled() bool {
	return u != nil && u.v2Verifier != nil
}

// Login 走真实流程(W3 ②):
//  1. repo.FindByAccount → 拿 bcrypt 哈希
//  2. passwd.Verify(stored, clientDigest) 比对
//  3. repo.CheckBanned → 必须 false
//  4. 用 signer 签 session(24h) + hub_ticket(5min)
//  5. sessions.Set 写入 redis(顶号策略:同 key 覆盖)
//  6. repo.TouchDevice 异步语义(同步调,失败仅日志)
//  7. 返回 hub_ds_addr + 两份 JWT
//
// 任何步骤失败返回 *errcode.Error,由 service 层翻译。
func (u *LoginUsecase) Login(ctx context.Context, account, passwordHash, deviceID string) (*LoginResult, error) {
	h := plog.With(ctx)

	playerID, expected, err := u.repo.FindByAccount(ctx, account)
	if err != nil {
		// 账号不存在:开发期“假注册” / 免密任一开关打开 → 首登自动注册(不阻断登录)。
		if errcode.As(err) != errcode.ErrLoginAccountNotFound || !(u.devAutoRegister || u.devSkipPassword) {
			h.Debugw("msg", "login_account_not_found", "account", account)
			return nil, err
		}
		playerID, err = u.ensureAccount(ctx, account, passwordHash)
		if err != nil {
			h.Errorw("msg", "login_auto_register_failed", "err", err, "account", account)
			return nil, err
		}
		// 刚注册:密码即客户端本次所发,无需再校验。
		h.Warnw("msg", "login_dev_auto_registered", "account", account, "player_id", playerID)
	} else if u.devSkipPassword {
		// 账号已存在 + 免密模式 → 跳过密码校验。
		h.Warnw("msg", "login_dev_skip_password", "account", account, "player_id", playerID)
	} else if verr := passwd.Verify(expected, passwordHash); verr != nil {
		h.Debugw("msg", "login_password_mismatch", "account", account, "player_id", playerID)
		return nil, errcode.New(errcode.ErrLoginPasswordMismatch, "password mismatch")
	}

	banned, err := u.repo.CheckBanned(ctx, playerID, deviceID)
	if err != nil {
		return nil, err
	}
	if banned {
		// 封禁登录是安全审计 / 客服排查的关键事件;ErrLoginAccountBanned 是业务码,
		// access log 只记 rpc_ok/failed 的泛化码,故在此显式 WARN 带上下文。
		plog.With(ctx).Warnw("msg", "login_account_banned", "player_id", playerID, "device_id", deviceID)
		return nil, errcode.New(errcode.ErrLoginAccountBanned, "account banned player_id=%d", playerID)
	}

	sessJTI := uuid.NewString()
	sessionToken, sessExpMs, err := u.signer.SignSession(playerID, sessJTI)
	if err != nil {
		h.Errorw("msg", "sign_session_failed", "err", err, "player_id", playerID)
		return nil, errcode.New(errcode.ErrInternal, "sign session failed: %v", err)
	}

	// 写 session(R7 收口,并发 Login 定序):先 MySQL 原子分配单调代际(登录定序权威,
	// fail-closed),再对 Redis 做「仅更高代际可覆盖」的条件写。任意并发交错下两个存储
	// 最终都收敛到最高代际那次登录;输掉定序的登录(条件写被拒)直接失败,不交付凭据,
	// 不再出现「Redis=B、MySQL=A」的撕裂(旧实现先写 Redis 再无条件覆盖 MySQL 的缺陷)。
	//
	// 部分失败口径(R9 复审 P0-2 收口 + R10 复审 P0-1 消歧):MySQL 已提交、Redis 写失败
	// (网络类错误)时本次登录失败,并由 reconcileFailedSessionWrite 把「Redis 到底提交
	// 没有」这个不确定态**判定**掉再决定回补/墓碑(见该函数注释)。定序失败
	// (ErrSessionSuperseded)不进入该路径:行已属于赢家,回补会回滚别人的登录。
	sessTTL := u.signer.SessionTTL()
	var sessGen uint64
	var sessGenLease data.SessionGenerationLease
	if u.sessionGen != nil {
		lease, gerr := u.sessionGen.PersistSessionJTI(ctx, playerID, sessJTI)
		if gerr != nil && data.IsCommitAmbiguous(gerr) {
			// R11 复审 P0-1 问题 A:COMMIT 结果不确定。不猜——用本次 jti 作唯一标记读回
			// 权威判定(§9.22「结果不确定必须 fail-closed,禁止冒充默认状态」的落法是把
			// 不确定态判定掉,而不是把它当失败)。
			lease, gerr = u.resolveAmbiguousSessionGeneration(ctx, playerID, sessJTI, lease)
		}
		if gerr != nil {
			h.Errorw("msg", "session_generation_persist_failed", "err", gerr, "player_id", playerID)
			return nil, errcode.NewCause(errcode.ErrUnavailable, gerr,
				"session generation persistence unavailable; login rejected")
		}
		sessGenLease = lease
		sessGen = lease.Generation
	}
	if u.sessions != nil {
		if err := u.sessions.Set(ctx, playerID, sessionToken, sessJTI, deviceID, sessTTL, sessGen); err != nil {
			// ErrSessionSuperseded = 并发更新一代登录已完成写入,本次登录定序失败;
			// 其余为基础设施错误。两者都不得交付凭据。
			h.Warnw("msg", "session_set_failed", "err", err, "player_id", playerID, "gen", sessGen)
			if u.sessionGen != nil && errcode.As(err) != errcode.ErrSessionSuperseded {
				u.reconcileFailedSessionWrite(ctx, playerID, sessJTI, sessGen, sessGenLease)
			}
			return nil, err
		}
	}

	// 确定性 region/cell 路由落点(scale-cellular-20m.md §3.2/§3.3):多 Cell 部署时算出玩家落点,
	// 一处算好,既供客户端 / 边缘网关连到正确 Cell,又盖进自签 hub 票据(§3.3 防跨单元串号)。
	// router 为 nil(单 Cell / dev)或 Route 报错 → 降级 0/0(同单 Cell 行为),不阻断登录。
	regionID, cellID := u.routeRegionCell(ctx, playerID)

	// 断线重连(docs/design/battle-reconnect.md §2.1):玩家在 battle DS 中掉线重登时,
	// 查 player_locator 若发现其仍处于 BATTLE 态(TTL 租约,由 DS 心跳按 roster 续期),
	// 直接下发原对局的 battle DS 直连信息,而非把玩家丢回大厅。
	//
	// 路由权威 = locator 租约 + match 权威三态门(tryBattleReconnect 内分诊):
	//   租约活着且 match Active → 回原局;Terminal/租约过期 → 进 Hub;
	//   权威暂时不可用 → 可重试 Unavailable(最长 ~30s 租约到期自愈,绝不永久卡死)。
	// DS 崩溃/删除 → 心跳停 → 租约 30s 蒸发 → 玩家自动进 Hub,无需任何 cleanup。
	// hubFenceMatchID:tryBattleReconnect 判定「终局后 TTL 残留」继续走 Hub 时带回的
	// 原对局 match_id(Battle→Hub 回流 fence),签进 hub 票据 source_match_id claim。
	var hubFenceMatchID uint64
	if u.notifier == nil {
		if u.requireHubAssignmentBinding {
			return nil, errcode.New(errcode.ErrUnavailable,
				"player locator is required before B1 hub assignment")
		}
	} else {
		res, terminalFence, reconnectErr := u.tryBattleReconnect(ctx, playerID, deviceID, sessionToken, sessExpMs, regionID, cellID, sessJTI)
		if reconnectErr != nil {
			return nil, reconnectErr
		}
		if res != nil {
			// R5 复审 P0-5:battle 重连路径同样先做交付终检(见 fenceLoginDelivery 注释),
			// 并发新登录轮换 jti 后,旧流程不得把 battle 直连凭据交给旧设备。
			if ferr := u.fenceLoginDelivery(ctx, playerID, sessJTI); ferr != nil {
				return nil, ferr
			}
			return res, nil
		}
		hubFenceMatchID = terminalFence
	}

	// 读玩家已选角色(选角权威化 2026-07-08):弱依赖,读失败按 0(未选角)处理不阻断登录。
	// 透传给 resolveHub → hub 票据 claim;同时回给客户端选角界面预选中。
	selectedRoleID := u.loadSelectedRole(ctx, playerID)

	// B1 先建立 LOGIN_PENDING 权威位置，再调用 Hub allocator。写入失败时既不分配
	// Hub，也不会产生/交付 Hub 票；local/off 保留历史上的分配后 best-effort 通知顺序。
	pendingNotified := false
	if u.requireHubAssignmentBinding {
		if u.notifier == nil {
			return nil, errcode.New(errcode.ErrUnavailable,
				"player locator is required before B1 hub assignment")
		}
		if err := u.notifier.NotifyLoginPending(ctx, playerID, deviceID); err != nil {
			h.Warnw("msg", "locator_notify_failed", "err", err, "player_id", playerID)
			return nil, errcode.NewCause(errcode.ErrUnavailable, err,
				"player locator refused LOGIN_PENDING; hub was not assigned")
		}
		pendingNotified = true
	}

	// 解析 hub 分片 + hub 票据(W4 ⑥):
	// hub_allocator 是 hub 票据权威,优先调 AssignHub 拿真实地址 + 票据;
	// 未配 / 调用失败 → 回退自签票据(盖 region/cell 戳) + 静态 hubDSAddr(弱依赖,不阻断登录)。
	hubDSAddr, hubTicket, hubExpMs, err := u.resolveHub(ctx, playerID, regionID, cellID, selectedRoleID, hubFenceMatchID, sessJTI)
	if err != nil {
		h.Errorw("msg", "resolve_hub_failed", "err", err, "player_id", playerID)
		return nil, err
	}

	// 记录最近登录设备(失败不阻塞登录,只日志告警)
	if err := u.repo.TouchDevice(ctx, playerID, deviceID); err != nil {
		h.Warnw("msg", "touch_device_failed", "err", err, "player_id", playerID, "device_id", deviceID)
	}

	// local/off 在 Hub 解析后 best-effort 通知；B1 已在分配前成功写入，不能重复写。
	if !pendingNotified && u.notifier != nil {
		if err := u.notifier.NotifyLoginPending(ctx, playerID, deviceID); err != nil {
			h.Warnw("msg", "locator_notify_failed", "err", err, "player_id", playerID)
		}
	}

	// R5 复审 P0-5:副作用交付终检——本流程写入的 sessJTI 必须仍是当前一代才允许把
	// session token / hub 票据交给调用方。sessions.Set 之后的分配、locator、签票各步
	// 都不复核现行性,并发新登录 B 在其间再次轮换 jti 时,旧流程 A 若继续交付,旧设备
	// 将取得"看似有效"的完整登录态。复核失败 → 不返回任何凭据(票据已签但从未离开
	// 服务端 = 未取得);已写的 LOGIN_PENDING 等 locator 投影由 B 自己的写覆盖
	// (locator 是 presence 投影,非权威,§9.22)。
	if ferr := u.fenceLoginDelivery(ctx, playerID, sessJTI); ferr != nil {
		return nil, ferr
	}

	// 确定性 region/cell 路由已在上方一次算好(regionID/cellID),这里直接复用。
	h.Debugw("msg", "login_ok", "player_id", playerID, "device_id", deviceID,
		"session_exp_ms", sessExpMs, "hub_ticket_exp_ms", hubExpMs,
		"region_id", regionID, "cell_id", cellID)

	return &LoginResult{
		PlayerID:       playerID,
		SessionToken:   sessionToken,
		SessionExpMs:   sessExpMs,
		HubDSAddr:      hubDSAddr,
		HubTicket:      hubTicket,
		HubTicketExpMs: hubExpMs,
		RegionID:       regionID,
		CellID:         cellID,
		SelectedRoleID: selectedRoleID,
		Resume:         ResumeContextResult{Route: loginv1.ResumeRoute_RESUME_ROUTE_HUB},
	}, nil
}

// resumeStageFromMatchStage 显式映射 matchmaker 权威 stage → login resume stage。
// 两个枚举数值语义并不对齐(match STARTING=1 vs login NONE=1),严禁数值强转。
func resumeStageFromMatchStage(s matchv1.PlayerMatchResumeStage) loginv1.ResumeMatchStage {
	switch s {
	case matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_STARTING:
		// start saga 在飞:对客户端等价于已受理排队。
		return loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_QUEUED
	case matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_QUEUED:
		return loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_QUEUED
	case matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_CONFIRMING:
		return loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_CONFIRMING
	case matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_ALLOCATING:
		return loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_ALLOCATING
	case matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_READY:
		return loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_READY
	default:
		return loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_UNSPECIFIED
	}
}

// hubResumeFromMatchAuthority 组装 HUB 路由的 ResumeContext。玩家持有活跃撮合 claim
// (排队/确认/分配中)时带上 match_id/stage/game_mode——冷启动客户端必须先恢复
// x-pandora-game-mode 路由头才能 Cancel/Confirm/GetProgress
// (login.proto ResumeContext.game_mode 契约)。无 claim / 未查到 → 裸 HUB。
func hubResumeFromMatchAuthority(ma *data.PlayerMatchAuthority) ResumeContextResult {
	out := ResumeContextResult{Route: loginv1.ResumeRoute_RESUME_ROUTE_HUB}
	if ma != nil && ma.State == matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE {
		out.MatchID = ma.MatchID
		out.MatchStage = resumeStageFromMatchStage(ma.Stage)
		out.GameMode = ma.GameMode
		out.MapID = ma.MapID
	}
	return out
}

// buildBattleResume 组装 BATTLE 路由的 ResumeContext。game_mode 必须来自 matchmaker
// 持久权威(ResolvePlayerMatchContext 的 canonical 读,PVE/PVP 记录同源可解),
// login 不做任何硬编码猜测。
//
// ma 是 resolveBattleAuthority 已查得的权威(presence 未命中、由 READY claim 合成
// 的路径);presence 命中的快路径 ma==nil,这里补查一次(零副作用只读)。
//
// stage:presence 命中(locator BATTLE 租约活着)= RUNNING(玩家已在 DS 上);
// 由 READY claim 合成 = 按权威 stage 显式映射(READY)。
//
// fail-closed(B1):game_mode 拿不到(resolver 未配/查询失败/claim 漂移/记录缺字段)
// → ErrUnavailable 可重试。缺 game_mode 的 BATTLE resume 会让客户端 DS 恢复协调器
// 无法恢复路由头(rejecting unknown authoritative game_mode),交付它就是交付 bug。
// local/off 保留弱降级(空 game_mode + 告警,dev 裸跑不阻断)。
func (u *LoginUsecase) buildBattleResume(
	ctx context.Context, playerID uint64, bl data.BattleLocation, ma *data.PlayerMatchAuthority,
) (ResumeContextResult, error) {
	h := plog.With(ctx)
	if ma == nil && u.matchResolver != nil {
		fetched, merr := u.matchResolver.ResolvePlayerMatchContext(ctx, playerID)
		if merr != nil {
			if u.requireHubAssignmentBinding {
				return ResumeContextResult{}, errcode.NewCause(errcode.ErrUnavailable, merr,
					"cannot resolve canonical game_mode for battle resume; retry")
			}
			h.Warnw("msg", "battle_resume_game_mode_query_degraded", "err", merr, "player_id", playerID)
		} else {
			ma = &fetched
		}
	}
	gameMode := ""
	mapID := uint32(0)
	if ma != nil &&
		ma.State == matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE &&
		ma.MatchID == bl.MatchID {
		gameMode = ma.GameMode
		// map_id 与 game_mode 不同,不 fail-closed:缺失时客户端保留地图名反查兜底。
		mapID = ma.MapID
	}
	if gameMode == "" {
		if u.requireHubAssignmentBinding {
			return ResumeContextResult{}, errcode.New(errcode.ErrUnavailable,
				"canonical game_mode unavailable for battle resume (match_id=%d); retry", bl.MatchID)
		}
		h.Warnw("msg", "battle_resume_game_mode_missing",
			"player_id", playerID, "match_id", bl.MatchID)
	}
	stage := loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_RUNNING
	if bl.PresenceState != locatorv1.LocationState_LOCATION_STATE_BATTLE && ma != nil {
		stage = resumeStageFromMatchStage(ma.Stage)
	}
	return ResumeContextResult{
		Route:      loginv1.ResumeRoute_RESUME_ROUTE_BATTLE,
		MatchID:    bl.MatchID,
		MatchStage: stage,
		GameMode:   gameMode,
		MapID:      mapID,
	}, nil
}

// ResolveBattleEndpoint 为 authenticated IssueDSTicket(battle) 与完整 Login
// 复用同一条票据签发链：统一经 roster 权威门(本人+成员+match live)现签。
func (u *LoginUsecase) ResolveBattleEndpoint(
	ctx context.Context,
	playerID, matchID uint64,
	sessJTI string,
) (addr, ticket string, expMs int64, err error) {
	if playerID == 0 || matchID == 0 {
		return "", "", 0, errcode.New(errcode.ErrInvalidArg,
			"Battle endpoint requires player_id and match_id")
	}
	if u.battleTicketIssuer == nil {
		return "", "", 0, errcode.New(errcode.ErrUnavailable,
			"battle reconnect ticket authority unavailable")
	}
	regionID, cellID := u.routeRegionCell(ctx, playerID)
	result, issueErr := u.battleTicketIssuer.IssueBattleDSTicketAtCell(
		ctx, playerID, matchID, regionID, cellID, sessJTI)
	if issueErr != nil || result == nil || result.BattleDSAddr == "" || result.Ticket == "" {
		return "", "", 0, errcode.NewCause(errcode.ErrUnavailable, issueErr,
			"battle reconnect ticket authority unavailable")
	}
	return result.BattleDSAddr, result.Ticket, result.ExpiresAtMs, nil
}

// battleLocationQueryRetries / battleLocationQueryBackoff:BATTLE 位置查询的有界重试
// (docs/design/battle-reconnect.md §2.3)。local/off 下 locator 是弱依赖；B1 下它是
// Hub 分配前的权威门。偶发抖动/超时不该让
// "正在战斗的玩家"被误判成"不在战斗"从而错进大厅——重试把可恢复失败救回来,拿到
// InBattle 就照常跳回 battle。重试全失败时 local/off 才降级走 Hub，B1 则返回
// Unavailable。重试只发生在错误路径(罕见),不加正常登录延迟。
const (
	battleLocationQueryRetries = 3
	battleLocationQueryBackoff = 50 * time.Millisecond
)

// queryBattleLocation 查玩家 BATTLE 位置,对可恢复的查询失败做有界重试(§2.3)。
// 重试期间 ctx 被取消则立刻返回；重试全失败返回最后一次错误，由调用方按 profile
// 决定 local/off 降级或 B1 fail-closed。
func (u *LoginUsecase) queryBattleLocation(ctx context.Context, playerID uint64) (data.BattleLocation, error) {
	h := plog.With(ctx)
	var lastErr error
	for attempt := 0; attempt < battleLocationQueryRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return data.BattleLocation{}, ctx.Err()
			case <-time.After(battleLocationQueryBackoff):
			}
		}
		bl, err := u.notifier.GetBattleLocation(ctx, playerID)
		if err == nil {
			return bl, nil
		}
		lastErr = err
		h.Debugw("msg", "battle_location_query_retry", "err", err,
			"player_id", playerID, "attempt", attempt+1, "max", battleLocationQueryRetries)
	}
	return data.BattleLocation{}, lastErr
}

// resolveBattleAuthority 是"玩家是否在对局"的统一判定入口(P0 修复 2026-07-15)。
//
// 层次:
//  1. locator presence(30s 租约投影):命中 BATTLE → 直接返回(快路径,后续仍经
//     InspectBattleRoute 三态门验真)。
//  2. presence 未命中:租约可能蒸发/投影未写(READY 与 notifyBattle 之间的窗口)。
//     查 matchmaker 耐久权威(player claim + match 记录,ReleaseMatch 才释放):
//     ACTIVE+READY → 合成 InBattle=true 返回(后续同样过三态门,终局残留 claim
//     会被 Terminal 分流回 Hub,不会误锁);ACTIVE 早期阶段(排队/确认/分配中)
//     → 玩家物理上本就该在 Hub(撒配从 Hub 发起,READY 推送走 hub 连接),不改路由;
//     NONE → Hub。UNKNOWN/查询失败 → B1 fail-closed 可重试,local/off 弱降级。
//
// 第二返回值是本次已查得的 matchmaker 权威(含 canonical game_mode/stage),供调用方
// 组装 ResumeContext 时复用,避免重复 RPC;presence 命中的快路径不查,返 nil
// (game_mode 由 buildBattleResume 按需补查)。
//
// matchResolver 未配(dev/local) → 退化为纯 presence 判定(历史行为)。
func (u *LoginUsecase) resolveBattleAuthority(ctx context.Context, playerID uint64) (data.BattleLocation, *data.PlayerMatchAuthority, error) {
	h := plog.With(ctx)
	bl, err := u.queryBattleLocation(ctx, playerID)
	if err != nil {
		return data.BattleLocation{}, nil, err
	}
	if bl.InBattle || u.matchResolver == nil {
		return bl, nil, nil
	}
	ma, merr := u.matchResolver.ResolvePlayerMatchContext(ctx, playerID)
	if merr != nil {
		if u.requireHubAssignmentBinding {
			return data.BattleLocation{}, nil, errcode.NewCause(errcode.ErrUnavailable, merr,
				"cannot consult durable match authority; retry")
		}
		h.Warnw("msg", "match_authority_query_degraded", "err", merr, "player_id", playerID)
		return bl, nil, nil // local/off 弱降级:保留 presence 判定。
	}
	switch ma.State {
	case matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE:
		if ma.Stage == matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_READY && ma.MatchID != 0 {
			// READY 局但 locator 投影缺失(TTL 蒸发 / notifyBattle 窗口):以耐久权威为准。
			h.Infow("msg", "battle_authority_recovered_from_match_claim",
				"player_id", playerID, "match_id", ma.MatchID)
			return data.BattleLocation{
				InBattle:      true,
				MatchID:       ma.MatchID,
				BattleAddr:    ma.BattleDSAddr,
				PresenceState: bl.PresenceState,
			}, &ma, nil
		}
		// 排队/确认/分配中:玩家应在 Hub 等 READY 推送,不改路由。
		return bl, &ma, nil
	case matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_NONE:
		return bl, &ma, nil
	default:
		// UNKNOWN(索引漂移/坏记录):B1 不猜,可重试;local/off 弱降级。
		if u.requireHubAssignmentBinding {
			return data.BattleLocation{}, nil, errcode.New(errcode.ErrUnavailable,
				"durable match authority state unknown; retry")
		}
		h.Warnw("msg", "match_authority_state_unknown_degraded", "player_id", playerID)
		return bl, nil, nil
	}
}

// tryBattleReconnect 检测玩家是否在 battle DS 中掉线,是则组装"直连 battle DS 重连"的
// LoginResult(docs/design/battle-reconnect.md §2.1)。返回 nil 表示未命中重连 → 调用方继续
// 走正常 hub 登录流程。
//
// local/off 查询失败按既有 §2.3 弱依赖策略返回 (nil,0,nil) 走 Hub；B1 查询失败返回
// Unavailable(可重试)。只有明确 !InBattle 才允许 B1 继续走 Hub。
//
// locator 明确 InBattle 时,用 InspectBattleRoute 三态分诊(租约推导模型):
//
//	Active   → 签票回原局(签票失败 = 可重试 Unavailable,不得继续 Hub);
//	Terminal → locator BATTLE 仅为 TTL 残留,直接走 Hub(无需等 TTL 蒸发),
//	           第二返回值带出残留 match_id 作为 Battle→Hub 回流 fence,
//	           调用方签进 hub 票据 source_match_id claim;
//	Unknown  → 可重试 Unavailable(match 权威抖动;最长 ~30s 租约到期后 InBattle
//	           自然变 false,永不永久卡死)。
//
// 命中重连时不调 NotifyLoginPending / 不分配 hub(避免把 BATTLE 位置顶成 HUB)。
func (u *LoginUsecase) tryBattleReconnect(
	ctx context.Context, playerID uint64, deviceID, sessionToken string, sessExpMs int64, regionID, cellID uint32, sessJTI string,
) (*LoginResult, uint64, error) {
	h := plog.With(ctx)

	bl, ma, err := u.resolveBattleAuthority(ctx, playerID)
	if err != nil {
		h.Warnw("msg", "battle_location_query_failed", "err", err, "player_id", playerID)
		if u.requireHubAssignmentBinding {
			return nil, 0, errcode.NewCause(errcode.ErrUnavailable, err,
				"cannot prove player is outside battle before B1 hub assignment")
		}
		// local/off 保留历史弱依赖降级。
		return nil, 0, nil
	}
	if !bl.InBattle {
		return nil, 0, nil
	}

	// locator 租约说在战斗:用 match 权威三态门区分“仍在活局”与“终局后 TTL 残留”。
	if u.battleTicketIssuer == nil {
		h.Errorw("msg", "battle_reconnect_ticket_issuer_unavailable",
			"player_id", playerID, "match_id", bl.MatchID)
		return nil, 0, errcode.New(errcode.ErrUnavailable, "battle reconnect ticket authority unavailable")
	}
	state, rerr := u.battleTicketIssuer.InspectBattleRoute(ctx, playerID, bl.MatchID)
	switch state {
	case data.BattleRouteTerminal:
		// match 已显式终局(ended/abandoned):locator 记录只是 TTL 残留,直接进 Hub。
		// 残留 match_id 作为回流 fence 带回,签进 hub 票据后 Hub DS 才能立即改写
		// locator(否则要等 TTL 蒸发,期间匹配 4007)。
		h.Infow("msg", "battle_reconnect_skipped_terminal_match",
			"player_id", playerID, "match_id", bl.MatchID)
		return nil, bl.MatchID, nil
	case data.BattleRouteActive:
		// 继续下方签票回原局。
	default:
		// UNKNOWN(match 权威抖动/roster 不可读):不猜。可重试,最长 30s 租约到期自愈。
		h.Warnw("msg", "battle_reconnect_route_unknown_retryable",
			"player_id", playerID, "match_id", bl.MatchID, "err", rerr)
		return nil, 0, errcode.NewCause(errcode.ErrUnavailable, rerr,
			"battle route authority temporarily unavailable; retry")
	}

	// 先拿 canonical game_mode/stage 再签票:resume 组不出来(B1 fail-closed)时
	// 直接可重试退出,不留已签票据的副作用。
	resume, resumeErr := u.buildBattleResume(ctx, playerID, bl, ma)
	if resumeErr != nil {
		return nil, 0, resumeErr
	}

	battleResult, terr := u.battleTicketIssuer.IssueBattleDSTicketAtCell(
		ctx, playerID, bl.MatchID, regionID, cellID, sessJTI)
	if terr != nil {
		// roster/Redis/签票任一失败 → 本次路由可重试,绝不直签或继续分配 Hub。
		h.Errorw("msg", "authorize_battle_reconnect_ticket_failed", "err", terr,
			"player_id", playerID, "match_id", bl.MatchID)
		return nil, 0, errcode.NewCause(errcode.ErrUnavailable, terr,
			"battle reconnect ticket authority unavailable")
	}
	battleTicket, battleExpMs := battleResult.Ticket, battleResult.ExpiresAtMs
	if battleResult.BattleDSAddr == "" {
		return nil, 0, errcode.New(errcode.ErrUnavailable, "battle reconnect target address unavailable")
	}

	// 记录最近登录设备(失败不阻塞登录,只日志告警)。
	if err := u.repo.TouchDevice(ctx, playerID, deviceID); err != nil {
		h.Warnw("msg", "touch_device_failed", "err", err, "player_id", playerID, "device_id", deviceID)
	}

	h.Debugw("msg", "login_battle_reconnect", "player_id", playerID, "device_id", deviceID,
		"match_id", bl.MatchID, "battle_ds_addr", battleResult.BattleDSAddr,
		"battle_ticket_exp_ms", battleExpMs, "region_id", regionID, "cell_id", cellID)

	return &LoginResult{
		PlayerID:          playerID,
		SessionToken:      sessionToken,
		SessionExpMs:      sessExpMs,
		BattleDSAddr:      battleResult.BattleDSAddr,
		BattleTicket:      battleTicket,
		BattleTicketExpMs: battleExpMs,
		MatchID:           bl.MatchID,
		RegionID:          regionID,
		CellID:            cellID,
		Resume:            resume,
	}, 0, nil
}

// GetResumeContext 是前台/冷启动恢复入口(session 仍有效、不走完整 Login 时)。
// 租约推导模型下它是纯读:locator BATTLE 租约活着且 match Active → BATTLE;
// 否则 → HUB。不再有任何 placement 恢复 mutation。
func (u *LoginUsecase) GetResumeContext(ctx context.Context, sessionToken string) (ResumeContextResult, error) {
	if u.verifier == nil {
		return ResumeContextResult{}, errcode.New(errcode.ErrUnavailable, "session verifier unavailable")
	}
	claims, err := u.verifier.VerifySession(sessionToken)
	if err != nil || claims.PlayerID() == 0 {
		return ResumeContextResult{}, errcode.New(errcode.ErrUnauthorized, "invalid session")
	}
	playerID := claims.PlayerID()
	if cerr := u.requireCurrentSession(ctx, playerID, claims.ID); cerr != nil {
		return ResumeContextResult{}, cerr
	}
	out, rerr := u.resolveResumeRoute(ctx, playerID)
	if rerr != nil {
		return out, rerr
	}
	// §9.23 query-first 叠加(migrate ①,默认关):owner 权威确认同路由时补 exact placement,
	// 不改路由决策;owner 查询失败/无记录/路由不一致 → 保持既有 out(新旧双门并行)。
	if u.ownerQueryFirst && u.ownerPlacement != nil {
		out = u.overlayOwnerPlacement(ctx, playerID, out)
	}
	return out, nil
}

// resolveResumeRoute 是恢复路由的既有解析(presence/match 权威;§9.23 query-first 叠加前的基线)。
func (u *LoginUsecase) resolveResumeRoute(ctx context.Context, playerID uint64) (ResumeContextResult, error) {
	if u.notifier == nil {
		return ResumeContextResult{Route: loginv1.ResumeRoute_RESUME_ROUTE_HUB}, nil
	}
	bl, ma, qerr := u.resolveBattleAuthority(ctx, playerID)
	if qerr != nil {
		if u.requireHubAssignmentBinding {
			return ResumeContextResult{}, errcode.NewCause(errcode.ErrUnavailable, qerr,
				"cannot resolve battle location; retry")
		}
		return ResumeContextResult{Route: loginv1.ResumeRoute_RESUME_ROUTE_HUB}, nil
	}
	if !bl.InBattle {
		// HUB 路由:若持有活跃撮合 claim(排队/确认/分配中),把权威
		// match_id/stage/game_mode 带给冷启动客户端恢复撮合会话。
		return hubResumeFromMatchAuthority(ma), nil
	}
	if u.battleTicketIssuer == nil {
		return ResumeContextResult{}, errcode.New(errcode.ErrUnavailable,
			"battle route authority unavailable")
	}
	state, rerr := u.battleTicketIssuer.InspectBattleRoute(ctx, playerID, bl.MatchID)
	switch state {
	case data.BattleRouteActive:
		return u.buildBattleResume(ctx, playerID, bl, ma)
	case data.BattleRouteTerminal:
		return ResumeContextResult{Route: loginv1.ResumeRoute_RESUME_ROUTE_HUB}, nil
	default:
		return ResumeContextResult{}, errcode.NewCause(errcode.ErrUnavailable, rerr,
			"battle route authority temporarily unavailable; retry")
	}
}

// ensureAccount 在开发期假注册 / 免密模式下为不存在的账号首登注册一条记录,返回稳定 player_id。
//
// snowflake 分配新 player_id 写入 accounts(uk_account 唯一),密码存入本次客户端所发
// passwordHash 的 bcrypt 哈希 → 后续用同密码可走正常 bcrypt 校验(真实“首登即注”)。
// 并发下若已被别的请求建好,CreateAccount 返回 ErrAlreadyExists,回查拿已存在的
// player_id(保证同 account 名稳定)。
func (u *LoginUsecase) ensureAccount(ctx context.Context, account, passwordHash string) (uint64, error) {
	bcryptHash, err := passwd.Hash(passwordHash, passwd.DevCost)
	if err != nil {
		return 0, errcode.New(errcode.ErrInternal, "hash password for auto-register: %v", err)
	}
	newID := u.sf.Generate()
	if err := u.repo.CreateAccount(ctx, newID, account, bcryptHash); err != nil {
		if errcode.As(err) == errcode.ErrAlreadyExists {
			id, _, ferr := u.repo.FindByAccount(ctx, account)
			if ferr != nil {
				return 0, ferr
			}
			return id, nil
		}
		return 0, err
	}
	return newID, nil
}

// resolveHub 解析玩家进大厅需要的 hub_ds_addr + hub_ticket(+ 票据过期 unix ms)。
//
// 优先级(W4 ⑥):
//  1. hubAssigner 非 nil → 调 hub_allocator.AssignHub。local/off 按 JOSE alg 验 legacy；
//     RS256 profile 只接受 v2，校验 player / Hub 类型 / 目标实例绑定并读取已验签 exp。
//     解析、验签或绑定任一失败均 fail-closed，不回退估算 exp，也不把坏票返回客户端。
//  2. 仅完全未配置 v2 且 assignment binding 关闭的 local/off，Hub 不可用时才可回退
//     自签 HS256 hub 票据 + 静态 hubDSAddr。
//
// 回退分支保证 login 可独立联调(本机不起 hub_allocator 也能拿到可连 hub 的票据,
// 因为 login 与 hub_allocator 共享同一 JWT secret/issuer/audience)。
//
// regionID / cellID 是玩家确定性路由落点(由 Login / ResolveHubEndpoint 一次算好传入)。
// 回退自签分支把落点盖进 hub 票据(scale-cellular-20m.md §3.3 防跨单元串号);单 Cell / dev 为 0。
// hub_allocator 路径的票据由其自身签发(其内部落点绑定属 Codex/hub_allocator 职责)。
//
// roleID(选角权威化 2026-07-08):玩家已选角色。两条路径都把它盖进 hub 票据 claim:
// AssignHub 透传给 allocator 签;回退自签用 SignHubDSTicketFull。0 = 未选角(claim 不序列化)。
//
// sourceMatchID(Battle→Hub 回流 fence,2026-07-21):三态门证明原对局已终局时的原
// Battle match_id;两条路径都盖进 hub 票据 source_match_id claim,Hub DS 准入后用它写
// SetLocation(HUB, fence) 通过 locator 的 BATTLE→HUB guard,消除终局 TTL 残留导致的
// 「4007 玩家正在战斗中」。0 = 普通登录/非回流。
// sessJTI(R6 复审 P0-3):请求方登录会话 jti;AssignHub 路径透传给 allocator 签进
// hub 票据 sjti claim(VerifyDSTicket 在线核销时复核现行性)。空 = dev 无证据(兼容窗)。
func (u *LoginUsecase) resolveHub(ctx context.Context, playerID uint64, regionID, cellID, roleID uint32, sourceMatchID uint64, sessJTI string) (addr, ticket string, expMs int64, err error) {
	h := plog.With(ctx)

	if u.hubAssigner != nil {
		assign, aerr := u.hubAssigner.AssignHub(ctx, playerID, u.hubRegion, 0, roleID, sourceMatchID, sessJTI)
		if aerr == nil && assign == nil {
			aerr = errcode.New(errcode.ErrUnavailable, "hub allocator returned an empty assignment")
		}
		if aerr == nil {
			expMs, verr := u.verifyHubAssignmentTicket(playerID, assign)
			if verr != nil {
				h.Errorw("msg", "hub_assigner_returned_invalid_ticket", "err", verr,
					"player_id", playerID, "hub_pod", assign.HubPodName)
				return "", "", 0, errcode.New(errcode.ErrUnavailable,
					"hub allocator returned an invalid ticket: %v", verr)
			}
			h.Debugw("msg", "hub_assigned", "player_id", playerID,
				"hub_pod", assign.HubPodName, "shard_id", assign.ShardID, "hub_ds_addr", assign.HubDSAddr)
			return assign.HubDSAddr, assign.HubTicket, expMs, nil
		}
		if u.requireHubAssignmentBinding || u.rs256DSTicketProfileEnabled() {
			return "", "", 0, errcode.New(errcode.ErrUnavailable,
				"hub allocator required for RS256/assignment-bound ticket: %v", aerr)
		}
		// hub_allocator 不可用 → 回退自签,不阻断登录(玩家仍可凭票据连静态 hub DS)
		h.Warnw("msg", "hub_assign_failed_fallback_self_sign", "err", aerr, "player_id", playerID)
	}
	if u.requireHubAssignmentBinding || u.rs256DSTicketProfileEnabled() {
		return "", "", 0, errcode.New(errcode.ErrUnavailable,
			"hub allocator is required by the RS256/assignment-bound ticket profile")
	}

	ticket, expMs, err = u.signer.SignHubDSTicketFull(playerID, regionID, cellID, roleID, sourceMatchID, uuid.NewString())
	if err != nil {
		return "", "", 0, errcode.New(errcode.ErrInternal, "sign hub ticket failed: %v", err)
	}
	return u.hubDSAddr, ticket, expMs, nil
}

// routeRegionCell 算玩家确定性路由落点(scale-cellular-20m.md §3.2/§3.3)。
//
// router 为 nil(单 Cell / dev)或 Route 报错(配置缺口)→ 降级为 0/0(同单 Cell 行为),
// 仅告警不阻断登录。
func (u *LoginUsecase) routeRegionCell(ctx context.Context, playerID uint64) (regionID, cellID uint32) {
	if u.router == nil {
		return 0, 0
	}
	loc, err := u.router.Route(playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "cellroute_failed", "err", err, "player_id", playerID)
		return 0, 0
	}
	return loc.RegionID, loc.CellID
}

// ResolveHubEndpoint 复用登录时的 hub 分配链路(resolveHub → hub_allocator.AssignHub),
// 返回"当前有效"的大厅 DS 地址 + 一张全新的一次性 hub 票据。
//
// 用途(结算返回大厅):客户端不能复用登录时缓存的 hub_ds_addr / hub_ticket。
//   - 旧 Hub DS 可能已被 Agones 判 Unhealthy/Deleted/换端口,缓存地址已失效;
//   - 旧 hub 票据的 jti 已在首次进大厅时被消费,复用会被 DS 判 ticket replay。
//
// AssignHub 幂等且自愈:玩家原分片仍 ready → 重签票返回同地址;原分片下线 → 自动改派到
// 健康分片并返回新地址。两种情况都返回新签的票据(新 jti),不破坏 DS ticket 一次性语义。
//
// hubAssigner 未配 / 调用失败时,resolveHub 回退自签票据 + 静态 hubDSAddr(与登录一致,不阻断)。
//
// P0 止血(2026-07-14,docs §7.16.3 候选 A 下沉):本入口是客户端直连 battle 失败/重连超时
// 回大厅的旁路,必须先过 active-BATTLE 三态权威门;BATTLE_ACTIVE/UNKNOWN 时零副作用拒绝,
// 绝不先 AssignHub 再补偿。
// sessJTI(R6 复审 P0-3):请求方登录会话 jti,签进 hub 票 sjti claim;空 = 兼容窗。
func (u *LoginUsecase) ResolveHubEndpoint(ctx context.Context, playerID uint64, sessJTI string) (addr, ticket string, expMs int64, err error) {
	return u.ResolveHubEndpointFromMatch(ctx, playerID, 0, sessJTI)
}

// ResolveHubEndpointFromMatch 是结算/离开战斗回大厅路径。sourceMatchID 仅作日志
// 参考;路由权威完全由 guardHubRouteAgainstActiveBattle 的三态门决定
// (Active→拒绝,Terminal→放行,Unknown→可重试)。回流 fence 同样取门的权威判定
// (locator BATTLE 残留的 match_id),不信客户端上报的 sourceMatchID。
func (u *LoginUsecase) ResolveHubEndpointFromMatch(ctx context.Context, playerID, sourceMatchID uint64, sessJTI string) (addr, ticket string, expMs int64, err error) {
	if playerID == 0 {
		return "", "", 0, errcode.New(errcode.ErrInvalidArg, "playerID must be > 0")
	}
	fenceMatchID, gerr := u.guardHubRouteAgainstActiveBattle(ctx, playerID)
	if gerr != nil {
		return "", "", 0, gerr
	}
	regionID, cellID := u.routeRegionCell(ctx, playerID)
	// 选角权威化:返回大厅路径也把已选角盖进新票(与登录同语义,DS 重入时同样能 spawn 对角色)。
	return u.resolveHub(ctx, playerID, regionID, cellID, u.loadSelectedRole(ctx, playerID), fenceMatchID, sessJTI)
}

// guardHubRouteAgainstActiveBattle 是所有非 Login 主链 Hub 签票入口(IssueDSTicket(hub) /
// SelectRole)的 active-BATTLE 三态权威门(P0 止血,封 battle-reconnect.md §7.3 A 双归属漏洞):
//
//	ACTIVE  (locator BATTLE 且 roster 权威证明 live) → ErrInvalidState 拒绝,零副作用;
//	  客户端应重新 Login 走权威路由(会被 tryBattleReconnect 直连回原局)。
//	TERMINAL(locator BATTLE 但投影记录显式 ended/abandoned) → 放行。
//	  这是唯一允许的“BATTLE 残留”证明,覆盖正常结算回大厅(位置 TTL 尚未过期)。
//	UNKNOWN (locator 查询失败 / roster 漂移 / 记录缺失 / stale / 不可读) → fail-closed:
//	  locator 阳性 BATTLE 下不分 profile 一律 ErrUnavailable;仅“locator 查询本身失败”
//	  在 local/off 保留历史弱降级(dev 裸跑)。
//
// P0 修复(2026-07-15,Codex 复审):不再把通用 ErrPermissionDeny 当终态证明——它同时
// 覆盖 roster 漂移/非成员/记录缺失,那些必须 UNKNOWN 拒绝。只有投影记录显式终态才放行。
//
// 第一返回值(2026-07-21):Terminal 放行时返回该终局对局的 match_id 作为 Battle→Hub
// 回流 fence,调用方把它签进 hub 票据 source_match_id claim——locator 的 BATTLE 残留
// 只接受带同 match_id 令牌的 HUB 写(guardTransition,不变量 §1),没有 fence 的 Hub
// 准入会让 locator 停留在 BATTLE 直到 TTL 过期,期间匹配一律 4007。其余分支返回 0。
func (u *LoginUsecase) guardHubRouteAgainstActiveBattle(ctx context.Context, playerID uint64) (uint64, error) {
	h := plog.With(ctx)
	if u.notifier == nil {
		if u.requireHubAssignmentBinding {
			return 0, errcode.New(errcode.ErrUnavailable,
				"player locator is required before hub ticket issuance")
		}
		return 0, nil // local/off 无 locator:保留历史行为(dev 裸跑)。
	}
	bl, _, err := u.resolveBattleAuthority(ctx, playerID) // hub 门只关心在局与否,不需要 game_mode
	if err != nil {
		if u.requireHubAssignmentBinding {
			return 0, errcode.NewCause(errcode.ErrUnavailable, err,
				"cannot prove player is outside battle before hub ticket issuance")
		}
		h.Warnw("msg", "hub_route_gate_locator_degraded", "err", err, "player_id", playerID)
		return 0, nil // local/off 保留历史弱降级。
	}
	if !bl.InBattle {
		return 0, nil
	}
	// locator 明确 InBattle:必须由 roster 权威区分“仍在活局”与“显式终局后 TTL 残留”。
	// 不可判定时不分 profile 一律 fail-closed:阳性 BATTLE 信号下猜“已结束”就是双归属。
	if u.battleTicketIssuer == nil {
		return 0, errcode.New(errcode.ErrUnavailable,
			"battle route authority unavailable while locator reports BATTLE")
	}
	state, rerr := u.battleTicketIssuer.InspectBattleRoute(ctx, playerID, bl.MatchID)
	switch state {
	case data.BattleRouteActive:
		h.Warnw("msg", "hub_route_rejected_active_battle",
			"player_id", playerID, "match_id", bl.MatchID)
		return 0, errcode.New(errcode.ErrInvalidState,
			"player is in active battle (match_id=%d); reconnect via Login instead of hub ticket", bl.MatchID)
	case data.BattleRouteTerminal:
		// 权威记录显式终态(ended/abandoned) → locator BATTLE 仅为 TTL 残留,放行 Hub
		// (正常结算回大厅),并把残留 match_id 作为回流 fence 交给签票路径。
		return bl.MatchID, nil
	default:
		// UNKNOWN(含 roster 漂移/非成员/记录缺失/stale/错误):不得猜测,拒绝。
		h.Warnw("msg", "hub_route_rejected_unknown_battle_state",
			"player_id", playerID, "match_id", bl.MatchID, "err", rerr)
		return 0, errcode.NewCause(errcode.ErrUnavailable, rerr,
			"cannot prove battle is over before hub ticket issuance")
	}
}

// loadSelectedRole 读玩家已选角色(player_roles)。弱依赖:roleRepo 未配 / 读失败 → 0
// (未选角语义,仅告警不阻断)。只用于读路径;写路径(SelectRole)失败必须报错。
func (u *LoginUsecase) loadSelectedRole(ctx context.Context, playerID uint64) uint32 {
	if u.roleRepo == nil {
		return 0
	}
	roleID, err := u.roleRepo.GetRole(ctx, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "load_selected_role_failed", "err", err, "player_id", playerID)
		return 0
	}
	return roleID
}

// SelectRole 选角用例(选角权威化 2026-07-08,docs 综述见 login.proto SelectRole 注释)。
//
// 流程:
//  1. 校验 roleID:必须 >0;allowedRoleIDs 非空时必须在白名单;白名单为空时 fail-closed
//     一律拒绝(防改包客户端签任意 role_id 进 hub 票据),仅 devAllowAnyRole=true 放宽为只校非 0。
//  2. roleRepo.SetRole 落库(权威数据,失败必须报错——没落库就不能发票,否则重登后角色回退)。
//  3. resolveHub(带 roleID) → hub_allocator 把 role_id 签进全新 hub 票据 + 返回当前有效地址。
//
// 幂等:重复选同角 / 换角重选都是覆盖式 upsert + 重签新票(新 jti),不破坏票据一次性语义。
// roleRepo 未配(dev 裸跑)时跳过落库只签票,Warn 提示。
//
// sessJTI(R6 复审 P0-3):请求方会话 jti(service 层已预检 == 当前一代)。两个用途:
//  1. 角色落库 precommit fencing:SetRole 事务在 UPSERT 后、COMMIT 前复核 jti 仍现行,
//     被顶旧会话的角色写 ROLLBACK 不落地(不再"落库后才终检");
//  2. 签进 hub 票据 sjti claim(VerifyDSTicket 在线核销时复核现行性)。
//
// 空 = dev 无网关证据:两处都跳过(与其余现行性门 dev 语义一致)。
func (u *LoginUsecase) SelectRole(ctx context.Context, playerID uint64, roleID uint32, sessJTI string) (addr, ticket string, expMs int64, err error) {
	h := plog.With(ctx)
	if playerID == 0 {
		return "", "", 0, errcode.New(errcode.ErrInvalidArg, "playerID must be > 0")
	}
	if roleID == 0 {
		return "", "", 0, errcode.New(errcode.ErrInvalidArg, "roleID must be > 0")
	}
	// SelectRole 也是 Hub 物理副作用入口:先过 active-BATTLE 三态权威门。
	fenceMatchID, gerr := u.guardHubRouteAgainstActiveBattle(ctx, playerID)
	if gerr != nil {
		return "", "", 0, gerr
	}
	if len(u.allowedRoleIDs) > 0 {
		if _, ok := u.allowedRoleIDs[roleID]; !ok {
			h.Warnw("msg", "select_role_not_allowed", "player_id", playerID, "role_id", roleID)
			return "", "", 0, errcode.New(errcode.ErrInvalidArg, "role_id=%d not allowed", roleID)
		}
	} else if !u.devAllowAnyRole {
		// fail-closed:白名单没配就放行任意 role_id = 改包客户端可把任意角色配置 ID 签进 hub 票据
		// (hub_allocator 无二次校验)。生产必须配 allowed_role_ids;dev 宽松需显式开 dev_allow_any_role。
		h.Errorw("msg", "select_role_rejected_no_whitelist", "player_id", playerID, "role_id", roleID,
			"hint", "configure login.allowed_role_ids (prod) or enable login.dev_allow_any_role (dev only)")
		return "", "", 0, errcode.New(errcode.ErrInvalidState, "role selection disabled: allowed_role_ids not configured")
	}

	if u.roleRepo != nil {
		// 双层 fencing(R6 P0-3 + R7 P0-4):
		//  1. expectedSessJTI → SetRole 在同一 MySQL 事务内 FOR UPDATE 复核持久化会话代际,
		//     与登录代际写串行化,确定性挡掉被顶旧会话(主防线;由 sessionGenEnforce 门
		//     控制——滚动窗口内旧 Login Pod 不写代际,MySQL 行陈旧会误拒合法会话,
		//     必须全 fleet emit + 旧版本排空后才激活,见 SetSessionGenerationEnforce);
		//  2. precommit → COMMIT 前读 Redis 会话权威复核(纵深,不受强制门控制)。
		// sessJTI 空(dev 无网关证据)→ 两层都跳过,单语句路径行为不变。
		var precommit func(context.Context) error
		expectedSessJTI := ""
		if sessJTI != "" && u.sessions != nil {
			if u.sessionGenEnforce {
				expectedSessJTI = sessJTI
			}
			precommit = func(pctx context.Context) error {
				return u.requireCurrentSession(pctx, playerID, sessJTI)
			}
		}
		if serr := u.roleRepo.SetRole(ctx, playerID, roleID, expectedSessJTI, precommit); serr != nil {
			h.Errorw("msg", "select_role_persist_failed", "err", serr, "player_id", playerID, "role_id", roleID)
			return "", "", 0, serr
		}
	} else {
		h.Warnw("msg", "select_role_repo_nil_skip_persist", "player_id", playerID, "role_id", roleID)
	}

	regionID, cellID := u.routeRegionCell(ctx, playerID)
	addr, ticket, expMs, err = u.resolveHub(ctx, playerID, regionID, cellID, roleID, fenceMatchID, sessJTI)
	if err != nil {
		h.Errorw("msg", "select_role_resolve_hub_failed", "err", err, "player_id", playerID, "role_id", roleID)
		return "", "", 0, err
	}
	h.Debugw("msg", "select_role_ok", "player_id", playerID, "role_id", roleID, "hub_ds_addr", addr)
	return addr, ticket, expMs, nil
}

// verifyHubAssignmentTicket 验证 hub_allocator 返回的票据并读取已验签 exp。
//
// 迁移期只允许两条显式路径：HS256=legacy，RS256=v2。alg 仅用于选择 verifier，随后分支
// 必须完成各自签名/claims 校验；其它算法、缺 verifier、坏票和不完整绑定全部 fail-closed。
func (u *LoginUsecase) verifyHubAssignmentTicket(playerID uint64, assign *data.HubAssignment) (int64, error) {
	if assign == nil || assign.HubTicket == "" {
		return 0, errcode.New(errcode.ErrLoginTicketInvalid, "hub assignment ticket is empty")
	}
	alg, err := auth.DSTicketAlgorithm(assign.HubTicket)
	if err != nil {
		return 0, err
	}
	switch alg {
	case "HS256":
		// v2 verifier 非 nil 是 Login 主链的机械 RS256-only 开关；不能因为收到一张
		// HS256 票就退回 legacy verifier。SessionToken 的 HS256 验证不经过本函数。
		if u.rs256DSTicketProfileEnabled() {
			return 0, errcode.New(errcode.ErrLoginTicketInvalid,
				"legacy HS256 DSTicket is disabled by the RS256 profile")
		}
		if u == nil || u.verifier == nil {
			return 0, errcode.New(errcode.ErrUnavailable, "legacy DSTicket verifier unavailable")
		}
		claims, err := u.verifier.VerifyDSTicket(assign.HubTicket)
		if err != nil {
			return 0, err
		}
		if claims.PlayerID() != playerID || claims.DSType != string(auth.DSTypeHub) {
			return 0, errcode.New(errcode.ErrLoginTicketInvalid,
				"legacy hub ticket player or ds_type mismatch")
		}
		if claims.ExpiresAt == nil {
			return 0, errcode.New(errcode.ErrLoginTicketInvalid, "legacy hub ticket missing exp")
		}
		// 兼容窗内的旧票允许完全没有实例绑定；一旦携带 pod，就必须与 allocator 响应一致。
		if claims.DSPodName != "" && claims.DSPodName != assign.HubPodName {
			return 0, errcode.New(errcode.ErrLoginTicketInvalid, "legacy hub ticket target pod mismatch")
		}
		if u.requireHubAssignmentBinding &&
			(assign.HubPodName == "" || claims.DSPodName != assign.HubPodName ||
				claims.DSInstanceUID == "" || claims.DSProtocolEpoch == 0 ||
				claims.DSCredentialGen == 0 || claims.DSCredentialJTI == "" ||
				claims.HubAssignmentID == "" || claims.DSWriterEpoch != auth.DSAuthWriterEpochV2) {
			return 0, errcode.New(errcode.ErrLoginTicketInvalid,
				"legacy hub ticket assignment binding is incomplete")
		}
		return claims.ExpiresAt.UnixMilli(), nil

	case "RS256":
		if u == nil || u.v2Verifier == nil {
			return 0, errcode.New(errcode.ErrUnavailable, "DSTicket v2 verifier unavailable")
		}
		claims, err := u.v2Verifier.Verify(assign.HubTicket)
		if err != nil {
			return 0, err
		}
		if claims.PlayerID() != playerID || claims.DSType != string(auth.DSTypeHub) {
			return 0, errcode.New(errcode.ErrLoginTicketInvalid,
				"hub DSTicket v2 player or ds_type mismatch")
		}
		if assign.HubPodName == "" || claims.DSPodName != assign.HubPodName {
			return 0, errcode.New(errcode.ErrLoginTicketInvalid, "hub DSTicket v2 target pod mismatch")
		}
		if claims.DSInstanceUID == "" || claims.DSInstanceEpoch == 0 ||
			claims.HubAssignmentID == "" ||
			(claims.ReleaseTrack != auth.ReleaseTrackStable && claims.ReleaseTrack != auth.ReleaseTrackCanary) {
			return 0, errcode.New(errcode.ErrLoginTicketInvalid,
				"hub DSTicket v2 instance binding is incomplete")
		}
		if claims.ExpiresAt == nil {
			return 0, errcode.New(errcode.ErrLoginTicketInvalid, "hub DSTicket v2 missing exp")
		}
		return claims.ExpiresAt.UnixMilli(), nil

	default:
		// DSTicketAlgorithm 已先拒绝其它 alg；保留 default 作为未来改动的 fail-closed 保险。
		return 0, errcode.New(errcode.ErrLoginTicketInvalid, "DSTicket algorithm unsupported")
	}
}

// Logout 真实化(W3 ②):验 session_token 拿 player_id,DEL redis session。
//
// 客户端实际很少调 Logout(直接关进程),所以本路径不要求强一致:
// token 验签失败 → 也返回 OK(让客户端能 fire-and-forget,清理本地状态);只记日志。
func (u *LoginUsecase) Logout(ctx context.Context, sessionToken string) error {
	h := plog.With(ctx)
	if u.verifier == nil || u.sessions == nil {
		h.Infow("msg", "logout_ok_noop")
		return nil
	}
	claims, err := u.verifier.VerifySession(sessionToken)
	if err != nil {
		// token 不合法不算业务错(可能客户端 token 过期了),直接返 OK
		h.Debugw("msg", "logout_verify_session_failed", "err", err)
		return nil
	}
	playerID := claims.PlayerID()
	if playerID == 0 {
		h.Warnw("msg", "logout_session_no_player")
		return nil
	}
	// P0 修复(2026-07-15,codex P0-10):只删"本 token 对应的那一代 session"。
	// 顶号后旧设备的迟到 Logout 携带旧 jti,CAS 不命中 → 不影响新设备 session。
	deleted, err := u.sessions.DeleteIfJTI(ctx, playerID, claims.ID)
	if err != nil {
		h.Errorw("msg", "logout_session_del_failed", "err", err, "player_id", playerID)
		return err
	}
	if !deleted {
		h.Infow("msg", "logout_stale_session_ignored", "player_id", playerID)
		return nil
	}
	// MySQL 代际墓碑(R8 收口,P2 纵深):只删 Redis 会让 player_session_generations
	// 行继续持有已登出的旧 jti。条件 CAS 写(仅行内仍是本 jti 才改墓碑),并发新登录
	// 已轮换则 no-op,不毒化新会话。best-effort:Redis 删除(主权威)已成功,MySQL
	// 墓碑失败仅告警——残留旧 jti 行只在 Redis 同时失效的双故障下才可见,且所有
	// 现行性门对 Redis 会话消失均 fail-closed。
	if u.sessionGen != nil {
		if _, terr := u.sessionGen.TombstoneSessionJTI(ctx, playerID, claims.ID); terr != nil {
			h.Warnw("msg", "logout_session_generation_tombstone_failed_weak",
				"player_id", playerID, "err", terr)
		}
	}
	// owner 迁移释放(owner-authority.md migrate ⑤,弱依赖):显式登出后释放当前 owner。
	// Query→Release 携带观察到的 epoch+operation(compare-delete 自己):并发迁移竞态下
	// Release 在 owner 侧幂等 no-op,绝不误删新 owner;失败仅告警,不影响登出结果。
	if u.ownerReleaser != nil {
		if rec, oerr := u.ownerReleaser.QueryOwner(ctx, playerID); oerr != nil {
			h.Warnw("msg", "logout_owner_query_failed_weak", "player_id", playerID, "err", oerr)
		} else if rec.OwnerType != 0 {
			if rerr := u.ownerReleaser.ReleaseOwner(ctx, playerID, rec.OwnerEpoch, rec.OperationID); rerr != nil {
				h.Warnw("msg", "logout_owner_release_failed_weak", "player_id", playerID, "err", rerr)
			}
		}
	}
	h.Debugw("msg", "logout_ok", "player_id", playerID)
	return nil
}

// RequireTicketSessionCurrent 票据兑换点会话复核(R6 复审 P0-3 → R8 收口,§9.23):DS 经
// VerifyDSTicket 在线核销票据时,对票内 sjti claim 复核会话权威——签发与响应写出
// 之间被新登录轮换的旧票,即使已交付到旧设备,也在兑换点作废。
//   - sessions 未配(dev 裸跑):跳过(无权威可比,与其余现行性门同语义);
//   - sjti 空(R8 收口,P0-5 滚动兼容):由 requireTicketSJTI 门控制——默认兼容档
//     告警放行(滚动窗口内旧签发面仍持续签空票,硬拒会令混版期战斗准入整体不可用);
//     全 fleet 签发面必带 sjti + 旧版本排空 + 等满票据最大 TTL 后开门硬拒(空票是
//     绕过会话绑定的万能票,收口后不再允许)。发布顺序见
//     docs/design/session-generation-rollout.md;
//   - 权威不可达:ErrUnavailable(fail-closed,DS 拒绝准入可重试);
//   - 会话已消失:ErrUnauthorized;已被新登录轮换:ErrSessionSuperseded。
func (u *LoginUsecase) RequireTicketSessionCurrent(ctx context.Context, playerID uint64, ticketSessJTI string) error {
	if u == nil || u.sessions == nil {
		return nil
	}
	if ticketSessJTI == "" {
		if u.requireTicketSJTI {
			plog.With(ctx).Warnw("msg", "ticket_missing_session_binding_rejected", "player_id", playerID)
			return errcode.New(errcode.ErrUnauthorized,
				"ticket lacks session binding (sjti); reissue required")
		}
		plog.With(ctx).Infow("msg", "ticket_missing_session_binding_compat_allow",
			"player_id", playerID,
			"hint", "混版兼容窗;签发面排空+等满票据最大 TTL 后开 login.require_ticket_sjti 收口")
		return nil
	}
	return u.requireCurrentSession(ctx, playerID, ticketSessJTI)
}

// reconcileFailedSessionWrite 收口「MySQL 代际已提交、Redis 条件写报错」留下的跨存储
// 不确定态(R10 复审 P0-1)。前置:调用方已判定这是基础设施错误(非 ErrSessionSuperseded),
// 本次登录必定失败且不交付任何凭据。
//
// 难点:「Redis 报错 ≠ Redis 未提交」。Lua 已执行、应答在网络上丢失时 Redis 实际已持有
// 本次新 jti。旧实现用 GetJTI 读回判断,但**读回本身也可能失败**——此时旧实现落到
// else 分支无条件回补 MySQL 旧 jti,即在「Redis=新 jti」的可能性下造出
// 「Redis=新、MySQL=旧」的跨存储撕裂(§9.22:结果不确定必须 fail-closed,禁止猜测)。
//
// 本实现不猜,而是把不确定态**消灭**掉:
//
//	① sessions.DeleteIfJTI(sessJTI) 是按 jti 的 CAS 删。返回无错误即证明「Redis 此刻
//	   不再持有本次 jti」——要么本就没提交(未命中),要么已提交并被本次 CAS 精确撤销
//	   (deleted=true;并发新登录已轮换则不命中,绝不误删新会话)。此时把 MySQL 条件
//	   回补到覆盖前的 jti 是**可证明一致**的,保留了 R9 P0-2 的目的(不误拒上一代
//	   合法会话的 SetRole)。
//	② DeleteIfJTI 本身失败(Redis 不可达)= 仍无法证明 Redis 状态。此时禁止回补,改为
//	   条件墓碑 MySQL 本次代际行:任何陈旧 jti 都不再匹配,两侧一致地拒绝所有旧凭据
//	   (fail-closed)。墓碑是 CAS,只命中本次登录写入的行,并发新登录已轮换则 no-op。
//	   玩家下一次成功登录原子推进两个存储自愈(客户端对本次失败拿到的是可重试错误)。
//
// 全部为 best-effort 补偿:补偿本身失败只记 Error(登录已失败,不会因此交付能力)。
//
// R11 复审 P0-1 问题 B:补偿**不得**跑在请求 ctx 上。触发 Redis 写失败的原因常常就是
// handler deadline / 客户端断连导致的 ctx 取消(login-dev.yaml grpc timeout 15s),那时
// 这三步全部立刻失败,只剩日志,不确定态原样留在库里。改用 plog.Detach 派生脱离取消、
// 不携带请求 transport 的 ctx(CLAUDE.md §16.7 指定的 helper),并加独立有界超时。
func (u *LoginUsecase) reconcileFailedSessionWrite(
	ctx context.Context, playerID uint64, sessJTI string, sessGen uint64, lease data.SessionGenerationLease,
) {
	h := plog.With(ctx)
	ctx, cancel := context.WithTimeout(plog.Detach(ctx), sessionReconcileTimeout)
	defer cancel()
	if lease.SnapshotUnknown {
		// 覆盖前状态不可知(代际来自不确定 COMMIT 的读回判定)→ 回补两条路都会错误复活
		// 旧凭据,只能条件墓碑:两侧一致拒绝所有陈旧 jti。
		u.tombstoneFailedSessionGeneration(ctx, playerID, sessJTI, sessGen, nil)
		return
	}
	deleted, derr := u.sessions.DeleteIfJTI(ctx, playerID, sessJTI)
	if derr != nil {
		// ② 不可证明 → 墓碑 fail-closed,不制造「Redis 新 / MySQL 旧」撕裂。
		u.tombstoneFailedSessionGeneration(ctx, playerID, sessJTI, sessGen, derr)
		return
	}
	if deleted {
		// Redis 确实已提交本次 jti(应答丢失),已被 CAS 精确撤销;无人持有该 jti。
		h.Warnw("msg", "session_write_reconcile_committed_rolled_back",
			"player_id", playerID, "gen", sessGen)
	}
	// ① Redis 已确证不持有本次 jti → 条件回补 MySQL(generation 不回退)。
	restored, rerr := u.sessionGen.RestoreSessionJTI(ctx, playerID, sessJTI, lease)
	switch {
	case rerr != nil:
		h.Errorw("msg", "session_generation_restore_failed", "err", rerr,
			"player_id", playerID, "gen", sessGen)
	case !restored:
		h.Infow("msg", "session_generation_restore_noop",
			"player_id", playerID, "gen", sessGen)
	}
}

// sessionReconcileTimeout 是跨存储补偿链的独立预算(脱离请求 ctx 后必须自带上界,
// 否则补偿会变成无界后台调用)。三步条件写都是单语句 CAS,5s 足够且远小于会话 TTL。
const sessionReconcileTimeout = 5 * time.Second

// tombstoneFailedSessionGeneration 条件墓碑本次代际行:只命中仍持有本次 jti 的行,
// 并发新登录已轮换则 no-op(不毒化新会话)。redisErr 仅用于日志区分触发原因。
func (u *LoginUsecase) tombstoneFailedSessionGeneration(
	ctx context.Context, playerID uint64, sessJTI string, sessGen uint64, redisErr error,
) {
	h := plog.With(ctx)
	tombstoned, terr := u.sessionGen.TombstoneSessionJTI(ctx, playerID, sessJTI)
	if terr != nil {
		h.Errorw("msg", "session_write_reconcile_tombstone_failed",
			"player_id", playerID, "gen", sessGen, "err", terr, "redis_err", redisErr,
			"hint", "两存储状态不可证明且墓碑失败;下一次成功登录会原子推进两存储自愈")
		return
	}
	h.Errorw("msg", "session_write_reconcile_unknown_tombstoned",
		"player_id", playerID, "gen", sessGen, "tombstoned", tombstoned, "err", redisErr,
		"hint", "两存储一致性不可证明 → 墓碑代际行 fail-closed,旧凭据一律失效")
}

// resolveAmbiguousSessionGeneration 判定「COMMIT 结果不确定」(R11 复审 P0-1 问题 A)。
//
// 本次 jti 是全局唯一的一次性标记,因此读回权威行即可把不确定态判成事实,三种结果:
//
//	① 行内 jti == 本次 jti → COMMIT **确实生效**。返回成功继续登录:随后 Redis 条件写
//	   与凭据交付照常进行,两存储收敛到同一代际、该 jti 被真正交付。这条路径同时消灭了
//	   审核列出的三种禁止终态(两存储不同代 / 未交付 JTI 成为当前代 / 旧凭据复活)。
//	   代价:覆盖前快照不可信(随失败事务丢失),故标记 SnapshotUnknown,后续补偿只准墓碑。
//	② 行不存在,或 jti 已不是本次 → 本次写没落地,或已被更高代际的登录取代(定序输家)。
//	   直接失败,零补偿:行属于赢家,任何回补/墓碑都会破坏别人的登录。
//	③ 读回本身失败 → 仍不可判定。条件墓碑(命中则两侧一致拒绝所有旧凭据,未命中即 no-op)
//	   后 fail-closed 失败本次登录,不留「MySQL 领先且无人可用」的静默态。
func (u *LoginUsecase) resolveAmbiguousSessionGeneration(
	ctx context.Context, playerID uint64, sessJTI string, lease data.SessionGenerationLease,
) (data.SessionGenerationLease, error) {
	h := plog.With(ctx)
	// 判定读与补偿都不能跑在可能已取消的请求 ctx 上(同问题 B)。
	probeCtx, cancel := context.WithTimeout(plog.Detach(ctx), sessionReconcileTimeout)
	defer cancel()

	currentJTI, generation, found, err := u.sessionGen.LoadSessionGeneration(probeCtx, playerID)
	if err != nil {
		h.Errorw("msg", "session_generation_commit_unresolved", "player_id", playerID, "err", err,
			"hint", "COMMIT 结果不确定且读回失败 → 条件墓碑后 fail-closed")
		u.tombstoneFailedSessionGeneration(probeCtx, playerID, sessJTI, lease.Generation, err)
		return data.SessionGenerationLease{}, errcode.NewCause(errcode.ErrUnavailable, err,
			"session generation commit result unresolved; login rejected")
	}
	if !found || currentJTI != sessJTI {
		h.Warnw("msg", "session_generation_commit_did_not_land", "player_id", playerID,
			"found", found, "hint", "本次写未落地或已被更高代际登录取代;零补偿失败本次登录")
		return data.SessionGenerationLease{}, errcode.New(errcode.ErrUnavailable,
			"session generation commit did not land; retry login")
	}
	h.Warnw("msg", "session_generation_commit_ambiguous_confirmed_landed",
		"player_id", playerID, "gen", generation,
		"hint", "COMMIT 已生效(仅回包丢失) → 按成功继续,两存储收敛到本代际并交付凭据")
	return data.SessionGenerationLease{Generation: generation, SnapshotUnknown: true}, nil
}

// fenceLoginDelivery 登录副作用交付终检(R5 复审 P0-5,INC-20260722-004):
// Login 在 sessions.Set 写入 sessJTI 后仍有分配/locator/签票等多步副作用,期间并发
// 新登录可再次轮换 jti;交付凭据前必须复核本流程写入的 jti 仍是当前一代。
//
//   - sessions 未配(dev 裸跑)→ 跳过(与其余现行性门同语义);
//   - 权威不可达 → ErrUnavailable(fail-closed 扣留凭据,客户端重试);
//   - 已被轮换/会话消失 → ErrSessionSuperseded(旧设备转交互登录,不得自动反顶)。
//
// 诚实边界:这是"检查后交付",非跨存储原子事务——复核通过与响应写出之间仍有
// 进程内窗口,但该窗口内旧流程只是交付了"已再次被轮换的 token",后续任何按
// §9.23/P0-1 过门的请求都会被拒,不构成持续能力。
func (u *LoginUsecase) fenceLoginDelivery(ctx context.Context, playerID uint64, sessJTI string) error {
	if u.sessions == nil {
		return nil
	}
	cur, found, err := u.sessions.GetJTI(ctx, playerID)
	if err != nil {
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"session authority unavailable; login credentials withheld")
	}
	if !found || cur != sessJTI {
		plog.With(ctx).Warnw("msg", "login_delivery_fenced_superseded", "player_id", playerID)
		return errcode.New(errcode.ErrSessionSuperseded,
			"session superseded during login; credentials withheld")
	}
	return nil
}

// requireCurrentSession 校验调用方持有的 session token 仍是"当前一代"(P0 修复
// 2026-07-15,codex P0-10)。JWT 验签只证明"曾经登录过",不证明"未被顶号":
// 顶号后旧 token 在 exp 前仍能验过,两台设备可各自拿票造成双在场。
// 本门用 Redis session 的 jti 做现行性判定:不匹配 = 已被新登录取代,拒绝。
// sessions 未配(dev 裸跑) → 跳过;Redis 故障 → 可重试 Unavailable(fail-closed)。
func (u *LoginUsecase) requireCurrentSession(ctx context.Context, playerID uint64, jti string) error {
	if u.sessions == nil {
		return nil
	}
	cur, found, err := u.sessions.GetJTI(ctx, playerID)
	if err != nil {
		return errcode.NewCause(errcode.ErrUnavailable, err, "session authority unavailable; retry")
	}
	if !found {
		return errcode.New(errcode.ErrUnauthorized, "session expired or logged out; login again")
	}
	if jti == "" {
		// 缺 jti 证据(绕网关/无 payload 头):无法证明现行性,维持普通未授权语义,
		// 不得当顶号——客户端对 ErrUnauthorized 允许自动换新,对顶号码则转交互登录。
		return errcode.New(errcode.ErrUnauthorized, "session jti evidence required")
	}
	if cur != jti {
		// 顶号专属码(→ gRPC ABORTED,R4 P0 互踢循环):与自然过期/登出的 ErrUnauthorized
		// 可判别。被顶设备对本码只能转交互登录,不得用缓存凭据自动完整 Login——那会
		// 轮换会话 jti 反顶新设备,两台设备互踢死循环。
		plog.With(ctx).Warnw("msg", "session_superseded_rejected", "player_id", playerID)
		return errcode.New(errcode.ErrSessionSuperseded, "session superseded by a newer login")
	}
	return nil
}

// RequireCurrentSessionToken 供 service 层在携带原始 token 的 RPC(IssueDSTicket)上
// 做现行性门:验签 + 与 ctx 已鉴权 playerID 一致 + jti 为当前一代。
// 注:SelectRole 请求体无 token,由 RequireCurrentSessionJTI 从 Envoy 验签后的
// payload 头取 jti 走同一道门(2026-07-18,免 proto 字段)。
func (u *LoginUsecase) RequireCurrentSessionToken(ctx context.Context, playerID uint64, sessionToken string) error {
	if u.sessions == nil || u.verifier == nil {
		return nil
	}
	if sessionToken == "" {
		if u.requireHubAssignmentBinding {
			return errcode.New(errcode.ErrUnauthorized, "session token required")
		}
		return nil // dev 兼容:旧客户端未传 token 时不阻断。
	}
	claims, err := u.verifier.VerifySession(sessionToken)
	if err != nil || claims.PlayerID() == 0 || claims.PlayerID() != playerID {
		return errcode.New(errcode.ErrUnauthorized, "session token invalid for caller")
	}
	return u.requireCurrentSession(ctx, playerID, claims.ID)
}

// RequireCurrentSessionJTI 是请求体不带 token 的鉴权 RPC(SelectRole)的会话现行性门
// (封 battle-reconnect.md 已知边界 3,2026-07-18):jti 来自 Envoy jwt_authn 验签成功后
// 重写的 x-pandora-jwt-payload 头(入站无条件剥离,客户端无法伪造),与 IssueDSTicket 的
// 请求体 token 走同一 requireCurrentSession 判定。
//
// jti 为空 = 未经 Envoy 网关(直连内网端口联调 / dev 裸跑):B1 严格档 fail-closed 拒绝
// (生产 SelectRole 必经 :8443 jwt_authn,该头必然存在);local/off 保留历史放行。
func (u *LoginUsecase) RequireCurrentSessionJTI(ctx context.Context, playerID uint64, jti string) error {
	// sessions 是当前会话代际的唯一权威；未注入仅代表 dev 裸跑，不伪造现行性结论。
	if u.sessions == nil {
		return nil // dev 裸跑:未配 session 权威,与其余现行性门同语义直通。
	}
	// 严格档把缺失 jti 视为无法证明调用方仍持有当前会话，必须在任何选角副作用前拒绝。
	if jti == "" {
		if u.requireHubAssignmentBinding {
			return errcode.New(errcode.ErrUnauthorized, "session payload required")
		}
		return nil
	}
	// 非空 jti 复用统一现行性门，确保 SelectRole 与 IssueDSTicket 的顶号 fencing 语义一致。
	return u.requireCurrentSession(ctx, playerID, jti)
}
