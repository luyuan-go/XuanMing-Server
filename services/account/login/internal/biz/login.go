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
	"github.com/luyuancpp/pandora/pkg/safego"
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

	// PlayerNo 角色编号(展示专用,player-no-and-login-surge.md §3)。
	// 0 = 补号任务尚未分配(客户端显示「生成中」);读取 fail-soft,失败也是 0。
	PlayerNo uint64
	Resume   ResumeContextResult
}

type ResumeContextResult struct {
	Route      loginv1.ResumeRoute
	MatchID    uint64
	MatchStage loginv1.ResumeMatchStage
	GameMode   string
	// MapID 本局副本编号(透传 matchmaker 权威,0=未指定/默认;语义见 login.proto ResumeContext.map_id)。
	MapID uint32
	// ── owner 权威 placement(§9.23 query-first;R11 复审 架构 P0 起 owner 是路由第一权威)──
	// 开关 login.owner_query_first=true 时:owner 有归属记录即由它决定 Route 与 exact target,
	// locator/matchmaker 降为富化;owner 不可达返回 WAIT/UNKNOWN 而**不回落**旧路由。
	// 客户端用 (EntryState, exact target, OwnerEpoch) 三元组判定幂等 no-op(§9.23)。
	PlacementState  loginv1.ResumePlacementState
	OperationID     string
	DSPodName       string
	DSInstanceUID   string
	DSInstanceEpoch uint32
	HubAssignmentID string
	AllocationID    string
	ReleaseTrack    string
	// OwnerEpoch 每玩家单调归属代次(§9.22 权威本体);0 = 无 owner 记录 / 未启用 query-first。
	OwnerEpoch uint64
	// EntryState / WaitReason / RetryAfterMs 是 §9.23 最小状态集(见 login.proto 同名字段)。
	EntryState   loginv1.ResumeEntryState
	WaitReason   loginv1.ResumeWaitReason
	RetryAfterMs uint32
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
	// loginLimiter 登录失败 Quota(anti-abuse §6 第 4 项;弱依赖,nil=不限)。
	loginLimiter LoginRateLimiter
	// ownerPlacement:§9.23 query-first 的 owner 权威查询器。**唯一路由权威**,不再有开关,
	// 也不再有 locator-first 旧基线可回落;nil(owner_addr 未配)时进场按 WAIT 处理。
	ownerPlacement OwnerPlacementQuerier
	sf             *snowflake.Node
	hubDSAddr      string // 回退用静态 hub DS 地址(hub_allocator 未配 / 调用失败时)
	hubRegion      string // 传给 hub_allocator.AssignHub 的 region(空=allocator 选最空分片)
	signer         *auth.Signer
	verifier       *auth.Verifier
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

	// ownerResolveLog 只用于 owner_placement_resolved 的日志降噪(见 owner_query.go)。
	// 非权威、进程内、有界、重启即空;不参与任何判定。
	ownerResolveLog ownerResolveLogDedup
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
// sf 由 main.go 经 etcdnode.MustProvideSnowflake 装配(static / etcd 两态 + 失租 fencing
// 退出契约都收敛在那里,不要改回裸 snowflake.NewNode);hubDSAddr / hubRegion 从 conf 读;signer / legacy verifier /
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

// strictBattleGateProfile 是「战斗态查不到时能否放行进 Hub」的唯一档位判据。
//
// 它必须与 resolveHub 出票档位判据(requireHubAssignmentBinding || rs256DSTicketProfileEnabled,
// 见本文件 IssueDSTicket 前的两处校验)**逐字一致**:两者一旦分叉,就会出现
// 「弱档放行 + 强档出票」的组合——玩家在依赖抖动时被判定为"不在战斗",却拿到一张
// hub_allocator 签发、DS 会正常接受的正式绑定票,于是同时存在于 Battle 与 Hub 两台
// 可操作 DS(§9 不变量 1)。
//
// 两轴正交:requireHubAssignmentBinding 是归属绑定的滚动激活栅栏(默认 false),
// rs256DSTicketProfileEnabled 只看 login.ds_ticket 是否配了 verifier。因此仅按前者分档
// 会漏掉"RS256 已配、binding 未激活"这一激活窗口。
//
// 弱降级(返回 false)只允许存在于两轴都关的 legacy HS256 dev 裸跑档——那里 login 自签票,
// 本就没有生产级权威可言,保留历史行为不影响线上。
func (u *LoginUsecase) strictBattleGateProfile() bool {
	return u != nil && (u.requireHubAssignmentBinding || u.rs256DSTicketProfileEnabled())
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
// loginWaitRetryAfterMs 是 Login 侧暂时失败给客户端的建议退避。
// 与 owner_query.go 的 ownerUnknownRetryAfterMs 同源推导:依赖(locator / hub_allocator /
// 角色权威)的故障切换与重选举量级在秒内,1s 足够跨过瞬时抖动又不让玩家傻等;
// 客户端仍会叠加自己的退避 + jitter,这里只是下界提示。
const loginWaitRetryAfterMs uint32 = 1000

// playerNoReadTimeout 是登录主链上纯展示 PK 点查的独立短预算。
// 同服务单语句 MySQL 记账的健康 P99 只有毫秒级(touchDeviceTimeout 注释),
// 250ms 已给跨节点 TiDB 点查留出数十倍余量，同时只占 prod 5s 登录预算的 5%。
// 超时只取消子 ctx、降级为 0，不得取消父登录 ctx；真实 P99 待洪峰压测复核。
const playerNoReadTimeout = 250 * time.Millisecond

// waitLogin 把**会话已建立之后**的暂时性失败收敛成携带新 session 的 WAIT。
//
// §9.23:「路由、匹配、分配、签票、Travel、Admission 的暂时失败不得清空会话、要求重新
// 输入账号密码,或另起一条本地 fallback 路由」。原实现在这些步骤失败时直接返回 error,
// service 层只回一个业务码——**刚刚写入并已成为当前一代的 session 就此丢失**:客户端拿
// 不到 token,而服务端已把旧会话顶掉,玩家只能重新输账号密码,且重登又会再顶一次。
//
// 返回值刻意是 (result, nil):对客户端这不是"登录失败",而是"登录成功、进场待定",
// 由 coordinator 按 retry_after 重查同一入口继续推进。
func waitLogin(base *LoginResult, reason loginv1.ResumeWaitReason) *LoginResult {
	out := *base
	out.Resume = ResumeContextResult{
		Route:        loginv1.ResumeRoute_RESUME_ROUTE_UNKNOWN,
		EntryState:   loginv1.ResumeEntryState_RESUME_ENTRY_STATE_WAIT,
		WaitReason:   reason,
		RetryAfterMs: loginWaitRetryAfterMs,
	}
	return &out
}

func (u *LoginUsecase) Login(ctx context.Context, account, passwordHash, deviceID string) (*LoginResult, error) {
	h := plog.With(ctx)
	// loginStartedAt 只供 login_ok / login_wait_returned 的 dur_total_ms 用(§11.3「慢在哪」):
	// access log 只有一个总 latency,分不出慢在 bcrypt、AssignHub 还是 matchmaker 探测。
	loginStartedAt := time.Now()

	// 登录失败 Quota(anti-abuse §6 第 4 项):锁窗内直接拒,连 bcrypt 都不算——
	// 撞库/爆破在入口被吸收。fail-open:limiter 未注入 / Redis 故障放行。
	clientIP := clientIPFrom(ctx)
	if lerr := u.checkLoginFailLock(ctx, account, clientIP); lerr != nil {
		return nil, lerr
	}

	playerID, expected, err := u.repo.FindByAccount(ctx, account)
	if err != nil {
		// 账号不存在:开发期“假注册” / 免密任一开关打开 → 首登自动注册(不阻断登录)。
		if errcode.As(err) != errcode.ErrLoginAccountNotFound || !(u.devAutoRegister || u.devSkipPassword) {
			// 只有「明确不存在」记凭据失败(账号枚举/撞库面);DB 故障绝不计——
			// 否则故障风暴会把全服玩家锁死(§2 fail-open 方向)。
			if errcode.As(err) == errcode.ErrLoginAccountNotFound {
				u.recordLoginFailure(ctx, account, clientIP)
				// §11.3 R2:凭据面拒绝在线上必须可见。ErrLoginAccountNotFound 不是
				// IsServerFault,access log 落 rpc_ok=Debug,原本这条 Debug 等于没有——
				// 「玩家说登不进去」时连「这个账号名压根不存在」都证明不了。
				h.Warnw("msg", "login_account_not_found", "reason", "account_not_found",
					"account", account, "device_id", deviceID, "client_ip", clientIP)
			} else {
				// 同一个 if 此前把「账号明确不存在」与「账号库查询失败」打成同一条
				// login_account_not_found:DB 抖动导致全服登不进时,日志显示成一片
				// 「账号不存在」,排查方向被直接带偏。拆成独立 msg + reason。
				h.Errorw("msg", "login_account_lookup_failed", "err", err,
					"reason", "account_lookup_failed", "account", account, "device_id", deviceID)
			}
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
		u.recordLoginFailure(ctx, account, clientIP)
		// 同上:ErrLoginPasswordMismatch 在 access log 里落 rpc_ok(Debug),
		// 「密码错」与「账号不存在」必须在线上可判别(撞库面统计也只能靠这条)。
		h.Warnw("msg", "login_password_mismatch", "reason", "password_mismatch",
			"account", account, "player_id", playerID, "device_id", deviceID, "client_ip", clientIP)
		return nil, errcode.New(errcode.ErrLoginPasswordMismatch, "password mismatch")
	}

	// §11.3 R3:login 是未鉴权面,plog.With(ctx) 不会自动注入 player_id。身份定下来后
	// 写进 ctx,让本请求后续所有经 plog.With(ctx) 新建的日志(含 data 层与 detached
	// 记账)自动带上这个 join key。纯日志字段:pkg/grpcclient 不读它,不改任何下游调用。
	// h 是在 playerID 已知之前建的,故本函数内既有的显式 player_id kv 不会重复。
	ctx = plog.WithPlayerID(ctx, playerID)

	banned, err := u.repo.CheckBanned(ctx, playerID, deviceID)
	if err != nil {
		// 封禁闸门是 fail-closed 点:查询失败 = 登录直接失败。DB 抖动时全服登不进,
		// 而 access log 只有一个泛化 code,看不出是这道闸的查询挂了(§11.3 R2)。
		h.Errorw("msg", "ban_check_failed", "err", err,
			"account", account, "player_id", playerID, "device_id", deviceID)
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
	// (网络类错误)时本次登录失败,并由 reconcileFailedSessionWrite 对两处权威写入
	// generation-bounded 无能力墓碑；绝不恢复无法证明已交付的即时前代。定序失败
	// (ErrSessionSuperseded)不进入该路径:行已属于赢家,墓碑会破坏别人的登录。
	sessTTL := u.signer.SessionTTL()
	var sessGen uint64
	if u.sessionGen != nil {
		lease, gerr := u.sessionGen.PersistSessionJTI(ctx, playerID, sessJTI)
		if gerr != nil && data.IsCommitAmbiguous(gerr) {
			// R11 复审 P0-1 问题 A:COMMIT 结果不确定。不猜——用本次 jti 作唯一标记读回
			// 权威判定(§9.22「结果不确定必须 fail-closed,禁止冒充默认状态」的落法是把
			// 不确定态判定掉,而不是把它当失败)。
			lease, gerr = u.resolveAmbiguousSessionGeneration(ctx, playerID, sessJTI, sessTTL, lease)
		}
		if gerr != nil {
			h.Errorw("msg", "session_generation_persist_failed", "err", gerr, "player_id", playerID)
			return nil, errcode.NewCause(errcode.ErrUnavailable, gerr,
				"session generation persistence unavailable; login rejected")
		}
		sessGen = lease.Generation
	}
	if u.sessions != nil {
		if err := u.sessions.Set(ctx, playerID, sessionToken, sessJTI, deviceID, sessTTL, sessGen); err != nil {
			// ErrSessionSuperseded = 并发更新一代登录已完成写入,本次登录定序失败;
			// 其余为基础设施错误。两者都不得交付凭据。
			// reason 枚举化(§11.3 R2):「我被顶号了」按 device_id 分辨同账号多设备互踢,
			// 与 Redis 故障必须可判别——两者都是 Warn,不枚举就会互相淹没。
			sessionSetReason := "infra"
			if errcode.As(err) == errcode.ErrSessionSuperseded {
				sessionSetReason = "superseded"
			}
			h.Warnw("msg", "session_set_failed", "err", err, "player_id", playerID, "gen", sessGen,
				"reason", sessionSetReason, "account", account, "device_id", deviceID, "sess_jti", sessJTI)
			if u.sessionGen != nil && errcode.As(err) != errcode.ErrSessionSuperseded {
				u.reconcileFailedSessionWrite(ctx, playerID, sessJTI, sessGen, sessTTL)
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
	// 会话已是当前一代:此后的每一步暂时失败都必须带着它返回 WAIT,而不是丢弃(§9.23)。
	// 角色编号(展示专用):fail-soft——查失败只打日志置 0(客户端显示「生成中」),
	// 绝不因展示字段拒登录;存量库尚未收敛到 pandora_account 000006 时同样落此分支。
	playerNoCtx, playerNoCancel := context.WithTimeout(ctx, playerNoReadTimeout)
	playerNo, playerNoErr := u.repo.GetPlayerNo(playerNoCtx, playerID)
	playerNoCancel()
	if playerNoErr != nil {
		h.Warnw("msg", "player_no_read_failed", "err", playerNoErr, "player_id", playerID)
		playerNo = 0
	}

	base := &LoginResult{
		PlayerID:     playerID,
		SessionToken: sessionToken,
		SessionExpMs: sessExpMs,
		RegionID:     regionID,
		CellID:       cellID,
		PlayerNo:     playerNo,
	}
	// logLoginOutcome 是登录返回面的**唯一收口日志**(§11.3 R1/R3)。
	//
	// 为什么必须是 INFO:Login 是未鉴权面(server/grpc.go 用 AuthOptional),plog.With(ctx)
	// 拿不到 player_id,这一行是全链**唯一**把 account ↔ player_id ↔ device_id ↔ trace_id
	// 绑在一起的地方。它一 Debug,客服拿到账号名就无法映射到 player_id,
	// hub_allocator / locator / matchmaker / owner 那边按 player_id 索引的日志一条都串不起来。
	//
	// WAIT 是「登录成功但进不去场景」的降级,按 R2 走 Warn + 枚举 wait_reason:
	// WAIT 返回的是 code=OK,access log 落 rpc_ok(Debug),线上 info 级下
	// 「玩家卡在登录转圈」这个现象在后端原本完全不可见,连有多少人正在 WAIT 都统计不出来。
	logLoginOutcome := func(out *LoginResult) {
		r := out.Resume
		fields := []any{
			"account", account, "player_id", playerID, "device_id", deviceID,
			"entry_state", r.EntryState.String(), "route", r.Route.String(),
			"placement_state", r.PlacementState.String(),
			"wait_reason", r.WaitReason.String(), "retry_after_ms", r.RetryAfterMs,
			"selected_role_id", out.SelectedRoleID,
			"session_gen", sessGen, "sess_jti", sessJTI, "session_exp_ms", out.SessionExpMs,
			"owner_epoch", r.OwnerEpoch, "operation_id", r.OperationID,
			"hub_ds_addr", out.HubDSAddr, "hub_ticket_exp_ms", out.HubTicketExpMs,
			"hub_assignment_id", r.HubAssignmentID,
			"battle_ds_addr", out.BattleDSAddr, "match_id", r.MatchID,
			"match_stage", r.MatchStage.String(), "game_mode", r.GameMode, "map_id", r.MapID,
			"ds_pod", r.DSPodName, "ds_instance_uid", r.DSInstanceUID,
			"ds_instance_epoch", r.DSInstanceEpoch, "release_track", r.ReleaseTrack,
			"region_id", out.RegionID, "cell_id", out.CellID, "player_no", out.PlayerNo,
			"dur_total_ms", time.Since(loginStartedAt).Milliseconds(),
		}
		if r.EntryState == loginv1.ResumeEntryState_RESUME_ENTRY_STATE_WAIT {
			h.Warnw(append([]any{"msg", "login_wait_returned"}, fields...)...)
			return
		}
		h.Infow(append([]any{"msg", "login_ok"}, fields...)...)
	}

	// 交付前置终检:凭 base 交付 session 同样要过 fenceLoginDelivery(见下方 R5 复审 P0-5
	// 注释)。抽成闭包,WAIT 与正常返回共用同一道门,避免 WAIT 路径成为绕过终检的后门。
	deliver := func(out *LoginResult) (*LoginResult, error) {
		if ferr := u.fenceLoginDelivery(ctx, playerID, sessJTI); ferr != nil {
			return nil, ferr
		}
		logLoginOutcome(out)
		return out, nil
	}

	var hubFenceMatchID uint64
	if u.notifier == nil {
		if u.strictBattleGateProfile() {
			// 部署配置缺失(不是瞬时故障):本部署内**每一次**登录都会直接 WAIT,
			// 表现为「全服没人能登进去」。没有这条日志,排查会从业务链一路往下找。
			h.Errorw("msg", "login_locator_not_configured",
				"account", account, "player_id", playerID,
				"hint", "strict 档必须配 player_locator 地址;否则登录恒 WAIT/OWNER_UNKNOWN")
			return deliver(waitLogin(base, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN))
		}
	} else {
		res, terminalFence, reconnectErr := u.tryBattleReconnect(ctx, playerID, deviceID, sessionToken, sessExpMs, regionID, cellID, sessJTI)
		if reconnectErr != nil {
			// 重连权威不可判定 → WAIT + 保留会话(绝不默认 Hub:locator key miss 不能证明
			// 玩家已离开旧 Battle DS,§9.22)。
			// 这里把 err 落盘:下游把至少五种原因(locator 查询失败 / 三态门 UNKNOWN /
			// 签票失败 / game_mode 不可解 / battle 地址空)统一翻译成 WAIT/OWNER_UNKNOWN,
			// 只看 wait_reason 会误导排查方向去查 owner 服务。
			h.Warnw("msg", "login_battle_reconnect_unresolved", "err", reconnectErr,
				"account", account, "player_id", playerID)
			return deliver(waitLogin(base, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN))
		}
		if res != nil {
			// R5 复审 P0-5:battle 重连路径同样先做交付终检(见 fenceLoginDelivery 注释),
			// 并发新登录轮换 jti 后,旧流程不得把 battle 直连凭据交给旧设备。
			if ferr := u.fenceLoginDelivery(ctx, playerID, sessJTI); ferr != nil {
				return nil, ferr
			}
			res.PlayerNo = playerNo // 重连路径同样带出角色编号(展示字段,与路由无关)
			// 重连成功也是一次登录返回,走同一条收口日志(它不经 deliver)。
			logLoginOutcome(res)
			return res, nil
		}
		hubFenceMatchID = terminalFence
	}

	// 角色权威门(§9.23 最小状态集):必须区分三种结果,不能都折叠成 role=0。
	//   - 查询失败 → WAIT/ROLE_UNKNOWN,保留会话退避重查(不得冒充"未选角");
	//   - role=0(权威明确"没选过") → ROLE_REQUIRED,并且**到此为止**:
	//     §9.23 明文「未选角时不得提前分配 Hub、占座或签进场票」。客户端本来就是登录后
	//     先进选角关卡、由 SelectRole 拿含 role_id 的 hub 票进大厅,原先在这里先分配一次
	//     Hub 属于白占座位 + 白签一张没有 role 的票。
	//   - role>0 → 继续分配 Hub。
	selectedRoleID, roleErr := u.loadSelectedRole(ctx, playerID)
	if roleErr != nil {
		return deliver(waitLogin(base, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_ROLE_UNKNOWN))
	}
	// roleRepo == nil = 没部署角色权威(dev 裸跑),"选没选角"这个问题不成立,不设门。
	// 只有角色权威在线且明确回答 0 时才是 ROLE_REQUIRED。
	if u.roleRepo != nil && selectedRoleID == 0 {
		roleRequired := *base
		roleRequired.Resume = ResumeContextResult{
			Route:      loginv1.ResumeRoute_RESUME_ROUTE_HUB,
			EntryState: loginv1.ResumeEntryState_RESUME_ENTRY_STATE_ROLE_REQUIRED,
		}
		return deliver(&roleRequired)
	}

	// B1 先建立 LOGIN_PENDING 权威位置，再调用 Hub allocator。写入失败时既不分配
	// Hub，也不会产生/交付 Hub 票；local/off 保留历史上的分配后 best-effort 通知顺序。
	pendingNotified := false
	if u.requireHubAssignmentBinding {
		if u.notifier == nil {
			// 部署配置缺失(不是瞬时故障):本部署内每一次登录都会在这里硬失败。
			h.Errorw("msg", "login_locator_not_configured", "reason", "b1_hub_assign_requires_locator",
				"account", account, "player_id", playerID,
				"hint", "require_hub_assignment_binding 已开;必须配 player_locator 地址")
			return nil, errcode.New(errcode.ErrUnavailable,
				"player locator is required before B1 hub assignment")
		}
		if err := u.notifier.NotifyLoginPending(ctx, playerID, deviceID); err != nil {
			h.Warnw("msg", "locator_notify_failed", "err", err, "player_id", playerID,
				"reason", "login_pending_write_failed", "device_id", deviceID,
				"hint", "LOGIN_PENDING 写失败:不分配 Hub,带会话返回 WAIT 由客户端重查")
			return deliver(waitLogin(base, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN))
		}
		pendingNotified = true
	}

	// 解析 hub 分片 + hub 票据(W4 ⑥):
	// hub_allocator 是 hub 票据权威,优先调 AssignHub 拿真实地址 + 票据;
	// 未配 / 调用失败 → 回退自签票据(盖 region/cell 戳) + 静态 hubDSAddr(弱依赖,不阻断登录)。
	// 注意:contract 阶段起,AssignHub 内的 owner Begin 是强依赖——归属写不进权威时
	// hub_allocator 直接拒绝签票,错误在此收敛成 WAIT/NO_CAPACITY,客户端退避重查,
	// owner 恢复即自动继续(§9.22 fail-closed + 底线第 1 条"短暂不可用后自动恢复")。
	hubDSAddr, hubTicket, hubExpMs, err := u.resolveHub(ctx, playerID, regionID, cellID, selectedRoleID, hubFenceMatchID, sessJTI)
	if err != nil {
		h.Errorw("msg", "resolve_hub_failed", "err", err, "player_id", playerID,
			"reason", "hub_resolve_failed", "account", account, "role_id", selectedRoleID,
			"source_match_id", hubFenceMatchID,
			"hint", "带会话返回 WAIT,不清空会话不要求重新登录(§9.23)")
		return deliver(waitLogin(base, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_NO_CAPACITY))
	}

	// 记录最近登录设备:纯记账副作用(失败本就只日志),移出登录关键路径
	// (压测审核【必修-1】),不给 prod 5s 登录预算叠加一次 MySQL upsert 往返。
	u.touchDeviceAsync(ctx, playerID, deviceID)

	// local/off 在 Hub 解析后 best-effort 通知；B1 已在分配前成功写入，不能重复写。
	if !pendingNotified && u.notifier != nil {
		if err := u.notifier.NotifyLoginPending(ctx, playerID, deviceID); err != nil {
			h.Warnw("msg", "locator_notify_failed", "err", err, "player_id", playerID,
				"reason", "login_pending_write_failed_weak", "device_id", deviceID,
				"hint", "local/off 弱依赖:只影响 presence 投影,不阻断登录")
		}
	}

	// R5 复审 P0-5:副作用交付终检——本流程写入的 sessJTI 必须仍是当前一代才允许把
	// session token / hub 票据交给调用方。sessions.Set 之后的分配、locator、签票各步
	// 都不复核现行性,并发新登录 B 在其间再次轮换 jti 时,旧流程 A 若继续交付,旧设备
	// 将取得"看似有效"的完整登录态。复核失败 → 不返回任何凭据(票据已签但从未离开
	// 服务端 = 未取得);已写的 LOGIN_PENDING 等 locator 投影由 B 自己的写覆盖
	// (locator 是 presence 投影,非权威,§9.22)。
	// 确定性 region/cell 路由已在上方一次算好(regionID/cellID),这里直接复用。
	// login_ok 已下沉到 deliver 的 logLoginOutcome:必须等 Resume 定下来(entry_state /
	// route / wait_reason / exact target)才打,否则日志里最关键的进场判定全是空的。

	// Resume 必须是**真正的 §9.23 TARGET**,不能是硬编码的 {Route: HUB}。
	// AssignHub 已经在权威里写下了本次归属(contract 阶段强 Begin),此处回查一次即可拿到
	// exact 实例身份 + owner_epoch + operation_id 三元组——客户端据此判定"当前连接是否
	// 已精确匹配 owner",匹配就幂等 no-op,不再重复 Travel / 占座。
	// 回查失败不推翻已完成的分配:降级为 WAIT,客户端重查同一入口即可拿到 TARGET。
	out := *base
	out.HubDSAddr = hubDSAddr
	out.HubTicket = hubTicket
	out.HubTicketExpMs = hubExpMs
	out.SelectedRoleID = selectedRoleID
	decided, owned := u.resolveResumeFromOwner(ctx, playerID)
	switch {
	case !decided:
		// owner 明确"无归属"却刚分配完 Hub:归属记录与分配结果不自洽,不冒充 TARGET。
		h.Warnw("msg", "login_owner_missing_after_assign", "player_id", playerID,
			"reason", "owner_record_missing_after_assign", "account", account,
			"hub_ds_addr", hubDSAddr,
			"hint", "不冒充 TARGET,返回 WAIT 让客户端重查权威")
		out.Resume = waitLogin(base, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN).Resume
	default:
		out.Resume = u.enrichResumeFromMatchAuthority(ctx, playerID, owned)
	}
	return deliver(&out)
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

// hubResumeFromMatchAuthority 随 resolveResumeRoute 旧基线一并删除(2026-07-29)。
// 它产出的是「Route=HUB + 撮合富化字段、entry_state/owner_epoch 全空」的**旧协议形态**,
// 客户端会判定为 legacy 兼容响应。归属现由 owner 权威给出 TARGET,撮合字段由
// enrichResumeFromMatchAuthority 叠加——留着这个没有调用方的函数只会让下一个人
// 顺手用回去,把刚收口的五态又打出一个洞。

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
// battleResumeGameModeReason 把「game_mode 解不出来」这一个 if 收敛的多种原因拆成
// 枚举 reason(§11.3 R2)。纯函数,只读入参,不参与任何判定。
func battleResumeGameModeReason(bl data.BattleLocation, ma *data.PlayerMatchAuthority) string {
	switch {
	case ma == nil:
		return "match_authority_missing"
	case ma.State != matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE:
		return "match_state_not_active"
	case ma.MatchID != bl.MatchID:
		return "claim_match_id_drift"
	default:
		return "game_mode_empty"
	}
}

// matchAuthorityStateName / matchAuthorityStageName / matchAuthorityMatchID 是 nil-safe
// 的日志取值器:matchmaker 权威可能没查(presence 快路径)或查失败,日志必须显式区分
// 「没查」与「查到 NONE」——前者正常、后者可能是丢局事故。
func matchAuthorityStateName(ma *data.PlayerMatchAuthority) string {
	if ma == nil {
		return "NOT_QUERIED"
	}
	return ma.State.String()
}

func matchAuthorityStageName(ma *data.PlayerMatchAuthority) string {
	if ma == nil {
		return "NOT_QUERIED"
	}
	return ma.Stage.String()
}

func matchAuthorityMatchID(ma *data.PlayerMatchAuthority) uint64 {
	if ma == nil {
		return 0
	}
	return ma.MatchID
}

func (u *LoginUsecase) buildBattleResume(
	ctx context.Context, playerID uint64, bl data.BattleLocation, ma *data.PlayerMatchAuthority,
) (ResumeContextResult, error) {
	h := plog.With(ctx)
	if ma == nil && u.matchResolver != nil {
		fetched, merr := u.matchResolver.ResolvePlayerMatchContext(ctx, playerID)
		if merr != nil {
			if u.requireHubAssignmentBinding {
				// prod strict 档的静默返回:错误沿 tryBattleReconnect → Login 一路被吞成
				// waitLogin(OWNER_UNKNOWN),code=OK 落 access log 的 rpc_ok(Debug)。
				// 玩家表现为「断线重连按 retry_after 无限重查、永远进不去」,而后端零日志。
				h.Errorw("msg", "battle_resume_game_mode_unavailable", "err", merr,
					"player_id", playerID, "match_id", bl.MatchID,
					"reason", "match_query_failed",
					"hint", "strict 档 fail-closed;WAIT 原因被硬编码成 OWNER_UNKNOWN,真根因在 matchmaker 查询")
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
			// 同上:strict 档静默 Unavailable。reason 把这一个 if 收敛的多种原因拆开
			// (§11.3 R2)——「claim 漂移」是数据不一致事故,「记录缺 game_mode」是配置/写入
			// 缺陷,两者处理方式完全不同,而返回给客户端的 WAIT 长得一模一样。
			h.Errorw("msg", "battle_resume_game_mode_unavailable",
				"player_id", playerID, "match_id", bl.MatchID,
				"reason", battleResumeGameModeReason(bl, ma),
				"match_state", matchAuthorityStateName(ma), "match_stage", matchAuthorityStageName(ma),
				"claim_match_id", matchAuthorityMatchID(ma),
				"hint", "缺 game_mode 的 BATTLE resume 会让客户端 DS 恢复协调器拒绝路由头,交付它就是交付 bug")
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
	out := ResumeContextResult{
		Route:      loginv1.ResumeRoute_RESUME_ROUTE_BATTLE,
		MatchID:    bl.MatchID,
		MatchStage: stage,
		GameMode:   gameMode,
		MapID:      mapID,
	}
	// §9.23 五态必须在**所有**路径上有值。断线重连这条路原先只填 route + 撮合富化字段,
	// entry_state / owner_epoch / exact target 全空——那正是客户端 legacy 兼容分支赖以存在的
	// 最后一条路径(四个 additive 字段全零 = 被判定为旧协议响应)。
	//
	// 归属仍以 owner 为唯一权威:locator presence 只证明"看起来还在 battle",不能授权进入
	// (§9.22)。owner 说 BATTLE 才给 TARGET;owner 不可判定或指向别处 → WAIT 退避重查,
	// 绝不凭 presence 直接把玩家送回旧 DS。
	decided, owned := u.resolveResumeFromOwner(ctx, playerID)
	if !decided || owned.EntryState == loginv1.ResumeEntryState_RESUME_ENTRY_STATE_WAIT ||
		owned.Route != loginv1.ResumeRoute_RESUME_ROUTE_BATTLE {
		h.Warnw("msg", "battle_resume_owner_not_battle", "player_id", playerID,
			"match_id", bl.MatchID, "owner_decided", decided, "owner_route", owned.Route,
			"hint", "presence 说在 battle 但 owner 未确认;返回 WAIT 让客户端重查,不凭投影放行")
		return waitResume(loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN,
			ownerUnknownRetryAfterMs), nil
	}
	// owner 定归属身份(entry_state / placement_state / exact target / owner_epoch /
	// operation_id),locator+matchmaker 只补展示字段。
	owned.MatchID, owned.MatchStage, owned.GameMode, owned.MapID = out.MatchID, out.MatchStage, out.GameMode, out.MapID
	return owned, nil
}

// ResolveBattleEndpoint 为 authenticated IssueDSTicket(battle) 与完整 Login
// 复用同一条票据签发链：统一经 roster 权威门(本人+成员+match live)现签。
func (u *LoginUsecase) ResolveBattleEndpoint(
	ctx context.Context,
	playerID, matchID uint64,
	sessJTI string,
) (addr, ticket string, expMs int64, err error) {
	if playerID == 0 || matchID == 0 {
		plog.With(ctx).Warnw("msg", "battle_endpoint_rejected", "reason", "missing_player_or_match",
			"player_id", playerID, "match_id", matchID)
		return "", "", 0, errcode.New(errcode.ErrInvalidArg,
			"Battle endpoint requires player_id and match_id")
	}
	if u.battleTicketIssuer == nil {
		plog.With(ctx).Errorw("msg", "battle_endpoint_rejected", "reason", "ticket_issuer_not_configured",
			"player_id", playerID, "match_id", matchID)
		return "", "", 0, errcode.New(errcode.ErrUnavailable,
			"battle reconnect ticket authority unavailable")
	}
	regionID, cellID := u.routeRegionCell(ctx, playerID)
	result, issueErr := u.battleTicketIssuer.IssueBattleDSTicketAtCell(
		ctx, playerID, matchID, regionID, cellID, sessJTI)
	if issueErr != nil || result == nil || result.BattleDSAddr == "" || result.Ticket == "" {
		// 四种性质截然不同的原因塌成一个 ErrUnavailable(issueErr 为 nil 时连 cause 都是空的),
		// 而 access log 只剩一个泛化 rpc_inband_error:分不出是 roster 不认这个人(数据问题,
		// 去查 matchmaker)还是 Redis 抖动(重试即可)。R2 拆成枚举 reason。
		reason := "empty_ticket"
		switch {
		case issueErr != nil:
			reason = "issue_error"
		case result == nil:
			reason = "nil_result"
		case result.BattleDSAddr == "":
			reason = "empty_addr"
		}
		plog.With(ctx).Warnw("msg", "battle_endpoint_unavailable", "err", issueErr,
			"player_id", playerID, "match_id", matchID, "reason", reason,
			"region_id", regionID, "cell_id", cellID)
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
		// 一个 if 收敛了两条性质不同的路径(§11.3 R2 同理适用于 decision):
		// presence 命中 = 回 Battle;matchResolver 未配 = presence-only 降级后回 Hub。
		decision := battleAuthorityPresenceOnly
		if bl.InBattle {
			decision = battleAuthorityPresenceInBattle
		}
		u.logBattleAuthorityResolved(ctx, playerID, decision, bl, nil)
		return bl, nil, nil
	}
	ma, merr := u.matchResolver.ResolvePlayerMatchContext(ctx, playerID)
	if merr != nil {
		if u.strictBattleGateProfile() {
			return data.BattleLocation{}, nil, errcode.NewCause(errcode.ErrUnavailable, merr,
				"cannot consult durable match authority; retry")
		}
		h.Warnw("msg", "match_authority_query_degraded", "err", merr, "player_id", playerID)
		u.logBattleAuthorityResolved(ctx, playerID, battleAuthorityQueryDegraded, bl, nil)
		return bl, nil, nil // local/off 弱降级:保留 presence 判定。
	}
	switch ma.State {
	case matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE:
		if ma.Stage == matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_READY && ma.MatchID != 0 {
			// READY 局但 locator 投影缺失(TTL 蒸发 / notifyBattle 窗口):以耐久权威为准。
			h.Infow("msg", "battle_authority_recovered_from_match_claim",
				"player_id", playerID, "match_id", ma.MatchID)
			recovered := data.BattleLocation{
				InBattle:      true,
				MatchID:       ma.MatchID,
				BattleAddr:    ma.BattleDSAddr,
				PresenceState: bl.PresenceState,
			}
			u.logBattleAuthorityResolved(ctx, playerID, battleAuthorityRecoveredFromClaim, recovered, &ma)
			return recovered, &ma, nil
		}
		// 排队/确认/分配中:玩家应在 Hub 等 READY 推送,不改路由。
		u.logBattleAuthorityResolved(ctx, playerID, battleAuthorityActiveStageNotReady, bl, &ma)
		return bl, &ma, nil
	case matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_NONE:
		u.logBattleAuthorityResolved(ctx, playerID, battleAuthorityMatchNone, bl, &ma)
		return bl, &ma, nil
	default:
		// UNKNOWN(索引漂移/坏记录):B1 不猜,可重试;local/off 弱降级。
		if u.strictBattleGateProfile() {
			return data.BattleLocation{}, nil, errcode.New(errcode.ErrUnavailable,
				"durable match authority state unknown; retry")
		}
		h.Warnw("msg", "match_authority_state_unknown_degraded", "player_id", playerID)
		u.logBattleAuthorityResolved(ctx, playerID, battleAuthorityStateUnknownDegraded, bl, &ma)
		return bl, nil, nil
	}
}

// battle_authority_resolved 的 decision 枚举(§11.3 R2:reason 用 snake_case 常量,
// 不是自由文本)。resolveBattleAuthority 是「回 Battle 还是回 Hub」的分水岭,
// 这些取值是「登录后被丢回大厅、对局没了」唯一的对账依据。
const (
	// presence 命中 BATTLE 租约 → 走重连链(后续仍过 InspectBattleRoute 三态门验真)。
	battleAuthorityPresenceInBattle = "presence_in_battle"
	// matchResolver 未配(dev/local):退化成纯 presence 判定,presence 未命中即回 Hub。
	battleAuthorityPresenceOnly = "presence_only_no_match_resolver"
	// presence 未命中但 matchmaker 说 READY:以耐久权威为准,恢复回原局。
	battleAuthorityRecoveredFromClaim = "recovered_from_ready_claim"
	// 有活跃 claim 但阶段是排队/确认/分配中:玩家物理上本就该在 Hub 等 READY 推送。
	battleAuthorityActiveStageNotReady = "match_active_stage_not_ready"
	// matchmaker 明确「无活跃对局」:对局真的结束了,回 Hub 是正常结果。
	battleAuthorityMatchNone = "match_none"
	// 查询失败后的 local/off 弱降级:仍按 presence 判定(strict 档在此之前已 fail-closed)。
	battleAuthorityQueryDegraded = "match_query_degraded"
	// 权威状态 UNKNOWN(索引漂移/坏记录)后的 local/off 弱降级。
	battleAuthorityStateUnknownDegraded = "match_state_unknown_degraded"
)

// logBattleAuthorityResolved 落盘路由分水岭的判定结果与全部依据(§11.3 R1:
// 路由/权威判定结果打 INFO,每玩家每链路每阶段至多一条)。
//
// 为什么必须带这些字段:玩家反馈「登录后被丢回大厅、对局没了」时,要能证明当时
// locator 返回的是 UNSPECIFIED(key miss)还是 MATCHING,以及 matchmaker claim 是
// NONE(对局真结束了)还是 ACTIVE/ALLOCATING(对局还在,只是 READY 还没推)——
// 前者正常、后者是丢局事故,处理方式完全相反,而日志里原本长得一模一样。
func (u *LoginUsecase) logBattleAuthorityResolved(
	ctx context.Context, playerID uint64, decision string,
	bl data.BattleLocation, ma *data.PlayerMatchAuthority,
) {
	plog.With(ctx).Infow("msg", "battle_authority_resolved",
		"player_id", playerID,
		"decision", decision,
		"in_battle", bl.InBattle,
		"presence_state", bl.PresenceState.String(),
		"locator_match_id", bl.MatchID,
		"match_state", matchAuthorityStateName(ma),
		"match_stage", matchAuthorityStageName(ma),
		"claim_match_id", matchAuthorityMatchID(ma),
		"strict_profile", u.strictBattleGateProfile())
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
		// reason 拆开:strict 档这是「无法证明玩家不在战斗」→ 整条登录 WAIT;
		// local/off 是弱降级继续走 Hub。两者返回给玩家的表现完全不同。
		reason := "battle_authority_degraded_continue_hub"
		if u.strictBattleGateProfile() {
			reason = "battle_authority_unavailable_fail_closed"
		}
		h.Warnw("msg", "battle_location_query_failed", "err", err, "player_id", playerID,
			"reason", reason, "strict_profile", u.strictBattleGateProfile())
		if u.strictBattleGateProfile() {
			return nil, 0, errcode.NewCause(errcode.ErrUnavailable, err,
				"cannot prove player is outside battle before B1 hub assignment")
		}
		// local/off 保留历史弱依赖降级。
		return nil, 0, nil
	}
	if !bl.InBattle {
		return nil, 0, nil
	}

	// §11.3 R3:请求内解析出 match_id 后写进 ctx,让本请求后续每一条日志(含
	// buildBattleResume / 签票链 / data 层)自动带上这个 join key,不必逐处手写。
	ctx = plog.WithMatchID(ctx, bl.MatchID)
	h = plog.With(ctx)

	// locator 租约说在战斗:用 match 权威三态门区分“仍在活局”与“终局后 TTL 残留”。
	if u.battleTicketIssuer == nil {
		h.Errorw("msg", "battle_reconnect_ticket_issuer_unavailable",
			"reason", "ticket_issuer_not_configured",
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
			"player_id", playerID, "match_id", bl.MatchID,
			"decision", "route_hub_with_source_match_fence")
		return nil, bl.MatchID, nil
	case data.BattleRouteActive:
		// 继续下方签票回原局。
	default:
		// UNKNOWN(match 权威抖动/roster 不可读):不猜。可重试,最长 30s 租约到期自愈。
		h.Warnw("msg", "battle_reconnect_route_unknown_retryable",
			"reason", "battle_route_unknown",
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
			"reason", "battle_ticket_issue_failed",
			"player_id", playerID, "match_id", bl.MatchID, "sess_jti", sessJTI)
		return nil, 0, errcode.NewCause(errcode.ErrUnavailable, terr,
			"battle reconnect ticket authority unavailable")
	}
	battleTicket, battleExpMs := battleResult.Ticket, battleResult.ExpiresAtMs
	if battleResult.BattleDSAddr == "" {
		// 票已签出(副作用已发生:jti 已生成、roster 门已过)却因地址为空整条退回 WAIT。
		// 这是「roster 权威给出了合法目标但没有地址」的数据不一致信号(allocator 写
		// projection 漏 addr),成规模发生时原本无法被发现。
		h.Errorw("msg", "battle_reconnect_target_addr_missing",
			"reason", "roster_target_addr_empty",
			"player_id", playerID, "match_id", bl.MatchID, "ticket_jti", battleResult.JTI)
		return nil, 0, errcode.New(errcode.ErrUnavailable, "battle reconnect target address unavailable")
	}

	// 记录最近登录设备:同主登录路径,移出关键路径(压测审核【必修-1】)。
	u.touchDeviceAsync(ctx, playerID, deviceID)

	// §11.3 R1:这是「玩家被路由回原对局」的不可逆判定 + 一次 Battle 票据签发,
	// 必须 INFO。原为 Debug——线上默认 info 级下,「重连到底有没有发生」一条都看不到,
	// 而 Login 收口日志(login_ok)在重连路径上不带 battle 票 jti。
	h.Infow("msg", "login_battle_reconnect", "player_id", playerID, "device_id", deviceID,
		"match_id", bl.MatchID, "battle_ds_addr", battleResult.BattleDSAddr,
		"ticket_jti", battleResult.JTI, "sess_jti", sessJTI,
		"battle_ticket_exp_ms", battleExpMs, "region_id", regionID, "cell_id", cellID,
		"match_stage", resume.MatchStage.String(), "game_mode", resume.GameMode,
		"entry_state", resume.EntryState.String(), "owner_epoch", resume.OwnerEpoch)

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
		// 部署缺配置(不是玩家问题):本入口对全服恒不可用。
		plog.With(ctx).Errorw("msg", "resume_context_session_invalid", "reason", "verifier_not_configured")
		return ResumeContextResult{}, errcode.New(errcode.ErrUnavailable, "session verifier unavailable")
	}
	claims, err := u.verifier.VerifySession(sessionToken)
	if err != nil || claims.PlayerID() == 0 {
		// 「重进游戏一直转圈」的第一嫌疑就是这里;返回 ERR_UNAUTHORIZED 而 IsServerFault
		// 为 false,access log 落 rpc_ok(Debug),线上原本什么也看不到。
		// 两种原因必须可判别:验签失败(token 过期/被换密钥) vs claims 里没有 player_id。
		reason := "verify_failed"
		if err == nil {
			reason = "no_player"
		}
		plog.With(ctx).Warnw("msg", "resume_context_session_invalid", "reason", reason, "err", err)
		return ResumeContextResult{}, errcode.New(errcode.ErrUnauthorized, "invalid session")
	}
	playerID := claims.PlayerID()
	// §11.3 R3:恢复入口同样是"session token 面",ctx 里没有 player_id;解析出来后写进
	// ctx,让 owner/角色/撮合各步的日志自动带上这个 join key。纯日志字段,不影响调用。
	ctx = plog.WithPlayerID(ctx, playerID)
	if cerr := u.requireCurrentSession(ctx, playerID, claims.ID); cerr != nil {
		return ResumeContextResult{}, cerr
	}
	// §9.23 query-first:**owner 先问**,它是归属的唯一权威。开关已删除,这是唯一路径。
	//
	// 为什么不再保留 locator-first 旧基线:那条路的判据是 locator presence,而 §9.22 明文
	// 「key miss 只能说明 presence 不可见,不能证明玩家已离开旧 DS,也不能授权进入另一台
	// DS」——保留它就是保留一条会 fail-open 的第二权威,且它永远填不出 §9.23 的五态
	// (客户端因此永远走 legacy 兼容分支)。§15.3 同样反对留一个只会置 true 的开关。
	//
	//   · 有归属记录 → owner 定 Route 与 exact target;matchmaker 只补富化字段。
	//   · 查询不可判定 → WAIT/UNKNOWN,退避重查,绝不猜路由。
	//   · 明确"无归属" → 首次进场链(角色门 → 分配首个 Hub),仍由本入口收敛。
	if decided, owned := u.resolveResumeFromOwner(ctx, playerID); decided {
		if owned.EntryState == loginv1.ResumeEntryState_RESUME_ENTRY_STATE_WAIT {
			logResumeContextOutcome(ctx, playerID, "owner_wait", owned)
			return owned, nil // 权威不可达 / 屏障未开:客户端按 retry_after 重查
		}
		out := u.enrichResumeFromMatchAuthority(ctx, playerID, owned)
		logResumeContextOutcome(ctx, playerID, "owner_target", out)
		return out, nil
	}
	out, ferr := u.resolveFirstEntry(ctx, playerID, claims.ID)
	if ferr == nil {
		logResumeContextOutcome(ctx, playerID, "first_entry", out)
	}
	return out, ferr
}

// logResumeContextOutcome 落盘恢复入口的最终五态(§11.3 R4)。
//
// 级别是 Debug 而不是 Info:GetResumeContext 是客户端 WAIT 期间按 retry_after 反复重查
// 的入口,每玩家几秒一次,打 Info 会把同文件里的 WARN 拒绝冲走。真正的**状态迁移**
// 由 owner_placement_resolved 的去重逻辑保证仍是 Info(三元组变了才打),这条只是
// 「本次返回了什么」的完整快照,排障时对单 pod 开 LOG_LEVEL=debug 即可。
func logResumeContextOutcome(ctx context.Context, playerID uint64, source string, out ResumeContextResult) {
	plog.With(ctx).Debugw("msg", "resume_context_returned",
		"player_id", playerID, "source", source,
		"entry_state", out.EntryState.String(), "route", out.Route.String(),
		"placement_state", out.PlacementState.String(),
		"wait_reason", out.WaitReason.String(), "retry_after_ms", out.RetryAfterMs,
		"owner_epoch", out.OwnerEpoch, "operation_id", out.OperationID,
		"ds_pod", out.DSPodName, "ds_instance_uid", out.DSInstanceUID,
		"ds_instance_epoch", out.DSInstanceEpoch, "release_track", out.ReleaseTrack,
		"hub_assignment_id", out.HubAssignmentID, "allocation_id", out.AllocationID,
		"match_id", out.MatchID, "match_stage", out.MatchStage.String(),
		"game_mode", out.GameMode, "map_id", out.MapID)
}

// resolveFirstEntry 处理 owner 明确回答"无归属"的情况:这是首次进场(或登出释放后重进),
// 不是故障。§9.23 要求统一入口在此也给出明确五态,而不是像旧基线那样返回一个
// 什么都没填的裸 HUB(那正是客户端 legacy 兼容分支赖以存在的根源)。
//
//	角色查询失败 → WAIT/ROLE_UNKNOWN(不得冒充未选角);
//	role=0       → ROLE_REQUIRED(且不分配 Hub、不占座、不签票);
//	role>0       → 分配首个 Hub,再回查权威给出 TARGET。
func (u *LoginUsecase) resolveFirstEntry(ctx context.Context, playerID uint64, sessJTI string) (ResumeContextResult, error) {
	roleID, roleErr := u.loadSelectedRole(ctx, playerID)
	if roleErr != nil {
		return waitResume(loginv1.ResumeWaitReason_RESUME_WAIT_REASON_ROLE_UNKNOWN,
			ownerUnknownRetryAfterMs), nil
	}
	// 同 Login 的角色门:没部署角色权威(dev 裸跑)时不设门,直接分配 Hub。
	if u.roleRepo != nil && roleID == 0 {
		// Debug 而非 Info:恢复入口会被客户端反复重查,未选角期间每次都会走到这里。
		// 首次登录的 ROLE_REQUIRED 已由 login_ok(Info)记录。
		plog.With(ctx).Debugw("msg", "first_entry_role_required", "player_id", playerID,
			"reason", "role_not_selected")
		return ResumeContextResult{
			Route:      loginv1.ResumeRoute_RESUME_ROUTE_HUB,
			EntryState: loginv1.ResumeEntryState_RESUME_ENTRY_STATE_ROLE_REQUIRED,
		}, nil
	}
	// 已选角但无归属:补分配首个 Hub。复用既有 ResolveHubEndpoint(内部 AssignHub →
	// 强 Begin 写权威),不新起第二条分配路径(§9.23 单一入口 / §15.2 复用)。
	if _, _, _, err := u.ResolveHubEndpoint(ctx, playerID, sessJTI); err != nil {
		plog.With(ctx).Warnw("msg", "first_entry_hub_assign_failed", "player_id", playerID, "err", err,
			"reason", "hub_assign_failed", "role_id", roleID,
			"hint", "带 retry_after 的 WAIT,客户端重查本入口继续推进")
		return waitResume(loginv1.ResumeWaitReason_RESUME_WAIT_REASON_NO_CAPACITY,
			ownerUnknownRetryAfterMs), nil
	}
	// 分配成功 → 权威里已有归属,回查给出 exact TARGET。
	if decided, owned := u.resolveResumeFromOwner(ctx, playerID); decided &&
		owned.EntryState != loginv1.ResumeEntryState_RESUME_ENTRY_STATE_WAIT {
		return u.enrichResumeFromMatchAuthority(ctx, playerID, owned), nil
	}
	// 刚分配完却查不到归属:不自洽,不冒充 TARGET,让客户端重查(§9.22 fail-closed)。
	plog.With(ctx).Warnw("msg", "first_entry_owner_missing_after_assign", "player_id", playerID,
		"reason", "owner_record_missing_after_assign", "role_id", roleID,
		"hint", "刚分配完 Hub 却查不到归属记录;返回 WAIT 让客户端重查权威")
	return waitResume(loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN,
		ownerUnknownRetryAfterMs), nil
}

// enrichResumeFromMatchAuthority 在 owner 已定路由后,补充**非归属**的展示/恢复字段
// (权威 match stage / game_mode / map_id)。它绝不改 Route、target、owner_epoch 或状态:
// 撮合权威只回答"这个玩家在撮合流程的哪一步",不回答"他归谁管"。
// 富化失败只降级为字段缺失(客户端有 DS 握手后反查关卡表的兜底路径),不影响进场判定。
func (u *LoginUsecase) enrichResumeFromMatchAuthority(ctx context.Context, playerID uint64,
	out ResumeContextResult) ResumeContextResult {
	if u.matchResolver == nil {
		return out
	}
	ma, err := u.matchResolver.ResolvePlayerMatchContext(ctx, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "resume_match_enrichment_unavailable",
			"player_id", playerID, "err", err,
			"hint", "只影响 match_stage/game_mode/map_id 展示字段,不影响 owner 定的进场判定")
		return out
	}
	// 只有活跃 claim 才是可恢复的撮合会话;终态/漂移记录不该带给客户端。
	if ma.State != matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE {
		return out
	}
	if out.MatchID == 0 {
		out.MatchID = ma.MatchID
	}
	if out.MatchStage == loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_UNSPECIFIED {
		out.MatchStage = resumeStageFromMatchStage(ma.Stage)
	}
	if out.GameMode == "" {
		out.GameMode = ma.GameMode
	}
	if out.MapID == 0 {
		out.MapID = ma.MapID
	}
	return out
}

// resolveResumeRoute(locator-first 旧基线)已于 2026-07-29 删除。
//
// 它按 locator presence 推路由:key miss → 裸 HUB。§9.22 明文「key miss 只能说明 presence
// 不可见,不能证明玩家已离开旧 DS,也不能授权进入另一台 DS」——这条路径正是那个被禁止的
// fail-open 推导。它同时也永远填不出 §9.23 的五态(只给 route + match 富化字段),
// 客户端因此只能长期停在 legacy 兼容分支。
//
// 现在归属唯一权威是 owner:GetResumeContext → resolveResumeFromOwner,
// 明确"无归属"才落 resolveFirstEntry(角色门 → 分配首个 Hub → 回查 TARGET)。
// Battle 路由由 owner 记录的 owner_type=BATTLE 给出,不再由 locator 租约推导。

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
		assignStartedAt := time.Now()
		assign, aerr := u.hubAssigner.AssignHub(ctx, playerID, u.hubRegion, 0, roleID, sourceMatchID, sessJTI)
		assignMs := time.Since(assignStartedAt).Milliseconds()
		if aerr == nil && assign == nil {
			aerr = errcode.New(errcode.ErrUnavailable, "hub allocator returned an empty assignment")
		}
		if aerr == nil {
			summary, verr := u.verifyHubAssignmentTicket(playerID, assign)
			if verr != nil {
				h.Errorw("msg", "hub_assigner_returned_invalid_ticket", "err", verr,
					"reason", "ticket_verify_failed",
					"player_id", playerID, "hub_pod", assign.HubPodName)
				return "", "", 0, errcode.New(errcode.ErrUnavailable,
					"hub allocator returned an invalid ticket: %v", verr)
			}
			// §9.3 五要件必须落盘:Hub DS 拒票(target pod mismatch / assignment binding 类)时,
			// 这是 login 侧唯一能证明「我发出去的票绑的是哪个 assignment / 哪个实例 /
			// 哪条 release track」的记录;缺了它两边日志无法对账,灰度期 stable↔canary
			// 串号这类问题彻底查不出来。role_id/source_match_id 同时记「请求值」与「票内值」,
			// 用于发现 allocator 透传漂移。
			h.Infow("msg", "hub_assigned", "player_id", playerID,
				"hub_pod", assign.HubPodName, "shard_id", assign.ShardID, "hub_ds_addr", assign.HubDSAddr,
				"hub_assignment_id", summary.HubAssignmentID,
				"ds_instance_uid", summary.DSInstanceUID,
				"ds_instance_epoch", summary.DSInstanceEpoch,
				"ds_protocol_epoch", summary.DSProtocolEpoch,
				"release_track", summary.ReleaseTrack,
				"ticket_jti", summary.JTI, "ticket_exp_ms", summary.ExpMs,
				"ticket_ver", summary.TicketVersion,
				"ticket_role_id", summary.RoleID, "ticket_source_match_id", summary.SourceMatchID,
				"ticket_sjti", summary.SessJTI,
				"role_id", roleID, "source_match_id", sourceMatchID, "sess_jti", sessJTI,
				"region_id", regionID, "cell_id", cellID,
				"dur_assign_ms", assignMs)
			return assign.HubDSAddr, assign.HubTicket, summary.ExpMs, nil
		}
		if u.requireHubAssignmentBinding || u.rs256DSTicketProfileEnabled() {
			// strict 档:allocator 是唯一签票权威,拿不到票就没有 Hub 可进。
			// 此前这条直接静默 return,「全服卡在登录」时 login 侧连一行都没有,
			// 只能去 hub_allocator 侧猜(而那边可能根本没收到请求)。
			h.Errorw("msg", "hub_assign_failed", "err", aerr,
				"reason", "allocator_unavailable_strict",
				"player_id", playerID, "role_id", roleID, "region", u.hubRegion,
				"source_match_id", sourceMatchID, "dur_assign_ms", assignMs,
				"require_binding", u.requireHubAssignmentBinding,
				"rs256_profile", u.rs256DSTicketProfileEnabled())
			return "", "", 0, errcode.New(errcode.ErrUnavailable,
				"hub allocator required for RS256/assignment-bound ticket: %v", aerr)
		}
		// hub_allocator 不可用 → 回退自签,不阻断登录(玩家仍可凭票据连静态 hub DS)
		h.Warnw("msg", "hub_assign_failed_fallback_self_sign", "err", aerr, "player_id", playerID,
			"reason", "allocator_unavailable_fallback_self_sign",
			"role_id", roleID, "region", u.hubRegion, "dur_assign_ms", assignMs)
	}
	if u.requireHubAssignmentBinding || u.rs256DSTicketProfileEnabled() {
		// 部署配置缺口:strict 档却没配 hub_allocator 地址 → 本部署恒不可进 Hub。
		h.Errorw("msg", "hub_assign_failed", "reason", "allocator_not_configured_strict",
			"player_id", playerID, "role_id", roleID,
			"require_binding", u.requireHubAssignmentBinding,
			"rs256_profile", u.rs256DSTicketProfileEnabled(),
			"hint", "配置 login.hub_allocator addr,或关掉 RS256/assignment binding 档")
		return "", "", 0, errcode.New(errcode.ErrUnavailable,
			"hub allocator is required by the RS256/assignment-bound ticket profile")
	}

	ticket, expMs, err = u.signer.SignHubDSTicketFull(playerID, regionID, cellID, roleID, sourceMatchID, uuid.NewString())
	if err != nil {
		h.Errorw("msg", "sign_hub_ticket_failed", "err", err, "reason", "sign_failed",
			"player_id", playerID, "role_id", roleID)
		return "", "", 0, errcode.New(errcode.ErrInternal, "sign hub ticket failed: %v", err)
	}
	// R1:自签也是一次进场票据签发(dev/local 档),必须与 hub_assigned 同级别可见,
	// 否则「玩家连的是静态 hub 地址」这个事实在日志里完全不存在。
	h.Infow("msg", "hub_self_signed", "player_id", playerID,
		"hub_ds_addr", u.hubDSAddr, "ticket_exp_ms", expMs,
		"role_id", roleID, "source_match_id", sourceMatchID,
		"region_id", regionID, "cell_id", cellID,
		"hint", "legacy HS256 dev 档:hub_allocator 未配/不可用时的自签回退")
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
		plog.With(ctx).Warnw("msg", "hub_endpoint_rejected", "reason", "missing_player_id",
			"source_match_id", sourceMatchID)
		return "", "", 0, errcode.New(errcode.ErrInvalidArg, "playerID must be > 0")
	}
	fenceMatchID, gerr := u.guardHubRouteAgainstActiveBattle(ctx, playerID)
	if gerr != nil {
		return "", "", 0, gerr
	}
	regionID, cellID := u.routeRegionCell(ctx, playerID)
	// 选角权威化:返回大厅路径也把已选角盖进新票(与登录同语义,DS 重入时同样能 spawn 对角色)。
	// 角色权威不可判定时 fail-closed:回大厅是**签票**路径,拿不准角色就不能签
	// (签出 role=0 的票 = DS 侧按无角色 spawn,等于用一次查询抖动把玩家的角色抹掉)。
	roleID, roleErr := u.loadSelectedRole(ctx, playerID)
	if roleErr != nil {
		return "", "", 0, roleErr
	}
	return u.resolveHub(ctx, playerID, regionID, cellID, roleID, fenceMatchID, sessJTI)
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
		if u.strictBattleGateProfile() {
			h.Errorw("msg", "hub_route_rejected", "reason", "locator_not_configured",
				"player_id", playerID,
				"hint", "strict 档必须配 player_locator;否则无法证明玩家不在战斗")
			return 0, errcode.New(errcode.ErrUnavailable,
				"player locator is required before hub ticket issuance")
		}
		return 0, nil // local/off 无 locator:保留历史行为(dev 裸跑)。
	}
	bl, _, err := u.resolveBattleAuthority(ctx, playerID) // hub 门只关心在局与否,不需要 game_mode
	if err != nil {
		if u.strictBattleGateProfile() {
			h.Warnw("msg", "hub_route_rejected", "reason", "battle_authority_unavailable",
				"err", err, "player_id", playerID)
			return 0, errcode.NewCause(errcode.ErrUnavailable, err,
				"cannot prove player is outside battle before hub ticket issuance")
		}
		h.Warnw("msg", "hub_route_gate_locator_degraded", "err", err, "player_id", playerID,
			"reason", "battle_authority_degraded_allow")
		return 0, nil // local/off 保留历史弱降级。
	}
	if !bl.InBattle {
		return 0, nil
	}
	// locator 明确 InBattle:必须由 roster 权威区分“仍在活局”与“显式终局后 TTL 残留”。
	// 不可判定时不分 profile 一律 fail-closed:阳性 BATTLE 信号下猜“已结束”就是双归属。
	if u.battleTicketIssuer == nil {
		h.Errorw("msg", "hub_route_rejected", "reason", "battle_route_authority_not_configured",
			"player_id", playerID, "match_id", bl.MatchID)
		return 0, errcode.New(errcode.ErrUnavailable,
			"battle route authority unavailable while locator reports BATTLE")
	}
	state, rerr := u.battleTicketIssuer.InspectBattleRoute(ctx, playerID, bl.MatchID)
	switch state {
	case data.BattleRouteActive:
		h.Warnw("msg", "hub_route_rejected_active_battle", "reason", "battle_route_active",
			"player_id", playerID, "match_id", bl.MatchID)
		return 0, errcode.New(errcode.ErrInvalidState,
			"player is in active battle (match_id=%d); reconnect via Login instead of hub ticket", bl.MatchID)
	case data.BattleRouteTerminal:
		// 权威记录显式终态(ended/abandoned) → locator BATTLE 仅为 TTL 残留,放行 Hub
		// (正常结算回大厅),并把残留 match_id 作为回流 fence 交给签票路径。
		// R1:这是"从 Battle 回流到 Hub"的路由判定结果,且它决定了票据里的
		// source_match_id;Hub DS 侧 BATTLE→HUB guard 失败时要靠这条对账。
		h.Infow("msg", "hub_route_allowed_terminal_battle",
			"player_id", playerID, "match_id", bl.MatchID,
			"decision", "allow_hub_with_source_match_fence")
		return bl.MatchID, nil
	default:
		// UNKNOWN(含 roster 漂移/非成员/记录缺失/stale/错误):不得猜测,拒绝。
		h.Warnw("msg", "hub_route_rejected_unknown_battle_state",
			"reason", "battle_route_unknown",
			"player_id", playerID, "match_id", bl.MatchID, "err", rerr)
		return 0, errcode.NewCause(errcode.ErrUnavailable, rerr,
			"cannot prove battle is over before hub ticket issuance")
	}
}

// loadSelectedRole 读玩家已选角色(player_roles)。
//
// **返回 error 而不是把失败折叠成 0**(§9.23:「角色查询失败不等于 role=0」)。
// 折叠会把两件性质完全不同的事混成一个值:
//   - role=0 = 权威明确回答"没选角" → ROLE_REQUIRED,客户端去选角界面;
//   - 查询失败 = 结果不可判定(UNKNOWN) → WAIT + 退避重查(§9.22 禁冒充默认状态)。
//
// 混淆的后果不只是文案不准:未选角时不得提前分配 Hub、占座或签票(§9.23),
// 而把"查询失败"当成 role=0 会让一个其实已选角的玩家走进未选角分支。
//
// roleRepo 未配(dev 裸跑)= 无角色权威,按"没选角"处理并告警,不算查询失败。
func (u *LoginUsecase) loadSelectedRole(ctx context.Context, playerID uint64) (uint32, error) {
	if u.roleRepo == nil {
		return 0, nil
	}
	roleID, err := u.roleRepo.GetRole(ctx, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "load_selected_role_failed", "err", err, "player_id", playerID,
			"hint", "角色权威不可判定:按 WAIT/ROLE_UNKNOWN 处理,不得当作未选角")
		return 0, errcode.NewCause(errcode.ErrUnavailable, err,
			"selected role unavailable player_id=%d", playerID)
	}
	return roleID, nil
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
		h.Warnw("msg", "select_role_rejected", "reason", "missing_player_id", "role_id", roleID)
		return "", "", 0, errcode.New(errcode.ErrInvalidArg, "playerID must be > 0")
	}
	if roleID == 0 {
		h.Warnw("msg", "select_role_rejected", "reason", "missing_role_id", "player_id", playerID)
		return "", "", 0, errcode.New(errcode.ErrInvalidArg, "roleID must be > 0")
	}
	// SelectRole 也是 Hub 物理副作用入口:先过 active-BATTLE 三态权威门。
	fenceMatchID, gerr := u.guardHubRouteAgainstActiveBattle(ctx, playerID)
	if gerr != nil {
		// 门内已按 reason 落盘;这里补一条把「被拒的是 SelectRole」钉死,
		// 否则 hub_route_rejected* 分不清来自 SelectRole 还是 IssueDSTicket(hub)。
		h.Warnw("msg", "select_role_rejected", "reason", "hub_route_gate_rejected",
			"player_id", playerID, "role_id", roleID, "err", gerr)
		return "", "", 0, gerr
	}
	if len(u.allowedRoleIDs) > 0 {
		if _, ok := u.allowedRoleIDs[roleID]; !ok {
			h.Warnw("msg", "select_role_not_allowed", "reason", "role_not_in_whitelist",
				"player_id", playerID, "role_id", roleID)
			return "", "", 0, errcode.New(errcode.ErrInvalidArg, "role_id=%d not allowed", roleID)
		}
	} else if !u.devAllowAnyRole {
		// fail-closed:白名单没配就放行任意 role_id = 改包客户端可把任意角色配置 ID 签进 hub 票据
		// (hub_allocator 无二次校验)。生产必须配 allowed_role_ids;dev 宽松需显式开 dev_allow_any_role。
		h.Errorw("msg", "select_role_rejected_no_whitelist", "reason", "whitelist_not_configured",
			"player_id", playerID, "role_id", roleID,
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
			// reason 拆开:会话被顶(ErrSessionSuperseded / ErrUnauthorized)是"旧设备被
			// fencing",与 DB 写失败必须可判别——前者是预期安全行为,后者是事故。
			reason := "persist_failed"
			switch errcode.As(serr) {
			case errcode.ErrSessionSuperseded:
				reason = "session_superseded"
			case errcode.ErrUnauthorized:
				reason = "session_not_current"
			case errcode.ErrUnavailable:
				reason = "session_authority_unavailable"
			}
			h.Errorw("msg", "select_role_persist_failed", "err", serr, "reason", reason,
				"player_id", playerID, "role_id", roleID,
				"gen_enforce", u.sessionGenEnforce, "sess_jti", sessJTI)
			return "", "", 0, serr
		}
	} else {
		h.Warnw("msg", "select_role_repo_nil_skip_persist", "reason", "role_repo_not_configured",
			"player_id", playerID, "role_id", roleID)
	}

	regionID, cellID := u.routeRegionCell(ctx, playerID)
	addr, ticket, expMs, err = u.resolveHub(ctx, playerID, regionID, cellID, roleID, fenceMatchID, sessJTI)
	if err != nil {
		h.Errorw("msg", "select_role_resolve_hub_failed", "err", err, "reason", "hub_resolve_failed",
			"player_id", playerID, "role_id", roleID, "source_match_id", fenceMatchID)
		return "", "", 0, err
	}
	// R1:选角完成是不可逆状态推进(角色已落库 + 已签出新 hub 票),必须 INFO。
	// 原为 Debug——线上默认 info 级下,「玩家到底选没选角、选的哪个角色、拿到哪台 Hub」
	// 全部不可见,而这正是"卡在选角界面"类问题的第一个判据。
	h.Infow("msg", "select_role_ok", "player_id", playerID, "role_id", roleID,
		"hub_ds_addr", addr, "hub_ticket_exp_ms", expMs,
		"source_match_id", fenceMatchID, "sess_jti", sessJTI,
		"region_id", regionID, "cell_id", cellID)
	return addr, ticket, expMs, nil
}

// hubTicketSummary 是 verifyHubAssignmentTicket 从**已验签** hub 票 claims 里提取的只读摘要。
//
// 本函数此前只返回 exp:五要件(hub_assignment_id / ds_uid / ds_instance_epoch /
// release_track / jti)刚校验完就被全部丢弃(data.HubAssignment 里也没有这些字段),
// 于是 Hub DS 拒票时 login 侧拿不出任何能与 DS 日志对账的记录。摘要只用于日志,
// 不参与任何判定,也不改变原有的 fail-closed 校验顺序。
type hubTicketSummary struct {
	// TicketVersion:1=legacy HS256,auth.DSTicketVersion2=v2 RS256。
	TicketVersion   int
	ExpMs           int64
	JTI             string
	HubAssignmentID string
	DSInstanceUID   string
	DSInstanceEpoch uint32 // v2 稳定实例代次;legacy 票恒 0
	DSProtocolEpoch uint32 // legacy callback credential 绑定;v2 票恒 0
	ReleaseTrack    string // v2 灰度轨道;legacy 票恒空
	SessJTI         string // v2 会话绑定(§9.23);legacy 票恒空
	RoleID          uint32
	SourceMatchID   uint64
}

// verifyHubAssignmentTicket 验证 hub_allocator 返回的票据并读取已验签 exp 与五要件摘要。
//
// 迁移期只允许两条显式路径：HS256=legacy，RS256=v2。alg 仅用于选择 verifier，随后分支
// 必须完成各自签名/claims 校验；其它算法、缺 verifier、坏票和不完整绑定全部 fail-closed。
func (u *LoginUsecase) verifyHubAssignmentTicket(playerID uint64, assign *data.HubAssignment) (hubTicketSummary, error) {
	if assign == nil || assign.HubTicket == "" {
		return hubTicketSummary{}, errcode.New(errcode.ErrLoginTicketInvalid, "hub assignment ticket is empty")
	}
	alg, err := auth.DSTicketAlgorithm(assign.HubTicket)
	if err != nil {
		return hubTicketSummary{}, err
	}
	switch alg {
	case "HS256":
		// v2 verifier 非 nil 是 Login 主链的机械 RS256-only 开关；不能因为收到一张
		// HS256 票就退回 legacy verifier。SessionToken 的 HS256 验证不经过本函数。
		if u.rs256DSTicketProfileEnabled() {
			return hubTicketSummary{}, errcode.New(errcode.ErrLoginTicketInvalid,
				"legacy HS256 DSTicket is disabled by the RS256 profile")
		}
		if u == nil || u.verifier == nil {
			return hubTicketSummary{}, errcode.New(errcode.ErrUnavailable, "legacy DSTicket verifier unavailable")
		}
		claims, err := u.verifier.VerifyDSTicket(assign.HubTicket)
		if err != nil {
			return hubTicketSummary{}, err
		}
		if claims.PlayerID() != playerID || claims.DSType != string(auth.DSTypeHub) {
			return hubTicketSummary{}, errcode.New(errcode.ErrLoginTicketInvalid,
				"legacy hub ticket player or ds_type mismatch")
		}
		if claims.ExpiresAt == nil {
			return hubTicketSummary{}, errcode.New(errcode.ErrLoginTicketInvalid, "legacy hub ticket missing exp")
		}
		// 兼容窗内的旧票允许完全没有实例绑定；一旦携带 pod，就必须与 allocator 响应一致。
		if claims.DSPodName != "" && claims.DSPodName != assign.HubPodName {
			return hubTicketSummary{}, errcode.New(errcode.ErrLoginTicketInvalid, "legacy hub ticket target pod mismatch")
		}
		if u.requireHubAssignmentBinding &&
			(assign.HubPodName == "" || claims.DSPodName != assign.HubPodName ||
				claims.DSInstanceUID == "" || claims.DSProtocolEpoch == 0 ||
				claims.DSCredentialGen == 0 || claims.DSCredentialJTI == "" ||
				claims.HubAssignmentID == "" || claims.DSWriterEpoch != auth.DSAuthWriterEpochV2) {
			return hubTicketSummary{}, errcode.New(errcode.ErrLoginTicketInvalid,
				"legacy hub ticket assignment binding is incomplete")
		}
		return hubTicketSummary{
			TicketVersion:   1,
			ExpMs:           claims.ExpiresAt.UnixMilli(),
			JTI:             claims.ID,
			HubAssignmentID: claims.HubAssignmentID,
			DSInstanceUID:   claims.DSInstanceUID,
			DSProtocolEpoch: claims.DSProtocolEpoch,
			RoleID:          claims.RoleID,
			SourceMatchID:   claims.SourceMatchID,
		}, nil

	case "RS256":
		if u == nil || u.v2Verifier == nil {
			return hubTicketSummary{}, errcode.New(errcode.ErrUnavailable, "DSTicket v2 verifier unavailable")
		}
		claims, err := u.v2Verifier.Verify(assign.HubTicket)
		if err != nil {
			return hubTicketSummary{}, err
		}
		if claims.PlayerID() != playerID || claims.DSType != string(auth.DSTypeHub) {
			return hubTicketSummary{}, errcode.New(errcode.ErrLoginTicketInvalid,
				"hub DSTicket v2 player or ds_type mismatch")
		}
		if assign.HubPodName == "" || claims.DSPodName != assign.HubPodName {
			return hubTicketSummary{}, errcode.New(errcode.ErrLoginTicketInvalid, "hub DSTicket v2 target pod mismatch")
		}
		if claims.DSInstanceUID == "" || claims.DSInstanceEpoch == 0 ||
			claims.HubAssignmentID == "" ||
			(claims.ReleaseTrack != auth.ReleaseTrackStable && claims.ReleaseTrack != auth.ReleaseTrackCanary) {
			return hubTicketSummary{}, errcode.New(errcode.ErrLoginTicketInvalid,
				"hub DSTicket v2 instance binding is incomplete")
		}
		if claims.ExpiresAt == nil {
			return hubTicketSummary{}, errcode.New(errcode.ErrLoginTicketInvalid, "hub DSTicket v2 missing exp")
		}
		return hubTicketSummary{
			TicketVersion:   auth.DSTicketVersion2,
			ExpMs:           claims.ExpiresAt.UnixMilli(),
			JTI:             claims.ID,
			HubAssignmentID: claims.HubAssignmentID,
			DSInstanceUID:   claims.DSInstanceUID,
			DSInstanceEpoch: claims.DSInstanceEpoch,
			ReleaseTrack:    claims.ReleaseTrack,
			SessJTI:         claims.SessJTI,
			RoleID:          claims.RoleID,
			SourceMatchID:   claims.SourceMatchID,
		}, nil

	default:
		// DSTicketAlgorithm 已先拒绝其它 alg；保留 default 作为未来改动的 fail-closed 保险。
		return hubTicketSummary{}, errcode.New(errcode.ErrLoginTicketInvalid, "DSTicket algorithm unsupported")
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
		// token 不合法不算业务错(可能客户端 token 过期了),直接返 OK。
		// 但它意味着**本次登出什么都没做**:session 没删、owner 没释放。
		// 「玩家说退出后还显示在线」的第一嫌疑就是这里,故按 R2 升 Warn + reason。
		h.Warnw("msg", "logout_verify_session_failed", "err", err, "reason", "session_verify_failed",
			"hint", "本次 Logout 未删除任何会话/归属,返回 OK 供客户端 fire-and-forget")
		return nil
	}
	playerID := claims.PlayerID()
	if playerID == 0 {
		h.Warnw("msg", "logout_session_no_player", "reason", "session_no_player_id")
		return nil
	}
	// §11.3 R3:Logout 同样是 session token 面,ctx 里没有 player_id。
	ctx = plog.WithPlayerID(ctx, playerID)
	// P0 修复(2026-07-15,codex P0-10):只删"本 token 对应的那一代 session"。
	// 顶号后旧设备的迟到 Logout 携带旧 jti,CAS 不命中 → 不影响新设备 session。
	deleted, err := u.sessions.DeleteIfJTI(ctx, playerID, claims.ID)
	if err != nil {
		h.Errorw("msg", "logout_session_del_failed", "err", err, "reason", "session_del_failed",
			"player_id", playerID, "sess_jti", claims.ID)
		return err
	}
	if !deleted {
		h.Infow("msg", "logout_stale_session_ignored", "player_id", playerID,
			"reason", "session_jti_not_current", "sess_jti", claims.ID)
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
				"reason", "generation_tombstone_failed", "player_id", playerID, "err", terr)
		}
	}
	// owner 迁移释放(owner-authority.md migrate ⑤,弱依赖):显式登出后释放当前 owner。
	// Query→Release 携带观察到的 epoch+operation(compare-delete 自己):并发迁移竞态下
	// Release 在 owner 侧幂等 no-op,绝不误删新 owner;失败仅告警,不影响登出结果。
	if u.ownerReleaser != nil {
		if rec, oerr := u.ownerReleaser.QueryOwner(ctx, playerID); oerr != nil {
			h.Warnw("msg", "logout_owner_query_failed_weak", "reason", "owner_query_failed",
				"player_id", playerID, "err", oerr)
		} else if rec.OwnerType != 0 {
			if rerr := u.ownerReleaser.ReleaseOwner(ctx, playerID, rec.OwnerEpoch, rec.OperationID); rerr != nil {
				h.Warnw("msg", "logout_owner_release_failed_weak", "reason", "owner_release_failed",
					"player_id", playerID, "owner_epoch", rec.OwnerEpoch,
					"operation_id", rec.OperationID, "err", rerr)
			} else {
				// R1:owner 释放是不可逆归属推进(玩家从此不再属于任何 DS)。
				// 缺了它,「玩家重登后为什么被分到新 Hub / 旧 DS 为什么被踢」无从对账。
				h.Infow("msg", "logout_owner_released", "player_id", playerID,
					"owner_type", rec.OwnerType, "owner_epoch", rec.OwnerEpoch,
					"operation_id", rec.OperationID)
			}
		}
	}
	// R1:登出是链路终点的不可逆状态推进(§11.3 点名的成对阶段 login_ok / logout_ok)。
	// 原为 Debug——线上默认 info 级下,「玩家到底主动登出过没有」查不到,
	// 于是"顶号"与"自己退了又登"这两种完全不同的现象无法区分。
	h.Infow("msg", "logout_ok", "player_id", playerID, "sess_jti", claims.ID)
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

// reconcileFailedSessionWrite 收口「MySQL 代际已提交、Redis 条件写结果不确定」。
// 本次 Login 必定失败且零凭据交付，因此补偿不能猜测即时前代就是最后已交付会话：
// A(已交付)→B(未交付)→C(未交付) 时，恢复 C 的即时前代会把 B 永久立为 current。
//
// 标准 fail-closed 做法是两侧各自写无能力墓碑并保留单调 generation：
//   - MySQL 只在当前仍是本次 (jti,gen) 时墓碑，更新赢家已推进则 no-op；
//   - Redis 在 current.gen <= failedGen 时清能力并推进到 failedGen。不能只精确匹配
//     failedJTI：C 的 Redis 写未落地时 Redis 仍可能停在未交付 B，必须一并 fence；
//   - Redis 已是更高代际则 no-op，绝不影响赢家。
//
// 两步使用独立 detached 有界 context，且无论一侧 error/no-op 都执行另一侧；否则
// “MySQL 已被 C 推进、C 的 Redis 尚未落地”会跳过必要的 Redis fence。补偿仍可能因
// 依赖持续不可达而失败，登录以 ErrUnavailable 触发客户端有界重试，新 gen+1 自愈。
func (u *LoginUsecase) reconcileFailedSessionWrite(
	ctx context.Context, playerID uint64, sessJTI string, sessGen uint64, sessTTL time.Duration,
) {
	h := plog.With(ctx)
	mysqlCtx, cancelMySQL := context.WithTimeout(plog.Detach(ctx), sessionReconcileTimeout)
	mysqlFenced, mysqlErr := u.sessionGen.TombstoneFailedSessionJTI(mysqlCtx, playerID, sessJTI, sessGen)
	cancelMySQL()
	if mysqlErr != nil {
		h.Errorw("msg", "session_generation_failed_attempt_tombstone_failed", "err", mysqlErr,
			"player_id", playerID, "gen", sessGen)
	} else if mysqlFenced {
		h.Warnw("msg", "session_generation_failed_attempt_tombstoned",
			"player_id", playerID, "gen", sessGen)
	}

	redisCtx, cancelRedis := context.WithTimeout(plog.Detach(ctx), sessionReconcileTimeout)
	redisFenced, redisErr := u.sessions.FenceFailedSet(redisCtx, playerID, sessJTI, sessGen, sessTTL)
	cancelRedis()
	if redisErr != nil {
		h.Errorw("msg", "session_failed_attempt_redis_fence_failed", "err", redisErr,
			"player_id", playerID, "gen", sessGen)
	} else if redisFenced {
		h.Warnw("msg", "session_failed_attempt_redis_fenced",
			"player_id", playerID, "gen", sessGen)
	}
}

// sessionReconcileTimeout 是跨存储补偿链每一步的独立预算(脱离请求 ctx 后必须自带
// 上界,否则补偿会变成无界后台调用)。条件写都是单语句 CAS,5s 足够且远小于会话 TTL。
const sessionReconcileTimeout = 5 * time.Second

// touchDeviceTimeout 是 detached 设备记账的独立预算(单语句 upsert,健康 P99 ms 级,
// 取保守偏大值;待实测复核)。脱离请求 ctx 后必须自带上界(§16.7)。
const touchDeviceTimeout = 3 * time.Second

// touchDeviceAsync 把「最近登录设备」记账移出登录关键路径(压测审核【必修-1】):
// 纯记账副作用,失败本就只记日志,没有理由让它的 MySQL 往返占用 prod 5s 登录预算。
// detached ctx(plog.Detach 只复制 trace/player 等日志字段,剥离请求取消与 transport,
// §16.7)+ 有界超时;身份参数按值显式传递;safego 兜 panic(不崩进程)。
// at-most-once 语义可接受:丢一次记录,下次登录 upsert 自然补上(无业务语义)。
func (u *LoginUsecase) touchDeviceAsync(ctx context.Context, playerID uint64, deviceID string) {
	dctx, cancel := context.WithTimeout(plog.Detach(ctx), touchDeviceTimeout)
	safego.Go(dctx, "login_touch_device", func() {
		defer cancel()
		if err := u.repo.TouchDevice(dctx, playerID, deviceID); err != nil {
			plog.With(dctx).Warnw("msg", "touch_device_failed",
				"err", err, "player_id", playerID, "device_id", deviceID)
		}
	})
}

// fenceUnresolvedSessionGeneration 处理「MySQL COMMIT 不确定且读回也失败」。
// 此时 Redis Set 尚未执行，而事务内读到的 generation 只有在 MySQL 条件墓碑明确
// 命中后才被证明已持久占用。若 MySQL 返回 no-op/error，该 generation 可能由后续赢家
// 复用；拿它清 Redis 会误杀“同 generation、不同 jti”的合法赢家。因此：
//   - MySQL 墓碑命中：再用独立预算清理不高于该 generation 的 Redis 能力；
//   - MySQL no-op/error：禁止猜测，不碰 Redis，等待下一次 Login/权威恢复收敛。
//
// 这与普通 sessions.Set 失败不同：后者执行前 MySQL COMMIT 已被确认，generation 不会
// 被复用，故两侧 fencing 可以且必须无条件各自尝试。
func (u *LoginUsecase) fenceUnresolvedSessionGeneration(
	ctx context.Context, playerID uint64, sessJTI string, sessTTL time.Duration,
	lease data.SessionGenerationLease, probeErr error,
) {
	h := plog.With(ctx)
	mysqlCtx, cancelMySQL := context.WithTimeout(plog.Detach(ctx), sessionReconcileTimeout)
	fenced, ferr := u.sessionGen.TombstoneFailedSessionJTI(mysqlCtx, playerID, sessJTI, lease.Generation)
	cancelMySQL()
	switch {
	case ferr != nil:
		h.Errorw("msg", "session_generation_unresolved_tombstone_failed",
			"player_id", playerID, "gen", lease.Generation, "err", ferr, "probe_err", probeErr,
			"hint", "MySQL 可能停在从未交付的新代际;下一次成功登录会原子推进两存储自愈")
	case fenced:
		h.Warnw("msg", "session_generation_unresolved_tombstoned",
			"player_id", playerID, "gen", lease.Generation,
			"hint", "不确定 COMMIT 已落地且被条件改为无能力墓碑")
	default:
		h.Infow("msg", "session_generation_unresolved_tombstone_noop",
			"player_id", playerID, "gen", lease.Generation,
			"hint", "条件未命中:本次写未落地或已被更高代际登录取代")
	}

	if !fenced || u.sessions == nil {
		return
	}
	redisCtx, cancelRedis := context.WithTimeout(plog.Detach(ctx), sessionReconcileTimeout)
	redisFenced, redisErr := u.sessions.FenceFailedSet(
		redisCtx, playerID, sessJTI, lease.Generation, sessTTL)
	cancelRedis()
	if redisErr != nil {
		h.Errorw("msg", "session_generation_unresolved_redis_fence_failed",
			"player_id", playerID, "gen", lease.Generation, "err", redisErr, "probe_err", probeErr)
	} else if redisFenced {
		h.Warnw("msg", "session_generation_unresolved_redis_fenced",
			"player_id", playerID, "gen", lease.Generation,
			"hint", "读回不可用时撤销不高于失败代际的 Redis 能力")
	}
}

// resolveAmbiguousSessionGeneration 判定「COMMIT 结果不确定」(R11 复审 P0-1 问题 A)。
//
// 本次 jti 是全局唯一的一次性标记,因此读回权威行即可把不确定态判成事实,三种结果:
//
//	① 行内 jti == 本次 jti → COMMIT **确实生效**。返回成功继续登录；若随后 Redis
//	   失败，统一把本代墓碑，不恢复即时前代。
//	② 行不存在,或 jti 已不是本次 → 本次写没落地,或已被更高代际的登录取代(定序输家)。
//	   直接失败,零补偿:行属于赢家,任何墓碑都会破坏别人的登录。
//	③ 读回本身失败 → 仍不可判定。对本次 (jti,gen) 做条件无能力墓碑后
//	   fail-closed；下一次可重试 Login 以更高 generation 自愈。
func (u *LoginUsecase) resolveAmbiguousSessionGeneration(
	ctx context.Context, playerID uint64, sessJTI string, sessTTL time.Duration,
	lease data.SessionGenerationLease,
) (data.SessionGenerationLease, error) {
	h := plog.With(ctx)
	// 判定读与补偿都不能跑在可能已取消的请求 ctx 上(同问题 B)。
	// **两者各自独立计时**:共用一个预算时,读回吃满 5s 会让补偿一点执行时间都不剩,
	// 否则判定读耗尽预算后，墓碑在最需要它的场景里恰好没有执行时间。
	probeCtx, cancelProbe := context.WithTimeout(plog.Detach(ctx), sessionReconcileTimeout)
	defer cancelProbe()

	currentJTI, generation, found, err := u.sessionGen.LoadSessionGeneration(probeCtx, playerID)
	if err != nil {
		h.Errorw("msg", "session_generation_commit_unresolved", "player_id", playerID, "err", err,
			"hint", "COMMIT 结果不确定且读回失败 → 条件无能力墓碑后 fail-closed")
		u.fenceUnresolvedSessionGeneration(ctx, playerID, sessJTI, sessTTL, lease, err)
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
	lease.Generation = generation
	return lease, nil
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

// GetPlayerNo 查**当前角色**的角色编号(展示专用,player-no-and-login-surge.md §3)。
//
// 入参名为 playerID 是历史口径:今天 player_id 一身兼两职(账号身份 + 角色身份)。
// 编号语义上绑定**角色实体**(§3.6.1,卖角色业务决定:一账号建 N 角色 = N 个编号,
// 过户时随角色走),故多角色改造后本方法签名不变——它查的一直是「当前角色的编号」。
//
// 存在理由:编号由补号任务异步分配,而「首登即注册」使 Login 响应里的编号必然是 0;
// 没有本查询,新玩家整个首次会话都停在「生成中」。客户端拿到 0 时补拉即可。
//
// 返回 0 不是错误,是「仍在补号窗口内」(约 15s = 5s 补号周期 + 10s 水位滞后)的正常态,
// 与 repo 层「NULL / 行不存在 → 0」同一口径。查询失败才返回 error(客户端可重试)。
//
// 刻意不做会话现行性(sjti)复核:本 RPC 只读且只能读自己,零副作用、不发凭据、不改归属,
// 顶号场景下旧设备读到自己的展示编号无任何安全含义。加 fencing 只会让「被顶号后界面
// 显示异常」多一种表现,不增加任何保护(对比 SelectRole 必须复核——它签发 hub 票据)。
func (u *LoginUsecase) GetPlayerNo(ctx context.Context, playerID uint64) (uint64, error) {
	if playerID == 0 {
		return 0, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	return u.repo.GetPlayerNo(ctx, playerID)
}
