// login_test.go — LoginUsecase.resolveHub 行为单测(W4 ⑥,2026-06-06)。
//
// 覆盖 hub_allocator 弱依赖三态:
//   - hubAssigner 非 nil 且 AssignHub 成功 → 用 allocator 返回的 hub_ds_addr + hub_ticket
//   - hubAssigner 为 nil → 回退自签 hub 票据 + 静态 hubDSAddr
//   - hubAssigner 返回错误 → 回退自签(不阻断登录)
package biz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/passwd"
	"github.com/luyuancpp/pandora/pkg/snowflake"
	locatorv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/locator/v1"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	"github.com/luyuancpp/pandora/services/account/login/internal/data"
)

const (
	testSecret   = "pandora-dev-jwt-secret-change-me-32!" // 36 字节,满足 HS256 ≥32
	testPlayerNo = uint64(100001)
)

// mustBcrypt 用 DevCost 哈希明文密码,失败 fatal。
func mustBcrypt(t *testing.T, plain string) string {
	t.Helper()
	h, err := passwd.Hash(plain, passwd.DevCost)
	if err != nil {
		t.Fatalf("passwd.Hash: %v", err)
	}
	return h
}

// ---- fakes ----

type fakeAccountRepo struct {
	playerID     uint64
	passwordHash string
	banned       bool
	playerNo     uint64
	playerNoFn   func(context.Context) (uint64, error)
}

func (f *fakeAccountRepo) FindByAccount(_ context.Context, _ string) (data.AccountIdentity, error) {
	return data.AccountIdentity{AccountID: f.playerID, PlayerID: f.playerID, PasswordHash: f.passwordHash}, nil
}
func (f *fakeAccountRepo) FindByAccountID(_ context.Context, accountID uint64) (data.AccountIdentity, error) {
	return data.AccountIdentity{AccountID: accountID, PlayerID: f.playerID, PasswordHash: f.passwordHash}, nil
}
func (f *fakeAccountRepo) CreateAccount(_ context.Context, _, _ uint64, _, _ string) error {
	return nil
}
func (f *fakeAccountRepo) BackfillAccountID(_ context.Context, _, candidate uint64) (uint64, error) {
	return candidate, nil
}
func (f *fakeAccountRepo) CheckBanned(_ context.Context, _ uint64, _ string) (bool, error) {
	return f.banned, nil
}
func (f *fakeAccountRepo) TouchDevice(_ context.Context, _ uint64, _ string) error { return nil }
func (f *fakeAccountRepo) GetPlayerNo(ctx context.Context, _ uint64) (uint64, error) {
	if f.playerNoFn != nil {
		return f.playerNoFn(ctx)
	}
	return f.playerNo, nil
}

// fakeSessionRepo 记住 Set 写入的 jti 并在 GetJTI 返回真实值:R5 复审 P0-5 起 Login
// 在交付前复核本次写入的 jti 仍是当前一代,无状态假件会被终检误判为"会话已消失"。
type fakeSessionRepo struct {
	mu  sync.Mutex
	jti map[uint64]string
	gen map[uint64]uint64
}

func newFakeSessionRepo() *fakeSessionRepo {
	return &fakeSessionRepo{jti: map[uint64]string{}, gen: map[uint64]uint64{}}
}

func (f *fakeSessionRepo) Set(_ context.Context, playerID uint64, _, jti, _ string, _ time.Duration, gen uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.jti == nil { // 零值可用:嵌入方(rotatingSessionRepo 等)不经构造函数
		f.jti = map[uint64]string{}
		f.gen = map[uint64]uint64{}
	}
	f.jti[playerID] = jti
	f.gen[playerID] = gen
	return nil
}

func (f *fakeSessionRepo) Delete(_ context.Context, playerID uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.jti, playerID)
	delete(f.gen, playerID)
	return nil
}

func (f *fakeSessionRepo) GetJTI(_ context.Context, playerID uint64) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jti[playerID]
	return j, ok && j != "", nil
}

// DeleteIfJTI 与生产实现同语义的 CAS 删(仅当前 jti 相等才删)，覆盖迟到 Logout 场景。
func (f *fakeSessionRepo) DeleteIfJTI(_ context.Context, playerID uint64, jti string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if cur, ok := f.jti[playerID]; ok && cur == jti {
		delete(f.jti, playerID)
		return true, nil
	}
	return false, nil
}

func (f *fakeSessionRepo) FenceFailedSet(_ context.Context, playerID uint64, _ string, gen uint64, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if curGen, ok := f.gen[playerID]; ok && curGen > gen {
		return false, nil
	}
	delete(f.jti, playerID)
	f.gen[playerID] = gen
	return true, nil
}

type loginBattleAuthorizerFake struct {
	err         error
	target      data.BattleTicketTarget
	returnEmpty bool

	// authorizeCalls:AuthorizeBattleTicket 被调用次数(验证“resume 组不出来就不签票”零副作用)。
	authorizeCalls int

	// ---- InspectBattleRoute(Hub 门三态)可控项 ----
	// routeStates 非空:按调用序依次弹出(TOCTOU 并发终局切换场景)。
	// routeErr 非 nil:恒返 (Unknown, routeErr)。
	// routeState 显式非 Unknown:恒返 (routeState, nil)。
	// 都未设:err 非 nil → (Unknown, err)；否则 (Active, nil)。
	routeStates []data.BattleRouteState
	routeErr    error
	routeState  data.BattleRouteState
	routeCalls  int
}

var _ data.BattleRouteInspector = (*loginBattleAuthorizerFake)(nil)

func (f *loginBattleAuthorizerFake) AuthorizeBattleTicket(context.Context, uint64, uint64) (data.BattleTicketTarget, error) {
	f.authorizeCalls++
	if f.err != nil {
		return data.BattleTicketTarget{}, f.err
	}
	if f.returnEmpty {
		return data.BattleTicketTarget{}, nil
	}
	if f.target.DSAddr == "" {
		f.target = data.BattleTicketTarget{DSAddr: "10.1.2.3:7000", PodName: "battle-test"}
	}
	return f.target, nil
}

func (f *loginBattleAuthorizerFake) InspectBattleRoute(context.Context, uint64, uint64) (data.BattleRouteState, error) {
	f.routeCalls++
	if len(f.routeStates) > 0 {
		s := f.routeStates[0]
		f.routeStates = f.routeStates[1:]
		return s, nil
	}
	if f.routeErr != nil {
		return data.BattleRouteUnknown, f.routeErr
	}
	if f.routeState != data.BattleRouteUnknown {
		return f.routeState, nil
	}
	if f.err != nil {
		return data.BattleRouteUnknown, f.err
	}
	return data.BattleRouteActive, nil
}

type fakeHubAssigner struct {
	res *data.HubAssignment
	err error

	gotPlayerID      uint64
	gotRegion        string
	gotTeamID        uint64
	gotRoleID        uint32
	gotSourceMatchID uint64
}

func (f *fakeHubAssigner) AssignHub(_ context.Context, playerID uint64, region string, teamID uint64, roleID uint32, sourceMatchID uint64, _ string) (*data.HubAssignment, error) {
	f.gotPlayerID = playerID
	f.gotRegion = region
	f.gotTeamID = teamID
	f.gotRoleID = roleID
	f.gotSourceMatchID = sourceMatchID
	if f.err != nil {
		return nil, f.err
	}
	return f.res, nil
}

type unavailableRoleRepo struct {
	err error
}

func (f *unavailableRoleRepo) GetRole(context.Context, uint64) (uint32, error) {
	return 0, f.err
}

func (f *unavailableRoleRepo) SetRole(context.Context, uint64, uint32, string, func(context.Context) error) error {
	return nil
}

// fakeNotifier 实现 data.LocationNotifier(断线重连测试用)。
type fakeNotifier struct {
	bl            data.BattleLocation
	blErr         error
	notifyErr     error
	loginPendingN int

	// failFirst:前 failFirst 次 GetBattleLocation 返回 blErr(模拟 locator 瞬时抖动),
	// 之后返回 bl(验证有界重试能把可恢复失败救回来)。0 表示行为由 blErr 恒定决定。
	failFirst int
	getN      int // GetBattleLocation 被调用次数
}

func (f *fakeNotifier) NotifyLoginPending(_ context.Context, _ uint64, _ string) error {
	f.loginPendingN++
	return f.notifyErr
}

func (f *fakeNotifier) GetBattleLocation(_ context.Context, _ uint64) (data.BattleLocation, error) {
	f.getN++
	if f.getN <= f.failFirst {
		err := f.blErr
		if err == nil {
			err = errcode.New(errcode.ErrInternal, "transient locator blip")
		}
		return data.BattleLocation{}, err
	}
	return f.bl, f.blErr
}

// newTestUsecase 构造一个登录用例(密码 bcrypt 校验在 biz 之外,这里直接给明文等值匹配)。
func newTestUsecase(t *testing.T, hub data.HubAssigner) *LoginUsecase {
	t.Helper()
	return newTestUsecaseWithNotifier(t, hub, nil)
}

// newTestUsecaseWithNotifier 同 newTestUsecase,但可注入 locator notifier(断线重连测试用)。
func newTestUsecaseWithNotifier(t *testing.T, hub data.HubAssigner, notifier data.LocationNotifier) *LoginUsecase {
	t.Helper()
	cfg := auth.Config{Secret: []byte(testSecret)}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	// bcrypt 哈希一个固定密码 "pw",让 passwd.Verify 通过。
	hash := mustBcrypt(t, "pw")
	repo := &fakeAccountRepo{playerID: 42, passwordHash: hash, playerNo: testPlayerNo}
	sf := snowflake.NewNode(1)
	uc := NewLoginUsecase(repo, newFakeSessionRepo(), notifier, hub, &fakeRoleRepo{roleID: 7}, sf, "127.0.0.1:7777", "cn", signer, verifier, nil, false, false, nil, false)
	ticketUC := NewTicketUsecase(signer, verifier, nil)
	ticketUC.SetBattleTicketAuthorizer(&loginBattleAuthorizerFake{})
	uc.SetBattleTicketIssuer(ticketUC)
	uc.SetOwnerPlacementQuerier(&fakeOwnerPlacementQuerier{view: stableTestOwnerPlacement(ownerTypeHub)})
	return uc
}

func stableTestOwnerPlacement(ownerType int8) data.OwnerPlacementView {
	view := data.OwnerPlacementView{
		OwnerEpoch:               7,
		OwnerType:                ownerType,
		Phase:                    ownerPhaseAdmitted,
		PodName:                  "hub-stable-1",
		InstanceUID:              "hub-uid-1",
		InstanceEpoch:            11,
		AssignmentOrAllocationID: "hub-assignment-1",
		ReleaseTrack:             "stable",
		OperationID:              "11111111-1111-4111-8111-111111111111",
		LeaseDeadlineMs:          time.Now().Add(time.Minute).UnixMilli(),
	}
	if ownerType == ownerTypeBattle {
		view.PodName = "battle-stable-1"
		view.InstanceUID = "battle-uid-1"
		view.AssignmentOrAllocationID = "battle-allocation-1"
	}
	return view
}

func requireLoginWait(t *testing.T, res *LoginResult, err error, reason loginv1.ResumeWaitReason) {
	t.Helper()
	if err != nil {
		t.Fatalf("Login WAIT returned error: %v", err)
	}
	if res == nil {
		t.Fatal("Login WAIT returned nil result")
	}
	if res.PlayerID == 0 || res.SessionToken == "" || res.SessionExpMs <= time.Now().UnixMilli() {
		t.Fatalf("Login WAIT must preserve a usable session, got player=%d token_len=%d exp_ms=%d",
			res.PlayerID, len(res.SessionToken), res.SessionExpMs)
	}
	if res.Resume.Route != loginv1.ResumeRoute_RESUME_ROUTE_UNKNOWN ||
		res.Resume.EntryState != loginv1.ResumeEntryState_RESUME_ENTRY_STATE_WAIT ||
		res.Resume.WaitReason != reason ||
		res.Resume.RetryAfterMs != loginWaitRetryAfterMs {
		t.Fatalf("Login WAIT resume = %+v, want UNKNOWN/WAIT/%v/retry=%d",
			res.Resume, reason, loginWaitRetryAfterMs)
	}
	if res.HubDSAddr != "" || res.HubTicket != "" || res.BattleDSAddr != "" || res.BattleTicket != "" {
		t.Fatalf("Login WAIT must not leak DS endpoint/ticket: hub=%q hub_ticket_len=%d battle=%q battle_ticket_len=%d",
			res.HubDSAddr, len(res.HubTicket), res.BattleDSAddr, len(res.BattleTicket))
	}
}

func TestLogin_NoSelectedRoleReturnsRoleRequiredWithoutHubSideEffects(t *testing.T) {
	hub := &fakeHubAssigner{res: &data.HubAssignment{
		HubDSAddr: "10.0.0.9:7777", HubTicket: "must-not-be-used",
	}}
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: false}}
	uc := newTestUsecaseWithNotifier(t, hub, notifier)
	uc.roleRepo = &fakeRoleRepo{roleID: 0}

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res == nil || res.PlayerID != 42 || res.SessionToken == "" || res.SessionExpMs <= time.Now().UnixMilli() {
		t.Fatalf("ROLE_REQUIRED must preserve a usable session, got %+v", res)
	}
	if res.Resume.Route != loginv1.ResumeRoute_RESUME_ROUTE_HUB ||
		res.Resume.EntryState != loginv1.ResumeEntryState_RESUME_ENTRY_STATE_ROLE_REQUIRED {
		t.Fatalf("Resume = %+v, want HUB/ROLE_REQUIRED", res.Resume)
	}
	if res.HubDSAddr != "" || res.HubTicket != "" || hub.gotPlayerID != 0 || notifier.loginPendingN != 0 {
		t.Fatalf("ROLE_REQUIRED must not allocate/notify/leak ticket: result=%+v hub_player=%d pending=%d",
			res, hub.gotPlayerID, notifier.loginPendingN)
	}
}

func TestLogin_RoleAuthorityFailureReturnsWaitWithoutHubSideEffects(t *testing.T) {
	hub := &fakeHubAssigner{res: &data.HubAssignment{
		HubDSAddr: "10.0.0.9:7777", HubTicket: "must-not-be-used",
	}}
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: false}}
	uc := newTestUsecaseWithNotifier(t, hub, notifier)
	uc.roleRepo = &unavailableRoleRepo{err: errcode.New(errcode.ErrUnavailable, "role authority unavailable")}

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	requireLoginWait(t, res, err, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_ROLE_UNKNOWN)
	if hub.gotPlayerID != 0 || notifier.loginPendingN != 0 {
		t.Fatalf("ROLE_UNKNOWN must not allocate/notify: hub_player=%d pending=%d",
			hub.gotPlayerID, notifier.loginPendingN)
	}
}

func TestLogin_HubAssignerSuccess(t *testing.T) {
	hub := &fakeHubAssigner{res: &data.HubAssignment{
		HubDSAddr:  "10.0.0.9:7777",
		HubTicket:  "", // 见下:用真实签名替换以便 verifier 能解析 exp
		HubPodName: "pandora-hub-cn-2",
		ShardID:    2,
	}}
	uc := newTestUsecase(t, hub)

	// 用 uc.signer 真实签一张 hub 票据塞进 allocator 返回,模拟 hub_allocator 用共享 secret 签的票。
	tk, _, err := uc.signer.SignDSTicket(42, auth.DSTypeHub, 0, "jti-hub")
	if err != nil {
		t.Fatalf("sign hub ticket: %v", err)
	}
	hub.res.HubTicket = tk

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res.HubDSAddr != "10.0.0.9:7777" {
		t.Errorf("HubDSAddr = %q, want allocator addr", res.HubDSAddr)
	}
	if res.HubTicket != tk {
		t.Errorf("HubTicket not the allocator-signed ticket")
	}
	if res.HubTicketExpMs <= 0 {
		t.Errorf("HubTicketExpMs = %d, want >0 (parsed from ticket)", res.HubTicketExpMs)
	}
	if res.PlayerNo != testPlayerNo {
		t.Errorf("PlayerNo = %d, want %d", res.PlayerNo, testPlayerNo)
	}
	if hub.gotPlayerID != 42 || hub.gotRegion != "cn" || hub.gotTeamID != 0 {
		t.Errorf("AssignHub args = (%d,%q,%d), want (42,\"cn\",0)", hub.gotPlayerID, hub.gotRegion, hub.gotTeamID)
	}
}

func TestLogin_PlayerNoTimeoutDoesNotCancelParentLogin(t *testing.T) {
	uc := newTestUsecase(t, nil)
	repo := uc.repo.(*fakeAccountRepo)
	repo.playerNoFn = func(ctx context.Context) (uint64, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	}

	parent, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started := time.Now()
	res, err := uc.Login(parent, "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if parent.Err() != nil {
		t.Fatalf("展示查询超时不得取消父登录 ctx: %v", parent.Err())
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("展示查询未在短预算内降级: elapsed=%v", elapsed)
	}
	if res.PlayerNo != 0 {
		t.Fatalf("PlayerNo = %d, want 0 after timeout", res.PlayerNo)
	}
}

func TestLogin_HubAssignerNil_FallbackSelfSign(t *testing.T) {
	uc := newTestUsecase(t, nil)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res.HubDSAddr != "127.0.0.1:7777" {
		t.Errorf("HubDSAddr = %q, want static fallback addr", res.HubDSAddr)
	}
	// 自签票据应能被 verifier 验通过且是 hub 类型。
	claims, verr := uc.verifier.VerifyDSTicket(res.HubTicket)
	if verr != nil {
		t.Fatalf("self-signed hub ticket not verifiable: %v", verr)
	}
	if claims.DSType != string(auth.DSTypeHub) || claims.PlayerID() != 42 {
		t.Errorf("self-signed ticket claims = (%s, pid=%d), want (hub, 42)", claims.DSType, claims.PlayerID())
	}
}

func TestLogin_HubAssignerError_FallbackSelfSign(t *testing.T) {
	hub := &fakeHubAssigner{err: errors.New("hub_allocator down")}
	uc := newTestUsecase(t, hub)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login should fall back, got err: %v", err)
	}
	if res.HubDSAddr != "127.0.0.1:7777" {
		t.Errorf("HubDSAddr = %q, want static fallback addr on AssignHub error", res.HubDSAddr)
	}
	if _, verr := uc.verifier.VerifyDSTicket(res.HubTicket); verr != nil {
		t.Fatalf("fallback hub ticket not verifiable: %v", verr)
	}
}

// ---- cellroute 接线(全服扩容三层化)----

// singleCellRouter 构造一张把所有 logical_cell 都指向 (region, cell) 的路由器,
// 便于确定性断言登录返回的落点。
func singleCellRouter(t *testing.T, region, cell uint32) *cellroute.Router {
	t.Helper()
	entries, regionOfCell, err := cellroute.BuildBalancedEntries([]cellroute.CellSpec{{RegionID: region, CellID: cell}})
	if err != nil {
		t.Fatalf("BuildBalancedEntries: %v", err)
	}
	tbl, err := cellroute.NewStaticTable(entries, regionOfCell)
	if err != nil {
		t.Fatalf("NewStaticTable: %v", err)
	}
	r, err := cellroute.NewRouter(tbl)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return r
}

// TestLogin_CellRoute_ReturnsLocation 验证设了 Router 时,登录返回算出的 region/cell。
func TestLogin_CellRoute_ReturnsLocation(t *testing.T) {
	uc := newTestUsecase(t, nil)
	uc.SetCellRouter(singleCellRouter(t, 7, 77))

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res.RegionID != 7 || res.CellID != 77 {
		t.Errorf("login region/cell = (%d,%d), want (7,77)", res.RegionID, res.CellID)
	}
}

// TestLogin_CellRoute_NilRouterZero 验证未设 Router(单 Cell/dev)时,落点为 0,不阻断登录。
func TestLogin_CellRoute_NilRouterZero(t *testing.T) {
	uc := newTestUsecase(t, nil) // 不调 SetCellRouter

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res.RegionID != 0 || res.CellID != 0 {
		t.Errorf("nil router login region/cell = (%d,%d), want (0,0)", res.RegionID, res.CellID)
	}
}

// TestLogin_CellRoute_HubTicketBindsCell 验证设了 Router 时,自签 hub 票据把 region/cell 盖进
// JWT(scale-cellular-20m.md §3.3 防跨单元串号);DS 侧据此校验"票据 Cell == 本 DS Cell"。
func TestLogin_CellRoute_HubTicketBindsCell(t *testing.T) {
	uc := newTestUsecase(t, nil)
	uc.SetCellRouter(singleCellRouter(t, 7, 77))

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	claims, verr := uc.verifier.VerifyDSTicket(res.HubTicket)
	if verr != nil {
		t.Fatalf("hub ticket not verifiable: %v", verr)
	}
	if claims.RegionID != 7 || claims.CellID != 77 {
		t.Errorf("hub ticket region/cell = (%d,%d), want (7,77)", claims.RegionID, claims.CellID)
	}
}

// TestLogin_CellRoute_NilRouterHubTicketZeroCell 验证未设 Router 时,hub 票据 region/cell = 0
// (单 Cell / dev 语义),与历史票据兼容。
func TestLogin_CellRoute_NilRouterHubTicketZeroCell(t *testing.T) {
	uc := newTestUsecase(t, nil) // 不调 SetCellRouter

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	claims, verr := uc.verifier.VerifyDSTicket(res.HubTicket)
	if verr != nil {
		t.Fatalf("hub ticket not verifiable: %v", verr)
	}
	if claims.RegionID != 0 || claims.CellID != 0 {
		t.Errorf("nil router hub ticket region/cell = (%d,%d), want (0,0)", claims.RegionID, claims.CellID)
	}
}

// TestIssueDSTicket_CellRoute 验证 TicketUsecase 设了 Router 时,IssueDSTicket(battle 票据)
// 把 region/cell 盖进 JWT;VerifyDSTicket 原样透传出来(scale-cellular-20m.md §3.3)。
func TestIssueDSTicket_CellRoute(t *testing.T) {
	cfg := auth.Config{Secret: []byte(testSecret)}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	tu := NewTicketUsecase(signer, verifier, nil)
	tu.SetBattleTicketAuthorizer(&battleTicketAuthorizerFake{})
	tu.SetCellRouter(singleCellRouter(t, 5, 55))

	issued, err := tu.IssueDSTicket(context.Background(), 42, string(auth.DSTypeBattle), 9001, "")
	if err != nil {
		t.Fatalf("IssueDSTicket: %v", err)
	}
	claims, err := tu.VerifyDSTicket(context.Background(), issued.Ticket, "ds-pod-1")
	if err != nil {
		t.Fatalf("VerifyDSTicket: %v", err)
	}
	if claims.RegionID != 5 || claims.CellID != 55 {
		t.Errorf("battle ticket region/cell = (%d,%d), want (5,55)", claims.RegionID, claims.CellID)
	}
	if claims.MatchID != 9001 || claims.PlayerID != 42 {
		t.Errorf("battle ticket match/player = (%d,%d), want (9001,42)", claims.MatchID, claims.PlayerID)
	}
}

// TestIssueDSTicket_NilRouterZeroCell 验证 TicketUsecase 未设 Router 时,票据 region/cell = 0。
func TestIssueDSTicket_NilRouterZeroCell(t *testing.T) {
	cfg := auth.Config{Secret: []byte(testSecret)}
	signer, _ := auth.NewSigner(cfg)
	verifier, _ := auth.NewVerifier(cfg)
	tu := NewTicketUsecase(signer, verifier, nil) // 不调 SetCellRouter

	issued, err := tu.IssueDSTicket(context.Background(), 42, string(auth.DSTypeHub), 0, "")
	if err != nil {
		t.Fatalf("IssueDSTicket: %v", err)
	}
	claims, err := tu.VerifyDSTicket(context.Background(), issued.Ticket, "ds-pod-1")
	if err != nil {
		t.Fatalf("VerifyDSTicket: %v", err)
	}
	if claims.RegionID != 0 || claims.CellID != 0 {
		t.Errorf("nil router ticket region/cell = (%d,%d), want (0,0)", claims.RegionID, claims.CellID)
	}
}

// ---- dev_skip_password fakes / tests ----

// devFakeRepo 模拟 MySQL 行为:按 account 名查/建,验证免密模式下的懒注册稳定性。
type devFakeRepo struct {
	accounts   map[string]uint64 // account -> player_id(角色实体 ID)
	accountIDs map[string]uint64 // account -> account_id(账号身份;0 = 存量行尚未补铸)
	hashes     map[string]string // account -> bcrypt password_hash
	created    []string          // 记录被 CreateAccount 的账号(断言"只建一次")
}

func newDevFakeRepo() *devFakeRepo {
	return &devFakeRepo{
		accounts:   map[string]uint64{},
		accountIDs: map[string]uint64{},
		hashes:     map[string]string{},
	}
}

func (r *devFakeRepo) FindByAccount(_ context.Context, account string) (data.AccountIdentity, error) {
	if id, ok := r.accounts[account]; ok {
		return data.AccountIdentity{AccountID: r.accountIDs[account], PlayerID: id, PasswordHash: r.hashes[account]}, nil
	}
	return data.AccountIdentity{}, errcode.New(errcode.ErrLoginAccountNotFound, "account=%s not found", account)
}
func (r *devFakeRepo) FindByAccountID(_ context.Context, accountID uint64) (data.AccountIdentity, error) {
	for account, id := range r.accountIDs {
		if id == accountID {
			return data.AccountIdentity{AccountID: accountID, PlayerID: r.accounts[account], PasswordHash: r.hashes[account]}, nil
		}
	}
	return data.AccountIdentity{}, errcode.New(errcode.ErrLoginAccountNotFound, "account_id=%d not found", accountID)
}
func (r *devFakeRepo) CreateAccount(_ context.Context, accountID, playerID uint64, account, passwordHash string) error {
	if _, ok := r.accounts[account]; ok {
		return errcode.New(errcode.ErrAlreadyExists, "account=%s exists", account)
	}
	r.accounts[account] = playerID
	r.accountIDs[account] = accountID
	r.hashes[account] = passwordHash
	r.created = append(r.created, account)
	return nil
}
func (r *devFakeRepo) BackfillAccountID(_ context.Context, playerID, candidate uint64) (uint64, error) {
	for account, id := range r.accounts {
		if id == playerID {
			if existing := r.accountIDs[account]; existing != 0 {
				return existing, nil
			}
			r.accountIDs[account] = candidate
			return candidate, nil
		}
	}
	return 0, errcode.New(errcode.ErrLoginAccountNotFound, "player_id=%d not found", playerID)
}
func (r *devFakeRepo) CheckBanned(_ context.Context, _ uint64, _ string) (bool, error) {
	return false, nil
}
func (r *devFakeRepo) TouchDevice(_ context.Context, _ uint64, _ string) error { return nil }
func (r *devFakeRepo) GetPlayerNo(_ context.Context, _ uint64) (uint64, error) {
	return 0, nil // 展示字段:单测不关心编号,0=未分配即可
}

func newDevSkipUsecase(t *testing.T, repo data.AccountRepo) *LoginUsecase {
	t.Helper()
	cfg := auth.Config{Secret: []byte(testSecret)}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	sf := snowflake.NewNode(1)
	// hubAssigner=nil 走自签回退;devSkipPassword=true。
	return NewLoginUsecase(repo, newFakeSessionRepo(), nil, nil, nil, sf, "127.0.0.1:7777", "cn", signer, verifier, nil, true, false, nil, false)
}

// newDevAutoRegUsecase 构造 devAutoRegister=true 、 devSkipPassword=false 的用例。
func newDevAutoRegUsecase(t *testing.T, repo data.AccountRepo) *LoginUsecase {
	t.Helper()
	cfg := auth.Config{Secret: []byte(testSecret)}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	sf := snowflake.NewNode(1)
	return NewLoginUsecase(repo, newFakeSessionRepo(), nil, nil, nil, sf, "127.0.0.1:7777", "cn", signer, verifier, nil, false, true, nil, false)
}

// newSelectRoleUsecase 构造选角测试用例(hubAssigner=nil 走自签回退;roleRepo=nil 跳过落库)。
func newSelectRoleUsecase(t *testing.T, allowedRoleIDs []uint32, devAllowAnyRole bool) *LoginUsecase {
	t.Helper()
	cfg := auth.Config{Secret: []byte(testSecret)}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	sf := snowflake.NewNode(1)
	repo := &fakeAccountRepo{playerID: 42, passwordHash: mustBcrypt(t, "pw")}
	return NewLoginUsecase(repo, newFakeSessionRepo(), nil, nil, nil, sf, "127.0.0.1:7777", "cn", signer, verifier, nil, false, false, allowedRoleIDs, devAllowAnyRole)
}

// TestSelectRole_EmptyWhitelistFailClosed 验证:白名单为空且未开 dev_allow_any_role 时,
// SelectRole 一律拒绝(fail-closed,防改包客户端签任意 role_id 进 hub 票据)。
func TestSelectRole_EmptyWhitelistFailClosed(t *testing.T) {
	uc := newSelectRoleUsecase(t, nil, false)
	_, _, _, err := uc.SelectRole(context.Background(), 42, 7, "")
	if errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("empty whitelist should fail-closed with ErrInvalidState, got %v", err)
	}
}

// TestSelectRole_EmptyWhitelistDevAllowAnyRole 验证:dev_allow_any_role=true 时空白名单
// 只校验非 0(dev 宽松语义,回退自签票据路径)。
func TestSelectRole_EmptyWhitelistDevAllowAnyRole(t *testing.T) {
	uc := newSelectRoleUsecase(t, nil, true)
	addr, ticket, _, err := uc.SelectRole(context.Background(), 42, 7, "")
	if err != nil {
		t.Fatalf("dev_allow_any_role should accept non-zero role_id, got %v", err)
	}
	if addr == "" || ticket == "" {
		t.Fatalf("want fallback addr+ticket, got addr=%q ticket=%q", addr, ticket)
	}
	if _, _, _, err := uc.SelectRole(context.Background(), 42, 0, ""); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("role_id=0 should be ErrInvalidArg, got %v", err)
	}
}

// TestSelectRole_Whitelist 验证:非空白名单严格校验(命中放行 / 未命中拒绝,
// 与 dev_allow_any_role 无关——开关只对空白名单生效)。
func TestSelectRole_Whitelist(t *testing.T) {
	uc := newSelectRoleUsecase(t, []uint32{1, 2}, true)
	if _, _, _, err := uc.SelectRole(context.Background(), 42, 2, ""); err != nil {
		t.Fatalf("whitelisted role should pass, got %v", err)
	}
	if _, _, _, err := uc.SelectRole(context.Background(), 42, 9, ""); errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("non-whitelisted role should be ErrInvalidArg, got %v", err)
	}
}

// TestLogin_DevSkipPassword_AutoProvision 验证:免密模式下任意新账号自动建号,
// 同一账号名两次登录拿到同一个稳定 player_id,且账号只被创建一次。
func TestLogin_DevSkipPassword_AutoProvision(t *testing.T) {
	repo := newDevFakeRepo()
	uc := newDevSkipUsecase(t, repo)

	res1, err := uc.Login(context.Background(), "anybody", "whatever", "dev-1", false)
	if err != nil {
		t.Fatalf("first login: %v", err)
	}
	if res1.PlayerID == 0 {
		t.Fatalf("PlayerID = 0, want auto-provisioned id")
	}

	res2, err := uc.Login(context.Background(), "anybody", "another-pw", "dev-2", false)
	if err != nil {
		t.Fatalf("second login: %v", err)
	}
	if res2.PlayerID != res1.PlayerID {
		t.Errorf("PlayerID not stable: first=%d second=%d", res1.PlayerID, res2.PlayerID)
	}
	if len(repo.created) != 1 {
		t.Errorf("account created %d times, want exactly 1", len(repo.created))
	}
}

// TestLogin_DevSkipPassword_ExistingAccountWrongPassword 验证:已存在账号在免密模式下
// 任意密码都放行(不做 bcrypt 校验)。
func TestLogin_DevSkipPassword_ExistingAccountWrongPassword(t *testing.T) {
	repo := newDevFakeRepo()
	repo.accounts["known"] = 777
	uc := newDevSkipUsecase(t, repo)

	res, err := uc.Login(context.Background(), "known", "definitely-wrong", "dev-1", false)
	if err != nil {
		t.Fatalf("login with wrong password should pass in skip mode: %v", err)
	}
	if res.PlayerID != 777 {
		t.Errorf("PlayerID = %d, want existing 777", res.PlayerID)
	}
	if len(repo.created) != 0 {
		t.Errorf("existing account should not be re-created, got %d creates", len(repo.created))
	}
}

// TestLogin_DevAutoRegister_FirstLoginRegisters 验证假注册(不免密)语义:
//   - 首登未知账号 → 自动注册,存本次密码;
//   - 同账号同密码再登 → 走正常 bcrypt 校验通过,player_id 稳定;
//   - 同账号错密码 → ErrLoginPasswordMismatch(密码仍生效)。
func TestLogin_DevAutoRegister_FirstLoginRegisters(t *testing.T) {
	repo := newDevFakeRepo()
	uc := newDevAutoRegUsecase(t, repo)

	res1, err := uc.Login(context.Background(), "newbie", "pw1", "dev-1", false)
	if err != nil {
		t.Fatalf("first login (register): %v", err)
	}
	if res1.PlayerID == 0 {
		t.Fatalf("PlayerID = 0, want auto-registered id")
	}
	if len(repo.created) != 1 {
		t.Fatalf("account created %d times, want exactly 1", len(repo.created))
	}

	// 同密码复登 → bcrypt 校验通过,同一 player_id。
	res2, err := uc.Login(context.Background(), "newbie", "pw1", "dev-2", false)
	if err != nil {
		t.Fatalf("second login (verify): %v", err)
	}
	if res2.PlayerID != res1.PlayerID {
		t.Errorf("PlayerID not stable: first=%d second=%d", res1.PlayerID, res2.PlayerID)
	}

	// 错密码 → 仍拦(假注册不等于免密)。
	if _, err := uc.Login(context.Background(), "newbie", "wrong-pw", "dev-3", false); err == nil {
		t.Errorf("wrong password should be rejected when only auto_register is on")
	} else if errcode.As(err) != errcode.ErrLoginPasswordMismatch {
		t.Errorf("err code = %d, want ErrLoginPasswordMismatch(%d)", errcode.As(err), errcode.ErrLoginPasswordMismatch)
	}
}

// ---- 断线重连(docs/design/battle-reconnect.md)----

// TestLogin_BattleReconnect_ReturnsBattleAndSkipsHub 验证:玩家在 battle DS 中掉线重登时,
// Login 直接下发 battle DS 直连信息(battle_ds_addr/battle_ticket/match_id),且:
//   - 跳过 hub 分配(hub 字段为空);
//   - 跳过 NotifyLoginPending(不顶掉 BATTLE 位置);
//   - battle 票据可被 verifier 验通过,类型=battle、绑定正确 player_id/match_id。
func TestLogin_BattleReconnect_ReturnsBattleAndSkipsHub(t *testing.T) {
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: true, MatchID: 9001, BattleAddr: "10.1.2.3:7000"}}
	// hub 传 nil:命中重连时根本不该走 hub 分配,自签回退也不该发生(battle 分支提前 return)。
	uc := newTestUsecaseWithNotifier(t, nil, notifier)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res.BattleDSAddr != "10.1.2.3:7000" {
		t.Errorf("BattleDSAddr = %q, want battle ds addr", res.BattleDSAddr)
	}
	if res.MatchID != 9001 {
		t.Errorf("MatchID = %d, want 9001", res.MatchID)
	}
	if res.PlayerNo != testPlayerNo {
		t.Errorf("PlayerNo = %d, want %d", res.PlayerNo, testPlayerNo)
	}
	if res.HubDSAddr != "" || res.HubTicket != "" {
		t.Errorf("battle reconnect should skip hub, got addr=%q ticket_len=%d", res.HubDSAddr, len(res.HubTicket))
	}
	if notifier.loginPendingN != 0 {
		t.Errorf("battle reconnect should skip NotifyLoginPending, got %d calls", notifier.loginPendingN)
	}
	claims, verr := uc.verifier.VerifyDSTicket(res.BattleTicket)
	if verr != nil {
		t.Fatalf("battle reconnect ticket not verifiable: %v", verr)
	}
	if claims.DSType != string(auth.DSTypeBattle) || claims.PlayerID() != 42 || claims.MatchID != 9001 {
		t.Errorf("battle ticket claims = (ds=%s pid=%d match=%d), want (battle,42,9001)",
			claims.DSType, claims.PlayerID(), claims.MatchID)
	}
}

func TestLogin_BattleReconnect_UsesAuthoritativeProjectionAddress(t *testing.T) {
	notifier := &fakeNotifier{bl: data.BattleLocation{
		InBattle: true, MatchID: 9001, BattleAddr: "10.1.2.3:7000", // locator 旧实例 A
	}}
	uc := newTestUsecaseWithNotifier(t, nil, notifier)
	ticketUC := NewTicketUsecase(uc.signer, uc.verifier, nil)
	ticketUC.SetBattleTicketAuthorizer(&loginBattleAuthorizerFake{target: data.BattleTicketTarget{
		DSAddr: "10.9.8.7:7100", PodName: "battle-new", InstanceUID: "uid-new", InstanceEpoch: 2,
	}})
	uc.SetBattleTicketIssuer(ticketUC)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res.BattleDSAddr != "10.9.8.7:7100" {
		t.Fatalf("BattleDSAddr=%q, want Redis projection B instead of stale locator A", res.BattleDSAddr)
	}
	if res.HubDSAddr != "" || notifier.loginPendingN != 0 {
		t.Fatalf("authoritative reconnect unexpectedly entered hub: hub=%q pending=%d",
			res.HubDSAddr, notifier.loginPendingN)
	}
}

func TestLogin_BattleReconnect_EmptyAuthoritativeAddressDoesNotAssignHub(t *testing.T) {
	notifier := &fakeNotifier{bl: data.BattleLocation{
		InBattle: true, MatchID: 9001, BattleAddr: "10.1.2.3:7000",
	}}
	hub := &fakeHubAssigner{res: &data.HubAssignment{HubDSAddr: "10.0.0.9:7777", HubTicket: "must-not-be-used"}}
	uc := newTestUsecaseWithNotifier(t, hub, notifier)
	ticketUC := NewTicketUsecase(uc.signer, uc.verifier, nil)
	ticketUC.SetBattleTicketAuthorizer(&loginBattleAuthorizerFake{returnEmpty: true})
	uc.SetBattleTicketIssuer(ticketUC)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	requireLoginWait(t, res, err, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN)
	if hub.gotPlayerID != 0 || notifier.loginPendingN != 0 {
		t.Fatalf("empty target mutated hub/login-pending: hub_player=%d pending=%d",
			hub.gotPlayerID, notifier.loginPendingN)
	}
}

func TestLogin_BattleReconnect_RosterAuthorityFailureDoesNotAssignHub(t *testing.T) {
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: true, MatchID: 9001, BattleAddr: "10.1.2.3:7000"}}
	hub := &fakeHubAssigner{res: &data.HubAssignment{HubDSAddr: "10.0.0.9:7777", HubTicket: "must-not-be-used"}}
	uc := newTestUsecaseWithNotifier(t, hub, notifier)
	ticketUC := NewTicketUsecase(uc.signer, uc.verifier, nil)
	ticketUC.SetBattleTicketAuthorizer(&loginBattleAuthorizerFake{
		err: errcode.New(errcode.ErrPermissionDeny, "player not in authoritative roster"),
	})
	uc.SetBattleTicketIssuer(ticketUC)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	requireLoginWait(t, res, err, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN)
	if hub.gotPlayerID != 0 {
		t.Fatalf("roster rejection called AssignHub for player %d", hub.gotPlayerID)
	}
	if notifier.loginPendingN != 0 {
		t.Fatalf("roster rejection wrote LOGIN_PENDING %d times", notifier.loginPendingN)
	}
}

func TestLogin_BattleReconnect_MissingIssuerDoesNotAssignHub(t *testing.T) {
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: true, MatchID: 9001, BattleAddr: "10.1.2.3:7000"}}
	hub := &fakeHubAssigner{res: &data.HubAssignment{HubDSAddr: "10.0.0.9:7777", HubTicket: "must-not-be-used"}}
	uc := newTestUsecaseWithNotifier(t, hub, notifier)
	uc.SetBattleTicketIssuer(nil)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	requireLoginWait(t, res, err, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN)
	if hub.gotPlayerID != 0 || notifier.loginPendingN != 0 {
		t.Fatalf("missing issuer mutated hub/login-pending: hub_player=%d pending=%d",
			hub.gotPlayerID, notifier.loginPendingN)
	}
}

// fakeMatchResolver 实现 data.MatchContextResolver(matchmaker 耐久权威兜底测试用)。
type fakeMatchResolver struct {
	out   data.PlayerMatchAuthority
	err   error
	calls int
}

func (f *fakeMatchResolver) ResolvePlayerMatchContext(_ context.Context, _ uint64) (data.PlayerMatchAuthority, error) {
	f.calls++
	return f.out, f.err
}

// TestLogin_BattleReconnect_RecoversReadyMatchWhenPresenceMissing 验证 P0-2/P0-4 修复:
// locator presence 蒸发(TTL 过期 / READY↔投影窗口)但 matchmaker 耐久权威显示 ACTIVE+READY
// 时,登录仍把玩家路由回原对局,绝不误进 Hub。
func TestLogin_BattleReconnect_RecoversReadyMatchWhenPresenceMissing(t *testing.T) {
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: false}} // 租约已蒸发
	hub := &fakeHubAssigner{res: &data.HubAssignment{HubDSAddr: "10.0.0.9:7777", HubTicket: "must-not-be-used"}}
	uc := newTestUsecaseWithNotifier(t, hub, notifier)
	resolver := &fakeMatchResolver{out: data.PlayerMatchAuthority{
		State:   matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE,
		Stage:   matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_READY,
		MatchID: 9001,
	}}
	uc.SetMatchContextResolver(resolver)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res.MatchID != 9001 || res.BattleDSAddr == "" {
		t.Fatalf("durable READY match must route back to battle, got match=%d addr=%q", res.MatchID, res.BattleDSAddr)
	}
	if res.HubDSAddr != "" || notifier.loginPendingN != 0 {
		t.Fatalf("presence-miss recovery must not enter hub: hub=%q pending=%d", res.HubDSAddr, notifier.loginPendingN)
	}
	if resolver.calls == 0 {
		t.Fatalf("matchmaker durable authority was never consulted")
	}
}

// TestLogin_BattleReconnect_QueuedMatchStillGoesHub 验证:排队/确认/分配中的玩家本就
// 该在 Hub 等 READY 推送,matchmaker 权威兜底不得把他们锁在门外。
func TestLogin_BattleReconnect_QueuedMatchStillGoesHub(t *testing.T) {
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: false}}
	uc := newTestUsecaseWithNotifier(t, nil, notifier)
	uc.SetMatchContextResolver(&fakeMatchResolver{out: data.PlayerMatchAuthority{
		State: matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE,
		Stage: matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_QUEUED,
	}})

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res.BattleDSAddr != "" || res.HubDSAddr == "" {
		t.Fatalf("queued player must go hub, got battle=%q hub=%q", res.BattleDSAddr, res.HubDSAddr)
	}
}

// TestLogin_BattleReconnect_NotInBattleFallsToHub 验证:玩家不在战斗中时,走正常 hub 流程,
// battle 字段为空,且 NotifyLoginPending 被调用。
func TestLogin_BattleReconnect_NotInBattleFallsToHub(t *testing.T) {
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: false}}
	uc := newTestUsecaseWithNotifier(t, nil, notifier) // hub=nil → 自签回退

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res.BattleDSAddr != "" || res.MatchID != 0 {
		t.Errorf("not-in-battle should not set battle fields, got addr=%q match=%d", res.BattleDSAddr, res.MatchID)
	}
	if res.HubDSAddr == "" || res.HubTicket == "" {
		t.Errorf("not-in-battle should go hub, got addr=%q ticket_len=%d", res.HubDSAddr, len(res.HubTicket))
	}
	if notifier.loginPendingN != 1 {
		t.Errorf("normal login should NotifyLoginPending once, got %d", notifier.loginPendingN)
	}
}

// TestLogin_BattleReconnect_QueryErrorFallsToHub 验证:locator 查询失败(弱依赖)不阻断登录,
// 降级走正常 hub 流程。
func TestLogin_BattleReconnect_QueryErrorFallsToHub(t *testing.T) {
	notifier := &fakeNotifier{blErr: errcode.New(errcode.ErrInternal, "locator down")}
	uc := newTestUsecaseWithNotifier(t, nil, notifier)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login should not fail when locator query errors: %v", err)
	}
	if res.BattleDSAddr != "" {
		t.Errorf("query error should not set battle fields, got addr=%q", res.BattleDSAddr)
	}
	if res.HubDSAddr == "" {
		t.Errorf("query error should fall back to hub, got empty hub addr")
	}
}

func TestLogin_B1RequiresConfiguredLocatorBeforeHubAssignment(t *testing.T) {
	hub := &fakeHubAssigner{}
	uc := newTestUsecaseWithNotifier(t, hub, nil)
	uc.SetRequireHubAssignmentBinding(true)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	requireLoginWait(t, res, err, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN)
	if hub.gotPlayerID != 0 {
		t.Fatalf("Hub allocator called before locator proof, player=%d", hub.gotPlayerID)
	}
}

func TestLogin_B1LocatorQueryFailureDoesNotAssignHub(t *testing.T) {
	notifier := &fakeNotifier{blErr: errcode.New(errcode.ErrInternal, "locator down")}
	hub := &fakeHubAssigner{}
	uc := newTestUsecaseWithNotifier(t, hub, notifier)
	uc.SetRequireHubAssignmentBinding(true)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	requireLoginWait(t, res, err, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN)
	if notifier.getN != battleLocationQueryRetries {
		t.Fatalf("locator query count=%d, want %d", notifier.getN, battleLocationQueryRetries)
	}
	if hub.gotPlayerID != 0 || notifier.loginPendingN != 0 {
		t.Fatalf("failed locator proof mutated hub/pending: hub_player=%d pending=%d",
			hub.gotPlayerID, notifier.loginPendingN)
	}
}

func TestLogin_B1NotifyLoginPendingFailureDoesNotDeliverHubTicket(t *testing.T) {
	keys := newHubV2TestKeys(t)
	ticket, _ := signHubV2ForResolve(t, keys, "", nil)
	hub := &fakeHubAssigner{res: &data.HubAssignment{
		HubDSAddr: "10.0.0.9:7777", HubTicket: ticket, HubPodName: "hub-stable-1", ShardID: 7,
	}}
	notifier := &fakeNotifier{
		bl:        data.BattleLocation{InBattle: false},
		notifyErr: errcode.New(errcode.ErrInternal, "locator write failed"),
	}
	uc := newTestUsecaseWithNotifier(t, hub, notifier)
	uc.v2Verifier = keys.verifier
	uc.SetRequireHubAssignmentBinding(true)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	requireLoginWait(t, res, err, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN)
	if hub.gotPlayerID != 0 || notifier.loginPendingN != 1 {
		t.Fatalf("hub_player=%d pending_calls=%d, want 0/1", hub.gotPlayerID, notifier.loginPendingN)
	}
}

func TestLogin_B1LocatorPendingSucceedsBeforeHubAssignment(t *testing.T) {
	keys := newHubV2TestKeys(t)
	ticket, _ := signHubV2ForResolve(t, keys, "", nil)
	hub := &fakeHubAssigner{res: &data.HubAssignment{
		HubDSAddr: "10.0.0.9:7777", HubTicket: ticket, HubPodName: "hub-stable-1", ShardID: 7,
	}}
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: false}}
	uc := newTestUsecaseWithNotifier(t, hub, notifier)
	uc.v2Verifier = keys.verifier
	uc.SetRequireHubAssignmentBinding(true)
	owner := stableTestOwnerPlacement(ownerTypeHub)
	owner.InstanceUID = "uid-hub-stable-1"
	owner.InstanceEpoch = 7
	owner.AssignmentOrAllocationID = "assignment-42-v7"
	uc.SetOwnerPlacementQuerier(&fakeOwnerPlacementQuerier{view: owner})

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil || res == nil || res.HubTicket != ticket {
		t.Fatalf("result=%+v ticket_match=%v err=%v", res, res != nil && res.HubTicket == ticket, err)
	}
	if hub.gotPlayerID != 42 || notifier.loginPendingN != 1 {
		t.Fatalf("hub_player=%d pending_calls=%d, want 42/1", hub.gotPlayerID, notifier.loginPendingN)
	}
}

// allocator 票据与 owner Resume 各自合法但 target 不同，也不得出现在同一 LoginResponse。
// 这是 2026-08-17 事故的原样形状：ticket=B、owner=A、物理 pod 甚至可以相同。
func TestLogin_WithholdsHubTicketWhenOwnerResumeTargetDiffers(t *testing.T) {
	keys := newHubV2TestKeys(t)
	ticket, _ := signHubV2ForResolve(t, keys, "", nil)
	hub := &fakeHubAssigner{res: &data.HubAssignment{
		HubDSAddr: "10.0.0.9:7777", HubTicket: ticket, HubPodName: "hub-stable-1", ShardID: 7,
	}}
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: false}}
	uc := newTestUsecaseWithNotifier(t, hub, notifier) // 默认 owner=hub-assignment-1，与票中 v7 不同。
	uc.v2Verifier = keys.verifier
	uc.SetRequireHubAssignmentBinding(true)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	requireLoginWait(t, res, err, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN)
	if hub.gotPlayerID != 42 || notifier.loginPendingN != 1 {
		t.Fatalf("assignment still executes once before exact post-check: hub_player=%d pending=%d",
			hub.gotPlayerID, notifier.loginPendingN)
	}
}

func TestLogin_WithholdsHubTicketWhenOwnerResumeTrackDiffers(t *testing.T) {
	keys := newHubV2TestKeys(t)
	ticket, _ := signHubV2ForResolve(t, keys, "", nil) // v2 ticket release_track=stable
	hub := &fakeHubAssigner{res: &data.HubAssignment{
		HubDSAddr: "10.0.0.9:7777", HubTicket: ticket, HubPodName: "hub-stable-1", ShardID: 7,
	}}
	uc := newTestUsecaseWithNotifier(t, hub, &fakeNotifier{bl: data.BattleLocation{InBattle: false}})
	uc.v2Verifier = keys.verifier
	uc.SetRequireHubAssignmentBinding(true)
	owner := stableTestOwnerPlacement(ownerTypeHub)
	owner.InstanceUID = "uid-hub-stable-1"
	owner.InstanceEpoch = 7
	owner.AssignmentOrAllocationID = "assignment-42-v7"
	owner.ReleaseTrack = auth.ReleaseTrackCanary
	uc.SetOwnerPlacementQuerier(&fakeOwnerPlacementQuerier{view: owner})

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	requireLoginWait(t, res, err, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN)
}

func TestLogin_LegacyNotifyLoginPendingFailureRemainsBestEffort(t *testing.T) {
	notifier := &fakeNotifier{
		bl:        data.BattleLocation{InBattle: false},
		notifyErr: errcode.New(errcode.ErrInternal, "locator write failed"),
	}
	uc := newTestUsecaseWithNotifier(t, nil, notifier)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil || res == nil || res.HubTicket == "" {
		t.Fatalf("legacy weak dependency changed: result=%+v err=%v", res, err)
	}
	if notifier.loginPendingN != 1 {
		t.Fatalf("NotifyLoginPending calls=%d, want 1", notifier.loginPendingN)
	}
}

// TestLogin_BattleReconnect_TransientErrorRetriesThenReconnects 验证:locator 瞬时抖动(前几次
// 查询失败)时,有界重试能把可恢复失败救回来——只要重试内拿到 InBattle,仍然跳去 battle 重连,
// 不会因为"第一次没查着"就把战斗中的玩家误送进 hub(docs/design/battle-reconnect.md §2.3)。
func TestLogin_BattleReconnect_TransientErrorRetriesThenReconnects(t *testing.T) {
	notifier := &fakeNotifier{
		bl:        data.BattleLocation{InBattle: true, MatchID: 9001, BattleAddr: "10.1.2.3:7000"},
		failFirst: 2, // 前两次查询抖动失败,第三次成功返回 InBattle
	}
	uc := newTestUsecaseWithNotifier(t, nil, notifier)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if res.BattleDSAddr != "10.1.2.3:7000" || res.MatchID != 9001 {
		t.Errorf("transient blip should still reconnect to battle, got addr=%q match=%d", res.BattleDSAddr, res.MatchID)
	}
	if res.HubDSAddr != "" {
		t.Errorf("recovered reconnect should skip hub, got hub addr=%q", res.HubDSAddr)
	}
	if notifier.getN != 3 {
		t.Errorf("GetBattleLocation called %d times, want 3 (2 fail + 1 success)", notifier.getN)
	}
	if notifier.loginPendingN != 0 {
		t.Errorf("battle reconnect should skip NotifyLoginPending, got %d", notifier.loginPendingN)
	}
}

// ---------------------------------------------------------------------------
// canonical game_mode 进 Resume(2026-07-16 修复:客户端 DS 恢复协调器
// "rejecting unknown authoritative game_mode ''" 死循环)。
// game_mode 唯一权威 = matchmaker 持久记录(ResolvePlayerMatchContext),
// login 绝不按 PVE/PVP 硬编码猜测。
// ---------------------------------------------------------------------------

// presenceHitBattle 构造 locator presence 命中 BATTLE 的 fakeNotifier(真实 locator
// 命中路径恒有 PresenceState=BATTLE,见 data/locator_client.go)。
func presenceHitBattle(matchID uint64) *fakeNotifier {
	return &fakeNotifier{bl: data.BattleLocation{
		InBattle:      true,
		MatchID:       matchID,
		BattleAddr:    "10.1.2.3:7000",
		PresenceState: locatorv1.LocationState_LOCATION_STATE_BATTLE,
	}}
}

// TestLogin_BattleReconnect_ResumeCarriesCanonicalGameMode:presence 命中重连时,
// Resume 必须携带 matchmaker 权威 game_mode——PVE(pve_coop)与 PVP(5v5_ranked)
// 同一条读取链,无任何模式特判。stage=RUNNING(BATTLE 租约活着=玩家已在 DS 上)。
func TestLogin_BattleReconnect_ResumeCarriesCanonicalGameMode(t *testing.T) {
	for _, mode := range []string{"pve_coop", "5v5_ranked"} {
		t.Run(mode, func(t *testing.T) {
			notifier := presenceHitBattle(9001)
			uc := newTestUsecaseWithNotifier(t, nil, notifier)
			// 玩家确实在 battle:owner 权威必须同口径说 BATTLE。locator presence 只是投影,
			// 单凭它放行是 §9.22 禁止的 fail-open,服务端已改为 owner 未确认即 WAIT。
			uc.SetOwnerPlacementQuerier(&fakeOwnerPlacementQuerier{view: stableTestOwnerPlacement(ownerTypeBattle)})
			resolver := &fakeMatchResolver{out: data.PlayerMatchAuthority{
				State:    matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE,
				Stage:    matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_READY,
				MatchID:  9001,
				GameMode: mode,
			}}
			uc.SetMatchContextResolver(resolver)

			res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
			if err != nil {
				t.Fatalf("Login: %v", err)
			}
			r := res.Resume
			if r.Route != loginv1.ResumeRoute_RESUME_ROUTE_BATTLE || r.MatchID != 9001 {
				t.Fatalf("Resume route/match = %v/%d, want BATTLE/9001", r.Route, r.MatchID)
			}
			if r.GameMode != mode {
				t.Fatalf("Resume.GameMode = %q, want canonical %q (this is the client dead-loop bug)", r.GameMode, mode)
			}
			if r.MatchStage != loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_RUNNING {
				t.Fatalf("Resume.MatchStage = %v, want RUNNING (live BATTLE lease)", r.MatchStage)
			}
			if resolver.calls != 1 {
				t.Fatalf("resolver calls = %d, want exactly 1", resolver.calls)
			}
		})
	}
}

// TestLogin_BattleReconnect_B1FailClosedWhenGameModeUnavailable:B1 下 game_mode
// 拿不到(权威查询失败 / claim 漂移 / 记录缺字段)→ 可重试 Unavailable,且零副作用:
// 不签票、不派 Hub、不写 LOGIN_PENDING。缺 game_mode 的 BATTLE resume 就是交付 bug。
func TestLogin_BattleReconnect_B1FailClosedWhenGameModeUnavailable(t *testing.T) {
	cases := []struct {
		name     string
		resolver *fakeMatchResolver
	}{
		{"resolver_error", &fakeMatchResolver{err: errcode.New(errcode.ErrInternal, "matchmaker down")}},
		{"active_empty_game_mode", &fakeMatchResolver{out: data.PlayerMatchAuthority{
			State:   matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE,
			Stage:   matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_READY,
			MatchID: 9001,
		}}},
		{"claim_match_id_drift", &fakeMatchResolver{out: data.PlayerMatchAuthority{
			State:    matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE,
			Stage:    matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_READY,
			MatchID:  8888, // ≠ locator 9001:漂移不猜
			GameMode: "pve_coop",
		}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			notifier := presenceHitBattle(9001)
			hub := &fakeHubAssigner{}
			uc := newTestUsecaseWithNotifier(t, hub, notifier)
			authorizer := &loginBattleAuthorizerFake{}
			ticketUC := NewTicketUsecase(uc.signer, uc.verifier, nil)
			ticketUC.SetBattleTicketAuthorizer(authorizer)
			uc.SetBattleTicketIssuer(ticketUC)
			uc.SetMatchContextResolver(tc.resolver)
			uc.SetRequireHubAssignmentBinding(true)

			res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
			requireLoginWait(t, res, err, loginv1.ResumeWaitReason_RESUME_WAIT_REASON_OWNER_UNKNOWN)
			if authorizer.authorizeCalls != 0 {
				t.Fatalf("ticket issued %d times on rejected resume path, want 0 (no side effects)", authorizer.authorizeCalls)
			}
			if hub.gotPlayerID != 0 || notifier.loginPendingN != 0 {
				t.Fatalf("fail-closed path mutated hub/pending: hub_player=%d pending=%d",
					hub.gotPlayerID, notifier.loginPendingN)
			}
		})
	}
}

// TestLogin_BattleReconnect_LocalDegradesWithoutResolver:local/off 无 resolver
// (dev 裸跑)保留历史弱降级——照常重连,game_mode 空 + 告警,不阻断登录。
func TestLogin_BattleReconnect_LocalDegradesWithoutResolver(t *testing.T) {
	notifier := presenceHitBattle(9001)
	uc := newTestUsecaseWithNotifier(t, nil, notifier) // resolver 未配,B1 off
	// owner 是归属唯一权威:presence 不能单独授权回原局(§9.22)。
	uc.SetOwnerPlacementQuerier(&fakeOwnerPlacementQuerier{view: stableTestOwnerPlacement(ownerTypeBattle)})

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	r := res.Resume
	if r.Route != loginv1.ResumeRoute_RESUME_ROUTE_BATTLE || r.MatchID != 9001 ||
		r.MatchStage != loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_RUNNING {
		t.Fatalf("local degrade must still reconnect: %+v", r)
	}
	if r.GameMode != "" {
		t.Fatalf("no resolver configured yet GameMode=%q; where did it come from?", r.GameMode)
	}
}

// TestLogin_BattleReconnect_PresenceMissReadyClaimCarriesGameMode:locator 投影蒸发、
// 由 READY claim 合成的重连,Resume 复用同一次权威查询(恰好 1 次 RPC)携带
// game_mode,stage 按权威显式映射为 READY(而非谎报 RUNNING)。
func TestLogin_BattleReconnect_PresenceMissReadyClaimCarriesGameMode(t *testing.T) {
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: false}} // 租约已蒸发
	uc := newTestUsecaseWithNotifier(t, nil, notifier)
	// presence 已蒸发但 owner 仍持 BATTLE 归属(由 READY claim 合成的路径)。
	uc.SetOwnerPlacementQuerier(&fakeOwnerPlacementQuerier{view: stableTestOwnerPlacement(ownerTypeBattle)})
	resolver := &fakeMatchResolver{out: data.PlayerMatchAuthority{
		State:        matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE,
		Stage:        matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_READY,
		MatchID:      9001,
		BattleDSAddr: "10.9.9.9:7000",
		GameMode:     "pve_coop",
	}}
	uc.SetMatchContextResolver(resolver)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	r := res.Resume
	if r.Route != loginv1.ResumeRoute_RESUME_ROUTE_BATTLE || r.MatchID != 9001 || r.GameMode != "pve_coop" {
		t.Fatalf("synthesized reconnect resume = %+v, want BATTLE/9001/pve_coop", r)
	}
	if r.MatchStage != loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_READY {
		t.Fatalf("Resume.MatchStage = %v, want READY (authority stage, no RUNNING lie)", r.MatchStage)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want exactly 1 (reuse the authority fetched by resolveBattleAuthority)", resolver.calls)
	}
}

// signTestSession 给 GetResumeContext 测试签一个玩家 42 的有效 session token,
// 并关掉 session 现行性门(sessions=nil → requireCurrentSession 直通,聚焦路由断言)。
func signTestSession(t *testing.T, uc *LoginUsecase) string {
	t.Helper()
	uc.sessions = nil
	tok, _, err := uc.signer.SignSession(42, "jti-resume-test")
	if err != nil {
		t.Fatalf("SignSession: %v", err)
	}
	return tok
}

// TestGetResumeContext_BattleRouteCarriesGameModeAndStage 验证 owner-first 冷启动恢复:
// owner 给出 exact BATTLE TARGET,matchmaker 只补 canonical game_mode/stage。
func TestGetResumeContext_BattleRouteCarriesGameModeAndStage(t *testing.T) {
	notifier := presenceHitBattle(9001)
	uc := newTestUsecaseWithNotifier(t, nil, notifier)
	uc.SetOwnerPlacementQuerier(&fakeOwnerPlacementQuerier{view: stableTestOwnerPlacement(ownerTypeBattle)})
	resolver := &fakeMatchResolver{out: data.PlayerMatchAuthority{
		State:    matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE,
		Stage:    matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_READY,
		MatchID:  9001,
		GameMode: "pve_coop",
	}}
	uc.SetMatchContextResolver(resolver)
	tok := signTestSession(t, uc)

	r, err := uc.GetResumeContext(context.Background(), tok)
	if err != nil {
		t.Fatalf("GetResumeContext: %v", err)
	}
	if r.Route != loginv1.ResumeRoute_RESUME_ROUTE_BATTLE || r.MatchID != 9001 ||
		r.GameMode != "pve_coop" || r.MatchStage != loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_READY {
		t.Fatalf("resume = %+v, want BATTLE/9001/pve_coop/READY", r)
	}
	if r.EntryState != loginv1.ResumeEntryState_RESUME_ENTRY_STATE_TARGET ||
		r.PlacementState != loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_STABLE ||
		r.DSPodName != "battle-stable-1" || r.AllocationID != "battle-allocation-1" {
		t.Fatalf("owner target not preserved in battle resume: %+v", r)
	}
}

// TestGetResumeContext_QueuedClaimEnrichesHubRoute:排队/确认中的玩家冷启动仍回 HUB,
// 但 Resume 带上权威 match_id/stage/game_mode——客户端必须先恢复 x-pandora-game-mode
// 路由头才能 Cancel/Confirm/GetProgress。
func TestGetResumeContext_QueuedClaimEnrichesHubRoute(t *testing.T) {
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: false}}
	uc := newTestUsecaseWithNotifier(t, nil, notifier)
	uc.SetMatchContextResolver(&fakeMatchResolver{out: data.PlayerMatchAuthority{
		State:    matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE,
		Stage:    matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_QUEUED,
		MatchID:  777,
		GameMode: "5v5_ranked",
	}})
	tok := signTestSession(t, uc)

	r, err := uc.GetResumeContext(context.Background(), tok)
	if err != nil {
		t.Fatalf("GetResumeContext: %v", err)
	}
	if r.Route != loginv1.ResumeRoute_RESUME_ROUTE_HUB {
		t.Fatalf("route = %v, want HUB (queued player waits for READY push)", r.Route)
	}
	if r.MatchID != 777 || r.GameMode != "5v5_ranked" ||
		r.MatchStage != loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_QUEUED {
		t.Fatalf("hub resume = %+v, want 777/5v5_ranked/QUEUED enrichment", r)
	}
}

// TestGetResumeContext_NoClaimPlainHub:无任何撮合 claim → 裸 HUB(不带撮合字段)。
func TestGetResumeContext_NoClaimPlainHub(t *testing.T) {
	notifier := &fakeNotifier{bl: data.BattleLocation{InBattle: false}}
	uc := newTestUsecaseWithNotifier(t, nil, notifier)
	uc.SetMatchContextResolver(&fakeMatchResolver{out: data.PlayerMatchAuthority{
		State: matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_NONE,
	}})
	tok := signTestSession(t, uc)

	r, err := uc.GetResumeContext(context.Background(), tok)
	if err != nil {
		t.Fatalf("GetResumeContext: %v", err)
	}
	if r.Route != loginv1.ResumeRoute_RESUME_ROUTE_HUB || r.MatchID != 0 || r.GameMode != "" ||
		r.MatchStage != loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_UNSPECIFIED {
		t.Fatalf("plain hub resume = %+v, want empty HUB", r)
	}
}

// TestGetResumeContext_OwnerTargetSurvivesMatchEnrichmentFailure 验证 owner 是归属唯一权威:
// match 查询失败只丢失展示/恢复富化字段,不能推翻已确定的 exact Battle TARGET。
func TestGetResumeContext_OwnerTargetSurvivesMatchEnrichmentFailure(t *testing.T) {
	notifier := presenceHitBattle(9001)
	uc := newTestUsecaseWithNotifier(t, nil, notifier)
	uc.SetOwnerPlacementQuerier(&fakeOwnerPlacementQuerier{view: stableTestOwnerPlacement(ownerTypeBattle)})
	resolver := &fakeMatchResolver{err: errcode.New(errcode.ErrInternal, "matchmaker down")}
	uc.SetMatchContextResolver(resolver)
	tok := signTestSession(t, uc)

	r, err := uc.GetResumeContext(context.Background(), tok)
	if err != nil {
		t.Fatalf("GetResumeContext: %v", err)
	}
	if r.Route != loginv1.ResumeRoute_RESUME_ROUTE_BATTLE ||
		r.EntryState != loginv1.ResumeEntryState_RESUME_ENTRY_STATE_TARGET ||
		r.PlacementState != loginv1.ResumePlacementState_RESUME_PLACEMENT_STATE_STABLE ||
		r.DSPodName != "battle-stable-1" || r.DSInstanceUID != "battle-uid-1" ||
		r.DSInstanceEpoch != 11 || r.AllocationID != "battle-allocation-1" ||
		r.OwnerEpoch != 7 || r.OperationID != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("owner target must survive enrichment failure: %+v", r)
	}
	if r.MatchID != 0 || r.MatchStage != loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_UNSPECIFIED ||
		r.GameMode != "" || r.MapID != 0 {
		t.Fatalf("failed enrichment must leave match fields empty: %+v", r)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want exactly 1", resolver.calls)
	}
}

// TestResumeStageFromMatchStage_ExplicitMapping:match/login 两个枚举数值语义不对齐
// (match STARTING=1 vs login NONE=1),必须显式映射,严禁数值强转。
func TestResumeStageFromMatchStage_ExplicitMapping(t *testing.T) {
	cases := []struct {
		in   matchv1.PlayerMatchResumeStage
		want loginv1.ResumeMatchStage
	}{
		{matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_STARTING, loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_QUEUED},
		{matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_QUEUED, loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_QUEUED},
		{matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_CONFIRMING, loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_CONFIRMING},
		{matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_ALLOCATING, loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_ALLOCATING},
		{matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_READY, loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_READY},
		{matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_UNSPECIFIED, loginv1.ResumeMatchStage_RESUME_MATCH_STAGE_UNSPECIFIED},
	}
	for _, tc := range cases {
		if got := resumeStageFromMatchStage(tc.in); got != tc.want {
			t.Errorf("resumeStageFromMatchStage(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}
