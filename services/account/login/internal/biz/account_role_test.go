// account_role_test.go — 账号 / 角色分离与两步登录的行为钉子(2026-08-18)。
//
// 这里只钉三类东西,都是「写错了不会编译失败、但线上后果很重」的:
//  1. EnterRole 的**归属校验**(安全):拿自己的账号 token 去进别人的角色必须被拒;
//  2. 台账的 fail-closed 边界(安全):台账已启用却解不出归属时必须拒绝登录,
//     绝不回落 accounts.player_id —— 唯一例外是「本部署根本没启用台账」这个配置态;
//  3. 播种失败时的**降级**(可用性):角色名拿不到不影响登录。
package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/account/login/internal/data"
)

// fakeRoleLedger 是 account_roles 台账的内存 fake。
type fakeRoleLedger struct {
	rows    map[uint64]data.AccountRole // player_id -> 行
	listErr error
	created []data.AccountRole
}

func newFakeRoleLedger(rows ...data.AccountRole) *fakeRoleLedger {
	l := &fakeRoleLedger{rows: map[uint64]data.AccountRole{}}
	for _, r := range rows {
		l.rows[r.PlayerID] = r
	}
	return l
}

func (l *fakeRoleLedger) ListByAccount(_ context.Context, accountID uint64) ([]data.AccountRole, error) {
	if l.listErr != nil {
		return nil, l.listErr
	}
	var out []data.AccountRole
	for _, r := range l.rows {
		if r.AccountID == accountID && r.Status == 0 {
			out = append(out, r)
		}
	}
	return out, nil
}

func (l *fakeRoleLedger) GetByPlayer(_ context.Context, playerID uint64) (data.AccountRole, error) {
	r, ok := l.rows[playerID]
	if !ok {
		return data.AccountRole{}, errcode.New(errcode.ErrLoginRoleNotFound, "role %d not found", playerID)
	}
	return r, nil
}

func (l *fakeRoleLedger) Create(_ context.Context, r data.AccountRole) error {
	if _, ok := l.rows[r.PlayerID]; ok {
		return errcode.New(errcode.ErrAlreadyExists, "exists")
	}
	l.rows[r.PlayerID] = r
	l.created = append(l.created, r)
	return nil
}

func (l *fakeRoleLedger) TouchLogin(_ context.Context, playerID uint64) error {
	if r, ok := l.rows[playerID]; ok {
		r.LastLoginAt = time.Unix(1_700_000_000, 0)
		l.rows[playerID] = r
	}
	return nil
}

func (l *fakeRoleLedger) NextSlot(_ context.Context, accountID uint64) (uint32, error) {
	var next uint32
	for _, r := range l.rows {
		if r.AccountID == accountID && r.Slot+1 > next {
			next = r.Slot + 1
		}
	}
	return next, nil
}

// TestEnterRoleRejectsRoleOwnedByAnotherAccount 是本次改造最重要的一条安全断言。
//
// EnterRole 的 player_id 来自请求体。它**不是**身份声明,而是「从我的角色列表里选哪个」;
// 身份是账号态 JWT 的 sub。少了这道归属回查,任何人拿自己的账号 token 填别人的
// player_id 就能直接进别人的号 —— 拿到对方的完整 SessionToken 与 Hub 票据。
//
// 这条红了不要改测试,去看 EnterRole 里的 role.AccountID != accountID 分支还在不在。
func TestEnterRoleRejectsRoleOwnedByAnotherAccount(t *testing.T) {
	uc := newTestUsecase(t, nil)
	uc.SetRoleLedger(newFakeRoleLedger(data.AccountRole{
		PlayerID: 999, AccountID: 555, Slot: 0, RoleName: "victim",
	}))

	// 攻击者持有 account_id=111 的合法账号态 token,却去进 account_id=555 名下的角色。
	_, err := uc.EnterRole(context.Background(), 111, 999, "attacker-device")
	if errcode.As(err) != errcode.ErrLoginRoleNotOwned {
		t.Fatalf("越权进入他人角色未被拒:err=%v(期望 ErrLoginRoleNotOwned)", err)
	}
}

// TestEnterRoleRejectsMissingAccountIdentity:拿不到账号身份(Envoy 没挂账号态 provider、
// 或直连内网端口)时必须硬拒,不能当成 account_id=0 去查台账。
func TestEnterRoleRejectsMissingAccountIdentity(t *testing.T) {
	uc := newTestUsecase(t, nil)
	uc.SetRoleLedger(newFakeRoleLedger())

	if _, err := uc.EnterRole(context.Background(), 0, 999, "dev"); errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("缺账号身份时未硬拒:err=%v(期望 ErrUnauthorized)", err)
	}
}

// TestEnterRoleReportsNotImplementedWhenLedgerAbsent:未部署台账的环境(dev 裸跑 /
// 000008 迁移还没跑)必须回 ErrNotImplemented,而不是 Internal。
//
// 区别很实在:§9.21 要求调用方能分辨「对端这个版本没有这个能力」(重试永远不会成功)
// 与「暂时不可用」(重试会好)。回 Internal 会让客户端一直重试选角。
func TestEnterRoleReportsNotImplementedWhenLedgerAbsent(t *testing.T) {
	uc := newTestUsecase(t, nil)
	if _, err := uc.EnterRole(context.Background(), 111, 999, "dev"); errcode.As(err) != errcode.ErrNotImplemented {
		t.Fatalf("无台账时错误码不对:err=%v(期望 ErrNotImplemented)", err)
	}
	if _, err := uc.ListAccountRoles(context.Background(), 111); errcode.As(err) != errcode.ErrNotImplemented {
		t.Fatalf("无台账时 ListAccountRoles 错误码不对:err=%v(期望 ErrNotImplemented)", err)
	}
}

// TestLoginRejectsWhenRoleLedgerUnavailable:台账**已启用但读不出来**时,登录必须失败。
//
// 2026-08-18 用户拍板 fail-closed,推翻了此前「台账挂了就降级放行」的写法。
// 原写法的降级目标是 accounts.player_id —— 而账号 / 角色分离要管住的正是这个指针
// (角色被软删或过户后它可能还指着旧角色)。台账一抖就回落,等于给「进已经不属于
// 自己的角色」开了一条只在 DB 抖动时才打开的旁路,事后完全查不清。
//
// 这条红了不要改成「期望成功」,去看 resolveAccountView 是不是又被改回 fail-soft 了。
func TestLoginRejectsWhenRoleLedgerUnavailable(t *testing.T) {
	uc := newTestUsecase(t, nil)
	ledger := newFakeRoleLedger()
	ledger.listErr = errors.New("account_roles table is gone")
	uc.SetRoleLedger(ledger)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err == nil {
		t.Fatalf("台账不可用却放行了登录(fail-closed 被破坏): res=%+v", res)
	}
	if res != nil {
		t.Fatalf("拒绝路径不得交付任何结果: %+v", res)
	}
}

// TestLoginRejectsWhenAccountHasNoAvailableRole 钉住 fail-closed 想挡住的那个具体后果。
//
// 场景:该账号名下唯一的角色被软删(status!=0),或已过户到别的账号,而
// accounts.player_id 还指着它。老的 fail-soft 路径会回落这个指针,把玩家直接送进一个
// 已经不属于他的角色;新路径必须以 ErrLoginNoRole 拒绝,并且**不能**顺手再建一个。
func TestLoginRejectsWhenAccountHasNoAvailableRole(t *testing.T) {
	uc := newTestUsecase(t, nil)
	// player_id=42 就是 accounts.player_id(fakeAccountRepo),但它已被软删。
	ledger := newFakeRoleLedger(data.AccountRole{
		PlayerID: 42, AccountID: 42, Slot: 0, RoleName: "acc", Status: 1,
	})
	uc.SetRoleLedger(ledger)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if errcode.As(err) != errcode.ErrLoginNoRole {
		t.Fatalf("软删角色未被拒:err=%v res=%+v(期望 ErrLoginNoRole)", err, res)
	}
	if len(ledger.created) != 0 {
		t.Fatalf("拒绝路径不得补建角色(会给同一账号无限造角色): %+v", ledger.created)
	}
}

// TestLoginRejectsWhenRoleTransferredToAnotherAccount:角色已过户,accounts.player_id
// 仍是旧指针。与上一条同一条不变式,但换成「卖角色 / 过户」这个真实业务路径。
func TestLoginRejectsWhenRoleTransferredToAnotherAccount(t *testing.T) {
	uc := newTestUsecase(t, nil)
	// 角色 42 现在挂在 account_id=555 名下,登录的却是 account_id=42。
	ledger := newFakeRoleLedger(data.AccountRole{
		PlayerID: 42, AccountID: 555, Slot: 0, RoleName: "buyer",
	})
	uc.SetRoleLedger(ledger)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err == nil {
		t.Fatalf("过户走的角色仍被放行进入(归属旁路): %+v", res)
	}
	if ledger.rows[42].AccountID != 555 {
		t.Fatalf("拒绝路径篡改了角色归属: %+v", ledger.rows[42])
	}
}

// TestLoginDegradesWhenLedgerNotConfigured:**未部署**台账仍必须放行。
//
// 这是 fail-closed 唯一的例外,而且它不是「故障」是「配置态」:dev 裸跑、以及新二进制
// 已上线但 000008 迁移还没跑完的滚动窗口。这两种情况下集群里根本没有 account_roles,
// 拒绝登录等于把整个滚动升级窗口变成一次停服。
//
// 与上面几条的判别点是 u.roleLedger == nil,不是「查询失败」—— 别把两者合并处理。
func TestLoginDegradesWhenLedgerNotConfigured(t *testing.T) {
	uc := newTestUsecase(t, nil) // 不调 SetRoleLedger = 本部署未启用

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", false)
	if err != nil {
		t.Fatalf("未启用台账时登录被拒(滚动升级窗口会变成停服): %v", err)
	}
	if res.PlayerID != 42 {
		t.Fatalf("兼容档应回落 accounts.player_id=42,实际 %d", res.PlayerID)
	}
	if res.AccountToken != "" || res.AccountID != 0 {
		t.Fatalf("未启用台账时不该有账号层身份:token_present=%t account_id=%d",
			res.AccountToken != "", res.AccountID)
	}
}

// TestLoginDeferRoleEntryRejectsWhenLedgerNotConfigured:新客户端撞上未启用台账的部署,
// 必须拿到 ErrNotImplemented,而不是「成功 + 空 token + 空角色列表」。
//
// 老写法在这里回 OK,新客户端拿到的是一个没有任何错误码的死局:它接着要调的
// ListAccountRoles / EnterRole 都会回 ErrNotImplemented,而选角界面上一个角色都没有,
// 玩家只能重启客户端。§9.21 的降级纪律要求调用方**能分辨**对端没有这个能力。
func TestLoginDeferRoleEntryRejectsWhenLedgerNotConfigured(t *testing.T) {
	uc := newTestUsecase(t, nil) // 未启用台账

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", true)
	if errcode.As(err) != errcode.ErrNotImplemented {
		t.Fatalf("两步登录撞上未启用台账时错误码不对:err=%v res=%+v(期望 ErrNotImplemented)", err, res)
	}
}
// TestLoginDeferRoleEntryReturnsAccountLayerOnly:新客户端置 defer_role_entry 后,
// Login 只认证账号,不该白进一次角色(白占 Hub 座位 + 白签票 + 白轮换一次会话代际)。
func TestLoginDeferRoleEntryReturnsAccountLayerOnly(t *testing.T) {
	uc := newTestUsecase(t, nil)
	uc.SetRoleLedger(newFakeRoleLedger(data.AccountRole{
		PlayerID: 42, AccountID: 42, Slot: 0, RoleName: "acc",
	}))

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", true)
	if err != nil {
		t.Fatalf("Login(defer): %v", err)
	}
	if res.AccountToken == "" || res.AccountID == 0 {
		t.Fatalf("defer 路径必须给出账号层身份:token_present=%t account_id=%d",
			res.AccountToken != "", res.AccountID)
	}
	if len(res.Roles) != 1 || res.Roles[0].PlayerID != 42 {
		t.Fatalf("defer 路径角色列表不对: %+v", res.Roles)
	}
	if res.SessionToken != "" || res.HubTicket != "" {
		t.Fatalf("defer 路径不得交付角色层凭据:session=%q hub_ticket=%q",
			res.SessionToken, res.HubTicket)
	}
}

// TestLoginSeedsRoleNameFromAccountName 钉住用户这次要的那件事本身:
// 角色名 = 账号名,且它是通过**播种到 player 服务**生效的(不是只在响应里塞个字符串)。
func TestLoginSeedsRoleNameFromAccountName(t *testing.T) {
	uc := newTestUsecase(t, nil)
	uc.SetRoleLedger(newFakeRoleLedger(data.AccountRole{
		PlayerID: 42, AccountID: 42, Slot: 0, RoleName: "acc",
	}))
	seeder := &fakeProfileSeeder{}
	uc.SetProfileSeeder(seeder)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", true)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if len(seeder.seeded) != 1 || seeder.seeded[0] != "acc" {
		t.Fatalf("没有把账号名播种给 player 服务: %+v", seeder.seeded)
	}
	if len(res.Roles) != 1 || res.Roles[0].RoleName != "acc" {
		t.Fatalf("选角列表里的角色名不是账号名: %+v", res.Roles)
	}
}

// TestLoginSurvivesProfileSeedFailure:player 服务挂了 / 还没滚上 EnsureProfile 时,
// 登录必须照常成功,角色名降级用台账名(§9.21 弱依赖降级)。
func TestLoginSurvivesProfileSeedFailure(t *testing.T) {
	uc := newTestUsecase(t, nil)
	uc.SetRoleLedger(newFakeRoleLedger(data.AccountRole{
		PlayerID: 42, AccountID: 42, Slot: 0, RoleName: "acc",
	}))
	uc.SetProfileSeeder(&fakeProfileSeeder{
		err: errcode.New(errcode.ErrNotImplemented, "peer too old"),
	})

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", true)
	if err != nil {
		t.Fatalf("播种失败导致登录失败(应当降级而非阻断): %v", err)
	}
	if len(res.Roles) != 1 || res.Roles[0].RoleName != "acc" {
		t.Fatalf("播种失败时应降级用台账名: %+v", res.Roles)
	}
}

// TestLoginBackfillsRoleLedgerForLegacyAccount:旧二进制在滚动窗口里注册的账号
// (accounts 有行、account_roles 没行)必须被补登记成 slot 0,且**复用既有 player_id**。
//
// 这里刻意断言 PlayerID 而不只是「有一行」:另铸一个新 player_id 会让该账号原有的
// 全部游戏数据(以 player_id 为键)当场失联,等于把存档丢了。
func TestLoginBackfillsRoleLedgerForLegacyAccount(t *testing.T) {
	uc := newTestUsecase(t, nil)
	ledger := newFakeRoleLedger() // 空台账 = 旧二进制注册的存量账号
	uc.SetRoleLedger(ledger)

	res, err := uc.Login(context.Background(), "acc", "pw", "dev-1", true)
	if err != nil {
		t.Fatalf("Login: %v", err)
	}
	if len(ledger.created) != 1 {
		t.Fatalf("存量账号没有被补登记角色: %+v", ledger.created)
	}
	if got := ledger.created[0].PlayerID; got != 42 {
		t.Fatalf("补登记铸了新 player_id=%d(应复用 accounts.player_id=42,否则存档失联)", got)
	}
	if got := ledger.created[0].RoleName; got != "acc" {
		t.Fatalf("补登记的角色名应为账号名,实际 %q", got)
	}
	if len(res.Roles) != 1 {
		t.Fatalf("补登记后角色列表应有 1 个: %+v", res.Roles)
	}
}

// fakeProfileSeeder 记录被播种的名字,并可注入失败。
type fakeProfileSeeder struct {
	seeded []string
	err    error
}

func (f *fakeProfileSeeder) EnsureProfile(_ context.Context, _ uint64, nickname string) (data.SeededProfile, error) {
	if f.err != nil {
		return data.SeededProfile{}, f.err
	}
	f.seeded = append(f.seeded, nickname)
	return data.SeededProfile{Created: true, Nickname: nickname, Level: 1}, nil
}
