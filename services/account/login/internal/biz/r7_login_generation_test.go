// r7_login_generation_test.go — 并发 Login 代际定序回归(R7 收口,2026-07-23)。
//
// 缺陷背景:旧实现先写 Redis 再无条件覆盖 MySQL,交错「A 写 Redis → B 写 Redis+MySQL
// 登录成功 → A 迟到覆盖 MySQL」后 Redis=B、MySQL=A,合法的 B 被 SetRole 代际复核拒绝。
// 修复后:Login 先 MySQL 原子分配单调代际(定序权威),再对 Redis 做「仅更高代际可
// 覆盖」条件写;输掉定序的登录直接失败,零凭据交付。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/snowflake"
	"github.com/luyuancpp/pandora/services/account/login/internal/data"
)

// genOrderSessionRepo 记录 Set 收到的代际与调用时序,并可注入条件写失败。
type genOrderSessionRepo struct {
	fakeSessionRepo
	gotGen    uint64
	setCalled bool
	setErr    error
	callOrder *[]string
}

func (f *genOrderSessionRepo) Set(ctx context.Context, playerID uint64, token, jti, deviceID string, ttl time.Duration, gen uint64) error {
	f.setCalled = true
	f.gotGen = gen
	if f.callOrder != nil {
		*f.callOrder = append(*f.callOrder, "redis-set")
	}
	if f.setErr != nil {
		return f.setErr
	}
	return f.fakeSessionRepo.Set(ctx, playerID, token, jti, deviceID, ttl, gen)
}

// fakeSessionGenRepo 模拟 MySQL 定序权威:返回预设代际或注入失败,并记录条件回补调用。
type fakeSessionGenRepo struct {
	gen        uint64
	err        error
	called     bool
	callOrder  *[]string
	restoreErr error
	// restoreCalls 记录每次 RestoreSessionJTI 收到的 (failedJTI, lease)。
	restoreCalls []struct {
		FailedJTI string
		Lease     data.SessionGenerationLease
	}
	// tombstoneCalls 记录每次 TombstoneSessionJTI 收到的 jti(R10 P0-1 不可证明分支)。
	tombstoneCalls []string
	tombstoneErr   error
}

func (f *fakeSessionGenRepo) PersistSessionJTI(_ context.Context, _ uint64, jti string) (data.SessionGenerationLease, error) {
	f.called = true
	if f.callOrder != nil {
		*f.callOrder = append(*f.callOrder, "mysql-gen")
	}
	if f.err != nil {
		return data.SessionGenerationLease{}, f.err
	}
	return data.SessionGenerationLease{Generation: f.gen, PrevJTI: "prev-jti", HadPrev: f.gen > 1}, nil
}

func (f *fakeSessionGenRepo) RestoreSessionJTI(_ context.Context, _ uint64, failedJTI string, lease data.SessionGenerationLease) (bool, error) {
	f.restoreCalls = append(f.restoreCalls, struct {
		FailedJTI string
		Lease     data.SessionGenerationLease
	}{failedJTI, lease})
	if f.callOrder != nil {
		*f.callOrder = append(*f.callOrder, "mysql-restore")
	}
	if f.restoreErr != nil {
		return false, f.restoreErr
	}
	return true, nil
}

func (f *fakeSessionGenRepo) TombstoneSessionJTI(_ context.Context, _ uint64, jti string) (bool, error) {
	f.tombstoneCalls = append(f.tombstoneCalls, jti)
	if f.callOrder != nil {
		*f.callOrder = append(*f.callOrder, "mysql-tombstone")
	}
	if f.tombstoneErr != nil {
		return false, f.tombstoneErr
	}
	return true, nil
}

func newGenUsecase(t *testing.T, sessions *genOrderSessionRepo, gen *fakeSessionGenRepo) *LoginUsecase {
	t.Helper()
	signer, verifier := newTicketTestPair(t)
	repo := &fakeAccountRepo{playerID: 42, passwordHash: mustBcrypt(t, "pw")}
	uc := NewLoginUsecase(repo, sessions, nil, nil, nil, snowflake.NewNode(1),
		"127.0.0.1:7777", "cn", signer, verifier, nil, false, false, nil, false)
	uc.SetSessionGenerationRepo(gen)
	return uc
}

// MySQL 定序权威必须先于 Redis 写入,且分配到的代际原样传给条件写。
func TestLogin_GenerationAllocatedBeforeRedisWrite(t *testing.T) {
	var order []string
	sessions := &genOrderSessionRepo{callOrder: &order}
	gen := &fakeSessionGenRepo{gen: 7, callOrder: &order}
	uc := newGenUsecase(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err != nil {
		t.Fatalf("login failed: %v", err)
	}
	if res == nil || res.SessionToken == "" {
		t.Fatal("login must deliver credentials on success")
	}
	if len(order) < 2 || order[0] != "mysql-gen" || order[1] != "redis-set" {
		t.Fatalf("MySQL generation must be allocated before Redis write, got order=%v", order)
	}
	if sessions.gotGen != 7 {
		t.Fatalf("Redis conditional write must receive the allocated generation, got %d want 7", sessions.gotGen)
	}
}

// 条件写被更高代际拒绝(并发新登录已完成)→ 本次登录失败且零凭据交付;
// 行已属于赢家,绝不触发条件回补(R9 复审 P0-2)。
func TestLogin_SupersededByNewerGeneration_NoCredentials(t *testing.T) {
	sessions := &genOrderSessionRepo{
		setErr: errcode.New(errcode.ErrSessionSuperseded, "superseded"),
	}
	gen := &fakeSessionGenRepo{gen: 3}
	uc := newGenUsecase(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err == nil {
		t.Fatal("superseded conditional write must fail the login")
	}
	if errcode.As(err) != errcode.ErrSessionSuperseded {
		t.Fatalf("want ErrSessionSuperseded, got: %v", err)
	}
	if res != nil {
		t.Fatalf("no credentials may leak past a lost generation race, got: %+v", res)
	}
	if len(gen.restoreCalls) != 0 {
		t.Fatalf("lost sequencing race must not restore the winner's row, got %d restore calls", len(gen.restoreCalls))
	}
}

// Redis 条件写基础设施失败 → 登录失败、零凭据,且必须条件回补 MySQL 代际行
// (R9 复审 P0-2):否则撕裂窗口内上一代合法会话会被 SetRole 强制门误拒。
func TestLogin_RedisInfraFailure_RestoresMySQLGeneration(t *testing.T) {
	var order []string
	sessions := &genOrderSessionRepo{
		callOrder: &order,
		setErr:    errcode.New(errcode.ErrUnavailable, "redis down"),
	}
	gen := &fakeSessionGenRepo{gen: 5, callOrder: &order}
	uc := newGenUsecase(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err == nil || res != nil {
		t.Fatalf("infra failure must fail the login with zero credentials, err=%v res=%+v", err, res)
	}
	if len(gen.restoreCalls) != 1 {
		t.Fatalf("Redis infra failure must trigger exactly one conditional restore, got %d", len(gen.restoreCalls))
	}
	call := gen.restoreCalls[0]
	if call.FailedJTI == "" || call.Lease.Generation != 5 || !call.Lease.HadPrev || call.Lease.PrevJTI != "prev-jti" {
		t.Fatalf("restore must carry the failed jti and the exact persisted lease, got %+v", call)
	}
	want := []string{"mysql-gen", "redis-set", "mysql-restore"}
	if len(order) != 3 || order[0] != want[0] || order[1] != want[1] || order[2] != want[2] {
		t.Fatalf("restore must follow the failed Redis write, got order=%v", order)
	}
}

// 回补自身失败只允许影响日志:登录仍以原始基础设施错误失败,不得掩盖或改写错误。
func TestLogin_RestoreFailure_DoesNotMaskOriginalError(t *testing.T) {
	sessions := &genOrderSessionRepo{
		setErr: errcode.New(errcode.ErrUnavailable, "redis down"),
	}
	gen := &fakeSessionGenRepo{gen: 2, restoreErr: errcode.New(errcode.ErrInternal, "mysql down too")}
	uc := newGenUsecase(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if res != nil {
		t.Fatalf("no credentials on failure, got %+v", res)
	}
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("login must surface the original Redis failure, got: %v", err)
	}
}

// committedButErroredSessionRepo:Set 报网络类错误但写入**实际已提交**(Lua 已执行、
// 应答丢失);数据实际落进内嵌 fake 的 map,故 DeleteIfJTI 会真命中。
type committedButErroredSessionRepo struct {
	fakeSessionRepo
}

func (f *committedButErroredSessionRepo) Set(ctx context.Context, playerID uint64, token, jti, deviceID string, ttl time.Duration, gen uint64) error {
	_ = f.fakeSessionRepo.Set(ctx, playerID, token, jti, deviceID, ttl, gen) // 已提交
	return errcode.New(errcode.ErrUnavailable, "redis reply lost after commit")
}

// R10 复审 P0-1:「Redis 报错但实际已提交」时不得停在撕裂态(Redis=无人持有的新 jti、
// MySQL=旧 jti)。补偿链先用按 jti 的 CAS 删精确撤销本次已提交写(证明 Redis 不再持有
// 本次 jti),再条件回补 MySQL——两存储一致回到上一代,登录仍失败零凭据。
func TestLogin_RedisCommittedButErrored_RollsBackThenRestores(t *testing.T) {
	sessions := &committedButErroredSessionRepo{}
	gen := &fakeSessionGenRepo{gen: 6}
	uc := newGenUsecase2(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err == nil || res != nil {
		t.Fatalf("commit-but-errored write must still fail the login, err=%v res=%+v", err, res)
	}
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("login must surface the original infra failure, got: %v", err)
	}
	if _, found, _ := sessions.GetJTI(context.Background(), 42); found {
		t.Fatal("the committed-but-undelivered jti must be CAS-removed from Redis")
	}
	if len(gen.restoreCalls) != 1 {
		t.Fatalf("after proving Redis no longer holds the jti, MySQL must be restored once, got %d", len(gen.restoreCalls))
	}
	if len(gen.tombstoneCalls) != 0 {
		t.Fatalf("provable state must not take the fail-closed tombstone branch, got %v", gen.tombstoneCalls)
	}
}

// unprovableSessionRepo:Set 与 DeleteIfJTI 双双报错 —— Redis 是否已提交**不可证明**。
type unprovableSessionRepo struct {
	fakeSessionRepo
}

func (f *unprovableSessionRepo) Set(context.Context, uint64, string, string, string, time.Duration, uint64) error {
	return errcode.New(errcode.ErrUnavailable, "redis unreachable")
}

func (f *unprovableSessionRepo) DeleteIfJTI(context.Context, uint64, string) (bool, error) {
	return false, errcode.New(errcode.ErrUnavailable, "redis unreachable")
}

// R10 复审 P0-1 核心回归:Redis 状态不可证明时禁止猜「未提交」去回补旧 jti
// (那会在 Redis 实际已提交的分支上造出 Redis=新 / MySQL=旧 的跨存储撕裂,§9.22)。
// 必须走条件墓碑:任何陈旧 jti 都不再匹配,两侧一致 fail-closed。
func TestLogin_RedisStateUnprovable_TombstonesInsteadOfRestore(t *testing.T) {
	var order []string
	sessions := &unprovableSessionRepo{}
	gen := &fakeSessionGenRepo{gen: 9, callOrder: &order}
	uc := newGenUsecase2(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err == nil || res != nil {
		t.Fatalf("infra failure must fail the login with zero credentials, err=%v res=%+v", err, res)
	}
	if len(gen.restoreCalls) != 0 {
		t.Fatalf("unprovable Redis state must never restore the previous jti, got %d restore calls", len(gen.restoreCalls))
	}
	if len(gen.tombstoneCalls) != 1 {
		t.Fatalf("unprovable Redis state must tombstone exactly once, got %v", gen.tombstoneCalls)
	}
	if len(order) != 2 || order[0] != "mysql-gen" || order[1] != "mysql-tombstone" {
		t.Fatalf("tombstone must follow the failed Redis write, got order=%v", order)
	}
}

// 墓碑本身失败只影响日志:登录仍以原始基础设施错误失败,不得掩盖或改写错误。
func TestLogin_TombstoneFailure_DoesNotMaskOriginalError(t *testing.T) {
	sessions := &unprovableSessionRepo{}
	gen := &fakeSessionGenRepo{gen: 4, tombstoneErr: errcode.New(errcode.ErrInternal, "mysql down too")}
	uc := newGenUsecase2(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if res != nil {
		t.Fatalf("no credentials on failure, got %+v", res)
	}
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("login must surface the original Redis failure, got: %v", err)
	}
}

// newGenUsecase2 与 newGenUsecase 相同,但接受任意 SessionRepo 实现。
func newGenUsecase2(t *testing.T, sessions data.SessionRepo, gen *fakeSessionGenRepo) *LoginUsecase {
	t.Helper()
	signer, verifier := newTicketTestPair(t)
	repo := &fakeAccountRepo{playerID: 42, passwordHash: mustBcrypt(t, "pw")}
	uc := NewLoginUsecase(repo, sessions, nil, nil, nil, snowflake.NewNode(1),
		"127.0.0.1:7777", "cn", signer, verifier, nil, false, false, nil, false)
	uc.SetSessionGenerationRepo(gen)
	return uc
}

// currentJTISessionRepo:GetJTI 恒返回固定"当前一代"jti(precommit 复核用)。
type currentJTISessionRepo struct {
	fakeSessionRepo
	cur string
}

func (f *currentJTISessionRepo) GetJTI(_ context.Context, _ uint64) (string, bool, error) {
	return f.cur, f.cur != "", nil
}

// capturingRoleRepo 捕获 SetRole 收到的 expectedSessJTI 与 precommit 存在性。
type capturingRoleRepo struct {
	gotExpectedJTI string
	gotPrecommit   bool
	setCalls       int
}

func (f *capturingRoleRepo) GetRole(context.Context, uint64) (uint32, error) { return 0, nil }
func (f *capturingRoleRepo) SetRole(ctx context.Context, _ uint64, _ uint32, expectedSessJTI string, precommit func(context.Context) error) error {
	f.setCalls++
	f.gotExpectedJTI = expectedSessJTI
	f.gotPrecommit = precommit != nil
	if precommit != nil {
		if err := precommit(ctx); err != nil {
			return err
		}
	}
	return nil
}

// SetRole 代际强制门:默认(emit-only)不下发 expectedSessJTI,precommit 纵深仍在;
// 开启后 expectedSessJTI 原样下发。滚动窗口内旧 Login Pod 不写代际,提前强制会误拒。
func TestSelectRole_GenerationEnforceGate(t *testing.T) {
	for _, tc := range []struct {
		name        string
		enforce     bool
		wantJTIPass string
	}{
		{name: "default_emit_only", enforce: false, wantJTIPass: ""},
		{name: "enforce_active", enforce: true, wantJTIPass: "jti-current"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			signer, verifier := newTicketTestPair(t)
			roles := &capturingRoleRepo{}
			sessions := &currentJTISessionRepo{cur: "jti-current"}
			uc := NewLoginUsecase(&fakeAccountRepo{playerID: 42}, sessions, nil, nil, roles,
				snowflake.NewNode(1), "127.0.0.1:7777", "cn", signer, verifier, nil,
				false, false, nil, true /*devAllowAnyRole*/)
			uc.SetSessionGenerationEnforce(tc.enforce)

			if _, _, _, err := uc.SelectRole(context.Background(), 42, 7, "jti-current"); err != nil {
				t.Fatalf("SelectRole failed: %v", err)
			}
			if roles.setCalls != 1 {
				t.Fatalf("SetRole calls=%d, want 1", roles.setCalls)
			}
			if roles.gotExpectedJTI != tc.wantJTIPass {
				t.Fatalf("expectedSessJTI=%q, want %q (enforce=%v)", roles.gotExpectedJTI, tc.wantJTIPass, tc.enforce)
			}
			if !roles.gotPrecommit {
				t.Fatal("precommit(Redis 现行性纵深)必须始终存在,不受强制门控制")
			}
		})
	}
}

// MySQL 定序权威失败 → fail-closed:登录失败且 Redis 写从未发生(顺序即防线)。
func TestLogin_GenerationPersistFailure_FailClosedBeforeRedis(t *testing.T) {
	sessions := &genOrderSessionRepo{}
	gen := &fakeSessionGenRepo{err: errcode.New(errcode.ErrInternal, "mysql down")}
	uc := newGenUsecase(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err == nil {
		t.Fatal("generation persistence failure must fail the login")
	}
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("want ErrUnavailable, got: %v", err)
	}
	if res != nil {
		t.Fatalf("no credentials on fail-closed path, got: %+v", res)
	}
	if sessions.setCalled {
		t.Fatal("Redis session write must not happen when generation allocation failed")
	}
}
