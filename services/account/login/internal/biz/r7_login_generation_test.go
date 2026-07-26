// r7_login_generation_test.go — 并发 Login 代际定序回归(R7 收口,2026-07-23)。
//
// 缺陷背景:旧实现先写 Redis 再无条件覆盖 MySQL,交错「A 写 Redis → B 写 Redis+MySQL
// 登录成功 → A 迟到覆盖 MySQL」后 Redis=B、MySQL=A,合法的 B 被 SetRole 代际复核拒绝。
// 修复后:Login 先 MySQL 原子分配单调代际(定序权威),再对 Redis 做「仅更高代际可
// 覆盖」条件写;输掉定序的登录直接失败,零凭据交付。
package biz

import (
	"context"
	"fmt"
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

	// ── R11 复审 P0-1 故障注入 ──
	// ambiguousCommit=true 时 PersistSessionJTI 模拟「COMMIT 已生效但返回错误」:
	// 按真实实现的口径同时返回快照 lease 与包了 data.ErrCommitAmbiguous 的错误。
	ambiguousCommit bool
	// lastJTI 是最近一次 PersistSessionJTI 收到的 jti(判定读回时当作"行内值")。
	lastJTI string
	// loadRowJTI 覆盖读回时行内的 jti;为空则回显 lastJTI(= COMMIT 确实生效)。
	loadRowJTI string
	// loadNotFound / loadErr 分别模拟"无行"与"读回本身失败"。
	loadNotFound bool
	loadErr      error
	loadCalls    int
	// ctxSeen 记录每次补偿/判定调用时 ctx 的存活情况,用于断言补偿没跑在已取消的
	// 请求 ctx 上(R11 复审 P0-1 问题 B)。
	ctxSeen []genRepoCtxProbe
}

// genRepoCtxProbe 是一次调用看到的 ctx 状态快照。
type genRepoCtxProbe struct {
	Op          string
	Err         error
	HasDeadline bool
}

func (f *fakeSessionGenRepo) probeCtx(ctx context.Context, op string) {
	_, hasDeadline := ctx.Deadline()
	f.ctxSeen = append(f.ctxSeen, genRepoCtxProbe{Op: op, Err: ctx.Err(), HasDeadline: hasDeadline})
}

func (f *fakeSessionGenRepo) PersistSessionJTI(_ context.Context, _ uint64, jti string) (data.SessionGenerationLease, error) {
	f.called = true
	f.lastJTI = jti
	if f.callOrder != nil {
		*f.callOrder = append(*f.callOrder, "mysql-gen")
	}
	if f.ambiguousCommit {
		// 真实实现在 COMMIT 报错时返回 (快照 lease, 包裹 ErrCommitAmbiguous 的错误)。
		return data.SessionGenerationLease{Generation: f.gen, PrevJTI: "prev-jti", HadPrev: f.gen > 1},
			errcode.NewCause(errcode.ErrInternal,
				fmt.Errorf("%w: connection reset by peer", data.ErrCommitAmbiguous),
				"commit session generation: connection reset by peer")
	}
	if f.err != nil {
		return data.SessionGenerationLease{}, f.err
	}
	return data.SessionGenerationLease{Generation: f.gen, PrevJTI: "prev-jti", HadPrev: f.gen > 1}, nil
}

func (f *fakeSessionGenRepo) LoadSessionGeneration(ctx context.Context, _ uint64) (string, uint64, bool, error) {
	f.loadCalls++
	f.probeCtx(ctx, "mysql-load")
	if f.callOrder != nil {
		*f.callOrder = append(*f.callOrder, "mysql-load")
	}
	if f.loadErr != nil {
		return "", 0, false, f.loadErr
	}
	if f.loadNotFound {
		return "", 0, false, nil
	}
	row := f.loadRowJTI
	if row == "" {
		row = f.lastJTI
	}
	return row, f.gen, true, nil
}

func (f *fakeSessionGenRepo) RestoreSessionJTI(ctx context.Context, _ uint64, failedJTI string, lease data.SessionGenerationLease) (bool, error) {
	f.restoreCalls = append(f.restoreCalls, struct {
		FailedJTI string
		Lease     data.SessionGenerationLease
	}{failedJTI, lease})
	f.probeCtx(ctx, "mysql-restore")
	if f.callOrder != nil {
		*f.callOrder = append(*f.callOrder, "mysql-restore")
	}
	if f.restoreErr != nil {
		return false, f.restoreErr
	}
	return true, nil
}

func (f *fakeSessionGenRepo) TombstoneSessionJTI(ctx context.Context, _ uint64, jti string) (bool, error) {
	f.tombstoneCalls = append(f.tombstoneCalls, jti)
	f.probeCtx(ctx, "mysql-tombstone")
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

// ─── R11 复审 P0-1 问题 A:COMMIT 已成功但返回错误 ────────────────────────────
//
// 关闭标准要求故障注入覆盖该交错,并证明最终不存在:两存储不同代 / 未交付 JTI 成为
// 当前代 / 旧凭据错误复活。三个分支各一条测试。

// ① 读回判定「COMMIT 确实生效」→ 登录必须继续走完,凭据真正交付,两存储收敛同代际。
// 修复前:直接 ErrUnavailable 失败,MySQL 停在带着未交付 jti 的新代际,仍在线的上一代
// 会话被 SetRole 代际门错误拒绝。
func TestLogin_AmbiguousCommitLanded_ResolvesAndDelivers(t *testing.T) {
	var order []string
	sessions := &genOrderSessionRepo{callOrder: &order}
	gen := &fakeSessionGenRepo{gen: 6, ambiguousCommit: true, callOrder: &order}
	uc := newGenUsecase(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err != nil || res == nil {
		t.Fatalf("a COMMIT that actually landed must not fail the login: err=%v res=%+v", err, res)
	}
	if gen.loadCalls != 1 {
		t.Fatalf("ambiguity must be resolved by exactly one authoritative read-back, got %d", gen.loadCalls)
	}
	if !sessions.setCalled || sessions.gotGen != 6 {
		t.Fatalf("Redis must be written with the confirmed generation, setCalled=%v gen=%d",
			sessions.setCalled, sessions.gotGen)
	}
	// 未交付 JTI 成为当前代:被消灭——凭据交付了,Redis 持有的就是同一代 jti。
	if _, found, _ := sessions.GetJTI(context.Background(), 42); !found {
		t.Fatal("the confirmed generation must be delivered to Redis (no undelivered current jti)")
	}
	if len(gen.restoreCalls) != 0 || len(gen.tombstoneCalls) != 0 {
		t.Fatalf("a resolved-landed commit needs no compensation, restore=%d tombstone=%v",
			len(gen.restoreCalls), gen.tombstoneCalls)
	}
	if len(order) != 3 || order[0] != "mysql-gen" || order[1] != "mysql-load" || order[2] != "redis-set" {
		t.Fatalf("order must be persist → read-back → redis, got %v", order)
	}
}

// ② 读回判定「本次写没落地 / 已被更高代际取代」→ 零补偿失败,绝不动别人的行。
func TestLogin_AmbiguousCommitDidNotLand_FailsWithZeroCompensation(t *testing.T) {
	sessions := &genOrderSessionRepo{}
	gen := &fakeSessionGenRepo{gen: 6, ambiguousCommit: true, loadNotFound: true}
	uc := newGenUsecase(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err == nil || res != nil {
		t.Fatalf("a commit that did not land must fail the login: err=%v res=%+v", err, res)
	}
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("failure must be retryable ErrUnavailable, got %v", err)
	}
	if sessions.setCalled {
		t.Fatal("Redis must not be written when the sequencing write did not land")
	}
	if len(gen.restoreCalls) != 0 || len(gen.tombstoneCalls) != 0 {
		t.Fatalf("nothing landed → zero compensation; restore=%d tombstone=%v",
			len(gen.restoreCalls), gen.tombstoneCalls)
	}

	// 同分支:行存在但已属于并发赢家(更高代际)——同样零补偿。
	sessions2 := &genOrderSessionRepo{}
	gen2 := &fakeSessionGenRepo{gen: 6, ambiguousCommit: true, loadRowJTI: "winner-jti"}
	uc2 := newGenUsecase(t, sessions2, gen2)
	if _, err := uc2.Login(context.Background(), "acc", "pw", "device-A"); err == nil {
		t.Fatal("losing the sequencing race must fail the login")
	}
	if len(gen2.restoreCalls) != 0 || len(gen2.tombstoneCalls) != 0 {
		t.Fatalf("must never compensate a row owned by the winner, restore=%d tombstone=%v",
			len(gen2.restoreCalls), gen2.tombstoneCalls)
	}
}

// ③ 读回本身也失败 → 仍不可判定:**条件回补到覆盖前 jti** + fail-closed 失败本次登录。
//
// 为什么不是墓碑(R11 二轮复审收口):现场是「Redis 仍持有已交付的上一代 A、MySQL 可能已变成
// 从未交付的 B」。墓碑把 MySQL 推成哨兵,撕裂并没消失只是换形状——活着的 A 能过客户端面
// RPC(以 Redis 为权威)却过不了 SetRole(以 MySQL 代际为权威)。回补则让两存储都回到 A。
func TestLogin_AmbiguousCommitUnresolvable_RestoresPreviousJTIFailClosed(t *testing.T) {
	sessions := &genOrderSessionRepo{}
	gen := &fakeSessionGenRepo{
		gen: 6, ambiguousCommit: true,
		loadErr: errcode.New(errcode.ErrInternal, "mysql unreachable"),
	}
	uc := newGenUsecase(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err == nil || res != nil {
		t.Fatalf("unresolvable commit must fail the login: err=%v res=%+v", err, res)
	}
	if sessions.setCalled {
		t.Fatal("Redis must not be written while the sequencing result is unknown")
	}
	if len(gen.restoreCalls) != 1 {
		t.Fatalf("unresolvable state must conditionally restore the previous jti exactly once, got %d",
			len(gen.restoreCalls))
	}
	// 回补必须带**真实快照**(数据层在 COMMIT 报错时连快照一起返回),否则条件 CAS 无从命中。
	got := gen.restoreCalls[0]
	if got.Lease.PrevJTI != "prev-jti" || !got.Lease.HadPrev {
		t.Fatalf("restore must carry the pre-write snapshot, got %+v", got.Lease)
	}
	if got.Lease.SnapshotUnknown {
		t.Fatal("commit-error path has a real snapshot; it must not be marked SnapshotUnknown")
	}
	if len(gen.tombstoneCalls) != 0 {
		t.Fatalf("tombstone would leave Redis=A / MySQL=sentinel torn, got %v", gen.tombstoneCalls)
	}
}

// 回补必须跑在**独立预算**上:判定读吃满预算后,补偿仍须有完整时间执行
// (共用一个 ctx 时"读不出来就回补"恰好在最需要它的场景里不生效)。
func TestLogin_AmbiguousCommitUnresolvable_RestoreGetsItsOwnBudget(t *testing.T) {
	sessions := &genOrderSessionRepo{}
	gen := &fakeSessionGenRepo{
		gen: 6, ambiguousCommit: true,
		loadErr: errcode.New(errcode.ErrInternal, "mysql unreachable"),
	}
	uc := newGenUsecase(t, sessions, gen)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 请求 ctx 早已取消:补偿必须仍能跑
	if _, err := uc.Login(ctx, "acc", "pw", "device-A"); err == nil {
		t.Fatal("unresolvable commit must fail the login")
	}
	if len(gen.restoreCalls) != 1 {
		t.Fatalf("restore must still run on a detached budget, got %d", len(gen.restoreCalls))
	}
	for _, probe := range gen.ctxSeen {
		if probe.Err != nil || !probe.HasDeadline {
			t.Fatalf("%s ctx must be detached and bounded, err=%v hasDeadline=%v",
				probe.Op, probe.Err, probe.HasDeadline)
		}
	}
}

// ④ 判定为已生效后若 Redis 条件写再失败:快照不可信,补偿只准墓碑,禁止回补。
func TestLogin_AmbiguousCommitLandedThenRedisFails_TombstonesNotRestores(t *testing.T) {
	sessions := &unprovableSessionRepo{}
	gen := &fakeSessionGenRepo{gen: 6, ambiguousCommit: true}
	uc := newGenUsecase2(t, sessions, gen)

	if _, err := uc.Login(context.Background(), "acc", "pw", "device-A"); err == nil {
		t.Fatal("redis failure after a resolved commit must still fail the login")
	}
	if len(gen.restoreCalls) != 0 {
		t.Fatalf("snapshot is unknown after an ambiguous commit; restore is forbidden, got %d",
			len(gen.restoreCalls))
	}
	if len(gen.tombstoneCalls) != 1 {
		t.Fatalf("must fall back to the conditional tombstone, got %v", gen.tombstoneCalls)
	}
}

// ─── R11 复审 P0-1 问题 B:Redis Set 返回时请求已取消 ─────────────────────────
//
// 触发原因常常就是 handler deadline / 客户端断连,补偿若继续用请求 ctx 会整条立刻失败,
// 只剩日志,不确定态原样留在库里。补偿必须跑在脱离取消、自带上界的 ctx 上。
func TestLogin_RequestCancelled_CompensationRunsOnDetachedContext(t *testing.T) {
	sessions := &unprovableSessionRepo{}
	gen := &fakeSessionGenRepo{gen: 9}
	uc := newGenUsecase2(t, sessions, gen)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 请求 ctx 在 Redis 写返回时已取消
	if _, err := uc.Login(ctx, "acc", "pw", "device-A"); err == nil {
		t.Fatal("infra failure must fail the login")
	}
	if len(gen.ctxSeen) == 0 {
		t.Fatal("compensation must still run when the request ctx is dead")
	}
	for _, probe := range gen.ctxSeen {
		if probe.Err != nil {
			t.Fatalf("%s ran on a cancelled ctx (%v): compensation is detached by contract",
				probe.Op, probe.Err)
		}
		if !probe.HasDeadline {
			t.Fatalf("%s ran without a deadline: detached compensation must stay bounded", probe.Op)
		}
	}
	if len(gen.tombstoneCalls) != 1 {
		t.Fatalf("unprovable Redis state must still tombstone once, got %v", gen.tombstoneCalls)
	}
}

// 判定读同样不得跑在已取消的请求 ctx 上:否则「COMMIT 是否生效」永远判不出来。
func TestLogin_RequestCancelled_AmbiguityProbeRunsOnDetachedContext(t *testing.T) {
	sessions := &genOrderSessionRepo{}
	gen := &fakeSessionGenRepo{gen: 6, ambiguousCommit: true}
	uc := newGenUsecase(t, sessions, gen)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := uc.Login(ctx, "acc", "pw", "device-A"); err != nil {
		t.Fatalf("a landed commit must resolve even when the request ctx is dead: %v", err)
	}
	if gen.loadCalls != 1 {
		t.Fatalf("read-back must happen exactly once, got %d", gen.loadCalls)
	}
	for _, probe := range gen.ctxSeen {
		if probe.Err != nil || !probe.HasDeadline {
			t.Fatalf("%s ctx must be detached and bounded, err=%v hasDeadline=%v",
				probe.Op, probe.Err, probe.HasDeadline)
		}
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
