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
	gotGen     uint64
	setCalled  bool
	setErr     error
	callOrder  *[]string
	fenceCtx   context.Context
	fenceCalls int
}

func (f *genOrderSessionRepo) FenceFailedSet(
	ctx context.Context, playerID uint64, jti string, gen uint64, ttl time.Duration,
) (bool, error) {
	f.fenceCtx = ctx
	f.fenceCalls++
	return f.fakeSessionRepo.FenceFailedSet(ctx, playerID, jti, gen, ttl)
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

// fakeSessionGenRepo 模拟 MySQL 定序权威:返回预设代际或注入失败,并记录失败代际墓碑。
type fakeSessionGenRepo struct {
	gen                  uint64
	err                  error
	called               bool
	callOrder            *[]string
	failedTombstoneErr   error
	failedTombstoneNoop  bool
	failedTombstoneCalls []struct {
		FailedJTI  string
		Generation uint64
	}
	// tombstoneCalls 记录每次 TombstoneSessionJTI 收到的 jti(R10 P0-1 不可证明分支)。
	tombstoneCalls []string
	tombstoneErr   error

	// ── R11 复审 P0-1 故障注入 ──
	// ambiguousCommit=true 时 PersistSessionJTI 模拟「COMMIT 已生效但返回错误」:
	// 按真实实现的口径同时返回 generation lease 与包了 data.ErrCommitAmbiguous 的错误。
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
	// failedTombstoneCtx 用于确认 Redis 与 MySQL fencing 没有误共用同一个 timeout ctx。
	failedTombstoneCtx context.Context
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
		// 真实实现在 COMMIT 报错时返回 generation lease 与包了 ErrCommitAmbiguous 的错误。
		return data.SessionGenerationLease{Generation: f.gen},
			errcode.NewCause(errcode.ErrInternal,
				fmt.Errorf("%w: connection reset by peer", data.ErrCommitAmbiguous),
				"commit session generation: connection reset by peer")
	}
	if f.err != nil {
		return data.SessionGenerationLease{}, f.err
	}
	return data.SessionGenerationLease{Generation: f.gen}, nil
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

func (f *fakeSessionGenRepo) TombstoneFailedSessionJTI(ctx context.Context, _ uint64, failedJTI string, generation uint64) (bool, error) {
	f.failedTombstoneCtx = ctx
	f.failedTombstoneCalls = append(f.failedTombstoneCalls, struct {
		FailedJTI  string
		Generation uint64
	}{failedJTI, generation})
	f.probeCtx(ctx, "mysql-failed-tombstone")
	if f.callOrder != nil {
		*f.callOrder = append(*f.callOrder, "mysql-failed-tombstone")
	}
	if f.failedTombstoneErr != nil {
		return false, f.failedTombstoneErr
	}
	if f.failedTombstoneNoop {
		return false, nil
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
	// gen>1 时先放入一个旧能力，验证失败补偿会明确清能力，而不是把旧候选恢复为 current。
	if gen.gen > 1 {
		_ = sessions.fakeSessionRepo.Set(context.Background(), 42, "prev-token", "prev-jti",
			"prev-device", time.Hour, gen.gen-1)
	}
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

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A", false)
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
// 行已属于赢家,绝不触发失败代际墓碑。
func TestLogin_SupersededByNewerGeneration_NoCredentials(t *testing.T) {
	sessions := &genOrderSessionRepo{
		setErr: errcode.New(errcode.ErrSessionSuperseded, "superseded"),
	}
	gen := &fakeSessionGenRepo{gen: 3}
	uc := newGenUsecase(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A", false)
	if err == nil {
		t.Fatal("superseded conditional write must fail the login")
	}
	if errcode.As(err) != errcode.ErrSessionSuperseded {
		t.Fatalf("want ErrSessionSuperseded, got: %v", err)
	}
	if res != nil {
		t.Fatalf("no credentials may leak past a lost generation race, got: %+v", res)
	}
	if len(gen.failedTombstoneCalls) != 0 {
		t.Fatalf("lost sequencing race must not tombstone the winner's row, got %d calls", len(gen.failedTombstoneCalls))
	}
}

// Redis 条件写基础设施失败 → 登录失败、零凭据，且两处权威都必须进入无能力墓碑。
func TestLogin_RedisInfraFailure_FencesBothAuthorities(t *testing.T) {
	var order []string
	sessions := &genOrderSessionRepo{
		callOrder: &order,
		setErr:    errcode.New(errcode.ErrUnavailable, "redis down"),
	}
	gen := &fakeSessionGenRepo{gen: 5, callOrder: &order}
	uc := newGenUsecase(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A", false)
	if err == nil || res != nil {
		t.Fatalf("infra failure must fail the login with zero credentials, err=%v res=%+v", err, res)
	}
	if len(gen.failedTombstoneCalls) != 1 {
		t.Fatalf("Redis infra failure must trigger exactly one MySQL tombstone, got %d", len(gen.failedTombstoneCalls))
	}
	if got, found, gerr := sessions.GetJTI(context.Background(), 42); gerr != nil || found || got != "" {
		t.Fatalf("Set 结果不确定必须 fail-closed 清能力: jti=%q found=%v err=%v", got, found, gerr)
	}
	call := gen.failedTombstoneCalls[0]
	if call.FailedJTI == "" || call.Generation != 5 {
		t.Fatalf("tombstone must carry the failed jti/generation, got %+v", call)
	}
	want := []string{"mysql-gen", "redis-set", "mysql-failed-tombstone"}
	if len(order) != 3 || order[0] != want[0] || order[1] != want[1] || order[2] != want[2] {
		t.Fatalf("tombstone must follow the failed Redis write, got order=%v", order)
	}
}

// MySQL 墓碑失败只允许影响日志:登录仍以原始基础设施错误失败,不得掩盖或改写错误。
func TestLogin_FailedTombstoneFailure_DoesNotMaskOriginalError(t *testing.T) {
	sessions := &genOrderSessionRepo{
		setErr: errcode.New(errcode.ErrUnavailable, "redis down"),
	}
	gen := &fakeSessionGenRepo{gen: 2, failedTombstoneErr: errcode.New(errcode.ErrInternal, "mysql down too")}
	uc := newGenUsecase(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A", false)
	if res != nil {
		t.Fatalf("no credentials on failure, got %+v", res)
	}
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("login must surface the original Redis failure, got: %v", err)
	}
}

// committedButErroredSessionRepo:Set 报网络类错误但写入**实际已提交**(Lua 已执行、
// 应答丢失);数据实际落进内嵌 fake 的 map，失败补偿必须清能力而不是恢复即时前代。
type committedButErroredSessionRepo struct {
	fakeSessionRepo
}

func (f *committedButErroredSessionRepo) Set(ctx context.Context, playerID uint64, token, jti, deviceID string, ttl time.Duration, gen uint64) error {
	_ = f.fakeSessionRepo.Set(ctx, playerID, token, jti, deviceID, ttl, gen) // 已提交
	return errcode.New(errcode.ErrUnavailable, "redis reply lost after commit")
}

// 「Redis 报错但实际已提交」时不得把未交付会话留成 current，也不得恢复即时前代。
// 两处权威都只保留失败 generation 的无能力水位，登录仍失败且零凭据。
func TestLogin_RedisCommittedButErrored_FencesUndeliveredSession(t *testing.T) {
	sessions := &committedButErroredSessionRepo{}
	_ = sessions.fakeSessionRepo.Set(context.Background(), 42, "prev-token", "prev-jti",
		"prev-device", time.Hour, 5)
	gen := &fakeSessionGenRepo{gen: 6}
	uc := newGenUsecase2(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A", false)
	if err == nil || res != nil {
		t.Fatalf("commit-but-errored write must still fail the login, err=%v res=%+v", err, res)
	}
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("login must surface the original infra failure, got: %v", err)
	}
	if got, found, _ := sessions.GetJTI(context.Background(), 42); found || got != "" {
		t.Fatalf("the committed-but-undelivered jti must be fenced, got=%q found=%v",
			got, found)
	}
	if len(gen.failedTombstoneCalls) != 1 {
		t.Fatalf("MySQL must tombstone the undelivered generation once, got %d", len(gen.failedTombstoneCalls))
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

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A", false)
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
	if len(gen.failedTombstoneCalls) != 0 || len(gen.tombstoneCalls) != 0 {
		t.Fatalf("a resolved-landed commit needs no compensation, failedTombstone=%d logoutTombstone=%v",
			len(gen.failedTombstoneCalls), gen.tombstoneCalls)
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

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A", false)
	if err == nil || res != nil {
		t.Fatalf("a commit that did not land must fail the login: err=%v res=%+v", err, res)
	}
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("failure must be retryable ErrUnavailable, got %v", err)
	}
	if sessions.setCalled {
		t.Fatal("Redis must not be written when the sequencing write did not land")
	}
	if len(gen.failedTombstoneCalls) != 0 || len(gen.tombstoneCalls) != 0 {
		t.Fatalf("nothing landed → zero compensation; failedTombstone=%d logoutTombstone=%v",
			len(gen.failedTombstoneCalls), gen.tombstoneCalls)
	}

	// 同分支:行存在但已属于并发赢家(更高代际)——同样零补偿。
	sessions2 := &genOrderSessionRepo{}
	gen2 := &fakeSessionGenRepo{gen: 6, ambiguousCommit: true, loadRowJTI: "winner-jti"}
	uc2 := newGenUsecase(t, sessions2, gen2)
	if _, err := uc2.Login(context.Background(), "acc", "pw", "device-A", false); err == nil {
		t.Fatal("losing the sequencing race must fail the login")
	}
	if len(gen2.failedTombstoneCalls) != 0 || len(gen2.tombstoneCalls) != 0 {
		t.Fatalf("must never compensate a row owned by the winner, failedTombstone=%d logoutTombstone=%v",
			len(gen2.failedTombstoneCalls), gen2.tombstoneCalls)
	}
}

// ③ 读回本身也失败 → 仍不可判定：只对本次 (jti,generation) 条件写无能力墓碑，
// 不恢复无法证明已交付的即时前代；本次登录 fail-closed。
func TestLogin_AmbiguousCommitUnresolvable_TombstonesFailedGeneration(t *testing.T) {
	sessions := &genOrderSessionRepo{}
	gen := &fakeSessionGenRepo{
		gen: 6, ambiguousCommit: true,
		loadErr: errcode.New(errcode.ErrInternal, "mysql unreachable"),
	}
	uc := newGenUsecase(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A", false)
	if err == nil || res != nil {
		t.Fatalf("unresolvable commit must fail the login: err=%v res=%+v", err, res)
	}
	if sessions.setCalled {
		t.Fatal("Redis must not be written while the sequencing result is unknown")
	}
	if sessions.fenceCalls != 1 {
		t.Fatalf("unresolved COMMIT must fence Redis exactly once, got %d", sessions.fenceCalls)
	}
	if jti, found, ferr := sessions.GetJTI(context.Background(), 42); ferr != nil || found || jti != "" {
		t.Fatalf("unresolved COMMIT left Redis capability live: jti=%q found=%v err=%v", jti, found, ferr)
	}
	if len(gen.failedTombstoneCalls) != 1 {
		t.Fatalf("unresolvable state must conditionally tombstone the failed generation once, got %d",
			len(gen.failedTombstoneCalls))
	}
	got := gen.failedTombstoneCalls[0]
	if got.Generation != 6 {
		t.Fatalf("tombstone must carry the failed generation, got %+v", got)
	}
	if len(gen.tombstoneCalls) != 0 {
		t.Fatalf("tombstone would leave Redis=A / MySQL=sentinel torn, got %v", gen.tombstoneCalls)
	}
	if sessions.fenceCtx == nil || gen.failedTombstoneCtx == nil || sessions.fenceCtx == gen.failedTombstoneCtx {
		t.Fatal("unresolved COMMIT must fence MySQL and Redis with independent bounded contexts")
	}
}

func TestLogin_AmbiguousCommitUnresolvable_MySQLFenceFailureDoesNotGuessRedis(t *testing.T) {
	sessions := &genOrderSessionRepo{}
	gen := &fakeSessionGenRepo{
		gen: 6, ambiguousCommit: true,
		loadErr:            errcode.New(errcode.ErrInternal, "mysql unreachable"),
		failedTombstoneErr: errcode.New(errcode.ErrInternal, "mysql tombstone unavailable"),
	}
	uc := newGenUsecase(t, sessions, gen)

	if _, err := uc.Login(context.Background(), "acc", "pw", "device-A", false); err == nil {
		t.Fatal("unresolvable commit must fail the login")
	}
	if sessions.fenceCalls != 0 {
		t.Fatalf("unproven generation must not fence Redis, calls=%d", sessions.fenceCalls)
	}
	if jti, found, ferr := sessions.GetJTI(context.Background(), 42); ferr != nil || !found || jti != "prev-jti" {
		t.Fatalf("unproven generation changed Redis: jti=%q found=%v err=%v", jti, found, ferr)
	}
}

// B 的 COMMIT 实际没落地时，后续 C 可以合法复用同一个 generation。B 的迟到消歧
// 若拿未证实的 gen 去 fence Redis，会误杀已经交付的 C。
func TestLogin_AmbiguousCommitUnresolvable_NoopNeverFencesSameGenerationWinner(t *testing.T) {
	sessions := &genOrderSessionRepo{}
	gen := &fakeSessionGenRepo{
		gen: 6, ambiguousCommit: true,
		loadErr:             errcode.New(errcode.ErrInternal, "mysql unreachable"),
		failedTombstoneNoop: true,
	}
	uc := newGenUsecase(t, sessions, gen)
	if err := sessions.fakeSessionRepo.Set(
		context.Background(), 42, "token-C", "jti-C", "device-C", time.Hour, 6,
	); err != nil {
		t.Fatalf("seed same-generation winner C: %v", err)
	}

	if _, err := uc.Login(context.Background(), "acc", "pw", "device-B", false); err == nil {
		t.Fatal("unresolvable B commit must fail the login")
	}
	if sessions.fenceCalls != 0 {
		t.Fatalf("B must not fence reusable generation owned by C, calls=%d", sessions.fenceCalls)
	}
	if jti, found, ferr := sessions.GetJTI(context.Background(), 42); ferr != nil || !found || jti != "jti-C" {
		t.Fatalf("same-generation winner C was changed: jti=%q found=%v err=%v", jti, found, ferr)
	}
}

// 墓碑必须跑在**独立预算**上:判定读吃满预算后,补偿仍须有完整时间执行。
func TestLogin_AmbiguousCommitUnresolvable_TombstoneGetsItsOwnBudget(t *testing.T) {
	sessions := &genOrderSessionRepo{}
	gen := &fakeSessionGenRepo{
		gen: 6, ambiguousCommit: true,
		loadErr: errcode.New(errcode.ErrInternal, "mysql unreachable"),
	}
	uc := newGenUsecase(t, sessions, gen)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 请求 ctx 早已取消:补偿必须仍能跑
	if _, err := uc.Login(ctx, "acc", "pw", "device-A", false); err == nil {
		t.Fatal("unresolvable commit must fail the login")
	}
	if len(gen.failedTombstoneCalls) != 1 {
		t.Fatalf("tombstone must still run on a detached budget, got %d", len(gen.failedTombstoneCalls))
	}
	if sessions.fenceCalls != 1 {
		t.Fatalf("proved generation must run Redis fence on its own detached budget, got %d", sessions.fenceCalls)
	}
	for _, probe := range gen.ctxSeen {
		if probe.Err != nil || !probe.HasDeadline {
			t.Fatalf("%s ctx must be detached and bounded, err=%v hasDeadline=%v",
				probe.Op, probe.Err, probe.HasDeadline)
		}
	}
}

// ④ 判定 COMMIT 已生效后若 Redis 条件写再失败：两处都 fence 本次未交付 generation，
// 不能恢复即时前代。
func TestLogin_AmbiguousCommitLandedThenRedisFails_FencesFailedGeneration(t *testing.T) {
	sessions := &unprovableSessionRepo{}
	gen := &fakeSessionGenRepo{gen: 6, ambiguousCommit: true}
	uc := newGenUsecase2(t, sessions, gen)

	if _, err := uc.Login(context.Background(), "acc", "pw", "device-A", false); err == nil {
		t.Fatal("redis failure after a resolved commit must still fail the login")
	}
	if len(gen.failedTombstoneCalls) != 1 {
		t.Fatalf("landed COMMIT must tombstone the failed generation; calls=%d",
			len(gen.failedTombstoneCalls))
	}
	if got := gen.failedTombstoneCalls[0].Generation; got != 6 {
		t.Fatalf("tombstone must bind the failed generation, got %+v", got)
	}
	if len(gen.tombstoneCalls) != 0 {
		t.Fatalf("single-sided tombstone is forbidden, got %v", gen.tombstoneCalls)
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
	if _, err := uc.Login(ctx, "acc", "pw", "device-A", false); err == nil {
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
	if len(gen.failedTombstoneCalls) != 1 {
		t.Fatalf("unprovable Redis state must tombstone the failed generation once, got %d", len(gen.failedTombstoneCalls))
	}
}

// 判定读同样不得跑在已取消的请求 ctx 上:否则「COMMIT 是否生效」永远判不出来。
func TestLogin_RequestCancelled_AmbiguityProbeRunsOnDetachedContext(t *testing.T) {
	sessions := &genOrderSessionRepo{}
	gen := &fakeSessionGenRepo{gen: 6, ambiguousCommit: true}
	uc := newGenUsecase(t, sessions, gen)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := uc.Login(ctx, "acc", "pw", "device-A", false); err != nil {
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

// unprovableSessionRepo:Set 与 FenceFailedSet 双双报错 —— Redis 是否已提交**不可证明**。
type unprovableSessionRepo struct {
	fakeSessionRepo
	fenceCtx  context.Context
	callOrder *[]string
}

func (f *unprovableSessionRepo) Set(context.Context, uint64, string, string, string, time.Duration, uint64) error {
	return errcode.New(errcode.ErrUnavailable, "redis unreachable")
}

func (f *unprovableSessionRepo) FenceFailedSet(ctx context.Context, _ uint64, _ string, _ uint64, _ time.Duration) (bool, error) {
	f.fenceCtx = ctx
	if f.callOrder != nil {
		*f.callOrder = append(*f.callOrder, "redis-fence")
	}
	return false, errcode.New(errcode.ErrUnavailable, "redis unreachable")
}

// Redis 状态不可证明时，两处权威仍须各自尝试 generation-bounded fencing；
// 任一侧失败都不能跳过另一侧，也不能恢复即时前代。
func TestLogin_RedisStateUnprovable_FencesBothAuthorities(t *testing.T) {
	var order []string
	sessions := &unprovableSessionRepo{callOrder: &order}
	gen := &fakeSessionGenRepo{gen: 9, callOrder: &order}
	uc := newGenUsecase2(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A", false)
	if err == nil || res != nil {
		t.Fatalf("infra failure must fail the login with zero credentials, err=%v res=%+v", err, res)
	}
	if len(gen.failedTombstoneCalls) != 1 {
		t.Fatalf("unprovable Redis state must tombstone the failed generation once, got %d", len(gen.failedTombstoneCalls))
	}
	if sessions.fenceCtx == nil || gen.failedTombstoneCtx == nil || sessions.fenceCtx == gen.failedTombstoneCtx {
		t.Fatal("Redis and MySQL fencing must use independent bounded contexts")
	}
	if len(gen.tombstoneCalls) != 0 {
		t.Fatalf("single-sided tombstone is forbidden, got %v", gen.tombstoneCalls)
	}
	if len(order) != 3 || order[0] != "mysql-gen" || order[1] != "mysql-failed-tombstone" || order[2] != "redis-fence" {
		t.Fatalf("both authorities must receive independent fail-closed fencing, got order=%v", order)
	}
}

// MySQL fencing 失败只影响日志:登录仍以原始基础设施错误失败,且不能跳过 Redis fencing。
func TestLogin_UnprovableMySQLFenceFailure_DoesNotMaskOriginalError(t *testing.T) {
	sessions := &unprovableSessionRepo{}
	gen := &fakeSessionGenRepo{gen: 4, failedTombstoneErr: errcode.New(errcode.ErrInternal, "mysql down too")}
	uc := newGenUsecase2(t, sessions, gen)

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A", false)
	if res != nil {
		t.Fatalf("no credentials on failure, got %+v", res)
	}
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("login must surface the original Redis failure, got: %v", err)
	}
	if sessions.fenceCtx == nil {
		t.Fatal("MySQL tombstone failure must not skip independent Redis fencing")
	}
}

// MySQL 条件墓碑未命中表示并发赢家已推进，但 Redis 仍须按 generation 自行判断；
// Redis 看到更高代际时会原子 no-op，不能因一侧 no-op 跳过另一侧收敛。
func TestLogin_MySQLFenceNoopDoesNotSkipRedisFence(t *testing.T) {
	sessions := &unprovableSessionRepo{}
	gen := &fakeSessionGenRepo{gen: 4, failedTombstoneNoop: true}
	uc := newGenUsecase2(t, sessions, gen)

	if _, err := uc.Login(context.Background(), "acc", "pw", "device-A", false); err == nil {
		t.Fatal("original Redis write failure must still fail login")
	}
	if len(gen.failedTombstoneCalls) != 1 {
		t.Fatalf("MySQL conditional tombstone calls=%d, want 1", len(gen.failedTombstoneCalls))
	}
	if sessions.fenceCtx == nil {
		t.Fatal("MySQL no-op must not skip generation-bounded Redis fencing")
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

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A", false)
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
