// entry_ratelimit_test.go —— 进场侧限流回归(anti-abuse-scene-entry.md §6 第 2/3/7/8 项)。
//
// 通用验收底线逐条对应:
//   - 零副作用拒绝:被限流的 StartMatch 不创建 start operation;
//   - fail-open:limiter Redis 故障时 StartMatch 照常受理;
//   - 不卡玩家:业务失败立即释放冷却,可原速重试;
//   - 退出路径未被波及:no-show 惩罚期内 CancelMatch 照常可用;
//   - 容量耗尽:玩家看到带倒计时的 QUEUEING,不闪 FAILED,票据自动退队重试。
package biz

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/redisx"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/data"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/model"
)

// newEntryLimiter 起独立 miniredis 承载限流键(与 fixture 的撮合存储 Redis 解耦,
// 便于单独注入故障 / FastForward 冷却窗)。
func newEntryLimiter(t *testing.T) (*miniredis.Miniredis, redis.UniversalClient, *data.RedisEntryLimiter) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb, data.NewRedisEntryLimiter(rdb)
}

// brokenEntryLimiter 指向已关闭 miniredis 的限流器(注入 Redis 故障)。
func brokenEntryLimiter(t *testing.T) *data.RedisEntryLimiter {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = rdb.Close() })
	mr.Close()
	return data.NewRedisEntryLimiter(rdb)
}

// ── §6 第 2 项:StartMatch per-队长/per-队伍冷却 ─────────────────────────────

func TestStartMatch_CooldownRejectsRapidRepeatWithZeroSideEffect(t *testing.T) {
	f := newFixture(t, 9000)
	mr, _, lim := newEntryLimiter(t)
	f.uc.SetEntryLimiter(lim)
	ctx := context.Background()

	if _, err := f.uc.StartMatch(ctx, 7100, 500, 800, 0, entryUnset); err != nil {
		t.Fatalf("first StartMatch: %v", err)
	}
	// 冷却窗内同队长立即重试:ErrRateLimited,且零副作用(不创建新 operation)。
	_, err := f.uc.StartMatch(ctx, 7101, 500, 800, 0, entryUnset)
	if errcode.As(err) != errcode.ErrRateLimited {
		t.Fatalf("want ErrRateLimited, got %v", err)
	}
	if _, found, gerr := f.repo.GetStartOperation(ctx, 7101); gerr != nil || found {
		t.Fatalf("throttled StartMatch must not create operation: found=%v err=%v", found, gerr)
	}
	// 窗口过后不再被冷却门拦(此时同队长撞的是一人一票 preflight,证明冷却已放行)。
	mr.FastForward(f.cfg.StartMatchCooldown.Std() + time.Millisecond)
	_, err = f.uc.StartMatch(ctx, 7102, 500, 800, 0, entryUnset)
	if errcode.As(err) != errcode.ErrMatchAlreadyMatching {
		t.Fatalf("after window want ErrMatchAlreadyMatching (cooldown released), got %v", err)
	}
}

func TestStartMatch_CooldownReleasedOnBusinessFailure(t *testing.T) {
	f := newFixture(t, 9010)
	_, _, lim := newEntryLimiter(t)
	f.uc.SetEntryLimiter(lim)
	ctx := context.Background()

	f.locator.inBattle[810] = true
	if _, err := f.uc.StartMatch(ctx, 7110, 510, 810, 0, entryUnset); errcode.As(err) != errcode.ErrMatchInBattle {
		t.Fatalf("want ErrMatchInBattle, got %v", err)
	}
	// 失败已释放冷却:立即重试拿到的仍是业务错误,而不是 ErrRateLimited(§9.20)。
	if _, err := f.uc.StartMatch(ctx, 7111, 510, 810, 0, entryUnset); errcode.As(err) != errcode.ErrMatchInBattle {
		t.Fatalf("immediate retry after failure want ErrMatchInBattle, got %v", err)
	}
}

func TestStartMatch_CooldownFailOpenOnRedisError(t *testing.T) {
	f := newFixture(t, 9020)
	f.uc.SetEntryLimiter(brokenEntryLimiter(t))
	ctx := context.Background()

	if _, err := f.uc.StartMatch(ctx, 7120, 520, 820, 0, entryUnset); err != nil {
		t.Fatalf("limiter redis down must fail-open, got %v", err)
	}
}

// ── §6 第 8 项:no-show 退避执行点 ───────────────────────────────────────────

func TestStartMatch_NoShowPenaltyBlocksThenExpires(t *testing.T) {
	f := newFixture(t, 9030)
	mr, rdb, lim := newEntryLimiter(t)
	f.uc.SetEntryLimiter(lim)
	ctx := context.Background()

	// 模拟 ds_allocator 记罚(写者侧同一 key 构造)。
	if err := redisx.ArmPenalty(ctx, rdb, redisx.RLKey("match", "noshowcd", 830), 30*time.Second); err != nil {
		t.Fatal(err)
	}
	_, err := f.uc.StartMatch(ctx, 7130, 530, 830, 0, entryUnset)
	if errcode.As(err) != errcode.ErrRateLimited || !strings.Contains(err.Error(), "no-show") {
		t.Fatalf("want no-show ErrRateLimited with visible countdown, got %v", err)
	}
	if _, found, gerr := f.repo.GetStartOperation(ctx, 7130); gerr != nil || found {
		t.Fatalf("penalized StartMatch must not create operation: found=%v err=%v", found, gerr)
	}
	// 罚窗过期自动恢复,无需任何人工清理。
	mr.FastForward(30*time.Second + time.Millisecond)
	if _, err := f.uc.StartMatch(ctx, 7131, 530, 830, 0, entryUnset); err != nil {
		t.Fatalf("StartMatch after penalty expiry: %v", err)
	}
}

func TestCancelMatch_UsableDuringNoShowPenalty(t *testing.T) {
	f := newFixture(t, 9040)
	_, rdb, lim := newEntryLimiter(t)
	f.uc.SetEntryLimiter(lim)
	ctx := context.Background()

	if _, err := f.uc.StartMatch(ctx, 7140, 540, 840, 0, entryUnset); err != nil {
		t.Fatal(err)
	}
	if err := f.uc.advanceStartOperationsOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := redisx.ArmPenalty(ctx, rdb, redisx.RLKey("match", "noshowcd", 840), time.Minute); err != nil {
		t.Fatal(err)
	}
	// 验收底线 4:退出路径在任何限流状态下必须可用。
	if err := f.uc.CancelMatch(ctx, 840); err != nil {
		t.Fatalf("CancelMatch during penalty must succeed, got %v", err)
	}
}

// ── §6 第 7 项:solo 换新雪花 match_id + 成局级冷却 ─────────────────────────

func TestFormSoloMatch_UsesFreshSnowflakeMatchID(t *testing.T) {
	f := newFixtureWith(t, 9050, func(c *conf.MatchConf) { c.WalkIn = true })
	ctx := context.Background()

	if _, err := f.uc.StartMatch(ctx, 7200, 560, 8200, 0, entryUnset); err != nil {
		t.Fatal(err)
	}
	if err := f.uc.advanceStartOperationsOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	ticket, found, err := f.repo.GetTicket(ctx, 7200)
	if err != nil || !found {
		t.Fatalf("ticket after solo form: found=%v err=%v", found, err)
	}
	// 决策文档 §2.3:solo 不得复用 ticket_id 做 match_id(撞 2h abandoned claim)。
	if ticket.GetMatchId() == 7200 {
		t.Fatal("solo match must NOT reuse ticket_id as match_id")
	}
	if ticket.GetMatchId() != 9050 {
		t.Fatalf("solo match_id = %d, want fresh snowflake 9050", ticket.GetMatchId())
	}
	if _, found, err := f.repo.GetMatch(ctx, 9050); err != nil || !found {
		t.Fatalf("match 9050: found=%v err=%v", found, err)
	}
}

func TestFormSoloMatch_FormCooldownDampsReformation(t *testing.T) {
	f := newFixtureWith(t, 9060, func(c *conf.MatchConf) { c.WalkIn = true })
	mr, _, lim := newEntryLimiter(t)
	f.uc.SetEntryLimiter(lim)
	ctx := context.Background()

	if _, err := f.uc.StartMatch(ctx, 7300, 570, 8300, 0, entryUnset); err != nil {
		t.Fatal(err)
	}
	if err := f.uc.advanceStartOperationsOnce(ctx); err != nil {
		t.Fatal(err)
	}
	// 预布成局静默窗(容量耗尽退票会走同一机制)。
	if err := lim.ArmFormCooldown(ctx, 7300, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	ticket, _, _ := f.repo.GetTicket(ctx, 7300)
	if ticket.GetMatchId() != 0 {
		t.Fatalf("ticket in form cooldown must stay queued, got match_id=%d", ticket.GetMatchId())
	}
	// 窗过自动恢复成局。
	mr.FastForward(5*time.Second + time.Millisecond)
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	ticket, _, _ = f.repo.GetTicket(ctx, 7300)
	if ticket.GetMatchId() == 0 {
		t.Fatal("ticket must form after cooldown window")
	}
}

// ── §6 第 3 项:容量耗尽 → QUEUEING(带倒计时)而非 FAILED ──────────────────

// noCapacityAllocator 恒返回确定性 5001(agones 无可用 GameServer)。
type noCapacityAllocator struct{}

func (noCapacityAllocator) AllocateBattle(context.Context, uint64, []uint64, uint32) (*model.BattleAllocation, error) {
	return nil, errcode.New(errcode.ErrDSNoAvailable, "no gameserver")
}
func (noCapacityAllocator) AbortBattleAllocation(context.Context, uint64, string, *model.BattleAllocation) error {
	return nil
}
func (noCapacityAllocator) SignBattleTickets(context.Context, uint64, []uint64, *model.BattleAllocation) (map[uint64]string, error) {
	return nil, errcode.New(errcode.ErrInvalidState, "unexpected ticket signing")
}
func (noCapacityAllocator) SignBattleTicket(context.Context, uint64, uint64, *model.BattleAllocation) (string, error) {
	return "", errcode.New(errcode.ErrInvalidState, "unexpected ticket signing")
}

func TestAllocationNoCapacity_RequeuesAsQueueingWithRetryAfter(t *testing.T) {
	f := newFixture(t, 9600)
	ctx := context.Background()
	_, _, lim := newEntryLimiter(t)
	f.uc.SetEntryLimiter(lim)
	seedAllocatingMatch(t, ctx, f, 9600, time.Now().Add(time.Minute).UnixMilli())
	f.uc.allocator = noCapacityAllocator{}

	m, found, err := f.repo.GetMatch(ctx, 9600)
	if err != nil || !found {
		t.Fatalf("seed match: found=%v err=%v", found, err)
	}
	if aerr := f.uc.advanceAllocation(ctx, m); aerr == nil {
		t.Fatal("definitive no-capacity must surface an error to the worker")
	}

	// match 内部终态 FAILED(新一轮成局换新 match_id,不复用本记录)。
	after, found, _ := f.repo.GetMatch(ctx, 9600)
	if !found || after.GetStage() != stageFailed {
		t.Fatalf("match stage = %v, want internal FAILED", after.GetStage())
	}
	// 票据全部无过错退队。
	for _, tid := range []uint64{100, 200} {
		ticket, tfound, terr := f.repo.GetTicket(ctx, tid)
		if terr != nil || !tfound || ticket.GetMatchId() != 0 {
			t.Fatalf("ticket %d must requeue: found=%v match_id=%d err=%v", tid, tfound, ticket.GetMatchId(), terr)
		}
		// 静默窗已布设:满载期重试节拍被压到 no_capacity_requeue_delay。
		if in, ierr := lim.InFormCooldown(ctx, tid); ierr != nil || !in {
			t.Fatalf("ticket %d must be in form cooldown: in=%v err=%v", tid, in, ierr)
		}
	}
	// 玩家可见语义:绝无 FAILED 推送;QUEUEING 携带 estimated_wait_seconds 倒计时。
	f.pusher.mu.Lock()
	defer f.pusher.mu.Unlock()
	var queueingWithWait int
	for _, e := range f.pusher.events {
		switch e.GetProgress().GetStage() {
		case stageFailed:
			t.Fatalf("no-capacity must not push FAILED, got event %+v", e)
		case stageQueueing:
			if e.GetProgress().GetEstimatedWaitSeconds() > 0 {
				queueingWithWait++
			}
		}
	}
	if queueingWithWait == 0 {
		t.Fatal("want QUEUEING push with estimated_wait_seconds countdown")
	}
}
