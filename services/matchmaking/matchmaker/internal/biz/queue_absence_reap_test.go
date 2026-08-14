// queue_absence_reap_test.go — 排队票离线回收(INC-20260814-001 隔夜幽灵票)。
//
// 事故形状:test0052 排队后关掉客户端,非终态票据持久且无任何链路转终态
// (team offline_leave 刻意不联动且单人队不处理;liveness_gate 已回退关闭),
// 16h53m 后被拿去与次日新玩家 test3 成局——幽灵对手,DS 只进来一个人。
//
// 判据契约与 StartMatch 在线闸(start_presence_gate_test.go)同源:按「离开了多久」判,
// 绝不按「此刻查不查得到」判(INC-20260724-001 结构性假阳性)。因此这里的用例同样
// 成对出现:**真离场超窗的要回收**,**UNKNOWN/窗内/查询失败的一张都不许动**。
package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/errcode"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
)

// reapFixture 造一个带可控 presence 的夹具(复用 start_presence_gate_test.go 的 fakePresence)。
func reapFixture(t *testing.T, mutate func(*conf.MatchConf)) (*fixture, *fakePresence) {
	t.Helper()
	f := newFixtureWith(t, 9000, mutate)
	p := &fakePresence{online: map[uint64]bool{}, lastSeen: map[uint64]int64{}}
	f.uc.SetPresenceReader(p)
	return f, p
}

// seedTicketAt 与 seedTicket 相同,但可指定入队时刻(造隔夜旧票)。
func (f *fixture) seedTicketAt(t *testing.T, ctx context.Context, ticketID uint64, playerIDs []uint64, enqueuedAtMs int64) {
	t.Helper()
	members := make([]*matchv1.MatchMemberStorageRecord, 0, len(playerIDs))
	for _, pid := range playerIDs {
		if _, ok, err := f.repo.ClaimPlayer(ctx, pid, ticketID, f.cfg.TicketTTL.Std()); err != nil || !ok {
			t.Fatalf("claim player %d: ok=%v err=%v", pid, ok, err)
		}
		members = append(members, &matchv1.MatchMemberStorageRecord{
			PlayerId: pid, TeamId: ticketID, Mmr: 1000, Confirm: confirmPending,
		})
	}
	ticket := &matchv1.MatchTicketStorageRecord{
		TicketId: ticketID, TeamId: ticketID, CaptainId: playerIDs[0],
		Members: members, AvgMmr: 1000, EnqueuedAtMs: enqueuedAtMs,
	}
	if err := f.repo.AddTicket(ctx, ticket, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatalf("add ticket %d: %v", ticketID, err)
	}
}

// ── 队列周期扫除 ──────────────────────────────────────────────────────────────

// 事故正形:玩家隔夜离场(16h ≫ 120s 判死窗)→ 票被回收、归属释放,可立刻重排;
// 在线玩家的票原样留队。
func TestQueueAbsenceSweep_隔夜幽灵票被回收(t *testing.T) {
	ctx := context.Background()
	f, p := reapFixture(t, nil)
	overnight := time.Now().Add(-16 * time.Hour)
	f.seedTicketAt(t, ctx, 100, []uint64{1}, overnight.UnixMilli()) // 幽灵票
	f.seedTicket(t, ctx, 200, []uint64{2}, 1000)                    // 在线票

	p.online[2] = true
	p.lastSeen[1] = overnight.UnixMilli() // 离场基线:16h 前

	if err := f.uc.queueAbsenceSweepOnce(ctx); err != nil {
		t.Fatalf("queueAbsenceSweepOnce: %v", err)
	}
	if _, found, _ := f.repo.GetTicket(ctx, 100); found {
		t.Fatal("隔夜幽灵票必须被回收")
	}
	if _, found, _ := f.repo.GetPlayerTicket(ctx, 1); found {
		t.Fatal("回收后必须释放 player claim(玩家重连后可立刻再排)")
	}
	if left, _ := f.repo.RangeQueueTickets(ctx); len(left) != 1 || left[0] != 200 {
		t.Fatalf("queue = %v, want [200](在线票不许动)", left)
	}
	if got := f.pusher.lastStageFor(1); got != stageFailed {
		t.Fatalf("被回收票的成员 last push stage = %v, want FAILED(重连后 GetMatchProgress 兜底)", got)
	}
}

// 组队票:任一成员离场超窗即整票回收,同票在线队友收到 FAILED 推送(不再干等)。
func TestQueueAbsenceSweep_组队票按成员判死并通知队友(t *testing.T) {
	ctx := context.Background()
	f, p := reapFixture(t, nil)
	f.seedTicket(t, ctx, 100, []uint64{1, 2, 3}, 1000)
	p.online[1] = true
	p.online[2] = true
	p.lastSeen[3] = time.Now().Add(-10 * time.Minute).UnixMilli() // 队员 3 离场 10min

	if err := f.uc.queueAbsenceSweepOnce(ctx); err != nil {
		t.Fatalf("queueAbsenceSweepOnce: %v", err)
	}
	if _, found, _ := f.repo.GetTicket(ctx, 100); found {
		t.Fatal("含离场超窗成员的组队票必须整票回收")
	}
	if got := f.pusher.lastStageFor(1); got != stageFailed {
		t.Fatalf("在线队友必须收到 FAILED 推送, got %v", got)
	}
}

// 反向:刚离开 30s(< 120s 判死窗,可能正在重连)→ 一张不许动。
func TestQueueAbsenceSweep_窗内离场不回收(t *testing.T) {
	ctx := context.Background()
	f, p := reapFixture(t, nil)
	f.seedTicket(t, ctx, 100, []uint64{1}, 1000)
	p.lastSeen[1] = time.Now().Add(-30 * time.Second).UnixMilli()

	if err := f.uc.queueAbsenceSweepOnce(ctx); err != nil {
		t.Fatalf("queueAbsenceSweepOnce: %v", err)
	}
	if _, found, _ := f.repo.GetTicket(ctx, 100); !found {
		t.Fatal("窗内离场(正在重连)的票不得回收")
	}
}

// ★ 防倒退:UNKNOWN 不得冒充 OFFLINE(§9.22)。查不到位置也没有任何离开基线
// (Hub DS 整台崩溃 / 超保留期)→ 放行,这正是 liveness_gate 当年误杀的形状。
func TestQueueAbsenceSweep_无离开基线UNKNOWN不回收(t *testing.T) {
	ctx := context.Background()
	f, p := reapFixture(t, nil)
	f.seedTicket(t, ctx, 100, []uint64{1}, 1000)
	_ = p // 既不在 online 也没有 lastSeen 基线

	if err := f.uc.queueAbsenceSweepOnce(ctx); err != nil {
		t.Fatalf("queueAbsenceSweepOnce: %v", err)
	}
	if _, found, _ := f.repo.GetTicket(ctx, 100); !found {
		t.Fatal("UNKNOWN 不得冒充 OFFLINE:无离开基线的票绝不回收")
	}
}

// 弱依赖:presence 任一跳查询失败 → 整轮跳过,绝不在不确定时删票。
func TestQueueAbsenceSweep_Presence查询失败整轮跳过(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		mutate func(p *fakePresence)
	}{
		{"BatchOnline失败", func(p *fakePresence) { p.onlineErr = errors.New("locator down") }},
		{"BatchLastSeen失败", func(p *fakePresence) { p.lastSeenErr = errors.New("locator down") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, p := reapFixture(t, nil)
			f.seedTicket(t, ctx, 100, []uint64{1}, 1000)
			p.lastSeen[1] = time.Now().Add(-16 * time.Hour).UnixMilli()
			tc.mutate(p)

			if err := f.uc.queueAbsenceSweepOnce(ctx); err != nil {
				t.Fatalf("弱依赖路径不得上抛: %v", err)
			}
			if _, found, _ := f.repo.GetTicket(ctx, 100); !found {
				t.Fatal("presence 查询失败时一张票都不许删")
			}
		})
	}
}

// 负值关闭整条回收:即使明确离场 16h 也不动。
func TestQueueAbsenceSweep_负值关闭(t *testing.T) {
	ctx := context.Background()
	f, p := reapFixture(t, func(c *conf.MatchConf) {
		c.QueueAbsenceReapAfter = config.Duration(-1)
	})
	f.seedTicket(t, ctx, 100, []uint64{1}, 1000)
	p.lastSeen[1] = time.Now().Add(-16 * time.Hour).UnixMilli()

	if err := f.uc.queueAbsenceSweepOnce(ctx); err != nil {
		t.Fatalf("queueAbsenceSweepOnce: %v", err)
	}
	if _, found, _ := f.repo.GetTicket(ctx, 100); !found {
		t.Fatal("回收关闭(负值)时不得删票")
	}
}

// ── 成局装箱前复查(扫除节流间隔内的竞态窗) ─────────────────────────────────────

// 事故端到端复现:隔夜幽灵票 + 次日新玩家,1v1 恰好凑满 → 修复后**不得成局**;
// 幽灵票当场回收,无辜新票留队等真人。
func TestMatchOnce_幽灵票不得与新玩家成局(t *testing.T) {
	ctx := context.Background()
	f, p := reapFixture(t, func(c *conf.MatchConf) { c.TeamSize = 1 }) // 1v1,两张单人票即满
	overnight := time.Now().Add(-16 * time.Hour)
	f.seedTicketAt(t, ctx, 100, []uint64{1}, overnight.UnixMilli()) // 幽灵票(test0052 形状)
	f.seedTicket(t, ctx, 200, []uint64{2}, 1000)                    // 新玩家(test3 形状)

	p.online[2] = true
	p.lastSeen[1] = overnight.UnixMilli()

	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}
	// 不得产生任何 match:玩家 2 的 claim 仍指向自己的排队票。
	if tid, found, _ := f.repo.GetPlayerTicket(ctx, 2); !found || tid != 200 {
		t.Fatalf("新玩家 claim = (%d,%v), 必须仍指向排队票 200(不得被冻进幽灵局)", tid, found)
	}
	t200, found, _ := f.repo.GetTicket(ctx, 200)
	if !found || t200.MatchId != 0 {
		t.Fatalf("新玩家的票必须原样留队: found=%v match_id=%d", found, t200.GetMatchId())
	}
	// 幽灵票被复查当场回收,claim 释放。
	if _, found, _ := f.repo.GetTicket(ctx, 100); found {
		t.Fatal("幽灵票必须在成局复查中被回收")
	}
	if _, found, _ := f.repo.GetPlayerTicket(ctx, 1); found {
		t.Fatal("幽灵票成员 claim 必须释放")
	}
	if left, _ := f.repo.RangeQueueTickets(ctx); len(left) != 1 || left[0] != 200 {
		t.Fatalf("queue = %v, want [200]", left)
	}
}

// walk-in/solo 直进路径同样过复查:本事故正是单人票,不复查等于白拉一台 DS。
func TestFormSoloMatch_幽灵票不得成局(t *testing.T) {
	ctx := context.Background()
	f, p := reapFixture(t, nil)
	overnight := time.Now().Add(-16 * time.Hour)
	f.seedTicketAt(t, ctx, 100, []uint64{1}, overnight.UnixMilli())
	p.lastSeen[1] = overnight.UnixMilli()

	ticket, found, err := f.repo.GetTicket(ctx, 100)
	if err != nil || !found {
		t.Fatalf("get ticket: found=%v err=%v", found, err)
	}
	err = f.uc.formSoloMatch(ctx, ticket)
	if errcode.As(err) != errcode.ErrMatchMemberOffline {
		t.Fatalf("solo 幽灵票必须被拒: err=%v code=%d", err, errcode.As(err))
	}
	if _, found, _ := f.repo.GetTicket(ctx, 100); found {
		t.Fatal("solo 幽灵票必须被回收")
	}
}

// ★ 防倒退(INC-20260724-001):复查对依赖故障必须 fail-open——presence 查不通时照常成局,
// 绝不给 locator 抖动阻断全部成局的权力(真离线者由 DS roster 到齐期限兜底)。
func TestMatchOnce_Presence查询失败照常成局(t *testing.T) {
	ctx := context.Background()
	f, p := reapFixture(t, func(c *conf.MatchConf) { c.TeamSize = 1 })
	f.seedTicket(t, ctx, 100, []uint64{1}, 1000)
	f.seedTicket(t, ctx, 200, []uint64{2}, 1000)
	p.onlineErr = errors.New("locator down")

	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}
	// 两张票都应被预留进同一场 match(fail-open 放行)。
	t100, _, _ := f.repo.GetTicket(ctx, 100)
	t200, _, _ := f.repo.GetTicket(ctx, 200)
	if t100.GetMatchId() == 0 || t100.GetMatchId() != t200.GetMatchId() {
		t.Fatalf("presence 故障时必须照常成局: match_id=%d/%d", t100.GetMatchId(), t200.GetMatchId())
	}
}

// oldestTicketAgeMs:取最老票龄;全部无 enqueued_at_ms(滚动升级旧票)→ 0,不误报告警。
func TestOldestTicketAgeMs(t *testing.T) {
	now := time.Now().UnixMilli()
	tickets := []*matchv1.MatchTicketStorageRecord{
		{EnqueuedAtMs: now - 5_000},
		{EnqueuedAtMs: now - 60_000},
		{EnqueuedAtMs: 0}, // 旧票无字段
	}
	if got := oldestTicketAgeMs(now, tickets); got != 60_000 {
		t.Fatalf("oldestTicketAgeMs = %d, want 60000", got)
	}
	if got := oldestTicketAgeMs(now, []*matchv1.MatchTicketStorageRecord{{}, {}}); got != 0 {
		t.Fatalf("全部无票龄信息时必须返回 0(不触发告警), got %d", got)
	}
}
