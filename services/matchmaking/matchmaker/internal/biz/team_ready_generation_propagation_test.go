// team_ready_generation_propagation_test.go — 代际必须走完「票据 → match 镜像」全链。
//
// 审计发现(2026-08-17):formSoloMatch / formMatch 重建 match 成员时丢弃
// TeamReadyGeneration,而 ReleaseMatch 主路径只从 match 成员收 roster ——
// EndTeamMatch 的跨代 CAS 在主路径上恒为退化档(gen=0),迟到重投会抹掉玩家
// 结算后新点的准备。旧回归测试在 match 成员上手填代际,恰好绕过了被掐断的装配段;
// 本文件从**票据**造数据,穿过真实装配,防同类断链再次静默发生。
package biz

import (
	"context"
	"testing"
	"time"

	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
)

// seedTicketWithGen 造一张成员带 ready 代际的票据(模拟 resolveMembers 的产物)。
func (f *fixture) seedTicketWithGen(t *testing.T, ctx context.Context, ticketID uint64, playerIDs []uint64, gen uint64) {
	t.Helper()
	members := make([]*matchv1.MatchMemberStorageRecord, 0, len(playerIDs))
	for _, pid := range playerIDs {
		if _, ok, err := f.repo.ClaimPlayer(ctx, pid, ticketID, f.cfg.TicketTTL.Std()); err != nil || !ok {
			t.Fatalf("claim player %d: ok=%v err=%v", pid, ok, err)
		}
		members = append(members, &matchv1.MatchMemberStorageRecord{
			PlayerId: pid, TeamId: ticketID, Mmr: 1000, Confirm: confirmPending,
			TeamReadyGeneration: gen,
		})
	}
	ticket := &matchv1.MatchTicketStorageRecord{
		TicketId: ticketID, TeamId: ticketID, CaptainId: playerIDs[0],
		Members: members, AvgMmr: 1000, EnqueuedAtMs: time.Now().UnixMilli(),
	}
	if err := f.repo.AddTicket(ctx, ticket, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatalf("add ticket %d: %v", ticketID, err)
	}
}

func assertMatchMemberGens(t *testing.T, f *fixture, ctx context.Context, ticketID uint64, wantGen uint64) {
	t.Helper()
	ticket, found, err := f.repo.GetTicket(ctx, ticketID)
	if err != nil || !found {
		t.Fatalf("get ticket %d: found=%v err=%v", ticketID, found, err)
	}
	if ticket.GetMatchId() == 0 {
		t.Fatalf("票据 %d 未进 match", ticketID)
	}
	match, found, err := f.repo.GetMatch(ctx, ticket.GetMatchId())
	if err != nil || !found {
		t.Fatalf("get match %d: found=%v err=%v", ticket.GetMatchId(), found, err)
	}
	for _, m := range match.GetMembers() {
		if m.GetTeamReadyGeneration() != wantGen {
			t.Fatalf("match 成员 %d 的代际断链: got=%d want=%d(装配段丢字段 = EndTeamMatch 跨代 CAS 主路径全废)",
				m.GetPlayerId(), m.GetTeamReadyGeneration(), wantGen)
		}
	}
}

func TestFormSoloMatch_代际透传到match成员(t *testing.T) {
	ctx := context.Background()
	f, _ := reapFixture(t, nil)
	f.seedTicketWithGen(t, ctx, 300, []uint64{31}, 7)

	ticket, _, err := f.repo.GetTicket(ctx, 300)
	if err != nil {
		t.Fatalf("get ticket: %v", err)
	}
	if err := f.uc.formSoloMatch(ctx, ticket); err != nil {
		t.Fatalf("formSoloMatch: %v", err)
	}
	assertMatchMemberGens(t, f, ctx, 300, 7)
}

func TestFormMatch_代际透传到match成员(t *testing.T) {
	ctx := context.Background()
	f, _ := reapFixture(t, func(c *conf.MatchConf) { c.TeamSize = 1 })
	f.seedTicketWithGen(t, ctx, 310, []uint64{41}, 9)
	f.seedTicketWithGen(t, ctx, 320, []uint64{42}, 9)

	tA, _, _ := f.repo.GetTicket(ctx, 310)
	tB, _, _ := f.repo.GetTicket(ctx, 320)
	if err := f.uc.formMatch(ctx, [][]*matchv1.MatchTicketStorageRecord{
		{tA}, {tB},
	}); err != nil {
		t.Fatalf("formMatch: %v", err)
	}
	assertMatchMemberGens(t, f, ctx, 310, 9)
	assertMatchMemberGens(t, f, ctx, 320, 9)
}
