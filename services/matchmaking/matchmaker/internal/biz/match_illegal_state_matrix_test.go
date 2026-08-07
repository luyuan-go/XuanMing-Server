package biz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"

	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/data"
)

type illegalStateTeamReader struct {
	team *teamv1.Team
}

func (r illegalStateTeamReader) GetTeam(context.Context, uint64) (*teamv1.Team, bool, error) {
	return r.team, r.team != nil, nil
}

// BeginTeamMatch 复刻真实 team 侧在锁内做的三项校验(存在 / READY / 队长),
// 让这张非法状态矩阵表继续覆盖到组票入口 —— 校验挪进了 team 的锁,断言不能跟着丢。
func (r illegalStateTeamReader) BeginTeamMatch(_ context.Context, teamID, captainID uint64, _ string, _ int64) (*teamv1.Team, error) {
	if r.team == nil {
		return nil, errcode.New(errcode.ErrMatchTeamNotReady, "team %d not found", teamID)
	}
	if r.team.State != teamv1.TeamState_TEAM_STATE_READY {
		return nil, errcode.New(errcode.ErrMatchTeamNotReady, "team %d not ready (state=%d)", teamID, r.team.State)
	}
	if r.team.CaptainId != captainID {
		return nil, errcode.New(errcode.ErrTeamNotCaptain, "player %d not captain of team %d", captainID, teamID)
	}
	return r.team, nil
}

type selectiveBattleLocator struct {
	inBattle map[uint64]bool
	errs     map[uint64]error
}

func (l *selectiveBattleLocator) NotifyMatching(context.Context, []uint64, uint64) error {
	return nil
}

func (l *selectiveBattleLocator) NotifyBattle(context.Context, []uint64, uint64, string) error {
	return nil
}

func (l *selectiveBattleLocator) IsInBattle(_ context.Context, playerID uint64) (bool, error) {
	if err := l.errs[playerID]; err != nil {
		return false, err
	}
	return l.inBattle[playerID], nil
}

func (*selectiveBattleLocator) FindOfflinePlayers(context.Context, []uint64) ([]uint64, error) {
	return nil, nil
}

func readyIllegalStateTeam(teamID, captainID uint64, playerIDs ...uint64) *teamv1.Team {
	members := make([]*teamv1.TeamMember, 0, len(playerIDs))
	for _, playerID := range playerIDs {
		members = append(members, &teamv1.TeamMember{
			PlayerId: playerID,
			Mmr:      1000,
			Ready:    true,
		})
	}
	return &teamv1.Team{
		TeamId:    teamID,
		CaptainId: captainID,
		State:     teamv1.TeamState_TEAM_STATE_READY,
		Members:   members,
		MaxSize:   int32(len(members)),
	}
}

func assertNoStartArtifacts(t *testing.T, f *fixture, ticketID uint64, playerIDs ...uint64) {
	t.Helper()
	ctx := context.Background()
	if _, found, err := f.repo.GetStartOperation(ctx, ticketID); err != nil || found {
		t.Fatalf("start operation must not exist: ticket=%d found=%v err=%v", ticketID, found, err)
	}
	if _, found, err := f.repo.GetTicket(ctx, ticketID); err != nil || found {
		t.Fatalf("ticket must not exist: ticket=%d found=%v err=%v", ticketID, found, err)
	}
	for _, playerID := range playerIDs {
		if owner, found, err := f.repo.GetStartPlayerOperation(ctx, playerID); err != nil || found {
			t.Fatalf("start-player index must not exist: player=%d owner=%d found=%v err=%v", playerID, owner, found, err)
		}
		if owner, found, err := f.repo.GetPlayerTicket(ctx, playerID); err != nil || found {
			t.Fatalf("player claim must not exist: player=%d owner=%d found=%v err=%v", playerID, owner, found, err)
		}
	}
	if ids, err := f.repo.RangeQueueTickets(ctx); err != nil || len(ids) != 0 {
		t.Fatalf("queue must stay empty: ids=%v err=%v", ids, err)
	}
	if ids, err := f.repo.RangeDueStartOperations(ctx, time.Now().Add(time.Hour).UnixMilli()); err != nil || len(ids) != 0 {
		t.Fatalf("start due index must stay empty: ids=%v err=%v", ids, err)
	}
}

func TestStartMatchRejectsWholeTeamWhenAnyMemberIsInBattleWithoutSideEffects(t *testing.T) {
	const (
		teamID    = uint64(51001)
		captainID = uint64(51011)
		memberID  = uint64(51012)
		ticketID  = uint64(51021)
	)
	f := newFixture(t, 51100)
	f.uc.reader = illegalStateTeamReader{team: readyIllegalStateTeam(teamID, captainID, captainID, memberID)}
	f.uc.locator = &selectiveBattleLocator{inBattle: map[uint64]bool{memberID: true}, errs: map[uint64]error{}}

	if _, err := f.uc.StartMatch(context.Background(), ticketID, teamID, captainID, 0); errcode.As(err) != errcode.ErrMatchInBattle {
		t.Fatalf("team with one in-battle member must be rejected: err=%v", err)
	}
	assertNoStartArtifacts(t, f, ticketID, captainID, memberID)
}

func TestStartMatchFailsClosedWhenAnyTeamMemberBattleStateIsUnknownWithoutSideEffects(t *testing.T) {
	const (
		teamID    = uint64(52001)
		captainID = uint64(52011)
		memberID  = uint64(52012)
		ticketID  = uint64(52021)
	)
	f := newFixture(t, 52100)
	f.uc.reader = illegalStateTeamReader{team: readyIllegalStateTeam(teamID, captainID, captainID, memberID)}
	f.uc.locator = &selectiveBattleLocator{
		inBattle: map[uint64]bool{},
		errs:     map[uint64]error{memberID: errors.New("locator unavailable")},
	}

	if _, err := f.uc.StartMatch(context.Background(), ticketID, teamID, captainID, 0); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("unknown battle state must fail closed: err=%v", err)
	}
	assertNoStartArtifacts(t, f, ticketID, captainID, memberID)
}

func TestRepeatedWholeTeamStartMatchKeepsOriginalOperationAndCreatesOnlyOneTicket(t *testing.T) {
	const (
		teamID         = uint64(53001)
		playerID       = uint64(53011)
		memberID       = uint64(53012)
		firstTicketID  = uint64(53021)
		secondTicketID = uint64(53022)
	)
	f := newFixture(t, 53100)
	f.uc.reader = illegalStateTeamReader{team: readyIllegalStateTeam(teamID, playerID, playerID, memberID)}
	ctx := context.Background()

	if got, err := f.uc.StartMatch(ctx, firstTicketID, teamID, playerID, 0); err != nil || got != firstTicketID {
		t.Fatalf("first StartMatch: ticket=%d err=%v", got, err)
	}
	if _, err := f.uc.StartMatch(ctx, secondTicketID, teamID, playerID, 0); errcode.As(err) != errcode.ErrMatchAlreadyMatching {
		t.Fatalf("repeated StartMatch must be rejected as already matching: err=%v", err)
	}
	for _, currentPlayerID := range []uint64{playerID, memberID} {
		if owner, found, err := f.repo.GetStartPlayerOperation(ctx, currentPlayerID); err != nil || !found || owner != firstTicketID {
			t.Fatalf("original start operation lost: player=%d owner=%d found=%v err=%v", currentPlayerID, owner, found, err)
		}
	}
	if _, found, err := f.repo.GetStartOperation(ctx, secondTicketID); err != nil || found {
		t.Fatalf("rejected repeat created canonical operation: found=%v err=%v", found, err)
	}

	if err := f.uc.advanceStartOperationsOnce(ctx); err != nil {
		t.Fatalf("advance original start operation: %v", err)
	}
	for _, currentPlayerID := range []uint64{playerID, memberID} {
		if owner, found, err := f.repo.GetPlayerTicket(ctx, currentPlayerID); err != nil || !found || owner != firstTicketID {
			t.Fatalf("player claim must remain on original ticket: player=%d owner=%d found=%v err=%v", currentPlayerID, owner, found, err)
		}
	}
	if ticket, found, err := f.repo.GetTicket(ctx, firstTicketID); err != nil || !found || len(ticket.GetMembers()) != 2 {
		t.Fatalf("original whole-team ticket missing or incomplete: ticket=%+v found=%v err=%v", ticket, found, err)
	}
	if _, found, err := f.repo.GetTicket(ctx, secondTicketID); err != nil || found {
		t.Fatalf("second ticket must not exist: found=%v err=%v", found, err)
	}
	if ids, err := f.repo.RangeQueueTickets(ctx); err != nil || len(ids) != 1 || ids[0] != firstTicketID {
		t.Fatalf("queue must contain only original ticket: ids=%v err=%v", ids, err)
	}
}

type duplicateStartBarrierRepo struct {
	data.MatchRepo

	mu      sync.Mutex
	calls   int
	ready   chan struct{}
	release chan struct{}
}

func (r *duplicateStartBarrierRepo) GetStartPlayerOperation(ctx context.Context, playerID uint64) (uint64, bool, error) {
	owner, found, err := r.MatchRepo.GetStartPlayerOperation(ctx, playerID)
	if err != nil || found {
		return owner, found, err
	}

	// 两个调用都完成真实的“当前没有 start operation”读取后再放行，避免 barrier
	// 位于读取之前时，合法调度让第二个调用读到赢家并返回 AlreadyMatching 的 flaky。
	r.mu.Lock()
	r.calls++
	if r.calls == 2 {
		close(r.ready)
	}
	r.mu.Unlock()

	select {
	case <-r.release:
	case <-ctx.Done():
		return 0, false, ctx.Err()
	}
	return owner, found, nil
}

func TestConcurrentDuplicateStartMatchConvergesToOneLiveTicket(t *testing.T) {
	const (
		playerID = uint64(54011)
		ticketA  = uint64(54021)
		ticketB  = uint64(54022)
	)
	f := newFixture(t, 54100)
	barrier := &duplicateStartBarrierRepo{
		MatchRepo: f.repo,
		ready:     make(chan struct{}),
		release:   make(chan struct{}),
	}
	f.uc.repo = barrier

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errCh := make(chan error, 2)
	for _, ticketID := range []uint64{ticketA, ticketB} {
		ticketID := ticketID
		go func() {
			_, err := f.uc.StartMatch(ctx, ticketID, ticketID, playerID, 0)
			errCh <- err
		}()
	}
	select {
	case <-barrier.ready:
		close(barrier.release)
	case <-ctx.Done():
		t.Fatal("duplicate StartMatch calls did not reach preflight barrier")
	}
	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("canonical ACCEPTED call must not report a false failure: %v", err)
		}
	}

	for i := 0; i < 3; i++ {
		if err := f.uc.advanceStartOperationsOnce(ctx); err != nil {
			t.Fatalf("advance duplicate start operations: %v", err)
		}
	}
	claimedTicket, found, err := f.repo.GetPlayerTicket(ctx, playerID)
	if err != nil || !found || (claimedTicket != ticketA && claimedTicket != ticketB) {
		t.Fatalf("player must have exactly one canonical claim: ticket=%d found=%v err=%v", claimedTicket, found, err)
	}
	otherTicket := ticketA
	if claimedTicket == ticketA {
		otherTicket = ticketB
	}
	if _, found, err := f.repo.GetTicket(ctx, claimedTicket); err != nil || !found {
		t.Fatalf("claimed ticket missing: ticket=%d found=%v err=%v", claimedTicket, found, err)
	}
	if _, found, err := f.repo.GetTicket(ctx, otherTicket); err != nil || found {
		t.Fatalf("losing duplicate ticket must not survive: ticket=%d found=%v err=%v", otherTicket, found, err)
	}
	if ids, err := f.repo.RangeQueueTickets(ctx); err != nil || len(ids) != 1 || ids[0] != claimedTicket {
		t.Fatalf("duplicate starts must converge to one queued ticket: claim=%d ids=%v err=%v", claimedTicket, ids, err)
	}
}
