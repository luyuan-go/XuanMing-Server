package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/placement"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/data"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/model"
)

type allocationErrorStub struct {
	err   error
	calls int
}

func (s *allocationErrorStub) AllocateBattle(context.Context, uint64, []uint64, uint32) (*model.BattleAllocation, error) {
	s.calls++
	return nil, s.err
}

func (s *allocationErrorStub) AllocateBattleWithCombatFactions(
	ctx context.Context, matchID uint64, playerIDs []uint64, _ map[uint64]uint32, mapID uint32,
) (*model.BattleAllocation, error) {
	return s.AllocateBattle(ctx, matchID, playerIDs, mapID)
}

func (s *allocationErrorStub) AbortBattleAllocation(context.Context, uint64, string, *model.BattleAllocation) error {
	return nil
}

func (*allocationErrorStub) SignBattleTickets(context.Context, uint64, []uint64, *model.BattleAllocation) (map[uint64]string, error) {
	return nil, errors.New("unexpected ticket signing")
}

func (*allocationErrorStub) SignBattleTicket(context.Context, uint64, uint64, *model.BattleAllocation) (string, error) {
	return "", errors.New("unexpected ticket signing")
}

func acceptedMembers(tickets ...*matchv1.MatchTicketStorageRecord) []*matchv1.MatchMemberStorageRecord {
	members := make([]*matchv1.MatchMemberStorageRecord, 0)
	for side, ticket := range tickets {
		for _, member := range ticket.GetMembers() {
			members = append(members, &matchv1.MatchMemberStorageRecord{
				PlayerId: member.GetPlayerId(), TeamId: member.GetTeamId(),
				Mmr: member.GetMmr(), Side: int32(side), Confirm: confirmAccepted,
			})
		}
	}
	return members
}

func TestAutoConfirmCrashBeforeAllocationCommitIsRecoveredFromCanonicalGraph(t *testing.T) {
	ctx := context.Background()
	f := newFixtureWith(t, 9901, func(c *conf.MatchConf) { c.AutoConfirmMatch = true })
	for i := uint64(1); i <= 10; i++ {
		f.seedTicket(t, ctx, 100+i, []uint64{i}, 1000)
	}

	faulty := &faultyPersistRepo{MatchRepo: f.repo, failRemaining: 1}
	allocation := model.BattleAllocation{Address: "10.0.0.9:7777", Target: placement.Target{
		PodName: "battle-formation", InstanceUID: "uid-formation", InstanceEpoch: 1,
		AllocationID: "allocation-formation", ReleaseTrack: "stable",
	}}
	allocator := &switchingAllocationStub{first: allocation, second: allocation}
	uc := NewMatchUsecase(faulty, nil, f.pusher, allocator, &fakeIDGen{next: 9901}, f.locator, f.cfg)

	var sideA, sideB []*matchv1.MatchTicketStorageRecord
	for i := uint64(1); i <= 10; i++ {
		ticket, found, err := f.repo.GetTicket(ctx, 100+i)
		if err != nil || !found {
			t.Fatalf("get ticket %d: found=%v err=%v", 100+i, found, err)
		}
		if i <= 5 {
			sideA = append(sideA, ticket)
		} else {
			sideB = append(sideB, ticket)
		}
	}
	// This models a process dying after every reservation was committed but
	// before the final claim ACK/allocation transition.
	if err := uc.formMatch(ctx, [][]*matchv1.MatchTicketStorageRecord{sideA, sideB}); err == nil {
		t.Fatal("injected pre-handoff failure was hidden")
	}
	forming, found, err := f.repo.GetMatch(ctx, 9901)
	if err != nil || !found || forming.GetStage() != stageConfirm || forming.GetAllocationOperationId() != "" {
		t.Fatalf("partial formation escaped CONFIRM: found=%v match=%+v err=%v", found, forming, err)
	}
	if err := uc.advanceAllocationsOnce(ctx); err != nil {
		t.Fatalf("CONFIRM should not be an allocation job: %v", err)
	}
	if allocator.allocateCalls != 0 {
		t.Fatalf("Battle DS allocated before formation commit: calls=%d", allocator.allocateCalls)
	}

	// A fresh process only needs the canonical scan; it repairs the missing
	// claim edge and submits exactly one durable allocation operation.
	restarted := NewMatchUsecase(f.repo, nil, f.pusher, allocator, &fakeIDGen{next: 10000}, f.locator, f.cfg)
	if err := restarted.reconcileActiveOnce(ctx); err != nil {
		t.Fatalf("canonical formation reconciliation failed: %v", err)
	}
	queued, found, err := f.repo.GetMatch(ctx, 9901)
	if err != nil || !found || queued.GetStage() != stageAllocating || queued.GetAllocationOperationId() == "" {
		t.Fatalf("reconciler did not commit allocation: found=%v match=%+v err=%v", found, queued, err)
	}
	if err := restarted.advanceAllocationsOnce(ctx); err != nil {
		t.Fatalf("recovered allocation did not complete: %v", err)
	}
	ready, found, err := f.repo.GetMatch(ctx, 9901)
	if err != nil || !found || ready.GetStage() != stageReady || allocator.allocateCalls != 1 {
		t.Fatalf("recovered formation result: found=%v match=%+v calls=%d err=%v",
			found, ready, allocator.allocateCalls, err)
	}
}

func TestLegacyPartialAllocatingGraphNeverCreatesBattleDS(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9902)
	f.seedTicket(t, ctx, 100, []uint64{1, 2, 3, 4, 5}, 1000)
	f.seedTicket(t, ctx, 200, []uint64{6, 7, 8, 9, 10}, 1000)
	ta, _, _ := f.repo.GetTicket(ctx, 100)
	tb, _, _ := f.repo.GetTicket(ctx, 200)
	ta.MatchId = 9902
	if err := f.repo.ReserveTicket(ctx, ta, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatal(err)
	}
	match := &matchv1.MatchStorageRecord{
		MatchId: 9902, Stage: stageAllocating, Members: acceptedMembers(ta, tb),
		TicketIds: []uint64{100, 200}, CreatedAtMs: time.Now().UnixMilli(),
		ConfirmDeadlineMs:     time.Now().Add(time.Minute).UnixMilli(),
		AllocationOperationId: "legacy-partial", AllocationNextAttemptAtMs: 0,
	}
	if err := f.repo.CreateMatch(ctx, match, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatal(err)
	}
	if err := f.repo.RemoveActive(ctx, 9902); err != nil {
		t.Fatal(err)
	}
	allocator := &allocationErrorStub{err: errors.New("must not be called")}
	f.uc.allocator = allocator
	if err := f.uc.reconcileActiveOnce(ctx); err == nil {
		t.Fatal("partial graph must remain an explicit reconciliation error")
	}
	if ids, err := f.repo.RangeActiveMatches(ctx); err != nil || len(ids) != 1 || ids[0] != 9902 {
		t.Fatalf("partial canonical job did not regain active index: ids=%v err=%v", ids, err)
	}
	if err := f.uc.advanceAllocationsOnce(ctx); err == nil {
		t.Fatal("missing reservation must remain an explicit retryable error")
	}
	still, found, err := f.repo.GetMatch(ctx, 9902)
	if err != nil || !found || still.GetStage() != stageAllocating || allocator.calls != 0 {
		t.Fatalf("partial graph leaked allocation: found=%v match=%+v calls=%d err=%v",
			found, still, allocator.calls, err)
	}
}

func TestAllocationRequiresExactTicketRosterUnion(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9906)
	seedAllocatingMatch(t, ctx, f, 9906, time.Now().Add(time.Minute).UnixMilli())
	ticket, found, err := f.repo.GetTicket(ctx, 200)
	if err != nil || !found {
		t.Fatalf("get ticket: found=%v err=%v", found, err)
	}
	ticket.Members = ticket.Members[:len(ticket.Members)-1]
	if err := f.repo.ReserveTicket(ctx, ticket, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatal(err)
	}
	allocator := &allocationErrorStub{err: errors.New("must not be called")}
	f.uc.allocator = allocator
	if err := f.uc.advanceAllocationsOnce(ctx); err == nil {
		t.Fatal("ticket roster subset must fail closed")
	}
	if allocator.calls != 0 {
		t.Fatalf("allocator called with non-exact roster graph: calls=%d", allocator.calls)
	}
}

func TestAllocationUnknownOutcomeNeverFailsOrRequeuesMatch(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9903)
	seedAllocatingMatch(t, ctx, f, 9903, time.Now().Add(time.Minute).UnixMilli())
	allocator := &allocationErrorStub{err: errcode.New(errcode.ErrUnavailable, "allocator ACK lost")}
	f.uc.allocator = allocator
	if err := f.uc.advanceAllocationsOnce(ctx); err == nil {
		t.Fatal("unknown allocation outcome must be reported for retry")
	}
	pending, found, err := f.repo.GetMatch(ctx, 9903)
	if err != nil || !found || pending.GetStage() != stageAllocating || allocator.calls != 1 {
		t.Fatalf("unknown outcome was made terminal: found=%v match=%+v calls=%d err=%v",
			found, pending, allocator.calls, err)
	}
	if queued, err := f.repo.RangeQueueTickets(ctx); err != nil || len(queued) != 0 {
		t.Fatalf("unknown outcome requeued players: queue=%v err=%v", queued, err)
	}
}

type failOnceGetMatchRepo struct {
	data.MatchRepo
	fail bool
}

func (r *failOnceGetMatchRepo) GetMatch(ctx context.Context, matchID uint64) (*matchv1.MatchStorageRecord, bool, error) {
	if r.fail {
		r.fail = false
		return nil, false, errors.New("redis read transient")
	}
	return r.MatchRepo.GetMatch(ctx, matchID)
}

func TestRedisTransientNeverRemovesActiveAndCanonicalScanRebuildsIt(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9907)
	seedAllocatingMatch(t, ctx, f, 9907, time.Now().Add(time.Minute).UnixMilli())

	failing := &failOnceGetMatchRepo{MatchRepo: f.repo, fail: true}
	worker := NewMatchUsecase(failing, nil, f.pusher, NewStubDSAllocator("127.0.0.1:7777"),
		&fakeIDGen{next: 10000}, f.locator, f.cfg)
	if err := worker.advanceAllocationsOnce(ctx); err == nil {
		t.Fatal("injected Redis transient was hidden")
	}
	if ids, err := f.repo.RangeActiveMatches(ctx); err != nil || len(ids) != 1 || ids[0] != 9907 {
		t.Fatalf("transient read removed active job: ids=%v err=%v", ids, err)
	}

	if err := f.repo.RemoveActive(ctx, 9907); err != nil {
		t.Fatal(err)
	}
	restarted := NewMatchUsecase(f.repo, nil, f.pusher, NewStubDSAllocator("127.0.0.1:7777"),
		&fakeIDGen{next: 10001}, f.locator, f.cfg)
	if err := restarted.reconcileActiveOnce(ctx); err != nil {
		t.Fatalf("canonical scan failed to rebuild active: %v", err)
	}
	if ids, err := f.repo.RangeActiveMatches(ctx); err != nil || len(ids) != 1 || ids[0] != 9907 {
		t.Fatalf("canonical scan did not rebuild active: ids=%v err=%v", ids, err)
	}
}

func TestLastConfirmationSurvivesProcessCrashBeforeAllocationWorker(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9908)
	for i := uint64(1); i <= 10; i++ {
		f.seedTicket(t, ctx, 100+i, []uint64{i}, 1000)
	}
	allocation := model.BattleAllocation{Address: "10.0.0.8:7777", Target: placement.Target{
		PodName: "battle-confirm", InstanceUID: "uid-confirm", InstanceEpoch: 1,
		AllocationID: "allocation-confirm", ReleaseTrack: "stable",
	}}
	allocator := &switchingAllocationStub{first: allocation, second: allocation}
	f.uc.allocator = allocator
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 10; i++ {
		if err := f.uc.ConfirmMatch(ctx, i, 9908, true); err != nil {
			t.Fatalf("confirm %d: %v", i, err)
		}
	}
	if allocator.allocateCalls != 0 {
		t.Fatalf("Confirm RPC performed external allocation: calls=%d", allocator.allocateCalls)
	}

	restarted := NewMatchUsecase(f.repo, nil, f.pusher, allocator, &fakeIDGen{next: 10000}, f.locator, f.cfg)
	if err := restarted.advanceAllocationsOnce(ctx); err != nil {
		t.Fatalf("restarted allocation worker failed: %v", err)
	}
	ready, found, err := f.repo.GetMatch(ctx, 9908)
	if err != nil || !found || ready.GetStage() != stageReady || allocator.allocateCalls != 1 {
		t.Fatalf("durable confirm handoff lost: found=%v match=%+v calls=%d err=%v",
			found, ready, allocator.allocateCalls, err)
	}
}

type releaseClaimABARepo struct {
	data.MatchRepo
	playerID    uint64
	oldTicketID uint64
	newTicketID uint64
	reads       int
}

func (r *releaseClaimABARepo) GetPlayerTicket(ctx context.Context, playerID uint64) (uint64, bool, error) {
	if playerID == r.playerID {
		r.reads++
		if r.reads == 2 {
			// The release discovery read the exact old edge, then a new StartMatch
			// wins before claim cleanup. Compare-delete must not remove this value.
			if err := r.MatchRepo.DeletePlayerIndexIfMatches(ctx, playerID, r.oldTicketID); err != nil {
				return 0, false, err
			}
			if _, claimed, err := r.MatchRepo.ClaimPlayer(ctx, playerID, r.newTicketID, time.Hour); err != nil || !claimed {
				return 0, false, errors.Join(err, errors.New("inject new claim failed"))
			}
		}
	}
	return r.MatchRepo.GetPlayerTicket(ctx, playerID)
}

func TestReleaseMatchCanonicalMissingDiscoversExactTicketAndProtectsNewClaimABA(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9904)
	const playerID, oldTicketID, newTicketID, oldMatchID = uint64(1), uint64(100), uint64(200), uint64(9904)
	f.seedTicket(t, ctx, oldTicketID, []uint64{playerID}, 1000)
	oldTicket, _, _ := f.repo.GetTicket(ctx, oldTicketID)
	oldTicket.MatchId = oldMatchID
	if err := f.repo.ReserveTicket(ctx, oldTicket, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatal(err)
	}
	// The replacement ticket record can exist before it owns the player claim.
	if err := f.repo.CreateTicketRecord(ctx, &matchv1.MatchTicketStorageRecord{
		TicketId: newTicketID, TeamId: newTicketID, CaptainId: playerID,
		Members:      []*matchv1.MatchMemberStorageRecord{{PlayerId: playerID, TeamId: newTicketID}},
		EnqueuedAtMs: time.Now().UnixMilli(),
	}, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatal(err)
	}

	abaRepo := &releaseClaimABARepo{MatchRepo: f.repo, playerID: playerID,
		oldTicketID: oldTicketID, newTicketID: newTicketID}
	uc := NewMatchUsecase(abaRepo, nil, f.pusher, NewStubDSAllocator("127.0.0.1:7777"),
		&fakeIDGen{next: 10000}, f.locator, f.cfg)
	if err := uc.ReleaseMatch(ctx, oldMatchID, []uint64{playerID}); err != nil {
		t.Fatalf("exact fallback release failed: %v", err)
	}
	if _, found, err := f.repo.GetTicket(ctx, oldTicketID); err != nil || found {
		t.Fatalf("exact old ticket survived: found=%v err=%v", found, err)
	}
	if owner, found, err := f.repo.GetPlayerTicket(ctx, playerID); err != nil || !found || owner != newTicketID {
		t.Fatalf("new ABA claim was deleted: owner=%d found=%v err=%v", owner, found, err)
	}
	if _, found, err := f.repo.GetTicket(ctx, newTicketID); err != nil || !found {
		t.Fatalf("new ABA ticket was deleted: found=%v err=%v", found, err)
	}
}

func TestReleaseMatchCanonicalMissingAmbiguousClaimFailsClosed(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9905)
	const playerID, missingTicketID, matchID = uint64(1), uint64(404), uint64(9905)
	if _, claimed, err := f.repo.ClaimPlayer(ctx, playerID, missingTicketID, f.cfg.TicketTTL.Std()); err != nil || !claimed {
		t.Fatalf("seed ambiguous claim: claimed=%v err=%v", claimed, err)
	}
	err := f.uc.ReleaseMatch(ctx, matchID, []uint64{playerID})
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("ambiguous fallback code=%d err=%v, want ErrUnavailable", errcode.As(err), err)
	}
	if owner, found, getErr := f.repo.GetPlayerTicket(ctx, playerID); getErr != nil || !found || owner != missingTicketID {
		t.Fatalf("ambiguous claim was guessed away: owner=%d found=%v err=%v", owner, found, getErr)
	}
}
