package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/data"
)

// claimCollisionAfterPreflightRepo injects the precise race where StartMatch's
// friendly preflight saw no owner, but another operation wins player discovery
// immediately before this operation commits ACCEPTED.
type claimCollisionAfterPreflightRepo struct {
	data.MatchRepo
	playerID         uint64
	competingTicket  uint64
	injected         bool
	failCompensating int
}

func (r *claimCollisionAfterPreflightRepo) UpdateStartOperationWithLock(
	ctx context.Context,
	ticketID uint64,
	maxRetry int,
	mutate func(*matchv1.MatchStartOperationStorageRecord) error,
	ttl time.Duration,
) error {
	return r.MatchRepo.UpdateStartOperationWithLock(ctx, ticketID, maxRetry,
		func(op *matchv1.MatchStartOperationStorageRecord) error {
			if err := mutate(op); err != nil {
				return err
			}
			if op.GetPhase() == matchv1.MatchStartPhase_MATCH_START_PHASE_COMPENSATING && r.failCompensating > 0 {
				r.failCompensating--
				return errors.New("injected COMPENSATING persistence failure")
			}
			return nil
		}, ttl)
}

func (r *claimCollisionAfterPreflightRepo) CreateStartOperation(
	ctx context.Context,
	op *matchv1.MatchStartOperationStorageRecord,
	ttl time.Duration,
) error {
	if !r.injected {
		r.injected = true
		if _, claimed, err := r.MatchRepo.ClaimStartPlayer(ctx, r.playerID, r.competingTicket, ttl); err != nil || !claimed {
			return err
		}
	}
	return r.MatchRepo.CreateStartOperation(ctx, op, ttl)
}

func TestStartMatchClaimRaceAfterAcceptedReturnsSuccessThenDurablyCompensates(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	const playerID = uint64(4242)
	const ticketID = uint64(7101)
	const competingTicket = uint64(7999)
	repo := &claimCollisionAfterPreflightRepo{
		MatchRepo: f.repo, playerID: playerID, competingTicket: competingTicket, failCompensating: 1,
	}
	uc := NewMatchUsecase(repo, nil, f.pusher, NewStubDSAllocator("127.0.0.1:7777"),
		&fakeIDGen{next: 10000}, f.locator, f.cfg)

	accepted, err := uc.StartMatch(ctx, ticketID, ticketID, playerID, 0, entryUnset)
	if err != nil || accepted != ticketID {
		t.Fatalf("post-commit claim race was reported as rejection: ticket=%d err=%v", accepted, err)
	}
	op, found, err := f.repo.GetStartOperation(ctx, ticketID)
	if err != nil || !found || op.GetPhase() != matchv1.MatchStartPhase_MATCH_START_PHASE_ACCEPTED {
		t.Fatalf("canonical ACCEPTED missing: found=%v op=%+v err=%v", found, op, err)
	}

	if err := uc.advanceStartOperationsOnce(ctx); err == nil {
		t.Fatal("injected COMPENSATING persistence failure was hidden")
	}
	stillAccepted, found, err := f.repo.GetStartOperation(ctx, ticketID)
	if err != nil || !found || stillAccepted.GetPhase() != matchv1.MatchStartPhase_MATCH_START_PHASE_ACCEPTED {
		t.Fatalf("failed phase write manufactured another state: found=%v op=%+v err=%v", found, stillAccepted, err)
	}
	if _, found, err := f.repo.GetTicket(ctx, ticketID); err != nil || found {
		t.Fatalf("ACCEPTED conflict silently entered queue after phase write failure: found=%v err=%v", found, err)
	}
	// Expire the worker lease without waiting 15 seconds, then prove the exact
	// canonical operation is retried into an explicit terminal phase.
	if err := f.repo.UpdateStartOperationWithLock(ctx, ticketID, f.cfg.OptimisticRetry,
		func(op *matchv1.MatchStartOperationStorageRecord) error {
			op.LeaseToken = ""
			op.LeaseDeadlineMs = 0
			op.NextAttemptAtMs = 0
			return nil
		}, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatal(err)
	}
	if err := uc.advanceStartOperationsOnce(ctx); err != nil {
		t.Fatalf("durable compensation retry failed: %v", err)
	}
	failed, found, err := f.repo.GetStartOperation(ctx, ticketID)
	if err != nil || !found || failed.GetPhase() != matchv1.MatchStartPhase_MATCH_START_PHASE_FAILED {
		t.Fatalf("claim collision did not reach explicit terminal phase: found=%v op=%+v err=%v", found, failed, err)
	}
	if owner, found, err := f.repo.GetStartPlayerOperation(ctx, playerID); err != nil || !found || owner != competingTicket {
		t.Fatalf("compensation deleted competing owner: owner=%d found=%v err=%v", owner, found, err)
	}
	if _, found, err := f.repo.GetTicket(ctx, ticketID); err != nil || found {
		t.Fatalf("failed operation left a ticket record: found=%v err=%v", found, err)
	}
	if got := f.pusher.lastStageFor(playerID); got != stageFailed {
		t.Fatalf("accepted race did not publish terminal FAILED: stage=%s", got)
	}
}
