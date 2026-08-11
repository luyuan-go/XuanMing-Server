package biz

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/placement"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/data"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/model"
	"google.golang.org/protobuf/proto"
)

type switchingAllocationStub struct {
	first            model.BattleAllocation
	second           model.BattleAllocation
	allocateCalls    int
	signCalls        int
	signFailFirst    bool
	abortCalls       int
	abortErr         error
	abortMatchID     uint64
	abortOperationID string
	abortAllocation  *model.BattleAllocation
}

func assertAllocationOwnershipIntact(t *testing.T, ctx context.Context, f *fixture, matchID uint64) {
	t.Helper()
	for _, ticketID := range []uint64{100, 200} {
		ticket, found, err := f.repo.GetTicket(ctx, ticketID)
		if err != nil || !found || ticket.GetMatchId() != matchID {
			t.Fatalf("allocation race changed ticket %d: found=%v ticket=%+v err=%v",
				ticketID, found, ticket, err)
		}
	}
	for playerID := uint64(1); playerID <= 10; playerID++ {
		ticketID, found, err := f.repo.GetPlayerTicket(ctx, playerID)
		if err != nil || !found || (ticketID != 100 && ticketID != 200) {
			t.Fatalf("allocation race changed player %d claim: ticket=%d found=%v err=%v",
				playerID, ticketID, found, err)
		}
	}
}

func (s *switchingAllocationStub) AllocateBattle(context.Context, uint64, []uint64, uint32) (*model.BattleAllocation, error) {
	s.allocateCalls++
	allocation := s.first
	if s.allocateCalls > 1 {
		allocation = s.second
	}
	return &allocation, nil
}

// 阵营已是分配的必填输入，分配器不实现 CombatFactionDSAllocator 时 MatchUsecase 定性失败。
// 本桩只需承载该能力，分配行为仍走上面的 first/second 切换逻辑。
func (s *switchingAllocationStub) AllocateBattleWithCombatFactions(
	ctx context.Context, matchID uint64, playerIDs []uint64,
	_ map[uint64]uint32, mapID uint32,
) (*model.BattleAllocation, error) {
	return s.AllocateBattle(ctx, matchID, playerIDs, mapID)
}

func (s *switchingAllocationStub) AbortBattleAllocation(_ context.Context, matchID uint64, operationID string, allocation *model.BattleAllocation) error {
	s.abortCalls++
	s.abortMatchID = matchID
	s.abortOperationID = operationID
	if allocation != nil {
		copy := *allocation
		s.abortAllocation = &copy
	}
	return s.abortErr
}

func (s *switchingAllocationStub) SignBattleTickets(
	_ context.Context,
	matchID uint64,
	playerIDs []uint64,
	_ *model.BattleAllocation,
) (map[uint64]string, error) {
	s.signCalls++
	if s.signFailFirst && s.signCalls == 1 {
		// The real signer may fail after the exact target was checkpointed.
		return nil, errors.New("injected ticket signing failure after checkpoint")
	}
	tickets := make(map[uint64]string, len(playerIDs))
	for _, playerID := range playerIDs {
		tickets[playerID] = fmt.Sprintf("ticket-%d-%d", matchID, playerID)
	}
	return tickets, nil
}

func (*switchingAllocationStub) SignBattleTicket(context.Context, uint64, uint64, *model.BattleAllocation) (string, error) {
	return "ticket", nil
}

func TestAllocationSagaCheckpointsExactTargetBeforeSigningAndRestartReusesIt(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9901)
	seedAllocatingMatch(t, ctx, f, 9901, time.Now().Add(time.Minute).UnixMilli())

	first := model.BattleAllocation{Address: "10.0.0.1:7777", Target: placement.Target{
		PodName: "battle-a", InstanceUID: "uid-a", InstanceEpoch: 1,
		AllocationID: "allocation-a", ReleaseTrack: "stable",
	}}
	second := model.BattleAllocation{Address: "10.0.0.2:7777", Target: placement.Target{
		PodName: "battle-b", InstanceUID: "uid-b", InstanceEpoch: 2,
		AllocationID: "allocation-b", ReleaseTrack: "stable",
	}}
	allocator := &switchingAllocationStub{first: first, second: second, signFailFirst: true}
	f.uc.allocator = allocator

	if err := f.uc.advanceAllocationsOnce(ctx); err == nil {
		t.Fatal("post-checkpoint signing failure must keep the durable job retryable")
	}
	afterFailure, found, err := f.repo.GetMatch(ctx, 9901)
	if err != nil || !found || afterFailure.GetStage() != stageAllocating {
		t.Fatalf("signing failure lost ALLOCATING job: found=%v match=%+v err=%v", found, afterFailure, err)
	}
	checkpoint, complete := allocationFromStoredTarget(afterFailure.GetBattleTarget())
	if !complete || !sameBattleAllocation(checkpoint, &first) {
		t.Fatalf("exact target was not checkpointed before signing: %+v", afterFailure.GetBattleTarget())
	}
	if afterFailure.GetBattleDsAddr() != "" || f.pusher.lastStageFor(1) == stageReady || allocator.signCalls != 1 {
		t.Fatalf("failed signing leaked READY: ds=%q pushed=%s sign_calls=%d",
			afterFailure.GetBattleDsAddr(), f.pusher.lastStageFor(1), allocator.signCalls)
	}

	// Simulate a process restart.  The allocator deliberately returns a different
	// target on its second call; the restarted worker must not call it because the
	// canonical exact target is already checkpointed.
	if err := f.repo.UpdateMatchWithLock(ctx, 9901, f.cfg.OptimisticRetry, func(rec *matchv1.MatchStorageRecord) error {
		rec.AllocationNextAttemptAtMs = 0
		return nil
	}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatal(err)
	}
	restarted := NewMatchUsecase(f.repo, nil, f.pusher, allocator, &fakeIDGen{next: 10000}, f.locator, f.cfg)
	if err := restarted.advanceAllocationsOnce(ctx); err != nil {
		t.Fatalf("restart could not resume exact allocation operation: %v", err)
	}

	ready, found, err := f.repo.GetMatch(ctx, 9901)
	if err != nil || !found || ready.GetStage() != stageReady {
		t.Fatalf("exact retry did not reach READY: found=%v match=%+v err=%v", found, ready, err)
	}
	readyTarget, complete := allocationFromStoredTarget(ready.GetBattleTarget())
	if !complete || !sameBattleAllocation(readyTarget, &first) || ready.GetBattleDsAddr() != first.Address {
		t.Fatalf("READY drifted to a new allocation: %+v", ready.GetBattleTarget())
	}
	if allocator.allocateCalls != 1 || allocator.signCalls != 2 {
		t.Fatalf("unexpected saga calls: allocate=%d sign=%d",
			allocator.allocateCalls, allocator.signCalls)
	}
}

func TestCheckpointBattleAllocationRejectsAbortingEvenForSameTarget(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9909)
	seedAllocatingMatch(t, ctx, f, 9909, time.Now().Add(time.Minute).UnixMilli())
	allocation := &model.BattleAllocation{Address: "10.0.0.9:7777", Target: placement.Target{
		PodName: "battle-checkpoint-abort", InstanceUID: "uid-checkpoint-abort", InstanceEpoch: 9,
		AllocationID: "allocation-checkpoint-abort", ReleaseTrack: "stable"}}
	operationID := allocationOperationID()
	if err := f.repo.UpdateMatchWithLock(ctx, 9909, f.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			rec.AllocationOperationId = operationID
			rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING
			return nil
		}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatal(err)
	}
	job, found, err := f.repo.GetMatch(ctx, 9909)
	if err != nil || !found {
		t.Fatalf("get requesting job: found=%v err=%v", found, err)
	}
	if err := f.repo.UpdateMatchWithLock(ctx, 9909, f.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			rec.BattleTarget = battleTargetStorage(allocation)
			return nil
		}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatal(err)
	}
	if got, err := f.uc.checkpointBattleAllocation(ctx, job, allocation); err != nil ||
		!sameBattleAllocation(got, allocation) {
		t.Fatalf("same REQUESTING checkpoint was not idempotent: got=%+v err=%v", got, err)
	}
	if err := f.repo.UpdateMatchWithLock(ctx, 9909, f.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING
			return nil
		}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatal(err)
	}
	if _, err := f.uc.checkpointBattleAllocation(ctx, job, allocation); errcode.As(err) != errcode.ErrMatchConcurrent {
		t.Fatalf("ABORTING checkpoint code=%d err=%v, want ErrMatchConcurrent", errcode.As(err), err)
	}
	current, found, err := f.repo.GetMatch(ctx, 9909)
	stored, complete := allocationFromStoredTarget(current.GetBattleTarget())
	if err != nil || !found || current.GetStage() != stageAllocating ||
		current.GetAllocationPhase() != matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING ||
		current.GetAllocationOperationId() != operationID || !complete || !sameBattleAllocation(stored, allocation) {
		t.Fatalf("checkpoint overwrote ABORTING: found=%v match=%+v err=%v", found, current, err)
	}
	assertAllocationOwnershipIntact(t, ctx, f, 9909)
}

type abortBeforePostCheckpointFenceRepo struct {
	data.MatchRepo
	matchID   uint64
	triggered atomic.Bool
}

func (r *abortBeforePostCheckpointFenceRepo) UpdateMatchWithLock(
	ctx context.Context,
	matchID uint64,
	maxRetry int,
	mutate func(*matchv1.MatchStorageRecord) error,
	ttl time.Duration,
) error {
	if matchID == r.matchID && !r.triggered.Load() {
		current, found, err := r.MatchRepo.GetMatch(ctx, matchID)
		if err != nil {
			return err
		}
		if found && current.GetStage() == stageAllocating &&
			current.GetAllocationPhase() == matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING &&
			current.GetBattleTarget() != nil && r.triggered.CompareAndSwap(false, true) {
			if err := r.MatchRepo.UpdateMatchWithLock(ctx, matchID, maxRetry,
				func(rec *matchv1.MatchStorageRecord) error {
					rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING
					return nil
				}, ttl); err != nil {
				return err
			}
		}
	}
	return r.MatchRepo.UpdateMatchWithLock(ctx, matchID, maxRetry, mutate, ttl)
}

func TestAbortFenceStopsPostCheckpointExternalSideEffects(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9930)
	seedAllocatingMatch(t, ctx, f, 9930, time.Now().Add(time.Minute).UnixMilli())
	allocation := model.BattleAllocation{Address: "10.0.0.30:7777", Target: placement.Target{
		PodName: "battle-before-signing", InstanceUID: "uid-before-signing", InstanceEpoch: 30,
		AllocationID: "allocation-before-signing", ReleaseTrack: "stable"}}
	allocator := &switchingAllocationStub{first: allocation, second: allocation}
	f.uc.allocator = allocator
	f.uc.repo = &abortBeforePostCheckpointFenceRepo{MatchRepo: f.repo, matchID: 9930}
	initial, found, err := f.repo.GetMatch(ctx, 9930)
	if err != nil || !found {
		t.Fatalf("get initial allocation: found=%v err=%v", found, err)
	}
	if err := f.uc.advanceAllocation(ctx, initial); errcode.As(err) != errcode.ErrMatchConcurrent {
		t.Fatalf("post-checkpoint fence result=%v", err)
	}
	current, found, err := f.repo.GetMatch(ctx, 9930)
	stored, complete := allocationFromStoredTarget(current.GetBattleTarget())
	if err != nil || !found || current.GetStage() != stageAllocating ||
		current.GetAllocationPhase() != matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING ||
		!complete || !sameBattleAllocation(stored, &allocation) || allocator.allocateCalls != 1 ||
		allocator.signCalls != 0 {
		t.Fatalf("abort fence leaked side effects: found=%v match=%+v allocate=%d sign=%d err=%v",
			found, current, allocator.allocateCalls, allocator.signCalls, err)
	}
	assertAllocationOwnershipIntact(t, ctx, f, 9930)
}

type blockingOfflineLocator struct {
	*mockLocator
	started chan struct{}
	proceed chan struct{}
}

func (l *blockingOfflineLocator) FindOfflinePlayers(context.Context, []uint64) ([]uint64, error) {
	close(l.started)
	<-l.proceed
	return []uint64{1}, nil
}

func TestAllocationLivenessFailureCannotOverwriteCheckpointedAbortingGeneration(t *testing.T) {
	ctx := context.Background()
	f := newFixtureWith(t, 9915, func(c *conf.MatchConf) { c.LivenessGateEnabled = true })
	seedAllocatingMatch(t, ctx, f, 9915, time.Now().Add(time.Minute).UnixMilli())
	locator := &blockingOfflineLocator{mockLocator: f.locator,
		started: make(chan struct{}), proceed: make(chan struct{})}
	f.uc.locator = locator
	allocator := &switchingAllocationStub{}
	f.uc.allocator = allocator

	initial, found, err := f.repo.GetMatch(ctx, 9915)
	if err != nil || !found {
		t.Fatalf("get initial allocation: found=%v err=%v", found, err)
	}
	done := make(chan error, 1)
	go func() { done <- f.uc.advanceAllocation(ctx, initial) }()
	select {
	case <-locator.started:
	case <-time.After(2 * time.Second):
		t.Fatal("allocation worker did not reach liveness query")
	}
	allocation := model.BattleAllocation{Address: "10.0.0.15:7777", Target: placement.Target{
		PodName: "battle-liveness-race", InstanceUID: "uid-liveness-race", InstanceEpoch: 15,
		AllocationID: "allocation-liveness-race", ReleaseTrack: "stable"}}
	if err := f.repo.UpdateMatchWithLock(ctx, 9915, f.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			if rec.GetAllocationPhase() != matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING ||
				!placement.ValidOperationID(rec.GetAllocationOperationId()) {
				return errors.New("worker did not checkpoint REQUESTING generation")
			}
			rec.BattleTarget = battleTargetStorage(&allocation)
			rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING
			return nil
		}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatal(err)
	}
	close(locator.proceed)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stale liveness result unexpectedly committed FAILED")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("liveness worker did not return")
	}
	current, found, err := f.repo.GetMatch(ctx, 9915)
	checkpoint, complete := allocationFromStoredTarget(current.GetBattleTarget())
	if err != nil || !found || current.GetStage() != stageAllocating ||
		current.GetAllocationPhase() != matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING ||
		!complete || !sameBattleAllocation(checkpoint, &allocation) || allocator.allocateCalls != 0 {
		t.Fatalf("liveness result overwrote checkpoint/abort fence: found=%v match=%+v allocation=%+v calls=%d err=%v",
			found, current, checkpoint, allocator.allocateCalls, err)
	}
	assertAllocationOwnershipIntact(t, ctx, f, 9915)
}

type blockingDefiniteAllocationStub struct {
	started chan struct{}
	proceed chan struct{}
	err     error
	calls   int
}

func (s *blockingDefiniteAllocationStub) AllocateBattle(context.Context, uint64, []uint64, uint32) (*model.BattleAllocation, error) {
	s.calls++
	close(s.started)
	<-s.proceed
	return nil, s.err
}

func (s *blockingDefiniteAllocationStub) AllocateBattleWithCombatFactions(
	ctx context.Context, matchID uint64, playerIDs []uint64, _ map[uint64]uint32, mapID uint32,
) (*model.BattleAllocation, error) {
	return s.AllocateBattle(ctx, matchID, playerIDs, mapID)
}

func (*blockingDefiniteAllocationStub) AbortBattleAllocation(context.Context, uint64, string, *model.BattleAllocation) error {
	return errors.New("unexpected allocation abort")
}

func (*blockingDefiniteAllocationStub) SignBattleTickets(context.Context, uint64, []uint64, *model.BattleAllocation) (map[uint64]string, error) {
	return nil, errors.New("unexpected ticket signing")
}

func (*blockingDefiniteAllocationStub) SignBattleTicket(context.Context, uint64, uint64, *model.BattleAllocation) (string, error) {
	return "", errors.New("unexpected ticket signing")
}

func TestDefinitiveAllocatorFailureCannotOverwriteChangedGeneration(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(*matchv1.MatchStorageRecord, *model.BattleAllocation)
	}{
		{name: "checkpointed target", mutate: func(rec *matchv1.MatchStorageRecord, allocation *model.BattleAllocation) {
			rec.BattleTarget = battleTargetStorage(allocation)
		}},
		{name: "aborting checkpoint", mutate: func(rec *matchv1.MatchStorageRecord, allocation *model.BattleAllocation) {
			rec.BattleTarget = battleTargetStorage(allocation)
			rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING
		}},
		{name: "operation changed", mutate: func(rec *matchv1.MatchStorageRecord, _ *model.BattleAllocation) {
			rec.AllocationOperationId = allocationOperationID()
		}},
		{name: "phase changed", mutate: func(rec *matchv1.MatchStorageRecord, _ *model.BattleAllocation) {
			rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_PENDING
		}},
	}
	for index, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			matchID := uint64(9920 + index)
			f := newFixture(t, matchID)
			seedAllocatingMatch(t, ctx, f, matchID, time.Now().Add(time.Minute).UnixMilli())
			allocator := &blockingDefiniteAllocationStub{started: make(chan struct{}), proceed: make(chan struct{}),
				err: errcode.New(errcode.ErrDSNoAvailable, "definitive no capacity")}
			f.uc.allocator = allocator
			initial, found, err := f.repo.GetMatch(ctx, matchID)
			if err != nil || !found {
				t.Fatalf("get initial allocation: found=%v err=%v", found, err)
			}
			done := make(chan error, 1)
			go func() { done <- f.uc.advanceAllocation(ctx, initial) }()
			select {
			case <-allocator.started:
			case <-time.After(2 * time.Second):
				t.Fatal("allocation worker did not reach allocator")
			}
			checkpoint := &model.BattleAllocation{Address: "10.0.0.20:7777", Target: placement.Target{
				PodName: "battle-definite-race", InstanceUID: "uid-definite-race", InstanceEpoch: 20,
				AllocationID: "allocation-definite-race", ReleaseTrack: "stable"}}
			if err := f.repo.UpdateMatchWithLock(ctx, matchID, f.cfg.OptimisticRetry,
				func(rec *matchv1.MatchStorageRecord) error {
					if rec.GetAllocationPhase() != matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING ||
						!placement.ValidOperationID(rec.GetAllocationOperationId()) {
						return errors.New("worker did not checkpoint REQUESTING generation")
					}
					tc.mutate(rec, checkpoint)
					return nil
				}, f.cfg.MatchTTL.Std()); err != nil {
				t.Fatal(err)
			}
			before, _, _ := f.repo.GetMatch(ctx, matchID)
			close(allocator.proceed)
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("stale definitive failure unexpectedly committed FAILED")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("allocation worker did not return")
			}
			current, found, err := f.repo.GetMatch(ctx, matchID)
			if err != nil || !found || current.GetStage() != stageAllocating ||
				current.GetAllocationPhase() != before.GetAllocationPhase() ||
				current.GetAllocationOperationId() != before.GetAllocationOperationId() ||
				!proto.Equal(current.GetBattleTarget(), before.GetBattleTarget()) || allocator.calls != 1 {
				t.Fatalf("definitive failure overwrote generation: found=%v before=%+v current=%+v calls=%d err=%v",
					found, before, current, allocator.calls, err)
			}
			assertAllocationOwnershipIntact(t, ctx, f, matchID)
		})
	}
}

type blockingAbortSuccessAllocator struct {
	started    chan struct{}
	proceed    chan struct{}
	calls      int
	allocation *model.BattleAllocation
}

func (*blockingAbortSuccessAllocator) AllocateBattle(context.Context, uint64, []uint64, uint32) (*model.BattleAllocation, error) {
	return nil, errors.New("unexpected allocation")
}

func (*blockingAbortSuccessAllocator) AllocateBattleWithCombatFactions(
	context.Context, uint64, []uint64, map[uint64]uint32, uint32,
) (*model.BattleAllocation, error) {
	return nil, errors.New("unexpected allocation")
}

func (s *blockingAbortSuccessAllocator) AbortBattleAllocation(_ context.Context, _ uint64, _ string, allocation *model.BattleAllocation) error {
	s.calls++
	if allocation != nil {
		copy := *allocation
		s.allocation = &copy
	}
	close(s.started)
	<-s.proceed
	return nil
}

func (*blockingAbortSuccessAllocator) SignBattleTickets(context.Context, uint64, []uint64, *model.BattleAllocation) (map[uint64]string, error) {
	return nil, errors.New("unexpected ticket signing")
}

func (*blockingAbortSuccessAllocator) SignBattleTicket(context.Context, uint64, uint64, *model.BattleAllocation) (string, error) {
	return "", errors.New("unexpected ticket signing")
}

func TestAllocationAbortAckCannotFailDriftedTargetSnapshot(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9925)
	seedAllocatingMatch(t, ctx, f, 9925, time.Now().Add(time.Minute).UnixMilli())
	first := &model.BattleAllocation{Address: "10.0.0.25:7777", Target: placement.Target{
		PodName: "battle-abort-first", InstanceUID: "uid-abort-first", InstanceEpoch: 25,
		AllocationID: "allocation-abort-first", ReleaseTrack: "stable"}}
	second := &model.BattleAllocation{Address: "10.0.0.26:7777", Target: placement.Target{
		PodName: "battle-abort-second", InstanceUID: "uid-abort-second", InstanceEpoch: 26,
		AllocationID: "allocation-abort-second", ReleaseTrack: "stable"}}
	operationID := allocationOperationID()
	if err := f.repo.UpdateMatchWithLock(ctx, 9925, f.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			rec.AllocationOperationId = operationID
			rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING
			rec.BattleTarget = battleTargetStorage(first)
			return nil
		}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatal(err)
	}
	job, found, err := f.repo.GetMatch(ctx, 9925)
	if err != nil || !found {
		t.Fatalf("get abort job: found=%v err=%v", found, err)
	}
	allocator := &blockingAbortSuccessAllocator{started: make(chan struct{}), proceed: make(chan struct{})}
	f.uc.allocator = allocator
	cause := errcode.New(errcode.ErrLocatorConflict, "original placement conflict")
	done := make(chan error, 1)
	go func() { done <- f.uc.advanceAllocationAbort(ctx, job, cause, false) }()
	select {
	case <-allocator.started:
	case <-time.After(2 * time.Second):
		t.Fatal("abort worker did not reach allocator")
	}
	if err := f.repo.UpdateMatchWithLock(ctx, 9925, f.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			rec.BattleTarget = battleTargetStorage(second)
			return nil
		}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatal(err)
	}
	close(allocator.proceed)
	select {
	case err := <-done:
		if errcode.As(err) != errcode.ErrLocatorConflict {
			t.Fatalf("drifted abort result=%v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("abort worker did not return")
	}
	current, found, err := f.repo.GetMatch(ctx, 9925)
	checkpoint, complete := allocationFromStoredTarget(current.GetBattleTarget())
	if err != nil || !found || current.GetStage() != stageAllocating ||
		current.GetAllocationPhase() != matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING ||
		current.GetAllocationOperationId() != operationID || !complete || !sameBattleAllocation(checkpoint, second) ||
		allocator.calls != 1 || !sameBattleAllocation(allocator.allocation, first) {
		t.Fatalf("abort ACK failed drifted generation: found=%v match=%+v sent=%+v calls=%d err=%v",
			found, current, allocator.allocation, allocator.calls, err)
	}
	assertAllocationOwnershipIntact(t, ctx, f, 9925)
}

func TestAllocationAbortUnknownKeepsFailedSagaActiveAndRestartRetriesExactTarget(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9912)
	seedAllocatingMatch(t, ctx, f, 9912, time.Now().Add(time.Minute).UnixMilli())
	allocation := model.BattleAllocation{Address: "10.0.0.12:7777", Target: placement.Target{
		PodName: "battle-abort", InstanceUID: "uid-abort", InstanceEpoch: 4,
		AllocationID: "allocation-abort", ReleaseTrack: "stable"}}
	// A legacy conflict/compensation writer left this generation durably ABORTING
	// with the exact checkpointed target. The abort saga must drive it to FAILED
	// only on a definitive allocator ACK.
	operationID := allocationOperationID()
	if err := f.repo.UpdateMatchWithLock(ctx, 9912, f.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			rec.AllocationOperationId = operationID
			rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING
			rec.BattleTarget = battleTargetStorage(&allocation)
			rec.AllocationNextAttemptAtMs = 0
			return nil
		}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatal(err)
	}
	allocator := &switchingAllocationStub{first: allocation, second: allocation,
		abortErr: errcode.New(errcode.ErrUnavailable, "allocator abort ACK lost")}
	f.uc.allocator = allocator

	if err := f.uc.advanceAllocationsOnce(ctx); err == nil {
		t.Fatal("unknown abort must keep ALLOCATING saga retryable")
	}
	pending, found, err := f.repo.GetMatch(ctx, 9912)
	if err != nil || !found || pending.GetStage() != stageAllocating ||
		pending.GetAllocationPhase() != matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING ||
		pending.GetAllocationNextAttemptAtMs() == -1 || allocator.abortCalls != 1 {
		t.Fatalf("abort UNKNOWN advanced canonical state: found=%v match=%+v calls=%d err=%v",
			found, pending, allocator.abortCalls, err)
	}
	for _, ticketID := range []uint64{100, 200} {
		ticket, ticketFound, ticketErr := f.repo.GetTicket(ctx, ticketID)
		if ticketErr != nil || !ticketFound || ticket.GetMatchId() != 9912 {
			t.Fatalf("abort UNKNOWN changed ticket %d: found=%v ticket=%+v err=%v",
				ticketID, ticketFound, ticket, ticketErr)
		}
	}
	for playerID := uint64(1); playerID <= 10; playerID++ {
		ticketID, claimed, claimErr := f.repo.GetPlayerTicket(ctx, playerID)
		if claimErr != nil || !claimed || (ticketID != 100 && ticketID != 200) {
			t.Fatalf("abort UNKNOWN changed player %d claim: ticket=%d claimed=%v err=%v",
				playerID, ticketID, claimed, claimErr)
		}
	}
	active, activeErr := f.repo.RangeActiveMatches(ctx)
	if activeErr != nil || !slices.Contains(active, uint64(9912)) {
		t.Fatalf("abort UNKNOWN removed active recovery job: active=%v err=%v", active, activeErr)
	}
	firstOperation := allocator.abortOperationID
	if firstOperation != operationID || !sameBattleAllocation(allocator.abortAllocation, &allocation) ||
		allocator.signCalls != 0 || f.pusher.lastStageFor(1) == stageReady {
		t.Fatalf("abort UNKNOWN drift/leak: op=%q allocation=%+v sign=%d stage=%s",
			firstOperation, allocator.abortAllocation, allocator.signCalls, f.pusher.lastStageFor(1))
	}

	// Simulate a restarted service worker after the remote allocator journal has
	// recognized the first call despite its lost response.
	allocator.abortErr = nil
	if err := f.repo.UpdateMatchWithLock(ctx, 9912, f.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			rec.AllocationNextAttemptAtMs = 0
			return nil
		}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatal(err)
	}
	restarted := NewMatchUsecase(f.repo, nil, f.pusher, allocator,
		&fakeIDGen{next: 20000}, f.locator, f.cfg)
	if err := restarted.advanceAllocationsOnce(ctx); err != nil {
		t.Fatalf("restart abort retry: %v", err)
	}
	finished, found, err := f.repo.GetMatch(ctx, 9912)
	if err != nil || !found || finished.GetStage() != stageFailed ||
		finished.GetAllocationNextAttemptAtMs() != -1 || allocator.abortCalls != 2 ||
		allocator.abortOperationID != firstOperation || !sameBattleAllocation(allocator.abortAllocation, &allocation) {
		t.Fatalf("restart did not finish exact abort: found=%v match=%+v calls=%d op=%q allocation=%+v err=%v",
			found, finished, allocator.abortCalls, allocator.abortOperationID, allocator.abortAllocation, err)
	}
	for _, ticketID := range []uint64{100, 200} {
		if _, ticketFound, ticketErr := f.repo.GetTicket(ctx, ticketID); ticketErr != nil || ticketFound {
			t.Fatalf("definitive abort retained ticket %d: found=%v err=%v", ticketID, ticketFound, ticketErr)
		}
	}
}

type abortReadyRaceAllocator struct {
	allocation   model.BattleAllocation
	signStarted  chan struct{}
	signProceed  chan struct{}
	abortStarted chan struct{}
	abortProceed chan struct{}
	signCalls    atomic.Int32
	abortCalls   atomic.Int32
}

func (a *abortReadyRaceAllocator) AllocateBattle(context.Context, uint64, []uint64, uint32) (*model.BattleAllocation, error) {
	copy := a.allocation
	return &copy, nil
}

func (a *abortReadyRaceAllocator) AllocateBattleWithCombatFactions(
	ctx context.Context, matchID uint64, playerIDs []uint64, _ map[uint64]uint32, mapID uint32,
) (*model.BattleAllocation, error) {
	return a.AllocateBattle(ctx, matchID, playerIDs, mapID)
}

func (a *abortReadyRaceAllocator) AbortBattleAllocation(context.Context, uint64, string, *model.BattleAllocation) error {
	if a.abortCalls.Add(1) == 1 {
		close(a.abortStarted)
	}
	<-a.abortProceed
	return errcode.New(errcode.ErrUnavailable, "abort response unknown")
}

func (a *abortReadyRaceAllocator) SignBattleTickets(
	_ context.Context, matchID uint64, playerIDs []uint64, _ *model.BattleAllocation,
) (map[uint64]string, error) {
	if a.signCalls.Add(1) == 1 {
		close(a.signStarted)
	}
	<-a.signProceed
	tickets := make(map[uint64]string, len(playerIDs))
	for _, playerID := range playerIDs {
		tickets[playerID] = fmt.Sprintf("race-ticket-%d-%d", matchID, playerID)
	}
	return tickets, nil
}

func (*abortReadyRaceAllocator) SignBattleTicket(context.Context, uint64, uint64, *model.BattleAllocation) (string, error) {
	return "ticket", nil
}

func TestAllocationAbortingFencePreventsConcurrentReadyCAS(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9913)
	seedAllocatingMatch(t, ctx, f, 9913, time.Now().Add(time.Minute).UnixMilli())
	allocation := model.BattleAllocation{Address: "10.0.0.13:7777", Target: placement.Target{
		PodName: "battle-race", InstanceUID: "uid-race", InstanceEpoch: 5,
		AllocationID: "allocation-race", ReleaseTrack: "stable"}}
	allocator := &abortReadyRaceAllocator{
		allocation: allocation, signStarted: make(chan struct{}), signProceed: make(chan struct{}),
		abortStarted: make(chan struct{}), abortProceed: make(chan struct{}),
	}
	f.uc.allocator = allocator

	first, found, err := f.repo.GetMatch(ctx, 9913)
	if err != nil || !found {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- f.uc.advanceAllocation(ctx, first) }()
	select {
	case <-allocator.signStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first worker never reached ticket signing")
	}
	// A concurrent compensation authority fences this generation into ABORTING
	// while the first worker holds locally signed tickets but has not reached
	// the READY CAS.
	if err := f.repo.UpdateMatchWithLock(ctx, 9913, f.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			if rec.GetBattleTarget() == nil ||
				rec.GetAllocationPhase() != matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING {
				return errors.New("first worker did not checkpoint REQUESTING")
			}
			rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING
			rec.AllocationNextAttemptAtMs = 0
			return nil
		}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatal(err)
	}
	second, found, err := f.repo.GetMatch(ctx, 9913)
	if err != nil || !found {
		t.Fatal(err)
	}
	secondDone := make(chan error, 1)
	go func() { secondDone <- f.uc.advanceAllocation(ctx, second) }()
	select {
	case <-allocator.abortStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("second worker never fenced/started abort")
	}

	close(allocator.signProceed)
	select {
	case readyErr := <-firstDone:
		if readyErr == nil {
			t.Fatal("stale worker unexpectedly committed READY after ABORTING fence")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stale READY worker did not return")
	}
	pending, found, err := f.repo.GetMatch(ctx, 9913)
	if err != nil || !found || pending.GetStage() != stageAllocating ||
		pending.GetAllocationPhase() != matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING ||
		pending.GetBattleDsAddr() != "" || f.pusher.lastStageFor(1) == stageReady {
		t.Fatalf("ABORTING fence leaked READY: found=%v match=%+v pushed=%s err=%v",
			found, pending, f.pusher.lastStageFor(1), err)
	}
	close(allocator.abortProceed)
	select {
	case abortErr := <-secondDone:
		if abortErr == nil {
			t.Fatal("injected unknown abort unexpectedly succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("abort worker did not return")
	}
	for _, ticketID := range []uint64{100, 200} {
		ticket, ticketFound, ticketErr := f.repo.GetTicket(ctx, ticketID)
		if ticketErr != nil || !ticketFound || ticket.GetMatchId() != 9913 {
			t.Fatalf("ABORTING UNKNOWN changed ticket %d: found=%v ticket=%+v err=%v",
				ticketID, ticketFound, ticket, ticketErr)
		}
	}
}

type incompleteTicketSetAllocator struct {
	allocation model.BattleAllocation
	signCalls  int
}

func (a *incompleteTicketSetAllocator) AllocateBattle(context.Context, uint64, []uint64, uint32) (*model.BattleAllocation, error) {
	allocation := a.allocation
	return &allocation, nil
}

func (a *incompleteTicketSetAllocator) AllocateBattleWithCombatFactions(
	ctx context.Context, matchID uint64, playerIDs []uint64, _ map[uint64]uint32, mapID uint32,
) (*model.BattleAllocation, error) {
	return a.AllocateBattle(ctx, matchID, playerIDs, mapID)
}

func (*incompleteTicketSetAllocator) AbortBattleAllocation(context.Context, uint64, string, *model.BattleAllocation) error {
	return errors.New("unexpected allocation abort")
}

func (a *incompleteTicketSetAllocator) SignBattleTickets(
	_ context.Context, matchID uint64, playerIDs []uint64, _ *model.BattleAllocation,
) (map[uint64]string, error) {
	a.signCalls++
	tickets := make(map[uint64]string, len(playerIDs)-1)
	for _, playerID := range playerIDs[:len(playerIDs)-1] {
		tickets[playerID] = fmt.Sprintf("ticket-%d-%d", matchID, playerID)
	}
	return tickets, nil
}

func (*incompleteTicketSetAllocator) SignBattleTicket(context.Context, uint64, uint64, *model.BattleAllocation) (string, error) {
	return "", errors.New("unexpected single ticket signing")
}

func TestAllocationSagaRejectsIncompleteSignedTicketSet(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 9902)
	seedAllocatingMatch(t, ctx, f, 9902, time.Now().Add(time.Minute).UnixMilli())
	allocator := &incompleteTicketSetAllocator{allocation: model.BattleAllocation{
		Address: "10.0.0.3:7777",
		Target: placement.Target{PodName: "battle-c", InstanceUID: "uid-c", InstanceEpoch: 3,
			AllocationID: "allocation-c", ReleaseTrack: "stable"},
	}}
	f.uc.allocator = allocator

	if err := f.uc.advanceAllocationsOnce(ctx); err == nil {
		t.Fatal("incomplete ticket set must fail closed")
	}
	match, found, err := f.repo.GetMatch(ctx, 9902)
	if err != nil || !found || match.GetStage() != stageAllocating || match.GetBattleDsAddr() != "" {
		t.Fatalf("incomplete ticket set leaked READY: found=%v match=%+v err=%v", found, match, err)
	}
	if allocator.signCalls != 1 || f.pusher.lastStageFor(1) == stageReady {
		t.Fatalf("incomplete ticket set reached push: sign_calls=%d stage=%s",
			allocator.signCalls, f.pusher.lastStageFor(1))
	}
}
