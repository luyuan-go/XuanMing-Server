// match_test.go — matchmaker biz 层撮合流水线测试(miniredis 真实跑通)。
package biz

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/placement"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"

	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/data"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/model"
)

// ── 测试桩 ────────────────────────────────────────────────────────────────────

type mockPusher struct {
	mu      sync.Mutex
	events  []*matchv1.MatchProgressEvent
	failErr error // 非 nil 时 PushMatchProgress 一律失败(模拟 Kafka 不可用)
}

// failWith 设置 / 清除注入的推送失败(nil = 恢复正常)。
func (m *mockPusher) failWith(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failErr = err
}

// readyTicketFor 返回该玩家最近一条 READY 推送里的 battle_ticket。
func (m *mockPusher) readyTicketFor(playerID uint64) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.events) - 1; i >= 0; i-- {
		e := m.events[i]
		if e.ToPlayerId == playerID && e.Progress != nil && e.Progress.Stage == stageReady {
			return e.Progress.BattleTicket, true
		}
	}
	return "", false
}

type captureResumeAllocator struct {
	DSAllocator
	playerID   uint64
	matchID    uint64
	allocation model.BattleAllocation
}

func (a *captureResumeAllocator) SignBattleTicket(
	_ context.Context,
	playerID, matchID uint64,
	allocation *model.BattleAllocation,
) (string, error) {
	a.playerID, a.matchID = playerID, matchID
	if allocation != nil {
		a.allocation = *allocation
	}
	return "canonical-ready-resume-ticket", nil
}

func (m *mockPusher) PushMatchProgress(_ context.Context, _ uint64, to []uint64, payload []byte) (int, error) {
	m.mu.Lock()
	if m.failErr != nil {
		err := m.failErr
		m.mu.Unlock()
		return 0, err
	}
	m.mu.Unlock()
	var e matchv1.MatchProgressEvent
	if err := proto.Unmarshal(payload, &e); err == nil {
		m.mu.Lock()
		m.events = append(m.events, &e)
		m.mu.Unlock()
	}
	return len(to), nil
}

func (m *mockPusher) lastStageFor(playerID uint64) matchv1.MatchStage {
	m.mu.Lock()
	defer m.mu.Unlock()
	stage := matchv1.MatchStage_MATCH_STAGE_UNSPECIFIED
	for _, e := range m.events {
		if e.ToPlayerId == playerID && e.Progress != nil {
			stage = e.Progress.Stage
		}
	}
	return stage
}

// fakeIDGen 返回可预测的 match_id 序列。
type fakeIDGen struct {
	mu   sync.Mutex
	next uint64
}

func (f *fakeIDGen) Generate() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.next
	f.next++
	return id
}

// mockLocator 记录 matchmaker 上报的 MATCHING / BATTLE 状态，用于断言状态机串联。
type mockLocator struct {
	mu       sync.Mutex
	matching map[uint64]uint64 // playerID -> matchID
	battle   map[uint64]string // playerID -> battlePod
	inBattle map[uint64]bool   // playerID -> 强制 IsInBattle 返回值(拦截测试用)
	queryErr error             // 非 nil 时 IsInBattle 一律返回该错误(模拟 locator 抖动 / fail-closed 测试用)
	offline  map[uint64]bool   // playerID -> 强制 FindOfflinePlayers 判离线(成局前在线校验测试用)
}

func newMockLocator() *mockLocator {
	return &mockLocator{matching: map[uint64]uint64{}, battle: map[uint64]string{}, inBattle: map[uint64]bool{}, offline: map[uint64]bool{}}
}

func (m *mockLocator) NotifyMatching(_ context.Context, ids []uint64, matchID uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		m.matching[id] = matchID
	}
	return nil
}

func (m *mockLocator) NotifyBattle(_ context.Context, ids []uint64, matchID uint64, pod string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range ids {
		m.battle[id] = pod
	}
	return nil
}

func (m *mockLocator) IsInBattle(_ context.Context, id uint64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queryErr != nil {
		return false, m.queryErr
	}
	return m.inBattle[id], nil
}

func (m *mockLocator) FindOfflinePlayers(_ context.Context, ids []uint64) ([]uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.queryErr != nil {
		return nil, m.queryErr
	}
	var out []uint64
	for _, id := range ids {
		if m.offline[id] {
			out = append(out, id)
		}
	}
	return out, nil
}

func (m *mockLocator) matchingOf(id uint64) (uint64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.matching[id]
	return v, ok
}

func (m *mockLocator) battleOf(id uint64) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.battle[id]
	return v, ok
}

// ── 测试夹具 ──────────────────────────────────────────────────────────────────

type fixture struct {
	repo    *data.RedisMatchRepo
	pusher  *mockPusher
	locator *mockLocator
	uc      *MatchUsecase
	cfg     conf.MatchConf
}

// progressHandoffRepo 在 GetMatchProgress 完成第一次 Match/Ticket miss 后，
// 精确模拟 worker 的“先写 Ticket、后删 StartOperation”交接。
type progressHandoffRepo struct {
	data.MatchRepo
	op   *matchv1.MatchStartOperationStorageRecord
	ttl  time.Duration
	once sync.Once
	err  error
}

func (r *progressHandoffRepo) GetStartOperation(
	ctx context.Context,
	ticketID uint64,
) (*matchv1.MatchStartOperationStorageRecord, bool, error) {
	r.once.Do(func() {
		if r.op == nil || r.op.GetTicketId() != ticketID {
			r.err = errors.New("handoff test operation mismatch")
			return
		}
		if err := r.MatchRepo.CreateTicketRecord(ctx, ticketFromStartOperation(r.op), r.ttl); err != nil {
			r.err = err
			return
		}
		for _, member := range r.op.GetMembers() {
			if err := r.MatchRepo.DeleteStartPlayerIfMatches(ctx, member.GetPlayerId(), ticketID); err != nil {
				r.err = err
				return
			}
		}
		r.err = r.MatchRepo.DeleteStartOperation(ctx, ticketID)
	})
	if r.err != nil {
		return nil, false, r.err
	}
	return r.MatchRepo.GetStartOperation(ctx, ticketID)
}

func newFixture(t *testing.T, firstMatchID uint64) *fixture {
	return newFixtureWith(t, firstMatchID, nil)
}

// newFixtureWith 与 newFixture 相同，但允许在构造 usecase 前修改 MatchConf（如打开 BattleGateFailOpen）。
func newFixtureWith(t *testing.T, firstMatchID uint64, mutate func(*conf.MatchConf)) *fixture {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis run: %v", err)
	}
	t.Cleanup(mr.Close)

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	var c conf.Config
	c.Defaults()
	if mutate != nil {
		mutate(&c.Match)
	}
	repo := data.NewRedisMatchRepo(rdb, "")
	pusher := &mockPusher{}
	locator := newMockLocator()
	idGen := &fakeIDGen{next: firstMatchID}
	uc := NewMatchUsecase(repo, nil, pusher, NewStubDSAllocator("127.0.0.1:7777"), idGen, locator, c.Match)
	return &fixture{repo: repo, pusher: pusher, locator: locator, uc: uc, cfg: c.Match}
}

func TestResolvePlayerMatchContext_StartSagaHandsOffToQueued(t *testing.T) {
	f := newFixture(t, 9000)
	ctx := context.Background()
	const playerID = uint64(4201)
	if _, err := f.uc.StartMatch(ctx, 9101, 9101, playerID, 0); err != nil {
		t.Fatal(err)
	}
	starting, err := f.uc.ResolvePlayerMatchContext(ctx, playerID)
	if err != nil || starting.GetState() != matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE ||
		starting.GetStage() != matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_STARTING || starting.GetTicketId() != 9101 {
		t.Fatalf("STARTING context = %+v err=%v", starting, err)
	}
	if err := f.uc.advanceStartOperationsOnce(ctx); err != nil {
		t.Fatal(err)
	}
	queued, err := f.uc.ResolvePlayerMatchContext(ctx, playerID)
	if err != nil || queued.GetState() != matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE ||
		queued.GetStage() != matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_QUEUED || queued.GetTicketId() != 9101 {
		t.Fatalf("QUEUED context = %+v err=%v", queued, err)
	}
	if _, found, err := f.repo.GetStartOperation(ctx, 9101); err != nil || found {
		t.Fatalf("queued handoff must explicitly delete start operation: found=%v err=%v", found, err)
	}
}

// failingResignAllocator:SignBattleTicket 恒失败(签发链在场但故障),其余委托 stub。
type failingResignAllocator struct {
	DSAllocator
}

func (a *failingResignAllocator) SignBattleTicket(context.Context, uint64, uint64, *model.BattleAllocation) (string, error) {
	return "", errcode.New(errcode.ErrUnavailable, "signer down")
}

// R7 收口(P1):READY 重签失败必须让查询 fail-closed(可重试),绝不降级返回存储旧票
// ——存量票绑定 claim 时刻的 sjti,顶号换机后必被 DS 兑换点拒绝,交出去只会让客户端
// 拿废票撞墙。签发恢复后查询即拿到新 jti 票。
func TestGetMatchProgress_ResignFailureWithholdsStoredTicket(t *testing.T) {
	f := newFixture(t, 9600)
	ctx := context.Background()
	const (
		playerID = uint64(4601)
		ticketID = uint64(9611)
		matchID  = uint64(9601)
	)
	m := &matchv1.MatchStorageRecord{
		MatchId: matchID, Stage: stageReady,
		TicketIds:    []uint64{ticketID},
		Members:      []*matchv1.MatchMemberStorageRecord{{PlayerId: playerID, TeamId: ticketID}},
		BattleDsAddr: "10.0.0.9:7777",
		BattleTicket: "stale-stored-ticket-bound-to-old-sjti",
		BattleTarget: &matchv1.MatchBattleTargetStorageRecord{
			DsAddr: "10.0.0.9:7777", DsPodName: "battle-1", DsInstanceUid: "uid-b1",
			DsInstanceEpoch: 3, AllocationId: "alloc-1", ReleaseTrack: "stable",
		},
	}
	if err := f.repo.CreateMatch(ctx, m, time.Hour); err != nil {
		t.Fatal(err)
	}

	// ① 签发失败:查询整体 fail-closed,旧票绝不外泄。
	failUC := NewMatchUsecase(f.repo, nil, f.pusher, &failingResignAllocator{DSAllocator: NewStubDSAllocator("127.0.0.1:7777")}, &fakeIDGen{next: 1}, nil, f.cfg)
	prog, err := failUC.GetMatchProgress(ctx, playerID, matchID)
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("resign failure must fail the query, prog=%+v code=%v err=%v", prog, errcode.As(err), err)
	}
	if prog != nil {
		t.Fatalf("no progress (with stale ticket) may leak on resign failure, got %+v", prog)
	}

	// ② 签发正常(stub):现签新票覆盖存储旧票。
	prog, err = f.uc.GetMatchProgress(ctx, playerID, matchID)
	if err != nil {
		t.Fatalf("healthy resign query err: %v", err)
	}
	if prog.GetBattleTicket() == "" || prog.GetBattleTicket() == "stale-stored-ticket-bound-to-old-sjti" {
		t.Fatalf("healthy path must deliver a freshly signed ticket, got %q", prog.GetBattleTicket())
	}
}

func TestGetMatchProgress_ReadsAcceptedDurableStartOperationBeforeTicketExists(t *testing.T) {
	f := newFixture(t, 9000)
	ctx := context.Background()
	const (
		playerID = uint64(4211)
		ticketID = uint64(9111)
	)
	if _, err := f.uc.StartMatch(ctx, ticketID, ticketID, playerID, 7); err != nil {
		t.Fatal(err)
	}
	if _, found, err := f.repo.GetTicket(ctx, ticketID); err != nil || found {
		t.Fatalf("worker 前不应已有 canonical ticket: found=%v err=%v", found, err)
	}

	progress, err := f.uc.GetMatchProgress(ctx, playerID, ticketID)
	if err != nil {
		t.Fatalf("accepted start operation 不应瞬时返回 4001: %v", err)
	}
	if progress.GetMatchId() != ticketID || progress.GetStage() != stageQueueing {
		t.Fatalf("progress=%+v, want ticket=%d stage=QUEUEING", progress, ticketID)
	}
	withoutHandle, err := f.uc.GetMatchProgress(ctx, playerID, 0)
	if err != nil || withoutHandle.GetMatchId() != ticketID || withoutHandle.GetStage() != stageQueueing {
		t.Fatalf("丢失句柄后的 durable start 恢复 = %+v err=%v", withoutHandle, err)
	}

	if _, err := f.uc.GetMatchProgress(ctx, playerID+1, ticketID); errcode.As(err) != errcode.ErrMatchNotFound {
		t.Fatalf("非成员读取 start operation = %v, want ErrMatchNotFound", err)
	}
}

func TestGetMatchProgress_ProjectsEveryDurableStartPhase(t *testing.T) {
	tests := []struct {
		phase     matchv1.MatchStartPhase
		wantStage matchv1.MatchStage
		wantCode  errcode.Code
	}{
		{matchv1.MatchStartPhase_MATCH_START_PHASE_ACCEPTED, stageQueueing, errcode.OK},
		{matchv1.MatchStartPhase_MATCH_START_PHASE_TICKET_READY, stageQueueing, errcode.OK},
		{matchv1.MatchStartPhase_MATCH_START_PHASE_CLAIMING, stageQueueing, errcode.OK},
		{matchv1.MatchStartPhase_MATCH_START_PHASE_CLAIMS_READY, stageQueueing, errcode.OK},
		{matchv1.MatchStartPhase_MATCH_START_PHASE_QUEUED, stageQueueing, errcode.OK},
		{matchv1.MatchStartPhase_MATCH_START_PHASE_COMPENSATING, stageFailed, errcode.OK},
		{matchv1.MatchStartPhase_MATCH_START_PHASE_FAILED, stageFailed, errcode.OK},
		{matchv1.MatchStartPhase_MATCH_START_PHASE_UNSPECIFIED, 0, errcode.ErrUnavailable},
	}
	for i, tc := range tests {
		tc := tc
		t.Run(tc.phase.String(), func(t *testing.T) {
			f := newFixture(t, 9400+uint64(i))
			ctx := context.Background()
			playerID := uint64(4400 + i)
			ticketID := uint64(9500 + i)
			if _, err := f.uc.StartMatch(ctx, ticketID, ticketID, playerID, 0); err != nil {
				t.Fatal(err)
			}
			if err := f.repo.UpdateStartOperationWithLock(ctx, ticketID, f.cfg.OptimisticRetry,
				func(rec *matchv1.MatchStartOperationStorageRecord) error {
					rec.Phase = tc.phase
					return nil
				}, f.uc.ticketTTL()); err != nil {
				t.Fatal(err)
			}

			progress, err := f.uc.GetMatchProgress(ctx, playerID, ticketID)
			if tc.wantCode != errcode.OK {
				if errcode.As(err) != tc.wantCode {
					t.Fatalf("phase=%s err=%v, want code=%d", tc.phase, err, tc.wantCode)
				}
				return
			}
			if err != nil || progress.GetMatchId() != ticketID || progress.GetStage() != tc.wantStage {
				t.Fatalf("phase=%s progress=%+v err=%v", tc.phase, progress, err)
			}
		})
	}
}

func TestGetMatchProgress_ClosesStartOperationToTicketHandoffRace(t *testing.T) {
	f := newFixture(t, 9600)
	ctx := context.Background()
	const (
		playerID = uint64(4501)
		ticketID = uint64(9601)
	)
	if _, err := f.uc.StartMatch(ctx, ticketID, ticketID, playerID, 0); err != nil {
		t.Fatal(err)
	}
	op, found, err := f.repo.GetStartOperation(ctx, ticketID)
	if err != nil || !found {
		t.Fatalf("start operation missing: found=%v err=%v", found, err)
	}
	f.uc.repo = &progressHandoffRepo{MatchRepo: f.repo, op: op, ttl: f.uc.ticketTTL()}

	progress, err := f.uc.GetMatchProgress(ctx, playerID, ticketID)
	if err != nil || progress.GetMatchId() != ticketID || progress.GetStage() != stageQueueing {
		t.Fatalf("handoff race returned progress=%+v err=%v", progress, err)
	}
}

func TestGetMatchProgress_AuthorizesStartOperationMemberBeforeMode(t *testing.T) {
	f := newFixture(t, 9700)
	ctx := context.Background()
	const (
		playerID = uint64(4601)
		ticketID = uint64(9701)
	)
	if _, err := f.uc.StartMatch(ctx, ticketID, ticketID, playerID, 0); err != nil {
		t.Fatal(err)
	}
	if err := f.repo.UpdateStartOperationWithLock(ctx, ticketID, f.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStartOperationStorageRecord) error {
			rec.GameMode = "foreign-mode"
			return nil
		}, f.uc.ticketTTL()); err != nil {
		t.Fatal(err)
	}

	if _, err := f.uc.GetMatchProgress(ctx, playerID+1, ticketID); errcode.As(err) != errcode.ErrMatchNotFound {
		t.Fatalf("非成员在 mode 校验前未被隐藏: %v", err)
	}
	if _, err := f.uc.GetMatchProgress(ctx, playerID, ticketID); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("成员读取 foreign mode = %v, want ErrInvalidState", err)
	}
}

func TestCancelMatchCommitsAgainstEveryPreQueueStartPhase(t *testing.T) {
	preQueuePhases := []matchv1.MatchStartPhase{
		matchv1.MatchStartPhase_MATCH_START_PHASE_ACCEPTED,
		matchv1.MatchStartPhase_MATCH_START_PHASE_TICKET_READY,
		matchv1.MatchStartPhase_MATCH_START_PHASE_CLAIMING,
		matchv1.MatchStartPhase_MATCH_START_PHASE_CLAIMS_READY,
	}
	for i, phase := range preQueuePhases {
		phase := phase
		t.Run(phase.String(), func(t *testing.T) {
			f := newFixture(t, 9200+uint64(i))
			ctx := context.Background()
			playerID := uint64(4300 + i)
			ticketID := uint64(9300 + i)
			if _, err := f.uc.StartMatch(ctx, ticketID, ticketID, playerID, 0); err != nil {
				t.Fatal(err)
			}
			op, found, err := f.repo.GetStartOperation(ctx, ticketID)
			if err != nil || !found {
				t.Fatalf("start operation missing: found=%v err=%v", found, err)
			}
			if phase != matchv1.MatchStartPhase_MATCH_START_PHASE_ACCEPTED {
				if err := f.repo.CreateTicketRecord(ctx, ticketFromStartOperation(op), f.uc.ticketTTL()); err != nil {
					t.Fatal(err)
				}
			}
			if phase == matchv1.MatchStartPhase_MATCH_START_PHASE_CLAIMING ||
				phase == matchv1.MatchStartPhase_MATCH_START_PHASE_CLAIMS_READY {
				if owner, claimed, err := f.repo.ClaimPlayer(ctx, playerID, ticketID, f.uc.ticketTTL()); err != nil || (!claimed && owner != ticketID) {
					t.Fatalf("claim player: owner=%d claimed=%v err=%v", owner, claimed, err)
				}
			}
			if err := f.repo.UpdateStartOperationWithLock(ctx, ticketID, f.cfg.OptimisticRetry,
				func(rec *matchv1.MatchStartOperationStorageRecord) error {
					rec.Phase = phase
					rec.LeaseToken = "worker-that-must-be-fenced"
					rec.LeaseDeadlineMs = time.Now().Add(time.Minute).UnixMilli()
					return nil
				}, f.uc.ticketTTL()); err != nil {
				t.Fatal(err)
			}

			if err := f.uc.CancelMatch(ctx, playerID); err != nil {
				t.Fatalf("cancel STARTING phase %s: %v", phase, err)
			}
			cancelled, found, err := f.repo.GetStartOperation(ctx, ticketID)
			if err != nil || !found {
				t.Fatalf("cancelled operation missing: found=%v err=%v", found, err)
			}
			if cancelled.GetPhase() != matchv1.MatchStartPhase_MATCH_START_PHASE_COMPENSATING ||
				cancelled.GetLeaseToken() != "" || cancelled.GetLeaseDeadlineMs() != 0 {
				t.Fatalf("cancel did not durably fence worker: %+v", cancelled)
			}
			resume, err := f.uc.ResolvePlayerMatchContext(ctx, playerID)
			if err != nil || resume.GetState() != matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_UNSPECIFIED {
				t.Fatalf("compensating resume must remain UNKNOWN: %+v err=%v", resume, err)
			}

			if err := f.uc.advanceStartOperation(ctx, cancelled); err != nil {
				t.Fatalf("advance compensation: %v", err)
			}
			failed, found, err := f.repo.GetStartOperation(ctx, ticketID)
			if err != nil || !found || failed.GetPhase() != matchv1.MatchStartPhase_MATCH_START_PHASE_FAILED {
				t.Fatalf("operation not terminal FAILED: found=%v op=%+v err=%v", found, failed, err)
			}
			if owner, found, err := f.repo.GetStartPlayerOperation(ctx, playerID); err != nil || found {
				t.Fatalf("start-player index survived: owner=%d found=%v err=%v", owner, found, err)
			}
			if owner, found, err := f.repo.GetPlayerTicket(ctx, playerID); err != nil || found {
				t.Fatalf("player claim survived: owner=%d found=%v err=%v", owner, found, err)
			}
			if _, found, err := f.repo.GetTicket(ctx, ticketID); err != nil || found {
				t.Fatalf("ticket survived compensation: found=%v err=%v", found, err)
			}
		})
	}
}

func TestResolvePlayerMatchContext_CrossModeCanonicalAndDrift(t *testing.T) {
	ctx := context.Background()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	pveRepo := data.NewRedisMatchRepo(rdb, "pve_coop")
	pvpRepo := data.NewRedisMatchRepo(rdb, "pvp")
	var c conf.Config
	c.Defaults()
	resumeAllocator := &captureResumeAllocator{DSAllocator: NewStubDSAllocator("127.0.0.1:7777")}
	resolver := NewMatchUsecase(pvpRepo, nil, nil, resumeAllocator, &fakeIDGen{next: 1}, nil, c.Match)

	// Login intentionally talks to the PVP-audience endpoint only. Start/player
	// discovery keys are global canonical records, so that reader must still see
	// a PVE operation before its Ticket exists; a second audience/query is not
	// required and would turn identical shared authority into false split-brain.
	const pveStartingPlayer, pveStartingTicket = uint64(51), uint64(9199)
	if err := pveRepo.CreateStartOperation(ctx, &matchv1.MatchStartOperationStorageRecord{
		OperationId: "9849ab5b-2ecf-4fc3-983d-2d8df53cc008",
		TicketId:    pveStartingTicket, TeamId: pveStartingTicket, CaptainId: pveStartingPlayer,
		Members:  []*matchv1.MatchMemberStorageRecord{{PlayerId: pveStartingPlayer, TeamId: pveStartingTicket}},
		Phase:    matchv1.MatchStartPhase_MATCH_START_PHASE_ACCEPTED,
		GameMode: "pve_coop",
	}, time.Hour); err != nil {
		t.Fatal(err)
	}
	starting, err := resolver.ResolvePlayerMatchContext(ctx, pveStartingPlayer)
	if err != nil || starting.GetStage() != matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_STARTING ||
		starting.GetTicketId() != pveStartingTicket || starting.GetGameMode() != "pve_coop" {
		t.Fatalf("cross-mode PVE STARTING = %+v err=%v", starting, err)
	}
	if err := pveRepo.DeleteStartOperation(ctx, pveStartingTicket); err != nil {
		t.Fatal(err)
	}
	if err := pveRepo.DeleteStartPlayerIfMatches(ctx, pveStartingPlayer, pveStartingTicket); err != nil {
		t.Fatal(err)
	}

	const playerID, ticketID, matchID = uint64(52), uint64(9201), uint64(9301)
	ticket := &matchv1.MatchTicketStorageRecord{
		TicketId: ticketID, TeamId: ticketID, MatchId: matchID,
		Members:  []*matchv1.MatchMemberStorageRecord{{PlayerId: playerID, TeamId: ticketID}},
		GameMode: "pve_coop",
	}
	if err := pveRepo.AddTicket(ctx, ticket, time.Hour); err != nil {
		t.Fatal(err)
	}
	if existing, claimed, err := pveRepo.ClaimPlayer(ctx, playerID, ticketID, time.Hour); err != nil || !claimed || existing != ticketID {
		t.Fatalf("claim: existing=%d claimed=%v err=%v", existing, claimed, err)
	}
	opID := "9849ab5b-2ecf-4fc3-983d-2d8df53cc009"
	m := &matchv1.MatchStorageRecord{
		MatchId: matchID, Stage: matchv1.MatchStage_MATCH_STAGE_CONFIRM,
		TicketIds: []uint64{ticketID}, ConfirmDeadlineMs: time.Now().Add(time.Minute).UnixMilli(),
		Members:               []*matchv1.MatchMemberStorageRecord{{PlayerId: playerID, TeamId: ticketID}},
		AllocationOperationId: opID,
		GameMode:              "pve_coop",
	}
	if err := pveRepo.CreateMatch(ctx, m, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := resolver.CancelMatch(ctx, playerID); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("wrong-mode CancelMatch must fail closed, got %v", err)
	}
	if err := resolver.ConfirmMatch(ctx, playerID, matchID, true); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("wrong-mode ConfirmMatch must fail closed, got %v", err)
	}
	if err := resolver.reconcileActiveOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if ids, err := pvpRepo.RangeActiveMatches(ctx); err != nil || len(ids) != 0 {
		t.Fatalf("PVP reconciler adopted PVE active job: ids=%v err=%v", ids, err)
	}

	confirming, err := resolver.ResolvePlayerMatchContext(ctx, playerID)
	if err != nil || confirming.GetStage() != matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_CONFIRMING || confirming.GetMatchId() != matchID {
		t.Fatalf("cross-mode CONFIRMING = %+v err=%v", confirming, err)
	}
	if confirming.GetGameMode() != "pve_coop" {
		t.Fatalf("cross-mode resolver lost canonical game_mode: %+v", confirming)
	}
	if err := pveRepo.UpdateMatchWithLock(ctx, matchID, 2, func(rec *matchv1.MatchStorageRecord) error {
		rec.Stage = matchv1.MatchStage_MATCH_STAGE_ALLOCATING
		return nil
	}, time.Hour); err != nil {
		t.Fatal(err)
	}
	allocating, err := resolver.ResolvePlayerMatchContext(ctx, playerID)
	if err != nil || allocating.GetStage() != matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_ALLOCATING {
		t.Fatalf("cross-mode ALLOCATING = %+v err=%v", allocating, err)
	}
	if err := pveRepo.UpdateMatchWithLock(ctx, matchID, 2, func(rec *matchv1.MatchStorageRecord) error {
		rec.Stage = matchv1.MatchStage_MATCH_STAGE_READY
		rec.BattleDsAddr = "10.0.0.8:7777"
		rec.BattleTarget = &matchv1.MatchBattleTargetStorageRecord{
			DsAddr: rec.BattleDsAddr, DsPodName: "battle-1", DsInstanceUid: "uid-1",
			DsInstanceEpoch: 7, AllocationId: "alloc-1", ReleaseTrack: "stable",
		}
		return nil
	}, time.Hour); err != nil {
		t.Fatal(err)
	}
	ready, err := resolver.ResolvePlayerMatchContext(ctx, playerID)
	if err != nil || ready.GetStage() != matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_READY {
		t.Fatalf("cross-mode READY = %+v err=%v", ready, err)
	}
	if ready.GetBattleDsAddr() != "10.0.0.8:7777" || ready.GetBattleTicket() != "canonical-ready-resume-ticket" {
		t.Fatalf("READY resolver did not return freshly bound route credential: %+v", ready)
	}
	if resumeAllocator.playerID != playerID || resumeAllocator.matchID != matchID ||
		resumeAllocator.allocation.Address != "10.0.0.8:7777" ||
		resumeAllocator.allocation.Target.PodName != "battle-1" ||
		resumeAllocator.allocation.Target.InstanceUID != "uid-1" ||
		resumeAllocator.allocation.Target.InstanceEpoch != 7 ||
		resumeAllocator.allocation.Target.AllocationID != "alloc-1" ||
		resumeAllocator.allocation.Target.ReleaseTrack != "stable" {
		t.Fatalf("READY re-sign input was not canonical exact target: %+v",
			resumeAllocator.allocation)
	}

	if err := pveRepo.DeleteMatch(ctx, matchID); err != nil {
		t.Fatal(err)
	}
	drift, err := resolver.ResolvePlayerMatchContext(ctx, playerID)
	if err != nil || drift.GetState() != matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_UNSPECIFIED {
		t.Fatalf("broken ticket->match edge must be UNKNOWN: %+v err=%v", drift, err)
	}
}

func (f *fixture) advanceStarts(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := f.uc.advanceStartOperationsOnce(ctx); err != nil {
		t.Fatalf("advanceStartOperationsOnce: %v", err)
	}
}

func (f *fixture) advanceAllocations(t *testing.T, ctx context.Context) {
	t.Helper()
	if err := f.uc.advanceAllocationsOnce(ctx); err != nil {
		t.Fatalf("advanceAllocationsOnce: %v", err)
	}
}

// seedTicket 写一张票据并声明其全体成员归属。
func (f *fixture) seedTicket(t *testing.T, ctx context.Context, ticketID uint64, playerIDs []uint64, avgMMR int32) {
	t.Helper()
	members := make([]*matchv1.MatchMemberStorageRecord, 0, len(playerIDs))
	for _, pid := range playerIDs {
		if _, ok, err := f.repo.ClaimPlayer(ctx, pid, ticketID, f.cfg.TicketTTL.Std()); err != nil || !ok {
			t.Fatalf("claim player %d: ok=%v err=%v", pid, ok, err)
		}
		members = append(members, &matchv1.MatchMemberStorageRecord{
			PlayerId: pid,
			TeamId:   ticketID,
			Mmr:      avgMMR,
			Confirm:  confirmPending,
		})
	}
	ticket := &matchv1.MatchTicketStorageRecord{
		TicketId:     ticketID,
		TeamId:       ticketID,
		CaptainId:    playerIDs[0],
		Members:      members,
		AvgMmr:       avgMMR,
		EnqueuedAtMs: time.Now().UnixMilli(),
	}
	if err := f.repo.AddTicket(ctx, ticket, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatalf("add ticket %d: %v", ticketID, err)
	}
}

// ── 用例 ──────────────────────────────────────────────────────────────────────

// P0 修复(2026-07-15,codex P0-8):placement 硬删后 locator BATTLE 状态门重新生效——
// 战斗中玩家不得入队(不变量 §1 一人一 DS)。终局后 BATTLE 投影 ≤30s 自然蒸发,
// 玩家随后可正常 StartMatch;误拦截窗口有界且可自愈。
func TestStartMatch_RejectsPlayerInBattle(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	const captain = uint64(45)
	f.locator.mu.Lock()
	f.locator.inBattle[captain] = true
	f.locator.mu.Unlock()
	if _, err := f.uc.StartMatch(ctx, 7004, 7004, captain, 0); errcode.As(err) != errcode.ErrMatchInBattle {
		t.Fatalf("in-battle player must be rejected from queueing, got err=%v", err)
	}
	// BATTLE 投影蒸发后同一玩家可入队。
	f.locator.mu.Lock()
	delete(f.locator.inBattle, captain)
	f.locator.mu.Unlock()
	if id, err := f.uc.StartMatch(ctx, 7004, 7004, captain, 0); err != nil || id != 7004 {
		t.Fatalf("player outside battle must queue normally: id=%d err=%v", id, err)
	}
}

// 10 张单人票据 → matchOnce 凑成一场 5+5,进确认期。
func TestMatchOnce_FormsMatch(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)

	for i := uint64(1); i <= 10; i++ {
		f.seedTicket(t, ctx, 100+i, []uint64{i}, 1000)
	}

	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}

	m, found, err := f.repo.GetMatch(ctx, 999)
	if err != nil || !found {
		t.Fatalf("get match 999: found=%v err=%v", found, err)
	}
	if m.Stage != stageConfirm {
		t.Fatalf("stage = %v, want CONFIRM", m.Stage)
	}
	if len(m.Members) != 10 {
		t.Fatalf("members = %d, want 10", len(m.Members))
	}
	var sideA, sideB int
	for _, mb := range m.Members {
		if mb.Side == 0 {
			sideA++
		} else {
			sideB++
		}
	}
	if sideA != 5 || sideB != 5 {
		t.Fatalf("sides = %d/%d, want 5/5", sideA, sideB)
	}
	// 队列票据应已预留(移出 queue)
	left, _ := f.repo.RangeQueueTickets(ctx)
	if len(left) != 0 {
		t.Fatalf("queue left = %d, want 0", len(left))
	}
}

// 全员确认 → match READY,带 ds 地址。
func TestConfirmMatch_AllAccept_Ready(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	for i := uint64(1); i <= 10; i++ {
		f.seedTicket(t, ctx, 100+i, []uint64{i}, 1000)
	}
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}

	for i := uint64(1); i <= 10; i++ {
		if err := f.uc.ConfirmMatch(ctx, i, 999, true); err != nil {
			t.Fatalf("confirm player %d: %v", i, err)
		}
	}
	allocating, found, err := f.repo.GetMatch(ctx, 999)
	if err != nil || !found || allocating.GetStage() != stageAllocating ||
		!placement.ValidOperationID(allocating.GetAllocationOperationId()) {
		t.Fatalf("durable allocation operation not UUIDv4: found=%v match=%+v err=%v", found, allocating, err)
	}
	stableOperationID := allocating.GetAllocationOperationId()
	f.advanceAllocations(t, ctx)

	m, found, _ := f.repo.GetMatch(ctx, 999)
	if !found {
		t.Fatal("match 999 gone")
	}
	if m.Stage != stageReady {
		t.Fatalf("stage = %v, want READY", m.Stage)
	}
	if m.BattleDsAddr == "" {
		t.Fatal("battle_ds_addr empty")
	}
	if m.GetAllocationOperationId() != stableOperationID || m.GetBattleTarget() == nil {
		t.Fatalf("READY target was not persisted with stable operation: %+v", m)
	}
	if got := f.pusher.lastStageFor(1); got != stageReady {
		t.Fatalf("player 1 last push stage = %v, want READY", got)
	}
}

func TestReleaseMatch_CleansReadyMatchState(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	for i := uint64(1); i <= 10; i++ {
		f.seedTicket(t, ctx, 100+i, []uint64{i}, 1000)
	}
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}
	for i := uint64(1); i <= 10; i++ {
		if err := f.uc.ConfirmMatch(ctx, i, 999, true); err != nil {
			t.Fatalf("confirm player %d: %v", i, err)
		}
	}

	if err := f.uc.ReleaseMatch(ctx, 999, nil); err != nil {
		t.Fatalf("ReleaseMatch: %v", err)
	}
	if _, found, err := f.repo.GetMatch(ctx, 999); err != nil || found {
		t.Fatalf("match after release: found=%v err=%v, want gone", found, err)
	}
	for i := uint64(1); i <= 10; i++ {
		ticketID := 100 + i
		if _, found, err := f.repo.GetTicket(ctx, ticketID); err != nil || found {
			t.Fatalf("ticket %d after release: found=%v err=%v, want gone", ticketID, found, err)
		}
		if got, found, err := f.repo.GetPlayerTicket(ctx, i); err != nil || found {
			t.Fatalf("player %d claim after release: ticket=%d found=%v err=%v, want gone", i, got, found, err)
		}
	}
}

func TestReleaseMatch_DoesNotDeleteNewClaim(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	for i := uint64(1); i <= 10; i++ {
		f.seedTicket(t, ctx, 100+i, []uint64{i}, 1000)
	}
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}
	for i := uint64(1); i <= 10; i++ {
		if err := f.uc.ConfirmMatch(ctx, i, 999, true); err != nil {
			t.Fatalf("confirm player %d: %v", i, err)
		}
	}

	// 模拟旧局释放与新一局入队竞态:player 1 已经拥有一张不属于旧 match 的新票据。
	if err := f.repo.DeletePlayerIndex(ctx, 1); err != nil {
		t.Fatalf("delete old player index: %v", err)
	}
	const newTicketID uint64 = 9001
	if _, ok, err := f.repo.ClaimPlayer(ctx, 1, newTicketID, f.cfg.TicketTTL.Std()); err != nil || !ok {
		t.Fatalf("claim new ticket: ok=%v err=%v", ok, err)
	}
	if err := f.repo.AddTicket(ctx, &matchv1.MatchTicketStorageRecord{
		TicketId:     newTicketID,
		TeamId:       newTicketID,
		CaptainId:    1,
		Members:      []*matchv1.MatchMemberStorageRecord{{PlayerId: 1, TeamId: newTicketID, Confirm: confirmPending}},
		AvgMmr:       1000,
		EnqueuedAtMs: time.Now().UnixMilli(),
	}, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatalf("add new ticket: %v", err)
	}

	if err := f.uc.ReleaseMatch(ctx, 999, nil); err != nil {
		t.Fatalf("ReleaseMatch: %v", err)
	}
	got, found, err := f.repo.GetPlayerTicket(ctx, 1)
	if err != nil || !found || got != newTicketID {
		t.Fatalf("player 1 new claim after old release: ticket=%d found=%v err=%v, want %d", got, found, err, newTicketID)
	}
}

// 僵尸 claim 清理 / 回滚必须 CAS:claim 已指向新一局票据时,旧票据的清理路径不准误删。
func TestRollbackClaims_DoesNotDeleteNewClaim(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)

	// player 1 的 claim 指向新票据 300
	if _, ok, err := f.repo.ClaimPlayer(ctx, 1, 300, f.cfg.TicketTTL.Std()); err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	// 旧票据 100 的回滚(模拟「读到旧 claim → 过期 → 新 claim 写入 → 回滚执行」竞态)
	f.uc.rollbackClaims(ctx, 100, []uint64{1})
	if got, found, _ := f.repo.GetPlayerTicket(ctx, 1); !found || got != 300 {
		t.Fatalf("new claim after stale rollback: ticket=%d found=%v, want 300", got, found)
	}
	// 本票据 300 的回滚 → 正常删除
	f.uc.rollbackClaims(ctx, 300, []uint64{1})
	if _, found, _ := f.repo.GetPlayerTicket(ctx, 1); found {
		t.Fatal("claim should be gone after matching rollback")
	}
}

// 撮合成局 → locator 上报全员 MATCHING(带 match_id);全员确认就绪 → 上报 BATTLE(带 ds_addr)。
func TestLocatorState_MatchingThenBattle(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	for i := uint64(1); i <= 10; i++ {
		f.seedTicket(t, ctx, 100+i, []uint64{i}, 1000)
	}

	// 成局:进确认期,全员应被标记 MATCHING(match_id=999)
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}
	for i := uint64(1); i <= 10; i++ {
		got, ok := f.locator.matchingOf(i)
		if !ok || got != 999 {
			t.Fatalf("player %d MATCHING match_id = %d ok=%v, want 999", i, got, ok)
		}
		// 此阶段尚未进战斗,不应有 BATTLE 上报
		if _, ok := f.locator.battleOf(i); ok {
			t.Fatalf("player %d unexpectedly BATTLE before confirm", i)
		}
	}

	// 全员确认 → READY,全员应被标记 BATTLE(battle_pod = ds_addr)
	for i := uint64(1); i <= 10; i++ {
		if err := f.uc.ConfirmMatch(ctx, i, 999, true); err != nil {
			t.Fatalf("confirm player %d: %v", i, err)
		}
	}
	f.advanceAllocations(t, ctx)
	m, _, _ := f.repo.GetMatch(ctx, 999)
	for i := uint64(1); i <= 10; i++ {
		pod, ok := f.locator.battleOf(i)
		if !ok || pod == "" {
			t.Fatalf("player %d BATTLE pod = %q ok=%v, want non-empty", i, pod, ok)
		}
		if pod != m.BattleDsAddr {
			t.Fatalf("player %d BATTLE pod = %q, want ds_addr %q", i, pod, m.BattleDsAddr)
		}
	}
}

// 任一玩家拒绝 → match FAILED,其余整队退回队列,拒绝者票据删除。
func TestConfirmMatch_Reject_FailsAndRequeues(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	// 两张五人票:ticket 100(player 1-5)、ticket 200(player 6-10)
	f.seedTicket(t, ctx, 100, []uint64{1, 2, 3, 4, 5}, 1000)
	f.seedTicket(t, ctx, 200, []uint64{6, 7, 8, 9, 10}, 1000)
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}

	// player 1(属 ticket 100)拒绝
	if err := f.uc.ConfirmMatch(ctx, 1, 999, false); err != nil {
		t.Fatalf("reject: %v", err)
	}

	m, found, _ := f.repo.GetMatch(ctx, 999)
	if !found || m.Stage != stageFailed {
		t.Fatalf("match stage = %v found=%v, want FAILED", m.GetStage(), found)
	}

	// ticket 200 应退回队列,ticket 100 应被删除
	left, _ := f.repo.RangeQueueTickets(ctx)
	if len(left) != 1 || left[0] != 200 {
		t.Fatalf("queue = %v, want [200]", left)
	}
	if _, found, _ := f.repo.GetTicket(ctx, 100); found {
		t.Fatal("rejecter ticket 100 should be deleted")
	}
	// 退回的票据保留排队时长(enqueued_at_ms 不为 0)且 match_id 清零
	rq, found, _ := f.repo.GetTicket(ctx, 200)
	if !found || rq.MatchId != 0 || rq.EnqueuedAtMs == 0 {
		t.Fatalf("requeued ticket bad: found=%v match_id=%d enq=%d", found, rq.GetMatchId(), rq.GetEnqueuedAtMs())
	}
}

// 成局最终门:全员确认后、分配 DS 前发现有人掉线(locator 判 OFFLINE)→
// match FAILED,掉线者所在票据删除,其余票据退回队列,不上报 BATTLE。
func TestConfirmMatch_OfflineMember_FailsAndRequeues(t *testing.T) {
	ctx := context.Background()
	f := newFixtureWith(t, 999, func(c *conf.MatchConf) { c.LivenessGateEnabled = true })
	f.seedTicket(t, ctx, 100, []uint64{1, 2, 3, 4, 5}, 1000)
	f.seedTicket(t, ctx, 200, []uint64{6, 7, 8, 9, 10}, 1000)
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}

	// player 6(属 ticket 200)在确认期内掉线(断报 ≥30s,locator key 过期)
	f.locator.mu.Lock()
	f.locator.offline[6] = true
	f.locator.mu.Unlock()

	for i := uint64(1); i <= 10; i++ {
		if err := f.uc.ConfirmMatch(ctx, i, 999, true); err != nil {
			t.Fatalf("confirm player %d: %v", i, err)
		}
	}
	f.advanceAllocations(t, ctx)

	m, found, _ := f.repo.GetMatch(ctx, 999)
	if !found || m.Stage != stageFailed {
		t.Fatalf("match stage = %v found=%v, want FAILED", m.GetStage(), found)
	}
	// 掉线者所在 ticket 200 删除,无辜 ticket 100 退回队列
	if _, found, _ := f.repo.GetTicket(ctx, 200); found {
		t.Fatal("offline member ticket 200 should be deleted")
	}
	left, _ := f.repo.RangeQueueTickets(ctx)
	if len(left) != 1 || left[0] != 100 {
		t.Fatalf("queue = %v, want [100]", left)
	}
	// 不应有任何 BATTLE 上报(未进分配)
	for i := uint64(1); i <= 10; i++ {
		if _, ok := f.locator.battleOf(i); ok {
			t.Fatalf("player %d unexpectedly notified BATTLE for liveness-failed match", i)
		}
	}
}

// 成局最终门弱依赖:locator 查询失败 → 跳过在线校验,照常成局(不误杀正常对局)。
func TestConfirmMatch_LivenessQueryError_ProceedsReady(t *testing.T) {
	ctx := context.Background()
	f := newFixtureWith(t, 999, func(c *conf.MatchConf) { c.LivenessGateEnabled = true })
	for i := uint64(1); i <= 10; i++ {
		f.seedTicket(t, ctx, 100+i, []uint64{i}, 1000)
	}
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}

	// locator 抖动:FindOfflinePlayers 一律报错 → 应跳过校验继续成局
	f.locator.mu.Lock()
	f.locator.queryErr = errcode.New(errcode.ErrInternal, "locator down")
	f.locator.mu.Unlock()

	for i := uint64(1); i <= 10; i++ {
		if err := f.uc.ConfirmMatch(ctx, i, 999, true); err != nil {
			t.Fatalf("confirm player %d: %v", i, err)
		}
	}
	f.advanceAllocations(t, ctx)
	m, found, _ := f.repo.GetMatch(ctx, 999)
	if !found || m.Stage != stageReady {
		t.Fatalf("match stage = %v found=%v, want READY (liveness check skipped on error)", m.GetStage(), found)
	}
}

// 队列在线扫除:掉线玩家的死票被主动删除并释放归属,在线票据不受影响。
func TestLivenessSweep_ReapsOfflineTickets(t *testing.T) {
	ctx := context.Background()
	f := newFixtureWith(t, 999, func(c *conf.MatchConf) { c.LivenessGateEnabled = true })
	// ticket 100(player 1-5 组队,player 3 掉线)、ticket 200(player 6 单排,在线)
	f.seedTicket(t, ctx, 100, []uint64{1, 2, 3, 4, 5}, 1000)
	f.seedTicket(t, ctx, 200, []uint64{6}, 1000)

	f.locator.mu.Lock()
	f.locator.offline[3] = true
	f.locator.mu.Unlock()

	if err := f.uc.livenessSweepOnce(ctx); err != nil {
		t.Fatalf("livenessSweepOnce: %v", err)
	}

	// 死票 100 删除 + 全体成员归属释放(可立刻再排)
	if _, found, _ := f.repo.GetTicket(ctx, 100); found {
		t.Fatal("dead ticket 100 should be reaped")
	}
	for i := uint64(1); i <= 5; i++ {
		if _, found, _ := f.repo.GetPlayerTicket(ctx, i); found {
			t.Fatalf("player %d claim should be released after reap", i)
		}
	}
	// 在线票据 200 原样保留在队列
	left, _ := f.repo.RangeQueueTickets(ctx)
	if len(left) != 1 || left[0] != 200 {
		t.Fatalf("queue = %v, want [200]", left)
	}
	// 同票在线队友(player 1)收到 FAILED 推送
	if got := f.pusher.lastStageFor(1); got != stageFailed {
		t.Fatalf("player 1 last push stage = %v, want FAILED", got)
	}
}

// 队列在线扫除弱依赖:locator 查询失败 → 本轮不删任何票。
func TestLivenessSweep_QueryError_NoReap(t *testing.T) {
	ctx := context.Background()
	f := newFixtureWith(t, 999, func(c *conf.MatchConf) { c.LivenessGateEnabled = true })
	f.seedTicket(t, ctx, 100, []uint64{1}, 1000)

	f.locator.mu.Lock()
	f.locator.offline[1] = true
	f.locator.queryErr = errcode.New(errcode.ErrInternal, "locator down")
	f.locator.mu.Unlock()

	if err := f.uc.livenessSweepOnce(ctx); err != nil {
		t.Fatalf("livenessSweepOnce: %v", err)
	}
	if _, found, _ := f.repo.GetTicket(ctx, 100); !found {
		t.Fatal("ticket must survive when locator query fails (weak dependency)")
	}
}

// 开关默认关闭(LivenessGateEnabled=false):Hub DS player_ids 心跳未联发前,
// locator 判离线不生效——队列扫除不删票,成局最终门放行照常 READY
// (否则旧 Hub DS 发空 player_ids 时在线玩家 30s 后会被误判离线、票据被扫)。
func TestLivenessGate_DisabledByDefault_NoOfflineJudgement(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999) // 默认配置:开关关闭
	f.seedTicket(t, ctx, 100, []uint64{1, 2, 3, 4, 5}, 1000)
	f.seedTicket(t, ctx, 200, []uint64{6, 7, 8, 9, 10}, 1000)

	// locator 把全员判离线(模拟旧 Hub DS 发空 player_ids → 位置过期)
	f.locator.mu.Lock()
	for i := uint64(1); i <= 10; i++ {
		f.locator.offline[i] = true
	}
	f.locator.mu.Unlock()

	// 队列扫除:不删任何票
	if err := f.uc.livenessSweepOnce(ctx); err != nil {
		t.Fatalf("livenessSweepOnce: %v", err)
	}
	if left, _ := f.repo.RangeQueueTickets(ctx); len(left) != 2 {
		t.Fatalf("queue = %v, want both tickets intact when gate disabled", left)
	}

	// 成局最终门:照常成局 READY
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}
	for i := uint64(1); i <= 10; i++ {
		if err := f.uc.ConfirmMatch(ctx, i, 999, true); err != nil {
			t.Fatalf("confirm player %d: %v", i, err)
		}
	}
	f.advanceAllocations(t, ctx)
	m, found, _ := f.repo.GetMatch(ctx, 999)
	if !found || m.Stage != stageReady {
		t.Fatalf("match stage = %v found=%v, want READY (liveness gate disabled)", m.GetStage(), found)
	}
}

// 确认期超时 → expireOnce 标记 FAILED;含未确认(AFK)成员的票据被删除并释放归属,
// 不退回队列(否则同一批人 + 同一挂机者会无限重凑同一场 → 再超时)。
func TestExpireOnce_Timeout_Fails(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	f.seedTicket(t, ctx, 100, []uint64{1, 2, 3, 4, 5}, 1000)
	f.seedTicket(t, ctx, 200, []uint64{6, 7, 8, 9, 10}, 1000)

	// 手动建一场确认期已超时的 match(deadline 在过去)
	ta, _, _ := f.repo.GetTicket(ctx, 100)
	tb, _, _ := f.repo.GetTicket(ctx, 200)
	members := make([]*matchv1.MatchMemberStorageRecord, 0, 10)
	for _, t := range ta.Members {
		members = append(members, &matchv1.MatchMemberStorageRecord{PlayerId: t.PlayerId, TeamId: t.TeamId, Side: 0, Confirm: confirmPending})
	}
	for _, t := range tb.Members {
		members = append(members, &matchv1.MatchMemberStorageRecord{PlayerId: t.PlayerId, TeamId: t.TeamId, Side: 1, Confirm: confirmPending})
	}
	now := time.Now().UnixMilli()
	match := &matchv1.MatchStorageRecord{
		MatchId:           999,
		Stage:             stageConfirm,
		Members:           members,
		TicketIds:         []uint64{100, 200},
		CreatedAtMs:       now - 60000,
		ConfirmDeadlineMs: now - 1000, // 已超时
	}
	// reserve 票据(写 match_id,移出 queue),模拟 formMatch 后状态
	ta.MatchId = 999
	tb.MatchId = 999
	_ = f.repo.ReserveTicket(ctx, ta, f.cfg.TicketTTL.Std())
	_ = f.repo.ReserveTicket(ctx, tb, f.cfg.TicketTTL.Std())
	if err := f.repo.CreateMatch(ctx, match, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatalf("create match: %v", err)
	}

	if err := f.uc.expireOnce(ctx); err != nil {
		t.Fatalf("expireOnce: %v", err)
	}

	m, found, _ := f.repo.GetMatch(ctx, 999)
	if !found || m.Stage != stageFailed {
		t.Fatalf("stage = %v found=%v, want FAILED", m.GetStage(), found)
	}
	// 超时且全员未确认(PENDING)→ 两张票均判责:删票、释放归属、不退回队列
	left, _ := f.repo.RangeQueueTickets(ctx)
	if len(left) != 0 {
		t.Fatalf("queue = %v, want empty (AFK tickets dropped, not requeued)", left)
	}
	for _, tid := range []uint64{100, 200} {
		if _, found, _ := f.repo.GetTicket(ctx, tid); found {
			t.Fatalf("ticket %d should be deleted", tid)
		}
	}
	for pid := uint64(1); pid <= 10; pid++ {
		if _, ok, _ := f.repo.GetPlayerTicket(ctx, pid); ok {
			t.Fatalf("player %d claim should be released", pid)
		}
		if got := f.pusher.lastStageFor(pid); got != stageFailed {
			t.Fatalf("player %d last stage = %v, want FAILED", pid, got)
		}
	}
}

// 超时时已全员接受的票据退回队列(不能连坐),含未确认成员的票据判责删除。
func TestExpireOnce_Timeout_MixedConfirm_RequeuesInnocentDropsAFK(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	f.seedTicket(t, ctx, 100, []uint64{1, 2, 3, 4, 5}, 1000)
	f.seedTicket(t, ctx, 200, []uint64{6, 7, 8, 9, 10}, 1000)

	ta, _, _ := f.repo.GetTicket(ctx, 100)
	tb, _, _ := f.repo.GetTicket(ctx, 200)
	members := make([]*matchv1.MatchMemberStorageRecord, 0, 10)
	for _, m := range ta.Members { // ticket 100 全员已接受
		members = append(members, &matchv1.MatchMemberStorageRecord{PlayerId: m.PlayerId, TeamId: m.TeamId, Side: 0, Confirm: confirmAccepted})
	}
	for _, m := range tb.Members { // ticket 200 全员未确认(AFK)
		members = append(members, &matchv1.MatchMemberStorageRecord{PlayerId: m.PlayerId, TeamId: m.TeamId, Side: 1, Confirm: confirmPending})
	}
	now := time.Now().UnixMilli()
	match := &matchv1.MatchStorageRecord{
		MatchId:           999,
		Stage:             stageConfirm,
		Members:           members,
		TicketIds:         []uint64{100, 200},
		CreatedAtMs:       now - 60000,
		ConfirmDeadlineMs: now - 1000,
	}
	ta.MatchId = 999
	tb.MatchId = 999
	_ = f.repo.ReserveTicket(ctx, ta, f.cfg.TicketTTL.Std())
	_ = f.repo.ReserveTicket(ctx, tb, f.cfg.TicketTTL.Std())
	if err := f.repo.CreateMatch(ctx, match, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatalf("create match: %v", err)
	}

	if err := f.uc.expireOnce(ctx); err != nil {
		t.Fatalf("expireOnce: %v", err)
	}

	// ticket 100(无过错)退回队列且 match_id 清零;ticket 200(AFK)被删除
	left, _ := f.repo.RangeQueueTickets(ctx)
	if len(left) != 1 || left[0] != 100 {
		t.Fatalf("queue = %v, want [100]", left)
	}
	rq, found, _ := f.repo.GetTicket(ctx, 100)
	if !found || rq.MatchId != 0 {
		t.Fatalf("requeued ticket bad: found=%v match_id=%d", found, rq.GetMatchId())
	}
	if _, found, _ := f.repo.GetTicket(ctx, 200); found {
		t.Fatal("AFK ticket 200 should be deleted")
	}
	for pid := uint64(6); pid <= 10; pid++ {
		if _, ok, _ := f.repo.GetPlayerTicket(ctx, pid); ok {
			t.Fatalf("AFK player %d claim should be released", pid)
		}
	}
	// 无过错方收到“已回到队列”补推
	if got := f.pusher.lastStageFor(1); got != stageQueueing {
		t.Fatalf("innocent player last stage = %v, want QUEUEING", got)
	}
}

// seedAllocatingMatch 造一场 ALLOCATING 阶段的 match(票据已预留、deadline 可指定)。
func seedAllocatingMatch(t *testing.T, ctx context.Context, f *fixture, matchID uint64, deadlineMs int64) {
	t.Helper()
	f.seedTicket(t, ctx, 100, []uint64{1, 2, 3, 4, 5}, 1000)
	f.seedTicket(t, ctx, 200, []uint64{6, 7, 8, 9, 10}, 1000)
	ta, _, _ := f.repo.GetTicket(ctx, 100)
	tb, _, _ := f.repo.GetTicket(ctx, 200)
	members := make([]*matchv1.MatchMemberStorageRecord, 0, 10)
	for _, m := range ta.Members {
		members = append(members, &matchv1.MatchMemberStorageRecord{PlayerId: m.PlayerId, TeamId: m.TeamId, Side: 0, Confirm: confirmAccepted})
	}
	for _, m := range tb.Members {
		members = append(members, &matchv1.MatchMemberStorageRecord{PlayerId: m.PlayerId, TeamId: m.TeamId, Side: 1, Confirm: confirmAccepted})
	}
	match := &matchv1.MatchStorageRecord{
		MatchId:           matchID,
		Stage:             stageAllocating,
		Members:           members,
		TicketIds:         []uint64{100, 200},
		CreatedAtMs:       deadlineMs - 15000,
		ConfirmDeadlineMs: deadlineMs,
	}
	ta.MatchId = matchID
	tb.MatchId = matchID
	_ = f.repo.ReserveTicket(ctx, ta, f.cfg.TicketTTL.Std())
	_ = f.repo.ReserveTicket(ctx, tb, f.cfg.TicketTTL.Std())
	if err := f.repo.CreateMatch(ctx, match, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatalf("create match: %v", err)
	}
}

// ALLOCATING 在宽限期内 → expireOnce 不动它(留在 active 继续观察,不判失败)。
func TestExpireOnce_AllocatingWithinGrace_Kept(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	// deadline 刚过 1s,仍在 allocatingGrace(60s)内
	seedAllocatingMatch(t, ctx, f, 999, time.Now().UnixMilli()-1000)

	if err := f.uc.expireOnce(ctx); err != nil {
		t.Fatalf("expireOnce: %v", err)
	}
	m, found, _ := f.repo.GetMatch(ctx, 999)
	if !found || m.Stage != stageAllocating {
		t.Fatalf("stage = %v found=%v, want ALLOCATING kept", m.GetStage(), found)
	}
	// 仍在 active(留观),下一轮还能扫到
	expired, _ := f.repo.RangeExpiredMatches(ctx, time.Now().UnixMilli())
	if len(expired) != 1 || expired[0] != 999 {
		t.Fatalf("active = %v, want [999] kept for observation", expired)
	}
}

// ALLOCATING 是 durable job；无论多久都不能用本地超时把外部未知结果推断为失败。
func TestExpireOnce_AllocatingLongRunning_Kept(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	// deadline 已过 61s > allocatingGrace(60s)
	seedAllocatingMatch(t, ctx, f, 999, time.Now().UnixMilli()-61_000)

	if err := f.uc.expireOnce(ctx); err != nil {
		t.Fatalf("expireOnce: %v", err)
	}
	m, found, _ := f.repo.GetMatch(ctx, 999)
	if !found || m.Stage != stageAllocating {
		t.Fatalf("stage = %v found=%v, want ALLOCATING", m.GetStage(), found)
	}
	left, _ := f.repo.RangeQueueTickets(ctx)
	if len(left) != 0 {
		t.Fatalf("queue = %v, want no speculative requeue", left)
	}
}

// 卡死回归:超宽限判失败后 set-ready 迟到(分配副本没死只是慢)→ stage 守卫拒绝
// FAILED→READY 回流,已退队票据不被"拉进战斗"。
func TestOnAllConfirmed_LateReady_DoesNotOverrideFailed(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	seedAllocatingMatch(t, ctx, f, 999, time.Now().UnixMilli()-61_000)
	if err := f.repo.UpdateMatchWithLock(ctx, 999, f.cfg.OptimisticRetry, func(m *matchv1.MatchStorageRecord) error {
		m.Stage = stageFailed
		return nil
	}, f.cfg.MatchTTL.Std()); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	stale, _, _ := f.repo.GetMatch(ctx, 999)
	stale.Stage = stageAllocating
	if err := f.uc.advanceAllocation(ctx, stale); err != nil {
		t.Fatalf("advance stale: %v", err)
	}

	got, found, _ := f.repo.GetMatch(ctx, 999)
	if !found || got.Stage != stageFailed {
		t.Fatalf("stage = %v, want FAILED preserved (no READY override)", got.GetStage())
	}
}

// CancelMatch 与撮合循环竞态:票据已被 ReserveTicket 抢先(match_id 已写)时,
// 排队取消路径必须转"拒绝确认"(match 失败),绝不盲删已进 match 的票据。
func TestCancelMatch_TicketJustReserved_TurnsIntoReject(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	f.seedTicket(t, ctx, 100, []uint64{1, 2, 3, 4, 5}, 1000)
	f.seedTicket(t, ctx, 200, []uint64{6, 7, 8, 9, 10}, 1000)
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}

	// player 1 取消:其票据已被撮进 match 999 → 应等价拒绝,match 失败、对方票退回队列
	if err := f.uc.CancelMatch(ctx, 1); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	m, found, _ := f.repo.GetMatch(ctx, 999)
	if !found || m.Stage != stageFailed {
		t.Fatalf("stage = %v found=%v, want FAILED", m.GetStage(), found)
	}
	left, _ := f.repo.RangeQueueTickets(ctx)
	if len(left) != 1 || left[0] != 200 {
		t.Fatalf("queue = %v, want [200]", left)
	}
}

// 排队路径取消(未撮合)后必须给票据全体成员补推 FAILED:取消可能不是本人发起
// (队长取消 / team 离队联动撤票),其余队友的客户端不能一直停在 QUEUEING。
func TestCancelMatch_QueuePath_PushesFailedToAllMembers(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	f.seedTicket(t, ctx, 100, []uint64{1, 2, 3, 4, 5}, 1000)

	if err := f.uc.CancelMatch(ctx, 1); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// 票据已删、队列已空
	if _, found, _ := f.repo.GetTicket(ctx, 100); found {
		t.Fatal("ticket should be deleted")
	}
	left, _ := f.repo.RangeQueueTickets(ctx)
	if len(left) != 0 {
		t.Fatalf("queue = %v, want empty", left)
	}
	// 全体成员(含发起者)收到 FAILED
	for pid := uint64(1); pid <= 5; pid++ {
		if got := f.pusher.lastStageFor(pid); got != stageFailed {
			t.Errorf("player %d last stage = %v, want FAILED", pid, got)
		}
	}
	// claim 已释放:全员可立即重新排队
	for pid := uint64(1); pid <= 5; pid++ {
		if _, ok, err := f.repo.ClaimPlayer(ctx, pid, 300, f.cfg.TicketTTL.Std()); err != nil || !ok {
			t.Fatalf("player %d should be claimable again: ok=%v err=%v", pid, ok, err)
		}
	}
}

// claim 指向已消失票据(崩溃残留)→ StartMatch 自愈清理后正常入队,不再卡 4002 半小时。
func TestStartMatch_HealsStaleClaim(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)

	const captain = uint64(50)
	// 残留形态:claim 活着但票据 8888 不存在(onMatchFailed 删票后、释放 claim 前崩溃)
	if _, ok, err := f.repo.ClaimPlayer(ctx, captain, 8888, f.cfg.TicketTTL.Std()); err != nil || !ok {
		t.Fatalf("seed stale claim: ok=%v err=%v", ok, err)
	}

	id, err := f.uc.StartMatch(ctx, 7010, 7010, captain, 0)
	if err != nil {
		t.Fatalf("StartMatch should heal stale claim: %v", err)
	}
	if id != 7010 {
		t.Fatalf("ticket = %d, want 7010", id)
	}
	f.advanceStarts(t, ctx)
	if got, found, _ := f.repo.GetPlayerTicket(ctx, captain); !found || got != 7010 {
		t.Fatalf("claim = %d found=%v, want 7010", got, found)
	}
}

// claim 指向仍存活的票据 → StartMatch 绝不自愈误删,照常拒绝 4002(真占用)。
func TestStartMatch_LiveClaimStillRejected(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	f.seedTicket(t, ctx, 100, []uint64{60}, 1000)

	_, err := f.uc.StartMatch(ctx, 7011, 7011, 60, 0)
	if code := errcode.As(err); code != errcode.ErrMatchAlreadyMatching {
		t.Fatalf("code = %d, want ErrMatchAlreadyMatching(%d)", code, errcode.ErrMatchAlreadyMatching)
	}
	// 原票据与 claim 原样保留
	if got, found, _ := f.repo.GetPlayerTicket(ctx, 60); !found || got != 100 {
		t.Fatalf("claim = %d found=%v, want 100", got, found)
	}
	if _, found, _ := f.repo.GetTicket(ctx, 100); !found {
		t.Fatal("live ticket 100 must survive")
	}
	// 写序铁律回滚断言:失败的 StartMatch 必须删掉自己先落地的票据主体,不残留孤儿记录。
	if _, found, _ := f.repo.GetTicket(ctx, 7011); found {
		t.Fatal("aborted ticket 7011 record must be rolled back")
	}
}

// 写序铁律回归(镜像 team TestCreateTeamRealCollisionStillRejected 的结论):
// 并发 StartMatch 的 in-flight 形态 =「票据主体已写、claim 已占、尚未入队」。
// 第二次 StartMatch 撞上该 claim 时,GetTicket 能看到主体 → 判真占用拒绝,
// 绝不把 in-flight claim 当僵尸 CAS 删掉(否则同批玩家双票入队,违反不变量 §1)。
func TestStartMatch_InFlightClaimNotHealed(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)

	const player = uint64(70)
	// 模拟请求 A 的 in-flight 中间态:主体已落地(未入队)+ claim 已占。
	inflight := &matchv1.MatchTicketStorageRecord{
		TicketId:  8100,
		TeamId:    8100,
		CaptainId: player,
		Members:   []*matchv1.MatchMemberStorageRecord{{PlayerId: player, TeamId: 8100, Confirm: confirmPending}},
	}
	if err := f.repo.CreateTicketRecord(ctx, inflight, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatalf("seed inflight record: %v", err)
	}
	if _, ok, err := f.repo.ClaimPlayer(ctx, player, 8100, f.cfg.TicketTTL.Std()); err != nil || !ok {
		t.Fatalf("seed inflight claim: ok=%v err=%v", ok, err)
	}

	// 请求 B(同玩家、新 ticketID)必须被拒,且 A 的 claim / 主体原样保留。
	_, err := f.uc.StartMatch(ctx, 8200, 8200, player, 0)
	if code := errcode.As(err); code != errcode.ErrMatchAlreadyMatching {
		t.Fatalf("code = %d, want ErrMatchAlreadyMatching(%d)", code, errcode.ErrMatchAlreadyMatching)
	}
	if got, found, _ := f.repo.GetPlayerTicket(ctx, player); !found || got != 8100 {
		t.Fatalf("inflight claim = %d found=%v, want 8100 intact", got, found)
	}
	if _, found, _ := f.repo.GetTicket(ctx, 8100); !found {
		t.Fatal("inflight ticket 8100 record must survive")
	}
	if _, found, _ := f.repo.GetTicket(ctx, 8200); found {
		t.Fatal("aborted ticket 8200 record must be rolled back")
	}
}

// 票据 match_id 指向已不存在的 match(崩溃残留孤儿)→ CancelMatch 收割:删票 + 释放归属 + 推 FAILED。
func TestCancelMatch_OrphanTicket_CleansUp(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	f.seedTicket(t, ctx, 100, []uint64{1, 2, 3, 4, 5}, 1000)

	tk, _, _ := f.repo.GetTicket(ctx, 100)
	tk.MatchId = 4242 // match 4242 从未创建(回滚中途崩溃的残留形态)
	if err := f.repo.ReserveTicket(ctx, tk, f.cfg.TicketTTL.Std()); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	if err := f.uc.CancelMatch(ctx, 1); err != nil {
		t.Fatalf("cancel orphan ticket: %v", err)
	}

	if _, found, _ := f.repo.GetTicket(ctx, 100); found {
		t.Fatal("orphan ticket should be deleted")
	}
	for pid := uint64(1); pid <= 5; pid++ {
		if _, ok, _ := f.repo.GetPlayerTicket(ctx, pid); ok {
			t.Fatalf("player %d claim should be released", pid)
		}
		if got := f.pusher.lastStageFor(pid); got != stageFailed {
			t.Errorf("player %d last stage = %v, want FAILED", pid, got)
		}
	}
}

// ALLOCATING(全员已确认、分配中)阶段拒绝 → 诚实报错 ErrInvalidState,match 不受影响;
// accept 仍幂等成功。防止"取消成功却被拉进战斗"的假成功。
func TestConfirmMatch_RejectWhileAllocating_ReturnsError(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	seedAllocatingMatch(t, ctx, f, 999, time.Now().UnixMilli()+15000)

	err := f.uc.ConfirmMatch(ctx, 1, 999, false)
	if code := errcode.As(err); code != errcode.ErrInvalidState {
		t.Fatalf("reject while allocating: code = %d err=%v, want ErrInvalidState(%d)", code, err, errcode.ErrInvalidState)
	}
	m, found, _ := f.repo.GetMatch(ctx, 999)
	if !found || m.Stage != stageAllocating {
		t.Fatalf("stage = %v found=%v, want ALLOCATING unchanged", m.GetStage(), found)
	}
	if err := f.uc.ConfirmMatch(ctx, 1, 999, true); err != nil {
		t.Fatalf("accept while allocating should stay idempotent-success: %v", err)
	}
}

// ── ReserveTicket 失败一致性 ──────────────────────────────────────────────────

// faultyReserveRepo 包装真实 repo,在第 failOnCall 次 ReserveTicket 调用上注入失败,
// 用于验证 formMatch 中途预留失败时的补偿(退回队列、不留残缺 match)。
type faultyReserveRepo struct {
	data.MatchRepo
	calls      int
	failOnCall int // 第几次 ReserveTicket 调用返回错误(1-based);0 表示全部失败
}

type faultyPersistRepo struct {
	data.MatchRepo
	failRemaining int
}

func (r *faultyPersistRepo) PersistPlayerClaim(ctx context.Context, playerID, ticketID uint64) error {
	if r.failRemaining > 0 {
		r.failRemaining--
		return errors.New("injected persist failure")
	}
	return r.MatchRepo.PersistPlayerClaim(ctx, playerID, ticketID)
}

func (r *faultyReserveRepo) ReserveTicket(ctx context.Context, ticket *matchv1.MatchTicketStorageRecord, ttl time.Duration) error {
	r.calls++
	if r.failOnCall == 0 || r.calls == r.failOnCall {
		return errors.New("injected reserve failure")
	}
	return r.MatchRepo.ReserveTicket(ctx, ticket, ttl)
}

// formMatch 预留到一半失败 → 已预留票据全部退回队列,不建 match(无悬空残留)。
func TestFormMatch_ReserveFailsMidway_RollsBackNoMatch(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	for i := uint64(1); i <= 10; i++ {
		f.seedTicket(t, ctx, 100+i, []uint64{i}, 1000)
	}

	// 第 2 次 ReserveTicket 失败:第 1 张已预留,应被回滚退回队列
	faulty := &faultyReserveRepo{MatchRepo: f.repo, failOnCall: 2}
	uc := NewMatchUsecase(faulty, nil, f.pusher, NewStubDSAllocator("127.0.0.1:7777"), &fakeIDGen{next: 999}, nil, f.cfg)

	sideA := make([]*matchv1.MatchTicketStorageRecord, 0, 5)
	sideB := make([]*matchv1.MatchTicketStorageRecord, 0, 5)
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

	if err := uc.formMatch(ctx, [][]*matchv1.MatchTicketStorageRecord{sideA, sideB}); err == nil {
		t.Fatal("formMatch should fail when ReserveTicket fails")
	}

	// match 已先建后回滚:预留失败时必须把 match 删干净(否则 expireOnce 会把
	// 已退回队列的票据当成本局成员重复处理)
	if _, found, _ := f.repo.GetMatch(ctx, 999); found {
		t.Fatal("match 999 should not exist after reserve failure")
	}
	// 全部 10 张票据应仍在队列(第 1 张回滚退回 + 其余从未预留)
	left, _ := f.repo.RangeQueueTickets(ctx)
	if len(left) != 10 {
		t.Fatalf("queue = %d tickets, want 10 (consistent, no orphan)", len(left))
	}
	// 每张票据 match_id 必须清零,否则下一轮会被当作已撮合跳过/或重复处理
	for i := uint64(1); i <= 10; i++ {
		ticket, found, _ := f.repo.GetTicket(ctx, 100+i)
		if !found {
			t.Fatalf("ticket %d gone", 100+i)
		}
		if ticket.MatchId != 0 {
			t.Fatalf("ticket %d match_id = %d, want 0", 100+i, ticket.MatchId)
		}
	}
}

func TestFormMatchClaimPersistFailureKeepsCompleteCanonicalGraphAndPublishesNothing(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	for i := uint64(1); i <= 10; i++ {
		f.seedTicket(t, ctx, 100+i, []uint64{i}, 1000)
	}
	faulty := &faultyPersistRepo{MatchRepo: f.repo, failRemaining: 1}
	uc := NewMatchUsecase(faulty, nil, f.pusher, NewStubDSAllocator("127.0.0.1:7777"),
		&fakeIDGen{next: 999}, nil, f.cfg)

	sideA := make([]*matchv1.MatchTicketStorageRecord, 0, 5)
	sideB := make([]*matchv1.MatchTicketStorageRecord, 0, 5)
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
	if err := uc.formMatch(ctx, [][]*matchv1.MatchTicketStorageRecord{sideA, sideB}); err == nil {
		t.Fatal("claim persistence failure must stop FOUND publication")
	}
	m, found, err := f.repo.GetMatch(ctx, 999)
	if err != nil || !found {
		t.Fatalf("canonical retry graph missing: found=%v err=%v", found, err)
	}
	left, err := f.repo.RangeQueueTickets(ctx)
	if err != nil || len(left) != 0 {
		t.Fatalf("partial reservation remained visible in queue: ids=%v err=%v", left, err)
	}
	for i := uint64(1); i <= 10; i++ {
		ticket, ticketFound, getErr := f.repo.GetTicket(ctx, 100+i)
		if getErr != nil || !ticketFound || ticket.GetMatchId() != 999 {
			t.Fatalf("ticket %d not durably reserved: found=%v ticket=%+v err=%v",
				100+i, ticketFound, ticket, getErr)
		}
	}
	f.pusher.mu.Lock()
	eventCount := len(f.pusher.events)
	f.pusher.mu.Unlock()
	if eventCount != 0 {
		t.Fatalf("FOUND/CONFIRM published before claims durable: events=%d", eventCount)
	}
	if err := uc.ensureMatchDiscovery(ctx, m); err != nil {
		t.Fatalf("reconciler could not finish exact graph: %v", err)
	}
}

// matchOnce 在 ReserveTicket 持续失败时不留"已建 match + 票据仍在队列"的不一致(防重复撮合)。
func TestMatchOnce_ReserveFails_NoOrphanMatch(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	for i := uint64(1); i <= 10; i++ {
		f.seedTicket(t, ctx, 100+i, []uint64{i}, 1000)
	}

	faulty := &faultyReserveRepo{MatchRepo: f.repo, failOnCall: 0} // 全部失败
	uc := NewMatchUsecase(faulty, nil, f.pusher, NewStubDSAllocator("127.0.0.1:7777"), &fakeIDGen{next: 999}, nil, f.cfg)

	if err := uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce should swallow form errors and continue: %v", err)
	}

	// 没有任何 match 被建出来
	if _, found, _ := f.repo.GetMatch(ctx, 999); found {
		t.Fatal("no match should be created when all reserves fail")
	}
	// 全部票据仍在队列,可被后续轮次正常重试
	left, _ := f.repo.RangeQueueTickets(ctx)
	if len(left) != 10 {
		t.Fatalf("queue = %d tickets, want 10 (all retryable)", len(left))
	}
}

// ── 两级撮合(region 感知)接线 ───────────────────────────────────────────────

// singleRegionRouter 构造一张把所有 logical_cell 都指向 (region, cell) 的路由器,
// 用于验证 region 感知主循环在"全员同 region"时与单桶行为一致(非回归)。
func singleRegionRouter(t *testing.T, region, cell uint32) *cellroute.Router {
	t.Helper()
	entries, regionOfCell, err := cellroute.BuildBalancedEntries([]cellroute.CellSpec{{RegionID: region, CellID: cell}})
	if err != nil {
		t.Fatalf("BuildBalancedEntries: %v", err)
	}
	tbl, err := cellroute.NewStaticTable(entries, regionOfCell)
	if err != nil {
		t.Fatalf("NewStaticTable: %v", err)
	}
	r, err := cellroute.NewRouter(tbl)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return r
}

// 设了 Router 且全员同 region 时,matchOnce 仍正常凑成一场 5+5(region 感知主循环非回归)。
func TestMatchOnce_RegionAware_SingleRegionFormsMatch(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, 999)
	f.uc.SetCellRouter(singleRegionRouter(t, 3, 30)) // 所有玩家 → region 3

	for i := uint64(1); i <= 10; i++ {
		f.seedTicket(t, ctx, 100+i, []uint64{i}, 1000)
	}
	if err := f.uc.matchOnce(ctx); err != nil {
		t.Fatalf("matchOnce: %v", err)
	}

	m, found, err := f.repo.GetMatch(ctx, 999)
	if err != nil || !found {
		t.Fatalf("get match 999: found=%v err=%v", found, err)
	}
	if m.Stage != stageConfirm || len(m.Members) != 10 {
		t.Fatalf("stage=%v members=%d, want CONFIRM/10", m.Stage, len(m.Members))
	}
	left, _ := f.repo.RangeQueueTickets(ctx)
	if len(left) != 0 {
		t.Fatalf("queue left = %d, want 0", len(left))
	}
}

// ticketRegion 在 router 为 nil 时恒返回 0(单 Cell / dev 语义),不阻断撮合。
func TestTicketRegion_NilRouterZero(t *testing.T) {
	f := newFixture(t, 999)
	tk := &matchv1.MatchTicketStorageRecord{TicketId: 1, CaptainId: 12345}
	if r := f.uc.ticketRegion(tk); r != 0 {
		t.Fatalf("nil router ticketRegion = %d, want 0", r)
	}
}

// battlePlacement 在 router 为 nil 时返回 ok=false(单 Cell / dev:不带放置提示)。
func TestBattlePlacement_NilRouterNotOk(t *testing.T) {
	f := newFixture(t, 999)
	if _, ok := f.uc.battlePlacement([]uint64{1, 2, 3}); ok {
		t.Fatal("nil router battlePlacement should return ok=false")
	}
}

// battlePlacement 在所有玩家落同一 (region, cell) 时返回该落点(单 region 路由非回归)。
func TestBattlePlacement_SingleRegionAllAgree(t *testing.T) {
	f := newFixture(t, 999)
	f.uc.SetCellRouter(singleRegionRouter(t, 7, 70)) // 所有玩家 → region 7 / cell 70
	got, ok := f.uc.battlePlacement([]uint64{11, 22, 33, 44, 55})
	if !ok {
		t.Fatal("expected ok with router set")
	}
	if got.RegionID != 7 || got.CellID != 70 {
		t.Fatalf("placement = %+v, want {7,70}", got)
	}
}

// ticketTier 经 regionPolicy.MmrTier 把票据 avg_mmr 映射到段位档(默认策略:普通段 0、高分段更高)。
func TestTicketTier_FollowsPolicy(t *testing.T) {
	f := newFixture(t, 999) // 默认 DefaultRegionMatchPolicy
	low := &matchv1.MatchTicketStorageRecord{TicketId: 1, AvgMmr: 1500}
	high := &matchv1.MatchTicketStorageRecord{TicketId: 2, AvgMmr: 3300}
	if tr := f.uc.ticketTier(low); tr != 0 {
		t.Fatalf("low mmr tier = %d, want 0", tr)
	}
	if tr := f.uc.ticketTier(high); tr != 3 {
		t.Fatalf("high mmr tier = %d, want 3", tr)
	}
	if tr := f.uc.ticketTier(nil); tr != 0 {
		t.Fatalf("nil ticket tier = %d, want 0", tr)
	}
}

// TestGetMatchProgress_TicketHandleNotShadowedByAliasedForeignMatch 钉住跨 ID 空间句柄混叠:
// ticket_id 与 match_id 由同一 nodeID 的两个发号器铸造,同一秒里各自的第 K 个号**逐位相同**
// (infra.md §8.2① 契约,pkg/snowflake/etcdnode/provider_test.go 已钉死),因此某玩家的
// ticket_id 完全可能等于另一局无关的 match_id。修复前 GetMatchProgress 先探 match 空间,
// 撞上无关对局后 caller 非成员即短路 4001,把排队中的玩家误判成"不在任何队列",
// 客户端据此把匹配错误降级成 Hub 路由。
func TestGetMatchProgress_TicketHandleNotShadowedByAliasedForeignMatch(t *testing.T) {
	f := newFixture(t, 9700)
	ctx := context.Background()
	const (
		playerID    = uint64(4701)
		foreignA    = uint64(5701)
		foreignB    = uint64(5702)
		strangerID  = uint64(6701)
		collisionID = uint64(9711) // 同时是 playerID 的 ticket_id 和无关对局的 match_id
	)

	if _, err := f.uc.StartMatch(ctx, collisionID, collisionID, playerID, 7); err != nil {
		t.Fatal(err)
	}
	// 无关对局:match_id 与 playerID 的 ticket_id 逐位相同,成员里没有 playerID。
	foreign := &matchv1.MatchStorageRecord{
		MatchId: collisionID,
		Stage:   matchv1.MatchStage_MATCH_STAGE_CONFIRM,
		Members: []*matchv1.MatchMemberStorageRecord{
			{PlayerId: foreignA, TeamId: 8801},
			{PlayerId: foreignB, TeamId: 8802},
		},
	}
	if err := f.repo.CreateMatch(ctx, foreign, time.Hour); err != nil {
		t.Fatal(err)
	}

	// ① canonical ticket 尚未落地(仍在 durable start operation 阶段):
	//    match 空间被无关对局占着,必须继续探 start operation,而不是短路 4001。
	prog, err := f.uc.GetMatchProgress(ctx, playerID, collisionID)
	if err != nil {
		t.Fatalf("start-operation 阶段被无关 match 遮蔽: err=%v", err)
	}
	if prog.GetStage() != stageQueueing {
		t.Fatalf("start-operation 阶段 stage=%v, want QUEUEING (prog=%+v)", prog.GetStage(), prog)
	}

	// ② 交接出 canonical ticket 后:match 空间仍被无关对局占着,必须落到 ticket 空间。
	if err := f.uc.advanceStartOperationsOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, found, err := f.repo.GetTicket(ctx, collisionID); err != nil || !found {
		t.Fatalf("canonical ticket 应已交接: found=%v err=%v", found, err)
	}
	prog, err = f.uc.GetMatchProgress(ctx, playerID, collisionID)
	if err != nil {
		t.Fatalf("canonical ticket 被无关 match 遮蔽: err=%v", err)
	}
	if prog.GetStage() != stageQueueing {
		t.Fatalf("ticket 阶段 stage=%v, want QUEUEING (prog=%+v)", prog.GetStage(), prog)
	}

	// ③ 无关对局的真实成员仍能用同一句柄读到自己那局(match 路径未被改坏)。
	foreignProg, err := f.uc.GetMatchProgress(ctx, foreignA, collisionID)
	if err != nil {
		t.Fatalf("无关对局成员读取自己的对局: err=%v", err)
	}
	if foreignProg.GetStage() != matchv1.MatchStage_MATCH_STAGE_CONFIRM {
		t.Fatalf("无关对局成员 stage=%v, want CONFIRM", foreignProg.GetStage())
	}

	// ④ 两个空间都不属于他的第三方,仍然只拿到 4001(不泄露他人对局存在性)。
	if _, err := f.uc.GetMatchProgress(ctx, strangerID, collisionID); errcode.As(err) != errcode.ErrMatchNotFound {
		t.Fatalf("无关第三方 = %v, want ErrMatchNotFound", err)
	}
}
