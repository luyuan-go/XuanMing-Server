package biz

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/account/login/internal/data"
)

type bindingCheckerFake struct {
	err   error
	calls atomic.Int32
}

type battleTicketAuthorizerFake struct {
	err         error
	calls       atomic.Int32
	target      data.BattleTicketTarget
	returnEmpty bool
}

func (f *battleTicketAuthorizerFake) AuthorizeBattleTicket(_ context.Context, _, _ uint64) (data.BattleTicketTarget, error) {
	f.calls.Add(1)
	if f.err != nil {
		return data.BattleTicketTarget{}, f.err
	}
	if f.returnEmpty {
		return data.BattleTicketTarget{}, nil
	}
	if f.target.DSAddr == "" {
		f.target = data.BattleTicketTarget{DSAddr: "127.0.0.1:7801", PodName: "battle-test"}
	}
	return f.target, nil
}

func (f *bindingCheckerFake) CheckCurrent(_ context.Context, _ uint64, _ data.HubAssignmentBinding) error {
	f.calls.Add(1)
	return f.err
}

type concurrentJTIRepo struct {
	mu        sync.Mutex
	used      map[string]struct{}
	markCalls int
}

type admissionReplayRepo struct {
	mu        sync.Mutex
	markers   map[string]string
	peekCalls int
	markCalls int
}

func (r *admissionReplayRepo) MarkUsed(context.Context, string, time.Duration) error { return nil }

func (r *admissionReplayRepo) PeekAdmission(_ context.Context, jti, attemptOwner string) (data.AdmissionMarkerStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peekCalls++
	current, ok := r.markers[jti]
	if !ok {
		return data.AdmissionMarkerMissing, nil
	}
	if current == attemptOwner {
		return data.AdmissionMarkerExisting, nil
	}
	return data.AdmissionMarkerConflict, nil
}

func (r *admissionReplayRepo) MarkUsedByAdmission(_ context.Context, jti, attemptOwner, _ string, _ time.Duration) (data.AdmissionMarkerStatus, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.markCalls++
	if r.markers == nil {
		r.markers = make(map[string]string)
	}
	if current, ok := r.markers[jti]; ok {
		if current == attemptOwner {
			return data.AdmissionMarkerExisting, nil
		}
		return data.AdmissionMarkerConflict, errcode.New(errcode.ErrLoginTicketReplayed, "conflict")
	}
	r.markers[jti] = attemptOwner
	return data.AdmissionMarkerCreated, nil
}

func (r *admissionReplayRepo) counts() (int, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peekCalls, r.markCalls
}

type capturingAssignmentChecker struct {
	last data.HubAssignmentBinding
}

func (c *capturingAssignmentChecker) CheckCurrent(_ context.Context, _ uint64, binding data.HubAssignmentBinding) error {
	c.last = binding
	return nil
}

func (c *capturingAssignmentChecker) CheckCurrentStable(_ context.Context, _ uint64, _, active data.HubAssignmentBinding) error {
	c.last = active
	return nil
}

func (c *capturingAssignmentChecker) CheckCurrentAdmission(_ context.Context, _ uint64, _, active data.HubAssignmentBinding, _ bool) error {
	c.last = active
	return nil
}

func (r *concurrentJTIRepo) MarkUsed(_ context.Context, jti string, _ time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.markCalls++
	if r.used == nil {
		r.used = make(map[string]struct{})
	}
	if _, ok := r.used[jti]; ok {
		return errcode.New(errcode.ErrLoginTicketReplayed, "replayed")
	}
	r.used[jti] = struct{}{}
	return nil
}

func (r *concurrentJTIRepo) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.markCalls
}

func newTicketTestPair(t *testing.T) (*auth.Signer, *auth.Verifier) {
	t.Helper()
	cfg := auth.Config{Secret: []byte(testSecret)}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	return signer, verifier
}

func newBoundTicketUsecase(t *testing.T, jti data.TicketJTIRepo, checker data.HubAssignmentChecker, require bool) (*TicketUsecase, string, auth.DSTicketBinding) {
	t.Helper()
	signer, verifier := newTicketTestPair(t)
	binding := auth.DSTicketBinding{
		DSPodName:       "hub-cn-1",
		DSInstanceUID:   "uid-a",
		ProtocolEpoch:   7,
		CredentialGen:   42,
		CredentialJTI:   "credential-jti-a",
		HubAssignmentID: "assignment-a",
		WriterEpoch:     2,
	}
	ticket, _, err := signer.SignBoundHubDSTicket(1001, 3, 33, 9, 0, "entry-jti-a", binding)
	if err != nil {
		t.Fatalf("SignBoundHubDSTicket: %v", err)
	}
	uc := NewTicketUsecase(signer, verifier, jti)
	uc.SetHubAssignmentBindingPolicy(require, checker)
	return uc, ticket, binding
}

func TestVerifyBoundHubTicketChecksAssignmentBeforeConsumingJTI(t *testing.T) {
	t.Run("exact-match", func(t *testing.T) {
		jti := &concurrentJTIRepo{}
		checker := &bindingCheckerFake{}
		uc, ticket, binding := newBoundTicketUsecase(t, jti, checker, true)
		claims, err := uc.VerifyDSTicket(context.Background(), ticket, binding.DSPodName)
		if err != nil {
			t.Fatalf("VerifyDSTicket: %v", err)
		}
		if checker.calls.Load() != 1 || jti.calls() != 1 {
			t.Fatalf("checker=%d markUsed=%d, want 1/1", checker.calls.Load(), jti.calls())
		}
		if claims.PlayerID != 1001 || claims.DSPodName != binding.DSPodName ||
			claims.DSInstanceUID != binding.DSInstanceUID || claims.DSProtocolEpoch != binding.ProtocolEpoch ||
			claims.DSCredentialGen != binding.CredentialGen || claims.DSCredentialJTI != binding.CredentialJTI ||
			claims.HubAssignmentID != binding.HubAssignmentID || claims.DSWriterEpoch != binding.WriterEpoch {
			t.Fatalf("binding claims not passed through: %+v", claims)
		}
	})

	tests := []struct {
		name       string
		checkerErr error
		pod        string
		wantCode   errcode.Code
		wantChecks int32
	}{
		{"request-pod-mismatch", nil, "hub-cn-2", errcode.ErrUnauthorized, 0},
		{"assignment-mismatch", errcode.New(errcode.ErrLoginTicketInvalid, "stale assignment"), "hub-cn-1", errcode.ErrLoginTicketInvalid, 1},
		{"uid-rebuild", errcode.New(errcode.ErrLoginTicketInvalid, "uid changed"), "hub-cn-1", errcode.ErrLoginTicketInvalid, 1},
		{"redis-failure", errcode.New(errcode.ErrUnavailable, "redis down"), "hub-cn-1", errcode.ErrUnavailable, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jti := &concurrentJTIRepo{}
			checker := &bindingCheckerFake{err: tc.checkerErr}
			uc, ticket, _ := newBoundTicketUsecase(t, jti, checker, true)
			if _, err := uc.VerifyDSTicket(context.Background(), ticket, tc.pod); errcode.As(err) != tc.wantCode {
				t.Fatalf("code=%v err=%v, want=%v", errcode.As(err), err, tc.wantCode)
			}
			if got := jti.calls(); got != 0 {
				t.Fatalf("MarkUsed calls=%d, mismatch/failure must have zero jti side effects", got)
			}
			if got := checker.calls.Load(); got != tc.wantChecks {
				t.Fatalf("checker calls=%d, want=%d", got, tc.wantChecks)
			}
		})
	}

	t.Run("checker-not-wired", func(t *testing.T) {
		jti := &concurrentJTIRepo{}
		uc, ticket, binding := newBoundTicketUsecase(t, jti, nil, false)
		if _, err := uc.VerifyDSTicket(context.Background(), ticket, binding.DSPodName); errcode.As(err) != errcode.ErrUnavailable {
			t.Fatalf("code=%v err=%v", errcode.As(err), err)
		}
		if jti.calls() != 0 {
			t.Fatalf("checker missing consumed jti")
		}
	})
}

func TestHubBindingActivationRejectsLegacyTicketAndSelfSigning(t *testing.T) {
	cfg := auth.Config{Secret: []byte(testSecret)}
	signer, _ := auth.NewSigner(cfg)
	verifier, _ := auth.NewVerifier(cfg)
	legacy, _, err := signer.SignDSTicket(1001, auth.DSTypeHub, 0, "legacy-entry-jti")
	if err != nil {
		t.Fatal(err)
	}

	t.Run("compatibility-off-accepts-legacy", func(t *testing.T) {
		jti := &concurrentJTIRepo{}
		uc := NewTicketUsecase(signer, verifier, jti)
		if _, err := uc.VerifyDSTicket(context.Background(), legacy, "hub-cn-1"); err != nil {
			t.Fatalf("legacy compatibility: %v", err)
		}
		if jti.calls() != 1 {
			t.Fatalf("MarkUsed calls=%d, want=1", jti.calls())
		}
	})

	t.Run("activation-rejects-legacy-before-jti", func(t *testing.T) {
		jti := &concurrentJTIRepo{}
		uc := NewTicketUsecase(signer, verifier, jti)
		uc.SetHubAssignmentBindingPolicy(true, &bindingCheckerFake{})
		if _, err := uc.VerifyDSTicket(context.Background(), legacy, "hub-cn-1"); errcode.As(err) != errcode.ErrLoginTicketInvalid {
			t.Fatalf("code=%v err=%v", errcode.As(err), err)
		}
		if jti.calls() != 0 {
			t.Fatalf("legacy reject consumed jti")
		}
	})

	t.Run("activation-forbids-direct-hub-issue", func(t *testing.T) {
		uc := NewTicketUsecase(signer, verifier, nil)
		uc.SetHubAssignmentBindingPolicy(true, &bindingCheckerFake{})
		uc.SetBattleTicketAuthorizer(&battleTicketAuthorizerFake{})
		if _, err := uc.IssueDSTicket(context.Background(), 1001, string(auth.DSTypeHub), 0, ""); errcode.As(err) != errcode.ErrUnavailable {
			t.Fatalf("hub issue code=%v err=%v", errcode.As(err), err)
		}
		if _, err := uc.IssueDSTicket(context.Background(), 1001, string(auth.DSTypeBattle), 9001, ""); err != nil {
			t.Fatalf("battle issue must remain available: %v", err)
		}
	})
}

func TestIssueBattleDSTicketRequiresRosterAuthorizationBeforeSigning(t *testing.T) {
	signer, verifier := newTicketTestPair(t)
	uc := NewTicketUsecase(signer, verifier, nil)
	if _, err := uc.IssueDSTicket(context.Background(), 1001, string(auth.DSTypeBattle), 9001, ""); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("missing authorizer code=%v err=%v", errcode.As(err), err)
	}
	reject := &battleTicketAuthorizerFake{err: errcode.New(errcode.ErrPermissionDeny, "not in roster")}
	uc.SetBattleTicketAuthorizer(reject)
	if _, err := uc.IssueDSTicket(context.Background(), 1001, string(auth.DSTypeBattle), 9001, ""); errcode.As(err) != errcode.ErrPermissionDeny {
		t.Fatalf("non-member code=%v err=%v", errcode.As(err), err)
	}
	if reject.calls.Load() != 1 {
		t.Fatalf("authorizer calls=%d", reject.calls.Load())
	}
	allow := &battleTicketAuthorizerFake{}
	uc.SetBattleTicketAuthorizer(allow)
	if _, err := uc.IssueDSTicket(context.Background(), 1001, string(auth.DSTypeBattle), 9001, ""); err != nil {
		t.Fatalf("authorized battle ticket: %v", err)
	}
	empty := &battleTicketAuthorizerFake{returnEmpty: true}
	uc.SetBattleTicketAuthorizer(empty)
	if _, err := uc.IssueDSTicket(context.Background(), 1001, string(auth.DSTypeBattle), 9001, ""); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("empty authoritative address code=%v err=%v", errcode.As(err), err)
	}
}

func TestVerifyBoundHubTicketConcurrentSingleUse(t *testing.T) {
	jti := &concurrentJTIRepo{}
	checker := &bindingCheckerFake{}
	uc, ticket, binding := newBoundTicketUsecase(t, jti, checker, true)

	const workers = 32
	var okCount atomic.Int32
	var replayCount atomic.Int32
	var otherCount atomic.Int32
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_, err := uc.VerifyDSTicket(context.Background(), ticket, binding.DSPodName)
			switch errcode.As(err) {
			case errcode.OK:
				okCount.Add(1)
			case errcode.ErrLoginTicketReplayed:
				replayCount.Add(1)
			default:
				otherCount.Add(1)
			}
		}()
	}
	wg.Wait()
	if okCount.Load() != 1 || replayCount.Load() != workers-1 || otherCount.Load() != 0 {
		t.Fatalf("ok=%d replay=%d other=%d", okCount.Load(), replayCount.Load(), otherCount.Load())
	}
	if jti.calls() != workers || checker.calls.Load() != workers {
		t.Fatalf("MarkUsed=%d checker=%d, want=%d", jti.calls(), checker.calls.Load(), workers)
	}
}

func TestLoginBindingActivationNeverFallsBackToSelfSignedHubTicket(t *testing.T) {
	t.Run("allocator-not-configured", func(t *testing.T) {
		uc := newTestUsecase(t, nil)
		uc.SetRequireHubAssignmentBinding(true)
		if _, _, _, err := uc.ResolveHubEndpoint(context.Background(), 1001, ""); errcode.As(err) != errcode.ErrUnavailable {
			t.Fatalf("code=%v err=%v", errcode.As(err), err)
		}
	})

	t.Run("allocator-failure", func(t *testing.T) {
		hub := &fakeHubAssigner{err: errcode.New(errcode.ErrHubNoAvailable, "full")}
		uc := newTestUsecase(t, hub)
		uc.SetRequireHubAssignmentBinding(true)
		if _, _, _, err := uc.ResolveHubEndpoint(context.Background(), 1001, ""); errcode.As(err) != errcode.ErrUnavailable {
			t.Fatalf("code=%v err=%v", errcode.As(err), err)
		}
	})

	t.Run("old-allocator-unbound-ticket", func(t *testing.T) {
		hub := &fakeHubAssigner{res: &data.HubAssignment{
			HubDSAddr:  "10.0.0.1:7777",
			HubPodName: "hub-cn-1",
			ShardID:    1,
		}}
		uc := newTestUsecase(t, hub)
		legacy, _, err := uc.signer.SignDSTicket(1001, auth.DSTypeHub, 0, "legacy-allocator-jti")
		if err != nil {
			t.Fatal(err)
		}
		hub.res.HubTicket = legacy
		uc.SetRequireHubAssignmentBinding(true)
		if _, _, _, err := uc.ResolveHubEndpoint(context.Background(), 1001, ""); errcode.As(err) != errcode.ErrUnavailable {
			t.Fatalf("code=%v err=%v", errcode.As(err), err)
		}
	})

	t.Run("bound-allocator-ticket", func(t *testing.T) {
		hub := &fakeHubAssigner{res: &data.HubAssignment{
			HubDSAddr:  "10.0.0.1:7777",
			HubPodName: "hub-cn-1",
			ShardID:    1,
		}}
		// P0 Hub 路由门(2026-07-14):B1 下 ResolveHubEndpoint 必须先由 locator 证明玩家
		// 不在 battle 才能签票;本子用例只验证绑定票透传,注入“不在战斗”的 notifier 过门。
		uc := newTestUsecaseWithNotifier(t, hub, &fakeNotifier{})
		bound, _, err := uc.signer.SignBoundHubDSTicket(1001, 0, 0, 0, 0, "bound-allocator-jti", auth.DSTicketBinding{
			DSPodName:       "hub-cn-1",
			DSInstanceUID:   "uid-a",
			ProtocolEpoch:   7,
			CredentialGen:   42,
			CredentialJTI:   "credential-jti-a",
			HubAssignmentID: "assignment-a",
			WriterEpoch:     2,
		})
		if err != nil {
			t.Fatal(err)
		}
		hub.res.HubTicket = bound
		uc.SetRequireHubAssignmentBinding(true)
		addr, ticket, _, err := uc.ResolveHubEndpoint(context.Background(), 1001, "")
		if err != nil || addr != hub.res.HubDSAddr || ticket != bound {
			t.Fatalf("addr=%q ticketMatch=%v err=%v", addr, ticket == bound, err)
		}
	})
}

func admissionBinding(dsType auth.DSType, matchID uint64, pod, uid string, epoch uint32, gen uint64, credentialJTI string) data.DSAdmissionBinding {
	return data.DSAdmissionBinding{
		DSType: dsType, MatchID: matchID, PodName: pod, InstanceUID: uid, ProtocolEpoch: epoch,
		CredentialGen: gen, CredentialJTI: credentialJTI, ExpMs: time.Now().Add(time.Hour).UnixMilli(),
		Kid: "kid", TokenSHA256: "token-hash", WriterEpoch: auth.DSAuthWriterEpochV2,
		PlayerIDs: func() []uint64 {
			if dsType == auth.DSTypeBattle {
				return []uint64{1001, 1002}
			}
			return nil
		}(),
	}
}

func TestVerifyDSTicketAdmissionBeforeMarkerAndStableRotationRetry(t *testing.T) {
	const admissionID = "123e4567-e89b-42d3-a456-426614174000"
	cfg := auth.Config{Secret: []byte(testSecret)}
	signer, _ := auth.NewSigner(cfg)
	verifier, _ := auth.NewVerifier(cfg)

	t.Run("wrong-battle-match-zero-marker", func(t *testing.T) {
		repo := &admissionReplayRepo{}
		uc := NewTicketUsecase(signer, verifier, repo)
		ticket, _, err := signer.SignDSTicket(1001, auth.DSTypeBattle, 9001, "battle-entry-jti")
		if err != nil {
			t.Fatal(err)
		}
		admission := admissionBinding(auth.DSTypeBattle, 9002, "battle-2", "uid-b", 4, 8, "cred-b")
		if _, err := uc.VerifyDSTicketForAdmission(context.Background(), ticket, admission.PodName, admissionID, admission); errcode.As(err) != errcode.ErrLoginTicketInvalid {
			t.Fatalf("code=%v err=%v", errcode.As(err), err)
		}
		_, marks := repo.counts()
		if marks != 0 {
			t.Fatalf("wrong match wrote marker %d times", marks)
		}
	})

	for _, tc := range []struct {
		name   string
		roster []uint64
		want   errcode.Code
	}{
		{"non-member", []uint64{2001, 2002}, errcode.ErrLoginTicketInvalid},
		{"empty-roster", nil, errcode.ErrUnauthorized},
	} {
		t.Run(tc.name+"-zero-marker", func(t *testing.T) {
			repo := &admissionReplayRepo{}
			uc := NewTicketUsecase(signer, verifier, repo)
			ticket, _, _ := signer.SignDSTicket(1001, auth.DSTypeBattle, 9001, "roster-entry-jti-"+tc.name)
			admission := admissionBinding(auth.DSTypeBattle, 9001, "battle-1", "uid-b", 4, 8, "cred-b")
			admission.PlayerIDs = tc.roster
			if _, err := uc.VerifyDSTicketForAdmission(context.Background(), ticket, admission.PodName, admissionID, admission); errcode.As(err) != tc.want {
				t.Fatalf("code=%v want=%v err=%v", errcode.As(err), tc.want, err)
			}
			_, marks := repo.counts()
			if marks != 0 {
				t.Fatalf("rejected roster wrote marker %d times", marks)
			}
		})
	}

	t.Run("same-attempt-roster-drift-rejected", func(t *testing.T) {
		repo := &admissionReplayRepo{}
		uc := NewTicketUsecase(signer, verifier, repo)
		ticket, _, _ := signer.SignDSTicket(1001, auth.DSTypeBattle, 9001, "roster-drift-jti")
		admission := admissionBinding(auth.DSTypeBattle, 9001, "battle-1", "uid-b", 4, 8, "cred-b")
		if _, err := uc.VerifyDSTicketForAdmission(context.Background(), ticket, admission.PodName, admissionID, admission); err != nil {
			t.Fatal(err)
		}
		admission.PlayerIDs = []uint64{2001, 2002}
		if _, err := uc.VerifyDSTicketForAdmission(context.Background(), ticket, admission.PodName, admissionID, admission); errcode.As(err) != errcode.ErrLoginTicketInvalid {
			t.Fatalf("code=%v err=%v", errcode.As(err), err)
		}
		_, marks := repo.counts()
		if marks != 1 {
			t.Fatalf("roster drift retry rewrote marker, marks=%d", marks)
		}
	})

	t.Run("hub-response-lost-then-active-rotation", func(t *testing.T) {
		repo := &admissionReplayRepo{}
		checker := &capturingAssignmentChecker{}
		uc := NewTicketUsecase(signer, verifier, repo)
		uc.SetHubAssignmentBindingPolicy(true, checker)
		oldBinding := auth.DSTicketBinding{
			DSPodName: "hub-1", DSInstanceUID: "uid-h", ProtocolEpoch: 7,
			CredentialGen: 11, CredentialJTI: "cred-old", HubAssignmentID: "assignment-1",
			WriterEpoch: auth.DSAuthWriterEpochV2,
		}
		ticket, _, err := signer.SignBoundHubDSTicket(1001, 0, 0, 0, 0, "hub-entry-jti", oldBinding)
		if err != nil {
			t.Fatal(err)
		}
		first := admissionBinding(auth.DSTypeHub, 0, "hub-1", "uid-h", 7, 11, "cred-old")
		if _, err := uc.VerifyDSTicketForAdmission(context.Background(), ticket, first.PodName, admissionID, first); err != nil {
			t.Fatalf("first admission: %v", err)
		}
		rotated := admissionBinding(auth.DSTypeHub, 0, "hub-1", "uid-h", 7, 12, "cred-new")
		rotated.Kid = "kid-new"
		rotated.TokenSHA256 = "token-hash-new"
		if _, err := uc.VerifyDSTicketForAdmission(context.Background(), ticket, rotated.PodName, admissionID, rotated); err != nil {
			t.Fatalf("same admission after rotation: %v", err)
		}
		if checker.last.AssignmentID != oldBinding.HubAssignmentID || checker.last.CredentialGen != rotated.CredentialGen ||
			checker.last.CredentialJTI != rotated.CredentialJTI {
			t.Fatalf("retry did not validate current assignment credential: %+v", checker.last)
		}
		_, marks := repo.counts()
		if marks != 2 {
			t.Fatalf("same-attempt retry must confirm (not rewrite) marker, marks=%d", marks)
		}
	})

	t.Run("missing-marker-old-ticket-new-active-strict-reject", func(t *testing.T) {
		repo := &admissionReplayRepo{}
		uc := NewTicketUsecase(signer, verifier, repo)
		uc.SetHubAssignmentBindingPolicy(true, &capturingAssignmentChecker{})
		oldBinding := auth.DSTicketBinding{DSPodName: "hub-1", DSInstanceUID: "uid-h", ProtocolEpoch: 7,
			CredentialGen: 11, CredentialJTI: "cred-old", HubAssignmentID: "assignment-1", WriterEpoch: 2}
		ticket, _, _ := signer.SignBoundHubDSTicket(1001, 0, 0, 0, 0, "strict-entry-jti", oldBinding)
		rotated := admissionBinding(auth.DSTypeHub, 0, "hub-1", "uid-h", 7, 12, "cred-new")
		if _, err := uc.VerifyDSTicketForAdmission(context.Background(), ticket, rotated.PodName, admissionID, rotated); errcode.As(err) != errcode.ErrLoginTicketInvalid {
			t.Fatalf("code=%v err=%v", errcode.As(err), err)
		}
		_, marks := repo.counts()
		if marks != 0 {
			t.Fatalf("strict mismatch wrote marker %d times", marks)
		}
	})

	t.Run("different-admission-replay", func(t *testing.T) {
		repo := &admissionReplayRepo{}
		uc := NewTicketUsecase(signer, verifier, repo)
		ticket, _, _ := signer.SignDSTicket(1001, auth.DSTypeBattle, 9001, "different-admission-jti")
		admission := admissionBinding(auth.DSTypeBattle, 9001, "battle-1", "uid-b", 4, 8, "cred-b")
		if _, err := uc.VerifyDSTicketForAdmission(context.Background(), ticket, admission.PodName, admissionID, admission); err != nil {
			t.Fatal(err)
		}
		rebuilt := admission
		rebuilt.InstanceUID = "uid-rebuilt"
		if _, err := uc.VerifyDSTicketForAdmission(context.Background(), ticket, rebuilt.PodName,
			admissionID, rebuilt); errcode.As(err) != errcode.ErrLoginTicketReplayed {
			t.Fatalf("uid rebuild code=%v err=%v", errcode.As(err), err)
		}
		if _, err := uc.VerifyDSTicketForAdmission(context.Background(), ticket, admission.PodName,
			"123e4567-e89b-42d3-a456-426614174001", admission); errcode.As(err) != errcode.ErrLoginTicketReplayed {
			t.Fatalf("code=%v err=%v", errcode.As(err), err)
		}
	})
}

// battle v2 绑定七要件逐项判定:字段名用于 ds_ticket_binding_rejected 的
// mismatch_field,可观测性修复(拆开塌成一句 if 的七条件)不得改变任何判定语义。
func TestBattleV2BindingMismatchField(t *testing.T) {
	matched := &verifiedDSTicket{
		DSPodName: "battle-1", DSInstanceUID: "uid-b", DSInstanceEpoch: 3,
		AllocationID: "alloc-1", ReleaseTrack: "stable",
	}
	authority := data.BattleTicketTarget{
		PodName: "battle-1", InstanceUID: "uid-b", InstanceEpoch: 3,
		AllocationID: "alloc-1", ReleaseTrack: "stable",
	}
	if got := battleV2BindingMismatchField("battle-1", matched, authority); got != "" {
		t.Fatalf("全部一致必须放行, got mismatch %q", got)
	}
	cases := []struct {
		name      string
		caller    string
		mutClaims func(*verifiedDSTicket)
		mutTarget func(*data.BattleTicketTarget)
		want      string
	}{
		{name: "调用方pod为空", caller: "", want: "caller_ds_pod_empty"},
		{name: "调用方pod与票不符", caller: "battle-2", want: "caller_ds_pod"},
		{name: "权威pod漂移", caller: "battle-1",
			mutTarget: func(tg *data.BattleTicketTarget) { tg.PodName = "battle-9" }, want: "pod_name"},
		{name: "同名Pod重建uid变化", caller: "battle-1",
			mutTarget: func(tg *data.BattleTicketTarget) { tg.InstanceUID = "uid-rebuilt" }, want: "instance_uid"},
		{name: "instance_epoch递增", caller: "battle-1",
			mutTarget: func(tg *data.BattleTicketTarget) { tg.InstanceEpoch = 4 }, want: "instance_epoch"},
		{name: "allocation换代", caller: "battle-1",
			mutTarget: func(tg *data.BattleTicketTarget) { tg.AllocationID = "alloc-2" }, want: "allocation_id"},
		{name: "灰度换轨", caller: "battle-1",
			mutTarget: func(tg *data.BattleTicketTarget) { tg.ReleaseTrack = "canary" }, want: "release_track"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			claims := *matched
			target := authority
			if tc.mutClaims != nil {
				tc.mutClaims(&claims)
			}
			if tc.mutTarget != nil {
				tc.mutTarget(&target)
			}
			if got := battleV2BindingMismatchField(tc.caller, &claims, target); got != tc.want {
				t.Fatalf("mismatch field = %q, want %q", got, tc.want)
			}
		})
	}
}
