package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/transport"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/dbguard"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/middleware"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/biz"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/data"
)

var battleCredentialNow = time.UnixMilli(1_800_000_000_000)

const battleCredentialTestSecret = "test-only-ds-callback-secret-32bytes"

type battleAuthReaderStub struct {
	rec       *dsv1.BattleDSAuthStorageRecord
	battle    *dsv1.BattleStorageRecord
	found     bool
	err       error
	recordErr error
	recorded  int
}

func (s *battleAuthReaderStub) GetBattleAuthority(context.Context, uint64) (*dsv1.BattleDSAuthStorageRecord, *dsv1.BattleStorageRecord, bool, error) {
	return s.rec, s.battle, s.found, s.err
}
func (s *battleAuthReaderStub) RecordBattleResult(context.Context, data.BattleResultCredential, time.Duration) error {
	s.recorded++
	return s.recordErr
}

func validBattleCredential() (*middleware.VerifiedCredential, *dsv1.BattleDSAuthStorageRecord) {
	exp := battleCredentialNow.Add(time.Hour).UnixMilli()
	cred := &middleware.VerifiedCredential{DSType: auth.DSTypeBattle, MatchID: 9, Pod: "battle-9", InstanceUID: "uid-9", ProtocolEpoch: 3, Gen: 7, JTI: "j7", ExpMs: exp, TokenSHA256: "hash7", Kid: "kid7", WriterEpoch: auth.DSAuthWriterEpochV2}
	active := &dsv1.BattleDSCredential{Gen: 7, Jti: "j7", ExpMs: uint64(exp), Kid: "kid7", InstanceUid: "uid-9", InstanceEpoch: 3, TokenSha256: "hash7", WriterEpoch: auth.DSAuthWriterEpochV2}
	rec := &dsv1.BattleDSAuthStorageRecord{MatchId: 9, DsPodName: "battle-9", InstanceUid: "uid-9", InstanceEpoch: 3, Phase: dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE, Active: active, HighWaterGen: 7, RequiredWriterEpoch: auth.DSAuthWriterEpochV2, AllocationId: "alloc-9", LastActiveHeartbeatMs: battleCredentialNow.Add(-time.Second).UnixMilli()}
	return cred, rec
}

func futureWriterBattleTokenAndAuthority(t *testing.T) (string, *dsv1.BattleDSAuthStorageRecord, *dsv1.BattleStorageRecord) {
	t.Helper()
	cfg := auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte(battleCredentialTestSecret), NowFn: func() time.Time { return battleCredentialNow },
	}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := signer.SignBattleCredential(9, "battle-9", "uid-9", 3, 7, "j7", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	exp := jwt.NewNumericDate(battleCredentialNow.Add(time.Hour))
	claims := auth.DSCallbackClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: auth.DSCallbackIssuer, Subject: "battle-9",
			Audience: jwt.ClaimStrings{auth.DSCallbackAudience},
			IssuedAt: jwt.NewNumericDate(battleCredentialNow), ExpiresAt: exp, ID: "j7",
		},
		DSType: string(auth.DSTypeBattle), MatchID: 9, DSGen: 7, DSInstanceUID: "uid-9",
		DSProtocolEpoch: 3, DSWriterEpoch: 3, DSKid: seed.Kid,
	}
	tokenObj := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenObj.Header["kid"] = seed.Kid
	token, err := tokenObj.SignedString([]byte(battleCredentialTestSecret))
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(token))
	active := &dsv1.BattleDSCredential{
		Gen: 7, Jti: "j7", ExpMs: uint64(exp.UnixMilli()), Kid: seed.Kid,
		InstanceUid: "uid-9", InstanceEpoch: 3, TokenSha256: hex.EncodeToString(hash[:]), WriterEpoch: 3,
	}
	authRecord := &dsv1.BattleDSAuthStorageRecord{
		MatchId: 9, DsPodName: "battle-9", InstanceUid: "uid-9", InstanceEpoch: 3,
		Phase:  dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING,
		Active: active, Pending: proto.Clone(active).(*dsv1.BattleDSCredential),
		HighWaterGen: 7, RequiredWriterEpoch: auth.DSAuthWriterEpochV2,
		AllocationId: "alloc-9", LastActiveHeartbeatMs: battleCredentialNow.Add(-time.Second).UnixMilli(),
	}
	battle := &dsv1.BattleStorageRecord{
		MatchId: 9, DsPodName: "battle-9", State: "running", AllocationId: "alloc-9",
		GameserverUid: "uid-9", InstanceEpoch: 3, LastVerifiedGen: 7,
		LastVerifiedJti: "j7", LastVerifiedWriterEpoch: 3, PlayerIds: []uint64{1, 2},
	}
	return token, authRecord, battle
}

func TestBattleCredentialStateCheckerStrictTuple(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*middleware.VerifiedCredential, *dsv1.BattleDSAuthStorageRecord, *battleAuthReaderStub)
		want   errcode.Code
	}{
		{name: "active", want: errcode.OK},
		{name: "redis failure", mutate: func(_ *middleware.VerifiedCredential, _ *dsv1.BattleDSAuthStorageRecord, r *battleAuthReaderStub) {
			r.err = errors.New("down")
		}, want: errcode.ErrUnavailable},
		{name: "missing", mutate: func(_ *middleware.VerifiedCredential, _ *dsv1.BattleDSAuthStorageRecord, r *battleAuthReaderStub) {
			r.found = false
		}, want: errcode.ErrUnauthorized},
		{name: "pending", mutate: func(c *middleware.VerifiedCredential, r *dsv1.BattleDSAuthStorageRecord, _ *battleAuthReaderStub) {
			r.Active.Gen = 6
			r.Pending = &dsv1.BattleDSCredential{Gen: c.Gen, Jti: c.JTI}
		}, want: errcode.ErrUnauthorized},
		{name: "wrong uid", mutate: func(c *middleware.VerifiedCredential, _ *dsv1.BattleDSAuthStorageRecord, _ *battleAuthReaderStub) {
			c.InstanceUID = "other"
		}, want: errcode.ErrUnauthorized},
		{name: "wrong epoch", mutate: func(c *middleware.VerifiedCredential, _ *dsv1.BattleDSAuthStorageRecord, _ *battleAuthReaderStub) {
			c.ProtocolEpoch++
		}, want: errcode.ErrUnauthorized},
		{name: "stale gen", mutate: func(c *middleware.VerifiedCredential, _ *dsv1.BattleDSAuthStorageRecord, _ *battleAuthReaderStub) {
			c.Gen--
		}, want: errcode.ErrUnauthorized},
		{name: "future gen", mutate: func(c *middleware.VerifiedCredential, _ *dsv1.BattleDSAuthStorageRecord, _ *battleAuthReaderStub) {
			c.Gen++
		}, want: errcode.ErrUnauthorized},
		{name: "wrong jti", mutate: func(c *middleware.VerifiedCredential, _ *dsv1.BattleDSAuthStorageRecord, _ *battleAuthReaderStub) {
			c.JTI = "other"
		}, want: errcode.ErrUnauthorized},
		{name: "wrong hash", mutate: func(c *middleware.VerifiedCredential, _ *dsv1.BattleDSAuthStorageRecord, _ *battleAuthReaderStub) {
			c.TokenSHA256 = "other"
		}, want: errcode.ErrUnauthorized},
		{name: "low writer", mutate: func(c *middleware.VerifiedCredential, _ *dsv1.BattleDSAuthStorageRecord, _ *battleAuthReaderStub) {
			c.WriterEpoch = 1
		}, want: errcode.ErrUnauthorized},
		{name: "low required-active-claims writer 1", mutate: func(c *middleware.VerifiedCredential, r *dsv1.BattleDSAuthStorageRecord, reader *battleAuthReaderStub) {
			c.WriterEpoch = 1
			r.RequiredWriterEpoch = 1
			r.Active.WriterEpoch = 1
			r.Pending = proto.Clone(r.Active).(*dsv1.BattleDSCredential)
			reader.battle.LastVerifiedWriterEpoch = 1
		}, want: errcode.ErrUnauthorized},
		{name: "required 2 future active-claims writer 3", mutate: func(c *middleware.VerifiedCredential, r *dsv1.BattleDSAuthStorageRecord, reader *battleAuthReaderStub) {
			c.WriterEpoch = 3
			r.RequiredWriterEpoch = auth.DSAuthWriterEpochV2
			r.Active.WriterEpoch = 3
			r.Pending = proto.Clone(r.Active).(*dsv1.BattleDSCredential)
			reader.battle.LastVerifiedWriterEpoch = 3
		}, want: errcode.ErrUnauthorized},
		{name: "stale heartbeat", mutate: func(_ *middleware.VerifiedCredential, r *dsv1.BattleDSAuthStorageRecord, _ *battleAuthReaderStub) {
			r.LastActiveHeartbeatMs = battleCredentialNow.Add(-time.Minute).UnixMilli()
		}, want: errcode.ErrUnauthorized},
		{name: "future heartbeat", mutate: func(_ *middleware.VerifiedCredential, r *dsv1.BattleDSAuthStorageRecord, _ *battleAuthReaderStub) {
			r.LastActiveHeartbeatMs = battleCredentialNow.Add(time.Second).UnixMilli()
		}, want: errcode.ErrUnauthorized},
		{name: "quarantined", mutate: func(_ *middleware.VerifiedCredential, r *dsv1.BattleDSAuthStorageRecord, _ *battleAuthReaderStub) {
			r.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_QUARANTINED
		}, want: errcode.ErrUnauthorized},
		{name: "allocation missing", mutate: func(_ *middleware.VerifiedCredential, r *dsv1.BattleDSAuthStorageRecord, _ *battleAuthReaderStub) {
			r.AllocationId = ""
		}, want: errcode.ErrUnauthorized},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cred, rec := validBattleCredential()
			battle := &dsv1.BattleStorageRecord{
				MatchId: 9, DsPodName: "battle-9", State: "running", AllocationId: "alloc-9",
				GameserverUid: "uid-9", InstanceEpoch: 3, LastVerifiedGen: 7,
				LastVerifiedJti: "j7", LastVerifiedWriterEpoch: auth.DSAuthWriterEpochV2,
				PlayerIds: []uint64{1, 2},
			}
			reader := &battleAuthReaderStub{rec: rec, battle: battle, found: true}
			if tc.mutate != nil {
				tc.mutate(cred, rec, reader)
			}
			checker := &redisBattleCredentialStateChecker{reader: reader, now: func() time.Time { return battleCredentialNow }, maxActiveHeartbeatAge: 30 * time.Second}
			if got := errcode.As(checker.CheckActive(context.Background(), 9, cred)); got != tc.want {
				t.Fatalf("code=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestBattleResultRecorderRejectsFutureWriterBeforeRecorderCall(t *testing.T) {
	cred, _ := validBattleCredential()
	cred.WriterEpoch = 3
	reader := &battleAuthReaderStub{found: true}
	checker := &redisBattleCredentialStateChecker{
		reader: reader, recorder: reader, now: func() time.Time { return battleCredentialNow },
		maxActiveHeartbeatAge: 30 * time.Second,
	}
	if err := checker.MarkResultRecorded(context.Background(), 9, cred); errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("code=%v err=%v", errcode.As(err), err)
	}
	if reader.recorded != 0 {
		t.Fatalf("future writer reached receipt recorder: calls=%d", reader.recorded)
	}
}

func TestBattleCredentialCheckerPreservesUnknownFieldsReadOnly(t *testing.T) {
	_, rec := validBattleCredential()
	raw, err := proto.Marshal(rec)
	if err != nil || len(raw) == 0 {
		t.Fatalf("marshal err=%v", err)
	}
	// checker 只读，不存在 read-modify-write；此测试锁定完整 proto 可解码基线。
}

type resultTestHeader map[string][]string

func (h resultTestHeader) Get(k string) string {
	if len(h[k]) > 0 {
		return h[k][0]
	}
	return ""
}
func (h resultTestHeader) Set(k, v string) { h[k] = []string{v} }
func (h resultTestHeader) Add(k, v string) { h[k] = append(h[k], v) }
func (h resultTestHeader) Keys() []string {
	out := make([]string, 0, len(h))
	for k := range h {
		out = append(out, k)
	}
	return out
}
func (h resultTestHeader) Values(k string) []string { return h[k] }

type resultTestTransport struct{ request resultTestHeader }

func (*resultTestTransport) Kind() transport.Kind { return transport.KindGRPC }
func (*resultTestTransport) Endpoint() string     { return "" }
func (*resultTestTransport) Operation() string {
	return "/pandora.battle.v1.BattleResultService/ReportResult"
}
func (t *resultTestTransport) RequestHeader() transport.Header { return t.request }
func (*resultTestTransport) ReplyHeader() transport.Header     { return resultTestHeader{} }

type serviceBattleRepo struct {
	saveErr  error
	saved    bool
	terminal *data.TerminalReleaseRecord
	// result 是首笔落库时的 BattleResult 快照(biz 覆盖 canonical 元数据之后)。
	result *battlev1.BattleResult
}

func (r *serviceBattleRepo) SaveResult(
	_ context.Context,
	result *battlev1.BattleResult,
	_ []data.OutboxRecord,
	_ []data.DropOutboxRecord,
	terminal *data.TerminalReleaseRecord,
	_ uint64,
) (bool, data.ProgressSettleInfo, error) {
	if r.saveErr != nil {
		return false, data.ProgressSettleInfo{}, r.saveErr
	}
	if r.saved {
		return true, data.ProgressSettleInfo{}, nil
	}
	r.saved = true
	r.result = proto.Clone(result).(*battlev1.BattleResult)
	if terminal != nil {
		copyRecord := *terminal
		copyRecord.ID = 1
		r.terminal = &copyRecord
	}
	return false, data.ProgressSettleInfo{}, nil
}

func (*serviceBattleRepo) GetResult(context.Context, uint64) (*battlev1.BattleResult, bool, error) {
	return nil, false, nil
}
func (*serviceBattleRepo) ListPlayerHistory(context.Context, uint64, int, int64) ([]*battlev1.BattleResult, error) {
	return nil, nil
}
func (*serviceBattleRepo) FetchOutbox(context.Context, int) ([]data.OutboxRecord, error) {
	return nil, nil
}
func (*serviceBattleRepo) DeleteOutbox(context.Context, int64) error { return nil }
func (*serviceBattleRepo) FetchDropOutbox(context.Context, int) ([]data.DropOutboxRecord, error) {
	return nil, nil
}
func (*serviceBattleRepo) DeleteDropOutbox(context.Context, int64) error { return nil }
func (*serviceBattleRepo) FetchTerminalReleaseOutbox(context.Context, int, int64) ([]data.TerminalReleaseRecord, error) {
	return nil, nil
}
func (*serviceBattleRepo) MarkTerminalReleaseReleased(context.Context, uint64, int64) (bool, error) {
	return false, nil
}
func (*serviceBattleRepo) DeleteTerminalReleaseOutbox(context.Context, uint64) error { return nil }
func (*serviceBattleRepo) FetchMatchReleaseOutbox(context.Context, int, int64) ([]data.MatchReleaseRecord, error) {
	return nil, nil
}
func (*serviceBattleRepo) DeferMatchReleaseOutbox(context.Context, uint64, int64) error { return nil }
func (*serviceBattleRepo) DeleteMatchReleaseOutbox(context.Context, uint64) error       { return nil }
func (*serviceBattleRepo) GetProgressWatermark(context.Context, uint64) (data.ProgressWatermark, error) {
	return data.ProgressWatermark{}, nil
}
func (*serviceBattleRepo) DeferProgressOutbox(context.Context, int64) error  { return nil }
func (*serviceBattleRepo) MarkProgressStopped(context.Context, uint64) error { return nil }
func (*serviceBattleRepo) ClaimProgressLegacy(context.Context, uint64) (bool, error) {
	return true, nil
}
func (*serviceBattleRepo) ApplyProgress(context.Context, uint64, uint64, uint64, uint64, uint32, []data.ProgressPlayerDelta, []data.ProgressOutboxRecord, data.ProgressCaps) error {
	return nil
}
func (*serviceBattleRepo) FetchProgressOutbox(context.Context, int) ([]data.ProgressOutboxRecord, error) {
	return nil, nil
}
func (*serviceBattleRepo) FetchProgressOutboxForPlayer(context.Context, uint64, uint64, uint64) (data.ProgressOutboxRecord, bool, error) {
	return data.ProgressOutboxRecord{}, false, nil
}
func (*serviceBattleRepo) GetProgressAction(context.Context, uint64, uint64, uint64, data.ProgressGrantKind) (data.ProgressAction, bool, error) {
	return data.ProgressAction{}, false, nil
}
func (*serviceBattleRepo) ResolveProgressAction(context.Context, data.ProgressOutboxRecord, errcode.Code) (data.ProgressAction, error) {
	return data.ProgressAction{}, nil
}
func (*serviceBattleRepo) DeleteProgressOutbox(context.Context, int64) error { return nil }

// 保留期清理(§9.24):service 单测不涉及,默认 no-op。
func (*serviceBattleRepo) SweepExpiredBattles(_ context.Context, mode dbguard.Mode, _ int64, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}
func (*serviceBattleRepo) SweepSettledProgress(_ context.Context, mode dbguard.Mode, _ int64, _ int) (dbguard.Outcome, error) {
	return dbguard.Outcome{Mode: mode}, nil
}
func (*serviceBattleRepo) CountStaleUnsettledProgress(context.Context, int64) (int64, error) {
	return 0, nil
}

type guardedReportFixture struct {
	svc    *BattleResultService
	repo   *serviceBattleRepo
	reader *battleAuthReaderStub
	signer *auth.Signer
	token  string
	now    time.Time
	req    *battlev1.ReportResultRequest
}

func newGuardedReportFixture(t *testing.T) *guardedReportFixture {
	t.Helper()
	reportNow := time.Now().Add(-time.Second).Truncate(time.Second)
	authCfg := auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte(battleCredentialTestSecret), NowFn: func() time.Time { return reportNow },
	}
	signer, err := auth.NewSigner(authCfg)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewVerifier(authCfg)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := middleware.NewDSCallbackGuard(verifier, middleware.DSAuthEnforce)
	if err != nil {
		t.Fatal(err)
	}
	issued, err := signer.SignBattleCredential(9, "battle-9", "uid-9", 3, 7, "j7", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	active := &dsv1.BattleDSCredential{
		Gen: 7, Jti: "j7", ExpMs: uint64(issued.ExpMs), Kid: issued.Kid,
		InstanceUid: "uid-9", InstanceEpoch: 3, TokenSha256: issued.TokenSHA256,
		WriterEpoch: issued.WriterEpoch,
	}
	reader := &battleAuthReaderStub{
		found: true,
		rec: &dsv1.BattleDSAuthStorageRecord{
			MatchId: 9, DsPodName: "battle-9", InstanceUid: "uid-9", InstanceEpoch: 3,
			Phase: dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE, Active: active,
			HighWaterGen: 7, RequiredWriterEpoch: auth.DSAuthWriterEpochV2,
			AllocationId: "alloc-9", LastActiveHeartbeatMs: reportNow.Add(-time.Second).UnixMilli(),
		},
		battle: &dsv1.BattleStorageRecord{
			MatchId: 9, DsPodName: "battle-9", State: "running", AllocationId: "alloc-9",
			GameserverUid: "uid-9", InstanceEpoch: 3, LastVerifiedGen: 7,
			LastVerifiedJti: "j7", LastVerifiedWriterEpoch: auth.DSAuthWriterEpochV2,
			PlayerIds: []uint64{1, 2},
		},
	}
	repo := &serviceBattleRepo{}
	battleCfg := conf.BattleConf{
		EloKFactor: 32, BaseMMR: 1500,
		TerminalReleaseGrace: config.Duration(5 * time.Second),
	}
	uc := biz.NewBattleResultUsecase(repo, biz.NewStaticMMRReader(1500), nil, nil, battleCfg)
	svc := NewBattleResultService(uc)
	svc.SetDSCallbackGuard(guard)
	svc.SetBattleCredentialStateChecker(&redisBattleCredentialStateChecker{
		reader: reader, recorder: reader, now: func() time.Time { return reportNow },
		maxActiveHeartbeatAge: 30 * time.Second,
	})
	req := &battlev1.ReportResultRequest{Result: &battlev1.BattleResult{
		MatchId: 9, DsPodName: "battle-9", WinnerTeam: 0, EndedAtMs: reportNow.UnixMilli(),
		Stats: []*battlev1.PlayerStats{{PlayerId: 1, Team: 0}, {PlayerId: 2, Team: 1}},
	}}
	return &guardedReportFixture{svc: svc, repo: repo, reader: reader, signer: signer, token: issued.Token, now: reportNow, req: req}
}

func guardedReportContext(token string) context.Context {
	header := resultTestHeader{}
	header.Set("authorization", "Bearer "+token)
	header.Set(middleware.MetadataKeyDSGateway, "1")
	return transport.NewServerContext(context.Background(), &resultTestTransport{request: header})
}

func signedBattleToken(t *testing.T, signer *auth.Signer, gen uint64, jti string) auth.HubCredentialResult {
	t.Helper()
	issued, err := signer.SignBattleCredential(9, "battle-9", "uid-9", 3, gen, jti, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return issued
}

type rejectingBattleChecker struct{ calls int }

func (c *rejectingBattleChecker) CheckActive(context.Context, uint64, *middleware.VerifiedCredential) error {
	return errcode.New(errcode.ErrUnauthorized, "stale")
}
func (c *rejectingBattleChecker) AuthorizeResult(context.Context, uint64, *middleware.VerifiedCredential) (data.TerminalReleaseRecord, error) {
	c.calls++
	return data.TerminalReleaseRecord{}, errcode.New(errcode.ErrUnauthorized, "stale")
}
func (c *rejectingBattleChecker) MarkResultRecorded(context.Context, uint64, *middleware.VerifiedCredential) error {
	panic("must not record a rejected result")
}

func TestReportResultDBCommitSuccessReceiptFailureStillReturnsOKAndKeepsOutbox(t *testing.T) {
	f := newGuardedReportFixture(t)
	f.reader.recordErr = errors.New("redis receipt write unavailable")
	resp, err := f.svc.ReportResult(guardedReportContext(f.token), f.req)
	if err != nil || resp.GetCode() != commonv1.ErrCode_OK || resp.GetAlreadyRecorded() {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
	if !f.repo.saved || f.repo.terminal == nil || f.reader.recorded != 1 {
		t.Fatalf("durable success missing: saved=%v terminal=%+v receipt_calls=%d",
			f.repo.saved, f.repo.terminal, f.reader.recorded)
	}
	if f.repo.terminal.AuthGen != 7 || f.repo.terminal.AuthJTI != "j7" ||
		f.repo.terminal.ReleaseAfterMs <= f.now.UnixMilli() {
		t.Fatalf("durable terminal proof invalid: %+v", f.repo.terminal)
	}
}

func TestReportResultDBCommitFailureNeverReturnsOKOrWritesReceipt(t *testing.T) {
	f := newGuardedReportFixture(t)
	f.repo.saveErr = errors.New("mysql commit failed")
	resp, err := f.svc.ReportResult(guardedReportContext(f.token), f.req)
	if err != nil || resp.GetCode() == commonv1.ErrCode_OK {
		t.Fatalf("DB failure response=%+v err=%v", resp, err)
	}
	if f.repo.saved || f.repo.terminal != nil || f.reader.recorded != 0 {
		t.Fatalf("DB failure leaked success side effects: saved=%v terminal=%+v receipt_calls=%d",
			f.repo.saved, f.repo.terminal, f.reader.recorded)
	}
}

func TestReportResultRejectsRosterDriftBeforeAnySideEffect(t *testing.T) {
	tests := []struct {
		name  string
		stats []*battlev1.PlayerStats
	}{
		{name: "missing canonical player", stats: []*battlev1.PlayerStats{{PlayerId: 1, Team: 0}}},
		{name: "outsider replaces canonical player", stats: []*battlev1.PlayerStats{{PlayerId: 1, Team: 0}, {PlayerId: 3, Team: 1}}},
		{name: "duplicate canonical player", stats: []*battlev1.PlayerStats{{PlayerId: 1, Team: 0}, {PlayerId: 1, Team: 1}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newGuardedReportFixture(t)
			req := proto.Clone(f.req).(*battlev1.ReportResultRequest)
			req.Result.Stats = tc.stats
			resp, err := f.svc.ReportResult(guardedReportContext(f.token), req)
			if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED {
				t.Fatalf("response=%+v err=%v", resp, err)
			}
			if f.repo.saved || f.repo.terminal != nil || f.reader.recorded != 0 {
				t.Fatalf("rejected roster leaked side effects: saved=%v terminal=%+v receipt_calls=%d",
					f.repo.saved, f.repo.terminal, f.reader.recorded)
			}
		})
	}
}

// TestAuthorizeResultCopiesCanonicalGameModeAndMapID(测试一):AuthorizeResult 必须
// 从已通过精确比对的 canonical BattleStorageRecord 复制 game_mode/map_id 进
// TerminalReleaseRecord(与 roster 同源,不做二次查询、不从 DS 请求补值),
// 且既有 credential tuple / roster 语义不退化。
func TestAuthorizeResultCopiesCanonicalGameModeAndMapID(t *testing.T) {
	cred, rec := validBattleCredential()
	battle := &dsv1.BattleStorageRecord{
		MatchId: 9, DsPodName: "battle-9", State: "running", AllocationId: "alloc-9",
		GameserverUid: "uid-9", InstanceEpoch: 3, LastVerifiedGen: 7,
		LastVerifiedJti: "j7", LastVerifiedWriterEpoch: auth.DSAuthWriterEpochV2,
		PlayerIds: []uint64{2, 1}, GameMode: "pve_coop", MapId: 10,
	}
	checker := &redisBattleCredentialStateChecker{
		reader: &battleAuthReaderStub{rec: rec, battle: battle, found: true},
		now:    func() time.Time { return battleCredentialNow }, maxActiveHeartbeatAge: 30 * time.Second,
	}
	proof, err := checker.AuthorizeResult(context.Background(), 9, cred)
	if err != nil {
		t.Fatalf("AuthorizeResult err: %v", err)
	}
	if proof.GameMode != "pve_coop" || proof.MapID != 10 {
		t.Fatalf("canonical metadata not copied: game_mode=%q map_id=%d", proof.GameMode, proof.MapID)
	}
	if !reflect.DeepEqual(proof.PlayerIDs, []uint64{1, 2}) {
		t.Fatalf("canonical roster drifted: %v", proof.PlayerIDs)
	}
	if proof.MatchID != 9 || proof.AllocationID != "alloc-9" || proof.DSPodName != "battle-9" ||
		proof.GameserverUID != "uid-9" || proof.InstanceEpoch != 3 ||
		proof.AuthGen != 7 || proof.AuthJTI != "j7" || proof.AuthExpMs != cred.ExpMs ||
		proof.AuthKid != "kid7" || proof.AuthTokenSHA256 != "hash7" ||
		proof.AuthWriterEpoch != auth.DSAuthWriterEpochV2 ||
		proof.AuthorizedAtMs != battleCredentialNow.UnixMilli() {
		t.Fatalf("credential tuple regressed: %+v", proof)
	}
}

// TestReportResultPersistsCanonicalPVEOverForgedRequest(测试二端到端):全链
// Guard → AuthorizeResult(复制 canonical)→ ReportAuthorizedResult(覆盖请求体):
// canonical pve_coop 下 DS 伪报 game_mode/map_id/mmr_delta 全部无效,落库以
// canonical 为准且 delta 全 0,terminal release / receipt 仍正常生成。
func TestReportResultPersistsCanonicalPVEOverForgedRequest(t *testing.T) {
	f := newGuardedReportFixture(t)
	f.reader.battle.GameMode = "pve_coop"
	f.reader.battle.MapId = 10
	req := proto.Clone(f.req).(*battlev1.ReportResultRequest)
	req.Result.GameMode = "5v5_ranked"
	req.Result.MapId = 99
	req.Result.Stats[0].MmrDelta = 77
	req.Result.Stats[1].MmrDelta = -77

	resp, err := f.svc.ReportResult(guardedReportContext(f.token), req)
	if err != nil || resp.GetCode() != commonv1.ErrCode_OK || resp.GetAlreadyRecorded() {
		t.Fatalf("resp=%+v err=%v", resp, err)
	}
	if f.repo.result == nil {
		t.Fatal("settlement result not persisted")
	}
	if f.repo.result.GetGameMode() != "pve_coop" || f.repo.result.GetMapId() != 10 {
		t.Fatalf("persisted metadata not canonical: game_mode=%q map_id=%d",
			f.repo.result.GetGameMode(), f.repo.result.GetMapId())
	}
	for _, s := range f.repo.result.GetStats() {
		if s.GetMmrDelta() != 0 {
			t.Fatalf("canonical pve player %d mmr_delta got %d want 0", s.GetPlayerId(), s.GetMmrDelta())
		}
	}
	if f.repo.terminal == nil || f.repo.terminal.GameMode != "pve_coop" || f.repo.terminal.MapID != 10 {
		t.Fatalf("terminal proof missing canonical metadata: %+v", f.repo.terminal)
	}
	if f.reader.recorded != 1 {
		t.Fatalf("immediate receipt calls=%d want 1", f.reader.recorded)
	}
}

func TestBattleCredentialRejectsCorruptAuthorityRoster(t *testing.T) {
	for _, roster := range [][]uint64{nil, {0, 1}, {1, 1}} {
		cred, rec := validBattleCredential()
		battle := &dsv1.BattleStorageRecord{
			MatchId: 9, DsPodName: "battle-9", State: "running", AllocationId: "alloc-9",
			GameserverUid: "uid-9", InstanceEpoch: 3, LastVerifiedGen: 7,
			LastVerifiedJti: "j7", LastVerifiedWriterEpoch: auth.DSAuthWriterEpochV2,
			PlayerIds: roster,
		}
		checker := &redisBattleCredentialStateChecker{
			reader: &battleAuthReaderStub{rec: rec, battle: battle, found: true},
			now:    func() time.Time { return battleCredentialNow }, maxActiveHeartbeatAge: 30 * time.Second,
		}
		if _, err := checker.AuthorizeResult(context.Background(), 9, cred); errcode.As(err) != errcode.ErrUnauthorized {
			t.Fatalf("roster=%v code=%v err=%v", roster, errcode.As(err), err)
		}
	}
}

func TestReportResultRotatedCredentialReplayDoesNotReplaceDurableProof(t *testing.T) {
	f := newGuardedReportFixture(t)
	first, err := f.svc.ReportResult(guardedReportContext(f.token), f.req)
	if err != nil || first.GetCode() != commonv1.ErrCode_OK || first.GetAlreadyRecorded() {
		t.Fatalf("first response=%+v err=%v", first, err)
	}
	original := *f.repo.terminal
	if f.reader.recorded != 1 {
		t.Fatalf("first receipt calls=%d", f.reader.recorded)
	}

	rotated := signedBattleToken(t, f.signer, 8, "j8")
	f.reader.rec.Active = &dsv1.BattleDSCredential{
		Gen: 8, Jti: "j8", ExpMs: uint64(rotated.ExpMs), Kid: rotated.Kid,
		InstanceUid: "uid-9", InstanceEpoch: 3, TokenSha256: rotated.TokenSHA256,
		WriterEpoch: rotated.WriterEpoch,
	}
	f.reader.rec.HighWaterGen = 8
	f.reader.battle.LastVerifiedGen = 8
	f.reader.battle.LastVerifiedJti = "j8"
	f.reader.battle.LastVerifiedWriterEpoch = rotated.WriterEpoch
	second, err := f.svc.ReportResult(
		guardedReportContext(rotated.Token),
		&battlev1.ReportResultRequest{Result: proto.Clone(f.req.GetResult()).(*battlev1.BattleResult)},
	)
	if err != nil || second.GetCode() != commonv1.ErrCode_OK || !second.GetAlreadyRecorded() {
		t.Fatalf("rotated replay response=%+v err=%v", second, err)
	}
	if f.reader.recorded != 1 {
		t.Fatalf("idempotent replay rewrote immediate receipt: calls=%d", f.reader.recorded)
	}
	if f.repo.terminal == nil || !reflect.DeepEqual(*f.repo.terminal, original) ||
		f.repo.terminal.AuthGen != 7 || f.repo.terminal.AuthJTI != "j7" {
		t.Fatalf("rotated replay replaced durable proof: before=%+v after=%+v", original, f.repo.terminal)
	}
}

func TestReportResultChecksActiveBeforeUsecaseSideEffects(t *testing.T) {
	cfg := auth.Config{Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience, Secret: []byte(battleCredentialTestSecret), NowFn: func() time.Time { return battleCredentialNow }}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := middleware.NewDSCallbackGuard(verifier, middleware.DSAuthEnforce)
	if err != nil {
		t.Fatal(err)
	}
	result, err := signer.SignBattleCredential(9, "battle-9", "uid-9", 3, 7, "j7", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	header := resultTestHeader{}
	header.Set("authorization", "Bearer "+result.Token)
	header.Set(middleware.MetadataKeyDSGateway, "1")
	ctx := transport.NewServerContext(context.Background(), &resultTestTransport{request: header})
	checker := &rejectingBattleChecker{}
	svc := NewBattleResultService(nil) // 若门放错到 usecase 之后，此测试会 panic。
	svc.SetDSCallbackGuard(guard)
	svc.SetBattleCredentialStateChecker(checker)
	resp, err := svc.ReportResult(ctx, &battlev1.ReportResultRequest{Result: &battlev1.BattleResult{MatchId: 9, DsPodName: "battle-9"}})
	if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED || checker.calls != 1 {
		t.Fatalf("resp=%v err=%v checker_calls=%d", resp, err, checker.calls)
	}
}

func TestReportResultFutureWriterRejectedBeforeUsecaseAndReceipt(t *testing.T) {
	token, authRecord, battle := futureWriterBattleTokenAndAuthority(t)
	cfg := auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte(battleCredentialTestSecret), NowFn: func() time.Time { return battleCredentialNow },
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := middleware.NewDSCallbackGuard(verifier, middleware.DSAuthEnforce)
	if err != nil {
		t.Fatal(err)
	}
	header := resultTestHeader{}
	header.Set("authorization", "Bearer "+token)
	header.Set(middleware.MetadataKeyDSGateway, "1")
	ctx := transport.NewServerContext(context.Background(), &resultTestTransport{request: header})
	reader := &battleAuthReaderStub{rec: authRecord, battle: battle, found: true}
	checker := &redisBattleCredentialStateChecker{
		reader: reader, recorder: reader, now: func() time.Time { return battleCredentialNow },
		maxActiveHeartbeatAge: 30 * time.Second,
	}
	svc := NewBattleResultService(nil) // future writer 若越过 checker 会在 usecase 处 panic。
	svc.SetDSCallbackGuard(guard)
	svc.SetBattleCredentialStateChecker(checker)
	resp, err := svc.ReportResult(ctx, &battlev1.ReportResultRequest{
		Result: &battlev1.BattleResult{MatchId: 9, DsPodName: "battle-9"},
	})
	if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
	if reader.recorded != 0 {
		t.Fatalf("future writer reached result receipt: calls=%d", reader.recorded)
	}
}
