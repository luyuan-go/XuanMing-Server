package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/transport"
	"github.com/golang-jwt/jwt/v5"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/middleware"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"
	locatorv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/locator/v1"
	"github.com/luyuancpp/pandora/services/runtime/player_locator/internal/biz"
	"github.com/luyuancpp/pandora/services/runtime/player_locator/internal/data"
)

// 刻意带毫秒尾数：JWT NumericDate 默认按秒编码。授权记录 exp_ms 必须来自实际 claim，
// 若签发器错误地存原始 time.Time 毫秒，本测试会让“active 通过”路径精确失败。
var credentialTestNow = time.UnixMilli(1_800_000_000_123)

const credentialTestSecret = "test-only-ds-callback-secret-32bytes"

type stubHubAuthReader struct {
	rec   *hubv1.HubShardAuthStorageRecord
	found bool
	err   error
	calls int
}

func (s *stubHubAuthReader) GetHubAuth(_ context.Context, _ string) (*hubv1.HubShardAuthStorageRecord, bool, error) {
	s.calls++
	return s.rec, s.found, s.err
}

func validCredentialState() (*middleware.VerifiedCredential, *hubv1.HubShardAuthStorageRecord) {
	expMs := credentialTestNow.Add(time.Hour).UnixMilli()
	cred := &middleware.VerifiedCredential{
		Pod:           "hub-1",
		InstanceUID:   "uid-1",
		ProtocolEpoch: 2,
		Gen:           7,
		JTI:           "jti-7",
		ExpMs:         expMs,
		TokenSHA256:   "0123456789abcdef",
		Kid:           "kid-1",
		WriterEpoch:   auth.DSAuthWriterEpochV2,
	}
	rec := &hubv1.HubShardAuthStorageRecord{
		PodName:       "hub-1",
		InstanceUid:   "uid-1",
		ProtocolEpoch: 2,
		Phase:         hubv1.HubAuthPhase_HUB_AUTH_PHASE_ACTIVE,
		Active: &hubv1.HubDSCredential{
			Gen:           7,
			Jti:           "jti-7",
			ExpMs:         uint64(expMs),
			Kid:           "kid-1",
			InstanceUid:   "uid-1",
			ProtocolEpoch: 2,
			TokenSha256:   "0123456789abcdef",
			WriterEpoch:   auth.DSAuthWriterEpochV2,
		},
		HighWaterGen:          7,
		RequiredWriterEpoch:   auth.DSAuthWriterEpochV2,
		LastActiveHeartbeatMs: credentialTestNow.Add(-time.Second).UnixMilli(),
	}
	return cred, rec
}

func TestHubCredentialStateChecker_StrictActiveTuple(t *testing.T) {
	type mutateFn func(*middleware.VerifiedCredential, *hubv1.HubShardAuthStorageRecord, *stubHubAuthReader)
	cases := []struct {
		name     string
		mutate   mutateFn
		wantCode errcode.Code
	}{
		{name: "active 完整匹配", wantCode: errcode.OK},
		{name: "rotating 期间 active 仍有效", mutate: func(_ *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			r.Phase = hubv1.HubAuthPhase_HUB_AUTH_PHASE_ROTATING
		}, wantCode: errcode.OK},
		{name: "auth missing", mutate: func(_ *middleware.VerifiedCredential, _ *hubv1.HubShardAuthStorageRecord, s *stubHubAuthReader) {
			s.rec, s.found = nil, false
		}, wantCode: errcode.ErrUnauthorized},
		{name: "Redis failure", mutate: func(_ *middleware.VerifiedCredential, _ *hubv1.HubShardAuthStorageRecord, s *stubHubAuthReader) {
			s.err = errors.New("redis down")
		}, wantCode: errcode.ErrUnavailable},
		{name: "bootstrap", mutate: func(_ *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			r.Phase = hubv1.HubAuthPhase_HUB_AUTH_PHASE_BOOTSTRAP
		}, wantCode: errcode.ErrUnauthorized},
		{name: "quarantined", mutate: func(_ *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			r.Phase = hubv1.HubAuthPhase_HUB_AUTH_PHASE_QUARANTINED
		}, wantCode: errcode.ErrUnauthorized},
		{name: "wrong record pod", mutate: func(_ *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			r.PodName = "hub-2"
		}, wantCode: errcode.ErrUnauthorized},
		{name: "wrong uid", mutate: func(_ *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			r.InstanceUid = "uid-2"
		}, wantCode: errcode.ErrUnauthorized},
		{name: "wrong epoch", mutate: func(_ *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			r.ProtocolEpoch++
		}, wantCode: errcode.ErrUnauthorized},
		{name: "stale gen", mutate: func(c *middleware.VerifiedCredential, _ *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			c.Gen--
		}, wantCode: errcode.ErrUnauthorized},
		{name: "future gen", mutate: func(c *middleware.VerifiedCredential, _ *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			c.Gen++
		}, wantCode: errcode.ErrUnauthorized},
		{name: "wrong jti", mutate: func(c *middleware.VerifiedCredential, _ *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			c.JTI = "other"
		}, wantCode: errcode.ErrUnauthorized},
		{name: "wrong exp", mutate: func(c *middleware.VerifiedCredential, _ *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			c.ExpMs++
		}, wantCode: errcode.ErrUnauthorized},
		{name: "wrong token hash", mutate: func(c *middleware.VerifiedCredential, _ *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			c.TokenSHA256 = "bad"
		}, wantCode: errcode.ErrUnauthorized},
		{name: "active heartbeat stale", mutate: func(_ *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			r.LastActiveHeartbeatMs = credentialTestNow.Add(-time.Minute).UnixMilli()
		}, wantCode: errcode.ErrUnauthorized},
		{name: "active heartbeat future", mutate: func(_ *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			r.LastActiveHeartbeatMs = credentialTestNow.Add(time.Second).UnixMilli()
		}, wantCode: errcode.ErrUnauthorized},
		{name: "wrong kid", mutate: func(c *middleware.VerifiedCredential, _ *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			c.Kid = "other"
		}, wantCode: errcode.ErrUnauthorized},
		{name: "old writer epoch", mutate: func(c *middleware.VerifiedCredential, _ *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			c.WriterEpoch--
		}, wantCode: errcode.ErrUnauthorized},
		{name: "low required-active-claims writer 1", mutate: func(c *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			c.WriterEpoch = 1
			r.RequiredWriterEpoch = 1
			r.Active.WriterEpoch = 1
			r.Pending = proto.Clone(r.Active).(*hubv1.HubDSCredential)
		}, wantCode: errcode.ErrUnauthorized},
		{name: "required 2 future active-claims writer 3", mutate: func(c *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			c.WriterEpoch = 3
			r.RequiredWriterEpoch = auth.DSAuthWriterEpochV2
			r.Active.WriterEpoch = 3
			r.Pending = proto.Clone(r.Active).(*hubv1.HubDSCredential)
		}, wantCode: errcode.ErrUnauthorized},
		{name: "active nested uid wrong", mutate: func(_ *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			r.Active.InstanceUid = "uid-2"
		}, wantCode: errcode.ErrUnauthorized},
		{name: "active nested epoch wrong", mutate: func(_ *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			r.Active.ProtocolEpoch++
		}, wantCode: errcode.ErrUnauthorized},
		{name: "missing kid", mutate: func(_ *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			r.Active.Kid = ""
		}, wantCode: errcode.ErrUnauthorized},
		{name: "expired active", mutate: func(c *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			c.ExpMs = credentialTestNow.UnixMilli()
			r.Active.ExpMs = uint64(c.ExpMs)
		}, wantCode: errcode.ErrUnauthorized},
		{name: "high water rollback", mutate: func(_ *middleware.VerifiedCredential, r *hubv1.HubShardAuthStorageRecord, _ *stubHubAuthReader) {
			r.HighWaterGen = r.Active.Gen - 1
		}, wantCode: errcode.ErrUnauthorized},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseCred, baseRec := validCredentialState()
			cred := *baseCred
			rec := proto.Clone(baseRec).(*hubv1.HubShardAuthStorageRecord)
			reader := &stubHubAuthReader{rec: rec, found: true}
			if tc.mutate != nil {
				tc.mutate(&cred, rec, reader)
			}
			checker := &redisHubCredentialStateChecker{reader: reader, now: func() time.Time { return credentialTestNow }}
			err := checker.CheckActive(context.Background(), "hub-1", &cred)
			if got := errcode.As(err); got != tc.wantCode {
				t.Fatalf("code=%d want=%d err=%v", got, tc.wantCode, err)
			}
		})
	}

	checker := &redisHubCredentialStateChecker{reader: &stubHubAuthReader{found: true}, now: func() time.Time { return credentialTestNow }}
	if got := errcode.As(checker.CheckActive(context.Background(), "hub-1", nil)); got != errcode.ErrUnauthorized {
		t.Fatalf("nil credential code=%d", got)
	}
}

// 下列 fake 只记录 usecase 是否真正触发 Redis 位置副作用。
type sideEffectRepo struct {
	setCalls      int
	shrinkCalls   int
	refreshCalls  int
	lastSeenCalls int
	activated     data.HubPresenceFence
	shrunkFence   data.HubPresenceFence
}

func (s *sideEffectRepo) SetGuarded(_ context.Context, _ uint64, _ data.LocationRecord, _ time.Duration, _ int, _ func(data.LocationRecord, bool) error) error {
	s.setCalls++
	return nil
}
func (s *sideEffectRepo) Get(context.Context, uint64) (data.LocationRecord, bool, error) {
	return data.LocationRecord{}, false, nil
}
func (s *sideEffectRepo) BatchGet(context.Context, []uint64) (map[uint64]data.LocationRecord, error) {
	return map[uint64]data.LocationRecord{}, nil
}
func (s *sideEffectRepo) RefreshHubLocations(context.Context, string, []uint64, time.Duration, time.Duration) (int, error) {
	s.refreshCalls++
	return 0, nil
}
func (s *sideEffectRepo) ActivateHubPresence(_ context.Context, _ uint64, fence data.HubPresenceFence, _ time.Duration) (bool, error) {
	s.activated = fence
	return true, nil
}
func (s *sideEffectRepo) ValidateHubPresence(context.Context, uint64, data.HubPresenceFence) (bool, error) {
	return true, nil
}
func (s *sideEffectRepo) ShrinkHubTTL(_ context.Context, _ string, _ uint64, fence data.HubPresenceFence, _ time.Duration) (bool, bool, error) {
	s.shrinkCalls++
	s.shrunkFence = fence
	return true, true, nil
}
func (s *sideEffectRepo) RecordLastSeen(_ context.Context, _ uint64, _ data.HubPresenceFence, atMs int64, _ time.Duration) (bool, int64, error) {
	s.lastSeenCalls++
	return true, atMs, nil
}
func (s *sideEffectRepo) BatchGetLastSeen(context.Context, []uint64) (map[uint64]int64, error) {
	return map[uint64]int64{}, nil
}
func (s *sideEffectRepo) Delete(context.Context, uint64) error { return nil }

type testHeader map[string][]string

func (h testHeader) Get(key string) string {
	if v := h[key]; len(v) > 0 {
		return v[0]
	}
	return ""
}
func (h testHeader) Set(key, value string) { h[key] = []string{value} }
func (h testHeader) Add(key, value string) { h[key] = append(h[key], value) }
func (h testHeader) Keys() []string {
	out := make([]string, 0, len(h))
	for k := range h {
		out = append(out, k)
	}
	return out
}
func (h testHeader) Values(key string) []string { return h[key] }

type testTransport struct{ req testHeader }

func (t *testTransport) Kind() transport.Kind            { return transport.KindGRPC }
func (t *testTransport) Endpoint() string                { return "" }
func (t *testTransport) Operation() string               { return "/pandora.locator.v1.PlayerLocatorService/Test" }
func (t *testTransport) RequestHeader() transport.Header { return t.req }
func (t *testTransport) ReplyHeader() transport.Header   { return testHeader{} }

func modelBService(t *testing.T, reader data.HubAuthReader) (*LocatorService, *sideEffectRepo, string) {
	t.Helper()
	cfg := auth.Config{
		Issuer:   auth.DSCallbackIssuer,
		Audience: auth.DSCallbackAudience,
		Secret:   []byte(credentialTestSecret),
		NowFn:    func() time.Time { return credentialTestNow },
	}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	guard, err := middleware.NewDSCallbackGuard(verifier, middleware.DSAuthEnforce)
	if err != nil {
		t.Fatalf("NewDSCallbackGuard: %v", err)
	}
	result, err := signer.SignHubCredential("hub-1", "uid-1", 2, 7, "jti-7", time.Hour)
	if err != nil {
		t.Fatalf("SignHubCredential: %v", err)
	}
	repo := &sideEffectRepo{}
	uc := biz.NewLocatorUsecase(repo, 30*time.Second)
	svc := NewLocatorService(uc)
	svc.SetDSCallbackGuard(guard)
	checker := NewHubCredentialStateChecker(reader).(*redisHubCredentialStateChecker)
	checker.now = func() time.Time { return credentialTestNow }
	svc.SetHubCredentialStateChecker(checker)
	return svc, repo, result.Token
}

func futureWriterHubTokenAndRecord(t *testing.T) (string, *hubv1.HubShardAuthStorageRecord) {
	t.Helper()
	cfg := auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte(credentialTestSecret), NowFn: func() time.Time { return credentialTestNow },
	}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatal(err)
	}
	seed, err := signer.SignHubCredential("hub-1", "uid-1", 2, 7, "jti-7", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	exp := jwt.NewNumericDate(credentialTestNow.Add(time.Hour))
	claims := auth.DSCallbackClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: auth.DSCallbackIssuer, Subject: "hub-1",
			Audience: jwt.ClaimStrings{auth.DSCallbackAudience},
			IssuedAt: jwt.NewNumericDate(credentialTestNow), ExpiresAt: exp, ID: "jti-7",
		},
		DSType: string(auth.DSTypeHub), DSGen: 7, DSInstanceUID: "uid-1",
		DSProtocolEpoch: 2, DSWriterEpoch: 3, DSKid: seed.Kid,
	}
	tokenObj := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenObj.Header["kid"] = seed.Kid
	token, err := tokenObj.SignedString([]byte(credentialTestSecret))
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256([]byte(token))
	active := &hubv1.HubDSCredential{
		Gen: 7, Jti: "jti-7", ExpMs: uint64(exp.UnixMilli()), Kid: seed.Kid,
		InstanceUid: "uid-1", ProtocolEpoch: 2, TokenSha256: hex.EncodeToString(hash[:]), WriterEpoch: 3,
	}
	return token, &hubv1.HubShardAuthStorageRecord{
		PodName: "hub-1", InstanceUid: "uid-1", ProtocolEpoch: 2,
		Phase:  hubv1.HubAuthPhase_HUB_AUTH_PHASE_ROTATING,
		Active: active, Pending: proto.Clone(active).(*hubv1.HubDSCredential),
		HighWaterGen: 7, RequiredWriterEpoch: auth.DSAuthWriterEpochV2,
		LastActiveHeartbeatMs: credentialTestNow.Add(-time.Second).UnixMilli(),
	}
}

func requestContext(token string) context.Context {
	h := testHeader{}
	h.Set(middleware.MetadataKeyDSGateway, "1")
	h.Set("authorization", "Bearer "+token)
	return transport.NewServerContext(context.Background(), &testTransport{req: h})
}

func TestLocatorService_Off模式无Credential仍下传连接Fence(t *testing.T) {
	repo := &sideEffectRepo{}
	uc := biz.NewLocatorUsecase(repo, 30*time.Second)
	svc := NewLocatorService(uc) // dsGuard=nil 即 off；CheckHubCredential 返回 nil cred。
	resp, err := svc.SetLocation(context.Background(), &locatorv1.SetLocationRequest{
		PlayerId: 42,
		Location: &locatorv1.Location{State: locatorv1.LocationState_LOCATION_STATE_HUB, HubPod: "hub-1"},
		HubPresenceFence: &locatorv1.HubPresenceFence{
			AssignmentId: "assignment-42", AdmissionId: "admission-a", AdmissionSeq: 1,
		},
	})
	if err != nil || resp.GetCode() != commonv1.ErrCode_OK || repo.setCalls != 1 ||
		!repo.activated.IsComplete() {
		t.Fatalf("off 模式应连接 fence 应完整下传: resp=%v err=%v effects=%+v", resp, err, repo)
	}
}

func authRecordFromSignerResult(t *testing.T) (*hubv1.HubShardAuthStorageRecord, string) {
	t.Helper()
	cfg := auth.Config{
		Issuer:   auth.DSCallbackIssuer,
		Audience: auth.DSCallbackAudience,
		Secret:   []byte("test-only-ds-callback-secret-32bytes"),
		NowFn:    func() time.Time { return credentialTestNow },
	}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	result, err := signer.SignHubCredential("hub-1", "uid-1", 2, 7, "jti-7", time.Hour)
	if err != nil {
		t.Fatalf("SignHubCredential: %v", err)
	}
	cred := &hubv1.HubDSCredential{
		Gen:           7,
		Jti:           "jti-7",
		ExpMs:         uint64(result.ExpMs),
		Kid:           result.Kid,
		InstanceUid:   "uid-1",
		ProtocolEpoch: 2,
		TokenSha256:   result.TokenSHA256,
		WriterEpoch:   result.WriterEpoch,
	}
	return &hubv1.HubShardAuthStorageRecord{
		PodName:               "hub-1",
		InstanceUid:           "uid-1",
		ProtocolEpoch:         2,
		Phase:                 hubv1.HubAuthPhase_HUB_AUTH_PHASE_ACTIVE,
		Active:                cred,
		HighWaterGen:          7,
		RequiredWriterEpoch:   auth.DSAuthWriterEpochV2,
		LastActiveHeartbeatMs: credentialTestNow.Add(-time.Second).UnixMilli(),
	}, result.Token
}

func TestLocatorService_ActiveCredentialBeforeAnySideEffect(t *testing.T) {
	// 按真实 hub_allocator 路径从 SignHubCredential result 构造 Redis active 记录。
	// modelBService 使用同一固定时钟/密钥/claims，签出的 token 确定性相同。
	active, _ := authRecordFromSignerResult(t)

	t.Run("缺 Bearer 在读取权威和写位置前拒绝", func(t *testing.T) {
		reader := &stubHubAuthReader{rec: active, found: true}
		svc, effects, _ := modelBService(t, reader)
		resp, err := svc.SetLocation(context.Background(), &locatorv1.SetLocationRequest{
			PlayerId: 42,
			Location: &locatorv1.Location{State: locatorv1.LocationState_LOCATION_STATE_HUB, HubPod: "hub-1"},
		})
		if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED {
			t.Fatalf("resp=%v err=%v", resp, err)
		}
		if reader.calls != 0 || effects.setCalls != 0 {
			t.Fatalf("missing bearer caused side effect: authReads=%d setCalls=%d", reader.calls, effects.setCalls)
		}
	})

	t.Run("SetLocation stale token 零写入", func(t *testing.T) {
		stale := proto.Clone(active).(*hubv1.HubShardAuthStorageRecord)
		stale.Active.Jti = "new-active"
		reader := &stubHubAuthReader{rec: stale, found: true}
		svc, effects, tok := modelBService(t, reader)
		resp, err := svc.SetLocation(requestContext(tok), &locatorv1.SetLocationRequest{
			PlayerId: 42,
			Location: &locatorv1.Location{State: locatorv1.LocationState_LOCATION_STATE_HUB, HubPod: "hub-1", ShardId: 1},
		})
		if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED {
			t.Fatalf("resp=%v err=%v", resp, err)
		}
		if effects.setCalls != 0 || effects.shrinkCalls != 0 || effects.refreshCalls != 0 {
			t.Fatalf("rejected request changed locator: %+v", effects)
		}
	})

	t.Run("SetLocation required writer 低版本在写前拒绝", func(t *testing.T) {
		lowRequired := proto.Clone(active).(*hubv1.HubShardAuthStorageRecord)
		lowRequired.RequiredWriterEpoch = 1
		reader := &stubHubAuthReader{rec: lowRequired, found: true}
		svc, effects, tok := modelBService(t, reader)
		resp, err := svc.SetLocation(requestContext(tok), &locatorv1.SetLocationRequest{
			PlayerId: 42,
			Location: &locatorv1.Location{State: locatorv1.LocationState_LOCATION_STATE_HUB, HubPod: "hub-1", ShardId: 1},
		})
		if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED {
			t.Fatalf("resp=%v err=%v", resp, err)
		}
		if effects.setCalls != 0 || effects.shrinkCalls != 0 || effects.refreshCalls != 0 {
			t.Fatalf("low required writer reached locator side effects: %+v", effects)
		}
	})

	t.Run("SetLocation required 2 但 active pending claims writer 3 在写前拒绝", func(t *testing.T) {
		tok, future := futureWriterHubTokenAndRecord(t)
		reader := &stubHubAuthReader{rec: future, found: true}
		svc, effects, _ := modelBService(t, reader)
		resp, err := svc.SetLocation(requestContext(tok), &locatorv1.SetLocationRequest{
			PlayerId: 42,
			Location: &locatorv1.Location{State: locatorv1.LocationState_LOCATION_STATE_HUB, HubPod: "hub-1", ShardId: 1},
		})
		if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED {
			t.Fatalf("resp=%v err=%v", resp, err)
		}
		if effects.setCalls != 0 || effects.shrinkCalls != 0 || effects.refreshCalls != 0 {
			t.Fatalf("future writer reached locator side effects: %+v", effects)
		}
	})

	t.Run("SetLocation Redis failure 零写入", func(t *testing.T) {
		reader := &stubHubAuthReader{found: true, err: errors.New("redis down")}
		svc, effects, tok := modelBService(t, reader)
		resp, err := svc.SetLocation(requestContext(tok), &locatorv1.SetLocationRequest{
			PlayerId: 42,
			Location: &locatorv1.Location{State: locatorv1.LocationState_LOCATION_STATE_HUB, HubPod: "hub-1"},
		})
		if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_UNAVAILABLE {
			t.Fatalf("resp=%v err=%v", resp, err)
		}
		if effects.setCalls != 0 {
			t.Fatalf("Redis auth failure must not call SetGuarded: %d", effects.setCalls)
		}
	})

	t.Run("ReportDisconnect pending token 零 TTL 变更", func(t *testing.T) {
		oldActive := proto.Clone(active).(*hubv1.HubShardAuthStorageRecord)
		oldActive.Active.Gen = 6
		oldActive.Active.Jti = "old"
		oldActive.HighWaterGen = 7
		oldActive.Phase = hubv1.HubAuthPhase_HUB_AUTH_PHASE_ROTATING
		oldActive.Pending = proto.Clone(active.Active).(*hubv1.HubDSCredential)
		reader := &stubHubAuthReader{rec: oldActive, found: true}
		svc, effects, tok := modelBService(t, reader)
		resp, err := svc.ReportDisconnect(requestContext(tok), &locatorv1.ReportDisconnectRequest{HubPod: "hub-1", PlayerId: 42})
		if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED {
			t.Fatalf("resp=%v err=%v", resp, err)
		}
		if effects.shrinkCalls != 0 {
			t.Fatalf("pending token must not shrink TTL: %d", effects.shrinkCalls)
		}
	})

	t.Run("active 通过后才执行写入", func(t *testing.T) {
		reader := &stubHubAuthReader{rec: active, found: true}
		svc, effects, tok := modelBService(t, reader)
		resp, err := svc.SetLocation(requestContext(tok), &locatorv1.SetLocationRequest{
			PlayerId: 42,
			Location: &locatorv1.Location{State: locatorv1.LocationState_LOCATION_STATE_HUB, HubPod: "hub-1"},
			HubPresenceFence: &locatorv1.HubPresenceFence{
				AssignmentId: "assignment-42", AdmissionId: "admission-b", AdmissionSeq: 2,
			},
		})
		if err != nil || resp.GetCode() != commonv1.ErrCode_OK || effects.setCalls != 1 {
			t.Fatalf("resp=%v err=%v setCalls=%d", resp, err, effects.setCalls)
		}
		want := data.HubPresenceFence{AssignmentID: "assignment-42", AdmissionID: "admission-b", AdmissionSeq: 2}
		if !effects.activated.Equal(want) {
			t.Fatalf("service 未完整下传 SetLocation fence: got=%+v want=%+v", effects.activated, want)
		}
	})

	t.Run("active ReportDisconnect 下传 exact fence", func(t *testing.T) {
		reader := &stubHubAuthReader{rec: active, found: true}
		svc, effects, tok := modelBService(t, reader)
		resp, err := svc.ReportDisconnect(requestContext(tok), &locatorv1.ReportDisconnectRequest{
			HubPod: "hub-1", PlayerId: 42,
			HubPresenceFence: &locatorv1.HubPresenceFence{
				AssignmentId: "assignment-42", AdmissionId: "admission-b", AdmissionSeq: 2,
			},
		})
		if err != nil || resp.GetCode() != commonv1.ErrCode_OK || !resp.GetShrunk() {
			t.Fatalf("resp=%v err=%v", resp, err)
		}
		want := data.HubPresenceFence{AssignmentID: "assignment-42", AdmissionID: "admission-b", AdmissionSeq: 2}
		if effects.shrinkCalls != 1 || effects.lastSeenCalls != 1 || !effects.shrunkFence.Equal(want) {
			t.Fatalf("service 未完整下传 Disconnect fence/effects: %+v", effects)
		}
	})

	t.Run("RefreshHubLocations stale token 零续期", func(t *testing.T) {
		stale := proto.Clone(active).(*hubv1.HubShardAuthStorageRecord)
		stale.Active.Jti = "new-active"
		reader := &stubHubAuthReader{rec: stale, found: true}
		svc, effects, tok := modelBService(t, reader)
		resp, err := svc.RefreshHubLocations(requestContext(tok), &locatorv1.RefreshHubLocationsRequest{
			HubPod: "hub-1", PlayerIds: []uint64{42},
		})
		if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED || effects.refreshCalls != 0 {
			t.Fatalf("resp=%v err=%v refreshCalls=%d", resp, err, effects.refreshCalls)
		}
	})

	t.Run("RefreshHubLocations active 通过后才续期", func(t *testing.T) {
		reader := &stubHubAuthReader{rec: active, found: true}
		svc, effects, tok := modelBService(t, reader)
		resp, err := svc.RefreshHubLocations(requestContext(tok), &locatorv1.RefreshHubLocationsRequest{
			HubPod: "hub-1", PlayerIds: []uint64{42},
		})
		if err != nil || resp.GetCode() != commonv1.ErrCode_OK || effects.refreshCalls != 1 {
			t.Fatalf("resp=%v err=%v refreshCalls=%d", resp, err, effects.refreshCalls)
		}
	})

	t.Run("非 HUB 内部路径不误伤", func(t *testing.T) {
		reader := &stubHubAuthReader{err: errors.New("must not be read")}
		svc, effects, _ := modelBService(t, reader)
		resp, err := svc.SetLocation(context.Background(), &locatorv1.SetLocationRequest{
			PlayerId: 42,
			Location: &locatorv1.Location{State: locatorv1.LocationState_LOCATION_STATE_MATCHING, MatchId: 99},
		})
		if err != nil || resp.GetCode() != commonv1.ErrCode_OK || effects.setCalls != 1 || reader.calls != 0 {
			t.Fatalf("resp=%v err=%v setCalls=%d authReads=%d", resp, err, effects.setCalls, reader.calls)
		}
	})
}

func (s *sideEffectRepo) TouchAlive(context.Context, uint64, int64, time.Duration) error { return nil }
