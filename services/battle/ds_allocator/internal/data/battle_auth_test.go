package data

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/dsauthrecord"
	"github.com/luyuancpp/pandora/pkg/errcode"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
)

type battleAuthFixture struct {
	auth   *RedisBattleAuthRepo
	battle *RedisBattleRepo
	rdb    *redis.Client
	mr     *miniredis.Miniredis
	now    time.Time
}

func newBattleAuthFixture(t *testing.T) *battleAuthFixture {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	auth := NewRedisBattleAuthRepo(rdb)
	auth.EnableStrictModelBWrites()
	auth.now = func() time.Time { return now }
	battle := NewRedisBattleRepo(rdb)
	battle.EnableStrictModelBWrites()
	return &battleAuthFixture{
		auth: auth, battle: battle, rdb: rdb, mr: mr, now: now,
	}
}

func (f *battleAuthFixture) setNow(now time.Time) {
	f.now = now
	f.auth.now = func() time.Time { return f.now }
}

func seedModelBBattle(t *testing.T, f *battleAuthFixture, matchID uint64, allocationID, pod, uid string) {
	t.Helper()
	nowMs := f.now.UnixMilli()
	b := sampleBattle(matchID, nowMs)
	b.State = "warming"
	b.DsPodName = pod
	b.AllocationId = allocationID
	b.AllocatedAtMs = nowMs
	b.LastHeartbeatMs = nowMs
	b.GameserverUid = uid
	b.PodUid = "pod-uid-" + uid
	b.ReleaseTrack = "stable"
	b.InstanceEpoch = 0
	b.LastVerifiedGen = 0
	b.LastVerifiedJti = ""
	b.LastVerifiedWriterEpoch = 0
	if err := f.battle.CreateBattle(context.Background(), b, testTTL); err != nil {
		t.Fatalf("seed battle: %v", err)
	}
}

func authBinding(matchID uint64, allocationID, pod, uid string) BattleAuthorityBinding {
	return BattleAuthorityBinding{
		MatchID:             matchID,
		AllocationID:        allocationID,
		PodName:             pod,
		InstanceUID:         uid,
		RequiredWriterEpoch: BattleDSWriterEpochV2,
		AuthTTL:             testTTL,
		BattleTTL:           testTTL,
	}
}

func credentialFor(f *battleAuthFixture, seed BattleCredentialSeed, uid, suffix string) *dsv1.BattleDSCredential {
	return &dsv1.BattleDSCredential{
		Gen:           seed.Gen,
		Jti:           "jti-" + suffix,
		ExpMs:         uint64(f.now.Add(time.Hour).UnixMilli()),
		Kid:           "kid-v2",
		InstanceUid:   uid,
		InstanceEpoch: seed.InstanceEpoch,
		TokenSha256:   "sha256-" + suffix,
		WriterEpoch:   BattleDSWriterEpochV2,
	}
}

func identityFor(pod string, c *dsv1.BattleDSCredential) BattleCredentialIdentity {
	return BattleCredentialIdentity{
		PodName:       pod,
		InstanceUID:   c.InstanceUid,
		InstanceEpoch: c.InstanceEpoch,
		Gen:           c.Gen,
		JTI:           c.Jti,
		ExpMs:         c.ExpMs,
		Kid:           c.Kid,
		TokenSHA256:   c.TokenSha256,
		WriterEpoch:   c.WriterEpoch,
	}
}

func resultAuthorizationProof(id BattleCredentialIdentity, authorizedAtMs int64) BattleResultAuthorizationProof {
	return BattleResultAuthorizationProof{Credential: id, AuthorizedAtMs: authorizedAtMs}
}

func prepareAndStage(t *testing.T, f *battleAuthFixture, matchID uint64, allocationID, pod, uid string, delivered bool) (*dsv1.BattleDSCredential, BattleCredentialIdentity) {
	t.Helper()
	seed, err := f.auth.PrepareCredential(context.Background(), authBinding(matchID, allocationID, pod, uid))
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	cred := credentialFor(f, seed, uid, fmt.Sprintf("%d", seed.Gen))
	if _, err := f.auth.StagePending(context.Background(), BattleStageInput{
		MatchID: matchID, AllocationID: allocationID, Credential: cred, AuthTTL: testTTL,
	}); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if delivered {
		if err := f.auth.MarkDelivered(context.Background(), matchID, allocationID, cred, "rv-1", testTTL); err != nil {
			t.Fatalf("mark delivered: %v", err)
		}
	}
	return cred, identityFor(pod, cred)
}

func activateInput() BattleHeartbeatInput {
	return BattleHeartbeatInput{
		PlayerCount: 3, State: "running", AuthTTL: testTTL, BattleTTL: testTTL,
	}
}

// sameCutoffs 给不区分双阈值语义的既有用例复用:两条 cutoff 同值,行为等价旧单阈值。
func sameCutoffs(ms int64) BattleStaleCutoffs {
	return BattleStaleCutoffs{ActiveHeartbeatMs: ms, WarmingHeartbeatMs: ms}
}

func TestBattlePrepareRejectsExistingLowRequiredWriterWithoutMutation(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(699)
	seedModelBBattle(t, f, matchID, "6b11e033-78a2-4010-87c2-b3d9cd053e49", "pod-low", "uid-low")
	legacy := &dsv1.BattleDSAuthStorageRecord{
		MatchId: matchID, AllocationId: "6b11e033-78a2-4010-87c2-b3d9cd053e49", DsPodName: "pod-low",
		InstanceUid: "uid-low", InstanceEpoch: 1,
		Phase:               dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_BOOTSTRAP,
		RequiredWriterEpoch: 1,
	}
	payload, err := proto.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, battleAuthKey(matchID), payload, testTTL).Err(); err != nil {
		t.Fatal(err)
	}
	authBefore, _ := f.rdb.Get(ctx, battleAuthKey(matchID)).Bytes()
	battleBefore, _ := f.rdb.Get(ctx, battleKey(matchID)).Bytes()
	authTTLBefore := f.rdb.TTL(ctx, battleAuthKey(matchID)).Val()
	battleTTLBefore := f.rdb.TTL(ctx, battleKey(matchID)).Val()

	if _, err := f.auth.PrepareCredential(ctx, authBinding(matchID, "6b11e033-78a2-4010-87c2-b3d9cd053e49", "pod-low", "uid-low")); errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("low required writer must fail closed, code=%v err=%v", errcode.As(err), err)
	}
	authAfter, _ := f.rdb.Get(ctx, battleAuthKey(matchID)).Bytes()
	battleAfter, _ := f.rdb.Get(ctx, battleKey(matchID)).Bytes()
	if !bytes.Equal(authAfter, authBefore) || !bytes.Equal(battleAfter, battleBefore) ||
		f.rdb.TTL(ctx, battleAuthKey(matchID)).Val() != authTTLBefore ||
		f.rdb.TTL(ctx, battleKey(matchID)).Val() != battleTTLBefore ||
		f.mr.Exists(battleAuthGenKey(matchID)) {
		t.Fatal("low required writer rejection mutated auth/battle bytes, TTL, or generation counter")
	}
}

func TestBattlePreactiveAuthorityRemainsPersistentUntilActivation(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(698)
	seedModelBBattle(t, f, matchID, "04bea9ef-ef26-4de6-8475-b1e973f0a475", "pod-persist", "uid-persist")

	seed, err := f.auth.PrepareCredential(ctx,
		authBinding(matchID, "04bea9ef-ef26-4de6-8475-b1e973f0a475", "pod-persist", "uid-persist"))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{battleAuthKey(matchID), battleKey(matchID)} {
		if ttl := f.mr.TTL(key); ttl != 0 {
			t.Fatalf("PrepareCredential restored preactive TTL: key=%s ttl=%v", key, ttl)
		}
	}
	credential := credentialFor(f, seed, "uid-persist", "persist")
	// 模拟滚动窗口里旧 writer 留下的有限 TTL；新 Stage 必须机械升级两键。
	f.mr.SetTTL(battleAuthKey(matchID), time.Hour)
	f.mr.SetTTL(battleKey(matchID), time.Hour)
	if _, err := f.auth.StagePending(ctx, BattleStageInput{
		MatchID: matchID, AllocationID: "04bea9ef-ef26-4de6-8475-b1e973f0a475", Credential: credential, AuthTTL: testTTL,
	}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{battleAuthKey(matchID), battleKey(matchID)} {
		if ttl := f.mr.TTL(key); ttl != 0 {
			t.Fatalf("StagePending did not restore persistent fence: key=%s ttl=%v", key, ttl)
		}
	}
	f.mr.SetTTL(battleAuthKey(matchID), time.Hour)
	f.mr.SetTTL(battleKey(matchID), time.Hour)
	if err := f.auth.MarkDelivered(ctx, matchID, "04bea9ef-ef26-4de6-8475-b1e973f0a475", credential, "rv-persist", testTTL); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{battleAuthKey(matchID), battleKey(matchID)} {
		if ttl := f.mr.TTL(key); ttl != 0 {
			t.Fatalf("Stage/Mark restored preactive TTL: key=%s ttl=%v", key, ttl)
		}
	}
	if _, err := f.auth.ActivateHeartbeat(
		ctx, matchID, identityFor("pod-persist", credential), activateInput()); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{battleAuthKey(matchID), battleKey(matchID)} {
		if ttl := f.mr.TTL(key); ttl <= 0 {
			t.Fatalf("activation did not assign bounded TTL: key=%s ttl=%v", key, ttl)
		}
	}
}

func TestBattlePreactiveReleaseFencePersistsBeforeExternalRelease(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(697)
	seedModelBBattle(t, f, matchID, "68120480-b4ec-4075-8e85-b04d0d93ee5f", "pod-release", "uid-release")
	credential, _ := prepareAndStage(
		t, f, matchID, "68120480-b4ec-4075-8e85-b04d0d93ee5f", "pod-release", "uid-release", true)
	expected := BattleExpectedInstance{
		AllocationID: "68120480-b4ec-4075-8e85-b04d0d93ee5f", InstanceUID: "uid-release",
		InstanceEpoch: credential.GetInstanceEpoch(),
	}

	// 模拟未来版本附加字段，fence 的 RMW 不得丢弃。
	futureWire := []byte{0xf8, 0x07, 0x01}
	snapshot, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Auth.ProtoReflect().SetUnknown(append([]byte(nil), futureWire...))
	snapshot.Battle.ProtoReflect().SetUnknown(append([]byte(nil), futureWire...))
	aRaw, _ := proto.Marshal(snapshot.Auth)
	bRaw, _ := proto.Marshal(snapshot.Battle)
	if err := f.rdb.Set(ctx, battleAuthKey(matchID), aRaw, testTTL).Err(); err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, battleKey(matchID), bRaw, testTTL).Err(); err != nil {
		t.Fatal(err)
	}

	fenced, err := f.auth.FencePreactiveReleaseExpected(ctx, matchID, expected)
	if err != nil || !fenced {
		t.Fatalf("fence=%v err=%v", fenced, err)
	}
	fencedSnapshot, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil || fencedSnapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
		fencedSnapshot.Auth.GetActive() != nil || fencedSnapshot.Auth.GetPending() != nil ||
		fencedSnapshot.Battle.GetState() != BattleStatePreactiveReleasePending {
		t.Fatalf("preactive fence snapshot=%+v err=%v", fencedSnapshot, err)
	}
	if f.mr.TTL(battleAuthKey(matchID)) != 0 || f.mr.TTL(battleKey(matchID)) != 0 {
		t.Fatal("preactive release fence must persist both auth and battle")
	}
	if string(fencedSnapshot.Auth.ProtoReflect().GetUnknown()) != string(futureWire) ||
		string(fencedSnapshot.Battle.ProtoReflect().GetUnknown()) != string(futureWire) {
		t.Fatal("preactive release fence discarded future protobuf fields")
	}

	wrong := expected
	wrong.InstanceUID = "uid-rebuilt"
	aBefore := rawRedisBytes(t, f.rdb, battleAuthKey(matchID))
	bBefore := rawRedisBytes(t, f.rdb, battleKey(matchID))
	if purged, err := f.auth.PurgePreactiveReleasedExpected(ctx, matchID, wrong); err != nil || purged {
		t.Fatalf("wrong UID purge=%v err=%v", purged, err)
	}
	if !bytes.Equal(aBefore, rawRedisBytes(t, f.rdb, battleAuthKey(matchID))) ||
		!bytes.Equal(bBefore, rawRedisBytes(t, f.rdb, battleKey(matchID))) {
		t.Fatal("wrong UID purge mutated persistent fence")
	}
	if purged, err := f.auth.PurgePreactiveReleasedExpected(ctx, matchID, expected); err != nil || !purged {
		t.Fatalf("confirmed external release purge=%v err=%v", purged, err)
	}
	if f.mr.Exists(battleAuthKey(matchID)) || f.mr.Exists(battleKey(matchID)) {
		t.Fatal("confirmed preactive release did not purge authority keys")
	}
	if f.mr.TTL(battleAuthGenKey(matchID)) != 0 {
		t.Fatal("preactive purge removed/expired generation high-water")
	}
}

func TestBattlePreactiveReleaseFenceBeforeAuthExists(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(696)
	battle := sampleBattle(matchID, f.now.UnixMilli())
	battle.State = "warming"
	battle.AllocationId = "4e60de28-eaf6-4f1f-8c4e-0d5ae05e5cba"
	battle.DsPodName = "pod-no-auth"
	battle.GameserverUid = "uid-no-auth"
	battle.InstanceEpoch = 0
	if err := f.battle.CreateBattle(ctx, battle, testTTL); err != nil {
		t.Fatal(err)
	}
	expected := BattleExpectedInstance{AllocationID: "4e60de28-eaf6-4f1f-8c4e-0d5ae05e5cba", InstanceUID: "uid-no-auth"}
	if fenced, err := f.auth.FencePreactiveReleaseExpected(ctx, matchID, expected); err != nil || !fenced {
		t.Fatalf("fence without auth=%v err=%v", fenced, err)
	}
	snapshot, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil || snapshot.AuthFound || !snapshot.BattleFound ||
		snapshot.Battle.GetState() != BattleStatePreactiveReleasePending ||
		f.mr.TTL(battleKey(matchID)) != 0 {
		t.Fatalf("no-auth release fence snapshot=%+v err=%v", snapshot, err)
	}
	if purged, err := f.auth.PurgePreactiveReleasedExpected(ctx, matchID, expected); err != nil || !purged {
		t.Fatalf("no-auth confirmed purge=%v err=%v", purged, err)
	}
}

func rawRedisBytes(t *testing.T, rdb *redis.Client, key string) []byte {
	t.Helper()
	b, err := rdb.Get(context.Background(), key).Bytes()
	if err != nil {
		t.Fatalf("get raw %s: %v", key, err)
	}
	return b
}

func TestBattleAuthPrepareCredentialConcurrentMonotonicAndUIDEpoch(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const (
		matchID = uint64(701)
		n       = 24
	)
	seedModelBBattle(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "battle-pod", "uid-a")

	start := make(chan struct{})
	seeds := make(chan BattleCredentialSeed, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			seed, err := f.auth.PrepareCredential(ctx, authBinding(matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "battle-pod", "uid-a"))
			if err != nil {
				errs <- err
				return
			}
			seeds <- seed
		}()
	}
	close(start)
	wg.Wait()
	close(seeds)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent prepare: %v", err)
	}
	gens := make([]int, 0, n)
	for seed := range seeds {
		if seed.InstanceEpoch != 1 {
			t.Fatalf("same UID epoch=%d want 1", seed.InstanceEpoch)
		}
		gens = append(gens, int(seed.Gen))
	}
	sort.Ints(gens)
	for i, gen := range gens {
		if gen != i+1 {
			t.Fatalf("gens=%v, want 1..%d", gens, n)
		}
	}
	if ttl := f.mr.TTL(battleAuthGenKey(matchID)); ttl != 0 {
		t.Fatalf("generation counter must never expire, ttl=%v", ttl)
	}

	// GameServer 同名但新 allocation + 新 UID：instance_epoch 递增，gen 继续递增。
	rebuilt := sampleBattle(matchID, f.now.UnixMilli())
	rebuilt.State = "warming"
	rebuilt.DsPodName = "battle-pod"
	rebuilt.AllocationId = "6c9ac979-8290-4b88-8950-bd6ea7e0d5bb"
	rebuilt.GameserverUid = "uid-b"
	rebuilt.PodUid = "pod-uid-uid-b"
	rebuilt.AllocatedAtMs = f.now.UnixMilli()
	rebuilt.LastHeartbeatMs = f.now.UnixMilli()
	payload, err := marshalBattle(rebuilt)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, battleKey(matchID), payload, testTTL).Err(); err != nil {
		t.Fatal(err)
	}
	seed, err := f.auth.PrepareCredential(ctx, authBinding(matchID, "6c9ac979-8290-4b88-8950-bd6ea7e0d5bb", "battle-pod", "uid-b"))
	if err != nil {
		t.Fatalf("prepare rebuilt UID: %v", err)
	}
	if seed.InstanceEpoch != 2 || seed.Gen != n+1 {
		t.Fatalf("rebuilt seed=%+v want epoch=2 gen=%d", seed, n+1)
	}
}

func TestBattleAuthStageAndMarkDeliveredExpectedTupleCAS(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(702)
	seedModelBBattle(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a")
	seed, err := f.auth.PrepareCredential(ctx, authBinding(matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a"))
	if err != nil {
		t.Fatal(err)
	}
	cred := credentialFor(f, seed, "uid-a", "one")
	rec, err := f.auth.StagePending(ctx, BattleStageInput{MatchID: matchID, AllocationID: "d8a3fbe5-1831-4fe1-8521-779b4097aa51", Credential: cred, AuthTTL: testTTL})
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if rec.Pending == nil || rec.HighWaterGen != cred.Gen || rec.DeliveredRv != "" {
		t.Fatalf("stage record=%+v", rec)
	}
	// 响应丢失后完全相同的 Stage 是幂等成功。
	if _, err := f.auth.StagePending(ctx, BattleStageInput{MatchID: matchID, AllocationID: "d8a3fbe5-1831-4fe1-8521-779b4097aa51", Credential: cred, AuthTTL: testTTL}); err != nil {
		t.Fatalf("stage retry: %v", err)
	}

	wrong := proto.Clone(cred).(*dsv1.BattleDSCredential)
	wrong.ExpMs++ // exp 也是身份字段，不能只比较 gen/jti。
	before := rawRedisBytes(t, f.rdb, battleAuthKey(matchID))
	if err := f.auth.MarkDelivered(ctx, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", wrong, "rv-wrong", testTTL); errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("wrong expected tuple err=%v", err)
	}
	after := rawRedisBytes(t, f.rdb, battleAuthKey(matchID))
	if !bytes.Equal(before, after) {
		t.Fatal("wrong MarkDelivered mutated auth")
	}
	if err := f.auth.MarkDelivered(ctx, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", cred, "rv-good", testTTL); err != nil {
		t.Fatalf("mark delivered: %v", err)
	}
	snap, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil || snap.Auth.DeliveredRv != "rv-good" {
		t.Fatalf("snapshot=%+v err=%v", snap.Auth, err)
	}
}

func TestBattleAuthStageRejectsSupersededIssuedGeneration(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(710)
	seedModelBBattle(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a")
	seed1, err := f.auth.PrepareCredential(ctx, authBinding(matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a"))
	if err != nil {
		t.Fatal(err)
	}
	seed2, err := f.auth.PrepareCredential(ctx, authBinding(matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a"))
	if err != nil {
		t.Fatal(err)
	}
	oldCred := credentialFor(f, seed1, "uid-a", "old")
	before := rawRedisBytes(t, f.rdb, battleAuthKey(matchID))
	if _, err := f.auth.StagePending(ctx, BattleStageInput{
		MatchID: matchID, AllocationID: "d8a3fbe5-1831-4fe1-8521-779b4097aa51", Credential: oldCred, AuthTTL: testTTL,
	}); errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("superseded gen stage err=%v", err)
	}
	if !bytes.Equal(before, rawRedisBytes(t, f.rdb, battleAuthKey(matchID))) {
		t.Fatal("superseded gen mutated auth")
	}
	newCred := credentialFor(f, seed2, "uid-a", "new")
	if _, err := f.auth.StagePending(ctx, BattleStageInput{
		MatchID: matchID, AllocationID: "d8a3fbe5-1831-4fe1-8521-779b4097aa51", Credential: newCred, AuthTTL: testTTL,
	}); err != nil {
		t.Fatalf("latest gen stage: %v", err)
	}
	expected := BattleExpectedInstance{
		AllocationID: "d8a3fbe5-1831-4fe1-8521-779b4097aa51", InstanceUID: "uid-a", InstanceEpoch: seed2.InstanceEpoch,
	}
	if fenced, err := f.auth.FencePreactiveReleaseExpected(ctx, matchID, expected); err != nil || !fenced {
		t.Fatalf("bootstrap cleanup fenced=%v err=%v", fenced, err)
	}
	if purged, err := f.auth.PurgePreactiveReleasedExpected(ctx, matchID, expected); err != nil || !purged {
		t.Fatalf("bootstrap cleanup purged=%v err=%v", purged, err)
	}
	if f.mr.TTL(battleAuthGenKey(matchID)) != 0 {
		t.Fatal("bootstrap cleanup removed/expired generation counter")
	}
}

func TestBattleAuthActivateRequiresDeliveredAndStrictFullTupleZeroMutation(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(703)
	seedModelBBattle(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a")
	cred, id := prepareAndStage(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a", false)

	beforeAuth := rawRedisBytes(t, f.rdb, battleAuthKey(matchID))
	beforeBattle := rawRedisBytes(t, f.rdb, battleKey(matchID))
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput()); errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("undelivered activation err=%v", err)
	}
	if !bytes.Equal(beforeAuth, rawRedisBytes(t, f.rdb, battleAuthKey(matchID))) ||
		!bytes.Equal(beforeBattle, rawRedisBytes(t, f.rdb, battleKey(matchID))) {
		t.Fatal("undelivered activation mutated authority")
	}
	if err := f.auth.MarkDelivered(ctx, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", cred, "rv-good", testTTL); err != nil {
		t.Fatal(err)
	}

	baseAuth := rawRedisBytes(t, f.rdb, battleAuthKey(matchID))
	baseBattle := rawRedisBytes(t, f.rdb, battleKey(matchID))
	badIDs := []BattleCredentialIdentity{id, id, id, id, id, id}
	badIDs[0].InstanceUID = "uid-wrong"
	badIDs[1].JTI = "jti-wrong"
	badIDs[2].ExpMs++
	badIDs[3].Kid = "kid-wrong"
	badIDs[4].TokenSHA256 = "hash-wrong"
	badIDs[5].WriterEpoch = 3
	for i, bad := range badIDs {
		if _, err := f.auth.ActivateHeartbeat(ctx, matchID, bad, activateInput()); err == nil {
			t.Fatalf("bad identity %d unexpectedly accepted", i)
		}
		if !bytes.Equal(baseAuth, rawRedisBytes(t, f.rdb, battleAuthKey(matchID))) ||
			!bytes.Equal(baseBattle, rawRedisBytes(t, f.rdb, battleKey(matchID))) {
			t.Fatalf("bad identity %d mutated authority", i)
		}
	}

	res, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput())
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	if !res.FirstActivation || res.Terminal || res.Active != id || res.HeartbeatMs != f.now.UnixMilli() {
		t.Fatalf("activate result=%+v", res)
	}
	if res.Battle.State != "running" || res.Battle.GameserverUid != id.InstanceUID ||
		res.Battle.InstanceEpoch != id.InstanceEpoch || res.Battle.LastVerifiedGen != id.Gen ||
		res.Battle.LastVerifiedJti != id.JTI || res.Battle.LastVerifiedWriterEpoch != id.WriterEpoch {
		t.Fatalf("battle projection=%+v", res.Battle)
	}
	snap, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil {
		t.Fatal(err)
	}
	if ok, reason := snap.ReadyAuthorized(f.now.UnixMilli(), time.Minute.Milliseconds()); !ok {
		t.Fatalf("ready authority rejected: %s", reason)
	}
	if err := f.auth.CheckActive(ctx, matchID, id); err != nil {
		t.Fatalf("check active: %v", err)
	}
}

func TestBattleV2RejectsLowAndFutureWriterWithoutAnyAuthoritySideEffect(t *testing.T) {
	for _, tc := range []struct {
		name          string
		requiredEpoch uint32
		writerEpoch   uint32
	}{
		{name: "low-writer-1-required-1", requiredEpoch: 1, writerEpoch: 1},
		{name: "future-writer-3-required-2", requiredEpoch: BattleDSWriterEpochV2, writerEpoch: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newBattleAuthFixture(t)
			const matchID uint64 = 720
			const allocationID = "89d23603-ae10-4ee7-8313-a33d10c29514"
			const pod = "battle-720"
			seedModelBBattle(t, f, matchID, allocationID, pod, "uid-720")
			active, id := prepareAndStage(t, f, matchID, allocationID, pod, "uid-720", true)
			if _, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput()); err != nil {
				t.Fatalf("activate v2 fixture: %v", err)
			}
			snapshot, err := f.auth.ReadAuthority(ctx, matchID)
			if err != nil {
				t.Fatal(err)
			}
			authRecord := snapshot.Auth
			battleRecord := snapshot.Battle
			authRecord.RequiredWriterEpoch = tc.requiredEpoch
			authRecord.Active.WriterEpoch = tc.writerEpoch
			authRecord.Pending = proto.Clone(active).(*dsv1.BattleDSCredential)
			authRecord.Pending.Gen++
			authRecord.Pending.Jti = "jti-pending-non-v2"
			authRecord.Pending.TokenSha256 = "sha256-pending-non-v2"
			authRecord.Pending.WriterEpoch = tc.writerEpoch
			authRecord.HighWaterGen = authRecord.Pending.Gen
			authRecord.DeliveredRv = "rv-non-v2"
			authRecord.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING
			battleRecord.LastVerifiedWriterEpoch = tc.writerEpoch
			id.WriterEpoch = tc.writerEpoch
			authRaw, err := proto.Marshal(authRecord)
			if err != nil {
				t.Fatal(err)
			}
			battleRaw, err := marshalBattle(battleRecord)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.rdb.Set(ctx, battleAuthKey(matchID), authRaw, testTTL).Err(); err != nil {
				t.Fatal(err)
			}
			if err := f.rdb.Set(ctx, battleKey(matchID), battleRaw, testTTL).Err(); err != nil {
				t.Fatal(err)
			}
			queueKey := fmt.Sprintf("pandora:ds:gm:{%d}", matchID)
			if err := f.rdb.LPush(ctx, queueKey, "command-must-remain").Err(); err != nil {
				t.Fatal(err)
			}
			authTTLBefore, battleTTLBefore := f.mr.TTL(battleAuthKey(matchID)), f.mr.TTL(battleKey(matchID))

			candidate := proto.Clone(authRecord.Pending).(*dsv1.BattleDSCredential)
			candidate.Gen++
			candidate.Jti = "jti-stage-non-v2"
			candidate.TokenSha256 = "sha256-stage-non-v2"
			if _, err := f.auth.StagePending(ctx, BattleStageInput{
				MatchID: matchID, AllocationID: allocationID, Credential: candidate, AuthTTL: testTTL,
			}); err == nil {
				t.Fatal("V2 StagePending accepted non-v2 writer")
			}
			if err := f.auth.MarkDelivered(ctx, matchID, allocationID, authRecord.Pending, "rv-bad", testTTL); err == nil {
				t.Fatal("V2 MarkDelivered accepted non-v2 writer")
			}
			if _, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput()); err == nil {
				t.Fatal("V2 ActivateHeartbeat accepted non-v2 writer")
			}
			if err := f.auth.CheckActive(ctx, matchID, id); err == nil {
				t.Fatal("V2 CheckActive accepted non-v2 writer")
			}
			if commands, err := f.auth.PopCommandsIfActive(ctx, matchID, id, queueKey, 10); err == nil || len(commands) != 0 {
				t.Fatalf("V2 PopCommandsIfActive commands=%v err=%v", commands, err)
			}
			if result, err := f.auth.QuarantineExpected(ctx, matchID, BattleQuarantineExpected{
				AllocationID: allocationID, Credential: id,
			}, testTTL, testTTL); err == nil || result.AuthQuarantined || result.ProjectionAbandoned {
				t.Fatalf("V2 QuarantineExpected result=%+v err=%v", result, err)
			}
			if _, err := f.auth.AbandonIfStale(ctx, matchID, sameCutoffs(f.now.Add(time.Second).UnixMilli()), testTTL, testTTL); err == nil {
				t.Fatal("V2 AbandonIfStale accepted non-v2 authority")
			}
			terminated, err := f.auth.TerminateExpected(ctx, matchID, BattleExpectedInstance{
				AllocationID: allocationID, InstanceUID: "uid-720", InstanceEpoch: id.InstanceEpoch,
			}, "abandoned", testTTL, testTTL)
			if err != nil || terminated {
				t.Fatalf("V2 TerminateExpected terminated=%v err=%v", terminated, err)
			}
			latest, err := f.auth.ReadAuthority(ctx, matchID)
			if err != nil {
				t.Fatal(err)
			}
			if ok, _ := latest.ReadyAuthorized(f.now.UnixMilli(), time.Minute.Milliseconds()); ok {
				t.Fatal("V2 ReadyAuthorized accepted non-v2 authority")
			}

			if !bytes.Equal(authRaw, rawRedisBytes(t, f.rdb, battleAuthKey(matchID))) ||
				!bytes.Equal(battleRaw, rawRedisBytes(t, f.rdb, battleKey(matchID))) {
				t.Fatal("rejected non-v2 operation mutated battle authority")
			}
			if f.mr.TTL(battleAuthKey(matchID)) != authTTLBefore || f.mr.TTL(battleKey(matchID)) != battleTTLBefore {
				t.Fatal("rejected non-v2 operation refreshed authority TTL")
			}
			if got, err := f.rdb.LRange(ctx, queueKey, 0, -1).Result(); err != nil || len(got) != 1 || got[0] != "command-must-remain" {
				t.Fatalf("rejected non-v2 command pop changed queue: got=%v err=%v", got, err)
			}
		})
	}
}

func TestBattleAuthActivateConcurrentSinglePromotion(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const (
		matchID = uint64(704)
		n       = 12
	)
	seedModelBBattle(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a")
	_, id := prepareAndStage(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a", true)

	start := make(chan struct{})
	var first atomic.Int32
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput())
			if err != nil {
				errs <- err
				return
			}
			if res.FirstActivation {
				first.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent activate: %v", err)
	}
	if got := first.Load(); got != 1 {
		t.Fatalf("first activation winners=%d want 1", got)
	}
}

func TestBattleAuthPopCommandsChecksProjectionAndFullTupleBeforePop(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(705)
	seedModelBBattle(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a")
	_, id := prepareAndStage(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a", true)
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput()); err != nil {
		t.Fatal(err)
	}
	queueKey := fmt.Sprintf("pandora:ds:gm:{%d}:commands", matchID)
	if err := f.rdb.RPush(ctx, queueKey, "a", "b", "c").Err(); err != nil {
		t.Fatal(err)
	}
	bad := id
	bad.ExpMs++
	if _, err := f.auth.PopCommandsIfActive(ctx, matchID, bad, queueKey, 2); errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("wrong exp pop err=%v", err)
	}
	if n, _ := f.rdb.LLen(ctx, queueKey).Result(); n != 3 {
		t.Fatalf("wrong credential popped queue, len=%d", n)
	}

	// battle 投影被破坏时同样零弹出。
	b, _, _ := f.battle.GetBattle(ctx, matchID)
	b.LastVerifiedJti = "corrupt"
	payload, _ := marshalBattle(b)
	if err := f.rdb.Set(ctx, battleKey(matchID), payload, testTTL).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.auth.PopCommandsIfActive(ctx, matchID, id, queueKey, 2); errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("corrupt projection pop err=%v", err)
	}
	if n, _ := f.rdb.LLen(ctx, queueKey).Result(); n != 3 {
		t.Fatalf("corrupt projection popped queue, len=%d", n)
	}
	b.LastVerifiedJti = id.JTI
	payload, _ = marshalBattle(b)
	_ = f.rdb.Set(ctx, battleKey(matchID), payload, testTTL).Err()

	got, err := f.auth.PopCommandsIfActive(ctx, matchID, id, queueKey, 2)
	if err != nil {
		t.Fatalf("pop: %v", err)
	}
	if len(got) != 2 || got[0] != "c" || got[1] != "b" {
		t.Fatalf("pop=%v want [c b]", got)
	}
}

func TestBattleAuthPreactiveReleaseFenceCannotCaptureActivatedWinner(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(706)
	seedModelBBattle(t, f, matchID, "4221a0fc-3dfb-4d83-8dc3-a13f6c72d233", "pod-a", "uid-a")
	_, id := prepareAndStage(t, f, matchID, "4221a0fc-3dfb-4d83-8dc3-a13f6c72d233", "pod-a", "uid-a", true)
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput()); err != nil {
		t.Fatal(err)
	}
	if fenced, err := f.auth.FencePreactiveReleaseExpected(ctx, matchID, BattleExpectedInstance{
		AllocationID: "cc8a30ea-6fac-4d1b-8350-3e5a9001bbac", InstanceUID: id.InstanceUID, InstanceEpoch: id.InstanceEpoch,
	}); err != nil || fenced {
		t.Fatalf("loser cleanup fenced=%v err=%v", fenced, err)
	}
	// 即使旧 wait 使用了相同 allocation_id，ACTIVE/ready 赢家也不可被 cleanup API 捕获。
	expected := BattleExpectedInstance{
		AllocationID: "4221a0fc-3dfb-4d83-8dc3-a13f6c72d233", InstanceUID: id.InstanceUID, InstanceEpoch: id.InstanceEpoch,
	}
	if fenced, err := f.auth.FencePreactiveReleaseExpected(ctx, matchID, expected); err != nil || fenced {
		t.Fatalf("late cleanup fenced active winner=%v err=%v", fenced, err)
	}
	if _, found, _ := f.battle.GetBattle(ctx, matchID); !found {
		t.Fatal("active winner battle was deleted")
	}
	for _, wrong := range []BattleExpectedInstance{
		{AllocationID: "4221a0fc-3dfb-4d83-8dc3-a13f6c72d233", InstanceUID: "uid-rebuilt", InstanceEpoch: id.InstanceEpoch},
		{AllocationID: "4221a0fc-3dfb-4d83-8dc3-a13f6c72d233", InstanceUID: id.InstanceUID, InstanceEpoch: id.InstanceEpoch + 1},
	} {
		beforeAuth := rawRedisBytes(t, f.rdb, battleAuthKey(matchID))
		beforeBattle := rawRedisBytes(t, f.rdb, battleKey(matchID))
		if ok, err := f.auth.TerminateExpected(ctx, matchID, wrong, "ended", testTTL, testTTL); err != nil || ok {
			t.Fatalf("wrong UID/epoch terminate ok=%v err=%v expected=%+v", ok, err, wrong)
		}
		if !bytes.Equal(beforeAuth, rawRedisBytes(t, f.rdb, battleAuthKey(matchID))) ||
			!bytes.Equal(beforeBattle, rawRedisBytes(t, f.rdb, battleKey(matchID))) {
			t.Fatalf("wrong UID/epoch terminate mutated authority: %+v", wrong)
		}
	}
	if ok, err := f.auth.TerminateExpected(ctx, matchID, expected, "ended", testTTL, testTTL); errcode.As(err) != errcode.ErrInvalidState || ok {
		t.Fatalf("completed terminate without result receipt ok=%v err=%v", ok, err)
	}
	receipt := dsauthrecord.NewBattleResultReceipt(
		matchID, expected.AllocationID, id.PodName, id.InstanceUID, id.InstanceEpoch,
		id.Gen, id.JTI, int64(id.ExpMs), id.Kid, id.TokenSHA256, id.WriterEpoch, f.now.UnixMilli())
	receiptRaw, err := dsauthrecord.MarshalBattleResultReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, dsauthrecord.BattleResultReceiptKey(matchID), receiptRaw, testTTL).Err(); err != nil {
		t.Fatal(err)
	}
	if ok, err := f.auth.TerminateExpected(ctx, matchID, expected, "ended", testTTL, testTTL); err != nil || !ok {
		t.Fatalf("terminate ok=%v err=%v", ok, err)
	}
	if authTTL, battleTTL := f.mr.TTL(battleAuthKey(matchID)), f.mr.TTL(battleKey(matchID)); authTTL != 0 || battleTTL != 0 {
		t.Fatalf("TerminateExpected must persist release fence: auth=%v battle=%v", authTTL, battleTTL)
	}
	wrongPurge := expected
	wrongPurge.InstanceEpoch++
	if purged, err := f.auth.PurgeTerminatedExpected(ctx, matchID, wrongPurge); err != nil || purged {
		t.Fatalf("wrong epoch purge=%v err=%v", purged, err)
	}
	if purged, err := f.auth.PurgeTerminatedExpected(ctx, matchID, expected); err != nil || !purged {
		t.Fatalf("purge=%v err=%v", purged, err)
	}
	if f.mr.TTL(battleAuthGenKey(matchID)) != 0 {
		t.Fatal("purge must preserve non-expiring generation counter")
	}
}

func TestBattleAuthActivationRacesPreactiveReleaseFenceSingleWinner(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	for i := uint64(0); i < 20; i++ {
		matchID := uint64(730) + i
		allocationID := uuid.NewString()
		pod := fmt.Sprintf("pod-race-release-%d", i)
		uid := fmt.Sprintf("uid-race-release-%d", i)
		seedModelBBattle(t, f, matchID, allocationID, pod, uid)
		credential, id := prepareAndStage(t, f, matchID, allocationID, pod, uid, true)
		expected := BattleExpectedInstance{
			AllocationID: allocationID, InstanceUID: uid, InstanceEpoch: credential.GetInstanceEpoch(),
		}

		start := make(chan struct{})
		activateDone := make(chan error, 1)
		fenceDone := make(chan struct {
			ok  bool
			err error
		}, 1)
		go func() {
			<-start
			_, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput())
			activateDone <- err
		}()
		go func() {
			<-start
			ok, err := f.auth.FencePreactiveReleaseExpected(ctx, matchID, expected)
			fenceDone <- struct {
				ok  bool
				err error
			}{ok: ok, err: err}
		}()
		close(start)
		activateErr := <-activateDone
		fenceResult := <-fenceDone
		if fenceResult.err != nil {
			t.Fatalf("round %d fence err=%v", i, fenceResult.err)
		}
		snapshot, err := f.auth.ReadAuthority(ctx, matchID)
		if err != nil {
			t.Fatalf("round %d snapshot: %v", i, err)
		}
		switch {
		case activateErr == nil:
			if fenceResult.ok || snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE ||
				snapshot.Battle.GetState() != "running" || f.mr.TTL(battleAuthKey(matchID)) <= 0 ||
				f.mr.TTL(battleKey(matchID)) <= 0 {
				t.Fatalf("round %d activation winner inconsistent: fence=%v snapshot=%+v", i, fenceResult.ok, snapshot)
			}
		case fenceResult.ok:
			if errcode.As(activateErr) != errcode.ErrUnauthorized ||
				snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
				snapshot.Auth.GetActive() != nil ||
				snapshot.Battle.GetState() != BattleStatePreactiveReleasePending ||
				f.mr.TTL(battleAuthKey(matchID)) != 0 || f.mr.TTL(battleKey(matchID)) != 0 {
				t.Fatalf("round %d release-fence winner inconsistent: activate=%v snapshot=%+v", i, activateErr, snapshot)
			}
		default:
			t.Fatalf("round %d neither activation nor release fence won: activate=%v fence=%v", i, activateErr, fenceResult.ok)
		}
	}
}

func TestBattleAuthStaleZSetCannotAbandonFreshAuthority(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(707)
	seedModelBBattle(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a")
	_, id := prepareAndStage(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a", true)
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput()); err != nil {
		t.Fatal(err)
	}
	firstHB := f.now.UnixMilli()
	// 模拟 auth+battle 已收到新心跳，但跨 slot ZSET 仍是旧 score。
	if err := f.rdb.ZAdd(ctx, activeKey, redis.Z{Score: float64(firstHB - 60_000), Member: matchID}).Err(); err != nil {
		t.Fatal(err)
	}
	f.setNow(f.now.Add(10 * time.Second))
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput()); err != nil {
		t.Fatal(err)
	}
	result, err := f.auth.AbandonIfStale(ctx, matchID, sameCutoffs(firstHB+5_000), testTTL, testTTL)
	if err != nil {
		t.Fatalf("abandon fresh: %v", err)
	}
	if result.Abandoned || result.AlreadyTerminal {
		t.Fatalf("fresh authority falsely abandoned: %+v", result)
	}
	if fresh, err := f.auth.CheckHeartbeatFresh(ctx, matchID, firstHB+5_000); err != nil || !fresh {
		t.Fatalf("fresh=%v err=%v", fresh, err)
	}

	f.mr.FastForward(30 * time.Minute)
	result, err = f.auth.AbandonIfStale(ctx, matchID, sameCutoffs(f.now.UnixMilli()), testTTL, testTTL)
	if err != nil || !result.Abandoned || result.Battle.State != "abandoned" {
		t.Fatalf("stale abandon=%+v err=%v", result, err)
	}
	if got := f.mr.TTL(battleAuthKey(matchID)); got != 0 {
		t.Fatalf("abandon release fence must persist auth, ttl=%v", got)
	}
	if got := f.mr.TTL(battleKey(matchID)); got != 0 {
		t.Fatalf("abandon release fence must persist battle, ttl=%v", got)
	}
	if _, err := f.rdb.ZScore(ctx, activeKey, fmt.Sprint(matchID)).Result(); err != nil {
		t.Fatalf("abandoned lifecycle outbox member removed: %v", err)
	}
	if err := f.auth.CheckActive(ctx, matchID, id); errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("terminated active still accepted: %v", err)
	}
	expected := BattleExpectedInstance{
		AllocationID: "d8a3fbe5-1831-4fe1-8521-779b4097aa51", InstanceUID: id.InstanceUID, InstanceEpoch: id.InstanceEpoch,
	}
	if expired, err := f.auth.ExpireTerminatedExpected(
		ctx, matchID, expected, testTTL, testTTL); err != nil || !expired {
		t.Fatalf("post-release terminal expiry=%v err=%v", expired, err)
	}
	if authTTL, battleTTL := f.mr.TTL(battleAuthKey(matchID)), f.mr.TTL(battleKey(matchID)); authTTL <= 0 || battleTTL <= 0 {
		t.Fatalf("post-release lifecycle did not restore bounded TTL: auth=%v battle=%v", authTTL, battleTTL)
	}
}

func TestBattleAuthAbandonMissingAuthWarmingHalfFailure(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(712)
	seedModelBBattle(t, f, matchID, "b952759c-e53f-4f08-8331-44dcacabb086", "pod-half", "uid-half")
	result, err := f.auth.AbandonIfStale(ctx, matchID, sameCutoffs(f.now.UnixMilli()), testTTL, testTTL)
	if err != nil || !result.Abandoned || result.Battle == nil || result.Battle.State != "abandoned" {
		t.Fatalf("missing-auth warming abandon=%+v err=%v", result, err)
	}
	if got := f.mr.TTL(battleKey(matchID)); got != 0 {
		t.Fatalf("missing-auth abandon release fence must persist, ttl=%v", got)
	}
	if _, err := f.rdb.ZScore(ctx, activeKey, fmt.Sprint(matchID)).Result(); err != nil {
		t.Fatalf("missing-auth abandoned outbox removed: %v", err)
	}
}

// coldLoadCutoffs 以分配时刻为基准,模拟 sweep 在 age 之后按生产口径
// heartbeat_timeout=15s / ready_wait_timeout=120s 计算的双阈值。
func coldLoadCutoffs(allocMs int64, age time.Duration) BattleStaleCutoffs {
	sweepNowMs := allocMs + age.Milliseconds()
	return BattleStaleCutoffs{
		ActiveHeartbeatMs:  sweepNowMs - (15 * time.Second).Milliseconds(),
		WarmingHeartbeatMs: sweepNowMs - (120 * time.Second).Milliseconds(),
	}
}

// 冷加载回归(Artic01 事故):缺 auth 的 warming 分配,年龄超过业务心跳阈值(15s)
// 但未超过 ready 等待阈值(120s)时不得判弃且零副作用;超过 120s 后必须回收。
func TestBattleAuthAbandonWarmingColdLoadGraceMissingAuth(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(713)
	seedModelBBattle(t, f, matchID, "0a4c9d9e-9f10-4b7e-8f7f-52d6f3f6f001", "pod-cold", "uid-cold")
	allocMs := f.now.UnixMilli()

	battleBefore := rawRedisBytes(t, f.rdb, battleKey(matchID))
	ttlBefore := f.mr.TTL(battleKey(matchID))
	result, err := f.auth.AbandonIfStale(ctx, matchID, coldLoadCutoffs(allocMs, 30*time.Second), testTTL, testTTL)
	if err != nil || result.Abandoned || result.AlreadyTerminal {
		t.Fatalf("cold-load warming falsely abandoned at 30s: %+v err=%v", result, err)
	}
	if result.Battle == nil || result.Battle.State != "warming" {
		t.Fatalf("cold-load warming snapshot=%+v", result.Battle)
	}
	if !bytes.Equal(battleBefore, rawRedisBytes(t, f.rdb, battleKey(matchID))) ||
		f.mr.TTL(battleKey(matchID)) != ttlBefore {
		t.Fatal("grace-window sweep mutated warming battle bytes or TTL")
	}

	result, err = f.auth.AbandonIfStale(ctx, matchID, coldLoadCutoffs(allocMs, 130*time.Second), testTTL, testTTL)
	if err != nil || !result.Abandoned || result.Battle == nil || result.Battle.State != "abandoned" {
		t.Fatalf("cold-load warming not reclaimed after ready-wait cutoff: %+v err=%v", result, err)
	}
	if got := f.mr.TTL(battleKey(matchID)); got != 0 {
		t.Fatalf("cold-load abandon release fence must persist, ttl=%v", got)
	}
	if _, err := f.rdb.ZScore(ctx, activeKey, fmt.Sprint(matchID)).Result(); err != nil {
		t.Fatalf("cold-load abandoned outbox removed: %v", err)
	}
}

// BOOTSTRAP(已 Prepare/Stage/Deliver、尚未首次心跳)+ warming 同样享受冷加载宽限;
// 超过 ready 等待阈值后照常回收。
func TestBattleAuthAbandonWarmingColdLoadGraceBootstrap(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(714)
	const allocationID = "0a4c9d9e-9f10-4b7e-8f7f-52d6f3f6f002"
	seedModelBBattle(t, f, matchID, allocationID, "pod-cold-bs", "uid-cold-bs")
	allocMs := f.now.UnixMilli()
	prepareAndStage(t, f, matchID, allocationID, "pod-cold-bs", "uid-cold-bs", true)

	authBefore := rawRedisBytes(t, f.rdb, battleAuthKey(matchID))
	battleBefore := rawRedisBytes(t, f.rdb, battleKey(matchID))
	result, err := f.auth.AbandonIfStale(ctx, matchID, coldLoadCutoffs(allocMs, 30*time.Second), testTTL, testTTL)
	if err != nil || result.Abandoned || result.AlreadyTerminal {
		t.Fatalf("bootstrap warming falsely abandoned at 30s: %+v err=%v", result, err)
	}
	if !bytes.Equal(authBefore, rawRedisBytes(t, f.rdb, battleAuthKey(matchID))) ||
		!bytes.Equal(battleBefore, rawRedisBytes(t, f.rdb, battleKey(matchID))) {
		t.Fatal("grace-window sweep mutated bootstrap authority")
	}

	result, err = f.auth.AbandonIfStale(ctx, matchID, coldLoadCutoffs(allocMs, 130*time.Second), testTTL, testTTL)
	if err != nil || !result.Abandoned || result.Battle == nil || result.Battle.State != "abandoned" {
		t.Fatalf("bootstrap warming not reclaimed after ready-wait cutoff: %+v err=%v", result, err)
	}
	if authTTL, battleTTL := f.mr.TTL(battleAuthKey(matchID)), f.mr.TTL(battleKey(matchID)); authTTL != 0 || battleTTL != 0 {
		t.Fatalf("bootstrap abandon release fence must persist: auth=%v battle=%v", authTTL, battleTTL)
	}
}

// ABA 防护:WarmingForfeit(编排层判死)绑定被探测实例的 exact 身份。事务内在场的
// 已是同 match_id 的新分配 B 时,旧判死必须作废、B 按常规冷加载宽限存活;身份与
// 在场记录精确一致时才允许放弃宽限立即判弃。
func TestBattleAuthAbandonWarmingForfeitBoundToExactInstance(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(716)
	const allocA = "0a4c9d9e-9f10-4b7e-8f7f-52d6f3f6f004"
	const allocB = "0a4c9d9e-9f10-4b7e-8f7f-52d6f3f6f005"
	seedModelBBattle(t, f, matchID, allocA, "pod-aba-a", "uid-aba-a")
	prepareAndStage(t, f, matchID, allocA, "pod-aba-a", "uid-aba-a", true)
	snapshotA, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil {
		t.Fatal(err)
	}
	forfeitA := &BattleWarmingForfeit{
		Instance: BattleExpectedInstance{
			AllocationID:  allocA,
			InstanceUID:   "uid-aba-a",
			InstanceEpoch: snapshotA.Battle.GetInstanceEpoch(),
		},
		HeartbeatMs: f.now.UnixMilli(),
	}

	// probe 在途窗口内:A 被外部清理,同 match 新分配 B 就位。
	f.mr.Del(battleKey(matchID))
	f.mr.Del(battleAuthKey(matchID))
	seedModelBBattle(t, f, matchID, allocB, "pod-aba-b", "uid-aba-b")
	prepareAndStage(t, f, matchID, allocB, "pod-aba-b", "uid-aba-b", true)

	cutoffs := coldLoadCutoffs(f.now.UnixMilli(), 30*time.Second) // B 仍在冷加载宽限内
	cutoffs.WarmingForfeit = forfeitA
	result, err := f.auth.AbandonIfStale(ctx, matchID, cutoffs, testTTL, testTTL)
	if err != nil || result.Abandoned || result.AlreadyTerminal {
		t.Fatalf("stale forfeit for A killed replacement B: %+v err=%v", result, err)
	}
	if result.Battle == nil || result.Battle.GetAllocationId() != allocB ||
		result.Battle.GetState() != "warming" {
		t.Fatalf("replacement B snapshot=%+v", result.Battle)
	}

	// 对照:forfeit 身份与在场 B 精确一致 → 允许放弃宽限,立即判弃。
	snapshotB, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil {
		t.Fatal(err)
	}
	cutoffs.WarmingForfeit = &BattleWarmingForfeit{
		Instance: BattleExpectedInstance{
			AllocationID:  allocB,
			InstanceUID:   "uid-aba-b",
			InstanceEpoch: snapshotB.Battle.GetInstanceEpoch(),
		},
		HeartbeatMs: f.now.UnixMilli(),
	}
	result, err = f.auth.AbandonIfStale(ctx, matchID, cutoffs, testTTL, testTTL)
	if err != nil || !result.Abandoned || result.Battle == nil || result.Battle.GetState() != "abandoned" {
		t.Fatalf("matching forfeit did not reclaim: %+v err=%v", result, err)
	}
}

// 两阶段激活稳定性门(INC-20260727-001 第三 P0):证据不足的 staged 心跳零状态转移
// (不提升、无 ACK、battle 保持 warming、LastHeartbeatMs 不动);≥3 拍且跨度 ≥10s 才
// 原子提升并清证据键;首拍后线程阻塞(无后续拍)→ 120s 分配宽限照常判弃。
func TestBattleAuthActivationStabilityGate(t *testing.T) {
	ctx := context.Background()
	t.Run("promotes only after sustained beats", func(t *testing.T) {
		f := newBattleAuthFixture(t)
		const matchID = uint64(718)
		const allocationID = "0a4c9d9e-9f10-4b7e-8f7f-52d6f3f6f007"
		seedModelBBattle(t, f, matchID, allocationID, "pod-stab", "uid-stab")
		allocHeartbeatMs := f.now.UnixMilli()
		_, id := prepareAndStage(t, f, matchID, allocationID, "pod-stab", "uid-stab", true)
		in := activateInput()
		in.StabilityBeats = 3
		in.StabilitySpanMs = 10_000

		assertPending := func(step string) {
			res, err := f.auth.ActivateHeartbeat(ctx, matchID, id, in)
			if err != nil || !res.ActivationPending || res.FirstActivation {
				t.Fatalf("%s: res=%+v err=%v want pending", step, res, err)
			}
			if res.Active != (BattleCredentialIdentity{}) {
				t.Fatalf("%s: pending response leaked ACK identity: %+v", step, res.Active)
			}
			snap, serr := f.auth.ReadAuthority(ctx, matchID)
			if serr != nil || snap.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_BOOTSTRAP ||
				snap.Battle.GetState() != "warming" || snap.Battle.GetLastHeartbeatMs() != allocHeartbeatMs {
				t.Fatalf("%s: pending beat caused state transition: phase=%v state=%s hb=%d err=%v",
					step, snap.Auth.GetPhase(), snap.Battle.GetState(), snap.Battle.GetLastHeartbeatMs(), serr)
			}
		}
		assertPending("beat1")                    // T0:count=1
		f.setNow(f.now.Add(6 * time.Second))
		assertPending("beat2 +6s")                // count=2
		f.setNow(f.now.Add(3 * time.Second))
		assertPending("beat3 +9s span<10s")       // count=3 但跨度 9s
		f.setNow(f.now.Add(2 * time.Second))
		res, err := f.auth.ActivateHeartbeat(ctx, matchID, id, in) // +11s:count=4 跨度 11s
		if err != nil || res.ActivationPending || !res.FirstActivation ||
			res.Battle.GetState() != "running" {
			t.Fatalf("stabilized beat did not promote: res=%+v err=%v", res, err)
		}
		if f.mr.Exists(battleActivationEvidenceKey(matchID)) {
			t.Fatal("promotion did not clear stability evidence key")
		}
	})
	t.Run("rotation of proven active instance bypasses gate", func(t *testing.T) {
		f := newBattleAuthFixture(t)
		const matchID = uint64(721)
		const allocationID = "0a4c9d9e-9f10-4b7e-8f7f-52d6f3f6f009"
		seedModelBBattle(t, f, matchID, allocationID, "pod-rot", "uid-rot")
		_, firstID := prepareAndStage(t, f, matchID, allocationID, "pod-rot", "uid-rot", true)
		// 首次激活(门关,等价已通过稳定性证据的存量实例)。
		if _, err := f.auth.ActivateHeartbeat(ctx, matchID, firstID, activateInput()); err != nil {
			t.Fatalf("first activation: %v", err)
		}
		// 凭据轮换:新 staged 凭据在门开启下单拍即须完成提升——已证明实例不再付稳定性证据。
		_, rotatedID := prepareAndStage(t, f, matchID, allocationID, "pod-rot", "uid-rot", true)
		in := activateInput()
		in.StabilityBeats = 3
		in.StabilitySpanMs = 10_000
		res, err := f.auth.ActivateHeartbeat(ctx, matchID, rotatedID, in)
		if err != nil || res.ActivationPending || res.Active.JTI != rotatedID.JTI {
			t.Fatalf("rotation must bypass stability gate: res=%+v err=%v", res, err)
		}
	})
	t.Run("first beat then blocked thread reclaimed by warming grace", func(t *testing.T) {
		f := newBattleAuthFixture(t)
		const matchID = uint64(719)
		const allocationID = "0a4c9d9e-9f10-4b7e-8f7f-52d6f3f6f008"
		seedModelBBattle(t, f, matchID, allocationID, "pod-stab2", "uid-stab2")
		allocMs := f.now.UnixMilli()
		_, id := prepareAndStage(t, f, matchID, allocationID, "pod-stab2", "uid-stab2", true)
		in := activateInput()
		in.StabilityBeats = 3
		in.StabilitySpanMs = 10_000
		if res, err := f.auth.ActivateHeartbeat(ctx, matchID, id, in); err != nil || !res.ActivationPending {
			t.Fatalf("beat1: res=%+v err=%v", res, err)
		}
		// 20~30s 阻塞窗口内:warming 宽限保护,不得判弃。
		result, err := f.auth.AbandonIfStale(ctx, matchID, coldLoadCutoffs(allocMs, 30*time.Second), testTTL, testTTL)
		if err != nil || result.Abandoned {
			t.Fatalf("blocked-at-30s falsely reclaimed: %+v err=%v", result, err)
		}
		// 永久阻塞:120s 分配宽限到期照常判弃(pending 心跳不续命)。
		result, err = f.auth.AbandonIfStale(ctx, matchID, coldLoadCutoffs(allocMs, 130*time.Second), testTTL, testTTL)
		if err != nil || !result.Abandoned {
			t.Fatalf("blocked-forever not reclaimed at 130s: %+v err=%v", result, err)
		}
	})
}

// WRONGTYPE fail-closed(复审 P1-1):auth/battle 任一键被写成错误类型时,原子读必须
// 把 WRONGTYPE 当错误返回(MGET 会静默给 nil,把损坏伪装成"合法缺失"),AbandonIfStale
// 零副作用——不写 abandoned、不改另一键、不触发释放、outbox/TTL 原样。
func TestBattleAuthAbandonFailsClosedOnWrongTypeKey(t *testing.T) {
	ctx := context.Background()
	for _, corrupt := range []string{"auth", "battle"} {
		t.Run(corrupt, func(t *testing.T) {
			f := newBattleAuthFixture(t)
			const matchID = uint64(717)
			const allocationID = "0a4c9d9e-9f10-4b7e-8f7f-52d6f3f6f006"
			seedModelBBattle(t, f, matchID, allocationID, "pod-wt", "uid-wt")
			prepareAndStage(t, f, matchID, allocationID, "pod-wt", "uid-wt", true)

			corruptKey := battleAuthKey(matchID)
			intactKey := battleKey(matchID)
			if corrupt == "battle" {
				corruptKey, intactKey = battleKey(matchID), battleAuthKey(matchID)
			}
			// 把目标键改写成 LIST(WRONGTYPE 场景)。
			f.mr.Del(corruptKey)
			if _, err := f.mr.Lpush(corruptKey, "corrupted"); err != nil {
				t.Fatal(err)
			}
			intactBefore := rawRedisBytes(t, f.rdb, intactKey)
			scoreBefore, _ := f.rdb.ZScore(ctx, activeKey, fmt.Sprint(matchID)).Result()

			result, err := f.auth.AbandonIfStale(ctx, matchID,
				coldLoadCutoffs(f.now.UnixMilli(), 130*time.Second), testTTL, testTTL)
			if err == nil {
				t.Fatalf("wrong-type %s key must fail closed, result=%+v", corrupt, result)
			}
			if result.Abandoned || result.AlreadyTerminal {
				t.Fatalf("wrong-type %s key produced abandon side effects: %+v", corrupt, result)
			}
			if !bytes.Equal(intactBefore, rawRedisBytes(t, f.rdb, intactKey)) {
				t.Fatalf("wrong-type %s key mutated the intact peer key", corrupt)
			}
			if scoreAfter, zerr := f.rdb.ZScore(ctx, activeKey, fmt.Sprint(matchID)).Result(); zerr != nil || scoreAfter != scoreBefore {
				t.Fatalf("wrong-type %s key touched active outbox: before=%v after=%v err=%v",
					corrupt, scoreBefore, scoreAfter, zerr)
			}
		})
	}
}

// ACTIVE(已激活,含 ready/running)只认业务心跳阈值:失联超 15s 必须回收,
// 即使仍未超过 warming 冷加载阈值 —— 宽限绝不能延后 running DS 的崩溃补偿(不变量 §4)。
func TestBattleAuthAbandonActiveIgnoresWarmingGrace(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(715)
	seedModelBBattle(t, f, matchID, "0a4c9d9e-9f10-4b7e-8f7f-52d6f3f6f003", "pod-cold-act", "uid-cold-act")
	_, id := prepareAndStage(t, f, matchID, "0a4c9d9e-9f10-4b7e-8f7f-52d6f3f6f003", "pod-cold-act", "uid-cold-act", true)
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput()); err != nil {
		t.Fatal(err)
	}
	lastHB := f.now.UnixMilli()

	result, err := f.auth.AbandonIfStale(ctx, matchID, coldLoadCutoffs(lastHB, 30*time.Second), testTTL, testTTL)
	if err != nil || !result.Abandoned || result.Battle == nil || result.Battle.State != "abandoned" {
		t.Fatalf("active battle not reclaimed by heartbeat cutoff at 30s: %+v err=%v", result, err)
	}
	if authTTL, battleTTL := f.mr.TTL(battleAuthKey(matchID)), f.mr.TTL(battleKey(matchID)); authTTL != 0 || battleTTL != 0 {
		t.Fatalf("active abandon release fence must persist: auth=%v battle=%v", authTTL, battleTTL)
	}
}

// 首次 ActivateHeartbeat 与冷加载超时判弃并发:WATCH 事务保证恰好一方生效。
// 激活先赢 → running 且不判弃;判弃先赢 → 激活 Unauthorized;绝不允许
// "激活成功同时又 abandoned"。
func TestBattleAuthWarmingActivationRacesColdLoadSweepSingleWinner(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	for i := uint64(0); i < 20; i++ {
		matchID := uint64(760) + i
		allocationID := uuid.NewString()
		pod := fmt.Sprintf("pod-race-cold-%d", i)
		uid := fmt.Sprintf("uid-race-cold-%d", i)
		seedModelBBattle(t, f, matchID, allocationID, pod, uid)
		allocMs := f.now.UnixMilli()
		_, id := prepareAndStage(t, f, matchID, allocationID, pod, uid, true)
		// 冷加载已超 ready 等待阈值(sweep 有资格判弃);若 DS 首次心跳先赢,
		// LastActiveHeartbeatMs=当前时刻,按 ACTIVE 阈值必为新鲜,不得再判弃。
		f.setNow(f.now.Add(130 * time.Second))
		cutoffs := coldLoadCutoffs(allocMs, 130*time.Second)

		start := make(chan struct{})
		activateDone := make(chan error, 1)
		abandonDone := make(chan struct {
			result BattleAbandonResult
			err    error
		}, 1)
		go func() {
			<-start
			_, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput())
			activateDone <- err
		}()
		go func() {
			<-start
			result, err := f.auth.AbandonIfStale(ctx, matchID, cutoffs, testTTL, testTTL)
			abandonDone <- struct {
				result BattleAbandonResult
				err    error
			}{result: result, err: err}
		}()
		close(start)
		activateErr := <-activateDone
		abandon := <-abandonDone
		if abandon.err != nil {
			t.Fatalf("round %d abandon err=%v", i, abandon.err)
		}
		activated := activateErr == nil
		if activated == abandon.result.Abandoned {
			t.Fatalf("round %d must have exactly one winner: activated=%v abandoned=%v activateErr=%v",
				i, activated, abandon.result.Abandoned, activateErr)
		}
		snapshot, err := f.auth.ReadAuthority(ctx, matchID)
		if err != nil {
			t.Fatalf("round %d snapshot: %v", i, err)
		}
		if activated {
			if snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE ||
				snapshot.Battle.GetState() != "running" {
				t.Fatalf("round %d activation winner inconsistent: %+v", i, snapshot)
			}
		} else {
			if errcode.As(activateErr) != errcode.ErrUnauthorized {
				t.Fatalf("round %d late activation must be unauthorized, err=%v", i, activateErr)
			}
			if snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
				snapshot.Battle.GetState() != "abandoned" {
				t.Fatalf("round %d abandon winner inconsistent: %+v", i, snapshot)
			}
		}
	}
}

func TestBattleAuthHeartbeatTerminalLocksAuthInSameTransaction(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(708)
	seedModelBBattle(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a")
	_, id := prepareAndStage(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a", true)
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput()); err != nil {
		t.Fatal(err)
	}
	f.setNow(f.now.Add(time.Second))
	in := activateInput()
	in.State = "ended"
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, id, in); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("ended without result receipt must fail closed: %v", err)
	}
	preReceipt, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil || preReceipt.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE ||
		preReceipt.Battle.GetState() == "ended" {
		t.Fatalf("rejected ended changed authority: %+v err=%v", preReceipt, err)
	}
	receipt := dsauthrecord.NewBattleResultReceipt(
		matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", id.PodName, id.InstanceUID, id.InstanceEpoch, id.Gen, id.JTI,
		int64(id.ExpMs), id.Kid, id.TokenSHA256, id.WriterEpoch, f.now.UnixMilli())
	receiptRaw, err := dsauthrecord.MarshalBattleResultReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, dsauthrecord.BattleResultReceiptKey(matchID), receiptRaw, testTTL).Err(); err != nil {
		t.Fatal(err)
	}
	res, err := f.auth.ActivateHeartbeat(ctx, matchID, id, in)
	if err != nil {
		t.Fatalf("ended heartbeat: %v", err)
	}
	if !res.Terminal || res.FirstAbandon || res.Battle.State != "ended" {
		t.Fatalf("ended result=%+v", res)
	}
	snap, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil || snap.Auth.Phase != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING {
		t.Fatalf("terminal auth=%+v err=%v", snap.Auth, err)
	}
	if err := f.auth.CheckActive(ctx, matchID, id); errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("terminal CheckActive=%v", err)
	}
	if _, err := f.rdb.ZScore(ctx, activeKey, fmt.Sprint(matchID)).Result(); err != redis.Nil {
		t.Fatalf("ended battle active index was not removed, err=%v", err)
	}
	// 重复终态心跳只返回 stop/ACK，不刷新权威心跳，也不产生 FirstAbandon。
	lastHB := snap.Auth.LastActiveHeartbeatMs
	f.setNow(f.now.Add(time.Minute))
	res, err = f.auth.ActivateHeartbeat(ctx, matchID, id, in)
	if err != nil || !res.Terminal || res.FirstAbandon || res.HeartbeatMs != lastHB {
		t.Fatalf("terminal retry=%+v err=%v", res, err)
	}
}

func TestBattleAuthReceiptFencesCredentialRotation(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(713)
	seedModelBBattle(t, f, matchID, "90fd9260-9005-47e4-8365-575e50297517", "pod-r", "uid-r")
	_, first := prepareAndStage(t, f, matchID, "90fd9260-9005-47e4-8365-575e50297517", "pod-r", "uid-r", true)
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, first, activateInput()); err != nil {
		t.Fatal(err)
	}
	receipt := dsauthrecord.NewBattleResultReceipt(
		matchID, "90fd9260-9005-47e4-8365-575e50297517", first.PodName, first.InstanceUID, first.InstanceEpoch,
		first.Gen, first.JTI, int64(first.ExpMs), first.Kid, first.TokenSHA256,
		first.WriterEpoch, f.now.UnixMilli())
	receiptRaw, err := dsauthrecord.MarshalBattleResultReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, dsauthrecord.BattleResultReceiptKey(matchID), receiptRaw, testTTL).Err(); err != nil {
		t.Fatal(err)
	}

	if _, err := f.auth.PrepareCredential(ctx, authBinding(matchID, "90fd9260-9005-47e4-8365-575e50297517", "pod-r", "uid-r")); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("receipt did not fence credential preparation: %v", err)
	}
	ended := activateInput()
	ended.State = "ended"
	result, err := f.auth.ActivateHeartbeat(ctx, matchID, first, ended)
	if err != nil || !result.Terminal || result.Battle.GetState() != "ended" {
		t.Fatalf("receipt credential could not finish battle: result=%+v err=%v", result, err)
	}
}

func TestBattleAuthReceiptWinsRaceAgainstPendingPromotion(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(714)
	seedModelBBattle(t, f, matchID, "167c81e3-92fc-4432-8284-e4fb6dc93638", "pod-race", "uid-race")
	_, first := prepareAndStage(t, f, matchID, "167c81e3-92fc-4432-8284-e4fb6dc93638", "pod-race", "uid-race", true)
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, first, activateInput()); err != nil {
		t.Fatal(err)
	}

	seed, err := f.auth.PrepareCredential(ctx, authBinding(matchID, "167c81e3-92fc-4432-8284-e4fb6dc93638", "pod-race", "uid-race"))
	if err != nil {
		t.Fatal(err)
	}
	secondCredential := credentialFor(f, seed, "uid-race", "pending-before-result")
	if _, err := f.auth.StagePending(ctx, BattleStageInput{
		MatchID: matchID, AllocationID: "167c81e3-92fc-4432-8284-e4fb6dc93638", Credential: secondCredential, AuthTTL: testTTL,
	}); err != nil {
		t.Fatal(err)
	}
	if err := f.auth.MarkDelivered(ctx, matchID, "167c81e3-92fc-4432-8284-e4fb6dc93638", secondCredential, "rv-pending", testTTL); err != nil {
		t.Fatal(err)
	}

	receipt := dsauthrecord.NewBattleResultReceipt(
		matchID, "167c81e3-92fc-4432-8284-e4fb6dc93638", first.PodName, first.InstanceUID, first.InstanceEpoch,
		first.Gen, first.JTI, int64(first.ExpMs), first.Kid, first.TokenSHA256,
		first.WriterEpoch, f.now.UnixMilli())
	receiptRaw, err := dsauthrecord.MarshalBattleResultReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, dsauthrecord.BattleResultReceiptKey(matchID), receiptRaw, testTTL).Err(); err != nil {
		t.Fatal(err)
	}

	second := identityFor("pod-race", secondCredential)
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, second, activateInput()); errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("pending credential promoted after receipt committed: %v", err)
	}
	snapshot, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil || snapshot.Auth.GetActive().GetJti() != first.JTI ||
		snapshot.Battle.GetLastVerifiedJti() != first.JTI {
		t.Fatalf("receipt race changed active authority: %+v err=%v", snapshot, err)
	}
	ended := activateInput()
	ended.State = "ended"
	if result, err := f.auth.ActivateHeartbeat(ctx, matchID, first, ended); err != nil || !result.Terminal || result.Battle.GetState() != "ended" {
		t.Fatalf("original receipt credential could not finish: result=%+v err=%v", result, err)
	}
}

func TestBattleAuthTerminalResultAllowsExpiredProofAfterActiveRotation(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(760)
	const allocationID = "a34f3289-b63f-4631-8efd-b97568c04117"
	const pod = "pod-terminal-rotation"
	const uid = "uid-terminal-rotation"
	seedModelBBattle(t, f, matchID, allocationID, pod, uid)

	_, first := prepareAndStage(t, f, matchID, allocationID, pod, uid, true)
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, first, activateInput()); err != nil {
		t.Fatal(err)
	}
	proof := resultAuthorizationProof(first, f.now.UnixMilli())
	expected := BattleExpectedInstance{
		AllocationID: allocationID, InstanceUID: uid, InstanceEpoch: first.InstanceEpoch,
	}

	// 模拟 ReportResult 已提交 MySQL 后 callback credential 正常轮换。outbox 必须保留
	// 最初通过鉴权的 proof，不能被当前 active gen/jti 替换，也不能因旧 token 过期卡回收。
	f.setNow(f.now.Add(5 * time.Minute))
	_, second := prepareAndStage(t, f, matchID, allocationID, pod, uid, true)
	if second.Gen <= first.Gen || second.JTI == first.JTI || second.InstanceEpoch != first.InstanceEpoch {
		t.Fatalf("rotation fixture invalid: first=%+v second=%+v", first, second)
	}
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, second, activateInput()); err != nil {
		t.Fatalf("promote rotated credential: %v", err)
	}
	f.setNow(time.UnixMilli(int64(first.ExpMs) + time.Second.Milliseconds()))

	terminated, err := f.auth.TerminateResultExpected(ctx, matchID, expected, proof)
	if err != nil || !terminated {
		t.Fatalf("expired old proof after rotation: terminated=%v err=%v", terminated, err)
	}
	snapshot, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil || snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
		snapshot.Battle.GetState() != "ended" || snapshot.Auth.GetActive().GetGen() != second.Gen {
		t.Fatalf("terminal snapshot=%+v err=%v", snapshot, err)
	}
	receiptRaw, err := f.rdb.Get(ctx, dsauthrecord.BattleResultReceiptKey(matchID)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := dsauthrecord.UnmarshalBattleResultReceipt(receiptRaw)
	if err != nil || receipt.Gen != first.Gen || receipt.JTI != first.JTI || receipt.Gen == second.Gen {
		t.Fatalf("terminal receipt replaced original proof: receipt=%+v err=%v", receipt, err)
	}
	// response 丢失后的同 proof 重放仍成功，且只有 UID 条件释放明确成功后才恢复 TTL。
	if terminated, err = f.auth.TerminateResultExpected(ctx, matchID, expected, proof); err != nil || !terminated {
		t.Fatalf("terminal proof retry: terminated=%v err=%v", terminated, err)
	}
	if expired, err := f.auth.ExpireResultTerminatedExpected(ctx, matchID, expected, proof, testTTL); err != nil || !expired {
		t.Fatalf("expire confirmed tombstone: expired=%v err=%v", expired, err)
	}
	if authTTL, battleTTL, receiptTTL := f.mr.TTL(battleAuthKey(matchID)), f.mr.TTL(battleKey(matchID)), f.mr.TTL(dsauthrecord.BattleResultReceiptKey(matchID)); authTTL <= 0 || battleTTL <= 0 || receiptTTL <= 0 {
		t.Fatalf("confirmed tombstone TTLs: auth=%v battle=%v receipt=%v", authTTL, battleTTL, receiptTTL)
	}
	// finalize 响应丢失且 MySQL released 行未删，可能跨过完整 retention；三键全无
	// 代表 cleanup 已完成，finalize-only 重放必须幂等成功。
	f.mr.FastForward(testTTL + time.Second)
	if expired, err := f.auth.ExpireResultTerminatedExpected(ctx, matchID, expected, proof, testTTL); err != nil || !expired {
		t.Fatalf("post-TTL finalize retry: expired=%v err=%v", expired, err)
	}
}

func TestBattleAuthTerminalResultStableIdentityDriftHasZeroSideEffects(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(761)
	const allocationID = "39284eb8-b8db-4537-89f1-098516787a38"
	const pod = "pod-terminal-drift"
	const uid = "uid-terminal-drift"
	seedModelBBattle(t, f, matchID, allocationID, pod, uid)
	_, active := prepareAndStage(t, f, matchID, allocationID, pod, uid, true)
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, active, activateInput()); err != nil {
		t.Fatal(err)
	}
	baseExpected := BattleExpectedInstance{
		AllocationID: allocationID, InstanceUID: uid, InstanceEpoch: active.InstanceEpoch,
	}
	baseProof := resultAuthorizationProof(active, f.now.UnixMilli())
	authBefore := rawRedisBytes(t, f.rdb, battleAuthKey(matchID))
	battleBefore := rawRedisBytes(t, f.rdb, battleKey(matchID))
	authTTLBefore := f.rdb.TTL(ctx, battleAuthKey(matchID)).Val()
	battleTTLBefore := f.rdb.TTL(ctx, battleKey(matchID)).Val()

	tests := []struct {
		name     string
		expected BattleExpectedInstance
		proof    BattleResultAuthorizationProof
	}{
		{name: "allocation", expected: func() BattleExpectedInstance {
			e := baseExpected
			e.AllocationID = "9d783949-ab89-418c-8920-9946d3240a17"
			return e
		}(), proof: baseProof},
		{name: "uid", expected: func() BattleExpectedInstance {
			e := baseExpected
			e.InstanceUID = "uid-rebuilt"
			return e
		}(), proof: func() BattleResultAuthorizationProof {
			p := baseProof
			p.Credential.InstanceUID = "uid-rebuilt"
			return p
		}()},
		{name: "epoch", expected: func() BattleExpectedInstance {
			e := baseExpected
			e.InstanceEpoch++
			return e
		}(), proof: func() BattleResultAuthorizationProof {
			p := baseProof
			p.Credential.InstanceEpoch++
			return p
		}()},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			terminated, err := f.auth.TerminateResultExpected(ctx, matchID, tc.expected, tc.proof)
			if terminated || errcode.As(err) != errcode.ErrUnauthorized {
				t.Fatalf("drift accepted: terminated=%v code=%v err=%v", terminated, errcode.As(err), err)
			}
			if !bytes.Equal(authBefore, rawRedisBytes(t, f.rdb, battleAuthKey(matchID))) ||
				!bytes.Equal(battleBefore, rawRedisBytes(t, f.rdb, battleKey(matchID))) ||
				f.rdb.TTL(ctx, battleAuthKey(matchID)).Val() != authTTLBefore ||
				f.rdb.TTL(ctx, battleKey(matchID)).Val() != battleTTLBefore ||
				f.mr.Exists(dsauthrecord.BattleResultReceiptKey(matchID)) {
				t.Fatal("stable identity drift mutated auth/battle/TTL/receipt")
			}
			if _, err := f.rdb.ZScore(ctx, activeKey, fmt.Sprint(matchID)).Result(); err != nil {
				t.Fatalf("stable identity drift removed retry index: %v", err)
			}
		})
	}
}

func TestBattleAuthQuarantineExpectedFullTuple(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(709)
	seedModelBBattle(t, f, matchID, "95016eef-53b4-4b91-89bc-7f055821622f", "pod-q", "uid-q")
	_, id := prepareAndStage(t, f, matchID, "95016eef-53b4-4b91-89bc-7f055821622f", "pod-q", "uid-q", true)
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, id, activateInput()); err != nil {
		t.Fatal(err)
	}
	wrong := id
	wrong.JTI = "stale"
	if result, err := f.auth.QuarantineExpected(ctx, matchID, BattleQuarantineExpected{
		AllocationID: "95016eef-53b4-4b91-89bc-7f055821622f", Credential: wrong,
	}, testTTL, testTTL); err != nil || result.AuthQuarantined {
		t.Fatalf("stale quarantine result=%+v err=%v", result, err)
	}
	before, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil || before.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE ||
		before.Battle.GetState() == "abandoned" {
		t.Fatalf("mismatched quarantine mutated authority: %+v err=%v", before, err)
	}
	// auth tuple 正确但 battle 投影漂移时必须先吊销唯一权威，且不改错 battle/ZSET。
	drifted := proto.Clone(before.Battle).(*dsv1.BattleStorageRecord)
	drifted.GameserverUid = "uid-rebuilt"
	driftedRaw, err := proto.Marshal(drifted)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, battleKey(matchID), driftedRaw, testTTL).Err(); err != nil {
		t.Fatal(err)
	}
	authBefore := rawRedisBytes(t, f.rdb, battleAuthKey(matchID))
	battleBefore := rawRedisBytes(t, f.rdb, battleKey(matchID))
	scoreBefore, err := f.rdb.ZScore(ctx, activeKey, fmt.Sprint(matchID)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if result, err := f.auth.QuarantineExpected(ctx, matchID, BattleQuarantineExpected{
		AllocationID: "95016eef-53b4-4b91-89bc-7f055821622f", Credential: id,
	}, testTTL, testTTL); err != nil || !result.AuthQuarantined || result.ProjectionAbandoned {
		t.Fatalf("drifted projection quarantine result=%+v err=%v", result, err)
	}
	scoreAfter, err := f.rdb.ZScore(ctx, activeKey, fmt.Sprint(matchID)).Result()
	if err != nil || scoreAfter != scoreBefore ||
		bytes.Equal(authBefore, rawRedisBytes(t, f.rdb, battleAuthKey(matchID))) ||
		!bytes.Equal(battleBefore, rawRedisBytes(t, f.rdb, battleKey(matchID))) {
		t.Fatalf("drifted projection did not isolate auth-only: before=%v after=%v err=%v", scoreBefore, scoreAfter, err)
	}
	originalRaw, err := proto.Marshal(before.Battle)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, battleKey(matchID), originalRaw, testTTL).Err(); err != nil {
		t.Fatal(err)
	}
	if result, err := f.auth.QuarantineExpected(ctx, matchID, BattleQuarantineExpected{
		AllocationID: "95016eef-53b4-4b91-89bc-7f055821622f", Credential: id,
	}, testTTL, testTTL); err != nil || !result.AuthQuarantined || !result.ProjectionAbandoned {
		t.Fatalf("quarantine result=%+v err=%v", result, err)
	}
	after, err := f.auth.ReadAuthority(ctx, matchID)
	if err != nil || after.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_QUARANTINED ||
		after.Battle.GetState() != "abandoned" || after.Auth.GetPending() != nil {
		t.Fatalf("quarantined authority=%+v err=%v", after, err)
	}
	if err := f.auth.CheckActive(ctx, matchID, id); errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("quarantined credential remained active: %v", err)
	}
	if score, err := f.rdb.ZScore(ctx, activeKey, fmt.Sprint(matchID)).Result(); err != nil || score != 0 {
		t.Fatalf("quarantine compensation index score=%v err=%v", score, err)
	}
	if authTTL, battleTTL := f.mr.TTL(battleAuthKey(matchID)), f.mr.TTL(battleKey(matchID)); authTTL != 0 || battleTTL != 0 {
		t.Fatalf("quarantine tombstone must persist both keys: auth=%v battle=%v", authTTL, battleTTL)
	}

	// 模拟同名 GameServer 被重建、battle 投影已被旁路 writer 改成新 UID。
	// QUARANTINED auth 必须在 sameInstance/换实例逻辑之前拦截，不能被新 UID
	// 清空后重新发号；拒绝前后 bytes/TTL/counter 均零变更。
	rebuiltBattle := proto.Clone(after.Battle).(*dsv1.BattleStorageRecord)
	rebuiltBattle.GameserverUid = "uid-rebuilt"
	rebuiltBattle.State = "warming"
	rebuiltBattle.LastVerifiedGen = 0
	rebuiltBattle.LastVerifiedJti = ""
	rebuiltBattle.LastVerifiedWriterEpoch = 0
	rebuiltRaw, err := proto.Marshal(rebuiltBattle)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.rdb.Set(ctx, battleKey(matchID), rebuiltRaw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	authBytesBefore := rawRedisBytes(t, f.rdb, battleAuthKey(matchID))
	battleBytesBefore := rawRedisBytes(t, f.rdb, battleKey(matchID))
	counterBefore := rawRedisBytes(t, f.rdb, battleAuthGenKey(matchID))
	if _, err := f.auth.PrepareCredential(
		ctx, authBinding(matchID, "95016eef-53b4-4b91-89bc-7f055821622f", "pod-q", "uid-rebuilt")); errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("different UID bypassed quarantine tombstone: %v", err)
	}
	if !bytes.Equal(authBytesBefore, rawRedisBytes(t, f.rdb, battleAuthKey(matchID))) ||
		!bytes.Equal(battleBytesBefore, rawRedisBytes(t, f.rdb, battleKey(matchID))) ||
		!bytes.Equal(counterBefore, rawRedisBytes(t, f.rdb, battleAuthGenKey(matchID))) ||
		f.mr.TTL(battleAuthKey(matchID)) != 0 || f.mr.TTL(battleKey(matchID)) != 0 {
		t.Fatal("quarantine different-UID rejection mutated bytes, TTL, or generation counter")
	}
}

func TestBattleAuthHeartbeatEmptyAbandonHasSingleCASWinner(t *testing.T) {
	ctx := context.Background()
	f := newBattleAuthFixture(t)
	const matchID = uint64(711)
	seedModelBBattle(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a")
	_, id := prepareAndStage(t, f, matchID, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a", true)
	in := activateInput()
	in.PlayerCount = 0
	in.EmptyBattleTimeout = time.Minute
	if _, err := f.auth.ActivateHeartbeat(ctx, matchID, id, in); err != nil {
		t.Fatal(err)
	}
	f.setNow(f.now.Add(2 * time.Minute))
	res, err := f.auth.ActivateHeartbeat(ctx, matchID, id, in)
	if err != nil {
		t.Fatalf("empty abandon: %v", err)
	}
	if !res.Terminal || !res.FirstAbandon || res.Battle.State != "abandoned" {
		t.Fatalf("empty abandon result=%+v", res)
	}
	if _, err := f.rdb.ZScore(ctx, activeKey, fmt.Sprint(matchID)).Result(); err != nil {
		t.Fatalf("empty abandoned lifecycle outbox removed: %v", err)
	}
	f.setNow(f.now.Add(time.Second))
	res, err = f.auth.ActivateHeartbeat(ctx, matchID, id, in)
	if err != nil || !res.Terminal || res.FirstAbandon {
		t.Fatalf("empty abandon retry=%+v err=%v", res, err)
	}
}

func TestBattleAuthRedisFailureIsFailClosed(t *testing.T) {
	f := newBattleAuthFixture(t)
	seedModelBBattle(t, f, 709, "d8a3fbe5-1831-4fe1-8521-779b4097aa51", "pod-a", "uid-a")
	f.mr.Close()
	if _, err := f.auth.ReadAuthority(context.Background(), 709); err == nil {
		t.Fatal("Redis failure unexpectedly returned authority")
	}
}
