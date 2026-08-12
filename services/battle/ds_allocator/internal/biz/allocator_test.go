// allocator_test.go — ds_allocator biz 层测试(miniredis 真实跑通)。
package biz

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/battleabort"
	"github.com/luyuancpp/pandora/pkg/config"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/placement"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// 生产 readyPollInterval 是 1s;单测把它调小,避免每次 AllocateBattle 都等满一个轮询周期。
func init() { readyPollInterval = 10 * time.Millisecond }

func testCfg() conf.AllocatorConf {
	return conf.AllocatorConf{
		HeartbeatTimeout:   config.Duration(15 * time.Second),
		SweepInterval:      config.Duration(5 * time.Second),
		BattleTTL:          config.Duration(2 * time.Hour),
		ReadyWaitTimeout:   config.Duration(1 * time.Second), // 测试用短超时,避免慢测
		EmptyBattleTimeout: config.Duration(5 * time.Minute),
		MockDSAddrHost:     "127.0.0.1",
		MockDSPortBase:     30000,
		MockDSPortRange:    1000,
	}
}

// allocateReady 模拟正常时序:并发跑 AllocateBattle,待 warming 镜像出现后用对应 pod 上报一次
// running 心跳,使 DS 进入 running,AllocateBattle 等到 ready 后返回。
func allocateReady(t *testing.T, uc *AllocatorUsecase, repo *data.RedisBattleRepo, matchID uint64, playerIDs []uint64, mapID uint32, gameMode string) *AllocateResult {
	t.Helper()
	ctx := context.Background()
	type out struct {
		res *AllocateResult
		err error
	}
	done := make(chan out, 1)
	go func() {
		res, err := uc.AllocateBattle(ctx, matchID, playerIDs, mapID, gameMode)
		done <- out{res, err}
	}()
	feedReadyHeartbeat(t, uc, repo, matchID, int32(len(playerIDs)))
	r := <-done
	if r.err != nil {
		t.Fatalf("allocate match %d: %v", matchID, r.err)
	}
	return r.res
}

// feedReadyHeartbeat 等 warming 镜像出现后,用其记录的 pod 上报一次 running 心跳。
// 上报前确保 wall clock 已越过 AllocatedAtMs,保证 LastHeartbeatMs 严格大于分配时刻(满足 ready 判定)。
func feedReadyHeartbeat(t *testing.T, uc *AllocatorUsecase, repo *data.RedisBattleRepo, matchID uint64, playerCount int32) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(3 * time.Second)
	var rec *dsv1.BattleStorageRecord
	for {
		b, found, err := repo.GetBattle(ctx, matchID)
		if err == nil && found && b.DsPodName != "" {
			rec = b
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("warming record for match %d never appeared", matchID)
		}
		time.Sleep(5 * time.Millisecond)
	}
	for time.Now().UnixMilli() <= rec.AllocatedAtMs {
		time.Sleep(time.Millisecond)
	}
	if _, err := uc.Heartbeat(ctx, matchID, rec.DsPodName, playerCount, "running", time.Now().UnixMilli()); err != nil {
		t.Fatalf("heartbeat match %d: %v", matchID, err)
	}
}

// newUsecaseWithAlloc 用指定分配器装配 usecase + 真实 miniredis 仓储(返回 mr 供 TTL 断言)。
func newUsecaseWithAlloc(t *testing.T, alloc GameServerAllocator) (*AllocatorUsecase, *data.RedisBattleRepo, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis run: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	repo := data.NewRedisBattleRepo(rdb)
	return NewAllocatorUsecase(repo, alloc, testCfg()), repo, mr
}

func newUsecase(t *testing.T) (*AllocatorUsecase, *data.RedisBattleRepo) {
	t.Helper()
	uc, repo, _ := newUsecaseWithAlloc(t, NewMockGameServerAllocator(testCfg()))
	return uc, repo
}

func TestAllocateBattleRejectsCombatFactionDriftForExistingMatch(t *testing.T) {
	uc, repo := newUsecase(t)
	const matchID = uint64(61001)
	claim := &dsv1.BattleStorageRecord{
		MatchId: matchID, State: stateAllocating, AllocationId: "existing-allocation",
		PlayerIds: []uint64{11, 22}, MapId: 8, GameMode: "custom",
		PlayerCombatFactions: []*dsv1.BattlePlayerCombatFaction{
			{PlayerId: 11, CombatFactionId: 3},
			{PlayerId: 22, CombatFactionId: 3},
		},
	}
	if claimed, _, err := repo.ClaimBattle(t.Context(), claim, time.Hour); err != nil || !claimed {
		t.Fatalf("seed claim: claimed=%t err=%v", claimed, err)
	}
	_, err := uc.AllocateBattleWithCombatFactions(
		t.Context(), matchID, []uint64{22, 11}, map[uint64]uint32{11: 3, 22: 9}, 8, "custom",
		configpb.LevelRatingMode_LEVEL_RATING_MODE_UNSPECIFIED, "")
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("faction drift err=%v code=%v want unavailable", err, errcode.As(err))
	}
	stored, found, getErr := repo.GetBattle(t.Context(), matchID)
	if getErr != nil || !found || stored.GetAllocationId() != "existing-allocation" ||
		stored.GetPlayerCombatFactions()[1].GetCombatFactionId() != 3 {
		t.Fatalf("existing claim mutated: found=%t stored=%v err=%v", found, stored, getErr)
	}
}

func TestAllocateBattlePersistsCanonicalCombatFactions(t *testing.T) {
	uc, repo := newUsecase(t)
	const matchID = uint64(61002)
	type result struct {
		value *AllocateResult
		err   error
	}
	done := make(chan result, 1)
	go func() {
		value, err := uc.AllocateBattleWithCombatFactions(
			context.Background(), matchID, []uint64{22, 11},
			map[uint64]uint32{11: 3, 22: 9}, 8, "custom",
			configpb.LevelRatingMode_LEVEL_RATING_MODE_ELO, "5v5_ranked")
		done <- result{value: value, err: err}
	}()
	feedReadyHeartbeat(t, uc, repo, matchID, 2)
	allocated := <-done
	if allocated.err != nil || allocated.value == nil {
		t.Fatalf("allocate: value=%+v err=%v", allocated.value, allocated.err)
	}
	stored, found, err := repo.GetBattle(t.Context(), matchID)
	if err != nil || !found {
		t.Fatalf("get battle: found=%t err=%v", found, err)
	}
	got := stored.GetPlayerCombatFactions()
	if len(got) != 2 || got[0].GetPlayerId() != 11 || got[0].GetCombatFactionId() != 3 ||
		got[1].GetPlayerId() != 22 || got[1].GetCombatFactionId() != 9 {
		t.Fatalf("stored combat factions=%v", got)
	}
	// 计分模式必须与 roster / map_id / game_mode 同源定格进 canonical 记录:
	// battle_result 结算只认这一份,丢了它就会回落旧口径(game_mode="custom" → 静默算 Elo)。
	if stored.GetRatingMode() != configpb.LevelRatingMode_LEVEL_RATING_MODE_ELO {
		t.Fatalf("stored rating_mode=%v, want ELO(成局定格值必须落进 BattleStorageRecord)", stored.GetRatingMode())
	}
}

func TestCombatFactionMapFromRecordsRequiresCanonicalRosterOrder(t *testing.T) {
	_, err := combatFactionMapFromRecords([]uint64{11, 22}, []*dsv1.BattlePlayerCombatFaction{
		{PlayerId: 22, CombatFactionId: 1}, {PlayerId: 11, CombatFactionId: 1},
	})
	if err == nil {
		t.Fatal("out-of-order persisted combat factions must fail closed")
	}
}

func enableModelBForTest(
	t *testing.T,
	uc *AllocatorUsecase,
	mr *miniredis.Miniredis,
) (*data.RedisBattleAuthRepo, *redis.Client) {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	authRepo := data.NewRedisBattleAuthRepo(rdb)
	signer, err := auth.NewSigner(auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte("battle-model-b-lifecycle-fence-test-secret"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uc.EnableRedisAuthority(authRepo, signer, time.Hour); err != nil {
		t.Fatal(err)
	}
	return authRepo, rdb
}

func TestEnableRedisAuthorityIrreversiblyActivatesStrictStorageWriters(t *testing.T) {
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	if repo.StrictModelBWritesEnabled() || uc.modelB {
		t.Fatal("legacy/local test repository started in strict Model-B mode")
	}
	authRepo, _ := enableModelBForTest(t, uc, mr)
	if !repo.StrictModelBWritesEnabled() || !authRepo.StrictModelBWritesEnabled() || !uc.modelB {
		t.Fatalf("Model-B became visible without both strict writers: battle=%v auth=%v model_b=%v",
			repo.StrictModelBWritesEnabled(), authRepo.StrictModelBWritesEnabled(), uc.modelB)
	}
	// There is intentionally no disable operation. Re-enabling is idempotent
	// and can never return either writer to legacy semantics.
	repo.EnableStrictModelBWrites()
	authRepo.EnableStrictModelBWrites()
	if !repo.StrictModelBWritesEnabled() || !authRepo.StrictModelBWritesEnabled() {
		t.Fatal("strict storage gate regressed after idempotent enable")
	}
}

func seedActiveModelBLegacyPodUID(
	t *testing.T,
	repo *data.RedisBattleRepo,
	authRepo *data.RedisBattleAuthRepo,
	rdb *redis.Client,
	matchID uint64,
	allocationID, podName, gameServerUID string,
	playerCount int32,
) data.BattleCredentialIdentity {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UnixMilli()
	claim := &dsv1.BattleStorageRecord{
		MatchId: matchID, State: stateAllocating, AllocationId: allocationID,
		PlayerIds: []uint64{101, 102}, MapId: 1, GameMode: "ranked",
		AllocatedAtMs: now, LastHeartbeatMs: now, PlayerCount: playerCount,
	}
	if claimed, _, err := repo.ClaimBattle(ctx, claim, time.Hour); err != nil || !claimed {
		t.Fatalf("claim legacy battle: claimed=%v err=%v", claimed, err)
	}
	if fenced, err := repo.FenceBattleAllocation(ctx, matchID, allocationID); err != nil || !fenced {
		t.Fatalf("fence legacy battle: fenced=%v err=%v", fenced, err)
	}
	warming := proto.Clone(claim).(*dsv1.BattleStorageRecord)
	warming.State, warming.DsPodName, warming.DsAddr = stateWarming, podName, "10.0.0.95:7777"
	warming.GameserverUid, warming.PodUid, warming.ReleaseTrack = gameServerUID, "pre-upgrade-pod-uid", "stable"
	if finalized, err := repo.FinalizeFencedBattleAllocation(ctx, warming, time.Hour); err != nil || !finalized {
		t.Fatalf("finalize legacy battle: finalized=%v err=%v", finalized, err)
	}
	seed, err := authRepo.PrepareCredential(ctx, data.BattleAuthorityBinding{
		MatchID: matchID, AllocationID: allocationID, PodName: podName, InstanceUID: gameServerUID,
		RequiredWriterEpoch: data.BattleDSWriterEpochV2, AuthTTL: time.Hour, BattleTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	credential := &dsv1.BattleDSCredential{
		Gen: seed.Gen, Jti: fmt.Sprintf("legacy-jti-%d", matchID),
		ExpMs: uint64(time.Now().Add(time.Hour).UnixMilli()), Kid: "legacy-kid",
		InstanceUid: gameServerUID, InstanceEpoch: seed.InstanceEpoch,
		TokenSha256: fmt.Sprintf("legacy-sha-%d", matchID), WriterEpoch: data.BattleDSWriterEpochV2,
	}
	if _, err := authRepo.StagePending(ctx, data.BattleStageInput{
		MatchID: matchID, AllocationID: allocationID, Credential: credential, AuthTTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if err := authRepo.MarkDelivered(ctx, matchID, allocationID, credential, "legacy-rv", time.Hour); err != nil {
		t.Fatal(err)
	}
	id := data.BattleCredentialIdentity{
		PodName: podName, InstanceUID: gameServerUID, InstanceEpoch: seed.InstanceEpoch,
		Gen: credential.Gen, JTI: credential.Jti, ExpMs: credential.ExpMs, Kid: credential.Kid,
		TokenSHA256: credential.TokenSha256, WriterEpoch: credential.WriterEpoch,
	}
	if _, err := authRepo.ActivateHeartbeat(ctx, matchID, id, data.BattleHeartbeatInput{
		PlayerCount: playerCount, State: stateRunning, AuthTTL: time.Hour, BattleTTL: time.Hour,
		EmptyBattleTimeout: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	// Simulate bytes written by the immediately preceding binary, before tag 19
	// existed. The current writer must not be used to create this shape: its
	// continuous invariant correctly rejects clearing pod_uid.
	legacy, found, err := repo.GetBattle(ctx, matchID)
	if err != nil || !found {
		t.Fatalf("read legacy seed: found=%v err=%v", found, err)
	}
	legacy.PodUid = ""
	if playerCount == 0 {
		legacy.EmptySinceMs = time.Now().Add(-time.Second).UnixMilli()
	}
	payload, err := proto.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, fmt.Sprintf("pandora:ds:battle:{%d}", matchID), payload, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	return id
}

func seedPrecredentialModelBBattle(
	t *testing.T,
	repo *data.RedisBattleRepo,
	matchID uint64,
	allocationID, podName, gameServerUID, podUID string,
) *data.AuthoritativeGameServerAllocation {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UnixMilli()
	claim := &dsv1.BattleStorageRecord{
		MatchId: matchID, State: stateAllocating, AllocationId: allocationID,
		PlayerIds: []uint64{201, 202}, MapId: 1, GameMode: "ranked",
		AllocatedAtMs: now, LastHeartbeatMs: now, PlayerCount: 2,
	}
	if claimed, _, err := repo.ClaimBattle(ctx, claim, time.Hour); err != nil || !claimed {
		t.Fatalf("claim precredential battle: claimed=%v err=%v", claimed, err)
	}
	if fenced, err := repo.FenceBattleAllocation(ctx, matchID, allocationID); err != nil || !fenced {
		t.Fatalf("fence precredential battle: fenced=%v err=%v", fenced, err)
	}
	warming := proto.Clone(claim).(*dsv1.BattleStorageRecord)
	warming.State, warming.DsPodName, warming.DsAddr = stateWarming, podName, "10.0.0.96:7777"
	warming.GameserverUid, warming.PodUid, warming.ReleaseTrack = gameServerUID, podUID, "stable"
	if finalized, err := repo.FinalizeFencedBattleAllocation(ctx, warming, time.Hour); err != nil || !finalized {
		t.Fatalf("finalize precredential battle: finalized=%v err=%v", finalized, err)
	}
	return &data.AuthoritativeGameServerAllocation{
		PodName: podName, InstanceUID: gameServerUID, PodUID: podUID,
		AllocationID: allocationID, ReleaseTrack: "stable",
	}
}

// backdate 把 match 的 last_heartbeat_ms 回拨到远古,模拟心跳超时。
func backdate(t *testing.T, repo *data.RedisBattleRepo, matchID uint64) {
	t.Helper()
	if err := repo.UpdateBattleWithLock(context.Background(), matchID, 3, func(b *dsv1.BattleStorageRecord) error {
		b.LastHeartbeatMs = 1
		return nil
	}, 2*time.Hour); err != nil {
		t.Fatalf("backdate: %v", err)
	}
}

// countingAllocator 包 Mock 分配器并统计 Release 次数,验证补偿重试期间 pod 只回收一次。
type countingAllocator struct {
	inner    GameServerAllocator
	releases int
}

// gatedAllocator 把第一次外部分配卡在闸门上，制造两个 AllocateBattle 真并发的 Get/claim 窗口。
type gatedAllocator struct {
	inner   GameServerAllocator
	calls   atomic.Int32
	started chan struct{}
	proceed chan struct{}
}

type authoritativeTestAllocator struct {
	authoritativeCalls atomic.Int32
	legacyCalls        atomic.Int32
	releases           atomic.Int32
	delivered          chan map[string]string
	allocateResult     *data.AuthoritativeGameServerAllocation
	allocateErr        error
	releaseErr         error
	releaseCheck       func(*data.AuthoritativeGameServerAllocation) error
	resolvePodUID      string
	resolvePodUIDErr   error
	resolvePodUIDCalls atomic.Int32
	probeGone          bool
	probeErr           error
	probeCalls         atomic.Int32
	probeBlock         time.Duration // 模拟控制面挂死:每次 probe 阻塞该时长
	probeHook          func()        // probe 返回前执行(确定性注入 ABA:替换同 match 的分配)
}

// timeoutLateApplyAllocator 模拟 apiserver 在 GSA POST 已进入处理后客户端超时，
// GameServer 稍后才被 controller 标成 Allocated。响应永远不给严格 UID/RV。
type timeoutLateApplyAllocator struct {
	authoritativeTestAllocator
	postStarted chan struct{}
	returnError chan struct{}
	lateApplied atomic.Bool
}

type uncertainResolverTestAllocator struct {
	authoritativeTestAllocator
	resolveCalls  atomic.Int32
	resolveResult *data.AuthoritativeGameServerAllocation
	resolveFound  bool
	resolveErr    error
}

func (a *uncertainResolverTestAllocator) ResolveAllocationByID(
	_ context.Context,
	_ uint64,
	allocationID string,
	_ []uint64,
	_ map[uint64]uint32,
	_ uint32,
	_ string,
) (*data.AuthoritativeGameServerAllocation, bool, error) {
	a.resolveCalls.Add(1)
	if a.resolveResult == nil {
		return nil, a.resolveFound, a.resolveErr
	}
	out := *a.resolveResult
	if out.AllocationID == "" {
		out.AllocationID = allocationID
	}
	return &out, a.resolveFound, a.resolveErr
}

func (a *timeoutLateApplyAllocator) AllocateAuthoritative(
	_ context.Context,
	_ uint64,
	allocationID string,
	_ []uint64,
	_ map[uint64]uint32,
	_ uint32,
	_, _ string,
) (*data.AuthoritativeGameServerAllocation, error) {
	a.authoritativeCalls.Add(1)
	select {
	case a.postStarted <- struct{}{}:
	default:
	}
	<-a.returnError
	a.lateApplied.Store(true)
	return &data.AuthoritativeGameServerAllocation{AllocationID: allocationID},
		errors.New("GSA POST timeout, controller applied later")
}

// rejectingFenceRepo 只替换 POST 前 Redis fence，其他方法仍由真实 miniredis repo 执行。
type rejectingFenceRepo struct {
	data.BattleRepo
	fenceCalls atomic.Int32
}

func (r *rejectingFenceRepo) FenceBattleAllocation(context.Context, uint64, string) (bool, error) {
	r.fenceCalls.Add(1)
	return false, nil
}

func (r *rejectingFenceRepo) EnableStrictModelBWrites() {
	r.BattleRepo.(data.StrictModelBBattleStorage).EnableStrictModelBWrites()
}

func (r *rejectingFenceRepo) StrictModelBWritesEnabled() bool {
	return r.BattleRepo.(data.StrictModelBBattleStorage).StrictModelBWritesEnabled()
}

func (a *authoritativeTestAllocator) Allocate(context.Context, uint64, uint32, string, string) (string, string, string, error) {
	a.legacyCalls.Add(1)
	return "", "", "", errors.New("legacy allocation must not be used in Model B")
}

func (a *authoritativeTestAllocator) AllocateAuthoritative(
	_ context.Context,
	_ uint64,
	allocationID string,
	_ []uint64,
	_ map[uint64]uint32,
	_ uint32,
	_, releaseTrack string,
) (*data.AuthoritativeGameServerAllocation, error) {
	a.authoritativeCalls.Add(1)
	if a.allocateResult != nil || a.allocateErr != nil {
		if a.allocateResult == nil {
			return nil, a.allocateErr
		}
		out := *a.allocateResult
		if out.AllocationID == "" {
			out.AllocationID = allocationID
		}
		if out.ReleaseTrack == "" {
			out.ReleaseTrack = releaseTrack
		}
		return &out, a.allocateErr
	}
	return &data.AuthoritativeGameServerAllocation{
		PodName: "battle-auth-1", Addr: "10.0.0.9:7777", InstanceUID: "uid-auth-1",
		PodUID:          "pod-uid-auth-1",
		ResourceVersion: "101", AllocationID: allocationID, ReleaseTrack: releaseTrack, AnnotationsPresent: true,
	}, nil
}

func (a *authoritativeTestAllocator) DeliverCredential(
	_ context.Context,
	_ *data.AuthoritativeGameServerAllocation,
	annotations map[string]string,
) (string, error) {
	copyAnnotations := make(map[string]string, len(annotations))
	for k, v := range annotations {
		copyAnnotations[k] = v
	}
	a.delivered <- copyAnnotations
	return "102", nil
}

func (a *authoritativeTestAllocator) Release(context.Context, string) error {
	a.releases.Add(1)
	return a.releaseErr
}

func (a *authoritativeTestAllocator) ReleaseExpected(
	_ context.Context,
	allocation *data.AuthoritativeGameServerAllocation,
) error {
	if allocation == nil || (allocation.InstanceUID == "" && allocation.AllocationID == "") {
		return errors.New("missing expected UID")
	}
	if allocation.InstanceUID != "" && allocation.PodUID == "" {
		return errors.New("missing durable expected Pod UID")
	}
	a.releases.Add(1)
	if a.releaseCheck != nil {
		if err := a.releaseCheck(allocation); err != nil {
			return err
		}
	}
	return a.releaseErr
}

func (a *authoritativeTestAllocator) ResolveExpectedPodUID(
	_ context.Context,
	allocation *data.AuthoritativeGameServerAllocation,
) (string, error) {
	a.resolvePodUIDCalls.Add(1)
	if a.resolvePodUIDErr != nil {
		return "", a.resolvePodUIDErr
	}
	if a.resolvePodUID != "" {
		return a.resolvePodUID, nil
	}
	if allocation == nil || allocation.InstanceUID == "" {
		return "", errors.New("missing expected allocation")
	}
	return "pod-uid-for-" + allocation.InstanceUID, nil
}

func (a *authoritativeTestAllocator) ProbeExpectedInstanceGone(
	_ context.Context,
	podName, instanceUID, _ string,
) (bool, error) {
	a.probeCalls.Add(1)
	if podName == "" || instanceUID == "" {
		return false, errors.New("probe requires gameserver name and uid")
	}
	if a.probeBlock > 0 {
		time.Sleep(a.probeBlock)
	}
	if a.probeHook != nil {
		a.probeHook()
	}
	return a.probeGone, a.probeErr
}

func (g *gatedAllocator) Allocate(ctx context.Context, matchID uint64, mapID uint32, gameMode, releaseTrack string) (string, string, string, error) {
	if g.calls.Add(1) == 1 {
		close(g.started)
	}
	select {
	case <-ctx.Done():
		return "", "", "", ctx.Err()
	case <-g.proceed:
	}
	return g.inner.Allocate(ctx, matchID, mapID, gameMode, releaseTrack)
}

func (g *gatedAllocator) Release(ctx context.Context, podName string) error {
	return g.inner.Release(ctx, podName)
}

func (c *countingAllocator) Allocate(ctx context.Context, matchID uint64, mapID uint32, gameMode, releaseTrack string) (string, string, string, error) {
	return c.inner.Allocate(ctx, matchID, mapID, gameMode, releaseTrack)
}

func (c *countingAllocator) Release(ctx context.Context, podName string) error {
	c.releases++
	return c.inner.Release(ctx, podName)
}

// mockLifecycle 记录 PublishLifecycle 调用;前 failFirst 次返回错误(模拟 Kafka 临时不可用)。
type mockLifecycle struct {
	failFirst int
	calls     int
	delivered []uint64
}

type commitThenErrorLifecycle struct {
	calls     int
	delivered []uint64
	failNext  bool
}

func (m *commitThenErrorLifecycle) PublishLifecycle(_ context.Context, evt *dsv1.DSLifecycleEvent) error {
	m.calls++
	m.delivered = append(m.delivered, evt.GetMatchId())
	if m.failNext {
		m.failNext = false
		return errors.New("Kafka ACK response lost after broker commit")
	}
	return nil
}

func (m *mockLifecycle) PublishLifecycle(_ context.Context, evt *dsv1.DSLifecycleEvent) error {
	m.calls++
	if m.calls <= m.failFirst {
		return errors.New("kafka unavailable")
	}
	m.delivered = append(m.delivered, evt.GetMatchId())
	return nil
}

func TestLifecycleStartupGateAndDeliveryFailClosed(t *testing.T) {
	uc, _ := newUsecase(t)
	uc.SetLifecyclePusherRequired(true)
	if err := uc.ValidateLifecyclePusherReady(); err == nil {
		t.Fatal("required lifecycle publisher must be present before startup")
	}
	if delivered := uc.deliverAbandoned(context.Background(), 7, "battle-7", "gs-uid-7", []uint64{1}, 1, "ranked"); delivered {
		t.Fatal("missing required publisher must retain the recovery outbox")
	}

	life := &mockLifecycle{}
	uc.SetLifecyclePusher(life)
	if err := uc.ValidateLifecyclePusherReady(); err != nil {
		t.Fatalf("ready lifecycle publisher rejected: %v", err)
	}
	if delivered := uc.deliverAbandoned(context.Background(), 7, "battle-7", "gs-uid-7", []uint64{1}, 1, "ranked"); !delivered {
		t.Fatal("healthy required publisher should complete delivery")
	}
}

func TestEnableRedisAuthorityAutomaticallyRequiresLifecyclePublisher(t *testing.T) {
	uc, _, mr := newUsecaseWithAlloc(t, &authoritativeTestAllocator{})
	enableModelBForTest(t, uc, mr)
	if err := uc.ValidateLifecyclePusherReady(); err == nil {
		t.Fatal("Redis authority must require lifecycle publication even if main forgot to set policy")
	}
	uc.SetLifecyclePusher(&mockLifecycle{})
	if err := uc.ValidateLifecyclePusherReady(); err != nil {
		t.Fatalf("Redis authority rejected installed lifecycle publisher: %v", err)
	}
}

func TestAllocateBattle(t *testing.T) {
	uc, repo := newUsecase(t)

	res := allocateReady(t, uc, repo, 7, []uint64{10, 20, 30}, 1, "5v5_ranked")
	if res.DSPodName != "pandora-battle-7" {
		t.Fatalf("pod = %q, want pandora-battle-7", res.DSPodName)
	}
	if res.DSAddr != "127.0.0.1:30007" {
		t.Fatalf("addr = %q, want 127.0.0.1:30007", res.DSAddr)
	}
	// AllocateBattle 返回前 DS 必须已用心跳确认 ready/running
	got, _, _ := repo.GetBattle(context.Background(), 7)
	if got.State != stateRunning {
		t.Fatalf("state = %q, want running", got.State)
	}
	if got.LastHeartbeatMs <= got.AllocatedAtMs {
		t.Fatalf("LastHeartbeatMs %d must be > AllocatedAtMs %d (real heartbeat)", got.LastHeartbeatMs, got.AllocatedAtMs)
	}
}

func TestAllocateBattleTrueConcurrencyOnlyOneExternalAllocation(t *testing.T) {
	ctx := context.Background()
	gated := &gatedAllocator{
		inner:   NewMockGameServerAllocator(testCfg()),
		started: make(chan struct{}),
		proceed: make(chan struct{}),
	}
	uc, repo, _ := newUsecaseWithAlloc(t, gated)
	type result struct {
		res *AllocateResult
		err error
	}
	results := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() {
			res, err := uc.AllocateBattle(ctx, 700, []uint64{1, 2}, 1, "ranked")
			results <- result{res: res, err: err}
		}()
	}
	select {
	case <-gated.started:
	case <-time.After(time.Second):
		t.Fatal("external allocation never started")
	}
	// 第一调用仍卡在外部 API；第二调用已有充足时间撞同一持久 claim。
	time.Sleep(50 * time.Millisecond)
	if got := gated.calls.Load(); got != 1 {
		t.Fatalf("external Allocate calls=%d, want exactly 1", got)
	}
	close(gated.proceed)
	feedReadyHeartbeat(t, uc, repo, 700, 2)
	var first *AllocateResult
	for i := 0; i < 2; i++ {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatalf("concurrent allocate %d: %v", i, got.err)
			}
			if first == nil {
				first = got.res
			} else if got.res.DSPodName != first.DSPodName || got.res.DSAddr != first.DSAddr {
				t.Fatalf("callers observed different allocation: first=%+v got=%+v", first, got.res)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("concurrent AllocateBattle did not return")
		}
	}
	if got := gated.calls.Load(); got != 1 {
		t.Fatalf("external Allocate calls after completion=%d, want 1", got)
	}
}

func TestBattleModelB_EndToEndStageDeliverActivateReady(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, _, mr := newUsecaseWithAlloc(t, allocator)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	authRepo := data.NewRedisBattleAuthRepo(rdb)
	secret := []byte("battle-model-b-test-secret-32-bytes!!")
	signer, err := auth.NewSigner(auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience, Secret: secret,
	})
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	if err := uc.EnableRedisAuthority(authRepo, signer, 3*time.Hour); err != nil {
		t.Fatalf("EnableRedisAuthority: %v", err)
	}

	type allocationResult struct {
		res *AllocateResult
		err error
	}
	done := make(chan allocationResult, 1)
	go func() {
		res, err := uc.AllocateBattle(ctx, 800, []uint64{10, 20}, 1, "ranked")
		done <- allocationResult{res: res, err: err}
	}()

	var annotations map[string]string
	select {
	case annotations = <-allocator.delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("credential was not delivered")
	}
	if annotations[battleTokenAnnotationKey] == "" || annotations[battleTokenGenAnnotationKey] != "1" ||
		annotations[battleInstanceUIDAnnotationKey] != "uid-auth-1" || annotations[battleWriterEpochKey] != "2" {
		t.Fatalf("incomplete delivery annotations: %v", annotations)
	}

	var snapshot data.BattleAuthoritySnapshot
	deadline := time.Now().Add(time.Second)
	for {
		snapshot, err = authRepo.ReadAuthority(ctx, 800)
		if err == nil && snapshot.AuthFound && snapshot.Auth.GetDeliveredRv() == "102" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending not marked delivered: snapshot=%+v err=%v", snapshot, err)
		}
		time.Sleep(time.Millisecond)
	}
	pending := snapshot.Auth.GetPending()
	for time.Now().UnixMilli() <= snapshot.Battle.GetAllocatedAtMs() {
		time.Sleep(time.Millisecond)
	}
	hb, err := uc.HeartbeatAuthorized(ctx, 800, data.BattleCredentialIdentity{
		PodName: "battle-auth-1", InstanceUID: pending.GetInstanceUid(),
		InstanceEpoch: pending.GetInstanceEpoch(), Gen: pending.GetGen(), JTI: pending.GetJti(),
		ExpMs: pending.GetExpMs(), Kid: pending.GetKid(), TokenSHA256: pending.GetTokenSha256(),
		WriterEpoch: pending.GetWriterEpoch(),
	}, 2, stateRunning, time.Now().Add(24*time.Hour).UnixMilli()) // future ts 必须被忽略
	if err != nil {
		t.Fatalf("HeartbeatAuthorized: %v", err)
	}
	if hb.AcceptedTokenGen != pending.GetGen() || hb.AcceptedTokenJTI != pending.GetJti() ||
		hb.AcceptedInstanceUID != "uid-auth-1" || hb.AcceptedInstanceEpoch != 1 || hb.AcceptedWriterEpoch != 2 {
		t.Fatalf("incomplete activation ACK: %+v", hb)
	}

	select {
	case got := <-done:
		if got.err != nil || got.res == nil || got.res.DSPodName != "battle-auth-1" ||
			got.res.ReleaseTrack != auth.ReleaseTrackStable {
			t.Fatalf("AllocateBattle: res=%+v err=%v", got.res, got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AllocateBattle did not pass authoritative ready gate")
	}
	if allocator.authoritativeCalls.Load() != 1 || allocator.legacyCalls.Load() != 0 || allocator.releases.Load() != 0 {
		t.Fatalf("allocator calls authoritative=%d legacy=%d releases=%d",
			allocator.authoritativeCalls.Load(), allocator.legacyCalls.Load(), allocator.releases.Load())
	}

	activeSnapshot, err := authRepo.ReadAuthority(ctx, 800)
	if err != nil {
		t.Fatalf("read active: %v", err)
	}
	ready, reason := activeSnapshot.ReadyAuthorized(time.Now().UnixMilli(), time.Minute.Milliseconds())
	if !ready {
		t.Fatalf("authority not ready: %s snapshot=%+v", reason, activeSnapshot)
	}
	// ts_ms 是未来一天，但权威时间必须接近服务器当前时间，不能被客户端延长新鲜度。
	if activeSnapshot.Auth.GetLastActiveHeartbeatMs() > time.Now().Add(time.Second).UnixMilli() {
		t.Fatalf("client ts_ms contaminated authority heartbeat: %d", activeSnapshot.Auth.GetLastActiveHeartbeatMs())
	}
	// 重连查询必须纯只读：成员成功、非成员拒绝，均不能再次调用 GSA。
	beforeCalls := allocator.authoritativeCalls.Load()
	target, err := uc.ResolveBattleTarget(ctx, 800, 10)
	if err != nil || target == nil || target.DSPodName != "battle-auth-1" ||
		target.AllocationID == "" || target.ReleaseTrack != auth.ReleaseTrackStable {
		t.Fatalf("ResolveBattleTarget member: target=%+v err=%v", target, err)
	}
	if _, err := uc.ResolveBattleTarget(ctx, 800, 999); errcode.As(err) != errcode.ErrPermissionDeny {
		t.Fatalf("ResolveBattleTarget non-member err=%v code=%v", err, errcode.As(err))
	}
	if got := allocator.authoritativeCalls.Load(); got != beforeCalls {
		t.Fatalf("read-only reconnect query called GSA: before=%d after=%d", beforeCalls, got)
	}
}

func TestBattleModelBCleanupFencesBeforeReleaseThenPurges(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, _, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(30 * time.Millisecond)
	authRepo, _ := enableModelBForTest(t, uc, mr)

	allocator.releaseCheck = func(allocation *data.AuthoritativeGameServerAllocation) error {
		snapshot, err := authRepo.ReadAuthority(ctx, 820)
		if err != nil {
			return err
		}
		if !snapshot.AuthFound || !snapshot.BattleFound ||
			snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
			snapshot.Auth.GetActive() != nil || snapshot.Auth.GetPending() != nil ||
			snapshot.Battle.GetState() != statePreactiveReleasing ||
			snapshot.Battle.GetGameserverUid() != allocation.InstanceUID {
			return errors.New("ReleaseExpected observed no exact preactive Redis fence")
		}
		if authTTL, battleTTL := mr.TTL("pandora:ds:auth:{820}"), mr.TTL("pandora:ds:battle:{820}"); authTTL != 0 || battleTTL != 0 {
			return errors.New("ReleaseExpected observed expiring preactive fence")
		}
		return nil
	}

	if _, err := uc.AllocateBattle(ctx, 820, []uint64{1, 2}, 1, "ranked"); err == nil || errcode.As(err) != errcode.ErrDSAllocationFailed {
		t.Fatalf("ready timeout err=%v code=%v", err, errcode.As(err))
	}
	if allocator.releases.Load() != 1 {
		t.Fatalf("ReleaseExpected calls=%d", allocator.releases.Load())
	}
	snapshot, err := authRepo.ReadAuthority(ctx, 820)
	if err != nil || snapshot.AuthFound || snapshot.BattleFound {
		t.Fatalf("explicit release success did not purge: snapshot=%+v err=%v", snapshot, err)
	}
}

func TestBattleModelBCleanupReleaseUnknownKeepsFenceAndCrashRetry(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{
		delivered:  make(chan map[string]string, 1),
		releaseErr: errors.New("simulated DELETE timeout with unknown result"),
	}
	uc, _, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(30 * time.Millisecond)
	authRepo, rdb := enableModelBForTest(t, uc, mr)

	if _, err := uc.AllocateBattle(ctx, 821, []uint64{1}, 1, "ranked"); err == nil || errcode.As(err) != errcode.ErrDSAllocationFailed {
		t.Fatalf("ready timeout err=%v code=%v", err, errcode.As(err))
	}
	snapshot, err := authRepo.ReadAuthority(ctx, 821)
	if err != nil || !snapshot.AuthFound || !snapshot.BattleFound ||
		snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
		snapshot.Battle.GetState() != statePreactiveReleasing {
		t.Fatalf("release timeout lost fence: snapshot=%+v err=%v", snapshot, err)
	}
	if authTTL, battleTTL := mr.TTL("pandora:ds:auth:{821}"), mr.TTL("pandora:ds:battle:{821}"); authTTL != 0 || battleTTL != 0 {
		t.Fatalf("release timeout fence must be persistent: auth=%v battle=%v", authTTL, battleTTL)
	}
	authBefore, _ := rdb.Get(ctx, "pandora:ds:auth:{821}").Bytes()
	battleBefore, _ := rdb.Get(ctx, "pandora:ds:battle:{821}").Bytes()
	if _, err := uc.AllocateBattle(ctx, 821, []uint64{1}, 1, "ranked"); err == nil || errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("retry on release fence err=%v code=%v", err, errcode.As(err))
	}
	authAfter, _ := rdb.Get(ctx, "pandora:ds:auth:{821}").Bytes()
	battleAfter, _ := rdb.Get(ctx, "pandora:ds:battle:{821}").Bytes()
	if string(authAfter) != string(authBefore) || string(battleAfter) != string(battleBefore) ||
		mr.TTL("pandora:ds:auth:{821}") != 0 || mr.TTL("pandora:ds:battle:{821}") != 0 {
		t.Fatal("Allocate retry mutated release-timeout bytes or TTL")
	}
	if allocator.authoritativeCalls.Load() != 1 || allocator.releases.Load() != 1 {
		t.Fatalf("release-timeout retry side effects: POST=%d release=%d",
			allocator.authoritativeCalls.Load(), allocator.releases.Load())
	}

	// 模拟进程崩溃后由另一副本接棒：永久 fence 仍在；幂等 UID delete
	// 得到明确成功后才 purge。这里的重试改善 liveness，安全不依赖它一定发生。
	allocator.releaseErr = nil
	if err := rdb.ZAdd(ctx, "pandora:ds:active", redis.Z{Score: 0, Member: 821}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err = authRepo.ReadAuthority(ctx, 821)
	if err != nil || snapshot.AuthFound || snapshot.BattleFound {
		t.Fatalf("confirmed retry did not purge: snapshot=%+v err=%v", snapshot, err)
	}
	if allocator.authoritativeCalls.Load() != 1 || allocator.releases.Load() != 2 {
		t.Fatalf("crash retry side effects: POST=%d release=%d",
			allocator.authoritativeCalls.Load(), allocator.releases.Load())
	}
}

func TestPrecredentialEpochZeroCrashReleasePurgesWithoutTeardownFabrication(t *testing.T) {
	ctx := context.Background()
	const (
		matchID       = uint64(823)
		allocationID  = "cccccccc-1111-4111-8111-111111111111"
		podName       = "battle-precredential-823"
		gameServerUID = "gs-precredential-823"
		podUID        = "pod-precredential-823"
	)
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	allocator.releaseCheck = func(allocation *data.AuthoritativeGameServerAllocation) error {
		if allocation.InstanceEpoch != 0 || allocation.InstanceUID != gameServerUID ||
			allocation.PodUID != podUID || allocation.AllocationID != allocationID {
			return fmt.Errorf("precredential release tuple=%+v", allocation)
		}
		return nil
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	authRepo, _ := enableModelBForTest(t, uc, mr)
	seedPrecredentialModelBBattle(
		t, repo, matchID, allocationID, podName, gameServerUID, podUID)
	battle, found, err := repo.GetBattle(ctx, matchID)
	if err != nil || !found || battle.GetInstanceEpoch() != 0 {
		t.Fatalf("precredential fixture: found=%v battle=%+v err=%v", found, battle, err)
	}
	if outcome := uc.reconcilePreactiveRelease(ctx, battle); outcome != preactiveReleaseCompleted {
		t.Fatalf("epoch-zero precredential release outcome=%v want completed", outcome)
	}
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil || snapshot.AuthFound || snapshot.BattleFound {
		t.Fatalf("epoch-zero precredential release not purged: snapshot=%+v err=%v", snapshot, err)
	}
	if allocator.releases.Load() != 1 {
		t.Fatalf("epoch-zero release calls=%d", allocator.releases.Load())
	}
}

func TestPrecredentialEpochZeroCleanupPathPurgesExactAllocation(t *testing.T) {
	ctx := context.Background()
	const (
		matchID       = uint64(824)
		allocationID  = "dddddddd-1111-4111-8111-111111111111"
		podName       = "battle-precredential-824"
		gameServerUID = "gs-precredential-824"
		podUID        = "pod-precredential-824"
	)
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	authRepo, _ := enableModelBForTest(t, uc, mr)
	allocation := seedPrecredentialModelBBattle(
		t, repo, matchID, allocationID, podName, gameServerUID, podUID)
	uc.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, allocation)
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil || snapshot.AuthFound || snapshot.BattleFound {
		t.Fatalf("epoch-zero cleanup not purged: snapshot=%+v err=%v", snapshot, err)
	}
	if allocator.releases.Load() != 1 {
		t.Fatalf("epoch-zero cleanup release calls=%d", allocator.releases.Load())
	}
}

func TestPrecredentialEpochZeroReleaseUnknownKeepsPermanentFence(t *testing.T) {
	ctx := context.Background()
	const (
		matchID       = uint64(825)
		allocationID  = "eeeeeeee-1111-4111-8111-111111111111"
		podName       = "battle-precredential-825"
		gameServerUID = "gs-precredential-825"
		podUID        = "pod-precredential-825"
	)
	allocator := &authoritativeTestAllocator{
		delivered: make(chan map[string]string, 1), releaseErr: errors.New("DELETE ACK lost"),
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	authRepo, _ := enableModelBForTest(t, uc, mr)
	seedPrecredentialModelBBattle(
		t, repo, matchID, allocationID, podName, gameServerUID, podUID)
	battle, _, _ := repo.GetBattle(ctx, matchID)
	if outcome := uc.reconcilePreactiveRelease(ctx, battle); outcome != preactiveReleaseUnconfirmed {
		t.Fatalf("unknown epoch-zero delete outcome=%v want unconfirmed", outcome)
	}
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil || snapshot.AuthFound || !snapshot.BattleFound ||
		snapshot.Battle.GetState() != statePreactiveReleasing || snapshot.Battle.GetInstanceEpoch() != 0 {
		t.Fatalf("unknown epoch-zero release lost fence: snapshot=%+v err=%v", snapshot, err)
	}
	if ttl := mr.TTL("pandora:ds:battle:{825}"); ttl != 0 {
		t.Fatalf("unknown epoch-zero release fence TTL=%v", ttl)
	}
	if allocator.releases.Load() != 1 {
		t.Fatalf("unknown epoch-zero release calls=%d", allocator.releases.Load())
	}
	allocator.releaseErr = nil
	if outcome := uc.reconcilePreactiveRelease(ctx, snapshot.Battle); outcome != preactiveReleaseCompleted {
		t.Fatalf("epoch-zero ACK-loss retry outcome=%v want completed", outcome)
	}
	if _, found, err := repo.GetBattle(ctx, matchID); err != nil || found {
		t.Fatalf("epoch-zero retry not purged: found=%v err=%v", found, err)
	}
}

func TestBattleModelBTerminalOutboxReleaseUnknownKeepsPermanentTerminatingFence(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{
		delivered:  make(chan map[string]string, 1),
		releaseErr: errors.New("simulated ReleaseExpected timeout"),
	}
	uc, battleRepo, mr := newUsecaseWithAlloc(t, allocator)
	authRepo, _ := enableModelBForTest(t, uc, mr)
	const matchID = uint64(822)
	const allocationID = "2717e1e9-e1b5-4841-81fc-5be66f55b3cc"
	claim := &dsv1.BattleStorageRecord{
		MatchId: matchID, State: stateAllocating, AllocationId: allocationID,
		AllocatedAtMs: time.Now().UnixMilli(), LastHeartbeatMs: time.Now().UnixMilli(),
	}
	if claimed, _, err := battleRepo.ClaimBattle(ctx, claim, time.Hour); err != nil || !claimed {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	if fenced, err := battleRepo.FenceBattleAllocation(ctx, matchID, allocationID); err != nil || !fenced {
		t.Fatalf("allocation fence=%v err=%v", fenced, err)
	}
	battle := proto.Clone(claim).(*dsv1.BattleStorageRecord)
	battle.State, battle.DsPodName, battle.DsAddr = stateWarming, "battle-822", "10.0.0.82:7777"
	battle.GameserverUid = "uid-822"
	battle.PodUid, battle.ReleaseTrack = "pod-uid-822", "stable"
	if finalized, err := battleRepo.FinalizeFencedBattleAllocation(ctx, battle, time.Hour); err != nil || !finalized {
		t.Fatalf("finalize=%v err=%v", finalized, err)
	}
	seed, err := authRepo.PrepareCredential(ctx, data.BattleAuthorityBinding{
		MatchID: matchID, AllocationID: allocationID, PodName: "battle-822", InstanceUID: "uid-822",
		RequiredWriterEpoch: data.BattleDSWriterEpochV2, AuthTTL: time.Hour, BattleTTL: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	credential := &dsv1.BattleDSCredential{
		Gen: seed.Gen, Jti: "jti-822", ExpMs: uint64(time.Now().Add(time.Hour).UnixMilli()),
		Kid: "kid-822", InstanceUid: "uid-822", InstanceEpoch: seed.InstanceEpoch,
		TokenSha256: "sha256-822", WriterEpoch: data.BattleDSWriterEpochV2,
	}
	if _, err := authRepo.StagePending(ctx, data.BattleStageInput{
		MatchID: matchID, AllocationID: allocationID, Credential: credential, AuthTTL: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	if err := authRepo.MarkDelivered(ctx, matchID, allocationID, credential, "rv-822", time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := authRepo.ActivateHeartbeat(ctx, matchID, data.BattleCredentialIdentity{
		PodName: "battle-822", InstanceUID: "uid-822", InstanceEpoch: seed.InstanceEpoch,
		Gen: seed.Gen, JTI: credential.Jti, ExpMs: credential.ExpMs, Kid: credential.Kid,
		TokenSHA256: credential.TokenSha256, WriterEpoch: credential.WriterEpoch,
	}, data.BattleHeartbeatInput{PlayerCount: 1, State: stateRunning, AuthTTL: time.Hour, BattleTTL: time.Hour}); err != nil {
		t.Fatal(err)
	}
	expected := data.BattleExpectedInstance{
		AllocationID: allocationID, InstanceUID: credential.InstanceUid, InstanceEpoch: credential.InstanceEpoch,
	}
	proof := data.BattleResultAuthorizationProof{
		Credential: data.BattleCredentialIdentity{
			PodName: "battle-822", InstanceUID: credential.InstanceUid, InstanceEpoch: credential.InstanceEpoch,
			Gen: credential.Gen, JTI: credential.Jti, ExpMs: credential.ExpMs, Kid: credential.Kid,
			TokenSHA256: credential.TokenSha256, WriterEpoch: credential.WriterEpoch,
		},
		AuthorizedAtMs: time.Now().UnixMilli(),
	}
	allocator.releaseCheck = func(allocation *data.AuthoritativeGameServerAllocation) error {
		if allocation.PodUID != "pod-uid-822" {
			return errors.New("terminal release did not use durable Pod UID")
		}
		snapshot, err := authRepo.ReadAuthority(ctx, matchID)
		if err != nil {
			return err
		}
		if snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
			snapshot.Battle.GetState() != stateEnded ||
			snapshot.Battle.GetGameserverUid() != allocation.InstanceUID ||
			mr.TTL("pandora:ds:auth:{822}") != 0 || mr.TTL("pandora:ds:battle:{822}") != 0 ||
			mr.TTL("pandora:ds:result-receipt:{822}") != 0 {
			return errors.New("terminal outbox called ReleaseExpected before permanent TERMINATING fence")
		}
		return nil
	}

	if err := uc.ReleaseBattleExpected(ctx, matchID, "completed", "battle-822", expected, proof); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("release timeout code=%v err=%v", errcode.As(err), err)
	}
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil || !snapshot.AuthFound || !snapshot.BattleFound ||
		snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
		snapshot.Battle.GetState() != stateEnded {
		t.Fatalf("ReleaseBattle timeout lost terminal fence: snapshot=%+v err=%v", snapshot, err)
	}
	if authTTL, battleTTL, receiptTTL := mr.TTL("pandora:ds:auth:{822}"), mr.TTL("pandora:ds:battle:{822}"), mr.TTL("pandora:ds:result-receipt:{822}"); authTTL != 0 || battleTTL != 0 || receiptTTL != 0 {
		t.Fatalf("ReleaseBattle timeout fence must persist: auth=%v battle=%v receipt=%v", authTTL, battleTTL, receiptTTL)
	}
	if allocator.releases.Load() != 1 {
		t.Fatalf("first release calls=%d", allocator.releases.Load())
	}

	allocator.releaseErr = nil
	if err := uc.ReleaseBattleExpected(ctx, matchID, "completed", "battle-822", expected, proof); err != nil {
		t.Fatalf("idempotent release retry: %v", err)
	}
	snapshot, err = authRepo.ReadAuthority(ctx, matchID)
	if err != nil || !snapshot.AuthFound || !snapshot.BattleFound ||
		snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
		snapshot.Battle.GetState() != stateEnded {
		t.Fatalf("release retry lost retained tombstone: snapshot=%+v err=%v", snapshot, err)
	}
	if authTTL, battleTTL, receiptTTL := mr.TTL("pandora:ds:auth:{822}"), mr.TTL("pandora:ds:battle:{822}"), mr.TTL("pandora:ds:result-receipt:{822}"); authTTL != 0 || battleTTL != 0 || receiptTTL != 0 {
		t.Fatalf("phase1 must keep permanent tombstones until durable DB mark: auth=%v battle=%v receipt=%v", authTTL, battleTTL, receiptTTL)
	}
	if allocator.releases.Load() != 2 {
		t.Fatalf("release retry calls=%d", allocator.releases.Load())
	}

	if err := uc.FinalizeBattleReleaseExpected(ctx, matchID, "battle-822", expected, proof); err != nil {
		t.Fatalf("finalize after durable DB mark: %v", err)
	}
	if authTTL, battleTTL, receiptTTL := mr.TTL("pandora:ds:auth:{822}"), mr.TTL("pandora:ds:battle:{822}"), mr.TTL("pandora:ds:result-receipt:{822}"); authTTL <= 0 || battleTTL <= 0 || receiptTTL <= 0 {
		t.Fatalf("finalize must bound tombstone TTLs: auth=%v battle=%v receipt=%v", authTTL, battleTTL, receiptTTL)
	}
	if allocator.releases.Load() != 2 {
		t.Fatalf("finalize touched Kubernetes: releases=%d", allocator.releases.Load())
	}

	// finalize 响应丢失 + DB DELETE 长期失败可跨过完整 TTL。新进程重放 finalize-only
	// 时三键已自然清空，应幂等成功且绝不再调用 Kubernetes。
	mr.FastForward(3 * time.Hour)
	if err := uc.FinalizeBattleReleaseExpected(ctx, matchID, "battle-822", expected, proof); err != nil {
		t.Fatalf("finalize retry after tombstone TTL: %v", err)
	}
	if allocator.releases.Load() != 2 {
		t.Fatalf("post-TTL finalize touched Kubernetes: releases=%d", allocator.releases.Load())
	}
}

func TestBattleModelBEmptyPodUIDStopsBeforeCredentialAndFinalize(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{
		delivered: make(chan map[string]string, 1),
		allocateResult: &data.AuthoritativeGameServerAllocation{
			PodName: "battle-no-pod-uid", Addr: "10.0.0.91:7777",
			InstanceUID: "gs-uid-91", ResourceVersion: "91",
			ReleaseTrack: "stable",
		},
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	enableModelBForTest(t, uc, mr)
	if _, err := uc.AllocateBattle(ctx, 891, []uint64{1, 2}, 1, "ranked"); err == nil || errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("empty PodUID result=%v code=%v", err, errcode.As(err))
	}
	select {
	case <-allocator.delivered:
		t.Fatal("empty PodUID reached credential delivery")
	default:
	}
	record, found, err := repo.GetBattle(ctx, 891)
	if err != nil || !found || record.GetState() != stateAllocationUncertain || record.GetDsPodName() != "" {
		t.Fatalf("empty PodUID escaped persistent allocation fence: found=%v record=%+v err=%v", found, record, err)
	}
	if allocator.releases.Load() != 0 {
		t.Fatalf("empty PodUID triggered unsafe cleanup: releases=%d", allocator.releases.Load())
	}
}

func TestLegacyPodUIDBackfillsBeforeEmptyAbandonTerminalFence(t *testing.T) {
	ctx := context.Background()
	const (
		matchID       = uint64(892)
		allocationID  = "44444444-4444-4444-8444-444444444444"
		podName       = "battle-legacy-empty"
		gameServerUID = "gs-uid-legacy-empty"
	)
	allocator := &authoritativeTestAllocator{
		delivered:     make(chan map[string]string, 1),
		resolvePodUID: "pod-uid-backfilled-empty",
	}
	allocator.releaseCheck = func(allocation *data.AuthoritativeGameServerAllocation) error {
		if allocation.PodUID != "pod-uid-backfilled-empty" {
			return fmt.Errorf("release PodUID=%q", allocation.PodUID)
		}
		return nil
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.EmptyBattleTimeout = config.Duration(time.Millisecond)
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	lifecycle := &mockLifecycle{}
	uc.SetLifecyclePusher(lifecycle)
	id := seedActiveModelBLegacyPodUID(
		t, repo, authRepo, rdb, matchID, allocationID, podName, gameServerUID, 0)
	result, err := uc.HeartbeatAuthorized(ctx, matchID, id, 0, stateRunning, time.Now().UnixMilli())
	if err != nil || result == nil || result.Command != commandStop {
		t.Fatalf("legacy empty abandon result=%+v err=%v", result, err)
	}
	if allocator.resolvePodUIDCalls.Load() != 1 || allocator.releases.Load() != 1 {
		t.Fatalf("legacy empty effects: resolve=%d release=%d",
			allocator.resolvePodUIDCalls.Load(), allocator.releases.Load())
	}
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil || !snapshot.AuthFound || !snapshot.BattleFound ||
		snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
		snapshot.Battle.GetState() != stateAbandoned ||
		snapshot.Battle.GetPodUid() != "pod-uid-backfilled-empty" {
		t.Fatalf("legacy empty terminal snapshot=%+v err=%v", snapshot, err)
	}
	if lifecycle.calls != 1 {
		t.Fatalf("legacy empty lifecycle count=%d", lifecycle.calls)
	}
}

func TestLegacyPodUIDBackfillsBeforeCompletedTerminalFence(t *testing.T) {
	ctx := context.Background()
	const (
		matchID       = uint64(897)
		allocationID  = "bbbbbbbb-1111-4111-8111-111111111111"
		podName       = "battle-legacy-completed"
		gameServerUID = "gs-uid-legacy-completed"
	)
	allocator := &authoritativeTestAllocator{
		delivered:     make(chan map[string]string, 1),
		resolvePodUID: "pod-uid-backfilled-completed",
	}
	allocator.releaseCheck = func(allocation *data.AuthoritativeGameServerAllocation) error {
		if allocation.PodUID != "pod-uid-backfilled-completed" {
			return fmt.Errorf("completed release PodUID=%q", allocation.PodUID)
		}
		return nil
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	id := seedActiveModelBLegacyPodUID(
		t, repo, authRepo, rdb, matchID, allocationID, podName, gameServerUID, 1)
	expected := data.BattleExpectedInstance{
		AllocationID: allocationID, InstanceUID: gameServerUID, InstanceEpoch: id.InstanceEpoch,
	}
	proof := data.BattleResultAuthorizationProof{Credential: id, AuthorizedAtMs: time.Now().UnixMilli()}
	if err := uc.ReleaseBattleExpected(ctx, matchID, "completed", podName, expected, proof); err != nil {
		t.Fatalf("legacy completed release: %v", err)
	}
	if allocator.resolvePodUIDCalls.Load() != 1 || allocator.releases.Load() != 1 {
		t.Fatalf("legacy completed effects: resolve=%d release=%d",
			allocator.resolvePodUIDCalls.Load(), allocator.releases.Load())
	}
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil || snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
		snapshot.Battle.GetState() != stateEnded ||
		snapshot.Battle.GetPodUid() != "pod-uid-backfilled-completed" {
		t.Fatalf("legacy completed snapshot=%+v err=%v", snapshot, err)
	}
}

func TestLegacyPodUIDPreflightFailureDoesNotAbandonOrFence(t *testing.T) {
	ctx := context.Background()
	const (
		matchID       = uint64(893)
		allocationID  = "55555555-5555-4555-8555-555555555555"
		podName       = "battle-legacy-missing"
		gameServerUID = "gs-uid-legacy-missing"
	)
	allocator := &authoritativeTestAllocator{
		delivered:        make(chan map[string]string, 1),
		resolvePodUIDErr: errors.New("exact K8s GameServer/Pod missing or recreated"),
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.EmptyBattleTimeout = config.Duration(time.Millisecond)
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	lifecycle := &mockLifecycle{}
	uc.SetLifecyclePusher(lifecycle)
	id := seedActiveModelBLegacyPodUID(
		t, repo, authRepo, rdb, matchID, allocationID, podName, gameServerUID, 0)
	if _, err := uc.HeartbeatAuthorized(ctx, matchID, id, 0, stateRunning, time.Now().UnixMilli()); err == nil || errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("missing legacy Pod preflight err=%v code=%v", err, errcode.As(err))
	}
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil || snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE ||
		snapshot.Battle.GetState() != stateRunning || snapshot.Battle.GetPodUid() != "" {
		t.Fatalf("failed preflight changed authority: snapshot=%+v err=%v", snapshot, err)
	}
	if allocator.releases.Load() != 0 || lifecycle.calls != 0 {
		t.Fatalf("failed preflight side effects: release=%d lifecycle=%d",
			allocator.releases.Load(), lifecycle.calls)
	}
}

func TestLegacyPodUIDStaleSweepPreflightFailureDoesNotTerminate(t *testing.T) {
	ctx := context.Background()
	const (
		matchID       = uint64(896)
		allocationID  = "aaaaaaaa-1111-4111-8111-111111111111"
		podName       = "battle-legacy-stale-missing"
		gameServerUID = "gs-uid-legacy-stale-missing"
	)
	allocator := &authoritativeTestAllocator{
		delivered:        make(chan map[string]string, 1),
		resolvePodUIDErr: errors.New("legacy stale GameServer was replaced"),
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	// A future threshold makes the freshly seeded authority stale without
	// mutating its auth/battle projection consistency.
	uc.cfg.HeartbeatTimeout = config.Duration(-time.Second)
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	seedActiveModelBLegacyPodUID(
		t, repo, authRepo, rdb, matchID, allocationID, podName, gameServerUID, 1)
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatal(err)
	}
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil || snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE ||
		snapshot.Battle.GetState() != stateRunning || snapshot.Battle.GetPodUid() != "" {
		t.Fatalf("stale preflight failure changed authority: snapshot=%+v err=%v", snapshot, err)
	}
	if allocator.releases.Load() != 0 {
		t.Fatalf("stale preflight failure released=%d", allocator.releases.Load())
	}
}

func TestLegacyPodUIDBackfillsBeforeAllocationAbortFence(t *testing.T) {
	ctx := context.Background()
	const (
		matchID       = uint64(894)
		allocationID  = "66666666-6666-4666-8666-666666666666"
		podName       = "battle-legacy-abort"
		gameServerUID = "gs-uid-legacy-abort"
	)
	allocator := &authoritativeTestAllocator{
		delivered:     make(chan map[string]string, 1),
		resolvePodUID: "pod-uid-backfilled-abort",
	}
	allocator.releaseCheck = func(allocation *data.AuthoritativeGameServerAllocation) error {
		if allocation.PodUID != "pod-uid-backfilled-abort" {
			return fmt.Errorf("abort release PodUID=%q", allocation.PodUID)
		}
		return nil
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	lifecycle := &mockLifecycle{}
	uc.SetLifecyclePusher(lifecycle)
	id := seedActiveModelBLegacyPodUID(
		t, repo, authRepo, rdb, matchID, allocationID, podName, gameServerUID, 0)
	request := battleabort.Request{
		MatchID: matchID, OperationID: "77777777-7777-4777-8777-777777777777",
		Target: placement.Target{
			PodName: podName, InstanceUID: gameServerUID, InstanceEpoch: id.InstanceEpoch,
			AllocationID: allocationID, ReleaseTrack: "stable",
		},
	}
	if err := uc.AbortPreactiveBattle(ctx, request); err != nil {
		t.Fatalf("legacy allocation abort: %v", err)
	}
	if allocator.resolvePodUIDCalls.Load() != 1 || allocator.releases.Load() != 1 || lifecycle.calls != 1 {
		t.Fatalf("legacy abort effects: resolve=%d release=%d lifecycle=%d",
			allocator.resolvePodUIDCalls.Load(), allocator.releases.Load(), lifecycle.calls)
	}
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil || !snapshot.BattleFound || snapshot.Battle.GetPodUid() != "pod-uid-backfilled-abort" ||
		snapshot.Battle.GetState() != stateAbandoned {
		t.Fatalf("legacy abort snapshot=%+v err=%v", snapshot, err)
	}
}

func TestLegacyPodUIDAbortPreflightFailureCreatesNoAbortFence(t *testing.T) {
	ctx := context.Background()
	const (
		matchID       = uint64(895)
		allocationID  = "88888888-8888-4888-8888-888888888888"
		podName       = "battle-legacy-abort-missing"
		gameServerUID = "gs-uid-legacy-abort-missing"
	)
	allocator := &authoritativeTestAllocator{
		delivered:        make(chan map[string]string, 1),
		resolvePodUIDErr: errors.New("same-name replacement is not the expected Pod"),
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	id := seedActiveModelBLegacyPodUID(
		t, repo, authRepo, rdb, matchID, allocationID, podName, gameServerUID, 0)
	request := battleabort.Request{
		MatchID: matchID, OperationID: "99999999-9999-4999-8999-999999999999",
		Target: placement.Target{
			PodName: podName, InstanceUID: gameServerUID, InstanceEpoch: id.InstanceEpoch,
			AllocationID: allocationID, ReleaseTrack: "stable",
		},
	}
	if err := uc.AbortPreactiveBattle(ctx, request); err == nil || errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("legacy abort mismatch err=%v code=%v", err, errcode.As(err))
	}
	if _, _, found, err := authRepo.ReadAllocationAbort(ctx, matchID); err != nil || found {
		t.Fatalf("failed preflight created abort journal: found=%v err=%v", found, err)
	}
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil || snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE ||
		snapshot.Battle.GetState() != stateRunning || snapshot.Battle.GetPodUid() != "" {
		t.Fatalf("failed abort preflight changed authority: snapshot=%+v err=%v", snapshot, err)
	}
	if allocator.releases.Load() != 0 {
		t.Fatalf("failed abort preflight released=%d", allocator.releases.Load())
	}
}

func TestBattleModelB_StrictGETMissingUIDKeepsPersistentFence(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{
		delivered: make(chan map[string]string, 1),
		allocateResult: &data.AuthoritativeGameServerAllocation{
			PodName: "battle-partial", Addr: "10.0.0.8:7777", AllocationID: "ignored-by-fake",
		},
		allocateErr: errors.New("strict GET missing UID/RV"),
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	signer, err := auth.NewSigner(auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte("battle-model-b-partial-secret-32bytes"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uc.EnableRedisAuthority(data.NewRedisBattleAuthRepo(rdb), signer, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.AllocateBattle(ctx, 802, []uint64{1, 2}, 1, "ranked"); err == nil {
		t.Fatal("strict GET failure must fail allocation")
	}
	if allocator.releases.Load() != 0 {
		t.Fatalf("strict GET failure triggered external cleanup: releases=%d", allocator.releases.Load())
	}
	claim, found, err := repo.GetBattle(ctx, 802)
	if err != nil || !found || claim.GetState() != stateAllocationUncertain {
		t.Fatalf("strict GET failure lost uncertain fence: found=%v claim=%+v err=%v", found, claim, err)
	}
	if ttl := mr.TTL("pandora:ds:battle:{802}"); ttl != 0 {
		t.Fatalf("uncertain fence must be persistent, ttl=%s", ttl)
	}
}

func TestBattleModelB_UnknownUIDKeepsClaimAndBlocksSecondPOST(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{
		delivered: make(chan map[string]string, 1),
		allocateResult: &data.AuthoritativeGameServerAllocation{
			PodName: "battle-partial", Addr: "10.0.0.8:7777", AllocationID: "8f9a2819-8fe9-4c50-84d5-4f898d22f770",
		},
		allocateErr: errors.New("strict GET timeout"),
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(30 * time.Millisecond)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	signer, err := auth.NewSigner(auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte("battle-model-b-unknown-secret-32bytes"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uc.EnableRedisAuthority(data.NewRedisBattleAuthRepo(rdb), signer, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.AllocateBattle(ctx, 803, []uint64{1, 2}, 1, "ranked"); err == nil {
		t.Fatal("identity/delete uncertainty must fail allocation")
	}
	first, found, err := repo.GetBattle(ctx, 803)
	if err != nil || !found || first.GetState() != stateAllocationUncertain {
		t.Fatalf("uncertain allocation claim not retained: found=%v rec=%+v err=%v", found, first, err)
	}
	if ttl := mr.TTL("pandora:ds:battle:{803}"); ttl != 0 {
		t.Fatalf("uncertain allocation claim must not expire: ttl=%s", ttl)
	}
	started := time.Now()
	if _, err := uc.AllocateBattle(ctx, 803, []uint64{1, 2}, 1, "ranked"); err == nil {
		t.Fatal("retry should fail closed on retained claim")
	} else if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("retry code=%v, want ErrUnavailable", errcode.As(err))
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("uncertain retry waited instead of failing closed: %s", elapsed)
	}
	if allocator.authoritativeCalls.Load() != 1 {
		t.Fatalf("uncertain first POST allowed second POST: calls=%d", allocator.authoritativeCalls.Load())
	}
	if allocator.releases.Load() != 0 {
		t.Fatalf("uncertain retry triggered cleanup: releases=%d", allocator.releases.Load())
	}
}

func TestBattleModelB_POSTUnknownWithoutPodStillUsesAllocationFence(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{
		delivered:      make(chan map[string]string, 1),
		allocateResult: &data.AuthoritativeGameServerAllocation{AllocationID: "8f9a2819-8fe9-4c50-84d5-4f898d22f770"},
		allocateErr:    errors.New("POST timeout after possible apply"),
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	signer, err := auth.NewSigner(auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte("battle-model-b-post-unknown-secret-32b"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uc.EnableRedisAuthority(data.NewRedisBattleAuthRepo(rdb), signer, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.AllocateBattle(ctx, 804, []uint64{1}, 1, "ranked"); err == nil {
		t.Fatal("unknown POST must fail closed")
	}
	claim, found, err := repo.GetBattle(ctx, 804)
	if err != nil || !found || claim.GetState() != stateAllocationUncertain {
		t.Fatalf("unknown POST claim was removed: claim=%+v found=%v err=%v", claim, found, err)
	}
	if ttl := mr.TTL("pandora:ds:battle:{804}"); ttl != 0 {
		t.Fatalf("unknown POST claim must be persistent: ttl=%s", ttl)
	}
	if allocator.releases.Load() != 0 {
		t.Fatalf("unknown POST triggered reconciliation side effect: releases=%d", allocator.releases.Load())
	}
}

func TestAllocationUncertainPOSTResponseLossEmptyTerminatesDurably(t *testing.T) {
	ctx := context.Background()
	allocator := &uncertainResolverTestAllocator{
		authoritativeTestAllocator: authoritativeTestAllocator{
			delivered:   make(chan map[string]string, 1),
			allocateErr: errors.New("GSA POST response lost"),
		},
		resolveFound: false,
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	enableModelBForTest(t, uc, mr)
	life := &mockLifecycle{}
	uc.SetLifecyclePusher(life)

	if _, err := uc.AllocateBattle(ctx, 1801, []uint64{11, 12}, 1, "ranked"); err == nil || errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("POST response loss err=%v code=%v", err, errcode.As(err))
	}
	uncertain, found, err := repo.GetBattle(ctx, 1801)
	if err != nil || !found || uncertain.GetState() != stateAllocationUncertain {
		t.Fatalf("uncertain claim found=%t record=%+v err=%v", found, uncertain, err)
	}
	if _, err := mr.ZAdd("pandora:ds:active", 0, "1801"); err != nil {
		t.Fatal(err)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("uncertain sweep: %v", err)
	}
	terminal, found, err := repo.GetBattle(ctx, 1801)
	if err != nil || !found || terminal.GetState() != stateAllocationEmptyFence ||
		terminal.GetAllocationId() != uncertain.GetAllocationId() || len(terminal.GetPlayerIds()) != 2 {
		t.Fatalf("terminal claim found=%t record=%+v err=%v", found, terminal, err)
	}
	if ttl := mr.TTL("pandora:ds:battle:{1801}"); ttl != 0 {
		t.Fatalf("empty reconciliation must retain a permanent cleanup tombstone, got %v", ttl)
	}
	if allocator.authoritativeCalls.Load() != 1 || allocator.resolveCalls.Load() != 1 ||
		allocator.releases.Load() != 1 || life.calls != 1 || len(life.delivered) != 1 {
		t.Fatalf("POST=%d resolve=%d release=%d lifecycle_calls=%d delivered=%v",
			allocator.authoritativeCalls.Load(), allocator.resolveCalls.Load(), allocator.releases.Load(),
			life.calls, life.delivered)
	}
	if ids, err := repo.RangeActiveBattles(ctx); err != nil || len(ids) != 1 || ids[0] != 1801 {
		t.Fatalf("empty terminal lost permanent cleanup index: ids=%v err=%v", ids, err)
	}

	// The original timed-out POST may become visible only after the empty LIST
	// and Kafka ACK. Force the next cleanup pass: it must issue another exact
	// label release without re-publishing lifecycle or losing the tombstone.
	if _, err := mr.ZAdd("pandora:ds:active", 0, "1801"); err != nil {
		t.Fatal(err)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("late-appearance cleanup sweep: %v", err)
	}
	if allocator.releases.Load() != 2 || life.calls != 1 {
		t.Fatalf("late cleanup releases=%d lifecycle_calls=%d, want 2/1",
			allocator.releases.Load(), life.calls)
	}
	still, found, err := repo.GetBattle(ctx, 1801)
	if err != nil || !found || still.GetState() != stateAllocationEmptyFence ||
		mr.TTL("pandora:ds:battle:{1801}") != 0 {
		t.Fatalf("late cleanup lost tombstone found=%t record=%+v ttl=%v err=%v",
			found, still, mr.TTL("pandora:ds:battle:{1801}"), err)
	}
}

func TestAllocationUncertainUniqueResultCapturesExactTupleBeforeRelease(t *testing.T) {
	ctx := context.Background()
	const allocationID = "99999999-9999-4999-8999-999999999999"
	allocator := &uncertainResolverTestAllocator{
		authoritativeTestAllocator: authoritativeTestAllocator{
			delivered: make(chan map[string]string, 1),
			releaseCheck: func(got *data.AuthoritativeGameServerAllocation) error {
				if got.AllocationID != allocationID || got.PodName != "battle-reconcile-1802" ||
					got.InstanceUID != "gs-uid-1802" || got.PodUID != "pod-uid-1802" {
					return errors.New("release did not use exact reconciled GameServer+Pod tuple")
				}
				return nil
			},
		},
		resolveFound: true,
		resolveResult: &data.AuthoritativeGameServerAllocation{
			PodName: "battle-reconcile-1802", InstanceUID: "gs-uid-1802", PodUID: "pod-uid-1802",
			ResourceVersion: "401", AllocationID: allocationID, ReleaseTrack: "stable",
		},
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	enableModelBForTest(t, uc, mr)
	life := &mockLifecycle{}
	uc.SetLifecyclePusher(life)
	claim := &dsv1.BattleStorageRecord{
		MatchId: 1802, State: stateAllocating, AllocationId: allocationID,
		PlayerIds: []uint64{21, 22}, MapId: 1, GameMode: "ranked",
		AllocatedAtMs: 1, LastHeartbeatMs: 1, PlayerCount: 2,
	}
	if claimed, _, err := repo.ClaimBattle(ctx, claim, time.Hour); err != nil || !claimed {
		t.Fatalf("claim: claimed=%t err=%v", claimed, err)
	}
	if fenced, err := repo.FenceBattleAllocation(ctx, claim.GetMatchId(), allocationID); err != nil || !fenced {
		t.Fatalf("pre-POST fence: fenced=%t err=%v", fenced, err)
	}
	if _, err := mr.ZAdd("pandora:ds:active", 0, "1802"); err != nil {
		t.Fatal(err)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("unique reconcile sweep: %v", err)
	}
	terminal, found, err := repo.GetBattle(ctx, 1802)
	if err != nil || !found || terminal.GetState() != stateAbandoned ||
		terminal.GetGameserverUid() != "gs-uid-1802" || terminal.GetPodUid() != "pod-uid-1802" {
		t.Fatalf("exact terminal found=%t record=%+v err=%v", found, terminal, err)
	}
	if allocator.resolveCalls.Load() != 1 || allocator.releases.Load() != 1 || len(life.delivered) != 1 {
		t.Fatalf("resolve=%d release=%d lifecycle=%v",
			allocator.resolveCalls.Load(), allocator.releases.Load(), life.delivered)
	}
}

func TestAllocationUncertainReleaseFenceResumesAfterProcessRestart(t *testing.T) {
	ctx := context.Background()
	const allocationID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	allocator := &uncertainResolverTestAllocator{
		authoritativeTestAllocator: authoritativeTestAllocator{delivered: make(chan map[string]string, 1)},
		resolveErr:                 errors.New("resolver must not be called after durable exact fence"),
	}
	_, repo, mr := newUsecaseWithAlloc(t, allocator)
	claim := &dsv1.BattleStorageRecord{
		MatchId: 1803, State: stateAllocating, AllocationId: allocationID,
		PlayerIds: []uint64{31}, MapId: 1, GameMode: "ranked",
		AllocatedAtMs: 1, LastHeartbeatMs: 1, PlayerCount: 1,
	}
	if claimed, _, err := repo.ClaimBattle(ctx, claim, time.Hour); err != nil || !claimed {
		t.Fatalf("claim: claimed=%t err=%v", claimed, err)
	}
	if fenced, err := repo.FenceBattleAllocation(ctx, claim.GetMatchId(), allocationID); err != nil || !fenced {
		t.Fatalf("pre-POST fence: fenced=%t err=%v", fenced, err)
	}
	resolved := &data.AuthoritativeGameServerAllocation{
		PodName: "battle-reconcile-1803", InstanceUID: "gs-uid-1803", PodUID: "pod-uid-1803",
		ResourceVersion: "501", AllocationID: allocationID, ReleaseTrack: "stable",
	}
	if fenced, err := repo.FenceAllocationUncertainRelease(ctx, 1803, allocationID, resolved); err != nil || !fenced {
		t.Fatalf("durable release fence: fenced=%t err=%v", fenced, err)
	}
	// A new usecase instance represents restart after the exact fence committed
	// but before DELETE/terminal publication.
	restarted := NewAllocatorUsecase(repo, allocator, testCfg())
	enableModelBForTest(t, restarted, mr)
	life := &mockLifecycle{}
	restarted.SetLifecyclePusher(life)
	if _, err := mr.ZAdd("pandora:ds:active", 0, "1803"); err != nil {
		t.Fatal(err)
	}
	if err := restarted.sweepOnce(ctx); err != nil {
		t.Fatalf("restart reconcile sweep: %v", err)
	}
	if allocator.resolveCalls.Load() != 0 || allocator.releases.Load() != 1 || len(life.delivered) != 1 {
		t.Fatalf("restart resolve=%d release=%d lifecycle=%v",
			allocator.resolveCalls.Load(), allocator.releases.Load(), life.delivered)
	}
}

func TestAllocationUncertainDeleteFailureKeepsExactFenceForRestart(t *testing.T) {
	ctx := context.Background()
	const allocationID = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	allocator := &uncertainResolverTestAllocator{
		authoritativeTestAllocator: authoritativeTestAllocator{
			delivered:  make(chan map[string]string, 1),
			releaseErr: errors.New("DELETE response unknown"),
		},
		resolveFound: true,
		resolveResult: &data.AuthoritativeGameServerAllocation{
			PodName: "battle-reconcile-1804", InstanceUID: "gs-uid-1804", PodUID: "pod-uid-1804",
			ResourceVersion: "601", AllocationID: allocationID, ReleaseTrack: "stable",
		},
	}
	first, repo, mr := newUsecaseWithAlloc(t, allocator)
	enableModelBForTest(t, first, mr)
	first.SetLifecyclePusher(&mockLifecycle{})
	claim := &dsv1.BattleStorageRecord{
		MatchId: 1804, State: stateAllocating, AllocationId: allocationID,
		PlayerIds: []uint64{51}, MapId: 1, GameMode: "ranked",
		AllocatedAtMs: 1, LastHeartbeatMs: 1, PlayerCount: 1,
	}
	if claimed, _, err := repo.ClaimBattle(ctx, claim, time.Hour); err != nil || !claimed {
		t.Fatalf("claim: claimed=%t err=%v", claimed, err)
	}
	if fenced, err := repo.FenceBattleAllocation(ctx, claim.GetMatchId(), allocationID); err != nil || !fenced {
		t.Fatalf("pre-POST fence: fenced=%t err=%v", fenced, err)
	}
	if _, err := mr.ZAdd("pandora:ds:active", 0, "1804"); err != nil {
		t.Fatal(err)
	}
	if err := first.sweepOnce(ctx); err != nil {
		t.Fatalf("first sweep: %v", err)
	}
	pending, found, err := repo.GetBattle(ctx, 1804)
	if err != nil || !found || pending.GetState() != stateAllocationReconciling ||
		pending.GetGameserverUid() != "gs-uid-1804" || mr.TTL("pandora:ds:battle:{1804}") != 0 {
		t.Fatalf("DELETE failure lost exact fence found=%t record=%+v ttl=%v err=%v",
			found, pending, mr.TTL("pandora:ds:battle:{1804}"), err)
	}

	allocator.releaseErr = nil
	restarted := NewAllocatorUsecase(repo, allocator, testCfg())
	enableModelBForTest(t, restarted, mr)
	life := &mockLifecycle{}
	restarted.SetLifecyclePusher(life)
	if _, err := mr.ZAdd("pandora:ds:active", 0, "1804"); err != nil {
		t.Fatal(err)
	}
	if err := restarted.sweepOnce(ctx); err != nil {
		t.Fatalf("restart sweep: %v", err)
	}
	terminal, found, err := repo.GetBattle(ctx, 1804)
	if err != nil || !found || terminal.GetState() != stateAbandoned || len(life.delivered) != 1 {
		t.Fatalf("restart terminal found=%t record=%+v lifecycle=%v err=%v",
			found, terminal, life.delivered, err)
	}
}

func TestAllocationUncertainKafkaResponseLossRetriesImmutableTerminal(t *testing.T) {
	ctx := context.Background()
	const allocationID = "ffffffff-ffff-4fff-8fff-ffffffffffff"
	allocator := &uncertainResolverTestAllocator{
		authoritativeTestAllocator: authoritativeTestAllocator{delivered: make(chan map[string]string, 1)},
		resolveFound:               true,
		resolveResult: &data.AuthoritativeGameServerAllocation{
			PodName: "battle-reconcile-1805", InstanceUID: "gs-uid-1805", PodUID: "pod-uid-1805",
			ResourceVersion: "701", AllocationID: allocationID, ReleaseTrack: "stable",
		},
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	enableModelBForTest(t, uc, mr)
	life := &commitThenErrorLifecycle{failNext: true}
	uc.SetLifecyclePusher(life)
	claim := &dsv1.BattleStorageRecord{
		MatchId: 1805, State: stateAllocating, AllocationId: allocationID,
		PlayerIds: []uint64{61}, MapId: 1, GameMode: "ranked",
		AllocatedAtMs: 1, LastHeartbeatMs: 1, PlayerCount: 1,
	}
	if claimed, _, err := repo.ClaimBattle(ctx, claim, time.Hour); err != nil || !claimed {
		t.Fatalf("claim: claimed=%t err=%v", claimed, err)
	}
	if fenced, err := repo.FenceBattleAllocation(ctx, claim.GetMatchId(), allocationID); err != nil || !fenced {
		t.Fatalf("pre-POST fence: fenced=%t err=%v", fenced, err)
	}
	if _, err := mr.ZAdd("pandora:ds:active", 0, "1805"); err != nil {
		t.Fatal(err)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("first sweep: %v", err)
	}
	terminal, found, err := repo.GetBattle(ctx, 1805)
	if err != nil || !found || terminal.GetState() != stateAbandoned ||
		mr.TTL("pandora:ds:battle:{1805}") != 0 || life.calls != 1 {
		t.Fatalf("Kafka response loss lost terminal found=%t record=%+v ttl=%v calls=%d err=%v",
			found, terminal, mr.TTL("pandora:ds:battle:{1805}"), life.calls, err)
	}
	if _, err := mr.ZAdd("pandora:ds:active", 0, "1805"); err != nil {
		t.Fatal(err)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("Kafka retry sweep: %v", err)
	}
	if life.calls != 2 || len(life.delivered) != 2 || allocator.releases.Load() != 2 {
		t.Fatalf("Kafka retry calls=%d delivered=%v exact_releases=%d",
			life.calls, life.delivered, allocator.releases.Load())
	}
	if ttl := mr.TTL("pandora:ds:battle:{1805}"); ttl <= 0 {
		t.Fatalf("ACKed exact terminal missing retention TTL: %v", ttl)
	}
}

func TestAllocationUncertainAmbiguousOrAPIUnknownRemainsPermanent(t *testing.T) {
	for index, tc := range []struct {
		name string
		err  error
	}{
		{name: "multiple", err: errors.New("allocation_id resolved multiple GameServers")},
		{name: "api_unknown", err: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			matchID := uint64(1810 + index)
			allocationID := []string{
				"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
				"cccccccc-cccc-4ccc-8ccc-cccccccccccc",
			}[index]
			allocator := &uncertainResolverTestAllocator{
				authoritativeTestAllocator: authoritativeTestAllocator{delivered: make(chan map[string]string, 1)},
				resolveErr:                 tc.err,
			}
			uc, repo, mr := newUsecaseWithAlloc(t, allocator)
			enableModelBForTest(t, uc, mr)
			life := &mockLifecycle{}
			uc.SetLifecyclePusher(life)
			claim := &dsv1.BattleStorageRecord{
				MatchId: matchID, State: stateAllocating, AllocationId: allocationID,
				PlayerIds: []uint64{41}, MapId: 1, GameMode: "ranked",
				AllocatedAtMs: 1, LastHeartbeatMs: 1, PlayerCount: 1,
			}
			if claimed, _, err := repo.ClaimBattle(ctx, claim, time.Hour); err != nil || !claimed {
				t.Fatalf("claim: claimed=%t err=%v", claimed, err)
			}
			if fenced, err := repo.FenceBattleAllocation(ctx, matchID, allocationID); err != nil || !fenced {
				t.Fatalf("pre-POST fence: fenced=%t err=%v", fenced, err)
			}
			if _, err := mr.ZAdd("pandora:ds:active", 0, fmt.Sprintf("%d", matchID)); err != nil {
				t.Fatal(err)
			}
			before, _ := mr.Get(fmt.Sprintf("pandora:ds:battle:{%d}", matchID))
			if err := uc.sweepOnce(ctx); err != nil {
				t.Fatalf("unknown reconcile sweep: %v", err)
			}
			after, _ := mr.Get(fmt.Sprintf("pandora:ds:battle:{%d}", matchID))
			if after != before || mr.TTL(fmt.Sprintf("pandora:ds:battle:{%d}", matchID)) != 0 ||
				allocator.releases.Load() != 0 || life.calls != 0 {
				t.Fatalf("unknown result mutated fence: before_equal=%t ttl=%v releases=%d lifecycle=%d",
					after == before, mr.TTL(fmt.Sprintf("pandora:ds:battle:{%d}", matchID)),
					allocator.releases.Load(), life.calls)
			}
		})
	}
}

func TestBattleModelB_FenceCASFailureNeverCallsGSA(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	rejecting := &rejectingFenceRepo{BattleRepo: repo}
	uc.repo = rejecting
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	signer, err := auth.NewSigner(auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte("battle-model-b-fence-reject-secret-32b"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uc.EnableRedisAuthority(data.NewRedisBattleAuthRepo(rdb), signer, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.AllocateBattle(ctx, 806, []uint64{1}, 1, "ranked"); err == nil {
		t.Fatal("fence CAS failure must fail allocation")
	} else if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("fence failure code=%v, want ErrUnavailable", errcode.As(err))
	}
	if rejecting.fenceCalls.Load() != 1 || allocator.authoritativeCalls.Load() != 0 {
		t.Fatalf("fence_calls=%d GSA_POST_calls=%d, want 1/0",
			rejecting.fenceCalls.Load(), allocator.authoritativeCalls.Load())
	}
	claim, found, err := repo.GetBattle(ctx, 806)
	if err != nil || !found || claim.GetState() != stateAllocating {
		t.Fatalf("pre-POST claim changed unexpectedly: found=%v claim=%+v err=%v", found, claim, err)
	}
}

func TestBattleModelB_POSTTimeoutLateApplyConcurrentRetryStaysPersistent(t *testing.T) {
	ctx := context.Background()
	allocator := &timeoutLateApplyAllocator{
		authoritativeTestAllocator: authoritativeTestAllocator{delivered: make(chan map[string]string, 1)},
		postStarted:                make(chan struct{}, 1),
		returnError:                make(chan struct{}),
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	signer, err := auth.NewSigner(auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte("battle-model-b-late-apply-secret-32bytes"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uc.EnableRedisAuthority(data.NewRedisBattleAuthRepo(rdb), signer, time.Hour); err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, allocateErr := uc.AllocateBattle(ctx, 807, []uint64{1, 2}, 1, "ranked")
		firstDone <- allocateErr
	}()
	select {
	case <-allocator.postStarted:
	case <-time.After(time.Second):
		t.Fatal("first GSA POST did not start")
	}
	claim, found, err := repo.GetBattle(ctx, 807)
	if err != nil || !found || claim.GetState() != stateAllocationUncertain {
		t.Fatalf("POST began without persistent uncertain fence: found=%v claim=%+v err=%v", found, claim, err)
	}
	if ttl := mr.TTL("pandora:ds:battle:{807}"); ttl != 0 {
		t.Fatalf("POST-timeout fence must be persistent, ttl=%s", ttl)
	}

	// 第一请求仍卡在 POST 时并发重入：必须立即 unavailable，且绝不能第二次 POST。
	secondDone := make(chan error, 1)
	go func() {
		_, allocateErr := uc.AllocateBattle(ctx, 807, []uint64{1, 2}, 1, "ranked")
		secondDone <- allocateErr
	}()
	select {
	case secondErr := <-secondDone:
		if errcode.As(secondErr) != errcode.ErrUnavailable {
			t.Fatalf("concurrent retry code=%v, want ErrUnavailable", errcode.As(secondErr))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("concurrent retry waited instead of failing closed")
	}
	if calls := allocator.authoritativeCalls.Load(); calls != 1 {
		t.Fatalf("concurrent retry issued another GSA POST: calls=%d", calls)
	}

	// 模拟 controller 在客户端超时后才应用原 POST；原请求只能返回 unavailable，
	// 不得以 DeleteCollection/LIST 空作为“未应用”并撤掉 claim。
	close(allocator.returnError)
	select {
	case firstErr := <-firstDone:
		if errcode.As(firstErr) != errcode.ErrUnavailable {
			t.Fatalf("timeout-late-apply code=%v, want ErrUnavailable", errcode.As(firstErr))
		}
	case <-time.After(time.Second):
		t.Fatal("first allocation did not return after simulated timeout")
	}
	if !allocator.lateApplied.Load() {
		t.Fatal("test did not reach simulated late apply")
	}

	// 强制派生索引进入 stale；两轮 sweep 必须严格只读：不 Release、不删 claim、
	// 不刷新/恢复 TTL，也不移出 active。
	if _, err := mr.ZAdd("pandora:ds:active", 0, "807"); err != nil {
		t.Fatalf("backdate active index: %v", err)
	}
	rawBefore, err := rdb.Get(ctx, "pandora:ds:battle:{807}").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := uc.sweepOnce(ctx); err != nil {
			t.Fatalf("sweep %d: %v", i+1, err)
		}
	}
	rawAfter, err := rdb.Get(ctx, "pandora:ds:battle:{807}").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(rawAfter) != string(rawBefore) {
		t.Fatal("sweep mutated persistent uncertain claim")
	}
	if ttl := mr.TTL("pandora:ds:battle:{807}"); ttl != 0 {
		t.Fatalf("sweep restored/changed uncertain TTL: ttl=%s", ttl)
	}
	if allocator.releases.Load() != 0 || allocator.authoritativeCalls.Load() != 1 {
		t.Fatalf("uncertain sweep side effects: releases=%d GSA_POST_calls=%d",
			allocator.releases.Load(), allocator.authoritativeCalls.Load())
	}
	ids, err := repo.RangeActiveBattles(ctx)
	if err != nil || len(ids) != 1 || ids[0] != 807 {
		t.Fatalf("sweep removed uncertain audit index: ids=%v err=%v", ids, err)
	}
}

func TestAllocationUncertainLegacyWriterPathsAlsoFailClosed(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator) // 故意不 EnableRedisAuthority，模拟 legacy 配置副本
	claim := &dsv1.BattleStorageRecord{
		MatchId: 808, State: stateAllocating, AllocationId: "846da32b-76b3-49ca-8ddb-ded159354c97",
		AllocatedAtMs: 1, LastHeartbeatMs: 1,
	}
	if claimed, _, err := repo.ClaimBattle(ctx, claim, time.Hour); err != nil || !claimed {
		t.Fatalf("claim: claimed=%v err=%v", claimed, err)
	}
	if fenced, err := repo.FenceBattleAllocation(ctx, 808, "846da32b-76b3-49ca-8ddb-ded159354c97"); err != nil || !fenced {
		t.Fatalf("fence: fenced=%v err=%v", fenced, err)
	}
	if _, err := mr.ZAdd("pandora:ds:active", 0, "808"); err != nil {
		t.Fatalf("backdate active index: %v", err)
	}
	rawBefore, err := mr.Get("pandora:ds:battle:{808}")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uc.AllocateBattle(ctx, 808, []uint64{1}, 1, "ranked"); err == nil ||
		errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("legacy awaitExisting err=%v code=%v", err, errcode.As(err))
	}
	if err := uc.ReleaseBattle(ctx, 808, "failed"); err == nil || errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("legacy ReleaseBattle err=%v code=%v", err, errcode.As(err))
	}
	if hb, err := uc.Heartbeat(ctx, 808, "old-pod", 1, stateRunning, time.Now().UnixMilli()); err != nil || hb.Command != commandStop {
		t.Fatalf("legacy fenced heartbeat result=%+v err=%v", hb, err)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("legacy sweep: %v", err)
	}
	rawAfter, err := mr.Get("pandora:ds:battle:{808}")
	if err != nil {
		t.Fatal(err)
	}
	if rawAfter != rawBefore || mr.TTL("pandora:ds:battle:{808}") != 0 {
		t.Fatal("legacy path mutated/de-expired uncertain claim")
	}
	if allocator.legacyCalls.Load() != 0 || allocator.releases.Load() != 0 {
		t.Fatalf("legacy path touched external allocator: allocate=%d release=%d",
			allocator.legacyCalls.Load(), allocator.releases.Load())
	}
	ids, err := repo.RangeActiveBattles(ctx)
	if err != nil || len(ids) != 1 || ids[0] != 808 {
		t.Fatalf("legacy path removed uncertain audit index: ids=%v err=%v", ids, err)
	}
}

func TestPreactiveReleaseLegacyWriterPathsAlsoFailClosed(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, _, mr := newUsecaseWithAlloc(t, allocator) // 故意 legacy 配置
	record := &dsv1.BattleStorageRecord{
		MatchId: 809, State: statePreactiveReleasing, AllocationId: "b7488c4e-2b9d-41ab-8e1b-30e798add84c",
		DsPodName: "battle-809", GameserverUid: "uid-809",
		AllocatedAtMs: 1, LastHeartbeatMs: 1,
	}
	payload, err := proto.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	mr.Set("pandora:ds:battle:{809}", string(payload))
	rawBefore, _ := mr.Get("pandora:ds:battle:{809}")
	if _, err := uc.AllocateBattle(ctx, 809, []uint64{1}, 1, "ranked"); err == nil || errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("legacy Allocate release fence err=%v code=%v", err, errcode.As(err))
	}
	if err := uc.ReleaseBattle(ctx, 809, "failed"); err == nil || errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("legacy Release release fence err=%v code=%v", err, errcode.As(err))
	}
	if hb, err := uc.Heartbeat(ctx, 809, "battle-809", 1, stateRunning, time.Now().UnixMilli()); err != nil || hb.Command != commandStop {
		t.Fatalf("legacy Heartbeat release fence result=%+v err=%v", hb, err)
	}
	if _, err := mr.ZAdd("pandora:ds:active", 0, "809"); err != nil {
		t.Fatal(err)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatal(err)
	}
	rawAfter, _ := mr.Get("pandora:ds:battle:{809}")
	if rawAfter != rawBefore || mr.TTL("pandora:ds:battle:{809}") != 0 {
		t.Fatal("legacy paths mutated/de-expired preactive release fence")
	}
	if allocator.legacyCalls.Load() != 0 || allocator.releases.Load() != 0 {
		t.Fatalf("legacy paths touched external allocator: allocate=%d release=%d",
			allocator.legacyCalls.Load(), allocator.releases.Load())
	}
}

func TestBattleModelB_SweepReconcilesCrashedAllocatingClaim(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	signer, err := auth.NewSigner(auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte("battle-model-b-crash-claim-secret-32b"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := uc.EnableRedisAuthority(data.NewRedisBattleAuthRepo(rdb), signer, time.Hour); err != nil {
		t.Fatal(err)
	}
	claim := &dsv1.BattleStorageRecord{
		MatchId: 805, State: stateAllocating, AllocationId: "a81fe5f1-7176-43dc-8bef-241a616bca56",
		AllocatedAtMs:   time.Now().Add(-time.Minute).UnixMilli(),
		LastHeartbeatMs: time.Now().Add(-time.Minute).UnixMilli(),
	}
	if claimed, _, err := repo.ClaimBattle(ctx, claim, time.Hour); err != nil || !claimed {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if _, found, err := repo.GetBattle(ctx, 805); err != nil || found {
		t.Fatalf("crashed claim retained: found=%v err=%v", found, err)
	}
	if allocator.releases.Load() != 0 {
		t.Fatalf("pre-POST allocating claim must not call external release, calls=%d", allocator.releases.Load())
	}
}

func TestBattleModelBSweepReliableCompensationKeepsOutbox(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, battleRepo, mr := newUsecaseWithAlloc(t, allocator)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	authRepo := data.NewRedisBattleAuthRepo(rdb)
	signer, err := auth.NewSigner(auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte("battle-model-b-sweep-secret-32bytes!!"),
	})
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	if err := uc.EnableRedisAuthority(authRepo, signer, time.Hour); err != nil {
		t.Fatalf("EnableRedisAuthority: %v", err)
	}
	const matchID uint64 = 801
	const allocationID = "2116971b-c0c8-4fcf-8302-e82820213c22"
	const pod = "battle-auth-801"
	claim := &dsv1.BattleStorageRecord{
		MatchId: matchID, State: stateAllocating, AllocationId: allocationID,
		AllocatedAtMs:   time.Now().Add(-time.Second).UnixMilli(),
		LastHeartbeatMs: time.Now().Add(-time.Second).UnixMilli(),
	}
	if claimed, _, err := battleRepo.ClaimBattle(ctx, claim, time.Hour); err != nil || !claimed {
		t.Fatalf("claim: claimed=%v err=%v", claimed, err)
	}
	battle := proto.Clone(claim).(*dsv1.BattleStorageRecord)
	battle.State, battle.DsPodName, battle.DsAddr, battle.GameserverUid =
		stateWarming, pod, "10.0.0.9:7777", "uid-801"
	battle.PodUid, battle.ReleaseTrack = "pod-uid-for-uid-801", "stable"
	battle.PlayerIds, battle.MapId, battle.GameMode = []uint64{1, 2}, 1, "ranked"
	if ok, err := battleRepo.FinalizeBattleAllocation(ctx, battle, time.Hour); err != nil || !ok {
		t.Fatalf("finalize: ok=%v err=%v", ok, err)
	}
	seed, err := authRepo.PrepareCredential(ctx, data.BattleAuthorityBinding{
		MatchID: matchID, AllocationID: allocationID, PodName: pod, InstanceUID: "uid-801",
		RequiredWriterEpoch: data.BattleDSWriterEpochV2, AuthTTL: time.Hour, BattleTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	signed, err := signer.SignBattleCredential(
		matchID, pod, "uid-801", seed.InstanceEpoch, seed.Gen, uuid.NewString(), time.Hour)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	verifier, err := auth.NewVerifier(auth.Config{
		Issuer: auth.DSCallbackIssuer, Audience: auth.DSCallbackAudience,
		Secret: []byte("battle-model-b-sweep-secret-32bytes!!"),
	})
	if err != nil {
		t.Fatalf("verifier: %v", err)
	}
	claims, err := verifier.VerifyDSCallback(signed.Token)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	stored := &dsv1.BattleDSCredential{
		Gen: seed.Gen, Jti: claims.JTI(), ExpMs: uint64(signed.ExpMs), Kid: signed.Kid,
		InstanceUid: "uid-801", InstanceEpoch: seed.InstanceEpoch,
		TokenSha256: signed.TokenSHA256, WriterEpoch: signed.WriterEpoch,
	}
	if _, err := authRepo.StagePending(ctx, data.BattleStageInput{
		MatchID: matchID, AllocationID: allocationID, Credential: stored, AuthTTL: time.Hour,
	}); err != nil {
		t.Fatalf("stage: %v", err)
	}
	if err := authRepo.MarkDelivered(ctx, matchID, allocationID, stored, "102", time.Hour); err != nil {
		t.Fatalf("mark: %v", err)
	}
	if _, err := authRepo.ActivateHeartbeat(ctx, matchID, data.BattleCredentialIdentity{
		PodName: pod, InstanceUID: "uid-801", InstanceEpoch: seed.InstanceEpoch,
		Gen: seed.Gen, JTI: stored.Jti, ExpMs: stored.ExpMs, Kid: stored.Kid,
		TokenSHA256: stored.TokenSha256, WriterEpoch: stored.WriterEpoch,
	}, data.BattleHeartbeatInput{PlayerCount: 2, State: stateRunning, AuthTTL: time.Hour, BattleTTL: time.Hour}); err != nil {
		t.Fatalf("activate: %v", err)
	}

	// 同时回拨 auth+battle 权威心跳与派生 ZSET，模拟真实失联。
	authBytes, _ := rdb.Get(ctx, "pandora:ds:auth:{801}").Bytes()
	authRec := &dsv1.BattleDSAuthStorageRecord{}
	if err := proto.Unmarshal(authBytes, authRec); err != nil {
		t.Fatalf("unmarshal auth: %v", err)
	}
	battleBytes, _ := rdb.Get(ctx, "pandora:ds:battle:{801}").Bytes()
	battleRec := &dsv1.BattleStorageRecord{}
	if err := proto.Unmarshal(battleBytes, battleRec); err != nil {
		t.Fatalf("unmarshal battle: %v", err)
	}
	authRec.LastActiveHeartbeatMs, battleRec.LastHeartbeatMs = 1, 1
	authBytes, _ = proto.Marshal(authRec)
	battleBytes, _ = proto.Marshal(battleRec)
	if err := rdb.Set(ctx, "pandora:ds:auth:{801}", authBytes, time.Hour).Err(); err != nil {
		t.Fatalf("backdate auth: %v", err)
	}
	if err := rdb.Set(ctx, "pandora:ds:battle:{801}", battleBytes, time.Hour).Err(); err != nil {
		t.Fatalf("backdate battle: %v", err)
	}
	if _, err := mr.ZAdd("pandora:ds:active", 1, "801"); err != nil {
		t.Fatalf("backdate index: %v", err)
	}
	life := &mockLifecycle{failFirst: 1}
	uc.SetLifecyclePusher(life)
	allocator.releaseErr = errors.New("simulated sweep ReleaseExpected timeout")

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep1: %v", err)
	}
	if ids, _ := battleRepo.RangeActiveBattles(ctx); len(ids) != 1 {
		t.Fatalf("failed release lost outbox: active=%v", ids)
	}
	if allocator.releases.Load() != 1 || life.calls != 0 {
		t.Fatalf("sweep1 releases=%d lifecycle_calls=%d", allocator.releases.Load(), life.calls)
	}
	if authTTL, battleTTL := mr.TTL("pandora:ds:auth:{801}"), mr.TTL("pandora:ds:battle:{801}"); authTTL != 0 || battleTTL != 0 {
		t.Fatalf("release timeout lost permanent terminal fence: auth=%v battle=%v", authTTL, battleTTL)
	}
	allocator.releaseErr = nil
	// 新契约(队头饥饿全类根除):release 未确认后按分配身份退避 HeartbeatTimeout,
	// 下一轮不再立刻重试。此处显式越过退避窗口,验证退避到期后重试照常收敛。
	uc.pruneSweepDeferrals(time.Now().Add(uc.cfg.HeartbeatTimeout.Std() + time.Second))
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep2: %v", err)
	}
	if ids, _ := battleRepo.RangeActiveBattles(ctx); len(ids) != 1 {
		t.Fatalf("failed lifecycle delivery lost outbox: active=%v", ids)
	}
	if allocator.releases.Load() != 2 || life.calls != 1 {
		t.Fatalf("sweep2 releases=%d lifecycle_calls=%d", allocator.releases.Load(), life.calls)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep3: %v", err)
	}
	if ids, _ := battleRepo.RangeActiveBattles(ctx); len(ids) != 0 {
		t.Fatalf("delivered outbox not removed: active=%v", ids)
	}
	// lifecycle 未确认前保留 outbox；每轮都用 UID/allocation fencing 幂等确认外部对象已回收，
	// 避免首轮 DELETE 结果未知却只补偿不再清理。
	if allocator.releases.Load() != 3 || life.calls != 2 || len(life.delivered) != 1 {
		t.Fatalf("retry missed fenced release confirmation or delivery: releases=%d calls=%d delivered=%v",
			allocator.releases.Load(), life.calls, life.delivered)
	}
}

// seedWarmingModelBBattle 造一条已 Prepare/Stage/Deliver、尚未首次心跳的 warming 分配
// (Artic01 冷加载事故形态),权威心跳与派生 ZSET score 都回拨到 lastHeartbeatMs。
func seedWarmingModelBBattle(
	t *testing.T,
	repo *data.RedisBattleRepo,
	authRepo *data.RedisBattleAuthRepo,
	mr *miniredis.Miniredis,
	matchID uint64,
	allocationID, podName, gameServerUID string,
	lastHeartbeatMs int64,
) {
	t.Helper()
	ctx := context.Background()
	claim := &dsv1.BattleStorageRecord{
		MatchId: matchID, State: stateAllocating, AllocationId: allocationID,
		PlayerIds: []uint64{11, 22}, MapId: 8, GameMode: "pve",
		AllocatedAtMs: lastHeartbeatMs, LastHeartbeatMs: lastHeartbeatMs, PlayerCount: 2,
	}
	if claimed, _, err := repo.ClaimBattle(ctx, claim, 2*time.Hour); err != nil || !claimed {
		t.Fatalf("claim warming battle: claimed=%v err=%v", claimed, err)
	}
	if fenced, err := repo.FenceBattleAllocation(ctx, matchID, allocationID); err != nil || !fenced {
		t.Fatalf("fence warming battle: fenced=%v err=%v", fenced, err)
	}
	warming := proto.Clone(claim).(*dsv1.BattleStorageRecord)
	warming.State, warming.DsPodName, warming.DsAddr = stateWarming, podName, "10.0.0.12:7777"
	warming.GameserverUid, warming.PodUid, warming.ReleaseTrack = gameServerUID, "pod-uid-"+gameServerUID, "stable"
	if finalized, err := repo.FinalizeFencedBattleAllocation(ctx, warming, 2*time.Hour); err != nil || !finalized {
		t.Fatalf("finalize warming battle: finalized=%v err=%v", finalized, err)
	}
	seed, err := authRepo.PrepareCredential(ctx, data.BattleAuthorityBinding{
		MatchID: matchID, AllocationID: allocationID, PodName: podName, InstanceUID: gameServerUID,
		RequiredWriterEpoch: data.BattleDSWriterEpochV2, AuthTTL: time.Hour, BattleTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("prepare warming credential: %v", err)
	}
	credential := &dsv1.BattleDSCredential{
		Gen: seed.Gen, Jti: fmt.Sprintf("cold-jti-%d", matchID),
		ExpMs: uint64(time.Now().Add(time.Hour).UnixMilli()), Kid: "cold-kid",
		InstanceUid: gameServerUID, InstanceEpoch: seed.InstanceEpoch,
		TokenSha256: fmt.Sprintf("cold-sha-%d", matchID), WriterEpoch: data.BattleDSWriterEpochV2,
	}
	if _, err := authRepo.StagePending(ctx, data.BattleStageInput{
		MatchID: matchID, AllocationID: allocationID, Credential: credential, AuthTTL: time.Hour,
	}); err != nil {
		t.Fatalf("stage warming credential: %v", err)
	}
	if err := authRepo.MarkDelivered(ctx, matchID, allocationID, credential, "cold-rv", time.Hour); err != nil {
		t.Fatalf("deliver warming credential: %v", err)
	}
	if _, err := mr.ZAdd("pandora:ds:active", float64(lastHeartbeatMs), fmt.Sprint(matchID)); err != nil {
		t.Fatalf("seed active score: %v", err)
	}
}

// 冷加载宽限回归(Artic01 事故):heartbeat_timeout=15s + ready_wait_timeout=120s 下,
// warming 失联 30s 的 sweep 必须零副作用(仍 warming、不 Release、不移出 active);
// 超过 120s 才进入回收。
func TestBattleModelBSweepWarmingColdLoadGrace(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(120 * time.Second) // HeartbeatTimeout 已是 15s(testCfg)
	authRepo, _ := enableModelBForTest(t, uc, mr)
	const matchID = uint64(812)
	const allocationID = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0812"
	warmAgeMs := time.Now().Add(-30 * time.Second).UnixMilli()
	seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocationID, "battle-cold-812", "uid-cold-812", warmAgeMs)

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep at 30s: %v", err)
	}
	b, found, err := repo.GetBattle(ctx, matchID)
	if err != nil || !found || b.GetState() != stateWarming {
		t.Fatalf("cold-load warming reclaimed inside ready-wait grace: found=%v state=%q err=%v",
			found, b.GetState(), err)
	}
	if allocator.releases.Load() != 0 || allocator.resolvePodUIDCalls.Load() != 0 {
		t.Fatalf("grace-window sweep touched external allocator: releases=%d resolves=%d",
			allocator.releases.Load(), allocator.resolvePodUIDCalls.Load())
	}
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil || snapshot.Auth.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_BOOTSTRAP {
		t.Fatalf("grace-window sweep mutated auth phase: %+v err=%v", snapshot.Auth, err)
	}
	if ids, err := repo.RangeActiveBattles(ctx); err != nil || len(ids) != 1 || ids[0] != matchID {
		t.Fatalf("grace-window sweep removed active outbox: ids=%v err=%v", ids, err)
	}

	// 回拨到失联 130s(超过 ready_wait_timeout):必须进入回收。
	staleMs := time.Now().Add(-130 * time.Second).UnixMilli()
	raw, err := mr.Get(fmt.Sprintf("pandora:ds:battle:{%d}", matchID))
	if err != nil {
		t.Fatal(err)
	}
	rec := &dsv1.BattleStorageRecord{}
	if err := proto.Unmarshal([]byte(raw), rec); err != nil {
		t.Fatal(err)
	}
	rec.AllocatedAtMs, rec.LastHeartbeatMs = staleMs, staleMs
	payload, err := proto.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	mr.Set(fmt.Sprintf("pandora:ds:battle:{%d}", matchID), string(payload))
	if _, err := mr.ZAdd("pandora:ds:active", float64(staleMs), fmt.Sprint(matchID)); err != nil {
		t.Fatal(err)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep at 130s: %v", err)
	}
	// 超过 ready 等待阈值后进入回收:判弃 → preactive release fence → 外部 UID 条件
	// Release → purge,单轮即可物理清除(model_b_inflight_reconciled)。
	b, found, err = repo.GetBattle(ctx, matchID)
	if err != nil || (found && b.GetState() == stateWarming) {
		t.Fatalf("cold-load warming survived past ready-wait cutoff: found=%v state=%q err=%v",
			found, b.GetState(), err)
	}
	if allocator.releases.Load() != 1 {
		t.Fatalf("stale warming reclaim must release exactly once, releases=%d", allocator.releases.Load())
	}
}

// Agones 判死加速(接 SDK health ping):warming 仍在冷加载宽限内(age=30s<120s),但
// 编排层权威确认 exact 实例已死 → 本轮放弃时间宽限,立即判弃并走 fenced 回收,
// 不再空等 ready_wait 到期。
func TestBattleModelBSweepWarmingProbeGoneForfeitsGrace(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1), probeGone: true}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(120 * time.Second)
	authRepo, _ := enableModelBForTest(t, uc, mr)
	const matchID = uint64(814)
	const allocationID = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0814"
	warmAgeMs := time.Now().Add(-30 * time.Second).UnixMilli()
	seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocationID, "battle-cold-814", "uid-cold-814", warmAgeMs)

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if allocator.probeCalls.Load() == 0 {
		t.Fatal("warming sweep never probed orchestrator verdict")
	}
	b, found, err := repo.GetBattle(ctx, matchID)
	if err != nil || (found && b.GetState() == stateWarming) {
		t.Fatalf("confirmed-dead warming survived grace: found=%v state=%q err=%v",
			found, b.GetState(), err)
	}
	if allocator.releases.Load() != 1 {
		t.Fatalf("confirmed-dead warming reclaim must release exactly once, releases=%d",
			allocator.releases.Load())
	}
}

// probe 读失败(控制面不可用)必须回退 ready_wait 时间界:宽限内的 warming 保持原状,
// 绝不因"读不到"判死(fail-closed 到慢路径,INC-20260724-001 控制面抖动教训)。
func TestBattleModelBSweepWarmingProbeErrorKeepsGrace(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{
		delivered: make(chan map[string]string, 1),
		probeGone: true, probeErr: errors.New("apiserver context deadline exceeded"),
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(120 * time.Second)
	authRepo, _ := enableModelBForTest(t, uc, mr)
	const matchID = uint64(815)
	const allocationID = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0815"
	warmAgeMs := time.Now().Add(-30 * time.Second).UnixMilli()
	seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocationID, "battle-cold-815", "uid-cold-815", warmAgeMs)

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if allocator.probeCalls.Load() == 0 {
		t.Fatal("warming sweep never probed orchestrator verdict")
	}
	b, found, err := repo.GetBattle(ctx, matchID)
	if err != nil || !found || b.GetState() != stateWarming {
		t.Fatalf("probe failure must fall back to time bound, not reclaim: found=%v state=%q err=%v",
			found, b.GetState(), err)
	}
	if allocator.releases.Load() != 0 {
		t.Fatalf("probe failure released warming instance, releases=%d", allocator.releases.Load())
	}
}

// 确定性 ABA 复现(审查必修-1):probe 探旧分配 A 期间,A 被外部清理、同 match_id 的
// 新分配 B 就位,probe 返回"A 已死"。判死 forfeit 绑定 A 的 exact 身份,事务内核验
// 发现在场的是 B → 旧判死作废,B 按常规冷加载宽限存活,绝不被误杀。
func TestBattleModelBSweepWarmingProbeABADoesNotKillReplacement(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1), probeGone: true}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(120 * time.Second)
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	const matchID = uint64(817)
	const allocA = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0817"
	const allocB = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0818"
	warmAgeMs := time.Now().Add(-30 * time.Second).UnixMilli()
	seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocA, "battle-aba-a", "uid-aba-a", warmAgeMs)
	allocator.probeHook = func() {
		// probe 在途窗口:A 被清理,matchmaker 重试创建 B(同 match_id,全新身份,刚分配)。
		if err := rdb.Del(ctx, fmt.Sprintf("pandora:ds:battle:{%d}", matchID),
			fmt.Sprintf("pandora:ds:auth:{%d}", matchID)).Err(); err != nil {
			t.Errorf("hook del: %v", err)
		}
		seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocB, "battle-aba-b", "uid-aba-b",
			time.Now().UnixMilli())
	}

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if allocator.probeCalls.Load() == 0 {
		t.Fatal("probe never executed, ABA scenario not exercised")
	}
	b, found, err := repo.GetBattle(ctx, matchID)
	if err != nil || !found || b.GetState() != stateWarming || b.GetAllocationId() != allocB {
		t.Fatalf("replacement B killed by stale probe verdict: found=%v state=%q alloc=%q err=%v",
			found, b.GetState(), b.GetAllocationId(), err)
	}
	if allocator.releases.Load() != 0 {
		t.Fatalf("stale probe verdict released replacement B, releases=%d", allocator.releases.Load())
	}
}

// 控制面挂死的 probe 不得跨轮占住队头(审查必修-2,INC-20260724-001 纪律):失败后按
// 分配身份退避,下一轮跳过探测;排在 warming 之后的 ACTIVE 失联项必须在第 2 轮完成
// §9.4 补偿,而不是被反复挂死的 probe 饿到 warming 宽限结束。
func TestBattleModelBSweepBlockedProbeDoesNotStarveQueue(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{
		delivered:  make(chan map[string]string, 1),
		probeErr:   errors.New("apiserver hang"),
		probeBlock: 150 * time.Millisecond,
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(120 * time.Second)
	uc.cfg.SweepInterval = config.Duration(50 * time.Millisecond) // 单轮预算,故意小于 probeBlock
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	life := &mockLifecycle{}
	uc.SetLifecyclePusher(life)

	// warming 项:分配更早 → ZSET score 更老 → 恒在队头,probe 挂死。
	const warmingMatch = uint64(818)
	seedWarmingModelBBattle(t, repo, authRepo, mr, warmingMatch,
		"5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0819", "battle-hol-warm", "uid-hol-warm",
		time.Now().Add(-60*time.Second).UnixMilli())
	// ACTIVE 失联项:score 较新 → 排 warming 之后,依赖队列推进才能拿到补偿。
	const activeMatch = uint64(819)
	seedWarmingModelBBattle(t, repo, authRepo, mr, activeMatch,
		"5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0820", "battle-hol-act", "uid-hol-act",
		time.Now().UnixMilli())
	activateBackdatedModelBBattle(t, authRepo, rdb, mr, activeMatch, "battle-hol-act", "uid-hol-act", 30*time.Second)

	// 第 1 轮:挂死 probe 吃掉单轮预算,ACTIVE 项被让到下一轮;但 probe 失败必须已登记退避。
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep round 1: %v", err)
	}
	firstRoundProbes := allocator.probeCalls.Load()
	if firstRoundProbes == 0 {
		t.Fatal("round 1 never probed, head-of-line scenario not exercised")
	}
	if allocator.releases.Load() != 0 {
		t.Fatalf("round 1 unexpectedly reached active item, releases=%d", allocator.releases.Load())
	}
	// 第 2 轮:退避生效 → 零探测零阻塞,ACTIVE 项完成 terminate+release+lifecycle 补偿。
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep round 2: %v", err)
	}
	if got := allocator.probeCalls.Load(); got != firstRoundProbes {
		t.Fatalf("deferred probe re-executed and kept blocking the queue: calls=%d", got)
	}
	if allocator.releases.Load() != 1 || life.calls != 1 {
		t.Fatalf("active item starved behind blocked probe: releases=%d lifecycle=%d",
			allocator.releases.Load(), life.calls)
	}
	// warming 项仍受时间兜底保护,未被退避误伤。
	b, found, err := repo.GetBattle(ctx, warmingMatch)
	if err != nil || !found || b.GetState() != stateWarming {
		t.Fatalf("warming item lost time-bound protection: found=%v state=%q err=%v",
			found, b.GetState(), err)
	}
}

// waiter 与判死回收并发(复审 P1-1):sweep probe 判死→abandon→fence→UID Release→purge
// 清掉两键后,waiter 必须立刻失败(而不是空转满 ready_wait,实测曾 141.85s),且外部
// Release 恰好一次 —— waiter 侧 cleanup 读不到 battle 键,fence 拒绝,零第二次 Release。
func TestWaitBattleReadyFailsFastAfterConcurrentReclaimPurge(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1), probeGone: true}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(30 * time.Second)
	authRepo, _ := enableModelBForTest(t, uc, mr)
	const matchID = uint64(821)
	const allocationID = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0821"
	warmAgeMs := time.Now().Add(-30 * time.Second).UnixMilli()
	seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocationID, "battle-pw-821", "uid-pw-821", warmAgeMs)

	type waitOut struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan waitOut, 1)
	go func() {
		start := time.Now()
		_, err := uc.waitBattleReady(ctx, matchID, "battle-pw-821", allocationID)
		done <- waitOut{err: err, elapsed: time.Since(start)}
	}()
	// waiter 轮询期间执行完整判死回收链(probe gone → abandon → fence → Release → purge)。
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if _, found, _ := repo.GetBattle(ctx, matchID); found {
		t.Fatal("reclaim did not purge battle record; scenario not exercised")
	}
	out := <-done
	if errcode.As(out.err) != errcode.ErrDSAllocationFailed || errors.Is(out.err, errReadyWaitTimeout) {
		t.Fatalf("waiter err=%v code=%v", out.err, errcode.As(out.err))
	}
	// 所有权哨兵:owner 按契约跳过 cleanup(AllocateBattle 分支),不产生第二路 Release。
	if !errors.Is(out.err, errBattleWaitOwnershipLost) {
		t.Fatalf("purged wait must carry ownership-lost sentinel, err=%v", out.err)
	}
	if out.elapsed > 3*time.Second {
		t.Fatalf("waiter did not fail fast after purge: %v", out.elapsed)
	}
	if allocator.releases.Load() != 1 {
		t.Fatalf("reclaim must release exactly once, releases=%d", allocator.releases.Load())
	}
}

// 所有权 barrier(复审必修):abandon 已提交、sweep 的 ReleaseExpected 仍在途未完成时
// waiter 同时退出——必须携带 ownership-lost 哨兵(owner 跳过 cleanup,不发第二路
// FencePreactiveRelease/Release),全程 ReleaseExpected 恰一次。
func TestWaitBattleReadyOwnershipBarrierDuringInflightRelease(t *testing.T) {
	ctx := context.Background()
	releaseStarted := make(chan struct{})
	releaseGate := make(chan struct{})
	var startOnce sync.Once
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	allocator.releaseCheck = func(*data.AuthoritativeGameServerAllocation) error {
		startOnce.Do(func() { close(releaseStarted) })
		<-releaseGate // 卡住 Release:abandon 已提交、外部回收在途
		return nil
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(30 * time.Second)
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	const matchID = uint64(827)
	const allocationID = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0827"
	seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocationID,
		"battle-bar-827", "uid-bar-827", time.Now().UnixMilli())
	activateBackdatedModelBBattle(t, authRepo, rdb, mr, matchID, "battle-bar-827", "uid-bar-827", 30*time.Second)

	sweepDone := make(chan error, 1)
	go func() { sweepDone <- uc.sweepOnce(ctx) }()
	<-releaseStarted // abandon 已写入权威,Release 阻塞在途

	_, werr := uc.waitBattleReady(ctx, matchID, "battle-bar-827", allocationID)
	if !errors.Is(werr, errBattleWaitOwnershipLost) || errcode.As(werr) != errcode.ErrDSAllocationFailed {
		t.Fatalf("waiter during inflight release err=%v code=%v", werr, errcode.As(werr))
	}
	close(releaseGate)
	if err := <-sweepDone; err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if allocator.releases.Load() != 1 {
		t.Fatalf("inflight-release barrier must keep exactly one ReleaseExpected, releases=%d",
			allocator.releases.Load())
	}
}

// 删除宽限不占队头(复审 P1-2):abandoned 项的外部回收进入 termination grace 后按分配
// 身份退避 —— 第 2 轮不重复 DELETE/Release,队尾 ACTIVE 失联项完成补偿;宽限项的
// deliverAbandoned 本就门控在 release 确认后,退避不丢补偿(outbox 仍在)。
func TestBattleModelBSweepReleaseGraceDoesNotStarveQueue(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	pendingErr := fmt.Errorf("wrap: %w", data.ErrReleaseDeletionPending)
	allocator.releaseCheck = func(a *data.AuthoritativeGameServerAllocation) error {
		if a != nil && a.InstanceUID == "uid-grace-head" {
			time.Sleep(150 * time.Millisecond) // 模拟真实确认耗时,吃掉单轮预算
			return pendingErr
		}
		return nil
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(120 * time.Second)
	uc.cfg.SweepInterval = config.Duration(50 * time.Millisecond)
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	life := &mockLifecycle{}
	uc.SetLifecyclePusher(life)

	// 队头:失联更久的 ACTIVE 项,回收进入删除宽限。
	const headMatch = uint64(822)
	seedWarmingModelBBattle(t, repo, authRepo, mr, headMatch,
		"5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0822", "battle-grace-head", "uid-grace-head",
		time.Now().Add(-60*time.Second).UnixMilli())
	activateBackdatedModelBBattle(t, authRepo, rdb, mr, headMatch, "battle-grace-head", "uid-grace-head", 60*time.Second)
	// 队尾:刚失联 30s 的 ACTIVE 项,依赖队列推进拿补偿。
	const tailMatch = uint64(823)
	seedWarmingModelBBattle(t, repo, authRepo, mr, tailMatch,
		"5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0823", "battle-grace-tail", "uid-grace-tail",
		time.Now().UnixMilli())
	activateBackdatedModelBBattle(t, authRepo, rdb, mr, tailMatch, "battle-grace-tail", "uid-grace-tail", 30*time.Second)

	// 第 1 轮:队头 terminate+release → 宽限 pending → 登记退避;预算被吃掉,队尾让路。
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep round 1: %v", err)
	}
	if allocator.releases.Load() != 1 || life.calls != 0 {
		t.Fatalf("round 1 unexpected: releases=%d lifecycle=%d", allocator.releases.Load(), life.calls)
	}
	// 第 2 轮:队头退避生效(零重复 Release),队尾完成 terminate+release+lifecycle 补偿。
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep round 2: %v", err)
	}
	if allocator.releases.Load() != 2 || life.calls != 1 {
		t.Fatalf("grace head starved tail or repeated release: releases=%d lifecycle=%d",
			allocator.releases.Load(), life.calls)
	}
	// 队头仍留在补偿 outbox(release 未确认不得 deliver/expire)。
	if ids, err := repo.RangeActiveBattles(ctx); err != nil || len(ids) == 0 {
		t.Fatalf("grace head lost compensation outbox: ids=%v err=%v", ids, err)
	}
}

// 队头饥饿全类根除(复审必修):普通 release 错误(控制面超时等,非 deletion-pending)
// 同样首轮后按分配身份退避——第 2 轮零重试,队尾完成补偿(永久控制面故障形态)。
func TestBattleModelBSweepReleaseErrorDefersHead(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	allocator.releaseCheck = func(a *data.AuthoritativeGameServerAllocation) error {
		if a != nil && a.InstanceUID == "uid-err-head" {
			time.Sleep(150 * time.Millisecond)
			return errors.New("apiserver context deadline exceeded")
		}
		return nil
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(120 * time.Second)
	uc.cfg.SweepInterval = config.Duration(50 * time.Millisecond)
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	life := &mockLifecycle{}
	uc.SetLifecyclePusher(life)

	const headMatch = uint64(824)
	seedWarmingModelBBattle(t, repo, authRepo, mr, headMatch,
		"5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0824", "battle-err-head", "uid-err-head",
		time.Now().Add(-60*time.Second).UnixMilli())
	activateBackdatedModelBBattle(t, authRepo, rdb, mr, headMatch, "battle-err-head", "uid-err-head", 60*time.Second)
	const tailMatch = uint64(825)
	seedWarmingModelBBattle(t, repo, authRepo, mr, tailMatch,
		"5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0825", "battle-err-tail", "uid-err-tail",
		time.Now().UnixMilli())
	activateBackdatedModelBBattle(t, authRepo, rdb, mr, tailMatch, "battle-err-tail", "uid-err-tail", 30*time.Second)

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep round 1: %v", err)
	}
	if allocator.releases.Load() != 1 || life.calls != 0 {
		t.Fatalf("round 1 unexpected: releases=%d lifecycle=%d", allocator.releases.Load(), life.calls)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep round 2: %v", err)
	}
	if allocator.releases.Load() != 2 || life.calls != 1 {
		t.Fatalf("plain release error starved tail or repeated retry: releases=%d lifecycle=%d",
			allocator.releases.Load(), life.calls)
	}
	if ids, err := repo.RangeActiveBattles(ctx); err != nil || len(ids) == 0 {
		t.Fatalf("head lost compensation outbox: ids=%v err=%v", ids, err)
	}
}

// epoch=0 resume(§9.4 最后一棒)的外部确认失败同样退避:连续两轮只发一次 ReleaseExpected,
// outbox 保留(退避只延后节奏,不丢补偿)。
func TestBattleModelBSweepResumeAbandonedDefersOnFailure(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{
		delivered:  make(chan map[string]string, 1),
		releaseErr: errors.New("apiserver timeout"),
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(120 * time.Second)
	enableModelBForTest(t, uc, mr)

	const matchID = uint64(826)
	staleMs := time.Now().Add(-60 * time.Second).UnixMilli()
	rec := &dsv1.BattleStorageRecord{
		MatchId: matchID, State: stateAbandoned,
		AllocationId: "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0826",
		DsPodName:    "battle-resume-826", GameserverUid: "uid-resume-826", PodUid: "pod-uid-resume-826",
		InstanceEpoch: 0, PlayerIds: []uint64{11}, MapId: 8, GameMode: "pve",
		AllocatedAtMs: staleMs, LastHeartbeatMs: staleMs, ReleaseTrack: "stable",
	}
	payload, err := proto.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	mr.Set(fmt.Sprintf("pandora:ds:battle:{%d}", matchID), string(payload))
	if _, err := mr.ZAdd("pandora:ds:active", float64(staleMs), fmt.Sprint(matchID)); err != nil {
		t.Fatal(err)
	}

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep round 1: %v", err)
	}
	if allocator.releases.Load() != 1 {
		t.Fatalf("round 1 resume release calls=%d want 1", allocator.releases.Load())
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep round 2: %v", err)
	}
	if allocator.releases.Load() != 1 {
		t.Fatalf("deferred resume re-executed release: calls=%d", allocator.releases.Load())
	}
	if ids, err := repo.RangeActiveBattles(ctx); err != nil || len(ids) != 1 {
		t.Fatalf("resume deferral lost outbox: ids=%v err=%v", ids, err)
	}
}

// bootstrap/no-active 回收的结构化退避(复审 P1-2):release 未确认→按 allocation 退避
// (跨 abandoned→preactive_release_pending 状态迁移仍有效);第 2 轮零外部调用、队尾完成
// 补偿;越过退避后收敛(release 成功→purge,记录物理消失)。
func TestBattleModelBSweepBootstrapReleaseUnconfirmedDefersHead(t *testing.T) {
	ctx := context.Background()
	var headReleaseFail atomic.Bool
	headReleaseFail.Store(true)
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	allocator.releaseCheck = func(a *data.AuthoritativeGameServerAllocation) error {
		if a != nil && a.InstanceUID == "uid-bs-head" && headReleaseFail.Load() {
			time.Sleep(150 * time.Millisecond) // 吃满单轮预算,复现队头阻塞
			return errors.New("apiserver context deadline exceeded")
		}
		return nil
	}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(120 * time.Second)
	uc.cfg.SweepInterval = config.Duration(50 * time.Millisecond)
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	life := &mockLifecycle{}
	uc.SetLifecyclePusher(life)

	// 队头:BOOTSTRAP(已投递未激活)warming,已超 120s 冷加载宽限,score 最老。
	const headMatch = uint64(828)
	seedWarmingModelBBattle(t, repo, authRepo, mr, headMatch,
		"5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0828", "battle-bs-head", "uid-bs-head",
		time.Now().Add(-130*time.Second).UnixMilli())
	// 队尾:失联 30s 的 ACTIVE,依赖队列推进拿 §9.4 补偿。
	const tailMatch = uint64(829)
	seedWarmingModelBBattle(t, repo, authRepo, mr, tailMatch,
		"5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0829", "battle-bs-tail", "uid-bs-tail",
		time.Now().UnixMilli())
	activateBackdatedModelBBattle(t, authRepo, rdb, mr, tailMatch, "battle-bs-tail", "uid-bs-tail", 30*time.Second)

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep round 1: %v", err)
	}
	if allocator.releases.Load() != 1 || life.calls != 0 {
		t.Fatalf("round 1 unexpected: releases=%d lifecycle=%d", allocator.releases.Load(), life.calls)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep round 2: %v", err)
	}
	if allocator.releases.Load() != 2 || life.calls != 1 {
		t.Fatalf("bootstrap head repeated external call or starved tail: releases=%d lifecycle=%d",
			allocator.releases.Load(), life.calls)
	}
	// 越过退避窗口后必须收敛。
	headReleaseFail.Store(false)
	uc.pruneSweepDeferrals(time.Now().Add(uc.cfg.HeartbeatTimeout.Std() + time.Second))
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep round 3: %v", err)
	}
	if _, found, err := repo.GetBattle(ctx, headMatch); err != nil || found {
		t.Fatalf("bootstrap head did not converge after backoff: found=%v err=%v", found, err)
	}
	if allocator.releases.Load() != 3 {
		t.Fatalf("convergence release count=%d want 3", allocator.releases.Load())
	}
}

// abort/quarantine 接管态的并发 waiter(复审 P1-3):这些状态/相位已由回收或隔离链接管,
// waiter 必须立即携带所有权哨兵退出且零 Release。
func TestWaitBattleReadyOwnershipLostOnAbortAndQuarantine(t *testing.T) {
	ctx := context.Background()
	t.Run("allocation abort pending state", func(t *testing.T) {
		allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
		uc, repo, mr := newUsecaseWithAlloc(t, allocator)
		uc.cfg.ReadyWaitTimeout = config.Duration(30 * time.Second)
		authRepo, rdb := enableModelBForTest(t, uc, mr)
		const matchID = uint64(830)
		const allocationID = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0830"
		seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocationID,
			"battle-abort-830", "uid-abort-830", time.Now().UnixMilli())
		key := fmt.Sprintf("pandora:ds:battle:{%d}", matchID)
		raw, err := rdb.Get(ctx, key).Bytes()
		if err != nil {
			t.Fatal(err)
		}
		rec := &dsv1.BattleStorageRecord{}
		if err := proto.Unmarshal(raw, rec); err != nil {
			t.Fatal(err)
		}
		rec.State = stateAllocationAbort
		raw, _ = proto.Marshal(rec)
		if err := rdb.Set(ctx, key, raw, time.Hour).Err(); err != nil {
			t.Fatal(err)
		}
		_, werr := uc.waitBattleReady(ctx, matchID, "battle-abort-830", allocationID)
		if !errors.Is(werr, errBattleWaitOwnershipLost) {
			t.Fatalf("abort-pending wait err=%v", werr)
		}
		if allocator.releases.Load() != 0 {
			t.Fatalf("abort-pending waiter triggered release=%d", allocator.releases.Load())
		}
	})
	t.Run("quarantined auth with warming projection", func(t *testing.T) {
		allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
		uc, repo, mr := newUsecaseWithAlloc(t, allocator)
		uc.cfg.ReadyWaitTimeout = config.Duration(30 * time.Second)
		authRepo, rdb := enableModelBForTest(t, uc, mr)
		const matchID = uint64(831)
		const allocationID = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0831"
		seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocationID,
			"battle-quar-831", "uid-quar-831", time.Now().UnixMilli())
		key := fmt.Sprintf("pandora:ds:auth:{%d}", matchID)
		raw, err := rdb.Get(ctx, key).Bytes()
		if err != nil {
			t.Fatal(err)
		}
		rec := &dsv1.BattleDSAuthStorageRecord{}
		if err := proto.Unmarshal(raw, rec); err != nil {
			t.Fatal(err)
		}
		rec.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_QUARANTINED
		raw, _ = proto.Marshal(rec)
		if err := rdb.Set(ctx, key, raw, time.Hour).Err(); err != nil {
			t.Fatal(err)
		}
		_, werr := uc.waitBattleReady(ctx, matchID, "battle-quar-831", allocationID)
		if !errors.Is(werr, errBattleWaitOwnershipLost) {
			t.Fatalf("quarantined-auth wait err=%v", werr)
		}
		if allocator.releases.Load() != 0 {
			t.Fatalf("quarantined waiter triggered release=%d", allocator.releases.Load())
		}
	})
}

// 缺 auth 的 provisioning 宽限三态(复审 P1-3):宽限内继续等、宽限内恢复则成功、
// 超界携带所有权哨兵失败。
func TestWaitBattleReadyMissingAuthGraceLifecycle(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(30 * time.Second)
	uc.cfg.HeartbeatTimeout = config.Duration(300 * time.Millisecond) // 宽限=300ms,测试可控
	authRepo, rdb := enableModelBForTest(t, uc, mr)

	t.Run("in grace then recovered", func(t *testing.T) {
		const matchID = uint64(832)
		const allocationID = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0832"
		seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocationID,
			"battle-mag-832", "uid-mag-832", time.Now().UnixMilli())
		authKey := fmt.Sprintf("pandora:ds:auth:{%d}", matchID)
		savedAuth, err := rdb.Get(ctx, authKey).Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if err := rdb.Del(ctx, authKey).Err(); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, werr := uc.waitBattleReady(ctx, matchID, "battle-mag-832", allocationID)
			done <- werr
		}()
		// 宽限内:不得提前失败。
		select {
		case werr := <-done:
			t.Fatalf("waiter exited inside provisioning grace: %v", werr)
		case <-time.After(100 * time.Millisecond):
		}
		// 宽限内恢复:auth 回填并激活 → waiter 正常拿到 ready。
		if err := rdb.Set(ctx, authKey, savedAuth, time.Hour).Err(); err != nil {
			t.Fatal(err)
		}
		activateBackdatedModelBBattle(t, authRepo, rdb, mr, matchID, "battle-mag-832", "uid-mag-832", 0)
		select {
		case werr := <-done:
			if werr != nil {
				t.Fatalf("recovered provisioning must succeed, err=%v", werr)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("waiter did not observe recovery")
		}
	})
	t.Run("grace exceeded fails with ownership sentinel", func(t *testing.T) {
		const matchID = uint64(833)
		const allocationID = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0833"
		seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocationID,
			"battle-mag-833", "uid-mag-833", time.Now().UnixMilli())
		if err := rdb.Del(ctx, fmt.Sprintf("pandora:ds:auth:{%d}", matchID)).Err(); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		_, werr := uc.waitBattleReady(ctx, matchID, "battle-mag-833", allocationID)
		elapsed := time.Since(start)
		if !errors.Is(werr, errBattleWaitOwnershipLost) {
			t.Fatalf("grace-exceeded wait err=%v", werr)
		}
		if elapsed < 250*time.Millisecond || elapsed > 2*time.Second {
			t.Fatalf("grace bound not respected: elapsed=%v (want ~300ms)", elapsed)
		}
		if allocator.releases.Load() != 0 {
			t.Fatalf("grace-exceeded waiter triggered release=%d", allocator.releases.Load())
		}
	})
}

// 两阶段激活端到端(第三 P0):首拍不得放行 ds_addr(空 ACK、waiter 继续等),
// 跨度满足后第 3 拍原子提升,waiter 才拿到 ready。
func TestBattleModelBReadyRequiresStabilizedActivation(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(30 * time.Second)
	uc.cfg.ActivationStabilityBeats = 3
	uc.cfg.ActivationStabilitySpan = config.Duration(250 * time.Millisecond)
	authRepo, _ := enableModelBForTest(t, uc, mr)
	const matchID = uint64(834)
	const allocationID = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0834"
	seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocationID,
		"battle-stab-834", "uid-stab-834", time.Now().UnixMilli())
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil {
		t.Fatal(err)
	}
	id := data.BattleCredentialIdentity{
		PodName: "battle-stab-834", InstanceUID: "uid-stab-834",
		InstanceEpoch: snapshot.Auth.GetInstanceEpoch(), Gen: snapshot.Auth.GetPending().GetGen(),
		JTI: snapshot.Auth.GetPending().GetJti(), ExpMs: snapshot.Auth.GetPending().GetExpMs(),
		Kid: snapshot.Auth.GetPending().GetKid(), TokenSHA256: snapshot.Auth.GetPending().GetTokenSha256(),
		WriterEpoch: snapshot.Auth.GetPending().GetWriterEpoch(),
	}

	done := make(chan error, 1)
	go func() {
		_, werr := uc.waitBattleReady(ctx, matchID, "battle-stab-834", allocationID)
		done <- werr
	}()
	// 拍1:pending——空 ACK,不放行。
	hb, err := uc.HeartbeatAuthorized(ctx, matchID, id, 0, stateRunning, time.Now().UnixMilli())
	if err != nil || hb.AcceptedTokenGen != 0 || hb.AcceptedTokenJTI != "" {
		t.Fatalf("beat1 must return empty ack: hb=%+v err=%v", hb, err)
	}
	select {
	case werr := <-done:
		t.Fatalf("waiter released on first beat: %v", werr)
	case <-time.After(100 * time.Millisecond):
	}
	// 拍2(+150ms,跨度不足)仍 pending;拍3(+300ms)满足 ≥3 拍且跨度 ≥250ms → 提升。
	if _, err := uc.HeartbeatAuthorized(ctx, matchID, id, 0, stateRunning, time.Now().UnixMilli()); err != nil {
		t.Fatalf("beat2: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	hb3, err := uc.HeartbeatAuthorized(ctx, matchID, id, 0, stateRunning, time.Now().UnixMilli())
	if err != nil || hb3.AcceptedTokenGen != id.Gen || hb3.AcceptedTokenJTI != id.JTI {
		t.Fatalf("beat3 must promote and ack: hb=%+v err=%v", hb3, err)
	}
	select {
	case werr := <-done:
		if werr != nil {
			t.Fatalf("waiter after stabilized activation err=%v", werr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("waiter did not observe stabilized activation")
	}
}

// probe 退避绑定分配身份:同 match_id 的新分配(新 allocation_id)不继承旧退避,
// 且身份变化即清掉旧条目。
func TestWarmingProbeDeferralBoundToAllocation(t *testing.T) {
	uc, _ := newUsecase(t)
	now := time.Now()
	uc.noteSweepDeferral(context.Background(), 820, warmingProbeDeferPrefix+"alloc-A", now)
	if !uc.sweepDeferralActive(820, warmingProbeDeferPrefix+"alloc-A", now) {
		t.Fatal("probe deferral for A not active")
	}
	if uc.sweepDeferralActive(820, warmingProbeDeferPrefix+"alloc-B", now) {
		t.Fatal("replacement allocation B inherited A's probe deferral")
	}
	if uc.sweepDeferralActive(820, warmingProbeDeferPrefix+"alloc-A", now) {
		t.Fatal("stale deferral for A survived identity change")
	}
}

// waitBattleReady 对已被 sweep 判弃的分配必须立即失败让 matchmaker 重试,
// 不得白等完整 ready_wait(判死加速链的最后一环)。
func TestWaitBattleReadyFailsFastOnReclaimedBattle(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(30 * time.Second) // 远大于 fail-fast 断言窗口
	authRepo, _ := enableModelBForTest(t, uc, mr)
	const matchID = uint64(816)
	const allocationID = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0816"
	seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocationID,
		"battle-cold-816", "uid-cold-816", time.Now().UnixMilli())
	nowMs := time.Now().UnixMilli()
	if result, err := authRepo.AbandonIfStale(ctx, matchID, data.BattleStaleCutoffs{
		ActiveHeartbeatMs: nowMs, WarmingHeartbeatMs: nowMs,
	}, time.Hour, time.Hour); err != nil || !result.Abandoned {
		t.Fatalf("seed abandon=%+v err=%v", result, err)
	}

	start := time.Now()
	_, err := uc.waitBattleReady(ctx, matchID, "battle-cold-816", allocationID)
	if errcode.As(err) != errcode.ErrDSAllocationFailed || errors.Is(err, errReadyWaitTimeout) {
		t.Fatalf("reclaimed battle wait err=%v code=%v", err, errcode.As(err))
	}
	if !errors.Is(err, errBattleWaitOwnershipLost) {
		t.Fatalf("reclaimed wait must carry ownership-lost sentinel, err=%v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("reclaimed battle wait did not fail fast: %v", elapsed)
	}
}

// activateBackdatedModelBBattle 把 seedWarmingModelBBattle 的分配激活为 running,
// 并把权威 auth+battle 心跳与派生 ZSET 一起回拨 age,模拟已激活实例失联。
func activateBackdatedModelBBattle(
	t *testing.T,
	authRepo *data.RedisBattleAuthRepo,
	rdb *redis.Client,
	mr *miniredis.Miniredis,
	matchID uint64,
	podName, uid string,
	age time.Duration,
) {
	t.Helper()
	ctx := context.Background()
	snapshot, err := authRepo.ReadAuthority(ctx, matchID)
	if err != nil {
		t.Fatal(err)
	}
	id := data.BattleCredentialIdentity{
		PodName: podName, InstanceUID: uid,
		InstanceEpoch: snapshot.Auth.GetInstanceEpoch(), Gen: snapshot.Auth.GetPending().GetGen(),
		JTI: snapshot.Auth.GetPending().GetJti(), ExpMs: snapshot.Auth.GetPending().GetExpMs(),
		Kid: snapshot.Auth.GetPending().GetKid(), TokenSHA256: snapshot.Auth.GetPending().GetTokenSha256(),
		WriterEpoch: snapshot.Auth.GetPending().GetWriterEpoch(),
	}
	if _, err := authRepo.ActivateHeartbeat(ctx, matchID, id, data.BattleHeartbeatInput{
		PlayerCount: 2, State: stateRunning, AuthTTL: time.Hour, BattleTTL: time.Hour,
	}); err != nil {
		t.Fatalf("activate: %v", err)
	}
	staleMs := time.Now().Add(-age).UnixMilli()
	authKey := fmt.Sprintf("pandora:ds:auth:{%d}", matchID)
	battleKey := fmt.Sprintf("pandora:ds:battle:{%d}", matchID)
	authBytes, err := rdb.Get(ctx, authKey).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	authRec := &dsv1.BattleDSAuthStorageRecord{}
	if err := proto.Unmarshal(authBytes, authRec); err != nil {
		t.Fatal(err)
	}
	battleBytes, err := rdb.Get(ctx, battleKey).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	battleRec := &dsv1.BattleStorageRecord{}
	if err := proto.Unmarshal(battleBytes, battleRec); err != nil {
		t.Fatal(err)
	}
	authRec.LastActiveHeartbeatMs, battleRec.LastHeartbeatMs = staleMs, staleMs
	authBytes, _ = proto.Marshal(authRec)
	battleBytes, _ = proto.Marshal(battleRec)
	if err := rdb.Set(ctx, authKey, authBytes, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, battleKey, battleBytes, time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := mr.ZAdd("pandora:ds:active", float64(staleMs), fmt.Sprint(matchID)); err != nil {
		t.Fatal(err)
	}
}

// ACTIVE/running 失联只认 15s 业务心跳阈值:失联 30s(远未到 120s warming 宽限)
// 必须照常终止、Release 并投递 abandoned 补偿 —— 冷加载宽限不得延后崩溃补偿(不变量 §4)。
func TestBattleModelBSweepActiveIgnoresWarmingGrace(t *testing.T) {
	ctx := context.Background()
	allocator := &authoritativeTestAllocator{delivered: make(chan map[string]string, 1)}
	uc, repo, mr := newUsecaseWithAlloc(t, allocator)
	uc.cfg.ReadyWaitTimeout = config.Duration(120 * time.Second)
	authRepo, rdb := enableModelBForTest(t, uc, mr)
	life := &mockLifecycle{}
	uc.SetLifecyclePusher(life)
	const matchID = uint64(813)
	const allocationID = "5d0a7c3e-16f2-4c58-8f4e-3a1f5b9d0813"
	seedWarmingModelBBattle(t, repo, authRepo, mr, matchID, allocationID,
		"battle-cold-813", "uid-cold-813", time.Now().UnixMilli())
	activateBackdatedModelBBattle(t, authRepo, rdb, mr, matchID, "battle-cold-813", "uid-cold-813", 30*time.Second)

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	b, found, err := repo.GetBattle(ctx, matchID)
	if err != nil || !found || b.GetState() != stateAbandoned {
		t.Fatalf("running battle not abandoned at 30s silence: found=%v state=%q err=%v",
			found, b.GetState(), err)
	}
	if allocator.releases.Load() != 1 || life.calls != 1 {
		t.Fatalf("active reclaim skipped release/compensation: releases=%d lifecycle=%d",
			allocator.releases.Load(), life.calls)
	}
}

// TestAllocateBattleReadyWaitTimeout:没有 DS 心跳 → 等待超时 → 回收 pod + 删镜像 + 返回分配失败
// (绝不把 ds_addr 回给 matchmaker)。
func TestAllocateBattleReadyWaitTimeout(t *testing.T) {
	ctx := context.Background()
	alloc := &countingAllocator{inner: NewMockGameServerAllocator(testCfg())}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)

	_, err := uc.AllocateBattle(ctx, 7, []uint64{10, 20}, 1, "5v5_ranked")
	if err == nil {
		t.Fatal("expected allocation failure on ready wait timeout")
	}
	if errcode.As(err) != errcode.ErrDSAllocationFailed {
		t.Fatalf("err code = %v, want ErrDSAllocationFailed", errcode.As(err))
	}
	if _, found, _ := repo.GetBattle(ctx, 7); found {
		t.Fatal("battle record must be deleted after ready wait timeout")
	}
	if alloc.releases != 1 {
		t.Fatalf("pod released %d times, want exactly 1", alloc.releases)
	}
}

// TestAllocateBattleRejectsMismatchedPodHeartbeat:证明 match_id ↔ pod 绑定不可绕过。
// 一个携带正确 match_id 但 pod 名不符(旧 DS / 孤儿 DS / 抢跑的别局 DS)的心跳,
// 必须被拒(返回 commandStop)、不得写回镜像、更不得打开 AllocateBattle 的就绪门控:
// 最终 AllocateBattle 仍因等不到本局 pod 的真实心跳而超时失败,绝不回 ds_addr。
func TestAllocateBattleRejectsMismatchedPodHeartbeat(t *testing.T) {
	ctx := context.Background()
	uc, repo := newUsecase(t)

	type out struct {
		res *AllocateResult
		err error
	}
	done := make(chan out, 1)
	go func() {
		res, err := uc.AllocateBattle(ctx, 7, []uint64{10, 20}, 1, "5v5_ranked")
		done <- out{res, err}
	}()

	// 等 warming 镜像出现(本局 pod = pandora-battle-7),记录其分配时刻。
	deadline := time.Now().Add(3 * time.Second)
	var rec *dsv1.BattleStorageRecord
	for {
		b, found, err := repo.GetBattle(ctx, 7)
		if err == nil && found && b.DsPodName != "" {
			rec = b
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("warming record for match 7 never appeared")
		}
		time.Sleep(5 * time.Millisecond)
	}
	for time.Now().UnixMilli() <= rec.AllocatedAtMs {
		time.Sleep(time.Millisecond)
	}

	// 用「错误的 pod 名」上报 running 心跳:必须被门控拒绝并令其停机。
	hbRes, err := uc.Heartbeat(ctx, 7, "pandora-battle-999", 2, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("mismatched heartbeat returned hard error: %v", err)
	}
	if hbRes.Command != commandStop {
		t.Fatalf("mismatched-pod heartbeat command = %q, want stop", hbRes.Command)
	}

	// 镜像不得被异局心跳污染:仍停在 warming、未刷新到分配时刻之后。
	got, _, _ := repo.GetBattle(ctx, 7)
	if got.State != stateWarming {
		t.Fatalf("state = %q, want warming (foreign heartbeat must not flip state)", got.State)
	}
	if got.LastHeartbeatMs > got.AllocatedAtMs {
		t.Fatalf("LastHeartbeatMs %d must stay <= AllocatedAtMs %d (foreign heartbeat must not refresh)",
			got.LastHeartbeatMs, got.AllocatedAtMs)
	}

	// 门控不得放行:AllocateBattle 仍超时失败,绝不返回 ds_addr。
	r := <-done
	if r.err == nil {
		t.Fatalf("AllocateBattle must fail when only a mismatched pod heartbeat arrived, got addr=%q", r.res.DSAddr)
	}
	if errcode.As(r.err) != errcode.ErrDSAllocationFailed {
		t.Fatalf("err code = %v, want ErrDSAllocationFailed", errcode.As(r.err))
	}
	if _, found, _ := repo.GetBattle(ctx, 7); found {
		t.Fatal("battle record must be cleaned up after ready wait timeout")
	}
}

func TestAllocateBattleIdempotent(t *testing.T) {
	ctx := context.Background()
	uc, repo := newUsecase(t)

	first := allocateReady(t, uc, repo, 7, []uint64{10, 20}, 1, "5v5_ranked")
	// 幂等:已 ready/running 且有有效心跳 → 第二次直接返回已分配地址(不再等心跳)
	second, err := uc.AllocateBattle(ctx, 7, []uint64{10, 20}, 1, "5v5_ranked")
	if err != nil {
		t.Fatalf("second allocate: %v", err)
	}
	if first.DSAddr != second.DSAddr || first.AllocatedAtMs != second.AllocatedAtMs {
		t.Fatalf("idempotent mismatch: %+v vs %+v", first, second)
	}
}

func TestReleaseBattleIdempotent(t *testing.T) {
	ctx := context.Background()
	uc, repo := newUsecase(t)

	allocateReady(t, uc, repo, 7, []uint64{10}, 1, "5v5_ranked")
	if err := uc.ReleaseBattle(ctx, 7, "completed"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, found, _ := repo.GetBattle(ctx, 7); found {
		t.Fatal("battle 7 should be gone after release")
	}
	// 再次释放(已不存在)应幂等成功
	if err := uc.ReleaseBattle(ctx, 7, "completed"); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
}

func TestHeartbeatUpdatesState(t *testing.T) {
	ctx := context.Background()
	uc, repo := newUsecase(t)

	// allocateReady 已上报一次 running 心跳;再上报一次刷 player_count=8
	allocateReady(t, uc, repo, 7, []uint64{10, 20}, 1, "5v5_ranked")
	res, err := uc.Heartbeat(ctx, 7, "pandora-battle-7", 8, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if res.Command != "" {
		t.Fatalf("command = %q, want empty", res.Command)
	}
	got, _, _ := repo.GetBattle(ctx, 7)
	if got.State != "running" || got.PlayerCount != 8 {
		t.Fatalf("after heartbeat: %+v", got)
	}
}

// TestHeartbeatPodMismatchRejected:镜像已绑定某 pod,另一个 pod(旧/孤儿 DS)上报 → 返回 stop
// 且不写回镜像(不污染新对局的 state/心跳/player_count)。
func TestHeartbeatPodMismatchRejected(t *testing.T) {
	ctx := context.Background()
	uc, repo := newUsecase(t)

	now := time.Now().UnixMilli()
	rec := &dsv1.BattleStorageRecord{
		MatchId: 7, DsPodName: "pandora-battle-7", DsAddr: "127.0.0.1:30007",
		State: stateWarming, AllocatedAtMs: now, LastHeartbeatMs: now, PlayerCount: 2,
	}
	if err := repo.CreateBattle(ctx, rec, 2*time.Hour); err != nil {
		t.Fatalf("create: %v", err)
	}
	res, err := uc.Heartbeat(ctx, 7, "pandora-battle-OLD", 9, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if res.Command != "stop" {
		t.Fatalf("command = %q, want stop", res.Command)
	}
	got, _, _ := repo.GetBattle(ctx, 7)
	if got.State != stateWarming || got.PlayerCount == 9 || got.LastHeartbeatMs != now {
		t.Fatalf("mismatched pod must not update record: %+v", got)
	}
}

func TestHeartbeatOrphanReturnsStop(t *testing.T) {
	ctx := context.Background()
	uc, _ := newUsecase(t)

	// 无对应镜像的孤儿 DS 上报心跳 → 应被告知 stop
	res, err := uc.Heartbeat(ctx, 999, "pandora-battle-999", 1, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if res.Command != "stop" {
		t.Fatalf("command = %q, want stop", res.Command)
	}
}

// recordingAllocator 记录 Release 的 podName 并经 channel 通知,供异步 kill 断言。
type recordingAllocator struct {
	inner    GameServerAllocator
	released chan string
}

func (r *recordingAllocator) Allocate(ctx context.Context, matchID uint64, mapID uint32, gameMode, releaseTrack string) (string, string, string, error) {
	return r.inner.Allocate(ctx, matchID, mapID, gameMode, releaseTrack)
}

func (r *recordingAllocator) Release(ctx context.Context, podName string) error {
	_ = r.inner.Release(ctx, podName)
	r.released <- podName
	return nil
}

// TestHeartbeatOrphanKillsStrandedDS:local 模式(killOrphanOnStop=true)下,orphan 心跳除了回 stop,
// 还必须主动 Release 幽灵 pod——UE DS 收 stop 不自杀,不主动 kill 会让它占端口污染下一局。
func TestHeartbeatOrphanKillsStrandedDS(t *testing.T) {
	ctx := context.Background()
	rec := &recordingAllocator{inner: NewMockGameServerAllocator(testCfg()), released: make(chan string, 1)}
	uc, _, _ := newUsecaseWithAlloc(t, rec)
	uc.SetKillOrphanOnStop(true)

	res, err := uc.Heartbeat(ctx, 999, "pandora-battle-999", 1, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if res.Command != commandStop {
		t.Fatalf("command = %q, want stop", res.Command)
	}
	select {
	case pod := <-rec.released:
		if pod != "pandora-battle-999" {
			t.Fatalf("released pod = %q, want pandora-battle-999", pod)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected stranded DS to be released on orphan stop")
	}
}

// TestHeartbeatPodMismatchKillsOldDS:local 模式下 pod 不匹配(旧 DS 上报)时,主动 kill 的是**上报方**
// (旧 DS 的 pod),不动镜像里绑定的新 pod。
func TestHeartbeatPodMismatchKillsOldDS(t *testing.T) {
	ctx := context.Background()
	rec := &recordingAllocator{inner: NewMockGameServerAllocator(testCfg()), released: make(chan string, 1)}
	uc, repo, _ := newUsecaseWithAlloc(t, rec)
	uc.SetKillOrphanOnStop(true)

	now := time.Now().UnixMilli()
	b := &dsv1.BattleStorageRecord{
		MatchId: 7, DsPodName: "pandora-battle-7", DsAddr: "127.0.0.1:30007",
		State: stateWarming, AllocatedAtMs: now, LastHeartbeatMs: now, PlayerCount: 2,
	}
	if err := repo.CreateBattle(ctx, b, 2*time.Hour); err != nil {
		t.Fatalf("create: %v", err)
	}
	res, err := uc.Heartbeat(ctx, 7, "pandora-battle-OLD", 9, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if res.Command != commandStop {
		t.Fatalf("command = %q, want stop", res.Command)
	}
	select {
	case pod := <-rec.released:
		if pod != "pandora-battle-OLD" {
			t.Fatalf("released pod = %q, want pandora-battle-OLD (the stale reporter)", pod)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected stale DS to be released on pod_mismatch stop")
	}
}

// TestHeartbeatOrphanNoKillWhenDisabled:killOrphanOnStop 关闭(Agones/默认)时,orphan 心跳只回
// stop,不主动 Release——孤儿 pod 回收交 Agones 生命周期,避免 Redis 抖动误判 orphan 误删正常 pod。
func TestHeartbeatOrphanNoKillWhenDisabled(t *testing.T) {
	ctx := context.Background()
	rec := &recordingAllocator{inner: NewMockGameServerAllocator(testCfg()), released: make(chan string, 1)}
	uc, _, _ := newUsecaseWithAlloc(t, rec)
	// 不调 SetKillOrphanOnStop → 默认 false

	if _, err := uc.Heartbeat(ctx, 999, "pandora-battle-999", 1, "running", time.Now().UnixMilli()); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	select {
	case pod := <-rec.released:
		t.Fatalf("must NOT release when killOrphanOnStop disabled, got %q", pod)
	case <-time.After(200 * time.Millisecond):
		// 期望:无 Release 发生
	}
}

// TestHeartbeatOnAbandonedReturnsStopNoRefresh:abandoned 对局的 DS 若继续心跳(pod release
// 失败/延迟终止),Heartbeat 必须返回 stop 且**不写回记录**——不刷新 LastHeartbeatMs/TTL,也不
// 重新 ZAdd active。否则补偿重试会被推迟、BattleTTL 上界被不断刷新(W4 ⑧ Codex 复审 P1)。
func TestHeartbeatOnAbandonedReturnsStopNoRefresh(t *testing.T) {
	ctx := context.Background()
	alloc := &countingAllocator{inner: NewMockGameServerAllocator(testCfg())}
	uc, repo, mr := newUsecaseWithAlloc(t, alloc)
	life := &mockLifecycle{failFirst: 1000} // 始终投递失败,abandoned 对局保留在 active 重试
	uc.SetLifecyclePusher(life)

	allocateReady(t, uc, repo, 7, []uint64{10, 20}, 1, "5v5_ranked")
	backdate(t, repo, 7) // LastHeartbeatMs=1

	// sweep #1:投递失败 → 标记 abandoned、回收 pod、保留在 active 待重试
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep1: %v", err)
	}

	// 把 TTL 钉到已知小值,便于检测心跳是否误刷新
	key := "pandora:ds:battle:{7}"
	mr.SetTTL(key, 90*time.Second)
	ttlBefore := mr.TTL(key)
	if ttlBefore <= 0 {
		t.Fatalf("precondition: ttl not pinned, got %v", ttlBefore)
	}

	// abandoned 后 DS 继续心跳:必须返回 stop,且不写回记录
	res, err := uc.Heartbeat(ctx, 7, "pandora-battle-7", 9, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if res.Command != "stop" {
		t.Fatalf("command = %q, want stop", res.Command)
	}

	// 记录未被写回:LastHeartbeatMs 仍是回拨值 1(active score = LastHeartbeatMs 也未刷新),
	// state 仍 abandoned,PlayerCount 未被改成 9
	got, _, _ := repo.GetBattle(ctx, 7)
	if got.LastHeartbeatMs != 1 {
		t.Fatalf("LastHeartbeatMs = %d, want 1 (terminal heartbeat must not write back)", got.LastHeartbeatMs)
	}
	if got.State != "abandoned" {
		t.Fatalf("state = %q, want abandoned", got.State)
	}
	if got.PlayerCount == 9 {
		t.Fatalf("PlayerCount refreshed to 9, terminal record must not be written")
	}

	// TTL 未被心跳刷新(仍 ≤ 钉住的 90s)
	if ttlAfter := mr.TTL(key); ttlAfter > ttlBefore {
		t.Fatalf("TTL refreshed by terminal heartbeat: before=%v after=%v", ttlBefore, ttlAfter)
	}

	// active score 仍是陈旧值 → 下一轮 sweep 仍会命中重试
	stale, _ := repo.RangeStaleBattles(ctx, 1000)
	if len(stale) != 1 || stale[0] != 7 {
		t.Fatalf("stale = %v, want [7] (active score not refreshed, sweep still retries)", stale)
	}

	// 下一轮 sweep 仍重试投递(补偿没被心跳推迟)
	callsBefore := life.calls
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep2: %v", err)
	}
	if life.calls != callsBefore+1 {
		t.Fatalf("sweep2 publish calls = %d, want %d (retry continues)", life.calls, callsBefore+1)
	}
	if alloc.releases != 1 {
		t.Fatalf("pod released %d times, want exactly 1 (no re-release)", alloc.releases)
	}
}

func TestListBattles(t *testing.T) {
	ctx := context.Background()
	uc, repo := newUsecase(t)

	allocateReady(t, uc, repo, 1, []uint64{10}, 1, "5v5_ranked")
	allocateReady(t, uc, repo, 2, []uint64{20}, 1, "5v5_ranked")

	all, err := uc.ListBattles(ctx, "")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("list all = %d, want 2", len(all))
	}

	// 状态过滤:等到 ready 心跳后两局都是 running,ready 无
	running, _ := uc.ListBattles(ctx, "running")
	if len(running) != 2 {
		t.Fatalf("list running = %d, want 2", len(running))
	}
	ready, _ := uc.ListBattles(ctx, "ready")
	if len(ready) != 0 {
		t.Fatalf("list ready = %d, want 0", len(ready))
	}
}

func TestSweepMarksAbandoned(t *testing.T) {
	ctx := context.Background()
	uc, repo := newUsecase(t)

	allocateReady(t, uc, repo, 7, []uint64{10}, 1, "5v5_ranked")
	// 手动把 last_heartbeat_ms 回拨到远古,模拟心跳超时
	if err := repo.UpdateBattleWithLock(ctx, 7, 3, func(b *dsv1.BattleStorageRecord) error {
		b.LastHeartbeatMs = 1
		return nil
	}, 2*time.Hour); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	got, found, _ := repo.GetBattle(ctx, 7)
	if !found {
		t.Fatal("battle should still exist (terminal record retained)")
	}
	if got.State != "abandoned" {
		t.Fatalf("state = %q, want abandoned", got.State)
	}
	// 已移出 active,不再被扫描
	ids, _ := repo.RangeActiveBattles(ctx)
	if len(ids) != 0 {
		t.Fatalf("active should be empty after sweep, got %v", ids)
	}
}

func TestSweepMissingRequiredLifecyclePublisherRetainsRecoveryOutbox(t *testing.T) {
	ctx := context.Background()
	alloc := &countingAllocator{inner: NewMockGameServerAllocator(testCfg())}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)
	uc.SetLifecyclePusherRequired(true)

	allocateReady(t, uc, repo, 70, []uint64{10, 20}, 1, "5v5_ranked")
	backdate(t, repo, 70)
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	rec, found, err := repo.GetBattle(ctx, 70)
	if err != nil || !found || rec.GetState() != stateAbandoned {
		t.Fatalf("abandoned record missing: found=%v record=%+v err=%v", found, rec, err)
	}
	ids, err := repo.RangeActiveBattles(ctx)
	if err != nil || len(ids) != 1 || ids[0] != 70 {
		t.Fatalf("missing publisher silently completed recovery: active=%v err=%v", ids, err)
	}
	if alloc.releases != 1 {
		t.Fatalf("initial abandoned transition should release once, releases=%d", alloc.releases)
	}
}

// TestSweepEndedReclaimsLocalDS:local 模式(killOrphanOnStop=true)下,正常结算(ended)且失联的 DS
// 被扫到时,除了移出 active 还必须主动 Release——battle_result 不再直杀,DS 发完 ended 心跳即停心跳
// (无第二跳触发终态 kill),local Agones Shutdown 又是 no-op 不自退,不在此兜底 taskkill 就会幽灵占端口。
func TestSweepEndedReclaimsLocalDS(t *testing.T) {
	ctx := context.Background()
	rec := &recordingAllocator{inner: NewMockGameServerAllocator(testCfg()), released: make(chan string, 1)}
	uc, repo, _ := newUsecaseWithAlloc(t, rec)
	uc.SetKillOrphanOnStop(true)

	res := allocateReady(t, uc, repo, 8, []uint64{11, 22}, 1, "5v5_ranked")
	// 上报一次 ended 心跳:state → ended(首跳不 kill),仍留在 active 待扫描收尾。
	if _, err := uc.Heartbeat(ctx, 8, res.DSPodName, 0, "ended", time.Now().UnixMilli()); err != nil {
		t.Fatalf("ended heartbeat: %v", err)
	}
	backdate(t, repo, 8) // 心跳超时,进入 sweep 扫描窗口

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	select {
	case pod := <-rec.released:
		if pod != res.DSPodName {
			t.Fatalf("released pod = %q, want %q", pod, res.DSPodName)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("expected ended DS to be reclaimed on sweep (local ghost-process leak)")
	}
	if ids, _ := repo.RangeActiveBattles(ctx); len(ids) != 0 {
		t.Fatalf("active should be empty after ended sweep, got %v", ids)
	}
}

// TestSweepEndedNoReclaimOnAgones:Agones 模式(killOrphanOnStop=false)下,ended 对局扫到只移出
// active,绝不主动 Release——DS 已自身 Agones Shutdown,pod 回收交 Fleet 生命周期,后端不越权 kill。
func TestSweepEndedNoReclaimOnAgones(t *testing.T) {
	ctx := context.Background()
	rec := &recordingAllocator{inner: NewMockGameServerAllocator(testCfg()), released: make(chan string, 1)}
	uc, repo, _ := newUsecaseWithAlloc(t, rec)
	// 不调 SetKillOrphanOnStop → 默认 false(Agones 模式)

	res := allocateReady(t, uc, repo, 9, []uint64{33, 44}, 1, "5v5_ranked")
	if _, err := uc.Heartbeat(ctx, 9, res.DSPodName, 0, "ended", time.Now().UnixMilli()); err != nil {
		t.Fatalf("ended heartbeat: %v", err)
	}
	backdate(t, repo, 9)

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	select {
	case pod := <-rec.released:
		t.Fatalf("must NOT release ended DS on Agones (killOrphanOnStop off), got %q", pod)
	case <-time.After(200 * time.Millisecond):
		// 期望:无 Release 发生
	}
	if ids, _ := repo.RangeActiveBattles(ctx); len(ids) != 0 {
		t.Fatalf("active should be empty after ended sweep, got %v", ids)
	}
}

// TestSweepDeliversAbandonedFirstTry:配置 kafka 且首次投递成功 → 发 1 次事件、移出 active、回收 1 次。
func TestSweepDeliversAbandonedFirstTry(t *testing.T) {
	ctx := context.Background()
	alloc := &countingAllocator{inner: NewMockGameServerAllocator(testCfg())}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)
	life := &mockLifecycle{}
	uc.SetLifecyclePusher(life)

	allocateReady(t, uc, repo, 5, []uint64{1, 2}, 1, "5v5_ranked")
	backdate(t, repo, 5)

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if ids, _ := repo.RangeActiveBattles(ctx); len(ids) != 0 {
		t.Fatalf("active = %v, want empty after delivery", ids)
	}
	if life.calls != 1 || len(life.delivered) != 1 || life.delivered[0] != 5 {
		t.Fatalf("publish calls=%d delivered=%v, want 1 / [5]", life.calls, life.delivered)
	}
	if alloc.releases != 1 {
		t.Fatalf("releases=%d, want 1", alloc.releases)
	}
}

// TestSweepRechecksAuthorityBeforeAbandon 模拟权威 record 心跳写成功、跨 slot ZADD 失败留下
// 旧 score。sweep 必须先重读 record 修索引，绝不能误标 abandoned/Release/发补偿。
func TestSweepRechecksAuthorityBeforeAbandon(t *testing.T) {
	ctx := context.Background()
	alloc := &countingAllocator{inner: NewMockGameServerAllocator(testCfg())}
	uc, repo, mr := newUsecaseWithAlloc(t, alloc)
	life := &mockLifecycle{}
	uc.SetLifecyclePusher(life)
	allocateReady(t, uc, repo, 71, []uint64{1, 2}, 1, "ranked")

	if err := repo.UpdateBattleWithLock(ctx, 71, 3, func(b *dsv1.BattleStorageRecord) error {
		b.LastHeartbeatMs = time.Now().UnixMilli()
		return nil
	}, 2*time.Hour); err != nil {
		t.Fatalf("refresh record: %v", err)
	}
	if _, err := mr.ZAdd("pandora:ds:active", 1, "71"); err != nil {
		t.Fatalf("force stale derived index: %v", err)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	got, found, err := repo.GetBattle(ctx, 71)
	if err != nil || !found || got.State != stateRunning {
		t.Fatalf("fresh authority was abandoned: found=%v rec=%+v err=%v", found, got, err)
	}
	if alloc.releases != 0 || life.calls != 0 {
		t.Fatalf("stale index caused side effects: releases=%d lifecycle=%d", alloc.releases, life.calls)
	}
	stale, err := repo.RangeStaleBattles(ctx, time.Now().Add(-time.Second).UnixMilli())
	if err != nil || len(stale) != 0 {
		t.Fatalf("derived index was not repaired: stale=%v err=%v", stale, err)
	}
}

// TestSweepReliableCompensation_RetryUntilDelivered:Kafka 前两轮不可用 → abandoned 对局保留在
// active 重试,第三轮投递成功才移出;pod 只在首次转 abandoned 回收一次(不变量 §4 可靠补偿)。
func TestSweepReliableCompensation_RetryUntilDelivered(t *testing.T) {
	ctx := context.Background()
	alloc := &countingAllocator{inner: NewMockGameServerAllocator(testCfg())}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)
	life := &mockLifecycle{failFirst: 2} // 前两轮投递失败,第三轮成功
	uc.SetLifecyclePusher(life)

	allocateReady(t, uc, repo, 7, []uint64{10, 20}, 1, "5v5_ranked")
	backdate(t, repo, 7)

	// sweep #1:投递失败 → 标记 abandoned、回收 pod、保留在 active 待重试
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep1: %v", err)
	}
	if ids, _ := repo.RangeActiveBattles(ctx); len(ids) != 1 {
		t.Fatalf("after sweep1 active = %v, want still 1 (retry pending)", ids)
	}
	if got, _, _ := repo.GetBattle(ctx, 7); got.State != "abandoned" {
		t.Fatalf("after sweep1 state = %q, want abandoned", got.State)
	}

	// sweep #2:仍失败 → 仍保留 active,pod 不重复回收
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep2: %v", err)
	}
	if ids, _ := repo.RangeActiveBattles(ctx); len(ids) != 1 {
		t.Fatalf("after sweep2 active = %v, want still 1", ids)
	}

	// sweep #3:投递成功 → 移出 active
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep3: %v", err)
	}
	if ids, _ := repo.RangeActiveBattles(ctx); len(ids) != 0 {
		t.Fatalf("after sweep3 active = %v, want empty (delivered)", ids)
	}

	if alloc.releases != 1 {
		t.Fatalf("pod released %d times, want exactly 1 (no re-release during retry)", alloc.releases)
	}
	if life.calls != 3 {
		t.Fatalf("publish called %d times, want 3 (2 fail + 1 success)", life.calls)
	}
	if len(life.delivered) != 1 || life.delivered[0] != 7 {
		t.Fatalf("delivered = %v, want [7]", life.delivered)
	}
}

// ── 空场超时兜底(2026-07-06,全员掉线/从未连入的 DS 防空转)──────────────────────

// backdateEmptySince 把 EmptySinceMs 回拨到远古,模拟空场已持续超过 EmptyBattleTimeout。
func backdateEmptySince(t *testing.T, repo *data.RedisBattleRepo, matchID uint64) {
	t.Helper()
	if err := repo.UpdateBattleWithLock(context.Background(), matchID, 3, func(b *dsv1.BattleStorageRecord) error {
		b.EmptySinceMs = 1
		return nil
	}, 2*time.Hour); err != nil {
		t.Fatalf("backdate empty_since: %v", err)
	}
}

// TestHeartbeatEmptyBattleTimeout:running 对局 player_count==0 持续超 EmptyBattleTimeout →
// 心跳内判 abandoned + 回 stop + 回收 pod + 投递补偿事件 + 移出 active(空场兜底)。
func TestHeartbeatEmptyBattleTimeout(t *testing.T) {
	ctx := context.Background()
	alloc := &countingAllocator{inner: NewMockGameServerAllocator(testCfg())}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)
	life := &mockLifecycle{}
	uc.SetLifecyclePusher(life)

	res := allocateReady(t, uc, repo, 7, []uint64{10, 20}, 1, "5v5_ranked")

	// 第一跳空场:只盖 EmptySinceMs 起计时,不判弃
	hb, err := uc.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if hb.Command != commandNone {
		t.Fatalf("command = %q, want none (first empty beat only starts timer)", hb.Command)
	}
	got, _, _ := repo.GetBattle(ctx, 7)
	if got.EmptySinceMs == 0 {
		t.Fatal("EmptySinceMs should be set on first empty heartbeat")
	}
	if got.State != stateRunning {
		t.Fatalf("state = %q, want still running", got.State)
	}

	// 空场持续超时(回拨 EmptySinceMs)→ 下一跳判 abandoned
	backdateEmptySince(t, repo, 7)
	hb2, err := uc.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat2: %v", err)
	}
	if hb2.Command != commandStop {
		t.Fatalf("command = %q, want stop", hb2.Command)
	}
	got2, _, _ := repo.GetBattle(ctx, 7)
	if got2.State != stateAbandoned {
		t.Fatalf("state = %q, want abandoned", got2.State)
	}
	if alloc.releases != 1 {
		t.Fatalf("releases = %d, want 1", alloc.releases)
	}
	if len(life.delivered) != 1 || life.delivered[0] != 7 {
		t.Fatalf("delivered = %v, want [7] (段位回滚补偿事件)", life.delivered)
	}
	if ids, _ := repo.RangeActiveBattles(ctx); len(ids) != 0 {
		t.Fatalf("active = %v, want empty after empty-timeout abandon", ids)
	}
}

// TestHeartbeatEmptyResetWhenPlayersReturn:空场计时后有人重连回来 → EmptySinceMs 清零,不判弃
// (全员短暂掉线正在重连的局绝不能被误杀,阈值语义是「持续空场」)。
func TestHeartbeatEmptyResetWhenPlayersReturn(t *testing.T) {
	ctx := context.Background()
	uc, repo := newUsecase(t)

	res := allocateReady(t, uc, repo, 7, []uint64{10, 20}, 1, "5v5_ranked")
	if _, err := uc.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli()); err != nil {
		t.Fatalf("empty heartbeat: %v", err)
	}
	if got, _, _ := repo.GetBattle(ctx, 7); got.EmptySinceMs == 0 {
		t.Fatal("EmptySinceMs should be set")
	}
	// 玩家重连回来 → 清零
	if _, err := uc.Heartbeat(ctx, 7, res.DSPodName, 2, "running", time.Now().UnixMilli()); err != nil {
		t.Fatalf("rejoin heartbeat: %v", err)
	}
	got, _, _ := repo.GetBattle(ctx, 7)
	if got.EmptySinceMs != 0 {
		t.Fatalf("EmptySinceMs = %d, want 0 after players return", got.EmptySinceMs)
	}
	if got.State != stateRunning {
		t.Fatalf("state = %q, want running", got.State)
	}
}

// TestHeartbeatEmptyTimeoutDisabled:EmptyBattleTimeout 配负值 → 空场只计时不判弃(显式禁用)。
func TestHeartbeatEmptyTimeoutDisabled(t *testing.T) {
	ctx := context.Background()
	uc, repo := newUsecase(t)
	cfg := testCfg()
	cfg.EmptyBattleTimeout = config.Duration(-1) // 显式禁用
	ucDisabled := NewAllocatorUsecase(repo, NewMockGameServerAllocator(cfg), cfg)

	res := allocateReady(t, uc, repo, 7, []uint64{10, 20}, 1, "5v5_ranked")
	if _, err := ucDisabled.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli()); err != nil {
		t.Fatalf("empty heartbeat: %v", err)
	}
	backdateEmptySince(t, repo, 7)
	hb, err := ucDisabled.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if hb.Command != commandNone {
		t.Fatalf("command = %q, want none (empty timeout disabled)", hb.Command)
	}
	if got, _, _ := repo.GetBattle(ctx, 7); got.State != stateRunning {
		t.Fatalf("state = %q, want running (disabled must not abandon)", got.State)
	}
}

// ── 双阈值空场回收(2026-08-07,anti-abuse-scene-entry.md §3.2.1)────────────────
//
// 语义:「从未连入」(EverHadPlayers=false)走短阈值 no_show_battle_timeout;
// 「有人连入过」走长阈值 empty_battle_timeout(必须远大于 ~30s 断线重连窗)。
// 这两条测试是本次改动的核心断言 —— 它们同时锁住「防刷」和「不误杀掉线玩家」两个方向。

// allocateReadyNoShow 造一局「DS 已就绪但一个玩家都没连进来」的 no-show 局:
// ready 心跳上报 player_count=0,因此 EverHadPlayers 保持 false。
func allocateReadyNoShow(t *testing.T, uc *AllocatorUsecase, repo *data.RedisBattleRepo,
	matchID uint64, playerIDs []uint64,
) *AllocateResult {
	t.Helper()
	type out struct {
		res *AllocateResult
		err error
	}
	done := make(chan out, 1)
	go func() {
		res, err := uc.AllocateBattle(context.Background(), matchID, playerIDs, 1, "pve_coop")
		done <- out{res, err}
	}()
	feedReadyHeartbeat(t, uc, repo, matchID, 0) // 关键:0 人上报 ⇒ EverHadPlayers 保持 false
	r := <-done
	if r.err != nil {
		t.Fatalf("allocate match %d: %v", matchID, r.err)
	}
	return r.res
}

// backdateEmptySinceBy 把 EmptySinceMs 精确回拨 d,用于卡在两档阈值之间做判别。
func backdateEmptySinceBy(t *testing.T, repo *data.RedisBattleRepo, matchID uint64, d time.Duration) {
	t.Helper()
	if err := repo.UpdateBattleWithLock(context.Background(), matchID, 3, func(b *dsv1.BattleStorageRecord) error {
		b.EmptySinceMs = time.Now().UnixMilli() - d.Milliseconds()
		return nil
	}, 2*time.Hour); err != nil {
		t.Fatalf("backdate empty_since by %s: %v", d, err)
	}
}

// TestHeartbeatNoShowUsesShortTimeout:从未连入的局空场超过**短**阈值即判弃,
// 无需等满 empty_battle_timeout(否则每次分配白押一台 14Gi Pod 满 5 分钟)。
func TestHeartbeatNoShowUsesShortTimeout(t *testing.T) {
	ctx := context.Background()
	_, repo := newUsecase(t)
	cfg := testCfg()
	cfg.EmptyBattleTimeout = config.Duration(5 * time.Minute)
	cfg.NoShowBattleTimeout = config.Duration(90 * time.Second)
	uc := NewAllocatorUsecase(repo, NewMockGameServerAllocator(cfg), cfg)

	res := allocateReadyNoShow(t, uc, repo, 7, []uint64{10, 20})
	if got, _, _ := repo.GetBattle(ctx, 7); got.EverHadPlayers {
		t.Fatal("EverHadPlayers must stay false when nobody ever connected")
	}

	// 卡在两档之间:已超 no-show(90s),未到 empty(5m)
	backdateEmptySinceBy(t, repo, 7, 2*time.Minute)
	hb, err := uc.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if hb.Command != commandStop {
		t.Fatalf("command = %q, want stop (no-show 应走短阈值,不等满 5m)", hb.Command)
	}
	if got, _, _ := repo.GetBattle(ctx, 7); got.State != stateAbandoned {
		t.Fatalf("state = %q, want abandoned", got.State)
	}
}

// TestHeartbeatEverHadPlayersKeepsLongTimeout:有人连入过的局在同一时刻**不得**判弃 ——
// 短阈值只对 no-show 生效,断线重连的玩家必须仍有 empty_battle_timeout 那么久可以回来。
// 这条是防止「防刷改动误伤真实掉线玩家」的护栏(§9.20)。
func TestHeartbeatEverHadPlayersKeepsLongTimeout(t *testing.T) {
	ctx := context.Background()
	_, repo := newUsecase(t)
	cfg := testCfg()
	cfg.EmptyBattleTimeout = config.Duration(5 * time.Minute)
	cfg.NoShowBattleTimeout = config.Duration(90 * time.Second)
	uc := NewAllocatorUsecase(repo, NewMockGameServerAllocator(cfg), cfg)

	// allocateReady 会以 player_count=len(playerIDs)>0 上报 ⇒ EverHadPlayers 置位
	res := allocateReady(t, uc, repo, 7, []uint64{10, 20}, 1, "5v5_ranked")
	got, _, _ := repo.GetBattle(ctx, 7)
	if !got.EverHadPlayers {
		t.Fatal("EverHadPlayers should be set after a heartbeat with player_count>0")
	}

	// 全员掉线 → 起计时
	if _, err := uc.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli()); err != nil {
		t.Fatalf("empty heartbeat: %v", err)
	}
	// 与上面那条测试**完全相同**的空场时长,但因为有人连入过,必须仍然活着
	backdateEmptySinceBy(t, repo, 7, 2*time.Minute)
	hb, err := uc.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if hb.Command != commandNone {
		t.Fatalf("command = %q, want none (有人连入过 ⇒ 必须给断线重连留满 5m)", hb.Command)
	}
	if got2, _, _ := repo.GetBattle(ctx, 7); got2.State != stateRunning {
		t.Fatalf("state = %q, want still running", got2.State)
	}
}

// TestHeartbeatEverHadPlayersStickyAcrossDisconnect:EverHadPlayers 一经置位永不清零 ——
// 玩家进来又全部离开后,该局不得退回 no-show 短阈值档。
func TestHeartbeatEverHadPlayersStickyAcrossDisconnect(t *testing.T) {
	ctx := context.Background()
	uc, repo := newUsecase(t)

	res := allocateReady(t, uc, repo, 7, []uint64{10, 20}, 1, "5v5_ranked")
	// 连续多跳 0 人,EverHadPlayers 必须保持 true
	for i := 0; i < 3; i++ {
		if _, err := uc.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli()); err != nil {
			t.Fatalf("empty heartbeat %d: %v", i, err)
		}
	}
	got, _, _ := repo.GetBattle(ctx, 7)
	if !got.EverHadPlayers {
		t.Fatal("EverHadPlayers must stay true after players leave (sticky)")
	}
}

// TestHeartbeatNoShowDisabledFallsBackToEmpty:no_show_battle_timeout 配负值 = 显式禁用差异化,
// 退化成改动前的单阈值行为(no-show 局也享受长阈值,绝不能变成"永不回收")。
func TestHeartbeatNoShowDisabledFallsBackToEmpty(t *testing.T) {
	ctx := context.Background()
	_, repo := newUsecase(t)
	cfg := testCfg()
	cfg.EmptyBattleTimeout = config.Duration(5 * time.Minute)
	cfg.NoShowBattleTimeout = config.Duration(-1) // 显式禁用差异化
	uc := NewAllocatorUsecase(repo, NewMockGameServerAllocator(cfg), cfg)

	res := allocateReadyNoShow(t, uc, repo, 7, []uint64{10, 20})

	// 超短阈值但未到长阈值:禁用差异化后不应判弃
	backdateEmptySinceBy(t, repo, 7, 2*time.Minute)
	hb, err := uc.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if hb.Command != commandNone {
		t.Fatalf("command = %q, want none (差异化已禁用,应等满 empty_battle_timeout)", hb.Command)
	}

	// 但超过长阈值后仍必须回收 —— 禁用差异化 ≠ 永不回收
	backdateEmptySinceBy(t, repo, 7, 6*time.Minute)
	hb2, err := uc.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat2: %v", err)
	}
	if hb2.Command != commandStop {
		t.Fatalf("command = %q, want stop (超长阈值必须回收)", hb2.Command)
	}
}

// TestHeartbeatEmptyTimeoutDeliveryRetry:空场判弃时 Kafka 不可用 → 保留在 active;
// 后续 sweep 以 firstAbandon=false 路径重试投递(不重复回收 pod),闭环同心跳超时补偿(不变量 §4)。
func TestHeartbeatEmptyTimeoutDeliveryRetry(t *testing.T) {
	ctx := context.Background()
	alloc := &countingAllocator{inner: NewMockGameServerAllocator(testCfg())}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)
	life := &mockLifecycle{failFirst: 1} // 心跳内首投失败,sweep 重试成功
	uc.SetLifecyclePusher(life)

	res := allocateReady(t, uc, repo, 7, []uint64{10, 20}, 1, "5v5_ranked")
	if _, err := uc.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli()); err != nil {
		t.Fatalf("empty heartbeat: %v", err)
	}
	backdateEmptySince(t, repo, 7)
	hb, err := uc.Heartbeat(ctx, 7, res.DSPodName, 0, "running", time.Now().UnixMilli())
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if hb.Command != commandStop {
		t.Fatalf("command = %q, want stop", hb.Command)
	}
	// 投递失败 → 保留在 active 等 sweep 重试
	if ids, _ := repo.RangeActiveBattles(ctx); len(ids) != 1 {
		t.Fatalf("active = %v, want still 1 (delivery retry pending)", ids)
	}

	// DS 收 stop 停跳 → 心跳超时后 sweep 扫到,firstAbandon=false 路径重试投递成功
	backdate(t, repo, 7)
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if ids, _ := repo.RangeActiveBattles(ctx); len(ids) != 0 {
		t.Fatalf("active = %v, want empty after retry delivered", ids)
	}
	if alloc.releases != 1 {
		t.Fatalf("releases = %d, want exactly 1 (no re-release on retry)", alloc.releases)
	}
	if len(life.delivered) != 1 || life.delivered[0] != 7 {
		t.Fatalf("delivered = %v, want [7]", life.delivered)
	}
	// 终态镜像仍可查
	if rec, found, _ := repo.GetBattle(ctx, 7); !found || rec.State != "abandoned" {
		t.Fatalf("terminal record missing/wrong: found=%v rec=%+v", found, rec)
	}
}

// TestSweepReliableCompensation_KeepsTTLOnFailure:Kafka 持续不可用时,abandoned 标记 + 每轮重试
// 走 UpdateBattleKeepTTL(KEEPTTL),保留镜像原 TTL 不刷新 → BattleTTL 是补偿重试的天然上界
// (不变量 §4)。若误用刷新 TTL 的更新路径,会导致镜像永不过期、active 无限堆积。
func TestSweepReliableCompensation_KeepsTTLOnFailure(t *testing.T) {
	ctx := context.Background()
	alloc := &countingAllocator{inner: NewMockGameServerAllocator(testCfg())}
	uc, repo, mr := newUsecaseWithAlloc(t, alloc)
	life := &mockLifecycle{failFirst: 1000} // 始终投递失败
	uc.SetLifecyclePusher(life)

	allocateReady(t, uc, repo, 7, []uint64{10, 20}, 1, "5v5_ranked")
	backdate(t, repo, 7)

	// 把 TTL 钉到一个已知的小值,便于检测是否被重试刷新(CreateBattle/backdate 会先设成 BattleTTL 2h)
	key := "pandora:ds:battle:{7}"
	mr.SetTTL(key, 90*time.Second)
	ttlBefore := mr.TTL(key)
	if ttlBefore <= 0 {
		t.Fatalf("precondition: ttl not pinned, got %v", ttlBefore)
	}

	// 连续多轮 sweep,全部投递失败 → abandoned 对局保留在 active 重试
	for i := 0; i < 3; i++ {
		if err := uc.sweepOnce(ctx); err != nil {
			t.Fatalf("sweep #%d: %v", i+1, err)
		}
	}

	// 关键断言:TTL 没被重试刷新(仍 ≤ 钉住的 90s,而非回弹到 BattleTTL 2h)
	ttlAfter := mr.TTL(key)
	if ttlAfter > ttlBefore {
		t.Fatalf("TTL refreshed on retry: before=%v after=%v(KEEPTTL 未生效,BattleTTL 上界不成立)", ttlBefore, ttlAfter)
	}
	// 仍保留在 active 等待重试,状态 abandoned,pod 只回收一次
	if ids, _ := repo.RangeActiveBattles(ctx); len(ids) != 1 {
		t.Fatalf("active = %v, want still 1 (retry pending)", ids)
	}
	if got, _, _ := repo.GetBattle(ctx, 7); got.State != "abandoned" {
		t.Fatalf("state = %q, want abandoned", got.State)
	}
	if alloc.releases != 1 {
		t.Fatalf("pod released %d times, want exactly 1 (no re-release during retry)", alloc.releases)
	}
}

// ---- 断线重连:心跳续期 BATTLE 位置(docs/design/battle-reconnect.md)----

type refreshCall struct {
	players []uint64
	matchID uint64
	dsAddr  string
}

// fakeRefresher 记录 RefreshBattleLocations 调用(异步续期,用带缓冲 channel 接收)。
type fakeRefresher struct {
	calls chan refreshCall
}

func newFakeRefresher() *fakeRefresher { return &fakeRefresher{calls: make(chan refreshCall, 8)} }

func (f *fakeRefresher) RefreshBattleLocations(_ context.Context, players []uint64, matchID uint64, dsAddr string) error {
	cp := append([]uint64(nil), players...)
	f.calls <- refreshCall{players: cp, matchID: matchID, dsAddr: dsAddr}
	return nil
}

// TestHeartbeatRefreshesBattleLocation 验证:running 心跳后异步给在场玩家续期 BATTLE 位置,
// 携带正确的 match_id / ds_addr / 玩家列表(让登录侧能在整局内识别重连)。
func TestHeartbeatRefreshesBattleLocation(t *testing.T) {
	ctx := context.Background()
	uc, repo, _ := newUsecaseWithAlloc(t, NewMockGameServerAllocator(testCfg()))
	matchID := uint64(555)
	players := []uint64{1, 2, 3}
	res := allocateReady(t, uc, repo, matchID, players, 10, "5v5_ranked")

	fr := newFakeRefresher()
	uc.SetLocationRefresher(fr)

	b, found, err := repo.GetBattle(ctx, matchID)
	if err != nil || !found {
		t.Fatalf("get battle: err=%v found=%v", err, found)
	}
	if _, err := uc.Heartbeat(ctx, matchID, b.DsPodName, int32(len(players)), "running", time.Now().UnixMilli()); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	select {
	case c := <-fr.calls:
		if c.matchID != matchID {
			t.Errorf("refresh matchID = %d, want %d", c.matchID, matchID)
		}
		if c.dsAddr != res.DSAddr {
			t.Errorf("refresh dsAddr = %q, want %q", c.dsAddr, res.DSAddr)
		}
		if len(c.players) != len(players) {
			t.Errorf("refresh players = %v, want %v", c.players, players)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("running heartbeat did not trigger battle location refresh")
	}
}

// TestHeartbeatNoRefreshWhenNilRefresher 验证:未注入 refresher(dev / 未配 locator_addr)时,
// running 心跳不 panic、正常返回(弱依赖降级)。
func TestHeartbeatNoRefreshWhenNilRefresher(t *testing.T) {
	ctx := context.Background()
	uc, repo, _ := newUsecaseWithAlloc(t, NewMockGameServerAllocator(testCfg()))
	matchID := uint64(556)
	players := []uint64{1, 2}
	allocateReady(t, uc, repo, matchID, players, 10, "5v5_ranked") // 未 SetLocationRefresher

	b, _, _ := repo.GetBattle(ctx, matchID)
	if _, err := uc.Heartbeat(ctx, matchID, b.DsPodName, int32(len(players)), "running", time.Now().UnixMilli()); err != nil {
		t.Fatalf("heartbeat with nil refresher should not fail: %v", err)
	}
}
