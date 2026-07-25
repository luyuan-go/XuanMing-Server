package biz

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/transport"

	"github.com/luyuancpp/pandora/pkg/errcode"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/data"
)

// ── 测试替身 ──────────────────────────────────────────────────────────────────

// fakeRepo 是 data.HubRepo 的内存实现(无 Redis)。所有读返回克隆,避免别名污染。
type fakeRepo struct {
	mu               sync.Mutex
	shards           map[string]*hubv1.HubShardStorageRecord
	active           map[string]int64 // pod → last_heartbeat_ms
	assignments      map[uint64]*hubv1.HubAssignmentStorageRecord
	teamShards       map[uint64]string
	members          map[string]map[uint64]bool // pod → set(player_id)
	cooldowns        map[uint64]bool            // player_id → 切线冷却占坑
	transferCleanups map[string]map[uint64]map[string]bool

	// setAssignErr 非 nil 时，SetAssignment 直接返回该错误（测试注入失败用）。
	setAssignErr error
	// advanceFenceCalls 记录 AdvanceWriterFences 调用次数（继任者水位推扫测试用）。
	advanceFenceCalls int
	// advanceFenceTokens 记录接流前激活钩子传入的显式 token（R10 P0-4 硬门）。
	advanceFenceTokens []uint64
}

func (f *fakeRepo) AdvanceWriterFences(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advanceFenceCalls++
	return nil
}

func (f *fakeRepo) AdvanceWriterFencesForToken(_ context.Context, token uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.advanceFenceTokens = append(f.advanceFenceTokens, token)
	return nil
}

type emptyObservedFleet struct {
	observation HubInstanceObservation
	err         error
}

func (f *emptyObservedFleet) ListShards(context.Context, string) ([]ShardCandidate, error) {
	return nil, nil
}

func (f *emptyObservedFleet) ObserveShardInstance(context.Context, string) (HubInstanceObservation, error) {
	return f.observation, f.err
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{
		shards:           map[string]*hubv1.HubShardStorageRecord{},
		active:           map[string]int64{},
		assignments:      map[uint64]*hubv1.HubAssignmentStorageRecord{},
		teamShards:       map[uint64]string{},
		members:          map[string]map[uint64]bool{},
		cooldowns:        map[uint64]bool{},
		transferCleanups: map[string]map[uint64]map[string]bool{},
	}
}

func (f *fakeRepo) GetShard(_ context.Context, pod string) (*hubv1.HubShardStorageRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.shards[pod]
	if !ok {
		return nil, false, nil
	}
	return proto.Clone(s).(*hubv1.HubShardStorageRecord), true, nil
}

func (f *fakeRepo) ListShards(_ context.Context) ([]*hubv1.HubShardStorageRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*hubv1.HubShardStorageRecord, 0, len(f.shards))
	for _, s := range f.shards {
		out = append(out, proto.Clone(s).(*hubv1.HubShardStorageRecord))
	}
	return out, nil
}

func (f *fakeRepo) CreateShard(_ context.Context, rec *hubv1.HubShardStorageRecord, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.shards[rec.HubPodName] = proto.Clone(rec).(*hubv1.HubShardStorageRecord)
	return nil
}

func (f *fakeRepo) UpdateShardWithLock(_ context.Context, pod string, _ int, fn func(*hubv1.HubShardStorageRecord) error, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.shards[pod]
	if !ok {
		return errcode.New(errcode.ErrHubNoAvailable, "shard %s not found", pod)
	}
	clone := proto.Clone(s).(*hubv1.HubShardStorageRecord)
	if err := fn(clone); err != nil {
		return err
	}
	f.shards[pod] = clone
	return nil
}

func (f *fakeRepo) HeartbeatShard(_ context.Context, pod string, playerCount int32, state string, tsMs int64, _ uint64, _ bool, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.shards[pod]
	if !ok {
		return false, nil
	}
	s.PlayerCount = playerCount
	// 镜像 RedisHubRepo:禁止 DS 上报的 ready 把 allocator 标记的 draining/stopping 降级,
	// 但心跳超时误标的 draining(DrainingSinceMs==0)可被健康 ready 心跳复位(存活恢复)。
	switch {
	case state == "":
		// 空上报:不动状态。
	case fakeDrainRank(state) >= fakeDrainRank(s.State):
		s.State = state
	case state == "ready" && s.State == "draining" && s.DrainingSinceMs == 0:
		s.State = "ready"
	default:
		// 其余降级(强制整合 draining 被 ready 冲)→ 保持不变。
	}
	s.LastHeartbeatMs = tsMs
	f.active[pod] = tsMs
	return true, nil
}

func fakeDrainRank(state string) int {
	switch state {
	case "draining":
		return 1
	case "stopping":
		return 2
	default:
		return 0
	}
}

func (f *fakeRepo) RemoveShard(_ context.Context, pod string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.shards, pod)
	delete(f.active, pod)
	delete(f.members, pod)
	return nil
}

func (f *fakeRepo) RangeStaleShards(_ context.Context, thresholdMs int64) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for pod, ts := range f.active {
		if ts > 0 && ts <= thresholdMs {
			out = append(out, pod)
		}
	}
	return out, nil
}

func (f *fakeRepo) RemoveActive(_ context.Context, pod string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.active, pod)
	return nil
}

func (f *fakeRepo) GetAssignment(_ context.Context, playerID uint64) (*hubv1.HubAssignmentStorageRecord, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.assignments[playerID]
	if !ok {
		return nil, false, nil
	}
	return proto.Clone(a).(*hubv1.HubAssignmentStorageRecord), true, nil
}

func (f *fakeRepo) SetAssignment(_ context.Context, rec *hubv1.HubAssignmentStorageRecord, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setAssignErr != nil {
		return f.setAssignErr
	}
	f.assignments[rec.PlayerId] = proto.Clone(rec).(*hubv1.HubAssignmentStorageRecord)
	return nil
}

func (f *fakeRepo) CompareAndSwapAssignment(_ context.Context, playerID uint64, expected, next *hubv1.HubAssignmentStorageRecord, _ time.Duration) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.setAssignErr != nil {
		return false, f.setAssignErr
	}
	current, found := f.assignments[playerID]
	if expected == nil {
		if found {
			return false, nil
		}
	} else if !found || !proto.Equal(current, expected) {
		return false, nil
	}
	if next == nil {
		delete(f.assignments, playerID)
	} else {
		f.assignments[playerID] = proto.Clone(next).(*hubv1.HubAssignmentStorageRecord)
	}
	return true, nil
}

func (f *fakeRepo) DeleteAssignmentIfPodMatches(_ context.Context, playerID uint64, pod string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.assignments[playerID]
	if !ok || a.HubPodName != pod {
		return false, nil
	}
	delete(f.assignments, playerID)
	return true, nil
}

func (f *fakeRepo) GetTeamShard(_ context.Context, teamID uint64) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	pod, ok := f.teamShards[teamID]
	return pod, ok, nil
}

func (f *fakeRepo) SetTeamShard(_ context.Context, teamID uint64, pod string, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.teamShards[teamID] = pod
	return nil
}

func (f *fakeRepo) AddShardMember(_ context.Context, pod string, playerID uint64, _ time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.members[pod] == nil {
		f.members[pod] = map[uint64]bool{}
	}
	f.members[pod][playerID] = true
	return nil
}

func (f *fakeRepo) RemoveShardMember(_ context.Context, pod string, playerID uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m, ok := f.members[pod]; ok {
		delete(m, playerID)
	}
	return nil
}

func (f *fakeRepo) ListShardMembers(_ context.Context, pod string) ([]uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]uint64, 0, len(f.members[pod]))
	for pid := range f.members[pod] {
		out = append(out, pid)
	}
	return out, nil
}

func (f *fakeRepo) RegisterTransferCleanup(_ context.Context, sourcePod string, ref data.TransferCleanupRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.transferCleanups[sourcePod] == nil {
		f.transferCleanups[sourcePod] = map[uint64]map[string]bool{}
	}
	if f.transferCleanups[sourcePod][ref.PlayerID] == nil {
		f.transferCleanups[sourcePod][ref.PlayerID] = map[string]bool{}
	}
	f.transferCleanups[sourcePod][ref.PlayerID][ref.TargetAssignmentID] = true
	return nil
}

func (f *fakeRepo) RemoveTransferCleanup(_ context.Context, sourcePod string, ref data.TransferCleanupRef) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if byPlayer := f.transferCleanups[sourcePod]; byPlayer != nil {
		if assignments := byPlayer[ref.PlayerID]; assignments != nil {
			delete(assignments, ref.TargetAssignmentID)
			if len(assignments) == 0 {
				delete(byPlayer, ref.PlayerID)
			}
		}
	}
	return nil
}

func (f *fakeRepo) ListTransferCleanupPods(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.transferCleanups))
	for pod := range f.transferCleanups {
		out = append(out, pod)
	}
	return out, nil
}

func (f *fakeRepo) ListTransferCleanups(_ context.Context, sourcePod string) ([]data.TransferCleanupRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []data.TransferCleanupRef
	for playerID, assignments := range f.transferCleanups[sourcePod] {
		for assignmentID := range assignments {
			out = append(out, data.TransferCleanupRef{PlayerID: playerID, TargetAssignmentID: assignmentID})
		}
	}
	return out, nil
}

func (f *fakeRepo) TryTransferCooldown(_ context.Context, playerID uint64, cooldown time.Duration) (bool, error) {
	if cooldown <= 0 {
		return true, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.cooldowns[playerID] {
		return false, nil
	}
	f.cooldowns[playerID] = true
	return true, nil
}

func (f *fakeRepo) ClearTransferCooldown(_ context.Context, playerID uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.cooldowns, playerID)
	return nil
}

// playerCount 是测试断言辅助。
func (f *fakeRepo) playerCount(pod string) int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if s, ok := f.shards[pod]; ok {
		return s.PlayerCount
	}
	return -1
}

func (f *fakeRepo) setHeartbeatTime(pod string, tsMs int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if shard, ok := f.shards[pod]; ok {
		shard.LastHeartbeatMs = tsMs
	}
	f.active[pod] = tsMs
}

// fakeSigner 返回确定性假票据。
type fakeSigner struct {
	calls       int
	lastRole    uint32 // 最近一次签票携带的 role_id(选角权威化断言用)
	lastBinding HubTicketBinding
}

func (s *fakeSigner) SignHubTicket(playerID uint64, roleID uint32, binding HubTicketBinding) (string, int64, error) {
	s.calls++
	s.lastRole = roleID
	s.lastBinding = binding
	return "hub-ticket-fake", time.Now().Add(5 * time.Minute).UnixMilli(), nil
}

// fakeMigratePusher 记录强制整合迁移推送(测试断言用);err 非 nil 时模拟发布失败。
type fakeMigratePusher struct {
	mu     sync.Mutex
	pushes []uint64
	err    error
}

func (p *fakeMigratePusher) PushMigrate(_ context.Context, playerID uint64, _ []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.pushes = append(p.pushes, playerID)
	return nil
}

func (p *fakeMigratePusher) setErr(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.err = err
}

func (p *fakeMigratePusher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pushes)
}

var _ data.HubRepo = (*fakeRepo)(nil)
var _ TicketSigner = (*fakeSigner)(nil)
var _ HubMigratePusher = (*fakeMigratePusher)(nil)
var _ HubFleetScaler = (*memFleetScaler)(nil)

// memFleetScaler 是测试用的可变副本数 Fleet scaler。
// MockHubFleetProvider 本身不再实现 HubFleetScaler(拓扑-only),故 reconcile/consolidation
// 测试需要它来让 NewHubUsecase 检测到 scaler 从而启用治理;Set 真实改变 replicas(非 no-op)。
type memFleetScaler struct {
	*MockHubFleetProvider
	mu       sync.Mutex
	replicas int32
}

func (f *memFleetScaler) GetFleetReplicas(context.Context) (int32, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.replicas, nil
}

func (f *memFleetScaler) SetFleetReplicas(_ context.Context, r int32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replicas = r
	return nil
}

// ── 测试夹具 ──────────────────────────────────────────────────────────────────

func testConf() conf.HubConf {
	c := conf.Config{}
	c.Defaults()
	return c.Hub
}

func newTestUsecase(capacity int32, shardCount int) (*HubUsecase, *fakeRepo, *fakeSigner) {
	cfg := testConf()
	cfg.DefaultCapacity = capacity
	cfg.MockShardCount = shardCount
	repo := newFakeRepo()
	fleet := NewMockHubFleetProvider(cfg)
	signer := &fakeSigner{}
	return NewHubUsecase(repo, fleet, signer, cfg), repo, signer
}

func TestReconcileTopologyCandidateAbsenceRetainsPhysicalOwnerFence(t *testing.T) {
	cfg := testConf()
	repo := newFakeRepo()
	seedShard(repo, "hub-physical", 1, 1)
	if err := repo.UpdateShardWithLock(context.Background(), "hub-physical", 1,
		func(s *hubv1.HubShardStorageRecord) error {
			s.GameserverUid = "uid-live"
			return nil
		}, time.Minute); err != nil {
		t.Fatal(err)
	}
	fleet := &emptyObservedFleet{observation: HubInstanceObservation{
		GameServerFound: true, GameServerUID: "uid-live", PodFound: true,
		PodOwnerGameServerUID: "uid-live",
	}}
	uc := NewHubUsecase(repo, fleet, &fakeSigner{}, cfg)
	if err := uc.reconcileShardTopology(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, found, err := repo.GetShard(context.Background(), "hub-physical")
	if err != nil || !found {
		t.Fatalf("unroutable live shard must remain a physical-owner fence: found=%v err=%v", found, err)
	}
	if got.GetState() != stateDraining || got.GetPlayerCount() != 1 || got.GetGameserverUid() != "uid-live" {
		t.Fatalf("candidate absence must only fence routing, got %+v", got)
	}
}

// ── 测试用例 ──────────────────────────────────────────────────────────────────

func TestAssignHub_LazySeedAndLeastLoaded(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()

	res, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, "")
	if err != nil {
		t.Fatalf("AssignHub err: %v", err)
	}
	// 空集合 lazy-seed 后,最空分片并列取 shard_id 最小 → shard 1
	if res.ShardID != 1 {
		t.Fatalf("want shard 1, got %d", res.ShardID)
	}
	if res.HubTicket == "" {
		t.Fatal("want hub ticket")
	}
	if got := repo.playerCount("pandora-hub-global-1"); got != 1 {
		t.Fatalf("want player_count 1, got %d", got)
	}
	// 共种 3 个分片
	shards, _ := repo.ListShards(ctx)
	if len(shards) != 3 {
		t.Fatalf("want 3 seeded shards, got %d", len(shards))
	}
}

// TestAssignHub_SourceMatchFenceIntoTicket:Battle→Hub 回流 fence(2026-07-21)。
// AssignHub 携带 source_match_id 时必须原样盖进签票 binding(首次分配与幂等重签同语义);
// 普通登录(0)不携带。fence 只进票据,不进归属镜像。
func TestAssignHub_SourceMatchFenceIntoTicket(t *testing.T) {
	uc, _, signer := newTestUsecase(500, 3)
	ctx := context.Background()

	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 9001, ""); err != nil {
		t.Fatalf("AssignHub with fence err: %v", err)
	}
	if signer.lastBinding.SourceMatchID != 9001 {
		t.Fatalf("signed binding source_match_id = %d, want 9001", signer.lastBinding.SourceMatchID)
	}
	// 幂等重签(同玩家再次 AssignHub,普通登录无 fence)→ 本次票据不携带旧 fence。
	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("second assign err: %v", err)
	}
	if signer.lastBinding.SourceMatchID != 0 {
		t.Fatalf("plain re-assign must not carry stale fence, got %d", signer.lastBinding.SourceMatchID)
	}
}

func TestAssignHub_Idempotent(t *testing.T) {
	uc, repo, signer := newTestUsecase(500, 3)
	ctx := context.Background()

	r1, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, "")
	if err != nil {
		t.Fatalf("first assign err: %v", err)
	}
	r2, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, "")
	if err != nil {
		t.Fatalf("second assign err: %v", err)
	}
	if r1.HubPodName != r2.HubPodName {
		t.Fatalf("idempotent assign should return same pod: %s vs %s", r1.HubPodName, r2.HubPodName)
	}
	// 不重复占位:player_count 仍为 1
	if got := repo.playerCount(r1.HubPodName); got != 1 {
		t.Fatalf("idempotent should not double-count, got %d", got)
	}
	// 两次都重签票
	if signer.calls != 2 {
		t.Fatalf("want 2 sign calls, got %d", signer.calls)
	}
}

func TestAssignHub_SpreadAcrossShards(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()

	// 3 个玩家应分散到 3 个分片(每次选最空)
	pods := map[string]bool{}
	for i := uint64(1); i <= 3; i++ {
		res, err := uc.AssignHub(ctx, i, "global", 0, 0, 0, "")
		if err != nil {
			t.Fatalf("assign p%d err: %v", i, err)
		}
		pods[res.HubPodName] = true
	}
	if len(pods) != 3 {
		t.Fatalf("want 3 distinct shards, got %d", len(pods))
	}
	for pod := range pods {
		if got := repo.playerCount(pod); got != 1 {
			t.Fatalf("shard %s want count 1, got %d", pod, got)
		}
	}
}

func TestAssignHub_CapacityFull(t *testing.T) {
	uc, _, _ := newTestUsecase(1, 1) // 1 分片,容量 1
	ctx := context.Background()

	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("first assign err: %v", err)
	}
	_, err := uc.AssignHub(ctx, 1002, "global", 0, 0, 0, "")
	if err == nil {
		t.Fatal("want capacity-full error")
	}
	if errcode.As(err) != errcode.ErrHubNoAvailable {
		t.Fatalf("want ErrHubNoAvailable, got code %d", errcode.As(err))
	}
}

func TestAssignHub_TeammateColocation(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()

	r1, err := uc.AssignHub(ctx, 1001, "global", 7, 0, 0, "") // team 7
	if err != nil {
		t.Fatalf("p1 assign err: %v", err)
	}
	r2, err := uc.AssignHub(ctx, 1002, "global", 7, 0, 0, "") // same team
	if err != nil {
		t.Fatalf("p2 assign err: %v", err)
	}
	if r1.HubPodName != r2.HubPodName {
		t.Fatalf("teammates should co-locate: %s vs %s", r1.HubPodName, r2.HubPodName)
	}
	if got := repo.playerCount(r1.HubPodName); got != 2 {
		t.Fatalf("co-located shard want count 2, got %d", got)
	}
}

func TestReleaseHub_DecrementAndIdempotent(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()

	res, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, "")
	if err != nil {
		t.Fatalf("assign err: %v", err)
	}
	if err := uc.ReleaseHub(ctx, 1001); err != nil {
		t.Fatalf("release err: %v", err)
	}
	if got := repo.playerCount(res.HubPodName); got != 0 {
		t.Fatalf("after release want count 0, got %d", got)
	}
	// 幂等:再次 release 不报错、不变负
	if err := uc.ReleaseHub(ctx, 1001); err != nil {
		t.Fatalf("idempotent release err: %v", err)
	}
	if got := repo.playerCount(res.HubPodName); got != 0 {
		t.Fatalf("idempotent release count drift, got %d", got)
	}
}

func TestTransferHub_MoveBetweenShards(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()

	r1, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, "") // shard 1
	if err != nil {
		t.Fatalf("assign err: %v", err)
	}
	unknown := protowire.AppendTag(nil, 2047, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 7)
	before, _, _ := repo.GetAssignment(ctx, 1001)
	withFuture := proto.Clone(before).(*hubv1.HubAssignmentStorageRecord)
	withFuture.ProtoReflect().SetUnknown(unknown)
	if swapped, err := repo.CompareAndSwapAssignment(ctx, 1001, before, withFuture, time.Minute); err != nil || !swapped {
		t.Fatalf("inject future field swapped=%v err=%v", swapped, err)
	}
	// 点名传送到 shard 2
	tr, err := uc.TransferHub(ctx, 1001, 2)
	if err != nil {
		t.Fatalf("transfer err: %v", err)
	}
	if tr.NewHubPodName == r1.HubPodName {
		t.Fatalf("transfer should change pod, still %s", tr.NewHubPodName)
	}
	// 旧分片退位、新分片占位
	if got := repo.playerCount(r1.HubPodName); got != 0 {
		t.Fatalf("old shard want 0, got %d", got)
	}
	if got := repo.playerCount(tr.NewHubPodName); got != 1 {
		t.Fatalf("new shard want 1, got %d", got)
	}
	// 归属更新到新分片
	a, found, _ := repo.GetAssignment(ctx, 1001)
	if !found || a.HubPodName != tr.NewHubPodName {
		t.Fatalf("assignment not moved: found=%v pod=%v", found, a)
	}
	if !bytes.Equal(a.ProtoReflect().GetUnknown(), unknown) {
		t.Fatalf("transfer lost future fields: got=%x want=%x", a.ProtoReflect().GetUnknown(), unknown)
	}
}

func TestTransferHub_NotInHub(t *testing.T) {
	uc, _, _ := newTestUsecase(500, 3)
	ctx := context.Background()
	_, err := uc.TransferHub(ctx, 9999, 0)
	if err == nil {
		t.Fatal("want transfer-failed for player not in hub")
	}
	if errcode.As(err) != errcode.ErrHubTransferFailed {
		t.Fatalf("want ErrHubTransferFailed, got %d", errcode.As(err))
	}
}

// TestTransferHub_SetAssignmentFailRollback 覆盖 SetAssignment 失败场景:
// 顺序为 reserve 新 → SetAssignment → release 旧;SetAssignment 失败时应回滚新分片占位,
// 且旧分片 player_count 与旧 assignment 都保持原状(玩家仍在旧 hub,无悬挂状态)。
func TestTransferHub_SetAssignmentFailRollback(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()

	r1, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, "") // 落在 shard 1
	if err != nil {
		t.Fatalf("assign err: %v", err)
	}
	oldPod := r1.HubPodName
	targetPod := "pandora-hub-global-2"

	// 注入 SetAssignment 失败
	repo.mu.Lock()
	repo.setAssignErr = errcode.New(errcode.ErrInternal, "redis down")
	repo.mu.Unlock()

	_, terr := uc.TransferHub(ctx, 1001, 2) // 点名传送到 shard 2
	if terr == nil {
		t.Fatal("want transfer error when SetAssignment fails")
	}

	// 1. 新分片占位已回滚 → player_count 0
	if got := repo.playerCount(targetPod); got != 0 {
		t.Fatalf("target shard seat not rolled back, count=%d want 0", got)
	}
	// 2. 旧分片 player_count 保持 1(未被提前扣减)
	if got := repo.playerCount(oldPod); got != 1 {
		t.Fatalf("old shard count drifted, count=%d want 1", got)
	}
	// 3. 旧 assignment 仍指向旧 pod(玩家没被悬挂)
	a, found, _ := repo.GetAssignment(ctx, 1001)
	if !found || a.HubPodName != oldPod {
		t.Fatalf("assignment should stay on old pod: found=%v pod=%v", found, a.GetHubPodName())
	}

	// 4. 修复后重试可正常传送
	repo.mu.Lock()
	repo.setAssignErr = nil
	repo.mu.Unlock()
	tr, rerr := uc.TransferHub(ctx, 1001, 2)
	if rerr != nil {
		t.Fatalf("retry transfer err: %v", rerr)
	}
	if tr.NewHubPodName != targetPod {
		t.Fatalf("retry should move to %s, got %s", targetPod, tr.NewHubPodName)
	}
	if got := repo.playerCount(oldPod); got != 0 {
		t.Fatalf("after successful transfer old shard want 0, got %d", got)
	}
	if got := repo.playerCount(targetPod); got != 1 {
		t.Fatalf("after successful transfer new shard want 1, got %d", got)
	}
}

func TestHeartbeat_SeedsTopologyBeforeCommand(t *testing.T) {
	uc, _, _ := newTestUsecase(500, 3)
	ctx := context.Background()

	// 没有 Redis 分片镜像时，首跳先刷新 Fleet 拓扑并重试，避免新 Hub 被误判孤儿。
	res, err := uc.Heartbeat(ctx, "pandora-hub-global-1", 42, "ready", time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("heartbeat err: %v", err)
	}
	if res.Command != commandNone {
		t.Fatalf("seeded heartbeat want no command, got %q", res.Command)
	}
}

func TestHeartbeat_UnknownShardWaitsForTopology(t *testing.T) {
	uc, _, _ := newTestUsecase(500, 3)
	ctx := context.Background()

	// Fleet 刷新后仍不存在的 pod 必须返回 Unavailable，service 不得继续刷新 locator presence。
	res, err := uc.Heartbeat(ctx, "pandora-hub-ghost-9", 0, "ready", time.Now().UnixMilli(), 0)
	if errcode.As(err) != errcode.ErrUnavailable || res != nil {
		t.Fatalf("unknown shard must fail unavailable without authorized result, res=%+v err=%v", res, err)
	}
}

func TestHeartbeat_KnownShardNoCommand(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()
	// 先 assign 触发种子,再心跳已知分片
	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("assign err: %v", err)
	}
	now := time.Now().UnixMilli()
	res, err := uc.Heartbeat(ctx, "pandora-hub-global-1", 42, "ready", now, 0)
	if err != nil {
		t.Fatalf("heartbeat err: %v", err)
	}
	if res.Command != commandNone {
		t.Fatalf("known shard want no command, got %q", res.Command)
	}
	if got := repo.playerCount("pandora-hub-global-1"); got != 42 {
		t.Fatalf("heartbeat should reconcile count to 42, got %d", got)
	}
}

func TestSweepOnce_MarksStaleDraining(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()
	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("assign err: %v", err)
	}
	pod := "pandora-hub-global-1"
	// 请求 ts_ms 已不再可信；先正常心跳，再直接构造派生索引/镜像陈旧态供 sweep 测试。
	staleTs := time.Now().Add(-1 * time.Hour).UnixMilli()
	if _, err := uc.Heartbeat(ctx, pod, 1, "ready", staleTs, 0); err != nil {
		t.Fatalf("heartbeat err: %v", err)
	}
	repo.setHeartbeatTime(pod, staleTs)

	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweepOnce err: %v", err)
	}
	s, _, _ := repo.GetShard(ctx, pod)
	if s.State != stateDraining {
		t.Fatalf("stale shard should be draining, got %q", s.State)
	}
	// 已移出 active(不再扫描)
	stale, _ := repo.RangeStaleShards(ctx, time.Now().UnixMilli())
	for _, p := range stale {
		if p == pod {
			t.Fatal("drained shard should be removed from active")
		}
	}
}

func TestSweepOnce_SkipsNeverHeartbeated(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()
	// 仅 assign(Mock 种子 last_heartbeat_ms=0,从不进 active)
	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("assign err: %v", err)
	}
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweepOnce err: %v", err)
	}
	// 种子分片不应被误标 draining
	s, _, _ := repo.GetShard(ctx, "pandora-hub-global-1")
	if s.State != stateReady {
		t.Fatalf("never-heartbeated seed should stay ready, got %q", s.State)
	}
}

func TestAssignHub_InvalidPlayer(t *testing.T) {
	uc, _, _ := newTestUsecase(500, 3)
	if _, err := uc.AssignHub(context.Background(), 0, "global", 0, 0, 0, ""); err == nil {
		t.Fatal("want invalid-arg error for player_id 0")
	} else if errcode.As(err) != errcode.ErrInvalidArg {
		t.Fatalf("want ErrInvalidArg, got %d", errcode.As(err))
	}
}

// ── 强制整合 + 迁移 ───────────────────────────────────────────────────────────

// newConsolidationUsecase 构造开启自动扩缩容 + 强制整合的 usecase,并注入迁移推送替身。
func newConsolidationUsecase(grace int32) (*HubUsecase, *fakeRepo, *fakeMigratePusher) {
	cfg := testConf()
	cfg.AutoScaleEnabled = true
	cfg.ConsolidationEnabled = true
	cfg.PlayersPerHub = 500
	cfg.MigrateGraceSeconds = grace
	cfg.ConsolidationBatch = 50
	repo := newFakeRepo()
	fleet := &memFleetScaler{
		MockHubFleetProvider: NewMockHubFleetProvider(cfg),
		replicas:             int32(cfg.MockShardCount),
	}
	pusher := &fakeMigratePusher{}
	uc := NewHubUsecase(repo, fleet, &fakeSigner{}, cfg)
	uc.SetMigratePusher(pusher)
	return uc, repo, pusher
}

// seedShard 直接在 fakeRepo 写入一个分片镜像。
func seedShard(repo *fakeRepo, pod string, shardID uint32, count int32) {
	_ = repo.CreateShard(context.Background(), &hubv1.HubShardStorageRecord{
		HubPodName:  pod,
		HubAddr:     pod + ":7777",
		Region:      "global",
		ShardId:     shardID,
		PlayerCount: count,
		Capacity:    500,
		State:       stateReady,
	}, time.Minute)
}

// seedPlayer 直接写入玩家归属 + 成员反向索引。
func seedPlayer(repo *fakeRepo, playerID uint64, pod string, shardID uint32) {
	ctx := context.Background()
	_ = repo.SetAssignment(ctx, &hubv1.HubAssignmentStorageRecord{
		PlayerId:   playerID,
		HubPodName: pod,
		HubAddr:    pod + ":7777",
		ShardId:    shardID,
		Region:     "global",
	}, time.Minute)
	_ = repo.AddShardMember(ctx, pod, playerID, time.Minute)
}

func TestReconcile_ConsolidationMigratesPlayers(t *testing.T) {
	uc, repo, pusher := newConsolidationUsecase(30)
	ctx := context.Background()

	// 两个 ready 分片:a 载 1 人,b 载 2 人。总在线 3 → need=1 → 多余 1 个分片(最空那个被排空)。
	seedShard(repo, "hub-a", 1, 1)
	seedShard(repo, "hub-b", 2, 2)
	seedPlayer(repo, 1001, "hub-a", 1)
	seedPlayer(repo, 1002, "hub-b", 2)
	seedPlayer(repo, 1003, "hub-b", 2)
	unknown := protowire.AppendTag(nil, 2046, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, []byte("future"))
	before, _, _ := repo.GetAssignment(ctx, 1001)
	withFuture := proto.Clone(before).(*hubv1.HubAssignmentStorageRecord)
	withFuture.ProtoReflect().SetUnknown(unknown)
	if swapped, err := repo.CompareAndSwapAssignment(ctx, 1001, before, withFuture, time.Minute); err != nil || !swapped {
		t.Fatalf("inject future field swapped=%v err=%v", swapped, err)
	}

	if err := uc.reconcileFleetReplicas(ctx); err != nil {
		t.Fatalf("reconcile err: %v", err)
	}

	// 最空分片 hub-a 被排空 → draining + 玩家迁到 hub-b。
	a, _, _ := repo.GetShard(ctx, "hub-a")
	if a.State != stateDraining {
		t.Fatalf("least-loaded shard hub-a should be draining, got %q", a.State)
	}
	if a.DrainingSinceMs == 0 {
		t.Fatal("draining shard should stamp DrainingSinceMs")
	}
	if got := repo.playerCount("hub-a"); got != 0 {
		t.Fatalf("drained shard hub-a want 0 players, got %d", got)
	}
	if got := repo.playerCount("hub-b"); got != 3 {
		t.Fatalf("target shard hub-b want 3 players, got %d", got)
	}
	// 玩家 1001 的归属已迁到 hub-b。
	asn, found, _ := repo.GetAssignment(ctx, 1001)
	if !found || asn.HubPodName != "hub-b" {
		t.Fatalf("player 1001 should be migrated to hub-b, got found=%v pod=%v", found, asn.GetHubPodName())
	}
	if !bytes.Equal(asn.ProtoReflect().GetUnknown(), unknown) {
		t.Fatalf("drain migration lost future fields: got=%x want=%x", asn.ProtoReflect().GetUnknown(), unknown)
	}
	// 推送了 1 条迁移通知(只有 hub-a 上的 1 个玩家被迁)。
	if pusher.count() != 1 {
		t.Fatalf("want 1 migrate push, got %d", pusher.count())
	}
}

func TestHeartbeat_DrainingShardReturnsDrainCommand(t *testing.T) {
	uc, repo, _ := newConsolidationUsecase(45)
	ctx := context.Background()
	seedShard(repo, "hub-x", 1, 0)
	// 标记 draining
	_ = repo.UpdateShardWithLock(ctx, "hub-x", 1, func(s *hubv1.HubShardStorageRecord) error {
		s.State = stateDraining
		s.DrainingSinceMs = time.Now().UnixMilli()
		return nil
	}, time.Minute)

	// DS 仍上报 ready,不应把 draining 降级回 ready。
	res, err := uc.Heartbeat(ctx, "hub-x", 0, "ready", time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("heartbeat err: %v", err)
	}
	if res.Command != commandDrain {
		t.Fatalf("draining shard want drain command, got %q", res.Command)
	}
	if res.GraceSeconds != 45 {
		t.Fatalf("want grace 45, got %d", res.GraceSeconds)
	}
	s, _, _ := repo.GetShard(ctx, "hub-x")
	if s.State != stateDraining {
		t.Fatalf("DS ready report must not downgrade draining, got %q", s.State)
	}
}

func TestHeartbeat_ReviveLivenessDrainOnHealthyReport(t *testing.T) {
	uc, repo, _ := newConsolidationUsecase(45)
	ctx := context.Background()
	seedShard(repo, "hub-x", 1, 0)
	// 心跳超时误标的 draining:draining_since_ms==0(非强制整合意图)。
	_ = repo.UpdateShardWithLock(ctx, "hub-x", 1, func(s *hubv1.HubShardStorageRecord) error {
		s.State = stateDraining
		s.DrainingSinceMs = 0
		return nil
	}, time.Minute)

	// 健康 DS 上报 ready → 应复位 ready 并回 no command(打断死锁)。
	res, err := uc.Heartbeat(ctx, "hub-x", 3, "ready", time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("heartbeat err: %v", err)
	}
	if res.Command != commandNone {
		t.Fatalf("revived shard want no command, got %q", res.Command)
	}
	s, _, _ := repo.GetShard(ctx, "hub-x")
	if s.State != stateReady {
		t.Fatalf("liveness-drain shard should revive to ready, got %q", s.State)
	}
}

func TestHeartbeat_ReviveAfterSweepFalsePositive(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()
	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("assign err: %v", err)
	}
	pod := "pandora-hub-global-1"
	// ① 构造一个陈旧的 Redis 派生索引 → sweep 标 draining(draining_since_ms 保持 0)。
	staleTs := time.Now().Add(-1 * time.Hour).UnixMilli()
	if _, err := uc.Heartbeat(ctx, pod, 1, "ready", staleTs, 0); err != nil {
		t.Fatalf("stale heartbeat err: %v", err)
	}
	repo.setHeartbeatTime(pod, staleTs)
	if err := uc.sweepOnce(ctx); err != nil {
		t.Fatalf("sweepOnce err: %v", err)
	}
	if s, _, _ := repo.GetShard(ctx, pod); s.State != stateDraining {
		t.Fatalf("sweep should mark stale shard draining, got %q", s.State)
	}
	// ② DS 其实还活着,下一跳按时上报 ready → 应复位 ready 且不再收到 drain。
	res, err := uc.Heartbeat(ctx, pod, 1, "ready", time.Now().UnixMilli(), 0)
	if err != nil {
		t.Fatalf("fresh heartbeat err: %v", err)
	}
	if res.Command != commandNone {
		t.Fatalf("revived shard want no command, got %q", res.Command)
	}
	s, _, _ := repo.GetShard(ctx, pod)
	if s.State != stateReady {
		t.Fatalf("healthy heartbeat should revive shard to ready, got %q", s.State)
	}
}

func TestReconcile_LogicalGraceCannotErasePhysicalOwnerFence(t *testing.T) {
	uc, repo, _ := newConsolidationUsecase(30)
	ctx := context.Background()

	// Logical empty+grace is not GameServer/Pod teardown proof.
	seedShard(repo, "hub-old", 1, 0)
	_ = repo.UpdateShardWithLock(ctx, "hub-old", 1, func(s *hubv1.HubShardStorageRecord) error {
		s.State = stateDraining
		s.DrainingSinceMs = time.Now().Add(-1 * time.Hour).UnixMilli() // 远超 grace
		return nil
	}, time.Minute)

	if err := uc.reconcileFleetReplicas(ctx); err != nil {
		t.Fatalf("reconcile err: %v", err)
	}
	if _, found, _ := repo.GetShard(ctx, "hub-old"); !found {
		t.Fatal("logical grace erased the physical-owner fence")
	}
	if replicas, err := uc.scaler.GetFleetReplicas(ctx); err != nil || replicas != 3 {
		t.Fatalf("unproven teardown scaled Fleet in: replicas=%d err=%v", replicas, err)
	}
}

func TestReconcile_KeepsDrainedShardWithinGrace(t *testing.T) {
	uc, repo, _ := newConsolidationUsecase(30)
	ctx := context.Background()

	// draining 已排空但未过 grace → 保持存活(让在场玩家完成倒计时切换)。
	seedShard(repo, "hub-young", 1, 0)
	_ = repo.UpdateShardWithLock(ctx, "hub-young", 1, func(s *hubv1.HubShardStorageRecord) error {
		s.State = stateDraining
		s.DrainingSinceMs = time.Now().UnixMilli()
		return nil
	}, time.Minute)

	if err := uc.reconcileFleetReplicas(ctx); err != nil {
		t.Fatalf("reconcile err: %v", err)
	}
	if _, found, _ := repo.GetShard(ctx, "hub-young"); !found {
		t.Fatal("drained shard within grace should NOT be reclaimed yet")
	}
}

// 大厅没人时,超出 min_replicas 的空 ready 分片必须被标 draining + 盖戳(可回收),
// 而不是直接缩 Fleet 留下不可回收的 stale 镜像。
func TestReconcile_ZeroPlayersDrainsEmptySurplusForReclaim(t *testing.T) {
	uc, repo, _ := newConsolidationUsecase(30)
	ctx := context.Background()

	// 三个空 ready 分片,总在线=0,min_replicas=1 → 保留 shard_id 最小的 1 个,排空其余 2 个。
	seedShard(repo, "hub-1", 1, 0)
	seedShard(repo, "hub-2", 2, 0)
	seedShard(repo, "hub-3", 3, 0)

	if err := uc.reconcileFleetReplicas(ctx); err != nil {
		t.Fatalf("reconcile err: %v", err)
	}

	// 保底分片 hub-1 保持 ready。
	s1, _, _ := repo.GetShard(ctx, "hub-1")
	if s1.State != stateReady {
		t.Fatalf("kept shard hub-1 should stay ready, got %q", s1.State)
	}
	// 多余空分片 hub-2 / hub-3 被标 draining 且盖戳(否则缩 Fleet 后镜像不可回收)。
	for _, pod := range []string{"hub-2", "hub-3"} {
		s, _, _ := repo.GetShard(ctx, pod)
		if s.State != stateDraining {
			t.Fatalf("surplus empty shard %s should be draining, got %q", pod, s.State)
		}
		if s.DrainingSinceMs == 0 {
			t.Fatalf("surplus empty shard %s must stamp DrainingSinceMs for reclaim", pod)
		}
	}
}

// ── 玩家侧:线路列表 + 主动切线 ────────────────────────────────────────────────

// fakeLocator 是 data.HubLocationChecker 的测试替身。
type fakeLocator struct {
	blocked bool
	err     error

	refreshedPod string   // 最近一次 RefreshHubLocations 的 pod
	refreshedIDs []uint64 // 最近一次 RefreshHubLocations 的 player_ids
}

func (f *fakeLocator) InBattleOrMatching(context.Context, uint64) (bool, error) {
	return f.blocked, f.err
}

func (f *fakeLocator) RefreshHubLocations(_ context.Context, hubPod string, playerIDs []uint64, _ string) (int, error) {
	if f.err != nil {
		return 0, f.err
	}
	f.refreshedPod = hubPod
	f.refreshedIDs = playerIDs
	return len(playerIDs), nil
}

var _ data.HubLocationChecker = (*fakeLocator)(nil)

// 线路号 = region 内 ready 分片按 shard_id 升序的 1-based 序号;当前线路/满员正确标注。
func TestListHubLinesForPlayer_OrderAndCurrent(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()

	// 乱序播三条 ready 线路 + 玩家在 shard 2(满员)。
	seedShard(repo, "hub-c", 3, 10)
	seedShard(repo, "hub-a", 1, 5)
	seedShard(repo, "hub-b", 2, 500)
	seedPlayer(repo, 1001, "hub-b", 2)

	lines, err := uc.ListHubLinesForPlayer(ctx, 1001, "")
	if err != nil {
		t.Fatalf("list err: %v", err)
	}
	if len(lines) != 3 {
		t.Fatalf("want 3 lines, got %d", len(lines))
	}
	// 按 shard_id 升序编号:1线→shard1, 2线→shard2, 3线→shard3。
	for i, wantShard := range []uint32{1, 2, 3} {
		if lines[i].LineNo != uint32(i+1) || lines[i].ShardID != wantShard {
			t.Fatalf("line[%d] = {no=%d shard=%d}, want {no=%d shard=%d}",
				i, lines[i].LineNo, lines[i].ShardID, i+1, wantShard)
		}
	}
	// 玩家在 2线 → is_current;2线 500/500 → is_full。
	if !lines[1].IsCurrent || !lines[1].IsFull {
		t.Fatalf("line 2 should be current+full, got current=%v full=%v", lines[1].IsCurrent, lines[1].IsFull)
	}
	if lines[0].IsCurrent || lines[2].IsCurrent {
		t.Fatal("only line 2 should be current")
	}
}

func TestTransferToLineForPlayer_Success(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()
	seedShard(repo, "hub-a", 1, 1)
	seedShard(repo, "hub-b", 2, 1)
	seedPlayer(repo, 1001, "hub-a", 1)

	res, err := uc.TransferToLineForPlayer(ctx, 1001, 2)
	if err != nil {
		t.Fatalf("transfer err: %v", err)
	}
	if res.NewShardID != 2 || res.LineNo != 2 {
		t.Fatalf("want shard 2 / line 2, got shard %d / line %d", res.NewShardID, res.LineNo)
	}
	if res.NewHubTicket == "" {
		t.Fatal("want new hub ticket")
	}
	a, _, _ := repo.GetAssignment(ctx, 1001)
	if a.HubPodName != "hub-b" {
		t.Fatalf("assignment not moved, pod=%s", a.HubPodName)
	}
}

func TestTransferToLineForPlayer_Cooldown(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()
	seedShard(repo, "hub-a", 1, 1)
	seedShard(repo, "hub-b", 2, 1)
	seedPlayer(repo, 1001, "hub-a", 1)

	if _, err := uc.TransferToLineForPlayer(ctx, 1001, 2); err != nil {
		t.Fatalf("first transfer err: %v", err)
	}
	// 冷却窗口内再切(此时在 hub-b,切回 shard 1)应被冷却拒绝。
	_, err := uc.TransferToLineForPlayer(ctx, 1001, 1)
	if errcode.As(err) != errcode.ErrHubTransferCooldown {
		t.Fatalf("want ErrHubTransferCooldown, got %d (err=%v)", errcode.As(err), err)
	}
}

func TestTransferToLineForPlayer_LineFull(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()
	seedShard(repo, "hub-a", 1, 1)
	seedShard(repo, "hub-b", 2, 500) // 满
	seedPlayer(repo, 1001, "hub-a", 1)

	_, err := uc.TransferToLineForPlayer(ctx, 1001, 2)
	if errcode.As(err) != errcode.ErrHubLineFull {
		t.Fatalf("want ErrHubLineFull, got %d (err=%v)", errcode.As(err), err)
	}
	// 满员失败应释放冷却占坑 → 玩家可立即改切未满线路。
	seedShard(repo, "hub-c", 3, 1)
	if _, err := uc.TransferToLineForPlayer(ctx, 1001, 3); err != nil {
		t.Fatalf("retry after full-rejection should succeed, err: %v", err)
	}
}

func TestTransferToLineForPlayer_CurrentFullLineResigns(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()
	seedShard(repo, "hub-a", 1, 500) // 当前线路已满,但玩家已经在里面
	seedPlayer(repo, 1001, "hub-a", 1)

	res, err := uc.TransferToLineForPlayer(ctx, 1001, 1)
	if err != nil {
		t.Fatalf("current full line should resign ticket, err: %v", err)
	}
	if res.NewShardID != 1 || res.LineNo != 1 || res.NewHubTicket == "" {
		t.Fatalf("unexpected resign result: shard=%d line=%d ticket_empty=%v",
			res.NewShardID, res.LineNo, res.NewHubTicket == "")
	}
}

func TestTransferToLineForPlayer_NotInHub(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()
	seedShard(repo, "hub-a", 1, 1)

	_, err := uc.TransferToLineForPlayer(ctx, 9999, 1)
	if errcode.As(err) != errcode.ErrHubTransferNotInHub {
		t.Fatalf("want ErrHubTransferNotInHub, got %d (err=%v)", errcode.As(err), err)
	}
}

func TestTransferToLineForPlayer_BattleBlocked(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	uc.SetLocationChecker(&fakeLocator{blocked: true})
	ctx := context.Background()
	seedShard(repo, "hub-a", 1, 1)
	seedShard(repo, "hub-b", 2, 1)
	seedPlayer(repo, 1001, "hub-a", 1)

	_, err := uc.TransferToLineForPlayer(ctx, 1001, 2)
	if errcode.As(err) != errcode.ErrHubTransferNotInHub {
		t.Fatalf("want ErrHubTransferNotInHub (battle block), got %d (err=%v)", errcode.As(err), err)
	}
	// 战斗护栏挡下不占冷却 → 战斗结束后可立即切。
	uc.SetLocationChecker(&fakeLocator{blocked: false})
	if _, err := uc.TransferToLineForPlayer(ctx, 1001, 2); err != nil {
		t.Fatalf("after battle ends transfer should succeed, err: %v", err)
	}
}

// locator 查询失败必须 fail-closed 拒绝切线且零副作用(INC-20260722-002 回归:
// 原"弱依赖告警放行"契约已废止——切线进入另一台 Hub DS,presence 不确定放行 =
// 潜在双 DS;§9.22 UNKNOWN 不得授权新归属)。
func TestTransferToLineForPlayer_LocatorErrorFailsClosed(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	uc.SetLocationChecker(&fakeLocator{err: errcode.New(errcode.ErrUnavailable, "locator down")})
	ctx := context.Background()
	seedShard(repo, "hub-a", 1, 1)
	seedShard(repo, "hub-b", 2, 1)
	seedPlayer(repo, 1001, "hub-a", 1)

	if _, err := uc.TransferToLineForPlayer(ctx, 1001, 2); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("locator error must fail-closed with retryable ErrUnavailable, got %v", err)
	}
	// 零副作用:拒绝发生在冷却占坑之前 → locator 恢复后立即可切,不吃冷却窗口。
	uc.SetLocationChecker(&fakeLocator{blocked: false})
	if _, err := uc.TransferToLineForPlayer(ctx, 1001, 2); err != nil {
		t.Fatalf("after locator recovers transfer should succeed immediately, err: %v", err)
	}
}

// nil checker = dev 联调模式(locator 未配):护栏跳过但放行留痕(生产装配缺失属部署
// 错误,由 Warn 日志暴露;彻底关死双 DS 仍需 Owner Authority 接线)。
func TestTransferToLineForPlayer_NilCheckerDevModeAllows(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	ctx := context.Background()
	seedShard(repo, "hub-a", 1, 1)
	seedShard(repo, "hub-b", 2, 1)
	seedPlayer(repo, 1001, "hub-a", 1)

	if _, err := uc.TransferToLineForPlayer(ctx, 1001, 2); err != nil {
		t.Fatalf("nil checker (dev mode) should allow transfer, err: %v", err)
	}
}

// ── R7 收口(P0-4):TransferToLine 临界区会话终检 ────────────────────────────────

// transferHeader/transferTransport:伪造 Kratos server transport,携带 Envoy 验签后
// 的 x-pandora-jwt-payload 头(base64url JSON),模拟玩家侧请求的会话自证。
type transferHeader map[string][]string

func (h transferHeader) Get(key string) string {
	if v := h[key]; len(v) > 0 {
		return v[0]
	}
	return ""
}
func (h transferHeader) Set(key, value string)      { h[key] = []string{value} }
func (h transferHeader) Add(key, value string)      { h[key] = append(h[key], value) }
func (h transferHeader) Values(key string) []string { return h[key] }
func (h transferHeader) Keys() []string {
	out := make([]string, 0, len(h))
	for k := range h {
		out = append(out, k)
	}
	return out
}

type transferTransport struct{ header transferHeader }

func (t *transferTransport) Kind() transport.Kind { return transport.KindGRPC }
func (t *transferTransport) Endpoint() string     { return "" }
func (t *transferTransport) Operation() string {
	return "/pandora.hub.v1.HubAllocatorService/TransferToLine"
}
func (t *transferTransport) RequestHeader() transport.Header { return t.header }
func (t *transferTransport) ReplyHeader() transport.Header   { return transferHeader{} }

// transferCtxWithJTI 构造带会话自证 jti 的玩家侧请求上下文。
func transferCtxWithJTI(jti string) context.Context {
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"1001","jti":"` + jti + `"}`))
	h := transferHeader{}
	h.Set(pmw.MetadataKeyJWTPayload, payload)
	return transport.NewServerContext(context.Background(), &transferTransport{header: h})
}

// 旧会话请求(caller jti 已被顶)必须在任何不可逆副作用前被拒:assignment 不动。
// RPC 入口中间件检查后到内部占坑/CAS 之间的窗口,由本临界区终检关闭。
func TestTransferToLine_SupersededCallerRejectedBeforeSideEffects(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	seedShard(repo, "hub-a", 1, 1)
	seedShard(repo, "hub-b", 2, 1)
	seedPlayer(repo, 1001, "hub-a", 1)
	uc.SetSessionGate(&ackFakeSessionGate{jti: "jti-new", found: true})

	ctx := transferCtxWithJTI("jti-old")
	_, err := uc.TransferToLineForPlayer(ctx, 1001, 2)
	if errcode.As(err) != errcode.ErrSessionSuperseded {
		t.Fatalf("superseded caller must be rejected, code=%v err=%v", errcode.As(err), err)
	}
	if a, _, _ := repo.GetAssignment(context.Background(), 1001); a.GetHubPodName() != "hub-a" {
		t.Fatalf("assignment must not move for a superseded caller, pod=%s", a.GetHubPodName())
	}
}

// 会话权威不可达 → fail-closed 拒绝,零副作用(§9.22:UNKNOWN 不得授权归属变更)。
func TestTransferToLine_GateOutageFailClosed(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	seedShard(repo, "hub-a", 1, 1)
	seedShard(repo, "hub-b", 2, 1)
	seedPlayer(repo, 1001, "hub-a", 1)
	uc.SetSessionGate(&ackFakeSessionGate{err: errors.New("redis down")})

	ctx := transferCtxWithJTI("jti-cur")
	if _, err := uc.TransferToLineForPlayer(ctx, 1001, 2); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("gate outage must fail closed, code=%v err=%v", errcode.As(err), err)
	}
	if a, _, _ := repo.GetAssignment(context.Background(), 1001); a.GetHubPodName() != "hub-a" {
		t.Fatalf("assignment must not move on gate outage, pod=%s", a.GetHubPodName())
	}
}

// 现行会话正常放行(终检不误杀);CAS 后复核第二次调用同代通过。
func TestTransferToLine_CurrentCallerAllowed(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	seedShard(repo, "hub-a", 1, 1)
	seedShard(repo, "hub-b", 2, 1)
	seedPlayer(repo, 1001, "hub-a", 1)
	uc.SetSessionGate(&ackFakeSessionGate{jti: "jti-cur", found: true})

	ctx := transferCtxWithJTI("jti-cur")
	res, err := uc.TransferToLineForPlayer(ctx, 1001, 2)
	if err != nil {
		t.Fatalf("current caller transfer err: %v", err)
	}
	if res.NewShardID != 2 || res.NewHubTicket == "" {
		t.Fatalf("want shard 2 with ticket, got %+v", res)
	}
}

// 确定性交错:前置终检通过后、CAS 落地前发生顶号 → post-check 检出,扣留票据,
// 并把路由副作用条件回退到原线路(R9 复审 P0-6):旧会话的失败请求不得把新会话的
// 归属留在目标线路。
func TestTransferToLine_PostCASRotationWithholdsTicket(t *testing.T) {
	uc, repo, _ := newTestUsecase(500, 3)
	seedShard(repo, "hub-a", 1, 1)
	seedShard(repo, "hub-b", 2, 1)
	seedPlayer(repo, 1001, "hub-a", 1)
	// 第一次(前置终检)返回 jti-old(现行) → 通过;第二次(post-check)返回 jti-new。
	uc.SetSessionGate(&ackFakeSessionGate{queue: []string{"jti-old", "jti-new"}, jti: "jti-new", found: true})

	ctx := transferCtxWithJTI("jti-old")
	res, err := uc.TransferToLineForPlayer(ctx, 1001, 2)
	if errcode.As(err) != errcode.ErrSessionSuperseded {
		t.Fatalf("post-CAS rotation must withhold the ticket, res=%+v code=%v err=%v",
			res, errcode.As(err), err)
	}
	if res != nil {
		t.Fatalf("no ticket may be delivered to a superseded caller, got %+v", res)
	}
	if a, _, _ := repo.GetAssignment(context.Background(), 1001); a.GetHubPodName() != "hub-a" {
		t.Fatalf("routing side effect must be reverted to the original line, pod=%s", a.GetHubPodName())
	}
}
