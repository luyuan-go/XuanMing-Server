// hub_modelb_test.go — Model B「Redis 唯一授权权威」biz 层**端到端**集成测试(真实 miniredis)。
//
// 审核二轮:废弃内存 fakeAuthRepo,改用真实 data.NewRedisHubRepo + data.NewRedisHubAuthRepo 跑在
// 同一 miniredis(authKey/shardKey 同 {pod} slot),让 biz 心跳 / 分配链路真正走 ActivateHeartbeat /
// ReserveRoutableSeat / CheckRoutable 的原子事务代码,而不是被 fake 短路掉正确性。
//
// 覆盖:
//   - 心跳激活线性化点:首个合法凭据心跳把分片 warming→ready + promote + 投影 last_verified,回显 gen。
//   - Model B 下 legacy 心跳(cred==nil)一律拒(CE1/CE2 删除 legacy 回退)。
//   - 心跳 stale fail-closed:旧凭据不刷镜像、不翻 ready。
//   - AssignHub 未激活分片拒(fail-closed);激活后放行 + 归属钉 active 元组。
//   - AssignHub 复用路径:实例漂移(active 元组变化)使旧归属失效并重分配。
package biz

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"

	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/data"
)

const (
	modelBAuthTTL         = 48 * time.Hour
	modelBTestWriterEpoch = uint32(2)
)

// newModelBUsecase 用真实 miniredis 装配 Model B 用例:HubUsecase.repo = RedisHubRepo,
// authRepo = RedisHubAuthRepo,同一 rdb。返回 uc + 两个真实仓 + mr。
func newModelBUsecase(t *testing.T, capacity int32, shardCount int) (*HubUsecase, *data.RedisHubRepo, *data.RedisHubAuthRepo, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis run: %v", err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	cfg := testConf()
	cfg.DefaultCapacity = capacity
	cfg.MockShardCount = shardCount
	repo := data.NewRedisHubRepo(rdb)
	authRepo := data.NewRedisHubAuthRepo(rdb)
	uc := NewHubUsecase(repo, NewMockHubFleetProvider(cfg), &fakeSigner{}, cfg)
	uc.SetAuthRepo(authRepo)
	uc.SetAuthTTL(modelBAuthTTL)
	return uc, repo, authRepo, mr
}

// seedWarming 在真实仓里播种一个 warming 分片(模拟拓扑种子)。
func seedWarming(t *testing.T, repo *data.RedisHubRepo, pod string, shardID uint32, capacity int32, nowMs int64) {
	t.Helper()
	rec := &hubv1.HubShardStorageRecord{
		HubPodName:      pod,
		HubAddr:         "127.0.0.1:7778",
		Region:          "global",
		ShardId:         shardID,
		Capacity:        capacity,
		State:           "warming",
		LastHeartbeatMs: nowMs,
		CreatedAtMs:     nowMs,
	}
	if err := repo.CreateShard(context.Background(), rec, modelBAuthTTL); err != nil {
		t.Fatalf("seed warming shard: %v", err)
	}
}

// activate 走 biz 心跳把某分片激活(init+stage 授权 → HeartbeatWithCredential → warming→ready + promote)。
// 纪元由 InitAuth 权威分配(首建=1,换 uid 递增),返回实际纪元供断言。
func activate(t *testing.T, uc *HubUsecase, authRepo *data.RedisHubAuthRepo, pod, uid string, gen uint64, jti string, nowMs int64) uint32 {
	t.Helper()
	ctx := context.Background()
	rec, err := authRepo.InitAuth(ctx, pod, uid, modelBAuthTTL)
	if err != nil {
		t.Fatalf("init auth: %v", err)
	}
	epoch := rec.ProtocolEpoch
	cred := &hubv1.HubDSCredential{Gen: gen, Jti: jti, ExpMs: 2_000_000_000_000, Kid: "kid-test", InstanceUid: uid, ProtocolEpoch: epoch, TokenSha256: "sha-" + jti, WriterEpoch: modelBTestWriterEpoch}
	if _, err := authRepo.StagePending(ctx, pod, cred, modelBAuthTTL); err != nil {
		t.Fatalf("stage pending: %v", err)
	}
	res, err := uc.HeartbeatWithCredential(ctx, pod, 0, nil, uint32(uc.cfg.DefaultCapacity), "ready", nowMs,
		&HubCredential{InstanceUID: uid, ProtocolEpoch: epoch, Gen: gen, JTI: jti, TokenSHA256: "sha-" + jti, Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch})
	if err != nil {
		t.Fatalf("activate heartbeat: %v", err)
	}
	if res.AcceptedTokenGen != gen || res.AcceptedTokenJTI != jti || res.AcceptedInstanceUID != uid ||
		res.AcceptedProtocolEpoch != epoch || res.AcceptedWriterEpoch != modelBTestWriterEpoch {
		t.Fatalf("activate must echo the complete credential identity, got %+v", res)
	}
	return epoch
}

func modelBPlayerIDs(count int) []uint64 {
	ids := make([]uint64, count)
	for i := range ids {
		ids[i] = uint64(i + 1)
	}
	return ids
}

// seedResetWarming 把已存在的分片镜像强制回 warming 并清 last_verified(模拟新 DS 实例接管前的种子态)。
func seedResetWarming(t *testing.T, repo *data.RedisHubRepo, pod string, nowMs int64) {
	t.Helper()
	if err := repo.UpdateShardWithLock(context.Background(), pod, 8, func(s *hubv1.HubShardStorageRecord) error {
		s.State = "warming"
		s.LastVerifiedGen = 0
		s.LastVerifiedJti = ""
		s.GameserverUid = ""
		s.AuthEpoch = 0
		s.LastVerifiedWriterEpoch = 0
		s.LastHeartbeatMs = nowMs
		return nil
	}, modelBAuthTTL); err != nil {
		t.Fatalf("reset warming: %v", err)
	}
}

// ── 心跳激活线性化点 ────────────────────────────────────────────────────────

func TestModelB_HeartbeatActivatesShard(t *testing.T) {
	uc, repo, authRepo, _ := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const pod = "pandora-hub-global-1"
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)

	activate(t, uc, authRepo, pod, "uid-A", 9, "j9", now)

	s, _, _ := repo.GetShard(ctx, pod)
	if s.State != "ready" {
		t.Fatalf("first credential heartbeat must flip warming→ready, got %s", s.State)
	}
	if s.LastVerifiedGen != 9 || s.LastVerifiedJti != "j9" || s.GameserverUid != "uid-A" || s.AuthEpoch != 1 {
		t.Fatalf("shard last_verified must be projected by active tuple, got %+v", s)
	}
	auth, _, _ := authRepo.GetAuth(ctx, pod)
	if auth.Active == nil || auth.Active.Gen != 9 || auth.Pending != nil || auth.Phase != hubv1.HubAuthPhase_HUB_AUTH_PHASE_ACTIVE {
		t.Fatalf("auth must be promoted to active gen9, got %+v", auth)
	}
}

func TestModelB_HeartbeatRejectsLegacyNoCredential(t *testing.T) {
	uc, repo, _, _ := newModelBUsecase(t, 500, 1)
	const pod = "pandora-hub-global-1"
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)

	// authRepo 已装配(Model B),但心跳不带凭据(legacy)→ 一律拒(CE1/CE2)。
	_, err := uc.Heartbeat(context.Background(), pod, 3, "ready", now, 0)
	if errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("model B heartbeat without credential must be rejected, got %v", err)
	}
	// 分片仍 warming(未被无凭据心跳翻 ready)。
	s, _, _ := repo.GetShard(context.Background(), pod)
	if s.State != "warming" {
		t.Fatalf("legacy heartbeat must not flip shard, got %s", s.State)
	}
}

func TestModelB_HeartbeatStaleFailClosed(t *testing.T) {
	uc, repo, authRepo, _ := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const pod = "pandora-hub-global-1"
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	activate(t, uc, authRepo, pod, "uid-A", 9, "j9", now)

	// 旧代际凭据(gen=3)→ fail-closed,分片不被刷。
	_, err := uc.HeartbeatWithCredential(ctx, pod, 99, modelBPlayerIDs(99), uint32(uc.cfg.DefaultCapacity), "ready", now+1000,
		&HubCredential{InstanceUID: "uid-A", ProtocolEpoch: 1, Gen: 3, JTI: "j3", TokenSHA256: "sha-j3", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch})
	if errcode.As(err) != errcode.ErrUnauthorized {
		t.Fatalf("stale credential heartbeat must fail-closed, got %v", err)
	}
	s, _, _ := repo.GetShard(ctx, pod)
	if s.PlayerCount == 99 {
		t.Fatalf("stale heartbeat must not mutate player_count")
	}
	if s.LastVerifiedGen != 9 {
		t.Fatalf("stale heartbeat must not re-project last_verified, got %d", s.LastVerifiedGen)
	}
}

// ── AssignHub 原子授权终态门 ────────────────────────────────────────────────

func TestModelB_AssignBlockedWithoutActivation(t *testing.T) {
	uc, repo, _, _ := newModelBUsecase(t, 500, 1)
	const pod = "pandora-hub-global-1"
	now := time.Now().UnixMilli()
	// 分片存在且 ready(直接播种 ready,但从未经授权激活)→ 仍应被拒。
	rec := &hubv1.HubShardStorageRecord{
		HubPodName: pod, HubAddr: "127.0.0.1:7778", Region: "global", ShardId: 1,
		Capacity: 500, State: "ready", LastHeartbeatMs: now, CreatedAtMs: now,
	}
	if err := repo.CreateShard(context.Background(), rec, modelBAuthTTL); err != nil {
		t.Fatalf("seed ready shard: %v", err)
	}

	if _, err := uc.AssignHub(context.Background(), 1001, "global", 0, 0, 0, ""); errcode.As(err) != errcode.ErrHubNoAvailable {
		t.Fatalf("assign to never-activated shard must be blocked, got %v", err)
	}
}

func TestModelB_AssignAllowsActivatedAndBinds(t *testing.T) {
	uc, repo, authRepo, _ := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const pod = "pandora-hub-global-1"
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	epoch := activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)

	res, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, "")
	if err != nil {
		t.Fatalf("assign to activated shard must succeed: %v", err)
	}
	if res.ShardID != 1 {
		t.Fatalf("want shard 1, got %d", res.ShardID)
	}
	// 归属钉 active 元组(uid/epoch/gen)。
	a, found, _ := repo.GetAssignment(ctx, 1001)
	if !found || a.HubInstanceUid != "uid-A" || a.AuthEpoch != epoch || a.AuthGen != 42 ||
		a.AuthJti != "j42" || a.AuthWriterEpoch != modelBTestWriterEpoch || a.AssignmentId == "" {
		t.Fatalf("assignment must bind active tuple, got found=%v %+v", found, a)
	}
	// 座位被原子占用(player_count=1)。
	s, _, _ := repo.GetShard(ctx, pod)
	if s.PlayerCount != 1 {
		t.Fatalf("reserve must seat++, got %d", s.PlayerCount)
	}
}

type concurrentBindingSigner struct {
	mu       sync.Mutex
	bindings []HubTicketBinding
}

type failingHubTicketSigner struct{}

func (*failingHubTicketSigner) SignHubTicket(uint64, uint32, HubTicketBinding) (string, int64, error) {
	return "", 0, errors.New("injected hub ticket signing failure")
}

type admissionPostCheckDriftRepo struct {
	data.HubRepo
	playerID uint64
	calls    int
	driftTo  *hubv1.HubAssignmentStorageRecord
}

func (r *admissionPostCheckDriftRepo) GetAssignment(
	ctx context.Context, playerID uint64,
) (*hubv1.HubAssignmentStorageRecord, bool, error) {
	r.calls++
	if playerID == r.playerID && r.calls == 2 {
		current, found, err := r.HubRepo.GetAssignment(ctx, playerID)
		if err != nil || !found {
			return current, found, err
		}
		if swapped, err := r.HubRepo.CompareAndSwapAssignment(ctx, playerID, current, r.driftTo, modelBAuthTTL); err != nil {
			return nil, false, err
		} else if !swapped {
			return nil, false, errors.New("injected admission assignment drift CAS lost")
		}
	}
	return r.HubRepo.GetAssignment(ctx, playerID)
}

func TestModelB_AdmissionCrossSlotDriftWaitsForPhysicalDeparture(t *testing.T) {
	uc, repo, authRepo, mr := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const pod = "pandora-hub-global-1"
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	epoch := activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)
	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatal(err)
	}
	oldAssignment, found, err := repo.GetAssignment(ctx, 1001)
	if err != nil || !found {
		t.Fatalf("old assignment found=%v err=%v", found, err)
	}
	drifted := proto.Clone(oldAssignment).(*hubv1.HubAssignmentStorageRecord)
	drifted.AssignmentId = uuid.NewString() // assignment IDs are never reused
	uc.repo = &admissionPostCheckDriftRepo{
		HubRepo: repo, playerID: 1001, driftTo: drifted,
	}
	admissionID := uuid.NewString()
	cred := &HubCredential{
		InstanceUID: "uid-A", ProtocolEpoch: epoch, Gen: 42, JTI: "j42",
		TokenSHA256: "sha-j42", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch,
	}
	_, err = uc.AcknowledgeAdmission(ctx, 1001, oldAssignment.GetAssignmentId(), pod,
		admissionID, 1, "", cred)
	if errcode.As(err) != errcode.ErrInvalidState {
		t.Fatalf("cross-slot drift code=%v err=%v", errcode.As(err), err)
	}
	current, _, _ := repo.GetAssignment(ctx, 1001)
	reservationKeys, _ := mr.HKeys("pandora:hub:reservations:{" + pod + "}")
	sessionKeys, _ := mr.HKeys("pandora:hub:sessions:{" + pod + "}")
	shard, _, _ := repo.GetShard(ctx, pod)
	if current.GetAssignmentId() != drifted.GetAssignmentId() || len(reservationKeys) != 0 ||
		len(sessionKeys) != 1 || sessionKeys[0] != oldAssignment.GetAssignmentId() ||
		shard.GetPlayerCount() != 1 {
		t.Fatalf("post-ACK drift must retain physical owner: assignment=%+v reservations=%v sessions=%v shard=%+v",
			current, reservationKeys, sessionKeys, shard)
	}
	departed, departureErr := uc.AcknowledgeDeparture(ctx, 1001, oldAssignment.GetAssignmentId(), pod,
		admissionID, 1, cred)
	if departureErr != nil || !departed.Departed || departed.Conflict {
		t.Fatalf("physical departure result=%+v err=%v", departed, departureErr)
	}
	if sessionKeys, _ = mr.HKeys("pandora:hub:sessions:{" + pod + "}"); len(sessionKeys) != 0 {
		t.Fatalf("exact physical departure left session: %v", sessionKeys)
	}
}

func TestModelB_AssignSignFailureExactReservationCleanup(t *testing.T) {
	uc, repo, authRepo, mr := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const pod = "pandora-hub-global-1"
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)
	uc.signer = &failingHubTicketSigner{}

	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("sign failure code=%v err=%v", errcode.As(err), err)
	}
	if assignment, found, err := repo.GetAssignment(ctx, 1001); err != nil || found {
		t.Fatalf("failed sign must not publish assignment: found=%v assignment=%+v err=%v", found, assignment, err)
	}
	shard, _, _ := repo.GetShard(ctx, pod)
	reservationKeys, _ := mr.HKeys("pandora:hub:reservations:{" + pod + "}")
	sessionKeys, _ := mr.HKeys("pandora:hub:sessions:{" + pod + "}")
	reservations, sessions := len(reservationKeys), len(sessionKeys)
	if shard.GetPlayerCount() != 0 || shard.GetReservedCount() != 0 || shard.GetConnectedOwnershipCount() != 0 ||
		reservations != 0 || sessions != 0 {
		t.Fatalf("failed sign leaked capacity: shard=%+v reservations=%d sessions=%d", shard,
			reservations, sessions)
	}
}

func TestModelB_TransferSignFailureKeepsOldOwnerAndCleansTargetReservation(t *testing.T) {
	uc, repo, authRepo, mr := newModelBUsecase(t, 500, 2)
	ctx := context.Background()
	const pod1, pod2 = "pandora-hub-global-1", "pandora-hub-global-2"
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod1, 1, 500, now)
	seedWarming(t, repo, pod2, 2, 500, now)
	activate(t, uc, authRepo, pod1, "uid-A", 42, "j42", now)
	activate(t, uc, authRepo, pod2, "uid-B", 52, "j52", now)
	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatal(err)
	}
	before, _, _ := repo.GetAssignment(ctx, 1001)
	uc.signer = &failingHubTicketSigner{}

	if _, err := uc.TransferHub(ctx, 1001, 2); errcode.As(err) != errcode.ErrInternal {
		t.Fatalf("transfer sign failure code=%v err=%v", errcode.As(err), err)
	}
	after, _, _ := repo.GetAssignment(ctx, 1001)
	from, _, _ := repo.GetShard(ctx, pod1)
	target, _, _ := repo.GetShard(ctx, pod2)
	targetReservationKeys, _ := mr.HKeys("pandora:hub:reservations:{" + pod2 + "}")
	targetSessionKeys, _ := mr.HKeys("pandora:hub:sessions:{" + pod2 + "}")
	targetReservations, targetSessions := len(targetReservationKeys), len(targetSessionKeys)
	if !proto.Equal(after, before) || from.GetPlayerCount() != 1 || target.GetPlayerCount() != 0 ||
		targetReservations != 0 || targetSessions != 0 {
		t.Fatalf("failed transfer sign mutated owner/leaked target: before=%+v after=%+v from=%+v target=%+v",
			before, after, from, target)
	}
}

func (s *concurrentBindingSigner) SignHubTicket(_ uint64, _ uint32, binding HubTicketBinding) (string, int64, error) {
	s.mu.Lock()
	s.bindings = append(s.bindings, binding)
	s.mu.Unlock()
	return binding.HubAssignmentID, time.Now().Add(time.Minute).UnixMilli(), nil
}

func TestModelB_ConcurrentAssignSingleSeatAndBinding(t *testing.T) {
	uc, repo, authRepo, _ := newModelBUsecase(t, 500, 1)
	signer := &concurrentBindingSigner{}
	uc.signer = signer
	const pod = "pandora-hub-global-1"
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)

	const workers = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	ticketCh := make(chan string, workers)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			<-start
			result, err := uc.AssignHub(context.Background(), 1001, "global", 0, 0, 0, "")
			if result != nil {
				ticketCh <- result.HubTicket
			}
			errCh <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	close(ticketCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent AssignHub: %v", err)
		}
	}
	shard, _, _ := repo.GetShard(context.Background(), pod)
	assignment, found, err := repo.GetAssignment(context.Background(), 1001)
	if err != nil || !found || shard.PlayerCount != 1 {
		t.Fatalf("one player must own exactly one seat: count=%d assignment=%+v err=%v", shard.PlayerCount, assignment, err)
	}
	returnedTickets := 0
	for ticket := range ticketCh {
		returnedTickets++
		if ticket != assignment.GetAssignmentId() {
			t.Fatalf("returned ticket was signed for losing assignment: got=%q winner=%q", ticket, assignment.GetAssignmentId())
		}
	}
	if returnedTickets != workers {
		t.Fatalf("every successful caller must receive one ticket: got=%d want=%d", returnedTickets, workers)
	}
	signer.mu.Lock()
	defer signer.mu.Unlock()
	if len(signer.bindings) < workers {
		t.Fatalf("sign attempts cannot be fewer than successful callers: got=%d want>=%d", len(signer.bindings), workers)
	}
	// 新 reservation 必须先签后 CAS，故并发 loser 可产生不会返回的预签名；所有预签名仍须
	// 绑定同一权威 Pod/instance，只有 winning assignment 的票能返回给调用方（上面已断言）。
	for _, binding := range signer.bindings {
		if binding.HubAssignmentID == "" || binding.PodName != assignment.HubPodName ||
			binding.InstanceUID != assignment.HubInstanceUid || binding.CredentialJTI != assignment.AuthJti ||
			binding.WriterEpoch != assignment.AuthWriterEpoch {
			t.Fatalf("ticket binding drifted from winning assignment: binding=%+v assignment=%+v", binding, assignment)
		}
	}
}

// ackFakeSessionGate 是可编程的会话现行性权威 fake(R7 复审 P0-3 测试用)。
// queue 非空时每次调用依次弹出作为当前 jti(found=true,err=nil),用于模拟
// 「两次检查之间会话被并发登录轮换」的确定性交错;弹空后回落到静态字段。
type ackFakeSessionGate struct {
	jti   string
	found bool
	err   error
	queue []string
}

func (g *ackFakeSessionGate) CurrentJTI(context.Context, uint64) (string, bool, error) {
	if len(g.queue) > 0 {
		j := g.queue[0]
		g.queue = g.queue[1:]
		return j, true, nil
	}
	return g.jti, g.found, g.err
}

// R7 复审 P0-3:v2 Hub 本地验票不经 Login 在线兑换,AcknowledgeAdmission 是唯一在线
// 权威接触点。装配 session gate 后:空 sjti 硬拒、非当前代拒(顶号旧票在入场确认点
// 作废)、权威不可达 fail-closed、无会话拒;全部失败路径不得消费 reservation;
// 当前代放行。确定性交错:每一步都在固定的 gate 状态下同步断言。
func TestModelB_AcknowledgeAdmissionSessionGate(t *testing.T) {
	uc, repo, authRepo, _ := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const (
		pod      = "pandora-hub-global-1"
		playerID = uint64(1001)
	)
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	epoch := activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)
	cred := &HubCredential{
		InstanceUID: "uid-A", ProtocolEpoch: epoch, Gen: 42, JTI: "j42",
		TokenSHA256: "sha-j42", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch,
	}
	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("assign: %v", err)
	}
	assignment, found, err := repo.GetAssignment(ctx, playerID)
	if err != nil || !found {
		t.Fatalf("assignment found=%v err=%v", found, err)
	}
	gate := &ackFakeSessionGate{jti: "jti-new", found: true}
	uc.SetSessionGate(gate)
	// 本测试断言强制档语义;兼容档(默认)见 TestModelB_AcknowledgeAdmissionEmptySJTITolerant。
	uc.SetSessionGateRequireSJTI(true)

	requireNotConsumed := func(step string) {
		t.Helper()
		s, _, serr := repo.GetShard(ctx, pod)
		if serr != nil || s.GetConnectedOwnershipCount() != 0 {
			t.Fatalf("%s must not consume reservation: shard=%+v err=%v", step, s, serr)
		}
	}
	// ① 空 sjti(旧签发面残票/旧 DS 镜像)→ 强制档 ErrUnauthorized,零副作用。
	if _, aerr := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		uuid.NewString(), 1, "", cred); errcode.As(aerr) != errcode.ErrUnauthorized {
		t.Fatalf("empty sjti must be rejected, code=%v err=%v", errcode.As(aerr), aerr)
	}
	requireNotConsumed("empty sjti")
	// ② 顶号旧票:票内 sjti=jti-old,当前会话 jti-new → ErrSessionSuperseded。
	if _, aerr := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		uuid.NewString(), 2, "jti-old", cred); errcode.As(aerr) != errcode.ErrSessionSuperseded {
		t.Fatalf("superseded sjti must be rejected, code=%v err=%v", errcode.As(aerr), aerr)
	}
	requireNotConsumed("superseded sjti")
	// ③ 权威不可达 → fail-closed ErrUnavailable(可重试,不消费)。
	gate.err = errors.New("redis down")
	if _, aerr := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		uuid.NewString(), 3, "jti-new", cred); errcode.As(aerr) != errcode.ErrUnavailable {
		t.Fatalf("gate outage must fail closed, code=%v err=%v", errcode.As(aerr), aerr)
	}
	gate.err = nil
	requireNotConsumed("gate outage")
	// ④ 已登出(无会话)→ ErrUnauthorized。
	gate.found = false
	if _, aerr := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		uuid.NewString(), 4, "jti-new", cred); errcode.As(aerr) != errcode.ErrUnauthorized {
		t.Fatalf("logged-out session must be rejected, code=%v err=%v", errcode.As(aerr), aerr)
	}
	gate.found = true
	requireNotConsumed("logged out")
	// ⑤ 当前代票放行,reservation 正常转 connected owner。
	if admitted, aerr := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		uuid.NewString(), 5, "jti-new", cred); aerr != nil || !admitted.Admitted {
		t.Fatalf("current-session admission result=%+v err=%v", admitted, aerr)
	}
}

// R7 收口(P0-5)混版兼容档:require_ticket_sjti 默认 false 时,空 sjti(旧 Hub DS 不
// 转发/旧签发面残票)告警放行,ACK 正常消费 reservation——滚动窗口内旧 DS 上的玩家
// 不被误拒。非空 sjti 仍全量复核(上面的强制档测试覆盖)。
func TestModelB_AcknowledgeAdmissionEmptySJTITolerant(t *testing.T) {
	uc, repo, authRepo, _ := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const (
		pod      = "pandora-hub-global-1"
		playerID = uint64(1001)
	)
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	epoch := activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)
	cred := &HubCredential{
		InstanceUID: "uid-A", ProtocolEpoch: epoch, Gen: 42, JTI: "j42",
		TokenSHA256: "sha-j42", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch,
	}
	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("assign: %v", err)
	}
	assignment, found, err := repo.GetAssignment(ctx, playerID)
	if err != nil || !found {
		t.Fatalf("assignment found=%v err=%v", found, err)
	}
	uc.SetSessionGate(&ackFakeSessionGate{jti: "jti-new", found: true})
	// 默认兼容档(不调用 SetSessionGateRequireSJTI):空 sjti 放行并正常入场。
	admitted, aerr := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		uuid.NewString(), 1, "", cred)
	if aerr != nil || !admitted.Admitted {
		t.Fatalf("tolerant profile must admit empty-sjti ticket: result=%+v err=%v", admitted, aerr)
	}
}

// R7 收口(P0-2)确定性交错:预检通过后、durable 消费 reservation 完成之间发生顶号轮换。
// durable 写后复核必须检出 → exact 回退 connected owner + ErrSessionSuperseded,旧会话
// 永远拿不到 spawn gate;曾短暂建立的 seat 不残留容量。
// R11 复审 P0-6 确定性交错:**成功响应形成之后**发生轮换。
//
// 这一条与下面那条不同,针对的是 DS 的「开门前复核」——DS 在真正开 spawn gate 之前会用
// **同一 (admission_id, seq)** 幂等重放一次 ACK。危险在于:ledger 对同 identity 的 ACK 是
// 幂等的(AlreadyAdmitted),如果重放路径直接返回"已准入"而不重跑会话复核,那么
// 「首次 ACK 成功 → 玩家会话被顶 → DS 重放复核 → 拿到 Admitted=true → 开门」
// 就会让**已被顶号的会话拿到可操作 Pawn**(违反数据完整性与一人一 DS)。
//
// 断言:重放必须重跑复核并返回 SessionSuperseded,绝不因幂等而放行。
func TestModelB_AcknowledgeAdmissionReplayAfterRotationStillRefused(t *testing.T) {
	uc, repo, authRepo, _ := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const (
		pod      = "pandora-hub-global-1"
		playerID = uint64(1001)
	)
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	epoch := activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)
	cred := &HubCredential{
		InstanceUID: "uid-A", ProtocolEpoch: epoch, Gen: 42, JTI: "j42",
		TokenSHA256: "sha-j42", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch,
	}
	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("assign: %v", err)
	}
	assignment, found, err := repo.GetAssignment(ctx, playerID)
	if err != nil || !found {
		t.Fatalf("assignment found=%v err=%v", found, err)
	}

	// 首次 ACK:预检与写后复核都拿到票据代 jti-old → 成功(此刻"成功响应已形成")。
	// 随后玩家被顶号:重放时两次复核都拿到 jti-new。
	gate := &ackFakeSessionGate{queue: []string{"jti-old", "jti-old", "jti-new", "jti-new"}}
	uc.SetSessionGate(gate)

	admissionID := uuid.NewString()
	first, aerr := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		admissionID, 1, "jti-old", cred)
	if aerr != nil || first == nil || !first.Admitted {
		t.Fatalf("首次 ACK 应成功(成功响应形成):result=%+v err=%v", first, aerr)
	}

	// DS 开门前用**同一 identity** 重放。轮换已发生 → 必须拒,不能幂等放行。
	replay, rerr := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		admissionID, 1, "jti-old", cred)
	if errcode.As(rerr) != errcode.ErrSessionSuperseded {
		t.Fatalf("开门前重放遇轮换必须被拒(否则被顶号会话拿到可操作 Pawn):result=%+v code=%v err=%v",
			replay, errcode.As(rerr), rerr)
	}
	if replay != nil && replay.Admitted {
		t.Fatal("重放绝不能因 ledger 幂等而返回 Admitted=true")
	}
}

func TestModelB_AcknowledgeAdmissionPostWriteRotationReverted(t *testing.T) {
	uc, repo, authRepo, _ := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const (
		pod      = "pandora-hub-global-1"
		playerID = uint64(1001)
	)
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	epoch := activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)
	cred := &HubCredential{
		InstanceUID: "uid-A", ProtocolEpoch: epoch, Gen: 42, JTI: "j42",
		TokenSHA256: "sha-j42", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch,
	}
	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("assign: %v", err)
	}
	assignment, found, err := repo.GetAssignment(ctx, playerID)
	if err != nil || !found {
		t.Fatalf("assignment found=%v err=%v", found, err)
	}
	// 第一次调用(预检)返回 jti-old(票据代,现行) → 通过;第二次调用(durable 写后
	// 复核)返回 jti-new → 轮换发生在消费点之后、开门之前,必须回退。
	gate := &ackFakeSessionGate{queue: []string{"jti-old", "jti-new"}}
	uc.SetSessionGate(gate)

	admitted, aerr := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		uuid.NewString(), 1, "jti-old", cred)
	if errcode.As(aerr) != errcode.ErrSessionSuperseded {
		t.Fatalf("post-write rotation must be superseded, result=%+v code=%v err=%v",
			admitted, errcode.As(aerr), aerr)
	}
	s, _, serr := repo.GetShard(ctx, pod)
	if serr != nil || s.GetConnectedOwnershipCount() != 0 {
		t.Fatalf("reverted admission must not leave a connected owner: shard=%+v err=%v", s, serr)
	}
	// 复核不可判定(权威不可达)fail-closed 拒绝开门,但**不回退** owner(R9 复审 P1):
	// 普通 reservation 已被消费且 Departure 不恢复它,回退会把重试变成死路。owner 保留 +
	// ledger 幂等重认,权威恢复后 DS 以同一 identity 重试即可拿到确定结果。
	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("re-assign: %v", err)
	}
	assignment, found, err = repo.GetAssignment(ctx, playerID)
	if err != nil || !found {
		t.Fatalf("re-assignment found=%v err=%v", found, err)
	}
	gate.queue = []string{"jti-old"} // 预检过;post-check 弹空回落到静态 err
	gate.err = errors.New("redis down")
	retryAdmissionID := uuid.NewString()
	if _, aerr := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		retryAdmissionID, 2, "jti-old", cred); errcode.As(aerr) != errcode.ErrUnavailable {
		t.Fatalf("post-check outage must fail closed, code=%v err=%v", errcode.As(aerr), aerr)
	}
	if s, _, serr := repo.GetShard(ctx, pod); serr != nil || s.GetConnectedOwnershipCount() != 1 {
		t.Fatalf("outage path must retain the owner for an idempotent retry: shard=%+v err=%v", s, serr)
	}
	// 权威恢复后同 identity 重试:幂等重认 + 两次复核通过 → 开门。
	gate.queue = nil
	gate.err = nil
	gate.jti, gate.found = "jti-old", true
	retried, rerr := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		retryAdmissionID, 2, "jti-old", cred)
	if rerr != nil || retried == nil || !retried.Admitted {
		t.Fatalf("same-identity retry after authority recovery must admit, result=%+v err=%v", retried, rerr)
	}
}

func TestModelB_AssignIdempotentReuse(t *testing.T) {
	uc, repo, authRepo, _ := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const pod = "pandora-hub-global-1"
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)

	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("first assign: %v", err)
	}
	// 再分配同玩家 → 复用(不重复占位):player_count 仍 1。
	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("reuse assign: %v", err)
	}
	s, _, _ := repo.GetShard(ctx, pod)
	if s.PlayerCount != 1 {
		t.Fatalf("idempotent reuse must not double-seat, got %d", s.PlayerCount)
	}
}

func TestModelB_CleanDepartureThenReloginRecreatesReservation(t *testing.T) {
	uc, repo, authRepo, mr := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const (
		pod      = "pandora-hub-global-1"
		playerID = uint64(1001)
	)
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	epoch := activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)
	cred := &HubCredential{
		InstanceUID: "uid-A", ProtocolEpoch: epoch, Gen: 42, JTI: "j42",
		TokenSHA256: "sha-j42", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch,
	}

	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("first assign: %v", err)
	}
	assignment, found, err := repo.GetAssignment(ctx, playerID)
	if err != nil || !found {
		t.Fatalf("assignment found=%v err=%v", found, err)
	}
	firstAdmissionID := uuid.NewString()
	if admitted, err := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		firstAdmissionID, 1, "", cred); err != nil || !admitted.Admitted {
		t.Fatalf("first admission result=%+v err=%v", admitted, err)
	}
	if _, err := uc.HeartbeatWithCredential(ctx, pod, 1, []uint64{playerID}, 500, "ready", now+1, cred); err != nil {
		t.Fatalf("heartbeat connected audit: %v", err)
	}
	if departed, err := uc.AcknowledgeDeparture(ctx, playerID, assignment.GetAssignmentId(), pod,
		firstAdmissionID, 1, cred); err != nil || !departed.Departed || departed.Conflict {
		t.Fatalf("clean departure result=%+v err=%v", departed, err)
	}
	reservationKey := "pandora:hub:reservations:{" + pod + "}"
	sessionKey := "pandora:hub:sessions:{" + pod + "}"
	if reservations, _ := mr.HKeys(reservationKey); len(reservations) != 0 {
		t.Fatalf("clean departure left reservations: %v", reservations)
	}
	if sessions, _ := mr.HKeys(sessionKey); len(sessions) != 0 {
		t.Fatalf("clean departure left sessions: %v", sessions)
	}
	if members, err := repo.ListShardMembers(ctx, pod); err != nil || len(members) != 1 || members[0] != playerID {
		t.Fatalf("offline stable assignment disappeared from drain index: members=%v err=%v", members, err)
	}
	// Departure 与下一次 5s heartbeat 之间，audit=1 > connected=0 必须安全暂停路由；
	// 不能为了重登在 DS 尚未证明连接已消失时超发。
	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); errcode.As(err) != errcode.ErrHubNoAvailable {
		t.Fatalf("stale connected audit must pause relogin, code=%v err=%v", errcode.As(err), err)
	}
	if _, err := uc.HeartbeatWithCredential(ctx, pod, 0, nil, 500, "ready", now+2, cred); err != nil {
		t.Fatalf("heartbeat departure audit: %v", err)
	}

	// assignment 仍用于线路粘性，但 admission ledger 已空；重签必须原子补回 reservation。
	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("relogin assign: %v", err)
	}
	current, found, err := repo.GetAssignment(ctx, playerID)
	if err != nil || !found || current.GetAssignmentId() != assignment.GetAssignmentId() {
		t.Fatalf("relogin must reuse assignment identity: current=%+v found=%v err=%v", current, found, err)
	}
	if reservations, _ := mr.HKeys(reservationKey); len(reservations) != 1 || reservations[0] != assignment.GetAssignmentId() {
		t.Fatalf("relogin must recreate exact reservation: %v", reservations)
	}
	shard, _, _ := repo.GetShard(ctx, pod)
	if shard.GetPlayerCount() != 1 || shard.GetReservedCount() != 1 || shard.GetConnectedOwnershipCount() != 0 {
		t.Fatalf("relogin reservation projection invalid: %+v", shard)
	}
	if admitted, err := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		uuid.NewString(), 2, "", cred); err != nil || !admitted.Admitted {
		t.Fatalf("relogin admission result=%+v err=%v", admitted, err)
	}
}

func TestModelB_ExpiredReservationThenReloginRefreshesLease(t *testing.T) {
	uc, repo, authRepo, mr := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const (
		pod      = "pandora-hub-global-1"
		playerID = uint64(1001)
	)
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	epoch := activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)
	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("first assign: %v", err)
	}
	assignment, found, err := repo.GetAssignment(ctx, playerID)
	if err != nil || !found {
		t.Fatalf("assignment found=%v err=%v", found, err)
	}
	reservationKey := "pandora:hub:reservations:{" + pod + "}"
	if ttl := mr.TTL(reservationKey); ttl <= 0 {
		t.Fatalf("reservation hash must have bounded retention, ttl=%s", ttl)
	}

	// Redis 整键 retention = reservation 绝对到期 + guard；推进后 assignment 仍存活。
	mr.FastForward(uc.reservationTTL() + time.Minute)
	if mr.Exists(reservationKey) {
		t.Fatalf("test precondition: expired reservation hash still exists")
	}
	if _, found, err := repo.GetAssignment(ctx, playerID); err != nil || !found {
		t.Fatalf("assignment must outlive reservation: found=%v err=%v", found, err)
	}

	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("assign after reservation expiry: %v", err)
	}
	current, _, _ := repo.GetAssignment(ctx, playerID)
	if current.GetAssignmentId() != assignment.GetAssignmentId() {
		t.Fatalf("expired lease relogin changed sticky assignment: before=%s after=%s",
			assignment.GetAssignmentId(), current.GetAssignmentId())
	}
	if reservations, _ := mr.HKeys(reservationKey); len(reservations) != 1 || reservations[0] != assignment.GetAssignmentId() {
		t.Fatalf("expired lease was not recreated exactly: %v", reservations)
	}
	cred := &HubCredential{
		InstanceUID: "uid-A", ProtocolEpoch: epoch, Gen: 42, JTI: "j42",
		TokenSHA256: "sha-j42", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch,
	}
	if admitted, err := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		uuid.NewString(), 1, "", cred); err != nil || !admitted.Admitted {
		t.Fatalf("admission after lease refresh result=%+v err=%v", admitted, err)
	}
}

func TestModelB_ActiveSessionReuseDoesNotDoubleCountOrDeleteOwner(t *testing.T) {
	uc, repo, authRepo, mr := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const (
		pod      = "pandora-hub-global-1"
		playerID = uint64(1001)
	)
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	epoch := activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)
	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("first assign: %v", err)
	}
	assignment, _, _ := repo.GetAssignment(ctx, playerID)
	admissionID := uuid.NewString()
	cred := &HubCredential{
		InstanceUID: "uid-A", ProtocolEpoch: epoch, Gen: 42, JTI: "j42",
		TokenSHA256: "sha-j42", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch,
	}
	if admitted, err := uc.AcknowledgeAdmission(ctx, playerID, assignment.GetAssignmentId(), pod,
		admissionID, 7, "", cred); err != nil || !admitted.Admitted {
		t.Fatalf("first admission result=%+v err=%v", admitted, err)
	}
	sessionKey := "pandora:hub:sessions:{" + pod + "}"
	sessionBefore := mr.HGet(sessionKey, assignment.GetAssignmentId())
	if sessionBefore == "" {
		t.Fatal("active session missing before reuse")
	}

	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("active session reuse: %v", err)
	}
	if reservations, _ := mr.HKeys("pandora:hub:reservations:{" + pod + "}"); len(reservations) != 0 {
		t.Fatalf("active session reuse created duplicate reservation: %v", reservations)
	}
	if sessions, _ := mr.HKeys(sessionKey); len(sessions) != 1 || sessions[0] != assignment.GetAssignmentId() {
		t.Fatalf("active session reuse changed ownership cardinality: %v", sessions)
	}
	if sessionAfter := mr.HGet(sessionKey, assignment.GetAssignmentId()); sessionAfter != sessionBefore {
		t.Fatal("active session reuse replaced or deleted the current admission owner")
	}
	shard, _, _ := repo.GetShard(ctx, pod)
	if shard.GetPlayerCount() != 1 || shard.GetReservedCount() != 0 || shard.GetConnectedOwnershipCount() != 1 {
		t.Fatalf("active session reuse double-counted capacity: %+v", shard)
	}
}

func TestModelB_AssignIdempotentReuseRefreshesAssignmentTTL(t *testing.T) {
	uc, repo, authRepo, mr := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const (
		pod      = "pandora-hub-global-1"
		playerID = uint64(1001)
	)
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)
	if err := repo.UpdateShardWithLock(ctx, pod, 1, func(*hubv1.HubShardStorageRecord) error { return nil }, modelBAuthTTL); err != nil {
		t.Fatalf("extend shard ttl: %v", err)
	}

	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("first assign: %v", err)
	}
	assignmentKey := "pandora:hub:player:1001"
	initialTTL := mr.TTL(assignmentKey)
	if initialTTL <= 0 {
		t.Fatalf("assignment must have positive TTL, got %s", initialTTL)
	}

	// 把归属推进到即将到期；auth/shard 使用更长测试 TTL，且 miniredis 快进不改变服务端心跳时钟。
	mr.FastForward(initialTTL - time.Second)
	if remaining := mr.TTL(assignmentKey); remaining <= 0 || remaining > time.Second {
		t.Fatalf("assignment must be near expiry before reuse, got %s", remaining)
	}
	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("reuse assign: %v", err)
	}
	if refreshed := mr.TTL(assignmentKey); refreshed < initialTTL-time.Second {
		t.Fatalf("idempotent reuse must refresh assignment TTL: got=%s initial=%s", refreshed, initialTTL)
	}
}

func TestModelB_AssignRejectsIncompleteOrFutureWriterAssignmentWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*hubv1.HubAssignmentStorageRecord)
	}{
		{name: "missing-jti", mutate: func(a *hubv1.HubAssignmentStorageRecord) { a.AuthJti = "" }},
		{name: "legacy-writer-zero", mutate: func(a *hubv1.HubAssignmentStorageRecord) { a.AuthWriterEpoch = 0 }},
		{name: "legacy-writer-one", mutate: func(a *hubv1.HubAssignmentStorageRecord) { a.AuthWriterEpoch = 1 }},
		{name: "future-writer-three", mutate: func(a *hubv1.HubAssignmentStorageRecord) { a.AuthWriterEpoch = 3 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc, repo, authRepo, _ := newModelBUsecase(t, 500, 1)
			ctx := context.Background()
			const pod = "pandora-hub-global-1"
			now := time.Now().UnixMilli()
			seedWarming(t, repo, pod, 1, 500, now)
			activate(t, uc, authRepo, pod, "uid-A", 42, "j42", now)
			if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
				t.Fatal(err)
			}
			before, found, err := repo.GetAssignment(ctx, 1001)
			if err != nil || !found {
				t.Fatalf("assignment found=%v err=%v", found, err)
			}
			poisoned := proto.Clone(before).(*hubv1.HubAssignmentStorageRecord)
			tc.mutate(poisoned)
			if swapped, err := repo.CompareAndSwapAssignment(ctx, 1001, before, poisoned, modelBAuthTTL); err != nil || !swapped {
				t.Fatalf("poison assignment swapped=%v err=%v", swapped, err)
			}
			shardBefore, _, _ := repo.GetShard(ctx, pod)
			if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); errcode.As(err) != errcode.ErrInvalidState {
				t.Fatalf("incomplete/future assignment must fail closed, code=%v err=%v", errcode.As(err), err)
			}
			after, _, _ := repo.GetAssignment(ctx, 1001)
			shardAfter, _, _ := repo.GetShard(ctx, pod)
			if !proto.Equal(after, poisoned) || shardAfter.GetPlayerCount() != shardBefore.GetPlayerCount() {
				t.Fatalf("rejected assignment mutated state: assignment=%+v shard_count=%d want=%d",
					after, shardAfter.GetPlayerCount(), shardBefore.GetPlayerCount())
			}
		})
	}
}

func TestModelB_FutureWriterAssignmentRejectedByAllConsumerPaths(t *testing.T) {
	for _, operation := range []string{"release", "transfer", "transfer-line", "list-lines", "drain-migrate"} {
		t.Run(operation, func(t *testing.T) {
			uc, repo, authRepo, mr := newModelBUsecase(t, 500, 2)
			ctx := context.Background()
			now := time.Now().UnixMilli()
			const pod1, pod2 = "pandora-hub-global-1", "pandora-hub-global-2"
			seedWarming(t, repo, pod1, 1, 500, now)
			seedWarming(t, repo, pod2, 2, 500, now)
			activate(t, uc, authRepo, pod1, "uid-A", 42, "j42", now)
			activate(t, uc, authRepo, pod2, "uid-B", 52, "j52", now)
			if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
				t.Fatal(err)
			}
			before, found, err := repo.GetAssignment(ctx, 1001)
			if err != nil || !found || before.GetHubPodName() != pod1 {
				t.Fatalf("assignment=%+v found=%v err=%v", before, found, err)
			}
			poisoned := proto.Clone(before).(*hubv1.HubAssignmentStorageRecord)
			poisoned.AuthWriterEpoch = 3
			if swapped, err := repo.CompareAndSwapAssignment(ctx, 1001, before, poisoned, modelBAuthTTL); err != nil || !swapped {
				t.Fatalf("poison assignment swapped=%v err=%v", swapped, err)
			}
			fromBefore, _, _ := repo.GetShard(ctx, pod1)
			targetBefore, _, _ := repo.GetShard(ctx, pod2)
			membersBefore, _ := repo.ListShardMembers(ctx, pod1)

			switch operation {
			case "release":
				if err := uc.ReleaseHub(ctx, 1001); errcode.As(err) != errcode.ErrInvalidState {
					t.Fatalf("future writer release code=%v err=%v", errcode.As(err), err)
				}
			case "transfer":
				if _, err := uc.TransferHub(ctx, 1001, 2); errcode.As(err) != errcode.ErrHubTransferFailed {
					t.Fatalf("future writer transfer code=%v err=%v", errcode.As(err), err)
				}
			case "transfer-line":
				if _, err := uc.TransferToLineForPlayer(ctx, 1001, 2); errcode.As(err) != errcode.ErrInvalidState {
					t.Fatalf("future writer line transfer code=%v err=%v", errcode.As(err), err)
				}
				if mr.Exists("pandora:hub:transfer_cd:1001") {
					t.Fatal("future writer rejection must happen before cooldown SET")
				}
			case "list-lines":
				if _, err := uc.ListHubLinesForPlayer(ctx, 1001, ""); errcode.As(err) != errcode.ErrInvalidState {
					t.Fatalf("future writer list lines code=%v err=%v", errcode.As(err), err)
				}
			case "drain-migrate":
				from, _, _ := repo.GetShard(ctx, pod1)
				target, _, _ := repo.GetShard(ctx, pod2)
				if uc.migratePlayer(ctx, 1001, from, target) {
					t.Fatal("future writer assignment must not be migrated")
				}
			}

			after, _, _ := repo.GetAssignment(ctx, 1001)
			fromAfter, _, _ := repo.GetShard(ctx, pod1)
			targetAfter, _, _ := repo.GetShard(ctx, pod2)
			membersAfter, _ := repo.ListShardMembers(ctx, pod1)
			if !proto.Equal(after, poisoned) || fromAfter.GetPlayerCount() != fromBefore.GetPlayerCount() ||
				targetAfter.GetPlayerCount() != targetBefore.GetPlayerCount() ||
				!slices.Equal(membersAfter, membersBefore) {
				t.Fatalf("rejected path mutated state: assignment=%+v from=%d/%d target=%d/%d", after,
					fromAfter.GetPlayerCount(), fromBefore.GetPlayerCount(),
					targetAfter.GetPlayerCount(), targetBefore.GetPlayerCount())
			}
		})
	}
}

// R7 收口(P1)迁移通知必达重试:drain 迁移已 CAS 落地(归属在目标分片、源 member 索引
// 未清)后,补发通知的会话权威不可达 → 返回 false 且**源索引必须保留**,下个 tick 重扫
// 补发;权威恢复后补发成功才清索引。旧实现先删索引,失败后玩家退出 drain 扫描,
// "下 tick 重试"永不发生 = 迁移通知永久丢失。
func TestModelB_DrainMigrateNotifyRetryKeepsSourceIndex(t *testing.T) {
	uc, repo, authRepo, _ := newModelBUsecase(t, 500, 2)
	ctx := context.Background()
	now := time.Now().UnixMilli()
	const (
		pod1, pod2 = "pandora-hub-global-1", "pandora-hub-global-2"
		playerID   = uint64(1001)
	)
	seedWarming(t, repo, pod1, 1, 500, now)
	seedWarming(t, repo, pod2, 2, 500, now)
	activate(t, uc, authRepo, pod1, "uid-A", 42, "j42", now)
	activate(t, uc, authRepo, pod2, "uid-B", 52, "j52", now)
	if _, err := uc.AssignHub(ctx, playerID, "global", 0, 0, 0, ""); err != nil {
		t.Fatal(err)
	}
	assign, found, err := repo.GetAssignment(ctx, playerID)
	if err != nil || !found || assign.GetHubPodName() != pod1 {
		t.Fatalf("assignment=%+v found=%v err=%v", assign, found, err)
	}

	// 手工推进到「迁移已落地、通知未发」状态(复现前一次尝试在 CAS 后失败):
	// 占目标座位 → 登记 owner cleanup → CAS 归属到 pod2;源 member 索引仍在 pod1。
	newID := uuid.NewString()
	seat, rerr := uc.reserveRoutableSeat(ctx, pod2, playerID, newID)
	if rerr != nil {
		t.Fatalf("reserve target seat: %v", rerr)
	}
	next := proto.Clone(assign).(*hubv1.HubAssignmentStorageRecord)
	target2, _, _ := repo.GetShard(ctx, pod2)
	next.HubPodName, next.HubAddr = pod2, target2.GetHubAddr()
	next.ShardId, next.Region = target2.GetShardId(), target2.GetRegion()
	next.AssignmentId = newID
	next.ReleaseTrack = target2.GetReleaseTrack()
	bindAssignmentAuth(next, seat)
	if cerr := uc.registerTransferCleanup(ctx, next, assign); cerr != nil {
		t.Fatalf("register cleanup: %v", cerr)
	}
	if swapped, serr := repo.CompareAndSwapAssignment(ctx, playerID, assign, next, uc.assignmentSagaTTL()); serr != nil || !swapped {
		t.Fatalf("CAS to target swapped=%v err=%v", swapped, serr)
	}

	// 显式落源 member 索引(drain 扫描来源;夹具 AssignHub 路径不维护该反向索引)。
	if aerr := repo.AddShardMember(ctx, pod1, playerID, time.Hour); aerr != nil {
		t.Fatalf("seed source member index: %v", aerr)
	}

	from, _, _ := repo.GetShard(ctx, pod1)
	target, _, _ := repo.GetShard(ctx, pod2)

	// ① 会话权威不可达:补发失败,返回 false,源索引必须保留(下个 tick 还能扫到)。
	gate := &ackFakeSessionGate{err: errors.New("redis down")}
	uc.SetSessionGate(gate)
	if uc.migratePlayer(ctx, playerID, from, target) {
		t.Fatal("notify with unavailable session authority must not report success")
	}
	members, merr := repo.ListShardMembers(ctx, pod1)
	if merr != nil || !slices.Contains(members, playerID) {
		t.Fatalf("source member index must survive failed notify: members=%v err=%v", members, merr)
	}

	// ② Kafka 发布失败(R9 复审 P2):重签成功但真实推送失败,同样必须保留源索引,
	// 不得把「发布失败」静默当作已送达。
	gate.err = nil
	gate.jti, gate.found = "jti-cur", true
	pusher := &fakeMigratePusher{}
	pusher.setErr(errors.New("kafka down"))
	uc.SetMigratePusher(pusher)
	if uc.migratePlayer(ctx, playerID, from, target) {
		t.Fatal("failed publish must not report migration notify success")
	}
	members, merr = repo.ListShardMembers(ctx, pod1)
	if merr != nil || !slices.Contains(members, playerID) {
		t.Fatalf("source member index must survive failed publish: members=%v err=%v", members, merr)
	}

	// ③ 发布恢复:重扫补发成功(true),源索引清理,退出 drain 扫描。
	pusher.setErr(nil)
	if !uc.migratePlayer(ctx, playerID, from, target) {
		t.Fatal("recovered notify retry must succeed")
	}
	if pusher.count() != 1 {
		t.Fatalf("recovered retry must deliver exactly one push, got %d", pusher.count())
	}
	members, merr = repo.ListShardMembers(ctx, pod1)
	if merr != nil || slices.Contains(members, playerID) {
		t.Fatalf("source member index must be cleared after successful notify: members=%v err=%v", members, merr)
	}
}

// TestModelB_ReuseInvalidatesOnInstanceDrift:玩家已分配后,分片被新 DS 实例顶替(uid 变、
// epoch 递增、active gen 前进),旧归属钉的元组不再等于当前 active → 复用失效 → 退旧重分配,
// 新归属钉新元组(审核二轮 CE1/CE2 misassignment 防护)。
func TestModelB_ReuseInvalidatesOnInstanceDrift(t *testing.T) {
	uc, repo, authRepo, _ := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const pod = "pandora-hub-global-1"
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	ep1 := activate(t, uc, authRepo, pod, "uid-A", 9, "j9", now)

	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("first assign: %v", err)
	}
	a1, _, _ := repo.GetAssignment(ctx, 1001)
	if a1.HubInstanceUid != "uid-A" || a1.AuthEpoch != ep1 || a1.AuthGen != 9 {
		t.Fatalf("first assignment tuple mismatch: %+v", a1)
	}

	// DS 实例漂移:换 uid-B(InitAuth 递增 epoch),复位分片回 warming 模拟新实例首跳,重新 stage+activate 新 gen。
	seedResetWarming(t, repo, pod, now)
	ep2 := activate(t, uc, authRepo, pod, "uid-B", 20, "j20", now)
	if ep2 == ep1 {
		t.Fatalf("instance drift must bump protocol epoch, still %d", ep2)
	}

	// 再分配同玩家:旧归属元组 (uid-A,ep1,gen9) != 当前 active (uid-B,ep2,gen20)
	// → reuseRoutable=false → 退旧重分配,新归属钉新元组。
	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("reassign after drift: %v", err)
	}
	a2, _, _ := repo.GetAssignment(ctx, 1001)
	if a2.HubInstanceUid != "uid-B" || a2.AuthEpoch != ep2 || a2.AuthGen != 20 {
		t.Fatalf("assignment must rebind to new active tuple after drift, got %+v", a2)
	}
}

func TestModelB_SameInstanceRotationRebindsWithoutDoubleSeatAndKeepsUnknown(t *testing.T) {
	uc, repo, authRepo, _ := newModelBUsecase(t, 500, 1)
	ctx := context.Background()
	const pod = "pandora-hub-global-1"
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod, 1, 500, now)
	epoch := activate(t, uc, authRepo, pod, "uid-A", 9, "j9", now)
	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatal(err)
	}
	old, found, err := repo.GetAssignment(ctx, 1001)
	if err != nil || !found {
		t.Fatalf("assignment found=%v err=%v", found, err)
	}
	oldID := old.GetAssignmentId()
	if admission, err := uc.AcknowledgeAdmission(ctx, 1001, oldID, pod, uuid.NewString(), 1, "", &HubCredential{
		InstanceUID: "uid-A", ProtocolEpoch: epoch, Gen: 9, JTI: "j9",
		TokenSHA256: "sha-j9", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch,
	}); err != nil || !admission.Admitted {
		t.Fatalf("acknowledge initial admission: result=%+v err=%v", admission, err)
	}
	unknown := protowire.AppendTag(nil, 2047, protowire.VarintType)
	unknown = protowire.AppendVarint(unknown, 12345)
	next := proto.Clone(old).(*hubv1.HubAssignmentStorageRecord)
	next.ProtoReflect().SetUnknown(unknown)
	if swapped, err := repo.CompareAndSwapAssignment(ctx, 1001, old, next, modelBAuthTTL); err != nil || !swapped {
		t.Fatalf("inject future field swapped=%v err=%v", swapped, err)
	}

	rotated := &hubv1.HubDSCredential{
		Gen: 10, Jti: "j10", ExpMs: 2_000_000_000_000, Kid: "kid-test",
		InstanceUid: "uid-A", ProtocolEpoch: epoch, TokenSha256: "sha-j10",
		WriterEpoch: modelBTestWriterEpoch,
	}
	if _, err := authRepo.StagePending(ctx, pod, rotated, modelBAuthTTL); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.HeartbeatWithCredential(ctx, pod, 1, []uint64{1001}, uint32(uc.cfg.DefaultCapacity), "ready", now+1000, &HubCredential{
		InstanceUID: "uid-A", ProtocolEpoch: epoch, Gen: 10, JTI: "j10",
		TokenSHA256: "sha-j10", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatal(err)
	}
	got, _, _ := repo.GetAssignment(ctx, 1001)
	shard, _, _ := repo.GetShard(ctx, pod)
	if shard.GetPlayerCount() != 1 {
		t.Fatalf("credential rebind double-seated player_count=%d", shard.GetPlayerCount())
	}
	if got.GetAuthGen() != 10 || got.GetAuthJti() != "j10" || got.GetAssignmentId() != oldID {
		t.Fatalf("assignment was not rebound in place: %+v", got)
	}
	if !bytes.Equal(got.ProtoReflect().GetUnknown(), unknown) {
		t.Fatalf("future assignment fields lost: got=%x want=%x", got.ProtoReflect().GetUnknown(), unknown)
	}
}
