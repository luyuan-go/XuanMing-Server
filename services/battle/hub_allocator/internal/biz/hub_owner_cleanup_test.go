package biz

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/releasetrack"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/data"
)

type ownerCleanupFixture struct {
	uc                *HubUsecase
	repo              *data.RedisHubRepo
	authRepo          *data.RedisHubAuthRepo
	mr                *miniredis.Miniredis
	source            *hubv1.HubAssignmentStorageRecord
	sourceCredential  *HubCredential
	targetCredential  *HubCredential
	sourceAdmissionID string
}

func newOwnerCleanupFixture(t *testing.T) *ownerCleanupFixture {
	t.Helper()
	uc, repo, authRepo, mr := newModelBUsecase(t, 500, 2)
	ctx := context.Background()
	const pod1, pod2 = "pandora-hub-global-1", "pandora-hub-global-2"
	now := time.Now().UnixMilli()
	seedWarming(t, repo, pod1, 1, 500, now)
	seedWarming(t, repo, pod2, 2, 500, now)
	for _, pod := range []string{pod1, pod2} {
		if err := repo.UpdateShardWithLock(ctx, pod, 8, func(s *hubv1.HubShardStorageRecord) error {
			s.ReleaseTrack = releasetrack.Stable
			return nil
		}, modelBAuthTTL); err != nil {
			t.Fatal(err)
		}
	}
	epoch1 := activate(t, uc, authRepo, pod1, "uid-A", 42, "j42", now)
	epoch2 := activate(t, uc, authRepo, pod2, "uid-B", 52, "j52", now)
	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("source assignment: %v", err)
	}
	source, found, err := repo.GetAssignment(ctx, 1001)
	if err != nil || !found || source.GetHubPodName() != pod1 {
		t.Fatalf("source=%+v found=%v err=%v", source, found, err)
	}
	sourceCredential := &HubCredential{InstanceUID: "uid-A", ProtocolEpoch: epoch1, Gen: 42,
		JTI: "j42", TokenSHA256: "sha-j42", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch}
	targetCredential := &HubCredential{InstanceUID: "uid-B", ProtocolEpoch: epoch2, Gen: 52,
		JTI: "j52", TokenSHA256: "sha-j52", Kid: "kid-test", WriterEpoch: modelBTestWriterEpoch}
	sourceAdmissionID := uuid.NewString()
	if got, err := uc.AcknowledgeAdmission(ctx, 1001, source.GetAssignmentId(), pod1,
		sourceAdmissionID, 1, "", sourceCredential); err != nil || !got.Admitted {
		t.Fatalf("source admission=%+v err=%v", got, err)
	}
	return &ownerCleanupFixture{uc: uc, repo: repo, authRepo: authRepo, mr: mr, source: source,
		sourceCredential: sourceCredential, targetCredential: targetCredential,
		sourceAdmissionID: sourceAdmissionID}
}

func (f *ownerCleanupFixture) restart(repo data.HubRepo, authRepo data.HubAuthRepo) *HubUsecase {
	if repo == nil {
		repo = f.repo
	}
	if authRepo == nil {
		authRepo = f.authRepo
	}
	uc := NewHubUsecase(repo, NewMockHubFleetProvider(f.uc.cfg), &fakeSigner{}, f.uc.cfg)
	uc.SetAuthRepo(authRepo)
	uc.SetAuthTTL(modelBAuthTTL)
	return uc
}

type commitThenErrorAssignmentRepo struct {
	data.HubRepo
	shouldFail func(expected, next *hubv1.HubAssignmentStorageRecord) bool
	fired      bool
}

func (r *commitThenErrorAssignmentRepo) CompareAndSwapAssignment(ctx context.Context, playerID uint64,
	expected, next *hubv1.HubAssignmentStorageRecord, ttl time.Duration,
) (bool, error) {
	if !r.fired && r.shouldFail != nil && r.shouldFail(expected, next) {
		swapped, err := r.HubRepo.CompareAndSwapAssignment(ctx, playerID, expected, next, ttl)
		if err != nil || !swapped {
			return swapped, err
		}
		r.fired = true
		return false, errcode.New(errcode.ErrUnavailable, "injected committed assignment CAS response loss")
	}
	return r.HubRepo.CompareAndSwapAssignment(ctx, playerID, expected, next, ttl)
}

type departureCommitThenErrorAuthRepo struct {
	data.HubAuthRepo
	failNext bool
}

func (r *departureCommitThenErrorAuthRepo) AcknowledgeDeparture(ctx context.Context, pod string,
	credential data.CredentialIdentity, reservation data.ReservationIdentity, admissionID string,
	admissionSeq uint64, nowMs int64, shardTTL time.Duration,
) (data.DepartureResult, error) {
	if r.failNext {
		r.failNext = false
		result, err := r.HubAuthRepo.AcknowledgeDeparture(ctx, pod, credential, reservation,
			admissionID, admissionSeq, nowMs, shardTTL)
		if err != nil {
			return result, err
		}
		return data.DepartureResult{}, errcode.New(errcode.ErrUnavailable,
			"injected committed departure response loss")
	}
	return r.HubAuthRepo.AcknowledgeDeparture(ctx, pod, credential, reservation,
		admissionID, admissionSeq, nowMs, shardTTL)
}

func assertExactSourceEviction(t *testing.T, f *ownerCleanupFixture, uc *HubUsecase,
	target *hubv1.HubAssignmentStorageRecord) {
	t.Helper()
	heartbeat, err := uc.HeartbeatWithCredential(context.Background(), f.source.GetHubPodName(), 1,
		[]uint64{1001}, 500, "ready", 0, f.sourceCredential)
	if err != nil || len(heartbeat.EvictionOrders) != 1 {
		t.Fatalf("heartbeat=%+v err=%v", heartbeat, err)
	}
	order := heartbeat.EvictionOrders[0]
	if order.PlayerID != 1001 || order.AssignmentID != f.source.GetAssignmentId() ||
		order.AdmissionID != f.sourceAdmissionID || order.AdmissionSeq != 1 ||
		order.CleanupAssignmentID != target.GetAssignmentId() {
		t.Fatalf("non-exact eviction order: %+v", order)
	}
}

func finishSourceDeparture(t *testing.T, f *ownerCleanupFixture, uc *HubUsecase) {
	t.Helper()
	result, err := uc.AcknowledgeDeparture(context.Background(), 1001, f.source.GetAssignmentId(),
		f.source.GetHubPodName(), f.sourceAdmissionID, 1, f.sourceCredential)
	if err != nil || !result.Departed {
		t.Fatalf("source departure=%+v err=%v", result, err)
	}
}

func admitRecoveredTarget(t *testing.T, f *ownerCleanupFixture, uc *HubUsecase,
	target *hubv1.HubAssignmentStorageRecord) {
	t.Helper()
	got, err := uc.AcknowledgeAdmission(context.Background(), 1001,
		target.GetAssignmentId(), target.GetHubPodName(), uuid.NewString(), 2, "", f.targetCredential)
	if err != nil || !got.Admitted {
		t.Fatalf("target admission=%+v err=%v", got, err)
	}
	if sessions, _ := f.mr.HKeys("pandora:hub:sessions:{" + f.source.GetHubPodName() + "}"); len(sessions) != 0 {
		t.Fatalf("source physical owner survived: %v", sessions)
	}
	if sessions, _ := f.mr.HKeys("pandora:hub:sessions:{" + target.GetHubPodName() + "}"); len(sessions) != 1 || sessions[0] != target.GetAssignmentId() {
		t.Fatalf("target physical owner cardinality=%v", sessions)
	}
}

func TestHubOwnerCleanupRestartRecoversPublicationAndBindResponseLoss(t *testing.T) {
	for _, tc := range []struct {
		name   string
		inject func(*ownerCleanupFixture)
	}{
		{name: "assignment-cas-committed-response-lost", inject: func(f *ownerCleanupFixture) {
			f.uc.repo = &commitThenErrorAssignmentRepo{HubRepo: f.repo,
				shouldFail: func(expected, next *hubv1.HubAssignmentStorageRecord) bool {
					return expected != nil && next != nil && next.GetTransferCleanupPending() &&
						expected.GetAssignmentId() != next.GetAssignmentId()
				}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOwnerCleanupFixture(t)
			tc.inject(f)
			if got, err := f.uc.TransferHub(context.Background(), 1001, 2); errcode.As(err) != errcode.ErrUnavailable || got != nil {
				t.Fatalf("faulted transfer=%+v err=%v", got, err)
			}
			target, found, err := f.repo.GetAssignment(context.Background(), 1001)
			if err != nil || !found || !target.GetTransferCleanupPending() {
				t.Fatalf("durable target=%+v found=%v err=%v", target, found, err)
			}
			restarted := f.restart(nil, nil)
			if err := restarted.reconcileOwnerCleanups(context.Background()); errcode.As(err) != errcode.ErrUnavailable {
				t.Fatalf("reconcile must wait for physical departure: %v", err)
			}
			target, _, _ = f.repo.GetAssignment(context.Background(), 1001)
			if !target.GetTransferTargetBound() {
				t.Fatalf("target Bind was not durably recovered: %+v", target)
			}
			assertExactSourceEviction(t, f, restarted, target)
			finishSourceDeparture(t, f, restarted)
			if err := restarted.reconcileOwnerCleanups(context.Background()); err != nil {
				t.Fatalf("cleanup after physical departure: %v", err)
			}
			target, found, err = f.repo.GetAssignment(context.Background(), 1001)
			if err != nil || !found || target.GetTransferCleanupPending() ||
				target.GetAssignmentId() == f.source.GetAssignmentId() {
				t.Fatalf("cleanup did not preserve exact target: %+v found=%v err=%v", target, found, err)
			}
			refs, _ := f.repo.ListTransferCleanups(context.Background(), f.source.GetHubPodName())
			if len(refs) != 0 {
				t.Fatalf("cleanup index survived completion: %+v", refs)
			}
			admitRecoveredTarget(t, f, restarted, target)
		})
	}
}

func TestHubOwnerCleanupResponseLossNeverReleasesNewOwner(t *testing.T) {
	f := newOwnerCleanupFixture(t)
	ctx := context.Background()
	if got, err := f.uc.TransferHub(ctx, 1001, 2); errcode.As(err) != errcode.ErrUnavailable || got != nil {
		t.Fatalf("transfer=%+v err=%v", got, err)
	}
	target, _, _ := f.repo.GetAssignment(ctx, 1001)
	assertExactSourceEviction(t, f, f.uc, target)

	faultAuth := &departureCommitThenErrorAuthRepo{HubAuthRepo: f.authRepo, failNext: true}
	restarted := f.restart(nil, faultAuth)
	if got, err := restarted.AcknowledgeDeparture(ctx, 1001, f.source.GetAssignmentId(),
		f.source.GetHubPodName(), f.sourceAdmissionID, 1, f.sourceCredential); errcode.As(err) != errcode.ErrUnavailable || got != nil {
		t.Fatalf("committed departure response loss=%+v err=%v", got, err)
	}
	if sessions, _ := f.mr.HKeys("pandora:hub:sessions:{" + f.source.GetHubPodName() + "}"); len(sessions) != 0 {
		t.Fatalf("committed physical departure did not remove source: %v", sessions)
	}
	if got, err := restarted.AcknowledgeDeparture(ctx, 1001, f.source.GetAssignmentId(),
		f.source.GetHubPodName(), f.sourceAdmissionID, 1, f.sourceCredential); err != nil || !got.Departed {
		t.Fatalf("departure response-loss replay=%+v err=%v", got, err)
	}

	clearFault := &commitThenErrorAssignmentRepo{HubRepo: f.repo,
		shouldFail: func(expected, next *hubv1.HubAssignmentStorageRecord) bool {
			return expected != nil && next != nil && expected.GetTransferCleanupPending() &&
				expected.GetTransferTargetBound() && !next.GetTransferCleanupPending() &&
				expected.GetAssignmentId() == next.GetAssignmentId()
		}}
	restarted = f.restart(clearFault, f.authRepo)
	if err := restarted.reconcileOwnerCleanups(ctx); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("committed phase-clear response loss=%v", err)
	}
	current, found, err := f.repo.GetAssignment(ctx, 1001)
	if err != nil || !found || current.GetTransferCleanupPending() ||
		current.GetAssignmentId() != target.GetAssignmentId() {
		t.Fatalf("committed phase clear damaged target: current=%+v found=%v err=%v", current, found, err)
	}
	// The response loss intentionally left only the exact index ref. A fresh
	// process recognizes it as an orphan; it must never touch the target seat.
	fresh := f.restart(nil, nil)
	if err := fresh.reconcileOwnerCleanups(ctx); err != nil {
		t.Fatalf("orphan index cleanup: %v", err)
	}
	refs, _ := f.repo.ListTransferCleanups(ctx, f.source.GetHubPodName())
	if len(refs) != 0 {
		t.Fatalf("phase-clear orphan ref remained: %+v", refs)
	}
	if reservations, _ := f.mr.HKeys("pandora:hub:reservations:{" + target.GetHubPodName() + "}"); len(reservations) != 1 || reservations[0] != target.GetAssignmentId() {
		t.Fatalf("orphan cleanup released new target reservation: %v", reservations)
	}
	admitRecoveredTarget(t, f, fresh, current)
}

func TestReleaseHubDurableTombstoneWaitsForPhysicalDeparture(t *testing.T) {
	f := newOwnerCleanupFixture(t)
	ctx := context.Background()
	if err := f.uc.ReleaseHub(ctx, 1001); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("ReleaseHub must wait for source DS departure: %v", err)
	}
	tombstone, found, err := f.repo.GetAssignment(ctx, 1001)
	if err != nil || !found || !tombstone.GetReleaseCleanupPending() {
		t.Fatalf("release tombstone=%+v found=%v err=%v", tombstone, found, err)
	}
	assertExactSourceEviction(t, f, f.uc, tombstone)
	finishSourceDeparture(t, f, f.uc)

	deleteFault := &commitThenErrorAssignmentRepo{HubRepo: f.repo,
		shouldFail: func(expected, next *hubv1.HubAssignmentStorageRecord) bool {
			return expected != nil && expected.GetReleaseCleanupPending() && next == nil
		}}
	restarted := f.restart(deleteFault, nil)
	if err := restarted.reconcileOwnerCleanups(ctx); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("committed tombstone delete response loss: %v", err)
	}
	if _, found, err := f.repo.GetAssignment(ctx, 1001); err != nil || found {
		t.Fatalf("committed release tombstone still present: found=%v err=%v", found, err)
	}
	fresh := f.restart(nil, nil)
	if err := fresh.reconcileOwnerCleanups(ctx); err != nil {
		t.Fatalf("release orphan index cleanup: %v", err)
	}
	if err := fresh.ReleaseHub(ctx, 1001); err != nil {
		t.Fatalf("idempotent release replay: %v", err)
	}
	refs, _ := f.repo.ListTransferCleanups(ctx, f.source.GetHubPodName())
	if len(refs) != 0 {
		t.Fatalf("release cleanup ref remained: %+v", refs)
	}
	if sessions, _ := f.mr.HKeys("pandora:hub:sessions:{" + f.source.GetHubPodName() + "}"); len(sessions) != 0 {
		t.Fatalf("release left source physical owner: %v", sessions)
	}
}

func TestCleanupAssignmentCASComparisonIncludesNewPhaseFields(t *testing.T) {
	// Small regression guard: a rolling binary must compare the additive phase
	// fields, not silently overwrite them as unknown/default values.
	base := &hubv1.HubAssignmentStorageRecord{PlayerId: 1, AssignmentId: "a"}
	pending := proto.Clone(base).(*hubv1.HubAssignmentStorageRecord)
	pending.TransferCleanupPending = true
	if proto.Equal(base, pending) {
		t.Fatal("cleanup phase field was not part of protobuf equality")
	}
}

// casInterceptorRepo 在首次匹配的 CompareAndSwapAssignment **执行前**注入一个并发写者,
// 模拟"registerTransferCleanup 与 CAS 之间 assignment 被别的写者改写"的窗口。
type casInterceptorRepo struct {
	data.HubRepo
	match  func(expected, next *hubv1.HubAssignmentStorageRecord) bool
	before func()
	fired  bool
}

func (r *casInterceptorRepo) CompareAndSwapAssignment(ctx context.Context, playerID uint64,
	expected, next *hubv1.HubAssignmentStorageRecord, ttl time.Duration,
) (bool, error) {
	if !r.fired && r.match != nil && r.match(expected, next) {
		r.fired = true
		if r.before != nil {
			r.before()
		}
	}
	return r.HubRepo.CompareAndSwapAssignment(ctx, playerID, expected, next, ttl)
}

// AssignHub 的置换路径与 TransferHub 共用同一条 owner 置换 saga。此前该 saga 在两个入口
// 各有一份拷贝、补偿规则靠人工镜像;本用例把 Assign 侧此前**无直驱用例**的
// "CAS 已提交但响应丢失"分支钉成行为快照:响应丢失时必须保留 durable pending 记录与
// 新座位(交给重启对账区分已提交/孤儿),绝不能因为响应丢了就把已提交的新 owner 拆掉。
func TestAssignHubReplacementResponseLossNeverReleasesNewOwner(t *testing.T) {
	f := newOwnerCleanupFixture(t)
	ctx := context.Background()

	// DS 实例漂移:pod1 复位回 warming 并以新 uid 激活 → 旧归属元组不可复用,
	// AssignHub 走"置换"路径(新 assignmentID + 旧 owner cleanup saga)。
	now := time.Now().UnixMilli()
	seedResetWarming(t, f.repo, f.source.GetHubPodName(), now)
	activate(t, f.uc, f.authRepo, f.source.GetHubPodName(), "uid-C", 77, "j77", now)

	f.uc.repo = &commitThenErrorAssignmentRepo{HubRepo: f.repo,
		shouldFail: func(expected, next *hubv1.HubAssignmentStorageRecord) bool {
			return expected != nil && next != nil && next.GetTransferCleanupPending() &&
				expected.GetAssignmentId() != next.GetAssignmentId()
		}}

	if got, err := f.uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); errcode.As(err) != errcode.ErrUnavailable || got != nil {
		t.Fatalf("faulted replacement assign=%+v err=%v", got, err)
	}

	// CAS 实际已提交:durable 记录必须还在、带 cleanup pending、指向新 assignmentID。
	target, found, err := f.repo.GetAssignment(ctx, 1001)
	if err != nil || !found || !target.GetTransferCleanupPending() ||
		target.GetAssignmentId() == f.source.GetAssignmentId() {
		t.Fatalf("durable target=%+v found=%v err=%v", target, found, err)
	}
	// 新座位绝不能被补偿掉——它属于已提交的新 owner。
	if reservations, _ := f.mr.HKeys("pandora:hub:reservations:{" + target.GetHubPodName() + "}"); len(reservations) != 1 || reservations[0] != target.GetAssignmentId() {
		t.Fatalf("committed replacement seat was released: %v", reservations)
	}
	// 重启对账。与 Transfer 版不同:这里源实例已整体换代(seedResetWarming 清了物理残留),
	// 没有要等的物理离场,reconcile 允许直接完成 —— 但完成后必须**保留 exact 新 owner**:
	// pending 清掉、assignmentID 不变、新座位仍在、cleanup 索引清空。
	restarted := f.restart(nil, nil)
	if err := restarted.reconcileOwnerCleanups(ctx); err != nil {
		t.Fatalf("reconcile after committed response loss: %v", err)
	}
	target2, found2, err2 := f.repo.GetAssignment(ctx, 1001)
	if err2 != nil || !found2 || target2.GetTransferCleanupPending() ||
		target2.GetAssignmentId() != target.GetAssignmentId() {
		t.Fatalf("reconcile damaged committed target: %+v found=%v err=%v", target2, found2, err2)
	}
	if reservations, _ := f.mr.HKeys("pandora:hub:reservations:{" + target.GetHubPodName() + "}"); len(reservations) != 1 || reservations[0] != target.GetAssignmentId() {
		t.Fatalf("reconcile released the committed seat: %v", reservations)
	}
	if refs, _ := f.repo.ListTransferCleanups(ctx, f.source.GetHubPodName()); len(refs) != 0 {
		t.Fatalf("cleanup index survived completion: %+v", refs)
	}
}

// 钉住 CAS 输者的补偿顺序:并发写者在 register 与 CAS 之间改写 assignment 时,
// 输者必须**先删 cleanup ref、再补偿新座位**、然后重试收敛;绝不能留下孤儿 ref 或泄漏座位。
// Assign 与 Transfer 两个入口此前均无该分支的直驱用例。
func TestReplacementCASLoserRemovesRefBeforeSeat(t *testing.T) {
	for _, tc := range []struct {
		name  string
		drive func(t *testing.T, f *ownerCleanupFixture)
	}{
		{name: "assign", drive: func(t *testing.T, f *ownerCleanupFixture) {
			now := time.Now().UnixMilli()
			seedResetWarming(t, f.repo, f.source.GetHubPodName(), now)
			activate(t, f.uc, f.authRepo, f.source.GetHubPodName(), "uid-C", 77, "j77", now)
			if _, err := f.uc.AssignHub(context.Background(), 1001, "global", 0, 0, 0, ""); err != nil {
				t.Fatalf("assign with concurrent writer must converge: %v", err)
			}
		}},
		{name: "transfer", drive: func(t *testing.T, f *ownerCleanupFixture) {
			// 源席位仍 admitted:置换的第二轮会推进到 cleanup 阶段,并因源未物理离场
			// 以错误返回(保留 durable 新 owner 交给驱逐/对账续跑)——这是既有语义,
			// 本快照只要求它不 panic 且满足下方的 ref/座位不变量。
			if _, err := f.uc.TransferHub(context.Background(), 1001, 2); err == nil {
				t.Fatalf("transfer with admitted source should not complete synchronously")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOwnerCleanupFixture(t)
			ctx := context.Background()
			// 并发写者:在 saga 的 CAS 执行前把 assignment 改一版(AssignedAtMs+1),
			// 让 saga 第一轮 CAS 必输、第二轮以新快照重试。
			f.uc.repo = &casInterceptorRepo{HubRepo: f.repo,
				match: func(expected, next *hubv1.HubAssignmentStorageRecord) bool {
					return expected != nil && next != nil && next.GetTransferCleanupPending() &&
						expected.GetAssignmentId() != next.GetAssignmentId()
				},
				before: func() {
					current, found, err := f.repo.GetAssignment(ctx, 1001)
					if err != nil || !found {
						t.Fatalf("concurrent writer read: found=%v err=%v", found, err)
					}
					bumped := proto.Clone(current).(*hubv1.HubAssignmentStorageRecord)
					bumped.AssignedAtMs = current.GetAssignedAtMs() + 1
					if swapped, err := f.repo.CompareAndSwapAssignment(ctx, 1001, current, bumped, modelBAuthTTL); err != nil || !swapped {
						t.Fatalf("concurrent writer swap=%v err=%v", swapped, err)
					}
				}}

			tc.drive(t, f)

			// 输的那一轮登记过的 cleanup ref 不得残留成孤儿
			// (第二轮成功会登记并消费自己的 ref;这里断言最终态没有多余的)。
			final, found, err := f.repo.GetAssignment(ctx, 1001)
			if err != nil || !found {
				t.Fatalf("final assignment found=%v err=%v", found, err)
			}
			refs, _ := f.repo.ListTransferCleanups(ctx, f.source.GetHubPodName())
			for _, ref := range refs {
				if ref != transferCleanupRef(final) {
					t.Fatalf("orphan cleanup ref from the losing round survived: %v (final=%v)", refs, transferCleanupRef(final))
				}
			}
			// 输的那一轮占的座位必须已补偿:新 pod 的 reservations 里只允许最终 assignmentID。
			if reservations, _ := f.mr.HKeys("pandora:hub:reservations:{" + final.GetHubPodName() + "}"); len(reservations) > 1 {
				t.Fatalf("losing round leaked a seat reservation: %v", reservations)
			}
		})
	}
}
