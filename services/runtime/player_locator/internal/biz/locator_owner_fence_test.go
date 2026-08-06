package biz

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/runtime/player_locator/internal/data"
)

func ownerSnapshot(pod, assignment string, epoch uint64, operation string) data.HubOwnerSnapshot {
	return data.HubOwnerSnapshot{
		OwnerEpoch: epoch, OperationID: operation,
		OwnerType: ownerTypeHub, Phase: ownerPhaseAdmitted,
		PodName: pod, InstanceUID: "uid-1", InstanceEpoch: 2,
		AssignmentID: assignment, ReleaseTrack: "stable",
		LeaseDeadlineMs: time.Now().Add(time.Minute).UnixMilli(),
	}
}

func fencedHubInput(pod, assignment, admission string, seq uint64) LocationInput {
	return LocationInput{
		PlayerID: 42, State: LocationStateHub, HubPod: pod,
		HubPresenceFence: HubPresenceFence{
			AssignmentID: assignment, AdmissionID: admission, AdmissionSeq: seq,
		},
	}
}

func TestSetLocation_FencedOwner依赖与Off模式兼容(t *testing.T) {
	ctx := context.Background()
	in := fencedHubInput("hub-1", "assignment-42", "admission-a", 1)

	t.Run("owner client 未配置时 fail-closed", func(t *testing.T) {
		repo := newStubRepo()
		uc := NewLocatorUsecase(repo, 30*time.Second)
		err := uc.SetLocation(ctx, in)
		if errcode.As(err) != errcode.ErrUnavailable {
			t.Fatalf("code=%d err=%v", errcode.As(err), err)
		}
		if len(repo.store) != 0 || len(repo.meta) != 0 {
			t.Fatalf("owner UNKNOWN 前不得写投影: store=%v meta=%v", repo.store, repo.meta)
		}
	})

	t.Run("off/dev 无 callback credential 仍按 owner pod+assignment 安全工作", func(t *testing.T) {
		repo := newStubRepo()
		uc := NewLocatorUsecase(repo, 30*time.Second)
		uc.SetHubOwnerAuthority(&stubHubOwner{rec: ownerSnapshot("hub-1", "assignment-42", 8, "op-8")})
		if err := uc.SetLocation(ctx, in); err != nil {
			t.Fatalf("无 Model B credential 的显式降级不应破坏本地服: %v", err)
		}
		got := repo.store[42].HubPresenceFence
		if !got.IsFullyFenced() || got.OwnerEpoch != 8 || got.OwnerOperationID != "op-8" {
			t.Fatalf("未绑定 owner 全序: %+v", got)
		}
	})
}

func TestSetLocation_Owner必须当前Admitted且租约有效(t *testing.T) {
	base := ownerSnapshot("hub-1", "assignment-42", 8, "op-8")
	boom := errors.New("owner unavailable")
	cases := []struct {
		name string
		edit func(*data.HubOwnerSnapshot)
		err  error
		code errcode.Code
	}{
		{name: "query failure", err: boom, code: errcode.ErrUnavailable},
		{name: "owner type battle", edit: func(r *data.HubOwnerSnapshot) { r.OwnerType = 2 }, code: errcode.ErrOwnerIdentityMismatch},
		{name: "phase pending", edit: func(r *data.HubOwnerSnapshot) { r.Phase = 1 }, code: errcode.ErrOwnerIdentityMismatch},
		{name: "lease expired", edit: func(r *data.HubOwnerSnapshot) { r.LeaseDeadlineMs = time.Now().Add(-time.Second).UnixMilli() }, code: errcode.ErrOwnerIdentityMismatch},
		{name: "wrong pod", edit: func(r *data.HubOwnerSnapshot) { r.PodName = "hub-2" }, code: errcode.ErrOwnerIdentityMismatch},
		{name: "wrong assignment", edit: func(r *data.HubOwnerSnapshot) { r.AssignmentID = "assignment-new" }, code: errcode.ErrOwnerIdentityMismatch},
		{name: "missing owner epoch", edit: func(r *data.HubOwnerSnapshot) { r.OwnerEpoch = 0 }, code: errcode.ErrOwnerIdentityMismatch},
		{name: "missing operation", edit: func(r *data.HubOwnerSnapshot) { r.OperationID = "" }, code: errcode.ErrOwnerIdentityMismatch},
		{name: "incomplete target uid", edit: func(r *data.HubOwnerSnapshot) { r.InstanceUID = "" }, code: errcode.ErrOwnerIdentityMismatch},
		{name: "incomplete target epoch", edit: func(r *data.HubOwnerSnapshot) { r.InstanceEpoch = 0 }, code: errcode.ErrOwnerIdentityMismatch},
		{name: "missing release track", edit: func(r *data.HubOwnerSnapshot) { r.ReleaseTrack = "" }, code: errcode.ErrOwnerIdentityMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := base
			if tc.edit != nil {
				tc.edit(&rec)
			}
			repo := newStubRepo()
			uc := NewLocatorUsecase(repo, 30*time.Second)
			uc.SetHubOwnerAuthority(&stubHubOwner{rec: rec, err: tc.err})
			err := uc.SetLocation(context.Background(), fencedHubInput("hub-1", "assignment-42", "admission-a", 1))
			if errcode.As(err) != tc.code {
				t.Fatalf("code=%d want=%d err=%v", errcode.As(err), tc.code, err)
			}
			if len(repo.store) != 0 || len(repo.meta) != 0 {
				t.Fatalf("owner 拒绝后必须零副作用: store=%v meta=%v", repo.store, repo.meta)
			}
		})
	}
}

func TestSetLocation_ModelBCredential存在时必须精确匹配Owner实例(t *testing.T) {
	cases := []struct {
		name  string
		uid   string
		epoch uint32
	}{
		{name: "exact", uid: "uid-1", epoch: 2},
		{name: "wrong uid", uid: "uid-old", epoch: 2},
		{name: "wrong epoch", uid: "uid-1", epoch: 1},
		{name: "partial uid only", uid: "uid-1", epoch: 0},
		{name: "partial epoch only", uid: "", epoch: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			repo := newStubRepo()
			uc := NewLocatorUsecase(repo, 30*time.Second)
			uc.SetHubOwnerAuthority(&stubHubOwner{rec: ownerSnapshot("hub-1", "assignment-42", 8, "op-8")})
			in := fencedHubInput("hub-1", "assignment-42", "admission-a", 1)
			in.HubInstanceUID, in.HubInstanceEpoch = tc.uid, tc.epoch
			err := uc.SetLocation(context.Background(), in)
			if tc.name == "exact" {
				if err != nil {
					t.Fatalf("exact credential 被拒: %v", err)
				}
				return
			}
			if errcode.As(err) != errcode.ErrOwnerIdentityMismatch {
				t.Fatalf("code=%d err=%v", errcode.As(err), err)
			}
			if len(repo.store) != 0 || len(repo.meta) != 0 {
				t.Fatal("credential mismatch 后不得写投影")
			}
		})
	}
}

type blockingHubOwner struct {
	oldRec  data.HubOwnerSnapshot
	newRec  data.HubOwnerSnapshot
	started chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (b *blockingHubOwner) QueryOwner(context.Context, uint64) (data.HubOwnerSnapshot, error) {
	if b.calls.Add(1) == 1 {
		close(b.started)
		<-b.release
		return b.oldRec, nil
	}
	return b.newRec, nil
}

func TestSetLocation_跨Assignment与Pod的旧查询迟到不得反向覆盖(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	authority := &blockingHubOwner{
		oldRec:  ownerSnapshot("hub-old", "assignment-old", 10, "op-10"),
		newRec:  ownerSnapshot("hub-new", "assignment-new", 11, "op-11"),
		started: make(chan struct{}), release: make(chan struct{}),
	}
	authority.newRec.InstanceUID = "uid-new"
	authority.newRec.InstanceEpoch = 3
	uc.SetHubOwnerAuthority(authority)

	oldDone := make(chan error, 1)
	go func() {
		oldDone <- uc.SetLocation(context.Background(), fencedHubInput(
			"hub-old", "assignment-old", "admission-old", 1))
	}()
	<-authority.started // 旧请求已拿到旧 owner 快照，但尚未进入 Redis。

	if err := uc.SetLocation(context.Background(), fencedHubInput(
		"hub-new", "assignment-new", "admission-new", 1)); err != nil {
		t.Fatalf("新 owner 写入失败: %v", err)
	}
	close(authority.release)
	if err := <-oldDone; errcode.As(err) != errcode.ErrLocatorConflict {
		t.Fatalf("旧 owner 迟到应被全局 epoch 拒绝: code=%d err=%v", errcode.As(err), err)
	}
	got := repo.store[42]
	if got.HubPod != "hub-new" || got.HubPresenceFence.OwnerEpoch != 11 ||
		got.HubPresenceFence.AssignmentID != "assignment-new" {
		t.Fatalf("旧 Set 反向覆盖了新 owner: %+v", got)
	}
}

func TestSetLocation_MatchingBattle守卫拒绝时Meta零污染(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, *LocatorUsecase)
		match uint64
	}{
		{name: "MATCHING", setup: func(t *testing.T, uc *LocatorUsecase) {
			if err := uc.SetLocation(context.Background(), LocationInput{PlayerID: 42, State: LocationStateMatching, MatchID: 99}); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "BATTLE", match: 100, setup: func(t *testing.T, uc *LocatorUsecase) {
			if err := uc.SetLocation(context.Background(), LocationInput{PlayerID: 42, State: LocationStateMatching, MatchID: 99}); err != nil {
				t.Fatal(err)
			}
			if err := uc.SetLocation(context.Background(), LocationInput{PlayerID: 42, State: LocationStateBattle, MatchID: 99, BattlePod: "battle-1"}); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			repo := newStubRepo()
			uc := NewLocatorUsecase(repo, 30*time.Second)
			authority := &stubHubOwner{rec: ownerSnapshot("hub-1", "assignment-old", 10, "op-10")}
			uc.SetHubOwnerAuthority(authority)
			if err := uc.SetLocation(context.Background(), fencedHubInput("hub-1", "assignment-old", "admission-old", 1)); err != nil {
				t.Fatal(err)
			}
			before := repo.meta[42]
			tc.setup(t, uc)
			authority.rec = ownerSnapshot("hub-2", "assignment-new", 11, "op-11")
			in := fencedHubInput("hub-2", "assignment-new", "admission-new", 1)
			in.MatchID = tc.match // BATTLE 用错误 source match fence，确保守卫拒绝。
			if err := uc.SetLocation(context.Background(), in); errcode.As(err) != errcode.ErrLocatorConflict {
				t.Fatalf("guard 应拒绝: code=%d err=%v", errcode.As(err), err)
			}
			if got := repo.meta[42]; !got.Equal(before) {
				t.Fatalf("guard 拒绝却污染 meta: before=%+v after=%+v", before, got)
			}
		})
	}
}

type failOnceSetRepo struct {
	*stubRepo
	err    error
	failed bool
}

func (r *failOnceSetRepo) SetGuarded(ctx context.Context, playerID uint64, rec data.LocationRecord,
	ttl time.Duration, retry int, guard func(data.LocationRecord, bool) error) error {
	if !r.failed {
		r.failed = true
		return r.err
	}
	return r.stubRepo.SetGuarded(ctx, playerID, rec, ttl, retry, guard)
}

type failOnceCommitRepo struct {
	*stubRepo
	err    error
	failed bool
}

type disconnectBeforeCommitRepo struct {
	*stubRepo
	arm             bool
	compensateCalls int
}

func (r *disconnectBeforeCommitRepo) SetGuarded(ctx context.Context, playerID uint64, rec data.LocationRecord,
	ttl time.Duration, retry int, guard func(data.LocationRecord, bool) error) error {
	if err := r.stubRepo.SetGuarded(ctx, playerID, rec, ttl, retry, guard); err != nil {
		return err
	}
	if r.arm {
		r.arm = false
		// 精确模拟：Set 的 validate 已过、location TTL 已被刷长；同 admission 的
		// Disconnect 随后在线性化 meta 上写入 left_at，赶在 commit 之前完成。
		r.lastSeen[playerID] = time.Now().UnixMilli()
	}
	return nil
}

func (r *disconnectBeforeCommitRepo) ShrinkHubTTL(ctx context.Context, hubPod string, playerID uint64,
	fence data.HubPresenceFence, grace time.Duration) (bool, bool, error) {
	r.compensateCalls++
	return r.stubRepo.ShrinkHubTTL(ctx, hubPod, playerID, fence, grace)
}

func (r *failOnceCommitRepo) ActivateHubPresence(ctx context.Context, playerID uint64,
	fence data.HubPresenceFence, retention time.Duration) (bool, error) {
	if !r.failed {
		r.failed = true
		return false, r.err
	}
	return r.stubRepo.ActivateHubPresence(ctx, playerID, fence, retention)
}

func TestSetLocation_两阶段部分失败可重试收敛(t *testing.T) {
	boom := errors.New("redis injected failure")
	t.Run("location CAS 失败不改 meta，重试成功", func(t *testing.T) {
		repo := &failOnceSetRepo{stubRepo: newStubRepo(), err: boom}
		uc := NewLocatorUsecase(repo, 30*time.Second)
		uc.SetHubOwnerAuthority(&stubHubOwner{rec: ownerSnapshot("hub-1", "assignment-42", 8, "op-8")})
		in := fencedHubInput("hub-1", "assignment-42", "admission-a", 1)
		if err := uc.SetLocation(context.Background(), in); !errors.Is(err, boom) {
			t.Fatalf("first err=%v", err)
		}
		if len(repo.meta) != 0 || len(repo.store) != 0 {
			t.Fatalf("validate 后 CAS 失败必须零副作用: store=%v meta=%v", repo.store, repo.meta)
		}
		if err := uc.SetLocation(context.Background(), in); err != nil {
			t.Fatalf("retry failed: %v", err)
		}
	})

	t.Run("meta commit 失败保留可验证 location，重试完成 commit", func(t *testing.T) {
		repo := &failOnceCommitRepo{stubRepo: newStubRepo(), err: boom}
		uc := NewLocatorUsecase(repo, 30*time.Second)
		uc.SetHubOwnerAuthority(&stubHubOwner{rec: ownerSnapshot("hub-1", "assignment-42", 8, "op-8")})
		in := fencedHubInput("hub-1", "assignment-42", "admission-a", 1)
		if err := uc.SetLocation(context.Background(), in); !errors.Is(err, boom) {
			t.Fatalf("first err=%v", err)
		}
		if got := repo.store[42].HubPresenceFence; !got.IsFullyFenced() || len(repo.meta) != 0 {
			t.Fatalf("commit 失败边界错误: location=%+v meta=%v", got, repo.meta)
		}
		if err := uc.SetLocation(context.Background(), in); err != nil {
			t.Fatalf("retry failed: %v", err)
		}
		if got := repo.meta[42]; !got.IsFullyFenced() || !got.Equal(repo.store[42].HubPresenceFence) {
			t.Fatalf("retry 未收敛 location/meta: location=%+v meta=%+v", repo.store[42].HubPresenceFence, got)
		}
	})
}

func TestSetLocation_Validate后ExactDisconnect抢先Commit必须补偿缩TTL(t *testing.T) {
	base := newStubRepo()
	authority := &stubHubOwner{rec: ownerSnapshot("hub-1", "assignment-42", 8, "op-8")}
	seedUC := NewLocatorUsecase(base, 30*time.Second)
	seedUC.SetHubOwnerAuthority(authority)
	in := fencedHubInput("hub-1", "assignment-42", "admission-a", 1)
	if err := seedUC.SetLocation(context.Background(), in); err != nil {
		t.Fatalf("seed Set failed: %v", err)
	}

	repo := &disconnectBeforeCommitRepo{stubRepo: base, arm: true}
	uc := NewLocatorUsecase(repo, 30*time.Second)
	uc.SetHubOwnerAuthority(authority)
	err := uc.SetLocation(context.Background(), in)
	if errcode.As(err) != errcode.ErrLocatorConflict {
		t.Fatalf("迟到 exact Set 必须以冲突结束: code=%d err=%v", errcode.As(err), err)
	}
	if repo.compensateCalls != 1 {
		t.Fatalf("commit 因 left_at 拒绝后必须 exact 重缩 TTL: calls=%d", repo.compensateCalls)
	}
	if repo.lastSeen[42] <= 0 {
		t.Fatal("补偿不得清除 Disconnect 已写入的 left_at")
	}
}

var _ HubOwnerAuthority = (*blockingHubOwner)(nil)
var _ data.LocationRepo = (*failOnceSetRepo)(nil)
var _ data.LocationRepo = (*failOnceCommitRepo)(nil)
var _ data.LocationRepo = (*disconnectBeforeCommitRepo)(nil)
