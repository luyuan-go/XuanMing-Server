// local_ticket_binding_test.go — legacy 本地面 hub 票据绑定回归(2026-08-04)。
//
// 背景:mode=local 下 bindAssignmentAuth(seat==nil)不写 writer-v2 绑定,签出的 hub 票缺
// hub_assignment_id 与实例绑定,UE Hub DS PostLogin fail-closed 踢人、客户端 7s 重连循环。
// 修复 = signHubTicket 在 legacy 面经 localTicketBinding 回源 LocalCredentialACK 补齐七元组。
// 本文件同时钉死线上隔离:Model B(authRepo != nil)与非本地 fleet 永不走本地绑定路径。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/releasetrack"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"

	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/data"
)

// localCredMockFleet 在 Mock fleet 拓扑上叠加 localHubCredentialSource,模拟 mode=local 的
// LocalHubFleetProvider 形状(拓扑用 mock 分片,凭据用注入的完整五元组)。
type localCredMockFleet struct {
	*MockHubFleetProvider
	pod  string
	cred LocalHubCredential
}

func (f *localCredMockFleet) LocalCredentialACK(pod string) (LocalHubCredential, bool) {
	if pod == "" || pod != f.pod || !f.cred.Complete() {
		return LocalHubCredential{}, false
	}
	return f.cred, true
}

func testLocalCredential() LocalHubCredential {
	return LocalHubCredential{
		InstanceUID:   "uid-local-1",
		ProtocolEpoch: 1,
		Gen:           7,
		JTI:           "cred-jti-local",
		WriterEpoch:   auth.DSAuthWriterEpochV2,
	}
}

// unboundTicketBinding 判定票据绑定是否为"无绑定"旧行为(七元组全空;SourceMatchID/
// SessionJTI 是独立 claim,不参与判定)。
func unboundTicketBinding(b HubTicketBinding) bool {
	return b.PodName == "" && b.InstanceUID == "" && b.ProtocolEpoch == 0 &&
		b.CredentialGen == 0 && b.CredentialJTI == "" && b.HubAssignmentID == "" && b.WriterEpoch == 0
}

func newLocalCredUsecase(pod string, cred LocalHubCredential) (*HubUsecase, *fakeRepo, *fakeSigner) {
	cfg := testConf()
	cfg.DefaultCapacity = 500
	cfg.MockShardCount = 1
	repo := newFakeRepo()
	fleet := &localCredMockFleet{
		MockHubFleetProvider: NewMockHubFleetProvider(cfg),
		pod:                  pod,
		cred:                 cred,
	}
	signer := &fakeSigner{}
	return NewHubUsecase(repo, fleet, signer, cfg), repo, signer
}

// legacy 本地面:AssignHub 签出的票必须带完整七元组绑定(修复前恒为零绑定票)。
func TestAssignHub_LocalCredentialCompletesTicketBinding(t *testing.T) {
	const pod = "pandora-hub-global-1"
	cred := testLocalCredential()
	uc, repo, signer := newLocalCredUsecase(pod, cred)
	ctx := context.Background()

	if _, err := uc.AssignHub(ctx, 1001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("AssignHub err: %v", err)
	}
	assignment, found, err := repo.GetAssignment(ctx, 1001)
	if err != nil || !found {
		t.Fatalf("归属记录必须存在: found=%v err=%v", found, err)
	}
	b := signer.lastBinding
	if b.PodName != pod || b.InstanceUID != cred.InstanceUID || b.ProtocolEpoch != cred.ProtocolEpoch ||
		b.CredentialGen != cred.Gen || b.CredentialJTI != cred.JTI ||
		b.WriterEpoch != auth.DSAuthWriterEpochV2 {
		t.Fatalf("legacy 本地面签票必须带完整实例绑定,got %+v", b)
	}
	if b.HubAssignmentID == "" || b.HubAssignmentID != assignment.GetAssignmentId() {
		t.Fatalf("票据 hub_assignment_id 必须与归属记录一致: binding=%q assignment=%q",
			b.HubAssignmentID, assignment.GetAssignmentId())
	}
	if b.ReleaseTrack != releasetrack.Stable {
		t.Fatalf("本地面轨道应为 stable,got %q", b.ReleaseTrack)
	}
}

// 凭据不全(签发被跳过/pod 不匹配)时必须保持旧行为签无绑定票,绝不糊半截绑定。
func TestAssignHub_IncompleteLocalCredentialStaysUnbound(t *testing.T) {
	const pod = "pandora-hub-global-1"
	incomplete := testLocalCredential()
	incomplete.JTI = "" // 五元组缺一即不完整
	uc, _, signer := newLocalCredUsecase(pod, incomplete)

	if _, err := uc.AssignHub(context.Background(), 1002, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("AssignHub err: %v", err)
	}
	if !unboundTicketBinding(signer.lastBinding) {
		t.Fatalf("凭据不全时必须签无绑定票,got %+v", signer.lastBinding)
	}
}

// 非本地 fleet(mock,与 Agones 同样不实现 localHubCredentialSource)保持旧行为:零绑定。
func TestAssignHub_NonLocalFleetKeepsUnboundTicket(t *testing.T) {
	uc, _, signer := newTestUsecase(500, 1)
	if _, err := uc.AssignHub(context.Background(), 1003, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("AssignHub err: %v", err)
	}
	if !unboundTicketBinding(signer.lastBinding) {
		t.Fatalf("非本地 fleet 必须保持零绑定旧行为,got %+v", signer.lastBinding)
	}
}

// legacy 心跳必须续写 owner 实例租约(分片镜像带实例身份时;第三道墙回归 2026-08-04):
// login applyOwnerPlacement 只在「ADMITTED 且租约剩余 > 安全余量」报 STABLE,不续租
// = 客户端 Admission ACK 已到手却永远等不到 STABLE。
func TestLegacyHeartbeat_RenewsOwnerInstanceLease(t *testing.T) {
	const pod = "pandora-hub-global-1"
	uc, repo, _ := newTestUsecase(500, 1)
	ctx := context.Background()
	seedShard(repo, pod, 1, 0)
	if err := repo.UpdateShardWithLock(ctx, pod, 1, func(s *hubv1.HubShardStorageRecord) error {
		s.GameserverUid = "uid-live"
		return nil
	}, time.Minute); err != nil {
		t.Fatal(err)
	}
	lease := &fakeLeaseRenewer{}
	uc.SetOwnerLeaseRenewer(lease, false)

	if _, err := uc.Heartbeat(ctx, pod, 1, "ready", time.Now().UnixMilli(), 0); err != nil {
		t.Fatalf("Heartbeat err: %v", err)
	}
	if lease.calls != 1 || lease.pod != pod || lease.uid != "uid-live" || lease.epoch != 0 {
		t.Fatalf("legacy 心跳必须以分片镜像实例身份续租,got %+v", lease)
	}
}

// 镜像无实例身份(mock 旧行为)时不得续租:不能拿空 uid 糊一条租约。
func TestLegacyHeartbeat_NoInstanceUidSkipsLeaseRenew(t *testing.T) {
	const pod = "pandora-hub-global-1"
	uc, repo, _ := newTestUsecase(500, 1)
	ctx := context.Background()
	seedShard(repo, pod, 1, 0)
	lease := &fakeLeaseRenewer{}
	uc.SetOwnerLeaseRenewer(lease, false)

	if _, err := uc.Heartbeat(ctx, pod, 1, "ready", time.Now().UnixMilli(), 0); err != nil {
		t.Fatalf("Heartbeat err: %v", err)
	}
	if lease.calls != 0 {
		t.Fatalf("镜像无实例身份时不得续租,got %+v", lease)
	}
}

// Model B 隔离护栏:authRepo != nil 时 localTicketBinding 必须返回零值,即使 fleet
// 碰巧实现了 localHubCredentialSource 也不碰本地凭据源(线上权威只认 Redis 授权记录)。
func TestLocalTicketBinding_ModelBAuthorityNeverTakesLocalPath(t *testing.T) {
	const pod = "pandora-hub-global-1"
	uc, _, _ := newLocalCredUsecase(pod, testLocalCredential())
	uc.authRepo = &data.RedisHubAuthRepo{} // 非 nil 即 Model B 姿态;本测试不触其方法

	a := &hubv1.HubAssignmentStorageRecord{
		PlayerId: 1004, HubPodName: pod, AssignmentId: "assign-x",
	}
	if got := uc.localTicketBinding(a); !unboundTicketBinding(got) {
		t.Fatalf("Model B 下 localTicketBinding 必须返回零值,got %+v", got)
	}
}
