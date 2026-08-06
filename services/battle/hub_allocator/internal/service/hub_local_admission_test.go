package service

import (
	"context"
	"testing"

	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/biz"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/data"
)

// localAdmissionRepo 只实现本用例需要的两个读方法:准入复核读归属,
// ownerTargetForHubTicket 在归属未带 uid/epoch 时回源分片镜像。
type localAdmissionRepo struct {
	data.HubRepo
	assignment *hubv1.HubAssignmentStorageRecord
	shard      *hubv1.HubShardStorageRecord
}

func (r *localAdmissionRepo) GetAssignment(context.Context, uint64) (*hubv1.HubAssignmentStorageRecord, bool, error) {
	if r.assignment == nil {
		return nil, false, nil
	}
	return r.assignment, true, nil
}

func (r *localAdmissionRepo) GetShard(context.Context, string) (*hubv1.HubShardStorageRecord, bool, error) {
	if r.shard == nil {
		return nil, false, nil
	}
	return r.shard, true, nil
}

func newLocalAdmissionService(t *testing.T) *HubService {
	t.Helper()
	repo := &localAdmissionRepo{
		assignment: &hubv1.HubAssignmentStorageRecord{
			PlayerId: 42, AssignmentId: "assignment-local-1", HubPodName: "pandora-hub-local-abc12345",
		},
		// 归属记录在 legacy 面恒无 writer-v2 绑定;实例身份由 local fleet 播种进分片镜像,
		// ownerTargetForHubTicket 据此回源补齐 —— 与签票点 Begin 的目标同源。
		shard: &hubv1.HubShardStorageRecord{
			HubPodName: "pandora-hub-local-abc12345", GameserverUid: "uid-local-1", AuthEpoch: 1,
		},
	}
	svc := NewHubService(biz.NewHubUsecase(repo, nil, nil, conf.HubConf{}))
	svc.SetLocalAdmission(true)
	return svc
}

// TestLocalAdmissionAdmitsWithoutModelBCredential 锁住本次修复的正向行为:
// mode=local 下 DS 的 Admission ACK 不携带 Model B 凭据(ds_auth.mode=off 根本签不出),
// 必须仍能拿到 OK+Admitted=true。回归前这里是 ERR_INVALID_ARG,DS 侧判"明确拒绝"
// 直接 KickPlayer,玩家刚连上大厅就被踢下线。
func TestLocalAdmissionAdmitsWithoutModelBCredential(t *testing.T) {
	svc := newLocalAdmissionService(t)
	resp, err := svc.AcknowledgeAdmission(context.Background(), &hubv1.AcknowledgeAdmissionRequest{
		PlayerId: 42, AssignmentId: "assignment-local-1", HubPodName: "pandora-hub-local-abc12345",
	})
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_OK || !resp.GetAdmitted() {
		t.Fatalf("local admission: code=%v admitted=%v, want OK/true", resp.GetCode(), resp.GetAdmitted())
	}
}

// TestLocalAdmissionRejectsStaleAssignment 准入仍须复核归属三元组:票据签发后玩家
// 已被重新分配到别的 pod 时不得开门(否则一人两 DS)。
func TestLocalAdmissionRejectsStaleAssignment(t *testing.T) {
	svc := newLocalAdmissionService(t)
	resp, err := svc.AcknowledgeAdmission(context.Background(), &hubv1.AcknowledgeAdmissionRequest{
		PlayerId: 42, AssignmentId: "assignment-local-1", HubPodName: "pandora-hub-local-OTHER",
	})
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if resp.GetCode() == commonv1.ErrCode_OK || resp.GetAdmitted() {
		t.Fatalf("stale pod must not be admitted: code=%v admitted=%v", resp.GetCode(), resp.GetAdmitted())
	}
}

// TestLocalAdmissionIncompleteIdentityRejected 身份不全一律拒,不给"空 assignment 也放行"。
func TestLocalAdmissionIncompleteIdentityRejected(t *testing.T) {
	svc := newLocalAdmissionService(t)
	for _, req := range []*hubv1.AcknowledgeAdmissionRequest{
		{AssignmentId: "assignment-local-1", HubPodName: "pandora-hub-local-abc12345"},
		{PlayerId: 42, HubPodName: "pandora-hub-local-abc12345"},
		{PlayerId: 42, AssignmentId: "assignment-local-1"},
	} {
		resp, err := svc.AcknowledgeAdmission(context.Background(), req)
		if err != nil {
			t.Fatalf("transport error: %v", err)
		}
		if resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG || resp.GetAdmitted() {
			t.Fatalf("incomplete identity %+v: code=%v admitted=%v, want INVALID_ARG", req, resp.GetCode(), resp.GetAdmitted())
		}
	}
}

// TestModelBAuthorityNeverTakesLocalAdmissionPath 是**线上 k8s 隔离护栏**:
// 即使 localAdmission 被误置为 true,只要 modelBAuthority 开着(集群恒为 agones+enforce+
// authority_mode=redis,由 gen_cluster_config.ps1 硬校验),就必须走原 Model B 分支——
// 无 DS 回调凭据即 ERR_UNAUTHORIZED,绝不能被 legacy 通道降级放行。
func TestModelBAuthorityNeverTakesLocalAdmissionPath(t *testing.T) {
	svc := NewHubService(nil) // usecase 为 nil:一旦误走 local 分支必 panic,是比断言更硬的证据
	svc.SetModelBAuthority(true)
	svc.SetLocalAdmission(true)
	resp, err := svc.AcknowledgeAdmission(context.Background(), &hubv1.AcknowledgeAdmissionRequest{
		PlayerId: 1, AssignmentId: "assignment-1", HubPodName: "hub-0",
		AdmissionId: "00000000-0000-4000-8000-000000000001", AdmissionSeq: 1,
	})
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED || resp.GetAdmitted() {
		t.Fatalf("model B must stay fail-closed: code=%v admitted=%v, want ERR_UNAUTHORIZED", resp.GetCode(), resp.GetAdmitted())
	}
}

// localDepartureRequest 是 DS 真实发出的离场 ACK 形状:QueueHubDeparture 在
// admission_id 为空或 admission_seq=0 时压根不入队,所以线上到达的请求五元组必然齐备。
func localDepartureRequest() *hubv1.AcknowledgeDepartureRequest {
	return &hubv1.AcknowledgeDepartureRequest{
		PlayerId: 42, AssignmentId: "assignment-local-1", HubPodName: "pandora-hub-local-abc12345",
		AdmissionId: "00000000-0000-4000-8000-000000000001", AdmissionSeq: 1,
	}
}

// TestLocalDepartureReturnsOKAndDeparted 锁住本次修复的正向行为:mode=local 下 DS 的
// Departure ACK 同样拿不出 Model B 凭据,必须仍拿到 OK+Departed=true。回归前这里**与请求
// 内容无关地**恒返回 ERR_INVALID_ARG,而 DS 侧 ShouldCompleteQueuedHubDeparture 只认
// code=0 才出队,于是按 1s 周期永久重发:DS 日志每秒一条 "DS call failed:
// .../AcknowledgeDeparture code=4",PendingHubDepartures 条目永不释放(2026-08-06 实测)。
func TestLocalDepartureReturnsOKAndDeparted(t *testing.T) {
	svc := newLocalAdmissionService(t)
	resp, err := svc.AcknowledgeDeparture(context.Background(), localDepartureRequest())
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_OK || !resp.GetDeparted() {
		t.Fatalf("local departure: code=%v departed=%v, want OK/true", resp.GetCode(), resp.GetDeparted())
	}
}

// TestLocalDepartureStaleAssignmentStillCompletes 钉死与 Admission 刻意相反的一条:离场
// **不得**复核归属三元组。玩家离场最常见的原因就是归属已被改写(进 Battle / 顶号 / 切线),
// 若照搬 TestLocalAdmissionRejectsStaleAssignment 的 fail-closed,这里必然返回非 OK,
// DS 就又回到每秒重试 —— 正好把本次修的 bug 原样重建。准入是开门(归属过期必须拒,否则
// 一人两 DS),离场是关门,没有可授权的对象。
func TestLocalDepartureStaleAssignmentStillCompletes(t *testing.T) {
	svc := newLocalAdmissionService(t)
	req := localDepartureRequest()
	req.HubPodName = "pandora-hub-local-OTHER" // 归属已指向别处
	resp, err := svc.AcknowledgeDeparture(context.Background(), req)
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_OK || !resp.GetDeparted() {
		t.Fatalf("stale assignment departure: code=%v departed=%v, want OK/true(否则 DS 永久重试)",
			resp.GetCode(), resp.GetDeparted())
	}
}

// TestLocalDepartureRejectsIncompleteIdentity 身份不全仍须拒:真实 DS 不可能发出这种请求
// (QueueHubDeparture 有同样的门),留着是防止有人把本分支放宽成"无脑回 OK"。
func TestLocalDepartureRejectsIncompleteIdentity(t *testing.T) {
	svc := newLocalAdmissionService(t)
	for _, mutate := range []func(*hubv1.AcknowledgeDepartureRequest){
		func(r *hubv1.AcknowledgeDepartureRequest) { r.PlayerId = 0 },
		func(r *hubv1.AcknowledgeDepartureRequest) { r.AssignmentId = "" },
		func(r *hubv1.AcknowledgeDepartureRequest) { r.HubPodName = "" },
	} {
		req := localDepartureRequest()
		mutate(req)
		resp, err := svc.AcknowledgeDeparture(context.Background(), req)
		if err != nil {
			t.Fatalf("transport error: %v", err)
		}
		if resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG || resp.GetDeparted() {
			t.Fatalf("incomplete identity %+v: code=%v departed=%v, want INVALID_ARG",
				req, resp.GetCode(), resp.GetDeparted())
		}
	}
}

// TestModelBAuthorityNeverTakesLocalDeparturePath 与 Admission 侧同款**线上 k8s 隔离护栏**:
// 即使 localAdmission 被误置为 true,只要 modelBAuthority 开着就必须走原 Model B 分支,
// 无 DS 回调凭据即 ERR_UNAUTHORIZED,绝不能被 legacy 通道降级成"无脑放行"。
func TestModelBAuthorityNeverTakesLocalDeparturePath(t *testing.T) {
	svc := NewHubService(nil) // usecase 为 nil:一旦误走 local 分支必 panic,是比断言更硬的证据
	svc.SetModelBAuthority(true)
	svc.SetLocalAdmission(true)
	resp, err := svc.AcknowledgeDeparture(context.Background(), localDepartureRequest())
	if err != nil {
		t.Fatalf("transport error: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED || resp.GetDeparted() {
		t.Fatalf("model B must stay fail-closed: code=%v departed=%v, want ERR_UNAUTHORIZED",
			resp.GetCode(), resp.GetDeparted())
	}
}
