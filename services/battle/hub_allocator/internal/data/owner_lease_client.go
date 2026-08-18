// owner_lease_client.go — hub_allocator 调 owner.RenewInstanceLease 双写实例租约
// (owner-authority.md migrate ⑥,2026-07-22)。
//
// 接线对齐 battle_result 的 inventory_client:内网 insecure 直连,无 JWT(系统接口,
// owner service 层拒带玩家 JWT 的调用)。租约秒数固定用 placement.DSFenceLeaseMaxSeconds
// (协议上限;owner 侧还会再钳一次,双保险)。
package data

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	"github.com/luyuancpp/pandora/pkg/placement"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	ownerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/owner/v1"
)

// ownerLeaseRPCTimeout 单次续租调用上限:心跳链路内同步调用,必须远小于 DS 心跳周期(5s),
// 让弱依赖失败快速放行、强依赖失败不拖垮心跳超时预算。
const ownerLeaseRPCTimeout = 2 * time.Second

// GrpcOwnerLeaseRenewer 用 owner 服务 gRPC client 实现 biz.OwnerLeaseRenewer。
type GrpcOwnerLeaseRenewer struct {
	conn *grpc.ClientConn
	cli  ownerv1.OwnerServiceClient
}

// NewGrpcOwnerLeaseRenewer 直连 owner 服务 endpoint(host:port,内网 insecure)。
func NewGrpcOwnerLeaseRenewer(ownerAddr string) *GrpcOwnerLeaseRenewer {
	conn := grpcclient.MustDialInsecure(ownerAddr)
	return &GrpcOwnerLeaseRenewer{conn: conn, cli: ownerv1.NewOwnerServiceClient(conn)}
}

// Close 关闭底层连接。
func (g *GrpcOwnerLeaseRenewer) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// RenewInstanceLease 续写实例租约(deadline 只前进,owner 侧幂等)。
func (g *GrpcOwnerLeaseRenewer) RenewInstanceLease(ctx context.Context, podName, instanceUID string, instanceEpoch uint32, releaseTrack string) error {
	callCtx, cancel := context.WithTimeout(ctx, ownerLeaseRPCTimeout)
	defer cancel()
	resp, err := g.cli.RenewInstanceLease(callCtx, &ownerv1.RenewInstanceLeaseRequest{
		Target: &ownerv1.OwnerTarget{
			PodName:       podName,
			InstanceUid:   instanceUID,
			InstanceEpoch: instanceEpoch,
			ReleaseTrack:  releaseTrack,
		},
		LeaseSeconds: placement.DSFenceLeaseMaxSeconds,
	})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()),
			"owner renew lease rejected pod=%s uid=%s", podName, instanceUID)
	}
	return nil
}

// ── migrate ①-④ 弱依赖调用(owner-authority.md §4;同一连接复用)─────────────

// QueryOwner 读当前 owner 记录(migrate 弱依赖:失败由 biz 告警放行)。
func (g *GrpcOwnerLeaseRenewer) QueryOwner(ctx context.Context, playerID uint64) (OwnerRecordView, error) {
	callCtx, cancel := context.WithTimeout(ctx, ownerLeaseRPCTimeout)
	defer cancel()
	resp, err := g.cli.QueryOwner(callCtx, &ownerv1.QueryOwnerRequest{PlayerId: playerID})
	if err != nil {
		return OwnerRecordView{}, err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return OwnerRecordView{}, errcode.New(errcode.Code(resp.GetCode()), "owner query rejected player=%d", playerID)
	}
	return recordView(resp.GetRecord()), nil
}

// BeginTransition 发起 owner 迁移(EPOCH_CONFLICT 也按 error 返回)。
//
// 回传记录而非只回 error:owner.proto BeginTransitionResponse.record 成功时是**新记录**、
// EPOCH_CONFLICT 时是**当前记录**,两者调用方都要用——
//   - 成功:biz 侧要拿新记录的 owner_epoch + operation_id 才能在部分失败时精确 Release
//     回滚(Release 要求两者全等,自铸或推算都不行);
//   - 冲突:当前记录供重查决策。
//
// 此前这里把 resp.Record 整个丢掉,回滚所需的两个值在调用方**无从取得**。
func (g *GrpcOwnerLeaseRenewer) BeginTransition(ctx context.Context, playerID, expectEpoch uint64, operationID string, ownerType int8, target OwnerTargetView) (OwnerRecordView, error) {
	callCtx, cancel := context.WithTimeout(ctx, ownerLeaseRPCTimeout)
	defer cancel()
	resp, err := g.cli.BeginTransition(callCtx, &ownerv1.BeginTransitionRequest{
		PlayerId:    playerID,
		ExpectEpoch: expectEpoch,
		OperationId: operationID,
		OwnerType:   ownerv1.OwnerType(ownerType),
		Target:      targetProto(target),
		// 来源版本单独发,不进 Target(见 OwnerTargetView.SourceRevision 注释)。
		SourceRevision: target.SourceRevision,
	})
	if err != nil {
		return OwnerRecordView{}, err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return recordView(resp.GetRecord()), errcode.New(errcode.Code(resp.GetCode()), "owner begin rejected player=%d", playerID)
	}
	return recordView(resp.GetRecord()), nil
}

// Admit 准入提交;BARRIER_NOT_OPEN 返回剩余毫秒(biz 下次心跳重试)。
func (g *GrpcOwnerLeaseRenewer) Admit(ctx context.Context, playerID, ownerEpoch uint64, operationID string, target OwnerTargetView) (int64, error) {
	callCtx, cancel := context.WithTimeout(ctx, ownerLeaseRPCTimeout)
	defer cancel()
	resp, err := g.cli.Admit(callCtx, &ownerv1.AdmitRequest{
		PlayerId:    playerID,
		OwnerEpoch:  ownerEpoch,
		OperationId: operationID,
		Target:      targetProto(target),
	})
	if err != nil {
		return 0, err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return resp.GetRetryAfterMs(), errcode.New(errcode.Code(resp.GetCode()), "owner admit rejected player=%d", playerID)
	}
	return 0, nil
}

func targetProto(t OwnerTargetView) *ownerv1.OwnerTarget {
	return &ownerv1.OwnerTarget{
		PodName:                  t.PodName,
		InstanceUid:              t.InstanceUID,
		InstanceEpoch:            t.InstanceEpoch,
		AssignmentOrAllocationId: t.AssignmentOrAllocationID,
		ReleaseTrack:             t.ReleaseTrack,
	}
}

func recordView(r *ownerv1.OwnerRecord) OwnerRecordView {
	return OwnerRecordView{
		OwnerEpoch:               r.GetOwnerEpoch(),
		OwnerType:                int8(r.GetOwnerType()),
		Phase:                    int8(r.GetPhase()),
		PodName:                  r.GetTarget().GetPodName(),
		InstanceUID:              r.GetTarget().GetInstanceUid(),
		InstanceEpoch:            r.GetTarget().GetInstanceEpoch(),
		AssignmentOrAllocationID: r.GetTarget().GetAssignmentOrAllocationId(),
		ReleaseTrack:             r.GetTarget().GetReleaseTrack(),
		OperationID:              r.GetOperationId(),
		AdmitNotBeforeMs:         r.GetAdmitNotBeforeMs(),
	}
}

// OwnerTargetView exact DS 实例身份视图(pb 解耦:biz 依赖本类型不依赖生成代码)。
type OwnerTargetView struct {
	PodName                  string
	InstanceUID              string
	InstanceEpoch            uint32
	AssignmentOrAllocationID string
	ReleaseTrack             string

	// SourceRevision 是本次归属的来源版本(INC-20260818-003)。
	//
	// 它**不进 proto 的 OwnerTarget**:OwnerTarget 是 exact 身份,Admit 要拿它做全等校验,
	// 把一个会变的版本号塞进身份里会让 Admit 无谓失配。它只随 BeginTransitionRequest
	// 单独发出去。放在本视图里是为了搭现成的 target 传递链,且让
	// ownerTargetViewEqual 天然把"版本变了"算作"归属变了"——那本来就是同一件事。
	//
	// 0 = 本部署未启用写者租约(dev / 单副本),或调用方尚未滚上本协议。
	SourceRevision uint64
}

// OwnerRecordView 当前 owner 记录视图(migrate 决策用最小字段集)。
type OwnerRecordView struct {
	OwnerEpoch               uint64
	OwnerType                int8
	Phase                    int8
	PodName                  string
	InstanceUID              string
	InstanceEpoch            uint32
	AssignmentOrAllocationID string
	ReleaseTrack             string
	OperationID              string
	AdmitNotBeforeMs         int64
}
