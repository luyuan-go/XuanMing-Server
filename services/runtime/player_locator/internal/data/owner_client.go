// owner_client.go — player_locator 只读查询 owner authority，为 HUB presence 提供跨
// assignment 的全局 fencing 顺序。locator 仍只保存可重建投影，不写 owner 权威。
package data

import (
	"context"
	"time"

	"google.golang.org/grpc"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	ownerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/owner/v1"
)

const ownerQueryTimeout = 2 * time.Second

// HubOwnerSnapshot 是 locator 校验 HUB Set 所需的 owner authority 最小快照。
type HubOwnerSnapshot struct {
	OwnerEpoch      uint64
	OperationID     string
	OwnerType       int32
	Phase           int32
	PodName         string
	InstanceUID     string
	InstanceEpoch   uint32
	AssignmentID    string
	ReleaseTrack    string
	LeaseDeadlineMs int64
}

// GrpcHubOwnerAuthority 直连内部 owner 服务，只调用 QueryOwner。
type GrpcHubOwnerAuthority struct {
	conn *grpc.ClientConn
	cli  ownerv1.OwnerServiceClient
}

// NewGrpcHubOwnerAuthority 创建 owner 查询客户端。地址来自 locator.owner_addr。
func NewGrpcHubOwnerAuthority(ownerAddr string) *GrpcHubOwnerAuthority {
	conn := grpcclient.MustDialInsecure(ownerAddr)
	return &GrpcHubOwnerAuthority{conn: conn, cli: ownerv1.NewOwnerServiceClient(conn)}
}

// Close 关闭底层连接。
func (g *GrpcHubOwnerAuthority) Close() error {
	if g != nil && g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// QueryOwner 读取当前 owner；任何 RPC/in-band 错误都原样作为 UNKNOWN 交给 biz fail-closed。
func (g *GrpcHubOwnerAuthority) QueryOwner(ctx context.Context, playerID uint64) (HubOwnerSnapshot, error) {
	if g == nil || g.cli == nil {
		return HubOwnerSnapshot{}, errcode.New(errcode.ErrUnavailable, "owner client is unavailable")
	}
	callCtx, cancel := context.WithTimeout(ctx, ownerQueryTimeout)
	defer cancel()
	resp, err := g.cli.QueryOwner(callCtx, &ownerv1.QueryOwnerRequest{PlayerId: playerID})
	if err != nil {
		return HubOwnerSnapshot{}, err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return HubOwnerSnapshot{}, errcode.New(errcode.Code(resp.GetCode()),
			"owner query rejected player=%d", playerID)
	}
	rec := resp.GetRecord()
	if rec == nil {
		return HubOwnerSnapshot{}, errcode.New(errcode.ErrInternal,
			"owner query returned nil record player=%d", playerID)
	}
	target := rec.GetTarget()
	return HubOwnerSnapshot{
		OwnerEpoch:      rec.GetOwnerEpoch(),
		OperationID:     rec.GetOperationId(),
		OwnerType:       int32(rec.GetOwnerType()),
		Phase:           int32(rec.GetPhase()),
		PodName:         target.GetPodName(),
		InstanceUID:     target.GetInstanceUid(),
		InstanceEpoch:   target.GetInstanceEpoch(),
		AssignmentID:    target.GetAssignmentOrAllocationId(),
		ReleaseTrack:    target.GetReleaseTrack(),
		LeaseDeadlineMs: rec.GetLeaseDeadlineMs(),
	}, nil
}
