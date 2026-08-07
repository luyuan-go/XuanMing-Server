// team_reader.go 实现 biz.TeamReader:通过 gRPC 拉取 team 服务的队伍快照。
package data

import (
	"context"
	"github.com/luyuancpp/pandora/pkg/errcode"

	"google.golang.org/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcclient"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// GrpcTeamReader 用 team 服务 gRPC client 实现 biz.TeamReader。
type GrpcTeamReader struct {
	conn *grpc.ClientConn
	cli  teamv1.TeamServiceClient
}

// NewGrpcTeamReader 直连 team 服务 endpoint(host:port,内网 insecure)。
func NewGrpcTeamReader(teamAddr string) *GrpcTeamReader {
	conn := grpcclient.MustDialInsecure(teamAddr)
	return &GrpcTeamReader{conn: conn, cli: teamv1.NewTeamServiceClient(conn)}
}

// Close 关闭底层连接。
func (g *GrpcTeamReader) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// GetTeam 调 team 服务 GetTeam,返回完整队伍快照。
// team 服务返回非 OK code 时,统一转成 (nil, false, nil)(由 biz 决定如何处理)。
func (g *GrpcTeamReader) GetTeam(ctx context.Context, teamID uint64) (*teamv1.Team, bool, error) {
	resp, err := g.cli.GetTeam(ctx, &teamv1.GetTeamRequest{TeamId: teamID})
	if err != nil {
		return nil, false, err
	}
	if resp.GetCode() != commonv1.ErrCode_OK || resp.GetTeam() == nil {
		return nil, false, nil
	}
	return resp.GetTeam(), true, nil
}

// BeginTeamMatch 调 team 服务在其乐观锁内冻结名单并返回快照(见 biz.TeamReader 注释)。
//
// 与 GetTeam 不同,这里**不把非 OK code 压成 (nil,false,nil)**:组票拿不到锁是一个需要
// 调用方区分对待的结果(队伍不 READY / 不是队长 / 正被另一次组票占着),
// 压成"没找到"会让 StartMatch 报一个误导性的错误,也会把可重试的竞争说成终态。
func (g *GrpcTeamReader) BeginTeamMatch(
	ctx context.Context, teamID, captainID uint64, operationID string, leaseMs int64,
) (*teamv1.Team, error) {
	resp, err := g.cli.BeginTeamMatch(ctx, &teamv1.BeginTeamMatchRequest{
		TeamId:      teamID,
		CaptainId:   captainID,
		OperationId: operationID,
		LeaseMs:     leaseMs,
	})
	if err != nil {
		return nil, err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return nil, errcode.New(errcode.Code(resp.GetCode()),
			"team.BeginTeamMatch code=%d team=%d", resp.GetCode(), teamID)
	}
	if resp.GetTeam() == nil {
		return nil, errcode.New(errcode.ErrMatchTeamNotReady, "team %d returned empty roster", teamID)
	}
	return resp.GetTeam(), nil
}
