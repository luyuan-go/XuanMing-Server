// team_reader.go 实现 biz.TeamReader:通过 gRPC 拉取 team 服务的队伍快照。
package data

import (
	"context"
	"github.com/luyuancpp/pandora/pkg/errcode"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/luyuancpp/pandora/pkg/grpcclient"
	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// GrpcTeamReader 用 team 服务 gRPC client 实现 biz.TeamReader。
type GrpcTeamReader struct {
	conn *grpc.ClientConn
	cli  teamv1.TeamServiceClient
	// signer 给 BeginTeamMatch / EndTeamMatch 签 request-bound 凭据(A-13)。
	// nil = 未配密钥,照常发出;team 侧按其 require 档决定观察放行还是拒。
	signer *internalrpcauth.Signer
}

// NewGrpcTeamReader 直连 team 服务 endpoint(host:port,内网 insecure)。
func NewGrpcTeamReader(teamAddr string, signer *internalrpcauth.Signer) *GrpcTeamReader {
	conn := grpcclient.MustDialInsecure(teamAddr)
	return &GrpcTeamReader{conn: conn, cli: teamv1.NewTeamServiceClient(conn), signer: signer}
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
	ctx context.Context, teamID, captainID uint64, operationID string, leaseMs int64, requireReady bool,
) (*teamv1.Team, uint64, error) {
	ctx, serr := g.signCall(ctx, teamv1.TeamService_BeginTeamMatch_FullMethodName, teamID)
	if serr != nil {
		return nil, 0, serr
	}
	resp, err := g.cli.BeginTeamMatch(ctx, &teamv1.BeginTeamMatchRequest{
		TeamId:      teamID,
		CaptainId:   captainID,
		OperationId: operationID,
		LeaseMs:     leaseMs,
		// 关卡表 ready_mode 的解析结果。旧 team 服务不认这个字段会直接忽略 → 退化成无门槛,
		// 与本字段上线前一致(§9.21 两个方向都不会把玩家挡在开局之外)。
		RequireReady: requireReady,
	})
	if err != nil {
		return nil, 0, err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return nil, 0, errcode.New(errcode.Code(resp.GetCode()),
			"team.BeginTeamMatch code=%d team=%d", resp.GetCode(), teamID)
	}
	if resp.GetTeam() == nil {
		return nil, 0, errcode.New(errcode.ErrMatchTeamNotReady, "team %d returned empty roster", teamID)
	}
	// ready_generation=0 = 旧 team 服务(滚动升级窗口),照常建票;
	// EndTeamMatch 传 0 即退化为「只在还挂着 ready 时复位」的旧语义。
	return resp.GetTeam(), resp.GetReadyGeneration(), nil
}

// EndTeamMatch 调 team 服务在对局释放时复位队伍准备状态(见 biz.TeamReader 注释)。
//
// 与 BeginTeamMatch 一样不把非 OK code 压成 nil:释放链靠 battle_result outbox 重投到
// 成功为止,吞掉失败就等于让队伍永远停在 READY —— 那正是 INC-20260813-001 的第一根因。
// team 侧已把「本就没什么可复位的」(队伍已解散 / 已不存在 / 成员已离队)返回 OK,
// 所以这里如实上抛不会造成 outbox 空转。
func (g *GrpcTeamReader) EndTeamMatch(ctx context.Context, teamID uint64, playerIDs []uint64, expectedReadyGeneration uint64) error {
	ctx, serr := g.signCall(ctx, teamv1.TeamService_EndTeamMatch_FullMethodName, teamID)
	if serr != nil {
		return serr
	}
	resp, err := g.cli.EndTeamMatch(ctx, &teamv1.EndTeamMatchRequest{
		TeamId:                  teamID,
		PlayerIds:               playerIDs,
		ExpectedReadyGeneration: expectedReadyGeneration,
	})
	if err != nil {
		// 滚动升级共存窗口(§9.21):team 还没滚到带本 RPC 的版本时回 Unimplemented。
		// 这一类**重试永远不会成功**,必须与「对端会做只是这次没做成」区分开,
		// 否则 outbox 会一直空转并积压 canonical match。转成 ErrNotImplemented 交给
		// biz 按弱依赖降级(见 endTeamMatches)。
		if status.Code(err) == codes.Unimplemented {
			return errcode.New(errcode.ErrNotImplemented,
				"team.EndTeamMatch not implemented by peer (rolling upgrade window) team=%d", teamID)
		}
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()),
			"team.EndTeamMatch code=%d team=%d", resp.GetCode(), teamID)
	}
	return nil
}

// signCall 给一次东西向调用签 request-bound 凭据(方法 + team_id + 时间戳 + 一次性 nonce)。
// 未配 signer 时原样返回 ctx:照常发出,由 team 侧按其 require 档决定观察放行还是拒 ——
// 这样两边的密钥可以分两次发布配上,不产生「谁必须先上线」的隐含约束(§9.21)。
func (g *GrpcTeamReader) signCall(ctx context.Context, fullMethod string, teamID uint64) (context.Context, error) {
	if g.signer == nil {
		return ctx, nil
	}
	signed, err := g.signer.SignContext(ctx, fullMethod, teamID)
	if err != nil {
		return nil, errcode.NewCause(errcode.ErrInternal, err,
			"sign team call credential method=%s team=%d", fullMethod, teamID)
	}
	return signed, nil
}
