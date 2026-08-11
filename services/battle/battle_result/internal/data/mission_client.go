// mission_client.go — battle_result 调 mission.ReportMissionFacts 转发战斗事实
// 推进任务进度(docs/design/mission.md §5.1)。
//
// 接线对齐 inventory_client / mail_client:内网 insecure 直连,无 JWT
// (ReportMissionFacts 是系统 RPC,只认 callerID==0,不在 Envoy 暴露)。
// 幂等键 progress:{match_id}:{seq}:{player_id}:mission 由 biz 给定,mission 侧
// mission_fact_receipts(uk + 请求指纹)吸收 at-least-once 重放。
package data

import (
	"context"

	"google.golang.org/grpc"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	missionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/mission/v1"
)

// GrpcMissionReporter 用 mission 服务 gRPC client 实现 biz.MissionReporter。
type GrpcMissionReporter struct {
	conn *grpc.ClientConn
	cli  missionv1.MissionServiceClient
}

// NewGrpcMissionReporter 直连 mission 服务 endpoint(host:port,内网 insecure)。
func NewGrpcMissionReporter(missionAddr string) *GrpcMissionReporter {
	conn := grpcclient.MustDialInsecure(missionAddr)
	return &GrpcMissionReporter{conn: conn, cli: missionv1.NewMissionServiceClient(conn)}
}

// Close 关闭底层连接。
func (g *GrpcMissionReporter) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// ReportMissionFact 转发单条事实(一出箱行一事实,幂等键天然唯一)。
//
// 返回 nil 表示已入账或幂等命中(already=true 同样是成功,调用方照常删出箱行);
// 非 OK code 转 errcode 让调用方退避重投。
func (g *GrpcMissionReporter) ReportMissionFact(ctx context.Context, playerID uint64,
	category, slotValue, amount uint32, idempotencyKey string) error {
	resp, err := g.cli.ReportMissionFacts(ctx, &missionv1.ReportMissionFactsRequest{
		PlayerId: playerID,
		Facts: []*missionv1.MissionFact{{
			ConditionCategory: missionv1.MissionConditionCategory(category),
			ConditionIds:      []uint32{slotValue},
			Amount:            amount,
		}},
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return errcode.New(errcode.Code(resp.GetCode()),
			"report mission fact player=%d key=%s code=%d", playerID, idempotencyKey, int32(resp.GetCode()))
	}
	return nil
}
