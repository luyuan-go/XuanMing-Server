// mmr_reader.go 实现 biz.MMRReader:通过 gRPC 调 player 服务读玩家当前 MMR。
//
// 设计(W4 ④,2026-06-06):
//   - W4 ③ player 服务未上线时用 biz.StaticMMRReader 兜底(全返 base_mmr)
//   - W4 ④ player 上线后,battle_result 经此 reader 读真实当前 MMR 算 Elo 期望胜率
//   - 弱依赖语义:player.GetMMR 失败时由 biz.assignMMR 回退到 cfg.BaseMMR(不阻断落库)
package data

import (
	"context"

	"google.golang.org/grpc"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
)

// GrpcMMRReader 用 player 服务 gRPC client 实现 biz.MMRReader。
type GrpcMMRReader struct {
	conn *grpc.ClientConn
	cli  playerv1.PlayerServiceClient
}

// NewGrpcMMRReader 直连 player 服务 endpoint(host:port,内网 insecure)。
func NewGrpcMMRReader(playerAddr string) *GrpcMMRReader {
	conn := grpcclient.MustDialInsecure(playerAddr)
	return &GrpcMMRReader{
		conn: conn,
		cli:  playerv1.NewPlayerServiceClient(conn),
	}
}

// Close 关闭底层连接。
func (g *GrpcMMRReader) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// GetMMR 调 player.GetMMR 读玩家在某段位池下的当前 MMR。
// player 对该池无记录的玩家返回 base_mmr + OK(found=false);非 OK / 传输错误返回
// error(biz 回退 BaseMMR)。ratingPool 由本局定格值传入,空则由 player 侧归一到默认池。
func (g *GrpcMMRReader) GetMMR(ctx context.Context, playerID uint64, ratingPool string) (int, error) {
	resp, err := g.cli.GetMMR(ctx, &playerv1.GetMMRRequest{PlayerId: playerID, RatingPool: ratingPool})
	if err != nil {
		return 0, err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return 0, errcode.New(errcode.Code(resp.GetCode()), "player.GetMMR code=%d player=%d pool=%s", resp.GetCode(), playerID, ratingPool)
	}
	return int(resp.GetMmr()), nil
}
