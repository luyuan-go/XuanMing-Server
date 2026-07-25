// inventory_client.go — player 调 inventory.CheckItemsOwned 做出战装备预设的拥有权校验
// (2026-07-25,补齐 conf.go 里 2026-06-17 审查留下的 ownEquipment TODO)。
//
// 接线对齐 battle_result / mail / trade / auction 的跨服务客户端:内网 insecure 直连、无 JWT。
// CheckItemsOwned 是系统接口,要求 callerID==0(后端内部直连),故必须走 MustDialInsecure;
// 客户端 RPC GetInventory 反过来要求 callerID>0,内部直连会被判 ERR_UNAUTHORIZED,不能复用。
package data

import (
	"context"

	"google.golang.org/grpc"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/grpcclient"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"
)

// GrpcItemOwnershipChecker 用 inventory 服务 gRPC client 实现 biz.ItemOwnershipChecker。
type GrpcItemOwnershipChecker struct {
	conn *grpc.ClientConn
	cli  inventoryv1.InventoryServiceClient
}

// NewGrpcItemOwnershipChecker 直连 inventory 服务 endpoint(host:port,内网 insecure)。
func NewGrpcItemOwnershipChecker(inventoryAddr string) *GrpcItemOwnershipChecker {
	conn := grpcclient.MustDialInsecure(inventoryAddr)
	return &GrpcItemOwnershipChecker{conn: conn, cli: inventoryv1.NewInventoryServiceClient(conn)}
}

// Close 关闭底层连接。
func (g *GrpcItemOwnershipChecker) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// CheckItemsOwned 返回入参集合中玩家确实持有的子集。
//
// 传输错误与非 OK 业务码都原样返回 error,绝不降级成空集:调用方要靠
// 「查询失败」与「一件都没有」可区分才能 fail-closed(§9.22)。
func (g *GrpcItemOwnershipChecker) CheckItemsOwned(ctx context.Context, playerID uint64, itemConfigIDs []uint32) ([]uint32, error) {
	resp, err := g.cli.CheckItemsOwned(ctx, &inventoryv1.CheckItemsOwnedRequest{
		PlayerId:      playerID,
		ItemConfigIds: itemConfigIDs,
	})
	if err != nil {
		return nil, err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return nil, errcode.New(errcode.Code(resp.GetCode()), "inventory check items owned code=%d", resp.GetCode())
	}
	return resp.GetOwnedItemConfigIds(), nil
}
