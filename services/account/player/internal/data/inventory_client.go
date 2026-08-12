// inventory_client.go — player 调 inventory.CheckInstancesOwned 做出战装备预设的精确实例归属校验。
//
// 接线对齐 battle_result / mail / trade / auction 的跨服务客户端:内网 insecure 直连、无 JWT。
// CheckInstancesOwned 是系统接口,要求 callerID==0(后端内部直连),故必须走 MustDialInsecure;
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

// GrpcInstanceOwnershipChecker 用 inventory 服务 gRPC client 实现 biz.InstanceOwnershipChecker。
type GrpcInstanceOwnershipChecker struct {
	conn *grpc.ClientConn
	cli  inventoryv1.InventoryServiceClient
}

// NewGrpcInstanceOwnershipChecker 直连 inventory 服务 endpoint(host:port,内网 insecure)。
func NewGrpcInstanceOwnershipChecker(inventoryAddr string) *GrpcInstanceOwnershipChecker {
	conn := grpcclient.MustDialInsecure(inventoryAddr)
	return &GrpcInstanceOwnershipChecker{conn: conn, cli: inventoryv1.NewInventoryServiceClient(conn)}
}

// Close 关闭底层连接。
func (g *GrpcInstanceOwnershipChecker) Close() error {
	if g.conn != nil {
		return g.conn.Close()
	}
	return nil
}

// CheckInstancesOwned 返回入参中 instance_id + item_config_id 都精确匹配当前归属的
// ID 子集与权威鉴定快照。
//
// 传输错误与非 OK 业务码都原样返回 error,绝不降级成空集:调用方要靠
// 「查询失败」与「一件都没有」可区分才能 fail-closed(§9.22)。
func (g *GrpcInstanceOwnershipChecker) CheckInstancesOwned(
	ctx context.Context, playerID uint64, equipment []EquipmentSlot,
) (InstanceOwnershipResult, error) {
	queries := toInventoryOwnershipQueries(equipment)
	resp, err := g.cli.CheckInstancesOwned(ctx, &inventoryv1.CheckInstancesOwnedRequest{
		PlayerId:  playerID,
		Instances: queries,
	})
	if err != nil {
		return InstanceOwnershipResult{}, err
	}
	if resp.GetCode() != commonv1.ErrCode_OK {
		return InstanceOwnershipResult{}, errcode.New(errcode.Code(resp.GetCode()), "inventory check instances owned code=%d", resp.GetCode())
	}
	return toInstanceOwnershipResult(resp), nil
}

func toInstanceOwnershipResult(resp *inventoryv1.CheckInstancesOwnedResponse) InstanceOwnershipResult {
	result := InstanceOwnershipResult{
		OwnedInstanceIDs: append([]uint64(nil), resp.GetOwnedInstanceIds()...),
		OwnedInstances:   make([]OwnedEquipmentInstance, 0, len(resp.GetOwnedInstances())),
	}
	for _, inst := range resp.GetOwnedInstances() {
		item := OwnedEquipmentInstance{
			InstanceID: inst.GetInstanceId(), ItemConfigID: inst.GetItemConfigId(), Identified: inst.GetIdentified(),
			Attributes: make([]EquipmentAttributeSnapshot, 0, len(inst.GetAttributes())),
		}
		for _, attr := range inst.GetAttributes() {
			item.Attributes = append(item.Attributes, EquipmentAttributeSnapshot{AttrID: attr.GetAttrId(), Value: attr.GetValue()})
		}
		result.OwnedInstances = append(result.OwnedInstances, item)
	}
	return result
}

func toInventoryOwnershipQueries(equipment []EquipmentSlot) []*inventoryv1.InstanceOwnershipQuery {
	queries := make([]*inventoryv1.InstanceOwnershipQuery, 0, len(equipment))
	for _, e := range equipment {
		queries = append(queries, &inventoryv1.InstanceOwnershipQuery{
			InstanceId: e.InstanceID, ItemConfigId: e.ItemConfigID,
		})
	}
	return queries
}
