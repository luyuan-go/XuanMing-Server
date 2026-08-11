package service

import (
	"context"
	"testing"

	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"
)

func TestInstanceOwnershipProtoMappingKeepsExactPair(t *testing.T) {
	got := toInstanceOwnershipQueries([]*inventoryv1.InstanceOwnershipQuery{
		{InstanceId: 9001, ItemConfigId: 10003},
		{InstanceId: 9002, ItemConfigId: 10027},
	})
	if len(got) != 2 || got[0].InstanceID != 9001 || got[0].ItemConfigID != 10003 ||
		got[1].InstanceID != 9002 || got[1].ItemConfigID != 10027 {
		t.Fatalf("mapping lost exact pair: %+v", got)
	}
}

func TestNewSystemItemRPCsRejectPlayerCallerBeforeUsecase(t *testing.T) {
	ctx := plog.WithPlayerID(context.Background(), 7)
	svc := &InventoryService{} // 鉴权必须在触达 usecase 前拒绝，nil 可证明调用顺序。
	owned, err := svc.CheckInstancesOwned(ctx, &inventoryv1.CheckInstancesOwnedRequest{PlayerId: 7})
	if err != nil || owned.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("CheckInstancesOwned code=%v err=%v", owned.GetCode(), err)
	}
	consume, err := svc.ConsumeBattleItem(ctx, &inventoryv1.ConsumeBattleItemRequest{
		PlayerId: 7, ItemConfigId: 10001, Count: 1, IdempotencyKey: "k",
	})
	if err != nil || consume.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("ConsumeBattleItem code=%v err=%v", consume.GetCode(), err)
	}
	discard, err := svc.DiscardBattleItem(ctx, &inventoryv1.DiscardBattleItemRequest{
		PlayerId: 7, ItemConfigId: 10002, Count: 1, IdempotencyKey: "k",
	})
	if err != nil || discard.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("DiscardBattleItem code=%v err=%v", discard.GetCode(), err)
	}
}

func TestNewPlayerItemRPCsRejectBodyIdentityMismatch(t *testing.T) {
	ctx := plog.WithPlayerID(context.Background(), 7)
	svc := &InventoryService{}
	discard, err := svc.DiscardItem(ctx, &inventoryv1.DiscardItemRequest{PlayerId: 8})
	if err != nil || discard.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("DiscardItem code=%v err=%v", discard.GetCode(), err)
	}
	sell, err := svc.SellInstance(ctx, &inventoryv1.SellInstanceRequest{PlayerId: 8})
	if err != nil || sell.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("SellInstance code=%v err=%v", sell.GetCode(), err)
	}
}
