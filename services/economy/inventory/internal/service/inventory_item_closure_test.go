package service

import (
	"context"
	"testing"

	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/biz"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type ownershipSnapshotRepo struct {
	data.InventoryRepo
	owned []data.ItemInstance
}

func (r *ownershipSnapshotRepo) CheckInstancesOwned(context.Context, uint64, []data.InstanceOwnershipQuery) ([]data.ItemInstance, error) {
	return append([]data.ItemInstance(nil), r.owned...), nil
}

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

func TestCheckInstancesOwnedResponseContractFieldNumbers(t *testing.T) {
	fields := (&inventoryv1.CheckInstancesOwnedResponse{}).ProtoReflect().Descriptor().Fields()
	want := map[protoreflect.Name]protoreflect.FieldNumber{
		"code": 1, "owned_instance_ids": 2, "owned_instances": 3,
	}
	for name, number := range want {
		field := fields.ByName(name)
		if field == nil || field.Number() != number {
			t.Fatalf("CheckInstancesOwnedResponse.%s field=%v want number=%d", name, field, number)
		}
	}
}

func TestCheckInstancesOwnedReturnsIDsAndAuthoritativeSnapshot(t *testing.T) {
	repo := &ownershipSnapshotRepo{owned: []data.ItemInstance{{
		InstanceID: 9001, ItemConfigID: 10003, Identified: true, Bound: true, SlotIndex: 4,
		Attributes: []data.ItemAttribute{{AttrID: 21, Value: 37}, {AttrID: 8, Value: 12}},
	}}}
	svc := NewInventoryService(biz.NewInventoryUsecase(repo, conf.InventoryConf{}))
	resp, err := svc.CheckInstancesOwned(context.Background(), &inventoryv1.CheckInstancesOwnedRequest{
		PlayerId:  701,
		Instances: []*inventoryv1.InstanceOwnershipQuery{{InstanceId: 9001, ItemConfigId: 10003}},
	})
	if err != nil || resp.GetCode() != commonv1.ErrCode_OK {
		t.Fatalf("CheckInstancesOwned code=%v err=%v", resp.GetCode(), err)
	}
	if len(resp.GetOwnedInstanceIds()) != 1 || resp.GetOwnedInstanceIds()[0] != 9001 {
		t.Fatalf("owned ids=%v want=[9001]", resp.GetOwnedInstanceIds())
	}
	got := resp.GetOwnedInstances()
	if len(got) != 1 || got[0].GetInstanceId() != 9001 || got[0].GetItemConfigId() != 10003 ||
		!got[0].GetIdentified() || !got[0].GetBound() || got[0].GetSlotIndex() != 4 ||
		len(got[0].GetAttributes()) != 2 || got[0].GetAttributes()[0].GetAttrId() != 21 ||
		got[0].GetAttributes()[0].GetValue() != 37 || got[0].GetAttributes()[1].GetAttrId() != 8 ||
		got[0].GetAttributes()[1].GetValue() != 12 {
		t.Fatalf("authoritative snapshot not preserved: %+v", got)
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
