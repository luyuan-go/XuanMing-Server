package data

import (
	"testing"

	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"
)

func TestInventoryOwnershipQueryMappingKeepsExactPair(t *testing.T) {
	got := toInventoryOwnershipQueries([]EquipmentSlot{
		{Slot: 1, ItemConfigID: 10003, InstanceID: 9001},
		{Slot: 2, ItemConfigID: 10027, InstanceID: 9002},
	})
	if len(got) != 2 || got[0].GetInstanceId() != 9001 || got[0].GetItemConfigId() != 10003 ||
		got[1].GetInstanceId() != 9002 || got[1].GetItemConfigId() != 10027 {
		t.Fatalf("player -> inventory exact pair 映射漂移: %+v", got)
	}
}

func TestInventoryOwnershipResponsePreservesLegacyIDsAndInstanceDetails(t *testing.T) {
	got := toInstanceOwnershipResult(&inventoryv1.CheckInstancesOwnedResponse{
		OwnedInstanceIds: []uint64{9001, 9002},
		OwnedInstances: []*inventoryv1.ItemInstance{{
			InstanceId: 9001, ItemConfigId: 10003, Identified: true,
			Attributes: []*inventoryv1.ItemAttribute{{AttrId: 21, Value: 37}, {AttrId: 8, Value: 12}},
		}},
	})
	if len(got.OwnedInstanceIDs) != 2 || got.OwnedInstanceIDs[0] != 9001 || got.OwnedInstanceIDs[1] != 9002 {
		t.Fatalf("滚动升级兼容 IDs 丢失: %+v", got.OwnedInstanceIDs)
	}
	if len(got.OwnedInstances) != 1 || got.OwnedInstances[0].InstanceID != 9001 ||
		got.OwnedInstances[0].ItemConfigID != 10003 || !got.OwnedInstances[0].Identified ||
		len(got.OwnedInstances[0].Attributes) != 2 ||
		got.OwnedInstances[0].Attributes[0] != (EquipmentAttributeSnapshot{AttrID: 21, Value: 37}) ||
		got.OwnedInstances[0].Attributes[1] != (EquipmentAttributeSnapshot{AttrID: 8, Value: 12}) {
		t.Fatalf("鉴定详情丢失: %+v", got.OwnedInstances)
	}
}
