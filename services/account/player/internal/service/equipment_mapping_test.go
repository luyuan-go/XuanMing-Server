package service

import (
	"testing"

	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
	"github.com/luyuancpp/pandora/services/account/player/internal/data"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestLoadoutEquipmentContractFieldNumbers(t *testing.T) {
	fields := (&playerv1.LoadoutEquipment{}).ProtoReflect().Descriptor().Fields()
	want := map[protoreflect.Name]protoreflect.FieldNumber{
		"slot": 1, "item_config_id": 2, "instance_id": 3, "identified": 4, "attributes": 5,
	}
	for name, number := range want {
		field := fields.ByName(name)
		if field == nil || field.Number() != number {
			t.Fatalf("LoadoutEquipment.%s field=%v want number=%d", name, field, number)
		}
	}
}

func TestEquipmentMappingPreservesExactInstanceID(t *testing.T) {
	protoSlots := []*playerv1.LoadoutEquipment{
		{Slot: 1, ItemConfigId: 10003, InstanceId: 9001},
		{Slot: 2, ItemConfigId: 10027, InstanceId: 9002},
	}
	dataSlots := toDataEquipment(protoSlots)
	if len(dataSlots) != 2 || dataSlots[0].InstanceID != 9001 || dataSlots[0].ItemConfigID != 10003 ||
		dataSlots[1].InstanceID != 9002 || dataSlots[1].ItemConfigID != 10027 {
		t.Fatalf("proto -> data 丢失 exact pair: %+v", dataSlots)
	}

	roundTrip := toProtoEquipment(dataSlots)
	if len(roundTrip) != 2 || roundTrip[0].GetInstanceId() != 9001 ||
		roundTrip[1].GetInstanceId() != 9002 {
		t.Fatalf("data -> proto 丢失 instance_id: %+v", roundTrip)
	}
}

func TestSetEquipmentMappingIgnoresClientSuppliedIdentificationSnapshot(t *testing.T) {
	// LoadoutEquipment 同时用于写请求与读快照，但 identified/attributes 只能由
	// GetLoadout 从 inventory 权威详情组装；客户端在 SetEquipment 伪造的增益不得进 biz/data。
	got := toDataEquipment([]*playerv1.LoadoutEquipment{{
		Slot: 1, ItemConfigId: 10003, InstanceId: 9001, Identified: true,
		Attributes: []*inventoryv1.ItemAttribute{{AttrId: 999, Value: 999999}},
	}})
	if len(got) != 1 || got[0].Slot != 1 || got[0].ItemConfigID != 10003 || got[0].InstanceID != 9001 {
		t.Fatalf("exact pair mapping failed: %+v", got)
	}
	roundTrip := toProtoEquipment(got)
	if roundTrip[0].GetIdentified() || len(roundTrip[0].GetAttributes()) != 0 {
		t.Fatalf("客户伪造鉴定快照被信任: %+v", roundTrip[0])
	}
}

func TestSetEquipmentMappingDropsEveryClientAttributeEnvelope(t *testing.T) {
	clientSnapshots := []struct {
		name       string
		identified bool
		attributes []*inventoryv1.ItemAttribute
	}{
		{name: "valid-looking authority fields", identified: true, attributes: []*inventoryv1.ItemAttribute{{AttrId: 3, Value: 100}}},
		{name: "unknown attr", identified: true, attributes: []*inventoryv1.ItemAttribute{{AttrId: 999, Value: 1}}},
		{name: "duplicate attr", identified: true, attributes: []*inventoryv1.ItemAttribute{{AttrId: 3, Value: 1}, {AttrId: 3, Value: 2}}},
		{name: "negative value", identified: true, attributes: []*inventoryv1.ItemAttribute{{AttrId: 9, Value: -1}}},
		{name: "oversized value", identified: true, attributes: []*inventoryv1.ItemAttribute{{AttrId: 7, Value: 10_001}}},
		{name: "attrs while unidentified", identified: false, attributes: []*inventoryv1.ItemAttribute{{AttrId: 3, Value: 1}}},
	}
	for _, tt := range clientSnapshots {
		t.Run(tt.name, func(t *testing.T) {
			got := toDataEquipment([]*playerv1.LoadoutEquipment{{
				Slot: 1, ItemConfigId: 10003, InstanceId: 9001,
				Identified: tt.identified, Attributes: tt.attributes,
			}})
			if len(got) != 1 || got[0] != (data.EquipmentSlot{Slot: 1, ItemConfigID: 10003, InstanceID: 9001}) {
				t.Fatalf("客户端权威字段影响了 SetEquipment data: %+v", got)
			}
			if roundTrip := toProtoEquipment(got); roundTrip[0].GetIdentified() || len(roundTrip[0].GetAttributes()) != 0 {
				t.Fatalf("客户端鉴定快照穿透 service 边界: %+v", roundTrip[0])
			}
		})
	}
}

func TestEquipmentMappingKeepsLegacyZeroForReadOnlyDisplay(t *testing.T) {
	got := toProtoEquipment([]data.EquipmentSlot{{Slot: 1, ItemConfigID: 10003}})
	if len(got) != 1 || got[0].GetInstanceId() != 0 {
		t.Fatalf("旧行应以 instance_id=0 只读回显: %+v", got)
	}
}
