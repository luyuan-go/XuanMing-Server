package service

import (
	"context"
	"testing"

	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
)

func TestBagSaveCheckpointRejectsOversizeBeforeUsecase(t *testing.T) {
	attrs := make([]*bagv1.BagItemAttribute, 64)
	for i := range attrs {
		attrs[i] = &bagv1.BagItemAttribute{AttrId: uint32((i % 3) + 1), Value: int64(i + 1)}
	}
	items := make([]*bagv1.BagItem, 4096)
	for i := range items {
		items[i] = &bagv1.BagItem{
			ItemConfigId: 1,
			Count:        1,
			Slot:         uint32(i),
			InstanceId:   uint64(i + 1),
			Identified:   true,
			Attrs:        attrs,
		}
	}
	svc := &BagService{} // 尺寸闸必须在触达 nil usecase / owner 授权之前返回。
	resp, err := svc.SaveCheckpoint(context.Background(), &bagv1.SaveCheckpointRequest{
		PlayerId: 1,
		Snapshot: &bagv1.BagStorageRecord{Sections: []*bagv1.BagSection{{
			BagType: 0,
			Items:   items,
		}}},
	})
	if err != nil || resp.GetCode() != commonv1.ErrCode_ERR_BAG_QUOTA_EXCEEDED {
		t.Fatalf("oversize checkpoint code=%v err=%v", resp.GetCode(), err)
	}
}
