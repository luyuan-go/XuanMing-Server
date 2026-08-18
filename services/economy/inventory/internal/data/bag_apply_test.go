// bag_apply_test.go — 背包域 journal op 应用逻辑纯单测(无 DB;bag-domain.md §3/§5.1)。
package data

import (
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
)

// fakeSectionStore 事务内工作副本假实现(镜像 AppendJournal 的 loadSection 语义)。
type fakeSectionStore struct {
	sections map[uint32]*bagv1.BagSection
	dirty    map[uint32]bool
}

func newFakeSectionStore() *fakeSectionStore {
	return &fakeSectionStore{sections: map[uint32]*bagv1.BagSection{}, dirty: map[uint32]bool{}}
}

func (f *fakeSectionStore) load(bagType uint32) (*bagv1.BagSection, error) {
	if sec, ok := f.sections[bagType]; ok {
		return sec, nil
	}
	sec := &bagv1.BagSection{BagType: bagType}
	f.sections[bagType] = sec
	return sec, nil
}

func capacityOf(m map[uint32]uint32) BagSectionCapacity {
	return func(bagType uint32) uint32 { return m[bagType] }
}

// stackLimit 全部 config 统一堆叠上限(0 = 未配置,fail-closed)。
func stackLimit(n uint32) BagMaxStack {
	return func(uint32) uint32 { return n }
}

func stackItem(config, count uint32) *bagv1.BagItem {
	return &bagv1.BagItem{ItemConfigId: config, Count: count}
}

func instanceItem(config uint32, instance uint64) *bagv1.BagItem {
	return &bagv1.BagItem{ItemConfigId: config, Count: 1, InstanceId: instance}
}

func wantCode(t *testing.T, err error, code errcode.Code, what string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: 期望错误码 %d,实际成功", what, code)
	}
	if got := errcode.As(err); got != code {
		t.Fatalf("%s: 期望错误码 %d,实际 %d(%v)", what, code, got, err)
	}
}

// TestBagApplyPickupCarryOnly 随身组目标只记 journal 不改 bag_section。
func TestBagApplyPickupCarryOnly(t *testing.T) {
	store := newFakeSectionStore()
	entry := &bagv1.BagJournalEntry{
		JournalSeq: 1, BagType: 0,
		Op: &bagv1.BagJournalEntry_PickupGrant{PickupGrant: &bagv1.PickupGrantOp{
			Items: []*bagv1.BagItem{stackItem(10001, 3)},
		}},
	}
	opType, err := applyBagOpTx(entry, store.load, store.dirty, capacityOf(nil), stackLimit(99))
	if err != nil || opType != BagOpPickupGrant {
		t.Fatalf("随身组拾取应只记 journal: op=%d err=%v", opType, err)
	}
	if len(store.dirty) != 0 {
		t.Fatalf("随身组拾取不得标脏 bag_section: %v", store.dirty)
	}
}

// TestBagApplyMailClaimWarehouse 邮件领取入仓库:真实入段 + 容量/claim_key 校验。
func TestBagApplyMailClaimWarehouse(t *testing.T) {
	store := newFakeSectionStore()
	caps := capacityOf(map[uint32]uint32{BagWarehouseType: 2})
	claim := func(seq uint64, key string, items ...*bagv1.BagItem) *bagv1.BagJournalEntry {
		return &bagv1.BagJournalEntry{
			JournalSeq: seq, BagType: BagWarehouseType,
			Op: &bagv1.BagJournalEntry_MailClaim{MailClaim: &bagv1.MailClaimOp{
				MailId: 9, ClaimKey: key, Items: items,
			}},
		}
	}

	if _, err := applyBagOpTx(claim(1, "", stackItem(10001, 1)), store.load, store.dirty, caps, stackLimit(99)); err == nil {
		t.Fatal("缺 claim_key 应拒")
	}
	if _, err := applyBagOpTx(claim(2, "k1", stackItem(10001, 5), instanceItem(10003, 77)), store.load, store.dirty, caps, stackLimit(99)); err != nil {
		t.Fatalf("领取入仓库失败: %v", err)
	}
	sec := store.sections[BagWarehouseType]
	if len(sec.GetItems()) != 2 || !store.dirty[BagWarehouseType] {
		t.Fatalf("仓库应有 2 格并标脏: items=%d dirty=%v", len(sec.GetItems()), store.dirty)
	}
	// 同 config 堆叠合并不占新格;新 config 超容量拒。
	if _, err := applyBagOpTx(claim(3, "k2", stackItem(10001, 2)), store.load, store.dirty, caps, stackLimit(99)); err != nil {
		t.Fatalf("堆叠合并不应受容量限制: %v", err)
	}
	if got := sec.GetItems()[0].GetCount(); got != 7 {
		t.Fatalf("堆叠合并后数量应为 7,实际 %d", got)
	}
	wantCode(t, func() error {
		_, err := applyBagOpTx(claim(4, "k3", stackItem(10002, 1)), store.load, store.dirty, caps, stackLimit(99))
		return err
	}(), errcode.ErrBagCapacityFull, "满仓新格")
	// 重复实例拒。
	wantCode(t, func() error {
		_, err := applyBagOpTx(claim(5, "k4", instanceItem(10003, 77)), store.load, store.dirty, caps, stackLimit(99))
		return err
	}(), errcode.ErrInvalidArg, "重复实例")
}

// TestSectionAddItemsMaxStackSplit 固化服务端堆叠建模(2026-07-22 拍板,bag-domain.md §5.2):
// 后端驻留段按 MaxStack 拆堆存放,容量按拆堆后的格子数校验;与 UE 侧 FMyBag 语义同构,
// 客户端对后端段只读渲染。取代旧"同 config 无限合并单格"行为。
func TestSectionAddItemsMaxStackSplit(t *testing.T) {
	t.Run("超上限拆多格", func(t *testing.T) {
		sec := &bagv1.BagSection{BagType: BagWarehouseType}
		if err := sectionAddItems(sec, []*bagv1.BagItem{stackItem(10001, 12)}, 3, stackLimit(5)); err != nil {
			t.Fatalf("12 个上限 5 应拆 3 格: %v", err)
		}
		if len(sec.GetItems()) != 3 {
			t.Fatalf("应占 3 格,实际 %d", len(sec.GetItems()))
		}
		var counts []uint32
		for _, item := range sec.GetItems() {
			counts = append(counts, item.GetCount())
		}
		if counts[0] != 5 || counts[1] != 5 || counts[2] != 2 {
			t.Fatalf("拆堆应为 [5 5 2],实际 %v", counts)
		}
	})

	t.Run("先填既有零头再开新格", func(t *testing.T) {
		sec := &bagv1.BagSection{BagType: BagWarehouseType,
			Items: []*bagv1.BagItem{{ItemConfigId: 10001, Count: 3, Slot: 0}}}
		if err := sectionAddItems(sec, []*bagv1.BagItem{stackItem(10001, 4)}, 2, stackLimit(5)); err != nil {
			t.Fatalf("填 2 + 新开 1 格应成功: %v", err)
		}
		if len(sec.GetItems()) != 2 || sec.GetItems()[0].GetCount() != 5 || sec.GetItems()[1].GetCount() != 2 {
			t.Fatalf("应为既有堆填满 5 + 新格 2: %+v", sec.GetItems())
		}
	})

	t.Run("容量不足在写入前整体拒", func(t *testing.T) {
		sec := &bagv1.BagSection{BagType: BagWarehouseType,
			Items: []*bagv1.BagItem{{ItemConfigId: 10001, Count: 3, Slot: 0}}}
		err := sectionAddItems(sec, []*bagv1.BagItem{stackItem(10001, 10)}, 1, stackLimit(5))
		wantCode(t, err, errcode.ErrBagCapacityFull, "溢出无格")
		// 先规划后写入:失败时既有零头堆不得被部分填充。
		if sec.GetItems()[0].GetCount() != 3 {
			t.Fatalf("容量拒后既有堆被部分填充: %+v", sec.GetItems())
		}
	})

	t.Run("堆叠上限未配置fail-closed", func(t *testing.T) {
		sec := &bagv1.BagSection{BagType: BagWarehouseType}
		err := sectionAddItems(sec, []*bagv1.BagItem{stackItem(10001, 1)}, 10, stackLimit(0))
		wantCode(t, err, errcode.ErrBagSectionNotAllowed, "上限未配置")
	})

	t.Run("超满脏数据堆跳过不参与吸纳", func(t *testing.T) {
		// 历史无限合并遗留的超上限堆:不吸纳(防 maxStack-Count 下溢)、资产原样保留。
		sec := &bagv1.BagSection{BagType: BagWarehouseType,
			Items: []*bagv1.BagItem{{ItemConfigId: 10001, Count: 150, Slot: 0}}}
		if err := sectionAddItems(sec, []*bagv1.BagItem{stackItem(10001, 10)}, 5, stackLimit(99)); err != nil {
			t.Fatalf("脏数据堆应跳过并新开格: %v", err)
		}
		if len(sec.GetItems()) != 2 || sec.GetItems()[0].GetCount() != 150 || sec.GetItems()[1].GetCount() != 10 {
			t.Fatalf("应保留 150 脏堆 + 新格 10: %+v", sec.GetItems())
		}
	})

	t.Run("巨量计数不回绕", func(t *testing.T) {
		// 旧实现 stack.Count += n 是 uint32 无检查累加;新实现 uint64 规划,巨量入账
		// 折算格子数天然超容量被拒,回绕在构造上不可能发生。
		sec := &bagv1.BagSection{BagType: BagWarehouseType,
			Items: []*bagv1.BagItem{{ItemConfigId: 10001, Count: 90, Slot: 0}}}
		err := sectionAddItems(sec, []*bagv1.BagItem{stackItem(10001, 4294967295)}, 100, stackLimit(99))
		wantCode(t, err, errcode.ErrBagCapacityFull, "uint32 极值入账")
		if sec.GetItems()[0].GetCount() != 90 {
			t.Fatalf("拒后既有堆被改动: %+v", sec.GetItems())
		}
	})
}

// TestSectionOverCapacityDrainOnly 固化超容段"只出不进"(bag-domain.md §3.2 / D5 迁移
// 超容落位共用同一语义):占格数已超容量时,扣减照常、并入既有未满堆照常(不占新格),
// 新开格一律拒;低于容量后自然恢复。
func TestSectionOverCapacityDrainOnly(t *testing.T) {
	sec := &bagv1.BagSection{BagType: BagWarehouseType, Items: []*bagv1.BagItem{
		{ItemConfigId: 10001, Count: 99, Slot: 0},
		{ItemConfigId: 10001, Count: 40, Slot: 1},
		{ItemConfigId: 10002, Count: 5, Slot: 2},
		{ItemConfigId: 10003, Count: 1, Slot: 3},
		{ItemConfigId: 10004, Count: 1, Slot: 4},
	}}
	const capacity = 3 // 5 格 > 容量 3 = 超容段(迁移落位/崩溃恢复溢出的既成状态)

	// 出:照常。
	if err := sectionRemoveItems(sec, []*bagv1.BagItem{stackItem(10004, 1)}); err != nil {
		t.Fatalf("超容段扣减应照常: %v", err)
	}
	// 并入既有未满堆:不占新格,照常。
	if err := sectionAddItems(sec, []*bagv1.BagItem{stackItem(10002, 4)}, capacity, stackLimit(99)); err != nil {
		t.Fatalf("超容段并堆(不开新格)应照常: %v", err)
	}
	// 进(需新格):拒。
	err := sectionAddItems(sec, []*bagv1.BagItem{stackItem(30001, 1)}, capacity, stackLimit(99))
	wantCode(t, err, errcode.ErrBagCapacityFull, "超容段新开格")
	// 溢出既有堆吸纳量、需要开新格的部分同样拒(整体拒,不部分应用)。
	err = sectionAddItems(sec, []*bagv1.BagItem{stackItem(10001, 100)}, capacity, stackLimit(99))
	wantCode(t, err, errcode.ErrBagCapacityFull, "超容段溢出开格")
	// 降到容量以下后恢复可进。
	if err := sectionRemoveItems(sec, []*bagv1.BagItem{stackItem(10003, 1)}); err != nil {
		t.Fatalf("腾格: %v", err)
	}
	if err := sectionRemoveItems(sec, []*bagv1.BagItem{stackItem(10001, 139)}); err != nil {
		t.Fatalf("腾格: %v", err)
	}
	if err := sectionAddItems(sec, []*bagv1.BagItem{stackItem(30001, 1)}, capacity, stackLimit(99)); err != nil {
		t.Fatalf("低于容量后应恢复可进: %v", err)
	}
}

// TestBagApplyTransferAndConsume 转移与使用:扣/加同批应用,非法整批拒。
//
// 假仓没有事务回滚(真实 AppendJournal 里失败 = 整个 MySQL 事务回滚),
// 因此每个"应失败"场景各用独立 store,不做失败后的状态断言。
func TestBagApplyTransferAndConsume(t *testing.T) {
	const activityType = BagActivityTypeBase
	caps := capacityOf(map[uint32]uint32{BagWarehouseType: 10, activityType: 10})

	// seedWarehouse 建独立 store 并铺底仓库 5 个 10001。
	seedWarehouse := func(t *testing.T) *fakeSectionStore {
		t.Helper()
		store := newFakeSectionStore()
		seed := &bagv1.BagJournalEntry{
			JournalSeq: 1, BagType: BagWarehouseType,
			Op: &bagv1.BagJournalEntry_MailClaim{MailClaim: &bagv1.MailClaimOp{
				MailId: 1, ClaimKey: "seed", Items: []*bagv1.BagItem{stackItem(10001, 5)},
			}},
		}
		if _, err := applyBagOpTx(seed, store.load, store.dirty, caps, stackLimit(99)); err != nil {
			t.Fatalf("铺底失败: %v", err)
		}
		return store
	}

	t.Run("仓库到随身转移", func(t *testing.T) {
		store := seedWarehouse(t)
		transfer := &bagv1.BagJournalEntry{
			JournalSeq: 2, BagType: BagWarehouseType,
			Op: &bagv1.BagJournalEntry_Transfer{Transfer: &bagv1.TransferOp{
				ToBagType: 0, Items: []*bagv1.BagItem{stackItem(10001, 3)},
			}},
		}
		if _, err := applyBagOpTx(transfer, store.load, store.dirty, caps, stackLimit(99)); err != nil {
			t.Fatalf("仓库→随身转移失败: %v", err)
		}
		if got := store.sections[BagWarehouseType].GetItems()[0].GetCount(); got != 2 {
			t.Fatalf("转移后仓库应剩 2,实际 %d", got)
		}
	})

	t.Run("数量不足整批拒", func(t *testing.T) {
		store := seedWarehouse(t)
		insufficient := &bagv1.BagJournalEntry{
			JournalSeq: 2, BagType: BagWarehouseType,
			Op: &bagv1.BagJournalEntry_Transfer{Transfer: &bagv1.TransferOp{
				ToBagType: 0, Items: []*bagv1.BagItem{stackItem(10001, 99)},
			}},
		}
		_, err := applyBagOpTx(insufficient, store.load, store.dirty, caps, stackLimit(99))
		wantCode(t, err, errcode.ErrBagItemNotFound, "数量不足转移")
	})

	t.Run("活动段目标代际不符拒", func(t *testing.T) {
		store := seedWarehouse(t)
		wrongGen := &bagv1.BagJournalEntry{
			JournalSeq: 2, BagType: BagWarehouseType,
			Op: &bagv1.BagJournalEntry_Transfer{Transfer: &bagv1.TransferOp{
				ToBagType: activityType, ToGeneration: 7, Items: []*bagv1.BagItem{stackItem(10001, 1)},
			}},
		}
		_, err := applyBagOpTx(wrongGen, store.load, store.dirty, caps, stackLimit(99))
		wantCode(t, err, errcode.ErrBagGenerationMismatch, "目标代际不符")
	})

	t.Run("随身组consume只记journal不改段", func(t *testing.T) {
		// 2026-08-17 后端先行拍板:副本丢弃的扣减流水打随身段,存储侧只记 journal
		// 供崩溃恢复尾重放,段内容由 owner DS 经 checkpoint 覆盖(与随身拾取同构)。
		store := newFakeSectionStore()
		carryConsume := &bagv1.BagJournalEntry{
			JournalSeq: 1, BagType: 0,
			Op: &bagv1.BagJournalEntry_Consume{Consume: &bagv1.ConsumeOp{
				ConsumeItems: []*bagv1.BagItem{stackItem(10001, 1)},
			}},
		}
		opType, err := applyBagOpTx(carryConsume, store.load, store.dirty, capacityOf(nil), stackLimit(99))
		if err != nil || opType != BagOpConsume {
			t.Fatalf("随身组 consume 应只记 journal: op=%d err=%v", opType, err)
		}
		if len(store.dirty) != 0 {
			t.Fatalf("随身组 consume 不得标脏 bag_section: %v", store.dirty)
		}
	})

	t.Run("随身组consume带产出拒", func(t *testing.T) {
		// 随身组 consume 只承载丢弃扣减;带产出会绕开审计开出跨段发放通道,fail-closed。
		store := newFakeSectionStore()
		carryProduce := &bagv1.BagJournalEntry{
			JournalSeq: 1, BagType: 0,
			Op: &bagv1.BagJournalEntry_Consume{Consume: &bagv1.ConsumeOp{
				ConsumeItems:   []*bagv1.BagItem{stackItem(10001, 1)},
				ProduceBagType: BagWarehouseType, ProduceItems: []*bagv1.BagItem{stackItem(20001, 1)},
			}},
		}
		_, err := applyBagOpTx(carryProduce, store.load, store.dirty, capacityOf(nil), stackLimit(99))
		wantCode(t, err, errcode.ErrBagSectionNotAllowed, "随身组 consume 带产出")
	})

	t.Run("开箱扣产同批", func(t *testing.T) {
		store := seedWarehouse(t)
		openBox := &bagv1.BagJournalEntry{
			JournalSeq: 2, BagType: BagWarehouseType,
			Op: &bagv1.BagJournalEntry_Consume{Consume: &bagv1.ConsumeOp{
				ConsumeItems:   []*bagv1.BagItem{stackItem(10001, 1)},
				ProduceBagType: activityType, ProduceItems: []*bagv1.BagItem{stackItem(20001, 2)},
			}},
		}
		if _, err := applyBagOpTx(openBox, store.load, store.dirty, caps, stackLimit(99)); err != nil {
			t.Fatalf("开箱扣+产失败: %v", err)
		}
		if got := store.sections[BagWarehouseType].GetItems()[0].GetCount(); got != 4 {
			t.Fatalf("开箱后仓库应剩 4,实际 %d", got)
		}
		if got := store.sections[activityType].GetItems()[0].GetCount(); got != 2 {
			t.Fatalf("活动段产出应为 2,实际 %d", got)
		}
	})

	t.Run("容量未配置段fail-closed", func(t *testing.T) {
		store := seedWarehouse(t)
		unconfigured := &bagv1.BagJournalEntry{
			JournalSeq: 2, BagType: BagWarehouseType,
			Op: &bagv1.BagJournalEntry_Transfer{Transfer: &bagv1.TransferOp{
				ToBagType: BagActivityTypeBase + 1, Items: []*bagv1.BagItem{stackItem(10001, 1)},
			}},
		}
		_, err := applyBagOpTx(unconfigured, store.load, store.dirty, caps, stackLimit(99))
		wantCode(t, err, errcode.ErrBagSectionNotAllowed, "容量未配置段")
	})
}
