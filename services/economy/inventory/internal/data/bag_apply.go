// bag_apply.go — 背包域 journal op 应用逻辑(bag-domain.md §3/§5.1)。
//
// 纯段内存变换(不触 SQL,可独立单测):对后端驻留段(仓库/活动段)执行加/扣/转移;
// 随身组(DS 驻留)侧只记 journal 不改 bag_section(内存态由 owner DS 按 ACK 应用,
// 存储侧本体经 checkpoint 覆盖)。调用方保证:同段读改写走事务内工作副本,提交统一落库。
package data

import (
	"github.com/luyuancpp/pandora/pkg/errcode"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
)

// bagSectionLoader 事务内段工作副本加载回调(AppendJournal 注入;含代际归一)。
type bagSectionLoader func(bagType uint32) (*bagv1.BagSection, error)

// applyBagOpTx 应用一条 journal op:改动涉及的后端驻留段并标脏。
// 返回 op_type(落 bag_journal.op_type 列)。任何校验失败整批拒(调用方回滚)。
func applyBagOpTx(entry *bagv1.BagJournalEntry, load bagSectionLoader, dirty map[uint32]bool, capacity BagSectionCapacity, maxStack BagMaxStack) (int8, error) {
	switch op := entry.GetOp().(type) {
	case *bagv1.BagJournalEntry_PickupGrant:
		if err := grantIntoBagType(entry.GetBagType(), entry.GetGeneration(), op.PickupGrant.GetItems(), load, dirty, capacity, maxStack); err != nil {
			return 0, err
		}
		return BagOpPickupGrant, nil

	case *bagv1.BagJournalEntry_MailClaim:
		if op.MailClaim.GetClaimKey() == "" {
			return 0, errcode.New(errcode.ErrInvalidArg, "mail_claim requires claim_key")
		}
		if err := grantIntoBagType(entry.GetBagType(), entry.GetGeneration(), op.MailClaim.GetItems(), load, dirty, capacity, maxStack); err != nil {
			return 0, err
		}
		return BagOpMailClaim, nil

	case *bagv1.BagJournalEntry_Transfer:
		// 源段 = entry.bag_type(代际已在外层校验);目标段代际在此校验。
		if err := deductFromBagType(entry.GetBagType(), op.Transfer.GetItems(), load, dirty); err != nil {
			return 0, err
		}
		if err := grantIntoBagType(op.Transfer.GetToBagType(), op.Transfer.GetToGeneration(), op.Transfer.GetItems(), load, dirty, capacity, maxStack); err != nil {
			return 0, err
		}
		return BagOpTransfer, nil

	case *bagv1.BagJournalEntry_Consume:
		// 后端驻留段:使用语义(§5.1),真实扣段 + 可选产出。
		// 随身组段(0/2/3):丢弃/扣减流水(2026-08-17 后端先行拍板,推翻旧「随身消耗
		// 不进 journal」)——存储侧只记 journal 供崩溃恢复尾重放,段内容由 owner DS
		// 内存应用、经 checkpoint 覆盖(deductFromBagType 对随身组本就 no-op,与
		// pickup_grant 同构)。随身组 consume 不许携带产出(fail-closed,防经此开出
		// 未经审计的跨段发放通道)。
		if !IsBackendResidentBagType(entry.GetBagType()) && len(op.Consume.GetProduceItems()) > 0 {
			return 0, errcode.New(errcode.ErrBagSectionNotAllowed,
				"carry-section consume must not produce bag=%d", entry.GetBagType())
		}
		if err := deductFromBagType(entry.GetBagType(), op.Consume.GetConsumeItems(), load, dirty); err != nil {
			return 0, err
		}
		if len(op.Consume.GetProduceItems()) > 0 {
			if err := grantIntoBagType(op.Consume.GetProduceBagType(), op.Consume.GetProduceGeneration(), op.Consume.GetProduceItems(), load, dirty, capacity, maxStack); err != nil {
				return 0, err
			}
		}
		return BagOpConsume, nil

	default:
		// 未知 op fail-closed 整批拒(旧副本遇到新 op 不得静默 ACK 丢失,§9.21 混版纪律)。
		return 0, errcode.New(errcode.ErrInvalidArg, "unknown journal op seq=%d", entry.GetJournalSeq())
	}
}

// grantIntoBagType 把物品加进目标段:后端驻留段真实入段(容量/代际校验);
// 随身组目标只记 journal(DS 内存按 ACK 应用),此处 no-op。
func grantIntoBagType(bagType uint32, generation uint64, items []*bagv1.BagItem, load bagSectionLoader, dirty map[uint32]bool, capacity BagSectionCapacity, maxStack BagMaxStack) error {
	if err := validateBagItems(items); err != nil {
		return err
	}
	if !IsBackendResidentBagType(bagType) {
		return nil
	}
	sec, err := load(bagType)
	if err != nil {
		return err
	}
	// 活动段目标代际必须等于 current(load 已归一 sec.Generation=current;fail-closed)。
	if IsActivityBagType(bagType) && generation != sec.GetGeneration() {
		return errcode.New(errcode.ErrBagGenerationMismatch,
			"target generation mismatch bag=%d want=%d current=%d", bagType, generation, sec.GetGeneration())
	}
	sectionCap := capacity(bagType)
	if sectionCap == 0 {
		// 容量未配置 fail-closed:不允许向未登记的后端驻留段写入(配置错误不静默造格)。
		return errcode.New(errcode.ErrBagSectionNotAllowed, "capacity not configured bag=%d", bagType)
	}
	if err := sectionAddItems(sec, items, sectionCap, maxStack); err != nil {
		return err
	}
	dirty[bagType] = true
	return nil
}

// deductFromBagType 从源段扣物品:后端驻留段真实扣段;随身组源只记 journal,此处 no-op。
func deductFromBagType(bagType uint32, items []*bagv1.BagItem, load bagSectionLoader, dirty map[uint32]bool) error {
	if err := validateBagItems(items); err != nil {
		return err
	}
	if !IsBackendResidentBagType(bagType) {
		return nil
	}
	sec, err := load(bagType)
	if err != nil {
		return err
	}
	if err := sectionRemoveItems(sec, items); err != nil {
		return err
	}
	dirty[bagType] = true
	return nil
}

// validateBagItems 校验物品列表形状(config>0,count>0,实例 count 恒 1)。
func validateBagItems(items []*bagv1.BagItem) error {
	if len(items) == 0 {
		return errcode.New(errcode.ErrInvalidArg, "items required")
	}
	for _, it := range items {
		if it.GetItemConfigId() == 0 || it.GetCount() == 0 {
			return errcode.New(errcode.ErrInvalidArg, "invalid item config=%d count=%d", it.GetItemConfigId(), it.GetCount())
		}
		if it.GetInstanceId() != 0 && it.GetCount() != 1 {
			return errcode.New(errcode.ErrInvalidArg, "instance item must have count=1 instance=%d", it.GetInstanceId())
		}
	}
	return nil
}

// sectionAddItems 向段内加物品(2026-07-22 拍板,服务端建模堆叠上限,bag-domain.md §5.2):
// 可堆叠道具按 MaxStack 拆堆——先填既有未满同 config 堆,溢出按 MaxStack 整格新开;
// 实例每件独占一格。容量按条目数(格子数)校验,放不下该 item 时在写入前整体拒
// (镜像 FMyBag 先规划后写入;真实调用链失败 = MySQL 事务回滚,纯单测假仓也不脏)。
// 计数运算全程 uint64:旧"无限合并单格"的 uint32 回绕风险在此消除。
func sectionAddItems(sec *bagv1.BagSection, items []*bagv1.BagItem, capacity uint32, maxStackOf BagMaxStack) error {
	for _, it := range items {
		if it.GetInstanceId() != 0 {
			for _, exist := range sec.GetItems() {
				if exist.GetInstanceId() == it.GetInstanceId() {
					return errcode.New(errcode.ErrInvalidArg, "duplicate instance %d in bag=%d", it.GetInstanceId(), sec.GetBagType())
				}
			}
			if uint32(len(sec.GetItems()))+1 > capacity {
				return errcode.New(errcode.ErrBagCapacityFull, "bag=%d capacity=%d full", sec.GetBagType(), capacity)
			}
			sec.Items = append(sec.Items, &bagv1.BagItem{
				ItemConfigId: it.GetItemConfigId(),
				Count:        1,
				Slot:         lowestFreeBagSlot(sec),
				InstanceId:   it.GetInstanceId(),
				Identified:   it.GetIdentified(),
				Attrs:        it.GetAttrs(),
			})
			continue
		}

		// 可堆叠:堆叠上限未配置 fail-closed(配置错误不静默无限合并)。
		limit := uint64(maxStackOf(it.GetItemConfigId()))
		if limit == 0 {
			return errcode.New(errcode.ErrBagSectionNotAllowed,
				"max_stack not configured config=%d bag=%d", it.GetItemConfigId(), sec.GetBagType())
		}

		// ① 规划(不改段):既有未满同 config 堆可吸纳多少;超上限的脏数据堆跳过
		// (不参与吸纳也不做减法,防下溢;资产保留,只出不进)。
		type stackFill struct {
			idx  int
			fill uint64
		}
		var fills []stackFill
		remaining := uint64(it.GetCount())
		for idx, exist := range sec.GetItems() {
			if remaining == 0 {
				break
			}
			if exist.GetInstanceId() != 0 || exist.GetItemConfigId() != it.GetItemConfigId() {
				continue
			}
			cnt := uint64(exist.GetCount())
			if cnt >= limit {
				continue
			}
			fill := limit - cnt
			if fill > remaining {
				fill = remaining
			}
			fills = append(fills, stackFill{idx: idx, fill: fill})
			remaining -= fill
		}

		// ② 容量门:溢出部分按 MaxStack 整格折算新格数,放不下在任何写入前整体拒。
		var newGrids uint64
		if remaining > 0 {
			newGrids = (remaining + limit - 1) / limit
			if uint64(len(sec.GetItems()))+newGrids > uint64(capacity) {
				return errcode.New(errcode.ErrBagCapacityFull,
					"bag=%d capacity=%d full (need %d new slots)", sec.GetBagType(), capacity, newGrids)
			}
		}

		// ③ 应用:填堆(cnt+fill ≤ limit ≤ uint32 上限,转回 uint32 安全)+ 整格铺开。
		for _, f := range fills {
			sec.Items[f.idx].Count = uint32(uint64(sec.Items[f.idx].GetCount()) + f.fill)
		}
		for remaining > 0 {
			put := limit
			if remaining < put {
				put = remaining
			}
			sec.Items = append(sec.Items, &bagv1.BagItem{
				ItemConfigId: it.GetItemConfigId(),
				Count:        uint32(put),
				Slot:         lowestFreeBagSlot(sec),
			})
			remaining -= put
		}
	}
	return nil
}

// sectionRemoveItems 从段内扣物品:实例按 instance_id 精确移除;可堆叠按 config 扣数量,
// 扣空移格。不存在 / 数量不足 → ErrBagItemNotFound(整批拒,零部分应用)。
func sectionRemoveItems(sec *bagv1.BagSection, items []*bagv1.BagItem) error {
	for _, it := range items {
		if it.GetInstanceId() != 0 {
			removed := false
			for idx, exist := range sec.GetItems() {
				if exist.GetInstanceId() == it.GetInstanceId() && exist.GetItemConfigId() == it.GetItemConfigId() {
					sec.Items = append(sec.Items[:idx], sec.Items[idx+1:]...)
					removed = true
					break
				}
			}
			if !removed {
				return errcode.New(errcode.ErrBagItemNotFound,
					"instance %d not found in bag=%d", it.GetInstanceId(), sec.GetBagType())
			}
			continue
		}
		remaining := it.GetCount()
		for idx := 0; idx < len(sec.Items) && remaining > 0; {
			exist := sec.Items[idx]
			if exist.GetInstanceId() != 0 || exist.GetItemConfigId() != it.GetItemConfigId() {
				idx++
				continue
			}
			if exist.GetCount() > remaining {
				exist.Count -= remaining
				remaining = 0
				break
			}
			remaining -= exist.GetCount()
			sec.Items = append(sec.Items[:idx], sec.Items[idx+1:]...)
		}
		if remaining > 0 {
			return errcode.New(errcode.ErrBagItemNotFound,
				"insufficient item config=%d need=%d in bag=%d", it.GetItemConfigId(), it.GetCount(), sec.GetBagType())
		}
	}
	return nil
}

// lowestFreeBagSlot 分配段内最小空闲格位(后端驻留段格位仅展示用,随身组格位归 DS checkpoint)。
func lowestFreeBagSlot(sec *bagv1.BagSection) uint32 {
	used := map[uint32]bool{}
	for _, it := range sec.GetItems() {
		used[it.GetSlot()] = true
	}
	var slot uint32
	for used[slot] {
		slot++
	}
	return slot
}
