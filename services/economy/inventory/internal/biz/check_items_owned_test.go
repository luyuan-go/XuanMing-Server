package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/data"
)

// newOwnedTestUsecase 构造启用实例背包(Capacity>0)的 usecase,并把 repo 一并返回给用例铺数据。
func newOwnedTestUsecase() (*InventoryUsecase, *fakeRepo) {
	repo := newFakeRepo()
	return NewInventoryUsecase(repo, conf.InventoryConf{Capacity: 8}), repo
}

// TestCheckItemsOwnedStackAndInstance 覆盖两条持有路径:堆叠计数 > 0 与装备实例存在。
func TestCheckItemsOwnedStackAndInstance(t *testing.T) {
	uc, repo := newOwnedTestUsecase()
	const pid uint64 = 100

	repo.items[pid] = map[uint32]int64{
		2001: 3, // 有堆叠 → 持有
		2002: 0, // 计数为 0 → 不算持有(行可能因 uk 被保留)
	}
	repo.instances[pid] = map[uint64]*data.ItemInstance{
		9001: {InstanceID: 9001, ItemConfigID: 5001},
	}

	owned, err := uc.CheckItemsOwned(context.Background(), pid, []uint32{2001, 2002, 5001, 5002})
	if err != nil {
		t.Fatalf("CheckItemsOwned: %v", err)
	}
	// 返回值定序(升序),用例可直接比对。
	want := []uint32{2001, 5001}
	if len(owned) != len(want) {
		t.Fatalf("持有子集不符: got=%v want=%v", owned, want)
	}
	for i := range want {
		if owned[i] != want[i] {
			t.Fatalf("持有子集不符: got=%v want=%v", owned, want)
		}
	}
}

// TestCheckItemsOwnedInstanceIgnoredWhenCapacityDisabled 未启用实例背包时不读实例表,
// 只有堆叠计数能证明持有(与 GetInventoryFull 同一条件,避免旧库无该表时整表报错)。
func TestCheckItemsOwnedInstanceIgnoredWhenCapacityDisabled(t *testing.T) {
	repo := newFakeRepo()
	uc := NewInventoryUsecase(repo, conf.InventoryConf{Capacity: 0})
	const pid uint64 = 100
	repo.instances[pid] = map[uint64]*data.ItemInstance{
		9001: {InstanceID: 9001, ItemConfigID: 5001},
	}

	owned, err := uc.CheckItemsOwned(context.Background(), pid, []uint32{5001})
	if err != nil {
		t.Fatalf("CheckItemsOwned: %v", err)
	}
	if len(owned) != 0 {
		t.Fatalf("未启用实例背包时不应从实例判定持有: got=%v", owned)
	}
}

// TestCheckItemsOwnedNotOwnedIsEmpty 完全不持有时返回空集而不是错误——
// 「查询成功且一件都没有」与「查询失败」必须可区分,否则调用方无法 fail-closed。
func TestCheckItemsOwnedNotOwnedIsEmpty(t *testing.T) {
	uc, _ := newOwnedTestUsecase()
	owned, err := uc.CheckItemsOwned(context.Background(), 100, []uint32{7001})
	if err != nil {
		t.Fatalf("CheckItemsOwned: %v", err)
	}
	if len(owned) != 0 {
		t.Fatalf("不持有应返回空集: got=%v", owned)
	}
}

// TestCheckItemsOwnedArgGuards 参数边界:player_id / 空集 / 0 值 id / 超上限一律拒,
// 超上限尤其不能静默截断(截断会把"未查"伪装成"未持有")。
func TestCheckItemsOwnedArgGuards(t *testing.T) {
	uc, _ := newOwnedTestUsecase()
	ctx := context.Background()

	cases := []struct {
		name string
		pid  uint64
		ids  []uint32
	}{
		{"player_id 为 0", 0, []uint32{2001}},
		{"空集合", 100, nil},
		{"含 0 值 item_config_id", 100, []uint32{2001, 0}},
		{"超过上限", 100, make([]uint32, MaxCheckItemsOwned+1)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := uc.CheckItemsOwned(ctx, c.pid, c.ids); err == nil {
				t.Fatal("应返回参数错误")
			} else if errcode.As(err) != errcode.ErrInvalidArg {
				t.Fatalf("应为 ErrInvalidArg,实为 %v", err)
			}
		})
	}
}

// TestCheckItemsOwnedBoundaryExactLimit 恰好等于上限必须放行(边界值不能一并拒掉)。
func TestCheckItemsOwnedBoundaryExactLimit(t *testing.T) {
	uc, repo := newOwnedTestUsecase()
	const pid uint64 = 100
	ids := make([]uint32, 0, MaxCheckItemsOwned)
	repo.items[pid] = map[uint32]int64{}
	for i := 0; i < MaxCheckItemsOwned; i++ {
		id := uint32(2000 + i)
		ids = append(ids, id)
		repo.items[pid][id] = 1
	}
	owned, err := uc.CheckItemsOwned(context.Background(), pid, ids)
	if err != nil {
		t.Fatalf("恰好等于上限应放行: %v", err)
	}
	if len(owned) != MaxCheckItemsOwned {
		t.Fatalf("应全部持有: got=%d want=%d", len(owned), MaxCheckItemsOwned)
	}
}
