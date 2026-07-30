package rewardclaim

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestClaimPermanent_FirstThenIdempotent(t *testing.T) {
	r := New()

	if err := r.ClaimPermanent("sign_in", 3); err != nil {
		t.Fatalf("首次领取应成功, got %v", err)
	}
	if !r.IsPermanentClaimed("sign_in", 3) {
		t.Fatal("领取后应为已领取")
	}
	if err := r.ClaimPermanent("sign_in", 3); !errors.Is(err, ErrAlreadyClaimed) {
		t.Fatalf("重复领取应返回 ErrAlreadyClaimed, got %v", err)
	}
}

func TestPermanentSourcesIndependent(t *testing.T) {
	r := New()
	if err := r.ClaimPermanent("sign_in", 5); err != nil {
		t.Fatal(err)
	}
	// 不同来源相同 index 互不影响
	if r.IsPermanentClaimed("achievement", 5) {
		t.Fatal("不同来源不应共享 bit")
	}
	if err := r.ClaimPermanent("achievement", 5); err != nil {
		t.Fatalf("另一来源同 index 应可独立领取, got %v", err)
	}
	if r.PermanentCount("sign_in") != 1 || r.PermanentCount("achievement") != 1 {
		t.Fatal("各来源计数应独立为 1")
	}
}

func TestClaimActivity_AndIdempotent(t *testing.T) {
	r := New()
	const inst uint64 = 10001
	if err := r.ClaimActivity(inst, 0); err != nil {
		t.Fatal(err)
	}
	if err := r.ClaimActivity(inst, 0); !errors.Is(err, ErrAlreadyClaimed) {
		t.Fatalf("活动重复领取应幂等, got %v", err)
	}
	if !r.IsActivityClaimed(inst, 0) {
		t.Fatal("应为已领取")
	}
}

// 核心需求:活动下线删整条,下期新实例从零开始,即使复用相同 index 也不串味。
func TestEraseActivity_ReuseClean(t *testing.T) {
	r := New()
	const round1 uint64 = 20001
	const round2 uint64 = 20002

	if err := r.ClaimActivity(round1, 7); err != nil {
		t.Fatal(err)
	}
	if !r.HasActivity(round1) {
		t.Fatal("应存在 round1 记录")
	}

	// 活动下线:删整条
	if !r.EraseActivity(round1) {
		t.Fatal("EraseActivity 应返回 true")
	}
	if r.HasActivity(round1) {
		t.Fatal("删除后不应再存在 round1")
	}
	if r.IsActivityClaimed(round1, 7) {
		t.Fatal("删除后 round1 的 bit 应清空")
	}

	// 下期新活动:同样的 index 7 应能重新领取(不被上期污染)
	if err := r.ClaimActivity(round2, 7); err != nil {
		t.Fatalf("新实例复用 index 应可领取, got %v", err)
	}
	// round1 与 round2 各自独立
	if r.HasActivity(round1) {
		t.Fatal("round1 不应复活")
	}

	// 重复删不存在的实例返回 false
	if r.EraseActivity(round1) {
		t.Fatal("删除不存在的实例应返回 false")
	}
}

func TestActivityInstancesParallel(t *testing.T) {
	r := New()
	insts := []uint64{10001, 10002, 20001, 30001}
	for _, id := range insts {
		if err := r.ClaimActivity(id, 1); err != nil {
			t.Fatalf("inst %d 领取失败 %v", id, err)
		}
	}
	// 多活动并行,互不干扰
	for _, id := range insts {
		if r.ActivityCount(id) != 1 {
			t.Fatalf("inst %d 计数应为 1", id)
		}
	}
	// 删一个不影响其余
	r.EraseActivity(20001)
	for _, id := range insts {
		if id == 20001 {
			if r.HasActivity(id) {
				t.Fatal("20001 应已删除")
			}
			continue
		}
		if !r.HasActivity(id) {
			t.Fatalf("inst %d 不应被波及", id)
		}
	}
}

func TestIndexTooLarge(t *testing.T) {
	r := New()
	if err := r.ClaimPermanent("sign_in", MaxBitIndex); !errors.Is(err, ErrIndexTooLarge) {
		t.Fatalf("超界应返回 ErrIndexTooLarge, got %v", err)
	}
	if err := r.ClaimActivity(1, MaxBitIndex+10); !errors.Is(err, ErrIndexTooLarge) {
		t.Fatalf("超界应返回 ErrIndexTooLarge, got %v", err)
	}
	// 上界内最大合法索引应可领取
	if err := r.ClaimPermanent("sign_in", MaxBitIndex-1); err != nil {
		t.Fatalf("上界内应可领取, got %v", err)
	}
}

// 对标 C++ SetBit/TestBit(BitMap, bits, id):业务 ID 经表映射成 bit 位。
func TestClaimByID_MapsThroughBitIndex(t *testing.T) {
	r := New()
	// 读表生成的映射:配置 ID → bit 位(故意非连续,模拟真实表)
	missionBitMap := BitIndexMap{
		1001: 0,
		1002: 5,
		2050: 7,
	}

	if err := r.ClaimPermanentByID("mission", missionBitMap, 1002); err != nil {
		t.Fatalf("按 ID 领取应成功, got %v", err)
	}
	// 实际落在 bit 位 5,而不是 id 1002
	if !r.IsPermanentClaimed("mission", 5) {
		t.Fatal("ID 1002 应映射到 bit 位 5")
	}
	if r.IsPermanentClaimed("mission", 1002) {
		t.Fatal("不应直接把业务 ID 当 bit 索引")
	}
	if !r.IsPermanentClaimedByID("mission", missionBitMap, 1002) {
		t.Fatal("按 ID 查询应为已领取")
	}
	if r.IsPermanentClaimedByID("mission", missionBitMap, 1001) {
		t.Fatal("未领取的 ID 应为未领取")
	}
	if err := r.ClaimPermanentByID("mission", missionBitMap, 1002); !errors.Is(err, ErrAlreadyClaimed) {
		t.Fatalf("重复按 ID 领取应幂等, got %v", err)
	}
}

func TestClaimByID_UnknownID(t *testing.T) {
	r := New()
	bitMap := BitIndexMap{1001: 0}
	if err := r.ClaimPermanentByID("mission", bitMap, 9999); !errors.Is(err, ErrUnknownID) {
		t.Fatalf("未知 ID 应返回 ErrUnknownID, got %v", err)
	}
	if r.IsPermanentClaimedByID("mission", bitMap, 9999) {
		t.Fatal("未知 ID 查询应为未领取")
	}
}

func TestClaimActivityByID(t *testing.T) {
	r := New()
	const inst uint64 = 20001
	actBitMap := BitIndexMap{3001: 2, 3002: 9}

	if err := r.ClaimActivityByID(inst, actBitMap, 3002); err != nil {
		t.Fatalf("活动按 ID 领取应成功, got %v", err)
	}
	if !r.IsActivityClaimed(inst, 9) {
		t.Fatal("活动 ID 3002 应映射到 bit 位 9")
	}
	if !r.IsActivityClaimedByID(inst, actBitMap, 3002) {
		t.Fatal("按 ID 查询应为已领取")
	}
	if err := r.ClaimActivityByID(inst, actBitMap, 7777); !errors.Is(err, ErrUnknownID) {
		t.Fatalf("活动未知 ID 应返回 ErrUnknownID, got %v", err)
	}
}

func TestRetainActivities_GC(t *testing.T) {
	r := New()
	for _, id := range []uint64{20001, 20002, 30001, 40001} {
		if err := r.ClaimActivity(id, 1); err != nil {
			t.Fatal(err)
		}
	}
	// 仅保留两个仍有效的活动,其余应被清理
	active := map[uint64]struct{}{20002: {}, 40001: {}}
	removed := r.RetainActivities(active)
	if removed != 2 {
		t.Fatalf("应清理 2 条, got %d", removed)
	}
	if !r.HasActivity(20002) || !r.HasActivity(40001) {
		t.Fatal("有效活动应保留")
	}
	if r.HasActivity(20001) || r.HasActivity(30001) {
		t.Fatal("失效活动应被清理")
	}
	// 快照里也不应再有被清理的活动(缩小存档)
	_, act := r.Snapshot()
	if _, ok := act[20001]; ok {
		t.Fatal("清理后快照不应包含失效活动")
	}
	if len(act) != 2 {
		t.Fatalf("快照活动数应为 2, got %d", len(act))
	}
}

func TestRetainActivities_EmptyClearsAll(t *testing.T) {
	r := New()
	_ = r.ClaimActivity(1, 0)
	_ = r.ClaimActivity(2, 0)
	if removed := r.RetainActivities(map[uint64]struct{}{}); removed != 2 {
		t.Fatalf("空集合应清空全部, removed=%d", removed)
	}
	if len(r.ActivityIDs()) != 0 {
		t.Fatal("清空后不应有活动记录")
	}
}

func TestEraseActivities_Batch(t *testing.T) {
	r := New()
	for _, id := range []uint64{1, 2, 3} {
		_ = r.ClaimActivity(id, 0)
	}
	// 删 2 个存在 + 1 个不存在,只计实际删除数
	if removed := r.EraseActivities([]uint64{2, 3, 999}); removed != 2 {
		t.Fatalf("应删除 2 条, got %d", removed)
	}
	if !r.HasActivity(1) || r.HasActivity(2) || r.HasActivity(3) {
		t.Fatal("批量删除结果不符")
	}
}

func TestActivityIDs(t *testing.T) {
	r := New()
	_ = r.ClaimActivity(100, 0)
	_ = r.ClaimActivity(200, 0)
	ids := r.ActivityIDs()
	if len(ids) != 2 {
		t.Fatalf("应有 2 个活动 ID, got %d", len(ids))
	}
	seen := map[uint64]bool{}
	for _, id := range ids {
		seen[id] = true
	}
	if !seen[100] || !seen[200] {
		t.Fatal("ActivityIDs 应包含 100 与 200")
	}
}

func TestClaimedIndices(t *testing.T) {
	r := New()
	// 故意非连续、跨字节边界(0、7、8、63)
	for _, idx := range []uint32{0, 7, 8, 63} {
		if err := r.ClaimPermanent("sign_in", idx); err != nil {
			t.Fatal(err)
		}
	}
	got := r.PermanentClaimedIndices("sign_in")
	want := []uint32{0, 7, 8, 63}
	if len(got) != len(want) {
		t.Fatalf("已领取索引数应为 %d, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("索引应升序且一致, want %v got %v", want, got)
		}
	}
	// 空来源返回 nil
	if r.PermanentClaimedIndices("none") != nil {
		t.Fatal("空来源应返回 nil")
	}

	// 活动侧
	_ = r.ClaimActivity(20001, 2)
	_ = r.ClaimActivity(20001, 9)
	act := r.ActivityClaimedIndices(20001)
	if len(act) != 2 || act[0] != 2 || act[1] != 9 {
		t.Fatalf("活动已领取索引应为 [2 9], got %v", act)
	}
	if r.ActivityClaimedIndices(99999) != nil {
		t.Fatal("不存在的活动应返回 nil")
	}
}

func TestSnapshotLoadRoundTrip(t *testing.T) {
	r := New()
	_ = r.ClaimPermanent("sign_in", 0)
	_ = r.ClaimPermanent("sign_in", 9)
	_ = r.ClaimPermanent("achievement", 100)
	_ = r.ClaimActivity(20001, 3)
	_ = r.ClaimActivity(20002, 63)

	perm, act := r.Snapshot()
	// 落地后空来源不应出现
	if _, ok := perm["newbie"]; ok {
		t.Fatal("未领取的来源不应出现在快照")
	}

	r2 := Load(perm, act)

	if !r2.IsPermanentClaimed("sign_in", 0) || !r2.IsPermanentClaimed("sign_in", 9) {
		t.Fatal("永久 sign_in 往返丢失")
	}
	if !r2.IsPermanentClaimed("achievement", 100) {
		t.Fatal("永久 achievement 往返丢失")
	}
	if !r2.IsActivityClaimed(20001, 3) || !r2.IsActivityClaimed(20002, 63) {
		t.Fatal("活动往返丢失")
	}
	// 未置位的高索引不应误报
	if r2.IsPermanentClaimed("sign_in", 8) {
		t.Fatal("未领取档位误报已领取")
	}
}

func TestLoadDefensiveCopy(t *testing.T) {
	raw := []byte{0b0000_0001}
	r := Load(map[string][]byte{"sign_in": raw}, nil)
	// 改动入参底层数组不应影响已加载的 Record
	raw[0] = 0
	if !r.IsPermanentClaimed("sign_in", 0) {
		t.Fatal("Load 应做防御性拷贝,不与入参共享底层数组")
	}
}

func TestSnapshotTrimsTrailingZeros(t *testing.T) {
	// 加载一段尾部含全零字节的数据,快照后应被裁掉
	r := Load(map[string][]byte{"sign_in": {0b0000_0001, 0x00, 0x00}}, nil)
	perm, _ := r.Snapshot()
	if got := len(perm["sign_in"]); got != 1 {
		t.Fatalf("尾部全零字节应被裁掉, 期望 1 字节, got %d", got)
	}
}

// TestEntryLimits 条目数上限(§9.18 在 blob 内部的对应物,2026-07-22):
// 客户端可直调 ClaimReward 且 source/activity_instance_id 无配置表白名单,
// 每换一个值就永久新增一条位图 → record(LONGBLOB)无界增长。
// 本测试锁定:新增条目触顶必须被拒,而**已存在**的条目必须继续可领(不许回档)。
func TestEntryLimits(t *testing.T) {
	t.Run("永久来源条目数触顶拒新增", func(t *testing.T) {
		r := New()
		for i := 0; i < MaxPermanentSources; i++ {
			if err := r.ClaimPermanent(fmt.Sprintf("src_%d", i), 0); err != nil {
				t.Fatalf("第 %d 个来源应成功: %v", i, err)
			}
		}
		if err := r.ClaimPermanent("one_more", 0); !errors.Is(err, ErrTooManyEntries) {
			t.Fatalf("超出 MaxPermanentSources 应回 ErrTooManyEntries, got %v", err)
		}
		// 关键:已有来源不受影响,否则调小上限 = 老玩家领不到已有奖励(回档)。
		if err := r.ClaimPermanent("src_0", 1); err != nil {
			t.Fatalf("已存在来源必须继续可领, got %v", err)
		}
	})

	t.Run("活动实例条目数触顶拒新增", func(t *testing.T) {
		r := New()
		for i := 1; i <= MaxActivityInstances; i++ {
			if err := r.ClaimActivity(uint64(i), 0); err != nil {
				t.Fatalf("第 %d 个活动应成功: %v", i, err)
			}
		}
		if err := r.ClaimActivity(999999, 0); !errors.Is(err, ErrTooManyEntries) {
			t.Fatalf("超出 MaxActivityInstances 应回 ErrTooManyEntries, got %v", err)
		}
		if err := r.ClaimActivity(1, 5); err != nil {
			t.Fatalf("已存在活动必须继续可领, got %v", err)
		}
		// 回收一条后应能再新增(EraseActivity 是活动下线的正常回收路径)。
		if !r.EraseActivity(1) {
			t.Fatal("EraseActivity(1) 应返回 true")
		}
		if err := r.ClaimActivity(999999, 0); err != nil {
			t.Fatalf("回收一条后应可新增, got %v", err)
		}
	})

	t.Run("来源名超长拒绝", func(t *testing.T) {
		r := New()
		long := strings.Repeat("x", MaxSourceNameLen+1)
		if err := r.ClaimPermanent(long, 0); !errors.Is(err, ErrSourceNameTooLong) {
			t.Fatalf("超长来源名应回 ErrSourceNameTooLong, got %v", err)
		}
		if err := r.ClaimPermanent(strings.Repeat("x", MaxSourceNameLen), 0); err != nil {
			t.Fatalf("恰好等于上限的来源名应放行, got %v", err)
		}
	})

	t.Run("存量超限记录只冻结不报错", func(t *testing.T) {
		// Load 进来的存量记录(上限落地前写入的)可能已超限:必须能正常读写已有条目,
		// 只是不能再长——否则上线即让存量玩家领奖全挂。
		perm := map[string][]byte{}
		for i := 0; i < MaxPermanentSources+10; i++ {
			perm[fmt.Sprintf("legacy_%d", i)] = []byte{0x01}
		}
		r := Load(perm, nil)
		if err := r.ClaimPermanent("legacy_0", 1); err != nil {
			t.Fatalf("存量超限记录的已有来源必须可继续领取, got %v", err)
		}
		if err := r.ClaimPermanent("brand_new", 0); !errors.Is(err, ErrTooManyEntries) {
			t.Fatalf("存量超限记录不得再新增来源, got %v", err)
		}
	})
}
