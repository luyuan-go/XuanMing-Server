// local_instance_identity_test.go — legacy 本地面战斗 DS 实例身份回填回归(2026-08-04)。
//
// 背景:mode=local 的分配器只回 (pod, addr, track),战斗记录的 gameserver_uid /
// instance_epoch 恒为零值,matchmaker 据此判「ds_allocator 未回填完整 DS 目标」拒签 v2
// 战斗票,PVE 对局直接判 FAILED —— 玩家永远进不去副本。修复 = AllocateBattle 在 legacy
// 面经 localInstanceIdentitySource 回填拉起进程时生成(且已签进 DS 回调令牌)的同一组身份。
// 本文件同时钉死线上隔离:Model B 与不实现该接口的分配器永不走本地回填路径。
package biz

import (
	"testing"
)

// localIdentityAllocator 在 Mock 分配器上叠加 localInstanceIdentitySource,
// 模拟 data.LocalGameServerAllocator 的形状(拓扑用 mock,身份用注入值)。
type localIdentityAllocator struct {
	*MockGameServerAllocator
	uid   string
	epoch uint32
}

func (a *localIdentityAllocator) LocalInstanceIdentity(podName string) (string, uint32, bool) {
	if podName == "" || a.uid == "" || a.epoch == 0 {
		return "", 0, false
	}
	return a.uid, a.epoch, true
}

// legacy 本地面:分配出的战斗记录必须带 exact 实例身份(修复前恒为零值)。
func TestAllocateBattle_LocalIdentityBackfilled(t *testing.T) {
	cfg := testCfg()
	alloc := &localIdentityAllocator{
		MockGameServerAllocator: NewMockGameServerAllocator(cfg),
		uid:                     "uid-local-battle-1",
		epoch:                   1,
	}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)

	res := allocateReady(t, uc, repo, 90001, []uint64{1001}, 7, "pve_coop")
	if res.GameserverUID != alloc.uid || res.InstanceEpoch != alloc.epoch {
		t.Fatalf("legacy 本地面必须回填 exact 实例身份, got uid=%q epoch=%d",
			res.GameserverUID, res.InstanceEpoch)
	}
	if res.AllocationID == "" || res.DSPodName == "" || res.DSAddr == "" {
		t.Fatalf("分配结果其余字段不得受影响: %+v", res)
	}
}

// 身份不可得(pod 已回收 / 未拉起)时必须保持旧行为留零值,绝不糊半截身份。
func TestAllocateBattle_LocalIdentityUnavailableStaysZero(t *testing.T) {
	cfg := testCfg()
	alloc := &localIdentityAllocator{
		MockGameServerAllocator: NewMockGameServerAllocator(cfg),
		uid:                     "", // 台账查无 → LocalInstanceIdentity 返回 false
		epoch:                   0,
	}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)

	res := allocateReady(t, uc, repo, 90002, []uint64{1002}, 7, "pve_coop")
	if res.GameserverUID != "" || res.InstanceEpoch != 0 {
		t.Fatalf("身份不可得时必须留零值, got uid=%q epoch=%d", res.GameserverUID, res.InstanceEpoch)
	}
}

// 不实现该接口的分配器(Mock,与 Agones 同样不实现)保持旧行为:零身份。
func TestAllocateBattle_NonLocalAllocatorKeepsZeroIdentity(t *testing.T) {
	uc, repo := newUsecase(t)

	res := allocateReady(t, uc, repo, 90003, []uint64{1003}, 7, "pve_coop")
	if res.GameserverUID != "" || res.InstanceEpoch != 0 {
		t.Fatalf("非本地分配器必须保持零身份旧行为, got uid=%q epoch=%d",
			res.GameserverUID, res.InstanceEpoch)
	}
}
