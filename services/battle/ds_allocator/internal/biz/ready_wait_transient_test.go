// ready_wait_transient_test.go — legacy ready 等待对 Redis 瞬时错误的容忍(2026-08-04)。
//
// 背景:waitBattleReady 要等 DS 冷启动(editor 形态可达 60s+),每 tick 读一次 Redis。
// 原先任意一次读错误就整局放弃,而 Redis 读超时抛的是**原始错误**(非 errcode),上层判
// ErrUnknown(code=1)→ matchmaker ds_allocate_failed → 回滚 owner Begin → 玩家被弹回大厅。
// 实测:AllocateBattle 59.6s code=1,紧接着的重试 allocate_idempotent_hit 就成功了,
// 证明 DS 一直是好的,只是中间一次 Redis 读抖了一下。
package biz

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"

	"github.com/luyuancpp/pandora/pkg/config"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// flakyGetBattleRepo 让前 n 次 GetBattle 返回传输层错误,之后透传真实仓储。
type flakyGetBattleRepo struct {
	data.BattleRepo
	remaining atomic.Int32
	injected  atomic.Int32
}

func (r *flakyGetBattleRepo) GetBattle(ctx context.Context, matchID uint64) (*dsv1.BattleStorageRecord, bool, error) {
	if r.remaining.Load() > 0 {
		r.remaining.Add(-1)
		r.injected.Add(1)
		// 模拟 go-redis 的读超时:原始 error,不带 errcode。
		return nil, false, errors.New("redis: i/o timeout")
	}
	return r.BattleRepo.GetBattle(ctx, matchID)
}

// 瞬时 Redis 读错误必须容忍到下个 tick,不得让整局分配失败。
func TestWaitBattleReady_ToleratesTransientReadError(t *testing.T) {
	const matchID uint64 = 95001
	cfg := testCfg()
	alloc := &localIdentityAllocator{
		MockGameServerAllocator: NewMockGameServerAllocator(cfg),
		uid:                     "uid-flaky",
		epoch:                   1,
	}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)
	flaky := &flakyGetBattleRepo{BattleRepo: repo}
	flaky.remaining.Store(2) // 前两次读抖动
	uc.repo = flaky

	// allocateReady 内部并发跑 AllocateBattle 并喂 ready 心跳;心跳侧走 UpdateBattleWithLock,
	// 不受本注入影响,只有 ready 等待的 GetBattle 会读到错误。
	res := allocateReady(t, uc, repo, matchID, []uint64{6001}, 7, "pve_coop")
	if res == nil || res.DSAddr == "" {
		t.Fatalf("瞬时读错误不应让分配失败, got %+v", res)
	}
	if flaky.injected.Load() == 0 {
		t.Fatal("测试未真正注入错误,断言无意义")
	}
}

// 容忍不得变成无限等待:读**持续**失败时,必须在 ready_wait deadline 到点后失败。
func TestWaitBattleReady_PersistentReadErrorStillTimesOut(t *testing.T) {
	const matchID uint64 = 95002
	cfg := testCfg()
	cfg.ReadyWaitTimeout = config.Duration(300 * time.Millisecond)
	alloc := &localIdentityAllocator{
		MockGameServerAllocator: NewMockGameServerAllocator(cfg),
		uid:                     "uid-flaky-2",
		epoch:                   1,
	}
	uc, repo, _ := newUsecaseWithAlloc(t, alloc)
	uc.cfg = cfg
	flaky := &flakyGetBattleRepo{BattleRepo: repo}
	flaky.remaining.Store(1 << 30) // 永远失败
	uc.repo = flaky

	start := time.Now()
	_, err := uc.AllocateBattle(context.Background(), matchID, []uint64{6002}, 7, "pve_coop")
	if err == nil {
		t.Fatal("持续读失败必须最终失败,不得无限等待")
	}
	if elapsed := time.Since(start); elapsed > 30*time.Second {
		t.Fatalf("失败耗时 %v,deadline 兜底可能失效", elapsed)
	}
}

// flakyAuthRepo 让前 n 次 ReadAuthority 返回传输层错误,之后透传真实授权仓。
type flakyAuthRepo struct {
	data.BattleAuthRepo
	remaining atomic.Int32
	injected  atomic.Int32
}

func (r *flakyAuthRepo) ReadAuthority(ctx context.Context, matchID uint64) (data.BattleAuthoritySnapshot, error) {
	if r.remaining.Load() > 0 {
		r.remaining.Add(-1)
		r.injected.Add(1)
		return data.BattleAuthoritySnapshot{}, errors.New("redis: i/o timeout")
	}
	return r.BattleAuthRepo.ReadAuthority(ctx, matchID)
}

// Model B(生产路径)同样必须容忍瞬时 Redis 读错误,不得让整局分配失败。
func TestWaitBattleReady_ModelBToleratesTransientReadError(t *testing.T) {
	const (
		matchID      = uint64(95003)
		allocationID = "6717e1e9-e1b5-4841-81fc-5be66f55b3cc"
		podName      = "battle-95003"
		instanceUID  = "uid-95003"
	)
	uc, _, _ := modelBTerminalFixture(t, matchID, allocationID, podName, instanceUID, []uint64{6003})
	flaky := &flakyAuthRepo{BattleAuthRepo: uc.authRepo}
	flaky.remaining.Store(2)
	uc.authRepo = flaky

	// 对局已 active(夹具已 ActivateHeartbeat),ready 等待应在容忍两次读抖动后成功返回。
	res, err := uc.waitBattleReady(context.Background(), matchID, podName, allocationID)
	if err != nil {
		t.Fatalf("瞬时读错误不应让 ready 等待失败: %v", err)
	}
	if res == nil || res.DSAddr == "" {
		t.Fatalf("应返回可用目标, got %+v", res)
	}
	if flaky.injected.Load() == 0 {
		t.Fatal("测试未真正注入错误,断言无意义")
	}
}

// 权威判定不得被容忍逻辑一起吞掉:battle 键被 purge 时必须**立即**失败,
// 不能拖成空转满 ready_wait(历史上 141.85s 的根因)。
func TestWaitBattleReady_ModelBAuthoritativeLossStillFailsFast(t *testing.T) {
	const (
		matchID      = uint64(95004)
		allocationID = "7717e1e9-e1b5-4841-81fc-5be66f55b3cc"
		podName      = "battle-95004"
		instanceUID  = "uid-95004"
	)
	uc, _, _ := modelBTerminalFixture(t, matchID, allocationID, podName, instanceUID, []uint64{6004})
	// 直接抹掉 battle 记录:这是权威判定(本分配已不可能 ready),不是基础设施抖动。
	if err := uc.repo.DeleteBattle(context.Background(), matchID); err != nil {
		t.Fatal(err)
	}
	uc.cfg.ReadyWaitTimeout = config.Duration(30 * time.Second) // 若被误当抖动重试,会明显超时

	start := time.Now()
	if _, err := uc.waitBattleReady(context.Background(), matchID, podName, allocationID); err == nil {
		t.Fatal("battle 记录已 purge 必须失败")
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("权威判定被误当抖动重试了(耗时 %v),必须立即失败", elapsed)
	}
}
