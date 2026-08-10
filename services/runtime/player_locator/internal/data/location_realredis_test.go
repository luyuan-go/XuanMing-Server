// location_realredis_test.go — 对着**真 Redis** 跑的 Lua / 原子性验证(2026-08-06)。
//
// 为什么单独一份:其余 fence 测试用 miniredis,它是 Go 实现的仿真件,
// `EVAL` 走的是自带的 Lua 解释器,`PEXPIRE ... LT`、`ZADD GT/XX`、`HEXISTS` 这类
// 带修饰符的命令语义**不保证与真 Redis 一致**。而本功能的正确性恰恰全押在这些语义上:
// 判错一次就是「把在线玩家踢出队伍」。仿真件绿灯不能当作真 Redis 绿灯。
//
// 用法(不设环境变量则整份跳过,不影响 CI 与离线开发):
//
//	PANDORA_TEST_REDIS_ADDR=127.0.0.1:6380 go test ./services/runtime/player_locator/internal/data/ -run RealRedis
//
// 本测试只用 `pandora:locator:*` 下自己造的 player id(9_900_000+),跑完即清,
// 不碰任何业务数据。
package data

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// realRedisPlayerBase 是本文件专用的 player id 段,避开任何真实业务 id。
const realRedisPlayerBase uint64 = 9_900_000

func newRealRedis(t *testing.T) (*RedisLocationRepo, *redis.Client) {
	t.Helper()
	addr := os.Getenv("PANDORA_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("未设 PANDORA_TEST_REDIS_ADDR,跳过真 Redis 验证(miniredis 覆盖不了 Lua 修饰符语义)")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("连不上真 Redis %s: %v", addr, err)
	}
	t.Cleanup(func() {
		// 只删自己造的 key,不 FLUSHDB(那会波及同机跑着的其它调试数据)。
		for i := uint64(0); i < 20; i++ {
			pid := realRedisPlayerBase + i
			client.Del(ctx, locKey(pid), hubMetaKey(pid), lastSeenKey(pid))
		}
		_ = client.Close()
	})
	return NewRedisLocationRepo(client), client
}

// 同 assignment 内必须按 admission_seq 单调定序,admission_id 防同序 ABA;
// 跨 assignment 一律接受(归属是 hub_allocator 的权威,本投影不反向定序)。
func TestRealRedis_HubPresence定序语义(t *testing.T) {
	repo, _ := newRealRedis(t)
	ctx := context.Background()
	pid := realRedisPlayerBase + 1

	// 先把当前代推进到 seq=2,才好验「更旧的 seq=1 被拒」。
	// (seq=0 属残缺 fence,在入参校验就被拒,到不了 Lua —— 那条另有用例覆盖。)
	if ok, err := repo.ActivateHubPresence(ctx, pid, connFence("assign-A", "adm-1", 1), time.Hour); err != nil || !ok {
		t.Fatalf("首次 commit: ok=%v err=%v", ok, err)
	}
	if ok, err := repo.ActivateHubPresence(ctx, pid, connFence("assign-A", "adm-2", 2), time.Hour); err != nil || !ok {
		t.Fatalf("推进到 seq=2: ok=%v err=%v", ok, err)
	}

	// 同 assignment 的旧 seq → 拒(这正是「同 Pod 秒重连后旧连接迟到写」要挡的)。
	if ok, err := repo.ValidateHubPresence(ctx, pid, connFence("assign-A", "adm-1", 1)); err != nil || ok {
		t.Fatalf("同 assignment 旧 seq 必须拒: ok=%v err=%v", ok, err)
	}
	// 同 assignment 同 seq 但不同 admission_id(ABA)→ 拒。
	if ok, err := repo.ValidateHubPresence(ctx, pid, connFence("assign-A", "adm-OTHER", 2)); err != nil || ok {
		t.Fatalf("同序号不同 admission_id 必须拒(ABA): ok=%v err=%v", ok, err)
	}
	// 同 assignment 更大 seq → 接受(秒重连落回同一 Pod 的正常路径)。
	if ok, err := repo.ValidateHubPresence(ctx, pid, connFence("assign-A", "adm-3", 3)); err != nil || !ok {
		t.Fatalf("同 assignment 新 seq 必须接受: ok=%v err=%v", ok, err)
	}
	// 残缺 fence(seq=0)必须在入参层就被拒,不得当成 legacy 放行。
	if ok, err := repo.ValidateHubPresence(ctx, pid, HubPresenceFence{AssignmentID: "assign-A"}); err == nil || ok {
		t.Fatalf("残缺 fence 必须拒且不得冒充 legacy: ok=%v err=%v", ok, err)
	}
	// 跨 assignment → 接受,且 seq 更小也接受(不反向定序)。
	if ok, err := repo.ValidateHubPresence(ctx, pid, connFence("assign-B", "adm-x", 1)); err != nil || !ok {
		t.Fatalf("跨 assignment 必须接受(归属由 hub_allocator 定): ok=%v err=%v", ok, err)
	}
}

// PEXPIRE ... LT 是「只缩不涨」的关键,miniredis 与真 Redis 在这里最容易分叉。
func TestRealRedis_ShrinkHubTTL只缩不涨且认exact身份(t *testing.T) {
	repo, client := newRealRedis(t)
	ctx := context.Background()
	pid := realRedisPlayerBase + 2
	fence := connFence("assign-A", "adm-1", 1)

	if err := repo.SetGuarded(ctx, pid, LocationRecord{
		State: 3, HubPod: "hub-1", HubPresenceFence: fence,
	}, time.Hour, 1, nil); err != nil {
		t.Fatalf("SetGuarded: %v", err)
	}

	// 身份不匹配 → 一点都不许动。
	if accepted, shrunk, err := repo.ShrinkHubTTL(ctx, "hub-1", pid, connFence("assign-A", "adm-OTHER", 1), 10*time.Second); err != nil || accepted || shrunk {
		t.Fatalf("旧连接身份必须被拒: accepted=%v shrunk=%v err=%v", accepted, shrunk, err)
	}
	if ttl, err := client.TTL(ctx, locKey(pid)).Result(); err != nil || ttl < 50*time.Minute {
		t.Fatalf("被拒的上报不得改动 TTL: ttl=%v err=%v", ttl, err)
	}

	// exact 身份 → 缩到 grace。
	if accepted, shrunk, err := repo.ShrinkHubTTL(ctx, "hub-1", pid, fence, 10*time.Second); err != nil || !accepted || !shrunk {
		t.Fatalf("exact 身份应缩 TTL: accepted=%v shrunk=%v err=%v", accepted, shrunk, err)
	}
	ttl, err := client.TTL(ctx, locKey(pid)).Result()
	if err != nil || ttl > 10*time.Second || ttl <= 0 {
		t.Fatalf("TTL 应被缩到 ~10s: ttl=%v err=%v", ttl, err)
	}

	// 重复上报:身份仍匹配但 TTL 已更短 → accepted=true 但 shrunk=false(只缩不涨)。
	if accepted, shrunk, err := repo.ShrinkHubTTL(ctx, "hub-1", pid, fence, time.Hour); err != nil || !accepted || shrunk {
		t.Fatalf("PEXPIRE LT 必须只缩不涨: accepted=%v shrunk=%v err=%v", accepted, shrunk, err)
	}
	if ttl2, err := client.TTL(ctx, locKey(pid)).Result(); err != nil || ttl2 > 10*time.Second {
		t.Fatalf("重复上报把 TTL 涨回去了(PEXPIRE LT 语义不符): ttl=%v err=%v", ttl2, err)
	}
}

// RecordLastSeen 必须只认当前 exact fence,且重复调用返回第一次的时刻(不后移)。
func TestRealRedis_RecordLastSeen幂等且认身份(t *testing.T) {
	repo, _ := newRealRedis(t)
	ctx := context.Background()
	pid := realRedisPlayerBase + 3
	fence := connFence("assign-A", "adm-1", 1)

	if ok, err := repo.ActivateHubPresence(ctx, pid, fence, time.Hour); err != nil || !ok {
		t.Fatalf("commit: ok=%v err=%v", ok, err)
	}

	first := time.Now().UnixMilli()
	recorded, eff, err := repo.RecordLastSeen(ctx, pid, fence, first, time.Hour)
	if err != nil || !recorded || eff != first {
		t.Fatalf("首次记录: recorded=%v eff=%d err=%v", recorded, eff, err)
	}
	// 重复(迟到重投)→ 返回第一次的时刻,不得后移(后移会让超时判定被无限推迟)。
	if _, eff2, err := repo.RecordLastSeen(ctx, pid, fence, first+60_000, time.Hour); err != nil || eff2 != first {
		t.Fatalf("重复记录必须返回首次时刻: eff=%d want=%d err=%v", eff2, first, err)
	}
	// 别的连接身份 → 拒。
	if recorded, _, err := repo.RecordLastSeen(ctx, pid, connFence("assign-A", "adm-OTHER", 1), first, time.Hour); err != nil || recorded {
		t.Fatalf("非当前 fence 不得写离开时刻: recorded=%v err=%v", recorded, err)
	}
}

// Hub 崩溃兜底:心跳推的 last_alive_ms 是唯一能留下的时间线索;
// 同时心跳绝不能给「从没走过 fenced 路径」的玩家凭空建 meta(会造出毒 key)。
func TestRealRedis_心跳兜底与不凭空建Meta(t *testing.T) {
	repo, client := newRealRedis(t)
	ctx := context.Background()

	// ① 有 meta 的玩家:心跳推 last_alive,BatchGetLastSeen 能兜底回答。
	withMeta := realRedisPlayerBase + 4
	fence := connFence("assign-A", "adm-1", 1)
	if ok, err := repo.ActivateHubPresence(ctx, withMeta, fence, time.Hour); err != nil || !ok {
		t.Fatalf("commit: ok=%v err=%v", ok, err)
	}
	if err := repo.SetGuarded(ctx, withMeta, LocationRecord{
		State: 3, HubPod: "hub-1", HubPresenceFence: fence,
	}, time.Hour, 1, nil); err != nil {
		t.Fatalf("SetGuarded: %v", err)
	}
	before := time.Now().UnixMilli()
	if n, err := repo.RefreshHubLocations(ctx, "hub-1", []uint64{withMeta}, time.Hour, time.Hour); err != nil || n != 1 {
		t.Fatalf("RefreshHubLocations: n=%d err=%v", n, err)
	}
	got, err := repo.BatchGetLastSeen(ctx, []uint64{withMeta})
	if err != nil {
		t.Fatal(err)
	}
	if ms, ok := got[withMeta]; !ok || ms < before {
		t.Fatalf("心跳过后必须能回答最后一次在线时刻(Hub 崩溃时唯一线索): ms=%d ok=%v", ms, ok)
	}

	// ② 没有 meta 的玩家:心跳不得建 key,且之后仍能正常 fenced 上线。
	noMeta := realRedisPlayerBase + 5
	if err := repo.SetGuarded(ctx, noMeta, LocationRecord{State: 3, HubPod: "hub-1"}, time.Hour, 1, nil); err != nil {
		t.Fatalf("SetGuarded: %v", err)
	}
	if n, err := repo.RefreshHubLocations(ctx, "hub-1", []uint64{noMeta}, time.Hour, time.Hour); err != nil || n != 1 {
		t.Fatalf("RefreshHubLocations: n=%d err=%v", n, err)
	}
	if exists, err := client.Exists(ctx, hubMetaKey(noMeta)).Result(); err != nil || exists != 0 {
		t.Fatalf("心跳不得为无 meta 玩家凭空建 key(会成为无法接受 HUB 写的毒 key): exists=%d err=%v", exists, err)
	}
	if ok, err := repo.ActivateHubPresence(ctx, noMeta, connFence("assign-Z", "adm-1", 1), time.Hour); err != nil || !ok {
		t.Fatalf("无 meta 玩家应能正常 fenced 上线: ok=%v err=%v", ok, err)
	}

	// ③ 显式离开时刻必须压过心跳时刻(更精确的来源优先)。
	leftAt := time.Now().Add(time.Second).UnixMilli()
	if _, _, err := repo.RecordLastSeen(ctx, withMeta, fence, leftAt, time.Hour); err != nil {
		t.Fatalf("RecordLastSeen: %v", err)
	}
	if got, err := repo.BatchGetLastSeen(ctx, []uint64{withMeta}); err != nil {
		t.Fatal(err)
	} else if got[withMeta] != leftAt {
		t.Fatalf("left_at_ms 必须优先于 last_alive_ms: got=%d want=%d", got[withMeta], leftAt)
	}
}
