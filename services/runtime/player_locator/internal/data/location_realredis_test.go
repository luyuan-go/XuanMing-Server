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

// TouchAlive 的节流分支:BATTLE 心跳每 5s 每人一次,不节流会让 Redis 写量按 6 倍放大。
// **节流掉的只能是「写时间戳」,TTL 必须每次都续** —— 漏续会让 meta 先于在线状态过期,
// 反而丢掉判定依据。这条只能在真 Redis 上验(miniredis 的 PTTL / Lua 语义不保证一致)。
func TestRealRedis_TouchAlive节流但仍续TTL(t *testing.T) {
	repo, client := newRealRedis(t)
	ctx := context.Background()
	pid := realRedisPlayerBase + 6
	fence := connFence("assign-A", "adm-1", 1)
	if ok, err := repo.ActivateHubPresence(ctx, pid, fence, time.Hour); err != nil || !ok {
		t.Fatalf("commit: ok=%v err=%v", ok, err)
	}

	first := time.Now().UnixMilli()
	if err := repo.TouchAlive(ctx, pid, first, time.Hour); err != nil {
		t.Fatalf("首次 TouchAlive: %v", err)
	}
	got, err := repo.BatchGetLastSeen(ctx, []uint64{pid})
	if err != nil || got[pid] != first {
		t.Fatalf("首次应写入: got=%d want=%d err=%v", got[pid], first, err)
	}

	// 节流窗口内的第二次:时间戳不得推进。
	within := first + AliveTouchThrottle.Milliseconds()/2
	if err := repo.TouchAlive(ctx, pid, within, 2*time.Hour); err != nil {
		t.Fatalf("节流内 TouchAlive: %v", err)
	}
	if got, _ := repo.BatchGetLastSeen(ctx, []uint64{pid}); got[pid] != first {
		t.Fatalf("节流窗口内不得推进时间戳: got=%d want=%d", got[pid], first)
	}
	// 但 TTL 必须已被续到新值(2h),否则 meta 会先于在线状态过期。
	ttl, err := client.TTL(ctx, hubMetaKey(pid)).Result()
	if err != nil || ttl <= time.Hour {
		t.Fatalf("节流也必须续 TTL(否则 meta 先过期,判定依据丢失): ttl=%v err=%v", ttl, err)
	}

	// 超过节流窗口:必须推进。
	beyond := first + AliveTouchThrottle.Milliseconds() + 1
	if err := repo.TouchAlive(ctx, pid, beyond, time.Hour); err != nil {
		t.Fatalf("超窗 TouchAlive: %v", err)
	}
	if got, _ := repo.BatchGetLastSeen(ctx, []uint64{pid}); got[pid] != beyond {
		t.Fatalf("超过节流窗口必须推进: got=%d want=%d", got[pid], beyond)
	}
}

// 没有 meta 的玩家(从没走过 fenced 路径)不得被凭空建 key —— 同 RefreshHubLocations。
func TestRealRedis_TouchAlive不凭空建Meta(t *testing.T) {
	repo, client := newRealRedis(t)
	ctx := context.Background()
	pid := realRedisPlayerBase + 7
	if err := repo.TouchAlive(ctx, pid, time.Now().UnixMilli(), time.Hour); err != nil {
		t.Fatalf("TouchAlive: %v", err)
	}
	if n, err := client.Exists(ctx, hubMetaKey(pid)).Result(); err != nil || n != 0 {
		t.Fatalf("不得为无 meta 玩家建 key(会成为无法接受 HUB 写的毒 key): exists=%d err=%v", n, err)
	}
}

// ── INC-20260813-001 回归 ────────────────────────────────────────────────────

// touchHubAliveScript 用 ARGV[3] 做节流比较,而 RefreshHubLocations 里那次内联 Eval
// 曾**只传两个 ARGV**(2026-08-12 加节流时漏改)。后果是隐蔽的:第一次心跳时 meta 还没有
// last_alive_ms,`prev` 为 nil,短路不进比较分支 → 通过;从**第二次**心跳起 `prev` 非 nil,
// `(now - prev) < tonumber(nil)` 直接 Lua 报错,整批 EVAL 失败。
//
// 因为 EXPIRE 与 EVAL 在同一 pipeline 里各自独立执行,位置 TTL 照常续上,**只有
// last_alive_ms 静默停更**——线上唯一可见的症状是每 5s 一条 hub_presence_refresh_failed
// 与 `cmdstat_eval failed_calls` 一路涨(实测 1109/1152 = 96%)。
//
// 判据必须落在**第二跳**:只跑一跳的用例在修复前也是绿的。
func TestRealRedis_RefreshHubLocations第二跳不得Lua报错(t *testing.T) {
	repo, _ := newRealRedis(t)
	ctx := context.Background()
	pid := realRedisPlayerBase + 8
	fence := connFence("assign-A", "adm-1", 1)
	if ok, err := repo.ActivateHubPresence(ctx, pid, fence, time.Hour); err != nil || !ok {
		t.Fatalf("commit: ok=%v err=%v", ok, err)
	}
	if err := repo.SetGuarded(ctx, pid, LocationRecord{
		State: 3, HubPod: "hub-1", HubPresenceFence: fence,
	}, time.Hour, 1, nil); err != nil {
		t.Fatalf("SetGuarded: %v", err)
	}

	// 第一跳:meta 还没有 last_alive_ms,修复前后都能过。
	if n, err := repo.RefreshHubLocations(ctx, "hub-1", []uint64{pid}, time.Hour, time.Hour); err != nil || n != 1 {
		t.Fatalf("第一跳: n=%d err=%v", n, err)
	}
	// 第二跳:meta 已有 last_alive_ms → 修复前必然 Lua 报错。
	if n, err := repo.RefreshHubLocations(ctx, "hub-1", []uint64{pid}, time.Hour, time.Hour); err != nil || n != 1 {
		t.Fatalf("第二跳必须与第一跳同样成功(ARGV[3] 漏传会在这里炸): n=%d err=%v", n, err)
	}
	// 再来两跳,确认不是一次性的。
	for i := 0; i < 2; i++ {
		if _, err := repo.RefreshHubLocations(ctx, "hub-1", []uint64{pid}, time.Hour, time.Hour); err != nil {
			t.Fatalf("第 %d 跳: %v", i+3, err)
		}
	}
}

// census 全员都要刷 last_alive_ms —— **包括位置投影不是 HUB 的那些人**。
//
// 为什么:matchmaker 撮合成局会把成员写成 MATCHING,而 MATCHING **没有任何保活**
// (RefreshHubLocations 只续 state==HUB)。这局若失败/取消,matchmaker 按设计不回写 HUB,
// 于是玩家明明坐在大厅里,位置 key 却在 30s 后整条消失、last_alive_ms 也停在进 MATCHING
// 那一刻 —— 对一切按 presence 判定的消费方而言等同「早已离线」。
// INC-20260724-001 的成局最终门 100% 假阳性、以及 INC-20260813-001 的 StartMatch 在线闸
// 若照搬同一信号会误伤所有人,根子都在这里。
//
// 边界同样要守住:last_alive_ms 是另一把 meta key,可以按 census 写;
// **位置投影的 TTL 绝不能**跟着一起续(那会破不变量 §1 的「非 HUB 态记录一律不动」)。
func TestRealRedis_RefreshHubLocations按census刷新非HUB态的last_alive(t *testing.T) {
	repo, client := newRealRedis(t)
	ctx := context.Background()
	pid := realRedisPlayerBase + 9
	fence := connFence("assign-A", "adm-1", 1)
	if ok, err := repo.ActivateHubPresence(ctx, pid, fence, time.Hour); err != nil || !ok {
		t.Fatalf("commit: ok=%v err=%v", ok, err)
	}
	// 玩家人在大厅,但位置投影停在撮合态(matchmaker NotifyMatching 写的,失败后无人回写)。
	if err := repo.SetGuarded(ctx, pid, LocationRecord{
		State: 4 /* MATCHING */, MatchID: 777,
	}, 10*time.Minute, 1, nil); err != nil {
		t.Fatalf("SetGuarded: %v", err)
	}
	// 造一个「很久以前」的 last_alive_ms,好让节流窗口(30s)不挡住本次推进。
	stale := time.Now().Add(-time.Hour).UnixMilli()
	if err := client.HSet(ctx, hubMetaKey(pid), "last_alive_ms", stale).Err(); err != nil {
		t.Fatalf("HSet: %v", err)
	}

	// Hub DS 心跳把他报在 census 里 —— 这就是「此刻他连在本台 Hub 上」的权威事实。
	// 位置不是 HUB,所以 refreshed 计数必须是 0(位置 TTL 一条都没续)。
	n, err := repo.RefreshHubLocations(ctx, "hub-1", []uint64{pid}, time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("RefreshHubLocations: %v", err)
	}
	if n != 0 {
		t.Fatalf("非 HUB 态的位置 TTL 绝不能续(不变量 §1): refreshed=%d want=0", n)
	}

	got, err := repo.BatchGetLastSeen(ctx, []uint64{pid})
	if err != nil {
		t.Fatal(err)
	}
	if got[pid] <= stale {
		t.Fatalf("census 里的玩家必须刷新 last_alive_ms,否则坐在大厅也会被判早已离线: got=%d stale=%d", got[pid], stale)
	}

	// 反向断言:位置 key 的 TTL 没有被续到一小时(仍是 SetGuarded 时的 10 分钟量级)。
	if ttl, err := client.TTL(ctx, locKey(pid)).Result(); err != nil || ttl > 11*time.Minute {
		t.Fatalf("非 HUB 态位置 TTL 被误续: ttl=%v err=%v", ttl, err)
	}
}
