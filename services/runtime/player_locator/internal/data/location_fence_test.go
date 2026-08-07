package data

import (
	"context"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestParseLocationMap_HubPresenceFence保留完整Uint64序号(t *testing.T) {
	const maxSeq = ^uint64(0)
	rec := parseLocationMap(map[string]string{
		"state":             "3",
		"hub_pod":           "hub-1",
		"hub_assignment_id": "assignment-42",
		"hub_admission_id":  "admission-max",
		"hub_admission_seq": strconv.FormatUint(maxSeq, 10),
		"updated_at_ms":     "1234",
	})
	want := HubPresenceFence{
		AssignmentID: "assignment-42", AdmissionID: "admission-max", AdmissionSeq: maxSeq,
	}
	if !rec.HubPresenceFence.Equal(want) {
		t.Fatalf("Redis decimal uint64 fence round-trip lost precision: got=%+v want=%+v",
			rec.HubPresenceFence, want)
	}
}

func TestHubPresenceFence_完整性判定(t *testing.T) {
	if !(HubPresenceFence{}).IsZero() {
		t.Fatal("全零 fence 应识别为 legacy")
	}
	complete := HubPresenceFence{AssignmentID: "a", AdmissionID: "b", AdmissionSeq: 1}
	if !complete.IsComplete() || complete.IsZero() {
		t.Fatalf("完整 fence 判定错误: %+v", complete)
	}
	partial := HubPresenceFence{AssignmentID: "a"}
	if partial.IsZero() || partial.IsComplete() {
		t.Fatalf("残缺 fence 不得冒充 legacy 或完整身份: %+v", partial)
	}
}

func newFenceRedis(t *testing.T) (*RedisLocationRepo, *miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedisLocationRepo(client), mr, client
}

// connFence 造一个连接三元组 fence（本投影唯一认识的身份形态）。
func connFence(assignment, admission string, seq uint64) HubPresenceFence {
	return HubPresenceFence{AssignmentID: assignment, AdmissionID: admission, AdmissionSeq: seq}
}

func TestHubPresenceLua_坏Mode必须FailClosed且零修改(t *testing.T) {
	repo, mr, client := newFenceRedis(t)
	ctx := context.Background()
	key := hubMetaKey(42)
	mr.HSet(key, "mode", "corrupt", "sentinel", "keep")
	before, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	fence := connFence("assignment-42", "admission-a", 1)
	if ok, err := repo.ValidateHubPresence(ctx, 42, fence); err != nil || ok {
		t.Fatalf("corrupt mode validate must reject: ok=%v err=%v", ok, err)
	}
	if ok, err := repo.ActivateHubPresence(ctx, 42, fence, time.Hour); err != nil || ok {
		t.Fatalf("corrupt mode commit must reject: ok=%v err=%v", ok, err)
	}
	after, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("corrupt mode 被覆盖: before=%v after=%v", before, after)
	}

	mr.Del(key)
	mr.HSet(key, "sentinel", "mode-missing")
	before, _ = client.HGetAll(ctx, key).Result()
	if ok, err := repo.ValidateHubPresence(ctx, 42, fence); err != nil || ok {
		t.Fatalf("existing hash without mode validate must reject: ok=%v err=%v", ok, err)
	}
	if ok, err := repo.ActivateHubPresence(ctx, 42, fence, time.Hour); err != nil || ok {
		t.Fatalf("existing hash without mode commit must reject: ok=%v err=%v", ok, err)
	}
	after, _ = client.HGetAll(ctx, key).Result()
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("missing mode 被覆盖: before=%v after=%v", before, after)
	}
}

func TestHubPresenceLua_跨Assignment接受且Validate只读(t *testing.T) {
	repo, _, client := newFenceRedis(t)
	ctx := context.Background()
	oldFence := connFence("assignment-old", "admission-old", 1)
	newFence := connFence("assignment-new", "admission-new", 1)
	if ok, err := repo.ActivateHubPresence(ctx, 42, oldFence, time.Hour); err != nil || !ok {
		t.Fatalf("seed commit: ok=%v err=%v", ok, err)
	}
	before, err := client.HGetAll(ctx, hubMetaKey(42)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := repo.ValidateHubPresence(ctx, 42, newFence); err != nil || !ok {
		t.Fatalf("new epoch validate: ok=%v err=%v", ok, err)
	}
	afterValidate, err := client.HGetAll(ctx, hubMetaKey(42)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterValidate, before) {
		t.Fatalf("validate 不得推进 meta: before=%v after=%v", before, afterValidate)
	}
	if ok, err := repo.ActivateHubPresence(ctx, 42, newFence, time.Hour); err != nil || !ok {
		t.Fatalf("new assignment commit: ok=%v err=%v", ok, err)
	}
	// 跨 assignment **刻意双向接受**:玩家该属于哪台 Hub 由 hub_allocator 的
	// assignment / placement 权威决定;本投影没有全局代际、也不该有 —— 想在这里反向
	// 定序就得实时查 owner 服务,等于让「进大厅写位置」强依赖它(挂了玩家进不去大厅)。
	// 这不是漏判,是职责边界(§9.22:locator 是 presence 投影,不是 owner authority)。
	if ok, err := repo.ValidateHubPresence(ctx, 42, oldFence); err != nil || !ok {
		t.Fatalf("跨 assignment 应接受(归属由 hub_allocator 定): ok=%v err=%v", ok, err)
	}
}

// 这条才是本投影真正要守的线:同一个 assignment 内,旧 admission 的迟到写不得夺回投影。
// 「同 Pod 秒重连,旧连接的 Logout/SetLocation 晚到」就落在这里。
func TestHubPresenceLua_同Assignment内旧Admission必须拒(t *testing.T) {
	repo, _, _ := newFenceRedis(t)
	ctx := context.Background()
	current := connFence("assignment-42", "admission-new", 7)
	if ok, err := repo.ActivateHubPresence(ctx, 42, current, time.Hour); err != nil || !ok {
		t.Fatalf("seed commit: ok=%v err=%v", ok, err)
	}

	// ① 同 assignment、更小 seq = 旧连接迟到 → 拒。
	if ok, err := repo.ValidateHubPresence(ctx, 42, connFence("assignment-42", "admission-old", 6)); err != nil || ok {
		t.Fatalf("同 assignment 旧 seq 必须拒: ok=%v err=%v", ok, err)
	}
	// ② 同 seq 但不同 admission_id = 同序号 ABA → 拒。
	if ok, err := repo.ValidateHubPresence(ctx, 42, connFence("assignment-42", "admission-other", 7)); err != nil || ok {
		t.Fatalf("同序号 ABA 必须拒: ok=%v err=%v", ok, err)
	}
	// ③ 同 assignment、更大 seq = 真正的新连接 → 接受。
	if ok, err := repo.ValidateHubPresence(ctx, 42, connFence("assignment-42", "admission-newer", 8)); err != nil || !ok {
		t.Fatalf("同 assignment 新 seq 应接受: ok=%v err=%v", ok, err)
	}
}

func TestHubPresenceLua_大于2的53次方AdmissionSeq不折叠(t *testing.T) {
	repo, _, _ := newFenceRedis(t)
	ctx := context.Background()
	oldFence := connFence("assignment-42", "admission-old", 9_007_199_254_740_992)
	newFence := connFence("assignment-42", "admission-new", 9_007_199_254_740_993)
	if ok, err := repo.ActivateHubPresence(ctx, 42, oldFence, time.Hour); err != nil || !ok {
		t.Fatalf("old commit: ok=%v err=%v", ok, err)
	}
	if ok, err := repo.ActivateHubPresence(ctx, 42, newFence, time.Hour); err != nil || !ok {
		t.Fatalf("adjacent new seq must win: ok=%v err=%v", ok, err)
	}
	if ok, err := repo.ValidateHubPresence(ctx, 42, oldFence); err != nil || ok {
		t.Fatalf("adjacent old seq must reject: ok=%v err=%v", ok, err)
	}
}

func TestHubPresenceLua_Exact已Left不得复活(t *testing.T) {
	repo, mr, _ := newFenceRedis(t)
	ctx := context.Background()
	fence := connFence("assignment-42", "admission-a", 1)
	if ok, err := repo.ActivateHubPresence(ctx, 42, fence, time.Hour); err != nil || !ok {
		t.Fatalf("seed commit: ok=%v err=%v", ok, err)
	}
	mr.HSet(hubMetaKey(42), "left_at_ms", "1800000000123")
	if ok, err := repo.ValidateHubPresence(ctx, 42, fence); err != nil || ok {
		t.Fatalf("exact left validate must reject: ok=%v err=%v", ok, err)
	}
	if ok, err := repo.ActivateHubPresence(ctx, 42, fence, time.Hour); err != nil || ok {
		t.Fatalf("exact left commit must reject: ok=%v err=%v", ok, err)
	}
}

// ── Hub DS 整台崩溃:没有任何 ReportDisconnect，靠 last_alive_ms 兜底 ─────────────

// 这条覆盖 H2:Hub DS 崩溃 / 被 OOM kill / 网络分区时，locator 收不到任何 Logout，
// 写不出 left_at_ms。此前 BatchGetLastSeen 只能返回缺席(UNKNOWN)，消费方一律不动作，
// 那一批玩家就永远挂在队伍里。心跳续期顺手推的 last_alive_ms 是唯一能留下的时间线索。
func TestBatchGetLastSeen_Hub崩溃时回退到最后一次心跳时刻(t *testing.T) {
	repo, _, _ := newFenceRedis(t)
	ctx := context.Background()
	fence := connFence("assignment-42", "admission-a", 1)
	if ok, err := repo.ActivateHubPresence(ctx, 42, fence, time.Hour); err != nil || !ok {
		t.Fatalf("seed commit: ok=%v err=%v", ok, err)
	}

	// 还没有任何心跳、也没有离开上报 → 无从判断，必须缺席(UNKNOWN)而不是回填 0。
	if got, err := repo.BatchGetLastSeen(ctx, []uint64{42}); err != nil {
		t.Fatal(err)
	} else if _, ok := got[42]; ok {
		t.Fatalf("无任何时间线索时必须缺席判 UNKNOWN, got=%v", got)
	}

	// 心跳把该玩家报为在场 → 落 last_alive_ms。
	before := time.Now().UnixMilli()
	if _, err := repo.RefreshHubLocations(ctx, "hub-1", []uint64{42}, 0, time.Hour); err != nil {
		t.Fatalf("RefreshHubLocations: %v", err)
	}
	// 注意:该玩家没有 location 记录(模拟只维护 meta 的场景)，refreshed 会是 0，
	// 但 meta 的续期分支只在计数命中时才走 —— 所以这里补一条真实 HUB 位置再刷一次。
	if err := repo.SetGuarded(ctx, 42, LocationRecord{
		State: 3, HubPod: "hub-1", HubPresenceFence: fence,
	}, time.Hour, 1, nil); err != nil {
		t.Fatalf("SetGuarded: %v", err)
	}
	if n, err := repo.RefreshHubLocations(ctx, "hub-1", []uint64{42}, time.Hour, time.Hour); err != nil || n != 1 {
		t.Fatalf("RefreshHubLocations: n=%d err=%v", n, err)
	}

	got, err := repo.BatchGetLastSeen(ctx, []uint64{42})
	if err != nil {
		t.Fatal(err)
	}
	ms, ok := got[42]
	if !ok {
		t.Fatal("心跳过后必须能回答「最后一次被观测在线」，否则 Hub 崩溃那批玩家永远清不掉")
	}
	if ms < before {
		t.Fatalf("last_alive_ms 应为本次心跳时刻: got=%d before=%d", ms, before)
	}

	// 显式离开更精确，必须压过心跳时刻。
	leftAt := time.Now().Add(time.Second).UnixMilli()
	if recorded, eff, err := repo.RecordLastSeen(ctx, 42, fence, leftAt, time.Hour); err != nil || !recorded || eff != leftAt {
		t.Fatalf("RecordLastSeen: recorded=%v eff=%d err=%v", recorded, eff, err)
	}
	if got, err := repo.BatchGetLastSeen(ctx, []uint64{42}); err != nil {
		t.Fatal(err)
	} else if got[42] != leftAt {
		t.Fatalf("left_at_ms 必须优先于 last_alive_ms: got=%d want=%d", got[42], leftAt)
	}
}

// 心跳绝不能给「从没走过 fenced 路径」的玩家凭空建 meta:一个有内容但没有 mode 字段的
// meta 会被 hubPresenceScript 判为损坏并 fail-closed，等于造出永远无法接受 HUB 写的毒 key。
func TestRefreshHubLocations_不得凭空建Meta(t *testing.T) {
	repo, _, client := newFenceRedis(t)
	ctx := context.Background()
	if err := repo.SetGuarded(ctx, 77, LocationRecord{State: 3, HubPod: "hub-1"}, time.Hour, 1, nil); err != nil {
		t.Fatalf("SetGuarded: %v", err)
	}
	if n, err := repo.RefreshHubLocations(ctx, "hub-1", []uint64{77}, time.Hour, time.Hour); err != nil || n != 1 {
		t.Fatalf("RefreshHubLocations: n=%d err=%v", n, err)
	}
	if n, err := client.Exists(ctx, hubMetaKey(77)).Result(); err != nil || n != 0 {
		t.Fatalf("心跳不得为无 meta 的玩家建 key(会变成无法接受 HUB 写的毒 key): exists=%d err=%v", n, err)
	}
	// 而且这个玩家仍然能正常走 fenced 上线。
	if ok, err := repo.ActivateHubPresence(ctx, 77, connFence("assignment-77", "admission-a", 1), time.Hour); err != nil || !ok {
		t.Fatalf("无 meta 的玩家应能正常 fenced 上线: ok=%v err=%v", ok, err)
	}
}
