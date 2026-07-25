// writer_fence_redis8_test.go — 写者 fencing 的**真 Redis** 交错验证(R11 复审 P0-4)。
//
// 为什么必须有这一层:既有交错测试跑在 miniredis 上,而这两条交错的正确性完全押在
// **真实 WATCH/MULTI/EXEC 语义**上——"WATCH 期间键被改写则 EXEC 必须失败"。
// miniredis 是重实现,它与真 Redis 在这条语义上一致是**假设**而不是事实;而底线 3
// (数据完整性:不双写、不借尸还魂)就靠它。所以同样的交错要在真 Redis 上再跑一遍。
//
// 门控(沿用仓库既有约定,见 ds_allocator/internal/poduidpreflight/redis_security_test.go):
//
//	PANDORA_TEST_REDIS8_ADDR=127.0.0.1:6379 go test ./services/battle/hub_allocator/internal/data/ -run Redis8
//
// 未设置该环境变量时整组 Skip,所以默认 CI 不受影响。集群里可以直接:
//
//	kubectl -n pandora port-forward svc/redis 6379:6379
//
// 测试只使用带唯一前缀的键并在结束时清理,不会碰到业务数据。
package data

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// newRealRedisRepo 连真 Redis 并返回仓库;未配置地址即 Skip。
// playerID/pod 由调用方用 t.Name() 派生,避免并行测试互相踩键。
func newRealRedisRepo(t *testing.T) (*RedisHubRepo, *redis.Client) {
	t.Helper()
	addr := strings.TrimSpace(os.Getenv("PANDORA_TEST_REDIS8_ADDR"))
	if addr == "" {
		t.Skip("set PANDORA_TEST_REDIS8_ADDR for the real-Redis fencing interleave suite")
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: os.Getenv("PANDORA_TEST_REDIS8_PASSWORD"),
	})
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping real redis %s: %v", addr, err)
	}
	return NewRedisHubRepo(rdb), rdb
}

// cleanupKeys 结束时精确删掉本测试用过的键(不用 FLUSHDB —— 那会毁掉别人的数据)。
func cleanupKeys(t *testing.T, rdb *redis.Client, keys ...string) {
	t.Helper()
	t.Cleanup(func() {
		if len(keys) > 0 {
			_ = rdb.Del(context.Background(), keys...).Err()
		}
	})
}

// 真 Redis 版:问题 A(删除即复位 / 借尸还魂)。
// 交错:旧写者 token=7 暂停 → 继任者 token=9 创建并合法删除 → 旧写者恢复走创建路径。
// 关闭标准:旧写者零写入、零删除;墓碑必须留存。
func TestRedis8_AssignmentDeleteThenReviveByStaleWriterRejected(t *testing.T) {
	ctx := context.Background()
	repo, rdb := newRealRedisRepo(t)
	// 用极大 playerID 避开真实业务 ID 空间。
	const playerID = uint64(1 << 62)
	cleanupKeys(t, rdb, assignKey(playerID))

	// 起点干净(上一次失败的残留不能污染判定)。
	if err := rdb.Del(ctx, assignKey(playerID)).Err(); err != nil {
		t.Fatalf("reset assignment key: %v", err)
	}

	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})
	if swapped, err := repo.CompareAndSwapAssignment(ctx, playerID, nil,
		assignmentFixture(playerID, "pod-A"), testTTL); err != nil || !swapped {
		t.Fatalf("create assignment: swapped=%v err=%v", swapped, err)
	}
	stored, _, _ := repo.GetAssignment(ctx, playerID)

	repo.SetWriterFence(&fakeWriterFence{token: 9, held: true})
	if swapped, err := repo.CompareAndSwapAssignment(ctx, playerID, stored,
		assignmentFixture(playerID, "pod-B"), testTTL); err != nil || !swapped {
		t.Fatalf("successor swap: swapped=%v err=%v", swapped, err)
	}
	successor, _, _ := repo.GetAssignment(ctx, playerID)
	if deleted, err := repo.CompareAndSwapAssignment(ctx, playerID, successor, nil, 0); err != nil || !deleted {
		t.Fatalf("successor delete: deleted=%v err=%v", deleted, err)
	}
	if _, found, err := repo.GetAssignment(ctx, playerID); err != nil || found {
		t.Fatalf("after delete the player must have no assignment: found=%v err=%v", found, err)
	}
	// 真 Redis 上确认墓碑真的落盘了(裸 DEL 的话这里 exists=0)。
	if n, err := rdb.Exists(ctx, assignKey(playerID)).Result(); err != nil || n != 1 {
		t.Fatalf("delete must leave a fencing tombstone on real Redis: exists=%d err=%v", n, err)
	}

	// 前任恢复:走 expected=nil 创建路径,必须零写入被拒。
	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})
	if _, err := repo.CompareAndSwapAssignment(ctx, playerID, nil,
		assignmentFixture(playerID, "pod-C"), testTTL); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("stale writer must not revive a deleted assignment on real Redis, got %v", err)
	}
	if _, found, _ := repo.GetAssignment(ctx, playerID); found {
		t.Fatal("stale writer revived the assignment: zero-write contract violated on real Redis")
	}

	// 当届写者仍能在墓碑之上重建(墓碑不得变成永久拒服)。
	repo.SetWriterFence(&fakeWriterFence{token: 9, held: true})
	if swapped, err := repo.CompareAndSwapAssignment(ctx, playerID, nil,
		assignmentFixture(playerID, "pod-D"), testTTL); err != nil || !swapped {
		t.Fatalf("current writer must re-create over a tombstone: swapped=%v err=%v", swapped, err)
	}
	rebuilt, found, _ := repo.GetAssignment(ctx, playerID)
	if !found || rebuilt.GetHubPodName() != "pod-D" || rebuilt.GetWriterToken() != 9 {
		t.Fatalf("re-created assignment wrong: found=%v rec=%+v", found, rebuilt)
	}
}

// 真 Redis 版:问题 B(租约在事务内读)。
// 钩子在第 1 次 attempt 的事务体内原值重写归属键——只有当 Current() 确实在 Watch 回调内
// 被调用时,这次写才落在 WATCH 注册之后,真 Redis 才会让 EXEC 失败并触发重试;
// 第 2 次 attempt 时本副本已失租,必须零写入被拒。
//
// 这条同时是对真 Redis「WATCH 期间键被改写 → EXEC 失败」语义的直接断言。
func TestRedis8_AssignmentLeaseLostBetweenAttemptsRejected(t *testing.T) {
	ctx := context.Background()
	repo, rdb := newRealRedisRepo(t)
	const playerID = uint64(1<<62) + 1
	cleanupKeys(t, rdb, assignKey(playerID))
	if err := rdb.Del(ctx, assignKey(playerID)).Err(); err != nil {
		t.Fatalf("reset assignment key: %v", err)
	}

	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})
	if swapped, err := repo.CompareAndSwapAssignment(ctx, playerID, nil,
		assignmentFixture(playerID, "pod-A"), testTTL); err != nil || !swapped {
		t.Fatalf("create assignment: swapped=%v err=%v", swapped, err)
	}
	stored, _, _ := repo.GetAssignment(ctx, playerID)
	payload, merr := proto.Marshal(stored)
	if merr != nil {
		t.Fatalf("marshal stored record: %v", merr)
	}

	fence := &hookedWriterFence{token: 7, held: true}
	fence.onCall = func(f *hookedWriterFence, call int) {
		switch call {
		case 1:
			// 写脏被 WATCH 的键(原值重写,与水位比较无关,隔离问题 B)。
			if err := rdb.Set(ctx, assignKey(playerID), payload, testTTL).Err(); err != nil {
				t.Fatalf("dirty the watched key: %v", err)
			}
		case 2:
			f.held = false
		}
	}
	repo.SetWriterFence(fence)

	swapped, err := repo.CompareAndSwapAssignment(ctx, playerID, stored,
		assignmentFixture(playerID, "pod-C"), testTTL)
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("writer that lost its lease mid-CAS must be rejected on real Redis: swapped=%v err=%v",
			swapped, err)
	}
	if fence.calls < 2 {
		t.Fatalf("real Redis WATCH must have aborted attempt 1 and forced a re-read; lease reads=%d",
			fence.calls)
	}
	after, found, _ := repo.GetAssignment(ctx, playerID)
	if !found || after.GetHubPodName() != "pod-A" {
		t.Fatalf("superseded writer mutated the assignment on real Redis: found=%v rec=%+v", found, after)
	}
}

// 真 Redis 版:{pod} 域 A 级入口的落后 token 拒写(与 miniredis 版同契约)。
func TestRedis8_ShardWritePathsRejectStaleWriter(t *testing.T) {
	ctx := context.Background()
	repo, rdb := newRealRedisRepo(t)
	pod := fmt.Sprintf("pandora-hub-r11-%d", os.Getpid())
	const playerID = uint64(1<<62) + 2
	cleanupKeys(t, rdb, shardKey(pod), membersKey(pod), wfenceKey(pod),
		transferCleanupKey(pod), shardsSetKey, activeKey)

	// 继任者(第 9 届)已推扫过本 pod 的水位。
	if err := rdb.Set(ctx, wfenceKey(pod), "9", 0).Err(); err != nil {
		t.Fatalf("seed fence watermark: %v", err)
	}
	repo.SetWriterFence(&fakeWriterFence{token: 7, held: true})

	if err := repo.AddShardMember(ctx, pod, playerID, testTTL); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("AddShardMember 必须拒绝落后 token,got %v", err)
	}
	if n, _ := rdb.Exists(ctx, membersKey(pod)).Result(); n != 0 {
		t.Fatal("被拒的 AddShardMember 不得建成员索引")
	}
	if err := repo.RemoveShard(ctx, pod); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("RemoveShard 必须拒绝落后 token,got %v", err)
	}
	if err := repo.RegisterTransferCleanup(ctx, pod,
		TransferCleanupRef{PlayerID: playerID, TargetAssignmentID: "t-1"}); errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("RegisterTransferCleanup 必须拒绝落后 token,got %v", err)
	}
	// 水位必须保持 9(被拒的写不得推进/回退水位)。
	if v, _ := rdb.Get(ctx, wfenceKey(pod)).Result(); v != "9" {
		t.Fatalf("rejected writes must not touch the watermark, got %q", v)
	}

	// 当届写者(第 11 届)正常通过并把水位推到本届。
	repo.SetWriterFence(&fakeWriterFence{token: 11, held: true})
	if err := repo.AddShardMember(ctx, pod, playerID, testTTL); err != nil {
		t.Fatalf("current writer AddShardMember: %v", err)
	}
	if v, _ := rdb.Get(ctx, wfenceKey(pod)).Result(); v != "11" {
		t.Fatalf("current writer must advance the watermark to its own term, got %q", v)
	}
}
