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
		"state":                  "3",
		"hub_pod":                "hub-1",
		"hub_assignment_id":      "assignment-42",
		"hub_admission_id":       "admission-max",
		"hub_admission_seq":      strconv.FormatUint(maxSeq, 10),
		"hub_owner_epoch":        strconv.FormatUint(maxSeq, 10),
		"hub_owner_operation_id": "operation-max",
		"updated_at_ms":          "1234",
	})
	want := HubPresenceFence{
		AssignmentID: "assignment-42", AdmissionID: "admission-max", AdmissionSeq: maxSeq,
		OwnerEpoch: maxSeq, OwnerOperationID: "operation-max",
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
	complete := HubPresenceFence{
		AssignmentID: "a", AdmissionID: "b", AdmissionSeq: 1,
		OwnerEpoch: 2, OwnerOperationID: "op",
	}
	if !complete.IsComplete() || !complete.IsFullyFenced() || complete.IsZero() {
		t.Fatalf("完整 fence 判定错误: %+v", complete)
	}
	connectionOnly := HubPresenceFence{AssignmentID: "a", AdmissionID: "b", AdmissionSeq: 1}
	if !connectionOnly.IsComplete() || connectionOnly.IsFullyFenced() || connectionOnly.IsZero() {
		t.Fatalf("Disconnect 连接三元组与 owner 全序完整性必须分开: %+v", connectionOnly)
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

func authoritativeFence(assignment, admission string, seq, ownerEpoch uint64, operation string) HubPresenceFence {
	return HubPresenceFence{
		AssignmentID: assignment, AdmissionID: admission, AdmissionSeq: seq,
		OwnerEpoch: ownerEpoch, OwnerOperationID: operation,
	}
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
	fence := authoritativeFence("assignment-42", "admission-a", 1, 8, "op-8")
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

func TestHubPresenceLua_OwnerEpoch跨Assignment全序且Validate只读(t *testing.T) {
	repo, _, client := newFenceRedis(t)
	ctx := context.Background()
	oldFence := authoritativeFence("assignment-old", "admission-old", 1, 10, "op-10")
	newFence := authoritativeFence("assignment-new", "admission-new", 1, 11, "op-11")
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
		t.Fatalf("new epoch commit: ok=%v err=%v", ok, err)
	}
	if ok, err := repo.ValidateHubPresence(ctx, 42, oldFence); err != nil || ok {
		t.Fatalf("old cross-assignment epoch must reject: ok=%v err=%v", ok, err)
	}
}

func TestHubPresenceLua_大于2的53次方AdmissionSeq不折叠(t *testing.T) {
	repo, _, _ := newFenceRedis(t)
	ctx := context.Background()
	oldFence := authoritativeFence("assignment-42", "admission-old", 9_007_199_254_740_992, 8, "op-8")
	newFence := authoritativeFence("assignment-42", "admission-new", 9_007_199_254_740_993, 8, "op-8")
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
	fence := authoritativeFence("assignment-42", "admission-a", 1, 8, "op-8")
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
