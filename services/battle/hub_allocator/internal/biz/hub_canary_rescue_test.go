// hub_canary_rescue_test.go — 已粘 canary 轨的玩家在 canary 失容量时的 stable 回退
// 回归测试(2026-07-29 审计修复)。
//
// 修复前的缺陷:AssignHub 的 canary→stable 容量回退带 `!found` 条件,只有**首次分配**
// 才允许回退。已有 assignment 的玩家由 stickyReleaseTrack(existing) 强制留在 canary,
// 于是 canary Fleet 失去容量时(CrashLoop / 永不心跳 / 回滚把 replicas 调 0)整批
// "座位已失效"的 canary cohort 反复拿 ErrHubNoAvailable,直到 assignment TTL(30m)过期。
//
// 更关键的是运维止血手段失灵:把 canary_percent 调 0 只改 releasePolicy.Select,
// 而 found 分支根本不读策略、只读持久化记录 —— 违反 §9.21 与验收底线第 7 条
// 「异常时能立即把 Canary 权重归零,Stable 继续服务」。
//
// 修复:去掉 `!found`,回退只由 desiredTrack==Canary 把关(反向 stable→canary 仍然禁止)。
package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/releasetrack"
)

// killShard 模拟一个分片失去承载能力:Pod CrashLoop / 永不心跳后被 sweep 标 draining。
// legacy 路径的 assignmentRoutable 读的是 repo 的 shard 记录(hub.go GetShard 分支)而非
// fleet 列表,所以只从 fleet.candidates 摘掉分片不足以让既有归属失效——必须同时改 repo。
func killShard(t *testing.T, repo *fakeRepo, pod string) {
	t.Helper()
	repo.mu.Lock()
	defer repo.mu.Unlock()
	shard, ok := repo.shards[pod]
	if !ok {
		t.Fatalf("测试前提失效:repo 中不存在分片 %s", pod)
	}
	shard.State = stateDraining
	delete(repo.active, pod)
}

func TestAssignHubStickyCanaryFallsBackToStableWhenCanaryCapacityGone(t *testing.T) {
	fleet := &staticTrackFleet{candidates: []ShardCandidate{
		trackCandidate("hub-stable", releasetrack.Stable, 1),
		trackCandidate("hub-canary", releasetrack.Canary, 2),
	}}
	uc, repo := newTrackUsecase(t, 100, fleet)
	ctx := context.Background()

	// ① 玩家先落到 canary,归属被持久化为 canary 轨。
	if _, err := uc.AssignHub(ctx, 2001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("首次分配失败: %v", err)
	}
	first, found, _ := repo.GetAssignment(ctx, 2001)
	if !found || first.GetReleaseTrack() != releasetrack.Canary {
		t.Fatalf("测试前提失效,应先粘上 canary: %+v", first)
	}

	// ② canary Fleet 失去全部容量(镜像坏了 / 回滚 replicas=0),只剩 stable。
	fleet.candidates = []ShardCandidate{trackCandidate("hub-stable", releasetrack.Stable, 1)}
	killShard(t, repo, "hub-canary")

	// ③ 玩家再次进大厅:修复前在此拿 ErrHubNoAvailable 并被锁到 assignment TTL 过期。
	if _, err := uc.AssignHub(ctx, 2001, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("已粘 canary 的玩家在 canary 无容量时应回退 stable,实际错误: %v", err)
	}
	rescued, _, _ := repo.GetAssignment(ctx, 2001)
	if rescued.GetHubPodName() != "hub-stable" || rescued.GetReleaseTrack() != releasetrack.Stable {
		t.Fatalf("应已回退到 stable 分片,实际 assignment=%+v", rescued)
	}
}

func TestAssignHubStickyStableNeverFallsForwardToCanaryEvenWhenStableGone(t *testing.T) {
	// 守住回退的方向性:去掉 !found 之后,反向(stable→canary)必须仍然被禁止,
	// 否则 stable 玩家会在 stable 抖动时被甩进未验证的 canary 轨。
	fleet := &staticTrackFleet{candidates: []ShardCandidate{
		trackCandidate("hub-stable", releasetrack.Stable, 1),
	}}
	uc, repo := newTrackUsecase(t, 0, fleet)
	ctx := context.Background()

	if _, err := uc.AssignHub(ctx, 2002, "global", 0, 0, 0, ""); err != nil {
		t.Fatalf("首次分配失败: %v", err)
	}
	first, found, _ := repo.GetAssignment(ctx, 2002)
	if !found || first.GetReleaseTrack() != releasetrack.Stable {
		t.Fatalf("测试前提失效,应先粘上 stable: %+v", first)
	}

	// stable 消失、只剩 canary:必须 fail-closed,不得跨轨。
	fleet.candidates = []ShardCandidate{trackCandidate("hub-canary", releasetrack.Canary, 2)}
	killShard(t, repo, "hub-stable")
	_, err := uc.AssignHub(ctx, 2002, "global", 0, 0, 0, "")
	if errcode.As(err) != errcode.ErrHubNoAvailable {
		t.Fatalf("stable 玩家不得回退进 canary,应 ErrHubNoAvailable,实际: %v", err)
	}
}
