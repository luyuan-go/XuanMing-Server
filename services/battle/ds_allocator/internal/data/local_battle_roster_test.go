// local_battle_roster_test.go — mode=local 战斗准入元数据 env 投递回归(2026-08-04)。
//
// 背景:UE 的 ExpectedPlayers 只从 Agones GameServer annotation 装载;mode=local 没有
// annotation 通道,花名册恒空,APandoraPveGameMode 主动退出因 roster_count==0 恒判
// AuthorityNotReady —— 玩家能进副本能打怪,点「退出副本」却永远被拒(实测 DS 日志
// result=4 canonical_pve=1 roster_count=0)。修法 = 经 DS 进程 env 投递同一份三件套。
//
// 本文件锁死编码契约:必须与 UE 的 ParseCanonicalBattleRoster 逐条同规则,
// 否则投出去也会被对面整份判非法(把"没投"伪装成"格式错",更难查)。
package data

import (
	"context"
	"strings"
	"testing"
	"time"
)

// 死锁回归(2026-08-04 实伤):Allocate 全程持 l.mu,而 buildEnv 在同一调用链内被调用。
// buildEnv 若再取 l.mu(Go 互斥不可重入)即自锁死 —— 表现为 DS 进程永远不被拉起、
// AllocateBattle 无限挂起、对局最终按空 pod 判弃,且**没有任何报错日志**。
// 本测试用带超时的完整 Allocate 路径把它钉死:回归时超时失败而不是静默挂住整个测试进程。
func TestAllocate_WithPendingRosterDoesNotDeadlock(t *testing.T) {
	l := &LocalGameServerAllocator{
		procs:     make(map[string]*launchedProc),
		usedPorts: make(map[int]struct{}),
		portProbe: func(int) bool { return true },
	}
	l.cfg.PortBase = 7800
	l.cfg.PortRange = 10
	l.cfg.AdvertiseHost = "127.0.0.1"
	// 假进程:不真 exec,但仍走 defaultStart 之外的同一 startProc 契约。
	l.startProc = func(podName string, port int, matchID uint64, mapID uint32, gameMode, token string) (dsProcess, error) {
		// 模拟 defaultStart 在持锁状态下调用 buildEnv 的真实时序。
		_ = l.buildEnv(podName, matchID, mapID, gameMode, token)
		return fakeDSProcess{}, nil
	}
	l.SetPendingBattleRoster(777, []uint64{1001}, "alloc-x", "stable")

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, _, _, err := l.Allocate(context.Background(), 777, 7, "pve_coop", "stable"); err != nil {
			t.Errorf("Allocate err: %v", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Allocate 超时:buildEnv 很可能在持锁链路里重复取 l.mu(自锁死)")
	}
}

type fakeDSProcess struct{}

func (fakeDSProcess) Kill() error { return nil }
func (fakeDSProcess) Wait() error { return nil }

func TestCanonicalRosterText(t *testing.T) {
	cases := []struct {
		name string
		in   []uint64
		want string
	}{
		{"单人", []uint64{1001}, "1001"},
		{"乱序必须升序输出", []uint64{300, 100, 200}, "100,200,300"},
		{"重复必须去重", []uint64{7, 7, 7}, "7"},
		{"去重后仍升序", []uint64{9, 3, 9, 1}, "1,3,9"},
		{"大 uint64 不丢精度", []uint64{20157286542508032}, "20157286542508032"},
		{"空名单不投递", nil, ""},
		{"含零 ID 整份拒绝", []uint64{1001, 0}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := canonicalRosterText(c.in); got != c.want {
				t.Fatalf("canonicalRosterText(%v) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// UE 侧 MaxBattleRosterPlayers=128:恰好 128 通过,129 整份拒绝(不许截断后投一半)。
func TestCanonicalRosterText_RespectsUpperBound(t *testing.T) {
	ids := make([]uint64, 0, 129)
	for i := 1; i <= 128; i++ {
		ids = append(ids, uint64(i))
	}
	got := canonicalRosterText(ids)
	if got == "" || strings.Count(got, ",") != 127 {
		t.Fatalf("128 人应通过且有 127 个分隔符, got %d 个", strings.Count(got, ","))
	}
	if over := canonicalRosterText(append(ids, 129)); over != "" {
		t.Fatalf("129 人必须整份拒绝,不得截断, got %q", over)
	}
}

// 三件套必须同投同不投:缺任一项都不投递(UE 对缺项整份判非法)。
func TestBuildEnv_BattleAdmissionTripletAllOrNothing(t *testing.T) {
	const matchID uint64 = 4242
	cases := []struct {
		name         string
		players      []uint64
		allocationID string
		releaseTrack string
		wantInjected bool
	}{
		{"三件套齐备", []uint64{1001}, "alloc-1", "stable", true},
		{"缺 allocation_id", []uint64{1001}, "", "stable", false},
		{"缺 release_track", []uint64{1001}, "alloc-1", "", false},
		{"名单为空", nil, "alloc-1", "stable", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			l := &LocalGameServerAllocator{}
			l.SetPendingBattleRoster(matchID, c.players, c.allocationID, c.releaseTrack)
			env := l.buildEnv("pod-x", matchID, 7, "pve_coop", "")
			joined := strings.Join(env, "\n")
			has := strings.Contains(joined, "PANDORA_BATTLE_ROSTER=")
			if has != c.wantInjected {
				t.Fatalf("投递与否不符预期: got=%v want=%v", has, c.wantInjected)
			}
			if c.wantInjected {
				for _, key := range []string{"PANDORA_BATTLE_ROSTER=1001",
					"PANDORA_ALLOCATION_ID=alloc-1", "PANDORA_RELEASE_TRACK=stable"} {
					if !strings.Contains(joined, key) {
						t.Fatalf("缺少 env %q", key)
					}
				}
			} else {
				// 半份投递比不投更坏:UE 会把它判成"格式错"而不是"没投"。
				for _, key := range []string{"PANDORA_ALLOCATION_ID=", "PANDORA_RELEASE_TRACK="} {
					if strings.Contains(joined, key) {
						t.Fatalf("不完整时不得投递任何一件: 命中 %q", key)
					}
				}
			}
		})
	}
}

// 登记必须按 matchID 精确匹配,且消费后即删(不泄漏给下一局、台账不无界增长)。
func TestSetPendingBattleRoster_ConsumedOncePerMatch(t *testing.T) {
	l := &LocalGameServerAllocator{}
	l.SetPendingBattleRoster(100, []uint64{1001}, "alloc-a", "stable")

	if joined := strings.Join(l.buildEnv("pod", 200, 7, "pve_coop", ""), "\n"); strings.Contains(joined, "PANDORA_BATTLE_ROSTER=") {
		t.Fatal("别的 match 不得拿到本局名单")
	}
	if joined := strings.Join(l.buildEnv("pod", 100, 7, "pve_coop", ""), "\n"); !strings.Contains(joined, "PANDORA_BATTLE_ROSTER=1001") {
		t.Fatal("本局应拿到自己的名单")
	}
	if joined := strings.Join(l.buildEnv("pod", 100, 7, "pve_coop", ""), "\n"); strings.Contains(joined, "PANDORA_BATTLE_ROSTER=") {
		t.Fatal("消费后必须删除,不得重复投递给同 match 的二次拉起")
	}
}
