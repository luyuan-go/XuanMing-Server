// locator_test.go — LocatorUsecase 单测(W3 ⑤,2026-06-05)。
//
// 覆盖:
//   - SetLocation 输入校验(player_id 0、state 越界、HUB 缺 hub_pod、MATCHING 缺 match_id、BATTLE 缺 battle_pod)
//   - SetLocation OK + 回放 GetLocation 读取
//   - GetLocation 不存在 → OFFLINE 占位
//   - ClearLocation OK + 再 Get → OFFLINE
//
// 不接真实 redis;用一个简易的内存 stub 替 data.LocationRepo,验 biz 逻辑闭环。
package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/placement"
	"github.com/luyuancpp/pandora/services/runtime/player_locator/internal/data"
)

// stubRepo 内存版 LocationRepo,只供单测用。
type stubRepo struct {
	lastAlive      map[uint64]int64
	store          map[uint64]data.LocationRecord
	lastSeen       map[uint64]int64
	meta           map[uint64]data.HubPresenceFence
	metaExists     map[uint64]bool
	refreshMetaTTL time.Duration
}

func newStubRepo() *stubRepo {
	return &stubRepo{
		store:      map[uint64]data.LocationRecord{},
		lastSeen:   map[uint64]int64{},
		meta:       map[uint64]data.HubPresenceFence{},
		metaExists: map[uint64]bool{},
	}
}

func (s *stubRepo) SetGuarded(_ context.Context, playerID uint64, rec data.LocationRecord, _ time.Duration, _ int, guard func(cur data.LocationRecord, found bool) error) error {
	cur, found := s.store[playerID]
	if guard != nil {
		if err := guard(cur, found); err != nil {
			return err
		}
	}
	s.store[playerID] = rec
	return nil
}

func (s *stubRepo) Get(_ context.Context, playerID uint64) (data.LocationRecord, bool, error) {
	rec, ok := s.store[playerID]
	if !ok {
		return data.LocationRecord{}, false, nil
	}
	return rec, true, nil
}

func (s *stubRepo) BatchGet(_ context.Context, playerIDs []uint64) (map[uint64]data.LocationRecord, error) {
	out := make(map[uint64]data.LocationRecord, len(playerIDs))
	for _, pid := range playerIDs {
		if pid == 0 {
			continue
		}
		if rec, ok := s.store[pid]; ok {
			out[pid] = rec
		}
	}
	return out, nil
}

func (s *stubRepo) Delete(_ context.Context, playerID uint64) error {
	delete(s.store, playerID)
	return nil
}

func (s *stubRepo) ValidateHubPresence(_ context.Context, playerID uint64, fence data.HubPresenceFence) (bool, error) {
	// 与 data 层 Lua 同一套判定:只在**同 assignment 内**定序;跨 assignment 一律接受
	// (归属由 hub_allocator 权威决定,本投影不反向定序)。
	if current, ok := s.meta[playerID]; ok && current.IsComplete() {
		if fence.IsZero() {
			return false, nil
		}
		if current.AssignmentID == fence.AssignmentID &&
			(fence.AdmissionSeq < current.AdmissionSeq ||
				(fence.AdmissionSeq == current.AdmissionSeq && fence.AdmissionID != current.AdmissionID)) {
			return false, nil
		}
		if current.Equal(fence) {
			if _, left := s.lastSeen[playerID]; left {
				return false, nil
			}
		}
	}
	return true, nil
}

func (s *stubRepo) ActivateHubPresence(ctx context.Context, playerID uint64, fence data.HubPresenceFence, _ time.Duration) (bool, error) {
	accepted, err := s.ValidateHubPresence(ctx, playerID, fence)
	if err != nil || !accepted {
		return accepted, err
	}
	s.meta[playerID] = fence
	s.metaExists[playerID] = true
	delete(s.lastSeen, playerID)
	return true, nil
}

func (s *stubRepo) BatchGetLastSeen(_ context.Context, playerIDs []uint64) (map[uint64]int64, error) {
	out := make(map[uint64]int64, len(playerIDs))
	for _, pid := range playerIDs {
		if pid == 0 {
			continue
		}
		if ms, ok := s.lastSeen[pid]; ok {
			out[pid] = ms
		}
	}
	return out, nil
}

func (s *stubRepo) ShrinkHubTTL(_ context.Context, hubPod string, playerID uint64, fence data.HubPresenceFence, _ time.Duration) (bool, bool, error) {
	rec, ok := s.store[playerID]
	if !ok || rec.State != LocationStateHub || rec.HubPod != hubPod || !rec.HubPresenceFence.Equal(fence) {
		return false, false, nil
	}
	return true, true, nil
}

func (s *stubRepo) RecordLastSeen(_ context.Context, playerID uint64, fence data.HubPresenceFence, atMs int64, _ time.Duration) (bool, int64, error) {
	current, ok := s.meta[playerID]
	if ok && !current.Equal(fence) {
		return false, 0, nil
	}
	if !ok {
		s.meta[playerID] = fence
		s.metaExists[playerID] = true
	}
	if existing, exists := s.lastSeen[playerID]; exists {
		return true, existing, nil
	}
	s.lastSeen[playerID] = atMs
	return true, atMs, nil
}

func (s *stubRepo) RefreshHubLocations(_ context.Context, hubPod string, playerIDs []uint64, _, metaTTL time.Duration) (int, error) {
	s.refreshMetaTTL = metaTTL
	refreshed := 0
	seen := map[uint64]bool{}
	for _, pid := range playerIDs {
		if pid == 0 || seen[pid] {
			continue
		}
		seen[pid] = true
		if rec, ok := s.store[pid]; ok && rec.State == LocationStateHub && rec.HubPod == hubPod {
			refreshed++
		}
	}
	return refreshed, nil
}

func TestSetLocation_InvalidInput(t *testing.T) {
	uc := NewLocatorUsecase(newStubRepo(), 30*time.Second)

	cases := []struct {
		name string
		in   LocationInput
	}{
		{"zero player_id", LocationInput{PlayerID: 0, State: LocationStateHub, HubPod: "p1"}},
		{"state out of range", LocationInput{PlayerID: 1, State: 99}},
		{"hub without pod", LocationInput{PlayerID: 1, State: LocationStateHub}},
		{"matching without match_id", LocationInput{PlayerID: 1, State: LocationStateMatching}},
		{"battle missing match_id", LocationInput{PlayerID: 1, State: LocationStateBattle, BattlePod: "bp"}},
		{"battle missing battle_pod", LocationInput{PlayerID: 1, State: LocationStateBattle, MatchID: 1001}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := uc.SetLocation(context.Background(), c.in); err == nil {
				t.Fatalf("expected error for %+v, got nil", c.in)
			}
		})
	}
}
func TestSetLocation_AndGet(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	in := LocationInput{
		PlayerID: 42,
		State:    LocationStateHub,
		HubPod:   "hub-pod-7",
		ShardID:  3,
	}
	if err := uc.SetLocation(ctx, in); err != nil {
		t.Fatalf("SetLocation failed: %v", err)
	}

	out, err := uc.GetLocation(ctx, 42)
	if err != nil {
		t.Fatalf("GetLocation failed: %v", err)
	}
	if out.State != LocationStateHub {
		t.Errorf("state mismatch: got %d, want %d", out.State, LocationStateHub)
	}
	if out.HubPod != "hub-pod-7" {
		t.Errorf("hub_pod mismatch: got %q, want %q", out.HubPod, "hub-pod-7")
	}
	if out.ShardID != 3 {
		t.Errorf("shard_id mismatch: got %d, want 3", out.ShardID)
	}
	if out.UpdatedAtMs == 0 {
		t.Errorf("updated_at_ms not set")
	}
}

func TestGetLocation_OfflineWhenMissing(t *testing.T) {
	uc := NewLocatorUsecase(newStubRepo(), 30*time.Second)
	out, err := uc.GetLocation(context.Background(), 999)
	if err != nil {
		t.Fatalf("GetLocation should not error on miss: %v", err)
	}
	if out.State != LocationStateOffline {
		t.Errorf("miss should return OFFLINE(%d), got %d", LocationStateOffline, out.State)
	}
}

func TestBatchGetLocation(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	// 42 在 HUB,7 在 BATTLE,999 不存在(应缺席)。
	if err := uc.SetLocation(ctx, LocationInput{PlayerID: 42, State: LocationStateHub, HubPod: "hub-1", ShardID: 2}); err != nil {
		t.Fatalf("SetLocation(42): %v", err)
	}
	if err := uc.SetLocation(ctx, LocationInput{PlayerID: 7, State: LocationStateBattle, MatchID: 1001, BattlePod: "bp-9"}); err != nil {
		t.Fatalf("SetLocation(7): %v", err)
	}

	// 含 0 与重复 id,验证被跳过 / 去重不报错。
	got, err := uc.BatchGetLocation(ctx, []uint64{42, 7, 999, 0, 42})
	if err != nil {
		t.Fatalf("BatchGetLocation: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 entries (42,7), got %d: %+v", len(got), got)
	}
	if _, ok := got[999]; ok {
		t.Errorf("missing player 999 should be absent (offline), but present")
	}
	if _, ok := got[0]; ok {
		t.Errorf("player_id 0 should be skipped, but present")
	}
	if got[42].State != LocationStateHub || got[42].HubPod != "hub-1" || got[42].ShardID != 2 {
		t.Errorf("player 42 mismatch: %+v", got[42])
	}
	if got[7].State != LocationStateBattle || got[7].MatchID != 1001 || got[7].BattlePod != "bp-9" {
		t.Errorf("player 7 mismatch: %+v", got[7])
	}
}

func TestBatchGetLocation_Empty(t *testing.T) {
	uc := NewLocatorUsecase(newStubRepo(), 30*time.Second)
	got, err := uc.BatchGetLocation(context.Background(), nil)
	if err != nil {
		t.Fatalf("BatchGetLocation(nil): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty input should yield empty map, got %d", len(got))
	}
}

func TestClearLocation(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 7,
		State:    LocationStateMatching,
		MatchID:  1001,
	}); err != nil {
		t.Fatalf("SetLocation failed: %v", err)
	}
	if err := uc.ClearLocation(ctx, 7); err != nil {
		t.Fatalf("ClearLocation failed: %v", err)
	}

	out, err := uc.GetLocation(ctx, 7)
	if err != nil {
		t.Fatalf("GetLocation after clear: %v", err)
	}
	if out.State != LocationStateOffline {
		t.Errorf("after clear should be OFFLINE, got state=%d", out.State)
	}
}

func TestClearLocation_InvalidPlayerID(t *testing.T) {
	uc := NewLocatorUsecase(newStubRepo(), 30*time.Second)
	err := uc.ClearLocation(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error for player_id=0, got nil")
	}
	// 确认有错误就行,具体 code 不在本测试范围
	if errors.Is(err, nil) {
		t.Fatal("err should not be nil")
	}
}

// TestRefreshHubLocations 验证 Hub DS 心跳捎带的在线保活:只有「state==HUB 且
// hub_pod 匹配」的记录被续期计数,MATCHING/BATTLE/别的 pod/不存在的玩家一律不动。
func TestRefreshHubLocations(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	// p1: HUB@hub-a → 续;p2: HUB@hub-b → 不续(别的 pod);p3: MATCHING → 不续;p4: 不存在
	mustSet := func(in LocationInput) {
		t.Helper()
		if err := uc.SetLocation(ctx, in); err != nil {
			t.Fatalf("SetLocation(%+v): %v", in, err)
		}
	}
	mustSet(LocationInput{PlayerID: 1, State: LocationStateHub, HubPod: "hub-a"})
	mustSet(LocationInput{PlayerID: 2, State: LocationStateHub, HubPod: "hub-b"})
	mustSet(LocationInput{PlayerID: 3, State: LocationStateMatching, MatchID: 900})

	refreshed, err := uc.RefreshHubLocations(ctx, "hub-a", []uint64{1, 2, 3, 4, 0, 1})
	if err != nil {
		t.Fatalf("RefreshHubLocations: %v", err)
	}
	if refreshed != 1 {
		t.Errorf("expected refreshed=1 (only p1 HUB@hub-a), got %d", refreshed)
	}
	if repo.refreshMetaTTL != defaultLastSeenRetention {
		t.Fatalf("心跳必须同时续 Hub meta retention: got=%s want=%s",
			repo.refreshMetaTTL, defaultLastSeenRetention)
	}
}

func TestRefreshHubLocations_InvalidArgs(t *testing.T) {
	uc := NewLocatorUsecase(newStubRepo(), 30*time.Second)
	ctx := context.Background()

	if _, err := uc.RefreshHubLocations(ctx, "", []uint64{1}); err == nil {
		t.Error("expected error for empty hub_pod")
	}
	if n, err := uc.RefreshHubLocations(ctx, "hub-a", nil); err != nil || n != 0 {
		t.Errorf("empty player_ids should be no-op, got n=%d err=%v", n, err)
	}
}

// TestReportDisconnect:只缩「state==HUB 且 hub_pod 匹配」的记录;
// 别的 pod / MATCHING / 不存在 → shrunk=false 且不报错(正常路径)。
func TestReportDisconnect(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()
	fence := testHubFence(1)

	repo.store[1] = data.LocationRecord{State: LocationStateHub, HubPod: "hub-a", HubPresenceFence: fence.toData()}
	repo.store[2] = data.LocationRecord{State: LocationStateHub, HubPod: "hub-b", HubPresenceFence: fence.toData()}
	repo.store[3] = data.LocationRecord{State: LocationStateMatching, MatchID: 99}

	if shrunk, err := uc.ReportDisconnect(ctx, "hub-a", 1, fence); err != nil || !shrunk {
		t.Errorf("p1 HUB@hub-a expected shrunk=true, got shrunk=%v err=%v", shrunk, err)
	}
	if shrunk, err := uc.ReportDisconnect(ctx, "hub-a", 2, fence); err != nil || shrunk {
		t.Errorf("p2 on hub-b: stale pod report must not shrink, got shrunk=%v err=%v", shrunk, err)
	}
	if shrunk, err := uc.ReportDisconnect(ctx, "hub-a", 3, fence); err != nil || shrunk {
		t.Errorf("p3 MATCHING must not shrink (travel-to-battle immunity), got shrunk=%v err=%v", shrunk, err)
	}
	if shrunk, err := uc.ReportDisconnect(ctx, "hub-a", 4, fence); err != nil || shrunk {
		t.Errorf("missing player: no-op expected, got shrunk=%v err=%v", shrunk, err)
	}
}

func TestReportDisconnect_InvalidArgs(t *testing.T) {
	uc := NewLocatorUsecase(newStubRepo(), 30*time.Second)
	ctx := context.Background()

	if _, err := uc.ReportDisconnect(ctx, "", 1, testHubFence(1)); err == nil {
		t.Error("expected error for empty hub_pod")
	}
	if _, err := uc.ReportDisconnect(ctx, "hub-a", 0, testHubFence(1)); err == nil {
		t.Error("expected error for player_id=0")
	}
}

func TestNewLocatorUsecase_DefaultTTL(t *testing.T) {
	// uc 覆盖零值配置，确认构造器回填常规 30s 默认 TTL。
	uc := NewLocatorUsecase(newStubRepo(), 0)
	if uc.ttl != 30*time.Second {
		t.Errorf("default ttl should be 30s, got %v", uc.ttl)
	}
	// uc2 覆盖非法负值，确认它与未配置走同一默认分支。
	uc2 := NewLocatorUsecase(newStubRepo(), -1)
	if uc2.ttl != 30*time.Second {
		t.Errorf("negative ttl should fall to 30s, got %v", uc2.ttl)
	}
	// 脑裂再入屏障机械下限(pkg/placement.DSFenceReentryBarrier=27s,2026-07-16;2026-07-18 余量 5→7):
	// BATTLE presence 是 login/matchmaker 再入门的第一道信号,TTL 低于屏障会在
	// 分区旧 DS 完成自我 fencing 之前放行再入,构成一人两 DS 窗口 → 机械抬回。
	// uc3 故意传入 5s，验证配置不会绕过共享的 fencing 正确性下限。
	uc3 := NewLocatorUsecase(newStubRepo(), 5*time.Second)
	if uc3.ttl != placement.DSFenceReentryBarrier {
		t.Errorf("ttl below fence barrier must be raised to %v, got %v",
			placement.DSFenceReentryBarrier, uc3.ttl)
	}
	// 屏障之上的显式配置保持原值。
	// uc4 锁定“只抬低值、不覆盖合法高值”，避免把安全门误做成固定 TTL。
	uc4 := NewLocatorUsecase(newStubRepo(), 40*time.Second)
	if uc4.ttl != 40*time.Second {
		t.Errorf("explicit ttl=40s expected, got %v", uc4.ttl)
	}
}

// --- W4 ⑩ 状态机守卫(不变量 §1) ---

// TestGuard_HubRejectedDuringMatching:玩家在 MATCHING 时,hub DS 的 HUB 上报被拒,
// 且 MATCHING 状态不被顶掉。
func TestGuard_HubRejectedDuringMatching(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	// matchmaker 写 MATCHING(控制面)
	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 100, State: LocationStateMatching, MatchID: 8888,
	}); err != nil {
		t.Fatalf("set MATCHING failed: %v", err)
	}

	// hub DS 上报 HUB(stale)→ 应被拒
	err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 100, State: LocationStateHub, HubPod: "hub-pod-2", ShardID: 1,
	})
	if err == nil {
		t.Fatal("expected ErrLocatorConflict for HUB report during MATCHING, got nil")
	}
	if got := errcode.As(err); got != errcode.ErrLocatorConflict {
		t.Errorf("expected ErrLocatorConflict(9202), got %d", got)
	}

	// MATCHING 不被顶掉
	out, _ := uc.GetLocation(ctx, 100)
	if out.State != LocationStateMatching {
		t.Errorf("MATCHING should survive stale HUB report, got state=%d", out.State)
	}
	if out.MatchID != 8888 {
		t.Errorf("match_id should remain 8888, got %d", out.MatchID)
	}
}

// TestGuard_ControlPlaneAlwaysWins:控制面写(MATCHING→BATTLE、LOGIN_PENDING 顶号)不受守卫拦截。
func TestGuard_ControlPlaneAlwaysWins(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	// MATCHING → BATTLE(matchmaker 全员确认)
	if err := uc.SetLocation(ctx, LocationInput{PlayerID: 1, State: LocationStateMatching, MatchID: 7}); err != nil {
		t.Fatalf("set MATCHING failed: %v", err)
	}
	if err := uc.SetLocation(ctx, LocationInput{PlayerID: 1, State: LocationStateBattle, MatchID: 7, BattlePod: "bp-1"}); err != nil {
		t.Fatalf("MATCHING→BATTLE should pass, got %v", err)
	}
	if out, _ := uc.GetLocation(ctx, 1); out.State != LocationStateBattle {
		t.Errorf("state should be BATTLE, got %d", out.State)
	}

	// BATTLE fence 加固(§5):LOGIN_PENDING(裸登录 / 断线重登降级)不得顶掉 active BATTLE,
	// 否则客户端反复重登会把玩家冲出战斗 → 一人两处。断言:被拒 + BATTLE 存活。
	err := uc.SetLocation(ctx, LocationInput{PlayerID: 1, State: LocationStateLoginPending})
	if err == nil {
		t.Fatal("expected ErrLocatorConflict: LOGIN_PENDING must not evict active BATTLE, got nil")
	}
	if got := errcode.As(err); got != errcode.ErrLocatorConflict {
		t.Errorf("expected ErrLocatorConflict(9202), got %d", got)
	}
	if out, _ := uc.GetLocation(ctx, 1); out.State != LocationStateBattle || out.MatchID != 7 {
		t.Errorf("BATTLE(match_id=7) should survive LOGIN_PENDING, got state=%d match_id=%d", out.State, out.MatchID)
	}
}

// TestGuard_HubAllowedFromNonMatching:HUB 上报在 OFFLINE / LOGIN_PENDING / HUB 时放行。
// （BATTLE 回流受 W4 ⑪ fence 约束，另见 TestFence_* 用例。）
func TestGuard_HubAllowedFromNonMatching(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		seed *LocationInput // nil = 不预置（OFFLINE）
	}{
		{"from offline", nil},
		{"from login_pending", &LocationInput{PlayerID: 1, State: LocationStateLoginPending}},
		{"from hub", &LocationInput{PlayerID: 1, State: LocationStateHub, HubPod: "hub-a"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			uc := NewLocatorUsecase(newStubRepo(), 30*time.Second)
			if c.seed != nil {
				if err := uc.SetLocation(ctx, *c.seed); err != nil {
					t.Fatalf("seed failed: %v", err)
				}
			}
			if err := uc.SetLocation(ctx, LocationInput{PlayerID: 1, State: LocationStateHub, HubPod: "hub-b", ShardID: 2}); err != nil {
				t.Fatalf("HUB report should pass from %s, got %v", c.name, err)
			}
			if out, _ := uc.GetLocation(ctx, 1); out.State != LocationStateHub || out.HubPod != "hub-b" {
				t.Errorf("state should be HUB(hub-b), got state=%d pod=%s", out.State, out.HubPod)
			}
		})
	}
}

// --- W4 ⑪ BATTLE fence（不变量 §1） ---

// TestFence_HubReturnFromBattleWithToken:玩家在 BATTLE（match_id=5），hub DS 携带正确
// match_id=5 的 HUB 回流上报 → 放行，切到 HUB，且不持久化 match_id/battle_pod。
func TestFence_HubReturnFromBattleWithToken(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 200, State: LocationStateBattle, MatchID: 5, BattlePod: "bp-5",
	}); err != nil {
		t.Fatalf("set BATTLE failed: %v", err)
	}

	// hub DS 回流，携带刚结束那场战斗的 fence 令牌 match_id=5
	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 200, State: LocationStateHub, HubPod: "hub-7", ShardID: 2, MatchID: 5,
	}); err != nil {
		t.Fatalf("HUB return with matching fence token should pass, got %v", err)
	}

	out, _ := uc.GetLocation(ctx, 200)
	if out.State != LocationStateHub || out.HubPod != "hub-7" {
		t.Errorf("state should be HUB(hub-7), got state=%d pod=%s", out.State, out.HubPod)
	}
	// fence 令牌不持久化：HUB 记录里 match_id/battle_pod 应被清零
	if out.MatchID != 0 || out.BattlePod != "" {
		t.Errorf("HUB record must not persist fence match_id/battle_pod, got match_id=%d battle_pod=%q", out.MatchID, out.BattlePod)
	}
}

// TestFence_StaleHubRejectedDuringBattle:玩家在 BATTLE（match_id=5），stale hub DS 不知道
// 该局，上报 HUB 携带 match_id=0 → 被拒，BATTLE 不被顶掉。
func TestFence_StaleHubRejectedDuringBattle(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 201, State: LocationStateBattle, MatchID: 5, BattlePod: "bp-5",
	}); err != nil {
		t.Fatalf("set BATTLE failed: %v", err)
	}

	err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 201, State: LocationStateHub, HubPod: "hub-stale", ShardID: 1,
	})
	if err == nil {
		t.Fatal("expected ErrLocatorConflict for stale HUB(match_id=0) during BATTLE, got nil")
	}
	if got := errcode.As(err); got != errcode.ErrLocatorConflict {
		t.Errorf("expected ErrLocatorConflict(9202), got %d", got)
	}

	out, _ := uc.GetLocation(ctx, 201)
	if out.State != LocationStateBattle || out.MatchID != 5 {
		t.Errorf("BATTLE(match_id=5) should survive stale HUB, got state=%d match_id=%d", out.State, out.MatchID)
	}
}

// TestFence_WrongMatchHubRejectedDuringBattle:玩家在 BATTLE（match_id=5），hub DS 上报
// HUB 携带错误 / 陈旧的 match_id=9 → 被拒，BATTLE 不被顶掉。
func TestFence_WrongMatchHubRejectedDuringBattle(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 202, State: LocationStateBattle, MatchID: 5, BattlePod: "bp-5",
	}); err != nil {
		t.Fatalf("set BATTLE failed: %v", err)
	}

	err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 202, State: LocationStateHub, HubPod: "hub-old", ShardID: 1, MatchID: 9,
	})
	if err == nil {
		t.Fatal("expected ErrLocatorConflict for HUB with wrong fence match_id during BATTLE, got nil")
	}
	if got := errcode.As(err); got != errcode.ErrLocatorConflict {
		t.Errorf("expected ErrLocatorConflict(9202), got %d", got)
	}

	out, _ := uc.GetLocation(ctx, 202)
	if out.State != LocationStateBattle || out.MatchID != 5 {
		t.Errorf("BATTLE(match_id=5) should survive wrong-token HUB, got state=%d match_id=%d", out.State, out.MatchID)
	}
}

// --- BATTLE fence 加固(2026-07-02,docs/design/battle-reconnect.md §5)---
// 严重 bug 修复:原守卫只拦 HUB 上报,LOGIN_PENDING(裸登录 / 断线重登降级)能无条件顶掉
// active BATTLE → matchmaker 误判空闲 → 一人两处(破 §1)。

// TestFence_LoginPendingRejectedDuringBattle:玩家在 BATTLE,login 断线重登降级写 LOGIN_PENDING
// → 被拒,BATTLE 存活。客户端反复重登也不会把玩家顶出战斗。
func TestFence_LoginPendingRejectedDuringBattle(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 300, State: LocationStateBattle, MatchID: 5, BattlePod: "bp-5",
	}); err != nil {
		t.Fatalf("set BATTLE failed: %v", err)
	}

	// 模拟客户端猛重登:多次 LOGIN_PENDING 都应被拒,BATTLE 每次都存活。
	for i := 0; i < 5; i++ {
		err := uc.SetLocation(ctx, LocationInput{PlayerID: 300, State: LocationStateLoginPending})
		if err == nil {
			t.Fatalf("attempt %d: expected ErrLocatorConflict for LOGIN_PENDING during BATTLE, got nil", i)
		}
		if got := errcode.As(err); got != errcode.ErrLocatorConflict {
			t.Errorf("attempt %d: expected ErrLocatorConflict(9202), got %d", i, got)
		}
		out, _ := uc.GetLocation(ctx, 300)
		if out.State != LocationStateBattle || out.MatchID != 5 || out.BattlePod != "bp-5" {
			t.Fatalf("attempt %d: BATTLE(match_id=5,bp-5) must survive, got state=%d match_id=%d pod=%q",
				i, out.State, out.MatchID, out.BattlePod)
		}
	}
}

// TestFence_BattleHeartbeatRefreshAllowed:ds_allocator 心跳续期(BATTLE→BATTLE 同 match)
// 在 active BATTLE 下放行,是位置续期(battle-reconnect §2.2)的前提。
func TestFence_BattleHeartbeatRefreshAllowed(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 301, State: LocationStateBattle, MatchID: 5, BattlePod: "bp-5",
	}); err != nil {
		t.Fatalf("set BATTLE failed: %v", err)
	}
	// 心跳再次写 BATTLE(同 match,续 TTL / 可换 battle_pod 地址)
	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 301, State: LocationStateBattle, MatchID: 5, BattlePod: "bp-5b",
	}); err != nil {
		t.Fatalf("BATTLE heartbeat refresh should pass, got %v", err)
	}
	if out, _ := uc.GetLocation(ctx, 301); out.State != LocationStateBattle || out.BattlePod != "bp-5b" {
		t.Errorf("state should be BATTLE(bp-5b), got state=%d pod=%q", out.State, out.BattlePod)
	}
}

// TestFence_BattleHeartbeatDifferentMatchRejected:旧对局 / 旧 allocator 的迟到心跳不得把
// 当前 active BATTLE 覆盖成另一个 match_id(否则位置指向已死旧 DS,破 §1)。
func TestFence_BattleHeartbeatDifferentMatchRejected(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 303, State: LocationStateBattle, MatchID: 5, BattlePod: "bp-5",
	}); err != nil {
		t.Fatalf("set BATTLE failed: %v", err)
	}
	err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 303, State: LocationStateBattle, MatchID: 6, BattlePod: "bp-6",
	})
	if err == nil {
		t.Fatal("expected ErrLocatorConflict for different-match BATTLE write, got nil")
	}
	if got := errcode.As(err); got != errcode.ErrLocatorConflict {
		t.Errorf("expected ErrLocatorConflict(9202), got %d", got)
	}
	if out, _ := uc.GetLocation(ctx, 303); out.State != LocationStateBattle || out.MatchID != 5 || out.BattlePod != "bp-5" {
		t.Errorf("BATTLE(match_id=5,bp-5) should survive, got state=%d match_id=%d pod=%q",
			out.State, out.MatchID, out.BattlePod)
	}
}

// TestFence_MatchingAllowedDuringBattle:BATTLE 结束后紧接下一局撮合,matchmaker 写 MATCHING
// 在 active BATTLE 下放行(对局生命周期控制面写不被 fence 拦)。
func TestFence_MatchingAllowedDuringBattle(t *testing.T) {
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	ctx := context.Background()

	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 302, State: LocationStateBattle, MatchID: 5, BattlePod: "bp-5",
	}); err != nil {
		t.Fatalf("set BATTLE failed: %v", err)
	}
	if err := uc.SetLocation(ctx, LocationInput{PlayerID: 302, State: LocationStateMatching, MatchID: 8}); err != nil {
		t.Fatalf("MATCHING during BATTLE should pass (control-plane), got %v", err)
	}
	if out, _ := uc.GetLocation(ctx, 302); out.State != LocationStateMatching || out.MatchID != 8 {
		t.Errorf("state should be MATCHING(match_id=8), got state=%d match_id=%d", out.State, out.MatchID)
	}
}

// TestFence_LoginPendingAllowedWhenNotBattle:未在战斗时(OFFLINE/HUB/MATCHING),LOGIN_PENDING
// 顶号放行——fence 只保护 active BATTLE,不误伤正常登录。
func TestFence_LoginPendingAllowedWhenNotBattle(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		seed *LocationInput
	}{
		{"from offline", nil},
		{"from hub", &LocationInput{PlayerID: 1, State: LocationStateHub, HubPod: "hub-a"}},
		{"from matching", &LocationInput{PlayerID: 1, State: LocationStateMatching, MatchID: 7}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			uc := NewLocatorUsecase(newStubRepo(), 30*time.Second)
			if c.seed != nil {
				if err := uc.SetLocation(ctx, *c.seed); err != nil {
					t.Fatalf("seed failed: %v", err)
				}
			}
			if err := uc.SetLocation(ctx, LocationInput{PlayerID: 1, State: LocationStateLoginPending}); err != nil {
				t.Fatalf("LOGIN_PENDING should pass from %s, got %v", c.name, err)
			}
			if out, _ := uc.GetLocation(ctx, 1); out.State != LocationStateLoginPending {
				t.Errorf("state should be LOGIN_PENDING, got %d", out.State)
			}
		})
	}
}

// TouchAlive 在 stub 里等价于「按节流写 last_alive」——单测不模拟节流,
// 直接覆盖即可:节流本身在 data 层的真 Redis 用例里验证。
func (s *stubRepo) TouchAlive(_ context.Context, playerID uint64, atMs int64, _ time.Duration) error {
	if s.lastAlive == nil {
		s.lastAlive = map[uint64]int64{}
	}
	s.lastAlive[playerID] = atMs
	return nil
}
