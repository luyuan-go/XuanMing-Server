// roster_join_deadline_test.go — 花名册到齐期限(INC-20260813-001)。
//
// 事故形状:matchmaker 把一个已经关掉客户端的队员冻进票据,DS 拿到 6 人 roster
// 却只进来 5 个人。**空场两档一个都拦不住** —— player_count 恒为 5(非 0),
// EmptySinceMs 一次都没盖过章,一场 3v2 就这么打完了,DS 的终局判定还按 roster=6 算。
//
// 本闸是最后一道兜底:前面的在线闸/软化档都在 StartMatch 那一刻判定,
// 而「判定通过 → DS 分配完成」之间还有真空(事故当天 AllocateBattle 就花了 18.4s)。
//
// 误动一次的代价极高(判弃一场正在打的对局),所以「什么情况下不许判弃」的用例
// 必须比「该判弃」的多 —— 尤其是 roster_ever_complete 那条。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/config"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// backdateRosterIncompleteBy 把 RosterIncompleteSinceMs 回拨 d,用于跨过期限。
func backdateRosterIncompleteBy(t *testing.T, repo *data.RedisBattleRepo, matchID uint64, d time.Duration) {
	t.Helper()
	if err := repo.UpdateBattleWithLock(context.Background(), matchID, 3, func(b *dsv1.BattleStorageRecord) error {
		if b.RosterIncompleteSinceMs == 0 {
			t.Fatalf("前提不成立:RosterIncompleteSinceMs 还没盖章,说明缺员根本没被识别")
		}
		b.RosterIncompleteSinceMs = time.Now().UnixMilli() - d.Milliseconds()
		return nil
	}, 2*time.Hour); err != nil {
		t.Fatalf("backdate roster_incomplete_since by %s: %v", d, err)
	}
}

// census 心跳:snapshotPresent=true + 真实在场名单。
func censusHeartbeat(t *testing.T, uc *AllocatorUsecase, matchID uint64, pod string, present []uint64) *HeartbeatResult {
	t.Helper()
	hb, err := uc.HeartbeatWithCensus(context.Background(), matchID, pod,
		int32(len(present)), "running", time.Now().UnixMilli(), true, present)
	if err != nil {
		t.Fatalf("census heartbeat: %v", err)
	}
	return hb
}

func rosterCfg(t *testing.T, deadline time.Duration) (*AllocatorUsecase, *data.RedisBattleRepo) {
	t.Helper()
	// enforce + 策略代 1:本文件大多数用例测的是判弃本体,须把激活协议翻到执行档
	// (observe 只采证的行为由 TestRosterJoinDeadline_observe档只采证不判弃 单独钉)。
	return rosterCfgMode(t, deadline, conf.RosterJoinModeEnforce, 1)
}

func rosterCfgMode(t *testing.T, deadline time.Duration, mode string, gen uint64) (*AllocatorUsecase, *data.RedisBattleRepo) {
	t.Helper()
	_, repo := newUsecase(t)
	cfg := testCfg()
	cfg.EmptyBattleTimeout = config.Duration(5 * time.Minute)
	cfg.NoShowBattleTimeout = config.Duration(150 * time.Second)
	cfg.RosterJoinDeadline = config.Duration(deadline)
	cfg.RosterJoinDeadlineMode = mode
	cfg.RosterPolicyGeneration = gen
	return NewAllocatorUsecase(repo, NewMockGameServerAllocator(cfg), cfg), repo
}

// 事故本体:6 人 roster 只到 5 个,超过期限 → 判弃 roster_incomplete。
func TestRosterJoinDeadline_缺员超期判弃(t *testing.T) {
	ctx := context.Background()
	uc, repo := rosterCfg(t, 90*time.Second)
	roster := []uint64{10, 20, 30, 40, 50, 60}
	res := allocateReady(t, uc, repo, 7, roster, 1, "5v5_ranked")

	// 只有 5 个人连进来 —— player_count=5,空场计时器**永远不会起**。
	present := roster[:5]
	censusHeartbeat(t, uc, 7, res.DSPodName, present)
	got, _, _ := repo.GetBattle(ctx, 7)
	if got.EmptySinceMs != 0 {
		t.Fatal("前提校验:有人在场时空场计时器必须是 0(正是它拦不住缺员的原因)")
	}
	if got.RosterIncompleteSinceMs == 0 {
		t.Fatal("缺员必须盖章起计时")
	}
	if got.RosterEverComplete {
		t.Fatal("从没到齐过,不得置位豁免")
	}

	backdateRosterIncompleteBy(t, repo, 7, 2*time.Minute)
	hb := censusHeartbeat(t, uc, 7, res.DSPodName, present)
	if hb.Command != commandStop {
		t.Fatalf("command = %q, want stop", hb.Command)
	}
	if got, _, _ := repo.GetBattle(ctx, 7); got.State != stateAbandoned {
		t.Fatalf("state = %q, want abandoned", got.State)
	}
}

// 期限之内不动:玩家可能还在加载地图 / travel 途中。
func TestRosterJoinDeadline_期限内不判弃(t *testing.T) {
	ctx := context.Background()
	uc, repo := rosterCfg(t, 90*time.Second)
	roster := []uint64{10, 20, 30}
	res := allocateReady(t, uc, repo, 7, roster, 1, "5v5_ranked")

	censusHeartbeat(t, uc, 7, res.DSPodName, roster[:2])
	backdateRosterIncompleteBy(t, repo, 7, 30*time.Second) // 还没到 90s
	hb := censusHeartbeat(t, uc, 7, res.DSPodName, roster[:2])
	if hb.Command == commandStop {
		t.Fatal("期限内绝不能判弃(玩家可能还在加载)")
	}
	if got, _, _ := repo.GetBattle(ctx, 7); got.State != stateRunning {
		t.Fatalf("state = %q, want still running", got.State)
	}
}

// 人补齐 → 清零并永久豁免。
func TestRosterJoinDeadline_补齐后置位豁免(t *testing.T) {
	ctx := context.Background()
	uc, repo := rosterCfg(t, 90*time.Second)
	roster := []uint64{10, 20, 30}
	res := allocateReady(t, uc, repo, 7, roster, 1, "5v5_ranked")

	censusHeartbeat(t, uc, 7, res.DSPodName, roster[:2]) // 缺一个,起计时
	censusHeartbeat(t, uc, 7, res.DSPodName, roster)     // 补齐
	got, _, _ := repo.GetBattle(ctx, 7)
	if !got.RosterEverComplete {
		t.Fatal("全员同时在场过必须置位豁免")
	}
	if got.RosterIncompleteSinceMs != 0 {
		t.Fatalf("补齐后必须清零: %d", got.RosterIncompleteSinceMs)
	}
}

// ★ 最重要的一条:到齐过之后的局中掉线**绝不能**被本闸判弃。
// 少了 roster_ever_complete 这层豁免,任何一次局中掉线都会重新起计时,
// 并在 deadline 后判弃一场正打着的对局 —— 那比本次事故严重得多。
func TestRosterJoinDeadline_到齐后局中掉线不得判弃(t *testing.T) {
	ctx := context.Background()
	uc, repo := rosterCfg(t, 90*time.Second)
	roster := []uint64{10, 20, 30}
	res := allocateReady(t, uc, repo, 7, roster, 1, "5v5_ranked")

	censusHeartbeat(t, uc, 7, res.DSPodName, roster) // 先全员到齐
	// 打到一半有人掉线,连续很多跳都少一个人。
	for i := 0; i < 5; i++ {
		censusHeartbeat(t, uc, 7, res.DSPodName, roster[:2])
	}
	got, _, _ := repo.GetBattle(ctx, 7)
	if got.RosterIncompleteSinceMs != 0 {
		t.Fatalf("到齐过的局不得再起缺员计时: since=%d", got.RosterIncompleteSinceMs)
	}
	if got.State != stateRunning {
		t.Fatalf("state = %q, want still running(局中掉线归 empty_battle_timeout 管)", got.State)
	}
}

// DS 不上报 census(legacy / local-off-v1 档)→ 整道闸跳过。
// 拿 player_count 硬猜会把每一局都判成缺员,且说不出缺的是谁。
func TestRosterJoinDeadline_无census时整道跳过(t *testing.T) {
	ctx := context.Background()
	uc, repo := rosterCfg(t, 90*time.Second)
	roster := []uint64{10, 20, 30}
	res := allocateReady(t, uc, repo, 7, roster, 1, "5v5_ranked")

	// snapshotPresent=false:老 DS 只报数量。
	for i := 0; i < 3; i++ {
		if _, err := uc.Heartbeat(ctx, 7, res.DSPodName, 2, "running", time.Now().UnixMilli()); err != nil {
			t.Fatalf("legacy heartbeat: %v", err)
		}
	}
	if got, _, _ := repo.GetBattle(ctx, 7); got.RosterIncompleteSinceMs != 0 {
		t.Fatalf("无 census 时不得盖章(否则每一局都会被判成缺员): since=%d", got.RosterIncompleteSinceMs)
	}
}

// 负值 = 显式关闭整道闸。
func TestRosterJoinDeadline_负值关闭(t *testing.T) {
	ctx := context.Background()
	uc, repo := rosterCfg(t, -1)
	roster := []uint64{10, 20, 30}
	res := allocateReady(t, uc, repo, 7, roster, 1, "5v5_ranked")

	for i := 0; i < 3; i++ {
		censusHeartbeat(t, uc, 7, res.DSPodName, roster[:2])
	}
	if got, _, _ := repo.GetBattle(ctx, 7); got.RosterIncompleteSinceMs != 0 || got.State != stateRunning {
		t.Fatalf("关闭时不得有任何动作: since=%d state=%q", got.RosterIncompleteSinceMs, got.State)
	}
}

// 缺席者点名必须准确 —— 「只罚缺席者、不罚按时到场的人」全押在这个差集上。
func TestRosterAbsentees(t *testing.T) {
	for _, tc := range []struct {
		name   string
		roster []uint64
		census []uint64
		want   []uint64
	}{
		{"全员到齐", []uint64{1, 2, 3}, []uint64{3, 1, 2}, nil},
		{"缺一个", []uint64{1, 2, 3}, []uint64{1, 3}, []uint64{2}},
		{"缺多个保持roster原序", []uint64{5, 4, 3, 2, 1}, []uint64{3}, []uint64{5, 4, 2, 1}},
		{"census为空视为全员缺席", []uint64{1, 2}, nil, []uint64{1, 2}},
		{"census含roster外的人不影响", []uint64{1, 2}, []uint64{1, 2, 99}, nil},
		{"roster为空", nil, []uint64{1}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := rosterAbsentees(tc.roster, tc.census)
			if len(got) != len(tc.want) {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got=%v want=%v", got, tc.want)
				}
			}
		})
	}
}

// ── 武装窗:保护已过窗老局（不能覆盖窗内混版接管）────────────────────────────

// ★ 这条只证明已超过武装窗的老局受保护。
//
// roster_ever_complete 是**新版**副本才写的记忆。滚动升级时:一局在**旧副本**手里已经
// 全员到齐过(没人写这个标记)→ 打到一半有人掉线 → 心跳被**新副本**接手 →
// 新副本看到 census 缺人且标记为假 → 起表 → deadline 到 → **判弃一场正在打的对局**。
//
// 过窗任何副本都不再动手；窗口内旧副本见过到齐、新副本接手时缺人的场景不在本测试覆盖内,
// 仍须用 observe → policy generation enforce 解决。这里把 allocated_at 回拨到很久以前,
// 只模拟「新副本接手一局已经过窗的老 battle」。
func TestRosterJoinDeadline_滚动升级接手老对局不得判弃(t *testing.T) {
	ctx := context.Background()
	uc, repo := rosterCfg(t, 90*time.Second)
	roster := []uint64{10, 20, 30}
	res := allocateReady(t, uc, repo, 7, roster, 1, "5v5_ranked")

	// 模拟「这局是旧副本开的,已经打了很久」:allocated_at 回拨到远超武装窗。
	backdateAllocatedBy(t, repo, 7, time.Hour)

	// 局中掉线:census 缺一个人,而 roster_ever_complete 是假(旧副本没写过)。
	for i := 0; i < 5; i++ {
		censusHeartbeat(t, uc, 7, res.DSPodName, roster[:2])
	}
	got, _, _ := repo.GetBattle(ctx, 7)
	if got.RosterIncompleteSinceMs != 0 {
		t.Fatalf("过了武装窗就不该再起表(否则滚动升级会判弃正在打的对局): since=%d", got.RosterIncompleteSinceMs)
	}
	if got.State != stateRunning {
		t.Fatalf("state = %q, want still running", got.State)
	}
}

// 反向:窗内的新局照常武装 —— 别把闸整个关死了。
func TestRosterJoinDeadline_武装窗内照常生效(t *testing.T) {
	ctx := context.Background()
	uc, repo := rosterCfg(t, 90*time.Second)
	roster := []uint64{10, 20, 30}
	res := allocateReady(t, uc, repo, 7, roster, 1, "5v5_ranked")

	censusHeartbeat(t, uc, 7, res.DSPodName, roster[:2])
	if got, _, _ := repo.GetBattle(ctx, 7); got.RosterIncompleteSinceMs == 0 {
		t.Fatal("刚分配的局在窗内,必须照常起表")
	}
}

// allocated_at 缺失(旧记录 / mock)→ 不武装。拿不到年龄就不动手,
// 漏防一局缺员局远好过误判弃一局正在打的对局。
func TestRosterGateArmable(t *testing.T) {
	const window = int64(200_000)
	const now = int64(1_000_000_000_000)
	for _, tc := range []struct {
		name        string
		allocatedAt int64
		window      int64
		want        bool
	}{
		{"刚分配", now, window, true},
		{"窗内", now - window/2, window, true},
		{"边界上", now - window, window, true},
		{"过窗", now - window - 1, window, false},
		{"远超窗(滚动升级接手老局)", now - 3_600_000, window, false},
		{"allocated_at 缺失", 0, window, false},
		{"时钟回拨致年龄为负", now + 5_000, window, false},
		{"闸本身关着", now, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := rosterGateArmable(tc.allocatedAt, now, tc.window); got != tc.want {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
		})
	}
}

// backdateAllocatedBy 把 AllocatedAtMs 回拨 d,模拟「这局已经开了很久」。
func backdateAllocatedBy(t *testing.T, repo *data.RedisBattleRepo, matchID uint64, d time.Duration) {
	t.Helper()
	if err := repo.UpdateBattleWithLock(context.Background(), matchID, 3, func(b *dsv1.BattleStorageRecord) error {
		b.AllocatedAtMs = time.Now().UnixMilli() - d.Milliseconds()
		return nil
	}, 2*time.Hour); err != nil {
		t.Fatalf("backdate allocated_at by %s: %v", d, err)
	}
}

// ── observe→enforce 激活协议(decision-revisit §5,2026-08-17 落码)──────────────
//
// 判弃执行 = mode(enforce)+ 策略代匹配 双闸;observe 只采证。误判弃一场正在打的
// 对局比缺员局打完严重得多,所以「不许判弃」的组合逐一钉死。

// observe(默认档):到点只采证,绝不判弃;事实(盖章/豁免)照常记录。
func TestRosterJoinDeadline_observe档只采证不判弃(t *testing.T) {
	ctx := context.Background()
	uc, repo := rosterCfgMode(t, 90*time.Second, conf.RosterJoinModeObserve, 0)
	roster := []uint64{10, 20, 30, 40, 50, 60}
	res := allocateReady(t, uc, repo, 41, roster, 1, "5v5_ranked")

	censusHeartbeat(t, uc, 41, res.DSPodName, roster[:5])
	got, _, _ := repo.GetBattle(ctx, 41)
	if got.RosterIncompleteSinceMs == 0 {
		t.Fatal("observe 档必须照常记录缺员事实(采证是它存在的意义)")
	}

	backdateRosterIncompleteBy(t, repo, 41, 2*time.Minute)
	censusHeartbeat(t, uc, 41, res.DSPodName, roster[:5])
	got, _, _ = repo.GetBattle(ctx, 41)
	if got.State == "abandoned" {
		t.Fatal("observe 档到点绝不判弃(只打 roster_incomplete_would_abandon)")
	}
}

// enforce 但策略代不匹配(battle 是旧代/legacy 0 代):永不判弃 —— 滚动升级窗口的
// 「旧副本见过到齐、新副本接手时恰好缺人」正是靠这条机制性豁免。
func TestRosterJoinDeadline_enforce下旧代局豁免(t *testing.T) {
	ctx := context.Background()
	// 配置代 2;下面把 battle 的代改回 1(模拟「配置代前进后仍在打的存量局」)。
	uc, repo := rosterCfgMode(t, 90*time.Second, conf.RosterJoinModeEnforce, 2)
	roster := []uint64{10, 20, 30, 40, 50, 60}
	res := allocateReady(t, uc, repo, 42, roster, 1, "5v5_ranked")
	if err := repo.UpdateBattleWithLock(ctx, 42, 3, func(b *dsv1.BattleStorageRecord) error {
		b.RosterPolicyGeneration = 1
		return nil
	}, 2*time.Hour); err != nil {
		t.Fatalf("构造旧代局: %v", err)
	}

	censusHeartbeat(t, uc, 42, res.DSPodName, roster[:5])
	backdateRosterIncompleteBy(t, repo, 42, 2*time.Minute)
	censusHeartbeat(t, uc, 42, res.DSPodName, roster[:5])
	got, _, _ := repo.GetBattle(ctx, 42)
	if got.State == "abandoned" {
		t.Fatal("代不匹配的局必须豁免(再激活不复用旧计时器的机制保证)")
	}

	// legacy(gen=0,旧副本创建)同样豁免。
	if err := repo.UpdateBattleWithLock(ctx, 42, 3, func(b *dsv1.BattleStorageRecord) error {
		b.RosterPolicyGeneration = 0
		return nil
	}, 2*time.Hour); err != nil {
		t.Fatalf("构造 legacy 局: %v", err)
	}
	censusHeartbeat(t, uc, 42, res.DSPodName, roster[:5])
	got, _, _ = repo.GetBattle(ctx, 42)
	if got.State == "abandoned" {
		t.Fatal("legacy(gen=0)局永不执行期限")
	}
}

// off 档:整道闸关死,连事实都不记。
func TestRosterJoinDeadline_off档不记事实(t *testing.T) {
	ctx := context.Background()
	uc, repo := rosterCfgMode(t, 90*time.Second, conf.RosterJoinModeOff, 0)
	roster := []uint64{10, 20, 30}
	res := allocateReady(t, uc, repo, 43, roster, 1, "5v5_ranked")

	censusHeartbeat(t, uc, 43, res.DSPodName, roster[:2])
	got, _, _ := repo.GetBattle(ctx, 43)
	if got.RosterIncompleteSinceMs != 0 || got.RosterEverComplete {
		t.Fatal("off 档不得记录任何到齐事实")
	}
}

// Allocate 冻结策略代:新局的 battle 记录必须带当前配置代。
func TestRosterJoinDeadline_分配时冻结策略代(t *testing.T) {
	ctx := context.Background()
	uc, repo := rosterCfgMode(t, 90*time.Second, conf.RosterJoinModeEnforce, 7)
	roster := []uint64{10, 20}
	allocateReady(t, uc, repo, 44, roster, 1, "5v5_ranked")
	got, _, _ := repo.GetBattle(ctx, 44)
	if got.GetRosterPolicyGeneration() != 7 {
		t.Fatalf("Allocate 必须冻结当前配置代: got=%d want=7", got.GetRosterPolicyGeneration())
	}
}

// 配置自洽:enforce + generation=0 必须启动拒(静默 no-op 配置)。
func TestRosterJoinDeadline_enforce零代配置拒(t *testing.T) {
	var c conf.Config
	c.Allocator.RosterJoinDeadlineMode = conf.RosterJoinModeEnforce
	c.Allocator.RosterPolicyGeneration = 0
	if err := c.ValidateRosterJoinDeadlineConfig(); err == nil {
		t.Fatal("enforce+generation=0 是静默 no-op,必须启动失败")
	}
	c.Allocator.RosterPolicyGeneration = 1
	if err := c.ValidateRosterJoinDeadlineConfig(); err != nil {
		t.Fatalf("enforce+generation=1 应放行: %v", err)
	}
	c.Allocator.RosterJoinDeadlineMode = "enfocre" // 手滑拼错
	if err := c.ValidateRosterJoinDeadlineConfig(); err == nil {
		t.Fatal("未知档位必须报错不猜")
	}
}
