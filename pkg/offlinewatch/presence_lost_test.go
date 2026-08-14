// presence_lost_test.go — 软化档回调 PresenceLostHandler(INC-20260813-001)。
//
// 这一档的价值在「早」:Threshold 那一档要等 180s 才动手,而下游在这 180s 里会把
// 「还留在队伍里」误读成「有资格被拉进对局」。因此本文件除了验回调本身,
// 更要钉死**它挂在哪条路径上** —— 挂错地方就是一个永不触发的空钩子,
// 而所有业务用例照样绿(这正是原方案差点犯的错,见 TestSweep 那条用例的注释)。
package offlinewatch

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
)

// softHandler 同时实现 Handler 与 PresenceLostHandler。
type softHandler struct {
	recordingHandler
	lost    []uint64
	lostErr error
}

func (h *softHandler) OnPlayerPresenceLost(_ context.Context, playerID uint64, sinceMs int64) error {
	if sinceMs <= 0 {
		panic(fmt.Sprintf("骨架不得用无基线的时刻回调: player=%d since=%d", playerID, sinceMs))
	}
	h.lost = append(h.lost, playerID)
	return h.lostErr
}

// 离线但未满阈值 → Observe 必须回调软化档,且**不得**触发破坏性的那一档。
func TestObserve_不在线未满阈值触发软化档(t *testing.T) {
	now := int64(1_000_000_000_000)
	h := &softHandler{}
	w, _ := newTestWatcher(t, &fakeReader{
		online:   map[uint64]bool{},
		lastSeen: map[uint64]int64{42: now - 20_000}, // 才离开 20s,阈值是 180s
	}, h, now)

	if err := w.Observe(context.Background(), []uint64{42}); err != nil {
		t.Fatalf("Observe: %v", err)
	}
	if len(h.lost) != 1 || h.lost[0] != 42 {
		t.Fatalf("未满阈值也必须回调软化档: %v", h.lost)
	}
	if len(h.seen) != 0 {
		t.Fatalf("未满阈值绝不能触发摘人那一档: %v", h.seen)
	}
}

// 在线的人一律不回调 —— 软化档同样以 locator「此刻在不在场」为第一判据。
func TestObserve_在线不触发软化档(t *testing.T) {
	now := int64(1_000_000_000_000)
	h := &softHandler{}
	w, _ := newTestWatcher(t, &fakeReader{
		online:   map[uint64]bool{42: true},
		lastSeen: map[uint64]int64{42: now - 20_000},
	}, h, now)

	if err := w.Observe(context.Background(), []uint64{42}); err != nil {
		t.Fatal(err)
	}
	if len(h.lost) != 0 {
		t.Fatalf("在线玩家不得被软化: %v", h.lost)
	}
}

// 查不到位置、也拿不到任何离开基线 = UNKNOWN。§9.22:不确定不得冒充 OFFLINE,
// 软化档同样不动作(否则 Hub DS 整台崩溃时会把全服在线玩家一起软化)。
func TestObserve_无基线判UNKNOWN不触发软化档(t *testing.T) {
	now := int64(1_000_000_000_000)
	h := &softHandler{}
	w, _ := newTestWatcher(t, &fakeReader{online: map[uint64]bool{}, lastSeen: map[uint64]int64{}}, h, now)

	if err := w.Observe(context.Background(), []uint64{42}); err != nil {
		t.Fatal(err)
	}
	if len(h.lost) != 0 {
		t.Fatalf("UNKNOWN 不得触发任何动作: %v", h.lost)
	}
}

// 软化档失败只记日志,绝不能影响本轮其余玩家的观测与排期 ——
// 破坏性那一档有自己的 due/claim/retry 闭环,不该被一个软化动作拖住。
func TestObserve_软化档失败不影响排期(t *testing.T) {
	now := int64(1_000_000_000_000)
	h := &softHandler{lostErr: errors.New("team unavailable")}
	w, _ := newTestWatcher(t, &fakeReader{
		online:   map[uint64]bool{},
		lastSeen: map[uint64]int64{42: now - 20_000},
	}, h, now)

	if err := w.Observe(context.Background(), []uint64{42}); err != nil {
		t.Fatalf("软化档失败不得让 Observe 整体失败: %v", err)
	}
	if got := evidenceMembers(t, w)[strconvU(42)]; got != now-20_000 {
		t.Fatalf("排期必须照常建立: evidence=%d", got)
	}
	if _, ok := dueMembers(t, w)[strconvU(42)]; !ok {
		t.Fatal("到期项必须照常入队")
	}
}

// ErrDeferred 是已知的业务暂缓(队伍正被组票租约占住),同样不算失败。
func TestObserve_软化档ErrDeferred不算失败(t *testing.T) {
	now := int64(1_000_000_000_000)
	h := &softHandler{lostErr: ErrDeferred}
	w, _ := newTestWatcher(t, &fakeReader{
		online:   map[uint64]bool{},
		lastSeen: map[uint64]int64{42: now - 20_000},
	}, h, now)

	if err := w.Observe(context.Background(), []uint64{42}); err != nil {
		t.Fatalf("ErrDeferred 不得让 Observe 失败: %v", err)
	}
}

// 没实现 PresenceLostHandler 的业务:整条软化路径不存在,行为与本接口落地前一字不差。
func TestObserve_未实现软化档时零影响(t *testing.T) {
	now := int64(1_000_000_000_000)
	h := &recordingHandler{} // 只实现 Handler
	w, _ := newTestWatcher(t, &fakeReader{
		online:   map[uint64]bool{},
		lastSeen: map[uint64]int64{42: now - 20_000},
	}, h, now)

	if err := w.Observe(context.Background(), []uint64{42}); err != nil {
		t.Fatalf("未实现软化档时 Observe 必须照常: %v", err)
	}
	if w.lost != nil {
		t.Fatal("类型断言不该凭空造出软化档")
	}
}

// ★ 钉死「回调该挂在哪」的结构性事实。
//
// 原方案想把软化档挂在 Sweep 的 waiting 分支上。但 upsertEvidence 把 due 排在
// `离开时刻 + Threshold`,所以一次正常离场在满阈值**之前根本不会被 Sweep 扫到** ——
// waiting 分支对新鲜离场是不可达代码,挂在那里等于挂了个永不触发的钩子,
// 而所有业务用例都会照样绿。本用例用一次真实的「离场 → 立刻 Sweep」把它证死。
func TestSweep_新鲜离场在满阈值前根本扫不到(t *testing.T) {
	now := int64(1_000_000_000_000)
	h := &softHandler{}
	w, _ := newTestWatcher(t, &fakeReader{
		online:   map[uint64]bool{},
		lastSeen: map[uint64]int64{42: now - 20_000},
	}, h, now)

	// 一条正常的 kafka 离场事件。
	if err := w.Enqueue(context.Background(), 42, now-20_000); err != nil {
		t.Fatal(err)
	}
	// due 被排在 离开时刻+180s,远在当前时刻之后。
	due := dueMembers(t, w)[strconvU(42)]
	if int64(due) <= now {
		t.Fatalf("前提变了:due 应排在未来(离开+阈值), due=%.0f now=%d", due, now)
	}
	// 立刻扫一轮:什么都扫不到,waiting 分支不可达。
	w.Sweep(context.Background())
	if len(h.lost) != 0 || len(h.seen) != 0 {
		t.Fatalf("满阈值前 Sweep 不该动任何人: lost=%v offline=%v", h.lost, h.seen)
	}

	// 而 Observe 立刻就能发现他 —— 这就是软化档必须挂在 Observe 上的原因。
	if err := w.Observe(context.Background(), []uint64{42}); err != nil {
		t.Fatal(err)
	}
	if len(h.lost) != 1 {
		t.Fatalf("Observe 必须当轮就发现他: lost=%v", h.lost)
	}
}

func strconvU(v uint64) string { return strconv.FormatUint(v, 10) }
