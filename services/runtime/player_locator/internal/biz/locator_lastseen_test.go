// locator_lastseen_test.go — last-seen 时刻 + 离场事件的单测(2026-08-06)。
//
// 这两样是「按离线时长做业务决策」(组队自动退队等)唯一的权威时间来源,
// 它们的正确性全押在一条规矩上:**只有 ShrinkHubTTL 守卫通过才算真的离开了大厅**。
// 守卫没过还记时刻 / 还发事件,会让一个其实在线(travel 去战斗、切线中)的玩家
// 在下一次真离线时被算成已离线很久,提前被踢——这正是本文件要挡住的回归。
package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/services/runtime/player_locator/internal/data"
)

// recordingNotifier 记录 DepartureNotifier 收到的调用。
type recordingNotifier struct {
	calls []struct {
		playerID uint64
		leftAtMs int64
		hubPod   string
	}
	err error
}

func (n *recordingNotifier) NotifyLeftHub(_ context.Context, playerID uint64, leftAtMs int64, hubPod string) error {
	n.calls = append(n.calls, struct {
		playerID uint64
		leftAtMs int64
		hubPod   string
	}{playerID, leftAtMs, hubPod})
	return n.err
}

// newHubUsecase 造一个「玩家 42 在 hub-1 的 HUB 态」的 usecase + stub。
func newHubUsecase(t *testing.T) (*LocatorUsecase, *stubRepo, *recordingNotifier) {
	t.Helper()
	repo := newStubRepo()
	uc := NewLocatorUsecase(repo, 30*time.Second)
	notifier := &recordingNotifier{}
	uc.SetDepartureNotifier(notifier)
	if err := uc.SetLocation(context.Background(), LocationInput{
		PlayerID: 42, State: LocationStateHub, HubPod: "hub-1",
	}); err != nil {
		t.Fatalf("准备 HUB 记录失败: %v", err)
	}
	return uc, repo, notifier
}

func TestReportDisconnect_守卫通过才记时刻并发事件(t *testing.T) {
	uc, repo, notifier := newHubUsecase(t)

	before := time.Now().UnixMilli()
	shrunk, err := uc.ReportDisconnect(context.Background(), "hub-1", 42)
	if err != nil {
		t.Fatalf("ReportDisconnect 失败: %v", err)
	}
	if !shrunk {
		t.Fatal("HUB 态 + pod 匹配应当缩 TTL 成功")
	}

	ms, ok := repo.lastSeen[42]
	if !ok {
		t.Fatal("守卫通过后必须记 last-seen 时刻,否则消费方永远查到 UNKNOWN、功能静默失效")
	}
	if ms < before {
		t.Fatalf("last-seen 时刻不应早于调用时刻: got=%d before=%d", ms, before)
	}

	if len(notifier.calls) != 1 {
		t.Fatalf("应当恰好发一条离场事件, got=%d", len(notifier.calls))
	}
	c := notifier.calls[0]
	if c.playerID != 42 || c.hubPod != "hub-1" {
		t.Fatalf("事件字段不对: %+v", c)
	}
	if c.leftAtMs != ms {
		t.Fatalf("事件里的 left_at_ms 必须与写进 last-seen 的时刻同源: event=%d stored=%d", c.leftAtMs, ms)
	}
}

func TestReportDisconnect_守卫没过一律不留痕(t *testing.T) {
	// 这两种情形都是「Logout 了但人没下线 / 报文不该被信」的正常路径:
	//   - 玩家 travel 去战斗:matchmaker 已把 state 写成 MATCHING,守卫拒;
	//   - 切线后旧 pod 的迟到报文:pod 不匹配,守卫拒。
	cases := []struct {
		name     string
		setup    func(*testing.T, *LocatorUsecase)
		hubPod   string
		playerID uint64
	}{
		{
			name: "travel 去战斗(state=MATCHING)",
			setup: func(t *testing.T, uc *LocatorUsecase) {
				t.Helper()
				if err := uc.SetLocation(context.Background(), LocationInput{
					PlayerID: 42, State: LocationStateMatching, MatchID: 777,
				}); err != nil {
					t.Fatalf("切 MATCHING 失败: %v", err)
				}
			},
			hubPod: "hub-1", playerID: 42,
		},
		{
			name:   "旧 pod 的迟到断线报文",
			setup:  func(*testing.T, *LocatorUsecase) {},
			hubPod: "hub-OLD", playerID: 42,
		},
		{
			name:   "根本没有位置记录",
			setup:  func(*testing.T, *LocatorUsecase) {},
			hubPod: "hub-1", playerID: 999,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uc, repo, notifier := newHubUsecase(t)
			tc.setup(t, uc)

			shrunk, err := uc.ReportDisconnect(context.Background(), tc.hubPod, tc.playerID)
			if err != nil {
				t.Fatalf("ReportDisconnect 不应报错(守卫拒属正常路径): %v", err)
			}
			if shrunk {
				t.Fatal("守卫应当拒绝本次上报")
			}
			if _, ok := repo.lastSeen[tc.playerID]; ok {
				t.Fatal("守卫没过却记了 last-seen:会让在线玩家在下次真离线时被算成已离线很久,提前踢人")
			}
			if len(notifier.calls) != 0 {
				t.Fatalf("守卫没过不得发离场事件, got=%d", len(notifier.calls))
			}
		})
	}
}

func TestReportDisconnect_事件与时刻失败都不阻断上报(t *testing.T) {
	// 断线上报本身是尽力而为的在线态优化:kafka 抖动不能把它变成失败,
	// 否则 Hub DS 会反复重试真正重要的 TTL 收缩。
	uc, _, notifier := newHubUsecase(t)
	notifier.err = errors.New("kafka unavailable")

	shrunk, err := uc.ReportDisconnect(context.Background(), "hub-1", 42)
	if err != nil {
		t.Fatalf("事件投递失败不应让 ReportDisconnect 失败: %v", err)
	}
	if !shrunk {
		t.Fatal("TTL 收缩本身仍应成功")
	}
}

func TestSetLocation_回到Hub必须清掉上一次的离开时刻(t *testing.T) {
	// 「断线 → 秒重连」是本条最容易被漏掉的路径。留着旧时刻**当下**不会出错
	// (消费方永远先看「此刻是否在线」),但会在下一次掉线时爆:
	// Hub DS 整台挂掉 → 压根不调 ReportDisconnect → 写不出新时刻 → 消费方拿半小时前
	// 那个旧时刻算出「已离线半小时」→ 把刚掉线 10 秒的玩家立刻踢掉,180s 宽限形同虚设。
	uc, repo, _ := newHubUsecase(t)
	ctx := context.Background()

	if _, err := uc.ReportDisconnect(ctx, "hub-1", 42); err != nil {
		t.Fatalf("ReportDisconnect 失败: %v", err)
	}
	if _, ok := repo.lastSeen[42]; !ok {
		t.Fatal("前置条件不成立:这次断线应当记下时刻")
	}

	// 秒重连:PostLogin 重新写 HUB 位置。
	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 42, State: LocationStateHub, HubPod: "hub-1",
	}); err != nil {
		t.Fatalf("重连写 HUB 失败: %v", err)
	}

	if ms, ok := repo.lastSeen[42]; ok {
		t.Fatalf("回到 Hub 后必须清掉离开时刻(不变量:last-seen 存在 ⟺ 离开后没回来过), 残留=%d", ms)
	}
}

func TestSetLocation_非Hub状态不动离开时刻(t *testing.T) {
	// BATTLE 心跳链路(ds_allocator 每 5s 每人一次 SetLocation)不该为此多一次 Redis 往返;
	// 而且玩家要再次离开必然先回到 HUB,HUB 那一处已覆盖全部路径。
	uc, repo, _ := newHubUsecase(t)
	ctx := context.Background()

	if _, err := uc.ReportDisconnect(ctx, "hub-1", 42); err != nil {
		t.Fatalf("ReportDisconnect 失败: %v", err)
	}
	if err := uc.SetLocation(ctx, LocationInput{
		PlayerID: 42, State: LocationStateMatching, MatchID: 777,
	}); err != nil {
		t.Fatalf("写 MATCHING 失败: %v", err)
	}
	if _, ok := repo.lastSeen[42]; !ok {
		t.Fatal("非 HUB 状态不该动离开时刻")
	}
}

func TestBatchGetLastSeen_缺席即UNKNOWN不回填零值(t *testing.T) {
	uc, _, _ := newHubUsecase(t)
	if _, err := uc.ReportDisconnect(context.Background(), "hub-1", 42); err != nil {
		t.Fatalf("ReportDisconnect 失败: %v", err)
	}

	out, err := uc.BatchGetLastSeen(context.Background(), []uint64{42, 43, 0})
	if err != nil {
		t.Fatalf("BatchGetLastSeen 失败: %v", err)
	}
	if _, ok := out[42]; !ok {
		t.Fatal("有记录的玩家必须在结果里")
	}
	if _, ok := out[43]; ok {
		t.Fatal("无记录的玩家必须缺席(UNKNOWN),不能回填 0 —— 回填会被调用方当成「离开于纪元时刻」而立刻超时")
	}
	if _, ok := out[0]; ok {
		t.Fatal("player_id=0 必须被跳过")
	}
}

// 编译期确认 stubRepo 仍满足完整的 LocationRepo 接口(新增方法后不会悄悄漏实现)。
var _ data.LocationRepo = (*stubRepo)(nil)
