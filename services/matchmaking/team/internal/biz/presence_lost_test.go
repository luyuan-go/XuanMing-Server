// presence_lost_test.go — 离线成员「软档」:取消准备但不摘人(INC-20260813-001)。
//
// 硬档(OnPlayerOffline,满 180s 摘人)的用例在 offline_leave_test.go。
// 本文件测的是那 180s **之内**该发生什么:队员一掉线,队伍立刻掉出 READY,
// 队长根本点不动开始匹配 —— 这样才不会把一个已经关掉客户端的人冻进对局票据。
//
// 与硬档同样的纪律:重点在闸门。误动一次的后果是「队伍莫名其妙不能开局」,
// 所以「什么情况下不许动」的用例必须比「正常能动」多。
package biz

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/offlinewatch"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// readyTeam 建一支两人队并让两人都点准备,队伍进入 READY。
func readyTeam(t *testing.T, uc *TeamUsecase, teamID, captainID, memberID uint64) {
	t.Helper()
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, teamID, captainID, memberID)
	for _, pid := range []uint64{memberID, captainID} {
		if _, err := uc.SetReady(ctx, teamID, pid, true, 0); err != nil {
			t.Fatalf("SetReady(%d): %v", pid, err)
		}
	}
	team, err := uc.GetTeam(ctx, teamID)
	if err != nil || team.State != stateReady {
		t.Fatalf("前置:队伍应为 READY, state=%v err=%v", team.GetState(), err)
	}
}

func teamOf(t *testing.T, uc *TeamUsecase, teamID uint64) *teamv1.TeamStorageRecord {
	t.Helper()
	team, err := uc.GetTeam(context.Background(), teamID)
	if err != nil {
		t.Fatalf("GetTeam: %v", err)
	}
	return team
}

func memberReady(team *teamv1.TeamStorageRecord, playerID uint64) bool {
	idx := memberIndex(team.Members, playerID)
	if idx < 0 {
		return false
	}
	return team.Members[idx].Ready
}

// 事故本体:队员掉线 → 取消他的准备 + 队伍回 FORMING + **人还在队里**。
func TestOnPlayerPresenceLost_取消准备但不摘人(t *testing.T) {
	uc, pusher := newOfflineLeaveUsecase(t)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	ctx := context.Background()
	readyTeam(t, uc, 9701, 7701, 7702)

	if err := uc.OnPlayerPresenceLost(ctx, 7702, time.Now().Add(-20*time.Second).UnixMilli()); err != nil {
		t.Fatalf("OnPlayerPresenceLost: %v", err)
	}

	team := teamOf(t, uc, 9701)
	if team.State != stateForming {
		t.Fatalf("队伍必须掉出 READY,否则队长照样点得动开始匹配: state=%v", team.State)
	}
	if memberReady(team, 7702) {
		t.Fatal("掉线成员的 ready 必须被清掉")
	}
	// ★ 人必须还在队里:180s 的重连余量是刻意保留的,软档不许缩它。
	if !hasMember(team, 7702) || len(team.Members) != 2 {
		t.Fatalf("软档绝不能摘人(那是硬档 OnPlayerOffline 的职责): members=%d", len(team.Members))
	}
	if len(pusher.calls) == 0 {
		t.Fatal("状态变了必须推送,否则队长界面还显示全员已准备")
	}
}

// 幂等:离线期间每轮 Observe 都会来一次,第二次起必须零写零推送 ——
// 否则一个挂机离线的玩家会让他所在队伍每 15s 白写一次 Redis 并广播一次无意义推送。
func TestOnPlayerPresenceLost_重复调用不再写不再推(t *testing.T) {
	uc, pusher := newOfflineLeaveUsecase(t)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	ctx := context.Background()
	readyTeam(t, uc, 9702, 7711, 7712)

	if err := uc.OnPlayerPresenceLost(ctx, 7712, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	afterFirst := len(pusher.calls)
	updatedAt := teamOf(t, uc, 9702).UpdatedAtMs

	for i := 0; i < 3; i++ {
		if err := uc.OnPlayerPresenceLost(ctx, 7712, time.Now().UnixMilli()); err != nil {
			t.Fatalf("第 %d 次重复调用: %v", i+2, err)
		}
	}
	if got := len(pusher.calls); got != afterFirst {
		t.Fatalf("重复调用不得再推送: %d → %d", afterFirst, got)
	}
	if got := teamOf(t, uc, 9702).UpdatedAtMs; got != updatedAt {
		t.Fatalf("重复调用不得再写(updated_at 变了说明写回了): %d → %d", updatedAt, got)
	}
}

// ★ 与 matchmaker 的共同线性化点:组票租约在手时必须推迟,不能改 ready ——
// 否则票据里那份「全员已准备」的快照与队伍状态当场打架。
func TestOnPlayerPresenceLost_组票租约在手时推迟(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	ctx := context.Background()
	readyTeam(t, uc, 9703, 7721, 7722)

	if _, _, err := uc.BeginTeamMatch(ctx, 9703, 7721, "op-presence", 5000, false); err != nil {
		t.Fatalf("BeginTeamMatch: %v", err)
	}
	// 方案 A:Begin 已消费 ready 转 FORMING。租约在手时软化必须整体推迟,不得再叠写。
	before := teamOf(t, uc, 9703)
	if before.State != stateForming {
		t.Fatalf("前提不成立:Begin 应已消费 ready 转 FORMING, got=%v", before.State)
	}
	beforeGen := before.GetReadyGeneration()
	beforeUpdated := before.UpdatedAtMs

	err := uc.OnPlayerPresenceLost(ctx, 7722, time.Now().UnixMilli())
	if !errors.Is(err, offlinewatch.ErrDeferred) {
		t.Fatalf("租约在手必须 ErrDeferred(保留任务,租约自净后重来): %v", err)
	}
	after := teamOf(t, uc, 9703)
	if after.GetReadyGeneration() != beforeGen || after.UpdatedAtMs != beforeUpdated {
		t.Fatal("推迟时不得留下半步副作用(代际/updated_at 变了说明写回了)")
	}
}

// 单人队不动:没有队友会被拖累,取消他自己的准备毫无意义。
func TestOnPlayerPresenceLost_单人队不动(t *testing.T) {
	uc, pusher := newOfflineLeaveUsecase(t)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	ctx := context.Background()
	if _, err := uc.CreateTeam(ctx, 9704, 7731); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.SetReady(ctx, 9704, 7731, true, 0); err != nil {
		t.Fatal(err)
	}
	before := len(pusher.calls)
	if err := uc.OnPlayerPresenceLost(ctx, 7731, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if len(pusher.calls) != before {
		t.Fatal("单人队不该产生任何推送")
	}
	if !memberReady(teamOf(t, uc, 9704), 7731) {
		t.Fatal("单人队的 ready 不该被清")
	}
}

// 不在任何队伍 / 队伍已不含该成员:直接完成,不得报错(报错会一直重试到保留期结束)。
func TestOnPlayerPresenceLost_无队伍直接完成(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	if err := uc.OnPlayerPresenceLost(context.Background(), 7741, time.Now().UnixMilli()); err != nil {
		t.Fatalf("没队伍的玩家必须直接返回 nil: %v", err)
	}
}

// 功能未启用(配置关 / 依赖没注入)时整条路径不存在,行为与落地前一字不差。
func TestOnPlayerPresenceLost_未启用时零动作(t *testing.T) {
	uc, pusher := newOfflineLeaveUsecase(t)
	// 刻意不注入 matchCommitment → offlineLeaveEnabled() 为假
	ctx := context.Background()
	readyTeam(t, uc, 9705, 7751, 7752)
	before := len(pusher.calls)

	if err := uc.OnPlayerPresenceLost(ctx, 7752, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if len(pusher.calls) != before {
		t.Fatal("未启用时不得有任何推送")
	}
	if teamOf(t, uc, 9705).State != stateReady {
		t.Fatal("未启用时不得改状态")
	}
}

// 玩家重连回来重新点准备 → 队伍能正常回到 READY(软档不是单向门)。
func TestOnPlayerPresenceLost_重连后可重新准备(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	ctx := context.Background()
	readyTeam(t, uc, 9706, 7761, 7762)

	if err := uc.OnPlayerPresenceLost(ctx, 7762, time.Now().UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.SetReady(ctx, 9706, 7762, true, 0); err != nil {
		t.Fatalf("重连后重新准备: %v", err)
	}
	if got := teamOf(t, uc, 9706).State; got != stateReady {
		t.Fatalf("重新准备后应回到 READY: state=%v", got)
	}
}

// TeamUsecase 必须真的满足 offlinewatch.PresenceLostHandler —— 否则 New() 里的
// 类型断言拿不到它,整条软档路径会静默不存在(而所有业务用例照样绿)。
func TestTeamUsecase实现PresenceLostHandler(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	if _, ok := interface{}(uc).(offlinewatch.PresenceLostHandler); !ok {
		t.Fatal("TeamUsecase 必须实现 offlinewatch.PresenceLostHandler,否则骨架的类型断言会静默跳过软档")
	}
}
