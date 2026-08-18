// end_team_match_test.go — 对局结束复位残留 ready(源自 INC-20260813-001 v2)。
//
// 2026-08-17 起 ready 不再是开局门槛(LoL 式流程):本路径的职责收窄为
// 「清残留 ready(存量客户端显示)+ 滚动升级共存窗口的赛后复位」,
// 幂等 / 跨代 CAS / 租约推迟等契约不变。
package biz

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// 事故本体:一局结束 → 全员 ready 清掉 + 队伍回 FORMING，队长点不动开始匹配。
func TestEndTeamMatch_结算后复位准备(t *testing.T) {
	uc, pusher := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9801, 7801, 7802)

	if err := uc.EndTeamMatch(ctx, 9801, []uint64{7801, 7802}, 0); err != nil {
		t.Fatalf("EndTeamMatch: %v", err)
	}

	team := teamOf(t, uc, 9801)
	if team.State != stateForming {
		t.Fatalf("打完一局队伍必须回 FORMING(清残留 ready + 回到招募列表): state=%v", team.State)
	}
	for _, pid := range []uint64{7801, 7802} {
		if memberReady(team, pid) {
			t.Fatalf("成员 %d 的 ready 必须被清掉", pid)
		}
	}
	// 人一个都不能少 —— 复位准备不是解散队伍。
	if len(team.Members) != 2 {
		t.Fatalf("复位不得动成员: members=%d", len(team.Members))
	}
	if len(pusher.calls) == 0 {
		t.Fatal("状态变了必须推送,否则队长界面还显示全员已准备")
	}
}

// 2026-08-17 起 ready 不再是开局门槛:复位后队长可**直接**再开局(LoL 式,不用重按准备)。
// 「带缺席者开局」由 matchmaker 在线闸(4011 点名)+ 撮合确认期(CONFIRM 超时判过错)兜住。
func TestEndTeamMatch_复位后队长可直接再开局(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9802, 7811, 7812)

	if err := uc.EndTeamMatch(ctx, 9802, []uint64{7811, 7812}, 0); err != nil {
		t.Fatal(err)
	}
	snapshot, _, err := uc.BeginTeamMatch(ctx, 9802, 7811, "op-next", 5000, false)
	if err != nil {
		t.Fatalf("复位后(FORMING)组票必须放行: %v", err)
	}
	if len(snapshot.Members) != 2 {
		t.Fatalf("组票名单必须是当前全员: members=%d", len(snapshot.Members))
	}
}

// 幂等:battle_result 的 outbox 会重投到成功为止,第二次起必须零写零推送。
func TestEndTeamMatch_重复调用幂等(t *testing.T) {
	uc, pusher := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9803, 7821, 7822)

	if err := uc.EndTeamMatch(ctx, 9803, []uint64{7821, 7822}, 0); err != nil {
		t.Fatal(err)
	}
	afterFirst := len(pusher.calls)
	updatedAt := teamOf(t, uc, 9803).UpdatedAtMs

	for i := 0; i < 3; i++ {
		if err := uc.EndTeamMatch(ctx, 9803, []uint64{7821, 7822}, 0); err != nil {
			t.Fatalf("第 %d 次重投: %v", i+2, err)
		}
	}
	if got := len(pusher.calls); got != afterFirst {
		t.Fatalf("重投不得再推送: %d → %d", afterFirst, got)
	}
	if got := teamOf(t, uc, 9803).UpdatedAtMs; got != updatedAt {
		t.Fatalf("重投不得再写: %d → %d", updatedAt, got)
	}
}

// 队伍已不存在 = 本就没什么可复位的,必须返回成功 —— 报错会让 outbox 永远重试。
func TestEndTeamMatch_队伍已不存在返回成功(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	if err := uc.EndTeamMatch(context.Background(), 9899, []uint64{7831}, 0); err != nil {
		t.Fatalf("队伍不存在必须幂等成功,否则 outbox 空转: %v", err)
	}
}

// 对局期间新加入、自己点过准备的人不该被这一局的结束复位 ——
// 他没参与这一局，凭什么被它清掉。
func TestEndTeamMatch_只复位本局成员(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9804, 7841, 7842)
	// 第三个人在对局期间加入并点了准备。
	if _, err := uc.AcceptInvite(ctx, 0, 9804, 7843); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}
	if _, err := uc.SetReady(ctx, 9804, 7843, true, 0); err != nil {
		t.Fatalf("SetReady: %v", err)
	}

	// 只复位打了这一局的两个人。
	if err := uc.EndTeamMatch(ctx, 9804, []uint64{7841, 7842}, 0); err != nil {
		t.Fatal(err)
	}
	team := teamOf(t, uc, 9804)
	if memberReady(team, 7841) || memberReady(team, 7842) {
		t.Fatal("本局成员必须被复位")
	}
	if !memberReady(team, 7843) {
		t.Fatal("对局期间才加入的成员不该被这一局的结束复位")
	}
	if team.State != stateForming {
		t.Fatalf("只要有人被复位,队伍就不再是全员已准备: state=%v", team.State)
	}
}

// 名单留空 = 复位全队(调用方拿不到 roster 时的兜底)。
func TestEndTeamMatch_名单留空复位全队(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9805, 7851, 7852)

	if err := uc.EndTeamMatch(ctx, 9805, nil, 0); err != nil {
		t.Fatal(err)
	}
	team := teamOf(t, uc, 9805)
	if memberReady(team, 7851) || memberReady(team, 7852) || team.State != stateForming {
		t.Fatalf("留空应复位全队: state=%v", team.State)
	}
}

// 队长已经开了下一局(组票租约在手)→ 返回可重试错误,不得改 ready ——
// 那会与那一局已冻结的名单快照打架。
func TestEndTeamMatch_组票租约在手时可重试(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9806, 7861, 7862)
	if _, _, err := uc.BeginTeamMatch(ctx, 9806, 7861, "op-next", 5000, false); err != nil {
		t.Fatalf("BeginTeamMatch: %v", err)
	}

	// 方案 A:Begin 已在同一把锁内消费 ready 并转 FORMING —— 那是 Begin 自己的合法写,
	// 不是「半步副作用」。本测试守的是:租约在手时 End 必须整体推迟,**不得再叠加任何写**。
	before := teamOf(t, uc, 9806)
	if before.State != stateForming {
		t.Fatalf("前提不成立:Begin 应已消费 ready 转 FORMING, got=%v", before.State)
	}
	beforeGen := before.GetReadyGeneration()
	beforeUpdated := before.UpdatedAtMs

	err := uc.EndTeamMatch(ctx, 9806, []uint64{7861, 7862}, 0)
	if errcode.As(err) != errcode.ErrTeamConcurrent {
		t.Fatalf("租约在手必须返回可重试错误: err=%v code=%d", err, errcode.As(err))
	}
	after := teamOf(t, uc, 9806)
	if after.GetReadyGeneration() != beforeGen || after.UpdatedAtMs != beforeUpdated {
		t.Fatal("推迟时不得留下半步副作用(代际/updated_at 变了说明写回了)")
	}
}

// team_id=0 是调用方 bug,必须显式拒而不是静默成功。
func TestEndTeamMatch_零队伍编号拒绝(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	if errcode.As(uc.EndTeamMatch(context.Background(), 0, nil, 0)) != errcode.ErrInvalidArg {
		t.Fatal("team_id=0 必须报 ErrInvalidArg")
	}
}

// 单人队同样要复位:一个人打完 PVE 回大厅,不该自动排进下一局。
func TestEndTeamMatch_单人队也复位(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	if _, err := uc.CreateTeam(ctx, 9807, 7871); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.SetReady(ctx, 9807, 7871, true, 0); err != nil {
		t.Fatal(err)
	}
	if err := uc.EndTeamMatch(ctx, 9807, []uint64{7871}, 0); err != nil {
		t.Fatal(err)
	}
	if memberReady(teamOf(t, uc, 9807), 7871) {
		t.Fatal("单人队打完一局同样必须重新点准备")
	}
}
