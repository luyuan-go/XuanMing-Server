// begin_consume_ready_test.go — BeginTeamMatch 一次性消费 ready 的契约(方案 A)。
//
// 对应 decision-revisit-team-match-lifecycle-and-roster-rollout.md §7 验收标准 1/2/4:
// 响应丢失重试不重复消费、代际前进后不误判重入、旧 ready 不能授权第二局。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// 消费语义本体:Begin 成功后 ready 清空、队伍转 FORMING、收据记下消费前后两代。
func TestBeginTeamMatch_消费ready并开收据(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9901, 8101, 8102)
	genBefore := teamOf(t, uc, 9901).GetReadyGeneration()

	snapshot, _, err := uc.BeginTeamMatch(ctx, 9901, 8101, "op-consume", 5000)
	if err != nil {
		t.Fatalf("BeginTeamMatch: %v", err)
	}
	// 返回给 matchmaker 的必须是**消费前**的名单:全员 ready、状态 READY。
	if snapshot.State != stateReady || readyCount(snapshot.Members) != 2 {
		t.Fatalf("快照必须是消费前的名单: state=%v ready=%d", snapshot.State, readyCount(snapshot.Members))
	}
	if snapshot.GetReadyGeneration() != genBefore {
		t.Fatalf("快照代际应为消费前那一代: got=%d want=%d", snapshot.GetReadyGeneration(), genBefore)
	}
	// 存储里的队伍必须已经消费:ready 清空、FORMING、代际推进、收据在位。
	team := teamOf(t, uc, 9901)
	if team.State != stateForming || readyCount(team.Members) != 0 {
		t.Fatalf("消费后队伍应为 FORMING 且无人 ready: state=%v ready=%d", team.State, readyCount(team.Members))
	}
	if team.GetReadyGeneration() != genBefore+1 {
		t.Fatalf("消费必须推进代际: got=%d want=%d", team.GetReadyGeneration(), genBefore+1)
	}
	rec := team.GetMatchStartReceipt()
	if rec == nil {
		t.Fatal("消费必须留下收据")
	}
	if rec.GetAttemptId() != "op-consume" ||
		rec.GetConsumedReadyGeneration() != genBefore ||
		rec.GetPostReadyGeneration() != genBefore+1 ||
		len(rec.GetRoster()) != 2 {
		t.Fatalf("收据内容不对: %+v", rec)
	}
}

// §7.1 响应丢失重试:同 attempt 重入拿回同一份消费前名单,不再次消费。
func TestBeginTeamMatch_同attempt重试返回收据快照(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9902, 8111, 8112)

	first, _, err := uc.BeginTeamMatch(ctx, 9902, 8111, "op-retry", 5000)
	if err != nil {
		t.Fatalf("首次 Begin: %v", err)
	}
	genAfterFirst := teamOf(t, uc, 9902).GetReadyGeneration()

	retry, _, err := uc.BeginTeamMatch(ctx, 9902, 8111, "op-retry", 5000)
	if err != nil {
		t.Fatalf("同 attempt 重试必须成功(响应丢失后的重发): %v", err)
	}
	if retry.State != stateReady || readyCount(retry.Members) != 2 {
		t.Fatalf("重试必须拿回消费前名单: state=%v ready=%d", retry.State, readyCount(retry.Members))
	}
	if retry.GetReadyGeneration() != first.GetReadyGeneration() {
		t.Fatalf("重试快照代际应与首次一致: %d != %d", retry.GetReadyGeneration(), first.GetReadyGeneration())
	}
	// 重入不得再次消费:代际不再推进。
	if got := teamOf(t, uc, 9902).GetReadyGeneration(); got != genAfterFirst {
		t.Fatalf("重入不得再次消费(代际又动了): %d → %d", genAfterFirst, got)
	}
}

// §7.2 跨局区分:打完一局全队重新点准备后,同一个 operation_id 的下一次 Begin
// **不是**重入 —— 代际已前进,必须重新消费新一代,绝不能拿回上一局的旧名单。
func TestBeginTeamMatch_重新ready后同attempt不是重入(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9903, 8121, 8122)

	if _, _, err := uc.BeginTeamMatch(ctx, 9903, 8121, "op-stable", 2000); err != nil {
		t.Fatalf("第一局 Begin: %v", err)
	}
	waitRosterLockExpired()

	// 全队重新点准备(= 新一轮真实意图,代际前进)。
	for _, pid := range []uint64{8121, 8122} {
		if _, err := uc.SetReady(ctx, 9903, pid, true, 0); err != nil {
			t.Fatalf("重新 SetReady(%d): %v", pid, err)
		}
	}
	genBeforeSecond := teamOf(t, uc, 9903).GetReadyGeneration()

	// matchmaker 的 operation_id 按 (team, captain) 派生,第二局还是同一个值。
	second, _, err := uc.BeginTeamMatch(ctx, 9903, 8121, "op-stable", 5000)
	if err != nil {
		t.Fatalf("第二局 Begin: %v", err)
	}
	if second.GetReadyGeneration() != genBeforeSecond {
		t.Fatalf("第二局必须消费**新**一代(拿回旧名单=事故): got=%d want=%d",
			second.GetReadyGeneration(), genBeforeSecond)
	}
	if got := teamOf(t, uc, 9903).GetReadyGeneration(); got != genBeforeSecond+1 {
		t.Fatalf("第二局必须真的消费(代际推进): got=%d want=%d", got, genBeforeSecond+1)
	}
}

// §7.4 消费后的 ready 不能再授权一局:不重新点准备,同队长换 attempt 也开不出第二局。
func TestBeginTeamMatch_消费后不重按开不出第二局(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9904, 8131, 8132)

	if _, _, err := uc.BeginTeamMatch(ctx, 9904, 8131, "op-one", 2000); err != nil {
		t.Fatalf("首局 Begin: %v", err)
	}
	waitRosterLockExpired()

	_, _, err := uc.BeginTeamMatch(ctx, 9904, 8131, "op-two", 5000)
	if err == nil {
		t.Fatal("ready 已消费且无人重按,第二局必须被拒")
	}
	if errcode.As(err) != errcode.ErrTeamWrongState {
		t.Fatalf("应为 ErrTeamWrongState(需重新点准备): %v", err)
	}
}

// legacy 迁移:升级前写下的 READY(代际=0)不能授权开局,必须被保守作废并要求重按。
func TestBeginTeamMatch_legacy零代际READY作废(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9905, 8141, 8142)

	// 模拟旧版记录:直接把代际抹回 0(旧版没有代际字段,读出来就是 0)。
	// 走 repo 原始写以绕过 updateTeam 的代际推进 —— 正是旧版的行为。
	if err := uc.repo.UpdateWithLock(ctx, 9905, uc.cfg.OptimisticRetry, func(team *teamv1.TeamStorageRecord) error {
		team.ReadyGeneration = 0
		return nil
	}, time.Hour); err != nil {
		t.Fatalf("构造 legacy 记录: %v", err)
	}

	_, _, err := uc.BeginTeamMatch(ctx, 9905, 8141, "op-legacy", 5000)
	if err == nil {
		t.Fatal("legacy READY 必须被作废而不是放行")
	}
	if errcode.As(err) != errcode.ErrTeamWrongState {
		t.Fatalf("应为 ErrTeamWrongState: %v", err)
	}
	team := teamOf(t, uc, 9905)
	if team.State != stateForming || readyCount(team.Members) != 0 {
		t.Fatalf("legacy READY 应被清空: state=%v ready=%d", team.State, readyCount(team.Members))
	}
}

// 新链路全程:Begin 消费 → matchmaker 拿消费前代际调 End → CAS 落空 no-op(不产生新写)。
func TestBeginTeamMatch_End在新链路下是no_op(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9906, 8151, 8152)

	snapshot, _, err := uc.BeginTeamMatch(ctx, 9906, 8151, "op-endcompat", 2000)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	waitRosterLockExpired()
	afterBegin := teamOf(t, uc, 9906)

	// ReleaseMatch 会带着 Begin 返回的(消费前)代际调 End —— 新 team 下必须 no-op。
	if err := uc.EndTeamMatch(ctx, 9906, []uint64{8151, 8152}, snapshot.GetReadyGeneration()); err != nil {
		t.Fatalf("End 必须幂等成功: %v", err)
	}
	afterEnd := teamOf(t, uc, 9906)
	if afterEnd.GetReadyGeneration() != afterBegin.GetReadyGeneration() ||
		afterEnd.UpdatedAtMs != afterBegin.UpdatedAtMs {
		t.Fatal("End 在新链路下应零写(代际 CAS 落空)")
	}
}

// receiptReentry 纯函数边界:重入窗过期后不再当重试。
func TestReceiptReentry_窗口边界(t *testing.T) {
	now := time.Now().UnixMilli()
	rec := &teamv1.MatchStartReceipt{
		AttemptId:               "op-x",
		PostReadyGeneration:     7,
		ConsumedReadyGeneration: 6,
		CreatedAtMs:             now,
	}
	if !receiptReentry(rec, "op-x", 7, now+matchStartReceiptWindow.Milliseconds()) {
		t.Fatal("窗内应判重入")
	}
	if receiptReentry(rec, "op-x", 7, now+matchStartReceiptWindow.Milliseconds()+1) {
		t.Fatal("过窗必须拒判重入(防久后的真实点击拿回旧名单)")
	}
	if receiptReentry(rec, "op-x", 8, now) {
		t.Fatal("代际前进后必须拒判重入")
	}
	if receiptReentry(rec, "op-y", 7, now) {
		t.Fatal("不同 attempt 必须拒判重入")
	}
	if receiptReentry(nil, "op-x", 7, now) {
		t.Fatal("无收据必须拒判重入")
	}
	// 时钟回拨(now < created):按在窗内处理,方向是放宽重试窗,安全。
	if !receiptReentry(rec, "op-x", 7, now-5000) {
		t.Fatal("时钟回拨应按窗内处理")
	}
}
