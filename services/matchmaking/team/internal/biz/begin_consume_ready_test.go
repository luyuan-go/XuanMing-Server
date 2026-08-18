// begin_consume_ready_test.go — BeginTeamMatch 冻结名单 + 清 ready 的契约。
//
// 2026-08-18 起 ready 是不是开局门槛**按关卡表 ready_mode 分图决定**,由 requireReady 入参承载:
//   - requireReady=false(POST_CONFIRM,含表留空与旧 matchmaker 不发该字段):FORMING/READY 都放行,
//     「带缺席者开局」由 matchmaker 在线闸(4011)+ 撮合确认期(CONFIRM)兜住;
//   - requireReady=true (PRE_READY):队伍必须 State==READY 才放行。
//
// 本文件守的契约:两种模式各自的放行 / 拒绝、响应丢失重试不重复冻结、代际前进后不误判重入、
// 冻结仍清 ready 并盖收据,以及**门槛必须排在租约冲突判定之后**(否则并发重试会被误报成未准备)。
package biz

import (
	"context"
	"testing"
	"time"

	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// 消费语义本体:Begin 成功后 ready 清空、队伍转 FORMING、收据记下消费前后两代。
func TestBeginTeamMatch_消费ready并开收据(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9901, 8101, 8102)
	genBefore := teamOf(t, uc, 9901).GetReadyGeneration()

	snapshot, _, err := uc.BeginTeamMatch(ctx, 9901, 8101, "op-consume", 5000, false)
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

	first, _, err := uc.BeginTeamMatch(ctx, 9902, 8111, "op-retry", 5000, false)
	if err != nil {
		t.Fatalf("首次 Begin: %v", err)
	}
	genAfterFirst := teamOf(t, uc, 9902).GetReadyGeneration()

	retry, _, err := uc.BeginTeamMatch(ctx, 9902, 8111, "op-retry", 5000, false)
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

	if _, _, err := uc.BeginTeamMatch(ctx, 9903, 8121, "op-stable", 2000, false); err != nil {
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
	second, _, err := uc.BeginTeamMatch(ctx, 9903, 8121, "op-stable", 5000, false)
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

// LoL 式:冻结后无人重按,同队长换 attempt 也能直接开第二局(FORMING 放行)。
// 「带缺席者开局」不再靠 ready 门槛,由 matchmaker 在线闸 + 确认期兜住。
func TestBeginTeamMatch_冻结后不重按也能开第二局(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9904, 8131, 8132)

	if _, _, err := uc.BeginTeamMatch(ctx, 9904, 8131, "op-one", 2000, false); err != nil {
		t.Fatalf("首局 Begin: %v", err)
	}
	waitRosterLockExpired()

	second, _, err := uc.BeginTeamMatch(ctx, 9904, 8131, "op-two", 5000, false)
	if err != nil {
		t.Fatalf("FORMING 队伍必须放行(队长直接再开局): %v", err)
	}
	if len(second.Members) != 2 {
		t.Fatalf("第二局必须拿到当前名单: members=%d", len(second.Members))
	}
	// 换 attempt 走的是正常冻结路径:收据必须盖成新 attempt,不能把 op-one 的旧收据留在场上。
	if got := teamOf(t, uc, 9904).GetMatchStartReceipt().GetAttemptId(); got != "op-two" {
		t.Fatalf("收据应盖成新 attempt: %s", got)
	}
}

// 主路径:从未有人点准备的 FORMING 队伍,队长直接开局成功(LoL 式流程本体)。
func TestBeginTeamMatch_FORMING队伍直接开局(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9908, 8171, 8172)

	snapshot, _, err := uc.BeginTeamMatch(ctx, 9908, 8171, "op-forming", 5000, false)
	if err != nil {
		t.Fatalf("FORMING 队伍直接开局必须成功: %v", err)
	}
	if len(snapshot.Members) != 2 {
		t.Fatalf("冻结名单必须是当前全员: members=%d", len(snapshot.Members))
	}
	team := teamOf(t, uc, 9908)
	if team.GetMatchStartReceipt() == nil {
		t.Fatal("冻结必须留下收据")
	}
	if team.State != stateForming {
		t.Fatalf("冻结后队伍保持 FORMING: state=%v", team.State)
	}
}

// legacy 记录(代际=0 的 READY)不再作废:ready 不授权任何东西,按正常冻结放行。
func TestBeginTeamMatch_legacy零代际READY直接放行(t *testing.T) {
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

	snapshot, _, err := uc.BeginTeamMatch(ctx, 9905, 8141, "op-legacy", 5000, false)
	if err != nil {
		t.Fatalf("legacy READY 应按正常冻结放行: %v", err)
	}
	if len(snapshot.Members) != 2 {
		t.Fatalf("冻结名单必须是当前全员: members=%d", len(snapshot.Members))
	}
	team := teamOf(t, uc, 9905)
	if team.State != stateForming || readyCount(team.Members) != 0 {
		t.Fatalf("冻结后残留 ready 应被清空: state=%v ready=%d", team.State, readyCount(team.Members))
	}
}

// 新链路全程:Begin 消费 → matchmaker 拿消费前代际调 End → CAS 落空 no-op(不产生新写)。
func TestBeginTeamMatch_End在新链路下是no_op(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9906, 8151, 8152)

	snapshot, _, err := uc.BeginTeamMatch(ctx, 9906, 8151, "op-endcompat", 2000, false)
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

// ── PRE_READY 模式(关卡表 ready_mode=1,requireReady=true) ────────────────────

// 门槛本体:PRE_READY 图上,没全员准备的队伍开不了局,且**一个字节都不能改**。
//
// 「零副作用」和「被拒」同等重要:如果被拒时已经把租约上掉或把 ready 清了,
// 队长重新点准备再开会撞上自己刚才留下的租约,表现是「点了没反应」。
func TestBeginTeamMatch_PRE_READY未准备被拒且零副作用(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9910, 8181, 8182)
	genBefore := teamOf(t, uc, 9910).GetReadyGeneration()

	_, _, err := uc.BeginTeamMatch(ctx, 9910, 8181, "op-pre-noready", 5000, true)
	if err == nil {
		t.Fatal("PRE_READY 图上未准备的队伍必须被拒")
	}
	if got := errcode.As(err); got != errcode.ErrTeamWrongState {
		t.Fatalf("拒绝码应为 ErrTeamWrongState,得 %v", got)
	}
	team := teamOf(t, uc, 9910)
	if team.GetMatchStartReceipt() != nil {
		t.Fatal("被拒不得留下收据")
	}
	if team.GetMatchLockUntilMs() != 0 {
		t.Fatalf("被拒不得上租约: lock_until=%d", team.GetMatchLockUntilMs())
	}
	if team.GetReadyGeneration() != genBefore {
		t.Fatalf("被拒不得推进代际: got=%d want=%d", team.GetReadyGeneration(), genBefore)
	}
	if team.State != stateForming {
		t.Fatalf("被拒不得改状态: state=%v", team.State)
	}
}

// PRE_READY 放行路径:全员准备后开局成功,且 ready 仍被消费掉
// (一次准备只授权一次开局 —— 不消费就等于队长能拿同一次准备连开两局,正是 INC-20260813-001 的形状)。
func TestBeginTeamMatch_PRE_READY全员准备后放行并消费(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9911, 8191, 8192)
	genBefore := teamOf(t, uc, 9911).GetReadyGeneration()

	snapshot, _, err := uc.BeginTeamMatch(ctx, 9911, 8191, "op-pre-ok", 5000, true)
	if err != nil {
		t.Fatalf("PRE_READY 图上全员准备后必须放行: %v", err)
	}
	// 交给 matchmaker 建票的仍是**消费前**名单(全员 ready)。
	if snapshot.State != stateReady || readyCount(snapshot.Members) != 2 {
		t.Fatalf("快照必须是消费前名单: state=%v ready=%d", snapshot.State, readyCount(snapshot.Members))
	}
	team := teamOf(t, uc, 9911)
	if team.State != stateForming || readyCount(team.Members) != 0 {
		t.Fatalf("放行后必须消费 ready: state=%v ready=%d", team.State, readyCount(team.Members))
	}
	if team.GetReadyGeneration() != genBefore+1 {
		t.Fatalf("消费必须推进代际: got=%d want=%d", team.GetReadyGeneration(), genBefore+1)
	}
	// 消费之后队伍已是 FORMING:同一次准备再开第二局必须被门槛拦下。
	if _, _, err := uc.BeginTeamMatch(ctx, 9911, 8191, "op-pre-second", 5000, true); err == nil {
		t.Fatal("同一次准备不得授权第二局(消费后应回到未准备,被门槛拦下)")
	}
}

// 判定顺序守护:门槛必须排在**租约冲突之后**。
//
// 走到门槛时队伍常常刚被上一次冻结转成 FORMING;若先判 READY,一个「几秒后租约自净、
// 重试即可」的暂态会被说成「队伍未准备」这种终态,客户端据此引导玩家重新点准备 ——
// 而其实什么都不用做。本例锁死这个顺序:必须拿到 3007(并发),不是 wrong-state。
func TestBeginTeamMatch_PRE_READY门槛排在租约冲突之后(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	readyTeam(t, uc, 9912, 8201, 8202)

	// 第一次冻结:成功,队伍转 FORMING 并持有租约。
	if _, _, err := uc.BeginTeamMatch(ctx, 9912, 8201, "op-hold", 5000, true); err != nil {
		t.Fatalf("首次冻结应成功: %v", err)
	}
	// 另一个 attempt 撞上未过期租约:此刻队伍恰好也不是 READY,两个条件同时成立。
	_, _, err := uc.BeginTeamMatch(ctx, 9912, 8201, "op-other", 5000, true)
	if err == nil {
		t.Fatal("租约未过期时另一 attempt 必须被拒")
	}
	if got := errcode.As(err); got != errcode.ErrTeamConcurrent {
		t.Fatalf("必须报并发冲突(可重试暂态)而非未准备(终态): got=%v", got)
	}
}

// 同一支队伍在两种模式下的差别只有门槛:POST_CONFIRM 放行、PRE_READY 拒绝。
// 这条把「模式选择器真的生效」钉死 —— 只改 requireReady 一个入参,结果必须相反。
func TestBeginTeamMatch_同一队伍两模式结果相反(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9913, 8211, 8212)

	if _, _, err := uc.BeginTeamMatch(ctx, 9913, 8211, "op-post", 5000, false); err != nil {
		t.Fatalf("POST_CONFIRM 图上未准备也应放行: %v", err)
	}

	setupTwoMemberTeam(t, uc, 9914, 8221, 8222)
	if _, _, err := uc.BeginTeamMatch(ctx, 9914, 8221, "op-pre", 5000, true); err == nil {
		t.Fatal("PRE_READY 图上未准备必须被拒(同一支队伍、只差模式)")
	}
}
