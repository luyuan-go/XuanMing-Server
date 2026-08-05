package biz

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/data"
)

// ── 测试基础设施 ──────────────────────────────────────────────────────────────

// mockPusher 记录 PushTeamUpdate 的调用参数。
type mockPusher struct {
	calls []pushCall
}

type pushCall struct {
	// 字段含义:
	// - caller 记录本次推送应排除的发起玩家;
	// - to 记录本次推送的目标玩家集合;
	// - eventType 记录客户端选择 payload 类型所用的域内判别值。
	caller    uint64
	to        []uint64
	eventType uint32
}

func (m *mockPusher) PushTeamUpdate(_ context.Context, callerPlayerID uint64, toPlayerIDs []uint64, _ []byte) (int, error) {
	m.calls = append(m.calls, pushCall{caller: callerPlayerID, to: toPlayerIDs})
	return len(toPlayerIDs), nil
}

// PushTeamEvent 记录带域内事件类型的推送,供测试核对 dedicated 与 legacy 是否都发送。
func (m *mockPusher) PushTeamEvent(_ context.Context, callerPlayerID uint64, toPlayerIDs []uint64, _ []byte, eventType uint32) (int, error) {
	// calls 保存完整调用顺序和参数,eventType 用于区分独立邀请事件与旧队伍快照。
	m.calls = append(m.calls, pushCall{caller: callerPlayerID, to: toPlayerIDs, eventType: eventType})
	// mock 把目标数量视为成功发送数,不在本测试模拟传输失败。
	return len(toPlayerIDs), nil
}

func newTestUsecase(t *testing.T) (*TeamUsecase, *mockPusher, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	repo := data.NewRedisTeamRepo(rdb)
	pusher := &mockPusher{}

	cfg := conf.TeamConf{}
	cfg2 := conf.Config{}
	cfg2.Team = cfg
	cfg2.Defaults()

	uc := NewTeamUsecase(repo, pusher, cfg2.Team)
	cleanup := func() {
		_ = rdb.Close()
		mr.Close()
	}
	return uc, pusher, cleanup
}

func newTestUsecaseWithMR(t *testing.T) (*TeamUsecase, *mockPusher, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	repo := data.NewRedisTeamRepo(rdb)
	pusher := &mockPusher{}

	cfg2 := conf.Config{}
	cfg2.Defaults()
	uc := NewTeamUsecase(repo, pusher, cfg2.Team)
	t.Cleanup(func() {
		_ = rdb.Close()
		mr.Close()
	})
	return uc, pusher, mr
}

// ── CreateTeam ────────────────────────────────────────────────────────────────

// TestCreateTeamSuccess 验证创建队伍成功,返回正确快照。
func TestCreateTeamSuccess(t *testing.T) {
	uc, pusher, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	team, err := uc.CreateTeam(ctx, 1001, 2001)
	if err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	if team.TeamId != 1001 || team.CaptainId != 2001 || team.State != stateForming {
		t.Errorf("unexpected team: %+v", team)
	}
	if len(team.Members) != 1 || team.Members[0].PlayerId != 2001 {
		t.Errorf("unexpected members: %+v", team.Members)
	}
	// push 给创建者自身
	if len(pusher.calls) != 1 {
		t.Errorf("expected 1 push call, got %d", len(pusher.calls))
	}
}

// TestCreateTeamAlreadyInTeam 验证已在其他队的玩家不能再创建。
func TestCreateTeamAlreadyInTeam(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("first CreateTeam: %v", err)
	}
	_, err := uc.CreateTeam(ctx, 1002, 2001)
	if errcode.As(err) != errcode.ErrTeamAlreadyInTeam {
		t.Errorf("expected ErrTeamAlreadyInTeam, got: %v", err)
	}
}

// TestCreateTeamHealsOrphanIndex 复现线上死锁:player 索引残留指向已消失的队伍主体
// (孤儿索引)时,CreateTeam 不该被 3004 拦死,而应清脏索引后成功建队。
func TestCreateTeamHealsOrphanIndex(t *testing.T) {
	uc, _, mr := newTestUsecaseWithMR(t)
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("first CreateTeam: %v", err)
	}
	// 队伍主体 TTL 到期被回收,player 索引仍残留(孤儿索引)。
	mr.Del("pandora:team:{1001}")
	if !mr.Exists("pandora:team:player:2001") {
		t.Fatal("precondition: orphan player index should still exist")
	}

	team, err := uc.CreateTeam(ctx, 1002, 2001)
	if err != nil {
		t.Fatalf("CreateTeam with orphan index should heal and succeed, got: %v", err)
	}
	if team.TeamId != 1002 || team.CaptainId != 2001 {
		t.Errorf("unexpected healed team: %+v", team)
	}
	// 索引已改指向新队伍。
	if got, _ := mr.Get("pandora:team:player:2001"); got != "1002" {
		t.Errorf("expected index -> 1002 after heal, got %q", got)
	}
}

// TestCreateTeamRealCollisionStillRejected 验证自愈不放水:玩家真实在活跃队伍中时,
// CreateTeam 仍必须被 ERR_TEAM_ALREADY_IN_TEAM 拦下(不变量 §1)。
func TestCreateTeamRealCollisionStillRejected(t *testing.T) {
	uc, _, mr := newTestUsecaseWithMR(t)
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("first CreateTeam: %v", err)
	}
	// 队伍主体仍在(真实活跃)。
	_, err := uc.CreateTeam(ctx, 1002, 2001)
	if errcode.As(err) != errcode.ErrTeamAlreadyInTeam {
		t.Errorf("expected ErrTeamAlreadyInTeam for live team, got: %v", err)
	}
	// 索引未被误清,仍指向原队伍。
	if got, _ := mr.Get("pandora:team:player:2001"); got != "1001" {
		t.Errorf("expected index unchanged -> 1001, got %q", got)
	}
	// 写序反转后:失败的 CreateTeam 已先写过主体 1002,claim 失败必须回滚删掉,
	// 不留无主队伍主体。
	if mr.Exists("pandora:team:{1002}") {
		t.Error("expected rolled-back team body 1002 deleted")
	}
}

// ── Invite ────────────────────────────────────────────────────────────────────

// TestInvitePushOnlyTarget 验证 Invite push 只发给 target,不发给 inviter(协议原则 2)。
func TestInvitePushOnlyTarget(t *testing.T) {
	uc, pusher, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	pusher.calls = nil // 清除 create 的 push

	_, err := uc.Invite(ctx, 9001, 1001, 2001, 3001)
	if err != nil {
		t.Fatalf("Invite: %v", err)
	}
	// 默认 InvitePushMode="dual"(金丝雀共存期):双发独立事件(event_type=1)+ legacy(event_type=0)。
	// 两条都必须只发给 target(3001)、绝不发给 inviter(2001)。
	if len(pusher.calls) != 2 {
		t.Fatalf("expected 2 pushes (dual mode), got %d", len(pusher.calls))
	}
	// sawDedicated 与 sawLegacy 分别标记独立邀请事件和旧 TeamUpdateEvent 是否出现。
	var sawDedicated, sawLegacy bool
	// call 是 mock 按发送顺序记录的单次推送,逐条核对发起方、接收方和事件类型。
	for _, call := range pusher.calls {
		if call.caller != 2001 {
			t.Errorf("expected caller=2001, got %d", call.caller)
		}
		// 接收方只有 target(3001),不含 inviter(2001)
		// id 是当前推送记录中的一个接收玩家 ID。
		for _, id := range call.to {
			if id == 2001 {
				t.Error("inviter(2001) should not receive push")
			}
		}
		// eventType=1 表示独立邀请 payload,0 表示向后兼容的旧队伍更新 payload。
		switch call.eventType {
		case 1: // TEAM_PUSH_EVENT_TYPE_INVITE(独立事件)
			sawDedicated = true
		case 0: // legacy TeamUpdateEvent
			sawLegacy = true
		}
	}
	// dual 模式缺少任一类型都会导致对应代际客户端收不到邀请。
	if !sawDedicated || !sawLegacy {
		t.Errorf("dual mode should emit both dedicated(1) and legacy(0), got dedicated=%v legacy=%v", sawDedicated, sawLegacy)
	}
}

// TestInviteTeamFull 验证满员时 Invite 返 ErrTeamFull。
func TestInviteTeamFull(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	// 手动填满 5 人
	for playerID := uint64(3001); playerID <= 3004; playerID++ {
		inviteID := playerID + 9000
		if _, err := uc.Invite(ctx, inviteID, 1001, 2001, playerID); err != nil {
			t.Fatalf("Invite %d: %v", playerID, err)
		}
		if _, err := uc.AcceptInvite(ctx, inviteID, 1001, playerID); err != nil {
			t.Fatalf("AcceptInvite %d: %v", playerID, err)
		}
	}
	// 现在队满(5 人),再邀请应返 ErrTeamFull
	_, err := uc.Invite(ctx, 9999, 1001, 2001, 9999)
	if errcode.As(err) != errcode.ErrTeamFull {
		t.Errorf("expected ErrTeamFull, got: %v", err)
	}
}

// ── AcceptInvite ──────────────────────────────────────────────────────────────

// TestAcceptInviteFullAutoReady 验证第 5 人加入时队伍全员 ready 自动变 READY。
func TestAcceptInviteFullAutoReady(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}

	// 队长先 ready
	if _, err := uc.SetReady(ctx, 1001, 2001, true, 0); err != nil {
		t.Fatalf("SetReady captain: %v", err)
	}

	// 加入 4 名成员,都 ready 后触发 READY 状态
	for i := uint64(0); i < 4; i++ {
		pid := uint64(3001) + i
		inviteID := uint64(9001) + i
		if _, err := uc.Invite(ctx, inviteID, 1001, 2001, pid); err != nil {
			t.Fatalf("Invite %d: %v", pid, err)
		}
		result, err := uc.AcceptInvite(ctx, inviteID, 1001, pid)
		if err != nil {
			t.Fatalf("AcceptInvite %d: %v", pid, err)
		}
		// SetReady each new member
		if _, err := uc.SetReady(ctx, 1001, pid, true, 0); err != nil {
			t.Fatalf("SetReady %d: %v", pid, err)
		}
		_ = result
	}

	// 最终 GetTeam 应为 READY
	team, err := uc.GetTeam(ctx, 1001)
	if err != nil {
		t.Fatalf("GetTeam: %v", err)
	}
	if team.State != stateReady {
		t.Errorf("expected READY, got state=%d", team.State)
	}
}

// TestAcceptInviteExpired 验证邀请令牌过期后 AcceptInvite 返 ErrTeamInviteExpired。
func TestAcceptInviteExpired(t *testing.T) {
	uc, _, mr := newTestUsecaseWithMR(t)
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	if _, err := uc.Invite(ctx, 9001, 1001, 2001, 3001); err != nil {
		t.Fatalf("Invite: %v", err)
	}

	// 快进时钟超过 invite_ttl(60s 默认)
	mr.FastForward(2 * time.Minute)

	_, err := uc.AcceptInvite(ctx, 9001, 1001, 3001)
	if errcode.As(err) != errcode.ErrTeamInviteExpired {
		t.Errorf("expected ErrTeamInviteExpired, got: %v", err)
	}
}

// TestAcceptInviteAlreadyInTeam 验证不变量 §1:已在 A 队的玩家接受 B 队邀请被拒,
// 且 B 队成员列表不被污染(ClaimPlayer SETNX 在改成员前拦截)。
func TestAcceptInviteAlreadyInTeam(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	// 玩家 3001 先加入 A 队(1001)
	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam A: %v", err)
	}
	if _, err := uc.Invite(ctx, 9001, 1001, 2001, 3001); err != nil {
		t.Fatalf("Invite A: %v", err)
	}
	if _, err := uc.AcceptInvite(ctx, 9001, 1001, 3001); err != nil {
		t.Fatalf("AcceptInvite A: %v", err)
	}

	// B 队(1002)邀请同一玩家 3001
	if _, err := uc.CreateTeam(ctx, 1002, 2002); err != nil {
		t.Fatalf("CreateTeam B: %v", err)
	}
	if _, err := uc.Invite(ctx, 9002, 1002, 2002, 3001); err != nil {
		t.Fatalf("Invite B: %v", err)
	}

	// 接受 B 邀请应被 §1 拒绝
	_, err := uc.AcceptInvite(ctx, 9002, 1002, 3001)
	if errcode.As(err) != errcode.ErrTeamAlreadyInTeam {
		t.Fatalf("expected ErrTeamAlreadyInTeam, got: %v", err)
	}

	// B 队成员列表不应被污染,仍只有队长 2002
	teamB, err := uc.GetTeam(ctx, 1002)
	if err != nil {
		t.Fatalf("GetTeam B: %v", err)
	}
	if len(teamB.Members) != 1 || teamB.Members[0].PlayerId != 2002 {
		t.Errorf("team B polluted: %+v", teamB.Members)
	}
}

// ── LeaveTeam ─────────────────────────────────────────────────────────────────

// TestLeaveTeamCaptainTransfer 验证队长离队时队长转移给第一个成员。
func TestLeaveTeamCaptainTransfer(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	if _, err := uc.Invite(ctx, 9001, 1001, 2001, 3001); err != nil {
		t.Fatalf("Invite: %v", err)
	}
	if _, err := uc.AcceptInvite(ctx, 9001, 1001, 3001); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}

	result, err := uc.LeaveTeam(ctx, 1001, 2001) // 队长离队
	if err != nil {
		t.Fatalf("LeaveTeam: %v", err)
	}
	if result.CaptainId != 3001 {
		t.Errorf("expected new captain=3001, got %d", result.CaptainId)
	}
}

// TestLeaveTeamReadyBackToForming 验证 READY 状态下有人离开回到 FORMING。
func TestLeaveTeamReadyBackToForming(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	if _, err := uc.Invite(ctx, 9001, 1001, 2001, 3001); err != nil {
		t.Fatalf("Invite: %v", err)
	}
	if _, err := uc.AcceptInvite(ctx, 9001, 1001, 3001); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}
	// 两人都 ready → READY
	if _, err := uc.SetReady(ctx, 1001, 2001, true, 0); err != nil {
		t.Fatalf("SetReady 2001: %v", err)
	}
	if _, err := uc.SetReady(ctx, 1001, 3001, true, 0); err != nil {
		t.Fatalf("SetReady 3001: %v", err)
	}
	team, _ := uc.GetTeam(ctx, 1001)
	if team.State != stateReady {
		t.Fatalf("expected READY, got %d", team.State)
	}

	// 3001 离队 → 回 FORMING
	result, err := uc.LeaveTeam(ctx, 1001, 3001)
	if err != nil {
		t.Fatalf("LeaveTeam: %v", err)
	}
	if result.State != stateForming {
		t.Errorf("expected FORMING after leave, got %d", result.State)
	}
}

// ── Kick ─────────────────────────────────────────────────────────────────────

// TestKickNotCaptain 验证非队长踢人返 ErrTeamNotCaptain。
func TestKickNotCaptain(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	if _, err := uc.Invite(ctx, 9001, 1001, 2001, 3001); err != nil {
		t.Fatalf("Invite: %v", err)
	}
	if _, err := uc.AcceptInvite(ctx, 9001, 1001, 3001); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}

	// 3001(非队长)踢 2001 → ErrTeamNotCaptain
	_, err := uc.Kick(ctx, 1001, 3001, 2001)
	if errcode.As(err) != errcode.ErrTeamNotCaptain {
		t.Errorf("expected ErrTeamNotCaptain, got: %v", err)
	}
}

// ── SetReady ──────────────────────────────────────────────────────────────────

// TestSetReadyAllReady 验证全员 ready 后状态变 READY。
func TestSetReadyAllReady(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	if _, err := uc.Invite(ctx, 9001, 1001, 2001, 3001); err != nil {
		t.Fatalf("Invite: %v", err)
	}
	if _, err := uc.AcceptInvite(ctx, 9001, 1001, 3001); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}

	if _, err := uc.SetReady(ctx, 1001, 2001, true, 0); err != nil {
		t.Fatalf("SetReady 2001: %v", err)
	}
	result, err := uc.SetReady(ctx, 1001, 3001, true, 0)
	if err != nil {
		t.Fatalf("SetReady 3001: %v", err)
	}
	if result.State != stateReady {
		t.Errorf("expected READY, got %d", result.State)
	}
}

// 加入一支已经 READY 的队伍必须把队伍打回 FORMING —— 新成员默认 ready=false。
//
// 回归 2026-08-05 线上实录:单人队长点准备 → 队伍 READY;另一个号 AcceptInvite 进队后
// 队伍**仍是 READY**,而他自己 ready=false。这几秒里队长点开始,matchmaker.resolveMembers
// 只校验 State==READY、不逐人校验 ready,会带着没准备的人开局。
func TestJoinTeamDemotesReadyTeamBackToForming(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	// 单人队长点准备 → 全员(就他一个)已准备 → READY。
	ready, err := uc.SetReady(ctx, 1001, 2001, true, 0)
	if err != nil {
		t.Fatalf("SetReady: %v", err)
	}
	if ready.State != stateReady {
		t.Fatalf("单人准备后应为 READY,实际 %d", ready.State)
	}

	joined, err := uc.AcceptInvite(ctx, 0, 1001, 3001)
	if err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}
	if joined.State != stateForming {
		t.Fatalf("有人没准备时队伍必须回退 FORMING,实际 %d", joined.State)
	}
	if allReady(joined.Members) {
		t.Fatal("新成员默认不该是已准备")
	}

	// 新成员点完准备后重新回到 READY(两个方向都要通)。
	after, err := uc.SetReady(ctx, 1001, 3001, true, 0)
	if err != nil {
		t.Fatalf("SetReady 3001: %v", err)
	}
	if after.State != stateReady {
		t.Fatalf("全员准备后应为 READY,实际 %d", after.State)
	}
}

// ── GetMyTeam ─────────────────────────────────────────────────────────────────

// TestGetMyTeamHasTeam 验证已在队伍中的玩家能查回自己队伍。
func TestGetMyTeamHasTeam(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}

	team, hasTeam, err := uc.GetMyTeam(ctx, 2001)
	if err != nil {
		t.Fatalf("GetMyTeam: %v", err)
	}
	if !hasTeam {
		t.Fatal("expected hasTeam=true")
	}
	if team.TeamId != 1001 || team.CaptainId != 2001 || len(team.Members) != 1 {
		t.Errorf("unexpected team: %+v", team)
	}
}

// TestGetMyTeamNoTeam 验证没有队伍的玩家返回 hasTeam=false 且不报错(正常态)。
func TestGetMyTeamNoTeam(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()

	team, hasTeam, err := uc.GetMyTeam(context.Background(), 7777)
	if err != nil {
		t.Fatalf("GetMyTeam: %v", err)
	}
	if hasTeam || team != nil {
		t.Errorf("expected no team, got hasTeam=%v team=%+v", hasTeam, team)
	}
}

// TestGetMyTeamStaleIndexCleaned 验证索引命中但队伍记录已过期(TTL 竞态)时:
// 按无队伍处理 + 顺手清掉脏索引,玩家随后可以正常再建队(不被 SETNX 挡住)。
func TestGetMyTeamStaleIndexCleaned(t *testing.T) {
	uc, _, mr := newTestUsecaseWithMR(t)
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	// 模拟队伍 key TTL 到期被回收,player 索引仍残留
	mr.Del("pandora:team:{1001}")

	team, hasTeam, err := uc.GetMyTeam(ctx, 2001)
	if err != nil {
		t.Fatalf("GetMyTeam: %v", err)
	}
	if hasTeam || team != nil {
		t.Errorf("expected no team after stale index, got hasTeam=%v team=%+v", hasTeam, team)
	}
	// 脏索引已清理,可重新建队
	if _, err := uc.CreateTeam(ctx, 1002, 2001); err != nil {
		t.Fatalf("CreateTeam after cleanup: %v", err)
	}
}

// TestGetMyTeamDisbandedTreatedAsNoTeam 验证队伍已解散(短 TTL 保留期内)但索引残留时
// 按无队伍处理并清掉脏索引(走 DISBANDED 分支,而非 key miss 分支)。
func TestGetMyTeamDisbandedTreatedAsNoTeam(t *testing.T) {
	uc, _, mr := newTestUsecaseWithMR(t)
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	// 队长离队 → 单人队解散(DISBANDED 记录短 TTL 保留,正常路径会删索引)
	if _, err := uc.LeaveTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("LeaveTeam: %v", err)
	}
	// 人为残留脏索引,指向仍在保留期内的 DISBANDED 记录
	if err := mr.Set("pandora:team:player:2001", "1001"); err != nil {
		t.Fatalf("set stale index: %v", err)
	}

	team, hasTeam, err := uc.GetMyTeam(ctx, 2001)
	if err != nil {
		t.Fatalf("GetMyTeam: %v", err)
	}
	if hasTeam || team != nil {
		t.Errorf("expected no team after disband, got hasTeam=%v team=%+v", hasTeam, team)
	}
	if mr.Exists("pandora:team:player:2001") {
		t.Error("expected stale index cleaned")
	}
}

// TestGetMyTeamTouchRefreshesTTL 验证在线轮询 GetMyTeam 命中活跃队伍时把队伍 key 与
// 玩家索引 key 的 TTL 拉回 active_ttl(在线心跳保活,防挂机队伍被 active_ttl 误回收)。
func TestGetMyTeamTouchRefreshesTTL(t *testing.T) {
	uc, _, mr := newTestUsecaseWithMR(t)
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	// 快进 30m:两 key 剩余 TTL 只剩一半(默认 active_ttl=60m)
	mr.FastForward(30 * time.Minute)

	if _, hasTeam, err := uc.GetMyTeam(ctx, 2001); err != nil || !hasTeam {
		t.Fatalf("GetMyTeam: hasTeam=%v err=%v", hasTeam, err)
	}

	// touch 后 TTL 应回到接近 active_ttl(60m)
	if ttl := mr.TTL("pandora:team:{1001}"); ttl < 50*time.Minute {
		t.Errorf("team key TTL not refreshed by GetMyTeam: %v", ttl)
	}
	if ttl := mr.TTL("pandora:team:player:2001"); ttl < 50*time.Minute {
		t.Errorf("player index TTL not refreshed by GetMyTeam: %v", ttl)
	}
}

// TestGetMyTeamTouchThrottled 验证续期节流:同一玩家 15 分钟(真实时钟)内的第二次
// GetMyTeam 不再重复续期(TTL 不被再次拉满)。
func TestGetMyTeamTouchThrottled(t *testing.T) {
	uc, _, mr := newTestUsecaseWithMR(t)
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	// 第一次 GetMyTeam:触发 touch(TTL 满 60m),并记录节流时刻
	if _, hasTeam, err := uc.GetMyTeam(ctx, 2001); err != nil || !hasTeam {
		t.Fatalf("GetMyTeam#1: hasTeam=%v err=%v", hasTeam, err)
	}
	// 快进 20m(仅 miniredis 时钟;真实时钟仍在节流窗口内)
	mr.FastForward(20 * time.Minute)

	if _, hasTeam, err := uc.GetMyTeam(ctx, 2001); err != nil || !hasTeam {
		t.Fatalf("GetMyTeam#2: hasTeam=%v err=%v", hasTeam, err)
	}
	// 节流生效:第二次不 touch,TTL 应停留在 ~40m 而不是被拉回 60m
	if ttl := mr.TTL("pandora:team:{1001}"); ttl > 45*time.Minute {
		t.Errorf("second GetMyTeam within throttle window should not re-touch, TTL=%v", ttl)
	}
}

// TestSetReadyPartialStillForming 验证部分 ready 时仍是 FORMING。
func TestSetReadyPartialStillForming(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	if _, err := uc.Invite(ctx, 9001, 1001, 2001, 3001); err != nil {
		t.Fatalf("Invite: %v", err)
	}
	if _, err := uc.AcceptInvite(ctx, 9001, 1001, 3001); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}

	result, err := uc.SetReady(ctx, 1001, 2001, true, 0) // 只有队长 ready
	if err != nil {
		t.Fatalf("SetReady 2001: %v", err)
	}
	if result.State != stateForming {
		t.Errorf("expected FORMING(partial ready), got %d", result.State)
	}
}

// ── GetTeam ───────────────────────────────────────────────────────────────────

// TestGetTeamNotFound 验证查不存在的队伍返 ErrTeamNotFound。
func TestGetTeamNotFound(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	_, err := uc.GetTeam(ctx, 9999)
	if errcode.As(err) != errcode.ErrTeamNotFound {
		t.Errorf("expected ErrTeamNotFound, got: %v", err)
	}
}

// ── 状态机不变量 ──────────────────────────────────────────────────────────────

// TestDisbandedRejectsAllWrites 验证 DISBANDED 状态下所有写操作返 ErrTeamWrongState。
func TestDisbandedRejectsAllWrites(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	// 队长离队 → 队伍空 → DISBANDED
	if _, err := uc.LeaveTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("LeaveTeam: %v", err)
	}

	_, err := uc.SetReady(ctx, 1001, 2001, true, 0)
	if errcode.As(err) != errcode.ErrTeamWrongState {
		t.Errorf("SetReady on DISBANDED: expected ErrTeamWrongState, got: %v", err)
	}
}

// TestConcurrentRetrySucceeds 验证 WATCH 冲突重试后能成功(miniredis 模拟)。
func TestConcurrentRetrySucceeds(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}

	// 顺序 SetReady 两次,验证乐观锁在无并发情况下成功(miniredis 单线程,不测真并发冲突)
	if _, err := uc.SetReady(ctx, 1001, 2001, true, 0); err != nil {
		t.Fatalf("SetReady 1: %v", err)
	}
	if _, err := uc.SetReady(ctx, 1001, 2001, false, 0); err != nil {
		t.Fatalf("SetReady 2: %v", err)
	}

	team, _ := uc.GetTeam(ctx, 1001)
	if team.Members[0].Ready {
		t.Error("expected ready=false after second SetReady")
	}
}

// ── 匹配联动(离队/踢人 → 撤销 matchmaker 票据)────────────────────────────────

// mockCanceler 记录 CancelMatch 调用,可注入返回错误。
type mockCanceler struct {
	calls []uint64
	err   error
}

func (m *mockCanceler) CancelMatch(_ context.Context, playerID uint64) error {
	m.calls = append(m.calls, playerID)
	return m.err
}

// makeTwoMemberTeam 建 2 人队(队长 2001 + 成员 3001)。
func makeTwoMemberTeam(t *testing.T, uc *TeamUsecase, ctx context.Context) {
	t.Helper()
	if _, err := uc.CreateTeam(ctx, 1001, 2001); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	if _, err := uc.Invite(ctx, 9001, 1001, 2001, 3001); err != nil {
		t.Fatalf("Invite: %v", err)
	}
	if _, err := uc.AcceptInvite(ctx, 9001, 1001, 3001); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}
}

// TestLeaveTeamCancelsMatchmaking 验证成员离队后按离队者 player_id 联动撤销匹配票据。
func TestLeaveTeamCancelsMatchmaking(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()
	makeTwoMemberTeam(t, uc, ctx)

	canceler := &mockCanceler{}
	uc.SetMatchCanceler(canceler)

	if _, err := uc.LeaveTeam(ctx, 1001, 3001); err != nil {
		t.Fatalf("LeaveTeam: %v", err)
	}
	if len(canceler.calls) != 1 || canceler.calls[0] != 3001 {
		t.Errorf("expected CancelMatch(3001) once, got %v", canceler.calls)
	}
}

// TestKickCancelsMatchmaking 验证踢人后按被踢者 player_id 联动撤销匹配票据(而非队长)。
func TestKickCancelsMatchmaking(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()
	makeTwoMemberTeam(t, uc, ctx)

	canceler := &mockCanceler{}
	uc.SetMatchCanceler(canceler)

	if _, err := uc.Kick(ctx, 1001, 2001, 3001); err != nil {
		t.Fatalf("Kick: %v", err)
	}
	if len(canceler.calls) != 1 || canceler.calls[0] != 3001 {
		t.Errorf("expected CancelMatch(3001) once, got %v", canceler.calls)
	}
}

// TestLeaveTeamCancelMatchmakingNotFoundIgnored 验证离队者本没在排队(matchmaker 返 4004)
// 时按常态忽略,离队本身照常成功;其余错误也不阻断离队(弱依赖)。
func TestLeaveTeamCancelMatchmakingNotFoundIgnored(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()
	makeTwoMemberTeam(t, uc, ctx)

	canceler := &mockCanceler{err: errcode.New(errcode.ErrMatchNotFound, "not queueing")}
	uc.SetMatchCanceler(canceler)

	result, err := uc.LeaveTeam(ctx, 1001, 3001)
	if err != nil {
		t.Fatalf("LeaveTeam should not fail on canceler 4004: %v", err)
	}
	if len(result.Members) != 1 {
		t.Errorf("expected 1 member left, got %d", len(result.Members))
	}
}
