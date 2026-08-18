// offline_leave_test.go — 离线成员自动退队单测(2026-08-06)。
//
// 这个功能踢错人是不可逆的(队伍散了、正在打的局被拆),所以测试重点全在**闸门**上:
// 什么情况下**不许**动手,比"正常情况下能摘掉"重要得多。
package biz

import (
	"context"
	"errors"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/pkg/offlinewatch"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/data"
)

// newOfflineLeaveUsecase 造一个开启了「离线自动退队」的 usecase。
func newOfflineLeaveUsecase(t *testing.T) (*TeamUsecase, *mockPusher) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	pusher := &mockPusher{}

	var cfg conf.Config
	cfg.Team.OfflineLeave.Enabled = true
	cfg.Defaults()

	uc := NewTeamUsecase(data.NewRedisTeamRepo(rdb), pusher, cfg.Team)
	t.Cleanup(func() {
		_ = rdb.Close()
		mr.Close()
	})
	return uc, pusher
}

// setupTwoMemberTeam 建一支「队长 captainID + 队员 memberID」的队伍。
// 先建队再注入 commitment reader —— 否则入队闸门会拿它做判定,干扰用例意图。
func setupTwoMemberTeam(t *testing.T, uc *TeamUsecase, teamID, captainID, memberID uint64) {
	t.Helper()
	ctx := context.Background()
	if _, err := uc.CreateTeam(ctx, teamID, captainID); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	if _, err := uc.AcceptInvite(ctx, 0, teamID, memberID); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}
}

func teamMemberIDs(t *testing.T, uc *TeamUsecase, teamID uint64) []uint64 {
	t.Helper()
	team, err := uc.GetTeam(context.Background(), teamID)
	if err != nil {
		t.Fatalf("GetTeam: %v", err)
	}
	return memberIDs(team)
}

// failDeleteIndexRepo 注入 player→team compare-delete 的瞬时失败，复现
// 「队伍主体 CAS 已成功、归属索引清理失败」的部分成功窗口。
type failDeleteIndexRepo struct {
	data.TeamRepo
	failures int
	calls    int
	err      error
}

func (r *failDeleteIndexRepo) DeletePlayerIndexIfMatches(ctx context.Context, playerID, teamID uint64) error {
	r.calls++
	if r.failures > 0 {
		r.failures--
		return r.err
	}
	return r.TeamRepo.DeletePlayerIndexIfMatches(ctx, playerID, teamID)
}

// ── 摘人成功路径 ────────────────────────────────────────────────────────────

func TestOnPlayerOffline_摘掉离线队员并清归属(t *testing.T) {
	uc, pusher := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9601, 7601, 7602)
	uc.SetMatchCommitmentReader(&mockCommitment{})

	before := len(pusher.calls)
	if err := uc.OnPlayerOffline(ctx, 7602, 1_000); err != nil {
		t.Fatalf("OnPlayerOffline: %v", err)
	}

	if got := teamMemberIDs(t, uc, 9601); len(got) != 1 || got[0] != 7601 {
		t.Fatalf("离线队员应被摘掉,剩余成员=%v", got)
	}
	// 归属索引必须一起清:留着他就再也建不了新队(不变量 §1 的残留侧漏洞)。
	if _, found, err := uc.repo.GetPlayerTeamID(ctx, 7602); err != nil {
		t.Fatalf("GetPlayerTeamID: %v", err)
	} else if found {
		t.Fatal("被摘成员的归属索引必须清掉,否则他重连回来建不了新队")
	}
	if len(pusher.calls) == before {
		t.Fatal("摘人必须推送,否则队友界面上会一直挂着一个已经不在队里的人")
	}
	last := pusher.calls[len(pusher.calls)-1]
	if last.caller != 0 {
		t.Fatalf("系统行为无发起者,caller 应为 0 让所有人都收到, got=%d", last.caller)
	}
	var sawRemoved bool
	for _, pid := range last.to {
		if pid == 7602 {
			sawRemoved = true
		}
	}
	if !sawRemoved {
		t.Fatal("被摘的人本人也要收到:他若刚好重连回来,得立刻知道自己已不在队里")
	}
}

func TestOnPlayerOffline_索引删除失败会保留任务并在重试时修复(t *testing.T) {
	uc, pusher := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9614, 7741, 7742)
	uc.SetMatchCommitmentReader(&mockCommitment{})

	deleteErr := errors.New("delete player index unavailable")
	faultRepo := &failDeleteIndexRepo{
		TeamRepo: uc.repo,
		failures: 2,
		err:      deleteErr,
	}
	uc.repo = faultRepo

	beforePushes := len(pusher.calls)
	err := uc.OnPlayerOffline(ctx, 7742, 1_000)
	if !errors.Is(err, deleteErr) {
		t.Fatalf("主体已写但索引未清时必须返回 error 保留复查任务, got=%v", err)
	}
	if got := teamMemberIDs(t, uc, 9614); len(got) != 1 || got[0] != 7741 {
		t.Fatalf("故障窗口应已完成队伍主体 CAS, 剩余成员=%v", got)
	}
	if got, found, getErr := uc.repo.GetPlayerTeamID(ctx, 7742); getErr != nil {
		t.Fatalf("GetPlayerTeamID: %v", getErr)
	} else if !found || got != 9614 {
		t.Fatalf("首次 compare-delete 失败后旧索引应仍可供重试定位, got=%d found=%v", got, found)
	}
	if len(pusher.calls) == beforePushes {
		t.Fatal("索引瞬时失败不得跳过主体写后的队伍更新推送")
	}
	afterFirstPushes := len(pusher.calls)

	// 第二轮会看到「旧索引仍在，但队伍已不含该玩家」。它必须继续 compare-delete；
	// 清理仍失败时继续返回 error，而不是把业务终态误当成整个任务已完成。
	if err := uc.OnPlayerOffline(ctx, 7742, 1_000); !errors.Is(err, deleteErr) {
		t.Fatalf("终态分支清索引失败时仍须保留复查任务, got=%v", err)
	}
	if got, found, getErr := uc.repo.GetPlayerTeamID(ctx, 7742); getErr != nil {
		t.Fatalf("GetPlayerTeamID after failed retry: %v", getErr)
	} else if !found || got != 9614 {
		t.Fatalf("终态清理失败后旧索引必须仍在, got=%d found=%v", got, found)
	}
	if len(pusher.calls) != afterFirstPushes {
		t.Fatalf("终态重试只修索引，不应重复推送队伍变更, pushes=%d want=%d", len(pusher.calls), afterFirstPushes)
	}

	if err := uc.OnPlayerOffline(ctx, 7742, 1_000); err != nil {
		t.Fatalf("索引恢复后重试应完成精确清理: %v", err)
	}
	if _, found, getErr := uc.repo.GetPlayerTeamID(ctx, 7742); getErr != nil {
		t.Fatalf("GetPlayerTeamID after retry: %v", getErr)
	} else if found {
		t.Fatal("重试必须清掉仍指向旧 teamID 的残留索引")
	}
	if faultRepo.calls != 3 {
		t.Fatalf("首次摘人及每轮终态重试都必须执行 compare-delete, calls=%d", faultRepo.calls)
	}
	if len(pusher.calls) != afterFirstPushes {
		t.Fatalf("终态重试只修索引，不应重复推送队伍变更, pushes=%d want=%d", len(pusher.calls), afterFirstPushes)
	}
}

func TestOnPlayerOffline_队长离线则转移队长(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9602, 7611, 7612)
	uc.SetMatchCommitmentReader(&mockCommitment{})

	if err := uc.OnPlayerOffline(ctx, 7611, 1_000); err != nil {
		t.Fatalf("OnPlayerOffline: %v", err)
	}
	team, err := uc.GetTeam(ctx, 9602)
	if err != nil {
		t.Fatalf("GetTeam: %v", err)
	}
	if team.CaptainId != 7612 {
		t.Fatalf("队长掉线被摘后必须转移,否则队伍永远没人能改图/审批, got=%d", team.CaptainId)
	}
}

func TestOnPlayerOffline_READY队摘人后回退FORMING(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9603, 7621, 7622)
	if _, err := uc.SetReady(ctx, 9603, 7621, true, 1); err != nil {
		t.Fatalf("SetReady: %v", err)
	}
	if _, err := uc.SetReady(ctx, 9603, 7622, true, 1); err != nil {
		t.Fatalf("SetReady: %v", err)
	}
	uc.SetMatchCommitmentReader(&mockCommitment{})

	if err := uc.OnPlayerOffline(ctx, 7622, 1_000); err != nil {
		t.Fatalf("OnPlayerOffline: %v", err)
	}
	team, err := uc.GetTeam(ctx, 9603)
	if err != nil {
		t.Fatalf("GetTeam: %v", err)
	}
	if team.State != stateForming {
		t.Fatalf("少了人就不再是「全员已准备」,应回 FORMING, got=%v", team.State)
	}
}

func TestOnPlayerOffline_幂等(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9604, 7631, 7632)
	uc.SetMatchCommitmentReader(&mockCommitment{})

	for i := 0; i < 3; i++ {
		if err := uc.OnPlayerOffline(ctx, 7632, 1_000); err != nil {
			t.Fatalf("第 %d 次调用应当幂等成功: %v", i+1, err)
		}
	}
	if got := teamMemberIDs(t, uc, 9604); len(got) != 1 {
		t.Fatalf("重复调用不得继续摘人, 剩余=%v", got)
	}
}

// ── 闸门:这些情况一律不许动手 ──────────────────────────────────────────────

func TestOnPlayerOffline_整队被对局占住时不动(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9605, 7641, 7642)
	// 队长被对局占住 = 这支队伍正在排队 / 确认 / 拉 DS / 打整场。
	uc.SetMatchCommitmentReader(&mockCommitment{committed: map[uint64]bool{7641: true}})

	if err := uc.OnPlayerOffline(ctx, 7642, 1_000); !errors.Is(err, offlinewatch.ErrDeferred) {
		t.Fatalf("整队被对局占住时必须延后并保留复查任务, got=%v", err)
	}
	if got := teamMemberIDs(t, uc, 9605); len(got) != 2 {
		t.Fatalf("绝不能拆一支正在打的队伍(会波及还在正常游戏的队友), 剩余=%v", got)
	}
}

func TestOnPlayerOffline_玩家自己被对局占住时不动(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9606, 7651, 7652)
	// 队长没被占住、但这名成员自己还持有票据:locator 与 matchmaker 的短暂不一致。
	uc.SetMatchCommitmentReader(&mockCommitment{committed: map[uint64]bool{7652: true}})

	if err := uc.OnPlayerOffline(ctx, 7652, 1_000); !errors.Is(err, offlinewatch.ErrDeferred) {
		t.Fatalf("玩家仍被对局占住时必须延后并保留复查任务, got=%v", err)
	}
	if got := teamMemberIDs(t, uc, 9606); len(got) != 2 {
		t.Fatalf("玩家自己还在对局里就不能摘, 剩余=%v", got)
	}
}

func TestOnPlayerOffline_对局状态读不到必须失败重试而不是放行(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9607, 7661, 7662)
	uc.SetMatchCommitmentReader(&mockCommitment{err: errors.New("matchmaker unavailable")})

	err := uc.OnPlayerOffline(ctx, 7662, 1_000)
	if err == nil {
		t.Fatal("读不到对局状态必须 fail-closed 返回 error(下轮重试),绝不能当成「没在对局中」放行")
	}
	if got := teamMemberIDs(t, uc, 9607); len(got) != 2 {
		t.Fatalf("fail-closed 时不得改动队伍, 剩余=%v", got)
	}
}

func TestOnPlayerOffline_单人队不动(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	if _, err := uc.CreateTeam(ctx, 9608, 7671); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	uc.SetMatchCommitmentReader(&mockCommitment{})

	if err := uc.OnPlayerOffline(ctx, 7671, 1_000); err != nil {
		t.Fatalf("OnPlayerOffline: %v", err)
	}
	if got := teamMemberIDs(t, uc, 9608); len(got) != 1 {
		t.Fatalf("单人队没有队友受影响,应留给 active_ttl 自然回收(重连回来队伍还在), 剩余=%v", got)
	}
}

func TestOnPlayerOffline_不在任何队伍属正常路径(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	// 返回 error 会让骨架一直重试到保留期结束 —— 这不是失败,是没事可做。
	if err := uc.OnPlayerOffline(context.Background(), 7681, 1_000); err != nil {
		t.Fatalf("玩家没有队伍时应返回 nil(处理完成), got=%v", err)
	}
}

func TestOnPlayerOffline_功能关闭时完全不动(t *testing.T) {
	uc, pusher, cleanup := newTestUsecase(t) // 默认配置 = 功能关闭
	defer cleanup()
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9609, 7691, 7692)
	uc.SetMatchCommitmentReader(&mockCommitment{})

	before := len(pusher.calls)
	if err := uc.OnPlayerOffline(ctx, 7692, 1_000); err != nil {
		t.Fatalf("关闭时应静默返回: %v", err)
	}
	if got := teamMemberIDs(t, uc, 9609); len(got) != 2 {
		t.Fatalf("功能关闭时行为必须与历史完全一致, 剩余=%v", got)
	}
	if len(pusher.calls) != before {
		t.Fatal("功能关闭时不得产生任何推送")
	}
}

func TestOnPlayerOffline_没接matchmaker时按关闭处理(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t) // Enabled=true 但没注入 commitment reader
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9610, 7701, 7702)

	if err := uc.OnPlayerOffline(ctx, 7702, 1_000); err != nil {
		t.Fatalf("装配不全时应静默按关处理: %v", err)
	}
	if got := teamMemberIDs(t, uc, 9610); len(got) != 2 {
		t.Fatalf("缺了对局闸门就绝不能摘人(有拆掉在打队伍的风险), 剩余=%v", got)
	}
}

// ── 读路径兜底 ──────────────────────────────────────────────────────────────

type fakeInspector struct {
	calls [][]uint64
	err   error
}

func (f *fakeInspector) Observe(_ context.Context, ids []uint64) error {
	f.calls = append(f.calls, append([]uint64(nil), ids...))
	return f.err
}

func TestGetMyTeam_兜底把完整成员一次交给统一观察入口(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9611, 7711, 7712)
	uc.SetMatchCommitmentReader(&mockCommitment{})

	insp := &fakeInspector{}
	uc.SetPresenceInspector(insp)

	if _, has, err := uc.GetMyTeam(ctx, 7711); err != nil || !has {
		t.Fatalf("GetMyTeam: err=%v has=%v", err, has)
	}
	if len(insp.calls) != 1 {
		t.Fatalf("每次读 Team 只能调用一次 Observe, calls=%v", insp.calls)
	}
	got := insp.calls[0]
	if len(got) != 2 || got[0] != 7711 || got[1] != 7712 {
		t.Fatalf("Team 必须把完整成员列表交给 offlinewatch, got=%v", got)
	}
	// 分类与排期由 offlinewatch 完成，Team 读路径本身不动队伍。
	if members := teamMemberIDs(t, uc, 9611); len(members) != 2 {
		t.Fatalf("读路径不得直接摘人, 剩余=%v", members)
	}
}

func TestGetMyTeam_兜底失败不得影响读返回(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9612, 7721, 7722)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	uc.SetPresenceInspector(&fakeInspector{err: errors.New("locator unavailable")})

	// 组队面板打不开,比多留一个离线成员严重得多。
	team, has, err := uc.GetMyTeam(ctx, 7721)
	if err != nil || !has || team == nil {
		t.Fatalf("locator 挂了也必须照常返回队伍快照: err=%v has=%v", err, has)
	}
}

func TestGetMyTeam_未接兜底时行为不变(t *testing.T) {
	uc, _, cleanup := newTestUsecase(t)
	defer cleanup()
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9613, 7731, 7732)

	if _, has, err := uc.GetMyTeam(ctx, 7731); err != nil || !has {
		t.Fatalf("未注入 inspector 时读路径必须与历史一致: err=%v has=%v", err, has)
	}
}

// 编译期确认 TeamUsecase 满足骨架的 Handler 契约(签名漂了要在这里就炸,
// 而不是等到 main 装配或线上第一次掉线)。
var _ interface {
	OnPlayerOffline(ctx context.Context, playerID uint64, offlineSinceMs int64) error
} = (*TeamUsecase)(nil)

var _ = teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_OFFLINE_LEFT

// ── TOCTOU 窗口补偿 ─────────────────────────────────────────────────────────

// 闸门放行后、改队伍前，队长恰好点了开始匹配 → matchmaker 把这名离线成员冻进票据。
// 摘人已经发生，此时必须撤票让全队重新匹配，而不是让他被拉进一场自己不在场的对局。
func TestOnPlayerOffline_窗口内被冻进票据时撤票补偿(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9614, 7741, 7742)

	// 闸门读到「都没被占住」放行；写完队伍后的复核才看到票据已成立。
	commitment := &raceCommitment{}
	uc.SetMatchCommitmentReader(commitment)
	canceler := &recordingCanceler{}
	uc.SetMatchCanceler(canceler)

	if err := uc.OnPlayerOffline(ctx, 7742, 1_000); err != nil {
		t.Fatalf("OnPlayerOffline: %v", err)
	}
	if got := teamMemberIDs(t, uc, 9614); len(got) != 1 {
		t.Fatalf("摘人本身应当完成, 剩余=%v", got)
	}
	if len(canceler.cancelled) != 1 || canceler.cancelled[0] != 7742 {
		t.Fatalf("窗口命中必须撤票(否则他会被拉进一场自己不在场的对局), got=%v", canceler.cancelled)
	}
}

func TestOnPlayerOffline_窗口未命中不得撤票(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9615, 7751, 7752)
	uc.SetMatchCommitmentReader(&mockCommitment{})
	canceler := &recordingCanceler{}
	uc.SetMatchCanceler(canceler)

	if err := uc.OnPlayerOffline(ctx, 7752, 1_000); err != nil {
		t.Fatalf("OnPlayerOffline: %v", err)
	}
	// 常态下根本没有票可撤，多打一次 RPC 只会制造误导性日志。
	if len(canceler.cancelled) != 0 {
		t.Fatalf("没被占住时不得撤票, got=%v", canceler.cancelled)
	}
}

// raceCommitment 模拟 TOCTOU：前两次(闸②队长 / 闸③本人)都说没被占住，
// 摘人之后的复核才说已被占住。
type raceCommitment struct{ calls int }

func (m *raceCommitment) IsPlayerCommittedToMatch(_ context.Context, _ uint64) (bool, error) {
	m.calls++
	return m.calls > 2, nil
}

type recordingCanceler struct{ cancelled []uint64 }

func (c *recordingCanceler) CancelMatch(_ context.Context, playerID uint64) error {
	c.cancelled = append(c.cancelled, playerID)
	return nil
}

// ── 组票 roster fence:与 matchmaker 的共同线性化点 ───────────────────────────

// 这条是 TOCTOU 真正被消除的判据:BeginTeamMatch 上锁之后,摘人必须在**同一把锁内**
// 被拒。此前两者分属两把锁,只能靠事后补偿收敛后果。
func TestBeginTeamMatch_上锁后摘人必须被拒(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9620, 7761, 7762)
	if _, err := uc.SetReady(ctx, 9620, 7761, true, 1); err != nil {
		t.Fatalf("SetReady: %v", err)
	}
	if _, err := uc.SetReady(ctx, 9620, 7762, true, 1); err != nil {
		t.Fatalf("SetReady: %v", err)
	}
	uc.SetMatchCommitmentReader(&mockCommitment{})

	frozen, expiresAt, err := uc.BeginTeamMatch(ctx, 9620, 7761, "op-1", 5000)
	if err != nil {
		t.Fatalf("BeginTeamMatch: %v", err)
	}
	if len(frozen.Members) != 2 {
		t.Fatalf("冻结的名单应含全部成员, got=%d", len(frozen.Members))
	}
	if expiresAt <= time.Now().UnixMilli() {
		t.Fatalf("租约必须在未来: expires=%d", expiresAt)
	}

	// 组票已经把这份名单冻进票据的路上 —— 此刻摘人会造出「人在票据、不在队伍」。
	err = uc.OnPlayerOffline(ctx, 7762, 1_000)
	if !errors.Is(err, offlinewatch.ErrDeferred) {
		t.Fatalf("上锁期间摘人必须推迟(ErrDeferred)而不是执行: %v", err)
	}
	if got := teamMemberIDs(t, uc, 9620); len(got) != 2 {
		t.Fatalf("上锁期间队伍成员不得被改动, 剩余=%v", got)
	}
}

// 租约到期自净:matchmaker 崩在半路也不会把队伍永久卡住(不变量 §20)。
func TestBeginTeamMatch_租约过期后摘人恢复(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9621, 7771, 7772)
	if _, err := uc.SetReady(ctx, 9621, 7771, true, 1); err != nil {
		t.Fatalf("SetReady: %v", err)
	}
	if _, err := uc.SetReady(ctx, 9621, 7772, true, 1); err != nil {
		t.Fatalf("SetReady: %v", err)
	}
	uc.SetMatchCommitmentReader(&mockCommitment{})

	if _, _, err := uc.BeginTeamMatch(ctx, 9621, 7771, "op-1", 1); err != nil {
		t.Fatalf("BeginTeamMatch: %v", err)
	}
	// lease 被钳到下限 2s；直接把租约改成过去,模拟到期(不 sleep 真实时间)。
	if err := uc.repo.UpdateWithLock(ctx, 9621, 3, func(team *teamv1.TeamStorageRecord) error {
		team.MatchLockUntilMs = time.Now().Add(-time.Second).UnixMilli()
		return nil
	}, uc.activeTTL()); err != nil {
		t.Fatalf("过期租约: %v", err)
	}

	if err := uc.OnPlayerOffline(ctx, 7772, 1_000); err != nil {
		t.Fatalf("租约过期后应恢复正常摘人: %v", err)
	}
	if got := teamMemberIDs(t, uc, 9621); len(got) != 1 {
		t.Fatalf("租约过期后应当摘掉离线成员, 剩余=%v", got)
	}
}

// 同 operation 的重试必须幂等续租,不能把自己的重试判成冲突(§9.23)。
func TestBeginTeamMatch_同Operation幂等续租(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9622, 7781, 7782)
	if _, err := uc.SetReady(ctx, 9622, 7781, true, 1); err != nil {
		t.Fatalf("SetReady: %v", err)
	}
	if _, err := uc.SetReady(ctx, 9622, 7782, true, 1); err != nil {
		t.Fatalf("SetReady: %v", err)
	}

	if _, _, err := uc.BeginTeamMatch(ctx, 9622, 7781, "op-same", 5000); err != nil {
		t.Fatalf("首次 Begin: %v", err)
	}
	if _, _, err := uc.BeginTeamMatch(ctx, 9622, 7781, "op-same", 5000); err != nil {
		t.Fatalf("同 operation 重试必须幂等续租,不得判冲突: %v", err)
	}
	// 另一次组票在租约内必须被拒。
	if _, _, err := uc.BeginTeamMatch(ctx, 9622, 7781, "op-other", 5000); err == nil {
		t.Fatal("租约内的另一次组票必须被拒")
	} else if errcode.As(err) != errcode.ErrTeamConcurrent {
		t.Fatalf("应为 ErrTeamConcurrent, got=%v", err)
	}
}

// 队长校验挪进锁内后不能丢;ready 门槛已删(2026-08-17),FORMING 队长直接放行。
func TestBeginTeamMatch_锁内仍复核队长(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9623, 7791, 7792)

	// 非队长不得上锁。op 在 matchmaker 按 (team, captain) 派生,非队长天然是另一个 op,
	// 不会命中队长那次 attempt 的收据重入。
	if _, _, err := uc.BeginTeamMatch(ctx, 9623, 7792, "op-member", 5000); err == nil {
		t.Fatal("非队长不得上锁")
	} else if errcode.As(err) != errcode.ErrTeamNotCaptain {
		t.Fatalf("应为 ErrTeamNotCaptain, got=%v", err)
	}
	// ready 不再是门槛:FORMING 队伍队长直接放行。
	if _, _, err := uc.BeginTeamMatch(ctx, 9623, 7791, "op-captain", 5000); err != nil {
		t.Fatalf("FORMING 队伍队长开局应放行: %v", err)
	}
}
