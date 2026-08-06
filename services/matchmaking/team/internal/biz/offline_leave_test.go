// offline_leave_test.go — 离线成员自动退队单测(2026-08-06)。
//
// 这个功能踢错人是不可逆的(队伍散了、正在打的局被拆),所以测试重点全在**闸门**上:
// 什么情况下**不许**动手,比"正常情况下能摘掉"重要得多。
package biz

import (
	"context"
	"errors"
	"testing"

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

	if err := uc.OnPlayerOffline(ctx, 7642, 1_000); err != nil {
		t.Fatalf("跳过属正常路径,不该报错: %v", err)
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

	if err := uc.OnPlayerOffline(ctx, 7652, 1_000); err != nil {
		t.Fatalf("跳过属正常路径,不该报错: %v", err)
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
	verdicts map[uint64]offlinewatch.Verdict
	err      error
	enqueued []uint64
	enqErr   error
}

func (f *fakeInspector) Inspect(_ context.Context, ids []uint64) (map[uint64]offlinewatch.Verdict, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[uint64]offlinewatch.Verdict, len(ids))
	for _, id := range ids {
		out[id] = f.verdicts[id]
	}
	return out, nil
}

func (f *fakeInspector) EnqueueDue(_ context.Context, ids []uint64) error {
	if f.enqErr != nil {
		return f.enqErr
	}
	f.enqueued = append(f.enqueued, ids...)
	return nil
}

func TestGetMyTeam_兜底把超时成员排进复查队列(t *testing.T) {
	uc, _ := newOfflineLeaveUsecase(t)
	ctx := context.Background()
	setupTwoMemberTeam(t, uc, 9611, 7711, 7712)
	uc.SetMatchCommitmentReader(&mockCommitment{})

	insp := &fakeInspector{verdicts: map[uint64]offlinewatch.Verdict{
		7711: offlinewatch.VerdictOnline,
		7712: offlinewatch.VerdictOffline,
	}}
	uc.SetPresenceInspector(insp)

	if _, has, err := uc.GetMyTeam(ctx, 7711); err != nil || !has {
		t.Fatalf("GetMyTeam: err=%v has=%v", err, has)
	}
	if len(insp.enqueued) != 1 || insp.enqueued[0] != 7712 {
		t.Fatalf("只有判定为已超时的成员该被排进队列, got=%v", insp.enqueued)
	}
	// 读路径只排队、不动队伍:读请求要快,也要能在依赖抖动时照常返回快照。
	if got := teamMemberIDs(t, uc, 9611); len(got) != 2 {
		t.Fatalf("读路径不得直接摘人, 剩余=%v", got)
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
