// release_end_team_match_test.go — ReleaseMatch 必须复位各队准备状态
// (INC-20260813-001 v2 第一根因的 matchmaker 那一半)。
//
// BeginTeamMatch 冻结名单开一局，ReleaseMatch 就必须把队伍打回 FORMING。
// **少了 End 那一半**，一局打完队伍仍停在 READY、全员 ready 标记原样保留，
// 队长可以在队友还卡在结算界面 / 回大厅路上的时候立刻再开一局。
package biz

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/luyuancpp/pandora/pkg/errcode"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// recordingTeamReader 记录 EndTeamMatch 的调用，可注入失败。
type recordingTeamReader struct {
	mu       sync.Mutex
	ended    map[uint64][]uint64
	endedGen map[uint64]uint64
	calls    int
	err      error
}

func newRecordingTeamReader() *recordingTeamReader {
	return &recordingTeamReader{ended: map[uint64][]uint64{}, endedGen: map[uint64]uint64{}}
}

func (r *recordingTeamReader) GetTeam(context.Context, uint64) (*teamv1.Team, bool, error) {
	return nil, false, nil
}

func (r *recordingTeamReader) BeginTeamMatch(context.Context, uint64, uint64, string, int64) (*teamv1.Team, uint64, error) {
	return nil, 0, errors.New("not used in these tests")
}

func (r *recordingTeamReader) EndTeamMatch(_ context.Context, teamID uint64, playerIDs []uint64, expectedGen uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.err != nil {
		return r.err
	}
	r.ended[teamID] = append([]uint64(nil), playerIDs...)
	r.endedGen[teamID] = expectedGen
	return nil
}

func (r *recordingTeamReader) snapshot() (map[uint64][]uint64, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[uint64][]uint64, len(r.ended))
	for k, v := range r.ended {
		out[k] = append([]uint64(nil), v...)
	}
	return out, r.calls
}

func TestReleaseMatch_按队复位准备状态(t *testing.T) {
	f := newFixture(t, 9000)
	reader := newRecordingTeamReader()
	f.uc.reader = reader
	ctx := context.Background()

	const matchID = uint64(555001)
	// 两队各 2 人 + 一个没有队伍的单人入口成员（team_id=0，必须被跳过）。
	if err := f.repo.CreateMatch(ctx, newTwoTeamMatchRecord(matchID), f.cfg.MatchTTL.Std()); err != nil {
		t.Fatalf("seed match: %v", err)
	}

	if err := f.uc.ReleaseMatch(ctx, matchID, nil); err != nil {
		t.Fatalf("ReleaseMatch: %v", err)
	}

	ended, calls := reader.snapshot()
	if calls != 2 {
		t.Fatalf("两支队伍各复位一次(team_id=0 的单人入口必须跳过): calls=%d", calls)
	}
	if got := ended[8001]; len(got) != 2 {
		t.Fatalf("team 8001 应带上本局它那两名成员: %v", got)
	}
	if got := ended[8002]; len(got) != 2 {
		t.Fatalf("team 8002 应带上本局它那两名成员: %v", got)
	}
	if _, ok := ended[0]; ok {
		t.Fatal("team_id=0 是单人入口,没有队伍可复位,不得调用")
	}
}

// 复位失败必须向上抛,让 battle_result 的 outbox 重投 —— 吞掉就等于队伍永远停在 READY。
// 同时 match 镜像必须保留,否则下一轮重投拿不到 roster。
func TestReleaseMatch_复位失败保留镜像并上抛(t *testing.T) {
	f := newFixture(t, 9000)
	reader := newRecordingTeamReader()
	reader.err = errors.New("team unavailable")
	f.uc.reader = reader
	ctx := context.Background()

	const matchID = uint64(555002)
	if err := f.repo.CreateMatch(ctx, newTwoTeamMatchRecord(matchID), f.cfg.MatchTTL.Std()); err != nil {
		t.Fatalf("seed match: %v", err)
	}

	if err := f.uc.ReleaseMatch(ctx, matchID, nil); err == nil {
		t.Fatal("复位失败必须上抛,否则队伍永远停在 READY 且无人重试")
	}
	if _, found, err := f.repo.GetMatch(ctx, matchID); err != nil || !found {
		t.Fatalf("复位未成功时必须保留 canonical match 供 outbox 重投: found=%v err=%v", found, err)
	}

	// team 恢复后重投:必须成功并删掉镜像。
	reader.err = nil
	if err := f.uc.ReleaseMatch(ctx, matchID, nil); err != nil {
		t.Fatalf("重投应成功: %v", err)
	}
	if _, found, err := f.repo.GetMatch(ctx, matchID); err != nil || found {
		t.Fatalf("成功后必须删镜像: found=%v err=%v", found, err)
	}
}

// ★ 滚动升级共存窗口(§9.21):team 还没滚到带 EndTeamMatch 的版本时,
// **必须弱依赖降级放行**,而不是把 ReleaseMatch 卡住。
//
// 若在这里 fail-closed,就等于给发布引入一条「team 必须先于 matchmaker 上线」的顺序约束;
// 而顺序搞错的后果是 outbox 无限空转 + canonical match 持续积压,且没有任何机械手段能拦住。
// 降级本身是安全的:跳过的后果恰好等于本修复落地之前的行为(队伍停在 READY),
// 不产生任何新的错误状态;team 升级后此后每一局都会正常复位。
func TestReleaseMatch_对端未实现时降级放行(t *testing.T) {
	f := newFixture(t, 9000)
	reader := newRecordingTeamReader()
	reader.err = errcode.New(errcode.ErrNotImplemented, "peer has no EndTeamMatch")
	f.uc.reader = reader
	ctx := context.Background()

	const matchID = uint64(555004)
	if err := f.repo.CreateMatch(ctx, newTwoTeamMatchRecord(matchID), f.cfg.MatchTTL.Std()); err != nil {
		t.Fatalf("seed match: %v", err)
	}

	if err := f.uc.ReleaseMatch(ctx, matchID, nil); err != nil {
		t.Fatalf("共存窗口必须降级放行,否则发布顺序一错就会积压: %v", err)
	}
	// 释放链必须走完 —— 镜像删掉才说明没有积压。
	if _, found, err := f.repo.GetMatch(ctx, matchID); err != nil || found {
		t.Fatalf("降级放行后必须照常删镜像(不积压): found=%v err=%v", found, err)
	}
}

// team_addr 未配(reader==nil,骨架联调)时整段跳过,行为与本链落地前一致。
func TestReleaseMatch_未注入reader时跳过(t *testing.T) {
	f := newFixture(t, 9000)
	ctx := context.Background()
	const matchID = uint64(555003)
	if err := f.repo.CreateMatch(ctx, newTwoTeamMatchRecord(matchID), f.cfg.MatchTTL.Std()); err != nil {
		t.Fatalf("seed match: %v", err)
	}
	if err := f.uc.ReleaseMatch(ctx, matchID, nil); err != nil {
		t.Fatalf("未注入 reader 时 ReleaseMatch 必须照常成功: %v", err)
	}
}

// collectTeamRosters 的分组 / 去重 / 跳过 team_id==0 —— 「只复位本局这支队的这些人」
// 全押在它身上。
func TestCollectTeamRosters(t *testing.T) {
	out := map[uint64]*teamRoster{}
	collectTeamRosters(out, matchMembers(
		member{8001, 1}, member{8001, 2}, member{8002, 3},
		member{0, 4},    // 单人入口:没有队伍
		member{8001, 1}, // 重复:必须去重
		member{8003, 0}, // 非法 player_id:跳过
	))
	if len(out) != 2 {
		t.Fatalf("应只有两支队伍: %v", out)
	}
	if got := out[8001].players; len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("team 8001 去重后应是 [1 2]: %v", got)
	}
	if got := out[8002].players; len(got) != 1 || got[0] != 3 {
		t.Fatalf("team 8002 应是 [3]: %v", got)
	}
	if _, ok := out[0]; ok {
		t.Fatal("team_id==0 不得成组")
	}
	if _, ok := out[8003]; ok {
		t.Fatal("player_id==0 不得成组")
	}
}

// 代际必须跟着 roster 一起被收集起来 —— 它是 EndTeamMatch 跨代 CAS 的唯一凭据,
// 丢了就退化成「只看谁还挂着 ready」,重投会抹掉玩家的新意图。
func TestCollectTeamRosters_带上ready代际(t *testing.T) {
	out := map[uint64]*teamRoster{}
	collectTeamRosters(out, []*matchv1.MatchMemberStorageRecord{
		{TeamId: 8001, PlayerId: 1, TeamReadyGeneration: 7},
		{TeamId: 8001, PlayerId: 2, TeamReadyGeneration: 7}, // 同队必然同值
		{TeamId: 8002, PlayerId: 3},                         // 旧记录:代际为 0
	})
	if got := out[8001].readyGeneration; got != 7 {
		t.Fatalf("team 8001 代际应是 7: %d", got)
	}
	if got := out[8002].readyGeneration; got != 0 {
		t.Fatalf("旧记录代际应保持 0(退化语义): %d", got)
	}
}

// ── 测试夹具 ────────────────────────────────────────────────────────────────

type member struct {
	teamID   uint64
	playerID uint64
}

func matchMembers(ms ...member) []*matchv1.MatchMemberStorageRecord {
	out := make([]*matchv1.MatchMemberStorageRecord, 0, len(ms))
	for _, m := range ms {
		out = append(out, &matchv1.MatchMemberStorageRecord{TeamId: m.teamID, PlayerId: m.playerID})
	}
	return out
}

// newTwoTeamMatchRecord 造一局「两队各 2 人 + 一个单人入口成员」的 match 镜像。
// 单人入口那位(team_id=0)是刻意放的:他没有队伍可复位,必须被跳过。
func newTwoTeamMatchRecord(matchID uint64) *matchv1.MatchStorageRecord {
	return &matchv1.MatchStorageRecord{
		MatchId: matchID,
		Stage:   matchv1.MatchStage_MATCH_STAGE_READY,
		Members: matchMembers(
			member{8001, 7901}, member{8001, 7902},
			member{8002, 7903}, member{8002, 7904},
			member{0, 7905},
		),
	}
}
