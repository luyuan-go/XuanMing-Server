// player_team_membership_test.go — DS 队伍反查以成员表为准,不以索引为准。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/data"
)

// newMembershipUsecase 起一个 miniredis 支撑的真 repo(与本包其余用例同款)。
// 返回 repo 是因为本用例要直接写索引,精确复现「删索引那一步失败过」的残留形状。
func newMembershipUsecase(t *testing.T) (*TeamUsecase, data.TeamRepo, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	repo := data.NewRedisTeamRepo(rdb)

	var cfg conf.Config
	cfg.Defaults()

	uc := NewTeamUsecase(repo, nil, cfg.Team)
	return uc, repo, func() {
		_ = rdb.Close()
		mr.Close()
	}
}

// TestGetPlayerTeamID_RejectsIndexPointingAtNonMember 钉死「成员表才是权威」。
//
// 残留怎么来的:退队 / 被踢 / 离线清扫都会走 DeletePlayerIndexIfMatches 删索引,
// 但那一步删失败时只打一条 warn —— 不回滚已经生效的退队(best-effort 是对的,
// 为了删缓存失败去回滚一次成功的退队才是更坏的设计)。于是留下这样一个窗口:
// 索引还指向队伍 T,T 也确实存在、未解散,只是这名玩家早已不在 T 的成员表里。
//
// 只校验「T 存在且未解散」放不掉这种残留 —— 那正是本次加固前的行为。后果不对称:
// DS 只在进场时查这一次,一个已退队的人会被整场显示成队友且永不纠正。
func TestGetPlayerTeamID_RejectsIndexPointingAtNonMember(t *testing.T) {
	uc, repo, cleanup := newMembershipUsecase(t)
	defer cleanup()
	ctx := context.Background()

	const (
		teamID     = uint64(9001)
		captainID  = uint64(1)
		strangerID = uint64(99) // 从来不是、或早已不是这支队伍的成员
	)

	if _, err := uc.CreateTeam(ctx, teamID, captainID); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}

	// 前置:队长自己查得到,证明这条链本身是通的(否则下面那条断言没有意义)。
	gotID, hasTeam, err := uc.GetPlayerTeamID(ctx, captainID)
	if err != nil {
		t.Fatalf("GetPlayerTeamID(captain): %v", err)
	}
	if !hasTeam || gotID != teamID {
		t.Fatalf("前置不成立:队长应查到自己的队伍 want=%d, got has=%v id=%d", teamID, hasTeam, gotID)
	}

	// 精确复现残留:索引指向一支**存在且未解散**的队伍,但该玩家不在其成员表里。
	if err := repo.SetPlayerIndex(ctx, strangerID, teamID, time.Hour); err != nil {
		t.Fatalf("SetPlayerIndex: %v", err)
	}

	gotID, hasTeam, err = uc.GetPlayerTeamID(ctx, strangerID)
	if err != nil {
		t.Fatalf("GetPlayerTeamID(stranger): %v", err)
	}
	if hasTeam || gotID != 0 {
		t.Fatalf("索引残留必须按无队伍处理(否则路人整场被显示成队友): got has=%v id=%d", hasTeam, gotID)
	}
}

// TestGetPlayerTeamID_AcceptsRealMember 反向对照:成员校验不得误伤正常成员。
//
// 只有上面那条会因为「一律返回 has=false」而通过,所以必须有这条把它钉住 ——
// 否则把成员校验写成恒假也能让上一条转绿。
func TestGetPlayerTeamID_AcceptsRealMember(t *testing.T) {
	uc, _, cleanup := newMembershipUsecase(t)
	defer cleanup()
	ctx := context.Background()

	const (
		teamID    = uint64(9002)
		captainID = uint64(2)
		memberID  = uint64(3)
	)

	if _, err := uc.CreateTeam(ctx, teamID, captainID); err != nil {
		t.Fatalf("CreateTeam: %v", err)
	}
	// InviteId=0 走服务端仅校验 TeamId 的兼容路径(与本包其余用例同款入队方式)。
	if _, err := uc.AcceptInvite(ctx, 0, teamID, memberID); err != nil {
		t.Fatalf("AcceptInvite: %v", err)
	}

	for name, playerID := range map[string]uint64{"队长": captainID, "普通成员": memberID} {
		gotID, hasTeam, err := uc.GetPlayerTeamID(ctx, playerID)
		if err != nil {
			t.Fatalf("[%s] GetPlayerTeamID: %v", name, err)
		}
		if !hasTeam || gotID != teamID {
			t.Fatalf("[%s] 真实成员必须查得到队伍 want=%d: got has=%v id=%d", name, teamID, hasTeam, gotID)
		}
	}
}
