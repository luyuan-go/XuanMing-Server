package service

import (
	"context"
	"testing"

	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// TestGetPlayerTeamRejectsClientCaller 守住内部东西向接口的方向。
//
// Envoy 按 /pandora.team.v1.TeamService/ 整前缀路由,没有按方法的白名单 —— 也就是说带着
// 合法玩家 JWT 的客户端同样能打到 GetPlayerTeam。少了 systemOnly 这道门,它就是一个
// 「查任意玩家在哪支队」的 IDOR 口子。
//
// nil usecase 是本测试的关键:它能证明拒绝发生在触达业务与 Redis 之前 —— 若门没生效,
// 这里会 nil 解引用 panic 而不是安静地返回一个错误码。
func TestGetPlayerTeamRejectsClientCaller(t *testing.T) {
	svc := NewTeamService(nil, nil, nil)
	authCtx := context.WithValue(context.Background(), plog.CtxKeyPlayerID, uint64(7))

	// 连"查自己"都必须拒:本方法的语义是内部读,不是客户端接口。客户端要查自己的队伍
	// 有 GetMyTeam,那条路径的 player_id 一律取自 JWT。
	for _, playerID := range []uint64{0, 7, 99} {
		resp, err := svc.GetPlayerTeam(authCtx, &teamv1.GetPlayerTeamRequest{PlayerId: playerID})
		if err != nil {
			t.Fatalf("unexpected transport error: %v", err)
		}
		if resp.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
			t.Fatalf("带玩家 JWT 调内部 RPC 必须拒(player_id=%d): got=%s", playerID, resp.GetCode())
		}
		if resp.GetHasTeam() || resp.GetTeamId() != 0 {
			t.Fatalf("拒绝分支不得泄露任何队伍事实(player_id=%d): has_team=%v team_id=%d",
				playerID, resp.GetHasTeam(), resp.GetTeamId())
		}
	}
}

// TestGetPlayerTeamRejectsZeroPlayerID 内部调用也必须带 player_id。
//
// 与上面那条的区别:这里 caller 是后端内部(ctx 无 player_id),门已放行,拒绝理由是入参非法。
// 若不挡,playerID=0 会一路查到 Redis 索引,把"没有这个 key"当成"这个玩家没队伍"返回 OK。
func TestGetPlayerTeamRejectsZeroPlayerID(t *testing.T) {
	svc := NewTeamService(nil, nil, nil)

	resp, err := svc.GetPlayerTeam(context.Background(), &teamv1.GetPlayerTeamRequest{})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
		t.Fatalf("内部调用缺 player_id 必须拒: got=%s", resp.GetCode())
	}
}
