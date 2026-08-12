package service

import (
	"context"
	"testing"

	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	guildv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/guild/v1"
)

// TestGetPlayerGuildRejectsClientCaller 守住内部东西向接口的方向。
//
// Envoy 按 /pandora.guild.v1.GuildService/ 整前缀路由,没有按方法的白名单,带合法玩家 JWT
// 的客户端同样能打到 GetPlayerGuild。少了 systemOnly 这道门,它就是「查任意玩家属于哪个
// 公会」的 IDOR 口子。
//
// nil usecase 证明拒绝发生在触达业务与缓存之前:门若失效,这里会 nil 解引用 panic。
func TestGetPlayerGuildRejectsClientCaller(t *testing.T) {
	svc := NewGuildService(nil, nil, nil)
	authCtx := context.WithValue(context.Background(), plog.CtxKeyPlayerID, uint64(7))

	// 查自己也拒:客户端查自己的公会走 GetMyGuild,身份一律取自 JWT。
	for _, playerID := range []uint64{0, 7, 99} {
		resp, err := svc.GetPlayerGuild(authCtx, &guildv1.GetPlayerGuildRequest{PlayerId: playerID})
		if err != nil {
			t.Fatalf("unexpected transport error: %v", err)
		}
		if resp.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
			t.Fatalf("带玩家 JWT 调内部 RPC 必须拒(player_id=%d): got=%s", playerID, resp.GetCode())
		}
		if resp.GetHasGuild() || resp.GetGuildId() != 0 {
			t.Fatalf("拒绝分支不得泄露任何公会事实(player_id=%d): has_guild=%v guild_id=%d",
				playerID, resp.GetHasGuild(), resp.GetGuildId())
		}
	}
}

// TestGetPlayerGuildRejectsZeroPlayerID 内部调用也必须带 player_id。
//
// 门已放行(ctx 无 player_id),拒绝理由是入参非法。不挡的话 playerID=0 会一路查到反查缓存,
// 把"没有这个 key"当成"这个玩家没公会"返回 OK。
func TestGetPlayerGuildRejectsZeroPlayerID(t *testing.T) {
	svc := NewGuildService(nil, nil, nil)

	resp, err := svc.GetPlayerGuild(context.Background(), &guildv1.GetPlayerGuildRequest{})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
		t.Fatalf("内部调用缺 player_id 必须拒: got=%s", resp.GetCode())
	}
}
