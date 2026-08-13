// ds_callback_guard_test.go — GetPlayerGuild 的 DS 回调令牌门。
//
// 与 internal_rpc_auth_test.go 的分工:那边守 systemOnly(挡「带玩家 JWT 的客户端」),
// 这边守 dsGuard(挡「不带玩家 JWT、但也证明不了自己是 DS」的调用方)。
// 与 team 侧 GetPlayerTeam 的门逐条同构 —— 两边任何一侧单独放松都会重新打开
// 「查任意 player_id 的社交归属」这个口子。
package service

import (
	"context"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/transport"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/middleware"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	guildv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/guild/v1"
)

// ── 最小 transport 假件(pkg/middleware 里的同名假件不导出,各包自备)──────────────

type dsFakeHeader map[string][]string

func (h dsFakeHeader) Get(key string) string {
	if vs := h[key]; len(vs) > 0 {
		return vs[0]
	}
	return ""
}
func (h dsFakeHeader) Set(key, value string) { h[key] = []string{value} }
func (h dsFakeHeader) Add(key, value string) { h[key] = append(h[key], value) }
func (h dsFakeHeader) Keys() []string {
	ks := make([]string, 0, len(h))
	for k := range h {
		ks = append(ks, k)
	}
	return ks
}
func (h dsFakeHeader) Values(key string) []string { return h[key] }

type dsFakeTransport struct{ req dsFakeHeader }

func (t *dsFakeTransport) Kind() transport.Kind { return transport.KindGRPC }
func (t *dsFakeTransport) Endpoint() string     { return "" }
func (t *dsFakeTransport) Operation() string {
	return "/pandora.guild.v1.GuildService/GetPlayerGuild"
}
func (t *dsFakeTransport) RequestHeader() transport.Header { return t.req }
func (t *dsFakeTransport) ReplyHeader() transport.Header   { return dsFakeHeader{} }

func dsCtx(headers map[string]string) context.Context {
	h := dsFakeHeader{}
	for k, v := range headers {
		h.Set(k, v)
	}
	return transport.NewServerContext(context.Background(), &dsFakeTransport{req: h})
}

func newEnforceGuard(t *testing.T) (*middleware.DSCallbackGuard, *auth.Signer) {
	t.Helper()
	cfg := auth.Config{
		Issuer:   auth.DSCallbackIssuer,
		Audience: auth.DSCallbackAudience,
		Secret:   []byte("pandora-dev-shared-secret-32bytes!!"),
	}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	g, err := middleware.NewDSCallbackGuard(verifier, middleware.DSAuthEnforce)
	if err != nil {
		t.Fatalf("NewDSCallbackGuard: %v", err)
	}
	return g, signer
}

// TestGetPlayerGuild_RejectsUntokenedEastWestCall 见 team 侧同名测试的理由说明。
//
// nil usecase:证明拒绝发生在触达业务与 MySQL 之前 —— 门若没生效,
// 这里是 nil 解引用 panic,不是安静返回错误码。
func TestGetPlayerGuild_RejectsUntokenedEastWestCall(t *testing.T) {
	guard, _ := newEnforceGuard(t)
	svc := NewGuildService(nil, nil, nil)
	svc.SetDSCallbackGuard(guard)

	for name, ctx := range map[string]context.Context{
		"直连无令牌":    dsCtx(nil),
		"经网关无令牌":   dsCtx(map[string]string{middleware.MetadataKeyDSGateway: "1"}),
		"非 Bearer": dsCtx(map[string]string{"authorization": "Basic abc"}),
		"令牌是垃圾串":   dsCtx(map[string]string{"authorization": "Bearer not-a-jwt"}),
	} {
		resp, err := svc.GetPlayerGuild(ctx, &guildv1.GetPlayerGuildRequest{PlayerId: 42})
		if err != nil {
			t.Fatalf("[%s] unexpected transport error: %v", name, err)
		}
		if resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED {
			t.Fatalf("[%s] 无有效 DS 令牌必须拒: got=%s", name, resp.GetCode())
		}
		if resp.GetHasGuild() || resp.GetGuildId() != 0 {
			t.Fatalf("[%s] 拒绝分支不得泄露任何公会事实: has_guild=%v guild_id=%d",
				name, resp.GetHasGuild(), resp.GetGuildId())
		}
	}
}

// TestGetPlayerGuild_AcceptsAnyValidDSToken 公会反查两种 DS 都要调(战斗里会友关系同样要显示),
// 所以 scope 不绑 ds_type 这一点在这里比队伍侧更是硬需求。
func TestGetPlayerGuild_AcceptsAnyValidDSToken(t *testing.T) {
	guard, signer := newEnforceGuard(t)
	svc := NewGuildService(nil, nil, nil)
	svc.SetDSCallbackGuard(guard)

	hubToken, _, err := signer.SignDSCallback(auth.DSTypeHub, "pandora-hub-0", 0, time.Minute)
	if err != nil {
		t.Fatalf("sign hub token: %v", err)
	}
	battleToken, _, err := signer.SignDSCallback(auth.DSTypeBattle, "", 20260812, time.Minute)
	if err != nil {
		t.Fatalf("sign battle token: %v", err)
	}

	for name, token := range map[string]string{"hub 令牌": hubToken, "battle 令牌": battleToken} {
		ctx := dsCtx(map[string]string{
			"authorization":                 "Bearer " + token,
			middleware.MetadataKeyDSGateway: "1",
		})
		resp, err := svc.GetPlayerGuild(ctx, &guildv1.GetPlayerGuildRequest{})
		if err != nil {
			t.Fatalf("[%s] unexpected transport error: %v", name, err)
		}
		if resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
			t.Fatalf("[%s] 有效 DS 令牌应过门并停在入参校验: got=%s", name, resp.GetCode())
		}
	}
}

// TestGetPlayerGuild_GuardOffPreservesLegacyBehavior 守住「默认关不改变行为」。
func TestGetPlayerGuild_GuardOffPreservesLegacyBehavior(t *testing.T) {
	svc := NewGuildService(nil, nil, nil) // 不调 SetDSCallbackGuard ⇒ dsGuard 为 nil ⇒ mode=off

	resp, err := svc.GetPlayerGuild(dsCtx(nil), &guildv1.GetPlayerGuildRequest{})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
		t.Fatalf("mode=off 时应与接线前一致(停在入参校验): got=%s", resp.GetCode())
	}
}
