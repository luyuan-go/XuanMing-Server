// ds_callback_guard_test.go — GetPlayerTeam 的 DS 回调令牌门。
//
// 与 internal_rpc_auth_test.go 的分工:那边守的是 systemOnly(挡「带玩家 JWT 的客户端」),
// 这边守的是 dsGuard(挡「不带玩家 JWT、但也证明不了自己是 DS」的调用方)。
// 两道门缺一不可:systemOnly 只能证明 callerID==0,而 :8444 没有 jwt_authn,任何能连到
// 网关或直连本服务 20010 端口的东西都满足 callerID==0。
package service

import (
	"context"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/transport"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/middleware"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// ── 最小 transport 假件 ─────────────────────────────────────────────────────────
// pkg/middleware 里的同名假件不导出,各包只能自备一份。

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

func (t *dsFakeTransport) Kind() transport.Kind            { return transport.KindGRPC }
func (t *dsFakeTransport) Endpoint() string                { return "" }
func (t *dsFakeTransport) Operation() string               { return "/pandora.team.v1.TeamService/GetPlayerTeam" }
func (t *dsFakeTransport) RequestHeader() transport.Header { return t.req }
func (t *dsFakeTransport) ReplyHeader() transport.Header   { return dsFakeHeader{} }

func dsCtx(headers map[string]string) context.Context {
	h := dsFakeHeader{}
	for k, v := range headers {
		h.Set(k, v)
	}
	return transport.NewServerContext(context.Background(), &dsFakeTransport{req: h})
}

// newEnforceGuard 造一把真验签的 enforce 守卫(不用 stub:令牌验签本身就是被测行为的一部分)。
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

// TestGetPlayerTeam_RejectsUntokenedEastWestCall 是本次修复的核心回归。
//
// scope.RequireToken=true 的含义:本方法**只可能**来自 DS —— 全仓没有任何内部 Go 服务调它。
// 因此即使请求不经 :8444(没有网关标记头),没有有效令牌也一律拒。这一条堵的是
// 「被攻破的业务 Pod 绕过 Envoy 直连 20010、无标记无令牌却被当东西向内部信任」的旁路。
// 若哪天有人把 RequireToken 去掉,这里会立刻红。
//
// nil usecase 同 internal_rpc_auth_test 的用法:证明拒绝发生在触达业务与 Redis 之前 ——
// 门若没生效,这里是 nil 解引用 panic,不是安静返回错误码。
func TestGetPlayerTeam_RejectsUntokenedEastWestCall(t *testing.T) {
	guard, _ := newEnforceGuard(t)
	svc := NewTeamService(nil, nil, nil)
	svc.SetDSCallbackGuard(guard)

	for name, ctx := range map[string]context.Context{
		"直连无令牌":    dsCtx(nil),
		"经网关无令牌":   dsCtx(map[string]string{middleware.MetadataKeyDSGateway: "1"}),
		"非 Bearer": dsCtx(map[string]string{"authorization": "Basic abc"}),
		"令牌是垃圾串":   dsCtx(map[string]string{"authorization": "Bearer not-a-jwt"}),
	} {
		resp, err := svc.GetPlayerTeam(ctx, &teamv1.GetPlayerTeamRequest{PlayerId: 42})
		if err != nil {
			t.Fatalf("[%s] unexpected transport error: %v", name, err)
		}
		if resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED {
			t.Fatalf("[%s] 无有效 DS 令牌必须拒: got=%s", name, resp.GetCode())
		}
		if resp.GetHasTeam() || resp.GetTeamId() != 0 {
			t.Fatalf("[%s] 拒绝分支不得泄露任何队伍事实: has_team=%v team_id=%d",
				name, resp.GetHasTeam(), resp.GetTeamId())
		}
	}
}

// TestGetPlayerTeam_AcceptsAnyValidDSToken 钉死「不绑 Type/Pod/MatchID」这个刻意的选择。
//
// 队伍反查在修复后只有大厅会发,但 scope 仍不绑 ds_type:令牌本身已证明调用方是 DS,
// 再绑一层的安全收益是零,却会让将来任何新玩法的合法查询变成鉴权失败。
// 用 playerID=0 触发入参门:能走到 ERR_INVALID_ARG 就证明令牌门已放行,且全程没碰 nil usecase。
func TestGetPlayerTeam_AcceptsAnyValidDSToken(t *testing.T) {
	guard, signer := newEnforceGuard(t)
	svc := NewTeamService(nil, nil, nil)
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
		resp, err := svc.GetPlayerTeam(ctx, &teamv1.GetPlayerTeamRequest{})
		if err != nil {
			t.Fatalf("[%s] unexpected transport error: %v", name, err)
		}
		if resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
			t.Fatalf("[%s] 有效 DS 令牌应过门并停在入参校验: got=%s", name, resp.GetCode())
		}
	}
}

// TestGetPlayerTeam_GuardOffPreservesLegacyBehavior 守住「默认关不改变行为」。
//
// ds_auth.mode 默认 off → main 拿到 nil guard → Check 直接放行。接线本身不得改变
// 未启用时的任何行为,否则这次改动会在所有还没配 ds_auth 的环境里变成一次静默的准入收紧。
func TestGetPlayerTeam_GuardOffPreservesLegacyBehavior(t *testing.T) {
	svc := NewTeamService(nil, nil, nil) // 不调 SetDSCallbackGuard ⇒ dsGuard 为 nil ⇒ mode=off

	resp, err := svc.GetPlayerTeam(dsCtx(nil), &teamv1.GetPlayerTeamRequest{})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
		t.Fatalf("mode=off 时应与接线前一致(停在入参校验): got=%s", resp.GetCode())
	}
}
