// ds_callback_guard_test.go — GetLoadout 的 DS 回调令牌门(2026-08-13)。
//
// 起因:GetLoadout 这天被挂上了 Envoy DS 面(:8444)精确路由,供 Hub/Battle DS 在
// PlayerState 初始化时拉专精与预设装备。那个监听器没有 jwt_authn,经它进来的调用
// callerID 恒为 0 —— resolvePlayerID 的「callerID==0 即后端内部可信,信请求体 player_id」
// 于是在本方法上失效:任何能连 :8444、或绕过 Envoy 直连 20002 的进程都能拿任意玩家的
// 出战快照(装备实例 ID / 词条 / 天赋)。本文件把「callerID==0 必须自证是 DS」钉死。
//
// 与 player_rpc_boundary_matrix_test.go 的分工:那边守 resolvePlayerID(挡「带玩家 JWT
// 的客户端读他人存档」),这边守 dsGuard(挡「不带玩家 JWT、也证明不了自己是 DS」的调用方)。
// 与 team.GetPlayerTeam / guild.GetPlayerGuild 的同名门逐条同构 —— 三处任何一处单独放松,
// 都会重新打开「查任意 player_id 的玩家数据」这个口子。
package service

import (
	"context"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/transport"

	"github.com/luyuancpp/pandora/pkg/auth"
	plog "github.com/luyuancpp/pandora/pkg/log"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	playerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/player/v1"
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
	return "/pandora.player.v1.PlayerService/GetLoadout"
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

// newEnforceGuard 造一把真验签的 enforce 守卫(不用 stub:令牌验签本身就是被测行为的一部分)。
func newEnforceGuard(t *testing.T) (*pmw.DSCallbackGuard, *auth.Signer) {
	t.Helper()
	signer, err := auth.NewSigner(auth.Config{
		Issuer:   auth.DSCallbackIssuer,
		Audience: auth.DSCallbackAudience,
		Secret:   []byte("pandora-dev-shared-secret-32bytes!!"),
	})
	if err != nil {
		t.Fatalf("new signer: %v", err)
	}
	verifier, err := auth.NewDSCallbackVerifier(auth.Config{
		Issuer:   auth.DSCallbackIssuer,
		Audience: auth.DSCallbackAudience,
		Secret:   []byte("pandora-dev-shared-secret-32bytes!!"),
	})
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	guard, err := pmw.NewDSCallbackGuard(verifier, pmw.DSAuthEnforce)
	if err != nil {
		t.Fatalf("new guard: %v", err)
	}
	return guard, signer
}

// 无玩家 JWT 又无有效 DS 令牌 = 既不是客户端也证明不了是 DS,一律拒。
// 「直连无令牌」这一条尤其重要:它守的是绕过 Envoy 直连 20002 的旁路,
// RequireToken 就是为它而设,去掉它这个门只剩半扇。
func TestGetLoadout_RejectsUntokenedCallWithoutPlayerJWT(t *testing.T) {
	guard, _ := newEnforceGuard(t)
	svc := NewPlayerService(nil)
	svc.SetDSCallbackGuard(guard)

	for name, ctx := range map[string]context.Context{
		"直连无令牌":    dsCtx(nil),
		"经网关无令牌":   dsCtx(map[string]string{pmw.MetadataKeyDSGateway: "1"}),
		"非 Bearer": dsCtx(map[string]string{"authorization": "Basic abc"}),
		"令牌是垃圾串":   dsCtx(map[string]string{"authorization": "Bearer not-a-jwt"}),
	} {
		resp, err := svc.GetLoadout(ctx, &playerv1.GetLoadoutRequest{PlayerId: 42})
		if err != nil {
			t.Fatalf("[%s] unexpected transport error: %v", name, err)
		}
		if resp.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED {
			t.Fatalf("[%s] 无有效 DS 令牌必须拒: got=%s", name, resp.GetCode())
		}
		if resp.GetLoadout() != nil {
			t.Fatalf("[%s] 拒绝分支不得泄露任何出战快照", name)
		}
	}
}

// Hub 与 Battle DS 都在进场时查这一次(大厅要显示专精/装备,战斗要按预设结算),
// 所以 scope 不绑 ds_type / pod / match_id —— 这两种令牌都必须过门。
func TestGetLoadout_AcceptsAnyValidDSToken(t *testing.T) {
	guard, signer := newEnforceGuard(t)
	svc := NewPlayerService(nil)
	svc.SetDSCallbackGuard(guard)

	hubToken, _, err := signer.SignDSCallback(auth.DSTypeHub, "pandora-hub-0", 0, time.Minute)
	if err != nil {
		t.Fatalf("sign hub token: %v", err)
	}
	battleToken, _, err := signer.SignDSCallback(auth.DSTypeBattle, "", 20260813, time.Minute)
	if err != nil {
		t.Fatalf("sign battle token: %v", err)
	}

	for name, token := range map[string]string{"hub 令牌": hubToken, "battle 令牌": battleToken} {
		ctx := dsCtx(map[string]string{
			"authorization":          "Bearer " + token,
			pmw.MetadataKeyDSGateway: "1",
		})
		// 空 player_id:过门后停在 resolvePlayerID 的入参校验,不碰 usecase(uc 为 nil)。
		resp, err := svc.GetLoadout(ctx, &playerv1.GetLoadoutRequest{})
		if err != nil {
			t.Fatalf("[%s] unexpected transport error: %v", name, err)
		}
		if resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
			t.Fatalf("[%s] 有效 DS 令牌应过门并停在入参校验: got=%s", name, resp.GetCode())
		}
	}
}

// 回归钉子:带玩家 JWT 的客户端调用**不得**进 DS 令牌门。
//
// 客户端自助查自己的出战快照走 :8443,其 authorization 头里是**玩家** JWT;
// 若把守卫写成无条件调用,VerifyDSCallback 会拿玩家 JWT 去验 DS 回调签名并必然失败,
// 整个客户端出战面板会被这道本不该管它的门拒成 ERR_UNAUTHORIZED。
// 这里用「请求体 player_id 与调用者不一致」验收:能拿到 ERR_PERMISSION_DENY 就同时证明了
// ①没被 DS 门拦住 ②resolvePlayerID 的自读限制仍然生效。
func TestGetLoadout_PlayerJWTCallBypassesDSGuard(t *testing.T) {
	guard, _ := newEnforceGuard(t)
	svc := NewPlayerService(nil)
	svc.SetDSCallbackGuard(guard)

	ctx := context.WithValue(dsCtx(nil), plog.CtxKeyPlayerID, uint64(42))
	resp, err := svc.GetLoadout(ctx, &playerv1.GetLoadoutRequest{PlayerId: 99})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.GetCode() == commonv1.ErrCode_ERR_UNAUTHORIZED {
		t.Fatal("带玩家 JWT 的客户端调用被 DS 令牌门拦下了:守卫必须只在 callerID==0 时生效")
	}
	if resp.GetCode() != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("客户端读他人出战快照必须拒: got=%s", resp.GetCode())
	}
}

// 守住「显式关闭不改变行为」:直接构造服务且不注入守卫时保持旧行为。
func TestGetLoadout_GuardOffPreservesLegacyBehavior(t *testing.T) {
	svc := NewPlayerService(nil) // 不调 SetDSCallbackGuard ⇒ dsGuard 为 nil ⇒ mode=off

	resp, err := svc.GetLoadout(dsCtx(nil), &playerv1.GetLoadoutRequest{})
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if resp.GetCode() != commonv1.ErrCode_ERR_INVALID_ARG {
		t.Fatalf("mode=off 时应与接线前一致(停在入参校验): got=%s", resp.GetCode())
	}
}
