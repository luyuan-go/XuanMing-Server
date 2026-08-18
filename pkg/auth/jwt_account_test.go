package auth

import (
	"strings"
	"testing"
	"time"

	jwt "github.com/golang-jwt/jwt/v5"
)

// TestSignAccountRoundTrip:账号态 token 能被 VerifyAccount 解回同一个 account_id。
func TestSignAccountRoundTrip(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s, v := newTestSigner(t, now)

	const accountID uint64 = 7_234_567_890_123
	token, expMs, err := s.SignAccount(accountID, "jti-account-1")
	if err != nil {
		t.Fatalf("SignAccount: %v", err)
	}
	if want := now.Add(s.AccountTTL()).UnixMilli(); expMs != want {
		t.Fatalf("expMs = %d, want %d", expMs, want)
	}

	claims, err := v.VerifyAccount(token)
	if err != nil {
		t.Fatalf("VerifyAccount: %v", err)
	}
	if got := claims.AccountID(); got != accountID {
		t.Fatalf("AccountID = %d, want %d", got, accountID)
	}
	if claims.ID != "jti-account-1" {
		t.Fatalf("jti = %q, want %q", claims.ID, "jti-account-1")
	}
}

// TestAccountAndSessionTokensAreNotInterchangeable 是这次两步登录改造的**安全根基**。
//
// 账号态 token 的 sub 是 account_id;Envoy 的 jwt_authn 会把 sub 通过 claim_to_headers
// 注入 x-pandora-player-id,全后端拿这个头当玩家身份。若两种 token 能互通,一张账号
// token 就能把 account_id 冒充成 player_id,打进 team / chat / inventory 等一切玩家面
// RPC —— 直接越权。隔离靠的是**不同 aud**,而 aud 与签名在同一层校验,伪造不了。
//
// 这条测试红了就说明隔离塌了,不要靠"改测试"绕过去。
func TestAccountAndSessionTokensAreNotInterchangeable(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s, v := newTestSigner(t, now)

	accountToken, _, err := s.SignAccount(4242, "jti-account")
	if err != nil {
		t.Fatalf("SignAccount: %v", err)
	}
	sessionToken, _, err := s.SignSession(4242, "jti-session")
	if err != nil {
		t.Fatalf("SignSession: %v", err)
	}

	if _, err := v.VerifySession(accountToken); err == nil {
		t.Fatal("账号态 token 通过了 VerifySession —— account_id 会被当成 player_id 注入,越权")
	}
	if _, err := v.VerifyAccount(sessionToken); err == nil {
		t.Fatal("玩家态 SessionToken 通过了 VerifyAccount —— 角色态凭据不该解锁整个账号")
	}
}

// TestConfigRejectsSharedAudience:两种 aud 配成同一个值时必须启动期就拒。
// 这是上面那条越权路径在**配置层**的唯一机械拦截点。
func TestConfigRejectsSharedAudience(t *testing.T) {
	cfg := Config{
		Secret:          []byte("pandora-dev-shared-secret-32bytes!!"),
		Audience:        "pandora-client",
		AccountAudience: "pandora-client",
		SessionTTL:      time.Hour,
		DSTicketTTL:     5 * time.Minute,
		AccountTTL:      10 * time.Minute,
	}
	if _, err := NewSigner(cfg); err == nil {
		t.Fatal("NewSigner 接受了 AccountAudience == Audience(越权配置),应当拒绝")
	} else if !strings.Contains(err.Error(), "AccountAudience") {
		t.Fatalf("错误信息没点明是 AccountAudience 配置问题: %v", err)
	}
	if _, err := NewVerifier(cfg); err == nil {
		t.Fatal("NewVerifier 接受了 AccountAudience == Audience(越权配置),应当拒绝")
	}
}

// TestSignAccountRejectsZeroID:account_id=0 是"未知账号"的哨兵值,绝不能签出票。
func TestSignAccountRejectsZeroID(t *testing.T) {
	s, _ := newTestSigner(t, time.Unix(1_700_000_000, 0))
	if _, _, err := s.SignAccount(0, "jti"); err == nil {
		t.Fatal("SignAccount 接受了 accountID=0")
	}
	if _, _, err := s.SignAccount(1, ""); err == nil {
		t.Fatal("SignAccount 接受了空 jti")
	}
}

// TestAccountTokenExpires:账号态 token 过期后必须被拒(短窗口是它的安全前提)。
func TestAccountTokenExpires(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	s, _ := newTestSigner(t, now)
	token, _, err := s.SignAccount(99, "jti-exp")
	if err != nil {
		t.Fatalf("SignAccount: %v", err)
	}

	later := now.Add(s.AccountTTL() + time.Minute)
	vLater, err := NewVerifier(Config{
		Secret:      []byte("pandora-dev-shared-secret-32bytes!!"),
		SessionTTL:  time.Hour,
		DSTicketTTL: 5 * time.Minute,
		NowFn:       func() time.Time { return later },
	})
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	if _, err := vLater.VerifyAccount(token); err == nil {
		t.Fatal("过期的账号态 token 仍被接受")
	}
}

// TestAccountTokenCarriesNoKidHeader 钉住「不设 kid」这个决定。
//
// 账号态 token 要经 Envoy jwt_authn 校验,而 envoy.yaml 的 local_jwks 里 kid 是固定
// 字面量 "pandora-dev",不是密钥指纹。一旦签发侧写进指纹 kid,Envoy 会按 kid 精确找
// key、找不到直接拒 —— 账号态入口全线 401,且这种失败只在过 Envoy 时出现,Go 单测
// 全绿,极难定位。
func TestAccountTokenCarriesNoKidHeader(t *testing.T) {
	s, _ := newTestSigner(t, time.Unix(1_700_000_000, 0))
	token, _, err := s.SignAccount(1234, "jti-kid")
	if err != nil {
		t.Fatalf("SignAccount: %v", err)
	}
	parsed, _, err := jwt.NewParser().ParseUnverified(token, jwt.MapClaims{})
	if err != nil {
		t.Fatalf("ParseUnverified: %v", err)
	}
	if kid, ok := parsed.Header["kid"]; ok {
		t.Fatalf("账号态 token 带了 kid=%v,过 Envoy jwt_authn 会因 JWKS kid 不匹配被拒", kid)
	}
}
