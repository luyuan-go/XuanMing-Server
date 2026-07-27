// login_delivery_fence_test.go — Login 副作用交付终检回归(R5 复审 P0-5,INC-20260722-004)。
//
// 场景:完整 Login 在 sessions.Set(轮换点)之后还有分配/locator/签票多步副作用,
// 期间并发新登录 B 再次轮换 jti。旧流程 A 交付前必须复核本次写入的 jti 仍是当前一代:
//   - 已被轮换 → ErrSessionSuperseded,不返回任何 token/票据;
//   - 权威不可达 → ErrUnavailable(fail-closed 扣留凭据);
//   - 未被轮换 → 正常交付(终检不误杀)。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	"github.com/luyuancpp/pandora/pkg/snowflake"
)

// rotatingSessionRepo 在 Set 完成后模拟并发新登录:GetJTI 恒返回"更新一代"的 jti
// (与本流程写入值必然不同),复现「检查已过、交付未发生」窗口内的会话轮换。
type rotatingSessionRepo struct {
	fakeSessionRepo
	rotatedJTI string
	getErr     error
}

func (r *rotatingSessionRepo) GetJTI(_ context.Context, _ uint64) (string, bool, error) {
	if r.getErr != nil {
		return "", false, r.getErr
	}
	return r.rotatedJTI, r.rotatedJTI != "", nil
}

func newFenceUsecase(t *testing.T, sessions interface {
	Set(ctx context.Context, playerID uint64, token, jti, deviceID string, ttl time.Duration, gen uint64) error
	Delete(ctx context.Context, playerID uint64) error
	GetJTI(ctx context.Context, playerID uint64) (string, bool, error)
	DeleteIfJTI(ctx context.Context, playerID uint64, jti string) (bool, error)
	FenceFailedSet(ctx context.Context, playerID uint64, jti string, gen uint64, ttl time.Duration) (bool, error)
}) *LoginUsecase {
	t.Helper()
	cfg := auth.Config{Secret: []byte(testSecret)}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}
	repo := &fakeAccountRepo{playerID: 42, passwordHash: mustBcrypt(t, "pw")}
	sf := snowflake.NewNode(1)
	return NewLoginUsecase(repo, sessions, nil, nil, nil, sf, "127.0.0.1:7777", "cn", signer, verifier, nil, false, false, nil, false)
}

// 交付前发现 jti 已被并发登录轮换:必须 ErrSessionSuperseded 且零凭据返回。
func TestLogin_DeliveryFencedWhenSessionRotatedMidFlight(t *testing.T) {
	uc := newFenceUsecase(t, &rotatingSessionRepo{rotatedJTI: "jti-of-device-B"})

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err == nil {
		t.Fatal("P0-5: rotated-mid-flight login must not deliver credentials")
	}
	if errcode.As(err) != errcode.ErrSessionSuperseded {
		t.Fatalf("fence must be discriminable (ErrSessionSuperseded), got: %v", err)
	}
	if res != nil {
		t.Fatalf("no LoginResult may leak past the delivery fence, got: %+v", res)
	}
}

// 交付前会话已消失(并发登出/TTL):同样扣留凭据(不可按"仍现行"猜测)。
func TestLogin_DeliveryFencedWhenSessionVanished(t *testing.T) {
	uc := newFenceUsecase(t, &rotatingSessionRepo{rotatedJTI: ""})

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err == nil || res != nil {
		t.Fatalf("vanished session must withhold credentials, res=%+v err=%v", res, err)
	}
	if errcode.As(err) != errcode.ErrSessionSuperseded {
		t.Fatalf("vanished-at-delivery maps to ErrSessionSuperseded, got: %v", err)
	}
}

// 权威不可达:fail-closed 扣留凭据(ErrUnavailable,客户端整体重试)。
func TestLogin_DeliveryFenceAuthorityDownFailClosed(t *testing.T) {
	uc := newFenceUsecase(t, &rotatingSessionRepo{
		getErr: errcode.New(errcode.ErrUnavailable, "session authority down"),
	})

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err == nil || res != nil {
		t.Fatalf("authority-down at delivery must fail-closed, res=%+v err=%v", res, err)
	}
	if errcode.As(err) != errcode.ErrUnavailable {
		t.Fatalf("authority-down maps to ErrUnavailable, got: %v", err)
	}
}

// 未被轮换:终检放行,凭据正常交付(不误杀唯一登录流程)。
func TestLogin_DeliveryFencePassesForCurrentSession(t *testing.T) {
	uc := newFenceUsecase(t, newFakeSessionRepo())

	res, err := uc.Login(context.Background(), "acc", "pw", "device-A")
	if err != nil {
		t.Fatalf("sole login flow must pass the delivery fence: %v", err)
	}
	if res == nil || res.SessionToken == "" {
		t.Fatalf("credentials must be delivered for the current session, res=%+v", res)
	}
}
