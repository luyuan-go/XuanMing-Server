// login_session_jti_test.go — RequireCurrentSessionJTI(SelectRole 会话现行性门,2026-07-18)。
//
// 该门封 battle-reconnect.md 已知边界「SelectRole 请求体无 token,顶号后旧设备仍可拿 hub 票」:
// jti 来自 Envoy jwt_authn 验签后重写的 x-pandora-jwt-payload 头,与 IssueDSTicket 的
// 请求体 token 走同一 requireCurrentSession 判定。
package biz

import (
	"context"
	"testing"
	"time"

	"github.com/luyuancpp/pandora/pkg/errcode"
)

// jtiSessionRepo 是可控现行性判定的 SessionRepo fake。
type jtiSessionRepo struct {
	// 字段含义:
	// - cur 模拟 Redis 中该玩家当前生效的会话 jti;
	// - found 区分“权威记录存在”与“会话已过期或已登出”;
	// - err 注入 Redis 超时等权威读取故障,验证 fail-closed 分支。
	cur   string
	found bool
	err   error
}

func (f *jtiSessionRepo) Set(_ context.Context, _ uint64, _, _, _ string, _ time.Duration, _ uint64) error {
	return nil
}
func (f *jtiSessionRepo) Delete(_ context.Context, _ uint64) error { return nil }
func (f *jtiSessionRepo) GetJTI(_ context.Context, _ uint64) (string, bool, error) {
	return f.cur, f.found, f.err
}
func (f *jtiSessionRepo) DeleteIfJTI(_ context.Context, _ uint64, _ string) (bool, error) {
	return true, nil
}
func (f *jtiSessionRepo) FenceFailedSet(_ context.Context, _ uint64, _ string, _ uint64, _ time.Duration) (bool, error) {
	return true, nil
}

// newSessionJTIUsecase 构造只包含会话现行性门所需依赖的测试用例对象。
// sessions 控制会话权威状态；strict 控制 B1 严格档与 local/off 兼容档。
func newSessionJTIUsecase(t *testing.T, sessions *jtiSessionRepo, strict bool) *LoginUsecase {
	t.Helper()
	// signer 与 verifier 使用同一测试密钥对，满足 LoginUsecase 构造约束而不参与本组断言。
	signer, verifier := newTicketTestPair(t)
	// uc 保存按当前夹具装配出的 LoginUsecase，两个分支只改变 session 仓库是否注入。
	var uc *LoginUsecase
	if sessions == nil {
		// nil 仓库模拟未配置 session 权威的 dev 裸跑环境。
		uc = NewLoginUsecase(nil, nil, nil, nil, nil, nil, "127.0.0.1:7777", "cn",
			signer, verifier, nil, false, false, nil, false)
	} else {
		// 非 nil 仓库承载每个子测试显式给出的现行 jti、缺失态或读取故障。
		uc = NewLoginUsecase(nil, sessions, nil, nil, nil, nil, "127.0.0.1:7777", "cn",
			signer, verifier, nil, false, false, nil, false)
	}
	// strict 直接映射生产 B1 的 Hub assignment binding 严格门。
	uc.SetRequireHubAssignmentBinding(strict)
	return uc
}

func TestRequireCurrentSessionJTI(t *testing.T) {
	// ctx 不携带额外身份，确保断言只由显式 playerID、jti 与 session 权威夹具决定。
	ctx := context.Background()

	t.Run("sessions 未配(dev 裸跑)直通", func(t *testing.T) {
		// uc 使用 nil session 仓库，验证严格开关不会凭空制造权威记录。
		uc := newSessionJTIUsecase(t, nil, true)
		// err 是 dev 裸跑直通结果，非 nil 即表示兼容分支被意外收紧。
		if err := uc.RequireCurrentSessionJTI(ctx, 42, "any"); err != nil {
			t.Fatalf("nil sessions must pass, got %v", err)
		}
	})

	t.Run("jti 缺失:B1 严格档 fail-closed 拒绝", func(t *testing.T) {
		// uc 模拟权威可用但网关 jti 证据缺失的生产严格档。
		uc := newSessionJTIUsecase(t, &jtiSessionRepo{cur: "cur", found: true}, true)
		// err 必须是未授权，不能把“无法证明当前会话”降级为放行。
		err := uc.RequireCurrentSessionJTI(ctx, 42, "")
		if errcode.As(err) != errcode.ErrUnauthorized {
			t.Fatalf("empty jti under strict profile must be Unauthorized, got %v", err)
		}
	})

	t.Run("jti 缺失:local/off 保留历史放行", func(t *testing.T) {
		// uc 使用非严格档，锁定本地直连调试没有 Envoy payload 头时的历史行为。
		uc := newSessionJTIUsecase(t, &jtiSessionRepo{cur: "cur", found: true}, false)
		// err 是兼容档结果，非 nil 表示本地调试路径被严格档语义误伤。
		if err := uc.RequireCurrentSessionJTI(ctx, 42, ""); err != nil {
			t.Fatalf("empty jti under dev profile must pass, got %v", err)
		}
	})

	t.Run("顶号后旧 jti 拒绝(核心负例)", func(t *testing.T) {
		// uc 的权威代际为 new-generation，调用方故意携带已被顶掉的旧代际。
		uc := newSessionJTIUsecase(t, &jtiSessionRepo{cur: "new-generation", found: true}, true)
		// err 锁定旧设备必须被 fencing，且用顶号专属码(R4 P0):与自然过期可判别，
		// 被顶设备据此转交互登录而非自动重登反顶新设备。
		err := uc.RequireCurrentSessionJTI(ctx, 42, "old-generation")
		if errcode.As(err) != errcode.ErrSessionSuperseded {
			t.Fatalf("superseded jti must be ErrSessionSuperseded, got %v", err)
		}
	})

	t.Run("当前代 jti 放行", func(t *testing.T) {
		// uc 的权威 jti 与调用方证据完全一致，代表未被顶号的当前设备。
		uc := newSessionJTIUsecase(t, &jtiSessionRepo{cur: "current", found: true}, true)
		// err 是成功路径结果，非 nil 表示现行会话被误拒。
		if err := uc.RequireCurrentSessionJTI(ctx, 42, "current"); err != nil {
			t.Fatalf("current jti must pass, got %v", err)
		}
	})

	t.Run("session 已过期/登出拒绝", func(t *testing.T) {
		// uc 用 found=false 模拟权威记录已消失，不把调用方自报 jti 当成有效会话。
		uc := newSessionJTIUsecase(t, &jtiSessionRepo{found: false}, true)
		// err 必须是未授权，明确区分会话终态与可重试的依赖故障。
		err := uc.RequireCurrentSessionJTI(ctx, 42, "whatever")
		if errcode.As(err) != errcode.ErrUnauthorized {
			t.Fatalf("missing session must be Unauthorized, got %v", err)
		}
	})

	t.Run("Redis 故障可重试 Unavailable(fail-closed 不放行)", func(t *testing.T) {
		// uc 注入权威读取超时，验证 UNKNOWN 不得冒充当前会话或会话终态。
		uc := newSessionJTIUsecase(t, &jtiSessionRepo{err: context.DeadlineExceeded}, true)
		// err 必须保持可重试 Unavailable，同时保证本次请求零选角、零签票副作用。
		err := uc.RequireCurrentSessionJTI(ctx, 42, "whatever")
		if errcode.As(err) != errcode.ErrUnavailable {
			t.Fatalf("session authority failure must be Unavailable, got %v", err)
		}
	})
}
