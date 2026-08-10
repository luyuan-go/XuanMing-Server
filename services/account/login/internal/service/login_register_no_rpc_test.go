// login_register_no_rpc_test.go — GetRegisterNo RPC 的 service 层语义验证
// (register-no-and-login-surge.md §3.7,2026-08-10)。
//
// 与同目录 login_register_no_test.go 分工:那份验「Login 响应带出编号」,本份验
// 「补拉 RPC」——即首登拿到 0 之后客户端赖以脱离「生成中」的那条路径。
//
// 四条断言对应四个真实失效模式:
//	① 有 player_id → 返回库里的编号(补拉链路本身);
//	② 编号为 0 → code 仍是 OK(**不是错误**):0 = 仍在补号窗口(约 15s)。若这里返回
//	   错误码,客户端会把正常的补号窗口当故障处理(弹错误/停止重试),新玩家反而永远
//	   看不到编号——正是本 RPC 要修的那个 bug 的翻版;
//	③ ctx 无 player_id → ErrUnauthorized:该 path 未列进 envoy jwt_authn rules 时就是
//	   这个形态(未列到的 path 默认放行不验签 → 上游拿不到 x-pandora-player-id),
//	   必须硬拒而不是当成 player_id=0 去查库;
//	④ repo 报错 → 错误码透传,不把故障伪装成「编号 0 / 仍在生成」(§9.22 不得冒充默认态)。
package service

import (
	"context"
	"testing"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"
	"github.com/luyuancpp/pandora/services/account/login/internal/biz"
)

// registerNoRPCRepo 是可注入错误的账号仓储 fake(同目录 registerNoAccountRepo 只回固定值)。
type registerNoRPCRepo struct {
	registerNo uint64
	err        error
}

func (r *registerNoRPCRepo) FindByAccount(context.Context, string) (uint64, string, error) {
	return 42, "", nil
}
func (r *registerNoRPCRepo) CreateAccount(context.Context, uint64, string, string) error { return nil }
func (r *registerNoRPCRepo) CheckBanned(context.Context, uint64, string) (bool, error) {
	return false, nil
}
func (r *registerNoRPCRepo) TouchDevice(context.Context, uint64, string) error { return nil }
func (r *registerNoRPCRepo) GetRegisterNo(context.Context, uint64) (uint64, error) {
	if r.err != nil {
		return 0, r.err
	}
	return r.registerNo, nil
}

func newRegisterNoRPCService(t *testing.T, repo *registerNoRPCRepo) *LoginService {
	t.Helper()
	cfg := auth.Config{Secret: []byte("pandora-register-no-rpc-test-32!!!!!!")}
	signer, err := auth.NewSigner(cfg)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := auth.NewVerifier(cfg)
	if err != nil {
		t.Fatal(err)
	}
	uc := biz.NewLoginUsecase(repo, nil, nil, nil, nil, nil,
		"127.0.0.1:7777", "cn", signer, verifier, nil, true, false, nil, false)
	return NewLoginService(uc, nil)
}

// ① 正常补拉:ctx 带 player_id → 返回库里的编号。
func TestGetRegisterNo_ReturnsAssignedNumber(t *testing.T) {
	const want = uint64(100001)
	svc := newRegisterNoRPCService(t, &registerNoRPCRepo{registerNo: want})
	ctx := context.WithValue(context.Background(), plog.CtxKeyPlayerID, uint64(42))

	res, rpcErr := svc.GetRegisterNo(ctx, &loginv1.GetRegisterNoRequest{})
	if rpcErr != nil {
		t.Fatalf("GetRegisterNo transport error: %v", rpcErr)
	}
	if res.GetCode() != commonv1.ErrCode_OK {
		t.Fatalf("code = %v, want OK", res.GetCode())
	}
	if res.GetRegisterNo() != want {
		t.Fatalf("register_no = %d, want %d", res.GetRegisterNo(), want)
	}
}

// ② 补号窗口内:编号 0 必须是 code=OK 的正常态,不能是错误码。
// 若这条挂了,客户端会把「还没补到号」当故障,新玩家永远脱不出「生成中」。
func TestGetRegisterNo_PendingIsOkNotError(t *testing.T) {
	svc := newRegisterNoRPCService(t, &registerNoRPCRepo{registerNo: 0})
	ctx := context.WithValue(context.Background(), plog.CtxKeyPlayerID, uint64(42))

	res, rpcErr := svc.GetRegisterNo(ctx, &loginv1.GetRegisterNoRequest{})
	if rpcErr != nil {
		t.Fatalf("GetRegisterNo transport error: %v", rpcErr)
	}
	if res.GetCode() != commonv1.ErrCode_OK {
		t.Fatalf("补号窗口内 code = %v, want OK(0 是正常态不是错误)", res.GetCode())
	}
	if res.GetRegisterNo() != 0 {
		t.Fatalf("register_no = %d, want 0", res.GetRegisterNo())
	}
}

// ③ ctx 无 player_id(该 path 漏配 envoy jwt_authn rules 时的真实形态)→ 硬拒。
func TestGetRegisterNo_RejectsMissingPlayerID(t *testing.T) {
	svc := newRegisterNoRPCService(t, &registerNoRPCRepo{registerNo: 100001})

	res, rpcErr := svc.GetRegisterNo(context.Background(), &loginv1.GetRegisterNoRequest{})
	if rpcErr != nil {
		t.Fatalf("GetRegisterNo transport error: %v", rpcErr)
	}
	if res.GetCode() != commonv1.ErrCode_ERR_UNAUTHORIZED {
		t.Fatalf("code = %v, want ERR_UNAUTHORIZED(未验签不得当 player_id=0 去查库)", res.GetCode())
	}
	if res.GetRegisterNo() != 0 {
		t.Fatalf("未鉴权却返回了 register_no = %d", res.GetRegisterNo())
	}
}

// ④ 查询失败必须透传错误码,不得伪装成「编号 0 / 仍在生成」(§9.22)。
func TestGetRegisterNo_PropagatesRepoFailure(t *testing.T) {
	svc := newRegisterNoRPCService(t, &registerNoRPCRepo{
		err: errcode.New(errcode.ErrInternal, "mysql down"),
	})
	ctx := context.WithValue(context.Background(), plog.CtxKeyPlayerID, uint64(42))

	res, rpcErr := svc.GetRegisterNo(ctx, &loginv1.GetRegisterNoRequest{})
	if rpcErr != nil {
		t.Fatalf("GetRegisterNo transport error: %v", rpcErr)
	}
	if res.GetCode() == commonv1.ErrCode_OK {
		t.Fatal("查询失败却返回 OK:故障被伪装成「仍在补号」,客户端会一直等下去")
	}
}
