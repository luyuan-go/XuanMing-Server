// match_call_auth_test.go — matchmaker→team 验签的三档行为(INC-20260813-001 A-13)。
//
// 三档是**为了不重蹈 §12.3 那个坑**才有的:直接上强制会要求「matchmaker 必须先滚完」,
// 而发布顺序是人执行的、搞错没有机械手段能拦。所以每一档的行为都必须被钉死,
// 尤其是「观察期不许拒」和「强制期必须拒」这一对 —— 任一边写反,三档就等于没有。
package service

import (
	"context"
	"errors"
	"testing"

	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"
)

// stubVerifier 按注入的错误决定验签结果。
type stubVerifier struct {
	err   error
	calls int
}

func (v *stubVerifier) Verify(context.Context, string, uint64) error {
	v.calls++
	return v.err
}

const anyMethod = teamv1.TeamService_BeginTeamMatch_FullMethodName

// 档 1:未配密钥 → 整道不启用,一次都不该去验。
func TestMatchCallAuth_未配密钥整道跳过(t *testing.T) {
	svc := NewTeamService(nil, nil, nil)
	if code := svc.verifyMatchCall(context.Background(), anyMethod, 9001); code != commonv1.ErrCode_OK {
		t.Fatalf("未配密钥必须放行: got=%s", code)
	}
}

// 档 2(观察期):**验不过也必须放行**。
// 这一档存在的全部理由就是让两边的密钥能分两次发布配上;写成拒就等于回到发布顺序依赖。
func TestMatchCallAuth_观察期验不过仍放行(t *testing.T) {
	v := &stubVerifier{err: errors.New("bad signature")}
	svc := NewTeamService(nil, nil, nil)
	svc.SetMatchCallAuth(v, false)

	if code := svc.verifyMatchCall(context.Background(), anyMethod, 9001); code != commonv1.ErrCode_OK {
		t.Fatalf("观察期必须放行(否则共存窗口里旧 matchmaker 会被全拒): got=%s", code)
	}
	if v.calls != 1 {
		t.Fatalf("观察期仍必须**真的验一次**,否则日志里看不到 observed 计数: calls=%d", v.calls)
	}
}

// 档 3(强制):验不过一律 PERMISSION_DENY。
func TestMatchCallAuth_强制期验不过必须拒(t *testing.T) {
	svc := NewTeamService(nil, nil, nil)
	svc.SetMatchCallAuth(&stubVerifier{err: errors.New("bad signature")}, true)

	if code := svc.verifyMatchCall(context.Background(), anyMethod, 9001); code != commonv1.ErrCode_ERR_PERMISSION_DENY {
		t.Fatalf("强制期验不过必须拒: got=%s", code)
	}
}

// 验得过 → 放行(两档都一样)。
func TestMatchCallAuth_验得过放行(t *testing.T) {
	for _, require := range []bool{false, true} {
		svc := NewTeamService(nil, nil, nil)
		svc.SetMatchCallAuth(&stubVerifier{}, require)
		if code := svc.verifyMatchCall(context.Background(), anyMethod, 9001); code != commonv1.ErrCode_OK {
			t.Fatalf("require=%v 时验签通过必须放行: got=%s", require, code)
		}
	}
}

// ★ 重放存储不可用 → `ERR_UNAVAILABLE` 而不是 DENY。
//
// 两者对调用方的含义完全不同:UNAVAILABLE 是「我说不清,你重试」,DENY 是「你没资格,别再来了」。
// 把「查不出是不是重放」判成越权,会让一次 Redis 抖动变成 matchmaker 永久放弃复位。
func TestMatchCallAuth_重放存储不可用回Unavailable(t *testing.T) {
	svc := NewTeamService(nil, nil, nil)
	svc.SetMatchCallAuth(&stubVerifier{err: internalrpcauth.ErrUnavailable}, true)

	if code := svc.verifyMatchCall(context.Background(), anyMethod, 9001); code != commonv1.ErrCode_ERR_UNAVAILABLE {
		t.Fatalf("说不清是不是重放时必须回 UNAVAILABLE(可重试),不能当成越权: got=%s", code)
	}
}

// 两个 RPC 都必须真的过这道门 —— 只接一个等于另一个仍对全内网敞开。
func TestMatchCallAuth_两个RPC都接了门(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(*TeamService) commonv1.ErrCode
	}{
		{"BeginTeamMatch", func(s *TeamService) commonv1.ErrCode {
			resp, _ := s.BeginTeamMatch(context.Background(), &teamv1.BeginTeamMatchRequest{
				TeamId: 9001, CaptainId: 7, OperationId: "op-1", LeaseMs: 5000,
			})
			return resp.GetCode()
		}},
		{"EndTeamMatch", func(s *TeamService) commonv1.ErrCode {
			resp, _ := s.EndTeamMatch(context.Background(), &teamv1.EndTeamMatchRequest{TeamId: 9001})
			return resp.GetCode()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// nil usecase:门若没接上会 nil 解引用 panic,而不是安静返回 DENY。
			svc := NewTeamService(nil, nil, nil)
			svc.SetMatchCallAuth(&stubVerifier{err: errors.New("bad signature")}, true)
			if code := tc.call(svc); code != commonv1.ErrCode_ERR_PERMISSION_DENY {
				t.Fatalf("%s 未接验签门(或拒绝发生在触达业务之后): got=%s", tc.name, code)
			}
		})
	}
}
