// match_call_auth.go — matchmaker → team 东西向调用的验签(INC-20260813-001 A-13)。
//
// # 为什么 systemOnly 不够
//
// `systemOnly` 只能证明「本次调用不带玩家 JWT」——:8444 没有 jwt_authn,集群内网里任何
// Pod、任何能连到本服务端口的东西都满足 callerID==0。而这两个方法的杀伤力是实打实的:
//   - BeginTeamMatch 能给**任意**队伍上 roster 租约,反复调 = 让那支队伍永远开不了局;
//   - EndTeamMatch 能把**任意**队伍打回 FORMING。
//
// 所以要像 team→matchmaker 那样验签,caller 固定 "matchmaker"。
//
// # 三档与「不靠发布顺序」
//
// 直接上强制会踩 §12.3 那个坑:team 先要求签名而 matchmaker 还没滚到签名版本 → 全线拒。
// 因此做成可降级三档(留空 / 观察 / 强制),上线顺序是「两边配密钥 → 观察 → 翻 require」,
// 每一步单独都安全。
package service

import (
	"context"
	"errors"

	"github.com/luyuancpp/pandora/pkg/internalrpcauth"
	plog "github.com/luyuancpp/pandora/pkg/log"
	commonv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/common/v1"
)

// internalRequestVerifier 是本包对 internalrpcauth.Verifier 的最小依赖面(便于单测注入)。
type internalRequestVerifier interface {
	Verify(ctx context.Context, fullMethod string, subject uint64) error
}

// SetMatchCallAuth 注入 matchmaker 调用的验签器与是否强制。
// verifier 为 nil = 未配密钥,整道跳过(行为与接线前完全一致)。
func (s *TeamService) SetMatchCallAuth(v internalRequestVerifier, require bool) {
	s.matchCallAuth = v
	s.matchCallRequire = require
}

// verifyMatchCall 校验本次调用确实来自 matchmaker。
// 返回 OK 表示放行(含观察期的降级放行);其余为应当回给调用方的错误码。
func (s *TeamService) verifyMatchCall(ctx context.Context, fullMethod string, subject uint64) commonv1.ErrCode {
	if s.matchCallAuth == nil {
		return commonv1.ErrCode_OK // 未配密钥:整道不启用
	}
	err := s.matchCallAuth.Verify(ctx, fullMethod, subject)
	if err == nil {
		return commonv1.ErrCode_OK
	}
	if !s.matchCallRequire {
		// 观察期:只记不拒。运维据此确认 matchmaker 是否已全量滚上签名版本
		// (这条日志归零后才翻 require=true)。
		plog.With(ctx).Warnw("msg", "team_match_call_auth_observed",
			"method", fullMethod, "subject", subject, "err", err,
			"hint", "观察期未强制;matchmaker 全量滚上签名版本后把 match_call_auth_require 置 true")
		return commonv1.ErrCode_OK
	}
	code := commonv1.ErrCode_ERR_PERMISSION_DENY
	if errorsIsUnavailable(err) {
		// 重放存储不可用 → 说不清是不是重放,按不确定回,让调用方重试而不是当成越权。
		code = commonv1.ErrCode_ERR_UNAVAILABLE
	}
	plog.With(ctx).Warnw("msg", "team_match_call_auth_rejected",
		"method", fullMethod, "subject", subject, "code", code, "err", err)
	return code
}

func errorsIsUnavailable(err error) bool {
	return errors.Is(err, internalrpcauth.ErrUnavailable)
}
