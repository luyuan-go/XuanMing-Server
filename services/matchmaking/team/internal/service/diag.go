// diag.go — service 层入参 / 身份门的可诊断性(infra.md §11.3,2026-08-13)。
//
// # 为什么这一层非补不可
//
// 本服务的每个 RPC 都以 `return &XxxResponse{Code: ERR_XXX}, nil` 的形状拒绝 ——
// **error 是 nil**。于是 `pkg/middleware/logging.go` 把它记成 `rpc_ok`,而 rpc_ok 是
// DEBUG 级:线上默认 info 级下,"未登录被拒"与"team_id 没填"这两类拒绝**一条都不出**。
// 客户端只看到一个错误码,服务端侧完全空白 —— 这正是 §11.3 R2 点名不许指望 access log
// 兜底的原因。
//
// 所以入参门与身份门必须由业务代码自己显式打 WARN,且带枚举 reason。
package service

import (
	"context"

	plog "github.com/luyuancpp/pandora/pkg/log"
)

// ── R2 枚举 reason(service 层) ───────────────────────────────────────────────
const (
	// reasonUnauthenticated:ctx 里没有 JWT 注入的 player_id。
	// 正常玩家面不该出现(Envoy jwt_authn 已在路由层 require JWT),持续出现意味着
	// 网关配置漂了或有人在绕过网关直连。
	reasonUnauthenticated = "missing_player_identity"
	// reasonMissingTeamID:请求没带 team_id(客户端本地队伍态丢了 / 拼错字段)。
	reasonMissingTeamID = "missing_team_id"
	// reasonMissingTargetPlayerID:邀请 / 踢人没带目标玩家。
	reasonMissingTargetPlayerID = "missing_target_player_id"
	// reasonMissingApplicantID:审批申请没带申请人。
	reasonMissingApplicantID = "missing_applicant_id"
	// reasonMissingPlayerID:内部反查接口没带被查的 player_id。
	reasonMissingPlayerID = "missing_player_id"
	// reasonSystemRPCByClient:带玩家 JWT 的调用打到了只允许内部东西向的方法
	// (systemOnly 门)。字面量原本内联在 systemOnly 里,提上来与其它 reason 同源。
	reasonSystemRPCByClient = "system_rpc_by_client"
)

// logRPCRejected 打一条 service 层拒绝日志。
//
// msg 固定 team_rpc_rejected + rpc 字段:一个 grep 就能拉出"这一分钟里各接口分别因为
// 什么被拒了多少次",不必按接口逐个记 msg 名。
func logRPCRejected(ctx context.Context, rpc, reason string, kv ...any) {
	args := make([]any, 0, 6+len(kv))
	args = append(args, "msg", "team_rpc_rejected", "rpc", rpc, "reason", reason)
	args = append(args, kv...)
	plog.With(ctx).Warnw(args...)
}
