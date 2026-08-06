// Package server — gRPC server 注册(2026-06-29)。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	mailv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/mail/v1"

	"github.com/luyuancpp/pandora/services/social/mail/internal/conf"
	"github.com/luyuancpp/pandora/services/social/mail/internal/service"
)

// NewGRPCServer 构造 gRPC server 并注册 MailService。
//
// 端口由 cfg.Server.Grpc.Addr 决定(默认 :20009)。
// pmw.AuthOptional() 从 Envoy 注入的 player_id 注入 ctx;玩家 RPC 在 service 层兜底 callerID==0。
// SendSystemMail/SendGuildMail/SendPersonalMail 为内网运营 RPC,不经 Envoy 对客户端开放
// (内网调用不带 x-pandora-jwt-payload,SessionCurrent 对其天然放行)。
// pmw.SessionCurrent 校验客户端面请求 jti == login 会话权威当前一代(R5 复审 P0-1:
// 顶号后旧 JWT 在 exp 前不得继续按 player_id 定向操作,INC-20260722-004)。
func NewGRPCServer(cfg *conf.Config, mailSvc *service.MailService, sessGate sessiongate.Gate) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional(),
		pmw.SessionCurrent(sessGate, cfg.SessionGate.Require))
	mailv1.RegisterMailServiceServer(srv, mailSvc)
	return srv
}
