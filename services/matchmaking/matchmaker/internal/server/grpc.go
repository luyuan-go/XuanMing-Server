// Package server — gRPC server 注册。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	configv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"

	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/service"
)

// NewGRPCServer 构造 gRPC server 并注册 MatchService。
//
// 端口由 cfg.Server.Grpc.Addr 决定(默认 :20011)。
// pmw.AuthOptional() 从 Envoy 注入的 x-pandora-player-id header 读 player_id 注入 ctx。
// Envoy jwt_authn 已在路由层 require JWT;service 层再做 callerID==0 拦截兜底。
// pmw.SessionCurrent 校验客户端面请求 jti == login 会话权威当前一代(R5 复审 P0-1:
// 顶号后旧 JWT 在 exp 前不得继续按 player_id 定向操作,INC-20260722-004)。
//
// ctAdmin 非 nil(config_table.dir 已配置)时同端口挂配置表热更入口:
// 内部接口,Envoy 无该 service 路由,玩家流量到不了;service 层再做 callerID!=0 拒绝兜底
// (内网调用不带 x-pandora-jwt-payload,SessionCurrent 对其天然放行)。
func NewGRPCServer(cfg *conf.Config, svc *service.MatchService, ctAdmin *service.ConfigTableAdminService, sessGate sessiongate.Gate) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional(),
		pmw.SessionCurrent(sessGate, cfg.SessionGate.Require))
	matchv1.RegisterMatchServiceServer(srv, svc)
	if ctAdmin != nil {
		configv1.RegisterConfigTableAdminServiceServer(srv, ctAdmin)
	}
	return srv
}
