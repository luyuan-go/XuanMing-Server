// Package server — gRPC server 注册。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	locatorv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/locator/v1"

	"github.com/luyuancpp/pandora/services/runtime/player_locator/internal/conf"
	"github.com/luyuancpp/pandora/services/runtime/player_locator/internal/service"
)

// NewGRPCServer 构造 gRPC server 并把 PlayerLocatorService 注册上去。
//
// 端口由 cfg.Server.Grpc.Addr 决定(默认 :20006)。
//
// 不挂 AuthRequired:本服务的 RPC 由内网 DS / login 服务调用,不直接暴露给玩家;
// W3+ Envoy 路由层用 ext_authz / route 黑白名单限制本路径只允许内网。
func NewGRPCServer(cfg *conf.Config, svc *service.LocatorService) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server)
	locatorv1.RegisterPlayerLocatorServiceServer(srv, svc)
	return srv
}
