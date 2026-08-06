// Package server — gRPC server 注册。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	auctionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/auction/v1"

	"github.com/luyuancpp/pandora/services/economy/auction/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/auction/internal/service"
)

// NewGRPCServer 构造 gRPC server 并注册 AuctionService。
//
// 端口由 cfg.Server.Grpc.Addr 决定(默认 :20016)。
// pmw.AuthOptional() 从 Envoy 注入的 x-pandora-player-id header 读 player_id 注入 ctx。
// Envoy jwt_authn 已在路由层 require JWT;service 层再做一次 callerID==0 拦截兜底。
func NewGRPCServer(cfg *conf.Config, svc *service.AuctionService) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional())
	auctionv1.RegisterAuctionServiceServer(srv, svc)
	return srv
}
