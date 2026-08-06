// Package server — gRPC server 注册。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"

	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/service"
)

// NewGRPCServer 构造 gRPC server 并注册 HubAllocatorService。
//
// 端口由 cfg.Server.Grpc.Addr 决定(默认 :20021)。
// 调用方为后端内部(login / 大厅 DS),pmw.AuthOptional() 保持与其它服一致;
// 本服多数 RPC 不依赖 player_id 授权(player_id 由 login 显式传入)。
//
// pmw.SessionCurrent 校验客户端面请求 jti == login 会话权威当前一代(R5 复审 P0-1,
// INC-20260722-004):Envoy 只对 ListHubLines / TransferToLine 两个玩家 method 精确
// 开放并 require JWT,其余内部/DS RPC 不带 x-pandora-jwt-payload 天然放行。
func NewGRPCServer(cfg *conf.Config, svc *service.HubService, sessGate sessiongate.Gate) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional(),
		pmw.SessionCurrent(sessGate, cfg.SessionGate.Require))
	hubv1.RegisterHubAllocatorServiceServer(srv, svc)
	return srv
}
