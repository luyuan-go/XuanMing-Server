// Package server — gRPC server 注册。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	configv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
	gmv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/gm/v1"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/gm"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/service"
)

// NewGRPCServer 构造 gRPC server 并注册 DSAllocatorService + GmService。
//
// 端口由 cfg.Server.Grpc.Addr 决定(默认 :50020)。
// 调用方为后端内部(matchmaker / 战斗 DS / 运维 GM 工具),pmw.AuthOptional() 保持与其它服一致;
// 本服 RPC 不依赖 player_id 授权,GmService 属内部接口不经 Envoy 暴露给玩家客户端。
//
// GmService 与 DSAllocatorService 同进程复用同一 gRPC 端口:ds_allocator 已持有
// match_id→战斗 DS 的注册表,战斗 DS 也已与之心跳直连,GM 指令下发天然与之同域。
// ctAdmin 为配置表热更入口(§9.15),仅 config_table.dir 配置时非 nil;nil 则不注册该服务。
func NewGRPCServer(cfg *conf.Config, svc *service.AllocatorService, gmSvc *gm.Service,
	ctAdmin configv1.ConfigTableAdminServiceServer) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional())
	dsv1.RegisterDSAllocatorServiceServer(srv, svc)
	gmv1.RegisterGmServiceServer(srv, gmSvc)
	if ctAdmin != nil {
		configv1.RegisterConfigTableAdminServiceServer(srv, ctAdmin)
	}
	return srv
}
