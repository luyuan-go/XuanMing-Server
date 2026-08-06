// Package server — gRPC server 注册。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	datav1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/data_service/v1"

	"github.com/luyuancpp/pandora/services/data/data_service/internal/conf"
	"github.com/luyuancpp/pandora/services/data/data_service/internal/service"
)

// NewGRPCServer 构造 gRPC server 并注册 DataService。
//
// 端口由 cfg.Server.Grpc.Addr 决定(默认 :20003)。
//
// 不挂 AuthRequired:本服务是内网数据网关,由 player / 其它内网服务调用,不直接暴露给玩家;
// 由 Envoy / 内网 RPC 黑白名单限制本路径只允许内网访问。
func NewGRPCServer(cfg *conf.Config, svc *service.DataService) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server)
	datav1.RegisterDataServiceServer(srv, svc)
	return srv
}
