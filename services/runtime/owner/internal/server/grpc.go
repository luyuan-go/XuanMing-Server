// Package server — gRPC server 注册。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	ownerv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/owner/v1"

	"github.com/luyuancpp/pandora/services/runtime/owner/internal/conf"
	"github.com/luyuancpp/pandora/services/runtime/owner/internal/service"
)

// NewGRPCServer 构造 gRPC server 并注册 OwnerService(端口默认 :20017)。
//
// pmw.AuthOptional():合法调用者是后端内部直连(无 JWT,callerID==0);
// service 层拒绝 callerID>0 的客户端调用,Envoy 侧对 /pandora.owner.v1/ 前缀 403 双保险。
func NewGRPCServer(cfg *conf.Config, svc *service.OwnerService) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional())
	ownerv1.RegisterOwnerServiceServer(srv, svc)
	return srv
}
