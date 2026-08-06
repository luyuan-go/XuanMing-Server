// Package server — gRPC server 注册。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	battlev1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/battle/v1"

	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/battle_result/internal/service"
)

// NewGRPCServer 构造 gRPC server 并注册 BattleResultService。
//
// 端口由 cfg.Server.Grpc.Addr 决定(默认 :20022)。
// 调用方为后端内部 / 运维,pmw.AuthOptional() 与其它服保持一致。
func NewGRPCServer(cfg *conf.Config, svc *service.BattleResultService) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional())
	battlev1.RegisterBattleResultServiceServer(srv, svc)
	return srv
}
