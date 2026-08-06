// Package server — gRPC server 注册。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	bagv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/bag/v1"
	inventoryv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/inventory/v1"

	"github.com/luyuancpp/pandora/services/economy/inventory/internal/conf"
	"github.com/luyuancpp/pandora/services/economy/inventory/internal/service"
)

// NewGRPCServer 构造 gRPC server 并注册 InventoryService(+ 可选 BagService)。
//
// 端口由 cfg.Server.Grpc.Addr 决定(默认 :20015)。
// pmw.AuthOptional() 从 Envoy 注入的 x-pandora-player-id header 读 player_id 注入 ctx。
// 用 AuthOptional 而非 AuthRequired:GrantItems 是后端内部直连(无 JWT,callerID==0)需放行;
// 客户端 RPC(GetInventory/UseItem/SellItem)在 service 层用 callerPlayerID 强制鉴权 +
// 校验请求体 player_id == 调用者,GrantItems 在 service 层拒绝 callerID>0 的客户端调用。
//
// bagSvc 非 nil 时注册背包域(pandora.bag.v1,bag-domain.md phase 1 由本进程承载;
// 全部内部系统接口,service 层拒客户端 JWT,Envoy 侧另有 /pandora.bag.v1/ 403 拦截)。
//
// pmw.SessionCurrent 校验客户端面请求 jti == login 会话权威当前一代(R5 复审 P0-1:
// 顶号后旧 JWT 在 exp 前不得继续按 player_id 定向操作——UseItem/SellItem 属被审计点,
// INC-20260722-004;内部直连不带 x-pandora-jwt-payload,GrantItems 等系统 RPC 天然放行)。
func NewGRPCServer(cfg *conf.Config, svc *service.InventoryService, bagSvc *service.BagService, sessGate sessiongate.Gate) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional(),
		pmw.SessionCurrent(sessGate, cfg.SessionGate.Require))
	inventoryv1.RegisterInventoryServiceServer(srv, svc)
	if bagSvc != nil {
		bagv1.RegisterBagServiceServer(srv, bagSvc)
	}
	return srv
}
