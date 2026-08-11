// Package server — gRPC server 注册。
package server

import (
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/luyuancpp/pandora/pkg/grpcserver"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	missionv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/mission/v1"

	"github.com/luyuancpp/pandora/services/social/mission/internal/conf"
	"github.com/luyuancpp/pandora/services/social/mission/internal/service"
)

// NewGRPCServer 构造 gRPC server 并注册 MissionService。
//
// 端口由 cfg.Server.Grpc.Addr 决定(默认 :20019)。
// pmw.AuthOptional() 从 Envoy 注入的 x-pandora-player-id 读身份注入 ctx;
// pmw.SessionCurrent 校验客户端面请求 jti == login 会话权威当前一代
// (顶号后旧 JWT 不得继续操作,INC-20260722-004;对齐 friend——dialogue 没接是旧模板缺口)。
// 系统 RPC(ReportMissionFacts / CompleteAllMissions)无 JWT,callerID==0 直接过这两层,
// service 层 systemOnly 负责拒带玩家身份的越权调用。
func NewGRPCServer(cfg *conf.Config, svc *service.MissionService, sessGate sessiongate.Gate) *kgrpc.Server {
	srv := grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional(),
		pmw.SessionCurrent(sessGate, cfg.SessionGate.Require))
	missionv1.RegisterMissionServiceServer(srv, svc)
	return srv
}
