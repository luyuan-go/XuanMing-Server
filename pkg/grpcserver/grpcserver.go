// Package grpcserver 提供 Pandora 服务的 gRPC server 包装(基于 Kratos transport/grpc)。
//
// 设计:
//   - 包装 Kratos transport/grpc.NewServer(...)
//   - 默认挂接 Pandora middleware 链(Recovery → Trace → Logging → Metrics)
//   - 业务侧:`grpcserver.MustNewServer(cfg.Server)` + `pb.RegisterXxxServer(srv, impl)` + `kratos.New(...).Run()`
//
// 跟之前 go-zero 版本的区别(2026-06-04 重写):
//   - go-zero zrpc.MustNewServer → Kratos transport/grpc.NewServer
//   - go-zero unary interceptor → Kratos middleware(协议无关,gRPC + HTTP 共用)
//   - 新增 server stream / bidi 支持(go-zero 不支持,Kratos 原生)
package grpcserver

import (
	"time"

	"github.com/go-kratos/kratos/v2/middleware"
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/luyuancpp/pandora/pkg/config"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
)

// MustNewServer 创建并配置一个 Kratos gRPC Server。
//
// 默认 middleware 链(从外到内):
//  1. Recovery     最外层捕 panic
//  2. Trace        trace_id 注入 / 透传
//  3. Logging      access log
//  4. Metrics      Prometheus 指标
//
// 业务可通过 customMW 追加自定义 middleware(顺序在默认之后):
//
//	srv := grpcserver.MustNewServer(cfg.Server,
//	    pmw.AuthRequired(),  // 业务级鉴权
//	)
//	pb.RegisterLoginServiceServer(srv, loginsvc.New(...))
//	app := kratos.New(kratos.Server(srv))
//	app.Run()
func MustNewServer(c config.Server, customMW ...middleware.Middleware) *kgrpc.Server {
	// 默认 middleware 链(从外到内):
	//   Recovery → Trace → Logging → Metrics → [RateLimit] → KillSwitch → custom → business
	//
	// RateLimit / KillSwitch 放在 Metrics 内层:被限流 / 被关停的请求仍会被
	// Logging + Metrics 记录(pandora_rpc_total{code=...} 能看到拒绝次数),便于观测。
	base := []middleware.Middleware{
		pmw.Recovery(),
		pmw.Trace(),
		pmw.Logging(),
		pmw.Metrics(),
	}
	// 第 4 层:BBR 自适应限流(过载保护)。dev 默认关,prod 显式开。
	if c.Grpc.EnableRateLimit {
		base = append(base, pmw.RateLimit())
	}
	// 第 3 层:RPC 级临时关停(Kill-Switch)。无条件挂载——未装配时 fail-open 放行,
	// 开销仅一次 atomic load + map 查询。
	base = append(base, pmw.KillSwitch())

	mws := append(base, customMW...)

	opts := []kgrpc.ServerOption{
		kgrpc.Address(c.Grpc.Addr),
		kgrpc.Middleware(mws...),
	}
	if c.Grpc.Network != "" {
		opts = append(opts, kgrpc.Network(c.Grpc.Network))
	}
	if c.Grpc.Timeout > 0 {
		opts = append(opts, kgrpc.Timeout(c.Grpc.Timeout.Std()))
	}

	// 连接轮换(2026-07-08,zero-downtime-update.md §6.2):gRPC 是 HTTP/2 长连接,
	// k8s Service 的 L4 均衡按连接分配 —— 老连接不断,滚动更新时新副本分不到流量。
	// MaxConnectionAge 让达龄连接优雅 GOAWAY(在途 RPC 有 Grace 宽限排空),客户端
	// 透明重拨到新副本。dev yaml 已全量开 15m;不配(零值)= 关,行为不变。
	if c.Grpc.MaxConnAge > 0 {
		grace := c.Grpc.MaxConnAgeGrace.Std()
		if grace <= 0 {
			grace = 30 * time.Second
		}
		opts = append(opts, kgrpc.Options(grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      c.Grpc.MaxConnAge.Std(),
			MaxConnectionAgeGrace: grace,
		})))
	}

	// gRPC reflection 开关化(W3 ③,2026-06-05):
	//   - Kratos transport/grpc.NewServer 默认开 reflection(v1 + v1alpha)。
	//   - dev:cfg.Server.Grpc.EnableReflection = true,保留默认行为,
	//     `grpcurl :20001 list` / `describe pandora.login.v1.LoginService` 可用。
	//   - prod(默认 false):调 kgrpc.DisableReflection() 关闭,
	//     避免 schema 泄露 / 攻击面扩大。
	if !c.Grpc.EnableReflection {
		opts = append(opts, kgrpc.DisableReflection())
	}
	return kgrpc.NewServer(opts...)
}
