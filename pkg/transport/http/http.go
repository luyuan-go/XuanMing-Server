// Package http 提供 Pandora 服务的 HTTP server 包装(基于 Kratos transport/http)。
//
// 用途:
//   - 给 protoc-gen-go-http 生成的 RESTful handler 用(Kratos 标准)
//   - 给运营后台 / 第三方 webhook 这种"非 gRPC 客户端"用
//
// W2 阶段:
//   - 14 个业务服默认只暴露 gRPC 端口(20001-20022)
//   - HTTP 端口(21001-21022 metrics 之外的端口段,如 21001 加 +10000 = 61001)按需启用
//   - login 服务作为 W2 第一个验证点,同时暴露 gRPC + HTTP(测 protoc-gen-go-http 工作流)
//
// 注意:**Pandora 客户端不直接走 HTTP**(走 Envoy gRPC-Web → gRPC),所以业务服 HTTP server
// 主要给运营场景用。Envoy 也直接转 gRPC 给业务,不需要业务服开 HTTP。
package http

import (
	"github.com/go-kratos/kratos/v2/middleware"
	khttp "github.com/go-kratos/kratos/v2/transport/http"

	"github.com/luyuancpp/pandora/pkg/config"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
)

// MustNewServer 创建并配置一个 Kratos HTTP Server。
//
// 默认 middleware 链跟 grpcserver 一致(同一套 Pandora middleware,协议无关):
//  1. Recovery     最外层捕 panic
//  2. Trace        trace_id 注入 / 透传
//  3. Logging      access log
//  4. Metrics      Prometheus 指标
//
// 用法:
//
//	srv := http.MustNewServer(cfg.Server.Http)
//	pb.RegisterLoginServiceHTTPServer(srv, loginsvc.New(...))
//	app := kratos.New(kratos.Server(srv))
func MustNewServer(c config.Http, customMW ...middleware.Middleware) *khttp.Server {
	mws := append([]middleware.Middleware{
		pmw.Recovery(),
		pmw.Trace(),
		pmw.Logging(),
		pmw.Metrics(),
	}, customMW...)

	opts := []khttp.ServerOption{
		khttp.Address(c.Addr),
		khttp.Middleware(mws...),
	}
	if c.Network != "" {
		opts = append(opts, khttp.Network(c.Network))
	}
	if c.Timeout > 0 {
		opts = append(opts, khttp.Timeout(c.Timeout.Std()))
	}

	return khttp.NewServer(opts...)
}
