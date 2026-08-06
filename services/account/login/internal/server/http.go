// Package server — HTTP server 注册。
//
// W2 设计:同一个 HTTP server(:21001)同时承载:
//   - /metrics                    Prometheus 抓取端点(infra.md §10)
//   - /v1/login / /v1/logout ...  Kratos protoc-gen-go-http 生成的 RESTful handler
//
// 选择单 server 双用途的原因:
//  1. 减少进程内端口数量,运维 / 防火墙更简单
//  2. Prometheus 已经配 scrape 21001(deploy/prometheus/prometheus.yml)
//  3. Pandora 客户端不直走 HTTP RPC(走 Envoy gRPC-Web),HTTP RPC 仅给运营 / 联调用,QPS 低
//
// W3+ 可拆:metrics 独占 21001,HTTP RPC 移到 61001(见 pkg/transport/http 注释)。
package server

import (
	khttp "github.com/go-kratos/kratos/v2/transport/http"

	"github.com/luyuancpp/pandora/pkg/metrics"
	phttp "github.com/luyuancpp/pandora/pkg/transport/http"
	loginv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/login/v1"

	"github.com/luyuancpp/pandora/services/account/login/internal/conf"
	"github.com/luyuancpp/pandora/services/account/login/internal/service"
)

// NewHTTPServer 构造 HTTP server,注册 metrics + LoginService HTTP handler。
func NewHTTPServer(cfg *conf.Config, svc *service.LoginService) *khttp.Server {
	srv := phttp.MustNewServer(cfg.Server.Http)

	// /metrics 端点(纯 Prometheus,不经过 Pandora middleware,避免 trace/log 污染监控)
	srv.Handle("/metrics", metrics.MustHandler())

	// LoginService 的 RESTful handler(/v1/login, /v1/logout 等)
	loginv1.RegisterLoginServiceHTTPServer(srv, svc)

	return srv
}
