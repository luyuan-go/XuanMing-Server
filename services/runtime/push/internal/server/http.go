// Package server — HTTP server 注册。
//
// W2 设计:push 服务 HTTP server(:21014)只承载 /metrics。
//
// 跟 login 服务的区别(login 同时挂 /v1/login 等 RESTful):
//   - push.proto **没有** google.api.http 注解(详见 proto/pandora/push/v1/push.proto)
//   - 因此 buf 不会生成 push_http.pb.go,也没有 RegisterPushServiceHTTPServer
//   - server stream 在 HTTP/1.1 RESTful 下表达不出来,客户端必须走 gRPC-Web(Envoy 转发)
//
// 选择仍起 HTTP server 的原因:
//  1. /metrics 端口跟 login 对齐(infra.md §6.3 各服务 metrics 端口 21001~21022)
//  2. Prometheus 配置 deploy/prometheus/prometheus.yml 已固定 21014 抓取目标
//  3. 给将来运营后台 / 健康检查留一个 RESTful 入口
package server

import (
	khttp "github.com/go-kratos/kratos/v2/transport/http"

	"github.com/luyuancpp/pandora/pkg/metrics"
	phttp "github.com/luyuancpp/pandora/pkg/transport/http"

	"github.com/luyuancpp/pandora/services/runtime/push/internal/conf"
)

// NewHTTPServer 构造 HTTP server,仅注册 /metrics(无 RESTful RPC handler)。
func NewHTTPServer(cfg *conf.Config) *khttp.Server {
	srv := phttp.MustNewServer(cfg.Server.Http)
	srv.Handle("/metrics", metrics.MustHandler())
	return srv
}
