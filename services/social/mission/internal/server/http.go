// Package server 的 HTTP 侧 — 仅 /metrics(Prometheus 抓取;对齐 dialogue / friend)。
//
// mission.proto 没有 google.api.http 注解,本 HTTP server 只挂 /metrics。
package server

import (
	khttp "github.com/go-kratos/kratos/v2/transport/http"

	"github.com/luyuancpp/pandora/pkg/metrics"
	phttp "github.com/luyuancpp/pandora/pkg/transport/http"

	"github.com/luyuancpp/pandora/services/social/mission/internal/conf"
)

// NewHTTPServer 构造 HTTP server(默认 :21019),仅注册 /metrics。
func NewHTTPServer(cfg *conf.Config) *khttp.Server {
	srv := phttp.MustNewServer(cfg.Server.Http)
	srv.Handle("/metrics", metrics.MustHandler())
	return srv
}
