// Package server — HTTP server 注册(/metrics + /healthz/writer)。
//
// hub/allocator.proto 没有 google.api.http 注解,本 HTTP server 只挂运维面:
// Prometheus 抓 /metrics;写者继任租约健康度走 /healthz/writer(见 WriterHealthHolder)。
package server

import (
	"encoding/json"
	"net/http"

	khttp "github.com/go-kratos/kratos/v2/transport/http"

	"github.com/luyuancpp/pandora/pkg/dsauthfence/writerlease"
	"github.com/luyuancpp/pandora/pkg/metrics"
	phttp "github.com/luyuancpp/pandora/pkg/transport/http"

	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/conf"
)

// WriterLeaseHealth 是 /healthz/writer 的数据源(*writerlease.Lease 满足)。
type WriterLeaseHealth interface {
	Health() writerlease.HealthSnapshot
}

// WriterHealthHolder 让 HTTP server 先于写者租约构造(租约只在 Model B 下启动,且必须
// 排在 dsauthfence capability 之后),由 main 在租约就绪后注入。
//
// **它不是 K8s 探针**(R10 复审 P0-2,方案见 audit-residual-architecture-20260724.md A1):
// 失去领导权的副本是**有意的热备**——拒写但可秒级接管。把 readiness 门成"必须是 writer"
// 会在滚动升级时死锁(新副本要 Ready 才能让旧副本 Resign,旧副本不 Resign 新副本就当不上
// writer),全体无法当选时更会把"写降级"放大成"整服零端点"。故只暴露状态供告警:
// degraded=true 持续超过 lease TTL 兜底窗口 = 真的长期无主,该报警。
type WriterHealthHolder struct {
	// 只在 main 启动阶段写一次,之后只读;HTTP handler 要到 app.Run() 之后才可能被调用,
	// 与该写入之间有 happens-before(kratos Server.Start),无需额外同步。
	lease WriterLeaseHealth
	mode  string
}

// Set 注入租约与档位(mode 取值见 conf.WriterLeaseMode)。未调用 = 未启用继任租约。
func (h *WriterHealthHolder) Set(lease WriterLeaseHealth, mode string) {
	h.lease = lease
	h.mode = mode
}

// writerHealthBody 是 /healthz/writer 的响应体(运维/告警可直接解析)。
type writerHealthBody struct {
	Enabled                   bool   `json:"enabled"`
	Mode                      string `json:"mode,omitempty"`
	Held                      bool   `json:"held"`
	Token                     uint64 `json:"token"`
	ConsecutiveCampaignErrs   uint64 `json:"consecutive_campaign_errs"`
	LastCampaignErr           string `json:"last_campaign_err,omitempty"`
	ConsecutiveActivationErrs uint64 `json:"consecutive_activation_errs"`
	LastActivationErr         string `json:"last_activation_err,omitempty"`
	EscalateAfter             uint64 `json:"escalate_after"`
	Degraded                  bool   `json:"degraded"`
}

// ServeHTTP 输出快照。状态码:degraded → 503,其余 → 200(含"健康热备")。
func (h *WriterHealthHolder) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	body := writerHealthBody{}
	if h.lease != nil {
		snap := h.lease.Health()
		body = writerHealthBody{
			Enabled:                   true,
			Mode:                      h.mode,
			Held:                      snap.Held,
			Token:                     snap.Token,
			ConsecutiveCampaignErrs:   snap.ConsecutiveCampaignErrs,
			LastCampaignErr:           snap.LastCampaignErr,
			ConsecutiveActivationErrs: snap.ConsecutiveActivationErrs,
			LastActivationErr:         snap.LastActivationErr,
			EscalateAfter:             snap.EscalateAfter,
			Degraded:                  snap.Degraded(),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	if body.Degraded {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(body)
}

// NewHTTPServer 构造 HTTP server,注册 /metrics 与 /healthz/writer。
func NewHTTPServer(cfg *conf.Config, writerHealth *WriterHealthHolder) *khttp.Server {
	srv := phttp.MustNewServer(cfg.Server.Http)
	srv.Handle("/metrics", metrics.MustHandler())
	srv.Handle("/healthz/writer", writerHealth)
	return srv
}
