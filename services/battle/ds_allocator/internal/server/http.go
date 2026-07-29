// Package server — HTTP server 注册(/metrics + /healthz/writer)。
//
// ds/allocator.proto 没有 google.api.http 注解,本 HTTP server 只挂运维面:
// Prometheus 抓 /metrics;心跳扫描的写者继任租约健康度走 /healthz/writer。
package server

import (
	"encoding/json"
	"net/http"

	khttp "github.com/go-kratos/kratos/v2/transport/http"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/luyuancpp/pandora/pkg/dsauthfence/writerlease"
	"github.com/luyuancpp/pandora/pkg/metrics"
	phttp "github.com/luyuancpp/pandora/pkg/transport/http"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
)

// WriterLeaseHealth 是 /healthz/writer 的数据源(*writerlease.Lease 满足)。
type WriterLeaseHealth interface {
	Health() writerlease.HealthSnapshot
}

// WriterHealthHolder 让 HTTP server 先于写者租约构造(租约只在 Model B 下启动,且必须
// 排在 dsauthfence capability 之后),由 main 在租约就绪后注入。
//
// **它不是 K8s 探针**(与 hub_allocator 同一结论):失去领导权的副本是**有意的热备**——
// 它照常服务 Heartbeat / AllocateBattle,只是不跑心跳超时扫描。把 readiness 门成
// "必须是 writer" 会在滚动升级时死锁,并把"扫描降级"放大成"整服零 allocator 端点",
// 而那正是本次事故要消除的东西。故只暴露状态供告警。
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

// ── Prometheus 指标 ────────────────────────────────────────────────────────
//
// JSON 端点只能人工 curl,告警系统无法表达"全集群都没有扫描者"。真正的全局信号是
// **没有任何副本上报 held=1**:
//
//	sum(pandora_ds_allocator_writer_held) == 0  持续 > lease TTL 兜底窗口
//	  → 长期无人执行心跳超时扫描 = §9 不变量 4 的 DS 崩溃补偿链停摆,必须告警。
//
// 它同时覆盖 Campaign 永久阻塞、全体竞选失败两种形态,不依赖"函数返回了错误"。
var (
	writerHeldGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pandora_ds_allocator_writer_held",
		Help: "1 = this replica currently holds the ds_allocator heartbeat-sweep writer lease.",
	})
	writerDegradedGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pandora_ds_allocator_writer_degraded",
		Help: "1 = this replica is neither the sweep writer nor able to become one (campaign failing persistently).",
	})
	writerTokenGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pandora_ds_allocator_writer_token",
		Help: "Current fencing token of this replica's writer term (0 = not held).",
	})
	writerCampaignErrsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pandora_ds_allocator_writer_campaign_errors",
		Help: "Consecutive writer-lease campaign failures (reset on election).",
	})
	writerActivationErrsGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pandora_ds_allocator_writer_activation_errors",
		Help: "Consecutive writer-lease activation failures (elected but not yet writable; reset on success).",
	})
	writerLeaseEnabledGauge = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pandora_ds_allocator_writer_lease_enabled",
		Help: "1 = the sweep writer succession lease is wired on this replica (0 = off; single sweeper relies on the deploy strategy).",
	})
)

func init() {
	metrics.Register(writerHeldGauge)
	metrics.Register(writerDegradedGauge)
	metrics.Register(writerTokenGauge)
	metrics.Register(writerCampaignErrsGauge)
	metrics.Register(writerActivationErrsGauge)
	metrics.Register(writerLeaseEnabledGauge)
}

// snapshot 取一次快照并同步刷新指标(/metrics 与 /healthz/writer 同源,不会漂移)。
// 未注入租约时明示 enabled=false 并把 held 记为 0——不能把"根本没启用继任租约"
// 伪装成健康(hub_allocator R11 复审指出的 fail-open,此处不重犯)。
func (h *WriterHealthHolder) snapshot() writerHealthBody {
	if h.lease == nil {
		writerLeaseEnabledGauge.Set(0)
		writerHeldGauge.Set(0)
		writerDegradedGauge.Set(0)
		writerTokenGauge.Set(0)
		return writerHealthBody{Enabled: false, Mode: h.mode}
	}
	snap := h.lease.Health()
	body := writerHealthBody{
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
	writerLeaseEnabledGauge.Set(1)
	writerHeldGauge.Set(boolGauge(body.Held))
	writerDegradedGauge.Set(boolGauge(body.Degraded))
	writerTokenGauge.Set(float64(body.Token))
	writerCampaignErrsGauge.Set(float64(body.ConsecutiveCampaignErrs))
	writerActivationErrsGauge.Set(float64(body.ConsecutiveActivationErrs))
	return body
}

func boolGauge(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// ServeHTTP 输出快照。状态码:degraded → 503,其余 → 200(含"健康热备")。
func (h *WriterHealthHolder) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	body := h.snapshot()
	w.Header().Set("Content-Type", "application/json")
	if body.Degraded {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(body)
}

// NewHTTPServer 构造 HTTP server,注册 /metrics 与 /healthz/writer。
// /metrics 前置刷新写者指标:抓取端只抓 /metrics 也能拿到最新写者状态。
func NewHTTPServer(cfg *conf.Config, writerHealth *WriterHealthHolder) *khttp.Server {
	srv := phttp.MustNewServer(cfg.Server.Http)
	promHandler := metrics.MustHandler()
	srv.Handle("/metrics", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writerHealth.snapshot()
		promHandler.ServeHTTP(w, r)
	}))
	srv.Handle("/healthz/writer", writerHealth)
	return srv
}
