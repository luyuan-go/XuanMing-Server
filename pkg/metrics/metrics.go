// Package metrics 提供 Pandora 通用的 Prometheus 注册工具。
//
// 设计目标:
//   - 不抽业务指标(那是各服务自己写),只提供通用工具
//   - 统一 promhttp Handler(`MustHandler()`)
//   - 提供标准时间分桶常量(`StandardBuckets`)
//   - Register 包装,忽略 AlreadyRegisteredError,允许 init 重复注册
//
// 命名规范(docs/design/infra.md §10):
//
//	pandora_<service>_<metric>{<labels>}
//
// 强制 label:service / instance(由抓取端加,代码不写)
// 禁止高基数 label:player_id / match_id 不能放 label
package metrics

import (
	"errors"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StandardBuckets 是延迟直方图的标准分桶(1ms ~ 4s,12 桶)。
// 适合大部分 RPC / DB / Redis / Kafka 调用。
var StandardBuckets = prometheus.ExponentialBuckets(0.001, 2, 12)

// LongRunningBuckets 适合慢操作(5ms ~ 20s,12 桶),如 cross-process flush。
var LongRunningBuckets = prometheus.ExponentialBuckets(0.005, 2, 12)

// MustHandler 返回标准 promhttp.Handler。
//
//	http.Handle("/metrics", metrics.MustHandler())
//	http.ListenAndServe(":21001", nil)
func MustHandler() http.Handler {
	return promhttp.Handler()
}

// Register 注册 collector,忽略 AlreadyRegisteredError。
// 业务在 init() 里直接调:
//
//	var loginQPS = prometheus.NewCounterVec(...)
//	func init() { metrics.Register(loginQPS) }
func Register(c prometheus.Collector) {
	if err := prometheus.Register(c); err != nil {
		var alreadyRegistered prometheus.AlreadyRegisteredError
		if errors.As(err, &alreadyRegistered) {
			// 多个 init 重复注册,默默吞掉
			return
		}
		panic("metrics.Register failed: " + err.Error())
	}
}

// MustRegister 注册 collector,失败 panic(包括 AlreadyRegisteredError)。
// 业务慎用,主要给单测。
func MustRegister(cs ...prometheus.Collector) {
	prometheus.MustRegister(cs...)
}
