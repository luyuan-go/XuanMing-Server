// Package biz —— player_locator 服务业务级 prometheus 指标。
//
// 命名规范(docs/design/infra.md §10):
//
//	pandora_locator_<metric>{<label>...}
//
// 强制 label:service / instance 由抓取端加,代码不写。
// 禁止高基数 label:player_id / hub_pod 永远不能放 label。
package biz

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/luyuancpp/pandora/pkg/metrics"
)

// HubPresenceLegacyDegraded 统计「Hub DS 没带连接级 fence,只能走安全降级」的次数。
//
// label:
//   - op:"report_disconnect"(断线上报被整条跳过)| "set_location"(HUB 写走 legacy 模式)。
//     低基数(2 值)。
//
// **为什么这条必须有指标而不只是日志**:连接级 fence 是 Hub DS 与 locator 之间的**新协议**,
// 客户端没接上时服务端只能安全降级。降级本身是对的(滚动升级要能跑),但后果是
// 静默的 —— `report_disconnect` 降级时:不缩 TTL(10s 快速离线判定退回 30s)、
// 不记 last-seen、不发离场事件,于是**所有按「离线满 N 秒」做决策的下游功能
// (组队自动退队等)一个都不会触发**,而链路上每一环看起来都健康。
//
// 这正是「测试全绿但功能一个人也踢不掉」的那类故障:没有报错、没有失败请求,
// 只是什么都没发生。所以必须计数 + 告警,不能只留一条日志等人去翻。
//
// 告警阈值:滚动升级窗口之外 rate(...{op="report_disconnect"}[5m]) > 0 即告警 ——
// 稳态下所有 Hub DS 都应带 fence,持续 > 0 说明客户端根本没接这个协议。
var HubPresenceLegacyDegraded = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "pandora_locator_hub_presence_legacy_degraded_total",
		Help: "Hub DS 未携带连接级 presence fence 而走安全降级的次数(稳态应恒为 0;report_disconnect 降级会让离线时长类功能整体静默失效)",
	},
	[]string{"op"},
)

// legacy 降级的 op label 取值。
const (
	legacyOpReportDisconnect = "report_disconnect"
	legacyOpSetLocation      = "set_location"
)

func init() {
	metrics.Register(HubPresenceLegacyDegraded)
}
