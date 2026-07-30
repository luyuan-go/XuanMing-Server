// payload.go — 写入侧 payload 字节数守护(dbguard 的"根治层",2026-07-22)。
//
// 为什么写入侧校验才是根治:
//
//	巡检(Check/CheckColumns)只能在数据**已经**变胖之后发现,属事后补救;
//	MySQL 列长度是最后一道闸,但触发时要么整条写入失败(严格模式,业务报错),
//	要么静默截断(非严格,数据损坏)——两种都是"已经出事"。
//	写入侧在序列化后、落库前拦一道,才能既保住数据完整性,又在**逼近**上限时提前告警。
//
// 三档语义(核心设计:不是只有"爆了才报",而是"逼近就报"):
//
//	size >= Max          → 返回 error(fail-closed 拒写)+ ERROR 日志。宁可拒绝一次写入,
//	                       也不能让超长数据落库被截断(验收底线第 3 条:完整性优先)。
//	size >= Max*WarnRatio → 放行 + WARN 日志。**这是留给人排查的窗口**:数据还没坏,
//	                       但增长趋势已异常,此时查根因(哪个 repeated 字段在涨)成本最低。
//	否则                  → 放行,不打日志(热路径零噪音)。
//
// 所有档位都记 histogram,因为**只看 max 会被单个异常值带偏**:要看 p50/p95/p99 才能
// 区分"个别玩家数据畸形"(p99 正常、max 爆)与"全体在普遍变胖"(p99 一起涨,说明是设计问题)。
package dbguard

import (
	"context"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/metrics"
)

// WarnRatio 是逼近告警线(占 Max 的比例)。达到即 WARN,给人留出排查窗口。
const WarnRatio = 0.8

// payloadBytes 记录每次写入的 payload 字节分布。
// 桶 64B ~ 64KB(覆盖 VARBINARY(512) 到 BLOB(65535) 的全部量级)。
var payloadBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "pandora_db_payload_bytes",
	Help:    "写入 payload 字节分布。看 p99 而非 max:p99 正常+max 爆=个别数据畸形;p99 一起涨=设计性无界增长。",
	Buckets: prometheus.ExponentialBuckets(64, 2, 11),
}, []string{"db", "table", "column"})

var payloadRejects = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "pandora_db_payload_rejected_total",
	Help: "因超过字节上限被拒绝的写入次数(fail-closed)。非零即需人工排查。",
}, []string{"db", "table", "column"})

func init() {
	metrics.Register(payloadBytes)
	metrics.Register(payloadRejects)
}

// PayloadLimit 描述一个大字段的写入侧字节预算。
//
// Max 的取值口径:**必须 ≤ DB 列类型容量**,且应留出余量。
// 例如列是 VARBINARY(512),Max 取 512 即可(DB 会兜底);列是 BLOB(65535) 但设计只装
// 20 个附件 ≈ 2KB,Max 就该取 4096 而不是 65535——**按设计期望定,不按类型上限定**,
// 否则等于没设:数据涨到 60KB 才告警时,业务语义早就崩了(附件表已经无界)。
type PayloadLimit struct {
	DB     string // metric label,如 pandora_social
	Table  string
	Column string
	Max    int // 字节上限(含),>= 该值拒写
	// Hint 进日志,告诉排查者"该看哪个字段/哪个上限配置"。
	Hint string
}

// CheckPayload 在落库前校验 payload 字节数。
//
// 返回非 nil 时调用方**必须**放弃本次写入(fail-closed),并把错误按业务语义包成 errcode:
// 客户端输入导致的包 ErrInvalidArg,系统内部生成的包 ErrInternal(那是服务端 bug)。
//
// ctx 只用于取日志上下文(trace/player id),不做取消——校验是纯内存计算。
func CheckPayload(ctx context.Context, limit PayloadLimit, payload []byte) error {
	size := len(payload)
	payloadBytes.WithLabelValues(limit.DB, limit.Table, limit.Column).Observe(float64(size))
	if limit.Max <= 0 {
		return nil // 未设预算:不校验(但仍记 histogram,便于事后定阈值)
	}
	if size >= limit.Max {
		payloadRejects.WithLabelValues(limit.DB, limit.Table, limit.Column).Inc()
		plog.With(ctx).Errorw(
			"msg", "db_payload_too_large_rejected",
			"db", limit.DB, "table", limit.Table, "column", limit.Column,
			"size", size, "max", limit.Max, "hint", limit.Hint,
			"detail", "已拒绝本次写入(fail-closed);放行会被 MySQL 报错或静默截断。见 docs/design/db-capacity-guard.md",
		)
		return fmt.Errorf("payload too large for %s.%s: %d bytes >= limit %d (%s)",
			limit.Table, limit.Column, size, limit.Max, limit.Hint)
	}
	if float64(size) >= float64(limit.Max)*WarnRatio {
		// 逼近上限:数据还没坏,但趋势异常——这是排查成本最低的时点。
		plog.With(ctx).Warnw(
			"msg", "db_payload_approaching_limit",
			"db", limit.DB, "table", limit.Table, "column", limit.Column,
			"size", size, "max", limit.Max, "ratio", float64(size)/float64(limit.Max),
			"hint", limit.Hint,
			"detail", "尚未超限但已达告警线;此时查 blob 内哪个 repeated 字段在增长,成本远低于爆表后补救",
		)
	}
	return nil
}
