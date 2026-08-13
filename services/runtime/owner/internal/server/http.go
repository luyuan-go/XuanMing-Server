// Package server — HTTP server 注册(/metrics,以及按需开启的 /debug/pprof)。
//
// owner.proto 没有 google.api.http 注解,本 HTTP server 只挂 /metrics 给 Prometheus 抓。
package server

import (
	"net/http"
	"net/http/pprof"
	"os"
	"runtime"
	"time"

	khttp "github.com/go-kratos/kratos/v2/transport/http"

	"github.com/luyuancpp/pandora/pkg/metrics"
	phttp "github.com/luyuancpp/pandora/pkg/transport/http"

	"github.com/luyuancpp/pandora/services/runtime/owner/internal/conf"
)

// pprofEnableEnv 是 /debug/pprof 的开关(值为 "1" 时开启)。
//
// 为什么用环境变量而不是配置项:pprof 是**排障时临时开、查完就关**的能力,
// 走 conf 就得改 schema + 改 ConfigMap + 重启,而排障现场需要的恰恰是最短路径。
// 环境变量改 Deployment 一行即可,且默认不开——profile 端点会暴露函数名、
// goroutine 栈与内存布局,不该在无人排障时长期开着。
const pprofEnableEnv = "PANDORA_PPROF"

// NewHTTPServer 构造 HTTP server:恒挂 /metrics;PANDORA_PPROF=1 时额外挂 /debug/pprof。
func NewHTTPServer(cfg *conf.Config) *khttp.Server {
	srv := phttp.MustNewServer(cfg.Server.Http)
	srv.Handle("/metrics", metrics.MustHandler())

	// 显式逐条注册,不用 http.DefaultServeMux:导入 net/http/pprof 的副作用是往
	// DefaultServeMux 注册,而把整个 DefaultServeMux 挂上去会连带暴露任何第三方库
	// 偷偷注册过的处理器——那是不可控的攻击面。
	if os.Getenv(pprofEnableEnv) == "1" {
		// 阻塞/互斥采样默认是关的(rate=0),不开的话 /debug/pprof/block 与 mutex 恒为空。
		// 而本服务要查的正是「中位 1ms、P99 19s」的长尾——CPU profile 对这种问题没用
		// (卡住的 goroutine 不烧 CPU),block profile 才能指出它阻塞在 SQL、连接池还是锁上。
		//
		// 采样率取 1e7 纳秒(10ms):只记录阻塞超过约 10ms 的事件。全采(rate=1)会给热路径
		// 带来可观开销,而我们要找的是秒级长尾,10ms 门槛足够细且几乎无成本。
		runtime.SetBlockProfileRate(int(10 * time.Millisecond.Nanoseconds()))
		runtime.SetMutexProfileFraction(100)

		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		srv.HandlePrefix("/debug/pprof/", mux)
	}
	return srv
}
