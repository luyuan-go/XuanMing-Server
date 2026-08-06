// Package grpcclient 提供 Pandora 服务调用其它 gRPC 服务的客户端包装(基于 Kratos transport/grpc)。
//
// 设计:
//   - 包装 Kratos transport/grpc.Dial / DialInsecure
//   - 默认挂接 Pandora client middleware(Trace 透传 + Metrics)
//   - 服务发现:Kratos registry/etcd(W3+ 接入)/ 直连 endpoint(W2 简化版)
//
// 用法(直连):
//
//	conn := grpcclient.MustDial("127.0.0.1:20001")
//	defer conn.Close()
//	cli := loginpb.NewLoginServiceClient(conn)
//
// 用法(经服务发现):
//
//	conn := grpcclient.MustDialDiscovery("discovery:///pandora.login", reg)
//	cli := loginpb.NewLoginServiceClient(conn)
package grpcclient

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/selector"
	"github.com/go-kratos/kratos/v2/selector/wrr"
	kgrpc "github.com/go-kratos/kratos/v2/transport/grpc"
	"google.golang.org/grpc"

	pmw "github.com/luyuancpp/pandora/pkg/middleware"
)

// DefaultTimeout 是单次 RPC 默认超时(可被 ctx.WithTimeout 覆盖)。
const DefaultTimeout = 15 * time.Second

func init() {
	// 设置全局默认负载均衡为加权轮询(WRR)
	selector.SetGlobalSelector(wrr.NewBuilder())
}

// roundRobinServiceConfig 让 ClientConn 对解析出的**多个**后端做每-RPC round_robin
// (gRPC 官方 LB 策略,非 Kratos selector——后者只在走 WithDiscovery 时生效)。
const roundRobinServiceConfig = `{"loadBalancingConfig":[{"round_robin":{}}]}`

// MustDial 直连指定 endpoint(host:port),不走服务发现。
// W2 简化版用,W3+ 切到 MustDialDiscovery。
//
// 默认挂载 Trace + Metrics middleware,默认 15s 超时。
func MustDial(endpoint string, customMW ...middleware.Middleware) *grpc.ClientConn {
	return mustDial(false, endpoint, nil, DefaultTimeout, "", customMW...)
}

// MustDialDiscovery 经服务发现连接(target 形如 "discovery:///pandora.login")。
// reg 是 Kratos registry.Discovery 实现(etcd / consul / nacos)。
func MustDialDiscovery(endpoint string, reg registry.Discovery, customMW ...middleware.Middleware) *grpc.ClientConn {
	return mustDial(false, endpoint, reg, DefaultTimeout, "", customMW...)
}

// MustDialInsecure 同 MustDial,但显式声明 insecure(不强制 TLS)。
// 内网服务间通信用这个;Envoy 入站才用 TLS。
func MustDialInsecure(endpoint string, customMW ...middleware.Middleware) *grpc.ClientConn {
	return mustDial(true, endpoint, nil, DefaultTimeout, "", customMW...)
}

// MustDialInsecureRoundRobin 同 MustDialInsecure,但启用 gRPC round_robin 客户端负载均衡。
// endpoint 应指向 headless Service 的 DNS(形如 "dns:///hub-allocator-headless.pandora.svc.cluster.local:20018",
// headless = clusterIP:None,DNS 返回全部 Pod IP);gRPC dns 解析 + round_robin 每-RPC 轮询后端。
//
// 用途(P0#5):hub_allocator 是单写者,普通 ClusterIP 直连被 L4 钉在某一 Pod,落到非-writer
// 就永远非-writer。改用 round_robin 后,调用方对「非-writer 可重试错误」重发时每次 RPC 轮到
// 不同副本,数次内必命中当前 writer。单端点(dev 静态 host:port / passthrough,无 dns:/// 前缀)
// 下 round_robin 退化为单后端,行为与 MustDialInsecure 一致(§14 默认不坏)。
//
// 注意:LB 分发效果依赖 k8s headless DNS + gRPC dns 解析,只能在集群内运行时验证;本机 build
// 只能证明装配正确,不能证明分发行为(交接为在集群做冒烟:观察 RPC 落到多个 Pod)。
func MustDialInsecureRoundRobin(endpoint string, customMW ...middleware.Middleware) *grpc.ClientConn {
	return mustDial(true, endpoint, nil, DefaultTimeout, roundRobinServiceConfig, customMW...)
}

// MustDialInsecureTimeout 同 MustDialInsecure,但单次 RPC 默认超时用 timeout 而非 DefaultTimeout(15s)。
// 用于服务端会长时间阻塞的 RPC(如 matchmaker→ds_allocator.AllocateBattle 同步等 DS ready
// 心跳,agones allocate 5s + ready_wait 45s + 余量 ≈ 60s):kratos client 的 WithTimeout 是
// 每次调用都生效的中间件上限,不改拨号就算业务 ctx 给更长 deadline 也会被 15s 截断。
// timeout ≤ 0 时回退 DefaultTimeout。
func MustDialInsecureTimeout(endpoint string, timeout time.Duration, customMW ...middleware.Middleware) *grpc.ClientConn {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return mustDial(true, endpoint, nil, timeout, "", customMW...)
}

func mustDial(insecure bool, endpoint string, reg registry.Discovery, timeout time.Duration, serviceConfig string, customMW ...middleware.Middleware) *grpc.ClientConn {
	// 默认 client middleware:Trace 透传 + Metrics + 第 4 层熔断(SRE breaker)。
	// 熔断挂在 client 侧:下游故障时快速失败,避免雪崩拖垮调用方。
	mws := append([]middleware.Middleware{
		pmw.Trace(),
		pmw.Metrics(),
		pmw.CircuitBreaker(),
	}, customMW...)

	opts := []kgrpc.ClientOption{
		kgrpc.WithEndpoint(endpoint),
		kgrpc.WithTimeout(timeout),
		kgrpc.WithMiddleware(mws...),
	}
	if reg != nil {
		opts = append(opts, kgrpc.WithDiscovery(reg))
	}
	if serviceConfig != "" {
		// 注入 gRPC 官方 LB / 服务配置(如 round_robin):经 kgrpc.WithOptions 透传原生 DialOption。
		opts = append(opts, kgrpc.WithOptions(grpc.WithDefaultServiceConfig(serviceConfig)))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var (
		conn *grpc.ClientConn
		err  error
	)
	if insecure {
		conn, err = kgrpc.DialInsecure(ctx, opts...)
	} else {
		conn, err = kgrpc.Dial(ctx, opts...)
	}
	if err != nil {
		panic("grpcclient.MustDial " + endpoint + ": " + err.Error())
	}
	return conn
}

// WithTimeout 是给业务侧用的便捷函数,在 ctx 上设默认超时(如果 ctx 已有 deadline 则不覆盖)。
func WithTimeout(parent context.Context) (context.Context, context.CancelFunc) {
	if _, ok := parent.Deadline(); ok {
		return parent, func() {}
	}
	return context.WithTimeout(parent, DefaultTimeout)
}
