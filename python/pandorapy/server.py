"""双 server 启动骨架 —— 一个进程同时跑 grpc.aio 和 FastAPI/uvicorn。

这是"Kratos 那一层"在 Python 侧的另一半替代物。对应 Go 侧:

    app := kratos.New(
        kratos.Name(serviceName),
        kratos.Logger(logger),
        kratos.Server(grpcSrv, httpSrv),   ← 两个 transport 平行跑
    )
    app.Run()

拓扑(与现状完全一致,端口号都不变):

                        ┌── :2001x  gRPC ──→ grpc.aio  ← Envoy 打这里
    一个进程 ───────────┤
                        └── :2101x  HTTP ──→ FastAPI   ← Prometheus 抓这里

    21 个服务现在每个都有 internal/server/grpc.go + internal/server/http.go,
    这里就是那两个文件合起来的对应物。**不是串联** —— 两个 server 互不经过对方。

为什么 gRPC 不用 FastAPI 扛:
    FastAPI 是 ASGI/HTTP 框架,说不了 h2c gRPC(HTTP/2 分帧 + grpc-status trailer +
    grpc-timeout)。让它扛 RPC 就得手写 gRPC-Web 分帧和 trailer,而客户端是 UE C++、
    中间还夹着 Envoy 的 grpc_web filter,三方任一理解偏差都要在 UE 里打断点比对字节。
    grpcio 底下是官方的 C 版 gRPC core —— 和 Go 侧 google.golang.org/grpc 同一份,
    所以这些细节不用自己承担。
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
from collections.abc import Awaitable, Callable, Sequence
from typing import Any

import grpc
import uvicorn
from fastapi import FastAPI
from grpc_reflection.v1alpha import reflection

from pandorapy import config as pconfig
from pandorapy import interceptors as pintercept
from pandorapy import log as plog
from pandorapy import metrics


def build_grpc_server(
    grpc_conf: pconfig.GrpcConf,
    *,
    auth_required: bool = False,
    extra_interceptors: Sequence[grpc.aio.ServerInterceptor] = (),
) -> grpc.aio.Server:
    """构造 gRPC server(未注册 servicer、未 start)。

    对应 Go 侧 pkg/grpcserver.MustNewServer(cfg.Server, pmw.AuthOptional())。
    """
    options: list[tuple[str, Any]] = []

    # max_conn_age → grpc.max_connection_age_ms。
    # 作用与 Go 侧一致:达龄发 GOAWAY 让客户端重拨,滚动更新时流量才能滚到新副本
    # (zero-downtime §6.2)。不设这个,老连接会一直粘在旧 Pod 上。
    max_age = grpc_conf.max_conn_age_td()
    if max_age.total_seconds() > 0:
        options.append(("grpc.max_connection_age_ms", int(max_age.total_seconds() * 1000)))

    # 拦截器顺序:先可观测(要能记录到 auth 的拒绝),再鉴权。
    # 与 Kratos 的 middleware 链同序 —— recovery/metrics 在最外层。
    chain: list[grpc.aio.ServerInterceptor] = [
        pintercept.ObservabilityInterceptor(),
        pintercept.AuthInterceptor(required=auth_required),
        *extra_interceptors,
    ]
    return grpc.aio.server(interceptors=chain, options=options)


def enable_reflection(server: grpc.aio.Server, service_full_names: Sequence[str]) -> None:
    """按配置开 reflection。

    对应 Go 侧 cfg.Server.Grpc.EnableReflection —— dev 开(grpcurl list 能用),
    prod 零值 false = 关,少暴露一个攻击面。调用方负责判断开关,这里只负责注册。
    """
    reflection.enable_server_reflection(
        [*service_full_names, reflection.SERVICE_NAME], server
    )


def build_http_app(service_name: str) -> FastAPI:
    """构造 HTTP app。

    20 个服务里它只承载 /metrics + /healthz —— 对应 Go 侧那 20 份
    `internal/server/http.go`(注释都写着「仅 /metrics」)。
    只有 login 一个服务需要在这上面加 10 个 REST 路由(login.proto 的 http 注解)。
    """
    app = FastAPI(
        title=f"pandora-{service_name}",
        # docs 默认关:这是运维面而不是公开 API,而且 openapi 会把内部路由结构暴露出去。
        # 需要时用 PANDORA_HTTP_DOCS=1 单独打开(dev 联调)。
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    # /metrics 不套业务中间件 —— 与 Go 侧注释一致:「纯 Prometheus,不经过 Pandora
    # middleware,避免 trace/log 污染监控」。
    app.add_route("/metrics", metrics.metrics_endpoint, methods=["GET"])

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """存活探针。K8s liveness/readiness 用;Go 侧靠 Kratos 内置健康检查。"""
        return {"status": "ok", "service": service_name}

    return app


def normalize_grpc_addr(addr: str) -> str:
    """把 Go 风格的裸端口 ":20013" 转成 grpcio 能接受的形式。

    实测差异(2026-08-18):
        Go 的 net.Listen("tcp", ":20013") 接受裸端口,含义是"监听所有接口、双栈"。
        grpcio 的 add_insecure_port(":20013") 直接失败:
            Failed to add port to server: Unparsable name: :20013
            RuntimeError: Failed to bind to address :20013

    而 21 份 etc/*.yaml 里写的**全都是**裸端口形式(":20013"),这些 yaml 要同时喂给
    Go 版和 Python 版,不能改。所以在这里做归一化,而不是去改配置。

    选 "[::]:" 而不是 "0.0.0.0:":前者是双栈(IPv4 + IPv6),与 Go 的 ":port" 语义一致;
    用 0.0.0.0 会只监听 IPv4,若客户端或 Envoy 走 IPv6 回环就连不上。
    """
    if addr.startswith(":"):
        return f"[::]{addr}"
    return addr


def _addr_to_host_port(addr: str, default_port: int) -> tuple[str, int]:
    """把 yaml 的 ":21013" / "0.0.0.0:21013" 拆成 uvicorn 需要的 (host, port)。

    裸端口的 host 取 "0.0.0.0" 而**不是** "::" —— 这里和 normalize_grpc_addr 的选择相反,
    是实测出来的差异(2026-08-18):

        grpcio  add_insecure_port("[::]:20013")  → 双栈,netstat 同时出现
                                                   0.0.0.0:20013 和 [::]:20013
        uvicorn host="::"                        → **仅 IPv6**,netstat 只有 [::]:21013,
                                                   curl http://127.0.0.1:21013 直接 connection refused

    为什么这个差异必须按 IPv4 收口:
        HTTP 端口唯一的消费者是 Prometheus 抓取和 K8s liveness/readiness 探针,
        它们在集群里走 IPv4。绑成 IPv6-only 的后果是 /metrics 抓不到 →
        **Grafana 面板静默变空**,而服务本身完全健康、日志一切正常,没有任何报错。
        这跟日志字段漂移是同一类故障,只是入口不同。

    IPv6-only 环境需要时,在 yaml 里显式写 "[::]:21013",这里会原样透传。
    """
    if not addr:
        return "0.0.0.0", default_port
    host, _, port = addr.rpartition(":")
    return (host or "0.0.0.0"), int(port)


async def run(
    *,
    service_name: str,
    grpc_server: grpc.aio.Server,
    grpc_addr: str,
    http_app: FastAPI | None = None,
    http_addr: str = "",
    http_default_port: int = 0,
    on_ready: Callable[[], None] | None = None,
    background: Sequence[Callable[[], Awaitable[None]]] = (),
) -> None:
    """启动两个 server + 后台任务,阻塞到收到 SIGINT/SIGTERM。

    background 是需要随进程生命周期存活的协程(如 dialogue 的会话过期清理),
    对应 Go 侧那些 `go runXxx(ctx)` 的 goroutine。它们会在收到停止信号时被取消。
    """
    logger = plog.get()

    # 归一化:yaml 里是 Go 风格裸端口 ":20013",grpcio 不接受,见 normalize_grpc_addr。
    grpc_server.add_insecure_port(normalize_grpc_addr(grpc_addr))
    await grpc_server.start()

    tasks: list[asyncio.Task] = []
    http_server: uvicorn.Server | None = None
    if http_app is not None:
        host, port = _addr_to_host_port(http_addr, http_default_port)
        http_server = uvicorn.Server(
            uvicorn.Config(
                http_app,
                host=host,
                port=port,
                # log_config=None:不让 uvicorn 装自己的 logging 配置,
                # 否则它会覆盖 structlog 的 handler,HTTP 侧日志字段口径就跟 Go 对不上了。
                log_config=None,
                access_log=False,
                lifespan="on",
            )
        )
        tasks.append(asyncio.create_task(http_server.serve(), name="http"))

    for factory in background:
        tasks.append(asyncio.create_task(factory(), name=getattr(factory, "__name__", "bg")))

    if on_ready is not None:
        on_ready()

    stop = asyncio.Event()

    def _request_stop(*_args: object) -> None:
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError, ValueError):
            # Windows 的 ProactorEventLoop 不支持 add_signal_handler;
            # 退化到 signal.signal,行为一致(dev 在 Windows 跑,prod 在 Linux)。
            loop.add_signal_handler(sig, _request_stop)
    else:
        with contextlib.suppress(ValueError, OSError):
            signal.signal(signal.SIGINT, _request_stop)
            signal.signal(signal.SIGTERM, _request_stop)

    await stop.wait()
    logger.info("service_stopping", service=service_name)

    # 优雅停机:先停收新请求(grace 期内让在途请求做完),再停 HTTP,最后取消后台任务。
    # 顺序与 Kratos 的 app.Stop 一致 —— 反过来会让在途请求访问到已关闭的资源。
    await grpc_server.stop(grace=5.0)
    if http_server is not None:
        http_server.should_exit = True
    for task in tasks:
        task.cancel()
    for task in tasks:
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
    logger.info("service_stopped", service=service_name)
