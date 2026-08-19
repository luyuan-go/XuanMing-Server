"""Prometheus 指标 —— 对应 Go 侧 pkg/metrics(60 行,自定义指标很少)。

Grafana 面板认的是**指标名 + label 名**,和日志字段口径同理:名字漂移 = 面板静默变空。
Go 侧现有自定义指标只有 pandora_db_table_rows / _budget 两族(dbguard 用),
dialogue 不产生自定义业务指标,所以本模块当前只负责:
  - 暴露一个 /metrics ASGI app,挂到 FastAPI 上(端口 2100x,与 Go 侧一致)
  - 提供 RPC 层通用指标,供拦截器统一打点

⚠️ 刻意**不**用 prometheus_client 的默认全局 REGISTRY 之外的自定义 registry:
   默认 registry 自带 process_* / python_gc_* 采集器,是排查 Python 侧内存/GC 问题
   (迁移期最可能出问题的地方)的免费信息源,不要为了"干净"把它关掉。
"""

from __future__ import annotations

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from starlette.requests import Request
from starlette.responses import Response

# ── RPC 层通用指标 ────────────────────────────────────────────────────────────
#
# 命名沿用 Prometheus/gRPC 生态惯例(grpc_server_*),而不是自造 pandora_rpc_*:
# 迁移期 Go 版和 Python 版会同时在线,同名指标能让同一块面板直接对比两个实现的
# 延迟与错误率 —— 这正是灰度时最需要看的东西。
RPC_STARTED = Counter(
    "grpc_server_started_total",
    "gRPC 请求进入数",
    ["grpc_service", "grpc_method"],
)
RPC_HANDLED = Counter(
    "grpc_server_handled_total",
    "gRPC 请求完成数(按业务错误码分)",
    # errcode 而不是 grpc_code:本仓库的业务失败走 response.code(errcode),
    # gRPC status 基本恒为 OK。只看 grpc_code 会以为一切正常。
    ["grpc_service", "grpc_method", "errcode"],
)
RPC_LATENCY = Histogram(
    "grpc_server_handling_seconds",
    "gRPC 请求处理耗时",
    ["grpc_service", "grpc_method"],
    # 分桶按游戏后端 unary RPC 的实际关切设置:1ms 到 5s。
    # Python 侧比 Go 慢是预期的,桶必须能分辨"慢一点"和"慢一个数量级"。
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
RPC_PANIC = Counter(
    "grpc_server_panics_total",
    "gRPC handler 未捕获异常数",
    ["grpc_service", "grpc_method", "exc_type"],
)


async def metrics_endpoint(_request: Request) -> Response:
    """/metrics 处理器。对应 Go 侧 srv.Handle("/metrics", metrics.MustHandler())。

    Go 侧注释明确写了「纯 Prometheus,不经过 Pandora middleware,避免 trace/log 污染监控」
    —— 这里同样直接挂 route,不套业务中间件。
    """
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
