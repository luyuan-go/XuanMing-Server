"""grpc.aio 服务端拦截器 —— 对应 Go 侧 pkg/middleware 的 Kratos middleware 链。

这是"Kratos 那一层"在 Python 侧的替代物之一。Kratos 提供的是一套 middleware 约定 +
现成件;grpcio 只给 ServerInterceptor 原语,所以约定要自己定。本文件就是那份约定。

对齐的 Go 侧行为:
    pmw.AuthOptional()  —— 从 Envoy jwt_authn 注入的 x-pandora-player-id 头读 player_id,
                           有就注入 ctx,没有就放过(登录前 RPC 也要能调)。
    pmw.AuthRequired()  —— 同上但缺 player_id 直接 401。
    plog.With(ctx)      —— 让本请求后续所有日志自动带 player_id / trace_id。
    safego.Run          —— panic 兜底:单个请求崩不能把进程带走。

身份来源的安全前提(与 Go 侧完全一致,不可放宽):
    x-pandora-player-id 是 **Envoy 验签 JWT 之后重写**的头,入站时被无条件剥离。
    因此在客户端面(:8443)它是可信的。直连内网端口联调时没有网关注入 → player_id=0,
    按匿名处理,由业务层 fail-closed。
    ⚠️ 绝不能改成"请求体里带 player_id 就信" —— R5 已经把 proto 里的 player_id 字段
       整批 reserved 掉了,正是为了消灭伪造他人身份的路径。
"""

from __future__ import annotations

import time
from collections.abc import Awaitable, Callable
from typing import Any

import grpc

from pandorapy import log as plog
from pandorapy import metrics

# Envoy jwt_authn 验签后注入的玩家身份头(全后端统一用这个头当玩家身份)。
METADATA_KEY_PLAYER_ID = "x-pandora-player-id"
# Envoy 验签成功后重写的 JWT payload 头(会话现行性门用,dialogue 暂不需要)。
METADATA_KEY_JWT_PAYLOAD = "x-pandora-jwt-payload"
# 链路追踪头,没有就不打(与 Go 侧一致,不自造 trace_id 免得和上游对不上)。
METADATA_KEY_TRACE_ID = "x-request-id"


def _metadata_get(metadata: Any, key: str) -> str:
    """从 invocation_metadata 取一个值。grpcio 给的是 (key, value) 元组序列。"""
    if not metadata:
        return ""
    for entry in metadata:
        # grpcio 的 metadata key 已经是小写(HTTP/2 规范要求),这里仍做一次归一化
        # 以防被 Envoy 或测试夹具改过大小写。
        if entry[0].lower() == key:
            value = entry[1]
            return value.decode() if isinstance(value, bytes) else str(value)
    return ""


def extract_player_id(context: grpc.aio.ServicerContext) -> int:
    """从 metadata 提取 player_id。取不到 / 非法 → 0(匿名)。

    对应 Go 的 pkg/middleware.extractPlayerID。非法值按 0 处理而不是报错:
    Envoy 注入的头永远合法,能走到"非法"分支说明是直连联调,按匿名更合理。
    """
    raw = _metadata_get(context.invocation_metadata(), METADATA_KEY_PLAYER_ID)
    if not raw:
        return 0
    try:
        value = int(raw)
    except ValueError:
        return 0
    return value if value > 0 else 0


def _split_method(full_method: str) -> tuple[str, str]:
    """把 /pandora.dialogue.v1.DialogueService/StartDialogue 拆成 (service, method)。

    用于指标 label。拆不开时整串当 method,免得指标里出现空 label。
    """
    trimmed = full_method.lstrip("/")
    if "/" in trimmed:
        service, _, method = trimmed.partition("/")
        return service, method
    return "", trimmed


class AuthInterceptor(grpc.aio.ServerInterceptor):
    """把 Envoy 注入的 player_id 绑到日志上下文。

    required=False 对应 Go 的 AuthOptional(dialogue 用的就是这个);
    required=True 对应 AuthRequired,缺 player_id 直接 UNAUTHENTICATED。

    为什么 dialogue 用 Optional 而 service 层还要再查一次 callerID==0:
    Envoy jwt_authn 已在路由层 require JWT,拦截器不重复拒绝;但直连内网端口联调
    没有网关,业务层那道 `player_id == 0 → ERR_UNAUTHORIZED` 是兜底。两层都要在。
    """

    def __init__(self, *, required: bool = False) -> None:
        self._required = required

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        handler = await continuation(handler_call_details)
        if handler is None:
            return handler
        # 只包 unary-unary:本仓库 208 个 RPC 里 207 个是 unary,唯一的流是
        # push.Subscribe。流式路径的身份提取走的是另一套(Go 侧注释也说明了
        # stream 不跑 unary middleware 链),等迁 push 时单独接,不在这里凑。
        if handler.request_streaming or handler.response_streaming:
            return handler

        inner = handler.unary_unary
        required = self._required

        async def wrapper(request: Any, context: grpc.aio.ServicerContext) -> Any:
            player_id = extract_player_id(context)
            trace_id = _metadata_get(context.invocation_metadata(), METADATA_KEY_TRACE_ID)

            tokens = []
            if player_id:
                tokens.append(plog.bind_player_id(player_id))
            if trace_id:
                tokens.append(plog.bind_trace_id(trace_id))
            try:
                if required and player_id == 0:
                    await context.abort(
                        grpc.StatusCode.UNAUTHENTICATED, "missing or invalid player_id"
                    )
                return await inner(request, context)
            finally:
                # contextvars 的 Token 必须逆序 reset,否则嵌套调用会串上下文。
                for token in reversed(tokens):
                    token.var.reset(token)

        return grpc.unary_unary_rpc_method_handler(
            wrapper,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )


class ObservabilityInterceptor(grpc.aio.ServerInterceptor):
    """指标 + panic 兜底 —— 对应 Go 侧的 recovery/metrics middleware + safego。

    panic 兜底为什么必须有:
        Go 里一个 handler panic 会被 Kratos recovery middleware 拦住,只毁这一个请求。
        Python 里未捕获异常会被 grpcio 转成 UNKNOWN 返回给客户端,**并且**堆栈只在
        grpcio 内部打印 —— 不走我们的 structlog,于是 Loki 里什么都看不到。
        这正是迁 Python 最大的新风险(类型错误从编译期挪到运行期),必须在这里收口:
        统一打成结构化日志 + 计数,才能在 Grafana/Sentry 里看见。
    """

    async def intercept_service(
        self,
        continuation: Callable[[grpc.HandlerCallDetails], Awaitable[grpc.RpcMethodHandler]],
        handler_call_details: grpc.HandlerCallDetails,
    ) -> grpc.RpcMethodHandler:
        handler = await continuation(handler_call_details)
        if handler is None or handler.request_streaming or handler.response_streaming:
            return handler

        service, method = _split_method(handler_call_details.method)
        inner = handler.unary_unary

        async def wrapper(request: Any, context: grpc.aio.ServicerContext) -> Any:
            metrics.RPC_STARTED.labels(service, method).inc()
            started = time.perf_counter()
            try:
                response = await inner(request, context)
            except grpc.aio.AbortError:
                # context.abort 的正常控制流(如 AuthRequired 的 401),不是故障。
                raise
            except BaseException as exc:  # noqa: BLE001 —— 兜底就是要抓全部
                metrics.RPC_PANIC.labels(service, method, type(exc).__name__).inc()
                plog.get().exception(
                    "rpc_handler_unhandled_exception",
                    grpc_service=service,
                    grpc_method=method,
                    exc_type=type(exc).__name__,
                )
                raise
            finally:
                metrics.RPC_LATENCY.labels(service, method).observe(
                    time.perf_counter() - started
                )
            # 业务错误码从 response.code 取(本仓库的失败语义在 body 里,不在 gRPC status)。
            code = getattr(response, "code", 0)
            metrics.RPC_HANDLED.labels(service, method, str(int(code))).inc()
            return response

        return grpc.unary_unary_rpc_method_handler(
            wrapper,
            request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer,
        )
