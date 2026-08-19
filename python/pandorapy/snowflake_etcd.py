"""snowflake nodeID 的 etcd 抢占 —— 对应 Go 侧 pkg/snowflake/etcdnode。

与 etcdleader 的**失主语义完全相反**,这是本模块最容易写错的地方:

    etcdleader 失去领导权  → 只停 leader 任务,**进程继续服务 RPC**
    本模块 nodeID 失租     → **必须停止发号并退出进程**

    理由:etcd Lease 是 nodeID 独占权的**事实来源**。lease 一丢,另一个副本就可能
    抢到同一个 nodeID 并开始发号。两个副本用同一 nodeID 发号 = 重号(违反不变量 §9.11),
    而重号的 ID 会进库、进客户端、进日志,不可回收。
    所以这里宁可让 k8s 重新拉起(重新抢一个号),也不能继续发号。

    KeepAlive **不是健康检查,是独占权信号** —— 这句话是 Go 侧注释的原话,照搬。

⚠️ aetcd 没有自动 KeepAlive,续约循环由本模块驱动(同 etcdleader)。
   本地安全截止线比 lease 到期提前 TTL/3:越过就认为独占权已不可证明,立刻触发退出。
"""

from __future__ import annotations

import asyncio
import contextlib
import time

import aetcd

from pandorapy import log as plog
from pandorapy import snowflake

# 与 Go 侧 etcdnode 常量对齐。
DEFAULT_PREFIX = "/pandora/snowflake/node/"
DEFAULT_LEASE_TTL_SEC = 15  # docs/design/infra.md §8.1

_KEEPALIVE_DIVISOR = 3
_SAFETY_MARGIN_DIVISOR = 3


class NodeIDExhaustedError(RuntimeError):
    """[0, MaxNodeID) 全被占用。多副本数超过 131072 才可能发生,实际是配置错。"""


class NodeIDLostError(RuntimeError):
    """nodeID 独占权丢失。调用方**必须**停止发号并退出进程。"""


class Holder:
    """持有一个抢占成功的 nodeID 及其 etcd lease。

    用法(对应 Go 侧 MustProvideSnowflake 的 etcd 档):

        holder = await acquire(endpoints, service_name="dialogue")
        node = snowflake.Node(holder.node_id)

        async def on_lost() -> None:
            logger.error("snowflake_nodeid_lease_lost",
                         hint="停止发号并退出,交给 k8s 重新拉起重新抢号")
            os._exit(1)      # ← 必须退出,不能降级继续发号

        holder.start_keepalive(on_lost)
    """

    __slots__ = (
        "node_id",
        "_client",
        "_lease",
        "_key",
        "_ttl",
        "_task",
        "_lost",
    )

    def __init__(
        self,
        node_id: int,
        client: aetcd.Client,
        lease: aetcd.Lease,
        key: bytes,
        ttl_sec: int,
    ) -> None:
        self.node_id = node_id
        self._client = client
        self._lease = lease
        self._key = key
        self._ttl = ttl_sec
        self._task: asyncio.Task | None = None
        self._lost = asyncio.Event()

    @property
    def lost(self) -> asyncio.Event:
        """独占权丢失事件。调用方可以 await 它然后退出进程。"""
        return self._lost

    def start_keepalive(self, on_lost=None) -> None:  # noqa: ANN001
        """启动续约循环。丢失独占权时置位 lost 并(若提供)调用 on_lost。"""
        if self._task is not None:
            return
        self._task = asyncio.create_task(
            self._keepalive_loop(on_lost), name=f"snowflake-node-{self.node_id}"
        )

    async def _keepalive_loop(self, on_lost) -> None:  # noqa: ANN001
        interval = self._ttl / _KEEPALIVE_DIVISOR
        margin = self._ttl / _SAFETY_MARGIN_DIVISOR
        safe_deadline = time.monotonic() + self._ttl - margin
        logger = plog.get()

        while True:
            try:
                await asyncio.sleep(min(interval, max(0.05, safe_deadline - time.monotonic())))
                now = time.monotonic()
                await asyncio.wait_for(self._lease.refresh(), timeout=interval)
                # ★ 只有成功响应才推进安全线,且从**发起时刻**算(保守方向)。
                safe_deadline = now + self._ttl - margin
            except asyncio.CancelledError:
                return
            except Exception:  # noqa: BLE001
                if time.monotonic() < safe_deadline:
                    # 还在安全窗口内:etcd 短抖动很常见,继续试。
                    continue
                logger.error(
                    "snowflake_nodeid_lease_lost",
                    node_id=self.node_id,
                    hint=(
                        "nodeID 独占权已不可证明。必须停止发号并退出进程 —— "
                        "另一副本可能已抢到同一 nodeID,继续发号会重号(不变量 §9.11)"
                    ),
                )
                self._lost.set()
                if on_lost is not None:
                    result = on_lost()
                    if asyncio.iscoroutine(result):
                        await result
                return

    async def close(self) -> None:
        """正常退出:释放 lease 并停止续约。"""
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await self._task
        with contextlib.suppress(Exception):
            await self._client.revoke_lease(self._lease.id)
        with contextlib.suppress(Exception):
            await self._client.close()


async def acquire(
    endpoints: list[str],
    service_name: str,
    *,
    prefix: str = DEFAULT_PREFIX,
    lease_ttl_sec: int = DEFAULT_LEASE_TTL_SEC,
    max_node_id: int = 0,
) -> Holder:
    """在 [0, max_node_id) 区间抢占一个独占 nodeID。对应 Go 的 etcdnode.Acquire。

    抢占用 txn 的 version == 0(仅当 key 不存在时写入)——**不能**用"先 get 看有没有,
    没有就 put":那是 TOCTOU,两个副本会同时抢到同一个号。
    """
    if not endpoints:
        raise ValueError("etcdnode: endpoints 不能为空")
    if not service_name:
        raise ValueError("etcdnode: service_name 不能为空")
    limit = max_node_id if max_node_id > 0 else snowflake.NODE_MASK + 1
    ttl = lease_ttl_sec if lease_ttl_sec > 0 else DEFAULT_LEASE_TTL_SEC

    host, _, port = endpoints[0].rpartition(":")
    client = aetcd.Client(host=host or "127.0.0.1", port=int(port or 2379))

    lease = await client.lease(ttl)
    try:
        for node_id in range(limit):
            key = f"{prefix}{service_name}/{node_id}".encode()
            succeeded, _ = await client.transaction(
                compare=[client.transactions.version(key) == 0],
                success=[client.transactions.put(key, str(node_id).encode(), lease=lease.id)],
                failure=[],
            )
            if succeeded:
                plog.get().info(
                    "snowflake_nodeid_acquired",
                    service=service_name,
                    node_id=node_id,
                    lease_id=lease.id,
                    ttl_sec=ttl,
                )
                return Holder(node_id, client, lease, key, ttl)
        raise NodeIDExhaustedError(
            f"etcdnode: service={service_name} 的 [0, {limit}) 全被占用 —— "
            f"副本数不可能这么多,检查是否有残留 key 未随 lease 过期"
        )
    except BaseException:
        with contextlib.suppress(Exception):
            await client.revoke_lease(lease.id)
        with contextlib.suppress(Exception):
            await client.close()
        raise


async def provide_node(
    endpoints: list[str],
    service_name: str,
    static_node_id: int,
    node_id_source: str = "",
    **kwargs: object,
) -> tuple[snowflake.Node, Holder | None]:
    """按 node_id_source 选路 —— 对应 Go 的 MustProvideSnowflake。

    ""/"static" → 用 yaml 的 node.node_id(单副本 / dev)
    "etcd"      → etcd 自动抢占 + 失租退出(多副本)

    两条路径都是完整实现(CLAUDE.md §14.2:开关打开后的分支必须是真实实现,不是空壳)。
    """
    source = (node_id_source or "static").lower()
    if source in ("", "static"):
        return snowflake.Node(static_node_id), None
    if source != "etcd":
        raise ValueError(
            f"etcdnode: 未知的 node_id_source={node_id_source!r}(只支持 static / etcd)"
        )
    holder = await acquire(endpoints, service_name, **kwargs)  # type: ignore[arg-type]
    return snowflake.Node(holder.node_id), holder
