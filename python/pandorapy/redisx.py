"""Redis 封装 —— 对应 Go 侧 pkg/redisx + pkg/redislock。

全仓有 **190 处** Redis 原子操作(Lua / 事务),它们承载的是限额校验、名额预留、
会话 fencing 这类"错了就是数据损坏"的逻辑。迁移策略:

    **Lua 脚本原样搬,一个字都不改。**

    Lua 在 Redis 服务端执行,与调用方语言无关。把已经在生产跑过的脚本原样搬过来,
    等于把这 190 处的正确性风险降到接近零 —— 需要重新验证的只剩"参数传对没有"。
    反过来,如果借迁移之机"顺手用 Python 重写成几条命令",就等于把 190 个原子操作
    重新实现一遍,每一个都是新的竞态入口。

redis-py 的 async 客户端(`redis.asyncio`)与 grpc.aio 同一个 event loop,
不会像同步客户端那样阻塞整个循环。
"""

from __future__ import annotations

import contextlib
from typing import Any

import redis.asyncio as aioredis
from redis.asyncio.client import Redis
from redis.commands.core import AsyncScript

from pandorapy import log as plog

# 与 Go 侧 pkg/redislock 一致:锁 TTL 上限 30s(不变量 §9.10),业务跑完主动释放。
MAX_LOCK_TTL_SEC = 30


class LockTTLTooLongError(ValueError):
    """锁 TTL 超过 30s。不变量 §9.10 的机械闸。"""


def new_client(
    addr: str,
    *,
    db: int = 0,
    password: str = "",
    dial_timeout_sec: float = 2.0,
) -> Redis:
    """建一个 async Redis 客户端。参数名对齐 yaml 的 node.redis_client 段。

    decode_responses=False:全仓大量存 protobuf bytes,自动解码会炸。
    与 Go 侧 go-redis 的默认行为(返回 []byte)一致。
    """
    host, _, port = addr.rpartition(":")
    return aioredis.Redis(
        host=host or "127.0.0.1",
        port=int(port or 6379),
        db=db,
        password=password or None,
        socket_connect_timeout=dial_timeout_sec,
        socket_timeout=dial_timeout_sec,
        decode_responses=False,
        # health_check_interval:连接空闲后被中间设备静默断开时,下次使用前先 PING。
        # 不设的话会在长空闲后收到一次莫名其妙的 ConnectionError。
        health_check_interval=30,
    )


class LuaScript:
    """一段 Lua 脚本 —— 直接承载从 Go 侧原样搬来的脚本文本。

    用法:

        CLAIM_SLOT = LuaScript(name="claim_slot", body='''
            local n = redis.call('SCARD', KEYS[1])
            if n >= tonumber(ARGV[1]) then return 0 end
            redis.call('SADD', KEYS[1], ARGV[2])
            return 1
        ''')
        ok = await CLAIM_SLOT(client, keys=[key], args=[limit, member])

    为什么包一层而不是直接用 redis-py 的 register_script:
      - 强制给脚本起名字,失败日志里能看出是哪段脚本(190 段脚本靠 sha 排查是灾难)
      - 统一 NOSCRIPT 后的重新加载(集群故障切换后脚本缓存会丢)
    """

    __slots__ = ("name", "body")

    def __init__(self, name: str, body: str) -> None:
        self.name = name
        self.body = body

    async def __call__(
        self, client: Redis, keys: list[Any] | None = None, args: list[Any] | None = None
    ) -> Any:
        """执行脚本。

        ⚠️ **不缓存 Script 对象**(2026-08-18 实测踩到的缺陷)。
        最初写成 `if self._script is None: self._script = client.register_script(...)`,
        于是模块级的 LuaScript 实例把 Script 连同**第一个传进来的 client** 缓存了下来;
        之后换了 client 再调用,脚本仍然打到旧 client 上。

        生产里通常只有一个 client,所以这个 bug 会被掩盖 —— 直到:
          - 连接池被替换 / 故障切换后重建 client
          - 或者像测试里那样每个用例一个独立 client(4 个用例因此变红,
            而且单独跑全过、全量跑才挂,是最难查的形态)

        `client.register_script` 本身只是包一层并本地算 sha1(很便宜),
        真正的 EVALSHA→EVAL 回退由 redis-py 在连接池层处理,所以每次新建没有性能问题。
        """
        script: AsyncScript = client.register_script(self.body)
        try:
            return await script(keys=keys or [], args=args or [])
        except Exception as exc:  # noqa: BLE001
            # NOSCRIPT:节点重启 / 故障切换后脚本缓存丢失。redis-py 的 Script 会自动用
            # EVAL 兜底,这里只记一笔 —— 频繁出现说明 Redis 在反复重启。
            if "NOSCRIPT" in str(exc):
                plog.get().warning("redis_script_cache_miss", script=self.name)
                return await script(keys=keys or [], args=args or [])
            plog.get().warning(
                "redis_script_failed", script=self.name, err=str(exc), exc_type=type(exc).__name__
            )
            raise


# ── 分布式锁 ─────────────────────────────────────────────────────────────────

# 释放锁必须校验持有者 —— 否则会释放掉**别人**的锁:
#   A 拿锁 → A 卡住超过 TTL → 锁自动过期 → B 拿到锁 → A 恢复并 DEL → B 的锁没了
# 这是 Redis 分布式锁最经典的错误。用 Lua 保证"比对 + 删除"原子。
_UNLOCK_SCRIPT = LuaScript(
    name="unlock_if_owner",
    body="""
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
""",
)


@contextlib.asynccontextmanager
async def lock(client: Redis, key: str, token: str, ttl_sec: int):
    """分布式锁。对应 Go 侧 pkg/redislock。

    ttl_sec 超过 30s 直接拒绝(不变量 §9.10)。token 必须是本次持有的唯一标识
    (调用方通常用 snowflake 或 uuid),释放时用它校验持有者。

    ⚠️ 这把锁只降低冲突概率,**不能**作为最终正确性的唯一保证 —— §16.1 要求
    共享写的正确性由数据库条件更新 / 唯一键 / CAS / Lua 保证。锁过期、进程暂停、
    网络分区都会让"我以为我还持有"变成假的。
    """
    if ttl_sec > MAX_LOCK_TTL_SEC:
        raise LockTTLTooLongError(
            f"redislock: TTL {ttl_sec}s 超过上限 {MAX_LOCK_TTL_SEC}s(不变量 §9.10);"
            f"长任务应当分段并在段间续租,而不是把锁 TTL 拉长"
        )
    acquired = await client.set(key, token, nx=True, ex=ttl_sec)
    if not acquired:
        raise TimeoutError(f"redislock: 获取锁失败 key={key}")
    try:
        yield
    finally:
        with contextlib.suppress(Exception):
            await _UNLOCK_SCRIPT(client, keys=[key], args=[token])


async def ping(client: Redis) -> bool:
    """探活。启动期强依赖检查用,失败必须 fail-fast 而不是降级 ——
    Redis 不通时 hub_allocator 拉不起大厅 Hub DS,玩家会卡在连大厅。"""
    try:
        return bool(await client.ping())
    except Exception:  # noqa: BLE001
        return False
