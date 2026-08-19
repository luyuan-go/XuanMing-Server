"""data_service 数据层 —— 对应 Go 侧 internal/data(store.go + cache.go)。

MySQL 是**事实源**,Redis 只是旁路缓存(弱一致)。

schema 唯一来源是 PlayerData proto:表名 / 主键写在 proto option 里,每个标量字段即一列。
Go 侧靠 `proto2mysql` 库(你们自己的库)做这件事;Python 侧用 pandorapy/protosql
从描述符推导 —— 见那个模块的头注释解释为什么不照抄整个库。

乐观锁语义(与 Go 逐条对齐):
    version == 0 → 新建,INSERT 起始版本 1(冲突即已存在 → ErrDataVersionMismatch)
    version  > 0 → UPDATE ... WHERE player_id=? AND version=?
                   受影响行 0(版本不匹配 / 不存在)→ ErrDataVersionMismatch

★ update_mask 为什么在更新时**必须非空**(不变量 §9.17):
    空掩码 = 全量覆盖。滚动升级期间旧副本不认得新加的列,一次全量写会把新列**清零**。
    这是零停机更新的硬约束,不是风格问题。所以空掩码更新直接拒绝。
"""

from __future__ import annotations

import datetime as _dt
from typing import Protocol

from pandora.data_service.v1 import data_service_pb2
from redis.asyncio.client import Redis

from pandorapy import errcode, mysqlx, protosql

# 表结构从 proto 描述符推导。显式传表名/主键(显式 > 隐式),与 proto option 一致。
PLAYER_DATA_SCHEMA = protosql.schema_of(
    data_service_pb2.PlayerData, table_name="player_data", primary_key="player_id"
)

PK_FIELD = "player_id"
VERSION_FIELD = "version"

# 可经 update_mask 更新的业务列 —— **从描述符动态推导**,新增 proto 字段自动纳入。
# 手工维护列表漏一个字段,那个字段就永远写不进 MySQL,而且不报错(Go 侧注释点名了这点)。
UPDATABLE_FIELDS: tuple[str, ...] = tuple(PLAYER_DATA_SCHEMA.updatable_fields(VERSION_FIELD))
_UPDATABLE_SET = frozenset(UPDATABLE_FIELDS)


def is_updatable_field(name: str) -> bool:
    """判断字段名是否是可经 update_mask 更新的业务列(非主键、非 version)。"""
    return name in _UPDATABLE_SET


def cache_key(player_id: int) -> str:
    """与 Go 侧一致:pandora:data:player:<id>。"""
    return f"pandora:data:player:{player_id}"


class PlayerStore(Protocol):
    async def read(self, player_id: int): ...
    async def write(self, pd, update_fields: list[str]) -> int: ...


class PlayerCache(Protocol):
    async def get(self, player_id: int) -> tuple[object | None, bool]: ...
    async def set(self, pd, ttl: _dt.timedelta) -> None: ...
    async def delete(self, player_id: int) -> None: ...


class MySQLPlayerStore:
    """基于 asyncmy / aiomysql 的 PlayerStore。"""

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001
        self._pool = pool

    async def ensure_schema(self) -> None:
        """按 pb 建表。用户已确认「没上线、库可清空」,故只建不做增量同步(§15.3)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(PLAYER_DATA_SCHEMA.create_table_sql())
            await conn.commit()

    async def read(self, player_id: int):
        """读玩家数据。不存在返回 None(对应 Go 的 (nil, false, nil))。"""
        cols = ", ".join(f"`{c}`" for c in PLAYER_DATA_SCHEMA.column_names())
        sql = f"SELECT {cols} FROM `{PLAYER_DATA_SCHEMA.table_name}` WHERE `{PK_FIELD}` = %s"  # noqa: S608
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(sql, (player_id,))
                row = await cur.fetchone()
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "read player_data %d: %s", player_id, exc
            ) from exc
        if row is None:
            return None
        pd = data_service_pb2.PlayerData()
        for name, value in zip(PLAYER_DATA_SCHEMA.column_names(), row, strict=True):
            if value is not None:
                setattr(pd, name, value)
        return pd

    async def write(self, pd, update_fields: list[str]) -> int:
        """乐观锁写。返回写入后的新版本号(= 期望版本 + 1)。**不修改入参 pd**。"""
        expect = pd.version
        if expect == 0:
            return await self._insert(pd)
        return await self._update(pd, update_fields)

    async def _insert(self, pd) -> int:
        """新建:整条 INSERT,起始版本 1。主键冲突 → ErrDataVersionMismatch(已存在)。"""
        names = PLAYER_DATA_SCHEMA.column_names()
        placeholders = ", ".join(["%s"] * len(names))
        cols = ", ".join(f"`{n}`" for n in names)
        sql = f"INSERT INTO `{PLAYER_DATA_SCHEMA.table_name}` ({cols}) VALUES ({placeholders})"  # noqa: S608
        values = [1 if n == VERSION_FIELD else getattr(pd, n) for n in names]
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(sql, values)
                await conn.commit()
        except Exception as exc:  # noqa: BLE001
            if mysqlx.is_duplicate_entry(exc):
                # 已存在 —— 对调用方就是"版本不匹配"(它以为是新建,实际不是)。
                raise errcode.PandoraError(
                    errcode.ErrDataVersionMismatch,
                    "player_data %d already exists",
                    pd.player_id,
                ) from exc
            raise errcode.PandoraError(
                errcode.ErrInternal, "insert player_data %d: %s", pd.player_id, exc
            ) from exc
        return 1

    async def _update(self, pd, update_fields: list[str]) -> int:
        """CAS 更新:只 SET 掩码内的列,version 单独 +1。

        调用方(biz)须已校验 update_fields 合法(非空、不含主键/version/未知字段)。
        这里再挡一道 —— 数据层不信任调用方是防御性编程的正当用法,
        因为拼进 SQL 的列名如果没校验就是注入面。
        """
        safe = [f for f in update_fields if is_updatable_field(f)]
        if not safe or len(safe) != len(update_fields):
            raise errcode.PandoraError(
                errcode.ErrInvalidArg,
                "invalid update_mask for player_data %d",
                pd.player_id,
            )
        assignments = ", ".join(f"`{f}` = %s" for f in safe)
        sql = (
            f"UPDATE `{PLAYER_DATA_SCHEMA.table_name}` "  # noqa: S608
            f"SET {assignments}, `{VERSION_FIELD}` = `{VERSION_FIELD}` + 1 "
            f"WHERE `{PK_FIELD}` = %s AND `{VERSION_FIELD}` = %s"
        )
        values = [getattr(pd, f) for f in safe] + [pd.player_id, pd.version]
        try:
            async with self._pool.acquire() as conn, conn.cursor() as cur:
                await cur.execute(sql, values)
                affected = cur.rowcount
                await conn.commit()
        except Exception as exc:  # noqa: BLE001
            raise errcode.PandoraError(
                errcode.ErrInternal, "update player_data %d: %s", pd.player_id, exc
            ) from exc
        if not affected:
            # 版本不匹配或行不存在 —— 两者对调用方是同一种处置(重读再试)。
            raise errcode.PandoraError(
                errcode.ErrDataVersionMismatch,
                "player_data %d version mismatch (expect %d)",
                pd.player_id,
                pd.version,
            )
        return pd.version + 1


class RedisPlayerCache:
    """Redis 旁路缓存,存 protobuf bytes。对应 Go 侧 cache.go。

    存 pb bytes 而不是 JSON:与 Go 侧同一份字节格式,迁移期两个实现可以读到对方写的缓存
    (虽然用户已说库可清空,但缓存格式一致是零成本的,没理由不做)。
    """

    __slots__ = ("_rdb",)

    def __init__(self, rdb: Redis) -> None:
        self._rdb = rdb

    async def get(self, player_id: int) -> tuple[object | None, bool]:
        """返回 (数据, 是否命中)。反序列化失败视为 miss —— 旧结构缓存不应让读整个失败。"""
        raw = await self._rdb.get(cache_key(player_id))
        if raw is None:
            return None, False
        pd = data_service_pb2.PlayerData()
        try:
            pd.ParseFromString(raw)
        except Exception:  # noqa: BLE001
            # 缓存里是旧 pb 结构(如切换 schema 后的残留)→ 当 miss 处理,
            # 让它回落 MySQL 并被新结构覆盖。缓存是旁路,不能因为它让读失败。
            return None, False
        return pd, True

    async def set(self, pd, ttl: _dt.timedelta) -> None:
        ms = int(ttl.total_seconds() * 1000)
        await self._rdb.set(cache_key(pd.player_id), pd.SerializeToString(), px=max(ms, 1))

    async def delete(self, player_id: int) -> None:
        await self._rdb.delete(cache_key(player_id))
