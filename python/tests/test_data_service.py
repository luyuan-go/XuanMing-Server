"""data_service 测试 —— cache-aside 编排 + update_mask 不变量 + 日志限流窗口。

重点覆盖:
  1. ★ update_mask 更新时必须非空(§9.17 零停机滚动升级的硬约束)
  2. ★ 缓存是**旁路**:缓存挂了读写都必须照常成功
  3. ★ MySQL 是**事实源**:它挂了必须报错,不能拿缓存假装成功
  4. ★ 降级日志限流:首错必打、窗口内一条、恢复时一条
  5. proto → SQL schema 推导(替代 proto2mysql)
"""

from __future__ import annotations

import datetime as _dt

import pytest
from pandora.data_service.v1 import data_service_pb2

from pandorapy import errcode, logwindow, protosql
from pandorapy.services.data_service import biz as dbiz
from pandorapy.services.data_service import data as ddata


class FakeStore:
    def __init__(self) -> None:
        self.rows: dict[int, object] = {}
        self.fail_read = False
        self.fail_write = False
        self.write_calls: list[tuple[int, list[str]]] = []

    async def read(self, player_id: int):
        if self.fail_read:
            raise errcode.PandoraError(errcode.ErrInternal, "mysql down")
        return self.rows.get(player_id)

    async def write(self, pd, update_fields: list[str]) -> int:
        if self.fail_write:
            raise errcode.PandoraError(errcode.ErrInternal, "mysql down")
        self.write_calls.append((pd.player_id, list(update_fields)))
        if pd.version == 0:
            if pd.player_id in self.rows:
                raise errcode.PandoraError(errcode.ErrDataVersionMismatch, "exists")
            stored = data_service_pb2.PlayerData()
            stored.CopyFrom(pd)
            stored.version = 1
            self.rows[pd.player_id] = stored
            return 1
        current = self.rows.get(pd.player_id)
        if current is None or current.version != pd.version:
            raise errcode.PandoraError(errcode.ErrDataVersionMismatch, "mismatch")
        for f in update_fields:
            setattr(current, f, getattr(pd, f))
        current.version += 1
        return current.version


class FakeCache:
    def __init__(self) -> None:
        self.store: dict[int, object] = {}
        self.fail = False
        self.deleted: list[int] = []

    async def get(self, player_id: int):
        if self.fail:
            raise RuntimeError("redis down")
        pd = self.store.get(player_id)
        return (pd, True) if pd is not None else (None, False)

    async def set(self, pd, ttl) -> None:  # noqa: ANN001
        if self.fail:
            raise RuntimeError("redis down")
        self.store[pd.player_id] = pd

    async def delete(self, player_id: int) -> None:
        if self.fail:
            raise RuntimeError("redis down")
        self.deleted.append(player_id)
        self.store.pop(player_id, None)


class Cfg:
    def cache_ttl_td(self) -> _dt.timedelta:
        return _dt.timedelta(minutes=5)


def _pd(player_id: int, version: int = 0, **fields) -> object:
    return data_service_pb2.PlayerData(player_id=player_id, version=version, **fields)


# ── ★ update_mask 不变量(§9.17)────────────────────────────────────────────


async def test_update_with_empty_mask_is_rejected() -> None:
    """★ 更新时 update_mask 必须非空。

    空掩码 = 全量覆盖。滚动升级期间旧副本不认得新加的列,一次全量写会把新列**清零**。
    这是零停机更新的硬约束,不是风格问题。
    """
    store, cache = FakeStore(), FakeCache()
    uc = dbiz.DataUsecase(store, cache, Cfg())
    await uc.write_player(_pd(1001, 0, nickname="a"), [])  # 新建可以空掩码
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.write_player(_pd(1001, 1, nickname="b"), [])
    assert exc.value.code == errcode.ErrInvalidArg
    assert "update_mask required" in exc.value.msg


@pytest.mark.parametrize("bad_field", ["player_id", "version", "no_such_column", "1=1"])
async def test_update_mask_rejects_pk_version_and_unknown(bad_field: str) -> None:
    """★ 掩码不能含主键 / version / 未知字段。

    未知字段那条同时是**注入防线** —— 列名会被拼进 SQL。
    """
    store, cache = FakeStore(), FakeCache()
    uc = dbiz.DataUsecase(store, cache, Cfg())
    await uc.write_player(_pd(1001, 0, nickname="a"), [])
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.write_player(_pd(1001, 1, nickname="b"), [bad_field])
    assert exc.value.code == errcode.ErrInvalidArg
    assert "invalid update_mask" in exc.value.msg


async def test_new_record_ignores_mask() -> None:
    """新建(version==0)整条 INSERT,掩码被忽略。"""
    store, cache = FakeStore(), FakeCache()
    uc = dbiz.DataUsecase(store, cache, Cfg())
    assert await uc.write_player(_pd(1001, 0, nickname="x", level=3), []) == 1


async def test_version_mismatch_surfaces() -> None:
    """乐观锁冲突返回 ErrDataVersionMismatch(良性竞争,调用方重读再试)。"""
    store, cache = FakeStore(), FakeCache()
    uc = dbiz.DataUsecase(store, cache, Cfg())
    await uc.write_player(_pd(1001, 0, nickname="a"), [])
    with pytest.raises(errcode.PandoraError) as exc:
        await uc.write_player(_pd(1001, 99, nickname="b"), ["nickname"])
    assert exc.value.code == errcode.ErrDataVersionMismatch


# ── ★ 缓存是旁路 ─────────────────────────────────────────────────────────────


async def test_read_falls_back_to_mysql_when_cache_down() -> None:
    """★ 缓存挂了读必须照常成功(回落 MySQL)。"""
    store, cache = FakeStore(), FakeCache()
    store.rows[1001] = _pd(1001, 1, nickname="alice")
    cache.fail = True
    uc = dbiz.DataUsecase(store, cache, Cfg())
    pd = await uc.read_player(1001)
    assert pd is not None and pd.nickname == "alice"


async def test_write_succeeds_when_cache_del_fails() -> None:
    """★ 写后删缓存失败只告警,**不回滚** —— 缓存最终随 TTL 失效。"""
    store, cache = FakeStore(), FakeCache()
    uc = dbiz.DataUsecase(store, cache, Cfg())
    await uc.write_player(_pd(1001, 0, nickname="a"), [])
    cache.fail = True
    assert await uc.write_player(_pd(1001, 1, nickname="b"), ["nickname"]) == 2


async def test_cache_hit_skips_mysql() -> None:
    """命中缓存直返,不读库。"""
    store, cache = FakeStore(), FakeCache()
    cache.store[1001] = _pd(1001, 1, nickname="cached")
    store.fail_read = True  # 库挂了也不影响,因为不该读它
    uc = dbiz.DataUsecase(store, cache, Cfg())
    pd = await uc.read_player(1001)
    assert pd.nickname == "cached"


async def test_read_backfills_cache_on_miss() -> None:
    store, cache = FakeStore(), FakeCache()
    store.rows[1001] = _pd(1001, 1, nickname="alice")
    uc = dbiz.DataUsecase(store, cache, Cfg())
    await uc.read_player(1001)
    assert 1001 in cache.store


async def test_write_deletes_cache() -> None:
    """写后删缓存,避免读到旧版本。"""
    store, cache = FakeStore(), FakeCache()
    uc = dbiz.DataUsecase(store, cache, Cfg())
    await uc.write_player(_pd(1001, 0, nickname="a"), [])
    assert 1001 in cache.deleted


async def test_no_cache_configured_works() -> None:
    """cache 为 None(未配置)时退化为直连 MySQL。"""
    store = FakeStore()
    uc = dbiz.DataUsecase(store, None, Cfg())
    await uc.write_player(_pd(1001, 0, nickname="a"), [])
    assert (await uc.read_player(1001)).nickname == "a"


# ── ★ MySQL 是事实源 ────────────────────────────────────────────────────────


async def test_mysql_read_failure_raises_not_silent() -> None:
    """★ 库读失败必须抛出,不能静默返回 None。

    返回 None 会被 service 层转成 ErrNotFound —— 调用方会以为"这个玩家不存在",
    可能据此创建重复数据。§16 禁止静默吞错。
    """
    store, cache = FakeStore(), FakeCache()
    store.fail_read = True
    uc = dbiz.DataUsecase(store, cache, Cfg())
    with pytest.raises(errcode.PandoraError):
        await uc.read_player(1001)


async def test_read_zero_player_id_returns_none() -> None:
    """player_id=0 直接返回 None(不查库)。"""
    uc = dbiz.DataUsecase(FakeStore(), FakeCache(), Cfg())
    assert await uc.read_player(0) is None


async def test_write_zero_player_id_rejected() -> None:
    uc = dbiz.DataUsecase(FakeStore(), FakeCache(), Cfg())
    with pytest.raises(errcode.PandoraError, match="player_id required"):
        await uc.write_player(_pd(0, 0), [])


# ── ★ 降级日志限流窗口 ───────────────────────────────────────────────────────


def test_window_first_error_always_logs() -> None:
    """首错必打 —— 降级开始的时刻必须精确。"""
    w = logwindow.Window()
    should, streak = w.admit(1000, 5000)
    assert should and streak == 1


def test_window_suppresses_within_window() -> None:
    """窗口内只打一条,但累计数继续增长。"""
    w = logwindow.Window()
    w.admit(1000, 5000)
    for i in range(1, 10):
        should, streak = w.admit(1000 + i, 5000)
        assert not should
        assert streak == i + 1


def test_window_logs_again_after_window() -> None:
    w = logwindow.Window()
    w.admit(1000, 5000)
    w.admit(2000, 5000)
    should, streak = w.admit(6000, 5000)
    assert should and streak == 3


def test_window_recovered_reports_and_resets() -> None:
    """恢复时返回累计失败数并归零 —— 用于给降级区间画右边界。"""
    w = logwindow.Window()
    for i in range(5):
        w.admit(1000 + i, 5000)
    failed, _extra = w.recovered()
    assert failed == 5
    assert w.recovered() == (0, 0)  # 再次调用不重复报告


def test_window_zero_interval_logs_every_time() -> None:
    """window_ms <= 0 退化为不限流。"""
    w = logwindow.Window()
    for _ in range(5):
        should, _ = w.admit(1000, 0)
        assert should


# ── proto → SQL schema(替代 proto2mysql)────────────────────────────────────


def test_schema_derived_from_proto_descriptor() -> None:
    """表结构从 proto 描述符推导,不手写 SQL。"""
    s = ddata.PLAYER_DATA_SCHEMA
    assert s.table_name == "player_data"
    assert s.primary_key == ("player_id",)
    assert "player_id" in s.column_names()
    assert "version" in s.column_names()


def test_unsigned_semantics_carried_into_column_types() -> None:
    """★ uint64/uint32 必须建成 UNSIGNED 列。

    建成有符号列时,超过 2^31 的 player_id 会溢出 —— 严格模式下报错、
    非严格模式下**静默截断**,两种都不可接受(§5.12 + §9.24)。
    """
    sql = ddata.PLAYER_DATA_SCHEMA.create_table_sql()
    assert "`player_id` BIGINT UNSIGNED" in sql
    assert "`version` INT UNSIGNED" in sql


def test_updatable_fields_exclude_pk_and_version() -> None:
    """可更新列 = 全部列 - 主键 - version,**动态推导**。

    手工维护列表漏一个字段,那个字段就永远写不进 MySQL 且不报错。
    """
    fields = set(ddata.UPDATABLE_FIELDS)
    assert "player_id" not in fields
    assert "version" not in fields
    assert "nickname" in fields
    # 与描述符里的字段总数对得上
    all_fields = {f.name for f in data_service_pb2.PlayerData.DESCRIPTOR.fields}
    assert fields == all_fields - {"player_id", "version"}


def test_is_updatable_field_guards_injection() -> None:
    """未知列名必须被拒 —— 它会被拼进 SQL。"""
    assert ddata.is_updatable_field("nickname")
    assert not ddata.is_updatable_field("player_id")
    assert not ddata.is_updatable_field("version")
    assert not ddata.is_updatable_field("nickname`; DROP TABLE x; --")


def test_schema_rejects_repeated_and_message_fields() -> None:
    """repeated / 嵌套 message 无法映射成标量列 → 建表期 fail-fast。"""
    from pandora.trade.v1 import trade_pb2

    with pytest.raises(protosql.SchemaError):
        protosql.schema_of(trade_pb2.Order, table_name="t", primary_key="order_id")


def test_cache_key_matches_go_format() -> None:
    """缓存 key 与 Go 侧一致。"""
    assert ddata.cache_key(1001) == "pandora:data:player:1001"
