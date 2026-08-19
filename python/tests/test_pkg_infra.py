"""共享基础件测试 —— cellroute / killswitch / dbguard / mysqlx。

重点覆盖"写错了不报错"的不变量:
  - cellroute:region/cell 错配必须在**建表期**就被拒(否则玩家 owner 数据分散两个 region)
  - cellroute:路由失败必须抛异常,不能返回默认落点(§9.22 禁止冒充默认值)
  - killswitch:规则解析失败必须保留旧快照,不能清空(清空 = 维护中的 RPC 突然接流量)
  - killswitch:规则源缺失 fail-open(它是运维工具,不是全服故障开关)
  - dbguard:payload 三档告警
"""

from __future__ import annotations

import pathlib

import pytest

from pandorapy import cellroute, dbguard, killswitch, mysqlx


# ── cellroute ────────────────────────────────────────────────────────────────


def test_logical_cell_count_is_4096() -> None:
    """LogicalCellCount 是永久契约,必须与 Go 侧同值。

    改这个数 = 全体玩家重新分片 = 所有 owner 数据错位。迁移期两个实现同时在线,
    不同值会让同一玩家被路由到两个 cell,而且不报错。
    """
    assert cellroute.LOGICAL_CELL_COUNT == 4096


def test_logical_cell_is_deterministic_modulo() -> None:
    """logical_cell = player_id % 4096,纯函数。"""
    assert cellroute.logical_cell_of(0) == 0
    assert cellroute.logical_cell_of(4096) == 0
    assert cellroute.logical_cell_of(4097) == 1
    assert cellroute.logical_cell_of(25380000000000000) == 25380000000000000 % 4096


def test_router_routes_same_player_to_same_cell() -> None:
    """★ 同一 player_id 必须恒落同一 (region, cell) —— owner 不变量的基础。"""
    entries, region_of_cell = cellroute.build_balanced_entries(
        [cellroute.CellSpec(region_id=1, cell_id=c) for c in range(1, 5)]
        + [cellroute.CellSpec(region_id=2, cell_id=c) for c in range(5, 9)]
    )
    router = cellroute.Router(cellroute.StaticTable(entries, region_of_cell))
    for player_id in (1001, 25380000000000000, 999999999):
        first = router.route(player_id)
        for _ in range(10):
            assert router.route(player_id) == first


def test_static_table_rejects_region_cell_mismatch() -> None:
    """★ region/cell 错配必须在建表期被拒。

    放过一个错配,那批玩家的 owner 数据会分散在两个 region(档案在 A、背包在 B),
    读不回来,且要等玩家实际访问才发现。
    """
    entries = [cellroute.Entry(region_id=1, cell_id=7)] * cellroute.LOGICAL_CELL_COUNT
    # 拓扑里 cell 7 属于 region 2,与 entry 声明的 region 1 冲突
    with pytest.raises(cellroute.CellRouteError, match="region 不匹配"):
        cellroute.StaticTable(entries, {7: 2})


def test_static_table_rejects_unregistered_cell() -> None:
    """entry 引用了拓扑里没登记的 cell → 拒绝建表。"""
    entries = [cellroute.Entry(region_id=1, cell_id=99)] * cellroute.LOGICAL_CELL_COUNT
    with pytest.raises(cellroute.CellRouteError, match="未在 region_of_cell 中登记"):
        cellroute.StaticTable(entries, {1: 1})


def test_static_table_rejects_wrong_length() -> None:
    """表长度必须恰好是 LOGICAL_CELL_COUNT —— 短了会让部分玩家路由不到。"""
    with pytest.raises(cellroute.CellRouteError, match="LogicalCellCount"):
        cellroute.StaticTable([cellroute.Entry(1, 1)], {1: 1})


def test_build_balanced_entries_covers_all_shards_evenly() -> None:
    """铺表必须覆盖全部 4096 个分片,且尽量均匀(余数摊到前几个 Cell)。"""
    cells = [cellroute.CellSpec(region_id=1, cell_id=c) for c in range(1, 8)]  # 7 个
    entries, region_of_cell = cellroute.build_balanced_entries(cells)
    assert len(entries) == cellroute.LOGICAL_CELL_COUNT
    assert len(region_of_cell) == 7
    counts: dict[int, int] = {}
    for e in entries:
        counts[e.cell_id] = counts.get(e.cell_id, 0) + 1
    # 4096 / 7 = 585 余 1 → 前 1 个 Cell 拿 586,其余 585
    assert sorted(counts.values()) == [585] * 6 + [586]


def test_build_balanced_entries_rejects_cell_in_two_regions() -> None:
    """同一 cell 被声明在两个 region → 拒绝(错配的源头)。"""
    with pytest.raises(cellroute.CellRouteError, match="两个 region"):
        cellroute.build_balanced_entries(
            [cellroute.CellSpec(1, 5), cellroute.CellSpec(2, 5)]
        )


def test_route_failure_raises_not_default() -> None:
    """★ 路由失败必须抛异常,不能返回默认落点(§9.22 禁止冒充默认值)。"""

    class EmptyTable(cellroute.StaticTable):
        def __init__(self) -> None:  # 绕过父类校验,构造一个查不到的表
            pass

        def lookup(self, logical_cell: int):  # noqa: ANN201
            return None

    router = cellroute.Router(EmptyTable())
    with pytest.raises(cellroute.CellRouteError, match="未映射"):
        router.route(1001)


# ── killswitch ───────────────────────────────────────────────────────────────


def test_disabled_normalizes_leading_slash() -> None:
    """gRPC full method 带前导 "/",规则文件里通常不带 —— 两种写法必须都命中。"""
    mgr = killswitch.Manager()
    mgr.replace({"pandora.trade.v1.TradeService/CreateOrder": "交易维护中"})
    for op in (
        "/pandora.trade.v1.TradeService/CreateOrder",
        "pandora.trade.v1.TradeService/CreateOrder",
    ):
        blocked, reason = mgr.disabled(op)
        assert blocked and reason == "交易维护中", f"{op} 没命中规则"


def test_empty_reason_falls_back_to_default_text() -> None:
    """规则值为空串时用默认文案 —— 客户端不能收到空的维护提示。"""
    mgr = killswitch.Manager()
    mgr.replace({"a.b.C/D": ""})
    blocked, reason = mgr.disabled("/a.b.C/D")
    assert blocked and reason


def test_unlisted_operation_is_allowed() -> None:
    mgr = killswitch.Manager()
    mgr.replace({"a.b.C/D": "x"})
    assert mgr.disabled("/a.b.C/Other") == (False, "")


def test_package_level_disabled_is_fail_open_without_manager() -> None:
    """★ 没设默认 Manager 时必须 fail-open 放行。

    killswitch 是"临时关停"工具,默认状态是不关停。若规则源不可用就把所有 RPC 关掉,
    等于把运维工具变成全服故障开关 —— 这是刻意与 §9.22 的 fail-closed 相反的一处。
    """
    killswitch.set_default(None)
    assert killswitch.disabled("/anything/At") == (False, "")


def test_parse_rules_accepts_yaml_and_json() -> None:
    yaml_rules = killswitch.parse_rules(b'rules:\n  "a.b.C/D": "\xe7\xbb\xb4\xe6\x8a\xa4"\n')
    assert yaml_rules == {"a.b.C/D": "维护"}
    json_rules = killswitch.parse_rules(b'{"rules": {"a.b.C/D": "x"}}')
    assert json_rules == {"a.b.C/D": "x"}
    assert killswitch.parse_rules(b"") == {}


def test_file_source_missing_file_is_fail_open(tmp_path: pathlib.Path) -> None:
    """规则文件不存在 = 无规则,不是错误(本地联调常态)。"""
    src = killswitch.FileSource(tmp_path / "nope.yaml")
    assert src.load() == 0
    assert src.manager.disabled("/a/B") == (False, "")


def test_file_source_parse_failure_keeps_old_snapshot(tmp_path: pathlib.Path) -> None:
    """★ 解析失败必须保留旧快照,不能清空。

    清空 = 所有关停规则突然放开 = 正在维护的 RPC 重新接流量。
    与配置表热更的"加载失败保留旧配置"同理。
    """
    path = tmp_path / "ks.yaml"
    path.write_text('rules:\n  "a.b.C/D": "维护中"\n', encoding="utf-8")
    src = killswitch.FileSource(path)
    assert src.load() == 1
    assert src.manager.disabled("/a.b.C/D")[0]

    # 写入坏内容后重载
    path.write_text("rules: [this is a list not a mapping]\n", encoding="utf-8")
    src.load()
    still_blocked, reason = src.manager.disabled("/a.b.C/D")
    assert still_blocked, "解析失败后规则被清空了 —— 维护中的 RPC 会突然接流量"
    assert reason == "维护中"


def test_disabled_error_uses_service_disabled_code() -> None:
    """关停错误码必须是 ErrServiceDisabled(13),客户端据此提示维护而不是重试。"""
    from pandorapy import errcode

    err = killswitch.disabled_error("维护中")
    assert err.code == errcode.ErrServiceDisabled == 13


# ── dbguard ──────────────────────────────────────────────────────────────────


def test_check_payload_rejects_oversize() -> None:
    """超上限必须抛异常拒写 —— 这是"数据被静默截断"的唯一防线。"""
    with pytest.raises(dbguard.PayloadTooLargeError, match="超过上限"):
        dbguard.check_payload("bag_items", b"x" * 101, max_bytes=100)


def test_check_payload_allows_under_limit() -> None:
    dbguard.check_payload("bag_items", b"x" * 50, max_bytes=100)  # 不应抛


def test_check_payload_warns_at_80_percent() -> None:
    """达 80% 放行但告警 —— 留出排查窗口。"""
    assert dbguard.WARN_RATIO == 0.8
    dbguard.check_payload("bag_items", b"x" * 80, max_bytes=100)  # 放行,只 WARN


# ── mysqlx ───────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("8.0.11-TiDB-v8.5.0", (8, 5, 0)),
        ("5.7.25-TiDB-v6.1.0", (6, 1, 0)),
        ("8.4.0", None),  # 普通 MySQL
        ("8.0.36-MariaDB", None),
        ("", None),
    ],
)
def test_parse_tidb_version(version: str, expected: tuple[int, int, int] | None) -> None:
    """TiDB 版本解析必须与 Go 侧 tidbVersionRe 同结果。

    误把普通 MySQL 认成 TiDB 的后果:依赖 TiDB 语义的逻辑(无 gap 锁前提下的
    守卫行写法)会悄悄跑偏,不报错。
    """
    assert mysqlx.parse_tidb_version(version) == expected


def test_error_code_classification() -> None:
    """1062/1406/1213 必须能正确判别 —— 幂等与重试都依赖它。"""

    class FakeDBError(Exception):
        pass

    assert mysqlx.is_duplicate_entry(FakeDBError(1062, "dup"))
    assert mysqlx.is_data_too_long(FakeDBError(1406, "too long"))
    assert mysqlx.is_deadlock(FakeDBError(1213, "deadlock"))
    assert not mysqlx.is_deadlock(FakeDBError(1062, "dup"))
    assert not mysqlx.is_duplicate_entry(ValueError("not a db error"))


def test_map_db_error_does_not_swallow_deadlock() -> None:
    """死锁不该被映射成业务码 —— 它应当在数据层重试。

    走到 map_db_error 的死锁说明重试已耗尽,那是真的内部错误。
    """
    from pandorapy import errcode

    class FakeDBError(Exception):
        pass

    assert mysqlx.map_db_error(FakeDBError(1062, "dup")) == errcode.ErrAlreadyExists
    assert mysqlx.map_db_error(FakeDBError(1406, "long")) == errcode.ErrInvalidArg
    assert mysqlx.map_db_error(FakeDBError(1213, "deadlock")) == errcode.ErrInternal
