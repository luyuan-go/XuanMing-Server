"""push 投递缓冲测试 —— 打 fakeredis(真 Lua)。

这段 Lua 是投递语义的地基,必须真跑:游标分配、入缓冲、窗口修剪、条数修剪、
哨兵维护五件事在同一次执行里完成。mock 掉就等于什么都没测。

重点:
  1. ★ 游标严格递增且唯一(每玩家全序)
  2. ★ 游标基线用**服务端 now**,不是帧自带 ts(否则修剪会删掉刚写的帧)
  3. ★ Range(>X) 返回完整前缀,不漏
  4. ★ 修剪后 fl 哨兵记录最高被删游标(gap 判定的唯一依据)
  5. ★ 交付时 ts_ms 被重铸为游标
"""

from __future__ import annotations

import asyncio
import os

import pytest
from pandora.push.v1 import push_pb2

from pandorapy.services.push import offline as poff


@pytest.fixture
async def rdb():
    """★ 必须打**真 Redis**,fakeredis 在这段 Lua 上不够用。

    实测(2026-08-18):fakeredis 的 lupa 桥对 `redis.call('ZADD', key, fl, 'fl')`
    报 "Lua redis lib command arguments must be strings or integers" ——
    因为 `tonumber()` 的结果在 lupa 里是 float,而真 Redis 会自行转换。

    这不是脚本的 bug:脚本从 Go 侧**原样搬来**、在真 Redis 上跑了很久。
    正确的处置是换真依赖去测,**不是**为了迁就 fake 去改这段已验证的 Lua ——
    改了就等于把"原样搬"这条策略的全部价值丢掉。

    没有 Redis 就整体 skip(不假装通过):
        docker run -d --name pandora-redis-verify -p 16379:6379 redis:8-alpine
    """
    import redis.asyncio as aioredis

    addr = os.getenv("PANDORA_TEST_REDIS_ADDR", "127.0.0.1:16379")
    host, _, port = addr.rpartition(":")
    client = aioredis.Redis(
        host=host or "127.0.0.1", port=int(port), db=0,
        decode_responses=False, socket_connect_timeout=3, socket_timeout=3,
    )
    try:
        await asyncio.wait_for(client.ping(), timeout=4)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"Redis 不可用 @ {addr} ({exc}) —— push 投递缓冲测试跳过(不假装通过)。"
            f"起一个:docker run -d -p 16379:6379 redis:8-alpine"
        )
    await client.flushdb()
    try:
        yield client
    finally:
        await client.flushdb()
        await client.aclose()


def _frame(event_type: int = 1, ts_ms: int = 0):
    return push_pb2.PushFrame(event_type=event_type, ts_ms=ts_ms)


NOW = 1_760_000_000_000  # 固定"服务端 now",测试不依赖真实时钟


# ── ★ 游标严格递增且唯一 ────────────────────────────────────────────────────


async def test_cursor_strictly_increases(rdb) -> None:
    """★ 每玩家全序 —— 游标必须严格递增且唯一。

    Range(>X) 的"完整前缀"语义全靠这条:游标重复会让某一帧被 ZSET 的
    member 去重吃掉(若 member 不带游标前缀的话)。
    """
    cache = poff.RedisOfflineCache(rdb)
    cursors = []
    for i in range(20):
        cursors.append(await cache.assign_and_buffer(7, _frame(event_type=i), NOW))
    assert cursors == sorted(cursors), "游标非单调"
    assert len(set(cursors)) == len(cursors), "游标有重复"


async def test_same_millisecond_still_increases(rdb) -> None:
    """★ 同一毫秒内的多帧仍须严格递增 —— base+1 兜底。

    多 Pod 时钟偏差也靠这条不破坏单调性。
    """
    cache = poff.RedisOfflineCache(rdb)
    a = await cache.assign_and_buffer(7, _frame(1), NOW)
    b = await cache.assign_and_buffer(7, _frame(2), NOW)  # 同一个 now
    c = await cache.assign_and_buffer(7, _frame(3), NOW)
    assert a < b < c


async def test_clock_going_backwards_does_not_regress_cursor(rdb) -> None:
    """★ now 回拨(多 Pod 时钟偏差)不能让游标回退。"""
    cache = poff.RedisOfflineCache(rdb)
    a = await cache.assign_and_buffer(7, _frame(1), NOW)
    b = await cache.assign_and_buffer(7, _frame(2), NOW - 60_000)  # 时钟回拨一分钟
    assert b > a, "时钟回拨导致游标回退 —— 客户端会重复收到旧帧或漏帧"


# ── ★ 游标基线用服务端 now 而不是帧 ts ─────────────────────────────────────


async def test_stale_frame_ts_does_not_get_trimmed_immediately(rdb) -> None:
    """★ 这是审计 R4 P1-1 的核心:积压/重投帧的原始 ts 可能早于保留窗下界。

    若拿帧自带的 ts 当游标,同一个 Lua 里紧接着的窗口修剪会把**刚写入的帧
    连同哨兵一起删掉**,随后 ack kafka = 静默丢帧。
    用服务端 now 则写入的帧必然存活到本次修剪之后。
    """
    cache = poff.RedisOfflineCache(rdb, retention_sec=300)
    # 帧自带一个远早于保留窗的 ts(模拟 kafka 积压重投)
    ancient = _frame(event_type=9, ts_ms=NOW - 86_400_000)  # 一天前
    cursor = await cache.assign_and_buffer(7, ancient, NOW)
    assert cursor >= NOW, "游标用了帧自带的旧 ts"
    frames = await cache.range_after(7, 0, NOW)
    assert len(frames) == 1, "刚写入的帧被同一轮修剪删掉了 —— 静默丢帧"


async def test_far_future_frame_ts_does_not_poison_cursor(rdb) -> None:
    """远未来 ts 污染游标一并消除 —— 不信任任何外部时间戳。"""
    cache = poff.RedisOfflineCache(rdb)
    evil = _frame(event_type=9, ts_ms=2**52)  # 恶意/错误的远未来 ts
    cursor = await cache.assign_and_buffer(7, evil, NOW)
    assert cursor < 2**45, f"游标被外部 ts 污染:{cursor}"


# ── ★ Range 返回完整前缀 ────────────────────────────────────────────────────


async def test_range_returns_complete_prefix(rdb) -> None:
    """★ Range(>X) 必须返回 X 之后的完整前缀,一帧不漏。"""
    cache = poff.RedisOfflineCache(rdb)
    cursors = [await cache.assign_and_buffer(7, _frame(i), NOW) for i in range(10)]
    got = await cache.range_after(7, 0, NOW)
    assert [f.cursor for f in got] == cursors


async def test_range_is_strictly_after_cursor(rdb) -> None:
    """严格大于 —— 不能把客户端已收到的那帧再发一次。"""
    cache = poff.RedisOfflineCache(rdb)
    cursors = [await cache.assign_and_buffer(7, _frame(i), NOW) for i in range(5)]
    got = await cache.range_after(7, cursors[2], NOW)
    assert [f.cursor for f in got] == cursors[3:]


async def test_sentinels_are_never_delivered(rdb) -> None:
    """★ 哨兵 wm / fl 参与 score 排序但**不是帧**,绝不能交付给客户端。"""
    cache = poff.RedisOfflineCache(rdb)
    await cache.assign_and_buffer(7, _frame(1), NOW)
    got = await cache.range_after(7, 0, NOW)
    assert len(got) == 1
    for f in got:
        assert f.frame.event_type == 1


# ── ★ ts_ms 重铸为游标 ─────────────────────────────────────────────────────


async def test_delivered_ts_is_rewritten_to_cursor(rdb) -> None:
    """★ 交付时 frame.ts_ms 必须被重铸为投递游标。

    客户端用它推进恢复游标;若交付原始 kafka ts,客户端游标会与服务端游标体系
    脱节 —— 补推跳过或重复,而且不报错。
    """
    cache = poff.RedisOfflineCache(rdb)
    original_ts = NOW - 999
    cursor = await cache.assign_and_buffer(7, _frame(1, ts_ms=original_ts), NOW)
    got = await cache.range_after(7, 0, NOW)
    assert got[0].frame.ts_ms == cursor
    assert got[0].frame.ts_ms != original_ts


# ── ★ 修剪与 fl 哨兵 ────────────────────────────────────────────────────────


async def test_count_trim_records_floor(rdb) -> None:
    """★ 条数修剪后 fl 记录**最高被删游标** —— gap 判定的唯一依据。

    不记的话客户端从一个已被修剪的游标续传时,服务端无法判断"这段确实没了",
    只会返回空 —— 客户端永远等一段永远不会到的帧。
    """
    cache = poff.RedisOfflineCache(rdb, max_frames=5)
    cursors = [await cache.assign_and_buffer(7, _frame(i), NOW) for i in range(12)]
    lost = await cache.lost_since(7, 0, NOW)
    assert lost > 0, "修剪了却没记 fl —— 客户端无法判断丢失"
    assert lost in cursors, "fl 不是一个真实分配过的游标"
    # 缓冲里只剩上限内的帧
    remaining = await cache.range_after(7, 0, NOW)
    assert len(remaining) <= 5


async def test_lost_since_returns_zero_when_nothing_lost(rdb) -> None:
    """没有丢失时返回 0 —— 调用方据此决定不发 resync。"""
    cache = poff.RedisOfflineCache(rdb, max_frames=100)
    for i in range(5):
        await cache.assign_and_buffer(7, _frame(i), NOW)
    assert await cache.lost_since(7, 0, NOW) == 0


async def test_lost_since_only_reports_above_client_cursor(rdb) -> None:
    """★ 只有 fl > 客户端游标时才算丢失。

    客户端已经越过被修剪的那段时,补推是能闭合的,不该误发 resync
    (每发一次 resync 客户端就要全量重拉一次状态)。
    """
    cache = poff.RedisOfflineCache(rdb, max_frames=5)
    for i in range(12):
        await cache.assign_and_buffer(7, _frame(i), NOW)
    floor = await cache.lost_since(7, 0, NOW)
    assert floor > 0
    # 客户端游标已经在 fl 之后 → 无丢失
    assert await cache.lost_since(7, floor + 1000, NOW) == 0


# ── 隔离与格式 ──────────────────────────────────────────────────────────────


async def test_players_do_not_share_cursor_space(rdb) -> None:
    """每玩家独立 key,游标空间互不干扰。"""
    cache = poff.RedisOfflineCache(rdb)
    await cache.assign_and_buffer(1, _frame(1), NOW)
    await cache.assign_and_buffer(2, _frame(2), NOW)
    assert len(await cache.range_after(1, 0, NOW)) == 1
    assert len(await cache.range_after(2, 0, NOW)) == 1


def test_key_format_matches_go() -> None:
    assert poff.offline_key(1001) == "pandora:push:offline:1001"


async def test_corrupt_member_is_skipped_not_fatal(rdb) -> None:
    """脏数据(旧格式残留)跳过而不是让整次拉取失败。

    ⚠️ 这**不是兼容手段** —— 上线后演进 member 格式必须按 §9.17 双向兼容
    纪律另行设计,不得复用静默跳过。
    """
    cache = poff.RedisOfflineCache(rdb)
    good = await cache.assign_and_buffer(7, _frame(1), NOW)
    # 手工塞一条不符合 %020d + 0x1f 格式的成员
    await rdb.zadd(poff.offline_key(7), {b"garbage-member": good + 1})
    got = await cache.range_after(7, 0, NOW)
    assert [f.cursor for f in got] == [good]
