"""临时群名额占用测试 —— 打真实 MySQL。

重点是那个"两把锁 + 对账"模式的三条约束:
  1. ★ 明细 COUNT 是权威,计数行只是串行化点 —— 计数行写脏不能让上限判错
  2. ★ 对账用**绝对值回写**,脏计数必须自愈(不是 ±1 把错误累积下去)
  3. ★ 并发不能突破上限

没有库就整体 skip(不假装通过)。
"""

from __future__ import annotations

import asyncio
import os

import pytest

from pandorapy import errcode
from pandorapy.services.guild import group_repo as grepo

DSN = os.getenv("PANDORA_TEST_MYSQL_DSN", "root:pandora_dev_root@tcp(127.0.0.1:13306)/")

_DDL = [
    """CREATE TABLE IF NOT EXISTS chat_groups (
        group_id BIGINT UNSIGNED NOT NULL,
        owner_id BIGINT UNSIGNED NOT NULL,
        member_count INT NOT NULL DEFAULT 0,
        PRIMARY KEY (group_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS chat_group_members (
        group_id BIGINT UNSIGNED NOT NULL,
        player_id BIGINT UNSIGNED NOT NULL,
        role INT NOT NULL DEFAULT 0,
        PRIMARY KEY (group_id, player_id),
        KEY idx_player (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS player_group_counts (
        player_id BIGINT UNSIGNED NOT NULL,
        group_count INT NOT NULL DEFAULT 0,
        PRIMARY KEY (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]

_TABLES = ["chat_groups", "chat_group_members", "player_group_counts"]


def _parse_dsn(dsn: str) -> dict:
    cred, _, rest = dsn.partition("@tcp(")
    user, _, password = cred.partition(":")
    hostport, _, dbpart = rest.partition(")/")
    host, _, port = hostport.partition(":")
    return {
        "user": user, "password": password,
        "host": host or "127.0.0.1", "port": int(port or 3306),
        "db": (dbpart.split("?")[0] or "pandora_owner"),
    }


@pytest.fixture
async def pool():
    asyncmy = pytest.importorskip("asyncmy")
    pytest.importorskip("cryptography", reason="MySQL 8.x caching_sha2_password 需要它")
    cfg = _parse_dsn(DSN)
    try:
        p = await asyncio.wait_for(
            asyncmy.create_pool(
                host=cfg["host"], port=cfg["port"], user=cfg["user"],
                password=cfg["password"], db=cfg["db"] or "pandora_owner",
                minsize=2, maxsize=24, autocommit=True,
            ),
            timeout=8,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"MySQL 不可用 ({exc}) —— 临时群测试跳过(不假装通过)")
    try:
        async with p.acquire() as conn, conn.cursor() as cur:
            for ddl in _DDL:
                await cur.execute(ddl)
            for t in _TABLES:
                await cur.execute(f"TRUNCATE TABLE {t}")  # noqa: S608
        yield p
    finally:
        p.close()
        await p.wait_closed()


@pytest.fixture
async def repo(pool):
    return grepo.MySQLGroupRepo(pool)


# ── ★ 明细 COUNT 是权威 ─────────────────────────────────────────────────────


async def test_dirty_counter_does_not_break_limit(repo, pool) -> None:
    """★ 计数行被写脏时,上限判定仍必须正确 —— 权威是明细 COUNT。

    这是这个模式存在的理由:计数行只是串行化点,写脏(旧 Pod 残留 / 手工改)
    不能让玩家凭空多出名额或被误判满员。
    """
    await repo.create_group(1, owner_id=100, member_ids=[], max_members=50,
                            max_groups_per_player=3)
    # 手工把计数行改成一个荒谬的大值(模拟旧 Pod 留脏)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE player_group_counts SET group_count = 999 WHERE player_id = 100"
        )
    # 上限 3、实际只在 1 个群 → 必须还能继续建群
    await repo.create_group(2, owner_id=100, member_ids=[], max_members=50,
                            max_groups_per_player=3)
    await repo.create_group(3, owner_id=100, member_ids=[], max_members=50,
                            max_groups_per_player=3)
    assert sorted(await repo.list_my_groups(100)) == [1, 2, 3]


async def test_dirty_counter_self_heals(repo, pool) -> None:
    """★ 脏计数必须**自愈** —— 对账是绝对值回写,不是 ±1。

    写成 `group_count + 1` 的话,一旦脏了就永远脏下去并越滚越大。
    """
    await repo.create_group(1, owner_id=200, member_ids=[], max_members=50,
                            max_groups_per_player=10)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "UPDATE player_group_counts SET group_count = 777 WHERE player_id = 200"
        )
    assert await repo.player_group_count(200) == 777  # 确认脏了
    # 任意一次名额操作都应把它拉回真实值
    await repo.create_group(2, owner_id=200, member_ids=[], max_members=50,
                            max_groups_per_player=10)
    assert await repo.player_group_count(200) == 2, "脏计数没有自愈"


async def test_counter_matches_details_after_leave(repo) -> None:
    """退群后计数行必须与明细一致(release 是重算不是 -1)。"""
    await repo.create_group(1, owner_id=300, member_ids=[301], max_members=50,
                            max_groups_per_player=10)
    await repo.create_group(2, owner_id=300, member_ids=[], max_members=50,
                            max_groups_per_player=10)
    assert await repo.player_group_count(300) == 2
    await repo.remove_member(1, 300)
    assert await repo.player_group_count(300) == 1
    assert await repo.list_my_groups(300) == [2]


# ── ★ 上限(§9.18)────────────────────────────────────────────────────────────


async def test_groups_per_player_limit(repo) -> None:
    for gid in range(1, 4):
        await repo.create_group(gid, owner_id=400, member_ids=[], max_members=50,
                                max_groups_per_player=3)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.create_group(99, owner_id=400, member_ids=[], max_members=50,
                                max_groups_per_player=3)
    assert exc.value.code == errcode.ErrGroupJoinLimit


async def test_group_member_limit(repo) -> None:
    await repo.create_group(1, owner_id=500, member_ids=[501, 502], max_members=3,
                            max_groups_per_player=10)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.add_member(1, 503, max_members=3, max_groups_per_player=10)
    assert exc.value.code == errcode.ErrGroupFull


async def test_create_rejects_oversized_roster_upfront(repo) -> None:
    """建群时成员数就超上限 → 直接拒,不留半个群。"""
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.create_group(1, owner_id=600, member_ids=[601, 602, 603],
                                max_members=2, max_groups_per_player=10)
    assert exc.value.code == errcode.ErrGroupFull
    assert await repo.list_my_groups(600) == []


# ── ★ 并发 ──────────────────────────────────────────────────────────────────


async def test_concurrent_joins_do_not_exceed_group_limit(repo) -> None:
    """★ 并发加人不能突破群成员上限 —— 群行锁是每群的串行化点。"""
    await repo.create_group(1, owner_id=700, member_ids=[], max_members=5,
                            max_groups_per_player=100)
    results = await asyncio.gather(
        *(
            repo.add_member(1, 800 + i, max_members=5, max_groups_per_player=100)
            for i in range(20)
        ),
        return_exceptions=True,
    )
    ok = [r for r in results if not isinstance(r, BaseException)]
    full = [r for r in results if isinstance(r, errcode.PandoraError)
            and r.code == errcode.ErrGroupFull]
    assert len(ok) == 4, f"上限 5(含 owner)被突破:成功了 {len(ok)} 个"
    assert len(full) == 16
    assert len(ok) + len(full) == 20, "有非预期错误(死锁没被重试掉?)"


async def test_concurrent_creates_do_not_exceed_player_limit(repo) -> None:
    """★ 同一玩家并发建群不能突破"所在群"上限 —— 计数行是串行化点。"""
    results = await asyncio.gather(
        *(
            repo.create_group(1000 + i, owner_id=900, member_ids=[],
                              max_members=50, max_groups_per_player=4)
            for i in range(16)
        ),
        return_exceptions=True,
    )
    ok = [r for r in results if not isinstance(r, BaseException)]
    limited = [r for r in results if isinstance(r, errcode.PandoraError)
               and r.code == errcode.ErrGroupJoinLimit]
    assert len(ok) == 4, f"上限被突破:成功建了 {len(ok)} 个群"
    assert len(ok) + len(limited) == 16, "有非预期错误"
    assert len(await repo.list_my_groups(900)) == 4


# ── 幂等 ────────────────────────────────────────────────────────────────────


async def test_add_existing_member_is_idempotent(repo) -> None:
    """重复加同一个人 → 幂等 no-op,**且不占额外名额**。"""
    await repo.create_group(1, owner_id=1100, member_ids=[1101], max_members=50,
                            max_groups_per_player=10)
    await repo.add_member(1, 1101, max_members=50, max_groups_per_player=10)
    await repo.add_member(1, 1101, max_members=50, max_groups_per_player=10)
    assert await repo.player_group_count(1101) == 1
    assert await repo.list_my_groups(1101) == [1]


async def test_remove_nonexistent_member_is_noop(repo) -> None:
    await repo.create_group(1, owner_id=1200, member_ids=[], max_members=50,
                            max_groups_per_player=10)
    await repo.remove_member(1, 9999)  # 不在群里
    assert await repo.player_group_count(1200) == 1


async def test_add_to_missing_group_rejected(repo) -> None:
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.add_member(9999, 1300, max_members=50, max_groups_per_player=10)
    assert exc.value.code == errcode.ErrGroupNotFound


# ── 读取侧上限 ──────────────────────────────────────────────────────────────


async def test_list_my_groups_has_sql_limit(repo) -> None:
    """§9.18 读取侧兜底。"""
    for gid in range(1, 8):
        await repo.create_group(gid, owner_id=1400, member_ids=[], max_members=50,
                                max_groups_per_player=100)
    assert len(await repo.list_my_groups(1400, limit=3)) == 3
    assert len(await repo.list_my_groups(1400)) == 7
