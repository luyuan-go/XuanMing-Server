"""friend 数据层测试 —— **打真实 MySQL**。

★ 本文件存在的首要理由是复现 2026-08-11 那次 1213 死锁:

    16 个并发申请(**互不相同的 requester 与 target**,没有任何共享行)在 RR 隔离下必炸。
    修复是两条:① 写事务显式 READ COMMITTED;② player 守卫提到所有锁定读之前。

    这个死锁**只在 MySQL 上炸**(TiDB 无 gap 锁),所以只跑 TiDB 会一直是绿的 ——
    双后端都要跑才看得见。

没有库就整体 skip(不假装通过)。
"""

from __future__ import annotations

import asyncio
import os

import pytest

from pandorapy import errcode
from pandorapy.services.friend import repo as frepo

DSN = os.getenv("PANDORA_TEST_MYSQL_DSN", "root:pandora_dev_root@tcp(127.0.0.1:13306)/")

_DDL = [
    """CREATE TABLE IF NOT EXISTS friendships (
        player_id BIGINT UNSIGNED NOT NULL,
        friend_id BIGINT UNSIGNED NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id, friend_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS friend_requests (
        request_id BIGINT UNSIGNED NOT NULL,
        requester_id BIGINT UNSIGNED NOT NULL,
        target_id BIGINT UNSIGNED NOT NULL,
        status INT NOT NULL DEFAULT 1,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (request_id),
        UNIQUE KEY uk_requester_target (requester_id, target_id),
        KEY idx_target_status (target_id, status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS blocks (
        player_id BIGINT UNSIGNED NOT NULL,
        blocked_id BIGINT UNSIGNED NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (player_id, blocked_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS friend_player_guards (
        player_id BIGINT UNSIGNED NOT NULL,
        PRIMARY KEY (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS friend_pair_guards (
        lo_id BIGINT UNSIGNED NOT NULL,
        hi_id BIGINT UNSIGNED NOT NULL,
        PRIMARY KEY (lo_id, hi_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]

_TABLES = [
    "friendships",
    "friend_requests",
    "blocks",
    "friend_player_guards",
    "friend_pair_guards",
]


def _parse_dsn(dsn: str) -> dict:
    cred, _, rest = dsn.partition("@tcp(")
    user, _, password = cred.partition(":")
    hostport, _, dbpart = rest.partition(")/")
    host, _, port = hostport.partition(":")
    return {
        "user": user,
        "password": password,
        "host": host or "127.0.0.1",
        "port": int(port or 3306),
        "db": (dbpart.split("?")[0] or "pandora_owner"),
    }


@pytest.fixture
async def pool():
    """function 作用域 —— async fixture 的 event loop 绑定,见 test_owner_repo 的解释。"""
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
        pytest.skip(f"MySQL 不可用 ({exc}) —— friend 数据层测试跳过(不假装通过)")
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
    return frepo.MySQLFriendRepo(pool)


# ── ★ 死锁复现(本文件的首要理由)──────────────────────────────────────────


async def test_many_distinct_pairs_concurrently_no_deadlock(repo) -> None:
    """★ 16 个并发申请、**互不相同的 requester 与 target** → 必须全部成功。

    这正是 2026-08-11 的死锁形状:这些事务**没有任何共享行**,
    但 RR 下未命中的 FOR UPDATE 锁的是间隙,间隙跨 pair 共享 → 1213。

    若把 repo 的隔离级别改回 RR(或把 player 守卫挪到探针之后),这条必红。
    """
    results = await asyncio.gather(
        *(
            repo.create_request(900_000 + i, 1000 + i, 2000 + i, max_incoming=200)
            for i in range(16)
        ),
        return_exceptions=True,
    )
    failures = [r for r in results if isinstance(r, BaseException)]
    assert not failures, f"{len(failures)}/16 个并发申请失败(死锁?):{failures[:2]}"
    assert all(created for _rid, created in results)


async def test_many_requesters_same_target_no_deadlock(repo) -> None:
    """★ N 个不同 requester 指向**同一 target** —— 死锁日志里那个 supremum 间隙的形状。

    它们共享 target 的 player 守卫,所以会串行;串行是对的,死锁不是。
    """
    results = await asyncio.gather(
        *(
            repo.create_request(910_000 + i, 3000 + i, 4242, max_incoming=200)
            for i in range(16)
        ),
        return_exceptions=True,
    )
    failures = [r for r in results if isinstance(r, BaseException)]
    assert not failures, f"{len(failures)}/16 个失败(死锁?):{failures[:2]}"
    incoming = await repo.list_incoming_requests(4242)
    assert len(incoming) == 16


# ── ★ §9.18 三个列表上限 ────────────────────────────────────────────────────


async def test_incoming_request_limit_enforced(repo) -> None:
    """★ 收件箱上限在**事务内**校验,并发也不能突破。"""
    for i in range(3):
        await repo.create_request(920_000 + i, 5000 + i, 6001, max_incoming=3)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.create_request(920_099, 5099, 6001, max_incoming=3)
    assert exc.value.code == errcode.ErrFriendRequestLimit


async def test_incoming_limit_holds_under_concurrency(repo) -> None:
    """★ 并发申请不能突破上限 —— 守卫行 + 守卫锁内的锁定读才是权威。

    若限额只靠事务外预检(或 COUNT 用普通读),这里会超。
    """
    limit = 5
    results = await asyncio.gather(
        *(
            repo.create_request(930_000 + i, 7000 + i, 8001, max_incoming=limit)
            for i in range(20)
        ),
        return_exceptions=True,
    )
    ok = [r for r in results if not isinstance(r, BaseException)]
    rejected = [r for r in results if isinstance(r, errcode.PandoraError)]
    assert len(ok) == limit, f"上限 {limit} 被突破:成功了 {len(ok)} 个"
    assert all(r.code == errcode.ErrFriendRequestLimit for r in rejected)
    assert len(await repo.list_incoming_requests(8001)) == limit


async def test_friend_limit_checked_for_both_sides(repo) -> None:
    """★ 接受申请时**双方**的好友数上限都要校验 —— 只校验一方会让另一方越界。"""
    # 给 9001 塞满好友
    async with repo._pool.acquire() as conn, conn.cursor() as cur:  # noqa: SLF001
        for i in range(3):
            await cur.execute(
                "INSERT INTO friendships (player_id, friend_id) VALUES (%s, %s)",
                (9001, 100 + i),
            )
    # 9002 申请加 9001,9001 接受 —— 应因 **9001** 满而拒
    await repo.create_request(940_001, 9002, 9001, max_incoming=200)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.accept_request(940_001, actor_id=9001, max_friends=3)
    assert exc.value.code == errcode.ErrFriendLimit


async def test_block_limit_enforced(repo) -> None:
    for i in range(3):
        await repo.block(9101, 200 + i, max_blocks=3)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.block(9101, 299, max_blocks=3)
    assert exc.value.code == errcode.ErrFriendBlockLimit


async def test_list_read_hard_limit_caps_result(repo) -> None:
    """★ 读取侧 SQL LIMIT 兜底 —— 防历史脏数据造成无界返回。"""
    async with repo._pool.acquire() as conn, conn.cursor() as cur:  # noqa: SLF001
        for i in range(20):
            await cur.execute(
                "INSERT INTO friendships (player_id, friend_id) VALUES (%s, %s)",
                (9201, 300 + i),
            )
    assert len(await repo.list_friends(9201, limit=5)) == 5
    # limit 超过硬上限时被钳到硬上限
    assert len(await repo.list_friends(9201, limit=99999)) == 20


# ── 幂等与状态机 ────────────────────────────────────────────────────────────


async def test_duplicate_request_is_idempotent_and_takes_no_new_slot(repo) -> None:
    """★ 重复申请同一目标 → 幂等返回既有 pending,**不占新名额**。

    否则一个人反复点"加好友"就能刷爆别人的收件箱。
    """
    rid1, created1 = await repo.create_request(950_001, 9301, 9302, max_incoming=200)
    rid2, created2 = await repo.create_request(950_002, 9301, 9302, max_incoming=200)
    assert created1 and not created2
    assert rid1 == rid2
    assert len(await repo.list_incoming_requests(9302)) == 1


async def test_blocked_pair_cannot_request(repo) -> None:
    """★ 拉黑是双向的 —— 任一方向拉黑都不允许申请。"""
    await repo.block(9401, 9402, max_blocks=200)
    for a, b in ((9401, 9402), (9402, 9401)):
        with pytest.raises(errcode.PandoraError) as exc:
            await repo.create_request(960_001, a, b, max_incoming=200)
        assert exc.value.code == errcode.ErrFriendBlocked


async def test_block_removes_friendship_and_pending(repo) -> None:
    """★ 拉黑必须彻底:删好友关系 + 作废两个方向的 pending。

    留着关系或 pending 会让"拉黑了还能收到他消息 / 还能被他加回来"。
    """
    await repo.create_request(970_001, 9501, 9502, max_incoming=200)
    await repo.accept_request(970_001, actor_id=9502, max_friends=200)
    assert 9502 in await repo.list_friends(9501)

    await repo.create_request(970_002, 9503, 9501, max_incoming=200)  # 另一条 pending
    await repo.block(9501, 9502, max_blocks=200)

    assert 9502 not in await repo.list_friends(9501)
    assert 9501 not in await repo.list_friends(9502)
    # 只作废与被拉黑者相关的那条,别人的 pending 不受影响
    assert [r[1] for r in await repo.list_incoming_requests(9501)] == [9503]


async def test_only_target_can_accept_or_reject(repo) -> None:
    """★ 只有 target 能处理申请 —— requester 自己不能接受自己的申请。

    ★ 返回码必须是 ErrFriendNotFound 而**不是** ErrUnauthorized:
    后者等于告诉调用方「这条申请确实存在」,是信息泄露。
    非 target 应当无从区分"没这条申请"和"有但不是给你的"(与 Go 侧一致)。
    """
    await repo.create_request(980_001, 9601, 9602, max_incoming=200)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.accept_request(980_001, actor_id=9601, max_friends=200)
    assert exc.value.code == errcode.ErrFriendNotFound
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.reject_request(980_001, actor_id=9601)
    assert exc.value.code == errcode.ErrFriendNotFound


async def test_accept_creates_bidirectional_friendship(repo) -> None:
    await repo.create_request(990_001, 9701, 9702, max_incoming=200)
    await repo.accept_request(990_001, actor_id=9702, max_friends=200)
    assert 9702 in await repo.list_friends(9701)
    assert 9701 in await repo.list_friends(9702)


async def test_accept_twice_is_rejected(repo) -> None:
    """已处理的申请不能再处理(状态机)。"""
    await repo.create_request(991_001, 9801, 9802, max_incoming=200)
    await repo.accept_request(991_001, actor_id=9802, max_friends=200)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.accept_request(991_001, actor_id=9802, max_friends=200)
    assert exc.value.code == errcode.ErrFriendNotFound


async def test_rejected_request_can_be_resent(repo) -> None:
    """被拒后可以重新申请(复活成 pending),且要重新过限额。"""
    await repo.create_request(992_001, 9901, 9902, max_incoming=200)
    await repo.reject_request(992_001, actor_id=9902)
    assert await repo.list_incoming_requests(9902) == []
    rid, created = await repo.create_request(992_002, 9901, 9902, max_incoming=200)
    assert created and rid == 992_001  # 复用既有行(uk_requester_target)
    assert len(await repo.list_incoming_requests(9902)) == 1


async def test_already_friends_cannot_request(repo) -> None:
    await repo.create_request(993_001, 9911, 9912, max_incoming=200)
    await repo.accept_request(993_001, actor_id=9912, max_friends=200)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.create_request(993_002, 9911, 9912, max_incoming=200)
    assert exc.value.code == errcode.ErrFriendAlreadyAdded


# ── 守卫行 ──────────────────────────────────────────────────────────────────


async def test_pair_guard_is_order_independent(repo, pool) -> None:
    """★ 同一对玩家无论谁发起,必须落到**同一行**守卫,否则守卫形同虚设。"""
    await repo.create_request(994_001, 100, 200, max_incoming=200)
    await repo.block(200, 300, max_blocks=200)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute("SELECT lo_id, hi_id FROM friend_pair_guards ORDER BY lo_id")
        rows = await cur.fetchall()
    for lo, hi in rows:
        assert lo <= hi, f"守卫行未归一化:({lo},{hi})"


async def test_isolation_level_is_read_committed() -> None:
    """★ 隔离级别是**正确性要求**,不是调优 —— 改回 RR 会让并发申请死锁。"""
    assert frepo.WRITE_TX_ISOLATION == "READ COMMITTED"
