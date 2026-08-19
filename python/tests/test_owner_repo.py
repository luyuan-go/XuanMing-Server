"""owner 数据层测试 —— **打真实 MySQL**,不用 mock。

为什么必须打真库:
    被测的就是**事务本身** —— SELECT ... FOR UPDATE 的串行化、同事务读旧租约算屏障、
    epoch 单调 CAS、PENDING→ADMITTED 推进。用 fake repo 测等于把被测对象换成了
    我对事务语义的想象,而 §9.22 的全部价值恰恰在于这些语义真的成立。

没有库就整体 skip(**不假装通过**):
    docker run -d --name pandora-mysql-verify -p 13306:3306 \
      -e MYSQL_ROOT_PASSWORD=pandora_dev_root -e MYSQL_DATABASE=pandora_owner \
      mysql:8.4 --sql-mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"

环境变量 PANDORA_TEST_MYSQL_DSN 可覆盖(与 Go 侧 CI 的门控变量同名)。
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

from pandorapy import errcode, placement
from pandorapy.services.owner import data as odata
from pandorapy.services.owner import repo as orepo

DSN = os.getenv(
    "PANDORA_TEST_MYSQL_DSN", "root:pandora_dev_root@tcp(127.0.0.1:13306)/"
)

# 与 deploy/mysql-init/15-owner-tables.sql 同构(TiDB 侧 02-owner-tidb.sql 同 DDL)。
_DDL = [
    """CREATE TABLE IF NOT EXISTS owner_record (
        player_id BIGINT UNSIGNED NOT NULL,
        owner_epoch BIGINT UNSIGNED NOT NULL DEFAULT 0,
        owner_type TINYINT NOT NULL DEFAULT 0,
        phase TINYINT NOT NULL DEFAULT 0,
        pod_name VARCHAR(128) NOT NULL DEFAULT '',
        instance_uid VARCHAR(128) NOT NULL DEFAULT '',
        instance_epoch INT UNSIGNED NOT NULL DEFAULT 0,
        assignment_or_allocation_id VARCHAR(128) NOT NULL DEFAULT '',
        release_track VARCHAR(32) NOT NULL DEFAULT '',
        operation_id VARCHAR(64) NOT NULL DEFAULT '',
        admit_not_before_ms BIGINT NOT NULL DEFAULT 0,
        hub_source_revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
        updated_at_ms BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (player_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS ds_instance_lease (
        instance_uid VARCHAR(128) NOT NULL,
        pod_name VARCHAR(128) NOT NULL DEFAULT '',
        instance_epoch INT UNSIGNED NOT NULL DEFAULT 0,
        release_track VARCHAR(32) NOT NULL DEFAULT '',
        lease_deadline_ms BIGINT NOT NULL DEFAULT 0,
        updated_at_ms BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY (instance_uid)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS owner_transition_log (
        id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        player_id BIGINT UNSIGNED NOT NULL,
        from_epoch BIGINT UNSIGNED NOT NULL,
        to_epoch BIGINT UNSIGNED NOT NULL,
        op TINYINT NOT NULL,
        operation_id VARCHAR(64) NOT NULL,
        detail VARCHAR(512) NOT NULL DEFAULT '',
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (id),
        KEY idx_player (player_id),
        KEY idx_created_at (created_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
]


def _parse_dsn(dsn: str) -> dict:
    """解析 Go 风格 DSN `user:pass@tcp(host:port)/db`。"""
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
    """每个用例一个连接池 —— **必须 function 作用域**。

    ⚠️ Python 迁移特有的坑(2026-08-18 实测):
        pytest-asyncio 默认给**每个用例**新建一个 event loop,而 module 作用域的
        async fixture 建在另一个 loop 里。asyncmy 的池把内部 Task 绑在创建时的 loop 上,
        于是第二个用例开始全部报
            RuntimeError: got Future attached to a different loop
        表现是"第一个用例失败、其余 21 个 ERROR",很容易被误读成连接池坏了。

        Go 侧没有这一层 —— database/sql 的池与 goroutine 无绑定关系。
        凡是"持有后台任务的异步资源"(DB 池、redis 池、grpc channel、kafka client)
        在 Python 测试里都要注意这个作用域约束。

    代价是每个用例重建池(本地 MySQL 上约几十毫秒),换来完全隔离,值得。
    """
    asyncmy = pytest.importorskip("asyncmy")
    pytest.importorskip(
        "cryptography",
        reason="MySQL 8.x 默认 caching_sha2_password 认证,Python 驱动需要 cryptography",
    )
    cfg = _parse_dsn(DSN)
    cfg["db"] = cfg["db"] or "pandora_owner"
    try:
        p = await asyncio.wait_for(
            asyncmy.create_pool(
                host=cfg["host"], port=cfg["port"], user=cfg["user"],
                password=cfg["password"], db=cfg["db"], minsize=1, maxsize=8,
                autocommit=True,
            ),
            timeout=8,
        )
    except Exception as exc:  # noqa: BLE001
        pytest.skip(
            f"MySQL 不可用 @ {cfg['host']}:{cfg['port']} ({exc}) —— "
            f"owner 数据层测试整体跳过(不假装通过)。"
            f"起一个:docker run -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=pandora_dev_root "
            f"-e MYSQL_DATABASE=pandora_owner mysql:8.4 "
            f'--sql-mode="STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION"'
        )
    try:
        async with p.acquire() as conn, conn.cursor() as cur:
            for ddl in _DDL:
                await cur.execute(ddl)
            for t in ("owner_record", "ds_instance_lease", "owner_transition_log"):
                await cur.execute(f"TRUNCATE TABLE {t}")  # noqa: S608
        yield p
    finally:
        p.close()
        await p.wait_closed()


@pytest.fixture
async def repo(pool):
    return orepo.MySQLOwnerRepo(pool)


def _target(**kw) -> odata.OwnerTarget:
    base = {
        "pod_name": "hub-1",
        "instance_uid": "uid-A",
        "instance_epoch": 1,
        "assignment_or_allocation_id": "assign-1",
        "release_track": "stable",
    }
    base.update(kw)
    return odata.OwnerTarget(**base)


def _op() -> str:
    return str(uuid.uuid4())


SKEW = placement.DS_FENCE_SKEW_MARGIN_SECONDS


# ── 严格模式(§9.24 唯一允许拒绝启动的检查)────────────────────────────────


async def test_sql_mode_is_strict(pool) -> None:
    """★ 前置条件:测试库必须是严格模式。

    非严格模式下超长写入**静默截断**,后面所有"列宽钳制"的断言都会变成假通过。
    """
    from pandorapy import dbguard

    async with pool.acquire() as conn:
        await dbguard.assert_strict_mode(conn)  # 不抛即通过


# ── ★ epoch 单调 CAS ────────────────────────────────────────────────────────


async def test_first_transition_starts_at_epoch_1(repo) -> None:
    rec = await repo.begin_transition(
        1001, 0, _op(), odata.OWNER_TYPE_HUB, _target(), 0, SKEW
    )
    assert rec.owner_epoch == 1
    assert rec.phase == odata.OWNER_PHASE_PENDING


async def test_concurrent_begin_only_one_wins(repo) -> None:
    """★ 并发 Begin 同一玩家、同一 expect_epoch → **只有一个成功**。

    这是"玩家同一时刻只在一个可操作 DS"的第一道闸。若两个都成功,
    两台 DS 会各自拿到一个 PENDING 记录并各自去 Admit。
    """
    results = await asyncio.gather(
        *(
            repo.begin_transition(
                2001, 0, _op(), odata.OWNER_TYPE_HUB,
                _target(instance_uid=f"uid-{i}", assignment_or_allocation_id=f"a-{i}"),
                0, SKEW,
            )
            for i in range(12)
        ),
        return_exceptions=True,
    )
    ok = [r for r in results if isinstance(r, odata.OwnerRecord)]
    conflicts = [r for r in results if isinstance(r, errcode.PandoraError)]
    assert len(ok) == 1, f"{len(ok)} 个并发 Begin 同时成功 —— epoch CAS 失效"
    assert all(c.code == errcode.ErrOwnerEpochConflict for c in conflicts)


async def test_epoch_conflict_carries_current_record(repo) -> None:
    """★ epoch 冲突必须**附当前记录** —— 调用方靠它决定重试还是放弃。"""
    await repo.begin_transition(3001, 0, _op(), odata.OWNER_TYPE_HUB, _target(), 0, SKEW)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.begin_transition(
            3001, 0, _op(), odata.OWNER_TYPE_HUB, _target(instance_uid="uid-B"), 0, SKEW
        )
    assert exc.value.code == errcode.ErrOwnerEpochConflict
    assert getattr(exc.value, "current_record", None) is not None
    assert exc.value.current_record.owner_epoch == 1


async def test_same_exact_target_is_idempotent_noop(repo) -> None:
    """★ 同 exact 实例的重复投递 → 原样返回既有记录(含**原** operation_id)。

    这是"权威铸造 operation"能成立的前提:重复投递不换 operation,
    否则同一次进场的重连/重复交付会写出不同 operation,幂等键失效。
    """
    target = _target()
    first = await repo.begin_transition(
        4001, 0, _op(), odata.OWNER_TYPE_HUB, target, 0, SKEW
    )
    again = await repo.begin_transition(
        4001, 999, _op(), odata.OWNER_TYPE_HUB, target, 0, SKEW  # expect_epoch 故意乱填
    )
    assert again.owner_epoch == first.owner_epoch
    assert again.operation_id == first.operation_id, "重复投递换了 operation_id"


# ── ★ 屏障 ──────────────────────────────────────────────────────────────────


async def test_admit_rejected_before_barrier(repo) -> None:
    """★ 屏障未开时 Admit 必须拒 —— 这是核心时序不等式的执行点。"""
    battle = _target(pod_name="battle-1", instance_uid="uid-B", assignment_or_allocation_id="al-1")
    # 先让 battle 实例有一个还没过期的租约
    await repo.renew_instance_lease(battle, 20)
    op1 = _op()
    await repo.begin_transition(5001, 0, op1, odata.OWNER_TYPE_BATTLE, battle, 0, SKEW)
    await repo.admit(5001, 1, op1, battle)  # BATTLE 首次:旧 owner 是 none,屏障 = now

    # 现在从 BATTLE 迁到 HUB —— 旧 owner 是 BATTLE,屏障 = 旧租约截止 + 余量
    hub = _target()
    op2 = _op()
    rec = await repo.begin_transition(5001, 1, op2, odata.OWNER_TYPE_HUB, hub, 0, SKEW)
    assert rec.admit_not_before_ms > odata.now_ms(), "BATTLE→HUB 屏障没有推迟"

    with pytest.raises(errcode.PandoraError) as exc:
        await repo.admit(5001, 2, op2, hub)
    assert exc.value.code == errcode.ErrOwnerBarrierNotOpen


async def test_admit_succeeds_after_barrier_opens(repo) -> None:
    """屏障开后 Admit 成功,phase → ADMITTED。"""
    op = _op()
    target = _target()
    await repo.begin_transition(6001, 0, op, odata.OWNER_TYPE_HUB, target, 0, SKEW)
    rec, retry = await repo.admit(6001, 1, op, target)
    assert rec.phase == odata.OWNER_PHASE_ADMITTED
    assert retry == 0


async def test_hub_predecessor_opens_barrier_immediately(repo) -> None:
    """★ HUB→HUB 迁移屏障不等待 —— 否则每次换线都卡 27 秒。"""
    op1, op2 = _op(), _op()
    a = _target(instance_uid="uid-A")
    b = _target(instance_uid="uid-B", assignment_or_allocation_id="assign-2")
    await repo.begin_transition(7001, 0, op1, odata.OWNER_TYPE_HUB, a, 0, SKEW)
    await repo.admit(7001, 1, op1, a)
    await repo.renew_instance_lease(a, 20)  # 旧 hub 租约还很长

    rec = await repo.begin_transition(7001, 1, op2, odata.OWNER_TYPE_HUB, b, 0, SKEW)
    assert rec.admit_not_before_ms <= odata.now_ms() + 50, "HUB 前任把屏障推迟了"
    admitted, _ = await repo.admit(7001, 2, op2, b)
    assert admitted.phase == odata.OWNER_PHASE_ADMITTED


# ── ★ Admit 幂等与 exact 匹配 ───────────────────────────────────────────────


async def test_admit_replay_is_idempotent(repo) -> None:
    """★ ACK 丢失后重放必须返回**同一结果**,不能再分配或创建第二 owner(§9.23)。"""
    op, target = _op(), _target()
    await repo.begin_transition(8001, 0, op, odata.OWNER_TYPE_HUB, target, 0, SKEW)
    first, _ = await repo.admit(8001, 1, op, target)
    for _ in range(5):
        again, retry = await repo.admit(8001, 1, op, target)
        assert again.owner_epoch == first.owner_epoch
        assert again.phase == odata.OWNER_PHASE_ADMITTED
        assert retry == 0


@pytest.mark.parametrize(
    ("bad_epoch", "bad_op", "bad_target", "expect_reason"),
    [
        (99, False, False, "owner_epoch_mismatch"),
        (None, True, False, "operation_id_mismatch"),
        (None, False, True, "target_instance_mismatch"),
    ],
)
async def test_admit_rejects_any_identity_mismatch(
    repo, bad_epoch, bad_op, bad_target, expect_reason
) -> None:
    """★ epoch / operation / exact 实例任一不符 → fail-closed 拒。"""
    op, target = _op(), _target()
    await repo.begin_transition(9001, 0, op, odata.OWNER_TYPE_HUB, target, 0, SKEW)
    use_epoch = bad_epoch if bad_epoch is not None else 1
    use_op = _op() if bad_op else op
    use_target = _target(instance_uid="uid-OTHER") if bad_target else target
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.admit(9001, use_epoch, use_op, use_target)
    assert exc.value.code == errcode.ErrOwnerIdentityMismatch


# ── ★ 租约只前进 ────────────────────────────────────────────────────────────


async def test_lease_deadline_only_advances(repo) -> None:
    """★ deadline **只前进**。

    允许回退等于让一次迟到的短续租**缩短**已经算进屏障的截止时刻,
    新 owner 就可能提前开始可玩 —— 脑裂。
    """
    target = _target()
    long_deadline = await repo.renew_instance_lease(target, 20)
    short_deadline = await repo.renew_instance_lease(target, 1)  # 迟到的短续租
    assert short_deadline == long_deadline, "短续租把 deadline 拉回去了"


async def test_lease_epoch_mismatch_rejected(repo) -> None:
    """实例纪元不符拒(只对双方都非零且不同)。"""
    await repo.renew_instance_lease(_target(instance_epoch=3), 10)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.renew_instance_lease(_target(instance_epoch=5), 10)
    assert exc.value.code == errcode.ErrOwnerLeaseRegressed


async def test_lease_zero_epoch_is_allowed(repo) -> None:
    """hub 凭据不携带实例纪元 → 0 放行(uid 全局唯一已足够)。"""
    await repo.renew_instance_lease(_target(instance_epoch=3), 10)
    assert await repo.renew_instance_lease(_target(instance_epoch=0), 10) > 0


# ── ★ Release ───────────────────────────────────────────────────────────────


async def test_release_keeps_epoch_and_source_revision(repo) -> None:
    """★ Release **不清 epoch、不清 hub_source_revision**。

    清 epoch → 下一次 Begin 的 expect_epoch=0 能通过,旧写者迟到 CAS 又能命中。
    清 revision → 「打完一局回大厅」就把门重新对 legacy(0)敞开,
                  滚动窗口里的旧写者随即又能写进来(INC-20260818-003)。
    """
    op, target = _op(), _target()
    await repo.begin_transition(
        10001, 0, op, odata.OWNER_TYPE_HUB, target, source_revision=77, skew_margin_seconds=SKEW
    )
    await repo.admit(10001, 1, op, target)
    released = await repo.release(10001, 1, op)
    assert released.owner_epoch == 1, "Release 把 epoch 清零了"
    assert released.hub_source_revision == 77, "Release 把来源版本清零了"
    assert released.owner_type == odata.OWNER_TYPE_NONE
    persisted = await repo.query(10001)
    assert persisted.owner_epoch == 1
    assert persisted.hub_source_revision == 77


async def test_stale_release_is_noop_not_error(repo) -> None:
    """★ 迟到 Release 幂等 no-op,**不报错**。

    迟到登出是正常现象,报错会让调用方无谓重试;更重要的是它绝不能删掉新会话的记录。
    """
    op1, target = _op(), _target()
    await repo.begin_transition(11001, 0, op1, odata.OWNER_TYPE_HUB, target, 0, SKEW)
    # 迟到的 Release 拿着旧 epoch / 别的 operation
    rec = await repo.release(11001, 999, _op())
    assert rec.owner_epoch == 1, "迟到 Release 影响了当前记录"
    assert rec.owner_type == odata.OWNER_TYPE_HUB


# ── ★ hub_source_revision 单调 ──────────────────────────────────────────────


async def test_source_revision_advances(repo) -> None:
    op1, op2 = _op(), _op()
    a, b = _target(instance_uid="uid-A"), _target(instance_uid="uid-B", assignment_or_allocation_id="assign-2")
    await repo.begin_transition(12001, 0, op1, odata.OWNER_TYPE_HUB, a, 10, SKEW)
    rec = await repo.begin_transition(12001, 1, op2, odata.OWNER_TYPE_HUB, b, 20, SKEW)
    assert rec.hub_source_revision == 20


async def test_stale_source_revision_rejected(repo) -> None:
    """★ 来源版本倒退 → 拒。

    事故反例里旧 binary 恰好能拿到**合法的** expect_epoch(它先 Begin 后 CAS),
    所以只靠 epoch 挡不住它;能挡住的只有来源版本。
    """
    op1, op2 = _op(), _op()
    a = _target(instance_uid="uid-A")
    b = _target(instance_uid="uid-B", assignment_or_allocation_id="assign-2")
    await repo.begin_transition(13001, 0, op1, odata.OWNER_TYPE_HUB, a, 50, SKEW)
    with pytest.raises(errcode.PandoraError) as exc:
        await repo.begin_transition(13001, 1, op2, odata.OWNER_TYPE_HUB, b, 20, SKEW)
    assert exc.value.code == errcode.ErrOwnerSourceRevisionStale


async def test_legacy_zero_revision_is_allowed(repo) -> None:
    """source_revision=0 = 调用方尚未滚上本协议(兼容窗)→ 放行,且不覆盖高水位。"""
    op1, op2 = _op(), _op()
    a = _target(instance_uid="uid-A")
    b = _target(instance_uid="uid-B", assignment_or_allocation_id="assign-2")
    await repo.begin_transition(14001, 0, op1, odata.OWNER_TYPE_HUB, a, 60, SKEW)
    rec = await repo.begin_transition(14001, 1, op2, odata.OWNER_TYPE_HUB, b, 0, SKEW)
    assert rec.hub_source_revision == 60, "legacy 写者把高水位抹掉了"


# ── 审计 ────────────────────────────────────────────────────────────────────


async def test_transition_log_written_for_each_op(repo, pool) -> None:
    op, target = _op(), _target()
    await repo.begin_transition(15001, 0, op, odata.OWNER_TYPE_HUB, target, 0, SKEW)
    await repo.admit(15001, 1, op, target)
    await repo.release(15001, 1, op)
    async with pool.acquire() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT op, detail FROM owner_transition_log WHERE player_id=%s ORDER BY id",
            (15001,),
        )
        rows = await cur.fetchall()
    assert [r[0] for r in rows] == [
        odata.TRANSITION_OP_BEGIN,
        odata.TRANSITION_OP_ADMIT,
        odata.TRANSITION_OP_RELEASE,
    ]
    assert "uid=uid-A" in rows[0][1], "审计没带 exact 实例身份"


async def test_oversized_detail_does_not_fail_transition(repo) -> None:
    """★ 超长审计字段**不能**让一次本该成功的迁移失败。

    严格模式下超长是 Error 1406 而非截断 —— 所以必须在应用侧钳到列宽。
    这条用真库跑才有意义:非严格模式下会静默截断,断言会假通过。
    """
    op = _op()
    huge = _target(
        pod_name="p" * 120,
        assignment_or_allocation_id="a" * 120,
        instance_uid="u" * 120,
    )
    rec = await repo.begin_transition(16001, 0, op, odata.OWNER_TYPE_HUB, huge, 0, SKEW)
    assert rec.owner_epoch == 1
