"""临时群数据层 —— 对应 Go 侧 internal/data/group_repo.go。

★ 名额占用用的是一个**两把锁 + 对账**的模式,三条都不能改:

════ ① 固定锁顺序:计数行 → 明细范围 ════

    reserve 先锁 `player_group_counts` 的单行,再锁 `chat_group_members` 的明细范围。
    调用方涉及多个玩家时**必须按 player_id 升序**依次取 —— 顺序不固定就是环。

    两把锁各有分工:
      - 计数行:TiDB 没有 gap 锁,`COUNT ... FOR UPDATE` 在零行时一把锁都不加,
        所以必须有一行**确定存在**的行可锁,它才是新版之间的串行化点。
      - 明细范围:复用 MySQL 旧版 `COUNT ... FOR UPDATE` 的索引锁,
        让旧/新混跑时新版必看到旧写提交后的明细。

════ ② 明细 COUNT 是权威,计数行只是串行化点 + 读优化 ════

    判定用的是 `SELECT COUNT(*) FROM chat_group_members`,不是计数行的值。
    计数行写错 / 被旧 Pod 留脏都不会让上限判错。

════ ③ 对账用**绝对值回写**,不是 ±1 ════

    `UPDATE ... SET group_count = <实际值>` 而不是 `group_count + 1`。
    增量写在计数已经脏掉时会把错误永久累积;绝对值回写让脏计数**下次操作自愈**。
    release 同理:重算而不是 -1(可能被旧 Pod 留脏)。
"""

from __future__ import annotations

import contextlib

from pandorapy import errcode, mysqlx
from pandorapy import log as plog

# 事务死锁重试次数。兜底二级索引间隙锁等偶发 1213/1205;fn 必须可安全重放。
TX_MAX_RETRIES = 3


class MySQLGroupRepo:
    """临时群数据层。"""

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001
        self._pool = pool

    # ── 事务封装(带死锁重试)───────────────────────────────────────────────

    @contextlib.asynccontextmanager
    async def _tx_once(self):
        async with self._pool.acquire() as conn:
            await conn.begin()
            try:
                async with conn.cursor() as cur:
                    yield cur
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

    async def _run_tx(self, fn):  # noqa: ANN001
        """跑一个事务,遇可重试错误(死锁 / 锁等待超时)重试。

        ⚠️ fn **必须可安全重放** —— 它会被整体重跑,不能有事务外的副作用
        (发 kafka、调下游 RPC)。
        """
        last: BaseException | None = None
        for _ in range(TX_MAX_RETRIES + 1):
            try:
                async with self._tx_once() as cur:
                    return await fn(cur)
            except Exception as exc:  # noqa: BLE001
                if not _is_retryable(exc):
                    raise
                last = exc
                plog.get().debug("group_tx_retry", err=str(exc))
        assert last is not None
        raise last

    # ── ★ 名额占用 ─────────────────────────────────────────────────────────

    @staticmethod
    async def _reconcile_player_group_count(cur, player_id: int) -> int:  # noqa: ANN001
        """取计数行锁 → 用明细锁定读算出**实际**群数 → 绝对值回写。返回实际值。

        惰性建行:`INSERT ... ON DUPLICATE KEY UPDATE player_id = player_id` 是
        "存在就锁、不存在就建并锁"的标准写法 —— 保证已有行和首次创建都成为
        本版本的**唯一串行化点**(普通 SELECT FOR UPDATE 在行不存在时,TiDB 一把锁都不加)。
        """
        await cur.execute(
            "INSERT INTO player_group_counts (player_id, group_count) VALUES (%s, 0) "
            "ON DUPLICATE KEY UPDATE player_id = player_id",
            (player_id,),
        )
        # 第一把锁:计数行(TiDB 新/新写的串行化点)
        await cur.execute(
            "SELECT group_count FROM player_group_counts WHERE player_id = %s FOR UPDATE",
            (player_id,),
        )
        row = await cur.fetchone()
        stored = int(row[0]) if row else 0

        # 第二把锁:明细范围(与 MySQL 旧版的 COUNT..FOR UPDATE 互斥)
        # ★ 这个 COUNT 才是**权威**,计数行只是串行化点 + 读优化值。
        await cur.execute(
            "SELECT COUNT(*) FROM chat_group_members WHERE player_id = %s FOR UPDATE",
            (player_id,),
        )
        row = await cur.fetchone()
        actual = int(row[0]) if row else 0

        if stored != actual:
            # ★ **绝对值回写**,不是 ±1 —— 增量写会把已经脏掉的计数永久累积。
            await cur.execute(
                "UPDATE player_group_counts SET group_count = %s WHERE player_id = %s",
                (actual, player_id),
            )
            plog.get().debug(
                "group_count_reconciled",
                player_id=player_id,
                stored=stored,
                actual=actual,
            )
        return actual

    async def _reserve_player_group_slot(
        self, cur, player_id: int, max_groups: int
    ) -> None:  # noqa: ANN001
        """为玩家占用一个「所在群」名额(§9.18 写入侧上限)。"""
        count = await self._reconcile_player_group_count(cur, player_id)
        if max_groups > 0 and count >= max_groups:
            raise errcode.PandoraError(
                errcode.ErrGroupJoinLimit,
                "group join limit reached for %d (max %d)",
                player_id,
                max_groups,
            )
        await cur.execute(
            "UPDATE player_group_counts SET group_count = %s WHERE player_id = %s",
            (count + 1, player_id),
        )

    async def _release_player_group_slot(self, cur, player_id: int) -> None:  # noqa: ANN001
        """释放名额。

        ★ **重算而不是 -1** —— 计数可能被旧 Pod 留脏,-1 会把脏值继续带下去。
        调用方须已按 count-row → detail-range 顺序锁定并完成明细 DELETE。
        """
        await self._reconcile_player_group_count(cur, player_id)

    # ── 建群 / 加人 / 退群 ─────────────────────────────────────────────────

    async def create_group(
        self,
        group_id: int,
        owner_id: int,
        member_ids: list[int],
        *,
        max_members: int,
        max_groups_per_player: int,
    ) -> None:
        """建群。owner 也算成员。

        ★ 多玩家取锁**必须按 player_id 升序** —— 两个并发建群若顺序相反就是环。
        """
        members = sorted({owner_id, *member_ids})
        if max_members > 0 and len(members) > max_members:
            raise errcode.PandoraError(
                errcode.ErrGroupFull,
                "group %d would have %d members (max %d)",
                group_id,
                len(members),
                max_members,
            )

        async def body(cur):  # noqa: ANN001
            await cur.execute(
                "INSERT INTO chat_groups (group_id, owner_id, member_count) VALUES (%s, %s, %s)",
                (group_id, owner_id, len(members)),
            )
            # ★ 升序取每个玩家的名额锁
            for pid in members:
                await self._reserve_player_group_slot(cur, pid, max_groups_per_player)
            for pid in members:
                await cur.execute(
                    "INSERT INTO chat_group_members (group_id, player_id, role) "
                    "VALUES (%s, %s, %s)",
                    (group_id, pid, 1 if pid == owner_id else 0),
                )
            # 明细写完后重算一遍,让计数行与明细一致
            for pid in members:
                await self._reconcile_player_group_count(cur, pid)

        await self._run_tx(body)

    async def add_member(
        self, group_id: int, player_id: int, *, max_members: int, max_groups_per_player: int
    ) -> None:
        """加人。群成员上限与玩家所在群上限**都要过**。"""

        async def body(cur):  # noqa: ANN001
            # 群行是每群操作的第一把锁(串行化点)
            await cur.execute(
                "SELECT member_count FROM chat_groups WHERE group_id = %s FOR UPDATE",
                (group_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGroupNotFound, "group %d not found", group_id
                )
            member_count = int(row[0])
            if max_members > 0 and member_count >= max_members:
                raise errcode.PandoraError(
                    errcode.ErrGroupFull,
                    "group %d is full (max %d)",
                    group_id,
                    max_members,
                )

            await self._reserve_player_group_slot(cur, player_id, max_groups_per_player)
            try:
                await cur.execute(
                    "INSERT INTO chat_group_members (group_id, player_id, role) "
                    "VALUES (%s, %s, 0)",
                    (group_id, player_id),
                )
            except Exception as exc:  # noqa: BLE001
                if mysqlx.is_duplicate_entry(exc):
                    # 已在群里 —— 幂等 no-op,但要把刚占的名额还回去(重算即可)
                    await self._reconcile_player_group_count(cur, player_id)
                    return
                raise
            await cur.execute(
                "UPDATE chat_groups SET member_count = member_count + 1 WHERE group_id = %s",
                (group_id,),
            )
            await self._reconcile_player_group_count(cur, player_id)

        await self._run_tx(body)

    async def remove_member(self, group_id: int, player_id: int) -> None:
        async def body(cur):  # noqa: ANN001
            await cur.execute(
                "SELECT owner_id FROM chat_groups WHERE group_id = %s FOR UPDATE",
                (group_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrGroupNotFound, "group %d not found", group_id
                )
            await cur.execute(
                "DELETE FROM chat_group_members WHERE group_id = %s AND player_id = %s",
                (group_id, player_id),
            )
            if cur.rowcount:
                await cur.execute(
                    "UPDATE chat_groups SET member_count = member_count - 1 "
                    "WHERE group_id = %s AND member_count > 0",
                    (group_id,),
                )
            # 无论是否真删都重算(幂等)
            await self._release_player_group_slot(cur, player_id)

        await self._run_tx(body)

    async def list_my_groups(self, player_id: int, limit: int = 200) -> list[int]:
        """§9.18 读取侧:SQL LIMIT 兜底(写入侧上限已把规模钉在几十)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT group_id FROM chat_group_members WHERE player_id = %s "
                "ORDER BY group_id LIMIT %s",
                (player_id, limit),
            )
            return [int(r[0]) for r in await cur.fetchall()]

    async def player_group_count(self, player_id: int) -> int:
        """读计数行(只读路径,不加锁)。仅供展示 —— 权威始终是明细 COUNT。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT group_count FROM player_group_counts WHERE player_id = %s",
                (player_id,),
            )
            row = await cur.fetchone()
            return int(row[0]) if row else 0


def _is_retryable(exc: BaseException) -> bool:
    """1213 死锁 / 1205 锁等待超时 → 可重试。"""
    args = getattr(exc, "args", ())
    return bool(args) and args[0] in (mysqlx.ER_LOCK_DEADLOCK, 1205)
