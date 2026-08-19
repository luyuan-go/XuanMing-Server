"""friend 数据层 —— 对应 Go 侧 internal/data/friend_repo.go。

★ 这个文件有两条**正确性要求(不是调优)**,都是真实事故的修复,移植时一个都不能丢:

════ ① 写事务必须显式 READ COMMITTED ════

    本域所有权威判定读都是 `SELECT ... FOR UPDATE`,而这些探针**绝大多数查不到行**
    (首次申请 / 首次拉黑)。RR 下未命中的锁定读锁的不是"某一行"而是**该键所在的间隙**:

        TRX A 持 uk_requester_target 某间隙的 X 锁,等在同一间隙插入;
        TRX B 持同一间隙的 X 锁,也等在同一间隙插入;   → 1213 死锁

    间隙锁彼此相容,所以 N 个事务都能拿到;冲突发生在随后的 insert intention。
    真 MySQL 8.4 实测:16 个并发申请(**互不相同的 requester 与 target**,没有任何
    共享行)必炸。**这不是锁序问题**,重排守卫顺序解决不了 —— 它们根本没有共享的守卫行。

    降到 RC 安全的理由:本域的并发正确性从设计之初就**不依赖 gap 锁** ——
    限额权威来自守卫行 + 守卫锁内的锁定读,唯一性来自唯一键,两者在 RC 下都成立。
    RR 的 gap 锁在 MySQL 侧是纯多余的副作用,只贡献死锁。

════ ② 锁序:pair 守卫 → player 守卫 → 探针 ════

    player 守卫必须在**任何锁定读之前**取。原实现把它放在限额校验里(即三条 FOR UPDATE
    探针之后),依据是"本事务持有的行锁只属于本 pair" —— **这条前提不成立**:
    未命中的 FOR UPDATE 锁的是间隙,N 个不同 requester 指向同一 target 时全部落在
    同一个 supremum 间隙里。InnoDB 死锁日志逐字印证了这个环:

        TRX A 持 friend_requests 间隙的 X 锁,等 guards 主键行;
        TRX B 持 guards 主键行,等 friend_requests 同一间隙的 insert intention。

    修法:把守卫提到探针之前,所有间隙锁都在守卫的串行化**内部**取得。

    ⚠️ 守卫**无条件取**,不受限额开关门控 —— 间隙锁的暴露与限额是否开启无关,
    关掉限额不该把锁序纪律一起关掉。

    ⚠️ 这个死锁**只在 MySQL 上炸**(TiDB 无 gap 锁),所以只跑 TiDB 会一直是绿的。
"""

from __future__ import annotations

import contextlib

from pandora.friend.v1 import friend_pb2

from pandorapy import errcode, mysqlx

# 好友 / 申请 / 黑名单列表单次返回的防御性 SQL 上限(§9.18 读取侧兜底)。
# 写入侧上限默认 200,正常列表远低于此;这里取更宽松的硬上限,仅防历史脏数据
# / 极端场景下的无界扫描。
LIST_READ_HARD_LIMIT = 1000

REQUEST_STATUS_PENDING = 1
REQUEST_STATUS_ACCEPTED = 2
REQUEST_STATUS_REJECTED = 3

# ★ 见文件头 ①。前置:binlog_format=ROW(MySQL 8.4 默认)。
WRITE_TX_ISOLATION = "READ COMMITTED"


class MySQLFriendRepo:
    """基于 asyncmy / aiomysql 的 friend 数据层。"""

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001
        self._pool = pool

    @contextlib.asynccontextmanager
    async def _write_tx(self):
        """写事务:**显式 RC**。见文件头 ① —— 这是正确性要求不是调优。"""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(f"SET TRANSACTION ISOLATION LEVEL {WRITE_TX_ISOLATION}")
            await conn.begin()
            try:
                async with conn.cursor() as cur:
                    yield cur
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

    # ── 守卫行 ───────────────────────────────────────────────────────────────

    @staticmethod
    async def _acquire_pair_guard(cur, a: int, b: int) -> None:  # noqa: ANN001
        """取"这一对玩家"的守卫行锁 —— 与同对的 Block/Accept 串行化。

        `INSERT ... ON DUPLICATE KEY UPDATE lo_id = lo_id` 是"存在就锁、不存在就建并锁"
        的标准写法:它在两种情况下都取得该主键行的排他锁,而普通 `SELECT FOR UPDATE`
        在行不存在时(TiDB)一把锁都不加。

        ★ lo/hi 归一化:同一对玩家无论谁发起,必须落到**同一行**,否则守卫形同虚设。
        """
        lo, hi = (a, b) if a <= b else (b, a)
        await cur.execute(
            "INSERT INTO friend_pair_guards (lo_id, hi_id) VALUES (%s, %s) "
            "ON DUPLICATE KEY UPDATE lo_id = lo_id",
            (lo, hi),
        )

    @staticmethod
    async def _acquire_player_guard(cur, player_id: int) -> None:  # noqa: ANN001
        """取单玩家守卫行锁(该玩家限额域的写串行化)。"""
        await cur.execute(
            "INSERT INTO friend_player_guards (player_id) VALUES (%s) "
            "ON DUPLICATE KEY UPDATE player_id = player_id",
            (player_id,),
        )

    # ── CreateRequest ────────────────────────────────────────────────────────

    async def create_request(
        self, request_id: int, requester_id: int, target_id: int, max_incoming: int
    ) -> tuple[int, bool]:
        """创建好友申请。返回 (request_id, 是否新建)。

        ★ 顺序是契约(见文件头 ②),**不要重排**:
            1. pair 守卫    ← 与同对的 Block/Accept 串行化
            2. player 守卫  ← 必须在任何锁定读之前(死锁根因)
            3. block 探针   ← 以下三条都是锁定读
            4. friendship 探针
            5. 既有请求行
            6. 限额校验 + INSERT
        """
        async with self._write_tx() as cur:
            # 1 & 2:两把守卫,无条件取。
            await self._acquire_pair_guard(cur, requester_id, target_id)
            await self._acquire_player_guard(cur, target_id)

            # 3. 双向拉黑探针。任一方向拉黑都不允许申请。
            await cur.execute(
                "SELECT 1 FROM blocks "
                "WHERE (player_id = %s AND blocked_id = %s) "
                "   OR (player_id = %s AND blocked_id = %s) LIMIT 1 FOR UPDATE",
                (requester_id, target_id, target_id, requester_id),
            )
            if await cur.fetchone() is not None:
                raise errcode.PandoraError(
                    errcode.ErrFriendBlocked,
                    "blocked between %d and %d",
                    requester_id,
                    target_id,
                )

            # 4. 已是好友探针。
            await cur.execute(
                "SELECT 1 FROM friendships WHERE player_id = %s AND friend_id = %s "
                "LIMIT 1 FOR UPDATE",
                (requester_id, target_id),
            )
            if await cur.fetchone() is not None:
                raise errcode.PandoraError(
                    errcode.ErrFriendAlreadyAdded,
                    "already friends: %d-%d",
                    requester_id,
                    target_id,
                )

            # 5. 既有请求行(锁定读)。
            await cur.execute(
                "SELECT request_id, status FROM friend_requests "
                "WHERE requester_id = %s AND target_id = %s FOR UPDATE",
                (requester_id, target_id),
            )
            existing = await cur.fetchone()

            if existing is None:
                # 6. 新增前先校验 target 收件箱未满(§9.18)。
                await self._check_incoming_limit(cur, target_id, max_incoming)
                await cur.execute(
                    "INSERT INTO friend_requests (request_id, requester_id, target_id, status) "
                    "VALUES (%s, %s, %s, %s)",
                    (request_id, requester_id, target_id, REQUEST_STATUS_PENDING),
                )
                return request_id, True

            existing_id, status = int(existing[0]), int(existing[1])
            if status == REQUEST_STATUS_PENDING:
                # 重复申请同一目标:幂等返回既有 pending,**不占新名额**。
                return existing_id, False

            # 历史请求已被拒/已接受 → 复活成 pending,同样要过限额。
            await self._check_incoming_limit(cur, target_id, max_incoming)
            await cur.execute(
                "UPDATE friend_requests SET status = %s WHERE request_id = %s",
                (REQUEST_STATUS_PENDING, existing_id),
            )
            return existing_id, True

    @staticmethod
    async def _check_incoming_limit(cur, target_id: int, max_incoming: int) -> None:  # noqa: ANN001
        """校验 target 的「收到的待处理申请」上限(§9.18)。

        前置条件:调用方**已在任何锁定读之前**取得 target 的 player 守卫。
        这里刻意不再自取 —— 2026-08-11 的 1213 死锁根因正是"守卫在此处才取",
        那时三条 FOR UPDATE 探针的间隙锁已经拿在手里,取得再早也来不及。

        COUNT 用锁定读拿当前读:普通 COUNT 在 RR 陈旧快照下会漏计守卫等待期间提交的
        pending(R9 复审 P1)。
        """
        if max_incoming <= 0:
            return
        await cur.execute(
            "SELECT COUNT(*) FROM friend_requests WHERE target_id = %s AND status = %s "
            "FOR UPDATE",
            (target_id, REQUEST_STATUS_PENDING),
        )
        row = await cur.fetchone()
        count = int(row[0]) if row else 0
        if count >= max_incoming:
            raise errcode.PandoraError(
                errcode.ErrFriendRequestLimit,
                "incoming friend request limit reached for %d (max %d)",
                target_id,
                max_incoming,
            )

    # ── AcceptRequest ────────────────────────────────────────────────────────

    async def accept_request(
        self, request_id: int, actor_id: int, max_friends: int
    ) -> tuple[int, int]:
        """接受好友申请。返回 (requester_id, target_id)。

        锁序与 create_request 一致(pair → player),不引入跨路径反序。
        ★ 双方的好友数上限**都要校验** —— 只校验一方会让另一方越界。
        """
        async with self._write_tx() as cur:
            await cur.execute(
                "SELECT requester_id, target_id, status FROM friend_requests "
                "WHERE request_id = %s FOR UPDATE",
                (request_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound, "request %d not found", request_id
                )
            requester_id, target_id, status = int(row[0]), int(row[1]), int(row[2])

            if actor_id != target_id:
                # ★ 与 Go 侧一致地返回 ErrFriendNotFound 而**不是** ErrUnauthorized:
                # 后者等于告诉调用方「这条申请确实存在」,是信息泄露 ——
                # 非 target 无从区分"没这条申请"和"有但不是给你的"。
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound,
                    "request %d not for %d",
                    request_id,
                    actor_id,
                )
            if status != REQUEST_STATUS_PENDING:
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound,
                    "request %d is not pending (status=%d)",
                    request_id,
                    status,
                )

            await self._acquire_pair_guard(cur, requester_id, target_id)
            # 两个 player 守卫按 **ID 升序**取 —— 固定全局顺序才不会与另一个
            # 反向 Accept 形成环。
            for pid in sorted((requester_id, target_id)):
                await self._acquire_player_guard(cur, pid)

            for pid in sorted((requester_id, target_id)):
                await self._check_friend_limit(cur, pid, max_friends)

            await cur.execute(
                "UPDATE friend_requests SET status = %s WHERE request_id = %s",
                (REQUEST_STATUS_ACCEPTED, request_id),
            )
            # 好友关系双向各一行(查询侧只需单向索引)。
            await cur.execute(
                "INSERT IGNORE INTO friendships (player_id, friend_id) VALUES (%s, %s), (%s, %s)",
                (requester_id, target_id, target_id, requester_id),
            )
            return requester_id, target_id

    @staticmethod
    async def _check_friend_limit(cur, player_id: int, max_friends: int) -> None:  # noqa: ANN001
        if max_friends <= 0:
            return
        await cur.execute(
            "SELECT COUNT(*) FROM friendships WHERE player_id = %s FOR UPDATE", (player_id,)
        )
        row = await cur.fetchone()
        if row and int(row[0]) >= max_friends:
            raise errcode.PandoraError(
                errcode.ErrFriendLimit,
                "friend limit reached for %d (max %d)",
                player_id,
                max_friends,
            )

    # ── RejectRequest ────────────────────────────────────────────────────────

    async def reject_request(self, request_id: int, actor_id: int) -> tuple[int, int]:
        async with self._write_tx() as cur:
            await cur.execute(
                "SELECT requester_id, target_id, status FROM friend_requests "
                "WHERE request_id = %s FOR UPDATE",
                (request_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound, "request %d not found", request_id
                )
            requester_id, target_id, status = int(row[0]), int(row[1]), int(row[2])
            if actor_id != target_id:
                # ★ 与 Go 侧一致地返回 ErrFriendNotFound 而**不是** ErrUnauthorized:
                # 后者等于告诉调用方「这条申请确实存在」,是信息泄露 ——
                # 非 target 无从区分"没这条申请"和"有但不是给你的"。
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound,
                    "request %d not for %d",
                    request_id,
                    actor_id,
                )
            if status != REQUEST_STATUS_PENDING:
                raise errcode.PandoraError(
                    errcode.ErrFriendNotFound,
                    "request %d is not pending",
                    request_id,
                )
            await cur.execute(
                "UPDATE friend_requests SET status = %s WHERE request_id = %s",
                (REQUEST_STATUS_REJECTED, request_id),
            )
            return requester_id, target_id

    # ── Block ────────────────────────────────────────────────────────────────

    async def block(self, player_id: int, blocked_id: int, max_blocks: int) -> None:
        """拉黑。★ 同时**删除既有好友关系与 pending 申请** —— 拉黑必须是彻底的。

        锁序与 create_request 一致(pair → player)。
        """
        async with self._write_tx() as cur:
            await self._acquire_pair_guard(cur, player_id, blocked_id)
            await self._acquire_player_guard(cur, player_id)

            if max_blocks > 0:
                await cur.execute(
                    "SELECT COUNT(*) FROM blocks WHERE player_id = %s FOR UPDATE",
                    (player_id,),
                )
                row = await cur.fetchone()
                if row and int(row[0]) >= max_blocks:
                    raise errcode.PandoraError(
                        errcode.ErrFriendBlockLimit,
                        "block limit reached for %d (max %d)",
                        player_id,
                        max_blocks,
                    )

            await cur.execute(
                "INSERT IGNORE INTO blocks (player_id, blocked_id) VALUES (%s, %s)",
                (player_id, blocked_id),
            )
            # 拉黑即解除关系:双向删好友 + 作废两个方向的 pending 申请。
            await cur.execute(
                "DELETE FROM friendships WHERE (player_id = %s AND friend_id = %s) "
                "OR (player_id = %s AND friend_id = %s)",
                (player_id, blocked_id, blocked_id, player_id),
            )
            await cur.execute(
                "UPDATE friend_requests SET status = %s "
                "WHERE status = %s AND ((requester_id = %s AND target_id = %s) "
                "OR (requester_id = %s AND target_id = %s))",
                (
                    REQUEST_STATUS_REJECTED,
                    REQUEST_STATUS_PENDING,
                    player_id,
                    blocked_id,
                    blocked_id,
                    player_id,
                ),
            )

    async def unblock(self, player_id: int, blocked_id: int) -> None:
        async with self._write_tx() as cur:
            await cur.execute(
                "DELETE FROM blocks WHERE player_id = %s AND blocked_id = %s",
                (player_id, blocked_id),
            )

    # ── 读 ───────────────────────────────────────────────────────────────────

    async def list_friends(self, player_id: int, limit: int = 0) -> list[int]:
        """★ SQL LIMIT 兜底(§9.18 读取侧单次返回上限)。"""
        capped = min(limit, LIST_READ_HARD_LIMIT) if limit > 0 else LIST_READ_HARD_LIMIT
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT friend_id FROM friendships WHERE player_id = %s "
                "ORDER BY friend_id LIMIT %s",
                (player_id, capped),
            )
            return [int(r[0]) for r in await cur.fetchall()]

    async def list_incoming_requests(self, player_id: int, limit: int = 0) -> list[tuple]:
        capped = min(limit, LIST_READ_HARD_LIMIT) if limit > 0 else LIST_READ_HARD_LIMIT
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT request_id, requester_id FROM friend_requests "
                "WHERE target_id = %s AND status = %s ORDER BY request_id LIMIT %s",
                (player_id, REQUEST_STATUS_PENDING, capped),
            )
            return [(int(r[0]), int(r[1])) for r in await cur.fetchall()]

    async def list_blocks(self, player_id: int, limit: int = 0) -> list[int]:
        capped = min(limit, LIST_READ_HARD_LIMIT) if limit > 0 else LIST_READ_HARD_LIMIT
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT blocked_id FROM blocks WHERE player_id = %s ORDER BY blocked_id LIMIT %s",
                (player_id, capped),
            )
            return [int(r[0]) for r in await cur.fetchall()]


__all__ = ["MySQLFriendRepo", "LIST_READ_HARD_LIMIT", "WRITE_TX_ISOLATION", "mysqlx"]
