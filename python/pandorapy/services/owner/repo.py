"""owner 权威 MySQL/TiDB 数据层 —— 对应 Go 侧 internal/data/owner_repo.go。

★ 全仓正确性最敏感的一段代码。核心约束(§9.22):

    owner_epoch、lease 截止、admit_not_before、PENDING→ADMITTED
    **必须处于同一个线性一致事务域**。

    禁止把 owner 放 MySQL、准入 lease / 屏障放 Redis 或 etcd 后再跨存储"先查后写" ——
    那样 CAS 的线性化点与屏障计算不在同一致性域,脑裂窗口重新打开。

所以每个 transition 的形状固定为:

    BEGIN
      SELECT ... FROM owner_record WHERE player_id=? FOR UPDATE     ← 串行化锚点
      SELECT ... FROM ds_instance_lease WHERE instance_uid=? FOR UPDATE  ← 屏障取值
      (判定 / 计算 / CAS / 写审计)
    COMMIT

锁序固定 `owner_record → ds_instance_lease`(Renew 只锁 lease 行)—— 无环无死锁。

TiDB 安全:只锁**存在行** + 条件更新,不依赖间隙锁。
⚠️ TiDB 无 gap 锁,`FOR UPDATE` 在零行时**不加锁** —— 所以"记录不存在"的分支
不能靠 FOR UPDATE 互斥,必须靠主键 INSERT 的唯一键冲突来兜(见 _ensure_record)。
"""

from __future__ import annotations

import contextlib

from pandorapy import errcode, mysqlx
from pandorapy import log as plog
from pandorapy.services.owner import data as odata


class MySQLOwnerRepo:
    """基于 asyncmy / aiomysql 的 OwnerRepo。"""

    __slots__ = ("_pool",)

    def __init__(self, pool) -> None:  # noqa: ANN001
        self._pool = pool

    # ── 读 ───────────────────────────────────────────────────────────────────

    async def query(self, player_id: int) -> odata.OwnerRecord:
        """读当前记录(无行返回 epoch=0/none;附带派生 lease 截止)。

        ★ 调用方查询失败一律按 UNKNOWN 处理 —— 所以这里数据库出错必须**抛异常**,
        绝不能返回一条空记录。空记录的语义是"确实没有 owner",会让调用方放行第二个 owner。
        """
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(_SQL_SELECT_RECORD, (player_id,))
            row = await cur.fetchone()
            if row is None:
                return odata.OwnerRecord(player_id=player_id)
            rec = _row_to_record(row)
            lease = await _read_lease(cur, rec.target.instance_uid)
        return _with_lease(rec, lease)

    # ── BeginTransition ──────────────────────────────────────────────────────

    async def begin_transition(
        self,
        player_id: int,
        expect_epoch: int,
        operation_id: str,
        owner_type: int,
        target: odata.OwnerTarget,
        source_revision: int,
        skew_margin_seconds: int,
    ) -> odata.OwnerRecord:
        """CAS expect_epoch → epoch+1 / PENDING / newTarget。

        判定顺序(与 Go 侧一致,顺序本身是契约):
          1. 同 (player, exact target) 的重复投递 → **原样返回既有记录**(no-op 幂等)
          2. expect_epoch 不符 → ErrOwnerEpochConflict(**附当前记录**,调用方重查再决策)
          3. hub_source_revision 倒退 → ErrOwnerSourceRevisionStale
          4. 真实迁移 → epoch+1、算屏障、写记录 + 审计
        """
        now = odata.now_ms()
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await _ensure_record(cur, player_id)
                    await cur.execute(_SQL_SELECT_RECORD_FOR_UPDATE, (player_id,))
                    row = await cur.fetchone()
                    current = _row_to_record(row) if row else odata.OwnerRecord(player_id=player_id)

                    # ① 同 exact 实例的重复投递 → no-op,原样返回(含**原** operation_id)。
                    #    这正是"权威铸造 operation"能成立的前提:重复投递不换 operation。
                    if (
                        current.owner_epoch > 0
                        and current.owner_type == owner_type
                        and current.target == target
                    ):
                        lease = await _read_lease(cur, target.instance_uid)
                        await conn.commit()
                        return _with_lease(current, lease)

                    # ② epoch CAS。附当前记录 —— 调用方要靠它决定是重试还是放弃。
                    if current.owner_epoch != expect_epoch:
                        lease = await _read_lease(cur, current.target.instance_uid)
                        await conn.rollback()
                        raise _epoch_conflict(_with_lease(current, lease), expect_epoch)

                    # ③ Hub 来源版本单调(INC-20260818-003)。
                    #    只对 HUB 有意义;BATTLE 迁移忽略(注释见 data.OwnerRecord)。
                    #    source_revision=0 = 调用方尚未滚上本协议(兼容窗),放行。
                    next_revision = current.hub_source_revision
                    if owner_type == odata.OWNER_TYPE_HUB and source_revision > 0:
                        if source_revision < current.hub_source_revision:
                            await conn.rollback()
                            raise errcode.PandoraError(
                                errcode.ErrOwnerSourceRevisionStale,
                                "hub source revision %d < high-water %d (stale writer)",
                                source_revision,
                                current.hub_source_revision,
                            )
                        next_revision = source_revision

                    # ④ 屏障:同事务 FOR UPDATE 读**旧**实例租约,取 CAS 线性化点观察值。
                    #    读的是旧 target 的 uid —— 屏障问的是"旧 owner 什么时候一定停了"。
                    old_lease = 0
                    if current.target.instance_uid:
                        old_lease = await _read_lease_for_update(
                            cur, current.target.instance_uid
                        )
                    barrier = odata.compute_admit_not_before_ms(
                        current.owner_type, old_lease, now, skew_margin_seconds
                    )

                    new_epoch = current.owner_epoch + 1
                    await cur.execute(
                        _SQL_UPDATE_RECORD,
                        (
                            new_epoch,
                            owner_type,
                            odata.OWNER_PHASE_PENDING,
                            target.pod_name,
                            target.instance_uid,
                            target.instance_epoch,
                            target.assignment_or_allocation_id,
                            target.release_track,
                            operation_id,
                            barrier,
                            next_revision,
                            now,
                            player_id,
                            current.owner_epoch,  # 再次 CAS:防同事务外的并发写
                        ),
                    )
                    if cur.rowcount != 1:
                        await conn.rollback()
                        raise _epoch_conflict(current, expect_epoch)

                    await _write_log(
                        cur,
                        player_id,
                        current.owner_epoch,
                        new_epoch,
                        odata.TRANSITION_OP_BEGIN,
                        operation_id,
                        odata.transition_detail(target, barrier, current.target.pod_name),
                    )
                    new_lease = await _read_lease(cur, target.instance_uid)
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

        plog.get().info(
            "owner_transition_begin",
            player_id=player_id,
            from_epoch=current.owner_epoch,
            to_epoch=new_epoch,
            owner_type=owner_type,
            operation_id=operation_id,
            admit_not_before_ms=barrier,
            barrier_wait_ms=max(0, barrier - now),
        )
        return odata.OwnerRecord(
            player_id=player_id,
            owner_epoch=new_epoch,
            owner_type=owner_type,
            phase=odata.OWNER_PHASE_PENDING,
            target=target,
            operation_id=operation_id,
            admit_not_before_ms=barrier,
            lease_deadline_ms=new_lease,
            updated_at_ms=now,
            hub_source_revision=next_revision,
        )

    # ── Admit ────────────────────────────────────────────────────────────────

    async def admit(
        self, player_id: int, owner_epoch: int, operation_id: str, target: odata.OwnerTarget
    ) -> tuple[odata.OwnerRecord, int]:
        """屏障开 + epoch/operation/实例全等 → PENDING→ADMITTED。

        已 ADMITTED **幂等重放**(ACK 丢失后重放必须返回同一结果,不能再分配)。
        屏障未开 → ErrOwnerBarrierNotOpen(retry_after_ms > 0)。
        """
        now = odata.now_ms()
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(_SQL_SELECT_RECORD_FOR_UPDATE, (player_id,))
                    row = await cur.fetchone()
                    found = row is not None
                    current = _row_to_record(row) if found else odata.OwnerRecord(player_id=player_id)

                    reason = odata.admit_mismatch_reason(
                        found, current, owner_epoch, operation_id, target
                    )
                    if reason:
                        await conn.rollback()
                        plog.get().warning(
                            "owner_admit_rejected",
                            player_id=player_id,
                            reason=reason,
                            req_epoch=owner_epoch,
                            cur_epoch=current.owner_epoch,
                            req_operation_id=operation_id,
                            cur_operation_id=current.operation_id,
                            req_pod=target.pod_name,
                            cur_pod=current.target.pod_name,
                        )
                        raise errcode.PandoraError(
                            errcode.ErrOwnerIdentityMismatch, "admit rejected: %s", reason
                        )

                    # 已 ADMITTED:幂等重放,直接返回(不再写库、不重复审计)。
                    if current.phase == odata.OWNER_PHASE_ADMITTED:
                        lease = await _read_lease(cur, target.instance_uid)
                        await conn.commit()
                        return _with_lease(current, lease), 0

                    # ★ 屏障:now < admit_not_before 一律拒。这是核心时序不等式的执行点。
                    wait_ms = odata.barrier_wait_ms(current, now)
                    if wait_ms > 0:
                        await conn.rollback()
                        odata.log_barrier_not_open(player_id, current, wait_ms)
                        raise odata.barrier_not_open_error(wait_ms)

                    await cur.execute(
                        _SQL_ADMIT,
                        (odata.OWNER_PHASE_ADMITTED, now, player_id, owner_epoch, operation_id),
                    )
                    if cur.rowcount != 1:
                        await conn.rollback()
                        raise errcode.PandoraError(
                            errcode.ErrOwnerIdentityMismatch,
                            "admit lost race for player %d epoch %d",
                            player_id,
                            owner_epoch,
                        )
                    await _write_log(
                        cur,
                        player_id,
                        owner_epoch,
                        owner_epoch,
                        odata.TRANSITION_OP_ADMIT,
                        operation_id,
                        odata.transition_detail(target, current.admit_not_before_ms),
                    )
                    lease = await _read_lease(cur, target.instance_uid)
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

        plog.get().info(
            "owner_transition_admit",
            player_id=player_id,
            owner_epoch=owner_epoch,
            operation_id=operation_id,
        )
        admitted = odata.OwnerRecord(
            player_id=player_id,
            owner_epoch=owner_epoch,
            owner_type=current.owner_type,
            phase=odata.OWNER_PHASE_ADMITTED,
            target=target,
            operation_id=operation_id,
            admit_not_before_ms=current.admit_not_before_ms,
            lease_deadline_ms=lease,
            updated_at_ms=now,
            hub_source_revision=current.hub_source_revision,
        )
        return admitted, 0

    # ── RenewInstanceLease ───────────────────────────────────────────────────

    async def renew_instance_lease(
        self, target: odata.OwnerTarget, lease_seconds: int
    ) -> int:
        """实例租约续期。★ deadline **只前进**;实例纪元不符拒。

        只前进很重要:允许回退等于让一次迟到的短续租**缩短**已经算进屏障的截止时刻,
        新 owner 就可能提前开始可玩 —— 脑裂。
        """
        now = odata.now_ms()
        want = now + lease_seconds * 1000
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(_SQL_SELECT_LEASE_FOR_UPDATE, (target.instance_uid,))
                    row = await cur.fetchone()
                    if row is None:
                        await cur.execute(
                            _SQL_INSERT_LEASE,
                            (
                                target.instance_uid,
                                target.pod_name,
                                target.instance_epoch,
                                target.release_track,
                                want,
                                now,
                            ),
                        )
                        await conn.commit()
                        return want

                    cur_pod, cur_epoch, cur_deadline = row[1], int(row[2]), int(row[4])
                    # 纪元守卫:只对"双方都非零且不同"拒 —— hub 凭据不携带实例纪元。
                    if target.instance_epoch and cur_epoch and target.instance_epoch != cur_epoch:
                        await conn.rollback()
                        raise errcode.PandoraError(
                            errcode.ErrOwnerLeaseRegressed,
                            "instance epoch mismatch for %s: req=%d cur=%d",
                            target.instance_uid,
                            target.instance_epoch,
                            cur_epoch,
                        )
                    effective = max(cur_deadline, want)
                    await cur.execute(
                        _SQL_UPDATE_LEASE,
                        (
                            target.pod_name or cur_pod,
                            target.instance_epoch or cur_epoch,
                            target.release_track,
                            effective,
                            now,
                            target.instance_uid,
                        ),
                    )
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise
        return effective

    # ── Release ──────────────────────────────────────────────────────────────

    async def release(
        self, player_id: int, owner_epoch: int, operation_id: str
    ) -> odata.OwnerRecord:
        """epoch + operation 匹配 → 置 none(**epoch 保留**);不匹配(迟到)幂等 no-op。

        ★ 两个关键点:
          - epoch **不清零**:清零等于让下一次 Begin 的 expect_epoch=0 通过,
            旧写者的迟到 CAS 又能命中。
          - hub_source_revision **不动**:清零等于「打完一局回大厅」就把门重新对
            legacy(0)敞开,滚动窗口里的旧写者随即又能写进来(INC-20260818-003)。
        """
        now = odata.now_ms()
        async with self._pool.acquire() as conn:
            try:
                await conn.begin()
                async with conn.cursor() as cur:
                    await cur.execute(_SQL_SELECT_RECORD_FOR_UPDATE, (player_id,))
                    row = await cur.fetchone()
                    if row is None:
                        await conn.commit()
                        return odata.OwnerRecord(player_id=player_id)
                    current = _row_to_record(row)

                    # 迟到 Release:epoch 或 operation 不符 → 幂等 no-op 返回当前。
                    # **不能报错** —— 迟到登出是正常现象,报错会让调用方无谓重试。
                    if (
                        current.owner_epoch != owner_epoch
                        or current.operation_id != operation_id
                    ):
                        await conn.commit()
                        plog.get().debug(
                            "owner_release_stale_noop",
                            player_id=player_id,
                            req_epoch=owner_epoch,
                            cur_epoch=current.owner_epoch,
                        )
                        return current

                    await cur.execute(_SQL_RELEASE, (now, player_id, owner_epoch))
                    await _write_log(
                        cur,
                        player_id,
                        owner_epoch,
                        owner_epoch,
                        odata.TRANSITION_OP_RELEASE,
                        operation_id,
                        odata.transition_detail(current.target, 0),
                    )
                await conn.commit()
            except BaseException:
                with contextlib.suppress(Exception):
                    await conn.rollback()
                raise

        return odata.OwnerRecord(
            player_id=player_id,
            owner_epoch=owner_epoch,  # ★ epoch 保留
            owner_type=odata.OWNER_TYPE_NONE,
            phase=odata.OWNER_PHASE_NONE,
            operation_id="",
            updated_at_ms=now,
            hub_source_revision=current.hub_source_revision,  # ★ 永不清零
        )

    # ── 审计清理 ─────────────────────────────────────────────────────────────

    async def sweep_transition_log(self, retention_days: int, batch: int) -> int:
        """删除超保留期审计行(有界批量,§9.24)。"""
        async with self._pool.acquire() as conn, conn.cursor() as cur:
            await cur.execute(_SQL_SWEEP_LOG, (retention_days, batch))
            deleted = cur.rowcount or 0
            await conn.commit()
        return deleted


# ── SQL ──────────────────────────────────────────────────────────────────────

_RECORD_COLS = (
    "player_id, owner_epoch, owner_type, phase, pod_name, instance_uid, instance_epoch, "
    "assignment_or_allocation_id, release_track, operation_id, admit_not_before_ms, "
    "hub_source_revision, updated_at_ms"
)

_SQL_SELECT_RECORD = f"SELECT {_RECORD_COLS} FROM owner_record WHERE player_id = %s"  # noqa: S608
_SQL_SELECT_RECORD_FOR_UPDATE = _SQL_SELECT_RECORD + " FOR UPDATE"

# TiDB 无 gap 锁,FOR UPDATE 在零行时不加锁 —— "记录不存在"的并发分支靠主键
# INSERT IGNORE 的唯一键来互斥,确保后续 FOR UPDATE 一定锁到存在行。
_SQL_ENSURE_RECORD = "INSERT IGNORE INTO owner_record (player_id) VALUES (%s)"

_SQL_UPDATE_RECORD = """UPDATE owner_record SET
    owner_epoch = %s, owner_type = %s, phase = %s,
    pod_name = %s, instance_uid = %s, instance_epoch = %s,
    assignment_or_allocation_id = %s, release_track = %s,
    operation_id = %s, admit_not_before_ms = %s,
    hub_source_revision = %s, updated_at_ms = %s
WHERE player_id = %s AND owner_epoch = %s"""

_SQL_ADMIT = """UPDATE owner_record SET phase = %s, updated_at_ms = %s
WHERE player_id = %s AND owner_epoch = %s AND operation_id = %s"""

# ★ Release 只置 type/phase/operation,**不动 owner_epoch 与 hub_source_revision**。
_SQL_RELEASE = """UPDATE owner_record SET
    owner_type = 0, phase = 0, operation_id = '', admit_not_before_ms = 0,
    pod_name = '', instance_uid = '', instance_epoch = 0,
    assignment_or_allocation_id = '', release_track = '', updated_at_ms = %s
WHERE player_id = %s AND owner_epoch = %s"""

_SQL_SELECT_LEASE = (
    "SELECT instance_uid, pod_name, instance_epoch, release_track, lease_deadline_ms "
    "FROM ds_instance_lease WHERE instance_uid = %s"
)
_SQL_SELECT_LEASE_FOR_UPDATE = _SQL_SELECT_LEASE + " FOR UPDATE"

_SQL_INSERT_LEASE = (
    "INSERT INTO ds_instance_lease "
    "(instance_uid, pod_name, instance_epoch, release_track, lease_deadline_ms, updated_at_ms) "
    "VALUES (%s, %s, %s, %s, %s, %s)"
)

_SQL_UPDATE_LEASE = """UPDATE ds_instance_lease SET
    pod_name = %s, instance_epoch = %s, release_track = %s,
    lease_deadline_ms = %s, updated_at_ms = %s
WHERE instance_uid = %s"""

_SQL_INSERT_LOG = (
    "INSERT INTO owner_transition_log "
    "(player_id, from_epoch, to_epoch, op, operation_id, detail) "
    "VALUES (%s, %s, %s, %s, %s, %s)"
)

_SQL_SWEEP_LOG = (
    "DELETE FROM owner_transition_log "
    "WHERE created_at < DATE_SUB(NOW(), INTERVAL %s DAY) LIMIT %s"
)


# ── 辅助 ─────────────────────────────────────────────────────────────────────


async def _ensure_record(cur, player_id: int) -> None:  # noqa: ANN001
    """保证 owner_record 行存在,让后续 FOR UPDATE 一定锁到存在行(TiDB 无 gap 锁)。"""
    await cur.execute(_SQL_ENSURE_RECORD, (player_id,))


def _row_to_record(row) -> odata.OwnerRecord:  # noqa: ANN001
    return odata.OwnerRecord(
        player_id=int(row[0]),
        owner_epoch=int(row[1]),
        owner_type=int(row[2]),
        phase=int(row[3]),
        target=odata.OwnerTarget(
            pod_name=row[4] or "",
            instance_uid=row[5] or "",
            instance_epoch=int(row[6] or 0),
            assignment_or_allocation_id=row[7] or "",
            release_track=row[8] or "",
        ),
        operation_id=row[9] or "",
        admit_not_before_ms=int(row[10] or 0),
        hub_source_revision=int(row[11] or 0),
        updated_at_ms=int(row[12] or 0),
    )


def _with_lease(rec: odata.OwnerRecord, lease_deadline_ms: int) -> odata.OwnerRecord:
    return dataclasses_replace(rec, lease_deadline_ms=lease_deadline_ms)


def dataclasses_replace(rec: odata.OwnerRecord, **changes) -> odata.OwnerRecord:
    import dataclasses

    return dataclasses.replace(rec, **changes)


async def _read_lease(cur, instance_uid: str) -> int:  # noqa: ANN001
    if not instance_uid:
        return 0
    await cur.execute(_SQL_SELECT_LEASE, (instance_uid,))
    row = await cur.fetchone()
    return int(row[4]) if row else 0


async def _read_lease_for_update(cur, instance_uid: str) -> int:  # noqa: ANN001
    """★ 屏障取值必须 FOR UPDATE —— 取的是 CAS **线性化点**的观察值。

    普通读会让一次并发的续租在我们算完屏障之后提交,于是屏障基于一个已经过期的
    截止时刻算出,新 owner 提前开始可玩。
    """
    if not instance_uid:
        return 0
    await cur.execute(_SQL_SELECT_LEASE_FOR_UPDATE, (instance_uid,))
    row = await cur.fetchone()
    return int(row[4]) if row else 0


async def _write_log(  # noqa: ANN001
    cur, player_id: int, from_epoch: int, to_epoch: int, op: int, operation_id: str, detail: str
) -> None:
    await cur.execute(
        _SQL_INSERT_LOG, (player_id, from_epoch, to_epoch, op, operation_id, detail)
    )


def _epoch_conflict(current: odata.OwnerRecord, expect: int) -> errcode.PandoraError:
    """epoch 冲突 —— **附当前记录**,调用方靠它决定重试还是放弃。"""
    err = errcode.PandoraError(
        errcode.ErrOwnerEpochConflict,
        "expect_epoch %d != current %d",
        expect,
        current.owner_epoch,
    )
    err.current_record = current  # type: ignore[attr-defined]
    return err


# mysqlx 的错误判别在这里也用得上(唯一键冲突 = 并发建行,可安全忽略)。
__all__ = ["MySQLOwnerRepo", "mysqlx"]
