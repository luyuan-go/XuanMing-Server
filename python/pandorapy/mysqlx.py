"""MySQL / TiDB 连接 —— 对应 Go 侧 pkg/mysqlx。

选型:`asyncmy`(全 async)+ 回退 `aiomysql`。
    grpc.aio 下**整条链路必须 async** —— 任何一个同步 DB 调用都会阻塞整个 event loop,
    把并发打回单请求串行。这是迁 Python 最容易在压测时才暴露的性能陷阱,
    所以驱动层从一开始就不给同步选项。

TiDB 检测(对应 pkg/mysqlx/backend_check.go):
    TiDB 对客户端就是 MySQL 线协议,全仓唯一的 TiDB 专属代码就是解析 VERSION() 里的
    `-TiDB-vX.Y.Z`。login 等服务用 `require_tidb: true` 断言权威库确实是 TiDB ——
    误连到普通 MySQL 会让依赖 TiDB 特性的逻辑(如无 gap 锁前提下的守卫行写法)
    行为漂移,而且不报错。
"""

from __future__ import annotations

import re

from pandorapy import errcode

# 与 Go 侧 tidbVersionRe 完全一致。
_TIDB_VERSION_RE = re.compile(r"-TiDB-v(\d+)\.(\d+)\.(\d+)")

# MySQL 错误码:数据被截断 / 超长。严格模式下会以这些码报错而不是静默砍断。
ER_DATA_TOO_LONG = 1406
ER_DUP_ENTRY = 1062
ER_LOCK_DEADLOCK = 1213


class NotTiDBError(RuntimeError):
    """要求 TiDB 但实际连的不是。调用方打 account_backend_not_tidb 后退出。"""


def parse_tidb_version(version_string: str) -> tuple[int, int, int] | None:
    """从 VERSION() 结果解析 TiDB 版本。不是 TiDB 返回 None。

    形如:`8.0.11-TiDB-v8.5.0` → (8, 5, 0)
    """
    m = _TIDB_VERSION_RE.search(version_string)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3))


async def assert_tidb(conn, *, min_major: int = 0, min_minor: int = 0) -> tuple[int, int, int]:  # noqa: ANN001
    """断言连的是 TiDB(可选最低版本)。对应 Go 的 mysqlx 后端校验。

    误连普通 MySQL 不会报错、只会让依赖 TiDB 语义的逻辑悄悄跑偏 ——
    典型的是 TiDB 无 gap 锁,`FOR UPDATE` 在零行时不加锁,所以 friend / mission
    的限额校验必须先锁守卫行。在 MySQL 上那套写法是多余但无害的,
    反过来(以为是 TiDB 其实是 MySQL)才危险。
    """
    async with conn.cursor() as cur:
        await cur.execute("SELECT VERSION()")
        row = await cur.fetchone()
    version_string = (row[0] if row else "") or ""
    parsed = parse_tidb_version(version_string)
    if parsed is None:
        raise NotTiDBError(
            f"权威库要求 TiDB(require_tidb=true),实际 VERSION()={version_string!r}"
        )
    if (parsed[0], parsed[1]) < (min_major, min_minor):
        raise NotTiDBError(
            f"TiDB 版本过低:要求 >= v{min_major}.{min_minor},实际 "
            f"v{parsed[0]}.{parsed[1]}.{parsed[2]}"
        )
    return parsed


def is_deadlock(exc: BaseException) -> bool:
    """判断是否 MySQL 1213 死锁(可重试)。

    Go 侧靠 errors.As 沿链检出 *mysql.MySQLError 再看 Number;Python 侧驱动把错误码
    放在 args[0]。死锁**必须**可重试而不是当成业务失败返回给客户端 ——
    TiDB 下并发事务撞死锁是正常现象,不重试会让玩家看到随机失败。
    """
    args = getattr(exc, "args", ())
    return bool(args) and args[0] == ER_LOCK_DEADLOCK


def is_duplicate_entry(exc: BaseException) -> bool:
    """判断是否唯一键冲突(1062)。

    这是幂等实现的主力:插入幂等键冲突 = 这次操作之前已经做过,
    应当返回"已完成"而不是报错(与 Go 侧各服务的幂等写法一致)。
    """
    args = getattr(exc, "args", ())
    return bool(args) and args[0] == ER_DUP_ENTRY


def is_data_too_long(exc: BaseException) -> bool:
    """判断是否 1406 数据超长。

    只有在 sql_mode 含 STRICT_TRANS_TABLES 时才会抛这个错;非严格模式下会**静默截断**
    —— 这正是 dbguard.assert_strict_mode 必须在启动期 fail-fast 的原因。
    """
    args = getattr(exc, "args", ())
    return bool(args) and args[0] == ER_DATA_TOO_LONG


def map_db_error(exc: BaseException) -> int:
    """把数据库异常映射成业务错误码。

    刻意**不**把死锁映射成错误码 —— 死锁应当在数据层重试,不该走到这里。
    走到这里的死锁说明重试已耗尽,那才是真的内部错误。
    """
    if is_duplicate_entry(exc):
        return errcode.ErrAlreadyExists
    if is_data_too_long(exc):
        # 对客户端是"参数非法",服务端另有 WARN 暴露真实原因 ——
        # 不把列容量这种内部细节泄露给客户端。
        return errcode.ErrInvalidArg
    return errcode.ErrInternal
