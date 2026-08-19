"""数据库守卫 —— 对应 Go 侧 pkg/dbguard。

三件事,重要性递减:

1. **sql_mode 严格模式断言(启动期 fail-fast)**
   这是 CLAUDE.md §9.24 里**唯一允许因数据库检查而拒绝启动**的场景。理由:
   非严格模式下超长写入不报错而是**静默截断** —— 真 MySQL 8.4 实测:往
   VARBINARY(16) 写 100 字节,严格模式 → Error 1406 写入失败;非严格 → err=nil
   且实际只存 16 字节。玩家数据被无声砍断且无任何错误可观测。
   静默数据损坏远比服务起不来严重,所以这里必须硬失败。

   ⚠️ 必须断言 `@@session.sql_mode` 而不是 `@@global` —— DSN 参数能覆盖 session,
   查 global 会得到"看起来没问题"的假象。

2. **容量预算巡检(只告警不阻断)**
   超预算是"要去查的问题",不是"服务不能跑的理由";拒绝启动会把容量问题升级成
   可用性事故。走 information_schema 估算(毫秒级不锁表),**禁止 COUNT(*)**
   拖垮启动。

3. **写入侧 payload 上限(三档告警)**
   达上限拒写 / 达 80% 放行但 WARN(留排查窗口)/ 否则静默。
   §9.24 要求集合序列化列同时有单元素、集合条目、整体字节三个上限,缺一个就有洞。
"""

from __future__ import annotations

import dataclasses
import enum

from pandorapy import log as plog

# 与 Go 侧 strictModeProbeTimeout 一致:够慢网络一次往返,又不会把启动挂死。
STRICT_MODE_PROBE_TIMEOUT_SEC = 5.0

# 逼近告警阈值:达 80% 放行但 WARN,留出排查窗口(与 Go 侧一致)。
WARN_RATIO = 0.8


class StrictModeError(RuntimeError):
    """sql_mode 缺 STRICT_TRANS_TABLES。调用方必须打 mysql_strict_mode_required 后退出。"""


class PayloadTooLargeError(RuntimeError):
    """序列化 payload 超过列容量上限。"""


async def assert_strict_mode(conn) -> None:  # noqa: ANN001 —— 兼容 aiomysql / asyncmy 游标
    """断言 session sql_mode 含 STRICT_TRANS_TABLES。对应 Go 的 dbguard.AssertStrictMode。

    调用方(各服务 main 装配完连接池后立刻调):

        try:
            await dbguard.assert_strict_mode(conn)
        except dbguard.StrictModeError as exc:
            logger.error("mysql_strict_mode_required", err=str(exc))
            return 1

    为什么不在本函数里直接退出:各服务的日志与退出约定不同,把决定权留给调用方,
    这里只统一"怎么探测"(与 Go 侧同样的分工)。
    """
    async with conn.cursor() as cur:
        # 必须是 session 不是 global —— DSN 参数可覆盖 session。
        await cur.execute("SELECT @@session.sql_mode")
        row = await cur.fetchone()
    mode = (row[0] if row else "") or ""
    for part in mode.split(","):
        if part.strip() == "STRICT_TRANS_TABLES":
            return
    raise StrictModeError(
        f"dbguard: session sql_mode 缺 STRICT_TRANS_TABLES(当前={mode!r})。"
        "非严格模式下超长写入会被静默截断(err=nil 但数据被砍断),等于无声的数据损坏。"
        "修法:MySQL 服务端 --sql-mode 保留默认值,或从 DSN 中移除覆盖 sql_mode 的参数"
    )


# ── 容量预算 ─────────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True, slots=True)
class TableBudget:
    """一张表的容量预算。各服务在自己的 budgets.py 里声明。

    上限值按**设计期望**定,不按列类型上限定 —— 写成列类型上限等于没设(数据涨到
    快撑爆才告警,业务语义早已崩坏)。这是 §9.24 明写的要求。
    """

    table: str
    max_rows: int = 0
    max_avg_row_length: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class Violation:
    table: str
    metric: str
    actual: int
    budget: int


@dataclasses.dataclass(slots=True)
class CheckResult:
    checked: int
    violations: list[Violation]


async def check_budgets(conn, schema: str, budgets: list[TableBudget]) -> CheckResult:  # noqa: ANN001
    """跑一轮容量巡检。**只告警不阻断** —— 返回结果由调用方打日志 + 计数。

    走 information_schema.TABLES 估算(毫秒级、不锁表)。刻意**不用 COUNT(*)**:
    在大表上会拖垮启动,而这只是个告警指标,不需要精确值。
    """
    if not budgets:
        return CheckResult(checked=0, violations=[])

    by_table = {b.table: b for b in budgets}
    placeholders = ",".join(["%s"] * len(by_table))
    async with conn.cursor() as cur:
        await cur.execute(
            f"SELECT TABLE_NAME, TABLE_ROWS, AVG_ROW_LENGTH "  # noqa: S608 —— 表名来自代码常量非用户输入
            f"FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA = %s AND TABLE_NAME IN ({placeholders})",
            (schema, *by_table.keys()),
        )
        rows = await cur.fetchall()

    violations: list[Violation] = []
    for name, table_rows, avg_len in rows:
        budget = by_table.get(name)
        if budget is None:
            continue
        if budget.max_rows and (table_rows or 0) > budget.max_rows:
            violations.append(Violation(name, "rows", int(table_rows or 0), budget.max_rows))
        if budget.max_avg_row_length and (avg_len or 0) > budget.max_avg_row_length:
            violations.append(
                Violation(name, "avg_row_length", int(avg_len or 0), budget.max_avg_row_length)
            )
    return CheckResult(checked=len(rows), violations=violations)


def log_violations(result: CheckResult) -> None:
    """把巡检结果打成 ERROR 日志。事件名与 Go 侧一致,供 Grafana 复用既有面板。"""
    logger = plog.get()
    for v in result.violations:
        logger.error(
            "db_budget_violation",
            table=v.table,
            metric=v.metric,
            actual=v.actual,
            budget=v.budget,
        )


# ── 保留期清理(§9.24)──────────────────────────────────────────────────────


class Mode(str, enum.Enum):
    """清理模式。**零值 / 留空 = REPORT_ONLY**(用户 2026-07-22 指令)。

    为什么默认只报告不删:
        自动删生产数据不可逆。清理条件 / 保留期 / 幂等窗口任一处配错都会静默删掉
        不该删的玩家数据,而且**删完才发现**。把"何时删"的决定权交回人手里。
        代价是 report_only 下库会继续增长 —— 所以待清理量必须持续可见
        (WARN + metric + dbcheck -pending),让人能判断何时开删。

    唯一例外:battle_result 的战报清理默认真删(产品口径"最多存最近六个月")。
    """

    REPORT_ONLY = "report_only"
    DELETE = "delete"


def parse_mode(raw: str) -> Mode:
    """解析配置里的 retention_mode。

    ⚠️ 无法识别的值**报错而非猜成 delete** —— 拼错一个字母就开始删生产数据
    是不可接受的失败模式(与 Go 侧 ParseMode 同一决定)。
    """
    text = (raw or "").strip().lower()
    if text == "":
        return Mode.REPORT_ONLY
    try:
        return Mode(text)
    except ValueError:
        raise ValueError(
            f"dbguard: 无法识别的 retention_mode={raw!r}(只支持 report_only / delete)。"
            f"拒绝猜测 —— 猜成 delete 会开始删生产数据"
        ) from None


@dataclasses.dataclass(slots=True)
class Outcome:
    """一轮清理的结果。"""

    mode: Mode
    matched: int  # 满足清理条件的行数
    deleted: int  # 实际删除的行数(report_only 恒为 0)


async def sweep_table(  # noqa: ANN001
    conn,
    mode: Mode,
    schema: str,
    table: str,
    where: str,
    limit: int,
    *params,
) -> Outcome:
    """按 mode 处理满足 where 的行。对应 Go 的 dbguard.SweepTable。

    ★ Count 与 Delete **共用同一个 where 字符串** —— 这是从机制上排除
    "报告说 0 行、实际删了 10 万行"的条件漂移。条件只写一遍,不允许调用方传两份。

    小批量 DELETE ... LIMIT 防长事务锁表;多副本并发跑幂等(删的是同一批行)。
    """
    async with conn.cursor() as cur:
        await cur.execute(
            f"SELECT COUNT(*) FROM `{schema}`.`{table}` WHERE {where}",  # noqa: S608
            params,
        )
        row = await cur.fetchone()
        matched = int(row[0]) if row else 0

        if mode is not Mode.REPORT_ONLY and matched > 0:
            await cur.execute(
                f"DELETE FROM `{schema}`.`{table}` WHERE {where} LIMIT %s",  # noqa: S608
                (*params, limit),
            )
            deleted = cur.rowcount or 0
        else:
            deleted = 0

    if mode is Mode.REPORT_ONLY and matched > 0:
        # 待清理量必须持续可见 —— 这是 report_only 默认的配套要求,
        # 否则库悄悄涨到撑爆都没人知道。
        plog.get().warning(
            "db_retention_pending",
            table=table,
            pending_rows=matched,
            hint="retention_mode=report_only,未删任何数据;需要真删请显式配置 delete",
        )
    return Outcome(mode=mode, matched=matched, deleted=deleted)


# ── 写入侧 payload 上限 ───────────────────────────────────────────────────────


def check_payload(name: str, payload: bytes, max_bytes: int) -> None:
    """整体字节上限(§9.24 三个上限里的第 ③ 条)。对应 Go 的 dbguard.CheckPayload。

    三档(与 Go 一致):
      - 超上限   → 抛异常拒写(数据会被静默截断的唯一防线)
      - 达 80%   → 放行但 WARN,留出排查窗口
      - 否则     → 静默

    ⚠️ 这一条只管"整体字节"。§9.24 要求集合序列化列**同时**有:
       ① 单元素上限 ② 集合条目上限 ③ 整体字节上限
    只设 ③ 会漏掉"单个格子胖到 60KB 但整体没超";只设 ①② 会漏掉"每项合规但
    项数×大小仍超列容量"。①② 属于业务层校验,不在本函数职责内 —— 别以为调了
    check_payload 就达标了。真实教训:bag 管住 items 条数却没管单个 item 的
    attrs 条数(深度无闸);rewardclaim 管住单条位图大小却没管位图条目数(广度无闸)。
    """
    size = len(payload)
    if size > max_bytes:
        raise PayloadTooLargeError(
            f"dbguard: {name} 序列化后 {size} 字节,超过上限 {max_bytes}。"
            f"拒写而不是让它进库被静默截断"
        )
    if size >= int(max_bytes * WARN_RATIO):
        plog.get().warning(
            "db_payload_approaching_limit",
            name=name,
            size=size,
            limit=max_bytes,
            ratio=round(size / max_bytes, 3),
        )
