"""Snowflake 发号器 —— 与 Go 侧 pkg/snowflake 的位布局**完全一致**。

为什么位布局必须逐位一致:
    dialogue_id / trade_id / mail 游标等等都是 snowflake,它们已经落库、已经在客户端
    手里、已经在日志里。Python 版和 Go 版在迁移期会同时发号(strangler 双栈),
    如果位布局不同:
      - 同一个逻辑秒里两边可能发出**相同**的 ID(node 段错位)
      - 或者 ID 大小顺序错乱(时间段位宽不同),破坏"ID 单调递增"这个被依赖的性质
    这是静默数据损坏,不是崩溃。

Go 侧口径(pkg/snowflake/snowflake.go:22):
    Epoch    = 1781161165        # 2026-06-11 06:59:25 UTC,**秒**级,不是毫秒
    NodeBits = 17                # node 段,最多 131072 个节点
    StepBits = 15                # 每个逻辑秒每节点 32768 个序号
    timeShift = 17 + 15 = 32
    nodeShift = 15

    ID = (unix_sec - Epoch) << 32 | (node_id << 15) | step

⚠️ 时间粒度是**秒**而不是毫秒 —— 这点和大多数 snowflake 实现不同,照抄网上的实现
   一定会错。每节点每秒上限 32768 个 ID,超了就阻塞到下一秒(与 Go 一致,不是丢号)。

node_id 唯一性:
    Go 侧有 pkg/snowflake/etcdnode 做 etcd 自动抢占 + 失租退出。Python 侧本轮只实现
    static 模式(读 yaml 的 node.node_id),够 dialogue 单副本用。多副本部署前必须先
    补 etcd 抢占,否则会重号 —— 见 CLAUDE.md §9 不变量 11。
"""

from __future__ import annotations

import threading
import time

# ── 位布局(与 Go 逐个对齐,不要改)────────────────────────────────────────────
EPOCH: int = 1781161165
NODE_BITS: int = 17
STEP_BITS: int = 15

_TIME_SHIFT = NODE_BITS + STEP_BITS  # 32
_NODE_SHIFT = STEP_BITS  # 15
_STEP_MASK = (1 << STEP_BITS) - 1  # 32767
NODE_MASK: int = (1 << NODE_BITS) - 1  # 131071


class ClockBeforeEpochError(RuntimeError):
    """系统时钟早于 Epoch。

    Go 侧在这里直接 panic(snowflake.go:202),理由是:uint64 减法会下溢出成垃圾时间位,
    发出的 ID 会污染全局有序性且无法回收。Python 侧同样必须硬失败而不是返回一个坏 ID。
    """


class Node:
    """一个发号节点。线程安全(对应 Go 侧的 sync.Mutex)。

    注意:这里用线程锁而不是 asyncio.Lock —— 发号是纯 CPU 操作、不 await,
    用线程锁能同时保护 asyncio 与线程池两条调用路径,且不需要把 Generate 变成协程
    (变协程会让每个调用点都得 await,污染整条业务链路)。
    """

    __slots__ = ("_node_shifted", "_last_sec", "_step", "_lock", "node_id")

    def __init__(self, node_id: int) -> None:
        if not 0 <= node_id <= NODE_MASK:
            raise ValueError(
                f"snowflake node_id={node_id} 超出范围 [0, {NODE_MASK}]"
                f"(NodeBits={NODE_BITS});多副本请走 etcd 自动分配"
            )
        self.node_id = node_id
        self._node_shifted = node_id << _NODE_SHIFT
        self._last_sec = -1
        self._step = 0
        self._lock = threading.Lock()

    def generate(self) -> int:
        """铸一个 ID。步池耗尽时阻塞到下一逻辑秒(与 Go 一致:不丢号、不重号)。"""
        with self._lock:
            now = _now_epoch()
            if now == self._last_sec:
                self._step = (self._step + 1) & _STEP_MASK
                if self._step == 0:
                    # 本逻辑秒的 32768 个序号用光了 —— 等到时钟走过这一秒。
                    # Go 侧在这里会打一条 error 日志(超过 2s 视为时钟异常),
                    # Python 侧交由调用方观测 QPS,不在锁内做 I/O。
                    now = _wait_next_second(self._last_sec)
                    self._last_sec = now
            else:
                if now < self._last_sec:
                    # 时钟回拨:继续用上一个逻辑秒,序号往下走。绝不发小于已发出的 ID。
                    self._step = (self._step + 1) & _STEP_MASK
                    if self._step == 0:
                        now = _wait_next_second(self._last_sec)
                        self._last_sec = now
                    return (self._last_sec << _TIME_SHIFT) | self._node_shifted | self._step
                self._last_sec = now
                self._step = 0
            return (self._last_sec << _TIME_SHIFT) | self._node_shifted | self._step


def _now_epoch() -> int:
    """当前时间距 Epoch 的**秒**数。时钟早于 Epoch 直接硬失败。"""
    ts = int(time.time())
    if ts < EPOCH:
        raise ClockBeforeEpochError(
            f"系统时钟 {ts} 早于 snowflake epoch {EPOCH} "
            f"({time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(EPOCH))})"
        )
    return ts - EPOCH


def _wait_next_second(last_sec: int) -> int:
    """自旋到时钟越过 last_sec。"""
    while True:
        now = _now_epoch()
        if now > last_sec:
            return now
        # 秒级粒度,睡到本秒结束就够了;不做忙等,避免烧 CPU。
        time.sleep(0.005)


def timestamp_of(snowflake_id: int) -> int:
    """从 ID 反解 unix 秒。排障时把一个 ID 还原成"什么时候发的"。"""
    return (snowflake_id >> _TIME_SHIFT) + EPOCH


def node_of(snowflake_id: int) -> int:
    """从 ID 反解 node_id。查"重号是哪两个副本"时用。"""
    return (snowflake_id >> _NODE_SHIFT) & NODE_MASK


def step_of(snowflake_id: int) -> int:
    """从 ID 反解逻辑秒内序号。"""
    return snowflake_id & _STEP_MASK
