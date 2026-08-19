"""Kafka 封装 —— 对应 Go 侧 pkg/kafkax(consistent.go / producer.go / consumer.go)。

一致性哈希分区器必须与 Go **逐位一致**,这是本模块最硬的约束:
    不变量 §9.9 要求 "kafka topic key = 业务实体 ID",目的是让同一玩家 / 同一对局的
    事件有序。有序性由"同一 key 恒落同一 partition"保证 —— Kafka 只在单 partition 内
    保证顺序。

    迁移期 Go 版和 Python 版会同时生产同一批 topic。如果两边的 key→partition 映射
    不同,同一个玩家的事件会被劈到两个 partition:
      - 消费侧看到的事件顺序错乱(后发生的先到)
      - 而且**不报错** —— 只是偶发的状态错乱,极难定位
    所以这里逐行照抄 Go 的算法,并有交叉验证测试(tests/test_kafkax_parity.py)
    直接对比两边的完整路由表。

Go 侧算法(pkg/kafkax/consistent.go):
    replicaCount = 20(每 partition 20 个虚拟节点)
    replicaKey   = 8 字节大端 = partition(int32 4 字节) ++ replicaIdx(低 32 位 4 字节)
    hash         = FNV-1a 32 位
    路由         = 环上第一个 >= keyHash 的节点,越界回绕到 0

客户端选型:kafka-python(2026-08-18 改判,原先选的是 confluent-kafka)
    实测 kafka-python 3.x 两个月内发了 6 个版本,修的是 rebalance stuck partition、
    leader epoch fencing 这类协议层硬骨头;且纯 Python 无 C 扩展 —— 开发机是
    Windows + 360 主动防御,二进制 wheel 正是它最爱拦的东西(已有 namecheck 的前例)。
"""

from __future__ import annotations

import bisect
import threading

# 与 Go 侧 NewConsistent 的默认值一致。改这个数会让整张路由表重排,
# 等于把所有 key 重新分配 partition —— 迁移期两边必须同值。
DEFAULT_REPLICA_COUNT = 20

# FNV-1a 32 位参数(Go 的 hash/fnv.New32a)。
_FNV_32A_OFFSET = 0x811C9DC5
_FNV_32A_PRIME = 0x01000193
_UINT32_MASK = 0xFFFFFFFF


def fnv1a_32(data: bytes) -> int:
    """FNV-1a 32 位 —— 与 Go 的 hash/fnv.New32a() 逐字节等价。

    刻意不用 Python 内置 hash():那个带随机化种子,进程间都不一致,更不用说跨语言。
    """
    h = _FNV_32A_OFFSET
    for byte in data:
        h ^= byte
        h = (h * _FNV_32A_PRIME) & _UINT32_MASK
    return h


def _gen_replica_key(partition: int, replica_idx: int) -> bytes:
    """生成虚拟节点的 hash 输入 —— 对应 Go 的 genReplicaKey。

    Go 那边是手工位移拼 8 个字节(大端),partition 是 int32、replicaIdx 是 int 但
    只取低 32 位。这里用 to_bytes 得到同样的字节序列。
    signed=True 是为了在 partition 为负时也与 Go 的 byte(int32) 截断行为一致
    (实际 partition 非负,但对齐算法不留"只在正常输入下相同"的缝)。
    """
    return (partition & _UINT32_MASK).to_bytes(4, "big") + (
        replica_idx & _UINT32_MASK
    ).to_bytes(4, "big")


class Consistent:
    """key → partition 的稳定路由表(一致性哈希)。对应 Go 的 kafkax.Consistent。

    读多写少,用 RLock 保护(Go 侧是 sync.RWMutex)。
    """

    __slots__ = ("_ring", "_sorted_hashes", "_replica_count", "_partitions", "_lock")

    def __init__(self, replica_count: int = DEFAULT_REPLICA_COUNT) -> None:
        self._replica_count = replica_count if replica_count > 0 else DEFAULT_REPLICA_COUNT
        self._ring: dict[int, int] = {}
        self._sorted_hashes: list[int] = []
        self._partitions: set[int] = set()
        self._lock = threading.RLock()

    def add_partition(self, partition: int) -> None:
        """加入一个 partition。重复添加是 no-op(与 Go 一致)。"""
        with self._lock:
            if partition in self._partitions:
                return
            for idx in range(self._replica_count):
                hash_val = fnv1a_32(_gen_replica_key(partition, idx))
                # 注意:Go 那边 ring[hashVal] = partition 会**覆盖**已有条目(哈希碰撞时
                # 后添加的赢),且 sortedHashes 会留下重复值。这里如实照抄该行为,
                # 不"优化"成去重 —— 去重会让碰撞情况下的路由结果与 Go 不同。
                self._ring[hash_val] = partition
                self._sorted_hashes.append(hash_val)
            self._partitions.add(partition)
            self._sorted_hashes.sort()

    def get_partition(self, key: str) -> tuple[int, bool]:
        """把 key 路由到 partition。环为空返回 (0, False),与 Go 的 (0, false) 一致。"""
        with self._lock:
            if not self._ring:
                return 0, False
            key_hash = fnv1a_32(key.encode())
            # bisect_left 等价于 Go 的 sort.Search(需要第一个 >= keyHash 的位置)
            idx = bisect.bisect_left(self._sorted_hashes, key_hash)
            if idx == len(self._sorted_hashes):
                idx = 0  # 回绕,与 Go 一致
            return self._ring[self._sorted_hashes[idx]], True

    def partition_count(self) -> int:
        with self._lock:
            return len(self._partitions)

    def partitions(self) -> list[int]:
        with self._lock:
            return sorted(self._partitions)


def partitioner_for(consistent: Consistent):
    """把 Consistent 适配成 kafka-python 的 partitioner 回调。

    kafka-python 的签名是 partitioner(key_bytes, all_partitions, available_partitions)。
    我们无视它给的 partition 列表而用自己的环 —— 因为环必须与 Go 侧完全一致,
    不能受"当前哪些 partition 可用"影响(可用性变化会让同一 key 漂移到别的 partition,
    破坏 §9.9 的有序性保证)。

    ⚠️ 反过来说:partition 列表变化时必须显式重建环并让 Go / Python 两边同时生效,
    这与 Go 侧的约束相同,不是 Python 引入的新问题。
    """

    def _partition(key_bytes, all_partitions, available_partitions):  # noqa: ANN001
        if key_bytes is None:
            # 无 key = 不要求有序,交回 kafka-python 的默认轮询。
            return None
        key = key_bytes.decode() if isinstance(key_bytes, bytes) else str(key_bytes)
        partition, ok = consistent.get_partition(key)
        if not ok:
            return None
        return partition

    return _partition
