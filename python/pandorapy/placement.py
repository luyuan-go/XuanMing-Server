"""DS 实例身份 + fence 租约协议常量 —— 对应 Go 侧 pkg/placement。

★ 这些是**正确性常量,不是调优参数**(CLAUDE.md §9.22):
    调大只增加故障恢复延迟;调小会重新打开「一名玩家同时存在于两台 DS」的脑裂窗口。
    跨仓契约 —— UE 侧 UPandoraDSBackendSubsystem 有一份对应实现,两边必须同值。

协议(docs/design/battle-reconnect.md §8):

  1. DS 以最近一次「绑定 active 凭据的权威心跳响应」为租约起点(**单调时钟**)。
     连续 DS_FENCE_LEASE_MAX_SECONDS 未能续租 → DS 必须对**存量玩家**自我 fencing:
     关闭输入、Kick 所有已准入连接、销毁 Pawn(**不只是拒新玩家**)。

  2. 服务端任何「把静默 DS 上的玩家交给新 DS」的再入门,必须等待该 DS 的
     last_heartbeat_ms 至少经过 DS_FENCE_REENTRY_BARRIER。
     由此保证核心时序:**旧 DS 最晚停止可玩时间 < 新 DS 最早开始可玩时间**。

  3. player_locator TTL 与 hub_allocator heartbeat_timeout 都必须 ≥ 再入屏障
     (当前默认 30s ≥ 27s,启动时有机械下限保护)。
"""

from __future__ import annotations

import re
import uuid as _uuid

# DS 侧授权租约的协议上限(秒)。UE 侧把租约硬钳在 [5, 本值],配置无法放大。
DS_FENCE_LEASE_MAX_SECONDS = 20

# 安全余量(秒)。预算构成必须完整覆盖三项,不能按单机观测的平均延迟缩小:
#   ① 心跳响应在途上限(UE HeartbeatRequestTimeoutSeconds = 4s)
#   ② fencing 检测粒度(1s ticker)
#   ③ 服务间时钟漂移专属预留(≥2s)—— ds_allocator 写 last_heartbeat_ms 与
#      login 读 now() 是**两台机器的时钟**
# 2026-07-18 从 5 提到 7:原值被前两项恰好占满,时钟漂移零预留。
DS_FENCE_SKEW_MARGIN_SECONDS = 7

# 服务端再入屏障:自 DS 最后一次心跳起必须经过该时长,才允许把这台 DS 上的玩家
# 路由到任何新 DS。派生值 = 27s,保持"租约上限 + 余量"只有一个权威计算入口。
DS_FENCE_REENTRY_BARRIER_SECONDS = DS_FENCE_LEASE_MAX_SECONDS + DS_FENCE_SKEW_MARGIN_SECONDS

# canonical 小写 RFC4122 UUID 的形状。先用正则挡掉大写 / 花括号 / URN 前缀这些
# uuid.UUID() 会**接受但改写**的形式 —— 我们要的是"原样 canonical",不是"能解析"。
_CANONICAL_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


def valid_operation_id(value: str) -> bool:
    """只接受 canonical 小写 RFC4122 UUIDv4。对应 Go 的 ValidOperationID。

    ★ 为什么要求"canonical 原样相等"而不只是"能解析":
        operation_id 是 §9.23 的端到端幂等键。`uuid.UUID(s)` 会接受大写、
        带花括号 `{...}`、带 `urn:uuid:` 前缀等多种写法并归一化 —— 如果只校验
        "能解析",同一次进场用不同写法重试就会被当成**两个不同的 operation**,
        幂等键失效,于是重复占座 / 重复分配 DS / 产生第二个 owner。
        Go 侧靠 `id.String() == value` 挡这一层,这里靠正则 + 版本/变体复核。
    """
    if not isinstance(value, str) or not _CANONICAL_UUID_RE.match(value):
        return False
    try:
        parsed = _uuid.UUID(value)
    except ValueError:
        return False
    return (
        parsed.version == 4
        and parsed.variant == _uuid.RFC_4122
        and int(parsed) != 0
        and str(parsed) == value
    )


def new_operation_id() -> str:
    """铸一个新的 canonical operation_id。"""
    return str(_uuid.uuid4())
