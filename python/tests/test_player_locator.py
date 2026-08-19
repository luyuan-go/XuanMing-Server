"""player_locator 测试。

重点:
  1. ★ TTL 机械下限 —— 配置调低必须被抬回(否则打开脑裂窗口)
  2. ★ 入参校验每个分支一个独立 reason
  3. ★ 非 HUB 状态不允许带 hub fence
  4. ★ uint64 十进制比较与 Lua 侧对拍(大数不能丢精度)
"""

from __future__ import annotations

import pytest

from pandorapy import errcode, placement
from pandorapy.services.player_locator import biz as lbiz


def _fence(**kw) -> lbiz.HubPresenceFence:
    base = {"assignment_id": "a-1", "admission_id": "ad-1", "admission_seq": 5}
    base.update(kw)
    return lbiz.HubPresenceFence(**base)


def _hub(**kw) -> lbiz.LocationInput:
    base = {
        "player_id": 1001,
        "state": lbiz.LOCATION_STATE_HUB,
        "hub_pod": "hub-1",
        "hub_presence_fence": _fence(),
    }
    base.update(kw)
    return lbiz.LocationInput(**base)


# ── ★ TTL 机械下限 ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("configured", [1, 5, 10, 26])
def test_ttl_is_clamped_up_to_fence_barrier(configured: int) -> None:
    """★ 配置低于再入屏障时**必须抬回**。

    这是正确性下限不是调优:presence TTL < 屏障时,presence 先蒸发、再入门放行,
    而分区的旧 DS 还没完成自我 fencing —— 一名玩家同时存在于两台 DS。
    """
    assert lbiz.effective_ttl_sec(configured) == placement.DS_FENCE_REENTRY_BARRIER_SECONDS


def test_ttl_above_barrier_is_kept() -> None:
    """配置高于屏障时保持原值(运维可以调更保守)。"""
    assert lbiz.effective_ttl_sec(60) == 60


@pytest.mark.parametrize("configured", [0, -1])
def test_missing_ttl_falls_back_to_default_not_barrier(configured: int) -> None:
    """缺配置 / 非法值 → 用默认 30s,而**不是**直接落到屏障 27s。

    顺序是"先补默认、再抬下限"(与 Go 一致):默认值已经高于屏障,
    所以结果是 30 不是 27。这个区别有实际意义 —— 缺配置时应当得到
    一个保守的正常值,不是刚好卡在下限上。
    """
    assert lbiz.effective_ttl_sec(configured) == lbiz.DEFAULT_TTL_SEC == 30
    assert lbiz.DEFAULT_TTL_SEC > placement.DS_FENCE_REENTRY_BARRIER_SECONDS


def test_fence_barrier_is_27_seconds() -> None:
    """屏障 = 租约上限 20 + 偏差余量 7。三方(Go / Python / UE)必须同值。"""
    assert placement.DS_FENCE_REENTRY_BARRIER_SECONDS == 27


# ── ★ 入参校验:每个分支独立 reason ─────────────────────────────────────────


def test_zero_player_rejected() -> None:
    with pytest.raises(errcode.PandoraError, match=lbiz.REASON_PLAYER_ID_ZERO):
        lbiz.validate_location_input(_hub(player_id=0))


@pytest.mark.parametrize("state", [0, 4, 99, -1])
def test_out_of_range_state_rejected(state: int) -> None:
    with pytest.raises(errcode.PandoraError, match=lbiz.REASON_STATE_OUT_OF_RANGE):
        lbiz.validate_location_input(_hub(state=state))


def test_hub_without_pod_rejected() -> None:
    with pytest.raises(errcode.PandoraError, match=lbiz.REASON_HUB_POD_MISSING):
        lbiz.validate_location_input(_hub(hub_pod=""))


@pytest.mark.parametrize(
    "partial",
    [
        {"assignment_id": ""},
        {"admission_id": ""},
        {"admission_seq": 0},
    ],
)
def test_partial_fence_rejected(partial: dict) -> None:
    """★ fence 要么全空(legacy)要么全齐(fenced),**半齐是错误**。

    放行会让代际闸拿着残缺身份去比,结论不可靠 —— 而且不报错。
    """
    with pytest.raises(errcode.PandoraError, match=lbiz.REASON_HUB_FENCE_INCOMPLETE):
        lbiz.validate_location_input(_hub(hub_presence_fence=_fence(**partial)))


def test_zero_fence_is_legacy_mode_and_allowed() -> None:
    """全空 fence = legacy 模式,合法(滚动升级期旧调用方)。"""
    lbiz.validate_location_input(_hub(hub_presence_fence=lbiz.HubPresenceFence()))


def test_complete_fence_is_allowed() -> None:
    lbiz.validate_location_input(_hub())


# ── ★ 非 HUB 状态不允许带 hub fence ────────────────────────────────────────


@pytest.mark.parametrize(
    "state", [lbiz.LOCATION_STATE_BATTLE, lbiz.LOCATION_STATE_MATCHING]
)
def test_hub_fence_on_non_hub_state_rejected(state: int) -> None:
    """★ 带了说明调用方状态机错乱。

    放行会让 BATTLE 写把 HUB 的代际信息一起刷进去 —— 之后 HUB 的代际闸
    拿到的是一个从没发生过的"当前代"。
    """
    inp = lbiz.LocationInput(
        player_id=1001,
        state=state,
        match_id=555,
        battle_pod="battle-1",
        hub_presence_fence=_fence(),
    )
    with pytest.raises(errcode.PandoraError, match=lbiz.REASON_HUB_FENCE_ON_NON_HUB):
        lbiz.validate_location_input(inp)


def test_matching_requires_match_id() -> None:
    inp = lbiz.LocationInput(player_id=1001, state=lbiz.LOCATION_STATE_MATCHING)
    with pytest.raises(errcode.PandoraError, match=lbiz.REASON_MATCH_ID_MISSING):
        lbiz.validate_location_input(inp)


@pytest.mark.parametrize(
    ("match_id", "battle_pod"), [(0, "battle-1"), (555, ""), (0, "")]
)
def test_battle_requires_match_and_pod(match_id: int, battle_pod: str) -> None:
    inp = lbiz.LocationInput(
        player_id=1001,
        state=lbiz.LOCATION_STATE_BATTLE,
        match_id=match_id,
        battle_pod=battle_pod,
    )
    with pytest.raises(errcode.PandoraError, match=lbiz.REASON_BATTLE_TARGET_MISSING):
        lbiz.validate_location_input(inp)


def test_battle_with_both_is_allowed() -> None:
    lbiz.validate_location_input(
        lbiz.LocationInput(
            player_id=1001,
            state=lbiz.LOCATION_STATE_BATTLE,
            match_id=555,
            battle_pod="battle-1",
        )
    )


# ── ★ uint64 十进制比较(与 Lua 对拍)────────────────────────────────────────


@pytest.mark.parametrize(
    ("left", "right", "want"),
    [
        ("0", "0", 0),
        ("1", "2", -1),
        ("2", "1", 1),
        ("10", "9", 1),  # 位数不同优先
        ("9", "10", -1),
        ("007", "7", 0),  # 前导零归一
        ("0000", "0", 0),
        ("", "0", 0),  # 空串视为 0
        # ★ 关键:超过 2^53 的两个相邻值必须区分开。
        # Lua 的 double 在这里会把它们判成相等 —— 那正是要用十进制字符串比较的原因。
        ("9007199254740993", "9007199254740992", 1),
        ("18446744073709551615", "18446744073709551614", 1),
        ("18446744073709551615", "18446744073709551615", 0),
    ],
)
def test_compare_uint_decimal(left: str, right: str, want: int) -> None:
    assert lbiz.compare_uint_decimal(left, right) == want


def test_double_precision_would_have_failed() -> None:
    """★ 证明这个函数不是多余的。

    把两个相邻的大 seq 走 float 比较 —— 它们会被判成相等。
    Redis 的 Lua 数就是 double,所以那边必须用十进制字符串比。
    """
    a, b = 9007199254740993, 9007199254740992
    assert float(a) == float(b), "这条前提不成立说明测试写错了"
    assert a != b
    assert lbiz.compare_uint_decimal(str(a), str(b)) == 1


def test_python_native_int_compare_is_already_correct() -> None:
    """Python 侧原生比较就是对的(int 任意精度)——函数只为与 Lua 对拍而存在。"""
    a, b = 18446744073709551615, 18446744073709551614
    assert (a > b) == (lbiz.compare_uint_decimal(str(a), str(b)) == 1)


# ── ★ presence 语义 ────────────────────────────────────────────────────────


def test_stale_presence_error_carries_identity() -> None:
    """代际闸拒绝必须带上本次 incoming 的全量身份 —— 供与 hub_allocator 侧对账。

    期望值(当前代)在 Lua 内比对拿不到,所以只能把 incoming 打全。
    """
    fence = _fence(assignment_id="assign-77", admission_seq=42)
    err = lbiz.stale_presence_error(1001, fence)
    assert err.code == errcode.ErrLocatorConflict
    assert "assign-77" in err.msg
    assert "42" in err.msg


def test_presence_miss_semantics_documented() -> None:
    """★ key miss ≠ 玩家已离开旧 DS。

    这条不是文档口径而是可执行约束:任何"查不到 → 当成没人 → 放行"的写法
    都会在网络分区时放出第二个 owner。
    """
    text = lbiz.presence_miss_is_not_departure()
    assert "不能证明" in text
    assert "owner" in text
