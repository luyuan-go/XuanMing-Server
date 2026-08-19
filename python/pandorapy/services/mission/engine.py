"""任务事实引擎 —— 对应 Go 侧 internal/biz/mission.go 的 applyFactsEngine。

这是一个**分轮工作队列**,不是简单的 for 循环。理解这一点是移植的关键:

    完成一个任务时,把 COMPLETE_MISSION 事实**重新入队到下一轮**,
    而不是当场处理。因为这一轮里刚刚通过 next_ids 链式激活的新任务,
    只有到下一轮才会出现在"活跃任务"集合里。

    当场处理的后果:链上后环任务永远收不到"前环已完成"这条事实 —— 而且
    **不报错**,只表现为"做完前置任务后,后续任务的进度一直是 0"。
    这正是"任务事实顺序无关"那条结论被推翻的原因。

其余几条同样是"写错了不报错"的:

  - **空 slot_values / amount=0 的事实必须跳过**:全空槽条件匹配任意同类事件,
    放行无槽位值的事实会误推进度。
  - **已达标的槽不再累加**:否则超额进度会在配置调低目标时溢出。
  - **全槽达标判定 fail-closed**:进度槽比配置短时判**未达标**。
    原实现取 min 会让热更新增的条件被整段跳过 → **白送完成**。
  - **迭代顺序必须确定**:Go 的 map 遍历是随机的,所以那边显式 sort;
    Python 的 dict 虽然保插入序,但插入序 ≠ 排序,同一批事实在不同
    历史下会产生不同结果。这里同样显式 sorted。
"""

from __future__ import annotations

import dataclasses

# 扇出轮数上限。链式任务 A→B→C 每层消耗一轮;超过即截断并置标记。
# 防的是配置写出环(A 的 next 指回 A)导致无限循环 —— 那时截断比挂死好。
MAX_FANOUT_ROUNDS = 16


@dataclasses.dataclass(slots=True)
class Fact:
    """一条条件事实。COMPLETE_MISSION 再入也用它。"""

    category: int = 0
    slot_values: tuple[int, ...] = ()
    amount: int = 0


@dataclasses.dataclass(slots=True)
class ActiveMission:
    mission_config_id: int
    progress: list[int] = dataclasses.field(default_factory=list)


@dataclasses.dataclass(slots=True)
class DoneMission:
    mission_config_id: int
    reward_state: int = 0
    completed_at_ms: int = 0


@dataclasses.dataclass(slots=True)
class PlayerState:
    player_id: int
    active: dict[int, ActiveMission] = dataclasses.field(default_factory=dict)
    done: dict[int, DoneMission] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(slots=True)
class Mutation:
    """一次事实应用产生的全部变更(同一事务内落库)。"""

    upsert_active: list[ActiveMission] = dataclasses.field(default_factory=list)
    delete_active: list[int] = dataclasses.field(default_factory=list)
    insert_done: list[DoneMission] = dataclasses.field(default_factory=list)
    reward_logs: list[object] = dataclasses.field(default_factory=list)
    progressed: list[int] = dataclasses.field(default_factory=list)
    completed: list[int] = dataclasses.field(default_factory=list)
    auto_accepted: list[int] = dataclasses.field(default_factory=list)
    fanout_truncated: bool = False


# 条件类别:完成指定任务(链式任务的连接件)。
CONDITION_CATEGORY_COMPLETE_MISSION = 1

REWARD_STATE_NONE = 0
REWARD_STATE_CLAIMABLE = 1


def saturating_add(a: int, b: int, cap: int = 2**31 - 1) -> int:
    """饱和加 —— 溢出钳到上限而不是回绕。

    Python 的 int 无限精度,不会像 Go 的 uint32 那样回绕;但如果不钳,
    进度值会越界写不进 INT UNSIGNED 列(严格模式下报错、非严格下静默截断)。
    """
    return min(a + b, cap)


def apply_facts(catalog, state: PlayerState, facts: list[Fact], now_ms: int) -> Mutation:
    """把一批事实应用到玩家任务状态。返回同一事务内要落库的全部变更。

    catalog 需提供:
        mission_by_id(mid)      → row 或 None
        condition_by_id(cid)    → cond 或 None
        mission_condition_ids(row)  → [cid, ...]
        mission_slot_target(row, i) → 覆盖目标值(0 = 用条件自带的)
        mission_next_ids(row)   → [mid, ...]
        condition_is_fulfilled(cond, value, override) → bool
        condition_matches_slots(cond, slot_values)    → bool
        condition_clamp(cond, value, override)        → int
    """
    mut = Mutation()
    changed: dict[int, bool] = {}
    queue: list[Fact] = list(facts)

    rounds = 0
    while queue:
        if rounds >= MAX_FANOUT_ROUNDS:
            # 截断而不是挂死。置标记让调用方能看见(否则玩家会遇到"链上任务
            # 莫名其妙没接上"而服务端零信号)。
            mut.fanout_truncated = True
            break
        rounds += 1

        batch, queue = queue, []
        completed: list[int] = []

        for fact in batch:
            # ★ 护栏:空槽值 / amount=0 / 无类别的事实一律跳过。
            # 全空槽条件匹配任意同类事件,放行无槽位值的事实会误推进度。
            if fact.amount == 0 or not fact.slot_values or fact.category == 0:
                continue
            # ★ 显式排序 —— 不能依赖 dict 的插入序。
            for mission_id in sorted(state.active):
                am = state.active[mission_id]
                row = catalog.mission_by_id(am.mission_config_id)
                if row is None:
                    continue  # 配置热更删行:活跃孤行不再推进,不报错
                if not _progress_mission(catalog, row, am, fact):
                    continue
                changed[am.mission_config_id] = True
                if _all_conditions_fulfilled(catalog, row, am):
                    completed.append(am.mission_config_id)

        # 完成扇出(同一事务内)。
        for mid in completed:
            if mid not in state.active:
                continue  # 同批重复完成判定(不同事实各推一次)去重
            row = catalog.mission_by_id(mid)
            if row is None:
                continue

            del state.active[mid]
            changed.pop(mid, None)
            mut.delete_active.append(mid)

            dm = DoneMission(
                mission_config_id=mid,
                reward_state=REWARD_STATE_NONE,
                completed_at_ms=now_ms,
            )
            reward_id = catalog.mission_reward_id(row)
            if reward_id > 0:
                if catalog.mission_auto_reward(row) > 0:
                    entry = catalog.build_reward_log(state.player_id, row)
                    if entry is not None:
                        mut.reward_logs.append(entry)
                else:
                    dm.reward_state = REWARD_STATE_CLAIMABLE
            state.done[mid] = dm
            mut.insert_done.append(dm)
            mut.completed.append(mid)

            # 自动接后续链(校验不过跳过该条,不阻断整批)。
            for nid in catalog.mission_next_ids(row):
                nxt = _accept_into(catalog, state, nid)
                if nxt is None:
                    continue
                mut.upsert_active.append(nxt)
                mut.auto_accepted.append(nid)

            # ★ COMPLETE_MISSION 条件**再入下一轮**。
            # 这一步是整个引擎的关键:此时链上新任务已经激活,下一轮它才能
            # 收到"前环已完成"这条事实。当场处理的话新任务还不在 active 里,
            # 事实会被静默丢弃,而且不报错。
            queue.append(
                Fact(
                    category=CONDITION_CATEGORY_COMPLETE_MISSION,
                    slot_values=(mid,),
                    amount=1,
                )
            )

    # 进度有变化且仍活跃的任务 → 突变 + 推送快照。显式排序保证输出确定。
    for mid in sorted(changed):
        am = state.active.get(mid)
        if am is None:
            continue
        mut.upsert_active.append(am)
        mut.progressed.append(mid)

    return mut


def _progress_mission(catalog, row, am: ActiveMission, fact: Fact) -> bool:
    """按一条事实推进一个任务的进度槽。返回是否有推进。"""
    _align_progress_slots(catalog, row, am)
    cond_ids = catalog.mission_condition_ids(row)
    slots = min(len(cond_ids), len(am.progress))

    progressed = False
    for i in range(slots):
        cond = catalog.condition_by_id(cond_ids[i])
        if cond is None:
            continue
        override = catalog.mission_slot_target(row, i)
        # ★ 已达标槽不再累加 —— 否则配置调低目标时会留下超额进度。
        if catalog.condition_is_fulfilled(cond, am.progress[i], override):
            continue
        if catalog.condition_category(cond) != fact.category:
            continue
        if not catalog.condition_matches_slots(cond, fact.slot_values):
            continue
        nxt = saturating_add(am.progress[i], fact.amount)
        am.progress[i] = catalog.condition_clamp(cond, nxt, override)
        progressed = True
    return progressed


def _all_conditions_fulfilled(catalog, row, am: ActiveMission) -> bool:
    """全槽达标判定。

    ★ 按**配置的全部条件槽**判定,槽数不足一律 fail-closed 判未达标。
    原实现取 min 会让热更新增的条件被整段跳过 → **白送完成**。
    """
    cond_ids = catalog.mission_condition_ids(row)
    for i, cid in enumerate(cond_ids):
        if i >= len(am.progress):
            return False  # 进度槽比配置短(未对齐):宁可不完成,不误发奖
        cond = catalog.condition_by_id(cid)
        if cond is None:
            return False  # 条件行被热更删掉:同样宁可不完成
        if not catalog.condition_is_fulfilled(
            cond, am.progress[i], catalog.mission_slot_target(row, i)
        ):
            return False
    return True


def _align_progress_slots(catalog, row, am: ActiveMission) -> None:
    """把进度槽补齐到配置的条件槽数(热更新增条件时)。

    只补不删:配置删条件时多出来的槽留着无害,而截断会让"已完成"的判定
    读到错位的槽。
    """
    need = len(catalog.mission_condition_ids(row))
    if len(am.progress) < need:
        am.progress.extend([0] * (need - len(am.progress)))


def _accept_into(catalog, state: PlayerState, mission_id: int) -> ActiveMission | None:
    """接一个任务进活跃集。已接 / 已完成 / 配置缺失 → 返回 None(跳过不报错)。"""
    if mission_id in state.active or mission_id in state.done:
        return None
    row = catalog.mission_by_id(mission_id)
    if row is None:
        return None
    am = ActiveMission(
        mission_config_id=mission_id,
        progress=[0] * len(catalog.mission_condition_ids(row)),
    )
    state.active[mission_id] = am
    return am
