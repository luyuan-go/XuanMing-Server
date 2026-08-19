"""拍卖挂单 / 出价的统一入口 —— 对应 Go 侧 internal/biz/auction.go 的 submit。

流程:PENDING 幂等登记 → 幂等冻结 → 权威撮合 → 激活未成交余量。

★ PENDING 这个中间态**不是多余的**:
    它让进程在「登记后、冻结前」退出时,订单**不会被撮合选中**。
    没有它的话,一个还没冻结资产的订单会被撮合成交 —— 卖家的道具没扣、
    买家的钱没冻,成交却发生了。

★ 三层幂等键,各管一段,**不能共用**:

    订单登记   idempotency_key(uk owner+key)  → 重试不重复挂单
    资产冻结   order_id                        → 重试只确认同一笔冻结
    成交对转   match_id                        → 资产只转一次
    退还残余   order_id                        → 撤单/过期/完全成交后退 escrow

★ 最容易写错的一条(Go 侧有专门注释警告):

    ClaimOrder 只要返回权威快照,就**必须无条件沿用其中的 canonical order_id**。
    `already` 只决定"能不能直接回放终态",**不能**决定后续资产幂等键用哪个 ID。
    用本地新铸的 order_id 去 Freeze 会绕开幂等 —— 同一次重试冻两次资产。
"""

from __future__ import annotations

import dataclasses
import re

from pandorapy import errcode
from pandorapy import log as plog

# 订单状态。
STATUS_PENDING = 0  # 已登记未冻结 —— 撮合**看不到**它
STATUS_ACTIVE = 1
STATUS_FILLED = 2
STATUS_CANCELED = 3
STATUS_EXPIRED = 4
STATUS_REJECTED = 5

_TERMINAL = frozenset({STATUS_FILLED, STATUS_CANCELED, STATUS_EXPIRED, STATUS_REJECTED})

SIDE_SELL = 0
SIDE_BUY = 1

# 幂等键字符集:1..64 个 ASCII [A-Za-z0-9._:-]。
# 限死字符集是因为它会进 uk 索引并出现在日志 / 审计里 —— 放开会引入编码与注入面。
#
# ⚠️ 必须用 `\Z` 而不是 `$`(2026-08-18 被测试抓到的 Python 特有陷阱):
#     Python 的 `$` 匹配"字符串末尾**或末尾换行之前**",于是 "a\n" 会通过校验 ——
#     一个带尾换行的幂等键就此进入 uk 索引和日志行。
#     Go 的 `regexp` 里 `$`(非多行模式)只匹配文本末尾,没有这个行为,
#     所以照抄 Go 的正则会**静默放宽**校验。`\Z` 才是 Python 里的"绝对末尾"。
_IDEM_KEY_RE = re.compile(r"\A[A-Za-z0-9._:-]{1,64}\Z")

# int64 上界(下游 inventory 结算会算 total = quantity * unit_price)。
_MAX_INT64 = 2**63 - 1


def valid_idempotency_key(key: str) -> bool:
    return bool(_IDEM_KEY_RE.match(key or ""))


def is_terminal(status: int) -> bool:
    return status in _TERMINAL


@dataclasses.dataclass(slots=True)
class OrderRecord:
    order_id: int = 0
    market_id: int = 0
    owner_id: int = 0
    side: int = SIDE_SELL
    item_config_id: int = 0
    quantity: int = 0
    filled_quantity: int = 0
    price: int = 0
    status: int = STATUS_PENDING
    release_pending: bool = False
    match_pending: bool = False
    idempotency_key: str = ""
    created_at_ms: int = 0
    updated_at_ms: int = 0


def validate_submit(
    *,
    owner_id: int,
    market_id: int,
    item_config_id: int,
    quantity: int,
    price: int,
    idem_key: str,
    max_quantity: int,
    max_price: int,
) -> None:
    """入口校验。★ 顺序与 Go 一致 —— 频率配额在**更外层**,先于这里。"""
    if owner_id == 0:
        raise errcode.PandoraError(errcode.ErrInvalidArg, "owner required")
    if market_id == 0 or item_config_id == 0:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "market_id / item_config_id required"
        )
    if quantity <= 0 or quantity > max_quantity:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "quantity out of range: %d (max %d)",
            quantity,
            max_quantity,
        )
    if price <= 0 or price > max_price:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg, "price out of range: %d (max %d)", price, max_price
        )
    # ★ 成交总额溢出守卫:即便单值都在上界内,极端组合仍可能溢出 int64。
    # 下游 inventory 会算 total = quantity * unit_price —— 溢出后金额回绕,
    # 可能变成负数或极小值,**不报错**。必须在入口拒。
    if quantity > _MAX_INT64 // price:
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "total value overflow: quantity %d * price %d",
            quantity,
            price,
        )
    if not valid_idempotency_key(idem_key):
        raise errcode.PandoraError(
            errcode.ErrInvalidArg,
            "idempotency_key must be 1..64 ASCII characters [A-Za-z0-9._:-]",
        )


class AuctionSubmitter:
    """挂单 / 出价的统一入口。"""

    __slots__ = ("_repo", "_ledger", "_slots", "_snowflake", "_cfg")

    def __init__(self, repo, ledger, slot_limiter, snowflake, cfg) -> None:
        self._repo = repo
        self._ledger = ledger
        self._slots = slot_limiter
        self._snowflake = snowflake
        self._cfg = cfg

    async def submit(
        self,
        *,
        owner_id: int,
        side: int,
        market_id: int,
        item_config_id: int,
        quantity: int,
        price: int,
        idem_key: str,
        now_ms: int,
    ) -> OrderRecord:
        validate_submit(
            owner_id=owner_id,
            market_id=market_id,
            item_config_id=item_config_id,
            quantity=quantity,
            price=price,
            idem_key=idem_key,
            max_quantity=self._cfg.max_quantity_per_order,
            max_price=self._cfg.max_price,
        )

        rec = OrderRecord(
            order_id=self._snowflake.generate(),
            market_id=market_id,
            owner_id=owner_id,
            side=side,
            item_config_id=item_config_id,
            quantity=quantity,
            price=price,
            status=STATUS_PENDING,
            idempotency_key=idem_key,
            created_at_ms=now_ms,
            updated_at_ms=now_ms,
        )

        existing, already = await self._repo.claim_order(rec)

        # ★ 只要拿到权威快照就**无条件沿用它的 order_id**。
        # already 只决定"能不能直接回放",**不能**决定后续资产幂等键 ——
        # 用本地新铸的 order_id 去 Freeze 会绕开幂等,同一次重试冻两次资产。
        if existing is not None:
            rec = existing

        if already and rec.status != STATUS_PENDING:
            # 已激活 / 已终态 → 直接回放。只有 PENDING 才需要恢复冻结+激活。
            if is_terminal(rec.status):
                await self._release_owner_slot(rec)
            return rec

        # ★ PENDING 已落库**之后**才预留 owner 名额:
        # 这样成员始终能按 market_id + order_id 回查到权威状态。
        # 预留失败必须**条件终态化** —— 不能留下一个绕过配额的可恢复 PENDING。
        try:
            await self._slots.reserve(rec)
        except errcode.PandoraError:
            await self._reject_pending(rec, now_ms)
            raise

        # 冻结。幂等键 = order_id:首次成功后即使在激活前崩溃,同 idem 重试也只确认同一笔。
        try:
            await self._ledger.freeze(
                owner_id=rec.owner_id,
                order_id=rec.order_id,
                side=rec.side,
                item_config_id=rec.item_config_id,
                quantity=rec.quantity,
                price=rec.price,
            )
        except Exception as exc:  # noqa: BLE001
            # ★ 冻结失败的结果**可能包含网络不确定性** —— 也许其实冻成功了只是响应丢了。
            # 所以终态化的同时登记 release_pending,由 Release(幂等键 order_id)
            # 消除"其实冻结成功但响应丢失"造成的永久锁资。
            changed = await self._repo.reject_pending_order(
                rec.market_id, rec.order_id, now_ms
            )
            if changed:
                await self._release_owner_slot(rec)
                await self._try_release(rec)
            raise

        activated = await self._repo.activate_order(rec.market_id, rec.order_id, now_ms)
        if activated is not None:
            rec = activated
        return rec

    async def _reject_pending(self, rec: OrderRecord, now_ms: int) -> None:
        changed = await self._repo.reject_pending_order(rec.market_id, rec.order_id, now_ms)
        if changed:
            await self._release_owner_slot(rec)

    async def _release_owner_slot(self, rec: OrderRecord) -> None:
        try:
            await self._slots.release(rec)
        except Exception as exc:  # noqa: BLE001
            # 名额释放失败只影响该玩家后续能挂多少单,不影响资产正确性 → WARN 不阻断。
            plog.get().warning(
                "auction_release_owner_slot_failed",
                owner_id=rec.owner_id,
                order_id=rec.order_id,
                err=str(exc),
            )

    async def _try_release(self, rec: OrderRecord) -> None:
        """尽力退还 escrow 残余(幂等键 = order_id)。失败留痕,由后台补偿链收敛。"""
        try:
            await self._ledger.release(rec.owner_id, rec.order_id)
        except Exception as exc:  # noqa: BLE001
            plog.get().warning(
                "auction_release_escrow_failed",
                owner_id=rec.owner_id,
                order_id=rec.order_id,
                err=str(exc),
                hint="escrow 残余未退还,由后台 release_pending 补偿链收敛",
            )
