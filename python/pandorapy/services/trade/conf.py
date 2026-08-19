"""trade 服务私有配置 —— 对应 Go 侧 internal/conf/conf.go。

读同一份 services/economy/trade/etc/trade-dev.yaml,默认值逐个与 Go 侧对齐。
"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import Field

from pandorapy import config as pconfig

DEFAULT_GRPC_ADDR = ":20012"
DEFAULT_HTTP_ADDR = ":21012"


class TradeConf(pconfig.BaseModel):
    """trade 私有段。"""

    # order_ttl 订单 Redis key 存活时长(默认 10m)。应 > order_expire,
    # 给已结算 / 已取消订单留一段查询窗口(ListMyOrders 客户端回看)。
    order_ttl: str = ""
    # order_expire 订单从创建到自动过期的时长(默认 5m)。
    # 超时未完成两阶段确认的订单在被访问时惰性置 EXPIRED。
    order_expire: str = ""
    # optimistic_retry WATCH/MULTI/EXEC 乐观锁最大重试次数(默认 3)。耗尽 → ErrTradeLockFailed。
    optimistic_retry: int = 0
    # max_items_per_order 单订单最大物品条目数(默认 20)。
    max_items_per_order: int = 0
    # rate_quota_per_min 下单/撤单的 per-player 每分钟频率配额(默认 20;负值 = 关闭)。
    # 与 max_orders_per_player 总量闸正交:总量限「同时挂多少」,本值限「刷多快」。
    rate_quota_per_min: int = 0
    # max_orders_per_player 单玩家同时参与的订单总数上限(默认 200,不变量 §18)。
    max_orders_per_player: int = 0
    # inventory_addr inventory 服务 gRPC 直连地址。配置后走真实 P2P 原子对转。
    inventory_addr: str = ""
    # allow_noop_ledger 显式允许退回 NoopResourceLedger(结算永远成功、不真实扣转)。
    # 默认 False:未接真实账本即 fail-fast,防止生产漏配后仍以「成交不扣减」静默启动。
    allow_noop_ledger: bool = False

    def order_ttl_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.order_ttl)

    def order_expire_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.order_expire)


class Config(pconfig.BaseConf):
    """trade 完整配置。"""

    trade: TradeConf = Field(default_factory=TradeConf)

    def apply_defaults(self) -> None:
        """默认值必须与 Go 侧 Defaults() 逐个相同 —— 端口尤其重要(Envoy cluster 钉在上面)。"""
        t = self.trade
        if t.order_ttl_td().total_seconds() <= 0:
            t.order_ttl = "10m"
        if t.order_expire_td().total_seconds() <= 0:
            t.order_expire = "5m"
        if t.optimistic_retry <= 0:
            t.optimistic_retry = 3
        if t.max_items_per_order <= 0:
            t.max_items_per_order = 20
        if t.rate_quota_per_min == 0:
            t.rate_quota_per_min = 20
        if t.max_orders_per_player <= 0:
            t.max_orders_per_player = 200
        if not self.server.grpc.addr:
            self.server.grpc.addr = DEFAULT_GRPC_ADDR
        if not self.server.http.addr:
            self.server.http.addr = DEFAULT_HTTP_ADDR

    @classmethod
    def load(cls, path: str) -> "Config":
        raw: dict[str, Any] = pconfig.load_yaml(path)
        cfg = cls.model_validate(raw)
        cfg.apply_defaults()
        return cfg
