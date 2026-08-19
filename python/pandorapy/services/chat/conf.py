"""chat 服务私有配置 —— 对应 Go 侧 internal/conf/conf.go。"""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import Field

from pandorapy import config as pconfig, dbguard

DEFAULT_GRPC_ADDR = ":20005"
DEFAULT_HTTP_ADDR = ":21005"


class ChatConf(pconfig.BaseModel):
    """chat 私有段。"""

    # max_content_len 单条消息最大长度,按 **Unicode 码点**计(默认 256)。
    max_content_len: int = 0
    # history_limit PullHistory 单次返回上限(默认 50)。
    history_limit: int = 0
    # team_addr / guild_addr / group_addr:成员解析的 gRPC 直连地址。
    team_addr: str = ""
    guild_addr: str = ""
    group_addr: str = ""
    # sensitive_words 敏感词列表。空 = 不过滤。
    sensitive_words: list[str] = Field(default_factory=list)
    # world_cooldown 世界频道 per-player 冷却(默认 5s)。广播成本 ≈ 速率 × 全服在线数。
    world_cooldown: str = ""
    # non_world_cooldown 非世界频道 per-player per-频道 冷却(默认 1s)。
    non_world_cooldown: str = ""
    # history_retention_days 私聊历史保留期(默认 90 天,§9.24)。
    history_retention_days: int = 0
    sweep_interval: str = ""
    sweep_batch: int = 0
    # retention_mode 留空 = report_only(**只统计不删**)。
    retention_mode: str = ""

    def world_cooldown_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.world_cooldown)

    def non_world_cooldown_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.non_world_cooldown)

    def sweep_interval_td(self) -> _dt.timedelta:
        return pconfig.parse_duration(self.sweep_interval)

    def retention_mode_parsed(self) -> dbguard.Mode:
        """解析清理模式。拼错的值会**抛异常**,由启动期 fail-fast 拦下。"""
        return dbguard.parse_mode(self.retention_mode)


class Config(pconfig.BaseConf):
    chat: ChatConf = Field(default_factory=ChatConf)

    def apply_defaults(self) -> None:
        c = self.chat
        if c.max_content_len <= 0:
            c.max_content_len = 256
        if c.history_limit <= 0:
            c.history_limit = 50
        if c.world_cooldown_td().total_seconds() <= 0:
            c.world_cooldown = "5s"
        if c.non_world_cooldown_td().total_seconds() <= 0:
            c.non_world_cooldown = "1s"
        if c.history_retention_days <= 0:
            c.history_retention_days = 90
        if c.sweep_interval_td().total_seconds() <= 0:
            c.sweep_interval = "1h"
        if c.sweep_batch <= 0:
            c.sweep_batch = 1000
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
