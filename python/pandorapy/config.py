"""配置模型 —— 与 Go 侧 pkg/config 的 yaml 口径一致,直接读现有的 etc/*.yaml。

设计约束(为什么不重新设计配置格式):
    迁移期间 Go 版和 Python 版会长期并存,同一个服务的两种实现必须能读**同一份**
    etc/xxx-dev.yaml。原因:
      - 21 份 yaml 里有大量运维已经熟悉的字段和注释,重新设计等于让运维背两套
      - deploy/ 下 55 个 K8s manifest 用 ConfigMap 覆盖主配置(gen_cluster_config.ps1
        生成的"集群版"),那套生成器只认现有字段名
      - Envoy / 端口约定(gRPC 2000x / HTTP 2100x)也钉在这些字段上
    所以这里是"照着 Go 结构体抄一份 pydantic 模型",不是设计新格式。

Duration 字段:
    Go 侧 pkg/config.Duration 支持 yaml 里直接写 "60s" / "15m" / "24h"。
    Python 侧用 _parse_duration 解析成 datetime.timedelta,语义对齐。
"""

from __future__ import annotations

import datetime as _dt
import pathlib
import re
from typing import Any

import yaml
from pydantic import BaseModel, Field

# Go 的 time.ParseDuration 支持 ns/us/ms/s/m/h。yaml 里实际只用到 s/m/h,
# 这里把 ms 也收进来(有配置写过 "500ms"),其余按 Go 的单位表补齐。
_DURATION_RE = re.compile(r"(?P<value>\d+(?:\.\d+)?)(?P<unit>ns|us|ms|s|m|h)")
_UNIT_SECONDS = {
    "ns": 1e-9,
    "us": 1e-6,
    "ms": 1e-3,
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
}


def parse_duration(raw: Any) -> _dt.timedelta:
    """把 yaml 里的 "15m" / "1h30m" / 数字秒 解析成 timedelta。

    对应 Go 侧 pkg/config.Duration 的 UnmarshalYAML。空值 → 0,由各服务
    Defaults() 兜底(和 Go 侧一样:零值不代表"无限",代表"用默认")。
    """
    if raw is None or raw == "":
        return _dt.timedelta(0)
    if isinstance(raw, _dt.timedelta):
        return raw
    if isinstance(raw, (int, float)):
        # 裸数字:Go 的 time.Duration 零值语义是纳秒,但 yaml 里从没这么写过;
        # 按秒解释更符合实际配置意图,且这里只在 Python 侧发生。
        return _dt.timedelta(seconds=float(raw))
    text = str(raw).strip()
    matches = list(_DURATION_RE.finditer(text))
    if not matches:
        raise ValueError(f"无法解析 duration: {raw!r}(期望形如 15m / 30s / 1h30m)")
    # 校验整串都被吃掉,避免 "15x" 这种被静默解析成 15 而不报错。
    consumed = "".join(m.group(0) for m in matches)
    if consumed != text:
        raise ValueError(f"duration {raw!r} 含无法识别的部分(已解析 {consumed!r})")
    total = sum(float(m.group("value")) * _UNIT_SECONDS[m.group("unit")] for m in matches)
    return _dt.timedelta(seconds=total)


class GrpcConf(BaseModel):
    """对应 Go 的 pkg/config.Grpc。"""

    network: str = "tcp"
    addr: str = ""
    timeout: str = ""
    # enable_reflection:dev 开(grpcurl 联调),prod 零值 false = 关,少一个攻击面。
    # 与 Go 侧 pkg/grpcserver.MustNewServer 的行为对齐。
    enable_reflection: bool = False
    # max_conn_age:达龄 GOAWAY 让客户端重拨,滚动更新时流量能滚到新副本
    # (zero-downtime §6.2)。grpcio 侧映射到 grpc.max_connection_age_ms。
    max_conn_age: str = ""

    def timeout_td(self) -> _dt.timedelta:
        return parse_duration(self.timeout)

    def max_conn_age_td(self) -> _dt.timedelta:
        return parse_duration(self.max_conn_age)


class HttpConf(BaseModel):
    """对应 Go 的 pkg/config.Http。20 个服务里它只承载 /metrics。"""

    network: str = "tcp"
    addr: str = ""


class ServerConf(BaseModel):
    grpc: GrpcConf = Field(default_factory=GrpcConf)
    http: HttpConf = Field(default_factory=HttpConf)


class NodeConf(BaseModel):
    """对应 Go 的 pkg/config.NodeConfig(只取 Python 侧当前用得到的字段)。

    node_id 是 snowflake 的 node 段,**不是玩家选区**。同一服务的多副本必须各自唯一,
    否则发重号(CLAUDE.md §9 不变量 11)。dev 单副本填 1。
    """

    node_id: int = 0
    session_expire_min: int = 0


class ConfigTableConf(BaseModel):
    """对应 Go 的 pkg/config.ConfigTableConf。dir 指向 active 批次目录。"""

    dir: str = ""


class BaseConf(BaseModel):
    """对应 Go 的 pkg/config.Base —— 各服务私有配置继承它。"""

    server: ServerConf = Field(default_factory=ServerConf)
    node: NodeConf = Field(default_factory=NodeConf)
    config_table: ConfigTableConf = Field(default_factory=ConfigTableConf)

    # 未在 Python 侧建模的段(snowflake / locker / registry / timeouts / cellroute ...)
    # 会落到这里而不是被拒绝。刻意这样:同一份 yaml 要同时喂给 Go 和 Python,
    # Python 侧还没迁到的功能段必须能原样存在,否则 Go 版一改字段 Python 版就起不来。
    model_config = {"extra": "allow"}


def load_yaml(path: str | pathlib.Path) -> dict[str, Any]:
    """读一份 yaml。缺文件 / 解析失败都抛异常,由 main 打 config_load_failed 后退出。

    对应 Go 侧 kconfig.New(file.NewSource(...)).Load() + Scan()。
    """
    p = pathlib.Path(path).resolve()
    with p.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"配置文件根节点必须是 mapping,实际是 {type(data).__name__}: {p}")
    return data
