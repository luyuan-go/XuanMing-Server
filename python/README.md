# Pandora Python 后端(strangler 迁移，与 Go 版并存)

本目录是 Go 后端向 Python 迁移的**第 0 批地基 + 第 1 批首个服务**。分支 `python-migration`。

设计前提：**不是替换，是并存**。同一份 `.proto`、同一份 `etc/*.yaml`、同一套 Envoy 路由、同一套
Grafana/Loki/Alloy/Prometheus。Python 服务与 Go 服务在协议层同构，Envoy 不知道后面是哪个语言，
所以可以按服务逐个灰度、逐个回滚。

## 已验证事实（不是"应该能"，是实测）

| 项 | 结果 |
|---|---|
| 60 个 `.proto` 生成 Python stub | 120 个 `_pb2.py` + 60 个 `.pyi`，**逐个 import 全通过** |
| 自定义 extension（`excel.proto` / `proto2mysql`） | 生成并可读（读回 option 值需重解析，见下文已知问题） |
| `push.Subscribe` 流式方法 | 生成成功（本轮未实现服务端） |
| **Go 版 vs Python 版响应逐行 diff** | **10 组固定请求，零差异** |
| 日志字段口径 | `ts`/`level`/`caller`/`service`/`msg` 与 Go 实测输出一致 |
| 测试 | 50 passed |

响应对比用的是真实 `configtable/dist`（version `20260818003`），覆盖：正常开对话、终止节点、
不存在的 NPC、零值参数、无身份头、越权访问、非法 option、幂等 EndDialogue。

## 目录

```
python/
  pyproject.toml            两个包根：. → pandorapy，gen → pandora/proto2mysql
  pandorapy/
    _utf8.py                强制 UTF-8 I/O（Windows cp1252 会丢整条中文日志）
    log.py                  structlog，字段口径逐字对齐 pkg/log 的 zap
    errcode.py              ★ 由 tools/gen_errcode.py 从 Go 源码生成，勿手改
    config.py               pydantic 模型，读现有 etc/*.yaml
    configtable.py          manifest + sha256 + 整批 fail-closed
    snowflake.py            位布局逐位对齐 pkg/snowflake（秒级，非毫秒）
    metrics.py              prometheus_client
    interceptors.py         grpcio 拦截器：身份提取 + 指标 + panic 兜底
    server.py               grpcio + FastAPI 双 server（并联，非串联）
    godur.py                Go time.Duration.String() 格式
    services/dialogue/      biz / data / service / conf / main
  tools/gen_errcode.py      errcode 生成器（支持 --check 做 CI 门）
  gen/                      buf 生成产物（pandora/ + proto2mysql/）
  tests/                    50 个测试，含跨语言 parity 门
```

## 跑起来

```bash
cd python && uv venv --python 3.13 && uv pip install -e ".[dev]"
```

```bash
cd proto && buf generate --template buf.gen.python.yaml
```

启动（**必须在服务目录下**，`config_table.dir` 相对进程工作目录，与 Go 版同一契约）：

```bash
cd services/social/dialogue && PYTHONUTF8=1 ../../../python/.venv/Scripts/python.exe -m pandorapy.services.dialogue.main -conf etc/dialogue-dev.yaml
```

```bash
cd python && .venv/Scripts/python.exe -m pytest tests/ -q
```

## 迁移中发现的真实差异（都已修，逐条记在代码注释里）

这些全部是"**不报错、只静默出错**"的类型，是迁移的主要风险来源：

1. **`level` 词表不同** —— zap 是 `warn`/`fatal`，Python logging 是 `warning`/`critical`。
   而 `deploy/alloy/config.alloy` 把 `level` **直接提成 Loki label**，所以按 `{level="warn"}`
   过滤的面板会静默漏掉 Python 侧全部警告。→ `log.py` 的 `_ZAP_LEVEL_NAMES`
2. **服务名字段是 `service` 不是 `logger`** —— Go 侧 zap 的 `NameKey` 虽配成 `logger` 但从未使用，
   服务名是 `pkg/log/log.go:70` 以普通字段加的。且**必须进程级绑定**：最初绑在 `setup()` 返回的
   logger 上，导致 `biz.py` 用 `plog.get()` 打的行全都没有 `service`
3. **`msg` vs `event`** —— structlog 默认字段名是 `event`，1449 个事件名的 LogQL 全靠 `msg`
4. **Windows stdout 是 cp1252** —— 日志含中文直接抛 `UnicodeEncodeError` 并**丢掉整条**；
   若发生在 `except` 分支会盖掉真正的故障
5. **grpcio 不接受裸端口 `:20013`** —— Go 的 `net.Listen` 接受。21 份 yaml 全是裸端口形式
6. **uvicorn `host="::"` 在 Windows 只绑 IPv6** —— grpcio 的 `[::]` 是双栈。IPv6-only 会让
   Prometheus 抓不到 `/metrics` → 面板静默变空
7. **uvicorn/grpcio 的日志是纯文本** —— 不是 JSON，Alloy 的 `stage.json` 解析不了。
   已把 stdlib logging 接进同一条渲染链
8. **`google.api` 需要单独装包** —— Go 侧由 `genproto` 提供，Python 侧要 `googleapis-common-protos`
9. **时长格式** —— Go 打 `time.Duration.String()`（5 分钟 = `"5m0s"`），不是 yaml 原值 `"5m"`

## 顺带发现的 Go 侧既有问题（未改动 Go 代码）

**Go 日志有重复的 `msg` 键**：

```json
{"level":"info","ts":"...","caller":"dialogue/main.go:63","msg":"","service":"dialogue","msg":"service_starting","conf":"..."}
```

zap 的 `MessageKey="msg"` 写了个空消息，`Infow("msg", "service_starting")` 又加了一个同名字段。
JSON 重复键的取值取决于解析器 —— Alloy/Loki 取最后一个，所以线上恰好正常。但任何取第一个的
解析器都会拿到空字符串。Python 侧不复制这个缺陷（只输出一个 `msg`，等价于 Go 的有效值）。

## 明确未做的部分

按 `CLAUDE.md §14`，这里如实列出，不留"以后再接"的空壳：

| 未做 | 原因 | 影响 |
|---|---|---|
| `cellroute`（region/cell 路由） | 依赖 etcd，Python 的 etcd 客户端是本次迁移唯一高风险项 | 接缝已留（`set_cell_router`，None-safe），行为与 Go 侧单 Cell（router=nil）**完全一致** |
| snowflake `node_id_source=etcd` 抢占 | 同上 | 只有 static 模式。**多副本部署前必须先补**，否则重号（§9 不变量 11） |
| Redis 版 SessionStore | Go 版同阶段也只有内存版 | 内存会话不跨实例、重启即丢，与 Go 版限制相同 |
| `push.Subscribe` 流式服务端 | 全项目唯一的流，单独迁 | 不影响 dialogue |
| `login` 的 10 个 REST 端点 | 属第 3 批 | 20 个服务的 FastAPI 只需一行 `/metrics` |
| Sentry SDK 接入 | 建议加，补 Python 运行期异常聚合 | 目前未捕获异常靠 `ObservabilityInterceptor` 打结构化日志 + 计数 |

## etcd 这一项必须先验（决定后续能否继续）

Python 侧 etcd v3 客户端生态的天花板是 `kragniz/python-etcd3`（450★，**2024-12 起 20 个月未更新，
202 个 open issue**）；还在维护的是 `martyanov/aetcd`（33★）。而 `pkg/dsauthfence`(4299 行)、
`pkg/leader/etcdleader`、`pkg/snowflake/etcdnode`、`pkg/cellroute` 全压在 lease keepalive +
watch 断线重连 + txn CAS 这三个语义上。

**建议下一步不是继续迁服务，而是先做 etcd 验证**：拿 `pkg/leader/etcdleader` 那 184 行做样本，
用候选客户端（或用 etcd 官方 `.proto` 直接 `grpcio-tools` 生成客户端，绕开社区库）实现一遍
选主 + lease 续约，然后**拔网线 / kill etcd 节点**，验证：

- lease 过期后旧 leader 是否真收到通知并让位
- watch 断线重连后事件有没有丢
- txn CAS 在并发下是否真互斥

这一项过不了，全量迁 Python 应重新评估；过了，剩下的都是工作量。

## 灰度接线（尚未做）

Envoy 侧零改动即可切流量 —— Python 版监听同样的 `:20013`，说同样的 h2c gRPC。
灰度做法是在 `deploy/envoy/envoy.yaml` 给 `dialogue_cluster` 加第二个 endpoint 并配权重。
`service_ready` 日志里已加 `runtime: "python"` 字段，供 Grafana 区分两个实现的指标与日志。

`run_services.ps1` 尚未接 Python 启动路径 —— 需要时加一个 `-Runtime python` 开关，
按服务选择跑 Go exe 还是 `python -m`。
