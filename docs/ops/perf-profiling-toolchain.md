# 压测负载与性能剖析工具链选型(perf-profiling-toolchain)

> 用途:统一说明 **Go 服务 + UE DS** 的压测打流量工具、性能观测工具、以及
> **怎么看到函数级热点(CPU / 内存 / 网络)**。
>
> 关联:`docs/ops/linux-ds-observability.md`(DS 崩溃与性能观测手册)、
> `docs/ops/ue-ds-perf-pitfalls.md`(UE 专服性能陷阱案例:poison malloc / tick≠物理 / 流送时机)、
> `docs/design/stress-discipline.md`(压测纪律)、
> `docs/design/stress-single-cell-client.md`(单 Cell 压测客户端)、
> `robot/stress/`(业务语义压测机器人 stressbot)。
>
> 结论先放前面:
> - **监控 ≠ profiler**。监控(Prometheus/Grafana、netdata)告诉你"什么时候、哪个进程"CPU/内存/网络高;
>   **profiler** 才能告诉你"具体哪个函数"吃 CPU / 分配内存 / 卡帧。要看函数热点必须上 profiler。
> - **不引入 netdata**:它是系统级监控,和已有 Prometheus + Grafana + Loki + Alloy 栈功能重叠,
>   且给不了函数热点。想要秒级系统指标就加 `node_exporter`,别再单起一套监控。

---

## 0. 一张表分清"监控"和"profiler"

| 类别 | 回答的问题 | 能看函数热点? | 本项目选型 |
|---|---|---|---|
| 监控 / metrics(时序) | 什么时候、哪个进程 CPU/内存/网络高 | ❌ 不能 | Prometheus + Grafana(已有);系统级指标加 `node_exporter` |
| 日志 | 发生了什么、报了什么错 | ❌ 不能 | Loki + Alloy(已有) |
| Profiler(采样 / instrument) | 具体哪个**函数**吃 CPU / 分配内存 / 卡帧 | ✅ 能(火焰图) | Go pprof + Pyroscope;UE Unreal Insights;perf / Parca(eBPF) |

> netdata / cAdvisor / node_exporter 都属于第一类,漂亮但看不到函数。
> 决定"能不能看函数热点"的永远是 profiler 这一环。

---

## 1. 压测打流量的工具(负载生成)

| 工具 | 适用 | 说明 |
|---|---|---|
| `robot/stress/stressbot`(自研) | 业务语义压测 | 登录→匹配→战斗→结算等真实链路,首选,业务口径以它为准 |
| **ghz** | 纯 gRPC 基准 | 后端全 gRPC,单接口 QPS / 延迟分布首选 |
| **k6**(Grafana 出品) | 脚本化 gRPC/HTTP | 结果可直接进 Grafana,和现有栈最搭,适合场景化压测 |
| fortio / vegeta / wrk | 轻量 HTTP/gRPC 打点 | 快速单接口基准,按需用 |

> 压测纪律(prev-summary、清空 redis/mysql/etcd/kafka offset/GameServer、至少 3 次 prom snapshot、
> summarize 出二维表)以 `docs/design/stress-discipline.md` 为准,本文件不重复。

---

## 2. Go 服务:CPU / 内存 / 网络 + 函数热点

Go 这块工具链最成熟,函数级热点是标配。

### 2.1 `net/http/pprof`(内置,先接这个)

每个 go 服务挂一个 pprof 端口即可抓函数级 profile:

```bash
# CPU 火焰图(采样 30s)
go tool pprof -http=:9000 http://<svc>:6060/debug/pprof/profile?seconds=30
# 堆内存(哪个函数在分配 / 占内存 → 查内存泄漏)
go tool pprof -http=:9000 http://<svc>:6060/debug/pprof/heap
# 协程 / 阻塞 / 锁竞争
go tool pprof -http=:9000 http://<svc>:6060/debug/pprof/goroutine
go tool pprof -http=:9000 http://<svc>:6060/debug/pprof/mutex
```

落地约定:

- 在 `pkg` 里加一个**默认关闭的 pprof 开关**(feature flag),压测环境打开、生产默认关,
  遵循"默认值不改变现有行为"的接线纪律(参照 snowflake `node_id_source` 开关模式)。
- pprof 端口**不经 Envoy 对客户端暴露**,只在集群内 / 压测环境可达。

### 2.2 Grafana Pyroscope(持续 profiling,推荐)

压测时"一直录",事后按时间段回放火焰图,不用手动去抓那 30 秒。

- 是 Grafana 系,直接塞进现有 Grafana,和 Loki/Prometheus 并排一个界面。
- Alloy 也能采 Pyroscope profile:现有 `deploy/alloy/config.alloy` 目前只采日志,
  可扩一段 `pyroscope.scrape` 采 go 服务的 pprof 端点。

### 2.3 Go runtime metrics 进 Prometheus

GC 停顿、堆大小、goroutine 数、alloc rate 作为"什么时候该去看火焰图"的触发信号,
和 §2.2 火焰图配合定位。

---

## 3. UE DS(C++):CPU / 内存 / 网络 + 函数热点

UE DS 是 C++,工具链和 Go 完全不同。

| 工具 | 能力 | 说明 |
|---|---|---|
| **Unreal Insights**(官方,金标准) | CPU 每帧函数耗时(Timing)、内存(Memory/LLM)、网络(Networking)、帧率 | trace 制,一次录全,能精确到哪个 `Tick`/函数吃满一帧;Linux Release/Shipping DS 也能开 trace |
| `stat` 控制台命令 | 快速粗看 | `stat unit` / `stat game` / `stat memory` / `stat net`,不用工具链 |
| **perf + FlameGraph** | 系统级 C++ 火焰图 | 不想接 UE 专用工具时对 DS 进程直接采样 |
| **Parca / Pyroscope(eBPF 模式)** | 无侵入火焰图 | 不用改 UE 代码、不用重编,一个 agent 能**同时** profile Go 服务和 UE DS,函数热点统一界面看 |

**前提(已在 `linux-ds-observability.md` §3/§5 写明)**:Release DS 必须
`-g -fno-omit-frame-pointer` 保留帧指针 + 符号按 `version/commit` 归档,
否则 profiler 只出一堆地址,看不到函数名。

---

## 4. 落地路线(按现有栈最省事)

1. **监控层**:继续 Prometheus + Grafana + Loki,**不引 netdata**;要系统级秒级指标加 `node_exporter`。
2. **打流量**:业务链路用 `stressbot`;纯 gRPC 接口基准补 **ghz**;场景化脚本压测补 **k6**(结果进 Grafana)。
3. **Go 函数热点**:先接 `net/http/pprof`(零成本、默认关的开关),再上 **Pyroscope**(塞进 Grafana,压测持续录)。
4. **UE DS 函数热点**:深度分析用 **Unreal Insights**(CPU/内存/网络一把梭);
   线上 / 压测长期观测再叠 **Parca eBPF**(顺带把 Go 一起 profile,一个面板看两边)。

```
打流量  →  stressbot / ghz / k6
监控    →  Prometheus + Grafana + Loki(已有)   [看:什么时候哪个进程热]
Go 热点 →  net/http/pprof  →  Pyroscope(Grafana) [看:哪个 Go 函数热]
UE 热点 →  Unreal Insights  +  Parca/perf(eBPF)   [看:哪个 C++ 函数/帧热]
```

---

## 5. 待办 / 接线清单

- [ ] `pkg` 加默认关闭的 pprof 开关(压测启用、生产关),端口不经 Envoy。
- [ ] `deploy/alloy/config.alloy` 扩 `pyroscope.scrape` 采 go 服务 pprof 端点。
- [ ] Grafana 加 Pyroscope 数据源与火焰图面板。
- [ ] UE Linux DS 构建保留 `-g -fno-omit-frame-pointer` 并归档符号(见 `linux-ds-observability.md`)。
- [ ] 压测环境验证过一次 Unreal Insights trace + 一次 Go CPU/heap profile 可正常出火焰图。
