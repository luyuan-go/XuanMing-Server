# Linux DS 崩溃与性能观测手册

> 用途:线上 / 测试服 k8s 集群里跑 UE Linux Dedicated Server(DS)时,统一说明
> **崩溃怎么看、Release DS 怎么还原堆栈、性能指标怎么看**。
>
> 结论先放前面:线上跑 **Linux Release / Shipping DS 是对的**,但必须同时保留
> crash dump、debug symbols、日志和 metrics。不要只靠 ssh 进 Linux 机器看进程。
>
> 姊妹篇:`docs/ops/ue-ds-perf-pitfalls.md`(UE 专服性能陷阱与优化手册:poison malloc、
> 挂起检测阈值、stdout 缓冲、tick≠物理、流送时机等真实案例的坑/因/修/验)。

---

## 0. 排障顺序

1. 先看 k8s / Agones 状态:Pod 是否重启、退出码是什么、GameServer 是否 Ready / Allocated。
2. 再看 DS 进程日志:当前日志 + `--previous` 上一次崩溃前日志。
3. 再看 crash 产物:core / minidump / UE crash report,用同版本符号还原调用栈。
4. 性能问题看 metrics / profiler,不要只看 `top`。
5. 一旦确认崩溃或 P0,立即从 [`docs/incidents/template.md`](../incidents/template.md) 建档并登记 [`docs/incidents/index.md`](../incidents/index.md);先保全证据再清理/重启,后续持续更新到关闭。

> 经验法则:如果只能看到 "Pod 重启了",说明观测链路还没建完;至少要能拿到
> `版本号 + pod + exit code + 崩溃堆栈 + 崩溃前日志 + 当时核心指标`。
>
> 取证手册回答“怎么查”,事故档案回答“这一次发生了什么以及是否真正关闭”。任何 runtime fatal、OOM、CrashLoop、非预期进程退出或 P0 都必须有独立 Incident;不得只在聊天记录、设计文档或 `PROGRESS.md` 留一句结论。原始日志含 JWT/DSTicket/Secret 时只记录受控位置和脱敏片段,严禁把凭证写入仓库。

---

## 1. k8s / Agones 第一现场

先确认命名空间,本项目业务默认是 `pandora`,Agones 控制面默认是 `agones-system`:

```bash
kubectl get pod -n pandora -o wide
kubectl get gameservers -n pandora -L agones.dev/fleet,pandora.dev/region
kubectl get fleet -n pandora
kubectl get events -n pandora --sort-by=.lastTimestamp
```

定位具体 Pod 后:

```bash
kubectl describe pod <pod> -n pandora
kubectl logs <pod> -n pandora
kubectl logs <pod> -n pandora --previous
```

重点看这些状态:

| 现象 | 常见含义 | 下一步 |
|---|---|---|
| `CrashLoopBackOff` | DS 反复崩溃 / 启动失败 | 看 `--previous` 日志 + crash dump |
| `OOMKilled` | 超过内存 limit 被 kubelet 杀 | 看内存曲线、limit、峰值分配 |
| `ExitCode 139` | 常见 `SIGSEGV` 段错误 | 用 core/minidump + 符号还原堆栈 |
| `ExitCode 134` | 常见 `abort` / assert | 找 assert 日志和调用栈 |
| `ExitCode 137` | `SIGKILL`,常见 OOM 或强杀 | 对照 events / node 压力 |
| `ExitCode 143` | `SIGTERM`,常见滚动更新 / 缩容 / 探针 | 对照 Deployment/Fleet 操作和 events |
| GameServer 非 Ready | Agones SDK health / DS 启动未就绪 | 看 GameServer events 和 DS 启动日志 |
| Allocated 后无心跳 | DS 被分配但业务心跳没起来 | 看 DS 回调地址、Envoy DS 面、allocator 日志 |

> `kubectl logs --previous` 很关键:容器已经重启后,当前日志可能只剩新进程启动日志,
> 上一次崩溃前的输出要从 `--previous` 拿。

---

## 2. DS 必须记录什么

每个 DS 启动首行日志必须带版本信息,方便从线上 pod 反查源码和符号:

```text
version=v1.2.3 commit=abc1234 build_time=2026-06-23T10:00:00Z ds_role=battle pod=pandora-battle-xxx
```

DS 日志至少要包含:

- DS 类型:`hub` / `battle`
- `pod_name` / `game_server_name`
- `match_id` / `shard_id`
- 监听地址、回调 `DsGatewayAddr`
- Agones Ready / Allocated / Shutdown 关键状态
- 业务心跳成功 / 失败
- 玩家进入 / 离开 / 结算关键事件
- crash 前最后 N 条关键日志

崩溃产物不要只存在容器本地临时目录。Pod 被重建后本地文件可能消失,推荐:

- DS 崩溃时生成 minidump / UE crash report。
- crash 产物命名带 `version + commit + pod + match_id/shard_id + timestamp`。
- 用 sidecar、启动脚本或 DS 自身把 crash 产物上传到对象存储 / 日志平台 / 崩溃平台。
- crash 产物与 debug symbols 按同一个版本号归档。

---

## 3. Release DS 与符号归档

线上跑 Release / Shipping DS 没问题,但必须能回溯堆栈。推荐构建参数保留可 profiler 的栈:

```bash
-O2 -g -fno-omit-frame-pointer
```

发布产物拆成两份:

1. 线上镜像:Release / Shipping DS 可执行文件,可以 strip。
2. 符号归档:同版本 debug symbols,不要丢,按 `version/commit` 存起来。

C/C++ 常见拆符号方式:

```bash
objcopy --only-keep-debug PandoraServer PandoraServer.debug
strip --strip-debug PandoraServer
objcopy --add-gnu-debuglink=PandoraServer.debug PandoraServer
```

如果使用 UE Dedicated Server,同样要把对应 Linux Server 包的符号文件单独归档。
符号文件和 DS 镜像必须一一对应,否则堆栈地址会对不上。

---

## 4. 崩溃堆栈怎么还原

如果拿到 core:

```bash
gdb ./PandoraServer core
(gdb) bt full
(gdb) thread apply all bt
```

如果是 minidump / UE crash report,按 UE CrashReportClient 或内部崩溃平台的符号化流程处理。
关键是 crash 产物、可执行文件、debug symbols 三者必须来自同一个版本。

k8s 里直接靠系统 core dump 有几个坑:

- `ulimit -c`、`core_pattern`、容器权限和写入路径都可能影响 core 生成。
- core 文件可能很大,不适合长时间留在容器层。
- Pod 重建后本地 core 可能丢失。

所以线上更推荐 DS 自己产出 minidump / crash report,并自动上传。
core dump 适合作为测试服 / 压测环境的补充手段。

---

## 5. 性能看什么

粗粒度先看:

```bash
kubectl top pod -n pandora
kubectl top node
```

但 DS 性能不能只看 CPU / 内存。DS 自己要暴露业务 metrics,接 Prometheus / Grafana:

| 指标 | 说明 |
|---|---|
| `tick_cost_ms` / `frame_cost_ms` | 单帧 / 单 tick 耗时,p95/p99 必看 |
| `online_players` | 当前在线玩家数 |
| `rooms` / `matches` / `entities` | 房间、战斗、实体数量 |
| `net_in_bytes` / `net_out_bytes` | 网络吞吐 |
| `net_in_packets` / `net_out_packets` | 包量,比字节数更容易暴露小包风暴 |
| `rpc_callback_latency_ms` | DS 回调 allocator / locator / battle_result 延迟 |
| `heartbeat_success` / `heartbeat_fail` | DS 业务心跳是否稳定 |
| `memory_rss_bytes` | 常驻内存 |
| `alloc_rate_bytes` | 分配速率,用于判断内存抖动 |
| `gc_time_ms` | 如有 GC / 托管运行时,看 GC 停顿 |

Profiler 方向:

- CPU 热点:Linux `perf`,或 Parca / Pyroscope 这类持续 profiler。
- 内存增长:heap profiler、jemalloc/tcmalloc profile、UE 内存统计。
- 网络问题:DS 自身包量指标 + `tcpdump` / eBPF 按需抓样。
- 卡顿问题:按 `match_id` / `shard_id` 把 p95/p99 tick cost 打出来,不要只看平均值。

> Release DS 也能 profile。关键是构建时保留 frame pointer 和符号归档,
> 否则 profiler 只能看到一堆地址,很难定位真实函数。

> 压测打流量工具、Go/UE DS 的函数热点(火焰图)工具选型与落地路线,
> 见 `docs/ops/perf-profiling-toolchain.md`。

---

## 6. 上线前检查项

- [ ] DS 镜像 tag、启动日志版本号、git tag 三者一致。
- [ ] 每次发布的 DS debug symbols 已归档。
- [ ] DS 崩溃会生成 minidump / crash report,并能自动上传。
- [ ] `kubectl logs --previous` 能看到崩溃前日志。
- [ ] Pod 退出码 / reason 能被日志平台或告警系统采集。
- [ ] Prometheus 能采到 DS 进程指标和业务指标。
- [ ] Grafana 有 DS 总览:在线、tick p95/p99、CPU、内存、网络、心跳、崩溃次数。
- [ ] 压测环境验证过一次故意 crash,确认能拿到堆栈。
- [ ] 压测环境验证过一次 OOM / limit 触发,确认能分辨 `OOMKilled` 与普通崩溃。

---

## 7. 常用命令速查

```bash
# 看 pod 状态
kubectl get pod -n pandora -o wide

# 看 DS 所属 Agones 状态
kubectl get gameservers -n pandora -L agones.dev/fleet,pandora.dev/region

# 看某个 pod 详细事件和退出原因
kubectl describe pod <pod> -n pandora

# 当前进程日志
kubectl logs <pod> -n pandora

# 上一次崩溃前日志
kubectl logs <pod> -n pandora --previous

# 按时间看事件
kubectl get events -n pandora --sort-by=.lastTimestamp

# 粗看资源
kubectl top pod -n pandora
kubectl top node
```
