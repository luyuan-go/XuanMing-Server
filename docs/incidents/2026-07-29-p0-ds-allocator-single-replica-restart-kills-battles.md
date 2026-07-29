# [INC-20260729-001][P0] ds_allocator 单副本重启 → Battle DS 20s 授权租约超窗 → 在场玩家被踢

> **状态**：修复已落码，未部署未验证（未关闭）
> **类型**：`availability` / `split-brain 保护副作用`
> **环境**：本机 k8s（Minikube on Docker/WSL2，Windows 11）
> **首次发生时间（UTC）**：2026-07-29 06:13:41
> **首次发现时间（UTC）**：2026-07-29 06:14（玩家侧断线弹窗）
> **负责人**：待指定
> **受影响服务/版本**：`ds_allocator` `eed8ce2c-dirty`，image `pandora/ds-allocator:geed8ce2-p03-da4bf6c7-20260728-062100`，Go 1.26.5
> **最后更新**：2026-07-29

## 0. 一句话结论

节点落盘 I/O 严重卡顿（etcd WAL `fdatasync` 最长 39.4s）导致 `ds_allocator` 丢失 `dsauthfence` capability 并按契约 `os.Exit(1)` 保护性退出；因为它是 `replicas:1 + Recreate`，退出即全服 `DSAllocatorService/Heartbeat` 断流 160s，远超 Battle DS 的 20s 授权租约（`pkg/placement.DSFenceLeaseMaxSeconds`），DS 主动 fencing 踢掉在场玩家。**磁盘卡顿是触发条件，结构性根因是「allocator 的重启预算没有闭合」——任何重启（含例行换镜像升级）都会打断全部进行中的对局。**

## 1. 影响与范围

- 玩家影响：对局中被强制断线，客户端恢复协调器约 2 分 53 秒后重新进入 Hub；期间首个弹窗还因客户端 bug 提前 35 秒误报「已等满 30s」。
- 影响人数/对局：本次 1 名玩家 / 1 局（`match=17798469028741120`）。**但影响面按设计是全量**：allocator 是所有 Battle DS 心跳的唯一后端。
- 服务影响：`ds_allocator` 06:13:53 退出 → 06:16:21 `service_ready`（160s）。同窗口 `hub_allocator`、`player_locator` 亦 `ds_auth_fence_lost` 退出。
- 数据与安全影响：**无数据丢失、无回档、无双写**。DS 侧 fencing 是 fail-closed 的正确行为，撤离/结算链未产生撕裂态。
- 开始/结束时间：06:13:41 ~ 06:16:52（玩家确认 Hub admission）。
- 是否仍可复发：**是**。修复未部署前，任一次 allocator 重启（崩溃、失租、`kubectl apply` 换镜像、节点排空）都会重现。
- 严重级别判定理由：打断全部在场对局 + 违反验收底线 7「任何升级都不得打断对局」+ `CLAUDE.md §16.8`「关键 writer 的租约与重启时间预算必须闭合」不成立。

## 2. 第一现场与证据

### 2.1 症状

- 客户端症状：`Pandora.log` 06:13:59.471Z 收到 `ControlChannelClose`，随后 `ConnectionLost`；同毫秒输出「authoritative entry wait reached its 30s deadline」弹窗（**误报**，见 §5.3）。
- 服务端症状：`ds_allocator` 最后一条 `ds_auth_fence_lost`，容器 `Reason=Error / ExitCode=1 / Signal 空 / Message 空`；previous-container 日志**无** `panic:`、`fatal error:` 或 goroutine 堆栈。
- K8s/Agones 状态：kubelet `Failed to update lease: etcdserver: request timed out`；`etcdctl/mysqladmin/redis-cli` 探针命令超时；`Housekeeping took longer than expected actual="14.671s"`。

### 2.2 原始证据

```text
2026-07-29T06:13:42.399Z caller=ds_allocator/main.go:420 service=ds_allocator
  msg=ds_auth_fence_lost
  hint=立即退出，禁止失租/旧 epoch allocator 继续分配或接收 DS 写回

[Battle DS 06:13:53.153] Battle 心跳启动间隔 15.94s 超过后端 ACTIVE 判弃阈值 15s
[Battle DS 06:13:57.185] HTTP request timed out after 4.00 seconds
                            URL=.../DSAllocatorService/Heartbeat
[Battle DS 06:13:59.401] 授权租约超窗，对存量玩家自我 fencing：
                            authority lease expired: no credential-bound heartbeat ACK within 20s
[Battle DS 06:13:59.402] 授权租约 fencing 完成：kicked=1 orphan_pawns=0 pending_rejected=0

[etcd 06:14:01] slow fdatasync took="39.428572671s" expected-duration="1s"
[etcd 06:15:12] slow fdatasync took="16.851090168s" expected-duration="1s"
[redis  同期  ] Asynchronous AOF fsync is taking too long (disk is busy?)
```

### 2.3 已排除的噪声

| 现象 | 排除理由 |
|---|---|
| Go panic / OOM | previous-container 日志无 `panic:`/`fatal error:`/堆栈；`ExitCode=1` 且 `Signal` 为空（OOMKill 会是 137/`Reason=OOMKilled`）。历史上确有 `concurrent map iteration and map write` 的 allocator panic，那是 INC-20260721-001，本次日志中无该 panic。 |
| `owner_lease_renew_failed_weak` | 弱依赖，`biz/owner_lease.go:43` 明确告警后放行，本身不会让进程退出。 |
| `aqProf.dll` / VTune 加载失败 | UE 启动时探测可选分析器，与本故障无关。 |
| Epic DataRouter `libcurl error 65` | 发生在断线**之后**，目标 `datarouter.ol.epicgames.com`，非 Pandora 链路。 |
| Pod 被 K8s 删除导致断线 | 时序相反：旧 Battle Pod 到 06:17:01 才被删除，断线发生在 06:13:59。 |

## 3. 时间线

服务端日志为 UTC；截图中自定义时间为纽约夏令时（EDT），晚 4 小时。

| UTC | 组件 | 事件 |
|---|---|---|
| 06:06:35 | ds_allocator | Battle GS 分配成功（allocation `6d3bae53-…`，pod `pandora-battle-stable-4vgnp-4w6gr`） |
| 06:06:55 | ds_allocator | DS credential 激活 |
| 06:06:56 | ds_allocator | `battle_ready_after_heartbeat`，对局可进入 |
| 06:07:07 | client | 确认 Battle admission |
| 06:13:07 | ds_allocator | owner 租约双写开始 `DeadlineExceeded` |
| 06:13:23–39 | ds_allocator | 失败累计 streak 3–6，Heartbeat 每次卡约 2s |
| 06:13:35 | hub_allocator | writer lease 本地安全期限过期 |
| 06:13:37 | hub_allocator | `ds_auth_fence_lost` |
| 06:13:39 | player_locator | `ds_auth_fence_lost` |
| 06:13:40 | ds_allocator | 查询两个 Agones Fleet 均 `context deadline exceeded` |
| **06:13:41** | **ds_allocator** | **`ds_auth_fence_lost` → `os.Exit(1)`** |
| 06:13:53 | containerd / Battle DS | 容器结束；DS 检测到 15.94s 心跳间隔 |
| 06:13:57 | Battle DS | Heartbeat HTTP 4s 超时 |
| **06:13:59.401** | **Battle DS** | **20s 授权租约超窗，自我 fencing，kicked=1** |
| 06:13:59.471 | client | `ControlChannelClose` → `ConnectionLost` |
| 06:13:59.476 | client | **误报**「authoritative entry wait reached its 30s deadline」 |
| 06:14:34.766 | client | 本轮真实的 30s 窗口才应到期 |
| 06:14:59 | k8s | allocator 新容器启动 |
| 06:16:05 | ds_allocator | 新进程首条启动日志 |
| 06:16:21 | ds_allocator | `ds_auth_fence_ready` / `service_ready`（距失租 **160s**） |
| 06:16:28 | ds_allocator → DS | 下发 `stop`，DS 请求 Agones Shutdown |
| 06:16:47 / 06:16:52 | client | 取得 Hub 票据 / 确认 Hub admission |
| 06:17:01 | k8s | 删除旧 Battle Pod |
| 06:17:23 | Agones | Fleet 补建的新 Battle GS Ready |

## 4. 调用链与关键变量

```text
节点落盘 I/O 卡顿
  → etcd WAL fdatasync 39.4s
  → dsauthfence capability lease keepalive 中断 / required watch 中断（分支不可辨,见 §10 R-1）
  → Holder.signalLost() → Holder.Lost() 关闭
  → ds_allocator main.go:418 goroutine → os.Exit(1)
  → replicas:1 ⇒ Service ds-allocator 无任何 Ready 端点
  → Battle DS 的 Heartbeat unary 全部超时
  → DS 本地 20s 授权租约(DSFenceLeaseMaxSeconds)到期
  → 对存量玩家自我 fencing → kicked
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享 | 事故中的作用 |
|---|---|---|---|---|
| `fence` (`*dsauthfence.Holder`) | `main.go:403` | 进程级，`Lost()` 一次性 | 是（监控 goroutine） | 失效即触发保护性退出 |
| capability key | etcd `(service, PodUID)` | 每 Pod 一把，异 Pod 不互相接管 | 否 | **此性质使多副本本就合法**，是修复的前提 |
| DS 授权租约 | Battle DS 本地单调时钟 | 每玩家 | 否 | 20s 无 ACK 即 Kick |
| `AuthorityWaitDeadlineSeconds` | UE `UMyDsRecoveryCoordinator` | 应与一轮恢复同生命周期 | 否 | 跨轮残留 → 弹窗提前 35s |

## 5. 根因

### 5.1 直接根因

**结构性根因：`ds_allocator` 的重启恢复时间预算未闭合。**

三个事实相乘即成立，全部有代码/清单证据：

1. `deploy/k8s/services/services.yaml` 中 `ds-allocator` 为 `replicas: 1` + `strategy: Recreate`；
2. `cmd/ds_allocator/main.go` 把**整个进程**门控在 `dsauthfence.AcquireRuntime` 上，失效即 `os.Exit(1)`（该退出本身是正确的 fail-closed，不是缺陷）；
3. Battle DS 端 `DSFenceLeaseMaxSeconds = 20`（`pkg/placement/placement.go:35`），20s 无凭据绑定 ACK 即踢人。

⇒ **恢复最坏耗时（实测 160s）吃光业务安全租约（20s）**，正是 `CLAUDE.md §16.8` 明令禁止的形态。

**触发根因（外部）**：Docker/WSL2 承载的 Minikube 节点内部文件同步/存储 I/O 完成时间严重异常。**证据止于此**——没有宿主机逐进程 CPU/磁盘历史指标，不能进一步断言是物理盘、Docker VHDX、杀毒软件扫描、Windows 进程还是虚机调度暂停；无 OOM、无内核 I/O error、无 Windows 硬件事件。

### 5.2 触发条件

- etcd WAL `fdatasync` 达数十秒 → capability 租约或 required watch 失效。
- 同一时刻 Redis AOF fsync、kubelet housekeeping、Agones API 全部退化，容器重启本身也被拖慢（06:13:53 退出 → 06:16:05 才有首条日志）。

### 5.3 故障放大因素

1. **单副本**：把「一个进程的保护性退出」放大成「全服心跳断流」。
2. **恢复慢**：I/O 卡顿下容器启动 + etcd 重新 acquire + Redis 重连共 160s。
3. **客户端弹窗提前 35s**（独立缺陷）：`UMyDsRecoveryCoordinator::AuthorityWaitDeadlineSeconds` 是绝对时刻，只在 `==0` 时由 `ScheduleAuthorityRetry` 重新武装，而清零只发生在 4 个点中的 2 个——两处「admission 确认」终态（`Operation = FMyDsRecoveryOperation()`）漏了配对归零。上一轮进入战斗时留下的过期 deadline 被本轮断线继承，0ms 即命中。它**不影响掉线本身**，只让玩家更早看到错误措辞。

### 5.4 为什么现有保护没有挡住

| 保护 | 为何无效 |
|---|---|
| `dsauthfence` 保护性退出 | 按设计生效了。问题不在它，而在「退出后无人接手」。 |
| Battle DS 20s fencing | 按设计生效了。DS 无法区分「allocator 挂了」和「我丢了归属」，fail-closed 正确，**不得放宽**。 |
| K8s 自愈重启 | 重启了，但耗时 160s ≫ 20s 租约。 |
| 多副本 | **当时没有**。这正是缺口。 |
| readiness 探针 | 只能摘掉不健康端点，无法在零副本时凭空提供端点。 |
| matchmaker 对 `AllocateBattle` 的重试 | 只覆盖「分配」路径，不覆盖「心跳」路径。 |

## 6. 全仓同类问题扫描

- 扫描基线：本地工作区 HEAD（2026-07-29）。
- 扫描范围：`services/*/cmd/*/main.go` 全部 `dsauthfence.AcquireRuntime` 调用点 + `deploy/k8s/services/services.yaml` 全部 Deployment。
- 搜索模式：`AcquireRuntime|ds_auth_fence_lost`、`replicas:|strategy:`。
- **Confirmed 同型命中**：无第二例。`hub_allocator` 已于 R9 P0-7 迁至 `pkg/dsauthfence/writerlease` 并跑 `RollingUpdate maxUnavailable=0`；`ds_allocator` 是最后一个持有 `Recreate` 例外的写者。
- **结构性隐患（同型但影响较小，未纳入本次修复）**：`login`、`player_locator`、`battle_result` 同样 `AcquireRuntime` + 失租退出。它们的下游没有 20s 级的硬租约，退出窗口只导致可重试失败，不打断对局；按验收底线推论「短暂不可用不违反底线」，**不为它们引入额外机制**。
- 已排除项：`owner`（不持 capability fence）。
- 未覆盖边界：Agones Fleet 侧无同类问题（DS 生命周期由 Agones 管）。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 无（故障自行恢复，全程只读排查） | — | — | — |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| **P0-1** ds-allocator 改 2 副本 + `RollingUpdate maxUnavailable=0` + `PodDisruptionBudget minAvailable=1` + 跨节点反亲和 | 已落码，未部署 | `deploy/k8s/services/services.yaml` | 清单契约测试 3 项 PASS |
| **P0-1a** 心跳超时扫描由 `writerlease` 选举串行化（`election=ds_allocator/sweep`），非 leader 转热备继续服务 RPC | 已落码 | `internal/biz/allocator.go` `SweepWriterLease`/`sweepIsLeader`；`cmd/ds_allocator/main.go` | `internal/biz` 3 项新单测 PASS |
| **P0-1b** 档位开关 `allocator.writer_lease_mode`（enforce/warmup/off，空=enforce），非法值 fail-fast | 已落码 | `internal/conf/conf.go` | `TestResolveWriterLeaseMode` PASS |
| **P0-1c** 机械门禁：`RollingUpdate × mode!=enforce` 启动即退出；受管 k8s 内缺 `PANDORA_DEPLOY_STRATEGY` 亦 fail-closed | 已落码 | `cmd/ds_allocator/main.go` | 清单契约测试钉住 annotation↔strategy↔env 三者一致 |
| **P0-1d** 运维面：`containerPort 51020` 声明 + Service 暴露 + `/healthz/writer` + 6 个 `pandora_ds_allocator_writer_*` 指标 + Grafana critical 告警 | 已落码 | `internal/server/http.go`、`services.yaml`、`deploy/grafana/.../rules.yaml` | YAML 解析校验通过，无重复 uid |
| **P1-1** `ds_auth_fence_lost` 带 `reason`（6 个分支常量），5 个调用点全部打印 | 已落码 | `pkg/dsauthfence/fence.go` + 5 处 main.go | `TestHolderLostReasonIdentifiesBranch` 6 子用例 PASS |
| **P1-2** UE 恢复协调器等待窗口与 `Operation` 严格配对归零 | 已落码，**待用户编译** | `MyDsRecoveryCoordinator.cpp` 两处终态 + `.h` 不变量注释 | UE 编译与实机未跑 |
| **ENV-1** 360 实时监控对 Docker/WSL2/minikube 数据目录加白名单；vhdx 迁至空闲 NVMe | **未做（需用户在宿主机操作）** | — | — |

### 7.3 防复发规则

- 本次未新增 `CLAUDE.md` 条款：`§9.21`（金丝雀/滚动）、`§16.8`（重启预算闭合）、`§16.9`（事故档案）已完整覆盖本事故，属**执行缺口**而非规范缺口。
- 新增机械门禁替代人工纪律：`cmd/ds_allocator/writer_lease_manifest_test.go` 三项契约测试，任何人把 replicas 改回 1、退回 Recreate、去掉 PDB、或让 annotation 与 strategy 漂移，`go test` 直接失败。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| 扫描领导权门单测 | 不存在 | PASS ×3 | `go test ./internal/biz -run HeartbeatSweepTick\|RunHeartbeatSweepInitial` | 热备 0 副作用 / 接任立即恢复 / 让位重新停手 |
| fence 失效原因分支单测 | 不存在（6 分支同形） | PASS ×6 | `go test ./pkg/dsauthfence -run TestHolderLostReason` | 每分支落到唯一常量 |
| 档位解析单测 | 不存在 | PASS | `go test ./internal/conf -run TestResolveWriterLeaseMode` | 非法值 fail-fast |
| 清单契约测试 | 不存在 | PASS ×3 | `go test ./cmd/ds_allocator -run DsAllocator` | 副本/策略/PDB/annotation/端口全钉死 |
| 模块全量回归 | — | 全绿 | `go test ./...`（ds_allocator / dsauthfence） | 无回归 |
| 受影响模块构建 | — | 全绿 | hub_allocator / battle_result / login / player_locator `go build && go vet` | — |
| **`go test -race`** | — | **未执行（阻断）** | 本机无 gcc，`-race` 需 CGO | 须在 Linux/CI 补跑 |
| **滚动升级故障注入** | — | **未执行** | 需真集群：滚动期间持续心跳，断言无 DS fencing | — |
| **玩家 E2E** | — | **未执行** | 对局中滚动升级 allocator，玩家不掉线 | — |
| **UE 编译 / 实机弹窗时机** | — | **未执行** | 交用户 | — |

## 9. 部署、回滚与观察

- 修复 commit：**未提交**（本次会话只落码，未 commit、未构建镜像、未部署）。
- 构建产物/镜像 digest：无。
- ⚠ **首次滚动必须两步走**：旧二进制不参与 `ds_allocator/sweep` 选举，也不受其约束，滚动重叠窗口内会出现无保护的并发扫描者。
  1. 先临时保持 `replicas:1 + strategy:{type:Recreate}` 只换镜像（该跳的秒级重启窗口按验收底线推论视为可接受）；
  2. 新镜像全量后再单独 `apply` `replicas:2 + RollingUpdate`（strategy/replicas 变更不重建 Pod，零额外窗口）。
- 回滚条件与步骤：出现并发扫描导致的误回收（`allocation_sweep_*` 异常）→ 把 `allocator.writer_lease_mode` 置 `off` 并回退 `replicas:1 + Recreate`（两者必须同时改，否则启动期门禁会 fail-closed 拒绝启动，这是有意的）。
- 观察窗口与指标：部署后 24h 观察 `sum(pandora_ds_allocator_writer_held)`（应恒为 1）、`pandora_ds_allocator_writer_campaign_errors`、以及滚动升级期间是否出现 `授权租约超窗` DS 日志。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 |
|---|---|---|---|---|---|
| R-1 | P2 | 本次到底是 keepalive 失租还是 required watch 断，仍不可辨；P1-1 修复后下次才有分支证据 | 待指定 | OPEN | 下次复现时回填 §4 |
| R-2 | P1 | `go test -race` 未跑（本机无 CGO 工具链） | 待指定 | OPEN | Linux/CI 补跑 |
| R-3 | P0 | 滚动升级故障注入 + 玩家 E2E 未跑，**修复有效性尚未在真集群证明** | 待指定 | OPEN | 部署后执行 |
| R-4 | P1 | 宿主机 I/O 卡顿未根治（360 白名单 / vhdx 迁盘未做），随时可再次触发 | 用户 | OPEN | ENV-1 |
| R-5 | P2 | 事故镜像 tag 带 `-dirty`，构建时存在未提交差异，产物可追溯性不足 | 待指定 | OPEN | 发布卫生 |
| R-6 | P2 | 无宿主机逐进程 CPU/磁盘历史指标，I/O 卡顿无法定谳到具体元凶 | 待指定 | OPEN | 常驻采样 |

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据
- [x] 修复前失败、修复后通过的回归存在（清单契约测试与领导权门单测在改动前均不存在且必然失败）
- [ ] race/集成/故障注入达到本事故风险要求（R-2、R-3）
- [x] 同类代码扫描完成
- [ ] 目标环境已加载可追溯的新产物（未构建未部署）
- [ ] 玩家路径、恢复和补偿路径验证通过（R-3）
- [ ] 观察窗口无复发
- [ ] 剩余风险已解决或另建 Incident/任务（R-1~R-6 均 OPEN）
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭。修复已落码并通过静态与单元验证，但**未部署、未在真集群证明**，不得按「服务重新 Ready」宣称已修复。
