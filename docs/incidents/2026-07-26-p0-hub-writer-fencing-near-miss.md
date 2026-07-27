# [INC-20260726-001][P0] Hub allocator 写者租约可续活旧任期且归属补偿失效

> **状态**：修复实施中（未关闭）  
> **类型**：`split-brain` / `data` / `availability` / `near-miss`  
> **环境**：上线前静态审计 + 本机单元测试；未在生产发生  
> **首次发生时间（UTC）**：不适用（上线前 near-miss）  
> **首次发现时间（UTC）**：2026-07-26 10:00:00  
> **负责人**：待指定  
> **受影响服务/版本**：`hub_allocator` / `pkg/dsauthfence/writerlease`，基线 `37e4dc4d9599a65c1357d3efa606a3bbb3189a0b`  
> **最后更新**：2026-07-26

## 0. 一句话结论

上线前复审确认 writer lease 把“`Lost()` 尚未关闭”误当成 keepalive 成功证据，网络分区或长暂停恢复后可能续活已失效 token；assignment 写后补偿又读取“当前 token”而非本次写入 token，失租返回 0 或快速再选到更大 token 时无法撤销旧届写。同 term 删除墓碑没有操作身份还会产生 ABA：迟到补偿可能把更晚删除恢复成旧归属。另有激活超时仅合作式、补偿恢复 TTL 变永久键、`CreateShard` 绕过 writer fence，以及 canonical green strategy/annotation 漂移。上述进程内/普通竞态已整改并有确定性单测；但仓库支持的 Redis Sentinel/Cluster 在主切时仍可能回滚已确认 assignment/fence 写，线性一致 owner authority 尚未全链接通，且真实多副本故障注入、部署产物与观察窗未完成，事故不得关闭。

## 1. 影响与范围

- 玩家影响：若缺陷上线，writer 交接窗口可能留下旧归属、误删/覆盖归属或短暂无 writer；发布链漂移可在 Recreate 删除旧 Pod 后让新 Pod fail-closed，造成登录/进 Hub 暂时不可用。
- 影响人数/对局/请求数：未上线，无实际玩家影响；理论范围为交接窗口内命中 `hub_allocator` 的分配、释放、切线请求。
- 服务影响：`hub_allocator` assignment/容量写者交接与首次 writerlease 发布。
- 数据与安全影响：assignment 是执行细节但参与席位/出票；旧届写残留会破坏单写/fencing 完整性，最终准入仍由 owner/admission 门继续 fail-closed。
- 开始/结束时间：不适用（near-miss）。
- 是否仍可复发：当前工作树已修主要可执行路径；未部署且 crash/Redis 不可达残余仍在，状态保持未关闭。
- 严重级别判定理由：触及 §9.21/§9.22 单写者、脑裂与一人一可玩 DS 硬不变量，按 P0 near-miss 建档。

## 2. 第一现场与证据

### 2.1 症状

- 客户端症状：未在线上观察；理论表现为分配重试、进 Hub 延迟或短时不可用。
- 服务端症状：旧实现可出现 `Current()==(0,false)` 后补偿按 token 0 比较、快速 token 7→9 后旧写误报成功、健康任期仅凭本地 ticker 无限续期。
- K8s/Agones 状态：本次尝试直连本机 k8s Redis 时 Pod 为 `0/1 Running`，端口转发不可达；不作为代码失败证据。

### 2.2 原始证据

```text
基线 writerlease.Current：失主/本地过期返回 (0,false)。
基线 revertAssignmentIfWriterLost：再次调用 Current()，用返回 token 匹配刚写记录。
基线 holdUntilTermEnds：ticker 到点无服务端确认直接 renewHold()。
基线 assignmentTTLFromRecord：真实归属一律返回 0，补偿把 30m TTL 恢复成永久键。
基线 activate：context.WithTimeout 后同步调用 onElected；回调忽略 ctx 时 timeout 无法返回。
canonical green：spec.strategy 改 Recreate，但继承 Pod annotation=RollingUpdate；进程按 annotation fail-closed。
```

### 2.3 已排除的噪声

- `writer_token < current token` 的已有归属不自动视为脏数据：它可能是上届合法留下的当前归属，必须允许本届 CAS 原子接续；仅凭换届删除会回档/踢玩家。
- miniredis 通过不证明真 Redis WATCH 语义；因此保留 env-gated Redis 8 用例，不以默认测试绿替代集成证据。
- 本机 Redis Pod `0/1` 是环境阻断，不作为新代码回归。

## 3. 时间线

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 2026-07-26 10:00 | review | 静态复审确认失租 token、deadline 盲续、TTL 恢复与激活期限缺口 | 当前基线 diff/源码 |
| 2026-07-26 10:30 | writerlease | 改为原子任期快照 + etcd `TimeToLive` 证据续期；失败立即自 fencing/Resign | `writerlease.go` 与确定性交错测试 |
| 2026-07-26 10:40 | hub data | assignment CAS 捕获操作 token/完整 intended/原 PTTL，独立有界 ctx 精确补偿 | `hub_repo.go` 与 data tests |
| 2026-07-26 10:45 | integration | 尝试 Pod port-forward 跑 Redis 8 用例，Pod 0/1 导致端口不可达 | 本次工具输出；未生成通过证据 |

## 4. 调用链与关键变量

```text
etcd Campaign(token=N)
  → OnElected 推进 Redis pod fence
  → Current() 放行 Hub 写
  → CompareAndSwapAssignment WATCH/MULTI/EXEC(writer_token=N)
  → 写后按“本次 N”复核
      ├─ 仍 held 且 token=N：成功
      └─ 失租 / 快速再选 token=M：独立 ctx 精确恢复前值+剩余 PTTL
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| `holdState` | writer 当选后 | 单届任期；原子指针发布 | deadline 可原子前推 | 将 token/Lost/deadline 绑定，防跨届混读 |
| `writtenToken` | assignment 单次 CAS | 单请求/单 attempt | 不共享 | 补偿必须使用它，不能重新猜当前 token |
| `intended` | assignment 单次 CAS | 单请求克隆 | 不变 | 防止同 token 的后写被迟到补偿误删 |
| `previousTTL` | WATCH 内 PTTL | 单请求 | 剩余值随时间递减 | 恢复原记录时保持有限寿命 |
| 请求 `ctx` | gRPC handler | 请求生命周期 | 可取消 | 不得作为提交后补偿唯一 context |

## 5. 根因

### 5.1 直接根因

1. 本地 deadline 的续期没有 etcd 服务端 lease 现行性证据，只检查 `term.Lost()` 尚未关闭；客户端 goroutine 尚未处理断链不等于 lease 仍有效。
2. 补偿丢失 operation-scoped fencing token，事后重读 `Current()`；生产失租返回 0、快速再选返回新 token，均无法精确识别刚写值。
3. 补偿只比较 token，不比较完整 intended；同一届并发后写可能被旧请求误回滚。
4. 补偿猜测真实 assignment 应恢复为永久 TTL，和生产 `assignment_ttl=30m` 不一致。
5. `context.WithTimeout` 无法中断同步且不尊重 ctx 的 Go 回调，注释把合作式取消误写成强期限。
6. 发布器只改 Deployment `spec.strategy`，没有同步 Pod annotation/env，运行时门禁看到错误策略并拒启。
7. 同一 writer term 的所有删除墓碑只有 `(player_id, writer_token)`，A/B 两次删除字节相同；A 的迟到补偿无法区分 B 的更新，形成 tombstone ABA。
8. `CreateShard` 仍直接 `SET NX`，没有在 `{pod}` slot 内与 writer fence 水位同一 EXEC。
9. 持有期 TTL proof 失败会自 fencing，但没有进入 Health 连续失败计数；反复短命任期可长期不稳定却不触发 degraded。

### 5.2 触发条件

- etcd 分区、进程 freeze/长暂停、session keepalive/业务 goroutine 调度交错；或写者快速 token 7→9 再选。
- assignment EXEC 已提交后恰好失租、请求取消或响应不确定。
- 首次 writerlease canonical green 以 Recreate 发布且继承旧 RollingUpdate annotation。

### 5.3 故障放大因素

- assignment key 与 pod fence key 不在同一 Redis Cluster slot，无法和 etcd lease 做跨存储原子提交。
- post-write reconcile 复用请求 ctx，取消后补偿必然失败。
- 单测 fake 在失租时仍返回旧 token，和生产 `Current()` 语义不一致，掩盖缺陷。

### 5.4 为什么现有保护没有挡住

- 入口 `Current()` 只缩小窗口，不能约束已进入 EXEC 的请求。
- assignment 自带 token 只阻止更小 token 覆盖已存在的更大 token；键尚未被继任者触及时仍需写后核验。
- OnElected 推扫保护 pod 同 slot 键，不覆盖每玩家 assignment 的跨 slot/跨存储窗口。

## 6. 全仓同类问题扫描

- 扫描基线 commit：`37e4dc4d9599a65c1357d3efa606a3bbb3189a0b`。
- 扫描目录和文件类型：`pkg/dsauthfence/writerlease/*.go`、`services/battle/hub_allocator/internal/data/*.go`、writer 装配与发布脚本。
- 搜索模式/工具：`rg Current|Lost|writer_token|CompareAndSwapAssignment|TxPipelined|assignmentTTL|OnElected|deploy-strategy`。
- Confirmed 同型命中：`CompareAndSwapAssignment`、`DeleteAssignmentIfPodMatches` 两个 per-player 写入口；均接入同一补偿 helper。
- 结构性隐患：etcd lease 与 Redis assignment 仍非同事务域；owner authority 才是最终线性一致归属权威。
- 已排除项及理由：pod hashtag 域写走 `guardWriterFence`，水位与业务写同一 Redis EXEC；软提示键只影响偏好且有 TTL，不参与准入。
- 未覆盖边界：进程在 EXEC 成功后、执行自检前直接崩溃；补偿回读时 Redis 不可达；真实 Redis Cluster/etcd quorum 多副本故障注入。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 未部署前保持 writer rollout 不宣称关闭 | 执行中 | 本档状态 | 不影响线上（尚未生产） |
| canonical green 同步 strategy annotation/env | 已在主线工作树修复 | `tools/scripts/lib/ds_auth_activation_contract.ps1` + 契约测试 diff | 由主任务统一验证/提交 |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| 任期原子快照 + `Lost` 直检 + 单调 `selfFenced` 终态 | 已落码 | `writerlease.go` | 截止过期后迟到 TTL proof 不得复活同 token 的确定性交错绿 |
| 仅以 etcd `TimeToLive` 成功证据续本地 deadline；proof 锚定请求发出前的单调时刻，失败/过期立即永久 self-fencing/Resign | 已落码 | `Term.RemainingTTL` / `etcdTerm` | 首次证明失败、余量等于 margin、响应后长暂停三类 fake 单测绿；真 etcd 待验 |
| 激活回调强期限外壳 + 单飞；旧 token 推扫只进不退 | 已落码 | `Lease.activate` / `AdvanceWriterFencesForToken` | 忽略 ctx 回调单测 + 水位单调测试绿 |
| assignment 捕获写入 token/完整 intended/原 PTTL | 已落码 | `hub_repo.go` | token=0、7→9、同届后写、PTTL 用例绿 |
| 提交后补偿用独立 3s context | 已落码 | `newAssignmentReconcileContext` | 原请求取消交错绿 |
| 读到未来 token fail-closed | 已落码 | `GetAssignment` | 定向测试绿 |
| 每次删除墓碑复用既有 `assignment_id` 写入 UUID 操作身份；旧空身份墓碑仍可读、wire schema 不变 | 已落码 | `writer_fence.go` | R0→T7(A)→R1→T7(B)→A迟到补偿确定性交错绿 |
| `CreateShard` 初始化与 `{pod}` writer 水位同一 WATCH/MULTI/EXEC | 已落码 | `hub_repo.go` | 落后 token 零写入、当届正常推进测试绿 |
| 持有期 proof 失败累计到 Health；新任期至少完成一轮稳定续证才清零 | 已落码 | `writerlease.go` | 失败跨重选保留、稳定续证清零测试绿 |

### 7.3 防复发规则

- 遵守 `CLAUDE.md §9.21/§9.22` 与 `§16.3/§16.4/§16.8`。
- fencing 补偿必须携带操作时捕获的 token/identity，不得事后从全局 current 猜测。
- 本地 lease deadline 只能由权威服务端确认续期；channel 未关闭不是 ACK。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| writerlease 确定性单测 | fake 未覆盖 token/TTL 真语义 | PASS | `go test -count=1 ./writerlease` | 本次 13s 通过 |
| hub data 确定性交错 | token=0/快速再选/PTTL/取消缺覆盖 | PASS | `go test -count=1 ./internal/data` | 本次 3.4s 通过 |
| hub_allocator 全模块 | 基线 PASS | PASS | `go test ./...` | 本次通过 |
| 真 Redis 8 WATCH/PTTL/7→9/canceled ctx | 未执行 | **受阻/跳过**：本轮未配置 `PANDORA_TEST_REDIS8_ADDR`，本机 `127.0.0.1:6379` 不可达 | `go test ./internal/data -run '^TestRedis8_' -count=1 -v` | 6 条 env-gated 用例均明确 SKIP；待真实环境 |
| 真 etcd 分区/TTL | 未执行 | 未执行 | 多副本故障注入 | OPEN |
| `go test -race` | 未执行 | 未执行 | 支持 CGO 的 Linux/CI | OPEN |
| SIGKILL：EXEC 后自检前崩溃 | 未执行 | 未执行 | 故障注入 | OPEN |
| canonical green 首发演练 | 旧链理论 fail-closed | 未执行 | 本机 k8s 三跳发布 | OPEN |

## 9. 部署、回滚与观察

- 修复 commit：未提交。
- 构建产物/镜像 digest：无。
- 部署时间与目标环境：未部署。
- 实际 Pod `imageID` / GameServer provenance：无。
- 回滚条件和步骤：未部署前无回滚；部署后若 writer degraded、assignment reconcile error 或分配 P99 异常，停止新版本分流并保留旧 writer，按 writer rollout 合约处理。
- 观察窗口、指标与结果：未开始。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| HWF-1 | P0 验收门 | 真 Redis Cluster 跑 WATCH/PTTL/7→9/canceled ctx 用例 | 待指定 | OPEN（本轮地址未配置/本机端口不可达） | 本档 |
| HWF-2 | P0 验收门 | 真 etcd 多副本：分区、lease 到期、快速继任、旧实例恢复 | 待指定 | OPEN | 本档 |
| HWF-3 | P0 验收门 | SIGKILL 在 EXEC 成功→自检之间，确认 owner/继任对账收敛 | 待指定 | OPEN | 本档 |
| HWF-4 | P1 | 补偿回读 Redis 不可达时，stale assignment 仍可能保留到继任者触碰/owner 权威覆盖；补可观测与验收 | 待指定 | OPEN | 本档 |
| HWF-5 | P0 验收门 | canonical green Recreate→enforce→RollingUpdate 演练，核对 annotation/env 与进程门禁 | 待指定 | OPEN | 本档 |
| HWF-6 | P0 验收门 | Linux `-race`、新镜像部署 provenance、观察窗口 | 待指定 | OPEN | 本档 |
| HWF-7 | P0 架构阻断 | Redis Sentinel/Cluster 主切期间异步复制可能回滚已确认 assignment/fence 写；当前未配置可证明 durability 的共识提交。不得用 `WAIT`/`min-replicas` 冒充线性一致，最终迁移到 §9.22 owner authority 同事务域闭环（详见 §10.2） | 待指定 | OPEN | 本档 / `deploy/redis/README.md` 支持模型 / [R13 审核 §8](../reviews/R13-审核-20260726.md#8-架构级单写者闭环分析为什么-redis-ha-层修不了) |

### 10.2 HWF-7 展开：为什么 Redis HA 层无法闭合单写者

#### 不变量前提

本轮全部 fencing 的正确性归结为一条不变量：**水位单调不回退**——`writer_token`、
`generation`、墓碑一旦被 Redis ACK，永不消失、永不变小。证明链：

```text
etcd token 严格递增（线性一致，这半边没问题）
   ↓
Redis 水位只进不退          ← 全部正确性押在这一条
   ↓
迟到旧写者必然读到 ≥ 自己的水位 → 零写入
```

Redis Sentinel/Cluster 是异步复制：主库 ACK 一笔写 → 还没复制出去就挂了 → 副本晋升 →
**这笔已确认的写凭空消失**。水位回退。

后果：被 fence 出局的旧写者重读，看到回退后的旧水位 → 比较通过 → 合法写入 →
**借尸还魂**，且每一步在它自己看来完全合法。应用层无法检测：新主库没有"我丢过写"的
标记，回退后的状态和从未发生过那笔写的状态在字节上不可区分。

#### 三种常见缓解为什么都不能闭合

| 方案 | 为什么不行 |
|---|---|
| **写后回读确认** | 回读发生在主切之前，确认的是"旧主库有这笔写"，证明不了"晋升的副本也有" |
| **`WAIT N`** | 只保证 N 个副本**收到**，不保证故障切换**选中**的恰好是收到的那个；分区期间旧主还能继续收写。成功 ≠ 写在切换后存活 |
| **`min-replicas-to-write`** | 只是主库在副本不足时拒写的门槛，复制仍是异步的；`min-replicas-max-lag` 窗口内旧主照常 ACK。降概率 ≠ 给证明 |

三者共同点：把"大概率不丢"包装成"不丢"。验收底线第 3 条要求完整性证明，概率性缓解
写成关闭就是掩盖——本项 HWF-7 明文禁止这么干。

#### 为什么不能把水位挪到 etcd/MySQL

当前设计的核心价值是**比较与写入在同一原子域**——`guardWriterFence` 的水位比较、
业务写、水位推进落在同一个 Redis `WATCH/MULTI/EXEC` 里，同 slot，没有 check-then-act 窗口。

把水位搬去 etcd/MySQL，业务数据（assignment、席位、容量账本）还在 Redis：

```text
① 查 etcd 水位（通过）→ ② 写 Redis 业务数据
        ↑______这中间失租/被继任______↑
```

跨存储先查后写——正是 §9.22 点名禁止的 TOCTOU，也正是当前设计花力气消掉的东西。

**fencing 的比较点必须和被保护的写在同一个事务域里**。所以要么水位跟着数据走（现状，
被 Redis 复制模型拖累），要么数据跟着水位走（真解）。

#### 真正的修法

唯一正解是后者：**把"归属"这个状态本体搬进线性一致事务域**——§9.22 Owner Authority：
`owner_epoch`、lease 截止、`admit_not_before`、`PENDING→ADMITTED` 在同一个线性一致存储
（TiDB）里完成 CAS，Redis 里的 assignment 降级为非权威投影。

这不是 hub_allocator 的一个 patch——消费方是全链。INC-20260722-002 §10.1 论证了
五条确定性阻断（Login 错误折叠/票据不带 epoch/Battle Begin 弱依赖/取消链缺 Release/
match 级 epoch 不替代每玩家代次），最小不可拆分批次：Login + Hub + Battle + matchmaker +
JWT/proto + UE 入场门 + coordinated rollout。

#### 现状定位

| 态 | 保护 | 缺口 |
|---|---|---|
| 正常态（Redis 不主切） | 进程内 fencing 正确，确定性交错测试覆盖 | 无 |
| 主切窗口 | Admission 会话复核 fail-closed + assignment 非准入最终权威 + TTL 兜底蒸发 | 窗口内一个 EXEC 的回滚可能被旧写者利用；三道外围门限制损害成有界 |
| 终态 | §9.22 Owner Authority（[设计文档](../design/owner-authority.md)），TiDB 同事务域 | 全链接线待独立工作流 |

关联审核文档：[R13 审核 §8](../reviews/R13-审核-20260726.md#8-架构级单写者闭环分析为什么-redis-ha-层修不了)

## 11. 关闭审核

- [x] 直接根因和放大因素均有静态与确定性交错证据
- [x] 修复前失败、修复后通过的回归存在
- [ ] race/集成/故障注入达到本事故风险要求
- [x] 同类代码扫描完成（本次服务/包范围）
- [ ] 目标环境已加载可追溯的新产物
- [ ] 玩家路径、恢复和补偿路径验证通过
- [ ] 观察窗口无复发
- [ ] 剩余风险已解决或另建 Incident/任务
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭。写后自检把普通竞态损害压到一个补偿往返，但**没有消除**两个结构性窗口：进程可在 EXEC 成功后直接崩溃；补偿回读时 Redis 也可能不可达。必须以 owner authority/继任对账和真实故障注入证明最终收敛后才能关闭。
