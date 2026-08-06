# [INC-20260806-001][P0] Hub 秒重连后旧 Disconnect 可污染新位置并触发误退队

> **状态**：修复已实现待审核与集成验证（未关闭）  
> **类型**：`availability` / `session-fencing` / `near-miss`  
> **环境**：代码审查；功能默认关闭，未发现线上实际误踢证据  
> **首次发生时间（UTC）**：未发生（上线前 near-miss）  
> **首次发现时间（UTC）**：2026-08-06 13:59:59  
> **负责人**：待指定  
> **受影响服务/版本**：`player_locator`、`pkg/offlinewatch`、`team`，基线 `a94e738a`  
> **最后更新**：2026-08-06

## 0. 一句话结论

`ReportDisconnect` 只有 `(hub_pod, player_id)`，同一 Hub Pod 上旧连接的迟到 Logout 无法与新连接区分；同时 HUB 位置写入与 last-seen 清理分两步且清理失败仍返回成功。功能若按基线启用，秒重连玩家可能被旧请求缩短新位置 TTL、留下陈旧离线时刻，并最终被 `team` 误判为离线退队。该问题在上线前审查发现，未观察到线上事故。

## 1. 影响与范围

- 玩家影响：理论上会把已经重连在线的玩家从队伍移除；locator 短时还会把在线玩家显示为离线。
- 影响人数/对局/请求数：未知；新功能默认关闭，当前没有生产命中证据。
- 服务影响：`player_locator` 的 HUB presence 投影被旧连接污染，`offlinewatch` 可能把污染放大为业务写。
- 数据与安全影响：队伍成员与 player→team 索引可能部分成功漂移；未发现持久角色数据损坏。
- 开始/结束时间：未发生；为上线前 near-miss。
- 是否仍可复发：基线 `a94e738a` 可稳定复现；当前未提交服务端修复的定向单测已封堵，但 UE 接线、race、故障注入、部署与玩家 E2E 尚未完成，不能关闭。
- 严重级别判定理由：若开启会造成在线玩家被自动踢出队伍，符合 `CLAUDE.md §16.9` 的 P0 玩家影响口径。

## 2. 第一现场与证据

### 2.1 症状

- 客户端症状：审查推演为“秒重连成功后，稍后队伍成员被自动移除”；尚无真实客户端样本。
- 服务端症状：旧 `ReportDisconnect` 仍返回 `shrunk=true`，随后写入旧代 last-seen 并发离场事件。
- K8s/Agones 状态：不要求 Pod 切换；重连仍落同一 `hub_pod` 即可触发。

### 2.2 原始证据

```text
基线 a94e738a:
locator.proto: ReportDisconnectRequest 只有 hub_pod + player_id
location.go:   ShrinkHubTTL Lua 只校验 state=HUB + hub_pod
locator.go:    SetLocation(HUB) 成功后再 best-effort ClearLastSeen
offlinewatch:  BatchOnline 后到 Handler 前没有最终在线复核

修复前定向回归:
连接 A 与秒重连 B 使用同一 hub_pod；B 已写 HUB 后再上报 A 的 Disconnect，
基线返回 shrunk=true，位置 TTL 被旧请求缩短，证明 hub_pod 不能充当连接代际。
```

基线测试只覆盖“旧 Pod 迟到”，没有覆盖“同 Pod、旧 admission 迟到”；本轮已把该时序固化为修复后绿测。

### 2.3 已排除的噪声

- MATCHING/BATTLE travel：现有 state 守卫能拒绝，不是本次同 Pod ABA 根因。
- 跨 Pod 重连：现有 `hub_pod` 守卫能拒绝；本事故只需同 Pod 重连即可成立。
- locator 查询失败：gRPC/Redis 整批失败会返回 error 并 fail-closed，不会被压成离线。

## 3. 时间线

以下为可复现的逻辑时间线，不是线上日志；UTC 绝对时间不适用。

| 逻辑时刻 | 组件 | 事件 | 结果 |
|---|---|---|---|
| t0 | Hub 旧连接 A | Admission 成功并写 HUB 位置 | 位置只记录 player/pod，无连接代际 |
| t1 | 客户端 | A 断线后立即建立同 Pod 新连接 B | B 的 `SetLocation(HUB)` 重写位置 |
| t2 | Hub 旧连接 A | 迟到 `ReportDisconnect(player,pod)` | 守卫无法区分 A/B，缩短 B 的位置 TTL并写离线时刻 |
| t3 | locator/offlinewatch | 位置到期且达到业务阈值 | 可能调用 team 自动退队 |

另一个独立窗口：B 写位置成功后进程在 `ClearLastSeen` 前崩溃或 Redis 清理失败，旧离线时刻仍会残留。

## 4. 调用链与关键变量

```text
APandoraHubGameMode::Logout(旧 Controller)
  → UPandoraDSBackendSubsystem::ReportHubDisconnect
  → PlayerLocatorService.ReportDisconnect
  → LocatorUsecase.ReportDisconnect
  → RedisLocationRepo.ShrinkHubTTL(state + hub_pod)
  → SetLastSeen + PlayerLeftHubEvent
  → offlinewatch.Sweep
  → TeamUsecase.OnPlayerOffline
  → TeamRepo.UpdateWithLock
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| `assignment_id + admission_id + admission_seq` | Hub Admission | assignment 绑定 owner 目标；admission 绑定单物理连接，重试固定复用 | 不同连接代可排序 | 基线未透传；修复后作为调用方连接 identity |
| `owner_epoch + operation_id` | owner authority | 每次 owner 迁移单调推进，operation 在整条进场链稳定 | locator 只读、不可由调用方传入 | 修复后的跨 assignment 全局 fence |
| `hub_pod` | Hub DS 实例 | Pod 生命周期 | 同 Pod 多连接共享 | 只能挡跨 Pod 旧报文，挡不住同 Pod ABA |
| locator location key | player_locator Redis | 30s presence TTL | 多请求覆盖 | 被旧连接误缩 TTL |
| last-seen key | player_locator Redis | 1h | 与 location 分键 | HUB 写与清理不能原子提交 |

## 5. 根因

### 5.1 直接根因

1. 断线上报没有连接级 identity；`hub_pod` 不是连接代际，无法对同 Pod 重连进行 fencing。
2. HUB 上线与 last-seen 清理是两个独立 Redis 操作，失败语义把“清理失败”当作成功后的告警。
3. `offlinewatch` 在首次 presence 读取和业务写之间允许重连，且旧 Handler 契约禁止最终复核；业务调用方还能自行拼接 `Inspect + EnqueueDue` 两阶段，容易遗漏安全步骤。

### 5.2 触发条件

- 旧连接 Logout/异步 RPC 晚于新连接 HUB 写到达；或 HUB 写后清理 last-seen 失败/进程退出。
- 离线退队开关已开启，且后续调度项达到阈值。

### 5.3 故障放大因素

- last-seen 保留 1h，陈旧证据跨越多次在线期。
- consumer 与 Redis 调度均为 at-least-once，旧候选可长期存在。
- team 更新与 player→team 索引清理不是一个原子写，失败可能留下残留索引。

### 5.4 为什么现有保护没有挡住

- `hub_pod` 只证明物理实例，不证明物理连接。
- `PEXPIRE LT` 只保证不延长 TTL，不提供 admission fencing。
- 先查在线、再执行 Handler 是跨服务 TOCTOU；重试和幂等只能吸收重复，不能证明玩家仍离线。
- `ClearLastSeen` 的 best-effort 方向并不安全：清理失败会保留可被后续消费的旧证据。

## 6. 全仓同类问题扫描

- 扫描基线 commit：`a94e738a`。
- 扫描目录和文件类型：`proto/pandora/locator`、`player_locator`、`pkg/offlinewatch`、`team`、UE Hub DS 调用端；Go/proto/C++。
- 搜索模式/工具：`rg "ReportDisconnect|SetLocationHub|ClearLastSeen|OnPlayerOffline|DeletePlayerIndexIfMatches"`。
- Confirmed 同型命中：locator 同 Pod 旧 Logout、非原子上线标记、watcher 最终复核、team 索引部分成功；均已补定向修复/回归。
- 结构性隐患：team 自动退队与 matchmaker `StartMatch` 缺共享线性化点；在“不改 matchmaker”约束下需保持 fail-closed 并明确剩余边界。
- 已排除项及理由：login、matchmaker、ds_allocator、hub_allocator 原有主流程不需要修改；它们不生成本次旧连接身份。
- 未覆盖边界：真实 Kafka 重投、Redis Cluster 故障、UE 真机网络乱序尚未注入。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 保持 `team.offline_leave.enabled=false` | 已有默认值 | 基线配置 | 功能不生效，但不会误退队 |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| 复用 Hub Admission 连接 identity，并由 locator 实时 QueryOwner 绑定 `owner_epoch + operation_id` | 服务端已实现；UE 待官方同步/接线 | proto / player_locator / UE Hub DS caller | locator owner/fence 单测通过；UE 未验证 |
| HUB 上线按 owner+Admission 换代，旧代只能 exact 幂等清自己；legacy Disconnect no-op | 已实现待审核 | player_locator data/biz | 同 Pod、跨 Pod/assignment、大整数、乱序与部分失败测试通过 |
| 无事件且无 last-seen 时保持 UNKNOWN，不以第一次 key miss 或本机时钟猜离线起点 | 已实现 | `pkg/offlinewatch` | UNKNOWN/权威证据出现后排期测试通过 |
| `Observe` 统一入口、Handler 前最终复核、条件 claim/finish/retry、`ErrDeferred` 与索引失败收敛 | 已实现待审核 | `pkg/offlinewatch` / `team` | 模块单测通过；跨服务线性化仍由业务 fence 负责 |
| Team 自动退队与 StartMatch 共用 roster fence | 本轮明确不实施 | team / matchmaker | 用户要求不改 matchmaker；`enabled=true` 启动 fail-fast |

### 7.3 防复发规则

- 不新增抽象框架；复用已有 admission identity 和通用 `offlinewatch`。
- 新回归必须覆盖同 Pod 不同 admission 的乱序，以及“读取离线后、Handler 前重连”。
- 发布必须 server-first：先全量升级 locator 并配置/验证 `owner_addr`，再让 Hub/UE 发送 fence。旧 Hub
  过渡期只失去快速缩 TTL；禁止新协议先打到仍执行旧断线语义的旧 locator。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| 同 Pod 旧 admission 迟到断线 | `shrunk=true`，误缩 B 的 TTL | 旧 A no-op；B 的位置/meta 不变 | player_locator 定向单测 | PASS（服务端） |
| owner 不可用/过期/错 target 与跨 assignment 乱序 | 基线无 owner fence | 全部 fail-closed；新 owner 单调胜出 | player_locator biz/data 单测 | PASS |
| HUB validate→location→meta 部分失败/Disconnect 插入 | 基线分步清理可残留旧证据 | 可重试收敛；exact TTL 补偿且不碰更新代 | player_locator biz/data 单测 | PASS |
| Handler 前重连、旧 claim 与并发新离场 | 基线缺最终复核/条件提交 | 在线不动作；旧 attempt 不删除/重排新任务 | offlinewatch 单测 | PASS |
| Team 索引删除部分成功与 match 暂缓 | 可能残留索引/永久丢任务 | compare-delete 重试收敛；`ErrDeferred` 保留 | team 单测 | PASS |
| 受影响模块 build/test/vet | — | 已通过 | locator/offlinewatch/team，详见本轮执行记录 | PASS |
| proto lint + Go 生成 | — | 已通过 | `pwsh tools/scripts/proto_gen.ps1` | PASS |
| `go test -race` | — | CGO 编译前失败：`C compiler "gcc" not found` | 当前 Windows 环境 | BLOCKED（未运行测试） |
| UE 官方 proto 同步 / UE Server 编译 | — | 未执行；修复按要求保持未提交，官方同步要求 server proto 已提交且干净 | `Tool\Build\_GenerateClientProto.bat -UpdateLock` / UBT | BLOCKED / NOT RUN |
| 真实 owner+Redis+Hub/UE 网络乱序、进程崩溃故障注入 | — | 未执行 | 需集成环境 | OPEN |
| 玩家 E2E | — | 未执行 | 秒断秒连并保持队伍 | OPEN |

## 9. 部署、回滚与观察

- 修复 commit：未提交；用户要求修复保持工作区 diff 供 Claude Code 审核。
- 构建产物/镜像 digest：无。
- 部署时间与目标环境：未部署。
- 实际 Pod `imageID` / GameServer provenance：无。
- 回滚条件和步骤：修复未部署；运行时继续保持开关关闭。
- 观察窗口、指标与结果：未开始。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| R-1 | P0 | Claude Code 审核未提交的 locator/offlinewatch/team 修复 | 用户/Claude Code | OPEN | 本 Incident |
| R-2 | P0 | 审核通过并提交 server proto 后，按官方流程同步 UE C++，把 Admission fence 接到 Set/Disconnect | 待指定 | OPEN | 本 Incident |
| R-3 | P1 | 若要启用 team offline leave，设计并实现与 StartMatch 共用的 roster version/operation fence；此前保持 fail-fast | 待指定 | OPEN | 本 Incident |
| R-4 | P1 | 在 Linux/CI 跑 race，并做 Redis/Kafka/owner/进程故障注入 | 待指定 | OPEN | 本 Incident |
| R-5 | P0 | 按 server-first 顺序部署，完成 UE Server 编译、真机秒断秒连和队伍 E2E | 用户 | OPEN | 本 Incident |

## 11. 关闭审核

- [x] 直接根因和放大因素均有代码证据
- [x] 修复前失败、修复后通过的服务端回归存在
- [ ] race/集成/故障注入达到本事故风险要求
- [x] 第一轮同类代码扫描完成
- [ ] 目标环境已加载可追溯的新产物
- [ ] 玩家路径、恢复和补偿路径验证通过
- [ ] 观察窗口无复发
- [ ] 剩余风险已解决或另建 Incident/任务
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭。当前是上线前 near-miss；服务端修复已实现并通过普通测试/编译，
但 Claude 审核、race、UE 官方同步与编译、故障注入、可追溯部署和玩家 E2E 均未完成。
