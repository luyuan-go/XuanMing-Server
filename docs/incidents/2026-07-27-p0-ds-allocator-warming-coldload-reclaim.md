# [INC-20260727-001][P0] Artic01 冷加载期间 warming DS 被心跳 sweep 提前回收，玩家无法进场

> **状态**：已部署但验证失败（第二 P0 根因已修复待重新部署，未关闭）  
> **类型**：`availability`  
> **环境**：本机 k8s（minikube + Agones，dev 全链路）  
> **首次发生时间（UTC）**：2026-07-27 13:40:54（首个可证的误删）  
> **首次发现时间（UTC）**：2026-07-27（用户重复匹配 map_id=8 无法进场后定位）  
> **负责人**：luhailong  
> **受影响服务/版本**：ds_allocator（git de19c92c 及之前）、UE Battle DS 镜像 `pandora/battle-ds:dev`（r1553 之前构建）、Fleet `pandora-battle-stable`/`-canary`  
> **最后更新**：2026-07-27

## 0. 一句话结论

`map_id=8`（Artic01，World Partition 大图）从 Agones Allocated 到 Battle GameMode BeginPlay 首次业务心跳约需 28s，而 `ds_allocator` 的 stale sweep 对**所有状态**统一使用 `heartbeat_timeout=15s`，warming（尚未激活、按设计无业务心跳）DS 在冷加载中即被判失联删除；`ready_wait_timeout` 放宽到 120s 也会被 15s sweep 提前击穿，玩家反复匹配反复触发分配→删除循环，始终无法进场。属设计缺陷（单阈值同时监管启动与稳态，k8s 引入 startupProbe 所修的同款反模式），非 UDP/地图配置/OOM/Crash/Agones Health 问题。

## 1. 影响与范围

- 玩家影响：匹配 `map_id=8` 的玩家永远拿不到 `ds_addr`，无法进入 Artic01；每次尝试都消耗并销毁一个 GameServer。
- 影响人数/对局/请求数：dev 环境单人测试，多次匹配全部失败。
- 服务影响：battle fleet 反复 Allocated→Deleted churn；matchmaker 每次等满 `ds_allocate_timeout` 失败。
- 数据与安全影响：无数据丢失/回档；分配记录经 fenced 回收链清理，无泄漏（符合底线 2/3/4）。
- 开始/结束时间：2026-07-27 首测 Artic01 起持续存在；代码修复完成日仍未部署（见 §9）。
- 是否仍可复发：**是**——旧 `ds-allocator` 二进制仍在跑，继续测试仍会复现（已通知暂停点匹配）。
- 严重级别判定理由：玩家无法进场（index §1「无法登录/匹配/进场」），且每次重试破坏性消耗 GameServer。

## 2. 第一现场与证据

### 2.1 症状

- 客户端症状：匹配后长期停留等待，从未收到 `ds_addr`（尚未进入 UDP 连接阶段）。
- 服务端症状：allocator 在 DS 加载中发起 GameServer 删除；日志出现 ready 前的 `preactive_release_unconfirmed`；matchmaker 无 `match_ready`。
- K8s/Agones 状态：GameServer Allocated 后 ~21s 被 allocator DELETE；Pod 随之删除；Agones Health 本身未判 Unhealthy（threshold 已临时抬到 12）。

### 2.2 原始证据

代表性时间线（UTC，用户在事故现场收集）：

```text
13:40:33  GameServer Allocated，battle 记录进入 warming
13:40:33  DS 执行 ServerTravel(Artic01)
13:40:54  allocator sweep 开始删除该 GameServer（分配后约 21s = 15s 阈值 + sweep 周期）
13:40:57  Pod DELETE
13:41:01  Artic01 进入 Battle GameMode BeginPlay，首次业务心跳启动——Pod 已死
```

关联事实：Artic01 Windows 编辑器暖载实测 17.9s；Linux cooked 冷 IO 首载约 28s（本次时间线 33→01 即 28s）。

**证据缺口（明确标注缺失，不推测补造）**：

- 被误删实例的 GameServer 名称/UID、Pod UID、allocation_id、match_id：**未归档**。上表时间线来自现场观察记录，未附原始日志行。可回捞位置：`kubectl logs deploy/ds-allocator -n pandora`（`battle_warming` / `preactive_release_*` 行含 match_id/pod）、`kubectl get events -n default`（GameServer 删除事件，dev 集群 events 默认 1h TTL，**大概率已失**）。
- allocator 侧删除决策的原始日志行（`battle_abandoned_heartbeat_timeout` 或 preactive 回收链）：**未归档**，同上可回捞（Pod 若已重启则丢失，无集中日志平台）。
- DS 侧 `ServerTravel` / `BeginPlay` 时间戳的原始 UE 日志：**未归档**（Pod 已删，stdout 随之丢失）。
- Artic01 "28s" 冷加载：来自本次时间线推算（13:40:33→13:41:01），无独立复测样本；A3 行动项要求 Linux cooked 复测。

### 2.3 已排除的噪声

- **UDP/端口/网络**：客户端从未拿到 `ds_addr`，未进入连接阶段。
- **OOM/Crash**：本轮时间线内 DS 进程正常推进到 BeginPlay（早前另有 memcg OOM 2Gi 顶死事件，独立根因独立建档 [INC-20260727-002](2026-07-27-p0-battle-ds-artic01-memcg-oom.md)，与本次删除无因果——删除发起方是 allocator sweep，不是 kubelet）。
- **Agones Health 误杀**：fleet `failureThreshold` 已临时抬到 12（120s），Agones 未判 Unhealthy；删除来自 allocator。
- **地图配置/关卡表**：`map_id=8` 已正确进入匹配并完成 Agones 分配。

## 3. 时间线

见 §2.2（单一整齐序列，不重复列表）。修复过程时间线：同日完成三层代码修复（§7.2），未部署。

## 4. 调用链与关键变量

```text
sweepOnce (biz/allocator.go)
  → threshold = now - HeartbeatTimeout(15s)          ← 单一阈值（缺陷点①）
  → RangeStaleBattles(threshold)                     ← warming 记录 score=分配时刻,15s 后必然入列
  → AbandonIfStale(ctx, mid, threshold, ...)         ← 对 ACTIVE 与 BOOTSTRAP/warming 用同一阈值（缺陷点②）
      → battle.State=abandoned（WATCH 事务,正确但阈值错）
  → reconcilePreactiveRelease → FencePreactiveRelease → UID 条件 DELETE GameServer
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| `battle.LastHeartbeatMs` | AllocateBattle finalize（=分配时刻） | Redis battle record，业务心跳覆写 | WATCH 事务内可变 | warming 期恒为分配时刻，被单阈值判「失联」 |
| `HeartbeatTimeout` | conf（15s，不变量 §4） | 进程配置 | 否 | 被同时用于稳态失联与启动宽限（错） |
| `ReadyWaitTimeout` | conf（dev 已放宽 120s） | 进程配置 | 否 | 只约束在途 `waitBattleReady`，sweep 不认它 |
| DS 业务心跳 | `PandoraBattleGameMode::BeginPlay` 起跳 | DS 进程 | — | 冷加载期不存在属**设计内行为** |
| Agones health ping（旧镜像） | 游戏线程 TimerManager | DS 进程 | — | 阻塞加载期断流（放大因素，逼出 threshold=12 临时值） |

## 4.1 第一次部署验证结果（2026-07-27，只读复审执行）

部署链（编译出包→镜像→部署→故障注入）已实际执行：Linux DS 全量编译/Cook/Pak 成功（UAT exit 0，
`PandoraServer` SHA-256 `E25B891838916D1575A58D17FED2BEFF4A20792FE4F0DA1E5D236055460E0C42`），
不可变镜像 `pandora/battle-ds:r1553-dirty-20260727-133010` 部署；ds-allocator Recreate 以 250ms
采样确认零共存；fleet `failureThreshold=3` 生效。**验证失败，暴露第二个 P0 根因**：

- 三次 map8 分配同型失败：首次 staged activation 后，**第二发业务心跳分别延迟
  20.63s / 21.06s / 20.17s**（>15s ACTIVE 阈值），sweep 在第二发到达前删 Pod；第二发
  收到 `TERMINATING/stop`。threshold=3 下 map8 曾在 22.43s 返回 READY 仍被回收。
- 根因：`APandoraBattleGameMode::BeginPlay` **内**立即 `StartBattleHeartbeat("running")`——
  此刻仍处 LoadMap 的世界 BeginPlay 派发中，Artic01 剩余 Actor 初始化阻塞游戏线程 >15s，
  游戏线程 TimerManager 驱动的第二拍被饿死。即"过早宣告 running"：把加载期误标成稳态，
  ACTIVE 15s 阈值按其契约正确地杀掉了它。
- 精度边界：live 日志未打印第二发的 `CredentialUse`，不能断言第二发一定是 Active（也可能
  是 ACK 回调被阻塞后的 staged 重试）；"激活过早且下一发间隔 >15s"已实锤。
- 杀 warming Pod 故障注入：probe 约 18s 判死并完成 purge，但 `waitBattleReady` 因两键均
  消失而继续空转满 120s，重试成功总耗时 **141.85s**（目标 ~40s，P1-1）。
- 每实例 6 次 `model_b_sweep_release_failed`：DELETE 已 200、Pod 处正常 30s 终止宽限，被
  当作失败重复 DELETE ≥7 次并占 sweep 队头（P1-2）。

## 5. 根因

### 5.1 直接根因

`sweepOnce`/`AbandonIfStale` 用单一 `heartbeat_timeout`（15s）同时监管「已激活实例的稳态失联」与「尚未激活的 warming 冷加载」。后者按设计没有业务心跳（心跳在 Battle GameMode BeginPlay 才启动），凡冷加载 > 15s+sweep 周期的地图必然被误判失联回收。证据：§2.2 时间线 + 代码（单 threshold 传参链）+ 修复前失败/修复后通过的回归测试（§8）。

### 5.2 触发条件

- 大图冷加载耗时 > ~20s（Artic01 World Partition 83 cell，server cook 190.7MB，Linux 冷 IO ≈28s）。
- 任何路径进入该图的匹配（`map_id=8`）。

### 5.3 故障放大因素

- **DS health ping 由游戏线程 TimerManager 驱动**：`BlockTillLevelStreamingCompleted` 阻塞游戏线程期间 Agones ping 同样断流——迫使 fleet `failureThreshold` 临时抬到 12，Agones 层丧失快速判死能力（正确方向，但掩盖了「活性信号不该依赖游戏线程」的根因）。
- memcg OOM（旧 limits 2Gi 被 Artic01 加载顶死，dmesg 9 例）曾与本缺陷叠加，使早期排查方向发散。
- matchmaker `ds_allocate_timeout` 与 allocator `ready_wait_timeout` 逐级放宽（150s/120s）都无法生效——sweep 的 15s 在更下层击穿。

### 5.4 为什么现有保护没有挡住

- `ready_wait_timeout`：只作用于在途 `waitBattleReady` 轮询，sweep 判弃不读它。
- `AbandonIfStale` 的 WATCH 单赢家门：只防「新心跳 vs 判弃」竞态，不防阈值语义本身用错。
- Agones health：被临时 threshold=12 钝化；且旧镜像 ping 依赖游戏线程，加载期无法自证存活。
- 重试：matchmaker 重试只会分配下一个 DS 再次被杀（破坏性重试循环）。

## 6. 全仓同类问题扫描

- 扫描基线 commit：de19c92c（+ 本次未提交修复）。
- 扫描范围：`services/battle/ds_allocator`（sweep/abandon 全链）、`services/battle/hub_allocator`（同为 DS 生命周期管理）。
- Confirmed 同型命中：无第二处。
- 已排除项及理由：**hub_allocator 不受影响**——Hub DS 在进程启动时加载固定城图，Agones **Ready 之后**才被播种为分片 warming，首个鉴权心跳即刻可达（不存在"分配后才开始的冷加载"窗口）；其 `HeartbeatTimeout`(30s，钳 ≥27s fence 屏障) 监管的都是已在跑图的实例。hub fleet `failureThreshold` 本来就是 3。
- 未覆盖边界：未来若引入「分配后 ServerTravel 换图」的 Hub 玩法，需重新评估。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 暂停继续点匹配（防破坏性重试） | 已执行 | 用户操作 | 无 |
| fleet `failureThreshold` 3→12 | 已生效（集群实测 12） | `kubectl get fleet` | 钝化 Agones 判死，属过渡态，修复后必须收回（已随修复收回，见 7.2） |
| memory limits 2Gi→12Gi（量测用上界） | 已生效 | fleet yaml 注释 | 跑完一局量 `memory.peak` 后按 peak×1.3~1.5 回调 |

### 7.2 永久修复（三层闭环：活着必有信号 → 判死有权威 → 判弃仍单赢家）

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| ① UE：health ping 改独立线程 `FPandoraAgonesHealthPinger`（5s 间隔，`CompleteOnHttpThread`，零游戏线程依赖），加载期发拍不断流 | **已提交 r1553**；Linux DS 镜像**未重打** | `Pandora/Source/Pandora/{Public,Private}/Net/PandoraAgonesHealthPinger.*`、`PandoraAgonesSubsystem.cpp` | 待镜像部署后 `Log LogPandoraAgonesHealth Verbose` 实测加载期连续性 |
| ② allocator：`BattleStaleCutoffs` 双阈值——ACTIVE 按 15s、warming 按 `ready_wait_timeout`；由 `AbandonIfStale` 在 WATCH 事务内按权威快照选择（防外层 State 快照与首次 `ActivateHeartbeat` 的 TOCTOU） | **已落码未提交** | `internal/data/battle_auth.go`、`internal/biz/allocator.go`（legacy 路径同改于 `UpdateBattleKeepTTL` 闭包内） | 6 个回归 + race 绿（§8） |
| ③ allocator：warming 判死加速 `ProbeExpectedInstanceGone`——GameServer+Pod UID 双确认消失或 Agones `Unhealthy`（即 SDK health ping 判决）才放弃时间宽限；读失败回退时间界；`waitBattleReady` 见终态立即分配失败 | **已落码未提交** | `internal/data/agones_allocator.go`、`internal/biz/gameserver.go`（`WarmingInstanceProber`+编译期断言）、`internal/biz/allocator.go` | probe 10 case 表测 + 3 个 biz 回归 + race 绿 |
| ④ fleet：`failureThreshold` 12→3 收回（stable/canary 同改，§9.21） | **已改未 apply**；⚠️ apply 顺序硬门：必须先上 ①镜像并实测 | `deploy/k8s/agones/20-fleet-battle.yaml`、`21-fleet-battle-canary.yaml` | 待部署验证 |
| ⑤ 复审必修-1：probe 判死结果绑定 exact 身份（`BattleWarmingForfeit`），`AbandonIfStale` 事务内按 allocation_id+gameserver_uid+instance_epoch 精确核验——probe 在途窗口内同 match_id 换成新分配 B 时，A 的判死作废，B 按常规宽限存活（ABA 防护） | **已落码未提交** | `internal/data/battle_auth.go`、`internal/biz/allocator.go` | data 事务级 ABA 测试 + biz probeHook 确定性 ABA 测试，绿 |
| ⑥ 复审必修-2：probe 失败按分配身份记队头退避（复用既有 `sweepDeferral`，键 `warming-probe:<allocation_id>`）——控制面挂死的 probe 不跨轮重复占队头，时间兜底判弃不受影响，同 match 新分配不继承旧退避 | **已落码未提交** | `internal/biz/allocator.go` | 阻塞 probe 双轮公平性测试（第 2 轮 ACTIVE 项完成补偿、probe 零重试）+ 退避不继承单测，绿 |
| ⑦ 复审必修-3：滚动升级共存窗口——旧二进制 sweep 恒按 15s 单阈值删 warming 且无运行时手段停手，本跳 ds-allocator Deployment 改 `strategy: Recreate`（零共存；单副本秒级重启窗口内 AllocateBattle 失败由 matchmaker 重试，按验收底线推论可接受）。**为何不用双阶段功能开关**：开关只能控制新二进制，旧二进制两阶段都照删 warming，开关保护不了任何东西，只新增 §15.3 禁止的死配置 | **已 apply 并实测零共存**（250ms 采样） | `deploy/k8s/services/services.yaml` | 2026-07-27 部署实测 |
| ⑧ 第二 P0（§4.1）：`APandoraBattleGameMode::BeginPlay` 内过早启动业务心跳/宣告 running → 心跳启动移到 `PostLoadMapWithWorld`（引擎在全部初始 Actor BeginPlay 与首次流送 flush 完成后广播；工程内 `UMyLevelModel` 已复用同一委托，非无缝 ServerTravel/直启两路径均覆盖）；触发即自解绑、EndPlay 成对解绑、`StartBattleHeartbeat` 幂等门保持；业务心跳仍留在游戏线程（它就是游戏线程活性证明，搬后台线程=假报存活）；心跳 5s/ACTIVE 15s 不变。加 Verbose 逐拍日志（match_id/pod/state/use_active/use_staged/单调时钟间隔，无票据）+ 派发间隔 >15s 的 Warning 自曝 | **已落码，待 UE 编译+重打镜像** | `PandoraBattleGameMode.h/.cpp`、`PandoraDSBackendSubsystem.h/.cpp` | 待验收门（§8） |
| ⑨ 复审 P1-1：`waitBattleReady` 对 battle 键消失（purge/TTL）立即分配失败；缺 auth 且状态超出 allocation grace（非 allocating/warming）fail-closed。**偏离说明**：未按"AuthFound≠BattleFound 一律 fail-closed"字面执行——battle∈{allocating,warming} 缺 auth 是 ClaimBattle→Prepare、Finalize→provision 的合法在途窗口（joiner 经 awaitExistingBattle 必然经过），一律 fail-closed 会打断正常分配；采用与 `AbandonIfStale` 相同的 allocation-grace 契约 | **已落码未提交** | `internal/biz/allocator.go` | 并发 waiter×回收 purge 测试：waiter 快速失败、Release 恰一次 |
| ⑩ 复审 P1-2：exact 回收识别 `deletionTimestamp` 三态（live/deleting/gone）——已受理删除不重复 DELETE、宽限内快速返回 `ErrReleaseDeletionPending` 哨兵（不再空耗 5s 轮询）、abandoned 项按分配身份进程内退避让出队头（不写 ZSET score，重启即清）、teardown proof 仍只在双对象物理消失后落 | **已落码未提交** | `internal/data/agones_allocator.go`、`internal/biz/allocator.go` | 三态契约 5 case + 宽限队头双轮公平性测试 |
| ⑪ 二轮复审：⑧的 include 写错（`UObject/CoreDelegates.h` 在 UE 5.8 不存在）导致 Linux DS 全量编译失败（1235/1412 Action，UAT exit 6）→ 改 `UObject/UObjectGlobals.h`；同步清除 6 处仍声称"BeginPlay 启动心跳"的旧注释 | **已落码，待 UE 编译验证** | UE 4+2 文件 | 待 Codex 全量编译 |
| ⑫ 二轮复审：Linux `-race -count=50` 可重复失败 8/50（`active projection corrupt`）——定谳为**实现缺陷**：`AbandonIfStale`/`readBoundAuthority` 在 WATCH 闭包内分两次 GET 读 auth+battle，首次 `ActivateHeartbeat` 的 EXEC 可落在两次 GET 之间，撕裂快照被跨键一致性校验在 EXEC 之前误判为 corrupt（WATCH 只保护到 EXEC，保护不了闭包内分次读）；生产表现为对刚激活对局的假 `authority_check_failed`。根修：同槽两键改**单条 MGET 原子读**（`readAuthorityPairAtomic`），撕裂读结构性不可能 | **已落码未提交** | `internal/data/battle_auth.go` | 审查方精确复现命令 `-race -count=60` 通过（修复前 8/50 失败） |
| ⑬ 二轮复审：wait outcome 所有权——reclaimed/purged/superseded/不可授权一律携带 `errBattleWaitOwnershipLost` 哨兵（errcode cause 链），owner 见哨兵跳过 cleanup，消除 waiter×sweep 并发双 `ReleaseExpected`；缺 auth 等待钳 `HeartbeatTimeout`(15s) 有界宽限（推导：GSA POST ≤5s + Deliver PATCH ≤5s + Redis 往返；owner 完成 provision 才进 wait，正常永不触发），不再复用 120s 冷加载宽限 | **已落码未提交** | `internal/biz/allocator.go` | barrier 测试（abandon 已提交、Release 在途时 waiter 退出，Release 恰一次）+ 哨兵断言 |
| ⑭ 二轮复审：队头饥饿全类根除——abandoned 路径**所有**未确认 release/preflight 错误（不只 deletion-pending）按分配身份退避；epoch=0 resume（§9.4 最后一棒）外部确认失败同样退避（15s 只延后节奏，outbox 语义不变）；allocation-id 回退改 **LIST 先行**（全 deleting 直接 pending 不重复 DeleteCollection，空集合保留 DeleteCollection+后置 LIST 的 timeout-late-apply 防线）；UE 心跳诊断只在真发送时记时/打拍、Start/Stop 重置基准（防假连续性证据）；pinger 加 `PANDORA_AGONES_HEALTH_VERBOSE=1` 定向开关（不开放任意 LogCmds）+ 默认级别 60s 发拍摘要（窗口最大间隔即硬门证据） | **已落码未提交** | `internal/biz/allocator.go`、`internal/data/agones_allocator.go`、UE pinger/subsystem | 普通错误双轮公平性 + resume 退避 + 连续 DeleteCollection 单删测试，全绿 |

### 7.3 防复发规则

- `ds_allocator/README.md` §3 已补「双阈值判弃 + warming 判死加速」契约。
- 判别口径沿用 `CLAUDE.md` §16.10 / 客户端 CLAUDE.md「定时器判别标准」：warming 宽限属「阶段 deadline 兜底」（到期重查权威回收），非「掩盖时序」（到期不假设成功）；启动界与稳态界分离即 k8s startupProbe/livenessProbe 模式。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| 针对性单测（warming 30s 不弃/130s 弃、BOOTSTRAP 同、ACTIVE 30s 必弃、Activate×Abandon 并发单赢家 20 轮、probe 10 case、probe-gone 单轮回收、probe-err 回退、fail-fast<2s） | 旧代码下 warming 30s 即被弃（即事故行为） | 全部 PASS | `go test ./services/battle/ds_allocator/internal/{data,biz}` | 本机 2026-07-27 |
| 复审必修回归（⑤ABA：data 事务级换记录 + biz probeHook 确定性注入，B 存活零 Release；⑥公平性：阻塞 probe 双轮 sweep，第 2 轮 ACTIVE 项完成 terminate+release+lifecycle 且 probe 零重试；退避身份不继承） | 修复前 B 被 A 的判死误杀 / ACTIVE 项被饿 | 全部 PASS | 同上 | 本机 2026-07-27 |
| 全包回归 | — | 全部 ok（9 包） | `go test ./services/battle/ds_allocator/...` | 本机 2026-07-27 |
| `go test -race` | — | data/biz 均 ok | docker `golang:1.26.5-bookworm`（Windows 本机无 gcc，Linux 容器执行） | 本机 2026-07-27 |
| 故障注入（加载中杀 warming Pod → probe 判死 → ~40s 内重试成功） | 未执行 | **失败**：probe 18s 判死+purge ✓，但 waiter 空转满 120s，总耗时 141.85s（P1-1 已修，待重验） | 2026-07-27 部署实测 | §4.1 |
| map8 分配存活（READY 后不被回收） | 失败（本事故） | **失败**：3/3 例第二拍延迟 20.17~21.06s 被 ACTIVE sweep 回收（第二 P0，⑧ 已修待重验） | 2026-07-27 部署实测 | §4.1 |
| 新验收门 A：map8 packaged DS 无客户端运行 ≥60s，服务端连续收 ≥12 发业务心跳、最大间隔 <15s、期间不删 Pod | — | 未执行 | 待 ⑧ 镜像重打后 | OPEN |
| 新验收门 B：warming Pod kill 连续注入 ≥3 次，每次 ~40s 内完成重试 | — | 未执行 | 待 ⑨ 部署后 | OPEN |
| 新验收门 C：真实 UE 客户端拿 ds_addr→UDP 进场→运行→退出→重连 | — | 未执行 | 待 A/B 过门后 | OPEN |
| /health 独立线程 pinger 硬门（加载期每 ~5s 连续发拍，覆盖 >30s 阻塞窗口；canary 轨同验） | — | **未完成**：仅见"线程已启动"；本次 map8 加载 22.43s < 30s 漏拍窗口，不构成证明；canary 0 副本未验。观测手段已补：默认级别 60s 发拍摘要（含窗口最大间隔）+ `PANDORA_AGONES_HEALTH_VERBOSE=1` 定向逐拍（无需 LogCmds） | 待验收门 A 同场采集 | OPEN |
| Linux DS 全量编译（⑧ 修复后首验） | **失败**：`UObject/CoreDelegates.h` not found（1235/1412，UAT exit 6，⑪ 已修） | 未执行 | 待 Codex 全量编译 | OPEN |
| Linux `-race -count=50` 目标用例 | **失败 8/50**（⑫ 已根修） | 修复后审查方原命令 `-count=60` 通过；待复审重跑确认 | docker golang:1.26.5 | 本机 2026-07-27 |

## 9. 部署、回滚与观察

- 修复 commit：UE r1553（已提交）+ 第二 P0 修复（工作区未提交，待编译）；Go 侧未提交（工作区 diff）。
- 已部署产物（2026-07-27 第一次验证）：DS 镜像 `pandora/battle-ds:r1553-dirty-20260727-133010`
  （`PandoraServer` SHA-256 `E25B891838916D1575A58D17FED2BEFF4A20792FE4F0DA1E5D236055460E0C42`）、
  新 ds-allocator（Recreate 零共存实测）、fleet threshold=3；两 fleet yaml 已回填不可变 tag 防
  `kubectl apply` 降回 `:dev`（P1-4）。
- 部署顺序（硬性）：① 重打 DS 镜像（≥r1553）→ 换入 fleet → 实测加载期 ping 连续；② apply services.yaml（含 Recreate 策略）并部署新 ds-allocator；③ apply fleet yaml（threshold 3）；④ E2E + 故障注入。
- **零共存验证（②的门，修复 ⑦）**：`kubectl apply` 后执行 `kubectl rollout status deploy/ds-allocator -n pandora` 并用 `kubectl get pods -n pandora -l app=ds-allocator -w` 确认时序为「旧 Pod Terminating→消失 → 新 Pod Pending→Running→Ready」，**任一时刻不得出现两个 ds-allocator Pod 同时 Running**；新 Pod Ready 前的 AllocateBattle 失败属预期（matchmaker 重试）。确认零共存后才允许恢复 map_id=8 匹配测试。
- 回滚条件和步骤：若新镜像加载期 ping 仍断流 → 不 apply ③ 并回查 pinger；若 probe 误判死 → `warming_instance_confirmed_dead_forfeit_grace` 有日志可稽（含 allocation_id），probe 为 advisory 可单独回滚该改动，时间双阈值仍兜底。
- 观察窗口、指标与结果：待部署。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A1 | P0-关闭门 | 重打 DS 镜像（≥r1553）+ 实测加载期 ping 连续 | luhailong | 待执行 | 本档 §9 顺序① |
| A2 | P0-关闭门 | 部署新 ds-allocator + E2E + 杀 Pod 故障注入 | luhailong | 待执行 | §9 顺序②④ |
| A3 | P1 | `ready_wait_timeout=120s` 与 `failureThreshold=3` 取值用 Linux cooked 实测复核（fleet yaml 注释已标「待实测复核」） | luhailong | 待执行 | §7.2④ |
| A4 | P1 | memory limits 12Gi 量测后按 peak×1.3~1.5 回调 | luhailong | 待执行 | fleet yaml 注释 |
| A5 | P2 | Artic01 资产减负（server cook 190.7MB/83 cell）缩短冷加载本身 | 待指定 | 未排期 | — |
| A6 | P1 | 回捞 §2.2 标注缺失的原始证据（allocator Pod 日志、k8s events）；捞不到则在本档确认永久缺失 | luhailong | 待执行 | §2.2 证据缺口 |
| A7 | P2 | 双阈值版本全量铺开后评估恢复 ds-allocator RollingUpdate（本跳 Recreate 的收回条件） | 待指定 | 未排期 | §7.2⑦ |
| A8 | P0-关闭门 | ⑧ UE 编译 → 重打不可变镜像 → 换入 fleet → 过验收门 A（≥60s/≥12 拍/最大间隔<15s/不删 Pod）+ pinger 硬门（含 canary） | luhailong | 待执行 | §8 |
| A9 | P0-关闭门 | ⑨⑩ 部署新 ds-allocator → 过验收门 B（3 次杀 Pod 注入,~40s 内重试）→ 验收门 C（真实 UE 客户端 E2E）→ 观察窗口 | luhailong | 待执行 | §8 |

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据
- [x] 修复前失败、修复后通过的回归存在
- [x] race 达到本事故风险要求（Linux 容器执行）
- [x] 同类代码扫描完成（hub_allocator 排除，理由 §6）
- [ ] 目标环境已加载可追溯的新产物
- [ ] 玩家路径、恢复和补偿路径验证通过（E2E + 故障注入待部署）
- [ ] 观察窗口无复发
- [ ] 剩余风险已解决或另建 Incident/任务
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭。
