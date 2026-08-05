# [INC-20260804-001][P0] mode=local 全链断裂：legacy 面 owner 权威接线成片漏接，玩家无法进大厅/进副本/退副本

> **状态**：修复实施中(未关闭)
> **类型**：`availability`
> **环境**：本机进程（`mode=local` + `launcher=editor` + `ds_auth` `local-off-v1`）
> **首次发生时间（UTC）**：2026-08-04 16:43:00（首次可复现，玩家进大厅即被踢）
> **首次发现时间（UTC）**：2026-08-04 16:43:11
> **负责人**：待指定
> **受影响服务/版本**：`hub_allocator` / `ds_allocator` / UE DS（`PandoraEditor Win64 Development`）；基线 commit `8e23b63`
> **最后更新**：2026-08-04

> ⚠️ **待验证清单见 [PENDING-VERIFICATION](2026-08-04-p0-local-legacy-owner-wiring-PENDING-VERIFICATION.md)**。
> 其中 Model B / Agones 面（⑦-B 等）**只有代码级证据，本机无法验证**，在真集群证据填上之前一律按未修复对待。

## 0. 一句话结论

`Model B`（Redis 授权权威 / Agones）落地后新增的 **owner 权威接线**在 `legacy`（`mode=local`）面**成片漏接**：票据不带实例绑定、心跳不续 owner 实例租约、分配不回传实例身份、DS 拿不到权威花名册。六处缺口逐个在玩家链路上表现为「进大厅被踢 → 进大厅确认不了 → PVE 对局判 FAILED → 进副本确认不了 → 退副本被拒」。六处均已落码并经真实客户端验证到「进副本确认成功」；**退副本结算的端到端验证尚未完成**，故本档不关闭。

## 1. 影响与范围

- **玩家影响**：本机联调环境下玩家完全不可玩。按修复顺序依次表现为：①登录后连上大厅 DS 立即被踢，客户端 7 秒一次无限重连；②进入大厅后永远等不到 `STABLE`，30 秒后弹「重连时间较长」兜底面板；③点进 PVE 副本，对局恒判 `FAILED`，进不去；④进入副本地图后同样等不到 `STABLE`，再次弹兜底面板（但副本内战斗实际正常）；⑤点「退出副本 / 失败结算」恒报「副本权威信息尚未就绪，请稍后重试」。
- **影响人数/对局/请求数**：本机单人联调，涉及玩家 `20157286542508032`；PVE 对局至少 8 局被判 `FAILED` 或空场判弃。
- **服务影响**：无服务崩溃、无重启。全部为 fail-closed 拒绝，方向安全。
- **数据与安全影响**：无。所有拒绝均发生在准入/签票阶段，未产生错误写入；owner 权威未出现双 owner。
- **开始/结束时间**：2026-08-04 16:43 UTC 首次复现 ~ 2026-08-05 01:15 UTC 进副本确认通过（退副本待验）。
- **是否仍可复发**：`legacy` 面同类漏接已按第 6 节扫描修复；**同一模式的剩余漏接不能证明为零**（见 §10 行动项 A-1）。
- **严重级别判定理由**：符合建档范围「玩家无法登录/匹配/进场、永久中间态」。虽只影响本机联调档位，但直接阻断全部开发验证，且暴露的是**架构级接线缺陷**而非单点 bug。

## 2. 第一现场与证据

### 2.1 症状

- **客户端症状**：见 §1。关键日志形态为 `LogMyDsRecoveryCoordinator` 反复输出 `post-travel owner target is still PENDING; waiting for STABLE admission`，直至 `authoritative entry wait reached its 30s deadline: reason=2`，随后 `LogMyAccountModel` 弹出 C++ 兜底恢复面板。
- **服务端症状**：`hub_allocator` / `ds_allocator` 全程 `rpc_ok`，无 error 级日志。**这是本事故最难查的一点——服务端视角一切正常**，故障只在客户端与 UE DS 侧可见。`owner` 服务侧可观测到 `Admit` 调用计数长期为 0。
- **K8s/Agones 状态**：不适用（本机进程模式，无 k8s、无 Agones）。

### 2.2 原始证据

```text
# 缺口①：UE Hub DS 拒绝已通过验票的玩家（PostLogin fail-closed）
LogPandoraHubFlow: Error: Hub DS PostLogin 缺可信 player/assignment/placement claims，fail-closed 踢出且不生成 Pawn。

# 缺口①根因：hub 票据 payload 解码后无任何实例绑定 claim
{"iss":"pandora-login","sub":"20157286542508032","aud":["pandora-client"],
 "exp":...,"iat":...,"jti":"...","ds_type":"hub","role_id":1004}
# ↑ 缺 ds_pod / ds_uid / ds_epoch / ds_gen / ds_credential_jti / hub_assignment_id / ds_writer_epoch

# 缺口②/④：owner 记录停在 PENDING，实例租约过期
"ownerType": "OWNER_TYPE_BATTLE", "phase": "OWNER_PHASE_PENDING",
"admitNotBeforeMs": "1785888960528",   # 屏障早已打开（now - admitNotBefore = +176s）
"leaseDeadlineMs":  "1785889131028"    # 租约已过期 6s，未被续写

# 缺口③：matchmaker 拒签 v2 战斗票
ERROR msg=ds_allocate_failed err=errcode=5002 ds_allocator 未回填完整 DS 目标
      (pod="pandora-battle-local-..." uid="" epoch=0 alloc="..." track="stable"),无法签 v2 票

# 缺口⑤：editor 形态 DS 冷启动撞 ready 等待超时
WARN msg=battle_ready_wait_timeout match_id=... pod=pandora-battle-local-...

# 缺口⑥：UE 战斗 DS 权威花名册为空，主动退出被拒
LogPandoraPveDungeonLeave: Warning: 单人 PVE 主动退出被拒：result=4
      player_id=20157286542508032 canonical_pve=1 roster_count=0 rule=SettlementRule
```

### 2.3 已排除的噪声

| 噪声 | 排除理由 |
|---|---|
| `GetMatchProgress failed: HTTP 503 without gRPC trailer` | 时间戳与运维侧 `matchmaker_pve` 重启完全重合；重启结束即消失，且客户端自动重试成功。 |
| `push stream closed: stream_transport_error` + 重订阅 | 流已存活 924 秒后被正常回收，1 秒后自动重订阅成功。属正常生命周期。 |
| `LogPython: AttributeError: module 'unreal' has no attribute 'NiagaraToolset_Info'` | 引擎自带 Niagara 工具集插件的 Python 脚本报错，与网络/准入链无关，非 DS 运行期路径。 |
| `Video memory has been exhausted` | 客户端渲染告警，副本内战斗表现正常，与准入判定无关。 |
| `CreateSavedMove: Hit limit of 96 saved moves` | 连接已断后客户端侧移动缓冲堆积，是**后果**不是原因。 |

## 3. 时间线

以 UTC 为主（本机为 UTC-4，括号内为本地时间）。

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 16:43:11 (12:43) | UE 客户端 | 玩家登录成功、选角完成，随后连上 Hub DS 即被踢，进入 7 秒重连循环 | `Pandora_2.log` |
| 17:02:50 (13:02) | UE Hub DS | 缺口①修复后首次 `InitNewPlayer accepted` + `SpawnPawn`，PostLogin 门通过 | `Pandora.log` |
| 17:03:20 (13:03) | UE 客户端 | 但 `entry wait reached its 30s deadline`，弹兜底面板 → 暴露缺口② | `Pandora_2.log` |
| 23:49:34 (19:49) | UE 客户端 | 缺口②修复后 `ResumeContext confirmed HUB admission: generation=3`，进大厅全链通 | `Pandora_2.log` |
| 23:50:37 (19:50) | UE 客户端 | 首次 travel 进副本成功加载 `SonglinTown`，但持续 `still PENDING` → 暴露缺口④ | `Pandora_2.log` |
| 00:05:33 (20:05) | ds_allocator | `battle_ready_wait_timeout`，PVE 对局判 FAILED → 暴露缺口⑤ | `ds_allocator.err.log` |
| 00:33:54 (20:33) | owner | 缺口④修复后 `OWNER_PHASE_ADMITTED` 首次达成 | `QueryOwner` 实时查询 |
| 00:33:49 (20:33) | UE 客户端 | `ResumeContext confirmed BATTLE admission`，进副本全链通 | `Pandora.log` |
| 00:40:58 (20:40) | UE 战斗 DS | 点退出副本被拒 `result=4 roster_count=0` → 暴露缺口⑥ | `Pandora_3.log` |
| 01:06:58 (21:06) | ds_allocator | **修复引入的自锁死**：DS 进程完全未被拉起，对局按空 pod 判弃 | `ds_allocator.err.log`（`pod=` 为空） |
| 01:14:40 (21:14) | UE 战斗 DS | 死锁修复 + 缺口⑥修复后 `本地 battle 准入元数据已从 env 装载：roster_count=1` | `Pandora_3.log` |
| 01:15:19 (21:15) | UE 客户端 | 再次 `confirmed BATTLE admission`，链路稳定 | `Pandora.log` |

## 4. 调用链与关键变量

```text
登录 → login.SelectRole
  → hub_allocator.AssignHub
    → signHubTicket → ticketBindingFromAssignment   ← 缺口① 返回零值绑定
    → ownerBeginPlayer(HUB)                          （owner 置 PENDING）
  → 客户端 ClientTravel → UE Hub DS PostLogin        ← 缺口① 在此 fail-closed 踢人
  → hub_allocator.AcknowledgeLocalAdmission → owner.Admit
  → hub_allocator.Heartbeat(legacy 分支)             ← 缺口② 不续 owner 实例租约
  → login.GetResumeContext → applyOwnerPlacement     （租约过期 ⇒ 永不报 STABLE）

匹配 → matchmaker_pve.StartMatch
  → ds_allocator.AllocateBattle
    → LocalGameServerAllocator.Allocate               ← 缺口③ 生成 uid/epoch 却不回传
    → battle.GameserverUid / InstanceEpoch 恒零
  → matchmaker battleTargetFromResponse               ← 缺口③ 在此拒签 v2 票，对局 FAILED
  → 等待 DS ready 心跳                                 ← 缺口⑤ editor 冷启动撞 120s 超时
  → ds_allocator.Heartbeat(legacy 分支)               ← 缺口④ 不续租、不代提交 Admit
  → UE 战斗 DS ApplyAgonesAdmissionMetadata           ← 缺口⑥ 无 annotation 通道，roster 恒空
  → APandoraPveGameMode::RequestSoloDungeonLeave      ← 缺口⑥ 在此判 AuthorityNotReady
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| `HubTicketBinding` | `hub.go ticketBindingFromAssignment` | 单次签票 | 否 | 缺口①：`seat==nil` ⇒ 归属记录无 writer-v2 绑定 ⇒ 恒返回零值 |
| `LocalHubCredential` | `local_fleet.go buildEnv` | 随 Hub DS 进程 | 是（`l.mu` 保护） | 缺口①修复的身份来源：与下发给 DS 的令牌**同源** |
| `instanceUID` / `instanceEpoch` | `local_allocator.go Allocate` | 随战斗 DS 进程 | 是（`l.mu` 保护） | 缺口③：原先只用于签令牌，从未回传上层 |
| `BattleStorageRecord.GameserverUid` | `allocator.go` 分配路径 | Redis 记录 | 是 | 缺口③的落点；缺口④续租与 Admit 的身份来源 |
| `pendingRosters` | `local_allocator.go` | `LocalGameServerAllocator` | 是（`l.mu` 保护） | 缺口⑥的投递台账；**自锁死事故的当事变量** |
| `ExpectedPlayers` | UE `PandoraBattleGameMode` | 随 DS World | 否 | 缺口⑥：只从 Agones annotation 装载，local 恒空 |

## 5. 根因

### 5.1 直接根因

**六处缺口共享同一条直接根因**：`Model B`（Redis 唯一授权权威 + Agones）落地时新增的 owner 权威接线（实例绑定票据、实例租约续写、census 代提交 Admit、权威花名册投递），**只在 Model B 分支实现，未在 legacy 分支实现**。而 `mode=local` 恒走 legacy 分支。

| # | 缺口 | 位置 | 最小必要条件 |
|---|---|---|---|
| ① | hub 票据无实例绑定 | `hub_allocator/internal/biz/hub.go` `ticketBindingFromAssignment` | 唯一写入点 `bindAssignmentAuth` 在 `seat==nil`（legacy 恒成立）时直接 return |
| ② | hub legacy 心跳不续 owner 租约 | 同上 `heartbeat()` legacy 分支 | `renewOwnerLeaseGate` 只在 `heartbeatModelB` 内被调用 |
| ③ | local 战斗分配器不回传实例身份 | `ds_allocator/internal/data/local_allocator.go` `Allocate` | 已生成 `instanceUID`/`instanceEpoch`，但返回值只有 `(pod, addr, track)` |
| ④ | battle legacy 心跳不续租、不 Admit | `ds_allocator/internal/biz/allocator.go` `Heartbeat` | `renewOwnerLeaseGate` 与 `ownerAdmitCensusWeak` 只在 `HeartbeatAuthorizedWithPlayers` 内 |
| ⑤ | editor 超时被 packaged 配置顶掉 | `ds_allocator-dev.yaml` | 代码的 editor 放宽条件是「仅当该项未显式配置」，而 yaml 按 packaged 写死了 120s/15s |
| ⑥ | 权威花名册无 local 投递通道 | UE `PandoraBattleGameMode::ApplyAgonesAdmissionMetadata` | `ExpectedPlayers` 只从 Agones annotation 装载；local 无 annotation |
| ⑦ | **对局正常结算不释放 owner（全部署形态，非 local 专属）** | legacy：心跳终态分支；Model B：`ReleaseBattleExpected` | 全仓 owner 释放此前只接了「登出 / 判弃 / saga 中止」三条**异常或主动离场**路径，**「对局正常打完」一条都没接** |

**⑦ 的作用域必须单独说明**：它不是 local 专属。全仓 `ReleaseOwner` 仅三个调用点（login 登出、`rollbackOwnerBegins`、`ownerReleaseAbandonedPlayersWeak`），而后者此前只被 `ReleaseBattle`（legacy release RPC，实测本流程调用次数 **0**）、`AbortPreactiveBattle`、`deliverAbandoned` 调用。三种部署形态的实际覆盖：

| 部署形态 | 正常结算是否释放 owner（修复前） | 证据强度 |
|---|---|---|
| `mode=local` | ❌ | **真实客户端两轮实测复现** |
| Agones + legacy authority | ❌ | 代码阅读（同一 legacy 心跳终态分支） |
| Agones + Model B（标准生产） | ❌ | 代码阅读（`ReleaseBattleExpected` 内 `ownerRelease*` 出现次数为 0） |

**缺口⑥有一条额外的结构性约束**，使它无法靠"补 census"绕过：UE 的 `BuildCompleteBattlePlayerCensus` 要求每个 owner 的 claims 满足 `IsCompleteBattleOwnerClaims`（`dst_ver==2` 且带 `ds_pod`/`ds_uid`/`ds_epoch`/`allocation_id`）；而 `local-off-v1` 刻意只签 HS256 legacy 战斗票；legacy 战斗票又**带不了**实例绑定（`pkg/auth.signDSTicket` 明令 binding 只许 hub 票用）。三者叠加 ⇒ **census 在该档位下结构上不可能成立**。

### 5.2 触发条件

- `mode=local`（`ds_allocator` / `hub_allocator` 均非 Agones、非 Redis 授权权威）；
- `ds_auth` 档位 `local-off-v1`；
- 缺口⑤额外需要 `launcher=editor`（packaged 形态启动快，通常不撞 120s）。

### 5.3 故障放大因素

1. **故障现象高度同形**。缺口②与缺口④在客户端表现**完全一致**（同一句 `still PENDING` + 同一个 30 秒兜底面板），一个在 hub 侧、一个在 battle 侧。修好前者后后者原样复现，极易误判为"没修好"。
2. **服务端全绿**。六处缺口在服务端日志里全是 `rpc_ok`，无任何 error/warn，只能靠客户端日志与 owner 记录实时查询定位。
3. **缺口⑤是擦线故障**。editor DS 冷启动实测 53s（单 DS）～超 120s（三个 UE 进程并存抢 CPU），早期偶发成功掩盖了配置问题。
4. **运维操作二次放大**：排查中为清场杀掉全部 UE 进程，连带杀死 Hub DS；而 `LocalHubFleetProvider` 的懒拉起是 `sync.Once`，**进程内只拉一次、被杀不自愈**，导致玩家点登录无反应，被误认为新 bug（见 §10 A-2）。

### 5.4 为什么现有保护没有挡住

| 保护机制 | 为何无效 |
|---|---|
| **fail-closed 设计** | 完全生效且方向正确——正是它把每个缺口变成明确拒绝而非数据错误。但它**只保证安全，不保证可用**，六处 fail-closed 叠加即"完全不可玩"。 |
| **客户端有界重试 / watchdog** | 生效（§9.19 要求的有界驱动全部满足，无无界等待），但重试的是一个**永远不会成立的条件**，只能撞 deadline 后弹可交互面板。 |
| **单元测试** | 全绿。既有测试覆盖的是 Model B 路径；legacy 路径的 owner 接线**从来没有被断言过**——不存在"legacy 也必须续租/Admit"的测试。 |
| **`owner` 权威 CAS + 屏障** | 生效且从未被绕过（全程未出现双 owner）。但它是"拒绝错误写入"的闸，不负责发现"根本没人来写"。 |
| **配置注释** | 缺口⑤的 yaml 注释明确写了「配套三处一起改」，但**没有任何机械检查**保证 launcher 切换后取值仍自洽。 |

## 6. 全仓同类问题扫描

- **扫描基线 commit**：`8e23b63`
- **扫描目录和文件类型**：`services/battle/{ds_allocator,hub_allocator}/**/*.go`、`Pandora-Client-SVN/Pandora/Source/Pandora/**/*.{h,cpp}`
- **搜索模式/工具**：`if u.modelB` / `authRepo != nil` / `RedisAuthorityEnabled()` / `IsAgonesEnabled()` 的分支穷举；辅以 18 个并行 agent 的多角度审计（31 条候选 → 4 条确认）
- **Confirmed 同型命中**：本档记录的缺口 ①②③④⑥ 即扫描结果本身，均已落码。
- **结构性隐患**：
  - **`local-off-v1` 的战斗票无法携带实例绑定**（`pkg/auth` 硬约束），导致 UE 侧 census 在该档位结构上不可用。当前以「花名册兜底 Admit」绕过，属**近似**而非等价（见 §10 A-3）。
  - **local 与 Agones 面存在一处语义差**：Agones 另投 `pandora.dev/combat-factions`，local 的 env 通道未投递阵营映射。单人 PVE 退出不依赖它，但这是显式取舍而非遗漏。
- **已排除项及理由**：`matchmaker` / `login` / `battle_result` 的 legacy 分支经审计未发现同型 owner 接线漏接。
- **未覆盖边界**：本次扫描只覆盖 owner 权威接线这一类；**其它 Model B 专属能力（如 eviction order、departure 对账）在 legacy 面的完备性未审计**。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 无 | — | 本机联调环境，直接做永久修复，未采取临时绕过 | — |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| ① hub 票据补七元组绑定 | 已落码 | `hub.go` 新增 `localTicketBinding`，身份取自 `LocalCredentialACK`（与下发 DS 的令牌同源） | 真实客户端 PostLogin 通过 + Pawn 生成；票据解码七元组齐全 |
| ② hub legacy 心跳续 owner 租约 | 已落码 | `hub.go` `heartbeat()` legacy 分支补 `renewOwnerLeaseGate`，实例身份回源分片镜像 | `confirmed HUB admission`，`still PENDING` 计数 0 |
| ③ local 战斗分配器回传实例身份 | 已落码 | `local_allocator.go` 身份入台账 + 新增 `LocalInstanceIdentity`；`allocator.go` 经 `localInstanceIdentitySource` 回填 | PVE 对局到达 `MATCH_STAGE_READY` 并签出战斗票 |
| ④ battle legacy 心跳续租 + 花名册兜底 Admit | 已落码 | `allocator.go` 新增 `HeartbeatWithCensus`；`service/allocator.go` 透传 census | `OWNER_PHASE_ADMITTED` + `confirmed BATTLE admission` |
| ⑤ editor 超时放宽 | 已落码 | `ds_allocator-dev.yaml` `ready_wait 120s→300s`、`heartbeat 15s→120s`、`grpc timeout 150s→330s`、`grace 180s→360s`；`matchmaker-pve.yaml` `ds_allocate_timeout 150s→330s` | 分配成功，不再 `ready_wait_timeout` |
| ⑥ 权威花名册 env 投递 | 已落码 | Go：`local_allocator.go` `SetPendingBattleRoster` + `canonicalRosterText`；UE：`PandoraAgonesSubsystem::StageEnvironmentBattleAdmission` + `PandoraBattleGameMode` 装载门 | DS 日志 `roster_count=1`（修复前恒 0） |
| ⑦ 正常结算释放 owner | 已落码 | **收口点在心跳终态分支**（`errHeartbeatTerminal` → `killStrandedDS` 之后），复用 `ownerReleaseAbandonedPlayersWeak`。第一版误接在 `ReleaseBattle` 上，实测该函数在本流程调用次数为 **0**，单测全绿但真机无效 | `TestLegacyTerminalHeartbeat_ReleasesOwner` + `..._SkipsOwnerPointingElsewhere`（直接驱动「记录已 ended → 再来一跳」真实路径） |
| ⑦-B **Model B 正常结算释放 owner** | 已落码 | `ReleaseBattleExpected` 在 `releaseGameServer` **成功之后**（＝K8s UID precondition 删除已确认）释放；弱依赖，失败只告警不改返回值（否则 outbox 会重放已删 GameServer 的回收） | `TestModelBTerminalRelease_ReleasesOwner` / `..._SkipsOwnerPointingElsewhere` / `..._OwnerAbsentDoesNotFailRelease`；**真集群未验证** |
| ⑧ **修复引入的自锁死** | 已落码 | `buildEnv` 内误取 `l.mu`，而 `Allocate` 全程持锁（Go 互斥不可重入）；改为按「调用方持锁」约定直接读写 | 新增带 5s 超时的 `TestAllocate_WithPendingRosterDoesNotDeadlock` |
| ⑨ **ready 等待被单次 Redis 抖动打掉**（**排查中新发现，全部署形态**） | 已落码 | `waitBattleReady` 两个分支（Model B 的 `ReadAuthority` + legacy 的 `GetBattle`）原先任一次读错误即 `return nil, err`；Redis 读超时抛的是**原始错误**→上层判 `ErrUnknown(code=1)`→ matchmaker 回滚 owner Begin → 玩家被弹回大厅。改为**只**容忍传输层错误到下个 tick，deadline 仍是唯一上界；权威判定（battle purge / 分配被取代 / auth fenced / 状态不可推进）保持立即失败 | `TestWaitBattleReady_ToleratesTransientReadError` / `_PersistentReadErrorStillTimesOut` / `_ModelBToleratesTransientReadError` / `_ModelBAuthoritativeLossStillFailsFast`；**Model B 分支真集群未验证（V-10）** |
| — 配套 | 已落码 | dev 面 Redis `read_timeout`/`write_timeout` 1s→5s（`ds_allocator-dev.yaml` + `hub_allocator-dev.yaml`）。**生产 1s 刻意不动**：放宽只是让超时更难触发，代价是 Redis 真挂时 goroutine 多占 5 倍时间；⑨ 的重试才是正解 | 本机实测（AllocateBattle 59.6s code=1 → 修复后不再整局失败） |

**⑦ 与 ⑦-B 的隔离性与前六处不同，是刻意的（2026-08-04 用户拍板）**：前六处修复都有「运行模式门 + 类型断言门」双重机械隔离，生产二进制里是死代码；⑦/⑦-B **只有运行模式门，没有类型断言**，因此 Agones + legacy authority 的灰度部署与标准 Model B 生产都会执行到。这是有意为之——同一缺陷在这些形态下同样存在，一并修复；安全性由 `ownerReleaseAbandonedPlayersWeak` 的三条边界（回收后时序、exact 身份门、compare-delete）保证，不因部署形态而不同。

**线上隔离（为什么 Agones 生产路径零变更）**：每处修复均有双重机械门——①运行模式判定（`authRepo == nil` / `!u.modelB` / `!RedisAuthorityEnabled()` / `!bAgonesEnabled`）；②Go 侧类型断言（`localHubCredentialSource` / `localInstanceIdentitySource` / `localBattleRosterSink` 三个接口**只有 Local\* 实现，Agones 与 Mock 均不实现**，生产二进制里是机械死代码）。配置改动只触碰 `*-dev.yaml` 与 `matchmaker-pve.yaml`，未触碰任何 prod/agones 配置。

### 7.3 防复发规则

- 待补：`CLAUDE.md` §9.22 增补「新增 owner 权威接线时，legacy/local 面必须同批实现或显式声明不适用」——见 §10 A-1。
- 待补：`docs/design/agones-dev.md` 增补「local 面与 Agones 面的能力对照表」，避免下次再靠逐个撞发现差异。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| 针对性单测 | 不存在（legacy 面无断言） | 通过（新增 15 条） | `go test ./services/battle/{ds_allocator,hub_allocator}/...` | 见 §7.2 各项 |
| 集成回归 | — | 两服务全量测试绿 | 同上 | 全绿 |
| `go test -race` | **未执行** | **未执行** | 需 CGO Linux/CI | **阻断项，保留** |
| fatal/OOM/SIGKILL 重启注入 | **未执行** | **未执行** | — | **阻断项，保留** |
| 玩家 E2E：进大厅 | 被踢/超时 | `confirmed HUB admission`，一次成功零重试 | 真实客户端 | `Pandora.log` |
| 玩家 E2E：进副本 | 对局 FAILED / 超时 | `confirmed BATTLE admission`，5.5 秒确认 | 真实客户端 | `Pandora.log` |
| 玩家 E2E：**退副本结算** | `result=4 roster_count=0` | **待验证**（花名册已装载 `roster_count=1`，端到端未点击验证） | 真实客户端 | **未完成，本档不关闭的主因** |
| Agones 集群回归 | — | **未执行** | 需真集群 | **阻断项，保留**（隔离性仅由静态断言与类型系统保证） |

## 9. 部署、回滚与观察

- **修复 commit**：`8e23b63`（缺口①～④的 Go 改动，由并发会话/工具提交）；缺口⑤⑥、自锁死修复与全部 UE 改动**仍在工作区未提交**。
- **构建产物**：UE `PandoraEditor Win64 Development`，839/839 Succeeded（2026-08-05 00:56 UTC 最后一次）。
- **部署时间与目标环境**：本机进程模式，随 `run_services.ps1 -Action restart` 生效。
- **实际 Pod `imageID` / GameServer provenance**：不适用。
- **回滚条件和步骤**：`git checkout` 相关文件 + 重启对应服务；UE 侧回滚需重编。由于全部改动被双重门限制在 local 面，回滚风险仅限本机联调。
- **观察窗口、指标与结果**：**尚未建立**。当前只有单次成功路径证据，无持续观察窗口。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A-1 | 高 | 把「新增 owner 权威接线必须 legacy/local 同批实现或显式声明不适用」写入 `CLAUDE.md` §9.22；否则下一次新增能力会重演本事故 | 待指定 | 未开始 | 本档 §5.1 |
| A-2 | 中 | `LocalHubFleetProvider` / `LocalGameServerAllocator` 的懒拉起是 `sync.Once`，DS 被外部杀死后不自愈，表现为「点登录无反应」。需改为可重拉或至少显式告警 | 待指定 | 未开始 | 本档 §5.3 |
| A-3 | 中 | 缺口④的「花名册兜底 Admit」是**近似**：名册内尚未 travel 到场的玩家会被提前判 `ADMITTED`。当前由 `playerCount>0` + owner 侧 exact CAS + 屏障三重约束兜住，且仅限 legacy 面。若将来 local 能签带绑定的战斗票，应回退为 exact census | 待指定 | 未开始 | 本档 §6 |
| A-4 | 中 | **退副本结算端到端验证未完成**，本档关闭的前置条件 | 待指定 | 进行中 | 本档 §8 |
| A-5 | 中 | 缺口⑤的三处超时是人工保持一致的，无机械检查。应加启动期断言：`grpc.timeout > ready_wait_timeout` 且 `ds_allocate_timeout ≥ grpc.timeout` | 待指定 | 未开始 | 本档 §5.4 |
| A-6 | 低 | local 面未投递 `combat-factions`，与 Agones 面存在语义差。当前无消费者，但需在设计文档登记为显式取舍 | 待指定 | 未开始 | 本档 §6 |
| A-7 | 低 | 缺口⑥的关键不变量「roster 必须在 `Allocate` 之前登记」目前**零测试覆盖**：把登记块移到 `Allocate` 之后，现有测试仍全绿而 roster 恒不送达 | 待指定 | 未开始 | 并行审计确认项 |

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据
- [x] 修复前失败、修复后通过的回归存在
- [ ] race/集成/故障注入达到本事故风险要求（`-race` 与故障注入均未执行）
- [x] 同类代码扫描完成（限 owner 接线一类；其它 Model B 专属能力未审计）
- [x] 目标环境已加载可追溯的新产物
- [ ] 玩家路径、恢复和补偿路径验证通过（**退副本结算未验证**）
- [ ] 观察窗口无复发（未建立观察窗口）
- [ ] 剩余风险已解决或另建 Incident/任务（A-1～A-7 全部未开始）
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭。
