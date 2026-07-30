# [INC-20260729-002][P0] Battle DS 被回收后 PIE 客户端无法退出副本

> **状态**：**根因已定谳**（2026-07-30 07:11 同型复发被完整捕获：游戏线程卡死在内存 Realloc，H1 成立、H2 排除）；A/B/C 修复部分已实测生效，玩家路径已闭环（断线→回大厅 2.96s）；**次级根因（DS 用 Development 构建导致 poison malloc 全程 memset）修复未做**，未关闭  
> **类型**：`availability`  
> **环境**：本机 k8s（Minikube + Agones）/ Windows UE PIE  
> **首次发生时间（UTC）**：2026-07-30 02:42:53.874（首个客户端可见异常；最后一次已确认业务心跳为 02:42:46.224，停跳精确时刻未知）  
> **首次发现时间（UTC）**：2026-07-30 02:50:33.041（用户点击退出后无权威响应）  
> **负责人**：待指定  
> **受影响服务/版本**：Battle DS image `pandora/battle-ds:r1597-dirty-cfglevel-20260729-221938`；`ds_allocator` imageID `sha256:38f745aa09306d2c61f605e4bf52bbb15169d1f95b84303be375ce031d92e9a9`；客户端为 Windows UE PIE，取证时工作副本 `svnversion=1487:1598M`（混合修订且有本地修改，不能据此还原唯一二进制）  
> **最后更新**：2026-07-30 UTC（补入 §2.2.7 日志管道取证、§5.1 两假设、§7.2 已落码修复）

> Incident ID 和文件日期采用现场本地发现日 2026-07-29（EDT）；正文时间线统一使用 UTC。UE 日志外层前缀与 K8s/服务日志均对齐 UTC，尖括号内自定义时间为 EDT（UTC-4）。

## 0. 一句话结论

对局 `18116498472108032` 已完成 Battle DS 激活、READY、客户端 `Welcomed` 和 Admission；该对局最后一拍可确认的 ACTIVE 业务心跳在 `02:42:46.224Z` 被 `ds_allocator` 接受，随后没有新心跳，`02:43:02Z` GameServer/Pod 进入删除，符合现场生效的 `heartbeat_timeout=15s` ACTIVE stale 回收链。**DS 为什么停止心跳仍未定谳。** Windows UE PIE 会把当前连接设为 `bNoTimeouts`，本次也没有产生 `ConnectionLost`/`ConnectionTimeout`，客户端因而一直留在已删除 DS 的旧世界；用户在 Pod 删除约 7 分 31 秒后点击退出，日志只证明本地发起退出意图，不能证明已删除的 DS 收到 RPC。

**2026-07-30 复查补充（两条结论性更正，证据见 §2.2.7、§2.2.8）**：

1. 原文把「删除前约 53 秒没有 DS 业务日志」当作现场事实，据此认为无从判断停跳原因。复查证明该静默**是采集管道产物，不是 DS 行为**：DS 的 stdout 在容器里走 libc 块缓冲，事故 Pod 的 11 条「每分钟一次」health 窗口摘要（UE 时间 `02:30:20`→`02:40:21`，跨 10 分钟）在 `02:40:47.252` 一次性到达 Loki；进程被回收时最后一批只吐出 2 行且首行被截断。**UE 时间 `02:42:09.073` 之后 DS 写的所有日志都随缓冲区丢失**——正好覆盖需要解释停跳的整个窗口。
2. 因此停跳原因收敛为两个仍不可判别的单因假设（H1 游戏线程停摆 / H2 Pod 对外网络中断，见 §5.1），而**判别它们所需的证据恰好就是被丢弃的那批日志**。根因门 A1 的真实阻塞项是日志管道，不是"没有线索"。

另有一条与停跳原因无关、无论 H1/H2 都成立的独立 P0：**§9.4 的 abandoned 补偿链被设计为「玩家解放出口」，但它没有任何面向客户端的投递**（见 §5.4），客户端因此没有任何机制得知本局已被判弃。该项已落码修复（§7.2）。

## 1. 影响与范围

- 玩家影响：1 名玩家进入 Artic01 后，移动确认停止推进；点击“退出副本”后没有结算、Hub 新票据或 `ClientTravel`，不能从界面退出副本。
- 影响人数/对局：已确认 1 名玩家、1 局；`player_id=17732562755518464`，`match_id=18116498472108032`。
- 服务影响：该 Battle GameServer 被回收；`battle_result` 记录 abandoned，matchmaker 随后释放该 match。
- 数据与安全影响：本次只确认 `battle_abandoned_recorded`；背包、掉落、战绩等数据结果未在本次取证中核验，不能写成“无数据影响”，也没有证据证明已发生数据丢失。
- 开始/结束时间：首个客户端异常为 `02:42:53.874Z`；客户端直到 `03:07:31Z` PIE world teardown 前仍持续打印 saved-move 告警。该会话由 PIE 结束而终止，不是自动恢复成功。
- 是否仍可复发：未知。没有实施修复，也没有完成复测。
- 严重级别判定理由：玩家已进场但连接指向已删除 DS，退出和自动恢复均未完成，符合 `docs/incidents/index.md` 中“掉线/永久中间态/无法退出玩家路径”的 P0 建档范围。
- 与既有事故关系：本次是在执行 [INC-20260727-001](2026-07-27-p0-ds-allocator-warming-coldload-reclaim.md) 尚未完成的 Gate C（进场→运行→退出→重连）时发现的独立事故。旧事故发生在 warming/进场前；本次已完成 ACTIVE 和 Admission，不是旧 warming 误删的同一现场结论。

## 2. 第一现场与证据

### 2.1 症状

- 客户端症状：`02:42:53.874Z` 起出现 `CreateSavedMove: Hit limit of 96 saved moves`；当前日志共 354 条，持续到 `03:07:31.389Z`。用户在 `02:50:33.041Z` 点击退出后，无退出拒绝、终态 ACK、Hub 票据、Hub `ClientTravel` 或网络失败恢复日志。
- 服务端症状：同一 match 完成激活和 READY；业务心跳逐拍成功到 `02:42:46.224Z`，随后停止。`02:43:02.640Z` allocator 已处于该 allocation 的 `release-pending`。
- K8s/Agones 状态：`02:43:02Z` GameServer event 为 `Deleting Pod pandora-battle-stable-ldm7w-6987d`，同秒 kubelet 停止 DS 和 Agones sidecar；`02:43:34Z` 的 liveness connection-refused 发生在删除启动 32 秒后。
- 客户端停留状态：没有 `ConnectionLost`、`ConnectionTimeout`、`PendingConnectionFailure` 或 `TravelFailure`；直到手动结束 PIE 才发生 `OnWorldEndPlay:ExampleLevel_Artic01`。

### 2.2 原始证据

#### 2.2.1 证据位置与查询条件

- 客户端完整日志：`F:\work\Pandora-Client-SVN\Pandora\Saved\Logs\Pandora.log`，本次相关范围约第 3788–4688 行。日志仍被 UE 进程占用，取证时未能生成稳定文件哈希。
- 用户提交的截取日志：`C:\Users\Administrator\.codex\attachments\4202b6a2-c4b9-4c98-8a08-9d6bcb5b4ac8\pasted-text.txt`，SHA-256 `8B2B6AAFAA1DF2C661DE3F343600312C7E803C931479409895EAC9958A6C2B49`。
- Battle DS Loki：LogQL `{instance="default/pandora-battle-stable-ldm7w-6987d:pandora-battle-ds"}`，UTC 查询窗口 `02:35:00–02:45:00`，`direction=forward`，`limit=5000`。DS stdout 有批量缓冲；带 UE 时间前缀的行按其内嵌 UTC 时间排序。
- 服务日志：namespace `pandora` 中 `ds-allocator-*`、`player-locator-*`、`matchmaker-pve-*`、`battle-result-*`、`matchmaker-*`，查询窗口从 `2026-07-30T02:40:00Z` 开始。
- K8s events：namespace `default`，筛选 involved object/name `pandora-battle-stable-ldm7w-6987d`。
- 事故标识：
  - match `18116498472108032`
  - allocation `c633f672-bec6-41e4-9875-d23a6ed6d453`
  - GameServer `pandora-battle-stable-ldm7w-6987d`
  - GameServer UID `8fdf4766-9d0e-4fc8-b84a-1ccb2e246ef0`
  - Pod UID `6055139c-8764-4110-9c34-03d23726e2a2`
  - DS endpoint `192.168.2.28:7055`
  - map `8` / `/Game/ScifiArctic/Maps/ExampleLevel_Artic01`

事故文档未复制任何 JWT、DSTicket、Authorization header、Secret 原值或完整 Travel URL。

#### 2.2.2 分配、激活、入场证据

```text
02:40:41.243Z matchmaker-pve  match_start_accepted map_id=8 player_id=17732562755518464
02:40:42.748Z matchmaker-pve  solo_match_found match_id=18116498472108032 players=1
02:40:43.210Z Battle DS       map_id=8 → ServerTravel(Artic01)
02:40:43.218Z ds_allocator    battle_warming match_id=18116498472108032
                                      pod=pandora-battle-stable-ldm7w-6987d
                                      ds_addr=192.168.2.28:7055 players=1
02:40:43.227Z Battle DS       staged credential 已安装并绑定当前 Pod/UID/epoch
02:41:13.775Z Battle DS       Battle 权威准入元数据已安装：match=18116498472108032 roster_count=1
02:41:13.776Z Battle DS       战斗业务心跳启动：match=18116498472108032 interval=5.0s
02:41:50.789Z ds_allocator    battle_ds_activation_pending match_id=18116498472108032
02:41:55.819Z ds_allocator    battle_ds_activation_pending match_id=18116498472108032
02:42:01.383Z ds_allocator    battle_ds_credential_activated match_id=18116498472108032
02:42:02.726Z ds_allocator    battle_ready_after_heartbeat match_id=18116498472108032
02:42:02.735Z matchmaker-pve  match_ready match_id=18116498472108032 ds_addr=192.168.2.28:7055
02:42:06.089Z UE client       Welcomed by server (Artic01 / PandoraPveGameMode)
02:42:09.065Z UE client       TravelCompleted
02:42:09.072Z Battle DS       InitNewPlayer accepted player=17732562755518464
02:42:31.114Z UE client       ResumeContext confirmed BATTLE admission: match=18116498472108032
```

这些记录证明本局已越过 warming、三拍激活、READY、UDP 建连和玩家准入边界。

#### 2.2.3 ACTIVE 心跳逐拍与 match 绑定

`ds_allocator` 的 `rpc_ok /pandora.ds.v1.DSAllocatorService/Heartbeat` 行本身不打印 match_id；下表通过相同 `trace_id` 对应的 `player_locator location_set` 精确绑定到本 match 和 endpoint。事故记录不把无 match 字段的单行伪写成自带 match 证据。

| allocator Heartbeat UTC | trace_id | player_locator 同 trace 结果 |
|---|---|---|
| 02:42:05.880 | `8f34df09-8b12-4571-bc82-9d8264c6da9a` | match `18116498472108032`, battle `192.168.2.28:7055` |
| 02:42:10.913 | `9d9f4479-d281-4c57-b1b4-0a51558dd3cd` | 同上 |
| 02:42:15.939 | `53c4c8b8-ee02-4122-8a8c-17426ee15032` | 同上 |
| 02:42:20.973 | `9b805b63-d4fc-4069-a10b-9a3c9a1e64c3` | 同上 |
| 02:42:26.001 | `d4022fe3-2347-4eab-b3d8-e2dfe0bbcb5d` | 同上 |
| 02:42:31.031 | `91778f77-9238-4f63-b251-1d398248ab43` | 同上 |
| 02:42:36.112 | `21cdf7fd-b808-4ea7-b543-e31027e156cc` | 同上 |
| 02:42:41.150 | `f484fd31-f8df-4b13-84b4-b93216b8bce2` | 同上 |
| **02:42:46.224** | `3023976a-e808-4f6e-948a-84c310193b4e` | **同上；最后一拍** |

现场 Secret 只读提取的非敏感有效配置：

```yaml
ready_wait_timeout: "120s"
heartbeat_timeout: "15s"
sweep_interval: "5s"
```

#### 2.2.4 回收、补偿与客户端卡住证据

```text
02:42:53.874Z UE client       CreateSavedMove: Hit limit of 96 saved moves
02:43:02.000Z K8s GameServer Deleting Pod pandora-battle-stable-ldm7w-6987d
02:43:02.000Z K8s Pod        Stopping container pandora-battle-ds / agones sidecar
02:43:02.640Z ds_allocator   allocation_sweep_head_of_line_deferred
                              state=release-pending:c633f672-bec6-41e4-9875-d23a6ed6d453
02:43:02.640Z ds_allocator   model_b_sweep_release_grace_pending
02:43:17.635Z ds_allocator   release grace 仍 pending
02:43:37.651Z ds_allocator   ds_lifecycle_published match_id=18116498472108032
02:43:37.681Z battle_result  battle_abandoned_recorded match_id=18116498472108032 players=1
02:43:38.630Z matchmaker     match_released match_id=18116498472108032
02:50:33.041Z UE client      已向权威 PVE DS 发送单人退出意图。
03:07:31.389Z UE client      最后一条 96 saved-move 告警
03:07:31.519Z UE client      OnWorldEndPlay:ExampleLevel_Artic01
```

最后一拍 Heartbeat 到 K8s 开始删除间隔约 `15.776s`；到 allocator 打出 release-pending 间隔约 `16.416s`。源码 `allocator.go` 使用 `now - HeartbeatTimeout` 计算 ACTIVE cutoff，再由 `AbandonIfStale` 权威事务判弃并进入 fenced GameServer release。该数值链与本次事件时序一致。

Loki 中 Battle DS 在 `02:42:09.073Z` 后没有完整业务日志，下一条为删除开始后的 `02:43:02.690Z FUnixPlatformMisc::RequestExit`；没有 Fatal、OOM 或 crash stack。这个缺口只能支持“停跳原因未知”，不能支持任一具体致因。

#### 2.2.5 健康对照样本

同一客户端进程中的上一局 SonglinTown 正常退出：

```text
02:40:35.006Z 已向权威 PVE DS 发送单人退出意图
02:40:35.103Z 结算完成，开始 IssueDSTicket hub
02:40:35.136Z IssueDSTicket succeeded
02:40:35.154Z DS ClientTravel route=Hub
```

该样本只证明退出链在上一台仍存活 DS 上完成过一次；不证明当前 Artic01 DS 收到退出 RPC，也不证明当前问题根因位于某一具体模块。

#### 2.2.6 源码证据锚点

| 行为 | 文件与行号（取证时） |
|---|---|
| ACTIVE cutoff 使用 `now - HeartbeatTimeout`；warming 使用独立 timeout | `services/battle/ds_allocator/internal/biz/allocator.go:2392-2405` |
| `AbandonIfStale`、fenced terminate/release、abandoned 补偿 | `services/battle/ds_allocator/internal/biz/allocator.go:2595-2705` |
| dev 配置 `ready_wait_timeout=120s`、`heartbeat_timeout=15s`、`sweep_interval=5s` | `services/battle/ds_allocator/etc/ds_allocator-dev.yaml:77-93`；live Secret 同值 |
| Battle 心跳由 GameInstance TimerManager 每 5s 驱动 | `PandoraDSBackendSubsystem.cpp:1582-1598,1616-1680` |
| 退出确认先置 pending/写日志，再调用 Server RPC | `MyMainView.cpp:808-832` |
| pending 时退出按钮禁用 | `MyMainView.cpp:536-545,754-756` |
| Server RPC 转交 PVE GameMode；非 Accepted 才回拒绝 | `MyEntityPlayerController.cpp:957-985` |
| 权威退出冻结终态并等待既有 ACK 回流 | `PandoraPveGameMode.cpp:107-192` |
| 终态确认后客户端才调用 `ReturnToHubDs` | `MyEntityPlayerController.cpp:1019-1027` |
| 恢复协调器只接管三类 NetworkFailure | `MyDsRecoveryCoordinator.cpp:2335-2420` |
| PIE 强制 `bNoTimeouts=true` | UE `NetDriver.cpp:848-857` |
| `bNoTimeouts` 时连接 timeout 返回 `UE_MAX_FLT` | UE `NetConnection.cpp:4745-4754` |
| saved move 上限 96 及告警入口 | UE `CharacterMovementComponent.cpp:12389,12467-12474` |

#### 2.2.7 DS stdout 块缓冲导致事故窗口日志整体丢失（2026-07-30 取证）

复查时集群仍在运行，直接按 §2.2.1 的 LogQL 重取该 Pod 的 174 行记录，并把 **Loki 摄取时间**与**行内 UE 时间**两列对齐：

| Loki 摄取时间 | 该批次内 UE 时间跨度 | 行数 | 说明 |
|---|---|---|---|
| `02:40:47.252` | `02:30:20` → `02:40:47` | 16 | 含 **11 条**每分钟一次的 `LogPandoraAgonesHealth` 窗口摘要（`02:30:20`、`02:31:20` … `02:40:21`） |
| `02:40:47.521` … `02:40:49.961` | `02:40:47` → `02:40:49` | 各 6–19 | Artic01 `LoadMap` / `OnPostWorldCreation` 密集输出，缓冲区被迅速填满故批次密集 |
| `02:41:13.777` | `02:41:13` | 19 | 准入元数据安装、心跳启动 |
| `02:42:09.074` | `02:41:45` → `02:42:09.072` | 22 | 最后一条 health 窗口摘要（UE 时间 `02:41:45.248`）、握手、`InitNewPlayer accepted` |
| **`02:43:02.690`** | `02:42:09.073`（**行内被截断**） | **2** | 截断行 + `FUnixPlatformMisc::RequestExit` |
| `02:43:33.998` | — | 1 | 空行 |

两条可断言的事实：

- **跨 10 分钟的日志在单一时刻到达**，说明 stdout 不是行缓冲；批次的摄取时刻 ≈ 该批最后一行的写入时刻，符合"攒满一个 libc 缓冲块才 flush"的行为。
- **最后一次 flush 只吐出 2 行且首行在字符中间被切断**（`...RoleId=1004 NickN` 直接拼上 `FUnixPlatformMisc::RequestExitWithStatus`）。即 UE 时间 `02:42:09.073` 之后缓冲区里的内容**没有**被完整写出。

引擎侧机制（UE 5.8 本地源码）：`Engine/Source/Runtime/Core/Private/Unix/UnixPlatformMisc.cpp:239-243` 只在

```cpp
if (FPlatformMisc::HasBeenStartedRemotely() || FPlatformMisc::IsDebuggerPresent())
{
    setvbuf(stdout, NULL, _IONBF, 0);
}
```

时把 stdout 设为无缓冲，而 `FUnixPlatformMisc::HasBeenStartedRemotely()`（同文件 `:1450`）只检查环境变量 `SSH_CONNECTION`。容器里两个条件都不成立，stdout 对着管道即为块缓冲。`-FORCELOGFLUSH` 不覆盖此路径——它只作用于 `OutputDeviceFile`（`Private/Misc/OutputDeviceFile.cpp:569-577`），不管 stdout。

**这解释了为什么本事故（以及 INC-20260727-001 的 A10）都停在"关键窗口没有 DS 日志"**：本来应当出现在该窗口的行至少包括
① `TickBattleHeartbeat` 的派发连续性诊断（原为 `Verbose`，见 §2.2.8）、
② `HandleBattleHeartbeatResponse` 的 `战斗心跳失败：transport=...`（若为 H2 必然出现）、
③ health pinger 在 `02:42:21` 前后的下一条窗口摘要（可直接回答"独立线程是否仍在跑"）。
三者全部随缓冲区消失，因此无一可用。

#### 2.2.8 三条使停跳原因不可判别的观测缺口

| 缺口 | 证据锚点 | 后果 |
|---|---|---|
| 业务心跳派发诊断是 `Verbose`，DS 默认级别不输出；且 `>15s` 的 Warning 只在**真发出下一拍时**才可能触发 | `PandoraDSBackendSubsystem.cpp` `TickBattleHeartbeat` 的 `MY_LOG(..., Verbose, ...)` 与紧随其后的 `SinceLastDispatch > 15.0` 分支 | 游戏线程一旦不再 pump `TimerManager`，`TickBattleHeartbeat` 不再被调用，H1 下**一条日志都不会有**；而 H2 下的 Verbose 派发行也看不到，两者在日志上等价 |
| 引擎挂起检测阈值（25s）大于判弃阈值（15s），等于永远来不及打堆栈 | `FThreadHeartBeat::InitSettingsInternal` 读 `[Core.System] HangDuration`，**默认 25.0**（`ThreadHeartBeat.cpp:387-400`）；起线程条件是 `AllowThreadHeartBeat() && ConfigHangDuration>0` 且 `USE_HANG_DETECTION`（`:92-100`）。三者本次**都成立**：`FGenericPlatformMisc::AllowThreadHeartBeat()` 是 `!FParse::Param(..., "noheartbeatthread")` 默认为真；`USE_HANG_DETECTION` 在 server 构建下为真（`Build.h:444-447`，`ALLOW_HANG_DETECTION` 默认 1、`!WITH_EDITORONLY_DATA` 成立） | ⚠️ **不是"没开检测"，是阈值配错了**：检测线程本来就在跑，但 25s > 15s 判弃阈值 → Pod 先被回收，`OnHang` 的 `Error` + 卡住线程完整堆栈（Linux 走 `MINIMAL_FATAL_HANG_DETECTION==0` 分支，非 `abort()`）永远来不及输出。本次停跳窗口约 16s，恰好落在 15s 与 25s 之间——**即使日志没丢，25s 的检测也照样不会触发**。另注：`UE_ASSERT_ON_HANG` 未被外部定义时默认 0（`ThreadHeartBeat.cpp:28-30`），故 `bHangsAreFatal` 默认已是 false，本次显式写 `HangsAreFatal=False` 是钉住行为、防后续有人定义为 1 |
| 无 cadvisor / node-exporter / kubelet 指标 | 复查时对 Prometheus 查 `count(container_memory_working_set_bytes)`、`count(node_cpu_seconds_total)`、`count(kubelet_running_pods)` 均返回空；`job` 标签只有 `pandora-pods` 与 `prometheus` | 原文 §2.3 中"CPU/IO/内存/宿主压力未取证"不是漏做，而是**当前监控栈没有采集这些指标**，事后无法补取 |

#### 2.2.9 可缩小假设空间但不足以定谳的两条旁证（2026-07-30）

- **被删除时 GameServer 仍处 `Allocated`，从未转 `Unhealthy`**。复查时 K8s events 仍在保留期内：

  ```text
  02:40:43Z  Normal   Allocated  Allocated
  02:43:02Z  Normal   Allocated  Deleting Pod pandora-battle-stable-ldm7w-6987d
  02:43:02Z  Normal   Killing    Stopping container agones-gameserver-sidecar
  02:43:02Z  Normal   Killing    Stopping container pandora-battle-ds
  02:43:34Z  Warning  Unhealthy  Liveness probe failed（删除后 32s，见 §2.3）
  ```

  live Fleet health 配置为 `{failureThreshold:3, initialDelaySeconds:15, periodSeconds:10}`，即 30 秒容忍窗口。删除时点没有 Unhealthy 转移，说明独立线程的 `FPandoraAgonesHealthPinger` **至少到 `02:42:32` 前后仍在发拍**（进程活、非游戏线程活、HTTP 子系统可用）。⚠️ 30s 容忍窗覆盖不到 `02:42:46`–`02:43:02` 这 16 秒，故**不能**据此断定停跳窗口内 health 仍正常。
- **Artic01 是 World Partition 重图，且服务端流送关闭**。`Content/__ExternalActors__/ScifiArctic/Maps/ExampleLevel_Artic01` 下有 933 个 OFPA actor；DS 日志 `LogWorldPartition: UWorldPartition::Initialize Context : ... IsServerStreamingEnabled = 0`；`Content/ScifiArctic/MAAS/LevelInstances` 下存在多个 `*_PCG` LevelInstance，`OnPostWorldCreation` 在加载期连续输出数十条。工程自身已记录过该类图会长时间阻塞游戏线程：`PandoraAgonesHealthPinger.h` 的设计注释写明「World Partition 重图在 `BlockTillLevelStreamingCompleted` 内同步加载的十几到几十秒里游戏线程不 tick」，INC-20260727-001 亦实测过两例 17s 阻塞。⚠️ 这只说明 H1 在本图上**有先例**，不构成本次停跳的证据；且 `IsServerStreamingEnabled=0` 意味着"玩家移动触发服务端 cell 流送"这一具体机制不适用。

#### 2.2.10 2026-07-30 07:11 同型复发：根因定谳现场

A 组修复部署后（DS 镜像 `pandora/battle-ds:r1601-dirty-73157330-20260730-023716`，entrypoint 已含 `stdbuf`+`HangDuration=10`），同一失败模式复发一次并被完整捕获。

**证据一：日志管道已修好。** 该 Pod 267 行日志的 Loki 摄取时刻分布在 **16 个不同的秒**（`07:08:52`、`07:09:02`、`07:09:06` … `07:11:44`），不再是事故当天"跨 10 分钟挤在单一时刻、退出时只吐 2 行截断"。启动日志确认走的是主路径：

```text
[entrypoint] stdout 缓冲模式=stdbuf(line-buffered)
```

**证据二：挂起检测拿到了游戏线程堆栈。**

```text
07:11:06.122  LogCore: Error: Hang detected on GameThread (thread hasn't sent a heartbeat for 10.00 seconds):
              ...
              PandoraServer!FTickTaskManager::RunTickGroup(ETickingGroup, bool)()
              PandoraServer!UWorld::Tick(ELevelTick, float)()
              PandoraServer!UGameEngine::Tick(float, bool)()
              PandoraServer!FEngineLoop::Tick()()
              PandoraServer!GuardedMain(char16_t const*)()
07:11:06.122  LogCore: Error: Hang detected on GameThread:
              PandoraServer!FUnixPlatformStackWalk::CaptureStackBackTrace(...)   ← 采样器自身
              PandoraServer!ThreadStackWalker(int, siginfo_t*, void*)            ← 采样信号帧
              libc.so.6!UnknownFunction(0x4251f)
              libc.so.6!UnknownFunction(0x1a0e46)
              PandoraServer!FMallocBinned2::Realloc(void*, unsigned long, unsigned int)()
              PandoraServer!FMallocPoisonProxy::Realloc(void*, unsigned long, unsigned int)()
```

即：**游戏线程在 actor tick group 执行过程中，卡在一次内存 realloc 里超过 10 秒**。

**证据三：挂起检测没有杀进程**（`HangsAreFatal=False` 与 `PLATFORM_USE_MINIMAL_HANG_DETECTION=0` 均按预期生效）。K8s events 证明是 allocator 的 fenced release 删的 Pod：

```text
07:09:02Z  Allocated   Allocated
07:11:14Z  Allocated   Deleting Pod pandora-battle-stable-2zj8t-jhc6v
07:11:14Z  Killing     Stopping container pandora-battle-ds
07:11:44Z  Unhealthy   Liveness probe failed（删除后 30s，同 §2.3 旧条目）
```

DS 侧 `FUnixPlatformMisc::RequestExit` 出现在 `07:11:14.080`，即响应 SIGTERM，不是自杀。

**证据四：进程与非游戏线程始终存活。** 独立线程 health pinger 的窗口摘要在 `07:10:52` 仍是 `尝试=12 启动=12 完成2xx=12 启动失败=0 完成失败=0 相邻启动最大间隔=5.01s`，即卡死前最后一个完整分钟零漏拍。

**时间线（UTC）**：

| 时刻 | 事件 |
|---|---|
| 06:45:49 | Pod 创建；**`FailedScheduling: 0/1 nodes are available: 1 Insufficient memory`**（1 秒后调度成功） |
| 07:09:02 | Allocated |
| 07:09:19.571 | 第 1 次 `Hang detected on GameThread`（10s）——发生在 Artic01 冷加载期，心跳尚未启动，无害 |
| 07:09:30.959 | `PostLoadMapWithWorld` 后启动战斗业务心跳（interval 5.0s） |
| 07:10:52.954 | health 窗口摘要最后一条：12/12 全成功 |
| ~07:10:56 | 游戏线程卡死（由 10s 检测阈值倒推） |
| **07:11:06.122** | **`Hang detected on GameThread` + 完整堆栈** |
| 07:11:14 | allocator 判弃 → fenced release → Pod 删除（SIGTERM） |
| 07:11:56.407 | 客户端 `ConnectionTimeout`（`Threshold: 60.00`）→ 权威恢复 |
| 07:11:59.368 | `Welcomed by server (MainCity / PandoraHubGameMode)`——**已回大厅** |

#### 2.2.11 2026-07-30 08:22 第三次复发：完整调用链，卡点精确到 Chaos 物理

第三次同型复发（match `18203978365992960`，pod `pandora-battle-stable-srfks-2gmgm`，玩家报告"**怪物突然没反应**"）拿到了中间帧齐全的堆栈，把 §2.2.10 只到 `RunTickGroup` 的精度推进到具体容器：

```text
08:22:31.866 LogCore: Error: Hang detected on GameThread (thread hasn't sent a heartbeat for 10.00 seconds):
  UWorld::Tick(ELevelTick, float)
   → FTickTaskManager::RunTickGroup(ETickingGroup, bool)
   → FTickTaskSequencer::ReleaseTickGroup(...)
   → FTaskGraphCompatibilityImplementation::ProcessUntilTasksComplete(...)
   → FNamedTaskThread::ProcessTasksUntilIdle / ProcessTasksNamedThread
   → UE::Tasks::Private::FTaskBase::TryExecuteTask
   → TGraphTask<FTickFunctionTask>::ExecuteTask
   → FEndPhysicsTickFunction::ExecuteTick(...)                              ← EndPhysics tick group
   → FChaosScene::EndFrame()
   → FChaosScene::CopySolverAccelerationStructure()
   → Chaos::FPBDRigidsEvolutionBase::UpdateExternalAccelerationStructure_External(...)
   → Chaos::FPBDRigidsEvolutionBase::FlushExternalAccelerationQueue(...)
   → Chaos::FPendingSpatialDataQueue::Remove(Chaos::FUniqueIdx)             ← 具体容器
   → TSizedHeapAllocator<32, FMemory>::ForAnyElementType::ResizeAllocation(...)
   → FMemory::Realloc
   → FMallocPoisonProxy::Realloc                                            ← poison 全量 memset
   → FMallocBinned2::Realloc
   → libc.so.6!UnknownFunction(0x1a0e96)
```

**卡点定性**：游戏线程在 `EndPhysics` tick group 里、`FChaosScene::EndFrame` 刷新外部空间加速结构队列时，对 `FPendingSpatialDataQueue` 的内部 `TArray` 做一次 `ResizeAllocation`，该 realloc 经 poison proxy 的全量 memset 落到 libc 后卡住 ≥10 秒。

**为什么这个队列会大到卡死**：Artic01 是 World Partition 重图（933 个 OFPA actor），且 DS 侧 `IsServerStreamingEnabled = 0` → 全图刚体常驻、无分块卸载，pending spatial data 队列规模随全图静态刚体数量走。队列越大，`Remove` 触发的 resize 块越大，poison memset 的代价与主机换页概率同步放大。

**玩家可见症状对应关系（本次首次由玩家直接确认）**：怪物 AI tick、移动 ACK、业务心跳都挂在同一条 `UWorld::Tick` 上，游戏线程卡在 EndPhysics 即三者同时停摆。所以"怪物突然没反应"与"业务心跳消失"是同一件事的两个观察面，不是两个问题。

**本次时间线（UTC）**：

| 时刻 | 事件 |
|---|---|
| 08:19:44.439 | health 窗口摘要 12/12 全成功 |
| 08:20:25.825 | 第 1 次 `Hang detected`（加载期；心跳 08:20:36 才启动，无害） |
| 08:20:36.201 | 业务心跳启动（interval 5.0s） |
| 08:21:44.487 | health 窗口摘要仍 12/12 全成功 → 进程与非游戏线程正常 |
| **08:22:31.866** | **第 2 次 `Hang detected` + 完整堆栈（上文）** |
| 08:22:33.870 | `RequestExit`（响应 allocator fenced release 的 SIGTERM，2s 后） |

**附带观察（日志设备锁）**：`08:21:22.183` 摄取的一条 health 摘要其内嵌时间是 `08:20:44.462`，滞后 ~38 秒。health pinger 在独立线程，但 UE 输出设备有全局锁；游戏线程卡死期间持锁会连带阻塞其它线程的 `MY_LOG`。这意味着**卡死期间连非游戏线程的日志也会被推迟**（不是丢，是延后），排查时间线时须按内嵌时间而非摄取时间读。

**放大因素实测（主机侧）**：物理内存 63.8 GB / 可用 9.6 GB / 提交 98.3 GB；`.wslconfig` 配 `memory=60GB`（minikube private 52.7 GB），同机 UE 编辑器 Artic01 PIE private 18 GB。60+18 > 63.8 → 必然换页。同期 `ds_allocator` 报 `AllocateBattle latency_ms=88254`（88 秒）。

### 2.3 已排除的噪声和不得下结论项

| 现象/说法 | 证据结论 |
|---|---|
| `CreateSavedMove: Hit limit of 96` 是根因 | 否。UE `CharacterMovementComponent.cpp` 中 `MaxSavedMoveCount=96`，达到上限即打印该警告；它证明未确认移动累计，是连接异常的症状，不说明 DS 为何停跳。 |
| 这是 INC-20260727-001 的 warming 误删复发 | 否。本次存在 `credential_activated`、`battle_ready_after_heartbeat`、`Welcomed`、`InitNewPlayer accepted`、Admission 和多拍 ACTIVE Heartbeat；旧事故发生在准入前。 |
| Artic01 OOM / crash | 未证实。K8s 事件没有 OOMKilled，Loki 没有 Fatal、OOM 或 crash stack；本次可见的 `RequestExit` 出现在 K8s 删除启动后。不得沿用 INC-20260727-002 的历史 OOM 结论。 |
| `02:43:34Z` liveness connection-refused 导致删除 | 否。删除和 Killing 在 `02:43:02Z` 已开始，该事件晚 32 秒，是删除后的现象。 |
| 打开 Demo 资源或 42.23s Editor tick stall 导致本事故 | 否。Demo 在 `02:49:43Z` 才打开，tick stall 在 `02:50:22Z`，均晚于 Pod 删除超过 6 分钟，不能是本次停跳和删除的起因。 |
| PCG、CPU、I/O、内存、宿主压力或地图逻辑导致停跳 | 未取证。当前没有对应指标、堆栈或删除前 DS 日志，不得写成根因。**2026-07-30 补充**：这些指标不是"漏抓"，而是监控栈根本没采集（§2.2.8 第三行），事后无法补取；删除前 DS 日志则是被 stdout 缓冲丢弃（§2.2.7）。 |
| "删除前 53 秒 DS 没打日志"是现场事实 | **已推翻（2026-07-30）**。那是 stdout 块缓冲 + 退出时 flush 被截断造成的采集缺口，不是 DS 沉默：跨 10 分钟的日志曾在单一时刻到达，最后一次 flush 只吐出 2 行且首行断在字符中间（§2.2.7）。原文据此推出的"无从判断"应改述为"判别所需证据被日志管道丢弃"。 |
| Agones health 停拍导致回收 | 否。GameServer 被删除时状态仍是 `Allocated`，全程没有 `Unhealthy` 转移（§2.2.9）；回收由 `ds_allocator` 的 ACTIVE stale 事务 + fenced release 驱动。 |
| health pinger 在 `02:42:21` 停跳（该分钟摘要缺失） | 不成立为证据。该摘要的缺失与 `02:42:09.073` 之后所有日志的缺失同源（缓冲区未 flush），无法区分"线程停了"与"日志丢了"（§2.2.7）。 |
| “已向权威 DS 发送退出意图”证明 DS 收到 RPC | 否。客户端源码先置 pending、打印日志，再调用 Server RPC；该行只证明本地调用路径执行到 RPC 发起点。 |
| ACTIVE 15s 阈值配置错误 | 未证实。本次只证明现场配置和代码按 15s stale 契约执行；没有证据支持调大阈值，本文不提出该改动。 |
| packaged 客户端一定也会永久卡住 | 未验证。本次是 PIE；`bNoTimeouts` 为 Editor/PIE 路径，不能外推 packaged 行为。 |

## 3. 时间线

| UTC 时间 | EDT 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|---|
| 02:25:18 | 22:25:18 | Agones | 创建 GameServer/Pod | K8s events，GS/Pod UID 见 §2.2.1 |
| 02:25:29 | 22:25:29 | Battle DS/Agones | `SDK.Ready()` 完成 | K8s event `Ready` |
| 02:40:41.243 | 22:40:41.243 | matchmaker-pve | 接受 map 8 单人匹配 | service log |
| 02:40:42.748 | 22:40:42.748 | matchmaker-pve | `solo_match_found` | service log |
| 02:40:43.218 | 22:40:43.218 | ds_allocator | match 进入 warming，绑定 Pod 和 endpoint | service log |
| 02:41:50.789 | 22:41:50.789 | ds_allocator | 第 1 次 `activation_pending` | service log |
| 02:41:55.819 | 22:41:55.819 | ds_allocator | 第 2 次 `activation_pending` | service log |
| 02:42:01.383 | 22:42:01.383 | ds_allocator | credential 激活，进入 ACTIVE | service log |
| 02:42:02.726 | 22:42:02.726 | ds_allocator | `battle_ready_after_heartbeat` | service log |
| 02:42:02.735 | 22:42:02.735 | matchmaker-pve | 向客户端发布 `match_ready` | service log |
| 02:42:05.996 | 22:42:05.996 | UE client | `ClientTravel` 到 `192.168.2.28:7055` | `Pandora.log` |
| 02:42:06.089 | 22:42:06.089 | UE client | `Welcomed by server` | `Pandora.log` |
| 02:42:09.065 | 22:42:09.065 | UE client | `TravelCompleted` | `Pandora.log` |
| 02:42:09.072 | 22:42:09.072 | Battle DS | `InitNewPlayer accepted` | Loki |
| 02:42:31.114 | 22:42:31.114 | UE client | Battle Admission 确认 | `Pandora.log` |
| **02:42:46.224** | **22:42:46.224** | **ds_allocator** | **最后一拍可绑定本 match 的 Heartbeat `rpc_ok`** | allocator + locator 同 trace |
| 02:42:53.874 | 22:42:53.874 | UE client | 第一条 96 saved-move 告警 | `Pandora.log` |
| **02:43:02.000** | **22:43:02.000** | **Agones/K8s** | **Deleting Pod；停止 DS 与 sidecar** | K8s events |
| 02:43:02.640 | 22:43:02.640 | ds_allocator | fenced release 已进入 deletion grace | allocator log |
| 02:43:34.000 | 22:43:34.000 | kubelet | liveness connection-refused（删除后） | K8s event |
| 02:43:37.651 | 22:43:37.651 | ds_allocator | lifecycle published | allocator log |
| 02:43:37.681 | 22:43:37.681 | battle_result | abandoned 已记录 | service log |
| 02:43:38.630 | 22:43:38.630 | matchmaker | match 已释放 | service log |
| **02:50:33.041** | **22:50:33.041** | **UE client** | **用户点击退出，只出现本地退出意图日志** | `Pandora.log` |
| 03:07:31.389 | 23:07:31.389 | UE client | 最后一条 saved-move 告警 | `Pandora.log` |
| 03:07:31.519 | 23:07:31.519 | UE client | PIE world teardown | `Pandora.log` |

## 4. 调用链与关键变量

### 4.1 服务端分配、心跳与回收链

```text
matchmaker-pve StartMatch(map_id=8)
  → ds_allocator AllocateBattle
  → Agones GameServerAllocation
  → battle_warming
  → Battle DS staged Heartbeat ×2
  → credential_activated / ACTIVE
  → battle_ready_after_heartbeat
  → client ClientTravel / Welcomed / InitNewPlayer / Admission
  → Battle DS 每 5s Heartbeat
  → 最后一拍 02:42:46.224Z
  → 后续 Heartbeat 缺失（原因未知）
  → ds_allocator sweep: activeCutoff = now - 15s
  → AbandonIfStale 权威事务判弃
  → TerminateExpected + fenced GameServer release
  → Agones/K8s 删除 Pod
  → battle_abandoned_recorded
  → match_released
```

### 4.2 客户端退出与恢复链

```text
点击退出并确认
  → UMyMainView::HandlePveDungeonExitConfirmed
      bPveDungeonLeavePending = true
      打印“已向权威 PVE DS 发送单人退出意图”
      ServerRequestPandoraPveDungeonLeave()
  → [本次无 DS 收包证据；Pod 已删除]

正常成功链应继续：
  ServerRequestPandoraPveDungeonLeave
  → APandoraPveGameMode::RequestSoloDungeonLeave
  → terminal ACK
  → ClientPandoraBattleSettledReturnToHub
  → UMyMatchModel::ReturnToHubDs
  → IssueDSTicket(hub)
  → ClientTravel(hub)

断线恢复入口：
  NetworkFailure(ConnectionLost / ConnectionTimeout / PendingConnectionFailure)
  → UMyDsRecoveryCoordinator::HandleNetworkFailure
  → 查询权威位置并恢复

本次 PIE：
  UNetDriver::EvaluateNoTimeouts
  → GEditor && GEditor->PlayWorld
  → bNoTimeouts = true
  → UNetConnection::GetTimeoutValue 返回 UE_MAX_FLT
  → 本次没有 ConnectionTimeout/ConnectionLost
  → 恢复入口没有被调用
```

### 4.3 关键变量与生命周期

| 变量/对象 | 创建/来源 | 所有者与生命周期 | 事故中的事实作用 |
|---|---|---|---|
| `match_id=18116498472108032` | matchmaker-pve | 单局权威标识 | 串联匹配、allocator、locator、result 和 client 证据 |
| `allocation_id=c633f672-...` | ds_allocator/Agones allocation | 单次 GameServer 分配身份 | fenced release 精确绑定对象 |
| GameServer UID `8fdf4766-...` | Agones | 单个 GS 实例 | 证明不是仅按 Pod 名模糊回收 |
| Pod UID `6055139c-...` | K8s | 单个 Pod 实例 | K8s 创建、Killing、liveness 事件对象 |
| `LastHeartbeatMs` | ds_allocator Redis authority | 每次授权 ACTIVE Heartbeat 刷新 | ACTIVE stale cutoff 的权威时间基准 |
| `heartbeat_timeout=15s` | live `pandora-config` Secret | ds_allocator 进程配置 | 最后一拍后达到 stale 回收条件 |
| `bPveDungeonLeavePending` | `UMyMainView` | 当前 UI 实例 | 点击后立即置 true，按钮禁用；只有拒绝回调/销毁等路径清理 |
| `UNetDriver::bNoTimeouts` | UE NetDriver | PIE GameNetDriver | PIE 下连接超时值为 `UE_MAX_FLT`，本次没有超时恢复事件 |

## 5. 根因

### 5.1 直接根因

分两层记录，禁止混写：

1. **已确认的直接故障机制**：本局 ACTIVE Heartbeat 在 `02:42:46.224Z` 后没有继续到达；现场 `heartbeat_timeout=15s`，allocator 按 ACTIVE stale 事务将该 match 标记 abandoned 并 fenced release GameServer，K8s 在约 15.8 秒后开始删除 Pod。
2. **未确认的底层根因**：Battle DS 为什么在 `02:42:46.224Z` 后停止业务心跳，**仍未定谳**。删除前关键窗口没有可读的 DS 业务日志、没有堆栈、没有 OOM 记录、也没有对应资源指标。

   **2026-07-30 复查把假设空间收敛到两个仍不可判别的单因解释**（其余可被现有日志排除，见下表）：

   | 假设 | 内容 | 与现有证据是否相容 |
   |---|---|---|
   | **H1** | DS 游戏线程停摆（`TimerManager` 不再 pump → `TickBattleHeartbeat` 不再被调用） | 相容。同时解释业务心跳消失与客户端 `96 saved moves`（服务端不再回 move ACK 需要游戏线程）。本图有 17s 阻塞先例（§2.2.9） |
   | **H2** | Pod 对外网络中断（经 Envoy 的心跳与客户端 UDP 同时不可达，loopback 的 Agones health 不受影响） | 相容。同样能用单一原因解释两个症状 |

   **已可排除的其它解释**（每条都会留下 `Log`/`Warning` 级日志，而这些日志级别在丢失窗口之外也没有出现过）：`StopBattleHeartbeat` 主动停跳、`ProcessRequest=false` 未启动请求、心跳响应失败后仍在重试、Agones 判 Unhealthy 清场。

   **H1 与 H2 无法判别的原因是确定的**：判别它们的唯一证据（心跳派发是否仍在发生 / 响应是否报传输失败 / health 线程窗口摘要）全部落在 §2.2.7 描述的日志丢失窗口内。**根因门 A1 的阻塞项是取证管道，不是线索匮乏。**

   **【2026-07-30 07:11 定谳】H1 成立、H2 排除。** 依据见 §2.2.10：挂起检测在同型复发中打出游戏线程堆栈，卡点在 `FMallocPoisonProxy::Realloc → FMallocBinned2::Realloc → libc`，调用自 `UWorld::Tick → FTickTaskManager::RunTickGroup`；同期 health pinger（独立线程 + loopback）零漏拍，证明进程与网络栈均正常，H2 不成立。

   **次级根因（已定位，未修）：DS 以 `Development` 配置构建，导致 poison malloc 全程生效。**
   `UE_USE_MALLOC_FILL_BYTES` 的定义是
   `((UE_BUILD_DEBUG || UE_BUILD_DEVELOPMENT) && !WITH_EDITORONLY_DATA && !PLATFORM_USES_FIXED_GMalloc_CLASS && !USING_ADDRESS_SANITISER)`
   （`Core/Public/HAL/MallocPoisonProxy.h:11-13`）。Linux 专服构建 `WITH_EDITORONLY_DATA=0`，而 Pod 日志自报 `LogCsvProfiler: Metadata set : config="Development"` → 该宏为真 → `UnrealMemory.cpp:398-399` 用 `FMallocPoisonProxy` 包住 GMalloc，**每次 alloc/free/realloc 都要把整块内存 memset 成填充字节**。这正是堆栈里 `FMallocPoisonProxy::Realloc` 出现的原因，也解释了为什么一次 realloc 能吃掉 10 秒以上。

   放大因素：节点内存吃紧。该 Pod 创建时出现过 `FailedScheduling: 0/1 nodes are available: 1 Insufficient memory`；节点 61.7Gi 容量、battle DS 每个 `requests=limits=14Gi`（INC-20260727-002 定的值），同时只容得下 2 个 battle DS。内存压力下 libc 的大块 realloc 会触发页回收/映射，与 poison memset 叠加放大。

   ⚠️ 仍未证明的部分：**没有确认是哪个容器/数组的 realloc**（堆栈只到分配器层，上面是 tick group，没有具体 actor/component 帧），也没有内存分配 profile。因此"修掉 poison malloc 就不会再卡"属推论而非结论，必须以修复后复测为准。

客户端无法退出的已确认机制是：Pod 在 `02:43:02Z` 已删除，用户到 `02:50:33Z` 才点击退出；退出日志在 RPC 调用前打印，后续没有 DS 接收、拒绝或终态 ACK。与此同时 PIE 的 `bNoTimeouts` 使连接不产生正常超时，本次未触发恢复协调器。

### 5.2 触发条件

- allocator 侧已确认条件：ACTIVE match 的最后授权心跳超过现场 15 秒阈值。
- 客户端侧已确认条件：运行于 PIE；退出点击发生在目标 Pod 删除后。
- 停跳的初始触发事件：未知。

### 5.3 故障放大因素

- PIE 明确禁用连接 timeout，导致已删除 DS 没有通过 `ConnectionTimeout` 进入既有权威恢复链。
- 退出按钮在本地先进入 pending 并禁用；本次既没有 DS 拒绝回调，也没有成功终态回流。
- DS stdout 在删除前约 53 秒没有完整业务日志，缺少解释停跳初始原因的第一现场。

### 5.4 为什么现有保护没有挡住

- `ds_allocator` ACTIVE stale sweep 本次按既有安全契约执行了回收，不是“没有动作”；它负责防止失联 DS 继续作为权威，不负责让仍连接旧 socket 的 PIE 客户端自动换服。
- 客户端已有恢复协调器只处理 `ConnectionLost`、`ConnectionTimeout` 和 `PendingConnectionFailure`。本次 PIE `bNoTimeouts`，日志中没有这些事件，所以恢复入口未启动。
- 主动退出依赖当前权威 Battle DS 受理、结算终态 ACK 后再通知客户端回 Hub。点击时 DS 已不存在，现有证据中没有任何服务端受理结果。
- `battle_abandoned_recorded` 和 `match_released` 完成了后端 abandoned 补偿，但客户端本地连接状态没有随之转换。

**2026-07-30 复查补充：abandoned 补偿链被设计为「玩家解放出口」，但该出口在 Go 侧就断了。**

UE 侧 `PandoraAgonesHealthPinger.h` 的设计注释明确写道：业务心跳故意留在游戏线程，因为挪到后台线程会「瘫痪 allocator 的 abandoned 判定**这条玩家解放出口**」。复查逐段走了这条链，结论是它从未真正到达客户端：

| 环节 | 事故当天状态 |
|---|---|
| `deliverAbandoned` → Kafka `DSLifecycleEvent{ABANDONED}` | ✅ 有（`02:43:37.651`） |
| `battle_result` 记 abandoned | ✅ 有（`02:43:37.681`） |
| `matchmaker` 释放 match | ✅ 有（`02:43:38.630`） |
| **推送给玩家** | ❌ **无**。`battle_result` 唯一的 pusher 是 `pandora.player.update`（MMR/经验），不承载"本局已终止" |
| **释放 owner 权威** | ❌ **无**。修复前全仓 `ReleaseOwner` 只有 login 登出一个调用点（`services/account/login/internal/biz/login.go:1364`） |
| 客户端局内自驱重查 | ❌ **无**。`MyMatchModel` 的 `MatchProgressPollTimer` 在拿到 `ds_addr` 后即停（`MyMatchModel.cpp` `bStartProgressPolling && MatchStage > 0 && MatchStage <= MatchStageReady`），入场后无任何权威重查 |
| 客户端唯一恢复入口 | `HandleNetworkFailure` 的三类 NetworkFailure；PIE 下 `bNoTimeouts` 恒真 → 永不触发 |
| 退出 pending 的解除条件 | 仅「DS 明确拒绝回调」与「View `NativeDestruct`」两点（`MyMainView.cpp` 修复前的 `bPveDungeonLeavePending`）。DS 已删除时两者都不会发生 → 按钮永久禁用 |

即：**无论停跳根因是 H1 还是 H2，玩家都会被卡住**；这是一条独立于根因的 P0，且直接违反 `CLAUDE.md §9.19/§9.20` 与验收底线第 1 条。

⚠️ owner 释放缺失当前尚未在生产语义上暴露，因为 `login-dev.yaml` / `login-dev-tidb.yaml` 的 `owner_query_first: false`，恢复查询走旧链（`resolveResumeRoute` → `InspectBattleRoute`），而旧链对 `abandoned` 的处理是正确的：等过 `placement.DSFenceReentryBarrier` 再入屏障后返回 `BattleRouteTerminal` → HUB（`services/account/login/internal/data/battle_ticket_authorizer.go:159-171`）。一旦 `owner_query_first` 打开，缺失的释放会让恢复查询持续返回 `TARGET(已删除的 battle Pod)`，比不接 owner 更糟。

## 6. 全仓同类问题扫描

- 扫描基线：服务端 Git `332b2fcf`；客户端为取证时 SVN 混合工作副本 `1487:1598M`；UE Engine `5.8.0-release` 本地源码。
- 扫描范围：
  - `services/battle/ds_allocator/internal/biz/allocator.go`
  - `services/battle/ds_allocator/etc/ds_allocator-dev.yaml` 与 live `pandora-config`
  - `PandoraDSBackendSubsystem.cpp`
  - `MyMainView.cpp`
  - `MyEntityPlayerController.cpp`
  - `PandoraPveGameMode.cpp`
  - `MyDsRecoveryCoordinator.cpp`
  - UE `NetDriver.cpp`、`NetConnection.cpp`、`CharacterMovementComponent.cpp`
- 搜索模式：`HeartbeatTimeout`、`activeCutoff`、`AbandonIfStale`、`bPveDungeonLeavePending`、`ServerRequestPandoraPveDungeonLeave`、`ReturnToHubDs`、`ConnectionLost`、`ConnectionTimeout`、`bNoTimeouts`、`MaxSavedMoveCount`。
- Confirmed 同型命中：
  - UE PIE 的所有当前 NetDriver 都受 `GEditor && GEditor->PlayWorld → bNoTimeouts` 影响；这是引擎级 PIE 行为，不只属于 Artic01。
  - 当前主动退出 UI 会在 RPC 前置 pending 并打印“发送意图”；日志语义本身不能作为服务端收包证明。
- 已排除项：INC-20260727-001 warming 误删、INC-20260727-002 memcg OOM、删除后的 liveness failure，理由见 §2.3。
- 未覆盖边界：由于停跳根因未定谳，本次没有完成全仓“致停跳原因”扫描；没有 packaged client 对照；没有删除前 CPU/IO/memory/profile/stack 证据；没有核验本局业务数据最终一致性。

**2026-07-30 补扫：客户端「等 DS Client RPC 才解除的交互门闩」全仓扫描**

危险模式的判据不是"有个 pending 布尔"，而是：**门闩由 Server RPC 发起、只能由 DS 回程 Client RPC 解除**。UE 的 RPC 没有完成回调、没有传输超时，DS 一旦消失，门闩就再无驱动者。走后端 HTTP/gRPC unary 的 pending 不属此类（§9.19 已要求 unary 有界超时，回调必到 success/error/timeout 之一）。

扫描 `Source/Pandora` 全部 `b*Pending / b*Waiting / b*InFlight / b*Requested` 布尔（27 处），逐个核对置位点与清除点：

| 分类 | 命中 | 判定 |
|---|---|---|
| **Server RPC 发起、仅 DS 回程 Client RPC 解除** | `UMyMainView::bPveDungeonLeavePending` | **唯一命中，即本事故**。已修（§7.2 B3） |
| DS 侧（`HasAuthority()`）等后端进度通道回执 | `AMyDropItemActor::bClaimPending` | 不同类：运行在 DS 上，不门控玩家的离开能力；DS 消亡时该状态随进程消失。未改动 |
| 走后端 HTTP/gRPC unary（有界超时，回调必到） | `MyLobbyBagView` / `MyRoleAttrView` / `MyRoleEquipView` / `MyRoleTalentView` 的 `bWritePending`、`MyGuildView::bConfirmPending`、`MyGuildModel::bActionPending`、`MyHubLineModel::bTransferInFlight/bRefreshInFlight`、`MyMatchModel` 的 5 个、`PandoraLoginClient::bLoginInFlight`、`MyAccountModel::bSelectRoleInFlight` 等 | 不同类，未改动 |
| 协调器自身的阶段标志 | `bLevelTransitionInFlight`、`bPostTravelAuthorityCheck` | 已各自绑定阶段 deadline（§9.19 既有实现） |

**未被上述门闩覆盖、但同属"DS 中途消失"的场景**：正常结算（非玩家主动退出）时客户端等 `ClientPandoraBattleSettledReturnToHub`，此时没有任何按钮被禁用，玩家只是留在战斗世界。该场景由 §7.2 C 项恢复的引擎 `ConnectionTimeout`（默认 60s）→ 既有 `HandleNetworkFailure` 恢复链兜底，同样有界。两条路径合起来构成本事故对验收底线第 1 条的完整覆盖。

⚠️ 上述判定为**静态阅读结论**，未经运行验证；`bClaimPending` 的 DS 侧行为边界尤其只做了调用链核对，未做故障注入。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 结束 PIE，使客户端离开已删除 DS 的旧世界 | 已发生于 `03:07:31Z` | `OnWorldEndPlay:ExampleLevel_Artic01` | 仅终止本次本地会话，不构成系统恢复或修复 |

### 7.2 永久修复

分两组：**A 组只解除取证阻塞，不宣称修根因**；**B/C 组修的是与根因无关、已被证据确认的卡死链**。全部只落码，未部署、未验证。

| 组 | 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|---|
| A1 | DS stdout 改行缓冲，防事故窗口日志丢失 | 已落码 | `deploy/ds/entrypoint.sh`：`exec` 前加 `stdbuf -oL -eL`；`stdbuf` 不可用时回退置 `SSH_CONNECTION` 触发 UE 自身 `setvbuf(_IONBF)`；两条都打进启动日志 | 已在 live DS Pod 内以非 root（uid 10001）实测 `stdbuf -oL -eL /bin/echo` 可执行、`/usr/libexec/coreutils/libstdbuf.so` 可读；`bash -n` 语法通过。**镜像重建后的端到端验证未做** |
| A2 | 业务心跳窗口摘要（`Log` 级） | 已落码 | `PandoraDSBackendSubsystem`：新增 60s 窗口摘要（尝试/启动/成功/失败/相邻启动最大间隔），`StopBattleHeartbeat` 输出残窗；逐拍派发日志保留 `Verbose` | 未编译、未运行（UE 编译由用户执行） |
| A3 | 引擎挂起检测阈值降到判弃阈值之下 | 已落码 | `entrypoint.sh` 追加 `-ini:Engine:[Core.System]:HangDuration=10.0` 与 `HangsAreFatal=False`。**是把已在运行的检测从 25s 调到 10s，不是从零开启**（见 §2.2.8） | 同上，未运行 |
| B1 | 判弃后释放 owner 权威 | 已落码 | `ds_allocator`：`OwnerAuthority` 新增 `ReleaseOwner`；`ownerReleaseAbandonedPlayersWeak` 在 `deliverAbandoned` 内执行，带三道门（回收已确认后才调、exact pod+uid 身份匹配、compare-delete 用 Query 读到的 epoch/operation） | `go build` / `go vet` 通过；新增 4 个单测 + 全包 `go test` 全绿；**真集群未验证** |
| B2 | 客户端局内终态等待有界化 | 已落码 | `UMyDsRecoveryCoordinator` 新增 `BeginBattleTerminalWait/EndBattleTerminalWait/IsBattleTerminalWaitPending`：绝对时间戳 deadline（30s）+ 一次性 ticker，到期无条件解除等待；连接已断则转既有权威恢复链，连接仍开只恢复按钮（避免踢断结算中的健康连接） | 新增 automation test `BattleTerminalWaitIsBoundedAndIdempotent`；未编译、未运行 |
| B3 | 退出 pending 去影子状态 | 已落码 | `UMyMainView` 删除 `bPveDungeonLeavePending`，改为只读投影协调器状态；`NativeDestruct` 不再清等待；协调器不可用时不进入禁用态而是提示重试；`AlreadySettling` 分支同样走有界等待 | 同上 |
| C | PIE 连远程 DS 时恢复连接超时 | 已落码 | `RestoreDsConnectionTimeouts`：在 `OnWorldBeginPlay` 端点核对通过后，对该条 GameNetDriver 复位 PIE 强制的 `bNoTimeouts`；显式 `-NoTimeouts` 仍受尊重 | 同上 |
| — | DS 停止业务心跳的根因修复 | **未制定；根因未定谳** | 无改动 | 未执行 |

关于 B2/B3 修复后的预期行为（**尚未实测**）：玩家点击退出后 30s 内若无权威终态回流，退出入口恢复可点（可立即重试）；若对端确已消失，引擎 `ConnectionTimeout`（`BaseEngine.ini` 默认 60s，PIE 下由 C 项恢复）触发 `HandleNetworkFailure` → 既有权威恢复链 → 回 Hub。两条路径都有界。

**刻意未做的改动**（避免用复杂度换措辞，`CLAUDE.md §15.3`）：

- 不调 `heartbeat_timeout=15s`。游戏线程停摆 15s 的 DS 对玩家本就不可用，回收是正确动作；问题在"回收后没人告诉玩家"，不在阈值。原文 §2.3 拒绝该改动的判断成立。
- 不把业务心跳挪到后台线程。UE 侧注释给出的理由（后台线程心跳 = 游戏线程真挂死时仍上报 running，是假心跳）成立。
- 不为 abandoned 新增面向客户端的 push 事件类型。客户端侧有界重查 + 既有 `ConnectionTimeout` 已能收敛；在证明重查周期不够快之前不提前加通道。

### 7.3 防复发规则

- 2026-07-29：新增 Incident 文档、索引和旧 Gate C 失败交叉链接。
- 2026-07-30：落码 §7.2 的 A/B/C 组改动（3 个仓库位置：`XuanMing-Server/deploy/ds`、`XuanMing-Server/services/battle/ds_allocator`、`Pandora-Client-SVN/Pandora/Source/Pandora`）。
- 仍未修改 `AGENTS.md`、`CLAUDE.md` 与设计规范：本次没有产生新的稳定约束，A 组是补观测、B/C 组是既有条款（§9.19/§9.20/§9.22/§9.23）的实现补齐，不引入新规则。**待 A 组产出根因后再评估是否需要写入规范。**

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| 真实 UE Gate C：ds_addr→UDP→运行→退出→重连 | **失败 1 次**：入场和 Admission 成功；DS 后续被回收；退出无响应；未重连 | 未执行 | Windows UE PIE + 本机 k8s | 本档 §2–§3 |
| `ds_allocator` 单元/集成（含 B1 判弃释放 owner） | 未执行 | **通过**：`go build` / `go vet` 干净；新增 4 个 `TestOwnerReleaseAbandoned_*`（仍归属本实例→释放 / 已迁走→跳过 / 释放失败→弱降级 / 身份缺失→no-op）；`go test ./services/battle/ds_allocator/...` 全包 11 项全 ok | `go test -count=1` | 本次执行 |
| `stdbuf` 在 DS 镜像内可用性 | 未执行 | **通过**：live Pod 内 uid 10001 执行 `stdbuf -oL -eL /bin/echo` 成功，`/usr/libexec/coreutils/libstdbuf.so` 世界可读；`bash -n entrypoint.sh` 通过 | `kubectl exec` | 本次执行 |
| A1 行缓冲端到端 | 事故当天：跨 10 分钟日志单批到达，退出仅吐 2 行截断 | **通过**：267 行分布在 16 个摄取秒；启动日志 `stdout 缓冲模式=stdbuf(line-buffered)`；崩溃窗口日志完整 | 07-30 07:08–07:12 真实对局 | §2.2.10 |
| A3 挂起检测端到端 | 事故当天：25s 阈值 > 15s 判弃，永不触发 | **通过**：10.00s 触发两次，打出完整游戏线程堆栈；未杀进程（Pod 由 allocator fenced release 删除） | 同上 | §2.2.10 |
| A2 心跳窗口摘要 | 未执行 | **未验证**：该 DS 二进制不含此改动（日志中 `业务心跳窗口摘要` 命中 0；Linux DS 包早于本次 UE 改动），需重打 `stage/LinuxServer` 后再测 | — | OPEN |
| B1 判弃释放 owner（真集群） | 未执行 | **通过**：`owner_release_abandoned_weak players=1 released=1 skipped_not_self=0 query_failed=0 release_failed=0` | 同上 | allocator log |
| C PIE 连接超时 + 权威恢复闭环 | 事故当天：PIE `bNoTimeouts` → 永不超时 → 恢复入口从未被调用 | **通过**：`ConnectionTimeout` 于 `Threshold: 60.00` 触发 → `requery authority` → `IssueDSTicket succeeded` → `ClientTravel(Hub)` → `Welcomed by server (MainCity)`，**断线到回大厅 2.96 秒** | 同上 | 客户端 `Pandora-backup-2026.07.30-07.13.44.log` |
| B2/B3 退出等待有界化 | 未执行 | **未验证**：本次复发中玩家未点击"退出副本"，门闩未被触发 | — | OPEN |
| ACTIVE Heartbeat/回收时序核对 | 最后一拍至删除约 15.776s，现场阈值 15s | 未执行 | allocator/locator logs + K8s events + live config | §2.2.3–2.2.4 |
| packaged client 断线恢复 | 未执行 | 未执行 | — | OPEN |
| 针对性单测 | 未执行 | 未执行 | — | 尚无修复 |
| 集成回归 | 未执行 | 未执行 | — | 尚无修复 |
| `go test -race` | 未执行 | 未执行 | — | 本次未修改 Go 代码 |
| fatal/OOM/SIGKILL/停跳故障注入 | 未执行 | 未执行 | — | OPEN |
| 玩家业务数据核验 | 未执行 | 未执行 | — | OPEN |

本次没有编译、测试、部署或修改集群；不能把只读取证当作修复后验证。

## 9. 部署、回滚与观察

- 修复 commit：无。
- 构建产物/镜像 digest：无新产物。事故现场 DS tag 和 allocator imageID 见文首。
- 部署时间与目标环境：无部署。
- 实际 Pod/GameServer provenance：Pod/GS UID、DS image 见 §2.2.1；allocator imageID 见文首。
- 回滚条件和步骤：不适用，未变更运行状态。
- 观察窗口、指标与结果：未开始；没有修复后观察样本。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A1 | P0-根因门 | 取得能解释 DS 停止 Heartbeat 的直接证据并在 H1/H2 间定谳 | — | **CLOSED**（2026-07-30 07:11 复发定谳：H1 成立） | 本档 §2.2.10、§5.1 |
| A1a | P0-取证前置 | 重建 DS 镜像并部署，实测 A 组生效 | — | **部分 CLOSED**：A1 行缓冲、A3 挂起堆栈已实测；**A2 心跳窗口摘要仍 OPEN**（DS 二进制未含该改动，需重打 `stage/LinuxServer`） | 本档 §7.2、§8 |
| **A6** | **P0-次级根因** | **DS 停用 poison malloc**：Linux 专服改用 `Shipping`/`Test` 配置，或在 server target 显式 `UE_USE_MALLOC_FILL_BYTES=0`；修复后复测同一 Artic01 对局，断言不再出现 `Hang detected on GameThread` | 待指定 | **OPEN（当前最高优先级）** | 本档 §5.1 |
| A7 | P1-放大因素 | 节点内存容量与 battle DS `requests=limits=14Gi` 的比例复核（61.7Gi 节点只容得下 2 个；已观察到 `FailedScheduling: Insufficient memory`）；确认是否需要扩容或下调 | 待指定 | OPEN | 本档 §5.1 |
| A8 | P1-定位精度 | 查明"是哪个容器/数组在 realloc" | — | **CLOSED**（08:22 第三次复发拿到完整调用链：`FEndPhysicsTickFunction` → `FChaosScene::EndFrame` → `CopySolverAccelerationStructure` → `FlushExternalAccelerationQueue` → `Chaos::FPendingSpatialDataQueue::Remove` → `TArray::ResizeAllocation`） | 本档 §2.2.11 |
| **A9** | **P0-负载** | **DS 侧 Chaos 物理负载治理**：Artic01 在 DS 上 `IsServerStreamingEnabled=0`、933 个 OFPA actor 全图刚体常驻，`FPendingSpatialDataQueue` 规模随之膨胀。评估①开服务端 WP 流送，或②把纯装饰刚体在 DS 上排除（`ClassesExcludedOnDedicatedServer` 同款手段，参考 `DefaultEngine.ini` 已有的 `PCGLandscapeCache` 先例），或③降低碰撞体数量。**与 A6 是两个独立因子：A6 降低单次 realloc 成本，A9 降低 realloc 规模与频次，都要做** | 待指定 | OPEN | 本档 §2.2.11 |
| A10 | P2-可观测 | 卡死期间 UE 输出设备全局锁会连带推迟**非游戏线程**日志（实测 health 摘要滞后 38s）。读时间线必须按行内嵌时间，不能按 Loki 摄取时间；考虑在 runbook 里写明 | 待指定 | OPEN | 本档 §2.2.11 |
| A1b | P1-观测缺口 | 评估补齐 cadvisor / node-exporter 采集（当前 Prometheus 只抓 `pandora-pods`），否则"宿主/容器资源导致停跳"这一类假设永远无法事后取证 | 待指定 | OPEN | 本档 §2.2.8 |
| A2 | P0-修复门 | 根因确认后记录实际修复、修复前失败和修复后通过证据 | 待指定 | OPEN | 本档 §7–§8 |
| A2a | P0-修复门 | §7.2 B/C 组的修复前失败/修复后通过回归：以"权威 DS 被判弃回收后玩家仍能在有界时间内离开副本"为断言，PIE 与 packaged 分开记录 | 待指定 | OPEN（已落码，未编译未验证） | 本档 §7.2、§8 |
| A3 | P0-关闭门 | 完成真实客户端退出与权威恢复路径验证；packaged 与 PIE 分开记录 | 待指定 | OPEN | 本档 §8；INC-20260727-001 Gate C |
| A4 | P1-数据边界 | 核验该 abandoned 对局的背包、掉落、战绩等最终状态 | 待指定 | OPEN | 本档 §1/§8 |
| A5 | P1-潜伏 | `owner_query_first` 打开前必须先确认 B1 的判弃释放已在真集群生效，否则恢复查询会持续指向已删除的 battle Pod | 待指定 | OPEN（B1 已落码，真集群未验证） | 本档 §5.4、§7.2 |

## 11. 关闭审核

- [x] 停止业务心跳的底层根因有直接证据（2026-07-30 07:11 复发拿到游戏线程堆栈：卡在 FMallocPoisonProxy::Realloc；H1 成立、H2 排除）
- [x] 已确认的 ACTIVE stale 回收机制和 PIE 放大链有日志/源码证据
- [x] 阻塞根因取证的观测缺口已定位（stdout 块缓冲 / 派发日志 Verbose / 无挂起检测 / 无容器与节点指标）
- [ ] 修复前失败、修复后通过的回归存在（B/C 组已落码并补单测/automation test，但未编译、未在真环境跑修复前失败样本）
- [ ] race/集成/故障注入达到本事故风险要求
- [ ] 同类代码扫描完成（当前只完成已知路径扫描）
- [ ] 目标环境已加载可追溯的新产物
- [ ] 玩家退出、恢复和补偿路径验证通过
- [ ] 观察窗口无复发
- [ ] 数据影响边界已核验
- [ ] 剩余风险已解决或另建 Incident/任务
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭。直接根因已定谳（游戏线程卡在内存 realloc，H1 成立）且玩家路径已实测闭环（断线→回大厅 2.96s）；但**次级根因未修**（A6：DS 用 Development 构建导致 poison malloc 全程 memset），A2 心跳窗口摘要未进 DS 二进制、B2/B3 退出门闩未被触发验证，观察窗口与数据边界仍空。修好 A6 并复测前不得关闭。
