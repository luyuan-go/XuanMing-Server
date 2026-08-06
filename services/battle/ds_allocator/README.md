# ds_allocator

> 战斗 DS 调度服务:matchmaker 全员确认后调 `AllocateBattle` 申请战斗 DS(Agones GameServer / 本机 UE 进程),
> 等 DS 用心跳确认 ready 才回 `ds_addr`;战斗 DS 每 5s `Heartbeat` 续命,心跳超时由后台扫描标记 abandoned
> 并发补偿事件。同进程还托管 `GmService`(GM / 运维指令下发)。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`
> 的 [`ds-arch.md`](../../../docs/design/ds-arch.md)、[`agones-dev.md`](../../../docs/design/agones-dev.md)、
> [`decision-revisit-ds-callback-auth.md`](../../../docs/design/decision-revisit-ds-callback-auth.md);
> 跨服务要约见 [`go-services.md §2.11`](../../../docs/design/go-services.md)。
>
> 代码行号锚点以**函数名**为准,行号截至当前 HEAD(会随改动漂移)。

## 职责与边界

- **职责**:为一场 match 申请 / 回收战斗 DS,维护 `match_id → 战斗 DS 实例` 的 Redis 镜像,并在 DS 崩溃 /
  心跳超时 / 空场时发补偿事件(不变量 §4)。
- **权威态**:战斗 DS 实例镜像(pod / addr / UID / epoch / allocation_id / roster)与心跳活跃索引全在
  **Redis**(进程内只做后台扫描,不缓存权威);Model B 开启后 Redis 是**唯一授权权威**,K8s(Agones)只作
  凭据投递镜像,不作授权来源。
- **调用方全是后端内部**:matchmaker(申请 / 补偿)、battle_result(回收)、login(重连重签查询)、战斗 DS
  (心跳 / 拉 GM 指令),**不面向玩家客户端**——gRPC 层用 `pmw.AuthOptional()`,service 层不从 ctx 取
  `player_id`(`internal/service/allocator.go` 包注释)。
- **不做的事**:不算 MMR / 经验 / 掉落(那是 battle_result,不变量 §9.6);不做撮合(那是 matchmaker);
  不是玩家归属权威的唯一本体(owner_epoch 权威见 `owner-authority.md`,本服务当前只做实例租约双写弱依赖)。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20020` | 内部 RPC(matchmaker / battle_result / login / 战斗 DS / 运维 GM 工具),不经 Envoy 对客户端开放 |
| HTTP | `:21020` | 仅 `/metrics`(`ds/allocator.proto` 无 `google.api.http` 注解,无 RESTful RPC) |

端口来自 `internal/conf/conf.go` 的 `Defaults()`(`Server.Grpc.Addr=:20020` / `Server.Http.Addr=:21020`);
与 `infra.md §6` 端口表一致。

## 对外接口

代码入口:`internal/service/allocator.go`(`DSAllocatorService`)+ `internal/gm/gm.go`(`GmService`,同进程复用
同一 gRPC 端口)。**所有 RPC 都是内部专用**:不经 Envoy 暴露给玩家,不读玩家 JWT `player_id`。

### DSAllocatorService

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `AllocateBattle(match_id, player_ids, combat_factions, map_id, game_mode)` | matchmaker | 申请战斗 DS,等 DS ready 心跳后回 `ds_addr` + 实例绑定元组(UID/epoch/allocation_id) | 内部(无 player JWT);Model B 下签发 DS 回调凭据 |
| `ResolveBattleTarget(match_id, player_id)` | login(冷重连重签) | **只读**返回当前可重连目标并校验 roster 成员;不建 claim、不调 Agones、不刷 TTL | 内部;仅 Model B 可用,目标须仍 `ReadyAuthorized` 且 player 在 roster |
| `ReleaseBattle(match_id, reason, [expected tuple + auth proof])` | battle_result | 对局结束回收 DS。Model B 必须带 `completed` / `completed-finalize` + 完整 DS auth proof + expected 实例元组;legacy 仅按 `match_id` 回收 | 内部;Model B 拒绝 `match_id`-only 回收(防误杀重建实例) |
| `AbortPreactiveBattle(match_id, allocation_operation_id, ds tuple)` | matchmaker(割当 saga 补偿) | 撤销「已 POST GSA、尚未签发 battle 票」的分配;durable abort journal 认 ACK-loss 重放 | **matchmaker 独立信任域**:`internalrpcauth` HMAC(full-payload 绑定 + Redis nonce 防重放);未接线 fail-closed |
| `Heartbeat(match_id, ds_pod_name, player_count, state, active_player_ids, ...)` | 战斗 DS(每 5s) | 续命 + 刷新 state/player_count;Model B 内含 pending 激活、census 准入、Battle→Hub 物理离场闭环 | DS 回调令牌守卫(`middleware.DSCallbackGuard`);Model B 要求完整 DS 凭据,legacy/nil 拒绝 |
| `ListBattles(state_filter)` | 运维 / 调试 | 列当前战斗实例镜像 | 内部 |
| `EnsurePlayerDeparture(...)` | (已废弃硬切) | placement 路由体系已删,旧调用方一律 `ErrServiceDisabled` | — |

### GmService(GM / 运维指令下发,`internal/gm/gm.go`)

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `SendCommand(match_id, payload)` | 运维 / GM 工具 | 下发一条 GM 指令入 Redis 队列(当前仅 `AddItem`);前置校验目标对局有活跃镜像 | 内部,**不经 DS 面网关**;非业务幂等(每次新 `idempotency_key`) |
| `PollCommands(match_id, ds_pod_name, max)` | 战斗 DS | 复用心跳节奏轮询拉本局指令(RPOP,取即出队,FIFO,at-most-once) | DS 回调令牌(只能拉自己这局);Model B 把 active 校验与 RPOP 放同 slot 事务 |
| `AckCommand(match_id, idempotency_key, ok)` | 战斗 DS | 回报执行结果(仅审计日志,不影响队列) | DS 回调令牌 / Model B active 校验 |

> **鉴权模型三分**:①普通内部 RPC(`AllocateBattle` / `ResolveBattleTarget` / `ListBattles`)靠「不经 Envoy +
> 不读 player_id」隔离玩家;②DS 回调(`Heartbeat` / `Poll` / `Ack` / Model B `ReleaseBattle`)用 DS 回调令牌 /
> 完整 Model B 凭据;③`AbortPreactiveBattle` 用与玩家 JWT、placement proof、DS 凭据**都互不相同**的 matchmaker
> 服务鉴权密钥(启动期 `ValidateAllocationAbortAuthConfig` 强校验不同源)。

## 目录结构(Kratos 标准分层,对齐 matchmaker / login)

```
cmd/ds_allocator/main.go        启动入口(redis + allocator 后端选装 + DS 回调签发/守卫 + ds.lifecycle producer + owner 租约 + sweep 装配)
cmd/ds_allocator/pod_uid_*.go   Model B 激活的一次性 release preflight / config-compare 子命令(非常驻)
cmd/gmctl/main.go               GM 指令 CLI 工具(SendCommand 客户端)
cmd/battle_auth_quarantine/     Model B battle auth 隔离/清理运维工具
cmd/pod_uid_acl_cleanup/        Pod UID ACL 清理运维工具
etc/ds_allocator-dev.yaml       开发期配置(mode=local 本机 UE DS)
etc/ds_allocator-prod.yaml.example  线上配置样例
internal/
  conf/conf.go                 配置结构(Config + AllocatorConf + AgonesConf + LocalDSConf;Defaults/校验)
  service/
    allocator.go               DSAllocatorService RPC 入口(proto ↔ biz 互转,errcode 映射)
  gm/gm.go                      GmService(GM 指令队列:SendCommand/PollCommands/AckCommand)
  biz/
    allocator.go               AllocatorUsecase 核心(AllocateBattle/Release/Heartbeat/RunHeartbeatSweep)
    gameserver.go              GameServerAllocator 接口 + Mock 实现
    capacity.go                Agones Fleet 容量巡检(CapacityWatcher)
    owner_lease.go             owner 权威实例租约双写门(migrate ⑥,renewOwnerLeaseGate)
    owner_authority.go         owner 迁移弱依赖:READY Begin(BATTLE) + census 代 Admit(migrate ②/③)
  data/
    battle.go                  RedisBattleRepo(pandora:ds:battle / active claim/finalize/sweep 仓储)
    battle_auth.go             Model B Redis 授权权威(auth+battle 同 slot 事务:激活/心跳/离场)
    battle_abort.go            割当 saga abort journal + fence
    battle_departure.go        Battle→Hub 物理离场协调(census / eviction order)
    battle_lifecycle_proof.go  ds.lifecycle 已发布 durable proof
    agones_allocator.go        AgonesGameServerAllocator(k8s apiserver REST 调 GameServerAllocation)
    local_allocator.go         LocalGameServerAllocator(本机 exec Windows UE DS 进程)
    locator_client.go          player_locator gRPC client(续期 BATTLE presence,弱依赖)
    owner_lease_client.go      owner 权威 gRPC client(实例租约)
  poduidpreflight/             Model B Pod UID 激活 preflight 扫描 / 审计
  server/                      grpc(DSAllocatorService + GmService) / http(/metrics) 注册
```

## 核心调用链

### 1. AllocateBattle —— 申请 DS,等 ready 心跳才回地址

`AllocateBattleWithCombatFactions`(`internal/biz/allocator.go:354`)。**关键:Agones `Allocated`(pod 被分配)
≠ DS `Ready`**——DS 进程要先读到 `match_id` 才能在 PreLogin 放行客户端票据,所以不一拿到 pod 就回地址:

```
AllocateBattle (matchmaker 全员确认后调)
├─ CanonicalRoster / CanonicalCombatFactions   规范化 roster + 阵营快照(internal/biz/allocator.go:365)
├─ releasePolicy.Select(match_id)              确定性 canary/stable cohort(同局不拆轨)
├─ repo.ClaimBattle (SET NX)                   ★ 并发 AllocateBattle 的线性化点;输家走 awaitExistingAllocation 幂等等待
│                                              (:405;claim 携带 allocation_id,只有赢家能调外部 Agones)
├─ [Model B] repo.FenceBattleAllocation        POST 前把 claim CAS 成永久 allocation_uncertain(:425)
│                                              —— 「是否允许 POST」的唯一线性化点,失败/未知绝不碰 K8s
├─ AllocateAuthoritative / alloc.Allocate      向 Agones / 本机 exec 申请 pod(:433/:439)
├─ FinalizeFencedBattleAllocation              写 state=warming 镜像(:496)
├─ [Model B] provisionBattleCredential         Redis stage → K8s 条件投递 annotation → delivered CAS(:520/:646)
├─ waitBattleReady                             ★ 轮询 Redis 镜像等 DS ready 心跳(:794),超时→failReadyWaitTimeout 回收
├─ ownerBeginPlayersWeak(BATTLE)               owner 迁移弱 Begin(migrate ②,失败仅告警,:545)
└─ return AllocateResult{ds_addr, pod, UID, instance_epoch, allocation_id, release_track}
```

- **ready 判定**:`battleReadyForPod`(`allocator.go:785`)要求 `LastHeartbeatMs > allocatedAtMs`(真实心跳,
  非分配基准)+ state ∈ {ready, running} + pod/match 对得上。用 **Redis 镜像轮询**而非内存 channel:Heartbeat
  RPC 可能落到另一个 ds_allocator pod,只有共享 Redis 能跨 pod 观察到就绪。
- **幂等**:同 `match_id` 已有镜像 → `awaitExistingAllocation`(`allocator.go:737`):ready/running 直接回;
  warming 继续等;`allocation_uncertain` / `preactive_release_pending` 等永久墓碑态返回 `Unavailable`,永不发第二次 POST。
- **超时清理**:`waitBattleReady` 超 `ReadyWaitTimeout` → `cleanupAllocatedBattle`(`allocator.go:860`)用与入站 ctx
  解耦的短超时 ctx 执行「永久 release fence → UID 条件回收 → 明确成功才 purge」,绝不把 `ds_addr` 回给 matchmaker。

### 2. Heartbeat —— DS 续命 + census + 补偿信号源

Model B 走 `HeartbeatAuthorizedWithPlayers`(`internal/biz/allocator.go:1658`);legacy 走 `Heartbeat`
(`allocator.go:1799`)。核心副作用集中在 data 层同 slot 事务 `authRepo.ActivateHeartbeat`(auth + battle 投影 +
服务端接收时间一次 EXEC;**请求 `ts_ms` 不参与任何授权 / TTL 判断**):

```
Heartbeat (战斗 DS 每 5s)
├─ dsGuard.CheckBattleCredential           校验完整 Model B 凭据(service/allocator.go:211)
├─ [首心跳] ensureDurableReleasePodUID     滚动升级补 pod_uid(:1695)
├─ authRepo.ActivateHeartbeat              pending→active 激活 / active 续命 / 空场→ABANDONED(:1703)
├─ renewOwnerLeaseGate                     owner 实例租约双写(migrate ⑥,弱/强依赖,:1719)
├─ ownerAdmitCensusWeak                    census 代提交 Admit(migrate ③,弱依赖,:1726)
├─ ReconcilePlayerDepartures               Battle→Hub 物理离场:回 eviction order(:1742)
├─ [空场] finishEmptyAbandon               player_count==0 超 EmptyBattleTimeout → abandoned + 补偿(:1753)
├─ [终态] 返回 command=stop                通知孤儿 / 终态 DS 自行停机(:1761)
└─ refreshBattleLocations                  弱依赖:续期玩家 BATTLE presence(重连,:1765)
```

### 3. RunHeartbeatSweep —— 心跳超时补偿(at-least-once outbox)

`RunHeartbeatSweep`(`internal/biz/allocator.go:2053`)每 `SweepInterval`(默认 5s)跑一轮,进程启动立即先跑一次
(避免丢失的永久墓碑被隐藏一整个间隔)。核心是把 **active ZSET 自身当作补偿事件 outbox**:

```
sweepOnce (每 5s;internal/biz/allocator.go:2093)
├─ reconcileActiveIndexIfDue        从 canonical battle 记录重建派生 active ZSET(节流 30s)
├─ RangeStaleBattles(threshold)     取 last_heartbeat_ms 早于 now-HeartbeatTimeout 的 match
└─ 逐条按 state 分派:
   ├─ allocation_uncertain / reconcile_*   → reconcileAllocationUncertain / resumeEmpty*(Model B 才动,legacy 只读跳过)
   ├─ preactive_release_pending            → reconcilePreactiveRelease(UID 条件 Release + purge)
   ├─ allocation_abort_pending             → 读 abort journal 重放 AbortPreactiveBattle
   ├─ allocating(Model B 尚未 POST)        → DeleteBattleIfAllocationMatches 直接撤销 claim
   └─ 活跃超时                              → authRepo.AbandonIfStale → deliverAbandoned(发 ds.lifecycle)
                                              投递成功才 Expire 移出 active;失败保留下一轮重试
```

- **双阈值判弃**(`data.BattleStaleCutoffs`):ACTIVE(已收到首次业务心跳)按 `HeartbeatTimeout`(15s);
  尚未激活的 warming 按 `ReadyWaitTimeout` 宽限(大图冷加载在 GameMode BeginPlay 前没有业务心跳是正常行为,
  与 AllocateBattle 在途的 `waitBattleReady` 同一口径)。阈值由 `AbandonIfStale` 在 WATCH 事务内按
  权威快照选择,不用外层 sweep 的 State 快照(与首次 ActivateHeartbeat 存在 TOCTOU)。
- **warming 判死加速**(`WarmingInstanceProber`,接 Agones SDK health ping):宽限期内 sweep 只读探测
  exact 实例——GameServer+关联 Pod UID 双确认消失,或 Agones 判 `Unhealthy`(DS 侧 r1553 起 health ping
  由独立线程 pinger 驱动,断流即真死)——确认死亡才放弃时间宽限,本轮交事务判弃并走 fenced 回收;
  探测失败只回退时间界。`waitBattleReady` 见到终态立即按分配失败返回,matchmaker 无需空等 `ReadyWaitTimeout`。

- **可靠补偿(不变量 §4)**:abandoned 的对局在 `pandora.ds.lifecycle` 事件成功投递前**不移出 active**,下一轮 sweep
  再次命中重试,配合 battle_result 幂等消费(不变量 §2)构成穿越 Kafka 临时不可用的 at-least-once 闭环
  (`deliverAbandoned`,`allocator.go:2396`)。
- **生产强依赖门**:Redis authority / Agones+enforce 下 `RequiresReliableLifecyclePublication()==true`,`main.go`
  在装配期 `ValidateLifecyclePusherReady` fail-closed——缺 Kafka broker / producer 直接拒启动,不让 abandoned 静默丢失。

### 4. Release / Abort —— 回收链

- `ReleaseBattleExpected`(`allocator.go:1417`,Model B `reason=completed`):MySQL 持久 proof 与 Redis stable
  identity 原子 CAS → K8s UID delete precondition 回收;`FinalizeBattleReleaseExpected` 收尾。**绝不恢复 Redis TTL**,
  未知结果保留永久墓碑。
- `AbortPreactiveBattle`(`allocator.go:1330`):matchmaker 割当 saga 补偿。先 durable abort journal + Redis fence,
  再 UID 条件 Release + 发 ABANDONED lifecycle,最后 CAS 完成;ACK-loss 重放由 journal 识别。
- legacy `ReleaseBattle`(`allocator.go:1288`)仅按 `match_id` 回收;Model B 下该路径直接 `ErrInvalidArg`
  (防旧请求误杀同 match 重建的新 UID)。

## 战斗实例状态机

`state` 字段(`internal/biz/allocator.go` 常量 + `internal/data/battle.go` 常量):

```
AllocateBattle ─► allocating ─► warming ─► ready/running ─► ended        (正常:结算回收)
   (claim)          占位         等 ready     可玩(DS 心跳)       └─► abandoned  (心跳超时 / 空场兜底 → 补偿)
```

**永久墓碑态(不带 TTL,旧 writer 一律只读跳过,防重复 GSA POST / 误杀重建实例)**:

| 状态 | 语义 | 常量锚点 |
|---|---|---|
| `allocation_uncertain` | allocation_id 已封死、GSA POST 结果未知 | `data/battle.go` `BattleStateAllocationUncertain` |
| `allocation_reconcile_release_pending` | 已解析唯一 UID、DELETE 前 durable 捕获 | `BattleStateAllocationReconcileReleasePending` |
| `allocation_reconcile_empty_tombstone` | 首次对账未见 GameServer,但迟到 POST 仍可能应用 | `BattleStateAllocationReconcileEmptyTombstone` |
| `preactive_release_pending` | 已确认 UID 的未激活分配正在回收 | `BattleStatePreactiveReleasePending` |
| `allocation_abort_pending` | matchmaker 签名 abort 写下的永久 fence | `BattleStateAllocationAbortPending` |

> 这些态的共同不变量:**任何外部结果未知宁可保留永久 fence 返回 `Unavailable`,也不删 claim / 不发第二次 POST**;
> 只有 UID 条件回收 + lifecycle 投递都明确成功后才恢复有界 TTL。设计背景见
> [`ds-arch.md`](../../../docs/design/ds-arch.md)、[`agones-dev.md`](../../../docs/design/agones-dev.md)。

## 存储(Redis)

| Key | 类型 | TTL | 用途 |
|---|---|---|---|
| `pandora:ds:battle:{<match_id>}` | string(`BattleStorageRecord` pb) | `battle_ttl`(默认 2h) | 战斗实例镜像(pod/addr/UID/epoch/allocation_id/roster/state) |
| `pandora:ds:active` | ZSET(score=`last_heartbeat_ms`,member=`match_id`) | 无(索引) | 心跳超时扫描 + 补偿 outbox |
| `pandora:ds:auth:{<match_id>}` | string(pb) | `battle_token_ttl` | **Model B** 授权权威(pending/active DS 凭据) |
| `pandora:ds:authgen:{<match_id>}` | string | — | **Model B** 实例 epoch/gen 单调计数 |
| `pandora:ds:allocation-abort:{<match_id>}` | string(pb) | — | **Model B** 割当 abort journal |
| `pandora:ds:allocation-lifecycle-published:{<match_id>}` | string | — | **Model B** ds.lifecycle 已发布 durable proof |
| `pandora:gm:queue:{<match_id>}` | LIST(`GmCommand` pb) | 30m(每次入队刷新) | GM 指令队列(`internal/gm/gm.go`,LPUSH/RPOP + LTRIM 256) |

`{<match_id>}` hashtag 锁同一 Redis Cluster slot;状态写用 WATCH/MULTI/EXEC 乐观锁,冲突重试耗尽返
`ErrDSAllocationFailed`(`internal/data/battle.go` 包注释)。

## 配置项(`internal/conf/conf.go`)

`allocator.*`(私有配置,`AllocatorConf`):

| 键 | 默认 | 说明 |
|---|---|---|
| `heartbeat_timeout` | `15s` | DS 心跳超时阈值(不变量 §4);超过没心跳 → abandoned |
| `sweep_interval` | `5s` | 后台心跳超时扫描间隔 |
| `battle_ttl` | `2h` | 战斗镜像 Redis key TTL(防僵尸;须 ≤ `ds_auth.battle_token_ttl - 15m` 重连余量) |
| `ready_wait_timeout` | `10s`(dev yaml 设 `120s`,INC-20260727-001 Artic01 冷加载) | `AllocateBattle` 等 DS ready 心跳的最长时间;同时是 sweep 对 warming 的判弃宽限(双阈值判弃,见 §3);冷加载大图须调大 |
| `empty_battle_timeout` | `5m` | 空场超时兜底(活跃但 `player_count==0` 超此时长判 abandoned);负值禁用 |
| `owner_addr` | `""` | owner 权威服务地址(空=不双写实例租约,migrate ⑥) |
| `owner_lease_required` | `false` | 实例租约续租失败是否令心跳失败(弱依赖→强依赖切换) |
| `allocation_abort_auth_secret` / `allocation_abort_auth_audience` | — | matchmaker→abort 服务鉴权密钥/受众(Model B 必填,须独立信任域) |
| `mock_ds_addr_host` / `mock_ds_port_base` / `mock_ds_port_range` | `127.0.0.1` / `30000` / `1000` | `mode=mock` 假地址参数(port = base + match_id % range) |

顶层 / 后端选装:

| 键 | 默认 | 说明 |
|---|---|---|
| `mode` | 留空按 legacy `enabled` 推导 | `local`(本机 exec Windows UE DS)/ `agones`(k8s 生产)/ `mock`(假地址离线联调) |
| `locator_addr` | `""` | player_locator 地址,续期 BATTLE presence(弱依赖;空=不续期,不得据此回 Hub) |
| `kafka.brokers` | — | `pandora.ds.lifecycle` producer(Redis authority / Agones+enforce 下强依赖) |
| `ds_auth.mode` / `ds_auth.authority_mode` | `off` / `legacy` | DS 回调令牌校验挡位(off→permissive→enforce);`authority_mode=redis` = Model B(须 agones+enforce+签名密钥) |
| `agones.fleet_name` / `map_fleets` / `canary_*` / `capacity_watch_interval` / `capacity_warn_ratio` | — / `30s` / `0.8` | Agones 通用池 + 按 map 专属预热池 + canary 轨 + Fleet 容量巡检 |
| `local_ds.executable_path` / `loader_map` / `port_base` | — / — / `7777` | 本机 UE DS 可执行文件 + 端口池;`loader_map` 非空则 DS 统一启到加载/分发关卡,由 UE Loader 查表决定目标图 |
| `config_table.dir` | `""` | 策划配置表 active 目录(§9.15)。**`mode=local` 必配**(除非配了 `loader_map`):allocator 按 `map_id` 现查关卡表(`g_关卡.xlsx` 的 `asset_path` + `game_mode_class`)拼 DS 关卡 URL。查不到即分配失败,不回退兜底图。热更走 `ConfigTableAdminService.ReloadConfigTable`(:20020),新增副本无需重启 |

> **Model B 激活门(`main.go`)**:`authority_mode=redis` 要求 `mode=agones` + `ds_auth.mode=enforce` + 签名密钥,
> 缺一即 fail-closed 拒启动,不存在「配置说 redis authority、实际悄悄回退 legacy」的半开状态。

## 本地启动

```powershell
# 1. 基础设施(redis 强依赖 + kafka;起 matchmaker / player_locator / owner 后可跑全链)
pwsh tools/scripts/dev_up.ps1

# 2. 启 ds_allocator(dev 默认 mode=local:匹配成局后本机 exec 打包好的 Windows UE DS)
go run ./services/battle/ds_allocator/cmd/ds_allocator -conf services/battle/ds_allocator/etc/ds_allocator-dev.yaml
```

> dev yaml 默认 `mode: local`,需要 `local_ds.executable_path` 指向打包好的 `PandoraServer.exe`;若只想跑后端骨架
> 而无真实 DS,把 `mode` 改为 `mock`(确定性假地址)。上集群改 `mode: agones` + `agones.enabled: true` + `fleet_name`。
> `ready_wait_timeout` / `server.grpc.timeout` / matchmaker `ds_allocate_timeout` 四处须一起调大以覆盖冷加载大图。

## 关联文档

- [`go-services.md §2.11`](../../../docs/design/go-services.md) — ds_allocator 服务要约(RPC / 职责 / 不变量)
- [`ds-arch.md`](../../../docs/design/ds-arch.md) — 战斗 DS 调度 / 生命周期总体架构
- [`agones-dev.md`](../../../docs/design/agones-dev.md) — Agones Fleet / GameServerAllocation / 空场自结算 / JTI 有界纪律
- [`decision-revisit-ds-callback-auth.md`](../../../docs/design/decision-revisit-ds-callback-auth.md) — DS 回调令牌 / Model B 授权权威
- [`decision-revisit-internal-service-auth.md`](../../../docs/design/decision-revisit-internal-service-auth.md) — `AbortPreactiveBattle` 服务鉴权信任域
- [`battle-reconnect.md`](../../../docs/design/battle-reconnect.md) — 重连 no-freeze 契约 / BATTLE presence 续期
- [`owner-authority.md`](../../../docs/design/owner-authority.md) — owner_epoch 权威本体 / 实例租约 migrate ②③⑥
- [`decision-dungeon-entry-modes.md`](../../../docs/design/decision-dungeon-entry-modes.md) — `map_id` 透传 / 选副本 / PVE 副本
- [`zero-downtime-update.md`](../../../docs/design/zero-downtime-update.md) — canary 发布轨 / 不停服滚动更新
- [`local-mode-dsticket-v2-migration-handoff.md`](../../../docs/design/local-mode-dsticket-v2-migration-handoff.md) — local-off-v1 完整 Model-B tuple 交接
