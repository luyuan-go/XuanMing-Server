# UE 主链路 + 本地 Agones 联调设计（W4 ⑬）

> 2026-06-09。承接「UE↔后端 gRPC-Web Login + Subscribe + Kafka Push 已通过」，推进 UE 主链路：
> **登录 → 拉/分配 Hub DS → 进大厅 → 匹配 → 拉/分配 Battle DS → 进战斗 → 结算 → 回大厅**。
>
> 本文是设计/契约层；本地 Agones 环境搭建与 apply 命令见 [`deploy/k8s/agones/README.md`](../../deploy/k8s/agones/README.md)。
> UE 侧代码在独立仓库 `Pandora-Client`，命名一律 **Pandora**。

---

## 1. 主链路全景 + 各段责任

```
[UE Client] --gRPC-Web/Envoy--> login.Login
     login --gRPC--> hub_allocator.AssignHub  ──► 真实 hub_ds_addr + hub_ticket(JWT)
[UE Client] --NetDriver--> Hub DS(进大厅, 全图自由 PvP)
     Hub DS --gRPC every5s--> hub_allocator.Heartbeat
     Hub DS --gRPC--> player_locator.SetLocation(HUB)            ← 数据面上报
[UE Client] --gRPC-Web--> matchmaker.StartMatch ... ConfirmMatch
     matchmaker --gRPC--> ds_allocator.AllocateBattle ──► 真实 battle_ds_addr
     matchmaker 签 battle_ticket + player_locator.SetLocation(MATCHING/BATTLE)
     matchmaker --kafka pandora.match.progress--> push --stream--> Client(进战斗通知)
[UE Client] --NetDriver--> Battle DS(5v5 战斗)
     Battle DS --gRPC every5s--> ds_allocator.Heartbeat
     Battle DS --gRPC ReportResult--> battle_result(结算 + Elo MMR)
     战斗结束 → Client 回 Hub DS, Hub DS SetLocation(HUB, fence=match_id)
```

### 各段当前状态（后端 vs UE）

| 链路段 | 后端 | UE（Pandora-Client，独立仓库）|
|---|---|---|
| 登录 gRPC-Web | ✅ login（W3）| ✅ `UPandoraBackendSubsystem.Login`（已通）|
| 分配 Hub | ✅ hub_allocator.AssignHub（W4 ⑤/⑥）+ **Agones 发现（W4 ⑬）** | ⬜ NetDriver 连 Hub DS（客户端段）|
| 进大厅 | ✅ login 返真实 hub_ds_addr（agones.enabled=true 后）| ⬜ NetDriver 连 Hub DS |
| Hub 心跳 | ✅ hub_allocator.Heartbeat | ✅ `APandoraHubGameMode` 骨架 + Agones SDK 已落（每 5s 调，§3 契约）|
| 组队 | ✅ team（W3 ⑦）| ✅ `UPandoraBackendSubsystem` 7 RPC（CreateTeam/Invite/Accept/Leave/Kick/SetReady/GetTeam，§6）|
| 匹配 | ✅ matchmaker（W4 ①/⑦）| ✅ `UPandoraBackendSubsystem` 4 RPC（StartMatch/Cancel/Confirm/GetMatchProgress，§6）|
| 分配 Battle | ✅ ds_allocator.AllocateBattle + **真 Agones（W4 ⑫）** | ⬜ NetDriver 连 Battle DS（客户端段）|
| 进战斗推送 | ✅ kafka match.progress → push stream | ✅ OnPushFrame 已通 |
| Battle 心跳 | ✅ ds_allocator.Heartbeat | ✅ `APandoraBattleGameMode` 骨架 + Agones SDK 已落（每 5s 调，§3 契约）|
| 结算 | ✅ battle_result（W4 ③/⑨）| 🟡 Battle DS 经 `ReportResult` 同步上报（§5，非 kafka）|
| locator HUB/BATTLE 上报 | ✅ guard + fence（W4 ⑩/⑪）| ✅ Hub DS `SetLocation(HUB)` 骨架已落（带 fence，§4）|

> **结论**：后端主链路骨架已全部就位；UE DS 后端联调骨架（心跳 / SetLocation / ReportResult）
> 已在 Pandora-Client 当前脏工作树落地（见 §5）。**DS gRPC-Web 入口 wiring（§5.1 方案 A）已在本仓库
> envoy.yaml 落地**（:8444 独立 DS 面），本地/集群启动脚本会强制重载静态 Envoy 配置并注入
> Fleet 回调地址。UE 当前实现已解析 UID/annotation bundle、维护 active+staged、在七个受保护业务回调
> 统一带 active Bearer；玩家准入已拍板为 B1（RS256 + public JWKS 纯本地验票），Login 的
> `VerifyDSTicket` 仅保留兼容/诊断用途，不是每次 PreLogin 的网络依赖。但生产行为激活仍被
> `decision-revisit-ds-callback-auth.md` §7.15–§7.16 的状态机/部署项阻断，不能由本段“已落地”推导为可上线。

---

## 2. Agones 两模型（后端已实现，详见 deploy README §0）

- **战斗 DS = 按需分配**：`ds_allocator/internal/data/agones_allocator.go`（W4 ⑫）POST GameServerAllocation。
- **大厅 Hub DS = 常驻分片**：`hub_allocator/internal/biz/agones_fleet.go`（W4 ⑬）LIST GameServer
  （`agones.dev/fleet=pandora-hub-{stable|canary},pandora.dev/release-track=<track>,pandora.dev/region=<region>`），
  lazy-seed 分片到 Redis。
- 两者 `agones.enabled=false` 默认走 Mock，`=true` 走真 Agones。**biz 逻辑零改**，只换 provider + main 装配。

Stable/Canary 是四个物理 Fleet，而不是同池随机挑 Pod。Battle 对 `match_id`、Hub 对 `player_id` 用
稳定 seed 做确定性 cohort；实际命中 track 会持久化，并进入 DS metadata 与 DSTicket claims。详见
[`zero-downtime-update.md` §6.3.1](zero-downtime-update.md)。

### 2.1 Hub Fleet 自动扩缩容（2026-06-15）

大厅「常驻分片」在拓扑发现之上加一层**按在线人数自动扩缩容 Hub Fleet 副本**的策略，
走 Agones Fleet 副本控制（直接读/改 Fleet `spec.replicas`），**不引入 FleetAutoscaler CRD**，
后端自己 reconcile（与心跳超时 sweep 同节拍，复用 `hub.sweep_interval`）。

> 当前 Stable/Canary 双 Hub Fleet 模式禁止同时启用旧的单 Fleet autoscale：构造 provider 时会
> fail-closed。两个轨的容量要由发布/运维显式管理，不能让旧 scaler 只改 Stable 而造成 Canary 漂移。

- `HubFleetScaler` 接口（`hub_allocator/internal/biz/fleet.go`）：`GetFleetReplicas` /
  `SetFleetReplicas`。**仅 `AgonesHubFleetProvider` 实现**：GET Fleet 读 `spec.replicas`，
  `application/merge-patch+json` PATCH `{"spec":{"replicas":N}}`（标准库 net/http，零新增依赖）。
  `MockHubFleetProvider` 是拓扑-only **不实现 scaler**（不给退化 no-op，避免门控误判）。
- 门控：`hub.autoscale_enabled=true` **且** provider 实现 scaler（即 `agones.enabled=true` 才有真
  scaler）。Mock 模式下 `scaler==nil` → 自动扩缩容/强制整合恒不运行，进程启动打
  `autoscale_inert_under_mock` 告警；搬迁/回收逻辑本身由 biz 单测覆盖。
- 策略：
  - 开服默认拉起 `hub.min_replicas`（默认 1）个大厅。
  - `desired = ceil(total_players / hub.players_per_hub)`（默认 500/hub），受 `hub.max_replicas`
    （默认 20）上限约束，**稳态只扩不缩**（避免抖动）。
  - 总在线 = 0 → 回收到 `hub.min_replicas`（空大厅自动回收）。
  - `AssignHub` 遇分片全满（`ErrHubNoAvailable`）→ 兜底 `+1` 扩容，上游重试进新大厅。
- Hub Fleet 默认 `replicas: 1`（`deploy/k8s/agones/30-fleet-hub.yaml`，对齐「开服拉起 + 按人数扩」）。
- 阶段限制：当前「空大厅回收」是「总在线=0 才回收到 min」的粗粒度策略；「单大厅空闲 N 分钟回收」
  需再加可配置空闲阈值 + 逐分片空闲计时（留后续）。真集群验 PATCH 扩缩容需 `agones.enabled=true`
  + minikube/Agones 环境（Codex/人）。

### 2.2 强制整合 + 玩家迁移通知（2026-06-15）

在 §2.1「空大厅回收」之上加**主动整合**：低负载时不等大厅自然空，而是**强制把人少的大厅排空、
服务端权威搬迁玩家到该去的大厅，并在切换前给玩家提示**。门控 `hub.consolidation_enabled=true`
（且 `hub.autoscale_enabled=true`）。

- **谁被排空**：reconcile 算 `need = ceil(total_players / players_per_hub)`，ready 分片多于 need
  时按负载升序选**最空的多余分片**标 `draining` + 盖 `draining_since_ms`，保留最满的 need 个分片。
- **怎么搬**：逐分片每 tick 最多搬 `hub.consolidation_batch`（默认 50）人到同 region 最空 ready
  分片，搬迁顺序镜像 `TransferHub`（占新位 → 切归属 → 退旧位）+ 重签 hub 票据，**服务端权威**
  （归属立即转移，账面即时一致；物理玩家最多滞留 `migrate_grace_seconds`）。
- **切换前提示（双通道，互为兜底）**：
  - **通道 A — Hub DS drain 心跳指令**：draining 分片的 Hub DS 下次 `Heartbeat` 收
    `command="drain"` + `grace_seconds`（默认 30）。UE Hub DS 据此弹场内 UMG「N 秒后切换大厅」
    倒计时，到点强制重连（重连走 `AssignHub` 幂等返回迁移后新分片）。
  - **通道 B — Kafka 推送 `pandora.hub.migrate`**：后端搬迁完成按 `key=player_id` 推
    `HubMigrateEvent{from_hub_pod, to_hub_ds_addr, to_hub_ticket, to_hub_pod_name, to_shard_id,
    grace_seconds, reason="consolidation", ts_ms}` → push 服务转发 → 客户端可无缝倒计时后用新
    票据直连新大厅。漏听推送的玩家靠通道 A 兜底。
- **何时缩 pod**：draining 分片**已排空（player_count=0）且过 `migrate_grace_seconds`**后才
  `RemoveShard` + 降 Fleet `spec.replicas`，避免提前杀 pod 打断在场玩家倒计时。
- **阶段限制**：降 `spec.replicas` 后 **Agones 自行挑 GameServer 删，不保证就是被排空那个**；当前
  靠「只在排空且过 grace 后才缩容」规避（被删 pod 已无在场玩家），精确按 pod 删除待接 Agones
  game-server-shutdown SDK 再细化。成员反向索引（`pandora:hub:shard:members:{<pod>}`）是
  best-effort（TTL=assignment_ttl），漂移不影响正确性——通道 A 兜底。cpp pb 同步到 UE 仓库
  留 Codex/人。
- **首次整合降级（部署前已在线的老玩家）**：成员反向索引**只在 `AssignHub`/`TransferHub` 时写入**，
  部署/上线整合功能**之前**就已在线、已有 assignment 的老玩家不在 set 里。`drainAndMigrate` 只枚举
  set 成员，**不会对这些老玩家做通道 B 的服务端权威搬迁 + 推送**；他们靠**通道 A（Hub DS drain 心跳
  → 客户端重连 `AssignHub`）兜底**：幂等路径发现旧分片非 `ready` → 释放旧位重分到 ready 分片，旧分片
  `player_count` 随之递减，**最终一致 + 分片可回收**，只是少了无缝推送体验。降级窗口受 set TTL
  （=assignment_ttl，默认 30min）约束——活跃老玩家每次 `AssignHub`（含重连自愈）都会补回索引。
  `drainAndMigrate` 在 `len(members) < player_count` 时打 `drain_members_index_incomplete` 告警便于观测。
  若后续要对老玩家也做服务端权威搬迁，需让 Hub DS 心跳/上报带成员列表或加索引 backfill（非本期范围）。
- **空大厅缩容防 stale 镜像**：总在线=0 时**不直接把 Fleet 缩到 min**，而是先把超出 `min_replicas`
  的空 ready 分片标 `draining` + 盖 `draining_since_ms`，交回收路径删镜像后再降 `spec.replicas`。
  否则盲缩 Fleet → Agones 删掉的 pod 只会被心跳超时扫成「无 `draining_since_ms`」的 `draining` 分片，
  `reclaimDrainedShards` 跳过它 → 镜像变成不可回收的 stale shard 残留在 `pandora:hub:shards` 集合里。

### 2.3 匹配成功 → Agones 拉起新 Linux Battle DS 完整调用链

> 本节回答「匹配成功后 Agones 怎么启动新的 Linux DS」的完整链路。**关键先纠正一个直觉误区**:
> Agones **不是在匹配成功那一刻才「冷启动」一个 Linux DS pod**。当前被选中的
> `pandora-battle-stable` 或 `pandora-battle-canary` Fleet 预热保持
> `replicas`(dev=2)个**已经在运行、已 Ready** 的 Battle DS pod。匹配成功时 `GameServerAllocation`
> 只是**从预热池里挑一个 Ready 的占走(标 Allocated)**——这一步是毫秒级,不用等 UE Linux DS 冷启动。
> 被占走后,**Fleet 控制器异步再拉起一个新 pod 补回 Ready 缓冲**(这才是「Agones 启动新 Linux DS」
> 的真实时机:发生在分配**之后**为补池,不在玩家进战斗的关键路径上)。

#### 调用链(从客户端确认匹配到玩家拿到 Battle DS 地址)

```
① [UE Client] --gRPC-Web/Envoy:8443--> matchmaker.ConfirmMatch(match_id, accept=true)
        services/matchmaking/matchmaker/internal/service → biz.ConfirmMatch
        UpdateMatchWithLock: 标记该玩家 Confirm
        allAccepted(members) == true → Stage=ALLOCATING, outcome=outcomeAllReady
        触发 ▼

② matchmaker biz.onAllConfirmed(match)                     [biz/match.go:367]
        playerIDs := memberPlayerIDs(match.Members)
        dsAddr, tickets, err := u.allocator.AllocateBattle(ctx, matchID, playerIDs)  ▼

③ matchmaker data.GrpcDSAllocator.AllocateBattle           [data/ds_allocator.go]
        --gRPC unary--> ds_allocator.AllocateBattle(match_id, player_ids, map_id, game_mode)  ▼
        (拿到 ds_addr + allocation_id + DS uid/epoch + 实际 release_track 后)
        为每个玩家 DSTicketSigner.Sign(pid, DSTypeBattle, authoritative target)
        → battle DSTicket v2(RS256,默认 120s/上限 180s;DS 不可信,票据由可信后端签)

④ ds_allocator biz.AllocateBattle(match_id, ...)           [ds_allocator biz/allocator.go]
        幂等检查: repo.GetBattle(match_id) 命中且已有分配后有效心跳(ready/running) → 直接回已有 ds_addr;
          命中但仍 warming → 继续等 ready 心跳;终态/不可用 → 分配失败(防 matchmaker 重试重复拉 DS)
        未命中 ▼
        podName, addr, err := u.alloc.Allocate(ctx, matchID, mapID, gameMode)  ▼

⑤ ds_allocator data.AgonesGameServerAllocator.Allocate     [data/agones_allocator.go]
        构造 GameServerAllocation JSON:
          apiVersion: allocation.agones.dev/v1
          desired_track = deterministic(canary_seed, match_id)
          spec.selectors[*].matchLabels:
            { agones.dev/fleet: pandora-battle-<track>, pandora.dev/release-track: <track> }
          spec.metadata.labels: { pandora.dev/match-id, map-id, game-mode, release-track }
        --HTTP POST(带 ServiceAccount Bearer token + CA)-->
          {apiServer}/apis/allocation.agones.dev/v1/namespaces/{ns}/gameserverallocations  ▼

⑥ 【Agones controller(k8s 集群内)】★ Linux DS 真正被「占用/补充」的地方
        - 从所选 track Fleet 的 Ready 池里挑 1 个 GameServer → 标 Allocated(毫秒级,不冷启动)
        - Fleet 控制器发现 Ready 数 < replicas → 异步 kubelet 拉起 1 个新 Battle DS pod 补回缓冲
          (UE Linux Dedicated Server 容器冷启动,Agones SDK Ready 后进池,供下一场匹配用)
        - 返回 status.state=Allocated + gameServerName + address + ports[0].port  ▲

⑦ AgonesGameServerAllocator.Allocate 解析响应
        state=="Allocated" 才算成功(UnAllocated/Contention → ErrDSNoAvailable 5001)
        addr = "{status.address}:{status.ports[0].port}"
        严格回读 uid/allocation-id/release-track annotation 与 label，返回权威实例身份 + addr  ▲

⑧ ds_allocator biz 写 warming 镜像 → 等 DS 心跳 ready → 返回
        ⚠ Agones state=Allocated 只说明 pod 被分配,不代表 DS 进程已读到 pandora.dev/match-id;
          若此时就把 ds_addr 回给 matchmaker,客户端太快连入时 DS 内部 match_id 仍为 0,PreLogin 会拒票。
        repo.CreateBattle(BattleStorageRecord{match_id, ds_pod_name, ds_addr, state=warming,
          player_ids, allocated_at_ms, last_heartbeat_ms=now, ...}) → Redis pandora:ds:battle:{match_id}
        同步登记 active ZSET(score=last_heartbeat_ms,供心跳超时 sweep,不变量 §4)
        waitBattleReady: 轮询镜像等 DS 用正确 match_id/pod 的 Heartbeat 上报 ready/running
          (last_heartbeat_ms 严格大于 allocated_at_ms,即真实心跳),最长等 ready_wait_timeout(默认 10s)
          • 等到 → 心跳已刷新 active score,返回 AllocateResult{DSAddr, DSPodName}  ▲
          • 超时/ctx 取消 → 用独立 cleanup ctx 回收 pod + 删镜像,返回分配失败(绝不回 ds_addr)

⑨ 回到 matchmaker biz.onAllConfirmed
        UpdateMatchWithLock: Stage=READY, BattleDsAddr=dsAddr
        notifyBattle → player_locator.SetLocation(BATTLE)(标记玩家在该 DS,不变量 §1)
        对每个玩家 pushOne: kafka pandora.match.progress
          MatchProgress{stage=READY, battle_ds_addr=dsAddr, battle_ticket=各自 JWT}  ▼
        删票据、移出 active(玩家已进战斗)

⑩ push 服务消费 pandora.match.progress(key=player_id)→ server stream 推给客户端「进战斗通知」  ▼

⑪ [UE Client] 收到 MatchProgress{READY, battle_ds_addr, battle_ticket}
        --UE NetDriver(UDP)--> 连 Battle DS(带 battle_ticket 校验)

⑫ 被分配的 Battle DS(UE Linux DS)开始服务
        --gRPC unary 每 5s--> ds_allocator.Heartbeat(刷新 last_heartbeat_ms,不变量 §4)
        战斗结束 --gRPC-Web/Envoy:8444 ReportResult--> battle_result(结算 + Elo MMR)
          同一 MySQL 事务写 terminal-release proof；宽限窗后由后端两阶段回收：
          completed(永久 Redis terminal + UID precondition delete) → MySQL released CAS
          → completed-finalize(exact-proof TTL only,不再删 K8s) → delete released outbox
```

#### 关键点与不变量

- **预热 vs 冷启动**:玩家关键路径上拿到的是**预热好的 Ready DS**(步骤 ⑥ 毫秒级占用);Agones
  「启动新 Linux DS」是**分配后异步补池**(不阻塞玩家进战斗)。要让首场分配就有 DS 可用,
  对应 track Fleet 的 `replicas` 必须提供足够 Ready 缓冲。Canary 明确无容量时可二次尝试 Stable；
  transport/超时/响应不确定不能伪装成“无容量”回退。
- **职责切分**:`ds_allocator` 只「拉 pod 返地址」**不签票据**;battle DSTicket 由 `matchmaker` 用
  独立 `pkg/auth.DSTicketSigner` 签（RS256 短时 capability，与 SessionToken/DS callback key 分域）。
- **幂等**:步骤 ④ 同 `match_id` 已有镜像直接回,防 matchmaker 重试导致重复 `GameServerAllocation`
  浪费 Fleet 容量。
- **provider 无关**:步骤 ⑤ 用标准库 `net/http` 直连 k8s apiserver REST,不引 agones/client-go;
  minikube / ACK / 自建集群上的 Agones 分配 API 一致。`agones.enabled=false` 时步骤 ⑤ 换成
  `MockGameServerAllocator`(按 match_id 算确定性假地址,本地无 k8s 也能跑通 ②-⑪)。
- **无空闲 DS**:Fleet 池被占满(`state != Allocated`)→ `ErrDSNoAvailable(5001)` → matchmaker
  `onAllConfirmed` 收到错误 → `onMatchFailed` 整场失败、票据退回队列。生产需配 FleetAutoscaler
  或足够 `replicas` 兜底。
  **5001 只表示容量事实**(INC-20260724-001 收窄):`fleet 名未配置`这类配置错误改回
  `ErrDSAllocationFailed(5002)`,避免混进"无空闲副本"口径让运维照着扩容排查。
  matchmaker 对 5001/5002 处理一致(都算确定性失败),故该收窄不改变上游行为。
- **故障补偿**:DS 崩溃/心跳超时(15s)→ `ds_allocator` `RunHeartbeatSweep` 标 `abandoned` +
  `GameServerAllocation` 对应 GameServer Release + 发 `pandora.ds.lifecycle` 给 battle_result 段位
  回滚(不变量 §4,W4 ⑧ at-least-once)。

#### 联调 / 测试期迭代 DS 镜像的运维纪律（INC-20260724-001 后新增，强制）

事故主因复盘:为排查掉落问题反复 `kubectl delete gameserver` + 重建 battle DS,把
`pandora-battle-stable` churn 到 `ready=0`,同时给 k8s apiserver 加压,导致 Agones 分配与释放
双双 `context deadline exceeded`,玩家匹配成功却分不到 DS、在途对局被葬送。**这是本可避免的操作副作用。**

1. **迭代 DS 镜像必须走 canary 轨**,不要动 stable:
   用 `tools/scripts/start.ps1` 的 `-CanaryBattleDsImage` + `-BattleCanaryReplicas` 预热金丝雀池,
   stable 全程保留可分配容量。这与 §9.21「Canary 必须先预热 Ready 后才接小比例新分配」同源。
2. **任何时刻 `pandora-battle-stable` 的 `ready` 不得为 0**。删/重建 DS 前先确认仍有最小可分配副本;
   宁可多留一个副本,也不要出现"全池为空"的窗口。
3. **禁止删除或强杀仍承载玩家的 Allocated DS**(§9.21 原文)。确认 `state != Allocated` 只是必要条件,
   不是充分条件——高频 churn 会与玩家的异常退局叠加放大清理缺口。
4. **不要用高频删除代替滚动更新**。连续 delete/create 会把 apiserver 打到超时,
   而 Agones 分配、释放确认、owner lease 续租全部经由同一个 apiserver:
   控制面一慢,`allocation_uncertain` 会积压,`ds_allocator` 的 sweep 与 §9.4 补偿一并变慢。
5. **观察信号**:`ds_fleet_capacity_exhausted`(Error)、Grafana「战斗DS容量打满无空闲」critical。
   注意该告警自 INC-20260724-001 起会排除 `desired(spec.replicas)==0` 的 Fleet ——
   未做金丝雀发布时 canary 常态 0 副本属正常,**stable 出现该告警一律当真**。

### 2.4 本机 Windows Battle DS 调试模式（2026-06-16，与 Agones 并列的第二种启动方式）

调试 / 本机联调时不一定有 minikube + Agones,可让 `ds_allocator` 直接在本机 `exec` 打包好的
**UE Windows Dedicated Server** 进程,走完整匹配链路拿到真实可连地址,无需 k8s。三种 DS 启动方式
**互斥**,由 `ds_allocator` 配置选装配(`biz.GameServerAllocator` 接口零改,Mock/Agones/Local 三实现):

| 模式 | 配置 | 实现 | 用途 |
|---|---|---|---|
| Agones | `mode: "agones"` | `data.AgonesGameServerAllocator` | Linux 生产(GameServerAllocation) |
| **Local Windows** | `mode: "local"` | `data.LocalGameServerAllocator` | 本机 Windows DS 进程调试 |
| Mock | `mode: "mock"` | `biz.MockGameServerAllocator` | 确定性假地址,无真实 DS |

- `mode` 是单一权威开关；留空时才按旧 `agones.enabled` / `local_ds.enabled` 推导，供旧配置滚动兼容。
  显式 mode 与 legacy enabled 冲突会启动失败，不靠优先级猜测。
- Windows local 只有 `mode=local + ds_auth.mode=off + authority_mode=legacy + 完整 signer` 才注入
  `PANDORA_DS_LOCAL_PROFILE=local-off-v1`。UE 还会机械核验 Windows、非 Agones、本地 pod 前缀、scope
  与完整凭据；任一不符都保持在线 admission，不能用 profile 字符串单独伪造离线放行。
- **Allocate**:`exec` `local_ds.executable_path`,在 `[port_base, port_base+port_range)` 取空闲端口,
  命令行 `<map_name> -server -log -port=<port>` + `extra_args`;注入 env `AGONES_GAMESERVER_NAME`/
  `PANDORA_MATCH_ID`/`PANDORA_MAP_ID`/`PANDORA_GAME_MODE`(对齐 UE DS 侧 `PandoraAgonesProvider` 读取),
  返回 `advertise_host:port`(默认 `127.0.0.1`)。同 `match_id` 幂等(已在台账直接回原地址)。
- **Release / abandoned**:`Kill` 对应进程;台账无此 pod 视作已释放(幂等)。`ds_allocator` 进程退出
  时 `Close` 杀光在管 DS,避免遗留孤儿。进程自行崩溃由 reaper goroutine 清理台账释放端口(镜像仍靠
  心跳超时 sweep 标 abandoned,与 Agones pod 崩溃同语义)。
- **DS stdout/stderr** 落 `local_ds.log_dir`(默认 `run/dev/logs/ds`)下 `<pod>.log`,便于调试。
- **配置示例**见 `services/battle/ds_allocator/etc/ds_allocator-dev.yaml` 的 `local_ds` 段;打包好的
  UE Windows Server 在 `C:\work\Pandora-Client-SVN`(SVN 客户端工程)下,`executable_path` 指向其
  `PandoraServer.exe`。
- **职责切分不变**:Local 模式同样只「拉进程返地址」,battle DSTicket 仍由 `matchmaker` 签;客户端经
  `pandora.match.progress` 推送拿到 `battle_ds_addr` + `battle_ticket` 后用 NetDriver 连入本机 DS。

#### Linux(Agones)vs 本机 Windows 时序对比

`Allocate` 分叉:Agones 从**预热池占现成 Ready pod**(毫秒级),Local 是**当场 exec 新进程**;
两路拿到 pod 后 `ds_allocator` 都**先把镜像写 `warming`、阻塞等 DS 上报 `ready` 心跳才回地址**
(见 §1 流程 ⑧、`ready_wait_timeout` 默认 10s)。所以客户端拿到地址时 DS 一般已 `listen`,
首连基本成功;§3.3 的退避重试只作边界兜底(Local 冷启动慢时 ready 心跳来得晚,等待窗口更长)。

```mermaid
sequenceDiagram
    participant MM as matchmaker
    participant DA as ds_allocator
    participant K8S as k8s/Agones
    participant POOL as Fleet 预热池
    participant CLI as 客户端

    Note over MM,CLI: ① Linux / Agones —— 占预热好的 Ready pod
    MM->>DA: AllocateBattle(match_id)
    DA->>DA: 写 Redis 镜像(warming)+ active ZSET
    DA->>K8S: POST GameServerAllocation
    K8S->>POOL: 选一个 Ready GameServer 占用
    POOL-->>K8S: status.address:port（已就绪）
    K8S-->>DA: Allocated（毫秒级）
    POOL-->>DA: Heartbeat(state=ready, match_id)
    DA-->>MM: (podName, addr)（等 ready 心跳才回）
    MM->>CLI: match.progress(battle_ds_addr, battle_ticket)
    CLI->>POOL: NetDriver 连入（DS 已 listen，一次连上）
    Note over POOL: Agones 异步补一个新 Ready pod 进池

    Note over MM,CLI: ② 本机 Windows —— 当场 exec 新进程
    MM->>DA: AllocateBattle(match_id)
    DA->>DA: 写 Redis 镜像(warming)+ active ZSET
    DA->>DA: 端口池取空闲 port + exec PandoraServer.exe
    DA->>DA: 轮询等 DS ready 心跳（ready_wait_timeout 内）
    DA-->>MM: (podName, addr)（DS 已 ready/listen 才回）
    MM->>CLI: match.progress(battle_ds_addr, battle_ticket)
    CLI->>DA: NetDriver 连入（DS 已 listen）
    Note over CLI: 偏态边界仍可退避重试（§3.3）
```

`Release` 分叉:Agones DELETE pod 后 **Fleet 自动补一个新 Ready pod**(池子恒维持 `replicas`);
Local `Kill` 进程后 **不补**,端口立即归还池子,下一局匹配才再 exec。

```mermaid
sequenceDiagram
    participant DA as ds_allocator
    participant K8S as k8s/Agones
    participant POOL as Fleet 预热池
    participant PROC as 本机 DS 进程

    Note over DA,POOL: ① Linux / Agones —— 删 pod + 自动补池
    DA->>K8S: DELETE gameserver（404 视作已释放，幂等）
    K8S->>POOL: 回收该 pod
    POOL->>POOL: 自动拉起新 Ready pod 维持 replicas

    Note over DA,PROC: ② 本机 Windows —— Kill 进程，不补
    DA->>PROC: Kill（台账无此 pod 视作已释放，幂等）
    PROC-->>DA: 退出 → reaper goroutine 清台账 + 释放端口
    Note over DA: ds_allocator 自身退出时 Close() 杀光在管 DS 防孤儿
```

### 2.5 UE 5.8 Launcher / 源码版 / Installed Build 网络兼容性（2026-06-16 原记录，2026-07-25 按 5.8 更新）

> ⚠️ **联机前提已变（5.8）**：本节原来的「两边 `CompatibleChangelist` 一致才兼容」是 5.7 时代结论。
> 5.8 起客户端在 `FPandoraGameModule` 覆盖了 `FNetworkVersion`（`GetLocalNetworkVersionOverride`），
> NetCL 改为只按 `StrCrc32("Pandora_<PandoraNetProtocolVersion>_<EngineNetworkProtocolVersion>")` 计算，
> **不再含引擎构建 CL**，因此「发行版客户端 ↔ 源码版 / Installed Build DS」不再依赖 CL 一致。
> 联机契约以 Pandora-Client 仓库 `Doc/客户端/框架/网络/客户端网络版本互通.md` 为准；下面的 CL 对齐分析仅作历史背景。

UE 客户端能否连上 DS,核心看 `FNetworkVersion` 的输入。**本机实测的 5.8.0 Launcher**
（`E:\Program Files\UE_5.8\Engine\Build\Build.version`）:

| 字段 | 本机 5.8.0 Launcher 实测 |
|---|---:|
| Major/Minor/Patch | `5.8.0` |
| CompatibleChangelist 字段 | `0`（为 0 时 `FNetworkVersion` 回退用 `Changelist`） |
| Changelist | `55116800` |
| IsLicenseeVersion | `0` |
| BranchName | `++UE5+Release-5.8` |
| 有效 NetCL | `55116800`（与 `客户端网络版本互通.md` 记录一致） |

源码版 `D:\UnrealEngine` 的 5.8 `Build.version` **本机不存在,未核实**（5.7 时代记录曾为
`CompatibleChangelist=47537391`，5.8 起该值已不参与联机判定）。

**5.8 联机不再靠 CL 对齐**：客户端已覆盖 `FNetworkVersion`,「发行版客户端 + 源码版 / Installed Build DS」
只要两端二进制都带该 override 即互通（override 红线见 `客户端网络版本互通.md`）。原来「Installed Build 的
`CompatibleChangelist` 必须与 Launcher 一致」的约束在启用 override 后不再适用;未启 override 的旧包之间才需对齐有效 NetCL。

阶段纪律:

| 阶段 | 客户端引擎 | 服务器引擎 | 兼容性要求 |
|---|---|---|---|
| 个人打通链路 | Launcher `UE_5.8` | 源码版 `D:\UnrealEngine` | 5.8 靠客户端 `FNetworkVersion` override 互通,不再靠 CL 一致;两端二进制都要带该 override |
| 团队规模化 | 同一个 Installed Build | 同一个 Installed Build | 推荐方案,单一引擎天然一致 |
| 不推荐但可临时用 | Launcher `UE_5.8` | Installed Build | 带 override 即可;未启 override 的旧包才需人工对齐有效 NetCL |

**一劳永逸的团队方案**:一旦团队上 Installed Build,客户端和服务器都用同一个 Installed Build 出包,
不要长期维护「客户端 Launcher、服务器 Installed Build」两套引擎。这样只有一个引擎版本源,
不会再靠人工对齐网络版本。

验收要求（5.8）:客户端启用 `FNetworkVersion` override 后,联机不再要求 Launcher 与 DS 引擎的
`CompatibleChangelist` 一致;但跨引擎大版本(`EngineNetworkProtocolVersion` 变化)仍天然互斥。换 DS 引擎大版本或
怀疑握手不一致时,以 `客户端网络版本互通.md` 的验证方法(看 `Welcomed by server` / `OutdatedClient`)为准。

---

## 3. DS 业务心跳上报契约（UE 侧实现）

⚠️ **Agones SDK health ≠ Pandora 业务 Heartbeat**。前者让 GameServer 进 Ready（Agones 调度用），
后者是 DS 向 allocator 上报负载/状态（容量判定 + 心跳超时补偿，不变量 §4）。UE DS 两者都要做。

### 3.1 Hub DS → `hub_allocator.Heartbeat`（每 5s 单向 unary）

`HeartbeatRequest`（`pandora/hub/v1/allocator.proto`）：

| 字段 | UE 填法 |
|---|---|
| `hub_pod_name` | Agones GameServer 名（环境变量 / SDK `GameServer().ObjectMeta.Name`）|
| `player_count` | 当前在线人数（hub_allocator 回写对账）|
| `cpu_pct` / `mem_mb` | 进程负载（可选，先填 0）|
| `state` | `"ready"` / `"draining"` / `"stopping"` |
| `ts_ms` | `now` 毫秒 |

> ⚠️ `player_count` 只能表示 DS 当前实际连接数，不能同时承载 allocator 尚未入场的 reservation。
> 当前 `ReserveRoutableSeat` 对该字段递增、下一次心跳又用实报数覆盖，未连接 reservation 可跨多轮
> 心跳被反复抹掉并造成误分配；标准修复与失败矩阵见
> `decision-revisit-ds-callback-auth.md §7.16.4`，人拍板前属于生产阻断。同时必须把 Fleet/allocator
> capacity 与 UE `GameSession.MaxPlayers` 做机械一致性门；当前前者为 500，而现有 Linux 打包配置仍可见
> Engine 默认 16，UE 最终 `Server full` 不能替代 allocator 容量正确性。

响应 `command`：`""`=继续；`"drain"`=停止接新 **+ 强制整合排空**；`"stop"`=自行停机（孤儿分片）。

> `command="drain"` 同时带 `grace_seconds`（强制整合时为 `hub.migrate_grace_seconds`，默认 30）。
> UE Hub DS 收到 `drain` 应：① 停止接新玩家；② 弹场内 UMG「N 秒后切换大厅」倒计时；③ 到点
> 强制玩家重连（`AssignHub` 幂等返回迁移后的新分片）。配合后端 `pandora.hub.migrate` 推送可做
> 无缝切换（见 §2.2 双通道）。allocator 标 `draining` 后不会被 DS 上报的 `ready` 降级。

### 3.2 Battle DS → `ds_allocator.Heartbeat`（每 5s 单向 unary）

`HeartbeatRequest`（`pandora/ds/v1/allocator.proto`）：

| 字段 | UE 填法 |
|---|---|
| `ds_pod_name` | Agones GameServer 名 |
| `match_id` | 本对局 match_id（从 battle_ticket / 分配时下发取）|
| `player_count` | 当前战斗内人数 |
| `state` | `"warming"` / `"ready"` / `"running"` / `"ended"` |
| `ts_ms` | `now` 毫秒 |

响应 `command`：`""`=继续；`"stop"`=自行停机（孤儿 DS / 终态 / 空场超时判弃）。

> ⚠️ **`player_count` 必须上报「当前实际连入的活跃玩家连接数」**(NetDriver 在线连接),不是
> 分配名单人数——后端空场兜底(见下)以此判断 DS 是否空转,常报错值会导致误杀或漏收。

> **心跳超时（默认 15s）→ allocator sweep 标记 abandoned/draining**，Battle DS abandoned 经
> `pandora.ds.lifecycle` 触发 battle_result 段位回滚补偿（W4 ⑧ at-least-once 闭环）。
>
> 补偿链两段由真 UE DS 端到端验证（DS 侧已由 Pandora-Client 仓库接管）：
> - 第一段（DS 心跳超时 → abandoned → `ds.lifecycle`）：Battle DS 心跳中断后，观察 sweep 标
>   abandoned + `ds_lifecycle_published`。
> - 第二段（battle_result 事务出箱 → `player.update` → player 段位回滚）：DS 同步 ReportResult
>   后验 NORMAL Elo 守恒 / ABANDONED delta 全 0 / 幂等 / outbox 清零（见 W4 ⑨）。

#### 空场回收(全员掉线/从未连入,2026-07-06,双层)

对局 DS 里一个玩家都没有(全员掉线不归、或客户端从未连入)时**必须回收**,不许空转到
`battle_ttl`(2h)烧资源。业界标准 empty-server-timeout 模式,两层:

- **主路径(UE 仓库,Battle DS 侧,待实现)**:DS 追踪活跃连接数,归零起「空场计时器」
  (**建议 2~3 分钟**,必须 > 客户端重连总窗口 ~30s + 余量,见 battle-reconnect.md §6);
  有人重连回来即取消;到点 → 按 abandoned 语义走正常结束流程(发 `state="ended"` 心跳前
  先 ReportResult abandoned,或直接停跳交后端兜底)→ `SDK::Shutdown()`。
- **后端兜底(本仓库,已实现)**:`ds_allocator.Heartbeat` 检测对局活跃(ready/running)但
  `player_count==0` 持续超过 `allocator.empty_battle_timeout`(**默认 5m**,须大于 DS 侧
  计时器)→ 判 abandoned + 回收 pod + 回 `command="stop"` + 发 `ds.lifecycle{ABANDONED}`
  段位回滚补偿(与心跳超时补偿同链路,at-least-once 闭环)。存储侧用
  `BattleStorageRecord.empty_since_ms` 盖章跟踪,有人回来清零;从未连入(ready 后无人进)
  同样覆盖。设负值禁用。

### 3.3 客户端连入 Battle DS 的重试契约（UE 客户端侧实现）

⚠️ **本机 Windows DS 调试模式（§2.4）特有的「地址已下发但 DS 还没 listen」窗口必须靠客户端重试兜底。**

- **背景**:`ds_allocator.AllocateBattle` 现在会先把镜像写 `warming`,**阻塞等 DS 用正确 match_id/pod
  的 Heartbeat 上报 `ready`/`running`** 才把 `battle_ds_addr` 回给 matchmaker(超 `ready_wait_timeout`
  默认 10s 未就绪 → 回收 pod + 分配失败)。因此客户端拿到地址时 DS 通常已在 `listen`,
  首连成功率大幅提升;但付压/心跳与 `listen` 就绪间仍可能有毫秒级窗口,重试仍作兜底。
  (本机 Windows 调试模式、Agones Linux 生产两路径现在都走该 ready 门控,客户端不必分叉。)
- **客户端契约**(`match.progress` 推到 `MatchStage=READY` 拿到 `battle_ds_addr` + `battle_ticket` 后):
  1. 用 `ClientTravel` / `NetDriver` 连 `battle_ds_addr`,失败不立即报错。
  2. **指数退避重试**:建议初始 0.5s,倍增到上限 2s,**总预算 ≥ 30s**(覆盖本机冷启动 UE DS 的最坏耗时)。
  3. 重试期间 UMG 显示「正在进入战斗…」,不弹失败弹窗;超过总预算才报错并回大厅。
  4. 携带的 `battle_ticket` 不变（B1 默认 120s、硬上限 180s；30s 重试预算仍在有效窗内）。
- **生产(Agones)同样建议保留这套重试**:网络抖动 / pod 刚 Ready 的边界仍可能首连失败,重试让两种
  方式客户端代码**一致**,不必为调试/生产分叉(只是 Agones 下几乎一次连上,退避循环基本不触发)。
- **后端表现**:DS 起好后正常每 5s 调 `ds_allocator.Heartbeat`(§3.2);镜像先以 `warming` 写入,
  后端等首个带正确 match_id/pod 的心跳把它翻成 `ready`/`running` 后才下发 `battle_ds_addr`;
  之后的心跳只续期 + 容量对账。

---

## 4. player_locator HUB/BATTLE 上报闭环契约（UE 侧实现）

后端守卫（W4 ⑩/⑪）已就位：用 state 识别写入方权威，HUB 上报带 `match_id` 作 fence。
**MATCHING/BATTLE 由 matchmaker 写（控制面），HUB 由 Hub DS 写（数据面）**。UE Hub DS 负责 HUB 上报：

### 4.1 玩家进 Hub DS → `player_locator.SetLocation(HUB)`

`SetLocationRequest{ player_id, Location{ state=LOCATION_STATE_HUB, hub_pod, shard_id, match_id } }`：

| 场景 | `match_id` 填法（fence 令牌）|
|---|---|
| 全新登录进大厅 | `0`（无来源对局）|
| **战斗结束回大厅** | **填刚结束那场的 match_id**（从 battle DSTicket 取）|

- 后端 guard：`cur=BATTLE` 时，仅当 `in.match_id == cur.match_id && != 0` 才允许 `BATTLE→HUB`
  回流（合法）；不匹配/为 0 = stale hub DS 顶 active BATTLE → 拒 `ERR_LOCATOR_CONFLICT=9202`。
- 后端持久化 HUB 记录时**清零 match_id/battle_pod**（fence 仅供判定，进 HUB 后无活跃对局）。
- `cur=MATCHING`（确认期 ~15s）时 HUB 上报一律拒（玩家物理上还连着 hub，但权威态是 MATCHING）。

### 4.2 时序要点（避免顶号冲突）

- Hub DS 进大厅那刻才 SetLocation(HUB)，不要在 matchmaker 写 MATCHING 后还重复刷 HUB。
- 战斗结束回流必须带 fence match_id，否则会被后端正确拒绝（这是防 stale 的设计，不是 bug）。

---

## 5. UE Hub DS / Battle DS 骨架要点（Pandora-Client 仓库，命名 Pandora）

> 仅列后端联调相关的骨架职责；GAS / Iris / Replication 细节见 `ds-arch.md`。

**✅ 已落地（2026-06-09 起，Pandora-Client `Source/Pandora/`）**：

| 文件 | 职责 |
|---|---|
| `Public/Net/PandoraDSBackendSubsystem.h` + `Private/Net/...cpp` | DS→后端 7 个 unary（Hub/Battle Heartbeat、GM Poll/Ack、SetLocation、ReportDisconnect、ReportResult），复用 gRPC-Web codec |
| `Public/Server/PandoraAgonesProvider.h` + cpp | Agones SDK adapter（读 env/downward API：`AGONES_GAMESERVER_NAME` / `PANDORA_MATCH_ID` / `PANDORA_REGION`），Ready/Health/Shutdown 接 Agones 生命周期 |
| `Public/Server/PandoraHubGameMode.h` + cpp | 大厅 DS：5s 心跳 + PostLogin 落 `SetLocation(HUB)`（带 fence match_id，§4） |
| `Public/Server/PandoraBattleGameMode.h` + cpp | 战斗 DS：5s 心跳 + `ReportResultAndEndMatch`（结算同步上报，不报 mmr_delta，§6） |
| `Public/Auth/PandoraDSTicket.h` + `Private/Auth/...cpp` | B1 DS 侧本地校验器：严格 RS256 public JWKS + v2 claims；生产不接受玩家 HMAC/私钥，HS256 只留精确 `local-off-v1` 开发档 |
| `Public/Gameplay/Default/PandoraDSGameModeBase.h` + cpp | DS GameMode 校验基类：`PreLoginAsync` 纯本地验签/租约闸门、`InitNewPlayer` 有界 `jti` 防重放、`PostLogin` 单会话顶号、roster 名单 |
| `Public/Gameplay/Default/PandoraBattleGameMode.h` + cpp | 战斗 DS GameMode：`ds_type=battle` + 强制 `match_id` 绑定（从 `UPandoraAgonesSubsystem` 取）+ roster |
| `Public/Gameplay/Default/PandoraHubGameMode.h` + cpp | 大厅 DS GameMode：`ds_type=hub`，不绑 match_id（大厅常驻分片，开放进入） |

- **模块**：当前暂放客户端模块 `Source/Pandora/`（M1.5 服务端模块未拆）；后续按 CLAUDE §11.3 迁
  `PandoraHubServer` / `PandoraBattleServer`。`UPandoraDSBackendSubsystem::ShouldCreateSubsystem`
  门控 `IsRunningDedicatedServer()`，客户端不背 DS 逻辑。
- **传输方案（与原契约偏差，刻意为之）**：原 §5 设想 DS 走**标准 gRPC**；但原生 gRPC 需引入
  grpc-cpp（80MB+）并改 UE 构建环境，触碰「客户端/DS 零额外依赖」铁律（CLAUDE §12）+ Claude 不动
  构建环境（AGENTS §11.1）。故 DS 复用**已有 gRPC-Web codec**（`FPandoraProtoWriter` +
  `FPandoraGrpcWeb` + `FHttpModule`），与客户端 `UPandoraBackendSubsystem` 同源、零新依赖。
  代价：见 §5.1 需要 grpc-web 入口 wiring。原生 gRPC 路线留作未来可选项（抽象在 subsystem 后，可换）。
- **Agones SDK**：已接入 Agones 生命周期；GameMode 通过 provider 调 Ready/Health/Shutdown。
- **GameServer 名 / match_id**：从 env / Agones downward API / allocation label 透传。
- **玩家身份（B1 RS256 已接线，尚待真 DS E2E 解除生产硬门）**：Hub/Battle DS 在 `PreLogin` + `InitNewPlayer`
  强制校验 DSTicket(JWT)，身份只认票里的 `sub`，**不再信** ClientTravel URL option 的 `?PlayerId=`。
  校验链 = RS256 + JWKS `kid` 精确选键 + `exp-iat≤180s` + `iss`/`aud` + `ds_type` + 唯一实例绑定
  （`ds_uid`/`ds_instance_epoch`，Hub 再绑 `hub_assignment_id`，Battle 再绑 `match_id`/`allocation_id`）
  + `release_track` + roster（battle）+ `jti` 防重放 + 本地授权租约/drain 原子拒新闸门 +
  `PostLogin` 单会话顶号。客户端连接时用 `?ticket=<JWT>` 带票；Login/Redis/:8444 暂时故障时，
  已签且仍在本地租约内的短票仍可准入，这是选 B1 避免持续误拦玩家的核心理由。
- **公开材料与信任域**：四个 Fleet 只挂 immutable public
  `ConfigMap/pandora-dsticket-jwks-r<revision>`，Ready 前核对文件 revision；DS 永不持玩家私钥或 HMAC。
  Model-B callback token 是 DS→后端的另一套身份，可以给 DS 使用，但与玩家 DSTicket keyset 不得混用。
  Login 的 `VerifyDSTicket` 只用于兼容/诊断，也挂同 hash public JWKS；不在 B1 PreLogin 关键路径。
- **本地 JTI 重放缓存有界**：`PreLoginAsync` 本地验证通过后只检查可用性（可清过期项但不新增），
  `InitNewPlayer` 在真正接纳时原子消费；精确 `local-off-v1` 是显式开发例外。缓存保存
  `JTI → ticket exp + clock leeway + 1s`，每次检查/消费先在同一锁内清过期；JTI
  UTF-8 最多 256 bytes。硬上限为
  `min((GameSession.MaxPlayers>0 ? MaxPlayers : fallback_players=256) × entries_per_player_window=16,
  configured_absolute_max=4096, safety_ceiling=65536)`；无效配置或满载均 fail-closed，绝不驱逐仍未
  过期条目（否则会重新允许重放）。这些默认值可配，但写入侧上限和读取/清理规则不可移除。
- **真 DS 验证**：`deploy/k8s/agones` Fleet 已使用 `pandora/battle-ds:dev` / `pandora/hub-ds:dev`；
  心跳 / locator 闭环、战斗结算 → 段位补偿链由真 UE DS（Pandora-Client 仓库）端到端验证
  （Heartbeat + SetLocation、同步 ReportResult + GetMatchResult →
  事务出箱 → player.update → 段位回写）。

### 5.1 DS gRPC-Web 入口 wiring（方案 A 已落地 2026-06-16，本仓库 envoy.yaml）

UE DS 客户端走 gRPC-Web，但内部服务（hub_allocator :50021 / ds_allocator :50020 /
player_locator :50006 / battle_result :50022）裸端口是**原生 gRPC**（HTTP/2 framing），
gRPC-Web 报文打不通。要让 UE DS 端到端跑通，需在内部服务前补一层 grpc_web 转换。

**✅ 方案 A（已采纳并落地）**：给 Envoy 新增一个**独立的 DS 面监听器 `:8444`**
（`pandora_ds_listener`，`deploy/envoy/envoy.yaml`；k8s 同款 `deploy/k8s/agones/16-ds-envoy.yaml`），
挂 grpc_web filter 做协议转换，**按方法白名单（path 精确匹配，deny-by-default）**路由到 5 个内部
服务 cluster（`hub_allocator_cluster` / `ds_allocator_cluster` / `locator_cluster` /
`battle_result_cluster` / `login_cluster`）。docker-compose 已映射 `8444:8444`。

> **⚠️ 只放行 DS 文档明确会回调的 method（2026-07-10 收紧,回应审核 P1）**：
> `HubAllocatorService/Heartbeat`、`DSAllocatorService/Heartbeat`、`GmService/PollCommands`、
> `GmService/AckCommand`、`PlayerLocatorService/SetLocation`、`PlayerLocatorService/ReportDisconnect`、
> `BattleResultService/ReportResult`、`LoginService/VerifyDSTicket`。控制面（Allocate/Release/Transfer/List*/AssignHub 等）、
> 客户端面（ListHubLines/TransferToLine/GetLocation/Subscribe* 等）与后端内部
> （ClearLocation/RefreshHubLocations）**一律不挂 :8444**,未匹配请求 Envoy 直接 404，阻断
> Allocate/Release/Transfer/List/ClearLocation 等非 DS 回调方法。新增 DS 方法必须显式加一行。
> **DS 回调服务令牌(2026-07-10 拍板+实现,审核 P1 #1)**:白名单内 8 个方法由
> `middleware.DSCallbackGuard` 校验 allocator 签发的 scope-bound HS256 JWT(battle 绑 match_id /
> hub 绑 pod),Envoy :8444 盖 `x-pandora-ds-gateway` 标记头区分 DS 面与内部直连;
> Redis authority 下不允许 legacy/permissive fallback；兼容/诊断 RPC `VerifyDSTicket` 被显式调用时，
> 仍会在 Guard 后核验 Redis active/projection、assignment/roster 与 JTI admission marker，但 B1
> `PreLogin` 不依赖该 RPC。生产激活不是手工 permissive→enforce 顺序，
> 必须先完成 §7.15 的机械激活方案。
> 详见 `decision-revisit-ds-callback-auth.md`。

> **为何独立监听器而不复用 :8443**：`:8443` 是客户端面（玩家 SessionToken +
> jwt_authn），jwt_authn rules 只对列出的 path 强制鉴权，**未列出的 path 默认 allow_missing
> 放行**。若把内部服务路由挂 :8443，任意公网客户端可直接调 `battle_result.ReportResult` /
> `locator.SetLocation` 伪造战绩/定位。故 DS 面物理隔离到 :8444。**:8444 不挂 player
> jwt_authn**（DS→后端内部调用不带玩家 SessionToken，内部服务用 `pmw.AuthOptional`）。UE
> NetDriver 层的 DSTicket（PreLogin/InitNewPlayer，§5）只认证「玩家→DS」，**不认证 DS→后端**；
> DS→后端身份由 DS 回调服务令牌承担。当前仅精确 `local-off-v1` 可关闭在线 admission；Redis
> authority 代码要求 enforce，但生产行为激活仍被 §7.15 阻断。回调凭据的生产硬化方向是公钥校验 /
> Envoy jwt_authn 先验（见 decision-revisit-ds-callback-auth.md §3），与玩家 DSTicket keyset 分离。

UE DS `UPandoraDSBackendSubsystem` 的 4 个 Endpoint 默认填裸 gRPC 端口（占位），运行时由 Fleet env
覆盖为 `pandora-envoy.pandora.svc:8444`。同集群 DS 面按 `gateway-decision.md §16` 权威口径走明文
（`PANDORA_DS_ALLOCATOR_TLS=0` / `bUseTls=false`）；仅当 DS 位于集群外并接入已配置证书链的 TLS
边缘时，才显式设为 `1`。

**联调/生产残留（留人决策/真集群验收）**：
- UE 当前脏工作树已实现 annotation/env、active+staged、七个业务回调 Bearer 与在线 Verify；
  Hub/Battle 心跳命令还要求 ACK 精确绑定请求 snapshot 且普通响应回调时仍为 current active，
  `grpc-status` 使用 canonical 有界解析，JTI replay cache 有硬容量与按 exp 回收。当前本地
  Automation 12/12、Editor UBT 714/714 与 Dedicated Server UBT 824/824 已通过；仍须在完成下述
  生产阻断后，以安全 `:8444` 上的真 Hub/Battle 往返作最终交付证据。
- 当前四个 Fleet 已只读挂 public JWKS，并机械禁止 `PANDORA_DS_TICKET_SECRET`、
  `Secret/pandora-dsticket-signer-r<revision>`、`private.pem`、私钥 JWK/`kty=oct`。Stable/Canary 普通发布共用同一 revision，
  不换钥。仍须以真 DS 完成“有效 v2 票准入、篡改/错实例/错轨拒绝、租约过期拒新不断旧、drain”证据；
  在此之前 online 的 DSTicket 硬门保持，不能用运维 ACK 绕过。
- :8444 不可裸露公网；还需 digest pin、旧 RS/Fleet/GameServerSet 回滚保留、集群内固定 digest
  synthetic、etcd/Redis TLS+ACL 与 §7.15 blue/green activation。当前 Fleet 的 `TLS=0` 还会让 active
  Bearer、玩家票与 GM 命令在集群内明文传输；NetworkPolicy 不提供机密性/服务端身份，必须拍板并
  交付 mesh mTLS 或等价双向 TLS。当前禁止 Apply。

**方案 B（未用）**：每内部服务挂一个 grpcwebproxy / Envoy sidecar。
**方案 C（长期）**：DS 换原生 gRPC（引 grpc-cpp，触碰零依赖，暂不做）。

---

## 6. 阶段限制（留后续）

- **Battle DS 结算走同步 `ReportResult` gRPC，非 kafka `pandora.battle.result`**：
  UE 直接生产 kafka 较重，改用 battle_result 已有的同步兜底 RPC，
  复用同一 gRPC-Web 客户端、更轻。生产只消费 `pandora.ds.lifecycle`；无凭据 battle-result topic
  启动即拒绝。落库幂等 + Elo 重算与 terminal-release proof 同一 MySQL 事务；DB commit 成功后
  receipt 即时写失败仍由 durable outbox 恢复，DB commit 失败绝不返回 OK。终态 worker 以
  `completed → released_at_ms CAS → completed-finalize` 两阶段回收，phase2 绝不再删 K8s。
- **UE DS 走 gRPC-Web 而非原生 gRPC**（§5 传输方案偏差）：**§5.1 方案 A 已落地**（:8444 独立 DS 面
  监听器 + 5 个内部服务 cluster）；启动脚本已自动重载 Envoy，并由 Fleet env 覆盖 DS endpoints。
- **玩家身份已上 DSTicket 校验**（2026-06-09）：Hub/Battle DS 在 `PreLogin`/`InitNewPlayer` 强制验
  DSTicket v2（RS256 public JWKS + exp/实例/assignment 或 allocation/release-track/roster/jti），不再信 `?PlayerId=`。
  落地为**可选挂载** GameMode（地图不切到 `APandoraHub/BattleGameMode` 不生效，不影响现有客户端开发）；
  生产已选 B1 纯本地公钥验签，DS 不得持玩家签名 secret；精确 `local-off-v1` 只用于开发。
- hub_allocator `AgonesHubFleetProvider` 只在 region 首次无分片时 lazy-seed，Fleet 扩缩容后新
  GameServer 不自动发现（周期性 reconcile 留后续）。
- 真 UE DS 已接入 Fleet；心跳超时 sweep / locator / 结算闭环仍须用真客户端端到端验收。
- 本地 D7 已完成:minikube + Agones dev + 端到端 hello world 已跑通；生产 k8s 形态（ACK / 自建 / 其他）另行定稿，不再作为 D7 阻塞项。
- 真集群（指向生产形态 Agones）联调 + UE DS 深度玩法验证后，更新本文与 PROGRESS。

---

## 7. UE 客户端 组队 / 匹配 gRPC-Web API（Pandora-Client，命名 Pandora）

> ⚠️ 区分：§5 是 **DS 侧**（Hub/Battle DS → 内部服务）；本节是 **客户端侧**（玩家
> → Envoy:8443 → team/matchmaker），两条链路不同。组队/匹配是玩家发起的、带
> SessionToken 鉴权的客户端调用，**不在 DS 子系统**，落在
> `UPandoraBackendSubsystem`（与 Login/Subscribe 同一客户端子系统）。

**落地（Source/Pandora/{Public,Private}/Net/PandoraBackendSubsystem.{h,cpp}）**：

- 复用零依赖 gRPC-Web codec（`FPandoraProtoWriter/Reader` + `FPandoraGrpcWeb`），
  经 `MakeGrpcWebRequest(FullMethod, Bytes, bWithAuth=true)` 带 `Authorization: Bearer <SessionToken>`。
- **组队 7 RPC**（`pandora.team.v1.TeamService`）：`CreateTeam` / `InviteToTeam(TeamId,Target)` /
  `AcceptTeamInvite(TeamId,InviteId)` / `LeaveTeam(TeamId)` / `KickFromTeam(TeamId,Target)` /
  `SetTeamReady(TeamId,bReady,HeroId)` / `GetTeam(TeamId)`。结果走 `OnTeamResult`（含 `FPandoraTeam`）。
  team 写 RPC 的 player_id 以 JWT sub 为准（请求体不传，方法签名不暴露）。
- **匹配 4 RPC**（`pandora.match.v1.MatchService`）：`StartMatch(TeamId)`（→ `OnStartMatchComplete` 带 MatchId）/
  `CancelMatch(MatchId)` / `ConfirmMatch(MatchId,bAccept)`（→ `OnMatchActionComplete`）/
  `GetMatchProgress(MatchId)`（→ `OnMatchProgress` 带 `FPandoraMatchProgress`）。
- **匹配进度主驱动是 push**：`pandora.match.progress` kafka → push server stream → `OnPushFrame`，
  `GetMatchProgress` 仅作主动轮询兜底。`FPandoraMatchProgress.TeamA/TeamB` 解 packed repeated uint64。

**Envoy wiring（已落地，本仓库 `deploy/envoy/envoy.yaml`）**：客户端组队/匹配路由与 §5.1 DS
独立入口均已补齐：

- `team_cluster`（→ :50010）+ `/pandora.team.v1.TeamService/` route，jwt_authn 要 `pandora_session`（W3 已有）。
- 本轮新增 `match_cluster`（→ :50011）+ `/pandora.match.v1.MatchService/` route + jwt_authn `pandora_session` 规则。
- 故玩家 `UPandoraBackendSubsystem` 经 Envoy:8443 调组队/匹配端到端可通（需 login 拿到 SessionToken 在先）。

**阶段限制**：UE 编辑器编译验证 + 端到端联调留 Codex/人（独立仓库，需 UE 5.8 编辑器）。

---

## 8. 待办：UDP 回程中继 DoS 加固（TODO，暂缓，2026-07-09 登记）

**背景**：`-Mode k8s`（`内网服务器一键启动-k8s集群.cmd`）已支持内网多机客户端——DS advertise 取本机局域网 IP、
容器版 UDP 回程中继（`tools/udp-relay/main.go`）的 docker publish 从 `127.0.0.1` 改成 `0.0.0.0`（`start.ps1` /
`e2e_k8s.ps1` 的 `-RelayBindHost`）。实测客户端从别的机器连 `192.168.2.46:8443` 登录、进 Hub 主城成功。

**风险边界**：下述中继 DoS 风险仅属于 dev/内网联调模式；生产 `online` 不使用本中继，而是让玩家
直连 Agones `status.address:status.ports[0].port`。但“没有中继”**不等于公网 UDP 已证明可达**：若
`status.address` 是玩家不可达的节点内网 IP，或云安全组 / 节点防火墙 / NAT 未放行、映射了错误动态端口，
仍会表现为“登录成功、已拿到 DS 地址，却进不去 Hub/Battle”。该等价风险必须由集群外真实 UDP 探针门禁消除，
不能用 Fleet Ready、Agones SDK health 或 DS 业务心跳替代。

本地中继对局域网开放后暴露 `UDP 7000-8000`（1001 端口）+ Envoy 客户端面 `TCP 8443`；
未鉴权的宿主 DS 面 `8444` 已固定回环，不对局域网开放。中继本身：

- 无鉴权、无限流（JWT 只在 DS PreLogin 校验，中继层谁都能打）。
- 按「源 IP:端口」建 session（`main.go` 每个新源 `net.DialUDP` + 起 goroutine）；UDP 源地址可伪造，
  攻击者伪造海量源端口猛发 → 瞬间堆出大量 session（fd + goroutine + map 项）。
- 清理只每 30s 跑一次、回收 2 分钟内无活动的（`cleanupSessions`）；这个窗口内可堆到打爆句柄/内存。

**暂缓结论**：这是本地/内网 dev 联调载体，可信内网做玩法联调风险可接受。**红线：绝不在公网 IP / 对互联网开放的
机器上跑这个 k8s 模式，也不要把这些端口经路由器端口转发到公网。** 生产严格走 `online` 模式。

**以后要做（未做）**：

1. 中继加 **session 上限 + 每源限速**（超限直接丢包），并缩短清理间隔 —— 挡住伪造源爆量。
2. 防火墙把入站 `UDP 7000-8000` / `TCP 8443` 的**来源限定到联调机网段**（如只放 `192.168.2.0/24`），
   而非任意来源（此项属系统/网络配置，由人执行，AI 不碰防火墙）。
3. 保持生产只用 `online` 模式，绝不暴露 dev 中继。

**生产另有独立发布硬门**：online 配置默认必须省略 `agones.advertise_host`，并按
[`docs/ops/release-checklist.md`](../ops/release-checklist.md) §2.3 从集群外验证每个放量轨道的
`status.address:动态 UDP port` 能到达 exact GameServer UID。若平台确需统一 `advertise_host`，必须先证明
它能把每个动态端口正确路由到对应节点；仅配置一个公网 IP / 域名不算闭环。
