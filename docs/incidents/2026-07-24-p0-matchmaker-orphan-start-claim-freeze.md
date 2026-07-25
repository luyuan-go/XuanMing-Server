# [INC-20260724-001][P0] 战斗中退出后回不到战斗、又匹配不了（matchmaker start-claim 疑似孤儿）

> **状态**：根因确认 / 修复实施中（未关闭）。2026-07-24 基于 matchmaker-pve / ds_allocator 服务端日志确认根因；同日完成代码级机械证明（见 §5.1 更正二）并落码 FIX-1 + FIX-2（服务端，本机单测绿、未在真集群验证）。原始"孤儿 start-claim"主假设与"travel churn 致 presence 失效"归因**均已被证据推翻**，按事故纪律保留原文并标注更正。
> **类型**：`availability`（no-freeze 红线，违反 §9.19 / §9.20 / §9.23）
> **环境**：本机 k8s（测试，集群 192.168.2.28，namespace `pandora`）
> **首次发生时间（UTC）**：2026-07-24 13:14:04（首个 4002；真实卡死起点可能更早，见时间线）
> **首次发现时间（UTC）**：2026-07-24 13:14:04（用户在客户端观察到"点匹配没反应"）
> **负责人**：待指定
> **受影响服务/版本**：matchmaker（commit / image digest 待补）、battle DS（baseline b4375d1 → 重建后 efc6043，见掉落排障记录）、客户端 UMyDsRecoveryCoordinator
> **最后更新**：2026-07-24

## 0. 一句话结论

单人玩家在一局战斗中途退出、该局结算/异常结束后，**登录/owner 侧已判定"不在战斗"（`battle=无 match_id=0`）并把玩家路由回 Hub，但 matchmaker 侧仍认为玩家"已在匹配/对局中"（`StartMatch` 报 4002 `ERR_MATCH_ALREADY_MATCHING`）**，两侧状态打架 → 玩家回不到战斗、又开不了新局、点匹配无反应，构成 no-freeze 红线级卡死。直接根因（谁没清、为什么没及时清那份 per-player start-claim）**仍在调查**：静态代码已确认该 claim 为非终态持久、只能由 saga（`ReleaseMatch`/`CancelMatch`）显式清除，不随 `ticket_ttl` 自愈；异常退出为何没走干净的 `abandoned → ReleaseMatch`、以及最终是哪条路径清掉了残留，尚需该 operation 的 phase 流水/服务端日志佐证。

> **更正（2026-07-24，基于 matchmaker-pve / ds_allocator 服务端日志）——上面这段以"孤儿 start-claim"为直接根因的判断已被推翻：**
>
> 服务端日志证明退局后的旧 claim **早已被清**——`13:14:03` 玩家新的 `StartMatch` **被受理**（`match_start_accepted` op `022d804c`），若旧 claim 还在，`preflightStartClaims` 会先拒。因此：
> 1. **4002 不是老局孤儿，而是客户端恢复循环重复发 StartMatch 的"自撞"**：`13:14:03` 第一发被受理、建了活 claim，紧接着 `13:14:04`/`13:14:12` 的重复发撞自己刚建的 claim → 4002。
> 2. **真正让玩家进不去的直接根因是"分不到战斗 DS"**：这局 `13:14:11` 已 `solo_match_found`，但 `13:14:20` 起 Agones 分配调用连续 `context deadline exceeded`（k8s API/控制面超时，AllocateBattle 延迟 7.7s），ds_allocator 报 `ds_fleet_capacity_exhausted`（battle fleet ready=0）与 `errcode=5002 ErrDSAllocationFailed`，并陷入 `allocation_uncertain_exact_release` 无法确认的清理循环（k8s API 持续超时约 40s）。
> 3. **玩家自己的这局被 liveness 判死**：`13:14:42 match_liveness_failed offline_players=[本人]`——玩家在客户端 travel/恢复循环里被判离线，matchmaker 遂放弃该局。
> 4. **恢复**：`13:24:42 battle_ready_after_heartbeat ds_addr=192.168.2.28:7045`——一台 stable 战斗 DS 恢复可用后，新局成功（即用户重启后进去那次）。
>
> **确证直接根因**：战斗 DS 分配不可用（battle-stable fleet churn 至 ready=0 + k8s API/Agones 控制面超时，allocate 与 release 双双 `context deadline exceeded`），叠加客户端恢复循环重复发 StartMatch（4002 自撞）并使玩家在等 DS 期间被 liveness 判离线。此控制面压力与 fleet churn 与本人为掉落排障反复删/重建 battle GameServer & DS pod 高度吻合（见 §5.3，本人操作已从"放大因素"上升为主因）。前述"非终态 claim 只能 saga 清、不随 TTL 自愈"仍是**真实结构性隐患**，只是本次事故**不是**由它触发。

## 1. 影响与范围

- 玩家影响：单个测试玩家 `player_id=15529223057866752` 在"战斗中退出→重进"后卡死：被路由回 Hub（主城）却无法重回原战斗，从主城点匹配反复 4002 无法开新局，UI 表现为"点匹配没反应"。
- 影响人数/对局/请求数：1 名玩家、1 条残留匹配状态；`StartMatch` 至少两次被拒（13:14:04、13:14:12 UTC），客户端恢复协调器空转 generation 2→13→18。
- 服务影响：matchmaker 逻辑正常运行，未崩溃；表现为对该玩家的准入门持续拒绝。
- 数据与安全影响：无数据丢失、无安全边界突破。存在一条与实际不符的残留 per-player 匹配态（脏状态），及一条僵尸单人队记录（READY，靠客户端轮询续命，非权威、可自然 GC）。
- 开始/结束时间：卡死可观测起点 2026-07-24 13:14:04 UTC；用户重启客户端后已解卡（能进主城/进场）；残留 `pandora:match:*` 键在本次排查时已全空（清理路径已触发，触发者待证）。
- 是否仍可复发：**是**。触发条件（战斗中途退出 / battle pod 被删 churn）未修复，保护缺口未定位。
- 严重级别判定理由：§9.19/§9.20/§9.23 明确"任何路径不得让玩家卡死/进不去场景"为需求级硬约束；本次出现无人驱动收敛的静默卡死（回不去+开不了），判 P0。

## 2. 第一现场与证据

### 2.1 症状

- 客户端症状：
  - 重新登录返回 `battle=无 hub=有 match_id=0`，被路由 登录→选角→主城（Hub）。
  - 主城内 `StartMatch failed: grpc=0 code=4002`（反复），客户端提示"未得到确定受理结果 → 转权威恢复"。
  - `UMyDsRecoveryCoordinator` 空转：`authoritative route is HUB but current connection Admission ACK is pending`、`post-travel authoritative route drifted from expected target; discarding expected target`、反复 `DS ClientTravel generation=2/13/18`。
  - 用户主观："点匹配没反应"。
- 服务端症状：matchmaker `preflightStartClaims` 命中残留 claim → 返回 4002；无崩溃、无 panic。
- K8s/Agones 状态：battle GameServer 在事故前后经历过重部署删除（每次删前确认无 Allocated，但高频 churn 存疑，见 §5.3）。

### 2.2 原始证据

客户端日志（用户提供，`<>` 为客户端本地钟，经 JWT `iat` 校准 = UTC−04:00；本档时间线已换算为 UTC）：

```text
LogPandoraLogin: <09:13:50> Login succeeded: player_id=15529223057866752 hub_endpoint=present
LogMyAccountModel: <09:13:50> 登录成功: player_id=15529223057866752 battle=无 hub=有 match_id=0
LogMyDsRecoveryCoordinator: <09:13:50> authority request completed without DS travel: generation=1 route=1
LogMyMatchModel: <09:13:52> 结算完成, 正在为返回大厅申请新的一次性票据+最新地址(IssueDSTicket hub)...
LogMyDsRecoveryCoordinator: <09:13:56> authoritative route is HUB but current connection Admission ACK is pending
LogMyDsRecoveryCoordinator: Warning: <09:14:02> post-travel authoritative route drifted from expected target; discarding expected target
LogMyMatchModel: Warning: <09:14:03> 忽略迟到 StartMatch 回调
LogPandoraMatch: Warning: <09:14:04> StartMatch failed: grpc=0 code=4002 err=
LogMyMatchModel: Warning: <09:14:04> StartMatch 未得到确定受理结果: grpc=0 code=4002 match=0 ；转权威恢复
LogPandoraMatch: Warning: <09:14:12> StartMatch failed: grpc=0 code=4002 err=
LogMyDsRecoveryCoordinator: <09:14:13> DS ClientTravel: generation=18 attempt=1 route=1 ...
```

排查期 Redis 实况（本机 k8s，pod `redis-86b78f686c-bpvfk`，排查时刻远晚于事故、清理已发生）：

```text
KEYS pandora:match:*                              → (empty)   # start-claim / ticket 已被清
GET  pandora:team:player:15529223057866752        → 16051662879752192
GET  pandora:team:{16051662879752192}  (STRLEN 51, TTL 3260s) → 见 §4 解析
```

`pandora:team:{16051662879752192}` 用 `TeamStorageRecord`（`proto/pandora/team/v1/team.proto:75`）反序列化：

```text
team_id=16051662879752192  captain_id=15529223057866752  state=TEAM_STATE_READY(2)
members=1: {player_id=15529223057866752, ready=true, hero_id=1}  max_size=5
created_at=2026-07-24 13:08:04.123 UTC   updated_at=2026-07-24 13:08:09.645 UTC
```

服务端日志（本机 k8s，pod `matchmaker-pve-6999fdcf87-spb9x` / `ds-allocator-758b9c4d8c-5dfpw`，UTC，当前容器实例仍覆盖事故窗口）：

```text
# matchmaker-pve —— 老局 → 新局自撞 → 分配失败 → liveness 判死 → 恢复
13:08:15.504 match_start_accepted ticket=16051692944556032 op=b8648e86 team=16051662879752192 map_id=7   # 进战斗那局
13:14:03.337 match_start_accepted ticket=16053196183109632 op=022d804c team=16051662879752192 map_id=7   # 新局被受理 → 旧 claim 此刻已清
13:14:11.347 solo_match_found match_id=16053196183109632 players=1
13:14:21~34  ds_allocate_failed match_id=16053196183109632 err=errcode=10 (matchmaker 侧) ×4
13:14:42.417 match_liveness_failed match_id=16053196183109632 offline_players=[15529223057866752]
13:24:33.870 match_start_accepted ticket=16055910602440704 op=035e8d9d ...                                # 用户重启后重试
13:24:42.769 match_ready match_id=16055910602440704 ds_addr=192.168.2.28:7045 players=1                   # 成功

# ds-allocator —— 容量枯竭 + k8s API/Agones 控制面超时
13:12:38 ERROR ds_fleet_capacity_exhausted fleet=pandora-battle-canary ready=0 allocated=0 usage_ratio=1
13:14:20 ERROR gameserver_allocate_failed match=16053196183109632 err=errcode=5002 agones: Post .../gameserverallocations: context deadline exceeded   # AllocateBattle latency 7728ms
13:14:43~15:18 WARN allocation_uncertain_exact_release_(unconfirmed|resume_failed) ... context deadline exceeded  # 连释放确认都被 k8s API 超时卡住 ~40s
13:24:42 INFO battle_ready_after_heartbeat match=16055910602440704 pod=pandora-battle-stable-l8ns2-hvzdj ds_addr=192.168.2.28:7045
13:25:00~17 WARN owner_lease_renew_failed_weak ... DeadlineExceeded + 多条 rpc_slow Heartbeat            # 控制面仍在压力下
```

当前状态（排查时 13:44 UTC）：`pandora-battle-stable` 已恢复 2 Ready，`pandora-battle-canary` 0/0/0（无金丝雀发布时为常态）。事故窗口内的容量枯竭 + 控制面超时已缓解。

> 脱敏说明：客户端日志含 DSTicket JWT，本档一律省略票据主体，仅摘录卡死证明所需字段。

### 2.3 已排除的噪声

- `superseded team snapshot skipped broadcast: req_seq=1/2 latest_seq=3` — 队伍快照按 seq 去重的正常行为，非本事故。
- `SetActorLabel failed: Actor names cannot be empty`、`PSO creation hitches` — 客户端渲染/编辑器噪声，与后端准入无关。
- 僵尸单人队 `TeamStorageRecord`（READY）**不是**卡死元凶：matchmaker 开局门恰好要求 `state==READY`（见 §4/§5.4），此队三项校验全过、不挡新匹配，仅为旁证与次要脏状态。

## 3. 时间线

以 UTC 为主。客户端 `<>` 时钟经首张 hub 票 JWT `iat=1784898832`（=13:13:52 UTC）与日志行 `<09:13:52> IssueDSTicket succeeded` 校准，得 `<>` = UTC−04:00。

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 13:08:04.123 | team | 队伍创建（退出前那次会话，单人 solo） | 解析 `created_at_ms` |
| 13:08:09.645 | team | SetReady（hero_id=1，state→READY），此后 `updated_at` 再未变动 | 解析 `updated_at_ms` |
| （缺口） | matchmaker/battle | 玩家匹配→进战斗→**中途退出**→该局结算/异常结束 | 客户端 `结算完成`；服务端流水待挖 |
| 13:13:50.619 | client/login | 重新登录：`battle=无 hub=有 match_id=0` | 客户端日志 |
| 13:13:50.620 | coordinator | `authority request completed without DS travel: route=1(HUB)` | 客户端日志 |
| 13:13:52.648 | client/match | `结算完成`，申请返回大厅票据 | 客户端日志 |
| 13:13:52.988 | coordinator | DS ClientTravel generation=2 → browse hub 192.168.2.28:7167 | 客户端日志 |
| 13:13:53.728 | client/net | Welcomed by server：MainCity / PandoraHubGameMode | 客户端日志 |
| 13:13:56 / 13:13:58 | coordinator | `route is HUB but Admission ACK is pending`（两次） | 客户端日志 |
| 13:14:02.648 | coordinator | `authoritative route drifted from expected target; discarding` | 客户端日志 |
| 13:14:04.298 | matchmaker | **StartMatch 4002（第一次）** → 转权威恢复 | 客户端日志 |
| 13:14:05.440 | coordinator | DS ClientTravel generation=13（重新 travel hub） | 客户端日志 |
| 13:14:12.927 | matchmaker | **StartMatch 4002（第二次）** | 客户端日志 |
| 13:14:13.997 | coordinator | DS ClientTravel generation=18 | 客户端日志 |
| 13:12:38 | ds_allocator | `ds_fleet_capacity_exhausted fleet=pandora-battle-canary ready=0` | ds_allocator 日志 |
| 13:14:03.337 | matchmaker | 新 `StartMatch 被受理` op=022d804c（旧 claim 此刻已清） | matchmaker-pve 日志 |
| 13:14:11.347 | matchmaker | `solo_match_found` match=16053196183109632 | matchmaker-pve 日志 |
| 13:14:20.737 | ds_allocator | `gameserver_allocate_failed ... context deadline exceeded`（AllocateBattle 7.7s） | ds_allocator 日志 |
| 13:14:21~34 | matchmaker | `ds_allocate_failed errcode=10` ×4 | matchmaker-pve 日志 |
| 13:14:42.417 | matchmaker | `match_liveness_failed offline_players=[本人]` → 放弃该局 | matchmaker-pve 日志 |
| 13:14:43~15:18 | ds_allocator | `allocation_uncertain_exact_release_*` 连续 `context deadline exceeded` ~40s | ds_allocator 日志 |
| 13:24:33.870 | matchmaker | 用户重启后 `StartMatch 被受理` op=035e8d9d | matchmaker-pve 日志 |
| 13:24:42.769 | matchmaker | `match_ready ds_addr=192.168.2.28:7045` → 进场成功 | matchmaker-pve 日志 |
| 13:44（排查时） | infra | battle-stable 恢复 2 Ready；`pandora:match:*` 已空 | kubectl / Redis |

## 4. 调用链与关键变量

4002 准入门：

```text
StartMatch(RPC)
  → MatchUsecase.resolveMembers        // 读 team，要求 state==READY（match.go:797-813）
  → MatchUsecase.ensureNoneInBattle    // locator BATTLE 门（match.go:437）
  → MatchUsecase.preflightStartClaims  // 查 start-claim / player→ticket（match.go:473-510）
       ├─ GetStartPlayerOperation(playerID)  → pandora:match:start:player:{id}
       │    └─ GetStartOperation 非终态 → ErrMatchAlreadyMatching(4002)   ← 疑似命中点
       └─ GetPlayerTicket(playerID)          → pandora:match:player:{id}
            └─ GetTicket 命中 → ErrMatchAlreadyMatching(4002)             ← 疑似命中点
```

释放链（正常应清 claim 的唯一路径）：

```text
battle 结算 / abandoned 补偿
  → battle_result 调 matchmaker.ReleaseMatch(matchID, fallbackPlayerIDs)   // match.go:1037
  → 清 ticket / start-claim / player index / queue（否则下次 StartMatch 撞 4002，见 match.go:1023-1027 注释）
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| `pandora:match:start:player:{id}` | matchmaker `ClaimStartPlayer` | matchmaker；**非终态持久，仅 saga 清**，非 TTL 自愈 | 权威、可变 | 疑似残留 → 4002 直接来源 |
| `pandora:match:player:{id}` / `pandora:match:ticket:{id}` | matchmaker 票据链 | matchmaker；非终态持久，显式终态后按 `ticket_ttl(30m)` 留存 | 权威、可变 | 备选 4002 来源 |
| owner/locator route | login/owner 权威 | 登录时判定=HUB、`battle=无 match_id=0` | 权威 | 与 matchmaker 侧不一致 → 打架 |
| `TeamStorageRecord`(READY) | team 服务 | 客户端 GetMyTeam 轮询 TouchTeam 续 TTL；停轮询后 ~TTL 自然 GC | 非权威投影 | 旁证：team 状态与战斗生命周期脱钩 |

## 5. 根因

### 5.1 直接根因

**仍在调查（未闭合）。** 已确认的事实：

1. 4002 来自 matchmaker `preflightStartClaims` 命中残留的 per-player start-claim / ticket（`match.go:473-510`）。
2. 该 start-claim / discovery index **非终态持久、不随 `ticket_ttl` 过期**，只能由 `ReleaseMatch`/`CancelMatch` 显式清除——配置注释（`matchmaker-pve.yaml:81`：`ticket_ttl 仅显式终态后的留存；QUEUEING/CONFIRM/ALLOCATING/READY 均持久`）与测试 `TestDurableStartOperationAndDiscoveryIndexDoNotExpireAtTicketTTL` 双重佐证。
3. 因此登录/owner 侧"不在战斗"与 matchmaker 侧"仍在匹配/对局"两份 per-player 状态在异常退局后不一致，且 matchmaker 侧的清理依赖一个可能未触发的显式事件。

**尚未证明**（需服务端日志/该 operation phase 流水）：退局瞬间 start operation 停在哪个 phase；`ReleaseMatch` 是否被调用/失败/延迟；最终清空 `pandora:match:*` 的是 saga 补偿还是 operation 走到终态。**在拿到这些证据前，不写"根因确认"。**

> 更正记录（2026-07-24）：排查中曾口头判断"4002 claim 已随 TTL 过期清掉"，经查配置/测试**该判断错误**——非终态 claim 不靠 `ticket_ttl` 自愈。已在本档纠正，保留原判断与更正证据。

**确证直接根因（2026-07-24，服务端日志闭合）：**

1. **战斗 DS 分配不可用**是玩家进不去的直接原因。`13:14:11` 已 `solo_match_found`，但 `13:14:20` Agones 分配 `Post .../gameserverallocations: context deadline exceeded`（AllocateBattle 延迟 7.7s），ds_allocator 报 `ds_fleet_capacity_exhausted`（battle fleet ready=0）与 `errcode=5002 ErrDSAllocationFailed`（matchmaker 侧映射为 code=10 Unavailable）。分配结果"不确定"后进入 `allocation_uncertain_exact_release`，连释放确认都因 k8s API 持续超时卡住约 40s（`gs_gone=false pod_gone=false ... context deadline exceeded`）。
2. **k8s API / Agones 控制面过载**是分配失败的机制：allocate 与 release 双双 `context deadline exceeded`，`13:25` 仍见 `owner_lease_renew_failed_weak DeadlineExceeded` 与大量 `rpc_slow Heartbeat`。与本人为掉落排障反复删/重建 battle GameServer & DS pod 的 churn 时间高度吻合。
3. **玩家在等 DS 期间被 liveness 判死**：`13:14:42 match_liveness_failed offline_players=[本人]`——客户端在 travel/恢复循环里被判离线，matchmaker 放弃该局，需重新发起。
4. **4002 是次要症状**：客户端恢复循环重复发 StartMatch，第一发（13:14:03）被受理建活 claim，后续（13:14:04/13:14:12）撞自己刚建的 claim。**与"退出未同步 locator"无关**——login `battle=无 match_id=0` 证明 locator/owner 侧已正确反映"不在战斗"，且 13:14:03 受理证明 `ensureNoneInBattle`（locator BATTLE 门）与 `preflightStartClaims` 均已放行（旧局状态早已清）。
5. **恢复线性化点**：`13:24:42 battle_ready_after_heartbeat ds_addr=192.168.2.28:7045`——stable 战斗 DS 恢复可用后新局 `match_ready`，玩家进场。

结构性隐患（本次未触发但仍需治理）：非终态 start-claim 只能 saga 显式清、不随 TTL 自愈，若某条退局路径漏调 `ReleaseMatch`，会构成真正的无限期 4002 卡死——单列为 §10 行动项 A2。

---

#### 更正二（2026-07-24，代码级机械证明，推翻上面第 3 条的归因）

上面第 3 条把 liveness 判死归因为「客户端在 travel/恢复循环里被判离线」。**该归因错误**：travel churn 只是放大器，**不是必要条件**。本门对「已成局 match 的成员」这一人群**结构上不可能判对**，机械证明（全部经本人逐行核对 HEAD 代码）：

1. MATCHING 投影的**唯一写者**是 matchmaker 自己的 `notifyMatching`，只在成局那一刻写一次，TTL 30s（`match.go:329-336`）。
2. Hub DS 心跳捎带 `player_ids` 的续期链**显式只认 HUB 态**：`int32(state) != 3 /* LOCATION_STATE_HUB */ || podStr != hubPod { continue }`（`services/runtime/player_locator/internal/data/location.go:241`）。而 MATCHING 是 `LocationStateMatching = 4`（`biz/locator.go:45`）⇒ **MATCHING 记录永不续期**。
3. Hub DS 想把它改写回 HUB 也被拒：`case LocationStateMatching: if in.State == LocationStateHub → ErrLocatorConflict`（`biz/locator.go:224-229`）。
4. locator 侧**没有任何 MATCHING 释放通道**（全仓 grep `LocationStateMatching` 只有写入与守卫，无释放）。

⇒ **成局后 30s 内查不到任何缺席（零真阳性）；超过 30s 后必然全员缺席（全假阳性）。** 与玩家是否真的在线、是否在 travel 完全无关——玩家静止不动坐在主城也必被判死。

事故算术精确闭合：`13:14:11.347 solo_match_found` → `13:14:42.417 match_liveness_failed` = **31.07s = 30s locator TTL + 一个 2s 撮合 tick**。

**第二受害面（同一缺陷的连带）**：MATCHING key 到期消失后，`RefreshHubLocations` 只 `EXPIRE` 不创建（`data/location.go:245`）⇒ locator 记录无法重建，该玩家此后对两道门恒判离线，连「匹配失败后重新排队」的新票据也会被 `livenessSweepOnce` 在 ≤10s 内误删。**故两道门必须一起关。**

**代码注释早已预言本事故**：`internal/conf/conf.go:83-86` 原文——「两道门把『locator 无 HUB 位置记录』判为离线……UE Hub DS 生产端尚未上报该字段前开启，**会把全部在线玩家在 locator TTL（30s）后误判离线、扫掉排队票据**。**必须等 Hub DS 侧联发后才可开启**」。代码默认值是 `false`，而实跑 yaml 被设成 `true`。2026-07-08 的实机验证覆盖的是 **HUB 态**续期链（对排队玩家有效），**不覆盖成局最终门查询的 MATCHING 态成员**。

#### N2（本档原未记录的既有冻结路径，与本次修复强耦合）

**ALLOCATING 期玩家无法取消**，且该阶段无任何自动终态：

- `ConfirmMatch` 对 `stageAllocating` 的 reject 直接 `ErrInvalidState`（`match.go:1230-1233`，修复前）；`CancelMatch → rejectOrReapOrphan`（`match.go:990-995`）只吞 `ErrMatchNotFound/ErrMatchDeclined`，`ErrInvalidState` 原样返回给玩家。
- `expireOnce` 对 `stageAllocating` 显式 keepActive、绝不判失败（`match.go:2981-2985` 原文：「ALLOCATING 是 durable job。外部结果可能未知（尤其 allocation_uncertain），本地时间绝不能把未知推断成失败并重排」）。
- 分配重试无最大次数、无阶段总时限。

⇒ **liveness 误杀门是修复前唯一会终止 ALLOCATING 的路径。** 单独关闭它而不补出口，会把「31 秒被误杀」换成「永久卡死」，触碰 §9.20 与 AGENTS.md §10 红线。因此 FIX-1（关门）与 FIX-2（补出口）**必须同批**。

### 5.2 触发条件

- 玩家在一局（单人 PVE 形态）**战斗中途退出**（切后台/杀进程/断线），使该局走向异常结束或结算。
- 需要该退局路径**未能及时、可靠地**触发 `ReleaseMatch` 清 per-player 匹配态。

### 5.3 故障放大因素

> 更正（2026-07-24）：经服务端日志确认，下述第 1 条"排查期高频删/重建 battle DS"**已从放大因素上升为直接根因链的一环**（见 §5.1）——它把 battle fleet churn 到 ready=0 并加压 k8s 控制面，直接导致 Agones 分配/释放超时。

- **排查期高频删/重建 battle GameServer & DS pod**（掉落排障重部署）：既把 battle-stable fleet 打到 ready=0，又给 k8s API/etcd 加压，致 Agones allocate/release `context deadline exceeded`。属**主因**，非仅放大。
- 客户端 `UMyDsRecoveryCoordinator` 在 route=HUB 与 Admission ACK pending 之间空转、反复 travel（generation 2→13→18），期间**重复发 StartMatch 撞 4002**，且反复 travel 使玩家在等 DS 期间被 matchmaker 判**离线**（`match_liveness_failed`），**主动葬送了自己正在等待分配的那一局**——这是放大观感之外的真实 §9.19/§9.23 收敛缺陷。

### 5.4 为什么现有保护没有挡住

- **Recovery**：客户端协调器持续 travel/重查，但 4002 由服务端权威态给出，客户端本地重试无法清服务端残留 claim（也不应由客户端清）——保护方向正确但缺服务端侧兜底清理，收敛不了。
- **TTL 自愈**：非终态 start-claim 刻意设计为持久（防成局中途丢票），因此 `ticket_ttl` **不兜底**这类孤儿；若 saga 清理链有缺口，卡死时长**不受 30min 上界约束**。
- **abandoned 补偿**：battle DS 心跳超时→abandoned→ReleaseMatch 是设计中的兜底，但玩家已退局、battle pod 可能已被删，该链是否真正跑到 ReleaseMatch 未证。
- **team 状态门**：`resolveMembers` 要求 `state==READY`；残留 READY 单人队恰好满足，不构成拦截也不构成兜底——team 状态与战斗生命周期脱钩，无法用于检测"该玩家其实还挂着一份匹配态"。

## 6. 全仓同类问题扫描

- 扫描基线 commit：当前 HEAD（2026-07-24）。
- 扫描目录和文件类型：`services/matchmaking/matchmaker/internal/biz/match.go`（start saga 全生命周期）、`matchmaker/etc/*.yaml`（liveness 配置）、`services/matchmaking/team/internal/data/team.go`。
- 搜索模式/工具：Grep `ReleaseMatch`/`CancelMatch`/`DeleteStartPlayer`/`startOperationTerminal`/`match_liveness_failed`/`requireLocalGameMode` + 逐函数通读 saga（StartMatch→advance→compensate→fail→cancel→reconcile）。

**A5 结论：start-claim 释放/取消/失败/补偿链未发现确认的孤儿泄漏 bug。** saga 防护完整：

- 终态 = `QUEUED`（成功，交接给 player→ticket 时删 start:player claim，`match.go:762`）或 `FAILED`。
- 失败/取消 → `COMPENSATING` → `compensateStartOperation` 删 player index + start:player claim（`match.go:613-618`）→ `FAILED` → `RemoveStartActive`。
- 非终态 op 常驻 durable active/due 集合（`EnsureStartActive`），带 lease + 指数退避（`deferStartOperation`）重试到终态。
- `reconcileStartOperationsOnce` 每 5s **全 Redis master 全量扫 canonical start operation**，把掉出 due 索引的非终态 op 重新捞回并重建 start:player claim（`match.go:2224-2274`），兜底"索引丢失"。
- `preflightStartClaims` 对**终态** op 的残留 start:player claim 惰性 CAS 清（`match.go:484-490`）。
- game_mode 门（`requireLocalGameMode`）：每个 op 只由 owning-mode matchmaker 驱动，外 mode 实例只跳过不破坏（`match.go:2243/2297`）。

**结构性残余依赖（非 bug，但属可用性依赖，登记为 A5/新行动项）：**
- R1：无孤儿保证依赖 owning-mode matchmaker worker（leader）存活 + 全量扫描能覆盖所有 Redis master。**若某 mode 的 matchmaker 全副本宕机，该 mode 的非终态 op 无人驱动 → 该 mode 玩家 4002 直到恢复**（PVP 实例明确跳过 PVE op）。由 matchmaker HA 兜底但确为依赖。
- R2：`preflightStartClaims` 只清终态 op；真正卡在非终态的 op 在 worker 恢复前无自愈（4002），仅靠 5s reconcile 缓解、不能消除。

**另确认的真实致因（非孤儿，是本次卡死机制的一环）：**
- **成局最终门 liveness 误杀在途局**：`match.go:1676-1704` 的 `findOfflineMembers` 门（由 `liveness_gate_enabled` 控制，**pve/dev/prod 配置均为 `true`**，见 `matchmaker-pve.yaml:72`）在分配 DS 前批量校验成员在线；玩家在客户端 DS travel/恢复循环里 locator presence 短暂失效 → 被判离线 → 其在途 match 被 CAS 翻 FAILED + `failMatch` reap。代码注释本身警告该门有"误判全员离线"的假阳性风险（`match.go:1674`）。判死后清理**干净**（`failMatch` 删过错票 + 释放 claim，故 13:24 能重开），但它**主动葬送了正在恢复的玩家的对局**，与客户端重复 StartMatch（4002 自撞）叠加放大卡死。
- 已排除：多份 per-player 状态无统一收敛点的担忧——本次证据显示各清理链各自正确闭合，不构成本事故因素。
- 未覆盖边界：多人队（本次单人）在同类退局的 claim 清理；Stable/Canary 新旧 matchmaker 组合；locator presence 在 DS travel 期间的真实覆盖率（liveness 假阳性根据）。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 指导用户重启客户端 + 重新登录 → 已解卡（能进主城/进场） | 已完成 | 用户确认"确实现在进去了" | 无；仅客户端侧恢复 |
| 确认排查期 `pandora:match:*` 已全空，无需手工清 claim | 已完成 | Redis `KEYS pandora:match:*` → empty | 无 |
| 僵尸 READY 单人队保留（非权威，客户端停轮询后 ~TTL 自然 GC） | 观察中 | TTL 3260s 递减 | 无需干预 |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| 定位退局未清 claim 的根因（phase 流水 + ReleaseMatch 调用/失败证据） | **已完成**（证伪：无孤儿，见 §5.1 更正一/二） | — | 服务端日志 |
| **FIX-1：两道 presence 判死门回退为关闭**（配置回落到 `conf.go` 代码默认值 `false`），并在两处门与配置注释写明机械原因与重开前置 | **已落码待部署验证** | `services/matchmaking/matchmaker/etc/matchmaker-{dev,pve}.yaml`、`matchmaker-prod.yaml.example`（纳管源头 3 份；`run/**` 共 8 份为 `.gitignore` 的生成副本，由 `gen_cluster_config.ps1` 从源头重生）；`internal/biz/match.go` 成局门与 `livenessSweepOnce` 注释 | V1-1 绿；V1-3/V1-4 未跑 |
| **FIX-2：ALLOCATING 期 pre-checkpoint 取消出口**（仅 `BattleTarget==nil` 且 phase∈{PENDING,REQUESTING} 允许 reject，走既有 `failMatch` 无过错退票；ABORTING/已 checkpoint/READY/未知阶段一律 fail-closed） | **已落码待部署验证** | `internal/biz/match.go`：`ConfirmMatch` reject 分支 + 新增 `cancelableUncheckpointedAllocation` 守卫 | V2-1/V2-2/V2-3 绿（修复前 V2-1 实测失败）；`-race` **阻断** |
| 补齐/加固退局→ReleaseMatch 的可靠触发或服务端兜底清理 | 已证伪，无需（A5 扫描：saga 链完整） | — | — |
| 登录/owner 恢复链对 matchmaker 残留态的一致性核对（评估必要性） | 待设计 | — | — |
| **S1-a：post-travel 保留连接地址（N1 根因修复）** —— `BeginRequest` 的既有 `bPreservePostTravelFence` 分支补齐 `Operation.Address`（仅当 `TargetRoute==Hub`，路由改判则作废旧地址） | **已落码待用户编译验证**（UE 侧，§11.6 由用户编译） | `Pandora-Client-SVN/.../MyDsRecoveryCoordinator.cpp` `BeginRequest` | 用户编译 + E2E；**未编译未验证** |
| **S1-b：post-travel 阶段 deadline（§9.19 点名强制）** —— deadline 查在**既有** `TickAuthorityRetry` 内（该 ticker 已是 post-travel 两条等待的唯一驱动点，故满足「一个共享 watchdog 按 phase 记 deadline」，**不新增 ticker、不新增 timer 状态机**）；用**绝对时间戳**（阶段内每轮退避都换代，按世代验身的看门狗必失效）；到期动作复用与路由漂移分支相同的逃生口（弃用 post-travel 预期 + `RestartAuthoritativeRecovery`），不新增 UI 分支、不改写权威路由；取值复用既有 `TravelAdmissionWatchdogSeconds`（由 `FPandoraDSAdmissionTiming` 单一事实源提供并有 static_assert 与 DS 侧预算绑定，**不拍新数字**）。新增私有 setter `SetPostTravelAuthorityCheck()` 作为标志的唯一写入口，标志与 deadline 同生同灭（标志有 2 处置位、**8 处清除**，散写必漏） | **已落码待用户编译验证** | `MyDsRecoveryCoordinator.{h,cpp}` | 用户编译 + E2E；**未编译未验证** |
| **S1-c-B：客户端「有没有在匹配」判据收紧（4002 自撞的结构性起点）** —— 两处判据（`MyDsRecoveryCoordinator` 的 post-travel 分支、`MyAccountModel` 的登录冷启动分支）改为**只看 stage**，不再因 `match_id` 缺失就当"没有匹配"。权威 ResumeContext 在 STARTING/QUEUED 阶段结构性不下发 `match_id`，旧判据把 `{MatchId=0, Stage=Queued}` 读成"无匹配"→`ResetMatchState()`→Start 按钮重新可点→再发 StartMatch→撞自己活 claim→4002 | **已落码待用户编译验证** | `MyDsRecoveryCoordinator.cpp`、`MyAccountModel.cpp` | 同上 |
| **S1-c-C：push 在飞抑制（按审查修正后的形态落地）** —— 靶子：玩家点匹配后 StartMatch 在飞期间 `CurrentMatchId` 仍为 0，此时该局 push 到达会命中 `ShouldRequeryAuthorityForMatchPush(0 != EventMatchId)` → `BeginRequest` + `RestartAuthoritativeRecovery` **连换两代** → 在飞的 StartMatch 令牌作废 → 服务端**已受理**的这一局被客户端当"迟到回调"丢弃、本地不留归属 → 再发即 4002 自撞。修法：新增纯静态判定 `ShouldDeferMatchPushForInFlightStartMatch()`，**键在 coordinator 的 generation+request-seq 身份**上而非裸 `bStartMatchInFlight`。**有界性双层保障（已逐条核实）**：① 任何来源的换代都会让 `Pending*` 与当前 `Operation` 不再相等，抑制立即解除；② StartMatch 经 `SendUnary` 设 `[5s,300s]` 传输超时（`ResolveUnaryTotalTimeoutSeconds` clamp），完成委托由 `MakeUnaryCompleteOnceDelegate` 保证恰好转发一次，而清单飞标志的 token 比对发生在 `IsCallbackCurrent` **之前** ⇒ 即使回调被判迟到，标志也必被清。故不存在"谁也不来"的悬挂态 | **已落码待用户编译验证** | `MyMatchModel.{h,cpp}` | 同上 |

**S1 关键更正（2026-07-24，逐行核实 HEAD 后）**：N1 的修复面比方案与审查设想的都小得多。`BeginRequest:178-206` **已经存在** post-travel 保留机制（原注释：「Travel 后的权威复核换代时仍需保留该次连接随机数，否则 Admission RPC 无法验身」）。`NotifyHubAdmissionCommitted` 的三项接收校验中：① `TargetRoute==Hub` 经 `BeginRequest` 入参保留（`RequestAuthorityRequeryFromAccount` 传的就是 `Operation.TargetRoute`，实参在调用前求值）；② `AdmissionAttemptId` 已由既有分支保留；③ **只有 `Operation.Address` 漏了** —— `InvalidateOperation` `.Reset()` 后无人补回，而 `DoesConnectedEndpointMatchTarget` 对空 `TargetAddress` **恒返回 false**。
⇒ 工作流规格提出的「新建整套身份保留集 + 新增 `HubAdmissionAckDeadlineSeconds` 字段」与对抗审查提出的「post-travel 重查不换代、只 bump RequestSeq」**两个方案都属过度设计**：前者重复造已有机制，后者要改「换代」语义（需 AGENTS.md §7 decision-revisit）。最标准的做法是沿既有模式补齐这一个字段（§15 复用既有机制 / 最少复杂度）。两条方案均已评估并记录否决理由，未采纳。
| ⚠️ **FIX-5 初版实现有缺陷，已在工作区修正但 HEAD 仍是缺陷版** —— 初版用 `TouchActive` 把退避写进 active ZSET score，三处踩坑（均经逐行核实）：① score 的权威语义是 `last_heartbeat_ms`（`data/battle.go:106` 接口注释），挪作调度时间戳会让「score ≤ 阈值 ⇔ 心跳超时」判据失效；② `score==0` 是 abort fence（`data/battle_abort.go:289`）与 auth quarantine（`data/battle_auth.go:1150`，注释原文「score=0 令下一次 sweep 立即对账/回收」）的哨兵，`data/battle_active_reconciler.go:143` 专门用 `ZADD NX` 保护它，改写等于把高危态的立即对账降级为延迟对账（而初版退避的状态里正含 `stateAllocationAbort`）；③ `TouchActive` 是无条件 `ZADD`（`data/battle.go:935`，无 NX/XX/GT），迟到写会把已 `RemoveActive` 的终态项复活回补偿 outbox。**修正为进程内退避表**（零存储写、重启即清空，同时保住 `RunHeartbeatSweep` 的「重启即扫」意图）。缺陷版已被并发会话提交进 HEAD，**修正尚未提交** | **修正待提交** | `internal/biz/allocator.go`：`sweepDeferUntil` 字段 + `sweepDeferral`/`noteSweepDeferral`/`sweepDeferralActive`/`pruneSweepDeferrals` | 新增 `TestSweepMustNotRewriteActiveScoreSentinel`（5 子例）作为该缺陷的回归守卫 |
| **FIX-5：sweep 队头公平性 + 单轮墙钟预算** —— 5 种「永久墓碑 / 靠重试收敛」状态在进入分支工作**之前**先 `TouchActive` 让出队头（顺序关键：ZAdd→工作→可能 ZRem，避免对账成功后被回插）；单轮预算取既有 `sweep_interval`，`processed>0` 保证每轮至少推进一项，超预算 break 并打 `allocation_sweep_round_budget_exhausted` | **已落码待部署验证** | `services/battle/ds_allocator/internal/biz/allocator.go`：`sweepOnce` + 新增 `stuckReconcileState` / `sweepRoundBudget` | V5-1 绿（5 个子例修复前全部实测失败）；`-race` **阻断** |
| **FIX-6：容量告警去噪 + 5001 语义收窄** —— `FleetCapacity` 增 `Desired`/`DesiredKnown`/`Canary`（`spec.replicas` 用指针解码，区分「缺失」与「显式 0」）；仅 canary 轨 `DesiredKnown && Desired==0` 短路为 OK，**stable 缩到 0 照常 exhausted 告警**；新增 `pandora_ds_allocator_fleet_desired_replicas` gauge（未解码到就不上报该序列）；Grafana 用 `unless` 而非 `and`（gauge 缺失时退化为继续告警）；fleet 未配置由 5001 改 5002 | **已落码待部署验证** | `internal/data/agones_allocator.go`、`internal/biz/capacity.go`、`deploy/grafana/provisioning/alerting/rules.yaml` | V6 五例绿（2 例修复前实测失败）；V6-5/6-6/6-7 人工未跑 |
| **FIX-7：运维纪律文档** —— 联调迭代 DS 镜像必须走 canary 轨、stable `ready` 不得为 0、禁止高频 delete 代替滚动更新 | **已落码** | `docs/design/agones-dev.md` §2.3「关键点与不变量」新增小节 | 文档 |

### 7.3 防复发规则

- 关联 §9.19 / §9.20 / §9.22 / §9.23（no-freeze、单一幂等进场、状态查唯一权威）。
- 具体新增/修订规范链接待根因确认后补。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| V1-1 `TestLivenessGate_DisabledByDefault_NoOfflineJudgement`（既有） | 绿 | **绿** | `go test ./internal/biz/` | 全量套件通过 |
| V1-3 DS 池 ready=0 持续 ≥90s，日志不再出现 `match_liveness_failed` / `liveness_sweep_reaped_ticket` | 出现 | **未跑** | 故障注入（须按 §9.21 只降 replicas，禁止强杀 Allocated DS） | — |
| V1-4 全量配置面 grep + 集群 ConfigMap 实测生效 | — | **未跑**（仅完成仓库内 grep：纳管 3 份已改，`run/**` 8 份待重生） | 人工 | 本档 §7.2 |
| V2-1 `TestConfirmMatchRejectDuringUncheckpointedAllocatingCancels`（PENDING/REQUESTING 两子例） | **实测失败**（`errcode=12 ... cannot reject`，临时短路守卫复现） | **绿** | `go test ./internal/biz/ -run TestConfirmMatchReject` | `internal/biz/allocation_cancel_test.go` |
| V2-2 `TestConfirmMatchRejectAfterCheckpointStillRejected`（守既有边界不被误伤） | 绿 | **绿** | 同上 | 同上 |
| V2-2b `TestConfirmMatchRejectDuringAbortingOrUnknownPhaseRejected`（ABORTING/未知阶段 fail-closed） | 绿 | **绿** | 同上 | 同上 |
| V2-3 `TestConfirmMatchCancelAndCheckpointAreMutuallyExclusive`（取消 vs checkpoint 只能一个赢、无撕裂态） | 绿 | **绿** | 同上 | 同上 |
| `go build ./... && go vet ./...`（matchmaker） | 绿 | **绿** | — | — |
| matchmaker 全量单测 | 绿 | **绿** | `go test ./... -count=1` | biz/conf/data/service/cmd 全 ok |
| `go test -race` | 未跑 | **阻断**：本机 Windows 无 gcc，`-race` 需 CGO（`cgo: C compiler "gcc" not found`）。须在支持 CGO 的 Linux/CI 执行，**不得记为已验证**（§16.7） | — | — |
| V5-1 `TestSweepDefersStuckReconcileStatesOffQueueHead`（5 种墓碑状态各一子例） | **实测失败**（5/5 子例，score 恒为 0 不让队头） | **绿** | `go test ./internal/biz/ -run TestSweep` | `ds_allocator/internal/biz/sweep_fairness_test.go` |
| V5-1b `TestSweepDoesNotDeferAbandonedCompensation`（§9.4 补偿最后一棒不被退避） | 绿 | **绿** | 同上 | 同上 |
| V5-2 `TestSweepRoundBudgetFromExistingInterval`（预算复用既有配置项，不新增） | 绿 | **绿** | 同上 | 同上 |
| V6-1 `TestLevelForCanaryDesiredZeroIsQuiet` | **实测失败** | **绿** | `go test ./internal/biz/ -run TestLevelFor` | `capacity_desired_test.go` |
| V6-2 `TestLevelForDesiredUnknownStillExhausted`（解码不到 spec 时保守告警） | 绿 | **绿** | 同上 | 同上 |
| V6-3 `TestLevelForStableDesiredZeroStillExhausted`（stable 缩零不被静音） | 绿 | **绿** | 同上 | 同上 |
| V6-3b `TestLevelForCanaryWithDesiredStillReportsExhausted`（canary 真跑金丝雀时打满照常告警） | 绿 | **绿** | 同上 | 同上 |
| V6-5 `TestObserveCanaryDesiredZeroEmitsNoEvent`（连续多轮静默） | **实测失败** | **绿** | 同上 | 同上 |
| ds_allocator 全量单测 + build + vet | 绿 | **绿** | `go test ./... -count=1` | 全 ok |
| V6-6/V6-7 人工：canary 常态 0 副本 ≥10min 无 Error；用"压满"而非 scale 造 stable ready=0 确认 critical 正常触发；desired gauge 缺失时仍能 firing | 未跑 | **未跑** | 真集群 + Grafana | — |
| V5-5 人工：反向代理让 K8s API 持续超时 ≥5min，观测 active ZSET 长度与 abandoned 补偿延迟分布 | 未跑 | **未跑** | 故障注入 | — |
| UE 客户端编译（S1-a/b/c-B/c-C 四项改动） | — | **已通过**（用户本人执行，2026-07-24；§11.6 UE 编译归用户） | 用户本地 | 用户口头确认 |
| 集成回归（战斗中退出→重登→匹配 E2E） | 未建 | **未跑** | — | — |
| V2-5 E2E：DS 缺容 ≥60s，玩家点取消 → 立刻回到可点匹配 | 未跑（修复前不可取消） | **未跑** | 真集群人工 | — |
| fatal/OOM/SIGKILL / battle pod 删除注入 | 未跑 | **未跑** | — | — |
| 玩家 E2E（本次为手工触发，未脚本化） | 手工复现一次 | 重启后解卡（属临时止血，非修复验证） | 本机 k8s | 客户端日志 |

未执行项保留，不删除。**当前所有"绿"仅为本机单测/静态检查，未在真集群加载新产物验证，不构成事故关闭依据（§16.9）。**

## 9. 部署、回滚与观察

- 修复 commit：无（尚未修）。
- 构建产物/镜像 digest：matchmaker 当前线上版本 digest 待补；battle DS baseline b4375d1 → 重建 efc6043（掉落排障记录）。
- 部署时间与目标环境：本机 k8s（192.168.2.28 / ns pandora）。
- 实际 Pod imageID / GameServer provenance：待补。
- 回滚条件和步骤：不适用（无修复变更）。
- 观察窗口、指标与结果：用户已解卡；`pandora:match:*` 空；持续观察是否复发。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A1 | P0 | 挖 matchmaker/ds_allocator 服务端日志定位根因 | 本人 | **已完成**（根因确认，见 §5.1） | 本 INC |
| A2 | P0 | 客户端 `UMyDsRecoveryCoordinator` 收敛（§9.19/§9.23）：S1-a 保留连接地址（N1 根因）、S1-b post-travel 阶段 deadline、S1-c-B 判据收紧、S1-c-C push 在飞抑制 —— **四项全部落码**（见 §7.2） | 本人 | **代码完成；用户已完成 UE 编译（2026-07-24）；行为验证（E2E/故障注入）未做** | 本 INC |
| A3 | P0 | matchmaker 成局最终门 liveness **已确认结构性 100% 假阳性**（非"偶发误判"，见 §5.1 更正二）。~~需区分"真离线"与"travel 短暂不可见"（宽限窗）~~ —— **该方向已否决**：缺席在本场景是时间的确定函数，重复采样不产生新信息，宽限窗只把误杀时刻从 T+30s 推到 T+30s+N，等于把门关掉却假装它开着（§14 空壳）。**已按 FIX-1 关闭两道门**（配置回落到代码默认值） | 本人 | **已落码待部署验证** | 本 INC |
| A3b | P0 | N2：ALLOCATING 期玩家无出口（见 §5.1）。**已按 FIX-2 落码** pre-checkpoint 取消 | 本人 | **已落码待部署验证** | 本 INC |
| A9 | P1 | **剩余风险：post-checkpoint 的 ALLOCATING 停滞仍无界**（已签票 / `notifyBattleStrict` / READY CAS / push 持续失败这一段，FIX-2 不覆盖）。**不能靠写代码关闭** —— 加自动终态会推翻 `expireOnce` 写死的既有决策（「本地时间绝不能把未知推断成失败并重排」，与 §9.22 同源），按 AGENTS.md §7 须先拍板。已产出决策文档 [`decision-revisit-allocating-bounded-terminal.md`](../design/decision-revisit-allocating-bounded-terminal.md)（列 4 个候选方案、3 条已识别硬伤、3 个待答问题；倾向"超时走既有 abort fence 拿确定性 ACK 再终止"+"可见性告警"）。不得因 FIX-2 落地就宣称 §9.20 已全量满足 | 待指定 | **待拍板**（文档已出） | 本 INC |
| A10 | P1 | **关门的已知代价（诚实登记，不得包装成无损）**：① 放弃"别给残局白拉 DS"的意图，每个残局多浪费一次分配（兜底：PVP `confirm_timeout=15s` 淘汰不响应者；单人 PVE 无第三方受害者；残局由 ds_allocator 15s 心跳 abandoned 回收，§9.4）；② 掉线者的死票会留在队列被凑局，其余成员多吃一轮 FAILED（票据退回队列、保留排队时长） | 待指定 | 已登记 | 本 INC |
| A11 | P1 | **`-race` 在本机不可执行**（Windows 无 gcc / CGO），V2-3 并发用例须在支持 CGO 的 Linux/CI 补跑；未跑前不得声称并发正确性已验证（§16.7） | 待指定 | 阻断 | 本 INC |
| A12 | P2 | 滚动升级窗口：`AllocationPhase==UNSPECIFIED` 的历史记录（若存在）在 FIX-2 下不可取消（fail-closed 保守拒绝，等于维持修复前行为）。当前两个写入点均设 PENDING，预期不产生此类记录，排空后自然消失 | 待指定 | 已登记 | 本 INC |
| A4 | P1 | ds_allocator 对 k8s API/Agones 控制面超时的韧性：`allocation_uncertain_exact_release` 在 API 持续 `context deadline exceeded` 下卡 ~40s 的行为是否合理，是否需更快 fail + 明确回传 matchmaker 让其带退避重排 | 待指定 | 待评估 | 本 INC |
| A5 | P1 | 结构性隐患：全仓扫描 `ReleaseMatch`/`CancelMatch` 调用点与失败分支，确认无退局路径漏调导致非终态 claim 永久残留（本次未触发但会造成无限期 4002） | 待指定 | 待做 | 本 INC |
| A6 | P1 | 运维/容量纪律：测试/联调期间不得把 battle-stable fleet churn 到 ready=0，删/重建 DS 前确保有最小可分配副本，避免同时加压 k8s 控制面（本次主因） | 本人 | 待落规范 | 本 INC |
| A7 | P2 | 僵尸 READY 单人队随客户端停轮询自然 GC；评估战斗生命周期是否应回写/复位 team 状态（非阻塞，仅一致性） | 待指定 | 待评估 | 本 INC |
| A8 | P2 | A5 结构性残余：owning-mode matchmaker 全宕时该 mode 非终态 op 无人驱动（R1）、preflight 不清非终态 claim（R2）——评估是否需跨 mode 兜底 reconcile 或非终态 claim 的独立老化上限。属可用性依赖，非本次触发 | 待指定 | 待评估 | 本 INC |

## 11. 关闭审核

- [ ] 直接根因和放大因素均有证据
- [ ] 修复前失败、修复后通过的回归存在
- [ ] race/集成/故障注入达到本事故风险要求
- [ ] 同类代码扫描完成
- [ ] 目标环境已加载可追溯的新产物
- [ ] 玩家路径、恢复和补偿路径验证通过
- [ ] 观察窗口无复发
- [ ] 剩余风险已解决或另建 Incident/任务
- [ ] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭。
