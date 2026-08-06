# hub_allocator

> 大厅 DS 分片调度:login 登录成功后 `AssignHub` 给玩家分一个 Hub DS 分片并签 hub DSTicket;
> Hub DS 每 5s `Heartbeat` 续命,心跳超时由后台扫描标记 draining 停止分配(不变量 §4);
> 玩家可 `TransferToLine` 主动切线,系统可按负载自动扩缩容 / 强制整合搬迁玩家。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见
> `docs/design`:大厅归属跨 slot 见 [`decision-revisit-hub-crossslot.md`](../../../docs/design/decision-revisit-hub-crossslot.md);
> DS 回调认证与 Model B「Redis 唯一授权权威」见 [`decision-revisit-ds-callback-auth.md`](../../../docs/design/decision-revisit-ds-callback-auth.md);
> 每玩家 owner 权威见 [`owner-authority.md`](../../../docs/design/owner-authority.md);跨服务要约见
> [`go-services.md §2.12`](../../../docs/design/go-services.md)。
>
> 代码锚点以**函数名**为准,行号截至当前 HEAD(会随改动漂移)。

## 职责与边界

- **职责**:大厅(Hub)DS 分片调度。`AssignHub` 选分片 + 签票、`ReleaseHub` 退座、`TransferHub` /
  `TransferToLine` 跨分片传送、`ListHubs` / `ListHubLines` 查负载、`Heartbeat` 收 Hub DS 上报、
  `AcknowledgeAdmission` / `AcknowledgeDeparture` 收 Hub DS 入场 / 离场确认,后台 `RunHeartbeatSweep`
  做心跳超时扫描 + 拓扑对账 + Fleet 扩缩容 + 强制整合。
- **权威态**:分片镜像、玩家→分片归属、逐 reservation / connected ownership 容量账本全在 **Redis**
  (进程内只做缓存 + 后台单写者扫描循环)。
- **状态权属**(不变量 §22):hub_allocator 是玩家 **Hub 归属** 与 **分片 ready/draining** 的调度权威;
  player_locator 的 `HUB` presence 只作在线 / 最近活跃投影,不是归属权威。玩家「战斗 / 匹配中」由
  player_locator 权威,切线护栏查询它并 fail-closed。
- **票据签发权威**:hub DSTicket 由本服务单一签发(HS256 legacy 或 RS256 DSTicket v2 实例绑定);
  role_id / source_match_id / session_jti 等 claim 都在这里盖进票据。
- **不做的事**:不做 cell 路由(region/cell 恒 0);不算 MMR / 经验 / 掉落;不持久化玩家档案。
- **一人一 Hub**(不变量 §1):`AssignHub` 幂等——已分配且分片可用则重签票不重复占座;真正堵死
  「一人两 Hub」的机械屏障是 `HeartbeatTimeout ≥ DS 授权租约上限 + 偏差余量`(见「配置项」)。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20021` | 内部 RPC(login / Hub DS 回调)+ 玩家 RPC(经 Envoy) |
| HTTP | `:21021` | 仅 `/metrics` |

端口取自 `internal/conf/conf.go` 的 `Defaults()`(`Server.Grpc.Addr` / `Server.Http.Addr`)。

## 对外接口

代码入口:`internal/service/hub.go`(gRPC service 层,实现 `hubv1.HubAllocatorServiceServer`)。
多数 RPC 是后端内部 / DS 回调,**不从 ctx 取 player_id**——`player_id` 由 login 等上游在请求里显式传入
(见 `service/hub.go` 顶部包注释);只有 `ListHubLines` / `TransferToLine` 是玩家侧,`player_id` 一律取自
JWT sub(`pmw.PlayerIDFromContext`),不信请求体。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `AssignHub(player_id, region, team_id, role_id, source_match_id, session_jti)` | login(内部) | 选分片 + 签 hub DSTicket;幂等重签 | 内部直连,`player_id` 请求体传入 |
| `ReleaseHub(player_id)` | login / 登出(内部) | 玩家离开大厅,退座 + 删归属,幂等 | 内部直连 |
| `TransferHub(player_id, target_hub_id)` | 内部 | 跨分片传送(占新→切归属→退旧→重签) | 内部直连 |
| `ListHubs(region)` | 运维 / 调试(内部) | 列分片负载 | 内部直连 |
| `Heartbeat(hub_pod_name, player_count, player_ids, state, ts_ms, ...)` | Hub DS(每 5s) | 续命 + 刷在线数;回下发 drain/stop 指令 + 驱逐单 | **DS 回调令牌**(`DSCallbackGuard`,sub==pod);Model B 下须完整凭据 |
| `AcknowledgeAdmission(player_id, assignment_id, hub_pod_name, admission_id, admission_seq, session_jti)` | Hub DS | 玩家真正接纳后,把 reservation 原子消费为 connected ownership | **DS 回调令牌**;Model B 专用(`cred==nil` 拒) |
| `AcknowledgeDeparture(player_id, assignment_id, hub_pod_name, admission_id, admission_seq)` | Hub DS | 同一已接纳连接 Logout 时,exact 删除 owner | **DS 回调令牌**;Model B 专用 |
| `EnsureHubDepartureForBattle(...)` | (已废弃) | placement 路由体系已硬切删除,一律返回 `ErrServiceDisabled` | — |
| `ListHubLines(region)` | 客户端 | 列当前 region 可切换线路(隐藏 pod 名 / 内部地址) | **JWT sub**(`player_id` 取自 ctx) |
| `TransferToLine(target_shard_id)` | 客户端 | 主动切线(换实例,带冷却 / 战斗匹配中禁切护栏) | **JWT sub** |

> **内部专用 RPC 拒绝玩家 JWT**:`ListHubLines` / `TransferToLine` 之外的方法只走后端东西向 / DS 回调,
> 不经 Envoy `:8443` 客户端面路由。玩家侧两方法在 `server/grpc.go` 额外挂 `pmw.SessionCurrent`——
> 请求 jti 必须是 login 会话权威当前一代(R5 复审 P0-1,顶号后旧 JWT 立即失效);内部 / DS RPC
> 不带 `x-pandora-jwt-payload` 天然放行。DS 回调令牌(hub 令牌 sub 必须等于上报 `hub_pod_name`)防拿
> A 分片令牌冒充 B 分片心跳 / 伪造在场玩家列表。

## 目录结构(Kratos 标准分层,对齐 login / matchmaker)

```
cmd/
  hub_allocator/main.go        启动入口(Redis + signer + fleet provider + 各弱依赖装配 + 后台 sweep)
  hub_auth_quarantine/main.go  Hub 凭据紧急吊销运维 CLI(独立小二进制;默认只审计,-apply 才 tuple CAS)
etc/
  hub_allocator-dev.yaml        mode=local dev 配置(本机常驻 Hub DS)
  hub_allocator-prod.yaml.example  生产样例
internal/
  conf/conf.go                 配置结构(HubConf + JWT + DSTicket v2 + Agones + LocalHub + DSAuth)+ Defaults()
  service/
    hub.go                     RPC 入口(HubAllocatorServiceServer;内部/DS/玩家三类鉴权分流)
  biz/
    hub.go                     HubUsecase 核心(Assign/Release/Transfer/Heartbeat/Admission/sweep)
    fleet.go                   HubFleetProvider/Scaler 抽象 + MockHubFleetProvider(确定性假分片)
    agones_fleet.go            AgonesHubFleetProvider(查 k8s GameServer 列表发现分片 + 令牌/凭据签发投递)
    local_fleet.go             LocalHubFleetProvider(本机 exec 一个常驻 Windows Hub DS 进程)
    owner_authority.go         owner 权威迁移调用面(migrate ①③④,census 代提交 Admit,弱依赖)
    owner_lease.go             owner 实例租约双写门(migrate ⑥,弱/强依赖语义)
  data/
    hub_repo.go                Redis 分片镜像 / 玩家归属 / active 心跳 ZSET / 切线冷却 / cleanup 索引仓储
    hub_authoritative.go       Model B 原子线性化写(ActivateHeartbeat / ReserveRoutableSeat / CheckRoutable)
    hub_capacity_ledger.go     逐 reservation / connected ownership / successor 容量权威(AcknowledgeAdmission 等)
    hub_auth_repo.go           HubAuthRepo 授权记录仓装配(pandora:hub:auth:{pod})
    writer_fence.go            存储级单调 fencing token(写者继任,同事务拦迟到旧写)
    locator_client.go          player_locator gRPC client(切线护栏:战斗/匹配中禁切)
    owner_lease_client.go      owner 权威服务 gRPC client(实例租约续租)
  server/
    grpc.go / http.go          gRPC(挂 AuthOptional + SessionCurrent)/ HTTP(/metrics)注册
```

## 分片状态机

```
(播种)──► warming ──►(首个鉴权心跳)──► ready ──►(心跳超时/整合)──► draining ──► stopping
             │                            │            (下发 drain 倒计时)   (下发 stop)
      不可被 AssignHub 选中          可被 AssignHub 选中
```

- 状态常量:`internal/biz/hub.go` 顶部(`stateWarming / stateReady / stateDraining / stateStopping`)。
- `warming`:`mode=agones` 下分片先播种为 warming,**首个通过 `DSCallbackGuard` 的心跳**才翻 `ready`
  ——PATCH / 进程拉起成功 ≠ DS 已真正鉴权回调,不能把玩家路由到从未心跳的 Hub(`SetRequireHeartbeatReady`)。
  `mock` / `local` 不置,直接播种 `ready` 保持 dev 联调不变。
- `draining` / `stopping`:心跳超时(`sweepOnce`)或强制整合把分片标 draining,`Heartbeat` 响应下发
  `drain` + `grace_seconds` 倒计时引导在场玩家切大厅;孤儿 pod(无镜像)下发 `stop`。

## 核心调用链

### 1. AssignHub —— 幂等选分片 + 签票

看 `internal/biz/hub.go` 的 `AssignHub`。它在一个乐观锁重试循环(最多 8 次)里:

```
AssignHub(player_id, region, role_id, source_match_id, session_jti)
├─ requireWriter                         非写者副本快速拒(ErrUnavailable,重试路由到写者)
├─ GetAssignment                         已有归属?
│   ├─ 有 cleanup pending → resumeAssignmentCleanup  先把上次未完的 owner-cleanup saga 跑完
│   └─ 可复用(assignmentRoutable/同实例)→ ensureExistingAssignmentSeat + CAS 刷 TTL → signResult(重签票)
├─ ensureShards(region, releaseTrack)    确保该 region+轨有 ready 分片(Fleet 发现 + 播种镜像)
├─ selectAndReserveShard                 选最空分片 + 原子占座(reservation),canary 无容量回退 stable
├─ signResult                            先签 hub DSTicket(失败 → compensateReservedSeat 精确退座)
├─ registerTransferCleanup(Model B)      换分片先登记旧 owner 的 index-first 精确清理
├─ CompareAndSwapAssignment              CAS 落新归属(带完整 v2 绑定);loser → 退座 + 重试
└─ resumeAssignmentCleanup / releaseAssignmentSeat  驱逐旧物理席位,返回 signedResult
```

- **幂等**:已分配且分片可用 → 重签票返回,不重复占座 / 不换分片(不变量 §1)。`role_id>0` 覆盖归属
  镜像角色(login 是角色权威);`source_match_id` 是 Battle→Hub 一次性回流 fence,只盖进票不进镜像。
- **签票在 CAS 前**:签名器失败可用 reservation identity 精确补偿,既不暴露拿不到票的归属也不泄漏容量。
- **写者复核**:出票前 `confirmWriterForTicket` 再确认仍持写者租约;入口后失主的在途请求宁可 `ErrUnavailable`
  引导重试重签,也绝不把票交给调用方(`writer_fence.go` 覆盖边界 ④)。

### 2. reservation → Admission —— 占座与真正接纳分离

`AssignHub` 只在容量账本占一个 **reservation**(带绝对 lease `ReservationTTL`),玩家拿票 `ClientTravel`
到 Hub DS;Hub DS `InitNewPlayer` 真正接纳后才回调 `AcknowledgeAdmission`,把 reservation 原子消费为
**connected ownership**(无时间 TTL 的连接态):

```
AcknowledgeAdmission(player_id, assignment_id, pod, admission_id, admission_seq, session_jti, cred)
├─ requireWriter + 必须 Model B(authRepo!=nil 且 cred!=nil)
├─ sessGate 预检:票据 sjti 仍是当前会话一代(空 sjti 由 require_ticket_sjti 门控)
├─ GetAssignment + assignmentMatchesAdmission  归属仍精确匹配 (player/assignment/pod/uid/epoch/writer_epoch)
├─ authRepo.AcknowledgeAdmission               durable ledger:reservation→session 线性化点
├─ sessGate 后检(TOCTOU 收口):写成功后再核会话现行性
│     ├─ 权威不可达 = 未知 → 保留 owner,返回 Unavailable(DS 同 identity 重试,幂等)
│     └─ 确定性否定(会话消失/被顶)→ AcknowledgeDeparture 精确回退 owner + 拒绝开 spawn gate
└─ 复查归属(跨 {pod} slot)仍匹配 → Admitted=true
```

- 容量权威见 `internal/data/hub_capacity_ledger.go`:`reservations:{pod}` / `sessions:{pod}` /
  `successors:{pod}` 三态账本 + 各自 expiry ZSET,`AcknowledgeAdmission` 把 reservation 迁到 session。
- `AcknowledgeDeparture` 只删 exact `(player, assignment, admission_id, seq)` owner;旧连接晚到的 Logout
  撞上已被新 `admission_id` 替换的所有权时返回 `departed=false`(Conflict 视作 OK),让旧连接停重试且
  不误删新接管连接。

### 3. Heartbeat —— Model B 单事务线性化点

`Heartbeat`(service 层,`service/hub.go`)先经 `DSCallbackGuard.CheckHubCredential` 验签并抽出凭据,再按
Model B 与否分流到 biz:

```
Heartbeat(hub_pod_name, player_count, player_ids, state, ...)
├─ CheckHubCredential(sub==pod, RequireToken)   验签;Model B 令牌→cred!=nil,legacy→cred=nil
├─ cred!=nil → HeartbeatWithCredential → heartbeatModelB
│     └─ authRepo.ActivateHeartbeat   单事务:凭据 promote(pending→active)+ warming→ready +
│                                     投影 active 元组 + 刷分片镜像;stale 一律 fail-closed(ErrUnauthorized)
│        ├─ renewOwnerLeaseGate       owner 实例租约双写(弱/强依赖)
│        ├─ ownerAdmitCensusWeak      按在场 player_ids 代提交 owner Admit(migrate ③,弱依赖)
│        └─ pendingHubEvictionOrders  发现该实例应清退的旧物理连接,回 EvictionOrders 给 DS
├─ modelBAuthority 且仅 legacy 令牌 → ErrUnauthorized(不给旧令牌借心跳保活/翻 ready)
└─ 否则 legacy 路径 heartbeat → HeartbeatShard(代际门可选)
     └─ 分片 draining/stopping → 回 drain/stop 指令 + grace_seconds
后置:RefreshHubPresence  把在场 player_ids 异步转发 locator 续 HUB presence TTL(弱依赖,独立超时)
```

- `authRepo` 与分片镜像共享 `{pod}` hashtag → **同 Redis slot**,故授权 promote 与分片状态翻转能压进
  一个 WATCH/MULTI/EXEC,消灭「半激活」/ TOCTOU 误分配窗口(见 `hub_authoritative.go` 顶注)。
- 服务端统一用**接收时刻**为心跳时间,请求 `ts_ms` 不参与授权 / 存活权威(future ts 不能延长可路由窗口)。

### 4. 后台 RunHeartbeatSweep(单写者)

`RunHeartbeatSweep`(`hub.go`)每 `SweepInterval`(默认 5s)跑一轮,随进程生命周期启停:

```
RunHeartbeatSweep (每 5s)
├─ sweepStaleOwnerAdmitted       清 census 准入缓存死实例项(内存卫生,写者门控之前无条件执行)
├─ [writerFence] 非写者副本跳 tick;AdvanceWriterFences 仅作再断言(推扫已前移为接流前硬门)
├─ reconcileOwnerCleanups        崩溃恢复:补跑 index-first transfer/release cleanup saga
├─ reconcileShardTopology        Fleet 发现对账:播种/清理分片镜像,同步令牌代际
├─ sweepOnce                     ★ last_heartbeat_ms 超阈值的分片 → 标 draining + 移出 active
└─ reconcileFleetReplicas        autoscale/consolidation:按在线人数调 Fleet 副本 + 强制整合搬迁
```

- **单写者**:多副本经 etcd `writerlease` 选举(`hub_allocator/writer`),仅当选副本可写;存储级最终防线是
  `data/writer_fence.go` 的同事务单调 fencing token 比较,迟到旧写者零写入(R9 P0-7,
  [`session-generation-rollout.md`](../../../docs/design/session-generation-rollout.md) §5)。
  R10 复审 P0-4 起再收紧两处:①**接流前硬门**——当选后必须先跑成功
  `AdvanceWriterFencesForToken`(`writerlease.Config.OnElected`)才宣告持有领导权,消除
  "已接写、继任推扫未完成"窗口;②**每玩家持久水位**——归属记录携带 `writer_token`
  (`allocator.proto` 31),被继任的旧写者既不能覆盖也不能删除继任者写的归属。
- **档位与首次引导升级**:`hub.writer_lease_mode`(`enforce` 默认 / `warmup` 只竞选观测 /
  `off` 仅历史 Recreate 部署);从不含 writerlease 的旧镜像首次升级必须按 rollout §5.4 三跳执行。
- **长期无主观测**:HTTP 端口 `/healthz/writer` 输出 held/token/竞选与激活失败计数/`degraded`
  (**故意不接 readiness**,失主副本是热备;理由见 `internal/server/http.go`)。
- `sweepOnce` 只标从未心跳过的 mock 种子分片(score=0)之外的**真正超时**分片,避免误标(不变量 §4)。

### 5. TransferToLine —— 玩家主动切线

`TransferToLineForPlayer`(`hub.go`)在任何副作用前先过护栏,再复用内部 `TransferHub`:

1. **战斗 / 匹配中禁切**:查 `player_locator.InBattleOrMatching`,RPC 失败 / 非 OK / 未知状态一律
   **fail-closed 拒绝**(§9.22 UNKNOWN 不得授权新归属,INC-20260722-002)。`locator` 未配(dev)每次放行留痕。
2. **完整 v2 绑定复核**(Model B):当前 assignment 必须仍精确绑定 Redis active,legacy / 缺 JTI 旧记录零变更拒绝。
3. **冷却防刷**:`TryTransferCooldown`(SET NX EX,默认 10s),窗口内再切拒 `ErrHubTransferCooldown`;后续失败释放占坑。
4. 委托 `TransferHub` 完成 占新→切归属→退旧→重签票;`requireCallerSessionCurrent` 在临界点复核请求方会话仍现行。

## 关键设计点 / 不变量

| 主题 | 约束 | 代码锚点 |
|---|---|---|
| 一人一 Hub | `AssignHub` 幂等重签;换分片走 owner-cleanup saga 先建后驱逐,绝不双 owner | `AssignHub` / `resumeAssignmentCleanup` |
| 再入屏障 | `HeartbeatTimeout ≥ DSFenceReentryBarrier`(27s),机械抬到下限,防「一人两 Hub」 | `conf.Defaults` / `pkg/placement` |
| reservation/session 分离 | 占座是 reservation(有 lease),真正接纳才转 connected ownership(无 TTL) | `hub_capacity_ledger.go` |
| Model B 单事务 | 授权记录与分片镜像同 `{pod}` slot,promote + ready + 占座各压一个事务 | `hub_authoritative.go` |
| 心跳授权 | Model B 心跳须带完整凭据,legacy 令牌一律拒;stale fail-closed | `heartbeatModelB` |
| 单写者 | etcd 选举 + 存储级 fencing token,迟到旧写者零写入 | `RunHeartbeatSweep` / `writer_fence.go` |
| 切线 fail-closed | 战斗/匹配中禁切,locator 查询失败 / 未知即拒 | `TransferToLineForPlayer` |
| 票据会话绑定 | hub 票盖 sjti,AcknowledgeAdmission 前后双复核现行性(TOCTOU 收口) | `AcknowledgeAdmission` |
| 玩家/DS 密钥不相交 | jwt 玩家面与 ds_auth DS 回调面密钥必须互不相同,enforce 下启动即拒 | `cmd/.../main.go` |

## 配置项(`internal/conf/conf.go`,默认值取自 `Defaults()`)

| 键 | 默认 | 说明 |
|---|---|---|
| `mode` | 由 `agones.enabled` 推导 | 分片来源:`local`(本机常驻 DS)/ `agones`(k8s Fleet)/ `mock`(假分片) |
| `hub.heartbeat_timeout` | `30s` | 心跳超时阈值(不变量 §4);机械抬到 `DSFenceReentryBarrier`(27s)下限 |
| `hub.sweep_interval` | `5s` | 后台心跳超时扫描 / 对账间隔 |
| `hub.shard_ttl` | `30min` | 分片镜像 Redis key TTL(Assign/Heartbeat 刷新) |
| `hub.assignment_ttl` | `30min` | 玩家→分片归属 key TTL(Assign/Transfer 刷新) |
| `hub.reservation_ttl` | `DSTicketMaxTTL + 15s` | 占座到 Admission ACK 的绝对 lease;Model B 校验 ≥ 票有效窗、≤ assignment_ttl |
| `hub.default_region` | `global` | AssignHub 未指定 region 的兜底分区(dev 配置写 `cn`) |
| `hub.default_capacity` | `500` | 单分片人数上限(大厅 500 人/实例) |
| `hub.optimistic_retry` | `3` | WATCH/MULTI/EXEC 乐观锁重试次数 |
| `hub.transfer_cooldown` | `10s` | 玩家主动切线冷却;`≤0` 不限流 |
| `hub.autoscale_enabled` | `false` | Hub Fleet 自动扩缩容(须 `mode=agones` 真 scaler 才生效) |
| `hub.players_per_hub` | `500` | 自动扩容阈值:单 Hub 目标承载人数 |
| `hub.min_replicas` / `max_replicas` | `1` / `20` | 大厅副本保底 / 上限 |
| `hub.consolidation_enabled` | `false` | 强制整合(须 `autoscale_enabled` + `kafka.brokers`) |
| `hub.migrate_grace_seconds` | `30` | 迁移优雅倒计时(秒)+ 排空分片可缩容的最短等待 |
| `hub.consolidation_batch` | `50` | 单次 reconcile 每分片最多迁移人数 |
| `hub.owner_addr` | 空 | owner 权威服务地址(空=不双写实例租约,弱依赖) |
| `hub.owner_lease_required` | `false` | 实例租约双写失败是否令授权心跳失败(强依赖开关) |
| `hub.locator_addr` | 空 | player_locator 地址(切线护栏;空=跳过战斗/匹配中检查) |
| `hub.mock_shard_count` / `mock_hub_addr_host` / `mock_hub_port_base` | `3` / `127.0.0.1` / `7777` | Mock 分片拓扑(真 Fleet 接入后废弃) |
| `ds_auth.mode` | `off` | DS 回调令牌校验:`off` / `permissive` / `enforce`(生产 agones 必须 enforce) |
| `ds_auth.authority_mode` | `legacy` | `redis` = 启用 Model B 唯一授权权威(须 agones + enforce + etcd fence) |
| `jwt.ds_ticket_ttl` | — | hub DSTicket 短时效(不变量 §3,dev `5m`) |
| `ds_ticket.private_key_file` | 空 | 配了即启用 RS256 DSTicket v2 实例绑定签发;`mode=agones` 必配 |
| `session_gate.require` | `false` | 玩家面请求 jti 须为会话权威当前一代(生产 `-Prod` 置 true) |
| `session_gate.require_ticket_sjti` | `false` | 票据 sjti 绑定硬门(旧 DS 排空后收口置 true) |

**启动强校验**(`cmd/hub_allocator/main.go`):`mode=agones` 必须配 DSTicket v2;`authority_mode=redis` 必须
`mode=agones` + etcd fence + `reservation_ttl` 覆盖票 TTL;玩家面 `jwt.secret` 与 DS 回调面 `ds_auth.secret`
密钥集必须不相交(enforce 下交叉即拒启)。

## 本地启动

```powershell
# 1. 基础设施(Redis 强依赖;起 player_locator :20006 / owner :20017 后可跑全链,留空走弱依赖降级)
pwsh tools/scripts/dev_up.ps1

# 2. 启 hub_allocator(mode=local dev 配置,首次 AssignHub 懒拉起一个常驻 Windows Hub DS)
go run ./services/battle/hub_allocator/cmd/hub_allocator -conf services/battle/hub_allocator/etc/hub_allocator-dev.yaml
```

> dev 配置 `mode=local` 会 exec 一个常驻 UE Windows DS(`local_hub.executable_path` 指向打包好的
> `PandoraServer.exe`);无 UE 产物时改 `mode: mock` 用确定性假分片纯跑后端逻辑。`ds_auth.mode=off` +
> `authority_mode=legacy` 是本机机械隔离档,生产走 `mode=agones` + `enforce` + `authority_mode=redis`。

## 关联文档

- [`go-services.md §2.12`](../../../docs/design/go-services.md) — hub_allocator 要约(RPC / 职责 / 拓扑)
- [`infra.md`](../../../docs/design/infra.md) — 服务端口(`:20021` / `:21021`)/ leader 选举 key / kafka topic
- [`decision-revisit-ds-callback-auth.md`](../../../docs/design/decision-revisit-ds-callback-auth.md) — DS 回调认证 + Model B「Redis 唯一授权权威」§7
- [`decision-revisit-hub-crossslot.md`](../../../docs/design/decision-revisit-hub-crossslot.md) — 大厅归属跨 slot 与 `{pod}` hashtag 布局
- [`owner-authority.md`](../../../docs/design/owner-authority.md) — 每玩家 owner_epoch / 实例租约 fencing / census Admit(migrate ①③④⑥)
- [`session-generation-rollout.md`](../../../docs/design/session-generation-rollout.md) §5 — 写者继任租约 + 会话代次门(R9 P0-7)
- [`agones-dev.md`](../../../docs/design/agones-dev.md) — Agones Fleet 发现 / DS 令牌 annotation 投递 / JTI cache 有界纪律
- [`zero-downtime-update.md`](../../../docs/design/zero-downtime-update.md) — 金丝雀发布 / 滚动更新 / Hub DS 排空退役(不变量 §16/§21)
- [`battle-reconnect.md`](../../../docs/design/battle-reconnect.md) — DS 过渡 no-freeze 契约与 owner 授权租约(不变量 §19/§22)
