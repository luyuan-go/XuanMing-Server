# player_locator

> 玩家位置索引服务:维护 `player_id → Location`(OFFLINE / LOGIN_PENDING / HUB / MATCHING / BATTLE),
> 是「玩家此刻在哪」的唯一查询入口,并用状态机守卫强制不变量 §1「玩家同一时刻只能在一个 Location」。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见
> `docs/design`:跨服务要约与 presence 投影语义见 [`go-services.md §2.6`](../../../docs/design/go-services.md);
> BATTLE fence 加固见 [`battle-reconnect.md §5`](../../../docs/design/battle-reconnect.md);
> presence 扇出见 [`friend-distributed-scaling.md §13`](../../../docs/design/friend-distributed-scaling.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:玩家位置(Location)的唯一权威索引 —— 覆盖式写(SetLocation)/ 查询(Get / BatchGet)/
  在线保活(RefreshHubLocations)/ 快速断线(ReportDisconnect)/ 清理(ClearLocation);好友在线态订阅扇出。
- **权威边界**(不变量 §22):player_locator 只是**短期 presence / 最近活跃投影**(TTL 位置租约),
  **不是玩家归属权威**。key miss 只说明「presence 不可见」,**不能**单独证明玩家已离开旧 DS,也不能授权进入另一台 DS。
  当前哪台 DS 有权控制玩家由 owner authority(§9.22)裁决;`MATCHING` 也只是投影,durable match stage 在 matchmaker。
- **单一真源**:位置投影全在 **Redis**(hash by `player_id`),无 MySQL、无进程内长期状态 —— 服务无状态、可水平扩展、可随时被杀被替换(不变量 §16)。带 fence 的 HUB 上线只按连接三元组在同 assignment 内定序；跨 assignment 归属由 hub_allocator 权威决定，locator 不反向定序、也不查 owner。
- **状态权属**:`MATCHING` / `BATTLE` 由 matchmaker / ds_allocator 控制面写;`HUB` 由 Hub DS 经回调令牌写;
  `LOGIN_PENDING` 由 login 写。写入方权威不同 → SetLocation 走状态机守卫分类放行 / 拒绝(见下)。
- **不做的事**:不算 MMR / 经验 / 掉落;不做 leader 选举 / 集群拓扑(无状态);不消费 `pandora.locator.update` topic
  (presence 扇出走 `pandora.presence.update`);候选 B placement/proof 系统已于 2026-07 硬切下线(见对外接口)。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20006` | 内部 RPC(login / matchmaker / friend / hub_allocator / Hub DS 经 Envoy) |
| HTTP | `:21006` | 仅 `/metrics`(Prometheus 抓,无 RESTful handler) |

取值来自 `internal/conf/conf.go` 的 `Defaults()`(`Server.Grpc.Addr` / `Server.Http.Addr` 零值兜底)。

## 对外接口

代码入口:`internal/service/locator.go`(实现 `locatorv1.PlayerLocatorServiceServer`,proto ↔ biz 互转、
errcode → proto.ErrCode 翻译,不抛 grpc error)。gRPC server **不挂玩家 `AuthRequired`**:本服务 RPC 由内网 DS /
login 等调用,不直接暴露给玩家客户端,Envoy 路由层限制本路径只允许内网(`internal/server/grpc.go`)。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `SetLocation(player_id, Location, hub_presence_fence)` | login(LOGIN_PENDING)/ matchmaker(MATCHING/BATTLE)/ ds_allocator(BATTLE)/ Hub DS(HUB) | 覆盖式写位置；带 fence 的 HUB 写以 assignment+admission 三元组守卫同 Pod ABA(同 assignment 内定序，跨 assignment 接受) | **HUB** 状态:Hub DS 回调令牌(`sub=hub_pod`,`RequireToken`);**其余状态**:`DenyDS`(带 DS 令牌 / 经网关即拒,仅内部东西向服务可写) |
| `GetLocation(player_id)` | login / team / matchmaker / friend 等 | 查单个玩家位置;key miss 返回 `OFFLINE` 占位(不报错) | 内网 |
| `BatchGetLocation(player_ids)` | friend(ListFriends 在线态) | Redis pipeline 一次往返批量查;miss 的 id **不回填占位**(调用方按缺席判离线) | 内网 |
| `SubscribePresence(subscriber_id, watched_player_ids)` | 客户端(打开好友面板) | 注册在线态订阅(替换语义);`presence.enabled=false` 时 no-op | 内网(`presence.enabled` 门控) |
| `UnsubscribePresence(subscriber_id)` | 客户端(关闭好友面板) | 退订;presence 未启用时 no-op | 内网 |
| `RefreshHubLocations(hub_pod, player_ids)` | hub_allocator(转发 Hub DS 心跳) | 批量续期「`state==HUB` 且 `hub_pod` 匹配」记录的 TTL(在线保活) | Hub DS 回调令牌(`sub=hub_pod`,`RequireToken`) |
| `ReportDisconnect(hub_pod, player_id, hub_presence_fence)` | Hub DS(Logout / 连接超时) | 仅 exact 当前 Admission 可把 HUB 位置 TTL 缩到 grace(~10s)并记录首次离场；legacy 请求安全 no-op | Hub DS 回调令牌(`sub=hub_pod`,`RequireToken`) |
| `ClearLocation(player_id)` | 内部(登出 / 清理) | `UNLINK` 位置 key | 内网(无 DS 守卫) |

> **已下线的 7 个 placement RPC**(`GetPlacement` / `BeginPlacementTransition` / `BindPlacementTarget` /
> `RetargetPlacementTarget` / `ConfirmPlacementSourceDeparture` / `CommitPlacementAdmission` /
> `BootstrapPlacement`):候选 B placement/proof 系统 2026-07 硬切下线,路由权威改回 TTL 位置租约。
> proto service 定义暂留(不 regen),句柄一律返回 `ERR_SERVICE_DISABLED`,无业务逻辑
> (`internal/service/locator.go` 尾部 `placementRemoved`)。

## 目录结构(Kratos 标准分层)

```
cmd/locator/main.go              启动入口(Redis 强依赖 Ping + 三层装配 + DS 守卫 / capability fence / presence worker)
etc/locator-dev.yaml             开发配置(:20006,ds_auth off,presence 纯拉)
etc/locator-prod.yaml.example    生产配置样例
internal/
  conf/conf.go                   配置结构(LocatorConf + PresenceConf + config.DSAuthConf)+ Defaults() + 校验
  service/
    locator.go                   RPC 入口(实现 PlayerLocatorServiceServer;DS 令牌 scope 绑定 + Model B 终态门 + placement 下线句柄)
    hub_credential.go            Model B Redis active credential 终态门(HubCredentialStateChecker)
  biz/
    locator.go                   LocatorUsecase 核心(SetLocation 校验 + guardTransition 状态机守卫 + Get/Batch/Refresh/Report/Clear)
    presence.go                  PresenceHub 好友在线态扇出 worker(订阅倒排 + 去抖 + 合并 + killswitch 降级)
    locator_sharding.go          位置 owner cell 锚定 / 分片键口径(LocationShardKey = player_id,nil-safe 观测)
  data/
    location.go                  RedisLocationRepo(location + HUB meta；SetGuarded / Admission fence / Refresh / exact Disconnect)
    hub_auth.go                  RedisHubAuthReader(只读 pandora:hub:auth:{pod},Model B 鉴权用)
  server/
    grpc.go                      gRPC server 注册(不挂玩家 AuthRequired)
    http.go                      HTTP server(仅 /metrics)
```

## 核心调用链

> 锚点以**函数名**为准,行号截至 HEAD 会漂移。

### 1. SetLocation —— 鉴权 → 校验 → 守卫写

```
service.SetLocation (service/locator.go)
├─ dsGuard.CheckHubCredential(scope)            HUB→hub 令牌(sub=pod,RequireToken);非 HUB→DenyDS
│     └─ [Model B] hubCredentialChecker.CheckActive(pod, cred)  读 Redis 唯一授权权威,fail-closed
├─ biz.SetLocation (biz/locator.go)             入参校验；完整 HUB fence → 同 assignment 内按 admission_seq/id 定序
│     ├─ repo.ValidateHubPresence               只读校验长 TTL meta，拒同 assignment 内的旧 Admission
│     ├─ repo.SetGuarded                        WATCH/MULTI/EXEC 写位置 + 状态机/当前连接代守卫
│     └─ repo.ActivateHubPresence               条件提交 meta 并清上一连接 left_at；并发 Disconnect 时 exact 补偿缩 TTL
├─ presence.Notify(player_id, state)            写成功后扇出(presence 未启用则 nil,跳过)
└─ logLocationPlacement                         router 注入时打 owner 落点观测日志(nil-safe)
```

- `nowMs` / TTL:`NewLocatorUsecase`(`biz/locator.go:94`)把有效 TTL 钳到 `≥ placement.DSFenceReentryBarrier`(27s)——
  BATTLE presence 是 login / matchmaker 再入门的第一道信号,TTL 太短会让分区旧 DS 尚未自我 fencing 就被放行(破 §1)。dev 默认 30s > 27s。
- `dsGuard` **nil-safe**:`ds_auth.mode=off`(dev 默认)时 guard 为 nil,`CheckHubCredential` 直接放行(不校验);
  `permissive` 灰度记 warn 放行;`enforce` 才真正拒绝。

### 2. GetLocation / BatchGetLocation —— 读投影

- `GetLocation`(`biz/locator.go:262`)→ `repo.Get`:key miss 返回 `LocationStateOffline` 占位(客户端 / DS 见此即判离线)。
- `BatchGetLocation`(`biz/locator.go:290`)→ `repo.BatchGet`(`data/location.go:164`):Redis pipeline 一次往返 HGETALL 多个玩家,
  **miss 不回填占位**(map 缺席即离线,避免响应被大量离线占位撞胀);`player_id==0` 与重复 id 由 data 层跳过 / 去重。

### 3. RefreshHubLocations —— Hub 心跳在线保活

`service.RefreshHubLocations`(`service/locator.go:143`)→ guard → `biz.RefreshHubLocations`(`biz/locator.go:319`)→
`repo.RefreshHubLocations`(`data/location.go:206`):两轮 pipeline(HMGET 批量读 state/hub_pod → 对「HUB 且 pod 匹配」批量 EXPIRE)。
非事务:竞争窗口内状态若切到 MATCHING/BATTLE,多续一次 30s TTL 无害(对局态由战斗链路自刷)。玩家掉线 → Hub DS 停报该 id → key 30s 自然过期 = 离线。

### 4. ReportDisconnect —— exact 连接快速断线

`service.ReportDisconnect` → guard → `biz.ReportDisconnect` → `repo.ShrinkHubTTL`：**Lua 原子单 location key**同时核对
`state=HUB + hub_pod + assignment_id + admission_id + admission_seq`，通过后才 `PEXPIRE LT`；随后在 HUB meta
内 exact compare-and-set 首次 `left_at_ms` 并发送离场事件。同 Pod 秒重连后，旧 Controller 的迟到请求身份不匹配，严格零副作用。
三字段全空的 legacy Disconnect 返回 OK/`shrunk=false`，等待正常 presence TTL；部分缺失拒绝。grace = 10s，绝不即时置 OFFLINE。

### 5. Presence 在线态扇出 worker(可选)

见下方「Presence 在线态扇出」小节。

## 位置状态机与守卫(不变量 §1)

`SetLocation` 的 `repo.SetGuarded` 在 WATCH/MULTI/EXEC 事务内先把当前记录交给 `guardTransition`(`biz/locator.go:218`)决策,
通过才覆盖写。判断分三层(旧状态越「重要」,门卫越挑剔):

- **玩家原本无记录 / 旧状态非对局态(OFFLINE / LOGIN_PENDING / HUB)** → 直接放行(普通顶号覆盖 = 自动顶号)。
- **旧 = `MATCHING`(撮合确认期)** → 只拦可能 stale 的 **HUB 上报**(玩家确认期仍连 Hub DS,Hub DS 会持续上报 HUB,
  放行会顶掉 matchmaker 刚写的 MATCHING),拒 `ERR_LOCATOR_CONFLICT`(9202);其余写放行。
- **旧 = `BATTLE`(active 战斗,最严)** → 只接受三类写,其余一律拒 `ERR_LOCATOR_CONFLICT`:
  - **`BATTLE` 且 `match_id` 相同**:同局心跳续期 / 推进 → 放行。不同 `match_id` = 旧 DS / 旧 allocator 的迟到心跳 → 拒
    (否则把当前对局位置覆盖成指向已死旧 DS 的旧对局,破 §1)。
  - **`MATCHING`**:对局生命周期控制面写(下一局撮合)→ 放行。
  - **`HUB` 带正确 `match_id` 令牌**(`== cur.MatchID` 且 `!= 0`):玩家打完回大厅的合法回流 → 放行。
  - **其余(`LOGIN_PENDING` 裸登录 / 断线重登降级、无令牌 HUB)** → 拒。这是 2026-07-02 BATTLE fence 加固
    (`battle-reconnect.md §5`)修的核心洞:防止断线重登把人从战斗里顶出去,导致 matchmaker 误判空闲、一人两处。

> **hub DS 上报契约**:玩家从战斗返回大厅时,Hub DS 上报 `HUB` 须从其 battle DSTicket 取出 `match_id` 一并带上
> (作 fence 令牌);玩家全新进入大厅(刚登录、未打过战斗)时 `match_id` 留 `0`。HUB 态的 `match_id` 仅作 fence 令牌,
> **不持久化**(`SetLocation` 进 HUB 时清 `match_id`/`battle_pod`,`biz/locator.go:163`)。

CAS 冲突耗尽 `optimisticRetry`(3 次,`biz/locator.go:50`)返回 `ERR_LOCATOR_CONFLICT`。

## 存储布局(Redis,单一真源)

### 位置记录

| Key | 类型 | TTL | 用途 |
|---|---|---|---|
| `pandora:locator:<player_id>` | hash | 30s(`location_ttl`,SetLocation / Refresh 刷新) | 玩家位置 |
| `pandora:locator:hubmeta:<player_id>` | hash | `last_seen_retention`(在线心跳续期) | 当前连接 Admission fence + 可选首次 `left_at_ms` |

字段(实际由 `data/location.go` `HSet` 写入,`parseLocationMap` 解析):

- `state`         `int32`,`LocationState` 枚举值(`0=UNSPECIFIED / 1=OFFLINE / 2=LOGIN_PENDING / 3=HUB / 4=MATCHING / 5=BATTLE`)
- `hub_pod`       HUB 时填(其余状态由 `SetLocation` 语义决定是否有值)
- `shard_id`      `uint32`(HUB shard,转字符串存)
- `match_id`      `uint64`,MATCHING / BATTLE 时填;HUB 时清 0(仅作 fence 令牌不落库)
- `battle_pod`    BATTLE 时填;HUB 时清空
- `updated_at_ms` `int64`,服务端记录的写入时刻
- `hub_assignment_id` / `hub_admission_id` / `hub_admission_seq`,HUB 连接 identity

写入用 `DEL + HSET + EXPIRE`(先 DEL 保证切状态时不残留旧字段,如 BATTLE→HUB 时 match_id 不清会误读)。
`Delete` 用 `UNLINK`(异步删,避免大 key 阻塞)。

### Hub 授权记录(只读,Model B 鉴权用)

| Key | 类型 | 写者 | 本服务用途 |
|---|---|---|---|
| `pandora:hub:auth:{pod}` | proto bytes(`HubShardAuthStorageRecord`) | hub_allocator(唯一写者) | `RedisHubAuthReader` 只读,做副作用前 active credential 校验 |

player_locator **只读不写**该记录(`data/hub_auth.go`),不参与 stage / promote —— 授权状态唯一写者仍是 hub_allocator,
避免旧副本 read-modify-write 丢弃未知 proto 字段(不变量 §17)。

## Presence 在线态扇出(`biz/presence.go`,默认关闭)

落地 `friend-distributed-scaling.md §13.4 / §13.5`,默认 `presence.enabled=false`(§13.7「先拉后推」,`SubscribePresence` 变 no-op、不起 worker)。
开启需配 `kafka.brokers`(往 `pandora.presence.update` 生产 → push 服务投递)。`PresenceHub`(`biz/presence.go:77`)四段处理:

- **只推订阅者**(§13.4.1):内存订阅倒排索引(`watchedID → 订阅者集合`),好友上线只推给「此刻正盯着这一行看的人」,扇出从 N 降到个位数。
- **去抖**(§13.4.2):变更进 `DebounceWindow`(默认 8s)窗口,窗口内回退到原状态判为抖动不推。
- **合并**(§13.4.3):后台 tick(`CoalesceTick` 默认 1s,`step` @ `biz/presence.go:234`)把同订阅者多条变更攒成一条 `PresenceBatchEvent`。
- **降采样**(§13.4.4):只推粗粒度 `PresenceStatus`(在线 / 离线 / 游戏中,`coarsePresence`)。
- **洪峰降级**(§13.5):挂 `pkg/killswitch`(key `presence/fanout`),降级时丢在途事件退回纯拉,保主链路。

由 `LocatorUsecase.SetLocation` / `ClearLocation` 成功后调 `presence.Notify` 喂入(非阻塞);Kafka key = `subscriber_id`
(不变量 §9 同订阅者事件保序,`cmd/locator/main.go` 的 `presencePusher`)。**架构取舍(v1)**:订阅倒排是单实例内存态
(与 push 服务 ConnectionManager 同档),多实例水平扩展需把倒排下沉 Redis + presence 变更走 Kafka 分区到单一 fan-out 消费组,列为后续。

## 配置项(`internal/conf/conf.go`)

| 键 | 默认 | 说明 |
|---|---|---|
| `server.grpc.addr` | `:20006` | gRPC 监听(`Defaults()` 兜底) |
| `server.http.addr` | `:21006` | HTTP 监听,仅 `/metrics` |
| `locator.location_ttl` | `30s` | 位置 hash TTL(有效值经代码钳到 `≥ 27s` DS 再入屏障下限) |
| `presence.enabled` | `false` | 是否开好友在线态订阅扇出;false = 纯拉(Subscribe/Unsubscribe no-op) |
| `presence.debounce_window` | `8s` | 上线去抖窗口(§13.4.2) |
| `presence.coalesce_tick` | `1s` | 合并 / flush tick 间隔(§13.4.3) |
| `presence.kill_switch_key` | `presence/fanout` | 洪峰降级 killswitch 规则 key(§13.5) |
| `ds_auth.mode` | `off` | DS 回调令牌校验:`off`(dev 默认)→ `permissive`(灰度)→ `enforce`(强制) |
| `ds_auth.authority_mode` | `legacy` | `legacy` 不读 hub auth record;`redis` = 启用 Model B active credential 终态门(须与 hub_allocator 同步切) |
| `ds_auth.active_heartbeat_max_age` | `30s` | Model B:active 凭据心跳新鲜度上限 |
| `ds_auth.secret` | — | DS 回调令牌验签密钥,必须与 hub_allocator `ds_auth.secret` 一致 |
| `ds_auth.fence.*` | — | `authority_mode=redis` 时的 etcd capability lease(endpoints / prefix / lease_ttl / keyset_revision) |

**强依赖**:Redis(`node.redis_client.host` 单实例或 `.addrs` 集群),启动期 `Ping` 失败直接 exit。
带 fence 的 HUB 上线**不引入额外服务依赖**：连接身份由 Hub DS 随请求携带，locator 只在同 assignment 内比对，
跨 assignment 的归属交给 hub_allocator 权威(§9.22，locator 不反向定序)。**启动校验**:`ValidateDSAuthAuthorityMode` 拒绝拼错的 `authority_mode`
(误写非 `legacy|redis` 会静默绕过 active credential 门);`authority_mode=redis` 时还校验 fence 配置并申请
`dsauthfence` capability,失租(`fence.Lost()`)立即 `os.Exit`,禁止旧 epoch 副本继续接受 Hub 写回。

## 本地启动

```powershell
# 1. 起 Redis(强依赖;dev 端口 6380,见 etc/locator-dev.yaml)
docker compose -f deploy/docker-compose.dev.yml up -d redis

# 2. 起 locator(dev 配置:ds_auth off / presence 纯拉 / reflection 开)
go run ./services/runtime/player_locator/cmd/locator -conf services/runtime/player_locator/etc/locator-dev.yaml

# 3. grpcurl 直连 :20006 联调(dev mode=off,HUB 写无需令牌;enforce 下 HUB/Refresh/Report 需 hub 回调令牌)
grpcurl -plaintext -d '{\"player_id\":10086,\"location\":{\"state\":3,\"hub_pod\":\"hub-0\",\"shard_id\":1}}' `
  127.0.0.1:20006 pandora.locator.v1.PlayerLocatorService/SetLocation

grpcurl -plaintext -d '{\"player_id\":10086}' 127.0.0.1:20006 pandora.locator.v1.PlayerLocatorService/GetLocation

grpcurl -plaintext -d '{\"player_ids\":[10086,10087]}' 127.0.0.1:20006 pandora.locator.v1.PlayerLocatorService/BatchGetLocation

grpcurl -plaintext -d '{\"player_id\":10086}' 127.0.0.1:20006 pandora.locator.v1.PlayerLocatorService/ClearLocation
```

> `state` 数值:`1=OFFLINE / 2=LOGIN_PENDING / 3=HUB / 4=MATCHING / 5=BATTLE`。写 MATCHING/BATTLE 需带 `match_id`
> (BATTLE 还需 `battle_pod`),否则 biz 校验返回 `ERR_INVALID_ARG`。

> **发布顺序**：先全量升级 locator 并配置/验证 `owner_addr`，再让 Hub/UE 发送 `hub_presence_fence`。旧 Hub
> 过渡期的 Disconnect 只退化为正常 TTL；新协议不能先打到仍执行旧断线语义的旧 locator。

## 关联文档

- [`go-services.md §2.6`](../../../docs/design/go-services.md) — player_locator 要约:唯一查询入口 + presence 投影(非归属权威)语义
- [`infra.md`](../../../docs/design/infra.md) — 端口(20006/21006)与 `pandora.locator.update` topic 登记
- [`friend-distributed-scaling.md §13`](../../../docs/design/friend-distributed-scaling.md) — BatchGetPresence(§13.3)+ presence 订阅扇出 / 去抖 / 合并 / killswitch(§13.4/§13.5/§13.7)
- [`battle-reconnect.md §5`](../../../docs/design/battle-reconnect.md) — BATTLE fence 加固(拒裸登录顶掉 active 战斗)
- [`scale-cellular-20m.md §4.2`](../../../docs/design/scale-cellular-20m.md) — 位置 owner cell 锚定 / 分片键口径(`LocationShardKey = player_id`)
- [`owner-authority.md`](../../../docs/design/owner-authority.md) — §9.22 owner 权威本体(locator 只作 presence 投影)
- [`decision-revisit-ds-callback-auth.md`](../../../docs/design/decision-revisit-ds-callback-auth.md) — DS 回调令牌校验 / Model B active credential 门
