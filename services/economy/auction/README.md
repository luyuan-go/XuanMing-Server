# auction

> 全服拍卖行 / 撮合引擎:玩家挂单(SELL)、出价(BUY),按 `market_id` 分片,**每个 market 单写者**
> 从 MySQL 精确物品候选做价格-时间优先撮合,成交经三段式 escrow 由 inventory 原子对转资产。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`
> 的 [`decision-revisit-auction-match-authority.md`](../../../docs/design/decision-revisit-auction-match-authority.md)
> (撮合权威 / 持久 saga / 不停服迁移,取代早期 Redis 撮合口径)与
> [`decision-revisit-auction-engine.md`](../../../docs/design/decision-revisit-auction-engine.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:挂单 / 出价 → 权威撮合(价格-时间优先)→ 成交结算(inventory 资产对转)→ 成交事件外发。
- **权威态**:订单、成交事实、结算意图、待补偿副作用**全在 MySQL**(`pandora_auction`,强依赖)。
  单写者从 MySQL 按 `item_config_id` 精确选候选串行撮合,权威订单不会并发改 → 不超卖。
- **Redis 的角色**(当前部署仍强依赖,但**不承担撮合正确性**):① 跨实例 per-market 单写者锁;
  ② 单玩家订单配额索引(Lua 原子);③ 旧版本兼容的订单簿 ZSET 缓存(滚动 / 回滚期旧实例可见,新版本不从它选候选)。
- **不做的事**:不自行扣转资产(`Freeze` / `Settle` / `Release` 全部委托 inventory,幂等键 order_id / match_id);
  不算价格 / 派生数值(成交价 = 被动挂单价,由撮合规则确定);不持久化玩家在线状态。
- **与 matchmaker 的关键区别**:auction 的 5 个 RPC 是**同步权威操作**(挂单那一刻在 market 锁内完成撮合、
  返回真实 `status` / `filled_quantity`),**不是**"已受理型 + push 驱动";异步的只有成交结算副作用与成交事件外发。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20016` | 客户端 RPC(经 Envoy jwt_authn)|
| HTTP | `:21016` | 仅 `/metrics` |

端口默认值来自 `internal/conf/conf.go` 的 `Defaults()`(`Server.Grpc.Addr` / `Server.Http.Addr`)。

## 对外接口

代码入口:`internal/service/auction.go`(实现 `auctionv1.AuctionServiceServer`)。Envoy 路由层已 require JWT,
`pmw.AuthOptional()` 从 `x-pandora-player-id` header 注入 `player_id` 到 ctx,service 层再用
`callerID(ctx)`(`auction.go:118`)兜底:`player_id=0` → `ERR_UNAUTHORIZED`。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `PlaceOrder(market_id, item_config_id, quantity, price, idempotency_key)` | 客户端 | 卖家挂单(SELL);同步撮合后返回 `order_id` / `status` / `filled_quantity` | JWT `player_id`(= seller) |
| `Bid(market_id, item_config_id, quantity, price, idempotency_key)` | 客户端 | 买家出价(BUY);同步撮合后返回同上 | JWT `player_id`(= buyer) |
| `CancelOrder(market_id, order_id)` | 客户端 | 撤单(仅本人、未终态);置 CANCELED + 退还 escrow | JWT `player_id` |
| `ListMarket(market_id, side, limit)` | 客户端 | 看某市场订单簿(`side` 省略 = 买卖两侧) | JWT(仅要求已登录) |
| `ListMyOrders(active_only, cursor_order_id, limit)` | 客户端 | 看自己的挂单 / 出价,按全局 `order_id` DESC 游标分页(默认 50 / 最大 100) | JWT `player_id` |

> **身份一律以 JWT 为准(R5)**:seller / buyer / player 都取 ctx 中 JWT `player_id`,**忽略请求体里的对应字段**,
> 防伪造他人身份。约束固化在 `internal/service/auth_boundary_test.go`(无 JWT 的每个入口都必须在触达
> 业务状态机 / market 锁 / 资产托管前返回 `ERR_UNAUTHORIZED`)。
>
> **auction 自身不暴露内部专用 RPC**。资产结算是**反向**关系:auction 作为 client 直连 inventory 的系统 RPC
> `SettleAuctionMatch` / `FreezeForOrder` / `ReleaseEscrow` / `EnsureAuctionEscrow`(内网 insecure、无 JWT →
> inventory 侧 `callerID==0` 认内网直连,见 `internal/data/settlement_client.go` 与
> [`decision-revisit-internal-service-auth.md`](../../../docs/design/decision-revisit-internal-service-auth.md))。

## 目录结构(Kratos 标准分层,对齐 matchmaker / login)

```
cmd/auction/main.go            启动入口(MySQL/Redis/Snowflake/Kafka/inventory 装配 + 后台补偿/清扫 goroutine)
etc/auction-dev.yaml           单库 dev 配置(gRPC :20016 / HTTP :21016)
internal/
  conf/conf.go                 AuctionConf + Defaults(端口 / 上限 / TTL / 保留期 / 锁参数默认值)
  service/
    auction.go                 RPC 入口(JWT ctx 取 player_id;proto ↔ biz 互转;errcode → proto ErrCode)
  biz/
    auction.go                 AuctionUsecase 核心(submit / match / 结算补偿 / owner 配额 / legacy 恢复)
    market_router.go           market → 实例 HRW(rendezvous)归属,纯函数(多实例锁竞争最小化)
  data/
    auction_repo.go            MySQL 权威仓储(订单 / 成交 / 幂等键 / owner guard;按 market_id / owner_id 分片路由)
    book.go                    Redis ZSET 订单簿兼容缓存(非权威,新版本不从它选候选)
    owner_slot_limiter.go      Redis SET 单玩家订单配额(SCARD+SADD Lua 原子预留 + Sync 预热)
    market_locker.go           Redis 跨实例 per-market 单写者锁(续租 / 失租 fail-stop)
    settlement_client.go       inventory gRPC client(Freeze / Ensure / Settle / Release)
    shard_topology.go          分片拓扑启动门禁(exact-match marker,任何漂移 fail-fast)
    retention.go               §9.24 保留期逐分片批删(终态订单 / 已结算成交 / 超期幂等键)
  server/
    grpc.go                    gRPC server 注册(AuthOptional 中间件)
    http.go                    HTTP server(仅 /metrics)
```

## 核心调用链

### 1. PlaceOrder / Bid —— 同步权威撮合(不是受理型)

`PlaceOrder`(`service/auction.go:37`)/ `Bid`(`auction.go:55`)→ `AuctionUsecase.PlaceOrder` / `Bid`
统一进 `submit`(`biz/auction.go:277`)。全程持 market 单写者锁,顺序不可倒:

```
submit(持 market 锁)
├─ 入参校验(quantity/price 上限、total=q*p 溢出、idempotency_key 字符集)
├─ ClaimOrder            PENDING 幂等登记(uk owner+idem;返回 canonical order_id)   data/auction_repo.go:207
├─ reserveOwnerSlotPruning  Redis Lua 原子预留配额(满则按 MySQL 权威惰性清理后重试一次) biz/auction.go:498
├─ ledger.Freeze         冻结 escrow(幂等键 order_id;不足 → ERR_AUCTION_INSUFFICIENT) settlement_client.go:47
├─ ConfirmOrderEscrow    escrow_verified=1(撮合准入前提)                            auction_repo.go:432
├─ match                 ★ 价格-时间优先逐笔撮合(下节)                             biz/auction.go:417
├─ ActivateOrder         未成交余量 PENDING → OPEN                                   auction_repo.go:425
└─ addBookCache / tryReleaseOrder + pushAudit,返回 toProtoOrder(rec)
```

关键点:`submit` 顶部注释(`biz/auction.go:274`)说明为何**先 PENDING 再冻结**——PENDING 使进程在
「登记后、冻结前」退出时订单不会被撮合选中;相同 idem 重试继续 `Freeze(order_id)` 并激活,不会另建订单
或把未冻结订单暴露给撮合。`StatusPending` 是**仅服务端可见**状态,绝不以 proto UNSPECIFIED 暴露给客户端列表。

### 2. match —— MySQL 权威价格-时间优先撮合

`match`(`biz/auction.go:417`)在 market 锁内让 incoming 与同一 `item_config_id` 的对手盘逐笔成交:

1. `FindBestActiveOrder`(`auction_repo.go:562`)从 MySQL 按精确物品 + 非本人 + 价格-时间(SELL 取最高买价 /
   BUY 取最低卖价,同价按 `order_id` 早者优先)选一张对手单。**候选必须来自 MySQL**:Redis ZSET 旧 key 只含
   `market_id`、无法区分同市场不同物品,放进正确性链路会跨物品成交 / 前缀饥饿 / 缓存失败即永久不可见。
2. `crosses`(`biz/auction.go:811`)判价格是否交叉;不交叉即停。
3. `ReserveMatch`(`auction_repo.go:585`)在**单个事务**内 `FOR UPDATE` 锁定两单(按 order_id 升序防死锁)、
   复验全部成交不变量(`validReservation`)、写 PENDING 成交意图、原子推进双方 `filled_quantity` / `status`。
   候选在 SELECT 与 FOR UPDATE 之间被别实例消费时 `reserved=false`(不产生写入),有限重试(≤64)防热点忙等。
4. `settleMatch`(`biz/auction.go:916`)对已持久化成交调 inventory `Settle`(见第 3 节),成功后条件更新 COMPLETED。
5. incoming 部分成交后 `match_pending=1` 持久续跑标记,避免进程退出留下已交叉却无人处理的 PARTIAL 单。

### 3. 成交结算 —— 先持久意图,后调外部,崩溃幂等补偿

外部账本调用绝不在事务里做;`ReserveMatch` 事务只写 PENDING 意图,提交后才调 inventory:

```
settleMatch (market 锁内)
├─ ledger.Settle(match_id)   inventory 卖↔买资产原子对转,幂等键 match_id     settlement_client.go:103
├─ 成功 → CompleteMatch      settlement_status PENDING→COMPLETED + event_pending=1  auction_repo.go:714
└─ 失败 → deferMatchSettlement  写退避时间,marker 保持 PENDING                  biz/auction.go:945
```

进程在「Settle 成功、Complete 前」退出也安全:`match_id` 是幂等键,下轮重复 Settle 不会重复转资。
后台 `ReconcilePendingSideEffects`(`biz/auction.go:1176`,main 每 `side_effect_reconcile_interval_seconds` 跑一轮)
按优先级扫描并幂等补齐:终态 marker 修复 → 就绪 escrow 释放(`ListReleasableOrders`)→ PENDING 冻结恢复
(`recoverPendingOrder`)→ legacy 未验证活跃单补冻(`recoverUnverifiedActiveOrder`)→ 撮合续跑
(`recoverMatchPendingOrder`)→ 待结算成交(`ListPendingMatches` + `settleMatch`)。多副本可并发跑,
`Settle`/`Release` 幂等、`Complete`/`Clear` 条件更新,不会重复转资。

### 4. 成交事件 outbox —— 至少一次,与资产补偿分 goroutine

`ReconcilePendingMatchEvents`(`biz/auction.go:1286`,**独立 goroutine**)消费成交事件 outbox:
`ListPendingMatchEvents`(`auction_repo.go:761`)→ `publishMatchEvent`(`biz/auction.go:869`)发
`pandora.auction.match`(kafka key = `match_id`,同一成交保序)→ 成功 `ClearMatchEventPending`。
必须独立于资产补偿:kafkax/Sarama 同步 `Send` 无法被 ctx 可靠中断,broker 故障只能阻塞事件 worker,
不能拖慢下一轮 Settle / Release。`ListPendingMatchEvents` 还以 `release_pending` 做屏障——关联终态订单的
escrow 未释放前成交事件不可见。订单流转 `audit` 事件(`pandora.auction.audit`,key = `order_id`)是**弱依赖**:
`pushAudit`(`biz/auction.go:892`)只做有界非阻塞入队,单 worker 锁外发送,队列满告警丢弃,绝不反压交易主路径。

### 5. 挂单幂等 —— 跨分片三段式 ClaimOrder

`ClaimOrder`(`auction_repo.go:207`)处理 owner coordinator 分片与 market 分片可能不同的问题
(绝不持 coordinator 事务连接再查 / 写其它 shard,防跨库连接池环):

```
① 事务外扫描 legacy(findOrdersByOwnerIdempotency,兼容历史订单)
② coordinator 单库事务:auction_owner_guards 行 FOR UPDATE → 锁内重读 registry → 登记不可变 canonical
③ 事务外幂等补回 market PENDING(ensureCanonicalOrder,1062 后权威回读收敛)
```

`uk(owner_id, idempotency_key)` 命中即返回已有 canonical `order_id`(`already=true`),指纹不一致
(`sameOrderFingerprint`)→ `ERR_AUCTION_IDEMPOTENCY_CONFLICT`。**canonical `order_id` 一旦登记不可变**——
`already` 只决定能否直接回放终态 / 活跃态,不改后续资产幂等键。

### 6. 过期清扫 + 保留期清理

- **过期清扫**(限制#1 补偿):`OrderTTLSeconds > 0` 时 main 起后台 ticker,`ExpireDueOrders`
  (`biz/auction.go:654`)→ `expireOne`(持锁重读防误改)把超 TTL 仍未成交挂单置 EXPIRED、移出簿、退还 escrow。
- **保留期清理**(§9.24 库增长有界):main 每 `retention_sweep_interval_seconds` 调
  `PurgeRetention`(`data/retention.go:30`)逐分片 `DELETE ... LIMIT` 批删——终态订单(`release_pending=0` /
  `match_pending=0` 且超期)、已结算成交(`event_pending=0` 且超期)、超期幂等键映射,默认保留 90 天。
  多副本各自跑,DELETE 幂等无需锁。

## 订单状态机

状态常量:`internal/data/auction_repo.go:29`(`StatusPending=0` / `Open=1` / `Partial=2` / `Filled=3` /
`Canceled=4` / `Expired=5`)。

```
                    Freeze 失败 / 配额失败
        ┌────────────────────────────────► CANCELED(release_pending=1)
        │
   PENDING ──Freeze+ConfirmEscrow──► OPEN ──部分成交──► PARTIAL ──全部成交──► FILLED
  (仅服务端可见)                       │  \                │  \
                                      │   撤单/TTL过期     │   撤单/TTL过期
                                      ▼                    ▼
                              CANCELED / EXPIRED    CANCELED / EXPIRED
```

- **PENDING** 是冻结前的仅服务端状态,不映射客户端业务状态、不进 `ListMyOrders`(`ListOwnerOrders`
  用 `status <> 0` 过滤,`auction_repo.go:952`)。
- **终态** = FILLED / CANCELED / EXPIRED(`isTerminal`,`biz/auction.go:819`);终态订单置 `release_pending=1`,
  由 Release 幂等退还 escrow 残余后清标记。
- **撮合准入**:只有 `escrow_verified=1` 且 `remaining>0` 的 OPEN / PARTIAL 单能成为撮合双方
  (`FindBestActiveOrder` / `validReservation` 强校验),防旧二进制遗留的未验证托管进场。

## 存储布局

### MySQL(`pandora_auction`,强依赖;单库或恰好 2 个有序 DSN 分片)

按 `market_id % N` 路由订单 / 成交(`ForMarket`),按 `owner_id % N` 路由幂等键 / owner guard(`ForOwner`);
分片拓扑经 `auction_shard_topology` marker exact-match 门禁,片数 / 顺序 / 目标库任何漂移都 fail-fast
(`data/shard_topology.go`)。`node.mysql_client.shards` > 2 直接拒绝启动(main.go:93)。

| 表 | 分片键 | 用途 / 关键列 |
|---|---|---|
| `auction_orders` | `market_id` | 订单权威。`status` / `filled_quantity` / `release_pending` / `match_pending` / `escrow_verified` / `*_next_attempt_at_ms` 退避列;`uk(owner_id, idempotency_key)` |
| `auction_matches` | `market_id` | 成交事实 + 结算意图。`settlement_status`(PENDING/COMPLETED)/ `event_pending` outbox 标记 / 退避列 |
| `auction_idempotency_keys` | `owner_id` | 挂单幂等 canonical 映射(不可变 `order_id`);与 orders 不同分片,故按 `created_at_ms` 独立清理 |
| `auction_owner_guards` | `owner_id` | 每 owner 一行,`ClaimOrder` `FOR UPDATE` 串行同 owner 幂等登记(慢增长,不清理) |
| `auction_shard_topology` | 每库单行 | 分片拓扑启动门禁 marker(不清理) |

### Redis(强依赖,但不承担撮合正确性)

| key | 类型 | 用途 |
|---|---|---|
| `pandora:auction:book:{<market_id>}:ask` / `:bid` | ZSET | 旧版本兼容订单簿缓存(hashtag 锁同市场同 slot;新版本不从它选候选) |
| `pandora:auction:owner-slots:{<owner_id>}` | SET | 单玩家 PENDING+活跃订单配额索引(SCARD+SADD Lua 原子预留,成员 `market_id:order_id`) |
| `pandora:auction:market:<market_id>` | string(redislock) | 跨实例 per-market 单写者锁 token(TTL ≤ 30s,续租 1/3 TTL,失租 fail-stop) |

### Kafka

| topic | key | 依赖强度 |
|---|---|---|
| `pandora.auction.match` | `match_id` | 成交事件,MySQL outbox 至少一次(`event_pending` 屏障 + 后台补投) |
| `pandora.auction.audit` | `order_id` | 订单流转审计,**弱依赖**(进程内有界队列,满则丢弃) |

## 关键设计点 / 不变量

| 主题 | 约束 | 代码锚点 |
|---|---|---|
| per-market 单写者 | 进程内 256 条带锁(总是)+ 可选 Redis 跨实例锁;同一 market 全程串行 | `guardMarket` / `lockMarket` |
| 撮合权威在 MySQL | 候选按 `item_config_id` 精确 + 价格-时间从 MySQL 选;Redis 只兼容缓存 | `match` / `FindBestActiveOrder` |
| 成交原子 + 幂等 | 事务锁双单 + 复验不变量 + 写 PENDING 意图;`match_id` 幂等,资产只转一次(§9.2/§9.7)| `ReserveMatch` / `CompleteMatch` |
| 三段式 escrow | Freeze(挂单冻)/ Settle(成交对转)/ Release(撤单过期退还),幂等键 order_id/match_id | `SettlementLedger` |
| 副作用先持久后调外部 | 事务写意图 → 提交后调 inventory → 崩溃后台幂等补偿,不丢意图不重复转资 | `ReconcilePendingSideEffects` |
| 挂单幂等 | `uk(owner+idem)` canonical `order_id` 不可变;跨分片三段式登记 | `ClaimOrder` |
| 单玩家配额上限 | `max_active_orders_per_player` 默认 200,Redis Lua 原子 + MySQL 权威惰性清理(fail-closed)| `reserveOwnerSlotPruning` |
| 库增长有界(§9.24) | 终态订单 / 已结算成交 / 超期幂等键默认 90 天逐分片批删 | `PurgeRetention` |
| 分片拓扑不漂移 | 片数 / 顺序 / 目标库 exact-match marker,漂移 fail-fast;>2 分片拒启 | `ValidateShardTopology` |
| 失租 fail-stop | market 锁续租失败即 `os.Exit(1)`,不再证明唯一写者时立即退出 | `marketLockLease.renewLoop` |
| 身份下沉 JWT | seller/buyer/player 一律取 ctx JWT,忽略请求体字段(R5)| `service/auction.go:callerID` |

## 配置项(`internal/conf/conf.go`)

| 键(`auction.*`) | 默认 | 说明 |
|---|---|---|
| `max_quantity_per_order` | `1_000_000` | 单挂单 / 出价最大数量,防天量 |
| `max_price` | `1_000_000_000` | 单价上限,防溢出 / 异常价(入口另拒 `quantity*price` 溢出 int64)|
| `max_active_orders_per_player` | `200` | 单玩家 PENDING+OPEN+PARTIAL 硬上限(§18 受管列表)|
| `default_list_limit` / `max_list_limit` | `50` / `200` | `ListMarket` 返回条数默认 / 上限(`ListMyOrders` 另为 50 / 100)|
| `inventory_addr` | `""`(dev `127.0.0.1:20015`)| inventory 内网 gRPC 地址;配了走真实结算,留空须显式 `allow_noop_settlement` |
| `allow_noop_settlement` | `false` | 仅联调 / 单测:`inventory_addr` 空时退回 Noop 结算,否则 fail-fast |
| `allow_noop_match_events` | `false` | 仅本地无 Kafka 联调:`brokers` 空时允许禁用成交事件,否则 fail-fast |
| `passive_warmup` | `false` | 蓝绿 R3 只读预热门禁:拒写 + 停 legacy 验证 / 补偿 / 清扫;旧实例全下线后才改回 |
| `shard_topology_generation` | `auction-v1` | 分片拓扑代际标识(写入每片 marker,不得靠改本字段覆盖既有 marker)|
| `allow_shard_topology_bootstrap` | `false` | 仅授权「所有 marker 都不存在」的首次双分片登记,成功后须恢复 false |
| `order_ttl_seconds` | `0`(dev `604800`)| >0 启用过期清扫(超 TTL 未成交挂单 EXPIRED + 退还 escrow);≤0 永不过期 |
| `expiry_sweep_interval_seconds` / `expiry_sweep_batch` | `60` / `200` | 过期清扫间隔 / 单批上限 |
| `side_effect_reconcile_interval_seconds` / `side_effect_reconcile_batch` | `5` / `100` | 结算 / escrow 释放 / 事件补偿扫描间隔 / 每片每表批量 |
| `audit_queue_capacity` | `1024` | 弱依赖 audit 进程内异步队列上限,满则告警丢弃 |
| `cross_instance_lock` | `false` | **兼容字段**:新二进制无论取值都启用跨实例 market 锁(Redis 已是强依赖)|
| `market_lock_ttl_seconds` | `30` | 跨实例 market 锁 TTL(钳到 ≤ 30s,不变量 §10)|
| `market_lock_max_wait_ms` | `3000` | 抢 market 锁最大等待,超时返回 `ERR_AUCTION_MARKET_BUSY` |
| `retention_days` | `90` | 终态挂单 / 已结算成交 / 超期幂等键保留天数(§9.24)|
| `retention_sweep_interval_seconds` / `retention_sweep_batch` | `3600` / `500` | 保留期清理间隔 / 每片每表批删上限 |

> 多实例部署另需 `cell_route.market_self` / `cell_route.market_peers`(`pkg/cellroute`):HRW 把同一 market
> 固定路由到 owner 实例,把跨实例锁竞争降到最低;留空退化为单实例拥有全部市场。

## 本地启动

```powershell
# 前置基础设施:MySQL(pandora_auction)+ Redis + Kafka;真实结算还需 inventory 服务(:20015)。
# dev yaml 已配 inventory_addr=127.0.0.1:20015、单库 dsn、redis :6380、kafka :9093。

go run ./services/economy/auction/cmd/auction -conf services/economy/auction/etc/auction-dev.yaml
```

> 无 inventory / 无 Kafka 的纯撮合联调:把 `inventory_addr` 留空并显式设 `allow_noop_settlement: true`
> (成交不真实扣转),`brokers` 留空并 `allow_noop_match_events: true`(禁用成交事件)。**生产两者都必须
> 保持默认 false**,漏配即 fail-fast,防止静默以「成交不结算 / 事件被禁用」启动。

## 关联文档

- [`decision-revisit-auction-match-authority.md`](../../../docs/design/decision-revisit-auction-match-authority.md) — 撮合权威 / 持久 saga / 不停服迁移(最终口径,取代早期 Redis 撮合)
- [`decision-revisit-auction-engine.md`](../../../docs/design/decision-revisit-auction-engine.md) — auction 作为独立服务的初版设计与市场分片
- [`auction-blockers-claude-review-20260712-final.md`](../../../docs/design/auction-blockers-claude-review-20260712-final.md) — 上线阻断项复审与剩余边界
- [`scale-cellular-20m.md`](../../../docs/design/scale-cellular-20m.md) §4.3 — 全局市场按 `market_id` 分片与 HRW 归属路由
- [`zero-downtime-update.md`](../../../docs/design/zero-downtime-update.md) — 蓝绿 `passive_warmup` / legacy 订单恢复 / 兼容字段的不停服演进契约
- [`decision-revisit-internal-service-auth.md`](../../../docs/design/decision-revisit-internal-service-auth.md) — auction 调 inventory 系统 RPC 的内网直连身份模型
- [`infra.md`](../../../docs/design/infra.md) — 服务端口 / key 命名规划
