# trade

> 玩家间交易服务:卖方挂单(`CreateOrder`)→ 买方先确认 → 卖方后确认触发结算,
> 双确认防单方面成交;订单状态机存 Redis,结算走 inventory 的 P2P 原子对转(幂等键 = `order_id`,不变量 §9.7),
> 每次状态流转发 kafka `pandora.trade.audit` 供审计 / 对账。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`
> 的 [`decision-revisit-trade-storage.md`](../../../docs/design/decision-revisit-trade-storage.md)、
> [`decision-revisit-trade-crossslot.md`](../../../docs/design/decision-revisit-trade-crossslot.md);
> 跨服务要约见 [`go-services.md §2.9`](../../../docs/design/go-services.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:玩家间两阶段确认交易 —— 挂单 → 双方确认 → 原子结算 → 审计。
- **权威态**:订单状态机(`Order` proto bytes)+ 玩家订单反查索引(set)全在 **Redis**
  (强依赖,进程内不缓存、不降级)。资产扣转的权威在 **inventory**,trade 只持有"订单意图 + 结算幂等键"。
- **状态权属**:trade 是订单 `OrderState` 状态机的唯一权威;`SELLER_CONFIRMED` 之后进入**结算围栏**,
  资产账本(inventory)成为收敛权威,trade 只按账本结果向 `COMPLETED` / `FAILED` 收敛。
- **不做的事**:不真实扣转背包 / 货币(那是 inventory `SettlePlayerTrade` 的活,trade 只挂幂等键委托);
  不做拍卖 / 撮合定价(那是 auction);不持久化到 MySQL(订单态是短生命周期 Redis 态,审计流水才落库,由 audit 消费者负责)。
- **无后台循环 / 无 leader 选举**:trade 没有撮合循环那样的单写者后台 worker;订单过期是**惰性判定**
  (访问订单时按 `expires_at_ms` 就地置 `EXPIRED`),配额清理也是**配额满时惰性回收**死成员。任意副本无状态、可水平扩展。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20012` | 客户端 RPC(经 Envoy)+ 内部 RPC |
| HTTP | `:21012` | 仅 `/metrics` |

端口默认值来自 `internal/conf/conf.go` 的 `Defaults()`(`Server.Grpc.Addr` / `Server.Http.Addr`);全局登记见 `infra.md` 端口规划表(trade = 20012 / 21012)。

## 对外接口

代码入口:`internal/service/trade.go`(gRPC service 层)。鉴权模型:

- **玩家身份一律以 JWT ctx 为准**(R5):`callerID(ctx)`(`service/trade.go:99`)从 `pmw.AuthOptional` 注入的
  `player_id`(Envoy `x-pandora-player-id` header)读取,**忽略请求体里的 `seller_id` / `player_id` 字段**,防伪造他人身份。
  `player_id == 0`(无 JWT)→ 直接返回 `ERR_UNAUTHORIZED`(回归测试 `auth_boundary_test.go` 锁死此约束)。
- **会话现行性门**:`pmw.SessionCurrent`(`server/grpc.go:24`)校验请求 jti == login 会话权威当前一代,顶号 / 登出后旧 JWT 在 exp 前立即失效(R5 复审 P0-1,INC-20260722-004)。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `CreateOrder(buyer_id, items, buyer_items, price)` | 客户端(卖方) | 卖方挂单,返回 `order_id`;seller 以 JWT 为准 | JWT `seller_id` |
| `ConfirmOrder(order_id)` | 客户端(买 / 卖任一方) | 两阶段确认;卖方后确认触发结算,返回最新 `OrderState` | JWT `player_id`(须为订单参与方) |
| `CancelOrder(order_id)` | 客户端(买 / 卖任一方) | 取消订单(仅 `PENDING` / `BUYER_CONFIRMED` 可取消) | JWT `player_id`(须为订单参与方) |
| `ListMyOrders(active_only, cursor, limit)` | 客户端 | 列本人参与的订单,`order_id` 降序游标分页 | JWT `player_id` |

> **无内部专用 RPC**:trade 自身不对外暴露内网系统接口(不像 matchmaker 有 `ReleaseMatch` / `ResolvePlayerMatchContext`);
> 4 个 RPC 全是客户端 JWT 面。trade 反过来是**内部 RPC 的调用方**:结算时以内网 insecure 直连调 `inventory.SettlePlayerTrade`
> (无 JWT → inventory 侧 `callerID==0` 认系统直连,见 [`decision-revisit-internal-service-auth.md`](../../../docs/design/decision-revisit-internal-service-auth.md))。
>
> **协议原则**:`ConfirmOrder` 的返回值是同步的最新 `OrderState`(结算成功即返 `COMPLETED`);对账 / 审计的最终事实以
> `pandora.trade.audit` 流水为准。相关排序契约见 [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md)。

## 目录结构(Kratos 标准分层,对齐 matchmaker / login)

```
cmd/trade/main.go              启动入口(redis Ping + snowflake + kafka audit + ResourceLedger 选路 + session gate 装配)
etc/trade-dev.yaml             开发配置(20012 / redis 6380 / kafka 9093 / allow_noop_ledger=true)
etc/trade-prod.yaml.example    生产配置样例
internal/
  conf/conf.go                 配置结构(TradeConf + Defaults;端口 / TTL / 上限 / inventory_addr)
  service/
    trade.go                   RPC 入口(实现 tradev1.TradeServiceServer,JWT 身份下沉 + errcode→proto 映射)
    auth_boundary_test.go       身份边界回归(无 JWT 必拒 body 身份;零 order_id 在 service 边界拒)
  biz/
    trade.go                   TradeUsecase 核心(CreateOrder / ConfirmOrder / CancelOrder / ListMyOrders + 状态机 + 配额)
    trade_settlement.go        结算腿幂等键口径(SettlementLegKey)+ 跨分片 / 跨 region 落点观测(nil-safe)
    trade_test.go / trade_settlement_test.go  状态机 / 结算 / 配额单测
  data/
    trade_repo.go              Redis 订单仓储(WATCH/MULTI/EXEC 乐观锁 + Lua 配额预留)
    settlement_client.go       GrpcResourceLedger:inventory.SettlePlayerTrade gRPC client(order_id 幂等)
  server/
    grpc.go                    gRPC server 注册(AuthOptional + SessionCurrent 中间件)
    http.go                    HTTP server(仅 /metrics)
```

## 订单状态机

```
CreateOrder ──► PENDING ──买方确认──► BUYER_CONFIRMED ──卖方确认──► SELLER_CONFIRMED ──结算成功──► COMPLETED
  (卖方挂单)      │                    │                            │  (结算围栏)
                 │                    │                            └──余额/物品不足──► FAILED
                 └──任一方 Cancel─────┴──► CANCELED                (Cancel/过期一律拒)
                 └──惰性过期──────────────► EXPIRED
```

- 状态常量:`tradev1.OrderState_*`(proto 枚举);终态判定 `isTerminal`(`biz/trade.go:501`)= `COMPLETED / FAILED / EXPIRED / CANCELED`。
- **`SELLER_CONFIRMED` 是结算围栏**(INC-20260722-001):结算意图先原子落库,资产转移可能已发生或即将发生。此态下
  `Cancel` / 惰性过期 / 配额清理**一律 fail-closed**,订单只向 `COMPLETED` / `FAILED` 收敛(账本为权威)。
- `order_id` / 参与方身份非秘密——每个 RPC 一律以 JWT `player_id` 校验是否为订单参与方(`o.SellerId` / `o.BuyerId`),不认 ID 本身。

## 核心调用链

### 1. CreateOrder —— 先写主体,后原子预留双方配额

`CreateOrder`(`biz/trade.go:121`)校验入参(卖买非同人、`items` 非空、条目数 ≤ `MaxItemsPerOrder`、`price >= 0`)后,
按**写序铁律**两步落地:

```
CreateOrder
├─ 校验入参(seller≠buyer / items 非空 / 条目数上限 / price≥0)
├─ repo.CreateOrder            ① 先写订单主体 pandora:trade:order:{id}(新发 snowflake,无人引用,天然安全)
├─ reserveSlotPruning(seller)  ② 原子预留卖方反查索引名额(Lua SCARD<max 才 SADD)
├─ reserveSlotPruning(buyer)   ② 原子预留买方反查索引名额；任一步失败 → 回滚已预留 + 删主体
└─ pushAudit → return order_id
```

- **先主体后索引**(`biz/trade.go:168`):主体先落地保证「索引成员指向 X 而 X 主体不在」≡ **真死成员**,配额清理绝不误删 in-flight 预留。
- **写入侧总量上限**(不变量 §18):`reserveSlotPruning`(`biz/trade.go:198`)预留失败(满员)时,先 `pruneDeadOrderSlots`
  (`biz/trade.go:225`)惰性回收已终态 / 已过期 / 主体已被 Redis 回收的死成员,清出名额再重试一次;仍满返 `ERR_TRADE_ORDER_LIMIT`(7006)。
  死成员清理**跳过 `SELLER_CONFIRMED`**(结算中,真占用)。
- 配额预留是 Redis Lua 原子脚本 `reserveOrderSlotScript`(`data/trade_repo.go:107`):`SISMEMBER` 命中刷 TTL 幂等成功 / `SCARD >= max` 拒 / 否则 `SADD + PEXPIRE`,单 key 单 slot,Cluster 安全。

### 2. ConfirmOrder —— 两阶段确认 + 结算意图先落库

`ConfirmOrder`(`biz/trade.go:262`)在 `UpdateWithLock`(WATCH/MULTI/EXEC 乐观锁)事务内分流:

```
ConfirmOrder(WATCH 事务内只改状态,不做外部副作用)
├─ 惰性过期:expireIfStale → 置 EXPIRED 落库 → 返回 ErrTradeOrderExpired
├─ 非参与方 → ErrUnauthorized
├─ 买方 + PENDING           → BUYER_CONFIRMED(事务后读回 audit)
├─ 卖方 + BUYER_CONFIRMED   → SELLER_CONFIRMED + 标记 driveSettle(★ 结算意图原子落库)
├─ 任一方 + SELLER_CONFIRMED → 标记 driveSettle(恢复驱动:幂等重入)
└─ 其它组合 → ErrTradeWrongState
        │
        └─(事务提交成功后,锁外)─► driveSettlement
```

- **结算意图先原子落库**(INC-20260722-001 根因修复):`Settle` 是不可回滚的跨服务资产转移,绝不能放在"可能不提交、也可能重跑"的 WATCH 回调里。
  卖方确认在事务内只把 `BUYER_CONFIRMED → SELLER_CONFIRMED` **原子提交**(= 本订单进入结算通道的线性化点),EXEC 成功即完成结算围栏 fencing,
  之后才在锁外调用 `driveSettlement`。
- **WATCH 重试安全**:回调开头 `driveSettle = nil`(`biz/trade.go:270`)清残留,只信最后一次 EXEC 成功的快照。

### 3. driveSettlement —— 结算与终态收敛(幂等可重入)

`driveSettlement`(`biz/trade.go:337`)驱动一个已落库 `SELLER_CONFIRMED` 意图的订单走完结算:

```
driveSettlement(order)
├─ ledger.Settle(order, idempotencyKey=order_id)      inventory P2P 原子对转
│    ├─ OK                        → CAS SELLER_CONFIRMED → COMPLETED + audit + 跨分片落点观测
│    ├─ ErrTradeInsufficient      → 资产未动(inventory 原子拒)→ CAS → FAILED 终态 + audit
│    └─ 瞬时/UNKNOWN(超时/不可达)→ 绝不回滚、绝不置 FAILED;停留 SELLER_CONFIRMED,返可重试错误
└─ 终态 CAS 写失败 → 同上停留 SELLER_CONFIRMED,重试 Confirm 收敛;Error 告警
```

- **恢复路径**:结算窗口内进程退出 / `Settle` 瞬时失败 / 终态写失败 → 订单停留 `SELLER_CONFIRMED`,买 / 卖任一方重试
  `ConfirmOrder` 即幂等重新驱动(`Settle` 幂等键 = `order_id` 命中即成功,终态 CAS 幂等),**无需回滚**(不变量 §9.7)。
- **账本为权威**:结算已成功却发现状态偏离预期,`driveSettlement` 按账本结果强制收敛到 `COMPLETED` 并打 Error 告警(`biz/trade.go:370`)。
- 结算落地委托 `GrpcResourceLedger.Settle`(`data/settlement_client.go:50`):调 `inventory.SettlePlayerTrade`(`order_id` 幂等),
  `ERR_INVENTORY_INSUFFICIENT` → `ErrTradeInsufficient`,传输错误原样透传(订单不置 `COMPLETED`)。

### 4. CancelOrder —— 结算围栏 fail-closed

`CancelOrder`(`biz/trade.go:395`)在 `UpdateWithLock` 内:非参与方 → `ErrUnauthorized`;`SELLER_CONFIRMED` → 拒
(`ErrTradeWrongState`,结算围栏);已终态 → 拒;否则 `PENDING` / `BUYER_CONFIRMED` → `CANCELED`,事务后读回 audit。

### 5. ListMyOrders —— 反查 + 游标分页 + 惰性过期

`ListMyOrders`(`biz/trade.go:425`)`SMEMBERS` 读玩家订单反查索引(被写入侧硬上限兜在几百内,全量读安全),`order_id` 降序,
按 `cursor + limit`(默认 50 / 最大 100,`clampLimit`)分页;逐条 `GetOrder`,已被 Redis 回收的跳过;顺路对已超时的非终态订单
惰性置 `EXPIRED`(回调内重判同样排除 `SELLER_CONFIRMED`);`active_only=true` 时过滤终态。分页上限决策见
[`decision-revisit-list-pagination.md`](../../../docs/design/decision-revisit-list-pagination.md)。

## 存储布局(`internal/data/trade_repo.go`)

| Redis key | 类型 | 内容 | 说明 |
|---|---|---|---|
| `pandora:trade:order:{order_id}` | string | `trade/v1.Order` proto bytes | 订单主体权威;hashtag `{}` 括住 `order_id` 保 cluster slot 一致;TTL = `order_ttl` |
| `pandora:trade:player:{player_id}` | set | 成员 = `order_id`(文本) | `ListMyOrders` 反查;写经 `ReserveOrderSlot`(Lua SCARD+SADD)限额,不变量 §18 |

- 订单主体直接用 proto `trade/v1.Order` 序列化存 value:`Order` 已是完整客户端可见结构且无服务端独有隐藏字段,存储 / 视图同构,不额外造 `OrderStorageRecord`(CLAUDE.md §5.10)。
- 状态机写统一走 `UpdateWithLock`(`data/trade_repo.go:150`):`GET → fn(modify) → MULTI/SET/EXEC`,EXEC 冲突重试至 `OptimisticRetry` 耗尽返 `ErrTradeLockFailed`(7005);`fn` 返回业务错误则透传不重试。
- **无 MySQL**:trade 进程不直接落库;审计流水经 kafka `pandora.trade.audit`(key = `order_id`,同订单事件保序)交由下游消费者落 `pandora_trade.trade_audit`。存储决策见 [`decision-revisit-trade-storage.md`](../../../docs/design/decision-revisit-trade-storage.md)。

## 关键设计点 / 不变量

| 主题 | 约束 | 代码锚点 |
|---|---|---|
| 结算意图先落库 | `SELLER_CONFIRMED` 经 WATCH/EXEC 原子提交(线性化点)后才在锁外 `Settle`,不把不可回滚转移放进可能重跑的回调 | `ConfirmOrder` / `driveSettlement` |
| 结算幂等 | 幂等键 = `order_id`,委托 inventory 原子对转;重试 Confirm 幂等命中不重复扣转(不变量 §9.7) | `GrpcResourceLedger.Settle` |
| 结算围栏 | `SELLER_CONFIRMED` 下 Cancel / 过期 / 配额清理一律 fail-closed,只向 `COMPLETED` / `FAILED` 收敛 | `CancelOrder` / `expireIfStale` / `pruneDeadOrderSlots` |
| 恢复无需回滚 | 结算窗口崩溃 / 瞬时失败 → 停留 `SELLER_CONFIRMED`,任一方重试 Confirm 收敛 | `driveSettlement` |
| 写入侧总量上限 | 单玩家买 / 卖两侧订单数 ≤ `MaxOrdersPerPlayer`(Lua 原子预留,满则惰性清死成员重试一次)| `reserveSlotPruning` / `reserveOrderSlotScript` |
| 先建后引用 | 先写订单主体再预留反查索引,「索引指向不存在主体」≡ 真死成员,清理不误删 in-flight | `CreateOrder` |
| 鉴权下沉 | 玩家身份以 JWT ctx 为准,忽略请求体身份字段;非参与方拒 | `service/trade.go` / `callerID` |
| Noop 结算 fail-fast | 未接真实账本(`inventory_addr` 空)且未显式 `allow_noop_ledger=true` → 启动即退出,拒绝"成交不扣减"静默上线 | `cmd/trade/main.go:128` |

## 配置项(`internal/conf/conf.go`)

| 键(`trade.*`) | 默认 | 说明 |
|---|---|---|
| `order_ttl` | `10m` | 订单 Redis key 存活时长(应 > `order_expire`,给已结算 / 已取消订单留查询窗口) |
| `order_expire` | `5m` | 订单从创建到自动过期时长(惰性置 `EXPIRED`) |
| `optimistic_retry` | `3` | WATCH/MULTI/EXEC 乐观锁最大重试次数(耗尽 → `ErrTradeLockFailed`) |
| `max_items_per_order` | `20` | 单订单最大物品条目数(`items` + `buyer_items`) |
| `max_orders_per_player` | `200` | 单玩家买 / 卖两侧订单总数上限(不变量 §18 写入侧硬上限,超限 `ERR_TRADE_ORDER_LIMIT`)|
| `inventory_addr` | `""` | inventory 服务 gRPC 直连地址;配上 → 结算走真实 P2P 原子对转 |
| `allow_noop_ledger` | `false` | 显式允许退回 `NoopResourceLedger`(占位,结算总成功不真实扣转);生产必须 false |

端口默认值(非 `trade.*` 键,由 `Defaults()` 兜底):`server.grpc.addr = :20012`、`server.http.addr = :21012`。

> **账本选路**(`cmd/trade/main.go:128`):`inventory_addr` 非空 → `GrpcResourceLedger`(真实结算);否则 `allow_noop_ledger=true` → `NoopResourceLedger`(仅联调 / 单测);两者皆无 → **fail-fast 拒启**,防止漏配后以"成交不扣减"静默上线。

## 本地启动

```powershell
# 1. 基础设施(redis 6380 强依赖;kafka 9093 弱依赖,不通则审计静默 fail 继续)
pwsh tools/scripts/dev_up.ps1

# 2. 启 trade(dev 配置:allow_noop_ledger=true,结算走 Noop 占位不真实扣转)
go run ./services/economy/trade/cmd/trade -conf services/economy/trade/etc/trade-dev.yaml
```

> dev 配置 `allow_noop_ledger: true` 让结算走占位实现(成交不真实扣转背包 / 货币),便于无 inventory 时联调;
> 要跑真实结算,取消 `etc/trade-dev.yaml` 里 `inventory_addr` 注释(指向 inventory `:20015`)并起 inventory 服务。
> 生产必须接真实账本并置 `allow_noop_ledger=false`(否则 `main` fail-fast)。

## 关联文档

- [`go-services.md §2.9`](../../../docs/design/go-services.md) — trade 要约(RPC / 两阶段流程 / 关键不变量)
- [`decision-revisit-trade-storage.md`](../../../docs/design/decision-revisit-trade-storage.md) — 订单存 Redis / 结算腿幂等键 uk(player_id, order_id, leg) / 审计落库
- [`decision-revisit-trade-crossslot.md`](../../../docs/design/decision-revisit-trade-crossslot.md) — 跨 slot 结算与买卖双方跨分片对转
- [`decision-revisit-internal-service-auth.md`](../../../docs/design/decision-revisit-internal-service-auth.md) — 内网系统接口(inventory `SettlePlayerTrade`)`callerID==0` 信任模型
- [`decision-revisit-list-pagination.md`](../../../docs/design/decision-revisit-list-pagination.md) — `ListMyOrders` 游标分页上限
- [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md) — 协议排序 / 审计事实口径
- [`scale-cellular-20m.md`](../../../docs/design/scale-cellular-20m.md) §4.2/§4.4 — 确定性 region/cell 路由与跨分片 / 跨 region 结算落点
- [`infra.md`](../../../docs/design/infra.md) — 端口(20012 / 21012)/ topic / MySQL 表登记
