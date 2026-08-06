# decision-revisit:全服拍卖行 / 撮合引擎独立服务(auction)

> 2026-07-12 补充（覆盖本文早期 Redis 撮合与非持久 saga 描述）：撮合候选已改由 MySQL 按
> `item_config_id` 权威选择；订单先 PENDING 冻结，成交由同分片事务写持久意图，结算/释放和
> 多笔撮合续跑均由后台幂等补齐。Redis ZSET 退为旧版本兼容缓存。最终口径、迁移与剩余边界见
> `decision-revisit-auction-match-authority.md`；下文保留的历史阶段说明不得再作为实现依据。

> 触发:`trade` 现为双方已知的两阶段 P2P 托管交易;业务新增需求 **全服拍卖行 / 跨人撮合**
> (无界挂单 + 价格-时间优先撮合 + 资产转移幂等)。该模型与 `trade` 的状态机、争用、分片
> 维度均不同,且碰 `CLAUDE.md` §9 不变量 #7(资源扣减原子 + 补偿幂等键)、#2(成交只落库一次)、
> #9(kafka key = 业务实体保序),按 `AGENTS.md` §7 升级为 decision-revisit。
> 决策级别:服务级(新增 `auction`)+ 存储路线(MySQL ShardSet vs TiDB)。
> 用户已于 2026-06-19 确认"确实需要全服拍卖行 / 跨人撮合 + 幂等",本文档定方案供拍板,
> 实现由另一窗口(另一 AI / 人)按 §5 执行。

---

## 1. 旧问题(现状)

- [`services/economy/trade`](../../services/economy/trade) 是 **P2P 托管交易**:买卖双方已知 → 卖家挂托管
  → 买家确认 → saga 扣减(幂等键 = `order_id`)。订单各自独立 key,无共享热点,无撮合。
- 拍卖行需求与之根本不同:

  | 维度 | `trade`(P2P 托管,保留) | 拍卖行 / 撮合(新需求) |
  |---|---|---|
  | 模型 | 双方已知 → 托管 → 确认(状态机) | 全服订单簿 + 撮合(价格-时间优先) |
  | 参与者 | 有界(买卖两人) | 无界(全服挂单 / 吃单) |
  | 热点 | 每订单独立 key,无共享热点 | 热门道具同一市场 = 单点高争用 |
  | 一致性边界 | 单边 owner,saga 补偿 | 同一市场内撮合必须原子(不超卖 / 不重复成交) |
  | 分片维度 | `order_id` | `market_id`(道具品类 / 市场) |

- 若把撮合塞进 `trade`:状态机混杂、争用模型冲突、分片维度打架,违反 `CLAUDE.md` §5.8
  "新业务优先独立 proto message,不造并行 struct"的精神,且会污染已稳定的 trade 服务。

## 2. 关键事实:撮合是"按市场分片的单写者",不是好友图那种任意人强一致

好友图走 TiDB(`friend-distributed-scaling.md`)是因为 **任意人↔任意人双向强一致**(A 加 B 要
同事务建反向边),无法靠分片消除跨分片事务。**拍卖撮合不是这样**:

- 每个 `market_id`(道具品类)由 **唯一一个撮合分片独占串行处理**(交易所 / LMAX 单写者模型);
- 同一市场内“锁双方订单 + 预留成交 + 推进 filled/status”落在 **一个 auction 分片本地事务**；
  双方资产实际在 inventory 权威库，以该事务先持久化的 `match_id` 意图走幂等 saga，不能误称为跨库本地事务;
- 正是 §9 #9(kafka key = 业务实体保序)同款思路:`key = market_id`,同市场事件有序、单写者消费。

**结论**:分片维度选 `market_id`(而非 `player_id`)，auction 内订单推进不跨分片，
**普通 MySQL `mysqlx.ShardSet`(按 market_id 分片)即可**；跨 inventory 的资产副作用用持久意图+
幂等账本补偿，不伪装成分布式 ACID。

## 3. 新方案(推荐)

### 3.1 拆独立服务 `services/economy/auction`(与 trade 平级)

| 资源 | 取值 | 依据 |
|---|---|---|
| 服务名 | `auction`(economy 域) | 与 `trade` / `inventory` 平级 |
| gRPC / metrics 端口 | **20016 / 21016** | `infra.md` §6.2 economy 段空档(trade 20012、inventory 20015 之后) |
| proto 包 | `pandora/auction/v1/auction.proto` | `proto-design.md` §1 目录规范 |
| 错误码段 | **12000-12999** | `proto-design.md` §4(7000-7999=trade,11000+ 预留,取新段) |
| MySQL 库 | **`pandora_auction`** | `infra.md` §2.1 "按职能分库",撮合表与 trade 解耦 |
| Kafka topic | `pandora.auction.match`(成交)、`pandora.auction.audit`(审计 append-only) | `proto-design.md` §5 `pandora.<domain>.<event>` |

> ⚠️ 上述端口 / 错误码段 / 库名 / topic 需同步登记进 `infra.md` §2.1/§4/§6.2 与
> `proto-design.md` §4/§5,实现窗口落地时一并更新(不允许 ad-hoc,见 `infra.md` 总则)。

### 3.2 单写者分市场撮合

- 每个 `market_id` 路由到一个撮合 worker(分区一致性哈希,沿用 `pkg/kafkax/consistent.go` 思路);
- 挂单 / 出价 / 撤单正常路径仍以 **同市场单写者** 降低冲突；防超卖的最终边界是
  MySQL `SELECT ... FOR UPDATE`、条件状态迁移与成交意图唯一键，不能只依赖 Redis 锁租约;
- 撮合命中 → 生成 `match_id`(雪花 uint64)→ 走资产转移结算。

### 3.3 存储分层

| 层 | 介质 | key / 表 | 说明 |
|---|---|---|---|
| 旧版兼容缓存 | Redis ZSET | `pandora:auction:book:{<market_id>}:ask/bid` | best-effort 双写；新版本不从中选候选，失败不改变业务结果 |
| 权威订单/续跑 | MySQL `ShardSet`(按 `market_id` 分片) | `auction_orders` + PENDING/match_pending/release_pending | 精确 item 候选与崩溃恢复事实源 |
| 成交/结算意图 | MySQL 同分片 | `auction_matches` uniq(match_id), settlement_status | 与双方 filled/status 在一个事务提交；资产转移随后按 match_id 幂等完成 |
| 审计 | Kafka append-only | `pandora.auction.audit` | 风控 / 对账 |

> 通用字段(`created_at/updated_at/deleted_at/version`)按 `infra.md` §2.2;
> 雪花主键热点须 `AUTO_RANDOM` 打散(沿用 friend §8.2 经验)。

### 3.4 两层幂等(回应用户"也要幂等")

1. **提交幂等**:挂单 / 出价 / 撤单带客户端 `idempotency_key`,重试不重复挂单、不重复冻结资产
   —— 对应 §9 #7"补偿幂等键"。冻结(下架 / 锁金币)由结算原语保证。
2. **结算幂等**:撮合成交以 `match_id`(雪花 uint64)为幂等键,资产转移**只落一次**
   —— 对应 §9 #2(同 match_id 只落库一次)同款不变量。
3. **结算复用 trade 的 saga / `ResourceLedger` 原语**,不重造:auction 撮合出 `match` →
   调统一结算路径(扣冻结 + 入账 + 补偿),幂等键 = `match_id`。

### 3.5 ID 类型(`CLAUDE.md` §5.5/§5.6)

| 字段 | 类型 | 说明 |
|---|---|---|
| `order_id` / `match_id` | `uint64`(雪花) | 运行时业务实体 |
| `owner_id` / `bidder_id` | `uint64`(雪花) | 玩家 ID |
| `item_config_id` | `uint32` | 配置表道具 ID |
| `market_id` | `uint32`(配置:道具品类 / 市场) | 撮合分片维度;若运行时动态市场则改 `uint64` 并文档说明 |
| 状态 / 撮合原因 | proto enum(int32 语义) | 不因非负改 uint32 |

### 否决的备选

- **(A) 塞进 `trade` 服务**:状态机 / 争用 / 分片维度冲突,污染已稳定服务。**否决**(见 §1)。
- **(B) 撮合库走 TiDB**:撮合是单写者分市场、无跨分片事务,TiDB 的跨人强一致能力用不上,
  徒增运维面与雪花热点风险。**否决**(见 §2);留作"全服统一订单簿且必须跨市场强一致"的极限场景再议。
- **(C) 分片维度用 `player_id`**:会让"同一市场撮合"跨分片,反而制造跨分片事务。**否决**。
- **(D) 用 `macrozheng/mall` 等 Java 电商替代**:违反 `CLAUDE.md` §12(go 服务 headless、禁 GUI 库)
  与 proto / 雪花 / Kafka / k8s 契约,引入第二语言栈。**否决**;其订单状态机 / 防超卖思路可借鉴,
  实现留在 Go gRPC 体系。

## 4. 风险

| 风险 | 等级 | 缓解 |
|---|---|---|
| 热门市场单写者成为吞吐瓶颈 | 中 | 单写者只串行"撮合判定",资产转移异步幂等;必要时按价格档 / 子市场再分片 |
| Redis 兼容缓存与 MySQL 漂移 | 低 | 新版本完全不从缓存选单；写失败只告警，无需用同故障域 marker 重建正确性 |
| 冻结资产或成交副作用在崩溃后未完成 | 中 | PENDING/match_pending/settlement_status/release_pending 均持久化，后台按 order_id/match_id 幂等补齐 |
| `match_id` 重复结算 | 无 | §9 #2 幂等键,唯一约束 uniq(match_id) 兜底 |
| 雪花主键写热点 | 低 | `AUTO_RANDOM` 打散 |

## 5. 迁移成本 / 实现范围(给实现窗口)

**纯新增,不改 trade**:

1. `proto/pandora/auction/v1/auction.proto`:`AuctionService`(ListMarket / PlaceOrder / Bid /
   CancelOrder / ListMyOrders)+ message(`AuctionOrder` 客户端可见结构、`AuctionOrderStorageRecord`
   存储快照若有独有字段、`AuctionMatchEvent` 事件)。proto 改动 commit 标 `[proto]`(`CLAUDE.md` §4)。
2. `services/economy/auction/`:cmd + internal(biz / data / service / server / conf),Kratos,
   骨架对齐 trade 目录结构。data 层 `redis.UniversalClient` + `mysqlx.ShardSet`。
3. `deploy/mysql-init/NN-auction-tables.sql`:`pandora_auction`.{`auction_orders`,`auction_matches`}。
4. 文档登记:`infra.md`(§2.1 库、§4 topic、§6.2 端口 20016/21016)、`proto-design.md`(§4 错误码 12000、§5 topic)、
   `pandora-arch.md` §3 服务清单加一行 + §11 决策表追加 + `PROGRESS.md` 追加。
5. 结算复用 trade `ResourceLedger` / saga;若该原语在 trade 内部,需抽到 `pkg/` 或 economy 公共层供两服务共用(实现窗口评估,避免复制)。

**不变**:trade 服务、其 proto、其库表零改动。

## 6. 验收标准

1. `go build ./...` + `go test ./...` auction 模块 EXIT=0 全绿;不破坏 trade / 其他模块(workspace 编译通过)。
2. 撮合单元测试:同市场并发挂单 / 吃单,断言无超卖、价格-时间优先正确、`match_id` 唯一。
3. 幂等测试:重复 PlaceOrder(同 idempotency_key)不重复挂单;重复结算(同 match_id)资产只转一次。
4. Redis Cluster 兼容:订单簿操作均在 `{market_id}` 同 slot,无 `CROSSSLOT`(grep 确认无跨 slot 多键事务)。
5. 文档登记齐全(§5.4),无 ad-hoc 端口 / topic / 库名。

---

## 决策

- [x] 方向确认:用户 2026-06-19 确认需要"全服拍卖行 / 跨人撮合 + 幂等",**独立 `auction` 服务**、
  **MySQL ShardSet 按 market_id 分片(不上 TiDB)**、**两层幂等(idempotency_key + match_id)**。
- [ ] 实现细节(proto 字段 / 端口 / 库表 / 错误码段最终值)审定后由实现窗口落地,落地时同步更新 §5.4 各文档。
- [ ] 若实现中发现"必须跨市场强一致"场景(推翻 §2 单写者前提),停下重走 decision-revisit。

---

## 7. 实现增补:四项遗留局限补齐(2026-06-20)

W1 撮合骨架落地后遗留四项局限,本轮全部补齐(代码 + 单测;真实多依赖端到端联调留给环境窗口)。

### 7.1 挂单冻结资产(escrow)—— 限制#1

**问题**:原先只在成交时结算(settle-at-match),挂单到成交之间资产可被它处花掉,导致成交瞬间余额不足而失败。

**方案**:三段式 escrow,资产在挂单时即冻结,成交只从 escrow 消费(永不触碰活跃余额 → 不会因余额不足失败):

- **冻结**:`inventory.FreezeForOrder`(幂等键 `uk(player_id, order_id)`)。卖单冻 `quantity` 个道具、
  买单冻 `quantity*unit_price` 金币,写入 `auction_escrow` 表并从活跃余额扣减(同库本地事务,原子)。
  冻结失败(余额不足)→ `auction` 挂单直接置 `CANCELED`,不进簿、不撮合,返回 `ERR_AUCTION_INSUFFICIENT`。
- **成交**:`inventory.SettleAuctionMatch`(幂等键 `inventory_ledger uk(player_id, "auction:settle:<match_id>")`)
  从买卖双方 escrow 消费完成对转,按 `player_id` 升序锁行避免死锁。买单按出价冻结、按**成交价**(被动挂单价)
  消费,价差残留在 escrow。
- **退还**:`inventory.ReleaseEscrow`(幂等键:escrow 行 `status active→closed`)在撤单 / 过期 / 完全成交后
  退还残余(买单价差 + 未成交部分)到活跃余额。`auction` 在 `CancelOrder` / 完全成交 / 过期清扫三处调用。

### 7.2 跨实例 per-market 单写者锁 —— 限制#2

**问题**:进程内 striped lock 只在单实例内串行;多实例部署时同一 market 可能落到不同实例并发撮合 → 订单簿与权威库被并发改 → 超卖。

**最终方案**:`MarketLocker` 接口(`biz`)+ Redis 单写者 token(`pkg/redislock`,TTL ≤ 30s,不变量 §10)，
新二进制始终启用并按 `ttl/3` 续租，失租 fail-stop。它用于正常路径串行和降低 MySQL 冲突，
但不是 fencing token；Redis key 被淘汰、主从切换或长暂停时仍可能短暂双持。因此权威写必须同时由
MySQL 行锁/条件更新/唯一键保证并发安全。一致性哈希路由继续用于降低锁竞争，不作为正确性前提。

### 7.3 撮合层主动跳过自撮合 —— 限制#3

**问题**:inventory 结算拒 `seller==buyer`,但撮合层应提前跳过,避免自成交浪费一次结算往返。

**最终方案**:MySQL 候选查询直接要求精确 `item_config_id` 且 `owner_id <> incoming.owner_id`，
不再临时移出/放回 Redis 条目；因此进程退出、缓存故障和固定自有/异物品前缀都不会丢单或饥饿。

### 7.4 过期清扫 sweeper —— 限制#1 补偿

`OrderTTLSeconds > 0` 时后台 ticker 周期扫 `ListExpirableOrders`(创建超 TTL 仍 OPEN/PARTIAL),
持 market 锁逐单置 `EXPIRED`、移出簿、退还 escrow。保证挂单冻结的资产不会因长期挂单永久锁死。

### 7.5 真依赖端到端联调 —— 限制#4(待环境窗口)

当前单测用内存 fake + miniredis 覆盖 escrow 冻结 / 成交 / 退还 / 价差返还 / 过期 / 自撮合 / 跨实例锁；
`auction_repo_mysql_test.go` 已在真实 MySQL 8.4 覆盖 PENDING 隔离、事务 Reserve 并发不超卖、结算前
禁止释放与旧二进制默认列兼容。仍未完成的是 auction↔inventory 真 gRPC + Kafka 的整链路故障注入，
它不能被上述 repo 集成测试冒充。

### 7.6 涉及文件

| 层 | 文件 |
|---|---|
| proto | `inventory.proto`(FreezeForOrder / ReleaseEscrow / SettleAuctionMatch+order_id / EscrowSide)、`errcode.proto`(12006) |
| inventory | `data/inventory_repo.go`(escrow 三方法 + consume 助手)、`biz/inventory.go`、`service/inventory.go`、`08-inventory-tables.sql`(`auction_escrow`) |
| auction | `biz/auction.go`(SettlementLedger 扩 Freeze/Release、MarketLocker、submit 冻结、match 释放、CancelOrder/expire 释放、ExpireDueOrders)、`data/settlement_client.go`、`data/market_locker.go`、`data/auction_repo.go`(ListExpirableOrders)、`conf/conf.go`、`cmd/auction/main.go`、`etc/auction-dev.yaml`、`09-auction-tables.sql`(`idx_status_created`) |
| 测试 | `auction_test.go`(冻结失败 / 撤单释放 / 完全成交释放 / 过期清扫)、`inventory_test.go`(冻结充足性 / 幂等 / 退还 / 价差返还) |
