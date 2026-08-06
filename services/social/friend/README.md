# friend

> 社交好友服务:好友请求 / 接受 / 拒绝 / 删除、黑名单、好友推荐,好友图落 `pandora_social`
> (MySQL / TiDB 强依赖);好友请求与接受经 kafka `pandora.friend.event` → push 推给接收方
> (弱依赖),`ListFriends` 经 player_locator 填在线状态(弱依赖)。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见
> `docs/design` 的 [`friend-distributed-scaling.md`](../../../docs/design/friend-distributed-scaling.md)(全区全服
> 千万级 `AcceptRequest` 拆解 / 软上限取舍)、[`go-services.md §2.4`](../../../docs/design/go-services.md)(friend
> 要约)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:好友关系的增删查(请求 / 接受 / 拒绝 / 删除)、黑名单(拉黑 / 取消 / 列表)、好友推荐。
- **权威态**:好友图 / 请求 / 黑名单全在 **MySQL / TiDB `pandora_social` 库**(本地 ACID 事务保原子)。
  进程内**无缓存权威态**——服务无状态、可水平扩展、任意副本随时可杀(不变量 §16)。
- **状态权属**:friend 只是好友图的权威;**在线状态不是本服务的**——`is_online` / `last_seen_ms` 经
  player_locator 查(弱依赖,查不到按离线);**昵称不是本服务的**——`nickname` 一律留空,由客户端按
  `player_id` 向 player 服务解析(最小数据单位,不跨库 join)。
- **不做的事**:不推"被拒绝"通知(`RejectFriend` 静默,业界惯例);不自动恢复被拉黑期间的好友关系
  (`Unblock` 后需重新加);不落在线状态 / 昵称影子表;当前不做分片(单 MySQL 事务实现,分片拆解见
  关联文档,本服务只留 nil-safe 分片落点观测)。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20004` | 客户端 RPC(经 Envoy jwt_authn)|
| HTTP | `:21004` | 仅 `/metrics`(`friend.proto` 无 `google.api.http` 注解,无 RESTful RPC)|

端口从 `internal/conf/conf.go` 的 `Defaults()` 取(`Server.Grpc.Addr` / `Server.Http.Addr`);登记见
[`infra.md §6`](../../../docs/design/infra.md)。

## 对外接口

代码入口:`internal/service/friend.go`(实现 `friendv1.FriendServiceServer`)。**全部 RPC 都是客户端面**:
Envoy jwt_authn 在路由层强制 JWT,`pmw.AuthOptional` 把 `x-pandora-player-id` header 注入 ctx,service 层
一律用 `callerID(ctx)` 取 `player_id`,**忽略请求体里的 `player_id` 字段**(R5 防伪造他人身份);
`player_id=0` → `ERR_UNAUTHORIZED` 兜底。**friend 无内部专用 RPC**(无 `callerID==0` 内网直连路径,
无内部 HMAC 信任域)。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `AddFriend(target_player_id)` | 客户端 | 发起好友请求,返回 `request_id`;不能加自己 / 互相拉黑 / 已是好友 | JWT `player_id`(requester)|
| `AcceptFriend(request_id)` | 客户端 | 接受请求,事务内建双向好友边;只有请求 target 本人可接受 | JWT `player_id`(=target)|
| `RejectFriend(request_id)` | 客户端 | 拒绝请求(置 rejected,不推 requester);requester 之后可再发起 | JWT `player_id`(=target)|
| `ListFriendRequests()` | 客户端 | 列「发给本人且仍 pending」的请求(离线补拉) | JWT `player_id` |
| `ListFriends()` | 客户端 | 列好友,经 locator 填在线状态 | JWT `player_id` |
| `RemoveFriend(target_player_id)` | 客户端 | 删双向好友边(幂等);不动黑名单 | JWT `player_id` |
| `Block(target_player_id)` | 客户端 | 拉黑:写黑名单 + 删好友边 + 取消两人间 pending 请求 | JWT `player_id` |
| `Unblock(target_player_id)` | 客户端 | 取消拉黑(幂等);不自动恢复好友关系 | JWT `player_id` |
| `ListBlocks()` | 客户端 | 列本人拉黑的人 | JWT `player_id` |
| `RecommendFriends(limit, exclude_player_ids)` | 客户端 | 推荐好友(策略链 mutual→random),客户端回传 exclude 刷新 | JWT `player_id` |

> **会话现行性门**(`pmw.SessionCurrent`,R5 复审 P0-1,INC-20260722-004):客户端面请求的 jti 必须是 login
> 会话权威(`pandora:sess`)**当前一代**——顶号 / 登出后旧 JWT 在 exp 前也不得继续按 `player_id` 定向操作。
> dev 宽松档 `session_gate.require=false`,`-Prod` 产物机械置 `true`(漏配端点拒启)。
>
> **推送原则 2**([`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md)):好友请求推给
> **target**、接受通知推给 **requester**,均不发给操作者本人;kafka key = `to_player_id`(同接收方事件保序)。

## 目录结构(Kratos 标准分层,对齐 login / matchmaker)

```
cmd/friend/main.go              启动入口(MySQL 强依赖 + snowflake + kafka/locator 弱依赖 + sweep goroutine 装配)
etc/friend-dev.yaml             开发期配置(单 MySQL,session_gate.require=false)
etc/friend-dev-tidb.yaml        TiDB 开发配置(守卫行方案的目标存储)
etc/friend-prod.yaml.example    生产配置样例
internal/
  conf/conf.go                  配置结构(FriendConf + config.Base)与 Defaults()
  service/
    friend.go                   RPC 入口(JWT 身份下沉 callerID,proto ↔ biz 互转,errcode → proto enum)
  biz/
    friend.go                   FriendUsecase 核心(AddFriend/AcceptFriend/.../RecommendFriends)
    recommend.go                推荐策略链(RecommendStrategy:mutual 熟人 / random 兜底,可插拔)
    friend_sharding.go          好友边分片落点观测 + 幂等键口径(nil-safe,单 Cell 不启)
    sweep.go                    终态请求 / pair 守卫行保留期清理(RunRequestSweep ticker)
  data/
    friend_repo.go              MySQL 好友图仓储(friendships / friend_requests / blocks + 守卫行)
    locator_client.go           player_locator gRPC client(BatchOnline 批量查在线,弱依赖)
  server/
    grpc.go                     gRPC server 注册(AuthOptional + SessionCurrent 中间件)
    http.go                     HTTP server(仅 /metrics)
```

## 核心调用链

所有客户端 RPC 都是**同步落库型**(非"已受理型 saga"):service 取 JWT `player_id` → biz 预检(fail-fast)
→ data 层单事务权威校验 + 写入 → 弱依赖 push(失败不影响主流程)。

### 1. AddFriend —— 发起好友请求

`service/friend.go` `AddFriend` 取 `callerID(ctx)` 作 requester,`target_player_id` 来自请求体 → `biz/friend.go`
`AddFriend`(`friend.go:89`):

```
AddFriend(requesterID=JWT, targetID, newRequestID=snowflake)
├─ 自检:requester==target → ErrInvalidArg
├─ repo.IsBlocked        fail-fast 预检(非权威,权威复核在事务内 pair 守卫下)
├─ repo.AreFriends       fail-fast 预检(已是好友 → ErrFriendAlreadyAdded)
├─ repo.CountFriends     fail-fast 预检(requester 好友已满 → ErrFriendLimit,非权威)
├─ repo.CreateRequest    ★ 事务内权威:pair 守卫 → block/好友复核 → 收件箱上限 → 写 pending
└─ pushEvent(target, REQUEST_RECEIVED)   弱依赖,pusher==nil 或失败只 warn
```

`repo.CreateRequest`(`friend_repo.go:183`)是权威提交点:`BeginTx` → `acquirePairGuard`(关系对守卫行,
与同对 Block/Accept 串行化)→ block 探针 `FOR UPDATE` → friendship 探针 `FOR UPDATE` → 锁请求行
`FOR UPDATE` 按历史分流:无历史 → `checkIncomingLimit`(target 收件箱上限)+ INSERT pending;已有 pending →
复用返回;rejected/expired/accepted → 换**新 request_id** 重置 pending 并刷新 `created_at`(旧 ID 失效,迟到
的旧 Accept 自然查无此请求)。

### 2. AcceptFriend —— 事务内建双向边(★好友域最复杂路径)

`biz/friend.go` `AcceptFriend`(`friend.go:146`):`GetRequest` 预检(不存在 / 非发给本人 / 非 pending → 直接
`ErrFriendNotFound`,免开事务)→ `repo.AcceptRequest` → 分片落点观测 → 推 requester。

`repo.AcceptRequest`(`friend_repo.go:297`)一个事务内完成全部权威校验与写入,返回 `accepted bool`:

```
AcceptRequest(requestID, accepterID=JWT, maxFriends)
├─ 步骤0 预读请求行(不加锁)只为拿 pair 身份 requester/target
├─ 步骤1 acquirePairGuard(requester,target)       与同对 Block/AddFriend 全序串行化
├─ 步骤2 acquirePlayerGuard(lo)→(hi)               player 守卫恒升序(防死锁)
├─ 步骤3 锁请求行 FOR UPDATE 复核 target==accepter && status==pending
│         (预读与取锁间可能被并发处理 → accepted=false,不推送)
├─ 步骤4 block 校验(FOR UPDATE 当前读)            双向任一拉黑 → ErrFriendBlocked
├─ 步骤5 好友上限 COUNT(FOR UPDATE)对 requester/target 双方   超限 → ErrFriendLimit
├─ UPDATE status=accepted
├─ INSERT IGNORE 双向好友边(requester→target,target→requester)
├─ 步骤6 反向 pending 收敛 accepted(A→B 与 B→A 各自 pending 时)
└─ Commit → return accepted=true
```

**锁序纪律**:`pair 守卫 → player 守卫(升序) → 业务行`,单事务至多一个 pair 守卫,消除死锁环。
**为什么读侧全用 `FOR UPDATE`**:步骤 0 的普通预读已把 InnoDB RR 一致读快照固定在守卫获取之前,后续普通
`SELECT` 会读陈旧快照(看不到守卫等待期间提交的 Block / 建边);`FOR UPDATE` 是当前读,MySQL InnoDB 与
TiDB 悲观事务都读最新已提交(R9 复审 P1)。`accepted=false` 时 biz **绝不推送 REQUEST_ACCEPTED**(避免假成功)。

### 3. Block / RejectFriend —— 同一 pair 守卫下串行

- `repo.Block`(`friend_repo.go:507`):`acquirePairGuard` → 黑名单上限校验(新拉黑才 `acquirePlayerGuard` +
  `COUNT FOR UPDATE`,超限 `ErrFriendBlockLimit`)→ `INSERT IGNORE` 黑名单 → 删双向好友边 → 取消两人间任一
  方向 pending 请求。与 Accept/AddFriend 同 pair 串行,消除「既好友又拉黑」交错。
- `repo.RejectRequest`(`friend_repo.go:422`):锁请求行 `FOR UPDATE` → 校验 target 本人 → 确认仍 pending →
  置 rejected;并发已处理 → `rejected=false`,biz 报找不到。

### 4. ListFriends —— 好友图 + 在线状态(弱依赖)

`biz/friend.go` `ListFriends`(`friend.go:242`)→ `repo.ListFriends`(SQL LIMIT 1000 兜底)拿好友 id →
`u.online.BatchOnline(ids)`(`locator_client.go:57`,一次 `BatchGetLocation` 批量查)填 `is_online` /
`last_seen_ms`。`online==nil`(locator addr 空)或整批失败 → 全部按离线,列表照常返回。`nickname` 留空。

### 5. RecommendFriends —— 策略链召回

`biz/friend.go` `RecommendFriends`(`friend.go:340`):`exclude` 恒带上自己 → 按 `strategies`(conf
`recommend_strategies`,默认 `mutual→random`,见 `recommend.go`)依次 `Candidates` 召回,每选一批追加进
exclude 避免跨策略重复,凑够 `limit`(硬上限 20)即止 → BatchOnline 填在线。`mutual` = 好友的好友按共同好友
数降序(`RecommendByMutual`),`random` = 好友图索引锚点扫兜底(`RecommendRandom`,`mutual` 恒 0)。两条 SQL
均排除:自己 / 已是好友 / 双向拉黑 / 双向 pending / exclude。服务端无状态,刷新靠客户端回传 exclude。

### 6. 后台保留期清理 RunRequestSweep(§9.24)

`main.go` `go uc.RunRequestSweep(sweepCtx)`(`sweep.go:35`)每 `SweepInterval`(默认 5m)跑一轮
`SweepTerminalRequests`:`DeleteTerminalRequestsBefore`(终态好友请求超 `RequestRetentionDays`,默认 90 天,
pending 永不清)+ `DeletePairGuardsBefore`(pair 守卫行超 `PairGuardRetentionDays`,默认 30 天)。**多副本各自跑,
无 leader 选举**——`DELETE ... LIMIT` 幂等,并发只多花空批(对齐 mail sweep)。

## 存储布局

**MySQL / TiDB `pandora_social` 库**(`deploy/mysql-init/06-social-tables.sql` / `tools/migrate/migrations/pandora_social`),
三张业务表都是结构化列直接映射(非 proto blob):

| 表 | 主键 / 唯一键 | 用途 |
|---|---|---|
| `friendships` | `(player_id, friend_id)` | 双向好友边(每对好友落两行,便于 `ListFriends` 单向查)|
| `friend_requests` | PK `request_id`(snowflake);uk `(requester_id, target_id)` | 好友请求;`status` 1=pending/2=accepted/3=rejected/4=expired |
| `blocks` | uk `(player_id, blocked_id)` | 黑名单 |
| `friend_player_guards` | PK `player_id` | 单玩家限额域写守卫行(锁载体,无业务数据)|
| `friend_pair_guards` | uk `(lo_id, hi_id)` | 关系对守卫行(双向规范化到 lo/hi,同对操作串行化载体)|

- **Redis 只读**:`node.redis_client` 指向的共享 Redis 仅供 `pmw.SessionCurrent` 只读 login 会话权威
  `pandora:sess`,friend **不在 Redis 存任何好友态**。
- **启动期 schema 检查**(`main.go`,R8 收口):`mysqlx.CheckTables` 断言五张表(含后补的两张守卫行表)
  存在,缺表 fail-fast 并指向迁移 SQL——避免既有库未重放守卫行迁移时,好友操作在首条守卫 INSERT 才炸。

## 并发不变量与守卫行锁序

好友域是**跨玩家关系写**,且目标存储 TiDB 悲观事务**没有 gap / next-key 锁**——原
`COUNT(*) ... FOR UPDATE` 只锁存在行,挡不住并发 INSERT 幻读。用**守卫行悲观锁**替代范围锁(R5 复审
P1-2/3/4,R9 复审 P1):

| 不变量 | 约束 | 代码锚点 |
|---|---|---|
| 上限不可穿透 | `max_friends` / `max_incoming_requests` / `max_blocks` 的 COUNT 必须在守卫锁内做当前读(TiDB 无 gap 锁)| `acquirePlayerGuard` / `checkIncomingLimit` |
| 同对操作全序 | Accept / Block / AddFriend 对同一对玩家先取 pair 守卫,消除「既好友又拉黑」「已好友+pending」交错 | `acquirePairGuard` |
| 锁序无环 | `pair 守卫 → player 守卫(升序) → 业务行`;单事务至多一个 pair 守卫 | `AcceptRequest` / `Block` 注释 |
| 读侧防陈旧快照 | 守卫锁内的权威判定读一律 `FOR UPDATE`(当前读),不用被 RR 快照固定的普通 SELECT | `friend_repo.go` block/COUNT 探针 |
| 接受不假成功 | `AcceptRequest` 返回 `accepted=false`(并发被处理)时,biz 不推送、不报成功 | `AcceptFriend` / `AcceptRequest` |
| 再次申请换新 ID | rejected/expired 复用旧行时换新 `request_id` + 刷新 `created_at`,旧 ID 失效 | `CreateRequest` |
| 列表写入 / 读取双上限 | 写入侧总量上限(守卫锁内校验)+ 读取侧 `SQL LIMIT 1000` 兜底(不变量 §9.18)| `listReadHardLimit` |

## 弱依赖与降级

| 依赖 | 强 / 弱 | 缺失时行为 |
|---|---|---|
| MySQL / TiDB `pandora_social` | **强** | DSN 空 → 启动失败;好友图落库不可降级 |
| kafka `pandora.friend.event` | 弱 | broker 不通 → `pusher=nil`,请求 / 接受通知静默丢弃,主流程照常成功 |
| player_locator | 弱 | `locator_addr` 空或整批失败 → `is_online` 全 false,列表照常返回 |
| Redis `pandora:sess`(会话门)| 视 `session_gate.require` | dev `false` 宽松;prod `true` 漏配拒启 |
| cellroute Router | 弱(观测)| nil(单 Cell / dev)→ 不打好友边分片落点日志,建边行为不变 |

## 配置项(`internal/conf/conf.go`)

`friend.*` 私有键(`Defaults()` 填默认):

| 键(`friend.*`)| 默认 | 说明 |
|---|---|---|
| `max_friends` | `200` | 单玩家好友上限(AcceptFriend 事务内对双方原子校验)|
| `max_incoming_requests` | `200` | 单玩家「收到的 pending 好友申请」上限(CreateRequest 校验 target 收件箱,不变量 §9.18)|
| `max_blocks` | `200` | 单玩家黑名单上限(Block 事务内校验)|
| `locator_addr` | `""` | player_locator gRPC 地址;空 → 在线状态全离线(弱依赖)|
| `recommend_limit` | `10` | 单次推荐好友数;硬上限 `20`,超界收敛到 20 |
| `recommend_strategies` | `["mutual","random"]` | 推荐策略链,按序召回直到凑够 limit |
| `request_retention_days` | `90` | 终态好友请求保留天数(pending 永不清,§9.24)|
| `sweep_interval` | `5m` | 保留期清理轮询间隔(多副本各自跑,无锁)|
| `sweep_batch` | `500` | 每轮清理行数上限(`DELETE ... LIMIT`)|
| `pair_guard_retention_days` | `30` | 关系对守卫行保留天数(R9 复审 P1,守卫行随社交图 O(n²) 累积须有界)|

通用 `config.Base` 键(见各 dev yaml):`server.grpc.addr`(默认 `:20004`)/ `server.http.addr`(默认 `:21004`)、
`node.node_id`、`node.mysql_client.dsn`(强依赖,必填)、`node.redis_client`(会话门只读)、
`kafka.brokers` / `group_id`、`session_gate.require`、`snowflake.node_id_source`(`static` 默认 / `etcd` 多副本抢占)。

## 本地启动

```powershell
# 1. 基础设施(MySQL pandora_social 库 + redis 会话门;起 push/kafka + player_locator 后可跑全链)
pwsh tools/scripts/dev_up.ps1

# 2. 启 friend(单 MySQL dev 配置)
go run ./services/social/friend/cmd/friend -conf services/social/friend/etc/friend-dev.yaml
```

> 目标存储切 TiDB 时用 `etc/friend-dev-tidb.yaml`(守卫行方案正是为 TiDB 无 gap 锁设计)。
> kafka / locator 未起时 friend 仍可启动:好友请求 / 接受落库正常,只是推送与在线状态降级。

## 关联文档

- [`go-services.md §2.4`](../../../docs/design/go-services.md) — friend 要约(RPC / 实现说明 / 单 MySQL 事务决策)
- [`friend-distributed-scaling.md`](../../../docs/design/friend-distributed-scaling.md) — 全区全服千万级 `AcceptRequest`
  拆解(request 单点 CAS + Kafka 异步幂等建边 + 软上限)、TiDB 跨节点强一致、`BatchGetPresence`
- [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md) — 推送原则 2(不发给操作者本人)
- [`decision-revisit-list-pagination.md`](../../../docs/design/decision-revisit-list-pagination.md) — 客户端可写列表的写入 / 读取双上限(不变量 §9.18)
- [`scale-cellular-20m.md`](../../../docs/design/scale-cellular-20m.md) §4.2 / §4.4 — 确定性 region/cell 路由与跨 region 好友边最小通道(分片落点观测依据)
- [`session-generation-rollout.md`](../../../docs/design/session-generation-rollout.md) — 会话现行性门(顶号后旧 JWT 失效)
- [`infra.md`](../../../docs/design/infra.md) — 服务端口 / kafka topic 登记
