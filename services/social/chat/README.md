# chat

> 聊天服务:世界 / 队伍 / 私聊 / 公会 / 临时群五频道消息。服务端做频道校验 + 内容长度 +
> 敏感词屏蔽,私聊落 MySQL(唯一有历史的频道),五频道经 kafka `pandora.chat.{world,team,private,guild,group}`
> → push 推送(弱依赖);队伍 / 公会 / 群成员经对应服务 gRPC 现查解析(弱依赖)。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`
> 的 [`decision-revisit-chat-group.md`](../../../docs/design/decision-revisit-chat-group.md)(补公会 / 临时群频道);
> 跨服务要约见 [`go-services.md §2.5`](../../../docs/design/go-services.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:收一条 `SendMessage` → 频道校验 + 内容清洗 → 按频道解析收件人 → kafka 扇出给 push;
  私聊额外落库支持离线 `PullHistory`。
- **权威态**:只有**私聊历史**是权威持久态(MySQL `pandora_social.chat_private_messages`)。
  世界 / 队伍 / 公会 / 群是**即时频道**,不落聊天历史,离线不补发(用户确认 2026-06-27)。
- **成员名单不自持**:队伍 / 公会 / 群成员**每次现查**对应服务(team / guild / group gRPC),chat 不缓存、
  不复制社交图(对齐不变量 §22 状态优先查权威)。
- **不做的事**:不维护在线连接(那是 push 的活)、不算敏感词以外的风控(仅最小化整词屏蔽,真正风控由独立
  服务后续接管)、不做已读回执 / 撤回 / 会话列表。
- **依赖分级**:MySQL **强依赖**(私聊落库不可降级,失败让客户端重试);kafka / team / guild / group 全
  **弱依赖**(未配置或不可达时按频道降级,见「频道与降级矩阵」)。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20005` | 客户端 RPC(经 Envoy)|
| HTTP | `:21005` | 仅 `/metrics`(`chat.proto` 无 `google.api.http` 注解,无 RESTful RPC)|

端口来自 `internal/conf/conf.go` 的 `Defaults()`(`Server.Grpc.Addr` / `Server.Http.Addr` 缺省时兜底)。

## 对外接口

代码入口:`internal/service/chat.go`(gRPC service 层,从 JWT ctx 取 `player_id` 防伪造,再转 biz)。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `SendMessage(channel, target_id, content)` | 客户端 | 发一条消息;返回 `message_id`。`sender_id` **强制取 JWT**,忽略请求体 | JWT `player_id`(`=0` → `ERR_UNAUTHORIZED`)|
| `PullHistory(channel, peer_id, limit, before_ms)` | 客户端 | 拉私聊历史(仅 `PRIVATE` 有;其余频道返空)。`player_id` **强制取 JWT** | JWT `player_id`(`=0` → `ERR_UNAUTHORIZED`)|

> **无内部专用 RPC**:chat 当前只暴露这两个客户端面 RPC,无 matchmaker `ReleaseMatch` 那类
> `callerID==0` 内网直连接口。两个 RPC 的身份都以 **JWT ctx 的 `player_id`** 为准
> (`callerID`,`service/chat.go:72`):`SendMessage` 的 `sender_id`、`PullHistory` 的 `player_id`
> 一律用 ctx 覆盖请求体对应字段(R5,防伪造他人身份 / 拉别人私聊)。
>
> **鉴权三道**(`internal/server/grpc.go`):Envoy `jwt_authn` 路由层 require JWT → `pmw.AuthOptional()`
> 从 Envoy 注入的 `x-pandora-player-id` header 读 `player_id` 注入 ctx → `pmw.SessionCurrent(sessGate, require)`
> 校验请求 jti == login 会话权威当前一代(顶号后旧 JWT 在 exp 前立即失效,R5 复审 P0-1 / INC-20260722-004)
> → service 层 `callerID==0` 兜底拦截。

## 目录结构(Kratos 标准分层,对齐 matchmaker / login)

```
cmd/chat/main.go               启动入口(mysql + snowflake + kafka 五 producer + team/guild/group client + sweep + session gate 装配)
etc/chat-dev.yaml              开发期配置(MySQL 强依赖 + kafka/team/guild 弱依赖)
etc/chat-dev-tidb.yaml         私聊历史迁 social TiDB 的变体(仅 DSN / collation 不同)
etc/chat-prod.yaml.example     生产配置样例
internal/
  conf/conf.go                 配置结构(ChatConf:内容上限 / 历史上限 / team&guild addr / 敏感词 / 保留期清理)
  service/
    chat.go                    RPC 入口(实现 chatv1.ChatServiceServer,JWT 身份下沉 + errcode→proto 映射)
  biz/
    chat.go                    ChatUsecase 核心(SendMessage 频道分流 / 五频道 send* / PullHistory / 敏感词)
    chat_routing.go            私聊跨 region 投递落点观测(nil-safe,单 Cell 不启用)
    sweep.go                   私聊历史保留期清理循环(§9.24)
  data/
    chat_repo.go               MySQLPrivateRepo(chat_private_messages 落库 / 查询 / 批删)
    team_reader.go             team 服务 gRPC client(GetTeam → 队伍成员)
    guild_reader.go            guild 服务 gRPC client(ListMembers → 公会成员)
    group_reader.go            group 服务 gRPC client(ListGroupMembers → 临时群成员,与 guild 同进程)
  server/                      grpc(AuthOptional + SessionCurrent)/ http(仅 /metrics)server 注册
```

## 核心调用链

### 1. SendMessage —— 频道校验 → 清洗 → 按频道分流

`service/chat.go:42` 取 JWT `senderID`(`=0` 直接 `ERR_UNAUTHORIZED`),预生成 `message_id`
(snowflake),转 `biz.ChatUsecase.SendMessage`(`internal/biz/chat.go:96`)。biz 里的公共前置:

```
SendMessage(senderID, channel, targetID, content, newMessageID)
├─ 频道白名单     只允许 WORLD / TEAM / PRIVATE / GUILD / GROUP;SYSTEM/UNSPECIFIED → ErrChatChannelInvalid  (chat.go:109)
├─ 内容校验       TrimSpace 后非空;utf8 rune 数 ≤ MaxContentLen(默认 256)否则 ErrChatMessageTooLong  (chat.go:120)
├─ maskSensitive  命中敏感词整词替换等长 *(列表空则不过滤)                                                (chat.go:128 → :373)
├─ 组 ChatMessage  SenderNickname 留空(客户端按 sender_id 解析,最小数据单位 §5.8)                        (chat.go:130)
└─ 按 channel 分流 → sendPrivate / sendTeam / sendGuild / sendGroup / sendWorld                            (chat.go:140)
```

### 2. 五频道各自的收件人解析 + 扇出

| 频道 | 函数 | 收件人来源 | 落库 | 推送 key | 推送原则 |
|---|---|---|---|---|---|
| PRIVATE | `sendPrivate`(`chat.go:155`)| `target_id`(点对点)| ✅ MySQL | 收件方 `player_id` | 只发收件方(原则 2)|
| TEAM | `sendTeam`(`chat.go:185`)| team gRPC 现查成员 | ❌ | 逐成员 `player_id` | 排除发送者(原则 2)|
| GUILD | `sendGuild`(`chat.go:236`)| guild gRPC 现查成员 | ❌ | 逐成员 `player_id` | 排除发送者(原则 2)|
| GROUP | `sendGroup`(`chat.go:285`)| group gRPC 现查成员 | ❌ | 逐成员 `player_id` | 排除发送者(原则 2)|
| WORLD | `sendWorld`(`chat.go:333`)| 全服广播 | ❌ | key 空 | 广播(原则 2 **例外**)|

- **PRIVATE**(`sendPrivate`):校验 `target_id != 0` 且 `!= sender`;**先 `SavePrivate` 落库(强依赖,失败整条失败让客户端重试)**,
  再 `PushPrivate` 推给收件方(弱依赖,失败只 warn——消息已落库,对方上线 `PullHistory` 兜底);末尾 `logPrivateRouting`
  打私聊跨 region 落点观测(见下节 4)。
- **TEAM / GUILD / GROUP**(`sendTeam` / `sendGuild` / `sendGroup`):三者结构同构——
  1. `target_id` 即 `team_id` / `guild_id` / `group_id`,`=0` → `ErrInvalidArg`;
  2. reader 或 pusher **未配置**(nil)→ 不报错、记一条 `*_degraded` warn、返回 `message_id`(客户端本地回显);
  3. reader **配置了但现查失败**(`err != nil`)→ **诚实报错 `ErrUnavailable` 让客户端重试**(不能假成功:成员解析不了
     则无人收到 + 成员校验被跳过);查到但 `!ok`(实体不存在)→ `ErrChatChannelInvalid`;
  4. **发送者必须在目标队伍 / 公会 / 群内**,否则 `ErrChatChannelInvalid`;
  5. 逐成员 `Push*`,**跳过发送者本人**(原则 2:客户端本地回显己方消息),单个 push 失败只 warn 不中断其余成员。
- **WORLD**(`sendWorld`):`ChatPushEvent{ToPlayerId: 0}`,`PushWorld` 用**空 key** 发 kafka,由 push 服务 `Broadcast`
  路由给全体(原则 2 例外:发送者也会收到)。

### 3. kafka producer 适配(main 侧)

biz 只依赖 `ChatPusher` 接口(`chat.go:35`);`cmd/chat/main.go:184` 的 `chatPusher` 把五个方法适配到五个
`kafkax.KeyOrderedProducer`(topic 常量见 `pkg/kafkax/topics.go`):

```
PushPrivate/Team/Guild/Group → key = strconv(收件方 player_id)  → 同接收方同 partition 保序(不变量 §9)
PushWorld                     → key = ""                          → push 服务 Broadcast
```

五 producer **任一初始化失败则整体降级**(关闭已建的、`pusher=nil`,聊天推送静默 fail,私聊仍落库);
`cfg.Kafka.Brokers` 为空时直接不建 producer(`main.go:98`)。

### 4. 私聊跨 region 投递落点观测(nil-safe,单 Cell 不启用)

`internal/biz/chat_routing.go` 是**纯观测**逻辑,不改投递路径:`main.go:137` 经 `etcdtable.WireRouter` 注入
`cellroute.Router`(`SetCellRouter`,`chat.go:90`)后,`sendPrivate` 末尾 `logPrivateRouting`(`chat_routing.go:70`)
用 router 把发件 / 收件方解析到各自 owner region,`Debugw` 打「本条私聊是否跨 region、桥 key = 接收方 player_id」。
`router == nil`(单 Cell / dev / 阶段 1~2)→ 不打日志,行为与历史一致。真正的跨 region Kafka 桥 / 区域总线拆分属
基础设施,由 Codex / 人接(`scale-cellular-20m.md §4.4`)。

### 5. PullHistory —— 仅私聊有历史

`service/chat.go:56` 取 JWT `playerID` → `biz.PullHistory`(`chat.go:347`):

```
PullHistory(playerID, channel, peerID, limit, beforeMs)
├─ channel != PRIVATE → 返回 nil(即时频道无持久化历史)          (chat.go:358)
├─ peerID == 0        → ErrInvalidArg                              (chat.go:362)
├─ limit 钳到 [1, HistoryLimit](默认 50,读取侧上限 §9.18)       (chat.go:365)
└─ repo.ListPrivate → 双向(sender/receiver 互换)+ before_ms 游标 + send_time_ms DESC + SQL LIMIT  (data/chat_repo.go:54)
```

### 6. 私聊历史保留期清理(后台循环)

`main.go:149` `go uc.RunHistorySweep(sweepCtx)` 起一条后台 ticker(`sweep.go:34`),每 `SweepInterval`
(默认 5m)调 `SweepHistory`(`sweep.go:20`):按 `HistoryRetentionDays`(默认 90)算 cutoff → `snowflake.MinIDAt`
换算成 message_id 边界 → `DeleteMessagesBefore`(`data/chat_repo.go:83`)按**主键范围** `DELETE ... LIMIT SweepBatch`
(默认 500)小批量删,不长事务锁表。**多副本各自跑,无锁**(DELETE 幂等,并发只多花空批);cutoff 早于雪花 Epoch
(上线未满保留期)时 `MinIDAt` 返 0,直接跳过。此为不变量 §9.24 只增表有界的落地。

## 频道与降级矩阵

| 频道 | 持久化 | 弱依赖组件 | 组件缺失(nil)| 组件现查失败 |
|---|---|---|---|---|
| PRIVATE | ✅ MySQL(强依赖)| pusher(kafka)| 落库成功,推送跳过(对方 PullHistory 兜底)| 落库强依赖失败 → 整条报错重试 |
| TEAM | ❌ | team gRPC + pusher | `chat_team_degraded` warn,返回 message_id | `ErrUnavailable` 让客户端重试 |
| GUILD | ❌ | guild gRPC + pusher | `chat_guild_degraded` warn,返回 message_id | `ErrUnavailable` 让客户端重试 |
| GROUP | ❌ | group gRPC + pusher | `chat_group_degraded` warn,返回 message_id | `ErrUnavailable` 让客户端重试 |
| WORLD | ❌ | pusher | `chat_world_degraded` warn,返回 message_id | push 失败只 warn |

**关键不变量**:
- **成员现查失败 ≠ 假成功**:TEAM/GUILD/GROUP 在 reader 已配置但 gRPC 出错时**必须报错**,不能返回 `message_id`——
  否则成员解析不出来则无人收到消息(静默丢失)且发送者成员校验被跳过(`chat.go:198` 等)。
- **私聊落库先于推送**:`SavePrivate` 成功才 `PushPrivate`,保证「返回成功 ⟺ 历史已落库」,推送丢失可由 PullHistory 补。
- **私聊双向历史**:`ListPrivate` 的 WHERE 同时匹配 `(sender=me,recv=peer)` 与 `(sender=peer,recv=me)`,两人对话合并按时间倒序。

## 存储

私聊历史落 MySQL `pandora_social` 库(建表 `deploy/mysql-init/06-social-tables.sql`;TiDB 变体
`deploy/tidb-init/01-social-tidb.sql`)。只有 PRIVATE 频道落库,即时频道不持久化。

| 表 | 主键 | 关键列 | 用途 |
|---|---|---|---|
| `chat_private_messages` | `message_id`(snowflake)| `sender_id` / `receiver_id` / `content` / `send_time_ms` | 私聊离线历史(结构化列直接映射,非 proto blob,§5.9)|

- **查询**:`ListPrivate` 按收发双方 + `send_time_ms DESC` + `before_ms` 游标翻页 + `SQL LIMIT`(读取侧上限)。
- **清理**:`DeleteMessagesBefore` 按 `message_id <` 主键范围批删(雪花时间段单调,无需额外时间列索引),§9.24 登记在册。
- **无 Redis 存储**:chat 只经 `node.redis_client` **只读** login 会话权威 `pandora:sess`(会话现行性门),不写 Redis。

## 配置项(`internal/conf/conf.go`)

`ChatConf`(`chat.*`)私有配置:

| 键(`chat.*`)| 默认 | 说明 |
|---|---|---|
| `max_content_len` | `256` | 单条消息最大字符数(utf8 rune 计),超长 → `ErrChatMessageTooLong` |
| `history_limit` | `50` | `PullHistory` 单次返回上限(读取侧上限 §9.18),请求 `limit` 超此值按此截断 |
| `team_addr` | `""` | team 服务 gRPC 地址;空 → 队伍频道降级(弱依赖)|
| `guild_addr` | `""` | guild 服务 gRPC 地址(GuildService + GroupService 同进程共用);空 → 公会 / 群频道降级 |
| `sensitive_words` | `[]` | 敏感词列表(命中整词替换等长 `*`,大小写敏感全等匹配);空 → 不过滤 |
| `history_retention_days` | `90` | 私聊历史保留天数(§9.24 只增表有界)|
| `sweep_interval` | `5m` | 保留期清理轮询间隔(多副本各自跑,无锁)|
| `sweep_batch` | `500` | 每轮清理行数上限(小批量防长事务锁表)|

共享 `config.Base` 关键项(main.go 依赖):`server.grpc.addr`(默认 `:20005`)/ `server.http.addr`(默认 `:21005`)、
`node.mysql_client.dsn`(**强依赖,必填,空则启动失败**,指向 `pandora_social`)、`node.node_id`(snowflake node)、
`node.redis_client`(会话门只读)、`kafka.brokers`(空则推送整体降级)、`snowflake.node_id_source`
(`static` / `etcd`)、`cellroute`(区域总线路由,可空)、`session_gate.require`(dev `false` / prod 机械置 `true`)。

## 本地启动

```powershell
# 1. 基础设施(MySQL pandora_social 强依赖;起 team/guild + kafka + redis 后可跑全链,留空则弱依赖降级)
pwsh tools/scripts/dev_up.ps1

# 2. 启 chat(开发期配置)
go run ./services/social/chat/cmd/chat -conf services/social/chat/etc/chat-dev.yaml
```

> `node.mysql_client.dsn` 必填(私聊落库不可降级),为空直接启动失败。TeamAddr / GuildAddr / kafka 留空只降级
> 对应频道,不影响进程启动。私聊历史迁 TiDB 用 `etc/chat-dev-tidb.yaml`(仅 DSN / collation 不同)。

## 关联文档

- [`go-services.md §2.5`](../../../docs/design/go-services.md) — chat 要约(五频道 RPC / fan-out / 落库策略)
- [`decision-revisit-chat-group.md`](../../../docs/design/decision-revisit-chat-group.md) — 补公会 / 临时群频道的设计推演与落地
- [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md) — 推送原则 2(不回发自己)及世界广播例外
- [`scale-cellular-20m.md`](../../../docs/design/scale-cellular-20m.md) §4.4 — 私聊跨 region 全局桥 key 口径与投递落点
- [`session-generation-rollout.md`](../../../docs/design/session-generation-rollout.md) — 会话现行性门(顶号后旧 JWT 失效)
- [`infra.md`](../../../docs/design/infra.md) — 端口 / topic / 库表规划
