# guild

> 社交域第三服:**公会**(常驻社团,单归属 + 职位审批)与**临时群聊**(轻量多人会话,多归属)
> 同进程两套 gRPC。成员关系落 MySQL `pandora_social`,公会成员变更经 kafka `pandora.guild.event`
> 推给在线成员。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`
> 的 [`decision-revisit-chat-group.md`](../../../docs/design/decision-revisit-chat-group.md)(公会 + 临时群拍板)、
> [`decision-revisit-guild-scaling.md`](../../../docs/design/decision-revisit-guild-scaling.md)(社交库扩容路线 / TiDB 计数写法);
> 服务要约见 [`go-services.md §2.5b`](../../../docs/design/go-services.md)。
>
> 代码锚点以**函数名**为准;行号截至当前 HEAD(会随改动漂移)。

## 职责与边界

- **职责**:公会成员管理(创建 / 申请 / 审批 / 退会 / 踢人 / 解散 / 转让会长 / 任命官员 / 查询) +
  临时群管理(建群 / 拉人 / 退群 / 踢人 / 解散 / 转让群主 / 查询)。
- **权威态**:公会 / 群的成员关系、职位、加入申请全在 **MySQL `pandora_social`**(结构化列,不是 pb blob);
  复合一致性操作(审批 / 退会 / 踢人 / 转让 / 解散)在单 MySQL 事务内完成。
- **两种社交结构对比**:公会 = **单归属**(玩家只属一个公会,`guild_members.player_id` 主键硬约束) +
  三级职位(leader / officer / member) + 申请审批;临时群 = **多归属**(玩家可在多个群) + 两级职位
  (owner / member) + 拉人即入。
- **弱依赖降级**:Redis(公会资料读缓存)与 kafka(成员变更推送)都是弱依赖,未配置 / 不通则降级
  (直连 MySQL / 静默丢推),不阻塞启动、不影响权威写。
- **不做的事**:不落聊天历史(公会 / 群聊是即时频道,由 chat 服务转发、guild 只提供成员名单给 chat
  fan-out);临时群成员变更 MVP 不单独推送(客户端拉 `ListMyGroups` 兜底)。

## 端口(`docs/design/infra.md`)

端口来自 `internal/conf/conf.go` 的 `Defaults()`。

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20008` | 客户端 RPC(经 Envoy)—— GuildService + GroupService 同端口 |
| HTTP | `:21008` | 仅 `/metrics`(`guild.proto` / `group.proto` 无 `google.api.http` 注解,无 RESTful RPC) |

## 对外接口

代码入口:`internal/service/guild.go`(GuildService)、`internal/service/group.go`(GroupService)。
两套 service 同进程、同 package,共用 `callerID` / `toProtoCode` 辅助。

**鉴权模型(R5)**:Envoy `jwt_authn` 在路由层 require JWT;`pmw.AuthOptional` 从 Envoy 注入的
`x-pandora-player-id` header 把 `player_id` 放进 ctx;`pmw.SessionCurrent` 校验请求 jti == login 会话权威
当前一代(顶号 / 登出后旧 JWT 立即失效)。**所有写 / 个人 RPC 强制用 ctx 里的 `player_id`,忽略请求体
里的 `player_id` 字段**(`callerID(ctx)`,防伪造他人身份);`player_id=0` → `ERR_UNAUTHORIZED`。
公会 / 群内的职位权限(leader/officer/owner)在 **MySQL 事务内持父行锁复核**,不信 biz 层预读快照。

> 本服务**没有内部专用 RPC**,也没有 HMAC 内部信任域(不同于 matchmaker 的 `ReleaseMatch` /
> `ResolvePlayerMatchContext`)——全部 RPC 都是客户端面。纯只读查询(`GetGuild` / `ListMembers` /
> `GetGroup` / `ListGroupMembers`)不校验 `callerID`,允许任意登录玩家查询。

### GuildService(`proto/pandora/guild/v1`)

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `CreateGuild(name)` | 客户端 | 建公会,创建者成为会长(单归属:已在公会则拒) | JWT `player_id` |
| `ApplyJoin(guild_id)` | 客户端 | 申请入会,返回 `request_id`;推通知会长 / 官员 | JWT `player_id` |
| `ApproveJoin(request_id)` | 客户端 | 审批通过(须 LEADER/OFFICER,事务内复核) | JWT `player_id` |
| `RejectJoin(request_id)` | 客户端 | 拒绝申请(须 LEADER/OFFICER) | JWT `player_id` |
| `LeaveGuild()` | 客户端 | 退会(LEADER 须先转让 / 解散) | JWT `player_id` |
| `KickMember(target_id)` | 客户端 | 踢人(LEADER 踢非会长;OFFICER 只踢 member) | JWT `player_id` |
| `DisbandGuild()` | 客户端 | 解散公会(仅 LEADER),推全体成员 | JWT `player_id` |
| `TransferLeader(target_id)` | 客户端 | 转让会长(仅现任 LEADER) | JWT `player_id` |
| `SetOfficer(target_id, is_officer)` | 客户端 | 任命 / 撤销官员(仅 LEADER) | JWT `player_id` |
| `GetGuild(guild_id)` | 客户端 | 查公会资料(只读,cache-aside) | 任意登录玩家 |
| `GetMyGuild()` | 客户端 | 查"我的公会";不在任何公会 → `OK` + 空 | JWT `player_id` |
| `ListMembers(guild_id, cursor, limit)` | 客户端 | 列成员,`player_id` 升序游标分页 | 任意登录玩家 |
| `ListJoinRequests(cursor, limit)` | 客户端 | 列本会挂起申请(须 LEADER/OFFICER) | JWT `player_id` |

### GroupService(`proto/pandora/group/v1`)

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `CreateGroup(name, member_ids)` | 客户端 | 建群,建群者成为群主;初始成员去重排除自己 | JWT `player_id` |
| `InviteToGroup(group_id, target_id)` | 客户端 | 拉人入群(操作者须在群内;已在群则幂等成功) | JWT `player_id` |
| `LeaveGroup(group_id)` | 客户端 | 退群(OWNER 须先转让 / 解散) | JWT `player_id` |
| `KickFromGroup(group_id, target_id)` | 客户端 | 踢人(仅 OWNER) | JWT `player_id` |
| `DisbandGroup(group_id)` | 客户端 | 解散群(仅 OWNER) | JWT `player_id` |
| `TransferOwner(group_id, target_id)` | 客户端 | 转让群主(仅现任 OWNER) | JWT `player_id` |
| `GetGroup(group_id)` | 客户端 | 查群资料(只读) | 任意登录玩家 |
| `ListGroupMembers(group_id)` | 客户端 | 列群成员(owner 在前,SQL 硬上限兜底) | 任意登录玩家 |
| `ListMyGroups()` | 客户端 | 列"我所在的群" | JWT `player_id` |

> **客户端只拿可见结构**(不变量 §14):RPC response 只回 `Guild` / `GuildMember` / `GuildJoinRequest` /
> `Group` / `GroupMember`,由服务端从存储行按最小视图组装(`toGuildView` / `toGroupView`),不外露
> `updated_at` 等内部列;`nickname` 留空由客户端按 `player_id` 解析。

## 目录结构(Kratos 标准分层,对齐 login / push)

```
cmd/guild/main.go              启动入口(MySQL 强依赖 + schema gate + snowflake + redis/kafka 弱依赖 + sweep goroutine 装配)
etc/guild-dev.yaml             开发期配置(单 MySQL,redis/kafka dev 端点)
etc/guild-dev-tidb.yaml        TiDB 后端联调配置(社交库扩容路线,opt-in)
etc/guild-prod.yaml.example    生产配置样例
internal/
  conf/conf.go                 配置结构(GuildConf:成员/申请/群上限 + 缓存 TTL + 保留期清理)+ Defaults()
  service/
    guild.go                   GuildService RPC 入口(JWT 身份下沉,proto ↔ biz 互转,errcode → proto code)
    group.go                   GroupService RPC 入口(同进程,复用 callerID / toProtoCode)
  biz/
    guild.go                   GuildUsecase(公会业务 + cache-aside 读 + guild.event 推送扇出)
    group.go                   GroupUsecase(临时群业务;memberIDs 去重排除建群者)
    sweep.go                   终态入会申请保留期清理循环(§9.24)
  data/
    guild_repo.go              MySQLGuildRepo(公会 / 成员 / 申请事务;guilds 父行统一加锁序防 ABBA 死锁)
    group_repo.go              MySQLGroupRepo(群 / 群成员事务;player_group_counts 计数行 + 升序加锁序)
    cache.go                   RedisGuildCache(cache-aside 读缓存 + 滚动升级字段位图投毒防护)
    schema.go                  ValidateRequiredSchema(接流量前校验计数列 / 计数表物理契约)
  server/
    grpc.go                    gRPC server 注册(AuthOptional + SessionCurrent 中间件,双 service 同链)
    http.go                    HTTP server(仅 /metrics)
```

## 核心调用链

RPC handler(`service`)→ Usecase(`biz`)→ Repo 事务(`data`),弱依赖(cache / kafka)旁路。

### 1. CreateGuild —— 单归属建会

`GuildService.CreateGuild`(`internal/service/guild.go:CreateGuild`)取 JWT `player_id` + snowflake 预生成
`guild_id` → `GuildUsecase.CreateGuild`(`internal/biz/guild.go:CreateGuild`):

```
CreateGuild(playerID, name, newGuildID)
├─ 校验 name 非空 + rune 数 ≤ MaxNameLen
├─ repo.CreateGuild (internal/data/guild_repo.go:CreateGuild) —— 单事务:
│    ├─ SELECT guild_members WHERE player_id  → 已在公会则 ErrGuildAlreadyInGuild(单归属)
│    ├─ INSERT guilds (member_count=1)         → uk_name 冲突 → ErrGuildNameTaken
│    └─ INSERT guild_members (role=leader)
└─ invalidateMember(playerID)                  —— 写后删 member 反查缓存(弱依赖)
```

### 2. ApplyJoin → ApproveJoin —— 申请审批(带 pending 上限)

```
ApplyJoin(playerID, guildID, newRequestID)              internal/biz/guild.go:ApplyJoin
├─ GetMember(playerID)         已在公会 → ErrGuildAlreadyInGuild
├─ GetGuild(guildID)           不存在 → ErrGuildNotFound
├─ repo.CreateJoinRequest(...maxPending)                internal/data/guild_repo.go:CreateJoinRequest
│    ├─ SELECT guilds ... FOR UPDATE          锁父行(统一加锁序 + 与 Disband 串行,防孤儿申请)
│    ├─ reconcileGuildPendingCount            以 pending 明细为权威校正计数列(TiDB / 滚动兼容)
│    ├─ pendingCount ≥ maxPending → ErrGuildRequestLimit   (不变量 §9.18 写入侧上限)
│    └─ INSERT / 复用申请行(rejected → 复位 pending)+ 更新 pending_request_count
└─ fanoutToManagers(guildID, JOIN_APPLIED)    推 leader/officer(排除申请人本人,原则 2)

ApproveJoin(approverID, requestID)                       internal/biz/guild.go:ApproveJoin
└─ repo.ApproveJoin(...maxMembers)                       internal/data/guild_repo.go:ApproveJoin
     ├─ 未锁读 request.guild_id(不可变列,为定加锁序)
     ├─ SELECT guilds ... FOR UPDATE 读 member_count     父行锁 = 职位变更串行化点
     ├─ SELECT request ... FOR UPDATE                    非 pending → 计数自愈后返回 approved=false
     ├─ SELECT approver role ... FOR UPDATE              非 leader/officer → ErrGuildNoPermission
     ├─ 申请人已在公会 → ErrGuildAlreadyInGuild
     ├─ member_count ≥ maxMembers → ErrGuildFull
     └─ INSERT member(PK player_id 兜底并发双批)+ 申请置 approved + member_count++
  → invalidateGuild(guildID) + invalidateMember(applicantID) + push(applicant, JOIN_APPROVED)
```

> **加锁序铁律**:本包所有公会写事务都**先锁 `guilds` 父行,再锁子表**(`guild_members` /
> `guild_join_requests`)。`guilds` 行成为「单公会唯一串行化闸门」,消除 Approve/Reject 的
> `request→guild` 与 Disband 的 `guild→request` 交叉形成的确定性 ABBA 死锁;`tx`(`guild_repo.go:tx`)
> 再带 1213/1205 有界重试(`txMaxRetries=3`)兜底二级索引间隙锁偶发死锁。

### 3. 会长 / 职位变更 —— 持父行锁复核(消 TOCTOU)

`TransferLeader` / `SetOfficer` / `KickMember` / `DisbandGuild` / `LeaveGuild` 全部在事务内
`SELECT guilds ... FOR UPDATE` 复核操作者**当前**职位,而不信 biz 层预读快照——防「检查通过后操作者
被并发降级 / 退会 / 目标被并发转成会长仍被操作」的 TOCTOU:

- `LeaveGuild` / `RemoveMember`:持父行锁禁止移除现任会长(`curLeader == playerID` → `ErrGuildNotLeader`)。
- `TransferLeader`:确认 `curLeader == oldLeaderID` 才降旧升新 + 改 `guilds.leader_id`,防并发双转让产生双 LEADER。
- `DisbandGuild`:持父行锁读全部成员 `player_id` 与删除同事务原子(不漏并发新批准成员的缓存失效 / 通知),
  返回删除集合供 biz 逐个 `invalidateMember` + 推 `DISBANDED`(全体事件,例外于原则 2)。

### 4. GetMyGuild —— 两级 cache-aside 读

`GuildUsecase.GetMyGuild`(`internal/biz/guild.go:GetMyGuild`):

```
GetMyGuild(playerID)
├─ cache.GetMemberGuildID(playerID)   命中 → cache.GetGuild(guildID) 命中 → 返回视图
├─ (任一 miss / Redis 故障 warn 后回落) repo.GetMyGuild(playerID)  ← MySQL 权威
│    └─ 不在任何公会 → invalidateMember(自愈残留反查) + 返回 (nil,nil)
└─ fillGuildCache + fillMemberCache    回填两级缓存(TTL=CacheTTL 默认 60s)
```

一致性:MySQL 唯一事实源;写路径**先写库事务、后删缓存**(cache-aside 写后删),删失败仅 warn 靠短 TTL
兜底;member 反查只缓存「已在某公会」正向映射,不做负缓存。缓存 key 用 hashtag 括业务 ID
(`pandora:guild:info:{guild_id}` / `pandora:guild:member:{player_id}`),兼容 Redis Cluster。

### 5. 临时群 —— player_group_counts 名额预留

群写事务先锁 `chat_groups` 群行(串行化点),涉及「我所在的群」上限时按 **`player_id` 升序**逐个
`reservePlayerGroupSlot`(`internal/data/group_repo.go:reservePlayerGroupSlot`)对 `player_group_counts`
计数行 `FOR UPDATE`——统一升序加锁序防两个成员集相反的并发建群形成 A↔B 锁环:

```
CreateGroup   → 校验 count ≤ maxMembers → 排序 (owner+members) 升序 → 逐个 reservePlayerGroupSlot
                → INSERT chat_groups + owner + members
AddMember     → 锁群行 → 复核 operator 在群 → 幂等命中直接返回 → reserve 目标名额 → INSERT + member_count++
Remove/Kick   → 锁群行(禁移群主)→ reconcile 计数 → DELETE + member_count-- → releasePlayerGroupSlot
Disband       → 锁群行复核 owner → 成员 player_id 升序 reconcile → 删成员 + 删群 + 逐个 release
```

`reconcilePlayerGroupCount`:先惰性建计数行 + `FOR UPDATE`,再 `COUNT(*)` 明细为权威绝对值回写——
TiDB 无间隙锁靠计数行串行,旧 Pod 只改明细也会被新版按明细自愈(滚动兼容)。

### 6. 成员变更推送 —— guild.event(弱依赖)

`GuildUsecase.push` / `fanoutToManagers`(`internal/biz/guild.go`)经 `GuildEventPusher` 接口发到 kafka
`pandora.guild.event`(main.go 的 `guildEventPusher` 适配 `kafkax.KeyOrderedProducer`),**kafka key =
接收方 `to_player_id`**(不变量 §9:同接收方事件保序;push 服务按 key 路由到该玩家 stream)。
`pusher == nil`(kafka 未配 / broker 不通)或 push 失败只 warn,不阻塞权威写。推送遵循协议原则 2
(不回发操作者本人),`DISBANDED` 是全体事件例外。

### 7. 终态申请保留期清理(§9.24)

`main.go` 起 `go guildUC.RunRequestSweep(ctx)`(`internal/biz/sweep.go:RunRequestSweep`):每
`SweepInterval`(默认 5m)调 `DeleteTerminalJoinRequestsBefore`——`DELETE guild_join_requests
WHERE status<>pending AND updated_at < NOW()-retentionDays LIMIT SweepBatch`。pending 永不清;终态行
无资产语义(成员权威在 `guild_members`),删后再申请等价全新 INSERT。**多副本各自跑、无 leader 选举**
(DELETE 幂等,并发只多花空批)。

## 存储布局(MySQL `pandora_social`,`deploy/mysql-init/11-guild-tables.sql`)

| 表 | 主键 | 用途 / 关键约束 |
|---|---|---|
| `guilds` | `guild_id`(snowflake) | 公会;`uk name`、`member_count`、`pending_request_count`(计数列,以明细为权威自愈) |
| `guild_members` | `player_id` | 公会成员——**PK = player_id 即单归属硬约束**;`role`(1 leader/2 officer/3 member) |
| `guild_join_requests` | `request_id`(snowflake) | 入会申请;`uk (guild_id, player_id)`、`status`(1 pending/2 approved/3 rejected) |
| `chat_groups` | `group_id`(snowflake) | 临时群;`owner_id`、`member_count` |
| `chat_group_members` | `(group_id, player_id)` | 群成员——复合 PK 即**多归属**;`role`(1 owner/2 member) |
| `player_group_counts` | `player_id` | 玩家「所在群数」计数行(§9.18 上限的 TiDB 安全串行化点 + 读优化值) |

角色 / 状态取值与 proto 枚举**数值对齐**(`GuildRole` / `GuildJoinStatus` / `GroupRole`)。成员关系是
结构化列直接映射,不走 pb blob(§5.9)。**Redis** 只存两类只读缓存 key(见调用链 §4),非权威。

**schema gate**:`main.go` 在接流量前调 `data.ValidateRequiredSchema`(`internal/data/schema.go`),
校验 `guilds.pending_request_count` / `player_group_counts.player_id`+`group_count` 的类型 / signedness /
NULL / default / 主键物理契约,缺失或不符则退出(`RequiredSchemaVersion = 2`,提示先跑 `tools/migrate`)。

## 权限与不变量

| 主题 | 约束 | 代码锚点 |
|---|---|---|
| 公会单归属 | 玩家只属一个公会,`guild_members.player_id` 主键 + 事务预检 + INSERT dup 兜底并发双批 | `CreateGuild` / `ApproveJoin` |
| 会长不落空 | LEADER 不能直接退会 / 被踢,须先 `TransferLeader` 或 `DisbandGuild`(数据层持锁兜底) | `RemoveMember` / `KickMember` |
| 职位复核在事务 | 操作者 / 目标职位在持 `guilds` 父行锁后权威复核,不信 biz 预读快照 | `ApproveJoin` / `SetRole` / `TransferLeader` |
| 统一加锁序 | 公会写全部 `guilds` 父行 → 子表;计数行按 `player_id` 升序,消 ABBA 死锁 + 有界重试兜底 | `guild_repo.go:tx` / `reservePlayerGroupSlot` |
| 写入侧上限 | 公会 pending 申请 ≤ `MaxPendingRequestsPerGuild`;玩家所在群 ≤ `MaxGroupsPerPlayer`;成员 ≤ 上限 | `CreateJoinRequest` / `reservePlayerGroupSlot` |
| 读取侧上限 | 成员 / 申请游标分页(默认 50 / 最大 100);群成员 / 我的群 SQL 硬上限 500 兜底 | `clampLimit` / `groupListReadHardLimit` |
| 计数自愈 | 计数列以明细 `COUNT(*)` 为权威回写,兼容滚动升级期旧 Pod 只改明细 | `reconcileGuildPendingCount` / `reconcilePlayerGroupCount` |
| 缓存投毒防护 | info 缓存打「写入方字段位图」,写入方字段集 ⊉ 本副本则当未命中回落 MySQL(滚动安全,§16/§17) | `cache.go:writerHasAllReaderFields` |
| 客户端只拿视图 | RPC 只回 `Guild`/`GuildMember`/`Group`... 组装视图,不外露存储列 | `toGuildView` / `toGroupView` |
| 会话现行性 | 请求 jti 须 == login 会话权威当前一代,顶号 / 登出后旧 JWT 立即失效 | `server/grpc.go` `pmw.SessionCurrent` |

## 配置项(`internal/conf/conf.go`,`guild.*`)

| 键 | 默认 | 说明 |
|---|---|---|
| `max_guild_members` | `100` | 单公会成员上限(`ApproveJoin` 事务原子校验;`ERR_GUILD_FULL`) |
| `max_group_members` | `50` | 单临时群成员上限(建群 / `AddMember` 校验;`ERR_GROUP_FULL`) |
| `max_pending_requests_per_guild` | `200` | 单公会挂起申请上限(`CreateJoinRequest` 校验;`ERR_GUILD_REQUEST_LIMIT`,§9.18) |
| `max_groups_per_player` | `50` | 单玩家可同时加入的群数上限(`ERR_GROUP_JOIN_LIMIT`,§9.18) |
| `max_name_len` | `24` | 公会 / 群名最大长度(utf8 rune) |
| `cache_ttl` | `60s` | 公会读缓存条目 TTL(cache-aside 写后删 + 短 TTL 兜底) |
| `request_retention_days` | `90` | 终态入会申请保留天数(§9.24;pending 永不清) |
| `sweep_interval` | `5m` | 保留期清理轮询间隔(多副本各自跑,DELETE 幂等) |
| `sweep_batch` | `500` | 每轮清理行数上限(`DELETE ... LIMIT`) |
| `server.grpc.addr` | `:20008` | gRPC 端口 |
| `server.http.addr` | `:21008` | HTTP `/metrics` 端口 |

依赖端点在 `node.*`:`mysql_client.dsn`(**强依赖,必填**,指向 `pandora_social`)、`redis_client`
(弱依赖,`host` 与 `addrs` 皆空则禁用缓存)、`kafka.brokers`(弱依赖,空则禁用推送);`session_gate.require`
控制会话门(dev `false`,生产由 `gen_cluster_config.ps1` 机械置 `true`)。

## 本地启动

```powershell
# 1. 基础设施(MySQL pandora_social + Redis;起 kafka 后可跑成员变更推送全链)
pwsh tools/scripts/dev_up.ps1

# 2. 启 guild(公会 + 临时群同进程)
go run ./services/social/guild/cmd/guild -conf services/social/guild/etc/guild-dev.yaml
```

> MySQL 是强依赖:DSN 未配 / schema 未升到 `version 2` 会在接流量前直接退出。Redis / kafka 是弱依赖,
> 未起则降级(直连 MySQL / 不推送),不阻塞启动。TiDB 后端联调用 `etc/guild-dev-tidb.yaml`(社交库扩容路线,opt-in)。

## 关联文档

- [`go-services.md §2.5b`](../../../docs/design/go-services.md) — guild 服务要约(公会 + 临时群职责 / 存储 / 事件)
- [`decision-revisit-chat-group.md`](../../../docs/design/decision-revisit-chat-group.md) — 公会聊天 + 临时群聊拍板与落地
- [`decision-revisit-guild-scaling.md`](../../../docs/design/decision-revisit-guild-scaling.md) — 社交库扩容路线(走 social TiDB)与计数 + 上限的 TiDB 安全写法
- [`read-cache-strategy.md`](../../../docs/design/read-cache-strategy.md) §3 — guild 读缓存(cache-aside)P0 落地方案
- [`decision-revisit-list-pagination.md`](../../../docs/design/decision-revisit-list-pagination.md) — 列表类 RPC 统一游标分页(含 `ListMembers` / `ListJoinRequests`)
- [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md) — 推送协议原则 2(不回发操作者本人)
- [`session-generation-rollout.md`](../../../docs/design/session-generation-rollout.md) — 会话现行性门(顶号 / 登出旧 JWT 失效)
- [`infra.md`](../../../docs/design/infra.md) — 端口规划(guild 20008/21008)与 kafka topic `pandora.guild.event`
