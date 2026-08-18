# team

> 组队服务:维护 MOBA 5v5 的队伍聚合(建队 / 邀请 / 入队 / 离队 / 踢人 / 准备),
> 权威态全在 **Redis**,队伍状态变更经 kafka `pandora.team.update` 推给 push 服务转 UE。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`
> 的 [`go-services.md §2.7`](../../../docs/design/go-services.md)、跨 region 组队见
> [`scale-cellular-20m.md §4.2/§4.4`](../../../docs/design/scale-cellular-20m.md);
> push 协议原则见 [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md)。
>
> 代码行号锚点以**函数名**为准,行号截至当前 HEAD(会随改动漂移)。

## 职责与边界

- **职责**:队伍生命周期状态机(`FORMING`/`READY`/`DISBANDED`)、邀请令牌、队长转移、准备/换英雄;
  队伍变更事件扇出(kafka → push);离队/被踢时联动撤销 matchmaker 匹配票据。
- **权威态**:队伍主体(`TeamStorageRecord`)、玩家归属索引(`player→team_id`)、邀请令牌全在
  **Redis**(无 MySQL);进程内只做 `lastTouch` 心跳节流缓存,不持久化任何影子状态。
- **不变量归属**:
  - 不变量 §1「一人只能在一个队」由 `ClaimPlayer`(SETNX)在 Redis 原子保证,不靠进程内锁。
  - 不变量 §9-18「客户端可堆积列表须有上限」由 `MaxPendingInvites` 在写入侧(邀请索引 Lua)兜住。
  - 不变量 §9-22「推送只是投影,权威在查询接口」由 `ListMyPendingInvites` 拉取兜底保证。
- **不做的事**:不算 MMR / 经验 / 掉落;不做撮合(那是 matchmaker 的活,team 只被其 `GetTeam` 读快照);
  **无后台循环 / 无 leader 选举 / 无 kafka consumer**——所有 RPC 在 biz 内完成状态迁移 + Redis 写 + push 后立即返回。

## 端口(`docs/design/infra.md §6.2`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20010` | 客户端 RPC(经 Envoy)+ 内部 `GetTeam`(matchmaker 直连) |
| HTTP | `:21010` | 仅 `/metrics`(`team.proto` 无 `google.api.http` 注解,无 RESTful RPC) |

端口来自 `internal/conf/conf.go` 的 `Defaults()`(`Server.Grpc.Addr=:20010` / `Server.Http.Addr=:21010`)。

## 对外接口

代码入口:`internal/service/team.go`(gRPC service 层)。**所有写 RPC 强制用 JWT ctx 里的 `player_id`
覆盖 request 字段**(R5,防伪造他人身份):`callerID(ctx)` 从 `plog.CtxKeyPlayerID` 取,
`player_id=0` 直接返 `ERR_UNAUTHORIZED`。鉴权链见下方目录结构与「鉴权模型」。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `CreateTeam()` | 客户端 | 建队,发起者即队长(`team_id` 由 service 层 snowflake 现发) | JWT `player_id`(ctx) |
| `Invite(team_id, target_player_id)` | 客户端 | 邀请目标玩家,写邀请令牌 + push 弹框事件 | JWT `inviter_id`(ctx) |
| `AcceptInvite(invite_id, team_id)` | 客户端 | 接受邀请入队(`invite_id` 可选,提供则校验令牌) | JWT `player_id`(ctx) |
| `LeaveTeam(team_id)` | 客户端 | 主动离队,空队则解散,队长转移 | JWT `player_id`(ctx) |
| `Kick(team_id, target_player_id)` | 客户端 | 队长踢人 | JWT `captain_id`(ctx) |
| `SetReady(team_id, ready, hero_id)` | 客户端 | 设准备态 / 换英雄,全员 ready → `READY` | JWT `player_id`(ctx) |
| `GetTeam(team_id)` | 客户端 / **matchmaker(内部)** | 读队伍完整快照(只读) | 无 JWT 强制;`team_id` 即授权 |
| `GetMyTeam()` | 客户端 | 查本人当前队伍(索引反查 + 脏索引自愈 + 在线续期) | JWT `player_id`(ctx) |
| `ListMyPendingInvites()` | 客户端 | 拉「发给我的待处理邀请」(推送兜底,唯一权威查询) | JWT `player_id`(ctx) |

> **鉴权模型**:team **没有** matchmaker 那种「拒绝玩家 JWT 的内部专用 RPC」。唯一被内部调用的
> `GetTeam` 是**只读**接口——`team_id` 是 Snowflake、`team` 快照已经是客户端可见结构,所以对客户端和
> matchmaker 一视同仁,不做 caller 身份区分。反向的内部调用发生在 **team → matchmaker**:离队/踢人时
> team 作为内部调用方(不带玩家 JWT、`callerID==0`)调 matchmaker `CancelMatch`,见核心调用链 §4。
>
> **push 契约**:队伍变更不提供 `StreamTeamUpdates` RPC,一律经 kafka `pandora.team.update`
> (key=`player_id`,同玩家保序)推给 push 服务。push **原则 2**(不发 caller 自身)由
> `PushToPlayers` 内部按 `callerPlayerID` 排除保证。

## 目录结构(Kratos 标准分层,对齐 matchmaker / login)

```
cmd/team/main.go               启动入口(redis + snowflake + kafka producer + matchmaker client + sessiongate 装配)
etc/team-dev.yaml              开发期配置(:20010 / redis 6380 / kafka 9093 / matchmaker 20011)
etc/team-prod.yaml.example     生产配置样例
internal/
  conf/conf.go                 配置结构(TeamConf)+ Defaults()(端口 / TTL / 上限 / 推送模式默认值)
  service/
    team.go                    RPC 入口(实现 teamv1.TeamServiceServer,JWT 身份下沉 + errcode→proto 映射)
  biz/
    team.go                    TeamUsecase 核心(9 个 RPC 业务逻辑 + claimPlayerHealingOrphan + push 辅助)
    team_sharding.go           队伍 owner cell 分片键口径 + 跨 region 组队观测(nil-safe,router 注入才生效)
    metrics.go                 pandora_team_invite_push_failed_total 指标
  data/
    team.go                    RedisTeamRepo(队伍主体 / 玩家索引 / 邀请令牌;WATCH/MULTI/EXEC + Lua)
    match_canceler.go          GrpcMatchCanceler(matchmaker gRPC client,离队联动撤票)
  server/
    grpc.go                    gRPC server 注册(AuthOptional + SessionCurrent 中间件)
    http.go                    HTTP server(仅 /metrics)
```

## 核心调用链

### 1. CreateTeam —— 先写队伍主体,后声明归属

`CreateTeam`(`internal/biz/team.go:182`)。**写序铁律:先写主体 → 后 `ClaimPlayer` 声明归属**,不可倒:

```
CreateTeam(teamID 新发, playerID)
├─ repo.Create           写队伍主体(此时无索引指向,对全世界不可见)     data/team.go:180
├─ claimPlayerHealingOrphan  SETNX 声明 player→team 归属(不变量 §1)     biz/team.go:587
│    └─ 声明失败 → repo.DeleteTeam 回滚刚写的主体,返 ErrTeamAlreadyInTeam(3004)
└─ pushUpdate(MEMBER_JOINED) 给队长自己一份快照确认
```

> **为什么不能倒过来**:`teamID` 是 Snowflake 新发、返回前只有创建者可见,所以「索引指向 X 但 X 主体不在」
> 永远是真孤儿(不是另一个建队的 in-flight 中间态)——这是 `claimPlayerHealingOrphan` 敢把「主体不存在」
> 判为孤儿并 CAS 清索引重试的安全前提。若「先 claim 后写主体」,并发自愈会误删 in-flight claim,
> 同一玩家可能进两支队伍(违反 §1)。

### 2. Invite —— 邀请令牌 + 灰度双发

`Invite`(`internal/biz/team.go:221`)→ `repo.SetInvite`(`data/team.go:363`)→ 按 `InvitePushMode` 推送:

```
Invite(inviteID 新发, teamID, inviterID, targetID)
├─ repo.Get + 成员/满员校验(inviter 必须在队,未满员)
├─ repo.SetInvite  claimInviteSlotScript(Lua,单 key 原子):
│    ZREMRANGEBYSCORE 清过期 → ZCARD ≥ MaxPendingInvites 则拒 ErrTeamInvitePendingLimit(3008)
│                            → 否则 ZADD 占位 + 写令牌 hash(先占位后写 hash,方向安全)
└─ 按 InvitePushMode 推送(原则 2:不发 inviter 自己):
     dual(默认)   pushInvite(独立 TeamInviteEvent, event_type=1) + pushUpdate(legacy INVITE_SENT)
     dedicated     只发独立 TeamInviteEvent(新客户端全量铺开后)
     legacy        只发旧 TeamUpdateEvent(回退用)
```

> **双发原理**:老客户端只认 `TeamUpdateEvent(reason=INVITE_SENT)`,新客户端只认独立
> `TeamInviteEvent`(域内 `event_type=1` 细路由,见 push README)。灰度共存期「双发」喂饱两代,
> **各弹一次不双弹**(新客户端已纯化、不再从 legacy 读邀请)。推送失败是弱依赖,只 `InvitePushFailed`
> 计数 + Warn,不反向失败邀请主流程——被邀请人靠 `ListMyPendingInvites` 拉取兜底。

### 3. AcceptInvite —— claim 先于改成员列表(防 TOCTOU)

`AcceptInvite`(`internal/biz/team.go:275`):

```
AcceptInvite(inviteID, teamID, playerID)
├─ inviteID≠0 → repo.GetInvite 校验令牌(target/team 匹配,否则 ErrTeamInviteExpired)
├─ claimPlayerHealingOrphan  ★先声明归属再改成员列表:杜绝两个并发 AcceptInvite 把同一人加进两队
├─ repo.UpdateWithLock(WATCH/MULTI/EXEC):
│    满员/解散/已在队校验 → append 成员 → 全员 ready 则 FORMING→READY → cloneTeam
│    失败 → DeletePlayerIndexIfMatches(CAS)回滚 claim
├─ repo.DeleteInvite  删令牌 + 释放 pending 配额
└─ pushUpdate(MEMBER_JOINED) 给全体(不发 playerID 自己)
```

### 4. LeaveTeam / Kick —— 移除成员 + 匹配联动(弱依赖)

`LeaveTeam`(`internal/biz/team.go:352`)/ `Kick`(`internal/biz/team.go:414`)结构一致:

```
UpdateWithLock:removeMember → 空队则 DISBANDED / 否则队长转移 + READY→FORMING → cloneTeam
├─ DeletePlayerIndexIfMatches(CAS)  仅当索引仍指向本队才删,防误删并发新归属
├─ cancelMatchmaking(best-effort)   biz/team.go:636 —— 见下
└─ push:DISBANDED 分支先 refreshDisbandedTTL(短 TTL)再 push DISBANDED;否则 push MEMBER_LEFT/KICKED
```

`cancelMatchmaking`(`internal/biz/team.go:636`)修复「排队中离队不撤票」的跨服务不一致:成员离队后其
matchmaker 票据里仍含已离队的人,成局会把他拉进战斗、残留 claim 还会阻塞他加入的新队 `StartMatch`。
弱依赖语义:`matchCanceler==nil`(未配 `matchmaker_addr`)→ 跳过;`ErrMatchNotFound(4004)`= 本就没排队,静默;
其余错误只 Warn 不阻断离队(残留票据由确认期超时 / TTL 兜底)。内部调用 `callerID==0`,身份走
`CancelMatchRequest.player_id`(`data/match_canceler.go:50`)。

### 5. GetMyTeam —— 索引反查 + 脏索引自愈 + 在线续期

`GetMyTeam`(`internal/biz/team.go:529`):`GetPlayerTeamID` 反查 → 主体不存在/已解散时
`DeletePlayerIndexIfMatches`(CAS)清脏索引按无队伍处理(否则玩家被 SETNX 永久挡住无法再建队)→
命中则 `maybeTouchTeam`(`biz/team.go:136`)在线续期。续期 15 分钟节流(`touchInterval`)+ best-effort,
只在 `GetMyTeam`(本人 + 索引校验过)续,`GetTeam`(任意 team_id)绝不续,防旁人读把弃队永久续命。
节流表 `lastTouch` 由 `maybeSweepLastTouch`(`biz/team.go:155`,CAS 抢占单 goroutine)每 15 分钟惰性清扫,
内存不随 DAU 无界增长。

### 6. ListMyPendingInvites —— 推送兜底(权威查询)

`ListPendingInvites`(`internal/biz/team.go:565`)→ `repo.ListPendingInvites`(`data/team.go:425`):
按被邀请人 zset 索引惰性清过期成员 → 升序取前 `MaxPendingInvites` 条 invite_id → pipeline 逐条读令牌 hash
(hash 已没则跳过并清索引残留)。邀请令牌唯一权威在 Redis,kafka→push 只是投影(不变量 §9-22);
客户端在登录 / 回前台 / 打开组队 UI 时调本接口,推送从「唯一通道」降级为「加速器」,丢帧最多延迟弹窗不丢邀请。

## 状态机与不变量

### 队伍状态机(team 服务实际驱动的迁移)

```
CreateTeam ──► FORMING ──(全员 ready)──► READY
                 ▲                          │
                 └──(leave/kick/取消 ready)──┘
                 │
                 └──(最后一名成员离队)──► DISBANDED(终态,任何写返 ErrTeamWrongState)
```

状态常量:`internal/biz/team.go:58`。`TEAM_STATE_MATCHING` / `TEAM_STATE_IN_BATTLE` 在 proto 里存在、
在本服务里定义了常量,但**当前 team 写路径不驱动这两态的迁移**(队伍进撮合/战斗后的态由 matchmaker/battle 侧管理),
team 只驱动 `FORMING ↔ READY ↔ DISBANDED`。

> **2026-08-17 起 ready 不再是开局门槛(LoL 式流程)**:`BeginTeamMatch` 对 FORMING/READY
> 都放行,队长直接点开始匹配即可组票;「带缺席者开局」由 matchmaker 在线闸(4011 点名)
> 与撮合确认期(`MATCH_STAGE_CONFIRM`)兜住。`SetReady` / READY 态 / `EndTeamMatch` 保留为
> 存量客户端兼容路径(expand 期),contract 时机见
> `docs/design/decision-revisit-team-match-lifecycle-and-roster-rollout.md`。

### 关键不变量

| 主题 | 约束 | 代码锚点 |
|---|---|---|
| 一人一队 | `ClaimPlayer` SETNX 原子声明归属;孤儿索引自愈不误拦 3004 | `claimPlayerHealingOrphan` / `ClaimPlayer` |
| 先建后声明 | 先写队伍主体再 claim,claim 失败回滚主体,崩溃残留无主体由 TTL 回收 | `CreateTeam` |
| 索引删除防误删 | 所有清理走 `DeletePlayerIndexIfMatches`(Lua CAS),仅当仍指向本队才删 | `data/team.go:328` |
| 邀请写入上限 | 同一被邀请人 pending 数 ≥ `MaxPendingInvites` 拒 3008,限流+占位单 key Lua 原子 | `claimInviteSlotScript` |
| 乐观锁 | 状态机写走 `UpdateWithLock`(WATCH/MULTI/EXEC),冲突重试 `OptimisticRetry` 次耗尽返 3007 | `data/team.go:194` |
| push 不发 caller | `pushUpdate` 传 `callerPlayerID`,`PushToPlayers` 内部排除自身(原则 2) | `pushUpdate` / `pushInvite` |
| 存储不外露 | RPC/push 只回 `recordToProto` 组装的客户端可见 `Team`,不暴露 `TeamStorageRecord` 存储字段 | `RecordToProto` |

### 存储布局(Redis,`internal/data/team.go` 顶部注释)

| Key | 类型 | TTL | 用途 |
|---|---|---|---|
| `pandora:team:{<team_id>}` | proto bytes(`TeamStorageRecord`) | `active_ttl`(60m)/ 解散后 `disbanded_retention`(5m) | 队伍主体;hashtag `{}` 保 cluster slot 一致 |
| `pandora:team:player:<player_id>` | string(team_id) | 跟随队伍(`TouchTeam` 续) | 玩家归属索引(SETNX claim) |
| `pandora:team:invite:<invite_id>` | hash(team_id/target/inviter/expires_at_ms) | `invite_ttl`(60s) | 邀请令牌(权威) |
| `pandora:team:invite:target:<player_id>` | zset(member=invite_id,score=expires_at_ms) | `invite_ttl`(每次写刷新) | 被邀请人 pending 索引:写入侧限流 + 拉取兜底 |

## 配置项(`internal/conf/conf.go`,默认值来自 `Defaults()`)

| 键(`team.*`) | 默认 | 说明 |
|---|---|---|
| `invite_ttl` | `60s` | 邀请令牌 Redis TTL,`AcceptInvite` 须在此前提交 |
| `disbanded_retention` | `5m` | 队伍解散后 key 保留时长,供客户端查最终状态 |
| `active_ttl` | `60m` | 活跃队伍 key 生命周期,无写操作超时即过期(防僵尸队伍) |
| `max_members` | `5` | 一队最多成员数(MOBA 5v5) |
| `optimistic_retry` | `3` | WATCH/MULTI/EXEC 冲突最大重试次数,耗尽返 `ErrTeamConcurrent(3007)` |
| `matchmaker_addr` | `""` | matchmaker gRPC 直连地址;留空 → 离队/踢人不撤匹配票据(弱依赖) |
| `invite_push_mode` | `dual` | 邀请推送灰度模式:`dual`(双发)/ `dedicated`(仅独立事件)/ `legacy`(仅旧事件) |
| `max_pending_invites` | `10` | 同一被邀请人未过期 pending 邀请上限(不变量 §9-18);超限返 `3008`,`ListMyPendingInvites` 也按此截断 |

服务级配置(`config.Base`,非 `team.*`):`server.grpc.addr`/`server.http.addr`(端口)、`node.redis_client`
(Redis 强依赖,`host` 单实例 / `addrs` cluster)、`kafka.brokers`(非空则 producer 为**启动强依赖**,
初始化失败在 gRPC Ready 前退出)、`snowflake`、`session_gate.require`(会话现行性门,顶号后旧 JWT 立即失效)。

## 本地启动

```powershell
# 1. 基础设施(redis + kafka;起 matchmaker 后可跑离队撤票全链,留空则跳过联动)
pwsh tools/scripts/dev_up.ps1

# 2. 启 team(dev 配置)
go run ./services/matchmaking/team/cmd/team -conf services/matchmaking/team/etc/team-dev.yaml
```

> `kafka.brokers` 显式为空时保留纯 RPC 本地调试模式(`Invite` 只落令牌不推送,`initializeTeamPublication`
> 明确放行,见 `cmd/team/main.go:195`);只要配了 broker,producer 就是启动强依赖,初始化失败拒绝 Ready。

## 关联文档

- [`go-services.md §2.7`](../../../docs/design/go-services.md) — team 要约(RPC 清单 / GetMyTeam 语义 / 邀请双发发布顺序)
- [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md) — push 原则 2(不发 caller)/ 立即完成型 RPC
- [`scale-cellular-20m.md §4.2/§4.4`](../../../docs/design/scale-cellular-20m.md) — 队伍锚定队长 owner cell 分片 / 跨 region 组队与 battle 放置
- [`session-generation-rollout.md`](../../../docs/design/session-generation-rollout.md) — `session_gate.require` 会话代际 / 顶号防护发布手册
- [`zero-downtime-update.md`](../../../docs/design/zero-downtime-update.md) — `max_conn_age` GOAWAY 重拨 / 金丝雀发布(邀请双发共存窗口)
- [`infra.md §6.2`](../../../docs/design/infra.md) — 服务端口规划 / Redis key / kafka topic 台账
