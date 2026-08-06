# Pandora Go 服务清单与契约

> 19 个 go 服务的职责边界、对外接口、关键状态、依赖矩阵。
>
> ⚠️ **2026-06-04 架构终版**:
> - 框架统一 **Kratos**(替代 go-zero,详见 `gateway-decision.md` §4)
> - Edge Gateway 用 **Envoy**(替代之前规划的 pandora-gateway 自研)
> - 推送 = **集中 push 服务 + gRPC server stream**(替代之前规划的自研 WebSocket)
> - 客户端协议:**gRPC-Web over HTTP/2 TLS**(UE FHttpModule + 自研协议解析)

## 1. 服务总览

| # | 服务 | gRPC 端口 | 状态性 | 主要存储 | 主要消费 kafka | 骨架状态 |
|---|---|---|---|---|---|---|
| 1 | login | 20001 | 无 | mysql + redis | (生产 login.event) | ✅ W2 ③(mock,W3 接 mysql/redis) |
| 2 | player | 20002 | 无 | mysql + redis | player.update | ✅ W4 ④(MMR 写回 + GetMMR reader) |
| 3 | data_service | 20003 | 无 | mysql + redis | (写穿层) | 🧪 已实现供本地开发/minikube 验证，**未正式上线**、无有效历史数据 |
| 4 | friend | 20004 | 弱(friend.event 推送) | mysql | pandora.friend.event | ✅ 2026-06-15(好友请求/接受/列表/拉黑 + locator 在线状态) |
| 5 | chat | 20005 | 弱 | mysql(私聊历史)+ kafka | chat.{world,team,private,guild,group} | ✅ 2026-06-27(五频道 + 内容校验 + 私聊落库 + team/guild/group fan-out,公会/群即时不落库) |
| 6 | player_locator | 20006 | 强 | redis | locator.update | ✅ W3 ⑤(W4 ⑦ matchmaker 上报 MATCHING/BATTLE) |
| 7 | leaderboard | 20007 | 无 | redis(实时榜)+ mysql(结算) | (生产 leaderboard.settle) | ✅ 2026-06-27(通用排行榜,全服/公会/副本/活动可扩展) |
| 8 | guild | 20008 | 弱(guild.event 推送) | mysql(pandora_social) | pandora.guild.event | ✅ 2026-06-27(公会 GuildService + 临时群 GroupService 同进程;公会/群聊不落库) |
| 9 | mail | 20009 | 无 | mysql(pandora_social) | (复用 system.notify 红点) | ✅ 2026-06-29(系统/公会邮件 channel+watermark 拉取,个人邮件写扩散离线可达,附件领取幂等) |
| 10 | team | 20010 | 强 | redis | - | ✅ W3 ⑦ |
| 11 | matchmaker | 20011 | 强 | redis | (生产 match.found) | ✅ W4 ①(W4 ⑦ 接 locator 串 MATCHING/BATTLE) |
| 12 | trade | 20012 | 强 | redis | trade.audit | ✅ 2026-06-16(两阶段确认订单状态机 + 乐观锁 + 结算幂等键 + 审计) |
| 13 | dialogue | 20013 | 无 | 配置驱动(内存,留 mysql hook) | - | ✅ 2026-06-16(配置对话树 + 内存会话状态机 Start/Choose/End) |
| 14 | **push** ⭐ | **20014**(gRPC server stream) | 强(连接索引) | redis(离线消息)| pandora.{team,match,chat,player,friend,system}.* | ✅ W2 ⑤(mock 5s tick,W3 接 kafka) |
| 15 | inventory | 20015 | 无 | mysql(pandora_trade) | - | ✅ W5 ③(大厅背包:货币+可堆叠道具,用/售/授予,ledger 幂等) |
| 16 | auction | 20016 | 强(per-market 串行撮合) | redis(订单簿)+ mysql(pandora_auction 权威) | (生产 auction.match/audit) | ✅ 2026-06-19(全服拍卖行/撮合引擎,两层幂等,ZSET 价格-时间优先) |
| 17 | ds_allocator | 20020 | 弱 | redis (+k8s) | (生产 ds.lifecycle) | ✅ W4 ②(Mock 分配器,W4 ③ 发 abandoned,W4 ⑧ abandoned 可靠补偿,W4 ⑫ 真 Agones REST allocator) |
| 18 | hub_allocator | 20021 | 弱 | redis (+k8s) | (生产 ds.lifecycle) | ✅ W4 ⑤ + 自动扩缩容(2026-06-15:按在线人数控 Agones Fleet 副本) |
| 19 | battle_result | 20022 | 无 | mysql | battle.result + ds.lifecycle | ✅ W4 ③(幂等落库 + Elo MMR + abandoned 补偿),W4 ⑨(player.update 事务出箱可靠化) |

⭐ = 2026-06-04 终版新增。push 是 Kratos transport/grpc 暴露的 server stream 服务,客户端通过 Envoy 连过来,详见 `gateway-decision.md` §6。

**Edge Gateway = Envoy**(端口 8443 HTTPS),不是 go 服务,不计在表格内。**状态**:✅ W2 ④ 落地(v1.38.0 docker,login_cluster + push_cluster + grpc_web/cors/router filters,详见 `PROGRESS.md` W2 ④ 段)。

### 1.1 Go RPC context 与异步边界（全服务契约）

Kratos 入站 gRPC handler 的请求 `ctx` 不是普通参数：它携带 server transport，而 transport 的 `ReplyHeader` 直接引用本次响应的 `metadata.MD`（底层为 `map[string][]string`）。handler 返回后，gRPC 会遍历该 map 发送响应头。因此，请求 `ctx`、server transport 和响应 metadata 的有效期只到 handler 返回为止。

以下写法全服务禁止：

```go
go func() {
    ctx, cancel := context.WithTimeout(context.WithoutCancel(requestCtx), timeout)
    defer cancel()
    downstreamClient.Call(ctx, req)
}()
```

`context.WithoutCancel` 只移除取消和 deadline，不会复制或删除任何 Value。上例仍继承 server transport；Kratos client interceptor 再加入 client transport 后，共用 Trace / Metrics / Logging middleware 可能同时看到二者。若 client hop 写入继承的 server `ReplyHeader`，就会与原 handler 回包时的 metadata 遍历并发，触发不可恢复的 `fatal error: concurrent map iteration and map write`。

异步任务必须遵守以下契约：

- 从 `context.Background()` 或项目统一 detached helper 建立干净根 context，只白名单复制 trace / player / match / team 等不可变日志字段；不得复制 transport、metadata、请求对象或取消链。
- 业务参数、玩家列表、地址和下游凭证显式传值；切片、map、指针等可变捕获必须复制或同步。每个后台调用必须有有界 timeout、`cancel`、错误日志和明确生命周期。
- 同步代码只有在调用在 handler 返回前完成、且不会保存或异步转交 context 时，才可为特定清理语义使用 `WithoutCancel`；review 必须沿所有被调用方证明这一点。它不能作为通用 detached API。
- 共用 middleware 必须按本次 interceptor 的调用方向互斥处理。client 调用只写本次 `RequestHeader`，不得写继承的 server `ReplyHeader`；Metrics / Logging 也必须记录下游 client 方法，不能因 server transport 仍在 context 中而误记成上游 handler。
- 新增或修改异步下游 RPC 时，必须覆盖双 transport 单元测试、handler 返回与后台 RPC 并发的集成测试，并在支持 CGO 的 Linux / CI 环境运行 `go test -race`。

关键 writer 的重启设计还必须把 capability / fence 旧 lease TTL、同 Pod 容器重启、CrashLoopBackOff、重新注册、DS 本地授权租约、heartbeat timeout 与启动 sweep 放到同一故障时间轴。最坏恢复时间必须小于业务可恢复预算；不能用删除或覆盖 fencing 换取快速启动。

## 2. 各服务详细契约

### 2.1 login

**职责**:
- 账号注册 / 登录 / 登出
- 颁发 Session Token(给客户端)
- 颁发 DS Ticket(JWT,给 UE DS)
- 验证 DS Ticket(防重放,jti 黑名单)

**对外 RPC**:
```
Login(account, password_hash, device_id) → session_token + hub_ds_addr + hub_ticket
Logout(session_token) → ok
IssueDSTicket(session_token, ds_type, target_id) → ticket
VerifyDSTicket(ticket, ds_pod_name, admission_id; Authorization: Bearer <active DS credential>)
  → player_id + claims
```

- `IssueDSTicket(ds_type=battle)` 与登录断线重连共用同一签发入口：签名前必须从 Redis 证明
  `player_id` 属于 live `target_id` roster；Redis 故障、坏 protobuf、空 roster、陈旧心跳或 Model-B
  active/projection 漂移均不签票。不得只信客户端自报 match_id 或 locator。locator 已明确
  `InBattle` 后签票权威失败会使 Login 返回 `Unavailable`，不会继续 AssignHub/写 LOGIN_PENDING。
  reconnect 最终地址取自同一次 Redis roster/active 快照，不再使用可能滞后的 locator 地址。
- Redis authority 下的 `VerifyDSTicket` 固定顺序为 DS Bearer Guard → active/projection →
  ticket 的 match/pod/UID/epoch/credential 与 assignment/roster → admission JTI marker。
  `admission_id` 是小写 RFC4122 UUIDv4；只给同一逻辑尝试 30s 有界幂等，不同尝试仍按 replay 拒绝。

**不该做的事**:
- ❌ 不存玩家档案(那是 player 服务)
- ❌ 不算 MMR
- ❌ 不广播大厅状态

**依赖**:
- 上游:客户端、UE DS(只用 VerifyDSTicket，走独立 :8444 DS 面)
- 下游:hub_allocator(给 hub_ds_addr)、player(查档案是否存在)、Redis(票据 JTI、Battle roster、
  Redis authority 下的 DS active/projection 与 Hub assignment)

---

### 2.2 player

**职责**:
- 玩家档案(昵称、头像、等级、段位)
- 英雄解锁记录
- 皮肤记录
- MMR 读写(写由 battle_result 调)
- 玩家经验幂等入账、连升结算与经验快照推送

**对外 RPC**:
```
GetProfile(player_id) → PlayerProfile
UpdateNickname(player_id, nickname) → ok
ListHeroes(player_id) → []hero_id
UnlockHero(player_id, hero_id, source) → ok
GetMMR(player_id) → mmr
UpdateMMR(player_id, delta, reason, idempotency_key) → new_mmr
AddExperience(player_id, delta, reason, idempotency_key) → level/exp/is_max
```

**关键不变量**:
- `UpdateMMR` 必须**幂等**(idempotency_key = match_id),防重复扣段位
- `AddExperience` 必须幂等;等级曲线唯一来自 `configtable/player_level_exp`(策划
  `j_玩家等级经验.xlsx`),不允许 YAML 兜底或调用方自报阈值。player 启动前校验整表与
  `players.level` 范围;生产正式数值确认前 `experience_enabled=false`。
- 配置表热更只保证单 player 进程原子切换;曲线变更需先关入账入口并完成全 fleet 收敛。
- 所有读优先走 redis 缓存(5min TTL)

---

### 2.3 data_service

**上线状态**：截至 2026-07-09 未正式上线，仅用于本地开发/minikube 验证；当前无外部调用方，
也没有需要保留的旧协议、Redis 缓存或 `player_data` 表有效数据。

**职责**:
- **玩家数据统一读写网关**(cache-aside 保证 cache + db 一致;不接 kafka)
- 缓存失效广播

**对外 RPC**:
```
ReadPlayer(player_id) → cached or db
WritePlayer(data, update_mask) → new_version  // 乐观锁;更新(version>0)必须带非空 update_mask 仅更新指定列(空掩码→ERR_INVALID_ARG,防清零新列),新建(version==0)整条 INSERT
InvalidateCache(player_id) → ok
```

**关键设计**:
- **写流程**:先写 DB 成功 → 删 cache(cache-aside);不发 kafka
- **读流程**:cache 命中返回,miss 读 db 写 cache
- **乐观锁**:`UPDATE ... WHERE version = ?`,失败让上层重试
- **部分更新**:`update_mask` 非空时只 SET 掩码列(仅 version>0 的 UPDATE 生效,INSERT 忽略掩码),
  避免滚动升级期旧调用方整条覆盖把新增列清零;掩码含 `player_id`/`version`/未知列 → `ERR_INVALID_ARG`

**为什么单独抽**:
- 玩家数据在多个服务读写(player / trade / battle_result),抽一层避免缓存不一致

---

### 2.4 friend

**职责**:好友 / 黑名单 / 拒绝列表

**对外 RPC**:
```
AddFriend(player_id, target_id) → request_id
AcceptFriend(player_id, request_id) → ok
RejectFriend(player_id, request_id) → ok
ListFriendRequests(player_id) → []FriendRequestInfo   // 待处理(收到的)请求
ListFriends(player_id) → []FriendInfo
RemoveFriend(player_id, target_player_id) → ok          // 删好友(双向,幂等)
Block(player_id, target_id) → ok
Unblock(player_id, target_player_id) → ok               // 取消拉黑(幂等)
ListBlocks(player_id) → []BlockInfo
```

**实现说明**：request_id 用 snowflake uint64（不变量 §9.11）；player_id 均以 JWT ctx 为准（R5）覆盖请求体。RejectFriend / RemoveFriend / Unblock 幂等；删好友不写黑名单(可重加)，取消拉黑不自动恢复好友关系(需重新加)。ListFriendRequests / ListBlocks 只回客户端可见结构(FriendRequestInfo / BlockInfo)，nickname 留空由客户端按 player_id 解析(§5.8)。

**2026-06-06 排期决策**（已提前）：friend 原定暂缓到最后。

**2026-06-15 实现**：按「补全 friend 模块」要求提前落地完整 Kratos 服务（第 11 个业务服）：好友图落 pandora_social（friendships / friend_requests / blocks），好友请求 / 接受经 kafka pandora.friend.event → push 推送给接收方，ListFriends 经 player_locator 填在线状态（弱依赖）。见 PROGRESS.md「社交域 ①」。

**2026-06-18 分布式好友图决策**：当前 friend 服务是**单 MySQL / 单库 `pandora_social` 方案**，`AcceptFriend` 能成立的前提是 `friend_requests`、`blocks`、`friendships` 三张表都在同一个 MySQL 实例内。当前实现依赖本地 ACID 事务：锁 `friend_requests` 行（`FOR UPDATE`）→ 同事务校验 block / 好友上限 → 标记 accepted → `INSERT IGNORE` 写双向 `friendships`。这只能保证单实例内的原子性和防 TOCTOU。

全区全服千万级玩家时，好友边会到十亿级，必须按 `player_id` 对好友图分库分表；此时 `requester` 与 `target` 的双向边大概率落在不同分片，当前跨玩家事务不再成立。把主存直接换成 Redis Cluster 也不能解决：`MULTI/EXEC` 和 Lua 只能操作同 slot key，`request:{request_id}`、`friends:{requester_id}`、`friends:{target_id}`、`block:{requester_id}`、`block:{target_id}` 会跨 slot；Redis 事务也没有逻辑回滚。因此禁止把当前 MySQL `FOR UPDATE` 事务原样搬到 Redis Cluster 或分片 MySQL。

目标形态采用 **request 单点权威 CAS + Kafka 异步幂等建边**：
- `friend_request` 是唯一权威实体，`pending -> accepted` 通过单行 / 单 key CAS 完成（MySQL `UPDATE ... WHERE status=pending` 或 Redis 单 key Lua 均可），谁 CAS 成功谁才算接受成功；重复 accept 天然幂等。
- CAS 成功后发布 `FriendshipEstablished(requester_id, target_id, request_id)` 事件，topic key 用业务实体 ID（优先 `request_id`，对齐 CLAUDE.md §9.9 的同实体有序）。
- 建边消费者分别按 owner 分片写 `requester -> target` 和 `target -> requester`，各自 `INSERT IGNORE` / `SETNX`，单分片幂等，失败重试。
- 好友上限改为软约束：建边时按 owner 本地校验，超限发补偿事件删边；或接受最终一致软上限。分片下无法强一致保证“双方同时不超限”，不引入 2PC / XA。
- block 校验也按 owner 分片本地执行；如果建边期间发现一侧已拉黑，发补偿事件清理另一侧边，补偿必须幂等。
- 好友图权威主存仍推荐分片 MySQL（按 owner `player_id` 分片），Redis 只做在线玩家 / 热好友列表缓存，不做十亿级好友边权威存储。

迁移边界：当前 W5 以内继续保留单 MySQL 事务实现；进入全服社交扩展前，必须先补 `friend` 的 outbox / 事件消费 / 补偿幂等键设计，再拆分 `AcceptFriend`。该迁移属于服务级架构改造，不允许只改存储连接串。

**2026-06-18 闭环补全**：补 RejectFriend / ListFriendRequests / RemoveFriend / Unblock / ListBlocks 五个 RPC，关闭"离线玩家无法处理请求(无待处理查询)、无法拒绝/删好友/取消拉黑"的功能缺口。RejectFriend 不向请求方推送(避免被拒尴尬)。存储切 TiDB,跨节点好友 A/B 仍由 Percolator 2PC 保证强一致(见 friend-distributed-scaling.md §8.5)。

---

### 2.5 chat

**职责**:五频道(世界 / 队伍 / 私聊 / 公会 / 临时群)

**对外 RPC**:
```
SendMessage(player_id, channel, content) → message_id
PullHistory(player_id, channel) → []ChatMessage   // 仅 PRIVATE 返历史
```

**实现**(2026-06-27 落地):
- WORLD:kafka `pandora.chat.world`(广播)
- TEAM:解析队伍成员 → 逐成员 kafka 扇出(key=接收方 player_id)
- PRIVATE:点对点 kafka + mysql `pandora_social` 落库(唯一有历史的频道)
- GUILD / GROUP:gRPC 调 guild 服务(`GuildReader.ListMembers` / `GroupReader.ListGroupMembers`)
  解析成员 → 逐成员 kafka 扇出排除发送者,**即时下发不落库**(用户「工会历史群聊不落库」)
- 发送者必须在目标队伍 / 公会 / 群内,否则 `ErrChatChannelInvalid`

**反作弊**:消息内容服务端过敏感词,长度 ≤256

---

### 2.5b guild(公会 + 临时群聊)

**职责**:公会(常驻社团)+ 临时群聊(轻量多人会话),同进程两套 RPC,社交域第三服。
**端口**:gRPC :20008 / HTTP :21008(⚠️ 20015 已被 inventory 占用,勿复用)。
**存储**:mysql `pandora_social` 强依赖(`11-guild-tables.sql`);kafka `pandora.guild.event` 弱依赖
(成员变更推送,key=接收方 player_id)。

**GuildService(13 RPC)**:
```
CreateGuild / ApplyJoin / ApproveJoin / RejectJoin / LeaveGuild / KickMember /
DisbandGuild / TransferLeader / SetOfficer / GetGuild / GetMyGuild / ListMembers / ListJoinRequests
```
角色 leader/officer/member;KickMember 分级(leader 踢任意非 leader,officer 只踢 member,
不可踢 leader / 自己);成员变更经 guild.event 推在线成员。

**GroupService(9 RPC,同进程)**:
```
CreateGroup / InviteToGroup / LeaveGroup / KickFromGroup / DisbandGroup /
TransferOwner / GetGroup / ListGroupMembers / ListMyGroups
```
owner/member 两级;InviteToGroup 幂等;owner 不能 LeaveGroup(须先 TransferOwner / Disband)。

**配置**:MaxGuildMembers(100)/ MaxGroupMembers(50)/ MaxNameLen(24,utf8 rune)。
**errcode**:`ERR_GUILD_*`(9401-9408)/ `ERR_GROUP_*`(9501-9505)。

---

### 2.6 player_locator

**职责**:**玩家当前在哪**(hub_id / battle_id)

**对外 RPC**:
```
SetLocation(player_id, location)
GetLocation(player_id) → Location
ClearLocation(player_id)
```

**Location 状态枚举**:
```
LOCATION_OFFLINE
LOCATION_LOGIN_PENDING
LOCATION_HUB { hub_pod, shard_id }
LOCATION_MATCHING { match_id }
LOCATION_BATTLE { match_id, battle_pod }
```

**状态查询与存储边界（全局不变量 §9.22 的 locator 落地）**:

- `LOCATION_STATE_HUB` / `BATTLE` 是**短期 presence / 最近活跃投影**（CLAUDE.md §9.22），不是持久归属权威，也不是永久玩家业务数据。玩家真正进入 Hub 后，只允许该 Hub DS 上报 `HUB + hub_pod + shard_id`；从 Battle 回流时额外携带 `match_id` 作为 fence，只参与原子迁移校验，通过后不持久化。player_locator 在 Redis 单键保存该投影并用 TTL 续期。Hub 心跳持续续期，玩家离开 / 断线后停止续期，key 到期才按契约推导为 `OFFLINE`。key miss 只说明 presence 不可见，**不能**单独证明玩家已离开旧 DS，也不授权进入另一台 DS；归属判定还须叠加 matchmaker 耐久 claim 与 `InspectBattleRoute` 三态门。
- **脑裂再入屏障下限（2026-07-17，`pkg/placement` 契约）**：locator TTL 是 login / matchmaker 再入门的第一道信号，`NewLocatorUsecase` 把 TTL 机械抬到 ≥ `placement.DSFenceReentryBarrier`(27s；2026-07-18 偏差余量 5→7,新增 ≥2s 服务间时钟漂移专属预留)。低于该值时，分区旧 DS（20s 授权租约 + 余量内自我 fencing）还没踢完存量玩家，presence 蒸发就会放行再入 → 一人两 DS。详见 `battle-reconnect.md` §8。
- player_locator 是 Location 的唯一查询入口。login、team、matchmaker、friend 等其他服务需要当前位置时调用 `GetLocation` / `BatchGetLocation`，不得把 `HUB` 再写入自己的 MySQL、Redis 或长期内存状态并据此做决定。允许短 TTL 展示缓存，但必须标记非权威、可失效、可重建，不能参与准入或归属迁移。
- “优先查询”指业务消费者查询 player_locator 这一统一权威索引，不是每次扫描 / 扇出查询所有 Hub DS。完全不保存位置索引会导致无法确定查询目标、Hub 故障与玩家不在无法区分，并破坏单一 Location 判定。
- key **确实不存在**可按本接口契约返回 `OFFLINE`；Redis 超时、网络错误或结果不确定必须返回 `UNKNOWN` / `UNAVAILABLE`，不得伪装成 `OFFLINE`。UNKNOWN 不能作为签票、占座、切换归属或放行进场的依据。
- 只读判断走 `GetLocation` / `BatchGetLocation`；`HUB → MATCHING → BATTLE → HUB` 等关键迁移不得“先 Get 再普通 Set”，必须在同一玩家单键上用事务 / CAS / 条件更新原子校验当前 state、实例、match_id / fence 后再写，避免并发请求同时通过检查。

**关键不变量**:
- 一个玩家**同一时刻只能在一个 Location**
- `HUB` 上报来自 hub DS,可能 stale;当前为 `MATCHING` 时拒绝覆盖。
- 当前为 `BATTLE` 时,`HUB` 回流上报必须携带刚结束战斗的 `match_id` 作 fence,且必须等于当前 `BATTLE.match_id`;通过后不持久化该 `match_id`。
- 所有 DS 上线 5s 内必须上报,否则 ds_allocator 视为僵死回收

---

### 2.7 team

**职责**:组队(5 人队)

**对外 RPC**:
```
CreateTeam(player_id) → team_id
Invite(team_id, target_player_id) → ok
AcceptInvite(player_id, team_id) → ok
LeaveTeam(team_id, player_id) → ok
Kick(team_id, target_id) → ok
SetReady(team_id, player_id, ready)
GetTeam(team_id) → Team 完整快照(只读)
GetMyTeam() → has_team_msg + Team 完整快照(只读;登录后进大厅时调一次,队伍主界面直接渲染;player_id 以 JWT 为准,查 pandora:team:player:<id> 索引;没队伍返 OK+has_team_msg=false;索引命中但队伍已过期/解散时按无队伍处理并清脏索引。带宽:一次性 unary,5 人队 ~200 字节,比拆两次 RPC 更省)
```
队伍状态变更推送走 kafka `pandora.team.update` → push 服务 server stream,**不提供** StreamTeamUpdates RPC。

**依赖约束**：Redis 是队伍权威状态强依赖；配置了 `kafka.brokers` 时，Kafka producer 也是
玩家可见邀请的启动强依赖。producer 初始化失败时 team 必须在 gRPC Ready 前退出，不能用
`pusher=nil` 接受 Invite 后静默丢通知。仅显式空 brokers 的纯 RPC 本地配置允许禁用推送。

**客户端同步约定(2026-06-15)**:
- `TeamUpdateEvent.team` 服务端已填充完整 `Team` 客户端可见快照,不是空信号;该快照来自 `TeamStorageRecord` 经 `recordToProto` 组装,不暴露存储侧字段。
- 常规队伍状态变更(`MEMBER_JOINED` / `MEMBER_LEFT` / `READY_CHANGED` / `CAPTAIN_CHANGED` / `DISBANDED` 等)客户端仍把 push 当"有变化"信号,收到后防抖合并调用 `GetMyTeam` 读取当前权威态,只在 `GetMyTeam` 回包路径写本地 `CurrentTeamSnapshot`。原因是 kafka → push → client stream 是 at-least-once 链路,可能重复、乱序或客户端处理时已过期;`GetMyTeam` 从 Redis 当前索引/队伍记录读取,并带脏索引清理逻辑,保证 UI 最终收敛到服务端权威态。
- `INVITE_SENT` 是例外:被邀请人此时还没入队,`GetMyTeam` 查不到这条邀请,客户端应直接读取 push 里的 `reason` / `invite_id` / `team` 展示邀请 UI。
- 客户端侧对 push 驱动的快照请求做短窗口防抖(当前 UE 为约 0.5s),避免多名队员同时变更或批量 push 时触发 `GetMyTeam` 请求风暴。5 人队完整 `Team` 约 200 字节,防抖后重拉一次 unary 成本很低,换来单一写入路径和抗乱序能力。

**状态机**:
```
FORMING → READY(全员 ready)→ MATCHING(进入匹配)→ IN_BATTLE → DISBANDED
```

**关键不变量**:
- 一人只能在一个队
- READY 状态下任意成员退出,自动回 FORMING
- DISBANDED 5min 后清理

---

### 2.8 matchmaker

> 模块级调用链 / 状态机 / 配置项详解见服务 README [`services/matchmaking/matchmaker/README.md`](../../services/matchmaking/matchmaker/README.md);本节为要约。

**职责**:撮合 5v5

**对外 RPC**:
```
StartMatch(team_id) → match_id
CancelMatch(match_id) → ok
ConfirmMatch(player_id, match_id, accept) → ok
GetMatchProgress(match_id) → MatchProgress   # match_id 可为 0(重连兜底)
```
> 无 `StreamMatchProgress`:go-zero zrpc 不支持 server stream;进度变化经 kafka
> `pandora.match.progress`(key=player_id)推给 Hub DS 转 UE,客户端按需 `GetMatchProgress` 拉一次。

**核心算法**:
1. 按 MMR 分段
2. 同段位优先,等待时间长 → 放宽 ±200 MMR
3. 队伍合并(2+3 / 2+2+1 / 5)
4. 凑齐 10 → 进入确认期(15s,任一人拒绝)
5. 全员确认 → 调 ds_allocator → 推 ds_addr 给玩家

**关键不变量**:
- 同一玩家只能在一个 match 队列
- 确认期内有人拒绝 → 其他人退回队列(保留排队时长)
- **`GetMatchProgress` 鉴权以 JWT player_id 为准,`match_id` 不是授权凭证**
  (不变量 §14):`match_id`/`ticket_id` 是 Snowflake、非秘密,谁拿到都能传。
  服务端必须校验 caller 在该 match/ticket 成员里,否则一律按 `ErrMatchNotFound`
  返回(不暴露他人对局存在性),杜绝外挂用任意 ID 拉别人对局的双方名单 / DS 地址。
- **重连兜底**:`match_id=0` 时服务端用 JWT player_id 反查本人当前票据
  (`GetPlayerTicket`),解决重新登录 / 换设备丢句柄后拿不到自己进度;READY 阶段
  额外为本人现签新 battle DSTicket(新 jti,sub 锁定本人)。
- **READY 推送 at-least-once(2026-07-20 事故修复)**:组队非队长成员没有 match_id,
  `pandora.match.progress` 推送是其得知 READY / Battle 落点的唯一服务端主动通道。
  机械不变量:**READY ∈ active ZSET ⟺ READY 推送交付未确认**——全员推送成功才移出
  active;READY CAS 后崩溃或推送时 Kafka 不可用,match 滞留 active,由撮合循环
  `finalizeReadyMatch` 幂等补推(每次重签全员新 jti 票据)直到交付或 match TTL 到期,
  `expireOnce`/`reconcileActiveOnce` 不得清除该表项。客户端契约:必须容忍重复 READY
  推送(幂等,coordinator 单写者保证不双 Travel)。配套:`kafka.brokers` 配置时
  producer 为**启动强依赖**(初始化失败在 Ready 前退出,与 team 同口径);UE 侧
  「本队 MATCHING 且本地无匹配归属」期间有 standby watchdog 周期触发 ResumeContext
  权威恢复,覆盖推送链路全灭的场景。

---

### 2.9 trade

**职责**:玩家间交易(两阶段)

**对外 RPC**:
```
CreateOrder(seller_id, buyer_id, items, price) → order_id
ConfirmOrder(player_id, order_id) → ok
CancelOrder(player_id, order_id) → ok
ListMyOrders(player_id) → []Order
```

**两阶段流程**:
1. seller 创建 → status=PENDING
2. buyer 看到 → 确认 → status=BUYER_CONFIRMED
3. seller 再确认 → status=SELLER_CONFIRMED → 原子扣双方资源 → status=COMPLETED
4. 任一阶段超时(5min)→ status=EXPIRED

**关键不变量**:
- 资源扣减必须**原子**(redis lua + mysql 两阶段或 saga)
- 每步都写 trade.audit topic
- 失败回滚必须有补偿幂等 key

---

### 2.10 dialogue

**职责**:NPC 对话树运行时

**对外 RPC**:
```
StartDialogue(player_id, npc_id) → DialogueState
ChooseOption(player_id, dialogue_id, option_id) → DialogueState
EndDialogue(player_id, dialogue_id) → ok
```

**对话树存储**:当前最小版本对话树内联在 `dialogue-dev.yaml`(配置驱动,`ConfigTreeProvider` 内存只读);后续接配置中心 / mysql `dialogue_trees` 表(json blob)只换 `TreeProvider` 实现,biz/service 不动。

**会话状态机**:`StartDialogue` 服务端分配 `dialogue_id` 建会话 → `ChooseOption` 按 `option_id` 推进节点 → `EndDialogue` 关闭;当前 `MemorySessionStore` 单实例内存会话(`session_ttl` 默认 5m),多实例部署改 `SessionStore` 接 Redis,biz/service 不动。

**MOBA 早期**:简单 if-else 即可,不上行为树。对话选项当前无副作用(领奖励 / 改任务等留后续接 trade / player 服务,届时在服务端权威判定 `visible` 前置条件)。

---

### 2.11 ds_allocator

**职责**:战斗 DS 调度(Agones GameServer)

**对外 RPC**:
```
AllocateBattle(match_id, player_ids, map_id, game_mode) → ds_addr + ds_pod_name
ReleaseBattle(match_id, expected allocation/pod/UID/epoch + result proof, reason) → ok
Heartbeat(request) → command           # DS 每 5s 单向 unary 主动调
ListBattles(filter) → []BattleInfo
```

**实现**:
- 调用 Agones K8s API:`GameServerAllocation` CRD。W4 ⑫ 已实现标准库 REST allocator,
  `agones.enabled=true` 时经 k8s apiserver POST `allocation.agones.dev/v1` 分配,
  `enabled=false` 时保留 Mock fallback 供本地无集群联调。
- `local_ds.enabled=true` 时可直接 exec 本机 Windows Dedicated Server 进程,与 Agones / Mock
  三种模式互斥,接口仍是同一个 `GameServerAllocator`。
- 维护 redis 中的 DS 状态镜像
- 正常结算(Model-B):DS 经 Guard+Redis active 同步 `ReportResult`；`battle_result` 把结算与
  terminal-release proof 同一 MySQL 事务落库。后台先以 `reason=completed` 建永久 Redis terminal
  墓碑并做 Kubernetes UID precondition release，再 CAS `released_at_ms`；下一轮仅以
  `reason=completed-finalize` 给 exact-proof 墓碑设 TTL，绝不再次碰 Kubernetes，成功后才删 released
  outbox。DS 的 ended/Shutdown 只改善通知体验，不再决定资源安全释放。
- Model-B 的 match-id-only `ReleaseBattle` 永久拒绝；该内部 proof RPC 只允许 battle-result mTLS
  workload identity，且不暴露在 `:8444` DS listener。legacy/off 本地配置保留旧管理兼容路径。
- 心跳超时 15s → 标记 abandoned + `alloc.Release(pod)` + 投递 `pandora.ds.lifecycle`
  给 battle_result(玩家段位回滚);投递失败留在 active 下轮重试,`BattleTTL` 是重试上界。
- 空场兜底(2026-07-06):对局活跃(ready/running)但 DS 上报 `player_count==0` 持续超过
  `empty_battle_timeout`(默认 5m,须 > 断线重连窗口 ~30s)→ 心跳内判 abandoned + 回收 +
  `command="stop"` + 同链路补偿(全员掉线未归 / 客户端从未连入,防 DS 空转烧资源)。
  主路径是 DS 侧空场计时器自结算(UE 仓库,建议 2~3 分钟),契约见 `agones-dev.md` §3.2。

---

### 2.12 hub_allocator

**职责**:大厅 DS 分片调度

**对外 RPC**:
```
AssignHub(player_id, region) → hub_ds_addr + ticket
ReleaseHub(player_id) → ok
TransferHub(player_id, target_hub_id) → new_ds_addr
ListHubs() → []HubInfo
```

**实现**:
- Hub DS Fleet 常驻 N 个 pod,每个 200~500 人上限
- 新玩家进来 → 选最空 + 同 region + 队友所在 hub
- 队友所在 hub 已满 → 加入 hub waitlist 或换 hub

**自动扩缩容策略**(2026-06-15,`hub.autoscale_enabled=true` + `agones.enabled=true` 生效):
- 走 Agones Fleet 副本控制(读/改 Fleet `spec.replicas`),不引入 FleetAutoscaler CRD
- 开服默认拉起 `hub.min_replicas`(默认 1)个大厅
- 后台 reconcile(复用 `hub.sweep_interval`)按 `desired = ceil(total_players / players_per_hub)`
  **只扩不缩**,受 `hub.max_replicas`(默认 20)上限约束(`players_per_hub` 默认 500)
- 总在线人数为 0 → 回收到 `hub.min_replicas`(空大厅自动回收)
- `AssignHub` 遇分片全满(`ErrHubNoAvailable`)→ 立即兜底 `+1` 扩容,上游重试进新大厅
- 配置项:`hub.autoscale_enabled` / `players_per_hub` / `min_replicas` / `max_replicas`

**强制整合 + 玩家迁移通知**(2026-06-15,`hub.consolidation_enabled=true` 生效):
- 不再只「空大厅自动回收」,而是低负载时**主动排空人少的大厅 + 服务端权威搬迁玩家**
- reconcile 发现 ready 分片多于负载所需(`need = ceil(total/players_per_hub)`)→ 按负载升序
  把**最空的多余分片**标 `draining` 并盖 `draining_since_ms`,逐分片每 tick 最多搬
  `consolidation_batch`(默认 50)人到同 region 最空 ready 分片,搬迁顺序镜像 TransferHub
  (占新位 → 切归属 → 退旧位)并重签 hub 票据
- **切换前提示走双通道**:
  - 通道 A:draining 分片的 Hub DS `Heartbeat` 收 `command="drain"` + `grace_seconds`(默认 30)
    → 场内 UMG 倒计时提示 → 到点重连(重连 `AssignHub` 幂等返回迁移后新分片)
  - 通道 B:后端按 `key=player_id` 推 `pandora.hub.migrate`/`HubMigrateEvent`(新地址 + 新票据 +
    倒计时)→ push 服务转发 → 客户端无缝倒计时切大厅
- 排空(`player_count=0`)且过 `migrate_grace_seconds` 后才 `RemoveShard` + 缩 Fleet 副本,
  避免提前杀 pod 打断在场玩家
- 配置项:`hub.consolidation_enabled` / `migrate_grace_seconds` / `consolidation_batch` + kafka
  producer 块;契约见 [`docs/design/agones-dev.md`](./agones-dev.md)

**关键不变量**:
- 同队伍优先在同一 hub
- 跨分片切换"先连新,后断旧",2 秒内完成

---

### 2.13 battle_result

**职责**:消费 `pandora.battle.result` topic,幂等落库

**对外 RPC**(查询用):
```
GetMatchResult(match_id) → BattleResult
ListPlayerHistory(player_id, limit) → []BattleResult
```

**核心流程**(消费者):
```
kafka msg → 验证签名 → 检查 mysql.battles WHERE match_id=? 
                      → 已存在?跳过(幂等)
                      → 不存在?事务{insert battles + insert battle_player_stats + insert player_update_outbox}
                      → ack
                      → 失败 3 次 → DLQ
后台 RunOutboxPublisher(2s):FetchOutbox(FIFO) → 投递 pandora.player.update → 成功才 DeleteOutbox
                      → 投递失败保留出箱行下轮重试(at-least-once,W4 ⑨ 不变量 §4)
```

**关键不变量**:
- **幂等键 = match_id**(unique index)
- **事务边界**:battles + stats + player_update_outbox 必须同一事务(W4 ⑨ 落库与待发布段位事件原子)
- **MMR 计算在这里**(不在 DS 算,DS 不可信)

---

## 3. 服务依赖矩阵

```
                    ┌── login
                    │     │ 验证票据
   client ──────────┤     ▼
                    │   hub DS / battle DS
                    │     │
                    └── hub_allocator / ds_allocator
                              │
                              ▼
                          team / matchmaker
                              │
                              ▼
                          player / data_service
                              │
                              ▼
                          mysql / redis / kafka
                              ▲
                              │
                          battle_result
                              ▲
                              │
                          kafka(battle.result topic)
                              ▲
                              │
                          battle DS 上报
```


## 4. W1 真正要写的服务(只写骨架)

W1 不写业务逻辑,只搭框架:

| 服务 | W1 范围 |
|---|---|
| login | main.go(Kratos)+ kratos.App 启动 + 健康检查 + 注册 etcd + 一个 mock Login RPC(返回固定票据) |
| ds_allocator | main.go + 健康检查 + Agones 客户端连接验证 + 一个 mock AllocateBattle RPC |
| hub_allocator | 同上 |
| 其它 10 个 | 只有空目录 + cmd/main.go 占位 + 注册 etcd |

W2 开始才正式写业务逻辑,顺序:

1. ✅ pkg 重写(Kratos)— W2 ①(commit 见 PROGRESS.md)
2. ✅ proto 全 buf STANDARD + 生成产物 — W2 ②⁺(commit `ee12479`)
3. ✅ **login** 骨架(Kratos 标准分层,mock 行为可联调)— W2 ③
4. ✅ **Envoy** v1.38 边缘网关(login_cluster + push_cluster + grpc_web/cors/router)— W2 ④
5. ✅ **push** 骨架(首个 server stream,5s mock tick)— W2 ⑤
6. ✅ 经 Envoy 端到端 hello world(login unary + push server stream + reflection)— W2 ⑥
7. ✅ player_locator / team / matchmaker / ds_allocator / battle_result / player 核心链路已完成到 W4 ④
8. ✅ **hub_allocator** 骨架(W4 ⑤):Mock Fleet 分片调度 + AssignHub/ReleaseHub/TransferHub/ListHubs/Heartbeat + 心跳超时扫描 + 签 hub DSTicket(接 login 待做)
9. ⏭️ login 接 hub_allocator.AssignHub(替换 mock hub_addr),补不变量 §1 的大厅入口闭环
10. 🟢 可靠补偿 / outbox:W4 ⑧ ds.lifecycle(Redis ZSET 当 outbox)+ W4 ⑨ player.update(MySQL 事务出箱)均已 at-least-once 可靠化;余真 Agones CRD / locator HUB 对账
11. ⏭️ UE 客户端 grpc-web(FHttpModule 自研解析)+ Envoy 全业务路由接入
12. ⏭️ UE Hub DS / Battle DS 骨架 + GAS / Iris / Agones 联调,打通登录→进大厅→匹配→进战斗→结算→回大厅
13. ⏭️ trade / dialogue 按 UE 主链路需要补最小版本；data_service 已完成开发实现，但尚未正式上线、尚无外部调用方
14. 🧊 chat 暂缓到最后：UE 与核心业务全部完成后再做完整实现（friend 已于 2026-06-15 提前上线，见 §2.4）

---

## 5. push 服务详细契约(2026-06-04 终版)

> ⚠️ 之前 2026-06-03 规划的 "pandora-gateway"(go-zero/gateway)已被否决,Edge Gateway 改用 **Envoy**(基础设施,不是 go 服务)。
> 之前规划的 "WebSocket pandora-push" 已被否决,改用 **gRPC server stream + Kratos**。

### 5.1 push 服务(Kratos transport/grpc + server stream)

**职责**:
- 客户端通过 Envoy 连过来,调 `PushService.Subscribe`(server stream)维持长连
- 集中持有所有在线客户端的 stream(内存索引 `player_id → grpc.ServerStream`)
- 消费多个推送 kafka topics,按 player_id 路由到对应 stream
- 离线消息缓存(redis ZSET,5min)
- 重连补推

**对外 API**(详见 `proto/pandora/push/v1/push.proto`,W2 时创建):

```proto
service PushService {
  // 客户端登录后立刻调,一直保持连接
  // 服务端通过 stream.Send(PushFrame) 持续推送 player_id 相关的所有事件
  rpc Subscribe(SubscribeRequest) returns (stream PushFrame);
}

message SubscribeRequest {
  string session_token = 1;  // JWT,Envoy 已校验,这里冗余检查
  int64  last_seen_ms  = 2;  // 重连补推用
}

message PushFrame {
  string topic      = 1;  // pandora.team.update / pandora.match.progress / ...(= 业务域,粗路由)
  bytes  payload    = 2;  // 业务 Event message 序列化(如 TeamUpdateEvent)
  int64  ts_ms      = 3;
  string trace_id   = 4;
  uint32 event_type = 5;  // 域内事件类型(细路由);0=该 topic 现有旧事件。见下「域内多事件类型路由」
}
```

#### 域内多事件类型路由(2026-07-17)

一个 `topic`(= 业务域)下要承载多种**结构不同**的事件 message(如 `pandora.team.update`
下有 `TeamUpdateEvent`,将来还会有 `TeamInviteEvent` 等)。为**不让 topic 爆炸**(拒绝一事件一
topic),用 `(topic, event_type)` 两级定位 payload 该反序列化成哪个 message:

- `topic` → 业务域(粗路由);`event_type` → 域内第几种事件(细路由)。
- **每域独立编号**:每个域定义自己的 `XxxPushEventType` enum,team 的 `1` 与 match 的 `1`
  互不相干,靠 `topic` 区分。**不需要全局 event_id 台账。** 当前 team 已启用
  `TEAM_PUSH_EVENT_TYPE_INVITE=1`；match/friend 目前仍只有 `UNSPECIFIED=0`。
  `pandora.team.v1.TeamPushEventType`、`pandora.match.v1.MatchPushEventType`、
  `pandora.friend.v1.FriendPushEventType` 是客户端已接 push 的三个域。
  邮件/交易行等域当前**无客户端 push 通道**,不涉及本路由,将来接入 push 时再加对应 enum。
- **`0` 永远 = 该 topic 现有旧事件**(team → `TeamUpdateEvent`、match → `MatchProgressEvent`、
  friend → `FriendEvent`),枚举值只增不复用。

**端到端传递路径**(与 `trace_id` 一致,走 kafka header,payload 保持裸 message、不包信封):

```
producer: PushToPlayers(..., payload) + kafka header "event_type"=N
   → kafka(topic 不变) →
push consumer: frame.EventType = header["event_type"]  →  stream.Send(PushFrame)
   → 客户端:switch (topic → 域) → switch (event_type → message)
```

**向后兼容(保护正在游戏中的老客户端,proto3 原生特性)**:

- 老 producer 不带 header → `event_type` 缺省 `0` → 天然落到「旧事件」,**老 producer 无需改**。
- 老客户端不认 `event_type` 字段 → proto3 未知字段静默忽略,仍按旧 payload 解,不崩;
  万一收到新事件也顶多字段对不上(protobuf 不崩),走客户端兜底,无害。
- 新客户端:`switch(event_type)`,`0`/未知 → 旧路径,已知新值 → 对应新 message。

**当前落地状态与滚动顺序(2026-07-20)**:

1. proto 已有 `PushFrame.event_type` 和 `TEAM_PUSH_EVENT_TYPE_INVITE=1`；push consumer 已把 Kafka
   `event_type` header 透传到 `PushFrame.EventType`。
2. team 邀请采用兼容期双发：`event_type=1` 的 `TeamInviteEvent` 给新客户端，另发
   `event_type=0` 的 legacy `TeamUpdateEvent(INVITE_SENT)` 保护旧客户端。
3. 发布必须遵守读先于写：先升级并确认 push 能透传 header，再升级 team 双发 producer，最后才
   发布只按专属邀请事件展示的新客户端。当前新客户端已发布时，恢复旧环境也必须严格按
   **push → team** 顺序升级，并验证滚动混跑窗口，不能只重启旧 team。


**实现**(Kratos 风格):
- 框架:Kratos `transport/grpc`(支持 server stream,go-zero zrpc 不支持是切换主因)
- WebSocket 库:**不用**(走标准 gRPC,不要自研 ws frame)
- kafka:`sarama` 消费推送 topics,复用 `pkg/kafkax`
- 内存索引:`sync.Map[playerID]*PushService_SubscribeServer`
- 离线消息:redis ZSET,score=ts_ms,member=encoded PushFrame
- 客户端连接:经 Envoy 转发(Envoy 处理 gRPC-Web ↔ gRPC),push 服务只看到标准 gRPC stream

**依赖**:
- 上游:Envoy(转发 gRPC-Web → gRPC stream)
- 下游:kafka(消费推送 topics)+ redis(离线消息 + 玩家在线索引)+ login(JWT 校验,可选)

**关键不变量**:
- 同一玩家同一时刻只有一条 stream(新 Subscribe 挤掉旧 stream)
- 推送至少送达一次(kafka at-least-once,客户端按 PushFrame.ts_ms 去重)
- 重连后自动补推最近 5min 离线消息
- push 重启不丢业务事件(kafka offset commit 保证)

**多实例扩展(W6+)**:
- 同一 consumer group `pandora-push`,kafka 按 partition 分配
- player_id → push_instance 索引存 redis,跨实例 gRPC 转发
- W1-W4 单实例够用,后置优化

**为什么不用自研 WebSocket envelope(2026-06-04 决策)**:
1. gRPC-Web 是 grpc.io 官方规范,Envoy 内置 grpc-web filter 转发
2. UE FHttpModule 已暴露 HTTP/2 + TLS(用户验证过源码,见 `gateway-decision.md` §3)
3. Kratos transport/grpc 原生支持 server stream,代码量比自研 WebSocket 少
4. 调试用 grpcurl 等标准工具,不用自己写 ws 调试器
5. 协议层标准化是 Pandora 铁律(大厂 / 最标准方案)

详见 `gateway-decision.md` §6 / §10。
