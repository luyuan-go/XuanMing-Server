# decision-revisit:聊天系统补全公会聊天 + 临时群聊

> 状态:**已落地,实现完成**(2026-06-27 拍板通过并实现)。提出人:Claude(Opus)/ 2026-06-27
> 落地说明:guild 服务(GuildService + GroupService 同进程,20008/21008)已实现并接入运行/部署/边缘入口
> (run_services.ps1 / gen_cluster_config.ps1 / docker-compose.services.yml / k8s / prometheus / envoy)。
> 后续如需调整以本文档为设计依据,代码改动同步更新本节状态。
> 触发:用户确认「聊天系统除世界 / 局内(队伍)/ 好友(私聊)外,**还要公会聊天 + 临时群聊,两个都全量实现**」。
> 关联代码:[chat biz](../../services/social/chat/internal/biz/chat.go)、[chat.proto](../../proto/pandora/chat/v1/chat.proto)、
> [chat team_reader](../../services/social/chat/internal/data/team_reader.go)、[friend 服务](../../services/social/friend/)
> 关联规范:[`CLAUDE.md`](../../CLAUDE.md) §5(proto 四类结构 / ID 规则)、§9 不变量(#9 kafka key=实体 ID、#14 客户端只拿可见结构)、
> [`AGENTS.md`](../../AGENTS.md) §7(大决策先写文档再拍板)、[infra.md](./infra.md) §4(topic)、[go-services.md](./go-services.md) §2.5(chat)、
> [friend-distributed-scaling.md](./friend-distributed-scaling.md)(同类「跨人成员关系 vs 分片」推演)。
>
> 本文设计已**拍板并落地**;下文设计推演保留作为实现依据与历史记录,后续改动以此为基线。

---

## 1. 旧问题:聊天只有三个频道,且没有"群成员"维度

当前 [chat.proto](../../proto/pandora/chat/v1/chat.proto) `ChatChannel` 只有:

| 频道 | 枚举 | 语义 | 成员来源 | 持久化 |
|---|---|---|---|---|
| 世界 | `CHAT_CHANNEL_WORLD = 1` | 全服广播 | 无(Broadcast) | 否 |
| 队伍 | `CHAT_CHANNEL_TEAM = 2` | 5 人组队小队 | gRPC 调 team 服务 | 否 |
| 私聊 | `CHAT_CHANNEL_PRIVATE = 3` | 两人 | 无(点对点) | MySQL(离线 PullHistory) |
| 系统 | `CHAT_CHANNEL_SYSTEM = 4` | 服务端→客户端 | — | 否 |

`chat` 服务的 fan-out 机制已成型(见 [chat.go](../../services/social/chat/internal/biz/chat.go)):
**解析成员名单 → 逐个发 kafka `pandora.chat.*` → push 服务投递**。队伍频道就是靠注入 `TeamReader`(gRPC 调 team)拿成员。

**缺口**:公会聊天 / 临时群聊都需要一个"谁在这个群里"的**成员名单来源**,但:
- **公会系统完全不存在**(`infra.md` §2.1 `pandora_social` 注释「公会(后期)」;leaderboard 只是预留了 `GUILD` scope 给未来公会服务消费)。
- **群组(临时群)存储也不存在**。

所以这不是"给 chat 加个频道"——而是要**先有公会实体 + 群组实体的成员管理**,聊天才有得 fan-out。

---

## 2. 新需求拆成两块,本质不同

| 维度 | 公会聊天(GUILD) | 临时群聊(GROUP) |
|---|---|---|
| 群组生命周期 | **长期**(公会持久存在,玩家加入退出) | **临时**(玩家自建,可解散;TTL 或显式销毁) |
| 成员规模 | 大(几十~几百) | 小(默认 ≤ 50,可配) |
| 成员关系 | 玩家**只属一个公会**(单归属) | 玩家**可在多个群**(多归属) |
| 管理 | 创建 / 申请 / 审批 / 踢人 / 职位(会长/官员/成员) | 建群 / 拉人 / 退群 / 群主转让 / 解散 |
| 持久化 | 公会信息 + 成员 + 聊天历史(可选) | 群信息 + 成员 + 聊天历史(可选) |
| 归属服务 | **新建 guild 服务** | **新建 group 服务**(或并入 guild 服务同进程) |

### 2.1 范围界定(本轮只做"支撑聊天"所需,不做完整公会玩法)

公会作为完整玩法(公会战 / 公会副本 / 公会银行 / 贡献度 / 公告栏 / 等级)是另一个大系统。
**本轮只实现支撑"公会聊天 + 临时群聊"的最小完整闭环**:

- **guild 服务**:创建公会、申请/审批加入、退出、踢人、解散、查公会、查成员、(会长/官员/成员三级职位,用于"谁能踢人/审批")。
- **group 服务**:建群、拉人入群、退群、群主转让、解散群、查群、查成员。
- **chat 集成**:`GUILD` / `GROUP` 两个新频道 + 对应 MemberReader,复用现有 kafka→push fan-out。

公会玩法(战/副本/银行等)留待后续 `docs/design/guild-*.md` 专文,不在本轮。

---

## 3. 关键设计决策

### 3.1 服务编排:新建 1 个 social 子服务进程承载 guild + group

- 在 `services/social/guild/` 新建 Kratos 服务(对齐 friend / chat 目录形态),**同一进程内同时提供 `GuildService` + `GroupService`**(两套 RPC,共用 MySQL 连接与 snowflake)。
  - 理由:group 是"轻量临时群",独立起进程不划算;guild / group 同属社交成员关系,放一个进程减少端口 / 部署面。两者 proto 分两个 package(`pandora.guild.v1` / `pandora.group.v1`),互不耦合,未来要拆进程也容易。
- 端口:沿用 `infra.md` §6 规划新增(见 §7 端口表),**不 ad-hoc 起端口**。

### 3.2 成员存储:MySQL `pandora_social`,结构化列(对齐 friend / chat)

不引入 proto bytes blob(`CLAUDE.md` §5.9 关系型表不强制),新增四张表(DDL 见 §6):
- `guilds` / `guild_members`(公会单归属:`guild_members.player_id` UNIQUE)
- `groups` / `group_members`(群多归属:uk `group_id+player_id`)

owner 维度按 `guild_id` / `group_id`,W5 内单 MySQL 实例;全服千万级再谈分片(届时另写 decision,参照 friend-distributed-scaling)。

### 3.3 聊天 fan-out:复用现有机制,加两个频道 + 两个 MemberReader

chat biz 新增 `sendGuild` / `sendGroup`,与 `sendTeam` 同构:
1. `target_id` = guild_id / group_id。
2. 经新增 `GuildReader` / `GroupReader`(gRPC 调 guild 服务)解析成员名单。
3. 校验发送者在群内 → 逐个发 kafka(排除自己,推送原则 2)→ push 投递。
4. 弱依赖:reader / pusher 为 nil 时降级(返回 message_id 让客户端本地回显,记 warn),与 TEAM 完全一致。

> **聊天历史**:公会 / 群聊默认是**即时频道,不落库**(与世界 / 队伍一致),`PullHistory` 不返回历史。
> 若后续要群聊历史,再加表(`chat_guild_messages` / `chat_group_messages`),不阻塞本轮。MVP 不做。

### 3.4 频道枚举(proto 字段编号:append-only,不复用)

`ChatChannel` 当前最大值是 `SYSTEM=4`。新增:
```proto
CHAT_CHANNEL_GUILD = 5;  // 公会频道(target_id = guild_id)
CHAT_CHANNEL_GROUP = 6;  // 临时群频道(target_id = group_id)
```
客户端可发频道白名单从 `{WORLD,TEAM,PRIVATE}` 扩为 `{WORLD,TEAM,PRIVATE,GUILD,GROUP}`(`SYSTEM` 仍禁发)。

### 3.5 ID 规则(`CLAUDE.md` §5.5 Snowflake 一律 uint64)

`guild_id` / `group_id` / `guild_request_id` / `invite_id` 均 **Snowflake uint64**;
职位 / 状态用 proto enum(int32 语义,不因非负改 uint32,§5.7)。

### 3.6 不变量对齐

- **#9 kafka key = 业务实体 ID**:公会 / 群聊推送 topic key = **接收方 player_id**(同 `pandora.chat.team`,保证同一接收方有序)。
- **#14 客户端只拿可见结构**:RPC / push 只回 `Guild` / `GuildMember` / `Group` / `GroupMember` 等"客户端可见结构",不直接返回 `*StorageRecord` / DB 整行。昵称留空由客户端按 player_id 解析(§5.8 最小数据单位)。
- **单归属公会**:`guild_members.player_id` UNIQUE 强制玩家只在一个公会(类比不变量"玩家只在一个 DS")。

---

## 4. 公会职位与权限(最小三级)

| 职位 enum | 值 | 权限 |
|---|---|---|
| `GUILD_ROLE_LEADER` | 1 | 解散公会 / 审批申请 / 踢任意成员 / 任命官员 / 转让会长 |
| `GUILD_ROLE_OFFICER` | 2 | 审批申请 / 踢普通成员 |
| `GUILD_ROLE_MEMBER` | 3 | 仅聊天 / 退会 |

群组只有 `GROUP_ROLE_OWNER` / `GROUP_ROLE_MEMBER`(群主可拉人 / 踢人 / 解散 / 转让;成员可聊天 / 退群)。

---

## 5. RPC 清单(草案)

### 5.1 GuildService(`pandora.guild.v1`)
```
CreateGuild(player_id, name) → guild_id              // 创建者成为 LEADER;已在公会则 ErrGuildAlreadyInGuild
ApplyJoinGuild(player_id, guild_id) → request_id     // 申请加入(挂起)
ApproveJoin(approver_id, request_id) → ok            // LEADER/OFFICER 审批,成员上限校验
RejectJoin(approver_id, request_id) → ok
LeaveGuild(player_id) → ok                            // 退会;LEADER 退会需先转让或解散
KickMember(operator_id, target_id) → ok              // 权限校验(LEADER 踢任意 / OFFICER 踢 MEMBER)
DisbandGuild(leader_id) → ok                          // 仅 LEADER;清成员
TransferLeader(leader_id, target_id) → ok
SetOfficer(leader_id, target_id, is_officer) → ok
GetGuild(guild_id) → Guild
ListMembers(guild_id) → []GuildMember                 // 供 chat GuildReader / 客户端
ListJoinRequests(guild_id) → []GuildJoinRequest       // LEADER/OFFICER 看挂起申请
GetMyGuild(player_id) → Guild|null                    // 玩家当前公会
```

### 5.2 GroupService(`pandora.group.v1`)
```
CreateGroup(owner_id, name, member_ids[]) → group_id  // 建群,可带初始成员(≤上限)
InviteToGroup(operator_id, group_id, player_id) → ok  // 拉人(OWNER 或允许成员拉,可配)
LeaveGroup(player_id, group_id) → ok
KickFromGroup(owner_id, group_id, target_id) → ok
DisbandGroup(owner_id, group_id) → ok
TransferOwner(owner_id, group_id, target_id) → ok
GetGroup(group_id) → Group
ListGroupMembers(group_id) → []GroupMember            // 供 chat GroupReader
ListMyGroups(player_id) → []Group                     // 玩家所在群
```

### 5.3 chat 集成
- `SendMessage(channel=GUILD, target_id=guild_id, content)` → fan-out 公会成员。
- `SendMessage(channel=GROUP, target_id=group_id, content)` → fan-out 群成员。

---

## 6. 表结构(DDL 草案,`pandora_social`)

```sql
-- 公会
CREATE TABLE guilds (
  guild_id     BIGINT UNSIGNED NOT NULL,           -- snowflake
  name         VARCHAR(64)     NOT NULL,
  leader_id    BIGINT UNSIGNED NOT NULL,
  member_count INT             NOT NULL DEFAULT 1,
  created_at   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (guild_id),
  UNIQUE KEY uk_name (name)
);
CREATE TABLE guild_members (
  player_id  BIGINT UNSIGNED NOT NULL,             -- 单归属 → 唯一
  guild_id   BIGINT UNSIGNED NOT NULL,
  role       TINYINT         NOT NULL DEFAULT 3,   -- 1 leader / 2 officer / 3 member
  joined_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (player_id),
  KEY idx_guild_role (guild_id, role)
);
CREATE TABLE guild_join_requests (
  request_id BIGINT UNSIGNED NOT NULL,             -- snowflake
  guild_id   BIGINT UNSIGNED NOT NULL,
  player_id  BIGINT UNSIGNED NOT NULL,
  status     TINYINT         NOT NULL DEFAULT 1,   -- 1 pending / 2 approved / 3 rejected
  created_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (request_id),
  UNIQUE KEY uk_guild_player (guild_id, player_id),
  KEY idx_guild_status (guild_id, status)
);

-- 临时群(多归属)
CREATE TABLE chat_groups (
  group_id   BIGINT UNSIGNED NOT NULL,             -- snowflake
  name       VARCHAR(64)     NOT NULL,
  owner_id   BIGINT UNSIGNED NOT NULL,
  member_count INT           NOT NULL DEFAULT 1,
  created_at DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (group_id)
);
CREATE TABLE chat_group_members (
  group_id   BIGINT UNSIGNED NOT NULL,
  player_id  BIGINT UNSIGNED NOT NULL,
  role       TINYINT         NOT NULL DEFAULT 2,   -- 1 owner / 2 member
  joined_at  DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (group_id, player_id),
  KEY idx_player (player_id)
);
```
> 表名 `chat_groups`(非 `groups`):`groups` 在部分 SQL 方言是保留字,避免转义麻烦。

---

## 7. infra 增量(topic / 端口 / 错误码)

### 7.1 kafka topic(对齐 `pandora.chat.team` 口径,key = 接收方 player_id)
| Topic | 分区 | 保留 | 生产者 | 消费者 | 备注 |
|---|---|---|---|---|---|
| `pandora.chat.guild` | 8 | 1h | chat | push | 公会聊天推送(key=player_id) |
| `pandora.chat.group` | 8 | 1h | chat | push | 临时群聊推送(key=player_id) |
| `pandora.guild.event` | 4 | 1d | guild | push | 公会申请/审批/踢人/解散通知(key=player_id) |

### 7.2 端口(已落地)
| 服务 | gRPC | metrics |
|---|---|---|
| guild(含 group) | 20008 | 21008 |

> chat 现有端口 20005 不变;push 现有端口 20014 不变。20015/21015 已被 inventory 占用,不得复用。

### 7.3 错误码(`pkg/errcode`,沿 9000-9999 社交段)
```
ErrChat: 9001-9003(已有)+ 新增 GUILD/GROUP 频道复用 ErrChatChannelInvalid
ErrGuild: 9401-9408 — NotFound / AlreadyInGuild / Full / NotLeader / NoPermission / NameTaken / RequestInvalid / NotMember
ErrGroup: 9501-9505 — NotFound / Full / NotOwner / NotMember / AlreadyIn
```

---

## 8. 风险与边界

1. **proto 跨仓库同步**(`CLAUDE.md` §5):chat.proto 改 + 新增 guild.proto / group.proto,commit 标 `[proto]`,cpp pb 同步 UE `Source/Pandora/Generated/Proto/`(由 Codex 执行 proto_gen.ps1)。
2. **proto_gen / 起 TiDB / 装表由 Codex/人执行**(`AGENTS.md` §11.1):Claude 写 .proto / .sql / Go,不自己跑环境收尾。
3. **单归属 vs 多归属**别写反:公会单归属(player_id PK),群多归属(group_id+player_id)。
4. **LEADER 退会 / 解散**边界:LEADER 直接 LeaveGuild 应拒绝(ErrGuildNotLeader 语义:需先 TransferLeader 或 DisbandGuild),否则公会无主。
5. **成员上限**:guild 默认上限可配(如 100),group 默认 50;ApproveJoin / InviteToGroup 时校验 member_count。
6. **跨人一致性**:guild/group 成员写都是 owner(guild_id/group_id)单键操作,无跨人事务,**不撞** friend 那套跨人强一致难题(单 MySQL 事务足够)。

---

## 9. 验收标准

- [x] chat.proto 加 `GUILD=5` / `GROUP=6`,客户端可发白名单更新,`SendMessage(GUILD/GROUP)` fan-out 成员、排除自己、弱依赖降级。
- [x] guild 服务:创建/申请/审批/退会/踢人/解散/转让/任命/查公会/查成员/查申请 全部带单测,单归属强制。
- [x] group 服务:建群/拉人/退群/踢人/解散/转让/查群/查成员 全部带单测,多归属,成员上限校验。
- [x] 新增四表 DDL 进 `deploy/mysql-init/`,topic 进 `infra.md` §4,端口 / 错误码登记。
- [x] `go build ./...` + `go test ./...` + lint 通过;proto 已生成 Go pb。
- [x] PROGRESS.md 追加本轮记录;go-services.md §2.5 / 服务表更新。

---

## 10. 已决问题

1. **端口**:使用 `20008/21008`,因为 `20015/21015` 已被 inventory 占用。
2. **服务形态**:guild + group 同进程,proto package 独立。
3. **公会 / 群聊历史**:本轮不落库,即时频道;只有 PRIVATE 保留离线历史。
4. **成员上限**:guild 100 / group 50,配置可调。
5. **公会名称**:唯一,DDL 保留 `uk_name`。
