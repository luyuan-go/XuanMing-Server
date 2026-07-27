# Pandora TiDB 初始化(好友图迁 TiDB)

好友图扩容存储路线拍板 = **(A) TiDB**(`docs/design/friend-distributed-scaling.md` §8 / §14)。
本目录是 friend(及同库 chat)迁 TiDB 的 schema,**与单 MySQL 的
`deploy/mysql-init/` 是两条独立线**,不互相覆盖。

> 2026-07-13 起 `01-social-tidb.sql` 追加公会 / 临时群表(guild 服务迁 social TiDB 的上线前代码
> 路径,`docs/design/decision-revisit-guild-scaling.md §6.1`):`guilds` / `guild_members` /
> `guild_join_requests` / `chat_groups` / `chat_group_members` + 计数表 `player_group_counts`。
> TiDB 无间隙锁,pending 申请 / 所在群上限改用计数列(`guilds.pending_request_count`)/ 计数表
> (`player_group_counts`),不再依赖 `COUNT(*)...FOR UPDATE`。guild 连 TiDB 用
> `services/social/guild/etc/guild-dev-tidb.yaml`(opt-in;运行默认仍单 MySQL)。

## 文件

- `01-social-tidb.sql` —— `pandora_social` 库表的 TiDB 版 DDL(已做 §8.2 雪花主键热点处理;
  含 friend/chat + guild/group + mail 全部同库表)。
- `02-owner-tidb.sql` —— `pandora_owner` 每玩家 owner 权威(§9.22,生产强制 TiDB)。
- `03-account-tidb.sql` —— `pandora_account` 登录账号 / 会话代际 / 选角(全服单点扩容,2026-07-27)。
- `../docker-compose.tidb.yml` —— 本地 TiDB 集群（PD + TiKV + TiDB，单副本，与单 MySQL 并存）。
- `../../tools/scripts/tidb_up.ps1` —— 一键起集群 + 建账号 + 装载**本目录全部** DDL。

> 2026-07-27 修正两处会静默出错的漂移:
> ① `tidb_up.ps1` 此前**硬编码只装载 `01-social-tidb.sql`**,`02-owner-tidb.sql` 从未被这条链路
> 装载过;现改为遍历本目录按文件名排序逐个装载(每份 DDL 都自包含,可重复执行)。
> ② `01-social-tidb.sql` **漏了整张 `player_mail_archive` 表**和四个 sweep 索引
> (`sys_mail`/`guild_mail` 的 `idx_end`、`player_mail` 的 `idx_expire`、`player_mail_claim` 的
> `idx_mail`)。而 mail 已经在 TiDB 上跑(`run_services.ps1` 用 `mail-dev-tidb.yaml`),
> `mail_repo.go` 的 `ArchiveAndDeletePersonal` 在同事务里写归档表 → 1146 → 整批回滚 →
> 保留期 sweep 永远卡在同一批。缺索引则让 §9.24 清理走全表扫描,且 `dbcheck` 门禁判红。

## 一条命令起（推荐）

```pwsh
pwsh tools/scripts/tidb_up.ps1          # 起集群 + 建 pandora 账号 + 装载本目录全部 DDL
```

装载后各服务连 TiDB:

```pwsh
login   --conf services/account/login/etc/login-dev-tidb.yaml
friend  --conf services/social/friend/etc/friend-dev-tidb.yaml
guild   --conf services/social/guild/etc/guild-dev-tidb.yaml
chat    --conf services/social/chat/etc/chat-dev-tidb.yaml
mail    --conf services/social/mail/etc/mail-dev-tidb.yaml
```

## ⚠️ 生产前置条件:collation 框架必须在建集群时开启(事后不可改)

**这一条比任何 DDL 都优先,配错只能重建集群。**

本目录多处使用列级 `utf8mb4_0900_ai_ci` 保「大小写 / 口音不敏感」的业务语义:
`guilds.name`、`chat_groups.name`(重名判定)、以及 `pandora_account` 全库
(尤其 `accounts.account` —— 客户端上报的账号名 + 唯一键,而 Go 侧对账号串**零归一化**)。

它依赖两个条件:

| 条件 | 要求 | 不满足时的表现 |
|---|---|---|
| TiDB 版本 | ≥ **v7.4.0** | 更早版本**静默回退**到 `utf8mb4_bin`,不报错 |
| 新 collation 框架 | 集群首次 bootstrap 时 `new_collations_enabled_on_first_bootstrap = true`(自 v6.0.0 起默认 true) | TiDB 只在**语法上**接受 `_ci`,**语义上按 binary 比较**,不报错 |

第二个参数**只在集群首次 bootstrap 生效,建完集群改不了**。用 TiUP / Operator 建生产集群时
如果没显式开,后果是:老玩家用与注册时大小写不同的账号名登录会「查无此人」,并且可以用大小写
变体抢注同名账号 —— 而且**全程不报错**,只在真实玩家登录时暴露。

本地 `docker-compose.tidb.yml` 固定 `pingcap/*:v8.5.1`(≥7.4 且默认开新框架),满足前置。
生产建集群后请先自查:

```sql
SELECT VERSION();
SELECT VARIABLE_VALUE FROM mysql.tidb WHERE VARIABLE_NAME='new_collation_enabled';  -- 必须 True
SELECT 'A' COLLATE utf8mb4_0900_ai_ci = 'a', 'a ' COLLATE utf8mb4_0900_ai_ci = 'a'; -- 必须 1, 0
```

不必只靠人工自查:`login` 在 `require_tidb: true`(-Prod 产物机械注入)时会在**启动期**用
`pkg/mysqlx.AssertColumnCollationSemantics` 对 `accounts.account` 做同样的行为探针,
不符即 fail-fast 拒启(§16 隐蔽 bug / 验收底线第 3 条 fail-closed 优先)。

停：`pwsh tools/scripts/tidb_up.ps1 -Down`（加 `-Volumes` 清数据）。

诊断：`docker compose -p pandora-tidb -f deploy/docker-compose.tidb.yml ps`。脚本固定使用
`pandora-tidb` 作为 Compose project name，避免与 `deploy/docker-compose.dev.yml` 的默认
`deploy` project 混在一起。

## 落地状态(2026-06-18)

| 部分 | 谁做 | 状态 |
|---|---|---|
| TiDB 版 DDL(热点调优) | Claude | ✅ 本目录 |
| friend 服务 TiDB 连接配置 | Claude | ✅ `services/social/friend/etc/friend-dev-tidb.yaml` |
| Go 业务代码改动 | —— | ✅ 零改动(TiDB 兼容 MySQL 协议,§8.1) |
| TiDB compose + 一键起脚本（含建账号 / 装载 DDL） | Claude | ✅ `docker-compose.tidb.yml` + `tidb_up.ps1` |
| 起 TiDB 集群（跑 `tidb_up.ps1`，拉镜像） | **Codex / 人** | ✅ Codex 已跑通（2026-06-18） |
| 单 MySQL → TiDB 数据迁移(如已有数据) | **Codex / 人** | ⏳ 待办 |

## Codex 实跑结果(2026-06-18)

- `pwsh tools/scripts/tidb_up.ps1` 已成功起 `pd` / `tikv` / `tidb` 并装载 DDL。
- `SHOW TABLES` 已确认 `blocks` / `chat_private_messages` / `friend_requests` / `friendships` 存在。
- friend 使用 `friend-dev-tidb.yaml` 连 `127.0.0.1:4000` 启动成功。
- grpcurl 以 `x-pandora-player-id` 模拟 Envoy 鉴权注入，验收 `AddFriend` / `AcceptFriend` /
  `ListFriends` / `Block` 通过；TiDB 回查确认 accepted 请求、双向好友边、拉黑后删边均符合预期。
- 验收结束后已清理 1001/1002 测试数据，`friend_requests` / `friendships` / `blocks` 为空。

## Codex / 人 交接步骤

**首选一键脚本**：`pwsh tools/scripts/tidb_up.ps1`（自动完成下面 1~3）。

手动等价步骤（脚本不可用时）：

1. 起 TiDB 集群：`docker compose -p pandora-tidb -f deploy/docker-compose.tidb.yml up -d`
   （或 `tiup playground`；生产用 TiUP / Operator）。默认 TiDB Server 端口 `4000`。
2. 建账号并授权 `pandora_social`（对齐 dsn 里的 `pandora` 用户）。
3. 装载 schema：`mysql -h 127.0.0.1 -P 4000 -u root < deploy/tidb-init/01-social-tidb.sql`。
4. （如已有单 MySQL 数据）用 Dumpling + Lightning（或 DM）迁移，在线双写灰度。
5. friend 服务改用 `friend-dev-tidb.yaml` 启动验证。

## TiDB 与 Redis 边界

本目录只解决 `pandora_social` 的存储扩容和跨人强一致过渡,**不代表上 TiDB 后可以删除 Redis**。

- TiDB / 分片:替代手工分库分表、应用侧路由和单 MySQL 容量瓶颈。
- Redis:继续负责热点读挡板、极低延迟临时状态和专用数据结构(session / locator / leaderboard / auction book 等)。
- 对 friend / chat:TiDB 降低数据层扩容复杂度;是否额外加 Redis 缓存仍按 `docs/design/read-cache-strategy.md` 的读热度、重复读命中率、共享度判据决策。

## TiDB 必知代价(§8.2)

- 雪花单调主键写热点:`friend_requests` / `chat_private_messages` 已用
  `NONCLUSTERED PK + SHARD_ROW_ID_BITS + PRE_SPLIT_REGIONS` 打散;
  `friendships` / `blocks` 代理主键用 `AUTO_RANDOM`。
- 跨节点 2PC 热路径延迟;PD + TiKV + TiDB Server 运维成本重一个量级。
