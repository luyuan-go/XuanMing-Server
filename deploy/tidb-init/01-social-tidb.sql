-- Pandora 社交库表结构 —— TiDB 版(friend 服务迁 TiDB,2026-06-18)
--
-- 背景:好友图扩容存储路线拍板 = (A) TiDB
--   (docs/design/friend-distributed-scaling.md §8 / §14)。
--   人工拍板「真把好友服切到 TiDB」覆盖原「不提前引入」,见 pandora-arch.md §11。
--
-- 与 deploy/mysql-init/06-social-tables.sql 的差异(只改 DDL,业务 SQL / Go 代码不变):
--   1. friendships / blocks 代理主键 id 从 AUTO_INCREMENT 改 AUTO_RANDOM —— 打散写热点
--      (§8.2:TiDB 单调主键集中写同一 Region → 热点;AUTO_RANDOM 高位随机分散)。
--      Go data 层不读 id / 不依赖 LastInsertId(friend_repo.go 全走 INSERT IGNORE +
--      player_id 查询),故改随机主键无副作用。
--   2. friend_requests / chat_private_messages 主键是显式雪花 ID(uint64,业务 ID 不变量
--      §9.11 不能改),无法用 AUTO_RANDOM。改用「主键 NONCLUSTERED + SHARD_ROW_ID_BITS
--      + PRE_SPLIT_REGIONS」:行实际按随机 _tidb_rowid 落盘,避开雪花时间序写热点;
--      代价是按 request_id / message_id 的点查多一次回表(这两表点查频率低,可接受)。
--   3. ENGINE=InnoDB / 字符集声明 TiDB 兼容接受;collation 用 utf8mb4_bin
--      (全部业务键是数值 BIGINT,大小写不敏感无意义;utf8mb4_bin 全 TiDB 版本可用,
--      避免老版本不支持 utf8mb4_0900_ai_ci)。
--
-- 跨人强一致保留:AcceptRequest 的 BEGIN / SELECT...FOR UPDATE / 多表写 / COMMIT
--   在 TiDB 悲观事务下跨节点原生可跑,maxFriends 硬上限语义不变(§8.1)。
--
-- 装载:TiDB 集群就绪后由 Codex / 人执行(见本目录 README / PROGRESS Codex 交接);
--   mysql -h <tidb-host> -P 4000 -u root < 01-social-tidb.sql
--   不进 mysql-init 自动装载流程(那条线连的是单 MySQL 容器)。

CREATE DATABASE IF NOT EXISTS `pandora_social`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_bin;

USE `pandora_social`;

-- 好友关系(双向各一行,uk player_id+friend_id,便于 ListFriends 单表查)。
-- 代理主键 id 用 AUTO_RANDOM 打散写热点;Go 侧不读 id。
CREATE TABLE IF NOT EXISTS `friendships` (
    `id`         BIGINT UNSIGNED NOT NULL AUTO_RANDOM,
    `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '关系归属玩家',
    `friend_id`  BIGINT UNSIGNED NOT NULL COMMENT '好友玩家',
    `created_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '成为好友时间(since_ms 来源)',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_friend` (`player_id`, `friend_id`),
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 好友关系(双向各一行,TiDB)';

-- 好友请求。主键是显式雪花 request_id,改 NONCLUSTERED + 行 ID 随机分片避热点。
CREATE TABLE IF NOT EXISTS `friend_requests` (
    `request_id`   BIGINT UNSIGNED NOT NULL COMMENT 'snowflake 好友请求 ID(uint64)',
    `requester_id` BIGINT UNSIGNED NOT NULL COMMENT '发起方',
    `target_id`    BIGINT UNSIGNED NOT NULL COMMENT '接收方',
    `status`       TINYINT         NOT NULL DEFAULT 1 COMMENT '1 pending / 2 accepted / 3 rejected / 4 expired',
    `created_at`   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`request_id`) /*T![clustered_index] NONCLUSTERED */,
    UNIQUE KEY `uk_requester_target` (`requester_id`, `target_id`),
    KEY `idx_target_status` (`target_id`, `status`),
    KEY `idx_status_updated` (`status`, `updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 好友请求(挂起 / 接受 / 拒绝,TiDB;终态行保留 90 天由 friend sweep 清理,§9.24)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

-- 黑名单。代理主键 id 用 AUTO_RANDOM 打散写热点。
CREATE TABLE IF NOT EXISTS `blocks` (
    `id`         BIGINT UNSIGNED NOT NULL AUTO_RANDOM,
    `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '拉黑发起方',
    `blocked_id` BIGINT UNSIGNED NOT NULL COMMENT '被拉黑玩家',
    `created_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_blocked` (`player_id`, `blocked_id`),
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 黑名单(TiDB)';

-- 好友域并发守卫行(R5 复审 P1-2/3/4,2026-07-22)。
-- TiDB 悲观事务无 gap/next-key 锁(§3.5),`COUNT(*) ... FOR UPDATE` 挡不住并发插入穿透
-- 上限(好友数/黑名单数/申请收件箱),Accept/Block/AddFriend 之间也缺少 pair 级串行化
-- (可并发形成「既好友又拉黑」「已拉黑+pending」)。guild 域用父行(guilds)/计数表锁;
-- friend 域无天然父行,引入显式守卫行:限额校验与关系变更先锁守卫行(存在行的悲观
-- 点锁在 MySQL/TiDB 语义一致),再在串行化临界区内做一致性 COUNT 与写入。
-- 行数界定(R9 复审 P1):player 守卫每玩家至多 1 行(§9.24 登记豁免,与
-- auction_owner_guards 同类);pair 守卫每关系对 1 行,随社交图 O(n²) 累积无上界 →
-- 增设 created_at 走保留期 sweep(守卫行无业务语义,任意时刻删除安全,下次 acquire 重建)。
CREATE TABLE IF NOT EXISTS `friend_player_guards` (
    `player_id` BIGINT UNSIGNED NOT NULL COMMENT '守卫行归属玩家(锁粒度=单玩家限额域)',
    PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 好友域每玩家写守卫行(TiDB 无 gap 锁,上限校验串行化;§9.24 豁免)';

CREATE TABLE IF NOT EXISTS `friend_pair_guards` (
    `lo_id` BIGINT UNSIGNED NOT NULL COMMENT '关系对较小 player_id',
    `hi_id` BIGINT UNSIGNED NOT NULL COMMENT '关系对较大 player_id',
    `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '首次取守卫时间(保留期 sweep 依据)',
    PRIMARY KEY (`lo_id`, `hi_id`),
    KEY `idx_created` (`created_at`) COMMENT '保留期 sweep 扫描索引(§9.24)'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 好友域关系对写守卫行(Accept/Block/AddFriend 同对串行化;保留期 sweep,§9.24)';

-- chat 私聊历史。主键是显式雪花 message_id,同 friend_requests 处理(NONCLUSTERED + 分片)。
-- 与好友图同库 pandora_social,迁 TiDB 时一并迁,避免拆库。
CREATE TABLE IF NOT EXISTS `chat_private_messages` (
    `message_id`   BIGINT UNSIGNED NOT NULL COMMENT 'snowflake 消息 ID(uint64)',
    `sender_id`    BIGINT UNSIGNED NOT NULL COMMENT '发送方玩家',
    `receiver_id`  BIGINT UNSIGNED NOT NULL COMMENT '接收方玩家',
    `content`      VARCHAR(512)    NOT NULL COMMENT '消息内容(服务端已校验长度 + 敏感词)',
    `send_time_ms` BIGINT          NOT NULL COMMENT '发送时间(毫秒,排序 / 翻页游标)',
    PRIMARY KEY (`message_id`) /*T![clustered_index] NONCLUSTERED */,
    KEY `idx_pair_time` (`sender_id`, `receiver_id`, `send_time_ms`),
    KEY `idx_receiver_time` (`receiver_id`, `send_time_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 私聊历史(离线 PullHistory,TiDB)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

-- ===================================================================================
-- 公会 / 临时群(guild 服务迁 social TiDB,decision-revisit-guild-scaling.md 方案 B,2026-07-13)
-- ===================================================================================
-- 与 deploy/mysql-init/11-guild-tables.sql 逻辑等价,TiDB 差异同 friend(§8.2 热点处理)再加计数列/计数表:
--   1. 雪花主键表主键 NONCLUSTERED + SHARD_ROW_ID_BITS + PRE_SPLIT_REGIONS,行按随机 _tidb_rowid
--      落盘,避开雪花时间序写热点(业务 ID 是显式 uint64 雪花,不变量 §9.11 不能用 AUTO_RANDOM)。
--   2. TiDB 无间隙锁(gap lock):原 `COUNT(*) ... FOR UPDATE` 上限校验在 TiDB 拦不住并发幻读插入
--      (§3.5)。故:
--        - 公会 pending 申请上限 → 用 `guilds.pending_request_count` 计数列(增删申请时同事务维护,
--          CreateJoinRequest 已锁 guilds 父行,读该列即串行化校验);
--        - 「我所在的群」上限 → 用 `player_group_counts` per-player 计数表(入群 / 退群同事务锁该玩家
--          计数行 + 增减);
--        - 成员数上限本就锁父行读 `member_count`(guilds / chat_groups),TiDB 安全,无需改。
--   3. `guilds.name` / `chat_groups.name` 列级 collation 显式声明 utf8mb4_0900_ai_ci,与现网单 MySQL
--      (deploy/mysql-init/11-guild-tables.sql)一致,保「大小写 / 口音不敏感」的重名判定不变(§5.1);
--      其余数值键表 / 列用库默认 utf8mb4_bin。TiDB v8.5 原生支持 utf8mb4_0900_ai_ci。

CREATE TABLE IF NOT EXISTS `guilds` (
    `guild_id`              BIGINT UNSIGNED NOT NULL COMMENT 'snowflake 公会 ID(uint64)',
    `name`                  VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '公会名(唯一,大小写/口音不敏感,与现网 MySQL collation 一致)',
    `leader_id`             BIGINT UNSIGNED NOT NULL COMMENT '会长 player_id',
    `member_count`          INT             NOT NULL DEFAULT 1 COMMENT '成员数(含会长)',
    `pending_request_count` INT             NOT NULL DEFAULT 0 COMMENT '挂起加入申请数(TiDB 无间隙锁,pending 上限校验走此计数列)',
    `max_members`           INT             NOT NULL DEFAULT 100 COMMENT '成员上限',
    `created_at`            DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间(created_ms 来源)',
    PRIMARY KEY (`guild_id`) /*T![clustered_index] NONCLUSTERED */,
    UNIQUE KEY `uk_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 公会(TiDB)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS `guild_members` (
    `player_id` BIGINT UNSIGNED NOT NULL COMMENT '成员 player_id(单归属 → 主键)',
    `guild_id`  BIGINT UNSIGNED NOT NULL COMMENT '所属公会',
    `role`      TINYINT         NOT NULL DEFAULT 3 COMMENT '1 leader / 2 officer / 3 member',
    `joined_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '加入时间(joined_ms 来源)',
    PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */,
    KEY `idx_guild_role` (`guild_id`, `role`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 公会成员(单归属,TiDB)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS `guild_join_requests` (
    `request_id` BIGINT UNSIGNED NOT NULL COMMENT 'snowflake 申请 ID(uint64)',
    `guild_id`   BIGINT UNSIGNED NOT NULL COMMENT '目标公会',
    `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '申请人',
    `status`     TINYINT         NOT NULL DEFAULT 1 COMMENT '1 pending / 2 approved / 3 rejected',
    `created_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`request_id`) /*T![clustered_index] NONCLUSTERED */,
    UNIQUE KEY `uk_guild_player` (`guild_id`, `player_id`),
    KEY `idx_guild_status` (`guild_id`, `status`),
    KEY `idx_status_updated` (`status`, `updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 公会加入申请(挂起 / 通过 / 拒绝,TiDB;终态行保留 90 天由 guild sweep 清理,§9.24)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS `chat_groups` (
    `group_id`     BIGINT UNSIGNED NOT NULL COMMENT 'snowflake 群 ID(uint64)',
    `name`         VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL COMMENT '群名',
    `owner_id`     BIGINT UNSIGNED NOT NULL COMMENT '群主 player_id',
    `member_count` INT             NOT NULL DEFAULT 1 COMMENT '成员数(含群主)',
    `max_members`  INT             NOT NULL DEFAULT 50 COMMENT '成员上限',
    `created_at`   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间(created_ms 来源)',
    PRIMARY KEY (`group_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 临时群(TiDB)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS `chat_group_members` (
    `group_id`  BIGINT UNSIGNED NOT NULL COMMENT '所属群',
    `player_id` BIGINT UNSIGNED NOT NULL COMMENT '成员 player_id',
    `role`      TINYINT         NOT NULL DEFAULT 2 COMMENT '1 owner / 2 member',
    `joined_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '加入时间(joined_ms 来源)',
    PRIMARY KEY (`group_id`, `player_id`) /*T![clustered_index] NONCLUSTERED */,
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 临时群成员(多归属,TiDB)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

-- per-player「我所在的群」计数表(TiDB 无间隙锁,取代 COUNT(*)...FOR UPDATE 的 max_groups_per_player
-- 校验,§3.5)。入群 / 退群同事务里锁该玩家计数行 + 增减 group_count;无对应父聚合行,故独立成表。
-- player_id 是显式雪花,主键 NONCLUSTERED + 分片避热点。
CREATE TABLE IF NOT EXISTS `player_group_counts` (
    `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '玩家 player_id',
    `group_count` INT             NOT NULL DEFAULT 0 COMMENT '该玩家当前所在临时群数(max_groups_per_player 校验用)',
    PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 玩家所在群计数(TiDB 安全上限校验)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

-- ===================================================================================
-- 邮件(mail 服务迁 social TiDB,2026-07-21)
-- ===================================================================================
-- 与 deploy/mysql-init/12-mail-tables.sql 逻辑等价,TiDB 差异同上(§8.2 热点处理):
--   1. 显式雪花主键表(sys_mail / guild_mail / player_mail,mail_id 是 uint64 雪花,不变量 §9.11
--      不能用 AUTO_RANDOM)→ 主键 NONCLUSTERED + SHARD_ROW_ID_BITS + PRE_SPLIT_REGIONS,行按随机
--      _tidb_rowid 落盘,避开雪花时间序写热点;代价是按 mail_id 点查多一次回表(可接受)。
--   2. player_mail_cursor(player_id PK)/ player_mail_claim(player_id+mail_id PK)同法处理。
--   3. collation 用 utf8mb4_bin:邮件表无大小写敏感字符串键(payload 是 BLOB,其余键全 BIGINT),
--      与本文件其余表一致;mysql-init 版的 utf8mb4_0900_ai_ci 仅是库默认,语义无差。
-- 拉取式游标 / 写扩散 / 附件领取幂等的事务语义(mail_repo.go)在 TiDB 悲观事务下原生可跑。
-- mail 读 guild_members 判玩家所属公会:guild 表已同库同迁 TiDB,依赖一致。

CREATE TABLE IF NOT EXISTS `sys_mail` (
    `mail_id`    BIGINT UNSIGNED NOT NULL COMMENT 'snowflake 系统邮件 ID(uint64,channel 内递增)',
    `start_ms`   BIGINT          NOT NULL DEFAULT 0 COMMENT '生效起 ms(0 立即)',
    `end_ms`     BIGINT          NOT NULL DEFAULT 0 COMMENT '失效止 ms(0 永不过期)',
    `payload`    BLOB            NOT NULL COMMENT 'MailContentStorageRecord 序列化(标题/正文/附件)',
    `created_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`mail_id`) /*T![clustered_index] NONCLUSTERED */,
    KEY `idx_end` (`end_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 系统邮件(全服一份,登录拉取,TiDB)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS `guild_mail` (
    `mail_id`    BIGINT UNSIGNED NOT NULL COMMENT 'snowflake 公会邮件 ID(uint64,channel 内递增)',
    `guild_id`   BIGINT UNSIGNED NOT NULL COMMENT '所属公会',
    `start_ms`   BIGINT          NOT NULL DEFAULT 0,
    `end_ms`     BIGINT          NOT NULL DEFAULT 0,
    `payload`    BLOB            NOT NULL COMMENT 'MailContentStorageRecord 序列化',
    `created_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`mail_id`) /*T![clustered_index] NONCLUSTERED */,
    KEY `idx_guild` (`guild_id`, `mail_id`),
    KEY `idx_end` (`end_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 公会邮件(每公会一份,成员拉取,TiDB)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS `player_mail` (
    `mail_id`    BIGINT UNSIGNED NOT NULL COMMENT 'snowflake 个人邮件 ID(uint64)',
    `player_id`  BIGINT UNSIGNED NOT NULL COMMENT '收件人',
    `status`     TINYINT         NOT NULL DEFAULT 1 COMMENT '1 unread / 2 read / 3 claimed',
    `claimed`    TINYINT         NOT NULL DEFAULT 0 COMMENT '附件是否已领',
    `expire_ms`  BIGINT          NOT NULL DEFAULT 0 COMMENT '过期 ms(0 永不过期)',
    `payload`    BLOB            NOT NULL COMMENT 'MailContentStorageRecord 序列化',
    `created_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`mail_id`) /*T![clustered_index] NONCLUSTERED */,
    KEY `idx_player_status` (`player_id`, `status`),
    KEY `idx_expire` (`expire_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 个人邮件收件箱(写扩散,离线可达,TiDB)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS `player_mail_cursor` (
    `player_id`          BIGINT UNSIGNED NOT NULL COMMENT '玩家',
    `last_sys_mail_id`   BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '系统邮件已读到的最大 id',
    `last_guild_mail_id` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '公会邮件已读到的最大 id',
    `updated_at`         DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 系统/公会邮件拉取游标(watermark,TiDB)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

CREATE TABLE IF NOT EXISTS `player_mail_claim` (
    `player_id`      BIGINT UNSIGNED NOT NULL COMMENT '领取人',
    `mail_id`        BIGINT UNSIGNED NOT NULL COMMENT '被领邮件(任意 channel)',
    `claimed`        TINYINT         NOT NULL DEFAULT 1 COMMENT '1=终态已领 0=DS 领取意图(bag phase 2)',
    `intent_payload` BLOB            NULL COMMENT 'DS 领取意图(pb MailClaimIntentStorageRecord;直连链为 NULL)',
    `claimed_at`     DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`, `mail_id`) /*T![clustered_index] NONCLUSTERED */,
    KEY `idx_mail` (`mail_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 邮件附件领取幂等 + DS 领取意图(player_id+mail_id 唯一,TiDB)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;

-- player_mail_archive:**2026-07-27 补建 —— 此前整张表漏在 TiDB 版之外**。
-- mail 已经在 TiDB 上跑(tools/scripts/run_services.ps1 用 mail-dev-tidb.yaml 启动),而
-- mail_repo.go:479 的 ArchiveAndDeletePersonal 在**同一个事务**里
-- `INSERT IGNORE INTO player_mail_archive ...` —— 表不存在 → 1146 → 整批事务回滚 →
-- 保留期 sweep 永远卡在同一批,过期邮件删不掉(未领附件的归档补偿链也断)。
-- 与 deploy/mysql-init/12-mail-tables.sql:93 逻辑等价;archived_at 索引供 §9.24 保留期清理
-- (dbcheck 登记 player_mail_archive → idx_archived)。
CREATE TABLE IF NOT EXISTS `player_mail_archive` (
    `mail_id`     BIGINT UNSIGNED NOT NULL COMMENT '原个人邮件 ID(snowflake uint64)',
    `player_id`   BIGINT UNSIGNED NOT NULL COMMENT '收件人',
    `status`      TINYINT         NOT NULL DEFAULT 1 COMMENT '归档时的状态',
    `expire_ms`   BIGINT          NOT NULL DEFAULT 0 COMMENT '原过期 ms',
    `created_ms`  BIGINT          NOT NULL DEFAULT 0 COMMENT '原创建 ms',
    `payload`     BLOB            NOT NULL COMMENT 'MailContentStorageRecord 序列化',
    `archived_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '归档时间(保留期清理列)',
    PRIMARY KEY (`mail_id`) /*T![clustered_index] NONCLUSTERED */,
    KEY `idx_player` (`player_id`),
    KEY `idx_archived` (`archived_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='Pandora 个人邮件归档(未领附件先归档再删收件箱,90 天保留期,TiDB)'
  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=4;
