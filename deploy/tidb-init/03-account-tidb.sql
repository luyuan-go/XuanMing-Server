-- Pandora 账号库表结构 —— TiDB 版(全服单点扩容,2026-07-27)
--
-- 背景:pandora_account 是**全服共享单点**——全国所有玩家的登录都写同一实例:
--   ① player_session_generations:每次 Login 事务内 upsert(generation+1),写 QPS = 全服登录 QPS;
--   ② account_devices:每次登录一次 upsert(TouchDevice);
--   ③ accounts:仅注册时 INSERT,但开服首日是雪花 ID 单调递增的写入尖峰。
-- Go 服务(login)本身无状态可水平扩,加副本只摊分 CPU / bcrypt,**摊不了这个库**。
-- dev 单机 MySQL 联调仍用 deploy/mysql-init/02-account-tables.sql(同构 DDL)。
--
-- 与 mysql-init 版的差异(只改 DDL 与存储属性,业务 SQL / Go 代码不变):
--   1. 雪花主键(accounts / player_roles / player_session_generations)→ NONCLUSTERED +
--      SHARD_ROW_ID_BITS + PRE_SPLIT_REGIONS 打散时间序写热点。代价是按 player_id 点查多一次
--      回表;login 的高频读本就走 uk_account(FindByAccount),行也极小,可接受。
--   2. 代理主键 AUTO_INCREMENT → AUTO_RANDOM(account_devices / account_bans)。
--      TiDB 的 AUTO_INCREMENT 按 TiDB Server 缓存分段分配,**跨节点非单调**,继续用它既有热点
--      又会让"按 id 单调"的假设失效。已核对 Go 侧不读 LastInsertId、不依赖 id 顺序
--      (TouchDevice 丢弃 Exec 结果;PurgeStaleDevices 只按 last_login_at 删)。
--   3. ⚠️ **collation 保持 utf8mb4_0900_ai_ci,不跟随 01-social-tidb.sql / 02-owner-tidb.sql
--      改用 utf8mb4_bin** —— 那两个库的业务键是数值 / uuid,大小写不敏感与否无意义;本库不同:
--        · accounts.account 是**客户端上报的账号名**且带唯一键 uk_account。改 _bin 会让
--          "Player1" 与 "player1" 变成两个不同账号:存量玩家可能登不进(大小写不同即查无此人),
--          同时开出"大小写变体抢注同名账号"的口子。
--        · account_devices.device_id 同为客户端上报且参与 uk_player_device,改 _bin 会让同一
--          设备的大小写变体各占一行(设备行重复,清理与"最近登录设备"语义漂移)。
--      即:本库的字符串比较语义是**业务正确性的一部分**,必须与单机 MySQL 逐字节等价。
--
-- ⚠️ TiDB 版本与 collation 框架前置条件(不满足会**静默**改变上面的语义,不报错):
--   · utf8mb4_0900_ai_ci 自 **TiDB v7.4.0** 起支持;更早版本会回退到 utf8mb4_bin。
--   · 大小写不敏感真正生效还要求集群启用了「新 collation 框架」
--     (new_collations_enabled_on_first_bootstrap,自 v6.0.0 起默认 true)。该配置
--     **只在集群首次 bootstrap 时生效、之后不可更改** —— 老集群或显式关过的集群上,
--     TiDB 只在语法上接受 _ci,语义上按 binary 比较,即 uk_account 变成大小写敏感。
--   · 因此不能只靠"部署时看一眼版本":login 启动期用行为探针 fail-fast 断言
--     (见 pkg/mysqlx.AssertCaseInsensitiveCollation),证不了就拒启,不允许带着
--     静默语义漂移上线(§16 隐蔽 bug / CLAUDE.md 验收底线第 3 条 fail-closed 优先)。
--   本目录的 docker-compose.tidb.yml 固定 pingcap/tidb:v8.5.1(≥7.4 且默认新框架),满足前置。

CREATE DATABASE IF NOT EXISTS `pandora_account`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

USE `pandora_account`;

-- accounts:账号身份。写=注册(低频,但开服首日尖峰且雪花 PK 单调 → 打散)。
-- 读=每次登录一次 FindByAccount(走 uk_account)。
CREATE TABLE IF NOT EXISTS `accounts` (
    `player_id`     BIGINT UNSIGNED  NOT NULL,
    `account`       VARCHAR(64)      NOT NULL,
    `password_hash` VARCHAR(80)      NOT NULL COMMENT 'bcrypt(client_digest),含 cost 前缀,固定 60 字节',
    `status`        TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '0=normal,1=banned,2=disabled',
    `register_no`   BIGINT UNSIGNED       NULL COMMENT '注册编号(展示专用,禁作身份键/外键;NULL=待补号,login 补号任务异步分配,register-no-and-login-surge.md §3.3)',
    `created_at`    DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`    DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */,
    UNIQUE KEY `uk_account` (`account`),
    UNIQUE KEY `uk_register_no` (`register_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 4
  COMMENT='Pandora 账号身份表(uk_account 必须大小写不敏感,见文件头)';
-- uk_register_no 是补号任务的顺序追加写(索引尾部),但写速率 = 补号批量节奏(每 5s 若干批),
-- 远低于登录 QPS,不构成需要打散的热点;不比照雪花 PK 做 SHARD 处理。

-- register_no_counter:注册编号全局发号计数器,恒 1 行(id=1)。
-- 补号事务先 FOR UPDATE 锁本行再批量编号:行锁即全局互斥,多 login 副本并发安全。
-- 单行表无热点/无分布可言,不做任何 SHARD/AUTO_RANDOM 处理;§9.24 登记豁免(权威闸,不清理)。
CREATE TABLE IF NOT EXISTS `register_no_counter` (
    `id`      TINYINT UNSIGNED NOT NULL,
    `next_no` BIGINT UNSIGNED  NOT NULL COMMENT '下一个待发注册编号',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 注册编号全局发号计数器(单行 id=1;发号权威闸)';

-- account_devices:每次登录一次 upsert(TouchDevice),写 QPS = 全服登录 QPS。
-- 真实写路径走 uk_player_device(按 player_id 天然打散,无尾部热点);代理主键仅占位 → AUTO_RANDOM。
CREATE TABLE IF NOT EXISTS `account_devices` (
    `id`            BIGINT UNSIGNED  NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,
    `player_id`     BIGINT UNSIGNED  NOT NULL,
    `device_id`     VARCHAR(128)     NOT NULL,
    `last_login_at` DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `last_login_ip` VARCHAR(45)      NOT NULL DEFAULT '' COMMENT 'IPv4/IPv6 字符串',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_device` (`player_id`, `device_id`),
    KEY `idx_device` (`device_id`),
    KEY `idx_last_login` (`last_login_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 账号设备绑定与最近登录(90 天保留期清理,§9.24)';
-- idx_last_login 服务保留期清理(DELETE WHERE last_login_at<cutoff LIMIT n)。
-- TiDB 上 DELETE ... LIMIT 同样是有界批删;单批 500(login main.go)远低于单事务限制。

-- account_bans:运营封禁,量级 = 运营操作数(§9.24 登记豁免,不清理)。
CREATE TABLE IF NOT EXISTS `account_bans` (
    `id`         BIGINT UNSIGNED  NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,
    `player_id`  BIGINT UNSIGNED       NULL COMMENT 'NULL 表示该 ban 仅按 device 维度',
    `device_id`  VARCHAR(128)          NULL COMMENT 'NULL 表示该 ban 仅按 player 维度',
    `reason`     VARCHAR(255)     NOT NULL DEFAULT '',
    `banned_at`  DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `expires_at` DATETIME              NULL COMMENT 'NULL=永久',
    PRIMARY KEY (`id`),
    KEY `idx_player_active`  (`player_id`,  `expires_at`),
    KEY `idx_device_active`  (`device_id`,  `expires_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 账号 / 设备 封禁记录';

-- player_roles:选角权威(每玩家 1 行,SelectRole 覆盖式 upsert)。
-- SetRole 在同事务内对 player_session_generations 做 FOR UPDATE 代际复核 —— 两张表都是
-- **按主键点锁已存在行**,不依赖间隙锁,TiDB 悲观事务下语义等价(详见 02-owner-tidb.sql 同款结论)。
CREATE TABLE IF NOT EXISTS `player_roles` (
    `player_id`  BIGINT UNSIGNED NOT NULL,
    `role_id`    INT UNSIGNED    NOT NULL COMMENT 'CfgRole.Id,配置表角色 ID',
    `updated_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 4
  COMMENT='Pandora 玩家已选角色(选角权威数据)';

-- player_session_generations:**本库写压力最高的表** —— 每次成功登录都在事务内
-- 「SELECT ... FOR UPDATE → upsert(generation+1) → 读回 → COMMIT」。
-- 行数被玩家数有界(每玩家 1 行),但写 QPS = 全服登录 QPS,这两件事要分开看:
-- 行数有界 ≠ 写压力有界,它恰恰是本库迁 TiDB 的主因。
CREATE TABLE IF NOT EXISTS `player_session_generations` (
    `player_id`  BIGINT UNSIGNED NOT NULL,
    `sess_jti`   VARCHAR(64)     NOT NULL COMMENT '当前会话 JWT 的 jti(uuid v4)',
    `generation` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '登录单调代际(每次 Login upsert +1,并发登录定序权威)',
    `updated_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 4
  COMMENT='Pandora 玩家会话代际(登录定序 + SetRole fencing;每玩家 1 行,写 QPS=全服登录 QPS)';
