-- 账号身份 / 角色身份分离(expand 阶段,2026-08-18)。
--
-- 背景:现状是「1 账号 = 1 player_id = 1 角色」——accounts 的 PK 就是 player_id,
-- 一个值同时充当账号身份与角色身份。本迁移把两者拆开,为「一账号多角色」以及
-- 「卖角色 = 角色过户(role → account 归属行)」打地基:
--   - accounts.account_id:新的**账号身份**(snowflake,login 铸)。
--   - account_roles:**角色归属台账**。player_id 下沉为角色实体 ID,account_id 是归属列;
--     角色过户 = 原子改这一列,全仓以 player_id 为键的业务数据零迁移。
--
-- expand 纪律(§9.21 滚动升级 Stable/Canary 共存):
--   - accounts 的 PK 与 player_id 列一律不动,旧 login 二进制照常工作(它不认识新列/新表)。
--   - account_id 可空:旧二进制注册的账号会留 NULL,由新 login 在下次登录时补铸。
--     MySQL 唯一索引允许多个 NULL,故留空的行之间不会互相冲突。
--   - contract(改 PK、删 accounts.player_id)必须等所有旧二进制退场并经过独立观测窗后,
--     另立版本执行;本版一律只加不减。
--
-- 角色名(account_roles.role_name):创建角色功能上线前,注册时固定取账号名。
--   它**不是**显示名权威——显示名权威仍是 pandora_player.players.nickname(uk_nickname
--   全局唯一)。本列的用途只有两个:
--     ① login 播种 players.nickname 时的取值来源;
--     ② 选角列表在 player 服务不可达时的降级兜底名。
--   刻意**不加唯一键**:唯一性只能有一个权威点。两个库各自加唯一键,必然出现
--   「account_roles 写进去了、players 那边被 uk_nickname 拒」的分叉,且无法自动收敛。

-- ===== 1) accounts 加账号身份列 =====

SET @has_account_id_col := (
    SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='accounts' AND COLUMN_NAME='account_id');

SET @ddl_add_account_id := IF(
    @has_account_id_col = 0,
    'ALTER TABLE `accounts` ADD COLUMN `account_id` BIGINT UNSIGNED NULL COMMENT ''账号身份 ID(snowflake);NULL=旧二进制注册尚未补铸'' AFTER `player_id`',
    'SELECT 1');
PREPARE stmt_v8_add_account_id FROM @ddl_add_account_id;
EXECUTE stmt_v8_add_account_id;
DEALLOCATE PREPARE stmt_v8_add_account_id;

SET @ddl_add_account_id_uk := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='accounts' AND INDEX_NAME='uk_account_id') = 0,
    'ALTER TABLE `accounts` ADD UNIQUE KEY `uk_account_id` (`account_id`)',
    'SELECT 1');
PREPARE stmt_v8_add_account_id_uk FROM @ddl_add_account_id_uk;
EXECUTE stmt_v8_add_account_id_uk;
DEALLOCATE PREPARE stmt_v8_add_account_id_uk;

-- 1:1 回填。现状每个账号恰好一个角色,故直接取 player_id 作 account_id 是安全的:
-- 两者同属一个 snowflake 空间,复用同一个值不会与任何**将来**铸出的 account_id 撞
-- (将来的值是新铸的 snowflake,不会重复发同一个 ID)。
--
-- 先 fail-closed:若已存在非空 account_id 且与 player_id 不一致,说明这套库已经被
-- 别的路径写过账号身份,不能猜哪个是权威,直接让迁移失败(引用不存在的表制造硬错)。
SET @account_id_backfill_conflicts := (
    SELECT COUNT(*) FROM `accounts`
     WHERE `account_id` IS NOT NULL AND `account_id` <> `player_id`);
SET @guard_account_id_backfill := IF(
    @account_id_backfill_conflicts = 0,
    'SELECT 1',
    'SELECT * FROM `__pandora_account_id_backfill_conflict__`');
PREPARE stmt_v8_guard_backfill FROM @guard_account_id_backfill;
EXECUTE stmt_v8_guard_backfill;
DEALLOCATE PREPARE stmt_v8_guard_backfill;

UPDATE `accounts` SET `account_id` = `player_id` WHERE `account_id` IS NULL;

-- ===== 2) 角色归属台账 =====

CREATE TABLE IF NOT EXISTS `account_roles` (
    `player_id`     BIGINT UNSIGNED  NOT NULL COMMENT '角色实体 ID(snowflake);全仓业务数据都以它为键',
    `account_id`    BIGINT UNSIGNED  NOT NULL COMMENT '归属账号 ID;角色过户 = 原子改本列',
    `slot`          TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '账号内角色槽位,0 起;与 account_id 组成唯一键',
    `role_name`     VARCHAR(64)      NOT NULL COMMENT '角色名;创建角色功能上线前 = 账号名。非显示名权威(见文件头)',
    `status`        TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '0=normal,1=deleted(软删,过户/找回需要留痕)',
    `last_login_at` DATETIME              NULL COMMENT '该角色最近一次进入游戏;选角界面默认选中最近登录的那个。NULL=从未进过',
    `created_at`    DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`    DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`),
    UNIQUE KEY `uk_account_slot` (`account_id`, `slot`),
    KEY `idx_account_status` (`account_id`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 角色归属台账(账号 → 角色;角色过户改 account_id)';

-- 回填:每个既有账号补一行 slot=0 的角色,角色名取账号名。
-- INSERT IGNORE 保证可重跑;已存在的行(比如新 login 已经写过)不被覆盖。
INSERT IGNORE INTO `account_roles` (`player_id`, `account_id`, `slot`, `role_name`, `status`, `created_at`)
SELECT a.`player_id`, a.`account_id`, 0, a.`account`, 0, a.`created_at`
  FROM `accounts` a
 WHERE a.`account_id` IS NOT NULL;
