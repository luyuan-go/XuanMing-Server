-- player_no 改名的 expand compatibility 阶段（2026-08-12）。
--
-- 000005/000006 已发布且 immutable，其中 000006 曾删除 legacy 三件套，违反滚动升级
-- Stable/Canary 共存要求。本迁移只向前恢复并长期保留 register_no 列、唯一索引与计数器表；
-- 不做 DROP/RENAME。新 login 从本版起同时锁两张 counter、取最大水位并双写两列；旧 login
-- 仍可使用 register_no 三件套。contract 只能在所有旧二进制退场并经过独立发布窗后另立版本。

SET @has_player_no_col := (
    SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='accounts' AND COLUMN_NAME='player_no');
SET @has_register_no_col := (
    SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='accounts' AND COLUMN_NAME='register_no');

SET @ddl_add_player_no := IF(
    @has_player_no_col = 0,
    'ALTER TABLE `accounts` ADD COLUMN `player_no` BIGINT UNSIGNED NULL COMMENT ''角色编号新名(expand 双写期;纯展示,禁作身份键/外键/幂等键)'' AFTER `status`',
    'SELECT 1');
PREPARE stmt_v7_add_player_no FROM @ddl_add_player_no;
EXECUTE stmt_v7_add_player_no;
DEALLOCATE PREPARE stmt_v7_add_player_no;

SET @ddl_add_register_no := IF(
    @has_register_no_col = 0,
    'ALTER TABLE `accounts` ADD COLUMN `register_no` BIGINT UNSIGNED NULL COMMENT ''角色编号旧名(expand 双写期兼容 Stable;禁止删除)'' AFTER `player_no`',
    'SELECT 1');
PREPARE stmt_v7_add_register_no FROM @ddl_add_register_no;
EXECUTE stmt_v7_add_register_no;
DEALLOCATE PREPARE stmt_v7_add_register_no;

SET @ddl_add_player_no_uk := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='accounts' AND INDEX_NAME='uk_player_no') = 0,
    'ALTER TABLE `accounts` ADD UNIQUE KEY `uk_player_no` (`player_no`)',
    'SELECT 1');
PREPARE stmt_v7_add_player_no_uk FROM @ddl_add_player_no_uk;
EXECUTE stmt_v7_add_player_no_uk;
DEALLOCATE PREPARE stmt_v7_add_player_no_uk;

SET @ddl_add_register_no_uk := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='accounts' AND INDEX_NAME='uk_register_no') = 0,
    'ALTER TABLE `accounts` ADD UNIQUE KEY `uk_register_no` (`register_no`)',
    'SELECT 1');
PREPARE stmt_v7_add_register_no_uk FROM @ddl_add_register_no_uk;
EXECUTE stmt_v7_add_register_no_uk;
DEALLOCATE PREPARE stmt_v7_add_register_no_uk;

CREATE TABLE IF NOT EXISTS `player_no_counter` (
    `id` TINYINT UNSIGNED NOT NULL,
    `next_no` BIGINT UNSIGNED NOT NULL COMMENT '下一个待发角色编号',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 角色编号全局发号计数器(新名;expand 双锁双写)';

CREATE TABLE IF NOT EXISTS `register_no_counter` (
    `id` TINYINT UNSIGNED NOT NULL,
    `next_no` BIGINT UNSIGNED NOT NULL COMMENT '下一个待发角色编号(旧名兼容)',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 角色编号全局发号计数器(旧名兼容;expand 双锁双写)';

-- 先拒绝同角色/跨角色值冲突，不能猜测哪一列是权威。
SET @player_no_expand_data_conflicts := (
    SELECT COUNT(*) FROM `accounts` a
     WHERE (a.`player_no` IS NOT NULL AND a.`register_no` IS NOT NULL
            AND a.`player_no` <> a.`register_no`)
        OR (a.`player_no` IS NOT NULL AND EXISTS (
              SELECT 1 FROM `accounts` b
               WHERE b.`player_id` <> a.`player_id` AND b.`register_no`=a.`player_no`))
        OR (a.`register_no` IS NOT NULL AND EXISTS (
              SELECT 1 FROM `accounts` b
               WHERE b.`player_id` <> a.`player_id` AND b.`player_no`=a.`register_no`)));
SET @guard_player_no_expand_data := IF(
    @player_no_expand_data_conflicts=0,
    'SELECT 1',
    'SELECT * FROM `__pandora_player_no_expand_data_conflict__`');
PREPARE stmt_v7_guard_data FROM @guard_player_no_expand_data;
EXECUTE stmt_v7_guard_data;
DEALLOCATE PREPARE stmt_v7_guard_data;

UPDATE `accounts` SET `register_no` = `player_no`
 WHERE `register_no` IS NULL AND `player_no` IS NOT NULL;
UPDATE `accounts` SET `player_no` = `register_no`
 WHERE `player_no` IS NULL AND `register_no` IS NOT NULL;

-- 两张表只能有 id=1；其它行无法安全自动合并，先 fail-closed。
SET @player_no_expand_counter_conflicts := (
    SELECT (SELECT COUNT(*) FROM `player_no_counter` WHERE `id`<>1)
         + (SELECT COUNT(*) FROM `register_no_counter` WHERE `id`<>1));
SET @guard_player_no_expand_counter := IF(
    @player_no_expand_counter_conflicts=0,
    'SELECT 1',
    'SELECT * FROM `__pandora_player_no_expand_counter_conflict__`');
PREPARE stmt_v7_guard_counter FROM @guard_player_no_expand_counter;
EXECUTE stmt_v7_guard_counter;
DEALLOCATE PREPARE stmt_v7_guard_counter;

INSERT IGNORE INTO `player_no_counter` (`id`,`next_no`)
SELECT `id`,`next_no` FROM `register_no_counter` WHERE `id`=1;
INSERT IGNORE INTO `register_no_counter` (`id`,`next_no`)
SELECT `id`,`next_no` FROM `player_no_counter` WHERE `id`=1;
UPDATE `player_no_counter` p JOIN `register_no_counter` r ON p.`id`=r.`id`
   SET p.`next_no`=GREATEST(p.`next_no`,r.`next_no`);
UPDATE `register_no_counter` r JOIN `player_no_counter` p ON p.`id`=r.`id`
   SET r.`next_no`=GREATEST(p.`next_no`,r.`next_no`);
