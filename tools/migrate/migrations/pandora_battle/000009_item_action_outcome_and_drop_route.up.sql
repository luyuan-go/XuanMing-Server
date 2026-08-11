-- 000009_item_action_outcome_and_drop_route
--
-- 1) consume/discard 增加 durable outcome 与 phase0 每场同 item pickup 支出余额；
-- 2) battle_drop_outbox 在首次入箱时冻结 stack/instance 路由，配置热更不改变重试路径。

-- fresh-init 的 05-battle-outbox.sql 已含两列；存量库才在线补列。
SET @ddl := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'battle_drop_outbox' AND COLUMN_NAME = 'stack_item_config_ids') = 0,
    'ALTER TABLE `battle_drop_outbox` ADD COLUMN `stack_item_config_ids` VARCHAR(512) NOT NULL DEFAULT '''' COMMENT ''首次入箱时冻结的可堆叠路由;发布重试不得按热配置重算'' AFTER `item_config_ids`, ALGORITHM=INSTANT',
    'SELECT 1');
PREPARE add_drop_stack_route FROM @ddl;
EXECUTE add_drop_stack_route;
DEALLOCATE PREPARE add_drop_stack_route;

SET @ddl := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'battle_drop_outbox' AND COLUMN_NAME = 'instance_item_config_ids') = 0,
    'ALTER TABLE `battle_drop_outbox` ADD COLUMN `instance_item_config_ids` VARCHAR(512) NOT NULL DEFAULT '''' COMMENT ''首次入箱时冻结的装备实例路由;发布重试不得按热配置重算'' AFTER `stack_item_config_ids`, ALGORITHM=INSTANT',
    'SELECT 1');
PREPARE add_drop_instance_route FROM @ddl;
EXECUTE add_drop_instance_route;
DEALLOCATE PREPARE add_drop_instance_route;

-- action count 不再用重复 CSV 表示：真实 max_stack 可大于 VARCHAR(512) 能容纳的约
-- 46 个十位 ID。单个 item_config_id 留在原列，count 紧凑存本列。
SET @ddl := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'battle_progress_outbox' AND COLUMN_NAME = 'item_count') = 0,
    'ALTER TABLE `battle_progress_outbox` ADD COLUMN `item_count` INT UNSIGNED NOT NULL DEFAULT 0 COMMENT ''kind=4/5:紧凑 action count;0=兼容旧重复CSV'' AFTER `item_config_ids`, ALGORITHM=INSTANT',
    'SELECT 1');
PREPARE add_progress_item_count FROM @ddl;
EXECUTE add_progress_item_count;
DEALLOCATE PREPARE add_progress_item_count;

-- v9 前的已持久行来自“装备白名单→GrantInstances”契约。按历史语义固化为 instance，
-- 不能用迁移时的热配置重新分类，否则旧 key 已成功但回包丢失的行可能换路由双发。
UPDATE `battle_drop_outbox`
SET `instance_item_config_ids` = `item_config_ids`
WHERE `stack_item_config_ids` = '' AND `instance_item_config_ids` = '';

CREATE TABLE IF NOT EXISTS `battle_progress_item_balance` (
    `match_id`       BIGINT UNSIGNED NOT NULL,
    `player_id`      BIGINT UNSIGNED NOT NULL,
    `item_config_id` INT UNSIGNED    NOT NULL,
    `picked_count`   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    `spent_count`    BIGINT UNSIGNED NOT NULL DEFAULT 0,
    `updated_at_ms`  BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`match_id`, `player_id`, `item_config_id`),
    CONSTRAINT `chk_battle_progress_item_balance` CHECK (`spent_count` <= `picked_count`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora phase0 每场玩家同配置 stack pickup/支出权威余额';

CREATE TABLE IF NOT EXISTS `battle_progress_action` (
    `match_id`       BIGINT UNSIGNED NOT NULL,
    `seq`            BIGINT UNSIGNED NOT NULL,
    `player_id`      BIGINT UNSIGNED NOT NULL,
    `kind`           TINYINT UNSIGNED NOT NULL COMMENT '4=consume_stack 5=discard_stack',
    `item_config_id` INT UNSIGNED    NOT NULL,
    `count`          INT UNSIGNED    NOT NULL,
    `status`         TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '0=pending 1=succeeded 2=failed',
    `result_code`    INT             NOT NULL DEFAULT 0 COMMENT 'failed 时稳定回放的 pandora ErrCode',
    `created_at_ms`  BIGINT          NOT NULL DEFAULT 0,
    `updated_at_ms`  BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`match_id`, `seq`, `player_id`, `kind`),
    KEY `idx_progress_action_match_player` (`match_id`, `player_id`, `seq`),
    CONSTRAINT `chk_battle_progress_action_status` CHECK (`status` IN (0,1,2))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 战斗消费/丢弃同步完成结果(响应丢失可重放)';
