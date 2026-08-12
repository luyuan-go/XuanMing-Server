-- 000006_equipment_instance_id — 出战装备预设从配置级升级为精确唯一实例。
--
-- 纯 additive(§9.21 expand → migrate → contract):
--   1. instance_id 允许 NULL，使滚动升级期旧 player 二进制仍可省略该列写入；
--   2. 新二进制只写非 0 instance_id，GetEquipment 将 NULL 映射成 0 供玩家看见并重选；
--   3. GetLoadout 对 NULL/0 旧行 fail-closed，不再把无法证明归属的配置级预设转成战斗效果；
--   4. 无法安全回填：同一 item_config_id 可能有多件不同词条实例，数据库不能替玩家猜哪件。
--
-- uk_player_instance 只在单玩家内唯一：实例转移后，旧玩家可能暂留一条陈旧预设，新玩家仍需
-- 能选中同一 instance_id；GetLoadout 的实时 exact ownership 复核会拒绝旧玩家的陈旧行。

SET @ddl := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_equipment' AND COLUMN_NAME = 'instance_id') = 0,
    'ALTER TABLE `player_equipment` ADD COLUMN `instance_id` BIGINT UNSIGNED NULL COMMENT ''唯一装备实例 ID(uint64);NULL 仅兼容 000006 前旧预设,新写必须非空'' AFTER `item_config_id`, ALGORITHM=INSTANT',
    'SELECT 1');
PREPARE add_equipment_instance_id FROM @ddl;
EXECUTE add_equipment_instance_id;
DEALLOCATE PREPARE add_equipment_instance_id;

SET @ddl := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_equipment' AND INDEX_NAME = 'uk_player_instance') = 0,
    'ALTER TABLE `player_equipment` ADD UNIQUE KEY `uk_player_instance` (`player_id`, `instance_id`), ALGORITHM=INPLACE',
    'SELECT 1');
PREPARE add_equipment_instance_unique FROM @ddl;
EXECUTE add_equipment_instance_unique;
DEALLOCATE PREPARE add_equipment_instance_unique;
