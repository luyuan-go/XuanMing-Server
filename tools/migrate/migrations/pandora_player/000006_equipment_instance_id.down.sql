-- 000006_equipment_instance_id 回滚。
--
-- 必须先把 player 服务回滚到不引用 instance_id 的旧版本并排空新副本，再执行本 down。
-- 只删除本迁移新增的索引和列；player_equipment 的旧 slot/item_config_id 预设仍保留。

ALTER TABLE `player_equipment` DROP INDEX `uk_player_instance`, ALGORITHM=INPLACE;
ALTER TABLE `player_equipment` DROP COLUMN `instance_id`, ALGORITHM=INSTANT;
