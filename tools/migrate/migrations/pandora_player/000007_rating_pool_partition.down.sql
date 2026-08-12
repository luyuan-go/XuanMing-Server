-- 回滚段位分区。注意:回滚会**丢弃分池段位分**(单列装不下多份),只恢复列结构;
-- 段位数据本身无法从多份还原成一份(该选哪一份是产品决策),故不尝试回填。
SET @has_mmr := (SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'players' AND COLUMN_NAME = 'mmr');
SET @stmt := IF(@has_mmr = 0,
    "ALTER TABLE `players` ADD COLUMN `mmr` INT NOT NULL DEFAULT 1500 COMMENT '段位分,floor 0' AFTER `exp`, ALGORITHM=INSTANT",
    'SELECT 1');
PREPARE s FROM @stmt; EXECUTE s; DEALLOCATE PREPARE s;

SET @has_pool := (SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mmr_history' AND COLUMN_NAME = 'rating_pool');
SET @stmt := IF(@has_pool > 0, 'ALTER TABLE `mmr_history` DROP COLUMN `rating_pool`, ALGORITHM=INSTANT', 'SELECT 1');
PREPARE s FROM @stmt; EXECUTE s; DEALLOCATE PREPARE s;

DROP TABLE IF EXISTS `player_mmr`;
