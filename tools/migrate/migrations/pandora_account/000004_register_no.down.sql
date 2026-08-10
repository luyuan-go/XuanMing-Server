-- 回滚注册编号(register_no):删计数器表 + 条件删索引/列(幂等)。
-- ⚠️ 回滚会丢弃已分配的全部编号;重新 up 后补号任务会按 created_at+player_id 重编,
-- 只要期间没有账号行被删,重编结果与原编号一致(全序确定性)。

DROP TABLE IF EXISTS `register_no_counter`;

SET @ddl_drop_uk := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_register_no') > 0,
    'ALTER TABLE `accounts` DROP INDEX `uk_register_no`',
    'SELECT 1');
PREPARE stmt_drop_uk_register_no FROM @ddl_drop_uk;
EXECUTE stmt_drop_uk_register_no;
DEALLOCATE PREPARE stmt_drop_uk_register_no;

SET @ddl_drop_col := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'register_no') > 0,
    'ALTER TABLE `accounts` DROP COLUMN `register_no`',
    'SELECT 1');
PREPARE stmt_drop_register_no FROM @ddl_drop_col;
EXECUTE stmt_drop_register_no;
DEALLOCATE PREPARE stmt_drop_register_no;
