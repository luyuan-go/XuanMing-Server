-- 回滚 player_no → register_no 改名(仅元数据,不动类型/数据)。
--
-- ⚠️ 回滚只是把名字改回 000004 的口径,**不代表旧语义(编号绑定账号)重新生效**——
-- 那个结论已在 §3.6.1 作废。真要回退语义须同时回退代码与文档。
--
-- 三步逆序、条件执行、幂等(与 up 对称);列注释一并还原为 000004 的原文。

-- ① 计数器表改回。
SET @ddl_rename_tbl := IF(
    (SELECT COUNT(*) FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_no_counter') > 0
    AND (SELECT COUNT(*) FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'register_no_counter') = 0,
    'RENAME TABLE `player_no_counter` TO `register_no_counter`',
    'SELECT 1');
PREPARE stmt_revert_player_no_tbl FROM @ddl_rename_tbl;
EXECUTE stmt_revert_player_no_tbl;
DEALLOCATE PREPARE stmt_revert_player_no_tbl;

-- ② 唯一索引改回。
SET @ddl_rename_idx := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_player_no') > 0
    AND (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_register_no') = 0,
    'ALTER TABLE `accounts` RENAME INDEX `uk_player_no` TO `uk_register_no`',
    'SELECT 1');
PREPARE stmt_revert_player_no_idx FROM @ddl_rename_idx;
EXECUTE stmt_revert_player_no_idx;
DEALLOCATE PREPARE stmt_revert_player_no_idx;

-- ③ 列改回 + 注释还原为 000004 原文。
SET @ddl_rename_col := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'player_no') > 0
    AND (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'register_no') = 0,
    'ALTER TABLE `accounts` RENAME COLUMN `player_no` TO `register_no`',
    'SELECT 1');
PREPARE stmt_revert_player_no_col FROM @ddl_rename_col;
EXECUTE stmt_revert_player_no_col;
DEALLOCATE PREPARE stmt_revert_player_no_col;

-- ⚠️ 下面这串必须与 000004 建列时的 COMMENT **逐字一致**(含旧词「注册编号」与旧文档名),
-- 它是回滚的匹配目标,不是本次改名的产物——批量改词时**不得**把它一起换掉,
-- 否则条件判断永远不成立,每次回滚都会白发一次 DDL。
SET @old_comment := '注册编号(展示专用,禁作身份键/外键;NULL=待补号,login 补号任务异步分配,register-no-and-login-surge.md §3.3)';

SET @ddl_revert_comment := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'register_no') > 0
    AND (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts'
       AND COLUMN_NAME = 'register_no' AND COLUMN_COMMENT = @old_comment) = 0,
    CONCAT('ALTER TABLE `accounts` MODIFY COLUMN `register_no` BIGINT UNSIGNED NULL COMMENT ', QUOTE(@old_comment)),
    'SELECT 1');
PREPARE stmt_revert_player_no_comment FROM @ddl_revert_comment;
EXECUTE stmt_revert_player_no_comment;
DEALLOCATE PREPARE stmt_revert_player_no_comment;
