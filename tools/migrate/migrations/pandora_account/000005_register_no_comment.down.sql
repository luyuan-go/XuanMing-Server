-- 回滚 register_no 列注释到 000004 时的旧文字(仅注释,不动类型/数据)。
--
-- ⚠️ 旧注释承载的是**已作废的结论**(编号绑定账号)。回滚本版只为保持 up/down 对称,
-- 不代表旧口径重新生效;真要回退语义须同时回退 §3.6.1 的拍板与代码注释。
--
-- 条件执行,幂等;列不存在时(000004 也已回滚)为空操作。

SET @old_comment := '注册编号(展示专用,禁作身份键/外键;NULL=待补号,login 补号任务按 created_at+player_id 序异步分配,register-no-and-login-surge.md §3.3)';

SET @ddl_revert_comment := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts'
       AND COLUMN_NAME = 'register_no' AND COLUMN_COMMENT = @old_comment) = 0
    AND (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts'
       AND COLUMN_NAME = 'register_no') > 0,
    CONCAT('ALTER TABLE `accounts` MODIFY COLUMN `register_no` BIGINT UNSIGNED NULL COMMENT ', QUOTE(@old_comment)),
    'SELECT 1');
PREPARE stmt_revert_register_no_comment FROM @ddl_revert_comment;
EXECUTE stmt_revert_register_no_comment;
DEALLOCATE PREPARE stmt_revert_register_no_comment;
