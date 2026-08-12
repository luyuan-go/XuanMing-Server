-- player_no schema 收敛(2026-08-12,docs/design/player-no-and-login-surge.md §3.6.3)。
--
-- 000005 已对 origin 暴露，按可能已执行处理，永久 immutable。本版只向前收敛三类库：
--   A. 普通存量库：只有 register_no / uk_register_no / register_no_counter；
--   B. 已完成目标库：只有 player_no / uk_player_no / player_no_counter；
--   C. mysql-init 先建目标 schema、迁移链又跑 000004 后形成的双列/双索引/双计数器库。
--
-- C 类库必须先验数据再删 legacy：同一角色两值不同，或任意两个角色跨新旧列持有同一编号，
-- 都说明无法无损自动判定权威值，迁移用语义化不存在表故意报错并停住。PREPARE 不能执行
-- SIGNAL，故采用该方式保留可检索的错误名。无冲突时，legacy 非 NULL 且 target 为 NULL 的值
-- 可安全补入 target；双计数器同 id 取 GREATEST(next_no)，避免回退发号水位造成重号。

-- ⓪ 零写入 preflight：后续 COMMENT 同步使用 MODIFY COLUMN，只有当前完整列定义已经与
-- canonical 类型一致时才允许执行，避免 COMMENT 修复顺手静默改变 signed/null/default/extra。
-- legacy 索引也必须在 RENAME 前证明是预期单列唯一索引，不能改名后才发现指错列。
SET @has_register_no_col := (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'register_no');
SET @has_player_no_col := (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'player_no');

SET @player_no_candidate_cols := (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts'
      AND COLUMN_NAME IN ('register_no', 'player_no'));
SET @player_no_valid_cols := (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts'
      AND COLUMN_NAME IN ('register_no', 'player_no')
      AND DATA_TYPE = 'bigint'
      AND LOWER(COLUMN_TYPE) = 'bigint unsigned'
      AND IS_NULLABLE = 'YES'
      AND COLUMN_DEFAULT IS NULL
      AND COALESCE(EXTRA, '') = '');
SET @sql_guard_player_no_column_shape := IF(
    @player_no_candidate_cols IN (1, 2)
    AND @player_no_candidate_cols = @player_no_valid_cols,
    'SELECT 1',
    'SELECT * FROM `__pandora_player_no_reconcile_column_shape_invalid__`');
PREPARE stmt_guard_player_no_column_shape FROM @sql_guard_player_no_column_shape;
EXECUTE stmt_guard_player_no_column_shape;
DEALLOCATE PREPARE stmt_guard_player_no_column_shape;

SET @player_no_counter_tables := (
    SELECT COUNT(*) FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME IN ('register_no_counter', 'player_no_counter')
      AND TABLE_TYPE = 'BASE TABLE');
SET @player_no_counter_cols := (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME IN ('register_no_counter', 'player_no_counter'));
SET @player_no_valid_counter_id_cols := (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME IN ('register_no_counter', 'player_no_counter')
      AND COLUMN_NAME = 'id'
      AND DATA_TYPE = 'tinyint'
      AND LOWER(COLUMN_TYPE) = 'tinyint unsigned'
      AND IS_NULLABLE = 'NO'
      AND COLUMN_DEFAULT IS NULL
      AND COALESCE(EXTRA, '') = '');
SET @player_no_valid_counter_next_cols := (
    SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME IN ('register_no_counter', 'player_no_counter')
      AND COLUMN_NAME = 'next_no'
      AND DATA_TYPE = 'bigint'
      AND LOWER(COLUMN_TYPE) = 'bigint unsigned'
      AND IS_NULLABLE = 'NO'
      AND COLUMN_DEFAULT IS NULL
      AND COALESCE(EXTRA, '') = '');
SET @player_no_counter_primary_rows := (
    SELECT COUNT(*) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME IN ('register_no_counter', 'player_no_counter')
      AND INDEX_NAME = 'PRIMARY');
SET @player_no_valid_counter_primary_rows := (
    SELECT COUNT(*) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME IN ('register_no_counter', 'player_no_counter')
      AND INDEX_NAME = 'PRIMARY' AND COLUMN_NAME = 'id'
      AND SEQ_IN_INDEX = 1 AND NON_UNIQUE = 0);
SET @sql_guard_player_no_counter_shape := IF(
    @player_no_counter_tables IN (1, 2)
    AND @player_no_counter_cols = @player_no_counter_tables * 2
    AND @player_no_counter_tables = @player_no_valid_counter_id_cols
    AND @player_no_counter_tables = @player_no_valid_counter_next_cols
    AND @player_no_counter_tables = @player_no_counter_primary_rows
    AND @player_no_counter_tables = @player_no_valid_counter_primary_rows,
    'SELECT 1',
    'SELECT * FROM `__pandora_player_no_reconcile_counter_shape_invalid__`');
PREPARE stmt_guard_player_no_counter_shape FROM @sql_guard_player_no_counter_shape;
EXECUTE stmt_guard_player_no_counter_shape;
DEALLOCATE PREPARE stmt_guard_player_no_counter_shape;

SET @register_no_index_rows := (
    SELECT COUNT(*) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_register_no');
SET @register_no_index_valid_rows := (
    SELECT COUNT(*) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_register_no'
      AND SEQ_IN_INDEX = 1 AND NON_UNIQUE = 0
      AND ((@has_register_no_col > 0 AND COLUMN_NAME = 'register_no')
           OR (@has_register_no_col = 0 AND @has_player_no_col > 0 AND COLUMN_NAME = 'player_no')));
SET @sql_guard_register_no_index := IF(
    @register_no_index_rows = 0
    OR (@register_no_index_rows = 1 AND @register_no_index_valid_rows = 1),
    'SELECT 1',
    'SELECT * FROM `__pandora_player_no_reconcile_legacy_index_invalid__`');
PREPARE stmt_guard_register_no_index FROM @sql_guard_register_no_index;
EXECUTE stmt_guard_register_no_index;
DEALLOCATE PREPARE stmt_guard_register_no_index;

SET @player_no_index_rows := (
    SELECT COUNT(*) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_player_no');
SET @player_no_index_valid_rows := (
    SELECT COUNT(*) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_player_no'
      AND COLUMN_NAME = 'player_no' AND SEQ_IN_INDEX = 1 AND NON_UNIQUE = 0);
SET @sql_guard_player_no_index := IF(
    @player_no_index_rows = 0
    OR (@player_no_index_rows = 1 AND @player_no_index_valid_rows = 1),
    'SELECT 1',
    'SELECT * FROM `__pandora_player_no_reconcile_target_index_invalid__`');
PREPARE stmt_guard_player_no_index FROM @sql_guard_player_no_index;
EXECUTE stmt_guard_player_no_index;
DEALLOCATE PREPARE stmt_guard_player_no_index;

-- ① 双列数据冲突门：仍在任何 UPDATE / DROP 之前执行。
SET @player_no_reconcile_conflicts := 0;
SET @sql_count_player_no_conflicts := IF(
    @has_register_no_col > 0 AND @has_player_no_col > 0,
    'SET @player_no_reconcile_conflicts := (
     SELECT COUNT(*) FROM `accounts` AS a
      WHERE (a.`register_no` IS NOT NULL AND a.`player_no` IS NOT NULL
             AND a.`register_no` <> a.`player_no`)
         OR (a.`register_no` IS NOT NULL AND EXISTS (
                SELECT 1 FROM `accounts` AS b
                 WHERE b.`player_id` <> a.`player_id`
                   AND (b.`register_no` = a.`register_no`
                        OR b.`player_no` = a.`register_no`)))
         OR (a.`player_no` IS NOT NULL AND EXISTS (
                SELECT 1 FROM `accounts` AS b
                 WHERE b.`player_id` <> a.`player_id`
                   AND (b.`register_no` = a.`player_no`
                        OR b.`player_no` = a.`player_no`))))',
    'SELECT 1');
PREPARE stmt_count_player_no_conflicts FROM @sql_count_player_no_conflicts;
EXECUTE stmt_count_player_no_conflicts;
DEALLOCATE PREPARE stmt_count_player_no_conflicts;

SET @sql_guard_player_no_conflicts := IF(
    @player_no_reconcile_conflicts > 0,
    'SELECT * FROM `__pandora_player_no_reconcile_data_conflict__`',
    'SELECT 1');
PREPARE stmt_guard_player_no_conflicts FROM @sql_guard_player_no_conflicts;
EXECUTE stmt_guard_player_no_conflicts;
DEALLOCATE PREPARE stmt_guard_player_no_conflicts;

-- ② 双列无冲突时，把 target 空值从 legacy 补齐；相等值无需改写。
SET @sql_merge_player_no_values := IF(
    @has_register_no_col > 0 AND @has_player_no_col > 0,
    'UPDATE `accounts` SET `player_no` = `register_no`
      WHERE `player_no` IS NULL AND `register_no` IS NOT NULL',
    'SELECT 1');
PREPARE stmt_merge_player_no_values FROM @sql_merge_player_no_values;
EXECUTE stmt_merge_player_no_values;
DEALLOCATE PREPARE stmt_merge_player_no_values;

-- ③ old-only 列直接原地改名。双列状态留到 target 索引建妥后再删 legacy 列。
SET @sql_rename_player_no_col := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'register_no') > 0
    AND (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'player_no') = 0,
    'ALTER TABLE `accounts` RENAME COLUMN `register_no` TO `player_no`',
    'SELECT 1');
PREPARE stmt_v6_rename_player_no_col FROM @sql_rename_player_no_col;
EXECUTE stmt_v6_rename_player_no_col;
DEALLOCATE PREPARE stmt_v6_rename_player_no_col;

-- ④ old-only 改列后，旧索引仍指向现已改名的 player_no，可安全只改索引名。
-- 双列状态的 uk_register_no 仍指向 legacy 列，绝不能误改名覆盖 target 索引。
SET @sql_rename_player_no_index := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'register_no') = 0
    AND (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_register_no') > 0
    AND (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_player_no') = 0,
    'ALTER TABLE `accounts` RENAME INDEX `uk_register_no` TO `uk_player_no`',
    'SELECT 1');
PREPARE stmt_v6_rename_player_no_index FROM @sql_rename_player_no_index;
EXECUTE stmt_v6_rename_player_no_index;
DEALLOCATE PREPARE stmt_v6_rename_player_no_index;

-- 双列或缺索引的兼容状态先给 target 补唯一索引，再删除 legacy 索引/列。
SET @sql_add_player_no_index := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'player_no') > 0
    AND (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_player_no') = 0,
    'ALTER TABLE `accounts` ADD UNIQUE KEY `uk_player_no` (`player_no`)',
    'SELECT 1');
PREPARE stmt_v6_add_player_no_index FROM @sql_add_player_no_index;
EXECUTE stmt_v6_add_player_no_index;
DEALLOCATE PREPARE stmt_v6_add_player_no_index;

SET @sql_drop_register_no_index := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_register_no') > 0,
    'ALTER TABLE `accounts` DROP INDEX `uk_register_no`',
    'SELECT 1');
PREPARE stmt_v6_drop_register_no_index FROM @sql_drop_register_no_index;
EXECUTE stmt_v6_drop_register_no_index;
DEALLOCATE PREPARE stmt_v6_drop_register_no_index;

SET @sql_drop_register_no_col := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'register_no') > 0
    AND (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'player_no') > 0,
    'ALTER TABLE `accounts` DROP COLUMN `register_no`',
    'SELECT 1');
PREPARE stmt_v6_drop_register_no_col FROM @sql_drop_register_no_col;
EXECUTE stmt_v6_drop_register_no_col;
DEALLOCATE PREPARE stmt_v6_drop_register_no_col;

-- ⑤ counter old-only 原地改表名；双表则先合并每个 id 的水位，再补 legacy 独有行。
SET @sql_rename_player_no_counter := IF(
    (SELECT COUNT(*) FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'register_no_counter') > 0
    AND (SELECT COUNT(*) FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_no_counter') = 0,
    'RENAME TABLE `register_no_counter` TO `player_no_counter`',
    'SELECT 1');
PREPARE stmt_v6_rename_player_no_counter FROM @sql_rename_player_no_counter;
EXECUTE stmt_v6_rename_player_no_counter;
DEALLOCATE PREPARE stmt_v6_rename_player_no_counter;

SET @has_register_no_counter := (
    SELECT COUNT(*) FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'register_no_counter');
SET @has_player_no_counter := (
    SELECT COUNT(*) FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_no_counter');

SET @sql_merge_player_no_counter := IF(
    @has_register_no_counter > 0 AND @has_player_no_counter > 0,
    'UPDATE `player_no_counter` AS p
        JOIN `register_no_counter` AS r ON r.`id` = p.`id`
         SET p.`next_no` = GREATEST(p.`next_no`, r.`next_no`)',
    'SELECT 1');
PREPARE stmt_v6_merge_player_no_counter FROM @sql_merge_player_no_counter;
EXECUTE stmt_v6_merge_player_no_counter;
DEALLOCATE PREPARE stmt_v6_merge_player_no_counter;

SET @sql_copy_player_no_counter := IF(
    @has_register_no_counter > 0 AND @has_player_no_counter > 0,
    'INSERT INTO `player_no_counter` (`id`, `next_no`)
     SELECT r.`id`, r.`next_no`
       FROM `register_no_counter` AS r
       LEFT JOIN `player_no_counter` AS p ON p.`id` = r.`id`
      WHERE p.`id` IS NULL',
    'SELECT 1');
PREPARE stmt_v6_copy_player_no_counter FROM @sql_copy_player_no_counter;
EXECUTE stmt_v6_copy_player_no_counter;
DEALLOCATE PREPARE stmt_v6_copy_player_no_counter;

SET @sql_drop_register_no_counter := IF(
    @has_register_no_counter > 0 AND @has_player_no_counter > 0,
    'DROP TABLE `register_no_counter`',
    'SELECT 1');
PREPARE stmt_v6_drop_register_no_counter FROM @sql_drop_register_no_counter;
EXECUTE stmt_v6_drop_register_no_counter;
DEALLOCATE PREPARE stmt_v6_drop_register_no_counter;

-- ⑥ 最终形态必须完整存在；缺对象时 fail-closed，不能把 schema_migrations 推到 v6。
SET @player_no_final_shape_ok := (
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'player_no') = 1
    AND (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_player_no'
       AND COLUMN_NAME = 'player_no' AND SEQ_IN_INDEX = 1 AND NON_UNIQUE = 0) = 1
    AND (SELECT COUNT(*) FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_no_counter') = 1);
SET @sql_guard_player_no_final_shape := IF(
    @player_no_final_shape_ok,
    'SELECT 1',
    'SELECT * FROM `__pandora_player_no_reconcile_target_shape_missing__`');
PREPARE stmt_guard_player_no_final_shape FROM @sql_guard_player_no_final_shape;
EXECUTE stmt_guard_player_no_final_shape;
DEALLOCATE PREPARE stmt_guard_player_no_final_shape;

-- ⑦ 无论从哪条路径到达，都把三处元数据刷成同一套角色语义。
SET @canonical_player_no_comment := '角色编号(展示专用,禁作身份键/外键/幂等键;绑定角色实体——今 player_id 即角色身份,卖角色过户时随角色走、值不变,故一账号建 N 角色 = N 个编号;NULL=待补号,login 补号任务按 created_at+player_id 序异步分配,player-no-and-login-surge.md §3.3/§3.6.1)';
SET @sql_sync_player_no_comment := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'player_no'
       AND COLUMN_COMMENT = @canonical_player_no_comment) = 0,
    CONCAT('ALTER TABLE `accounts` MODIFY COLUMN `player_no` BIGINT UNSIGNED NULL COMMENT ',
           QUOTE(@canonical_player_no_comment)),
    'SELECT 1');
PREPARE stmt_v6_sync_player_no_comment FROM @sql_sync_player_no_comment;
EXECUTE stmt_v6_sync_player_no_comment;
DEALLOCATE PREPARE stmt_v6_sync_player_no_comment;

SET @canonical_next_no_comment := '下一个待发角色编号';
SET @sql_sync_next_no_comment := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_no_counter' AND COLUMN_NAME = 'next_no'
       AND COLUMN_COMMENT = @canonical_next_no_comment) = 0,
    CONCAT('ALTER TABLE `player_no_counter` MODIFY COLUMN `next_no` BIGINT UNSIGNED NOT NULL COMMENT ',
           QUOTE(@canonical_next_no_comment)),
    'SELECT 1');
PREPARE stmt_v6_sync_next_no_comment FROM @sql_sync_next_no_comment;
EXECUTE stmt_v6_sync_next_no_comment;
DEALLOCATE PREPARE stmt_v6_sync_next_no_comment;

SET @canonical_player_no_counter_comment := 'Pandora 角色编号全局发号计数器(单行 id=1;发号权威闸)';
SET @sql_sync_player_no_counter_comment := IF(
    (SELECT COUNT(*) FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_no_counter'
       AND TABLE_COMMENT = @canonical_player_no_counter_comment) = 0,
    CONCAT('ALTER TABLE `player_no_counter` COMMENT=', QUOTE(@canonical_player_no_counter_comment)),
    'SELECT 1');
PREPARE stmt_v6_sync_player_no_counter_comment FROM @sql_sync_player_no_counter_comment;
EXECUTE stmt_v6_sync_player_no_counter_comment;
DEALLOCATE PREPARE stmt_v6_sync_player_no_counter_comment;
