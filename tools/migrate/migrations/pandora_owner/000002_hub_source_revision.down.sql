-- 000002 回滚:删掉来源版本高水位列。
--
-- ⚠️ 只有在**全部** hub_allocator 副本都已回退到不写 revision 的版本之后才可以执行。
-- 列还在、而新写者已经在写非零值时 DROP,会把每个玩家的高水位抹成 0 —— 逐玩家的
-- 「见过非零版本就永久拒 legacy」随之失效,门重新对旧写者敞开(比没有这道门更坏:
-- 上层还以为它在生效)。分阶段回滚顺序见
-- docs/incidents/2026-08-18-p1-hub-assignment-source-revision-rollout.md §5。
--
-- 同样条件化:列不存在时空跑,回滚可重复执行。

SET @ddl_drop_hub_source_revision := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'owner_record'
        AND COLUMN_NAME = 'hub_source_revision') = 1,
    'ALTER TABLE `owner_record` DROP COLUMN `hub_source_revision`',
    'SELECT 1');
PREPARE stmt_v2_drop_hub_source_revision FROM @ddl_drop_hub_source_revision;
EXECUTE stmt_v2_drop_hub_source_revision;
DEALLOCATE PREPARE stmt_v2_drop_hub_source_revision;
