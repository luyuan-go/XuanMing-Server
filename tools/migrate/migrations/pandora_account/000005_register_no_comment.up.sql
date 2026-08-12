-- register_no 列注释同步为「角色归属」语义(2026-08-10,
-- docs/design/register-no-and-login-surge.md §3.6.1/§3.6.2 用户拍板)。
--
-- 为什么需要独立一版:000004 已在存量库执行过,migrate 按版本号记录、不会重跑,
-- 改 000004 的 SQL 对已跑过的库无效。列注释是 DBA / 排障时读到的第一手口径,
-- 留旧文字会让人以为编号绑定账号(旧结论已作废),故补一版专门刷注释。
--
-- 语义变更(仅注释,不动类型/可空性/索引/数据):
--   旧:注册编号(展示专用,禁作身份键/外键;NULL=待补号,...)
--   新:补「绑定角色实体、卖角色过户随角色走、一账号建 N 角色 = N 个编号」+ 幂等键禁用。
--
-- 安全性:
--   - MODIFY COLUMN 必须完整重复列定义,漏写 UNSIGNED / NULL 会静默改变列语义。
--     此处 `BIGINT UNSIGNED NULL` 与 000004 的 ADD COLUMN 定义逐字一致(已核 live 库
--     information_schema:COLUMN_TYPE='bigint unsigned', IS_NULLABLE='YES', 无默认值/EXTRA);
--   - 条件执行:仅当现存注释与目标不同才发 DDL,重复跑本迁移零操作(幂等);
--   - 纯元数据变更,不重建表、不触碰行数据;MySQL 8 / TiDB 均为 online DDL。

SET @target_comment := '注册编号(展示专用,禁作身份键/外键/幂等键;绑定角色实体——今 player_id 即角色身份,卖角色过户时随角色走、值不变,故一账号建 N 角色 = N 个编号;NULL=待补号,login 补号任务异步分配,register-no-and-login-surge.md §3.3/§3.6.1)';

SET @ddl_sync_comment := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts'
       AND COLUMN_NAME = 'register_no' AND COLUMN_COMMENT = @target_comment) = 0
    AND (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts'
       AND COLUMN_NAME = 'register_no') > 0,
    CONCAT('ALTER TABLE `accounts` MODIFY COLUMN `register_no` BIGINT UNSIGNED NULL COMMENT ', QUOTE(@target_comment)),
    'SELECT 1');
PREPARE stmt_sync_register_no_comment FROM @ddl_sync_comment;
EXECUTE stmt_sync_register_no_comment;
DEALLOCATE PREPARE stmt_sync_register_no_comment;
