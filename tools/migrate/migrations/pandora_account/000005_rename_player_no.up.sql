-- register_no → player_no 改名(2026-08-10,docs/design/player-no-and-login-surge.md §3.6.3)。
--
-- 为什么改名:编号已拍板绑定**角色实体**(§3.6.1,项目卖角色/可过户)。「register(注册)」
-- 在通行语境里是账号动作,而角色是**创建**出来的——留 `register_no` 会持续把人往
-- 「账号级编号」上引。改后与既有命名体系配对:
--     player_id = 角色实体 ID(雪花,系统用) / player_no = 角色展示序号(人看)
-- 刻意**不叫** `role_no`:`role_id` 已被 CfgRole.Id(职业配置 ID)占用,再来一个 role_* 更糊涂。
--
-- 为什么独立一版而不是改 000004:migrate 按版本号记录、不重跑,改 000004 对已执行过的库无效;
-- 且 README 明定「迁移文件一旦执行即永久 immutable」。故 000004 保持原样(建 register_no),
-- 本版负责改名。两条路径最终一致:
--   · 用 deploy/*-init 建的新库 → 直接就是 player_no,本版三步全部跳过(幂等空操作);
--   · 用迁移建的库 → 000004 建 register_no,本版改名为 player_no。
--
-- 三步均条件执行(幂等,可重复跑):列、唯一索引、计数器表。
-- 安全性:RENAME 只改元数据,不重建表、不触碰行数据、不改类型;MySQL 8 / TiDB 均支持
-- RENAME COLUMN / RENAME INDEX / RENAME TABLE,且都是 online DDL。
-- ⚠️ 不用 `CHANGE old new <type>`(需完整重复列定义,漏写 UNSIGNED/NULL 会静默改变列语义);
--    RENAME COLUMN 不碰类型,是本场景唯一无脑安全的写法。列注释另由第 4 步单独刷。

-- ① 列改名:仅当 register_no 存在且 player_no 尚不存在。
SET @ddl_rename_col := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'register_no') > 0
    AND (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'player_no') = 0,
    'ALTER TABLE `accounts` RENAME COLUMN `register_no` TO `player_no`',
    'SELECT 1');
PREPARE stmt_rename_player_no_col FROM @ddl_rename_col;
EXECUTE stmt_rename_player_no_col;
DEALLOCATE PREPARE stmt_rename_player_no_col;

-- ② 唯一索引改名:仅当 uk_register_no 存在且 uk_player_no 尚不存在。
SET @ddl_rename_idx := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_register_no') > 0
    AND (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_player_no') = 0,
    'ALTER TABLE `accounts` RENAME INDEX `uk_register_no` TO `uk_player_no`',
    'SELECT 1');
PREPARE stmt_rename_player_no_idx FROM @ddl_rename_idx;
EXECUTE stmt_rename_player_no_idx;
DEALLOCATE PREPARE stmt_rename_player_no_idx;

-- ③ 计数器表改名:仅当 register_no_counter 存在且 player_no_counter 尚不存在。
SET @ddl_rename_tbl := IF(
    (SELECT COUNT(*) FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'register_no_counter') > 0
    AND (SELECT COUNT(*) FROM information_schema.TABLES
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_no_counter') = 0,
    'RENAME TABLE `register_no_counter` TO `player_no_counter`',
    'SELECT 1');
PREPARE stmt_rename_player_no_tbl FROM @ddl_rename_tbl;
EXECUTE stmt_rename_player_no_tbl;
DEALLOCATE PREPARE stmt_rename_player_no_tbl;

-- ④ 列注释同步为角色归属语义(RENAME COLUMN 不改注释,故单独一步)。
-- 仅当注释与目标不同才发 DDL;MODIFY COLUMN 需完整重复列定义,此处
-- `BIGINT UNSIGNED NULL` 与 000004 的 ADD COLUMN 逐字一致(无默认值 / 无 EXTRA)。
SET @target_comment := '角色编号(展示专用,禁作身份键/外键/幂等键;绑定角色实体——今 player_id 即角色身份,卖角色过户时随角色走、值不变,故一账号建 N 角色 = N 个编号;NULL=待补号,login 补号任务异步分配,player-no-and-login-surge.md §3.3/§3.6.1)';

SET @ddl_sync_comment := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'player_no') > 0
    AND (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts'
       AND COLUMN_NAME = 'player_no' AND COLUMN_COMMENT = @target_comment) = 0,
    CONCAT('ALTER TABLE `accounts` MODIFY COLUMN `player_no` BIGINT UNSIGNED NULL COMMENT ', QUOTE(@target_comment)),
    'SELECT 1');
PREPARE stmt_sync_player_no_comment FROM @ddl_sync_comment;
EXECUTE stmt_sync_player_no_comment;
DEALLOCATE PREPARE stmt_sync_player_no_comment;
