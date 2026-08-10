-- 注册编号(register_no)存量补齐(2026-08-10,docs/design/register-no-and-login-surge.md §3.3)。
--
-- fresh 库由 deploy/mysql-init/02-account-tables.sql(dev)/ deploy/tidb-init/03-account-tidb.sql
-- (prod TiDB)建表自带;已建 volume 的存量库走本版本条件补齐(幂等):
--   ① accounts 缺 register_no 列 → 条件加列(NULL = 待补号,login 补号任务异步分配);
--   ② accounts 缺 uk_register_no 唯一索引 → 条件加索引(双号防护兜底,唯一性权威在计数器事务);
--   ③ register_no_counter 计数器表不存在 → 整表创建(单行 id=1,发号权威闸)。
-- 计数器行本身不在此初始化:由 login 启动期 EnsureRegisterNoCounter 以 INSERT IGNORE 写入
-- (next_no = 配置 register_no_start,默认 1),保证起始号只在首次初始化生效。
-- 存量账号回填不需要独立步骤:补号任务对 register_no IS NULL 的行按 created_at+player_id
-- 全序批量编号,首轮自然追平(每轮 drain 上限 1 万行,大存量分多轮)。

SET @ddl_add_col := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND COLUMN_NAME = 'register_no') = 0,
    'ALTER TABLE `accounts` ADD COLUMN `register_no` BIGINT UNSIGNED NULL COMMENT ''注册编号(展示专用,禁作身份键/外键;NULL=待补号,login 补号任务异步分配,register-no-and-login-surge.md §3.3)'' AFTER `status`',
    'SELECT 1');
PREPARE stmt_add_register_no FROM @ddl_add_col;
EXECUTE stmt_add_register_no;
DEALLOCATE PREPARE stmt_add_register_no;

SET @ddl_add_uk := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'accounts' AND INDEX_NAME = 'uk_register_no') = 0,
    'ALTER TABLE `accounts` ADD UNIQUE KEY `uk_register_no` (`register_no`)',
    'SELECT 1');
PREPARE stmt_add_uk_register_no FROM @ddl_add_uk;
EXECUTE stmt_add_uk_register_no;
DEALLOCATE PREPARE stmt_add_uk_register_no;

CREATE TABLE IF NOT EXISTS `register_no_counter` (
    `id`      TINYINT UNSIGNED NOT NULL,
    `next_no` BIGINT UNSIGNED  NOT NULL COMMENT '下一个待发注册编号',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 注册编号全局发号计数器(单行 id=1;发号权威闸)';
