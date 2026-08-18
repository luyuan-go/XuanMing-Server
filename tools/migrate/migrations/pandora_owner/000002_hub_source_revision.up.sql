-- 000002_hub_source_revision — Hub assignment 来源版本高水位(INC-20260818-003 分阶段发布第 1 步 expand DDL)。
--
-- 背景:hub_allocator 的 assignment 此前没有可排序的来源版本,滚动升级窗口里旧 writer
-- 能用更旧的 target 抢过新 winner(确定性反例见 docs/incidents/
-- 2026-08-18-p1-hub-assignment-source-revision-rollout.md §1)。owner 需要一列**每玩家
-- 高水位**,在行锁事务内与 target/epoch 一起比较:低版本拒、同版本异 target 拒、
-- 高版本才可迁移;见 owner 的 classifySourceRevision。
--
-- 纯 additive(§9.16/§9.21 expand):只加一列。旧 owner 二进制的 SQL 不引用它,滚动升级
-- 窗口内不受影响;新 owner 缺列则**每个 RPC 都报 Error 1054**,启动期 fail-fast 拒启
-- (services/runtime/owner/internal/data/backend_check.go 的 AssertSourceRevisionColumn)。
-- 本迁移就是那道 fail-fast 指向的 expand 步骤,从此不需要任何人手工执行 ALTER。
--
-- 条件加列:deploy/mysql-init/15-owner-tables.sql 与 deploy/tidb-init/02-owner-tidb.sql 的
-- fresh-init 已直接建出本列,无条件 ADD COLUMN 会在那种库上 duplicate column 报错
-- (同 pandora_player 000002/000004 的处理)。
--
-- 刻意不写 ALGORITHM:生产 owner 权威库在 TiDB(§9.22),TiDB 有自己的 online DDL,
-- 不吃 MySQL 的 ALGORITHM 语义;同在 TiDB 的 pandora_account 000004/000007/000008 加列
-- 也一律不带该子句。MySQL 8 侧加带默认值的列本身就走 INSTANT,本列无索引,不会退化成
-- 锁表拷贝。

SET @ddl_add_hub_source_revision := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE()
        AND TABLE_NAME = 'owner_record'
        AND COLUMN_NAME = 'hub_source_revision') = 0,
    'ALTER TABLE `owner_record` ADD COLUMN `hub_source_revision` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT ''Hub 来源版本高水位(INC-20260818-003);只前进,Release/BATTLE 迁移都不清'' AFTER `admit_not_before_ms`',
    'SELECT 1');
PREPARE stmt_v2_add_hub_source_revision FROM @ddl_add_hub_source_revision;
EXECUTE stmt_v2_add_hub_source_revision;
DEALLOCATE PREPARE stmt_v2_add_hub_source_revision;

-- 不回填:DEFAULT 0 就是 legacy 哨兵(= 该玩家还没被带版本的写者服务过)。owner 的判定
-- 矩阵靠「高水位 0 且 incoming 0」放行来维持兼容窗;回填成非 0 会把所有存量玩家直接
-- 推进到「见过非零版本」,当场把仍在跑的旧 hub_allocator 全部拒掉 = 大厅分配停摆。
