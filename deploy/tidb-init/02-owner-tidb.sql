-- Pandora owner 权威表结构 —— TiDB 版(生产权威形态,owner-authority.md §1,2026-07-21)
--
-- 背景:§9.22 要求 owner 权威存储提供线性一致读/CAS、法定多数侧单写、已确认写故障切换
-- 不回滚;MySQL 异步复制不满足,生产必须部署本文件到 TiDB。dev 单机 MySQL 联调用
-- deploy/mysql-init/15-owner-tables.sql(同构 DDL)。
--
-- 与 mysql-init 版差异(只改 DDL,业务 SQL / Go 代码不变;同 01-social-tidb.sql 的经验):
--   1. owner_record 主键是雪花 player_id(§9.11 不可改)→ NONCLUSTERED +
--      SHARD_ROW_ID_BITS + PRE_SPLIT_REGIONS 打散雪花时间序写热点(代价:点查一次回表,
--      owner 记录行极小,可接受);
--   2. owner_transition_log 代理主键 AUTO_INCREMENT → AUTO_RANDOM(纯 append 审计,
--      Go 侧不读 id / 不依赖 LastInsertId);
--   3. collation 用 utf8mb4_bin(业务键为数值/uuid,大小写不敏感无意义,全版本可用);
--   4. 悲观事务:Go 侧全部「SELECT ... FOR UPDATE 锁存在行 + 条件更新」,不依赖间隙锁。

CREATE DATABASE IF NOT EXISTS `pandora_owner`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_bin;

USE `pandora_owner`;

CREATE TABLE IF NOT EXISTS `owner_record` (
    `player_id`           BIGINT UNSIGNED NOT NULL,
    `owner_epoch`         BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '单调不回退;每次 exact owner 变化 +1',
    `owner_type`          TINYINT         NOT NULL DEFAULT 0 COMMENT '0 none / 1 hub / 2 battle',
    `phase`               TINYINT         NOT NULL DEFAULT 0 COMMENT '0 无 / 1 PENDING / 2 ADMITTED',
    `pod_name`            VARCHAR(128)    NOT NULL DEFAULT '',
    `instance_uid`        VARCHAR(128)    NOT NULL DEFAULT '',
    `instance_epoch`      INT UNSIGNED    NOT NULL DEFAULT 0,
    `assignment_or_allocation_id` VARCHAR(128) NOT NULL DEFAULT '',
    `release_track`       VARCHAR(32)     NOT NULL DEFAULT '',
    `operation_id`        VARCHAR(64)     NOT NULL DEFAULT '',
    `admit_not_before_ms` BIGINT          NOT NULL DEFAULT 0,
    `hub_source_revision` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Hub 来源版本高水位(INC-20260818-003);只前进,Release/BATTLE 迁移都不清',
    `updated_at_ms`       BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`player_id`) /*T![clustered_index] NONCLUSTERED */
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  SHARD_ROW_ID_BITS = 4 PRE_SPLIT_REGIONS = 4
  COMMENT='每玩家 owner 权威记录(§9.22)';
-- 既有库需手动补(expand 阶段;TiDB 支持 IF NOT EXISTS,可重复执行):
--   ALTER TABLE owner_record ADD COLUMN IF NOT EXISTS `hub_source_revision`
--     BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Hub 来源版本高水位(INC-20260818-003)';
-- 回滚:ALTER TABLE owner_record DROP COLUMN IF EXISTS `hub_source_revision`;
--   ⚠️ 同 mysql-init:必须先把全部 hub_allocator 副本退回不写 revision 的版本再 DROP。

CREATE TABLE IF NOT EXISTS `ds_instance_lease` (
    `instance_uid`      VARCHAR(128)    NOT NULL,
    `pod_name`          VARCHAR(128)    NOT NULL DEFAULT '',
    `instance_epoch`    INT UNSIGNED    NOT NULL DEFAULT 0,
    `release_track`     VARCHAR(32)     NOT NULL DEFAULT '',
    `lease_deadline_ms` BIGINT          NOT NULL DEFAULT 0,
    `updated_at_ms`     BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`instance_uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='DS 实例级租约(玩家 owner lease 由此派生;行数=实例数,无热点)';

CREATE TABLE IF NOT EXISTS `owner_transition_log` (
    `id`           BIGINT UNSIGNED NOT NULL /*T![auto_rand] AUTO_RANDOM(5) */,
    `player_id`    BIGINT UNSIGNED NOT NULL,
    `from_epoch`   BIGINT UNSIGNED NOT NULL,
    `to_epoch`     BIGINT UNSIGNED NOT NULL,
    `op`           TINYINT         NOT NULL COMMENT '1 begin / 2 admit / 3 release',
    `operation_id` VARCHAR(64)     NOT NULL,
    `detail`       VARCHAR(512)    NOT NULL DEFAULT '',
    `created_at`   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `idx_player_created` (`player_id`, `created_at`),
    KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT='owner 迁移审计流水(90 天 sweep,§9.24)';
