-- Pandora owner 权威表结构(pandora_owner 库,owner-authority.md §2,2026-07-21)。
--
-- ⚠️ 本文件是 **dev 单机 MySQL 联调形态**;生产 owner 权威必须部署在 TiDB
-- (deploy/tidb-init/02-owner-tidb.sql,同构 DDL + 热点打散)——§9.22 要求线性一致 CAS、
-- 法定多数侧单写、已确认写故障切换不回滚,MySQL 异步复制主从切换不满足。
--
-- 表清单:
--   owner_record         每玩家 owner 权威(epoch 单调;记录永不 TTL 消失,Release 置 none)
--   ds_instance_lease    DS 实例级租约(allocator 心跳代写;deadline 只前进)
--   owner_transition_log 迁移审计(append;90 天 sweep,§9.24)
--
-- 一致性:三表同库,epoch/lease/admit_not_before/phase 的读改写单事务完成(§9.22 同一
-- 线性一致事务域);SQL 写法 TiDB 安全(锁存在行 + 条件更新,不依赖间隙锁)。

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
    `operation_id`        VARCHAR(64)     NOT NULL DEFAULT '' COMMENT '本次迁移的稳定 operation(UUIDv4)',
    `admit_not_before_ms` BIGINT          NOT NULL DEFAULT 0 COMMENT '迁移屏障(UTC ms;CAS 时点算定,后续旧实例续租不回写)',
    `hub_source_revision` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'Hub 来源版本高水位(INC-20260818-003);只前进,Release/BATTLE 迁移都不清',
    `updated_at_ms`       BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='每玩家 owner 权威记录(§9.22)';
-- 既有库需手动补(expand 阶段,可回滚;INC-20260818-003 §3 分阶段发布第 1 步):
--   ALTER TABLE owner_record ADD COLUMN `hub_source_revision` BIGINT UNSIGNED NOT NULL DEFAULT 0
--     COMMENT 'Hub 来源版本高水位(INC-20260818-003)';
-- 回滚:ALTER TABLE owner_record DROP COLUMN `hub_source_revision`;
--   ⚠️ 只有在**全部** hub_allocator 副本都回退到不写 revision 的版本之后才可以 DROP。
--   列还在而新写者已在写非零值时 DROP,会把该玩家的高水位抹成 0 = 门重新对 legacy 敞开。

CREATE TABLE IF NOT EXISTS `ds_instance_lease` (
    `instance_uid`      VARCHAR(128)    NOT NULL,
    `pod_name`          VARCHAR(128)    NOT NULL DEFAULT '',
    `instance_epoch`    INT UNSIGNED    NOT NULL DEFAULT 0,
    `release_track`     VARCHAR(32)     NOT NULL DEFAULT '',
    `lease_deadline_ms` BIGINT          NOT NULL DEFAULT 0 COMMENT '只前进;allocator 心跳代写,秒数钳制 ≤ placement.DSFenceLeaseMaxSeconds',
    `updated_at_ms`     BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`instance_uid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='DS 实例级租约(玩家 owner lease 由此派生)';

CREATE TABLE IF NOT EXISTS `owner_transition_log` (
    `id`           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `player_id`    BIGINT UNSIGNED NOT NULL,
    `from_epoch`   BIGINT UNSIGNED NOT NULL,
    `to_epoch`     BIGINT UNSIGNED NOT NULL,
    `op`           TINYINT         NOT NULL COMMENT '1 begin / 2 admit / 3 release',
    `operation_id` VARCHAR(64)     NOT NULL,
    `detail`       VARCHAR(512)    NOT NULL DEFAULT '',
    `created_at`   DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `idx_player_created` (`player_id`, `created_at`),
    KEY `idx_created_at` (`created_at`) COMMENT '90 天保留期 sweep(§9.24)'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='owner 迁移审计流水';
