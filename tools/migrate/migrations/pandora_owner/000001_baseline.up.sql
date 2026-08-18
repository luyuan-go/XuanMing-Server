-- Pandora pandora_owner baseline(owner authority §9.22,owner-authority.md §2)。
--
-- 与其它库的 baseline 同纪律:内容由 deploy/mysql-init/15-owner-tables.sql 生成,全部
-- `CREATE TABLE IF NOT EXISTS`。三种库跑到这里都必须安全:
--   ① 全新空库             —— 建出三张表;
--   ② 已由 mysql-init 建好 —— 全部空跑,只在 schema_migrations 记一个版本;
--   ③ 已由 tidb-init 建好  —— 同 ②(生产 owner 在 TiDB,建表由 deploy/tidb-init/
--      02-owner-tidb.sql 负责,那份带 NONCLUSTERED / SHARD_ROW_ID_BITS / AUTO_RANDOM 等
--      TiDB 专属属性;本 baseline 只是 IF NOT EXISTS 兜底,不会把 MySQL 形态盖上去)。
--
-- ⚠️ 本文件是**建立迁移集那一刻的历史形态**,不是当前 schema 的镜像:
-- `owner_record.hub_source_revision` 刻意不在这里,由 000002 条件加列补齐 —— 存量库
-- (卷早就建好、mysql-init 不会重放)只能靠那一版拿到该列。这正是本迁移集存在的理由。
-- 同 pandora_player 的 baseline 不含 players.exp(由 000002_experience 补)。
--
-- 没有 `USE`:迁移器连的就是目标库,库名由 targets 清单锁定。

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
    `updated_at_ms`       BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='每玩家 owner 权威记录(§9.22)';

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
