-- Pandora pandora_bag baseline(背包域 §9.6 / bag-domain.md §4)。
--
-- 与其它库的 baseline 同纪律:内容由 deploy/mysql-init/14-bag-tables.sql 生成,全部
-- `CREATE TABLE IF NOT EXISTS` —— 全新空库建表,已由 mysql-init 建好的库空跑,只在
-- schema_migrations 记一个版本。
--
-- 为什么现在才建这个集:pandora_bag 与 pandora_owner 一样是「后来新增的库」,一直没有
-- 迁移集。dev_migrate.ps1 的第 0 步重放 mysql-init 只能补**新表**(IF NOT EXISTS),补不了
-- **加列**;没有迁移集就等于这个库的任何列改动在存量库上永远不会自动进库,新镜像上线
-- 即崩(owner 的 hub_source_revision 已经踩过一次,INC-20260818-003)。本 baseline 先把
-- 落脚点建好:下一次 bag 加列直接写 000002,不必再临时补历史。
--
-- 没有 `USE`:迁移器连的就是目标库,库名由 targets 清单锁定。


CREATE TABLE IF NOT EXISTS `bag_meta` (
    `player_id`        BIGINT UNSIGNED NOT NULL,
    `owner_epoch`      BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当前写者 epoch(单调不回退;CAS 推进,旧 epoch 写一律拒)',
    `last_journal_seq` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '已应用 journal 水位(前缀确认)',
    `updated_at`       DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='背包域每玩家 fencing 锚点 + journal 水位';

CREATE TABLE IF NOT EXISTS `bag_checkpoint` (
    `player_id`           BIGINT UNSIGNED NOT NULL,
    `snapshot`            MEDIUMBLOB      NOT NULL COMMENT 'pb BagStorageRecord(仅随身组段;§9.17 兼容演进)',
    `covered_journal_seq` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '快照覆盖到的 journal 水位(恢复=快照+其后尾部重放)',
    `updated_at`          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='背包随身组 checkpoint 快照';

CREATE TABLE IF NOT EXISTS `bag_section` (
    `player_id`  BIGINT UNSIGNED NOT NULL,
    `bag_type`   INT UNSIGNED    NOT NULL COMMENT '1 仓库 / 100+ 活动段(后端驻留组;随身组不落本表)',
    `generation` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '活动段代际(固定段恒 0);读过滤 current,写校验 current',
    `section`    MEDIUMBLOB      NOT NULL COMMENT 'pb BagSection(§9.17 兼容演进)',
    `updated_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`, `bag_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='背包后端驻留段本体(与 journal 同事务变更)';

CREATE TABLE IF NOT EXISTS `bag_journal` (
    `id`              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `player_id`       BIGINT UNSIGNED NOT NULL,
    `journal_seq`     BIGINT UNSIGNED NOT NULL COMMENT '每玩家单调,从 1 起(前缀确认)',
    `owner_epoch`     BIGINT UNSIGNED NOT NULL COMMENT '写入时的 owner_epoch(审计 + 事故回放)',
    `op_type`         TINYINT         NOT NULL COMMENT '1 pickup_grant / 2 mail_claim / 3 transfer / 4 consume(对齐 bag.proto oneof)',
    `bag_type`        INT UNSIGNED    NOT NULL COMMENT '操作主段',
    `generation`      BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '主段代际',
    `payload`         BLOB            NOT NULL COMMENT 'pb BagJournalEntry(整条原文;重放 / 审计)',
    `idempotency_key` VARCHAR(128)    NOT NULL,
    `fingerprint`     CHAR(64)        NOT NULL COMMENT '请求内容指纹(sha256;同 key 不同内容 → 幂等冲突,防 key 复用串改账)',
    `created_at`      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_seq` (`player_id`, `journal_seq`),
    UNIQUE KEY `uk_player_idem` (`player_id`, `idempotency_key`),
    KEY `idx_created_at` (`created_at`) COMMENT '90 天保留期 sweep 扫描索引(§9.24;删除资格另需 checkpoint 覆盖,INC-20260722-003)',
    KEY `idx_player_created` (`player_id`, `created_at`) COMMENT '单玩家滑窗额度统计(五要件④)'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='背包流水(同步 journal;零丢失事件的持久化点)';

-- 旧 inventory 存量迁移幂等闸(decision-revisit-bag-replay-semantics.md D5,2026-07-22)。
--   contract 阶段旧写路径(GrantItems/UseItem/SellItem/escrow)冻结后,迁移作业把
--   player_items / player_item_instance 全量迁入仓库段(超容落位只出不进);
--   一玩家一行 = 永久幂等闸(重跑 / 多副本并发安全),行内计数供迁后总量对账。
--   增长有界:被玩家数有界,永不清理(同 leaderboard_settlement 豁免类,§9.24)。
CREATE TABLE IF NOT EXISTS `bag_migration` (
    `player_id`      BIGINT UNSIGNED NOT NULL,
    `stack_kinds`    INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '迁移的堆叠 config 种数(对账)',
    `stack_total`    BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '迁移的堆叠总数量(对账)',
    `instance_count` INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '迁移的实例件数(对账)',
    `migrated_at`    DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='旧 inventory 存量迁移幂等闸(一玩家一行,永久保留;§9.24 豁免登记)';

-- 玩家已购容量增量(bag-domain.md §5.3,2026-07-22 拍板)。
--   有效容量 = base(bag_type 配置) + extra;extra 单调只增(不支持退款回缩)、被配置
--   硬上限封顶(§9.18)。购买 = 经济域扣金币(inventory_ledger 幂等)+ 本表档数 CAS
--   两步 saga,幂等身份 = (player_id, bag_type, 第 purchases+1 档)。
--   增长有界:每玩家 × 可买段各一行(bounded,dbcheck 登记)。
CREATE TABLE IF NOT EXISTS `bag_capacity` (
    `player_id`  BIGINT UNSIGNED NOT NULL,
    `bag_type`   INT UNSIGNED    NOT NULL COMMENT '可买段(当前 0 身上 / 1 仓库,服务端配置权威)',
    `extra`      INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '已购增量总格数(单调只增,硬上限封顶)',
    `purchases`  INT UNSIGNED    NOT NULL DEFAULT 0 COMMENT '已购档数(阶梯价游标;购买幂等身份成分)',
    `updated_at` DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`, `bag_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='玩家背包容量已购增量(§5.3;bounded)';

CREATE TABLE IF NOT EXISTS `bag_generation` (
    `bag_type`           INT UNSIGNED    NOT NULL COMMENT '活动段类型(100+)',
    `current_generation` BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '当前代际(单调推进;切代即旧代逻辑清空)',
    `salvage_mode`       TINYINT         NOT NULL DEFAULT 0 COMMENT '切代物资去向:0 discard(默认) / 1 mail 补发(补发工具 phase 3)',
    `updated_at`         DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`bag_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='活动背包代际权威(类型重用;运营切代入口)';
