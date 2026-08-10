-- Pandora 通用排行榜 结算归档表结构(2026-06-27)
--
-- 装载方式:容器 entrypoint 自动扫 /docker-entrypoint-initdb.d/*.sql 顺序执行
-- (01-create-databases.sql 先建库 + grant,本文件接着建表)。
--
-- 设计依据:docs/design/decision-revisit-leaderboard.md
--   - 排行是「派生数据」:进行中的实时排名 / 临时榜只在 Redis ZSET,不落库(性能)。
--   - MySQL 只兜「结算结果 + 发奖凭证」(§9 #2 结算只落一次 / #7 发放原子 + 补偿幂等键):
--       leaderboard_settlement  结算批次头(uk settle_idempotency_key 防重复结算)
--       leaderboard_snapshot    结算 Top-N 名次快照(赛季回看 / 客服对账 / 审计)
--       leaderboard_reward_log  逐名次发奖记录(uk grant_idempotency_key 防重复发奖)
--
-- 约定:
--   - settlement_id / entity_id / scope_id 是雪花 / 业务 uint64(BIGINT UNSIGNED,§11)
--   - board_type 是配置 ID(uint32,§12);scope 是 LeaderboardScope(int)
--   - score / rank 用 BIGINT(可累积大额 / 大榜)

USE `pandora_leaderboard`;

CREATE TABLE IF NOT EXISTS `leaderboard_settlement` (
    `settlement_id`          BIGINT UNSIGNED NOT NULL COMMENT '雪花结算批次 ID',
    `board_type`             INT UNSIGNED    NOT NULL COMMENT '榜类型(配置 id,uint32)',
    `scope`                  TINYINT         NOT NULL COMMENT 'LeaderboardScope:1 GLOBAL 2 GUILD 3 INSTANCE 4 CUSTOM',
    `scope_id`               BIGINT UNSIGNED NOT NULL DEFAULT 0 COMMENT 'GUILD→guild_id / INSTANCE→match_id / GLOBAL→0',
    `period`                 VARCHAR(32)     NOT NULL DEFAULT '' COMMENT '周期标识("" / 2026-W26 / S5)',
    `top_n`                  INT             NOT NULL COMMENT '本次结算取前 N 名',
    `settled_count`          INT             NOT NULL DEFAULT 0 COMMENT '实际结算名次数',
    `settle_idempotency_key` VARCHAR(96)     NOT NULL COMMENT '防重复结算(不变量 §9.2)',
    `reset_after`            TINYINT         NOT NULL DEFAULT 0 COMMENT '结算后是否清空榜',
    `created_at_ms`          BIGINT          NOT NULL COMMENT '结算时间(毫秒)',
    PRIMARY KEY (`settlement_id`),
    UNIQUE KEY `uk_settle_idem` (`settle_idempotency_key`),
    KEY `idx_board` (`board_type`, `scope`, `scope_id`, `period`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 排行榜结算批次(uk settle_idem 防重复结算)';

CREATE TABLE IF NOT EXISTS `leaderboard_snapshot` (
    `settlement_id` BIGINT UNSIGNED NOT NULL COMMENT '所属结算批次',
    `rank`          BIGINT          NOT NULL COMMENT '1-based 名次',
    `entity_id`     BIGINT UNSIGNED NOT NULL COMMENT 'player_id / guild_id',
    `score`         BIGINT          NOT NULL COMMENT '结算时真实分数',
    `created_at_ms` BIGINT          NOT NULL COMMENT '落快照时间(毫秒)',
    PRIMARY KEY (`settlement_id`, `rank`),
    KEY `idx_entity` (`entity_id`),
    KEY `idx_created` (`created_at_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 排行榜结算 Top-N 名次快照(归档 / 对账;保留 90 天由 leaderboard 保留期清理回收,§9.24)';
-- 既有库需手动补:ALTER TABLE leaderboard_snapshot ADD KEY idx_created (created_at_ms);

CREATE TABLE IF NOT EXISTS `leaderboard_reward_log` (
    `id`                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    `settlement_id`         BIGINT UNSIGNED NOT NULL COMMENT '所属结算批次',
    `entity_id`             BIGINT UNSIGNED NOT NULL COMMENT '获奖 player_id / guild_id',
    `rank`                  BIGINT          NOT NULL COMMENT '获奖名次',
    `grant_idempotency_key` VARCHAR(96)     NOT NULL COMMENT '发奖幂等键 lb:<settlement_id>:<entity_id>(不变量 §9.7)',
    `status`                TINYINT         NOT NULL DEFAULT 0 COMMENT '0 PENDING 1 GRANTED 2 FAILED',
    `reward_pb`             VARBINARY(2048) NOT NULL DEFAULT '' COMMENT '发放道具明细(审计 + 补发重放入参;pb RewardGrantStorageRecord)',
    `created_at_ms`         BIGINT          NOT NULL COMMENT '创建时间(毫秒)',
    `updated_at_ms`         BIGINT          NOT NULL COMMENT '最后更新时间(毫秒)',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_grant_idem` (`grant_idempotency_key`),
    KEY `idx_settlement` (`settlement_id`),
    KEY `idx_entity` (`entity_id`),
    KEY `idx_status_updated` (`status`, `updated_at_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 排行榜逐名次发奖记录(uk grant_idem 防重复发奖;GRANTED 行保留 90 天由 leaderboard 保留期清理回收,PENDING/FAILED 永不清,§9.24)';
-- 既有库需手动补:ALTER TABLE leaderboard_reward_log ADD KEY idx_status_updated (status, updated_at_ms);
