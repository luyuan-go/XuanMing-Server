-- 段位分按 rating_pool 分区(2026-08-11)。
--
-- 产品口径「3v3 与 5v5 不共用同一份段位」要求一个玩家持有多份分,`players.mmr` 单列
-- 表达不了。分区键来自关卡表「段位池」列,由 matchmaker 成局定格 → canonical
-- BattleStorageRecord → battle_result 结算按本值入账。
--
-- ⚠️ 本迁移**不搬运存量段位分**:按用户 2026-08-11 指令,已有玩家数据清空,所有人
-- 在各池从基线分重新起算。若将来需要保留存量,必须先决定"单值怎么分裂成多份"
-- (复制到每个池 / 只给某一池 / 全部重置),那是产品决策,不能由迁移脚本替它选。
CREATE TABLE IF NOT EXISTS `player_mmr` (
    `player_id`   BIGINT UNSIGNED  NOT NULL,
    `rating_pool` VARCHAR(32)      NOT NULL COMMENT '段位池(分区键);空值在应用层归一为 default',
    `mmr`         INT              NOT NULL DEFAULT 1500 COMMENT '该池段位分,floor 0 由应用层保证',
    `updated_at`  DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`, `rating_pool`),
    KEY `idx_pool_mmr` (`rating_pool`, `mmr`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家分池段位分(段位唯一权威;PK=player_id+rating_pool)';

-- mmr_history 记录本次入账落在哪一份段位(审计与排查用;幂等键仍是 player_id+idempotency_key,
-- 刻意不把 rating_pool 并入幂等键 —— 一场对局只属于一个池,并入后同一 match 换个池名就能重复入账)。
SET @has_pool := (SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mmr_history' AND COLUMN_NAME = 'rating_pool');
SET @stmt := IF(@has_pool = 0,
    "ALTER TABLE `mmr_history` ADD COLUMN `rating_pool` VARCHAR(32) NOT NULL DEFAULT 'default' COMMENT '段位池(分区键)' AFTER `idempotency_key`, ALGORITHM=INSTANT",
    'SELECT 1');
PREPARE s FROM @stmt; EXECUTE s; DEALLOCATE PREPARE s;

-- players.mmr 退役:段位不再是账号级单值。条件删列(幂等,已删过则跳过)。
SET @has_mmr := (SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'players' AND COLUMN_NAME = 'mmr');
SET @stmt := IF(@has_mmr > 0, 'ALTER TABLE `players` DROP COLUMN `mmr`, ALGORITHM=INSTANT', 'SELECT 1');
PREPARE s FROM @stmt; EXECUTE s; DEALLOCATE PREPARE s;
