-- 段位分区滚动升级兼容 expand(2026-08-12)。
--
-- 000007 已发布且曾删除 players.mmr；旧 player 副本仍会读写该列，导致 Stable / Canary
-- 无法共存。本迁移只做加法：恢复旧列作为 default 池的兼容投影。待所有旧副本排空、
-- 经过独立 contract 发布后，才能在更高版本迁移中删除，禁止在本 expand 内 DROP。
SET @has_mmr := (SELECT COUNT(*) FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'players' AND COLUMN_NAME = 'mmr');
SET @stmt := IF(@has_mmr = 0,
    "ALTER TABLE `players` ADD COLUMN `mmr` INT NOT NULL DEFAULT 1500 COMMENT '旧副本兼容投影(default 池);contract 前保留' AFTER `exp`, ALGORITHM=INSTANT",
    'SELECT 1');
PREPARE s FROM @stmt; EXECUTE s; DEALLOCATE PREPARE s;

-- 若 000007 后已经有新版副本写过 default 池，把可恢复的值投影回旧列。没有 default
-- 行的玩家保留 1500；000007 明确采用存量段位重置口径，已删除的旧值无法推测恢复。
UPDATE `players` AS p
JOIN `player_mmr` AS pm
  ON pm.`player_id` = p.`player_id` AND pm.`rating_pool` = 'default'
-- 显式自赋 last_seen_at，避免该列的 ON UPDATE 把离线玩家误标成“刚上线”。
SET p.`mmr` = pm.`mmr`, p.`last_seen_at` = p.`last_seen_at`;

-- 旧副本/旧榜单可能仍依赖 idx_mmr；条件补齐避免 fresh-init 已带索引时重复失败。
SET @has_idx_mmr := (SELECT COUNT(*) FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'players' AND INDEX_NAME = 'idx_mmr');
SET @stmt := IF(@has_idx_mmr = 0,
    'ALTER TABLE `players` ADD KEY `idx_mmr` (`mmr`), ALGORITHM=INPLACE',
    'SELECT 1');
PREPARE s FROM @stmt; EXECUTE s; DEALLOCATE PREPARE s;
