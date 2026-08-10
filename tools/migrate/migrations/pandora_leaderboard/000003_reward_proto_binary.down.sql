-- 回滚(⚠️ 仅 dev):reward_pb → reward_json,同样丢弃列值(两种编码不同构)。
-- 生产严禁执行:回滚会让未发成的奖励失去补发入参。

SET @pandora_col_new := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'leaderboard_reward_log'
      AND column_name = 'reward_pb'
);
SET @pandora_sql := IF(
    @pandora_col_new = 1,
    'ALTER TABLE `leaderboard_reward_log` DROP COLUMN `reward_pb`, ADD COLUMN `reward_json` VARCHAR(2048) NOT NULL DEFAULT '''' COMMENT ''发放道具明细(审计;[{item_config_id,count}])'' AFTER `status`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;
