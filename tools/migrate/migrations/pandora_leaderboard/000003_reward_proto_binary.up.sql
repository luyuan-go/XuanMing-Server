-- 发奖明细列 reward_json(VARCHAR JSON) → reward_pb(VARBINARY,proto 二进制)
-- (2026-08-07,CLAUDE.md §5.8/§9.17;口径同 pandora_trade 000004)。
--
-- reward_pb 不只是审计:RetryUngrantedRewards 补发时要把它解回奖励列表重放,
-- 编码格式漂移会让整条发奖记录变成"永远补不出来的脏数据"。proto 二进制带字段编号
-- 与 unknown fields 保留,新旧副本共存下可安全演进。
--
-- 存量数据处理(2026-08-07 人拍板"干净切换"):旧 JSON 文本与 pb 不同构,DROP + ADD
-- 丢弃旧列值。**PENDING/FAILED 行的补发入参会一并丢失** → 迁移前必须确认无待补发行
-- (SELECT COUNT(*) FROM leaderboard_reward_log WHERE status <> 1),否则先等补扫跑完。
--
-- 幂等:只在旧列还在时执行(fresh 库由 baseline 直接建 reward_pb,count=0 → no-op)。
--
-- 这里"一条 ALTER 里 DROP 旧列 + ADD 新列"是**换名**,TiDB v8.5.1 已实测可用,故保持单条。
-- 注意别照抄成"删了再加同一个列名"——那种写法 TiDB 会报 ERROR 1060 Duplicate column name
-- (multi-schema change 拿每个子句去比对原表结构),必须拆两条,见
-- pandora_trade/000004_attributes_proto_binary.up.sql 顶部注释。

SET @pandora_col_old := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'leaderboard_reward_log'
      AND column_name = 'reward_json'
);
SET @pandora_sql := IF(
    @pandora_col_old = 1,
    'ALTER TABLE `leaderboard_reward_log` DROP COLUMN `reward_json`, ADD COLUMN `reward_pb` VARBINARY(2048) NOT NULL DEFAULT '''' COMMENT ''发放道具明细(审计 + 补发重放入参;pb RewardGrantStorageRecord)'' AFTER `status`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;
