-- 回滚(⚠️ 仅 dev):VARBINARY(pb) → JSON,同样丢弃列值(两种编码不同构,无法逐行反解)。
-- 生产严禁执行:回滚会让已鉴定实例词条全部消失。
--
-- DROP / ADD 拆两条的理由同 up:TiDB 不接受一条 ALTER 里"删掉再加同一个列名"
-- (ERROR 1060 Duplicate column name),详见 .up.sql 顶部注释。

-- ---------------------------------------------------------------------------
-- player_item_instance.attributes
-- ---------------------------------------------------------------------------
SET @pandora_drop_bin := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'player_item_instance'
      AND column_name = 'attributes'
      AND data_type = 'varbinary'
);
SET @pandora_sql := IF(
    @pandora_drop_bin = 1,
    'ALTER TABLE `player_item_instance` DROP COLUMN `attributes`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

SET @pandora_col_missing := (
    SELECT COUNT(*) = 0
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'player_item_instance'
      AND column_name = 'attributes'
);
SET @pandora_sql := IF(
    @pandora_col_missing = 1,
    'ALTER TABLE `player_item_instance` ADD COLUMN `attributes` JSON NULL COMMENT ''鉴定后随机属性 [{"attr_id":n,"value":m}];未鉴定为 NULL'' AFTER `identified`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

-- ---------------------------------------------------------------------------
-- mail_transfer_escrow.attributes
-- ---------------------------------------------------------------------------
SET @pandora_drop_bin := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'mail_transfer_escrow'
      AND column_name = 'attributes'
      AND data_type = 'varbinary'
);
SET @pandora_sql := IF(
    @pandora_drop_bin = 1,
    'ALTER TABLE `mail_transfer_escrow` DROP COLUMN `attributes`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

SET @pandora_col_missing := (
    SELECT COUNT(*) = 0
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'mail_transfer_escrow'
      AND column_name = 'attributes'
);
SET @pandora_sql := IF(
    @pandora_col_missing = 1,
    'ALTER TABLE `mail_transfer_escrow` ADD COLUMN `attributes` JSON NULL COMMENT ''扣出时词条原样保留;未鉴定为 NULL'' AFTER `identified`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;
