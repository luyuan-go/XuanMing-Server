-- 装备实例词条列 JSON → proto 二进制(2026-08-07,CLAUDE.md §5.8/§9.17)。
--
-- 为什么改:MySQL 里的非基础类型(集合 / 嵌套结构)一律用 proto 二进制序列化,不用 JSON。
-- JSON 没有字段编号语义(改名 / 加字段靠手写 tag 对齐)、不保留 unknown fields
-- (新旧副本 read-modify-write 会静默丢新字段,违反 §9.17),也无法与 pb 存储快照共用契约。
--
-- 存量数据处理(2026-08-07 人拍板"干净切换,不保留旧 JSON"):旧 JSON 文本与 pb 二进制
-- 不同构,原地 MODIFY 会让 proto.Unmarshal 读到 JSON 文本字节(报错或解出垃圾),故用
-- DROP + ADD 一次性丢弃旧列值——**未上线阶段的一次性动作**,不是常规做法。
--
-- ⚠️ DROP 与 ADD 必须拆成两条 ALTER,不准合并成一条语句里的两个子句(2026-08-09 实测)。
-- MySQL 8 按子句先后处理,所以合并写法在 MySQL 上能过;TiDB 的 multi-schema change 却是
-- 拿**每个子句**去和该语句执行前的原表结构比对,于是 ADD 看到那个还没被删掉的同名列:
--     ERROR 1060 (42S21): Duplicate column name 'attributes'
-- (TiDB v8.5.1 本机实测:整条语句被拒且原子回滚,表结构不变——所以失败不会留半迁移。)
-- 换名列(一条里 DROP a + ADD b)在 TiDB 可用,只有"删掉再加同一个列名"不行。
--
-- 幂等:DROP 只在列仍是 json 时执行;ADD 的条件**现查列是否存在**,不复用 DROP 的判断结果
-- ——两条语句之间进程被杀会留下"已删未加",现查才能在重跑时自愈。fresh 库由 baseline 直接
-- 建 VARBINARY,两步都是 no-op。
-- 发布顺序:先跑本迁移,再滚动发布新 inventory 版本(旧二进制读到 NULL 词条 = 未鉴定视图,
-- 不会崩;开发期实例背包正走 bag 域迁移,词条重刷由鉴定链自然补齐)。

-- ---------------------------------------------------------------------------
-- player_item_instance.attributes
-- ---------------------------------------------------------------------------
SET @pandora_drop_json := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'player_item_instance'
      AND column_name = 'attributes'
      AND data_type = 'json'
);
SET @pandora_sql := IF(
    @pandora_drop_json = 1,
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
    'ALTER TABLE `player_item_instance` ADD COLUMN `attributes` VARBINARY(1024) NULL COMMENT ''鉴定后随机属性(pb ItemInstanceAttributesStorageRecord);未鉴定为 NULL'' AFTER `identified`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;

-- ---------------------------------------------------------------------------
-- mail_transfer_escrow.attributes
-- ---------------------------------------------------------------------------
SET @pandora_drop_json := (
    SELECT COUNT(*)
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'mail_transfer_escrow'
      AND column_name = 'attributes'
      AND data_type = 'json'
);
SET @pandora_sql := IF(
    @pandora_drop_json = 1,
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
    'ALTER TABLE `mail_transfer_escrow` ADD COLUMN `attributes` VARBINARY(1024) NULL COMMENT ''扣出时词条原样保留(pb ItemInstanceAttributesStorageRecord);未鉴定为 NULL'' AFTER `identified`',
    'SELECT 1'
);
PREPARE pandora_stmt FROM @pandora_sql;
EXECUTE pandora_stmt;
DEALLOCATE PREPARE pandora_stmt;
