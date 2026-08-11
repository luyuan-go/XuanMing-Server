-- 000005_skill_cards — 技能卡:持有(等级 + 碎片)、卡槽装配、发放幂等收据。
--
-- 域模型:卡本体是配置(j_技能卡.xlsx),这里只存玩家的持有状态与装配。
--   player_skill_cards  一行 = 玩家持有的一张卡(等级 + 碎片余量)
--   player_skill_slots  一行 = 一个卡槽装了哪张卡(全量替换语义)
--   skill_card_grants   发放幂等收据(对齐 talent_point_grants / attr_point_grants)
--
-- 碎片为什么留在本库而不是走 inventory:升级要在**一个事务**里同时扣碎片和加等级。
-- 碎片放 inventory 就变成跨服务两阶段(扣了碎片但升级失败 = 玩家凭空掉材料),
-- 本仓没有为此铺 saga 的先例,也不该为一个养成功能引入。抽卡侧发碎片走
-- GrantSkillCards 的幂等收据,与 talent 点数发放同一套做法。
--
-- 纯 additive(§16/§17 不停服):只建新表,不动既有表。

CREATE TABLE IF NOT EXISTS `player_skill_cards` (
    `id`         BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`  BIGINT UNSIGNED  NOT NULL,
    `card_id`    INT UNSIGNED     NOT NULL COMMENT '技能卡配置 ID(uint32;j_技能卡.xlsx)',
    `level`      INT UNSIGNED     NOT NULL DEFAULT 1 COMMENT '已培养等级(获得即 1 级;上限查配置表)',
    `shards`     INT UNSIGNED     NOT NULL DEFAULT 0 COMMENT '碎片余量(升级从这里扣;重复获得同名卡转碎片)',
    `updated_at` DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_card` (`player_id`, `card_id`),
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家技能卡持有(等级 + 碎片余量)';

CREATE TABLE IF NOT EXISTS `player_skill_slots` (
    `id`         BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`  BIGINT UNSIGNED  NOT NULL,
    `slot`       INT UNSIGNED     NOT NULL COMMENT '卡槽序号(0..N-1;N 由服务端配置,当前 4 = 战斗 Q/W/E/R)',
    `card_id`    INT UNSIGNED     NOT NULL COMMENT '装在该槽的技能卡 ID(>0;空槽不落行,直接删)',
    `updated_at` DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_slot` (`player_id`, `slot`),
    -- 同一张卡不得同时占两个槽。放成库级唯一约束而不是只在 biz 里判:
    -- 并发两次 SetSkillSlots 各自校验都通过、合并后却撞车的窗口,只有库能兜住。
    UNIQUE KEY `uk_player_card_once` (`player_id`, `card_id`),
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家技能卡槽装配(全量替换;空槽不落行)';

CREATE TABLE IF NOT EXISTS `skill_card_grants` (
    `id`              BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `player_id`       BIGINT UNSIGNED NOT NULL,
    `idempotency_key` VARCHAR(128)    NOT NULL COMMENT '发放幂等键(抽卡/活动/GM 各自的单点入口约定)',
    `created_at`      DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_key` (`player_id`, `idempotency_key`),
    KEY `idx_player` (`player_id`),
    -- 保留期清理按 created_at 扫(§9.24 只增表必须有可用清理索引;dbcheck 会断言本索引存在)
    KEY `idx_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 技能卡发放幂等收据(保留期清理默认 report_only,见 §9.24)';

-- 清理索引条件补齐(幂等,与 000003_retention_indexes 同一写法)。
--
-- 为什么建表里已经有还要再补一次:上面用的是 `CREATE TABLE IF NOT EXISTS`,对"表已存在
-- 但形态不同"是**静默 no-op** —— 先跑过 deploy/mysql-init 早期版本、或开发期手搓过同名表的
-- 库,会带着一张没有 idx_created 的 skill_card_grants 进入 v5,而 dbcheck 把这条索引当发布
-- 门禁项(缺索引直接 FAIL),届时没有任何迁移能把它补回来。清理路径的索引不能依赖
-- "建表那次恰好建对了",必须自己兜住。表本来就带索引时这里退化成 SELECT 1。
--
-- 只兜 idx_created 不兜 uk_*:唯一键在有重复数据的表上加会失败,那种库属于开发期手搓残留,
-- 正确处置是人工 DROP 掉重来,而不是让迁移在生产链路上尝试一个可能失败的 DDL。
SET @ddl := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'skill_card_grants' AND INDEX_NAME = 'idx_created') = 0,
    'ALTER TABLE `skill_card_grants` ADD KEY `idx_created` (`created_at`), ALGORITHM=INPLACE',
    'SELECT 1');
PREPARE add_skill_card_grants_idx FROM @ddl;
EXECUTE add_skill_card_grants_idx;
DEALLOCATE PREPARE add_skill_card_grants_idx;
