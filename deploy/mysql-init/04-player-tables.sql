-- Pandora 玩家库 W4 ④ 表结构(2026-06-06)
--
-- 装载方式:容器 entrypoint 自动扫 /docker-entrypoint-initdb.d/*.sql 顺序执行
-- (01-create-databases.sql 先建库 + grant,本文件接着建表)。
--
-- 表清单(对齐 docs/design/infra.md §2.1 pandora_player):
--   players       玩家档案(player_id PK,昵称 / 等级 / 段位 mmr / 战绩计数 / 出战英雄 / 属性点)
--   player_heroes 英雄解锁记录(uk player_id+hero_id)
--   mmr_history   MMR 变化历史 + 幂等键(uk player_id+idempotency_key,不变量 §2)
--   player_attributes 属性加点已分配点(uk player_id+attr_key)
--   attr_point_grants 属性点授予幂等表(uk player_id+idempotency_key)
--   player_equipment  出战装备预设(uk player_id+slot)
--   player_talents    天赋树已点分配(uk player_id+talent_id,re-spec 全量替换)
--   talent_point_grants 天赋点授予幂等表(uk player_id+idempotency_key)
--
-- 约定:
--   - player_id 由 login 服务用 snowflake 生成(BIGINT UNSIGNED),player 服务不生成
--   - mmr 缺省 1500(与 battle_result base_mmr 对齐),floor 0 由应用层保证
--   - UpdateMMR 幂等:idempotency_key 一般是 match_id;mmr_history uk 命中即视为已处理
--   - 默认昵称 = 配置前缀 + player_id,保证 uk_nickname 不冲突

USE `pandora_player`;

CREATE TABLE IF NOT EXISTS `players` (
    `player_id`     BIGINT UNSIGNED  NOT NULL,
    `nickname`      VARCHAR(64)      NOT NULL COMMENT '玩家昵称,uk_nickname 唯一',
    `level`         INT              NOT NULL DEFAULT 1,
    `exp`           BIGINT UNSIGNED  NOT NULL DEFAULT 0 COMMENT '级内经验(实时成长;满级恒 0)',
    `mmr`           INT              NOT NULL DEFAULT 1500 COMMENT '段位分,floor 0',
    `avatar`        VARCHAR(255)     NOT NULL DEFAULT '',
    `total_battles` INT              NOT NULL DEFAULT 0,
    `total_wins`    INT              NOT NULL DEFAULT 0,
    `active_hero_id`      INT UNSIGNED NOT NULL DEFAULT 0 COMMENT '选定出战英雄 hero_id(0=未选定)',
    `unspent_attr_points` INT          NOT NULL DEFAULT 0 COMMENT '未分配属性点',
    `total_talent_points` INT          NOT NULL DEFAULT 0 COMMENT '累计授予天赋点(可点 = total - SUM(player_talents.spent_points))',
    `created_at`    DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `last_seen_at`  DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`),
    UNIQUE KEY `uk_nickname` (`nickname`),
    KEY `idx_mmr` (`mmr`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家档案表';

CREATE TABLE IF NOT EXISTS `player_heroes` (
    `id`          BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`   BIGINT UNSIGNED  NOT NULL,
    `hero_id`     INT UNSIGNED     NOT NULL COMMENT '配置表英雄 ID(uint32)',
    `source`      VARCHAR(32)      NOT NULL DEFAULT '' COMMENT 'purchase | reward | freebie',
    `unlocked_at` DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_hero` (`player_id`, `hero_id`),
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家英雄解锁记录';

CREATE TABLE IF NOT EXISTS `mmr_history` (
    `id`              BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`       BIGINT UNSIGNED  NOT NULL,
    `idempotency_key` VARCHAR(64)      NOT NULL COMMENT '幂等键,一般是 match_id',
    `delta`           INT              NOT NULL COMMENT '本次 MMR 变化(可负)',
    `reason`          VARCHAR(32)      NOT NULL DEFAULT '' COMMENT 'win | lose | draw | abandon | rollback',
    `old_mmr`         INT              NOT NULL,
    `new_mmr`         INT              NOT NULL,
    `created_at`      DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_idem` (`player_id`, `idempotency_key`),
    KEY `idx_player_created` (`player_id`, `created_at`),
    KEY `idx_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家 MMR 变化历史 + 幂等键(不变量 §2;保留期清理见 §9.24,默认关)';
-- idx_created 服务保留期清理(player RunHistoryJanitor)。存量库由 tools/migrate
-- pandora_player 000003_retention_indexes 条件补齐。

CREATE TABLE IF NOT EXISTS `player_attributes` (
    `id`         BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`  BIGINT UNSIGNED  NOT NULL,
    `attr_key`   VARCHAR(32)      NOT NULL COMMENT '属性键: str | agi | int | vit 等',
    `points`     INT              NOT NULL DEFAULT 0 COMMENT '该属性已分配点(>=0)',
    `updated_at` DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_attr` (`player_id`, `attr_key`),
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家属性加点已分配点';

CREATE TABLE IF NOT EXISTS `attr_point_grants` (
    `id`              BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`       BIGINT UNSIGNED  NOT NULL,
    `idempotency_key` VARCHAR(64)      NOT NULL COMMENT '防重复授予(如 level_up:<level> / reward:<id>)',
    `points`          INT              NOT NULL COMMENT '本次授予点数(>0)',
    `created_at`      DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_grant` (`player_id`, `idempotency_key`),
    KEY `idx_player` (`player_id`),
    KEY `idx_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 属性点授予幂等表(保留期清理见 §9.24,默认关)';
-- 存量库由 tools/migrate pandora_player 000003_retention_indexes 条件补齐。

CREATE TABLE IF NOT EXISTS `player_equipment` (
    `id`              BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`       BIGINT UNSIGNED  NOT NULL,
    `slot`            INT UNSIGNED     NOT NULL COMMENT '出战装备预设槽位序号',
    `item_config_id`  INT UNSIGNED     NOT NULL COMMENT '装备配置 ID(uint32)',
    `updated_at`      DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_slot` (`player_id`, `slot`),
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家出战装备预设(大厅态;开战前转初始 GameplayEffect)';

CREATE TABLE IF NOT EXISTS `player_talents` (
    `id`         BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`  BIGINT UNSIGNED  NOT NULL,
    `talent_id`  INT UNSIGNED     NOT NULL COMMENT '天赋配置 ID(uint32)',
    `level`      INT              NOT NULL DEFAULT 0 COMMENT '该天赋节点已点等级(>0)',
    `spent_points` INT            NOT NULL DEFAULT 0 COMMENT '该节点实际消耗天赋点(= 等级 × 专精表每级消耗;读取侧据此算可点数,不按 level 反推)',
    `updated_at` DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_talent` (`player_id`, `talent_id`),
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家天赋树已点分配(re-spec 全量替换)';

-- ── 玩家等级经验(实时成长,2026-07-20 realtime-progression.md)────────────────
--
--   exp_history        经验入账历史 + 幂等键(uk player_id+idempotency_key,不变量 §2;
--                      幂等键口径:progress:{match_id}:{seq}:{player_id}:exp / quest:{player}:{quest} / gm:{op})
--   player_push_outbox 经验推送事务出箱:与入账同事务原子提交(不变量 §4),
--                      后台发布器投 kafka pandora.player.update(event_type 走 kafka header),
--                      成功即删行,表只保留待发布事件不会无限增长。

CREATE TABLE IF NOT EXISTS `exp_history` (
    `id`              BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`       BIGINT UNSIGNED  NOT NULL,
    `idempotency_key` VARCHAR(64)      NOT NULL COMMENT '幂等键(来源单点入口清单见 realtime-progression.md §4.4)',
    `exp_delta`       BIGINT UNSIGNED  NOT NULL COMMENT '本次入账经验(>0)',
    `reason`          VARCHAR(32)      NOT NULL DEFAULT '' COMMENT 'monster_kill | quest | gm',
    `old_level`       INT              NOT NULL,
    `old_exp`         BIGINT UNSIGNED  NOT NULL,
    `new_level`       INT              NOT NULL,
    `new_exp`         BIGINT UNSIGNED  NOT NULL,
    `created_at`      DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_idem` (`player_id`, `idempotency_key`),
    KEY `idx_player_created` (`player_id`, `created_at`),
    KEY `idx_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家经验入账历史 + 幂等键(实时成长,不变量 §2)';

CREATE TABLE IF NOT EXISTS `player_push_outbox` (
    `id`            BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`     BIGINT UNSIGNED  NOT NULL,
    `event_type`    INT UNSIGNED     NOT NULL COMMENT 'PlayerPushEventType(1=EXPERIENCE)',
    `payload`       VARBINARY(512)   NOT NULL COMMENT '对应事件 message 的 proto bytes',
    `created_at_ms` BIGINT           NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家推送事务出箱(经验等 player.update 域内事件,不变量 §4)';

CREATE TABLE IF NOT EXISTS `talent_point_grants` (
    `id`              BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`       BIGINT UNSIGNED  NOT NULL,
    `idempotency_key` VARCHAR(64)      NOT NULL COMMENT '防重复授予(如 level_up:<level> / reward:<id>)',
    `points`          INT              NOT NULL COMMENT '本次授予点数(>0)',
    `created_at`      DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_talent_grant` (`player_id`, `idempotency_key`),
    KEY `idx_player` (`player_id`),
    KEY `idx_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 天赋点授予幂等表(保留期清理见 §9.24,默认关)';
-- 存量库由 tools/migrate pandora_player 000003_retention_indexes 条件补齐。
