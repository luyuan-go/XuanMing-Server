-- 000001_baseline — 任务域四表(docs/design/mission.md §3;语义移植自 luyuan/mmorpg)。
--
--   player_mission_active  活跃任务(每玩家每任务一行;≤ max_active_missions,§9.18)
--   player_mission_done    完成集 + 领奖状态(被任务表行数有界)
--   mission_reward_log     发奖流水 PENDING/GRANTED/FAILED(对齐 leaderboard_reward_log)
--   mission_fact_receipts  事实上报幂等收据(清理默认关,同 exp_history 之因)
--   mission_player_guards  每玩家写守卫行(TiDB 无 gap 锁;同 friend_player_guards)
--
-- 与 deploy/mysql-init/16-mission-tables.sql 同构(fresh-init 走那边,存量库走本迁移)。
-- 纯 additive(§9.16 不停服):只建新表。

CREATE TABLE IF NOT EXISTS `player_mission_active` (
    `id`                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `player_id`         BIGINT UNSIGNED NOT NULL,
    `mission_config_id` INT UNSIGNED    NOT NULL COMMENT '任务配置 ID(uint32;r_任务.xlsx)',
    `progress`          VARBINARY(256)  NOT NULL DEFAULT '' COMMENT '每条件槽进度(pb MissionProgressStorageRecord;槽数≤8,写入过 dbguard.CheckPayload)',
    `accepted_at_ms`    BIGINT          NOT NULL COMMENT '接取时间(毫秒)',
    `created_at`        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_mission` (`player_id`, `mission_config_id`),
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家活跃任务(每玩家 ≤ max_active_missions 行,§9.18 写入侧上限)';

CREATE TABLE IF NOT EXISTS `player_mission_done` (
    `id`                BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `player_id`         BIGINT UNSIGNED NOT NULL,
    `mission_config_id` INT UNSIGNED    NOT NULL COMMENT '任务配置 ID(uint32)',
    `reward_state`      TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '0 无需领取(无奖励/已自动发) 1 可领取 2 已领取',
    `completed_at_ms`   BIGINT          NOT NULL COMMENT '完成时间(毫秒)',
    `created_at`        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`        DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_mission` (`player_id`, `mission_config_id`),
    KEY `idx_player_state` (`player_id`, `reward_state`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家已完成任务 + 领奖状态(每玩家每任务至多 1 行,被任务表行数有界)';

CREATE TABLE IF NOT EXISTS `mission_reward_log` (
    `id`                    BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `player_id`             BIGINT UNSIGNED NOT NULL,
    `mission_config_id`     INT UNSIGNED    NOT NULL COMMENT '任务配置 ID(审计定位)',
    `grant_idempotency_key` VARCHAR(96)     NOT NULL COMMENT '发奖幂等键 mission:<player_id>:<mission_config_id>(不变量 §9.7)',
    `status`                TINYINT         NOT NULL DEFAULT 0 COMMENT '0 PENDING 1 GRANTED 2 FAILED(PENDING/FAILED 是补发工作集)',
    `reward_pb`             VARBINARY(2048) NOT NULL DEFAULT '' COMMENT '发放内容快照(pb MissionRewardStorageRecord;补发重放唯一入参)',
    `created_at_ms`         BIGINT          NOT NULL COMMENT '创建时间(毫秒)',
    `updated_at_ms`         BIGINT          NOT NULL COMMENT '最后更新时间(毫秒)',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_grant_idem` (`grant_idempotency_key`),
    KEY `idx_player` (`player_id`),
    KEY `idx_status_updated` (`status`, `updated_at_ms`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 任务发奖流水(uk grant_idem 防重复发奖;GRANTED 90 天清,PENDING/FAILED 永不清)';

CREATE TABLE IF NOT EXISTS `mission_push_outbox` (
    `id`            BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `player_id`     BIGINT UNSIGNED NOT NULL,
    `payload`       VARBINARY(2048) NOT NULL COMMENT 'MissionUpdateEvent proto bytes(biz 侧按大小分片,写入过 dbguard.CheckPayload)',
    `created_at_ms` BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 任务推送事务出箱(与状态写同事务;发布器投 kafka pandora.mission.update,成功即删行)';

CREATE TABLE IF NOT EXISTS `mission_fact_receipts` (
    `id`                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `player_id`           BIGINT UNSIGNED NOT NULL,
    `idempotency_key`     VARCHAR(128)    NOT NULL COMMENT '事实上报幂等键(如 progress:<match>:<seq>:<player>:mission)',
    `request_fingerprint` VARBINARY(32)   NOT NULL COMMENT '请求内容 sha256;同键不同指纹 fail-closed 拒',
    `created_at`          DATETIME        NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_key` (`player_id`, `idempotency_key`),
    KEY `idx_created` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 任务事实上报幂等收据(at-least-once 吸收;清理默认关,见 §9.24)';

-- 每玩家写守卫行(与 friend_player_guards 同款,理由见 000006_friend_guard_tables.up.sql):
-- TiDB 悲观事务没有 gap/next-key 锁,`SELECT ... FROM player_mission_active WHERE
-- player_id=? FOR UPDATE` 只锁**已存在**的行,零行时根本不加锁——两个并发 AcceptMission
-- 都读到同一份活跃集,双双通过 max_active_missions 上限与 (type,sub_type) 互斥校验后各插一行。
-- 对已存在行的点锁在 MySQL InnoDB 与 TiDB 悲观事务下语义一致(缺的是范围锁),
-- 所以所有玩家级临界区先 `INSERT ... ON DUPLICATE KEY UPDATE` 取守卫行锁再进。
-- 每玩家至多 1 行,被玩家数有界 → §9.24 登记豁免(同 friend_player_guards)。
CREATE TABLE IF NOT EXISTS `mission_player_guards` (
    `player_id` BIGINT UNSIGNED NOT NULL COMMENT '守卫行归属玩家(锁粒度=单玩家任务域)',
    PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 任务域每玩家写守卫行(活跃上限/类型互斥校验串行化;§9.24 豁免)';

-- 清理索引条件补齐(幂等;CREATE TABLE IF NOT EXISTS 对"表存在但形态不同"是静默 no-op,
-- dbcheck 把清理索引当发布门禁项,必须自己兜住 —— 与 pandora_player 000005 同一写法)。
SET @ddl := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mission_reward_log' AND INDEX_NAME = 'idx_status_updated') = 0,
    'ALTER TABLE `mission_reward_log` ADD KEY `idx_status_updated` (`status`, `updated_at_ms`), ALGORITHM=INPLACE',
    'SELECT 1');
PREPARE add_mission_reward_log_idx FROM @ddl;
EXECUTE add_mission_reward_log_idx;
DEALLOCATE PREPARE add_mission_reward_log_idx;

SET @ddl := IF(
    (SELECT COUNT(*) FROM information_schema.STATISTICS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'mission_fact_receipts' AND INDEX_NAME = 'idx_created') = 0,
    'ALTER TABLE `mission_fact_receipts` ADD KEY `idx_created` (`created_at`), ALGORITHM=INPLACE',
    'SELECT 1');
PREPARE add_mission_fact_receipts_idx FROM @ddl;
EXECUTE add_mission_fact_receipts_idx;
DEALLOCATE PREPARE add_mission_fact_receipts_idx;
