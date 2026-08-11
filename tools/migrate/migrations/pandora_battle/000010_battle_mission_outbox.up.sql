-- 000010_battle_mission_outbox — 任务事实转发出箱(docs/design/mission.md §5.1)。
--
-- 为什么独立于 battle_progress_outbox 而不是加一个 kind:
--   两张表都按每玩家 FIFO 投递,但**故障域**必须隔开。任务行若混进进度出箱,mission
--   服务不可用就会卡住队首,连带阻塞该玩家的经验 / 掉落投递 —— 把弱依赖变成强依赖。
--   分表后 mission 故障只堵任务事实,经验与掉落照常发放。
--
-- ⚠️ 曾经的说法「任务事实顺序无关,所以本表可以乱序平摊投递」是**错的**,已按本迁移
--    的 idx 与 FetchMissionOutbox 的 NOT EXISTS 谓词收口:任务链上前后两环的条件类别
--    通常不同(「杀 5 只狼」→「收集 3 张狼皮」)。后环任务在前环完成时才被自动接取,
--    此前到达的「狼皮」事实匹配不上任何活跃任务,会被收据吸收后**静默丢弃且永不重放**。
--    因此同一 **player_id** 必须按 id(插入序)严格 FIFO 投递,失败行退避时后续行一起等。
--    分组维度是玩家不是对局:任务链跨对局延续,按 (match_id, player_id) 分组时 A 局卡住的
--    队首挡不住 B 局的行,玩家打完 A 再打 B 照样能让 B 的事实抢在 A 前面落地。
--    排序键用 id 而非 seq:seq 每对局自增、跨对局不可比;id 是插入序,对局内等价于 seq 序
--    (ApplyProgress 一事务一批、批内按 seq 升序插),跨对局等价于对局发生序
--    (§9.1 保证玩家同一时刻只在一个可操作 DS,不存在两局并行产事实)。
--
-- pending_action:USE_ITEM 类事实的「扣除未落定」闸。局内消费(battle_progress_action)
--   可能以业务失败终态收场(道具不足等),此时 inventory 一件没扣。若任务事实照发,
--   「使用 N 个 X」型任务就能靠上报根本没有的消耗刷进度。故 USE_ITEM 行落库即
--   pending_action=1(不可投递,但仍占队首挡住后续行),由 ResolveProgressAction 在
--   **扣除结果同一事务**里置 0(成功)或删行(失败)。
--
-- 幂等:一事实一行,uk(match_id, seq, player_id) 天然唯一(每事件自带唯一 seq 且属单玩家);
-- 投递幂等键 = progress:{match_id}:{seq}:{player_id}:mission,由 mission 侧收据表吸收重放。
--
-- 纯 additive(§9.16 不停服):只建新表。

CREATE TABLE IF NOT EXISTS `battle_mission_outbox` (
    `id`                 BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `match_id`           BIGINT UNSIGNED NOT NULL,
    `seq`                BIGINT UNSIGNED NOT NULL COMMENT '产生本事实的事件 seq(幂等键组成 + 每玩家 FIFO 排序键)',
    `player_id`          BIGINT UNSIGNED NOT NULL,
    `category`           INT UNSIGNED    NOT NULL COMMENT 'MissionConditionCategory:1 杀怪 / 4 使用道具 / 9 拾取',
    `slot_value`         INT UNSIGNED    NOT NULL COMMENT '槽位1值(怪物配置 ID / 道具配置 ID)',
    `amount`             INT UNSIGNED    NOT NULL COMMENT '进度增量(击杀数 / 拾取数 / 使用数)',
    `pending_action`     TINYINT UNSIGNED NOT NULL DEFAULT 0 COMMENT '1=等局内消费扣除落定(USE_ITEM);占队首但不投递,由 ResolveProgressAction 置 0 或删行',
    `next_attempt_at_ms` BIGINT          NOT NULL DEFAULT 0 COMMENT '投递失败指数退避后的下次尝试时点',
    `attempt_count`      INT UNSIGNED    NOT NULL DEFAULT 0,
    `created_at_ms`      BIGINT          NOT NULL DEFAULT 0,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_match_seq_player` (`match_id`, `seq`, `player_id`),
    -- 每玩家 FIFO 队首查询(NOT EXISTS 子查询按 (player_id, id) 找前驱;分组是**玩家**维度
    -- 而非对局维度 —— 任务链跨对局延续,按对局分组时 A 局卡住的队首挡不住 B 局的行)
    KEY `idx_mission_player_fifo` (`player_id`, `id`),
    KEY `idx_mission_due` (`next_attempt_at_ms`, `id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 战斗任务事实转发出箱(每玩家 FIFO + at-least-once + mission 侧收据幂等 + 失败退避)';
