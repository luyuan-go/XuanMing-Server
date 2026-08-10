-- Pandora pandora_trade baseline (golang-migrate v000001)
-- 自动从 deploy/mysql-init/*.sql 生成(去 USE 行;DSN 已指定库)。
-- 这是该库的建表基线;后续结构改动新增 000002_*.up.sql,勿改本文件。

-- ===== from 08-inventory-tables.sql =====
-- Pandora 背包 / 经济库 W5 ③ 表结构(2026-06-18)
--
-- 装载方式:容器 entrypoint 自动扫 /docker-entrypoint-initdb.d/*.sql 顺序执行
-- (01-create-databases.sql 先建库 + grant,本文件接着建表)。
--
-- 表清单(对齐 docs/design/infra.md pandora_trade):
--   player_currency   玩家货币余额(player_id PK,金币)
--   player_items      背包道具堆叠(uk player_id+item_config_id)
--   inventory_ledger  发放 / 使用 / 出售幂等流水(uk player_id+idempotency_key,不变量 §9.7)
--
-- 约定:
--   - player_id 由 login 用 snowflake 生成(BIGINT UNSIGNED),inventory 不生成
--   - item_config_id 是配置表道具 ID(uint32,§12)
--   - 货币 / 道具数量用 BIGINT(可累积大额);非负由应用层事务保证
--   - GrantItems / UseItem / SellItem 幂等:inventory_ledger uk 命中即视为已处理(不变量 §9.7)
--   - 背包是大厅态持久化;战斗内即时道具走 UE GAS,不落本库(ds-arch §0.1)


CREATE TABLE IF NOT EXISTS `player_currency` (
    `player_id`  BIGINT UNSIGNED  NOT NULL,
    `gold`       BIGINT           NOT NULL DEFAULT 0 COMMENT '金币余额(>=0,应用层事务保证)',
    `updated_at` DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家货币余额';

CREATE TABLE IF NOT EXISTS `player_items` (
    `id`             BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`      BIGINT UNSIGNED  NOT NULL,
    `item_config_id` INT UNSIGNED     NOT NULL COMMENT '配置表道具 ID(uint32)',
    `count`          BIGINT           NOT NULL DEFAULT 0 COMMENT '持有数量(>=0;0 行可保留也可清理)',
    `updated_at`     DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_item` (`player_id`, `item_config_id`),
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家背包道具堆叠';

CREATE TABLE IF NOT EXISTS `inventory_ledger` (
    `id`                  BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`           BIGINT UNSIGNED  NOT NULL,
    `idempotency_key`     VARCHAR(64)      NOT NULL COMMENT '防重复入账/扣减(如 drop:<match_id> / use:<uuid>)',
    `op`                  VARCHAR(16)      NOT NULL COMMENT 'grant | use | sell | auction_sell | auction_buy | trade_sell | trade_buy',
    `request_fingerprint` CHAR(64)         NOT NULL DEFAULT '' COMMENT '请求指纹 sha256(op+item+count+gold);同 key 不同指纹判冲突',
    `result_remaining`    BIGINT           NOT NULL DEFAULT 0 COMMENT '首次执行后剩余数量快照(use/sell 用,回放返回)',
    `result_gold`         BIGINT           NOT NULL DEFAULT 0 COMMENT '首次执行后金币快照(grant/sell 用,回放返回)',
    `detail`              VARCHAR(255)     NOT NULL DEFAULT '' COMMENT '人读摘要(审计用,非业务字段)',
    `created_at`          DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_idem` (`player_id`, `idempotency_key`),
    KEY `idx_player_created` (`player_id`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 背包发放/使用/出售幂等流水(不变量 §9.7;指纹防 key 复用,快照可回放)';

CREATE TABLE IF NOT EXISTS `auction_escrow` (
    `id`             BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    `player_id`      BIGINT UNSIGNED  NOT NULL COMMENT '挂单玩家(冻结资产的所有者)',
    `order_id`       BIGINT UNSIGNED  NOT NULL COMMENT '挂单 order_id(escrow 键 + 冻结幂等键)',
    `kind`           TINYINT          NOT NULL COMMENT '1=item(卖单冻道具) 2=gold(买单冻金币)',
    `item_config_id` INT UNSIGNED     NOT NULL DEFAULT 0 COMMENT 'kind=1 时冻结的道具配置 ID',
    `frozen_qty`     BIGINT           NOT NULL DEFAULT 0 COMMENT 'kind=1 剩余冻结道具数(成交消费 / 退还递减)',
    `frozen_gold`    BIGINT           NOT NULL DEFAULT 0 COMMENT 'kind=2 剩余冻结金币(成交消费 / 退还递减)',
    `status`         TINYINT          NOT NULL DEFAULT 1 COMMENT '1=active 2=closed(退还/完结)',
    `created_at`     DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`     DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_player_order` (`player_id`, `order_id`),
    KEY `idx_player` (`player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 拍卖挂单托管(escrow:挂单冻结/成交消费/撤单过期退还,资产不在活跃余额可被双花)';

-- W5 ④ 装备实例背包(2026-07-08)
--
--   player_item_instance  装备类道具唯一实例(每件独立 + 鉴定后随机属性),不可堆叠。
--   与 player_items(可堆叠消耗品计数)并存:消耗品走计数,装备走实例(ds-arch §0.5 大厅态持久化)。
--   instance_id 由 inventory 服务 snowflake 生成(BIGINT UNSIGNED,§11)。
--   attributes 存鉴定后 roll 的随机属性(pb ItemInstanceAttributesStorageRecord 二进制);未鉴定为 NULL。
--   幂等发放沿用 inventory_ledger(op=grant_inst,记 instance_id 列表指纹);
--   鉴定天然幂等(identified=1 后不再 roll,回放已落定属性)。
--   slot_index 未分配格 = NULL(MySQL 唯一键允许多个 NULL,故多件未分配格不冲突);
--   分配到具体格后为 [0,capacity),(player_id,slot_index) 唯一,防两件叠同格。
CREATE TABLE IF NOT EXISTS `player_item_instance` (
    `instance_id`    BIGINT UNSIGNED  NOT NULL COMMENT '装备实例唯一 ID(snowflake,§11)',
    `player_id`      BIGINT UNSIGNED  NOT NULL COMMENT '持有玩家',
    `item_config_id` INT UNSIGNED     NOT NULL COMMENT '配置表道具 ID(uint32,§12)',
    `identified`     TINYINT          NOT NULL DEFAULT 0 COMMENT '0=未鉴定 1=已鉴定(鉴定后 attributes 落定)',
    `attributes`     VARBINARY(1024)  NULL COMMENT '鉴定后随机属性(pb ItemInstanceAttributesStorageRecord);未鉴定为 NULL',
    `slot_index`     INT              NULL     DEFAULT NULL COMMENT '背包格子索引([0,capacity);NULL=未分配格)',
    `bound`          TINYINT          NOT NULL DEFAULT 0 COMMENT '0=未绑定 1=绑定(绑定后不可交易/出售)',
    `created_at`     DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at`     DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`instance_id`),
    KEY `idx_player` (`player_id`),
    UNIQUE KEY `uk_player_slot` (`player_id`, `slot_index`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 玩家装备类道具唯一实例(实例化背包;每件独立 + 鉴定后随机属性)';

