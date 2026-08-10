-- 邮件 transfer 附件实例托管表(bag-domain.md §7.1 三不变量落地,2026-07-22)。
--
-- 纯 additive:旧 inventory 二进制不引用本表,先完成迁移再滚动发布带 transfer 托管链的
-- 版本即可,混跑期无兼容问题。fresh 库由 mysql-init/08-inventory-tables.sql 建同构表,
-- CREATE TABLE IF NOT EXISTS 幂等。
CREATE TABLE IF NOT EXISTS `mail_transfer_escrow` (
    `instance_id`      BIGINT UNSIGNED  NOT NULL COMMENT '托管中的实例唯一 ID(与 player_item_instance 互斥存在)',
    `item_config_id`   INT UNSIGNED     NOT NULL COMMENT '配置表道具 ID(uint32,§12;领取侧交叉核对)',
    `identified`       TINYINT          NOT NULL DEFAULT 0 COMMENT '扣出时鉴定态原样保留',
    `attributes`       VARBINARY(1024)  NULL COMMENT '扣出时词条原样保留(pb ItemInstanceAttributesStorageRecord);未鉴定为 NULL',
    `bound`            TINYINT          NOT NULL DEFAULT 0 COMMENT '扣出时绑定态原样保留(扣出侧已拒绑定实例,防御性冗余)',
    `source_player_id` BIGINT UNSIGNED  NOT NULL COMMENT '扣出来源玩家(释放归还目标;审计)',
    `to_player_id`     BIGINT UNSIGNED  NOT NULL COMMENT '预期领取人(领取校验,防其它邮件冒领)',
    `escrow_key`       VARCHAR(64)      NOT NULL COMMENT '托管操作键(审计;幂等由 inventory_ledger 承担)',
    `created_at`       DATETIME         NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`instance_id`),
    KEY `idx_to_player` (`to_player_id`),
    KEY `idx_source_player` (`source_player_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
  COMMENT='Pandora 邮件 transfer 附件实例托管(在途行,领取/释放即删;§9.24 豁免登记)';
