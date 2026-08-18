-- Pandora pandora_bag baseline down(⚠️ 仅 dev 回滚用;生产严禁执行)。
--
-- 破坏性:删掉背包域全部表 = 玩家仓库/活动段本体、随身组快照、流水与 fencing 锚点
-- (bag_meta.owner_epoch)全部丢失,不可恢复;bag_migration 是旧 inventory 存量迁移的
-- 永久幂等闸,删掉后重跑迁移会二次入账。
-- 回滚顺序纪律:先停 inventory 服务的背包域写路径(门控关闭),再执行本迁移。

DROP TABLE IF EXISTS `bag_generation`;
DROP TABLE IF EXISTS `bag_capacity`;
DROP TABLE IF EXISTS `bag_migration`;
DROP TABLE IF EXISTS `bag_journal`;
DROP TABLE IF EXISTS `bag_section`;
DROP TABLE IF EXISTS `bag_checkpoint`;
DROP TABLE IF EXISTS `bag_meta`;
