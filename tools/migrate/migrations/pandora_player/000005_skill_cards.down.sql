-- 000005_skill_cards 回滚:删技能卡三张表。
--
-- ⚠️ 破坏性:玩家的持卡、等级、碎片、装配全部丢失,且**不可恢复**
-- (碎片只存在于本库,没有第二份台账可以对账重建)。
-- 回滚前须确认要么还没上线发过卡,要么已单独备份这三张表。
--
-- 回滚风险三条(执行前逐条确认):
--   1. 本 down **不是** additive-only no-op(与 000003 不同,与 000002 同类):它真删表。
--      判据很简单 —— 000003 补的是既有表的索引,回滚删掉会让库比权威定义少东西;
--      000005 建的是本次全新的表,回滚就该把它们收走。代价是数据一起没。
--   2. `deploy/mysql-init/04-player-tables.sql`(fresh-init 权威定义)也建这三张表。
--      在 fresh-init 过的库上回滚,库会比 fresh-init 权威定义**少三张表**,
--      随后 dbcheck 会报「登记表缺失」。重新 up 即可恢复结构(数据不回来)。
--   3. 不停服纪律(§9.21 expand→migrate→contract):player 新副本会读写这三张表。
--      正确的回滚顺序是**先把服务回滚到不引用技能卡的版本、排空在途请求,再回滚本迁移**;
--      反过来做会让在线副本对着不存在的表报错。只回滚服务不回滚迁移是安全的
--      (表留着不用,纯 additive 的好处),生产优先走这条。
--
-- 本 down 只碰本次新增的三张表,不 ALTER / 不删列 / 不动 players 等既有表
-- (player_migration_test.go 的 TestPandoraPlayerV5DownTouchesOnlyNewTables 机械保证)。

DROP TABLE IF EXISTS `skill_card_grants`;
DROP TABLE IF EXISTS `player_skill_slots`;
DROP TABLE IF EXISTS `player_skill_cards`;
