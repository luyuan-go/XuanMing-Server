-- 000001_baseline 回滚 — 破坏性:删除任务域全部表(玩家任务进度 / 完成集 /
-- 发奖流水 / 事实收据全部丢失,不可恢复;守卫行表无业务数据)。
--
-- 回滚顺序纪律:先回滚 / 停止 mission 服务与 battle_result 的任务事实转发
-- (mission_addr 置空),再执行本迁移;反序会让在途写入撞"表不存在"。
-- 本迁移只 DROP 本版本新增的表,不触碰其他库任何对象。

DROP TABLE IF EXISTS `mission_player_guards`;
DROP TABLE IF EXISTS `mission_push_outbox`;
DROP TABLE IF EXISTS `mission_fact_receipts`;
DROP TABLE IF EXISTS `mission_reward_log`;
DROP TABLE IF EXISTS `player_mission_done`;
DROP TABLE IF EXISTS `player_mission_active`;
