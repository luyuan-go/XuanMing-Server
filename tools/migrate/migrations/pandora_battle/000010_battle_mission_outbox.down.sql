-- 000010 回滚 — 破坏性:删除任务事实转发出箱表。
--
-- 回滚顺序:先把 battle_result 的 mission_addr 置空(停止产生新行)并确认表已排空,
-- 再执行本迁移;未排空即回滚 = 已接受但未投递的任务进度事实永久丢失。

DROP TABLE IF EXISTS `battle_mission_outbox`;
