-- 000004_talent_spent_points 回滚:删已花点数列。
--
-- 回滚后读取侧退回按 Σ 等级 反推可点数。当前专精表全部 cost_per_level=1,账面一致;
-- 若届时已有 cost_per_level≠1 的节点被玩家点出,回滚会让这些玩家的可点数虚高(旧 bug 复现),
-- 需要在回滚后人工核对 total_talent_points。

ALTER TABLE `player_talents` DROP COLUMN `spent_points`;
