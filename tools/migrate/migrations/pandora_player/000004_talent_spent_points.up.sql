-- 000004_talent_spent_points — 天赋已花点数落列(修「写按每级消耗扣、读按等级和算」的口径分裂)。
--
-- 背景:SetTalents 的扣点是 Σ 等级 × cost_per_level(专精表列,biz 算好传入),
-- 但可点数的读取侧(talentUnspent / GetTalents)按 Σ 等级 反推。两个口径只有在
-- 全表 cost_per_level=1 时才碰巧一致 —— 当前专精表恰好全是 1,所以问题被掩盖着。
-- 策划把任一节点的每级消耗调成 2,玩家点满后刷新界面就会凭空多出可点数(写扣 6 读算 4),
-- 且能反复利用。repo 层看不到配置表,无法自行换算,因此把每节点实际消耗随分配一起落库。
--
-- 纯 additive(不变量 §16/§17 不停服):只加一列,滚动更新期间老副本的 SQL 不引用它。
-- ALGORITHM=INSTANT 显式声明在线 DDL(MySQL 8.0 加带默认值列不锁表不重建;
-- 目标实例不支持时明确失败,而不是静默退化成锁表拷贝)。
--
-- 条件加列:deploy/mysql-init/04-player-tables.sql 的 fresh-init 已直接建出本列,
-- 无条件 ADD COLUMN 会 duplicate column 报错(同 000002 的 players.exp 处理)。

SET @ddl := IF(
    (SELECT COUNT(*) FROM information_schema.COLUMNS
     WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'player_talents' AND COLUMN_NAME = 'spent_points') = 0,
    'ALTER TABLE `player_talents` ADD COLUMN `spent_points` INT NOT NULL DEFAULT 0 COMMENT ''该节点实际消耗天赋点(= 等级 × 专精表每级消耗;0 = 升级前老行,读取侧回退按 level 计)'' AFTER `level`, ALGORITHM=INSTANT',
    'SELECT 1');
PREPARE add_spent_points FROM @ddl;
EXECUTE add_spent_points;
DEALLOCATE PREPARE add_spent_points;

-- 存量行回填:升级前所有节点的 cost_per_level 都是 1,已花点数恒等于 level,
-- 回填后账面与升级前完全一致(不给任何玩家凭空退点,也不多扣)。
-- 真实消耗恒 >0(cost_per_level>=1 且 level>=1),所以 spent_points=0 只可能是未回填行,
-- 条件更新天然幂等,重复执行不会改动已回填的数据。
UPDATE `player_talents` SET `spent_points` = `level` WHERE `spent_points` = 0;
