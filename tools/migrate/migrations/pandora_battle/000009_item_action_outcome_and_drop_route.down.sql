-- no-op:本迁移只增加权威结果/余额/路由列。fresh-init 可能在执行 up 前已经由
-- 05-battle-outbox.sql 建好这些结构；回滚若 DROP 会误删权威数据，并让新旧副本混跑时
-- 丢失 action outcome 或改变历史掉落路由。代码回滚保留新结构，由旧代码忽略。
SELECT 1;
