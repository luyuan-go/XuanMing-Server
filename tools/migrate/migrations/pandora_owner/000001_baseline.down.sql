-- Pandora pandora_owner baseline down(⚠️ 仅 dev 回滚用;生产严禁执行)。
--
-- 破坏性:删掉 owner 权威三表 = 每玩家 owner_epoch / 实例租约 / 审计流水全部丢失。
-- epoch 是**单调不回退**的脑裂防线,重建后从 0 起算,在途的 DS 会拿着更大的旧 epoch
-- 与新记录对不上;§9.23 进场链以 owner 为第一权威,后果是全服进场判定失据。
-- 回滚顺序纪律:先停 owner 服务与两个 allocator 的 owner 接线(owner_addr 置空),再执行。

DROP TABLE IF EXISTS `owner_transition_log`;
DROP TABLE IF EXISTS `ds_instance_lease`;
DROP TABLE IF EXISTS `owner_record`;
