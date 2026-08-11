-- Pandora 开发环境数据库初始化
-- mysql:8.4 容器启动时自动加载本目录的 *.sql(/docker-entrypoint-initdb.d)
--
-- 数据库划分(对齐 docs/design/infra.md §2.1):
--   pandora_account  账号
--   pandora_player   玩家档案 / 段位 / 英雄池 / 皮肤
--   pandora_social   好友 / 黑名单 / 公会(后期)
--   pandora_battle   战斗结算历史 / 战绩
--   pandora_trade    交易订单 / 审计
--   pandora_auction  全服拍卖行挂单 / 成交(按 market_id 分片)
--   pandora_leaderboard 排行榜结算归档(结算批次 / Top-N 快照 / 发奖记录;进行中排名只在 Redis)
--   pandora_ops      运营日志 / 封禁 / 客诉
--
-- 字符集统一 utf8mb4_0900_ai_ci(MySQL 8.x 默认),禁用 utf8(3 字节)。

CREATE DATABASE IF NOT EXISTS `pandora_account`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS `pandora_player`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS `pandora_social`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS `pandora_battle`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS `pandora_trade`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS `pandora_auction`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS `pandora_leaderboard`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS `pandora_ops`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS `pandora_bag`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

-- dev 联调用;生产 owner 权威库必须在 TiDB(deploy/tidb-init/02-owner-tidb.sql,§9.22)。
CREATE DATABASE IF NOT EXISTS `pandora_owner`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

CREATE DATABASE IF NOT EXISTS `pandora_mission`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_0900_ai_ci;

-- 把 pandora 用户授权到所有 pandora_* 库
GRANT ALL PRIVILEGES ON `pandora_account`.* TO 'pandora'@'%';
GRANT ALL PRIVILEGES ON `pandora_player`.*  TO 'pandora'@'%';
GRANT ALL PRIVILEGES ON `pandora_social`.*  TO 'pandora'@'%';
GRANT ALL PRIVILEGES ON `pandora_battle`.*  TO 'pandora'@'%';
GRANT ALL PRIVILEGES ON `pandora_trade`.*   TO 'pandora'@'%';
GRANT ALL PRIVILEGES ON `pandora_auction`.* TO 'pandora'@'%';
GRANT ALL PRIVILEGES ON `pandora_leaderboard`.* TO 'pandora'@'%';
GRANT ALL PRIVILEGES ON `pandora_ops`.*     TO 'pandora'@'%';
GRANT ALL PRIVILEGES ON `pandora_bag`.*     TO 'pandora'@'%';
GRANT ALL PRIVILEGES ON `pandora_owner`.*   TO 'pandora'@'%';
GRANT ALL PRIVILEGES ON `pandora_mission`.* TO 'pandora'@'%';

FLUSH PRIVILEGES;
