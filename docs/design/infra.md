# Pandora 基础设施规范

> **此文档是写代码前的强制阅读**。所有 MySQL 表 / Redis key / Kafka topic / etcd 路径都按此规范命名,**不允许 ad-hoc**。

## 1. 命名总则

- **资源命名空间统一用 `pandora`(全小写)**,跟仓库名 `Pandora` 区分
- **多段命名按存储引擎习惯**:
  - Redis key:`:` 分隔
  - Kafka topic:`.` 分隔
  - MySQL 表:`_` 分隔(snake_case)
  - etcd path:`/` 分隔
- **小写 + 下划线**,不用驼峰

## 2. MySQL Schema

### 2.1 数据库划分

```
pandora_account        # 账号(login)
pandora_player         # 玩家档案 / 段位 / 英雄池 / 皮肤
pandora_social         # 好友 / 黑名单 / 公会(后期)
pandora_battle         # 战斗结算历史 / 战绩
pandora_trade          # 交易订单 / 审计
pandora_auction        # 全服拍卖行挂单 / 成交(按 market_id 分片)
pandora_leaderboard    # 排行榜结算批次 / Top-N 快照 / 发奖凭证(实时排名在 Redis,不落库)
pandora_ops            # 运营日志 / 封禁 / 客诉
```

⚠️ **不要把所有表塞 `pandora` 一个库**,按职能分库,后期容易拆服。

### 2.2 通用字段约定

每张业务表必须有:

```sql
id           BIGINT       PRIMARY KEY  AUTO_INCREMENT  -- 自增主键
created_at   DATETIME(3)  NOT NULL  DEFAULT CURRENT_TIMESTAMP(3)
updated_at   DATETIME(3)  NOT NULL  DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
deleted_at   DATETIME(3)  NULL                                   -- 软删
version      INT          NOT NULL  DEFAULT 0                    -- 乐观锁
```

**禁止**:`is_delete` / `del_flag` / `state=999` 之类的软删变体。统一 `deleted_at`。

### 2.3 关键表清单

#### `pandora_account`
| 表 | 用途 | 关键索引 |
|---|---|---|
| `accounts` | 账号 | uniq(account), uniq(email), idx(device_id) |
| `account_devices` | 设备绑定 | idx(account_id), uniq(device_id) |
| `account_bans` | 封禁记录 | idx(account_id, ban_until) |

#### `pandora_player`
| 表 | 用途 | 关键索引 |
|---|---|---|
| `players` | 玩家档案 | uniq(account_id), idx(nickname), idx(mmr) |
| `player_heroes` | 英雄解锁 | uniq(player_id, hero_id) |
| `player_skins` | 皮肤 | uniq(player_id, skin_id) |
| `player_currencies` | 金币 / 钻石 / 各种货币 | uniq(player_id, currency_type) |
| `player_inventory` | 道具背包 | idx(player_id), uniq(player_id, item_uid) |

#### `pandora_battle`
| 表 | 用途 | 关键索引 |
|---|---|---|
| `battles` | 一局对局元数据 | uniq(match_id), idx(ended_at) |
| `battle_player_stats` | 每个玩家的战绩 | idx(player_id, ended_at), idx(match_id) |
| `mmr_history` | MMR 变化历史 | idx(player_id, created_at) |

#### `pandora_trade`
| 表 | 用途 | 关键索引 |
|---|---|---|
| `trade_orders` | 交易订单 | uniq(order_id), idx(seller_id), idx(buyer_id) |
| `trade_audit` | 审计日志(append-only) | idx(order_id), idx(created_at) |
| `player_currency` | 玩家货币余额(inventory) | PK(player_id) |
| `player_items` | 玩家道具持有(inventory) | uk(player_id, item_config_id) |
| `inventory_ledger` | 资产变动流水 / 幂等键(inventory) | uk(player_id, idempotency_key) |
| `auction_escrow` | 拍卖挂单冻结(escrow:卖冻道具 / 买冻金币) | uk(player_id, order_id), idx(player_id) |

#### `pandora_auction`
按 `market_id` 分片(mysqlx ShardSet,shard = market_id % N;W1 单库，当前只批准 N≤2)。N 与有序
物理 DSN 身份由各 shard 的 topology marker 锁定，不允许直接 rehash。MySQL 是候选、订单状态与补偿意图权威；
`ReserveMatch` 在同一分片事务内锁双方订单并原子写成交意图，不依赖 Redis 锁承担防超卖正确性。

| 表 | 用途 | 关键索引 |
|---|---|---|
| `auction_orders` | 挂单 / 出价 + escrow 验证、撮合续跑、释放意图 | PK(order_id), uk(owner_id,idempotency_key), verified SELL/BUY 价格顺序索引, pending/match/release ready 索引, idx(owner_id,order_id) |
| `auction_matches` | 成交流水 + 待结算/成交事件 outbox | PK(match_id), settlement/event ready 索引, sell/buy pending 引用索引 |
| `auction_owner_guards` | 同 owner 全局幂等串行 guard | PK(owner_id) |
| `auction_idempotency_keys` | owner+key 跨 market canonical 映射 | PK(owner_id,idempotency_key), uk(order_id) |
| `auction_shard_topology` | id%N 有序物理拓扑 exact-match marker | PK(singleton_id), generation/count/index/identity hash |

#### `pandora_leaderboard`
排行榜实时排名权威在 Redis ZSET(board_store.go),MySQL 只兜结算结果 + 发奖凭证(结算是低频写,单库即可,无分库)。

| 表 | 用途 | 关键索引 |
|---|---|---|
| `leaderboard_settlement` | 结算批次头(幂等防重复结算) | PK(settlement_id), uk(settle_idempotency_key), idx(board_type, scope, scope_id) |
| `leaderboard_snapshot` | 结算 Top-N 名次快照(归档 / 对账) | PK(settlement_id, rank), idx(settlement_id, entity_id) |
| `leaderboard_reward_log` | 逐名次发奖记录(幂等防重复发奖) | PK(id), uk(grant_idempotency_key), idx(settlement_id) |

### 2.4 字符集 / 引擎

```sql
ENGINE=InnoDB
DEFAULT CHARSET=utf8mb4
COLLATE=utf8mb4_0900_ai_ci      -- MySQL 8.x 默认
```

⚠️ **不许用 utf8**(实际 3 字节),emoji 和复杂字符存不进。

## 3. Redis Key Schema

### 3.1 命名格式

```
pandora:<domain>:<entity>:<id>[:<field>]
```

**强制规则**:
- 全小写
- `:` 分隔
- 单段不超过 32 字符,总长不超过 128 字符
- **不准用动词**(`pandora:get_player:123` ❌,`pandora:player:123` ✅)

### 3.2 Key 清单(W1 规划)

#### Session / Token
| Key | 类型 | TTL | 用途 |
|---|---|---|---|
| `pandora:sess:<player_id>` | hash | 24h | 玩家 session |
| `pandora:ticket:<jti>` | string | 5min | DS 票据(防重放) |
| `pandora:locator:<player_id>` | hash | 30s heartbeat | 玩家位置 |
| `pandora:push:offline:<player_id>` | zset | 7d(帧 5min 窗口修剪) | push 投递缓冲(游标定序权威,含 wm/fl 哨兵) |
| `pandora:push:wake` | pub/sub channel | — | push 跨 Pod 投递唤醒信号(best-effort 加速器,R5 P2-10;丢失由 30s 兜底轮询收敛) |

#### Team
| Key | 类型 | TTL | 用途 |
|---|---|---|---|
| `pandora:team:{<team_id>}` | string(pb) | `active_ttl` 60m(`TouchTeam` 续期)/ 解散后 `disbanded_retention` 5m | 队伍主体 TeamStorageRecord(hashtag 锁 cluster slot) |
| `pandora:team:player:<player_id>` | string | 跟随队伍(同 `active_ttl`,`TouchTeam` 一并续) | 玩家所在队伍(SETNX claim,落"一人只在一个队") |
| `pandora:team:invite:<invite_id>` | hash | `invite_ttl` 60s | 邀请令牌(权威):team_id / target_player_id / inviter_id / expires_at_ms |
| `pandora:team:invite:target:<player_id>` | zset | `invite_ttl` 60s(每次写入刷新) | 被邀请人 pending 邀请索引(member=invite_id,score=expires_at_ms):写入侧限流 + 拉取兜底查询 |

⚠️ hashtag `{<team_id>}` 把队伍主体 key 锁到按 team_id 计算的 Redis Cluster slot(兜底,不可去掉);其余三个 key(`player` / `invite` / `invite:target`)不带 hashtag,按各自 id 分片,与主体不同 slot。

#### Match
| Key | 类型 | TTL | 用途 |
|---|---|---|---|
| `pandora:match:<game_mode>:queue` | sorted set | - | 撮合池(score=avg_mmr,member=ticket_id;按 game_mode 隔离,防同 Cell 多模式串池) |
| `pandora:match:<game_mode>:active` | sorted set | - | 确认期超时扫描(score=confirm_deadline_ms,member=match_id) |
| `pandora:match:ticket:<ticket_id>` | string(pb) | 30min | 排队票据 MatchTicketStorageRecord(全局唯一 ID,不分模式) |
| `pandora:match:{<match_id>}` | string(pb) | 30min | MatchStorageRecord(hashtag 锁 cluster slot) |
| `pandora:match:player:<player_id>` | string | 30min | 玩家所在 ticket_id(SETNX;**故意全局不分模式**,落"一人同一时刻只在一个队列") |

#### DS Allocator
| Key | 类型 | TTL | 用途 |
|---|---|---|---|
| `pandora:ds:battle:<pod_name>` | hash | 30s heartbeat | 战斗 DS 实例状态 |
| `pandora:ds:hub:<pod_name>` | hash | 30s heartbeat | 大厅 DS 实例状态 |
| `pandora:ds:battle:idle` | set | - | 空闲战斗 DS 池 |

#### Auction(旧版本兼容订单簿缓存)
| Key | 类型 | TTL | 用途 |
|---|---|---|---|
| `pandora:auction:book:{<market_id>}:ask` | zset | - | 卖盘(score=price,member=零padded order_id) |
| `pandora:auction:book:{<market_id>}:bid` | zset | - | 买盘(score=-price,价格-时间优先) |

⚠️ hashtag `{<market_id>}` 保持旧 key/member/score 与 Redis Cluster slot 语义。自 2026-07-12 起，
新版本不从该 ZSET 选撮合候选；它只是蓝绿切换期间供旧版本观察的 best-effort 兼容缓存，
`ZADD/ZREM` 失败不改变 MySQL 权威业务结果。跨实例 market Redis 锁只做正常串行与降冲突，
失锁窗口的最终防超卖由 MySQL 行锁、条件状态迁移和唯一成交意图负责。

#### Leaderboard(通用排行榜)
| Key | 类型 | TTL | 用途 |
|---|---|---|---|
| `pandora:lb:{<board>}:z` | zset | 临时榜带 TTL | 排名(member=entity_id,score=打包分,支持时间 tie-break) |
| `pandora:lb:{<board>}:t` | hash | 同 z | entity_id → updated_at_ms(展示 / 审计) |
| `pandora:lb:{<board>}:m` | hash | 同 z | 榜元信息(asc / tie,供读查询判排序方向) |

`<board>` = `<board_type>:<scope>:<scope_id>:<period>`(period 空用 `-`)。hashtag `{<board>}` 把同一榜的 z/t/m 锁到同一 Cluster slot,SubmitScore 的 Lua 原子碰三 key 不触发 CROSSSLOT。临时榜(副本局内 / 活动)靠 TTL 自动回收,实时排名权威在 Redis,MySQL 只兜结算。

#### Lock / Cache
| Key | 类型 | TTL | 用途 |
|---|---|---|---|
| `pandora:lock:<resource>` | string(NX EX) | ≤30s | 分布式锁 |
| `pandora:cache:player:<player_id>` | hash | 5min | 玩家档案缓存 |
| `pandora:cache:hero:list` | string(json) | 1h | 英雄列表配置缓存 |

⚠️ **lock TTL 严禁超过 30s**,业务跑完必须主动释放。

### 3.3 反模式禁令

- ❌ 不许用 `KEYS *` 遍历(用 `SCAN`)
- ❌ 不许把大对象塞 string(>1MB),用 hash 拆分
- ❌ 不许无 TTL 长期存(除了 sorted set 队列)
- ❌ 不许直接 `DEL` 大 key(用 `UNLINK`)

## 4. Kafka Topic Schema

### 4.1 命名格式

```
pandora.<domain>.<event>
pandora.dlq.<original_topic>     # 死信队列
```

### 4.2 Topic 清单

| Topic | 分区 | 保留 | 生产者 | 消费者 | 备注 |
|---|---|---|---|---|---|
| `pandora.login.event` | 8 | 7d | login | 风控、审计 | 登录登出 |
| `pandora.match.found` | 4 | 3d | matchmaker | ds_allocator | 匹配成功 |
| `pandora.match.failed` | 4 | 3d | matchmaker | (告警) | 匹配失败/超时 |
| `pandora.match.progress` ⭐ | 8 | 1h | matchmaker | **push** | 匹配进度推送(key=player_id)|
| `pandora.team.update` ⭐ | 8 | 1h | team | **push** | 队伍状态变更推送(key=player_id)|
| `pandora.chat.world` ⭐ | 16 | 1d | chat | **push** | 世界聊天推送(**广播类**,key 为空,push 走 `Broadcast` 而非按 key 路由) |
| `pandora.chat.team` ⭐ | 8 | 1h | chat | **push** | 队伍聊天推送(key=player_id)|
| `pandora.chat.private` ⭐ | 8 | 1d | chat | **push** | 私聊推送(key=target_player_id)|
| `pandora.chat.guild` ⭐ | 8 | 1h | chat | **push** | 公会聊天推送(key=接收方 player_id,逐成员扇出,不落库)|
| `pandora.chat.group` ⭐ | 8 | 1h | chat | **push** | 临时群聊推送(key=接收方 player_id,逐成员扇出,不落库)|
| `pandora.guild.event` ⭐ | 4 | 1d | guild | **push** | 公会成员变更通知(入会 / 退会 / 踢人 / 解散,key=接收方 player_id)|
| `pandora.player.update` | 8 | 7d | battle_result | player(MMR 入账) | **服务间事件,push 不订阅**;单事件类型 topic(混跑安全,见 `kafkax.TopicPlayerUpdate`)。player 域面向客户端的推送走 `pandora.player.experience` |
| `pandora.friend.event` ⭐ | 4 | 1d | friend | **push** | 好友请求 / 上线提醒 |
| `pandora.system.notify` | 4 | 7d | (无) | (无) | **规划中,尚未接线**:无 proto 定义、无 producer、不在 `kafkax.PushTopics`;已登记 `kafkax.BroadcastTopics`,上线时按广播类接入 |
| `pandora.hub.migrate` ⭐ | 4 | 1h | hub_allocator | **push** | 大厅强制整合迁移通知(key=player_id) |
| `pandora.presence.update` ⭐ | 4 | 1h | player_locator | **push** | 好友在线态订阅推送(key=subscriber_id,去抖合并后批量下发) |
| `pandora.player.experience` ⭐ | 4 | 1h | player | **push** | 实时经验 / 升级推送(key=player_id,event_type=1) |
| `pandora.ds.lifecycle` | 4 | 7d | ds_allocator / hub_allocator | 监控 | DS 拉起/回收/崩溃 |
| `pandora.battle.result` | 16 | 30d | Battle DS | battle_result | ⭐ 核心,at-least-once + 幂等落库 |
| `pandora.trade.audit` | 4 | 90d | trade | 审计、风控 | 交易日志(append-only) |
| `pandora.auction.match` | 4 | 90d | auction | 风控、对账 | 拍卖成交事件(key=match_id) |
| `pandora.auction.audit` | 4 | 90d | auction | 审计、风控 | 拍卖挂单流转(key=order_id,append-only) |
| `pandora.leaderboard.settle` | 4 | 90d | leaderboard | 工会 / 活动服务、对账 | 排行榜结算事件(key=settlement_id,含 Top-N;尤其 GUILD 榜由工会服务消费分发) |
| `pandora.locator.update` | 8 | 1h | hub DS / battle DS | player_locator | 玩家位置变更 |

⭐ = 推送 topic(2026-06-03 起陆续新增),推送架构见 `gateway-decision.md` §6。所有标 ⭐ 的 topic 都被 **pandora-push** 服务消费,经 Envoy 以 **gRPC-Web server stream** 推给客户端(push **不是** WebSocket 服务)。

⚠️ **push 订阅集的权威源是 `pkg/kafkax.PushTopics`**(当前 12 个),本表只是登记视图;新增推送 topic 必须同时改 `pkg/kafkax/topics.go` 与本表。`services/runtime/push/etc/push-prod.yaml.example` 刻意不列 `push.topics` 走默认全量集,避免显式列表漏项。

### 4.3 分区键约定

- **玩家相关 topic**:`key = player_id`(同一玩家事件有序)
- **战斗结算**:`key = match_id`(同一局事件有序,且能幂等去重)
- **DS lifecycle**:`key = pod_name`

### 4.4 死信策略

每个核心 topic 配套 `pandora.dlq.<topic>`,保留 30 天。消费失败 3 次进 DLQ,人工介入。

⚠️ **`pandora.battle.result` 必须有 DLQ**,丢战绩等于丢钱。

## 5. etcd Path Schema

### 5.1 路径格式

```
/pandora/<env>/<category>/<entity>
```

`<env>` = `dev` / `staging` / `prod`,**禁止跨环境共用 etcd cluster**。

### 5.2 路径清单

#### 服务发现
```
/pandora/dev/services/login/<instance_id>          → endpoint json
/pandora/dev/services/matchmaker/<instance_id>
/pandora/dev/services/ds_allocator/<instance_id>
...
```

#### 配置中心
```
/pandora/dev/config/login                          → toml/json 配置
/pandora/dev/config/matchmaker
/pandora/dev/config/global                         → 全局通用(MMR 公式参数等)
```

#### Leader Election
```
/pandora/dev/leader/ds_allocator
/pandora/dev/leader/hub_allocator
/pandora/leader/matchmaker/<game_mode>/r<region>   # 撮合循环单写者(pkg/leader/etcdleader)
```

matchmaker 撮合循环多副本部署时经 etcd 选举保证单写者(防重复成局,见
`decision-revisit-matchmaker-single-writer.md`);选举 key = `<prefix>matchmaker/<game_mode>/r<region>`,
prefix 默认 `/pandora/leader/`,可经 `match.leader.prefix` 按环境配成 `/pandora/<env>/leader/`。

### 5.3 TTL / lease

- 服务注册:lease 10s,5s 续约一次
- 配置:无 TTL,变更触发 watch
- Leader:lease 15s

## 6. 端口分配(开发环境)

### 6.1 基础设施(docker-compose)

| 服务 | 端口 | 备注 |
|---|---|---|
| MySQL | 3307 | 开发环境端口 |
| Redis | 6380 | 开发环境端口 |
| Kafka | 9093 | 开发环境端口 |
| Zookeeper | 2182 | |
| etcd client | 2380 | 开发环境端口 |
| etcd peer | 2381 | |
| Prometheus | 9091 | 开发环境端口 |
| Grafana | 3001 | 开发环境端口 |
| Jaeger UI | 16687 | 开发环境端口 |

### 6.2 Go 服务 gRPC 端口

| 服务 | gRPC 端口 | metrics 端口(+1000) |
|---|---|---|
| login | 50001 | 51001 |
| player | 50002 | 51002 |
| data_service | 50003 | 51003 |
| friend | 50004 | 51004 |
| chat | 50005 | 51005 |
| player_locator | 50006 | 51006 |
| leaderboard | 50007 | 51007 |
| guild | 50008 | 51008 |
| mail | 50009 | 51009 |
| team | 50010 | 51010 |
| matchmaker | 50011 | 51011 |
| trade | 50012 | 51012 |
| dialogue | 50013 | 51013 |
| **push** ⭐ | **50014**(gRPC server stream)| **51014** |
| inventory | 50015 | 51015 |
| auction | 50016 | 51016 |
| owner | 50017 | 51017 |
| ds_allocator | 50020 | 51020 |
| hub_allocator | 50021 | 51021 |
| battle_result | 50022 | 51022 |

⭐ = 2026-06-04 终版新增。push 服务用 Kratos transport/grpc 暴露 server stream,客户端经 Envoy 连过来(gRPC-Web → gRPC 转换)。

**所有 go 服务全部用 gRPC 端口**(50001-50022 段),协议统一。inventory(W5 ③ 新增,economy 域,50015/51015)落在 push(50014)与 battle 块(50020+)之间的空档。auction(2026-06-19 新增,全服拍卖行 / 撮合,economy 域,50016/51016)紧随 inventory。leaderboard(2026-06-27 新增,通用排行榜,runtime 域,50007/51007)落在 player_locator(50006)与 team 块(50010)之间的空档。guild(2026-06-27 新增,公会 + 临时群同进程,social 域,50008/51008)落在 leaderboard(50007)与 team 块(50010)之间的空档。mail(2026-06-29 新增,邮件系统,social 域,50009/51009)紧随 guild。

### 6.3 Edge Gateway(Envoy)

| 服务 | 端口 | 用途 |
|---|---|---|
| Envoy(HTTPS)| **8443** | 客户端入口,gRPC-Web over HTTP/2 TLS |
| Envoy admin | **9901** | 配置 / metrics / 健康检查 |

Envoy 是基础设施组件,**不是 go 服务**。它做:
- TLS 终止(客户端 HTTPS → 内网明文 gRPC)
- gRPC-Web ↔ gRPC 协议转换(envoy `grpc_web` filter)
- JWT 鉴权(envoy `jwt_authn` filter)
- 限流 / 熔断 / 重试

详见 `gateway-decision.md` §5。

### 6.4 UE DS 端口

- **目标规划**：Hub DS 使用 7000-7500，Battle DS 使用 7501-8000。
- **当前实现**：四条 Fleet 仅声明 `portPolicy: Dynamic`，仓库内尚未按角色落实上述分段；实际 hostPort
  由集群 Agones controller 的全局端口池分配。安全组 / 防火墙 / NAT 不得根据本节目标规划臆测端口已经分段。
- online 玩家直接连接 allocator 返回的 `status.address:status.ports[0].port`，不经过本地 UDP relay。
  上线必须按 [`docs/ops/release-checklist.md`](../ops/release-checklist.md) §2.3 回读实际 address/port/Node/UID，
  并从集群外完成 exact GameServer UDP 握手；仅 Fleet Ready 或集群内可达不算验收。

## 7. 时间约定

- **所有时间戳用 Unix milliseconds**(int64)
- **DB 字段类型 `DATETIME(3)`**(毫秒精度)
- **proto 字段命名 `xxx_at_ms`**(明确单位)
- **永远存 UTC**,展示时再转时区

⚠️ 禁止 `DATETIME` 不带精度(默认秒级,丢数据)。

## 8. ID 生成

- **player_id / team_id / match_id**:snowflake(`pkg/snowflake`)
- **trade_order_id**:snowflake + 业务前缀(`T` + 18 位)
- **数据库自增 id**:仅做物理主键,**不对外暴露**
- **session_token / jti**:UUID v4

⚠️ 禁止用自增 id 当业务标识对外。

### 8.1 Snowflake nodeID 分配决策

**当前阶段不引入中心化发号器,继续使用本地 snowflake + 静态 `node.node_id`。**

原因:
- `pkg/snowflake` 的 ID 生成是本地 CAS 纯内存路径,没有系统调用和网络往返;每个节点吞吐上限由位域设计约束,不是 Redis/数据库吞吐约束。
- `Redis INCR` 每次取号都要打网络,延迟比本地 snowflake 高 4~5 个数量级,且单 Redis 变成全服共享吞吐上限和可用性单点。
- `Redis INCR` 还有正确性硬伤:RDB/AOF 持久化窗口、主从复制滞后或故障切换都可能导致计数回退,重启后发出历史重复 ID;要堵住必须牺牲性能或人工跳号。
- 号段模式可以缓解吞吐,但仍依赖中心存储,ID 不含时间信息,对 Pandora 当前 snowflake 方案没有额外收益。

**Redis 不用于发业务 ID,也不作为 snowflake nodeID 租约服务。**

未来如果进入 k8s 多副本动态扩缩阶段,同一服务会跑 N 个 pod,静态 `node_id` 人工规划不再适合,再补一个 etcd Lease 版 nodeID 自动分配:

> **2026-06-19 落地,2026-07-01 接入最终版 helper**:该方案已实现为独立 module [`pkg/snowflake/etcdnode`](../../pkg/snowflake/etcdnode/etcdnode.go)(`etcdnode.Acquire` → `*Holder`,`Lost()` 失租信号;`etcdnode.MustProvideSnowflake` 统一 static / etcd 两态接线)。单副本 / dev 仍走静态 `node.node_id`;`SnowflakeConf.node_id_source="etcd"` 时切换。容量背景见 [`scale-cellular-20m.md`](./scale-cellular-20m.md)。

```
启动 -> etcd Grant lease(TTL 15s)
     -> 事务抢占 /pandora/snowflake/node/<id> 并绑定 lease
     -> 后台 KeepAlive 续租
     -> KeepAlive channel 关闭 = 租约丢失
     -> 进程主动退出,避免两个活进程共用同一 nodeID
```

注意:用了 etcd 之后仍然需要一个后台 `KeepAlive` / session monitor,但这不是 Redis 方案里自己拼的"看门狗"。区别是:
- etcd Lease 是 nodeID 独占权的事实来源;
- monitor 只负责持续接收 etcd 的 KeepAlive 确认;
- 一旦 KeepAlive channel 关闭、续租失败、lease 被 revoke 或 session done,进程必须先停止发号再主动退出;
- 不能把失租当普通告警处理,也不能在本地继续 `Generate`,否则会和新 holder 形成同 nodeID 双活。

落点:
- 新增 `snowflake.NewNodeFromEtcd(...)` 一类工厂;
- `snowflake.Node` 本体和 `Generate` CAS 热路径不改;
- 静态配置仍保留为本地/dev/单副本默认路径;
- etcd `KeepAlive` 不是普通健康检查,而是 nodeID 独占权的 fencing 信号;KeepAlive channel 关闭、续租失败或确认 lease 丢失时,进程必须立即停止发号并主动退出,不能只打日志继续运行。
- 不用 Redis `SETNX + TTL + 看门狗` 拼租约:Redis 看门狗只能努力续租,不能证明旧 holder 已停止发号;GC 停顿、网络分区、进程卡死但业务线程仍跑等场景下,租约可能过期并被新进程领走,旧进程恢复后形成同 nodeID 双活。

### 8.2 唯一性的作用域:三条必须记住的规则(2026-07-28)

snowflake 的唯一性**只在「同一个 `*snowflake.Node` 对象内」成立**,不在服务内、不在全局。
下面三条是从这一条推出的,违反任意一条都会静默发重号(已有单测钉死契约,见
`pkg/snowflake/etcdnode/provider_test.go`)。

**① 同一进程内多个 ID 空间 → `ProvideSnowflakeN`,共用 nodeID。**

一个服务里互不相干的 ID(team 的 `team_id` / `invite_id`,auction 的 `order_id` / `match_id`)
各持一个 `*snowflake.Node`,由 [`etcdnode.MustProvideSnowflakeN`](../../pkg/snowflake/etcdnode/provider.go)
一次取 n 个。它们**共用同一个 nodeID**:一次 `Acquire`、一个 lease、一处失租退出,
失败面不随 n 增长;每个 Node 有独立 step 池,合计上限 n × 32768/s(实测线性叠加)。

代价是**跨空间会发出逐位相同的 ID**——同一秒里各自的第 K 个号必然相同,这是常态不是小概率。
所以每个空间的值必须待在各自独立的表 / key 前缀 / 唯一键里,**禁止**把两个空间的 ID
放进同一张表、同一个 map 或同一个唯一键。

**② 跨服务共享的 ID 空间:etcd 模式救不了,必须显式共用 `etcd_service_name`。**

⚠️ 这条最反直觉。etcd 的 nodeID key 是 `/pandora/snowflake/node/<service>/<id>`,
**按服务名隔离、跨服务刻意复用**——两个不同服务各自都会分到 nodeID 0。
所以只要两个服务铸**同一种** ID,把 `node_id_source` 改成 `etcd` 非但不解决问题,
还会让它们稳定拿到相同的 nodeID。

现存实例:`instance_id` 由 inventory(`GrantInstances`)和 mail(`buildClaimIntent`)
两处铸造,两批实例可能汇进同一玩家的同一个背包段,被 `data/bag_apply.go` 的
`duplicate instance` 检查 fail-closed 拒掉(玩家领不了那封邮件)。
当前修法是 static 下给两服务分配不同 `node_id`(inventory=1,mail=2,配置里有注释);
若将来切 etcd,两者必须显式共用同一个 `etcd_service_name`。

新增「两个服务铸同一种 ID」的设计前,先问一句:能不能只让**一个**服务铸(§9.22 唯一权威)。

**③ 多副本 ⇒ 必须 etcd。金丝雀发布等价于多副本。**

static 模式下所有副本共用配置里的同一个 `node_id`,两个副本发出的号逐位相同。
集群产物由 `tools/scripts/gen_cluster_config.ps1` 的 `$MultiReplicaSnowflakeServices`
清单机械注入 etcd 模式(当前:`auction`、`login`)。判据是「可能同时跑多于一个副本
**且**会发 snowflake ID」——`player-locator` 有 2 副本但不发号,故不在列。

CLAUDE.md §9.21 的金丝雀发布要求 stable / canary 并存,那就是同服务 2 副本。
**任何服务纳入灰度发布前,必须先加进该清单**,否则灰度窗口内必然双活发重号。

### 8.3 批量发号 `GenerateInto`(2026-07-28)

按件数循环调用 `Generate` 的地方(inventory `GrantInstances`、mail 附件展开)改用
[`(*Node).GenerateInto(dst []uint64)`](../../pkg/snowflake/snowflake.go):一次 CAS 预留整段,
实测 9.2 ns/ID → 0.2 ns/ID。语义与逐个 `Generate` 完全一致(严格递增、唯一、可混用),
**但不保证相邻连续**——跨秒边界时间段跳变会有空洞,调用方只能依赖「递增 + 唯一」。

参数是调用方给的 `dst` 而非 `GenerateN(count)`:批量数量常来自外部输入,
让 API 按外部输入决定分配大小等于开一个内存 DoS 面(§9.18 / §16.5)。
批量入口仍须自行校验数量上限(例:mail 的 `max_instances_per_mail`)。

撮合类循环**不适用**:auction 每个 `match_id` 之间夹着 DB 往返且可能提前退出,
批量预留会白白浪费 ID。

## 9. 字符串长度上限(数据库 VARCHAR)

| 字段类型 | 上限 |
|---|---|
| nickname | 32 |
| account | 64 |
| email | 128 |
| device_id | 64 |
| ip_v6 | 64 |
| reason / remark | 256 |
| 长文本 / json | TEXT / JSON 类型 |

## 10. 监控指标命名(Prometheus)

```
pandora_<service>_<metric>{<labels>}
```

例:
```
pandora_login_request_total{method="Login",code="0"}
pandora_login_request_duration_seconds_bucket{method="Login",le="0.1"}
pandora_matchmaker_queue_size{bracket="diamond",region="cn"}
pandora_ds_allocator_pod_count{state="running"}
pandora_kafka_consumer_lag{topic="pandora.battle.result",group="battle_result"}
```

**强制 label**:`service`, `instance`(由抓取端加)
**禁止高基数 label**:不要把 `player_id` 放 label!

## 11. 日志格式(zap structured)

```json
{
  "ts": "2026-06-03T10:00:00.123Z",
  "level": "info",
  "service": "matchmaker",
  "trace_id": "abc123",
  "player_id": 1001,
  "match_id": "M_xxx",
  "msg": "match found",
  "queue_seconds": 42
}
```

**强制字段**:`ts` / `level` / `service` / `msg`
**业务字段**:`trace_id`, `player_id`, `match_id`, `team_id`, `error`
**禁止**:`fmt.Sprintf` 拼字符串到 msg(用 zap field);printf 风格 `Infof/Warnf/Errorf`(日志系统无法按字段索引,一律 `Infow/Warnw/Errorw` 结构化 kv)

### 11.1 日志级别与降噪约定(2026-07-09)

- **级别环境变量**:`LOG_LEVEL=debug|info|warn|error`(默认 info),排障时对单个 pod 临时开 debug,不用重编。
- **gRPC access log**(`pkg/middleware/logging.go`):
  - 成功请求 `rpc_ok` → **DEBUG**(生产 info 级下不输出,高 QPS 噪音主源已消除);
  - 慢请求 `rpc_slow` → **WARN**(阈值 `LOG_SLOW_RPC_MS`,默认 500ms);
  - 失败请求 `rpc_failed` → **ERROR**(带 code/reason/err)。
  - 请求量/延迟统计看 Prometheus(`middleware.Metrics()`),不靠数日志行。
- **周期任务日志**:定时 sweep / 上报类日志只在"有事发生"时打(如 `expired > 0`),空转窗口不准刷屏。
- **业务日志规范**:统一 `plog.With(ctx).Infow("msg", "<snake_case_event>", k, v, ...)`;`msg` 用稳定的事件名(便于日志系统按 `msg` 聚合告警),Warn/Error 必带相关业务 ID(team_id / match_id / player_id)与 `err`。

### 11.2 日志采集(Loki + Alloy,2026-07-09 已落地)

统一 stdout JSON,采集链路 **Grafana Loki(存储/LogQL 查询)+ Grafana Alloy(采集)+ Grafana(UI)**,与 Prometheus/Grafana 同生态。

**label 纪律**:只有低基数字段进 label —— `service` / `level` / `source`(docker|host|k8s)/ `namespace`;`trace_id` / `player_id` 等高基数字段留在 JSON 日志体,查询时用 LogQL `| json | trace_id="xxx"` 过滤,**严禁进 label**(同 §"player_id 不当 Prometheus label" 纪律)。

**部署形态**:

| 模式 | 采集路径 | 配置 |
|---|---|---|
| docker / battle(容器) | Alloy 经 docker.sock 采 `pandora-*` 容器 stdout | `deploy/alloy/config.alloy` |
| local / battle(宿主 go 进程) | Alloy tail `run/logs/*.log` | 同上(compose 挂载 `../run/logs`) |
| k8s(minikube) | 集群内 Alloy 经 kubelet API 采全部 Pod(业务 + Agones DS UE log) | `deploy/k8s/infra/loki.yaml`(start.ps1 -Mode k8s 自动 apply) |

Loki 端口 **3100**(compose 宿主直查),保留 7 天,filesystem 存储(volume `pandora-loki-data`)。Grafana 数据源经 provisioning 自动注入(`deploy/grafana/provisioning/`):`Loki`(compose)/ `Loki (k8s)`(需 `kubectl -n pandora port-forward svc/loki 3101:3100`)/ `Prometheus`。

**本地 dev 默认凭据**:仅用于本机/内网开发,生产/预发必须经 `.env` 或 k8s Secret 覆盖,强密码不进 git。

| 入口 | 地址 | 账号 | 密码来源 |
|---|---|---|---|
| Grafana | `http://localhost:3001` | `${GRAFANA_USER:-admin}` | `${GRAFANA_PASSWORD:-pandora_dev_admin}` |
| MySQL 普通用户 | `localhost:3307` | `${MYSQL_USER:-pandora}` | `${MYSQL_PASSWORD:-pandora_dev_pwd}` |
| MySQL root(仅本地排障) | `localhost:3307` | `root` | `${MYSQL_ROOT_PASSWORD:-pandora_dev_root}` |

**操作入口**:

1. docker / battle 容器模式:启动 dev 栈后打开 `http://localhost:3001`,Explore → 选 `Loki`。
2. local / battle 宿主进程模式:Go 进程日志写入 `run/logs/*.log`,Alloy 会自动采集,仍在 Grafana 的 `Loki` 数据源查询。
3. k8s(minikube)模式:`pwsh tools/scripts/start.ps1 -Mode k8s` 会自动 apply `deploy/k8s/infra/loki.yaml`;查看前先执行 `kubectl -n pandora port-forward svc/loki 3101:3100`,Grafana Explore → 选 `Loki (k8s)`。
4. 快速健康检查:`curl.exe http://localhost:3100/ready`;Grafana 数据源检查可调用 `curl.exe -u "$env:GRAFANA_USER`:$env:GRAFANA_PASSWORD" http://localhost:3001/api/datasources`。

常用 LogQL:

```logql
{service="matchmaker"}                                  # 单服务全部日志
{source="docker", level="error"}                        # 全服务错误日志
{service="login"} | json | player_id="1234"             # 按玩家过滤
{service=~".+"} | json | trace_id="abc123"              # 全链路按 trace_id 追一次请求
{service="matchmaker"} | json | msg="rpc_slow"          # 慢请求
{service="team"} | json | latency_ms > 200              # 按数值字段过滤
```
