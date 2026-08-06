# leaderboard

> 通用排行榜服务:Redis ZSET 做实时排名(全服 / 类型 / 工会 / 副本局内临时,一套复合 key 机制通吃),
> `SettleBoard` 取 Top-N 落 MySQL 快照 + 按 `RewardTable` 幂等发奖(调 inventory) + 发 kafka 结算事件。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`
> 的 [`decision-revisit-leaderboard.md`](../../../docs/design/decision-revisit-leaderboard.md);跨服务要约见
> [`go-services.md`](../../../docs/design/go-services.md);端口 / 库表 / topic 见
> [`infra.md`](../../../docs/design/infra.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:维护「榜 → (entity → 分数 / 名次)」的实时排名(Redis ZSET);结算 = 取 Top-N →
  落快照 + 幂等发奖 + 发事件 → 可选 reset。
- **玩法无关**:榜由**复合 key**(`board_type + scope + scope_id + period`)唯一标识,服务不内置任何
  具体玩法或奖励配置;`RewardTable`(名次区间 → 道具)由**调用方**在 `SettleBoard` 时传入。
- **权威分层**(不变量 §22):Redis ZSET 是实时排名的**计算层 / 强依赖**(不可降级);MySQL
  `pandora_leaderboard` 是**结算权威归档**(快照 + 发奖凭证);进行中的实时榜 / 临时榜**不落库**。
  榜外区间估算(直方图)是**非权威、可重建的派生展示状态**,不参与任何权威写。
- **不做的事**:不算分数来源(battle_result 等上游战后调 `SubmitScore` 报事实分,不变量 §9.6);
  不内置周期 cron(周期切换点由调用方 / 定时任务显式调 `SettleBoard`);工会榜(scope=GUILD)只落
  快照 + 发 kafka,**不直接发玩家背包**,由工会服务消费分发。
- **无 leader 选举**:本服务无状态、可水平扩展;后台补扫 / 保留期清理各副本各自跑,靠幂等键 + 批量
  有界保证并发安全(与 matchmaker 的单写者撮合循环不同)。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20007` | 读 RPC(客户端经 Envoy)+ 系统写 RPC(仅内网直连) |
| HTTP | `:21007` | 仅 `/metrics`(`leaderboard.proto` 无 `google.api.http` 注解,无 RESTful RPC) |

端口默认值来自 `internal/conf/conf.go` 的 `Defaults()`(`Server.Grpc.Addr=:20007` / `Server.Http.Addr=:21007`)。

## 对外接口

代码入口:`internal/service/leaderboard.go`(gRPC service 层,proto ↔ biz/data 互转 + `errcode → commonv1.ErrCode` 1:1 映射)。
RPC 定义:`proto/pandora/leaderboard/v1/leaderboard.proto`(共 7 个 unary RPC,无 stream)。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `SubmitScore(board, entity_id, score, mode, options)` | **系统**(battle_result / 副本 DS / 活动),内网直连 | 按 `mode`(SET_IF_HIGHER / SET / INCREMENT)写分;首次写带 `BoardOptions` 建榜 | **拒绝玩家 JWT**(callerID 必须 =0) |
| `GetRank(board, entity_id)` | 客户端 | 查名次:精确榜命中回精确名次;被截断出榜回直方图区间估算(`estimated=true`) | 允许玩家 JWT / 匿名读 |
| `GetRange(board, offset, limit)` | 客户端 | 取榜区间 + 榜总人数 | 允许玩家 JWT / 匿名读 |
| `GetAround(board, entity_id, radius)` | 客户端 | 取某 entity 上下 `radius` 名(含自身) | 允许玩家 JWT / 匿名读 |
| `RemoveEntry(board, entity_id)` | **系统**(封号 / 反作弊清理) | 从榜移除某 entity(直方图同步回扣) | **拒绝玩家 JWT** |
| `SettleBoard(board, top_n, reward_table, reset_after, settle_idempotency_key)` | **系统**(周期榜定时任务 / 副本 DS 局内榜) | 取 Top-N → 落快照 + 幂等发奖 → 发 kafka → 可选 reset | **拒绝玩家 JWT** |
| `DeleteBoard(board)` | **系统**(局内榜结束清理) | 删整个榜(`z/t/m/s/h` 全 key) | **拒绝玩家 JWT** |

> **系统写 RPC 的双重约束**(`internal/service/leaderboard.go` 各 handler 首行 `pmw.PlayerIDFromContext(ctx) != 0`
> 即回 `ERR_PERMISSION_DENY`):①这些路由**不在 Envoy 暴露**;②即便被误配进来,带玩家 JWT 的调用一律拒
> (同 `inventory.GrantItems` 模式)。读 RPC 只回**客户端可见结构**(`LeaderboardEntry`,不外露 `StorageRecord`,不变量 §14)。
>
> **会话现行性门**(`internal/server/grpc.go`,`pmw.SessionCurrent`):客户端面请求的 jti 必须是 login 会话
> 权威当前一代;顶号 / 登出后旧 JWT 在 exp 前即失效(R5 复审 P0-1,INC-20260722-004)。内网系统调用无 JWT,天然放行。

## 目录结构(Kratos 标准分层,对齐 matchmaker / inventory)

```
cmd/leaderboard/main.go        启动入口(mysql + redis + snowflake + kafka + inventory client 装配 + 补扫/清理 goroutine)
etc/leaderboard-dev.yaml       开发期配置(:20007 / redis 6380 / mysql 3307 / kafka 9093 / inventory 20015)
internal/
  conf/conf.go                 配置结构(嵌入 config.Base + LeaderboardConf)+ Defaults()
  service/
    leaderboard.go             RPC 入口(实现 leaderboardv1.LeaderboardServiceServer,鉴权下沉 + proto 互转)
  biz/
    leaderboard.go             LeaderboardUsecase 核心(Submit/读查询/Settle/发奖/补扫)+ RewardGranter/SettleEventPusher 接口
  data/
    board_store.go             Redis ZSET 排行榜(submitLua / removeLua 原子写 + 榜外直方图估算)
    leaderboard_repo.go        MySQL 结算归档(settlement / snapshot / reward_log + 保留期清理)
    reward_client.go           inventory 服务 gRPC client(GrantItems 幂等发奖,实现 biz.RewardGranter)
  server/
    grpc.go                    gRPC server 注册(AuthOptional + SessionCurrent 中间件)
    http.go                    HTTP server 注册(仅 /metrics)
```

## 核心调用链

### 1. SubmitScore —— 系统上报,单 Lua 原子写

`service.SubmitScore`(`leaderboard.go`)拒玩家 JWT → `toBoardKey` / `toOptions` → `biz.SubmitScore`
(`internal/biz/leaderboard.go`,函数 `SubmitScore`):校验 `board` / `entity_id != 0` / `mode` 钳到
`[SET_IF_HIGHER, INCREMENT]` / `EstimateBucketWidth` 兜默认值 → `board.Submit`
(`internal/data/board_store.go`,函数 `Submit` → `submitLua`)。**全部写在同一段 Lua 里原子完成**:

```
submitLua(KEYS: z/t/m/s/h)
├─ 首写定 meta:HSET :m {asc, tie};:m 无 bw 时补记桶宽(建榜后不可变)
├─ 读旧真实分:优先 :s 全员分,:s 无则回退 :z(升级前旧榜存量成员)
├─ 按 mode 算新真实分
│    · INCREMENT → 旧分 + score
│    · SET       → score
│    · SET_IF_HIGHER → 仅更优才写(降序取大 / 升序取小,否则 doWrite=false)
├─ tie-break 打包 → ZADD :z packed + HSET :t updated_at_ms   (§3.3 时间打包)
├─ 维护 :s 全员真实分 + :h 分数直方图(桶 = floor(分/bw),旧桶 -1 新桶 +1)
├─ 截断 max_size:只清 :z/:t 的出榜成员,:s/:h 保留全员(供榜外估算)
├─ 设 TTL(临时榜:z/t/m/s/h 一起 EXPIRE)
└─ 返回 {newReal, rank(1-based;出榜/不在榜=0)}
```

> **为什么读旧分优先 `:s` 而非 `:z`**:`max_size` 截断后被挤出精确榜的成员仍需保留真实分,才能让
> 后续 `INCREMENT` 正确累计、`SET_IF_HIGHER` 不被截断误判为「首次」而降级。`:z` 是有界精确榜、`:s` 是全员分。

### 2. GetRank —— 精确名次 + 榜外直方图区间估算

`service.GetRank` → `biz.GetRank`(`internal/biz/leaderboard.go`,函数 `GetRank`):

```
boardAscending(GetMeta 读 :m 的 asc)      榜不存在 → Found=false
   └─ board.Rank(ZRank/ZRevRank + ZScore + HGet :t)
        ├─ 命中(在精确榜)→ RankView{精确 Entry, Estimated=false}
        └─ 未命中 → board.Estimate(榜外估算)
             读 :s 拿真实分 → 读 :m 的 bw → HGETALL :h 直方图
             est = Σ(比我优的桶计数) + (本桶计数+1)/2      桶内取中位
             est 钳到 ZCARD+1 之后(榜外估算名次不得落进精确区)
             → RankView{估算 Entry, Estimated=true, TotalSubmitters=直方图口径总人数}
```

`board.Estimate`(`internal/data/board_store.go`,函数 `Estimate`)是**纯只读、无锁**;从未上报或升级前
旧榜(`:s` / `:m.bw` 缺失)→ `found=false`。估算 `Entry.UpdatedAtMs` 恒 0,客户端按「约 X 名 / 百分位」展示。

### 3. GetRange / GetAround —— 读区间

- `biz.GetRange`(函数 `GetRange`):`limit` 钳到 `[1, MaxListLimit]`(默认取 `DefaultListLimit`)→
  `board.Range`(`ZRange/ZRevRangeWithScores`)+ `board.Total`(`ZCard`)。
- `biz.GetAround`(函数 `GetAround`):`radius` 钳到 `[1, MaxListLimit]` → `board.Around` 先取自身名次
  再取 `[idx-radius, idx+radius]` 区间;不在榜 `found=false`。
- 两者都经 `boardAscending` 按榜 meta 的 `asc` 决定 `ZRange` 还是 `ZRevRange`。名次 = 区间首项 0-based
  下标 + i + 1(`toEntries`)。

### 4. SettleBoard —— 结算发奖双写 + 幂等

`service.SettleBoard` 拒玩家 JWT → `biz.SettleBoard`(`internal/biz/leaderboard.go`,函数 `SettleBoard`):

```
topN 兜底(DefaultSettleTopN=100);settle_idempotency_key 空 → "lb:" + board 串
 └─ boardAscending 校验榜存在(否则 ErrLeaderboardBoardNotFound=13001)
      └─ board.Range(0, topN, asc) 取 Top-N winners
           └─ settlement_id = snowflake.Generate()
                └─ repo.ClaimSettlement(INSERT uk=settle_idempotency_key)   ← 防重复结算(§9.2)
                     ├─ already=true → loadSnapshotWinners 从 MySQL 快照回放,不重复发奖
                     └─ 首次 → repo.SaveSnapshot(INSERT IGNORE Top-N 快照)
                          ├─ grantRewards(仅 scope != GUILD 且 reward_table 非空)
                          │    逐名次 rewardsForRank → repo.ClaimReward(uk=grant_idempotency_key,§9.7)
                          │      → granter.Grant(inventory.GrantItems,幂等键 lb:<sid>:<entity>)
                          │      → MarkReward(GRANTED / FAILED);单条失败不中断整批
                          ├─ events.PushSettle → kafka pandora.leaderboard.settle(key=settlement_id,弱依赖)
                          └─ reset_after=true → board.Clear(进入下一周期)
```

- **幂等命中为何从 MySQL 快照回放**:首次结算若 `reset_after=true` 已清空 Redis 榜,重放只能取
  `leaderboard_snapshot`(结算权威记录);Redis 是计算层、可 evict / TTL,不能当回放源(`biz/leaderboard.go`
  函数 `loadSnapshotWinners`)。
- **发奖幂等两道键**:`settle_idempotency_key`(批次级,防重复结算)+ `grant_idempotency_key =
  lb:<settlement_id>:<entity_id>`(名次级,防重复发奖,透传给 inventory `GrantItems`)。
- **GUILD 榜**:`entity_id` 是 `guild_id`,不调 `granter`(GrantItems 发给玩家),只落快照 + 发 kafka,
  由工会服务消费分发(`internal/data/reward_client.go` 顶部注释)。

### 5. 后台补扫 + 保留期清理(main.go goroutine)

`cmd/leaderboard/main.go` 起两个后台 ticker(各副本各自跑,幂等 + 批量有界,无需 leader):

- `runRewardRetrySweep`(每 1min,函数 `runRewardRetrySweep`)→ `uc.RetryUngrantedRewards`(`biz/leaderboard.go`
  函数 `RetryUngrantedRewards`):`ListUngrantedRewards`(`status<>GRANTED` 且 `updated_at_ms` 早于 grace)→
  重 `Grant`(幂等 no-op)→ `MarkReward`。补两类漏发:**FAILED**(inventory 拒绝 / 不可达)与 **PENDING**
  (`ClaimReward` 后、`MarkReward` 前进程崩残留)。`grace`(默认 2min)把「刚结算还在同步发」的批次挡在扫描外。
- `runRetentionSweep`(每 1h,函数 `runRetentionSweep`)→ `PurgeSnapshotsBefore` / `PurgeGrantedRewardsBefore`
  (`DELETE ... LIMIT` 单批 500)。保留期默认 90 天(不变量 §9.24);**`leaderboard_settlement` 故意不清**
  (settle uk 是防重复结算的永久闸,每批次 1 行慢增长豁免);`reward_log` 只清 GRANTED(PENDING/FAILED 是补发工作集)。

## 存储布局

### Redis ZSET(实时排名,强依赖)

同一 board 的 5 个 key 用 hashtag `{<board>}` 锁同一 Cluster slot(`submitLua` 一次碰全部,避免 CROSSSLOT)。
`<board>` = `"<board_type>:<scope>:<scope_id>:<period>"`(`period` 空用 `"-"` 占位)。

| Key | 类型 | 内容 | 清理 |
|---|---|---|---|
| `pandora:lb:{<board>}:z` | ZSET | `member=entity_id`,`score=打包分`(时间 tie-break);精确榜,`max_size` 截断只保留 Top-N | Delete / Clear / TTL |
| `pandora:lb:{<board>}:t` | HASH | `entity_id → updated_at_ms`(展示 / 审计;只留榜内成员) | 随截断清理 |
| `pandora:lb:{<board>}:m` | HASH | 榜元信息 `asc / tie / bw`(建榜时定死,后续变更忽略) | Delete / TTL |
| `pandora:lb:{<board>}:s` | HASH | `entity_id → 真实分`(**全员**,不随截断清理;截断后 INCREMENT / SET_IF_HIGHER 语义正确性靠它) | Delete / Clear / TTL |
| `pandora:lb:{<board>}:h` | HASH | `bucket(floor(分/bw)) → count` 分数直方图(**全员**;榜外区间估算) | 桶归零 HDEL / Delete / Clear |

> 分数打包与还原、直方图估算的完整推导见 `board_store.go` 顶部注释与
> [`decision-revisit-leaderboard.md §3.3`](../../../docs/design/decision-revisit-leaderboard.md)。桶索引钳制
> `±maxBucketIdx`(`1<<20`),Go 侧常量与 Lua 内常量必须一致。

### MySQL 结算归档(库 `pandora_leaderboard`,强依赖)

建表见 `deploy/mysql-init/10-leaderboard-tables.sql`;索引见 [`infra.md`](../../../docs/design/infra.md)。

| 表 | 用途 | 关键约束 |
|---|---|---|
| `leaderboard_settlement` | 结算批次头 | `uk(settle_idempotency_key)` 防重复结算(§9.2) |
| `leaderboard_snapshot` | 结算 Top-N 名次快照(归档 / 对账 / 幂等回放) | `PK(settlement_id, rank)`;写 `INSERT IGNORE` |
| `leaderboard_reward_log` | 逐名次发奖记录(PENDING / GRANTED / FAILED) | `uk(grant_idempotency_key)` 防重复发奖(§9.7) |

## 配置项(`internal/conf/conf.go`,键前缀 `leaderboard.*`)

| 键 | 默认 | 说明 |
|---|---|---|
| `default_list_limit` | `50` | `GetRange` 未指定 `limit` 时的默认返回条数 |
| `max_list_limit` | `200` | `GetRange` / `GetAround` 单次返回硬上限(读取侧上限,§9.18) |
| `default_around_radius` | `10` | `GetAround` 未指定 `radius` 时的默认上下名数 |
| `default_settle_top_n` | `100` | `SettleBoard` 未指定 `top_n` 时默认结算前 N 名 |
| `default_estimate_bucket_width` | `25` | 建榜未指定 `estimate_bucket_width` 时的直方图桶宽(MMR 量纲,建榜后不可变) |
| `inventory_addr` | 空 | inventory 内网 gRPC 地址(如 `127.0.0.1:20015`);配了走真实 `GrantItems`,留空退 Noop |
| `allow_noop_reward` | `false` | `inventory_addr` 为空时是否允许退回 `NoopRewardGranter`(不真实发奖);默认 false → 漏配即 **fail-fast** |
| `retention_days` | `90` | 名次快照 + 已发放发奖记录保留天数(§9.24;`settlement` 不清) |
| `retention_sweep_batch` | `500` | 每轮每表清理行数上限(`DELETE ... LIMIT`) |

**强 / 弱依赖**(`cmd/leaderboard/main.go` 启动顺序):MySQL(强,缺 DSN fail-fast)→ Redis + Ping(强,不可降级)
→ Snowflake(`settlement_id`)→ kafka producer(**弱**,broker 不通仅 warn)→ RewardGranter(配 `inventory_addr`
走真实发奖;留空且 `allow_noop_reward=true` 才退 Noop,否则 fail-fast)。

## 本地启动

```powershell
# 1. 基础设施(mysql 3307 + redis 6380 + kafka 9093;发真实奖励还需起 inventory :20015,留空走 Noop)
pwsh tools/scripts/dev_up.ps1

# 2. 启 leaderboard
go run ./services/runtime/leaderboard/cmd/leaderboard -conf services/runtime/leaderboard/etc/leaderboard-dev.yaml
```

> dev 配置 `enable_reflection: true` 便于 grpcurl 直连联调;`session_gate.require: false`(宽松档);
> 无背包联调时把 `inventory_addr` 留空并显式设 `allow_noop_reward: true`(否则启动 fail-fast)。生产由
> 集群配置生成器机械置 `session_gate.require=true` 且关 reflection。

## 关联文档

- [`decision-revisit-leaderboard.md`](../../../docs/design/decision-revisit-leaderboard.md) — 通用排行榜设计:复合 key / 临时 vs 非临时 / 分数打包 §3.3 / 榜外估算 / 结算发奖双写 / 调用方矩阵
- [`go-services.md`](../../../docs/design/go-services.md) — 后端服务目录(leaderboard 服务清单项)
- [`infra.md`](../../../docs/design/infra.md) — 端口 `20007/21007`、库表 `pandora_leaderboard`、topic `pandora.leaderboard.settle`
- [`zero-downtime-update.md`](../../../docs/design/zero-downtime-update.md) §6.2 — `max_conn_age` GOAWAY 重拨,滚动更新流量滚到新副本
