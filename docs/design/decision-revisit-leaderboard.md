# decision-revisit:通用排行榜服务(leaderboard)

> 触发:业务新增需求 **全局 / 通用排行榜**——全服排行、按类型排行、工会排行、副本局内
> 局部排行(临时),且部分排行结算后要 **发奖**。该需求横跨多个业务域(战斗 / 社交 / 经济),
> 是「全局运行时基础设施」,且碰 `CLAUDE.md` §9 不变量 #2(结算只落一次)、#7(资源发放原子 +
> 补偿幂等键),按 `AGENTS.md` §7 升级为 decision-revisit。
> 决策级别:服务级(新增 `leaderboard`)+ 存储路线(Redis ZSET + MySQL 结算归档)。
> 用户已于 2026-06-27 确认需求:通用可扩展排行(全服 / 类型 / 工会 / 局部)、临时与非临时分离、
> 结算发奖、内存要小、用 Redis 做排行。本文档定方案,实现见 §6。

---

## 1. 需求拆解

| 维度 | 诉求 | 设计含义 |
|---|---|---|
| 通用 / 可扩展 | 一套机制支持任意榜:全服、各类型、工会、局部 | 榜由 **复合 key**(类型 + scope + scope_id + 周期)唯一标识,服务不内置具体玩法 |
| 临时 vs 非临时 | 副本局内榜临时、用完即弃;全服 / 工会榜长期、周期重置 | 临时榜带 Redis `EXPIRE` TTL 自清,**不落库**;非临时榜持久,结算时落库归档 |
| 结算奖励 | 排行结算后按名次发奖 | `SettleBoard` 取 Top-N → 按 `RewardTable`(名次区间→奖励)发奖,**幂等** |
| 内存要小 | 海量榜 / 海量参与者下内存可控 | ① 临时榜 TTL 自清;② 每榜 `max_size` 截断只留 Top-N;③ 派生数据,不在 Redis 久留冷榜 |
| 用 Redis 做排行 | — | ✅ Redis ZSET(有序集合)是排行榜的业界标准结构,O(log N) 插入 / 查名次 / 范围查询 |

## 2. 关键事实:排行是「派生数据」,Redis 当计算层,MySQL 只兜结算

排行分数本身可由权威源(战斗结果 / 积分流水)重算,**不是不可再生的权威源**。因此:

- **进行中的实时排名**(高频 `ZADD` 热点)→ **只在 Redis ZSET**,不实时落库(落库纯属浪费 + 拖垮性能)。
- **临时榜(副本局内)**→ **纯 Redis + TTL 自清**,永不落库。
- **结算瞬间**(低频)→ 落一份 **Top-N 快照** + **发奖幂等记录** 到 MySQL:
  1. 发奖幂等是硬不变量(§9 #2 / #7),幂等键须有权威落地点,放 Redis 不安全(可能被 evict / TTL);
  2. 赛季回看 / 客服对账 / 审计需要权威记录,Redis 重置后就没了。

**Redis 持久化**:排行只需「重启别从零」,用默认 **RDB 快照 + AOF everysec** 即可,
**不必为排行单开纯 AOF**(每秒万次 ZADD 全落盘 = 写放大浪费,而我们不在乎丢最后 1 秒分数变化)。
真正「不能丢」的部分(发奖凭证 / 结算快照)由 MySQL 兜底。

## 3. 数据模型

### 3.1 榜标识:复合 BoardKey

```
board_type : uint32   榜类型(配置 id):竞技分 / 累计伤害 / 工会贡献 ...
scope      : enum     GLOBAL(全服)/ GUILD(工会)/ INSTANCE(副本局内,临时)/ CUSTOM(其它局部)
scope_id   : uint64   GUILD→guild_id,INSTANCE→match_id,GLOBAL→0
period     : string   ""=永久,"2026-W26" / "2026-06-27" / "S5"=周期(日 / 周 / 赛季)
```

→ 派生稳定的 board 串 `bt:scope:scopeId:period`,作为 Redis key / MySQL 行键。

### 3.2 Redis 结构(同一 board 用 hashtag 锁同 slot,防 CROSSSLOT)

```
pandora:lb:{<board>}:z   ZSET   member=entity_id(player_id / guild_id),score=packed(见 §3.3)
pandora:lb:{<board>}:t   HASH   entity_id → updated_at_ms(展示用 / 审计;只保留精确榜内成员)
pandora:lb:{<board>}:m   HASH   榜元信息(asc / tie / bw 桶宽,建榜时定死)
pandora:lb:{<board>}:s   HASH   entity_id → 真实分(全员,不随截断清理;见 §3.4)
pandora:lb:{<board>}:h   HASH   bucket(floor(score/bw)) → count 分数直方图(全员;见 §3.4)
```

- `entity_id` 既是 ZSET member(排序载体),又是更新 / 查询主键。
- `max_size > 0` 时,SubmitScore 后截断只保留 Top-N(内存控制);被截断成员的分数仍保留在 `:s`。
- `ttl_seconds > 0` 时(临时榜),对全部 key 设 `EXPIRE`(副本结束自动清,内存零残留)。

### 3.3 同分名次:「先达到者靠前」时间 tie-break

ZSET score 是 IEEE-754 double(52 位尾数)。开启 `tie_break_by_time` 时,把分数与时间打包进
单个 double:

```
normTs = ts_ms - EPOCH_MS                 // 自纪元的毫秒(>=0)
降序榜(分高在前): packed = score - normTs * 1e-13   // 同分时 normTs 小(先达)→ packed 大 → 名次高
升序榜(分低在前): packed = score + normTs * 1e-13   // 同分时 normTs 小(先达)→ packed 小 → 名次高
```

- 还原真实分:`score = round(packed)`(时间项 `normTs*1e-13 < 0.5` 恒成立到 ~2120 年,不影响取整)。
- 不开 tie-break 的榜:`packed = score` 直接存,同分按 member(entity_id)字典序(确定但非时间序)。
- INCREMENT 模式 + tie-break:读回 packed → `round` 还原真实分 → 加增量 → 重新打包(Lua 原子)。

> 精度边界:`score` 建议 < ~1e12;超大分值榜应关 tie-break。详见实现注释。

### 3.4 榜外名次区间估算(2026-07-21;头部精确 + 尾部估算)

**动机**:全服常驻榜不设上限时,ZSET 基数跟活跃 / 注册人数走(2000 万 DAU 量级 = 单 key 数 GB,
压死单 Redis 节点,违背 CLAUDE.md §9.18 有界精神)。业界标准解法:**精确名次只保留 Top-N
(`max_size` 可配,如 10 万),榜外玩家用分数直方图给区间估算名次**——第 400 万名的玩家不需要
精确数字,「约 X 名 / 超过 Y% 玩家」即可。

**机制**(全部在 SubmitScore 的同一 Lua 内原子维护):

- `:s` 全员真实分 HASH:是截断后的**分数事实来源**——出榜玩家 INCREMENT 继续累计、
  SET_IF_HIGHER 不降级都靠它(修复了原实现截断即丢分、累计分从 0 重算的缺陷);hash 条目远小于
  ZSET 条目,内存约为全员 ZSET 的一半以下;周期榜随 Clear / TTL 清理。
- `:h` 直方图 HASH:`floor(score/bw) → count`,上报时旧桶 -1 新桶 +1(减到 0 删 field);
  桶宽 `bw` 建榜定死(proto `BoardOptions.estimate_bucket_width`,0 = 服务默认
  `default_estimate_bucket_width`=25),按榜分数量纲选择;桶索引钳制 ±2^20 防异常分撑爆 field 数。
- **估算公式**:`估算名次 = 比我优的桶计数和 + 本桶一半`,并钳制到 `ZCARD+1` 之后
  (榜外估算永远排在精确榜之后,不与精确区打架);同时返回直方图总人数供百分位展示。
- **查询分层**(`GetRank`):精确榜命中 → 精确名次(`estimated=false`);被截断 → 估算名次
  (`estimated=true` + `total_submitters`);从未上报 → `found=false`。客户端对估算名次必须做
  粗粒度文案(「约 25 万名」/「超过 87% 玩家」),不展示假精度。
- `RemoveEntry`(封号清理)同步回扣 `:s`/`:h`;`Clear`(周期 reset)/`Delete`/TTL 一并清理。
- **不变量对齐**:`:h`/`:s` 是非权威、可重建的派生展示状态(§22),不参与结算 / 发奖等权威路径
  (SettleBoard 仍只读精确 ZSET Top-N);估算读路径纯只读。
- **升级兼容**:本改动前建的榜 `:s`/`:h` 为空 → 旧成员首次再上报时从 ZSET 回退补记
  (meta 无 `bw` 时同时补记);未再上报的存量成员不可估算(`found=false`),直方图对其低估
  「比我优」人数——属估算语义可接受,周期榜一个 reset 周期后自然收敛。

**已知边界**:直方图是增量计数器,极端故障(如 Lua 执行后主从切换丢写)可能产生微小漂移;
榜是展示数据 + 周期榜定期 reset 清零,当前不为此加全量重建 job(§15.3 拒绝预设复杂化);
永久榜若压测发现漂移可感知,再补「从 :s 全量重算 :h」的低峰任务。

## 4. RPC 契约(`pandora.leaderboard.v1.LeaderboardService`)

| RPC | 用途 | 调用方 |
|---|---|---|
| `SubmitScore` | 上报分数(SET_IF_HIGHER / SET / INCREMENT),首次写带 BoardOptions 建榜 | 系统(battle_result / 副本 DS / 活动),**非客户端**,内网直连 |
| `GetRank` | 查某 entity 名次 + 分数 | 客户端(经 Envoy)/ 系统 |
| `GetRange` | 取榜区间(Top-N / 分页) | 客户端 / 系统 |
| `GetAround` | 取某 entity 上下 N 名 | 客户端 / 系统 |
| `RemoveEntry` | 移除某 entity(封号 / 作弊清理) | 系统 |
| `SettleBoard` | 取 Top-N → 落快照 + 按 RewardTable 发奖(幂等)→ 可选 reset | 周期榜定时任务 / 副本 DS(局内榜显式调) |
| `DeleteBoard` | 删整个榜(临时榜提前清) | 系统 |

- **写入(SubmitScore / Settle / Remove / Delete)是系统接口**:不在 Envoy 暴露,带玩家 JWT 的调用一律拒绝(同 inventory.GrantItems)。
- **读取(GetRank / GetRange / GetAround)** 可经 Envoy 给客户端,只回 `LeaderboardEntry` 客户端可见结构(不变量 §14)。

### 4.1 结算发奖(SettleBoard)双写

用户确认「两者都要」:

1. 取 Top-N 快照 → 落 `leaderboard_snapshot`(归档);
2. 写 `leaderboard_settlement`(批次头 + `settle_idempotency_key` UK,防重复结算);
3. 按 `RewardTable` 逐名次发奖:
   - **直接调 `inventory.GrantItems`**(幂等键 = `lb:<settlement_id>:<entity_id>`,到背包),写 `leaderboard_reward_log`;
   - **同时发 kafka `pandora.leaderboard.settle`**(供别的服务消费 / 风控对账,弱依赖)。
4. `reset_after=true` 时结算后清空 ZSET(周期榜进入下一周期);临时榜结算后直接 `DeleteBoard`。

> 奖励表 `RewardTable`(名次区间→道具)由**调用方**在 Settle 时传入,leaderboard 服务保持
> 玩法无关(不内置奖励配置)。后续若要配置表驱动,可在调用方(活动 / 玩法服务)接配置中心,
> 不污染 leaderboard。

## 5. 触发与并发

- **周期榜结算**:由调用方 / 定时任务在周期切换点调 `SettleBoard`(本服务不内置 cron 玩法,
  仅提供幂等 Settle;`settle_idempotency_key = board + period` 保证一个周期只结算一次)。
- **副本局内榜**:DS / 玩法在局结束时显式调 `SettleBoard` + `DeleteBoard`。
- **写并发**:SubmitScore 的 read-modify-write(SET_IF_HIGHER / INCREMENT + 打包 + 截断 + TTL)
  用 **Lua 脚本** 在 Redis 端原子执行,避免竞态;无需进程锁。
- **发奖幂等**:`leaderboard_reward_log` 以 `grant_idempotency_key` UK 兜底,叠加 inventory 侧
  `GrantItems` 幂等键,双层幂等(对齐 auction 结算模式)。

## 6. 落地清单(2026-06-27 一次到位)

| 资源 | 取值 | 依据 |
|---|---|---|
| 服务名 | `leaderboard`(runtime 域) | 全局运行时基础设施,不属社交 / 经济单一域 |
| 目录 | `services/runtime/leaderboard` | 对齐 push / player_locator |
| gRPC / metrics 端口 | **20007 / 21007** | `infra.md` §6.2 runtime 段空档(locator 20006 之后) |
| proto 包 | `pandora/leaderboard/v1/leaderboard.proto` | `proto-design.md` §1 |
| errcode 段 | **13000-13999** | `errcode.go` 段规划(12000 auction 之后,11000 预留段不动) |
| MySQL 库 | `pandora_leaderboard` | 结算归档专用(`leaderboard_settlement / _snapshot / _reward_log`) |
| Redis | ZSET 订单簿同款强依赖 | §3.2 |
| Kafka | `pandora.leaderboard.settle`(弱依赖) | §4.1 |
| inventory | `GrantItems` 发奖(弱依赖,留空 noop) | §4.1 |

**进行中排名 / 临时榜一张 MySQL 表都不建**;MySQL 只服务「结算结果 + 发奖凭证」。

## 7. 与现有服务边界

- 与 `auction`(全服撮合)无关:auction 是订单簿撮合,leaderboard 是分数排序,两套 ZSET 用法。
- 与 `battle_result`:battle_result 是分数来源之一(战后调 SubmitScore),不在 battle_result 内做排行。
- 与 `inventory`:leaderboard 结算发奖复用 inventory.GrantItems 幂等发放,不自己动背包。
