# 数据库容量守护与大字段排查手册

> CLAUDE.md §9 不变量 24 的运行时侧。2026-07-22 落地。
> 回答两个问题:**超上限怎么知道**、**某个字段/某一格数据过大了怎么查**。

## 0.0 最重要的一条:保留期清理**默认只报告不删**(2026-07-22)

用户指令:**「不能清理我的数据,只能打印日志」**。所以:

- `retention_mode` 留空(默认)= `report_only`:周期统计"有多少行满足清理条件"→ 打 `WARN`
  日志 + `pandora_db_retention_pending_rows` gauge,**一行都不删**。
- 真删必须显式配 `retention_mode: delete`。写错的值(`del` / `true` / `1` / `on` …)
  **一律报错拒启,绝不猜成 delete** —— 拼错一个字母就开始删生产数据是不可接受的失败模式。
- `dbcheck` 工具**永不 DELETE**(旧 `-force-sweep -confirm=YES-DELETE` 已整块移除,
  且刻意不保留同名 flag:留着改语义会让按旧文档敲的命令静默变行为)。

**必须分清两类删除,别混为一谈**:

| | **业务语义删除**(照常删,不受约束) | **运维语义删除**(默认只报告) |
|---|---|---|
| 含义 | 东西本来就该没了 | 数据还有效,只是占地方 |
| 例子 | 道具过期、邮件失效、挂单置 EXPIRED、玩家丢弃道具/解散公会、出箱投递成功即删 | 幂等流水超 90 天、终态申请行、已结算成交流水、私聊历史、设备绑定行 |
| 归属 | 各业务模块自己的逻辑 | 本文档 + `pkg/dbguard/sweep.go` |

代价说清楚:**report-only 下库会继续增长**。这是有意的取舍——把"何时删"的决定权交回人手里,
换来的是"绝不会因为配错条件而静默删掉不该删的数据"。因此待清理量必须持续可见
(WARN 日志 + metric + `dbcheck -pending`),让人能判断何时该开删或调保留期。

## 0. 一句话结论

增长有两个**互相独立**的失控方向,只查一个必漏另一个:

| 方向 | 症状 | 信号 | 闸在哪 |
|---|---|---|---|
| **广度**:行数变多 | 表越来越大 | `TABLE_ROWS` | 保留期清理(§9.24 清单) |
| **深度**:单行变胖 | 行数正常但表照样大 | `AVG_ROW_LENGTH` | 写入侧 payload / 元素数上限 |

本仓真实案例(2026-07-22 审计),两个方向各错一个:

- **广度有闸、深度无闸**:bag 管住了单次操作的 items 条数(`MaxItemsPerOp=64`),
  但没管**单个 `BagItem` 的 `attrs` 条数** —— 单个"格子"可以无限胖。
- **深度有闸、广度无闸**:`rewardclaim` 管住了单条位图大小(`MaxBitIndex`=128KiB),
  但没管**位图条目数** —— 每条都不大,条数无限,整行照样爆(已修,见 §4)。

所以三个上限**必须同时设**,缺一个就有洞:

```
① 单元素上限     单个格子/单条附件/单条词条 ≤ M 字节、≤ K 个子元素
② 集合条目上限   格子数/附件数/位图条数     ≤ N 个
③ 整体字节上限   序列化后 payload          ≤ P 字节   ← 最后兜底,不能只靠它
```

只设 ③ 会漏掉"一个格子胖到 60KB 但整体还没超"的情况;
只设 ①② 会漏掉"每项都合规但项数×大小仍然超列容量"的情况。

## 1. sql_mode 必须严格 —— 否则一切都是白搭

**这是本文最重要的一条。** 非严格 `sql_mode` 下,超长写入不报错,而是**静默截断**:

真 MySQL 8.4 实测(2026-07-22):

```
[严格模式] 往 VARBINARY(16) 插 100 字节 → Error 1406 Data too long  (写入失败,数据安全)
[非严格]   往 VARBINARY(16) 插 100 字节 → err=nil,LENGTH()=16       (静默丢 84 字节)
```

后果:玩家背包/邮件附件被无声砍掉一半,`proto.Unmarshal` 失败,该玩家数据永久损坏,
而且**没有任何错误、没有任何日志**。等发现时已经没法追溯有多少玩家受影响。

因此:

- `pkg/dbguard.AssertStrictMode` 在**服务启动时断言**,不满足直接 `os.Exit(1)`。
  这是本项目唯一允许因数据库检查而拒绝启动的场景——静默数据损坏比服务起不来严重得多。
- MySQL 8.4 默认 `sql_mode` 含 `STRICT_TRANS_TABLES`,所以现状是安全的;
  但**默认值不是保证**:换镜像、运维改配置、DSN 里带 `sql_mode` 参数都会静默降级。
- 注意断言查的是 `@@session.sql_mode` 而非 `@@global` —— 真正决定写入行为的是 session。

## 2. 超上限怎么知道(自动告警)

### 2.1 服务启动 + 周期巡检

每个服务在 `internal/data/budgets.go` 声明**自己负责的表**的容量预算,启动时立刻跑一轮
(拿基线,让"上线时就已超限"当场可见),之后每小时一轮
(inventory 复用已有 sweep ticker;其余服务独立 ticker,均经 `safego.Run` 兜 panic)。

**覆盖状态(2026-07-22):12 个连 MySQL 的服务 12/12 全部接入**严格模式断言 + 容量巡检:
`login / player / battle_result / data_service / auction / inventory / leaderboard / owner /
chat / friend / guild / mail`。

两处需要注意的接法:

- **`pandora_social` 是 chat/friend/guild/mail 四服务共用库**:各服务只声明自己负责的那几张表,
  不整库巡检——否则同一张表被四个服务重复告警,且谁该处置不清楚。
- **`auction` 是分片库**:逐分片各建一个 `Guard`,预算按**单分片**量级给。
  严格模式断言同样逐分片做——各分片可能连不同实例,配置漂移完全可能只出现在个别分片上。

- 查 `information_schema`(估算,毫秒级、不锁表、不扫数据),**绝不用 `COUNT(*)`**
  ——千万行表要几十秒,放启动路径会拖垮滚动更新。
- 超预算 → `ERROR` 日志 + `pandora_db_budget_violations_total` 计数,
  **不阻止启动**:容量超限是"要去查的问题",不是"服务不能跑的理由"。
  拒绝启动会把容量问题升级成可用性事故(违反验收底线第 1 条)。

日志长这样,直接告诉排查者该看什么:

```
level=ERROR msg=db_capacity_budget_exceeded db=pandora_trade table=inventory_ledger
  kind=rows actual=61000000 budget=54000000
  note="幂等流水,保留期 90 天;超限先查 inventory sweep 是否在跑,再查写入速率是否异常"
  hint="见 docs/design/db-capacity-guard.md 排查手册;..."
```

### 2.2 写入侧"逼近告警"(比爆掉才报强得多)

`pkg/dbguard.CheckPayload` 在序列化后、落库前拦一道,三档:

| 条件 | 动作 | 为什么 |
|---|---|---|
| `size >= Max` | **拒写** + ERROR | 宁可拒绝一次写入,也不让超长数据落库被截断(完整性优先) |
| `size >= Max × 0.8` | 放行 + **WARN** | 数据还没坏,但趋势已异常——**这是排查成本最低的时点** |
| 否则 | 放行,不打日志 | 热路径零噪音 |

`Max` 的取值口径(**定错等于没定**):按**设计期望**定,不按列类型上限定。
例:列是 `BLOB(65535)` 但设计只装 16 个附件 ≈ 2KB,`Max` 就该取 4096 而不是 65535——
写成 65535 等于没设:等数据涨到 60KB 才告警时,业务语义早就崩了。

### 2.3 Prometheus(告警的权威来源,日志只是给人看的旁证)

| 指标 | 用途 |
|---|---|
| `pandora_db_table_rows{db,table}` | 行数;配 `_budget` 做超限告警 |
| `pandora_db_avg_row_bytes{db,table}` | **排查大字段最灵敏的信号**,见 §3 |
| `pandora_db_payload_bytes{db,table,column}` | 写入 payload 分布 histogram,**看 p99 不看 max** |
| `pandora_db_budget_violations_total{db,table,kind}` | 超限计数,非零即需人查 |
| `pandora_db_payload_rejected_total{db,table,column}` | 被拒写次数,非零即需人查 |

为什么强调 **p99 而非 max**:只看 max 会被单个异常值带偏。
p99 正常 + max 爆 = **个别**数据畸形(某个玩家被刷/被 bug 撑爆);
p99 一起涨 = **全体普遍**变胖(设计性无界增长)。两者的排查方向完全不同。

## 3. 排查 runbook:某一格/某个字段数据过大了怎么查

按顺序走,每一步把范围缩小一个量级。

### 第 1 步:确认是"变胖"还是"变多"

```sql
SELECT TABLE_NAME, TABLE_ROWS, DATA_LENGTH, AVG_ROW_LENGTH
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'pandora_bag' ORDER BY DATA_LENGTH DESC;
```

- `TABLE_ROWS` 涨、`AVG_ROW_LENGTH` 平 → **广度**问题,查清理任务(§9.24 清单)是否在跑。
- `AVG_ROW_LENGTH` 涨 → **深度**问题,继续第 2 步。**这一步最关键**:
  总量增长可能只是行数增长(正常),平均行长增长一定异常。

### 第 2 步:定位到列 + 判断"个例还是普遍"

```bash
cd tools/migrate
go run ./cmd/dbcheck -dsn '...' -size-check -top-rows 20
```

输出每个登记大字段的 `rows / max / avg / 预算 / 超限行数`,按超限程度排序,并给出
"超了该查什么"。手工等价查询:

```sql
SELECT COUNT(*)                                   AS rows_total,
       MAX(LENGTH(section))                       AS max_bytes,
       AVG(LENGTH(section))                       AS avg_bytes,
       SUM(LENGTH(section) > 262144)              AS over_rows
FROM pandora_bag.bag_section;
```

判读:

- `over_rows` 很小(个位数)、`avg` 正常 → **个例**:某几个玩家的数据畸形。
- `over_rows` 占比高 或 `avg` 也涨 → **普遍**:设计性无界增长,改设计不是清数据。

### 第 3 步:定位到具体行(是谁)

```sql
SELECT player_id, LENGTH(section) AS n
FROM pandora_bag.bag_section ORDER BY n DESC LIMIT 20;
```

(`dbcheck -size-check -top-rows 20` 已自动做这步。)

### 第 4 步:定位到字段(哪个 repeated 爆了)

把那几行 dump 出来反序列化。这一步没有捷径,但目标很明确:找出**哪个 repeated 字段
元素数异常**。

```sql
-- 先看是不是单纯条目多(以 bag_section 为例,存的是 pb BagSection)
SELECT player_id, LENGTH(section) FROM pandora_bag.bag_section WHERE player_id = <定位到的玩家>;
```

然后用 Go 小程序或 `protoc --decode` 反序列化,重点看:
`repeated items` 有多少条、其中**单个 item 的 `attrs` 有多少条**(这就是"某一格子数据过大")。

### 第 5 步:回到写入路径找为什么没有上限

拿着"哪个字段爆了",回代码里找该字段的写入点,检查 §0 的三个上限:

- 单元素上限有没有?(如 `len(item.Attrs) <= N`)
- 集合条目上限有没有?(如 `len(items) <= N`)
- 整体字节上限有没有?(`dbguard.CheckPayload`)

**只补最里层缺的那个**,不要三个都堆一遍(§15 简单性)。

## 4. 已知缺口与处置(2026-07-22 审计)

| 位置 | 问题 | 状态 |
|---|---|---|
| `pkg/rewardclaim` | 位图**条目数**无上限,而 `ClaimReward` 是客户端可直调、`source`/`activity_instance_id` 无白名单 → 单玩家 record(LONGBLOB)可被刷到无界 | **已修**:`MaxPermanentSources=64` / `MaxActivityInstances=256` / `MaxSourceNameLen=64`,只拦新增条目(已有条目继续可领,不回档);触顶打 ERROR 留证 |
| `BagItem.attrs` | 单个格子的词条数全仓零校验 | **未修**,现网 `attrs` 恒空(`identify_rules` 未配置)+ 背包域功能默认关,故为 P2;启用前必须补 `MaxAttrsPerItem` |
| `bag_migration.SeedLegacyWarehouse` | 容量参数显式传 `math.MaxUint32`,绕过容量闸;且拆堆是 O(N²) | **未修**,`LegacyMigrationEnabled` 默认关,contract 阶段前必须处理 |
| `match_release_outbox.payload` | `VARBINARY(1024)` 装含 `repeated player_ids` 的 record,队伍规模变大(5v5→10v10)会逼近上限 | **未修**,当前规模安全;已登记 dbcheck 预算 768(75% 预警线) |
| `leaderboard_reward_log.reward_json` | `VARCHAR(2048)` 装奖励明细,`RewardTier.items` 条数无上限 | **未修**,已登记预算 1536(75%) |
| `player_data.nickname/avatar` | proto2mysql 生成 `MEDIUMTEXT(16MB)`,写入侧无长度校验 | **未修**,P2 |

**根治方向**(比加上限更好):`rewardclaim.ClaimPermanentByID` + `BitIndexMap` 白名单
已经存在但没被使用,奖励配置表落地后应切过去——那样 `source`/`id` 必须在配置表里,
条目数天然有界,不需要人为设上限。

## 5. 上线前 / 压测怎么用

见 `stress-discipline.md` §4.1.1 / §4.3。核心:

```bash
# 上线前门禁(exit 0 = PASS):无未登记表 + 清理索引齐备 + outbox 无堆积
go run ./cmd/dbcheck -dsn '...'

# 上线前大字段体检(全表扫描,慢,单独跑)
go run ./cmd/dbcheck -dsn '...' -size-check -top-rows 20

# 压测前后:增量必须能用业务量解释
go run ./cmd/dbcheck -dsn '...' -exact -snapshot db-before.json
go run ./cmd/dbcheck -dsn '...' -exact -compare db-before.json
```

## 6. 新增表/新增大字段时的检查清单

- [ ] 表登记进 `CLAUDE.md §9.24` 清单 **和** `dbcheck` 的 `registry`(两处必须同步)
- [ ] 只增表有保留期 + 清理任务 + 清理索引(+ `tools/migrate` 幂等迁移)
- [ ] blob/JSON/CSV 列登记进 `dbcheck` 的 `bigFields`,预算按**设计期望**定
- [ ] 服务 `budgets.go` 声明表行数与 `MaxAvgRowBytes` 预算
- [ ] 写入路径三个上限齐全(单元素 / 集合条目 / 整体字节),缺哪个补哪个
- [ ] 列类型按设计期望选(别用 `LONGBLOB` 兜底,那等于 DB 层不设防)
