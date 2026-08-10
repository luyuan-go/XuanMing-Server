# 注册编号(register_no)与开服登录/注册洪峰应对

> 2026-08-09。触发:策划要求给玩家一个**自增的注册编号**(对标梦幻西游的玩家编号),
> 与既有 snowflake `player_id` 并存。讨论过程中暴露出更大的前置问题:开服首日
> 「首登即注册」洪峰打在哪里、现有防线是什么、缺口是什么。本文一并定案。
> 状态:**设计稿,待拍板**(§6 拍板清单:策划三问 + bcrypt cost + 排队立项)。
> 证据口径:全部 file:line 为 2026-08-09 HEAD,引用前建议重新 grep(行号会漂移)。

---

## 0. 结论速览

1. **注册编号方案**:`accounts` 加 `register_no` 列(注册时留空)+ 全局计数器表 +
   **异步批量补号任务**(单事务「锁计数器行 → 批量编号 → 推进计数器」,多副本天然互斥,
   无需 leader election)。严格连续无空洞、严格按注册先后;登录/注册关键路径**零改动、零新热点**。
   明确否决:同步事务内取号(单行热点,开服洪峰下是登录路径上的全局串行点)、
   TiDB `AUTO_INCREMENT`(本仓已论证否决,见 §2.4)。
2. **架构定性**:本项目是**全区全服单一逻辑世界**,不是分区分服;region/cell 是部署维度不外露
   (scale-cellular-20m.md 不变量「客户端最小视图」),账号命名空间全局一套(`uk_account`)。
   所以「注册编号」语义上等价于一个巨服的服内序号——梦幻西游靠分服号段回避的全局问题,
   我们必须正面解,但解出来的结果也更强(真·全局连续,永无合服重编号)。
3. **关键现状**:生产环境**今天不存在注册写路径**(`dev_auto_register`/`dev_skip_password`
   prod 均 false,「正式注册流程不属 login 职责」且本仓无注册服务)。注册编号方案对
   「谁写 accounts」零假设——补号任务只看表,未来正式注册流程接入时无需改造。
4. **洪峰防线现状**:有 BBR 自适应限流(prod 机械强制 14 服务)、client 熔断、KillSwitch、
   TiDB schema 打散;**没有** Envoy 边缘速率闸、**没有**登录排队/开服放量机制、
   压测**从未**打过注册/登录洪峰场景。缺口与对策见 §5。

---

## 1. 需求与对标:策划要的到底是什么

### 1.1 需求

策划希望看到玩家的**注册编号**:自增、可读、反映注册先后。与 `player_id`(snowflake,
uint64,无业务含义)并存,不替代。

### 1.2 对标拆解:梦幻西游的「自增编号」真实语义

梦幻西游是分区分服:编号空间**自带服标识**(号段偏移/前缀),「自增」只发生在服内。
合服不撞号靠的是号段隔离;代价是全局视角编号布满空洞、只在服内连续。玩家感知不到,
因为每人只看自己服的号段。

推论:
- 策划的对标其实**弱于**「全局严格连续」——梦幻从来给不出全服第 N 名。
- 我们没有「服」,给出的是真·全局连续编号,**严格优于**对标;也永无合服重编号的历史包袱。
- 若策划哪天要「每服短号」(分区开服的运营仪式感),那是先造「服」概念的产品决策,
  编号只是跟随,不在本文范围。

### 1.3 我们是不是「单服」?

不是「一台服务器」,也不是分区分服,而是**全区全服**:

| 维度 | 事实 | 证据 |
|---|---|---|
| 产品定位 | 全区全服爆款 MOBA(CCU 系数按此取上界) | scale-cellular-20m.md §1 |
| region/cell | 部署维度,不外露内部拓扑;登录后由服务端从 player_id 确定性算落点 | scale-cellular-20m.md 不变量表「客户端最小视图」、§3.2 |
| 账号命名空间 | 全局一套,唯一性完全由 `accounts.account` 列 + `uk_account` 决定,无任何按服切分维度 | conf.go 排序规则注释、02-account-tables.sql |
| 账号库 | 全服共享单点(TiDB),「全国所有玩家的登录都写同一实例」 | 03-account-tidb.sql:3 |
| login 配置里的 Region | hub_allocator 的大厅分片参数(空=选最空分片),与玩家选服无关 | login conf.go `HubAllocatorConf.Region` |

注意一个**前瞻不确定点**:cell 化方案(scale-cellular-20m.md)把 login 描述为「全局/区域薄层」,
§4.3 按 region 拆了 social TiDB/总线/matchmaker 但**未点名账号库落点**。若未来账号库按 region
切分,本方案的全局计数器需保留全局一份(或改号段方案——那时编号语义也该重新拍板)。
现状(2026-07-27 拍板)账号库是全服单点 TiDB,本方案按此设计。

---

## 2. 关键现状事实(带证据)

### 2.1 生产今天没有注册写路径

- `accounts` 的 INSERT 只存在于 dev 懒注册:`FindByAccount` 返回不存在且
  `(devAutoRegister || devSkipPassword)` 才走 `ensureAccount`(login biz/login.go)。
- prod 模板两开关均 false/未配(login-prod.yaml.example:84 `dev_skip_password: false`,
  `dev_auto_register` 无此键即默认 false);conf.go 注释明写「⚠️ 严禁上生产」。
- login README「不做的事」:**正式注册流程不属 login 职责(仅有 dev 假注册开关)**。
  全仓无其它写 `accounts.password_hash` 的服务——**正式注册流程尚不存在,待立项**。
- 但压测例外:robot/stress 每 VU 首登靠 `devAutoRegister` 自动建号(vu.go),
  即**压测环境会真实打注册路径**——这是补号任务的天然验证通道。

设计含义:注册编号**挂在表上而不是挂在某条代码路径上**。补号任务只认
「`accounts` 里 `register_no IS NULL` 的行」,无论行是 dev 懒注册、未来的正式注册服务、
还是运营导入写进来的,一视同仁。正式注册流程立项时**不需要**为编号做任何事。

### 2.2 一次登录的服务端账单(洪峰的分母)

| 项 | 性质 | 说明 |
|---|---|---|
| bcrypt ×1 | CPU | `passwd.Verify`,cost 由库中哈希内嵌值决定。**现状所有落库哈希实为 cost=4**(`ProdCost=10` 全仓零引用,压测前审核-20260724 已点名为口令强度弱点) |
| `player_session_generations` upsert | DB 写,**必经、fail-closed** | 事务内 `FOR UPDATE` + generation+1,失败拒登录;写 QPS = 全服登录 QPS,「本库写压力最高的表,恰恰是迁 TiDB 的主因」(03-account-tidb.sql) |
| Redis 会话 hash 条件写 | Redis 写 | Lua 仅更高代际可覆盖;失败(非定序输)走墓碑补偿 |
| `account_devices` TouchDevice | DB 写,异步旁路 | detached ctx,失败仅 WARN |
| 下游扇出 | RPC | 5s deadline 内**串行**扇出(压测前审核【必修-1】:任一下游变慢即大面积超时) |

### 2.3 账号库已有的 schema 级防线

- 雪花主键表(`accounts`/`player_roles`/`player_session_generations`):
  `NONCLUSTERED + SHARD_ROW_ID_BITS=4 + PRE_SPLIT_REGIONS=4` 打散时间序写热点。
- 代理主键表(`account_devices`/`account_bans`):`AUTO_RANDOM(5)`。
- 「开服首日是雪花 ID 单调递增的写入尖峰」已被点名并按上述打散(03-account-tidb.sql:6、:44)。

### 2.4 TiDB `AUTO_INCREMENT` 已被本仓否决(不重新论证)

> 「TiDB 的 AUTO_INCREMENT 按 TiDB Server 缓存分段分配,**跨节点非单调**,继续用它既有热点
> 又会让『按 id 单调』的假设失效。」——03-account-tidb.sql:14-17

即便 `AUTO_ID_CACHE=1` 集中发号,回滚/重启仍跳号(有空洞),且与 dev 单机 MySQL 行为不同构,
违背「只改存储属性,业务 SQL/Go 代码不变」约定(03-account-tidb.sql:10)。否决。

### 2.5 计数器表先例(照抄的模板)

social 库 `000002_guild_counter_tables` migration:计数列/计数表 + 从明细确定性 backfill;
guild 代码用法 =「INSERT..ON DUPLICATE 占位 → `SELECT..FOR UPDATE` 锁计数行 → 按明细校正」
(group_repo.go `reservePlayerGroupSlot`)。动机同样是 TiDB 无 gap 锁。本方案沿用同款模式。

### 2.6 洪峰防线与缺口现状

| 层 | 现状 | 证据 |
|---|---|---|
| Envoy 边缘 | **零速率闸**:`rate_limit`/`ext_authz` 明确列为不启用,无 circuit_breakers;anti-abuse-scene-entry.md 判「外挂的第一跳没有任何速率闸,完全缺失」;`local_ratelimit` 仅是该文档落地序 5 的**未实现设计** | deploy/envoy/envoy.yaml:18-21 |
| 服务层 | BBR 自适应限流 prod 机械强制 14 个客户端面服务(含 login,压测前审核门禁-A);客户端登录走 Envoy→login gRPC 链,被 BBR 覆盖。client 侧 SRE 熔断、KillSwitch 已有 | gen_cluster_config.ps1、grpcserver.go |
| 排队/放量 | **不存在**任何登录排队/开服放量机制。全仓「放量」只指金丝雀发布权重,「排队」只指匹配队列;DS 容量耗尽是终态硬失败 `ErrDSNoAvailable(5001)`(改 WAIT+retry_after 是 anti-abuse 落地序 3 待办) | 全仓 grep 查证 |
| login 业务级 | 无失败次数限制、无 IP/设备频率闸;dev 档 ensureAccount 使账号免费可无限刷 | anti-abuse-scene-entry.md §登录 |
| 压测 | 40 万 VU 设计 10 分钟线性爬坡(~650 VU/s),**刻意避开瞬时 connection storm;从未有注册/登录洪峰专项场景**;既有洪峰结论全部来自静态审计 | stress-single-cell-client.md:127 |
| 容量目标 | 2000 万 DAU 已拍板:登录峰值 ~十万级 QPS(早晚高峰+重连风暴),首日/版本尖峰再 ×1.3~1.5;注册总量 ~1 亿~2 亿 | scale-cellular-20m.md §1 |

---

## 3. 注册编号方案

### 3.1 语义先拍板(策划三问)

| 问题 | 选项 | 本文默认(待策划确认) |
|---|---|---|
| ① 要不要严格连续无空洞 | 严格连续(编号≈第 N 个注册,最大号≈总量) vs 大致递增有洞 | **严格连续**(这是「自增编号」的直觉语义;且实现代价并不更高,见 §3.2) |
| ② 给谁看 | 仅 GM/运营后台 vs 客户端玩家可见 | 待定。仅后台=后端一个查询字段;客户端可见=pb 加字段+UE 属性界面(WBP_RoleInfo)加一行 |
| ③ 起始号 | 1 vs 好看号段(如 100001) | 待定。= 计数器初始值,回填 migration 一并设置,之后不可改 |

### 3.2 选型:为什么是「异步批量补号」

| 方案 | 洪峰下行为 | 判定 |
|---|---|---|
| A. 注册事务内同步取号(`UPDATE counter SET v=v+1` 与 INSERT 同事务) | 计数器单行悲观锁排队,短事务实测量级**几百~1k TPS 到顶**;把全局串行点插进登录关键路径,开服洪峰下登录延迟整体抬升 | ❌ 否决(§16.5 容量边界) |
| B. `AUTO_INCREMENT`(含 `AUTO_ID_CACHE=1`) | 有空洞、跨节点非单调、与 dev MySQL 不同构 | ❌ 本仓已否决(§2.4) |
| C. **异步批量补号**(注册留空,后台任务批量编号) | 注册路径零改动零新热点;串行点被批量摊销(每批只碰一次计数器);积压时只是编号晚几秒出现,**优雅降级不雪崩** | ✅ 采用 |

方案 C 的吞吐上限是「批量 UPDATE 写 TiDB 的速度」(每秒数万行量级),真到 2000 万 DAU
开服体量,先到瓶颈的是 §2.2 那份每次登录都要付的账单,不是编号。

### 3.3 设计

**DDL**(mysql-init / tidb-init / tools/migrate 三处同步,遵循同构约定):

```sql
-- accounts 加列(NULL = 未编号;uk 兜底防双号,MySQL/TiDB unique 均允许多 NULL)
ALTER TABLE accounts
    ADD COLUMN register_no BIGINT UNSIGNED NULL COMMENT '注册编号(展示专用,禁作身份键/外键;NULL=待补号)',
    ADD UNIQUE KEY uk_register_no (register_no);

-- 全局计数器(单行;next_no = 下一个待发号)
CREATE TABLE IF NOT EXISTS register_no_counter (
    id       TINYINT UNSIGNED NOT NULL,
    next_no  BIGINT UNSIGNED  NOT NULL,
    PRIMARY KEY (id)
) COMMENT='注册编号全局计数器(单行 id=1;补号事务 FOR UPDATE 锁行即互斥)';
```

**补号任务**(挂 login 既有后台任务循环,与 `PurgeStaleDevices` 并列——§16.10 不新建
第二套 timer 状态机;周期建议 5s,批大小默认 500 对齐仓库 `DELETE..LIMIT 500` 惯例):

```
BEGIN;
SELECT next_no FROM register_no_counter WHERE id=1 FOR UPDATE;   -- ① 锁计数器 = 全局互斥
SELECT player_id FROM accounts WHERE register_no IS NULL
    ORDER BY created_at, player_id LIMIT 500 FOR UPDATE;          -- ② 稳定顺序取批
UPDATE accounts SET register_no = <next_no + rank> WHERE ...;     -- ③ 批内按序编号
UPDATE register_no_counter SET next_no = next_no + <批大小>;      -- ④ 推进计数器
COMMIT;                                                           -- 回滚则①-④一起消失,无空洞
```

要点:
- **多副本安全,无需 leader election**:计数器行 `FOR UPDATE` 天然串行化并发副本
  (§15.1 标准能力优先;同 guild counter 先例)。事务原子保证崩溃无空洞、无双号。
- **排序键 `created_at + player_id`**:created_at 秒级、跨 TiDB 节点毫秒级时钟差下,
  编号先后与真实提交序可能差一两位——展示编号要的是**确定性**,不是物理精确序,可接受。
- **积压语义**:洪峰下任务落后只表现为「新玩家编号晚几秒~几分钟可见」;查询侧对
  `register_no IS NULL` 显示「分配中」。
- **回填**:存量账号一次性 migration 按同一排序编号,`next_no` 初始化为
  `起始号 + 存量数`;起始号即策划三问③。
- **查询/下发**:pb 字段 `uint64 register_no`(§5.12 非负默认无符号;0=未分配,
  与「NULL=待补号」对应,天然满足 proto3 零值语义)。仅当策划三问②选「客户端可见」时
  才加进客户端可见结构并同步 UE。

**红线**:`register_no` 是**纯展示字段**。任何服务不得将其作身份键、外键、路由键或
幂等键——身份永远是 `player_id`(§9.11)。review 见到 `WHERE register_no =` 出现在
非展示查询里直接拒。

### 3.4 变体评估:独立 ID 服务 + 逐个异步申请 + 映射表(2026-08-09 用户提案)

提案形状:创建玩家时向一个 **ID 服务**异步申请编号,未返回前客户端显示「ID 生成中」,
编号与 player_id 的关系存**独立映射表**。与 §3.3 逐项对比:

| 维度 | 提案 | §3.3 方案 | 评估 |
|---|---|---|---|
| 异步 + 「生成中」占位 | ✅ | ✅(「分配中」) | **完全一致**——两个方案独立收敛到同一结论:编号必须退出创建关键路径 |
| 存储形态 | 独立映射表(`register_no` PK ↔ `player_id` uk) | `accounts` 加列 | **等价可选**。映射表优点:不动 accounts DDL、不依赖「谁写 accounts」;缺点:发现待编号行不能再用 `register_no IS NULL` 索引扫,需按 `(created_at, player_id)` 水位游标推进 + 时钟偏差安全滞后(处理 `created_at < now()-10s` 的行),多一套水位状态。加列方案的发现查询天然正确、零状态 |
| 分发机制 | 每次创建**逐个请求**ID 服务(push) | 后台任务**批量扫表**(pull) | push 有先天缺陷:INSERT 与「发申请」是无事务的双写,进程在两步之间崩溃/ID 服务不可用 → 该玩家永远没编号,**必须再配一个兜底扫描**才完整;而兜底扫描自身已是完备方案。即 push 不能替代 pull,只能叠在 pull 上换「编号秒级可见」的低延迟——展示场景不需要,按 §15.3 暂不建,留作未来优化位 |
| 部署形态 | 新增独立服务 | 挂 login 既有后台循环 | 单客户、单功能、无独立状态的「服务」不满足 §15.4 复杂度举证;且编号权威属账号域,留在 login(pandora_account 权威)不新增权威边界。**否决独立服务,保留任务形态** |

**追问:「push + 兜底扫描」组合行不行?(2026-08-09)** 行——正确性上成立(两条路径
都走同一个「锁计数器行 + 仍为 NULL 才赋号」事务即无双赋号)。不选它是性价比问题,账如下:

| | 买到 | 付出 |
|---|---|---|
| push 叠加兜底 | 稳态下编号**亚秒可见**(纯兜底为扫描周期级,~5s) | ① **顺序破坏**:push 按请求到达序赋号,兜底按 `created_at` 序补号——丢请求的玩家被补号时会拿到比"晚于他注册的玩家"更大的号。若拍板项 A① 选「严格按注册先后」,push 直接与之冲突;要 push 保序只能让它全局按 created_at 排队处理,那它就退化回兜底扫描本身。② **洪峰下自动失效**:push 逐个碰计数器行(几百/s 到顶),兜底批量摊销(几万/s)反而更快——最需要低延迟的时刻,push 落后于兜底,等价于纯兜底。③ 多一条创建侧发请求链路 + 一个处理端组件要建、要运维、要压测 |

即:push 只在「稳态 + 接受大致先来后到」的窗口里买到几秒延迟改善,而展示场景对这几秒
不敏感。结论:**吸收「异步 + 占位显示」(本就一致)与「映射表」为可选存储形态(拍板项 A
追加④列 vs 映射表,默认加列);「独立服务 + 逐个申请 + 兜底」不否定其正确性,按 §15.2/15.3
先不建,纯兜底起步**;将来真出现「编号必须秒级可见」的产品需求,或第二个编号类需求
(公会号、靓号池),再以真实需求重议叠加 push / 服务化。

### 3.5 落地清单

| # | 事项 | 落点 |
|---|---|---|
| 1 | DDL:加列 + 计数器表 | deploy/mysql-init/02、deploy/tidb-init/03(TiDB 版 `uk_register_no` 尾部热点在补号 QPS 量级下无碍,不需打散)、tools/migrate pandora_account 新迁移 |
| 2 | 回填 migration(含 next_no 初始化) | tools/migrate(参照 guild_counter backfill 写法) |
| 3 | 补号任务 | login `internal/biz`(挂既有后台循环)+ `internal/data`(上述事务) |
| 4 | 查询接口(运营/GM 侧带出 register_no) | 待策划三问②;客户端可见则 [proto] 标注同步 UE |
| 5 | dbcheck 登记 | `register_no_counter` 登记为豁免(单行,权威闸,不清理)(§9.24) |

---

## 4. 为什么「注册洪峰」=「登录洪峰」

本项目注册是**首登即注**(现状 dev 懒注册如此;未来正式注册大概率也是首登创建)。
开服那一刻每次登录就是一次注册:**注册峰值 ≈ 登录峰值,不存在「注册 QPS 很低」的前提**。
03-account-tidb.sql:6 早已点名「开服首日是写入尖峰」。这正是 §3.2 否决同步取号、
以及 §5 必须把洪峰防线补齐的原因。方案 C 对此的回答:编号完全退出关键路径,
洪峰整形(§5.2)之后剩下的账单,与编号无关。

---

## 5. 开服登录/注册洪峰:分层对策

### 5.0 账单的硬底在哪

十万级登录 QPS 下(scale-cellular-20m.md §1):
- **bcrypt CPU**:cost=4 单次 ~1ms 量级 → 十万 QPS ≈ 百核级,可扛;
  **若按安全要求升到 cost=10(≈×64)→ 单次 ~60ms → 需数千核**,bcrypt 立刻变成登录
  路径最大 CPU 项。数字为估算口径,待实测复核(§16.10.③)。
- **`player_session_generations` upsert**:与登录 QPS 1:1 的 fail-closed 事务写。
  行按 player 打散无单行热点,天花板 = TiKV 集群写容量,靠加节点水平扩——但必须有
  容量规划数字,目前没有实测(§5.4)。

结论:**bcrypt cost 决策(拍板项 B)与登录排队(拍板项 C)必须一起定**——cost 升档
把 CPU 账单放大 64 倍,没有排队整形就是拿开服赌估算。

### 5.1 第 0 层:Envoy 边缘速率闸

anti-abuse-scene-entry.md §4.1 已有完整设计(`local_ratelimit` 对未鉴权 path 按 IP
令牌桶,验收口径「登录洪峰下 429、后端 QPS 削平」),是该文档落地序 5 的待办。
**本文不重造,只把它的优先级从「防外挂」提为「开服生存必需」**。它是唯一能在
bcrypt/DB 之前挡住流量的层。

### 5.2 第 1 层:登录排队/开服放量(待立项)

现状不存在(§2.6)。设计要点(立项时展开成独立设计文档):
- **语义对齐既有契约**:排队响应复用 §9.23 统一入口的 `WAIT`(含 `retry_after`)状态,
  与 anti-abuse 落地序 3(DS 容量 WAIT 化)同一形状;客户端排队 UI 按 §9.19/20 有界驱动
  (watchdog 到期重查,不新建状态机)。
- **准入口径**:全局令牌桶,容量数字来自下游最薄弱环节的实测(bcrypt 核数、session upsert
  TPS、hub 分配速率),不拍脑袋。
- **开服放量** = 准入闸初始值从小往上调的运营操作,不是新机制。
- **前置条件**:先跑 §5.4 的洪峰压测拿到真实天花板,再定排队系统规模(§15.3:
  不为想象的数字提前建设;2000 万 DAU 目标已拍板,立项本身有据)。

### 5.3 第 2 层:服务与依赖

- BBR 已 prod 机械强制(门禁-A),保底「过载丢请求不雪崩」——但 BBR 是**尾部防线**,
  丢的是已到达服务的请求,体验上等于登录失败重试风暴,不能替代 §5.1/5.2。
- 【必修-1】登录 5s deadline 内串行扇出:洪峰下任一下游变慢即大面积超时,
  该项在压测前审核已立案,此处只标注它是洪峰放大器,修复归原审核清单。
- bcrypt cost:拍板项 B。若维持 cost=4 需在文档明示接受口令强度弱点;若升 10,
  CPU 账单进 §5.2 的容量口径。存量 cost=4 哈希可在登录成功时懒升级(verify 通过后
  重 hash 落库),不停服(§9.16)。

### 5.4 第 3 层:存储与压测验证

- schema 打散已就绪(§2.3);TiDB 容量靠加 TiKV 节点,但**没有登录洪峰下的实测写容量数字**。
- 压测缺口:新增**注册/登录洪峰专项场景**(瞬时 connection storm + 全新账号建号,
  robot/stress 现有 devAutoRegister 链路即真实注册路径),产出:①bcrypt 两档 cost 的
  CPU 曲线;②session upsert TPS 天花板;③补号任务积压/追平曲线(顺带验证 §3.3)。
  按 §8 压测纪律执行(prev-summary、dbcheck 基线/对比)。

---

## 6. 拍板清单

| # | 待拍板 | 责任方 | 依赖 |
|---|---|---|---|
| A | 策划三问:①严格连续(建议是)②给谁看 ③起始号;追加④存储形态:accounts 加列(默认)vs 独立映射表(§3.4) | 策划/用户 | 无——定了 §3 即可动工 |
| B | bcrypt cost:维持 4(明示接受弱点)或升 10(账单进容量口径,存量懒升级) | 用户 | 与 C 联动 |
| C | 登录排队/放量立项 + Envoy local_ratelimit 优先级提级 | 用户 | §5.4 洪峰压测数据 |
| D | 洪峰压测专项排期 | 用户 | robot/stress 现有能力即可开跑 |

## 7. 关联

- `deploy/tidb-init/03-account-tidb.sql` —— 账号库单点定性、AUTO_INCREMENT 否决、打散手段
- `docs/design/anti-abuse-scene-entry.md` —— §4.1 Envoy local_ratelimit 设计、落地序 3/5
- `docs/design/scale-cellular-20m.md` —— 容量基线、region/cell 语义、login 全局薄层
- `docs/reviews/压测前审核-20260724.md` —— 必修-1(串行扇出)、门禁-A(BBR)、bcrypt cost 弱点
- `tools/migrate/migrations/pandora_social/000002_guild_counter_tables.up.sql` —— 计数器表先例
- `docs/design/stress-single-cell-client.md` —— 现有压测 ramp 口径(刻意平滑,无洪峰场景)
