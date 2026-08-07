# 反外挂滥用:进出副本刷量与各功能面被刷的防护设计

> 2026-08-06 立档(Claude)。**当前状态:现状盘点 + 设计稿,除文中明确标注「已实现」的条目外,一行代码未动。**
> 实现前不得声称本项目「已防刷」。
>
> **核心结论先说(详见 §3.2.1):最高危不是「刷得快」,是「一次进场能把一台 14Gi 的
Battle DS 押约 6 分钟」。配合免费自动注册,攻击者只需 `maxReplicas` 个小号、每 6 分钟各点一次,
> 就能让 Fleet 永久满载、正常玩家进不去。这个速率比正常玩家还慢,任何频率限流都拦不住。
> Hub DS 按人数自动扩展,不在本文射程(2026-08-06 用户圈定)。**
>
> 关联:`CLAUDE.md` §9.18(客户端可写累积列表上限)、§9.19/§9.20(不卡玩家)、§9.22(唯一权威)、
> §9.23(单一幂等进场链)、§9.24(数据增长有界)、§17(单一进场接口);
> `docs/design/agones-dev.md`(DS 生命周期、空场回收、待实现的 DS 侧计时器);
> `docs/incidents/2026-07-27-p0-battle-ds-artic01-memcg-oom.md`(14Gi 内存定档的实测来源)、
> `docs/incidents/2026-07-27-p0-ds-allocator-warming-coldload-reclaim.md`(冷启动耗时实测、单阈值教训);
> `docs/ops/service-killswitch.md`(BBR 限流 / 熔断 / Kill-Switch);
> `docs/reviews/压测前审核-20260724.md`(必修-4 队列准入、必修-5 世界频道冷却);
> `docs/design/player-name-validation.md`(昵称层,同为设计稿)。

---

## 1. 威胁模型:外挂能做什么

外挂 = **持有合法 session、按协议发包、但把频率和顺序拉到人类做不到的程度**的客户端。它不需要
破解签名,只需要把正常 RPC 打 100 倍。因此本文的防护对象**不是伪造**(那由 §9.6 派生数值服务端算、
DS 票据不可伪造通道、`internalrpcauth` 负责),而是**合法请求的滥用速率与滥用组合**。

四类危害,按后果排序:

| 类别 | 外挂动作 | 后果 | 为什么普通鉴权拦不住 |
|---|---|---|---|
| **A. 资源放大** | 高频 StartMatch / 进出副本 / 切线 | 每次请求在后端拉起一台 GameServer Pod、占座、写 Redis/MySQL。放大比 1:N,N 是基础设施成本。**最危的子形态是 §3.2.1 的 Battle DS 占位耗尽** | 每一次单独看都是合法请求;且占位类攻击的**速率比正常玩家还低**,频率闸天然失效 |
| **B. 扇出放大** | 世界喊话、群发好友申请、批量邀请 | 一条请求 × 全服在线数的推送成本 | 同上 |
| **C. 状态搅动** | 反复进出场景、反复取消匹配、反复切线 | 制造 owner 迁移风暴,把 §9.22 的 epoch/lease 状态机推到边界;可能诱发脑裂等待、卡玩家 | 每次迁移本身是被支持的操作 |
| **D. 存储膨胀** | 高频产生只增行(流水、申请、消息) | 库按 §9.24 的增长曲线被拉爆,清理任务追不上 | 保留期清理管的是「多久删」,不管「多快写」 |

**A 与 C 正是用户问的「恶意外挂进出退出副本、创建副本」**,危害最大:它同时消耗 k8s 容量、
DS 进程、Redis 写、MySQL 写,而且直接压在 §9.19/§9.20「不卡玩家」的关键路径上——被刷爆的
allocator 会让**正常玩家进不去场景**。

---

## 2. 一条铁律:限流是背压,不是权威门

这是本文最容易写错的地方,先钉死。

| | **权威门**(不变量) | **限流 / 冷却**(背压) |
|---|---|---|
| 例子 | `ensureNoneInBattle`、owner lease fencing、Admission CAS、扣减原子性 | 世界频道冷却、切线冷却、队列准入 |
| 依赖故障时 | **fail-closed**(§9.22:查询失败返回 UNKNOWN/UNAVAILABLE,禁止冒充 OFFLINE) | **fail-open**(牺牲限流保可用,Warn 留证) |
| 被绕过的后果 | 数据错误 / 双 DS / 超卖,不可逆 | 成本上升 / 体验下降,可观测可追补 |
| 谁保证正确性 | 它自己 | 不保证,正确性另有权威门兜底 |

**推论**:
1. 限流器故障 **绝不能** 阻断玩家进场(否则限流本身变成 §9.20 的卡玩家源头)。已实现的
   `sendWorld` 与 `AllowWorld` 已按此写死(`services/social/chat/internal/biz/chat.go:396-403`,
   判定 error 时只 Warn 不拒)。新增限流点必须照抄这个语义。
2. 反过来,**已有的权威门不能被降级成「限流就够了」**。`ensureNoneInBattle` 不是防刷措施,
   它是 §9.1 一人一 DS 的门,永远 fail-closed。

---

## 3. 现状盘点(逐面,含证据)

图例:✅ 已实现 / ⚠️ 部分 / ❌ 缺失。

### 3.1 全局层

| 机制 | 状态 | 位置 | 边界 |
|---|---|---|---|
| gRPC server 侧 BBR 自适应限流 | ✅ 生产强制开 | `pkg/middleware/ratelimit.go`;`tools/scripts/gen_cluster_config.ps1:1478` `$GrpcRateLimitServiceNames`(= unary session-gate 12 服务 + login + push),`-Prod` 机械置 `enable_rate_limit: true`,契约测试 `tools/scripts/tests/gen_cluster_prod_ratelimit_contract_test.ps1` | **按 CPU/inflight/RT 自适应丢负载,不区分玩家**。它保护的是服务不倒,不是「某个外挂不能刷」——单个外挂在服务整体不过载时完全不触发 BBR |
| Kill-Switch(RPC 级临时关停) | ✅ | `pkg/killswitch`,`docs/ops/service-killswitch.md` | 运营手动止血,粒度是 RPC 不是玩家 |
| 会话现行性门 | ✅ | `pkg/sessiongate`,12 个 unary 服务 `session_gate.require: true` | 防顶号后旧会话继续写,不防频率 |
| **Envoy 边缘按连接/按 IP 限流** | ❌ **完全缺失** | `deploy/envoy/envoy.yaml:19` 只有一行注释提到 `rate_limit` filter,无任何实际 filter | 外挂的第一跳没有任何速率闸 |
| **per-player 通用令牌桶 / 滑动窗口库** | ❌ 无通用件 | 现有只有各自散写的 `SETNX` 冷却(见下) | 每个新限流点都要重写一遍 |

### 3.2 进入场景 / 副本(A 类,重点)

`MatchService.StartMatch` 是 §17 规定的**唯一**进场接口,所有副本共用它,因此这里是外挂的主战场。

| 检查 | 状态 | 位置 |
|---|---|---|
| `map_id` 关卡表白名单(非战斗类关卡拒) | ✅ | `services/matchmaking/matchmaker/internal/biz/match.go:232` `validateMapID`,错误码 `ErrMatchInvalidMap(4008)` |
| `team_size` 按表钳制 | ✅ | 同文件 `teamForMap` 系列,钳到 `[1, configtable.MaxLevelTeamSize]` |
| 战斗中不得再入队(§9.1 门) | ✅ fail-closed | `match.go:481` `ensureNoneInBattle`,locator 权威查询,错误码 `ErrMatchInBattle(4007)` |
| 一人一票 claim(去重) | ✅ | `preflightStartClaims` 做友好预检,真正线性化点在 durable worker 的 SETNX |
| durable saga(避免重复提交产生第二次分配) | ✅ | `CreateStartOperation` + `operation_id`,符合 §9.23 |
| **全局队列准入上限**(背压) | ✅ | `match.go:542-550`,`MaxQueueTickets`,超限 `ErrRateLimited(9)`,`queue_admission_test.go` 锁定。**注意这是全局队列长度闸,不是 per-player** |
| **per-player / per-team StartMatch 频率闸** | ❌ **缺失** | 无任何冷却。外挂可以在自己不在战斗、队列没满时,以 RPC 极限速率反复 StartMatch → CancelMatch |
| **成局级冷却 / 换 match_id** | ❌ 未落地 | `docs/design/decision-revisit-allocating-bounded-terminal.md:48` 明确记着:solo 路径 `matchID := ticket.TicketId`、`RequeueTicket` 不换 ticket_id、`CreateMatch` 是无 NX 的 SET ⇒ 同一 match_id 每 2s 重成局,会撞既有 uncertain/abandoned claim;文中写明「要做必须配套换新 match_id + 成局级冷却」 |
| **中途退出 / abandon 的代价** | ❌ 无冷却无惩罚 | abandoned 只走 15s 心跳超时 → 段位回滚补偿(`docs/design/agones-dev.md`),对**重复 abandon 的玩家**没有任何进入侧限制 |

**结论:进出副本这条链上,权威门齐全(不会造成数据错误或双 DS),但成本闸完全没有。**
外挂刷进出的直接后果是 GameServer 分配风暴,而不是数据损坏——这是好消息(不变量守住了),
也是坏消息(A 类危害完全敞开)。**具体有多敞开见下一节,那是本文最重要的一节。**

### 3.2.1 【最高危】Battle DS 占位耗尽:进→退→再进,把 Fleet 押死

这是本文的核心威胁,单独立节。**它不是「刷得快」的问题,是「一次进入能把一台 14Gi Pod 押多久」的问题**——
即使把 StartMatch 限到每分钟一次,只要持有时间够长、账号够便宜,Fleet 照样被押死。

#### 攻击形状

```
StartMatch(单人 PVE) → 分配一台 Battle GameServer(14Gi Pod)
  → 冷启动加载地图 22~58s          ← Pod 已被 Allocated,占着
  → 玩家「从未连入」或「连上就强退」  ← player_count 恒为 0
  → 空场计时 5min 后才判 abandoned  ← Pod 继续占着
  → 回收
换个号(自动注册,免费),重复
```

#### 成本账(这是要害)

| 项 | 值 | 证据 |
|---|---|---|
| 单台 Battle DS 内存 | `requests = limits = 14Gi` | `deploy/k8s/agones/20-fleet-battle.yaml:199,211`(由 INC-20260727-002 实测 `memory.peak≈10.43GiB` × 1.34 定档) |
| 同时最大局数 | 本地 `maxReplicas: 2`;线上由 `start.ps1 -BattleMaxReplicas` 按节点池容量覆写 | `deploy/k8s/agones/25-fleetautoscaler-battle.yaml:61` |
| 冷启动占用(未就绪也占 Pod) | 22s(页缓存热)/ 48~58s / >120s(宿主高载) | INC-20260727-001 §A3 实测 |
| **空场持有时间** | **5 分钟** | `empty_battle_timeout` 默认值,`conf.go:498`;判定在心跳 CAS 内,`battle_auth.go:910` |
| DS 侧 2~3min 自结算(**主**路径) | **UE 仍未实现** | `docs/design/agones-dev.md:465` 明写「待实现」——**当前 5min 后端兜底是唯一回收手段** |
| **单次 StartMatch 的放大比** | **1 次 RPC → 14Gi × 约 6 分钟** | 冷启动 ~30-60s + 空场 5min |

**押死整个 Fleet 所需成本**:`maxReplicas` 台 × 每 6 分钟点一次。线上若 `maxReplicas=20`,
**20 个小号、每号 6 分钟一次 StartMatch** 即可让 Fleet 永久满载。这个速率**连限流都触发不了**
(比正常玩家还慢),BBR 更不会响应(服务本身一点都不忙)。正常玩家此时拿到 `ErrDSNoAvailable(5001)`——
**进不去游戏**。

#### 为什么现有的门一个都拦不住

| 现有门 | 为什么拦不住 |
|---|---|
| `ensureNoneInBattle`(locator BATTLE) | **它确实拦住了单账号并发占多台**:`refreshBattleLocations` 刷的是 `b.PlayerIds` 即 **roster 全员**(`allocator.go:2382`),所以从未连入的玩家也被标 BATTLE,5min 内无法再开。**但这恰恰把乘数从「频率」换成了「账号数」** |
| 自动注册 `ensureAccount`(`login.go:1034`) | 账号不存在即建号 ⇒ **账号免费** ⇒ 上面那个乘数无成本 |
| `MaxQueueTickets` 队列准入 | 管的是**排队中**的票据数,不是**已分配**的 DS 数。DS 被押死时队列是空的 |
| BBR 自适应限流 | 按 CPU/inflight/RT 判过载。攻击速率极低,服务毫不繁忙,永不触发 |
| `battle_ttl` 2h | 是 Redis 镜像 TTL 上界,不是 Pod 回收时钟 |
| FleetAutoscaler | `maxReplicas` 是**硬上限**(注释原文:「防 bug/被刷时无限烧钱的安全护栏」)。它防的是烧钱,不防被押死 |

#### 一个必须同时看见的副作用

空场 5min 期间,**该玩家自己也被 locator BATTLE 锁住**,无法开新的匹配。对**正常玩家**而言这是
§9.20 的卡玩家:强退一局后要等最多 5 分钟才能再来一局,期间界面只会告诉他 `ErrMatchInBattle(4007)`
「正在战斗中」——而他明明已经退出了。**所以缩短空场回收既是防刷,也是修卡玩家,两件事同一个修。**

#### 最便宜的攻击面是单人 PVE

`teamSizeForMap` 允许 `team_size=1`(`pve_coop` 分池),因此 **1 个账号 = 1 台独占 DS**。
5v5 反而更贵(5 个账号才押 1 台)。任何新增的单人副本都会自动继承这个攻击面——
这是 §17「单一进场接口」的必然结果,不是缺陷,但意味着**防护也必须做在那唯一的入口上**。

### 3.3 Hub 切线 / 传送

| 检查 | 状态 | 位置 |
|---|---|---|
| per-player 切线冷却 | ✅ **本仓唯一做对的进场侧防刷范例** | `services/battle/hub_allocator/internal/biz/hub.go:968-984`;`data/hub_repo.go:1005` `TryTransferCooldown` = `SET pandora:hub:transfer:cd:{player_id} 1 NX EX`;配置 `hub.transfer_cooldown` 默认 `10s`(`≤0` 不限流);错误码 `ErrHubTransferCooldown(5104)` |
| 冷却先占坑、失败即释放 | ✅ | `hub.go:977-982`:`transferToLineInner` 出错时 `ClearTransferCooldown`,让玩家能立刻重试(符合 §9.20 不卡玩家) |
| 战斗/匹配中禁止切线 | ✅ fail-closed | locator 查询失败一律拒(`ErrHubTransferNotInHub` / `ErrUnavailable`),见 `docs/incidents/2026-07-22-p0-hub-allocator-locator-fail-open.md` |
| 临界区内会话终检 | ✅ | `hub.go:990` `requireCallerSessionCurrent` |

**这套「先占冷却 → 干活 → 失败释放」的形状就是 §4 要推广到 StartMatch 的模板。**

### 3.4 其它功能面

| 功能 | 频率闸 | 总量闸 | 位置 / 备注 |
|---|---|---|---|
| 世界聊天 | ✅ `chat.world_cooldown`,`SETNX`,fail-open | ❌ | `chat/internal/data/world_ratelimit.go`(压测审核必修-5,2026-07-26) |
| 私聊 / 队伍 / 公会 / 群聊 | ❌ | ❌(只有读侧 SQL LIMIT) | `chat/internal/biz/chat.go:161-167` 各 channel 分支,只校验长度。私聊虽无广播扇出,但**是 D 类存储膨胀入口**(`chat_private_messages` 是登记在册的只增表) |
| 好友申请 / 黑名单 / 好友 | ❌ 无频率 | ✅ 各 200 | §9.18 清单;`ErrFriendRequestLimit` 等 |
| 公会申请 / 成员 | ❌ 无频率 | ✅ 200 / 100 | §9.18 清单 |
| 组队邀请 / 入队申请 | ❌ 无频率 | ✅ 邀请 / 申请上限 10,Lua 原子占位 | §9.18 清单,`ErrTeamApplyPendingLimit(3009)` |
| 交易 / 拍卖下单撤单 | ❌ 无频率 | ✅ 各 200 | §9.18 清单。**下单-撤单-下单循环不受任何限制**,是 D 类高危面(每轮产生托管写 + 流水行) |
| 邮件 / 奖励领取 | ❌ 无频率 | ✅ 幂等(位图/乐观锁) | `pkg/rewardclaim`;重复领取不会重复发放,但重复请求仍打 DB |
| 排行榜查询 | ❌ | — | 读放大面,无闸 |
| 改名 | ❌ 无 CD | — | `docs/design/player-name-validation.md` §9 已把「改名 CD + 审计」列为待实现 |
| 登录 | ❌ **无失败次数限制、无 IP/设备频率闸** | ⚠️ `ErrLoginTooManyDevices(1005)` 有码 | `services/account/login/internal/biz/login.go:345` `Login`;`ensureAccount` 是**自动注册**(账号不存在即建号,`login.go:1034`)⇒ 外挂可无限刷账号 |
| push 长连 | ⚠️ 顶号语义去重 | ⚠️ 离线缓存 512 帧上限 | `services/runtime/push/internal/biz/connection.go` |
| DS 回调 | ✅ 强鉴权(签名 + Model B 授权记录) | ✅ JTI cache 有界 fail-closed | `docs/design/decision-revisit-ds-callback-auth.md`、`agones-dev.md §5` |

---

## 4. 设计:分层防护

按 §15「简单标准直达」,**不引入新中间件、不自研规则引擎、不做行为分析**。只做四层,每层用
已在本仓验证过的最简机制。

### 4.1 第 0 层:边缘(Envoy)——按连接/IP 的粗闸

**做什么**:在 `deploy/envoy/envoy.yaml` 接入 Envoy 原生
`envoy.filters.http.local_ratelimit`(token bucket,进程内,零外部依赖),对**未鉴权**路径
(登录、注册)按 downstream remote address 限速。

**为什么只在边缘做粗闸**:鉴权后的按玩家限流放在业务层更准(Envoy 拿不到 player_id 的语义,
且多 Envoy 副本的本地桶会被摊薄)。边缘只负责**挡住连 session 都没有的洪水**。

**为什么不上 Envoy 全局 RLS(ratelimit service)**:那是一个额外的有状态服务 + Redis 依赖,
按 §15.3 属于「出现真实需求前不引入」。local_ratelimit 单进程桶足够挡未鉴权洪水,精确按玩家的
配额由 4.2 用已有 Redis 做。

**验收**:压测断言登录洪峰下 Envoy 返回 429 且后端 login QPS 被削平。

### 4.2 第 1 层:per-player 冷却 / 配额(核心)

**先建通用件,再接点**。当前 `TryTransferCooldown` 与 `AllowWorld` 是两份几乎相同的
`SETNX` 代码,再加第三个点之前应抽成一份:

```
pkg/redisx/ratelimit.go
  Cooldown(ctx, key, window) (bool, error)   // SET key 1 NX PX window —— 单命令原子,窗口内只放一次
  Quota(ctx, key, limit, window) (bool, error) // Lua: INCR + 首次 PEXPIRE —— 窗口内放 N 次
```

两个原语覆盖全部需求,**不做滑动窗口、不做令牌桶**:固定窗口的边界双倍问题对「防外挂刷量」
无实质影响(2 倍仍然离外挂想要的 100 倍很远),而滑动窗口要存有序集合、要清理,是 §15.3 的
预设性复杂化。契约必须写死三条:①key 自带 PX 过期,**无后台清理任务**,内存有界 = 窗口内活跃玩家数;
②判定 error 一律 fail-open + Warn(§2 铁律);③返回 `ErrRateLimited(9)`,客户端按可重试处理
(`errcode.go:317` 已把它归入可重试类)。

**接入点与建议初值**(初值必须在压测/灰度后按实际数据复核,不是拍脑袋定死):

| 接入点 | 原语 | 建议 | 理由 |
|---|---|---|---|
| `StartMatch`(per 队长 + per 队伍) | Cooldown | 3~5s | 人类点「开始匹配」的物理下限;取消后立即重开是合法行为,所以取小值只削掉机器速率 |
| `StartMatch`(per 玩家) | Quota | 每小时 N 次成局 | 拦「刷一整天进出」的长程滥用;N 由关卡表 `Category` 分档 |
| `CancelMatch` | 不加闸 | — | **故意不加**:取消是玩家的退出路径,加闸等于 §9.20 卡玩家。刷取消的成本由 StartMatch 侧的闸吸收 |
| Hub 切线 | Cooldown | 已实现 10s | — |
| 私聊 / 队伍 / 公会 / 群聊 | Cooldown | 0.5~1s | 比世界频道松;主要挡 D 类存储膨胀 |
| 好友/公会/入队申请、邀请 | Quota | 每分钟 N | 总量闸已有,补频率闸挡「加满 200 → 全撤 → 再加满」的循环 |
| 交易/拍卖下单撤单 | Quota | 每分钟 N | 同上,循环下撤是 D 类高危 |
| 改名 | Cooldown | 天级 | 与 `player-name-validation.md` §9 的改名 CD 合并实现,不做两套 |
| 登录失败 | Quota(按账号 + 按 IP) | 失败 N 次锁 M 分钟 | **只对失败计数**,成功登录不受影响,避免误伤正常重连 |

**统一 key 规范**(沿用现有 `pandora:` 前缀,登记进 `docs/design/infra.md`):
`pandora:rl:<域>:<动作>:<主体id>`,例 `pandora:rl:match:start:1234567`。

### 4.3 第 2 层:Battle DS 占位闸(对应 §3.2.1,本文最高优先级)

限流只压频率,**压不住持有时间**。§3.2.1 的攻击速率比正常玩家还低,任何频率闸都拦不住它。
真正的杠杆只有三个,按收益排序:

#### ① 缩短空场持有时间——把「从未连入」和「全员掉线」分开(最高杠杆,且顺带修卡玩家)

> **状态:代码已实现(2026-08-07),未编译验证 / 未实测调参**。落点见本节末「实现落点」。

现在两种情况共用一个 5min 阈值,而它们的安全下界完全不同:

| 情况 | 安全下界由什么决定 | 建议阈值 |
|---|---|---|
| **从未连入**(no-show) | **只需覆盖「DS 已 ready 之后」的 travel + 连接 + Admission 最坏耗时** | **60~90s**(待实测复核) |
| **有人连过后全员掉线** | 必须 > 断线重连窗(~30s),让人能回来 | 2~3min(即 UE 侧待实现的那个计时器) |

> **关键事实(读 CAS 代码核准,别搞错)**:空场计时**只在 `state ∈ {ready, running}` 时才推进**
> (`heartbeatLegacy` 的 `if b.State == stateReady || b.State == stateRunning` 守卫)。
> 也就是说 **`warming` 期的冷启动 22~58s(高载 >120s)根本不计入空场窗口**,
> 那段由 `heartbeat_timeout` 那条独立时钟管(正是 INC-20260727-001 的战场)。
> 因此 no-show 阈值**不需要**给冷启动留余量 —— 它只需覆盖「DS 报了 ready 之后,客户端
> travel 过来并完成 Admission」这一段。这也是为什么 5min 对 no-show 而言过于宽松。
> 但**总持有时间仍是 冷启动 + 空场超时**(两段串行),所以 §3.2.1 的「约 6 分钟」放大比不变。

**为什么这条最值**:no-show 是攻击者的**唯一**姿势(他不会连进去),而 5min→~150s 直接把
放大比砍掉一半;同时它**不影响任何真实断线玩家**(那条走另一个阈值)。

**实现极廉价**:`BattleStorageRecord` 已有 `empty_since_ms`;只需再加一个
`ever_had_players`(bool,新字段编号,§9.17 加字段兼容),在心跳里 `playerCount>0` 时置位。
判定处 `battle_auth.go` / `allocator.go` 的 `case` 按该位选阈值即可,**不新增任何计时器或状态机**
(§16.10:这是「到期后重查权威并回收」的有界兜底,不是掩盖时序)。

##### 实现落点(2026-08-07)

| 文件 | 改动 |
|---|---|
| `proto/pandora/ds/v1/allocator.proto` | `BattleStorageRecord.ever_had_players = 21`(加字段,§9.17 双向兼容) |
| `internal/conf/conf.go` | 新增 `no_show_battle_timeout`;`DefaultNoShowBattleTimeout=150s`、`NoShowTimeoutFloor=60s`;`ResolveNoShowTimeout()` 统一解析 |
| `internal/biz/allocator.go` | `heartbeatLegacy` 按位选阈值;判弃日志分 `reason=no_show` / `all_disconnected` |
| `internal/data/battle_auth.go` | `ActivateHeartbeat`(Model B 生产路径)同款双阈值;`BattleHeartbeatInput.NoShowTimeout` |

**默认值 150s 的推导**:DSTicket v2 生产档 TTL 120s(§9.3,`pkg/auth/dsticket.go` 签发与验签双向
强制)+ 30s 余量。取这个数的理由是**可证明**:票据过期后该客户端已经**不可能**再进入这一局
(DS 侧验签必拒),继续押着 Pod 是纯浪费。这比拍一个「感觉够用」的数字站得住脚,也天然
满足「不误杀正在 travel 的正常玩家」——他们手里的票还没过期。

**两个 fail-safe 方向**(实现时踩过,写下来免得后人重犯):

1. **解析结果绝不允许是 0**。判定处是 `timeout > 0 && ...`,阈值为 0 会让分支永假 ⇒
   no-show 局**永不回收**,比不改还糟。`ResolveNoShowTimeout()` 在 `empty` 启用时保证返回正数,
   并有 `TestResolveNoShowTimeoutNeverSilentlyZero` 锁住。
2. **配小于 60s 被钳到 60s**。手滑把 `150s` 写成 `1.5s` 不能变成「玩家进不去场景」(§9.20 红线)。
   宁可回收得晚,不可误杀。

**回滚开关**:`no_show_battle_timeout: -1s` = 显式禁用差异化,退回改动前的单阈值行为。

> ⚠️ **仍待实测**:no-show 档最终取值必须实测「DS 报 ready → 客户端完成 Admission」的 P99 复核。
> 150s 是**有推导依据的保守初值**,不是实测值。

#### ② 让「占一台 DS」这件事有成本——账号不能免费

①把持有时间压到 ~2min 后,攻击者只需把账号数翻 3 倍就恢复原效果。**只要账号免费,任何
per-player 配额都可以用换号绕过**。因此必须把分配配额绑到比账号更稀缺的东西上:

- **生产环境关闭自动注册**(`ensureAccount` 的建号分支),或至少要求注册需通过一次外部验证;
- 配额维度改为 **账号 + 设备 + IP** 三者取最严,而不只按 `player_id`;
- 「首次进入战斗」附加轻量门槛(如需先完成新手/达到某等级)。等级/进度是攻击者刷不动的成本。

**这条属产品与运营决策,必须人拍板**(§7)。技术上我只能指出:不解决账号成本,①和③都是延缓而非解决。

#### ③ Fleet 逼近上限时的准入与降级(守住「正常玩家进得去」)

前两条降低被押死的概率,这条保证**即使被押死,正常玩家也不是硬失败**:

- **容量分级准入**:`CapacityWatcher` 已在采 `pandora_ds_allocator_fleet_usage_ratio`
  (`biz/capacity.go`,告警阈值 0.8)。在 usage 超阈值时**收紧**分配条件——优先服务
  有游戏进度/无近期 no-show 记录的玩家,而不是先到先得。
- **no-show 记账 → 指数退避**:玩家造成一次 empty-abandon,下次 StartMatch 前置一个
  递增冷却(带 `retry_after`)。这是对 §3.2.1 的**直接**反制:它精确惩罚「占了不玩」这个行为本身,
  且对正常玩家几乎零误伤(正常结算走 `ended`,不计数)。
  **必须是明确错误码 + 可见倒计时**,不能静默卡住(§9.20)。
- **容量耗尽时给排队而非硬拒**:`ErrDSNoAvailable(5001)` 目前是终态失败。应改为带
  `retry_after` 的 `WAIT`,由 §9.23 的同一恢复协调器驱动重试——玩家看到「排队中」而不是「进不去」。

#### ④ 在途成本闸(原「成本闸」条目,并入本节)

冷却按时间限速,但**没有限制在途成本**。一个外挂用 5s 冷却仍能持续占用分配管线:

- **per-player 在途分配唯一**:同一玩家同一时刻只能有一个未终结的进场 operation。这**不是新机制**
  ——`CreateStartOperation` 的 `operation_id` + claim SETNX 已经保证了,只需确认「取消后到新 operation
  可创建之间」不存在空窗被利用,并把它写成显式契约与测试。
- **成局级冷却 + 换 match_id**:落 `decision-revisit-allocating-bounded-terminal.md:48` 记下的前置条件。
  这条同时是**正确性修复**(避免同 match_id 撞 abandoned claim),不只是防刷。

#### 不做的事

- ❌ **不要靠调大 `maxReplicas` 解决**。那是把「被押死」换成「被烧钱」,而且节点池总有上限。
- ❌ **不要缩短 `heartbeat_timeout` 来加速回收**。INC-20260727-001 的根因正是单阈值同时监管
  启动与稳态,缩它会重新击穿冷加载中的 warming DS。空场回收和失联回收是两条独立时钟,不要合并。
- ❌ **不要在 DS 侧自行判断「该不该回收自己」之外的事**。回收决策的权威在 allocator。

### 4.4 第 3 层:可观测与止血

限流只削峰,识别与止血靠这层。**不新建监控系统**,复用现有 Prometheus + Kill-Switch:

- 每个限流点复用 `pandora_rpc_total{code}` 观察 `RATELIMIT` 占比(注意:**player_id 绝不能做 label**,§12)。
- 新增一个低基数计数器 `pandora_ratelimit_rejected_total{domain,action}`,只到动作级。
- 需要定位到具体玩家时走**日志**不走 metrics:拒绝时打 WARN 带 `player_id`,由 Loki 侧聚合。
- 单个功能被刷穿时,运营用已有 `pkg/killswitch` 关停该 RPC 止血(`docs/ops/service-killswitch.md`)。

---

## 5. 明确不做的事(避免后来者重提)

| 提案 | 为什么拒 |
|---|---|
| 行为分析 / 机器学习检测外挂 | §15.3 预设性复杂化。在没有任何速率闸的当下,先做闸;闸做完还有滥用再谈检测 |
| Envoy 全局 RLS(独立限流服务 + Redis) | 额外有状态组件。per-player 精确配额用业务层 Redis 已足够,边缘只需 local_ratelimit 粗闸 |
| 滑动窗口 / 精确令牌桶 | 固定窗口边界双倍对本威胁模型无实质差异,却要引入有序集合与清理。§15.2 |
| 客户端做频率限制 | 客户端在外挂手里。客户端可以做按钮灰化(体验),但那是展示,**不得替代服务端判定**(§17.3) |
| 用限流器做正确性门 | §2 铁律。限流 fail-open,拿它当门等于故障时门自动敞开 |
| 给 CancelMatch / 退出路径加闸 | §9.20 红线:退出与放弃必须永远可用 |
| 把封号做成自动触发 | 误伤不可逆。限流拒绝 + 日志留证 + 人工复核,自动化只到「拒绝」为止 |

---

## 6. 落地顺序与验收

按「危害 × 改动成本」排序,**每一项独立可上线,不打包**:

| 序 | 项 | 涉及 | 验收 |
|---|---|---|---|
| **0** | **no-show / 全员掉线双阈值空场回收**(§4.3①) | ds_allocator + proto | ✅ **代码已实现(2026-08-07)**,单测已锁定 no-show 走短阈值 / 有人连过走长阈值 / EverHadPlayers 粘滞 / 禁用差异化后仍会回收 / 解析结果绝不为 0。**未完成**:未编译验证(需先跑 proto 生成)、未做故障注入验证真实断线玩家不被误判、未给出「单次进场占用 Pod·分钟」前后对比 |
| 1 | `pkg/redisx/ratelimit.go` 两原语 + 契约测试 | pkg | 单测锁定:窗口内拒第二次、窗口后放行、error 时 fail-open 返回 allow |
| 2 | `StartMatch` per-队长/队伍 Cooldown | matchmaker | 单测:冷却内返回 `ErrRateLimited` 且**零副作用**(不创建 operation、不占 claim);Redis 故障时放行 |
| 3 | 容量耗尽改 `WAIT + retry_after`(§4.3③) | ds_allocator + matchmaker + 客户端 | 满载时玩家看到排队而非硬失败;必须并入 §9.23 同一恢复协调器,**不新建第二套状态机** |
| 4 | 登录失败 Quota(账号 + IP) | login | 单测:连续失败触发,成功登录不计数,锁定期过后自动恢复 |
| 5 | Envoy local_ratelimit(未鉴权路径) | envoy.yaml | 压测:登录洪峰下 429 生效、后端 QPS 削平 |
| 6 | 聊天非世界频道 + 社交/交易类 Quota | chat / friend / guild / team / trade / auction | 各自单测同 2 |
| 7 | 成局级冷却 + 换 match_id | matchmaker | 需先按 §7 拍板 `decision-revisit-allocating-bounded-terminal.md` |
| 8 | no-show 记账 → 进入侧指数退避 | matchmaker + ds_allocator | 产品决策,人拍板后再做 |
| 9 | 账号成本(关自动注册 / 设备·IP 维度配额) | login | 产品与运营决策,见 §7 |

**通用验收底线**(每项都必须验,缺一不算完成):
1. **零副作用拒绝**:被限流的请求不得留下任何持久化痕迹(不占坑、不创 operation、不写流水)。
   反面教材是「先做重活再判限流」——那等于限流器本身成了放大器。
2. **fail-open 已验证**:注入 Redis 故障,请求必须放行且只打 Warn。
3. **不卡玩家**:被拒时客户端拿到明确错误码 + 可重试语义,UI 有真实可点入口(§9.19/§9.20)。
4. **退出路径未被波及**:CancelMatch、Logout、放弃匹配在任何限流状态下都必须可用。
5. **压测断言**:按 `stress-discipline.md`,给出限流前后的 GameServer 分配次数 / DB 写入量对比表。
   无对比数字不算完成。
6. **占位闸专属**:必须给出「单次进场平均占用 Pod·分钟」的前后对比,以及
   「押死 Fleet 所需账号数 × 频率」的推算表。只说「缩短了空场超时」不算完成。

---

## 7. 待人拍板

1. **【最重要】自动注册是否保留**(`login.go:1034` `ensureAccount` 账号不存在即建号):这是本仓最大的
   「无成本创建身份」入口。开发期方便,但 §3.2.1 的攻击成本**完全由账号价格决定**——账号免费,
   则所有 per-player 配额都能被「换个号」绕过,§4.3① 的缩短持有时间也只是把所需账号数乘个常数。
   若保留,则 §4.2/§4.3 的配额必须按 **IP / 设备**维度而不只按账号维度,且必须接受「同 NAT 出口的
   正常玩家会被误伤」这个代价。
2. **no-show / abandon 惩罚是否做、罚多重**(§4.3③):涉及玩法体验,不是纯技术决策。
   注意区分「恶意占位」与「网络差反复掉线」——后者不该被惩罚。
3. **线上 `maxReplicas` 定多少、是否留保留位**:当前本地是 2。线上值 = 同时最大局数 = 被押死的
   门槛,必须和节点池内存(每台 14Gi)一起算。是否为「有进度的老玩家」预留一部分容量,属运营策略。
4. **UE 侧空场自结算计时器何时实现**(`agones-dev.md:465` 待实现):它是 §4.3① 的主路径,
   后端双阈值只是兜底。两者都做才是完整形状;只做后端也能显著收益,但 DS 侧自结算更快更省。
5. **各限流初值**:文中都是量级建议;正式值需按压测与灰度数据定,并写进各服务 yaml 而非硬编码。
