# 任务(mission)域设计 —— 自 luyuan/mmorpg C++ 模块移植

> 状态:**实现中**(2026-08-11 起)。本文档是 mission 服务的服务级决策记录(CLAUDE.md §7)。
> 来源:`D:\luyuan\mmorpg\cpp\libs\modules\{mission,condition,reward}` 三个 C++ 模块的语义移植。
> 移植原则:**语义一比一搬,落地形态全部换成本仓标准件**;凡与源实现刻意不同处,在 §7 对照表逐条注明。

## 0. 来源、范围与既有草案的关系

- **源模块语义**(以下简称 D 版):
  - `mission`:接取校验(重复接取 / 已完成 / 类型互斥)→ 条件事件驱动进度(倒排索引命中 + 槽位过滤 + clamp)→ 全条件满足即完成 → 完成扇出(发奖或标记可领、自动接后续链、广播"完成任务 X"条件事件)。GM 批量完成与正常完成是两条刻意分离的路径(D 版 todo.md #225 教训,本移植继承)。
  - `condition`:条件表驱动的通用判定件——五种比较器(>= / > / <= / < / ==)、condition1~4 四个槽位过滤、达标后 clamp 到目标值。
  - `reward`:RewardTable(id → [{item, count}])纯数据引用 + "领取状态归各业务系统自己管"的归属原则 + 可领/已领位集工具。D 版发放只到"发 OnMissionAwardEvent 事件"为止;本移植接到真实发放链(§6),比源头更完整。
- **不移植 ECS**:D 版 `entt::entity` 只是"按玩家索引组件"的句柄;Go 侧天然按 `player_id` 加载聚合状态,领域对象 + 存储层即可。
- **与 `sod-campaign.md` 草案的关系**:该草案(2026-06-19,未拍板)里的 quest 进度层与本域重叠,链路同构(幂等落库 + 事务出箱 + 推送)。本域是**通用任务系统**;campaign 玩法若立项,其任务部分应消费本域(新增 mission_type / 配置行),不再单独实现 quest 层。草案预留的 20017 端口已被 owner 服务占用,草案中 quest 相关数据模型以本文档为准。

## 1. 定域决策(同步登记 pandora-arch.md §11)

| 项 | 决策 | 依据 |
|---|---|---|
| 落点 | `services/social/mission` | 任务与 NPC 对话是近亲玩法(dialogue 同组,go-services.md §2.10 早已预留"对话改任务"对接点);进度事实来自 battle_result 转发、发奖下游是 inventory/player,mission 居中协调,不归 economy/account 单一域 |
| 端口 | gRPC **20019** / metrics **21019** | infra.md §6.2 端口段唯一空档(20018 被 matchmaker-pve 实占但未登记,本次顺手补登记) |
| 错误码 | **11000-11999** 整段 | pkg/errcode/errcode.go 唯一预留段 |
| 库 | 新库 **`pandora_mission`**(MySQL 唯一权威,直写) | 按职能分库(infra.md §2.1);leaderboard 独库先例;全仓无"每玩家领域态 Redis 缓存/回写"先例,进度写入被事实批次摊薄,与 exp/progress 同量级 |
| Redis | **无权威态**,仅 sessiongate 会话门只读 | player / inventory 同款边界 |
| 雪花 ID | **不铸** | 域内无雪花业务 ID(mission_config_id 是 uint32 配置 ID;流水表自增 PK + uk 幂等键足够)。main.go 不接 etcdnode,少一个依赖 |
| Kafka | 生产 `pandora.mission.update`(推送,key=player_id);**不消费** | 事实进入走内部 gRPC(§5),道具入账类链路全仓从未走 Kafka(§15.2 能同步不异步) |

## 2. 配置表(三张,configtable 注解流水线)

注册名 `mission` / `condition` / `reward`;xlsx 源放 `Pandora-Client-SVN/Table/任务/`(r_任务.xlsx、r_条件.xlsx、r_奖励.xlsx)。proto 定义在 `proto/pandora/config/v1/mission.proto`(一文件多表,skill_card.proto 先例)。**数组列一律逗号分隔 string**(导表器禁止 repeated 列,role.proto:49 先例),解析在 `pkg/configtable` 伴生文件做,格式错在加载边界拒批次。

### 2.1 列映射(D 版 → 本仓)

**mission 表**(源 MissionTable):

| D 版列 | 本仓列(excel_col) | 类型 | 说明 |
|---|---|---|---|
| id | id(ID) | uint32 | 主键 |
| mission_type | mission_type(类型) | uint32 | 与 sub_type 组成互斥键 |
| mission_sub_type | mission_sub_type(子类型) | uint32 | 0 = 不参与互斥(D 版 UnregisterMissionIndexes 语义) |
| condition_id | condition_ids(条件ID) | string 逗号数组 | 每元素须存在于 condition 表(AddValidator 跨表校验;fk 注解不支持数组) |
| target_count | target_counts(条件目标) | string 逗号数组 | 与 condition_ids 等长;元素 >0 时覆盖条件行 target_count |
| next_mission_id | next_mission_ids(后续任务) | string 逗号数组 | 完成后自动接取;元素须存在于本表 |
| reward_id | reward_id(奖励ID) | uint32 | 0=无奖励;>0 须存在于 reward 表 |
| auto_reward | auto_reward(自动发奖) | uint32 | >0 完成即发;=0 完成后标记可领 |
| condition_order | —— | —— | **不移植**,见 §2.2 |

**condition 表**(源 ConditionTable):

| D 版列 | 本仓列 | 类型 | 说明 |
|---|---|---|---|
| id | id(ID) | uint32 | |
| condition_category | condition_category(条件类别) | uint32 | 类别枚举见 §5.2 |
| condition1~4 | slot1~slot4(槽位1~4) | string 逗号数组 | 空=该槽不过滤;非空=事件对应槽位值须命中集合 |
| target_count | target_count(目标值) | uint32 | mission 行未覆盖时的缺省目标 |
| comparison_op | comparison_op(比较符) | uint32 | 0:>= 1:> 2:<= 3:< 4:==(伴生校验 ≤4) |
| valid_duration / quantity_type | —— | —— | **不移植**,见 §2.2 |
| design_note | —— | —— | **不移植**:策划备注列,无运行期语义 |

**reward 表**(源 RewardTable,D 版嵌套 repeated 拍平为平行数组):

| 列 | 类型 | 说明 |
|---|---|---|
| id(ID) | uint32 | |
| item_ids(道具ID) | string 逗号数组 | 每元素须存在于 item 表(跨表校验) |
| item_counts(道具数量) | string 逗号数组 | 与 item_ids 等长,元素 >0 |
| exp(经验) | uint32 | 0=无经验奖励;道具与经验可并存 |

### 2.2 源表里「设计了但从未实现」的四列(2026-08-11 核对原始数据后补记)

核对 `D:\luyuan\mmorpg\data\{Mission,Condition}.xlsx` 的**实际数据**(不只是 proto schema)后确认:
下面四列在源表里有明确策划语义,但在 D 版**手写 C++ 源码里零读取**(grep 命中仅生成物
`*_table.pb.*` / `*_table_comp.h` 与编译产物 `.obj`,无任何业务代码消费)。也就是说它们是
**表里的设计意图,不是已实现的功能**;本移植与 D 版代码行为一致地不实现它们。

| 列 | 表 | 策划语义(源表注释原文) | 源数据用了吗 | 不移植的代价 |
|---|---|---|---|---|
| `quantity_type` | Condition | `0:累计(如累计充值人民币1万元) 1:拥有(如拥有一万砖石)` | 全部行填 0(累计) | **最实质的一条**。本实现是纯事件累加(`progress += amount`);「拥有」型要求对**当前状态快照**求值(此刻背包里有多少),事件流模型表达不了,需要另接一条向权威域(inventory/player)查询的通道。现有内容不受影响(数据全是累计),做「拥有 N 个 X」型任务时必须先补这条通道 |
| `valid_duration` | Condition | `时间约束(如 1 小时内完成)` | 全空 | 限时任务无法表达;需要接取时记起始时间 + 到期判定(D 版 `mission_begin_time` 字段存在但同样只在 abandon 时被清理,无判定逻辑) |
| `condition_order` | Mission | `0并行1顺序` | 源数据 mission 2 填了 1(顺序) | 多条件任务当前一律**并行**推进(任一条件的事实来了就推进对应槽)。「顺序」型要求前一条件达标前后续条件不接收事实 |
| `design_note` | Condition | 策划备注(`designer` 列) | — | 无运行期语义,不需要移植 |

**若将来要补**:`quantity_type` 与 `condition_order` 都只需扩条件/任务表一列 + 引擎里加一个
分支,不动存储与协议;`valid_duration` 要给 `player_mission_active` 加到期列并补一个扫描器,
属于独立工作项(§9 非目标)。

### 2.3 校验分层

- 行级(伴生 `validateMissionRow` 等):数组格式合法、平行数组等长、条件槽数 1~8、comparison_op ∈ [0,4]、auto_reward/reward_id 组合合法。
- 批次级(消费服务 `Store.AddValidator`,matchmaker main.go:99 模板):condition_ids / next_mission_ids / reward_id / item_ids 跨表存在性;**next_mission_ids 链环检测**(DFS,环=拒批次——运行期有界迭代是兜底不是许可,§5.4)。

## 3. 数据模型(库 `pandora_mission`)

| 表 | 每玩家行数 | dbcheck 分类 | 说明 |
|---|---|---|---|
| `player_mission_active` | ≤ max_active_missions(50) | bounded | 活跃任务;uk(player_id, mission_config_id);`progress` 列 = `MissionProgressStorageRecord` pb → VARBINARY(256) |
| `player_mission_done` | ≤ mission 配置行数 | bounded | 完成集;uk(player_id, mission_config_id);`reward_state` TINYINT:0 无需领 / 1 可领 / 2 已领 |
| `mission_reward_log` | 发奖流水 | swept | 照抄 leaderboard_reward_log:uk grant_idempotency_key、status 0 PENDING/1 GRANTED/2 FAILED、reward_pb VARBINARY(2048)(CheckPayload 闸)、updated_at_ms;GRANTED 90 天清,PENDING/FAILED 是补发工作集永不清 |
| `mission_fact_receipts` | 事实入账收据 | swept(**清理默认关**) | uk(player_id, idempotency_key) + 请求指纹(同键异内容 fail-closed);与 exp_history 同构:上游 `battle_mission_outbox` 重试无总期限,删收据=事实重放双计,上游有界前不开删 |
| `mission_push_outbox` | 推送出箱 | outbox | 与状态写同事务落行;发布器投 `pandora.mission.update` 成功即删,稳态接近空 |
| `mission_player_guards` | 每玩家 1 行 | exempt | 写守卫行(**锁载体,无业务数据**);TiDB 无 gap 锁,`FOR UPDATE` 在该玩家零活跃行时不加锁,活跃数上限与类型互斥校验必须先锁本行再进临界区。同 `friend_player_guards`,被玩家数有界故 §9.24 登记豁免 |

另有 `pandora_battle.battle_mission_outbox`(battle_result 侧,§5.1):任务事实转发出箱,
outbox 类,独立于 `battle_progress_outbox` 以隔离故障域。

- **进度 blob 三上限**(§9.24 深度):①单槽 uint32;②槽数 ≤8(配置表行校验,写入侧再断言);③整体 `dbguard.CheckPayload` ≤256B。
- **派生态一律不落库**(§9.22):D 版 `eventMissionsClassify_` 倒排索引、`typeFilter_` 类型互斥集,Go 版在事务内从活跃行 + 配置现算(活跃数 ≤50,免维护索引一致性)。
- 通用字段按 infra.md §2.2;`budgets.go` 按"计划玩家数 × 每玩家行上界 × 3"申报。

## 4. 接口契约(`proto/pandora/mission/v1/mission.proto`)

| RPC | 面向 | 语义 |
|---|---|---|
| `ListMissions` | 客户端(Envoy) | 权威快照:活跃(含进度/目标)+ 已完成(含 reward_state)。全量返回,被写入侧上限兜住(§9.18 达标口径);也是 push resync 的回源接口 |
| `AcceptMission` | 客户端 | 接取;校验链见 §7 |
| `AbandonMission` | 客户端 | 放弃;已完成不可弃 |
| `ClaimMissionReward` | 客户端 | 领奖;reward_state 1→2 CAS + 同事务写 reward_log(PENDING),提交后同步尝试发放,失败留补扫 |
| `ReportMissionFacts` | 系统(callerID==0) | 唯一条件事实入口:`facts[]{condition_category, condition_ids[], amount}` + 幂等键;见 §5 |
| `CompleteAllMissions` | 系统(GM) | D 版 GM 批量完成:只置完成态清活跃,**不发奖不接链不广播**(与正常完成刻意分离,不得合并) |

- 身份纪律:客户端 RPC 一律 JWT `x-pandora-player-id`(请求体不带 player_id);系统 RPC 服务层 `systemOnly` + Envoy 精确 path `direct_response 403` 双保险(2026-07-08 player/inventory 放行先例)。
- 推送:进度变化 / 完成 / 可领,入账事务内写 push outbox → `pandora.mission.update`(key=player_id,event_type 走 header)→ push 透传;客户端判重按 mission_config_id + 状态,收到 `pandora.push.resync` 回源 `ListMissions`(protocol-ordering-rules 原则 5:推送不承担正确性)。

## 5. 条件事件链路

### 5.1 首版事实源:battle_result 出箱转发

Battle DS → `ReportProgress`(既有 Guard/roster/水位/额度)→ `ApplyProgress` **同事务**写
`battle_mission_outbox` 行(一事实一行)→ `RunMissionForwarder` → `mission.ReportMissionFacts`,
幂等键 `progress:{match_id}:{seq}:{player_id}:mission`。**不另开 DS→mission 通道**:battle_result
是 DS 事实唯一 ingest 权威(ds-arch §0.5 / §9.22);mission 侧收据表兜 at-least-once。
battle_result 的 `mission_addr` 空 = 转发整体关闭(**一行不产**,产了投不出去只会让出箱无界堆积),
**发布顺序 Go 先行**:先上 mission 服务再配地址开转发(§9.21)。

**为什么是独立表 `battle_mission_outbox` 而不是给 `battle_progress_outbox` 加一个 kind**
(实现期推翻了最初设计,理由入档):理由是**故障域隔离**,不是"任务事实可以乱序"。
`FetchProgressOutbox` 用 `NOT EXISTS` 子句强制每玩家严格 FIFO——item balance 权威要求同玩家的
pickup 与 consume/discard 有序投递。任务行混进那张表,mission 服务一旦不可用就会卡在队首,
**连带阻塞该玩家的经验与掉落投递**,把说好的弱依赖变成强依赖。分表后 mission 故障只堵任务
事实,经验与掉落照常发放;该契约由 `tools/migrate` 的 000010 迁移守卫测试钉住。

> **勘误(2026-08-11,实现期第二次推翻)**:本节原写「任务事实本身顺序无关(进度累加 + 达标
> clamp,同一集合任意投递序收敛到同一状态)」,据此让 `battle_mission_outbox` 按 id 平摊投递、
> 单行失败只退避自己。**该论断是错的**:任务链前后两环的条件类别通常不同(「杀 5 只狼」→
> 「收集 3 张狼皮」),而后环任务只在前环完成时才被自动接取。若「狼皮」事实先于「杀狼」事实
> 到达,mission 侧扫遍活跃任务匹配不上任何条件类别,该事实被收据吸收后**静默丢弃且永不重放**
> ——玩家的后续任务进度永久缺一块。乱序的来源正是 `DeferMissionOutbox`:队首失败退避后,
> 同玩家后续行会越过它先投。现已改为与进度出箱同款的每玩家 FIFO 队首阻塞(`NOT EXISTS`
> 前驱谓词),跨玩家仍互不影响。回归见 `internal/data/mission_outbox_mysql_test.go`。

**「使用道具」事实必须等扣除落定**(`pending_action` 列):局内消费走
`battle_progress_action` 的同步 action 路径,可能以**业务失败终态**收场(道具不足等),此时
inventory 一件没扣、UE 也保留本地物品。事实若照发,「使用 N 个 X」型任务就能靠上报根本没
发生的消耗刷完(§9.6 派生数值不信 DS)。故 USE_ITEM 行落库即 `pending_action=1`:不可投递,
但仍占队首挡住同玩家后续事实;`ResolveProgressAction` 在**扣除结果同一事务**里置 0(扣成功)
或删行(扣失败)。分两次提交会留下"已失败但已可投递"的窗口,足够让补扫把它发出去。
拾取 / 击杀不挂此闸:它们没有 action 结果行可等,且"捡到了"本身就是 DS 记录的事实,发放
失败属投递问题(满包转邮件 / 退避重试)而不是"这件事没发生"。

**三条转发纪律**(与发放白名单刻意不同,已固化进 `biz/mission_forward_test.go`):
①**经验表漏配的怪照样转发**——「这只怪被杀了」与「这只怪配没配经验」是两件事,role_level
漏配不该让杀怪任务跟着不计数;未被任何条件引用的怪物 ID 在 mission 侧自然匹配不上,不构成
放行面。②**非白名单拾取不转发**——那本就被判为可疑事实(发放已 skip),放它推进任务进度等于
另开一条绕过白名单的计数通道。③**丢弃(ItemDiscard)不转发**——扔掉不是用掉,否则「使用 N 个 X」
型任务能靠捡了再扔刷完。

### 5.2 条件类别枚举(1-8 继承 D 版,9 起本仓扩展)

| 类别 | 值 | 事件槽位 condition_ids | 首版事实源 |
|---|---|---|---|
| KILL_MONSTER | 1 | [monster_config_id] | battle 转发(MonsterKillFact,amount=count) |
| TALK_NPC | 2 | [npc_id] | 后续:dialogue 服务(未接) |
| COMPLETE_CONDITION | 3 | [condition_id] | 保留(D 版语义) |
| USE_ITEM | 4 | [item_config_id] | battle 转发(ItemConsumeFact) |
| INTERACT | 5 | [interact_id] | 后续:Hub 交互(无通道,未接) |
| LEVEL_UP | 6 | [level] | 后续:player 升级出箱(未接) |
| CUSTOM | 7 | 自定义 | 保留 |
| COMPLETE_MISSION | 8 | [mission_config_id] | **域内自产**:完成扇出时再入(§5.4) |
| PICKUP_ITEM | 9 | [item_config_id] | battle 转发(ItemPickupFact)。D 版无拾取类别,本仓扩展 |

后续事实源接入方式统一:调用方自己的事务出箱 + 同一 `ReportMissionFacts` RPC,mission 侧零改动。

### 5.3 进度推进(移植 D 版 UpdateMissionProgress / MatchesEventSlots)

单事务:锁 `mission_player_guards` 守卫行(玩家级串行,恒为第一把锁;TiDB 无 gap 锁,见 §3 表格)→ 收据 INSERT(撞 uk → 幂等 no-op 返回)→ `SELECT ... FOR UPDATE` 锁玩家活跃行 → 逐事实逐任务:类别命中 → 槽位过滤(条件行 slotN 非空则事件第 N 槽值须在集合内;全空槽条件匹配任意同类事件,但**空 condition_ids 的事实不推进任何进度**——D 版护栏)→ 已达标槽跳过 → progress += amount,达标 clamp 到 target → 全槽达标 → 完成扇出(§5.4)→ 变更行回写 + push outbox。

### 5.4 完成扇出(移植 D 版 OnMissionCompletion,同一事务)

对每个刚完成的任务:①删活跃行、写 done 行;②reward_id>0:auto_reward → done.reward_state=0 + reward_log(PENDING);否则 reward_state=1(可领);③next_mission_ids 逐个走接取校验自动接(校验不过跳过该条,不阻断);④生成 COMPLETE_MISSION(condition_ids=[mission_config_id], amount=1)内部事件入队再入 §5.3。内部队列**迭代上限 16 轮**,越界丢弃剩余事件 + ERROR 日志(D 版 dispatcher 异步无环保护,移植加固;配置环已在批次校验拦截,运行期上限是纵深兜底)。事务提交后:对本次产生的 PENDING reward_log 同步尝试发放一次,失败留补扫。

## 6. 发奖链(leaderboard 模式)

- `mission_reward_log` 状态机 + 补扫 worker(1min 间隔、grace 2min、单轮 200,safego 兜底)完全同构 leaderboard `RetryUngrantedRewards`;多副本并发补扫安全(靠下游幂等键)。
- 发放路由(granter,按 reward 表内容逐类发,任一类失败整条留 PENDING 重试):
  - 堆叠/货币 → inventory `GrantItems`,键 `mission:{player_id}:{mission_config_id}`;
  - 装备实例 → inventory `GrantInstances`(item catalog Equipment 判定),满包 → mail `SendOverflowMail` 传同一键作 `instance_grant_key`(直发链与邮件链至多一次,battle_result 模式);
  - 经验 → player `AddExperience(reason="quest_complete")`,键 `quest:{player_id}:{mission_config_id}`(player.proto:513 预留口径)。
- 领取语义:ClaimMissionReward 的 CAS(reward_state=1→2)与 reward_log(PENDING) 同事务 = "已领"立即权威生效,内容 at-least-once 必达(下游幂等去重)。与 mail"先入包后标记"殊途同归:两者的不变量都是**标记与发放之间 crash 不丢奖**。

## 7. C++ → Go 语义对照表

| D 版 | Go 落点 | 刻意差异 |
|---|---|---|
| `MissionsComp` bitset(completed/claimable) | done 行 + reward_state 列 | bitset→行存:免 bit_index 生成链,行数被配置表有界;§9.17 无 Redis pb 兼容问题 |
| `eventMissionsClassify_` 倒排索引 | 事务内现算 | 派生态不落库不驻内存(服务无状态可水平扩展) |
| `typeFilter_` 类型互斥集 | 事务内查活跃行 join 配置 | 同上 |
| `CheckMissionAcceptance` | biz.acceptChecks | 语义一致:未接取/未完成/配置存在/类型互斥;**新增** max_active_missions 上限(§9.18 硬要求,D 版无) |
| `AcceptMission` 建 progress 槽+索引 | active 行 progress pb 零值槽 | 一致;OnAcceptedMissionEvent → push outbox |
| `AbandonMission` | 删 active + 防御性删 done + 清可领 | 一致(D 版先校验未完成,再防御清完成位) |
| `HandleConditionEvent`/`UpdateMissionProgress`/`UpdateProgressIfConditionMatches` | §5.3 | 一致,含空 condition_ids 护栏、达标跳过、clamp |
| `condition_util` 五比较器/槽位匹配/clamp | pkg 内纯函数 | 一致;target_count 覆盖规则一致(>0 才覆盖) |
| `OnMissionCompletion` 四步扇出 | §5.4 同事务 | 一致;dispatcher enqueue → 有界内部队列(16 轮,加固) |
| `CompleteAllMissions`(GM) | 系统 RPC | 一致:无副作用路径,不得与正常完成合并(todo.md #225) |
| `OnMissionAwardEvent`(发放留白) | §6 真实发放链 | **超出源头**:reward_log + inventory/player + 补扫 |
| `GetMissionReward` 清可领位 | ClaimMissionReward | D 版只清位不发内容;Go 版 CAS + 发放链 |
| `mission_begin_time` / `valid_duration` | 不移植 | D 版时限判定未实现完(begin_time 只在 abandon 清理);见 §9 |
| `MissionListComp` 三 scope(任务/成就/日常) | 不移植 | §15.3 拒预设;mission_type 已够分类,成就/日常立项时再议 |
| `condition_order` 列 | 不移植 | D 版零读取死列 |

## 8. 不变量对照(CLAUDE.md §9)

- **9.6**:DS 只报事实,判定/换算全在 Go;mission 无 DS 直写。
- **9.18**:接取是客户端可写累积列表 → 写入侧 `max_active_missions`(默认 50,事务内校验,超限 `ErrMissionActiveLimit`)+ 读取侧全量返回被写入上限兜住;登记"现存受管列表清单"。
- **9.21**:新服务纯增量;battle_result 转发默认关,Go 先行;配置表先发 dist 再滚二进制(store 对未知表告警跳过)。
- **9.22**:MySQL 唯一权威;倒排索引/互斥集为派生态现算;推送非权威,ListMissions 兜底。
- **9.24**:六表登记(§3 表格)+ dbcheck 内嵌清单 + budgets.go;receipts 清理默认关(exp_history 同因);reward_pb/progress blob 三上限齐;`mission_player_guards` 登记为豁免(每玩家 1 行)。奖励侧装备数量另有 `configtable.MaxRewardEquipmentInstances`(=64)双闸:加载期 `ValidateMissionCrossTables` 拒批次 + 运行期 `deliver` fail-closed —— 装备按件展开成 instance 列表,数量**就是**切片长度,配置手滑成大数会在发放侧 OOM,且快照落库后每轮补扫再炸一次。
- **§15 简单性举证**:同步 gRPC 入账(全仓道具链无 Kafka 先例)、无缓存、无新中间件、无状态机框架;复杂点仅"同事务扇出 + 有界再入",消除的是"完成与发奖/接链撕裂"这个已确认问题。
- **§16**:幂等(收据 uk / reward uk / 下游幂等键)、乱序重复(at-least-once 收据吸收 + `battle_mission_outbox` 每玩家 FIFO 保住链上因果序,见 §5.1 勘误)、部分失败(PENDING 补扫闭环;USE_ITEM 事实与扣除结果同事务落定)、多副本(无进程权威态,守卫行 + FOR UPDATE + uk 原子;守卫行是 TiDB 无 gap 锁下的必需件,真 TiDB 并发回归见 `internal/data/mission_repo_mysql_test.go`)、边界(clamp/uint32 溢出饱和/空数组护栏/装备展开件数上限);测试矩阵见服务 README。

## 9. 非目标(首版)

时限任务(valid_duration)、成就/日常 scope、TALK_NPC / LEVEL_UP / Hub INTERACT 事实源接入(接缝已留:同一系统 RPC)、活动任务、任务 UI(UE 客户端仓,另行)、CfgMission 客户端侧导表(客户端消费时按双仓同步纪律补)。

## 10. 发布顺序与交接

1. dist 批次(含三张新表)先发——旧二进制对未知表告警跳过;
2. mission 服务上线(migrate 先行建库表);
3. battle_result 配 `mission_addr` 开转发;
4. Envoy 路由 + jwt 规则上线,客户端放行。

Codex 交接项:`proto_gen.ps1`(含 cpp pb 同步 UE)、新 module `go mod tidy` 清单(mission、battle_result)、configtable-gen 重建 + 跑表(需 xlsx 先就位)。
