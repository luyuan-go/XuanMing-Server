# player

> 玩家档案服务:昵称 / 等级经验 / 段位 MMR / 英雄池 / 出战养成(加点·装备预设·天赋)/ 领奖记录的
> **权威读写**,数据落 MySQL `pandora_player`。段位 MMR 由 battle_result 经 `pandora.player.update`
> 幂等驱动;经验实时入账后走出箱推给客户端刷经验条。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`:
> 服务契约见 [`go-services.md §2.2`](../../../docs/design/go-services.md),实时成长见
> [`realtime-progression.md`](../../../docs/design/realtime-progression.md),
> 等级经验表热更见 [`config-table-hotreload.md`](../../../docs/design/config-table-hotreload.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:玩家 owner 数据(昵称 / 等级 / 经验 / 段位 mmr / 战绩计数 / 英雄解锁 / 属性点 / 装备预设 /
  天赋 / 领奖记录)的读写与幂等结算,`GetLoadout` 组装开战前快照供匹配 / 进战下发。
- **权威态**:全部在 **MySQL `pandora_player`**(结构化列 + 领奖位图 blob),进程内无状态,可水平扩展。
- **不做的事**:
  - **不算派生数值**(不变量 §9.6)——MMR 增减 / 经验换算由 battle_result 等按事实换算,本服务只做
    幂等入账、按曲线进位与上限兜底;等级经验曲线唯一权威在 configtable(`j_玩家等级经验.xlsx`),不保留
    YAML 双数据源。
  - **不管战斗内逻辑**(ds-arch.md §0):技能 / 即时出装 / 用道具走 UE GAS,不经本服务;这里只持久化大厅态
    装备预设 / 天赋,`GetLoadout` 时转成 Battle DS 初始 GameplayEffect。
  - **不做玩家归属 / presence**(那是 player_locator / owner authority 的活)。档案是玩家 owner 数据,
    分片上线时锚定玩家 owner cell(§4.2,`ProfileShardKey = player_id`),本服务只做落点观测。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20002` | 客户端 RPC(经 Envoy)+ 内部直连 RPC |
| HTTP | `:21002` | 仅 `/metrics`(`player.proto` 无 `google.api.http` 注解,无 RESTful RPC) |

端口来自 `internal/conf/conf.go` 的 `Defaults()`。

## 对外接口

代码入口:`internal/service/player.go`(实现 `playerv1.PlayerServiceServer`)。**鉴权全部下沉到 service 层**,
以 Envoy `jwt_authn` 注入的调用者身份(`pmw.PlayerIDFromContext`)为准,**不信任请求体 `player_id`**。
三种鉴权模式(`service/player.go:38 / 55 / 70`):

| 模式 | 判定 | 用途 |
|---|---|---|
| `selfPlayerID` | 未鉴权(`callerID==0`)拒 `ERR_UNAUTHORIZED`;`req.player_id` 与调用者不一致拒 `ERR_PERMISSION_DENY` | 客户端**自助写**,只能改自己 |
| `resolvePlayerID` | `callerID==0`(内部直连)信任请求体;`callerID>0`(客户端)只能查自己 | **双模读** + 开局快照注入 |
| `systemOnly` | 带玩家 JWT(`callerID>0`)一律拒 `ERR_PERMISSION_DENY` | **内部专用系统 RPC**,不在 Envoy 暴露 |

此外 gRPC server 挂了 `pmw.SessionCurrent`(`server/grpc.go`):客户端面请求的 jti 必须等于 login 会话权威
`pandora:sess` 当前一代,顶号后旧 JWT 在 exp 前即失效(R5 复审 P0-1,INC-20260722-004);内部直连不带
`x-pandora-jwt-payload`,天然放行。

**客户端读 RPC(`resolvePlayerID`)**

| RPC | 调用方 | 语义 |
|---|---|---|
| `GetProfile` | 客户端 / 内部 reader | 读档案(懒创建);出参装饰经验派生字段 |
| `ListHeroes` | 客户端 | 列已解锁英雄 |
| `GetMMR` | 客户端 / battle_result reader | 读当前段位 MMR(未建档返回 `base_mmr`,不建行) |
| `GetActiveHero` | 客户端 | 读出战英雄(未选定返回 0) |
| `GetAttributes` | 客户端 | 读已分配属性点 + 未分配点 |
| `GetEquipment` | 客户端 | 读出战装备预设；存量旧行以 `instance_id=0` 只读回显并引导重选 |
| `GetTalents` | 客户端 | 读已点天赋 + 可点天赋点 |
| `GetLoadout` | 客户端 / matchmaker·DS 开局快照注入 | 组装开战前快照；装备按 exact instance 复核并携带权威鉴定词条 |
| `GetRewardClaims` | 客户端 | 查某来源已领取的 reward_id 列表 |

**客户端自助写 RPC(`selfPlayerID`)**

| RPC | 调用方 | 语义 |
|---|---|---|
| `UpdateNickname` | 客户端 | 改昵称(空 / 超 `max_nickname_len` / 占用 拒) |
| `SelectHero` | 客户端 | 设出战英雄(功能开关 + 须已解锁) |
| `AllocateAttributePoints` | 客户端 | 分配属性点(点数不足 / 越界拒) |
| `ResetAttributes` | 客户端 | 洗点(已分配点全退回) |
| `SetEquipment` | 客户端 | 全量替换出战装备预设(功能开关) |
| `SetTalents` | 客户端 | 全量重置天赋分配(功能开关) |
| `ResetTalents` | 客户端 | 清空天赋分配(功能开关) |
| `ClaimReward` | 客户端 | 领取一档奖励(幂等,乐观锁) |

**内部专用系统 RPC(`systemOnly`,拒玩家 JWT,不经 Envoy)**

| RPC | 调用方 | 语义 |
|---|---|---|
| `UnlockHero` | 后端内部(购买 / 奖励到账) | 解锁英雄(幂等,已拥有报 `ERR_PLAYER_HERO_ALREADY_OWN`) |
| `UpdateMMR` | 后端内部(同步兜底) | 幂等改 MMR + 战绩计数;正常链路走 kafka 消费 |
| `GrantAttributePoints` | 后端内部(升级 / 活动) | 幂等授予可分配属性点 |
| `GrantTalentPoints` | 后端内部(升级 / 活动) | 幂等授予天赋点 |
| `AddExperience` | 后端内部(battle_result progress 出箱 / 任务 / GM) | 幂等入账经验并结算等级(实时成长唯一入口) |

> 除玩家 RPC 外,gRPC server 还注册了 `ConfigTableAdminService`(`server/grpc.go`),供等级经验表热更 admin
> 调用(`internal/service` 复用 `pkg/configtable` 的 `NewAdminService`,不是本服务自研)。

## 目录结构(Kratos 标准分层,对齐 login / matchmaker)

```
cmd/player/main.go            启动入口(configtable + MySQL + schema gate + 出箱发布器 + kafka consumer 装配)
etc/player-dev.yaml           开发期配置(gRPC :20002 / MySQL / kafka / 等级经验表目录)
etc/player-prod.yaml.example  生产配置样例
internal/
  conf/conf.go                配置结构(嵌入 pkg/config.Base + PlayerConf + Defaults)
  service/
    player.go                 RPC 入口(实现 PlayerServiceServer;三态鉴权下沉 + errcode→proto 映射)
  biz/
    player.go                 PlayerUsecase 核心(档案 / 英雄 / MMR / 加点 / 装备 / 天赋 / GetLoadout)
    experience.go             AddExperience 实时成长 + 出箱发布器 + 两个保留期 janitor
    reward.go                 领奖记录(乐观锁 + 位图 + unknown-field 保留)
    consumer.go               pandora.player.update 消费 handler(幂等 UpdateMMR)
    profile_sharding.go       档案 owner cell 落点口径 + 观测(nil-safe,单 Cell 行为不变)
  data/
    player_repo.go            PlayerRepo 接口 + MySQLPlayerRepo(库表说明在文件头)
    profile_repo.go           players 档案(EnsureProfile 懒创建 / 昵称)
    mmr_repo.go               MMR 幂等事务(mmr_history uk)
    hero_repo.go              player_heroes 解锁 / 出战英雄
    attribute_repo.go         player_attributes / attr_point_grants
    equipment_repo.go         player_equipment 出战装备预设
    talent_repo.go            player_talents / talent_point_grants
    experience_repo.go        exp_history 幂等收据 + player_push_outbox(入账同事务)
    reward_repo.go            player_reward_claims(乐观锁 version)
  server/                     grpc(注册 PlayerService + ConfigTableAdmin + 鉴权中间件)/ http(/metrics)
```

## 核心调用链

RPC handler 做鉴权 + 参数校验后转 `biz.PlayerUsecase`,业务逻辑再落 `data.PlayerRepo`。所有写路径先
`EnsureProfile`(`INSERT IGNORE` 懒创建默认档案)保证后续行存在。

### 1. GetProfile —— 懒创建 + 经验派生装饰

`GetProfile`(`internal/biz/player.go:111`):`EnsureProfile` → `repo.GetProfile` → `DecorateExperience`
用当前等级曲线补 `exp_in_level` / `is_max_level`(满级夹紧为 0);曲线未配置(功能关)则原样返回,行为不变
(`internal/biz/experience.go:97`)。

### 2. 段位 MMR —— 幂等,正常走 kafka

`UpdateMMR`(`internal/biz/player.go:199`)是不变量 §2 的落点(`idempotency_key` 一般 = `match_id`):

```
[正常链路] battle_result 结算 → produce pandora.player.update(key=player_id)
   └─► KafkaConsumer → PlayerUpdateHandler (internal/biz/consumer.go:30)
         ├─ 校验 event_type header(非法→Poison 进 DLQ;未来事件→跳过告警)
         ├─ 解 PlayerUpdateEvent;player_id/match_id 缺失→丢弃告警
         └─► UpdateMMR(player, mmr_delta, reason, key=match_id)
[兜底链路] 内部直连 UpdateMMR RPC(systemOnly)────────────────┘
                                        │
              EnsureProfile → repo.ApplyMMRChange (data/mmr_repo.go, 单事务)
                ├─ INSERT mmr_history(uk player_id+idempotency_key)
                │    └─ 命中 1062 唯一键冲突 → already=true,读回已记录 new_mmr,不重复改 players.mmr
                └─ UPDATE players.mmr(clamp 到 floor)+ 按 reason 计 total_battles / total_wins
```

`reason` 决定战绩计数(`battleFlags`,`internal/biz/player.go:535`):`win`=计场+计胜,`lose`/`draw`=只计场,
其余(`abandon`/`rollback`)不计。写成功后 `logProfilePlacement` 打档案 owner 落点观测(单 Cell 时 no-op)。

### 3. AddExperience —— 实时成长(入账 + 出箱推送)

`AddExperience`(`internal/biz/experience.go:46`)是系统 RPC 的业务核心(realtime-progression.md §4.2):

```
AddExperience(player, delta, reason, idempotency_key)   [systemOnly]
  ├─ 校验:delta>0、<= max_exp_per_grant、experience_enabled、曲线已加载
  ├─ EnsureProfile
  └─► repo.ApplyExperience (data/experience_repo.go, 单事务)
        ├─ INSERT exp_history(uk player_id+idempotency_key)→ 冲突 already=true(幂等收据)
        ├─ 按曲线循环进位连升多级 / 最高等级封顶 / 满级 no-op
        ├─ UPDATE players.level, players.exp
        └─ INSERT player_push_outbox(入账与出箱同一事务,不变量 §4)
                              │
   后台 RunPushOutboxPublisher(internal/biz/experience.go:122,main 里 go 起)
        └─ 每 push_outbox_interval 取一批(FIFO 按 id 升序)→ 逐条投 kafka
             pandora.player.experience(key=player_id,event_type=EXPERIENCE header)
             └─ 成功才删出箱行;失败中断本轮保序;满批立即续下一批排空
                              │
                     push 服务透传 → 客户端刷经验条 / 播升级
```

- **经验事件必须走独立 topic** `pandora.player.experience`,**绝不能**发 `pandora.player.update`:旧 player
  副本消费 `player.update` 不看 `event_type`,会把经验事件误解码成 MMR 污染段位(金丝雀混跑,不变量 §21;
  见 `cmd/player/main.go` 出箱器注释)。
- **发布器无需 leader / claim / fencing**:事件是入账后的**全量权威快照**,客户端按 `(level, exp_in_level)`
  单调不回退去重,重复 / 旧快照晚到无副作用(at-least-once + 快照语义)。

### 4. ClaimReward —— 乐观锁幂等 + unknown-field 保留

`ClaimReward`(`internal/biz/reward.go:66`)读-改-写循环(`maxRewardClaimRetry=3`):

```
loadRewardRecord → Unmarshal RewardClaimStorageRecord(保留 stored 原 message)
  → rec.ClaimPermanent/ClaimActivity(位图置位,已领→ErrAlreadyClaimed→ERR_REWARD_ALREADY_CLAIMED)
  → saveRewardRecord:只覆盖 permanent/activity 两字段后 Marshal,乐观锁 WHERE version=expectVersion
       版本冲突(ErrPlayerVersionMismatch)→ 重读重试;耗尽重试仍冲突→报错
```

> **禁止重建 message**:回写必须带回读取时 `stored` 携带的 unknown fields,否则等效 `DiscardUnknown`,
> 金丝雀 / 滚动共存窗口内会静默清掉新副本写入的新字段(不变量 §17,zero-downtime-update.md §2.3 / §7.3)。

### 5. GetLoadout —— 开战前快照聚合(双模)

`GetLoadout`(`internal/biz/player.go`)聚合出战英雄 + 属性点 + 装备预设 + 天赋成 `PlayerLoadout`,
供匹配 / 进战下发。装备预设不盲信库里的旧快照：开战前经 inventory
`CheckInstancesOwned(instance_id+item_config_id)` 重新核验当前归属，并把同一回包的
`identified+attributes[]` 保真写进 `LoadoutEquipment`。存量 `instance_id=0`、实例已转移、
详情缺失（包括命中旧 inventory 仅回 IDs）或鉴定不变量破坏时全部 fail-closed，
不会产生错词条初始效果。`resolvePlayerID` 双模:内部直连(matchmaker / DS 开局快照注入)
信任请求体,客户端只读自己。滚动发布先后顺序见 `docs/design/ds-arch.md` §0.5。

## 存储

**MySQL `pandora_player`**(建表见 `deploy/mysql-init/04-player-tables.sql` 及 `tools/migrate` 各迁移):

| 表 | 主键 / 唯一键 | 用途 |
|---|---|---|
| `players` | PK `player_id`,uk `nickname` | 档案:昵称 / level / exp / mmr / 战绩计数 / 各类未分配点数 |
| `player_heroes` | uk `player_id+hero_id` | 英雄解锁池 + 出战英雄 |
| `mmr_history` | uk `player_id+idempotency_key` | MMR 变更历史 + 幂等键(不变量 §2) |
| `player_attributes` | `player_id+attr_key` | 已分配属性点 |
| `attr_point_grants` | uk `player_id+idempotency_key` | 属性点授予幂等收据 |
| `player_equipment` | uk `player_id+slot` / `player_id+instance_id` | 出战装备精确实例预设；`instance_id` nullable 仅为滚动升级及存量旧行 |
| `player_talents` | `player_id+talent_id` | 已点天赋等级 + 该节点实际消耗点数(`spent_points`,可点数按它求和,不按等级和反推) |
| `talent_point_grants` | uk `player_id+idempotency_key` | 天赋点授予幂等收据 |
| `player_skill_cards` | uk `player_id+card_id` | 技能卡持有状态(等级 + 碎片余量) |
| `player_skill_slots` | uk `player_id+slot` / `player_id+card_id` | 4 个技能卡槽的全量装配;同卡不可占两槽 |
| `skill_card_grants` | uk `player_id+idempotency_key` | 技能卡/碎片发放幂等收据 |
| `exp_history` | uk `player_id+idempotency_key` | 经验入账幂等收据 |
| `player_push_outbox` | PK `id` | 经验推送出箱(与入账同事务写,FIFO 发布) |
| `player_reward_claims` | PK `player_id` | 领奖位图 blob(`RewardClaimStorageRecord`)+ 乐观锁 `version` |

**Redis**:本服务**不拥有** Redis 权威态;仅经 `pkg/sessiongate` **只读** login 会话权威 `pandora:sess`
(`node.redis_client` 指向共享实例)做会话现行性门校验。

**保留期(不变量 §9.24)**:`exp_history`(默认 7d)、`mmr_history` / `attr_point_grants` /
`talent_point_grants` / `skill_card_grants`(默认 90d,钳 `[30,90]`)由 `RunExpHistoryJanitor` /
`RunHistoryJanitor` 每小时跑一轮,**默认只报告不删**(`retention_mode` 留空 = `report_only`):
只统计超期行数并打 `WARN db_retention_pending_not_deleted` + `pandora_db_retention_pending_rows`
gauge,一行都不删。报告口径与真删口径共用同一个 `WHERE`(`pkg/dbguard.SweepTable` 强制),
不会出现"报告说 0 行、实际删了一堆"。

**真删要两道闸同时开**:

| 闸 | 键 | 问的问题 |
|---|---|---|
| 服务总闸 | `retention_mode: "delete"` | 这个服务现在允许删数据吗?(运维口径,§9.24 默认不允许) |
| 每组前置条件 | `exp_history_cleanup_enabled` / `history_cleanup_enabled` | 这一组的上游重放窗口已经小于留存期了吗?(技术口径) |

分两道是因为前置条件对两组并不同时成立:`exp_history` 卡在 battle_result progress 出箱
**没有总重试期限**;`mmr_history` 那一组卡在 kafka retention 与授予补扫窗口。任一没确认就删幂等
收据 = 同一事件双发(重复加经验 / 段位分 / 点数 / 重复发卡),且删完才发现、不可逆。
`retention_mode` 写了无法识别的值启动即 fail-fast 拒启(不会被猜成 `delete`);
`-Prod` 产物由 `gen_cluster_config.ps1` 机械置成 `report_only` + 两个前置开关 `false`。
首次开删前先 `dbcheck -pending` 看积压规模,低峰期发布。

## 幂等与关键约束

| 主题 | 约束 | 代码锚点 |
|---|---|---|
| MMR 幂等 | 同一 `idempotency_key`(=match_id)只算一次,`mmr_history` uk 兜底 | `ApplyMMRChange` |
| 经验幂等 | `exp_history` uk;满级仍消费幂等键落 no-op 收据,防曲线扩容后滞留事件重入账 | `AddExperience` / `ApplyExperience` |
| 授予幂等 | 属性 / 天赋点授予以 `idempotency_key` 去重 | `GrantAttributePoints` / `GrantTalentPoints` |
| 入账-出箱同事务 | 经验入账与推送出箱在一个 MySQL 事务(不变量 §4) | `ApplyExperience` |
| 快照推送 at-least-once | 出箱事件是全量权威快照,客户端单调去重,可重复投递 | `RunPushOutboxPublisher` |
| 领奖乐观锁 | 位图 `version` 乐观锁;回写保留 unknown fields(不变量 §17) | `ClaimReward` / `saveRewardRecord` |
| 鉴权下沉 biz 前 | 写以调用者身份为准不认 body;系统 RPC 拒玩家 JWT;会话现行性门 | `selfPlayerID` / `systemOnly` / `SessionCurrent` |
| 数值不信 DS | 曲线 / 上限 / 换算在服务端,`max_exp_per_grant` 兜底一次灌满(不变量 §9.6) | `AddExperience` |
| 装备未做拥有权校验 | `SetEquipment` 只校验槽位不重复 + 非 0,**未接 inventory 前不可对客户端开放** | `SetEquipment` |

## 配置项(`internal/conf/conf.go`)

| 键(`player.*`) | 默认 | 说明 |
|---|---|---|
| `base_mmr` | `1500` | 新玩家缺省 MMR(建档 / `GetMMR` 未建档兜底,与 battle_result 对齐) |
| `mmr_floor` | `0` | MMR 下限(`UpdateMMR` 后 clamp) |
| `default_nickname_prefix` | `Player_` | 建档默认昵称前缀(`prefix+player_id` 保证 uk 唯一) |
| `max_nickname_len` | `32` | 昵称最大长度(rune 计) |
| `hero_selection_enabled` | `false` | 出战英雄选择开关(关闭时 `SelectHero` 返回 `ERR_PLAYER_FEATURE_DISABLED`) |
| `loadout_customize_enabled` | `false` | 装备预设 / 天赋自定义开关;**接 inventory 拥有权校验前生产必须 false** |
| `consume_topics` | `[pandora.player.update]` | 订阅的 kafka topic |
| `experience_enabled` | `false` | 经验入账功能放行(曲线始终来自 configtable;dev yaml 置 true) |
| `max_exp_per_grant` | `1000000` | 单次 `AddExperience` 入账上限(防一次灌满) |
| `push_outbox_interval` | `1s` | 经验推送出箱发布轮询间隔 |
| `push_outbox_batch` | `128` | 每轮出箱取多少条(满批自动续跑排空) |
| `retention_mode` | `report_only` | 保留期清理总闸(§9.24);`delete` 才真删,拼错启动 fail-fast |
| `exp_history_cleanup_enabled` | `false` | `exp_history` 删除前置条件确认(未确认 → 降级只报告;上游出箱无总重试期限时开启会破坏幂等) |
| `exp_history_retention` | `7d` | `exp_history` 留存期(下限 7d,上限钳 90d;报告与实删同一值) |
| `history_cleanup_enabled` | `false` | `mmr_history` / 点数授予 / 技能卡授予历史的删除前置条件确认(同上理由) |
| `history_retention_days` | `90` | 上述四表留存天数(下限 30,上限 90;报告与实删同一值) |

> **强依赖**:`config_table.dir`(等级经验表 `j_玩家等级经验.xlsx` 唯一数值源)、`node.mysql_client.dsn`
> (`pandora_player`)、`kafka.brokers`(消费 `player.update`)缺失均在监听端口前 fail-closed。启动还跑
> schema gate(`ValidateExperienceSchema` / `ValidateExperienceLevels`),经验相关表列缺失或存在高于表上限的
> 脏等级时拒启,避免副本 Ready 后首个请求才大面积报错。`session_gate.require` 由 `-Prod` 产物机械置 true。

## 本地启动

```powershell
# 1. 基础设施(MySQL pandora_player 库 + Redis pandora:sess + kafka)
pwsh tools/scripts/dev_up.ps1

# 2. 启 player(dev 配置:gRPC :20002 / HTTP :21002)
go run ./services/account/player/cmd/player -conf services/account/player/etc/player-dev.yaml
```

## 关联文档

- [`go-services.md §2.2`](../../../docs/design/go-services.md) — player 服务契约(RPC / 职责 / 不变量)
- [`realtime-progression.md`](../../../docs/design/realtime-progression.md) — 经验实时成长入账 + 出箱推送
- [`config-table-hotreload.md`](../../../docs/design/config-table-hotreload.md) — 等级经验表热更流水线(不变量 §15)
- [`ds-arch.md`](../../../docs/design/ds-arch.md) — 大厅态 / 战斗内边界,`GetLoadout` 开局快照转 GameplayEffect
- [`scale-cellular-20m.md`](../../../docs/design/scale-cellular-20m.md) §4.2 — 玩家 owner cell,档案落点口径(`ProfileShardKey`)
- [`zero-downtime-update.md`](../../../docs/design/zero-downtime-update.md) — Redis pb 双向兼容 / 领奖回写保留 unknown fields(不变量 §16/§17)
- [`session-generation-rollout.md`](../../../docs/design/session-generation-rollout.md) — 会话现行性门(顶号后旧 JWT 失效)
- [`infra.md`](../../../docs/design/infra.md) — 端口规划 / 服务命名
