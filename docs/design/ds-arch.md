# Pandora UE DS 架构设计

> Hub DS / Battle DS 的运行时设计、Iris + GAS 配置、500 人 PvP 关键路径、跨 DS 切换流程。

## 0. ⭐ 协议边界:GAS / Replication vs gRPC

**这是 Pandora 最重要的架构原则,任何 AI 会话开始前必须读完本节再动 proto / UE 代码**。

### 0.1 战斗内 / 大厅内 = 全走 UE Replication + GAS

走 **UE Replication / GAS,不走 gRPC** 的事(全部):

| 类别 | 例子 |
|---|---|
| 玩家移动 | 位置 / 速度 / 朝向 / 跳跃 |
| 玩家动作 | 普攻 / 释放技能(Q/W/E/R)/ 闪现 / 走位 |
| 战斗状态 | HP / MP / shield / buff / debuff / 技能 CD |
| 命中判定 | 服务端 trace,客户端 GAS 预测 |
| 伤害计算 | GameplayEffect Modifier(服务端权威) |
| 技能升级 | GAS Ability LevelUp(进战斗后用 Gold 升 Q/W/E/R) |
| **出装 / 购买道具** | GAS Ability `UPandoraAbility_PurchaseItem`(扣金币 + 加属性 = GameplayEffect) |
| 表现层 | GameplayCue(特效 / 音效 / 飘字)走 Multicast |
| 大厅互打 | 跟战斗里完全一样,只是 Map 不同 |
| 大厅 NPC 触发碰撞 | 走 Overlap + 服务端权威 |

### 0.2 客户端两连接 + 后端 gRPC 协议矩阵

⚠️ **架构决策 2026-06-04 终版**(详见 `gateway-decision.md`):
- Client **不走 gRPC**(自己直接走),也不走 HTTP/JSON
- Client 走 **gRPC-Web over HTTP/2 TLS**(UE 5.8 FHttpModule 自带,自研 grpc-web frame 解析)
- **gRPC 标准协议只存在于后台服务之间**(Envoy → Kratos / 服务之间互调)
- **客户端连接最终值 = 2 条**(① NetDriver / ② FHttpModule)

#### Client 侧两条连接

| Caller → Callee | 协议 | 用途 |
|---|---|---|
| Client → **Envoy**(8443 HTTPS) | **gRPC-Web over HTTP/2 + TLS** | 所有业务请求(unary)+ 推送接收(server stream)|
| Client → Hub DS / Battle DS | **UE NetDriver**(UDP-like) | 仅游戏内同步(GAS / Replication / 30~60Hz tick)|

✅ 两条连接职责清晰:① 高频游戏 tick,② 业务 + 推送复用同一 gRPC-Web 长连。
✅ 客户端零第三方 SDK(UE 引擎自带 NetDriver + FHttpModule)。

#### 后台服务之间(走标准 gRPC)

| Caller → Callee | 协议 | 用途 |
|---|---|---|
| Envoy → Kratos 业务服 | 标准 gRPC unary | gRPC-Web 请求转标准 gRPC 后路由 |
| Envoy → push 服务 | 标准 gRPC server stream | 客户端订阅推送的长连转发 |
| matchmaker → ds_allocator | gRPC unary | 匹配成功调度战斗 DS |
| Hub DS → hub_allocator | gRPC **unary** Heartbeat **每 5s** | 单向心跳 + 接收控制指令(Kratos 支持双向流,但本期保留 unary 简化)|
| Battle DS → ds_allocator | gRPC **unary** Heartbeat **每 5s** | 同上 |
| 各 go 服务 → 各 go 服务 | gRPC unary | 内部 RPC(如 player ↔ data_service)|

#### 异步事件(走 Kafka)

| Caller → Callee | 协议 | 用途 |
|---|---|---|
| 各业务服务 → kafka | 生产推送事件 | push 服务消费 → server stream 推给客户端 |
| push → kafka | 消费推送 topics | 转发到客户端 stream |
| Battle DS → battle_result | 同步 gRPC-Web `ReportResult` | Guard + Redis active 授权后结算；生产不消费无凭据 `pandora.battle.result` |

#### 服务发现 / 配置

| Caller → Callee | 协议 | 用途 |
|---|---|---|
| 各 go 服务 ↔ etcd / k8s Service DNS | etcd 协议 / DNS | 服务注册 / 发现 / 配置中心 |
| Envoy ↔ k8s Service | DNS(STRICT_DNS) | Edge Gateway 路由后端 |

**已删除的反模式连接**:
- ~~Client → 各业务 go 服务 直连 gRPC~~(改走 Envoy gRPC-Web)
- ~~Client → 自研 pandora-gateway HTTP/JSON~~(2026-06-04 推翻,改 gRPC-Web)
- ~~Client → 自研 pandora-push WebSocket~~(2026-06-04 推翻,改 server stream over Envoy)
- ~~Client → Battle DS gRPC `BattleRuntimeService`~~(整个 service 删除,UE ServerRPC 即可)
- ~~Client → Hub DS gRPC `HubRuntimeService`~~(同上,proto/ds_runtime/ 已删)
- ~~Hub DS / Battle DS Heartbeat 双向流~~(改 unary 每 5s 主动调)

### 0.3 反模式禁令(写代码前必须背诵)

- ❌ **不要**为"玩家放技能"写 RPC(走 GAS Ability)
- ❌ **不要**为"玩家造成伤害"写 RPC(走 GameplayEffect)
- ❌ **不要**为"玩家移动"写 RPC(走 CharacterMovement Replication)
- ❌ **不要**为"出装 / 升技能"写 RPC(走 GAS Ability,购买装备 = 扣 Gold + 加 GameplayEffect)
- ❌ **不要**为"大厅 NPC 触发"写 tick RPC(走 Overlap)
- ❌ **不要**给 Replication 字段写 proto(UE 自己用 GENERATED_BODY 生成)
- ❌ **不要**让 Client 直连业务 go 服务的 gRPC(走 Envoy 统一入口)
- ❌ **不要**让 Hub DS / Battle DS 兼任业务网关(详见 `architecture-rejected-strict-ds-only.md`)
- ❌ **不要**为 Client ↔ DS 的业务通信写 gRPC service(用 UE ServerRPC)
- ❌ **不要**写 BattleRuntimeService / HubRuntimeService 之类的 service(典型反模式,proto/ds_runtime/ 已删)
- ❌ **不要在 UE 客户端拉 grpc-cpp 大依赖**(80MB+ / SSL 冲突 / UE 5.x 兼容性差;用 FHttpModule + 自研 grpc-web 客户端)
- ❌ **不要装第三方 UE gRPC 插件**(同上 5 个共性坑,见 gateway-decision.md §11)

**为什么这样设计**:

**①  游戏内 tick 同步**(战斗 30~60Hz / 大厅 20~30Hz)用 UE NetDriver:
- UDP + delta + AOI,专为游戏 tick 设计,500 人 hub 能扛
- GAS 自带客户端预测 / 回滚 / 网络优化,自己写 RPC 是重复造轮子
- gRPC(TCP + HTTP/2)做不了这个频率,**协议层不为高频 tick 设计**

**②  业务请求 + 推送**(组队 / 匹配 / 商店 / 段位 / 推送)用 gRPC-Web over HTTP/2:
- 业务请求 1~10 req/s/玩家,推送几次/局,**gRPC-Web 完全够,无性能问题**
- "gRPC 不适合 tick 同步" ≠ "gRPC 不适合业务请求",**两个完全不同的频率档**
- Envoy 用工业标准 grpc-web filter,UE FHttpModule 天然兼容
- 客户端用 FHttpModule + 自研协议解析(~3-5 天),包体不增加 80MB

**③  两条连接物理独立,故障域完全隔离**(2026-06-04 用户提醒补充):
- ② gRPC-Web 卡了 / 重连 / 断开 → 对 ① NetDriver(游戏同步)**零影响**
- ① NetDriver 断了(进入大厅 / 战斗切换)→ 对 ② **零影响**(② 长连保持)
- Envoy 崩 → 玩家继续战斗(看不到大厅 UI 更新但战斗正常)
- Battle DS 崩 → 玩家断战斗但 UI 业务通过 ② 正常用

**④  Battle DS 内部用 gRPC 也不影响 tick**(W5-W6 实现约束):
- Battle DS → ds_allocator Heartbeat = 5s/次,极低频
- Battle DS → battle_result(via kafka)= 1 次/局
- Battle DS → login.VerifyDSTicket = 1 次/玩家进入
- **所有 gRPC client 调用必须在独立 goroutine + 超时(5s)**,不阻塞 UE 主 tick 线程

### 0.4 什么时候打破 0.1 原则?

**几乎不打破**。唯一例外:跨服 PvP 跨 DS 同步(W4+ 后期需求,目前不在范围)。

如果未来某个特性看起来"需要 tick 同步但又是跨服",优先方案是:
- 把它做成"非实时"(异步消息触发表现)
- 或者拆成小局对战(走 ds_allocator 跨服分配 battle DS)

不要为单一特性引入"通过 gRPC 做 tick"的混合架构,会让性能模型崩塌。

### 0.5 开战前养成快照下发契约(W5 养成/背包)

养成系统(选英雄 / 属性加点 / 装备槽 / 天赋树)全部是**大厅态持久化**,落库在 player 服务;
战斗内的技能/出装/购买道具/即时用道具仍走 UE GAS(§0.1),后端**只在开战前下发一次快照**,
战后只接收结算(`battle_result`)。下发链路如下:

```
匹配成功(matchmaker)
   → ds_allocator 分配 Battle DS,给每个 player 生成 DS 票据(JWT exp 5min,不变量 §9.3)
   → 客户端持票据连 Battle DS
   → Battle DS 启动后,对每个进场玩家调用 player.GetLoadout(player_id)
       拿到 PlayerLoadout{active_hero_id, attributes[], unspent_attr_points,
                          equipment[{slot,item_config_id,instance_id,identified,attributes[]}], talents[]}
   → Battle DS 用快照初始化该玩家的 GAS(英雄 / 属性基础值 / 装备初始 GameplayEffect / 天赋被动)
   → 之后战斗内一切变化(升技能/再出装/买道具)走 GAS,不回写 player 服务
```

**契约要点(不变量,任何 AI review 必须守):**

1. **快照只读一次**:Battle DS 进场时拉一次 `GetLoadout`,战斗中**不再轮询** player 服务。
2. **快照是客户端可见结构**:`GetLoadout` 返回 `PlayerLoadout`(客户端可见结构),
   **不下发** `*StorageRecord` / 数据库整行(不变量 §14)。
3. **DS 不可信**:快照里的属性/装备只是"开战初始值",最终段位/经济结算仍以 `battle_result`
   服务端重算为准(不变量 §6),DS 上报的数值只作展示与回放。
4. **空值降级**:`active_hero_id=0`(玩家没选英雄)时 Battle DS 用默认英雄兜底,不阻断进场。
5. **拉取超时**:`GetLoadout` 必须在独立 goroutine + 5s 超时(§0「④」),超时用空快照兜底进场,
   并打 `trace_id` 告警,绝不阻塞 UE 主 tick 线程。
6. **装备/天赋只影响初始**:装备槽(`equipment[]`)和天赋(`talents[]`)在开战前转成
   一组初始 `GameplayEffect` / 被动 Ability;战斗内的买装/换装是 GAS 行为,与 player 服务无关。
7. **装备按唯一实例核权**:`SetEquipment` 新写必须携带 `instance_id>0`,player 以
   inventory `CheckInstancesOwned(instance_id+item_config_id)` 精确校验归属；`GetLoadout` 交付
   战斗快照前再批量复核一次，并从同一权威回包取 `identified+attributes[]`
   进快照，DS 不得在局内再查 inventory。实例已转移/丢失、exact pair 不符、
   详情缺失或鉴定不变量破坏时全部 fail-closed。000006 前只存
   `item_config_id` 的旧预设仍可由 `GetEquipment` 以 `instance_id=0` 展示并让玩家重选，
   但不得继续产生战斗初始效果，避免同配置多件装备时错用鉴定词条。
   `SetEquipment` 是全量替换：请求未列的槽位视为卸下，`equipment=[]` 删除该玩家全部
   预设行；数据库不保存“空槽行”，也不用 `instance_id=0/NULL` 表示新空槽。

滚动发布顺序固定为：

1. 先执行 `pandora_player/000006` expand migration（`instance_id` nullable）；
2. 先将同时回 `owned_instance_ids=2` + `owned_instances=3` 的 inventory 新版升级到全 fleet，
   确认旧 inventory 副本排空；
3. 再将新 player 升级到全 fleet。新 player 命中旧 inventory 时 SetEquipment 仍可用字段 2
   核权，但 GetLoadout 因缺字段 3 必须 fail-closed，所以 inventory 必须先全量；
4. 最后发布会提交 `instance_id` 并消费鉴定词条的 UE/DS 客户端。

旧 player 二进制会忽略新字段并写出 NULL，因此客户端不得抢跑；服务回滚时优先只回滚
二进制并保留 nullable 列/新 proto 字段，不要在线删除列/索引。

> 实现位置:`player.GetLoadout` 已在 W5 ① 落地(英雄 + 属性点);装备槽 / 天赋树字段在 W5 ②
> 扩展到同一 `PlayerLoadout`(见 `go-services.md` §2.2 与 player 服务 proto)。Battle DS 侧的
> 快照消费代码在 UE DS 仓库,跟随本契约实现。

#### ③ 局中异步事实上报(realtime progression,2026-07-20 拍板 / 2026-07-21 合入)

在「① 开局快照 + ② 局后上报」之外,Battle DS 可在战斗中把**事实事件**(击杀了怪物 X /
拾取了白名单内物品 Y)异步批量上报 `battle_result.ReportProgress`,由后端换算入账
(经验 → player.AddExperience,掉落 → inventory.GrantInstances),实现"打到即所得、
DS 崩溃已入账部分保住"。约束(逐条硬性):

1. **绝不阻塞 tick**:上报在独立异步任务,fire-and-forget + 本地有界缓冲重试;
   战斗逻辑不等待、不读取任何回包结果(ACK 只用于清缓冲)。
2. **DS 只报事实不报数值**:不上报"加多少经验 / 发什么奖";换算与发放全在 Go
   (怪物经验表 + 掉落白名单在 battle_result,等级曲线在 player)。
3. **每事件幂等**:事件键 = `(match_id, seq)`,seq 每场单调;at-least-once 重放零副作用
   (服务端 battle_progress_stream 水位去重,水位推进与发放出箱同一 MySQL 事务)。
4. **Go 不可用不影响战斗**:上报失败只缓冲重试,战斗照常;进度晚到不丢
   (尾窗残余风险见 realtime-progression.md §9)。
5. **单一权威路径**:一场对局的发放要么全走本通道,要么全走局后结算;服务端按自己的
   水位表强制二选一(结算事务打终局标记 + 掉落发放抑制),不信 DS 声明。

回退路径:battle_result `progress_enabled`(缺省 false)置 false → **新对局**不再开流
(ReportProgress 拒 ERR_INVALID_STATE,DS 停流回退局后结算);**已开流的进行中对局**
继续收流到结算(每场模式以水位行存在性固化,防"部分实时 + 结算抑制"丢奖),已入账部分不回滚。
服务端不认识的新事实类型同样返回 ERR_INVALID_STATE 整场停流(Go 先行 / DS 后行纪律),
并**持久化停流标记**(已知事实批也不得重新开流;通道关闭拒开流同样落标记固化本场
legacy 模式,中途重开配置不得晚开流);**已开流(水位>0)对局停流后没有结算兜底**——
结算掉落保持抑制,停流之后的拾取 / 经验永久丢失(2026-07-22 拍板明示,与 battle.proto /
realtime-progression.md §9 同步;该场景只该出现在违反发布纪律时,不为违纪场景做兜底)。
首批即停流(零实时入账,水位=0)无抑制,结算路径正常发放全部产出(零双发面)。
完整设计 / 幂等模型 / 验收矩阵见 `docs/design/realtime-progression.md`。

#### ④ 背包 journal 直写(bag 域 phase 2,2026-07-22 接线)

owner DS(hub 与 battle)可把**背包写操作**(拾取入包 pickup_grant / 邮件领取 mail_claim)
异步批量直写 `pandora.bag.v1.BagService/AppendJournal`(受信写者,CLAUDE.md §9.6 五要件),
并以 LoadBag(Admission 后 checkout)/ SaveCheckpoint(周期快照)维护随身组权威内存态。
约束(逐条硬性,与 ③ 同族):

1. **绝不阻塞 tick**:journal 单飞行单条批 + 有界超时 + 指数退避;拾取 ACK 门控语义不变
   (入包 = 已持久化,回执确认才提交预留销毁掉落物)。
2. **五要件逐笔满足**:①DS 凭据 Bearer(服务侧 DSCallbackGuard 验签)②owner 授权
   (服务端 QueryOwner,record.target 与调用方 pod/uid 全等 + ADMITTED + 租约在效)
   ③owner_epoch fencing(服务端解析代填,存储侧 CAS)④额度(单批/滑窗封顶)⑤journal 审计。
3. **每笔幂等**:uk(player, idempotency_key)+ 内容指纹;原批原 seq 重发,前缀确认。
4. **EPOCH_FENCED 即停写**:owner 迁移/失租后本实例永久停写,pending 按"确定未应用"
   释放认领;新 owner DS 重新 checkout(flush-before-fence 时序继承 §9.22)。
5. **killswitch 回退**:UE `PANDORA_BAG_JOURNAL_ENABLED`(缺省关)关闭后拾取回 ③ 通道、
   邮件领取回客户端直连,phase 0 双路径行为不变;服务端幂等各自防双发。

完整设计见 `docs/design/bag-domain.md`(§2 权威模型 / §7 邮件三段式领取 / §10 迁移路径)。

### 0.6 ⭐ 延迟不变量:局外系统放 Go = 零战斗延迟(2026-07-08)

**结论**:把角色养成 / 装备 / 属性 / 天赋 / 背包这类**局外(meta)系统**放 Go,
**不会增加哪怕 1ms 的战斗延迟**。这是架构决定的,不是"调优"出来的。

**为什么(三条物理事实):**

1. **两条链路物理独立**:客户端 → Go 的大厅连接(gRPC-Web / HTTP/2,§0.2)与客户端 → DS
   的战斗连接(UE NetDriver / UDP,§0.2)是**物理上分开的两条链路**,不共享带宽预算、
   不共享故障域(§0.3 ③)。大厅侧再慢也压不到战斗帧。
2. **DS 帧循环里没有对 Go 的同步调用**:战斗 30~60Hz tick 全走 GAS / Replication(§0.1),
   DS → Go 只剩三种极低频异步调用(Heartbeat 5s、`GetLoadout` 开局一次、`battle_result`
   局后一次),且全部**独立 goroutine + 5s 超时,绝不阻塞 UE 主 tick 线程**(§0.3 ④)。
3. **边界守住就永不退化**:唯一会真正拖慢战斗延迟的错误做法,是**让 DS 在战斗中同步 RPC
   大厅服务**(边打边查背包 / 边算段位 / 边写档案)。只要守住 **"开局快照 + 局后上报"**
   这条边界(§0.5),战斗中 DS 对 Go 的同步依赖恒等于零,延迟退化就永不发生。

**一句话红线**:局外功能尽管往 Go 加,延迟安全的充要条件只有一个 ——
**DS 战斗中绝不同步调用大厅服务**。任何"战斗内实时读写 player/inventory/economy"的需求,
都必须改造成"开局快照"或"局后异步上报",否则直接推翻(参照 §0.4 打破原则的门槛)。
战斗中对大厅服务的**异步事实上报**(§0.5 ③,2026-07-20 拍板)与**背包 journal 直写**
(§0.5 ④,2026-07-22 bag 域 phase 2)不在禁列,但必须各自满足其硬性约束;
"即时"永远靠异步实现,不靠放开同步调用。

---

## 1. DS 双形态对比

| 维度 | Hub DS(大厅) | Battle DS(战斗) |
|---|---|---|
| Map | `HubMap`(单城镇 ~1km²) | `BattleMap`(MOBA 三路一河) |
| 玩家容量 | **500 人/实例**(目标) | 固定 10 人(5v5) |
| 生命周期 | 常驻 + 滚动热更 | 一局一进程,~25min,结束销毁 |
| Tick rate | 20~30 Hz | 30~60 Hz |
| GameMode | `APandoraHubGameMode` | `APandoraBattleGameMode` |
| GameState | `APandoraHubGameState` | `APandoraBattleGameState` |
| Replication | **Iris + AOI 网格**(强制) | Iris(默认即可) |
| GAS | 启用,大厅可放技能可互打 | 启用 |
| 死亡处理 | 复活点重生 | 等待复活计时 |
| 持久化 | 实时(玩家位置、状态) | 全内存,结算时一次 kafka 落库 |
| 接 go 服务 | 频繁(NPC / 商店 / 交易 / 组队 / 匹配) | 少量(开始 / 结束 / 异常) |
| Agones Fleet | `pandora-hub-fleet`(常驻) | `pandora-battle-fleet`(allocate on demand) |

## 2. UE 工程模块划分

```
C:/work/Pandora/Source/
├── Pandora/                  # 客户端
│   ├── PandoraGameInstance   # 登录流程、跨 DS 切换
│   ├── PandoraPlayerController
│   ├── PandoraHUD
│   └── UI/                   # UMG 蓝图
│
├── PandoraShared/            # 客户端 + DS 共用
│   ├── Auth/
│   │   ├── TicketVerifier    # JWT 票据校验
│   │   └── DSCredentials
│   ├── Network/
│   │   └── GrpcClient        # 用 grpc-cpp 包一层
│   ├── GAS/
│   │   ├── PandoraAttributeSet
│   │   ├── PandoraAbilitySystemComp
│   │   ├── PandoraGameplayAbility
│   │   ├── PandoraGameplayEffect
│   │   └── PandoraGameplayCue
│   ├── Character/
│   │   ├── PandoraCharacterBase
│   │   └── HeroData
│   └── Proto/
│       └── Generated/        # 从 Pandora/proto/ 生成
│
├── PandoraHubServer/         # 大厅 DS 专属
│   ├── HubGameMode
│   ├── HubGameState
│   ├── HubPlayerController
│   ├── HubCharacter
│   ├── AOI/
│   │   └── HubAOIGrid        # 自研 AOI 网格(500 人必须)
│   ├── Replication/
│   │   └── HubReplicationGraph (退路方案)
│   ├── Service/
│   │   ├── NPCService
│   │   ├── ShopService
│   │   ├── TransferService
│   │   └── EnterBattleService
│   └── Agones/
│       └── HubAgonesIntegration
│
└── PandoraBattleServer/      # 战斗 DS 专属
    ├── BattleGameMode
    ├── BattleGameState
    ├── BattlePlayerController
    ├── BattleCharacter
    ├── Match/
    │   ├── MatchPhaseController
    │   └── MMRReporter
    └── Agones/
        └── BattleAgonesIntegration
```

## 3. Iris vs Replication Graph 决策

### 3.1 默认走 Iris

UE 5.8 时代 Iris 应该已经 production-ready,**默认开 Iris**:

```ini
; Config/DefaultEngine.ini
[/Script/IrisCore.ReplicationSystem]
bEnableIris=True

[SystemSettings]
net.Iris.UseIrisReplication=1
```

**Iris 优势**(对 500 人 PvP 是关键):
- 数据驱动,不用 PreReplication 钩子
- 内置 prioritization
- 支持 partial state
- 内置 NetCullDistance + 自定义 filter

### 3.2 GAS 在 Iris 下的注意

GAS 早期是为 RepGraph 写的,Iris 适配在 5.5+ 才完整。已知坑:
- `FActiveGameplayEffectsContainer` 用 Fast Array Serializer
- `FGameplayAbilitySpec` 同上
- Prediction Key 跟 Iris 的 frame 模型对接

**风险缓解**:
- W5(GAS 集成阶段)留 1 周 buffer
- 退路:回退 Replication Graph(已有大量 GAS + RepGraph 案例)

### 3.3 Replication Graph 退路方案

预留 `Source/PandoraHubServer/Replication/HubReplicationGraph.h`,如果 Iris 不行就启用:
- 4 个 Connection Graph Node:
  1. `UReplicationGraphNode_GridSpatialization2D`
  2. `UReplicationGraphNode_DormancyNode`
  3. `UReplicationGraphNode_AlwaysRelevant`
  4. 自定义 `UPandoraHubVisibilityNode`

## 4. 500 人 Hub PvP 关键路径

### 4.1 网络预算

**单玩家上行**:目标 ≤ 20 KB/s
**单玩家下行**:目标 ≤ 100 KB/s
**总入站**:500 × 20 = 10 MB/s ≈ 80 Mbps
**总出站**:500 × 100 = 50 MB/s ≈ 400 Mbps

⚠️ **千兆网卡上行接近极限**,生产要走万兆 + 多机分片。

### 4.2 AOI 网格设计

**网格尺寸**:50m × 50m
**每格典型容纳**:5~30 人
**关注半径**:周围 9 格 = ~50~270 人

**复制规则**:
- 角色完整状态:仅 9 格内复制
- 心跳信号:18 格半径
- 全局事件(聊天 / 系统):全图(单独 channel)

### 4.3 技能命中判定

**禁止用 `OverlapMultiByObjectType`**(500 人时遍历开销爆炸)。

**自研空间索引**:`FHubSpatialIndex`,每 tick 维护一个 50m 网格的 `TMap`,O(1) 查格 + O(K) 遍历(K ≤ 30)。

```cpp
class FHubSpatialIndex {
public:
    void AddActor(APandoraCharacter* Actor);
    void RemoveActor(APandoraCharacter* Actor);
    void UpdateActor(APandoraCharacter* Actor);
    TArray<APandoraCharacter*> QueryRadius(FVector Center, float Radius);
private:
    TMap<FIntVector, TArray<TWeakObjectPtr<APandoraCharacter>>> Grid;
    static constexpr float CellSize = 5000.f;  // 50m
};
```

#### 4.3.1 命中延迟补偿(Server-side rewind)

客户端 GAS 预测只负责本地手感,**最终命中仍由 Hub DS / Battle DS 服务端权威判定**。为避免"客户端看见打中了,包到服务器时目标已经走远"的误判,DS 需要保存最近一小段玩家碰撞体历史,收到技能/普攻请求后按客户端开火时刻做回溯 trace:

1. 客户端请求只上报开火时间/tick、技能 id、开火位置/朝向等输入,不能上报"我命中了谁"作为事实。
2. DS 用服务器接收时间、玩家 RTT/时钟偏差估算开火对应的服务器时间点,从历史环形缓冲取目标当时的 capsule / hitbox。
3. 对历史姿态做插值后由服务端重新 raycast / shape trace / 网格查询,命中后再走服务端 GameplayEffect 结算伤害。
4. 最大回溯窗口必须有硬上限,建议先按 `200ms ~ 300ms` 起步,极端网络最多不超过设计上限;超过窗口的请求按当前服务端状态判定或直接拒绝补偿。

副作用是被打方可能感觉"已经躲到墙后仍被打中"。这是延迟补偿的正常代价,必须用最大回溯窗口、RTT 异常剔除、输入时间单调校验、技能类型白名单来收敛;严禁为了照顾高延迟玩家无限回溯,否则会破坏躲避/掩体体验与反作弊边界。

### 4.4 技能限流

**预算**:每 tick 最多处理 50 个技能激活,超出排队下一 tick(优先级队列)。

```cpp
TPriorityQueue<FAbilityActivationRequest> PendingAbilities;
constexpr int32 MaxAbilitiesPerTick = 50;
```

优先级:
1. 玩家正在被攻击 → 防御类优先
2. 队友受击 → 治疗优先
3. 普通主动技能
4. 自我增益

### 4.5 移动同步降频

- 0~30m:30Hz 完整同步
- 30~50m:15Hz
- 50~100m:5Hz(只位置)
- > 100m:每秒 1 次心跳

## 5. GAS 框架设计

### 5.1 Attribute 清单(初版)

`UPandoraAttributeSet`:

```cpp
ATTRIBUTE_ACCESSORS(MaxHealth)
ATTRIBUTE_ACCESSORS(Health)
ATTRIBUTE_ACCESSORS(MaxMana)
ATTRIBUTE_ACCESSORS(Mana)
ATTRIBUTE_ACCESSORS(AttackDamage)
ATTRIBUTE_ACCESSORS(AbilityPower)
ATTRIBUTE_ACCESSORS(Armor)
ATTRIBUTE_ACCESSORS(MagicResist)
ATTRIBUTE_ACCESSORS(MoveSpeed)
ATTRIBUTE_ACCESSORS(AttackSpeed)
ATTRIBUTE_ACCESSORS(CritChance)
ATTRIBUTE_ACCESSORS(CritDamage)
ATTRIBUTE_ACCESSORS(CooldownReduction)
ATTRIBUTE_ACCESSORS(LifeSteal)
ATTRIBUTE_ACCESSORS(Tenacity)

// Meta(只在服务端临时计算)
ATTRIBUTE_ACCESSORS(IncomingDamage)
ATTRIBUTE_ACCESSORS(IncomingHealing)

// 经济(战斗 DS only)
ATTRIBUTE_ACCESSORS(Gold)
ATTRIBUTE_ACCESSORS(Experience)
```

### 5.2 Ability 类型

`UPandoraGameplayAbility` 子类:

| 类 | 说明 | 例子 |
|---|---|---|
| `UPandoraAbility_Passive` | 被动 | 嗜血、暴击 |
| `UPandoraAbility_Targeted` | 单体瞄准 | 普攻、冲刺 |
| `UPandoraAbility_Skillshot` | 技能弹道 | 直线技能、AOE |
| `UPandoraAbility_AoE` | 范围 | 大招 |
| `UPandoraAbility_Channel` | 引导 | 持续治疗 |
| `UPandoraAbility_Movement` | 位移 | 闪现、突进 |

### 5.3 GameplayCue(表现层)

服务端**只算逻辑**,客户端**只播表现**。Cue 走 Multicast(AOI 过滤后)。

```
GameplayCue.Skill.Fireball.Hit       → 火球命中音效+特效
GameplayCue.Skill.Heal.Apply         → 治疗光环
GameplayCue.Character.Death          → 死亡表现
GameplayCue.UI.DamageNumber          → 飘字
```

### 5.4 Hero DataAsset

```cpp
class UPandoraHeroData : public UPrimaryDataAsset {
    int32 HeroId;
    FText DisplayName;
    UCurveTable* AttributeGrowth;
    TArray<TSubclassOf<UGameplayAbility>> StartingAbilities;
    TArray<TSubclassOf<UGameplayEffect>> StartingPassives;
    USkeletalMesh* Mesh;
    UAnimBlueprint* AnimBP;
};
```

## 6. 跨 DS 切换流程

### 6.1 Hub → Battle

```
Client (在 Hub DS)
   │ 1. 点开始匹配
   ▼
Hub DS → matchmaker (gRPC StartMatch)
   ▼
匹配成功 → matchmaker 通知 Hub DS
   ▼
Hub DS → Client (RPC: BattleReady{addr, ticket})
   ▼
Client:
   - SaveGame
   - UPandoraGameInstance::TravelToBattle(addr, ticket)
   - DisconnectFromHub → ConnectToBattle
```

**关键**:Hub DS 保留玩家"占位" 30s,Battle DS 没收到玩家连入则告警。

### 6.2 Battle → Hub

> **待做(2026-08-12 提出,未拍板)**:对局要切成**准备 / 战斗 / 结算**三段,「退出场景」应挂到
> 结算阶段结束这个边界上,而不是现在这样由客户端在 `BattleEnded` 之后自行发起。需求、现状核对
> 与待拍板清单见 [`match-phase-lifecycle.md`](match-phase-lifecycle.md);拍板前本节流程不变。

```
Battle DS 战斗结束
   ▼
Battle DS:
   - 同步 ReportResult(DS callback token；后端重算并事务落库)
   - 可发 state="ended" 的 BattleHeartbeat/客户端 BattleEnded（只影响通知体验）
   - 给客户端 RPC: BattleEnded{result, hub_ds_addr, hub_ticket}
   - 留短暂窗口给客户端回大厅
   - StopBattleHeartbeat
   - 可请求 Agones Shutdown；服务端持久 release outbox 是权威回收兜底
```

**当前实现补充**：正常结算采用服务端驱动的两阶段 durable release。`battle_result` 把结算与原始
Model-B proof 同一 MySQL 事务提交；宽限窗后调用 `ReleaseBattle(reason=completed)`，由
`ds_allocator` 建永久 Redis terminal/receipt 墓碑并按 UID precondition 回收 GameServer，随后 MySQL
CAS `released_at_ms`。下一轮 `reason=completed-finalize` 只给 exact-proof 墓碑恢复 TTL，不再删除
Kubernetes，最后仅删除 released outbox。响应丢失/DB mark 或 delete 失败均按 durable phase 重放；
match-id-only Model-B release 拒绝。异常路径仍由 `ds_allocator` 心跳 sweep 兜底：
Battle DS 超过 15s 未心跳会标 `abandoned`、释放 pod,并投递 `pandora.ds.lifecycle` 给
`battle_result` 做段位回滚补偿。完整回收生命周期见 [`agones-dev.md`](agones-dev.md) §2.3
「Battle DS 回收生命周期」。

### 6.3 Hub 跨分片切换

```
Client 走到传送点 → Hub DS A
   ▼
Hub A → hub_allocator: TransferHub
   ▼
返回 hub_b_addr + ticket
   ▼
Client:TravelToHub(2 秒内完成)
```

**优化点**:客户端预加载 Hub B Map(后台异步),ticket 提前签发。

## 7. Agones 集成

### 7.1 SDK 接入点

每个 DS 进程必须:
- **启动后**:`SDK::Ready()`
- **每 5s**:`SDK::Health()`(超时 15s 视为崩溃)
- **退出前**:`SDK::Shutdown()`
- **玩家进出**:`SDK::Alpha::PlayerConnect/Disconnect()`(可选)

### 7.2 Agones Fleet 配置(开发期)

```yaml
# deploy/k8s/hub-fleet.yaml
apiVersion: "agones.dev/v1"
kind: Fleet
metadata:
  name: pandora-hub-fleet
  namespace: pandora
spec:
  replicas: 2
  scheduling: Packed
  template:
    spec:
      ports:
      - name: default
        portPolicy: Dynamic
        containerPort: 7777
      health:
        initialDelaySeconds: 30
        periodSeconds: 5
        failureThreshold: 3
      template:
        spec:
          containers:
          - name: pandora-hub
            image: pandora-hub-ds:latest
            resources:
              requests: { cpu: "2", memory: "4Gi" }
              limits:   { cpu: "4", memory: "8Gi" }
```

```yaml
# deploy/k8s/battle-fleet.yaml
apiVersion: "agones.dev/v1"
kind: Fleet
metadata:
  name: pandora-battle-fleet
spec:
  replicas: 5      # 5 个空闲,allocate on demand
```

## 8. DS 监控指标

每个 DS 暴露 `:9100/metrics`(Prometheus):

```
pandora_ds_player_count{ds_type="hub",pod="..."}
pandora_ds_tick_duration_ms_bucket{ds_type,pod,le="..."}
pandora_ds_replication_packet_size_bytes_bucket{ds_type,pod}
pandora_ds_aoi_grid_max_pop{pod}
pandora_ds_ability_activations_total{ds_type,pod}
pandora_ds_kafka_send_total{topic}
pandora_ds_grpc_call_duration_ms_bucket{service,method}
```

## 9. 容错与崩溃恢复

### 9.1 DS 崩溃

- Battle DS 业务心跳超过 15s 未上报 → `ds_allocator.RunHeartbeatSweep` 标 `abandoned`
  → `alloc.Release(pod)` 回收 Agones GameServer / Local 进程。
- **空场回收(2026-07-06)**:对局活跃但全员掉线不归 / 客户端从未连入 → 双层回收,
  DS 侧空场计时器自结算为主(UE 仓库,2~3min)、`ds_allocator` 按 `player_count==0` 持续超
  `empty_battle_timeout`(默认 5m)判 abandoned 兜底;契约见 [`agones-dev.md`](agones-dev.md) §3.2。
- `ds_allocator` 投递 `pandora.ds.lifecycle{phase=ABANDONED}` → `battle_result` 幂等补偿:
  - 战斗 DS:写 abandoned 战绩 / 段位回滚补偿,不信任 DS 自报的异常结算。
  - 大厅 DS:由 `hub_allocator` 的心跳 sweep / draining 机制和 `player_locator` 自愈处理。
- Agones SDK health 仍用于 GameServer Ready/Unhealthy,但 Pandora 业务补偿以 allocator 的
  业务 Heartbeat 和 Redis 镜像为准。

### 9.2 客户端崩溃

- DS 检测 NetDriver 超时 60s → 销毁玩家 actor + 通知 player_locator
- 客户端重启后统一从 `Login`/`GetResumeContext` 恢复,但目的地由服务端权威 placement 决定,不是一律回 Hub:
  - locator+roster 证明玩家仍处于 active `BATTLE(match_id)` → 现签新 Battle 票,直连原 Battle DS。
  - 由 terminal/placement 权威证明已非 BATTLE → 按选角/Hub 分配链取得新 Hub 地址与一次性票据。
  - placement 不可判定 → fail-closed 返回 unavailable,恢复后重试;不得降级签 Hub 票。短 TTL locator/presence
    只表示网络在线情况；持久版本化 placement 缺失或损坏一律是 UNKNOWN，不是非 BATTLE 证明。
- 客户端断线不等于 Battle DS 心跳丢失。健康 Battle DS 会按整局 roster 继续续租 BATTLE;客户端本地
  30s 重连计时不能作为“对局已结束”的依据。
- UE 侧 GameInstance 级 Coordinator 是唯一 DS `ClientTravel` writer；前台恢复、无 PlayerController、push
  断开、network/travel failure 和 session 过期都会使旧 generation 失效并重查权威 route。无可用凭据时
  转显式登录页，而不是无限重试旧 token 或自行回 Hub。
- 任意时点断线/切后台的完整审计、已修 A~J 反例和发布前验收矩阵见
  [`battle-reconnect.md`](battle-reconnect.md) §7。2026-07-15 状态为**代码层闭环、真实环境故障注入待验**；
  未跑移动端+Redis+K8s/Agones+真实 UDP 矩阵前不得宣称生产已验证。

### 9.3 网络抖动

- UE 内置 RTT 探测 + 客户端预测/回滚
- 服务端权威,客户端预测错了就回滚
- GAS 的 Prediction Key 在 Iris 下要做适配

## 10. W1 D5-D6 写代码范围（已完成）

本节是历史骨架范围记录。UE DS 骨架代码已完成,包含 Hub/Battle GameMode 与 Agones SDK 接入；后续不再把 D5-D6 作为待办。

### `PandoraShared`
- [x] `TicketVerifier` / DS ticket 校验基础
- [x] `GrpcClient` / gRPC-Web 后端调用包装
- [x] `PandoraAttributeSet`(声明属性,初始值)
- [x] `PandoraAbilitySystemComponent` 空类继承
- [x] `PandoraCharacterBase` 空类(挂 ASC + Movement)
- [x] Build.cs

### `PandoraHubServer`
- [x] `APandoraHubGameMode`(Agones Ready/Health + 后端心跳)
- [x] `AHubGameState`
- [x] `AHubPlayerController`
- [x] `AHubCharacter` 继承 `PandoraCharacterBase`
- [x] Hub Agones integration
- [x] Build.cs

### `PandoraBattleServer`
- [x] 同上结构,GameMode 不同

### Config
- [x] `DefaultEngine.ini`:开 Iris、设 NetCullDistance、设 tick rate
- [x] `DefaultGame.ini`:GameMode 默认绑 HubGameMode

### 验收标准
1. UE 编辑器编译通过
2. Linux Server target 交叉编译通过
3. Package 出 Linux 二进制
4. 本地 hub/battle DS 骨架可启动并进入 GameMode / Agones 心跳链路
