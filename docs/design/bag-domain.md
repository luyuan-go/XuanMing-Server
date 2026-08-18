# 背包域设计(权威跟随 owner + journal/checkpoint + 活动背包代际)

> 状态:**方向已拍板(2026-07-21,用户逐项确认);phase 1 服务端已落码(2026-07-21 深夜,
> 用户指示开工)**:`proto/pandora/bag/v1/bag.proto` 契约已落文件(取代此前"暂不落 .proto"
> 的保守决定;owner_epoch/generation/seq 字段形状不依赖 owner authority 内部实现,先定契约),
> `pandora_bag` 五表 schema、BagRepo(epoch CAS fencing / generation fencing / 幂等 / 同事务
> 段变更 / 滑窗额度 / journal sweep)、BagUsecase、BagService 已实现并由 inventory 进程承载
> (bag.dsn 空 = 不启用,安全默认);Envoy 对 /pandora.bag.v1/ 整前缀 403。
> **pb 待 Codex proto_gen 重生(bag.proto + errcode.proto),重生前 go build 红**;
> MySQL 集成测试门控于 PANDORA_TEST_MYSQL_DSN。
> phase 0(拾取 ACK 门控 + 背包预留制)已落码待 UE 编译,见 `realtime-progression.md`。
> 本文档是 CLAUDE.md §9.6(受信写者五要件)的落地设计;owner authority(§9.22)仍是
> **phase 2 写权威切换**的硬前置(要件② owner 授权在其落地前只有 epoch 单调 CAS 的
> 存储侧机制,没有"该 DS 确实 own 该玩家"的权威校验,故 phase 1 不得接入 DS 生产写路径)。

## 0. 需求与结论

产品 MOBA → MMO 化,已确认的需求:

1. **局中已得掉落绝对不丢**(已由拾取 ACK 门控实现:入包 = 已持久化,战斗背包 ⊆ 后端已入账);
2. **玩家背包权威未来跟随 owner DS**(在线时 DS 内存权威,不再"每次操作打后端 RPC");
3. **多种背包并存**:战斗局内背包(实时)、身上背包、仓库、装备栏、**临时活动背包**;
4. **活动背包类型会重用**:活动结束时清空该类型背包的全部物品,同一类型给下一个活动复用;
5. 邮件 / 拍卖等离线系统必须与 DS 权威背包共存;
6. 未来可回摆 MOBA(掉落随局清零,`AreDropsPersistent()` 开关已落地)。

**结论**:新建**独立背包域**(协议 `pandora.bag.v1`),与 battle 域、经济域三域定界;
背包按**驻留分层**——随身组(身上背包/装备栏/临时格)在线时 checkout 进 owner DS 内存权威,
仓库与活动背包**后端驻留**(存储侧权威,DS 只发起操作不持有状态,2026-07-21 用户修正);
写入分 journal(同步)/ checkpoint(异步)两层;活动背包用**代际(generation)**实现类型重用
与瞬时清理。

## 1. 三域定界

| 域 | 协议 | 管什么 | DS 关系 | 信任模型 |
|---|---|---|---|---|
| battle 域 | battle.proto | 事实上报(经验等需服务端换算的)+ 局后结算(MMR/战绩/补偿) | DS 上报事实 | DS 不可信,服务端换算(§9.6 第一款) |
| **背包域(新)** | pandora.bag.v1 | 背包存储:journal / checkpoint / 加载交接;活动代际 | **DS 直连受信写,无转发层** | 五要件(§9.6 第二款) |
| 经济域 | inventory.proto | 货币、拍卖 escrow、交易结算、幂等账本 | 不直接对 DS | 玩家 JWT(客户端 RPC,收缩中)+ 内网系统调用 |

**拍板:货币留经济域,物品堆叠与装备实例迁背包域。**
理由:货币无格子/背包语义,拍卖 / 交易高频扣转且 escrow 机制已成熟;背包域只管"格子里的东西"。
DS 侧金币显示是投影(经济域推送/查询),不进背包权威。装备实例的鉴定/词条等"物品实例数据"
随实例迁背包域;鉴定的**随机 roll 仍在服务端**(派生数值不信 DS)。

三个协议的语义边界(拆两次讨论的结论):

- `ReportProgress`(battle.proto)= 事实语言,服务端换算——**不搬家**;迁移完成后它的掉落
  入账职责退役,经验等换算类事实保留;
- `pandora.bag.v1` = 背包写操作语言(受信写者)——**新生,不是 ReportProgress 挪窝**;
- `inventory.proto` 的 GrantItems / GrantInstances 系统入账调用链分阶段退役(§10)。

## 2. 权威模型:随身组权威跟随 owner,仓库/活动背包后端驻留

背包按驻留分两组,授权模型一致(五要件),但状态位置不同:

- **DS 驻留组(随身背包/装备栏/临时格)**:玩家被某 DS own(owner authority,§9.22)期间,
  该 DS 内存是这些段的**唯一可写副本**;变更由该 DS 发起并写背包域(journal/checkpoint)。
- **后端驻留组(仓库/活动背包,2026-07-21 用户修正)**:存储侧(背包域服务)是唯一权威,
  **不 checkout 进 DS 内存**。操作仍由 owner DS 发起(玩家在线才能开仓库/活动界面,授权
  走五要件),但 DS 只发 RPC + 持有只读视图,没有 flush/交接负担。低频 UI 操作,RPC 延迟
  可接受。好处:checkout/flush 面收窄到随身组,DS 崩溃影响面更小,活动切代不涉及 DS 状态。
- **跨组转移(仓库⇄身上)**:一条 journal 记录、背包域服务**单事务**完成两侧(它同时拥有
  journal 与仓库存储,无跨服务 saga):仓库→身上先在 DS 侧预留随身容量(预留制),ACK 后
  转正;身上→仓库由 DS 扣出 + 存储侧入库同事务。
- **离线**:存储中的静止态就是背包;无活副本,无冲突。离线改包(GM 清货/赛季清算)必须
  校验"该玩家当前无 active owner lease",或统一走邮件。
- **迁移 / 下线交接(flush-before-fence)**:旧 owner 必须在失租安全窗内完成 flush
  (checkpoint + journal 尾部);新 owner 加载时读到的必然是终态,因为旧 epoch 的写已被
  fencing 拒绝。核心时序继承 §9.22:
  `旧 DS 最晚可写时间 < 旧 lease 到期 < admit_not_before < 新 DS 首次加载`。
  同步 journal 事件(§3)不受 flush 窗口影响——它们本来就已落库;flush 只影响异步态的新鲜度。
- **五要件**(CLAUDE.md §9.6,每笔背包域写全部满足):
  ①身份(DS JWT / writer epoch);②owner 授权(服务端按 owner authority 校验"该 DS 当前
  持有该玩家",禁止只验"是合法 DS");③fencing(写带 `owner_epoch`,存储侧 CAS 校验,
  失租旧写一律拒);④额度(journal 层速率与单场/单时段上限,反作弊封顶);⑤审计(journal
  本身即流水,可发现可修复)。

## 3. journal / checkpoint 分层

**判据:操作效果是否被"本人可回档范围"之外观察到。**被观察到 → 回档圆不回来 → journal;
纯个人态 → 回档自洽(物品和效果一起回退)→ checkpoint。

| 层 | 写法 | 事件清单 | 丢失语义 |
|---|---|---|---|
| journal(同步,落库成功操作才算完成) | 逐笔 append,每玩家单调 `journal_seq`,幂等键,owner_epoch fencing | 拾取入包(现门控语义平移)、邮件附件领取、交易/给予扣转、拍卖冻结与释放、**仓库⇄身上转移**(跨驻留组,存储侧同事务)、活动背包入账与代际结算 | 零丢失(= 现在的 ACK 门控保证) |
| checkpoint(异步,周期快照,仅覆盖 DS 驻留组) | 整包 pb blob,带 `last_journal_seq`,周期(建议 5~10s)+ 关键点同步(owner 迁移/撤离/下线) | 格子摆放、堆叠合并整理、装备穿戴/卸下、耐久、个人消耗(用药) | DS 崩溃回档到最近 checkpoint(秒级,个人态自洽,经典 MMO 语义) |

后端驻留组(仓库/活动背包)没有 checkpoint 概念:它们的每笔变更都是存储侧事务
(journal 或 RPC 直改),天然零丢失、无回档。

### 3.1 容量判定与预留(判定点必须与权威同址,2026-07-21 追问定稿)

"目标段满不满"的判定点必须和该段的权威同址;"判定与提交的原子性"在两种驻留下由
**两种不同机制**实现,预留制不是可被"Go 权威"替代的冗余:

| 目标段驻留 | 谁能回答"此刻满不满" | 判定+提交原子性靠什么 | 预留 |
|---|---|---|---|
| 后端驻留(仓库/活动段) | 存储侧(权威本体在库里) | **MySQL 事务**(容量校验与入段同事务,满了整批拒) | 不需要 |
| DS 驻留(身上背包/战斗背包) | **只有 owner DS 内存** | **单写者内存预留 → 持久化 ACK → 转正**(FMyBag 预留制) | 必须 |

DS 驻留段为什么 Go 判不了:在线时权威在 DS 内存,存储侧只有**滞后的 checkpoint 视图**
——最近几秒的格子变动、整理、穿脱、个人消耗都还没落库,存储侧回答"满不满"必然基于
过期状态。判定只能在 DS 做;而 DS 判定与持久化确认(journal ACK)之间隔一次异步往返,
其它写源(另一次拾取/邮件领取/交易到账)可能在这个窗口挤占空间——预留制就是补这条缝:
发起时在单写者游戏线程内**原子占位**,ACK 后转正、失败释放,"总占用 = 已提交 + 已预留
≤ 容量"随时成立(§22 禁"先查再存";UE 侧实现与回归测试见 realtime-progression.md §7)。

推论(邮件领取的目标段决定链路形态,§7):领取进仓库 ⇒ 全后端闭环,无预留;
领取进身上背包 ⇒ DS 预留 + claim + journal。这是产品决策,两条链路机制都已具备。

**恢复 = 加载最近 checkpoint + 重放其后的 journal 尾部。**因此:
journal 必须可重放(幂等、自含、按 seq 有序);checkpoint 必须记录覆盖到的 `last_journal_seq`。

### 3.2 重放语义:资产守恒 + 布局容忍(2026-07-22 拍板,decision-revisit-bag-replay-semantics.md)★

重放失败按两类分别处置,**不得混淆**:

1. **数据完整性问题 fail-closed 不变**:journal 缺口 / 截断 / 指纹不符 = 唯一恢复数据被
   破坏,拒载 + 告警(INC-20260722-003,LoadBag 已实现)。
2. **容量不符 = 布局回档的预期物理现象,不拒载**:checkpoint 类操作(整理释放格子、用药
   腾格、穿脱)不进 journal,崩溃回档后布局比 journal 接受时刻旧;重放 grant 放不下时
   **溢出进临时格(bag_type 3)**,临时格也满则该背包进入**超容态——只出不进**(扣减 /
   转出照常,新开格拒,低于容量自动恢复),全程告警计数。溢出量有界:最多为 checkpoint
   周期内的 journal 尾部(秒级窗口)。初版"重放遇到容量不符 fail-closed"作废——那会让
   玩家资产把自己挡在场外(违反不变量 20)。

**组级寻址**:随身组(0/2/3)的 journal op 一律以**随身组整体**为寻址粒度(instance_id /
config+count),不绑段与格位;重放按组解析(例:卸下装备〔checkpoint 类,回档后仍在
装备栏〕→ 交易卖出〔已 journal〕,重放从装备栏找到该实例扣除,资产结局正确)。

**守恒不变量(重放永不因业务状态失败的证明)**:随身组个人态操作不创造资产(资产只经
journaled 入账进组),回档只会让"组内可扣总量"变多——journaled 扣减必然仍可满足,
journaled 入账必然可放置(给定溢出容忍)。

**checkout 失败按 WAIT 语义降级**:LoadBag 失败不得成为无兜底的进场硬门,也不得静默
卡死——有界重试 + §9.23 可见 WAIT,短故障 = 进场稍慢,长故障 = 停在可重试 UI,不清
session;背包域故障的爆炸半径不得放大为"全服进不了场"。

**整理后即时 checkpoint(优化,非正确性依赖)**:MergeAndCompact 等释放容量的操作后
触发一次异步 checkpoint,缩小溢出窗口;正确性仍由本节语义保证。

## 4. 存储模型(`pandora_bag` 库)

背包本体的主访问模式是"整包加载(checkout)/ 整包快照(checkpoint)",选 **pb blob 快照 +
行式 journal** 组合(§5.8 快照场景,§9.17 双向兼容纪律适用于 blob):

```text
bag_meta        player_id PK | owner_epoch | last_journal_seq | updated_at_ms
                (fencing 锚点:journal 写与 checkpoint 写都 CAS 本行 owner_epoch)
bag_checkpoint  player_id PK | snapshot(pb BagStorageRecord,仅 DS 驻留组段) | covered_journal_seq | updated_at_ms
bag_section     player_id | bag_type | generation | section(pb BagSection) | updated_at_ms
                PK(player_id, bag_type)
                (后端驻留组:仓库/活动背包本体;由背包域服务事务直改,与 journal 同事务)
bag_journal     id PK | player_id | journal_seq | owner_epoch | op_type | bag_type | generation
                | payload(pb) | idempotency_key | created_at_ms
                uk(player_id, journal_seq) + uk(player_id, idempotency_key)
                90 天保留期 sweep(§9.24;删除资格 = 超期 **且** journal_seq <=
                该玩家 bag_checkpoint.covered_journal_seq —— 未覆盖尾部是
                LoadBag 唯一恢复数据,时间到期也绝不删;无 checkpoint 玩家不删。
                LoadBag 校验 (covered, last] 尾部连续,缺口/截断 fail-closed 拒载,
                INC-20260722-003)
bag_generation  bag_type PK | current_generation | salvage_mode | updated_at_ms
                (活动代际权威,§6;经配置表热更管线发布,etcd version 通知)
```

- `BagStorageRecord`(proto)按背包类型分段:`repeated BagSection{ bag_type, generation,
  capacity, repeated BagItem }`;装备实例字段(鉴定态/词条/耐久)内嵌 `BagItem`。
  checkpoint blob 只含 DS 驻留组段;后端驻留组每段一行 `bag_section`(同一 `BagSection`
  message,不造并行结构)。
- 写路径统一校验:owner_epoch CAS(③)→ generation 校验(§6)→ 额度(④)→ append/覆盖;
  涉及后端驻留段的 journal(转移/领取/活动入账)在**同一事务**里改 `bag_section`。
- 增长有界(§9.18):每 bag_type 容量服务端校验;journal 有保留期;checkpoint/section 单行覆盖。
- **可分片性(单玩家事务域,2026-07-21 追问确认)**:背包域每笔写只涉及**一个 player_id**
  的行(meta/journal/checkpoint/section 全 player_id 键),扩容按 player_id 分片
  (Cell 架构"同 player 同 cell + CRC16 分片")⇒ 单玩家全部背包行恒同库,本地事务永远够用,
  不需要分布式事务(§15.2)。跨玩家操作(交易)刻意排除在本域外(走经济域 saga + 邮件到账),
  就是为保住这个性质。若未来迁 TiDB:分布式 ACID 单事务语义不变,且本设计用"锁 bag_meta
  单行"做串行化锚点、不依赖间隙锁,即 guild 迁 TiDB 确立的安全写法,零改动兼容。
  唯一全局键表 `bag_generation`(bag_type PK)分片后须每分片复制(运营切代经配置管线写全
  分片)或挪 configtable/etcd 下发;事务内读本分片副本安全(玩家恒在一分片、代际单调、
  错配 fail-closed)。

## 5. 背包类型 × 策略矩阵

`bag_type` 用 `uint32`:0-3 与 UE `EMyBagType` 对齐(值即持久化类型,不可重排);
**100 起为活动背包段**(动态,UE 侧走 DynamicBags,MyBagTypes.h 已预留)。

| bag_type | 背包 | 驻留 / 权威(迁移后) | 持久性 | 同步模型 |
|---|---|---|---|---|
| — | 战斗局内背包 | DS 运行时(唯一实时背包) | 随局清零;持久物是账本视图 | 实时(门控/预留,已落地);**不进背包域存储** |
| 0 | 身上背包 Inventory | **DS 驻留**(checkout 进 owner DS 内存) | 持久 | journal + checkpoint |
| 2 | 装备栏 Equipment | **DS 驻留** | 持久 | 穿戴/卸下 → checkpoint |
| 3 | 临时格 Temporary | **DS 驻留** | 持久(短期) | checkpoint |
| 1 | 仓库 Warehouse | **后端驻留**(存储侧权威,DS 只发起+只读视图) | 持久 | 每笔变更 = 存储侧事务(转移走 journal 同事务) |
| 100+ | **临时活动背包** | **后端驻留** | **活动周期内持久,切代即清** | 存储侧事务;generation 全程携带 |

### 5.1 后端驻留段物品的"使用"语义(2026-07-21 追问补充)

后端驻留段(仓库/活动背包)的物品**可以使用**,不需要为此搬进 DS 内存;按效果类型三分:

1. **持久产出型**(开活动箱/兑换/用券领奖,预计占绝大多数):一条 journal op 在存储侧
   **同一事务**完成「后端驻留段扣除 + 产出入账」;产出进身上背包(DS 收 ACK 后应用,
   容量先走预留制,与仓库→身上转移同构)或走邮件。零丢失、零复制、无崩溃窗口。
2. **局内瞬时效果型**(活动 buff/回血):DS 发起扣除(journal,幂等键)→ ACK 后施加效果。
   "已扣未生效"窗口内崩溃 = buff 与局内状态同命消失,回档自洽,可接受;贵重道具由策划
   做成"激活型"(激活后为持久 buff 记录)即归第 1 类,零窗口。
3. **高频消耗型**(活动玩法内当弹药连续使用):禁止逐次 RPC——先一条 journal **装填**
   (转移一批到随身组),再按随身组规则以 DS 速度消耗(个人消耗 = checkpoint 类;
   **丢弃例外,强制走 journal,见 §5.1.1**)。
   后端驻留段保持 UI 频率操作定位,不因玩法高频化被迫改驻留。

### 5.1.1 随身组的 consume:丢弃 = 事实先行流水(2026-08-17 拍板)★

§5.1 第 3 点的"个人消耗 = checkpoint 类"**保留为默认**(用药 / 弹药消耗不进 journal),
但**丢弃是唯一强制例外**,必须走 `ConsumeOp` journal:

**为什么丢弃不能是 checkpoint 类**:丢弃成功会在 DS 脚下生成掉落物,该掉落**公共先到先得**
(策划拍板,谁捡到是谁的)。于是丢弃瞬间起就存在"第二持有人"——checkpoint 语义下,快照回滚
会把「销毁」变成「复制」:原主回档拿回该堆,而拾取者已按 `pickup_grant` 入账,资产凭空翻倍
(跨玩家双份 P1);反向若掉落先被销毁而扣减已提交,则是资产蒸发(P2)。事实先行(扣减落
`bag_journal` 后才删本地堆并生成掉落)让两侧都成为 journaled 成对事实,双份与蒸发机制性消失,
残余风险只剩"地面物随 DS 崩溃丢失"——与怪物掉落同口径,已接受。

**语义**(执行点 `bag_apply.go` / 客户端 `MyBagPersistenceComponent`):

- 存储侧**只记 journal,不改 `bag_section`**——随身组本就不落 `bag_section`,`deductFromBagType`
  对随身段是 no-op,与 `pickup_grant` 打随身段完全同构。段本体仍由 owner DS 内存应用、
  经 checkpoint 覆盖。
- **不许带产出**:随身段 consume 的 `produce_items` 非空即 `ErrBagSectionNotAllowed`
  (fail-closed)。理由是防止经此开出一条未经审计的跨段发放通道——丢弃只该"只出"。
- **重放**:客户端 checkout 尾重放按 §3.2 组级寻址做组级扣减(与 transfer 随身源同构);
  带产出的随身段条目判为非法条目。
- **ACK 窗口漂移**:consume 已落库但本地因状态漂移未删堆时,下一个覆盖其 seq 的 checkpoint
  会整体取代该 consume(丢弃等效未发生),不丢件也不双份。

**装备暂未放开**(`EquipSlot > 0` 仍 fail-closed):journal 无带 `instance_id` 的丢弃形状,
checkpoint-only 丢弃 + 落地会在崩溃恢复后产出双份 `instance_id`。放开需后端先补带实例载荷的
流水,另立项。

**混版安全**:老后端 + 新客户端 = 随身段 consume 被 `ErrBagSectionNotAllowed`(14009,非可
重试)拒,客户端走确定性拒绝路径(出队 + `OnAck(false)`),道具保持在包里,不卡队列不围栏
——功能不通但零资产风险,符合 §9.21。

### 5.2 堆叠语义:一段一权威,语义与权威同址(2026-07-22 拍板)★

**每一段的堆叠 / MaxStack / 格局语义只在该段权威处实现一次;另一侧只读渲染,永不重算。**

| 段 | 堆叠/容量判定唯一权威 | 另一侧的角色 |
|---|---|---|
| 随身组(0/2/3)+ 战斗背包 | owner DS `FMyBag`(预留→ACK→转正) | 服务端对随身组 grant no-op,checkpoint 原样存 DS 布局 |
| 后端驻留段(1/100+) | Go `sectionAddItems`(MySQL 事务内) | DS/客户端只做 UI 预检(快速失败提示),结果不作数 |
| 仓库⇄身上转移 | 两侧各自判:身上侧 DS 预留,仓库侧 Go 事务 | — |

后端驻留段**服务端建模 MaxStack**(取代初版"同 config 无限合并单格,拆堆归客户端展示"):
可堆叠道具按单格上限拆堆存放,先填既有未满堆、溢出按上限整格新开,**容量按拆堆后的
格子数校验**,放不下整批拒(事务回滚)。初版无限合并被否的三个理由:①与格子 UI 的容量
语义矛盾(1 格可存 42 亿个,容量按条目数判定形同虚设,§9.18 被绕空);②`Count += n` 是
uint32 无检查累加,存在回绕(新实现规划全程 uint64,巨量入账折算格数天然超容拒,回绕
构造上不可能);③客户端"展示拆堆"会画出超过 capacity 的格子数。历史遗留的超上限脏堆:
跳过不吸纳(防下溢)、资产原样保留,只出不进。

MaxStack 数据源:与客户端 `CfgItem` 堆叠上限**同源导出**,正式管线为 §9.15 配置表热更
(道具表接入后生效);接入前由 inventory `bag.default_max_stack`(默认 99,与 UE
`MyBag::DefaultMaxStackSize` 同值)+ `bag.item_max_stacks` 覆盖表承载,启动 Validate
fail-fast。上限查询返回 0 = 配置非法,写入 fail-closed 拒(不静默无限合并)。
禁令:DS/客户端不得对后端段本地拆堆后写回;后端段布局以 GetSections 返回为准。
回归:`bag_apply_test.go TestSectionAddItemsMaxStackSplit`(拆堆/先填零头/写前整体拒/
上限未配置拒/脏堆跳过/uint32 极值不回绕)。

### 5.3 容量模型:配置初始值 + 玩家可购增量(2026-07-22 拍板并落码)★

产品需求:容量有配置初始值,玩家可花钱购买扩容。容量因此是**每玩家持久事实**,
不是配置常量。**Go 侧全链已落码**(2026-07-22 用户"按建议"拍板):可买段 = 身上 0 +
仓库 1(装备栏/临时格/活动段不可买,conf.Validate 锁死);默认档位 = 身上 10 档 ×10 格
(100→200,第 N 档 100N 金币)、仓库 15 档 ×20 格(200→500,第 N 档 200N 金币),
正式数值走配置表管线后覆盖;`bag_capacity` 表 + `PurchaseCapacity` RPC +
`LoadBagResponse.effective_capacities` + AppendJournal/GetSections 有效容量已接线。
UE 侧应用链(购买入口 + ACK 后 ExpandCapacity + LoadBag 权威容量 Init)待 cpp pb 同步后接。

1. **有效容量 = base + extra**:`base(bag_type)` 来自配置(可全服热更调整);
   `extra(player_id, bag_type)` 是玩家资产,落 `pandora_bag` 新表 `bag_capacity`
   (player_id + bag_type 一行)。extra **单调只增、被硬上限封顶**(§9.18:每 bag_type
   配 `max_extra`,超限拒购;不支持退款回缩——回缩会把 §3.2"只出不进"从恢复态变成
   常态)。现行 `bag.section_capacities` 即 base 的 interim 承载。
2. **信任边界:容量服务端权威,DS 只应用不决定**(§9.6 数值不信 DS)。checkpoint blob
   内 `BagSection.capacity` 为 DS 回显,**不作数**;LoadBag 下发各随身段权威有效容量,
   DS 据此 `Init/ExpandCapacity`;后端段判定在 AppendJournal **同事务读 `bag_capacity`**
   (判定与权威同址,无影子副本)。被攻破的 DS 造不出容量。
3. **购买链(跨库幂等两步)**:客户端 → owner DS → inventory `PurchaseBagCapacity`
   (内网 RPC,五要件授权):①trade 库事务扣金币 + `inventory_ledger` 幂等
   (op=buy_capacity,operation_id);②bag 库事务 upsert `bag_capacity`(同 operation_id
   幂等)。两步间崩溃凭 ledger 行重放 ②(幂等,at-least-once 收敛),不存在"扣钱未到账"
   终态;ACK 后 DS 才应用扩容并刷 UI。价格/封顶全在服务端,阶梯价走 §9.15 配置表管线
   (接入前服务配置承载)。
4. **与既有语义的协同**:购买扩容使超容段(迁移落位/崩溃恢复溢出)提前回到容量以下,
   "只出不进"自动解除;UE `FMyBag::ExpandCapacity` 已具备,仅在收到服务端 ACK 后调用。

## 6. 活动背包代际(类型重用,本次新增需求)★

需求:活动背包类型会被后续活动**重用**;活动结束时该类型背包的所有东西被清理。

**设计:清理 = 代际切换,不是删数据。**背包段身份 = `(player_id, bag_type, generation)`:

1. `bag_generation` 表是每个活动 bag_type 的**全局单调代号**权威;活动开启/结束由运营
   推进 `current_generation`(走 §9.15 配置表热更管线:staging + 版本号 + etcd version 通知,
   全 fleet 与所有背包域副本一致收敛,不停服)。
2. **读过滤**:加载/查询只返回 `generation == current` 的段;旧代物品瞬间对全系统不可见
   ——"清理"在切代那一刻逻辑上已完成,与玩家量无关,零停服零全量刷(§9.16)。
3. **写 fencing**:journal 写携带 generation,存储侧
   `generation != current_generation → 拒(ERR_BAG_GENERATION_MISMATCH,fail-closed)`。
   这封死关键竞态:活动刚结束、某台 DS 配置热更迟到,它发来的旧代入账/转移一律被拒,
   旧物品不可能漏进新活动。活动背包是后端驻留(§5),DS 不持有其状态——收到拒绝只需
   刷新只读视图并重拉配置,无内存段可清。
4. **物理清理**:后台 sweep 有界批量删除 `generation < current` 的旧代数据(checkpoint 内
   旧代段在下次快照自然消失;journal 行按保留期走 §9.24 sweep)。删除是纯回收,正确性
   不依赖它(读过滤已保证不可见)。
5. **切代物资去向**(运营按活动配置,`salvage_mode`):
   - `discard`(默认):直接作废;
   - `mail`:切代时后台任务把旧代物品按玩家折算成邮件附件补发(复用 mail 附件与领取链,
     领取回身上背包)。补发任务幂等键 = `(player_id, bag_type, generation)`,可断点续跑。
6. **类型重用安全性**:下一个活动复用同一 bag_type 时 generation 已不同——旧物品读不到、
   旧写进不来、新活动从空段开始。不存在"上个活动的东西串场"。

## 7. 邮件 = 唯一的离线→在线资产中转层

原则:**离线系统一律不直接写背包**;系统发奖、拍卖到账、活动补发统一"发邮件,领取才进包"。

领取时序(玩家在线,由 owner DS 执行):

```text
玩家点领取 → DS 背包预留容量(FMyBag 预留制,已落地;真满当场拒,附件留在邮件)
          → 调 mail 原子 claim(幂等 key;mail 只管 claimed 状态,不再调 inventory)
          → 预留转正入包 + 同步 journal(op=mail_claim,幂等键=claim key)
          → checkpoint 随后覆盖
```

上述时序是"领取进身上背包(DS 驻留)"的形态;若产品拍"领取进仓库(后端驻留)",
容量判定直接发生在 journal 事务内,无预留环节——两种形态的判定原则见 §3.1。
**默认形态定型为进身上背包**(2026-07-22 phase 2 接线,三段式链已落地测绿;
decision-revisit-bag-replay-semantics.md D3);进仓库保留为产品后备形态,切换 =
journal 目标段改仓库 + 免预留,零新机制。

崩溃恢复:claim 后、journal 前崩溃 → 重载后按待完成意图重放同一 key,mail 幂等返回同一
附件内容 → 补入包。恰好一次,与 inventory ledger 现行模式同构。
mail 与 inventory 的 GrantItems/CapacityFull-转邮件耦合随 phase 2 剪断(容量判定移 DS)。

### 7.1 附件三形态与实例托管转移(2026-07-22 拍板)

| 形态 | 语义 | 领取效果 | 用途 |
|---|---|---|---|
| stack | 可堆叠发放 | 按 config+count 计数入账 | 系统发材料/消耗品 |
| instance | **铸造凭证**(发"新"实例) | 领取时铸全新实例:新雪花 ID、未鉴定、词条鉴定时才 roll | 系统发新装备(掉落满转邮件等) |
| transfer | **既存实例托管转移** | **只改归属**:instance_id 与全部实例数据逐字节原样 | 拍卖成交到账、玩家转赠、活动补发已鉴定物 |

**误用边界(用户 2026-07-22 明确要求)**:"既存物品换主人"绝不允许走 instance 铸造——
那会把玩家的装备变成另一件东西(新 ID、白板未鉴定)。transfer 对**一切实例类物品通用**
(装备今天,未来任何带唯一 ID 的品类):机制只认 instance_id + 快照,不认品类,新品类零改动。

**transfer 三不变量**(mail.proto TransferAttachment 注释同文;**2026-07-22 已接线,
经济域托管闭环**——当前实例权威在 `player_item_instance`(经济域),phase 2 写权威切 DS
后领取入包路径迁 bag journal(mail_claim op),托管扣出/互斥持有语义不变):
1. **同一 instance 全局唯一**:inventory.EscrowOutInstances 同一 MySQL 事务从
   `player_item_instance` 扣出 + 写 `mail_transfer_escrow`(两表各以 instance_id 为 PK,
   行只能经事务性 INSERT...SELECT + DELETE 搬移),托管期间实例不存在于任何玩家背包;
   bound 实例拒(ERR_INVENTORY_INSTANCE_BOUND);
2. **领取 = 归属变更**:mail ClaimMail → inventory.ClaimTransferInstances 把托管行
   逐字节原样搬进领取人实例表(同事务;零重铸零重 roll)。**领取只认托管行**
   (escrow 权威,附件快照仅供展示与 instance_id+config 核对):伪造附件(声称托管但
   未扣出)领取必 fail-closed,整封保持未领取;
3. **约束**:仅个人邮件可携带(系统/公会邮件多人可领与单实例矛盾,发送侧拒);发送
   saga = EscrowOut → SendPersonalMail,失败补偿 ReleaseTransferEscrow 归还源玩家
   (行缺失 no-op 幂等,释放不设容量闸,slot NULL 入包);领取幂等键
   `mail_xfer:{mail}:{player}`(inventory_ledger op=transfer_claim),crash-after-claim
   重放恰好一次;过期未领个人邮件由 mail sweep 归档留补偿凭据,托管行保持在途
   (§9.24 豁免登记,运营凭归档重发或释放)。
   回归测试:mail biz TestTransferPersonalMailClaimChain 等 4 例 +
   inventory biz transfer_test.go + data TestMailTransferEscrow_MySQL(真 MySQL 已绿)。
   现状无生产发送方:拍卖成交到账/玩家转赠/活动补发接入时走上述 saga,零机制改动。

## 8. 拍卖 / 交易与背包域

- **挂单冻结在线发起**:DS journal 扣(op=escrow_freeze,幂等 operation_id)→ 经济域
  FreezeForOrder 记账;任一步失败按幂等键补偿回包(saga,两侧幂等已具备)。
- **成交到账走邮件**("拍卖成功请查收邮件"),彻底避开离线写背包。
- 撮合、escrow、防死锁、金币扣转全部留经济域,不迁。

## 9. `pandora.bag.v1` 契约草案

> **落地纪律**:phase 1 开工时才落 `.proto` 文件并由 Codex proto_gen(§5.1);此前编号可调。
> 现在不建文件的原因:journal 的 fencing 字段依赖 owner authority 的最终形态(§9.22 未建成),
> 提前冻结契约会二次返工。以下为审阅基准草案。

```proto
// pandora/bag/v1/bag.proto(草案)
service BagService {
  // 加载 DS 驻留组(owner DS checkout;Admission 通过后调用)。
  // 返回 checkpoint 快照(仅随身组段)+ covered_seq 之后的 journal 尾部,调用方重放构建内存态。
  rpc LoadBag(LoadBagRequest) returns (LoadBagResponse);
  // 追加 journal(同步入账;批量,批内 journal_seq 升序,前缀确认)。
  // 涉及后端驻留段的 op(转移/活动入账)由服务端在同一事务改 bag_section。
  rpc AppendJournal(AppendJournalRequest) returns (AppendJournalResponse);
  // 保存 checkpoint(异步周期 + 关键点;covered_journal_seq 必须 ≤ 已确认水位;仅随身组)。
  rpc SaveCheckpoint(SaveCheckpointRequest) returns (SaveCheckpointResponse);
  // 查询后端驻留段(仓库/活动背包,DS 只读视图与 UI 分页用;按 current generation 过滤)。
  rpc GetSections(GetSectionsRequest) returns (GetSectionsResponse);
}

message LoadBagRequest  { uint64 player_id = 1; uint64 owner_epoch = 2; }
message LoadBagResponse {
  pandora.common.v1.ErrCode code = 1;
  BagStorageRecord snapshot = 2;          // checkpoint(含各段 generation)
  repeated BagJournalEntry tail = 3;      // covered_seq 之后的已确认 journal
  uint64 last_journal_seq = 4;            // 权威水位(重放终点,后续 Append 起点)
}

message AppendJournalRequest {
  uint64 player_id = 1;
  uint64 owner_epoch = 2;                 // 五要件③:存储侧 CAS bag_meta.owner_epoch
  repeated BagJournalEntry entries = 3;   // 批内 journal_seq 升序;单飞行批,失败原批重发
}
message AppendJournalResponse {
  pandora.common.v1.ErrCode code = 1;     // GENERATION_MISMATCH / EPOCH_FENCED / QUOTA 等
  uint64 acked_seq = 2;                   // 前缀确认(语义同 ReportProgress.acked_seq)
}

message BagJournalEntry {
  uint64 journal_seq = 1;                 // 每玩家单调,从 1 起
  uint32 bag_type = 2;
  uint64 generation = 3;                  // 活动段必填;固定背包恒 0
  string idempotency_key = 4;             // uk(player_id, key);重放/重试去重
  int64  ts_ms = 5;                       // 审计
  oneof op {
    PickupGrantOp  pickup_grant  = 10;    // 拾取入包(取代 ReportProgress 掉落分支)
    MailClaimOp    mail_claim    = 11;    // 邮件附件领取
    TransferOutOp  transfer_out  = 12;    // 交易/给予/拍卖冻结 扣出
    TransferInOp   transfer_in   = 13;    // 交易对手方 转入
    GenerationSettleOp gen_settle = 14;   // 活动切代结算(salvage 补发标记)
  }
}
```

鉴权:内网 DS 直连(不经 Envoy 对客户端开放);DS JWT + owner 授权 + epoch fencing(§2)。
经验类事实仍走 battle.proto(换算域不变)。

## 10. 迁移路径(expand → migrate → contract,§9.21)

| 阶段 | 内容 | 前置 | 回退 |
|---|---|---|---|
| phase 0(已完成) | 拾取 ACK 门控 + 预留制;权威在 inventory;掉落经 battle_result 翻译 | — | — |
| phase 1 | **owner authority 落地(硬前置)**;建 `pandora_bag` 存储 + bag.v1 API(可先由 inventory 进程承载,契约稳定、拓扑后移);Hub DS read-through 加载(只迁读) | owner authority | 停用加载,读回 inventory |
| phase 2 | 写权威切 DS:journal 直写;拾取入账从 ReportProgress 掉落分支切到 journal(**UE 认领/回执/预留机制原样保留,只换后端调用**);邮件领取改 DS 执行;客户端直连 UseItem/SellItem 砍掉。**2026-07-22 已接线(门控默认关)**:Go 侧五要件①②全上(DSCallbackGuard 身份 + owner target 全等授权,epoch 服务端解析代填)、mail 三段式领取 RPC(意图落库稳定展开/Mark 消 transfer 托管行);UE 侧 UMyBagPersistenceComponent(checkout/单飞行 journal 写者/checkpoint/邮件领取驱动)+ BeginGatedPickup 选路 + PlayerState ServerClaimMailAttachments 入口;Envoy DS 面 bag/mail 路由 + UseItem/SellItem/ClaimMail cutover 403 预留(注释态)。启用开关:UE `PANDORA_BAG_JOURNAL_ENABLED` + 服务端 bag.dsn/owner_addr/ds_auth;UE 编译与联调待用户 | phase 1 全绿 | killswitch 停 journal,回 phase 0 双路径 |
| phase 3 | 拍卖/交易改在线扣 + 邮件到账;battle_result 回归纯结算(掉落发放退役,水位互斥耦合消失);inventory 收缩为经济域;活动背包上线;**存量迁移**:旧写路径(GrantItems/UseItem/SellItem/escrow)全部冻结后跑 bag 迁移作业——player_items/player_item_instance 全量迁仓库段(bag_migration 表一玩家一行幂等,超容落位只出不进,作业已落码 `bag.legacy_migration_enabled` 默认关)+ 总量对账(config 计数 + 实例集相等);**与邮件 transfer 托管链同窗口割接**(实例权威迁 bag 后 EscrowOut/ClaimTransfer/Release 改指 bag 域,在途托管行照常) | phase 2 排空旧 DS | 按 §9.21 金丝雀纪律逐步;迁移作业幂等可断点续跑,关门即停 |

混版纪律:每阶段新旧共存窗口内双向兼容;发放权互斥(水位表)保留到最后一个旧路径调用方
排空;`progress_enabled` 式的显式闸门逐阶段设置。

## 11. 失败模式与验证矩阵(§16)

| 风险 | 场景 | 防线 | 验证 |
|---|---|---|---|
| 脑裂双写 | 旧 DS 分区后恢复继续写 | owner_epoch CAS fencing(bag_meta) | 故障注入:双 DS 并发 Append,旧 epoch 全拒 |
| 交接丢新鲜度 | 迁移时 flush 未完成 | flush-before-fence 时序;同步 journal 不受影响 | 迁移竞态集成测试:杀旧 DS 后新 DS 加载重放 |
| 切代迟到写 | 活动结束后 DS 配置迟到仍入账 | generation fencing fail-closed | 热更迟到注入:旧代 Append 必拒 |
| 领取双发/丢失 | mail claim 与 journal 之间崩溃 | claim 幂等 key + 意图重放 | claim 后杀进程,重启补入包恰好一次 |
| journal 重放不齐 | checkpoint 与 journal 水位错位 | covered_seq ≤ acked 校验;重放冲突 fail-closed | 乱序/缺口重放单测 |
| 额度绕过 | 被攻破 DS 刷物品 | 五要件④ 速率/上限 + ⑤ 审计 | 超额批被拒 + 告警可见 |
| 容量竞态 | 多写源挤占 | DS 侧预留制(已落地,MyBagReservationTest) | 已有回归测试 |
| 重放容量冲突 | 整理/用药未 checkpoint,依赖其空格的 grant 已 ACK,崩溃回档后放不下 | §3.2 资产守恒:溢出临时格 → 超容只出不进,永不拒载 | 崩溃注入:整理后 kill,重进断言资产守恒 + 溢出告警 |
| 重放扣减找不到物品 | 卸下(未 checkpoint)→ 交易(已 journal)→ 崩溃 | §3.2 组级寻址:按随身组整体解析 | 组级扣减重放测试 |
| 存量迁移双算/漏算 | 迁移重跑 / 旧路径未冻结先迁 | bag_migration 一玩家一行幂等闸;作业配置门默认关,contract 冻结旧写后才开;迁后总量对账 | 幂等重跑测试 + 对账断言 |
| 跨驻留组转移撕裂 | 仓库⇄身上转移只完成一半 | 一条 journal、存储侧同事务改两侧;身上侧先预留 | 转移与拾取/领取并发集成测试 |
| 丢弃跨玩家双份 / 蒸发 | 丢弃落地被他人拾走,原主 checkpoint 回档 | §5.1.1 事实先行:扣减先落 journal(consume),ACK 后才删本地堆并生成掉落;拾取侧走 pickup_grant,两侧都是 journaled 成对事实 | 数据层"随身段 consume 只记 journal 不标脏" + biz 放行契约测试;丢弃后 kill DS,重进断言资产不翻倍不蒸发 |
| 随身段 consume 越权发放 | 伪造 produce_items 从随身段"开"出物品,绕过审计 | §5.1.1 fail-closed:随身段带产出即 ErrBagSectionNotAllowed;客户端重放同判非法 | data + biz 两层"带产出拒"用例 |

未覆盖验证前,不得宣称对应能力"已达成"(§16.6)。

## 12. 复杂度举证(§15.4)

简单方案"权威留 inventory、DS 全程 RPC"被否的已确认约束:MMO 高频背包操作的延迟与 QPS
全压经济服务;"两份背包"语义纠缠(局内视图 vs 账本,已在评审中造成理解成本);邮件/拍卖
与背包的容量判定耦合。新增组件:bag.v1 契约、journal/checkpoint 存储、代际机制——各自
对应一条已确认需求(零丢失、崩溃回档、类型重用),无预设性组件。回退路径:phase 边界
可长期停留;phase 2 有 killswitch 回 phase 0。

## 13. 与既有规范的关系

- §9.6 五要件:本文档 §2 是其唯一落地入口;
- §9.22 owner authority:phase 1 硬前置,背包域不自建 owner,只查询/校验;
- §9.16/17:bag blob 双向兼容、不停服切代/迁移;§9.18 容量与保留期;§9.24 journal 90 天;
- §9.21:分阶段金丝雀;ds-arch §0.5/§0.6:phase 2 时把"背包 journal 直写"补进合法通道
  清单并同步修订红线措辞(异步 / owner 授权 / 幂等三纪律);
- `realtime-progression.md`:phase 0 的实现记录;其掉落入账链在 phase 2 被本域取代,
  经验链保留。
