# 反外挂:宝箱品质渐进公开的信息泄露与「挑箱子」防护

> 2026-08-06 立档(Claude)。**状态：§3.2 的泄露已按建议 1 修复并落地（见 §4.4）。**
>
> 关联：`Pandora-Client-SVN/Pandora/Source/Pandora/{Public,Private}/Module/Loot/MyLootChestActor.{h,cpp}`；
> `Pandora-Client-SVN/Pandora/Source/Pandora/{Public,Private}/Entity/MyEntityPlayerController.{h,cpp}`；
> `docs/design/anti-abuse-scene-entry.md`(同系列，威胁模型 §1 的分类沿用)；
> CLAUDE.md §9.6(派生数值服务端算)、§9.22(唯一权威 / 客户端只做快照的确定性投影)。
>
> 本文只谈**宝箱品质信息面**，不谈掉落物本身的归属与拾取(那是 `bExclusiveToOpener` 的射程)。

---

## 0. 结论先说

用户提出的担忧是:「每次去打开的时候,玩家发现品质不好,就不开了」以及「有外挂能知道这个品质」。

盘完当前实现,两条都需要修正:

1. **「发现品质不好就不开」在当前机制下不成立。** 读条公开的是**单调上升的下界**,
   而不是品质本身;而箱子在到达真实品质的那一刻**自动开启并掉落**。玩家永远不会处于
   「已知它很差、但还没拿到」的状态——差消息抵达的形式就是「箱子开了,你拿到了绿的」。
   放弃读条在任何时刻都是严格劣策略,详见 §2。
2. **「外挂知道品质」在复制面上不成立,但存在一个真实的旁路:别人的读条进度。**
   本次预抽结果(`PreparedRolls` / `PreparedHighestQuality`)确实只在 DS,从未复制;
   但**每个读条者的 `CurrentStageQuality` 会复制给全体相关客户端**,于是「看别人读到第几档」
   等于免费获得品质下界,不用自己花时间。这才是需要处理的点,详见 §3.2。

换句话说:**要防的不是「他知道后不开」,是「他看别人读、自己白嫖情报后再决定要不要抢」。**

---

## 1. 当前机制(事实,非设计意图)

### 1.1 品质在生成时定死,不是每次开箱重摇

`AMyLootChestActor::PrepareAuthoritativeContentsForCycle()` 只在两处被调用:
权威端 `BeginPlay` 与 `HandleRespawn`。它一次性完成:

- `CycleCounter++`
- `RollChestDrops(LootTableId, PreparedRolls)` 抽出本周期全部掉落
- `ResolveHighestItemQuality(PreparedRolls, PreparedHighestQuality)` 定出本箱最高品质 0..5

自此 `PreparedHighestQuality` 固定不变。开箱时 `OpenedQuality = PreparedHighestQuality` 直接取用。
**取消读条不会重摇**:`CycleId` 的注释明确写着「每次重刷并重新预抽后递增;取消、背起和丢下不会改变」。

因此:读条过程中的任何抖动、回退、停顿,都不可能是随机造成的,只能是本端时钟问题。

### 1.2 品质按阶段挤牙膏公开

`HandleUnlockStageFinishedForPlayer()` 在每个阶段计时器到点时:

```
if (Entry.CurrentStageQuality >= PreparedHighestQuality) → OpenChestForWinner()  // 开箱
else                                                     → CurrentStageQuality++ // 进下一档
```

配表 `chest_quality_stage`(ruleset `Default`)的累计秒:白 2 / 绿 4 / 蓝 6 / 紫 8 / 橙 10 / 红 12。

所以对读条者而言，**"还在读"这件事本身就是情报**：

| 读到 | 说明本箱品质 |
|---|---|
| 撑过 2s 没开 | ≥ 绿 |
| 撑过 4s 没开 | ≥ 蓝 |
| 撑过 6s 没开 | ≥ 紫 |
| 撑过 8s 没开 | ≥ 橙 |
| 撑过 10s 没开 | = 红 |

注意这是**下界**，不是值。玩家在开箱前的任意时刻，只知道「至少这么好」，永远不知道上界。

### 1.3 时长 ≡ 品质（设计意图，不是旁路）

上表不是什么需要堵住的侧信道，它**就是玩法本身**：品质就是用时间换的，玩家能从
「我已经读了多久」推出「至少多好」是设计想要的张力。

但这条有一个**工程上的直接推论**，很容易在做修复时漏掉：

> 既然 `quality = f(elapsed)`，那么**把 `StartServerTime` 发给谁，就等于把品质发给谁**。
> `CfgChestQualityStage` 本就随客户端下发，`now - StartServerTime` 一减就出来了。

所以后文 §3.2 说的泄露，只把 `CurrentStageQuality` 字段清零是**掩耳盗铃**：
阶段和时间窗是同一个信息的两种写法，要么两个一起离开广播通道，要么都白搞。

---

## 2. 为什么「看到品质不好就不开」不成立

设玩家在 t 时刻已知下界为 q(t),q 随 t 单调**不减**。要做出「放弃」的决策,玩家必须获得
**坏消息**,即「上界很低」。但当前机制下上界只有一种揭示方式——**箱子开了**。而箱子一开,
`SpawnLootItemsAtomically` 已经把掉落生成完毕,玩家已经拿到了。

于是不存在「已知差、尚未到手、可以退出」这个窗口。读得越久,已知的品质**越高**,
放弃的期望收益只会越低。放弃在任何时刻都是严格劣策略。

**唯一的真实退出动机与品质无关**:被人打、要跑、时间不够。这些是正常玩法,不是滥用。

> ⚠️ 如果后续策划把机制改成「先明示最终品质、再让玩家决定要不要读满」,
> 或者「读满后弹出确认框」,那么 §0 的结论立刻失效,本节需要重写。
> **本文的结论强绑定于「揭示即开箱」这一条**。

### 2.1 一个次要但真实的残留:情报跨取消保留

玩家读到 8s 得知「≥ 紫」,随后取消。因为取消不改 `CycleId`,箱子的品质**保持不变**。
他可以去叫人、换个安全位置,回来时这只箱子仍然 ≥ 紫。

- 危害:低。他要重新从 0 读满(阶段从头计),时间成本照付;
- 但这确实让「侦查—撤退—带队再来」成为可行套路。是否接受属策划口径,不是安全问题。

---

## 3. 信息面审计:到底什么上了线

### 3.1 未泄露(已确认)

| 数据 | 位置 | 是否复制 |
|---|---|---|
| `PreparedRolls`(具体物品与数量) | `AMyLootChestActor` 私有 | **否**,注释明确「仅 DS 保存」 |
| `PreparedHighestQuality`(本箱真实最高品质) | 同上 | **否** |
| `InteractionSnapshot.OpenedQuality` | 快照 | 是,但**仅 Opened 状态有值**,此前恒为 0 |
| `OpenedEffectStart/EndServerTime` | 快照 | 是,但同样只在 Opened 才写 |
| 箱体品质材质 `OpenedChestBodyMaterials` | 蓝图默认值 | 客户端本就有,但**只在 Opened 按 `OpenedQuality` 取下标** |

`GetLifetimeReplicatedProps` 只挂了 6 项:`InteractionSnapshot`、`ChestVisualMesh`、
`UnlockRangeRadius`、`bCanCarry`、`QualityRuleSetId`、`CarryMoveAndAnimationRate`。
其中没有任何一项携带本次预抽结果。**结论:外挂无法从复制通道提前得知本箱品质。**

### 3.2 已泄露（真问题，**现已修复，见 §4.4**）

**逐玩家阶段与时间窗随整个快照复制给全体相关客户端。**

`FMyLootChestInteractionSnapshot::Unlockers` 是 `TArray<FMyLootChestUnlockerEntry>`，
数组里每个条目的 `CurrentStageQuality`、`StartServerTime`、`EndServerTime` 都是 `UPROPERTY`，
随快照一并复制。另有聚合字段 `InteractionSnapshot.CurrentStageQuality`(全体读条者的
最高已公开阶段)与领先者的 `StartServerTime`/`EndServerTime` 同样在线上。

后果：

- **旁观白嫖**。B 不读条，只看 A 的条目，即可免费获得与 A 等同的品质下界。
  A 花了 8 秒换来的情报，B 零成本拿到，然后决定要不要过去抢。
  这破坏的不是保密而是**成本**：玩法的前提是「想知道品质就得自己花时间读」。
- **光清品质字段没用**。按 §1.3，`StartServerTime` 与阶段是同一个信息；
  只把 `CurrentStageQuality` 归零、把时间戳留在线上，外挂照样一减就得到答案。
- **外挂放大**。正常客户端的 HUD 已经刻意只显示本地玩家自己的档位，
  但**这是表现层的自我克制，不是网络层的约束**——数据就在包里，改客户端即可读出，
  且不受视距/遮挡限制，只要 Actor 相关就能看到。
- 聚合字段更糟：它是全场最高，等于把「本箱至少这么好」直接播给所有人。

**根因是复制拓扑，不是字段选型**：`InteractionSnapshot` 是单一 `UPROPERTY`，
复制出去是**同一份字节**发给全体相关客户端；`DOREPLIFETIME_CONDITION` 只能按属性整体
过滤，**做不到按接收方裁剪结构体成员**。只要逐玩家进度还在这个快照里，它就一定是广播的。

这正是用户直觉里那个「外挂能知道品质」——只是路径不是「偷看服务器的预抽」,
而是「偷看别人的读条进度」。

### 3.3 配表层:分布泄露(可接受,但需知情)

客户端随包携带 `CfgChestPoint`(含 `LootTableId`)与 `CfgChestDrop`(含 `Probability`),
物品品质又可由物品配置查出。因此攻击者可以离线算出**每个刷取点的品质分布**,
并据此规划路线("80007 这组期望更高")。

- 泄露的是**分布**,不是**本次实例的结果**,与「知道这只箱子是红的」有本质区别;
- 且分布本就是玩家可以通过长期游玩统计出来的,属于可接受的公开信息;
- 除非策划要求同一刷取点在不同场次表现出不同期望,否则**不建议**为此把配表从客户端摘掉。

---

## 4. 处置：已实施建议 1

按性价比排序。**§3.2 是唯一需要动手的**，已于 2026-08-06 实施建议 1。

### 建议 1（已实施）：把逐玩家进度移出整箱快照

让每个客户端只收到**自己那一条**进度。因为 `DOREPLIFETIME_CONDITION` 只能按属性整体过滤、
无法按接收方裁剪结构体成员，所以不是"裁剪 `Unlockers`"，而是**把进度整体搬到另一个
天然只发给拥有者的 Actor 上** —— `APlayerController`。

- 与 §9.22「唯一权威 / 客户端只做快照的确定性投影」不冲突：权威仍在 DS，只是通道按人分。
- 落地细节见 §4.4。

### 建议 2（已被建议 1 涵盖）：只砍聚合字段

保留逐玩家条目、只删除聚合 `InteractionSnapshot.CurrentStageQuality`。
**属于止血不治本**：`Unlockers[i]` 的时间窗仍在线上，按 §1.3 一减即可还原品质。已随建议 1 一并删除。

### 建议 3（未采纳）：接受现状

如果「能看到别人读到哪一档」本身就是设计意图（制造抢夺张力、吸引玩家聚集），
那 §3.2 不是漏洞而是特性。用户口径否定了这一条：情报必须自己花时间换。

### 4.4 实施记录（2026-08-06）

**客户端 C++**（`Pandora-Client-SVN`，未提交 SVN）：

| 位置 | 变更 |
|---|---|
| `FMyLootChestUnlockerEntry` | 删除 `CurrentStageQuality` / `StartServerTime` / `EndServerTime`；只留 `PlayerId` / `Pawn` / `ActionId`（纯归属，不含任何进度量） |
| `FMyLootChestInteractionSnapshot` | 删除整箱 `CurrentStageQuality` / `StartServerTime` / `EndServerTime`；只留 `OpenedQuality`（仅 Opened 非 0，那时品质本就该公开） |
| `FMyLootChestUnlockRuntime`（DS-only，不复制） | 新增 `ActionId` / `CurrentStageQuality` / `StartServerTime` / `EndServerTime`，成为阶段与时间窗的唯一权威落点 |
| `AMyEntityPlayerController` | 新增 `FMyLootChestUnlockProgressState LocalUnlockProgress`，`COND_OwnerOnly` + push model；新增 `SetAuthoritativeUnlockProgress` / `ClearAuthoritativeUnlockProgress` / `GetUnlockProgressForChest` / `GetUnlockQualityStageForChest` |
| `AMyLootChestActor` | 新增 `PushUnlockProgressToOwner` / `ClearUnlockProgressOnOwner`，在开始读条、阶段推进、取消、判负各点逐连接投递 |
| 本端读条基准 `FMyLootChestUnlockClientClock` | 从宝箱搬到控制器（一人同时只读一只箱，单实例即可），契约由 `(CycleId, ActionId)` 变为 `(Chest, CycleId, ActionId)` |
| `UMyLootStatics` | 新增 `GetChestRuleSetTotalUnlockSeconds()`（原为宝箱 cpp 匿名 namespace 的私有函数，控制器侧也要用） |
| `Pandora.cpp` | `PandoraNetProtocolVersion` 6 → 7 |

**必须同版本发布**：复制属性布局与结构体成员表同时变化，新旧两端各按各的布局解析同一份字节。
网络版本号已升到 7，握手阶段直接拒绝跨版本连接 —— 客户端与 DS 必须一起出包。

**保留的蓝图兼容入口（语义已变，别当 bug"修"回去）**：

- `GetCurrentStageQuality()`：仅 Opened 返回 `OpenedQuality`，读条中恒 0；
- `GetCurrentQualityStage()`：非 Opened 恒 `nullptr`；
- `GetUnlockProgress()`：收敛到本地玩家自己那一条；
- `GetUnlockProgressForPawn` / `GetQualityStageForPawn`：改为经该 Pawn 的控制器取数，
  **查别人恒 0 / nullptr** —— 这不是缺陷，正是"想知道品质就得自己花时间读"的成本本身；
- `UnlockStartServerTime` 旧镜像字段已恒 0（整箱层面不再持有任何读条时间窗）。

### 4.5 残余泄露（设计接受，无法在不改玩法的前提下堵死）

`Unlockers` 里**条目出现的时刻**仍然可观测。一个从头盯着 A 开始读条的旁观者，
仍可自己掐表算出 A 的进度。要彻底堵死，只能改成**定长读条 + 开箱才揭示品质**
（甚至开箱时才摇），而这与"逐级揭示品质"的玩法**互斥**。

已封堵的是成本更低、危害更大的那一类：**中途路过、瞟一眼就立刻知道品质下界**。
现在旁观者必须从头盯到尾并自己计时，成本已与亲自读条同量级。

---

## 5. 需要核实的前提(本文未验证)

1. **是否有 UI/表现依赖别人的读条档位**。决定建议 1 的可行性,必须先查客户端全部消费点。
2. **`bExclusiveToOpener` 的实际配置值**。`CfgChestPoint` 里看到的是 `false`,
   意味着掉落物人人可捡。若为 false,则「抢夺读条」的意义被削弱——B 完全可以不读条,
   等 A 开完再去捡。**这可能是比品质泄露更严重的设计问题,但不在本文射程,建议单独确认。**
3. **服务端是否需要记录/风控**。当前宝箱逻辑完全在 DS 内闭环,后端无任何埋点。
   若要做「异常挑箱子行为」的检测,需要先有 DS→后端的开箱事件上报,目前**没有**。

---

## 6. 变更记录

| 日期 | 变更 | 作者 |
|---|---|---|
| 2026-08-06 | 立档。盘点复制面,澄清「发现品质差就不开」不成立,定位真实泄露为逐玩家 `CurrentStageQuality` 全量复制 | Claude |
| 2026-08-06 | 补 §1.3：时长 ≡ 品质是设计意图，其工程推论是「发时间戳等于发品质」，只清品质字段属掩耳盗铃 | Claude |
| 2026-08-06 | 实施建议 1：逐玩家进度整体移出整箱快照，改由 `AMyEntityPlayerController::LocalUnlockProgress`（`COND_OwnerOnly`）逐连接投递；网络版本 6→7；补 §4.4 实施记录与 §4.5 残余泄露 | Claude |
