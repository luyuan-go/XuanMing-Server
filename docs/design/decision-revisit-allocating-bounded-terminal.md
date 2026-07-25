# decision-revisit：ALLOCATING 阶段是否需要有界自动终态（matchmaker）

> 触发：INC-20260724-001（P0，`docs/incidents/2026-07-24-p0-matchmaker-orphan-start-claim-freeze.md`）
> 剩余风险 A9。事故修复已给玩家补了 **pre-checkpoint 取消出口**（FIX-2），但 **post-checkpoint 的
> ALLOCATING 停滞仍然无界**。要不要给它加自动终态，会与 `expireOnce` 里**写死的既有决策**正面冲突，
> 按 `AGENTS.md` §7 升级为 decision-revisit，等拍板后才可落码。
> 决策级别：服务级（matchmaker 分配 saga 的终止语义）。
> **本文只定方案供拍板，未落任何代码。**

---

## 1. 现状与冲突点

事故后已落地：`ConfirmMatch` 允许 **pre-checkpoint** 取消（`BattleTarget==nil` 且
phase∈{PENDING,REQUESTING}），走既有 `failMatch` 无过错退票。

**未覆盖的一段**：一旦 `checkpointBattleAllocation` 成功固化了 exact battle target，后续任一环节
（`SignBattleTickets` / `notifyBattleStrict` / READY CAS / `pushReadyStrict`）持续失败时：

- `ConfirmMatch` 拒绝取消（此时 DS 已固化、票可能已签，假装取消会与 READY 推送打架）；
- `expireOnce` 对 `stageAllocating` 显式 keepActive、**绝不判失败**；
- 分配重试无最大次数、无阶段总时限。

⇒ 玩家可能长时间停在 ALLOCATING，没有自动终点。

**冲突的既有决策**（`services/matchmaking/matchmaker/internal/biz/match.go` `expireOnce` 内注释原文）：

> ALLOCATING 是 durable job。外部结果可能未知（尤其 allocation_uncertain），
> **本地时间绝不能把未知推断成失败并重排**。

这条决策本身是对的，且与 `CLAUDE.md` §9.22「查询超时 / 结果不确定必须返回 UNKNOWN 并重试或
fail-closed，禁止冒充失败」同源。任何「到点就判死」的方案都是在推翻它。

---

## 2. 为什么不能简单加个 `allocation_max_attempts`

事故复盘期已识别三条硬伤（记录在案，避免重复提案）：

1. **按次数不成墙钟界**。`AllocateBattle` 是同步阻塞 RPC（matchmaker 侧 `ds_allocate_timeout`
   默认 60s、allocator 侧 dev `ready_wait_timeout` 45s），attempt=12 的真实上界是 ~12 分钟而非 90s；
   且 `RunMatchLoop` 是单 goroutine，阻塞会连带卡住 `matchOnce`/`expireOnce`/`livenessSweep`。
2. **守卫极易写错**。若把上限判定放进 `advanceAllocation` 后段那把锁（该闭包**没有**
   `BattleTarget==nil` 条件），会把已 checkpoint、已签票、只是 push 卡住的 match 判 FAILED，
   并**撂下一台 Allocated DS** —— 触碰 §9.1（一人一 DS）边界。
3. **requeue 风暴**。solo 路径 `matchID := ticket.TicketId`、`RequeueTicket` 不换 ticket_id、
   `CreateMatch` 是无 NX 的 SET ⇒ 同一 match_id 每 2s 重成局，撞既有 uncertain/abandoned claim
   （`ExpireBattle(battleTTL)` 保留 2h）。要做必须配套「换新 match_id + 成局级冷却」。

---

## 3. 候选方案

| 方案 | 内容 | 优点 | 代价 |
|---|---|---|---|
| **A. 维持现状** | 不加终态；靠 ds_allocator 15s 心跳 abandoned + 上游补偿最终收敛 | 零改动；不推翻既有决策 | post-checkpoint 停滞对玩家无自动终点，§9.20 不能宣称全量满足 |
| **B. 墙钟阶段总时限 + 严格守卫** | 给 ALLOCATING 加**墙钟**（非次数）总时限；判死守卫必须同时要求 `BattleTarget==nil`；已 checkpoint 的一律不判死，转由 abort fence 链处理 | 覆盖 pre-checkpoint 的长尾；不碰已固化 DS | 仍不覆盖 post-checkpoint（而那正是 A9 的缺口）；等于把 FIX-2 的边界自动化 |
| **C. post-checkpoint 走 abort fence 终止** | 超时后不直接判 FAILED，而是推进到既有 `ABORTING`，由 `advanceAllocationAbort` 用签名 abort 向 allocator 确认释放，**只有拿到确定性 ACK 才 CAS FAILED** | 不违反 §9.22（不靠本地时间推断未知）；不撂下 DS；复用既有 abort 状态机 | 需确认 abort 链在"票已签、玩家可能已连上"时的语义；改动面最大 |
| **D. 只做可见性** | 不加终态，但把 ALLOCATING 停滞时长做成指标 + 告警，并保证客户端停在可见可重试 UI | 最小风险；符合 §9.23「可停在可见可交互形态」 | 玩家仍需手动放弃；不是自动收敛 |

---

## 4. 倾向与待答问题

**倾向 C + D 组合**：C 是唯一既有界、又不违反「不得用本地时间把未知推断成失败」的路径
（它不推断，它去**要一个确定性答案**）；D 保证在 C 生效前玩家不会面对静默。
B 的价值已被 FIX-2 大部分覆盖，单独做收益有限。A 只能作为"暂不处理并如实登记"的兜底。

拍板前需要答：

1. post-checkpoint 且**票已签**时，abort 链的正确语义是什么？玩家可能已经连上那台 Battle DS —— 
   此时终止对局是否可接受，还是应当让它跑完？
2. 墙钟上限取值的推导依据（最坏合法完成时间）——需要 `SignBattleTickets`/`notifyBattleStrict`/
   push 三段的实测分布，当前**没有证据**，不得拍数字。
3. 是否接受为此在 ALLOCATING 引入新的持久字段（阶段开始墙钟时间戳），以及它的滚动升级兼容性
   （§9.17 只允许加新字段）。

---

## 5. 状态

**未拍板，未落码。** 在拍板前，A9 在事故档中保持为**已登记的剩余风险**，
且不得因 FIX-2 已落地就声称 §9.20 已全量满足。
