# matchmaker

> 撮合服务:把队伍(team)撮合成对局(match),凑齐 `need = 2 × teamSize` 人(1v1 凑 2 / 5v5 凑 10),
> 经确认期 → 申请战斗 DS → 推玩家进场。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`
> 的 [`decision-revisit-global-matchmaker.md`](../../../docs/design/decision-revisit-global-matchmaker.md)、
> [`decision-revisit-matchmaker-single-writer.md`](../../../docs/design/decision-revisit-matchmaker-single-writer.md);
> 跨服务要约见 [`go-services.md §2.8`](../../../docs/design/go-services.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:撮合成局 → 确认期 → 申请战斗 DS → 推 READY。
- **权威态**:排队票据(ticket)、对局镜像(match)、玩家归属(player→ticket claim)全在 **Redis**
  (进程内只做缓存 + 单写者撮合循环)。
- **状态权属**(不变量 §22):matchmaker 是 `MATCHING` / `BATTLE` 两个 Location 状态的权威;`HUB` 由
  Hub DS 上报。player_locator 只作 presence / 最近活跃投影,不是归属权威。
- **不做的事**:不算 MMR 增减 / 经验 / 掉落(那是 battle_result 的活,不变量 §9.6);不持久化 `HUB`。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20011` | 客户端 RPC(经 Envoy)+ 内部 RPC |
| HTTP | `:21011` | 仅 `/metrics` |

## 对外接口

代码入口:`internal/service/match.go`(gRPC service 层,从 JWT ctx 取 `player_id` 防伪造,再转 biz)。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `StartMatch(team_id, map_id)` | 客户端 | 把队伍入队,返回 `ticket_id`(= QUEUEING 阶段句柄) | JWT `captain_id` |
| `CancelMatch(player_id)` | 客户端 / team 联动 | 取消匹配(以 caller 本人为准;内部路径按 `player_id`) | JWT / 内网直连 |
| `ConfirmMatch(match_id, accept)` | 客户端 | 确认 / 拒绝已凑齐的对局 | JWT `player_id` |
| `GetMatchProgress(match_id)` | 客户端 | 拉一次进度;`match_id=0` 时按本人反查(重连兜底) | JWT `player_id` |
| `ReleaseMatch(match_id, player_ids)` | battle_result(内部) | 结算后释放全部撮合状态,幂等 | **拒绝玩家 JWT**(callerID 必须 =0) |
| `ResolvePlayerMatchContext(player_id)` | login(内部) | 冷重连时查本人 durable 撮合态 + 重签 READY 凭据 | Login HMAC(独立信任域) |

> **无 `StreamMatchProgress`**:进度变化经 kafka `pandora.match.progress`(key=`player_id`)推给
> Hub DS 转 UE,客户端按需 `GetMatchProgress` 拉一次兜底。**4 个客户端 RPC 全"已受理型"(协议原则 3,
> 见 [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md))**:返回 code 后 UI
> 状态机由 push 驱动,不靠 RPC 返回值判定成局。

## 目录结构(Kratos 标准分层,对齐 login / push)

```
cmd/matchmaker/main.go         启动入口(redis + team/locator/ds_allocator client + 单写者 RunMatchLoop 装配)
etc/matchmaker-dev.yaml         5v5 PVP 开发配置
etc/matchmaker-pve.yaml         PVE 副本配置(不同 game_mode / map_id,各自独立撮合)
internal/
  conf/conf.go                 配置结构(MatchConf + LeaderConf + JWT/DSTicket)
  service/
    match.go                   RPC 入口(实现 matchv1.MatchServiceServer,JWT 身份下沉鉴权)
    configtable_admin.go       配置表热更 admin RPC
  biz/
    match.go                   MatchUsecase 核心(StartMatch/ConfirmMatch/撮合循环/DS 割当)
    allocation_saga.go         DS 割当 saga(checkpoint / fence / 签票)
    region_affinity.go         跨 region 溢出策略(两级撮合)
    helpers.go                 装箱(binPack)/ MMR 窗口(withinWindow)/ 成员工具
    ds_stub.go                 StubDSAllocator(本地无 ds_allocator_addr 时兜底)
  data/
    match.go                   Redis 票据 / match / claim 仓储
    team_reader.go             team 服务 gRPC client(GetTeam)
    locator_client.go          player_locator gRPC client(位置上报 / 在线校验)
    ds_allocator.go            ds_allocator gRPC client(AllocateBattle / 签票)
  model/allocation.go          BattleAllocation 领域模型
  server/                      grpc / http server 注册
```

## 对局状态机

```
StartMatch ──► QUEUEING ──► (FOUND) ──► CONFIRM ──► ALLOCATING ──► READY
   (受理)       排队中        凑齐          确认期        拉 DS 中        就绪进场
                  │             │            │             │
                  └─────────────┴────────────┴─────────────┴──► FAILED
                       取消 / 拒绝 / 超时 / 掉线判责 / 分配失败
```

阶段常量:`internal/biz/match.go:123`(`stageQueueing / stageFound / stageConfirm /
stageAllocating / stageReady / stageFailed`)。`ticket_id` / `match_id` 都是 Snowflake、非秘密——
`GetMatchProgress` 一律以 JWT `player_id` 校验成员身份,不认 ID 本身(防外挂拉别人对局)。

## 核心调用链

### 1. StartMatch —— 只做「入队受理」,不等成局

看 `internal/biz/match.go:422` 的 `StartMatch` 函数体,它做的全是**准入校验 + 写一条 durable 排队 saga**,
最后 `return ticketID, nil`:

- `validateMapID` — 校验 `map_id` 是关卡表里的战斗类关卡(`match.go:425`)
- `resolveMembers` — 经 `TeamService.BeginTeamMatch` 在 team 锁内冻结名单(队长 / 存在性校验在那把锁里;**ready 门槛按关卡表 `ready_mode` 逐图决定**,由 `requiresPreMatchReady(map_id)` 解析后经 `require_ready` 传入,2026-08-18),本地只校验人数 ≤ 一方人数
- `ensureNoneInBattle` — 战斗中的人不许再排队(不变量 §1,`match.go:437`)
- `preflightStartClaims` — 一人只能在一个队列的快照检查(`match.go:441`)
- `CreateStartOperation` — 写一条 durable 排队 saga(`phase = ACCEPTED`,`match.go:462`)

返回的 `ticketID` 是你 **QUEUEING(排队中)** 阶段的句柄,**不是「匹配成功」**(`match.go:11` 顶部注释:
4 个 RPC 全"已受理型",UI 状态机由 push 驱动)。

> **关键:连"真正入队"都是异步的。** `StartMatch` 的唯一提交点是那条 `MatchStartOperation` durable saga
> (`match.go:460`)。真正的票据落库、玩家 claim、入队 ZADD 由后台 `advanceStartOperation`(`match.go:644`)
> 按阶段推进:`ACCEPTED → TICKET_READY → CLAIMING → CLAIMS_READY → QUEUED`。这样玩家断线、RPC ctx 取消、
> 进程重启都不会中断入队;失败自动补偿(`COMPENSATING`)回滚 claim。

### 2. 后台撮合循环 RunMatchLoop(单写者)

`RunMatchLoop`(`match.go:2182`)每 `MatchInterval`(默认 2s)跑一轮。多副本部署时经 etcd 选举,**只有当选
副本跑本循环**(`cmd/matchmaker/main.go` 用 `etcdleader.Run(..., uc.RunMatchLoop)` 装配;单副本 / dev 直接
`go uc.RunMatchLoop`)——避免同一玩家进两场(不变量 §1)。每轮顺序:

```
RunMatchLoop (每 2s)
├─ reconcileStartOperationsOnce  修复 StartMatch saga 的 due 索引(5s 节流)
├─ advanceStartOperationsOnce    推进每条排队 saga ACCEPTED→…→QUEUED
├─ matchOnce                     ★ 撮合:凑齐 need 人建 match
├─ reconcileActiveOnce           从 canonical match 修复 active ZSET(5s 节流)
├─ advanceAllocationsOnce        ★ 推进 ALLOCATING→READY;补推滞留 READY
├─ expireOnce                    确认超时 / TTL 过期收尾
└─ livenessSweepOnce             清扫队列里掉线玩家的死票(节流)
```

### 3. matchOnce —— MMR 贪心装箱

`matchOnce`(`match.go:2548`)→ `partitionTicketsByMap`(按副本分组)→ `formMatchesInPool`
→ `greedyFormMatches` → `formMatch`:

1. `RangeQueueTickets` 取队列全票据,过滤已消失 / 已进 match 的,按 `avg_mmr` **升序**排序。
2. **按 `map_id` 分组**(`partitionTicketsByMap`,`match.go:2663`):同 `game_mode` 下不同副本各自成局,
   新增副本自然形成新组,无需改代码。`map_id=0`(省略=默认副本)归一化到 `cfg.MapId` 同池。
3. **MMR 窗口贪心装箱**(`greedyFormMatches`,`match.go:2697`):从每张票起,累积后续票进一个组,直到
   总人数 `= need` 且 MMR 跨度在**动态窗口**内(`withinWindow`:窗口 = `MmrBaseWindow` +
   `MmrWidenPerSec × 等待秒数`,上限 `MmrMaxWindow`——等得越久越容易凑上)。`binPack` 拆成两边各 `teamSize`。
4. **多 Region**(`router` 已配,阶段 3):`formMatchesInPool`(`match.go:2599`)升级为两级——先各 region
   桶内独立贪心(同 region 优先、低延迟),本 region 凑不齐且久等的票据再进跨 region 溢出贪心(受
   `withinCrossRegionCap` 比例上限约束)。单 Cell / dev(`router==nil`)退化为单桶贪心,行为不变。
5. 成局走 `formMatch`(`match.go:2805`):**先建 match(stage=CONFIRM,写 active ZSET)→ 逐张预留票据
   → 持久化 claim → 推 FOUND/CONFIRM**。顺序不可倒:先建 match 保证任意点崩溃都能被 `expireOnce` 自愈,
   不留"match_id 指向不存在 match"的孤儿票据。

### 4. ConfirmMatch —— 确认期

`ConfirmMatch`(`match.go:1208`)在 `UpdateMatchWithLock` 事务内按 accept / reject 分流:

- **拒绝**(`accept=false`)→ `stage=FAILED` → `onMatchFailed`(`match.go:1362`):按**定责规则**把拒绝者
  所在票据删除释放归属,其余无过错票据**退回队列**(保留排队时长)+ 补推 QUEUEING。超时(`expireOnce`
  触发,`rejecterID=0`)则把含未确认 AFK 成员的票据判责,避免同一批人 + 挂机者无限重凑。
- **全员确认** → `stage=ALLOCATING`,写 `AllocationOperationId` + `phase=PENDING`。**最后一名确认者只提交
  这个 ALLOCATING job**,真正的 Allocate / placement / READY 交给 `RunMatchLoop` 后台 worker
  (`match.go:1273`),不再绑定玩家 RPC ctx。
- **仍有人未确认** → `stage=CONFIRM`,推 CONFIRM 进度给全体。
- 锁定态(ALLOCATING/READY)后:accept 幂等成功;reject 诚实报错(全员已确认不可反悔)。

> `WalkIn`(PVE walk-in 生产路径,见下方「PVE 实例 / walk-in 直进路径」)/ `AutoConfirmMatch`
>(versus 路径 dev/压测)会跳过客户端确认,但**仍先建 CONFIRM 态、落全部票据/claim,再 CAS ALLOCATING**
>(`queueAcceptedMatchAllocation`,`match.go:1297`)——保证崩溃不会给半成局拉起 Battle DS。

### 5. DS 割当 saga —— ALLOCATING → READY

`advanceAllocationsOnce`(`match.go:2489`)遍历 active 里的 match,按 stage 分派;ALLOCATING 走
`advanceAllocation`(割当 saga,`internal/biz/allocation_saga.go`):

```
申请 DS (AllocateBattle, map_id 透传决定加载哪张关卡)
  └─► checkpointBattleAllocation  CAS 把精确 target(pod/uid/epoch/allocId)写到 match
        └─► fenceRequestingAllocationCheckpoint  每个外部副作用前的授权线性化点
              └─► SignBattleTickets  给每个玩家现签 battle DSTicket(新 jti,sub 锁本人)
                    └─► CAS stage=READY + 推 READY(带各自票据 + ds_addr)
```

- **checkpoint 先于签票**:分配结果落 match 后才签票,进程重启不必猜分配了哪台 DS;同一 operation 重试
  永远复用该 checkpoint,不同 target 视为冲突绝不覆盖(`allocation_saga.go:41`)。
- **失败补偿**:签票前若要放弃,走 `AbortBattleAllocation`(带 `operation_id`,payload 鉴权)durable 补偿。

### 6. READY 推送 —— at-least-once

`finalizeReadyMatch`(`match.go:3152`)把 READY 交付到全体成员。**机械不变量:
`READY ∈ active ZSET ⟺ READY 推送交付未确认`**——全员推送成功才 `RemoveActive`;READY CAS 后崩溃或
Kafka 不可用时 match 滞留 active,`advanceAllocationsOnce` 每 tick 幂等补推(每次重签全员新 jti)直到
交付或 match TTL 到期。

- **为什么必须补推**:组队非队长成员没有 `match_id`,`pandora.match.progress` 推送是其得知 READY /
  Battle 落点的**唯一服务端主动通道**(2026-07-20 事故修复)。
- **原则 3 例外**:match 进度 `callerID=0` → 推给所有人(含发起方),key=`player_id` 保证同玩家有序。
- **客户端契约**:必须容忍**重复 READY 推送**(幂等,coordinator 单写者保证不双 Travel,§9.19)。

### 7. CancelMatch / ReleaseMatch

- `CancelMatch`(`match.go:840`):优先取消仍在进行的 StartMatch saga(`cancelStartingMatch`);否则按票据
  状态——仍排队则 CAS 条件删票 + 释放全体归属;已被撮合进 match 则等价于**拒绝确认**(走第 4 步失败流程)。
  用 `DeleteTicketIfUnmatched`(WATCH CAS)防"读到未撮合就盲删"撞上并发 `ReserveTicket`(会导致同人两场)。
- `ReleaseMatch`(`match.go:1037`):battle_result 结算落库后调用,**主动彻底释放**本局 claim + 票据 +
  match 镜像,玩家回 Hub 即可立刻再匹配。幂等,重复 / 已释放均返回 OK。
  ⚠️ **这是唯一释放点,不会自愈**:claim / 票据 / match 在非终态下是**无 TTL 的持久状态**
  (`MatchRepo.ClaimPlayer` 契约:「网络断开或进程停机绝不能靠 TTL 暗中释放玩家」;`ticket_ttl` /
  `match_ttl` 只在显式终态后用于留存)。`ReleaseMatch` 没跑成功,玩家会**永久**卡在
  `ErrMatchAlreadyMatching`(4002),不是"等 30 分钟就好"——所以失败必须由 battle_result outbox
  持续重试到成功。

## 为什么点匹配不是马上返回「匹配成功」

**「点匹配」和「匹配成功」是两件事**,中间隔着"凑齐对手 → 全员确认 → 拉起 DS"三段异步流程,时机都不确定:

1. **凑对手需要时间**:撮合是后台 `matchOnce` 按 MMR 相近程度贪心装箱。点匹配那一刻队列里未必有足够、
   分段相近的对手,可能等几秒到几十秒(还随 `MmrWidenPerSec` 动态放宽),不能让一个同步 RPC 一直挂住。
2. **成局后还有确认期**:凑齐后进 CONFIRM,要全员点接受才继续;有人拒绝 / 超时就 FAILED,其余退回队列。
3. **还要拉战斗服务器**:全员确认后才 `AllocateBattle` 申请 Battle DS、签票,成功才到 READY。

所以设计成**已受理型 + push 驱动**:`StartMatch` 返回的成功 = 「你已成功进入排队」(QUEUEING),真正的成局
(READY)由后台流水线完成后经 `match.progress` push 单独通知。这同时满足 durable saga 崩溃安全
(`match.go:460`)和不变量 §23「登录/匹配全程 push 驱动、有界恢复、不卡玩家」。

> **注意**:上面这套「撮合延迟」只发生在 **PVP**。**PVE 不走撮合**,见下节。

## PVE 实例 / walk-in 直进路径

matchmaker 是**一个二进制、按 game_mode 分实例部署**:PVE 与 PVP 用配置区分、互不串池
(队列命名空间 `pandora:match:<mode>:queue` + `requireLocalGameMode` fencing,改一个实例不影响另一个)。
上一节的「撮合延迟」只对 PVP 成立;**PVE 实例(`matchmaker-pve.yaml`)不撮合、不确认**:

| | PVP 实例(`matchmaker-dev.yaml`) | PVE 实例(`matchmaker-pve.yaml`) |
|---|---|---|
| `walk_in` | `false`(走撮合) | **`true`(walk-in 直进)** |
| `game_mode` / 端口 | `5v5_ranked` / `:20011` | `pve_coop` / `:20018` |
| 成局方式 | 凑齐 `2×team_size` + 确认期 | 每张票(单人/整队)立即成局(`formSoloMatch`) |
| 等对手 / 确认期 / MMR | 有 | **无**(直接建 `confirmAccepted`,跳过) |

- **路由**:Envoy 按 header `x-pandora-game-mode: pve` 把 MatchService 路由到 PVE 实例;不带头 = 默认 PVP。
- **仍是已受理型**:PVE 省掉的是「撮合」,但**拉 Battle DS 加载副本的延迟省不掉**(`ds_allocate_timeout` 默认 60s、dev 大图 yaml 已调 150s,
  冷加载几十秒,与 PVP 相同)。所以 `StartMatch` 照样返回 `ticket_id`,进场靠 READY push;`formSoloMatch`
  也由后台 `RunMatchLoop` 驱动(≤1 tick 调度,相对 DS 冷加载可忽略),不做成同步 RPC 返回 `ds_addr`。
- **正名与旧键兼容(2026-07-25)**:本开关原名 `enable_solo_match`(名字含 "solo"、注释曾写「测试专用」,
  与「入口是否撮合」的真实语义和 PVE 生产用途都不符),已正名为 **`walk_in`**。按 §9.21
  expand→migrate→contract,`conf.Defaults()` 仍读旧键并 **OR 并入** `walk_in`(漏迁移的部署若被静默判
  false,PVE 会退化成「等对手撮合」而 PVE 无单边成局逻辑 → 玩家永远等不到人),启动时打
  `deprecated_config_key` Warn;线上不再出现该 Warn 后才可删除旧字段。函数名 `formSoloMatch` 与
  `solo_match_found` 日志键**有意未改**(可观测性契约,被事故档案时间线引用)。详见
  [`decision-dungeon-entry-modes.md`](../../../docs/design/decision-dungeon-entry-modes.md)。
- **未来扩展**:「机器人补位」是该分叉点上的新 formation(等真人超时后用 bot 补满),
  同样只加在 PVE 侧,不影响 PVP 的 versus 撮合。

### 同一张图两个入口(关卡表 `entry_mode = BOTH`)

PVE 的目标形态是**同一张副本既能排队撮合、也能人不够时自己进**,由玩家在面板上二选一:

| | 排队撮合(`MATCHMAKE`) | 自己进(`WALK_IN`) |
|---|---|---|
| 成局条件 | 凑满 `side_count × team_size` **才**开局 | 当前这张票立即开局 |
| `team_size` 的用法 | **凑齐目标**(必须坐满) | **上限**(超编拒绝),实到几人带几人 |
| `min_team_size` | **不看**(排队正是为了凑人,单排天经地义) | **下限闸**,不足拒 `ERR_MATCH_TEAM_TOO_SMALL`(4009) |

- **「等不及」是玩家点出来的,不是服务端猜的**:撮合永远只凑满,不做「等 N 秒自动降到下限」。
  想少人开就改点直进 —— 少一套超时放宽的状态机,也少一类「我明明还想等」的投诉(§15.3)。
- **入口是「表允许什么」× 「玩家选什么」的交集**,`resolveEntryMode` 求交并 fail-closed:
  表填 `BOTH` 时请求**必须**明确选一种,留空即拒 `ERR_MATCH_ENTRY_MODE_DENIED`(4010)。
  **不替玩家猜** —— 猜错等于玩家以为在排队实则已单刷进本(或反之),而进本会消耗次数 / CD,
  不是重试能挽回的错误。
- **落定的进法写进票据**(`MatchTicketStorageRecord.entry_mode`),撮合循环按票分流而不是回查表:
  `BOTH` 的图上「这张票排队还是直进」只有玩家的选择知道,表答不出来;且 leader 交棒 / 进程重启后
  仍要按同一口径分流。存量旧票(无该字段)回退 `isWalkInMap`,**逐值复刻旧二进制的判定**
  (含 `BOTH` 落 `cfg.WalkIn` 这条——旧二进制不认识该枚举值),保证共存窗口内两边对同一张票的
  分流一致(§9.21)。
- **下限只在进场那一刻判**,不是局内不变量:开局后有人退出 / 掉线掉到下限以下,
  **绝不允许**据此踢人或强制收局(§9.19 / §9.20)。
- **PVP 不受影响**:PVP 关卡行 `entry_mode` 保持 `MATCHMAKE`、不填 `min_team_size`,
  撮合与装箱路径零改动(每方仍必须恰好坐满,不会出现 3v2)。

## 关键设计点 / 不变量

| 主题 | 约束 | 代码锚点 |
|---|---|---|
| 一人一队列 | `ClaimPlayer` SETNX 线性化;确认期有人拒绝 → 其余退回队列(保留排队时长) | `preflightStartClaims` / `onMatchFailed` |
| durable saga | RPC 唯一提交点是 durable operation,后台 worker 推进,崩溃 / 断线不中断 | `advanceStartOperation` / `advanceAllocation` |
| 单写者 | 撮合循环全局优化,天然单写者;多副本经 etcd 选举,失主自动交棒(不停机滚动) | `RunMatchLoop` + `LeaderConf` |
| 先建后引用 | 先建 match(进 active)再预留票据 / 签票,崩溃可被 `expireOnce` 自愈,不留孤儿 | `formMatch` / `queueAcceptedMatchAllocation` |
| allocation checkpoint | 分配结果先落 match 再签票;重试复用 checkpoint,异 target 视为冲突不覆盖 | `checkpointBattleAllocation` |
| READY at-least-once | `READY ∈ active ⟺ 交付未确认`;幂等补推,全员成功才移出 active | `finalizeReadyMatch` |
| 战斗中禁匹配 | 权威是 player_locator BATTLE 状态;查询失败默认 fail-closed(生产安全) | `ensureNoneInBattle` |
| 按副本分池 | 同 game_mode 下不同 `map_id` 各自撮合,`0` 归一化到 `cfg.MapId` | `partitionTicketsByMap` |
| 鉴权下沉 biz | `GetMatchProgress` / `ReleaseMatch` 以 JWT 成员身份为准,不认 ID | `service/match.go` |

## 配置项(`internal/conf/conf.go`)

| 键(`match.*`) | 默认 | 说明 |
|---|---|---|
| `team_size` | `5` | 一方人数(`need = 2 × team_size`);钳到 `[1, MaxLevelTeamSize]` 防 OOM/panic。副本可经关卡表 `team_size` 覆盖。**直进入口下限见关卡表 `min_team_size`,无全局配置项**(下限是逐副本的玩法参数,不是部署参数) |
| `confirm_timeout` | `15s` | 确认期时长 |
| `match_interval` | `2s` | 后台撮合循环扫描间隔 |
| `ticket_ttl` / `match_ttl` | `30min` | **仅显式终态后的留存时长**;非终态的票据 / claim / match 是无 TTL 的持久状态,只能被显式取消 / 失败 / `ReleaseMatch` 释放 |
| `mmr_base_window` | `200` | 初始 MMR 窗口半宽 |
| `mmr_widen_per_sec` | `20` | 每等待 1s 窗口放宽量 |
| `mmr_max_window` | `2000` | 窗口放宽上限 |
| `ds_allocate_timeout` | `60s`(dev PVE yaml 设 `150s`) | 调 ds_allocator 客户端超时(须 ≥ 其 server.grpc.timeout;冷加载大图链:ready_wait 120s < allocator server 150s ≤ 本值) |
| `map_id` | `1` | 默认副本(客户端省略 `map_id` 时兜底) |
| `game_mode` | `5v5_ranked` | **撮合池 / 部署标识**:池命名空间(`pandora:match:<mode>:queue`)+ JWT audience + 透传 ds_allocator(PVE 实例为 `pve_coop`)。**只表达"归哪个池"** —— 人数看关卡表 `team_size`×`side_count`(现有 `5v5_ranked` 的图 team_size 是 1 和 3,名字里的 5 是历史遗留标签),算不算段位看关卡表 `rating_mode`;禁止按本值字符串解析人数或推断计分 |
| `optimistic_retry` | `3` | WATCH/MULTI/EXEC 乐观锁重试次数 |
| `battle_gate_fail_open` | `false` | locator 查询失败时是否放行入队(生产必须 false) |
| `liveness_gate_enabled` | `false` | 是否启用在线保活两道离线门。**INC-20260724-001 后全部实跑配置已回退为 `false`,重开前置见下方《成局最终门的证据契约》** |
| `walk_in` | `false` | 「即时开局 / walk-in」分叉:PVP=false 走撮合;**PVE 实例=true**(单人/整队直进副本,见「PVE 实例」节)。旧键 `enable_solo_match` 仍兼容读取(OR 并入)并打废弃 Warn |
| `auto_confirm_match` | `false` | 撮合(versus)路径跳过确认期。**2026-08-18 起本开关不是唯一决定者**:最终判定是 `auto_confirm_match \|\| 本图 ready_mode==PRE_READY`。填了 `PRE_READY` 的图玩家已在组队面板点过准备,**恒不进确认期且关不掉**(让同一局点两次准备就是配错了)。本开关只剩「全图强制跳过」一个语义,给无 UI 的脚本联调 / 压测用;要让某张图有确认期请改关卡表,不要改本开关。dev 档为 false(robot/gatecheck 已支持手动确认)。与 walk-in 无关 |
| `leader.enabled` | `false` | 撮合循环单写者选举(多副本必开) |

**信任域隔离**(`Validate` 强校验):`match_resume_auth_secret`(Login 恢复读)、`allocation_abort_auth_secret`
(割当补偿)、玩家 JWT `secret` 三者密钥**必须互不相同**,否则启动拒绝。

## 成局最终门的证据契约(INC-20260724-001)

`liveness_gate_enabled` 控制的两道门——成局最终门 `findOfflineMembers` 与队列扫除
`livenessSweepOnce`——**当前一律关闭**,因为它们赖以判死的证据对目标人群结构性失效。

**为什么必须关(机械证明,非经验判断)**

| 环节 | 事实 | 位置 |
|---|---|---|
| MATCHING 投影唯一写者 | matchmaker `notifyMatching`,成局那一刻写一次,TTL 30s | `internal/biz/match.go` |
| Hub DS 心跳续期 | **只对 `state==3`(HUB) 续期**,MATCHING(=4) 直接 `continue` | `player_locator/internal/data/location.go:241` |
| Hub DS 改写回 HUB | 被 `guardTransition` 拒(`ErrLocatorConflict`) | `player_locator/internal/biz/locator.go:224-229` |
| MATCHING 释放通道 | **不存在** | 全仓 grep 无释放点 |

⇒ 成局最终门查询的正是「已成局 match 的成员」(MATCHING 态):**30s 内零真阳性,30s 后全假阳性**。
本事故实测 `solo_match_found → 判死 = 31.07s`(30s TTL + 一个 2s tick),与玩家是否在线无关。
连带:MATCHING key 过期后 `RefreshHubLocations` 只 `EXPIRE` 不创建,记录无法重建,
该玩家此后恒判离线,再排队的票据也会被队列扫除误删 —— **故两道门必须一起关**。

**重开前置(缺一不可)**

1. MATCHING 投影具备**创建 / 续期 / 释放**完整三段闭环(注意:放开续期而不补释放,会让匹配失败的
   玩家 presence 被 Hub 心跳无限续成 MATCHING、永远回不到 HUB,那是更严重的 §9.20 级卡死);
2. 判死证据改用**权威信号**而非 presence(§9.22:key miss 只说明 presence 不可见,不能单独证明
   玩家已离开,更不得冒充 `OFFLINE`);
3. 具备可秒级回滚的开关与 blanket-false 护栏(证据源整体失效时不得一次性判死全部对局)。

**关闭后的残余覆盖缺口(诚实登记,不得宣称无损)**

- 放弃「不给残局白拉 DS」的意图:每个残局多浪费一次分配。兜底 = PVP `confirm_timeout` 淘汰不响应者、
  ds_allocator 15s 心跳 abandoned 回收(§9.4);单人 PVE 无第三方受害者。
- 掉线者死票留在队列会被凑局,其余成员多吃一轮 FAILED(票据退回队列,保留排队时长)。

**配套的玩家出口**:关门后 ALLOCATING 阶段不再有任何自动终止路径,故 `ConfirmMatch` 增加了
**pre-checkpoint 取消**(仅 `BattleTarget==nil` 且 phase∈{PENDING,REQUESTING} 允许 reject,
走既有 `failMatch` 无过错退票;ABORTING / 已 checkpoint / READY / 未知阶段一律 fail-closed)。
两者**必须同批存在**,只关门不补出口 = 把误杀换成永久卡死。
post-checkpoint 的 ALLOCATING 停滞仍无界,属已登记剩余风险(事故档 §10 A9)。

## 本地启动

```powershell
# 1. 基础设施(redis;起 team / player_locator / ds_allocator 后可跑全链,留空则走骨架/Stub)
pwsh tools/scripts/dev_up.ps1

# 2. 启 matchmaker(5v5 PVP dev 配置)
go run ./services/matchmaking/matchmaker/cmd/matchmaker -conf services/matchmaking/matchmaker/etc/matchmaker-dev.yaml
```

> 起 **PVE 实例**改用 `-conf services/matchmaking/matchmaker/etc/matchmaker-pve.yaml`
>(`walk_in: true`,walk-in 直进,端口 :20018)。`auto_confirm_match` 仅用于撮合路径的
> dev/压测省人工确认,PVP 正式对局保持 false。

## 关联文档

- [`go-services.md §2.8`](../../../docs/design/go-services.md) — matchmaker 要约(RPC / 核心算法 / 不变量)
- [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md) — 已受理型协议原则 3 及其 push 例外
- [`scale-cellular-20m.md`](../../../docs/design/scale-cellular-20m.md) §4.4 — 两级 region 撮合与 battle placement
- [`decision-revisit-global-matchmaker.md`](../../../docs/design/decision-revisit-global-matchmaker.md) — 跨 region 溢出阈值 / 段位桶
- [`decision-revisit-matchmaker-single-writer.md`](../../../docs/design/decision-revisit-matchmaker-single-writer.md) — 撮合循环单写者选举
- [`decision-dungeon-entry-modes.md`](../../../docs/design/decision-dungeon-entry-modes.md) — PVP 撮合 vs PVE walk-in / `walk_in` 正名与旧键兼容契约
- [`owner-authority.md`](../../../docs/design/owner-authority.md) — 进场归属 / owner_epoch(§22/§23 的权威本体)
- [`battle-reconnect.md`](../../../docs/design/battle-reconnect.md) — READY 后进 Battle DS 的重连 / no-freeze 契约
