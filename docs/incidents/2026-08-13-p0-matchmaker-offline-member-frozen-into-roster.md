# [INC-20260813-001][P0] 打完一局队伍不复位 ready，队友还没回大厅就被冻进下一局票据

> **状态**：方案 A 已拍板并落码(2026-08-17),复核阻断解除;待部署验证(未关闭)
> **类型**：`availability`
> **环境**：本机 dev 栈（`start.ps1 -Mode local`，21 个 Go 服务跑在宿主 + editor 形态 UE DS）
> **首次发生时间（UTC）**：2026-08-13 12:14:10（队长开下一局、名单被冻结那一刻）
> **首次发现时间（UTC）**：2026-08-13 12:14:38 前后（玩家侧表现为「对面少一个人」，逐层排查后才定位到服务端）
> **负责人**：待指定
> **受影响服务/版本**：`matchmaker`、`team`、`player_locator`、`ds_allocator`（均为工作副本，无镜像）；Battle DS `-port=7802`
> **最后更新**：2026-08-13（v2 更正）

> ## ⚠️ v1 根因判断已被推翻（2026-08-13 当日更正）
>
> v1 把 Hub DS 的 `玩家离开大厅，上报 player_locator 断线`（12:11:37）读成「玩家退出客户端」，
> 据此认定根因是「离线 160 秒未满 team 的 180s 阈值仍被冻进票据」。
>
> **反查其它 battle DS 的 backup 日志后确认：那一行是他正常 travel 进入*上一局***
> （match `23458733413662720`，DS `-port=7801`，12:11:37 `Join succeeded`）。他真正的退出是
> 12:12:55 —— **阵亡后自己先退了战斗，比该局结算还早 13 秒**。
>
> 性质因此由「罕见的挂机掉线」上调为「**连打第二局的常态窗口**」：任何人打完一局、
> 队长手快立刻再开，都会命中。v1 的三条「排除进场掉线」论证保留在 §2.4，
> 其中第 1 条（"那一刻这局还不存在"）**只对新的那一局成立**，遗漏了上一局的存在，是本次误判的直接原因。
>
> 根因排序、修复清单与验收用例已按 v2 重排；v1 落码的四项修复**全部保留**（它们仍是真实缺口），
> 只是不再是第一位。

## 0. 一句话结论

**上一局他先退了，人还在「结算 → 回大厅 → 重登」的路上，队伍却仍是 READY，队长立刻开了下一局，把他冻进了票据。**

team 侧**从来没有任何一条 match-ended 路径** —— 全仓只有 `LeaveTeam` / `Kick` / 离线摘人会把
`TeamState` 打回 `FORMING`。于是一局打完，队伍仍停在 `TEAM_STATE_READY`、全员 ready 标记原样保留，
队长在队友退出仅 **75 秒**后就开了下一局；`StartMatch` 全链七道门又**没有任何一道看「人还在不在」**，
于是 `match_found players=6` → Battle DS 拿到 6 人 roster 只进来 5 个，一场 3v3 打成 3v2，
DS 的终局判定还按 `roster=6` 算。

**这跟掉线无关**：缺席者全程没有任何异常，只是连打第二局的正常窗口。所以光靠离线判定
（不论阈值多短）都堵不住 —— `offline_leave.threshold=180s` 与本事故无关，**不要去调它**。

## 1. 影响与范围

- **玩家影响**：一方 3 人、另一方 2 人，对局不公平且无法自愈；缺席者本人重登后停在选角界面，既回不到对局也收不到任何提示。
- **影响人数/对局/请求数**：本机联调环境，6 名测试账号、1 局（`match_id=23459506507776000`）。
- **服务影响**：无崩溃、无重启、无错误码。**全链所有服务都认为自己成功了** —— 这正是本事故最难发现的地方。
- **数据与安全影响**：无数据丢失、无越权。
- **开始/结束时间**：UTC 12:14:18 起；该局自然打完，无恢复动作。
- **是否仍可复发**：**是，且可稳定复现**（复现步骤见 §12.1）。修复未部署前每次都会发生。
- **严重级别判定理由**：命中 `index.md` §1 第二条「导致玩家……无法进场」——缺席者被算进 roster 却物理上无法进入该对局，且在场玩家被迫打一场缺员局。虽只在本机环境观察到，根因是**与部署形态无关的服务端准入缺口**，生产同样成立。

## 2. 第一现场与证据

### 2.1 症状

- **客户端症状**：进场后发现对面少一人；缺席者本人重登后拿到大厅票，停在选角界面，无任何「你正在一场对局中」的提示。
- **服务端症状**：无任何错误日志。`match_found players=6`、`battle_warming players=6`、`match_ready players=6` 全部正常；Battle DS 也正常宣告 `roster_count=6`。
- **K8s/Agones 状态**：本轮为 `mode=local`，无 K8s 参与。

### 2.2 原始证据

采集位置：`Server/run/dev/logs/*.err.log`（Go 服务业务日志）与 UE `Pandora*.log`。UE 日志前缀即 UTC，尖括号内为本地时间（UTC+8）。

```text
# ① 上一局(Pandora_3-backup-2026.08.13-12.13.33.log,-port=7801,match 23458733413662720)
#    12:11:37 那行 Hub「离开大厅」是 travel 进这一局,不是退出客户端
[12.11.37:538] LogPandoraDSAuth: InitNewPlayer accepted player=23458570204905472
[12.11.37:539] LogTemp: [SpawnPawn] player=23458570204905472 RoleId(CfgId)=1002 Camp=17 开始生成主角 pawn
[12.11.37:908] LogNet: Join succeeded: loong-D7FE5A334A5900
[12.12.48:105] LogPandoraBattleFlow: 玩家死亡终局判定：dead_player=23458570204905472 roster=6 dead=2
[12.12.55:095] LogNet: UNetConnection::Close: RemoteAddr: 192.168.2.251:49809, …
               <<<< 他阵亡后自己先退了,比本局结算还早 13 秒
[12.13.08:716] LogPandoraBattleFlow: 上报战斗正常结算：match_id=23458733413662720 players=6
[12.13.22 ~ 12.13.33] 其余 5 人才陆续 Close,回大厅

# ② team.err.log —— 两次 roster lock 之间**没有任何 set_ready / state 变化**
DEBUG msg=team_set_ready team_id=23457638197002240 player_id=23458570204905472 ready=true new_state=TEAM_STATE_READY
DEBUG msg=team_match_roster_locked team_id=23457638197002240 … members=3   <-- 上一局 12:11:17
--- 中间打了一整局,team 侧一条日志都没有 ---
DEBUG msg=team_match_roster_locked team_id=23457638197002240 … members=3   <-- 新一局 12:14:10,仍是 3 人 READY

# matchmaker / ds_allocator —— 全链无一处报错
[12:14:18.166] INFO msg=match_found   match_id=23459506507776000 players=6 auto_confirm=true
[12:14:18.178] INFO msg=battle_warming match_id=23459506507776000 ds_addr=…:7802 players=6
[12:14:36.821] WARN msg=rpc_slow op=/pandora.ds.v1.DSAllocatorService/AllocateBattle latency_ms=18446

# Battle DS(Pandora_4.log,-port=7802)—— 收 6 人 roster,实到 5 人
[2026.08.13-12.14.28:094] LogPandoraAgones: 本地 battle 准入元数据已从 env 装载：roster_count=6 …
[12.14.37 ~ 12.14.38] InitNewPlayer accepted ×5 / Join succeeded ×5
                      （23458570204905472 一次 TCP 连接都没发起过）
[12.16.14:308] LogPandoraBattleFlow: 玩家死亡终局判定：roster=6 dead=1 …   ← 终局仍按 6 算

# login.err.log —— 他在 match_found 之后 1 秒重登,被发了大厅票
[12:14:19.038] DEBUG msg=hub_assigned player_id=23458570204905472 hub_pod=pandora-hub-local-f33f787f
[12:14:19.038] DEBUG msg=login_ok     player_id=23458570204905472
                （此后没有 select_role_ok，Hub DS 也再没有他的 InitNewPlayer）
```

关键时序：他 12:12:55 退出上一局 → 队长 **12:14:10** 就开了下一局（相隔 **75 秒**）→ 他 12:14:19 才 `login_ok`。
也就是说，**新一局的名单在他重新登录之前 9 秒就已经冻结了** —— 他没有任何机会「回到大厅、重新点准备」，
因为服务端根本没要求他重新点。

次级时序：`match_found`=12:14:18.166，`match_ready`(才有 ds_addr)=12:14:36.814。他重登时 match 还在 warming，
`resolveBattleAuthority` 因此按「玩家应在 Hub 等 READY 推送」发大厅票 —— 这个判断本身没错，
错在他从没进 Hub，推送永远送不到。

### 2.3 已排除的噪声

| 同时出现的报错 | 为什么不是本事故根因 |
|---|---|
| `hub_presence_refresh_failed … locator code=2`（每 5s 一条，`cmdstat_eval failed_calls=1109/1152`） | **是一个真实的独立缺陷**（`touchHubAliveScript` 漏传 `ARGV[3]`，见 §5.7），但它只让 `last_alive_ms` 停更，位置 TTL 的 `EXPIRE` 在同一 pipeline 内照常成功（Redis pipeline 内各命令独立执行）。本事故的判定链没用到 `last_alive_ms`。它是**修复的前置依赖**，不是根因。 |
| `AllocateBattle latency_ms=18446` | 分配慢只是把「判定通过 → DS 就绪」的真空拉长到 18.4s，是**放大因素**不是根因：即使分配 0 秒，一个还没登录回来的人也一样进不来。 |
| `offlinewatch_swept … acted=15` | 离线摘人链路本身是活的，但**它在本事故里根本没有该动的对象** —— 缺席者 12:14:19 已经 `login_ok`，是个正常在线玩家。 |
| 客户端停在选角界面 | 是后果不是原因：他重登时 match 处于 ALLOCATING，服务端按设计只下发 `RESUME_MATCH_STAGE_ALLOCATING`，客户端未消费（§10 A-4）。 |

### 2.4 ~~已排除的替代假设：「他是进场时掉的」~~ → **本节的推理已被推翻（v2）**

> 以下三条是 v1 用来排除「进场时掉线」的论证。**结论方向是对的（他确实不是进场时掉的），
> 但第 1 条的推理是错的，并且正是它导致 v1 把整个根因判错。** 按事故纪律原文保留。

⚠️ **单看 `玩家离开大厅，上报 player_locator 断线` 这一行不足以证明他掉线了** —— locator proto 明写着「玩家 travel 去战斗同样会触发 Hub Logout」，两者产生完全相同的日志。真正的判据是下面三条：

1. ~~**时间上不可能**：他 12:11:37 离开大厅，而 `StartMatch` 是 12:14:17、`match_found` 是 12:14:18。**那一刻这局还不存在**，没有任何东西可以「进」。~~
2. **他做了一次完整重登**：12:14:19 `login_dev_skip_password account=DogEgg016`。travel 进战斗不产生重新登录，只有客户端进程结束 / 会话断掉才会走这条。
3. ~~**他没回来过**：12:10:41 `InitNewPlayer accepted` 进大厅 → 12:11:37 离开（只待了 56 秒），此后 Hub DS 日志里再没有他的 `InitNewPlayer`。~~

**推翻证据（2026-08-13 当日）**：第 1 条只检查了**新的那一局**是否存在，漏掉了**上一局**
（`23458733413662720`，12:11:17 开局）。12:11:37 那次「离开大厅」正是 travel 进那一局 ——
`Pandora_3-backup-2026.08.13-12.13.33.log` 里同一秒就有他的 `InitNewPlayer accepted` +
`Join succeeded`。第 3 条同理：他没回大厅是因为**人在战斗里**，不是因为掉线。
第 2 条仍然成立，但它证明的是「他 12:12:55 退出战斗后重登」，不是「他 12:11:37 掉线」。

**方法论教训（已固化到 §12.2 与 A-10）**：`玩家离开大厅` 这一行**必须**去 battle DS 日志反查
同一时刻有没有他的 `InitNewPlayer`；而且**换局会把 DS 日志 backup 成
`Pandora_N-backup-<时间>.log`，查历史局必须一起搜** —— v1 只搜了当前文件，所以看不到上一局。

## 3. 时间线

以 UTC 为主。

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 12:10:41 | Hub DS | `23458570204905472` 进大厅、申请入队、点准备，队伍 `TEAM_STATE_READY` | `InitNewPlayer accepted` / `team_set_ready` |
| 12:11:17 | team | 队长开**上一局**，`BeginTeamMatch` 冻结名单 members=3 | `team_match_roster_locked` |
| 12:11:37 | Hub DS → Battle DS 7801 | 他 travel 进上一局（**这行 Hub「离开大厅」不是掉线**），`Join succeeded` Camp=17 | Pandora_3-backup…log |
| 12:12:48 | Battle DS 7801 | 他阵亡（`dead=2`） | `玩家死亡终局判定` |
| **12:12:55** | Battle DS 7801 | **他自己退出了战斗**，比该局结算还早 13 秒 | `UNetConnection::Close: 192.168.2.251:49809` |
| 12:13:08 | Battle DS 7801 | 上一局 `finish=1` 正常结算并上报 battle_result | `上报战斗正常结算：match_id=23458733413662720` |
| 12:13:22 ~ 33 | Battle DS 7801 | 其余 5 人才陆续 `Close`，回大厅 | Pandora_3-backup…log |
| — | **team** | **本该在这里复位 ready —— 但这条路径根本不存在** | team.err.log 全程无记录 |
| **12:14:10** | team | 队长立刻开**下一局**（距他退出仅 **75 秒**），名单仍是 3 人 `READY`，他还在里面 | `team_match_roster_locked` |
| 12:14:18.166 | matchmaker | `match_found players=6` | matchmaker.err.log |
| 12:14:19.038 | login | 他这才 `login_ok` —— **比名单冻结晚了 9 秒**；此后无 `select_role_ok` | login.err.log |
| 12:14:28.094 | Battle DS 7802 | 装载权威准入元数据 `roster_count=6` | Pandora_4.log |
| 12:14:36.814 | matchmaker | `match_ready ds_addr=…:7802 players=6` | matchmaker.err.log |
| 12:14:37 ~ 38 | Battle DS 7802 | 只有 5 人 `Join succeeded`；他一次连接都没发起 | Pandora_4.log |
| 12:16:14 | Battle DS 7802 | 终局判定仍按 `roster=6` | `玩家死亡终局判定：roster=6` |

## 4. 调用链与关键变量

上一局的结束链 —— **本该在这里复位队伍，但整条分支不存在**：

```text
Battle DS ended → battle_result 结算落库 → outbox
  → matchmaker.ReleaseMatch(match_id, player_ids)
      → 删票据 / 删 player→ticket claim / 删 match 镜像
      → ✗ 没有任何一步通知 team                       ← 第一根因就在这个缺口
   (team 侧全量 grep:只有 LeaveTeam / Kick / removeOfflineMember
    会把 State 打回 FORMING,没有任何 match-ended 入口)
```

下一局的准入链 —— 七道门全过：

```text
MatchService.StartMatch
  → validateMapID          （关卡表）
  → tryStartCooldown       （防刷）
  → resolveEntryMode       （进法）
  → resolveMembers → TeamService.BeginTeamMatch   ← 队伍还是 READY,名单在这里被冻结,他还在里面
  → min_team_size          （人数下限:3 ≥ 3,过）
  → checkNoShowPenalty     （他没被罚过,过）
  → ensureNoneInBattle     （上一局已释放,他不在 BATTLE,过）  ← 唯一一道 presence 门,只看 BATTLE
  → preflightStartClaims   （上一局 claim 已释放,过）
  → CreateStartOperation   ← 票据落库,6 人 roster 就此不可逆
      → match_found → AllocateBattle(roster=6) → Battle DS
                                                   ← 只有 5 个人来
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| `team.State` | `CreateTeam` / `SetReady` | 队伍存续期 | 是 | **打完一局仍是 READY** —— 没有任何写者会因为「对局结束」把它打回 FORMING |
| `team.Members[i].Ready` | `SetReady` | 队伍存续期 | 是 | 同上，一局打完原样保留，队长因此点得动开始匹配 |
| `MatchStartOperationStorageRecord.Members` | `startMatchAdmitted` | 票据存续期 | 否（冻结后只读） | 6 人 roster 的唯一来源，落库后不可逆 |
| `team.MatchLockUntilMs` | `BeginTeamMatch` | 秒级租约，自净 | 是 | 只覆盖「组票 → ClaimPlayer 落地」，**刻意不是**「队伍在对局中」的状态位（proto 注释有明文），所以它也回答不了「这局打完了没有」 |
| locator 位置 key | Hub DS / matchmaker `SetLocation` | TTL 30s | 是 | 12:14:10 那一刻他既不是 HUB 也不是 BATTLE，**但没有任何人查它** |
| `offline_leave.threshold` | team 配置 | 静态 180s | 否 | **被两个不同的判断共用**：既决定「什么时候摘人」，又被隐含当成「什么时候不能开局」 |

## 5. 根因

> **v1 曾把下面的 ① 与 ② 顺序写反**（把 ② 当成直接根因，且用了一个错误的时间线）。v2 按证据重排。

### 5.1 直接根因①：team 打完一局不复位 ready / state（**第一位**）

**team 侧不存在任何 match-ended 路径。** 全量 grep 确认：只有 `LeaveTeam` / `Kick` /
`removeOfflineMember` 会把 `team.State` 打回 `stateForming`，没有 `OnMatchEnded` /
`OnMatchReleased` / `ResetReady` 之类的入口。

于是一局结束后队伍仍是 `TEAM_STATE_READY`、全员 ready 标记原样保留 →
**队长可以在队友还卡在结算界面 / 回大厅路上的时候立刻再开一局**。

证据是**否定性**的、也因此最容易被忽略：`team.err.log` 里两次 `team_match_roster_locked`
之间**一条日志都没有** —— 中间打完了一整局，team 服务对此毫不知情（§2.2 ②）。

最小必要条件（两者即可，不需要任何异常）：

1. 有人在上一局结束前后离开（阵亡先退、结算慢、回大厅慢，都算）；
2. 队长在他重新进大厅之前再点开始匹配 —— 本次窗口是 **75 秒**。

**这一条单独修好就能堵住本次事故。** 它也是唯一能覆盖「玩家全程无异常」这一形态的修复：
缺席者没有掉线、没有超时、没有任何错误，所以任何基于 presence / 离线阈值的判定都拦不住他
（他 12:14:19 已经 `login_ok`，只是还没走完选角进大厅）。

### 5.2 直接根因②：`StartMatch` 全链没有「成员在场」闸

`startMatchAdmitted` 的门禁清单是：关卡表 → 进法 → 冻结名单 → 人数下限 → no-show 退避 →
`ensureNoneInBattle` → claim 预检。其中唯一读 presence 的 `ensureNoneInBattle` **只拦 BATTLE 状态**
（它要解决的是「战斗中还点匹配」这个完全不同的问题）；`BeginTeamMatch` 的 fence 也只校验
`state==READY` / 是队长 / 未被别的 operation 锁。**没有一处问过「这个人现在在不在大厅」。**

12:14:10 那一刻他既不在 battle（上一局 12:13:08 已结算释放）也不在 hub —— 是 UNSPECIFIED/OFFLINE，
**没有任何一道门拦这个状态**。

这一条是纵深防御：即便将来又出现别的路径把不在场的人送进名单（比如 GM 拉人、新入口），它也能拦住。

### 5.3 直接根因③：没有缺员兜底

`empty_battle_timeout` / `no_show_battle_timeout` 两档都以 `player_count==0` 为触发条件。
本局 `player_count` 恒为 5，**空场计时器一次都没起过**，缺员局照常打完，
DS 终局判定还按 `roster=6` 算（`玩家死亡终局判定：roster=6 dead=1`）。

### 5.4 触发条件

任一队员在一局结束前后离开，队长在他重新进大厅之前再点开始匹配。窗口 ≈ **30~90 秒**
（结算 + travel 回大厅 + 重登 + 选角的耗时之和）。**这是连打第二局的常态窗口，不是异常路径。**

### 5.5 故障放大因素

1. **判定通过 → DS 就绪之间的真空**：本次 `AllocateBattle` 耗时 18.4s（editor 形态 DS 冷启），
   即便在场闸存在，这段时间内离开的人仍会缺席 —— 这正是 §5.3 那条兜底存在的理由。
2. **缺席者自己也没有出口**：他重登时 match 处于 ALLOCATING，服务端按设计不改路由、
   只下发 `RESUME_MATCH_STAGE_ALLOCATING`；客户端未消费该 stage，于是他停在选角界面，
   既不知道自己在一场对局里，也没有任何按钮可点。
3. **`startMatchAdmitted` 的每一处失败都是裸 `return 0, err`，一行日志都不打** ——
   本次排查只能靠 envoy 访问日志的响应体字节数（成功 49B / 只回 code 43B，`msg_len = BYTES_SENT - 40`）
   反推是哪道门拒的。这不是根因，但它把定位时间拉长了一个数量级。
4. **`玩家离开大厅，上报 player_locator 断线` 这行日志有歧义** —— 退出客户端与正常 travel 去战斗
   产生**完全相同**的一行。v1 因此误判了整个根因（见文首更正块）。这是排障工具面的缺陷，
   已记入 §12.2 与 A-10。

### 5.6 为什么现有保护没有挡住

| 保护 | 为何无效 |
|---|---|
| **对局结束复位准备** | **不存在**。这是第一根因本身（§5.1）。 |
| `team` 离线自动退队（`OnPlayerOffline`） | 他根本没离线满阈值 —— 事实上他 12:14:19 已经登录回来了。任何离线阈值（不论多短）都拦不住一个正常玩家。 |
| `ensureNoneInBattle` | 只拦 `state==BATTLE`。他的状态是「查不到」，不是 BATTLE。 |
| `BeginTeamMatch` roster fence | 解决的是「摘人 vs 组票」的 TOCTOU，只校验队伍状态与租约，不看在场。 |
| `preflightStartClaims` | 拦的是「已在撮合链路里」的玩家，他不在。 |
| matchmaker 成局最终门（`FindOfflinePlayers`） | **配置上是关的**，且关得有理：INC-20260724-001 证明它按「locator 查不到」判死，对已成局成员是**结构性 100% 假阳性**。直接把它打开会让每一局都被判死。 |
| `no_show_battle_timeout` / `empty_battle_timeout` | 两档都只在 `player_count==0` 时起算，拦不住「来了但没来齐」(§5.3)。 |
| DS 侧终局判定 | 直接用 `roster_count=6` 作分母，缺员本身对它是不可见的。 |

### 5.7 前置缺陷:`RefreshHubLocations` 的 Lua 调用漏传 `ARGV[3]`

不是本事故根因，但**修复在线闸必须先修它**，且它自己也是一个真实缺陷：

`location.go` 里 `RefreshHubLocations` 内联调用 `touchHubAliveScript.Eval` 时**只传了两个 ARGV**，而脚本在 2026-08-12 加节流后用到了 `ARGV[3]`。后果隐蔽：第一次心跳时 meta 还没有 `last_alive_ms`，`prev` 为 nil 短路通过；**从第二次心跳起** `(now - prev) < tonumber(nil)` 直接 Lua 报错。因为 `EXPIRE` 与 `EVAL` 在同一 pipeline 内各自独立执行，位置 TTL 照常续上，**只有 `last_alive_ms` 静默停更** —— 线上唯一可见的症状就是每 5s 一条 `hub_presence_refresh_failed` 和 `cmdstat_eval failed_calls` 一路涨（实测 1109/1152 = 96%）。

## 6. 全仓同类问题扫描

- **扫描基线 commit**：`4584606e`
- **扫描范围**：`services/matchmaking/**`、`services/battle/{ds_allocator,hub_allocator}/**`、`services/runtime/player_locator/**`、`pkg/offlinewatch/**`
- **搜索模式/工具**：`grep -rn "OnMatchEnded\|OnMatchReleased\|ResetReady\|stateForming"`（找 team 侧的 match-ended 路径，**零命中** = 第一根因的机械证明）；`grep -rn "IsInBattle\|FindOfflinePlayers\|BatchGetLocation\|BatchGetLastSeen"`；逐条核对 `startMatchAdmitted` 门禁链；逐条核对 `RefreshHubLocations` 与 `TouchAlive` 的调用形状
- **Confirmed 同型命中**：
  - `team` 缺 match-ended 复位路径（第一根因，已修）。
  - `matchmaker.startMatchAdmitted` 缺在场闸（已修）。
  - `player_locator.RefreshHubLocations` 漏传 `ARGV[3]`（§5.7，已修）。
  - `ds_allocator` 两档空场阈值均无法覆盖「部分缺员」（已修）。
- **结构性隐患（本次识别，比单点 bug 更值得记）**：
  1. **`Begin` 有、`End` 没有**。`BeginTeamMatch` 是 2026-08-06 为消除 TOCTOU 加的，做得很仔细（锁内冻结 + 自净租约 + 明文论证为什么不写 `TeamState=MATCHING`），但**只加了开始那一半**。凡是「A 服务在 B 服务上开了一个状态」的地方，都要问一句「谁负责关」——本次的答案是「没有人」，而且因为租约会自净、State 又刻意不写，**这个缺口在代码里完全看不出来**。
  2. **一个阈值被两个判断共用**。`offline_leave.threshold` 同时承担「什么时候把人摘出队伍」和（隐含地）「什么时候不能带他开局」，而这两件事的安全下界完全不同 —— 前者要给重连留足余量（宁可长），后者要贴近真实在场（宜短）。同一个数字不可能同时满足。
  3. **presence 的「此刻在不在场」被当成了「在不在线」**。位置投影在正常路径上就会短暂缺席（撮合失败后 MATCHING 无保活、切线换 Hub 的换手窗口），任何按缺席判死的门都必然假阳性 —— INC-20260724-001 已经付过一次学费。正确的判据是**「距最后一次被观测在场过去多久」**，而这需要一个与投影状态无关的连续信号（见 §7.2 第 2 项）。
  4. **一行日志承担两种互斥语义**。`玩家离开大厅，上报 player_locator 断线` 对「退出客户端」和「travel 去战斗」输出完全相同的文本，排障时无法区分 —— v1 的整个根因判断就栽在这里（§2.4）。凡是「离开」类事件，日志必须带上**去向**。
- **已排除项及理由**：`hub_allocator.InBattleOrMatching`（切线护栏）虽然也只放行 HUB，但它是 fail-closed 且作用于**单个玩家的主动操作**，拒绝的代价是「切线失败可重试」，与本事故的「整队开不了局」不同量级，且 INC-20260722-002 已按此定案，本次不改。
- **未覆盖边界**：`login` 侧的 resume 语义、UE 客户端对 `RESUME_MATCH_STAGE_ALLOCATING` 的消费（属客户端仓库，列 A-4）。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 口头规避：开始匹配前确认队里每个人都在线；中途退过客户端的人先踢出重新拉进来 | 已告知 | — | 依赖人工，不可靠 |

### 7.2 永久修复

七项同批落码，按根因优先级排列。**修复不是「把成局最终门打开」** —— 那道门被关掉是有充分理由的（§5.6）；本次先补上真正缺失的那条 match-ended 路径，再把 presence 信号修可信、在正确位置补纵深防御。

| # | 项目 | 状态 | 代码/配置 | 关键设计决定 |
|---|---|---|---|---|
| **0** | **对局结束复位队伍准备状态（第一根因）** | **已落码但复核阻断，禁止部署** | proto 新增 `TeamService.EndTeamMatch`；`team/internal/biz/offline_leave.go` 的 `EndTeamMatch` + `service/team.go` handler；`matchmaker` 的 `TeamReader.EndTeamMatch` + `ReleaseMatch` 里按 `team_id` 分组调用 | 直接根因判断仍成立，但当前请求只有 `team_id + player_ids`，没有 match/member/ready generation。ACK 丢失或暂态失败后，旧局 outbox 可在玩家 re-ready、离队重入或新一局租约到期后清掉新意图（ABA）；targets 已全离队时仍可能把全新 READY roster 打回 FORMING；claim 先释放到 End 成功之间也仍能用旧 ready 开下一局。传输层 at-least-once 不等于跨代幂等。详见 [`decision-revisit-team-match-lifecycle-and-roster-rollout.md`](../design/decision-revisit-team-match-lifecycle-and-roster-rollout.md) |
| 1 | locator `touchHubAliveScript` 补齐 `ARGV[3]` | 已落码 | `player_locator/internal/data/location.go` | 见 §5.7 |
| 2 | **心跳按 census 全员刷新 `last_alive_ms`** | 已落码 | 同上 | **本次修复的地基**。原先只对 `state==HUB` 的记录刷新，导致撮合失败后停在 MATCHING 的玩家（人坐在大厅里）`last_alive_ms` 永久停更、位置 key 30s 后消失 —— 那正是 INC-20260724-001 假阳性的根子。改为**只要出现在 Hub DS 的 census 里就刷**：census 里有他 = 他此刻连在这台 Hub 上，这个事实与位置投影处于哪一态无关。位置 TTL 的 `EXPIRE` 仍严守「非 HUB 态一律不动」（不变量 §1），两件事作用域刻意不同 |
| 3 | matchmaker `ensureAllPresent` 在线闸 | 已落码 | `matchmaker/internal/biz/match.go`、`data/locator_client.go` 复用 `pkg/offlinewatch.GrpcPresenceReader`；新增 `errcode.ErrMatchMemberOffline=4011`、配置 `start_presence_grace`（默认 30s，负值关闭） | 判据是「**离开了多久**」不是「此刻查不查得到」：不在场且 `now - lastSeen >= grace` 才拒；拿不到任何离开基线判 UNKNOWN **放行**（§9.22）。位置夹在 `ensureNoneInBattle` 之后（用冻结后的名单）、`preflightStartClaims` 之前（先拒无副作用的读闸，再去占坑）。locator 查不通默认 fail-closed，仅 dev `battle_gate_fail_open=true` 降级 |
| 4 | team 掉线立即清 ready（不摘人） | 已落码 | `pkg/offlinewatch` 新增可选 `PresenceLostHandler`；`team/internal/biz/offline_leave.go` 新增 `OnPlayerPresenceLost` | 把 §6 结构性隐患①拆开：**留人**（为重连，180s 不变）与**可开局**（要人在场）不再共用一个阈值。队长因此根本点不动开始匹配，不必先撞一个错误码。**回调挂在 `Observe` 而不是 `Sweep` 的 waiting 分支** —— `upsertEvidence` 把 due 排在「离开时刻+Threshold」，新鲜离场在满阈值前根本不会被 Sweep 扫到，挂在那里是不可达代码（已用 `TestSweep_新鲜离场在满阈值前根本扫不到` 钉死） |
| 5 | ds_allocator 花名册到齐期限 | 已落码（**滚动激活仍阻断**；武装窗仅为纵深防御） | `ds_allocator/internal/biz/allocator.go`（legacy）+ `internal/data/battle_auth.go`（Model B）；proto 加 `roster_incomplete_since_ms`/`roster_ever_complete`；配置 `roster_join_deadline`（默认 **45s**，下限 30s，负值关闭） | 静态判据本身如左。**滚动激活缺口**：2 副本 RollingUpdate 下旧副本不写「曾经到齐」，若全员到齐只被旧副本看到、切新副本时恰好已有局中掉线，新逻辑会把存量局当从未到齐，45s 后**误判弃正在打的局**。<br>现有第二道「武装窗」=`ready_wait_timeout + deadline + 30s`，只能保护已经超过窗口的老局；它保护不了**窗口内**旧副本已见过到齐、随后新副本接手时正好缺人的局。现有回归把 `allocated_at` 回拨一小时，只证明“过窗不武装”，不能外推为滚动语义安全。`allocated_at` 缺失（旧记录 / mock）不武装仍是有效的 fail-safe 纵深防御。<br>**最终发布必须**先 observe-only 全量采证，再只给全量新副本后创建的 `roster_policy_generation` 开 enforce；这不是可选的保守姿态。45s 仍是取舍，须实测「DS ready → 最后一个 `Join succeeded`」P99（A-7） |
| 6 | `startMatchAdmitted` 全部拒绝分支补日志 | 已落码 | `match.go` `reject()` 收口 | 修的是 §5.5 第 3 条：此前拒绝在服务端完全不可见 |

**proto 改动（`[proto]`，须同步 UE 仓库，见 CLAUDE.md §5）**：字段层面是纯加法，
但当前 End 缺代际 fence、allocator 缺激活 generation，**不能据此声称语义层滚动升级双向兼容**。

| proto | 改动 | 影响 |
|---|---|---|
| `pandora/team/v1/team.proto` | 新增 `rpc EndTeamMatch` + 请求/响应 message | 内部东西向（matchmaker→team）。service 层加了 `systemOnly` 守卫 —— 与 `BeginTeamMatch` 不同：Begin 拿队长身份当授权（越权收益近乎为零），而本方法能把**任意**队伍打回 FORMING，Envoy 整前缀放行下不显式拒就是一个「让任何队伍开不了局」的骚扰口子 |
| `pandora/common/v1/errcode.proto` | `ERR_MATCH_MEMBER_OFFLINE = 4011` | **客户端必须接**：当前 UE 会把 4011 当不确定结果进入权威恢复，Lobby UI 又忽略 Status，并不是可靠显示“裸数字”。`StartMatchResponse` 只有 code + match_id，biz error 里的 player_id 没有过线；不扩响应时最多显示固定“有队员不在大厅” |
| `pandora/common/v1/errcode.proto` | `ERR_NOT_IMPLEMENTED = 15`（公共段） | 「对端**这个版本**还没有这个能力」，gRPC `Unimplemented` 的业务侧对应物。与 `ERR_UNAVAILABLE` 的区别是**能不能靠重试解决** —— 这个区别决定调用方是退避重试还是弱依赖降级，是 §9.21 共存窗口的通用工具，本次只是第一个用它的地方 |
| `pandora/ds/v1/allocator.proto` | `BattleStorageRecord` 加 `roster_incomplete_since_ms=24` / `roster_ever_complete=25` | 服务端内部存储快照，客户端不可见；旧副本不写这两个字段，滚动升级窗口的退化行为已在字段注释里写明 |

`buf generate` 产物已生成但**未提交**（`proto/gen/go/**` 与 `proto/gen/cpp/**`）。官方 UE
生成器要求 server proto 已提交且该 scope clean；最终协议尚会变化，因此 A-9 暂停。实际客户端
目录是 `Source/PandoraProto/Public/Generated/Proto` 与 `Source/ThirdParty/PandoraProtoGenerated`，
不是旧文所写 `Source/Pandora/Generated/Proto`。

**刻意没做的事**：

- **没有缩短 `offline_leave.threshold`**。它与本事故无关（缺席者根本没离线满阈值，他 12:14:19 就登录回来了）；缩它只会把正常重连的玩家踢出队伍。
- **没有把 `TeamState` 写成 `MATCHING`**。team.proto 已有明文论证：写 State 需要有人负责改回来，matchmaker 中途崩溃就会把队伍永久卡在 MATCHING（违反不变量 §20）。`EndTeamMatch` 走的是「复位到 FORMING」而不是「进入/退出 MATCHING」，没有引入需要补偿的中间态。
- **没有打开 `liveness_gate_enabled`**。那两道门的判据（locator 查不到即判死）在结构上就是错的，INC-20260724-001 已定案；本次新增的闸判的是「离开多久」，是另一回事。
- **没有新增 `TEAM_UPDATE_REASON_MATCH_SETTLED` / `MEMBER_OFFLINE` 枚举**。两条复位路径的推送都复用既有 `MEMBER_READY` —— 它表达的事实就是「成员准备状态变了」，客户端刷新逻辑一字不用改。将来产品要区分文案时再按 §9.21 加法演进（列 A-5）。

### 7.3 防复发规则

本档案即规则载体，四条已在代码注释处固化：

- **跨服务开了状态，就必须有人负责关**。`BeginTeamMatch` / `EndTeamMatch` 现在成对；新增任何
  「A 服务在 B 服务上开一个状态」的接口时，同一个 PR 里必须回答「谁在什么路径上关它」。
  租约会自净、State 刻意不写 —— 这些都不能替代显式的结束路径（本次缺口就是这么藏了三个月）。
- **「还在队伍里」≠「有资格被拉进对局」**。任何按「玩家还在某个集合里」推导出「可以对他做某事」的地方，都要单独问一遍「他现在在不在场」。
- **presence 判定必须按「离开了多久」，不能按「此刻查不查得到」**。位置投影在正常路径上就会短暂缺席；按缺席判死是结构性假阳性（INC-20260724-001 + 本档 §6）。前提是那个「多久」有连续可信的来源 —— 这就是 §7.2 第 2 项存在的理由。
- **排障纪律：`玩家离开大厅` 必须去 battle DS 反查**（同一时刻有没有他的 `InitNewPlayer`），
  且**换局会 backup 日志文件，查历史局必须一起搜**。v1 漏了后半句就把根因判错了（§2.4、§12.2）。

尚未落为机械门禁（A-6）。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| **team 对局结束复位（9 条）** | 不存在 | PASSED | `go test ./services/matchmaking/team/internal/biz/ -run EndTeamMatch` | 含「复位后组票必须被拒」这条最终判据 |
| **ReleaseMatch 按队复位 + 共存窗口降级（5 条）** | 不存在 | PASSED | `go test ./services/matchmaking/matchmaker/internal/biz/ -run 'ReleaseMatch_\|CollectTeamRosters'` | 含「复位失败保留 canonical match 供 outbox 重投」 |
| **接线（过线）验证：RPC 已注册 + 客户端发对且能区分「对端未实现」（7 条）** | 不存在 | PASSED | `go test ./services/matchmaking/team/internal/service/ -run EndTeamMatch`；`go test ./services/matchmaking/matchmaker/internal/data/ -run EndTeamMatch` | 见下方「为什么单测不够」 |
| locator 第二跳不 Lua 报错 | **FAILED**（`attempt to compare number with nil`） | PASSED | 真 Redis 8：`PANDORA_TEST_REDIS_ADDR=… go test ./services/runtime/player_locator/internal/data/ -run RealRedis` | 变异测试见下 |
| census 全员刷新 `last_alive_ms`（非 HUB 态） | **FAILED** | PASSED | 同上 | 同上 |
| 位置 TTL 不因 census 被误续（不变量 §1） | PASSED | PASSED | 同上 | 反向断言在同一用例内 |
| matchmaker 在线闸 11 条用例 | 不存在 | PASSED | `go test ./services/matchmaking/matchmaker/internal/biz/ -run EnsureAllPresent` | — |
| StartMatch 被拒时**不产生 durable operation** | **FAILED**（票据照建） | PASSED | 同上 `-run StartMatch_成员离线` | — |
| 投影缺席但心跳仍在 → 不得误伤（防 INC-20260724-001 倒退） | — | PASSED | 同上 | 变异测试见下 |
| team 软化档 8 条用例 | 不存在 | PASSED | `go test ./services/matchmaking/team/internal/biz/ -run PresenceLost` | — |
| offlinewatch 软化档 7 条用例 | 不存在 | PASSED | `go test ./pkg/offlinewatch/` | — |
| ds_allocator 到齐期限 7 条用例 | 不存在 | PASSED | `go test ./services/battle/ds_allocator/internal/biz/ -run RosterJoinDeadline` | — |
| 全量回归（matchmaker/team/ds_allocator/hub_allocator/player_locator/offlinewatch/errcode） | — | **按 module 边界全绿** | 根 module 下原写的 `go test ./services/... ./pkg/...` 会报 pattern 无效；改为在 5 个服务 module 各跑 `go test -count=1 ./...`，根 module 跑 `go test -count=1 ./pkg/offlinewatch/... ./pkg/errcode/...` | 2026-08-13 Codex 当前工作树实跑：7 个目标全部 exit=0。命令口径已修正，不能继续引用无效的根目录 glob |
| `go test -race` | — | **目标分包均有通过证据；五族合并首跑不是全绿** | Linux 容器 `golang:1.26.5` | 五族合并首跑 exit=1：offlinewatch/team/ds_allocator/player_locator `ok`，matchmaker 在编译导入 grpc 时瞬态失败；随后 matchmaker 独立重跑通过。12:56 最新工作树再次跑 `pwsh tools/scripts/go_test_race.ps1 -Pattern ./services/matchmaking/matchmaker/... -Timeout 30m`，exit=0（243.2s），无 `WARNING: DATA RACE`。因此可记录每个目标有分包通过证据，不能把合并首跑改写为 exit=0 |
| 真集群 / Model B 路径集成回归 | — | **未执行** | — | A-2 |
| fatal/OOM/SIGKILL 重启注入 | — | **未执行** | — | A-3 |
| 玩家 E2E（复现步骤 §12.1） | 必现 3v2 | **未执行** | — | A-2 |

### 为什么光有单测不够（本事故本身就是「编译得过但没接上」的形状）

第一根因的教训是：`BeginTeamMatch` 每一部分都对、每一个测试都绿，**缺的是两个部分之间那条线**。
所以本次特意补了三段过线验证，它们各自覆盖单测**结构上答不了**的问题：

| 段 | 单测答不了的问题 | 怎么测 |
|---|---|---|
| matchmaker biz → `TeamReader` | — | 已有（mock reader） |
| `GrpcTeamReader` → 线 | **客户端到底发了什么、怎么解释回来的 code**。把非 OK 压成 nil 的后果是静默的：`ReleaseMatch` 当作成功、删掉 canonical match，队伍却还停在 READY 且再无重投机会 | bufconn + 记录型 server，断言请求体字段 + 业务码原样透传 + `Unimplemented` 必须上抛（= 灰度期 team 未先上线的形状，对应 A-11） |
| 线 → `TeamService` | **这个 RPC 到底注册上没有**。proto 加了、handler 写了、都编译通过，但没被 `RegisterTeamServiceServer` 带上时线上拿的是 `Unimplemented`，而**所有单测照样绿** | bufconn 起真 server + **生成的 client** 打进去 |
| `TeamService` → biz | 守卫是否挡在业务之前 | nil usecase：门失效就 panic，而不是安静返回错误码 |
| biz → Redis | — | 已有（9 条，miniredis） |

### 变异测试（证明断言是活的，不是摆设）

| 变异 | 结果 |
|---|---|
| **matchmaker：把 `endTeamMatches` 从 `ReleaseMatch` 摘掉**（= 修复前的现状） | **FAILED**，2 条命中 |
| **matchmaker：`GrpcTeamReader.EndTeamMatch` 把非 OK code 压成 nil** | **FAILED**，1 条命中（证明「静默吞掉复位失败」抓得住） |
| **matchmaker：不识别 `ErrNotImplemented`（漏降级）** | **FAILED**，1 条命中 —— 共存窗口会积压 |
| **matchmaker：所有错误都降级（过度降级）** | **FAILED**，1 条命中 —— 真故障会被静默跳过，队伍再也没人复位 |
| **ds_allocator：`rosterGateArmable` 恒真（去掉武装窗）** | **FAILED**，2 组命中（含「接手已过窗老对局不得判弃」）—— 证明武装窗能保护已过窗老局；**未覆盖窗内混版接管**，不能证明滚动安全 |
| **team：不把 READY 打回 FORMING**（只清 ready 标记） | **FAILED**，7 条命中（含「复位后组票必须被拒」）—— 证明只清标记不改 State 堵不住,队长照样点得动 |
| locator：`Eval` 去掉 `AliveTouchThrottle.Milliseconds()` | **FAILED**，2 条命中，报 `attempt to compare number with nil` |
| locator：把 `last_alive_ms` 刷新退回「仅 state==HUB」 | **FAILED**，精确命中 census 那 1 条，其余全绿 |
| matchmaker：把宽限窗判断改成恒 false（= 按缺席判死，即原始方案） | **FAILED**，2 条命中（「刚离开在宽限窗内放行」「投影缺席但心跳仍在不得误伤」）—— 直接证明按缺席判死会重演 INC-20260724-001 |
| matchmaker：把 `ensureAllPresent` 从准入链摘掉 | **FAILED**，端到端用例命中 |
| ds_allocator：去掉 `roster_ever_complete` 豁免 | **FAILED**，精确命中「到齐后局中掉线不得判弃」—— 证明少了这层会判弃正在打的对局 |

## 9. 部署、回滚与观察

- **修复 commit**：**未提交**（工作副本；仓库内另有他人未完成的 `configtable/dist/*` 与 proto 改动，本次未触碰也未一并提交）
- **构建产物/镜像 digest**：无（未构建）
- **部署时间与目标环境**：**未部署**
- **回滚条件和步骤**：后三道闸各有独立配置开关，可秒级回滚而不必回退代码 ——
  `matchmaker.start_presence_grace: -1`（关在线闸）、`allocator.roster_join_deadline: -1`（关到齐期限）、
  `team.offline_leave.enabled: false`（关掉线软化档，连带关掉原有的自动退队）。
  ⚠️ **第一根因那条（`EndTeamMatch`）刻意没有开关** —— 它不是「多一道闸」，是补回一条本就该存在的
  状态机边；给它加开关等于给「打完一局要不要复位」留一个能配错的旋钮。要回滚只能回退代码。
- **发布阻断：当前不能声称任意顺序安全，也不能只靠 team 先发收口**。
  `ErrNotImplemented` Warn + 跳过会让该局永久保留本 P0 的第一根因，不是安全弱依赖；
  fail-closed 又会在 claim 已释放、team 仍 READY 的窗口允许误开下一局。即使 team 已全量，
  旧新 matchmaker 共存时旧 Pod 仍会 ACK ReleaseMatch 并删 outbox。当前须按
  [`decision-revisit-team-match-lifecycle-and-roster-rollout.md`](../design/decision-revisit-team-match-lifecycle-and-roster-rollout.md)
  先补代际协议与 allocator 激活 generation，再落机械发布门和反向回滚顺序。
- **观察窗口、指标与结果**：**为零**（未部署）。上线后应观察：
  `team_match_ended_unready` 的频次应与结算局数同量级（明显偏低 = 复位链没接上）；
  `match_release_end_team_failed` 应为 0；
  `match_start_rejected gate=member_offline`（异常高说明 grace 配小了或 census 链路有问题）；
  `battle_abandoned_empty_timeout reason=roster_incomplete`（应极低，偏高说明 45s 配短了）；
  `hub_presence_refresh_failed` 应归零

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 |
|---|---|---|---|---|---|
| A-1 | — | 跑 `go test -race` | — | **目标分包证据完成；合并首跑 exit=1**（最新 matchmaker 独立 race 亦通过；见 §8） | 本档 |
| A-2 | 高 | 部署 + 按 §12.1 复现步骤做一次玩家 E2E；**Model B 路径的到齐期限从未在真集群跑过**（本次只有单测） | 待指定 | 未开始 | 本档 |
| A-3 | 中 | 故障注入：locator 不可用时 `StartMatch` 的 fail-closed 行为、Hub DS 整台崩溃时 census 停报的影响 | 待指定 | 未开始 | 本档 |
| A-4 | 中 | **客户端（UE 仓库）**：消费 `RESUME_MATCH_STAGE_ALLOCATING`，恢复「匹配中」UI + 轮询 `GetMatchProgress`。本次事故里缺席者拿到了这个 stage 却停在选角界面 | 待指定 | 未开始 | 客户端仓库 |
| A-5 | 低 | 若产品要区分「他掉线了」与「他手动取消准备」的文案，新增 `TEAM_UPDATE_REASON_MEMBER_OFFLINE` 并双仓同步 | 待指定 | 未开始 | 本档 §7.2 |
| A-9 | 中 | **客户端（UE 仓库）**：最终 proto 定稿并有真实 server commit 后，用官方生成器同步 cpp pb；4011 按确定性拒绝处理并显示固定提示。若产品要点名，先给 StartMatchResponse 加结构化缺席成员 ID（当前 biz error 文本不过线） | 待指定 | **阻断：服务端协议待改，真实 UE 工作树未同步** | 客户端仓库 |
| A-10 | 中 | **给「离开大厅」类日志加去向字段**（travel_to_battle / disconnect），消灭 §6 结构性隐患④。v1 的根因误判直接源于这一行的歧义 | 待指定 | 未开始 | 本档 §2.4 |
| A-11 | 高 | 补齐 team ready 代际幂等、旧新 MM 共存策略、live capability/digest/旧 RS 门与反向回滚；allocator 采用 observe→generation enforce | 待指定 | **阻断，禁止部署**；`Unimplemented` 当成功会永久漏复位，team 先发也挡不住旧 MM ACK | [`decision-revisit-team-match-lifecycle-and-roster-rollout.md`](../design/decision-revisit-team-match-lifecycle-and-roster-rollout.md) |
| A-12 | 低 | 复位语义是「打完一局必须重新点准备」，属**产品可见行为变更**。需与策划确认（尤其连打场景的手感），必要时改成「只复位缺席者」而非全队 | 待指定 | 未开始 | 本档 §7.2 #0 |
| A-6 | 中 | 把 §7.3 两条规则落为机械门禁（如 CI 扫「读 team 成员后未查 presence 就写票据」的形状） | 待指定 | 未开始 | 本档 |
| A-7 | 中 | `start_presence_grace=30s` 与 `roster_join_deadline=45s` 均未实测；到齐期限须量“DS ready → 最后一个 Join succeeded”的 P99。45s 是体验取舍，不是安全上界 | 待指定 | 未开始 | 本档 |
| A-8 | 低 | 撮合失败后玩家永久停在 MATCHING 投影这件事**本身**仍未修（§7.2 第 2 项只是让它不再影响判定）。彻底修法是给 MATCHING 一条释放通道，那也是重开 `liveness_gate_enabled` 的前置 | 待指定 | 未开始 | INC-20260724-001 D2 |

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据
- [x] 修复前失败、修复后通过的回归存在（§8，且经变异测试验证断言是活的）
- [x] **接线闭环**：跨服务两半各自过线验证（RPC 已注册 / 客户端发对且不吞码 / 守卫挡在业务前），
      不再有「各半都绿、合起来不通」的空档 —— 这正是本事故的形状
- [ ] race/集成/故障注入达到本事故风险要求（`-race` 已过；**真栈集成 A-2 / 故障注入 A-3 未完成**）
- [x] 同类代码扫描完成（§6，含四条结构性隐患）
- [ ] 目标环境已加载可追溯的新产物（**未提交未部署** —— 本仓库约定 Claude 不执行 git 提交）
- [ ] 玩家路径、恢复和补偿路径验证通过（A-2；缺席者侧的恢复路径还缺客户端 A-4）
- [ ] 观察窗口无复发（窗口为零）
- [ ] 剩余风险已解决或另建 Incident/任务（**A-11 为发布阻断**；A-2~A-10、A-12 未闭环）
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭。

**代码侧仍未闭环**（本节随第三、四轮更新，勿按旧结论理解）。

原列的五条代码缺口现状：

| 缺口 | 状态 |
|---|---|
| End 缺代际幂等 | **已修**（§12.5 ①：ready 代际 CAS + 自动推进包装器 + 契约门禁） |
| 旧新 MM 共存会漏局 | **已修**（§12.5 ②：待复位标记 + 权威回查兜底；触发点边界已如实记录） |
| `systemOnly` 不等于 matchmaker 身份 | **已修**（§12.5 A-13 三档可降级验签；§12.6 补完 5 份 yaml + 6 条三档用例 + 两方向变异） |
| `team_addr` 可静默跳过 | **已修**（§12.4：fail-closed） |
| allocator 缺滚动激活 generation | **已修**（§12.4：武装窗，判据是 battle 年龄，对新旧副本同一答案） |

**仍不得提交发布候选或部署**，剩余缺口：
① 方案 A 重构在途，`team/internal/biz` 3 条红待其收尾（`MatchStartReceipt` 仅存在于 proto，见 §12.6）；② 未提交未部署；③ 玩家 E2E 与 Model B 真集群未跑；
④ 观察窗口为零；⑤ 客户端 A-4/A-9 未落真实 SVN 工作树；⑥ A-12 产品语义待策划确认；
⑦ allocator 是否再叠一层 observe-only 采证期属发布策略选择，未拍板。
以 decision-revisit 的验收矩阵为准。**完整的待修复清单见 §13**(按阻断力分三档:阻断发布 / 已知不阻断 / 刻意不做)。

---

## 12. 附录

### 12.1 复现步骤（v2，与真实事故同形）

1. 三人组队（`map_id=9`，3v3），全员点准备，**开一局**。
2. 其中一人**在结算前就退出战斗**（阵亡后直接退，或战斗中关客户端）。
3. 该局结算后，其余人回到大厅，**队长立刻再点开始匹配** —— 要赶在退出者重登进大厅之前，
   窗口约 **30~90 秒**（结算 + travel 回大厅 + 重登 + 选角的耗时之和）。
4. 观察：队伍 state 仍是 `READY`（第 2 步那人没被清 ready）→ `match_found players=N` 仍含他
   → Battle DS 日志 `roster_count=N` 但 `Join succeeded` 只有 N-1 条。

**修复后**：第 3 步队长根本点不动 —— 上一局 `ReleaseMatch` 时队伍已被 `EndTeamMatch` 打回
`FORMING`，`BeginTeamMatch` 直接返回 `ErrTeamWrongState`。全员重新点准备后才能再开。

> v1 的复现步骤（「关客户端 → 180s 内开局」）保留作参考：它触发的是 §5.2 那道纵深防御
> （返回 `4011 ERR_MATCH_MEMBER_OFFLINE`），**不是本事故的真实形态**。

### 12.2 排障工具备忘（供后人）

- Go 服务业务日志在 `Server/run/dev/logs/*.err.log`。**注意 `.log` 只停在启动那一刻，业务日志全在 `.err.log`**；行内无时间戳，靠顺序对齐。
- UE 侧：`Pandora.log` = Hub DS(7777)，`Pandora_2/_3/_4.log` = 各 Battle DS(7800/7801/7802)；认哪个是哪个看日志开头 `LogInit: Command Line:` 里的 `-port=`。
  ⚠️ **换局会 backup 成 `Pandora_N-backup-<时间>.log`，查历史局必须一起搜** —— v1 判断出错就是漏了这步。
- ⚠️ **`玩家离开大厅，上报 player_locator 断线` 有歧义**：退出客户端与正常 travel 去战斗产生**完全相同**的一行。
  必须去 battle DS 日志（含 backup）反查同一时刻有没有他的 `InitNewPlayer accepted`。有 = travel，没有 = 真离开。
- 判断「某人到底进没进这局 DS」：在该 DS 日志里搜 player_id，看 `InitNewPlayer accepted` + `Join succeeded`
  + `[SpawnPawn] … Camp=`；再数 `NotifyAcceptedConnection` 条数对不对得上 `roster_count`。
- 这些日志文件被进程持有写句柄，`Get-ChildItem` 看 `Length` 恒为 0、`Get-Content` 可能读不全；用 `[IO.File]::Open(path,'Open','Read','ReadWrite')` + `StreamReader` 直读。
- 账号→player_id 映射查 `login.err.log` 的 `login_dev_skip_password account=xxx player_id=…`。
- 本事故**服务端零错误日志**，最初只能靠 envoy 访问日志的响应体字节数反推准入结果
  （成功 49B / 只回 code 43B，`msg_len = BYTES_SENT - 40`）。§7.2 第 6 项落地后不再需要这么干。
- **否定性证据同样是证据**：本次第一根因的机械证明是「两次 `team_match_roster_locked` 之间 team 侧
  一条日志都没有」。查「谁没做什么」时，`grep` 的零命中要当成结论来对待，而不是「大概在别处」。

### 12.3 已推翻的第二版：把 `Unimplemented` 当成功（保留作事故纪律，勿照做）

第一版修复给发布引入了一条隐含约束：`team` 的 `EndTeamMatch` 必须先于 `matchmaker` 上线，
否则 matchmaker 拿 `Unimplemented` → `ReleaseMatch` 失败 → outbox 无限重投 → canonical match 积压。
当时的处置是把它写进 [`release-checklist.md`](../ops/release-checklist.md)。

当日第二版曾把初版处置判为错误，理由是：

1. **它违反 §9.21**。滚动升级必须双向兼容、不得依赖发布顺序 —— 引入顺序依赖的那一刻就已经破规了，
   写进清单只是给破规发了张通行证。
2. **人执行的顺序没有机械手段能拦住**，而搞错的后果是静默的（补偿链空转 + 状态积压，无告警）。
   §16.10 的判别口诀在这里同样适用：**靠「记得按顺序做」的都是掩盖，靠「做错也安全」的才是解决**。

第二版据此尝试把约束**消掉**：新增 `errcode.ErrNotImplemented`（gRPC `Unimplemented` 的业务侧对应物），
`GrpcTeamReader` 识别后转码，`endTeamMatches` 对它弱依赖降级（Warn + 跳过，不重投），
其它任何错误照常 fail-closed 交 outbox。

当时写下的“安全性论证”如下；它只比较了当下状态和积压，漏掉了本局是否还有补偿机会：

| | 降级放行 | fail-closed（初版） |
|---|---|---|
| 队伍状态 | 停在 READY | **同样**停在 READY（调用根本没成功） |
| 额外代价 | 无 | canonical match 无限积压 |
| 恢复 | team 升级后每一局自动正常 | 同左，但要先把积压消化掉 |

**这段结论已被推翻。** `continue` 后 `ReleaseMatch` 会 DeleteMatch，battle_result 收到 OK 后删
outbox；该局永远失去补偿机会。team 升级后“后续每一局正常”不能补回“本局永久停 READY”。
而“本修复落地之前的行为”本身就是本 P0 第一根因，不是可接受的安全降级。

此外，旧新 matchmaker 共存时，battle_result 的长期 ClusterIP gRPC 连接可钉在旧 Pod；旧 Pod
同样会 ACK 并删 outbox。因此既不能把发布顺序当最终正确性，也不能把 `Unimplemented` 当成功。
完整推翻证据和候选修法见
[`decision-revisit-team-match-lifecycle-and-roster-rollout.md`](../design/decision-revisit-team-match-lifecycle-and-roster-rollout.md)。

最终保留的通用规则是：**新增「A 服务调 B 服务新 RPC」不能只靠“先上线谁”兜底，也不能
默认把 `Unimplemented` 吞掉**；必须拆成 expand → migrate → contract，并证明旧新 caller /
callee 的每一种组合都不会丢业务终态。

第二版新增的“`ErrNotImplemented` 必须删镜像”测试锁死的是错误语义，不能作为安全证据；修法
拍板后须替换为“旧 caller/callee 共存不丢本局补偿”的接口级测试。

### 12.4 第三轮:补三条「同一失效形状」的缺口（2026-08-13）

§11 的复核指出五条代码侧缺口。本轮修掉其中三条，共同点是它们都属于**与本事故同一类的失效形状**——
静默、无日志、所有测试照样绿。

**⑤ allocator 滚动激活 → 已修（加武装窗）**

`roster_ever_complete` 是**新版**副本才写的记忆。2 副本 RollingUpdate 下：一局在旧副本手里已全员到齐过
（无人写该标记）→ 打到一半有人掉线 → 心跳被新副本接手 → 看到 census 缺人且标记为假 → 起表 →
deadline 到 → **判弃一场正在打的对局**。那比本闸要防的缺员局严重得多。

修法是加**第二道独立守卫**：`武装窗 = ready_wait_timeout + roster_join_deadline + 30s`，自
`allocated_at_ms` 起算，过窗任何副本都不再武装本闸。

关键在于**判据是 battle 年龄，不是任何进程内状态** —— 因此它对新旧副本给出同一答案，
新副本接手一局老 battle 时根本不会动手，与它有没有那份记忆无关。`allocated_at` 缺失（旧记录 / mock）
同样不武装（fail-safe：漏防一局缺员局，远好过误判弃一局正在打的对局）。

两条心跳路径（legacy + Model B）都接了；变异测试把 `rosterGateArmable` 改成恒真 → 2 组用例红。
仍待拍板的只剩「是否再叠一层 observe-only 采证期」，属发布策略选择，不再是正确性缺口。

**③ 组票 fence 无鉴权 → 已修（且发现比原报告更严重）**

复核说「`systemOnly` 不等于 matchmaker 身份」。核查后发现更糟：**`BeginTeamMatch` 连 `systemOnly` 都没有，
一道守卫都不存在**。Envoy 按 `/pandora.team.v1.TeamService/` 整前缀路由，带玩家 JWT 的客户端直接打得到它，
而它能给**任意**队伍上一把 roster 租约 —— 反复调用即可让那支队伍始终处于「被别人的组票占住」的状态，
队长自己反而开不了局。这是一个**既有**的骚扰/DoS 口子，与本次事故无关，本轮顺带堵上。

已按 `GetPlayerTeam` 同款加 `systemOnly`（nil usecase 用例证明拒绝发生在触达业务之前）。
~~**升级到 `internalrpcauth`（真正校验调用方是 matchmaker）仍未做**，列为 A-13~~
→ **第四轮已落码，见 §12.5**：按预判做成了三档可降级（关 / 观察 / 强制），
上线顺序是「两边配密钥 → 观察日志归零 → 翻 require」，每一步单独都安全，不靠发布顺序。
✅ **§12.6 已补完**：5 份 yaml 全部写入 + 6 条三档用例 + 两方向变异验证，接线完整。

**④ `team_addr` 留空静默跳过 → 已修（fail-closed）**

`team_addr` 现在承载两条必需链路（组票 + 对局结束复位），留空则**两条都静默不走**：
StartMatch 不再校验队伍（任何玩家可为任意 `team_id` 开局），对局结束也不再复位准备状态
——正是本次事故的第一根因。不报错、不打 ERROR，配错了没有任何人会发现。

改为启动期 fail-closed：`team_addr` 为空且未显式 `allow_missing_team: true` → `Validate()` 失败拒启。
零值即安全（同 `battle_gate_fail_open`）：骨架联调要跳过就得写下那一行，写的人就知道自己关掉了什么。
错误信息里带上关掉的是什么 + 怎么显式跳过 + 事故编号，有用例断言这三样都在。
三份纳管配置与全部 `run/**` 生成配置均已确认配了 `team_addr`，不影响现有启动。

**本轮未动的两条（仍是发布阻断）**

- **① `EndTeamMatch` 缺代际幂等**：请求只有 `(team_id, player_ids)`，做不到跨代幂等。
  ACK 丢失后玩家 re-ready / 离队重入 / 开启新局，旧 outbox 重投都可能清掉新意图。
  标准修法需要一个单调的 ready 代际（`BeginTeamMatch` 返回、`EndTeamMatch` 带回做 CAS，
  任何 ready 意图变更都推进它），涉及 team + matchmaker + match 记录三处协议改动，**未做**。
- **② 新旧 matchmaker 共存会漏局**：§12.3 只处理了「team 旧」，没处理「matchmaker 旧」——
  旧 MM 副本释放的局根本不调 `EndTeamMatch`，那些队伍照样停在 READY。窗口内等同修复前行为，
  全量滚完后自愈，但**未做**任何兜底。

### 12.5 第四轮：跨代幂等 ①、旧 MM 共存 ②、内部鉴权 A-13（2026-08-13）

前三轮把「打完一局不复位」这条主链补齐了，但复核指出它本身还有三个洞。本轮全部落码。

#### ① `EndTeamMatch` 缺跨代幂等 → **ready 代际 CAS**

**洞**：`EndTeamMatch(team_id, players)` 没有任何版本凭据。`ReleaseMatch` 靠 outbox 重投到成功，
于是这条时序是真实可达的：

```
对局结束 → EndTeamMatch → ACK 丢了（outbox 保留任务）
        → 期间玩家重新点了准备 / 离队重入 / 队长已开新局
        → outbox 重投 → **把新意图一起抹平**
```

原先只靠「谁还挂着 ready」判断，完全区分不出这两种情况。

**修法**：`TeamStorageRecord` 加单调 `ready_generation`；`BeginTeamMatch` 返回冻结那一刻的代际，
matchmaker 存进 `MatchMemberStorageRecord.team_ready_generation`，`ReleaseMatch` 回传，
team 侧只在 `ready_generation == expected` 时才动手。

**关键取舍：代际推进做成自动的，不是每处写点手动 `++`**。
「改了 ready 意图就要推进代际」有 **7 处以上**写点（SetReady / 入队 / 离队 / 踢人 / 掉线软化 /
离线摘人 / 对局结束复位），散在两个文件。漏掉任何一处的后果是**静默的** —— 代际停在旧值，
CAS 照样通过，幂等保护形同虚设，而所有测试照样绿。**这正是本事故自身的失效形状**。
所以改成：所有队伍写必须走 `updateTeam`，它在同一把乐观锁内比较前后「ready 意图指纹」
（state + 成员集合 + 每人 ready 位，按 id 排序），变了才推进。忘记推进在结构上不可能。
绕过包装器由**契约测试**机械拦住（`TestNoDirectUpdateWithLockInBiz` 扫 biz 包源码）。

指纹刻意**不含** map_id / 队长 / 昵称 / 英雄：那些变了不影响「这一局该不该被复位」，
算进来只会让代际无谓地涨，使正常的 `EndTeamMatch` 频繁 CAS 失败 —— **该复位的反而不复位**。

#### ② 新旧 matchmaker 共存会漏局 → **待复位标记 + 权威回查兜底**

**洞**：§12.3 只解决了「team 旧」（`Unimplemented` 降级）。反过来「**matchmaker 旧**」时，
旧副本根本不调 `EndTeamMatch`，那些局释放后队伍会永远停在 READY。

**修法**：`BeginTeamMatch` 落 durable 标记 `pending_match_reset_gen`（值 = 当时代际），
`EndTeamMatch` 成功后清零。标记把「必须复位」从**一次可能丢失的通知**变成
**一个可重新发现的持久事实**。

兜底 `reconcilePendingMatchReset` **不自己判断对局有没有结束** —— 那会造出第二份判定（§9.22 禁止）。
判据仍只有 matchmaker 权威 `IsPlayerCommittedToMatch`（与离线摘人闸 ②③ 同一条）；
标记只是「该去问一下」的触发器。三个条件缺一不可：标记还在 + 代际没变 + 权威说没人占着对局；
权威读不确定一律不动（fail-closed）。

**触发点与它的边界（如实记录）**：挂在 `GetMyTeam` 读路径。这覆盖了真实风险路径 ——
队长要开下一局**必然**先看队伍面板。**但它确实依赖有人来读**：整队都不打开面板时标记会一直挂着
（后果 = 退化回修复前行为，不产生新的错误状态）。刻意不为此另起周期扫描：
那等于给一个**只在滚动升级窗口存在**的问题常驻一套后台循环（§15.3）。

#### A-13 `systemOnly` ≠ matchmaker 身份 → **三档可降级验签**

**洞**：`systemOnly` 只能证明「本次调用不带玩家 JWT」，集群内网里任何 Pod 都满足。
而这两个方法杀伤力实打实：`BeginTeamMatch` 能给**任意**队伍上 roster 租约（反复调 =
让那支队伍永远开不了局），`EndTeamMatch` 能把**任意**队伍打回 FORMING。
查下来比复核说的还糟：`BeginTeamMatch` 此前**一道守卫都没有**，不是「守卫不够强」。

**修法**：复用 `pkg/internalrpcauth`，caller 固定 `"matchmaker"`，密钥与既有
`match_resume_auth_secret`（team→matchmaker 方向）**必须不同** —— 共用等于让任一方能冒充另一方。

**三档，刻意可降级**（不重蹈 §12.3 的顺序依赖）：

| 档 | 配置 | 行为 |
|---|---|---|
| 关 | 密钥留空 | 完全不验（现状；先滚 team 不会打断任何调用） |
| 观察 | 密钥 + `require=false`（默认） | 验不过只 WARN 放行；靠日志确认 matchmaker 已全量滚上签名版本 |
| 强制 | 密钥 + `require=true` | 验不过一律 `ERR_PERMISSION_DENY` |

上线顺序是「两边配密钥 → 观察日志归零 → 翻 require」，**每一步单独都安全**。
重放存储不可用时回 `ERR_UNAVAILABLE` 而不是 DENY —— 说不清是不是重放就别当成越权。

#### 本轮验证

| 项 | 结果 |
|---|---|
| 新增用例 | 代际 / 跨代重投 / 兜底四态 / 契约门禁，共 10 条，全绿 |
| 变异测试 | 去掉 CAS → 「迟到重投不得抹掉新意图」红；去掉标记 → 「Begin 落标记」红；去掉自动推进 → 代际用例与跨代用例同时红 |
| 全量回归 | matchmaker + team 全绿 |

#### 本轮**没做**的（不得当成已完成）

- **A-13 的 yaml 与用例**：`match_call_auth_*` / `team_call_auth_*` 只加在 conf 结构体与装配链上，
  三份 team yaml、三份 matchmaker yaml **均未写入**；三档行为也**没有针对性用例**。
  当前默认（密钥留空）等价于本轮之前，不改变任何现网行为，但**这条不能算接线完整**（§14）。
- ② 的兜底只有读路径触发，边界见上。
- 仍未提交、未部署；玩家 E2E、Model B 真集群、观察窗口一律为零。

### 12.6 A-13 补完 + ② 随「方案 A」下线（2026-08-13）

#### A-13 接线完整（§12.5 里欠的两项已补）

| 项 | 内容 |
|---|---|
| yaml | 5 份全部写入：`team-{dev,prod.example}` 的 `match_call_auth_{secret,audience,require}`、`matchmaker-{dev,pve,prod.example}` 的 `team_call_auth_{secret,audience}` |
| 用例 | 6 条覆盖三档：未配跳过 / 观察期验不过仍放行且**仍真验一次** / 强制期拒 / 验得过放行 / 重放存储不可用回 `UNAVAILABLE` 而非 DENY / 两个 RPC 都真的接了门（nil usecase 证明拒在业务之前） |
| 变异 | 观察档写反成「拒」→ 观察用例红；写死成「永远放行」→ 强制档 3 条红。两个方向都钉住 |

**写 yaml 时抓到一个真会炸本地栈的坑**：`matchmaker-pve` 也调同一台 team，但它连
`team_resume_auth_secret` 都没配过。我原本只给 dev/prod 加了 key，而 team-dev 设的是
`require: true` —— **PVE 会被直接拒，本地开不了 PVE 局**。已给 pve 补上同一把 key 并写明原因。
这正是三档设计要防的那类事：强制档必须等所有调用方都配上，一个都不能漏。

dev 直接 `require: true`（本机两侧同时起，没有共存窗口，开着才测得到这道门）；
prod 模板保持 `false`，注释写明「先两边配密钥 → 观察 `team_match_call_auth_observed` 归零 →
单独发一次配置翻 true」。

#### ② 待复位标记：**已随方案 A 删除**（不是放弃，是被取代）

`BeginTeamMatch` 改为在同一把锁内**一次性消费 ready** 后，「结束后还欠一次复位」这个状态
本身就不存在了 —— 标记、读路径兜底、以及那 4 条用例一并移除，proto 字段 14 已 reserved。
`ready_generation`（§12.5 ①）保留：方案 A 的 `MatchStartReceipt` 重入判定正建立在它之上。

#### 本轮遗留（**不得当成已完成**）

`team/internal/biz` 有 **3 条红**，全部属于方案 A 的在途重构，与本轮改动无关：

| 用例 | 原因 |
|---|---|
| `TestBeginTeamMatch_同Operation幂等续租` | 期望 `ErrTeamConcurrent`，实得 `not ready (state=FORMING)` —— Begin 消费 ready 后同 operation 重入需要 `MatchStartReceipt` 判定，该 message 目前**只存在于 proto，Go 侧未实现** |
| `TestEndTeamMatch_组票租约在手时可重试` | 断言 `State == READY`，而方案 A 下 Begin 后已是 FORMING（断言是旧语义） |
| `TestOnPlayerPresenceLost_组票租约在手时推迟` | 同上 |

刻意**不修**：这三条要求先定死方案 A 的重入语义（receipt 的 attempt_id 与
post_ready_generation 如何配合），属于该重构自身的收尾，不是本轮范围；替它决定会撞车。

### 12.7 收尾核查：② 残留清理曾中断（2026-08-14）

如实记录：§12.6 写「② 已整块移除」时，那次清理实际**断在半截**（会话断线）——
`offline_leave.go` 里还残留着 `PendingMatchResetGen` 的标记写入、`readyClearOpts.clearPendingReset`
字段、以及整个 `reconcilePendingMatchReset` 兜底函数，而 proto 字段已删 → **team 构建是断的**。
2026-08-14 已全部移除，构建/vet/全量测试恢复全绿。

另核实两件事：

- §12.6 表里那 **3 条红已由方案 A 作者修复**（用例已改写为新语义，现全部 PASS），
  但 `MatchStartReceipt` 的 Go 侧实现**仍不存在**（全库仅剩注释引用），B-1 的实质缺口未变。
- `services/account/login` 当前构建是断的（`battleV2BindingMismatchField` 未定义），
  归属 **dsticket-v2 并行工作**的在途改动，与本事故无关，未替其补。
  **更正(2026-08-14)**:已补上该函数——语义与 HEAD 塌成一句 if 的七条件完全一致,
  只是逐项返回第一个不一致的字段名供 `ds_ticket_binding_rejected` 日志使用
  (该日志调用点已在途,缺的只是函数本体);七分支表驱动测试钉住每个字段名,
  login 模块 build/vet/全量测试恢复全绿。

---

## 13. 待修复清单（截至 2026-08-13 收盘，按阻断力排序）

> 本节是**当前唯一的「还差什么」权威列表**。§10 的 A-x 是历史行动项（含已完成的），
> 这里只收「现在还没做完、且会影响能不能发布」的。每条写清**为什么还没做**，
> 避免下一轮把「刻意不做」和「忘了做」混为一谈。

### 13.1 阻断发布（必须先解决）

| # | 事项 | 现状 / 为什么还没做 | 归属 |
|---|---|---|---|
| **B-1** | **方案 A 重构收尾：`MatchStartReceipt` Go 侧未实现** | `BeginTeamMatch` 已改为在锁内一次性消费 ready，但「同 attempt 重试拿回同一份快照、不二次消费」的 receipt 判定**只写在 proto 注释里**，Go 侧没有。~~直接后果是 3 条红~~ → 2026-08-14：那 3 条用例已由方案 A 作者改写为新语义并转绿（见 §12.7），但 receipt 实现本身仍缺。**刻意未替它修**：要先定死 `attempt_id` 与 `post_ready_generation` 如何配合，属该重构自身的语义决策，外人替它决定会撞车 | 方案 A 作者 |
| **B-2** | **未提交、未部署** | 全部改动仍在工作副本。本仓库约定 Claude 不动 git | 待指定 |
| **B-3** | **玩家 E2E 零执行** | §12.1 复现步骤（打完一局→有人先退→队长立刻再开）从未真跑过。这是本事故**唯一**能证明修好了的验收 | 待指定 |
| **B-4** | **Model B 真集群零验证** | 花名册到齐期限在 Model B 路径（`battle_auth.go`）只有单测，没在真 Agones 上跑过 | 待指定 |
| **B-5** | **观察窗口为零** | 无任何生产/联调运行时长佐证 | 待指定 |

### 13.2 已知缺口但不阻断（记录在案，别当没有）

| # | 事项 | 边界 |
|---|---|---|
| C-1 | `roster_join_deadline=45s` / `start_presence_grace=30s` 只有推导没有实测 | 需实测「DS ready → 最后一个 `Join succeeded`」P99 再复核（原 A-7） |
| C-2 | allocator 是否再叠一层 observe-only 采证期 | 武装窗已消除**误判弃存量局**这一具体危害；再加观察期属更保守的上线姿态，是**发布策略选择**，未拍板 |
| C-3 | 客户端（UE 仓库）两项未落真实 SVN 工作树 | 消费 `RESUME_MATCH_STAGE_ALLOCATING`（原 A-4）、4011 提示与 cpp pb 同步（原 A-9）。后者阻塞于「服务端协议须先定稿并提交」 |
| C-4 | `ERR_MATCH_MEMBER_OFFLINE` 客户端拿不到是谁 | `StartMatchResponse` 只过线 code + match_id，biz error 文本里的 player_id 不过线。要点名须**加结构化响应字段**，属产品决定 |
| C-5 | §7.3 两条规则未落机械门禁 | 「判离开多久而非缺席」「新增跨服务 RPC 必须可降级」目前只靠文档 + 评审（原 A-6） |
| C-6 | 「打完一局必须重新点准备」是产品语义变更 | 方案 A 把它变成**开局即消费**，玩家在排队取消 / 准入拒绝 / 成局失败后都要重点准备。失败方向安全（多按一次不会多开一局），但**须策划确认**（原 A-12） |
| C-7 | 「离开大厅」类日志仍无去向字段 | v1 误判的直接成因。加 `travel_target` 之类字段可让下次不必翻 backup 日志反查（原 A-10，UE 侧） |

### 13.3 刻意不做（不是遗漏，别再提）

| 事项 | 理由 |
|---|---|
| 调小 `offline_leave.threshold` | 与本事故**无关**：缺席者全程没掉线。缩它只会误伤真正在重连的玩家 |
| 打开 `liveness_gate_enabled` | 那两道门的判据（按 presence 缺席判死）在结构上就是错的，INC-20260724-001 已证 100% 假阳性 |
| 给 `EndTeamMatch` 加开关 | 它是补回一条本该存在的状态机边，加开关等于给「打完要不要复位」留一个能配错的旋钮 |
| 为 ② 的兜底另起周期扫描 | 已随方案 A 整体删除；即便保留，也不该为一个只在滚动升级窗口存在的问题常驻后台循环（§15.3） |

## 11. 追记(2026-08-17):方案 A 落码,复核阻断解除

复核提出的发布阻断(decision-revisit §2)以**方案 A**(ready 在 `BeginTeamMatch` 一次性
消费 + 收据重入)整体解除,拍板、落码清单、相对原方案的简化(不需要三阶段发布)与
剩余发布前置项见 `docs/design/decision-revisit-team-match-lifecycle-and-roster-rollout.md §9`。

同批审计另修两处(细节同文档 §9.2):match 成员装配丢 `team_ready_generation`(本事故
①号防线在主路径上恒退化的断链)、`team_call_auth_secret` 同钥塌缩无机械校验。

事故仍未关闭:关闭前置 = Linux -race、真集群双版本矩阵、玩家 E2E(§8 验证矩阵的
「集群档」行)与部署观察窗口。
