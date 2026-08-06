# Battle DS 断线重连(登录直连)

> 玩家已匹配进入 battle DS 后掉线,重新登录应直接回到那场对局的 battle DS,而不是被丢回大厅。
> 本文记录该能力的设计与落地(服务级决策,CLAUDE.md §5/§7)。
>
> **状态(2026-07-15,硬切简化后)**:持久版本化 placement 已整体删除(简化决策:回到"最简化+标准")。
> 当前权威模型:
> - **路由投影**:`player_locator` 30s 租约 presence,由 ds_allocator 心跳续期;它只是投影,不是持久权威。
> - **持久权威**:matchmaker 的 match claim(`ResolvePlayerMatchContext` 只读 RPC)。login 侧
>   `resolveBattleAuthority` 先查 presence,查不到时向 matchmaker 查 claim:ACTIVE+READY+match_id
>   → 按战斗路由重签票据(claim 即权威);更早阶段/NONE → Hub。matchmaker 在 READY 提交**之前**
>   强一致写 locator 投影(`notifyBattleStrict`),提交后再弱一致重申。
> - **准入门**:Battle DS 侧 InspectBattleRoute 三态门 + Hub Admission ACK(connection-specific
>   nonce);World BeginPlay 不是权威证明,客户端 Coordinator travel 后必须复核 GetResumeContext。
> - **会话围栏**:单写者 session JTI(Redis);GetResumeContext/Logout/IssueDSTicket 均校验
>   当前 JTI,旧会话操作零副作用。
> - **物理离场证明**依然保留:"逻辑路由已切换"不能代替"旧 DS 的 PlayerController/Pawn 已退出世界"。
> - **脑裂根治(2026-07-17)**:DS 授权租约 fencing + 服务端再入屏障,见 §8。需求级硬性要求
>   「玩家任何阶段不许卡死」的正式记录与代码级落地对照也在 §8。
>
> 下文 §1~§2.2 保留问题演进背景;涉及"版本化 placement / durable saga/outbox / placement 上下文"
> 的段落为**历史方案**,已随硬切废弃,阅读时以本状态块为准。

## 1. 问题与定性

**现象**:玩家匹配成功、进入 battle DS 后网络掉线,重新登录只拿到 Hub DS 地址,被送回大厅,原对局对他而言"消失"。

**定性**:这是 2026-07-14 确认的**历史设计缺口**，2026-07-15 已在代码层闭环。原始缺口是:

- `player_locator` 当时只有短 TTL `LOCATION_STATE_BATTLE` presence；它不是可靠的业务归属记录。
- matchmaker 当时能在 READY 现签 Battle 票，但未给冷启动暴露完整匹配/placement 上下文。
- login 当时不查玩家权威归属，无论玩家是否在战斗中都只返回 Hub 地址。
- BATTLE presence 当时可能在 30s 后过期，错误地把“看不见”当成“已离开 Battle”。

## 2. 方案演进与最终选定

第一阶段采用“登录检测 BATTLE + ds_allocator 心跳续期 BATTLE presence”止血；最终方案增加无 TTL 的
版本化 placement，presence 只用于在线/监控，不再决定路由。

### 2.1 login 侧:登录检测 BATTLE 直接下发重连信息

`LoginUsecase.Login` 鉴权成功后,调 `player_locator.GetLocation(playerID)`:

- 若 `state == BATTLE && match_id != 0 && battle_pod != ""`:
  1. 经 login 的统一 `BattleTicketAuthorizer` 从 Redis live roster（Redis authority 下再核完整
     active/projection tuple）证明 player 属于 match，并取得同一快照中的权威 `ds_addr`，然后现签
     一张**新 jti** 的 battle DS 票；不得再使用 locator 中可能属于旧 UID 的地址；
  2. `LoginResponse` 返回 `battle_ds_addr = 本次 roster authority 快照的 ds_addr`、
     `battle_ticket`、`match_id`;
  3. **跳过 hub 分配**(不调 `AssignHub`)与 **`NotifyLoginPending`**——避免把 BATTLE 位置顶成
     LOGIN_PENDING / HUB,把玩家从战斗里拉出来。
- locator 已明确 `BATTLE` 后，authorizer/Redis/签名失败返回 `Unavailable`，不得继续 Hub 链。
- 否则(不在战斗)走原有 hub 流程,`battle_*` 字段留空。

**客户端契约**:`LoginResponse.battle_ds_addr` 非空 → 直连 battle DS 重连;为空 → 走 hub。
battle DS 已结束但客户端仍持旧票时，旧票会因 terminal tombstone/placement version 不匹配被拒；客户端
重新读取 `ResumeContext`。只有 terminal/leave proof 已推进 placement，`IssueDSTicket(hub)` 才能进入 Hub
assignment；UNKNOWN 不产生 seat、assignment 或 ticket。

### 2.2 ds_allocator 侧:心跳续期玩家 BATTLE 位置 TTL

battle DS 每 5s 调 `ds_allocator.Heartbeat`。心跳成功且对局处于 `ready/running` 时,
ds_allocator 从 Redis 镜像 `BattleStorageRecord` 取 `player_ids` + `ds_addr`,best-effort 刷新
每个玩家的 BATTLE 位置(`SetLocation state=BATTLE, match_id, battle_pod=ds_addr`)。

- DS heartbeat 与 locator refresh 持续成功时，BATTLE 位置在整局内续期，登录重连检测对长对局有效。
- **弱依赖**:locator 不可用只 Warn,不影响心跳与对局。
- 续期用**独立 detached ctx**(不随心跳 RPC ctx 取消),fire-and-forget,不给心跳响应加尾延迟。
- 对局进入 `ended/abandoned` 后心跳不再续期 presence；路由切换不等待这 30s TTL，而由 BattleResult
  terminal tombstone + exit-proof outbox 显式推进 placement。

### 2.3 最终降级语义:查询失败/记录缺失 ≠ 不在战斗

两种结果必须区分:

| 结果/配置 | 含义 | 行为与安全性 |
|---|---|---|
| placement 为 STABLE/PENDING HUB，且 proof/version/operation 可核 | 玩家当前权威去向为 Hub | 按相同 operation 恢复 assignment/签票；Admission 再做最终 CAS |
| placement 为 STABLE/PENDING BATTLE(match)，且 canonical match target 可核 | 玩家仍属该 Battle | 只现签 exact Battle 票；不得 AssignHub |
| placement 空记录、损坏、依赖查询失败或与 match/roster 不一致 | 玩家真实状态 **UNKNOWN** | 返回 unavailable 并退避重查；Hub seat/assignment/ticket/spawn 全零副作用 |
| 短 TTL locator/presence 缺失 | 只说明未观测到在线心跳 | 不改变 placement，不允许作为 Hub proof |

2026-07-15 状态:`Login`、`SelectRole`、`IssueDSTicket(hub)`、Hub transfer/drain 与 Hub/Battle Admission
都使用同一 placement 门；配置生成器在 apply 前和锁内校验五把相互独立的 placement authority key、
内部 resume-auth key 的
continuity/不复用。对应负例测试断言 UNKNOWN/旧 version/错误 operation 下 assigner、seat、ticket、spawn
均无副作用。

### 2.4 不变量合规(CLAUDE.md §9)

- **§17 零停机 / pb 兼容**:`LoginResponse` 只**新增字段**(编号 8/9/10),不改编号/类型/语义。
- **§16 不停服更新**:不引入任何"必须停服"依赖;新老副本同时在线时,旧 login 副本不填
  battle 字段(客户端回退 hub),新副本填——双向兼容。
- **§14 客户端只拿最小视图**:只回 `battle_ds_addr/battle_ticket/match_id`,不外露 `StorageRecord`。
- **§1 一人一 DS**:所有 Hub 签票与 Hub/Battle Admission 对 active/unknown placement fail-closed；
  BATTLE→BATTLE 只接受同 match/operation/target，终局 tombstone 阻止旧 match 复活。
- **§11 业务 ID uint64**:`match_id` 为 uint64。

## 3. 落地清单

| 位置 | 改动 |
|---|---|
| `proto/pandora/login/v1/login.proto` | `LoginResponse` 加 `battle_ds_addr=8` / `battle_ticket=9` / `match_id=10` |
| `services/account/login/internal/data/locator_client.go` | `LocationNotifier` 加 `GetLocation`;实现查询 |
| `services/account/login/internal/biz/login.go` | `Login` 检测 BATTLE → 经统一 roster issuer 签票；明确 InBattle 后失败即 Unavailable，跳过 hub / login-pending |
| `services/account/login/internal/service/login.go` | 映射新字段到 `LoginResponse` |
| `services/battle/ds_allocator/internal/biz/allocator.go` | `Heartbeat` 成功后续期玩家 BATTLE 位置(`LocationRefresher` 弱依赖) |
| `services/battle/ds_allocator/internal/data/locator_client.go` | 新增 locator 客户端实现 `LocationRefresher` |
| 两个 `cmd/.../main.go` | 注入 locator 依赖 |

## 4. 被否方案

- **专门 `BattleReconnect` RPC**:多一次往返 + 客户端多一步状态机;`LoginResponse` 本就是
  "立即完成型必须含完整业务数据"(protocol-ordering 原则 1),直接塞进登录响应更简洁。
  留作未来精细化(如需要重连专属鉴权 / 二次校验成员名单)的空间。
- **调大 locator 全局 TTL**:BATTLE 位置续期问题不该用放大全局 TTL 解决(会拖长离线判定、
  放大好友在线态误差),用心跳精确续期更干净。

## 5. 严重 bug 记录:LOGIN_PENDING 顶掉 active BATTLE(一人两处)

> 级别:**严重**(破坏不变量 §1「玩家只能在一个 DS」)。发现于本次 battle-reconnect 评审,
> 由"客户端定时重登"设想暴露。**已修复**(见 §5.3)。

### 5.1 根因

`player_locator` 的状态机守卫 `guardTransition`(`services/runtime/player_locator/internal/biz/locator.go`)
原本**只守卫 HUB 上报**——开头即 `if in.State != LocationStateHub { return nil }`,把所有
控制面写(`LOGIN_PENDING` / `MATCHING` / `BATTLE`)**无条件顶号放行**。

W4⑪ 的 BATTLE fence 当初只堵了"stale hub DS 把玩家从战斗顶回大厅",因为那时 login 还没有
重连逻辑、重登必然经过 hub。**本次新增 login 重连后**,重登在"未检测到战斗"(locator 抖动/
查询失败降级)时会调 `NotifyLoginPending` 写 `LOGIN_PENDING`,而这条路径 guard **从未设防**。

### 5.2 触发时序(一人两处)

前提:玩家正打 match X,locator = `BATTLE(match_id=X)`,ds_allocator 每 5s 续期。玩家掉线,
客户端**每秒重登一次**。只要有一次重登恰好撞上 locator 抖动:

```
T0.0  重登 #N → login.GetBattleLocation → locator 抖动返回 err
T0.1  login 降级走 hub 分支 → NotifyLoginPending
T0.1  locator 写 LOGIN_PENDING,guard 放行 → BATTLE 被冲成 LOGIN_PENDING   ← BUG
T0.3  matchmaker 读到该玩家 = LOGIN_PENDING(空闲)→ 放行进匹配队列
      → 玩家既在 match X 的 battle DS,又进新匹配 → 一人两处,破坏 §1
T5.0  ds_allocator 心跳把 locator 改回 BATTLE(但抖动窗口已被利用)
```

重登频率越高(每秒),撞上 locator 抖动的概率越大,`BATTLE↔LOGIN_PENDING` 抖动窗口越频繁。
login 侧 §2.3 的短重试能**抑制**(拉高首查成功率、走 battle 分支不写 LOGIN_PENDING),
但抑制 ≠ 根除;把重试全交客户端猛重登会放大触发概率。**根因在 locator guard,必须在 locator 修。**

### 5.3 修复:BATTLE fence 扩展到"非对局写一律拒"

`guardTransition` 在 `cur.State == BATTLE` 时,只接受两类写,其余(含 `LOGIN_PENDING`)一律
`ErrLocatorConflict`:

1. **对局生命周期控制面写**:`BATTLE` 同 match 心跳续期 / 推进(不同 match_id 视为迟到旧写被拒)、`MATCHING`(下一局撮合决策);
2. **带正确 `match_id` 令牌的 HUB 回流**(玩家打完回大厅,W4⑪ 原逻辑)。

裸登录 / 断线重登降级写的 `LOGIN_PENDING` 无对局上下文,落入拒绝分支 → **再也顶不掉 active BATTLE**。

**为何安全**:
- 不误伤正常重连——login 检测到 BATTLE 走重连分支,**根本不调 NotifyLoginPending**;
- 不卡 liveness——历史/local-off 路径的 `NotifyLoginPending` 失败只 Warn;当前 B1 Login 在该写失败时
  fail-closed 返回 unavailable,待 locator 恢复后重试。对局真结束后心跳停续,BATTLE 位置约 30s 过期;
- 权威出口不受影响——matchmaker 写 MATCHING/BATTLE、hub DS 带令牌上报 HUB 两条合法路径照常放行;
  不同 match_id 的迟到 BATTLE 写会被拒,避免旧对局心跳覆盖新对局位置。
  "一次裸登录"本就不该有权终止一场进行中的战斗。

修复后:客户端**无论不重登、还是每秒猛重登**,都不会把玩家顶出战斗 → 可放心把重试压力交给
客户端 timer(见 §2.3)追求 login 吞吐。

### 5.4 落地

| 位置 | 改动 |
|---|---|
| `services/runtime/player_locator/internal/biz/locator.go` | `guardTransition`:`cur==BATTLE` 时非对局写(`LOGIN_PENDING` 等)拒绝顶号,且 `BATTLE→BATTLE` 必须同 match |
| `services/runtime/player_locator/internal/biz/locator_test.go` | 补测:`LOGIN_PENDING`/无令牌 `HUB` 遇 `BATTLE` 被拒;同 match `BATTLE`/`MATCHING`/带令牌 `HUB` 放行,不同 match `BATTLE` 被拒 |

### 5.5 遗留(次要,待评估)

`LOGIN_PENDING` 顶掉 `MATCHING`(确认期)同类洞仍在,但危害小(确认期短、掉线确认失败会
abandoned 补偿)。本次聚焦 BATTLE,MATCHING 保持"仅拦 stale HUB"现状,后续按需收紧。

## 6. 客户端对接契约(UE 仓库 Pandora-Client 实现)

> 后端已把重连所需数据全部塞进 `LoginResponse`,客户端**不自己判断在不在战斗**,严格照字段走。
> 所有安全性(不作弊 / 不一人两处)由服务端 fence 保证,客户端只负责"照字段连 + 便利重连"。

### 6.1 登录后按字段分流(必须)

`LoginResponse`(proto `pandora/login/v1/login.proto`)相关字段:

| 字段 | 号 | 含义 |
|---|---|---|
| `hub_ds_addr` / `hub_ticket` | 4/5 | 进大厅:地址 + hub JWT |
| `battle_ds_addr` | 8 | **非空 = 玩家在战斗中**,直连该 battle DS 地址 |
| `battle_ticket` | 9 | battle DS 握手用 JWT(新签,绑定 player_id + match_id) |
| `match_id` | 10 | 重连对局 ID(uint64),本地对账 / 显示用 |

**三字段要么全空、要么全填**;battle 字段非空时 `hub_ds_addr/hub_ticket` 必为空。分流伪码:

```cpp
if (!Resp.battle_ds_addr().empty()) {
    // 断线重连:直连 battle DS,握手带 battle_ticket
    ConnectBattleDS(Resp.battle_ds_addr(), Resp.battle_ticket(), Resp.match_id());
} else {
    // 正常进大厅(既有流程),握手带 hub_ticket
    ConnectHubDS(Resp.hub_ds_addr(), Resp.hub_ticket());
}
```

铁律:**battle DS 握手必须用 `battle_ticket`,不能用 `hub_ticket`**(票据类型不同,battle DS 会校验
`ds_type=="battle"` 且 `match_id` 匹配)。客户端不得凭本地状态自判走 hub 还是 battle,一切以字段为准。

### 6.2 直连 battle DS 失败后的权威重判(必须)

`battle_ds_addr` 非空但本次握手失败,**不能据此推断对局已经结束**,也不能无条件
`IssueDSTicket(ds_type="hub")`。客户端断线、票据过期、准入短暂失败时,Battle DS 仍可能健康并按整局
roster 续租玩家的 `BATTLE(match_id)` 位置。正确恢复顺序是:

1. 重新 `Login`(或调用等价的权威 placement 查询)取得当前路由和新票。
2. 返回 battle 三字段时,继续连接该 Battle DS;不得同时签发/使用 Hub 票。
3. 只有服务端明确判定已非 BATTLE,或显式离局事务已经用 `match_id` fence 完成
   `BATTLE → HUB_PENDING/HUB`,才允许签发新的 Hub 地址和一次性票据。

`IssueDSTicket(hub)` 自身也执行同一 placement 门，Hub Admission 再校验 ticket 中的
version/operation/target；不能把安全性只寄托在调用方。反例与当前实现见 §7.3 A/§7.3.1。

### 6.3 断线重连 timer(建议,提升体验)

掉线后定时**重新登录**(重登 = 再调 `Login`),直到某次 `LoginResponse` 带回 `battle_ds_addr` 就连回去:

- **指数退避**:1s → 2s → 4s,封顶 ~8–10s。**禁止定长每秒**(防登录风暴)。
- **前台重试提示窗口 ~30s**;它只是客户端体验阈值,不是 BATTLE 权威状态的 TTL,超时后按 §6.3.1
  重新判定去向。
- **幂等**:`Login` 可安全重复调(同 account 稳定 player_id)。
- **只重试 Login/ResumeContext 才是安全入口**:服务端按 placement+canonical match 判定 Battle/Hub;客户端不得在本地超时后
  自行把目的地改成 Hub。

#### 6.3.1 重连超时后如何真回到大厅(必须)

30s 只是客户端本地计时。若 Battle DS 健康,它仍会心跳并按 roster(包含暂时掉线者)维持
`BATTLE(match_id)`;只有 **Battle DS 业务心跳**停止约 15s 才会触发 allocator abandon。两者不能混为一谈。

超时后的标准流程:

1. 降低重试频率并显示可取消的恢复 UI,再次 `Login` 读取权威路由。
2. `LoginResponse` 仍带 battle 三字段:用新票继续回原局。
3. battle 三字段为空:使用同一响应的 Hub 地址/新票回大厅。
4. 若产品允许玩家主动放弃,必须先由服务端执行带 `match_id` fence 的显式离局/结算事务;事务成功后
   才能 `IssueDSTicket(hub)`。仅在客户端本地停止 timer 不算离局。
5. 所有 in-flight Login/IssueTicket 回调必须按 recovery generation 丢弃迟到结果,避免 Battle/Hub 两次
   `ClientTravel` 竞态。

2026-07-15 已按上述流程实现：超时不再直接回 Hub，服务端 `IssueDSTicket(hub)` 与最终 Admission
都检查 placement；UNKNOWN 继续退避，session 过期先完整 Login 换新。

### 6.4 保持兼容的部分

- **proto**:新字段全部 additive，Go/C++ 都由生成器输出；旧 tag 不复用。老客户端不认识
  `ResumeContext` 时仍可按原 Login 三字段分流，服务端安全门不依赖客户端版本。
- **鉴权 / 连接框架**:battle_ticket 与 hub_ticket 同一套 JWT 握手机制,走现有通道即可。

### 6.5 UE 侧落地清单（2026-07-15 已完成）

1. `LoginResponse` 处理:按 §6.1 分流(battle_ds_addr 非空 → 连 battle,否则连 hub)。
2. battle DS 握手改用 `battle_ticket`;透传 `match_id` 供 HUD / 重连对账。
3. 直连 battle 失败 → §6.2 权威重判;active BATTLE 继续回原局,不得无条件切 Hub。
4. 断线重连 timer:§6.3 指数退避 + 前后台恢复 + generation 防迟到回调;30s 只升级 UI,不改权威去向。
5. 老版本兼容:字段为空时行为与今天完全一致(纯进大厅),无需为兼容做额外分支。
6. `UMyDsRecoveryCoordinator` 成为唯一 DS travel writer，接入 foreground、World BeginPlay、push close、
   `OnTravelFailure`、`OnNetworkFailure` 与 session renewal；Account/Match model 不再直接 travel。

## 7. 全链路断线/切后台审计:任意时间点掉线会不会卡死(2026-07-15 修复复核)

> 审计问题:玩家在「登录 → 选角 → 进 Hub DS」或「匹配 → READY → 进 Battle DS」的
> **任意时间点**切后台、断网或杀进程,回来后能否自动/可操作地恢复,并且最终只进入一个权威 DS?
>
> **当前结论:设计要求已落到当前工作树，生产环境验收尚未完成。** 2026-07-14 发现的 A~J 静态反例已按
> §7.3.1 的单一权威 placement、持久 saga/outbox 和 UE 单写者恢复协调器修复；本轮新增的 source-departure、
> 全 master 恢复扫描、capacity ledger 与 UE 物理 census 的最终命令结果以
> `docs/reviews/ds-recovery-claude-audit.md` 为唯一验收记录，不能沿用前一轮测试结果替代。真实移动端前后台、
> UDP Admission、Redis Cluster/K8s/Agones 故障注入仍是发布前验收项；
> 因此可以宣称“代码不会靠 TTL 静默自愈、失败会进入权威重试或显式登录态”，不能宣称“已在生产验证”。

这里的“不卡死”必须同时满足:恢复动作有界、切后台再前台后继续推进、杀进程后可由服务端权威态恢复、
失败后存在可见且可重复的入口、迟到回调不触发相互竞争的 travel,以及任何时刻最多只有一个可准入 DS。

### 7.1 逐断点审计表（2026-07-15）

| # | 故障注入点 | 已有机制 | 复核状态 |
|---|---|---|---|
| 1 | Login / SelectRole 请求在飞时断网或挂起 | unary 发送失败也完成回调;Coordinator generation/request-seq 使迟到回包失效;foreground 重新读权威态 | **代码通过**;真实移动端 HTTP 黑洞待发布验收 |
| 2 | Hub `ClientTravel` / PreLogin / Admission 中断 | Hub assignment 是持久恢复日志;票绑定 placement;Admission 成功提交后才开放 spawn gate;响应丢失可同 operation 重试；客户端还必须收到本次 `recovery_attempt` 对应的 Reliable Admission committed RPC | **代码通过**;真实 UDP/PreLogin 故障注入待验 |
| 3 | 已在 Hub 后断线/切线/排空 | stable assignment、connected owner 与 shard-member index 均不靠 30m TTL；切线先持久化 cleanup，再由旧 Hub 心跳取得 exact eviction，Logout ACK 后才开放目标票/Admission | **代码通过**;真机 Kick/Logout/响应丢失待验 |
| 4 | StartMatch 任一步请求取消/进程退出 | durable start operation + due index + worker/reconciler;非终态 claim/票不靠 TTL;accepted 后冲突持久补偿 | **代码通过** |
| 5 | 最后一人 Confirm 后断线/切后台 | RPC 只 CAS `ALLOCATING`;服务 worker checkpoint 精确 allocator target,再完成全 roster placement/READY | **代码通过** |
| 6 | READY push 丢失或后台恢复 | push + polling + `ResumeContext`;无 PC 保留 pending travel,World/foreground 后继续驱动 | **代码通过** |
| 7 | 匹配驱动的 Battle 首次握手失败 | 唯一 Coordinator 接管 network/travel failure,旧 generation 失效并重读权威 placement | **代码通过**;真实跨 world UDP 待验 |
| 8 | 登录驱动的 Battle 直连失败 | Hub 签票和 Admission 共用 placement version/operation 最终门;UNKNOWN 只重试,不会签 Hub 票 | **代码通过** |
| 9 | Battle 局内掉线 | active Battle 只现签该 Battle 票;30s 仅升级 UI;session 过期完整 Login 换新,凭据不可用则显式回登录页 | **代码通过**;真机长后台待验 |
| 10 | Battle 中杀进程再启动 | Login/`GetResumeContext` 恢复 route、match stage、version、operation;placement 不因 presence TTL 消失 | **代码通过** |
| 11 | 结算、回 Hub、再次匹配 | canonical roster 同事务写 release/exit-proof outbox;永久 terminal tombstone 阻止旧 Battle 复活;Hub fence 保留到 ACK | **代码通过** |
| 12 | push stream 本身断开 | stream 自动重订;匹配阶段仍有 polling/`ResumeContext` 权威兜底 | **代码通过** |

### 7.2 已完成的恢复基础

1. **Battle-aware Login 与 ResumeContext**:placement 是独立于短 TTL presence 的权威记录；active
   BATTLE 再核 canonical match context 并现签 Battle 票，UNKNOWN fail-closed。冷启动和前台恢复同时返回
   route、match stage、placement version 与 operation id；内部 match resolver 使用独立 HMAC+nonce，
   不接受或转发玩家 JWT。
2. **Hub 容量/准入账本**:strict assignment、connected session 和 drain member index 持久到显式
   Departure/cleanup；reservation 才有有界 TTL。Hub→Hub/Release 使用 index-first durable cleanup，
   assignment CAS、Bind、Departure、phase-clear 任一“已提交但回包丢失”均可重放。删除 Redis session
   不再被当作 Pawn 已退出：旧 Hub 从 credential-bound Heartbeat 收到 exact
   `(player,assignment,admission,UID/epoch/writer)` eviction，Kick 后由 Logout 发 Departure proof；目标
   ticket、push 与 Admission 在 proof 前全部关闭。
3. **匹配服务端持久恢复**:Start/Confirm/Allocation 都由持久 operation、due/active index 与服务生命周期
   worker 推进；allocator exact target 在 placement 前 checkpoint，READY 只有在全 roster binding/ticket
   完整后可见。瞬态错误不删除恢复索引，canonical 记录可重建索引。
4. **票据刷新**:READY polling 与 Battle 重登录都会现签新 jti,不要求复用已消费/已过期票。
5. **locator fence**:stale Hub logout 不能覆盖 MATCHING/BATTLE;正常结算路径把 `match_id` 作为
   `fence_match_id` 交给 Hub 更新位置。

这些机制共同保证：任意失败后玩家要么只能进入唯一权威 DS，要么停在可见、可重试的 UNKNOWN/登录态；
服务端不会用 presence/claim/ticket TTL 猜测路由，客户端本地超时也不能推进 placement。

### 7.3 2026-07-14 阻断反例（历史记录）

以下 A~J 保留原始反例，便于以后 code review 继续构造 mutant；它们的 2026-07-15 修复状态与硬门见
§7.3.1。本节中“必须/待完成”描述的是发现反例时的状态，不再代表当前工作树。

**A. `IssueDSTicket(hub)` 绕过 active-BATTLE 门。** login service 的 hub 分支直接
`ResolveHubEndpoint → AssignHub`,不执行 Login 的 locator/B1/`NotifyLoginPending` 顺序,也不核
`match_id`。Hub admission 只核 assignment/DS credential;UE Hub 又在放开 spawn gate 后 best-effort
`SetLocation(HUB,fence=0)`,失败仅记日志。于是玩家可以物理进入 Hub,locator 却仍是 BATTLE,形成双归属并
导致后续重登/匹配异常。必须把 placement/fence 校验放进签 Hub 票和最终 admission,不能靠客户端规避。

> **P0 止血已落地(2026-07-14;2026-07-15 Codex 复审修正)**:`ResolveHubEndpoint` 与 `SelectRole` 前新增
> `guardHubRouteAgainstActiveBattle` 显式三态权威门(`InspectBattleRoute`:ACTIVE →
> `ErrInvalidState` 零副作用拒绝;TERMINAL=投影记录显式 state ∈ {ended, abandoned} 且 match_id
> 一致,唯一放行路径;其余一切——roster 漂移/非成员 PermissionDeny、记录缺失、stale、错误——
> UNKNOWN 一律 fail-closed。复审前曾把通用 `ErrPermissionDeny` 当终态证明,roster 漂移会被误放行,
> 已修正),负例测试见 `login/internal/biz/hub_route_gate_test.go`(含 TOCTOU 并发终局切换、
> SelectRole 零 role 落库)。2026-07-15 又完成 Hub Admission placement Commit、版本票据和持久 assignment
> 恢复日志；A/J 最终状态见 §7.3.1。
> 取舍:对局进行中的“主动退出回大厅”会被本门拒绝,需候选 B 的显式离局事务;正常结算不受影响。

**B. 30s 本地超时被误当成 Battle 已结束。** 客户端掉线不影响健康 Battle DS 的业务心跳;roster 仍含
该玩家,所以 locator 可在整局内保持 BATTLE。超时只能升级 UI/降低重试频率,不能直接改变 DS 去向。

> **P0 止血已落地(2026-07-14,客户端)**:`MyAccountModel` 删除 `FallbackToHubViaIssueDSTicket`;
> 窗口到期只触发一次 `OnBattleReconnectTimedOut`(可取消恢复面板)并降频到封顶间隔继续退避重登;
> battle 直连失败/无 PC 同样转权威重查;重连中收到无 battle 的 LoginResponse 视为服务端权威
> 大厅路由,接受并走正常登录分流;新增 `AbandonBattleReconnect` 供玩家主动放弃(回登录,不绕路由)。
> 2026-07-15 UE 全量 Editor 编译与恢复 Automation 已通过；服务端 TTL 缺口由持久 placement 根治。

**C. READY 时无 PlayerController 会永久去重。** `HandleBattleReady` 在实际 `ClientTravel` 前先停 polling、
写 `PendingBattleDsAddr`;`ConnectToBattleDs` 无 PC 时只返回。之后相同 READY 被去重,World BeginPlay 也不
补发连接。必须只在成功发起 travel 后提交去重态,或保留可跨 world/foreground 的待连接任务。

**D. 重启后直连 Battle 的结算出口缺上下文。** AccountModel 的 battle Login 分支没有把
`match_id` 写回 MatchModel,也没有 Hub endpoint;`ReturnToHubDs` 因 `CurrentMatchId==0` 且 Hub 地址为空可
直接返回 false。恢复上下文必须来自权威 Login/placement,不能依赖进程重启前内存。

**E. 回 Hub 失败后不可安全重试。** `ReturnToHubDs` 在申请票前就 `ResetMatchState`;失败只停在当前关卡。
再次调用会用已清零的 `CurrentMatchId` 覆盖保存的 fence。必须把 recovery operation/fence 保持到 Hub
Admission ACK,并为 HTTP、无 PC、Travel/Admission 失败提供幂等重试。

**F. 迟到 RPC 可触发相互竞争的 travel。** UE `HttpTotalTimeout` 默认 0;30s 重连截止不会取消/失效仍在飞的
Login。客户端没有 recovery generation,Account/Match 还共享全局 `OnIssueDSTicketComplete` multicast。
因此 Hub ticket 回调与旧 Login(Battle 或 RoleSelect)可先后改场景。每次恢复操作必须有 epoch/cancel token,
回调只允许当前 epoch 提交一次目的地。

**G. Matchmaker 的持久状态推进绑在玩家请求 ctx。** `StartMatch` 的 body→claims→queue 三步写在失败时用
原请求 ctx 回滚;若 ctx 已取消,可留下未入队的 body+claim,liveness sweep 看不到,重试返回 AlreadyMatching。
最后一人 `ConfirmMatch` 又在同一 ctx 内同步 AllocateBattle、写 READY、写 locator、发 push;切后台/断线可在
已经 CAS ALLOCATING 后掐断 finalize。必须用独立有界 ctx + durable work item/worker,并扫描未入队 claim。

**H. ALLOCATING/超时恢复索引可丢。** allocator error 直接 `onMatchFailed` 而未先 CAS
`ALLOCATING→FAILED`;`failMatch` 会尝试移出 active。`expireOnce` 的 `UpdateMatchWithLock` 遇 Redis 瞬态错误
也直接 removeActive。记录若仍为 CONFIRM/ALLOCATING,后续无人扫描,客户端可一直看到旧阶段直到 30min TTL。
只有观测终态或成功移交 durable worker 后才能移除 active;瞬态错误必须保留并重试。

**I. 赛后 match claim 释放不可靠。** Matchmaker `ReleaseMatch` 吞掉 Redis 清理错误仍返回 nil;
battle_result 的调用也是 best-effort。若 DB 已提交而 release 失败,幂等重报命中 `alreadyRecorded` 后直接返回,
不再释放旧 claim,玩家回 Hub 后可被 `AlreadyMatching` 挡到 TTL。match release 必须进入与结算提交关联的
durable outbox,幂等 replay 持续修复到 ACK。

**J. locator 缺失不等于不在 Battle。** B1 已做到“locator 查询报错→拒绝”,但如果 DS→locator 的
best-effort 刷新连续失败到 key 正常过期,查询会成功返回非 BATTLE;Login 不会再查 live roster,仍可走 Hub。
需要权威 placement lease,或在非 BATTLE/缺失时用可证明的 roster/terminal 状态排除 active Battle;
Hub 签票与 admission 必须执行相同最终门。

#### 7.3.1 2026-07-15 修复闭环

| 原反例 | 当前硬门 |
|---|---|
| A / J 双归属与 TTL 误判 | `player_locator` 持久版本化 placement 成为唯一权威；presence TTL 只表示网络在线。Hub/Battle ticket 同时绑定 `version + operation_id + target identity + source_match_id`，Hub `AcknowledgeAdmission` 和 Battle Admission 都校验当前 binding，旧版本票即使未过期也拒绝。|
| B 本地超时改路由 | 30s 只升级 UI/降低重试频率；Coordinator 重新调用 Login/`GetResumeContext`，只有服务端明确 HUB 才能 Hub travel。|
| C READY 无 PC | Coordinator 保留 pending target/ticket；World BeginPlay、foreground 和退避 ticker 继续驱动，只有实际发起 travel 后才提交 attempt。|
| D 冷启动丢上下文 | additive `ResumeContext` 返回 `route/match_id/match_stage/placement_version/operation_id`，同步恢复 Account/Match model。Login→match resolver 使用独立服务 HMAC、exact method/audience/player 绑定和共享 Redis nonce 防重放。|
| E 回 Hub 不可重试 | fence、operation、placement binding 保留到 Hub Admission 提交确认；ticket、PC、travel 或 ACK 任一步失败均由同一 operation 幂等恢复。|
| F 竞争 travel | GameInstance 级 `UMyDsRecoveryCoordinator` 是唯一 DS `ClientTravel` writer；每个回调同时校验 generation、request sequence 和 expected phase，前台/network/travel failure 先使旧 generation 失效。|
| G Start/Confirm 绑 RPC ctx | Start 是持久 operation + worker；durable ACCEPTED 后不再向调用方返回“未受理”。Confirm 只 CAS 到 ALLOCATING，服务 worker checkpoint allocator exact target 后独立完成 placement/READY。|
| H active 索引丢失 | Redis 瞬态错误保留 due/active；只有 READY/FAILED/记录不存在才移除。reconciler 从 canonical operation/match 重建索引；失败先持久 CAS terminal。|
| I 释放吞错 | BattleResult 用 canonical roster 做精确成员校验，并在结果事务内写 `match_release_outbox` 与 `battle_exit_proof_outbox`；失败/幂等重放持续重试，claim 使用 compare-delete。|
| 终局与旧 Battle 复活竞态 | 每个 canonical roster 成员先写无 TTL、版本无关的 signed terminal tombstone，再推进版本化 exit proof；旧 Match 的 Begin/Bind/Admission 与 tombstone 同 Redis slot 原子观察并 fail-closed。|
| Hub 切线/排空崩溃与双 Pawn | source exact identity、target binding 和 cleanup phase 随 assignment CAS 持久化，并用永久 exact index 供重启扫描；target Bind 成功后才给 source DS 下发 exact eviction，只有 source Logout `AcknowledgeDeparture` 或确认的 GameServer UID teardown 才能完成 phase。Redis capacity release 本身不是物理离场证明。|

实现约束：

1. `UMyAccountModel`、`UMyMatchModel` 不得直接出现 DS `ClientTravel`；静态门只允许
   `MyDsRecoveryCoordinator.cpp` 调用。
2. placement、terminal tombstone、非终态 start claim/ticket 和恢复 operation 不得以 TTL 作为业务终态；
   必须由显式 terminal、Admission ACK 或持久 worker 清理。
3. 对局仍 active 时，客户端“主动放弃”不能自行进入 Hub。若产品需要中途投降/离局，必须新增服务端
   roster/placement terminal 或 leave proof；当前安全行为是拒绝并继续恢复原 Battle。
4. session 过期时，内存中仍有登录凭据则走完整 Login 换新再恢复 placement；冷启动无凭据或凭据被拒绝时
   转显式登录页，不无限重放旧 token，也不绕到 Hub。
5. UNKNOWN、Redis/locator/match resolver 故障和凭据校验失败都只能暂时不可用；不能 AssignHub、签票或
   放开 spawn gate。最终不变量是：**唯一权威 DS，或者明确可重试/可登录状态；绝不双 Admission，绝不静默等 TTL。**
6. strict Hub assignment 与 shard-member reverse index 的 TTL 必须为 0；只允许 exact Departure、
   Release tombstone、transfer cleanup 或确认的 UID teardown 删除。长后台/长连接不得从 drain 枚举消失。
7. cleanup 的 source-complete 只能表示 physical Departure proof，不能由删除 Redis connected ledger 推断。
   live session 必须返回 `DepartureRequired`，并保持 target ticket/push/Admission gate。

### 7.4 必跑验收矩阵

| 注入阶段 | 最低通过条件 |
|---|---|
| Login、SelectRole 请求前/中/响应丢失 | foreground 后自动恢复或按钮可重试;所有 in-flight 有界释放;只产生一个 session/seat |
| `StartMatch` 已 ACCEPTED、worker 尚未创建 ticket 时立即查进度 | 命中 durable start operation 并返回 QUEUEING；不得瞬时 4001、不得因此重新签 Hub 票或触发同地址 Hub travel |
| Hub ticket 已签、UDP/PreLogin/Admission 任一点失败 | 不提前生成 Pawn/写 HUB;重登获得唯一新路由;旧 reservation 有界回收 |
| 已在 exact committed Hub 时恢复出 `Route=HUB + STARTING/QUEUEING/ALLOCATING` | 当前连接、完整 placement/target/assignment tuple 均相同则原地继续匹配轮询，零 `IssueDSTicket(hub)`、零 `ClientTravel` |
| 同 Hub 确需重连，旧 Departure 与新 Admission 两种先后顺序 | successor lease 不重复计容；两种顺序都收敛为一个 connected owner；无 exact successor/reservation 的 Admission 仍 fail-closed；迟到旧 Logout 不删新 owner |
| Hub 切线/Release/drain 在 assignment CAS、Bind、source Kick/Departure、phase-clear 前后崩溃或丢包 | 重启只恢复同一 target/op；source Pawn 未 exact Departure 前 target 无 ticket/push/session；完成后 source session=0、target session=1；长连 member 仍可排空 |
| StartMatch body/每个 claim/queue 写入前后取消 ctx | 不留未入队 claim;重试可复用或清理原 operation;恢复扫描不只依赖 queue ZSET |
| CONFIRMING→ALLOCATING→READY 任一点取消/allocator error/Redis error | durable worker 独立完成或 CAS FAILED;active 索引不因瞬态错误丢失;客户端有界看到 READY/FAILED |
| READY 后切后台、换 world、暂无 PC | 待连接任务跨生命周期保留;恢复 PC 后继续 travel,或权威失败后明确重排 |
| Battle 首次握手、局内任一点断网/切后台 | active Battle 只发 Battle 票并回原局;绝不并发 Hub travel |
| 30s 后 Battle 仍健康 / 已崩溃两种分支 | 健康时继续 Battle;崩溃补偿完成后才进 Hub;两种状态都只有一个可准入 DS |
| Battle 中杀进程、重启、结算、回 Hub | `match_id`/fence 可由服务端恢复;Hub 票/Travel 任一步失败均可幂等重试 |
| Result DB commit 后 match release 丢包/Redis 故障/进程重启 | outbox 持续重试至旧 claim/票据/match 精确清理;玩家可立即开始新匹配 |
| push stream、HTTP、locator、Redis、DS 心跳分别断开 | 最多暂时不可用,恢复后继续推进;未知 placement fail-closed,无 Hub/Battle 双归属 |

验收必须包含真 UE 客户端的前后台/断网/杀进程自动化与真实 UDP Admission;现有纯策略/Go 单测不能替代。

### 7.5 2026-07-15 本轮加固实施状态（当前工作树）

本节只记录可从当前代码与测试直接核实的事实；命令是否在最终合并后的同一快照全绿，以
`docs/reviews/ds-recovery-claude-audit.md` 的结果表为准。

1. **placement 的逻辑切换与物理离场拆成两阶段门。** `BeginPlacementTransition` 在清空目标字段前，
   原子捕获 immutable source binding 与 exact source DS tuple；非 bootstrap 的 `CommitPlacementAdmission`
   必须先看到与当前 PENDING `(version,operation,target,source)` 完全一致的
   `source_departure_confirmed`。`Begin`/`Retarget` 会清掉活动确认，Commit 只把它移入 audit-only
   `last_source_departure_*`，后续 transition 不能复用旧 proof。
2. **Hub→Battle 使用 Hub departure authority。** Matchmaker 在 READY/签票前按全 roster 调
   `EnsureHubDepartureForBattle`；Hub allocator 只有在 exact reservation/connected owner 已由 Logout
   Departure ACK 或 immutable GameServer UID teardown 证明消失后，才对当前 locator source tuple 生成稳定
   `hub-departure:<digest>`，签名调用 `ConfirmPlacementSourceDeparture`。locator ACK 丢失时，assignment cleanup
   journal 保留相同 source 与 proof id，重启后只重放同一证明。
3. **Battle→Hub 使用独立 Battle departure authority。** ds_allocator 先核当前 PENDING Hub placement，再用
   durable departure journal 驱动 exact eviction；只有 Battle DS 的完整 Controller/Pawn census 不再包含该
   exact owner，才得到稳定 `departure_id`。随后还必须用独立 Battle departure key 向 locator 确认；确认 RPC
   失败或 ACK 丢失对 login 仍是 retryable，不能提前返回 `Departed=true`、不能签 Hub ticket。
4. **UE census 不再只信 bookkeeping map。** Battle DS 同时检查 pending/admitted context、
   `ControllerToDSTicketClaims`、`ActivePlayers`、World 中全部 PlayerController/Pawn 与 weak Pawn claims ledger。
   任一物理对象无法由完整 v2 exact tuple 归因，整份 census fail-closed；Logout 先清 map、Pawn 后销毁时，
   weak ledger 仍把玩家报告为 active。exact eviction 在执行任何 Kick/Destroy 前先完整扫描 ABA 冲突，旧
   version/operation/UID/allocation 不能踢掉新 admission。
5. **locator 升级有机械迁移门。** `placement_preflight` 只读扫描 Redis Cluster 的每个 master；坏 key、坏
   protobuf、缺 exact target 的 STABLE、以及缺 immutable source 的非 bootstrap PENDING 都使进程非零退出且
   不改数据。canonical Begin-before-Confirm 的 departure marker 必须全空并允许通过（init 在每次 Pod restart
   都会执行，新 locator 启动后才能继续 Confirm）；marker 部分存在，或 confirmed type/id 与 physical source
   不符才阻断。player-locator Deployment 在本协议迁移期使用 `Recreate`，禁止仍会
   绕过新 Commit 门的旧 writer 与新 writer 同时运行。preflight 不通过时必须先完成/补偿遗留 operation，
   不能靠手改字段伪造 proof。
   online prod 还会对真正承流的 `player-locator-ds-auth-green` CAS replacement 强制同一 Recreate + same-digest
   init gate，并在 rollout 后回读终态；仅给 replicas=0 的 dormant blue 加 init 不算发布门。
   STABLE 的 `last_source_departure_*` 只作审计：legacy 四元组全空不阻断下一次安全 Begin；partial 或完整但
   version/op/type/id 不匹配才阻断。缺旧 admission/proof/timestamp 也不能凌驾于 exact current route/target/version/op
   之上成为永久 init 死锁。
6. **恢复索引不再只扫 Redis Cluster 的一个分片。** ds_allocator 启动立即、之后周期性对每个 master
   `SCAN pandora:ds:battle:{*}`，从 canonical battle record 重建丢失的 active ZSET。修复使用 `ZADD NX`，
   不覆盖 quarantine/release 写入的 score=0 立即唤醒；未知状态或坏 protobuf 整轮 fail-closed。永久恢复
   fence 会被重建，有 TTL 的 ended/已完成 abandoned audit 不会被复活。
7. **Matchmaker 新反例已收口。** auto-confirm/solo 先持久为 fully-accepted CONFIRM，只有 ticket union 与
   canonical roster 完全一致、全部 claim 已持久后才 CAS ALLOCATING；allocation worker 在外调 allocator 前
   再做同一 discovery 门。allocator `ERR_UNAVAILABLE` 保持未知结果，不再误作 definitive failure/requeue。
   release 对 ticket/claim 使用 exact compare-delete；canonical match 已缺失时只接受 fallback roster +
   ticket→match→member 的机械证明，歧义 fail-closed，ABA 新 claim 不删除。ticket canonical 已删但前次
   queue `ZREM` 失败时，幂等重放仍会清 stale derived queue。BattleResult release outbox 对下游
   commit-but-ACK-lost 保留并重放，幂等结果上报会把延迟行重新置为立即可执行。
8. **Hub 容量反例 §7.16.4 已按逐 identity ledger 实施。** reservation 与 connected ownership 分表，
   `player_count = reservations + sessions` 只是派生投影；心跳实报仅写审计字段，不能覆盖 reservation 或把
   漏报解释为离场。Admission 原子消费 reservation 转为无时间 TTL connected owner；Release/Transfer 对 live
   owner 只能返回 DepartureRequired，只有 exact Departure 或已确认 UID teardown 才能删除。路由门还要求
   DS 实报 `MaxPlayers == shard.capacity`，不一致不再继续分配。

客户端恢复的结构性硬门保持不变：Account/Match model 中不得出现 DS `ClientTravel`，Coordinator 是唯一
writer；每个异步结果同时匹配 generation、request sequence 与 expected phase。前台恢复、World BeginPlay、
network/travel failure 都重新读权威 `ResumeContext`。这些代码门能证明 stale callback 不能直接推进 travel，
但不能代替真设备 OS suspend、真实 PendingNetDriver/UDP 丢包和跨进程重启测试。

**声明边界**：当前工作树的目标安全结论是“唯一权威 DS，或者显式 retryable/登录态”。在最终全量 Go/UE
命令、真 Redis Cluster、K8s/Agones UID teardown、真实 Hub/Battle UDP Admission 与移动端前后台矩阵完成前，
不得写“任意故障已在生产无卡死”或“P0 已上线验证”。

### 7.6 Allocation abort、物理回收与旧 PodUID 升级门（2026-07-15 已实施）

StartMatch 在产生 operation/claim/ticket/queue 任何副作用前先要求 durable placement 是 exact
`STABLE_HUB`，allocation worker 在 Agones 外调前再做一次同样的权威检查。第一次检查后发生的
route 竞争不能因为 RPC 已启动就获得 Battle 实例。

已发生外部 allocation 但不再允许发布 READY 时，Match 先以 exact operation+full target CAS 从
`REQUESTING` 进入 `ABORTING`；READY 只接受仍是 exact REQUESTING 的相同目标。allocator 通过
payload-bound、独立生产密钥的 `AbortPreactiveBattle` 写持久 abort fence，按 GameServer UID+Pod UID
回收。RPC 或 K8s 结果未知时 Match 保持 ALLOCATING+ABORTING 与原 ticket/claim/active；只有回收
明确成功后才 FAILED 并精确清理，不会把未知当失败重排。

Model-B 的物理回收不再以 DELETE 2xx 或 GameServer 消失作为充分证明；新 allocation 必须在
usecase 和 durable finalize 两层持久真实 `pod_uid`。回收只在旧 GameServer UID 与 Pod UID 都明确
消失后返回成功，之后才写 teardown proof。Kafka 的 ABANDONED ACK 到达后再写绑定 full target 的
lifecycle marker；late abort 只能在这两份持久证明都精确匹配时补成。`RELEASED` 回收 journal
不代表 Redis authority cleanup ACK 已收到，重放仍需重读并精确清理剩余 auth/battle。

滚动升级前写入的旧 battle record 可能没有 `pod_uid`。completed、empty heartbeat、stale sweep、
abort 与 preactive 路径都在 terminal/permanent transition 前尝试精确回填：必须同时匹配 GameServer
name+UID、allocation_id 和 owned Pod。对象缺失或同名重建时保留 retryable fence，零 terminal/删除
副作用，绝不伪造 UID。pre-Prepare 崩溃的 `instance_epoch=0` 另走专用 preactive release：先持久
fence，按真实 GS/Pod UID 回收，确认成功才 purge，不生成未曾有 credential 的玩家 teardown proof。

该旧记录策略保证不误删/不双准入，但如果升级前 K8s 的不可变身份证据已丢失，代码不能
安全猜测原 Pod UID。因此发布前必须审计并清零这类空 `pod_uid` active record，或保存可审计的
不可变缺失证明；在该门未通过前，只能宣称 fail-closed，不能宣称所有历史记录都会自动收敛。

为了不把上述要求留成人工 checklist，ds_allocator serving image 已加入只读 one-shot PodUID
preflight。它以独立高熵只读 ACL 身份读取 credential-free immutable Redis 配置：Cluster 对 exact
16384 slots 的每个 master 做拓扑前后快照与 PING/SCAN/GET；standalone 必须证明
`cluster_enabled:0 + role:master`；Sentinel/Cluster membership 漂移、ACL 非 exact、坏 key/protobuf、key
中途消失、future/partial state、非 RFC4122 canonical UUIDv4 allocation_id、跨 master 重复 key，或 exact
identity 缺 `pod_uid`，都 fail-closed 且零数据写入。

激活证据改为同一 RunId、green immutable image/config、Redis config/target identity 与 topology 绑定的
三份不可变 Job：`prepare` 在排空前审计存量；`drained` 必须在旧 blue writer=0、capability 清空和 drain
marker 后、green 启动前完成；`final` 必须在 green exact capability/strict writer 启动后、生产 Service
仍全部指向 blue 时完成。final PASS 前不得切 Service 或 CAS epoch。epoch=2 的 Audit/幂等重跑只能读取
原三份证据，禁止事后建 Job。CAS 后临时只读 ACL 用户必须由独立控制身份固定 `DELUSER`，以 WHOAMI 与
GETUSER absent 双遍回读；失败保留明确 cleanup-pending，同 RunId 可幂等继续，不能把 CAS 成功当发布完成。

PodUID 约束同时落在写入面：`BattleStorageRecord` 的全部 23 个 Redis content write（包括独立 quarantine
命令）统一经过不可逆 strict Model-B gate。新/重写记录必须携带完整 PodUID；legacy 只允许 exact PodUID
backfill，PodUID 不可变且未知 protobuf bytes 必须保留。全局 required 值不是裸数字，而是固定
`2@ds-auth-v2-pod-uid-write-invariant-v1`；五个生产 writer 的 exact feature 集合被绑定到初读、capability
注册事务、watch 和 activation record。旧 numeric epoch=2 binary 无法解析该值，新 binary 也拒绝裸 `2`，
因此不能只回滚 feature/旧 writer 而继续写 Model-A。

当前还有一项明确的外部发布硬阻断：仓库没有可执行、可在线验证并覆盖整个 preflight→CAS 窗口的 Redis
topology-change/failover/reshard lease provider 与信任根。现有共享 PowerShell gate、`dsauth-activate -apply`
和核心 `AdvanceRequired` 均 fail-closed；普通 online release 也在 build/push/apply 前停止。ConfigMap 或一次
拓扑快照不能冒充执行锁。接入真实 provider 前，上述实现只能证明危险发布路径被关闭，不能宣称生产激活
能力已完成；接入后仍必须跑真实 Redis/K8s/Agones/UDP/移动端故障矩阵。

### 7.7 StartMatch 可见性与同 Hub 重连反序事故（2026-07-15）

本地 K8s 真 UE 复测出现过一次明确的“进 Battle 前闪回登录页，随后又被拉进 Battle”。它不是 Battle
Admission 失败，而是以下四个独立窗口串联：

1. `StartMatch` 已把 durable start operation 持久化并返回 ACCEPTED，但后台 worker 尚未创建 ticket；
   紧随其后的 `GetMatchProgress` 只查 Match/Ticket，瞬时返回 4001。
2. 客户端把该进度错误升级为权威恢复。此时物理 placement 仍是 HUB，`ResumeContext` 同时正确携带
   STARTING/QUEUED match；旧 Coordinator 却重新申请 Hub 票，并对当前同一 Hub 地址执行 `ClientTravel`。
3. Hub allocator 在旧 connected session 存在时没有建立 reservation。`ClientTravel` 先触发旧连接 Logout，
   exact Departure 删除 session；新连接稍后 Admission 时既无 session 也无 reservation，最终门以 code=8 拒绝。
4. `ConnectionLost` 来自当前 `GameNetDriver`，旧 OnlineSession 只拦 PendingNetDriver；UE 默认
   `Browse Entry?closed → Login` 抢走画面。与此同时 durable match worker 正常推进到 READY，Coordinator
   最终仍拿到 Battle 票并进场，所以用户看到的是“先被踢登录，再被拉进战斗”。

闭环必须同时具备以下四层；任何一层都不能用另一层代替：

- **Matchmaker 连续可见性**：`GetMatchProgress` 在 Match/Ticket miss 后读取 durable start operation，先做
  exact member 授权和 game-mode 隔离，再把 ACCEPTED/TICKET_READY/CLAIMING/CLAIMS_READY/QUEUED 投影为
  QUEUEING，COMPENSATING/FAILED 投影为 FAILED。若 start operation 也 miss，必须再次读取 Match/Ticket，
  覆盖 worker 的“先建 ticket、后删 operation”交接 TOCTOU。
- **UE exact committed-Hub no-op**：只有当前 World 的 exact open `GameNetDriver`、本连接收到的 Reliable
  Admission ACK，以及 `STABLE + placement version/operation + pod/UID/epoch + assignment + release track`
  全部一致，才允许原地完成恢复而不签票、不 travel。STARTING/QUEUEING/ALLOCATING 可以与物理 HUB 共存，
  MatchModel 继续恢复上下文和轮询；tuple 任一字段漂移仍走正常权威路由。
- **Hub successor lease**：同 assignment 在 connected 状态下重签时创建有界、placement-bound、同 Redis
  slot 的 successor。它在旧 session 存在时不重复计容；旧 Departure 先到时原子变成该 assignment 的一个
  reserved seat，新 Admission 先到时原子消费并替换 owner。Admission 仍必须消费 exact reservation/successor，
  迟到旧 Departure 仍由 `admission_seq + admission_id` 拒绝。Release、确认的 UID teardown、到期清理必须
  覆盖 successor；新旧 ledger writer 不得滚动并存，Hub allocator 发布使用 Recreate 单写者门。
- **UE 断线所有权**：Pending handshake、当前 DS、ReturnHub 的旧 Battle source 分别保存 exact weak
  NetDriver 和 generation，一次事件只能消费自己的 token。ReturnHub intent/AwaitingTicket 绝不提前授权；
  只有 pre-travel fence 已通过、紧邻实际 Hub `ClientTravel` 才 arm 旧 source。该 token 可跨权威重试、连续
  generation churn 和前后台挂起，直到 exact driver teardown 被消费；权威纠正到 Battle、ticket/travel
  失败、取消或改成非 Hub target 时必须显式清除。当前 DS 有可用 session 时发生 ConnectionLost/Timeout，
  OnlineSession 抑制 UE 默认 Entry/Login，由 Coordinator 重读权威路由；不同/stale driver 仍走引擎默认流程。

该事故的最低回归日志不变量：

1. ACCEPTED 后立即查询只能看到 QUEUEING/后续阶段，不能出现 4001。
2. 已在 exact committed Hub 且 match 为 STARTING/QUEUEING 时，不得出现新的 Hub `IssueDSTicket` 或
   `route=Hub ClientTravel`。
3. 强制重连测试分别执行 `old Departure → new Admission` 与 `new Admission → old Departure`，最终都必须
   `reserved=0, connected=1, occupancy=1`；旧 Logout 重放为 conflict/幂等，不能删除新 owner。
4. 从 StartMatch 到 Battle Admission 完成之间，不得出现 `Browse Entry?closed` 或 Login world；任意 DS
   ConnectionLost 要么启动权威恢复，要么因 exact in-flight source travel 被一次性消费，不能静默吞掉。

### 7.8 Hub successor policy V3 的不可回滚发布（2026-07-15）

successor lease 不能只靠新 Hub 的 feature 字符串或镜像 digest 区分版本。全局 required raw 必须从不可变
V2 `2@ds-auth-v2-pod-uid-write-invariant-v1` 前进到不可变 V3
`2@ds-auth-v2-hub-successor-lease-v1`；Redis writer epoch 仍是 2。Capability 的
`supported_policy_generation=3` 与 `supported_policy_id` 由 Go 库编译期自动写入，部署参数不能伪造。
V3 audit 对五类 writer 都精确要求该身份，因此旧 login/locator/ds_allocator/battle_result 即使 feature/digest
看似相同也不能混入；Hub 同时必须恰好一个 writer，Deployment 使用 `replicas: 1 + Recreate`。

发布顺序是硬契约：

1. 平台以强制 HTTPS+mTLS+auth 的 `tools/scripts/prepare_hub_successor_policy.ps1` 在 required V2 下 staging
   全部 V3-capable writer。它先对 exact-name 五个 Deployment 做 server-side dry-run，逐个回读合并后的 JSON，
   在任何 apply 前验证 identity Secret/template、immutable image pin、selector/count、locator preflight，以及
   Hub `replicas: 1 + Recreate`；错误清单不能先制造 writer overlap。新 Hub 只注册 lease capability，不启动
   RPC、后台 writer，也不进入 Service Endpoint；因此 staging 审计只能按显式 Pod UID 读取 etcd capability，
   禁止等待 Hub Ready。
2. prepare 脚本 create-only 创建 immutable `pandora-ds-auth-policy-v3-evidence`，精确绑定 staged service counts、全部 Pod UID、
   每服务 image digest、完整 V3 features、V2/V3 raw、run-id、Kube context/namespace 与
   `capability-lease-not-service-endpoint-v1` 契约。该 marker 必须是 exact schema、`immutable: true`、不得
   deletion；若已存在只能精确回读，不允许覆盖或用普通 ConfigMap 猜测证据。
3. 以同一组必填 HTTPS endpoint、CA/cert/key、server/client identity、identity revision 与 forbidden prefix
   运行 `tools/scripts/activate_hub_successor_policy.ps1 -Phase Audit`；通过后再以相同参数运行
   `-Phase Activate`。V2→V3 是同 writer epoch 的 policy-only CAS：事务同时比较 required raw/modRevision、
   activation lock、create-only record，以及所有 audited capability ModRevision，不依赖 Redis topology provider。
4. required watch 使 staging 进程 Lost/退出，Kubernetes 重启后只能在 V3 重新拿 capability；脚本在 CAS 与
   immutable record 二次验证后，从固定五个 canonical Deployment/selector 重新派生当前 live Pod UID，要求
   每个 Pod 的唯一 controller ReplicaSet→Deployment 链和目标 writer container Running/imageID，然后审计
   `acquired_policy_generation=3`。在解释 Endpoint 数量前，prepare/activate 都必须回读
   Service/hub-allocator，验证 exact green 三标签 selector（无额外 selector）、ClusterIP/20021 和
   `publishNotReady=false`；Hub ready Endpoint 在 CAS 前必须精确为 0，CAS 后必须精确为 1 且 UID等于当前
   唯一 Hub。最终再做一次 acquired-V3 audit，才创建 immutable completion marker。崩溃重跑若已是 V3，
   直接从 record-only proof 和 post-CAS finalize 继续，绝不重跑 V2-only NotReady 门或重复 CAS；只读 Audit
   和普通 online release 都必须回读 exact completion marker，缺失不能报告健康。

一次性 evidence 中的 staged UID/digest 只证明激活线性化点，不是普通发布的永久 allowlist。Pod eviction
换 UID 后，finalize/普通发布使用当次 canonical live UID 与 pinned digest 做审计；仍持续验证原 V2 pod_uid
evidence、独立 V3 record/provenance 和 completion marker，但不会要求当前 UID 等于历史 marker UID。

全新本地集群不走 missing→V1→V3 两事务窗口，而是在任何 writer 启动前执行
`--zero-writer-genesis-v3 --apply`：一个 etcd 事务同时断言 required/record 不存在、activation lock token
一致、整个 capability prefix 为空，并创建 V3 required 与 immutable genesis record。已有 V1/V2 状态绝不
自动冒充 fresh；只能显式 Reset 或走上述受控迁移。V1→V3 的 legacy zero-writer API 仍要求 exact V1 raw、
ModRevision、锁、空 capability range 和 create-only record，不能手工 `etcdctl put`。

本地 `:dev` tag 也不能绕过镜像身份：构建后必须从目标 minikube 节点读取实际 image config digest，写入
五个 writer 的 `pandora.dev/image-digest` Pod-template annotation；rollout 后再逐 Pod 对账 annotation 与
目标 writer container 的 `imageID`。宿主 buildx manifest-list digest、上次发布遗留 annotation 或 sidecar
imageID 都不能作为 capability provenance。`-Resume` 不重新构建镜像，只验证节点 tag 与持久 annotation
的关系：先等 etcd 并验证 V3 baseline，先 apply 当前清单并回到 Hub `replicas=1 + Recreate`，随后才以
节点现存的 immutable image config digest 显式重绑五个 Deployment；禁止在旧 RollingUpdate Hub 上先改
template。统一 rollout 后最终逐 Pod 对账。节点 tag 缺失、digest 非 canonical 或目标 writer container 的
实际 `imageID` 不一致仍 fail-closed。

### 7.9 Departure proof 有界保留与 PvE 冷恢复（2026-07-15）

`pandora:ds:departures:{match_id}` 与 `pandora:ds:teardown:{match_id}` 不是 presence，未终态时仍必须
`TTL=0`，避免 Redis 抖动或进程重启把“尚未证明离场”误判成可进 Hub；但终态 proof 也不能永久增长。
当前实现采用七天终态保留：exact teardown proof 落定后设置七天 TTL；departure journal 只有在全部
order 都进入 terminal 后才设置相同 retention。幂等重放使用 `KEEPTTL`/剩余 `PTTL`，不能把七天窗口
不断续期；历史遗留的永久 terminal proof/journal 会在下一次 exact replay 时修复为有界 TTL。仍有
pending/unknown order 时 journal 保持永久，必须由 reconciler 推进，不能用过期冒充完成。

PvE 排队态冷恢复不需要 Login 并发查询两个 matchmaker audience。PVP/PVE 实例共享同一套 canonical
player claim、start operation、ticket 与 match key；Login 使用单一受信 resolver 即可读取 PvE 的
STARTING/QUEUED/CONFIRMING/ALLOCATING/READY。若把同一全局 authority 当成两个独立来源再做“分歧”判断，
反而会制造假 split-brain。跨模式测试必须至少覆盖 PvE STARTING，以及 CONFIRMING、ALLOCATING、READY，
并验证 game-mode/member 授权仍按记录中的 canonical mode 执行。

### 7.10 Hub 离场确认后的 Battle placement 重放（2026-07-16）

本地 K8s 真 UE 复测中，两个账号分别建立了两场 `players=1` 的 PvE solo match；两场都完成 Battle
GameServer allocation 与 exact target checkpoint，却在约十二秒后被 Matchmaker 主动 abort，因此客户端
始终拿不到 Battle ticket。服务端顺序证据为：

1. `PrepareBattlePlacement` 首次写入同一 operation 的 `HUB/PENDING → BATTLE`，并 checkpoint exact
   Battle target；`EnsureHubDepartureForBattle` 暂时返回 retryable pending。
2. Hub 物理断开及 Departure ACK 完成后，locator 在同一 placement 上写入
   `source_departure_confirmed=true + HUB_DEPARTURE + proof_id`。
3. durable allocation worker 用同一 operation、match 与 target 重放 `PrepareBattlePlacement`。旧实现复用
   只允许“尚未确认、尚未绑定”的 allocation preflight 谓词，因此把合法进度误报为 `ErrLocatorConflict`。
4. Saga 将该假冲突解释成真正的 placement 竞争，调用 `AbortPreactiveBattle` 回收刚分配的 GameServer；
   两个账号因而独立复现相同的“匹配成功但进不去”。

修复必须保持两个不同的判定域，不能简单放宽 allocation 前门：

- **Allocate preflight** 继续只接受 exact stable Hub 或完全未绑定、未确认的初始 Pending；已经 checkpoint
  target 或确认离场的记录绝不能触发第二次 Allocate。
- **Prepare replay** 只有在 durable operation 已保存完整 Battle target 后，才接受同 operation/match、
  `HUB/PENDING → BATTLE`、连续 source version/operation、完整 immutable Hub source、canonical departure
  history、零 admission，以及“全空未确认”或 `true + HUB_DEPARTURE + non-empty proof` 两种确认形态。
  confirmed 状态的 active target 必须与 checkpoint target 全字段相等；partial/different target、跨
  operation/match/source lineage 或畸形 marker 仍是 `ErrLocatorConflict`。

durable worker 回归测试必须证明首次 Hub departure pending 后的下一轮：`AllocateBattle=1`、
`AbortBattleAllocation=0`、沿用原 checkpoint target，最终签票并进入 `READY/COMPLETED`。数据层测试同时
覆盖 confirmed exact retry 的正例和不同 target/operation/match/source/marker 的零写副作用负例。

同次 UE 日志还暴露了一个放大器：恢复协调器消费 exact disconnect 授权后抑制 UE 默认 `?closed → Entry/Login`
browse，但旧 OnlineSession 同时跳过 NetDriver cleanup，失败的当前 `GameNetDriver` 继续逐帧广播
`ConnectionLost`，不断使 recovery generation 失效。自定义 OnlineSession 现在只清理已授权的 exact driver：
Pending driver 先通过 owning `FWorldContext/PendingNetGame` 身份核对后调用公开的 `CancelPending(World)`；
当前 driver 只有在 `World->GetNetDriver() == NetDriver` 时才调用 `DestroyNamedNetDriver`。它仍不执行默认
Entry/Login travel，也不能销毁迟到回调时已经替换的新 driver。

截至本节记录时，服务端目标 Go 测试与 `pkg/placement` 编译已通过，UE 修复文件已编译；两个 Matchmaker
镜像正在滚动到本地集群。只有两个真实账号重新匹配并完成 Battle Admission 后，才能把本事故的本地 E2E
状态标记为通过。

### 7.11 需求固化与 2026-07-16 全链路复核

**需求（硬性，已同步 CLAUDE.md §9.19）**：玩家在进入 Hub DS 或匹配进入 Battle DS 的任意阶段切后台或
断网，回到前台后不允许出现任何卡死（无人驱动的静默等待、无入口的黑屏/遮罩），且必须能正确回到
唯一权威 DS 或显式登录态。客户端本地状态永远不改写权威路由；服务端 UNKNOWN 一律 fail-closed 零副作用。

本轮复核范围：服务端 2026-07-14 起全部提交（8ab6c59 → 1c21311）+ 未提交工作树（§7.10 修复），
UE 客户端 r1090–r1130（luhailong）+ 未提交工作树（Coordinator/OnlineSession/超时兜底面板）。结果：

1. **服务端代码级验证通过**：login/player_locator/ds_allocator/hub_allocator/matchmaker 全部
   `go build + go test` 绿（含 §7.10 的 `exactSamePreparedBattlePending` 修复及其正/负例）；
   `pkg/placement`、`pkg/battleabort` 绿。§7.10 的 Prepare replay 与 Allocate preflight 保持两个
   判定域，confirmed 离场只接受与 checkpoint target 全字段相等的重放。
2. **客户端断链所有权链路复核通过**：单写者 Coordinator 的 10 个 phase 均有 ticker/回调/前台事件
   驱动下一步；unary HTTP 超时钳制在 5–300s 且完成回调恰好一次（含 `ProcessRequest()==false` 补发）；
   OnlineSession 只清理 exact 授权 driver，杀掉逐帧 ConnectionLost 风暴且不执行默认 Entry travel。
3. **发现并修复一处前台恢复缺陷（UE，未提交工作树）**：`HandleApplicationHasEnteredForeground`
   原来无条件重启权威重查；从未登录（登录页/离线本地流程）或显式登出/放弃重连后，退避一轮会经
   `RenewSessionForRecovery` 因无凭据强制 `OpenDefaultLoginLevel`，把玩家从当前界面踢回登录关卡
   （离线本地城 → 登录页属于体验破坏）。修复：新增纯判定
   `ShouldRestartRecoveryOnForeground(session, reconnecting, cachedCredentials, fence)`，四者全空时
   前台跳过重启；`ReturnToLogin` / `AbandonBattleReconnect` 显式清 session + 缓存凭据，保证
   显式登录态不会被下一次前台恢复静默重放（对应 §7.3.1 实现约束 4 的"显式登录页"语义）。
   新增 Automation 负/正例：`Pandora.Module.Account.DsRecovery.ForegroundRestartRequiresRecoverableContext`。
4. **仍未完成（与 §7.5 声明边界一致）**：真实移动端前后台/断网矩阵、真 UDP Admission、
   Redis Cluster/K8s/Agones 故障注入尚未在生产环境验收；本节只证明代码级闭环。

## 8. 脑裂根治:DS 授权租约 fencing + 服务端再入屏障(2026-07-17)

> 目标不变量(CLAUDE.md §9.1/§9.22):**同一玩家同一时刻最多只能在一个可玩 DS**,
> 且核心时序 `旧 DS 最晚停止可玩时间 < 新 DS 最早开始可玩时间` 在网络分区下也成立。

### 8.1 根因与被封死的窗口

硬切简化后的既有防线(locator presence + matchmaker 耐久 claim + 三态 fail-closed 门 +
exact eviction)有一个共同盲区:**与后端分区、但客户端仍可达的 DS 永远不会自我停止**。
旧租约(`AdmissionLeaseSeconds` 30s)只拒新玩家、绝不影响存量连接;eviction order 又依赖
成功心跳响应送达——分区时恰好不可达。具体反例:

- **Battle→Hub**:battle DS 与后端分区,15s 判 `abandoned` → `InspectBattleRoute` 立即
  Terminal → 顶号/新设备 15s 起即可进 Hub;老设备在分区 DS 上无限期可玩 = 一人两 DS。
- **Hub→Hub**:hub 分区,30s `heartbeat_timeout` 后 `AssignHub` 改派新分片并立刻发新票;
  旧 hub 存量玩家无限期可玩。
- **Battle→新匹配**:abandon 补偿 15s 释放 match claim;locator BATTLE 30s 蒸发后
  `ensureNoneInBattle` 放行;旧 DS 仍可玩。

### 8.2 标准最简修复(定义)

经典 lease-shorter-than-failure-detector fencing,两半各一条规则,协议常量集中在
`pkg/placement`(跨仓库契约,UE 侧常量必须与之一致):

1. **DS 短租约自我 fencing(旧 DS 一半)**:DS 以最近一次「绑定 active 凭据的权威心跳
   响应」为租约起点(单调钟)。连续 `DSFenceLeaseMaxSeconds=20s` 续租失败 → 除拒新玩家外,
   **Kick 全部存量已准入玩家、销毁玩家 Pawn、拒绝 pending 准入**(一次 outage 只触发一次,
   续租成功后复位)。从未取得授权的 DS(local-off/未激活)无存量玩家可围栏,不触发。
2. **服务端再入屏障(新 DS 一半)**:任何「把静默 DS 上的玩家交给新 DS」的门,必须等该 DS
   `last_heartbeat_ms` 起至少 `DSFenceReentryBarrier = 20s + 7s(偏差余量) = 27s`。
   偏差余量构成(2026-07-18 从 5s 提到 7s,见 §8.6):响应在途上限(心跳 RPC 有界超时 4s)
   + fencing 检测粒度(1s ticker) + **服务间时钟漂移专属预留 ≥2s**(ds_allocator 写
   `last_heartbeat_ms` 与 login 读 `now()` 是两台机器的时钟;旧值 5s 被前两项恰好占满,
   漂移零预留)。不等式 `20 + 1 + 4 + 2 ≤ 27` 由 UE Automation 测试硬断言。

由此:旧 DS 最晚在 `last_response + 20s + 1s` 停止全部可玩性,而新 DS 最早在
`last_heartbeat + 27s` 才可能接纳该玩家(`last_response ≥ last_heartbeat`),时序成立。

这是 CLAUDE.md §9.22 owner 权威模型中「短 owner lease fencing + admit_not_before 屏障」
的最简落地;**每玩家 `owner_epoch` 线性一致 owner authority 仍是文档化目标**,当前由
session JTI 单写者围栏 + DSTicket v2 exact 实例绑定 + locator match fence 提供等价的
迟到写防护,§9.22 全量模型待真实需求/故障证据再升级(§15 复杂度举证)。

### 8.3 代码级落地对照

服务端(XuanMing-Server,均已提交,`go test` 全绿):

| 位置 | 改动 |
|---|---|
| `pkg/placement/placement.go` | 协议常量 `DSFenceLeaseMaxSeconds=20` / `DSFenceSkewMarginSeconds=7` / `DSFenceReentryBarrier=27s`(2026-07-18 余量 5→7,§8.6) |
| `services/account/login/internal/data/battle_ticket_authorizer.go` | `InspectBattleRoute`:`abandoned` 在 `last_heartbeat+27s` 内返回 UNKNOWN(可重试),之后才 Terminal;`ended`(DS 自报正常终局)立即 Terminal;`LastHeartbeatMs==0`(从未心跳→从未开门→无玩家)立即 Terminal。Login/GetResumeContext/IssueDSTicket(hub)/SelectRole 四门全部继承 |
| `services/battle/hub_allocator/internal/conf/conf.go` | `hub.heartbeat_timeout` 机械下限 ≥ 27s(改派新分片的等待窗) |
| `services/runtime/player_locator/internal/biz/locator.go` | locator TTL 机械下限 ≥ 27s(presence 蒸发即各再入门第一道信号) |
| 对应 `*_test.go` | 屏障内/边界/已过/从未心跳四态正负例;两处下限地板测试 |

UE 侧(Pandora-Client-SVN,待用户编译验证):

| 位置 | 改动 |
|---|---|
| `PandoraDSBackendSubsystem.h/.cpp` | 生效租约 `clamp(AdmissionLeaseSeconds,5,20)`(配置只能收紧);1s fencing watchdog 随 Start/Stop*Heartbeat 生命周期;超窗一次性广播 `OnAuthorityLeaseLost`;心跳 RPC 有界超时 4s(迟到响应不得刷新租约锚点);心跳间隔钳到 [1,5]s |
| `PandoraDSGameModeBase.h/.cpp` | Hub/Battle 共用:`FenceAllAdmittedPlayersForAuthorityLoss` = 拒 pending 准入 + Kick 全部已接纳连接(Kick 失效则直接关连接) + 销毁玩家 Pawn(含无 Controller 残留);census/departure 语义不变 |
| `PandoraDSTicketV2Test.cpp` | `AuthorityFencePolicy` Automation:不等式断言 + 纯判定四象限 + effective lease 钳制 |

### 8.4 玩家体验与 no-freeze 的一致性

被 fencing 踢出的客户端走 `UMyDsRecoveryCoordinator` 既有恢复链:断链 → 权威重查 →
屏障内各门返回可重试 Unavailable(客户端可见退避重试 UI)→ 屏障过后收敛到唯一新 DS。
等待有界(最长 ~27s + 分配耗时),每一步有 ticker/前台事件驱动,符合 §7.11 需求与
CLAUDE.md §9.19/20/23(「脑裂时安全优先但不能永久卡流程」)。正常路径零影响:
`ended` 正常结算回大厅不经过屏障,不增加任何延迟。

### 8.5 验收状态与剩余风险

- 服务端:`login`/`hub_allocator`/`player_locator`/`ds_allocator`/`matchmaker`/`pkg`
  `go test` 全绿(2026-07-17;2026-07-18 §8.6 加固后 `-count=1` 全量重跑仍全绿)。
- UE:代码与 Automation 测试已就位,**编译与真机验证由用户执行**(Live Coding 约束)。
- 剩余风险(发布前验收项):真实分区故障注入(见下方执行清单)证明 fencing 触发与
  再入时序;真实移动端前后台矩阵;§8.2 所述 owner_epoch 全量模型未实现,可达传输层的
  短暂(秒级)转移重叠窗口仍依赖 eviction order 送达,已记录为已知边界。

**分区注入验收执行清单(发布前必跑,记录进 docs/reviews/)**:

1. 准备:本地 K8s 集群跑起全链路,两个真实账号进入同一场 Battle;记录 Battle DS Pod 名。
2. 注入:对该 DS Pod 应用仅封锁「DS→后端」出站的 NetworkPolicy(保留 DS↔客户端 UDP):
   `podSelector` 精确匹配该 GameServer Pod,`egress` 只放行客户端网段,封 20006/20020/20021/20022。
3. 预期时序(全部以注入时刻 T0 起算,DS 日志 + login 日志双侧对时):
   - T0+15s 内:ds_allocator sweep 标记该局 `abandoned`(15s 心跳超时不变);
   - ≤ T0+21s:DS 日志出现「授权租约超窗…自我 fencing」,两个客户端被 Kick,Pawn 销毁;
   - T0+15s ~ T0+27s:两个客户端重登,`InspectBattleRoute` 返回 UNKNOWN,客户端可见退避
     重试 UI,**不得**拿到 Hub/新 Battle 票;
   - ≥ T0+27s:重登拿到 Hub 路由并成功 Admission;全程恰好一台可玩 DS,无双在场。
4. 变体:①注入后 20s 解除封锁(续租恢复,租约复位,玩家不被踢或已踢后正常回流);
   ②只封响应方向(半开链路,验证迟到响应不刷新锚点);③Hub DS 同法验证 Hub→Hub 改派。
5. 顶号矩阵:设备 A 在局内,设备 B 登录顶号 → A 的 GetResumeContext/IssueDSTicket/SelectRole
   全部 Unauthorized,A 收敛到登录页;B 正常重连回原局,DS 上 A 的旧连接被 PostLogin 踢除。

### 8.6 2026-07-18 加固:时钟漂移预留 / SelectRole 现行性 / 积压输入前置围栏

针对 2026-07-18 全量审计确认的三项已知边界,按「线上零已知边界」目标闭环:

1. **时钟漂移零预留 → 显式 2s 预算**。`DSFenceSkewMarginSeconds` 5→7(屏障 25s→27s):
   旧余量被「4s 响应在途 + 1s 检测粒度」恰好占满,ds_allocator 与 login 两侧时钟漂移
   没有专属预算,UE Automation 旧断言 `20+1+4 ≤ 25` 在等号处成立即无余量。新断言
   `20+1+4+2 ≤ 27`(常量 `ServerFenceReentryBarrierSeconds` / `InterServiceClockSkewReserveSeconds`
   镜像进 UE)。两个机械地板(locator TTL、hub heartbeat_timeout)符号引用自动抬到 27s,
   默认 30s 不变;负例锁定:`last_heartbeat+25s`(旧屏障值)现在必须仍返回 UNKNOWN。
   代价:abandoned 后再入等待 +2s,正常结算路径零影响。
2. **SelectRole 会话现行性缺口 → Envoy 受信 payload 头闭合(免 proto 字段)**。
   顶号后旧设备原可用未过期 session JWT 调 SelectRole 拿 hub 票(仅 Envoy 验签,无
   现行性判定)。Envoy 客户端面对 `x-pandora-jwt-payload` 入站无条件剥离、仅 jwt_authn
   验签成功后重写(header_mutation + forward_payload_header),故其中 jti 受信:
   `pmw.SessionJTIFromContext` 提取 → `LoginUsecase.RequireCurrentSessionJTI` 与
   IssueDSTicket 的请求体 token 走同一 Redis session 判定。jti 缺失(未经网关)时
   B1 严格档 fail-closed,local/off 保留放行。旧客户端零改动,无滚动兼容问题。
3. **进程挂起恢复首帧积压输入 → OnWorldTickStart 前置围栏(§9.22)**。DS 进程被
   cgroup freeze/VM 迁移挂起超过屏障后恢复,首帧 NetDriver TickDispatch 会先于 1s
   watchdog timer 分发积压输入。`FWorldDelegates::OnWorldTickStart` 在 `UWorld::Tick`
   中先于 `BroadcastTickDispatch`(引擎 LevelTick.cpp 顺序),订阅后每帧先复查租约:
   挂起恢复的第一帧在处理任何积压网络包之前完成 fencing(Kick+关连接+销毁 Pawn)。
   1s timer 保留为无 world tick 场景兜底,两者共用同一 edge-trigger 判定,租约在效时
   每帧成本为两次浮点比较。

改动清单:服务端 `pkg/placement`(常量)、`pkg/middleware`(payload 头解析,含纯函数
负例测试)、`login` biz/service(`RequireCurrentSessionJTI` 七态表驱动测试 + SelectRole
接线)、屏障边界测试字面量(27s 边界 + 25s 预留负例);UE `PandoraDSBackendSubsystem.h/.cpp`
(两常量、OnWorldTickStart 订阅/解绑/处理)、`PandoraDSTicketV2Test.cpp`(新不等式断言)、
注释全量同步 27s。验证:五服务 + 全 pkg `go test -count=1` 全绿;UE 编译与
`Pandora.Auth.DSTicketV2.AuthorityFencePolicy` 由用户执行。
