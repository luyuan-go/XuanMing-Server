# [INC-20260722-004][P0] 旧/被顶号会话 token 仍能订阅私有推送流

> **状态**：修复实施中（R4～R6 各轮窗口补修见 §4.1.1～§4.1.4；R7 复审（2026-07-23）逐条落码见 §4.1.5；R8 复审（同日）判定 R7 仍有 5 条 P0 级缺口，其中并发 Login 定序/Hub ACK TOCTOU/DS 本地缓存复核/Transfer 终检已在本轮落码（MySQL 单调代际 + Redis 条件写、ACK 后置复核+回滚、UE 两段到期+PostLogin 幂等重验、Transfer 前后双检）；空 sjti 硬拒改为 login/hub 两处分阶段门（默认兼容档，发布顺序见 docs/design/session-generation-rollout.md），见 §4.1.6；Go 侧相关服务测试绿；UE 侧改动待用户编译；真实 Envoy/共享 Redis/多 Pod/双设备/race/混版矩阵验收未跑，**未关闭**）
> **类型**：`security` / `session-fencing` / `near-miss`
> **环境**：本机源码与单测审计（未确认在线上发生）
> **首次发生时间（UTC）**：未在线上确认；受影响行为自 push Subscribe 上线（W3 ④）即存在
> **首次发现时间（UTC）**：2026-07-22（外部只读审计 R4 轮）
> **负责人**：修复由 Claude 实施，项目负责人待指定
> **受影响服务/版本**：`services/runtime/push`（Subscribe 建流路径）、`deploy/envoy/envoy.yaml` push 路由、`tools/scripts/gen_cluster_config.ps1` 生产产物
> **最后更新**：2026-07-22

## 0. 一句话结论

push `Subscribe` 只依赖 Envoy jwt_authn 验签取 `player_id`，不校验会话现行性（jti）：被顶号/已重登的**旧 token 在 exp 前仍能重建订阅流收取该玩家全部私有推送**（邮件、好友、战斗结果、经验入账等），且已建立的流在 JWT 过期后**无限期存活**（server stream 永不重验）。按项目"安全突破"口径属 P0（CLAUDE.md §9.23 会话 fencing：旧 session 不得再获得任何按 player_id 定向的服务端能力）。

## 1. 影响与范围

- 玩家影响：账号被盗后即使受害者重新登录顶号，攻击者持有的旧 token 仍能持续窃听该账号的私有推送（含资产、社交、对局数据）；同时旧设备建流会触发顶号语义，可反复顶掉新设备的合法连接（骚扰向拒绝服务）。
- 影响人数/请求数：线上是否被利用未知；本次为上线前静态审计确认，代码路径自 push 真实化起存在。
- 服务影响：`services/runtime/push` `service/push.go Subscribe` 全路径。
- 数据与安全影响：私有推送越权可读 = 会话 fencing 突破；不涉及写路径（push 只读投递）。
- 是否仍可复发：代码修复已落地但未经真实 Envoy + Redis 环境验收，视为可复发。
- 严重级别判定理由：破坏 §9.23 "旧 session 不得再签 DS 票/获得会话能力" 的安全不变量，越权面为全部定向推送内容。

## 2. 第一现场与证据

### 2.1 症状

- 无线上样本；审计静态证据如下。

### 2.2 原始证据（修复前基线）

```text
services/runtime/push/internal/service/push.go(修复前)
  Subscribe 仅 pmw.PlayerIDFromContext(ctx) 取 player_id 即 Register + RunSubscribeStream,
  无任何 jti 现行性校验;login 侧 pandora:sess:<pid> 的 jti 权威(2026-07-15 P0 修复引入)
  未被 push 消费。

deploy/envoy/envoy.yaml push 路由(修复前)
  timeout: 0s / idle_timeout: 0s 且无 max_stream_duration:流一旦建立永不重验 JWT,
  token 到期/被顶号后流无限期存活。

攻击路径:旧 token(exp 前)→ Envoy jwt_authn 验签通过 → Subscribe 建流成功
  → 顶掉新设备 slot(Register 顶号语义)→ 持续收取私有推送。
```

### 2.3 已排除的噪声

- Envoy `x-pandora-jwt-payload` 头由 jwt_authn 验签后重写、入站无条件剥离，客户端无法伪造 jti——头信道本身可信，缺的是"现行性"校验而不是"真实性"校验。
- 其它 unary 服务走 pmw 中间件链已有会话校验路径；本缺陷特定于 server stream 不跑 unary 中间件链。

## 3. 根因

1. **建流门缺失**：JWT 验签只证明"曾经登录过"，不证明"未被顶号"。login 已维护 `pandora:sess:<player_id>` hash `jti` 作为会话现行性权威（2026-07-15 INC 修复），但 push 从未消费该权威。
2. **流内复查缺失**：server stream 只在建流时过一次 Envoy 验签；顶号、登出、token 到期都不会中断已建立的流。跨 Pod 场景下新建流发生在另一副本，旧副本上的旧流无人踢。
3. **放大因素**：push 路由 `timeout: 0` 且无 `max_stream_duration`，流寿命无上界；Register 顶号语义让旧 token 还能主动断新设备连接。

## 4. 修复

### 4.1 代码修复（已落地，2026-07-22）

- `pkg/middleware/auth.go`：新增 `SessionClaimsFromContext` / `ParseJWTPayloadClaims`，从 Envoy 验签后重写的 `x-pandora-jwt-payload` 头解析 `jti` + `exp`（毫秒）。
- `services/runtime/push/internal/data/session_gate.go`（新增）：`RedisSessionGate` 只读 login 会话权威 `pandora:sess:<pid>` hash `jti`（key 格式为跨服务契约，与 `login internal/data/account.go` 同步维护）；Redis 错误返回 `ErrUnavailable`，调用方 fail-closed。
- `services/runtime/push/internal/biz/push.go`：
  - `AuthorizeSubscribe`（建流门，Register **之前**执行——旧 token 不再有机会顶掉新设备连接）：jti ≠ 权威当前一代 → 拒；无会话 → 拒；权威不可达 → fail-closed 拒；`require_session_gate: true` 档下缺 jti（绕网关）→ 拒。
  - `recheckSession`（流内 30s 周期复查）：jti 被轮换（顶号，含跨 Pod 旧流）/ 登出 / `exp` 已过 → 关流；权威查询失败按可重试计连败，连续 3 次 → fail-closed 关流（短抖动不误杀，持续故障不裸奔）。
- `services/runtime/push/internal/conf/conf.go` + `etc/push-dev.yaml`：`require_session_gate` 开关，dev 缺省 false（§14.2 直连联调不变；有 jti 仍校验），**生产由生成器机械置 true**。
- `tools/scripts/gen_cluster_config.ps1`：`Set-ProdPushSessionGateOn`，`-Prod` 产物强制 `require_session_gate: true`，模板锚点异常拒绝生成。
- `deploy/envoy/envoy.yaml`：push 路由加 `max_stream_duration: 3600s`（独立兜底层：流寿命有界，到期强制带新 token 重建再过建流门）。

### 4.1.1 R4 复审补修（2026-07-22，同日）

首轮修复存在两处仍可达窗口，复审确认后补修：

1. **建流校验/注册 TOCTOU**：`AuthorizeSubscribe` 与 `Register` 分离执行——旧会话校验
   通过后暂停、新会话完成校验+注册、旧会话恢复再注册，会反过来取消新设备连接并接管
   槽位（“旧 token 不再能顶掉新设备”不成立）。补修：`AuthorizeAndRegister` 在同玩家
   64 条带锁内串行执行「校验 → 注册」；任一交错都收敛到新会话持有连接（旧会话校验排
   在新会话注册后必读到已轮换 jti 被拒；排在前则旧流短暂注册后立即被新注册顶掉）。
   回归：`TestAuthorizeAndRegister_StaleSessionCannotDisplaceNewer`（用可阻塞 gate 确定
   性复现原交错）。**边界修正（R5 复审）**：条带锁是进程内互斥，只消除**单 Pod 内**的
   交错；跨 Pod 场景（旧流注册在 A Pod、新登录轮换经 B Pod）此锁不可达，由 4.1.3 第 3
   条的投递前 fencing 收口（旧流零轮换后私有帧），不依赖跨 Pod 共享连接槽。
2. **“30 秒内关闭旧流”无时限保证**：流内复查与写者共用同一 select，写者阻塞在
   `stream.Send`（慢客户端流控）或首轮 replay 期间，复查永远轮不到；实际最坏收敛由
   gRPC `max_connection_age`(15m)+grace 决定。补修：会话复查改为**独立看门狗 goroutine**
   （不受写者阻塞影响），失效后 ≤30s 取消流上下文；写者每次 Send 前检查取消，不再投
   递任何新帧，并以映射后的 gRPC 状态（顶号 ABORTED / 过期登出 UNAUTHENTICATED /
   权威故障 UNAVAILABLE，见 `pkg/errcode/grpc.go`）关流。**诚实契约**：30s 界定的是「停止投递 + 发起关流」；
   写者已阻塞在传输层的至多一帧、以及 TCP/流句柄的物理回收，仍由 keepalive/
   max_connection_age/Envoy max_stream_duration 有界收敛，不是 ≤30s。
   回归：`TestRunSubscribeStream_WatchdogClosesBlockedWriter`（写者阻塞在 Send 时顶号，
   断言解除阻塞后零新帧投递并以 ErrSessionSuperseded 关流）。

### 4.1.2 R4 二轮复审补修：顶号/过期可判别，双设备稳定赢家（2026-07-22，同日）

首轮+补修后仍存在一个 P0 行为环：push 关流与 login 会话门把「自然过期、登出、被新设备
顶号」全部映射为 `ErrUnauthorized` → gRPC UNAUTHENTICATED；UE 客户端对 UNAUTHENTICATED
统一走 `RenewSessionForRecovery`（用缓存凭据自动完整 Login），Login 必然轮换 jti，于是
被顶设备自动反顶新设备，两台设备互踢无限循环，没有稳定赢家。

判别契约（本次落地）：

- `pkg/errcode`：新增 `ErrSessionSuperseded = 14`（proto `ERR_SESSION_SUPERSEDED` 数值
  1:1 对齐；生成代码由 buf 重新生成）。`IsRetryable(14) == false`。
- `pkg/errcode/grpc.go`：`ErrSessionSuperseded → codes.Aborted`。选 ABORTED 而非
  UNAUTHENTICATED 变体：Envoy jwt_authn 对自然过期同样产出 UNAUTHENTICATED，只有独立
  顶层状态才能让客户端在流 trailer 上判别；ABORTED 在本项目其余路径未使用，语义唯一。
- 产生点：push `AuthorizeSubscribe` / `recheckSession` 的 jti 已轮换分支，以及 login
  `requireCurrentSession` 的 `cur != jti` 分支，改返 `ErrSessionSuperseded`；
  `jti == ""`（证据缺失）与过期/登出维持 `ErrUnauthorized`（允许客户端自动换新）。
- UE 客户端（`MyDsRecoveryCoordinator` / `MyAccountModel`）：收到业务码 14 或流关闭
  gRPC ABORTED(10) 时调用 `HandleSessionSupersededByOtherLogin`——停战斗重连、清缓存
  凭据、回登录关卡走**交互登录**，绝不自动完整 Login；UNAUTHENTICATED(16)/码 8 才保留
  自动换新。由此顶号收敛为：最后登录的设备是唯一稳定赢家。

### 4.1.3 R5 复审补修：全服务吊销 + 跨 Pod 投递 fencing + 交付终检（2026-07-22，同日）

R5 只读复审(基线 d5b2d2b7c4 / 客户端 r1306)确认 4.1.1「同玩家条带锁」只在**单进程**成立
(原文档措辞已按此纠正),并发现四条新的 P0 级窗口,同日全部落码:

1. **旧 JTI 未全局吊销(R5 P0-1)**：会话门只封了 login/push 入口,顶号后旧 JWT 在 exp 前
   (默认 24h)仍能执行 friend/trade/inventory 等全部按 player_id 定向的 unary RPC。修复:
   push 的 RedisSessionGate 提升为共享 `pkg/sessiongate`,新增 `pkg/middleware.SessionCurrent`
   按请求校验 payload jti == 会话权威当前一代(顶号 ABORTED/14、登出过期 UNAUTHENTICATED、
   权威不可达 fail-closed UNAVAILABLE;无 payload 头 = 内部面放行),接线全部 12 个客户端面
   服务(friend/chat/mail/guild+group/trade/team/matchmaker×2/player/inventory/leaderboard/
   hub-allocator 玩家 method + push unary 面);`gen_cluster_config.ps1` `-Prod` 机械置
   `session_gate.require: true` + 产物断言 + 契约测试(`gen_cluster_session_gate_contract_test.ps1` PASS)。
2. **过期旧设备自动反顶(R5 P0-2)**：`recheckSession` 先判 `exp` 后查 jti——「已过期且已被
   顶号」得到 UNAUTHENTICATED,UE 按自然过期自动完整 Login 反顶新设备,判别契约被到期分支
   短路。修复:先判会话代际(mismatch 无条件 ABORTED/14)后判到期;回归
   `TestRecheckSession_SupersededTakesPrecedenceOverExpiry`。
3. **跨 Pod 建流/投递 TOCTOU(R5 P0-4)**：`authRegMu` 是进程内锁,滚动/多副本下旧会话可在
   Pod A 读到旧 jti 后暂停、新会话在 Pod B 轮换并建流、Pod A 恢复注册并立即补推私有缓存
   (最长 30s)。修复:`drainBuffer` 投递前复核会话现行性(`sessionFenceDelivery`)——依据
   Redis session key 单点串行,任何「轮换后产生」的帧只能被轮换后的 Range 读到,其后的
   复核必失败 → **轮换后产生的帧零交付**,单/多 Pod、Cluster 均成立;权威不可达
   fail-closed 不投递(退避,游标不动不漏)。回归:
   `TestCrossPodStaleStream_ZeroFramesAfterRotation`(双 usecase 模拟双 Pod)、
   `TestSteadyStreamRotationWakeup_FencedBeforeSend`、`TestDeliveryFence_AuthorityDownFailClosedThenRecover`。
   **表述修正(R6 复审)**:R5 落码为**每批一次** fence,fence 通过后同批(≤512 帧)仍会
   发完——"轮换后旧流零私有帧"在批内交错下不成立;R6 已收紧为**逐帧** fence(§4.1.4),
   诚实上界 = 每条交付帧都产生于该帧自己的 fence 通过之前,在途暴露 ≤1 帧
   (「轮换瞬间起零帧」跨存储不可达,本档案不作此宣称)。
4. **login 副作用无终检(R5 P0-5)**：完整 Login 在写入 jti 后继续分配/locator/签票直至返回,
   期间不复核;SelectRole/IssueDSTicket 为「检查→稍后写/签」。修复:`fenceLoginDelivery`
   在交付前复核本流程写入的 jti 仍是当前一代(battle 重连与最终返回两处),失败扣留全部
   凭据;SelectRole/IssueDSTicket 三分支在副作用后、返回前二次过门(票据从未离开服务端 =
   未取得)。诚实边界:跨 Redis/MySQL 无原子事务,角色行等业务写在复核后仍有 ms 级窗口,
   残余为可被新会话覆盖的一次写,不构成进场能力。回归:`login_delivery_fence_test.go` 四例。
5. **UE 侧会话代次绑定(R5 P0-3)**：推送流建立时快照 Backend SessionGeneration;登录成功在
   会话提交点**无条件换代重订阅**(旧流迟到 ABORTED/帧按 generation 丢弃,S1 迟到回调对
   S2 零副作用);Coordinator 关流处置前校验关闭归属代次,登出态不再安排重订阅;
   `HandleSessionSupersededByOtherLogin` 补幂等 guard。待用户 UE 编译验证。

### 4.1.4 R6 复审补修：三条仍可达 P0 路径（2026-07-23）

R6 只读复审确认 R5 修复后仍有三条可达路径("P0 已全部完成"的结论不成立,本节修复后
**仍不宣称闭环**——真实环境验收未跑):

1. **Envoy 层过期拒绝使"先判代际"不可达**:token 过期的请求在 Envoy jwt_authn 即被
   UNAUTHENTICATED 拒绝,§4.1.3 第 2 条的应用层重排(先判代际后判到期)对离线到期设备
   根本不执行;UE 收 UNAUTHENTICATED 仍自动完整 Login 反顶当前设备(A 离线至过期 → B
   登录 → A 恢复 → 自动 Login 轮换掉 B)。修复(客户端,UE):`RenewSessionForRecovery`
   在自动重放前用本地解析的 token exp(留 5min 时钟偏差余量,方向保守)判定——
   **已过期/临近过期/不可解析 = 无法证明未被顶号 → 走与顶号相同的清理链转交互登录**,
   绝不自动重放;只有 token 确定未过期(此时 UNAUTHENTICATED 必然来自应用层登出语义,
   顶号已由 ABORTED 单独判别)才允许自动换新。行为变更:离线超过 session 寿命(24h)后
   回前台一律要求手动登录(标准行为,不再静默重放缓存凭据)。
2. **投递 fence 批内竞态**:见 §4.1.3 第 3 条表述修正——每批 fence 改逐帧 fence。
   回归:`TestDrainFence_RotationMidBatchStopsRemainingFrames`(fence 通过 1 帧后轮换,
   断言只交付该帧、余帧被拦、以 ABORTED 关流)、`TestDrainFence_RotationBeforeBatchDeliversNothing`。
3. **SelectRole 落库后才终检 + 票据不绑会话**:
   - 角色写 fencing:`PlayerRoleRepo.SetRole` 增加事务内 precommit 钩子——UPSERT 后、
     COMMIT 前复核请求方 jti 仍是当前一代,失败 ROLLBACK,**被顶旧会话的角色写不再落地**
     (此前是提交后才在 service 层终检)。残余:precommit(Redis 读)与 COMMIT 之间的
     进程内窗口,跨存储无统一事务域不可消除,但已从"检查前提交"收敛为"检查通过才提交"。
     回归:`TestSelectRole_RolePersistFencedOnMidFlightRotation`。
   - 票据会话绑定:DSTicket v2 claims 新增 `sjti`(签发请求方会话 jti)。签发链:
     login(Login/SelectRole/IssueDSTicket 三入口)→ resolveHub/battle v2 路径 →
     hub_allocator `AssignHubRequest.session_jti`(proto +1 字段,go pb 已本地重生)。
     **兑换点复核**:login `VerifyDSTicket`(DsAuthorityMode=redis 生产档的 DS 在线核销
     权威路径)对非空 sjti 复核会话权威当前一代,不匹配 → ABORTED/14——**签发与响应
     写出之间被轮换的旧票,即使已交付到旧设备,在兑换点作废**(不再依赖"响应扣留"
     单层防线)。回归:`TestRequireTicketSessionCurrent`、`TestDSTicketV2_SessionJTIRoundtrip`。
   - **兼容窗(诚实边界,不得当"已验")**:sjti 为空的票不参与判定——覆盖 matchmaker
     READY 批签/换设备重签(经会话 fencing 的推送流交付,由该通道自身的 fencing 保护)、
     hub_allocator Transfer/迁移重签(既有归属权威门)、滚动升级期旧票、dev 直连、
     legacy HS256 票(RS256 生产档已禁用)。B1 纯本地验票模式(非 redis authority)下
     DS 不调 VerifyDSTicket,兑换点复核不生效,由票据短 TTL 兜底(v2 RS256 默认 120s/
     上限 180s;legacy HS256 为 ds_ticket_ttl,默认 5min)——该取舍是 B1
     拍板时接受的吊销时延,本次不改变。**[R7 改硬拒 → R8 改分阶段门]**:R7 把空
     sjti 改为无条件硬拒;R8 复审指出混版滚动窗口内旧签发面**持续**签空票(不是
     只有存量票),硬拒会令战斗准入整体不可用,已改为 login.require_ticket_sjti /
     session_gate.require_ticket_sjti 分阶段门(默认兼容档告警放行,排空+等满票据
     最大 TTL 后收口),见 §4.1.6 与 docs/design/session-generation-rollout.md。

### 4.1.5 R7 复审收口（2026-07-23，同日；R8 复审推翻其中部分结论，修正见 §4.1.6）

R7 只读复审判定 R6 后仍有 4 条 P0、2 条 P1、1 条 P2 未真正修好，本轮逐条落码。
⚠️ 本节是 R7 当时的实施记录，**不是逐条关闭结论**：R8 复审判定其中“空 sjti 硬拒”在
混版滚动窗口下不可上线（已改分阶段门），且另有 5 条 P0 级缺口，见 §4.1.6。

1. **P0-1 客户端自动恢复仍可反顶（方案 A，UE）**：R6 的本地 exp 守卫只拦"已过期"，
   token 未过期窗口内自动完整 Login 仍会轮换 jti 反顶新设备。收口：
   `MyAccountModel::RenewSessionForRecovery` 删自动完整 Login 换新 + 删 R6 本地时钟
   exp 守卫（`ExtractJwtExpiryMs` 已移除），**会话失效一律转交互登录**（废恢复
   operation、停重连 UI、回登录关卡）；`bRecoverySessionRenewalInFlight` 标志及
   HandleLoginComplete 换新分支一并删除。行为变更：任何 session 失效（过期/登出/
   被顶）后都需玩家手动重新登录，自动反顶窗口彻底消除。
2. **P0-2 matchmaker READY 批签票无 sjti**：`GrpcDSAllocator.signBattleTicket` 注入
   `sessiongate.Gate` 逐玩家读当前 jti 签进 v2 claims（权威不可达 fail-closed
   UNAVAILABLE，无会话 UNAUTHORIZED）；main.go 装配。回归：
   `TestSignBattleTicketBindsCurrentSessionJTI`（签入当前 jti/轮换后签新 jti/权威故障
   拒签/无会话拒签，JWT payload 解码断言）。
3. **P0-3 DSTicket 会话绑定未闭环**：三处收口——
   - hub_allocator Transfer/迁移重签补 sjti：Transfer 用调用方 ctx jti（被顶调用方拿到
     的是兑不了的旧 jti 票），迁移重签读会话权威当前 jti（不可达则本 tick 不重签
     下 tick 重试）；
   - login `RequireTicketSessionCurrent` 空 sjti 改为**硬拒**（签发链已补齐，空票不再是
     兼容窗），并前置到 replay marker 写入之前（P2-1，双检：marker 前 + 响应写出前）
     ——**[R8 修正]** 混版滚动窗口内旧签发面持续签空票，硬拒不可上线；已改为
     `login.require_ticket_sjti` 分阶段门（默认 false 兼容档告警放行），见 §4.1.6 P0-5；
   - **Hub Admission ACK 会话复核**：`AcknowledgeAdmissionRequest` 新增 `session_jti=9`
     （proto go/cpp 已重生，UE 两处生成物已同步），DS 把票据 sjti 透传 ACK，
     hub_allocator 在任何副作用前对会话权威复核：空 sjti 拒（UNAUTHORIZED）、不匹配拒
     （ABORTED/14）、权威不可达 fail-closed（UNAVAILABLE，DS 重试）——v2 Hub 本地验票
     不调 VerifyDSTicket 的绕过路径自此有兑换点复核。UE 侧：PandoraDSTicket 解析
     `sjti` claim，PandoraHubGameMode PostLogin 存入 FHubAdmissionState，ACK 携带。
     回归：`TestModelB_AcknowledgeAdmissionSessionGate`（空票拒/旧 jti 拒且帧不消耗/
     权威故障 UNAVAILABLE/无会话拒/匹配准入）。
4. **P0-4 角色写跨存储竞态**：Redis precommit 与 MySQL COMMIT 间的窗口用**同事务域**
   fencing 关闭：Login 在 Redis 会话写入后同步写 `player_session_generations`（MySQL，
   fail-closed，失败不交付凭据）；`PlayerRoleRepo.SetRole` 在同一事务内 UPSERT 后
   `SELECT sess_jti ... FOR UPDATE` 比对调用方 jti，不匹配 ROLLBACK——行锁与新 Login 的
   代际写串行化，Redis 投影落后也确定性拒提交。建表：deploy/mysql-init/
   02-account-tables.sql，启动 `mysqlx.CheckTables` fail-fast。回归：
   `TestSelectRole_MySQLGenerationFencesEvenWhenRedisStale`。
5. **P1-1 push 断层永久漏**：`drainBuffer` 改为每页先查 `LostSince(baseline)`，检出断层
   **先发 resync 信号帧再发数据帧**（此前帧先于信号，客户端按帧推进游标后永久错过
   被修剪区间）；LostSince 失败 fail-closed 关流。回归：
   `TestRunSubscribeStream_GapSignalsResyncAfterReplay`、
   `TestRunSubscribeStream_GapInterleavedWithSurvivorStillSignals`（断言 resync 帧在前）、
   `TestRunSubscribeStream_GapCheckFailClosed`。
6. **P1-2 业务码 14 客户端非终态（UE）**：`HandleLoginComplete` 失败路径对
   `code==14 || grpc==ABORTED(10)` 立即废恢复 operation 并走顶号统一清理链
   （`HandleSessionSupersededByOtherLogin`，幂等），不再进退避重试循环。
7. **P2 marker 顺序 + 确定性交错测试**：见上述各条新增回归；VerifyDSTicket 会话门双检
   （marker 前 + 响应写出前终检）。

Go 侧 login/matchmaker/hub_allocator/push 全套测试绿；UE 侧改动（sjti 解析/ACK 携带/
恢复策略）待用户编译。部署耦合（R8 修正）：空 sjti 不再要求同步部署硬切——分阶段门
默认兼容档下旧票/旧签发面告警放行，排空后等满票据最大 TTL（v2 上限 180s；legacy
5min；混用取 5min）再开 require 收口；存量库需先跑 pandora_account 000003 迁移（含
generation 列，启动期 CheckTables+CheckColumns fail-fast）。发布顺序权威：
docs/design/session-generation-rollout.md。

### 4.1.6 R8 复审收口（2026-07-23，同日）

R8 只读复审判定 R7 后仍有 5 条上线阻断级 P0 及多项 P1/P2。逐条处置：

**P0（全部落码，测试绿）：**

1. **并发 Login 把 MySQL 代际回写成旧 jti**：R7 首版 `PersistSessionJTI` 是无条件
   覆盖 upsert，并发登录 A/B 交错时迟到方把 MySQL 回写成旧 jti（Redis=B、MySQL=A
   撕裂）。收口：`player_session_generations` 增单调 `generation` 列（迁移
   000003），Login **MySQL-first** 原子分配代际（fail-closed），再对 Redis 做
   「仅更高代际可覆盖」条件写——任意交错下两存储收敛到最高代际，输掉定序的登录
   直接失败不交付凭据。回归：`r7_login_generation_test.go`。
2. **Hub Admission ACK 会话 TOCTOU**：R7 的 ACK 复核是"检查→消费 reservation"，
   两步间会话可被轮换。收口：`AcknowledgeAdmission` 前置复核之外，
   `authRepo.AcknowledgeAdmission` 耐久写**之后**重读会话权威，不匹配/消失/不可达
   即调 `AcknowledgeDeparture`（同身份幂等）回滚 connected owner 再拒绝
   （检查→变更→复核→回滚）。回归：`hub_modelb_test.go` 交错用例。
3. **Battle DS 缓存 claims 到 InitNewPlayer 只消费本地缓存**（UE）：verify 成功到
   spawn 之间被轮换的旧会话仍可入场。收口：准入缓存双重到期（票据 exp + 有界消费
   窗）；InitNewPlayer 强制 admission claims 匹配消费；PostLogin 后以同
   (ticket, admission_id) 幂等重调 VerifyDSTicket 复核（PostSpawnSessionRecheck），
   失败踢下线。
4. **旧会话 TransferToLine 无终检**：`transferToLineInner` 在 TransferHub 前后各加
   `requireCallerSessionCurrent`（ctx jti 对会话权威复核，超越即 14/ABORTED）。
   回归：`hub_test.go` TransferToLine 终检用例。
5. **滚动发布不安全**：R7 的空 sjti 无条件硬拒/强制代际复核在混版窗口会误拒合法
   流量（旧 Login Pod 不写代际、旧签发面持续签空票、旧 DS 不发 session_jti）。
   收口：三个独立分阶段门——`login.session_generation_enforce`、
   `login.require_ticket_sjti`（本轮新增）、hub `session_gate.require_ticket_sjti`，
   全部默认 false=兼容档（新机制只写不强制），按「迁移 → 全 fleet emit/双写 →
   排空旧版本+等满票据最大 TTL → 开 require」顺序激活；发布顺序权威文档
   docs/design/session-generation-rollout.md（含 hub-allocator Recreate 单写者
   取舍的记录在案）。回归：兼容档/收口档双档测试（login + hub）。

**P1（落码）：**

- 存量迁移+dbcheck：pandora_account 000003（generation 列）+ login 启动期
  `CheckColumns`（新增 `mysqlx.CheckColumns`，识别"表存在但缺列"的半旧 schema）；
  pandora_social 000006（friend 守卫行表，此前只在 fresh-init）+ friend 启动期
  `CheckTables`。
- push 坏 member 自愈失败永久漏报：`markCorrupt` 的 Lua 原子折账失败时**扣发**坏帧
  游标之上的所有帧（宁可重推绝不静默跳过），游标不越过未折账损失。
- match.go 重签失败降级返回旧票：`refreshBattleTicket` 签发失败改 fail-closed
  返回 `ErrUnavailable`，不再回退旧票。
- hub 迁移部分成功丢通知：玩家在迁移通知送达前保留在源 member 索引（drain 扫描
  唯一来源），CAS 落地但 cleanup/通知未完成的所有失败路径重新加回源索引，下个
  tick 补发。回归：`hub_modelb_test.go` 迁移通知用例。

**P2（落码）：**

- Logout MySQL 代际墓碑：登出成功后条件 CAS（仅行内仍是本 jti）推进代际改写哨兵
  值，不毒化并发新登录；best-effort（Redis 删除是主权威）。
- `mysqlx.CheckColumns` 列级 schema 检查（见上）。
- push.proto 注释与实现对齐：每页拉取前 fl 断层检测、resync 信号先于幸存帧、
  逐帧（非每批）会话 fence、坏 member 折账语义；客户端 resync 契约（无 ACK 设计
  的理由：游标不因信号帧前进，信号丢失由重连再检出兜底）。

**按原样记录、未在本轮改变的取舍（诚实边界）：**

- 签票契约未在签发器结构性锁死：结构性拒绝点放在兑换点（login/hub 两个 require
  门），签发器无法区分 dev 无权威与漏传；migrate 对已登出玩家签空 sjti 票在
  require 档兑换点必拒、无害（rollout doc §4）。
- ticket 会话检查与 replay marker 非原子：会话门已前置到 marker 之前 + 响应写出前
  终检，两检之间进程内窗口无跨存储事务域可消除（§4.1.4 诚实边界）。
- resync 业务域覆盖：客户端当前消费推送的域（team/match/friend/DS recovery）已全
  部接回源；mail 是纯拉取（打开界面即回源），chat/guild 客户端推送消费模块尚未
  实现——实现时必须按 push.proto 契约接 resync（已写进 proto 注释）。
- 事故档案 §4.1.5 的"逐条关闭"表述已修正为实施记录；验收矩阵仍含未跑项，事故
  **未关闭**。

**部署警示**：本轮涉及的服务端实现文件多数尚未提交版本库（git/svn），上线前须
逐一核对提交清单；`require_*` 三开关首次上线必须保持 false。

### 4.1.7 R9 复审收口（2026-07-23，同日）

R9 只读复审（HEAD `4b5f9adb`）判定 7 条 P0 阻断面，结论"没有修完，P0 不能关闭，
当前不应上线"。逐条处置：

**P0：**

1. **fencing 默认未启用**（已落码）：deploy 模板 `session_generation_enforce` /
   `require_ticket_sjti` 置 true（生产口径硬拒），login/hub 启动期对开关组合
   fail-fast（enforce 开而 require 关等非法组合直接拒启）；rollout doc §1 记录
   「代码默认 false 仅为混版过渡档，模板即生产默认」。
2. **MySQL-first 撕裂会话权威**（已落码）：login 代际分配回归 MySQL 单权威定序，
   Redis 仅作「更高代际才覆盖」条件投影；`r7_login_generation_test.go` 扩展交错
   用例。
3. **混版流程漏算 24h Session 生命周期**（文档修正）：rollout doc §2 拆分
   「票据 TTL 窗口（v2 180s / legacy 5min）」与「session 24h 生命周期窗口」两个
   独立等待面；阶段 D 前置改为「最后一个旧版 login Pod 终止时刻 + 24h」或主动
   收敛。同时修正原文错误结论：emit-only 档 SetRole 传空 sjti 时**不执行** MySQL
   代际比对，没有可观测 mismatch 告警，不能以「无告警」判定窗口已满。
4. **Hub Admission spawn gate 打开后的终态竞态**（UE 落码，待用户编译）：
   `PandoraHubGameMode` 在 spawn gate 开放 + locator 写回后，以同
   (admission_id, seq, sjti) 幂等重发一次 ACK；服务端 AlreadyAdmitted 路径重跑
   前置+后置会话复核。定性失效（14/8/InvalidState 等非瞬态码）→ FailAdmission
   清退；结果未知 → 有界重试（共 3 次，ABA 门校验连接代际，可取消）；耗尽仍未
   定性 → fail-closed 清退。
5. **Battle spawn 后复核 fail-open**（UE 落码，待用户编译）：
   `PandoraDSGameModeBase` 的 PostSpawnSessionRecheck 从"一次性 best-effort、
   未知结果放行"改为在途状态机：结果未知/DS 凭据缺失一律按未知处理，2s 间隔
   有界重试（共 3 次，同 (ticket, admission_id) 幂等、不消耗防重放名额），耗尽
   仍未确证 → fail-closed Kick + 销毁 Pawn；Logout/EndPlay 全量取消复核定时器；
   非会话失效语义的定性拒绝仍不误杀（仅留痕）。
6. **TransferToLine 路由副作用不补偿**（已落码）：终检失败不再遗留半程路由副作用，
   失败路径补偿/回滚后再拒绝；`hub_test.go` 用例扩展。
7. **hub-allocator 单副本 Recreate 违反不停服红线**：**未解决，保持 OPEN**。
   rollout doc §5 重写为公开冲突记录：dsauthfence V3 单写者约束
   （`TestV3ActivationRequiresSingleHubWriter`）与不停服红线当前不可同时满足；
   附 succession-lease + 单调 fencing token 的继任协议设计草案；明令禁止在继任
   协议实现前单独把 strategy 改回 RollingUpdate（那会引入双写者，比停服窗口更糟）。

**P1/P2：**

- hub ACK postcheck 结果分型：权威不可达=未知，不回退 connected owner（返回
  Unavailable，DS 同 identity 重试重跑复核）；确定性否定才 exact 回退
  （AcknowledgeDeparture 幂等）。
- friend 热路径读加 FOR UPDATE（旧快照穿透）；`friend_pair_guards` 增 created_at
  + 保留期清扫（迁移 000006 扩展 + mysql-init/tidb-init 镜像同步 + dbcheck）。
- push resync 客户端回源失败无重试：Team/Friend 模型加脏标记 + 有界重试
  （3 次、2s、会话切换清理）；Match 依赖既有进度轮询，代码注释记录豁免理由。
- cursor=0 首连跳过 LostSince：判定为**有前提的交付契约**而非漏洞——依赖客户端
  「先订阅 push 再拉业务快照」时序（MyAccountModel 唯一订阅点，同步无事件泵）；
  已在 push.proto、drainBuffer、回归测试注释三处写死该前提。
- `mysqlx.CheckColumnSpecs` 新增：校验列类型/可空性/键形状（不止列名存在性），
  login 启动期接入；Kafka migrate 发布失败补偿；tools/migrate 测试修绿；
  push.proto resync 注释矛盾修正（每页发送前预检为主防线 + 拉空后终检兜底，
  proto 重生成仅 Go 侧变化，C++ 生成物与 UE 内副本逐字节一致无需同步）。

**诚实边界（R9 轮）：**

- P0-7 未解决，是当前唯一 OPEN 的 P0；关闭需要实现 hub-allocator 继任协议。
  > **2026-07-24 补注（后续只读复审 snapshot `86b15dbc` 超越本结论，勿据 R9 判定对外声称"只剩一个 P0"）**：
  > 上一行"唯一 OPEN 的 P0"是 R9 轮（07-23）判定，后续复审重新判定多条 P0 仍开放 / 修复不足——
  > login Redis 模糊提交仍撕裂双会话权威（原 P0-2）、writerlease 长期无主时所有 Pod 仍 Ready 且写永久
  > Unavailable、健康 writer 存在时请求仍可能钉在非 writer Pod（连接复用）、assignment CAS 与出票无持久
  > fencing token（旧 writer 失租通知前仍能签可兑换票）、首次 writerlease 升级仍无仓库内"不停服 + 单写"
  > 闭环（原 P0-7）、Hub/Battle 仍先 Spawn 后复核（原 P0-4/5 残余窗口）。因此本事故涉及的 P0 面不能以
  > "只剩一个 P0"表述；这些属于 owner authority / 发布策略级设计项，需专项设计后再落，不能凭静态复审急改。
- UE 侧五个文件（MyTeamModel / MyFriendModel / MyMatchModel /
  PandoraDSGameModeBase / PandoraHubGameMode）仅通过静态诊断，编译由用户执行，
  本轮无编译证据。
- 真实并发/混版矩阵/故障注入仍未跑；验收矩阵未跑项未变。事故**未关闭**，待 R10。

### 4.1.8 R12 Login 跨存储补偿更正（2026-07-26，未关闭）

R11 的 `sessions.Set` 失败补偿仍有两个确定性缺陷，静态复审确认属于同一会话
fencing near-miss：

1. MySQL COMMIT 报错但读回确认已落地时，生产数据层其实已经在 COMMIT 前保存了
   覆盖前 `sess_jti` 快照；biz 却主动丢掉快照并标成 `SnapshotUnknown`。随后 Redis
   Set 再失败只墓碑 MySQL，会产生 `Redis=A / MySQL=sentinel` 单边撕裂。
2. Redis Lua 已执行但响应丢失时，旧补偿 `DeleteIfJTI(B)` 会把整条 session key
   删除，无法恢复被 B 覆盖的仍在线上一代 A；文档所称“两存储回到上一代”与实现不符。
   Redis 判定/删除和 MySQL 回补还共用同一 timeout，前者耗尽预算会令后者根本不执行。

R12 实施：

- `PersistSessionJTI` 的覆盖前快照在不确定 COMMIT 读回确认后继续保留，不再制造
  `SnapshotUnknown` 分支。
- Redis 条件 Set Lua 在同一玩家 key 内原子保存覆盖前完整 session 到
  `_rollback_*` 字段；新增 `(jti,generation)` CAS 补偿脚本，命中时恢复旧
  token/device/**原绝对过期点**；generation 保持已消耗的本次单调值（与 MySQL
  回补后一致、不回退）。无可恢复旧 session 时清掉 token/jti 等能力字段，但保留
  只有 gen 的 TTL fencing 水位；MySQL 的 HadPrev=false 分支也不再 DELETE，改为
  generation 不变的无能力哨兵。两侧都避免仍在飞的低代际写趁 key/行缺失复活；
  更新代际已覆盖则 no-op。补偿不延长旧会话，也不误回滚并发赢家。
- 补偿顺序固定为 **MySQL 条件回补命中 → Redis 精确恢复**，两步使用两个独立的
  detached 有界预算。MySQL 回补失败/未命中不恢复 Redis 旧能力；Redis 不可达时
  MySQL 已回补真实快照：未提交分支收敛 A/A；已提交但补偿不可判定分支为
  Redis=B（B 从未交付）/MySQL=A，没有一个已交付凭据能同时越过 Redis 现行性门与
  MySQL 代际门，保持 fail-closed，下一次成功 Login 自愈。

新增/改写确定性回归覆盖：Set 未提交、Set 已提交但回包丢失、Redis CAS 补偿失败、
无旧快照时保留水位、A/B/C 三登录迟到低代际不得复活、迟到补偿不得覆盖更新赢家、
原过期点不延长、两个补偿预算互相独立。
login module 全量 Go 测试通过；真实 Redis 断链/响应丢失注入、真 MySQL COMMIT
模糊注入、双设备 E2E、部署产物与观察窗口均未执行，故事故状态保持未关闭。

### 4.1.9 R13 Login 补偿再次纠偏（2026-07-26，未关闭）

R12 的“恢复即时前代”仍不安全：A 已交付、B/C 均未交付时，C 保存的即时前代是
B；C 回补恢复 B 后，B 的迟到补偿又会被更高 generation 拒绝，最终把从未交付的
B 永久立为 current。R13 因此删除 `_rollback_*` 恢复机制，改为安全优先的单调
无能力墓碑：MySQL 只条件命中本次 `(jti,generation)`，Redis 在
`current.gen <= failedGen` 时清能力并推进水位，更高 generation 赢家 no-op；在
MySQL COMMIT 已确认的 Redis Set 失败路径，两侧使用独立有界预算且一侧失败/未命中
不跳过另一侧。旧字段仅在滚动窗口中清理，不再
参与状态决策。

COMMIT 模糊且读回失败的分支不能盲用 generation 清 Redis：未落地事务的 generation
可被后续赢家复用。该分支仅在 MySQL 条件墓碑明确命中时继续 Redis fence；no-op/error
不碰 Redis，并由“B 未落地、C 同代际交付、B 迟到消歧”常驻回归锁死。

常驻 A→B→C→D 确定性交错已证明不会恢复未交付 B，D 以更高 generation 自愈。
该修复不会假装解决 `sessions.Set` 成功后 placement 失败仍扣留新 session 的相邻
交付缺口；详情与关闭矩阵见
[`INC-20260726-002`](2026-07-26-p0-login-session-candidate-delivery.md)。

### 4.2 回归测试（已落地，全绿）

- `internal/biz/replay_duplicate_test.go`：`TestAuthorizeSubscribe_SessionCurrency`（现行放行/旧 jti 拒/登出拒/require 档缺 jti 拒/权威故障 fail-closed/dev 档语义）、`TestRecheckSession_ExpiryClosesStream`（token 到期关流）、`TestRecheckSession_SupersededAndRetryable`（顶号关流含跨 Pod 语义/权威故障可重试）。
- `internal/biz/session_register_race_test.go`：`TestKickedStaleSessionRetriesNeverDisplaceWinner`（R4 二轮：A 被顶后重试 5 次，每次均以 `ErrSessionSuperseded` 拒绝，B 从未被取消，A 迟到的 Unregister 不得删掉 B 的槽位——双设备稳定赢家回归）。
- push/login 相关断言全部收紧为区分 `ErrSessionSuperseded`（顶号）与 `ErrUnauthorized`（过期/登出/证据缺失），含 `login_session_jti_test.go` 顶号核心负例。
- `internal/data/session_gate_test.go`：miniredis 验证跨服务 key 契约、无会话、空 jti 防御、权威不可达必须报错。
- `tools/scripts/tests/gen_cluster_prod_progress_contract_test.ps1`：`-Prod` 产物恰好一处 `require_session_gate: true`、dev 产物保持 false（PASS）。

## 5. 验收矩阵（审计要求，关闭前必须逐项打勾）

| 场景 | 期望 | 状态 |
|---|---|---|
| 旧 token 重连（已被新登录顶号） | 建流拒绝 `session superseded`（gRPC **ABORTED**，业务码 14，与自然过期 UNAUTHENTICATED 可判别） | 单测绿；真实 Envoy 链路未验 |
| **双设备稳定赢家（R4 二轮 P0）**：A 被 B 顶号后反复重试 | A 每次均被 ABORTED/14 拒绝且不得顶回 B；客户端对 14/ABORTED 只转交互登录，不自动 Login 反顶 | 服务端确定性回归绿；真实双设备 E2E 未跑 |
| **建流并发交错（R4 复审①）**：旧会话校验通过后暂停 → 新会话注册 → 旧会话再注册 | 任意交错下新会话持有连接槽，旧会话不得取消新设备 | 确定性交错回归绿；真实并发/多 Pod 实测未跑 |
| 跨 Pod 顶号（旧流在 A Pod，新登录经 B Pod） | 旧流 ≤30s 停止投递并发起关流 | 单测绿（逻辑层）；多 Pod 实测未跑 |
| **写者阻塞在 Send（R4 复审②）**：慢客户端流控期间顶号 | 看门狗独立裁决；解除阻塞后零新帧、以 ABORTED（顶号）关流；句柄回收由 keepalive/max_conn_age/Envoy 1h 有界兜底 | 阻塞写者回归绿；真实流控/慢客户端实测未跑 |
| 会话轮换（同设备重登） | 旧 jti 流关闭，新 jti 建流成功 | 单测绿；E2E 未跑 |
| token 到期 | ≤30s 停止投递并发起关流；Envoy 1h 流寿命兜底 | 单测绿；Envoy max_stream_duration 未实测 |
| Redis 故障 | 建流 fail-closed 拒；在流连续 3 次失败关流 | 单测绿；真实故障注入未跑 |
| dev 直连联调 | 缺省档行为不变（无 jti 放行，有 jti 仍校验） | 单测绿 |
| **旧 JTI 全服务吊销（R5 P0-1）**：顶号后旧 JWT 调 friend/trade/inventory 等玩家 RPC | 一律 ABORTED/14（顶号可判别），登出过期 UNAUTHENTICATED，权威不可达 UNAVAILABLE | 中间件单测 + 生成器契约测试绿；真实 Envoy 全链路未验；**混版滚动期旧副本无门（R6 P1），旧副本排空前安全属性未生效** |
| **过期且被顶——流仍存活（R5 P0-2）** | 流内复查恒 ABORTED/14，不落 UNAUTHENTICATED | 单测绿 |
| **过期且被顶——离线到期后恢复（R6 P0-1 → R7 P0-1 方案 A）**：请求在 Envoy 即拒，应用层判别不可达 | 客户端自动换新全面停用：会话失效一律转交互登录，不存在自动反顶窗口；code14/ABORTED 终态不重试 | 代码完成（UE），待用户编译 + 双设备实测 |
| **跨 Pod 轮换后旧流私有帧（R5 P0-4 + R6 P0-2）** | 逐帧 fence：每条交付帧产生于该帧 fence 通过之前；轮换后产生的帧零交付；在途暴露 ≤1 帧 | 批内交错确定性回归绿；真实多 Pod 未跑 |
| **会话轮换后的旧在途 Login/SelectRole/IssueDSTicket（R5 P0-5 + R6 P0-3 → R7 收口）** | 凭据交付终检扣留 + 角色写 MySQL 同事务代际 fencing（FOR UPDATE，Redis 投影落后也拒） + 票据 sjti 兑换点作废（空 sjti 硬拒，签发链已补齐：matchmaker 批签/hub Transfer/迁移重签） + Hub ACK 会话复核 | 交错单测绿；真实并发双登录 E2E 未跑 |
| **push 断层信号顺序（R7 P1-1）**：重连回放区间被修剪 | 检出断层先发 resync 信号帧再发数据帧，客户端游标不会跳过信号；LostSince 故障 fail-closed 关流 | 确定性回归绿 |
| **S1 迟到关闭对 S2 零副作用（R5 P0-3，UE）** | 登录换代重订阅 + 关闭按会话代次归属判定 | 代码完成，待用户 UE 编译 + 双设备实测 |

## 6. 剩余风险与关闭条件

- **未关闭原因**：真实 Envoy(:8443 jwt_authn + payload 头) + 共享 Redis + 多 Pod 环境的验收矩阵未执行；建流并发交错与阻塞写者两条 R4 复审场景目前只有确定性单测，缺真实并发/流控实测；`go test -race` 需 CGO/Linux CI；R5/R6 的 UE 侧改动（会话代次绑定、过期换新守卫）待用户编译验证，friend TiDB 并发回归为 env-gated（`PANDORA_TEST_TIDB_DSN`）未跑；R5 会话中间件使 12 个客户端面服务新增「会话权威 Redis 可达」依赖（`node.redis_client` 指向 pandora:sess 所在实例），多 Redis 拆分时是部署契约 review 项。
- **R6 复审已确认、R8 后仍开放的关联缺口**：①混版滚动窗口——旧 Pod 无会话中间件，旧 JTI 在旧副本仍可用，「旧副本完全排空」必须列为安全属性生效门（已写入 docs/design/session-generation-rollout.md 阶段 C）；②③guild-prod.yaml.example／push-prod.yaml.example 生产模板缺 session gate 的问题**已修复并于 2026-07-24 复核**：guild-prod.yaml.example 已含 redis_client + `session_gate:`，push-prod.yaml.example 已含 redis_client + `require_session_gate: true`（services/ 与 run/docker-build/prebuilt/ 两处模板一致）；④friend 守卫临界区内 COUNT 为普通读——同一 (player) / (pair) 的写入已被 guard 行 FOR UPDATE 串行化，串行化下 COUNT 看到的是前一个持锁事务提交后的状态（RR 快照在加锁后建立），上限穿透需绕过 guard——风险降级但 TiDB 并发回归（env-gated）未跑，未列已验；⑤friend guard 表存量迁移 **已补**（pandora_social 000006 + friend 启动期 CheckTables fail-fast），friend_pair_guards 无界增长仍是登记在案的容量风险。R7 已处理：原⑦ matchmaker READY 批签票无 sjti 已收口（§4.1.5 第 2 条）；原⑥ cpp pb 重生已完成（含 hub session_jti，UE 两处生成物已同步）。部署项（R8 修正）：存量 pandora_account 库需跑 000003 迁移（启动 CheckTables+CheckColumns fail-fast 兜底）；空 sjti 收口不再要求同步部署硬切，改为分阶段门（§4.1.6 P0-5）。
- **票据兑换点复核的模式边界（R7 后）**：VerifyDSTicket 的 sjti 复核在 DsAuthorityMode=redis 生效；B1 纯本地验票模式下 Battle 票仍由短 TTL 兜底（v2 默认 120s/上限 180s；legacy HS256 默认 5min），但 Hub 路径已由 AcknowledgeAdmission 会话复核覆盖（ACK 必经 hub_allocator，不依赖 VerifyDSTicket）。
- 会话权威与 push 必须指向**同一** Redis（infra 单实例部署契约）；分库部署会让门失效——部署清单 review 项。
- 30s 复查窗口内旧 token 仍可短暂收推送（有界暴露，契约已注释）；缩窗需以 Redis QPS 为代价，当前取 30s。**表述修正（R4 复审）**：30s 界定的是「停止投递 + 发起关流」，不是流句柄消亡；句柄物理回收由 gRPC keepalive/max_connection_age(15m)+grace 与 Envoy max_stream_duration(1h) 有界收敛。修复前的准确表述是「已建立的流不能随会话失效及时关闭」（修复前另有 Envoy 1h 流寿命上界，非字面「无限期」）。
- 关闭条件：验收矩阵全绿 + 生产产物 dbcheck/发布门禁通过 + 观察窗口无复发。
