# [INC-20260722-002][P0] Hub 切线在归属查询 UNKNOWN 时 fail-open

> **状态**：修复实施中（fail-closed 门禁 + 回归已落地，2026-07-22；Owner Authority 全链路接线仍待独立工作流，未关闭）
> **类型**：`split-brain` / `near-miss`
> **环境**：本机源码与单测审计（未确认在线上发生）
> **首次发生时间（UTC）**：未在线上确认；受影响行为由 commit `7f851181` 于 2026-07-01 引入
> **首次发现时间（UTC）**：2026-07-22 04:49:00
> **负责人**：永久修复待最高可用 Claude 实现，项目负责人待指定
> **受影响服务/版本**：`services/battle/hub_allocator`，当前基线 `a138ff25d5fd56432b1fc2ad7d20d80c0f126b6b`；Owner Authority 全链路仍处 migrate 未接线阶段
> **最后更新**：2026-07-22

## 0. 一句话结论

`hub_allocator` 把 player_locator 当作可 fail-open 的弱依赖：查询失败时继续切线，key miss/非 OK 也按“未阻断”处理。现行不变量已明确 locator 只是 TTL presence 投影、UNKNOWN/key miss 不能授权新归属；而唯一 Owner Authority 又尚未接入 login/allocator/DS，因此旧 DS 仍持有有效 lease 时，当前路径不能排除给玩家预留/签发另一 Hub DS 的脑裂风险。该问题未修复、未部署验证，不能因现有放行测试通过而关闭。

## 1. 影响与范围

- 玩家影响：潜在同时保留旧 Battle/Hub DS 的可玩状态，又获得新 Hub 线路票据；也可能产生重复占座、错退旧 assignment 或迟到写影响新会话。
- 影响人数/对局/请求数：线上是否发生及请求数未知；本次是上线前静态证据和现有单测确认。
- 服务影响：`HubAllocatorService.TransferToLine` / `HubUsecase.TransferToLineForPlayer` 主动切线路径。
- 数据与安全影响：玩家 DS 归属可能出现双写/脑裂，且 locator TTL 投影被错误用于权威准入判断。
- 开始/结束时间：受影响行为自 `7f851181` 存在；当前 HEAD 仍存在。
- 是否仍可复发：是。
- 严重级别判定理由：可能破坏“同一玩家最多一个可玩 DS”的 P0 硬不变量，按 split-brain near-miss 建档。

## 2. 第一现场与证据

### 2.1 症状

- 客户端症状：潜在在战斗/匹配中仍收到 Hub 切线结果，或在两个 DS 之间发生状态竞争；本次无线上客户端样本。
- 服务端症状：locator 错误只写 `transfer_locator_check_failed` Warn，调用继续产生后续切线副作用。
- K8s/Agones 状态：不适用，本轮未触碰集群。

### 2.2 原始证据

```text
hub.go:783-790
  locator.InBattleOrMatching 返回 error 时只记录 Warn，不 return；流程继续。

locator_client.go:54-60
  GetLocation 非 OK（包括 key miss/未知）返回 (false, nil)，被解释为“未阻断”。

hub_test.go:1247-1258
  TestTransferToLineForPlayer_LocatorErrorAllows 明确断言 locator error 后切线成功。
```

- `CLAUDE.md:152-160` 要求 owner/route UNKNOWN fail-closed、key miss 不证明离开旧 DS、只有唯一 Owner Authority 可以切换归属。
- `docs/design/owner-authority.md:3-4,91,119` 明确权威本体虽已落码，但 login/allocator/DS 集成尚未接线；不可用时必须 UNKNOWN/WAIT，不能降级为旧门单独放行。
- `docs/design/pandora-arch.md:343` 同样把全链路集成标记为 migrate 未接线。

现有测试可直接确认 fail-open 行为：

```text
cd services/battle/hub_allocator
go test -count=1 ./internal/biz -run TestTransferToLineForPlayer_LocatorErrorAllows
结果：PASS（该 PASS 正是错误契约被固化的证据，不是安全性证明）
```

### 2.3 已排除的噪声

- locator 中存在 BATTLE/MATCHING 时，现有代码会拒绝切线；问题发生在依赖错误、非 OK、key miss、TTL 过期或投影残缺的 UNKNOWN 边界。
- Redis assignment、cooldown 和容量检查不能证明旧 DS owner lease 已失效，不能替代 owner authority。
- DS 侧 `SetLocation` 只能更新 presence 投影，不能提供 `owner_epoch + lease + admit_not_before` 的线性一致交接屏障。

## 3. 时间线

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 2026-07-01（commit 时间为 +08:00） | hub_allocator | `7f851181` 引入 locator 错误放行及对应测试 | `git blame` |
| 2026-07-21 | owner | 唯一 Owner Authority 本体落码，集成仍标为 migrate 未接线 | owner 设计与架构决策 |
| 2026-07-22 04:49:00 | 测试审计 | 确认现有测试固化 UNKNOWN fail-open，且与现行不变量冲突 | 源码、测试、设计交叉审计 |
| 2026-07-22 04:50:00 | 事故管理 | 建立 P0 near-miss 档案 | 本文档与索引 |

## 4. 调用链与关键变量

```text
HubAllocatorService.TransferToLine
  -> HubUsecase.TransferToLineForPlayer
  -> HubLocationChecker.InBattleOrMatching
  -> player_locator.GetLocation error / non-OK / key miss
  -> 被解释为未阻断
  -> cooldown / assignment / capacity 等检查
  -> TransferHub 占新、切归属、退旧、重签票
  -> 旧 DS owner lease 是否仍有效没有由唯一 authority 线性证明
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| locator location | player_locator TTL key | presence 投影、短 TTL | 共享可变、可缺失 | 被错误当成权威准入否定证明 |
| Hub assignment | hub_allocator Redis | Hub 容量/路由记录 | 共享可变 | 只能证明历史 Hub assignment，不能证明旧 Battle owner 已释放 |
| owner record/lease | owner authority | 每玩家线性一致权威 | 共享可变、单调 epoch | 正确交接所需，但当前调用链尚未接入 |
| `operation_id` / `owner_epoch` | owner transition | 跨重试稳定操作与 fencing | 当前切线路径未完整使用 | 缺失导致无法幂等证明“同一归属切换” |

## 5. 根因

### 5.1 直接根因

历史实现把大厅切线定义为“低危弱依赖”，在 locator 失败时主动 fail-open；同时用 TTL presence 投影代替 owner 权威的否定证明。项目随后确立唯一 Owner Authority、UNKNOWN fail-closed 和交接屏障不变量，但 `hub_allocator` 旧路径及旧测试没有随设计迁移，且新 authority 仍未全链路接线。

### 5.2 触发条件

- 玩家仍可能属于旧 DS，且 locator 查询失败、返回非 OK、key miss、TTL 过期或投影残缺。
- 玩家发起 Hub 切线，并且后续 assignment/capacity/cooldown 条件允许。

### 5.3 故障放大因素

- 配置允许 locator 地址为空，此时整个战斗/匹配护栏被跳过。
- 现有测试名称和断言把 fail-open 表述为预期行为，容易让普通回归误报安全。
- Owner Authority 已有服务本体但未接入调用链，形成“有权威设计、无权威门禁”的迁移窗口。

### 5.4 为什么现有保护没有挡住

- locator TTL/状态：只是投影，key miss 与 UNKNOWN 都不能证明旧 owner 已释放。
- Hub assignment/cooldown：保护容量与刷线，不提供跨 DS owner fencing。
- DS SetLocation：不能形成与 owner lease、epoch、Admission 同事务域的线性一致屏障。
- 重试/超时：fail-open 会把不确定直接转成成功副作用，重试无法恢复被错误签发的新归属。

## 6. 全仓同类问题扫描

- 扫描基线 commit：`a138ff25d5fd56432b1fc2ad7d20d80c0f126b6b`。
- 扫描目录和文件类型：本轮已扫描 matchmaker、hub_allocator、player_locator 与 owner 设计/实现相关 Go 文件。
- 搜索模式/工具：`rg` 搜索 `locator`、`key miss`、`UNKNOWN`、`IsInBattle`、`TransferToLine`，并用 `git blame` 追溯。
- Confirmed 同型命中：hub_allocator locator error/non-OK/nil checker 三类 fail-open；matchmaker 的 locator 查询错误在 biz 层已 fail-closed，但 key miss 仍无法替代 owner authority。
- 结构性隐患：Owner Authority 在 login/allocator/DS/battle_result 尚未接线；team 的 MATCHING/IN_BATTLE 状态迁移未实现。
- 已排除项及理由：matchmaker `ensureNoneInBattle` 对已配置 locator 的 RPC error 会零副作用拒绝，本轮新增整队 UNKNOWN 用例已覆盖这一局部行为。
- 未覆盖边界：旧 DS 分区后恢复、Stable/Canary 混跑、Admission ACK 丢失、迟到 Logout/Heartbeat、全链路 E2E 尚未验证。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 建档并禁止把 fail-open PASS 当作安全验证 | 已完成 | 本文档 | 不消除运行时风险 |
| 在目标环境禁用主动切线或确保入口不可用 | 未执行，需人决策 | 无 | 会降低功能可用性；不得由本次测试任务擅自操作 |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| 按 owner-authority migrate 方案把 Hub 切线接入稳定 `operation_id` 的 BeginTransition/Admission/lease 屏障 | 待实现（owner-authority 独立工作流；本事故先落 locator fail-closed 止血层） | 见 owner-authority migrate 阶段 | 重复/回包丢失/并发/重启矩阵 |
| 所有 owner 查询错误、UNKNOWN、key miss 在产生副作用前返回 WAIT/UNAVAILABLE，零占座、零签票 | **已实现(2026-07-22,locator 面)**：TransferToLine 的 locator RPC 失败/非 OK/OFFLINE(含 key miss、TTL 消失)/未知状态一律在冷却占坑等任何副作用前返回 ERR_UNAVAILABLE 拒绝；只有明确 HUB 放行；MATCHING/BATTLE 拒。nil checker 保留为 dev 联调跳过但每次放行 Warn 留痕 | `hub_allocator/internal/biz/hub.go` TransferToLineForPlayer + `data/locator_client.go` InBattleOrMatching | fail-closed 回归 + 零副作用(不吃冷却)断言 PASS |
| 移除/改写 `LocatorErrorAllows` 错误契约，并覆盖 nil checker、non-OK、TTL 消失 | **已实现(2026-07-22)**：`TestTransferToLineForPlayer_LocatorErrorAllows` 改写为 `..._LocatorErrorFailsClosed`(修复前必失败) + `..._NilCheckerDevModeAllows`；data 层新增 8 态映射矩阵单测(matching/battle/hub/offline/unspecified/未来态/非 OK/RPC error) | `hub_test.go` + `locator_client_test.go` | `go test -count=1` PASS |
| 完成 login/allocator/DS/battle_result 全链路 authority 接线 | 待实现 | 见 owner-authority migrate 阶段 | 玩家 E2E 与故障注入 |

### 7.3 防复发规则

- 现行 `CLAUDE.md` §9.22/§9.23 已是防复发硬规则：presence 不得参与权威写决策，UNKNOWN 必须 fail-closed 且等待有界、可见、可持续恢复。
- 测试必须区分“旧行为回归”与“现行不变量验收”；与现行设计冲突的旧 PASS 不能作为发布门禁。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| 现有 locator error 用例 | PASS：明确允许继续切线（错误契约） | 未执行 | `go test -count=1 ./internal/biz -run TestTransferToLineForPlayer_LocatorErrorAllows` | `hub_test.go:1247-1258` |
| 正确 fail-closed 回归 | FAIL（根据当前分支可直接推出；未留必失败用例） | 未执行 | 待生产修复时落地 | 阻断关闭 |
| matchmaker 整队 BATTLE/UNKNOWN | PASS，且零 ticket/claim/queue 副作用 | 不适用 | 新增 biz 测试，`-count=20` | 只证明 matchmaker 局部 fail-closed |
| Owner MySQL 线性 CAS | PASS | 不适用 | 本机开发 MySQL root DSN，`TestOwnerRepoMySQL` | 只证明 authority 本体，不证明调用链接线 |
| `go test -race` | PASS（仅 `hub_allocator/internal/service` 本轮变更包） | 未执行 | 既有 `golang:1.26.5-bookworm` 镜像，源码只读挂载 | 未覆盖 biz 的 locator/owner 并发交接，仍阻断关闭 |
| 旧 DS 分区/恢复与双 DS 故障注入 | 未执行 | 未执行 | 待多进程/集群环境 | 阻断关闭 |
| Stable/Canary 与玩家 E2E | 未执行 | 未执行 | 待目标环境 | 阻断关闭 |

## 9. 部署、回滚与观察

- 修复 commit：无。
- 构建产物/镜像 digest：无。
- 部署时间与目标环境：未部署；本次未执行 `kubectl apply`、`docker push` 或生产操作。
- 实际 Pod `imageID` / GameServer provenance：未核实。
- 回滚条件和步骤：当前无修复产物；若线上包含受影响行为，是否临时关闭切线由人/运维决策。
- 观察窗口、指标与结果：未开始；后续需观测 owner transition 冲突、UNKNOWN 等待、旧 epoch 拒绝、重复 assignment 与双 Admission。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A-001 | P0 | 按同一发布契约完成 Login + Hub + Battle 的 Owner Authority 强 Begin/Admit/Release、稳定 operation 与 WAIT 恢复闭环 | 最高可用 Claude | 待处理；禁止只开 Hub | 本 Incident |
| A-002 | P0 | 为 error/non-OK/key miss 落 locator 面 fail-closed 回归 | 最高可用 Claude | 已完成（仅止血层，不等于 Owner Authority 闭环） | 本 Incident |
| A-003 | P0 | 完成旧 DS 分区恢复、Admission 丢包、迟到写和 Stable/Canary 故障注入 | 最高可用 Claude / Codex 环境执行 | 待处理 | 本 Incident |
| A-004 | P0 | 核实目标环境版本与历史双归属/重复 assignment 证据 | 人/运维 | 待处理 | 本 Incident |
| A-005 | P1 | 接通 team READY→MATCHING→IN_BATTLE 并禁止活动队伍并发变更 | 最高可用 Claude | 待处理 | 独立实现缺口 |

### 10.1 2026-07-26 R12 全链激活复审（仍未关闭）

本轮按 §9.22/§9.23 重新检查“是否可以先完成并启用 Hub contract”。结论是否定的，
因此没有落仅 Hub 的半成品业务修改，`login.owner_query_first` 与 Hub/Battle allocator
强 contract 档继续保持关闭。确定性阻断如下：

1. `LoginUsecase.Login` 在 Redis/MySQL 会话已轮换后仍先走 locator/match/Hub 分配；
   locator、重连权威或 Hub 分配失败会直接返回错误。service 的错误响应只带业务码，
   不带刚建立的新 session，结果是旧在线会话已经失效、调用端却拿不到新 token。
   §9.23 要求此类暂时故障返回携带同一 session/original operation 的可见 `WAIT`。
2. 完整 Login、SelectRole、IssueDSTicket、ResolveHub/ResolveBattle 尚未统一 query-first；
   角色查询错误仍会被折叠成 `role_id=0`，无法区分 `ROLE_UNKNOWN → WAIT` 与确实未选角
   的 `ROLE_REQUIRED`，并可能在未选角前提前分配 Hub。
3. Hub `AssignHub`/Admission ACK 虽已有 additive placement 字段，但当前服务没有把稳定
   operation/owner epoch 贯穿 assignment、票据、强 Begin 与强 Admit；Begin 失败仍是弱依赖。
4. Battle 的稳定 match operation 尚未透传到 ds_allocator；roster Begin 使用随机 operation
   且失败照常交付 allocation，Admit 又在玩家可服务后的 census 才弱执行。取消/超时路径也
   缺少按同一 `(player, owner_epoch, operation, exact target)` 的精确 Release。
5. 票据/JWT 尚未携带每玩家 owner epoch + operation，不能在现有 pre-Pawn 门前完成强
   Admit；一个 match 级 epoch 也不能替代每玩家权威代次。

现有 `LoginResponse.resume_context` 的 `WAIT/TARGET/ROLE_REQUIRED` wire 足以承载登录入口
状态，但它不能单独弥补 allocator、票据、Admission 和取消链缺口。最小不可拆分批次仍需
同时覆盖 Login、Hub、Battle、matchmaker、JWT/proto、UE 入场门与 coordinated rollout。
在 UE 未编译、真实 TiDB/多副本分区/回包丢失未验的本轮边界内，架构 P0 不能宣称完成。

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据
- [ ] 修复前失败、修复后通过的回归存在
- [ ] race/集成/故障注入达到本事故风险要求
- [ ] 同类代码扫描完成
- [ ] 目标环境已加载可追溯的新产物
- [ ] 玩家路径、恢复和补偿路径验证通过
- [ ] 观察窗口无复发
- [ ] 剩余风险已解决或另建 Incident/任务
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭；当前仅确认旧 fail-open 契约与现行 owner 不变量冲突。
