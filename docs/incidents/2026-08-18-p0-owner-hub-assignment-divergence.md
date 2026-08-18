# [INC-20260818-001][P0] Hub 票据与 Owner assignment 分叉导致玩家恢复死循环

> **状态**：本机修复已部署并通过真实玩家 Hub E2E（本机 P0 已闭环；生产滚动 P1 另案阻断）
> **类型**：`availability` / `split-brain`
> **环境**：本机 k8s `pandora-agones` + UE PIE
> **首次发生时间（UTC）**：2026-08-18 03:23:42（客户端日志为 2026-08-17 23:23:42 EDT，UTC-4）
> **首次发现时间（UTC）**：2026-08-18 03:23:43
> **负责人**：Codex（修复与本机验证）
> **受影响服务/版本**：故障版本为 `login`、`hub-allocator`、`ds-allocator`、`owner` 的 `g125b5bf78af6-dirty-20260817-220730`（启动 commit `125b5bf7`）；本机修复版本见 §9
> **最后更新**：2026-08-18

## 0. 一句话结论

Hub Allocator 已发布并向玩家/Hub DS 交付 assignment B，但 Owner Authority 仍保存同一物理
Hub 实例上的 assignment A；客户端正确地拒绝这份互相矛盾的 ACK，并持续重查直至恢复超时。
直接触发点是 Owner 把“同物理实例、不同 assignment”误判为幂等 no-op；同型扫描又确认
Hub 的 assignment CAS 与 owner Begin 定序、epoch conflict 盲重试、ACK/Login 交付门均能重新
制造或放大同类分叉。代码修复、真 MySQL、全模块、Linux race、不可变镜像构建、本机安全
切换与真实 UE 玩家 Hub E2E 均已完成；第二轮在 HUB confirmed 后持续观察 79.658 秒，未再
请求 Resume、Travel、出现 mismatch/phase deadline 或恢复面板。生产混版滚动能力仍由
INC-20260818-003 阻断，不能据此宣称 production-ready。

## 1. 影响与范围

- 玩家影响：玩家已进入 Hub DS，但客户端不能确认 HUB admission；约每 1.3 秒重查
  `GetResumeContext`，每轮票据期限后重连，最终弹出恢复面板，表现为“进大厅后一直卡恢复”。
- 影响人数/请求数：第一现场确认玩家 `25160536895520768` 一人；全量受影响人数尚未统计。
- 服务影响：服务与 Pod 均 Ready，无 crash；这是两个权威出口返回互斥事实的逻辑可用性故障。
- 数据与安全影响：未发现角色持久数据丢失或凭证泄漏。Owner/Redis 的归属版本短时或永久分叉，
  若放宽 exact 校验则会升级为双归属，因此客户端和 DS 的 fail-closed 行为不能移除。
- 开始/结束时间：首次样本 2026-08-18 03:23:42 UTC；本机故障二进制于 05:49:45 UTC
  完成最后一项 Owner 切换，玩家侧于 06:45:44 UTC 完成第二轮 79.658 秒观察。
- 是否仍可复发：已知旧路径在本机部署面被移除；四个旧 ReplicaSet 均为 0，且旧 allocator
  在新 Owner 启动前已排空。两轮真实玩家链均 exact 收敛，第二轮观察超过 75 秒。本机事故
  已闭环；旧新 binary 普通滚动共存仍不安全，生产发布由 INC-20260818-003 fail-closed 阻断。
- 严重级别判定理由：真实玩家无法稳定完成进场，符合事故规范的 P0 availability；同时存在
  assignment/owner split-brain，普通 Pod Ready 不能代表恢复。

## 2. 第一现场与证据

### 2.1 症状

- 客户端症状：`ClientTravel` 后不断调用 `GetResumeContext`，收到 Hub ACK 却记录
  `owner_target_mismatch`；约 146 秒后 phase deadline，弹恢复面板。
- 服务端症状：Hub Allocator 与 Hub DS 都使用 assignment B；Owner 的 no-op 日志同时打印
  current assignment A 与 requested assignment B，后续 Query 仍返回 A。
- K8s/Agones 状态：`owner`、`hub-allocator` Deployment 均 1/1 Ready，证明本事故不由探针、
  CrashLoop 或资源回收直接触发。Hub GameServer/DS 能完成 PreLogin 和 ACK。

### 2.2 原始证据

含票据/JWT 的原始 UE 日志位于受控附件：
`C:\Users\Administrator\.codex\attachments\333feb11-a404-4f1f-ad80-2ef93d5673e5\pasted-text.txt`，
SHA-256 `8D4C2F7AA3E3CED19EABDFCF4BA732383ED4A88ECBF59E56E7D19C84D1FA34EC`。
本文只保留脱敏字段，禁止复制附件里的完整票据。

```text
2026-08-17 23:23:42.157 EDT  ClientTravel；owner Resume 期望 assignment=8ef119ed-…，owner_epoch=5
2026-08-17 23:23:43.309 EDT  Hub ACK 携 assignment=657ac5ce-…，client: owner_target_mismatch
2026-08-17 23:26:08 EDT      HUB phase deadline，进入恢复兜底
2026-08-17 23:26:12 EDT      路由转为 BATTLE
2026-08-17 23:26:19 EDT      ResumeContext confirmed BATTLE，轮询停止
```

运行时交叉证据：

```text
hub-allocator: hub_assigned assignment=657ac5ce-…
Hub DS:        ds_prelogin_ticket_ok -> hub_admission_ack_sent -> hub_entry_confirmed
               均使用 assignment=657ac5ce-…
owner:         owner_transition_noop current_assignment=8ef119ed-…
               requested_assignment=657ac5ce-…；后续 owner_queried 仍返回 8ef119ed-…
```

### 2.3 已排除的噪声

- 客户端轮询/退避不是根因：BATTLE exact target 到达后同一状态机在 7 秒内确认并停止轮询。
- Hub DS 没有拒绝票据：PreLogin、admission ACK、entry confirmed 均成功；“已生成 Pawn/已进门”
  不能替代 Owner exact assignment 一致性。
- Pod Ready、零 restart 不能排除本事故：故障发生在跨存储的业务身份上，不影响进程健康。
- 不能通过放宽 assignment/track 校验止血：那会允许旧票或旧 DS 打开 spawn gate，破坏 fencing。

## 3. 时间线

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 03:23:42.157 | UE client | 按 Owner assignment A 发起 HUB 恢复/Travel | 受控附件，EDT 23:23:42.157 |
| 03:23:43.309 | UE client / Hub | 收到 assignment B 的 ACK，因与 Owner A 不同而拒绝 | `owner_target_mismatch` |
| 03:26:08 | UE client | HUB 恢复期限耗尽，进入兜底面板 | 受控附件 |
| 03:26:12 | Login/Owner | 路由转为 BATTLE | 受控附件 |
| 03:26:19 | UE client | BATTLE exact target 确认，轮询停止 | `ResumeContext confirmed BATTLE` |
| 2026-08-18 审计期 | Owner MySQL | 基线回归复现同实例换发不收敛；第一版原地刷新又被迟到旧 epoch 确定性回滚 | §8 命令 |
| 2026-08-18 审计期 | Hub/Login/DS | 全仓扫描确认 CAS loser、副作用定序、盲重试与交付门缺口 | §6 |
| 06:35:52–06:36:41 | UE/Login/Hub/Owner | 第一轮真实玩家登录、exact ACK、HUB confirmed；玩家主动退出前无复发 | §8.2 |
| 06:44:23–06:45:44 | UE/Login/Hub/Owner | 第二轮复用同一 exact target，confirmed 后持续观察 79.658 秒，零续轮询/错配 | §8.2 |

## 4. 调用链与关键变量

```text
Login
  -> HubAllocator.AssignHub
     -> Redis assignment CAS（票据/Hub DS 数据源）
     -> Owner.BeginTransition（Owner/GetResumeContext 数据源）
  -> Owner.QueryOwner
  -> 同一 LoginResponse 交付 HubTicket + Resume TARGET
Hub DS
  -> Verify ticket
  -> AcknowledgeAdmission
  -> Owner.Admit(exact target)
UE client
  -> 比较 ACK target 与 Resume target
  -> 不一致则 fail-closed 丢弃并重查
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| Hub assignment B | Hub Allocator / Redis | seat/路由记录，换发可生成新 UUID | 共享可变 | 票据、Hub DS ACK 使用 B |
| Owner assignment A | Owner / MySQL `owner_record` | 玩家单调 epoch 的归属权威 | 共享可变 | GetResumeContext 一直返回 A |
| `owner_epoch` | Owner MySQL 事务 | 每次完整 target 迁移单调 +1 | 共享单调 | 第一版原地刷新绕过它，允许迟到请求回滚 |
| Ticket binding | Hub Allocator signer | assignment/pod/uid/epoch/track 等不可变声明 | 不可变 | 必须与 Redis winner 和 Owner exact target 一致 |
| Admission ACK | Hub DS -> Hub Allocator/UE | 单次 admission identity | 可重放 | B 与 Resume A 不同，被客户端正确拒绝 |
| Resume TARGET | Login <- Owner | 单次响应快照 | 可变快照 | 不能与另一时刻的互斥票据同时交付 |

## 5. 根因

### 5.1 直接根因

Owner `BeginTransition` 的幂等判断只比较 owner type、Pod、instance UID、instance epoch，不比较
`assignment_or_allocation_id` 与 `release_track`。Hub 在同一物理实例换发 assignment 后，Owner
将请求判为 no-op，导致 Redis/票据/DS 使用 B，而 Owner Query 仍使用 A。

第一版修法曾尝试在原 epoch 内原地覆盖 assignment/track；对抗回归证明它不安全：UUID 没有
单调版本，迟到的旧 `expect_epoch` 请求能把 B 写回 A；Battle allocation 或 release track
变化也会继承旧 `ADMITTED` 权限。正确语义是完整 target 只有全等才 no-op，任一身份字段变化
均走 `epoch+1/PENDING/new operation`。

### 5.2 触发条件

- 玩家在相同 Hub Pod/UID/protocol epoch 上获得新的 assignment UUID；
- Owner 当前记录仍是旧 assignment，且调用链把物理实例相等误当作完整 owner 身份相等；
- 客户端/ACK 继续执行正确的 exact assignment fencing。

### 5.3 故障放大因素

- Hub replacement、same/cross transfer、drain-new 原实现先调用含 Owner Begin 副作用的签票 helper，
  后执行 Redis CAS；并发 CAS loser 可把 Owner 指向一个从未发布的 target。
- Hub 与 DS 的 Owner helper 遇 epoch conflict 后用同一个旧 target 重查 epoch 再写，可能覆盖真正 winner。
- Hub ACK 的 `ADMITTED` 幂等快路只比物理实例，旧 assignment/错误 track 可绕过 exact Admit。
- Login 分别验证票据与 Owner，却不比较两者，能在同一响应交付 ticket B + Resume A。
- 现有 happy-path/并发测试只断言 Redis winner 或“最终有票”，没有断言 Owner 最终 exact target。

### 5.4 为什么现有保护没有挡住

- 重试会稳定重现分叉，因为旧 Owner 对每次 B 都返回同实例 no-op；重试不是收敛机制。
- epoch CAS 在第一版原地刷新分支之前没有生效；Hub/DS 的 conflict 盲重试又错误地把 CAS 当成
  “拿新 epoch 继续写旧 target”的许可。
- Admission 与客户端 exact 门确实挡住了错误归属，但代价是玩家无法进场；它们是最后安全线，
  不是造成分叉的根因。
- K8s readiness 只证明进程可服务，不检查 Redis assignment、Owner target、票据与 ACK 四方一致。

## 6. 全仓同类问题扫描

- 扫描基线 commit：`796da364` + 当前未提交修复。
- 扫描目录和文件类型：`services/runtime/owner`、`services/battle/hub_allocator`、
  `services/battle/ds_allocator`、`services/account/login` 的 Go 生产代码与测试；Owner 设计/事故规范。
- 搜索模式/工具：`rg` 扫 `BeginTransition`、`EPOCH_CONFLICT`、`signHubTicket`、assignment CAS、
  `ADMITTED` 快路、Hub ticket/Resume 映射；调用点逐条检查副作用先后。
- Confirmed 同型命中：Hub replacement、Transfer same/cross、drain-new 的 sign-before-CAS；Hub 与
  DS 的旧 target conflict 盲重试；Hub ACK 非 exact 快路；Login 跨出口无一致性门。
- 结构性隐患：DS claim loser 旧实现可在 winner Owner Begin 完成前直接交付 READY；
  已改为全员 Owner exact + allocation one-shot postcheck 的纯只读门。Begin 不确定提交的
  长期持久恢复增强已单列 [INC-20260818-002](2026-08-18-p1-ds-owner-bind-outcome-unknown.md)。
- 已排除项及理由：Hub assignment reuse 与 drain recovered-target 已是 assignment CAS 后签票；
  DS claim winner 的 allocation finalize/READY 发生在 Owner Begin 前，未发现 sign-before-allocation。
- 未覆盖边界：Battle DS 玩家可操作门的真实多副本交错、跨版本滚动矩阵与网络分区尚未执行。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 保持客户端、Login、ACK exact fail-closed，不放宽 assignment 校验 | 已确认 | BATTLE 可收敛；错误 HUB ACK 被拒 | 玩家仍卡恢复，但避免双归属 |
| 不单独升级 Owner；allocator 先停写排空 | 已完成 | 旧 Pod/lease 同时归零后静默 10.4s；新 Hub/DS writer 就绪后 Owner 最后切换 | Owner 已升级后禁止单独回滚 allocator 到旧版 |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| Owner 仅完整 target 全等才 no-op；换发走 epoch+1/PENDING/new op | 已落码 | `owner_repo.go` | 真 MySQL 换发/迟到旧 epoch/Battle allocation+track |
| Hub 纯签名与 Owner bind 拆分；只允许 Redis CAS winner Begin，前后 exact 重读 | 已落码 | `hub.go`、`owner_authority.go` | CAS loser、Begin 前后漂移、并发 winner 测试 |
| Hub ACK 在 ADMITTED 快路前比较完整 Owner target | 已落码 | `hub.go` | stale assignment / wrong track 拒绝且零 Admit |
| Login 同一响应强制 ticket 与 Owner Resume exact target 全等 | 已落码 | `login.go` | 不一致时扣票、返回 WAIT |
| DS Owner conflict 禁止旧 target 盲重试；校验 Begin 回包并处理不确定提交 | 已落码 | `ds_allocator/internal/biz/owner_authority.go` | conflict/non-exact/commit-then-error/outcome-unknown 交错全绿 |
| DS claim loser READY 交付硬门 | 已落码 | `ds_allocator/internal/biz/allocator.go` | winner Begin 前扣 READY；后续 exact 只读交付；allocation 漂移拒绝 |

### 7.3 防复发规则

- [`../design/owner-authority.md`](../design/owner-authority.md) 明确完整 target、CAS winner 副作用顺序、
  conflict 禁盲重试、Login/ACK exact 交付门。
- 回归必须同时断言 Redis assignment、Owner record、票据、ACK/Resume，不允许只以 DS 已进门或
  Pod Ready 判绿。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| Owner 原事故回归 | 基线 `796da364`：同实例换发后 Query 仍是旧 assignment，FAIL | PASS | `go test -p 1 -count=1 -run '^TestOwnerRepoMySQL$/^AssignmentRefreshConvergesResumeView$' ./internal/data` + 真 MySQL 3307 | epoch 1 -> 2，Query 新 assignment |
| Owner 反修法交错 | 第一版原 epoch 刷新：迟到旧 expect 可把 v2 回滚为 v1，FAIL | PASS | `AssignmentRefreshRejectsStaleEpochRollback` | stale expect=0 vs current=2 conflict |
| Owner Battle/track | 第一版会原地继承旧 epoch/phase | PASS | `BattleAllocationAndTrackChangesAdvanceEpoch` | 真 MySQL 3307 |
| Hub / Login 定向单测 | 旧测试未覆盖跨出口 exact invariant | PASS | Hub CAS loser/Begin drift/ACK exact；Login mismatch WAIT | 本 Incident 构建快照定向用例 |
| 四服务全模块 | 未针对本修复运行 | PASS（Owner/Hub/DS/Login） | `GOMAXPROCS=2; go test -p 1 -count=1 ./...`（各 module） | 2026-08-18 本 Incident 构建快照；真 MySQL 另见上方带 DSN 专项 |
| `go test -race` | 未运行 | PASS | `tools/scripts/go_test_race.ps1` / Linux `golang:1.26.5` + CGO；Hub/DS/Login/Owner 目标包 | 本 Incident 构建快照四包全绿，无 data race；后续并发工作树变更不计入 |
| Proto 契约与生成 | N/A | PASS | `pwsh tools/scripts/proto_gen.ps1 -Cpp -Breaking` | lint/breaking/Go/C++ 全通过；本 Incident 的 Owner proto 仅注释变化、wire 不变、Owner C++ 生成物无 diff；工作树其他并发 proto 变更不计入本结论 |
| 后端事故形状 A→Release→B | 旧版真实日志为 Redis/票据 B、Owner A | PASS | 专用 dev 玩家：Login A→仅 ReleaseHub→Login B→清理 | B assignment != A，Owner epoch 1→2；A/B 两轮 ticket、Resume、Owner 五字段全等，三类 mismatch 日志均 0 |
| 集群级并发故障注入 | 真实事故已命中 A/B 分叉；CAS loser/滚动混版未在集群注入 | 未执行 | 本机 k8s | 代码级 barrier/CAS loser 已覆盖；混版由 INC-20260818-003 阻断生产发布 |
| 玩家 Hub E2E | FAIL：HUB ACK 后持续轮询并超时 | PASS | UE 登录 -> HUB -> exact ACK -> 79.658s 观察 | 两轮均 exact；第二轮 confirmed 后 Resume 请求/Travel/mismatch/deadline/恢复面板均为 0 |
| 扩展 BATTLE/回 HUB E2E | 原事故后来靠人工进入 BATTLE 才退出恢复循环 | BLOCKED | UE HUB -> BATTLE -> HUB | StartMatch transport 成功但业务码 3006；队伍仍 FORMING、ready_count=0，未进入 READY/DS 分配，不归因于本事故 |

### 8.1 后端事故形状实跑

2026-08-18 05:58:53–05:58:56 UTC 使用专用 dev 玩家 `25218196831502336`（账号只记录
SHA-256 `471ad0c07b07348e7b2da7f0526d584d3802c37d3ccfc2379a2e9c4833df7973`）执行：

```text
Login A: assignment=809c68fc-8b12-4d26-b323-c0a3f8b93c23, owner_epoch=1/PENDING
ReleaseHub(A): Redis assignment/seat 删除，Owner 仍保持 epoch=1/A
Login B: assignment=e344a38b-589c-4f6c-8282-c5502b5b4560, owner_epoch=2/PENDING
```

A、B 两轮 ticket、Login Resume 与 Owner 的 pod/UID/instance epoch/assignment/track 全等；
`owner_target_mismatch`、`login_hub_ticket_owner_mismatch`、
`owner_record_missing_after_assign`、epoch conflict、admit identity mismatch 均为 0。随后
ReleaseHub(B)+Logout 成功，Owner 为 NONE、target 为空且 epoch 保留 2；无在线 session、
assignment 或 owner 残留。该实跑覆盖原事故的“同一实例换发 assignment”后端形状，但没有
单独建立 UE Admission ACK/客户端停止轮询证明；该证明由下节真实 UE E2E 补齐。

### 8.2 真实 UE 玩家 Hub E2E

2026-08-18 06:35:52–06:36:41 UTC，真实 UE PIE 玩家登录到 Hub：Login、Hub Allocator、
Owner、Hub DS 与客户端全程使用 assignment `a01b67ba-6c3d-4a24-8cde-ab495fbd6110`，
Owner epoch 6 -> 7、PENDING -> ADMITTED。Hub DS 依次完成本地验票、admission ACK、
pre-open recheck 与 `hub_entry_confirmed`；客户端在 06:35:56 收到 exact ACK，06:35:59
记录 `ResumeContext confirmed HUB admission`，主动退出前无 mismatch、deadline 或恢复循环。

为补足观察门，06:44:23 UTC 重新从登录入口进入同一 Hub exact target；06:44:24.318 收到
exact ACK，06:44:24.985 confirmed，至 06:45:44.643 结束 PIE 共持续 79.658 秒。该窗口内：

```text
GetResumeContext new requests = 0
extra ClientTravel             = 0
owner_target_mismatch          = 0
phase deadline                 = 0
recovery panel                 = 0
```

第一轮后端又持续观察至 06:42:40 UTC，四侧 assignment 集合仍只有上述一项，Owner epoch
conflict、identity mismatch、准入拒绝/失败均为 0；玩家退出也完成 exact departure ACK。
票据、JWT 与 session 原文均未写入本文。

### 8.3 扩展 Battle 玩家链尝试

06:47:48 UTC 用户点击开始匹配；StartMatch transport 为 HTTP 200 / gRPC 0，但业务返回
`3006 ERR_TEAM_WRONG_STATE`、match_id=0。Team 日志给出 `reason=team_not_ready`：单人队仍为
FORMING、ready_count=0，故没有进入匹配 READY、DS allocation 或 Owner BATTLE Begin；客户端随后
正确回查并保持当前 Hub exact connection。该阻断发生在本事故修复路径之前，不作为 Hub P0
回归失败；按用户要求于 06:49:12 UTC 停止 PIE，后续匹配确认/Ready 问题另行诊断。

## 9. 部署、回滚与观察

- 修复 commit：未提交；工作树基线 `796da364`，不得把 dirty 源码冒充 commit。
- 构建产物：唯一标签 `g796da364-dirty-20260818-014129-inc20260818`；启动信息
  `commit=796da364`、`build_time=2026-08-18T05:41:47Z`、`go1.26.5`。标签明确带 `dirty`，
  不冒充提交。
- 部署时间与目标环境：本机 `pandora-agones/pandora`；Login 05:44:29 UTC、Hub/DS
  05:48:24 UTC、Owner 05:49:45 UTC 启动。
- 实际 Pod `imageID`：Login `sha256:4541995fe2e6…`、Hub `sha256:7f8fbccabe39…`、
  DS（2 副本）`sha256:569e8abb4a38…`、Owner `sha256:83c74ca71733…`；四服务 Ready、
  restart=0，全部旧 ReplicaSet desired=0。K8s 清单四处 image 已同步钉住该唯一标签，
  后续 apply 不会把服务退回旧镜像。
- 实际发布顺序：Login 先停旧再启新；Hub/DS 同时 scale-to-zero，确认旧 Pod 与
  `/pandora/writerlease/hub_allocator/writer/`、`/pandora/writerlease/ds_allocator/sweep/`
  租约均消失，于 05:47:51 UTC 起额外静默 10.4s（严格大于旧最大 5s Begin 预算），
  再启新 Hub/DS；确认新 writer 身份后最后停旧并启动 Owner。未发生新旧 allocator 混跑。
- 发布边界：上述是本机无在场玩家的 quiescent cutover，不是生产滚动发布证明。仓库规范要求
  新旧副本共存；当前协议没有 assignment source revision，普通混版滚更仍可被旧 binary 的
  在途 blind Begin 打破。生产发布阻断单列 INC-20260818-003，未解决前不得宣称 production-ready。
- 回滚条件和步骤：任一新服务出现 admission/Assign 5xx 持续上升或 assignment/Owner exact
  mismatch，停止后续 rollout；按记录的旧 immutable image 逆序回滚。若 Owner 已发布，不能单独
  回滚 Hub 到 sign-before-CAS 版本。
- 观察窗口、指标与结果：已完成。第二轮玩家 E2E 在 HUB confirmed 后观察 79.658 秒，
  `GetResumeContext` 新请求、额外 Travel、`owner_target_mismatch`、phase deadline 与恢复面板均为 0；
  Login、Hub Allocator、Hub DS、Owner Query/Admit、客户端五处为同一 assignment/epoch/target。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A-001 | P0 | 完成 DS blind retry/exact response 修复 | Codex | 已完成 | 本 Incident |
| A-002 | P0 | 收口 DS claim loser 在 Owner Begin 前交付 READY 的竞态 | Codex | 已完成 | 本 Incident；持久 unknown 恢复增强见 INC-20260818-002 |
| A-003 | P0 | 完成 Linux race、CAS loser/真 epoch barrier 交错与混版发布反例复核 | Codex | 已完成 | 本 Incident |
| A-004 | P0 | 构建 immutable 镜像，按安全顺序部署并记录 digest/Pod | Codex | 已完成 | 本 Incident；§9 |
| A-005 | P0 | 执行 UE 玩家 Login -> HUB exact ACK 与观察窗口 | Codex/用户协助操作 UE | 已完成 | §8.2；79.658 秒零复发 |
| A-006 | P1 | 增加连续同值 `owner_target_mismatch` 告警/指标 | 待指定 | 待处理 | 后续可观测性任务 |
| A-007 | P1 | 增加持久单调 assignment source revision，支持旧新 allocator 无停机共存 | 待指定 | 阻断生产发布 | INC-20260818-003 |
| A-008 | P2 | 下一源码批统一 Hub/Owner 遗留“四元组”注释为 exact target 五字段 | 待指定 | 待处理 | 仅术语，不影响本批运行判定 |
| A-009 | P1 | 扩展执行 HUB -> BATTLE -> HUB 玩家链，覆盖同批 DS 加固 | 待指定 | 阻断 | StartMatch 3006/team_not_ready；不阻断本机 Hub P0，生产发布前另行诊断 |

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据
- [x] 修复前失败、修复后通过的 Owner 真 MySQL 回归存在
- [x] race/代码级确定性故障注入达到本事故风险要求
- [x] 同类代码扫描完成（DS 长期持久恢复增强已单列 INC-20260818-002）
- [x] 目标环境已加载可追溯的新产物
- [x] 玩家 Hub 路径、同实例换发后端形状与退出补偿验证通过
- [x] 玩家 active 观察窗口 79.658 秒无复发
- [x] 剩余风险已解决或另建 Incident/行动项
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：Codex；本机 `pandora-agones` 的 Hub P0 已闭环。不得把本结论扩写为
production-ready：旧新 allocator 混版滚动仍由 INC-20260818-003 阻断，DS 持久 unknown 恢复与
扩展 BATTLE/回 Hub 玩家链分别由 INC-20260818-002、A-009 跟踪。
