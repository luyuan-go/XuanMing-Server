# [INC-20260814-001][P0] 匹配器隔夜幽灵票成局:玩家关机 16h53m 后其排队票仍被拿去与次日新玩家开局

> **状态**：已修复待部署（未关闭）
> **类型**：`availability`
> **环境**：内网测试环境（本机 k8s 集群 + 内网 Battle DS 192.168.2.46）
> **首次发生时间（UTC）**：2026-08-14 06:19:16（本地 UTC+8 14:19:16）
> **首次发现时间（UTC）**：2026-08-14 约 06:25（本地玩家 test3 报告"对面没人"）
> **负责人**：待指定
> **受影响服务/版本**：matchmaker（services/matchmaking/matchmaker）；连带 ds_allocator 位置投影的观测性失真
> **最后更新**：2026-08-14

> 本文时间线以 UTC 为主；原始日志时钟为本地 UTC+8，换算关系:本地时间 − 8h = UTC。

## 0. 一句话结论

玩家 test0052 于 2026-08-13 21:26（本地）排入匹配队列后直接关闭客户端离场；**全链没有任何一条路径会把"人已经走了"的非终态排队票转为终态**——16h53m 后（次日 14:19 本地）这张隔夜票被匹配器拿去与新玩家 test3 的新鲜票成局，拉起一台 Battle DS，test3 独自进图面对"幽灵对手"。修复=新增**排队票离线回收**（周期扫除 + 成局装箱前复查，判据复用 INC-20260813-001 的"离开了多久"证据链），代码与回归/变异测试已完成，未提交未部署。

## 1. 影响与范围

- 玩家影响：test3 进入一场对面无人的对局（1v1 形状，对手从未连接 DS）；对局无法正常进行，需等 DS 到齐期限判弃或手动退出。
- 影响人数/对局/请求数：确认 1 场（match_id=23739104583778304），2 名玩家名下资源被占用（test3 被冻进幽灵局；test0052 的 claim 被旧票占着，若此刻回来重排会被拒）。
- 服务影响：为幽灵局白白分配一台 Battle DS；locator 把两名玩家都投影为 BATTLE（state=5），其中 test0052 实际离线 16h+——**观测面撒谎**，放大排查成本。
- 数据与安全影响：无数据损坏、无安全边界突破。
- 开始/结束时间：票据入队 2026-08-13 13:26:32 UTC；成局 2026-08-14 06:19:16 UTC；对局随后由人工/超时收敛。
- 是否仍可复发：**是**（修复未部署）。任何玩家排队后退出客户端，其票据都会无限期滞留队列，随时可能与真人成局。
- 严重级别判定理由：玩家无法正常对局 + 每次复发浪费一台 DS + 位置投影失真误导运维,符合 P0 可用性口径。

## 2. 第一现场与证据

### 2.1 症状

- 客户端症状：test3 匹配成功进图后对面始终无人；对手头像/名字显示为 test0052。
- 服务端症状：matchmaker 正常打出 `match_found players=2`（无任何错误日志——所有服务都认为自己成功了）；Battle DS（192.168.2.46:7800）的 UE 日志里只有 test3 一个人的 `InitNewPlayer`，test0052 的从未出现。
- K8s/Agones 状态：DS 正常 Allocated，无崩溃、无重启。

### 2.2 原始证据

事实链（来源：matchmaker 结构化日志、Redis 票据/claim 快照、Battle DS UE 日志、player_locator 投影查询；原始日志留存于内网日志平台，按 match_id/ticket_id/player_id 检索）：

1. **旧票入队**：ticket_id=23478116601069568（单人票，成员 test0052=23478082241331200）EnqueuedAtMs 对应 2026-08-13 21:26:32(+8)。
2. **玩家离场**：test0052 最后一次显式断开连接在 2026-08-13 21:36:48(+8)；此后 last_alive/left 基线再无刷新——离场证据链完整。
3. **隔夜成局**：2026-08-14 14:19:16(+8) matchmaker 打出 match_found，match_id=23739104583778304，成员为 test3（新鲜票，23397573079367680）与 test0052 的隔夜票——**距其票据入队 16h52m44s，距其离场 16h42m28s**。
4. **DS 单人进图**：Battle DS 192.168.2.46:7800 的 UE 日志仅有 test3 的 `InitNewPlayer`；roster 里两人，实到一人。
5. **投影失真**：player_locator 查询显示 test3 与 test0052 **均为 state=5(BATTLE)**——因为 ds_allocator 的 `RefreshBattleLocations` 按对局 canonical roster 全员刷新位置，不看谁真的连上了 DS。
6. **无关者排除**：test4（23738911310249984）同时段在线但不在本场 roster,与本事故无关。

```text
（脱敏事实摘录,非逐字日志）
2026-08-13 21:26:32(+8)  matchmaker  ticket enqueued   ticket=23478116601069568 members=[23478082241331200]
2026-08-13 21:36:48(+8)  gateway     test0052 断开连接（此后无任何重连/心跳）
2026-08-14 14:19:16(+8)  matchmaker  match_found       match=23739104583778304 tickets=[23397573079367680 的新票, 23478116601069568]
2026-08-14 14:19:2x(+8)  battle-ds   InitNewPlayer     仅 test3;test0052 从未出现
```

### 2.3 已排除的噪声

- **不是 INC-20260813-001 的"打完一局队友被冻进下一局"**：那起事故的缺席者是正常玩家处于换局窗口；本起的缺席者已经**离线 16h+**,形状完全不同。
- **不是 test4 的问题**：test4 不在本场 roster。
- **不是 locator 查询报错**：locator 响应正常,只是它如实转述了 ds_allocator 按 roster 刷出来的"两人都在战斗"——投影语义（routed vs admitted vs active 不区分）是独立缺陷,列入行动项 A-2。
- **不是票据 TTL 失效**：排队中票据的 TTL 会被持续续期,这是**设计如此**（防止排队中被误清）;`ticket_ttl=30m` 只约束终态后的残留。

## 3. 时间线

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 08-13 13:26:32 | matchmaker | test0052 单人票 23478116601069568 入队 | 票据 EnqueuedAtMs |
| 08-13 13:36:48 | gateway/locator | test0052 断开连接,离场基线定格 | 网关断连日志 / last_seen 基线 |
| 08-13 13:37 ~ 08-14 06:19 | matchmaker | 票据滞留队列 16h+;liveness 扫除关闭,无任何链路判死 | 配置 `liveness_gate_enabled=false` |
| 08-14 06:19:16 | matchmaker | 隔夜票与 test3 新票成局 match=23739104583778304 | match_found 日志 |
| 08-14 06:19:2x | battle-ds | 仅 test3 InitNewPlayer;test0052 缺席 | UE 日志(192.168.2.46:7800) |
| 08-14 06:19+ | ds_allocator | RefreshBattleLocations 把 roster 全员刷成 BATTLE,含离线 16h 的 test0052 | locator 查询 state=5 |

## 4. 调用链与关键变量

```text
matchTickOnce
  → RangeQueueTickets              # 隔夜票与新票同池
  → formMatchesInPool / formSoloMatch
  → (修复前:无任何在场性检查) → idGen.Generate → ReserveTicketsForMatch
  → allocate DS → match_found → RefreshBattleLocations(全 roster)
  → DS: 只有 test3 InitNewPlayer → 幽灵局
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| 排队票(非终态) | StartMatch → AddTicket | matchmaker Redis;**排队中 TTL 持续续期,事实上永生** | 共享 | 玩家离场后无人回收,滞留 16h53m |
| player claim | ClaimPlayer | 与票同生命周期 | 共享 | 占住 test0052 的重排资格 |
| `liveness_gate_enabled` | conf | 全环境 false(INC-20260724-001 回退) | — | 唯一可能判死排队票的旧机制处于关闭且**不可重开**(结构性假阳性) |
| last_seen 基线(left_at_ms→last_alive_ms) | player_locator | Hub DS census 心跳每 5s 刷新**全员** | 共享 | 修复所依赖的"离开了多久"证据链(INC-20260813-001 ③④) |

## 5. 根因

### 5.1 直接根因

**非终态排队票没有任何"人已离场"的回收路径。** 四个环节共同构成充分条件,全部有代码级证据：

1. **票据设计为排队期永生**：排队中票据 TTL 被周期续期（防误清）,`ticket_ttl` 只约束终态残留——离场玩家的票无限期滞留。
2. **liveness 判死门全环境关闭**：`liveness_gate_enabled=false`,系 INC-20260724-001 的回退（MATCHING 投影零保活 → 30s TTL 后 100% 假阳性）,且该门**不能简单重开**——投影缺 renewal/release 路径的结构性缺陷未修（其 D2/A-8 仍 OPEN）。
3. **team 侧刻意不联动**：掉线软档（offline_leave）设计上不取消匹配、且跳过单人队（有测试锁定 `TestOnPlayerPresenceLost_单人队不动`）——单人排队票在 team 侧是盲区。
4. **成局前无在场性检查**：修复前 formMatch/formSoloMatch 装箱只看 MMR/冷却/人数,不问"这些人还在不在"。（INC-20260813-001 新增的 `ensureAllPresent` 挂在 **StartMatch 入队时**,管不到已在队里的票。）

### 5.2 触发条件

- 玩家排队后退出客户端（不点取消匹配）——极常见的用户行为;
- 队列长期低水位（内网测试环境）,旧票不会被迅速消耗,而是滞留到次日与新玩家配对。

### 5.3 故障放大因素

- **全链零错误日志**：matchmaker/allocator/DS 各自都"成功"了,只有玩家知道不对。
- **locator 投影按 roster 刷新**：离线 16h 的人被投影成 BATTLE,排查时第一直觉被带偏。
- match_found 日志（修复前）不含票龄,无法一眼看出"这张票是昨天的"。

### 5.4 为什么现有保护没有挡住

| 保护 | 为什么无效 |
|---|---|
| liveness 扫除/成局门 | 全环境关闭(INC-20260724-001 回退),且判据是"此刻查不查得到"——重开必复发假阳性 |
| `ensureAllPresent` 在线闸(INC-20260813-001 ④) | 挂在 StartMatch **入队时**;本事故的票入队时人是在线的 |
| team offline_leave | 设计上不取消匹配 + 跳过单人队,双重不覆盖 |
| ticket TTL | 排队中被续期,契约上就不负责"人走了" |
| DS roster 到齐期限(INC-20260813-001 ⑥) | 只能事后判弃止损,DS 已经白拉了,test3 已经进图了 |

## 6. 全仓同类问题扫描

- 扫描基线：当前工作树（含 INC-20260813-001 未提交批次）。
- 扫描范围：matchmaker `internal/biz` 全部成局路径与队列消费者。
- 结论：
  - **Confirmed 覆盖**：`formMatchesInPool → formMatch`（组队/常规池）与 `formSoloMatch`（walk-in/solo 直进,**正是本事故形状**）是仅有的两条装箱路径,修复后均过 `rejectAbsentTickets` 复查;周期扫除 `queueAbsenceSweepOnce` 覆盖两者之外的滞留窗口。
  - **同型但另案**：PVE matchmaker 复用同一套代码,随本修复一并生效（etc/matchmaker-pve.yaml 已配）。
  - **已排除**：team 侧单人队不动的行为**保持不变**（有测试锁定,是刻意设计）——回收职责收敛到 matchmaker 自身,不再指望 team 联动。
  - **未覆盖边界**：locator BATTLE 投影语义失真（A-2）;MATCHING 投影零保活(INC-20260724-001 D2/A-8)本修复刻意绕开而非修复。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 手动清理该幽灵局与滞留票 | 已完成(内网) | match/ticket 已不存在 | — |

### 7.2 永久修复（已落码,未提交未部署）

设计取舍：**不重开 liveness 门**（结构性假阳性）,**不走事件推送**（仓库纪律:拉取校验而非事件推送）,新建独立的"排队票离线回收"链,判据复用 INC-20260813-001 的证据链——**按"离开了多久"判,绝不按"此刻查不查得到"判;UNKNOWN 永不冒充 OFFLINE(§9.22)**。

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| ① 新配置 `queue_absence_reap_after`(判死窗,默认 120s;负值关闭;必须 > start_presence_grace 30s、< team offline_leave.threshold 180s) | 已落码 | `internal/conf/conf.go`;etc/matchmaker-{dev,pve}.yaml、prod.example | 负值关闭有测试 |
| ② 判据原语 `absentBeyond`(从 ensureAllPresent 抽出共享:在线放行;无基线 UNKNOWN 放行;离场 ≥ 窗才判死) | 已落码 | `internal/biz/match.go` | 原在线闸测试全数保持绿 |
| ③ 周期扫除 `queueAbsenceSweepOnce`(挂 livenessSweep 同节拍;**弱依赖**——presence 查询失败整轮跳过,绝不在不确定时删票;回收=DeleteTicketIfUnmatched CAS + 释放 claim + FAILED 推送) | 已落码 | 同上 | 5 个测试 |
| ④ 成局装箱前复查 `rejectAbsentTickets`(formMatch + formSoloMatch 各一道,堵扫除节拍间隙;**fail-open**——presence 故障照常成局,不给 locator 抖动阻断全部成局的权力,真离线由 DS roster 到齐期限兜底) | 已落码 | 同上 | 3 个测试 |
| ⑤ 可观测性:match_found 增补 `ticket_ids`/`oldest_ticket_age_ms`,票龄 >10min 打 `stale_ticket_matched` 告警;solo 同理 | 已落码 | 同上 | oldestTicketAgeMs 单测(旧票无字段→0 不误报) |

**部署依赖（关键）**：判据证据链（`PresenceReader.BatchLastSeen`=left_at_ms 回退 last_alive_ms、Hub census 心跳刷**全员**基线）来自 INC-20260813-001 的未提交批次（该批次自身处于复核阻断 A-11）。**两批必须同车部署**;若本修复单独上线,presence 未接线时扫除/复查自动 no-op（安全降级,但零保护）。

### 7.3 防复发规则

- 判死判据统一收敛到 `absentBeyond` 单点——后续任何"人在不在"的判断禁止绕开它另起炉灶。
- 新增排队相关配置必须写明与相邻窗口(start_presence_grace / offline_leave.threshold)的偏序关系。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| 针对性单测(10 个,含事故端到端复现 `TestMatchOnce_幽灵票不得与新玩家成局`) | 不存在 | 全绿 | `go test ./internal/biz/ -count=1`(matchmaker 模块) | queue_absence_reap_test.go |
| **变异测试**(回收整条关闭 `queueAbsenceReapAfter()→-1`) | — | 4 个用例命中失败(断言是活的) | 同上 | 本档记录 |
| 防倒退:UNKNOWN 放行 / presence 故障不删票 / 窗内不回收 / fail-open 照常成局 | — | 全绿 | 同上 | 同上 |
| 存量回归(biz+service 全包,含 INC-20260813-001 在线闸测试) | 绿 | 绿(ensureAllPresent 重构无回归) | `go test ./internal/biz/ ./internal/service/ -count=1` | 本档记录 |
| `go test -race`(matchmaker 全包,Linux 容器) | — | **全绿**(cmd/biz/conf/data/service 五包 ok,同一命令一次通过) | `docker run … golang:1.26.5 go test -race -count=1 ./services/matchmaking/matchmaker/...` | 见下方内联输出 |

`-race` 原始输出（golang:1.26.5 容器,2026-08-14）：

```text
ok  github.com/luyuancpp/pandora/services/matchmaking/matchmaker/cmd/matchmaker      1.589s
ok  github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/biz        5.014s
ok  github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf       1.735s
ok  github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/data       1.845s
?   github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/model      [no test files]
?   github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/server     [no test files]
ok  github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/service    1.677s
```

| 集成回归/真集群 E2E(排队→关客户端→等 120s→验证票回收) | 未执行 | 未执行 | — | 阻断:未部署 |
| 玩家 E2E | 未执行 | 未执行 | — | 阻断:未部署 |
| 观察窗口 | — | 为零 | — | 未部署 |

## 9. 部署、回滚与观察

- 修复 commit：**未提交**(本仓库约定 Claude 不执行 git 提交;与 INC-20260813-001 批次同车)。
- 构建产物/镜像 digest：未构建。
- 回滚条件和步骤：**纯配置回滚**——`queue_absence_reap_after: "-1s"` 即整条关闭,无需回滚镜像;若出现误回收(理论上仅当离场基线被错误刷新),立即回滚配置并升级本档。
- 观察窗口、指标与结果：待部署。上线后观察 `queue_absence_reaped_ticket`(回收量与 via 分布)、`stale_ticket_matched`(应归零)、`queue_absence_sweep_skipped`(presence 弱依赖故障率)。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A-1 | P0 | 与 INC-20260813-001 批次同车提交/部署/真集群验证(该批次复核阻断 A-11 需先解除) | 待指定 | OPEN | INC-20260813-001 |
| A-2 | P1 | locator BATTLE 投影语义失真:RefreshBattleLocations 按 roster 全员刷新,不区分 routed/admitted/active——观测面撒谎放大一切战斗类事故的排查成本 | 待指定 | OPEN | 本档 |
| A-3 | P1 | MATCHING 投影零保活致 liveness 门永久关闭——补 renewal/release 路径 | 待指定 | OPEN(既有) | INC-20260724-001 D2/A-8 |
| A-4 | P2 | 120s 判死窗现场校准(与 start_presence_grace=30s、offline_leave.threshold=180s 的偏序需在真实网络下验证) | 待指定 | OPEN | 本档 |
| A-5 | P2 | ~~`-race` 合并跑全绿证据补记~~ 已完成:matchmaker 全包一次通过(见 §8) | — | 已完成 | 本档 §8 |

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据
- [x] 修复前失败、修复后通过的回归存在(变异测试证明断言可击杀)
- [ ] race/集成/故障注入达到本事故风险要求(-race 全绿;集成/故障注入未执行)
- [x] 同类代码扫描完成(两条装箱路径 + PVE 复用面均覆盖)
- [ ] 目标环境已加载可追溯的新产物(未提交未部署)
- [ ] 玩家路径、恢复和补偿路径验证通过
- [ ] 观察窗口无复发
- [ ] 剩余风险已解决或另建 Incident/任务(A-1~A-5 OPEN)
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭。
