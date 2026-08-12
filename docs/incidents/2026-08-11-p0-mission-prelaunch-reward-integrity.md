# [INC-20260811-001][P0] mission 域上线前审计:五类会导致错发 / 漏发 / 永久卡死的奖励与进度缺陷（near-miss）

> **状态**：已修复待部署
> **类型**：`near-miss`（`data`）
> **环境**：本机进程 / 本机 TiDB（**未部署到任何环境**，见 §1）
> **首次发生时间（UTC）**：无 —— 从未在任何环境运行，不存在线上发生
> **首次发现时间（UTC）**：2026-08-11（代码审查与逐条取证，非线上告警）
> **负责人**：待指定
> **受影响服务/版本**：`mission`（20019），commit `b65d5cdb` → `e27ffc63`；镜像 `pandora/mission:dev`（2026-08-11 07:53 构建，未部署）
> **最后更新**：2026-08-11

## 0. 一句话结论

mission 域移植批次在**上线前**的审计中发现五类缺陷，任一若上线都会造成 P0 级后果：白送奖励、同一份奖励发两次、奖励永久发不出去、任务永久无法完成、以及并发穿透接取上限。五类均已落码修复并有「修复前必红」的回归测试；**因该服务从未在任何环境部署，线上零影响**，故按 `docs/incidents/index.md` §1 第三条以 near-miss 建档。

**为什么合并为一个 Incident**：这不是五次独立事故，而是**同一个事件**——「mission 批次在上线前被审计」。五条有各自的根因，因此在 §5 分节独立取证；但它们共享同一时间点、同一发现途径、同一影响面（零）。为五个从未运行过的缺陷各建一份"事故"档案会把审计发现伪装成五起生产事故，违反 §1「不得伪装成线上事故」。

## 1. 影响与范围

- 玩家影响：**无**。取证：`kubectl get all -n pandora` 返回 `No resources found`；`docker ps` 无任何 pandora 业务容器；`battle_result` 的 `mission_addr` 在所有环境均未配置（未配即不产事实，见 §5.5），任务进度链从未通电。
- 影响人数/对局/请求数：0。
- 服务影响：无。
- 数据与安全影响：无（`pandora_mission` 库在任何环境都没有生产数据）。
- 开始/结束时间：不适用。
- 是否仍可复发：**是**——修复未部署（本服务尚未首次部署）。首次上线前必须带上本批修复。
- 严重级别判定理由：五条中有三条直接违反 §9 不变量 2/7（结算幂等、扣减有补偿幂等键）与 §9.6（派生数值服务端权威），后果是玩家资产错误且**不可逆**（多发的道具无法安全回收，白送的奖励无法追回）。按"若上线会造成的后果"定级 P0。

## 2. 第一现场与证据

### 2.1 症状

不适用——无线上症状。发现途径是代码审查 + 逐条取证复核，不是告警或玩家反馈。

### 2.2 原始证据

证据形态是**测试**而非日志：每条缺陷都有一个「把代码改回旧写法即变红」的回归用例（§8）。示例（GT 比较符活锁，退掉钳位修复后）：

```text
--- FAIL: TestGreaterThanConditionCompletesInsteadOfLivelocking (0.00s)
    mission_test.go:918: 杀了 8 只(目标 >5)任务仍未完成,进度钉死在 [5] —— 钳位把达标打回了未达标
```

```text
--- FAIL: TestValidateMissionCrossTables_EquipmentTotalAcrossEntries/多条装备合计超上限拒批次
    mission_cross_test.go:175: 装备累计 66 件超上限 64,必须拒批次
```

### 2.3 已排除的噪声

- `services_dsticket_secret` / `gen_cluster_b1` / `ds_entrypoint_log_redaction` 三条 PowerShell 契约测试红：**与本批次无关**，在干净 HEAD 上同样红（PROGRESS.md 2969/2980/3000 已用 `git worktree --detach HEAD` 对照证实）。本次未复验（复验 agent 因 API 连接错误中断），故仍列为 §10 行动项。
- `services/account/player/internal/biz/player.go:802 unknown field InstanceId`：**并发编辑者在途改动**——`player.proto` 已加 `instance_id = 3`（工作区未提交），但 `player.pb.go` 未重生。与本事故无关，见 §10。

## 3. 时间线

无线上时间线。审计与修复均在 2026-08-11（UTC）当日完成，属开发期活动。

## 4. 调用链与关键变量

```text
ReportMissionFacts (系统 RPC)
  → biz.ReportFacts            ← ①配置批次快照在此取（P1 修复点）
  → repo.ApplyFactsTx          ← 收据幂等 + 每玩家守卫行点锁（④修复点）
      → biz.applyFactsEngine
          → progressMission    ← ②钳位在此写回进度（GT 活锁点）
          → allConditionsFulfilled  ← ③热更加条件的达标判定
          → buildRewardLog     ← ⑤发放形态冻结位在此写快照
  → grantEntriesBestEffort → deliver → inventory / player / mail
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| `Catalog` 批次快照 | 修复前：每次方法调用各取一次 `Store.Tables()` | 修复前无所有者 | 是（reload 原子换指针） | 一次事务内可读到两个批次，奖励内容与冻结位出身不同批次 |
| `am.Progress[i]` | `player_mission_active.progress` pb | 单玩家，事务内 FOR UPDATE | 否 | 钳位写回点；GT 下被钳成不达标 |
| `MissionRewardItem.equipment` | `buildRewardLog` 落快照时 | 随 `reward_pb` 永久保存 | 否 | 冻结发放路由（`:inst` vs `:stack` 幂等键） |

## 5. 根因

### 5.1 直接根因（五条，各自独立）

**① 热更给已上线任务加条件 → 白送完成并发奖**
`progressMission` / `allConditionsFulfilled` 两处都取 `min(len(condition_ids), len(progress))`。任务原本单条件 2/3，热更加第二个条件后，下一条属于条件 1 的事实把槽 0 推到 3 → 达标判定只看 `min=1` 个槽 → 判定全条件满足并发奖，**新条件一次都没被检查过**。
注意这个 `min` 是从 C++ 原版 `mission.cpp:155` **忠实移植**的：D 版那个 `min` 覆盖的是"条件行查不到就不加槽"，而 Go 侧 accept 恒等长，唯一造成短槽的路径（配置增列）在原版不存在。**忠实移植不改变它在新语境下是 bug 的事实**——这是"照搬即正确"的思维盲区。

**② GT 比较符：钳位把达标打回不达标 → 任务永久活锁**
`ConditionClampIfFulfilled` 无脑钳到 `target`。进度是单调不减的累加器且达标槽不再累加，于是 `target=5` 的 GT 条件：进度 6 达标 → 钳回 5 → 再判 `5 > 5` 为假 → 不完成；下一条事实推到 6 又被钳回 5。**进度永久钉死在 5，任务 100% 完不成**，全程零日志零错误码。
同源问题：LE/LT 在 `progress=0` 时即为真（该槽永不累加、恒定达标 = 白送）；EQ 是单点集合，`amount>1` 一步跨过目标后永远不再相等。不变量是：**在单调累加计数器上，达标集合必须向上闭合**，只有 GE/GT 满足，且 GT 的最小达标值是 `target+1` 而非 `target`。

**③ 发放形态未冻结 → 滚动升级 / 配置热更期同一奖励用两个幂等键各发一次**
装备走 `GrantInstances`（`:inst` 键）、堆叠走 `GrantItems`（`:stack` 键），**两个键在 inventory 台账里互不相识**。原实现在发放时才回读道具表：首投走 `:stack` 成功后，若 `MarkReward` / 经验段 / 进程崩任一失败使行留非 GRANTED，期间道具被热更改成装备，补扫重放就换成 `:inst` 键 → inventory 查无此键 → **同一份奖励发两次**。部分翻转时反而撞 `claimLedger` 请求指纹冲突 fail-closed → **奖励永久发不出去**。

**④ TiDB 无 gap 锁 → 接取上限与类型互斥被并发穿透**
`SELECT ... FROM player_mission_active WHERE player_id=? FOR UPDATE` 在该玩家零活跃行时**一把锁都不加**。真 TiDB 实测：去掉守卫行 → 上限 3 放过 **12** 条、互斥 **8** 条全活。

**⑤ 装备件数上限只判单条，不判整条奖励累计 → 奖励永久发不出去 + 无界 FAILED 行**
加载期闸的是「单条道具的装备数量 ≤ 64」，而发放侧 `deliver` 闸的是「整条奖励展开出的 instance 切片总长 ≤ 64」。两者口径不一致，中间夹着一整类「加载期全绿、运行期永远发不出去」的配置：10 个不同装备各 64 件 = 640 件整批过审 → 落进 `reward_pb` 快照 → 任务同事务置 CLAIMED → `deliver` 在累计闸上必失败。快照是发放唯一入参、不回读配置表，**改表也救不回在途行**：玩家永久损失该任务全部奖励（含经验，装备失败先 return 走不到经验段）；补扫每轮重试一次且无上限；`SweepRewardLog` 只清 GRANTED，这些 FAILED 行**永不清理**，同时把"陈年 FAILED = 发放链有 bug"的审计信号淹没。

### 5.2 触发条件

| # | 触发条件 | 是否需要罕见时序 |
|---|---|---|
| ① | 给**已被玩家接取**的任务热更加一个条件 | 否，策划日常操作 |
| ② | 策划在条件表「比较符」列填 1(GT) / 2(LE) / 3(LT) / 4(EQ) | 否，该列已在 proto 注释里对策划公开 |
| ③ | 发放首投部分失败 + 期间道具 `equip_slot` 被热更改动（或滚动升级期新旧副本加载不同批次） | 是 |
| ④ | 同一玩家两个并发 AcceptMission，且该玩家当前零活跃任务 | 否，双击即可 |
| ⑤ | 一条奖励配多个装备且合计 > 64 件 | 否，配置手滑 |

### 5.3 故障放大因素

- ②③⑤ 全程**零日志零错误码**，线上表现与配置正确完全无法区分，只能靠玩家投诉发现。
- ⑤ 的 FAILED 行不被保留期清理，随时间无界增长并淹没审计信号。
- ③ 的下游幂等记录过保留期（90 天）后再重放，就从"幂等吸收"变成**真重复发放**。

### 5.4 为什么现有保护没有挡住

- **幂等键防不住 ③**：因为两次发放压根不是同一个键（`:stack` vs `:inst`）。幂等只在同键内成立。
- **`FOR UPDATE` 防不住 ④**：TiDB 悲观事务无 gap/next-key 锁，零行时该语句不加任何锁。这是 MySQL 与 TiDB 的真实语义差异，不是用法错误。
- **加载期跨表校验防不住 ⑤**：它逐条判，而运行期按整条累计判，两个口径不同。
- **配置热更流水线（§9.15）防不住 ①②**：它保证"整批加载成功才切换"，但这两条缺陷的配置**本身是合法的**——语法、外键、链环全过。
- **补扫防不住 ⑤**：补扫是重放机制，重放一个必然失败的坏快照只会稳定地失败下去。

## 6. 全仓同类问题扫描

- 扫描基线 commit：`e0a10786`
- 扫描目录和文件类型：`services/**/*.go`、`pkg/configtable/*.go`、`deploy/**/*.yaml`
- 搜索模式/工具：`MarkReward` 无条件 UPDATE；`FOR UPDATE` 零行路径；出箱 `DELETE ... WHERE id` 丢弃 `RowsAffected`；`min(len(...), len(...))` 型槽位截断
- **Confirmed 同型命中**：
  - `services/social/leaderboard/.../leaderboard_repo.go MarkReward` 与 mission 修复前同款**无条件 UPDATE**（GRANTED 可被并发副本打回 FAILED）。**属另一服务、本次未动**，见 §10 行动项 A-1。
  - `player_push_outbox` 发布器与 mission 推送出箱同款「全局未分区表 + 客户端可见全量快照」，多副本并发会让旧帧覆盖新帧。**本次已一并修复**（§7.2）。
- **已排除项及理由**：
  - `battle_result` 的 `player_update_outbox`：下游靠 `mmr_history` 唯一键幂等，乱序与重复天然无害，且无客户端可见的覆盖语义 → **有据地不接选举**，不是漏改（§15.3）。
- **结构性隐患**：「忠实移植 C++ 语义」在 Go 侧语境改变后可能变成 bug（根因①）。已写入 §7.3。
- **未覆盖边界**：其余 20 个服务的出箱发布器未逐一核对是否也是"未分区表 + 全量快照"组合。列为 §10 行动项 A-2。

## 7. 处置与永久修复

### 7.1 临时止血

不适用：服务从未部署，无线上流量可止。

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| ① 进度槽补零扩容 + 达标按配置全槽判、槽数不足 fail-closed | 已落码 | `biz/mission.go alignProgressSlots` / `allConditionsFulfilled` | `TestHotAddedConditionIsNotSkipped`（改回旧写法必红） |
| ② 钳位落到「最小达标值」 + 加载期拒绝非向上闭合比较符 | 已落码 | `pkg/configtable/condition.go ConditionMinFulfillingProgress`、`mission.go ValidateMissionCrossTables` | `TestGreaterThanConditionCompletesInsteadOfLivelocking`、`TestValidateMissionCrossTables_RejectsNonUpwardClosedComparator`（先红后绿实测） |
| ③ 发放形态冻结进快照 | 已落码 | `MissionRewardItem.equipment`（optional，缺省回退读表 + WARN） | `TestDeliverUsesFrozenRouteAcrossCatalogFlip` 等三条 |
| ④ 每玩家守卫行点锁 | 已落码 | `mission_player_guards` + `acquirePlayerGuard` | 真 TiDB 实测 12→3 / 8→1 |
| ⑤ 装备件数改**整条奖励累计**判，与发放侧同口径 | 已落码 | `pkg/configtable/mission.go` | `TestValidateMissionCrossTables_EquipmentTotalAcrossEntries`（先红后绿实测） |
| 附：配置批次快照按操作钉死（防③的同类撕裂） | 已落码 | `biz.CatalogSource` / `cmd/mission/configtable.go batchCatalog` | 全模块 build/vet/test 绿 |
| 附：`GRANTED` 不可被 FAILED 覆盖 | 已落码 | `MarkReward` 失败标记带 `status <> 1` | biz 用例 |
| 附：推送出箱单写者选举（mission + player） | 已落码 | `push_writer_lease`（conf + writerlease + 两道机械门禁） | `TestPushPublisherOnlyRunsOnLeader` 等 3 条 + 两组清单契约测试 |
| 附：完成集读取侧 SQL LIMIT + 任务表行数上限 | 已落码 | `doneReadLimit=2000` / `configtable.MaxMissionRows=2000` | 全模块 test 绿 |

### 7.3 防复发规则

- `CLAUDE.md` §9.18 受管列表：新增「已完成任务列表」行，写明**写入侧上限与读取侧上限必须同时存在**，并写明事务路径为何刻意不加 LIMIT（截断会让已完成任务被判成可重新接取 → 重复发奖）。
- `docs/design/infra.md`：补登记 `mission_player_guards`；`player_mission_done` / `mission_push_outbox` 的"有界"依据改为指向真实存在的常量与机制。
- **新增纪律（写入本档 §5.1①，待提炼进 `CLAUDE.md`）**：从其它代码库移植语义时，必须逐条复核"原版该写法所依赖的前提在新语境下是否仍成立"。忠实移植 ≠ 正确。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| 针对性单测（①②③⑤ + 推送单写者） | **红**（逐条实测退回旧写法） | 绿 | `go test ./...`（mission、pkg/configtable） | §2.2 两条失败原文 |
| 加载期门禁（比较符 / 装备累计 / 行数上限） | 红 | 绿 | `go test ./configtable/ -run MissionCrossTables` | 5+2 个子用例全绿 |
| 清单契约（Deployment ↔ annotation ↔ env ↔ 产物） | 不存在 | 绿 | `go test ./cmd/mission/` | 3 条全绿 |
| 集成回归（真实 SQL 引擎） | 红（12/8 穿透） | 绿（3/1） | `PANDORA_TEST_TIDB_DSN=... go test ./internal/data/` | tidb 子测试 PASS |
| 完成集读写侧上限（只读截断 / 事务不截断） | 红（变异:两条路径都截断 → `事务路径必须看到全部 6 行,实为 2`） | 绿 | 同上，`-run DoneReadLimit` | 真 TiDB 实测先红后绿 |
| 四条数组/行数上限（任务表行数 / 后续链 / 槽位取值 / 奖励条目） | 无覆盖（本轮补） | 绿 | `go test ./configtable/ -run TooMany` | 5 个用例含边界值放行 |
| **`push_writer_lease.mode=enforce` 分支** | — | **未执行** | 需 etcd（本机 2379/2380 均未监听） | 只验了 `pushIsLeader()` 的 fake 租约 + 清单/产物契约；`writerlease.Start()` 真实选举路径**一次都没跑过** |
| 真实 MySQL 后端 | — | **未执行** | 需 `PANDORA_TEST_MYSQL_DSN` | 本机只跑了 TiDB 容器；mysql 子测试 SKIP |
| `go test -race` | — | **未执行（阻断项）** | 需 Linux + CGO | CI 唯一测试入口 `ci_backend.ps1:50` 无 `-race`，Jenkins 跑 Windows；本机 `CGO_ENABLED=0` 下 `-race` 直接报错 |
| fatal/OOM/SIGKILL 重启注入 | — | **未执行** | — | 服务未部署，无注入对象 |
| 玩家 E2E | — | **未执行（物理上不可执行）** | — | 客户端零 mission 接线：无 cpp pb、无 `Module/Mission`、无任何 RPC 调用点，仅有 7 张图标贴图 |

未执行项一律保留在表中，不得删除。

## 9. 部署、回滚与观察

- 修复 commit：`b65d5cdb`、`e27ffc63` + 本轮工作区改动（**未提交**）
- 构建产物/镜像 digest：`pandora/mission:dev`，2026-08-11 07:53:36 构建；**该镜像早于本轮修复，必须重建**
- 部署时间与目标环境：**尚未部署**
- 实际 Pod `imageID`：无
- 回滚条件和步骤：首次上线，回滚 = 撤下 Deployment + 把 `battle_result.mission_addr` 置空（未配即不产事实，不留半截数据）
- 观察窗口、指标与结果：**为零**（无部署即无观察对象）。上线后至少需要：`mission_reward_grant_deferred` / `mission_reward_retry_failed` 速率、`mission_push_publish_raced` 计数、`mission_push_outbox` 行数、`pandora_db_budget_violations_total`。当前 `deploy/grafana` 下 5 条告警规则中 mission 相关 **0 条**。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A-1 | P1 | ~~`leaderboard_repo.go MarkReward` 同款无条件 UPDATE~~ **已修**:失败标记加 `status <> GRANTED` 守卫，抽出 `buildMarkRewardSQL` 并补两个子用例（对齐本文件既有 `buildSaveSnapshotSQL` 写法） | — | 已解决 | 本档 §6 |
| A-2 | P2 | ~~逐一核对其余出箱发布器~~ **已完成**:全仓 8 张出箱表逐张判定，只有两张 push 出箱满足「未分区 + 客户端可见全量快照 + 下游无幂等」三条判据（均已修）；其余六张的「不接」是有据差异。归档 `docs/reviews/2026-08-11-outbox-single-writer-audit.md` | — | 已解决 | 本档 §6 |
| **A-10** | **P1** | **placement 分权 key 是设计过但从未落地的能力**:生成器有参数/解析/注入循环，但 `$PlacementSecretBindings` 是空数组，且 **Go 侧零 conf 字段**（全仓 grep `placement_secret`/`PlacementSecret` 零命中），注入无处可注。它在 `gen_cluster_b1` 里的断言恒红，并**遮蔽了其后 14 条断言**（其中 match_resume_auth / allocation abort 分权是真实现了的）。2026-08-11 已隔离为显式 TODO 让真门禁恢复执行；落地须按「先落 conf 字段 + 生成器绑定 + locator 验签，再把断言改回 Assert-True」的顺序 | 待指定 | 未开始 | 本档 §6 |
| **A-11** | **P1** | ~~`login.matchmaker.auth_secret` 未登记进 `$MatchResumeAuthSecretBindings`~~ **已修**。**这是 A-10 遮蔽出来的真实生产缺陷**:该 key 与 `matchmaker.match.match_resume_auth_secret` 必须成对同值，漏绑导致运维用 `-MatchResumeAuth <生产密钥>` 时 matchmaker 拿生产密钥而 **login 留 dev 密钥** → ①login→matchmaker `ResolvePlayerMatchContext` 全部被拒（2026-07-15 P0 修复的兜底路径静默失效）②dev 密钥进生产 | — | 已解决 | 本轮实测 |
| A-3 | P1 | `go test -race` 需要 Linux + CGO CI 轨；当前是**阻断项**，不得写成已验证 | 待指定 | 未开始 | CLAUDE.md §16.7 |
| A-4 | P1 | CI 无法区分 SKIP 与 PASS：真库用例未设 DSN 时 `t.Skipf` 而整体 `EXITCODE=0` | 待指定 | 未开始 | 本档 §8 |
| A-5 | P1 | 客户端 mission 接线（cpp pb 同步 + `Module/Mission`），是玩家 E2E 的前置 | 待指定（Codex + 用户编译 UE） | 未开始 | CLAUDE.md §5.2 |
| A-6 | P2 | 三条 PowerShell 契约测试仍红（`services_dsticket_secret` / `gen_cluster_b1` / `ds_entrypoint_log_redaction`）。**2026-08-11 已用 `git worktree --detach HEAD` 复验：三条在干净 HEAD 上同样 FAIL，与本批次改动无关**（同批 `configtable_mount` / `local_k8s_profile` / `gen_cluster_session_gate` / `online_manifest` 四条 PASS）。缺陷本身仍未修 | 待指定 | 未开始 | PROGRESS.md 3000 |
| A-7 | P2 | ~~`player.proto` 已加 `instance_id` 但 `player.pb.go` 未重生，`player` 模块编译失败~~ **已由并发编辑者于同日重生 pb 解除；`player` 模块现 build+vet+test 全绿** | 并发编辑者 | 已解决 | 本档 §2.3 |
| A-9 | P2 | `pkg/configtable` 的 item 行校验被并发编辑者新增（`装备缩放X/Y/Z > 0` 无 `isEquipType` 守卫，对**非装备行也强制**；`identify_pool_id` 一致性），导致 `TestValidateItemRow` / `TestItemTableSlotQueries` / `TestLoad*` 等一批**该编辑者自己的**用例转红。任务域用例已按新规则补齐 fixture 转绿。缩放校验是否应只对装备生效，需该编辑者定夺 | 并发编辑者 | 未开始 | 本轮实测 |
| A-8 | P2 | mission 观察面缺失:0 条告警规则、无看板、无 runbook | 待指定 | 未开始 | 本档 §9 |

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据
- [x] 修复前失败、修复后通过的回归存在（逐条实测，非"写了个测试恰好过"）
- [ ] race/集成/故障注入达到本事故风险要求 —— **race 未执行（A-3）**，故障注入无对象
- [x] 同类代码扫描完成（含 1 条 Confirmed 命中，已列 A-1）
- [ ] 目标环境已加载可追溯的新产物 —— **未部署**，且现有镜像早于本轮修复
- [ ] 玩家路径、恢复和补偿路径验证通过 —— **客户端零接线，物理上无法执行（A-5）**
- [ ] 观察窗口无复发 —— **观察窗口为零**
- [ ] 剩余风险已解决或另建 Incident/任务 —— A-1..A-8 均未开始
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：**未关闭**。代码侧五条根因已闭环并有先红后绿的回归证据；但关闭门槛中的部署、观察窗口、玩家 E2E、`-race` 四项均未满足，且其中两项（E2E、观察窗口）在服务首次部署与客户端接线完成前**物理上无法满足**。不得因"单测全绿"标记关闭。
