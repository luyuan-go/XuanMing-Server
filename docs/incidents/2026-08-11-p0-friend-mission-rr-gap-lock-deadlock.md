# [INC-20260811-002][P0] friend / mission 写事务在 RR 间隙锁下确定性死锁(near-miss)

> **状态**：已修复待部署
> **类型**：`near-miss`（上线前发现；若上线即 P0 `availability`）
> **环境**：本机 dev（真 MySQL 8.4.9 容器）；**未在任何线上环境发生**
> **首次发生时间（UTC）**：未发生于线上。本机首次复现 2026-08-12 02:18:30
> **首次发现时间（UTC）**：2026-08-12 02:18:30（本机本地时区 UTC-4，对应本地 2026-08-11 22:18:30）
> **负责人**：待指定
> **受影响服务/版本**：`friend`、`mission`；基线 commit `7a858783`（未部署，无镜像 digest）
> **最后更新**：2026-08-12

## 0. 一句话结论

`friend.CreateRequest` / `friend.Block` / `mission` 的所有写事务在 MySQL 默认 REPEATABLE READ 下会确定性触发 InnoDB 1213 死锁：**未命中的 `SELECT ... FOR UPDATE` 锁的是键所在的间隙而非某一行**，多个事务各自持有同一间隙的相容间隙锁后再各自 INSERT，插入意向互相阻塞成环。缺陷从未上线；它长期不被发现的原因是 CI 从不设置 `PANDORA_TEST_MYSQL_DSN`，相关用例全部 Skip 而 `go test` 对全 Skip 的包只打印 `ok`。已改为四条写事务显式 READ COMMITTED（friend 另将 player 守卫前移），双后端全绿并逐条做了反向变异验证。

## 1. 影响与范围

- 玩家影响（**假设上线**）：并发下发送好友申请 / 拉黑返回 `ErrInternal`；任务进度与战斗结算事实上报失败。`mission` 那条打在 `ApplyFactsTx`，**每场战斗结算的必经路径**，且**互不相干的玩家会互相打死**（不是同玩家争用）。
- 实际影响人数/对局/请求数：**0**。未部署，无线上流量。
- 服务影响：无。
- 数据与安全影响：无。死锁事务整体回滚，不产生半写。
- 开始/结束时间：不适用（未上线）。
- 是否仍可复发：**是** —— 同型写法在全仓另有 14 个文件（见 §6），未逐一验证。
- 严重级别判定理由：符合 `index.md §1` 第三条「上线前发现但若上线会造成 P0 后果」。战斗结算路径在并发下成批失败，且失败率随在线人数上升，属 P0 可用性事故。

## 2. 第一现场与证据

### 2.1 症状

- 服务端症状：`errcode=2 ... Error 1213 (40001): Deadlock found when trying to get lock; try restarting transaction`。全仓无任何 1213 重试，错误直接抛回调用方。
- 客户端症状（推断，未实测）：好友申请 / 拉黑失败提示；任务进度不推进。
- K8s/Agones 状态：不适用。

### 2.2 原始证据

复现命令（本机，真 MySQL 8.4.9 容器 127.0.0.1:3307）：

```text
cd services/social/friend
PANDORA_TEST_MYSQL_DSN='root:<pw>@tcp(127.0.0.1:3307)/' \
  go test -count=1 -run TestFriendRepoIncomingLimitConcurrencyMySQLAndTiDB ./internal/data/
→ 3 次运行 3 次 FAIL：
  friend_repo_mysql_test.go:280: mysql 并发申请 0 意外错误:errcode=2
    acquire player guard 9101: Error 1213 (40001): Deadlock found when trying to get lock
```

`SHOW ENGINE INNODB STATUS` 的 LATEST DETECTED DEADLOCK（**形状一**，共享守卫行）：

```text
*** (1) TRANSACTION: INSERT INTO friend_player_guards (player_id) VALUES (9101)
                     ON DUPLICATE KEY UPDATE player_id = player_id
    HOLDS:  index uk_requester_target of `friend_requests`   lock_mode X   (supremum 间隙)
    WAITS:  index PRIMARY of `friend_player_guards`          lock_mode X locks rec but not gap waiting
*** (2) TRANSACTION: INSERT INTO friend_requests (...) VALUES (20000, 2001, 9101, 1)
    HOLDS:  index PRIMARY of `friend_player_guards`          lock_mode X locks rec but not gap
    WAITS:  index uk_requester_target of `friend_requests`   lock_mode X insert intention waiting
*** WE ROLL BACK TRANSACTION (1)
```

**形状二**（不共享任何守卫行，纯间隙 ↔ 插入意向）：

```text
*** (1) HOLDS: uk_requester_target  lock_mode X
        WAITS: uk_requester_target  lock_mode X insert intention waiting
*** (2) HOLDS: uk_requester_target  lock_mode X
        WAITS: uk_requester_target  lock_mode X insert intention waiting
```

mission 侧：

```text
mission_guard_lock_order_mysql_test.go:126: 跨玩家并发接取 触发 InnoDB 死锁(1213):
  upsert active mission=4: Error 1213 (40001): Deadlock found when trying to get lock
```

### 2.3 已排除的噪声

- **「守卫锁序错了」——已排除为根因**。第一版假设是 `acquirePlayerGuard` 取得太晚。重排确实修好了形状一与 Block，但形状二在没有任何共享守卫行的情况下照样死锁；反向变异也证明：保留 RC 而把守卫挪回原位，三个用例全绿（见 §8）。**锁序是放大因素，不是根因。**
- **同期 `pandora-mysql` 容器多次消失导致的 `connection refused` 批量失败与本事故无关**：本机另有进程在反复 `compose up/down/kill` 该栈（观测到 exit 0 优雅停机、容器被整体删除、`ExitCode=137` 且 `OOMKilled=false` 的外部 SIGKILL 各一次）。这些失败的报错是"目标机器积极拒绝"，与 1213 无关。

## 3. 时间线

本机时区 UTC-4；下表以 UTC 为主，括号内为本地时间。

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 2026-08-12 01:5x（08-11 21:5x） | 本机 | 首次把 `pandora-mysql` 起起来，跑全部 MySQL 门控用例 | 21 个门控文件，10 模块 |
| 2026-08-12 02:0x（08-11 22:0x） | friend | `TestFriendRepoIncomingLimitConcurrencyMySQLAndTiDB` 确定性 FAIL，3/3 | 见 §2.2 |
| 2026-08-12 02:18:30（08-11 22:18） | MySQL | 捕获 LATEST DETECTED DEADLOCK，确认形状一 | InnoDB status |
| 2026-08-12 02:4x（08-11 22:4x） | friend | 新增锁序回归用例，抓到形状二与 Block 同型 | `friend_guard_lock_order_mysql_test.go` |
| 2026-08-12 03:0x（08-11 23:0x） | mission | 跨玩家并发用例证伪「mission 不受影响」的读码结论 | `mission_guard_lock_order_mysql_test.go` |
| 2026-08-12 03:1x（08-11 23:1x） | friend/mission | 四条写事务改 RC，双后端全绿 | 见 §8 |

## 4. 调用链与关键变量

```text
friend.CreateRequest
  → BeginTx(RR)                       ← 缺陷位置：隔离级别
  → acquirePairGuard(requester,target)
  → SELECT blocks       ... FOR UPDATE  ← 未命中 → 间隙锁
  → SELECT friendships  ... FOR UPDATE  ← 未命中 → 间隙锁
  → SELECT friend_requests uk_requester_target FOR UPDATE ← 未命中 → 间隙锁（成环的那把）
  → checkIncomingLimit → acquirePlayerGuard(target)  ← 原实现在这里才取守卫
  → INSERT friend_requests            ← insert intention 撞上他人间隙锁 → 1213

mission.inTx(RR) → acquirePlayerGuard(player)  ← 守卫顺序本就正确
  → loadState(forUpdate=true) 对 player_mission_active / _done 零行 FOR UPDATE ← 间隙锁
  → persist → INSERT/UPSERT player_mission_active ← insert intention → 1213
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| 间隙锁（supremum） | InnoDB，未命中 `FOR UPDATE` 时 | 事务持有到提交 | **跨事务共享且相容** | 环的一半：人人都能拿到 |
| insert intention | INSERT 语句 | 语句级等待 | 与他人间隙锁互斥 | 环的另一半 |
| `friend_player_guards` 行 | `acquirePlayerGuard` | 事务持有到提交 | 同 target 唯一 | 形状一的排他点（放大因素） |

## 5. 根因

### 5.1 直接根因

MySQL REPEATABLE READ 下，`SELECT ... WHERE <key>=? FOR UPDATE` **未命中记录时锁的是该键所在的间隙**，而非"这一行"。空表或稀疏索引上，互不相同的键会落进**同一个** supremum 间隙。间隙锁彼此相容，N 个并发事务全部取得；随后各自的 INSERT 需要该间隙的插入意向锁，与他人尚未释放的间隙锁互斥，形成等待环。

代码里的原始注释把这条前提写反了：

> 请求行锁在 pair 守卫之后、player 守卫之前：本事务持有的**行锁只属于本 pair**，与只共享单个玩家的其它事务无共同行 → 不构成环，锁序安全

该断言在"行存在"时成立，而首次申请 / 首次拉黑恰恰是"行不存在"。InnoDB 死锁日志逐字否掉了它。

### 5.2 触发条件

- 后端为 **MySQL**（TiDB 无 gap 锁，同代码不复现）；
- 并发写同一张表，且各事务的 `FOR UPDATE` 都未命中；
- 事务在取得间隙锁后还要执行若干语句才轮到 INSERT（窗口越宽越必然）。

### 5.3 故障放大因素

- **守卫取得过晚**（friend）：`acquirePlayerGuard` 排在三条探针之后，使间隙锁落在守卫的串行化之外，把"偶发"变成"必然"。
- **全仓无 1213 重试**：死锁本可由调用方重试吸收，实际直接抛回业务。
- **无 TiDB/MySQL 差异意识**：双后端测试存在，但只有 TiDB 侧被跑过。

### 5.4 为什么现有保护没有挡住

| 保护 | 为何无效 |
|---|---|
| 双后端集成测试 | 用例写对了，但 CI 从不设 `PANDORA_TEST_MYSQL_DSN` → MySQL 侧全 Skip；`go test` 对全 Skip 的包打印 `ok`，报告里与真跑过无法区分 |
| 守卫行设计（R5 复审 P1-2） | 守卫解决的是"TiDB 无 gap 锁导致上限被穿透",与间隙锁死锁是两个问题；守卫本身还贡献了形状一的排他点 |
| 代码 review | 原注释的错误论断（"行锁只属于本 pair"）看起来完全合理，只有真库能证伪 |
| 重试 / 熔断 | 不存在 |

## 6. 全仓同类问题扫描

- **扫描基线 commit**：`7a858783`
- **扫描目录和文件类型**：`services/**/*.go`（排除 `_test.go`）
- **搜索模式/工具**：判据 = 同一文件内「某表既出现 `... FROM <表> ... FOR UPDATE` 又出现 `INSERT INTO <表>`」，且事务未显式 RC。
- **Confirmed 同型命中（已修）**：`friend`（`blocks` / `friendships` / `friend_requests`）、`mission`（`player_mission_active` / `player_mission_done`）。
- **结构性隐患（未验证，不等于有问题）**：静态扫描在 **14 个仍用默认 RR 的文件**里命中该形状：

  | 文件 | 命中表 |
  |---|---|
  | `login/internal/data/session_generation.go` | player_session_generations |
  | `player/internal/data/attribute_repo.go` | player_attributes |
  | `player/internal/data/skill_card_repo.go` | player_skill_cards |
  | `battle_result/internal/data/progress_repo.go` | battle_progress_stream |
  | `auction/internal/data/auction_repo.go` | auction_orders, auction_owner_guards |
  | `inventory/internal/data/bag_capacity.go` | bag_capacity, bag_meta |
  | `inventory/internal/data/bag_migration.go` | bag_meta, bag_section |
  | `inventory/internal/data/bag_repo.go` | bag_checkpoint, bag_meta, bag_section |
  | `inventory/internal/data/inventory_instance.go` | player_item_instance |
  | `inventory/internal/data/inventory_repo.go` | auction_escrow, player_currency, player_items |
  | `owner/internal/data/owner_repo.go` | ds_instance_lease |
  | `guild/internal/data/group_repo.go` | chat_group_members, chat_groups, player_group_counts |
  | `guild/internal/data/guild_repo.go` | guild_join_requests, guild_members, guilds |
  | `mail/internal/data/mail_repo.go` | player_mail |

- **未覆盖边界（必须写明）**：
  1. **静态扫描会漏**：`mission` 的 `loadState` 把 `" FOR UPDATE"` 拼成变量，正则看不见 —— 而它正是确诊病例之一。上表因此是**下界不是全集**。
  2. **动态普查未能给出结论**：`tools/migrate/rr_gap_lock_deadlock_survey_test.go` 试图对上述各表直接跑该形状，但**阳性对照 `friend_requests`（已确诊必炸）也报"安全"**，说明探针没能造出目标并发形态（屏障 5s 超时会让先到者提前插入并提交，后到者转而撞记录锁，整批被串行化）。该文件已改为**对照不亮即整体 Skip**，绝不输出会被误引的假阴性。
  3. 因此这 14 个文件当前状态是 **未验证**，既未证实有问题，也未证实安全。
- **已确证有效的方法**：按域走**真实 repo API** 的端到端并发用例（friend / mission 两例即由此抓获）。见 §10 A-1。

## 7. 处置与永久修复

### 7.1 临时止血

不适用：未上线，无需止血。

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| friend 四条写事务显式 READ COMMITTED | 已完成(`2292b2be`) | `friend_repo.go` `friendWriteTxIsolation` | §8 |
| friend `acquirePlayerGuard` 前移到任何锁定读之前（`CreateRequest` / `Block`） | 已完成(`2292b2be`) | 同上 | §8 |
| mission `inTx` 显式 READ COMMITTED | 已完成(`2292b2be`) | `mission_repo.go` | §8 |
| 锁序/死锁回归用例 | 已完成 | `friend_guard_lock_order_mysql_test.go`（3 例）、`mission_guard_lock_order_mysql_test.go`（2 例） | §8 |
| CI 依赖门控跳过审计（把"SKIP 显示成 ok"堵上） | 已完成 | `ci_backend.ps1` + `lib/go_test_skip_audit.ps1` + 契约测试 | §8 |

**为什么 RC 是安全的（不是调优取舍）**：这两个域的并发正确性**从设计之初就不依赖 gap 锁** —— 守卫行存在的唯一理由就是"TiDB 没有 gap 锁，零行 `FOR UPDATE` 一把锁都不加"（R5 复审 P1-2）。限额权威来自守卫行 + 守卫锁内的锁定读，唯一性来自唯一键，幂等来自收据表唯一键；三者在 RC 下全部成立。RR 的 gap 锁在 MySQL 侧是纯副作用。RC 的锁定读还总读最新已提交，比 RR 更"当前"，R9 复审要修的陈旧快照问题只会更稳。前置条件：`binlog_format=ROW`（MySQL 8.4 默认）。`login/register_no.go` 有更早的同款先例。

### 7.3 防复发规则

- `CLAUDE.md §16.1`（TOCTOU / 原子性）与 `§16.6`（验证必须对应风险）已覆盖本类问题，本次不新增条款，但补充一条**可执行判据**写在代码注释里：**凡「可能查空的 `FOR UPDATE` + 同表 INSERT」的写事务，必须显式声明隔离级别并说明理由**。
- CI 侧规则见 §7.2 最后一行：依赖门控用例被跳过时不得再被报告成通过。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| friend 原有并发用例 | **FAIL 3/3**（1213） | PASS | 真 MySQL 8.4.9 | §2.2 |
| friend 新增锁序用例（同 target / 跨 target / Block） | 3 例中 3 例红 | PASS | 同上 | `friend_guard_lock_order_mysql_test.go` |
| mission 新增用例（同玩家 / 跨玩家） | 跨玩家 FAIL | PASS | 同上 | `mission_guard_lock_order_mysql_test.go` |
| 反向变异①：friend 退回 RR（保留守卫前移） | — | **仅「跨 target」红** | 同上 | 证明守卫前移只覆盖共享守卫的两种形状 |
| 反向变异②：守卫挪回原位（保留 RC） | — | **全绿** | 同上 | **证明 RC 才是根治，守卫前移是纵深防御** |
| 反向变异③：mission 退回 RR | — | **跨玩家红** | 同上 | — |
| friend 全量套件 × 5 次 | — | 全绿无抖动 | 同上 | — |
| friend / mission 全量套件（MySQL + TiDB 双后端同开） | — | 全绿（16.1s / 7.1s） | MySQL 8.4.9 + TiDB v8.5.1 | — |
| CI 跳过审计契约测试 | — | PASS（6 组断言） | `go_test_skip_audit_contract_test.ps1` | 含"DSN 已设置却仍跳过 → 硬失败"与"普通 Skip 不得误判" |
| CI DB 生命周期契约 | — | PASS | `ci_db_contract_test.ps1` | 固定 MySQL 8.4 / TiDB 8.5.1、动态回环端口、无持久卷、Jenkins `post always` 清理 |
| CI DB 本机真实生命周期 | — | PASS | `ci_db.ps1 Up/Down`（2026-08-12） | 4 容器约 19s 全部 healthy；TiDB 初始化完成；Down 后容器/网络/状态文件均为零 |
| TiDB 真实后端行为探针 | — | PASS（2） | 本轮临时 TiDB v8.5.1 | `AssertTiDBBackend` 与 `accounts.account` collation 语义均执行，零 SKIP |
| Jenkins 完整 job | — | **SKIP / 未验证** | 需提交后由 Jenkins 拉取运行 | 当前只验证了本机等价 Docker 生命周期与脚本契约，不能冒充 Jenkins build 成功 |
| CI 审计对真实输出生效 | — | friend 模块无 DSN 时 **51 通过 / 14 门控跳过**；两个 DSN 都设置后 **65 通过 / 0 跳过** | 同上 | 量化了"CI 里 14 个用例从未执行" |
| `go test -race` | **未执行** | **未执行** | CI 唯一入口无 `-race`，本机 `CGO_ENABLED=0` 直接报错 | 阻断项，见 A-4 |
| 全仓同型动态普查 | — | **无结论**（阳性对照未复现） | `rr_gap_lock_deadlock_survey_test.go` | §6 未覆盖边界 2 |
| fatal/OOM/SIGKILL 重启注入 | **未执行** | **未执行** | 与本事故风险无关（死锁整体回滚，无半写） | — |
| 玩家 E2E | **未执行** | **未执行** | 未部署 | A-3 |

## 9. 部署、回滚与观察

- 修复 commit：`2292b2be`(friend/mission 死锁修复 + 回归)、`afcb45b4`(player/leaderboard 真 MySQL 回归)。CI 跳过审计被另一进程并发 `git add -A` 卷进了不相关的 `4e78155c`(提交信息 "rename player no",**与本改动无关**,追溯时按文件名找)。三者均已推到 `origin/main`
- 构建产物/镜像 digest：无
- 部署时间与目标环境：**未部署**(仅进版本库,无镜像构建)
- 实际 Pod `imageID` / GameServer provenance：不适用
- 回滚条件和步骤：单文件级回滚（`BeginTx` 参数改回 `nil`），无 schema 变更、无数据迁移，回滚零风险
- 观察窗口、指标与结果：**为零**。上线后建议观察 `Error 1213` 计数与 friend/mission 的 `ErrInternal` 率

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A-1 | P1 | 按域为 §6 表中 14 个文件补**真实 repo API 的跨实体并发用例**（唯一确证有效的方法）；逐域给出确诊/排除结论 | 待指定 | 未开始 | 本 Incident |
| A-2 | P1 | Jenkins 挂测试 MySQL/TiDB 并注入 DSN，随后开启 `ci_backend.ps1 -RequireDbTests` | Codex / Jenkins | **代码与本机生命周期已完成，待提交后 Jenkins 实跑确认**：新增隔离 compose + `ci_db.ps1`，Jenkins 已接 `Up → -RequireDbTests → post always Down`；完整 Jenkins job 当前 SKIP，不能关闭 | 本 Incident |
| A-3 | P1 | 提交 → 构建 → 部署 → 观察窗口 | 待指定 | 未开始 | 本 Incident |
| A-4 | P2 | `go test -race` 进 CI（需 CGO + Linux agent） | 待指定 | 未开始 | 沿用 INC-20260811-001 A-4 |
| A-5 | P2 | 修好动态普查探针（更强屏障）或明确废弃该文件，避免留一个长期 Skip 的工具 | 待指定 | 未开始 | 本 Incident |
| A-6 | P2 | 评估是否为全仓写事务引入统一的 1213 有界重试（当前零重试，死锁直接抛给业务） | 待指定 | 未开始 | 本 Incident |
| A-7 | P3 | 本机 `pandora-mysql` 被其它进程反复 `compose down/kill`，导致集成测试批量假失败；需约定独占测试库或固定端口的专用容器 | 待指定 | 已缓解 | 本次改用 `pandora-mysql-itest`（13399）绕开 |

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据（InnoDB 死锁日志两种形状 + 三组反向变异）
- [x] 修复前失败、修复后通过的回归存在
- [ ] race/集成/故障注入达到本事故风险要求（`-race` 未执行，A-4）
- [ ] 同类代码扫描完成（静态已做且已知有漏；动态普查无结论，A-1）
- [ ] 目标环境已加载可追溯的新产物（未提交未部署，A-3）
- [ ] 玩家路径、恢复和补偿路径验证通过
- [ ] 观察窗口无复发
- [ ] 剩余风险已解决或另建 Incident/任务
- [x] 文档已脱敏且时间线时区明确（本机 UTC-4，已标注换算）

**关闭结论与审批人**：未关闭。
