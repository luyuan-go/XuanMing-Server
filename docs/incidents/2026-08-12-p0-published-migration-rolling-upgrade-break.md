# [INC-20260812-001][P0] 两个已发布迁移做 contract 而非 expand,滚动升级期新旧副本无法共存

> **状态**：已止血（未关闭）
> **类型**：`availability` / `near-miss`
> **环境**：本机 k8s / dev（生产未部署，见 §1）
> **首次发生时间（UTC）**：不适用（未在线上发生；缺陷随 `4e78155c` / `b7178d0c` 合入）
> **首次发现时间（UTC）**：2026-08-12 07:00 前后（本轮改动审查）
> **负责人**：待指定
> **受影响服务/版本**：`services/account/login`、`services/account/player`、`tools/migrate`；迁移 `pandora_account/000006`、`pandora_player/000007`
> **最后更新**：2026-08-12

## 0. 一句话结论

`pandora_account/000006`(角色编号改名)与 `pandora_player/000007`(段位分池)都用 `RENAME`+`DROP`
一次性删掉了旧列/旧索引/旧表，这是 **contract** 而不是 expand：迁移一旦执行，尚未排空的旧
Go 副本(Stable)读写的对象当场消失，直接违反 CLAUDE.md §9.16 / §9.21「零停机滚动更新」与
「删除能力必须走 expand → migrate → contract」。已由 `000007_player_no_expand_compat` 与
`000008_rating_pool_expand_compat` 两个纯加法迁移向前回补兼容面并双写，**线上零影响**
(见 §1)；contract 尚未执行，兼容面必须长期保留。

## 1. 影响与范围

- 玩家影响：**无**。两条链路的缺陷版本都没有真正上线。
- 服务影响：
  - `pandora_player/000007` 在 MySQL 8.4 上必然失败——`players.mmr` 上挂着 `idx_mmr`，
    `DROP COLUMN mmr, ALGORITHM=INSTANT` 报 **1845**，留下 `schema_migrations` **v7 dirty**；
    `deploy/k8s/migrate/job.yaml` 是硬门禁(`backoffLimit: 0`，Job 成功才允许滚业务 Deployment)，
    所以 v7 版 player 镜像从未滚动上线。
  - `pandora_account/000006` 若在旧 login 副本仍在跑时执行，`register_no` 三件套消失 →
    补号事务报错、编号展示功能中断（登录主链 fail-soft，不掉线）。
- 数据与安全影响：无数据丢失。段位存量按 §3.6.3 / PROGRESS 2026-08-11 的既定口径**本就重置**。
- 开始/结束时间：不适用（未在线上发生）。
- 是否仍可复发：**是**——见 §10 A-1，未来的 contract 迁移若原样重放 `DROP players.mmr,
  ALGORITHM=INSTANT` 会复现同一个 1845 dirty。
- 严重级别判定理由：按 §1「上线前发现但若上线会造成 P0 后果」建 `near-miss`。若 000006 在
  多副本 login 滚动窗口内执行，属「关键服务功能中断」；若 000007 在已上线环境执行，会把
  `schema_migrations` 打成 dirty 并**阻断此后全部发布**。

## 2. 第一现场与证据

### 2.1 症状

- 服务端症状：`pandora_player` 迁移 Job 失败退出，`schema_migrations` = `version=7, dirty=1`；
  此后每次发布被 `rejectDirtyOrNewer` fail-closed 挡住。
- 静态症状：`000006`/`000007` up.sql 中出现 `RENAME COLUMN` / `RENAME INDEX` /
  `RENAME TABLE` / `DROP COLUMN` / `DROP TABLE`，而对应的旧 Go 副本仍在读写这些对象。

### 2.2 原始证据

```text
tools/migrate/migrations/pandora_account/000006_reconcile_player_no.up.sql
  ALTER TABLE `accounts` RENAME COLUMN `register_no` TO `player_no`
  ALTER TABLE `accounts` RENAME INDEX  `uk_register_no` TO `uk_player_no`
  ALTER TABLE `accounts` DROP INDEX  `uk_register_no`
  ALTER TABLE `accounts` DROP COLUMN `register_no`
  RENAME TABLE `register_no_counter` TO `player_no_counter`
  DROP TABLE `register_no_counter`

tools/migrate/migrations/pandora_player/000007_rating_pool_partition.up.sql
  ALTER TABLE `players` DROP COLUMN `mmr`, ALGORITHM=INSTANT   ← MySQL 8.4 报 1845(mmr 上有 idx_mmr)

tools/migrate/main.go:573-580   已发布 000007 在 MySQL 8.4 报 1845 的取证与 quarantine 说明
```

### 2.3 已排除的噪声

- **「新代码把旧副本刚落的 default 段位分覆盖回退」不成立**：能与新副本共存的 Stable 是
  **pre-000007** 版(`8feb325a`)，它读写的正是 `players.mmr`，与新代码的「default 以
  `players.mmr` 为兼容权威 + 双写」**双向兼容**。所谓「000007 版 Stable」在物理上不存在——
  那份代码的 `EnsureProfile` 仍 `INSERT INTO players(..., mmr, ...)`，而同 commit 的建表脚本
  已无 `mmr` 列，上线即 1054；且其自带的 CI 真库门禁会先把它判红。
- **「000007 双计数器合并 UPDATE 会被 login 补号事务锁死」不成立**：`SweepPlayerNo` 每批必
  提交(实测一批 500 行 ≈ 277ms)，持锁窗口百毫秒级；真库压测 59 次探针 0 次 1205，且等待
  不随副本数无界增长(6→16 副本，最大等待 5.2s→6.5s)。

## 3. 时间线

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 2026-08-10 | 设计 | 拍板 `register_no` → `player_no` 改名 | `docs/design/player-no-and-login-surge.md` §3.6.3 |
| 2026-08-11 | 设计 | 拍板段位按 `rating_pool` 分池、存量清空 | `PROGRESS.md` 2026-08-11（续） |
| 2026-08-11~12 | migrate | `000006` / `000007` 以 contract 形态合入 | `4e78155c`、`b7178d0c` |
| 2026-08-12 | migrate | MySQL 8.4 上 `000007` 报 1845，留 v7 dirty | `tools/migrate/main.go:573-580` |
| 2026-08-12 | 审查 | 本轮改动审查确认两处均违反 §9.21 | 本文档 |
| 2026-08-12 | migrate | `000007`(account) / `000008`(player) expand 回补落地 | `0fdb15f1` |

## 4. 调用链与关键变量

```text
migrate Job (backoffLimit=0)
  → golang-migrate m.Up()
  → 000006 / 000007 执行 RENAME / DROP
  → 旧 Stable 副本的 SQL 目标对象消失
  → login: SweepPlayerNo / EnsureRegisterNoCounter 报错(补号中断)
     player: EnsureProfile / ApplyMMRChange 报 1054(建档与结算中断)
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| `accounts.register_no` 三件套 | `000004` | 旧 login 副本读写 | 共享 | 被 `000006` 删除 → 旧副本补号失败 |
| `players.mmr` + `idx_mmr` | `000001_baseline` | 旧 player 副本读写 | 共享 | 被 `000007` DROP（且因 idx_mmr 触发 1845） |
| `schema_migrations` | golang-migrate | 全库单行 | 共享 | 1845 后留 `v7 dirty`，阻断后续全部发布 |

## 5. 根因

### 5.1 直接根因

改名/重构被当成一次性 DDL 完成，而不是「加新→双写→排空→删旧」三阶段。§3.6.3 用「生产零
注册路径、无存量数据」论证了改名成本最低点——该论证只覆盖**数据**风险，不覆盖**二进制共存**
风险，而 §9.21 约束的恰恰是后者。

### 5.2 触发条件

- 迁移执行时刻仍有旧版本 Go 副本在跑（滚动升级窗口内必然成立）；或
- `players.mmr` 上存在 `idx_mmr`（`000001_baseline:43` 起一直存在）→ `DROP ... ALGORITHM=INSTANT` 报 1845。

### 5.3 故障放大因素

- `deploy/k8s/migrate/job.yaml` `backoffLimit: 0` + `rejectDirtyOrNewer` fail-closed：一次
  dirty 就把**此后所有发布**卡死，不只卡本次。
- 迁移文件「一旦对 origin 暴露即 immutable」，不能就地改错，只能再加一个版本向前修。

### 5.4 为什么现有保护没有挡住

- 迁移契约测试只断言**片段存在**与 fresh-init 一致性，没有任何一条断言「up.sql 不得出现
  `DROP COLUMN` / `DROP TABLE` / `RENAME`，除非本版被显式标注为 contract」。
- `ALGORITHM=INSTANT` 的可行性没有在真 MySQL 8.4 上验证过：PROGRESS 2026-08-11 把「真实
  MySQL/TiDB 上跑 000007」明确列为**未验证/交接**项，缺陷就落在这个缺口里。
- CI 此前从不设 `PANDORA_TEST_MYSQL_DSN`，真库用例全 Skip 而 `go test` 打 `ok`（与
  INC-20260811-002 同一个遮蔽机制）。本批 `392ae6e1` 已把真库回归转成门禁。

## 6. 全仓同类问题扫描

- 扫描基线 commit：`0fdb15f1`
- 扫描目录：`tools/migrate/migrations/**`
- 搜索模式：`DROP COLUMN` / `DROP TABLE` / `DROP INDEX` / `RENAME COLUMN` / `RENAME INDEX` / `RENAME TABLE`
- Confirmed 同型命中：`pandora_account/000006`、`pandora_player/000007`（本事故两条）
- 结构性隐患：**迁移评审缺少「expand-only」机械门禁**，见 §10 A-2
- 未覆盖边界：本次未逐条复核 `pandora_social` / `pandora_auction` / `pandora_battle` 等其余
  migration set 是否存在同型 contract（审查扇出中断，见 §10 A-5）

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| `tools/migrate` 加 `repairPandoraPlayerV7Dirty` 精确 quarantine（校验 000007 正文 SHA-256 + 中间 schema 形态后标 clean，再跑 000008） | 已落码 | `tools/migrate/main.go:586-706` | 仅认 `version==7 && dirty`，任一前置不符 fail-closed；不是通用 force |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| `pandora_account/000007` expand：加回 `register_no` / `uk_register_no` / `register_no_counter` | 已落码 | `000007_player_no_expand_compat.up.sql` | `TestPandoraAccountV7PlayerNoExpandCompatibilityContract` |
| login 双锁双写（先锁 `player_no_counter` 再锁 `register_no_counter`，取 MAX 水位，双列同写） | 已落码 | `services/account/login/internal/data/player_no.go` | `TestPlayerNo_MySQLAndTiDB_StableCanaryShareOneAllocator` |
| `pandora_player/000008` expand：加回 `players.mmr` + `idx_mmr`，从 `player_mmr` 回填 default | 已落码 | `000008_rating_pool_expand_compat.up.sql` | `TestPandoraPlayerV8RestoresRollingCompatibility` + 真库 7 场景矩阵 |
| player 服务 default 池以 `players.mmr` 为兼容权威并双写 | 已落码 | `services/account/player/internal/data/mmr_repo.go` | `TestApplyMMRChangeRatingPoolExpandCompatibility_MySQL` |
| `register_no_counter` 登记 §9.24 + dbcheck registry | 已落码 | `CLAUDE.md` §9.24、`tools/migrate/cmd/dbcheck/main.go` | `go test ./tools/migrate/...` 转绿（修前 `TestFreshInitTablesAreRegistered` / `TestMigrationTablesAreRegistered` 双红） |
| CI 强制 MySQL 8.4 + TiDB 8.5.1 真库回归 | 已落码 | `392ae6e1`（`ci_db.ps1` / `docker-compose.ci-db.yml` / `Jenkinsfile`） | `ci_db_contract_test.ps1` |

### 7.3 防复发规则

- `CLAUDE.md` §9.21：已有条款即本事故判据，本次未新增条款，改为在设计文档落地说明——
  见 `docs/design/player-no-and-login-surge.md` **§3.6.4**（改名的正确落地方式 + contract 退出条件）。
- `CLAUDE.md` §9.24：新增 `register_no_counter` 登记。
- **expand-only 机械门禁**(A-2，已落码)：`tools/migrate/expand_only_contract_test.go`。
  遍历全部 `*.up.sql`(剥行注释后判)，命中 `DROP COLUMN` / `DROP TABLE` / `DROP INDEX|KEY` /
  `RENAME COLUMN|INDEX|TABLE` 即失败，除非二选一：①文件头写 `-- CONTRACT:` **且**写明
  「旧副本排空判据」；②登记在 `grandfatheredContractMigrations`(仅限本门禁上线前已对
  origin 暴露、按 `tools/migrate/README` 不可再修改的历史迁移，**只减不增**)。
  配套 `TestGrandfatheredContractListIsExact` 反向断言清单里每条都**确实还是**破坏性迁移
  且真实存在，防止这张表退化成永久豁免后门。
  已收录的 5 条历史违规：`pandora_account/000005`、`pandora_account/000006`、
  `pandora_player/000007`(本事故两条)、`pandora_leaderboard/000003`、
  `pandora_trade/000004`(后两条是 json→pb 表示法切换，同样未经 expand 窗口)。
  **变异验证**：摘掉任一条 grandfathered 登记后两条门禁立即转红(`... 含破坏性 DDL DROP COLUMN`
  / `... 在嵌入迁移里不存在`)，还原后转绿 —— 门禁不是空转的。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| dbcheck 登记契约 | FAIL ×2 | PASS | `go test ./tools/migrate/...` | 见 §7.2 |
| 编译 + vet（login / player / migrate） | — | PASS | `go build` / `go vet`（按 go.work use 列表） | 本轮实跑 |
| 单测（login / player / migrate 全模块） | — | PASS | `go test ./services/account/... ./tools/migrate/...` | 本轮实跑 |
| 真库迁移矩阵（fresh / v4 / v6 / v7-clean / v7-dirty-exact / v7-dirty-mismatch） | — | **未在本轮执行** | 需 `PANDORA_TEST_MYSQL_DSN` / `PANDORA_TEST_TIDB_DSN` | 用例已备（`player_migration_test.go`），本轮无 DSN → Skip |
| `go test -race` | — | **未执行** | 需 CGO Linux | — |
| 玩家 E2E（滚动窗口内新旧副本共存） | — | **未执行** | — | — |

## 9. 部署、回滚与观察

- 修复 commit：`0fdb15f1`（**注意**：该提交把本次 expand 修复、`392ae6e1`/`6fef1cb6` 两个
  在途提交、以及 4 份无关的 Agones Fleet 版本 yaml 一起推上了 `origin/main`，commit message
  也不符合 CLAUDE.md §4 的 `<type>(<scope>): <subject>` 格式）
- 构建产物/镜像 digest：未构建
- 部署时间与目标环境：**未部署**
- 回滚条件和步骤：`000007`/`000008` 的 `down.sql` 均**有意 no-op**——回滚服务版本不等于旧副本
  已排空，回滚时删兼容面会立刻打死仍在跑的旧副本
- 观察窗口、指标与结果：未开始

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A-1 | P1 | **contract 迁移不得原样重放 `DROP players.mmr, ALGORITHM=INSTANT`**：`idx_mmr` 在列上，必先删索引或改算法，否则复现同一个 1845 dirty | 待指定 | 未开始 | 本 Incident |
| A-2 | P1 | 加 **expand-only 机械门禁**：迁移契约测试断言 up.sql 不得出现 `DROP COLUMN`/`DROP TABLE`/`RENAME *`，除非文件头显式标注 `-- CONTRACT:` 并写明旧副本排空判据 | — | **已落码** | `tools/migrate/expand_only_contract_test.go`；见 §7.3 |
| A-3 | P1 | **contract 时必须反向回填 `player_mmr ← players.mmr`**：旧副本结算只写 `players.mmr` 不写 `player_mmr`，删列瞬间玩家 default 段位会回退到最后一次新副本写入的值。`000008` 注释只写了「以后删」，没写这一步 | 待指定 | 未开始 | 本 Incident |
| A-4 | P2 | `000008` 的兼容回填是一条不分批的多表 UPDATE，会对**每个已有 default 记录的玩家**的 `players` 行加记录锁并持到语句提交（真库实测 15 万行 ≈ 18s / 150001 把锁），期间这些玩家的 `ApplyMMRChange` `SELECT ... FOR UPDATE` 会等到 `innodb_lock_wait_timeout`(targets 配 15s) 后批量报 1205。**在受支持发布路径上该语句恒 0 行**（000007 必 1845 失败 → v7 代码从未上线），风险只存在于「按当前 `04-player-tables.sql` 全新初始化且 v7 代码跑过」的库。修法只能是按主键游标分批 + 每批独立提交，或在文件头写明该取舍（加 `WHERE p.mmr <> pm.mmr` **无效**：RR 下锁在判谓词之前就加） | 待指定 | 未开始 | 本 Incident |
| A-5 | P1 | **本轮审查未跑完**：5 个维度中 `migrate-quarantine` / `ci-and-proto` / `cross-cutting` 三个 agent 因连接中断未返回；9 条进入对抗复核的发现里 4 条的复核 agent 同样中断。已完成的 2 个维度共提出 19 条，仅 3 条完成裁决（1 成立 / 2 推翻）。**其余 16 条既未确认也未证伪**，其中至少 `SweepPlayerNo` 返回值语义变化导致 `player_no_assigned rows` 失真、`EXISTS(mmr_history)` 判据与保留期清理的相互作用、`change.Baseline` 在 default 池成为死参数、`idx_mmr` 无现役查询使用四条需要复跑 | 待指定 | 未开始 | 本 Incident |
| A-6 | P2 | `0fdb15f1` 把 4 份 Agones Fleet 版本 yaml（battle / battle-canary / hub / hub-canary，`r1971→r1977`）与本次 expand 修复混在同一提交推上 `origin/main`，未单独验证版本一致性 | 待指定 | 未开始 | 本 Incident |
| A-7 | P3 | `pandora_social` / `pandora_auction` / `pandora_battle` 等其余 migration set 未做同型 contract 扫描 | 待指定 | 未开始 | 本 Incident |

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据
- [x] 修复前失败、修复后通过的回归存在（dbcheck 登记契约）
- [ ] race/集成/故障注入达到本事故风险要求
- [ ] 同类代码扫描完成（仅覆盖 account/player 两个 set，见 A-7）
- [ ] 目标环境已加载可追溯的新产物（未部署）
- [ ] 玩家路径、恢复和补偿路径验证通过
- [ ] 观察窗口无复发
- [ ] 剩余风险已解决或另建 Incident/任务（A-1..A-7 全部未开始）
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭。expand 兼容面已落地并有回归，但 contract 退出条件、A-1/A-2/A-3
三条防复发项与 A-5 的审查补跑均未完成；真库迁移矩阵与玩家 E2E 零执行。
