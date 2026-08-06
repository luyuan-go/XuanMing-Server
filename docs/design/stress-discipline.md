# Pandora 压测纪律

> Pandora 压测执行规范,适配 Pandora 项目路径与工具脚本。

## 1. 总原则

1. **没有对比表不许声明"性能提升"**
2. **每轮压测前后必须做完整环境清理**,不许"上一次跑完接着跑"
3. **prom 数据只读 summarize 脚本输出,不许手 grep raw dump**
4. **结果文档复用脚本输出表格,不贴 raw count/sum 数字**
5. **压期间不上传任何日志**
6. **每次登录压测把所有 redis/mysql/etcd 数据全部删除再开新一轮**
7. **单 Cell 回归压测每周固定跑一轮**(见 §10),周对周对比,持续守住性能基线、尽早暴露退化

## 2. 压测目录结构

```
F:/work/Pandora/
├── robot/
│   ├── stress/                      # 机器人压测客户端
│   └── logs/
│       └── stress-<name>-<ts>/      # 单轮压测目录
│           ├── prom-snapshots/
│           │   ├── t2m_login.txt    # 2 分钟时刻 login 端口快照
│           │   ├── t2m_match.txt    # 2 分钟 matchmaker
│           │   ├── t2m_ds.txt       # 2 分钟 ds_allocator
│           │   ├── t5m_*.txt
│           │   └── ...
│           ├── robot-stats.jsonl    # 机器人侧每分钟统计
│           ├── prev-summary.txt     # ⭐ 上一轮 baseline
│           ├── summary.txt          # ⭐ 本轮 summarize 输出
│           └── round-N-vs-N-1.md    # 二维对比表
└── tools/
    └── scripts/
        ├── stress_summarize.ps1     # 单轮汇总(读 prom 快照,出二维表)
        ├── stress_snap.ps1          # 后台批量拉 prom snapshot
        ├── go_svc_stop.ps1          # 停所有 go 服务
        └── dev_tools.ps1            # 通用开发工具(含 kafka offset reset 等)
```

## 3. 端口分工

| 端口 | 服务组 | 主要看的指标 |
|---|---|---|
| `:21001` | login metrics | 登录 QPS、票据签发耗时 |
| `:21011` | matchmaker metrics | 队列长度、匹配等待、撮合耗时 |
| `:21020` | ds_allocator metrics | DS 拉起耗时、pod 数、Agones 调度 RTT |
| `:21022` | battle_result metrics | kafka lag、幂等命中率、写库耗时 |

`stress_snap.ps1` 默认并行拉这 4 端口,文件命名 `t<N>m_<svc>.txt`,`stress_summarize.ps1` 按后缀分流。

## 4. 压测前后强制流程

### 4.1 跑测前 ⚠️

1. **保存上一轮 summary**:把上次 `summary.txt` 复制为 `prev-summary.txt`
   - `prev-summary.txt` 不存在 → **不许开下一轮**
2. **清空污染数据**(每条都跑):
   ```powershell
   # robot 旧目录(留最近 1 个,其它删)
   Get-ChildItem F:/work/Pandora/robot/logs/stress-* | Sort LastWriteTime -Desc | Select -Skip 1 | Remove-Item -Recurse -Force

   # 各 go service stderr/stdout
   pwsh F:/work/Pandora/tools/scripts/go_svc_stop.ps1
   Remove-Item F:/work/Pandora/tools/scripts/.run/* -Recurse -Force

   # redis 清残留 lock / session
   redis-cli -p 6380 FLUSHALL

   # kafka offset reset
   pwsh F:/work/Pandora/tools/scripts/dev_tools.ps1 -Command kafka-offset-reset

   # mysql 完整删表再重建
   pwsh F:/work/Pandora/tools/scripts/dev_tools.ps1 -Command db-reset

   # etcd 清服务注册
   pwsh F:/work/Pandora/tools/scripts/dev_tools.ps1 -Command etcd-clear

   # prom snapshot 目录新建
   New-Item F:/work/Pandora/robot/logs/stress-<name>-<ts>/prom-snapshots/ -ItemType Directory
   ```
3. **DS pod 清理**:
   ```bash
   kubectl delete gameserver --all -n pandora
   kubectl delete fleet --all -n pandora && kubectl apply -f deploy/k8s/fleets.yaml
   ```

### 4.1.1 库增长基线快照(§9.24,强制)

清库重建后、开压前抓一次全库行数基线:

```powershell
cd tools/migrate
go run ./cmd/dbcheck -dsn 'root:<pwd>@tcp(127.0.0.1:3307)/' -exact -snapshot robot/logs/stress-<name>-<ts>/db-before.json
```

该命令同时断言:**所有 pandora_% 库的表已在登记清单内(§9.24)、swept 表清理索引齐备、
outbox 无堆积**。FAIL → 不许开压(新表未登记 = 无界增长风险,先登记 + 落清理再压)。

### 4.2 压测中

- **至少 3 次 snapshot**:ramp 完成 / 稳态中段 / 稳态末
- snapshot 命令:
  ```powershell
  pwsh tools/scripts/stress_snap.ps1 `
    -RunDir robot/logs/stress-<name>-<ts> `
    -StartTime '<yyyy-MM-dd HH:mm:ss>' `
    -Stages 2,5,10,15,18
  ```
- **不许手拉单端口**(`curl :21001/metrics > t2m.txt` 这种临时抓取不再用)

### 4.3 跑测后

1. 跑 `stress_summarize.ps1`:
   ```powershell
   pwsh tools/scripts/stress_summarize.ps1 -RunDir robot/logs/stress-<name>-<ts>
   ```
2. 与 `prev-summary.txt` 二维对比,写进 `round-N-vs-N-1.md`
3. **库增长断言(§9.24,强制)**:
   ```powershell
   cd tools/migrate
   go run ./cmd/dbcheck -dsn 'root:<pwd>@tcp(127.0.0.1:3307)/' -exact -compare robot/logs/stress-<name>-<ts>/db-before.json
   ```
   核对三件事,任一不过 = 本轮不许宣布通过:
   - **outbox 已排空**(工具自动断言 ≤ `-outbox-max`,默认 200;堆积 = 投递链堵塞);
   - **每张表的增量能用业务量解释**(流水/历史行数 ≈ 对局数×人数、操作数;数量级对不上 =
     重复写入 / 幂等失效 / 泄漏,当 bug 查);
   - **无未登记表**(压测中间接建出的新表也会被抓)。
4. **待清理量报告**(判断保留期与清理模式是否合适):
   ```powershell
   go run ./cmd/dbcheck -dsn '...' -pending
   ```
   输出各 swept 表在 cutoff=now 下的可清理行数(上界)。**只 COUNT,不删任何数据。**

   ⚠️ 早先版本的 `-force-sweep -confirm=YES-DELETE`(批删到空 + 输出 rows/s)已于
   2026-07-22 按用户指令**整块移除**:不允许"因为数据大了"就删数据。保留期清理现在
   默认 `retention_mode=report_only`(只 WARN 告警不删),真删要各服务显式配
   `retention_mode: delete`。**因此压测不再验证"批删速率"**——默认根本不删;
   若将来要开 delete 模式,再单独设计速率验证(需在可丢弃库上做)。
5. 贴决策行 + 更新 `docs/design/pandora-arch.md` §11
6. 更新 `PROGRESS.md`
7. **压期间不上传日志,只上传 summary 表格**

### 4.4 完成清单

```
[ ] prev-summary.txt 已存
[ ] redis/mysql/etcd 已清空
[ ] kafka offset 已 reset
[ ] DS pod 已清干净
[ ] prom-snapshots/ 目录已建
[ ] dbcheck 压前基线 db-before.json 已抓且 PASS(无未登记表/缺索引)
[ ] 至少 3 次 snapshot 已抓
[ ] summarize.ps1 输出五段表
[ ] dbcheck -compare 已跑:outbox 排空 + 增量可解释 + 无未登记表
[ ] dbcheck -pending 已记录各表待清理量(评估保留期/清理模式)
[ ] 对比表已写
[ ] 决策行已贴
[ ] PROGRESS.md 已更新
```

**漏一项重来**。

## 5. summarize 脚本输出五段表

适配 Pandora 关键路径:

| 段 | 内容 | 数据源 |
|---|---|---|
| 1. robot 每分钟 stats | 在线、登录、匹配、进 DS、断开 | robot-stats.jsonl |
| 2. matchmaker 关键阶段 | enqueue / matched / confirmed / dispatched 各阶段平均耗时 + p99 | `:21011` 指标 |
| 3. ds_allocator 子阶段 | k8s api / agones allocate / pod ready / first-conn 各阶段耗时 | `:21020` 指标 |
| 4. battle_result 子阶段 | kafka lag / decode / db write / ack 各阶段耗时 | `:21022` 指标 |
| 5. 大厅 DS Replication | hub 在线人数 / 包大小 / NetCullDistance 实际触发 / Iris stat | DS prom 端口 |

## 6. 反模式禁令

- ❌ 不许跨轮共用 `robot/logs/` 目录
- ❌ 不许在没清 redis 的情况下接着跑(残留 lock 会让 trade 测试错乱)
- ❌ 不许在跑测中途调整 go 服务参数(中段调参 = 数据废了)
- ❌ 不许把 raw count/sum 数字塞进文档(只贴 summarize 输出表)
- ❌ 不许在没有 `prev-summary.txt` 的情况下声明"性能提升"

## 7. Round N 命名规则

```
docs/design/stress-<round>-<topic>-<date>.md
```

例:
```
stress-1-login-burst-20260620.md
stress-2-match-throughput-20260625.md
stress-3-hub500ppl-20260701.md      ⭐ 关键里程碑:500 人 hub PvP
stress-4-battle-50rooms-20260710.md
```

每篇必须含:
1. 测试目标(一句话)
2. 测试参数(robot 数、ramp 时长、稳态时长)
3. 环境(go 版本 / UE 版本 / k8s 版本 / DS pod replica)
4. summarize 输出表
5. vs prev 二维对比
6. 瓶颈分析
7. 决策行(写回 pandora-arch.md §11)

## 8. Pandora 特有关注点

Pandora 是分布式后端 + UE DS,压测时额外关注:

| 维度 | 关注点 |
|---|---|
| 受测组件 | 14 个 go 服务 + UE Hub DS + UE Battle DS |
| 压测客户端 | go robot + UE headless client(后期) |
| 关键瓶颈 | matchmaker MMR / Replication Graph / Iris |
| 必看指标 | match.found 链路 / hub_player_count / ds_pod_ready_p99 |
| 清理重点 | redis lock / kafka offset / k8s GameServer / Agones Fleet |

## 9. UE 机器人压测分层

UE 侧压测必须按目标分层,不要把不同压力来源混成一个结论:

| 层级 | 工具形态 | 主要验证 | 不适合验证 |
|---|---|---|---|
| 服务端 Bot | DS 内 `AIController + Pawn` / `StressBotManager` 批量生成 | 单 DS tick、AI、GAS、技能、Actor 数、内存、CPU | 真实客户端连接数、NetDriver 握手、客户端带宽 |
| 无渲染 UE Client Bot | 批量启动打包客户端,带 `-nullrhi -nosound -unattended` | 真实登录、匹配、进房、NetDriver 连接、Replication、RPC、服务端带宽 | 大规模后端 API QPS 极限 |
| 轻量协议 Robot | Go / 脚本直接压 gRPC-Web / gRPC / HTTP 入口 | login、matchmaker、push、商城、队伍等后端链路 QPS | UE Replication / GAS / 客户端渲染相关问题 |

### 9.1 服务端 Bot

用于先把玩法服务器自身压力打满。推荐在 Hub DS / Battle DS 内做 `StressBotManager`,通过 GM 指令或启动参数生成 N 个 Bot,行为包含移动、寻路、攻击、释放技能、拾取、死亡复活、交互等真实玩法路径。

示例控制命令:

```text
SpawnStressBots 500
ClearStressBots
```

结论只允许表述为"DS 内部玩法负载 / AI / GAS / Actor 承载",不能等同于"真实 500 玩家在线"。

### 9.2 无渲染 UE Client Bot

用于验证真实客户端链路。机器人客户端必须走完整登录、匹配、DS ticket、连接 Hub/Battle DS 的流程,并尽量复用真实客户端代码路径。

Windows 批量启动示例:

```powershell
1..50 | ForEach-Object {
  Start-Process .\Pandora.exe -ArgumentList "127.0.0.1 -game -nullrhi -nosound -unattended -nopause -log -botId=$_"
}
```

常用参数:

```text
-nullrhi
-nosound
-unattended
-nopause
-log
```

单机能跑的 UE Client Bot 数量通常有限,不要用一台机器的上限反推服务器真实上限。需要更高并发时,按压测机器横向分摊,并在结果文档记录压测机 CPU / 内存 / 网卡 / Bot 数。

### 9.3 后端协议压测

登录、匹配、组队、商城、推送等后端链路优先用轻量 robot 压测,不要用 UE 客户端硬怼接口 QPS。UE Client Bot 只用于端到端链路与 NetDriver / Replication 验证。

### 9.4 指标与结论口径

每轮 UE 机器人压测必须同时记录:

1. Bot 类型:服务端 Bot / UE Client Bot / 轻量协议 Robot。
2. Bot 数量、ramp 时长、稳态时长、压测机规格。
3. DS 指标:`stat unit`、`stat game`、`stat net`、`stat memory`、CSV profile、Unreal Insights trace。
4. 后端指标:login / matchmaker / ds_allocator / battle_result 的 summarize 输出。
5. 结论边界:本轮结论能说明什么,不能说明什么。

推荐路线:

1. 先用服务端 Bot 测单 DS 玩法承载。
2. 再用无渲染 UE Client Bot 测真实连接、Replication、进出 DS。
3. 最后用轻量协议 Robot 单独压后端接口极限。

## 10. 单 Cell 回归压测节奏(每周固定)

> 缘起:`scale-cellular-20m.md` §7 把「单 Cell ~40 万 CCU 压测 + 对比表」定为阶段1→阶段2 的**一次性上线验收门槛**。蜂窝扩容服务内代码(④~⑱ + `pkg/cellroute` 地基)已全部落地,故把这道门槛**升级为每周固定回归基线**:既能持续守住性能、尽早暴露退化,又天然复用 §4.1 的 `prev-summary` 周对周对比机制。

### 10.1 节奏与产物

- **频率**:每周一轮,固定档期(建议每周一稳定窗口跑,周内不被需求挤掉)。
- **baseline**:上一周 `summary.txt` 即本周 `prev-summary.txt`(§4.1),**每周一个 baseline**,周对周二维对比。
- **产物**:`docs/design/stress-<round>-cell-weekly-<date>.md`(命名沿用 §7),含 summarize 五段表 + vs 上周对比 + 瓶颈/决策行;同步追加 `PROGRESS.md`。
- **退化即停**:任一关键指标(match.found 链路 / `ds_pod_ready_p99` / `hub_player_count` / battle_result kafka lag)较上周明显劣化,**先定位再继续迭代**,不许带着退化往下压。

### 10.2 规模随环境(重要边界)

- §7 阶段1 的「单 Cell ~40 万 CCU」是**大规模目标**,需相应规模环境(多机/集群,属基础设施,Codex/人,AGENTS.md §11.1)。
- 该环境就绪前,每周跑**力所能及规模的回归压测**(当前 dev/小规模),规模随环境能力逐步抬升。
- **达到 ~40 万 CCU 且出对比表前,「每周都在压」≠「已过阶段1 验收门槛」**;阶段门槛仍按 §7 单独验收,不因周回归已常态化而默认通过。

### 10.3 每周执行清单(在 §4.4 基础上)

```
[ ] 沿用 §4.4 完整清单(prev-summary / 清库 / 清 DS / snapshot×3 / 五段表 / 对比表 / 决策行 / PROGRESS)
[ ] 本周 RunDir 命名带周次/日期,baseline 取上周 summary
[ ] 对比维度固定:match.found 链路 / ds_pod_ready_p99 / hub_player_count / battle_result kafka lag
[ ] 规模标注当前环境上限,不把小规模结论冒充 40 万 CCU 验收
[ ] 出现退化先开 issue 定位,再决定是否继续迭代
```

### 10.4 执行分工

- **实际每周跑压测 = 环境操作**:清库/起服务/拉 DS/抓 snapshot 属 Codex/人(AGENTS.md §11.1)。
- **本纪律 + summarize/对比分析 + 退化定位**:Claude 可参与(纯文档/脚本/分析,不碰环境)。
