# battle_result

> 战斗结算服务:接 Battle DS 的对局结算(Model-B `ReportResult`)与战斗中实时进度
> (`ReportProgress`),**幂等落库** + **服务端算 MMR(Elo)** + 掉落/经验发放 + DS 崩溃补偿,
> 并把段位事件、装备掉落、撮合释放、终态资源回收全部经 **MySQL 事务出箱**可靠投递下游。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见
> `docs/design` 的 [`go-services.md §2.13`](../../../docs/design/go-services.md)、
> [`realtime-progression.md`](../../../docs/design/realtime-progression.md)、
> [`decision-revisit-ds-callback-auth.md`](../../../docs/design/decision-revisit-ds-callback-auth.md);
> 本 README 只**链接**它们,不重新论证。
>
> 代码行号锚点以**函数名**为准,行号截至当前 HEAD(会随改动漂移)。

## 职责与边界

- **职责**:对局结算幂等落库(不变量 §2:同一 `match_id` 只落一次)+ 服务端 Elo MMR 换算
  (不变量 §6:不信 DS 上报的 `mmr_delta`)+ 战斗中实时经验/掉落发放 + DS `ABANDONED` 崩溃补偿
  (不变量 §4)+ 战绩查询。
- **权威态**:对局头 `battles` / 玩家战绩 `battle_player_stats` / 实时进度水位
  `battle_progress_stream` 全在 **MySQL `pandora_battle` 库**(强依赖,结算落库不可降级)。
- **DS 不可信(不变量 §6)**:MMR 只由**胜负事实**在本服算;掉落只发放 `drop_whitelist` 白名单内
  的 `item_config_id`;经验只由**怪物击杀事实**查服务端换算表折算;`game_mode` / `map_id` 一律以
  服务端 canonical `BattleStorageRecord` 覆盖 DS 请求体。
- **不做的事**:不写玩家段位/经验的权威库(只发 `player.update` 出箱事件 + 调 `player.AddExperience`,
  由 player 服务落库)、不改玩家背包权威(调 `inventory.GrantInstances`)、不签发 DS 令牌
  (**verify-only**:令牌由 ds_allocator 签发)、不做撮合(结算后调 `matchmaker.ReleaseMatch` 释放)。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:50022` | DS 回调 RPC(`ReportResult` / `ReportProgress`,经 Envoy `:8444`)+ 内部查询 RPC |
| HTTP | `:51022` | 仅 `/metrics`(`battle.proto` 无 `google.api.http` 注解,无 RESTful RPC) |

端口来自 `internal/conf/conf.go` 的 `Defaults()`(`Server.Grpc.Addr` / `Server.Http.Addr`),
与 [`infra.md`](../../../docs/design/infra.md) §6 登记一致。

## 对外接口

代码入口:`internal/service/battle_result.go`(实现 `battlev1.BattleResultServiceServer`;gRPC server
挂 `pmw.AuthOptional()`,**本服不从 ctx 取 `player_id`**,身份完全靠 DS 回调令牌 Guard,见
`internal/server/grpc.go`)。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `ReportResult(result, final_progress_seq)` | Battle DS(内部) | 上报一场对局结算,幂等落库 + 算 MMR + 组织下游出箱 | **DS battle 令牌**(Guard `RequireToken`);`authority_mode=redis` 时再过 Redis active 精确校验。**拒绝玩家 JWT**(无令牌直连一律拒) |
| `ReportProgress(match_id, events)` | Battle DS(内部) | 战斗中实时进度事实批(击杀/拾取,`seq` 幂等) | 同上;roster 取自权威 `BattleStorageRecord`,非本场玩家事实直接拒 |
| `GetMatchResult(match_id)` | 后端内部 / 运维 | 查一场对局结算(含全部玩家战绩) | `AuthOptional`(不认 JWT 身份,`match_id` 由入参给出) |
| `ListPlayerHistory(player_id, limit, before_ms)` | 后端内部 / 运维 | 倒序游标列出玩家战绩历史 | `AuthOptional`(`player_id` 由入参给出) |

> **`ReportResult` / `ReportProgress` 是 DS 专用回调**:`middleware.DSScope{Type: DSTypeBattle,
> MatchID, RequireToken: true}` 把令牌 `match_id` 绑定到上报的 `match_id`(防拿 A 局令牌伪造 B 局
> 结算);`enforce` 模式下无令牌的东西向旁路直连被拒(堵绕过 Envoy)。这两个 RPC 不看玩家 JWT,
> 玩家侧无法调用。
>
> **另有一条 Kafka 结算入口(legacy)**:`pandora.battle.result` topic → `BattleResultHandler`。它**不带**
> Model-B 可核验凭据,故 `authority_mode=redis` 时被 `Config.ValidateRedisAuthorityIngress` 启动期
> **禁止**(见配置项);Redis 权威下唯一结算入口是受 Guard + Redis active + receipt 保护的同步
> `ReportResult` RPC。

## 目录结构(Kratos 标准分层,对齐 matchmaker / login)

```
cmd/battle_result/main.go       启动入口(MySQL/Redis/Kafka 装配 + 6 个后台 worker + dsauth 写者 fence)
etc/battle_result-dev.yaml      开发期配置(progress_enabled=true,单副本无混版窗口)
etc/battle_result-prod.yaml.example  生产配置样例(混版发布纪律)
internal/
  conf/conf.go                  配置结构 + Defaults() + ValidateRedisAuthorityIngress()
  service/
    battle_result.go            RPC 入口(ReportResult/ReportProgress/GetMatchResult/ListPlayerHistory)
    battle_credential.go        Redis active credential 终态门(AuthorizeResult 精确比对 → terminal 证明)
  biz/
    battle_result.go            BattleResultUsecase 核心(reportResult/HandleAbandoned/MMR/各出箱发布器)
    settlement.go               跨 region 结算回流落点观测 + 幂等键口径(nil-safe)
    progress.go                 实时进度通道(ReportProgress 校验/换算/水位裁决 + 进度出箱发布器)
    mmr.go                      Elo MMR 纯函数(eloDeltas / reasonForTeam)
    consumer.go                 kafka handler(BattleResultHandler / DSLifecycleHandler)
    retention.go                保留期清理(battles/stats + 已结算进度水位,§9.24)
  data/
    battle_repo.go              MySQL 结算落库 + 各出箱表 CRUD + 保留期批删(SaveResult 事务)
    progress_repo.go            进度水位 CAS + 累计上限判定 + 进度出箱(ApplyProgress / settleProgressStreamTx)
    battle_auth.go              Redis 授权记录只读校验(GetBattleAuthority)+ 结算 receipt(RecordBattleResult)
    terminal_releaser.go        ds_allocator 终态回收 gRPC client(ReleaseTerminal/FinalizeTerminal)
    terminal_release_schema.go  terminal_release_outbox schema 启动探测
    match_releaser.go           matchmaker.ReleaseMatch gRPC client
    mmr_reader.go               player 服务 MMR gRPC reader(弱依赖,空则 StaticMMRReader)
    inventory_client.go         inventory.GrantInstances gRPC client(掉落/进度装备发放)
    experience_client.go        player.AddExperience gRPC client(实时击杀经验)
    mail_client.go              mail.SendPersonalMail gRPC client(背包满溢出转邮件)
  server/                       grpc / http server 注册(http 仅 /metrics)
```

## 核心调用链

### 1. ReportResult —— Model-B 授权结算(唯一权威入口)

`service.ReportResult`(`internal/service/battle_result.go`,`ReportResult`)→ 逐层鉴权 → biz 落库:

```
ReportResult(result, final_progress_seq)
├─ dsGuard.CheckBattleCredential(DSScope{Battle, match_id, RequireToken})   验签 DS 令牌 + 绑 match_id
├─ battleCredentialChecker.AuthorizeResult(match_id, credential)            [authority_mode=redis]
│    └─ reader.GetBattleAuthority(match_id)  读 Redis 授权 + canonical BattleStorageRecord
│         └─ 精确比对 pod/uid/epoch/gen/jti/token_sha256/writer_epoch/心跳龄  任一不符 → ErrUnauthorized
│         └─ 产出 TerminalReleaseRecord{roster, game_mode, map_id, auth 证明}  ← 只来自服务端快照
├─ pod_mismatch 门:result.ds_pod_name 必须 == credential.Pod
└─ uc.ReportAuthorizedResult(result, terminalRelease, final_progress_seq)
     ├─ validateAuthorizedResultRoster   result.stats 集合必须 == 权威 roster(拒缺员/外人/重复)
     └─ reportResult(...)                 见下
```

`biz.reportResult`(`internal/biz/battle_result.go`,`reportResult`)是结算主干:

1. **权威字段覆盖**(§9.6):`terminalRelease != nil` 时用 canonical `game_mode` / `map_id` 覆盖 DS 请求体
   (canonical 为空的旧局也照覆盖为空,绝不信请求)。
2. **MMR 决策**(`assignMMR`,`battle_result.go`;纯函数 `eloDeltas` / `reasonForTeam` 在 `mmr.go`):`ABANDONED` → `mmr_delta` 全 0;canonical
   `pve_coop`(仅授权路径可判定,`decision-dungeon-entry-modes.md`)→ 全 0 且不碰 MMR reader;其余
   → 按两队当前 MMR 均值算 Elo,覆盖 DS 上报的 `mmr_delta`。
3. **组装出箱**:`buildOutbox`(每玩家一条 `player.update` 事件)+ `buildDropOutbox`(按 `drop_whitelist`
   过滤 + 每玩家 `MaxDropsPerPlayer()` 截断)+ `prepareTerminalRelease`(给终态回收证明打 `release_after_ms`
   宽限)。
4. **原子落库** `repo.SaveResult`(见下)。
5. 落库后 `reconcileProgress`(对账 DS `final_progress_seq` vs 服务端水位)+ `MarkResultRecorded`
   (best-effort receipt,失败也回 OK,后台 relay 兜底)。

### 2. SaveResult —— 六件事同一 MySQL 事务原子提交

`data.SaveResult`(`internal/data/battle_repo.go`,`SaveResult`)一个事务内(不变量 §4 不半成功):

```
BEGIN
├─ INSERT battles (PK match_id)                      幂等键;命中 1062 → alreadyRecorded=true 走重放分支
├─ INSERT battle_player_stats × N                    玩家战绩 + 最终 mmr_delta
├─ INSERT player_update_outbox × N                   段位事件出箱(→ worker 1)
├─ settleProgressStreamTx(match_id, final_seq)       ★ 收口实时进度水位:打终局标记(fencing 迟到进度)
│                                                     + 返回 DropsSuppressed(水位>0 → 掉落已归实时通道)
├─ INSERT battle_drop_outbox × N   [!DropsSuppressed] 装备掉落出箱(→ worker 2);已归实时通道则跳过只审计
├─ INSERT terminal_release_outbox  [terminalRelease] 终态资源回收证明(→ worker 6)
└─ INSERT match_release_outbox                        撮合释放出箱(→ worker 4)
COMMIT
```

幂等重放分支(`INSERT battles` 撞唯一键):仍恢复可能缺失的 `match_release_outbox` +
`settleProgressStreamTx` 收口(防旧副本首落时未打终局标记),返回 `(true, …)` 不重复写。

### 3. ReportProgress —— 战斗中实时进度(seq 幂等)

`service.ReportProgress` → Guard + `AuthorizeResult`(取权威 roster)→ `biz.ReportProgress`
(`internal/biz/progress.go`,`ReportProgress`):

```
ReportProgress(match_id, roster, events)
├─ repo.GetProgressWatermark(match_id)   读水位裁决:
│    ├─ Settled  → ErrInvalidState(结算后迟到 = 僵尸/分区恢复 DS,fencing)
│    ├─ Stopped  → ErrInvalidState(未知事实已停流,禁止重开)
│    └─ !Existed && !ProgressEnabled → ClaimProgressLegacy 固化本场 legacy 模式 → ErrInvalidState
├─ 逐事件:seq 严格升序 + ≤ MaxProgressSeqPerMatch;roster 成员校验(非本场玩家 → ErrUnauthorized)
│    ├─ MonsterKill → 计入 killsByPlayer;MonsterExpOf 查换算表(未配置怪跳过告警,水位照推)
│    ├─ ItemPickup  → IsDroppable 白名单(非白名单跳过告警);每拾取事实一行进度出箱
│    └─ 未知事实类型 → MarkProgressStopped 持久停流 + ErrInvalidState(新 DS 对旧 Go,能力不匹配)
└─ repo.ApplyProgress(expectedSeq, newSeq, 批 delta, caps)   ★ 事务内:
     ├─ 水位乐观 CAS(WHERE last_applied_seq=expected AND settled=0 AND stopped=0)
     ├─ 单场 + 单玩家累计上限在**同一事务一致快照**判定(§16.1 TOCTOU:不得事务外先判)
     └─ INSERT battle_progress_outbox(→ worker 3)
```

错误语义即 DS 侧行为契约(见 `progress.go` 抬头):`ErrUnavailable` DS 原批重试 /
`ErrInvalidArg`·`ErrUnauthorized` 丢批继续 / `ErrInvalidState` 停流不重试。

### 4. HandleAbandoned —— DS 崩溃补偿(不变量 §4)

`consumer.DSLifecycleHandler` 消费 `pandora.ds.lifecycle` 的 `ABANDONED` 阶段 →
`biz.HandleAbandoned`(`internal/biz/battle_result.go`,`HandleAbandoned`):写 `outcome=ABANDONED`、
`mmr_delta` 全 0 的补偿记录(幂等),`final_progress_seq=0` 收口进度水位(不掉段,崩溃前已入账的
经验/掉落按需求保留不回滚)。

### 5. 后台 worker(main 装配的 6 个循环 + 2 个 Kafka consumer)

`cmd/battle_result/main.go` 起 `go uc.Run*`;各 worker 轮询各自出箱表,**投递明确成功才删行**
(at-least-once + 下游幂等键去重),多副本各自跑幂等无锁:

| worker | 出箱表 | 下游 | 语义 |
|---|---|---|---|
| `RunOutboxPublisher` | `player_update_outbox` | Kafka `pandora.player.update`(key=player_id) | 段位事件,FIFO 保序,失败中断本轮保留重试 |
| `RunDropPublisher` | `battle_drop_outbox` | `inventory.GrantInstances` | 装备掉落;背包满且配 mail → 溢出转个人邮件(同键去重),单行失败不阻塞他人 |
| `RunProgressPublisher` | `battle_progress_outbox` | `player.AddExperience` / `inventory.GrantInstances` | 实时经验/掉落;单行失败指数退避推迟防队首阻塞 |
| `RunMatchReleasePublisher` | `match_release_outbox` | `matchmaker.ReleaseMatch` | 释放残留 claim/票据/match(修复结算回 Hub 后 4002 无法再匹配),失败指数退避 |
| `RunTerminalReleasePublisher` | `terminal_release_outbox` | `ds_allocator`(两阶段:ReleaseTerminal → FinalizeTerminal) | Model-B 终态资源回收 + UID 回收;`authority_mode=redis` 才启动 |
| `RunRetentionSweep` | — | MySQL 批删 | 保留期清理(见下) |

Kafka consumer(`mustBuildConsumers`,每 topic 一个,失败 3 次进 DLQ):`BattleResultHandler`
(`pandora.battle.result`,**legacy 才注册**)、`DSLifecycleHandler`(`pandora.ds.lifecycle`,恒订阅)。

## DS 授权 / fencing 模型

`battle_result` 对 DS 回调是 **verify-only**(不签票),三段门(`authority_mode=redis` 全开):

1. **令牌门**(`DSCallbackGuard`,`ds_auth.mode`):`off` 不校验 / `permissive` 灰度观察 / `enforce` 强制;
   `RequireToken` 拒无令牌直连。
2. **Redis active 门**(`battle_credential.go`,`AuthorizeResult`):已验签令牌必须此刻仍**逐字段等于**
   Redis 权威 `BattleDSAuthStorageRecord` + `BattleStorageRecord`(pod / gameserver_uid / instance_epoch /
   gen / jti / token_sha256 / writer_epoch,且 `last_active_heartbeat` 未超 `active_heartbeat_max_age`)。
   证明只从服务端快照构造,`roster` / `game_mode` / `map_id` 绝不取 DS 请求体。
3. **写者 epoch fence**(`dsauthfence.AcquireRuntime`,feature `battle-terminal-outbox-v1`,etcd 租约):
   `authority_mode=redis` 启动时抢占;`fence.Lost()` → 进程 `os.Exit(1)`,禁止失租/旧 epoch 副本继续结算
   (不变量 §9.22 / §16.8)。**注意:这不是单 leader 选举**——同 epoch 多副本可并存,出箱 worker 靠幂等并发。

进度/结算的**终局标记**(`battle_progress_stream.settled_at_ms` / `stopped_at_ms`)是另一层 fencing:
结算后或停流后,僵尸 / 分区恢复 DS 的迟到 `ReportProgress` 一律拒。

## 存储布局(MySQL `pandora_battle`)

| 表 | 键 | 用途 |
|---|---|---|
| `battles` | PK `match_id` | 对局结算头(幂等键,不变量 §2) |
| `battle_player_stats` | uk `match_id+player_id` | 玩家战绩 + `mmr_delta` |
| `player_update_outbox` | id | 段位事件事务出箱 |
| `battle_drop_outbox` | id | 装备掉落事务出箱(`item_config_ids` CSV) |
| `terminal_release_outbox` | id | Model-B 终态资源回收证明出箱 |
| `match_release_outbox` | uk `match_id` | matchmaker 撮合释放出箱 |
| `battle_progress_stream` | PK `match_id` | 实时进度水位 + `settled_at_ms` / `stopped_at_ms` 终局/停流标记 |
| `battle_progress_player` | uk `match_id+player_id` | 单玩家累计(经验/件数/击杀,单玩家上限依据) |
| `battle_progress_outbox` | uk `match+seq+player+kind` | 实时经验/掉落发放事务出箱 |

Redis(`authority_mode=redis` 才连):**只读**校验 DS 授权记录(`GetBattleAuthority`)+ best-effort
写结算 receipt(`RecordBattleResult`);authority 记录由 ds_allocator 等 owner 权威写,本服不写。

启动期 schema 探测:`ValidateRecoveryOutboxSchema` / `ValidateProgressSchema` 恒探测(每次结算都访问,
不能等首个结算才炸,§16.4);`ValidateTerminalReleaseSchema` 仅 `authority_mode=redis` 探测。

## 保留期清理(不变量 §9.24)

**产品口径:MySQL 里最多只存最近六个月的战报,超过六个月的就没有数据**(2026-08-03 用户指令,
§9.24 登记例外)。同一个 `HistoryRetentionDays` 既是清理 cutoff,也是玩家战报可见窗口
(`ListPlayerHistory` 只读 MySQL,没有冷存归档)。

⚠️ **本服 `retention_mode` 留空即 `delete`(真删)**,与其它域的 `report_only` 全局默认相反:
战报是产品上就有寿命的数据,只报告不删交付不了"六个月后没有数据"。拼错的模式值启动即
fail-fast 拒启(`ValidateRetentionMode`),避免六个月口径静默失效。排查误删嫌疑时显式配
`retention_mode: report_only` 立即停删(只统计待清理量打 WARN)。

`RunRetentionSweep` → `sweepRetentionOnce`(`internal/biz/retention.go`)每 `RetentionSweepInterval`
(默认 1h)小批量批删超 `HistoryRetentionDays`(默认 180,钳 `[30,180]`)的行:

- `battles` + `battle_player_stats`:按**服务端落库时间 `created_at`** 超期同事务批删(§9.6 不信 DS 上报的
  `ended_at_ms`,防伪造提前删/永不删)。
- `battle_progress_stream` + `battle_progress_player`:仅删 `settled_at_ms>0`(已结算)且超期的行。
- **陈年未结算水位**(`settled_at_ms=0` 且超期)= 结算补偿链 bug 证据,**永不自动清**,每轮打 `Error` 告警暴露。

出箱表投递成功即删,不在清理范围(积压属告警问题,不是增长问题)。

## 配置项(`internal/conf/conf.go`,`BattleConf`,默认值来自 `Defaults()`)

| 键(`battle.*`) | 默认 | 说明 |
|---|---|---|
| `elo_k_factor` | `32` | Elo K 系数(胜负 MMR 变化幅度上限 ≈ K) |
| `base_mmr` | `1500` | 玩家缺省 MMR(`player_addr` 空 → `StaticMMRReader` 全返此值) |
| `consume_topics` | `[battle.result, ds.lifecycle]` | 订阅 topic;`authority_mode=redis` 禁 `battle.result`(启动校验拒) |
| `player_addr` | 空 | player gRPC(弱依赖):MMR reader + 经验入账器;空则静态 MMR + 经验积压不发 |
| `matchmaker_addr` | 空 | matchmaker gRPC:释放撮合状态;`authority_mode=redis` 时为**强依赖**(空则启动失败) |
| `ds_allocator_addr` | 空 | 终态回收 relay;`authority_mode=redis` 时为**强依赖** |
| `inventory_addr` | 空 | inventory gRPC(弱依赖):掉落/进度装备发放;空则出箱积压不丢 |
| `mail_addr` | 空 | mail gRPC(弱依赖):背包满溢出转个人邮件 |
| `drop_whitelist` | 空 | 可掉落 `item_config_id` 白名单(DS 不可信);**空 = 一律不发放** |
| `max_drop_per_player` | `32` | 单场单玩家最多入库掉落条数(硬上限 46,超列宽) |
| `outbox_publish_interval` / `outbox_batch_size` | `2s` / `128` | `player.update` 出箱发布节奏 |
| `drop_publish_interval` / `drop_batch_size` | `2s` / `128` | 掉落出箱发布节奏 |
| `terminal_release_interval` / `_batch_size` / `_grace` | `2s` / `128` / `15s` | 终态回收节奏 + DS 通知客户端宽限窗(钳 `[5s,2m]`) |
| `progress_enabled` | `false` | 实时进度通道开关(§14.2 默认不改现有行为;**混版 P0**:全 fleet 升级后才可置 true) |
| `monster_exp` | `{}` | 怪物击杀经验表 `monster_config_id → exp`(空 = 击杀不折算经验,跳过告警) |
| `max_progress_batch` | `256` | 单批事件条数上限 |
| `max_progress_seq_per_match` | `100000` | 单场事件 seq 硬上限 |
| `max_kill_count_per_fact` / `max_pickup_count_per_fact` | `100` / `10` | 单事实计数上限(pickup 钳 ≤46) |
| `max_progress_exp_per_match` / `_items_per_match` | `1000000` / `500` | 单场累计硬上限(反作弊,事务权威侧封顶) |
| `max_progress_exp_per_player` / `_items_per_player` / `_kills_per_player` | `200000` / `100` / `1000` | 单场单玩家累计硬上限 |
| `progress_publish_interval` / `progress_batch_size` | `1s` / `128` | 进度出箱发布节奏 |
| `history_retention_days` | `180` | 战报(结算 + 已结算进度水位)保留天数 = 玩家可见窗口;钳 `[30,180]`(§9.24 登记例外) |
| `retention_mode` | `delete` | **本服留空即真删**(与全局 `report_only` 默认相反);`report_only` 停删只告警;拼错拒启 |
| `retention_sweep_interval` / `retention_sweep_batch` | `1h` / `200` | 保留期清理节奏(每轮循环删到追平,批间独立短事务) |

DS 回调鉴权在顶层 `ds_auth`(`config.DSAuthConf`):`mode`(off/permissive/enforce)、`authority_mode`
(legacy/redis)、`active_heartbeat_max_age`、`secret`(须与 ds_allocator 一致)、`fence.*`(etcd 租约)。

## 本地启动

```powershell
# 1. 基础设施(MySQL pandora_battle + Redis + Kafka;起 player/matchmaker/inventory/mail/ds_allocator 后可跑全链)
pwsh tools/scripts/dev_up.ps1

# 2. 启 battle_result(dev 配置:progress_enabled=true,ds_auth.mode=off/legacy)
go run ./services/battle/battle_result/cmd/battle_result -conf services/battle/battle_result/etc/battle_result-dev.yaml
```

> dev 配置为 `ds_auth.mode: off` + `authority_mode: legacy`(消费 `pandora.battle.result` 直接落库,
> 便于本地联调);生产走 `enforce` + `redis` 权威,唯一结算入口是受 Guard 保护的同步 `ReportResult`。
> 混版发布纪律(`progress_enabled` / `authority_mode` 切换)见 `etc/battle_result-prod.yaml.example`。

## 关联文档

- [`go-services.md §2.13`](../../../docs/design/go-services.md) — battle_result 要约(职责 / 数据流 / 端口)
- [`realtime-progression.md`](../../../docs/design/realtime-progression.md) — 战斗中实时进度通道(§3 幂等 seq / §5 单一权威路径 / §9 尾窗残余风险)
- [`decision-revisit-ds-callback-auth.md`](../../../docs/design/decision-revisit-ds-callback-auth.md) — Model-B DS 回调令牌 / Redis active 授权 / writer epoch fence
- [`owner-authority.md`](../../../docs/design/owner-authority.md) — §9.22 owner_epoch / lease fencing 权威本体(终态回收依赖)
- [`decision-dungeon-entry-modes.md`](../../../docs/design/decision-dungeon-entry-modes.md) — canonical `pve_coop` game_mode(PVE 副本不算 Elo)
- [`scale-cellular-20m.md`](../../../docs/design/scale-cellular-20m.md) §4.4 — 跨 region 结算回流落点
- [`infra.md`](../../../docs/design/infra.md) — 端口 / topic / MySQL 库表登记
