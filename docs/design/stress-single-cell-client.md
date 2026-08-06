# 单 Cell 压测客户端方案(stress-single-cell-client)

> 状态:设计稿(2026-06-26,Claude 出方案,待人确认机器/成本后由 Codex 接 harness/ps1 编排)。
> 关联:`docs/design/stress-discipline.md`(压测纪律)、`docs/design/scale-cellular-20m.md` §7(阶段纪律)、
> `PROGRESS.md`「Codex 阶段 1 压测预检」条目。
>
> 本文档只定义**轻量协议 Go Robot**(stress-discipline.md §9 第 3 层)的压测客户端,用于阶段 1
> 单 Cell ~40 万 CCU 后端链路压测。**不含** UE Client Bot / 服务端 Bot(§9 第 1、2 层,后期 UE 仓库做)。

## 1. 目标与非目标

### 1.1 目标(本客户端验证什么)

阶段 1 要回答的唯一问题:**单 Cell(单 region 单 cell,router 注入但只有一个落点)在 ~40 万 CCU 大厅
稳态 + 持续匹配/进战流量下,后端 16 个 Go 服务的链路是否扛得住、瓶颈在哪**。对应 §8 必看指标:
`match.found` 链路 / `hub_player_count` / `ds_pod_ready_p99` / matchmaker 队列 / battle_result kafka lag。

因此本客户端压**后端 gRPC 入口**,覆盖真实大厅在线玩家的后端行为:

- 登录(login `Login`,签 session JWT + 拿 hub 票据 + region/cell 落点)
- 进大厅后建立 push 长连接(push `Subscribe` server stream,这是 40 万 CCU 的**连接数主体**)
- 大厅态周期行为:档案读、队伍(team)、社交(friend/chat 抽样)、心跳式 locator 刷新
- 匹配链路:matchmaker `StartMatch` → 轮询 `GetMatchProgress` / 等 push → `ConfirmMatch`
- 战斗结算回流:battle_result `ReportResult`(robot 模拟 DS 同步上报用法,见 battle.proto 注释)
  → 段位经 `pandora.player.update` 幂等更新

### 1.2 非目标(本客户端**不**验证)

- ❌ UE Replication / GAS / Iris / NetCullDistance —— 属 §9 第 1、2 层(UE 仓库 StressBotManager /
  无渲染 UE Client Bot),本 Go Robot **不碰**,DS 侧指标(§5 段 5)阶段 1 用 stub/占位或留空。
- ❌ 真实 UE Battle DS 拉起的端到端时延 —— 阶段 1 本地 Agones 若不开,DS 链路留空/占位;
  真 DS 压测留到 §9 第 2 层。
- ❌ 客户端渲染带宽 / NetDriver 握手 —— 同上。
- ❌ Envoy gRPC-Web 转换层极限 —— 见 §4.2,阶段 1 主链路直连 gRPC 端口,Envoy 单列一组对照样本。

## 2. 分层定位(对齐 stress-discipline.md §9)

| §9 层级 | 本方案 | 何时做 | 谁做 |
|---|---|---|---|
| 第 3 层 轻量协议 Robot | ⭐ **本文档** Go Robot | 阶段 1(现在) | Claude 出方案 + 审,Codex 接 harness/ps1 |
| 第 2 层 无渲染 UE Client Bot | 不在本方案 | 阶段 1 通过后 | UE 仓库 + 人 |
| 第 1 层 服务端 Bot(DS 内) | 不在本方案 | 500 人 hub 里程碑 | UE 仓库 + 人 |

**结论**:本 Go Robot 出的结论只对"后端 API/链路 QPS 与时延"成立,**不得**用它声明 UE/DS/Replication 性能。

## 3. 业务流(虚拟用户状态机)

每个虚拟用户(VU)是一个状态机 goroutine,鉴权与连接模型见 §4。

```
                ┌────────────┐
   (1) Login    │  CONNECTING │  login.Login(account=stress_<id>, devSkipPassword)
   ───────────▶ │            │  → session_token / hub_addr / region / cell
                └─────┬──────┘
                      │ ok
                ┌─────▼──────┐
   (2) 进大厅    │   LOBBY     │  push.Subscribe(stream)  ← 长连接,贯穿整个会话
                │            │  player.GetProfile / GetMyTeam(冷启动各 1 次)
                └─────┬──────┘
                      │ 稳态循环(泊松间隔,见 §6 行为权重)
        ┌─────────────┼──────────────┬───────────────┐
        ▼             ▼              ▼               ▼
  (3a) 大厅心跳   (3b) 社交        (3c) 经济抽样     (3d) 组队+匹配
  locator 刷新   friend/chat     auction/trade    team→matchmaker
        │             │              │               │
        └─────────────┴──────────────┴───────┬───────┘
                                              │ 命中匹配的 VU 子集
                                        ┌─────▼──────┐
   (4) 匹配        StartMatch → 轮询 GetMatchProgress / 等 push MATCH_FOUND
                  → ConfirmMatch
                                        └─────┬──────┘
                                              │ matched
                                        ┌─────▼──────┐
   (5) 战斗结算    robot 扮 DS:battle_result.ReportResult(match_id 幂等)
                  → 段位 update → 回 (2) LOBBY
                                        └────────────┘
```

关键点:
- **push 长连接是 CCU 主体**:40 万 VU = 40 万条 push `Subscribe` stream 常驻。这是真正压"连接数"的地方,
  不是 QPS。matchmaker/battle 的 QPS 只由进战子集贡献(见 §6 比例)。
- **匹配只发生在一部分 VU**:稳态下大多数 VU 在大厅闲逛,只有 X%(可配,默认 10%)在排队/打架。
  这贴合 MOBA「大厅 500 人自由 PvP + 少量进 5v5」的真实分布(pvp-rules.md)。
- **battle_result 用同步 ReportResult**:阶段 1 没有真 UE DS 上报 kafka envelope,robot 直接调
  battle.proto 里"同步上报用法"的 `ReportResult`,保证 MMR/结算/幂等链路被压到(不变量 §2/§6)。

## 4. 鉴权与连接模型

### 4.1 直连 gRPC(主链路,阶段 1 默认)

robot 直连各 go 服务 gRPC 端口(`20001`-`20022`,见 infra.md §6.2),在每次 RPC 的 metadata 注入:

- `x-pandora-player-id: <player_id>` —— pkg/middleware/auth.go 直接认这个头(Envoy/gateway 鉴权后注入语义)。
  阶段 1 robot 自己造,**绕过 Envoy**,把后端 API 压满,不被 TLS/grpc-web 转换稀释。
- `x-pandora-trace-id: <uuid>` —— pkg/middleware/trace.go,全链路排查(不变量 §8 所有写带 trace_id)。

login 仍走真实 `Login`(devSkipPassword=true,开发期免密,见 login.go),拿到真实 player_id 后续 RPC 用。

### 4.2 经 Envoy(对照样本,小比例)

为量化 Envoy gRPC-Web 转换开销,留一个**小比例 VU 组(默认 1%,可配)**走 Envoy `8443` TLS + JWT,
和直连组对照(同样行为)。这组不计入 40 万主压力,只作单列样本。**不在阶段 1 主结论里**,
避免把 Envoy 开销混进后端结论(§6 反模式:不混不同压力来源)。

### 4.3 连接复用

- gRPC unary 调用:每台压测机对每个目标服务维持一个 **共享 `grpc.ClientConn`**(HTTP/2 多路复用),
  VU 复用,不每 VU 一条连接(否则 40 万条 TCP 直接打爆)。
- push `Subscribe`:**每 VU 一条 stream**(这是被测目标,必须真实);stream 复用底层 ClientConn 的 HTTP/2
  多路复用,但单条 HTTP/2 连接的并发 stream 上限要调(`MaxConcurrentStreams`),见 §5 机器拆分。

## 5. 40 万 CCU 机器拆分(需人决策成本)

⚠️ **单机造不出 40 万 CCU**。push 是 server stream 长连接,40 万条 stream + 心跳 + 周期 RPC 的内存/FD/
goroutine 开销必须横向铺开。下表是**估算起点**,实际机器数/规格由人按预算定(§11.1 性能决策归人):

| 维度 | 估算 | 说明 |
|---|---|---|
| 单机 VU 上限 | 2.5 万 ~ 5 万 | 受 goroutine(每 VU 至少 1 读 stream + 1 行为)、FD、内存约束;需先用 P0 冒烟实测 |
| 压测机数量 | 8 ~ 16 台 | 40 万 ÷ 单机上限;留 20% 余量 |
| 单机网卡 | ≥ 1Gbps | push 帧小但量大,心跳 + 周期 RPC 累积 |
| HTTP/2 ClientConn | 每服务多条 | 单 conn `MaxConcurrentStreams` 默认 100/250,40 万 stream 要按 `ceil(VU/上限)` 开多条 conn |
| 被测端单 Cell | 16 go 服务单实例 | 阶段 1 不分片,router 注入单 (region,cell);DS 用 stub |

ramp 与时长(对齐 §4.2 snapshot 阶段 `2,5,10,15,18`):

- ramp:0 → 40 万,**10 分钟线性爬**(避免瞬时 connection storm 失真;每秒约 +650 VU)
- 稳态:ramp 完成后**至少 15 分钟**;snapshot 抓 ramp 完(~t10m)、稳态中(~t15m)、稳态末(~t18m)
- 进战子集:稳态期持续 10% VU 在匹配/结算循环

## 6. VU 行为权重(稳态循环)

每个大厅 VU 按泊松间隔(默认均值 30s)抽一个行为执行,权重可配(下为默认):

| 行为 | 权重 | 调用 | 压什么 |
|---|---|---|---|
| 大厅心跳 / locator 刷新 | 40% | player_locator `SetLocation` | locator owner cell 写 + presence |
| 档案/队伍只读 | 20% | player `GetProfile` / team `GetMyTeam` | data cache-aside 读、team 读 |
| 社交抽样 | 15% | friend `ListFriends` / chat `SendMessage` | 社交分片 + push 投递 |
| 经济抽样 | 15% | auction `ListMarket` / `PlaceOrder` 抽样 | 撮合单写者 + 结算幂等 |
| 进入匹配 | 10% | team→matchmaker→confirm→battle_result | ⭐ 核心链路 match.found / MMR |

进战的 10% 子集走完整 (4)(5) 链路;其余在大厅循环。比例可按压测目标调,但**单轮内不许中途改**(§6 反模式)。

## 7. Robot 工程结构(Codex 接 harness 时落地)

新建 go module:`robot/stress`(go.work 加 `use ./robot/stress`),module path
`github.com/luyuancpp/pandora/robot/stress`。依赖 `proto`(复用生成的 pb client)与 `pkg`(log/metrics/trace)。

```
robot/stress/
├── go.mod
├── cmd/
│   └── stressbot/main.go        # 入口:读 config,起 N 个 VU,写 robot-stats.jsonl
├── internal/
│   ├── vu/                      # 虚拟用户状态机(CONNECTING/LOBBY/MATCH/BATTLE)
│   ├── client/                  # gRPC 连接池 + metadata 注入(x-pandora-player-id/trace)
│   ├── behavior/                # §6 行为权重调度(泊松 + 加权随机)
│   ├── scenario/               # 场景编排(single-cell-40w.yaml 对应的 Go 装配)
│   └── stats/                   # 每分钟聚合 → robot-stats.jsonl(对齐 §8 输出格式)
└── config/
    └── single-cell-40w.yaml     # VU 数 / ramp / 稳态 / 行为权重 / 目标地址 / 直连vs Envoy 比例
```

**分工**:
- `vu` / `behavior` / `scenario` 含业务流编排(调哪些 RPC、状态流转)= **业务逻辑,Claude 写或审**。
- `client` 连接池、`stats` 聚合、`main` 进程编排、config 解析 = **ops/脚手架,Codex 可主写,回报 Claude 审**。

## 8. 输出格式(喂给 stress_summarize.ps1 §5 段 1)

robot 每分钟 append 一行到 `robot-stats.jsonl`(每台压测机一份,summarize 汇总):

```jsonc
{
  "ts": "2026-06-26T12:34:00Z",
  "minute": 12,                  // 自 StartTime 起的分钟序号
  "machine": "stress-03",
  "vu_online": 39820,            // 当前在线(push stream 活跃)
  "login_ok": 412, "login_fail": 3,
  "subscribe_active": 39820,     // push 长连接活跃数
  "match_enqueue": 138, "match_confirmed": 130, "match_dispatched": 128,
  "battle_reported": 126,
  "rpc_p50_ms": 4, "rpc_p99_ms": 41,   // robot 侧观测的端到端 RPC 时延
  "errors": { "deadline": 2, "unavailable": 1 }
}
```

字段对齐 §5 段 1「robot 每分钟 stats:在线、登录、匹配、进 DS、断开」。**robot 侧只产这个 jsonl;
服务端时延 / 阶段拆分由 prom snapshot(§5 段 2-4)出,robot 不重复算**(§1 原则 3:不手 grep prom)。

## 9. 阶段实施(先冒烟,再满载)

| 子阶段 | VU | 目的 | 通过条件 |
|---|---|---|---|
| **P0 冒烟** | 100 ~ 1000(单机) | 跑通业务流、连接池、jsonl 输出、metadata 鉴权 | 全链路无 5xx,jsonl 正常,单机 VU 上限实测出来 |
| **P1 单机标定** | 单机推到上限 | 标定单机 VU 容量(定 §5 机器数) | 找到单机 OOM/FD 拐点,反推机器数 |
| **P2 单 Cell 满载** | 40 万(多机) | 阶段 1 正式压测,出对比表 | 满足 stress-discipline.md §4.4 完成清单 |

P0/P1 由 Codex 跑(ops);P2 满载需人确认机器后跑,结论由 Claude 审。

## 10. 与现有脚本/纪律的衔接

本客户端产出 `robot-stats.jsonl`,其余仍按 stress-discipline.md 既有口径,**不重复造**:

- snapshot:`stress_snap.ps1`(待补)拉 `:21001/:21011/:21020/:21022` prom(§3 端口分工)。
- 汇总:`stress_summarize.ps1`(待补)读 prom snapshot + `robot-stats.jsonl` 出五段表。
- 清库:`dev_tools.ps1`(待补)db-reset / kafka-offset-reset / etcd-clear;**停服复用现有
  `run_services.ps1 -Action stop`,不新建 `go_svc_stop.ps1`**(避免与文档示例双份,文档口径待统一)。

## 11. 开放问题(需人/后续决策)

1. **机器与成本**(§5):压测机数量/规格/网络,人按预算定;本地单机只能做 P0/P1。
2. **DS 链路**(§1.2):阶段 1 是否开本地 Agones 跑真 DS,还是全程 stub?建议阶段 1 用 stub(只压后端),
   真 DS 留 §9 第 2 层。需人确认。
3. **stress-discipline.md 脚本名漂移**:文档写 `go_svc_stop.ps1`,实际 `run_services.ps1`;建议改文档复用
   现有脚本,由 Claude 统一(避免双份),改前记此问题。
4. **40 万账号准备**:`stress_<id>` 账号靠 login devAutoRegister 首登即建,还是预灌 MySQL?
   建议 devAutoRegister(login.go 已支持),省预灌;但首登 ramp 期写库压力要计入。
5. **单 Cell 是否注入 router**:阶段 1 单 (region,cell),建议注入 router(单落点)以验证 owner cell
   观测日志链路真的被走到(⑧~⑱ 增量),而非 nil 短路。需在 main 装配确认。

## 12. 分工边界小结(§11.1)

| 事项 | 谁 |
|---|---|
| 本方案 / VU 业务流 / 行为编排 / scenario 装配 | Claude(写或审) |
| `robot/stress` 连接池 / stats 聚合 / main / config 解析 / go.work 接入 | Codex(主写,回报 Claude 审) |
| `dev_tools.ps1` / `stress_snap.ps1` / `stress_summarize.ps1` | Codex(ops 管道) |
| 机器/成本/是否开真 DS/上云 | 人决策 |
| 恢复本地 Agones / 清库 / 抓 snapshot / 跑 summarize | 人确认后 Codex 执行 |
| 压测结论是否达标 / 架构 review | Claude |
| git commit / push / tag | 人授权后 Codex/人 |
