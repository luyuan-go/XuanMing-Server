# [INC-20260803-002][P0] 载人 Allocated Battle DS 被当孤儿删除,玩家在局内被踢

> **状态**：已修复待部署  
> **类型**：`availability`  
> **环境**：本机 k8s(minikube profile `pandora-agones` + Agones)/ packaged Windows 客户端  
> **首次发生时间（UTC）**：2026-08-03 02:49:31(SIGTERM 到达 DS)  
> **首次发现时间（UTC）**：2026-08-03 02:50 前后(玩家报告「我又被踢出来了」)  
> **负责人**：待指定  
> **受影响服务/版本**：GameServer `pandora-battle-stable-s7lxs-q28jv`(Allocated,承载 `match_id=19598223534522368`);删除方为一次人工运维判断(AI 会话按日志证据误判孤儿)  
> **最后更新**：2026-08-03

## 0. 一句话结论

一台**完全健康、承载在局玩家**的 Allocated Battle DS 被当作「孤儿分配」人工删除:删除方核对了近 10 分钟日志无鉴权/owner 无 Begin,但 `kubectl logs --since` 窗口起点恰在玩家进场之后、且容器日志轮转把进场证据挡在窗口外——「日志里查无此人」被当成了「无人」。DS 收到 SIGTERM 干净退出 → 心跳停 → 15s 判弃 → 玩家连接被服务端关闭弹出。根治(ds_allocator 孤儿对账清扫 + 部署脚本禁删守卫)已落码待部署。

## 1. 影响与范围

- 玩家影响：1 名玩家(`player_id=19311014776700928`)在 map 8 对局约 19 分钟后被踢;被踢后客户端拿判弃前的旧落点回连死 DS 一次,约 1 分钟后自愈回大厅并成功重新匹配。
- 影响对局：`match_id=19598223534522368`(02:30:32Z 开始,02:49:31Z 被杀)。
- 服务影响：该 GameServer 被删,Fleet 补位;判弃补偿链正常执行。
- 数据与安全影响：走 abandoned 补偿;未发现已确认写丢失。
- 是否仍可复发：**修复部署前可复发**——人工删除路径仍物理可用;泄漏 Allocated 占位(删除动机)在清扫上线前依旧存在。
- 严重级别判定理由：玩家在局内被外力踢出,符合 P0;且为同类第二次(此前曾有 18h 泄漏 Allocated 占位引发的人工清理需求,见 §5.3)。

## 2. 第一现场与证据

### 2.1 症状

- 客户端:02:49:31.380Z `NMT_CloseReason: HostClosedConnection` → `FailureReceived`/`ConnectionLost`;02:49:35Z 协调器按权威旧落点 `ClientTravel generation=38` 回连已死 DS(判弃发生在 02:49:51,权威尚未更新);随后自愈。
- 服务端:DS 心跳到最后一分钟仍 12/12 全成功;02:49:51Z `battle_abandoned_heartbeat_timeout`——**判弃是删除的结果,不是原因**。
- K8s/Agones:`Allocated → Deleting Pod`(02:49:31Z Killing)。

### 2.2 原始证据

- Loki:`{instance="default/pandora-battle-stable-s7lxs-q28jv:pandora-battle-ds"}`:

```text
02:49:22.462 Battle 业务心跳窗口摘要: 尝试=12 启动=12 成功=12 失败=0  ← 被杀前最后摘要,全绿
02:49:31.290 FUnixPlatformMisc::RequestExitWithStatus / RequestExit    ← SIGTERM
02:49:32.049 StopBattleHeartbeat: 已停止战斗业务心跳
02:49:43.168 Exiting abnormally (error code: 143)                      ← 143 = SIGTERM,干净退出
```

- ds_allocator(`deploy/ds-allocator`,pod p6njj):`02:49:51 battle_abandoned_heartbeat_timeout match_id=19598223534522368 pod=...q28jv` → `owner_release_abandoned_weak` → `ds_lifecycle_published`。
- 客户端日志:`Pandora.log` 行 6352–6393(HostClosedConnection 与回连死 DS 的完整序列)。
- 删除方自述:工作区记忆 `never-delete-allocated-gameserver-20260803.md`(originSession b38b3bed):核对手段为日志窗口 grep,`--since` 起点晚于玩家进场(02:30),证据全被挡在窗口外。

### 2.3 已排除的噪声

| 现象/假设 | 结论 |
|---|---|
| DS 崩溃/卡死导致心跳停 | 否。退出码 143、无 Fatal/Hang,心跳直至 SIGTERM 全绿 |
| Agones 滚动更新删除 Allocated | 否。Agones 滚动只删 Ready;02:38Z 换代(GSS 4w66p)后 q28jv 作为旧代 Allocated 正确保留了 11 分钟 |
| DS 自杀引信(SDK Shutdown) | 否。SIGTERM 前无任何自我关闭请求日志 |
| 客户端回连死 DS(gen=38) | 判弃 15s 窗口内权威仍指旧落点,属既定行为,有界自愈;不作为本事故缺陷 |

## 3. 时间线

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 02:30:32 | matchmaker-pve | `match_start_accepted map_id=8` | service log |
| 02:30:54 | ds_allocator | `battle_ready_after_heartbeat`(q28jv) | service log |
| 02:31:21 | Battle DS | 玩家进场(EnsureOnlinePlayerData) | Loki |
| ~02:38:52 | Fleet | 滚动换代:新 GSS `4w66p`(新镜像);q28jv 作为 Allocated 正确保留 | GSS creationTimestamp |
| **02:49:31** | **运维(人工)** | **删除 q28jv(误判孤儿);DS 收 SIGTERM 干净退出** | Loki + K8s Killing 事件 |
| 02:49:31.380 | UE client | `HostClosedConnection`,玩家被踢 | Pandora.log:6352 |
| 02:49:35 | UE client | 按旧落点回连死 DS(gen=38,判弃未发生) | Pandora.log:6380 |
| 02:49:51 | ds_allocator | 心跳停 15s → 判弃 + owner 释放(删除的下游后果) | service log |
| 02:50:52 | matchmaker-pve | 玩家重新匹配成功(新镜像 22vdb) | service log |

## 4. 调用链与关键变量

```text
人工判断「疑似孤儿」
  → 证据 = kubectl logs --since 窗口 grep(起点晚于进场 + 轮转)→「查无鉴权/无 Begin」
  → kubectl delete gameserver(无任何 precondition,Allocated 状态未被视为反证)
  → Pod SIGTERM → DS 干净退出 → 心跳停
  → ds_allocator 15s 判弃 → abandoned 补偿(正常执行)
  → 客户端 HostClosedConnection → (15s 窗口内)回连死 DS → 自愈回大厅
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| GS `Allocated` 状态 | Agones GSA | 分配起至删除 | 集群共享 | 本身就是「可能有人」的声明,被删除方当成了待清理噪声 |
| `kubectl logs --since` 窗口 | 删除方 | 单次查询 | — | 三重失真(起点/轮转/级别)之一,制造「无人」假象 |

## 5. 根因

### 5.1 直接根因

人工删除了 Allocated GameServer,而「无人」判定建立在日志否定证据上。日志否定证据受 `--since` 窗口起点、容器日志轮转、级别静默三重失真,**永远构不成「无人」的证明**;Allocated 状态本身即「可能有人」的声明,反证责任在删除方。

### 5.2 触发条件

- 存在删除动机:Allocated 泄漏占位会锁死小容量匹配池(历史上有 18h 泄漏实例),而 Agones 生命周期不回收 Allocated → 形成「必须有人清」的压力;
- 当晚部署换代频繁,旧代 Allocated 残留被纳入「清理旧代」视野。

### 5.3 故障放大因素

- **系统缺口(本事故的结构性根因)**:对「无任何权威记录引用」的孤儿 Allocated GS,此前没有任何自动回收机制,人肉清理是唯一出路——误删是该缺口的必然产物,不是个人失误的偶然产物;
- `start.ps1 -ForceRecreateGameServers` 同样按 Fleet 标签整批删(含 Allocated),工具层为同类误删留着捷径。

### 5.4 为什么现有保护没有挡住

- Agones 滚动语义保护了 q28jv 11 分钟(只删 Ready),但对显式 `kubectl delete` 无防御;
- §9.21 是纪律条款,无机械 enforcement;
- 判弃/补偿/客户端恢复链全部按设计工作——它们只能把伤害收敛为「被踢后 1 分钟自愈」,不能阻止删除本身。

## 6. 全仓同类问题扫描

- `tools/scripts/start.ps1 -ForceRecreateGameServers`:整批删含 Allocated —— Confirmed 同型,已修(见 §7.2)。
- `tools/scripts/e2e_k8s.ps1` 全量清场:压测纪律授权的测试重置,但零守卫 —— Confirmed 同型,已修(fail-closed + 显式开关 + 非 force 路径逐台复查 + 与 `-SkipImageLoad` 互斥 fail-fast)。
- `tools/scripts/start.ps1 -Down` 的 Fleet 级联删除:第三条能杀载人 Allocated 的脚本路径(闭环审查第四轮补入,此前本节漏登记)—— **按 §9.21 豁免执行**:Down 语义是用户显式拆除整套本地栈(后端一并删,保留 DS 无意义),不加确认开关;但已补 Allocated 点名告警(列出将被级联杀掉的实例 + Ctrl+C 提示),消除「以为只是收工」与「正有人在打局」的静默重合。
- `ds_allocator` 判弃链的 `ReleaseExpected`/fenced delete:带 UID precondition 且由权威记录驱动,非同型,排除。
- 未覆盖边界:直接手敲 `kubectl delete` 无法技术性禁止,靠规则(§9.21/记忆)+ 消除动机(自动清扫)兜底。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 工作区记忆 `never-delete-allocated-gameserver-20260803` 立规:删前必须三条实时直接证据,拿不到=不删 | 已生效 | memory 文件 | — |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| ds_allocator 孤儿 Allocated GS 对账清扫(消除人肉清理动机):无权威记录引用 + 连续观察超阈值(默认 10m)→ 服务端复核 + UID+resourceVersion 双 precondition 删除;任何证据不可得整轮不删,且证据中断轮次候选观察重新起算 | 已落码待部署 | `services/battle/ds_allocator/internal/biz/orphan_gameserver.go`、`internal/data/agones_allocator.go`、`internal/conf/conf.go`、`etc/ds_allocator-dev.yaml` | `go build/vet` 绿;ds_allocator 全套单测绿;对抗审查两轮完成(下行) |
| **allocation 权威台账(对抗审查确认 P0「权威视图与被清扫集群零绑定」的整改)**:ClaimBattle 赢家在 GSA POST 前把 allocation_id 记入本权威 ZSET 台账(7 天保留,每轮修剪);清扫删除必须台账证明该 GS 出身本权威——空/错配 Redis(第二套部署、宿主残留进程、failover 到空实例)台账必空,一台都删不掉,并以 ERROR 暴露「疑似权威视图分裂」。bootstrap 限制:台账上线前的存量泄漏与手工 GSA 只告警不自动回收 | 已落码待部署 | `internal/data/allocation_ledger.go`、`biz/allocator.go`(claim 后记账)、`biz/orphan_gameserver.go`(防误删④) | 单测:台账查无永不删(权威分裂回归)、无 label 永不删、分配路径记账 |
| 孤儿轮墙钟预算(复用判弃链 sweepRoundBudget)+ 单轮删除封顶 3(对抗审查 P2:控制面退化时不得饿死同协程 §9.4 判弃链) | 已落码待部署 | `biz/orphan_gameserver.go` | 单测:5 候选单轮只删 3、下轮补齐 |
| `start.ps1 -ForceRecreateGameServers` 只删非 Allocated + 删前逐个重查状态(压缩 LIST→DELETE 竞态窗口至毫秒级) | 已落码 | `tools/scripts/start.ps1` | 语法解析零错;`local_k8s_profile_contract_test` PASS |
| `e2e_k8s.ps1` 存在 Allocated 时 fail-closed 中止,须显式 `-ForceDeleteAllocated` 才连删;非 force 路径逐台复查、窗口内出现 Allocated 即中止(对抗审查 P2:门检→批删 TOCTOU) | 已落码 | `tools/scripts/e2e_k8s.ps1` | 同上 |

| e2e `Wait-FleetReady` 收敛判据计入 Allocated(`Ready+Allocated ≥ desired`):保留载人 Allocated 后,一局在打时 buffer autoscaler 钳满 replicas,旧判据 `ready≥desired` 会把成功部署空转 240s 误报失败且错误归因 | 已落码 | `tools/scripts/e2e_k8s.ps1` Wait-FleetReady | 契约测试 PASS;闭环验证第三轮确认项(P1) |
| 契约测试补 §9.21 守卫断言(e2e 必须有 `-ForceDeleteAllocated` fail-closed 门、批删必须在授权分支内、start.ps1 强制重建必须逐台过滤非 Allocated + 删前重查、函数体内禁止整批删) | 已落码 | `tools/scripts/tests/local_k8s_profile_contract_test.ps1` | **变异实验 3/3 击落**(去门/批删逃出授权分支/回退整批删均使测试红),复原后基线绿——满足 §16.6「修复前失败、修复后通过」 |
| hub Fleet 可见性:注释澄清孤儿清扫只覆盖 battle Fleet(hub 常驻分片不走 GSA、原理上无 Allocated);强制重建删 Ready hub 副本前显式告警「hub 载人时仍是 Ready,在线玩家将被踢去重连」 | 已落码 | `tools/scripts/start.ps1` | 契约测试 PASS |

**对抗审查记录(三轮,视角 × 每发现 2 名反驳者)**:
- 第一轮确认 3 缺陷(start.ps1 LIST→DELETE 竞态 / e2e 零秒警告 / 注释安全依据缺半边),修复后被复核代理对照代码逐字确认落地;
- 第二轮确认 5 缺陷 —— **P0 权威视图零绑定**(台账整改)、P2 孤儿轮无预算、P2 节流测试为空测(变异实验证伪,已改真实调用计数断言)、P2 e2e 门检→批删 TOCTOU、P2「每轮核验」声明与实现不符(证据中断重新起算),全部落码回归绿;
- 第三轮(对修复的闭环验证):**P0 攻击角确认清扫误删向量已关死**(台账/refs/claim 绑定同一 Redis 实例,空/错配 Redis 一台都删不掉);其「心跳向量同拓扑攻击」被双反驳者以三层独立机制驳回(生产 Model B 门禁 fail-fast、空 Redis 心跳只回 ERR_UNAUTHORIZED 无指令、UE 凭据 ACK 五元组精确匹配才消费 stop——心跳向量由凭据 ACK 绑定承重,与台账同源等价);脚本回归角确认上表 P1/P2 两项,已修。

### 7.3 防复发规则

- `docs/design/zero-downtime-update.md` §6.3:2026-08-03 设计复议,废除「运维结合在线证据人工清理」条款,改为「有记录引用等自然排空;无记录孤儿由自动对账清扫回收;人工与脚本一律不得直接 delete Allocated GameServer」。
- 工作区记忆 `never-delete-allocated-gameserver-20260803`(feedback 级)。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| 孤儿清扫单测(四重防误删/复核失效/删除失败重试/候选修剪/封顶/台账查无永不删) | — | 全绿 | `go test ./ds_allocator/...` | 本地运行 |
| 部署脚本契约测试 + §9.21 守卫变异实验 | 变异体 3/3 **PASS(空证)** | 变异体 3/3 **FAIL**,基线 PASS | `local_k8s_profile_contract_test.ps1`;沙盒变异:去 e2e 门 / 批删逃出授权分支 / start.ps1 回退整批删 | 本地运行,满足 §16.6 |
| **真集群:台账写入路径生效** | — | **PASS** | 集群 `pandora-agones`,镜像 `pandora/ds-allocator:g00938143-dirty-20260803-082426`(部署 2026-08-03T12:36Z) | 12:55:27Z 的一次真实分配把 `allocation_id=b3aee60c-…` 写入 `pandora:ds:allocation_ledger`(ZSCORE=1785761727863);该写入证明 `u.allocationLedger` 接口断言在生产装配成功 |
| **真集群:载人对局全程不被清扫触碰** | — | **PASS(约 40 分钟观察)** | 同上 | GS `pandora-battle-stable-mfjtj-d95kt`(Allocated,match `19759259038318592`)权威记录 `pandora:ds:battle:{…}` 存在 → 三通道引用命中 → 全程零 `orphan_allocated_gs_candidate` 日志、GS 未被触碰(静默即正确行为) |
| 真集群:注入无记录 Allocated GS → 阈值后被清扫回收 | — | 未执行(当前集群无孤儿可观察;台账上线前的存量泄漏按设计只告警不回收) | 待构造孤儿后验证 | 剩余风险 |
| `go test -race` | — | 未执行(需 Linux/CGO CI,§16.7;新代码单协程域,风险低但未证) | — | 剩余风险 |

## 9. 部署、回滚与观察

- 修复 commit:未提交(工作区含多主题改动,由提交方按主题拆分;本事故相关文件清单见 §7.2)。
- 部署:ds_allocator 需重建 Go 服务镜像并滚动 Deployment(**用户自行执行**);脚本改动落盘即生效。
- 观察项:部署后注入一台无记录 Allocated GS 验证阈值回收;`pandora_ds_allocator_orphan_gameserver_reclaims_total` 指标出现且 `result=reclaimed` 仅对注入对象计数。完成前本档不得关闭。
- 关联:INC-20260803-001(同晚第一次被踢,DS 崩溃,根因不同);INC-20260729-002(Artic01 卡死判弃,不同机制)。
