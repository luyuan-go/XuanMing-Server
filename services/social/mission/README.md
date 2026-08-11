# mission — 通用任务域

> 设计与决策:`docs/design/mission.md`(语义移植自 luyuan/mmorpg C++ mission/condition/reward,
> C++→Go 对照表见该文档 §7)。本 README 只记服务契约与验证口径。

## 契约速览

- **端口**:gRPC :20019 / metrics :21019。错误码段 11000-11999。
- **库**:`pandora_mission`(MySQL 唯一权威;无 Redis 权威态,Redis 仅 sessiongate 会话门只读)。
- **客户端 RPC**(Envoy + JWT):`ListMissions` / `AcceptMission` / `AbandonMission` / `ClaimMissionReward`。
- **系统 RPC**(内网 callerID==0,Envoy 精确 path 403 双保险——见 deploy/envoy):
  `ReportMissionFacts`(进度唯一写入通道,幂等收据 + 指纹)/ `CompleteAllMissions`(GM 无副作用批量完成)。
- **推送**:状态变化事务出箱 → kafka `pandora.mission.update`(key=player_id)→ push 透传;
  推送不承担正确性,客户端 resync 回源 `ListMissions`。
- **发奖**:`mission_reward_log` PENDING/GRANTED/FAILED + 提交后同步尝试 + 1min 补扫;
  道具 `GrantItems`(键 `mission:<p>:<m>:stack`)/ 装备 `GrantInstances`(`:inst`,满包同键转邮件)/
  经验 `AddExperience`(`quest:<p>:<m>`,reason="quest")。三下游分键(inventory ledger uk 同键会指纹冲突)。
- **配置表**:mission / condition / reward(+item 只读),启动 fail-closed,
  跨表校验(数组列存在性 + 任务链环)在 `cmd/mission/configtable.go` 经 AddValidator 挂门禁。

## 领域语义要点(移植契约)

- 接取校验顺序:配置存在 → 未接取 → 未完成 → 活跃上限(50,§9.18)→ (type,sub_type) 互斥
  (sub_type=0 不参与互斥;互斥集从活跃行现算,无 D 版 typeFilter 泄漏)。
- 进度:类别相等 + 槽位过滤命中(slot1~4,空槽不过滤,全空槽匹配任意同类事实)→ 累加
  (饱和加法)→ 达标 clamp;**已达标槽不再累加**;空 condition_ids 的事实不推进任何进度。
- 完成扇出(与进度同一事务):删活跃 + 写完成行(auto_reward → reward_log;否则可领)→
  自动接后续链(校验不过跳过不阻断)→ COMPLETE_MISSION 条件再入(内部队列 ≤16 轮,
  触顶断链 ERROR;链环已在配置加载期拒批次)。
- GM `CompleteAllMissions`:只置完成清活跃,**不发奖不接链不再入**——与正常完成是两条
  刻意分离的路径(D 版 todo.md #225),不得合并。
- 刻意差异(相对 D 版):放弃不存在的任务返回 `ERR_MISSION_NOT_ACCEPTED`(D 版静默成功);
  发放链接到真实下游(D 版止于事件);bitset → 行存;倒排索引/互斥集 → 事务内现算。

## 验证矩阵(§16.6;biz 单测 = mission_test.go,repo 假件注入)

| 风险 | 验证 |
|---|---|
| 接取校验全分支(不存在/重复/已完成/超限/类型互斥/sub_type=0 不互斥) | TestAccept* |
| 槽位过滤/目标覆盖/clamp/已达标不累加/空槽匹配任意 | TestProgress* |
| 空 condition_ids / amount=0 / 未知类别事实零副作用 | TestFactGuards |
| 完成扇出:发奖分流(auto/可领/无奖)+ 自动接链 + COMPLETE_MISSION 再入连锁完成 | TestFanout* |
| 链自动接取撞上限/已完成 → 跳过不阻断 | TestFanoutChainSkip |
| 扇出 16 轮上限断链(人造环配置) | TestFanoutBounded |
| 事实幂等:同键同内容 already / 同键不同内容 fail-closed | ApplyFactsTx(repo 层,集成环境验证;biz 假件覆盖调用约定) |
| 领奖:可领→已领 CAS / 重复领 / 无奖任务领 | TestClaim* |
| 发放路由:堆叠/装备/经验分流,满包转邮件同键,任一类失败整条留 PENDING | TestDeliver* |
| 溢出:进度 uint32 饱和 / 推送 payload 分片 ≤2048 | TestSaturate / TestPushChunks |

**未在本地验证(交接为集成项)**:MySQL 事务/FOR UPDATE 真实并发(需 dev 库跑
`go test -tags integration` 或压测环境);`go test -race` 需 CGO Linux 环境(CI);
push→客户端全链路;battle_result 转发端到端。

## 运行

```bash
go run ./cmd/mission -conf etc/mission-dev.yaml
```

前置:MySQL 有 `pandora_mission` 库表(deploy/mysql-init/16 或 tools/migrate),
configtable dist 含 mission/condition/reward 批次。不起 inventory/player 时置
`allow_noop_reward: true`(奖励滞留 PENDING 可见,不假装发放)。
