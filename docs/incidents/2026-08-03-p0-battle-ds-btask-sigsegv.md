# [INC-20260803-001][P0] Artic01 战斗 DS SIGSEGV(MyBTTask_SpecifySkill 空指针)玩家被弹回

> **状态**：已部署待验证  
> **类型**：`crash`  
> **环境**：本机 k8s(minikube profile `pandora-agones` + Agones)/ packaged Windows 客户端  
> **首次发生时间（UTC）**：2026-08-02 08:02:43(pod `pandora-battle-stable-s4t4c-x8bvm`,证据为修复注释所引现场;本档完整取证的是第二次)  
> **首次发现时间（UTC）**：2026-08-03 01:5x(玩家报告「匹配 map 8 又被踢出来」)  
> **负责人**：待指定  
> **受影响服务/版本**：Battle DS 镜像 `pandora/battle-ds:r1642-dirty-9b5c80cc-20260802-063541`(修复前二进制);崩溃点源码在 UE 仓库 `Pandora-Client-SVN/Pandora/Source/Pandora/Private/AI/BT/Task/MyBTTask_SpecifySkill.cpp`  
> **最后更新**：2026-08-03

## 0. 一句话结论

玩家进入 map 8(Artic01)38 秒后,Battle DS 游戏线程在怪物行为树 `UMyBTTask_SpecifySkill::TickTask` 对已被 GC 的 `MoveTask` 弱指针调虚函数,SIGSEGV(读 0x0)整进程崩溃 → 心跳停 → 15s 判弃 → Pod 回收 → 客户端 60s 超时被弹回选角。修复(四指针 fail-closed 守卫)已提交 UE 仓库 r1648 并进入镜像 `r1647-dirty-20260802-221833`(已实证二进制含修复、fleet 已滚),观察窗口未完成。

## 1. 影响与范围

- 玩家影响：1 名玩家(`player_id=19311014776700928`)对局被打断,弹回选角/主城;当日同型共 2 次(08:02Z x8bvm / 01:51Z glkt4)。
- 影响对局：`match_id=19587103864193024`(map 8,单人 PVE);首崩对局未在本档取证。
- 服务影响：崩溃 GameServer 被判弃回收,Fleet 自动补位;无其他服务受损。
- 数据与安全影响：battle_result 走 abandoned 补偿链;拾取入账另有独立故障(battle-result `progress_item_grant_failed errcode=4` 无界重试,**不属本事故**,已单列任务排查)。
- 是否仍可复发：修复后二进制理论上不复发;观察窗口未完成前不得宣称关闭。
- 严重级别判定理由：玩家在局内被整进程崩溃打断,符合「掉线/被踢」P0 建档范围。

## 2. 第一现场与证据

### 2.1 症状

- 客户端：01:50:46Z 确认 BATTLE admission;01:52:24Z `ConnectionTimeout`(Elapsed 43.47s)→ 弹回选角。
- 服务端：01:51:36Z allocator 观察到 `release-pending`;01:51:56Z `owner_release_abandoned_weak` + `ds_lifecycle_published`。
- K8s/Agones：`Allocated → Deleting Pod`(事故当时 events 已取证,本档撰写时已过保留期)。

### 2.2 原始证据

- Loki:`{instance="default/pandora-battle-stable-s4t4c-glkt4:pandora-battle-ds"}`,窗口 01:47:00–01:52:10Z。查询时必须过滤 `LogScript`/`Accessed None`/`BP_NoFOW`/`LayeredFOW`/`Script call stack`(该图小地图蓝图每帧刷屏,3000 行 limit 秒被吃光,另列任务修复)。
- 客户端日志:`Pandora-Client-SVN/Packages/Client_Win64_Development/Windows/Pandora/Saved/Logs/Pandora.log` 行 1425–2151。

```text
01:51:11.461 LogBlueprintUserMessages: [None] ANS_PlayNiagaraWithSpeed Error
01:51:24.424 Signal 11 caught.
01:51:24.475 Unhandled Exception: SIGSEGV: invalid attempt to read memory at address 0x0000000000000000
01:51:24.475 PandoraServer!UMyBTTask_SpecifySkill::TickTask(...) [MyBTTask_SpecifySkill.cpp:103]
01:51:24.475 PandoraServer!UBTTaskNode::WrappedTickTask(...)
01:51:24.475 PandoraServer!UBehaviorTreeComponent::TickComponent(...)
01:51:24.475 ... FTickTaskManager::RunTickGroup → UWorld::Tick → UGameEngine::Tick
01:51:24.490 Engine crash handling finished; re-raising signal 11 for the default handler.
```

### 2.3 已排除的噪声

| 现象 | 结论 |
|---|---|
| 与 INC-20260729-002 同为「Artic01 上 DS 死亡」 | 机制不同:那次是 Chaos EndPhysics 卡死(无崩溃),本次是 SIGSEGV 整进程崩溃;独立事故 |
| 客户端 match_ready 后 3 分钟不 Travel(协调器 gen 6~10 静默) | 独立客户端 bug(违反 §9.19),不改变崩溃因果;已单列任务排查 |
| `ANS_PlayNiagaraWithSpeed Error` | 崩溃前 13s 的动画通知报错,与崩溃栈无调用关系,未证实相关 |

## 3. 时间线

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 08-02 08:02:43 | Battle DS x8bvm | 首次同型崩溃(触发路径实测记录于修复注释) | MyBTTask_SpecifySkill.cpp:110 注释 |
| 08-03 01:47:23 | matchmaker-pve | `match_start_accepted map_id=8` | service log |
| 01:47:45 | ds_allocator | `battle_ds_credential_activated` + `battle_ready_after_heartbeat`(glkt4) | service log |
| 01:50:39 | UE client | `DS ClientTravel generation=11`(此前 3 分钟协调器静默,见 §2.3) | Pandora.log:1487 |
| 01:50:46 | UE client | `ResumeContext confirmed BATTLE admission` | Pandora.log:2012 |
| **01:51:24.475** | **Battle DS glkt4** | **SIGSEGV,进程崩溃** | Loki 堆栈(§2.2) |
| 01:51:36 | ds_allocator | `release-pending`(心跳停 15s 判弃链启动) | service log |
| 01:51:56 | ds_allocator | `owner_release_abandoned_weak` → Pod 删除 | service log |
| 01:52:24 | UE client | `ConnectionTimeout` → 弹回选角 | Pandora.log:2133 |
| 02:04:48 | SVN | 修复提交 r1648(luhailong) | `svn log MyBTTask_SpecifySkill.cpp` |

## 4. 调用链与关键变量

```text
怪物 BT 释放技能任务 TryActiveSkill → 距离不足 → PerformMoveTask(记 MyMemory->MoveTask, bWaitForReleaseDis=true)
  → 距离剔除对远处怪 AAIController::StopMovement() 中止移动
  → OnGameplayTaskDeactivated 守卫要求 MoveTask->GetAIController() 非空,中止路径下已被清空
  → 守卫不成立 → FinishLatentTask/OnTaskFinished/CleanUp 整条清理链不执行,bWaitForReleaseDis 残留 true
  → GC 回收 UAITask_MoveTo,TWeakObjectPtr 变 null
  → 怪物重新激活,TickTask 对 null 调虚函数 ExternalCancel()(读 vtable 偏移 0)→ SIGSEGV
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| `FMySpecifySkillTaskMemory::MoveTask` | `PerformMoveTask` | BT 节点实例内存;TWeakObjectPtr,GC 可随时置空 | 单 BT 组件内 | 空解引用崩溃点 |
| `bWaitForReleaseDis` | 同上 | 应由 CleanUp 复位;清理链被跳过后残留 true | 同上 | 让 TickTask 走进危险分支 |

## 5. 根因

### 5.1 直接根因

`TickTask` 的等待释放距离分支对 `MoveTask`(TWeakObjectPtr)、`CurrentSkillCfg`、`GetAIOwner()`、`GetBlackboardComponent()` 四处指针不检查即解引用;`MoveTask` 在「移动被中止 → 清理回调守卫不成立 → 清理链跳过 → GC」的序列后为 null,对其调虚函数读 0x0。证据:两次崩溃同栈 + 修复注释记录的实测触发路径。

### 5.2 触发条件

- 怪物进入「等走近再放技能」状态后移动被距离剔除中止;
- GC 在下次 TickTask 前回收该 MoveTask;
- 怪物重新进入激活范围恢复 BT tick。

### 5.3 故障放大因素

- 崩溃即整进程死亡 → 单人局无冗余,玩家必被弹出;
- 客户端协调器 3 分钟静默(独立 bug)把玩家进场推迟到崩溃窗口。

### 5.4 为什么现有保护没有挡住

- BT 节点内存不是 UPROPERTY,弱指针失效无引擎层告警;
- 判弃链(15s)与客户端恢复按设计工作,把崩溃收敛为「弹回可重试 UI」——体验损伤但未卡死。

## 6. 全仓同类问题扫描

- 未系统执行。已知同文件 `TryActiveSkillImmediate`/`OnMessage` 对 `MyMemory->CurrentSkillCfg` 有判空;其他 BT Task 对 `NewBTAITask` 产物的弱引用未扫描,列为剩余风险。

## 7. 处置与永久修复

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| TickTask 四指针 fail-closed 守卫(状态失效即 FinishLatentTask 重新决策) | 已提交 | UE 仓库 SVN r1648 `MyBTTask_SpecifySkill.cpp:102-147` | 静态;无针对性回归测试(UE 侧无该类单测设施) |
| 修复进入 DS 镜像 | 已部署 | `pandora/battle-ds:r1647-dirty-20260802-221833`(二进制 mtime 08-03 02:14Z) | 在跑容器内 `tr -d '\0' \| grep` 命中修复独有日志串 `MoveTask=%d SkillCfg=%d Pawn=%d Target=%d` |

### 7.3 防复发规则

- 无新增全局规则;客户端 CLAUDE.md 既有初始化/生命周期条款已覆盖「不得假设弱指针有效」。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| 同型触发复现 | 2 崩/日 | 未执行(触发依赖 GC 时序,无确定性复现手段) | — | — |
| 修复后完整一局 map 8 | — | 未完成观察 | 本机集群真实客户端 | 待补 |
| 崩溃率观察窗口 | — | 未开始计时 | Loki `Signal 11` 检索 | 待补 |

## 9. 部署、回滚与观察

- 修复 commit:UE SVN r1648;镜像 `pandora/battle-ds:r1647-dirty-20260802-221833`;fleet `pandora-battle-stable` 已于 08-03 02:38Z 滚至该镜像。
- 观察项:map 8 完整一局无崩溃 + 24h 无 `Signal 11`;完成前本档不得关闭。
- 关联:INC-20260803-002(同晚第二次被踢,根因不同);客户端协调器静默、拾取入账重试、小地图刷屏三项已另列任务。
