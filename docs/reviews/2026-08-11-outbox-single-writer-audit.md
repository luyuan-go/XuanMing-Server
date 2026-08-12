# A-2 出箱发布器同型扫描(2026-08-11)

判据三条**同时**成立才需要单写者选举:
①表全局未分区且发布器整表 FIFO;②payload 是**客户端可见的全量/绝对值快照**(后到即覆盖);
③下游无幂等键吸收重复/乱序。

| 出箱表 | 服务 | payload 性质 | 下游 | 判定 |
|---|---|---|---|---|
| `mission_push_outbox` | mission | **客户端可见全量快照**(MissionUpdateEvent.progressed 逐任务全量进度) | push→客户端 | **需选举,已修** |
| `player_push_outbox` | player | **客户端可见绝对值快照**(PlayerExperienceEvent level/exp_in_level) | push→客户端 | **需选举,已修** |
| `player_update_outbox` | battle_result | MMR 增量事件 | player 服务,`mmr_history` uk 幂等 | 不需要(②③均不成立) |
| `battle_drop_outbox` | battle_result | 掉落发放指令 | inventory,幂等键入账 | 不需要(②③均不成立) |
| `match_release_outbox` | battle_result | 释放指令 | matchmaker.ReleaseMatch,幂等 | 不需要(②不成立) |
| `terminal_release_outbox` | battle_result | owner 释放指令 | owner 权威,幂等 | 不需要(②不成立) |
| `battle_progress_outbox` | battle_result | 事实上报 | 幂等键 + **已按每玩家 FIFO**(`NOT EXISTS prev`) | 不需要(①已收敛) |
| `battle_mission_outbox` | battle_result | 事实上报 | 幂等键 + **已按每玩家 FIFO** | 不需要(①已收敛) |

结论:全仓 8 张出箱表,只有两张 push 出箱满足三条判据,均已接 writerlease。
其余六张的"不接"是**有据的差异**(②或③不成立),不是漂移 —— §15.3 不为形式统一付复杂度。
