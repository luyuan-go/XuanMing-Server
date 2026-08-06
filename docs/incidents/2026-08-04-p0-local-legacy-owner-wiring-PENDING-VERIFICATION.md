# [INC-20260804-001] 待验证清单（未经验证 = 未修复）

> **配套事故档案**：[2026-08-04-p0-local-legacy-owner-wiring-gaps.md](2026-08-04-p0-local-legacy-owner-wiring-gaps.md)
> **本文件用途**：把「已落码但尚未由真实运行验证」的项单独列出。
> **纪律**：本页任一项在 `验证结果` 栏被真实证据填上之前，**不得对外声称该项已修复**。
> **最后更新**：2026-08-04

## 0. 为什么单独建这一页

本轮共修 8 处，其中 6 处已由真实客户端端到端验证。剩余项分两类：

1. **local 面已落码、待用户实测**——环境就在本机，随时可验。
2. **Model B / Agones 面已落码、无法在本机验证**——需要真 k8s 集群。这类**只有代码级证据**：读代码确认缺陷存在、按同一手法修复、单测覆盖。代码级证据**不等于**修复已生效。

本轮已经出现过一次「单测全绿但真机无效」的实例（见 §3），这正是本页存在的理由。

## 1. 名词对齐

| 说法 | 精确含义 |
|---|---|
| **Model B** | `ds_auth.authority_mode=redis`。启动时强制要求 `mode=agones` + `ds_auth.mode=enforce`（`main.go` `battle_model_b_invalid_activation` 门），否则进程直接退出。 |
| **线上 k8s** | 已确认即 Model B：`run/cluster/etc/{ds-allocator,hub-allocator,battle-result}.yaml` 均为 `authority_mode: "redis"`。 |
| **Agones + legacy** | `mode=agones` 但 `authority_mode≠redis` 的灰度姿态。属 Agones，但**不是** Model B。 |
| **local** | `mode=local` + `local-off-v1`，本机联调，恒非 Model B。 |

即：**Model B ⊆ Agones**，但 Agones 不全是 Model B。

## 2. 待验证项

### 2.1 local 面（本机可验，优先做）

| ID | 项目 | 验证步骤 | 通过判据 | 验证结果 |
|---|---|---|---|---|
| V-1 | ⑦ legacy 正常结算释放 owner | 进副本 → 打一会 → 点「退出副本」 | ①`ds_allocator.err.log` 出现 `owner_release_abandoned_weak released=1`；②`QueryOwner` 变无归属；③客户端**无需人工干预**出现 `confirmed HUB admission`；④全程无 `incomplete owner identity`、无 `30s deadline` | **✅ 已验证（2026-08-05 02:44–02:45 UTC / 本地 22:44–22:45）**<br>match_id=20343585043972096<br>①`released=1 skipped_not_self=0`（trace `030cd0f0`）；第二跳终态心跳 `released=0 skipped_not_self=1` 幂等跳过，未重复删<br>②`QueryOwner` 释放后为空，随后经首次进场链重新落到 HUB<br>③`confirmed HUB admission: generation=19`（02:45:26），全程零人工干预<br>④退出时刻之后 `incomplete owner identity` + `30s deadline` 计数 **0** |
| V-2 | ⑥ 花名册 env 投递（复验） | 同上一轮 | 战斗 DS 日志 `本地 battle 准入元数据已从 env 装载：roster_count=N` | **✅ 已验证**（2026-08-05 01:14 UTC，`roster_count=1`）；退出受理与回大厅链均已随 V-1 验证通过 |
| V-3 | 掉落入包 | 结算后查背包 | 无 `progress_item_grant_failed` | **未验证，且已知失败**：`err=errcode=4 (ErrInvalidArg) items=2`，属独立缺陷，不在本事故修复范围 |

### 2.2 Model B / 线上 k8s 面（本机无法验，需真集群）

| ID | 项目 | 验证步骤 | 通过判据 | 验证结果 |
|---|---|---|---|---|
| V-4 | ⑦-B `ReleaseBattleExpected` 释放 owner | 真集群跑完一局正常结算 | ①`battle_terminal_release_phase1_completed` 之后出现 `owner_release_abandoned_weak`；②该局玩家最终**不再指向已销毁的 battle 实例**（`QueryOwner` 无归属，或已推进到新的 Hub 归属）；③玩家能正常回 Hub。**判据①不得写成 `released=N`（N>0）** —— 见下方注 | **部分已验证（2026-08-06 真集群）**：①出现，②③见注 |

> **V-4 判据勘误（2026-08-06 真集群实测）**
>
> 2026-08-06T06:10:35Z 实测日志：
> `owner_release_abandoned_weak players=1 released=0 skipped_not_self=1 pod=pandora-battle-stable-4mr5g-7qdg4`
>
> `released=0` 在这里**是预期正确行为，不是失败**：玩家 06:10:19 主动退出副本时，owner 记录已被推进到新的 Hub 归属（`PENDING`），因此已不再指向本 battle 实例，`ownerReleaseAbandonedPlayersWeak` 的「② exact 身份门」（`rec.PodName != selfPod`）理应跳过——那道门正是用来防止误删已迁走玩家的活归属的（`services/battle/ds_allocator/internal/biz/owner_authority.go`）。
>
> 原判据把「函数被调用且释放了 ≥1 条」当成通过条件，等于要求 owner 记录**必须**还停在旧实例上，与 ⑦-B 自身的安全边界矛盾：正常退出/正常迁移越是工作良好，`released` 越应该是 0。真正要断言的是**结果**（玩家不再被钉在已销毁实例上），不是**簿记数字**（释放了几条）——与 §3 「绿色测试只证明函数行为正确，没证明它会被调用」是同一类错误的镜像：这次是「数字对不上就判失败」，而数字本就该为 0。
>
> 仍需真集群补验的是 `released>0` 的那条路径：**判弃**（DS 崩溃 / 心跳超时，玩家没有主动迁走，记录仍指向旧实例）时是否确实释放。正常结算/正常退出这条路验不到它。


| V-5 | ⑦-B 不破坏 outbox 幂等 | 结算 outbox 重放（ACK 丢失重试） | 重放不产生二次 GameServer 删除；`ReleaseBattleExpected` 返回值不因 owner 抖动改变 | **未验证** |
| V-6 | 前六处修复对 Agones 面零影响 | 集群回归 | Hub/Battle 分配、进场、心跳、结算全链与修复前行为一致 | **未验证**（隔离性目前仅由类型断言 + 静态审计保证） |
| V-7 | ⑦/⑦-B 在 Agones + legacy 灰度姿态下的行为 | 灰度部署跑一局 | owner 被释放且不误伤已迁走玩家 | **未验证** |
| V-10 | ⑨ `waitBattleReady` Model B 分支容忍瞬时 Redis 读错误 | 真集群分配期注入 Redis 抖动（主从切换 / 网络毛刺） | ①单次读失败不再整局 fail，日志出现 `battle_ready_wait_authority_read_transient`；②分配最终成功；③**battle 键被 purge / 分配被取代 / auth 被 fence 时仍立即失败**（不得被误当抖动重试） | **未验证（行为变更，生产路径）** |

### 2.3 工程验证（未执行，非本机能力）

| ID | 项目 | 阻断原因 |
|---|---|---|
| V-8 | `go test -race` | 需 CGO 的 Linux/CI 环境 |
| V-9 | fatal / OOM / SIGKILL 重启注入 | 需故障注入环境 |

## 3. 本轮的反面教材（为什么"单测绿"不算数）

⑦ 的**第一版**修复接在 `ReleaseBattle` 上：

- 单测 `TestLegacyReleaseBattle_ReleasesOwner` **通过**；
- 真机日志显示 `ReleaseBattle` 在该流程中**调用次数为 0**；
- 玩家照样回不了大厅。

绿色测试只证明了「这个函数的行为正确」，没证明「这个函数会被调用」。修正后的测试改为直接驱动真实路径（记录已 `ended` → 再来一跳心跳），而不是去调那个不会被走到的函数。

**推论**：V-4 目前的处境与当时的 ⑦ 完全相同——单测绿、代码读起来对、但没有任何证据证明 `ReleaseBattleExpected` 在真集群的结算流程中确实被调用、且我加的那行确实执行。在 V-4 被真实证据填上之前，⑦-B 一律按**未修复**对待。

## 4. 验证后的动作

- 每项通过后，在本页 `验证结果` 栏填入：日期（UTC）+ 证据位置（日志行 / 查询输出）。
- 全部通过后，同步更新事故档案 §8 验证矩阵与 §11 关闭审核，再考虑关闭。
- 任一项失败：在事故档案 §10 新增行动项，本页保留失败记录**不得删除**。
