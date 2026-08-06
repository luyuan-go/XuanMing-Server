# SODWLK PvE 玩法:任务 / 关卡进度的服务端权威存档

> 平行宇宙「SODWLK」PvE 战役(东瘟疫之地守城 → 推进主线 → 击杀巫妖王)的存档归属、
> 数据模型、上报链路设计。本文档定 **任务进度 / 关卡进度 / 声望 / 可带出产出** 的存储边界。
>
> ⚠️ 状态:**设计草案(2026-06-19)**,未拍板。涉及新服务 `campaign`(20017)、新库 `pandora_campaign`、
> 新 topic `pandora.pve.*`,评审通过后再登记进 `infra.md` / `go-services.md` 主表。

## 0. 玩法概述(背景)

- **题材**:WLK 前夕天灾入侵 → 玩着发现是另一条剧情线(女巫妖王 + 阿尔萨斯王子屠城)。
- **首关 SOD 场景**:东瘟疫之地圣光礼拜教堂路口,抵抗丧尸围攻。
  - 目标 1/2:守住防线 10 分钟 + 自己不死。
  - 目标 2/2:拾取 5 个丧尸令牌 → 交给圣光军团团长。
- **玩点核心**:刷怪 → 拾牌子 → **换声望 → 换东西 → 东西能带出局外**。
- **中长线**:在场景反复推进 SODWLK 主线章节。
- **闭环点**:你等的「援军」其实是 **成长后的自己**(历史 run 快照回放)。

## 1. 核心结论:服务端权威存档

**凡是「要保存、影响奖励、能带出局外、能兑换」的状态,一律服务端权威落库,客户端 / DS 不可信。**

这不是新规矩,是项目既有铁律的同类延伸:

| 既有铁律(`CLAUDE.md` §9) | 在本玩法的对应 |
|---|---|
| §6 MMR 在 `battle_result` 算,DS 不可信 | PvE 任务达成 / 奖励发放由后端判定,PvE DS 不可信 |
| §2 战斗结果幂等落库(同 `match_id` 只落一次) | PvE run 结果按 `run_id` 幂等落库,重复上报不重复发奖 |
| §14 客户端只拿客户端可见结构 | 任务面板 / 声望商店数据从后端拉,不本地权威存 |

**反例(为什么不能放客户端)**:玩点核心是「换声望 → 换东西 → 带出局外」。
存档若在客户端,改本地存档 = 无限刷声望 / 装备 → 局外经济(inventory / auction)直接崩。

## 2. 三层职责划分

| 层 | 负责 | 存储 | 权威性 |
|---|---|---|---|
| **服务端 `campaign` 等** | 任务最终判定、章节解锁、声望余额 + 兑换、存档点、可带出产出授予、援军快照 | MySQL + Redis | **权威** |
| **PvE DS(UE)** | 怪物刷新 / AI、波次计时(守 10 分钟)、令牌实时拾取的瞬时计数、战斗判定 | 内存(单局) | 不持久,结束上报 |
| **客户端(UE)** | 任务面板 UMG、剧情对白播放、声望商店 UI;数据从后端拉 | 不本地权威存 | 纯表现 |

> **关键拆分**:守城的「实时过程」(怪在哪、波次到几、玩家死没死)在 DS 跑——它最有判断力;
> 但「最终是否达成 + 拾取计数 + 用时 + outcome」由后端确认幂等入账。
> DS 只上报「这局我达成了 X」,等价于 battle DS 上报 `BattleResult` 给 `battle_result`。

## 3. 服务划分

### 3.1 新增服务:`campaign`(PvE 战役进度权威)

- **端口**:gRPC 20017 / metrics 21017(`auction` 20016 之后的下一个空位)。
- **存储**:MySQL 强依赖(新库 `pandora_campaign`)+ Redis(活跃 run 缓存,弱)。
- **消费 kafka**:`pandora.pve.result`(PvE DS 结算上报,幂等落库)。
- **生产 kafka**:`pandora.pve.progress`(进度推送给 `push`,key=player_id)。
- **pattern 复用**:消费 + 幂等落库复刻 [services/battle/battle_result](../../services/battle/battle_result);
  事务出箱(进度落库后再可靠发 inventory 授予 / push)复刻 `battle_result` W4 ⑨。

**职责**:
- 任务进度判定 / 完成(quest)
- 主线章节解锁 / 推进(chapter)
- 声望余额 + 兑换(reputation,扣声望→调 `inventory` 授予物品)
- 关卡存档点(checkpoint,支持断线续战 / 重开当前章节)
- PvE run 结算幂等入账(pve_runs)
- 援军快照(GetReinforcementSnapshot:返回该玩家历史 run 的 build,供 DS 生成「成长后的自己」NPC)

**不该做的事**:
- ❌ 不持有物品 / 货币真源(那是 `inventory`,§3.2)
- ❌ 不算 MMR / 段位(那是 `battle_result` / `player`)
- ❌ 不直接把 `*StorageRecord` 推给客户端(不变量 §14)

### 3.2 复用现有服务

| 服务 | 在本玩法的用途 |
|---|---|
| `inventory`(20015) | **可带出局外的产出真源**。campaign 兑换 / 发奖时调 `inventory` 授予货币 / 道具(ledger 幂等键防重复发) |
| `player`(20002) | 玩家档案 / 等级。PvE 不写 MMR;若 PvE 给经验 / 等级,走 player |
| `ds_allocator`(20020) | 拉 PvE DS(见 §3.3) |
| `push`(20014) | 任务进度 / 兑换结果 / 援军到达提示推送(消费 `pandora.pve.progress`) |
| `dialogue`(20013) | NPC 对白树(团长交令牌、剧情触发);对白是配置驱动,进度落点在 campaign |

> **声望归属决策**:声望本质是「按阵营计数的软货币」,但它绑定 PvE 战役语义(交令牌涨声望、
> 声望解锁章节奖励),内聚在 `campaign` 比塞进 `inventory` 货币体系更清晰。
> **兑换时**:campaign 事务内扣声望 → 事务出箱可靠调 `inventory.Grant`(幂等键=兑换单号),
> 避免「扣了声望没发货」或「发两次货」。

### 3.3 PvE DS 调度

- **推荐**:PvE DS 是 **独立 DS 类型**,复用 `ds_allocator` 调度链路,新增 Agones Fleet `pandora-pve`
  (selector `agones.dev/fleet=pandora-pve`),用 `game_mode` / `map_id` 区分守城关卡。
- **理由**:PvE 守城的怪物 AI / 波次 GameMode 与 5v5 battle 不同,但「分配 pod → 心跳 → 回收 →
  abandoned 补偿」链路与 battle DS 完全同构,`ds_allocator` 已支持(W4 ⑫ 真 Agones + W4 ⑧ 补偿)。
- **本机调试**:复用 `LocalGameServerAllocator`(repo 记忆 item 83)换 `map_name` / `game_mode` 即可起本地 PvE DS。
- **UE 侧**:复刻 `PandoraBattleGameMode.ReportResultAndEndMatch`(repo 记忆 item 67)→ 改为
  `PandoraPveGameMode.ReportRunResult`,经 DS gRPC-Web 入口 `:8444`(repo 记忆 item 82)上报 campaign。

## 4. 数据模型

### 4.1 MySQL:新库 `pandora_campaign`

> 通用字段(created_at/updated_at/deleted_at/version)按 `infra.md` §2.2。

| 表 | 用途 | 关键索引 / 幂等键 |
|---|---|---|
| `pve_runs` | 单局 run 结算记录(PvE 版 `battles`) | PK(run_id), uk(run_id), idx(player_id, ended_at) |
| `quest_progress` | 玩家任务进度 / 完成 | uk(player_id, quest_id), idx(player_id, status) |
| `chapter_progress` | 主线章节解锁 / 完成 | uk(player_id, chapter_id), idx(player_id) |
| `reputation` | 阵营声望余额 | uk(player_id, faction_id) |
| `reputation_ledger` | 声望变动流水(append-only + 幂等) | uk(player_id, idempotency_key), idx(player_id, created_at) |
| `checkpoints` | 关卡存档点 | uk(player_id, chapter_id) |
| `pve_run_outbox` | 事务出箱(发 inventory 授予 / push 进度) | PK(id) AUTO, uk(run_id, kind), payload=proto bytes |
| `reinforcement_snapshots` | 援军快照(成长后的自己) | idx(player_id, created_at), uk(run_id) |

- **幂等核心**:`pve_runs.uk(run_id)` —— PvE DS 同一局上报多次只落一次(不变量 §2)。
- **声望幂等**:`reputation_ledger.uk(player_id, idempotency_key)`,idempotency_key = `run_id`(刷怪入账)或兑换单号。
- **奖励发放原子**:`pve_runs` + `quest/chapter/reputation` 更新 + `pve_run_outbox` 写在 **同一事务**(复刻 W4 ⑨)。

### 4.2 Redis(弱,活跃 run 缓存)

| Key | 类型 | TTL | 用途 |
|---|---|---|---|
| `pandora:pve:run:{<run_id>}` | hash | run 时长 + 余量 | 活跃 run 运行时快照(断线续战参考) |
| `pandora:pve:player:<player_id>` | string | run 时长 | 玩家当前所在 run_id(落不变量 §1 一人一 run) |

> hashtag `{<run_id>}` 锁同一 Cluster slot。Redis 仅活跃索引,`pandora_campaign` 为权威。

### 4.3 Kafka topic(`pandora.<domain>.<event>`)

| Topic | 分区 | 保留 | 生产者 | 消费者 | key | 备注 |
|---|---|---|---|---|---|---|
| `pandora.pve.result` | 16 | 30d | PvE DS | campaign | run_id | ⭐ 核心,at-least-once + 幂等落库,必配 DLQ |
| `pandora.pve.progress` | 8 | 1h | campaign | push | player_id | 任务 / 章节 / 声望进度推送 |

- `pandora.pve.result` 与 `pandora.battle.result` 同级(丢 PvE run = 丢玩家产出),**必须配 `pandora.dlq.pve.result`**。
- 进 `kafkax.PushTopics`:加 `pandora.pve.progress`,push 订阅。

### 4.4 端口登记(待评审)

| 服务 | gRPC | metrics |
|---|---|---|
| campaign | 20017 | 21017 |

## 5. 关键链路

### 5.1 PvE 结算幂等入账(守城达成 → 发奖)

```
PvE DS 守城结束
  │ 1. 同步 ReportRunResult(经 :8444 gRPC-Web)  ── 或 ──  2. kafka pandora.pve.result(key=run_id)
  ▼
campaign 消费 / 收 RPC
  │ 3. 同一事务:
  │    - INSERT pve_runs(uk run_id)          ← 命中 dup ⇒ alreadyRecorded,直接返回(不变量 §2)
  │    - UPDATE quest_progress(守城达成 / 令牌计数)
  │    - UPDATE chapter_progress(章节推进)
  │    - INSERT reputation_ledger(uk player_id+run_id) + UPDATE reputation 余额
  │    - INSERT pve_run_outbox(kind=grant_inventory / push_progress)
  ▼
RunOutboxPublisher(FIFO,失败重试,同 player 保序)
  │ 4. 调 inventory.Grant(幂等键=run_id)授予可带出产出
  │ 5. 发 pandora.pve.progress → push → 客户端刷新任务面板
```

> DS **不上报奖励内容**(只报「达成 / 计数 / outcome」),奖励由 campaign 按配置表算 —— 同 §6 DS 不可信。

### 5.2 声望兑换(换东西带出局外)

```
客户端「兑换」→ campaign.RedeemReputation(faction_id, item_config_id, idempotency_key)
  │ 同一事务:
  │   - 校验 reputation 余额 >= 价格(FOR UPDATE 锁行)
  │   - INSERT reputation_ledger(uk player_id+idempotency_key,负向)
  │   - UPDATE reputation 扣减
  │   - INSERT pve_run_outbox(kind=grant_inventory)
  ▼
RunOutboxPublisher → inventory.Grant(幂等键=兑换单号)→ 产出进局外背包
```

### 5.3 关卡存档点 / 断线续战

- 玩家进 PvE DS → DS 向 campaign 拉 `GetCheckpoint(player_id, chapter_id)` 还原进度。
- run 内达成阶段目标 → DS 上报 → campaign 更新 `checkpoints`。
- 断线 / 崩溃 → 重连分新 DS,从 `checkpoints` 续(不靠客户端本地存档)。
- abandoned(DS 崩溃)→ `ds_allocator` 心跳超时补偿(不变量 §4),run 标 abandoned,存档点保留不回退。

### 5.4 闭环:援军 = 成长后的自己

- 每局有效 run 结束,campaign 落 `reinforcement_snapshots`(当时 build / 装备 / 战力快照)。
- 后续同章节 run,PvE DS 调 `GetReinforcementSnapshot(player_id, chapter_id)` 拉历史快照
  → 生成「援军 NPC」(成长后的自己)在守城后期入场。
- **纯服务端权威数据驱动**,客户端无从伪造援军强度。

## 6. proto 契约草案(命名遵循 `CLAUDE.md` §5)

```
// proto/pandora/campaign/v1/campaign.proto(草案)
service CampaignService {
  GetCampaignState(player_id)                         → CampaignState        // 客户端可见结构
  GetCheckpoint(player_id, chapter_id)                → Checkpoint
  RedeemReputation(faction_id, item_config_id, idem)  → RedeemResult
  GetReinforcementSnapshot(player_id, chapter_id)     → ReinforcementSnapshot
  ReportRunResult(PveRunResult)                       → ReportRunResultResponse  // PvE DS 同步上报(或走 kafka)
}
```

- ID 规则(§5.5/§5.6):`player_id` / `run_id` = `uint64`;`quest_id` / `chapter_id` / `faction_id` /
  `item_config_id` / `map_id` = `uint32`(配置表 ID)。
- 四类 message(§5.8):
  - 客户端可见:`CampaignState` / `Checkpoint` / `ReputationView`(最小视图,§5.11 不外泄存储字段)
  - 存储快照:`PveRunStorageRecord` / `ReputationStorageRecord`(MySQL blob / Redis value)
  - 事件:`PveRunResult`(DS 上报)/ `PveProgressEvent`(推 push)
- **禁止把 `*StorageRecord` 直接返客户端**(不变量 §14)。

## 7. 阶段限制与待决项

### 7.1 存档粒度(混合模型,推荐)

- **局内(run,临时)**:单局任务进度 / 波次 / 令牌瞬时计数 → DS 内存,结束清算。
- **局外(账号级,永久)**:声望 / 可带出产出 / 章节解锁 / 存档点 → MySQL 权威。
- 这正好对应玩点「反复刷 + 成长带出」:每局是 roguelike-ish 短局,账号进度永久累积。

### 7.2 待拍板

1. **PvE DS 是否独立 Fleet**:推荐独立 `pandora-pve` Fleet(§3.3);若初期省事,可复用 battle Fleet + `game_mode` 区分。
2. **声望归属**:推荐内聚 `campaign`(§3.2);若希望声望进统一货币体系,改放 `inventory` currency。
3. **结算通道**:同步 `ReportRunResult` RPC(简单、立即反馈)vs kafka `pandora.pve.result`(削峰、与 battle 同构)。
   推荐 **kafka 为主 + 同步 RPC 兜底**(同 `battle_result` 既消费 kafka 又有同步 ReportResult)。
4. **campaign 是否独立服 vs 并入 player**:PvE 进度独立性强(独立库 + 独立结算消费),推荐独立服。

### 7.3 落地顺序(草案,待评审后排期)

1. proto `campaign.proto` + 新库 `pandora_campaign` DDL(`deploy/mysql-init/`)。
2. `campaign` 服务骨架(复刻 `battle_result` 消费 + 事务出箱 pattern)。
3. `pandora.pve.*` topic + push 订阅 + DLQ。
4. `ds_allocator` 加 `pandora-pve` Fleet selector(或复用 battle + game_mode)。
5. UE `PandoraPveGameMode` 上报(独立仓库 `C:\work\Pandora-Client-SVN`,人 / Codex)。
6. 端口 / topic / 表登记进 `infra.md` + `go-services.md` 主表。
