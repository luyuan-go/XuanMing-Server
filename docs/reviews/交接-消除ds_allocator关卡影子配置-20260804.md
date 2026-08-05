# 交接单：消除 ds_allocator 的关卡影子配置（2026-08-04）

> **状态：§2 方案 A 已落地（2026-08-04 夜）**，改动清单与验证证据见文末 [§9](#9-落地记录2026-08-04)。
> 本文其余部分保留为决策背景；**§2.4「收尾」与 §5「机械校验」的建议已按 §9 的方式实现或作废**，
> 不要照着 §2 再做一遍。
>
> **给新会话的话**：本文自包含，不需要读上一轮对话。
>
> §3 记的是另一条已存在的设计路径（`loader_map`），**只作为背景与备选**，本次没做。
>
> **为什么读表是正当的**：`configtable`（源头 `g_关卡.xlsx`）是关卡数据的**唯一权威源**；
> 读它不构成「第二个源」。真正的影子副本是 `ds_allocator-dev.yaml` 里那张 `local_ds.maps`
> ——它把权威表的两个字段手抄了一遍。本次要消掉的就是它。

---

## 1. 问题是什么

`services/battle/ds_allocator/etc/ds_allocator-dev.yaml` 里有一张 `local_ds.maps` 表，
把每个 `map_id` 手工映射到 UE 关卡 URL：

```yaml
maps:
  - map_id: 7
    map_name: "/Game/Test/Level/SonglinTown?game=/Script/Pandora.PandoraPveGameMode"
```

而这两个字段**在关卡表里本来就有**（`configtable/dist/level.json`，源头是策划的 `g_关卡.xlsx`）：

| yaml 里手抄的 | 关卡表权威字段 |
|---|---|
| 关卡包路径 | `asset_path`（如 `/Game/Test/Level/SonglinTown.SonglinTown`） |
| GameMode | `game_mode_class`（如 `/Script/Pandora.PandoraPveGameMode`） |

**这是同一份事实抄了两处**，违反 `CLAUDE.md`：
- **§9.22**「状态优先查询唯一权威，不重复存储影子状态」
- **§17**「差异进表，不进接口签名——新增一个副本只应该改表」

### 1.1 漏配的实际后果（2026-08-04 实测，症状极具误导性）

关卡表里 `category=4`（战斗类）共 8 张：id = 4,5,6,7,8,9,10,11。
而 yaml 里**只配了 6 和 7**。玩家选 `map_id=11` 时：

```
map_id=11 未命中 maps
  → ResolveStartupMap 回退到默认 map_name（MobaLevel，PVP 图）
  → 战斗 DS 启动、加载 MobaLevel
  → DS 侧关卡门发现「已加载世界 ≠ 注入 map_id 期望资源」
  → 判 Mismatch → fail-closed 自杀退出
  → 战斗 DS 进程消失 → 分配卡在 warming → ready_wait 超时
```

DS 日志原文：
```
LogPandoraBattleFlow: Error: 已加载世界与注入 map_id=11 不符
    (期望资源=/Game/StylizedCyberpunk/Levels/StylizedCyberpunk...)
LogLoad: Took 0.32s to LoadMap(/Game/Test/Level/MobaLevel)
LogWorld: BeginTearingDown for /Game/Test/Level/MobaLevel
```

**玩家侧表现是「一直排队中」**，完全看不出是配置漏了一行。那道关卡门是**对的**
（防止玩家进错图），坏的只是配置。

**已做的止血（治标）**：2026-08-04 已把 8 张图全部补进 `maps`，并逐个校验 `.umap` 存在。
所以现在 4/5/6/7/8/9/10/11 都能进。本交接单要做的是**治本**。

---

## 2. 方案 A（本次要做）：ds_allocator 读关卡表，删掉 `maps` 影子配置

### 2.1 依赖现状（已核实，不用猜）

| 件 | 现状 |
|---|---|
| `pkg/configtable` 的 `LevelTable` | ✅ 已有 `ByID(id uint32) (*configpb.LevelRow, bool)`（`level_table.gen.go:50`）——**不需要新增访问器** |
| `LevelRow` 字段 | ✅ `GetAssetPath()` / `GetGameModeClass()`（`proto/pandora/config/v1/level.proto:62,64`） |
| `matchmaker` 已接 configtable | ✅ 可直接照抄装配范式 |
| **`ds_allocator` 是否已接** | ❌ **完全没有 import `configtable`**，需要新接线 |

### 2.2 现在的调用链（要改的地方）

```
biz.AllocateBattleWithCombatFactions
  → u.alloc.Allocate(ctx, matchID, mapID, gameMode, releaseTrack)     [data/local_allocator.go:162]
    → l.startProc(podName, port, matchID, mapID, gameMode, dsToken)   [:200]
      → defaultStart → l.buildArgs(port, mapID)                        [:412]
        → l.cfg.ResolveStartupMap(mapID)                               [conf/conf.go:225]
          → c.ResolveMapName(mapID)   ← 查 yaml 的 maps 表，未命中回退默认 map_name ❌
```

**问题就在最后一步的「未命中回退默认图」**——它把「配置漏了」伪装成「起了另一张图」，
随后被 DS 关卡门判 Mismatch 自杀（见 §1.1）。

### 2.3 建议做法

**注入一个可选的 URL 解析器**（与该文件已有的 `SetDSTokenIssuer` 同范式，别改 `GameServerAllocator` 接口）：

```go
// data/local_allocator.go
// mapURLResolver 由 main 在 configtable 就绪时注入；nil = 沿用 cfg.ResolveStartupMap(向后兼容)。
mapURLResolver func(mapID uint32) (string, error)

func (l *LocalGameServerAllocator) SetMapURLResolver(f func(uint32) (string, error)) { ... }
```

`buildArgs` 里优先用它；**解析失败必须让 Allocate 整体失败，绝不回退默认图**
（这是本次事故的直接成因，务必写死）。

**URL 转换规则**（必须与 UE 侧 `PandoraDSLoaderGameMode::BuildTravelURL` 逐字一致）：

```
asset_path = "/Game/StylizedCyberpunk/Levels/StylizedCyberpunk.StylizedCyberpunk"
  → 剥掉最后一个 '.' 之后的对象名（ObjectPath → PackagePath）
  → "/Game/StylizedCyberpunk/Levels/StylizedCyberpunk"
  → 拼 "?game=" + game_mode_class
  → "/Game/StylizedCyberpunk/Levels/StylizedCyberpunk?game=/Script/Pandora.PandoraPveGameMode"
```

⚠️ **剥对象名这一步不能省**：UE 的 `ServerTravel` / 命令行地图参数只吃包路径，
带 `.对象名` 会解析失败。UE 侧注释原文：
「ObjectPath 形如 /Game/Map/Battle.Battle，点号后的对象名不能进入 ServerTravel 地图路径」。

⚠️ **`game_mode_class` 可能为空**（关卡表里 id=5「测试场景」就没填）。为空时**不要拼 `?game=`**，
让 UE 用关卡自带的 GameMode。别塞一个猜的默认值。

**main.go 装配**：照抄 `services/matchmaking/matchmaker/cmd/matchmaker/main.go`
第 92-104 行（`configtable.NewStore()` + `AddValidator` 批次级校验）与第 279-281 行注入范式。
建议同样注册一个批次校验器：**关卡表里 `category=BATTLE` 的每一行都必须能构造出合法 URL**，
坏批次整批不切换（§9.15）。

**隔离**：只影响 `mode=local`。Agones 路径本来就不用 `maps`（`agones_allocator.go` 里搜不到
`ResolveStartupMap`/`MapEntry`/`cfg.Maps`），不要去动它。

### 2.4 收尾

- 删掉 `ds_allocator-dev.yaml` 的 `local_ds.maps` 整块（2026-08-04 为止血补的 8 行也一并删）。
- `map_name` 兜底：**建议也删**。保留它等于保留「未命中时静默起错图」的可能，
  而这正是本次事故的形状。宁可 fail-closed。

### 2.5 必须补的测试

| 测试 | 断言 |
|---|---|
| ObjectPath 剥离 | `/Game/A/B.B` → `/Game/A/B`；无点号的路径原样通过 |
| `game_mode_class` 为空 | 不拼 `?game=`，URL 无尾巴 |
| 关卡表缺 `map_id` | **Allocate 失败**，不得回退默认图 |
| 非战斗类关卡 | 拒绝（复用 `IsBattleLevel`） |
| 未注入 resolver | 沿用 `cfg.ResolveStartupMap`，行为与改动前逐字一致 |
| 8 张真实关卡 | 用 `configtable/dist/level.json` 的真实数据跑一遍，逐个比对生成的 URL |

### 2.6 验收标准（真正的那条）

> 在 `g_关卡.xlsx` 加一行战斗关卡 → 重导表 → **不改任何 yaml、不重启 ds_allocator**（热更）
> → 玩家能进那张图。

前面的单测只证明转换对了；这条才证明影子配置真的消掉了。

---

## 3. 背景：另一条已存在的设计路径 `loader_map`（**本次不做**）

### 3.1 为什么它是设计既定路径

`internal/conf/conf.go` 的 `ResolveStartupMap` 写得很清楚：

```go
// ResolveStartupMap 返回 DS 进程「首个加载」的关卡 URL。
//   - LoaderMap 非空 → 统一启到加载/分发关卡，目标副本由 UE Loader GameMode 读
//     PANDORA_MAP_ID 查 g_关卡.xlsx 后 ServerTravel 决定
//     （生产权威路径，allocator 只传数字 map_id，策划填表即用）。
//   - LoaderMap 空（默认）→ 按 map_id 直接启到目标图（Maps/MapName 的 dev 桥），向后兼容。
func (c LocalDSConf) ResolveStartupMap(mapID uint32) string {
	if c.LoaderMap != "" {
		return c.LoaderMap
	}
	return c.ResolveMapName(mapID)
}
```

**注意：`LoaderMap` 非空时直接 return，压根不看 `maps`。** 所以启用它 = 影子配置自动失效。

另一条佐证：**Agones 生产路径完全不用 `maps`**——
`internal/data/agones_allocator.go` 里搜不到 `ResolveStartupMap` / `MapEntry` / `cfg.Maps`。
生产靠的就是 DS 自己读 `PANDORA_MAP_ID` 查表。也就是说 `maps` 本来就只是 dev 专属的临时桥。

### 3.2 两边都已交付（已核实）

| 件 | 位置 | 状态 |
|---|---|---|
| Loader GameMode | `Pandora/Source/Pandora/Private/Gameplay/Default/PandoraDSLoaderGameMode.cpp` | ✅ 已实现 |
| Loader 关卡 | `Pandora/Content/Entry/Level/Lvl_Server_Entry.umap` | ✅ 存在 |
| Loader GM 蓝图 | `Pandora/Content/Entry/Level/GM_Server_Entry.uasset` | ✅ 存在 |
| yaml 配置行 | `ds_allocator-dev.yaml` 第 211 行 | ⚠️ **被注释掉了** |

Loader 的实现链（`PandoraDSLoaderGameMode.cpp` 行号为 2026-08-04 时点）：

```
:130  读 env PANDORA_MAP_ID
:165  CfgSystem->FindConstRow<FCfgLevel>(RowName)      ← 查客户端侧关卡表（同一份 g_关卡.xlsx）
:213  BuildTravelURL(CfgLevel)
:220    URL = CfgLevel.LevelAsset.GetAssetPathString()
:221    // ObjectPath 形如 /Game/Map/Battle.Battle，点号后的对象名要剥掉
:209  World->ServerTravel(TravelURL, bAbsolute=true)
```

失败路径也做了 fail-closed（查不到行 / URL 构造失败 / World 为空 → 主动退出，不留半初始化 DS）。

### 3.3 如果将来要切它，怎么做

把 `ds_allocator-dev.yaml` 第 211 行取消注释：

```yaml
  loader_map: "/Game/Entry/Level/Lvl_Server_Entry?game=/Script/Pandora.PandoraDSLoaderGameMode"
```

然后**建议同时把 `maps` 整块删掉或注释掉**——留着会让下一个人以为它还有用。
（`map_name` 兜底可以保留：Loader 路径下它也不再被 `ResolveStartupMap` 取到。）

重启：
```bash
pwsh tools/scripts/run_services.ps1 -Action restart -Service ds_allocator
```
（需先注入 `PANDORA_DS_*` 环境变量，见 §5）

### 3.4 切之前必须验证的（这条路本机从没跑过）

| 验证 | 判据 |
|---|---|
| DS 起在 Loader 关卡 | 战斗 DS 日志出现 `Lvl_Server_Entry` |
| Loader 读到 map_id | 日志 `map_id=N -> ServerTravel(...)` |
| ServerTravel 到目标图 | `LoadMap(/Game/.../目标图)`，且**无**「已加载世界与注入 map_id 不符」 |
| 玩家能进 | 客户端 `ResumeContext confirmed BATTLE admission` |
| 多张图都行 | **至少测 7 和 11 两张**（不同 GameMode：Pve vs Pve，建议再加一张 Battle 类如 6） |
| 关卡表新增行免改配置 | 在 `g_关卡.xlsx` 加一行战斗关卡 → 重导表 → **不改任何 yaml** → 能进 |

⚠️ **最后一条才是这件事的真正验收标准**。前面几条只证明 Loader 能跑。

### 3.5 已知风险

1. **多一次 ServerTravel**：DS 先起 Loader 再 travel 到目标图，冷启动更慢。
   editor 形态本来就慢（实测 53s~60s+），叠加后可能撞 `ready_wait_timeout`（当前 dev 已放宽到 300s）。
   若超时，先看是否真的在 travel，再决定要不要继续放宽。
2. **客户端侧关卡表必须是最新的**：Loader 查的是 **UE 侧的 `FCfgLevel`**（cook 进包或 editor 读散装），
   不是 Go 侧的 `level.json`。两仓关卡表漂移会让 Loader 查不到行 → DS 主动退出。
   历史事故：松林镇 4002 镜像关卡表漂移（见 `docs/incidents/`）。
3. **editor 形态未验证**：Loader 路径的注释写的是「生产权威路径」，
   本机 `launcher=editor` 下从没跑过，可能有未知问题。这正是要验的。

---

## 4. 附：早期误判的记录（勿照做）

> ⚠️ **不要先做这个。** 它是在 Go 侧重新实现一遍 UE Loader 已经做过的事，
> 会造出「Go 拼一份 URL、UE 也能拼一份」的新双源。只有当方案 A 被证明走不通时才考虑。

如果真要做：

- **依赖已就绪**：`pkg/configtable` 已有 `LevelTable`，`matchmaker` 已在用。
  但 **`ds_allocator` 完全没 import `configtable`**（grep 为空），需要新接线。
- **装配范式照抄 matchmaker**：`services/matchmaking/matchmaker/cmd/matchmaker/main.go`
  第 92-104 行（`configtable.NewStore()` + `AddValidator` 批次级校验）与第 279-281 行（`SetConfigTables`）。
- **`LevelTable` 当前只暴露 `IsBattleLevel(id)`**（`pkg/configtable/level.go:41`），
  要拿 `asset_path` / `game_mode_class` **需要新增访问器**。
- **URL 转换规则**（务必与 UE 的 `BuildTravelURL` 逐字一致）：
  `asset_path` 形如 `/Game/A/B.B` → 剥掉最后一个 `.` 之后的对象名 → `/Game/A/B`
  → 再拼 `?game=` + `game_mode_class`。
- **隔离**：只能影响 `mode=local`；Agones 路径不用 `maps` 也不该用这个。
- **必须加的测试**：`asset_path` 对象名剥离、`game_mode_class` 为空时的回退、
  关卡表缺 `map_id` 时 fail-closed（**不得回退默认图**——那正是本次事故的成因）。

---

## 5. 顺带建议：加一道机械校验

本次事故的本质是「两份清单漂移，且漂移无人发现」。建议加启动期断言或 dbcheck 式工具：

> `configtable` 里 `category=4` 的 id 集合，必须是 `local_ds.maps` 的 map_id 集合的子集
> （方案 A 下若 `maps` 删掉则跳过本校验）。

不匹配就在启动时 fail-fast 或打 ERROR，别等玩家撞。

---

## 6. 本机环境速查

**起服（会自动注入 `PANDORA_DS_*`）**：
```bash
pwsh tools/scripts/start.ps1 -Mode local -DsLauncher editor
```

**单服务重启（须自己注入 env，否则 local DS 拉不起来）**：
```bash
$env:PANDORA_DS_LAUNCHER='editor'
$env:PANDORA_DS_UPROJECT='F:\work\Pandora-Client-SVN\Pandora\Pandora.uproject'
$env:PANDORA_DS_EXE='F:\UnrealEngine-5.8.0-release\LocalBuilds\Engine\Windows\Engine\Binaries\Win64\UnrealEditor.exe'
$env:PANDORA_DS_DIR='F:\UnrealEngine-5.8.0-release\LocalBuilds\Engine\Windows\Engine\Binaries\Win64'
$env:PANDORA_DS_ADVERTISE_HOST='192.168.2.28'
pwsh tools/scripts/run_services.ps1 -Action restart -Service ds_allocator
```

**关键路径**：

| 用途 | 路径 |
|---|---|
| Go 后端 | `F:\work\XuanMing-Server`（git） |
| UE 客户端 | `F:\work\Pandora-Client-SVN\Pandora`（SVN） |
| Go 服务日志 | `run/dev/logs/*.err.log`（`*.log` 只有启动行） |
| **战斗 DS stdout** | `services/battle/ds_allocator/run/dev/logs/ds/<pod>.log` ⚠️ 相对进程 CWD，不在 `run/dev/logs/ds` |
| **DS / 客户端 UE 日志** | `Pandora/Saved/Logs/Pandora*.log` ⚠️ **谁先启动谁占主名，必须按 `LastWriteTime` 排序确认，别猜** |
| 关卡表（Go 侧） | `configtable/dist/level.json` |
| 关卡表（UE 侧） | `FCfgLevel` / `Table/CfgLevel.h` |

**踩过的坑（别重复）**：
1. **编译 UE 前必须关光所有 UE 进程**，包括 Hub DS 和战斗 DS（它们也是 `UnrealEditor.exe`，同样占 Live Coding 锁）。
   还要确保没有后台任务会在编译途中重启服务拉起 DS——本轮就因此在 838/839 处 `LNK1104` 失败过一次。
2. **杀了 DS 必须重启对应 allocator**：`LocalHubFleetProvider` / `LocalGameServerAllocator`
   的懒拉起是 `sync.Once`，**进程内只拉一次，被外部杀掉不会自愈**，表现为「点登录没反应」。
3. **服务全就绪前别操作客户端**：会撞 `HTTP 401`（Envoy 鉴权），不是 bug。

---

## 7. 关联上下文

- **事故档案**：[`docs/incidents/2026-08-04-p0-local-legacy-owner-wiring-gaps.md`](../incidents/2026-08-04-p0-local-legacy-owner-wiring-gaps.md)
  （本轮共修 9 处 owner 权威接线缺口，与本交接单是**不同的问题**，别混）
- **待验证清单**：[`docs/incidents/2026-08-04-p0-local-legacy-owner-wiring-PENDING-VERIFICATION.md`](../incidents/2026-08-04-p0-local-legacy-owner-wiring-PENDING-VERIFICATION.md)
- **规范依据**：`CLAUDE.md` §9.22（不重复存储影子状态）、§17（差异进表）、§9.15（配置表热更流水线）

---

## 8. 交接时的诚实边界

- 方案 A 的**全部结论来自代码阅读**：`ResolveStartupMap` 的分支、Loader GameMode 的实现、
  Loader 关卡与蓝图的存在性都已逐一核实；但 **`loader_map` 这条路本机一次都没实际跑过**。
- 「Agones 不用 `maps`」是 grep 得出的（`agones_allocator.go` 里搜不到相关符号），**未在真集群验证**。
- §4 的机械校验是**建议**，没实现。

---

## 9. 落地记录（2026-08-04）

方案 A 已实现：`ds_allocator` 起本机战斗 DS 时按 `map_id` **现查关卡表**拼关卡 URL，
`local_ds.maps` / `local_ds.map_name` 两处影子配置整块删除。

### 9.1 改了什么

| 文件 | 改动 |
|---|---|
| `pkg/configtable/level.go` | 新增 `LevelPackagePath`（ObjectPath → 长包名）、`(*LevelTable).BattleLaunchURL`（查不到 / 非战斗类 / 资源为空一律 error）、`ValidateBattleLaunchURLs`（批次级校验） |
| `services/battle/ds_allocator/internal/conf/conf.go` | 删 `MapName` / `Maps` / `MapEntry` / `ResolveMapName` / `ResolveStartupMap`；新增 `ValidateLocalMapSourceConfig`（`mode=local` 必须有 `config_table.dir` 或 `loader_map`，否则拒启动） |
| `services/battle/ds_allocator/internal/data/local_allocator.go` | 新增可选 `mapURLResolver` + `SetMapURLResolver`（照 `SetDSTokenIssuer` 范式）；`resolveStartupMap` 在 `Allocate` 内**取端口前**解析并 fail-closed；`startProc` / `buildArgs` 改传已定型的 `mapURL`（同一次分配只解析一次，避免热更瞬间"校验用 A、启动用 B"） |
| `services/battle/ds_allocator/cmd/ds_allocator/main.go` | 装配 `configtable.Store`（照 matchmaker 范式，含批次级校验器）；`mode=local` 注入解析器（**每局现查**，非启动快照）；注册通用 `configtable.AdminService` 支持热更 |
| `services/battle/ds_allocator/internal/server/grpc.go` | `NewGRPCServer` 增可选 `ctAdmin` 参数 |
| `etc/ds_allocator-dev.yaml` | 加 `config_table.dir: "../../../configtable/dist"`；删 `maps`（含当天止血补的 8 行）与 `map_name` |
| `run/cluster/etc/ds-allocator.yaml`、`run/docker-build/prebuilt/ds-allocator/etc/*.yaml` | 删同款死配置（`mode=agones` 下本就从未被读过） |
| `README.md` | 配置表更新：`map_name` / `maps` 退场，`config_table.dir` 登记为 `mode=local` 必配 |

### 9.2 关键取舍

- **失败即失败**：查不到 `map_id` → `Allocate` 返回 `ErrInvalidArg` 且**不占端口、不拉进程**，
  matchmaker 立刻拿到写明 `map_id` 的错误，不再有"起兜底图 → DS 判 Mismatch 自杀 → 卡到超时"这条路。
- **`map_name` 一并删**（按 §2.4 建议）：留着就等于留着静默起错图的可能。代价是 `mode=local`
  必须配 `config_table.dir` 或 `loader_map`，两者皆空**启动即拒**（有单测钉死，错误信息写明两条出路）。
- **agones 一字未动**：那条路的关卡本就由 DS 侧 Loader GameMode 查同一张表决定，allocator 只透传
  `map_id`。刻意**不**在 agones 加"后端预校验 `map_id`"——后端 dist 与 DS 镜像内烤死的表可能漂移，
  用后端表否决 DS 能跑的图会制造新的误杀。
- **§5 的机械校验作废**：两份清单已合成一份，不存在漂移对象。等价保障改由批次级校验器承担
  （`category=BATTLE` 的每一行都必须能拼出 URL，坏批次整批不切换、保留旧表）。

### 9.3 验证证据

| 验证 | 结果 |
|---|---|
| `go build` / `go vet` / `go test ./...`（ds_allocator 全模块 + `pkg/configtable`） | 全绿 |
| 单测：ObjectPath 剥离、`game_mode_class` 为空不拼 `?game=`、表缺 `map_id` → `Allocate` 失败且不占端口、非战斗类关卡拒绝、`loader_map` 优先、无关卡源 fail-closed、`ValidateLocalMapSourceConfig` 七种组合 | 全绿 |
| 真实 dist 逐张比对 8 张战斗图 URL（`pkg/configtable/realdist_test.go`） | 全绿；7 张与旧 yaml 手抄值**逐字一致** |
| dev yaml ↔ 关卡表接线（`cmd/ds_allocator/configtable_wiring_test.go`） | 全绿，4/5/6/7/8/9/10/11 全部拼得出 URL |
| 实机重启 ds_allocator（`launcher=editor`） | `configtable_loaded version=20260804002 levels=11`、`map_source=config_table(allocator 现查 g_关卡.xlsx)`、`service_ready` |
| 热更入口 | `grpcurl -plaintext -d '{}' 127.0.0.1:50020 pandora.config.v1.ConfigTableAdminService/ReloadConfigTable` → `activeVersion=20260804002, detail="version unchanged, no-op"` |

### 9.4 唯一行为差异：`map_id=5`

关卡表 id=5「测试场景」的 **GameMode类 列为空**，所以现在起的是关卡自带 GameMode，
而当天止血的 yaml 里我给它手填了 `?game=/Script/Pandora.PandoraBattleGameMode`（那是猜的）。
按"表是唯一权威源"，这里不塞猜测值。**若 5 需要战斗 GameMode，去 `g_关卡.xlsx` 填那一列**——
填完两条路（本机 allocator 与线上 Loader）同时生效。其余 7 张与改动前逐字一致。

### 9.5 仍未验证（留给下一次真人联调）

- **§2.6 的验收标准本身**：「加一行 → 重导表 → 不改 yaml、不重启 → 玩家能进」的**玩家侧那一半**
  需要真机开客户端打一局，本轮没跑。已验证的是它的两个前提：解析器每局现查表（非启动快照）、
  热更 RPC 在 :50020 可用且能原子换表。
- `loader_map` 那条路本机依旧一次都没跑过（与 §8 的诚实边界一致）。
