# UE 专服性能陷阱与优化手册(ue-ds-perf-pitfalls)

> 用途:沉淀 **UE Dedicated Server(Linux Battle/Hub DS)** 上真实踩到的性能陷阱、
> 定位方法与修复模式。区别于工具选型和观测流程,本文是**案例化的"坑 → 因 → 修 → 验"**。
>
> 关联:
> - `docs/ops/linux-ds-observability.md`(DS 崩溃与性能观测手册:第一现场、堆栈还原)
> - `docs/ops/perf-profiling-toolchain.md`(监控 vs profiler、函数热点工具链)
> - `docs/ops/性能优化-战斗DS画像与修复-20260803.md`(战斗 DS 完整画像专项:无头 Insights 导出
>   runbook、CPU/内存定谳、修复清单与验收基线;本文 §5b/§5c 是它二轮复测的提炼)
> - 完整证据链见事故档案 `docs/incidents/2026-07-29-p0-battle-ds-reclaimed-client-exit-stuck.md`
>   §2.2.7~§2.2.11、§5.5~§5.9(本文是其可复用部分的提炼,不替代事故档案)。
>
> 来源:INC-20260729-002(Battle DS 游戏线程每隔一两分钟卡死 >10s → 被 15s heartbeat_timeout
> 判弃回收 → 玩家被踢,表现为"怪物突然不动")。下面每条都在真机复现并验证过。

---

## 0. 结论先放前面

| # | 陷阱 | 一句话 | 只在专服出现? |
|---|---|---|---|
| 1 | **poison malloc** | Development 专服每次 alloc/realloc 都附带一次 `O(块大小)` memset | ✅ 编辑器恒关 |
| 2 | **挂起检测阈值配错** | `HangDuration` 默认 25s > 后端 15s 判死阈值,Pod 先被删,堆栈永远打不出 | — |
| 3 | **stdout 块缓冲** | 容器里 stdout 攒满 4KB 才 flush,进程被杀时最后一分钟日志整体丢失 | ✅ 容器特有 |
| 4 | **tick ≠ 物理** | 关 tick 省 CPU,但不减物理体;物理卡顿要减碰撞/减常驻,不是减 tick | — |
| 5 | **流送的时机而非空间** | 服务端流送后"零玩家时零加载",BeginPlay 就 spawn 的受重力物会坠落 | — |
| 6 | **Chaos 加速结构强制全量重建** | 单批入队 >1000 粒子引擎就放弃时间切片,一帧内全量重建+拷贝 175MB 结构 → 秒级停摆 | — |
| 7 | **画像埋点撞 ACTIVE 判弃线** | `-llm -trace` 把加载期游戏线程阻塞推到 15~22s,>15s 心跳判弃 → 带埋点的 DS 进场即被杀 | ✅ 只在带埋点时 |

**贯穿全篇的方法论(§6)**:先修取证 → 再看中间帧 → 先量后改 → 分清"看着相关"和"真的相关"。

---

## 1. 陷阱一:poison malloc(Development 专服的隐藏税)

### 症状
DS 游戏线程周期性卡死 >10s;挂起堆栈稳定停在:

```
UWorld::Tick → RunTickGroup → FEndPhysicsTickFunction::ExecuteTick
 → FChaosScene::EndFrame → CopySolverAccelerationStructure
 → FlushExternalAccelerationQueue → FPendingSpatialDataQueue::Remove
 → TArray::ResizeAllocation → FMemory::Realloc
 → FMallocPoisonProxy::Realloc → FMallocBinned2::Realloc → libc
```

### 根因
`FMallocPoisonProxy` 是调试分配器(`Engine/Source/Runtime/Core/Public/HAL/MallocPoisonProxy.h`):

```cpp
#define UE_DEBUG_FILL_FREED (0xdd)
#define UE_DEBUG_FILL_NEW   (0xcd)
FMemory::Memset(Result, UE_DEBUG_FILL_NEW, Size);                              // 分配:整块填 0xcd
FMemory::Memset((uint8*)Ptr + NewSize, UE_DEBUG_FILL_FREED, OldSize - NewSize);// 缩小:丢弃段填 0xdd
```

即**每次 alloc/realloc 附带一次 `O(块大小)` memset**。它由这个宏门控:

```cpp
#define UE_USE_MALLOC_FILL_BYTES ((UE_BUILD_DEBUG || UE_BUILD_DEVELOPMENT) \
    && !WITH_EDITORONLY_DATA && !PLATFORM_USES_FIXED_GMalloc_CLASS && !USING_ADDRESS_SANITISER)
```

关键在 `!WITH_EDITORONLY_DATA`:

| | `WITH_EDITORONLY_DATA` | poison malloc |
|---|---|---|
| 编辑器 | 1 | **恒关** |
| Development 专服 | 0 | **恒开** |

**所以这个税本地跑编辑器永远复现不到,只在 Linux DS 上出现。**

### 为什么在 Chaos 处放大 + 随时间恶化
`FPendingSpatialDataQueue`(`Chaos/Public/Chaos/PendingSpatialData.h`)的
`ParticleToPendingData` 是 `TArrayAsMap<FUniqueIdx,int32>`——**"用数组当 map",按 key 整数值直接下标寻址**。而 `FUniqueIdx` 单调递增分配、不紧凑复用:怪反复死亡重生 → index 一直涨 → 该数组只增不减 → 每次 realloc 的 memset 规模越来越大。**这解释了"卡死总在开局一两分钟后而非一开始"。**

### 修法(一行,只作用于 Linux 专服)
`<UE 客户端仓>/Source/PandoraServer.Target.cs` 的 Linux 块:

```csharp
GlobalDefinitions.Add("UE_USE_MALLOC_FILL_BYTES=0");
```

宏是 `#if !defined(...)` 守卫,**预定义会赢**。

- **不用换 Test 配置**:Test 也能关它,但 `Build.h` 的 `UE_BUILD_TEST` 分支把 `DO_CHECK`
  降为 `USE_CHECKS_IN_SHIPPING`、`DO_ENSURE` 降为 `USE_ENSURES_IN_SHIPPING`(默认都 0),
  会连带丢掉 `check()/ensure()`。此处只精确关分配器,Development 的日志/check/ensure 全保留。
- **生效前提**:源码引擎(无 `Engine/Build/InstalledBuild.txt`),`GlobalDefinitions` 能重编 Core。
  Installed Build 下预编译 Core 不重编,本行**静默失效**,须改用 Test/Shipping。
- **验证判据**:DS 运行日志中 `FMallocPoisonProxy::` 帧命中数 = 0。

---

## 2. 陷阱二:挂起检测阈值必须小于后端判死阈值

### 症状
明明进程还活着、只是游戏线程卡了十几秒,却拿不到任何堆栈证据。

### 根因
`FThreadHeartBeat`(`Core/Private/HAL/ThreadHeartBeat.cpp`)读 `[Core.System] HangDuration`,
**默认 25.0**;检测线程默认就在跑(`AllowThreadHeartBeat()` = `!Param("noheartbeatthread")`,
server 构建下 `USE_HANG_DETECTION` 成立)。问题不是"没开",而是 **25s > 后端 15s
heartbeat_timeout**:Pod 先被回收,`OnHang` 的 Error+堆栈永远来不及输出。停跳窗口若恰好
落在 15s~25s 之间,即使日志没丢也照样不触发。

### 修法(DS 启动参数,不写进 Config)
```
-ini:Engine:[Core.System]:HangDuration=10.0
-ini:Engine:[Core.System]:HangsAreFatal=False
```

- **取 10s**:必须 < 15s 判死阈值抢在回收前打堆栈;引擎下限 5s,取 10 留一拍 5s 心跳抖动余量。
- **`HangsAreFatal=False`**:`UE_ASSERT_ON_HANG` 未外部定义时默认 0,故本就非致命;显式钉住是
  防将来有人定义为 1 后 10s 阈值把一次长加载变成 assert 崩进程。Linux
  `PLATFORM_USE_MINIMAL_HANG_DETECTION=0`,走的是打堆栈而非 `abort()` 的分支。

### 读堆栈的注意点(见 §6.2)
栈顶两帧 `CaptureStackBackTrace` / `ThreadStackWalker` 是**抓栈动作自己**,不是卡点;真正
卡点在紧接其下的第一帧业务代码。

---

## 3. 陷阱三:容器里 UE stdout 是块缓冲,崩溃前的日志会整体丢

### 症状
"删除前 N 秒 DS 没打日志" ——**多半是采集缺口,不是 DS 沉默**。

### 根因
UE 只在 `FUnixPlatformMisc::HasBeenStartedRemotely()`(= 环境变量 `SSH_CONNECTION` 非空)或
有调试器时才 `setvbuf(stdout, NULL, _IONBF, 0)`(`Core/Private/Unix/UnixPlatformMisc.cpp`)。
容器里两者都不成立 → stdout 对管道走 libc 默认 4KB 块缓冲 → 攒满一块才吐给 kubelet/Loki;
进程被杀时缓冲区未 flush,最后一批日志(常含解释崩因的第一现场)整体丢失。
`-FORCELOGFLUSH` 救不了,它只作用于 `OutputDeviceFile`(写 .log 文件),不管 stdout。

判定手法:把 **Loki 摄取时间**与**行内嵌 UE 时间**对齐——若"跨分钟的行挤在同一摄取时刻",
即为块缓冲。

### 修法(entrypoint,行缓冲)
```bash
if command -v stdbuf >/dev/null 2>&1; then
  FINAL_LAUNCH=(stdbuf -oL -eL "${SERVER_LAUNCH[@]}")
else
  export SSH_CONNECTION="${SSH_CONNECTION:-0.0.0.0 0 0.0.0.0 0}"  # 兜底:触发 UE 自身 _IONBF
  FINAL_LAUNCH=("${SERVER_LAUNCH[@]}")
fi
exec "${FINAL_LAUNCH[@]}" ...
```

`stdbuf` 在 execvp 后被目标进程替换,**DS 仍是 PID 1**,SIGTERM 语义不变。验证判据:同一
Pod 的日志摄取时刻分散在多个秒(而非全挤一处)。

### 附带坑:卡死期间连非游戏线程日志也会延后
UE 输出设备有全局锁;游戏线程卡死持锁时,独立线程(如 health pinger)的 `MY_LOG` 会被推迟
(实测滞后 38s,是延后不是丢)。**读卡死期间的时间线必须按行内嵌时间,不能按 Loki 摄取时间。**

---

## 4. 陷阱四:关 tick ≠ 减物理体(常见误判)

物理体(Chaos 粒子)来自**开了碰撞的图元**,与 tick 无关:一个 `bCanEverTick=false` 的静态
网格,只要碰撞开着,照样注册物理体、照样占一个 `FUniqueIdx`。

| 层级 | 手段 | 对物理卡顿 | 对 CPU |
|---|---|---|---|
| 不进包 | `ClassesExcludedOnDedicatedServer` | 仅当该类**有碰撞**才有效 | 有效 |
| 加载但不常驻 | 服务端 WP 流送 | 有效(减常驻碰撞体) | 有效 |
| **加载但不 tick** | `bCanEverTick=false` / `SetActorTickEnabled(false)` | ❌ **无效** | ✅ 有效 |
| 加载但不参与物理 | 碰撞设 `NoCollision` | ✅ 有效 | 部分 |

**推论**:
- 减物理卡顿 → 减碰撞体 或 减常驻(流送),不是减 tick。
- 减 CPU → 关远处 tick(见 §5 的距离激活),但别指望它治物理卡顿。
- 纯渲染类(天光/大气/云/雾/后处理/RVT/小地图)专服剥离**只省内存**——它们没碰撞,不减物理体。
  剥离时刻意别加 `DirectionalLight`(玩法可能查太阳朝向,静默出错)与未证实是否加载的 `HLOD`。

**量物理体的一次性普查**(挂在世界就绪生命周期点,不进 tick):遍历本世界
`UPrimitiveComponent`,统计 `GetCollisionEnabled() != NoCollision` 的数量与 Top 归属类。
"开碰撞图元数"≈Chaos 已注册粒子数,即卡顿规模的 N 因子。

---

## 5. 陷阱五:服务端 WP 流送坏在"时机",不在"空间";用距离激活刷怪配套

### 5.1 空间上够,时机上不够
服务端流送(`wp.Runtime.EnableServerStreaming=1` + `EnableServerStreamingOut=1`)是唯一
"碰撞形状逐顶点不变"的减常驻手段:加载集 = 「以每个玩家为心、半径 `LoadingRange` 的球」相交的
cell(令 `LoadingRange ≈ CellSize` 即九宫格 3×3);多玩家取**并集**。两个 CVar 是
`ECVF_ReadOnly`,只能启动期设。原以为"流送会引入 `BlockTillLevelStreamingCompleted` 卡死"
——查源码后推翻:该阻塞只在 `OnWorldPreBeginPlay`/`OnWorldMatchStarting` 两处(世界启动),
开流送反而**缩短**它(只等出生点周边而非整图)。

**真陷阱是时机**:初始刷怪跑在 `OnWorldBeginPlay`,实测比首个玩家连入早 **82s**;那一刻
零 streaming source → 地形零加载 → 受重力的 spawn 物(Character)全部坠落。空间比较
(刷怪范围 2×2 格 vs 九宫格 3×3)在这里不适用——问题是"那一刻什么都没有"。

### 5.2 配套修法:刷怪按玩家距离激活(复用已有维护循环)
不新建 timer,挂在既有的 2s 维护 tick 上:

- **BeginPlay 不再刷**,只置就绪标记;首次填充由维护循环在玩家进入激活半径后完成。
- **距离门带 hysteresis**:未激活用 `R` 判入,已激活用 `R×1.25` 判出,防边界抖动反复刷怪/开关 tick。
- **激活半径默认 = CellSize**,与九宫格同口径,保证刷怪范围 ⊆ 加载范围。
- **地面门**:spawn 前从坐标上方向下射线(`ECC_WorldStatic`),不命中则本轮不刷、下轮重查
  (是"重查"不是"假设",见 §6.3);连续失败 edge-trigger 升一次 Warning(异步流入是合法暂态
  不刷屏,但坐标 Z 配错必须可见)。
- **远处只停 tick 不销毁**:`SetActorTickEnabled(false)` 保留血量/仇恨,玩家回来即恢复;
  销毁会让打了一半的怪消失或回满血 = 改玩法。
- **可一键回退**:激活半径 <= 0 即关闭整套机制,回旧行为。

这套改动**顺带解决了 5.1 的坠落**:刷怪只发生在某玩家(=source)的激活半径内 → 必在加载
范围内 → 脚下有地形。

---

## 5b. 陷阱六:Chaos 加速结构"超阈值就放弃时间切片"——玩家一移动就秒级卡死

> 来源:2026-08-03 map8 官方 Insights 画像(405s 战斗窗,146 刷怪点,poison malloc 已按 §1
> 关闭的 r1647 镜像——即本条是 §1 修完后**剩余**的秒级卡点,两者在同一条
> `FEndPhysicsTickFunction → FChaosScene::EndFrame` 路径上接力)。

### 症状
战斗期帧时长中位数 2.4ms,但 `FEngineLoop::Tick` I.Max = **5.24s**;业务心跳相邻启动间隔
每分钟窗出现 6~8.4s 尖峰;玩家体感"一移动就卡住,动都动不了"(服务器整帧停摆时移动输入
无人处理,客户端预测被拉回原地)。Insights 里尖峰帧落在:
`TG_EndPhysics → Flip Results → CreateExternalAccelerationStructure`(单帧 3.58s)与
`TG_StartPhysics → ComputeIntermediateSpatialAcceleration`(单帧 0.78s)。

### 根因(UE 5.8 源码,PBDRigidsEvolution.cpp)
引擎本有分帧(时间切片)重建,但有一个保守的放弃阈值:

- `:912` — `ForceFullBuild = AsyncAccelerationQueue.Num() > AccelerationStructureTimeSlicingMaxQueueSizeBeforeForce`(默认 **1000**);
- `:185` — ForceFullBuild 时 `MaxNumToProcess=0`,构建不再分帧;
- `:411` — ForceFullBuild 时 `ProgressCopyTimeSliced(..., -1)`,外部结构**无上限一帧拷完**
  (平时限额 `MaxBytesCopy` 默认仅 100KB/帧;实测 Artic01 ChaosAcceleration 结构 175MB)。

玩家移动 → WP 服务端流送换 cell → PCG 岩石 ISM **数千静态粒子一批入队** → 必然击穿 1000 →
强制全量重建 + 全量拷贝,全部发生在游戏线程一帧内。站桩不动时只有便宜的周期性增量 swap
(实测 405s 内 1301 次、均值 1.86ms),**不是"流送一直在重建"**;九宫格加载窗本身无罪,
罪在"单批变更量 × 放弃切片阈值"。

### 修法(fleet env 一行,已落 `deploy/k8s/agones/20-fleet-battle.yaml`)
```
-dpcvars=...,p.Chaos.AccelerationStructureTimeSlicingMaxQueueSizeBeforeForce=1000000,p.Chaos.AccelerationStructureTimeSlicingMaxBytesCopy=2000000
```
- 阈值 100 万 = cell 交换永不触发全量重建,走引擎自带分帧路径(新增静态体在 dirty 树中可查,
  正确性由引擎既有机制保证,代价是结构"追赶期"内查询略贵——平滑退化换掉硬停摆);
- 拷贝限额 100KB→2MB/帧:175MB ÷ 2MB ≈ 90 帧(1.5s)摊完,单帧 2MB memcpy 亚毫秒;
  默认 100KB 要 1750 帧,追赶期过长。
- **治本仍在资产侧**(与 §4 同一因果链):63 个 `CTF_UseComplexAsSimple` 岩石凸包化、
  纯装饰岩 DS 上 `NoCollision`,把单 cell 粒子量降到阈值量级以下。

### 验证判据
重图 map8 跑 ≥5 分钟并**持续跨 cell 移动**:①心跳摘要 `相邻启动最大间隔` 回落到 ~5.5s
节奏(不再出现 6s+ 尖峰);②(若带 trace)`CreateExternalAccelerationStructure` I.Max 从
3580ms 降到 <50ms;③`SceneQueryTotal` 总量无显著上涨(dirty 树代价可接受的证据)。

---

## 5c. 陷阱七:画像埋点会把 DS 加载期阻塞推过 ACTIVE 判弃线,带埋点的 DS 进场即被杀

### 症状
按画像手册注入 `-trace=... -llm -llmcsv -statnamedevents` 后,玩家进图 ~3 秒断线,DS 在分配
后 ~35s 被回收;DS 日志:`Battle 心跳启动间隔 15.64s/22.18s 超过后端 ACTIVE 判弃阈值 15s
(游戏线程被阻塞?)` → `收到 ds_allocator 指令 stop`。

### 根因
战斗业务心跳由**游戏线程**驱动,首跳要等地图加载完;LLM 逐分配打标 + trace 落盘让 Artic01
加载期游戏线程阻塞实测 **15.6~22.2s**(无埋点时 <15s 勉强过线),超过 ds_allocator
`heartbeat_timeout: "15s"` → 判弃。结构性对照:同配置 READY 阶段早因冷加载证据放宽
`ready_wait_timeout=120s`(INC-20260727-001),ACTIVE 阶段没有加载期宽限。
**根治方向(待立项,动判弃代码必须带回归测试)**:业务心跳线程化,或 allocator 对
"分配后首跳"给 phase-aware 宽限;不要一刀切抬 15s——那是所有真实崩溃场景的补偿时延(§9.4)。

### 画像期临时操作(用完必还原)
```bash
# 抬线(45s 依据:埋点加载实测最坏 22.2s ×2 余量;minikube 冷加载先例 45s)
kubectl get secret pandora-config -n pandora -o jsonpath='{.data.ds-allocator\.yaml}' | base64 -d \
  | sed 's/heartbeat_timeout: "15s"/heartbeat_timeout: "45s"/' | base64 -w0  # patch 回 secret 同 key
kubectl rollout restart deploy ds-allocator -n pandora   # distroless 无 sh,验证看启动日志 service_ready.heartbeat_timeout
# 画像结束后按原值还原并再次 rollout restart
```
注意:①生效配置在 Secret `pandora-config`(key `ds-allocator.yaml`),仓库 `etc/*.yaml` 只是
模板,只改仓库文件不生效;②DS 侧告警文案里的"15s"是硬编码,抬线后文案不会跟着变。

---

## 6. 方法论(比具体 bug 更值得带走)

### 6.1 先修取证,再谈修根因
不能靠"某段时间没日志"下结论——先排除采集缺口(§3 stdout 缓冲)和检测缺口(§2 阈值)。
本事故正是先修好这两条,才拿到堆栈,才有后面一切。

### 6.2 堆栈看中间帧,不看栈顶
挂起检测的栈顶是抓栈动作自己(`CaptureStackBackTrace`/`ThreadStackWalker`)。第一次只拿到
到 `RunTickGroup`,只能判"停在某 tick";中间帧齐了才定位到 `FChaosScene::EndFrame →
FPendingSpatialDataQueue::Remove`。**堆栈不全时别急着下结论,补齐中间帧再说。**

### 6.3 定时器判别:到期"重查"是兜底,到期"假设成功"是掩盖
| | ❌ 禁止 | ✅ 强制 |
|---|---|---|
| 到期动作 | 假设已就绪,继续往下走 | 重查权威,按结果决定 |
| 正确性 | 机器慢/网络抖就错 | 与时间无关 |

§5.2 的地面门是后者(不命中就不刷、下轮再查);若写成"延迟 2s 后假设地形好了直接刷",就是前者。
详见工作区 `CLAUDE.md §16.10`。

### 6.4 先量后改:数字会推翻直觉
本事故里三次判断被数据纠正:
- "流送引入阻塞卡死" → 查源码:阻塞只在世界启动两处,开流送反而缩短。
- "排除纯装饰类能减 N" → 查类分布:能安全排的那批没有碰撞,不减 N。
- "九宫格罩得住刷怪范围" → 查时间线:刷怪比玩家进场早 82s,空间比较不适用。

**优化前先做一次性普查(§4),拿到 N 的实际构成再定方向。** 本事故 N=613(不算多),
因此结论是"poison malloc 是主刀,流送主要省内存",而非最初以为的"减 N 修卡顿"。

### 6.5 分清"看着相关"和"真的相关"
`bCanEverTick=false` 看着省性能,对物理卡顿无效;纯渲染类剥离看着减负,没碰撞就不减 N。
每次优化前问一句:**要砍的这个量,和要解决的症状,中间的因果链是什么?链断了就是白做。**

---

## 7. 一次改动的验证判据清单(复用模板)
跑一局(**必须用重图 Artic01 / map_id=8,小图如松林镇不触发物理卡顿**,且跑 ≥3 分钟让怪死几轮):

1. `FMallocPoisonProxy::` 帧命中 = 0 → §1 生效
2. `Hang detected on GameThread` 是否出现、卡多久 → §1 真实效果
3. `DS 物理体普查: 图元总数=.. 开碰撞=..` → §4 拿到 N 实际值
4. stdout 摄取时刻分散在多个秒 → §3 生效
5. `刷怪成功` 时间戳晚于玩家进场、非 BeginPlay 一次性刷满 → §5.2 生效
6. `IsServerStreamingEnabled` = 期望值 → §5.1 是否启用
