#!/usr/bin/env bash
# Pandora Linux DS 容器入口。
# - 由 Agones 调度，sidecar 通过 env 注入 AGONES_SDK_HTTP_PORT / AGONES_SDK_GRPC_PORT。
# - DS 进程内 UPandoraAgonesSubsystem 读这些 env 后走本地 HTTP 调 Ready/Health/Shutdown。
# - 关卡 / 端口可由 K8s env 覆盖：PANDORA_DS_MAP / PANDORA_DS_PORT。
# - GameMode 可由 PANDORA_DS_GAMEMODE 指定（UE 标准 ?game= URL option，优先级高于地图 WorldSettings，
#   无需改地图资产）。战斗 Fleet 设成 /Script/Pandora.PandoraBattleGameMode，让其 BeginPlay 起业务心跳。
set -euo pipefail

MAP="${PANDORA_DS_MAP:-/Game/Entry/Entry}"
PORT="${PANDORA_DS_PORT:-7777}"
GAMEMODE="${PANDORA_DS_GAMEMODE:-}"
MAX_PLAYERS="${PANDORA_DS_MAX_PLAYERS:-}"
EXTRA_ARGS="${PANDORA_DS_EXTRA_ARGS:-}"

if [[ -n "${MAX_PLAYERS}" && ! "${MAX_PLAYERS}" =~ ^[1-9][0-9]*$ ]]; then
  echo "[entrypoint] PANDORA_DS_MAX_PLAYERS 必须是正整数。" >&2
  exit 2
fi
if [[ -n "${MAX_PLAYERS}" ]]; then
  # 禁止直接用 bash 整数比较超长十进制：超过机器整数宽度可能回绕。先按 canonical
  # 十进制长度挡住 >10 位，再对 10 位值做等长字典序比较。
  if (( ${#MAX_PLAYERS} > 10 )) ||
     (( ${#MAX_PLAYERS} == 10 )) && [[ "${MAX_PLAYERS}" > "2147483647" ]]; then
    echo "[entrypoint] PANDORA_DS_MAX_PLAYERS 超出 UE int32 上限。" >&2
    exit 2
  fi
fi

# EXTRA_ARGS 仍按历史行为支持普通 UE 启动参数，但禁止借日志/控制台命令重新打开
# LogNet 的 Display/Log 级别。否则引擎会再次把含 ticket 的 Login/Join URL 写入 stdout。
EXTRA_ARGS_LOWER="${EXTRA_ARGS,,}"
if [[ "${EXTRA_ARGS_LOWER}" == *"logcmds"* ||
      "${EXTRA_ARGS_LOWER}" == *"execcmds"* ||
      "${EXTRA_ARGS_LOWER}" == *"ini:engine:[core.log]"* ]]; then
  echo "[entrypoint] PANDORA_DS_EXTRA_ARGS 禁止覆盖日志分类或执行控制台命令。" >&2
  exit 2
fi

# 把 GameMode 作为 ?game= URL option 拼到地图 URL 上（UE 标准做法，优先级高于地图 GameModeOverride）。
MAP_URL="${MAP}"
if [[ -n "${GAMEMODE}" ]]; then
  MAP_URL="${MAP}?game=${GAMEMODE}"
fi
if [[ -n "${MAX_PLAYERS}" ]]; then
  # UE FURL 的 option 以重复 '?' 分隔；Hub Fleet 用它把 GameSession.MaxPlayers 与
  # allocator capacity 机械钉成同一个值。Battle Fleet 不设该 env，行为保持不变。
  MAP_URL="${MAP_URL}?MaxPlayers=${MAX_PLAYERS}"
fi

SERVER_BIN="/home/pandora/server/Pandora/Binaries/Linux/PandoraServer"
if [[ -x "${SERVER_BIN}" ]]; then
  # Dockerfile 已在镜像构建期设置执行位。直接启动二进制，避免 UE 归档脚本每次
  # chmod 都触发 overlayfs 对数百 MB 二进制做 copy-up，拖过 Agones Health 阈值。
  SERVER_LAUNCH=("${SERVER_BIN}" Pandora)
else
  SERVER_SH="/home/pandora/server/PandoraServer.sh"
  if [[ ! -x "${SERVER_SH}" ]]; then
    # 不同 UE 版本归档脚本名可能不同，做个兜底查找。
    SERVER_SH="$(find /home/pandora/server -maxdepth 2 -name "*Server.sh" | head -n1 || true)"
  fi

  if [[ -z "${SERVER_SH}" || ! -e "${SERVER_SH}" ]]; then
    echo "[entrypoint] 找不到服务器二进制或启动脚本，请检查 stage/LinuxServer 打包产物。" >&2
    exit 1
  fi
  SERVER_LAUNCH=("${SERVER_SH}")
fi

# stdout 行缓冲（INC-20260729-002 根因门 A1）。
#
# 为什么必须显式做：UE 只在 `FUnixPlatformMisc::HasBeenStartedRemotely()`（= 环境变量
# SSH_CONNECTION 非空）或有调试器时才 `setvbuf(stdout, NULL, _IONBF, 0)`
# （UnixPlatformMisc.cpp）。容器里两者都不成立，stdout 对着管道走 libc 默认的 4KB
# 块缓冲，日志要攒满一块才吐给 kubelet / Loki。实测后果：该事故 Pod 的 11 条
# 「每分钟一次」health 窗口摘要（UE 时间 02:30:20 → 02:40:21，跨 10 分钟）在
# 02:40:47.252 一次性到达 Loki；进程被回收时最后一批只吐出 2 行，且第一行在
# `NickN` 处被截断 —— 也就是 02:42:09 之后 DS 写的所有日志（含解释「为什么停止
# 业务心跳」的第一现场）全部随缓冲区丢失。
#
# `-FORCELOGFLUSH` 救不了：它只作用于 `OutputDeviceFile`（写 .log 文件），不管 stdout。
# 所以用 POSIX 标准工具 `stdbuf` 把 stdout/stderr 改成行缓冲。stdbuf 自身在设置
# LD_PRELOAD/_STDBUF_* 后 execvp 目标进程，**DS 仍是 PID 1**，SIGTERM 语义不变。
#
# 兜底：镜像若被裁剪掉 coreutils，退回 UE 自己的无缓冲路径（置 SSH_CONNECTION 触发
# 上述 setvbuf(_IONBF)）。两条都不静默失败——用哪条会打进启动日志。
# 注意：不用「可能为空的数组 + set -u 下展开」这种写法（bash 4.3 及更早会报错），
# 直接把 stdbuf 前缀并进最终 argv 数组，空前缀情形天然不存在。
if command -v stdbuf >/dev/null 2>&1; then
  FINAL_LAUNCH=(stdbuf -oL -eL "${SERVER_LAUNCH[@]}")
  STDOUT_MODE="stdbuf(line-buffered)"
else
  # UE：SSH_CONNECTION 非空 → HasBeenStartedRemotely() → setvbuf(stdout, _IONBF)。
  export SSH_CONNECTION="${SSH_CONNECTION:-0.0.0.0 0 0.0.0.0 0}"
  FINAL_LAUNCH=("${SERVER_LAUNCH[@]}")
  STDOUT_MODE="ue-setvbuf-unbuffered(fallback: stdbuf 不可用)"
fi

# UE 的 LogNet 默认会把 Login/Join URL 原样写入 stdout；URL 中含短期 DSTicket，
# 即使票据有 TTL/JTI/实例绑定也不应进入集中日志。用两个互补入口把该分类固定为
# Warning：ini override 覆盖启动期，LogCmds 覆盖运行期；两项都放在用户 EXTRA_ARGS
# 之后，避免调试参数意外重新打开含票的 Display/Log 级别。
#
# 同一批 ini override 里打开引擎自带的挂起检测（INC-20260729-002 根因门 A1）：
#   - HangDuration=10：**把已有的检测阈值从默认 25s 降到 10s**（不是从零开启——
#     ThreadHeartBeat.cpp 的 HangDuration 默认就是 25.0，且 AllowThreadHeartBeat() 是
#     `!FParse::Param(..., "noheartbeatthread")` 即默认为真，server 构建下
#     USE_HANG_DETECTION 也成立，所以检测线程本来就在跑）。25s 大于 ds_allocator 的
#     ACTIVE heartbeat_timeout=15s，Pod 会先被回收，`Hang detected on GameThread`
#     + 卡住线程堆栈永远来不及打出来；降到 10s 才能抢在回收前留下证据。
#     引擎下限是 5s，取 10s 留出一拍 5s 业务心跳的正常抖动余量，不误报。
#   - HangsAreFatal=False：**显式钉住而非纠正默认值**。UE_ASSERT_ON_HANG 未被外部定义时
#     默认是 0（ThreadHeartBeat.cpp:28-30），所以默认本来就不致命；这里写明是为了防止
#     以后有人在 Target/Build 里定义 UE_ASSERT_ON_HANG=1 后，10s 阈值把「一次长加载」
#     变成「直接 assert 崩进程」。我们要的是**证据**不是自杀：Error + 堆栈即可，回收由
#     allocator 的 §9.4 补偿链负责（验收底线第 1 条：短暂不可用可接受，杀进程不可接受）。
#     Linux 上 PLATFORM_USE_MINIMAL_HANG_DETECTION=0，故走的是打堆栈而非 abort() 的分支。
# 只挂在 DS 启动参数上，不写进 Config/DefaultEngine.ini，避免影响客户端 / 编辑器。
echo "[entrypoint] 启动 Pandora DS: ${FINAL_LAUNCH[*]} ${MAP_URL} -port=${PORT} -log [LogNet=Warning][HangDuration=10]"
echo "[entrypoint] stdout 缓冲模式=${STDOUT_MODE}"
echo "[entrypoint] AGONES_SDK_HTTP_PORT=${AGONES_SDK_HTTP_PORT:-<unset>} AGONES_SDK_GRPC_PORT=${AGONES_SDK_GRPC_PORT:-<unset>}"

# exec 让 DS 成为 PID 1，正确接收 SIGTERM（Agones 回收 Pod 时优雅退出）。
# stdbuf 在 execvp 后被目标进程替换，PID 1 仍是 DS 本体。
#
# 服务端 World Partition 流送（INC-20260729-002 A9c）——**当前已停用，见文末阻断原因**。
#
# ⚠️ 下面这段推导仍然成立，但两条 CVar 已按 A9d 的结论注释掉。启用前必读文末。
#
# 为什么：Artic01 在 DS 上原本 `IsServerStreamingEnabled = 0`，即**整张图**的 actor 与
# 碰撞体一次性全部实例化并永久常驻。Chaos 的 FPendingSpatialDataQueue 里
# ParticleToPendingData 是 TArrayAsMap<FUniqueIdx,int32>（按粒子唯一号下标寻址，
# 长度≈已注册物理体数），全图常驻 = 该结构恒为全图规模，每帧 EndPhysics 刷新时逐条
# Remove 触发的 realloc 就按全图规模付代价 —— 这是那次 >10s 游戏线程卡死的 N 因子。
#
# 为什么这是唯一"形状不变"的减 N 手段：加载哪些 cell 由玩家位置 + LoadingRange 决定。
# 玩家所在之处，周围碰撞几何与今天逐顶点相同；变的只是玩家不在的远处不常驻。
# 相比之下降地形碰撞精度 / 复杂碰撞换简单碰撞 / 装饰物关碰撞都会改变玩家实际体验。
#
# 关于阻塞加载（重要，与此前判断相反）：引擎里 WP 的 BlockTillLevelStreamingCompleted
# 只出现在 OnWorldPreBeginPlay 与 OnWorldMatchStarting 两处（WorldPartition.cpp:1212-1226），
# 即**世界启动时**，不是游戏中逐 cell 流送。所以：
#   - 关流送（今天）= 启动时必须阻塞等**整张图**流完 → 就是那 30s 加载与加载期那次 Hang；
#   - 开流送 = 启动时只需等出生点周边那一批 cell → 阻塞窗口**变短**。
# 游戏中跨 cell 是非阻塞的，前提是网格的 bBlockOnSlowStreaming 保持 false
# （WorldPartitionRuntimeSpatialHash.cpp:102 构造默认即 false，不要去打开它）。
#
# 九宫格（3×3）就是原生行为，不需要自写：加载集 = 与「以玩家为心、半径 LoadingRange 的球」
# 相交的所有 cell。令 LoadingRange ≈ CellSize 即最多 3×3=9 格。CellSize / LoadingRange 在
# **地图的 World Partition 运行时网格设置**里（World Settings → World Partition Setup →
# Runtime Hash → Grids），改不到这里来，需在编辑器里按图调。
#
# EnableServerStreamingOut=1 让服务端也**卸载**远离的 cell（否则只加载不卸载，玩家走遍
# 全图后 N 又回到全图规模）。代价见事故档案 A9c：未被加载/已被卸载 cell 里的 actor 在
# 服务端不存在，角落刷怪点、任务触发器这类必须常驻的对象要标 bIsSpatiallyLoaded=false。
#
# 两个 CVar 都是 ECVF_ReadOnly（非编辑器构建），只能启动期由 ini/命令行设定，运行中改无效；
# 地图的 ServerStreamingMode 构造默认为 ProjectDefault，因此这两个 CVar 即为生效值。
# 放在 DS 启动参数而非 Config/DefaultEngine.ini：只改专服行为，客户端与编辑器零影响。
#
# ══ 为什么现在是注释掉的（A9d 阻断项，事故档案 §5.8）══════════════════════════
#
# 初始刷怪跑在 UMySpawnModel::OnWorldBeginPlay，**没有任何玩家距离门控**
# （Distance / Proximity / NearestPlayer / PlayerInRange 在该文件命中均为 0）。
# 实测同一局时间线：
#     02:40:47.552  OnWorldBeginPlay(LifeCycle): 启动刷怪
#     02:42:09.072  InitNewPlayer accepted   ← 首个玩家连入，晚 82 秒
#
# 而服务端流送的加载集完全由 streaming source 决定，服务端的 source 只来自
# APlayerController。世界 BeginPlay 时**零玩家 → 零 source → 地形零加载**。
# 于是 146 个受重力的 AMyNpcCharacter（AMyCharacter → Character，有
# CharacterMovementComponent）会 spawn 到配置表写死的世界坐标上
# （ESpawnActorCollisionHandlingMethod::AlwaysSpawn，全程无地面射线），
# 脚下没有任何碰撞面 → 全部下坠 → 玩法直接损坏。
#
# 注意这里**不是**"九宫格够不够大"的问题：刷怪点实测只跨 2×2 格（276m × 273m，
# 整图 16×16 格 / 4096m），空间上罩得住；坏的是**时机**——那一刻什么都还没加载。
# 宝箱不受影响（AMyLootChestActor : AActor，不受重力，三个组件 NoCollision/QueryOnly）。
#
# 启用前必须先选定并落地事故档案 §5.8 的方案之一：
#   ① 初始刷怪改为等**明确就绪事件**（首个玩家 Admission 完成 / 对应 cell 加载完成）。
#      必须挂真实事件，**不得用 Timer/Delay 掩盖时序**（CLAUDE.md §16.10）。
#   ② 用 Data Layer / 非空间加载把刷怪区那 2×2 格设为 always loaded，其余 252 格照常流送。
#   ③ 刷怪改为玩家进范围才生成（最贴流送模型，工作量最大）。
# 另需逐图复核：关卡 7（松林镇）有 279 个刷怪点，分布范围未按同一口径量过（A9h）。
#
# 启用方式：删掉下面两行的注释与前一行末尾的对齐即可（记得给上一行补 `\`）。
# ═════════════════════════════════════════════════════════════════════════════
exec "${FINAL_LAUNCH[@]}" "${MAP_URL}" -port="${PORT}" -log ${EXTRA_ARGS} \
  '-ini:Engine:[Core.Log]:LogNet=Warning' \
  '-LogCmds=LogNet Warning' \
  '-ini:Engine:[Core.System]:HangDuration=10.0' \
  '-ini:Engine:[Core.System]:HangsAreFatal=False'
  # A9d 阻断中，勿直接启用（理由见上方 ══ 段）：
  #   '-ini:Engine:[SystemSettings]:wp.Runtime.EnableServerStreaming=1' \
  #   '-ini:Engine:[SystemSettings]:wp.Runtime.EnableServerStreamingOut=1'
