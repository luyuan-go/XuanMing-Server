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
exec "${FINAL_LAUNCH[@]}" "${MAP_URL}" -port="${PORT}" -log ${EXTRA_ARGS} \
  '-ini:Engine:[Core.Log]:LogNet=Warning' \
  '-LogCmds=LogNet Warning' \
  '-ini:Engine:[Core.System]:HangDuration=10.0' \
  '-ini:Engine:[Core.System]:HangsAreFatal=False'
