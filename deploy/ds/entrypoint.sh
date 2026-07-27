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

# UE 的 LogNet 默认会把 Login/Join URL 原样写入 stdout；URL 中含短期 DSTicket，
# 即使票据有 TTL/JTI/实例绑定也不应进入集中日志。用两个互补入口把该分类固定为
# Warning：ini override 覆盖启动期，LogCmds 覆盖运行期；两项都放在用户 EXTRA_ARGS
# 之后，避免调试参数意外重新打开含票的 Display/Log 级别。
#
# 不在启动日志回显 EXTRA_ARGS：它是运维扩展入口，未来可能承载敏感值。
echo "[entrypoint] 启动 Pandora DS: ${SERVER_LAUNCH[*]} ${MAP_URL} -port=${PORT} -log [LogNet=Warning]"
echo "[entrypoint] AGONES_SDK_HTTP_PORT=${AGONES_SDK_HTTP_PORT:-<unset>} AGONES_SDK_GRPC_PORT=${AGONES_SDK_GRPC_PORT:-<unset>}"

# exec 让 DS 成为 PID 1，正确接收 SIGTERM（Agones 回收 Pod 时优雅退出）。
exec "${SERVER_LAUNCH[@]}" "${MAP_URL}" -port="${PORT}" -log ${EXTRA_ARGS} \
  '-ini:Engine:[Core.Log]:LogNet=Warning' \
  '-LogCmds=LogNet Warning'
