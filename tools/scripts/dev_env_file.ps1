# 本机 dev.env 自举(dot-source 的纯函数库,不要直接 -File 执行)。
#
# 为什么必须自动做:`deploy/env/dev.env` 命中 .gitignore 第 45 行的 `*.env`,受版本控制的
# 只有 `dev.env.example`。也就是说**任何**新机器 / 新克隆第一次跑,这个文件必然不存在,而
# docker compose 的 `--env-file` 指到不存在的路径会直接失败 —— 基础设施起不来,后面全断。
# 让策划自己去读错误提示手工 Copy-Item,「一键启动」就不叫一键了(2026-08-12 现场:另一台
# 机器刚更新完双击一键启动,卡死在 [1/4] 基础设施,而外层还报「完成(退出码 0)」)。
#
# 自动复制是安全的:example 里全是 dev 级默认值(弱口令 + 空 webhook),没有一个真 secret;
# 群告警 URL 留空时 Grafana 不生成对应 receiver,不影响启动。真实 webhook/token 由本机改写,
# dev.env 被忽略,永远不会入库。
#
# 用法:
#   . "$PSScriptRoot/dev_env_file.ps1"
#   Confirm-DevEnvFile -ProjectRoot $ProjectRoot

function Confirm-DevEnvFile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][string]$ProjectRoot
    )

    $envFile     = Join-Path $ProjectRoot 'deploy/env/dev.env'
    $exampleFile = "$envFile.example"

    if (Test-Path -LiteralPath $envFile) { return }

    # 连样例都没有 = 工作区不完整(浅克隆 / 拉了一半),这才是真该硬失败的情况。
    if (-not (Test-Path -LiteralPath $exampleFile)) {
        throw "缺 $envFile,且样例 $exampleFile 也不在;工作区不完整,请先更新拿全 deploy/env/。"
    }

    $envDir = Split-Path -Parent $envFile
    if (-not (Test-Path -LiteralPath $envDir)) {
        New-Item -ItemType Directory -Force -Path $envDir | Out-Null
    }

    # 先写临时文件再原子改名:策划双击两次(两个窗口同时跑)时,另一个进程绝不会读到只写了
    # 一半的 dev.env。改名不覆盖 —— 谁先到算谁的,输的一方清掉临时文件即可,内容本来就一样。
    $tmpFile = "$envFile.$PID.tmp"
    try {
        Copy-Item -LiteralPath $exampleFile -Destination $tmpFile -Force
        [System.IO.File]::Move($tmpFile, $envFile, $false)
    } catch [System.IO.IOException] {
        # 目标已存在 = 另一个进程刚建好,按成功处理。
        if (-not (Test-Path -LiteralPath $envFile)) { throw }
    } finally {
        if (Test-Path -LiteralPath $tmpFile) { Remove-Item -LiteralPath $tmpFile -Force -ErrorAction SilentlyContinue }
    }

    Write-Host "[INFO] 本机缺 deploy/env/dev.env(被 git 忽略,不随更新发过来),已按 dev.env.example 初始化。" -ForegroundColor Cyan
    Write-Host "       内容是 dev 默认值,本地联调直接可用;要接企微/飞书告警再自行填 webhook(改动不入库)。" -ForegroundColor DarkGray
}
