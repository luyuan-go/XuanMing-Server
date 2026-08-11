<#
.SYNOPSIS
  Pandora 后端「策划一键启动」(只要装 Docker,双击即用)。

.DESCRIPTION
  面向策划的极简入口:默认 docker 模式不需要装 Go、不需要会编译,机器上只要有 Docker Desktop。
  做的事:
    1) 检查 Docker —— 没装就引导安装(能 winget 就自动装),没在跑就帮忙把 Docker Desktop 拉起来并等待就绪。
    2) Docker 就绪后,把整套后端跑起来(基础设施 + 22 个业务服务容器)。
       首次会在容器内编译镜像(稍慢),之后复用缓存秒起。
  -Battle(含战斗混合模式)【已废弃 2026-07-14】:Windows DS 只保留给 start.ps1 -Mode local
  断点调试;要真实 Hub/Battle DS 一律走 k8s + Agones(Linux DS)。-Battle -Stop 仍可用于
  清理旧机器遗留的含战斗环境。见 docs/design/decision-revisit-retire-battle-mode.md。

.EXAMPLE
  pwsh tools/scripts/play.ps1               # 本机 docker 版(DS=mock,测登录/业务)
  pwsh tools/scripts/play.ps1 -Intranet     # 内网服务器版(全容器,绑内网 IP,DS=mock)
  pwsh tools/scripts/play.ps1 -Battle -Stop # 清理遗留的含战斗环境(battle 模式已废弃)
  pwsh tools/scripts/play.ps1 -Status       # 看状态
  # 要真实战斗 DS:内网服务器双击 内网服务器一键启动-k8s集群.cmd(或 start.ps1 -Mode k8s)
#>
[CmdletBinding()]
param(
    [switch]$Stop,     # 停止整套后端
    [switch]$Status,   # 只看状态,不启动
    [switch]$Battle,   # 【已废弃】仅 -Battle -Stop/-Status 可用(清理/查看遗留含战斗环境);启动会拒绝,真 DS 走 k8s
    [switch]$Intranet, # 内网服务器模式:全容器 + 绑内网 IP,供局域网内策划客户端连(DS=mock)
    [switch]$OpenEditor, # 启动完成后打开发行版 UE Editor,用 PIE/Standalone 当客户端进服
    [switch]$OpenClient  # 启动完成后打开已打包 Windows 客户端
)

$ErrorActionPreference = 'Stop'
$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path "$ScriptDir/../..").Path
$StartPs1    = Join-Path $ScriptDir 'start.ps1'
$UeProject   = 'C:\work\Pandora-Client-SVN\Pandora\Pandora.uproject'
$UeEditorExe = 'F:\UnrealEngine-5.8.0-release\Engine\Binaries\Win64\UnrealEditor.exe'
$PackagedClientExe = 'C:\work\Pandora-Client-SVN\Pandora\Saved\StagedBuilds\Windows\Pandora.exe'

# ===== 输出辅助 =====
function Write-Info($m) { Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Write-Ok($m)   { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Write-Warn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Write-Err($m)  { Write-Host "[ERR ] $m" -ForegroundColor Red }
function Write-Step($m) { Write-Host "`n===== $m =====" -ForegroundColor Magenta }

function Test-CommandExists([string]$cmd) {
    return [bool](Get-Command $cmd -ErrorAction SilentlyContinue)
}

function Open-UeEditorClient {
    if (-not (Test-Path $UeEditorExe)) {
        Write-Warn "找不到 UE Editor:$UeEditorExe"
        Write-Warn '       可手动打开同版本发行版 Editor,再打开 Pandora.uproject。'
        return
    }
    if (-not (Test-Path $UeProject)) {
        Write-Warn "找不到 UE 工程:$UeProject"
        return
    }
    Write-Info '打开 UE Editor。进工程后用 Play/Standalone 作为客户端登录;不要用 Listen Server。'
    Start-Process -FilePath $UeEditorExe -ArgumentList "`"$UeProject`"" | Out-Null
}

function Open-PackagedClient {
    if (-not (Test-Path $PackagedClientExe)) {
        Write-Warn "找不到已打包 Windows 客户端:$PackagedClientExe"
        Write-Warn '       可先用 UE 打 Windows Client 包,或直接用 -OpenEditor 进 Editor 测。'
        return
    }
    Write-Info '打开已打包 Windows 客户端。'
    Start-Process -FilePath $PackagedClientExe -WorkingDirectory (Split-Path -Parent $PackagedClientExe) | Out-Null
}

function Maybe-OpenUeClient {
    if ($OpenEditor -and $OpenClient) {
        Write-Warn '同时传了 -OpenEditor / -OpenClient;只打开 Editor。'
        Open-UeEditorClient
        return
    }
    if ($OpenEditor) { Open-UeEditorClient; return }
    if ($OpenClient) { Open-PackagedClient; return }
}

function Test-DockerRunning {
    if (-not (Test-CommandExists 'docker')) { return $false }
    docker info *> $null
    return ($LASTEXITCODE -eq 0)
}

# 尝试找到 Docker Desktop 可执行文件
function Get-DockerDesktopExe {
    $candidates = @(
        (Join-Path $Env:ProgramFiles 'Docker\Docker\Docker Desktop.exe'),
        (Join-Path ${Env:ProgramFiles(x86)} 'Docker\Docker\Docker Desktop.exe')
    )
    foreach ($p in $candidates) {
        if ($p -and (Test-Path $p)) { return $p }
    }
    return $null
}

# 装 Docker 前确认 CPU 虚拟化已开(没开装了也起不来,先拦住省得白折腾)。
# 只做只读检查:返回 $false 表示明确未开启;读不到状态时放行(返回 $true)。
function Test-VirtualizationOn {
    try {
        $virt = (Get-CimInstance Win32_Processor -ErrorAction Stop |
                 Select-Object -First 1 -ExpandProperty VirtualizationFirmwareEnabled)
        if ($virt) {
            Write-Ok 'CPU 虚拟化已开启'
            return $true
        }
        Write-Err 'CPU 虚拟化没开 —— Docker Desktop 装了也起不来。'
        Write-Host '       请重启进 BIOS 开启 Intel VT-x / AMD-V(虚拟化),再双击本脚本。' -ForegroundColor Yellow
        return $false
    } catch {
        # 读不到就别拦策划,放行交给 Docker Desktop 自己引导
        return $true
    }
}

# 确保 Docker 命令存在
function Ensure-DockerInstalled {
    if (Test-CommandExists 'docker') {
        Write-Ok 'Docker 已安装'
        return $true
    }
    Write-Warn 'Docker 未安装'
    if (-not (Test-VirtualizationOn)) {
        return $false
    }
    if (Test-CommandExists 'winget') {
        Write-Info '尝试用 winget 安装 Docker Desktop(可能要几分钟)...'
        winget install --id Docker.DockerDesktop --silent --accept-source-agreements --accept-package-agreements | Out-Null
    } else {
        Write-Err '未找到 winget,无法自动安装。'
    }
    Write-Host ''
    Write-Warn '请完成 Docker Desktop 安装(可能需要重启电脑),'
    Write-Warn '然后『启动 Docker Desktop』、等右下角鲸鱼图标变绿,再重新双击本脚本。'
    Write-Host '       手动下载:https://www.docker.com/products/docker-desktop/' -ForegroundColor Yellow
    return $false
}

# 确保 Docker daemon 在跑(没跑就尝试拉起 Docker Desktop 并等待就绪)
function Ensure-DockerRunning {
    if (Test-DockerRunning) {
        Write-Ok 'Docker 正在运行'
        return $true
    }
    Write-Warn 'Docker 已装但还没运行,尝试启动 Docker Desktop...'
    $exe = Get-DockerDesktopExe
    if ($exe) {
        Start-Process -FilePath $exe | Out-Null
    } else {
        Write-Warn '没找到 Docker Desktop 程序,请手动从开始菜单启动它。'
    }

    Write-Info 'Docker 首次启动较慢,正在等待就绪(最多约 3 分钟)...'
    $maxTries = 90          # 90 * 2s = 180s
    for ($i = 1; $i -le $maxTries; $i++) {
        if (Test-DockerRunning) {
            Write-Ok 'Docker 已就绪'
            return $true
        }
        Start-Sleep -Seconds 2
        if ($i % 10 -eq 0) { Write-Info "  仍在等待 Docker 启动... ($($i*2)s)" }
    }
    Write-Err 'Docker 还没就绪。请确认 Docker Desktop 已启动(右下角鲸鱼图标变绿)后重试。'
    return $false
}

# 确保 mkcert 已安装(Envoy 本地 TLS 证书的自动签发 / 共享 CA 安装都靠它;
# 内网服务器要给局域网策划发 TLS 证书,缺了 dev_up 会在 Envoy 证书检查处报错退出)。
# 没装就 winget 自动装。返回 $true=已就绪;$false=刚装上但当前终端 PATH 没刷新,需新开终端重跑。
function Ensure-MkcertInstalled {
    if (Test-CommandExists 'mkcert') {
        Write-Ok 'mkcert 已就绪'
        return $true
    }
    Write-Warn 'mkcert 未安装(Envoy 本地 TLS 证书需要它)'
    if (-not (Test-CommandExists 'winget')) {
        Write-Err '未找到 winget,无法自动安装 mkcert;请手动装:https://github.com/FiloSottile/mkcert#installation'
        return $false
    }
    Write-Info '尝试用 winget 安装 mkcert(FiloSottile.mkcert,可能要几分钟)...'
    winget install --id FiloSottile.mkcert --silent --accept-source-agreements --accept-package-agreements | Out-Null
    if (Test-CommandExists 'mkcert') {
        Write-Ok 'mkcert 安装成功'
        return $true
    }
    Write-Warn 'mkcert 已装好,但当前终端还找不到 mkcert 命令(PATH 未刷新属正常)。'
    Write-Warn '       请『新开一个终端』(或重新双击本 .cmd)后再运行一次。'
    return $false
}

# ===== 主流程 =====
Write-Host ''
Write-Host '============================================' -ForegroundColor Magenta
Write-Host '  Pandora 后端  策划一键启动' -ForegroundColor Magenta
Write-Host '============================================' -ForegroundColor Magenta

if (-not (Test-Path $StartPs1)) {
    Write-Err "找不到 start.ps1:$StartPs1。请确认仓库完整(git pull)。"
    exit 1
}

# 看状态:不需要 daemon 也能看(看不到就提示)
if ($Status) {
    if ($Battle) {
        & $StartPs1 -Mode battle -Status
    } elseif ($Intranet) {
        & $StartPs1 -Mode intranet -Status
    } else {
        & $StartPs1 -Mode docker -Status
    }
    exit $LASTEXITCODE
}

# 停止
if ($Stop) {
    if (-not (Test-CommandExists 'docker')) {
        Write-Warn 'Docker 未安装,无需停止。'
        exit 0
    }
    Write-Step '停止整套后端'
    if ($Battle) {
        & $StartPs1 -Mode battle -Down
    } elseif ($Intranet) {
        & $StartPs1 -Mode intranet -Down
    } else {
        & $StartPs1 -Mode docker -Down
    }
    Write-Ok '已停止。'
    exit $LASTEXITCODE
}

# ===== 本地战斗版【已废弃 2026-07-14】 =====
# 决策(docs/design/decision-revisit-retire-battle-mode.md):Windows DS(PandoraServer.exe)
# 只保留给 start.ps1 -Mode local 断点调试;其他一切要真 DS 的场景一律 k8s + Agones(Linux DS)。
# -Battle -Stop / -Battle -Status 在上面已处理(保留,用于清理/查看旧机器遗留环境)。
if ($Battle) {
    Write-Err '-Battle(含战斗混合模式)已废弃:Windows DS 只在 start.ps1 -Mode local(断点调试)启动。'
    Write-Host '  要进真实 Hub/Battle DS 请用 k8s + Agones(Linux DS):' -ForegroundColor Yellow
    Write-Host '    内网服务器:双击 内网服务器一键启动-k8s集群.cmd(局域网客户端直连,策划机免安装)' -ForegroundColor Yellow
    Write-Host '    本机联调:  pwsh tools/scripts/start.ps1 -Mode k8s' -ForegroundColor Yellow
    Write-Host '  清理本机遗留的含战斗环境:pwsh tools/scripts/play.ps1 -Battle -Stop' -ForegroundColor DarkGray
    Write-Host '  详见 docs/design/decision-revisit-retire-battle-mode.md' -ForegroundColor DarkGray
    exit 1
}

# ===== 内网服务器版:全容器 + 绑内网 IP,供局域网内策划客户端连(DS=mock)=====
if ($Intranet) {
    Write-Step '内网服务器版(全容器,绑内网 IP,供多人连)'
    Write-Info '这台机器当「内网测试服」:基础设施 + 后端服务都跑在本机容器里,'
    Write-Info '策划只需在各自客户端填本机内网 IP 就能连进来,他们不用装 Docker。'

    Write-Step '检查 Docker'
    if (-not (Ensure-DockerInstalled)) { exit 1 }
    if (-not (Ensure-DockerRunning))   { exit 1 }

    # 内网服务器要给局域网策划发 TLS 证书,mkcert 必备(Envoy 证书自动签发 / 共享 CA 安装都靠它)。
    Write-Step '检查 mkcert(Envoy 本地 TLS 证书)'
    if (-not (Ensure-MkcertInstalled)) { exit 1 }

    # 委托 intranet 模式:同 docker 全容器(DS=mock),但绑 0.0.0.0 并打印内网地址。
    & $StartPs1 -Mode intranet
    $rc = $LASTEXITCODE
    Write-Host ''
    if ($rc -eq 0) {
        Write-Ok '内网测试服已启动!上面绿色行里的「内网 IP 地址」发给策划,'
        Write-Host '  - 策划把 UE 客户端后端地址指向那个 https://<内网IP>:8443 即可登录。' -ForegroundColor Green
        Write-Host '  - 策划机不用装 Docker / 不用装 Go,只要能连内网。' -ForegroundColor Green
        Write-Host '  - DS=mock:能测登录/大厅/业务,但进不了真实战斗 DS。' -ForegroundColor Yellow
        Write-Host '  - 看状态:        pwsh tools/scripts/play.ps1 -Intranet -Status' -ForegroundColor DarkGray
        Write-Host '  - 停止:          pwsh tools/scripts/play.ps1 -Intranet -Stop' -ForegroundColor DarkGray
    } else {
        Write-Err '启动过程中出错了,请把上面的红色 [ERR] 信息发给后端同学。'
    }
    exit $rc
}

# 启动:先把 Docker 准备好
Write-Step '检查 Docker'
if (-not (Ensure-DockerInstalled)) { exit 1 }
if (-not (Ensure-DockerRunning))   { exit 1 }

# Envoy 本地 TLS 证书需要 mkcert(自动签发 / 校验),没装先自动补上。
Write-Step '检查 mkcert(Envoy 本地 TLS 证书)'
if (-not (Ensure-MkcertInstalled)) { exit 1 }

# 委托给已验证的 docker 模式:基础设施 + 22 个业务服务全容器化
# (首次会在容器内编译镜像,稍慢;之后复用缓存。策划本机不需要装 Go。)
& $StartPs1 -Mode docker
$rc = $LASTEXITCODE

Write-Host ''
if ($rc -eq 0) {
    Write-Ok '后端已启动!'
    Write-Host '  - 客户端网关(Envoy): https://127.0.0.1:8443' -ForegroundColor Green
    Write-Host '  - docker 模式 DS=mock,只能测登录/业务;要进真实 Hub/Battle DS 请用 start.ps1 -Mode k8s。' -ForegroundColor Yellow
    Write-Host '  - 看运行状态:  pwsh tools/scripts/play.ps1 -Status' -ForegroundColor DarkGray
    Write-Host '  - 停止:        pwsh tools/scripts/play.ps1 -Stop' -ForegroundColor DarkGray
    Maybe-OpenUeClient
} else {
    Write-Err '启动过程中出错了,请把上面的红色 [ERR] 信息发给后端同学。'
}
exit $rc
