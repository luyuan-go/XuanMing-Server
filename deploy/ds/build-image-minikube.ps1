# 直接在 minikube 的 Docker daemon 里构建 Pandora DS 镜像（避开 minikube image load 这一步）。
#
# 为什么不用 `minikube image build`：
#   PowerShell 下 `minikube image build` 对 Windows 路径有解析 bug，会把 `F:\...` 的盘符
#   当成 `F:` 目录、把上下文路径切坏。最稳的做法是用 `minikube docker-env --shell powershell`
#   把当前会话的 docker CLI 指到 minikube 内置 daemon，再跑普通 `docker build`——镜像直接落在
#   minikube 里，省掉宿主 build + `minikube image load` 的两步。
#
# 前置：
#   1) minikube 已起（start-minikube-agones.ps1）。
#   2) deploy/ds/stage/LinuxServer 已就绪（客户端仓库 build-linux-ds.ps1 拷好）。
#   3) 装了 docker CLI（minikube docker-env 只是改 DOCKER_HOST 等 env，仍需本机 docker 客户端）。
#
# 用法（后端仓库根目录）：
#   pwsh deploy/ds/build-image-minikube.ps1                       # 默认建 battle + hub 两个 :dev
#   pwsh deploy/ds/build-image-minikube.ps1 -Tag dev             # 自定义 tag（默认 dev，与 Fleet yaml 一致）
#   pwsh deploy/ds/build-image-minikube.ps1 -Image battle        # 只建 battle
#   pwsh deploy/ds/build-image-minikube.ps1 -Profile pandora-agones # 指定 minikube profile
#   pwsh deploy/ds/build-image-minikube.ps1 -SourcePkg <客户端>\Packages\Server_Linux_Development\LinuxServer  # 显式指定 UE Linux 包
#   pwsh deploy/ds/build-image-minikube.ps1 -BuildOnHost -PushRegistry              # 宿主构建 + 推制品 registry（localhost:5000）
#   pwsh deploy/ds/build-image-minikube.ps1 -BuildOnHost -PushRegistry -RegistryHost harbor.xxx:5000  # 推到别的 registry
#
# 推制品 registry（-PushRegistry，发布线③制品库层）：
#   必须配合 -BuildOnHost。非宿主模式下 docker CLI 被 minikube docker-env 指向 minikube
#   内置 daemon，'localhost:5000' 会解析到 minikube VM 内部而非宿主 registry 容器，
#   造成"push 看着成功、制品库里没有"。脚本对此 fail-closed 直接报错，不静默推错地方。
#   registry 由 tools/devops 栈提供：pwsh XuanMing-Server/tools/devops/up.ps1
#
# UE Linux DS 包来源（不写死路径，按下列优先级解析）：
#   1) 显式 -SourcePkg 参数
#   2) 环境变量 PANDORA_DS_LINUX_PKG
#   3) 后端仓库【同级目录】下的客户端仓库：<sibling>\Packages\Server_Linux_Development\LinuxServer
#      （优先名字匹配 Pandora-Client* 的同级仓库）
# 解析到就 robocopy /MIR 同步进 stage\LinuxServer（docker build 只能 COPY 构建上下文内的目录）；
# 没解析到则沿用已暂存的 stage\LinuxServer。
#
# 构建完镜像已在 minikube 里，Fleet yaml 的 imagePullPolicy=IfNotPresent 直接命中；
# 跑 e2e_k8s.ps1 时加 -SkipImageLoad 跳过 minikube image load。

param(
    [ValidateSet('both', 'battle', 'hub')]
    [string]$Image = 'both',
    [string]$Tag = 'dev',
    [string]$Profile = '',
    [string]$BaseImage = 'ubuntu:22.04',
    # UE Linux DS 包路径（留空则按上面注释的优先级自动解析同级客户端仓库）
    [string]$SourcePkg = '',
    # 在【宿主 docker daemon】构建（不切到 minikube 内置 daemon）。内网/断网必用：
    # minikube 内置 daemon 往往没有 ubuntu:22.04 基础镜像也拉不到公网 → FROM 失败；
    # 宿主 docker 有 ubuntu + apt 缓存层，离线只重跑 COPY 层即可。构建完由调用方
    # （start.ps1 的 Sync-ImagesToMinikube）用 minikube image load 落进集群。
    [switch]$BuildOnHost,
    # 构建完把镜像推到制品 registry（tools/devops 的 registry:2，发布线③制品库层）。
    # 必须配合 -BuildOnHost：详见下方 Push-One 的说明（minikube 内置 daemon 的
    # localhost 是 VM 自己，不是宿主 registry，会推丢）。
    [switch]$PushRegistry,
    [string]$RegistryHost = 'localhost:5000'
)

$ErrorActionPreference = 'Stop'

# 前置校验放最前面：fail fast，别等 robocopy 同步完几 GB 才报错。
# -PushRegistry 只能在宿主 daemon 模式下用：非 -BuildOnHost 时本会话 docker CLI 被
# minikube docker-env 指向 minikube 内置 daemon，push 由那个 daemon 发起，
# 'localhost:5000' 解析到 minikube VM 自己而非宿主机的 registry 容器 → 静默推错地方。
# 宁可明确报错，也不做"看着成功、制品库里啥也没有"的假成功。
if ($PushRegistry -and -not $BuildOnHost) {
    throw "-PushRegistry 必须配合 -BuildOnHost 使用。原因：非宿主构建时 docker CLI 指向 minikube 内置 daemon，'$RegistryHost' 会解析到 minikube VM 内部，推不到宿主 registry。正确用法：pwsh deploy/ds/build-image-minikube.ps1 -BuildOnHost -PushRegistry"
}
# registry 探活也放最前：与其同步几 GB 包、构建十几分钟后才卡在 push，不如现在就报。
if ($PushRegistry) {
    try {
        Invoke-WebRequest -Uri "http://$RegistryHost/v2/" -TimeoutSec 5 -UseBasicParsing | Out-Null
        Write-Host "[build-image-minikube] 制品 registry 可达：$RegistryHost" -ForegroundColor DarkGray
    } catch {
        throw "制品 registry '$RegistryHost' 不可达：$($_.Exception.Message)。先起 DevOps 栈：pwsh XuanMing-Server/tools/devops/up.ps1"
    }
}

$ScriptDir = $PSScriptRoot
$StageDir = Join-Path $ScriptDir 'stage\LinuxServer'
$Dockerfile = Join-Path $ScriptDir 'Dockerfile'
$RepoRoot = (Resolve-Path (Join-Path $ScriptDir '..\..')).Path

# 解析 UE Linux DS 包（不写死路径）：显式 -SourcePkg > 环境变量 > 同级客户端仓库。
function Resolve-LinuxPkg {
    if (-not [string]::IsNullOrWhiteSpace($SourcePkg)) {
        if (-not (Test-Path -LiteralPath $SourcePkg)) { throw "指定的 -SourcePkg 不存在：$SourcePkg" }
        return (Resolve-Path -LiteralPath $SourcePkg).Path
    }
    if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_DS_LINUX_PKG) -and (Test-Path -LiteralPath $env:PANDORA_DS_LINUX_PKG)) {
        return (Resolve-Path -LiteralPath $env:PANDORA_DS_LINUX_PKG).Path
    }
    # 后端仓库同级目录里找客户端仓库（含 Packages\Server_Linux_Development\LinuxServer）
    $rel = 'Packages\Server_Linux_Development\LinuxServer'
    $parent = Split-Path $RepoRoot -Parent
    $cands = Get-ChildItem -LiteralPath $parent -Directory -ErrorAction SilentlyContinue |
        Where-Object { Test-Path -LiteralPath (Join-Path $_.FullName $rel) }
    if (-not $cands) { return $null }
    # 优先名字匹配 Pandora-Client* 的同级仓库，其次任意命中
    $pref = $cands | Where-Object { $_.Name -like 'Pandora-Client*' } | Select-Object -First 1
    $repo = if ($pref) { $pref } else { ($cands | Select-Object -First 1) }
    return (Join-Path $repo.FullName $rel)
}

# 若解析到客户端 Linux 包，就 robocopy /MIR 同步进 stage\LinuxServer（build 上下文内才能被 docker COPY）。
$srcPkg = Resolve-LinuxPkg
if ($srcPkg) {
    Write-Host "[build-image-minikube] 同步客户端 Linux DS 包 -> stage：$srcPkg" -ForegroundColor Cyan
    if (-not (Test-Path -LiteralPath $StageDir)) { New-Item -ItemType Directory -Path $StageDir -Force | Out-Null }
    & robocopy $srcPkg $StageDir /MIR /NFL /NDL /NJH /NJS /NP *> $null
    # robocopy 退出码 < 8 视为成功（0=无变化,1=有复制,3=复制+额外等）
    if ($LASTEXITCODE -ge 8) { throw "robocopy 同步 Linux DS 包失败（exit=$LASTEXITCODE）：$srcPkg" }
    $global:LASTEXITCODE = 0
} else {
    Write-Host "[build-image-minikube] 未发现同级客户端仓库的 Linux DS 包，沿用已暂存的 stage\LinuxServer" -ForegroundColor Yellow
}

if (-not (Test-Path $StageDir)) {
    throw "缺少 $StageDir，且未解析到客户端 Linux DS 包。请传 -SourcePkg，或先在客户端仓库跑 Tool/Server/Agones/build-linux-ds.ps1。"
}
if (-not (Test-Path $Dockerfile)) {
    throw "找不到 Dockerfile：$Dockerfile"
}
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "找不到 docker CLI；请确认 Docker 已安装并在 PATH 中。"
}

if ($BuildOnHost) {
    # 宿主构建路径：不切 docker-env、不校验 minikube。镜像落在宿主 docker daemon，
    # 由调用方 minikube image load 进集群。内网/断网靠宿主已有的 ubuntu + apt 缓存层。
    Write-Host "[build-image-minikube] 宿主构建模式（-BuildOnHost）：在宿主 docker daemon 构建，稍后由调用方 load 进 minikube。" -ForegroundColor Cyan
    # 关键：清掉从父进程/上一轮 `minikube docker-env` 继承来的 docker 环境变量，
    # 否则本会话的 docker CLI 仍连 minikube 内置 daemon，会“名为宿主构建、实为 minikube 构建”，
    # 落错 daemon 后调用方再 `minikube image load` 又找不到镜像。强制回落宿主 Docker Desktop。
    foreach ($ev in @('DOCKER_HOST', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH', 'MINIKUBE_ACTIVE_DOCKERD')) {
        if (Test-Path "env:$ev") {
            Write-Host "[build-image-minikube]   清除继承的 docker 环境变量 $ev（避免误连 minikube daemon）" -ForegroundColor DarkGray
            Remove-Item "env:$ev" -ErrorAction SilentlyContinue
        }
    }
    $dockerCtx = & docker info --format '{{.Name}}' 2>$null
    Write-Host "[build-image-minikube] 当前 docker daemon node = $dockerCtx" -ForegroundColor DarkGray
}
else {

if (-not (Get-Command minikube -ErrorAction SilentlyContinue)) {
    throw "找不到 minikube，可先跑 deploy/ds/install-minikube-windows.ps1。"
}

if ([string]::IsNullOrWhiteSpace($Profile)) {
    # 优先用当前 minikube profile；解析不到时 fallback 到 'pandora-agones'（本地 Agones 联调用 profile）。
    # 不要 fallback 到 'minikube'：旧默认 profile（192.168.49.x docker network）残留会让镜像落错 daemon，
    # 后续 relay/DS 走错网络，登录成功但 UDP 进不了 Hub DS。
    # 注意：`minikube profile` 首行可能是 `* pandora-agones`（带高亮前导星号），必须先剥掉 `^\*\s*`，
    # 否则 profile 名会含星号，后续 `minikube -p '* xxx'` 全部失败。
    $Profile = (((& minikube profile 2>$null | Select-Object -First 1) -replace '^\*\s*', '')).Trim()
    if ([string]::IsNullOrWhiteSpace($Profile)) {
        $Profile = 'pandora-agones'
        Write-Host "[build-image-minikube] 未指定 -Profile 且无法解析当前 minikube profile，fallback 到 '$Profile'" -ForegroundColor Yellow
    } else {
        Write-Host "[build-image-minikube] 未指定 -Profile，使用当前 minikube profile '$Profile'" -ForegroundColor DarkGray
    }
}
Write-Host "[build-image-minikube] 目标 minikube profile = '$Profile'" -ForegroundColor Cyan

# 校验 minikube profile 在跑（Running 才有可连的内置 daemon）。
$status = & minikube -p $Profile status --format '{{.Host}}' 2>$null
if ($LASTEXITCODE -ne 0 -or $status -notmatch 'Running') {
    throw "minikube profile '$Profile' 未在运行（status='$status'）。先跑 deploy/ds/start-minikube-agones.ps1。"
}

# 把当前 PowerShell 会话的 docker env 指到 minikube 内置 daemon。
# `minikube docker-env --shell powershell` 会输出一串 Set-Item env:... 语句，Invoke-Expression 应用即可。
Write-Host "[build-image-minikube] 切换 docker daemon -> minikube profile '$Profile'" -ForegroundColor Cyan
$envScript = & minikube -p $Profile docker-env --shell powershell
if ($LASTEXITCODE -ne 0) {
    throw "minikube docker-env 失败；确认 profile '$Profile' 用 docker driver 且在运行。"
}
$envScript | Invoke-Expression

# 确认现在连的是 minikube 里的 daemon（而不是宿主 Docker Desktop）。
$dockerCtx = & docker info --format '{{.Name}}' 2>$null
Write-Host "[build-image-minikube] 当前 docker daemon node = $dockerCtx" -ForegroundColor DarkGray

} # end else (非 -BuildOnHost：切到 minikube 内置 daemon)

function Build-One {
    param([string]$Name)
    $fullTag = "pandora/$Name-ds:$Tag"
    Write-Host "[build-image-minikube] docker build -t $fullTag --build-arg BASE_IMAGE=$BaseImage" -ForegroundColor Green
    & docker build --build-arg "BASE_IMAGE=$BaseImage" -f $Dockerfile -t $fullTag $ScriptDir
    if ($LASTEXITCODE -ne 0) {
        throw "docker build 失败：$fullTag"
    }
    $where = if ($BuildOnHost) { '宿主 docker daemon' } else { 'minikube' }
    Write-Host "[build-image-minikube] 完成（已落在 $where）：$fullTag" -ForegroundColor Green
}

# 推到制品 registry（发布线③制品库层，tools/devops 的 registry:2）。
# 只在宿主 daemon 模式下被调用（开头已 fail-closed 校验），localhost 才是宿主机。
function Push-One {
    param([string]$Name)
    $localTag = "pandora/$Name-ds:$Tag"
    $remoteTag = "$RegistryHost/pandora/$Name-ds:$Tag"

    Write-Host "[build-image-minikube] docker tag $localTag -> $remoteTag" -ForegroundColor Cyan
    & docker tag $localTag $remoteTag
    if ($LASTEXITCODE -ne 0) { throw "docker tag 失败：$localTag -> $remoteTag" }

    Write-Host "[build-image-minikube] docker push $remoteTag" -ForegroundColor Cyan
    & docker push $remoteTag
    if ($LASTEXITCODE -ne 0) {
        throw "docker push 失败：$remoteTag。确认 registry 在跑：pwsh XuanMing-Server/tools/devops/up.ps1"
    }

    # 发布 manifest 应记 digest 而非 tag：tag 可被覆盖，digest 不可变（见 docs/design/release-pipeline.md）。
    $digest = & docker image inspect $remoteTag --format '{{range .RepoDigests}}{{println .}}{{end}}' 2>$null |
        Where-Object { $_ -like "$RegistryHost/*" } | Select-Object -First 1
    if ($digest) {
        Write-Host "[build-image-minikube] 已推送（把这行 digest 记进发布 manifest）：$digest" -ForegroundColor Green
    } else {
        Write-Host "[build-image-minikube] 已推送：$remoteTag（未解析到 digest）" -ForegroundColor Yellow
    }
}

switch ($Image) {
    'battle' { Build-One 'battle'; if ($PushRegistry) { Push-One 'battle' } }
    'hub' { Build-One 'hub'; if ($PushRegistry) { Push-One 'hub' } }
    'both' {
        Build-One 'battle'; if ($PushRegistry) { Push-One 'battle' }
        Build-One 'hub'; if ($PushRegistry) { Push-One 'hub' }
    }
}

Write-Host ""
if ($BuildOnHost) {
    Write-Host "[build-image-minikube] 全部完成。镜像已在【宿主 docker daemon】；由调用方 minikube image load 进集群。" -ForegroundColor Green
} else {
    Write-Host "[build-image-minikube] 全部完成。镜像已在 minikube profile '$Profile' 内。" -ForegroundColor Green
    Write-Host "[build-image-minikube] 下一步：pwsh tools/scripts/e2e_k8s.ps1 -SkipImageLoad" -ForegroundColor Yellow
    Write-Host "[build-image-minikube] 注意：本会话的 docker env 已指向 minikube；要回宿主 Docker，重开终端或运行 minikube docker-env -u | Invoke-Expression。" -ForegroundColor DarkGray
}
