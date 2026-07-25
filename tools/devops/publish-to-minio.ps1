#!/usr/bin/env pwsh
<#
.SYNOPSIS
  把制品目录里已发布的产物同步到 MinIO，供团队下载。

.DESCRIPTION
  定位：MinIO 是**分发渠道**，本地制品目录仍是**权威制品库**。
  本脚本只做「把已发布的不可变版本目录复制上去」，不改变发布语义、不产生新版本。

  为什么用 MinIO 而不是别的：原子上传、内置校验、生命周期自动清理（桶上已配 30 天）、
  可签发限时下载链接（不用给对方账号）。

  实现上用官方 minio/mc 镜像跑，宿主机无需安装任何客户端；
  容器接进 compose 网络后以 http://minio:9000 直连，不经宿主端口。

  凭据解析顺序（都不写进仓库）：
    1. -AccessKey / -SecretKey 参数
    2. 环境变量 MINIO_ROOT_USER / MINIO_ROOT_PASSWORD（CI 用 Jenkins 凭据注入）
    3. 同目录 .env（本机开发用；该文件已被 .gitignore 排除）

.EXAMPLE
  # 同步某一次发布
  pwsh tools/devops/publish-to-minio.ps1 -SourceDir "F:\work\artifacts\snapshots\client\trunk_Client\Server_Linux_Development\r1489-dirty-20260725-102348"

  # 同步整条快照轨
  pwsh tools/devops/publish-to-minio.ps1 -SourceDir "F:\work\artifacts\snapshots"

  # 只看会传什么，不真传
  pwsh tools/devops/publish-to-minio.ps1 -SourceDir <路径> -DryRun
#>
[CmdletBinding()]
param(
    # 要上传的目录，必须位于制品根之下（用于推导对象前缀）
    [Parameter(Mandatory = $true)][string]$SourceDir,
    # 制品根；留空则用 PANDORA_ARTIFACT_ROOT，再退到 F:\work\artifacts（与 PublishPackages.ps1 一致）
    [string]$ArtifactRoot,
    [string]$Bucket,
    [string]$AccessKey,
    [string]$SecretKey,
    # 显式指定 .env 位置。CI 场景必需：脚本被检出到 Jenkins 工作区后，
    # $PSScriptRoot 指向工作区副本，而 .env 是机器本地文件（已 gitignore）不在其中。
    [string]$EnvFile,
    # compose 网络名与服务名；非默认部署时可覆盖
    [string]$Network = 'pandora-devops_default',
    [string]$Endpoint = 'http://minio:9000',
    # mirror 失败重试次数。宿主机负载高时 Docker 挂载会偶发短读，属瞬时故障；
    # mirror 幂等，重跑只补缺失对象，所以重试是安全的。
    [int]$MirrorRetries = 3,
    [switch]$DryRun
)

$ErrorActionPreference = 'Stop'

function Info($m) { Write-Host "[minio] $m" -ForegroundColor Cyan }
function Ok($m)   { Write-Host "[minio] $m" -ForegroundColor Green }
function Warn($m) { Write-Host "[minio] $m" -ForegroundColor Yellow }

# ── 前置 ──
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw '找不到 docker（本脚本用 minio/mc 容器执行，不需要宿主机装 mc）。' }
if (-not (Test-Path -LiteralPath $SourceDir)) { throw "SourceDir 不存在：$SourceDir" }
$SourceDir = (Resolve-Path -LiteralPath $SourceDir).Path

# ── 制品根与相对前缀 ──
if (-not $ArtifactRoot) {
    $ArtifactRoot = if ($env:PANDORA_ARTIFACT_ROOT) { $env:PANDORA_ARTIFACT_ROOT } else { 'F:\work\artifacts' }
}
if (-not (Test-Path -LiteralPath $ArtifactRoot)) { throw "制品根不存在：$ArtifactRoot（用 -ArtifactRoot 或 PANDORA_ARTIFACT_ROOT 指定）" }
$ArtifactRoot = (Resolve-Path -LiteralPath $ArtifactRoot).Path

if (-not $SourceDir.StartsWith($ArtifactRoot, [StringComparison]::OrdinalIgnoreCase)) {
    throw "SourceDir 必须位于制品根之下，否则无法推导对象路径。`n  SourceDir   = $SourceDir`n  ArtifactRoot= $ArtifactRoot"
}
# 对象前缀 = 相对制品根的路径，正斜杠
$relative = $SourceDir.Substring($ArtifactRoot.Length).TrimStart('\', '/') -replace '\\', '/'

# ── 配置解析（凭据不落盘、不进仓库）──
# .env 定位优先级。CI 与本机开发的差别在于：CI 里脚本是被检出到 Jenkins 工作区的副本，
# 同目录不会有 .env（它是机器本地文件、已 gitignore），必须回退到真正的后端仓库工作副本。
$envCandidates = @()
if ($EnvFile) { $envCandidates += $EnvFile }
$envCandidates += (Join-Path $PSScriptRoot '.env')
if ($env:PANDORA_DEVOPS_ENV) { $envCandidates += $env:PANDORA_DEVOPS_ENV }
# 节点上已配置 PANDORA_PROTO_SERVER_ROOT 指向后端仓库根，tools/devops 就在其下
if ($env:PANDORA_PROTO_SERVER_ROOT) { $envCandidates += (Join-Path $env:PANDORA_PROTO_SERVER_ROOT 'tools\devops\.env') }

$envFile = $envCandidates | Where-Object { $_ -and (Test-Path -LiteralPath $_) } | Select-Object -First 1
if ($envFile) { Info "读取配置：$envFile" }
$envMap = @{}
if ($envFile) {
    Get-Content -LiteralPath $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#=]+)=(.*)$') { $envMap[$Matches[1].Trim()] = $Matches[2].Trim() }
    }
}
function Resolve-Setting($param, $envName, $default) {
    if ($param) { return $param }
    if (Test-Path "env:$envName") { $v = (Get-Item "env:$envName").Value; if ($v) { return $v } }
    if ($envMap.ContainsKey($envName) -and $envMap[$envName]) { return $envMap[$envName] }
    return $default
}
$AccessKey = Resolve-Setting $AccessKey 'MINIO_ROOT_USER' $null
$SecretKey = Resolve-Setting $SecretKey 'MINIO_ROOT_PASSWORD' $null
$Bucket    = Resolve-Setting $Bucket    'MINIO_BUCKET' 'pandora-artifacts'
if (-not $AccessKey -or -not $SecretKey) {
    $tried = ($envCandidates | Where-Object { $_ }) -join "`n    "
    throw @"
缺少 MinIO 凭据。按以下任一方式提供（都不会进仓库）：
  1) 参数        -AccessKey / -SecretKey
  2) 环境变量    MINIO_ROOT_USER / MINIO_ROOT_PASSWORD（CI 可用 Jenkins 凭据注入）
  3) .env 文件   -EnvFile <路径>，或让下列任一路径存在

已尝试过的 .env 路径：
    $tried
"@
}

Info "源目录  : $SourceDir"
Info "制品根  : $ArtifactRoot"
Info "目标    : $Bucket/$relative"

# ── 执行 mc mirror（幂等：只传有差异的对象）──
# 凭据经 MC_HOST_<alias> 环境变量传入，不出现在命令行参数里（避免进程列表泄露）。
$mcHost = "http://{0}:{1}@{2}" -f $AccessKey, $SecretKey, ($Endpoint -replace '^https?://', '')
$mountSrc = $ArtifactRoot
$args = @(
    'run', '--rm',
    '--network', $Network,
    '-e', "MC_HOST_pandora=$mcHost",
    '-v', "${mountSrc}:/artifacts:ro",
    'minio/mc',
    # 不加 --overwrite：制品是不可变的，已存在的对象不该重传。
    # 更关键的是 --overwrite 会让每次重试都重传全部文件，重试永远无法收敛；
    # 去掉后 mirror 只补缺失/不一致的对象，失败重试才真正只处理失败的那部分。
    'mirror'
)
if ($DryRun) { $args += '--dry-run' }
$args += @("/artifacts/$relative", "pandora/$Bucket/$relative")

Info ("执行 mc mirror{0} ..." -f $(if ($DryRun) { '（DryRun）' } else { '' }))

# 带重试：宿主机高负载时，Docker 的 Windows 目录挂载偶发短读，
# mc 会报 "You did not provide the number of bytes specified by the Content-Length HTTP header"。
# 文件本身完好（本地校验一致），属传输层瞬时故障。mirror 幂等，重跑只补缺失的对象。
$maxAttempts = if ($DryRun) { 1 } else { $MirrorRetries }
$mirrorOk = $false
for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    if ($attempt -gt 1) {
        Warn "第 $attempt/$maxAttempts 次重试（mirror 幂等，只补前一次未完成的对象）..."
        Start-Sleep -Seconds ([Math]::Min(30, 5 * ($attempt - 1)))
    }
    & docker @args
    if ($LASTEXITCODE -eq 0) { $mirrorOk = $true; break }
}
if (-not $mirrorOk) {
    throw "mc mirror 连续 $maxAttempts 次失败（最后 exit=$LASTEXITCODE）。`n若报 Content-Length 不符，多为宿主机负载过高导致的挂载短读，可稍后重跑；`n若报连接失败，检查 MinIO：docker compose -f docker-compose.stack.yml ps"
}

if ($DryRun) { Ok 'DryRun 结束，未实际上传。'; return }

# ── 校验：对象数与本地文件数一致 ──
$localCount = (Get-ChildItem -LiteralPath $SourceDir -Recurse -File | Measure-Object).Count
$listArgs = @(
    'run', '--rm', '--network', $Network,
    '-e', "MC_HOST_pandora=$mcHost",
    'minio/mc', '--json', 'ls', '--recursive', "pandora/$Bucket/$relative"
)
$listed = & docker @listArgs 2>$null
$remoteCount = ($listed | Where-Object { $_ -match '"type"\s*:\s*"file"' } | Measure-Object).Count

Write-Host ''
if ($remoteCount -eq $localCount) {
    Ok ("上传完成并校验通过：{0} 个文件 -> {1}/{2}" -f $remoteCount, $Bucket, $relative)
} else {
    Warn ("对象数与本地不一致：本地 {0}，MinIO {1}。请复查上面的 mc 输出。" -f $localCount, $remoteCount)
}
Write-Host ''
Write-Host '查看 / 下载：' -ForegroundColor DarkGray
Write-Host ("  控制台  http://localhost:{0}  ->  桶 {1}" -f ($envMap['MINIO_CONSOLE_PORT'] ?? '9001'), $Bucket) -ForegroundColor DarkGray
Write-Host '  生成限时下载链接（无需给对方账号，默认 7 天）：' -ForegroundColor DarkGray
Write-Host ("  docker run --rm --network {0} -e MC_HOST_pandora=... minio/mc share download --expire 168h pandora/{1}/{2}" -f $Network, $Bucket, $relative) -ForegroundColor DarkGray
