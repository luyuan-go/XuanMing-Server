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
    # compose 网络名与服务名；非默认部署时可覆盖
    [string]$Network = 'pandora-devops_default',
    [string]$Endpoint = 'http://minio:9000',
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
$envFile = Join-Path $PSScriptRoot '.env'
$envMap = @{}
if (Test-Path -LiteralPath $envFile) {
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
    throw "缺少 MinIO 凭据。请用 -AccessKey/-SecretKey，或设置 MINIO_ROOT_USER/MINIO_ROOT_PASSWORD，或在 $envFile 中提供。"
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
    'mirror', '--overwrite'
)
if ($DryRun) { $args += '--dry-run' }
$args += @("/artifacts/$relative", "pandora/$Bucket/$relative")

Info ("执行 mc mirror{0} ..." -f $(if ($DryRun) { '（DryRun）' } else { '' }))
& docker @args
if ($LASTEXITCODE -ne 0) { throw "mc mirror 失败（exit=$LASTEXITCODE）。检查 MinIO 是否在跑：docker compose -f docker-compose.stack.yml ps" }

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
