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
    [ValidateRange(1, 10)][int]$MirrorRetries = 3,
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

$artifactPrefix = $ArtifactRoot.TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
if (-not $SourceDir.Equals($ArtifactRoot, [StringComparison]::OrdinalIgnoreCase) -and
    -not $SourceDir.StartsWith($artifactPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw "SourceDir 必须位于制品根之下，否则无法推导对象路径。`n  SourceDir   = $SourceDir`n  ArtifactRoot= $ArtifactRoot"
}
# 对象前缀 = 相对制品根的路径，正斜杠
$relative = [IO.Path]::GetRelativePath($ArtifactRoot, $SourceDir) -replace '\\', '/'
if ($relative -eq '.') { $relative = '' }

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

# ── 执行 mc mirror（不可变内容）+ mc cp（可变指针）──
# latest.json 是唯一允许变化的发布指针：版本目录先同步完，最后再原子覆盖指针。
# 不能把它混进“不覆盖”的 mirror，否则 mc 会报 Overwrite not allowed；更不能忽略
# mc 某些版本在打印 ERROR 后仍返回 0 的行为。
$endpointUri = [Uri]$Endpoint
if ($endpointUri.Scheme -notin @('http', 'https') -or
    $endpointUri.AbsolutePath -ne '/' -or
    $endpointUri.Query -or
    $endpointUri.Fragment) {
    throw "Endpoint 必须是无路径、query、fragment 的 http(s) 地址：$Endpoint"
}
$mcHost = "{0}://{1}:{2}@{3}" -f `
    $endpointUri.Scheme,
    [Uri]::EscapeDataString([string]$AccessKey),
    [Uri]::EscapeDataString([string]$SecretKey),
    $endpointUri.Authority
$mountSrc = $ArtifactRoot
$mcHostEnvName = 'MC_HOST_pandora'
$hadPreviousMcHost = Test-Path "env:$mcHostEnvName"
$previousMcHost = if ($hadPreviousMcHost) { (Get-Item "env:$mcHostEnvName").Value } else { $null }
Set-Item "env:$mcHostEnvName" $mcHost
$pointerSnapshotRoot = $null

try {
    # 只把变量名交给 docker；密码留在宿主进程环境，不进入 docker 命令行/进程列表。
    $dockerBaseArgs = @(
        'run', '--rm',
        '--network', $Network,
        '-e', $mcHostEnvName,
        '-v', "${mountSrc}:/artifacts:ro"
    )

function Invoke-McAttempt {
    param(
        [Parameter(Mandatory = $true)][string[]]$DockerArgs,
        [switch]$Quiet
    )

    $output = @(& docker @DockerArgs 2>&1)
    $nativeExit = $LASTEXITCODE
    $reportedError = $false
    foreach ($line in $output) {
        $text = [string]$line
        if (-not $Quiet) { Write-Host $text }
        if ($text -match '(?i)<ERROR>|\"status\"\s*:\s*\"error\"') {
            $reportedError = $true
            continue
        }
        try {
            $record = $text | ConvertFrom-Json -ErrorAction Stop
            if ($record.status -eq 'error') { $reportedError = $true }
        } catch {
            # 进度行和 mc cat 的正文不是 JSON；只有显式 ERROR 才按错误处理。
        }
    }

    [PSCustomObject]@{
        Success = ($nativeExit -eq 0 -and -not $reportedError)
        ExitCode = $nativeExit
        ReportedError = $reportedError
        Output = $output
    }
}

function Invoke-McWithRetry {
    param(
        [Parameter(Mandatory = $true)][string]$Label,
        [Parameter(Mandatory = $true)][string[]]$DockerArgs,
        [int]$Attempts = $MirrorRetries,
        [switch]$Quiet
    )

    $last = $null
    for ($attempt = 1; $attempt -le $Attempts; $attempt++) {
        if ($attempt -gt 1) {
            Warn "$Label 第 $attempt/$Attempts 次重试..."
            Start-Sleep -Seconds ([Math]::Min(30, 5 * ($attempt - 1)))
        }
        $last = Invoke-McAttempt -DockerArgs $DockerArgs -Quiet:$Quiet
        if ($last.Success) { return $last }
    }

    if ($Quiet -and $last.Output) {
        $last.Output | Select-Object -Last 20 | ForEach-Object { Write-Host ([string]$_) }
    }
    throw "$Label 连续 $Attempts 次失败（native exit=$($last.ExitCode), mc reported error=$($last.ReportedError)）。"
}

$sourceObjectRoot = if ($relative) { "/artifacts/$relative" } else { '/artifacts' }
$targetObjectRoot = if ($relative) { "pandora/$Bucket/$relative" } else { "pandora/$Bucket" }
$localFiles = @(Get-ChildItem -LiteralPath $SourceDir -Recurse -File)
if ($localFiles.Count -eq 0) {
    throw "SourceDir 为空，拒绝制造 0 文件的同步成功：$SourceDir"
}
$pointers = @($localFiles | Where-Object { $_.Name -ieq 'latest.json' } | ForEach-Object {
    [PSCustomObject]@{
        File = $_
        RelativeKey = ([IO.Path]::GetRelativePath($SourceDir, $_.FullName) -replace '\\', '/')
    }
})
$immutableCount = $localFiles.Count - $pointers.Count
$pointerByKey = [Collections.Generic.Dictionary[string, object]]::new([StringComparer]::Ordinal)

if ($pointers.Count -gt 0) {
    # 发布者可能在 mirror 期间原子替换 latest.json。先冻结本轮指针，再从冻结副本 cp，
    # 避免 mirror 已扫过目录后却把远端切到一个本轮没有上传的新版本（TOCTOU）。
    $snapshotParent = [IO.Path]::GetFullPath((Split-Path -Parent $ArtifactRoot))
    $pointerSnapshotRoot = [IO.Path]::GetFullPath((Join-Path $snapshotParent ('.pandora-minio-pointers-{0}' -f [guid]::NewGuid().ToString('N'))))
    if ([IO.Path]::GetDirectoryName($pointerSnapshotRoot) -ne $snapshotParent) {
        throw "不安全的指针快照路径：$pointerSnapshotRoot"
    }
    New-Item -ItemType Directory -Path $pointerSnapshotRoot | Out-Null

    for ($index = 0; $index -lt $pointers.Count; $index++) {
        $pointer = $pointers[$index]
        $snapshotName = ('{0:D4}-latest.json' -f $index)
        $snapshotPath = Join-Path $pointerSnapshotRoot $snapshotName
        $snapshotBytes = [IO.File]::ReadAllBytes($pointer.File.FullName)
        [IO.File]::WriteAllBytes($snapshotPath, $snapshotBytes)
        $pointer | Add-Member -NotePropertyName SnapshotPath -NotePropertyValue $snapshotPath
        $pointer | Add-Member -NotePropertyName SnapshotLength -NotePropertyValue ([int64]$snapshotBytes.Length)
        $pointer | Add-Member -NotePropertyName SnapshotContainerPath -NotePropertyValue "/pointer-snapshots/$snapshotName"
        $pointerByKey[$pointer.RelativeKey] = $pointer
    }
    $dockerBaseArgs += @('-v', "${pointerSnapshotRoot}:/pointer-snapshots:ro")
}

if ($immutableCount -gt 0) {
    $mirrorArgs = $dockerBaseArgs + @('minio/mc', '--json', 'mirror')
    foreach ($pointer in $pointers) { $mirrorArgs += @('--exclude', $pointer.RelativeKey) }
    if ($DryRun) { $mirrorArgs += '--dry-run' }
    $mirrorArgs += @($sourceObjectRoot, $targetObjectRoot)

    Info ("执行 mc mirror{0}（不可变文件 {1} 个，排除指针 {2} 个）..." -f $(if ($DryRun) { ' DryRun' } else { '' }), $immutableCount, $pointers.Count)
    $attempts = if ($DryRun) { 1 } else { $MirrorRetries }
    $null = Invoke-McWithRetry -Label 'mc mirror' -DockerArgs $mirrorArgs -Attempts $attempts
} else {
    Info '没有不可变文件需要 mirror。'
}

foreach ($pointer in $pointers) {
    $pointerSource = $pointer.SnapshotContainerPath
    $pointerTarget = "$targetObjectRoot/$($pointer.RelativeKey)"
    if ($DryRun) {
        Info "DryRun：mirror 成功后将覆盖指针 $pointerTarget"
        continue
    }
    Info "更新发布指针（内容之后、最后切换）：$pointerTarget"
    $copyArgs = $dockerBaseArgs + @('minio/mc', '--json', 'cp', $pointerSource, $pointerTarget)
    $null = Invoke-McWithRetry -Label "mc cp $($pointer.RelativeKey)" -DockerArgs $copyArgs
}

if ($DryRun) { Ok 'DryRun 结束，未实际上传。'; return }

# ── 校验：本地每个对象都必须在远端且大小一致；允许远端保留更多历史版本 ──
$listArgs = $dockerBaseArgs + @('minio/mc', '--json', 'ls', '--recursive', $targetObjectRoot)
$listResult = Invoke-McWithRetry -Label 'mc ls verification' -DockerArgs $listArgs -Quiet
$remoteFiles = [Collections.Generic.Dictionary[string, int64]]::new([StringComparer]::Ordinal)
foreach ($line in $listResult.Output) {
    try {
        $record = ([string]$line) | ConvertFrom-Json -ErrorAction Stop
        if ($record.status -eq 'success' -and $record.type -eq 'file' -and $record.key) {
            $remoteFiles[[string]$record.key] = [int64]$record.size
        }
    } catch {
        # Invoke-McAttempt 已处理显式错误；忽略非 JSON 进度行。
    }
}

$verificationErrors = @()
foreach ($file in $localFiles) {
    $key = [IO.Path]::GetRelativePath($SourceDir, $file.FullName) -replace '\\', '/'
    $expectedLength = if ($pointerByKey.ContainsKey($key)) { $pointerByKey[$key].SnapshotLength } else { $file.Length }
    if (-not $remoteFiles.ContainsKey($key)) {
        $verificationErrors += "远端缺少：$key"
    } elseif ($remoteFiles[$key] -ne $expectedLength) {
        $verificationErrors += "大小不一致：$key（local=$expectedLength, remote=$($remoteFiles[$key])）"
    }
}

foreach ($pointer in $pointers) {
    $pointerTarget = "$targetObjectRoot/$($pointer.RelativeKey)"
    $catArgs = $dockerBaseArgs + @('minio/mc', 'cat', $pointerTarget)
    $catResult = Invoke-McWithRetry -Label "mc cat $($pointer.RelativeKey)" -DockerArgs $catArgs -Quiet
    $remoteText = (@($catResult.Output) | ForEach-Object { [string]$_ }) -join "`n"
    $localText = [IO.File]::ReadAllText($pointer.SnapshotPath) -replace "`r`n", "`n"
    if ($localText.EndsWith("`n")) { $remoteText += "`n" }
    if ($remoteText -cne $localText) {
        $verificationErrors += "发布指针内容不一致：$($pointer.RelativeKey)"
    }
}

if ($verificationErrors.Count -gt 0) {
    throw "MinIO 校验失败：`n  $($verificationErrors -join "`n  ")"
}

Write-Host ''
Ok ("上传完成并校验通过：本地 {0} 个文件全部存在且大小一致（远端前缀共 {1} 个对象，允许历史保留） -> {2}/{3}" -f $localFiles.Count, $remoteFiles.Count, $Bucket, $relative)
Write-Host ''
Write-Host '查看 / 下载：' -ForegroundColor DarkGray
Write-Host ("  控制台  http://localhost:{0}  ->  桶 {1}" -f ($envMap['MINIO_CONSOLE_PORT'] ?? '9001'), $Bucket) -ForegroundColor DarkGray
Write-Host '  生成限时下载链接（无需给对方账号，默认 7 天）：' -ForegroundColor DarkGray
Write-Host ("  docker run --rm --network {0} -e MC_HOST_pandora=... minio/mc share download --expire 168h pandora/{1}/{2}" -f $Network, $Bucket, $relative) -ForegroundColor DarkGray
} finally {
    try {
        if ($pointerSnapshotRoot -and (Test-Path -LiteralPath $pointerSnapshotRoot)) {
            $snapshotParent = [IO.Path]::GetFullPath((Split-Path -Parent $ArtifactRoot))
            if ([IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($pointerSnapshotRoot)) -ne $snapshotParent) {
                throw "拒绝清理不安全的指针快照路径：$pointerSnapshotRoot"
            }
            Remove-Item -LiteralPath $pointerSnapshotRoot -Recurse -Force
        }
    } finally {
        if ($hadPreviousMcHost) {
            Set-Item "env:$mcHostEnvName" $previousMcHost
        } else {
            Remove-Item "env:$mcHostEnvName" -ErrorAction SilentlyContinue
        }
    }
}
