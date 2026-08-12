<#
.SYNOPSIS
  启停 Jenkins 专用的一次性 MySQL 8.4 + TiDB 8.5.1。

.DESCRIPTION
  Up 选择两个当前空闲的回环端口，以唯一 Compose project 启动无持久卷实例，等待 Docker
  health 后写状态 JSON。状态中的 DSN 不带库名，测试可安全创建随机库。

  Down 只接受由本脚本写出的状态文件，并校验 project 前缀后执行 compose down --volumes。
  它不触碰 docker-compose.dev.yml、pandora-mysql 或 pandora-tidb-*。
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('Up', 'Down')]
    [string]$Action,

    [Parameter(Mandatory)]
    [string]$StateFile,

    [string]$RunId = $env:BUILD_TAG,

    [ValidateRange(30, 900)]
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../..").Path
$ComposeFile = Join-Path $ProjectRoot 'deploy/docker-compose.ci-db.yml'

function Get-FreeLoopbackPort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    try {
        $listener.Start()
        return ([System.Net.IPEndPoint]$listener.LocalEndpoint).Port
    }
    finally {
        $listener.Stop()
    }
}

function ConvertTo-SafeProject([string]$Value) {
    $clean = (($Value ?? '') -replace '[^A-Za-z0-9_-]', '-').Trim('-').ToLowerInvariant()
    if ([string]::IsNullOrWhiteSpace($clean)) { $clean = [guid]::NewGuid().ToString('N') }
    if ($clean.Length -gt 36) { $clean = $clean.Substring($clean.Length - 36) }
    return "pandora-ci-db-$clean"
}

function Invoke-Compose([string[]]$Arguments, [string]$Project) {
    & docker compose --project-name $Project --file $ComposeFile @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose 失败(exit=$LASTEXITCODE): $($Arguments -join ' ')"
    }
}

function Read-State {
    if (-not (Test-Path -LiteralPath $StateFile)) { throw "CI DB 状态文件不存在：$StateFile" }
    $state = Get-Content -Raw -LiteralPath $StateFile | ConvertFrom-Json -ErrorAction Stop
    if ([string]::IsNullOrWhiteSpace([string]$state.project) -or
        -not ([string]$state.project).StartsWith('pandora-ci-db-', [System.StringComparison]::Ordinal)) {
        throw "拒绝清理非 CI DB project：$($state.project)"
    }
    return $state
}

if ($Action -eq 'Down') {
    if (-not (Test-Path -LiteralPath $StateFile)) {
        Write-Host "[SKIP] CI DB 状态文件不存在，无本轮资源可清理：$StateFile" -ForegroundColor DarkYellow
        exit 0
    }
    $state = Read-State
    $env:PANDORA_CI_MYSQL_PORT = [string]$state.mysql_port
    $env:PANDORA_CI_TIDB_PORT = [string]$state.tidb_port
    try {
        Invoke-Compose @('down', '--volumes', '--remove-orphans', '--timeout', '30') ([string]$state.project)
    }
    finally {
        Remove-Item -LiteralPath $StateFile -Force -ErrorAction SilentlyContinue
    }
    Write-Host "[ OK ] CI DB 已清理：$($state.project)" -ForegroundColor Green
    exit 0
}

if (Test-Path -LiteralPath $StateFile) {
    throw "CI DB 状态文件已存在，拒绝覆盖可能仍在运行的实例：$StateFile"
}
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw '找不到 docker。' }

$stateDir = Split-Path -Parent ([System.IO.Path]::GetFullPath($StateFile))
if (-not (Test-Path -LiteralPath $stateDir)) {
    New-Item -ItemType Directory -Path $stateDir -Force | Out-Null
}

$project = ConvertTo-SafeProject $RunId
$mysqlPort = Get-FreeLoopbackPort
do { $tidbPort = Get-FreeLoopbackPort } while ($tidbPort -eq $mysqlPort)
$env:PANDORA_CI_MYSQL_PORT = [string]$mysqlPort
$env:PANDORA_CI_TIDB_PORT = [string]$tidbPort

try {
    Invoke-Compose @('up', '-d', '--wait', '--wait-timeout', [string]$TimeoutSeconds) $project

    $state = [ordered]@{
        contract   = 'pandora-ci-db-v1'
        project    = $project
        compose    = $ComposeFile
        mysql_port = $mysqlPort
        tidb_port  = $tidbPort
        environment = [ordered]@{
            PANDORA_TEST_MYSQL_DSN = "root@tcp(127.0.0.1:$mysqlPort)/?parseTime=true&loc=UTC&charset=utf8mb4"
            PANDORA_TEST_TIDB_DSN  = "root@tcp(127.0.0.1:$tidbPort)/?parseTime=true&loc=UTC&charset=utf8mb4"
            # pkg/mysqlx 的探针必须连接初始化好的 pandora_account，故使用同一临时 TiDB
            # 的显式库 DSN；其它测试继续使用上面的无库名 DSN并自建随机库。
            PANDORA_TIDB_TEST_DSN  = "root@tcp(127.0.0.1:$tidbPort)/pandora_account?parseTime=true&loc=UTC&charset=utf8mb4"
        }
    }

    # 只有 pkg/mysqlx 的历史探针要求固定 pandora_account.accounts。用官方初始化 SQL
    # 建 schema；其它测试仍只拿无库名 DSN。mysql 客户端来自已 pin 的 mysql:8.4 镜像。
    $accountSql = Join-Path $ProjectRoot 'deploy/tidb-init/03-account-tidb.sql'
    Get-Content -Raw -LiteralPath $accountSql | & docker run --rm -i --network "${project}_ci-db" `
        mysql:8.4 mysql -h tidb -P 4000 -u root
    if ($LASTEXITCODE -ne 0) { throw '初始化 CI TiDB pandora_account schema 失败。' }

    $state | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $StateFile -Encoding utf8NoBOM
    Write-Host "[ OK ] CI DB 就绪：project=$project mysql=127.0.0.1:$mysqlPort tidb=127.0.0.1:$tidbPort" -ForegroundColor Green
    Write-Host "[INFO] 状态文件：$StateFile（DSN 值不输出）" -ForegroundColor Cyan
}
catch {
    try { Invoke-Compose @('down', '--volumes', '--remove-orphans', '--timeout', '30') $project }
    catch { Write-Warning "CI DB 失败后的清理也失败：$($_.Exception.Message)" }
    Remove-Item -LiteralPath $StateFile -Force -ErrorAction SilentlyContinue
    throw
}
