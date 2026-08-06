# build_release_binaries.ps1 — 把 21 个业务服务编成 Windows 预编译二进制,供【没装 Go 的机器】直接跑。
#
# 解决的问题:local 模式(宿主 go 进程 + docker 基础设施)原本每次启动都要 go build,
# 于是策划机必须装 Go 工具链 + 能联网拉模块。有了这份产物后:
#   * 策划机不装 Go 也能跑 local 模式(run_services.ps1 检测到没 Go 会自动拷这里的 exe);
#   * 启动从「分钟级编译」变成「秒级拷贝」;
#   * 升级只需替换 run/artifacts 这一个目录,不需要重装任何东西。
#
# 用法:
#   # 在装了 Go 的机器(后端同学的开发机 / CI)上生成产物
#   pwsh tools/scripts/build_release_binaries.ps1
#
#   # 顺便打成一个 zip,直接发给策划
#   pwsh tools/scripts/build_release_binaries.ps1 -Zip
#
# 产物落在 run/artifacts/windows/bin/*.exe(带 manifest.json 记录版本,便于对账)。
# 注意:这里只产 Windows 宿主二进制。Docker 镜像那条线仍走 export_images.ps1(离线镜像 tar),
# 两者互补 —— 业务进程免 Go,基础设施(MySQL/Redis/Kafka/etcd)仍由 Docker 提供。

[CmdletBinding()]
param(
    # 只编指定服务(连字符/下划线名,与 run_services.ps1 的 -Service 一致);留空 = 全部
    [string]$Service,
    # 编完打成 run/artifacts/pandora-server-bin-<yyyyMMdd-HHmmss>.zip
    [switch]$Zip
)

$ErrorActionPreference = 'Stop'

$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path "$ScriptDir/../..").Path
$ArtifactDir = Join-Path $ProjectRoot 'run/artifacts/windows/bin'

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    Write-Host "[ERR] 本机没装 Go,无法生成预编译产物。这个脚本只在【后端同学的开发机 / CI】上跑。" -ForegroundColor Red
    Write-Host "      装 Go: winget install GoLang.Go" -ForegroundColor Yellow
    exit 1
}

Write-Host "===== 生成 Windows 预编译产物 =====" -ForegroundColor Cyan
Write-Host "输出目录: $ArtifactDir" -ForegroundColor DarkGray

$buildArgs = @{ Action = 'build'; PublishArtifacts = $true }
if ($Service) { $buildArgs['Service'] = $Service }
& "$ScriptDir/run_services.ps1" @buildArgs
if ($LASTEXITCODE -ne 0) { Write-Host "[ERR] 构建失败" -ForegroundColor Red; exit 1 }

# 导表器也要给没装 Go 的机器带一份:策划改完 xlsx 要能自己导服务端表(策划一键导表.cmd),
# 否则每次改表都得等后端同学手动跑一趟,表漂移就是这么来的。
# 它是独立 module(tools/configtable-gen/go.mod),必须进目录单独 build,不能跟着 run_services 走。
# 只在整批构建时产出;-Service 是单服务定向构建,不该顺带重编工具。
if (-not $Service) {
    $genSrcDir = Join-Path $ProjectRoot 'tools/configtable-gen'
    $genExePath = Join-Path $ArtifactDir 'configtable-gen.exe'
    Write-Host "构建导表器 configtable-gen ..." -ForegroundColor DarkGray
    Push-Location $genSrcDir
    try {
        & go build -o $genExePath .
        $genExit = $LASTEXITCODE
    } finally {
        Pop-Location
    }
    if ($genExit -ne 0) { Write-Host "[ERR] configtable-gen 构建失败" -ForegroundColor Red; exit 1 }
    Write-Host "[ OK ] 导表器 -> $genExePath" -ForegroundColor Green
}

# manifest:记录这批二进制是哪个提交编出来的,升级/排查时能一眼对上版本。
$revision = try { (& git -C $ProjectRoot rev-parse --short HEAD 2>$null) } catch { $null }
$exes = @(Get-ChildItem -LiteralPath $ArtifactDir -Filter '*.exe' -ErrorAction SilentlyContinue)
$manifest = [ordered]@{
    built_at   = (Get-Date).ToString('s')
    built_on   = $env:COMPUTERNAME
    go_version = (& go version)
    revision   = if ($revision) { "$revision".Trim() } else { 'unknown' }
    binaries   = @($exes | ForEach-Object {
        [ordered]@{
            name   = $_.BaseName
            size   = $_.Length
            sha256 = (Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
        }
    })
}
$manifestPath = Join-Path (Split-Path -Parent $ArtifactDir) 'manifest.json'
$manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $manifestPath -Encoding UTF8

Write-Host "[ OK ] $($exes.Count) 个二进制 -> $ArtifactDir" -ForegroundColor Green
Write-Host "[ OK ] 清单 -> $manifestPath" -ForegroundColor Green

if ($Zip) {
    $zipPath = Join-Path $ProjectRoot ("run/artifacts/pandora-server-bin-{0}.zip" -f (Get-Date -Format 'yyyyMMdd-HHmmss'))
    if (Test-Path -LiteralPath $zipPath) { Remove-Item -LiteralPath $zipPath -Force }
    Compress-Archive -Path (Join-Path (Split-Path -Parent $ArtifactDir) '*') -DestinationPath $zipPath
    Write-Host "[ OK ] 分发包 -> $zipPath" -ForegroundColor Green
}

Write-Host ""
Write-Host "怎么用:把 run/artifacts 整个目录同步给策划(svn/git/共享盘都行),他们照常双击一键启动即可," -ForegroundColor Cyan
Write-Host "run_services.ps1 检测到本机没有 Go 会自动使用这里的二进制;以后升级只替换这个目录。" -ForegroundColor Cyan
