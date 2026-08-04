# Pandora 业务服务一键启停 / 单服务调试
#
# 大厂本地多服务开发的"进程编排"层(等价 Procfile / goreman / tilt,但零额外依赖)。
# 基础设施(MySQL/Redis/Kafka/etcd/Envoy)由 dev_up.ps1 负责,本脚本只管 Go 业务服务。
#
# 用法(只有两种启动方式:全起 或 单起某一个,不做分档启动):
#   # 起全部业务服务(默认)
#   pwsh tools/scripts/run_services.ps1
#
#   # 只起单个服务
#   pwsh tools/scripts/run_services.ps1 -Service team
#
#   # 全起但排除某个服务(team 留给 VS Code 断点调试)
#   pwsh tools/scripts/run_services.ps1 -Exclude team
#
#   # 查看状态 / 看日志 / 重启单个 / 全停
#   pwsh tools/scripts/run_services.ps1 -Action status
#   pwsh tools/scripts/run_services.ps1 -Action logs    -Service team
#   pwsh tools/scripts/run_services.ps1 -Action restart -Service team
#   pwsh tools/scripts/run_services.ps1 -Action down
#
#   # 单个服务前台运行(快速看完整日志,不进 IDE;Ctrl+C 结束)
#   pwsh tools/scripts/run_services.ps1 -Service team -Foreground

[CmdletBinding()]
param(
    [ValidateSet('up', 'down', 'status', 'logs', 'restart', 'build')]
    [string]$Action = 'up',

    # 全起时排除的服务(留给 IDE 调试);也可配合 restart/logs/foreground 指定单个服务
    [string[]]$Exclude = @(),

    # 只启动指定的几个服务(含战斗混合模式只在宿主起 ds_allocator/hub_allocator)。
    # 与 -Exclude 互补:先排除再取交集。按 $Services 数组顺序保留依赖启动先后。
    [string[]]$Only = @(),

    # 指定单个服务(logs / restart / -Foreground 时使用)
    [string]$Service,

    # 单服务前台运行(阻塞,Ctrl+C 退出),方便直接看日志
    [switch]$Foreground,

    # 跳过 go build(进程已是最新二进制时加速)
    [switch]$NoBuild,

    # 强制用预编译产物(run/artifacts/windows/bin)而不是现场 go build。
    # 没装 Go 的机器(策划机)会自动走这条路,不必显式传。
    [switch]$UseArtifacts,

    # 配合 -Action build:把产物编到 run/artifacts/windows/bin(而不是 run/dev/bin),
    # 供打包分发给没装 Go 的机器。
    [switch]$PublishArtifacts
)

$ErrorActionPreference = 'Stop'

# 国内网络拉 Go 依赖:默认 proxy.golang.org / sum.golang.org 在国内基本连不上,
# 会导致 go build 拉模块超时(dial tcp 142.251.188.141:443 ... connectex: 超时)。
# 这里在脚本进程内兜底切到 goproxy.cn(不改机器全局 go env,便于一键脚本分发到多台策划机)。
# 已显式自定义 GOPROXY(且不是默认公有代理)的机器保持不动,尊重企业内网配置。
if (-not $env:GOPROXY -or $env:GOPROXY -match 'proxy\.golang\.org') {
    $env:GOPROXY = 'https://goproxy.cn,direct'
}
if (-not $env:GOSUMDB -or $env:GOSUMDB -match 'sum\.golang\.org') {
    $env:GOSUMDB = 'sum.golang.google.cn'
}

$ProjectRoot = (Resolve-Path "$PSScriptRoot/../..").Path
$RunDir = Join-Path $ProjectRoot 'run/dev'
$BinDir = Join-Path $RunDir 'bin'
$LogDir = Join-Path $RunDir 'logs'
New-Item -ItemType Directory -Force -Path $BinDir, $LogDir | Out-Null

# 预编译产物目录:由 tools/scripts/build_release_binaries.ps1 在装了 Go 的机器上生成并随目录分发。
# 没装 Go 的机器(策划机)启动时直接拷这里的 exe,免装 Go 工具链、免联网拉模块。
$ArtifactBinDir = Join-Path $ProjectRoot 'run/artifacts/windows/bin'
$script:HasGo = [bool](Get-Command go -ErrorAction SilentlyContinue)

# ===== 服务清单(数组顺序 = 依赖启动顺序:leaf 依赖在前,login 最后)=====
# 全部 21 个服务(含 social/friend、social/chat、social/guild、social/mail、social/dialogue、
# data/data_service、economy/trade、economy/inventory、economy/auction、runtime/leaderboard 等)。
# 启动策略:要么全起(默认),要么用 -Service 单起某一个,不做分档启动。
$Services = @(
    @{ Name = 'player_locator'; Dir = 'services/runtime/player_locator';   Cmd = 'locator';        Conf = 'etc/locator-dev.yaml';        Port = 50006 }
    @{ Name = 'hub_allocator';  Dir = 'services/battle/hub_allocator';      Cmd = 'hub_allocator';  Conf = 'etc/hub_allocator-dev.yaml';  Port = 50021 }
    @{ Name = 'player';         Dir = 'services/account/player';            Cmd = 'player';         Conf = 'etc/player-dev.yaml';         Port = 50002 }
    @{ Name = 'ds_allocator';   Dir = 'services/battle/ds_allocator';       Cmd = 'ds_allocator';   Conf = 'etc/ds_allocator-dev.yaml';   Port = 50020 }
    @{ Name = 'push';           Dir = 'services/runtime/push';              Cmd = 'push';           Conf = 'etc/push-dev.yaml';           Port = 50014 }
    @{ Name = 'team';           Dir = 'services/matchmaking/team';          Cmd = 'team';           Conf = 'etc/team-dev.yaml';           Port = 50010 }
    @{ Name = 'friend';         Dir = 'services/social/friend';             Cmd = 'friend';         Conf = 'etc/friend-dev-tidb.yaml';    Port = 50004 }
    @{ Name = 'chat';           Dir = 'services/social/chat';               Cmd = 'chat';           Conf = 'etc/chat-dev-tidb.yaml';      Port = 50005 }
    @{ Name = 'guild';          Dir = 'services/social/guild';              Cmd = 'guild';          Conf = 'etc/guild-dev-tidb.yaml';     Port = 50008 }
    @{ Name = 'mail';           Dir = 'services/social/mail';               Cmd = 'mail';           Conf = 'etc/mail-dev-tidb.yaml';      Port = 50009 }
    @{ Name = 'dialogue';       Dir = 'services/social/dialogue';           Cmd = 'dialogue';       Conf = 'etc/dialogue-dev.yaml';       Port = 50013 }
    @{ Name = 'data_service';   Dir = 'services/data/data_service';         Cmd = 'data_service';   Conf = 'etc/data_service-dev.yaml';   Port = 50003 }
    @{ Name = 'trade';          Dir = 'services/economy/trade';             Cmd = 'trade';          Conf = 'etc/trade-dev.yaml';          Port = 50012 }
    @{ Name = 'inventory';      Dir = 'services/economy/inventory';         Cmd = 'inventory';      Conf = 'etc/inventory-dev.yaml';      Port = 50015 }
    @{ Name = 'leaderboard';    Dir = 'services/runtime/leaderboard';       Cmd = 'leaderboard';    Conf = 'etc/leaderboard-dev.yaml';    Port = 50007 }
    @{ Name = 'owner';          Dir = 'services/runtime/owner';             Cmd = 'owner';          Conf = 'etc/owner-dev.yaml';          Port = 50017 }
    @{ Name = 'auction';        Dir = 'services/economy/auction';           Cmd = 'auction';        Conf = 'etc/auction-dev.yaml';        Port = 50016 }
    @{ Name = 'battle_result';  Dir = 'services/battle/battle_result';      Cmd = 'battle_result';  Conf = 'etc/battle_result-dev.yaml';  Port = 50022 }
    @{ Name = 'matchmaker';     Dir = 'services/matchmaking/matchmaker';    Cmd = 'matchmaker';     Conf = 'etc/matchmaker-dev.yaml';     Port = 50011 }
    # PVE 匹配实例:同一 matchmaker 二进制、不同配置(game_mode=pve_coop + walk_in=true,
    # 单人/整队直进副本,副本由 StartMatchRequest.map_id 选)。Envoy 按 header x-pandora-game-mode: pve 分流。
    @{ Name = 'matchmaker_pve'; Dir = 'services/matchmaking/matchmaker';    Cmd = 'matchmaker';     Conf = 'etc/matchmaker-pve.yaml';     Port = 50018 }
    @{ Name = 'login';          Dir = 'services/account/login';             Cmd = 'login';          Conf = 'etc/login-dev.yaml';          Port = 50001 }
)

function Get-Service([string]$name) {
    $svc = $Services | Where-Object { $_.Name -eq $name }
    if (-not $svc) {
        Write-Host "[ERR] 未知服务: $name" -ForegroundColor Red
        Write-Host "可用服务: $(( $Services | ForEach-Object { $_.Name }) -join ', ')" -ForegroundColor Yellow
        exit 1
    }
    return $svc
}

function Get-TargetServices {
    # 全起策略:默认全部服务,仅剔除 -Exclude 指定的(留给 IDE 断点调试)
    $list = $Services | Where-Object { $Exclude -notcontains $_.Name }
    # -Only 非空时只保留列表内服务(仍按 $Services 数组顺序 = 依赖启动顺序)
    if ($Only.Count -gt 0) { $list = $list | Where-Object { $Only -contains $_.Name } }
    return $list
}

function Get-PidFile($svc) { Join-Path $LogDir "$($svc.Name).pid" }
function Get-LogFile($svc) { Join-Path $LogDir "$($svc.Name).log" }
function Get-ErrFile($svc) { Join-Path $LogDir "$($svc.Name).err.log" }

function Get-RunningProcess($svc) {
    $pidFile = Get-PidFile $svc
    if (-not (Test-Path $pidFile)) { return $null }
    $svcPid = (Get-Content $pidFile -Raw).Trim()
    if (-not $svcPid) { return $null }
    $proc = Get-Process -Id $svcPid -ErrorAction SilentlyContinue
    if (-not $proc) {
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
        return $null
    }

    $expectedExe = Join-Path $BinDir "$($svc.Name).exe"
    $actualExe = $null
    try { $actualExe = $proc.Path } catch { $actualExe = $null }
    if ($actualExe -and ([System.IO.Path]::GetFullPath($actualExe) -ne [System.IO.Path]::GetFullPath($expectedExe))) {
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
        return $null
    }

    if (-not $actualExe -and $proc.ProcessName -ne $svc.Name) {
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
        return $null
    }

    return $proc
}

function Test-PortOpen([int]$port) {
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $conn = $client.BeginConnect('127.0.0.1', $port, $null, $null)
        $ok = $conn.AsyncWaitHandle.WaitOne(400, $false)
        if ($ok) { $client.EndConnect($conn) }
        return $ok
    } catch {
        return $false
    } finally {
        $client.Close()
    }
}

# 清理"占着本服务 gRPC 端口的残留进程"。
# 背景:上一轮启动没干净退出(或 pidfile 丢了),旧实例还占着端口 → 新实例 app.Run() 直接
# `listen tcp :5002x: bind: Only one usage of each socket address` 崩溃退出;进程是隐藏窗口,
# 用户只看到"某服务没起来"却查不到原因。这里在启动前把占端口的进程揪出来:
#   - 若它就是本服务自己的 exe(BinDir 下同名二进制)→ 视为残留实例,直接 Kill;
#   - 若是别的程序占了端口 → 只告警不误杀,交由用户处理。
function Clear-PortSquatter($svc) {
    $owningPids = @()
    try {
        $owningPids = Get-NetTCPConnection -State Listen -LocalPort $svc.Port -ErrorAction SilentlyContinue |
            Select-Object -ExpandProperty OwningProcess -Unique
    } catch { $owningPids = @() }
    if (-not $owningPids) { return }

    $expectedExe = [System.IO.Path]::GetFullPath((Join-Path $BinDir "$($svc.Name).exe"))
    foreach ($opid in $owningPids) {
        if (-not $opid -or $opid -eq 0) { continue }
        $proc = Get-Process -Id $opid -ErrorAction SilentlyContinue
        if (-not $proc) { continue }
        $procPath = $null
        try { $procPath = $proc.Path } catch { $procPath = $null }
        $isOurs = $false
        if ($procPath) {
            $isOurs = ([System.IO.Path]::GetFullPath($procPath) -eq $expectedExe)
        } elseif ($proc.ProcessName -eq $svc.Name) {
            # 部分系统进程路径读不到;只在路径不可见时退回按进程名判断。
            $isOurs = $true
        }
        if ($isOurs) {
            Write-Host "  [kill] $($svc.Name) 端口 :$($svc.Port) 被残留实例 (PID $opid) 占用,先清理" -ForegroundColor Yellow
            Stop-Process -Id $opid -Force -ErrorAction SilentlyContinue
            # 等端口真正释放,避免紧接着的 Start 仍撞 bind
            for ($w = 0; $w -lt 20; $w++) {
                if (-not (Test-PortOpen $svc.Port)) { break }
                Start-Sleep -Milliseconds 200
            }
        } else {
            # 端口被非本服务进程占。最常见是 docker 业务容器(经 wslrelay/com.docker.backend 代理端口):
            # 上一轮跑过 docker/intranet 模式,容器还占着 50001-50022,宿主 go 进程会 bind 失败。
            $isDockerProxy = $proc.ProcessName -match 'wslrelay|com\.docker'
            if ($isDockerProxy) {
                Write-Host "  [WARN] $($svc.Name) 端口 :$($svc.Port) 被 docker 容器占用($($proc.ProcessName));宿主进程起不来。" -ForegroundColor Yellow
                Write-Host "         先停 docker 业务容器:docker compose -f deploy/docker-compose.services.yml down" -ForegroundColor Yellow
            } else {
                Write-Host "  [WARN] $($svc.Name) 端口 :$($svc.Port) 被非本服务进程占用 (PID $opid $($proc.ProcessName)),$($svc.Name) 可能起不来" -ForegroundColor Yellow
            }
        }
    }
}

# 业务服务启动前的基础设施预检:Redis / MySQL / Kafka / etcd 都是 go 服务的强依赖,
# 任一不通,服务起来也只会在日志里刷 "dial tcp 127.0.0.1:xxxx: connectex: ... actively refused"
# 无限重连(进程不退,端口探活还可能"假就绪"),表现成"服务起了但客户端连不上/进不了大厅"。
# 这里在拉起业务服务前先探基础设施端口,不通就直接拦下并给出明确修复指引,避免静默 crash-loop。
function Test-InfraReady {
    $infra = @(
        @{ Name = 'Redis';  Port = 6380 }
        @{ Name = 'MySQL';  Port = 3307 }
        @{ Name = 'Kafka';  Port = 9093 }
        @{ Name = 'etcd';   Port = 2380 }
    )
    $down = @()
    foreach ($i in $infra) {
        if (-not (Test-PortOpen $i.Port)) { $down += $i }
    }
    if ($down.Count -eq 0) { return $true }

    Write-Host "[ERR] 基础设施未就绪,业务服务无法启动:" -ForegroundColor Red
    foreach ($d in $down) {
        Write-Host "  - $($d.Name) 127.0.0.1:$($d.Port) 连不上(容器没起/端口没发布)" -ForegroundColor Red
    }
    Write-Host "原因:这些是 go 服务的强依赖;Redis 不通时 hub_allocator 也拉不起大厅 Hub DS,客户端会卡在连大厅。" -ForegroundColor Yellow
    Write-Host "修复:" -ForegroundColor Yellow
    Write-Host "  1) 确认 Docker Desktop 已启动(右下角鲸鱼图标变绿);" -ForegroundColor Yellow
    Write-Host "  2) 起基础设施:   pwsh tools/scripts/dev_up.ps1" -ForegroundColor Yellow
    Write-Host "  3) 查容器状态:   docker compose -f deploy/docker-compose.dev.yml ps" -ForegroundColor Yellow
    Write-Host "     Redis 应显示 healthy 且端口映射 0.0.0.0:6380->6379。" -ForegroundColor Yellow
    return $false
}

# Build-Service:产出可执行文件路径。
#
# 两条路,按「本机有没有 Go」自动选,不需要调用方关心:
#   有 Go(开发机)     → 现场 go build,保持原有行为不变(改代码即时生效)。
#   没 Go(策划机)     → 直接拷 run/artifacts/windows/bin 下的预编译产物。
# -UseArtifacts 可在有 Go 的机器上强制走预编译(验证分发包用);两条路都不通时给出
# 可执行的修复指引,而不是抛一个 'go 不是内部命令' 让策划自己猜。
function Build-Service($svc) {
    $outDir = if ($PublishArtifacts) { $ArtifactBinDir } else { $BinDir }
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null
    $exe = Join-Path $outDir "$($svc.Name).exe"

    if (($UseArtifacts -or -not $script:HasGo) -and -not $PublishArtifacts) {
        $artifact = Join-Path $ArtifactBinDir "$($svc.Name).exe"
        if (Test-Path -LiteralPath $artifact) {
            Write-Host "  [use ] $($svc.Name) (预编译产物)" -ForegroundColor DarkGray
            Copy-Item -LiteralPath $artifact -Destination $exe -Force
            return $exe
        }
        if (-not $script:HasGo) {
            Write-Host "[ERR] 本机没装 Go,也没找到预编译产物: $artifact" -ForegroundColor Red
            Write-Host "      修复(二选一):" -ForegroundColor Yellow
            Write-Host "        a) 找后端同学在装了 Go 的机器上跑 pwsh tools/scripts/build_release_binaries.ps1," -ForegroundColor Yellow
            Write-Host "           把 run/artifacts/ 整个目录一起发过来(以后升级也只需替换这个目录);" -ForegroundColor Yellow
            Write-Host "        b) 本机装 Go: winget install GoLang.Go" -ForegroundColor Yellow
            exit 1
        }
    }

    $svcDir = Join-Path $ProjectRoot $svc.Dir
    Write-Host "  [build] $($svc.Name) ..." -ForegroundColor DarkGray
    Push-Location $svcDir
    try {
        & go build -o $exe "./cmd/$($svc.Cmd)"
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERR] build 失败: $($svc.Name)" -ForegroundColor Red
            exit 1
        }
    } finally {
        Pop-Location
    }
    return $exe
}

function Start-Service($svc) {
    $existing = Get-RunningProcess $svc
    if ($existing) {
        Write-Host "  [skip] $($svc.Name) 已在运行 (PID $($existing.Id))" -ForegroundColor Yellow
        return
    }

    # 启动前清理占端口的残留实例,避免新进程 bind 端口失败静默崩溃。
    Clear-PortSquatter $svc

    $exe = Join-Path $BinDir "$($svc.Name).exe"
    if (-not $NoBuild -or -not (Test-Path $exe)) {
        $exe = Build-Service $svc
    }

    $svcDir = Join-Path $ProjectRoot $svc.Dir
    $log = Get-LogFile $svc
    $err = Get-ErrFile $svc

    $proc = Start-Process -FilePath $exe `
        -ArgumentList '-conf', $svc.Conf `
        -WorkingDirectory $svcDir `
        -RedirectStandardOutput $log `
        -RedirectStandardError $err `
        -WindowStyle Hidden `
        -PassThru

    $proc.Id | Out-File -FilePath (Get-PidFile $svc) -Encoding ascii

    # 端口探活
    $ready = $false
    for ($i = 0; $i -lt 30; $i++) {
        if ($proc.HasExited) { break }
        if (Test-PortOpen $svc.Port) { $ready = $true; break }
        Start-Sleep -Milliseconds 400
    }

    if ($proc.HasExited) {
        Write-Host "  [FAIL] $($svc.Name) 启动后立即退出 (exit $($proc.ExitCode)),看日志: $err" -ForegroundColor Red
    } elseif ($ready) {
        Write-Host "  [ OK ] $($svc.Name)  PID $($proc.Id)  :$($svc.Port)" -ForegroundColor Green
    } else {
        Write-Host "  [WARN] $($svc.Name) PID $($proc.Id) 已起但 :$($svc.Port) 未就绪,看日志: $log" -ForegroundColor Yellow
    }
}

function Stop-Service($svc) {
    $proc = Get-RunningProcess $svc
    $pidFile = Get-PidFile $svc
    if ($proc) {
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
        Write-Host "  [stop] $($svc.Name) (PID $($proc.Id))" -ForegroundColor DarkGray
    } else {
        Write-Host "  [----] $($svc.Name) 未运行" -ForegroundColor DarkGray
    }
    if (Test-Path $pidFile) { Remove-Item $pidFile -Force }
}

function Show-Status {
    Write-Host "===== Pandora 业务服务状态 =====" -ForegroundColor Cyan
    Write-Host ("{0,-16} {1,-8} {2,-8} {3,-8} {4}" -f 'SERVICE', 'PID', 'PORT', 'PORT-UP', 'STATE')
    foreach ($svc in $Services) {
        $proc = Get-RunningProcess $svc
        $portUp = Test-PortOpen $svc.Port
        if ($proc -and $portUp) { $state = 'running'; $color = 'Green' }
        elseif ($proc) { $state = 'starting?'; $color = 'Yellow' }
        elseif ($portUp) { $state = 'port-busy'; $color = 'Yellow' }  # 端口被别的进程占,或 IDE 在调试
        else { $state = 'stopped'; $color = 'DarkGray' }
        $svcPid = if ($proc) { $proc.Id } else { '-' }
        Write-Host ("{0,-16} {1,-8} {2,-8} {3,-8} {4}" -f $svc.Name, $svcPid, $svc.Port, $(if ($portUp) { 'yes' } else { 'no' }), $state) -ForegroundColor $color
    }
}

# ===== 主流程 =====
switch ($Action) {

    'status' { Show-Status; break }

    'logs' {
        if (-not $Service) { Write-Host "[ERR] -Action logs 需要 -Service <name>" -ForegroundColor Red; exit 1 }
        $svc = Get-Service $Service
        $log = Get-LogFile $svc
        if (-not (Test-Path $log)) { Write-Host "[ERR] 无日志文件: $log" -ForegroundColor Red; exit 1 }
        Write-Host "===== tail $($svc.Name) 日志 (Ctrl+C 退出) =====" -ForegroundColor Cyan
        Get-Content $log -Tail 40 -Wait
        break
    }

    'down' {
        Write-Host "===== 停止业务服务 =====" -ForegroundColor Cyan
        if ($Service) { Stop-Service (Get-Service $Service) }
        else { foreach ($svc in $Services) { Stop-Service $svc } }
        break
    }

    'build' {
        $targets = if ($Service) { ,(Get-Service $Service) } else { @(Get-TargetServices) }
        $targetCount = if ($Service) { 1 } else { $targets.Count }
        Write-Host "===== 构建 ($targetCount 个) =====" -ForegroundColor Cyan
        foreach ($svc in $targets) { Build-Service $svc | Out-Null }
        Write-Host "[done] 构建完成" -ForegroundColor Green
        break
    }

    'restart' {
        if (-not $Service) { Write-Host "[ERR] -Action restart 需要 -Service <name>" -ForegroundColor Red; exit 1 }
        $svc = Get-Service $Service
        Write-Host "===== 重启 $($svc.Name) =====" -ForegroundColor Cyan
        Stop-Service $svc
        Start-Sleep -Milliseconds 300
        Start-Service $svc
        break
    }

    'up' {
        # 单服务前台运行
        if ($Foreground) {
            if (-not $Service) { Write-Host "[ERR] -Foreground 需要 -Service <name>" -ForegroundColor Red; exit 1 }
            $svc = Get-Service $Service
            $running = Get-RunningProcess $svc
            if ($running) {
                Write-Host "[!] $($svc.Name) 已在后台运行 (PID $($running.Id)),先停掉它" -ForegroundColor Yellow
                Stop-Service $svc
            }
            $svcDir = Join-Path $ProjectRoot $svc.Dir
            Write-Host "===== 前台运行 $($svc.Name) (:$($svc.Port),Ctrl+C 退出) =====" -ForegroundColor Cyan
            # 没装 Go 的机器跑不了 go run,退回「预编译产物 + 前台执行」,日志一样直出。
            if (-not $script:HasGo -or $UseArtifacts) {
                $exe = Build-Service $svc
                Push-Location $svcDir
                try { & $exe -conf $svc.Conf } finally { Pop-Location }
                break
            }
            Push-Location $svcDir
            try { & go run "./cmd/$($svc.Cmd)" -conf $svc.Conf } finally { Pop-Location }
            break
        }

        $targets = if ($Service) { ,(Get-Service $Service) } else { @(Get-TargetServices) }
        $targetCount = if ($Service) { 1 } else { $targets.Count }
        if ($targetCount -eq 0) { Write-Host "[!] 排除后无服务可启动" -ForegroundColor Yellow; break }

        # 全起前先探基础设施(Redis/MySQL/Kafka/etcd);不通就拦下,别让服务空转 crash-loop。
        # 单起某个服务(-Service)时不强拦(可能就是要单独调该服务),仅靠日志暴露。
        if (-not $Service -and -not (Test-InfraReady)) {
            exit 1
        }

        Write-Host "===== 启动业务服务 ($targetCount 个) =====" -ForegroundColor Cyan
        if ($Exclude.Count -gt 0) { Write-Host "排除: $($Exclude -join ', ')  (留给 IDE 调试)" -ForegroundColor Yellow }
        Write-Host ""

        foreach ($svc in $targets) { Start-Service $svc }

        Write-Host ""
        Show-Status
        Write-Host ""
        Write-Host "客户端入口 (UE 连这个): Envoy https://localhost:8443" -ForegroundColor Green
        Write-Host "看日志:  pwsh tools/scripts/run_services.ps1 -Action logs -Service <name>" -ForegroundColor DarkGray
        Write-Host "全停止:  pwsh tools/scripts/run_services.ps1 -Action down" -ForegroundColor DarkGray
        break
    }
}
