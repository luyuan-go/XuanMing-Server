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
    [switch]$PublishArtifacts,

    # 社交四服(friend/chat/guild/mail)改连本机 MySQL 而不是 TiDB。
    # 用于免 Docker 的策划机模式:TiKV 没有可用的 Windows 原生部署,而这四服的
    # 两套配置(etc/*-dev.yaml / etc/*-dev-tidb.yaml)本来就并存,这里只是选前一套。
    # 默认关:docker / 内网 / k8s 模式继续走 TiDB,行为不变。
    [switch]$SocialOnMysql,

    # 免 Docker 的策划机模式:基础设施是本机原生进程,且**刻意不起 etcd**。
    # 只影响基础设施预检要不要把 etcd 算进强依赖,以及不通时给什么修复指引。
    [switch]$NoDocker,

    # 免 Docker MySQL 的已验证独立端口。0 表示从 run/localinfra/cfg/ports.json 读取；
    # Docker 路线仍固定 3307。
    [ValidateRange(0, 49151)]
    [int]$MysqlPort = 0
)

$ErrorActionPreference = 'Stop'

# 免 Docker 路线没有 TiDB，社交四服必须使用其 MySQL dev 配置；把这个关系收成不变量，
# 避免手工漏传 -SocialOnMysql 后仍写出一个看似匹配、实际握着 TiDB DSN 的运行态 marker。
if ($NoDocker) { $SocialOnMysql = $true }

# 国内网络拉 Go 依赖:默认 proxy.golang.org / sum.golang.org 在国内基本连不上,
# 会导致 go build 拉模块超时(dial tcp 142.251.188.141:443 ... connectex: 超时)。
# 这里在脚本进程内兜底切到 goproxy.cn(不改机器全局 go env,便于一键脚本分发到多台策划机)。
# 已显式自定义 GOPROXY(且不是默认公有代理)的机器保持不动,尊重企业内网配置。
# 分隔符必须是 `|`:逗号只在代理返回 404/410 时才退到下一个源,网络层错误(unexpected EOF /
# 超时 / 连接重置)直接失败,后面的 direct 根本轮不到(详见 deploy/services/Dockerfile)。
if (-not $env:GOPROXY -or $env:GOPROXY -match 'proxy\.golang\.org') {
    $env:GOPROXY = 'https://goproxy.cn|https://proxy.golang.org|direct'
}
if (-not $env:GOSUMDB -or $env:GOSUMDB -match 'sum\.golang\.org') {
    $env:GOSUMDB = 'sum.golang.google.cn'
}

$ProjectRoot = (Resolve-Path "$PSScriptRoot/../..").Path
$stateHelper = Join-Path $PSScriptRoot 'lib/local_infra_state.ps1'
. $stateHelper
if ($NoDocker) {
    $mustUseMysql = $Action -in @('up', 'restart')
    $mysqlState = Get-PandoraLocalInfraPortState $ProjectRoot
    if ($mustUseMysql -and -not $mysqlState) {
        throw '免 Docker MySQL 尚无已验证身份状态；拒绝生成配置或启动服务。'
    }
    if ($mysqlState) {
        $verifiedMysqlPort = [int]$mysqlState.MysqlPort
        if ($MysqlPort -eq 0) {
            $MysqlPort = $verifiedMysqlPort
        } elseif ($MysqlPort -ne $verifiedMysqlPort) {
            throw "-MysqlPort $MysqlPort 与已验证运行态端口 $verifiedMysqlPort 不一致；拒绝生成可能连错库的配置。"
        }
    }
} elseif ($MysqlPort -eq 0) {
    $MysqlPort = 3307
}
$serviceRuntimeMode = if ($NoDocker) { 'nodocker' } else { 'docker' }
$serviceRuntimeSocialOnMysql = [bool]$SocialOnMysql

function Test-ServiceRuntimeProfileMatches($State) {
    return $State -and $State.Mode -eq $serviceRuntimeMode -and
        [int]$State.MysqlPort -eq $MysqlPort -and
        [bool]$State.SocialOnMysql -eq $serviceRuntimeSocialOnMysql
}

function Assert-SingleServiceRuntimeCompatible {
    $applied = Get-PandoraServiceAppliedMysqlState $ProjectRoot
    if (-not $applied) {
        $alreadyRunning = @($Services | Where-Object {
            (Get-RunningProcess $_) -or (Test-PortOpen $_.Port)
        } | ForEach-Object { $_.Name })
        if ($alreadyRunning.Count -gt 0) {
            throw "全服务运行态未登记，但已有业务端口在运行:$($alreadyRunning -join ', ')。" +
                '拒绝单独启动/重启后形成混合 DSN；请先执行一次完整启动统一刷新。'
        }
        return
    }
    if (-not (Test-ServiceRuntimeProfileMatches $applied)) {
        throw "单服务启动/重启要求 $serviceRuntimeMode/:$MysqlPort/social_mysql=$serviceRuntimeSocialOnMysql，" +
            "但全服务已登记为 $($applied.Mode)/:$($applied.MysqlPort)/social_mysql=$($applied.SocialOnMysql)。" +
            '拒绝制造混合 DSN；请执行一次完整启动完成模式切换。'
    }
}

function Assert-NoDockerMysqlOwned {
    if (-not $NoDocker) { return }
    $currentState = Get-PandoraLocalInfraPortState $ProjectRoot
    if (-not $currentState -or [int]$currentState.MysqlPort -ne $MysqlPort -or
        -not (Get-PandoraLocalMysqlOwnedProcess $ProjectRoot $currentState)) {
        throw "免 Docker MySQL :$MysqlPort 当前 listener 未通过 PID + exe + my.ini 归属复核；拒绝连接、迁移或生成服务配置。"
    }
}
if ($mustUseMysql) { Assert-NoDockerMysqlOwned }
$RunDir = Join-Path $ProjectRoot 'run/dev'
$BinDir = Join-Path $RunDir 'bin'
$LogDir = Join-Path $RunDir 'logs'
New-Item -ItemType Directory -Force -Path $BinDir, $LogDir | Out-Null

# 预编译产物目录:由 tools/scripts/build_release_binaries.ps1 在装了 Go 的机器上生成并随目录分发。
# 没装 Go 的机器(策划机)启动时直接拷这里的 exe,免装 Go 工具链、免联网拉模块。
$ArtifactBinDir = Join-Path $ProjectRoot 'run/artifacts/windows/bin'
$script:HasGo = [bool](Get-Command go -ErrorAction SilentlyContinue)

# ===== 服务清单(数组顺序 = 依赖启动顺序:leaf 依赖在前,login 最后)=====
# 全部 22 个服务(含 social/friend、social/chat、social/guild、social/mail、social/dialogue、
# data/data_service、economy/trade、economy/inventory、economy/auction、runtime/leaderboard 等)。
# 启动策略:要么全起(默认),要么用 -Service 单起某一个,不做分档启动。
$Services = @(
    @{ Name = 'player_locator'; Dir = 'services/runtime/player_locator';   Cmd = 'locator';        Conf = 'etc/locator-dev.yaml';        Port = 20006 }
    @{ Name = 'hub_allocator';  Dir = 'services/battle/hub_allocator';      Cmd = 'hub_allocator';  Conf = 'etc/hub_allocator-dev.yaml';  Port = 20021 }
    @{ Name = 'player';         Dir = 'services/account/player';            Cmd = 'player';         Conf = 'etc/player-dev.yaml';         Port = 20002 }
    @{ Name = 'ds_allocator';   Dir = 'services/battle/ds_allocator';       Cmd = 'ds_allocator';   Conf = 'etc/ds_allocator-dev.yaml';   Port = 20020 }
    @{ Name = 'push';           Dir = 'services/runtime/push';              Cmd = 'push';           Conf = 'etc/push-dev.yaml';           Port = 20014 }
    @{ Name = 'team';           Dir = 'services/matchmaking/team';          Cmd = 'team';           Conf = 'etc/team-dev.yaml';           Port = 20010 }
    @{ Name = 'friend';         Dir = 'services/social/friend';             Cmd = 'friend';         Conf = 'etc/friend-dev-tidb.yaml';    Port = 20004 }
    @{ Name = 'chat';           Dir = 'services/social/chat';               Cmd = 'chat';           Conf = 'etc/chat-dev-tidb.yaml';      Port = 20005 }
    @{ Name = 'guild';          Dir = 'services/social/guild';              Cmd = 'guild';          Conf = 'etc/guild-dev-tidb.yaml';     Port = 20008 }
    @{ Name = 'mail';           Dir = 'services/social/mail';               Cmd = 'mail';           Conf = 'etc/mail-dev-tidb.yaml';      Port = 20009 }
    @{ Name = 'dialogue';       Dir = 'services/social/dialogue';           Cmd = 'dialogue';       Conf = 'etc/dialogue-dev.yaml';       Port = 20013 }
    @{ Name = 'mission';        Dir = 'services/social/mission';            Cmd = 'mission';        Conf = 'etc/mission-dev.yaml';        Port = 20019 }
    @{ Name = 'data_service';   Dir = 'services/data/data_service';         Cmd = 'data_service';   Conf = 'etc/data_service-dev.yaml';   Port = 20003 }
    @{ Name = 'trade';          Dir = 'services/economy/trade';             Cmd = 'trade';          Conf = 'etc/trade-dev.yaml';          Port = 20012 }
    @{ Name = 'inventory';      Dir = 'services/economy/inventory';         Cmd = 'inventory';      Conf = 'etc/inventory-dev.yaml';      Port = 20015 }
    @{ Name = 'leaderboard';    Dir = 'services/runtime/leaderboard';       Cmd = 'leaderboard';    Conf = 'etc/leaderboard-dev.yaml';    Port = 20007 }
    @{ Name = 'owner';          Dir = 'services/runtime/owner';             Cmd = 'owner';          Conf = 'etc/owner-dev.yaml';          Port = 20017 }
    @{ Name = 'auction';        Dir = 'services/economy/auction';           Cmd = 'auction';        Conf = 'etc/auction-dev.yaml';        Port = 20016 }
    @{ Name = 'battle_result';  Dir = 'services/battle/battle_result';      Cmd = 'battle_result';  Conf = 'etc/battle_result-dev.yaml';  Port = 20022 }
    @{ Name = 'matchmaker';     Dir = 'services/matchmaking/matchmaker';    Cmd = 'matchmaker';     Conf = 'etc/matchmaker-dev.yaml';     Port = 20011 }
    # PVE 匹配实例:同一 matchmaker 二进制、不同配置(game_mode=pve_coop + walk_in=true,
    # 单人/整队直进副本,副本由 StartMatchRequest.map_id 选)。Envoy 按 header x-pandora-game-mode: pve 分流。
    @{ Name = 'matchmaker_pve'; Dir = 'services/matchmaking/matchmaker';    Cmd = 'matchmaker';     Conf = 'etc/matchmaker-pve.yaml';     Port = 20018 }
    @{ Name = 'login';          Dir = 'services/account/login';             Cmd = 'login';          Conf = 'etc/login-dev.yaml';          Port = 20001 }
)

# -SocialOnMysql:把社交四服从 TiDB 配置切回 MySQL 配置。两套 yaml 仓库里本就都有,
# 这里只换文件名,不生成、不改写任何配置。
if ($SocialOnMysql) {
    foreach ($svc in $Services) {
        if ($svc.Conf -match '^etc/(.+)-dev-tidb\.yaml$') {
            $svc.Conf = "etc/$($Matches[1])-dev.yaml"
        }
    }
}

function Get-ServiceConfigPath($svc) {
    Assert-NoDockerMysqlOwned
    $svcDir = Join-Path $ProjectRoot $svc.Dir
    $source = Join-Path $svcDir $svc.Conf
    if (-not $NoDocker -or $MysqlPort -eq 3307) { return $svc.Conf }
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "服务 $($svc.Name) 配置不存在:$source"
    }

    $text = [IO.File]::ReadAllText($source)
    $dsnPattern = 'tcp\(127\.0\.0\.1:3307\)'
    if ($text -notmatch $dsnPattern) { return $svc.Conf }

    # 只改 DSN 的 tcp(...) 片段；注释、文案或其它用途的 127.0.0.1:3307 原样保留。
    $rewritten = [regex]::Replace($text, $dsnPattern, "tcp(127.0.0.1:$MysqlPort)")
    if ($rewritten -match $dsnPattern) {
        throw "服务 $($svc.Name) 的运行态配置仍残留 MySQL :3307 DSN，拒绝启动。"
    }

    $runtimeDir = Join-Path $ProjectRoot 'run/localinfra/cfg/services'
    New-Item -ItemType Directory -Force -Path $runtimeDir | Out-Null
    $target = Join-Path $runtimeDir "$($svc.Name).yaml"
    $tmp = "$target.$PID.tmp"
    try {
        [IO.File]::WriteAllText($tmp, $rewritten, [Text.UTF8Encoding]::new($false))
        [IO.File]::Move($tmp, $target, $true)
    } finally {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
    return $target
}

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

function Test-ServiceListenerOwned($svc, $proc) {
    if (-not $proc) { return $false }
    try {
        $owners = @(Get-NetTCPConnection -State Listen -LocalPort ([int]$svc.Port) -ErrorAction Stop |
            Select-Object -ExpandProperty OwningProcess -Unique)
        return $owners -contains [int]$proc.Id
    } catch { return $false }
}

function Wait-ServiceProcessExit($proc, [int]$TimeoutSec) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while (-not $proc.HasExited -and (Get-Date) -lt $deadline) { Start-Sleep -Milliseconds 200 }
    return [bool]$proc.HasExited
}

# 清理 allocator 在 mode=local 下用 exec 拉起的 UE DS 子进程(Hub DS / Battle DS)。
#
# 背景(为什么脚本非管不可):`LocalHubFleetProvider.Close()` 里确实有 `cmd.Process.Kill()`,
# 但它挂在 main() 的 defer 上;本脚本停服用的是 `Stop-Process -Force`(= Windows
# TerminateProcess),Go 的 defer **一行都不会跑**,而且 allocator 没注册 signal handler。
# 加上 Windows 不像 Linux 那样有进程组连坐、`exec.Command` 也没套 Job Object,
# 父进程一被强杀,DS 立刻变成无主进程继续占着 UDP 7777 → 下一轮新 DS 起不来。
#
# 误杀防护(绝不能碰策划自己开着的 UnrealEditor):三重收敛
#   1. 只认 UnrealEditor.exe / PandoraServer.exe 两个进程名;
#   2. 命令行必须同时含 `-server` 和关卡 URL `?game=/Script/Pandora.` —— 手工开的编辑器两者都没有;
#   3. 孤儿扫描额外要求父进程已不存在(即真的无主),在跑的正常 DS 不受影响。
$LocalDsProcNames = @('UnrealEditor', 'PandoraServer')
# 只有这两个 allocator 会在 mode=local 下 exec 拉起 DS(其余服务跳过整套扫描)。
$LocalDsSpawners = @('hub_allocator', 'ds_allocator')

function Test-IsLocalDsProcess($cim) {
    if (-not $cim) { return $false }
    $name = [System.IO.Path]::GetFileNameWithoutExtension($cim.Name)
    if ($LocalDsProcNames -notcontains $name) { return $false }
    $cmdline = $cim.CommandLine
    if (-not $cmdline) { return $false }
    return ($cmdline -match '(?i)(^|\s)-server(\s|$)') -and ($cmdline -match '(?i)\?game=/Script/Pandora\.')
}

# $svc 传入时先杀它的直系子进程(停服路径);-OrphansOnly 只清父进程已死的无主 DS(启动前路径)。
function Clear-LocalDsProcesses($svc, $ownerProc, [switch]$OrphansOnly) {
    $all = @()
    try { $all = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue) } catch { return }
    if (-not $all) { return }

    $livePids = @{}
    foreach ($p in $all) { $livePids[[int]$p.ProcessId] = $true }

    foreach ($p in $all) {
        if (-not (Test-IsLocalDsProcess $p)) { continue }

        $reason = $null
        if (-not $OrphansOnly -and $ownerProc -and [int]$p.ParentProcessId -eq [int]$ownerProc.Id) {
            $reason = "$($svc.Name) 拉起的 DS 子进程"
        } elseif (-not $livePids.ContainsKey([int]$p.ParentProcessId)) {
            $reason = '无主 DS(父进程已退出)'
        }
        if (-not $reason) { continue }

        Write-Host "  [kill] $reason (PID $($p.ProcessId) $($p.Name));否则它会一直占着 DS 端口" -ForegroundColor Yellow
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
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
            # 上一轮跑过 docker/intranet 模式,容器还占着 20001-20022,宿主 go 进程会 bind 失败。
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
    Assert-NoDockerMysqlOwned
    $infra = @(
        @{ Name = 'Redis';  Port = 6380 }
        @{ Name = 'MySQL';  Port = $MysqlPort }
        @{ Name = 'Kafka';  Port = 9093 }
    )
    # etcd 只有 docker 路线才起。免 Docker 模式刻意不起它,而 services/*/etc/*-dev.yaml 里
    # 一处 etcd 引用都没有(已核对,全 0 命中)—— 把它当强依赖会让免 Docker 路线在最后一步
    # 被自己的预检拦死:前面 Envoy / 迁移全绿,到 [3/3] 报「基础设施未就绪」,而那个"缺失"的
    # 组件按设计根本就不该存在。
    if (-not $NoDocker) { $infra += @{ Name = 'etcd'; Port = 2380 } }
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
    if ($NoDocker) {
        # 免 Docker 机上没有 Docker Desktop,叫人去看鲸鱼图标是把人往沟里带。
        Write-Host "  1) 起本机原生基础设施: pwsh tools/scripts/local_infra.ps1 -Action up" -ForegroundColor Yellow
        Write-Host "  2) 查状态:             pwsh tools/scripts/local_infra.ps1 -Action status" -ForegroundColor Yellow
        Write-Host "  3) 日志在 run/localinfra/logs/ 下(mysql.log / redis.log / kafka.log / envoy.log)。" -ForegroundColor Yellow
    } else {
        Write-Host "  1) 确认 Docker Desktop 已启动(右下角鲸鱼图标变绿);" -ForegroundColor Yellow
        Write-Host "  2) 起基础设施:   pwsh tools/scripts/dev_up.ps1" -ForegroundColor Yellow
        Write-Host "  3) 查容器状态:   docker compose -f deploy/docker-compose.dev.yml ps" -ForegroundColor Yellow
        Write-Host "     Redis 应显示 healthy 且端口映射 0.0.0.0:6380->6379。" -ForegroundColor Yellow
    }
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
        if (Test-ServiceListenerOwned $svc $existing) {
            Write-Host "  [skip] $($svc.Name) 已在运行 (PID $($existing.Id))" -ForegroundColor Yellow
            return $true
        }
        Write-Host "  [FAIL] $($svc.Name) PID $($existing.Id) 存活但未拥有 :$($svc.Port)，拒绝误报已运行。" -ForegroundColor Red
        return $false
    }

    # 启动前清理占端口的残留实例,避免新进程 bind 端口失败静默崩溃。
    Clear-PortSquatter $svc
    # 同理清无主 DS:它占着 7777,新 allocator 拉起的 DS 会 bind 失败(allocator 自己没事,
    # 于是表现成"服务全绿但进不去大厅",极难排查)。
    if ($LocalDsSpawners -contains $svc.Name) { Clear-LocalDsProcesses $svc $null -OrphansOnly }

    $exe = Join-Path $BinDir "$($svc.Name).exe"
    if (-not $NoBuild -or -not (Test-Path $exe)) {
        $exe = Build-Service $svc
    }

    $svcDir = Join-Path $ProjectRoot $svc.Dir
    $confPath = Get-ServiceConfigPath $svc
    $log = Get-LogFile $svc
    $err = Get-ErrFile $svc

    $proc = Start-Process -FilePath $exe `
        -ArgumentList '-conf', "`"$confPath`"" `
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
        if (Test-ServiceListenerOwned $svc $proc) { $ready = $true; break }
        Start-Sleep -Milliseconds 400
    }

    if ($proc.HasExited) {
        Write-Host "  [FAIL] $($svc.Name) 启动后立即退出 (exit $($proc.ExitCode)),看日志: $err" -ForegroundColor Red
        return $false
    } elseif ($ready) {
        Write-Host "  [ OK ] $($svc.Name)  PID $($proc.Id)  :$($svc.Port)" -ForegroundColor Green
        return $true
    } else {
        Write-Host "  [WARN] $($svc.Name) PID $($proc.Id) 已起但 :$($svc.Port) 未就绪,看日志: $log" -ForegroundColor Yellow
        return $false
    }
}

function Stop-Service($svc) {
    $proc = Get-RunningProcess $svc
    $pidFile = Get-PidFile $svc
    if ($proc) {
        # 先杀 DS 子进程再杀 allocator:反过来的话子进程会先失去父进程变成无主,
        # ParentProcessId 指向已回收的 PID,直系关系就认不出来了。
        if ($LocalDsSpawners -contains $svc.Name) { Clear-LocalDsProcesses $svc $proc }
        try { Stop-Process -Id $proc.Id -Force -ErrorAction Stop } catch {
            Write-Host "  [FAIL] $($svc.Name) (PID $($proc.Id)) 停止命令失败:$($_.Exception.Message)" -ForegroundColor Red
        }
        if (-not (Wait-ServiceProcessExit $proc 10)) {
            Write-Host "  [FAIL] $($svc.Name) (PID $($proc.Id)) 10s 内仍未退出；保留 PID 登记。" -ForegroundColor Red
            return $false
        }
        Write-Host "  [stop] $($svc.Name) (PID $($proc.Id))" -ForegroundColor DarkGray
    } else {
        Write-Host "  [----] $($svc.Name) 未运行" -ForegroundColor DarkGray
        if ($LocalDsSpawners -contains $svc.Name) { Clear-LocalDsProcesses $svc $null -OrphansOnly }
    }
    if (Test-Path $pidFile) { Remove-Item $pidFile -Force }
    return $true
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
$orchestrationLockEntered = $false
try {
if ($Action -in @('up', 'down', 'restart')) {
    Enter-PandoraOrchestrationLock -ProjectRoot $ProjectRoot -Operation "业务服务 $Action"
    $orchestrationLockEntered = $true
}
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
        $stopTargets = if ($Service) { ,(Get-Service $Service) } else { @($Services) }
        $stopFailed = @()
        foreach ($svc in $stopTargets) {
            if (-not (Stop-Service $svc)) { $stopFailed += $svc.Name }
        }
        if ($stopFailed.Count -gt 0) {
            Write-Host "[ERR] 业务服务未能全部停止:$($stopFailed -join ', ')" -ForegroundColor Red
            exit 1
        }
        exit 0
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
        Assert-SingleServiceRuntimeCompatible
        $svc = Get-Service $Service
        Write-Host "===== 重启 $($svc.Name) =====" -ForegroundColor Cyan
        if (-not (Stop-Service $svc)) { exit 1 }
        Start-Sleep -Milliseconds 300
        if (-not (Start-Service $svc)) { exit 1 }
        exit 0
        break
    }

    'up' {
        if ($Service) { Assert-SingleServiceRuntimeCompatible }
        # 单服务前台运行
        if ($Foreground) {
            if (-not $Service) { Write-Host "[ERR] -Foreground 需要 -Service <name>" -ForegroundColor Red; exit 1 }
            $svc = Get-Service $Service
            $running = Get-RunningProcess $svc
            if ($running) {
                Write-Host "[!] $($svc.Name) 已在后台运行 (PID $($running.Id)),先停掉它" -ForegroundColor Yellow
                if (-not (Stop-Service $svc)) { exit 1 }
            }
            $svcDir = Join-Path $ProjectRoot $svc.Dir
            $confPath = Get-ServiceConfigPath $svc
            Write-Host "===== 前台运行 $($svc.Name) (:$($svc.Port),Ctrl+C 退出) =====" -ForegroundColor Cyan
            # 没装 Go 的机器跑不了 go run,退回「预编译产物 + 前台执行」,日志一样直出。
            if (-not $script:HasGo -or $UseArtifacts) {
                $exe = Build-Service $svc
                Push-Location $svcDir
                try { & $exe -conf $confPath } finally { Pop-Location }
                break
            }
            Push-Location $svcDir
            try { & go run "./cmd/$($svc.Cmd)" -conf $confPath } finally { Pop-Location }
            break
        }

        $targets = if ($Service) { ,(Get-Service $Service) } else { @(Get-TargetServices) }
        $targetCount = if ($Service) { 1 } else { $targets.Count }
        if ($targetCount -eq 0) { Write-Host "[!] 排除后无服务可启动" -ForegroundColor Yellow; break }

        if (-not $Service) {
            $appliedMysql = Get-PandoraServiceAppliedMysqlState $ProjectRoot
            if (-not (Test-ServiceRuntimeProfileMatches $appliedMysql)) {
                $fromText = if ($appliedMysql) { "$($appliedMysql.Mode)/:$($appliedMysql.MysqlPort)/social_mysql=$($appliedMysql.SocialOnMysql)" } else { '未登记/旧版' }
                Write-Host "[INFO] 业务服务已应用运行态为 $fromText，当前为 $serviceRuntimeMode/:$MysqlPort/social_mysql=$serviceRuntimeSocialOnMysql；先完整停止已登记服务，禁止跨模式或旧 DSN 进程被 skip。" -ForegroundColor Cyan
                $stopFailed = @()
                foreach ($svc in $Services) {
                    if (-not (Stop-Service $svc)) { $stopFailed += $svc.Name }
                }
                if ($stopFailed.Count -gt 0) {
                    Write-Host "[ERR] 旧业务服务停止失败:$($stopFailed -join ', ')；不会带着旧 MySQL DSN 继续启动。" -ForegroundColor Red
                    exit 1
                }
            }
            if ($Exclude.Count -gt 0 -or $Only.Count -gt 0) {
                # 部分启动不能证明 22 个进程都用了同一套配置。先使旧的全局 marker 失效，
                # 否则下一次完整启动可能误信旧 marker 并 skip 本轮改过配置的进程。
                Clear-PandoraServiceAppliedMysqlState $ProjectRoot
                Write-Host '[WARN] 本轮是 -Exclude/-Only 部分启动；已清除“全服务已应用”运行态，下次完整启动会统一刷新。' -ForegroundColor Yellow
            }
        }

        # 全起前先探基础设施(Redis/MySQL/Kafka/etcd);不通就拦下,别让服务空转 crash-loop。
        # 单起某个服务(-Service)时不强拦(可能就是要单独调该服务),仅靠日志暴露。
        if (-not $Service -and -not (Test-InfraReady)) {
            exit 1
        }

        Write-Host "===== 启动业务服务 ($targetCount 个) =====" -ForegroundColor Cyan
        if ($Exclude.Count -gt 0) { Write-Host "排除: $($Exclude -join ', ')  (留给 IDE 调试)" -ForegroundColor Yellow }
        Write-Host ""

        $startFailed = @()
        foreach ($svc in $targets) {
            if (-not (Start-Service $svc)) { $startFailed += $svc.Name }
        }

        if ($startFailed.Count -gt 0) {
            Write-Host "[ERR] 业务服务未全部就绪:$($startFailed -join ', ')；不会登记已应用 MySQL 端口。" -ForegroundColor Red
            exit 1
        }
        if (-not $Service -and $Exclude.Count -eq 0 -and $Only.Count -eq 0) {
            Set-PandoraServiceAppliedMysqlPort -ProjectRoot $ProjectRoot -Mode $serviceRuntimeMode `
                -MysqlPort $MysqlPort -SocialOnMysql $serviceRuntimeSocialOnMysql | Out-Null
        } elseif (-not $Service) {
            Write-Host '[WARN] 部分启动不登记“全服务已应用”运行态。' -ForegroundColor Yellow
        }

        Write-Host ""
        Show-Status
        Write-Host ""
        Write-Host "客户端入口 (UE 连这个): Envoy https://localhost:8443" -ForegroundColor Green
        Write-Host "看日志:  pwsh tools/scripts/run_services.ps1 -Action logs -Service <name>" -ForegroundColor DarkGray
        Write-Host "全停止:  pwsh tools/scripts/run_services.ps1 -Action down" -ForegroundColor DarkGray
        exit 0
    }
}
} finally {
    if ($orchestrationLockEntered) { Exit-PandoraOrchestrationLock }
}
