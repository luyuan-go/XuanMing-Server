# 免 Docker 基础设施的运行态身份文件。
#
# ports.json 不只是端口记录：PID、mysqld 映像和精确的 --defaults-file 参数必须同时吻合，
# 调用方才可以把该 listener 当成本工作区 MySQL。端口可连、协议正确或密码碰巧相同都不算归属证明。

function Get-PandoraLocalInfraPortStatePath([string]$ProjectRoot) {
    return (Join-Path $ProjectRoot 'run/localinfra/cfg/ports.json')
}

function Get-PandoraServiceAppliedMysqlStatePath([string]$ProjectRoot) {
    return (Join-Path $ProjectRoot 'run/dev/mysql-port-applied.json')
}

function Get-PandoraOrchestrationLockPath([string]$ProjectRoot) {
    return (Join-Path $ProjectRoot 'run/orchestration.lock')
}

# start.ps1 -> dev_all.ps1 -> local_infra/run_services 都在同一个 PowerShell runspace 里通过 `&` 调用。
# 用 runspace-global 状态保存文件句柄和递归深度，让整条一键链持有同一把 OS 文件锁并允许子脚本
# 安全重入；同一进程里的其它并发 runspace 看不到该状态，仍会被文件锁挡住，不能误当成重入。
function Enter-PandoraOrchestrationLock {
    param(
        [Parameter(Mandatory)][string]$ProjectRoot,
        [Parameter(Mandatory)][string]$Operation
    )
    $stateVariable = '__PandoraLocalDevOrchestrationLockV1'
    $path = Get-PandoraFullPath (Get-PandoraOrchestrationLockPath $ProjectRoot)
    $held = Get-Variable -Name $stateVariable -Scope Global -ValueOnly -ErrorAction SilentlyContinue
    if ($held) {
        if (-not (Test-PandoraPathEqual $held.Path $path)) {
            throw "当前 pwsh 已持有另一工作区的本地编排锁:$($held.Path)"
        }
        $held.Depth = [int]$held.Depth + 1
        return
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $path) | Out-Null
    $stream = $null
    try {
        $stream = [IO.File]::Open($path, [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
    } catch {
        throw "另一轮 Pandora 本地启动/停止/reset 正在进行；本轮 $Operation 已中止，不会并发改动进程或数据库。"
    }
    Set-Variable -Name $stateVariable -Scope Global -Value ([pscustomobject]@{
        Path = $path; Stream = $stream; Depth = 1
    })
}

function Exit-PandoraOrchestrationLock {
    $stateVariable = '__PandoraLocalDevOrchestrationLockV1'
    $held = Get-Variable -Name $stateVariable -Scope Global -ValueOnly -ErrorAction SilentlyContinue
    if (-not $held) { return }
    $held.Depth = [int]$held.Depth - 1
    if ([int]$held.Depth -gt 0) { return }
    try { $held.Stream.Dispose() } finally { Remove-Variable -Name $stateVariable -Scope Global -Force -ErrorAction SilentlyContinue }
}

function Get-PandoraFullPath([string]$Path) {
    if ([string]::IsNullOrWhiteSpace($Path)) { throw '路径为空' }
    return [IO.Path]::GetFullPath($Path)
}

function Test-PandoraPathEqual([string]$Left, [string]$Right) {
    try {
        return [string]::Equals(
            (Get-PandoraFullPath $Left),
            (Get-PandoraFullPath $Right),
            [StringComparison]::OrdinalIgnoreCase
        )
    } catch { return $false }
}

function Test-PandoraMysqlExecutablePath([string]$ProjectRoot, [string]$Executable) {
    try {
        $exe = Get-PandoraFullPath $Executable
        $root = (Get-PandoraFullPath (Join-Path $ProjectRoot 'run/localinfra/dist/mysql')).TrimEnd('\', '/') + [IO.Path]::DirectorySeparatorChar
        return [IO.Path]::GetFileName($exe) -ieq 'mysqld.exe' -and
            $exe.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)
    } catch { return $false }
}

function Get-PandoraLocalInfraPortState([string]$ProjectRoot) {
    $path = Get-PandoraLocalInfraPortStatePath $ProjectRoot
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { return $null }

    try {
        $state = Get-Content -LiteralPath $path -Raw -Encoding utf8 | ConvertFrom-Json
        $schemaVersion = 0
        $port = 0
        $processId = 0
        $exe = "$($state.mysql_executable)"
        $defaultsFile = "$($state.mysql_defaults_file)"
        $expectedIni = Join-Path $ProjectRoot 'run/localinfra/cfg/my.ini'
        if (-not [int]::TryParse("$($state.schema_version)", [ref]$schemaVersion) -or $schemaVersion -ne 1 -or
            -not [int]::TryParse("$($state.mysql_port)", [ref]$port) -or
            $port -lt 1024 -or $port -gt 49151 -or
            -not [int]::TryParse("$($state.mysql_process_id)", [ref]$processId) -or $processId -lt 0 -or
            -not (Test-PandoraMysqlExecutablePath $ProjectRoot $exe) -or
            -not (Test-PandoraPathEqual $defaultsFile $expectedIni)) {
            throw '字段不合法'
        }
        return [pscustomobject]@{
            SchemaVersion = $schemaVersion
            MysqlPort = $port
            MysqlProcessId = $processId
            MysqlExecutable = Get-PandoraFullPath $exe
            MysqlDefaultsFile = Get-PandoraFullPath $defaultsFile
            Path = $path
        }
    } catch {
        throw "免 Docker MySQL 身份状态文件损坏:$path。请把该文件改名后重试；脚本不会据此连接未知数据库。详情:$($_.Exception.Message)"
    }
}

function Get-PandoraLocalMysqlPort([string]$ProjectRoot, [switch]$Required) {
    $state = Get-PandoraLocalInfraPortState $ProjectRoot
    if ($state) { return [int]$state.MysqlPort }
    if ($Required) {
        throw "免 Docker MySQL 尚无已验证身份状态。先运行:pwsh tools/scripts/local_infra.ps1 -Action up"
    }
    return 0
}

function Set-PandoraLocalInfraPortState {
    param(
        [Parameter(Mandatory)][string]$ProjectRoot,
        [Parameter(Mandatory)][ValidateRange(1024, 49151)][int]$MysqlPort,
        [Parameter(Mandatory)][ValidateRange(0, 2147483647)][int]$MysqlProcessId,
        [Parameter(Mandatory)][string]$MysqlExecutable,
        [Parameter(Mandatory)][string]$MysqlDefaultsFile
    )

    $exe = Get-PandoraFullPath $MysqlExecutable
    $defaultsFile = Get-PandoraFullPath $MysqlDefaultsFile
    $expectedIni = Join-Path $ProjectRoot 'run/localinfra/cfg/my.ini'
    if (-not (Test-PandoraMysqlExecutablePath $ProjectRoot $exe)) {
        throw "拒绝写入不属于本工作区 dist/mysql 的 mysqld:$exe"
    }
    if (-not (Test-PandoraPathEqual $defaultsFile $expectedIni)) {
        throw "拒绝写入非本工作区 my.ini:$defaultsFile"
    }

    $path = Get-PandoraLocalInfraPortStatePath $ProjectRoot
    $dir = Split-Path -Parent $path
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    $tmp = Join-Path $dir ("ports.json.{0}.{1}.tmp" -f $PID, [guid]::NewGuid().ToString('N'))
    try {
        $json = [ordered]@{
            schema_version = 1
            mysql_port = $MysqlPort
            mysql_process_id = $MysqlProcessId
            mysql_executable = $exe
            mysql_defaults_file = $defaultsFile
        } | ConvertTo-Json
        Set-Content -LiteralPath $tmp -Value $json -Encoding utf8NoBOM
        [IO.File]::Move($tmp, $path, $true)
    } finally {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
    return $path
}

function Get-PandoraProcessCommandLine([int]$ProcessId) {
    try {
        $row = Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction Stop
        return "$($row.CommandLine)"
    } catch { return '' }
}

function Get-PandoraDefaultsFileArgument([string]$CommandLine) {
    if ([string]::IsNullOrWhiteSpace($CommandLine)) { return $null }
    # Start-Process / WMI 在 Windows 上常见三种形态：整个 token 带引号、只给值加引号、
    # 或无空格的裸 token。全部要求 token 边界，不能让 my.ini.evil 前缀冒充。
    $match = [regex]::Match($CommandLine,
        '(?i)(?:^|\s)(?:"--defaults-file=(?<whole>[^"]+)"|--defaults-file="(?<inner>[^"]+)"|--defaults-file=(?<plain>\S+))(?=\s|$)')
    if ($match.Success) {
        return @($match.Groups['whole'].Value, $match.Groups['inner'].Value, $match.Groups['plain'].Value) |
            Where-Object { $_ } | Select-Object -First 1
    }
    return $null
}

function Get-PandoraLocalMysqlOwnedProcess([string]$ProjectRoot, $State = $null) {
    if (-not $State) { $State = Get-PandoraLocalInfraPortState $ProjectRoot }
    if (-not $State -or [int]$State.MysqlProcessId -le 0) { return $null }

    $proc = Get-Process -Id ([int]$State.MysqlProcessId) -ErrorAction SilentlyContinue
    if (-not $proc) { return $null }
    $actualExe = $null
    try { $actualExe = $proc.Path } catch { return $null }
    if (-not (Test-PandoraPathEqual $actualExe $State.MysqlExecutable) -or
        -not (Test-PandoraMysqlExecutablePath $ProjectRoot $actualExe)) { return $null }

    $actualIni = Get-PandoraDefaultsFileArgument (Get-PandoraProcessCommandLine ([int]$proc.Id))
    if (-not (Test-PandoraPathEqual $actualIni $State.MysqlDefaultsFile)) { return $null }

    try {
        $listenerPids = @(Get-NetTCPConnection -State Listen -LocalPort ([int]$State.MysqlPort) -ErrorAction Stop |
            Select-Object -ExpandProperty OwningProcess -Unique)
        if ($listenerPids -notcontains [int]$proc.Id) { return $null }
    } catch { return $null }
    return $proc
}

function Get-PandoraServiceAppliedMysqlState([string]$ProjectRoot) {
    $path = Get-PandoraServiceAppliedMysqlStatePath $ProjectRoot
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { return $null }
    try {
        $state = Get-Content -LiteralPath $path -Raw -Encoding utf8 | ConvertFrom-Json
        $schemaVersion = 0
        if (-not [int]::TryParse("$($state.schema_version)", [ref]$schemaVersion)) { throw 'schema_version 不合法' }
        $port = 0
        $mode = "$($state.mode)"
        # 未发布过 social profile 的旧 v1 marker 不能证明完整配置，只把它当“未登记”触发全停刷新；
        # JSON/字段真实损坏仍 fail closed，不能悄悄跳过。
        if ($schemaVersion -eq 1 -and $state.PSObject.Properties.Name -notcontains 'social_on_mysql') {
            if (-not [int]::TryParse("$($state.mysql_port)", [ref]$port) -or
                $port -lt 1024 -or $port -gt 49151 -or $mode -notin @('docker', 'nodocker')) {
                throw '旧版 v1 字段不合法'
            }
            return $null
        }
        $socialOnMysql = $state.social_on_mysql
        if ($schemaVersion -ne 2 -or
            -not [int]::TryParse("$($state.mysql_port)", [ref]$port) -or
            $port -lt 1024 -or $port -gt 49151 -or
            $mode -notin @('docker', 'nodocker') -or $socialOnMysql -isnot [bool]) { throw '字段不合法' }
        return [pscustomobject]@{
            SchemaVersion = $schemaVersion; Mode = $mode; MysqlPort = $port
            SocialOnMysql = [bool]$socialOnMysql; Path = $path
        }
    } catch {
        throw "业务服务已应用运行模式状态损坏:$path。拒绝跳过服务重启。详情:$($_.Exception.Message)"
    }
}

function Get-PandoraServiceAppliedMysqlPort([string]$ProjectRoot) {
    $state = Get-PandoraServiceAppliedMysqlState $ProjectRoot
    if ($state) { return [int]$state.MysqlPort }
    return 0
}

function Clear-PandoraServiceAppliedMysqlState([string]$ProjectRoot) {
    $path = Get-PandoraServiceAppliedMysqlStatePath $ProjectRoot
    if (Test-Path -LiteralPath $path) {
        Remove-Item -LiteralPath $path -Force -ErrorAction Stop
    }
    if (Test-Path -LiteralPath $path) {
        throw "无法清除业务服务已应用运行态:$path"
    }
}

function Set-PandoraServiceAppliedMysqlPort {
    param(
        [Parameter(Mandatory)][string]$ProjectRoot,
        [Parameter(Mandatory)][ValidateSet('docker', 'nodocker')][string]$Mode,
        [Parameter(Mandatory)][ValidateRange(1024, 49151)][int]$MysqlPort,
        [Parameter(Mandatory)][bool]$SocialOnMysql
    )
    $path = Get-PandoraServiceAppliedMysqlStatePath $ProjectRoot
    $dir = Split-Path -Parent $path
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    $tmp = Join-Path $dir ("mysql-port-applied.json.{0}.{1}.tmp" -f $PID, [guid]::NewGuid().ToString('N'))
    try {
        [ordered]@{
            schema_version = 2; mode = $Mode; mysql_port = $MysqlPort; social_on_mysql = $SocialOnMysql
        } | ConvertTo-Json |
            Set-Content -LiteralPath $tmp -Encoding utf8NoBOM
        [IO.File]::Move($tmp, $path, $true)
    } finally {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
    return $path
}
