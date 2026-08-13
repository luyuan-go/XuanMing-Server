# Pandora 本地 k8s 真 DS 联调的宿主 Envoy 桥接器
#
# 为什么需要它:
#   - k8s 模式里 22 个 Go Deployment 都跑在 pandora namespace 的 ClusterIP Service 后面
#   - UE 客户端打宿主 Envoy :8443；GameServer DS 回调走集群内 pandora-envoy :8444
#   - 现有 deploy/envoy/envoy.yaml 的 upstream 全指向 host.docker.internal:500xx
#
# 所以这里做两件事:
#   1) 对 Envoy 会访问到的每个 k8s Service 起本地 kubectl port-forward(127.0.0.1:500xx)
#   2) 单独拉起 docker compose 里的 envoy 容器,复用现有本地开发配置
#
# 端口占用安全(P1):只有「本 bridge 自己起的 kubectl port-forward svc/<name>」才算就绪复用;
#   若 127.0.0.1:500xx 被别的进程占用(本地 go 服务 / docker-compose 业务服务),会让 Envoy
#   连到旧后端而不是 k8s Service,导致 e2e「假通过」—— 此时默认 fail-fast,
#   或加 -Force 由本脚本杀掉占用者后重建。
#
# 旧 compose 业务容器预检(P1,2026-07-07):docker 发布端口监听在 0.0.0.0(owner 是
#   com.docker.backend),按 127.0.0.1 查监听根本看不到;kubectl port-forward 照样绑
#   127.0.0.1 成功 → 两个监听并存,Envoy host.docker.internal 流量去向不确定(时好时坏)。
#   重启电脑后 Docker Desktop 还会把上次 running 的业务容器自动复活再抢回端口。
#   所以 bridge 启动前先扫 `docker ps`:发布了 bridge 端口的 pandora-* 业务容器自动 stop
#   (幂等、可再手动 up 回来);非 pandora 容器默认 fail-fast,-Force 才 stop。

[CmdletBinding()]
param(
    [switch]$Force,   # 端口被非 bridge 进程占用时,杀掉占用者后重建 port-forward
    [Parameter(Mandatory = $true)]
    [string]$KubeContext, # port-forward 必须显式钉住的本地 minikube context
    [string]$MinikubeProfile = '' # endpoint 本地性校验；留空时与 KubeContext 同名
)

$ErrorActionPreference = 'Stop'
$ScriptDir = $PSScriptRoot
$ProjectRoot = (Resolve-Path "$ScriptDir/../..").Path
$ComposeFile = Join-Path $ProjectRoot 'deploy/docker-compose.dev.yml'
$EnvFile = Join-Path $ProjectRoot 'deploy/env/dev.env'
$StateDir = Join-Path $ProjectRoot 'run/k8s-envoy-bridge'
$K8sNamespace = 'pandora'

function Write-Info($m) { Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Write-Ok($m)   { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Write-Warn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Write-Step($m) { Write-Host "`n===== $m =====" -ForegroundColor Magenta }

function Test-KubeContextIsLocalMinikube([string]$Context, [string]$Profile) {
    $cluster = (kubectl config view -o jsonpath="{.contexts[?(@.name==`"$Context`")].context.cluster}" 2>$null)
    if ([string]::IsNullOrWhiteSpace($cluster)) { return $false }
    $server = (kubectl config view -o jsonpath="{.clusters[?(@.name==`"$cluster`")].cluster.server}" 2>$null)
    if ([string]::IsNullOrWhiteSpace($server)) { return $false }
    try { $apiHost = ([System.Uri]$server).Host } catch { return $false }
    if ($apiHost -in @('127.0.0.1', 'localhost', '::1', '[::1]')) { return $true }
    $mkIp = (minikube -p $Profile ip 2>$null | Out-String).Trim()
    return ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($mkIp) -and $apiHost -eq $mkIp)
}

# Envoy dev TLS 证书校验 / 自愈(与 dev_up.ps1 复用同一套逻辑)。
. "$ScriptDir/envoy_cert.ps1"
. "$ScriptDir/dev_env_file.ps1"

# Essential = 登录→Hub→匹配→Battle→结算 闭环必需的服务;非必需(社交/拍卖/交易等)
# 即便 Pod 没起来,也不该让整个 bridge / e2e 直接失败(只 WARN 跳过该 port-forward)。
$Forwards = @(
    @{ Name = 'login';          Port = 20001; Essential = $true  }
    @{ Name = 'player';         Port = 20002; Essential = $true  }
    @{ Name = 'data-service';   Port = 20003; Essential = $true  }
    @{ Name = 'friend';         Port = 20004; Essential = $false }
    @{ Name = 'chat';           Port = 20005; Essential = $false }
    @{ Name = 'player-locator'; Port = 20006; Essential = $true  }
    @{ Name = 'leaderboard';    Port = 20007; Essential = $false }
    @{ Name = 'guild';          Port = 20008; Essential = $false }
    @{ Name = 'mail';           Port = 20009; Essential = $false }
    @{ Name = 'team';           Port = 20010; Essential = $true  }
    @{ Name = 'matchmaker';     Port = 20011; Essential = $true  }
    # PVE 直进匹配实例(Envoy 按 x-pandora-game-mode: pve 分流到 host 20018);
    # 副本测试主链路依赖它,必须 fail-fast,避免客户端点击开局后才报 Envoy 503。
    @{ Name = 'matchmaker-pve'; Port = 20018; Essential = $true  }
    @{ Name = 'trade';          Port = 20012; Essential = $false }
    @{ Name = 'dialogue';       Port = 20013; Essential = $false }
    @{ Name = 'mission';        Port = 20019; Essential = $false }
    @{ Name = 'push';           Port = 20014; Essential = $true  }
    @{ Name = 'inventory';      Port = 20015; Essential = $false }
    @{ Name = 'auction';        Port = 20016; Essential = $false }
    # Owner 是登录/分配/DS 归属权威；即使 Envoy 不直接路由它，也必须映射并通过健康门禁。
    @{ Name = 'owner';          Port = 20017; Essential = $true  }
    @{ Name = 'ds-allocator';   Port = 20020; Essential = $true  }
    @{ Name = 'hub-allocator';  Port = 20021; Essential = $true  }
    @{ Name = 'battle-result';  Port = 20022; Essential = $true  }
)

function Ensure-File([string]$path) {
    if (-not (Test-Path $path)) {
        throw "缺少文件: $path"
    }
}

function Get-PidFile([string]$name) { Join-Path $StateDir "$name.pid" }

# 返回 $port 上 LISTEN 的占用进程 PID(无则 $null)。
# 不只查 127.0.0.1:绑 0.0.0.0/:: 的监听(宿主 go 服务、docker 发布端口)同样会截走
# Envoy host.docker.internal 的流量,必须一并检出;127.0.0.1 精确监听优先返回。
function Get-PortListenerPid([int]$port) {
    $conns = @(Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalAddress -in @('127.0.0.1', '0.0.0.0', '::', '::1') })
    if ($conns.Count -eq 0) { return $null }
    $exact = $conns | Where-Object { $_.LocalAddress -eq '127.0.0.1' } | Select-Object -First 1
    if ($exact) { return $exact.OwningProcess }
    return $conns[0].OwningProcess
}

# Docker Desktop 自身的转发进程:它替容器持有发布端口的 0.0.0.0 监听,
# 杀它 = 杀整个 Docker。这类占用只能 docker stop 对应容器,绝不 Stop-Process。
function Test-IsDockerBackendProc([int]$processId) {
    if (-not $processId) { return $false }
    $proc = Get-Process -Id $processId -ErrorAction SilentlyContinue
    if (-not $proc) { return $false }
    return ($proc.ProcessName -match '^(com\.docker\..*|docker|dockerd|vpnkit.*|wslrelay)$')
}

function Get-ProcCommandLine([int]$processId) {
    try {
        return (Get-CimInstance Win32_Process -Filter "ProcessId=$processId" -ErrorAction Stop).CommandLine
    } catch {
        return $null
    }
}

function Get-ProcDesc([int]$processId) {
    $proc = Get-Process -Id $processId -ErrorAction SilentlyContinue
    if ($proc) { return "$($proc.ProcessName) (PID=$processId)" }
    return "PID=$processId"
}

function Test-CommandLineOptionValue([string]$cmd, [string]$option, [string]$value) {
    if ([string]::IsNullOrWhiteSpace($cmd) -or [string]::IsNullOrWhiteSpace($value)) { return $false }
    # Start-Process 在 Windows 上可能给 flag/value 加双引号；按 token 边界精确匹配，避免
    # `pandora-agones` 错把 `pandora-agones-old` 当成同一 context。
    $pattern = '(?:^|\s)"?' + [regex]::Escape($option) + '"?(?:=|\s+)"?' +
        [regex]::Escape($value) + '"?(?=\s|$)'
    return [regex]::IsMatch($cmd, $pattern)
}

# 占用 $port 的进程是否具有本 bridge 的 port-forward 形状（暂不判断 context）。
function Test-IsBridgePortForwardShape([int]$processId, [string]$name, [int]$port) {
    if (-not $processId) { return $false }
    $proc = Get-Process -Id $processId -ErrorAction SilentlyContinue
    if (-not $proc -or $proc.ProcessName -ne 'kubectl') { return $false }
    $cmd = Get-ProcCommandLine $processId
    if (-not $cmd) { return $false }
    $sameNamespace = Test-CommandLineOptionValue $cmd '--namespace' $K8sNamespace
    return ($sameNamespace -and $cmd -like '*port-forward*' -and $cmd -like "*svc/$name*" -and $cmd -like "*${port}:${port}*")
}

# 占用 $port 的进程是不是「本 bridge + 当前显式 context」的 port-forward。
function Test-IsBridgePortForward([int]$processId, [string]$name, [int]$port) {
    if (-not (Test-IsBridgePortForwardShape $processId $name $port)) { return $false }
    $cmd = Get-ProcCommandLine $processId
    $sameContext = Test-CommandLineOptionValue $cmd '--context' $KubeContext
    return $sameContext
}

# 预检:停掉仍在发布 bridge 端口的旧 docker compose 业务容器。
# 场景:上次跑过 docker/battle 模式没 down,或重启电脑后 Docker Desktop 按 restart 策略
# 自动复活业务容器 → 它们经 com.docker.backend 持有 0.0.0.0:500xx,抢走 Envoy 流量。
# pandora-* 是本仓库 compose 固定 container_name,自动 stop 安全幂等;其它容器不越权。
function Stop-StaleComposeContainers {
    $bridgePorts = @($Forwards | ForEach-Object { [string]$_.Port })
    $lines = @(docker ps --format '{{.ID}}|{{.Names}}|{{.Ports}}' 2>$null)
    if ($LASTEXITCODE -ne 0 -or $lines.Count -eq 0) { return }
    $stopped = 0
    foreach ($line in $lines) {
        $parts = "$line".Split('|', 3)
        if ($parts.Count -lt 3) { continue }
        $cid = $parts[0]; $cname = $parts[1]; $portsDesc = $parts[2]
        $hit = $bridgePorts | Where-Object { $portsDesc -match ":$([regex]::Escape($_))->" } | Select-Object -First 1
        if (-not $hit) { continue }
        if ($cname -like 'pandora-*') {
            Write-Warn "旧 compose 业务容器 $cname 仍发布宿主端口 $hit(会截走 Envoy → k8s 的流量),自动停掉"
        } elseif ($Force) {
            Write-Warn "容器 $cname 发布 bridge 端口 $hit,-Force 停掉"
        } else {
            throw "容器 $cname 发布了 bridge 需要的宿主端口 $hit(非本仓库 pandora-* 容器,不自动处理);请手动 docker stop,或加 -Force"
        }
        docker stop $cid | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "docker stop $cname 失败,请手动处理后重试" }
        Write-Ok "已停 $cname"
        $stopped++
    }
    if ($stopped -eq 0) { Write-Ok '无旧业务容器占用 bridge 端口' }
}

# ---- gRPC 健康探测(修复"旧 port-forward 端口还 LISTEN 但底层 Pod 已消失"的假健康)----
# 背景:Go 服务滚动更新后,老 kubectl port-forward svc/<name> 仍占着 127.0.0.1:port 的
# LISTEN,但它绑定的 Pod/容器已被替换。端口层面看"就绪",可第一次真实 gRPC 请求进来才报
# "No such container / lost connection to pod",Envoy 于是回 503(HTTP 503 without gRPC
# trailer)。只查端口占用不足以证明后端可用,必须做真实 grpc.health.v1.Health/Check。
$script:GrpcurlResolved = $false
$script:GrpcurlPath = $null
function Get-GrpcurlPath {
    if (-not $script:GrpcurlResolved) {
        $script:GrpcurlResolved = $true
        $cmd = Get-Command grpcurl -ErrorAction SilentlyContinue
        if ($cmd) {
            $script:GrpcurlPath = $cmd.Source
        } else {
            $candidates = @((Join-Path $ProjectRoot 'tools/bin/grpcurl.exe'))
            if ($env:USERPROFILE) { $candidates += (Join-Path $env:USERPROFILE 'go/bin/grpcurl.exe') }
            if ($env:GOPATH) { $candidates += (Join-Path $env:GOPATH 'bin/grpcurl.exe') }
            foreach ($c in $candidates) {
                if ($c -and (Test-Path $c)) { $script:GrpcurlPath = $c; break }
            }
        }
    }
    return $script:GrpcurlPath
}

# 单次 gRPC 健康探测。返回 'serving' | 'unhealthy' | 'nogrpcurl'。
# 'nogrpcurl' 表示环境里没有 grpcurl,调用方据此决定保守策略(不盲目复用旧 port-forward)。
function Test-PortForwardServing([int]$port) {
    $grpcurl = Get-GrpcurlPath
    if (-not $grpcurl) { return 'nogrpcurl' }
    try {
        $out = & $grpcurl '-plaintext' '-max-time' '2' '-d' '{}' "127.0.0.1:$port" 'grpc.health.v1.Health/Check' 2>&1
    } catch {
        return 'unhealthy'
    }
    if ("$out" -match 'SERVING') { return 'serving' }
    return 'unhealthy'
}

# 带重试的健康探测(端口刚 LISTEN 到后端 SERVING 之间有窗口期)。
# grpcurl 不存在时立刻返回 'nogrpcurl',不做无谓重试。
function Wait-PortForwardServing([int]$port, [int]$retries = 3, [int]$delayMs = 500) {
    for ($i = 0; $i -lt $retries; $i++) {
        $status = Test-PortForwardServing $port
        if ($status -eq 'serving' -or $status -eq 'nogrpcurl') { return $status }
        if ($i -lt $retries - 1) { Start-Sleep -Milliseconds $delayMs }
    }
    return 'unhealthy'
}

function Start-PortForward([string]$name, [int]$port, [bool]$essential = $true) {
    $ownerPid = Get-PortListenerPid $port
    # 升级前版本的 bridge 没带 --context。若 pid 文件证明它确是本脚本留下的同名转发，
    # 自动淘汰后重建；无 pid 证据的相似手工进程不越权处理，仍走下面 fail-fast/-Force。
    $recordedPid = (Get-Content (Get-PidFile $name) -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($ownerPid -and "$recordedPid" -eq "$ownerPid" -and
        (Test-IsBridgePortForwardShape $ownerPid $name $port) -and
        -not (Test-IsBridgePortForward $ownerPid $name $port)) {
        Write-Warn "发现未锁定当前 context 的旧 bridge port-forward $name(PID=$ownerPid)，自动停掉重建"
        Stop-Process -Id $ownerPid -Force -ErrorAction SilentlyContinue
        for ($i = 0; $i -lt 10 -and (Get-PortListenerPid $port); $i++) { Start-Sleep -Milliseconds 300 }
        Remove-Item (Get-PidFile $name) -ErrorAction SilentlyContinue
        if (Get-PortListenerPid $port) { throw "旧 bridge port-forward 端口 $port 释放失败" }
        $ownerPid = $null
    }
    if ($ownerPid -and (Test-IsBridgePortForward $ownerPid $name $port)) {
        # 端口在 LISTEN 且命令行像 bridge 自己起的 port-forward —— 但"还在 LISTEN"不代表健康。
        # 滚动更新后旧 kubectl port-forward 会残留:端口照 LISTEN,底层 Pod/容器已被替换,
        # 第一次真实请求才报 "No such container / lost connection to pod" → Envoy 503。
        # 必须真实 gRPC 探活,SERVING 才复用;否则杀掉旧 port-forward 重建。
        $health = Wait-PortForwardServing $port 3 400
        if ($health -eq 'serving') {
            Write-Ok "port-forward 已在且 SERVING(bridge 自身)$name :127.0.0.1:$port (PID=$ownerPid)"
            Set-Content -Path (Get-PidFile $name) -Value $ownerPid -Encoding ASCII
            return
        }
        if ($health -eq 'nogrpcurl') {
            Write-Warn "未找到 grpcurl,无法确认 $name :127.0.0.1:$port 是否 SERVING;保守起见杀掉旧 port-forward 重建"
        } else {
            Write-Warn "$name :127.0.0.1:$port 端口在 LISTEN 但 gRPC 未 SERVING(旧 Pod 已失效),杀掉旧 port-forward 重建"
        }
        Stop-Process -Id $ownerPid -Force -ErrorAction SilentlyContinue
        for ($i = 0; $i -lt 10 -and (Get-PortListenerPid $port); $i++) { Start-Sleep -Milliseconds 300 }
        Remove-Item (Get-PidFile $name) -ErrorAction SilentlyContinue
        Remove-Item (Join-Path $StateDir "$name.log") -ErrorAction SilentlyContinue
        Remove-Item (Join-Path $StateDir "$name.err.log") -ErrorAction SilentlyContinue
        if (Get-PortListenerPid $port) {
            throw "端口 $port 释放失败(旧 bridge port-forward PID=$ownerPid),请手动停掉后重试"
        }
        $ownerPid = $null   # 已释放,落到下面重建
    }
    if ($ownerPid) {
        # 端口被「非 bridge」进程占用 —— 会让 Envoy 连到旧后端,必须处理
        $desc = Get-ProcDesc $ownerPid
        if (Test-IsDockerBackendProc $ownerPid) {
            throw @"
端口 $port 的监听属于 Docker Desktop 转发进程($desc),说明还有容器在发布这个宿主端口
(预检 Stop-StaleComposeContainers 没拦到,可能是非 pandora-* 容器)。
不能杀该进程(会杀掉整个 Docker),请 `docker ps` 找到发布 $port 的容器手动 stop 后重试。
"@
        }
        if (-not $Force) {
            throw @"
端口 $port 已被非 bridge 进程占用:$desc
这通常是本地 go 服务(run_services.ps1)或 docker-compose 业务服务还在跑,
会导致 Envoy 连到旧后端而不是 k8s Service —— e2e 可能"假通过"。
请先停掉它们:
  pwsh tools/scripts/start.ps1 -Mode local  -Down
  pwsh tools/scripts/start.ps1 -Mode docker -Down
或给本脚本加 -Force(经 e2e_k8s.ps1 -BridgeForce 透传),让它杀掉占用者后重建。
"@
        }

        Write-Warn "端口 $port 被非 bridge 进程占用:$desc —— -Force 杀掉后重建"
        Stop-Process -Id $ownerPid -Force -ErrorAction SilentlyContinue
        for ($i = 0; $i -lt 10 -and (Get-PortListenerPid $port); $i++) { Start-Sleep -Milliseconds 300 }
        if (Get-PortListenerPid $port) {
            throw "端口 $port 释放失败(占用者 $desc),请手动停掉后重试"
        }
    }

    $log = Join-Path $StateDir "$name.log"
    $err = Join-Path $StateDir "$name.err.log"
    $proc = Start-Process kubectl -PassThru -WindowStyle Hidden -RedirectStandardOutput $log -RedirectStandardError $err -ArgumentList @(
        '--context', $KubeContext,
        'port-forward',
        '--namespace', $K8sNamespace,
        '--address', '127.0.0.1',
        "svc/$name",
        "${port}:${port}"
    )
    Set-Content -Path (Get-PidFile $name) -Value $proc.Id -Encoding ASCII

    for ($i = 0; $i -lt 10; $i++) {
        if ($proc.HasExited) {
            $stderr = if (Test-Path $err) { (Get-Content $err -Raw) } else { '' }
            $msg = "port-forward 启动失败 svc/${name}:$port`n$stderr"
            # 后端 Pod 没在 Running(Pending / ImagePullBackOff / CrashLoop)时 kubectl 会立刻退出。
            # 必需服务 → 直接失败;非必需服务 → 只 WARN 跳过,别拖垮整个 bridge / e2e。
            Remove-Item (Get-PidFile $name) -ErrorAction SilentlyContinue
            if ($essential) { throw $msg }
            Write-Warn "非必需服务 $name 不可用,跳过其 port-forward(不影响登录/Hub/匹配/Battle/结算闭环):`n$msg"
            return
        }
        # 确认占用 $port 的就是我们刚起的这个进程(而不是别的进程抢先 LISTEN)
        $nowPid = Get-PortListenerPid $port
        if ($nowPid -eq $proc.Id) {
            # 端口绑定成功还不够:port-forward 刚连上时后端可能还没 SERVING,
            # 且要防"端口在但底层 Pod 已失效"的假就绪。补真实 gRPC 健康探测。
            $health = Wait-PortForwardServing $port 3 500
            if ($health -eq 'serving') {
                Write-Ok "port-forward 就绪且 SERVING $name :127.0.0.1:$port (PID=$($proc.Id))"
                return
            }
            if ($health -eq 'nogrpcurl') {
                Write-Warn "port-forward 就绪 $name :127.0.0.1:$port (PID=$($proc.Id)),但无 grpcurl 无法确认 SERVING(建议 go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest)"
                return
            }
            # 端口绑上了但 gRPC 一直不 SERVING —— 必需服务失败,非必需只 WARN
            $hmsg = "port-forward 已绑定端口但 gRPC 未 SERVING svc/${name}:$port"
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            Remove-Item (Get-PidFile $name) -ErrorAction SilentlyContinue
            if ($essential) { throw $hmsg }
            Write-Warn "非必需服务 $name 不可用,跳过(不影响登录/Hub/匹配/Battle/结算闭环):$hmsg"
            return
        }
        if ($nowPid -and -not (Test-IsBridgePortForward $nowPid $name $port)) {
            Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
            throw "端口 $port 被其它进程($(Get-ProcDesc $nowPid))抢占,bridge port-forward 未能绑定"
        }
        Start-Sleep -Milliseconds 500
    }

    # 超时未绑定:必需服务报错;非必需服务杀掉残留 kubectl 后跳过
    $msg = "port-forward 超时未就绪 svc/${name}:$port"
    if ($essential) { throw $msg }
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    Remove-Item (Get-PidFile $name) -ErrorAction SilentlyContinue
    Write-Warn "非必需服务 $name 不可用,跳过其 port-forward(不影响登录/Hub/匹配/Battle/结算闭环):$msg"
}

# 全量 gRPC 健康校验(末尾兜底):对所有已建立 port-forward 的服务再打一次 Health/Check。
# 必需服务任一未 SERVING → 整个 bridge 失败(避免客户端点开局才吃 Envoy 503);
# 非必需服务未 SERVING → 只 WARN。缺 grpcurl 时跳过(前面已尽量保守重建)。
function Invoke-BridgeHealthSweep {
    if (-not (Get-GrpcurlPath)) {
        Write-Warn "未找到 grpcurl,跳过 bridge 全量健康校验(建议 go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest)"
        return
    }
    $failed = @()
    foreach ($f in $Forwards) {
        # 没有 pid 文件 = 该(非必需)服务被跳过,必需服务缺失早已 throw,这里不再校验
        if (-not (Test-Path (Get-PidFile $f.Name))) { continue }
        $health = Wait-PortForwardServing $f.Port 3 500
        if ($health -eq 'serving') {
            Write-Ok "健康校验 SERVING $($f.Name) :127.0.0.1:$($f.Port)"
        } elseif ($f.Essential) {
            Write-Warn "健康校验失败(必需)$($f.Name) :127.0.0.1:$($f.Port) 未 SERVING"
            $failed += "$($f.Name):$($f.Port)"
        } else {
            Write-Warn "健康校验失败(非必需,忽略)$($f.Name) :127.0.0.1:$($f.Port) 未 SERVING"
        }
    }
    if ($failed.Count -gt 0) {
        throw "bridge 全量健康校验失败,以下必需服务未 SERVING:$($failed -join ', ')"
    }
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Magenta
Write-Host " Pandora k8s Envoy bridge" -ForegroundColor Magenta
Write-Host "============================================" -ForegroundColor Magenta

$KubeContext = $KubeContext.Trim()
if ([string]::IsNullOrWhiteSpace($MinikubeProfile)) { $MinikubeProfile = $KubeContext }
$MinikubeProfile = $MinikubeProfile.Trim()
if (-not (Test-KubeContextIsLocalMinikube $KubeContext $MinikubeProfile)) {
    throw "kube-context '$KubeContext' 的 endpoint 不是本机 minikube profile '$MinikubeProfile'，拒绝建立可能指向远端集群的 port-forward。"
}

function Stop-HostInfraContainersForK8s {
    # k8s 模式使用集群内 infra；宿主 compose 的 MySQL/Redis/Kafka/etcd/监控不参与链路。
    # 切换模式时主动停掉它们，既释放资源，也确保升级前曾以 0.0.0.0 发布的旧容器不继续暴露。
    $infraServices = @('mysql', 'redis', 'zookeeper', 'kafka', 'etcd', 'prometheus', 'grafana', 'loki', 'alloy')
    docker compose -f $ComposeFile --env-file $EnvFile stop @infraServices *> $null
    if ($LASTEXITCODE -ne 0) { throw '停止 k8s 模式不需要的宿主基础设施容器失败' }
    Write-Ok '宿主 compose 基础设施已停止(k8s 使用集群内 infra)'
}

# `docker compose up --force-recreate` 会先停旧 Envoy 再立即创建新容器。Docker Desktop
# 在 Windows 上偶尔晚几秒才释放 8443/8444/9901；直接 up 会因此报 "ports are not available"，
# 即使 K8s rollout 和全部 gRPC health 已经成功。显式 stop 并观察端口释放后再 recreate。
# Compose 默认 project 名可能在多个 checkout 之间碰撞，因此不能只凭 service/project 名 stop。
# 必须先验证 service 标签与规范化 config_files 标签都精确属于当前仓库，再按不可变 container ID
# 停止；标签不匹配或端口仍占用都 fail-fast，绝不杀 Docker backend 或猜测性停止其它容器。
function Stop-EnvoyForRecreate {
    $containerIds = @(docker compose -f $ComposeFile --env-file $EnvFile ps -aq envoy 2>$null |
        ForEach-Object { "$_".Trim() } | Where-Object { $_ })
    if ($LASTEXITCODE -ne 0) { throw '查询当前 compose envoy 容器失败' }

    $expectedConfigFile = [IO.Path]::GetFullPath($ComposeFile)
    foreach ($containerId in $containerIds) {
        $labelsJson = docker inspect --format '{{json .Config.Labels}}' $containerId 2>$null
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace("$labelsJson")) {
            throw "读取 envoy 容器标签失败:$containerId"
        }
        try { $labels = "$labelsJson" | ConvertFrom-Json -ErrorAction Stop } catch {
            throw "envoy 容器标签不是有效 JSON:$containerId"
        }
        $serviceLabel = [string]$labels.'com.docker.compose.service'
        $configFilesLabel = [string]$labels.'com.docker.compose.project.config_files'
        try { $actualConfigFile = [IO.Path]::GetFullPath($configFilesLabel) } catch {
            throw "envoy 容器 config_files 标签不是有效路径:$containerId"
        }
        if ($serviceLabel -cne 'envoy' -or
            -not $actualConfigFile.Equals($expectedConfigFile, [StringComparison]::OrdinalIgnoreCase)) {
            throw "拒绝停止不属于当前 compose 文件的容器:$containerId service=$serviceLabel config_files=$configFilesLabel"
        }

        docker stop --time 10 $containerId | Out-Null
        if ($LASTEXITCODE -ne 0) { throw "停止已验证的旧 envoy 容器失败:$containerId" }
    }

    foreach ($port in @(8443, 8444, 9901)) {
        for ($i = 0; $i -lt 40 -and (Get-PortListenerPid $port); $i++) {
            Start-Sleep -Milliseconds 250
        }
        $ownerPid = Get-PortListenerPid $port
        if ($ownerPid) {
            throw "旧 envoy 停止后端口 $port 仍未释放(占用者 $(Get-ProcDesc $ownerPid));拒绝误杀进程,请检查 docker ps"
        }
    }
    Write-Ok '旧 Envoy 已停止，8443/8444/9901 已释放'
}
Write-Ok "port-forward context 已锁定:$KubeContext"

Ensure-File $ComposeFile
# dev.env 被 git 忽略(真实 webhook/token 不入库);新机器首次跑时按 example 初始化。
# 这段原来是本文件独有的内联实现,现已提到 dev_env_file.ps1 —— local / docker 走的
# dev_up.ps1 当时没有自举,同样的「新机器缺 dev.env」在那条路上是硬失败(2026-08-12 现场)。
Confirm-DevEnvFile -ProjectRoot $ProjectRoot
Ensure-File $EnvFile
Ensure-File (Join-Path $ProjectRoot 'deploy/envoy/envoy.yaml')
# cert.pem / key.pem 不止判存在:必须是有效 PEM(key.pem 损坏会让 Envoy 启动直接退出)。
# 缺失/无效时自动用 mkcert 补齐;mkcert 不在则抛出带修复指引的明确错误。
Confirm-EnvoyDevCert -EnvoyDir (Join-Path $ProjectRoot 'deploy/envoy')
New-Item -ItemType Directory -Force -Path $StateDir | Out-Null

Write-Step "[1/4] 预检:停掉 k8s 不需要的宿主 infra + 发布 500xx 的旧业务容器"
Stop-HostInfraContainersForK8s
Stop-StaleComposeContainers

Write-Step "[2/4] 启本地 kubectl port-forward"
foreach ($forward in $Forwards) {
    Start-PortForward $forward.Name $forward.Port $forward.Essential
}

Write-Step "[3/4] 全量 gRPC 健康校验(必需服务须 SERVING,否则 fail-fast)"
Invoke-BridgeHealthSweep

Write-Step "[4/4] 启 docker envoy(:8443 / :8444)"
# envoy.yaml 是静态配置,挂载文件变化不会让既有 Envoy 进程自动重读;每次桥接显式重建容器。
Stop-EnvoyForRecreate
docker compose -f $ComposeFile --env-file $EnvFile up -d --force-recreate envoy
if ($LASTEXITCODE -ne 0) { throw 'envoy 容器启动失败' }

Write-Host ""
Write-Ok '宿主 Envoy 桥接已就绪。UE 客户端经 127.0.0.1:8443 访问 k8s 服务；GameServer DS 回调走集群内 pandora-envoy:8444。'
