# Pandora 本地「线上等价」真 DS 闭环一键编排(minikube + Agones,无 mock)
#
# 串起 -Mode k8s 之后剩余的手工步骤,让 codex 一条命令把真 DS 链路准备好:
#   1) 校验 minikube / Agones / Fleet / 后端 allocator 就绪
#   2) 把两个真 UE Linux DS 镜像(精确 tag,从 Fleet yaml 动态解析)load 进 minikube
#   3) 起宿主 Envoy 桥接(kubectl port-forward 所有 envoy upstream + docker envoy :8443/:8444)
#   4) 等 Stable Fleet Ready，并验证 Canary Fleet 是 Ready 或显式 0 副本休眠
#   5) (可选)起容器版 UDP 回程中继(--network <minikube profile>;docker driver 下客户端连 DS 必需)
#   6) 打印端到端验收清单(用真 UE 客户端验:登录→hub→战斗→结算回 hub)
#
# 前置(由 start.ps1 -Mode k8s 完成):minikube 起、Agones 装好、RBAC/Fleet apply、21 个后端 Deployment 部署。
#   pwsh tools/scripts/start.ps1 -Mode k8s
#   # start.ps1 会按 live allocator 地址自动把正确的 RelayBindHost 传给本脚本
#
# 用法:
#   pwsh tools/scripts/e2e_k8s.ps1                 # 仅 allocator 广播 127.0.0.1 的本机模式
#   pwsh tools/scripts/e2e_k8s.ps1 -RelayBindHost 0.0.0.0 # allocator 广播非回环 IP 的局域网模式
#   pwsh tools/scripts/e2e_k8s.ps1 -NoRelay        # 不自动起容器版中继(自己按提示起 pandora/udp-relay:dev 容器)
#   pwsh tools/scripts/e2e_k8s.ps1 -SkipImageLoad -RelayBindHost 0.0.0.0 # 局域网模式只重建桥接/中继
#   pwsh tools/scripts/e2e_k8s.ps1 -TimeoutSec 300 # 等 Fleet Ready 的超时(默认 240s)
#   pwsh tools/scripts/e2e_k8s.ps1 -MinikubeProfile pandora-agones # 指定 minikube profile / docker network
#   pwsh tools/scripts/e2e_k8s.ps1 -BridgeForce    # 500xx 端口被本地/compose 旧服务占用时,杀掉后重建 port-forward

[CmdletBinding()]
param(
    [switch]$NoRelay,        # 不自动起容器版 UDP 中继
    [switch]$SkipImageLoad,  # 跳过 minikube image load
    # 集群上存在 Allocated(可能载人)GameServer 时,全量清场默认**中止**;只有显式传本开关
    # 才连 Allocated 一起删(§9.21 / 2026-08-03 误删载人 DS 整改:零秒警告不构成守卫,
    # 必须 fail-closed + 显式确认)。确认无真人对局(如只剩上一轮 e2e 的合成残留)时才传。
    [switch]$ForceDeleteAllocated,
    [switch]$BridgeForce,    # 端口被非 bridge 进程占用时,杀掉占用者后重建 port-forward
    [switch]$HostBridge,     # 强制走宿主 port-forward 桥(默认:集群内 pandora-edge-envoy 可用即跳过桥)
    [Alias('Profile')]
    # 默认序:显式参数 > PANDORA_MINIKUBE_PROFILE(与 start.ps1 Get-ActiveMinikubeProfile 同一覆盖口径,
    # 多 profile 并存时防打错集群)> 'pandora-agones' 历史默认。
    [string]$MinikubeProfile = $(if (-not [string]::IsNullOrWhiteSpace($env:PANDORA_MINIKUBE_PROFILE)) { $env:PANDORA_MINIKUBE_PROFILE.Trim() } else { 'pandora-agones' }), # minikube profile;docker driver 下通常同名 docker network
    [string]$KubeContext = '', # 必须指向上述本地 minikube；留空时取与 profile 同名的 context
    [ValidateSet('127.0.0.1', '0.0.0.0')]
    [string]$RelayBindHost = '127.0.0.1', # 安全默认仅本机;allocator 广播非回环地址时必须显式传 0.0.0.0
    [int]$TimeoutSec = 240   # 等 Fleet Ready 超时秒
)

$ErrorActionPreference = 'Stop'
$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path "$ScriptDir/../..").Path
$AgonesDir   = Join-Path $ProjectRoot 'deploy/k8s/agones'
$K8sNamespace = 'pandora'   # 后端服务 + allocator 所在 ns
$FleetNamespace = 'default' # Fleet / GameServer 所在 ns(见 20/30-fleet-*.yaml)

function Write-Info($m) { Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Write-Ok($m)   { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Write-Warn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Write-Err($m)  { Write-Host "[ERR ] $m" -ForegroundColor Red }
function Write-Step($m) { Write-Host "`n===== $m =====" -ForegroundColor Magenta }

function Test-CommandExists([string]$c) { return [bool](Get-Command $c -ErrorAction SilentlyContinue) }

function Resolve-MinikubeProfile([string]$profile) {
    if (-not [string]::IsNullOrWhiteSpace($profile)) { return $profile.Trim() }

    $ctx = (kubectl config current-context 2>$null | Out-String).Trim()
    if (-not [string]::IsNullOrWhiteSpace($ctx)) { return $ctx }

    return 'pandora-agones'
}

# 读取 profile 的节点容器运行时(docker/containerd/cri-o),缓存;解析失败回落 'docker'(旧行为)。
# 与 start.ps1 的 Get-MinikubeNodeRuntime 同构(独立脚本按仓库惯例自带副本);用于节点内
# untag 命令分流——containerd 节点没有 docker CLI,`ssh -- docker rmi` 会静默失败。
$script:E2eRuntimeCache = @{}
function Get-MinikubeNodeRuntimeE2e([string]$Profile) {
    if ($script:E2eRuntimeCache.ContainsKey($Profile)) { return $script:E2eRuntimeCache[$Profile] }
    $runtime = 'docker'
    $json = (& minikube profile list -o json 2>$null | Out-String)
    if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($json)) {
        try {
            foreach ($p in @(($json | ConvertFrom-Json).valid)) {
                if ([string]$p.Name -ceq $Profile) {
                    $cr = [string]$p.Config.KubernetesConfig.ContainerRuntime
                    if (-not [string]::IsNullOrWhiteSpace($cr)) { $runtime = $cr.Trim() }
                    break
                }
            }
        } catch { }
    }
    $script:E2eRuntimeCache[$Profile] = $runtime
    return $runtime
}

function Test-KubeContextIsLocalMinikube([string]$Context, [string]$Profile) {
    if ([string]::IsNullOrWhiteSpace($Context) -or [string]::IsNullOrWhiteSpace($Profile)) { return $false }
    $cluster = (kubectl config view -o jsonpath="{.contexts[?(@.name==`"$Context`")].context.cluster}" 2>$null)
    if ([string]::IsNullOrWhiteSpace($cluster)) { return $false }
    $server = (kubectl config view -o jsonpath="{.clusters[?(@.name==`"$cluster`")].cluster.server}" 2>$null)
    if ([string]::IsNullOrWhiteSpace($server)) { return $false }
    try { $apiHost = ([System.Uri]$server).Host } catch { return $false }
    if ($apiHost -in @('127.0.0.1', 'localhost', '::1', '[::1]')) { return $true }
    $mkIp = (minikube -p $Profile ip 2>$null | Out-String).Trim()
    return ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($mkIp) -and $apiHost -eq $mkIp)
}

# 只读取 allocator 的顶层 agones 段，不能误取后续 local_ds/local_hub 的同名字段。
function Get-AgonesAdvertiseHostFromText {
    param(
        [Parameter(Mandatory)][string]$ConfigText,
        [Parameter(Mandatory)][string]$ConfigName
    )

    $insideAgones = $false
    $advertiseHost = $null
    $advertiseHostSeen = $false
    foreach ($line in ($ConfigText -split "`r?`n")) {
        if (-not $insideAgones) {
            if ($line -match '^agones:\s*(?:#.*)?$') { $insideAgones = $true }
            continue
        }
        if ($line -match '^[^\s#]') { break }
        if ($line -match '^\s+advertise_host:\s*(?<value>[^#]+)') {
            if ($advertiseHostSeen) {
                throw "live Secret/pandora-config 中 $ConfigName 的 agones.advertise_host 重复"
            }
            $advertiseHostSeen = $true
            $value = $Matches['value'].Trim()
            if (($value.StartsWith('"') -and $value.EndsWith('"')) -or
                ($value.StartsWith("'") -and $value.EndsWith("'"))) {
                $value = $value.Substring(1, $value.Length - 2).Trim()
            }
            $advertiseHost = $value
        }
    }
    if ([string]::IsNullOrWhiteSpace($advertiseHost)) {
        throw "live Secret/pandora-config 中 $ConfigName 缺少 agones.advertise_host"
    }
    return $advertiseHost
}

# Secret 内容只在内存中解码；错误信息不得回显 JSON、Base64 或 YAML。
function Get-LiveAllocatorAdvertiseContract {
    param(
        [Parameter(Mandatory)][string[]]$ContextArgs,
        [Parameter(Mandatory)][string]$Namespace
    )

    $secretOutput = @(& kubectl @ContextArgs get secret pandora-config -n $Namespace -o json 2>$null)
    $secretExitCode = $LASTEXITCODE
    if ($secretExitCode -ne 0 -or $secretOutput.Count -eq 0) {
        throw "读取本地 context 的 Secret/$Namespace/pandora-config 失败；未改动 Fleet、镜像、桥接或中继"
    }
    try {
        $secret = ($secretOutput -join "`n") | ConvertFrom-Json
    } catch {
        throw "解析本地 Secret/$Namespace/pandora-config 元数据失败；未回显 Secret 内容"
    }
    if ($null -eq $secret.data) {
        throw "Secret/$Namespace/pandora-config 缺少 data"
    }

    $hosts = @{}
    foreach ($configName in @('ds-allocator.yaml', 'hub-allocator.yaml')) {
        $property = $secret.data.PSObject.Properties[$configName]
        if ($null -eq $property -or [string]::IsNullOrWhiteSpace([string]$property.Value)) {
            throw "Secret/$Namespace/pandora-config 缺少 $configName"
        }
        try {
            $configBytes = [Convert]::FromBase64String([string]$property.Value)
            $configText = [Text.Encoding]::UTF8.GetString($configBytes)
            $hosts[$configName] = Get-AgonesAdvertiseHostFromText -ConfigText $configText -ConfigName $configName
        } catch {
            if ($_.Exception.Message -like 'live Secret/pandora-config*') { throw }
            throw "Secret/$Namespace/pandora-config 的 $configName 无法安全解码"
        }
    }

    $uid = [string]$secret.metadata.uid
    $resourceVersion = [string]$secret.metadata.resourceVersion
    if ([string]::IsNullOrWhiteSpace($uid) -or [string]::IsNullOrWhiteSpace($resourceVersion)) {
        throw "Secret/$Namespace/pandora-config 缺少不可变身份或版本信息"
    }
    return [pscustomobject]@{
        Uid = $uid
        ResourceVersion = $resourceVersion
        DsAdvertiseHost = [string]$hosts['ds-allocator.yaml']
        HubAdvertiseHost = [string]$hosts['hub-allocator.yaml']
    }
}

function Assert-RelayBindingMatchesAdvertiseHosts {
    param(
        [Parameter(Mandatory)][string]$DsAdvertiseHost,
        [Parameter(Mandatory)][string]$HubAdvertiseHost,
        [Parameter(Mandatory)][string]$RequestedBindHost,
        [Parameter(Mandatory)][bool]$RelayBindWasExplicit
    )

    if ($DsAdvertiseHost -cne $HubAdvertiseHost) {
        throw "ds-allocator 与 hub-allocator 的 agones.advertise_host 不一致；拒绝重建桥接/中继"
    }
    $parsed = $null
    if (-not [Net.IPAddress]::TryParse($HubAdvertiseHost, [ref]$parsed) -or
        $parsed.AddressFamily -ne [Net.Sockets.AddressFamily]::InterNetwork -or
        $parsed.ToString() -cne $HubAdvertiseHost -or
        $HubAdvertiseHost -in @('0.0.0.0', '255.255.255.255')) {
        throw "allocator agones.advertise_host 必须是规范、可下发给客户端的 IPv4 地址"
    }

    $isLoopback = [Net.IPAddress]::IsLoopback($parsed)
    if ($isLoopback) {
        if ($HubAdvertiseHost -cne '127.0.0.1' -or $RequestedBindHost -cne '127.0.0.1') {
            throw "allocator 广播回环地址时，UDP relay 只能绑定 127.0.0.1"
        }
    } elseif (-not $RelayBindWasExplicit -or $RequestedBindHost -cne '0.0.0.0') {
        throw "allocator 广播非回环地址时，必须显式传 -RelayBindHost 0.0.0.0；现有桥接/中继保持不变"
    }
    return $HubAdvertiseHost
}

function Assert-AdvertiseHostOwnedByLocalMachine([string]$AdvertiseHost) {
    if ([Net.IPAddress]::IsLoopback([Net.IPAddress]::Parse($AdvertiseHost)) -or -not $IsWindows) { return }
    $localIPv4 = @(Get-NetIPAddress -AddressFamily IPv4 -ErrorAction Stop | ForEach-Object { [string]$_.IPAddress })
    if ($localIPv4 -cnotcontains $AdvertiseHost) {
        throw "allocator agones.advertise_host=$AdvertiseHost 当前不属于本机任何 IPv4 网卡；拒绝开放 UDP relay"
    }
}

function Assert-LiveAllocatorContractUnchanged {
    param(
        [Parameter(Mandatory)]$Baseline,
        [Parameter(Mandatory)][string[]]$ContextArgs,
        [Parameter(Mandatory)][string]$Namespace,
        [Parameter(Mandatory)][string]$RequestedBindHost,
        [Parameter(Mandatory)][bool]$RelayBindWasExplicit
    )

    $latest = Get-LiveAllocatorAdvertiseContract -ContextArgs $ContextArgs -Namespace $Namespace
    $latestHost = Assert-RelayBindingMatchesAdvertiseHosts `
        -DsAdvertiseHost $latest.DsAdvertiseHost `
        -HubAdvertiseHost $latest.HubAdvertiseHost `
        -RequestedBindHost $RequestedBindHost `
        -RelayBindWasExplicit $RelayBindWasExplicit
    if ($latest.Uid -cne $Baseline.Uid -or $latest.ResourceVersion -cne $Baseline.ResourceVersion) {
        throw "Secret/$Namespace/pandora-config 在编排过程中已变化；拒绝继续重建桥接/中继"
    }
    Assert-AdvertiseHostOwnedByLocalMachine $latestHost
}

function ConvertTo-IPv4UInt32([string]$ip) {
    $parts = $ip.Split('.')
    if ($parts.Count -ne 4) { throw "不是 IPv4 地址: $ip" }
    $value = [uint32]0
    foreach ($part in $parts) {
        $octet = [int]$part
        if ($octet -lt 0 -or $octet -gt 255) { throw "不是 IPv4 地址: $ip" }
        $value = ($value -shl 8) -bor [uint32]$octet
    }
    return $value
}

function Test-IPv4InCidr([string]$ip, [string]$cidr) {
    $cidrParts = $cidr.Split('/')
    if ($cidrParts.Count -ne 2) { return $false }

    $maskBits = [int]$cidrParts[1]
    if ($maskBits -lt 0 -or $maskBits -gt 32) { return $false }

    $ipValue = ConvertTo-IPv4UInt32 $ip
    $networkValue = ConvertTo-IPv4UInt32 $cidrParts[0]
    if ($maskBits -eq 0) { return $true }

    $mask = ([uint32]::MaxValue) -shl (32 - $maskBits)
    return (($ipValue -band $mask) -eq ($networkValue -band $mask))
}

# 从 Fleet yaml 里解析 image:(精确 tag,避免脚本写死过时)
function Get-FleetImage([string]$yamlRelPath) {
    $path = Join-Path $ProjectRoot $yamlRelPath
    if (-not (Test-Path $path)) { throw "找不到 Fleet 文件: $yamlRelPath" }
    $line = Select-String -Path $path -Pattern '^\s*image:\s*(\S+)' | Select-Object -First 1
    if (-not $line) { throw "未在 $yamlRelPath 解析到 image:" }
    return $line.Matches[0].Groups[1].Value
}

# 读取指定 Fleet 当前 GameServer 对象。调用时 kubectlContextArgs 已初始化。
function Get-FleetGameServerItems([string]$fleet) {
    $json = (kubectl @kubectlContextArgs get gameservers -n $FleetNamespace `
        -l "agones.dev/fleet=$fleet" -o json 2>$null | Out-String)
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($json)) {
        throw "读取 Fleet '$fleet' 的 GameServer 失败"
    }
    return @((ConvertFrom-Json $json).items)
}

function Start-K8sEnvoyBridge {
    $bridgeScript = Join-Path $ScriptDir 'k8s_envoy_bridge.ps1'
    if (-not (Test-Path $bridgeScript)) { throw "缺少桥接脚本: $bridgeScript" }
    # 客户端面跟随 UDP 中继:RelayBindHost=0.0.0.0(内网多机)时只开放 8443。
    # k8s GameServer 经集群内 pandora-envoy 回连,宿主 DS 面 8444 永远只绑回环;
    # 显式覆盖父环境遗留值,不能只依赖 compose 默认值。admin 9901 同样恒绑本机。
    $env:PANDORA_EDGE_BIND_HOST = if ($RelayBindHost -eq '0.0.0.0') { '0.0.0.0' } else { '127.0.0.1' }
    $env:PANDORA_DS_EDGE_BIND_HOST = '127.0.0.1'
    $bridgeParams = @{
        KubeContext = $KubeContext
        MinikubeProfile = $MinikubeProfile
    }
    if ($BridgeForce) { $bridgeParams.Force = $true }
    & $bridgeScript @bridgeParams
    if ($LASTEXITCODE -ne 0) { throw "k8s Envoy 桥接启动失败" }
}

Write-Host ""
Write-Host "============================================" -ForegroundColor Magenta
Write-Host " Pandora 真 DS 闭环编排 (minikube + Agones)" -ForegroundColor Magenta
Write-Host "============================================" -ForegroundColor Magenta

# ── 0) 工具与集群就绪校验 ───────────────────────────────────────────────
Write-Step "[0/6] 校验 minikube / kubectl / Agones / 后端就绪"
foreach ($c in @('kubectl', 'minikube', 'docker')) {
    if (-not (Test-CommandExists $c)) { Write-Err "$c 未安装。先跑 start.ps1 -Mode k8s -Install。"; exit 1 }
}
$MinikubeProfile = Resolve-MinikubeProfile $MinikubeProfile
$KubeContext = if ([string]::IsNullOrWhiteSpace($KubeContext)) { $MinikubeProfile } else { $KubeContext.Trim() }
$MinikubeDockerNetwork = $MinikubeProfile
Write-Info "minikube profile: $MinikubeProfile"
minikube -p $MinikubeProfile status *> $null
if ($LASTEXITCODE -ne 0) { Write-Err "minikube profile '$MinikubeProfile' 未在运行。先跑:pwsh tools/scripts/start.ps1 -Mode k8s"; exit 1 }
Write-Ok "minikube profile '$MinikubeProfile' 运行中"

$contexts = @(kubectl config get-contexts -o name 2>$null)
if ($LASTEXITCODE -ne 0 -or $contexts -cnotcontains $KubeContext) {
    Write-Err "kubeconfig 中找不到 context '$KubeContext'，已中止。"
    exit 1
}
if (-not (Test-KubeContextIsLocalMinikube $KubeContext $MinikubeProfile)) {
    Write-Err "context '$KubeContext' 的 endpoint 不是本机 minikube profile '$MinikubeProfile'，为防误操作远端集群已中止。"
    exit 1
}
$kubectlContextArgs = @('--context', $KubeContext)
Write-Ok "kubectl 已显式钉在本机 context '$KubeContext'(不修改全局 current-context)"

# 在任何 Fleet apply、镜像 load/delete、桥接或中继改动前锁定 live allocator/relay 合约。
$relayBindWasExplicit = $PSBoundParameters.ContainsKey('RelayBindHost')
try {
    $relayContract = Get-LiveAllocatorAdvertiseContract -ContextArgs $kubectlContextArgs -Namespace $K8sNamespace
    $LiveAdvertiseHost = Assert-RelayBindingMatchesAdvertiseHosts `
        -DsAdvertiseHost $relayContract.DsAdvertiseHost `
        -HubAdvertiseHost $relayContract.HubAdvertiseHost `
        -RequestedBindHost $RelayBindHost `
        -RelayBindWasExplicit $relayBindWasExplicit
    Assert-AdvertiseHostOwnedByLocalMachine $LiveAdvertiseHost
    Write-Ok "live allocator/relay 合约通过:advertise=$LiveAdvertiseHost,bind=$RelayBindHost"
} catch {
    Write-Err $_.Exception.Message
    exit 1
}

kubectl @kubectlContextArgs get ns agones-system *> $null
if ($LASTEXITCODE -ne 0) { Write-Err "未检测到 Agones(agones-system)。先跑:pwsh tools/scripts/start.ps1 -Mode k8s"; exit 1 }
Write-Ok "Agones 已安装"

# 四条 Fleet 在不在(start.ps1 -Mode k8s 已 apply)
$fleetNames = @('pandora-battle-stable', 'pandora-battle-canary', 'pandora-hub-stable', 'pandora-hub-canary')
$missingFleets = @()
foreach ($fleet in $fleetNames) {
    kubectl @kubectlContextArgs get fleet $fleet -n $FleetNamespace *> $null
    if ($LASTEXITCODE -ne 0) { $missingFleets += $fleet }
}
if ($missingFleets.Count -gt 0) {
    Write-Warn "Fleet 不全(missing=$($missingFleets -join ',')),尝试 apply..."
    foreach ($manifest in @('10-rbac-allocator.yaml', '20-fleet-battle.yaml', '21-fleet-battle-canary.yaml', '30-fleet-hub.yaml', '31-fleet-hub-canary.yaml')) {
        kubectl @kubectlContextArgs apply -f (Join-Path $AgonesDir $manifest)
        if ($LASTEXITCODE -ne 0) { Write-Err "apply $manifest 失败"; exit 1 }
    }
}

# allocator 部署在不在 + 是否 agones 模式(读 configmap)
kubectl @kubectlContextArgs get deploy ds-allocator hub-allocator -n $K8sNamespace *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Warn "ds-allocator / hub-allocator 未部署。先跑:pwsh tools/scripts/start.ps1 -Mode k8s"
}

# ── 1) load 两个真 UE Linux DS 镜像(精确 tag) ──────────────────────────
$battleImg = Get-FleetImage 'deploy/k8s/agones/20-fleet-battle.yaml'
$hubImg    = Get-FleetImage 'deploy/k8s/agones/30-fleet-hub.yaml'
Write-Step "[1/6] load 真 DS 镜像进 minikube"
Write-Info "battle: $battleImg"
Write-Info "hub:    $hubImg"

if ($SkipImageLoad) {
    Write-Warn "已传 -SkipImageLoad,跳过 image load"
} else {
    foreach ($img in @($battleImg, $hubImg)) {
        # 校验宿主 docker 里有该镜像；随后复用 start.ps1 的强制刷新策略：节点内先 rmi -f
        # 仅解绑旧 tag（不影响运行容器），再 overwrite load，避免 :dev 同名 tag 静默复用旧包。
        $hostId = (docker image inspect -f '{{.Id}}' $img 2>$null | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or -not $hostId) {
            Write-Err "宿主 docker 没有镜像 $img"
            Write-Warn "  该镜像由 UE 侧 Linux DS 包构建(deploy/ds/build-image.sh)。"
            Write-Warn "  构建后重跑;或若 tag 不同,改 Fleet yaml 的 image: 再重试。"
            exit 1
        }
        Write-Info "  强制刷新 minikube tag:$img"
        # untag 按节点 runtime 分流(containerd 节点无 docker CLI);节点内 tag 确认统一走
        # `minikube image ls`(runtime 无关),与 start.ps1 Sync-ImagesToMinikube 同口径。
        if ((Get-MinikubeNodeRuntimeE2e $MinikubeProfile) -ceq 'docker') {
            minikube -p $MinikubeProfile ssh -- docker rmi -f $img 2>$null | Out-Null
        } else {
            minikube -p $MinikubeProfile ssh -- sudo crictl rmi "docker.io/$img" 2>$null | Out-Null
        }
        minikube -p $MinikubeProfile image load --daemon=true --overwrite=true $img
        if ($LASTEXITCODE -ne 0) { Write-Err "load 失败: $img"; exit 1 }
        $mkTagJson = (minikube -p $MinikubeProfile image ls --format json 2>$null | Out-String)
        $mkTagHit = $false
        if (-not [string]::IsNullOrWhiteSpace($mkTagJson)) {
            try {
                foreach ($entry in @(($mkTagJson | ConvertFrom-Json))) {
                    foreach ($tag in @($entry.repoTags)) {
                        if (($tag -replace '^docker\.io/', '') -ceq $img) { $mkTagHit = $true; break }
                    }
                    if ($mkTagHit) { break }
                }
            } catch { }
        }
        if (-not $mkTagHit) {
            Write-Err "强制刷新后 minikube 节点仍找不到 tag:$img"
            exit 1
        }
    }
    Write-Ok "DS 镜像 tag 已强制刷新并在节点内确认存在"

    $oldUidSet = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
    foreach ($fleet in $fleetNames) {
        foreach ($item in @(Get-FleetGameServerItems $fleet)) {
            if ($item.metadata.uid) { [void]$oldUidSet.Add([string]$item.metadata.uid) }
        }
    }
    # 只要执行过 image load，就必须按本项目四支 Fleet 精确重建 GameServer。即使 load 一次成功、
    # tag 校验已匹配，运行中的旧 Pod 也不会自动换镜像；若不删会把旧 Ready 实例误判为新包就绪。
    #
    # ⚠️ 本删除是 §8 压测纪律授权的**测试集群全量重置**(跑测前清空 GameServer),会连
    # Allocated 一起删。它只允许出现在 e2e/压测清场;任何日常部署/换镜像路径都必须走
    # start.ps1 的「只删非 Allocated」逻辑(§9.21:禁止删除仍承载玩家的 Allocated DS,
    # 2026-08-03 有误删载人 DS 实锤)。
    # 存在 Allocated 时 fail-closed 中止(对抗审查 P2:打印警告后同秒即删是零秒窗口,
    # 拦不住任何人;本脚本还常被无人值守执行,目视机会连受众都没有)。真要连 Allocated
    # 一起清,必须显式传 -ForceDeleteAllocated——决定权交给确认过"无真人对局"的人。
    $allocatedByFleet = @{}
    foreach ($fleet in $fleetNames) {
        $names = @(Get-FleetGameServerItems $fleet | Where-Object { [string]$_.status.state -ceq 'Allocated' } |
            ForEach-Object { $_.metadata.name })
        if ($names.Count -gt 0) { $allocatedByFleet[$fleet] = $names }
    }
    if ($allocatedByFleet.Count -gt 0 -and -not $ForceDeleteAllocated) {
        foreach ($fleet in $allocatedByFleet.Keys) {
            Write-Err "Fleet '$fleet' 存在 $($allocatedByFleet[$fleet].Count) 台 Allocated GameServer(可能载人):$($allocatedByFleet[$fleet] -join ', ')"
        }
        Write-Err "e2e 全量清场会删除上述 Allocated 实例;为防误删载人 DS(§9.21,2026-08-03 实锤)已中止。"
        Write-Err "确认集群上没有真人对局(例如只剩上一轮 e2e 的合成残留)后,显式加 -ForceDeleteAllocated 重跑。"
        exit 1
    }
    if ($allocatedByFleet.Count -gt 0) {
        foreach ($fleet in $allocatedByFleet.Keys) {
            Write-Warn "-ForceDeleteAllocated 已指定:Fleet '$fleet' 的 Allocated 实例将随全量重置删除:$($allocatedByFleet[$fleet] -join ', ')"
        }
    }
    Write-Warn "DS 镜像已刷新，按 Fleet 标签删除现存 GameServer，让 Agones 用新镜像重建..."
    foreach ($fleet in $fleetNames) {
        if ($ForceDeleteAllocated) {
            # 显式授权的全量重置:按 label 批删(连 Allocated)。仅限确认无真人对局的测试集群。
            kubectl @kubectlContextArgs delete gameservers -n $FleetNamespace -l "agones.dev/fleet=$fleet" --ignore-not-found
            if ($LASTEXITCODE -ne 0) {
                Write-Err "删除 Fleet '$fleet' 的旧 GameServer 失败；不能继续用旧 Ready 实例假报成功。"
                exit 1
            }
            continue
        }
        # 非 force 路径(对抗审查 P2 整改):上方门检到这里存在秒级窗口,窗口内可能出现
        # 新的 Allocated(分配在途)。改为逐台按名删除并在删前复查状态:复查失败或发现
        # Allocated 一律中止(与门检同一 fail-closed 语义),绝不静默连删。复查→删除仍有
        # 毫秒级残余窗口(kubectl 表达不了 precondition),被删的只可能是「复查后毫秒内
        # 刚被选中」的分配——分配尚在 warming、未向 matchmaker 放行 ds_addr,分配失败由
        # 后端既有补偿链重排,不产生玩家可见损伤。
        foreach ($item in @(Get-FleetGameServerItems $fleet)) {
            $gsName = [string]$item.metadata.name
            if ([string]::IsNullOrWhiteSpace($gsName)) { continue }
            $curState = (kubectl @kubectlContextArgs get gameserver $gsName -n $FleetNamespace --ignore-not-found -o jsonpath='{.status.state}' 2>$null | Out-String).Trim()
            if ($LASTEXITCODE -ne 0) {
                Write-Err "复查 GameServer '$gsName' 状态失败；无法证明其非 Allocated,绝不盲删,已中止。"
                exit 1
            }
            if ($curState -ceq 'Allocated') {
                Write-Err "GameServer '$gsName' 在清场窗口内变为 Allocated(可能载人)；为防误删已中止(§9.21)。确认无真人对局后用 -ForceDeleteAllocated 重跑。"
                exit 1
            }
            kubectl @kubectlContextArgs delete gameserver $gsName -n $FleetNamespace --ignore-not-found
            if ($LASTEXITCODE -ne 0) {
                Write-Err "删除 Fleet '$fleet' 的旧 GameServer '$gsName' 失败；不能继续用旧 Ready 实例假报成功。"
                exit 1
            }
        }
    }
    # delete 返回后仍显式确认旧 UID 已全部消失，再进入 Ready 等待；否则 Fleet status 的短暂旧值
    # 可能让后面立即假通过。
    $uidDeadline = (Get-Date).AddSeconds(60)
    do {
        $remainingOld = @()
        foreach ($fleet in $fleetNames) {
            $remainingOld += @(Get-FleetGameServerItems $fleet | Where-Object {
                $_.metadata.uid -and $oldUidSet.Contains([string]$_.metadata.uid)
            })
        }
        if ($remainingOld.Count -eq 0) { break }
        Start-Sleep -Seconds 1
    } while ((Get-Date) -lt $uidDeadline)
    if ($remainingOld.Count -gt 0) {
        Write-Err "旧 GameServer UID 在 60s 内未全部消失，拒绝把旧实例当作新镜像实例。"
        exit 1
    }
    Write-Ok "两个 DS 镜像已 load，四条 Fleet 的旧 GameServer 已重建"
}

# ── 2) 起宿主 Envoy 桥接 ────────────────────────────────────────────────
Write-Step "[2/6] 客户端面接入(集群内边缘 Envoy,回落宿主桥接)"
try {
    Assert-LiveAllocatorContractUnchanged -Baseline $relayContract -ContextArgs $kubectlContextArgs `
        -Namespace $K8sNamespace -RequestedBindHost $RelayBindHost `
        -RelayBindWasExplicit $relayBindWasExplicit
} catch {
    Write-Err $_.Exception.Message
    exit 1
}
# 2026-07-28 P2:退役本机 port-forward 桥。集群内已部署 pandora-edge-envoy(NodePort 31443)
# 且 minikube 节点容器发布了宿主 8443(-Reset 重建时经 PANDORA_MINIKUBE_PORTS 携带)时,
# 客户端仍连 127.0.0.1:8443,链路变为 宿主 8443 → 节点 31443 → 集群内 Envoy → Service,
# 无需 21 条 kubectl port-forward + 宿主 compose envoy。两条件任一不满足(其它机器/未重建的
# 旧集群)自动回落宿主桥接——他人路径不变;-HostBridge 强制走旧路径。
$edgeInCluster = $false
if (-not $HostBridge) {
    kubectl @kubectlContextArgs get svc pandora-edge-envoy -n $K8sNamespace *> $null
    if ($LASTEXITCODE -eq 0) {
        $nodePortMap = (docker port $MinikubeProfile 2>$null | Out-String)
        $edgeBind = [regex]::Match($nodePortMap, '(?m)^31443/tcp\s*->\s*(\S+):8443')
        if ($edgeBind.Success) {
            $edgeInCluster = $true
            $edgeBindHost = $edgeBind.Groups[1].Value
            # 端口发布绑定与本次意图对齐提示:回环发布下本机链路完整,但内网其它机器连不到
            # 8443(UDP 中继照常 0.0.0.0,不受影响)。不因内网诉求牺牲本机可用性,只如实告警;
            # 注意此时宿主桥也救不了——8443 已被回环发布占用,bridge 的 0.0.0.0 绑定会 EADDRINUSE。
            if ($RelayBindHost -eq '0.0.0.0' -and $edgeBindHost -notin @('0.0.0.0', '[::]', '::')) {
                Write-Warn "集群内边缘 8443 端口发布仅绑 $edgeBindHost —— 本机可用,内网其它机器连不到业务面 8443。"
                Write-Warn "  需内网多机时:`$env:PANDORA_MINIKUBE_PORTS='0.0.0.0:8443:31443' 后 start.ps1 -Mode k8s -Reset 重建集群。"
            }
        }
    }
}
if ($edgeInCluster) {
    Write-Ok "集群内边缘 Envoy 生效(宿主 ${edgeBindHost}:8443 → 节点 31443 → pandora-edge-envoy),跳过宿主桥接。"
} else {
    Start-K8sEnvoyBridge
}

# ── 3) 等 Fleet Ready ──────────────────────────────────────────────────
Write-Step "[3/6] 等 Fleet Ready(超时 ${TimeoutSec}s)"
function Wait-FleetReady([string]$fleet, [int]$timeoutSec) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $desired = (kubectl @kubectlContextArgs get fleet $fleet -n $FleetNamespace -o jsonpath='{.spec.replicas}' 2>$null)
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($desired) -or [int]$desired -lt 0) {
            Write-Err "读取 Fleet '$fleet' 的期望副本数失败或为负数，不能假验收。"
            return $false
        }
        $items = @(Get-FleetGameServerItems $fleet)
        $expectedTrack = if ($fleet -like '*-canary') { 'canary' } else { 'stable' }
        $wrongTrack = @($items | Where-Object { [string]$_.metadata.labels.'pandora.dev/release-track' -cne $expectedTrack })
        if ($wrongTrack.Count -gt 0) {
            Write-Err "$fleet 有 GameServer release-track 漂移，expected=$expectedTrack"
            return $false
        }
        $ready = @($items | Where-Object { $_.status.state -eq 'Ready' }).Count
        if ([int]$desired -eq 0 -and $ready -eq 0) {
            Write-Ok "$fleet 休眠:Ready=0/0（不接新分配）"
            return $true
        }
        if ($ready -ge [int]$desired) { Write-Ok "$fleet Ready=$ready/$desired"; return $true }
        Write-Host "  $fleet actual Ready GameServers=$ready/$desired ..." -ForegroundColor DarkGray
        Start-Sleep -Seconds 5
    }
    Write-Err "$fleet 在 ${timeoutSec}s 内未就绪"
    kubectl @kubectlContextArgs get gameservers -n $FleetNamespace -L agones.dev/fleet 2>$null
    Write-Warn "排查:kubectl --context $KubeContext describe fleet $fleet -n $FleetNamespace;kubectl --context $KubeContext logs <gs-pod> -n $FleetNamespace -c $fleet-ds"
    return $false
}
$fleetReady = @($fleetNames | ForEach-Object { Wait-FleetReady $_ $TimeoutSec })
if (@($fleetReady | Where-Object { -not $_ }).Count -gt 0) {
    Write-Err "Fleet 未全就绪,中止(DS 镜像未进 Ready 多半是 Agones SDK 未调通,看上面排查指引)。"
    exit 1
}

# ── 4) UDP 回程中继(docker driver 必需:容器版,挂 minikube profile 对应网络) ──────────
# 为什么必须用容器版而不是 Windows 进程版:
#   Windows + Docker Desktop + minikube docker driver 下,minikube 节点是跑在 Docker Desktop
#   Linux VM 里的容器,IP 在 profile 对应 docker 网络(如 pandora-agones / 192.168.58.0/24)。Windows 宿主进程的
#   UDP 直发 192.168.49.2:<port> 不可路由 —— relay 收到包但 minikube 节点 hostPort DNAT 不增长,
#   DS 收不到连接。把 relay 跑成容器并 `--network <profile>`,它才能直连 minikube 节点 IP;再用
#   `-p <RelayBindHost>:7000-8000:.../udp` 让 Docker Desktop 把宿主 UDP 转进容器。
#     client -> <RelayBindHost>:<port>(Win) -[Docker Desktop]-> relay 容器 -[minikube net]-> <minikube ip>:<port> -> DS
Write-Step "[4/6] UDP 回程中继(dockerized,--network $MinikubeDockerNetwork)"

function Stop-HostUdpRelay {
    # 杀掉旧的 Windows 进程版中继(udp_relay.ps1 / go run tools/udp-relay),避免占 127.0.0.1:7000-8000
    $procs = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object { $_.CommandLine -and ($_.CommandLine -match 'udp_relay\.ps1' -or $_.CommandLine -match 'udp-relay') }
    foreach ($p in $procs) {
        # 别误杀本脚本自己/编辑器
        if ($p.ProcessId -eq $PID) { continue }
        Write-Warn "  停掉旧 Windows 进程版中继 PID=$($p.ProcessId)"
        Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
    }
}

if ($NoRelay) {
    Write-Warn "已传 -NoRelay,未自动起中继。需手动起【容器版】中继(挂 $MinikubeDockerNetwork 网络):"
    Write-Warn "  docker build -t pandora/udp-relay:dev tools/udp-relay"
    Write-Warn "  docker run -d --name pandora-udp-relay --network $MinikubeDockerNetwork -p ${RelayBindHost}:7000-8000:7000-8000/udp -e TARGET_HOST=`$(minikube -p $MinikubeProfile ip) -e PORT_RANGE=7000-8000 pandora/udp-relay:dev"
} else {
    $relayImage = 'pandora/udp-relay:dev'
    $relayName  = 'pandora-udp-relay'
    $relayRange = '7000-8000'
    $relayDir   = Join-Path $ProjectRoot 'tools/udp-relay'

    # 4.1 解析 minikube 节点 IP 作为转发目标(容器在 profile 对应 docker 网络内可达)
    $relayTarget = (& minikube -p $MinikubeProfile ip 2>$null | Out-String).Trim()
    if ([string]::IsNullOrWhiteSpace($relayTarget)) {
        Write-Err "无法解析 minikube -p $MinikubeProfile ip,容器版中继无法确定 TARGET_HOST。先确认 minikube profile 在跑。"
        exit 1
    }
    Write-Info "  TARGET_HOST(minikube 节点)= $relayTarget"

    # 4.2 确认 profile 对应 docker 网络存在,且 target IP 落在该网络 subnet 内
    $networkJson = (docker network inspect $MinikubeDockerNetwork 2>$null | Out-String)
    if ($LASTEXITCODE -ne 0) {
        Write-Err "未找到 docker 网络 '$MinikubeDockerNetwork' —— 当前 minikube profile '$MinikubeProfile' 可能不是 docker driver,或 Docker 里残留/切到了其它 profile。容器版中继不适用。"
        exit 1
    }
    $network = ($networkJson | ConvertFrom-Json | Select-Object -First 1)
    $subnets = @($network.IPAM.Config | ForEach-Object { $_.Subnet } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $ipv4Subnets = @($subnets | Where-Object { $_ -match '^\d+\.\d+\.\d+\.\d+/\d+$' })
    if ($subnets.Count -eq 0) {
        Write-Warn "docker 网络 '$MinikubeDockerNetwork' 没有可校验的 IPAM subnet,跳过 TARGET_HOST/subnet 校验。"
    } elseif ($ipv4Subnets.Count -eq 0) {
        Write-Warn "docker 网络 '$MinikubeDockerNetwork' 没有可校验的 IPv4 subnet,跳过 TARGET_HOST/subnet 校验。原始 subnet: $($subnets -join ', ')"
    } else {
        $targetInNetwork = $false
        foreach ($subnet in $ipv4Subnets) {
            if (Test-IPv4InCidr $relayTarget $subnet) { $targetInNetwork = $true; break }
        }
        if (-not $targetInNetwork) {
            Write-Err "TARGET_HOST $relayTarget 不属于 docker 网络 '$MinikubeDockerNetwork' 的 IPv4 subnet: $($ipv4Subnets -join ', ')。"
            Write-Warn "这通常表示 relay 将挂到错误 Docker 网络,客户端会登录成功但 UDP 进不了 Hub DS。"
            Write-Warn "请确认 minikube profile '$MinikubeProfile' 与 docker network '$MinikubeDockerNetwork' 匹配,必要时清理旧 minikube network 后重启。"
            exit 1
        }
        Write-Ok "docker 网络 '$MinikubeDockerNetwork' subnet 校验通过: TARGET_HOST $relayTarget ∈ $($ipv4Subnets -join ', ')"
    }

    # 4.3 构建中继镜像(纯标准库,很快)
    Write-Info "  docker build $relayImage ..."
    docker build -t $relayImage $relayDir
    if ($LASTEXITCODE -ne 0) { Write-Err "中继镜像构建失败"; exit 1 }

    # 4.4 构建完成后再最终复核 Secret，随后才停止旧 relay；失败时保留原有可用链路。
    try {
        Assert-LiveAllocatorContractUnchanged -Baseline $relayContract -ContextArgs $kubectlContextArgs `
            -Namespace $K8sNamespace -RequestedBindHost $RelayBindHost `
            -RelayBindWasExplicit $relayBindWasExplicit
    } catch {
        Write-Err $_.Exception.Message
        exit 1
    }
    Stop-HostUdpRelay
    docker rm -f $relayName *> $null

    # 4.5 起容器:挂 profile 对应 docker 网络 + 发布宿主 $RelayBindHost:7000-8000/udp
    #     RelayBindHost=127.0.0.1 仅本机自测;=0.0.0.0 时对局域网开放(内网多机联调,需宿主防火墙放行 UDP 7000-8000)。
    Write-Info "  docker run --network $MinikubeDockerNetwork -p ${RelayBindHost}:${relayRange}:${relayRange}/udp(发布 1001 个 UDP 端口,稍慢)..."
    docker run -d --name $relayName --network $MinikubeDockerNetwork `
        -p "${RelayBindHost}:${relayRange}:${relayRange}/udp" `
        -e "TARGET_HOST=$relayTarget" `
        -e "PORT_RANGE=$relayRange" `
        $relayImage *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Err "中继容器启动失败(端口被占用 / docker 网络 '$MinikubeDockerNetwork' 不存在)。日志:"
        docker logs $relayName 2>$null
        exit 1
    }
    Start-Sleep -Seconds 2
    $relayRunning = (docker inspect -f '{{.State.Running}}' $relayName 2>$null)
    if ($relayRunning -ne 'true') {
        Write-Err "中继容器未处于 Running。日志:"
        docker logs $relayName 2>$null
        exit 1
    }
    Write-Ok "UDP 中继(dockerized)已启动:容器 $relayName,--network $MinikubeDockerNetwork,转发 ${RelayBindHost}:$relayRange/udp -> ${relayTarget}:$relayRange"
    Write-Info "  停止:docker rm -f $relayName    查看:docker logs -f $relayName"
    $verifyHost = $LiveAdvertiseHost
    Write-Info "  验证:发 UDP 到 ${verifyHost}:<port> 后,minikube ssh 里 'sudo iptables -t nat -L -n -v | grep dpt:<port>' 的 DNAT 计数应增长。"
}

# ── 5) 现状打印 ────────────────────────────────────────────────────────
Write-Step "[5/6] 当前 Fleet / GameServer"
kubectl @kubectlContextArgs get fleet -n $FleetNamespace
kubectl @kubectlContextArgs get gameservers -n $FleetNamespace -L agones.dev/fleet,pandora.dev/region

# ── 6) 端到端验收清单 ──────────────────────────────────────────────────
Write-Step "[6/6] 真 DS 闭环验收(用真 UE 客户端)"
$clientHost = $LiveAdvertiseHost
Write-Host @"
  后端入口(Envoy):客户端面 ${clientHost}:8443
  DS 回调入口:GameServer Pod → pandora-envoy.pandora.svc.cluster.local:8444
  宿主 DS 面:127.0.0.1:8444(仅本机调试,不对局域网开放)
  确认客户端 DefaultGame.ini 后端指向本机 Envoy,然后:

  [ ] 登录 -> AssignHub 返回真 hub-ds 地址 -> ClientTravel 进大厅地图(MainCity)
  [ ] 匹配 ConfirmMatch -> matchmaker 调 AllocateBattle -> Agones 分配真 battle-ds -> 进战斗地图(MobaLevel)
  [ ] 打完 -> DS 上报 ReportResult 结算 -> ClientTravel 回大厅 hub-ds
  [ ] allocator 日志:allocator_mode=agones / fleet_mode=agones(不是 mock)

  实时观察:
    kubectl --context $KubeContext get gameservers -n $FleetNamespace -w
    kubectl --context $KubeContext logs deploy/ds-allocator  -n $K8sNamespace -f
    kubectl --context $KubeContext logs deploy/hub-allocator -n $K8sNamespace -f
"@ -ForegroundColor Gray

Write-Host ""
Write-Ok "真 DS 闭环环境就绪。按上面清单用真 UE 客户端验证。"
