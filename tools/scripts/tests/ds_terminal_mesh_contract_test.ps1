[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$ComponentDir = Join-Path $ProjectRoot 'deploy/k8s/overlays/online/ds-terminal-mesh'
$SharedIdentityDir = Join-Path $ProjectRoot 'deploy/k8s/overlays/online/mesh-shared-identity'
$PolicyPath = Join-Path $ComponentDir 'authorization-policy.yaml'
$PeerPath = Join-Path $ComponentDir 'peer-authentication.yaml'
$IdentityPath = Join-Path $ComponentDir 'workload-identity.yaml'
$ServiceAccountPath = Join-Path $SharedIdentityDir 'serviceaccount.yaml'
$ServicesPath = Join-Path $ProjectRoot 'deploy/k8s/services/services.yaml'
$OnlineKustomizationPath = Join-Path $ProjectRoot 'deploy/k8s/overlays/online/kustomization.yaml'
$ReleasePath = '/pandora.ds.v1.DSAllocatorService/ReleaseBattle'
$BattlePrincipal = 'cluster.local/ns/pandora/sa/pandora-battle-result'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

function Assert-TerminalReleasePolicy([string]$Manifest) {
    Assert-True ([regex]::Matches($Manifest, '(?m)^kind:\s*AuthorizationPolicy\s*$').Count -eq 1) `
        'terminal component 必须只有一个 AuthorizationPolicy'
    Assert-True ($Manifest -match '(?m)^\s*name:\s*pandora-ds-terminal-release-exact-deny\s*$') `
        '缺 exact terminal ReleaseBattle policy'
    Assert-True ($Manifest -match '(?m)^\s*action:\s*DENY\s*$') 'terminal policy 必须是 DENY'
    Assert-True ([regex]::Matches($Manifest, [regex]::Escape($ReleasePath)).Count -eq 2) `
        '两条 DENY 必须都只绑定 exact ReleaseBattle path'
    Assert-True ([regex]::Matches($Manifest, '(?m)^\s*notPrincipals:\s*\[\s*"\*"\s*\]\s*$').Count -eq 1) `
        '必须显式拒绝 plaintext/空 principal，不能依赖 negative field 隐式语义'
    Assert-True ([regex]::Matches($Manifest,
        '(?m)^\s*notPrincipals:\s*\[\s*"' + [regex]::Escape($BattlePrincipal) + '"\s*\]\s*$').Count -eq 1) `
        '必须拒绝除 battle-result 外的其它 mTLS principal'
    Assert-True (-not $Manifest.Contains('/pandora.ds.v1.DSAllocatorService/*')) `
        '不得用服务通配符误封现有 allocator RPC'
}

$policy = Get-Content -LiteralPath $PolicyPath -Raw
Assert-TerminalReleasePolicy $policy

$peer = Get-Content -LiteralPath $PeerPath -Raw
Assert-True ($peer -match '(?m)^\s*name:\s*pandora-ds-allocator-terminal-permissive\s*$') `
    '缺 ds-allocator terminal PeerAuthentication'
Assert-True ($peer -match '(?m)^\s*mode:\s*PERMISSIVE\s*$') `
    '本轮必须 PERMISSIVE，不能切断 Heartbeat/Allocate 等既有调用'

$identity = Get-Content -LiteralPath $IdentityPath -Raw
$serviceAccount = Get-Content -LiteralPath $ServiceAccountPath -Raw
Assert-True ([regex]::Matches($identity, '(?m)^\s*serviceAccountName:\s*pandora-battle-result\s*$').Count -eq 1) `
    'battle-result 必须使用独立 workload principal'
Assert-True ([regex]::Matches($identity, '(?m)^\s*serviceAccountName:\s*pandora-allocator\s*$').Count -eq 1) `
    'ds-allocator 必须保留 Agones allocator principal'
Assert-True ($serviceAccount -match '(?m)^\s*name:\s*pandora-battle-result\s*$' -and
    $serviceAccount -match '(?m)^automountServiceAccountToken:\s*false\s*$') `
    '共享 mesh identity component 必须唯一声明 battle-result ServiceAccount 且默认不挂载 token'
Assert-True (-not [regex]::IsMatch($identity, '(?m)^\s*sidecar\.istio\.io/inject:\s*')) `
    'terminal RPC 两端只能由 revision 选择 injector，禁止 inject label/annotation'
Assert-True (-not [regex]::IsMatch($identity, '(?i)secretName|secretKeyRef|private[_-]?key|PANDORA_\w*SECRET')) `
    'terminal workload identity 不得新增或复用玩家/DS HMAC 密钥'

$services = Get-Content -LiteralPath $ServicesPath -Raw
# 断言只钉**意图**:20020 这一条端口必须带 name=grpc + appProtocol=grpc,path 级授权才解析得了。
# 不再钉整个 Service 的行内单行写法——2026-07-29 起 ds-allocator 多了运维面 21020 端口
# (writer 指标抓取路径),Service 改成多行列表;把排版一起钉死会让无关的合法演进撞红。
Assert-True ($services -match
    '(?ms)name:\s*ds-allocator,\s*namespace:\s*pandora\s*\}\s*\r?\nspec:.*?\{\s*name:\s*grpc,\s*appProtocol:\s*grpc,\s*port:\s*20020,\s*targetPort:\s*20020\s*\}') `
    'ds-allocator Service 20020 必须 name=grpc + appProtocol=grpc，确保 path 级授权可解析'

# 本组件只是独立静态候选，普通线上 overlay 不得默认引用，避免未完成真实 E2E 前误激活。
$onlineKustomization = Get-Content -LiteralPath $OnlineKustomizationPath -Raw
foreach ($candidate in @('mesh-shared-identity', 'inventory-mesh', 'ds-terminal-mesh')) {
    Assert-True (-not $onlineKustomization.Contains($candidate)) `
        "online overlay 不得默认接入静态候选:$candidate"
}

foreach ($envoyRelativePath in @('deploy/envoy/envoy.yaml', 'deploy/k8s/agones/16-ds-envoy.yaml')) {
    $envoy = Get-Content -LiteralPath (Join-Path $ProjectRoot $envoyRelativePath) -Raw
    Assert-True (-not $envoy.Contains($ReleasePath)) `
        "$envoyRelativePath 的 :8444 DS 面不得暴露内部 ReleaseBattle"
}

# 真实 kustomize 渲染锁定 component 能同时命中两个 Deployment，且不会破坏 Service 协议标记。
$OnlineParent = Join-Path $ProjectRoot 'deploy/k8s/overlays/online'
$Runtime = Join-Path $OnlineParent ('.ds-terminal-contract-' + [guid]::NewGuid().ToString('N'))
try {
    New-Item -ItemType Directory -Path $Runtime | Out-Null
    $kustomization = @"
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: pandora
resources:
  - ../../../services
components:
  - ../mesh-shared-identity
  - ../inventory-mesh/identity
  - ../ds-terminal-mesh
patches:
  - patch: |-
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: battle-result
        namespace: pandora
      spec:
        template:
          metadata:
            labels:
              istio.io/rev: istio-1-30
  - patch: |-
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: ds-allocator
        namespace: pandora
      spec:
        template:
          metadata:
            labels:
              istio.io/rev: istio-1-30
"@
    [System.IO.File]::WriteAllText((Join-Path $Runtime 'kustomization.yaml'), $kustomization,
        [System.Text.UTF8Encoding]::new($false))
    $renderedLines = @(& kubectl kustomize $Runtime 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "kubectl kustomize terminal component 失败:$($renderedLines -join [Environment]::NewLine)" }
    $rendered = $renderedLines -join [Environment]::NewLine
    $documents = @([regex]::Split($rendered, '(?m)^---\s*$'))
    $battleResultDeployments = @($documents | Where-Object {
        $_ -match '(?m)^kind:\s*Deployment\s*$' -and $_ -match '(?m)^\s*name:\s*battle-result\s*$'
    })
    $dsAllocatorDeployments = @($documents | Where-Object {
        $_ -match '(?m)^kind:\s*Deployment\s*$' -and $_ -match '(?m)^\s*name:\s*ds-allocator\s*$'
    })
    $battleResultServiceAccounts = @($documents | Where-Object {
        $_ -match '(?m)^kind:\s*ServiceAccount\s*$' -and $_ -match '(?m)^\s*name:\s*pandora-battle-result\s*$'
    })
    Assert-True ($battleResultServiceAccounts.Count -eq 1 -and
        $battleResultServiceAccounts[0] -match '(?m)^automountServiceAccountToken:\s*false\s*$') `
        'Inventory + terminal 双候选组合后必须只有一个 battle-result ServiceAccount'
    Assert-True ($battleResultDeployments.Count -eq 1 -and
        $battleResultDeployments[0] -match '(?m)^\s*serviceAccountName:\s*pandora-battle-result\s*$') `
        '渲染后 battle-result principal 漂移'
    Assert-True ($dsAllocatorDeployments.Count -eq 1 -and
        $dsAllocatorDeployments[0] -match '(?m)^\s*serviceAccountName:\s*pandora-allocator\s*$') `
        '渲染后 ds-allocator principal 漂移'
    Assert-True (-not [regex]::IsMatch(($battleResultDeployments[0] + $dsAllocatorDeployments[0]),
        '(?m)^\s*sidecar\.istio\.io/inject:\s*')) `
        '渲染后 terminal RPC 必须保持纯 revision 注入'
    Assert-True ([regex]::Matches(($battleResultDeployments[0] + $dsAllocatorDeployments[0]),
        '(?m)^\s*istio\.io/rev:\s*istio-1-30\s*$').Count -eq 2) `
        '渲染后 terminal RPC 两端必须绑定同一真实 revision'
    Assert-True ($rendered -match '(?ms)name:\s*ds-allocator\s+namespace:\s*pandora\s+spec:.*?appProtocol:\s*grpc\s+name:\s*grpc\s+port:\s*20020') `
        '渲染后 ds-allocator gRPC Service 协议标记丢失'
}
finally {
    if (Test-Path -LiteralPath $Runtime) {
        $resolved = [System.IO.Path]::GetFullPath($Runtime)
        if ((Split-Path -Parent $resolved) -cne [System.IO.Path]::GetFullPath($OnlineParent) -or
            (Split-Path -Leaf $resolved) -notmatch '^\.ds-terminal-contract-[0-9a-f]{32}$') {
            throw "拒绝清理未验证测试目录:$resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

# Mutants：删空 principal 门或把唯一 principal 换成其它服务都必须失败。
try {
    Assert-TerminalReleasePolicy ($policy.Replace('notPrincipals: ["*"]', 'principals: ["*"]'))
    throw 'ASSERT FAILED:plaintext mutant 未被拒绝'
} catch {
    if ($_.Exception.Message -eq 'ASSERT FAILED:plaintext mutant 未被拒绝') { throw }
}
try {
    Assert-TerminalReleasePolicy ($policy.Replace($BattlePrincipal, 'cluster.local/ns/pandora/sa/pandora-matchmaker'))
    throw 'ASSERT FAILED:principal mutant 未被拒绝'
} catch {
    if ($_.Exception.Message -eq 'ASSERT FAILED:principal mutant 未被拒绝') { throw }
}

Write-Host 'ds_terminal_mesh_contract_test: PASS' -ForegroundColor Green
