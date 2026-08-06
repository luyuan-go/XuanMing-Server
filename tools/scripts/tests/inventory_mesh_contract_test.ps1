[CmdletBinding()]
param(
    [switch]$ServerDryRun,
    [string]$KubeContext = ''
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
. (Join-Path $ProjectRoot 'tools/scripts/lib/inventory_mesh_contract.ps1')
. (Join-Path $ProjectRoot 'tools/scripts/lib/inventory_mesh_live.ps1')

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

function Assert-Throws([scriptblock]$Action, [string]$Message) {
    $thrown = $false
    try { & $Action } catch { $thrown = $true }
    if (-not $thrown) { throw "ASSERT FAILED:$Message" }
}

function Copy-TestObject($Value) {
    return (($Value | ConvertTo-Json -Depth 100) | ConvertFrom-Json -Depth 100)
}

$revision = 'istio-1-30'

# 7 Policy + 7 Binding；显式默认值避免 client candidate 与 apiserver defaulted live 恒不等。
$admission = @(Get-PandoraInventoryLocalAdmissionObjects)
Assert-True ($admission.Count -eq 14) 'Inventory/edge admission 对象必须恰好 14 个'
$policies = @($admission | Where-Object kind -CEQ 'ValidatingAdmissionPolicy')
Assert-True ($policies.Count -eq 7) '必须有七个 VAP'
foreach ($policy in $policies) {
    Assert-True ([string]$policy.spec.failurePolicy -ceq 'Fail') "$($policy.metadata.name) 必须 Fail"
    Assert-True ([string]$policy.spec.matchConstraints.matchPolicy -ceq 'Equivalent') "$($policy.metadata.name) 缺 matchPolicy"
    Assert-True ($null -ne $policy.spec.matchConstraints.namespaceSelector -and
        $null -ne $policy.spec.matchConstraints.objectSelector) "$($policy.metadata.name) 缺显式 selector defaults"
    foreach ($rule in @($policy.spec.matchConstraints.resourceRules)) {
        Assert-True ([string]$rule.scope -ceq '*') "$($policy.metadata.name) 缺显式 scope=*"
    }
}
$admissionPath = Join-Path $ProjectRoot 'deploy/k8s/overlays/online/inventory-mesh/gate/admission.yaml'
$admissionText = Get-Content -LiteralPath $admissionPath -Raw
foreach ($marker in @('pandora-inventory-mesh-deployments', 'pandora-inventory-mesh-replicasets',
        'pandora-inventory-mesh-pods', 'pandora-inventory-edge-deployments',
        'pandora-inventory-edge-replicasets', 'pandora-inventory-edge-pods')) {
    Assert-True ($admissionText.Contains("name: $marker", [StringComparison]::Ordinal)) "缺三层 gate:$marker"
}
Assert-True ($admissionText.Contains("'pod-template-hash'", [StringComparison]::Ordinal)) 'RS/Pod owner 链必须绑定 pod-template-hash'
Assert-True ($admissionText.Contains("'battle-result-ds-auth-green'", [StringComparison]::Ordinal)) 'gate 必须兼容 canonical green'
if ($ServerDryRun) {
    Assert-True (-not [string]::IsNullOrWhiteSpace($KubeContext)) 'server dry-run 必须显式提供非生产测试 KubeContext'
    $serverDryRunLines = @(& kubectl --context $KubeContext create --dry-run=server -f $admissionPath 2>&1)
    if ($LASTEXITCODE -ne 0) {
        throw "Inventory admission server dry-run/CEL 编译失败:$($serverDryRunLines -join [Environment]::NewLine)"
    }
} elseif (-not [string]::IsNullOrWhiteSpace($KubeContext)) {
    throw '仅提供 KubeContext 没有意义；如需真实 API server CEL 编译，请同时传 -ServerDryRun。'
}

# 构造独立的 9 个 system allow + 6 个 edge player allow；contract 必须拒绝任何扩边/通配。
$matrix = [ordered]@{
    'cluster.local/ns/pandora/sa/pandora-auction' = @('FreezeForOrder', 'EnsureAuctionEscrow', 'SettleAuctionMatch', 'ReleaseEscrow')
    'cluster.local/ns/pandora/sa/pandora-trade' = @('SettlePlayerTrade')
    'cluster.local/ns/pandora/sa/pandora-mail' = @('GrantItems', 'GrantInstances')
    'cluster.local/ns/pandora/sa/pandora-leaderboard' = @('GrantItems')
    'cluster.local/ns/pandora/sa/pandora-battle-result' = @('GrantInstances')
    'cluster.local/ns/pandora-ingress/sa/pandora-edge-envoy' = @('GetInventory', 'UseItem', 'SellItem', 'IdentifyItem', 'DiscardInstance', 'MoveInstance')
}
$rules = @()
foreach ($principal in $matrix.Keys) {
    $paths = @($matrix[$principal] | ForEach-Object { "/pandora.inventory.v1.InventoryService/$_" })
    $rules += [ordered]@{
        from = @([ordered]@{ source = [ordered]@{ principals = @($principal) } })
        to = @([ordered]@{ operation = [ordered]@{ methods = @('POST'); paths = $paths } })
    }
}
$exactPolicy = ([ordered]@{
        apiVersion = 'security.istio.io/v1'; kind = 'AuthorizationPolicy'
        metadata = [ordered]@{ name = 'pandora-inventory-exact-allow'; namespace = 'pandora'; uid = 'policy-uid'; annotations = [ordered]@{ 'pandora.dev/migration-phase' = 'enforce' } }
        spec = [ordered]@{ selector = [ordered]@{ matchLabels = [ordered]@{ app = 'inventory' } }; action = 'ALLOW'; rules = $rules }
    } | ConvertTo-Json -Depth 30 | ConvertFrom-Json -Depth 30)
Assert-PandoraInventoryAuthorizationPolicy -Policy $exactPolicy -Phase enforce
Assert-True (@(Get-PandoraInventoryExpectedAuthorizationRows).Count -eq 9) 'system allow 应为 9'
Assert-True (@(Get-PandoraInventoryExpectedDeniedSystemRows).Count -eq 26) 'system deny 应为 26'
Assert-True (@(Get-PandoraInventoryExpectedAuthorizationRows -IncludeEdge).Count -eq 15) '含 edge allow 应为 15'
$wildcard = Copy-TestObject $exactPolicy
$wildcard.spec.rules[0].to[0].operation.paths[0] = '/pandora.inventory.v1.InventoryService/*'
Assert-Throws { Assert-PandoraInventoryAuthorizationPolicy -Policy $wildcard } '通配 path mutant 未拒绝'
$extraPolicy = Copy-TestObject $exactPolicy
$extraPolicy.metadata.name = 'unexpected-wide-policy'; $extraPolicy.metadata.uid = 'extra'
Assert-Throws {
    Assert-PandoraInventoryAuthorizationPolicySet -Policies @($exactPolicy, $extraPolicy) `
        -InventoryPodLabels @([pscustomobject]@{ app = 'inventory'; 'security.istio.io/tlsMode' = 'istio' })
} '额外 applicable AuthorizationPolicy 未拒绝'

# live Pod contract：exact app+proxy、drop ALL、app 不挂 token、无旁加载体。
$probeJson = '{"/app-health/inventory/readyz":{"grpc":{"port":20015}}}'
$livePod = ([ordered]@{
        metadata = [ordered]@{
            name = 'inventory-test'; namespace = 'pandora'; labels = [ordered]@{ app = 'inventory'; 'istio.io/rev' = $revision }
            annotations = [ordered]@{
                'sidecar.istio.io/rewriteAppHTTPProbers' = 'true'
                'sidecar.istio.io/status' = '{"revision":"istio-1-30","containers":["istio-proxy"]}'
                'prometheus.io/scrape' = 'true'; 'prometheus.io/port' = '15020'; 'prometheus.io/path' = '/stats/prometheus'
            }
        }
        spec = [ordered]@{
            serviceAccountName = 'pandora-inventory'; automountServiceAccountToken = $false
            containers = @(
                [ordered]@{ name = 'inventory'; securityContext = [ordered]@{ allowPrivilegeEscalation = $false; privileged = $false; capabilities = [ordered]@{ drop = @('ALL') } }; readinessProbe = [ordered]@{ httpGet = [ordered]@{ port = 15020; path = '/app-health/inventory/readyz' } } },
                [ordered]@{ name = 'istio-proxy'; env = @([ordered]@{ name = 'ISTIO_KUBE_APP_PROBERS'; value = $probeJson }); volumeMounts = @([ordered]@{ name = 'istio-token'; readOnly = $true }) }
            )
            volumes = @([ordered]@{ name = 'istio-token'; projected = [ordered]@{ sources = @([ordered]@{ serviceAccountToken = [ordered]@{ path = 'istio-token' } }) } })
        }
    } | ConvertTo-Json -Depth 30 | ConvertFrom-Json -Depth 30)
Assert-PandoraInventoryMeshWorkload -Workload $livePod -Revision $revision -LivePod
foreach ($mutantName in @('automount', 'extra-container', 'app-token', 'ephemeral', 'extra-init', 'cap-add')) {
    $mutant = Copy-TestObject $livePod
    switch ($mutantName) {
        'automount' { $mutant.spec.automountServiceAccountToken = $true }
        'extra-container' { $mutant.spec.containers += [pscustomobject]@{ name = 'localhost-bypass' } }
        'app-token' { $mutant.spec.containers[0] | Add-Member volumeMounts @([pscustomobject]@{ name = 'istio-token' }) }
        'ephemeral' { $mutant.spec | Add-Member ephemeralContainers @([pscustomobject]@{ name = 'debug' }) }
        'extra-init' { $mutant.spec | Add-Member initContainers @([pscustomobject]@{ name = 'evil-init' }) }
        'cap-add' { $mutant.spec.containers[0].securityContext.capabilities | Add-Member add @('NET_RAW') }
    }
    Assert-Throws { Assert-PandoraInventoryMeshWorkload -Workload $mutant -Revision $revision -LivePod } "$mutantName mutant 未拒绝"
}

# MeshConfig 对缩进/flow JSON/merge 必须 fail closed，不能静默回安全默认。
$mesh = Get-PandoraInventoryMeshConfigContract -Text "rootNamespace: istio-system`ntrustDomain: cluster.local`ntrustDomainAliases: []`nenableAutoMtls: true`nenablePrometheusMerge: true"
Assert-True ($mesh.EnableAutoMtls -and $mesh.EnablePrometheusMerge) 'canonical MeshConfig 解析失败'
Assert-Throws { Get-PandoraInventoryMeshConfigContract -Text "  enableAutoMtls: false`n  trustDomainAliases:`n  - evil.local" } '缩进 YAML mutant 未拒绝'
Assert-Throws { Get-PandoraInventoryMeshConfigContract -Text '{"rootNamespace":"evil","enableAutoMtls":false}' } 'flow JSON mutant 未拒绝'
Assert-Throws { Get-PandoraInventoryMeshConfigContract -Text "defaults: &bad`n  enableAutoMtls: false`n<<: *bad" } 'YAML merge mutant 未拒绝'

# 通用 Pod mutator 不能被误算为 Istio injector；scope/matchConditions 也有 fail-closed 契约。
$genericWebhook = [pscustomobject]@{ name = 'mutate.example.com'; clientConfig = [pscustomobject]@{ service = [pscustomobject]@{ name = 'mutator'; namespace = 'system' } } }
$istioWebhook = [pscustomobject]@{ name = 'rev.object.sidecar-injector.istio.io'; clientConfig = [pscustomobject]@{ service = [pscustomobject]@{ name = 'istiod-istio-1-30'; namespace = 'istio-system' } } }
Assert-True (-not (Test-PandoraInventoryIstioInjectorWebhookIdentity $genericWebhook)) '通用 mutator 被误判为 injector'
Assert-True (Test-PandoraInventoryIstioInjectorWebhookIdentity $istioWebhook) '标准 revision injector 未识别'

# 旧 v1 单 JSON/布尔证据永久禁用；未完成 v2 前静态候选不得接入 ordinary online。
Assert-Throws {
    Assert-PandoraInventoryMeshAuditEvidence -Evidence ([pscustomobject]@{}) -KubeContext test `
        -Revision $revision -PolicyUID uid -PolicyGeneration 1
} '可伪造的 audit/v1 evidence 未禁用'
$startText = Get-Content -LiteralPath (Join-Path $ProjectRoot 'tools/scripts/start.ps1') -Raw
Assert-True (-not $startText.Contains('Assert-OnlineInventoryMeshPreflight', [StringComparison]::Ordinal)) `
    'B 收口后 ordinary start 不得残留 Inventory mesh preflight 半接线'
Assert-True (-not $startText.Contains('IstioRevision', [StringComparison]::Ordinal) -and
    -not $startText.Contains('inventory_mesh', [StringComparison]::Ordinal)) `
    'B 收口后 ordinary start 不得暴露 Inventory mesh 参数或 source helper'
$onlineKustomizationText = Get-Content -LiteralPath (Join-Path $ProjectRoot 'deploy/k8s/overlays/online/kustomization.yaml') -Raw
Assert-True (-not $onlineKustomizationText.Contains('inventory-mesh', [StringComparison]::Ordinal) -and
    -not $onlineKustomizationText.Contains('ds-terminal-mesh', [StringComparison]::Ordinal) -and
    -not $onlineKustomizationText.Contains('mesh-shared-identity', [StringComparison]::Ordinal)) `
    '默认 online overlay 不得引用未完成真实 E2E 的静态候选 component'

# 真实 Kustomize 结构渲染：仅 patch 7 个目标 Deployment + pandora Namespace；VAP 内 sentinel deny-list 不应误报。
$online = Join-Path $ProjectRoot 'deploy/k8s/overlays/online'
$overlayParent = Split-Path -Parent $online
$runtime = Join-Path $overlayParent ('.inventory-mesh-contract-' + [guid]::NewGuid().ToString('N'))
try {
    Copy-Item -LiteralPath $online -Destination $runtime -Recurse
    $kustomizationPath = Join-Path $runtime 'kustomization.yaml'
    $text = Get-Content -LiteralPath $kustomizationPath -Raw
    $components = @"

components:
  - mesh-shared-identity
  - inventory-mesh/identity
  - inventory-mesh/gate
  - inventory-mesh/enforce
  - inventory-mesh/network
"@
    $patches = [System.Text.StringBuilder]::new("`npatches:`n")
    foreach ($name in @('inventory', 'auction', 'trade', 'mail', 'leaderboard', 'battle-result')) {
        $null = $patches.Append("  - patch: |-`n      apiVersion: apps/v1`n      kind: Deployment`n      metadata:`n        name: $name`n        namespace: pandora`n      spec:`n        template:`n          metadata:`n            labels:`n              istio.io/rev: $revision`n")
    }
    $null = $patches.Append("  - patch: |-`n      apiVersion: v1`n      kind: Namespace`n      metadata:`n        name: pandora`n        labels:`n          pandora.dev/inventory-mesh-revision: $revision`n")
    [System.IO.File]::WriteAllText($kustomizationPath, $text + $components + $patches.ToString(), [System.Text.UTF8Encoding]::new($false))
    $renderedLines = @(& kubectl kustomize $runtime 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "kubectl kustomize Inventory runtime 失败:$($renderedLines -join [Environment]::NewLine)" }
    $rendered = ($renderedLines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    Assert-PandoraRenderedInventoryMeshTextContract -Manifest $rendered -Revision $revision
    $broadCandidate = $rendered.Replace('/pandora.inventory.v1.InventoryService/GrantItems',
        '/pandora.inventory.v1.InventoryService/*')
    Assert-Throws { Assert-PandoraRenderedInventoryMeshTextContract -Manifest $broadCandidate -Revision $revision } `
        'candidate broad AuthorizationPolicy 在 apply 前未拒绝'
}
finally {
    if (Test-Path -LiteralPath $runtime) {
        $resolved = [System.IO.Path]::GetFullPath($runtime)
        if ((Split-Path -Parent $resolved) -cne [System.IO.Path]::GetFullPath($overlayParent) -or
            (Split-Path -Leaf $resolved) -notmatch '^\.inventory-mesh-contract-[0-9a-f]{32}$') {
            throw "拒绝清理未验证测试目录:$resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

Write-Host 'inventory_mesh_contract_test: PASS' -ForegroundColor Green
