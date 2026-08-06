# Inventory 系统 RPC 的 Istio workload identity / 方法授权纯函数契约。
# 本文件不调用 kubectl、不写集群；外部命令与分阶段迁移由调用脚本负责。
Set-StrictMode -Version Latest

$script:PandoraInventoryMeshRevisionSentinel = 'PANDORA_ISTIO_REVISION_REQUIRED'
$script:PandoraInventoryMeshWorkloads = [ordered]@{
    inventory       = 'pandora-inventory'
    auction         = 'pandora-auction'
    trade           = 'pandora-trade'
    mail            = 'pandora-mail'
    leaderboard     = 'pandora-leaderboard'
    'battle-result' = 'pandora-battle-result'
}
$script:PandoraInventorySystemMethods = @(
    'GrantItems', 'GrantInstances', 'FreezeForOrder', 'EnsureAuctionEscrow',
    'SettleAuctionMatch', 'SettlePlayerTrade', 'ReleaseEscrow')
$script:PandoraInventoryPlayerMethods = @(
    'GetInventory', 'UseItem', 'SellItem', 'IdentifyItem', 'DiscardInstance', 'MoveInstance')
$script:PandoraInventoryPolicyName = 'pandora-inventory-exact-allow'
$script:PandoraInventoryPeerAuthenticationName = 'pandora-inventory-mtls'
$script:PandoraInventoryRenderedDocumentHashes = [ordered]@{
    'Service/inventory' = '7f92c8a42c9665bfa32aa78954c97f0f637c52b917f140a1e7fea4e8982c22de'
    'ValidatingAdmissionPolicy/pandora-inventory-edge-deployments' = '431316e80a10370169c57e0426948e44d81e05bfb2f813e1d01e6cd96758ae6a'
    'ValidatingAdmissionPolicy/pandora-inventory-edge-pods' = 'c586267f81d48b4da832936b524682084d45bfe5af3082b3a4ee1aaa9e81f94a'
    'ValidatingAdmissionPolicy/pandora-inventory-edge-replicasets' = '59d0d474f0716ad52dd38695e3a2adab874843dedc7ccc150b48b8b5fc81074a'
    'ValidatingAdmissionPolicy/pandora-inventory-mesh-deployments' = '0505ef57ccc1a971e72d82dc1dd59e4935f9323cfc36d364d1d35673c51dfd46'
    'ValidatingAdmissionPolicy/pandora-inventory-mesh-namespace-revision' = 'f260cd3f8e4d461ff626a2c2d3c3cf901b6d3f7b82ca48b5070b36ddd2c7e374'
    'ValidatingAdmissionPolicy/pandora-inventory-mesh-pods' = '37a523d2c0a563fc73b3cd2cd4d650a7d9d68938b23e3a7739b6f7b3ff31af35'
    'ValidatingAdmissionPolicy/pandora-inventory-mesh-replicasets' = '0f9459fca5ca33562ac4a7fe16a16ad5ec34d26270fb976fb69544ad98fd6cd3'
    'ValidatingAdmissionPolicyBinding/pandora-inventory-edge-deployments' = '3257790e70e04dd1dc1615a4794b44cf5445e11a125644fe064baa5356af4bd0'
    'ValidatingAdmissionPolicyBinding/pandora-inventory-edge-pods' = '54733a3768aa1d695839e4700f296029a4bb61c1fa7f592428973c0246e94f94'
    'ValidatingAdmissionPolicyBinding/pandora-inventory-edge-replicasets' = 'dd6771ebd85afbaf68502f26e234320aa2967261d82b304b3c6f6a5b9e244614'
    'ValidatingAdmissionPolicyBinding/pandora-inventory-mesh-deployments' = '13005c34d13d06fea4c9b2a2057c97c83868acc86aa0575a78f42a8cc770acc8'
    'ValidatingAdmissionPolicyBinding/pandora-inventory-mesh-namespace-revision' = '473eae7e70cfe75fc6e383c741280b15bee91b4f89b49d5752f1226cc1f37ef1'
    'ValidatingAdmissionPolicyBinding/pandora-inventory-mesh-pods' = 'c2addd25f6f0dd288c518cb1af89dbaef063023e9e38812b6bd637123359a824'
    'ValidatingAdmissionPolicyBinding/pandora-inventory-mesh-replicasets' = 'c494de1370ccd4330ac9a64ae1d840e48944d4048fb8acd5e8e4dd3530da706d'
    'NetworkPolicy/allow-app-mesh' = 'e1ab54a6d53f9938b10b60297722d3904e3877319c5c7b9c4a85271a28c11e64'
    'NetworkPolicy/allow-ds-to-envoy' = '35f6487d57a6a4f20becdb1b4baf2b806bf3e78fb89d362124af8200e9cc10a4'
    'NetworkPolicy/allow-etcd-ingress' = '660202edada31833ba1ee7845b2451ff54a8a0997ca57da461de1efae56e0ff9'
    'NetworkPolicy/allow-ingress-gateway' = '984a2b565adf3c1eb0df672d79811dc3f39e60e54c0d0a77e11f94e248863192'
    'NetworkPolicy/allow-inventory-grpc' = '39882b47d1d46468ee2174a6ad82de5ef0b897be619152d8cafa3cc211d35d11'
    'NetworkPolicy/allow-kafka-ingress' = '43899d5b1c42fe115b9da466122b17275de257bb26169b1cd18d992f984df36d'
    'NetworkPolicy/allow-mysql-ingress' = '07a0348ee2dfe576b1132bbd83b3590fdd881075d492357e70c745d9daa2855d'
    'NetworkPolicy/allow-redis-ingress' = 'e175a4d2c5496344b91fd8cf4a7c161e79ebcc686c3c49319fe921741e659899'
    'NetworkPolicy/allow-zookeeper-ingress' = '9097fcd8973b3c515e410e721da9e3fd5969a5a43bf3e9e532a075a1c5dab78c'
    'NetworkPolicy/default-deny-ingress' = '90cf769a75a92aae7784d65b68424cf8ab2c142de639a8e5c9f2cdd97c170c2d'
    'AuthorizationPolicy/pandora-inventory-exact-allow' = 'a7dc805f41f402e0bd77f8f85133d56450bfcacf6a640b6fd00ec6adc9e5a766'
    'PeerAuthentication/pandora-inventory-mtls' = '3314f81ef92e102c9c1a1b311a4a27af7661bdd8d0e8037deec5ed5b90c77774'
}

function Get-PandoraInventoryObjectProperty {
    param($Object, [Parameter(Mandatory = $true)][string]$Name)
    if ($null -eq $Object) { return $null }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) { return $null }
    return $property.Value
}

function Test-PandoraInventoryLabelSelectorMatch {
    param(
        $Selector,
        [Parameter(Mandatory = $true)]$Labels
    )
    if ($null -eq $Selector) { return $true }
    $matchLabels = Get-PandoraInventoryObjectProperty $Selector 'matchLabels'
    if ($null -ne $matchLabels) {
        foreach ($property in @($matchLabels.PSObject.Properties)) {
            if ([string](Get-PandoraInventoryObjectProperty $Labels ([string]$property.Name)) -cne [string]$property.Value) {
                return $false
            }
        }
    }
    $expressions = @((Get-PandoraInventoryObjectProperty $Selector 'matchExpressions') | Where-Object { $null -ne $_ })
    foreach ($expression in $expressions) {
        if ($null -eq $expression) { continue }
        $key = [string]$expression.key
        $operator = [string]$expression.operator
        $actualProperty = $Labels.PSObject.Properties[$key]
        $exists = $null -ne $actualProperty
        $actual = if ($exists) { [string]$actualProperty.Value } else { '' }
        $rawValues = Get-PandoraInventoryObjectProperty $expression 'values'
        $values = @($rawValues | Where-Object { $null -ne $_ } | ForEach-Object { [string]$_ })
        switch -CaseSensitive ($operator) {
            'In' {
                if ($values.Count -eq 0) { throw 'label selector In 必须带非空 values。' }
                if (-not $exists -or $values -cnotcontains $actual) { return $false }
            }
            'NotIn' {
                if ($values.Count -eq 0) { throw 'label selector NotIn 必须带非空 values。' }
                if ($exists -and $values -ccontains $actual) { return $false }
            }
            'Exists' {
                if ($values.Count -ne 0) { throw 'label selector Exists 禁止 values。' }
                if (-not $exists) { return $false }
            }
            'DoesNotExist' {
                if ($values.Count -ne 0) { throw 'label selector DoesNotExist 禁止 values。' }
                if ($exists) { return $false }
            }
            default { throw "无法机械求值的 label selector operator:$operator" }
        }
    }
    return $true
}

function Get-PandoraInventoryMeshWorkloads {
    return [ordered]@{} + $script:PandoraInventoryMeshWorkloads
}

function Assert-PandoraIstioRevision {
    param(
        [Parameter(Mandatory = $true)][string]$Revision,
        [switch]$AllowSentinel
    )
    if ($AllowSentinel -and $Revision -ceq $script:PandoraInventoryMeshRevisionSentinel) { return }
    if ([string]::IsNullOrWhiteSpace($Revision) -or
        $Revision -ceq $script:PandoraInventoryMeshRevisionSentinel -or
        $Revision -ceq 'default' -or $Revision -ceq 'latest' -or
        $Revision -cnotmatch '^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$') {
        throw "Istio revision『$Revision』非法；必须显式使用非 default/latest 的 revision 名，且不得保留模板占位。"
    }
}

function Get-PandoraInventoryExpectedAuthorizationRows {
    param([switch]$IncludeEdge)
    $prefix = '/pandora.inventory.v1.InventoryService/'
    $matrix = [ordered]@{
        'cluster.local/ns/pandora/sa/pandora-auction' = @(
            'FreezeForOrder', 'EnsureAuctionEscrow', 'SettleAuctionMatch', 'ReleaseEscrow')
        'cluster.local/ns/pandora/sa/pandora-trade' = @('SettlePlayerTrade')
        'cluster.local/ns/pandora/sa/pandora-mail' = @('GrantItems', 'GrantInstances')
        'cluster.local/ns/pandora/sa/pandora-leaderboard' = @('GrantItems')
        'cluster.local/ns/pandora/sa/pandora-battle-result' = @('GrantInstances')
    }
    if ($IncludeEdge) {
        $matrix['cluster.local/ns/pandora-ingress/sa/pandora-edge-envoy'] = $script:PandoraInventoryPlayerMethods
    }
    $rows = [System.Collections.Generic.List[string]]::new()
    foreach ($principal in $matrix.Keys) {
        foreach ($method in $matrix[$principal]) {
            $rows.Add("$principal`tPOST`t$prefix$method")
        }
    }
    return @($rows | Sort-Object -CaseSensitive)
}

function Get-PandoraInventoryExpectedDeniedSystemRows {
    $allowed = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($row in (Get-PandoraInventoryExpectedAuthorizationRows)) { $null = $allowed.Add($row) }
    $principals = @(
        'cluster.local/ns/pandora/sa/pandora-auction',
        'cluster.local/ns/pandora/sa/pandora-trade',
        'cluster.local/ns/pandora/sa/pandora-mail',
        'cluster.local/ns/pandora/sa/pandora-leaderboard',
        'cluster.local/ns/pandora/sa/pandora-battle-result')
    $prefix = '/pandora.inventory.v1.InventoryService/'
    $rows = [System.Collections.Generic.List[string]]::new()
    foreach ($principal in $principals) {
        foreach ($method in $script:PandoraInventorySystemMethods) {
            $row = "$principal`tPOST`t$prefix$method"
            if (-not $allowed.Contains($row)) { $rows.Add($row) }
        }
    }
    return @($rows | Sort-Object -CaseSensitive)
}

function Get-PandoraInventoryAuthorizationRows {
    param([Parameter(Mandatory = $true)]$Policy)
    $rows = [System.Collections.Generic.List[string]]::new()
    $rules = @($Policy.spec.rules)
    if ($rules.Count -ne 6) { throw "Inventory AuthorizationPolicy rules=$($rules.Count)，必须精确为 6 个 principal 规则。" }
    foreach ($rule in $rules) {
        if ($null -ne (Get-PandoraInventoryObjectProperty $rule 'when')) { throw 'Inventory AuthorizationPolicy 禁止 when 条件；方法矩阵必须可机械穷举。' }
        $from = @($rule.from)
        $to = @($rule.to)
        if ($from.Count -ne 1 -or $to.Count -ne 1) { throw '每条 Inventory 授权规则必须恰好一个 from 和一个 to。' }
        $sources = @($from[0].source.principals)
        if ($sources.Count -ne 1 -or [string]::IsNullOrWhiteSpace([string]$sources[0])) {
            throw '每条 Inventory 授权规则必须恰好一个非空 SPIFFE principal。'
        }
        $sourceProperties = @($from[0].source.PSObject.Properties.Name)
        if ($sourceProperties.Count -ne 1 -or $sourceProperties[0] -cne 'principals') {
            throw 'Inventory 授权 source 只允许 principals，禁止 namespace/IP/requestPrincipal 等旁路。'
        }
        $operationProperties = @($to[0].operation.PSObject.Properties.Name | Sort-Object -CaseSensitive)
        if (($operationProperties -join ',') -cne 'methods,paths') {
            throw 'Inventory 授权 operation 只允许 methods+paths，禁止 host/port/notPath 等旁路。'
        }
        $methods = @($to[0].operation.methods)
        $paths = @($to[0].operation.paths)
        if ($methods.Count -ne 1 -or [string]$methods[0] -cne 'POST' -or $paths.Count -eq 0) {
            throw 'Inventory gRPC 授权必须精确限定 HTTP POST 和非空完整 path。'
        }
        $principal = [string]$sources[0]
        if ($principal -match '[*?]' -or $principal -cnotmatch '^cluster\.local/ns/[a-z0-9-]+/sa/[a-z0-9-]+$') {
            throw "Inventory principal 非精确 SPIFFE 身份:$principal"
        }
        foreach ($pathValue in $paths) {
            $path = [string]$pathValue
            if ($path -match '[*?{}]' -or
                $path -cnotmatch '^/pandora\.inventory\.v1\.InventoryService/[A-Za-z][A-Za-z0-9]*$') {
                throw "Inventory gRPC path 必须是无通配符完整方法:$path"
            }
            $rows.Add("$principal`tPOST`t$path")
        }
    }
    $sorted = @($rows | Sort-Object -CaseSensitive)
    if (@($sorted | Sort-Object -CaseSensitive -Unique).Count -ne $sorted.Count) {
        throw 'Inventory AuthorizationPolicy 含重复 principal×path 单元格。'
    }
    return $sorted
}

function Assert-PandoraInventoryAuthorizationPolicy {
    param(
        [Parameter(Mandatory = $true)]$Policy,
        [ValidateSet('observe', 'enforce')][string]$Phase = 'enforce'
    )
    if ([string]$Policy.apiVersion -cne 'security.istio.io/v1' -or
        [string]$Policy.kind -cne 'AuthorizationPolicy' -or
        [string]$Policy.metadata.namespace -cne 'pandora' -or
        [string]$Policy.metadata.name -cne $script:PandoraInventoryPolicyName) {
        throw 'Inventory AuthorizationPolicy apiVersion/kind/namespace/name 漂移。'
    }
    if ([string]$Policy.spec.action -cne 'ALLOW' -or
        [string]$Policy.spec.selector.matchLabels.app -cne 'inventory' -or
        @($Policy.spec.selector.matchLabels.PSObject.Properties).Count -ne 1 -or
        $null -ne (Get-PandoraInventoryObjectProperty $Policy.spec 'targetRefs') -or
        $null -ne (Get-PandoraInventoryObjectProperty $Policy.spec 'targetRef')) {
        throw 'Inventory AuthorizationPolicy 必须 action=ALLOW 且 selector 只精确命中 app=inventory。'
    }
    $dryRun = [string](Get-PandoraInventoryObjectProperty $Policy.metadata.annotations 'istio.io/dry-run')
    $phaseAnnotation = [string](Get-PandoraInventoryObjectProperty $Policy.metadata.annotations 'pandora.dev/migration-phase')
    if ($Phase -ceq 'observe') {
        if ($dryRun -cne 'true' -or $phaseAnnotation -cne 'observe') {
            throw 'observe AuthorizationPolicy 必须 dry-run=true 且标记 observe。'
        }
    } elseif (-not [string]::IsNullOrWhiteSpace($dryRun) -or $phaseAnnotation -cne 'enforce') {
        throw 'enforce AuthorizationPolicy 禁止 dry-run，且必须标记 enforce。'
    }
    $actual = @(Get-PandoraInventoryAuthorizationRows -Policy $Policy)
    $expected = @(Get-PandoraInventoryExpectedAuthorizationRows -IncludeEdge)
    if ($actual.Count -ne $expected.Count) { throw "Inventory 授权边数量=$($actual.Count)，expected=$($expected.Count)。" }
    for ($i = 0; $i -lt $expected.Count; $i++) {
        if ([string]$actual[$i] -cne [string]$expected[$i]) {
            throw "Inventory 授权矩阵不精确:actual=$($actual[$i]) expected=$($expected[$i])。"
        }
    }
}

function Test-PandoraPolicySelectsInventory {
    param(
        [Parameter(Mandatory = $true)]$Policy,
        [Parameter(Mandatory = $true)]$InventoryPodLabels,
        [string]$RootNamespace = 'istio-system'
    )
    $namespace = [string]$Policy.metadata.namespace
    if ($namespace -cne 'pandora' -and $namespace -cne $RootNamespace) { return $false }
    $targetRefs = Get-PandoraInventoryObjectProperty $Policy.spec 'targetRefs'
    $targetRef = Get-PandoraInventoryObjectProperty $Policy.spec 'targetRef'
    if ($null -ne $targetRefs -or $null -ne $targetRef) {
        $refs = @(@($targetRefs) + @($targetRef) | Where-Object { $null -ne $_ })
        if ($refs.Count -eq 0) { throw 'AuthorizationPolicy targetRefs 是空结构，无法求值。' }
        foreach ($ref in $refs) {
            $kind = [string]$ref.kind
            $name = [string]$ref.name
            if ($kind -ceq 'Service' -and $name -ceq 'inventory') { return $true }
            if ($kind -notin @('Service', 'ServiceEntry', 'GatewayClass', 'Gateway')) {
                throw "无法机械求值的 AuthorizationPolicy targetRef:$kind/$name"
            }
        }
        return $false
    }
    $selector = Get-PandoraInventoryObjectProperty $Policy.spec 'selector'
    return Test-PandoraInventoryLabelSelectorMatch -Selector $selector -Labels $InventoryPodLabels
}

function Assert-PandoraInventoryAuthorizationPolicySet {
    param(
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$Policies,
        [Parameter(Mandatory = $true)][object[]]$InventoryPodLabels,
        [ValidateSet('observe', 'enforce')][string]$Phase = 'enforce',
        [string]$RootNamespace = 'istio-system'
    )
    if ($InventoryPodLabels.Count -eq 0) { throw '缺 live Inventory Pod labels，无法判定 AuthorizationPolicy selector。' }
    $selectedIdentity = ''
    foreach ($labels in $InventoryPodLabels) {
        $selected = @($Policies | Where-Object {
                Test-PandoraPolicySelectsInventory -Policy $_ -InventoryPodLabels $labels -RootNamespace $RootNamespace
            })
        if ($selected.Count -ne 1) {
            throw "命中 Inventory Pod 的 AuthorizationPolicy 数=$($selected.Count)，必须只有唯一精确策略；拒绝额外 namespace/root policy。"
        }
        Assert-PandoraInventoryAuthorizationPolicy -Policy $selected[0] -Phase $Phase
        $identity = "$( [string]$selected[0].metadata.namespace)/$( [string]$selected[0].metadata.name)/$( [string]$selected[0].metadata.uid)"
        if ([string]::IsNullOrWhiteSpace($selectedIdentity)) { $selectedIdentity = $identity }
        elseif ($identity -cne $selectedIdentity) { throw '不同 Inventory Pod 命中的 AuthorizationPolicy 集合不一致。' }
    }
}

function Assert-PandoraInventoryPeerAuthenticationSet {
    param(
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$PeerAuthentications,
        [Parameter(Mandatory = $true)][object[]]$InventoryPodLabels,
        [ValidateSet('PERMISSIVE', 'STRICT')][string]$Mode,
        [string]$RootNamespace = 'istio-system'
    )
    if ($InventoryPodLabels.Count -eq 0) { throw '缺 live Inventory Pod labels，无法判定 PeerAuthentication selector。' }
    foreach ($labels in $InventoryPodLabels) {
        $sameWorkloadScope = @($PeerAuthentications | Where-Object {
                $namespace = [string]$_.metadata.namespace
                if ($namespace -cne 'pandora' -and $namespace -cne $RootNamespace) { return $false }
                $selector = Get-PandoraInventoryObjectProperty $_.spec 'selector'
                $matchLabels = Get-PandoraInventoryObjectProperty $selector 'matchLabels'
                if ($null -eq $matchLabels -or @($matchLabels.PSObject.Properties).Count -eq 0) { return $false }
                return Test-PandoraInventoryLabelSelectorMatch -Selector $selector -Labels $labels
            })
        if ($sameWorkloadScope.Count -ne 1) {
            throw "Inventory workload scope PeerAuthentication 数=$($sameWorkloadScope.Count)，必须唯一。"
        }
        Assert-PandoraInventoryPeerAuthentication -PeerAuthentication $sameWorkloadScope[0] -Mode $Mode
    }
}

function New-PandoraInventoryMeshRevisionPatch {
    param(
        [Parameter(Mandatory = $true)][string]$Service,
        [Parameter(Mandatory = $true)][string]$Revision
    )
    Assert-PandoraIstioRevision -Revision $Revision
    if (-not $script:PandoraInventoryMeshWorkloads.Contains($Service)) {
        throw "非受管 Inventory mesh workload:$Service"
    }
    return @"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $Service
  namespace: pandora
spec:
  template:
    metadata:
      labels:
        istio.io/rev: $Revision
"@
}

function New-PandoraInventoryMeshNamespaceRevisionPatch {
    param([Parameter(Mandatory = $true)][string]$Revision)
    Assert-PandoraIstioRevision -Revision $Revision
    return @"
apiVersion: v1
kind: Namespace
metadata:
  name: pandora
  labels:
    pandora.dev/inventory-mesh-revision: $Revision
"@
}

function Assert-PandoraRenderedInventoryImmutableDocuments {
    param([Parameter(Mandatory = $true)][string]$Manifest)
    $actual = @{}
    foreach ($document in @([regex]::Split($Manifest, '(?m)^---\s*$') | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })) {
        $kind = [regex]::Match($document, '(?m)^kind:\s*(?<value>\S+)\s*$').Groups['value'].Value
        if ([string]::IsNullOrWhiteSpace($kind)) { continue }
        $name = [regex]::Match($document, '(?m)^  name:\s*(?<value>\S+)\s*$').Groups['value'].Value
        $inScope = ($kind -in @('AuthorizationPolicy', 'PeerAuthentication', 'ValidatingAdmissionPolicy',
                'ValidatingAdmissionPolicyBinding') -and $name.StartsWith('pandora-inventory-', [StringComparison]::Ordinal)) -or
            $kind -ceq 'NetworkPolicy' -or ($kind -ceq 'Service' -and $name -ceq 'inventory')
        if (-not $inScope) { continue }
        $key = "$kind/$name"
        if ([string]::IsNullOrWhiteSpace($name) -or $actual.ContainsKey($key)) {
            throw "Inventory 静态候选 render 含无名或重复安全对象:$key"
        }
        $normalized = $document.Replace("`r`n", "`n").Trim() + "`n"
        $sha = [System.Security.Cryptography.SHA256]::Create()
        try {
            $hash = ([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($normalized))) -replace '-', '').ToLowerInvariant()
        } finally { $sha.Dispose() }
        $actual[$key] = $hash
    }
    if ($actual.Count -ne $script:PandoraInventoryRenderedDocumentHashes.Count) {
        throw "Inventory 静态候选 render 安全对象数=$($actual.Count)，expected=$($script:PandoraInventoryRenderedDocumentHashes.Count)。"
    }
    $mismatches = [System.Collections.Generic.List[string]]::new()
    foreach ($entry in $script:PandoraInventoryRenderedDocumentHashes.GetEnumerator()) {
        $key = [string]$entry.Key
        $actualHash = if ($actual.ContainsKey($key)) { [string]$actual[$key] } else { '<missing>' }
        if ($actualHash -cne [string]$entry.Value) {
            $mismatches.Add("$key actual=$actualHash expected=$($entry.Value)")
        }
    }
    if ($mismatches.Count -gt 0) {
        throw "Inventory 静态候选 render 安全对象不等于审核锁定版本；拒绝激活前扩权:`n$($mismatches -join "`n")"
    }
}

function Assert-PandoraRenderedInventoryMeshTextContract {
    param(
        [Parameter(Mandatory = $true)][string]$Manifest,
        [Parameter(Mandatory = $true)][string]$Revision
    )
    Assert-PandoraIstioRevision -Revision $Revision
    Assert-PandoraRenderedInventoryImmutableDocuments -Manifest $Manifest
    $documents = @([regex]::Split($Manifest, '(?m)^---\s*$') | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $builtIns = [System.Collections.Generic.List[object]]::new()
    foreach ($document in $documents) {
        if ($document -notmatch '(?m)^kind:\s*(Deployment|Namespace)\s*$') { continue }
        $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-inventory-render-' + [guid]::NewGuid().ToString('N') + '.yaml')
        try {
            [System.IO.File]::WriteAllText($tmp, $document, [System.Text.UTF8Encoding]::new($false))
            $lines = @(& kubectl create --dry-run=client --validate=false -f $tmp -o json 2>&1)
            if ($LASTEXITCODE -ne 0) { throw "Inventory 静态候选 built-in 对象解析失败:$($lines -join [Environment]::NewLine)" }
            $builtIns.Add(((($lines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine) | ConvertFrom-Json -Depth 100))
        } finally {
            Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
        }
    }
    foreach ($entry in $script:PandoraInventoryMeshWorkloads.GetEnumerator()) {
        $matches = @($builtIns | Where-Object {
                [string]$_.kind -ceq 'Deployment' -and [string]$_.metadata.namespace -ceq 'pandora' -and
                [string]$_.metadata.name -ceq [string]$entry.Key
            })
        if ($matches.Count -ne 1) { throw "Inventory 静态候选 render 的 Deployment/$($entry.Key) 数=$($matches.Count)。" }
        Assert-PandoraInventoryMeshWorkload -Workload $matches[0] -Revision $Revision
    }
    $namespace = @($builtIns | Where-Object { [string]$_.kind -ceq 'Namespace' -and [string]$_.metadata.name -ceq 'pandora' })
    if ($namespace.Count -ne 1 -or
        [string](Get-PandoraInventoryObjectProperty $namespace[0].metadata.labels 'pandora.dev/inventory-mesh-revision') -cne $Revision) {
        throw 'Inventory 静态候选 render 的 namespace revision gate 未精确闭合。'
    }
    foreach ($marker in @(
        'name: pandora-inventory-exact-allow', 'name: pandora-inventory-mtls', 'mode: STRICT',
        'name: pandora-inventory-mesh-pods', 'name: pandora-inventory-mesh-replicasets',
        'name: pandora-inventory-mesh-namespace-revision',
        'name: pandora-inventory-edge-deployments', 'name: pandora-inventory-edge-replicasets',
        'name: pandora-inventory-edge-pods',
        'name: allow-inventory-grpc', 'appProtocol: grpc')) {
        if (-not $Manifest.Contains($marker, [StringComparison]::Ordinal)) { throw "Inventory 静态候选 render 缺 marker:$marker" }
    }
}

function Assert-PandoraInventoryPeerAuthentication {
    param(
        [Parameter(Mandatory = $true)]$PeerAuthentication,
        [ValidateSet('PERMISSIVE', 'STRICT')][string]$Mode
    )
    $wantPhase = if ($Mode -ceq 'STRICT') { 'enforce' } else { 'observe' }
    if ([string]$PeerAuthentication.apiVersion -cne 'security.istio.io/v1' -or
        [string]$PeerAuthentication.kind -cne 'PeerAuthentication' -or
        [string]$PeerAuthentication.metadata.namespace -cne 'pandora' -or
        [string]$PeerAuthentication.metadata.name -cne $script:PandoraInventoryPeerAuthenticationName -or
        [string](Get-PandoraInventoryObjectProperty $PeerAuthentication.metadata.annotations 'pandora.dev/migration-phase') -cne $wantPhase -or
        [string]$PeerAuthentication.spec.selector.matchLabels.app -cne 'inventory' -or
        @($PeerAuthentication.spec.selector.matchLabels.PSObject.Properties).Count -ne 1 -or
        [string]$PeerAuthentication.spec.mtls.mode -cne $Mode -or
        $null -ne (Get-PandoraInventoryObjectProperty $PeerAuthentication.spec 'portLevelMtls')) {
        throw "Inventory PeerAuthentication 必须唯一 selector app=inventory、mode=$Mode、无端口降级。"
    }
}

function Assert-PandoraInventoryServiceContract {
    param([Parameter(Mandatory = $true)]$Service)
    $ports = @($Service.spec.ports)
    $grpc = @($ports | Where-Object { [int]$_.port -eq 20015 -or [int]$_.targetPort -eq 20015 })
    if ([string]$Service.apiVersion -cne 'v1' -or [string]$Service.kind -cne 'Service' -or
        [string]$Service.metadata.namespace -cne 'pandora' -or [string]$Service.metadata.name -cne 'inventory' -or
        [string]$Service.spec.selector.app -cne 'inventory' -or $grpc.Count -ne 1 -or
        [string]$grpc[0].name -cne 'grpc' -or [string]$grpc[0].appProtocol -cne 'grpc' -or
        [int]$grpc[0].port -ne 20015 -or [int]$grpc[0].targetPort -ne 20015) {
        throw 'Inventory Service 20015 必须唯一且 name=grpc/appProtocol=grpc/selector app=inventory。'
    }
}

function Test-PandoraPortListContains {
    param([AllowEmptyString()][string]$Value, [int]$Port)
    if ([string]::IsNullOrWhiteSpace($Value)) { return $false }
    return @($Value.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ -ceq '*' -or $_ -ceq [string]$Port }).Count -gt 0
}

function Assert-PandoraInventoryMeshWorkload {
    param(
        [Parameter(Mandatory = $true)]$Workload,
        [Parameter(Mandatory = $true)][string]$Revision,
        [switch]$LivePod,
        [switch]$AllowRevisionSentinel,
        [string]$LogicalName = ''
    )
    Assert-PandoraIstioRevision -Revision $Revision -AllowSentinel:$AllowRevisionSentinel
    $name = if (-not [string]::IsNullOrWhiteSpace($LogicalName)) { $LogicalName } elseif ($LivePod) {
        [string]$Workload.metadata.labels.app
    } else { [string]$Workload.metadata.name }
    if (-not $script:PandoraInventoryMeshWorkloads.Contains($name)) { throw "非受管 Inventory mesh workload:$name" }
    $template = if ($LivePod) { $Workload } else { $Workload.spec.template }
    $spec = $template.spec
    $metadata = $template.metadata
    $expectedSA = [string]$script:PandoraInventoryMeshWorkloads[$name]
    if ([string](Get-PandoraInventoryObjectProperty $metadata.labels 'app') -cne $name) {
        throw "$name workload template/live app label 漂移。"
    }
    $ports = [ordered]@{ inventory = 20015; auction = 20016; trade = 20012; mail = 20009; leaderboard = 20007; 'battle-result' = 20022 }
    $injectAnnotation = [string](Get-PandoraInventoryObjectProperty $metadata.annotations 'sidecar.istio.io/inject')
    $dataplaneMode = [string](Get-PandoraInventoryObjectProperty $metadata.labels 'istio.io/dataplane-mode')
    if ([string]$spec.serviceAccountName -cne $expectedSA -or
        (Get-PandoraInventoryObjectProperty $spec 'automountServiceAccountToken') -ne $false -or
        [string]$metadata.labels.'istio.io/rev' -cne $Revision -or
        -not [string]::IsNullOrWhiteSpace([string](Get-PandoraInventoryObjectProperty $metadata.labels 'sidecar.istio.io/inject')) -or
        -not [string]::IsNullOrWhiteSpace($injectAnnotation) -or $dataplaneMode -ceq 'none' -or
        [string]$metadata.annotations.'sidecar.istio.io/rewriteAppHTTPProbers' -cne 'true') {
        throw "$name 必须绑定 SA=$expectedSA、仅由 revision=$Revision 选择 injector、probe rewrite=true；禁止 inject label/annotation 与 dataplane none。"
    }
    if ((Get-PandoraInventoryObjectProperty $spec 'hostNetwork') -eq $true) { throw "$name 禁止 hostNetwork 绕过 sidecar。" }
    foreach ($property in @($metadata.annotations.PSObject.Properties)) {
        if ([string]$property.Name -like 'traffic.sidecar.istio.io/*' -or
            [string]$property.Name -ceq 'proxy.istio.io/config' -or
            [string]$property.Name -ceq 'sidecar.istio.io/interceptionMode') {
            throw "$name 禁止自定义 sidecar 流量截获:$($property.Name)。"
        }
    }
    if ([string](Get-PandoraInventoryObjectProperty $metadata.annotations 'prometheus.istio.io/merge-metrics') -ceq 'false') {
        throw "$name 禁止关闭 Istio metrics merge。"
    }
    if (-not $LivePod -and -not [string]::IsNullOrWhiteSpace(
            [string](Get-PandoraInventoryObjectProperty $metadata.annotations 'sidecar.istio.io/status'))) {
        throw "$name Deployment 模板禁止预置 sidecar status。"
    }
    $appContainers = @($spec.containers | Where-Object name -CEQ $name)
    $expectedContainerCount = if ($LivePod) { 2 } else { 1 }
    if ($appContainers.Count -ne 1 -or @($spec.containers).Count -ne $expectedContainerCount) {
        throw "$name 容器集合不精确；模板只允许应用容器，live 只允许应用+istio-proxy。"
    }
    $appSecurityContext = Get-PandoraInventoryObjectProperty $appContainers[0] 'securityContext'
    $appCapabilities = Get-PandoraInventoryObjectProperty $appSecurityContext 'capabilities'
    $added = @((Get-PandoraInventoryObjectProperty $appCapabilities 'add') | Where-Object { $null -ne $_ } | ForEach-Object { [string]$_ })
    $dropped = @((Get-PandoraInventoryObjectProperty $appCapabilities 'drop') | Where-Object { $null -ne $_ } | ForEach-Object { [string]$_ })
    if ((Get-PandoraInventoryObjectProperty $appSecurityContext 'allowPrivilegeEscalation') -ne $false -or
        (Get-PandoraInventoryObjectProperty $appSecurityContext 'privileged') -ne $false -or
        $added.Count -ne 0 -or $dropped.Count -ne 1 -or $dropped[0] -cne 'ALL') {
        throw "$name 应用容器必须 allowPrivilegeEscalation=false、privileged=false、capabilities.drop=[ALL] 且无 add。"
    }
    if (@((Get-PandoraInventoryObjectProperty $appContainers[0] 'volumeMounts') | Where-Object {
                $null -ne $_ -and [string]$_.name -ceq 'istio-token'
            }).Count -ne 0) {
        throw "$name 应用容器禁止挂载 istio-token。"
    }
    if (-not $LivePod) {
        if ([string]$Workload.kind -cne 'Deployment' -or
            [string]$Workload.spec.strategy.type -cne 'RollingUpdate' -or
            [int]$Workload.spec.strategy.rollingUpdate.maxUnavailable -ne 0 -or
            [int]$Workload.spec.strategy.rollingUpdate.maxSurge -ne 1) {
            throw "$name 必须 maxUnavailable=0/maxSurge=1 滚动。"
        }
    }
    if ($LivePod) {
        $proxy = @($spec.containers | Where-Object name -CEQ 'istio-proxy')
        $status = [string](Get-PandoraInventoryObjectProperty $metadata.annotations 'sidecar.istio.io/status')
        if ($proxy.Count -ne 1 -or [string]::IsNullOrWhiteSpace($status)) {
            throw "Pod/$($Workload.metadata.name) 必须恰好一个应用容器和一个已注入 istio-proxy。"
        }
        if (@((Get-PandoraInventoryObjectProperty $spec 'ephemeralContainers') | Where-Object { $null -ne $_ }).Count -ne 0) {
            throw "Pod/$($Workload.metadata.name) 禁止任何 ephemeral container。"
        }
        $initContainers = @((Get-PandoraInventoryObjectProperty $spec 'initContainers') | Where-Object { $null -ne $_ })
        if ($initContainers.Count -gt 1 -or
            ($initContainers.Count -eq 1 -and [string]$initContainers[0].name -notin @('istio-init', 'istio-validation'))) {
            throw "Pod/$($Workload.metadata.name) init container 只允许零个或唯一 istio-init/istio-validation。"
        }
        try { $statusObject = $status | ConvertFrom-Json -Depth 20 } catch { throw "Pod/$($Workload.metadata.name) sidecar status 非法 JSON。" }
        if ([string]::IsNullOrWhiteSpace([string]$statusObject.revision) -or
            @($statusObject.containers | ForEach-Object { [string]$_ }) -cnotcontains 'istio-proxy') {
            throw "Pod/$($Workload.metadata.name) sidecar status 缺 actual revision/proxy。"
        }
        $readiness = Get-PandoraInventoryObjectProperty $appContainers[0] 'readinessProbe'
        $httpGet = Get-PandoraInventoryObjectProperty $readiness 'httpGet'
        if ($null -eq $httpGet -or [int]$httpGet.port -ne 15020 -or
            [string]$httpGet.path -cne "/app-health/$name/readyz") {
            throw "Pod/$($Workload.metadata.name) readiness 未改写到 15020 app-health。"
        }
        $probeEnvs = @($proxy[0].env | Where-Object name -CEQ 'ISTIO_KUBE_APP_PROBERS')
        if ($probeEnvs.Count -ne 1) { throw "Pod/$($Workload.metadata.name) proxy 缺唯一 ISTIO_KUBE_APP_PROBERS。" }
        try { $probeObject = ([string]$probeEnvs[0].value) | ConvertFrom-Json -Depth 20 }
        catch { throw "Pod/$($Workload.metadata.name) ISTIO_KUBE_APP_PROBERS 非法 JSON。" }
        $probeKey = "/app-health/$name/readyz"
        $originalProbe = Get-PandoraInventoryObjectProperty $probeObject $probeKey
        if ($null -eq $originalProbe -or [int]$originalProbe.grpc.port -ne [int]$ports[$name]) {
            throw "Pod/$($Workload.metadata.name) proxy 未保留原 gRPC readiness $($ports[$name])。"
        }
        $tokenVolumes = @($spec.volumes | Where-Object name -CEQ 'istio-token')
        $tokenMounts = @($proxy[0].volumeMounts | Where-Object name -CEQ 'istio-token')
        if ($tokenVolumes.Count -ne 1 -or $tokenMounts.Count -ne 1 -or $tokenMounts[0].readOnly -ne $true -or
            @($tokenVolumes[0].projected.sources | Where-Object { $null -ne (Get-PandoraInventoryObjectProperty $_ 'serviceAccountToken') }).Count -ne 1) {
            throw "Pod/$($Workload.metadata.name) istio-token projected SA token/mount 不完整。"
        }
        if ($name -ceq 'inventory' -and
            ([string](Get-PandoraInventoryObjectProperty $metadata.annotations 'prometheus.io/scrape') -cne 'true' -or
             [string](Get-PandoraInventoryObjectProperty $metadata.annotations 'prometheus.io/port') -cne '15020' -or
             [string](Get-PandoraInventoryObjectProperty $metadata.annotations 'prometheus.io/path') -cne '/stats/prometheus')) {
            throw 'Live Inventory Pod metrics 注解未由 Istio merge 重写。'
        }
    } else {
        if (@((Get-PandoraInventoryObjectProperty $spec 'initContainers') | Where-Object { $null -ne $_ }).Count -ne 0 -or
            @((Get-PandoraInventoryObjectProperty $spec 'ephemeralContainers') | Where-Object { $null -ne $_ }).Count -ne 0 -or
            @($spec.volumes | Where-Object name -CEQ 'istio-token').Count -ne 0) {
            throw "$name Deployment 模板禁止预置 init/ephemeral/istio-token。"
        }
        $grpc = Get-PandoraInventoryObjectProperty (Get-PandoraInventoryObjectProperty $appContainers[0] 'readinessProbe') 'grpc'
        if ($null -eq $grpc -or [int]$grpc.port -ne [int]$ports[$name]) { throw "$name 原生 gRPC readiness 端口漂移。" }
        if ($name -ceq 'inventory' -and
            ([string](Get-PandoraInventoryObjectProperty $metadata.annotations 'prometheus.io/scrape') -cne 'true' -or
             [string](Get-PandoraInventoryObjectProperty $metadata.annotations 'prometheus.io/port') -cne '21015' -or
             [string](Get-PandoraInventoryObjectProperty $metadata.annotations 'prometheus.io/path') -cne '/metrics')) {
            throw 'Inventory template 必须保留原始 metrics 21015/metrics 注解供 injector merge。'
        }
    }
}

function Assert-PandoraInventoryMeshAuditEvidence {
    param(
        [Parameter(Mandatory = $true)]$Evidence,
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$Revision,
        [Parameter(Mandatory = $true)][string]$PolicyUID,
        [Parameter(Mandatory = $true)][string]$PolicyGeneration
    )
    Assert-PandoraIstioRevision -Revision $Revision
    throw 'pandora.inventory-mesh-audit/v1 已永久禁用：它未绑定短时窗、live Pod/image/Deployment generation、' +
          'namespace/control-plane/webhook/policy resourceVersion，且混淆 dry-run shadow 与 active actual 语义。' +
          '在 observeEvidence + activeAllowEvidence v2 与锁内 live 复证完整实现前，任何证据都不得进入 enforce。'
}
