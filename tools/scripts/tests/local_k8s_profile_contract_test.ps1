$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw $Message }
}

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..\..')).Path
$startPath = Join-Path $ProjectRoot 'tools\scripts\start.ps1'
$bridgePath = Join-Path $ProjectRoot 'tools\scripts\k8s_envoy_bridge.ps1'
$tokens = $null
$errors = $null
$startAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $startPath, [ref]$tokens, [ref]$errors)
Assert-True (@($errors).Count -eq 0) 'start.ps1 必须可解析'

$bridgeTokens = $null
$bridgeErrors = $null
$bridgeAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $bridgePath, [ref]$bridgeTokens, [ref]$bridgeErrors)
Assert-True (@($bridgeErrors).Count -eq 0) 'k8s_envoy_bridge.ps1 必须可解析'

$e2ePath = Join-Path $ProjectRoot 'tools\scripts\e2e_k8s.ps1'
$e2eTokens = $null
$e2eErrors = $null
$e2eAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $e2ePath, [ref]$e2eTokens, [ref]$e2eErrors)
Assert-True (@($e2eErrors).Count -eq 0) 'e2e_k8s.ps1 必须可解析'
$e2eSource = Get-Content -LiteralPath $e2ePath -Raw

function Assert-Throws([scriptblock]$Action, [string]$Message) {
    $didThrow = $false
    try { & $Action | Out-Null } catch { $didThrow = $true }
    Assert-True $didThrow $Message
}

function Assert-DoesNotThrow([scriptblock]$Action, [string]$Message) {
    try { & $Action | Out-Null } catch { throw "$Message：$($_.Exception.Message)" }
}

function Get-OnlyFunctionAst($Ast, [string]$Name) {
    $definitions = @($Ast.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -ceq $Name
    }, $true))
    Assert-True ($definitions.Count -eq 1) "必须有唯一 $Name"
    return $definitions[0]
}

function Assert-CriticalInfraStartupOrdering([string]$InvokeSource) {
    $profileProbe = $InvokeSource.IndexOf(
        '$profileExistedBeforeStart = Test-MinikubeProfileExists', [StringComparison]::Ordinal)
    $minikubeStart = $InvokeSource.IndexOf('minikube start -p $mkProfile', [StringComparison]::Ordinal)
    $contextLock = $InvokeSource.IndexOf('$mkCtx = Resolve-MinikubeKubeContext', [StringComparison]::Ordinal)
    $namespaceProbe = $InvokeSource.IndexOf(
        '$pandoraNamespaceExistedBeforeStart = Test-KubernetesNamespaceExists', [StringComparison]::Ordinal)
    $persistenceGuard = $InvokeSource.IndexOf('Assert-ExistingLocalEtcdPersistence', [StringComparison]::Ordinal)
    $legacyCohortCollect = $InvokeSource.IndexOf('Get-LocalLegacyInfraOnlyAdoptionCohort', [StringComparison]::Ordinal)
    $namespaceAnchor = $InvokeSource.IndexOf('New-LocalFreshAnchoredNamespace', [StringComparison]::Ordinal)
    $namespaceApply = $InvokeSource.IndexOf("00-namespace.yaml", $persistenceGuard, [StringComparison]::Ordinal)
    $preinfraIntent = $InvokeSource.IndexOf('New-LocalFreshGenesisIntent', [StringComparison]::Ordinal)
    $anchorRemove = $InvokeSource.IndexOf('Remove-LocalFreshNamespaceAnchor', [StringComparison]::Ordinal)
    $configWrite = $InvokeSource.IndexOf('Apply-PandoraConfigSecret', [StringComparison]::Ordinal)
    $infraApply = $InvokeSource.IndexOf('apply -f $infraYaml', [StringComparison]::Ordinal)
    $etcdReady = $InvokeSource.IndexOf('rollout status deploy/etcd', [StringComparison]::Ordinal)
    $baseline = $InvokeSource.IndexOf('Assert-LocalDsAuthBaseline', [StringComparison]::Ordinal)
    $buildDs = $InvokeSource.IndexOf('Build-DsImagesForMinikube', [StringComparison]::Ordinal)
    $applyAgones = $InvokeSource.IndexOf('Apply-AgonesManifests', $buildDs, [StringComparison]::Ordinal)
    $businessApply = $InvokeSource.IndexOf('apply -k $servicesDir', [StringComparison]::Ordinal)

    foreach ($position in @($profileProbe, $minikubeStart, $contextLock, $namespaceProbe,
            $persistenceGuard, $legacyCohortCollect, $namespaceAnchor, $namespaceApply, $preinfraIntent, $anchorRemove, $configWrite, $infraApply,
            $etcdReady, $baseline, $buildDs, $applyAgones, $businessApply)) {
        Assert-True ($position -ge 0) '一键启动 fresh intent / baseline 顺序缺少必要调用'
    }
    Assert-True ($profileProbe -lt $minikubeStart -and $minikubeStart -lt $contextLock -and
        $contextLock -lt $namespaceProbe -and $namespaceProbe -lt $persistenceGuard -and
        $persistenceGuard -lt $legacyCohortCollect -and $legacyCohortCollect -lt $namespaceAnchor -and
        $namespaceAnchor -lt $namespaceApply -and
        $namespaceApply -lt $preinfraIntent -and $preinfraIntent -lt $anchorRemove) `
        'fresh namespace 必须原子带 anchor；apply 后建立正式 marker，回读成功才能移除 anchor'
    Assert-True ($anchorRemove -lt $configWrite -and $configWrite -lt $infraApply -and
        $infraApply -lt $etcdReady -and $etcdReady -lt $baseline) `
        'fresh intent 必须早于配置/infra，baseline 必须等 etcd Ready 后执行'
    Assert-True ($baseline -lt $buildDs -and $buildDs -lt $applyAgones -and
        $applyAgones -lt $businessApply) `
        'required V3 verify/complete 必须早于 DS、Agones 和业务 writer 创建'
}

function Assert-OneClickGenesisAuthorization([string]$InvokeSource) {
    foreach ($fragment in @(
            'if (-not $pandoraNamespaceExistedBeforeStart)',
            'New-LocalFreshAnchoredNamespace -KubeContext $mkCtx -MinikubeProfile $mkProfile',
            'if ($freshNamespaceAnchor -and $null -eq $currentGenesisMarker)',
            'Remove-LocalFreshNamespaceAnchor -KubeContext $mkCtx -MinikubeProfile $mkProfile',
            '-AllowPendingPvcForPreinfra:($initialGenesisMarkerState -ceq ''preinfra'')',
            '$allowLegacyInfraOnlyAdoption = $pandoraNamespaceExistedBeforeStart -and',
            '$null -eq $initialGenesisMarker -and',
            '-not [string]::IsNullOrWhiteSpace($legacyAdoptionPvcUid)',
            '-AllowFreshBootstrap:$true',
            '-AllowLegacyInfraOnlyAdoption:$allowLegacyInfraOnlyAdoption',
            '-ExpectedAdoptionPvcUid $legacyAdoptionPvcUid',
            '-ExpectedAdoptionCohortFingerprintSha256 $legacyAdoptionCohortFingerprintSha256',
            '-LegacyAdoptionCollectionTimeUnixMS $legacyAdoptionCollectionTimeUnixMS',
            '-LegacyAdoptionCohortPreflightError $legacyAdoptionCohortPreflightError',
            '-AllowNotReady')) {
        Assert-True ($InvokeSource.Contains($fragment)) "一键入口 genesis 授权表达式缺少:$fragment"
    }
    $legacyStart = $InvokeSource.IndexOf('$allowLegacyInfraOnlyAdoption =', [StringComparison]::Ordinal)
    $legacyEnd = $InvokeSource.IndexOf('Write-Step "[1/8] namespace"', $legacyStart, [StringComparison]::Ordinal)
    Assert-True ($legacyStart -ge 0 -and $legacyEnd -gt $legacyStart) '必须能提取唯一 legacy 收养授权赋值'
    $legacyExpression = $InvokeSource.Substring($legacyStart, $legacyEnd - $legacyStart)
    Assert-True ($legacyExpression -cnotmatch '(?:^|\s)-or(?:\s|$)') `
        'legacy 收养授权只能是 namespace+无 marker+Bound PVC UID 三项 AND，不得追加 OR 放宽'
    foreach ($predicate in @(
            '$pandoraNamespaceExistedBeforeStart',
            '$null -eq $initialGenesisMarker',
            '-not [string]::IsNullOrWhiteSpace($legacyAdoptionPvcUid)')) {
        Assert-True ([regex]::Matches($legacyExpression, [regex]::Escape($predicate)).Count -eq 1) `
            "legacy 收养授权 predicate 必须且只能出现一次:$predicate"
    }
}

function Assert-BaselineGenesisAuthorization([string]$BaselineSource) {
    foreach ($fragment in @(
            '$requiredIsMissing = $readText -match',
            'if (-not $AllowFreshBootstrap)',
            'if (-not $AllowLegacyInfraOnlyAdoption -or [string]::IsNullOrWhiteSpace($ExpectedAdoptionPvcUid))',
            "markerState -ceq 'complete'",
            "markerState -cne 'pending'",
            'Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext $KubeContext',
            '$recheckedCohort.FingerprintSha256 -cne $ExpectedAdoptionCohortFingerprintSha256',
            "markerState -cin @('preinfra', 'adopting')",
            '--prepare-zero-writer-genesis-v3',
            '--verify-genesis-continuity',
            '--genesis-continuity-token $continuityToken',
            'Ensure-LocalObservedV3Witness',
            'LocalFreshDsAuthMarkerEvidenceCompletedAnnotation',
            '[System.Threading.Mutex]::new',
            '$baselineMutex.WaitOne',
            '[System.Threading.AbandonedMutexException]',
            '$baselineMutex.ReleaseMutex()',
            '$prepareMarker = Get-LocalFreshGenesisIntent',
            '$prepareMarkerState -cnotin',
            '$prepareTokenProperty',
            '$prepareDeadline = [DateTime]::UtcNow.AddSeconds(45)',
            'continuity prepare 返回瞬时/不确定结果')) {
        Assert-True ($BaselineSource.Contains($fragment)) "baseline genesis 授权表达式缺少:$fragment"
    }
    $preinfraStart = $BaselineSource.IndexOf("if (`$markerState -cin @('preinfra', 'adopting'))", [StringComparison]::Ordinal)
    $pendingBarrier = $BaselineSource.IndexOf("if (`$markerState -cne 'pending')", $preinfraStart, [StringComparison]::Ordinal)
    Assert-True ($preinfraStart -ge 0 -and $pendingBarrier -gt $preinfraStart) `
        'baseline 必须有独立 preinfra 分支并在 pending barrier 前收口'
    $preinfraBranch = $BaselineSource.Substring($preinfraStart, $pendingBarrier - $preinfraStart)
    $preinfraPristine = $preinfraBranch.IndexOf('Assert-LocalEtcdStorePristineForGenesis', [StringComparison]::Ordinal)
    $preinfraLastPristine = $preinfraBranch.LastIndexOf('Assert-LocalEtcdStorePristineForGenesis', [StringComparison]::Ordinal)
    $preinfraZeroWriter = $preinfraBranch.IndexOf('Assert-NoLocalFreshGenesisWriters', [StringComparison]::Ordinal)
    $preinfraMarkerReread = $preinfraBranch.IndexOf('$prepareMarker = Get-LocalFreshGenesisIntent', [StringComparison]::Ordinal)
    $preinfraMarkerStateGate = $preinfraBranch.IndexOf('$prepareMarkerState -cnotin', [StringComparison]::Ordinal)
    $preinfraPrepare = $preinfraBranch.IndexOf('--prepare-zero-writer-genesis-v3', [StringComparison]::Ordinal)
    $preinfraSentinelVerify = $preinfraBranch.LastIndexOf('--verify-genesis-continuity', [StringComparison]::Ordinal)
    $preinfraSetPending = $preinfraBranch.IndexOf('-TargetState pending', [StringComparison]::Ordinal)
    Assert-True ($preinfraPristine -ge 0 -and $preinfraPristine -lt $preinfraZeroWriter -and
        $preinfraZeroWriter -lt $preinfraLastPristine -and $preinfraLastPristine -lt $preinfraPrepare -and
        $preinfraLastPristine -lt $preinfraMarkerReread -and $preinfraMarkerReread -lt $preinfraMarkerStateGate -and
        $preinfraMarkerStateGate -lt $preinfraPrepare -and
        $preinfraPrepare -lt $preinfraSentinelVerify -and $preinfraSentinelVerify -lt $preinfraSetPending) `
        'preinfra/adopting 必须按 pristine -> zero-writer -> pristine -> marker/token 回读 -> prepare sentinel -> verify -> pending 排序'

    $resumeGuardStart = $BaselineSource.IndexOf('if (-not $AllowFreshBootstrap)', [StringComparison]::Ordinal)
    $resumeGuardEnd = $BaselineSource.IndexOf('if (-not $requiredIsMissing)', $resumeGuardStart, [StringComparison]::Ordinal)
    $pendingGuardStart = $BaselineSource.IndexOf("if (`$markerState -cne 'pending')", [StringComparison]::Ordinal)
    $pendingGuardEnd = $BaselineSource.IndexOf('# pending 以后 sentinel', $pendingGuardStart, [StringComparison]::Ordinal)
    foreach ($guard in @(
            @{ Start = $resumeGuardStart; End = $resumeGuardEnd; Name = 'Resume allow=false' },
            @{ Start = $pendingGuardStart; End = $pendingGuardEnd; Name = 'pending state barrier' })) {
        Assert-True ($guard.Start -ge 0 -and $guard.End -gt $guard.Start) "无法提取 terminating guard:$($guard.Name)"
        $guardBranch = $BaselineSource.Substring($guard.Start, $guard.End - $guard.Start)
        Assert-True ($guardBranch -cmatch '(?m)^\s*throw\s') `
            "$($guard.Name) 必须以 throw 终止，不能只告警后继续到 CAS"
    }
    $exactV3Start = $BaselineSource.IndexOf('if ($readExit -eq 0)', [StringComparison]::Ordinal)
    $missingStart = $BaselineSource.IndexOf('$readText =', $exactV3Start, [StringComparison]::Ordinal)
    Assert-True ($exactV3Start -ge 0 -and $missingStart -gt $exactV3Start) '无法提取 exact V3 恢复分支'
    $exactV3Branch = $BaselineSource.Substring($exactV3Start, $missingStart - $exactV3Start)
    Assert-True ($exactV3Branch.Contains("if (`$markerState -ceq 'pending')") -and
        $exactV3Branch.Contains('-TargetState complete') -and
        -not $exactV3Branch.Contains("markerState -ceq 'pending' -and `$AllowFreshBootstrap")) `
        'exact evidence 已验证的 pending 必须在普通启动和 Resume 都收敛 complete'
    Assert-True (-not $exactV3Branch.Contains('Get-LocalLegacyInfraOnlyAdoptionCohort') -and
        -not $exactV3Branch.Contains('LegacyAdoptionCohortPreflightError')) `
        '已有 exact V3（包括无 marker）必须在 missing-only legacy cohort 门禁前返回'
}

foreach ($functionName in @('Get-AgonesAdvertiseHostFromText', 'Assert-RelayBindingMatchesAdvertiseHosts')) {
    $definitions = @($e2eAst.FindAll({
        param($node)
        $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
            $node.Name -ceq $functionName
    }, $true))
    Assert-True ($definitions.Count -eq 1) "必须有唯一 $functionName"
    Invoke-Expression $definitions[0].Extent.Text
}

$allocatorYaml = @'
agones:
  enabled: true
  advertise_host: "192.168.2.28"
local_hub:
  advertise_host: "127.0.0.1"
'@
Assert-True ((Get-AgonesAdvertiseHostFromText $allocatorYaml 'test.yaml') -ceq '192.168.2.28') `
    '必须只读取 agones.advertise_host，不能误取 local_hub/local_ds'
$missingAgonesHostYaml = @'
agones:
  enabled: true
local_hub:
  advertise_host: "127.0.0.1"
'@
Assert-Throws { Get-AgonesAdvertiseHostFromText $missingAgonesHostYaml 'test.yaml' } `
    'agones 段缺少 advertise_host 时必须拒绝，不能回退到 local_hub/local_ds'
$duplicateAgonesHostYaml = @'
agones:
  advertise_host: ""
  advertise_host: "192.168.2.28"
'@
Assert-Throws { Get-AgonesAdvertiseHostFromText $duplicateAgonesHostYaml 'test.yaml' } `
    '首个值为空时也必须拒绝重复的 agones.advertise_host'

Assert-DoesNotThrow {
    Assert-RelayBindingMatchesAdvertiseHosts '127.0.0.1' '127.0.0.1' '127.0.0.1' $false
} '回环 advertise + 回环 relay 应通过'
Assert-DoesNotThrow {
    Assert-RelayBindingMatchesAdvertiseHosts '192.168.2.28' '192.168.2.28' '0.0.0.0' $true
} 'LAN advertise + 显式开放 relay 应通过'
Assert-Throws {
    Assert-RelayBindingMatchesAdvertiseHosts '192.168.2.28' '192.168.2.28' '127.0.0.1' $false
} 'LAN advertise + 默认回环 relay 必须拒绝'
Assert-Throws {
    Assert-RelayBindingMatchesAdvertiseHosts '192.168.2.28' '192.168.2.28' '0.0.0.0' $false
} 'LAN relay 即使请求 0.0.0.0 也必须确认是显式传参'
Assert-Throws {
    Assert-RelayBindingMatchesAdvertiseHosts '127.0.0.1' '127.0.0.1' '0.0.0.0' $true
} '回环 advertise 不得无必要开放 LAN relay'
Assert-Throws {
    Assert-RelayBindingMatchesAdvertiseHosts '192.168.2.28' '192.168.2.29' '0.0.0.0' $true
} 'DS/Hub allocator advertise_host 漂移必须拒绝'
Assert-Throws {
    Assert-RelayBindingMatchesAdvertiseHosts '' '192.168.2.28' '0.0.0.0' $true
} 'allocator advertise_host 缺失必须拒绝'
Assert-Throws {
    Assert-RelayBindingMatchesAdvertiseHosts 'not-an-ip' 'not-an-ip' '0.0.0.0' $true
} 'allocator advertise_host 非法必须拒绝'

Assert-True ($e2eSource.Contains("[ValidateSet('127.0.0.1', '0.0.0.0')]")) `
    'RelayBindHost 只能是两种受支持的安全绑定'
Assert-True ($e2eSource.Contains('$PSBoundParameters.ContainsKey(''RelayBindHost'')')) `
    'LAN 开放必须区分参数是否由调用方显式传入'
foreach ($contractText in @('get secret pandora-config', 'ds-allocator.yaml', 'hub-allocator.yaml')) {
    Assert-True ($e2eSource.Contains($contractText)) "live allocator guard 缺少 $contractText"
}

function Test-IsInsideFunctionDefinition($Node) {
    $cursor = $Node.Parent
    while ($null -ne $cursor) {
        if ($cursor -is [System.Management.Automation.Language.FunctionDefinitionAst]) { return $true }
        $cursor = $cursor.Parent
    }
    return $false
}

$topCommands = @($e2eAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.CommandAst]
}, $true) | Where-Object { -not (Test-IsInsideFunctionDefinition $_) })

function Find-TopCommand([string]$Name, [string]$Fragment = '') {
    return @($topCommands | Where-Object {
        $_.GetCommandName() -ceq $Name -and
            ([string]::IsNullOrEmpty($Fragment) -or $_.Extent.Text.Contains($Fragment))
    })
}

$initialGuards = @(Find-TopCommand 'Get-LiveAllocatorAdvertiseContract')
Assert-True ($initialGuards.Count -eq 1) '主流程必须且只能有一个初始 live allocator guard'
$guardOffset = $initialGuards[0].Extent.StartOffset
$mutations = @(
    @{ Name = 'kubectl'; Fragment = 'apply -f'; Label = 'Fleet apply' },
    @{ Name = 'minikube'; Fragment = 'image load'; Label = '节点镜像 load' },
    @{ Name = 'kubectl'; Fragment = 'delete gameservers'; Label = 'GameServer 删除' },
    @{ Name = 'Start-K8sEnvoyBridge'; Fragment = ''; Label = 'Envoy 重建' },
    @{ Name = 'Stop-HostUdpRelay'; Fragment = ''; Label = '宿主 relay 停止' },
    @{ Name = 'docker'; Fragment = 'rm -f'; Label = 'relay 容器删除' },
    @{ Name = 'docker'; Fragment = 'build -t'; Label = 'relay 镜像构建' },
    @{ Name = 'docker'; Fragment = 'run -d --name'; Label = 'relay 容器启动' }
)
foreach ($mutation in $mutations) {
    $hits = @(Find-TopCommand $mutation.Name $mutation.Fragment)
    Assert-True ($hits.Count -ge 1) "缺少受保护操作：$($mutation.Label)"
    foreach ($hit in $hits) {
        Assert-True ($guardOffset -lt $hit.Extent.StartOffset) "live allocator guard 必须早于：$($mutation.Label)"
    }
}

$bridgeCalls = @(Find-TopCommand 'Start-K8sEnvoyBridge')
$stopRelayCalls = @(Find-TopCommand 'Stop-HostUdpRelay')
$rechecks = @(Find-TopCommand 'Assert-LiveAllocatorContractUnchanged')
$dockerBuildCalls = @(Find-TopCommand 'docker' 'build -t')
$dockerRunCalls = @(Find-TopCommand 'docker' 'run -d --name')
Assert-True ($bridgeCalls.Count -eq 1 -and $stopRelayCalls.Count -eq 1) '桥接和旧 relay 停止调用必须唯一'
Assert-True ($dockerBuildCalls.Count -eq 1 -and $dockerRunCalls.Count -eq 1) 'relay build/run 调用必须唯一'
Assert-True ($rechecks.Count -ge 2) 'Secret 版本必须在桥接前和停止旧 relay 前复核'
$bridgeOffset = $bridgeCalls[0].Extent.StartOffset
$stopRelayOffset = $stopRelayCalls[0].Extent.StartOffset
$dockerBuildOffset = $dockerBuildCalls[0].Extent.StartOffset
$dockerRunOffset = $dockerRunCalls[0].Extent.StartOffset
$recheckOffsets = @($rechecks | ForEach-Object { $_.Extent.StartOffset })
Assert-True (@($recheckOffsets | Where-Object { $_ -lt $bridgeOffset }).Count -ge 1) `
    '必须在 Envoy 重建前复核 live allocator 合约'
Assert-True (@($recheckOffsets | Where-Object { $_ -gt $bridgeOffset -and $_ -lt $stopRelayOffset }).Count -ge 1) `
    '必须在停止旧 relay 前再次复核 live allocator 合约'
Assert-True (@($recheckOffsets | Where-Object { $_ -gt $dockerBuildOffset -and $_ -lt $stopRelayOffset }).Count -ge 1) `
    '必须在 relay 镜像构建完成后、停止旧 relay 前做最终 Secret 复核'
Assert-True ($stopRelayOffset -lt $dockerRunOffset) '必须在最终复核后紧邻替换 relay，缩小 TOCTOU 窗口'

$source = Get-Content -LiteralPath $startPath -Raw
$invokeK8sFunctions = @($startAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -ceq 'Invoke-K8s'
}, $true))
Assert-True ($invokeK8sFunctions.Count -eq 1) '必须有唯一 Invoke-K8s'
$invokeK8sSource = $invokeK8sFunctions[0].Extent.Text
Assert-CriticalInfraStartupOrdering -InvokeSource $invokeK8sSource
Assert-OneClickGenesisAuthorization -InvokeSource $invokeK8sSource
Assert-Throws {
    Assert-CriticalInfraStartupOrdering -InvokeSource (
        $invokeK8sSource.Replace('New-LocalFreshGenesisIntent', 'Removed-LocalFreshGenesisIntent'))
} '删除入口中的 fresh intent 调用时顺序契约必须失败，禁止 helper-only 假绿'
Assert-Throws {
    Assert-CriticalInfraStartupOrdering -InvokeSource (
        $invokeK8sSource.Replace('Assert-LocalDsAuthBaseline', 'Removed-LocalDsAuthBaseline'))
} '删除入口中的 baseline 调用时顺序契约必须失败'
Assert-Throws {
    Assert-CriticalInfraStartupOrdering -InvokeSource (
        $invokeK8sSource.Replace('Get-LocalLegacyInfraOnlyAdoptionCohort', 'Removed-LocalLegacyInfraOnlyAdoptionCohort'))
} '删除任何 apply 前的 legacy cohort 采集时顺序契约必须失败'
Assert-True (-not $invokeK8sSource.Contains('AllowFreshBootstrap:(-not $profileExistedBeforeStart)')) `
    'genesis 授权不得继续依赖瞬时 profileExistedBeforeStart 布尔值'
Assert-True ($invokeK8sSource.Contains('-AllowLegacyInfraOnlyAdoption:$allowLegacyInfraOnlyAdoption')) `
    '普通一键入口必须显式传入受限 legacy infra-only 收养决策'
foreach ($mutant in @(
        $invokeK8sSource.Replace('-AllowFreshBootstrap:$true', '-AllowFreshBootstrap:$false'),
        $invokeK8sSource.Replace('New-LocalFreshAnchoredNamespace -KubeContext $mkCtx -MinikubeProfile $mkProfile', 'Removed-LocalFreshAnchoredNamespace'),
        $invokeK8sSource.Replace('Remove-LocalFreshNamespaceAnchor -KubeContext $mkCtx -MinikubeProfile $mkProfile', 'Removed-LocalFreshNamespaceAnchor'),
        $invokeK8sSource.Replace('-AllowPendingPvcForPreinfra:($initialGenesisMarkerState -ceq ''preinfra'')', '-AllowPendingPvcForPreinfra:$true'),
        $invokeK8sSource.Replace('if (-not $pandoraNamespaceExistedBeforeStart)', 'if ($pandoraNamespaceExistedBeforeStart)'),
        $invokeK8sSource.Replace('$allowLegacyInfraOnlyAdoption = $pandoraNamespaceExistedBeforeStart -and', '$allowLegacyInfraOnlyAdoption = $true -and'),
        $invokeK8sSource.Replace('-ExpectedAdoptionPvcUid $legacyAdoptionPvcUid', '-ExpectedAdoptionPvcUid $null'),
        $invokeK8sSource.Replace(
            '-not [string]::IsNullOrWhiteSpace($legacyAdoptionPvcUid)',
            '-not [string]::IsNullOrWhiteSpace($legacyAdoptionPvcUid) -or $true'))) {
    Assert-Throws {
        Assert-OneClickGenesisAuthorization -InvokeSource $mutant
    } '一键入口授权条件被放宽/禁用时 mutant 契约必须失败'
}

$baselineFunction = Get-OnlyFunctionAst $startAst 'Assert-LocalDsAuthBaseline'
$baselineSource = $baselineFunction.Extent.Text
Assert-BaselineGenesisAuthorization -BaselineSource $baselineSource
Assert-True (-not $baselineSource.Contains('-AllowNotReady')) `
    'apply 后 baseline cohort 复算必须保持严格 Ready，禁止沿用 preflight 放宽开关'
$requiredRead = $baselineSource.IndexOf('go run ./pkg/dsauthfence/cmd/dsauth-required', [StringComparison]::Ordinal)
$completeMissingGuard = $baselineSource.IndexOf("markerState -ceq 'complete'", [StringComparison]::Ordinal)
$legacyCohortRecheck = $baselineSource.IndexOf('Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext $KubeContext', [StringComparison]::Ordinal)
$firstPristine = $baselineSource.IndexOf('Assert-LocalEtcdStorePristineForGenesis', [StringComparison]::Ordinal)
$firstZeroWriter = $baselineSource.IndexOf('Assert-NoLocalFreshGenesisWriters', [StringComparison]::Ordinal)
$adoptIntent = $baselineSource.IndexOf('New-LocalAdoptedGenesisIntent', [StringComparison]::Ordinal)
$prepareContinuity = $baselineSource.IndexOf('--prepare-zero-writer-genesis-v3', [StringComparison]::Ordinal)
$setPending = $baselineSource.IndexOf(
    'Set-LocalFreshGenesisIntentState -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile -TargetState pending',
    [StringComparison]::Ordinal)
$lastZeroWriter = $baselineSource.LastIndexOf('Assert-NoLocalFreshGenesisWriters', [StringComparison]::Ordinal)
$evidencePersist = $baselineSource.IndexOf('Get-OrSetLocalFreshGenesisEvidenceCompletedAtMS', [StringComparison]::Ordinal)
$genesisCas = $baselineSource.IndexOf('--zero-writer-genesis-v3', [StringComparison]::Ordinal)
$requiredVerify = $baselineSource.LastIndexOf('go run ./pkg/dsauthfence/cmd/dsauth-required', [StringComparison]::Ordinal)
$completeMarker = $baselineSource.LastIndexOf(
    'Set-LocalFreshGenesisIntentState -KubeContext $KubeContext -MinikubeProfile $MinikubeProfile -TargetState complete',
    [StringComparison]::Ordinal)
foreach ($position in @($requiredRead, $completeMissingGuard, $legacyCohortRecheck, $firstPristine, $firstZeroWriter,
        $adoptIntent, $prepareContinuity, $setPending, $lastZeroWriter, $evidencePersist, $genesisCas,
        $requiredVerify, $completeMarker)) {
    Assert-True ($position -ge 0) 'baseline 状态机缺少 required/marker/pristine/zero-writer/CAS/verify/complete 调用'
}
Assert-True ($requiredRead -lt $completeMissingGuard -and $completeMissingGuard -lt $legacyCohortRecheck -and
    $legacyCohortRecheck -lt $firstPristine) `
    '必须先线性分类 required；complete+missing 要 fail closed，legacy cohort 复算必须早于收养审计'
Assert-True ($legacyCohortRecheck -lt $firstPristine -and $firstPristine -lt $firstZeroWriter -and $firstZeroWriter -lt $adoptIntent -and
    $adoptIntent -lt $prepareContinuity -and $prepareContinuity -lt $setPending -and
    $setPending -lt $lastZeroWriter -and $lastZeroWriter -lt $evidencePersist -and
    $evidencePersist -lt $genesisCas -and
    $genesisCas -lt $requiredVerify -and $requiredVerify -lt $completeMarker) `
    'legacy 收养必须 adopting 后 prepare/verify continuity 才 pending，并在 exact V3 verify 后 complete'
Assert-True ($baselineSource.Contains("markerState -cne 'pending'")) `
    'missing->V3 CAS 前必须将授权状态收敛为 pending'
Assert-True ($baselineSource.Contains('exact continuity + required/record create-only + activation lock')) `
    'CAS 前注释/契约必须保留 continuity 与完整 authority prefix 线性化兜底'
Assert-True ([regex]::Matches($baselineSource, '--require-activation-evidence-sha256').Count -ge 2 -and
    [regex]::Matches($baselineSource, '--require-activation-evidence-completed-at-ms').Count -ge 2) `
    'pending/complete 崩溃恢复与 CAS 后 verify 都必须精确核对同一个 evidence sha+time'
Assert-True ([regex]::Matches($baselineSource, '--require-genesis-continuity-token').Count -ge 2 -and
    [regex]::Matches($baselineSource, '--verify-genesis-continuity').Count -ge 3 -and
    $baselineSource.Contains('--prepare-zero-writer-genesis-v3')) `
    'pending/complete/CAS 必须绑定 exact create-only continuity sentinel，pending missing 禁止重建'

$evidenceFunction = Get-OnlyFunctionAst $startAst 'Get-OrSetLocalFreshGenesisEvidenceCompletedAtMS'
$evidenceSource = $evidenceFunction.Extent.Text
Assert-True ($evidenceSource.Contains("state -cne 'pending'")) `
    '只有 pending marker 可以持久化 genesis evidence time'
Assert-True ($evidenceSource.Contains('resourceVersion = [string]$marker.metadata.resourceVersion')) `
    'genesis evidence time 必须以 marker resourceVersion CAS 持久化'
$preinfraMutantStart = $baselineSource.IndexOf("if (`$markerState -cin @('preinfra', 'adopting'))", [StringComparison]::Ordinal)
$preinfraMutantEnd = $baselineSource.IndexOf("if (`$markerState -cne 'pending')", $preinfraMutantStart, [StringComparison]::Ordinal)
$preinfraMutantBranch = $baselineSource.Substring($preinfraMutantStart, $preinfraMutantEnd - $preinfraMutantStart)
$preinfraMutantBranch = $preinfraMutantBranch.Replace('Assert-LocalEtcdStorePristineForGenesis', 'Removed-LocalEtcdStorePristineForGenesis').Replace(
    'Assert-NoLocalFreshGenesisWriters', 'Removed-NoLocalFreshGenesisWriters').Replace(
    '--prepare-zero-writer-genesis-v3', '--removed-prepare-zero-writer-genesis-v3')
$preinfraGateMutant = $baselineSource.Substring(0, $preinfraMutantStart) + $preinfraMutantBranch + $baselineSource.Substring($preinfraMutantEnd)
$preinfraOriginalBranch = $baselineSource.Substring($preinfraMutantStart, $preinfraMutantEnd - $preinfraMutantStart)
$preinfraSetLine = @($preinfraOriginalBranch -split "`r?`n" | Where-Object { $_ -match 'Set-LocalFreshGenesisIntentState.+-TargetState pending' })[0]
$preinfraOrderMutantBranch = $preinfraOriginalBranch.Replace($preinfraSetLine, '').Replace(
    'Assert-LocalEtcdStorePristineForGenesis -KubeContext $KubeContext',
    "$preinfraSetLine`n                Assert-LocalEtcdStorePristineForGenesis -KubeContext `$KubeContext")
$preinfraOrderMutant = $baselineSource.Substring(0, $preinfraMutantStart) + $preinfraOrderMutantBranch + $baselineSource.Substring($preinfraMutantEnd)
$resumeGuardMutantStart = $baselineSource.IndexOf('if (-not $AllowFreshBootstrap)', [StringComparison]::Ordinal)
$resumeGuardMutantEnd = $baselineSource.IndexOf('if (-not $requiredIsMissing)', $resumeGuardMutantStart, [StringComparison]::Ordinal)
$resumeGuardBranch = $baselineSource.Substring($resumeGuardMutantStart, $resumeGuardMutantEnd - $resumeGuardMutantStart)
$resumeGuardWarnBranch = ([regex]::new('(?m)^(\s*)throw\s')).Replace($resumeGuardBranch, '$1Write-Warn ', 1)
$resumeGuardWarnMutant = $baselineSource.Substring(0, $resumeGuardMutantStart) + $resumeGuardWarnBranch + $baselineSource.Substring($resumeGuardMutantEnd)
$pendingGuardMutantStart = $baselineSource.IndexOf("if (`$markerState -cne 'pending')", [StringComparison]::Ordinal)
$pendingGuardMutantEnd = $baselineSource.IndexOf('# pending 以后 sentinel', $pendingGuardMutantStart, [StringComparison]::Ordinal)
$pendingGuardBranch = $baselineSource.Substring($pendingGuardMutantStart, $pendingGuardMutantEnd - $pendingGuardMutantStart)
$pendingGuardWarnBranch = ([regex]::new('(?m)^(\s*)throw\s')).Replace($pendingGuardBranch, '$1Write-Warn ', 1)
$pendingGuardWarnMutant = $baselineSource.Substring(0, $pendingGuardMutantStart) + $pendingGuardWarnBranch + $baselineSource.Substring($pendingGuardMutantEnd)
foreach ($mutant in @(
        $baselineSource.Replace('$requiredIsMissing = $readText -match', '$requiredIsMissing = $readText -notmatch'),
        $baselineSource.Replace('if (-not $AllowFreshBootstrap)', 'if ($AllowFreshBootstrap)'),
        $baselineSource.Replace('if (-not $AllowLegacyInfraOnlyAdoption -or [string]::IsNullOrWhiteSpace($ExpectedAdoptionPvcUid))', 'if ($false)'),
        $baselineSource.Replace('Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext $KubeContext', 'Removed-LocalLegacyInfraOnlyAdoptionCohort'),
        $baselineSource.Replace('$recheckedCohort.FingerprintSha256 -cne $ExpectedAdoptionCohortFingerprintSha256', '$false'),
        $baselineSource.Replace('[System.Threading.Mutex]::new', '[System.Threading.RemovedMutex]::new'),
        $baselineSource.Replace('$prepareMarker = Get-LocalFreshGenesisIntent', '$prepareMarker = $marker'),
        $preinfraGateMutant,
        $preinfraOrderMutant,
        $resumeGuardWarnMutant,
        $pendingGuardWarnMutant)) {
    Assert-Throws {
        Assert-BaselineGenesisAuthorization -BaselineSource $mutant
    } 'baseline missing/allow/adoption/preinfra 门禁被反转或删除时 mutant 契约必须失败'
}

$adoptFunction = Get-OnlyFunctionAst $startAst 'New-LocalAdoptedGenesisIntent'
$adoptSource = $adoptFunction.Extent.Text
Assert-True ($adoptSource.Contains("LocalFreshDsAuthMarkerStateAnnotation = 'adopting'")) `
    'legacy 收养 marker 创建时必须先是无 CAS 权限的 adopting'
Assert-True ($adoptSource.Contains('LocalFreshDsAuthMarkerPvcAnnotation = $pvcUid')) `
    'legacy 收养 marker 必须在同一个 create 中绑定 PVC UID'
Assert-True ($adoptSource.Contains('LocalLegacyAdoptionCohortFingerprintField = $ExpectedCohortFingerprintSha256')) `
    'legacy 收养 marker 必须在同一个 create 中持久化 cohort fingerprint'
Assert-True ($adoptSource.Contains('LocalFreshDsAuthContinuityTokenField = $continuityToken')) `
    'legacy adopting marker 必须在同一个 create 中持久化随机 continuity token'
Assert-True ($adoptSource.Contains("pvc.status.phase -cne 'Bound'")) `
    'legacy 收养前必须要求 PVC Bound'
Assert-True (-not $adoptSource.Contains('Set-LocalFreshGenesisIntentState')) `
    'legacy 收养函数自身不得在 sentinel prepare 前偷推进 pending'
$intentValidator = Get-OnlyFunctionAst $startAst 'Assert-LocalFreshGenesisIntent'
$intentValidatorSource = $intentValidator.Extent.Text
Assert-True ($intentValidatorSource.Contains("cohortFingerprint -cnotmatch '^sha256:[0-9a-f]{64}$'") -and
    $intentValidatorSource.Contains("continuityToken -cnotmatch '^nonce:[0-9a-f]{64}$'") -and
    $intentValidatorSource.Contains('-cnotin') -and $intentValidatorSource.Contains('-cin')) `
    'marker validator 必须校验 cohort/continuity，且 origin/state 集合必须严格区分大小写'

$observedWitnessFunction = Get-OnlyFunctionAst $startAst 'Ensure-LocalObservedV3Witness'
$observedWitnessSource = $observedWitnessFunction.Extent.Text
Assert-True ($observedWitnessSource.Contains('immutable = $true') -and
    $observedWitnessSource.Contains('etcd_pvc_uid = [string]$pvc.metadata.uid')) `
    'markerless exact V3 必须建立绑定 PVC 的 immutable terminal witness'
Assert-True ($baselineSource.Contains('observed V3 witness 建立后线性复查失败') -and
    $baselineSource.Contains('已有 immutable observed V3 witness')) `
    'observed witness 创建后必须复查 V3，未来 witness+missing 必须阻断 adoption'

$cohortFunction = Get-OnlyFunctionAst $startAst 'Get-LocalLegacyInfraOnlyAdoptionCohort'
$cohortSource = $cohortFunction.Extent.Text
foreach ($fragment in @(
        'LocalLegacyAdoptionCohortMaxAgeMS',
        'LocalLegacyAdoptionCohortMaxSpanMS',
        "claimRef.uid -cne `$pvcUid",
        "pvProvisioner -cne 'k8s.io/minikube-hostpath'",
        "persistentVolumeReclaimPolicy -cne 'Delete'",
        "storageClassName -cne 'standard'",
        'createdTimes.Count -ne 18',
        'FingerprintSha256 = "sha256:$hex"')) {
    Assert-True ($cohortSource.Contains($fragment)) "legacy cohort helper 缺少严格证据:$fragment"
}

$newNamespaceAnchorFunction = Get-OnlyFunctionAst $startAst 'New-LocalFreshAnchoredNamespace'
$newNamespaceAnchorSource = $newNamespaceAnchorFunction.Extent.Text
Assert-True ($newNamespaceAnchorSource.Contains("kind = 'Namespace'")) `
    'fresh anchor 必须与 Namespace create 同一个 API 对象原子落盘'
Assert-True ($newNamespaceAnchorSource.Contains('LocalFreshNamespaceAnchorKubeSystemUidAnnotation = $kubeSystemUid')) `
    'fresh namespace anchor 必须绑定 kube-system UID'
Assert-True ($newNamespaceAnchorSource.Contains('kubectl --context $KubeContext create -f -')) `
    'fresh namespace 必须 create-only，不能 apply 覆盖旧 namespace 冒充 fresh'
$removeNamespaceAnchorFunction = Get-OnlyFunctionAst $startAst 'Remove-LocalFreshNamespaceAnchor'
$removeNamespaceAnchorSource = $removeNamespaceAnchorFunction.Extent.Text
$anchorMarkerVerify = $removeNamespaceAnchorSource.IndexOf('Assert-LocalFreshGenesisIntent', [StringComparison]::Ordinal)
$anchorPatch = $removeNamespaceAnchorSource.IndexOf('kubectl --context $KubeContext patch', [StringComparison]::Ordinal)
Assert-True ($anchorMarkerVerify -ge 0 -and $anchorMarkerVerify -lt $anchorPatch) `
    '只有正式 immutable marker 回读验证后才可 patch 移除 namespace anchor'
Assert-True ($removeNamespaceAnchorSource.Contains('LocalFreshNamespaceAnchorSchemaAnnotation = $null')) `
    'namespace anchor 移除必须显式删除 anchor annotations'

$pristineFunction = Get-OnlyFunctionAst $startAst 'Assert-LocalEtcdStorePristineForGenesis'
$pristineSource = $pristineFunction.Extent.Text
Assert-True ($pristineSource.Contains('get /pandora/ds-auth/ --prefix --limit=1 --consistency=l -w json')) `
    'legacy 收养必须读取整个 ds-auth prefix'
Assert-True ($pristineSource.Contains('[long]$header.Value.revision -ne 1')) `
    'legacy 收养必须拒绝任何发生过写入的 etcd revision'

# 隔离执行 legacy cohort：覆盖 pwsh 7.6 将 K8s creationTimestamp 解析成 Utc DateTime、
# 缺失 initContainers/ephemeralContainers 的 StrictMode 路径，以及 UID/PV/时窗 fail-closed。
Invoke-Expression $cohortFunction.Extent.Text
$script:K8sNamespace = 'pandora'
$script:LocalLegacyAdoptionCohortMaxAgeMS = 2L * 60L * 60L * 1000L
$script:LocalLegacyAdoptionCohortMaxSpanMS = 10L * 60L * 1000L
$script:CohortCollectedAtMS = 1800000000000L
$script:CohortBaseCreatedAtMS = $script:CohortCollectedAtMS - 5L * 60L * 1000L
$script:CohortUidSequence = 0
function New-CohortUid {
    $script:CohortUidSequence++
    return ('00000000-0000-0000-0000-{0:d12}' -f $script:CohortUidSequence)
}
function New-CohortMetadata {
    param(
        [string]$Name,
        [long]$CreatedAtMS,
        [string]$Namespace = '',
        [object]$Labels = $null,
        [object]$Annotations = $null,
        [object[]]$Owners = @(),
        [long]$Generation = 0
    )
    $metadata = [ordered]@{
        name = $Name
        uid = New-CohortUid
        # 模拟 pwsh 7.6 ConvertFrom-Json 的真实 K8s 行为：Kind=Utc DateTime，而不是 ISO string。
        creationTimestamp = [DateTimeOffset]::FromUnixTimeMilliseconds($CreatedAtMS).UtcDateTime
    }
    if (-not [string]::IsNullOrWhiteSpace($Namespace)) { $metadata.namespace = $Namespace }
    if ($null -ne $Labels) { $metadata.labels = $Labels }
    if ($null -ne $Annotations) { $metadata.annotations = $Annotations }
    if ($Owners.Count -gt 0) { $metadata.ownerReferences = @($Owners) }
    if ($Generation -gt 0) { $metadata.generation = $Generation }
    return [pscustomobject]$metadata
}
function New-CohortPodSpec([string]$App, [string]$Image) {
    # 故意不提供 initContainers/ephemeralContainers，真实 canonical Pod 也是缺字段而非空数组。
    $container = [ordered]@{ name = $App; image = $Image }
    $volumes = $null
    if ($App -ceq 'etcd') {
        $container['command'] = @(
            '/usr/local/bin/etcd', '--name=pandora-etcd', '--data-dir=/etcd-data',
            '--listen-client-urls=http://0.0.0.0:2379', '--advertise-client-urls=http://etcd:2379',
            '--listen-peer-urls=http://0.0.0.0:2380', '--initial-advertise-peer-urls=http://etcd:2380',
            '--initial-cluster=pandora-etcd=http://etcd:2380', '--initial-cluster-token=pandora-etcd-cluster',
            '--initial-cluster-state=new'
        )
        $container['volumeMounts'] = @([pscustomobject]@{ name = 'data'; mountPath = '/etcd-data' })
        $volumes = @([pscustomobject]@{
            name = 'data'; persistentVolumeClaim = [pscustomobject]@{ claimName = 'etcd-data' }
        })
    }
    $podSpec = [ordered]@{ containers = @([pscustomobject]$container) }
    if ($null -ne $volumes) { $podSpec['volumes'] = $volumes }
    return [pscustomobject]$podSpec
}
function Initialize-CohortFixture {
    $script:CohortUidSequence = 0
    $tick = 0L
    function Next-CreatedAt { $script:CohortBaseCreatedAtMS + ($script:CohortUidSequence * 1000L) }
    $script:CohortNamespace = [pscustomobject]@{
        kind = 'Namespace'
        metadata = New-CohortMetadata -Name 'pandora' -CreatedAtMS (Next-CreatedAt)
    }
    $script:CohortPvcUid = New-CohortUid
    $pvcMetadata = [pscustomobject]@{
        name = 'etcd-data'; namespace = 'pandora'; uid = $script:CohortPvcUid
        creationTimestamp = [DateTimeOffset]::FromUnixTimeMilliseconds((Next-CreatedAt)).UtcDateTime
    }
    $script:CohortPvName = "pvc-$($script:CohortPvcUid)"
    $script:CohortPvc = [pscustomobject]@{
        kind = 'PersistentVolumeClaim'; metadata = $pvcMetadata
        spec = [pscustomobject]@{
            accessModes = @('ReadWriteOnce'); storageClassName = 'standard'
            volumeMode = 'Filesystem'; volumeName = $script:CohortPvName
        }
        status = [pscustomobject]@{ phase = 'Bound' }
    }
    $pvMetadata = New-CohortMetadata -Name $script:CohortPvName -CreatedAtMS (Next-CreatedAt) `
        -Annotations ([pscustomobject]@{ 'pv.kubernetes.io/provisioned-by' = 'k8s.io/minikube-hostpath' })
    $script:CohortPv = [pscustomobject]@{
        kind = 'PersistentVolume'; metadata = $pvMetadata
        spec = [pscustomobject]@{
            claimRef = [pscustomobject]@{
                kind = 'PersistentVolumeClaim'; namespace = 'pandora'; name = 'etcd-data'; uid = $script:CohortPvcUid
            }
            storageClassName = 'standard'; persistentVolumeReclaimPolicy = 'Delete'
            volumeMode = 'Filesystem'; accessModes = @('ReadWriteOnce')
            hostPath = [pscustomobject]@{ path = '/tmp/hostpath-provisioner/pandora/etcd-data'; type = '' }
        }
        status = [pscustomobject]@{ phase = 'Bound' }
    }
    $images = [ordered]@{
        mysql = 'mysql:8.4'; redis = 'redis:8.8.0-alpine'
        zookeeper = 'confluentinc/cp-zookeeper:7.9.7'; kafka = 'confluentinc/cp-kafka:7.9.7'
        etcd = 'quay.io/coreos/etcd:v3.6.12'
    }
    $items = [System.Collections.Generic.List[object]]::new()
    foreach ($app in @($images.Keys)) {
        $image = [string]$images[$app]
        $appLabels = [pscustomobject]@{ app = $app }
        $depMetadata = New-CohortMetadata -Name $app -Namespace 'pandora' -CreatedAtMS (Next-CreatedAt) `
            -Labels $appLabels -Annotations ([pscustomobject]@{ 'deployment.kubernetes.io/revision' = '1' }) -Generation 1
        $deployment = [pscustomobject]@{
            kind = 'Deployment'; metadata = $depMetadata
            spec = [pscustomobject]@{
                replicas = 1; selector = [pscustomobject]@{ matchLabels = $appLabels }
                template = [pscustomobject]@{
                    metadata = [pscustomobject]@{ labels = $appLabels }
                    spec = New-CohortPodSpec $app $image
                }
            }
            status = [pscustomobject]@{
                observedGeneration = 1; replicas = 1; updatedReplicas = 1; readyReplicas = 1; availableReplicas = 1
            }
        }
        $null = $items.Add($deployment)
        $hash = "hash$($script:CohortUidSequence)"
        $rsLabels = [pscustomobject]@{ app = $app; 'pod-template-hash' = $hash }
        $depOwner = [pscustomobject]@{
            controller = $true; kind = 'Deployment'; name = $app; uid = [string]$depMetadata.uid
        }
        $rsMetadata = New-CohortMetadata -Name "$app-$hash" -Namespace 'pandora' -CreatedAtMS (Next-CreatedAt) `
            -Labels $rsLabels -Annotations ([pscustomobject]@{ 'deployment.kubernetes.io/revision' = '1' }) `
            -Owners @($depOwner) -Generation 1
        $replicaSet = [pscustomobject]@{
            kind = 'ReplicaSet'; metadata = $rsMetadata
            spec = [pscustomobject]@{
                replicas = 1; selector = [pscustomobject]@{ matchLabels = $rsLabels }
                template = [pscustomobject]@{
                    metadata = [pscustomobject]@{ labels = $rsLabels }
                    spec = New-CohortPodSpec $app $image
                }
            }
            status = [pscustomobject]@{
                observedGeneration = 1; replicas = 1; readyReplicas = 1; availableReplicas = 1
            }
        }
        $null = $items.Add($replicaSet)
        $rsOwner = [pscustomobject]@{
            controller = $true; kind = 'ReplicaSet'; name = [string]$rsMetadata.name; uid = [string]$rsMetadata.uid
        }
        $podMetadata = New-CohortMetadata -Name "$app-$hash-pod" -Namespace 'pandora' -CreatedAtMS (Next-CreatedAt) `
            -Labels $rsLabels -Owners @($rsOwner)
        $pod = [pscustomobject]@{
            kind = 'Pod'; metadata = $podMetadata; spec = New-CohortPodSpec $app $image
            status = [pscustomobject]@{
                phase = 'Running'; conditions = @([pscustomobject]@{ type = 'Ready'; status = 'True' })
            }
        }
        $null = $items.Add($pod)
    }
    $script:CohortWorkloads = [pscustomobject]@{ items = $items.ToArray() }
}
function Get-KubectlJsonObject {
    param([string]$KubeContext, [object[]]$Arguments, [string]$Action)
    $resource = [string]$Arguments[1]
    if ($resource -ceq 'namespace/pandora') { return $script:CohortNamespace }
    if ($resource -ceq 'pvc/etcd-data') { return $script:CohortPvc }
    if ($resource -ceq "persistentvolume/$($script:CohortPvName)") { return $script:CohortPv }
    if ($resource -ceq 'deployment,replicaset,pod') { return $script:CohortWorkloads }
    throw "unexpected cohort fixture resource:$resource"
}
Initialize-CohortFixture
$cohort = Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
    -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
Assert-True ([string]$cohort.FingerprintSha256 -cmatch '^sha256:[0-9a-f]{64}$') `
    'canonical short-lived infra cohort 必须生成 lowercase sha256 fingerprint'
$cohortAgain = Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
    -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
Assert-True ([string]$cohortAgain.FingerprintSha256 -ceq [string]$cohort.FingerprintSha256) `
    '同一 collectedAt + 同一对象集合重算 fingerprint 必须稳定'

# 上次一键在 Pod 拉镜像/Ready 前中断时，本轮必须能在 apply 前先固定同一批对象身份，
# 等 rollout 后再走默认严格 Ready 复算；不能要求用户为了 preflight 再双击第二次。
foreach ($item in @($script:CohortWorkloads.items)) {
    if ([string]$item.kind -ceq 'Deployment') {
        $item.status = [pscustomobject]@{ observedGeneration = 1; replicas = 1; updatedReplicas = 1 }
    } elseif ([string]$item.kind -ceq 'ReplicaSet') {
        $item.status = [pscustomobject]@{ observedGeneration = 1; replicas = 1 }
    } elseif ([string]$item.kind -ceq 'Pod') {
        $item.status = [pscustomobject]@{ phase = 'Pending'; conditions = @() }
    }
}
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} '严格 baseline cohort 仍必须要求 infra Ready'
$notReadyCohort = Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
    -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid -AllowNotReady
Assert-True ([string]$notReadyCohort.FingerprintSha256 -ceq [string]$cohort.FingerprintSha256) `
    'apply 前未 Ready cohort 必须固定与 Ready 后相同的 UID/owner/spec/timestamp fingerprint'
Initialize-CohortFixture

$mysqlDeployment = @($script:CohortWorkloads.items | Where-Object {
    [string]$_.kind -ceq 'Deployment' -and [string]$_.metadata.name -ceq 'mysql'
})[0]
$mysqlRevisionProperty = $mysqlDeployment.metadata.annotations.PSObject.Properties['deployment.kubernetes.io/revision']
$mysqlRevisionProperty.Value = '2'
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} 'legacy cohort 只接受刚创建且 revision=1 的 Deployment'
$mysqlRevisionProperty.Value = '1'
$mysqlDeployment.metadata.generation = 2
$mysqlDeployment.status.observedGeneration = 2
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} 'Deployment 即使 revision 仍为1，发生过 scale/spec 操作导致 generation>1 也必须阻断'
$mysqlDeployment.metadata.generation = 1
$mysqlDeployment.status.observedGeneration = 1
$mysqlReplicaSet = @($script:CohortWorkloads.items | Where-Object {
    [string]$_.kind -ceq 'ReplicaSet' -and [string]$_.metadata.labels.app -ceq 'mysql'
})[0]
$mysqlReplicaSet.metadata.generation = 2
$mysqlReplicaSet.status.observedGeneration = 2
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} '唯一 ReplicaSet generation>1 也不属于首批未动过 cohort'
$mysqlReplicaSet.metadata.generation = 1
$mysqlReplicaSet.status.observedGeneration = 1
$etcdDeployment = @($script:CohortWorkloads.items | Where-Object {
    [string]$_.kind -ceq 'Deployment' -and [string]$_.metadata.name -ceq 'etcd'
})[0]
$originalEtcdDataDir = [string]$etcdDeployment.spec.template.spec.containers[0].command[2]
$etcdDeployment.spec.template.spec.containers[0].command[2] = '--data-dir=/tmp/not-the-pvc'
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} 'etcd 未精确使用 --data-dir=/etcd-data 时必须阻断'
$etcdDeployment.spec.template.spec.containers[0].command[2] = $originalEtcdDataDir
$etcdDeployment.spec.template.spec.containers[0] | Add-Member -NotePropertyName args `
    -NotePropertyValue @('--data-dir=/etcd-data/other-empty-dir')
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} 'etcd container.args 可覆盖固定 command 时必须阻断'
$etcdDeployment.spec.template.spec.containers[0].PSObject.Properties.Remove('args')
$etcdDataMount = $etcdDeployment.spec.template.spec.containers[0].volumeMounts[0]
$etcdDataMount | Add-Member -NotePropertyName subPath -NotePropertyValue 'other-empty-dir'
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} 'etcd /etcd-data 使用 subPath 指向另一空目录时必须阻断'
$etcdDataMount.PSObject.Properties.Remove('subPath')
$originalNamespaceTime = $script:CohortNamespace.metadata.creationTimestamp
$script:CohortNamespace.metadata.creationTimestamp = [DateTimeOffset]::FromUnixTimeMilliseconds(
    $script:CohortCollectedAtMS - $script:LocalLegacyAdoptionCohortMaxAgeMS - 1).UtcDateTime
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} '任一 cohort 对象超过两小时必须阻断'
$script:CohortNamespace.metadata.creationTimestamp = $originalNamespaceTime
$script:CohortNamespace.metadata.creationTimestamp = [DateTimeOffset]::FromUnixTimeMilliseconds(
    $script:CohortCollectedAtMS - 20L * 60L * 1000L).UtcDateTime
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} 'cohort 全体 creationTimestamp 跨度超过十分钟必须阻断'
$script:CohortNamespace.metadata.creationTimestamp = $originalNamespaceTime
$originalClaimUid = $script:CohortPv.spec.claimRef.uid
$script:CohortPv.spec.claimRef.uid = '00000000-0000-0000-0000-999999999999'
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} 'PV claimRef.uid 不匹配 PVC UID 必须阻断'
$script:CohortPv.spec.claimRef.uid = $originalClaimUid
$provisionerProperty = $script:CohortPv.metadata.annotations.PSObject.Properties['pv.kubernetes.io/provisioned-by']
$provisionerProperty.Value = 'example.invalid/provisioner'
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} '非 minikube-hostpath provisioner 必须阻断'
$provisionerProperty.Value = 'k8s.io/minikube-hostpath'
$pod = @($script:CohortWorkloads.items | Where-Object { [string]$_.kind -ceq 'Pod' })[0]
$originalPodUid = [string]$pod.metadata.uid
$pod.metadata.uid = '00000000-0000-0000-0000-888888888888'
$changedCohort = Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
    -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
Assert-True ([string]$changedCohort.FingerprintSha256 -cne [string]$cohort.FingerprintSha256) `
    '任一 cohort 对象 UID 漂移必须改变 fingerprint'
$pod.metadata.uid = $originalPodUid
$replicaSet = @($script:CohortWorkloads.items | Where-Object { [string]$_.kind -ceq 'ReplicaSet' })[0]
$duplicateReplicaSet = ($replicaSet | ConvertTo-Json -Depth 20 | ConvertFrom-Json)
$duplicateReplicaSet.metadata.uid = '00000000-0000-0000-0000-777777777777'
$duplicateReplicaSet.metadata.name = "$([string]$replicaSet.metadata.name)-duplicate"
$script:CohortWorkloads.items += $duplicateReplicaSet
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} '同一 Deployment 出现多个 current ReplicaSet 必须阻断'
$script:CohortWorkloads.items = @($script:CohortWorkloads.items | Where-Object { $_ -ne $duplicateReplicaSet })
$orphanPod = ($pod | ConvertTo-Json -Depth 20 | ConvertFrom-Json)
$orphanPod.metadata.uid = '00000000-0000-0000-0000-666666666666'
$orphanPod.metadata.name = "$([string]$pod.metadata.name)-orphan"
$orphanPod.metadata.ownerReferences[0].uid = '00000000-0000-0000-0000-555555555555'
$script:CohortWorkloads.items += $orphanPod
Assert-Throws {
    Get-LocalLegacyInfraOnlyAdoptionCohort -KubeContext 'minikube' `
        -CollectionTimeUnixMS $script:CohortCollectedAtMS -ExpectedPvcUid $script:CohortPvcUid
} '同 app orphan Pod 必须阻断 legacy cohort'
$script:CohortWorkloads.items = @($script:CohortWorkloads.items | Where-Object { $_ -ne $orphanPod })

# 隔离执行 zero-writer guard，覆盖真实 kubectl JSON 中 Deployment 没有 ownerReferences 的
# StrictMode 路径；静态 Contains 测试无法发现 `$null.controller` 这类运行时错误。
$writerGuardFunction = Get-OnlyFunctionAst $startAst 'Assert-NoLocalFreshGenesisWriters'
Invoke-Expression $writerGuardFunction.Extent.Text
$script:K8sNamespace = 'pandora'
$script:WriterGuardAgonesObject = ''
function Write-Ok([string]$Message) { }
function kubectl {
    $global:LASTEXITCODE = 0
    if (-not [string]::IsNullOrWhiteSpace($script:WriterGuardAgonesObject) -and
        $args -contains $script:WriterGuardAgonesObject) {
        return "$($script:WriterGuardAgonesObject).agones.dev/test"
    }
}
function New-InfraDeploymentFixture([string]$Name, [string]$Image) {
    return [pscustomobject]@{
        kind = 'Deployment'
        metadata = [pscustomobject]@{
            namespace = 'pandora'; name = $Name; labels = [pscustomobject]@{ app = $Name }
        }
        spec = [pscustomobject]@{
            replicas = 1
            template = [pscustomobject]@{
                spec = [pscustomobject]@{ containers = @([pscustomobject]@{ image = $Image }) }
            }
        }
    }
}
$script:WriterGuardItems = @(
    New-InfraDeploymentFixture 'mysql' 'mysql:8.4'
    New-InfraDeploymentFixture 'redis' 'redis:8.8.0-alpine'
    New-InfraDeploymentFixture 'zookeeper' 'confluentinc/cp-zookeeper:7.9.7'
    New-InfraDeploymentFixture 'kafka' 'confluentinc/cp-kafka:7.9.7'
    New-InfraDeploymentFixture 'etcd' 'quay.io/coreos/etcd:v3.6.12'
)
function Get-KubectlJsonObject {
    return [pscustomobject]@{ items = @($script:WriterGuardItems) }
}
Assert-DoesNotThrow {
    Assert-NoLocalFreshGenesisWriters -KubeContext 'minikube'
} 'canonical 五基础设施 Deployment（无 ownerReferences）必须通过 zero-writer guard'
$businessFixture = [pscustomobject]@{
    kind = 'Deployment'
    metadata = [pscustomobject]@{
        namespace = 'pandora'; name = 'login'; labels = [pscustomobject]@{ app = 'login' }
    }
    spec = [pscustomobject]@{
        replicas = 0
        template = [pscustomobject]@{
            spec = [pscustomobject]@{ containers = @([pscustomobject]@{ image = 'pandora/login:dev' }) }
        }
    }
}
$script:WriterGuardItems += $businessFixture
Assert-Throws {
    Assert-NoLocalFreshGenesisWriters -KubeContext 'minikube'
} 'replicas=0 的 Pandora 业务 Deployment 也必须阻断 genesis'
$script:WriterGuardItems = @($script:WriterGuardItems | Where-Object { $_.metadata.name -cne 'login' })
$crossNamespaceWriter = [pscustomobject]@{
    kind = 'Deployment'
    metadata = [pscustomobject]@{
        namespace = 'default'; name = 'login'; labels = [pscustomobject]@{ app = 'login' }
    }
    spec = [pscustomobject]@{
        replicas = 0
        template = [pscustomobject]@{
            spec = [pscustomobject]@{ containers = @([pscustomobject]@{ image = 'corp.example/game/login:v3' }) }
        }
    }
}
$script:WriterGuardItems += $crossNamespaceWriter
Assert-Throws {
    Assert-NoLocalFreshGenesisWriters -KubeContext 'minikube'
} '其它 namespace 的 exact writer identity 即使镜像不含 pandora 也必须阻断 genesis'
$script:WriterGuardItems = @($script:WriterGuardItems | Where-Object { $_.metadata.namespace -ceq 'pandora' })
$script:WriterGuardAgonesObject = 'fleet'
Assert-Throws {
    Assert-NoLocalFreshGenesisWriters -KubeContext 'minikube'
} '任一 Agones Fleet 必须阻断 genesis'
$script:WriterGuardAgonesObject = ''
Assert-True ($writerGuardFunction.Extent.Text.Contains('replicationcontroller')) `
    'zero-writer workload 枚举必须覆盖可 scaled-to-zero 的 ReplicationController'

$invokeK8sCommands = @($invokeK8sFunctions[0].FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.CommandAst]
}, $true))
foreach ($deployment in @('mysql', 'redis', 'etcd', 'zookeeper', 'kafka')) {
    $rolloutCommands = @($invokeK8sCommands | Where-Object {
        $_.GetCommandName() -ceq 'kubectl' -and
            $_.Extent.Text -cmatch ('\brollout\s+status\s+deploy/' + [regex]::Escape($deployment) + '\b')
    })
    Assert-True ($rolloutCommands.Count -eq 1) "Invoke-K8s 必须有唯一 $deployment rollout 等待命令"
    Assert-True ($rolloutCommands[0].Extent.Text -cmatch '(?:^|\s)--timeout=1800s(?:\s|$)') `
        "$deployment 首次冷拉实际等待必须为 1800s，与 Deployment progress deadline 对齐"
}
Assert-True (@($invokeK8sCommands | Where-Object {
    $_.GetCommandName() -ceq 'kubectl' -and
        $_.Extent.Text -cmatch '\brollout\s+status\s+deploy/(?:loki|alloy)\b'
}).Count -eq 0) 'Loki/Alloy 只能放宽 Deployment deadline，不得变成阻断业务启动的 rollout 等待'
Assert-True ($source.Contains('第三方镜像首次冷拉每个最多 1800s/30 分钟')) `
    '启动提示必须准确说明第三方基础设施最长等待时间'
$resumeK8sFunctions = @($startAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -ceq 'Resume-K8s'
}, $true))
Assert-True ($resumeK8sFunctions.Count -eq 1) '必须有唯一 Resume-K8s'
$resumeEtcdRollout = @($resumeK8sFunctions[0].FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.CommandAst] -and
        $node.GetCommandName() -ceq 'kubectl' -and
        $node.Extent.Text -cmatch '\brollout\s+status\s+deploy/etcd\b'
}, $true))
Assert-True ($resumeEtcdRollout.Count -eq 1 -and
    $resumeEtcdRollout[0].Extent.Text -cmatch '(?:^|\s)--timeout=1800s(?:\s|$)') `
    'Resume 恢复 etcd 时也必须允许第三方镜像重新冷拉 30 分钟'
$applyAgonesFunctions = @($startAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -ceq 'Apply-AgonesManifests'
}, $true))
Assert-True ($applyAgonesFunctions.Count -eq 1) '必须有唯一 Apply-AgonesManifests'
$applyAgonesSource = $applyAgonesFunctions[0].Extent.Text
Assert-True ($applyAgonesSource.Contains('helm status agones') -and
    $applyAgonesSource.Contains("info.status -ceq 'deployed'")) `
    'Agones 必须按 Helm release deployed 状态判断，不能只凭 namespace 存在就跳过修复'
$agonesHelmInstall = @($applyAgonesFunctions[0].FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.CommandAst] -and
        $node.GetCommandName() -ceq 'helm' -and
        $node.Extent.Text -cmatch '\bupgrade\s+--install\s+agones\b'
}, $true))
Assert-True ($agonesHelmInstall.Count -eq 1 -and
    $agonesHelmInstall[0].Extent.Text -cmatch '(?:^|\s)--timeout\s+30m(?:\s|$)') `
    'Agones 首次安装必须把 Helm 默认 5 分钟等待放宽到 30 分钟'
$retryCommand = 'pwsh tools/scripts/e2e_k8s.ps1 -SkipImageLoad -MinikubeProfile $mkProfile -KubeContext $mkCtx -RelayBindHost $relayBind'
Assert-True ([regex]::Matches($source, [regex]::Escape($retryCommand)).Count -ge 2) `
    'start.ps1 两处失败提示都必须保留 profile/context/bind，不能诱导回环绑定回归'
$pathRefreshFunction = $source.IndexOf('function Sync-ProcessPathFromRegistry', [StringComparison]::Ordinal)
$pathRefreshCall = $source.IndexOf("Sync-ProcessPathFromRegistry`r`n", $pathRefreshFunction + 1, [StringComparison]::Ordinal)
$prerequisiteCall = $source.LastIndexOf('$prereqOk = Resolve-Prerequisites $Mode', [StringComparison]::Ordinal)
Assert-True ($pathRefreshFunction -ge 0 -and $pathRefreshCall -gt $pathRefreshFunction) `
    'start.ps1 必须在每次运行时合并 Windows 机器/用户 PATH，兼容长期运行 web 的旧环境快照'
Assert-True ($source.Contains("[Environment]::GetEnvironmentVariable('Path', 'Machine')")) `
    'PATH 刷新必须读取 Windows 机器级 PATH'
Assert-True ($source.Contains("[Environment]::GetEnvironmentVariable('Path', 'User')")) `
    'PATH 刷新必须读取 Windows 用户级 PATH'
Assert-True ($pathRefreshCall -lt $prerequisiteCall) `
    'PATH 必须在工具前置检查前刷新'
$profileBoundLoad = "Sync-ImagesToMinikube -Images (Get-ServiceImages) -MinikubeArgs @('-p', `$mkProfile)"
Assert-True ($source.Contains($profileBoundLoad)) `
    '本地 K8s 业务镜像 load 必须显式绑定本次已校验的 minikube profile'
Assert-True (-not $source.Contains('Sync-ImagesToMinikube -Images (Get-ServiceImages)' + [Environment]::NewLine)) `
    '不得恢复依赖 active profile 的业务镜像 load 调用'

# Windows Docker Desktop 偶尔在 --force-recreate 时延迟释放 8443/8444/9901。桥接脚本必须先
# 精确核验 service/config-file 标签、按容器 ID 停本 compose 的 envoy、等待三个端口释放，
# 再启动新容器；禁止通过进程级命令杀 Docker backend。
$bridgeSource = Get-Content -LiteralPath $bridgePath -Raw
$forwardEntries = @([regex]::Matches(
    $bridgeSource,
    '@\{\s*Name\s*=\s*''(?<name>[^'']+)'';\s*Port\s*=\s*(?<port>\d+);\s*Essential\s*=\s*\$(?<essential>true|false)\s*\}'))
Assert-True ($forwardEntries.Count -eq 21) 'bridge 必须映射全部 21 个 Go Deployment'
$forwardNames = @($forwardEntries | ForEach-Object { $_.Groups['name'].Value })
$forwardPorts = @($forwardEntries | ForEach-Object { [int]$_.Groups['port'].Value })
Assert-True (@($forwardNames | Sort-Object -Unique).Count -eq 21) 'bridge 服务名必须 21 个且唯一'
Assert-True (@($forwardPorts | Sort-Object -Unique).Count -eq 21) 'bridge 服务端口必须 21 个且唯一'
$ownerForward = @($forwardEntries | Where-Object {
    $_.Groups['name'].Value -ceq 'owner' -and
    [int]$_.Groups['port'].Value -eq 50017 -and
    $_.Groups['essential'].Value -ceq 'true'
})
Assert-True ($ownerForward.Count -eq 1) 'owner 必须唯一映射 50017 且作为必需权威服务'
$bridgeFunctions = @($bridgeAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
        $node.Name -ceq 'Stop-EnvoyForRecreate'
}, $true))
Assert-True ($bridgeFunctions.Count -eq 1) '必须有唯一 Stop-EnvoyForRecreate'
$stopFunction = $bridgeFunctions[0].Extent.Text
Assert-True ($stopFunction.Contains('com.docker.compose.service')) '必须核验 compose service 标签'
Assert-True ($stopFunction.Contains('com.docker.compose.project.config_files')) '必须核验 compose config_files 标签'
Assert-True ($stopFunction.Contains('[IO.Path]::GetFullPath')) 'config_files 标签必须规范化后比较'
Assert-True ($stopFunction.Contains("`$serviceLabel -cne 'envoy'")) 'service 标签不匹配必须进入拒绝条件'
Assert-True ($stopFunction.Contains('-not $actualConfigFile.Equals($expectedConfigFile, [StringComparison]::OrdinalIgnoreCase)')) `
    'config_files 规范化结果必须真正参与拒绝条件'
Assert-True ($stopFunction.Contains('throw "拒绝停止不属于当前 compose 文件的容器:')) `
    '任一归属标签不匹配都必须 fail closed'
Assert-True ($stopFunction.Contains('docker stop --time 10 $containerId')) '只能按已验证的不可变容器 ID stop'
Assert-True (-not $stopFunction.Contains('stop envoy')) '不得只凭可能跨 checkout 冲突的 service 名 stop'
Assert-True ($stopFunction.Contains('@(8443, 8444, 9901)')) '必须等待 8443/8444/9901 三个 listener 释放'
Assert-True ($stopFunction.Contains('Get-PortListenerPid')) '端口释放必须观察真实 listener'
Assert-True (-not $stopFunction.Contains('Stop-Process')) '不得为释放 Envoy 端口杀 Docker backend 进程'
Assert-True (-not $stopFunction.Contains('docker kill')) '不得用 docker kill 绕过受控停止'
Assert-True (-not $stopFunction.Contains('docker rm -f')) '不得强制删除未经完整停止的容器'
Assert-True (-not $stopFunction.Contains('taskkill')) '不得用 taskkill 杀 Docker backend'
Assert-True (-not $stopFunction.Contains('wsl --shutdown')) '不得关闭整个 WSL/Docker 运行时释放端口'
$stopCall = $bridgeSource.LastIndexOf('Stop-EnvoyForRecreate', [StringComparison]::Ordinal)
$upCall = $bridgeSource.IndexOf('up -d --force-recreate envoy', [StringComparison]::Ordinal)
Assert-True ($stopCall -ge 0 -and $upCall -gt $stopCall) '必须先等待旧 Envoy 端口释放，再 force-recreate'

# ── §9.21 禁删 Allocated 守卫(INC-20260803-002;对抗审查确认此前零回归覆盖)──────
# 变异口径:删掉 e2e 的 -ForceDeleteAllocated 门、或把 start.ps1 只删非 Allocated 的
# 逐台循环回退成整批删,下列断言必须失败。

# (1) e2e:必须有 -ForceDeleteAllocated 开关,且存在「有 Allocated 且未显式授权 → fail-closed」门。
Assert-True ($e2eSource.Contains('[switch]$ForceDeleteAllocated')) `
    'e2e_k8s.ps1 必须保留 -ForceDeleteAllocated 显式授权开关(§9.21)'
$e2eAllocatedGate = @($e2eAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.IfStatementAst] -and
        $node.Extent.Text -cmatch '\$allocatedByFleet\.Count\s+-gt\s+0\s+-and\s+-not\s+\$ForceDeleteAllocated'
}, $true))
Assert-True ($e2eAllocatedGate.Count -ge 1 -and $e2eAllocatedGate[0].Extent.Text.Contains('exit 1')) `
    'e2e_k8s.ps1 检测到 Allocated 且未传 -ForceDeleteAllocated 时必须 fail-closed 中止,不得静默连删(§9.21)'

# (2) e2e:所有按 label 的 GameServer 批删命令必须处于 $ForceDeleteAllocated 显式授权分支内。
function Test-InsideForceDeleteAllocatedBranch($node) {
    $cur = $node.Parent
    while ($null -ne $cur) {
        if ($cur -is [System.Management.Automation.Language.IfStatementAst] -and
            $cur.Extent.Text -cmatch 'if\s*\(\s*\$ForceDeleteAllocated\s*\)') {
            return $true
        }
        $cur = $cur.Parent
    }
    return $false
}
$e2eBatchDeletes = @($e2eAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.CommandAst] -and
        $node.GetCommandName() -ceq 'kubectl' -and
        $node.Extent.Text -cmatch 'delete\s+gameservers\s' -and
        $node.Extent.Text.Contains('-l ')
}, $true))
Assert-True ($e2eBatchDeletes.Count -ge 1) 'e2e 全量清场必须保留显式授权下的按 label 批删(压测纪律 §8)'
foreach ($cmd in $e2eBatchDeletes) {
    Assert-True (Test-InsideForceDeleteAllocatedBranch $cmd) `
        "e2e 按 label 的 GameServer 批删必须包在 if(`$ForceDeleteAllocated) 分支内,发现裸批删:$($cmd.Extent.Text)"
}

# (3) start.ps1 Apply-AgonesManifests:强制重建只准逐台删非 Allocated,且删前必须重查状态;
#     函数体内不得出现按 label 的整批 gameservers 删除。
Assert-True ($applyAgonesSource -cmatch "-cne\s+'Allocated'") `
    'start.ps1 强制重建必须按 state -cne Allocated 过滤可删集(§9.21)'
Assert-True ($applyAgonesSource -cmatch "-ceq\s+'Allocated'") `
    'start.ps1 强制重建必须显式识别并保留 Allocated 集'
Assert-True ($applyAgonesSource.Contains('{.status.state}')) `
    'start.ps1 强制重建删除前必须逐台重查 GameServer 当前状态(LIST→DELETE 竞态窗口压缩)'
$applyAgonesBatchDeletes = @($applyAgonesFunctions[0].FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.CommandAst] -and
        $node.GetCommandName() -ceq 'kubectl' -and
        $node.Extent.Text -cmatch 'delete\s+gameservers\s'
}, $true))
Assert-True ($applyAgonesBatchDeletes.Count -eq 0) `
    'start.ps1 Apply-AgonesManifests 不得出现按 label 的整批 gameservers 删除(会连 Allocated 一起删,INC-20260803-002)'
$applyAgonesSingleDeletes = @($applyAgonesFunctions[0].FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.CommandAst] -and
        $node.GetCommandName() -ceq 'kubectl' -and
        $node.Extent.Text -cmatch 'delete\s+gameserver\s'
}, $true))
Assert-True ($applyAgonesSingleDeletes.Count -ge 1) `
    'start.ps1 强制重建必须保留逐台按名删除路径(整段删除消失=强制重建失效,须显式知情)'

Write-Host 'local_k8s_profile_contract_test: PASS' -ForegroundColor Green
