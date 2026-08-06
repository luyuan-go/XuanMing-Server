# Inventory mesh 的只读 live 证明。依赖 inventory_mesh_contract.ps1；不执行 apply/patch/delete。
Set-StrictMode -Version Latest

function Get-PandoraInventoryMeshKubectlJson {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [Parameter(Mandatory = $true)][string]$Action
    )
    $lines = @(& kubectl --context $KubeContext @Arguments 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "$Action 失败:$($lines -join [Environment]::NewLine)" }
    $raw = ($lines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine
    if ([string]::IsNullOrWhiteSpace($raw)) { throw "$Action 返回空 JSON。" }
    try { return $raw | ConvertFrom-Json -Depth 100 }
    catch { throw "$Action 返回非法 JSON:$($_.Exception.Message)" }
}

function Test-PandoraInventoryPodReady {
    param([Parameter(Mandatory = $true)]$Pod)
    if ([string]$Pod.status.phase -cne 'Running' -or $null -ne (Get-PandoraInventoryObjectProperty $Pod.metadata 'deletionTimestamp')) {
        return $false
    }
    $ready = @($Pod.status.conditions | Where-Object { [string]$_.type -ceq 'Ready' -and [string]$_.status -ceq 'True' })
    return $ready.Count -eq 1
}

function Test-PandoraInventoryWebhookHandlesPodCreate {
    param([Parameter(Mandatory = $true)]$Webhook)
    foreach ($rule in @($Webhook.rules)) {
        $operations = @($rule.operations | ForEach-Object { [string]$_ })
        $groups = @($rule.apiGroups | ForEach-Object { [string]$_ })
        $versions = @($rule.apiVersions | ForEach-Object { [string]$_ })
        $resources = @($rule.resources | ForEach-Object { [string]$_ })
        $scope = [string](Get-PandoraInventoryObjectProperty $rule 'scope')
        if (($operations -ccontains 'CREATE' -or $operations -ccontains '*') -and
            ($groups -ccontains '' -or $groups -ccontains '*') -and
            ($versions -ccontains 'v1' -or $versions -ccontains '*') -and
            ($resources -ccontains 'pods' -or $resources -ccontains '*') -and
            ([string]::IsNullOrWhiteSpace($scope) -or $scope -ceq '*' -or $scope -ceq 'Namespaced')) { return $true }
    }
    return $false
}

function Test-PandoraInventoryIstioInjectorWebhookIdentity {
    param([Parameter(Mandatory = $true)]$Webhook)
    $service = Get-PandoraInventoryObjectProperty $Webhook.clientConfig 'service'
    return $null -ne $service -and
        [string]$Webhook.name -cmatch '(^|\.)sidecar-injector\.istio\.io$' -and
        [string]$service.name -cmatch '^istiod(?:-|$)' -and
        -not [string]::IsNullOrWhiteSpace([string]$service.namespace)
}

function Get-PandoraInventoryPodCreationLabels {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)]$Pod
    )
    $owners = @($Pod.metadata.ownerReferences | Where-Object {
            [string]$_.apiVersion -ceq 'apps/v1' -and [string]$_.kind -ceq 'ReplicaSet' -and $_.controller -eq $true
        })
    if ($owners.Count -ne 1 -or [string]::IsNullOrWhiteSpace([string]$owners[0].uid)) {
        throw "Pod/$($Pod.metadata.name) 缺唯一 ReplicaSet controller owner。"
    }
    $replicaSet = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
        -Arguments @('get', "replicaset/$($owners[0].name)", '-n', ([string]$Pod.metadata.namespace), '-o', 'json') `
        -Action "读取 Pod/$($Pod.metadata.name) 创建时 ReplicaSet template"
    if ([string]$replicaSet.metadata.uid -cne [string]$owners[0].uid) {
        throw "Pod/$($Pod.metadata.name) ReplicaSet owner UID 与 live 对象不一致。"
    }
    $deploymentOwners = @($replicaSet.metadata.ownerReferences | Where-Object {
            [string]$_.apiVersion -ceq 'apps/v1' -and [string]$_.kind -ceq 'Deployment' -and $_.controller -eq $true
        })
    if ($deploymentOwners.Count -ne 1 -or [string]::IsNullOrWhiteSpace([string]$deploymentOwners[0].uid)) {
        throw "ReplicaSet/$($replicaSet.metadata.name) 缺唯一 Deployment controller owner。"
    }
    $deployment = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
        -Arguments @('get', "deployment/$($deploymentOwners[0].name)", '-n', ([string]$Pod.metadata.namespace), '-o', 'json') `
        -Action "读取 ReplicaSet/$($replicaSet.metadata.name) Deployment owner"
    if ([string]$deployment.metadata.uid -cne [string]$deploymentOwners[0].uid) {
        throw "ReplicaSet/$($replicaSet.metadata.name) Deployment owner UID 与 live 对象不一致。"
    }
    $labels = Get-PandoraInventoryObjectProperty $replicaSet.spec.template.metadata 'labels'
    if ($null -eq $labels -or
        [string](Get-PandoraInventoryObjectProperty $labels 'pod-template-hash') -cne
        [string](Get-PandoraInventoryObjectProperty $Pod.metadata.labels 'pod-template-hash')) {
        throw "Pod/$($Pod.metadata.name) 创建时 ReplicaSet labels 无法与 live pod-template-hash 绑定。"
    }
    return $labels
}

function Get-PandoraInventoryMeshScalar {
    param([Parameter(Mandatory = $true)][string]$Text, [Parameter(Mandatory = $true)][string]$Name)
    $matches = @([regex]::Matches($Text, "(?m)^$([regex]::Escape($Name))\s*:\s*(?<value>[^#\r\n]*?)\s*(?:#.*)?$"))
    if ($matches.Count -gt 1) { throw "MeshConfig.$Name 重复。" }
    if ($matches.Count -eq 0) { return $null }
    $value = $matches[0].Groups['value'].Value.Trim()
    if ($value.Length -ge 2 -and (($value[0] -ceq '"' -and $value[$value.Length - 1] -ceq '"') -or
                                  ($value[0] -ceq "'" -and $value[$value.Length - 1] -ceq "'"))) {
        return $value.Substring(1, $value.Length - 2)
    }
    return $value
}

function Get-PandoraInventoryMeshConfigContract {
    param([Parameter(Mandatory = $true)][string]$Text)
    if ($Text -cmatch '(?m)^\s*<<\s*:' -or $Text -cmatch '(?m):\s*[&*][A-Za-z0-9_-]+') {
        throw 'MeshConfig 安全字段禁止 YAML anchor/alias/merge；只接受可机械审计的 canonical block mapping。'
    }
    foreach ($key in @('rootNamespace', 'trustDomain', 'trustDomainAliases', 'enableAutoMtls', 'enablePrometheusMerge')) {
        $semantic = @([regex]::Matches($Text,
                "(?im)(?:^\s*|[,{]\s*)[`"']?$([regex]::Escape($key))[`"']?\s*:"))
        $canonical = @([regex]::Matches($Text, "(?m)^$([regex]::Escape($key))\s*:"))
        if ($semantic.Count -ne $canonical.Count) {
            throw "MeshConfig.$key 使用缩进、flow/JSON、quoted key 或其它非 canonical 表示；拒绝按默认值误判。"
        }
    }
    $root = Get-PandoraInventoryMeshScalar -Text $Text -Name 'rootNamespace'
    if ([string]::IsNullOrWhiteSpace([string]$root)) { $root = 'istio-system' }
    if ($root -cnotmatch '^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$') { throw "MeshConfig rootNamespace 非法:$root" }
    $trustDomain = Get-PandoraInventoryMeshScalar -Text $Text -Name 'trustDomain'
    if ([string]::IsNullOrWhiteSpace([string]$trustDomain)) { $trustDomain = 'cluster.local' }
    if ($trustDomain -cnotmatch '^[A-Za-z0-9](?:[-.A-Za-z0-9]{0,251}[A-Za-z0-9])?$') { throw "MeshConfig trustDomain 非法:$trustDomain" }
    $aliases = Get-PandoraInventoryMeshScalar -Text $Text -Name 'trustDomainAliases'
    if ($null -ne $aliases -and [string]$aliases -cne '[]') {
        throw 'MeshConfig trustDomainAliases 必须为空；非空会扩大等价 workload identity，需另行设计批准。'
    }
    $autoMtls = Get-PandoraInventoryMeshScalar -Text $Text -Name 'enableAutoMtls'
    if ([string]$autoMtls -cmatch '^(?i:false)$') { throw 'MeshConfig enableAutoMtls=false 会使调用方不自动发起 mTLS。' }
    if ($null -ne $autoMtls -and [string]$autoMtls -cnotmatch '^(?i:true|false)$') { throw "MeshConfig enableAutoMtls 非布尔值:$autoMtls" }
    $prometheusMerge = Get-PandoraInventoryMeshScalar -Text $Text -Name 'enablePrometheusMerge'
    if ([string]$prometheusMerge -cmatch '^(?i:false)$') { throw 'MeshConfig enablePrometheusMerge=false 会破坏 Inventory metrics merge。' }
    if ($null -ne $prometheusMerge -and [string]$prometheusMerge -cnotmatch '^(?i:true|false)$') {
        throw "MeshConfig enablePrometheusMerge 非布尔值:$prometheusMerge"
    }
    return [pscustomobject]@{
        RootNamespace = [string]$root
        TrustDomain = [string]$trustDomain
        EnableAutoMtls = if ($null -eq $autoMtls) { $true } else { [bool]::Parse([string]$autoMtls) }
        EnablePrometheusMerge = if ($null -eq $prometheusMerge) { $true } else { [bool]::Parse([string]$prometheusMerge) }
    }
}

function Get-PandoraInventoryIstiodMeshConfig {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)]$Pod
    )
    $candidates = @($Pod.spec.containers | Where-Object {
            [string]$_.name -ceq 'discovery' -or
            (@($_.args | ForEach-Object { [string]$_ }) -join ' ') -match 'pilot-discovery'
        })
    if ($candidates.Count -ne 1) { throw "istiod Pod/$($Pod.metadata.name) discovery container 数=$($candidates.Count)。" }
    $container = $candidates[0]
    $args = @($container.args | ForEach-Object { [string]$_ })
    $meshPath = '/etc/istio/config/mesh'
    for ($i = 0; $i -lt $args.Count; $i++) {
        if ($args[$i] -ceq '--meshConfig') {
            if ($i + 1 -ge $args.Count) { throw 'istiod --meshConfig 缺值。' }
            $meshPath = $args[$i + 1]
        } elseif ($args[$i] -cmatch '^--meshConfig=(.+)$') { $meshPath = $Matches[1] }
    }
    if ($meshPath -cnotmatch '^/') { throw "istiod meshConfig 不是绝对路径:$meshPath" }
    $mounts = @($container.volumeMounts | Where-Object {
            $mount = ([string]$_.mountPath).TrimEnd('/')
            $meshPath -ceq $mount -or $meshPath.StartsWith($mount + '/', [StringComparison]::Ordinal)
        } | Sort-Object { ([string]$_.mountPath).Length } -Descending)
    if ($mounts.Count -eq 0) { throw "istiod meshConfig 路径没有 volumeMount:$meshPath" }
    $mount = $mounts[0]
    $volumes = @($Pod.spec.volumes | Where-Object name -CEQ ([string]$mount.name))
    if ($volumes.Count -ne 1) { throw "istiod meshConfig volume=$($mount.name) 不唯一。" }
    $configMapVolume = Get-PandoraInventoryObjectProperty $volumes[0] 'configMap'
    if ($null -eq $configMapVolume -or [string]::IsNullOrWhiteSpace([string]$configMapVolume.name)) {
        throw 'istiod meshConfig 必须来自可审计 ConfigMap。'
    }
    $relative = if (-not [string]::IsNullOrWhiteSpace([string](Get-PandoraInventoryObjectProperty $mount 'subPath'))) {
        [string](Get-PandoraInventoryObjectProperty $mount 'subPath')
    } else {
        $meshPath.Substring(([string]$mount.mountPath).TrimEnd('/').Length).TrimStart('/')
    }
    $key = $relative
    $items = @((Get-PandoraInventoryObjectProperty $configMapVolume 'items') | Where-Object { $null -ne $_ })
    if ($items.Count -gt 0) {
        $matches = @($items | Where-Object { [string]$_.path -ceq $relative })
        if ($matches.Count -ne 1) { throw "istiod meshConfig ConfigMap items 无法唯一映射 path=$relative。" }
        $key = [string]$matches[0].key
    }
    if ([string]::IsNullOrWhiteSpace($key)) { throw 'istiod meshConfig ConfigMap key 为空。' }
    $cm = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
        -Arguments @('get', "configmap/$($configMapVolume.name)", '-n', ([string]$Pod.metadata.namespace), '-o', 'json') `
        -Action "读取 istiod MeshConfig/$($configMapVolume.name)"
    $text = [string](Get-PandoraInventoryObjectProperty $cm.data $key)
    if ([string]::IsNullOrWhiteSpace($text)) { throw "istiod MeshConfig ConfigMap/$($configMapVolume.name) 缺 key=$key。" }
    return Get-PandoraInventoryMeshConfigContract -Text $text
}

function Resolve-PandoraInventoryIstioControlPlane {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][string]$RequestedRevision,
        [Parameter(Mandatory = $true)][object[]]$PodLabels,
        [Parameter(Mandatory = $true)][string]$NamespaceName
    )
    Assert-PandoraIstioRevision -Revision $RequestedRevision
    $namespace = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
        -Arguments @('get', "namespace/$NamespaceName", '-o', 'json') -Action "读取 Namespace/$NamespaceName"
    $mwcs = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
        -Arguments @('get', 'mutatingwebhookconfigurations', '-o', 'json') -Action '读取 MutatingWebhookConfiguration'
    $matchedIdentity = ''
    $matchedWebhook = $null
    $matchedConfig = $null
    foreach ($labels in $PodLabels) {
        $matches = [System.Collections.Generic.List[object]]::new()
        foreach ($config in @($mwcs.items)) {
            foreach ($webhook in @($config.webhooks)) {
                if (-not (Test-PandoraInventoryWebhookHandlesPodCreate $webhook)) { continue }
                if (-not (Test-PandoraInventoryIstioInjectorWebhookIdentity $webhook)) { continue }
                if (-not (Test-PandoraInventoryLabelSelectorMatch -Selector (Get-PandoraInventoryObjectProperty $webhook 'namespaceSelector') -Labels $namespace.metadata.labels)) { continue }
                if (-not (Test-PandoraInventoryLabelSelectorMatch -Selector (Get-PandoraInventoryObjectProperty $webhook 'objectSelector') -Labels $labels)) { continue }
                if (@((Get-PandoraInventoryObjectProperty $webhook 'matchConditions') | Where-Object { $null -ne $_ }).Count -ne 0) {
                    throw "Istio injector webhook/$($webhook.name) 含无法机械求值的 matchConditions。"
                }
                $matches.Add([pscustomobject]@{ Config = $config; Webhook = $webhook })
            }
        }
        if ($matches.Count -ne 1) { throw "revision=$RequestedRevision 的 Pod 实际命中 injector webhook 数=$($matches.Count)，必须恰好 1。" }
        $entry = $matches[0]
        $identity = "$($entry.Config.metadata.uid)/$($entry.Webhook.name)"
        if ([string]::IsNullOrWhiteSpace($matchedIdentity)) {
            $matchedIdentity = $identity; $matchedWebhook = $entry.Webhook; $matchedConfig = $entry.Config
        } elseif ($identity -cne $matchedIdentity) { throw '六个 workload 命中不同 injector webhook。' }
    }
    if ([string]$matchedWebhook.failurePolicy -cne 'Fail' -or
        [string]$matchedWebhook.sideEffects -notin @('None', 'NoneOnDryRun') -or
        @($matchedWebhook.admissionReviewVersions | ForEach-Object { [string]$_ }) -cnotcontains 'v1') {
        throw '选中 injector webhook 必须 failurePolicy=Fail、sideEffects 安全且支持 admissionReview v1。'
    }
    $serviceRef = Get-PandoraInventoryObjectProperty $matchedWebhook.clientConfig 'service'
    if ($null -eq $serviceRef -or [string]::IsNullOrWhiteSpace([string]$serviceRef.name) -or
        [string]::IsNullOrWhiteSpace([string]$serviceRef.namespace)) { throw '选中 injector webhook 必须通过 Service 指向 control plane。' }
    $injectorService = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
        -Arguments @('get', "service/$($serviceRef.name)", '-n', ([string]$serviceRef.namespace), '-o', 'json') `
        -Action '读取 injector Service'
    $slices = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
        -Arguments @('get', 'endpointslices.discovery.k8s.io', '-n', ([string]$serviceRef.namespace),
            '-l', "kubernetes.io/service-name=$($serviceRef.name)", '-o', 'json') `
        -Action '读取 injector Service EndpointSlice'
    $podNames = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($slice in @($slices.items)) {
        foreach ($endpoint in @($slice.endpoints)) {
            if ((Get-PandoraInventoryObjectProperty $endpoint.conditions 'ready') -ne $true) { continue }
            $target = Get-PandoraInventoryObjectProperty $endpoint 'targetRef'
            if ($null -ne $target -and [string]$target.kind -ceq 'Pod' -and [string]$target.namespace -ceq [string]$serviceRef.namespace) {
                $null = $podNames.Add([string]$target.name)
            }
        }
    }
    if ($podNames.Count -eq 0) { throw 'injector Service 没有 Ready Pod Endpoint。' }
    $actualRevision = ''
    $meshContract = $null
    $controlPlanePodUIDs = [System.Collections.Generic.List[string]]::new()
    foreach ($podName in $podNames) {
        $pod = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
            -Arguments @('get', "pod/$podName", '-n', ([string]$serviceRef.namespace), '-o', 'json') -Action "读取 istiod Pod/$podName"
        if (-not (Test-PandoraInventoryPodReady $pod)) { throw "istiod Pod/$podName 非 Ready。" }
        if ([string]::IsNullOrWhiteSpace([string]$pod.metadata.uid)) { throw "istiod Pod/$podName 缺 UID。" }
        $controlPlanePodUIDs.Add([string]$pod.metadata.uid)
        $revision = [string](Get-PandoraInventoryObjectProperty $pod.metadata.labels 'istio.io/rev')
        if ([string]::IsNullOrWhiteSpace($revision)) { throw "istiod Pod/$podName 缺 actual revision label。" }
        if ([string]::IsNullOrWhiteSpace($actualRevision)) { $actualRevision = $revision }
        elseif ($revision -cne $actualRevision) { throw '同一 injector Service 后端混入多个 actual revision。' }
        $current = Get-PandoraInventoryIstiodMeshConfig -KubeContext $KubeContext -Pod $pod
        if ($null -eq $meshContract) { $meshContract = $current }
        elseif ($current.RootNamespace -cne $meshContract.RootNamespace -or $current.TrustDomain -cne $meshContract.TrustDomain -or
                $current.EnableAutoMtls -ne $meshContract.EnableAutoMtls -or
                $current.EnablePrometheusMerge -ne $meshContract.EnablePrometheusMerge) {
            throw '同一 injector Service 后端 MeshConfig 不一致。'
        }
    }
    return [pscustomobject]@{
        RequestedRevision = $RequestedRevision
        ActualRevision = $actualRevision
        RootNamespace = $meshContract.RootNamespace
        TrustDomain = $meshContract.TrustDomain
        WebhookUID = [string]$matchedConfig.metadata.uid
        WebhookResourceVersion = [string]$matchedConfig.metadata.resourceVersion
        WebhookName = [string]$matchedWebhook.name
        NamespaceUID = [string]$namespace.metadata.uid
        InjectorService = "$($serviceRef.namespace)/$($serviceRef.name)"
        InjectorServiceUID = [string]$injectorService.metadata.uid
        InjectorEndpointSliceUIDs = @($slices.items | ForEach-Object { [string]$_.metadata.uid } | Sort-Object -CaseSensitive)
        ControlPlanePodUIDs = @($controlPlanePodUIDs | Sort-Object -CaseSensitive)
    }
}

function Assert-PandoraInventoryControlPlaneIdentityUnchanged {
    param([Parameter(Mandatory = $true)]$Earlier, [Parameter(Mandatory = $true)]$Later)
    foreach ($name in @('RequestedRevision', 'ActualRevision', 'RootNamespace', 'TrustDomain', 'WebhookUID',
            'WebhookResourceVersion', 'WebhookName', 'NamespaceUID', 'InjectorService', 'InjectorServiceUID')) {
        if ([string](Get-PandoraInventoryObjectProperty $Earlier $name) -cne
            [string](Get-PandoraInventoryObjectProperty $Later $name)) {
            throw "Inventory mesh 早期预检到锁内重验期间 control-plane identity/$name 已变化。"
        }
    }
    foreach ($name in @('InjectorEndpointSliceUIDs', 'ControlPlanePodUIDs')) {
        $before = @((Get-PandoraInventoryObjectProperty $Earlier $name) | ForEach-Object { [string]$_ } | Sort-Object -CaseSensitive)
        $after = @((Get-PandoraInventoryObjectProperty $Later $name) | ForEach-Object { [string]$_ } | Sort-Object -CaseSensitive)
        if (($before -join ',') -cne ($after -join ',')) {
            throw "Inventory mesh 早期预检到锁内重验期间 control-plane endpoint/$name 已变化。"
        }
    }
}

function Assert-PandoraInventoryMeshCRDs {
    param([Parameter(Mandatory = $true)][string]$KubeContext)
    foreach ($name in @('peerauthentications.security.istio.io', 'authorizationpolicies.security.istio.io')) {
        $crd = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext -Arguments @('get', "crd/$name", '-o', 'json') -Action "读取 CRD/$name"
        $served = @($crd.spec.versions | Where-Object { [string]$_.name -ceq 'v1' -and $_.served -eq $true })
        $established = @($crd.status.conditions | Where-Object { [string]$_.type -ceq 'Established' -and [string]$_.status -ceq 'True' })
        if ($served.Count -ne 1 -or $established.Count -ne 1) { throw "CRD/$name 未 Established 或不 served security.istio.io/v1。" }
    }
    # ValidatingAdmissionPolicy/Binding 是 Kubernetes built-in API，不是 CRD；其可用性由下方
    # 精确 get live VAP/Binding 证明。禁止用 get crd 检查，否则任何合法集群都会恒失败。
}

function Assert-PandoraInventoryAdmissionApiPrerequisite {
    param([Parameter(Mandatory = $true)][string]$KubeContext)
    $version = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
        -Arguments @('get', '--raw=/version') -Action '读取 Kubernetes version'
    $minorText = ([string]$version.minor) -replace '[^0-9].*$', ''
    if ([string]$version.major -cne '1' -or [string]::IsNullOrWhiteSpace($minorText) -or [int]$minorText -lt 30) {
        throw "Inventory mesh ValidatingAdmissionPolicy v1 要求 Kubernetes >=1.30，当前=$($version.major).$($version.minor)。"
    }
    $discovery = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
        -Arguments @('get', '--raw=/apis/admissionregistration.k8s.io/v1') `
        -Action '发现 admissionregistration.k8s.io/v1'
    $resources = @($discovery.resources | ForEach-Object { [string]$_.name })
    foreach ($required in @('validatingadmissionpolicies', 'validatingadmissionpolicybindings')) {
        if ($resources -cnotcontains $required) {
            throw "admissionregistration.k8s.io/v1 未提供 $required；Inventory mesh 阶段必须零写退出。"
        }
    }
}

function Get-PandoraInventoryLiveWorkloadState {
    param([Parameter(Mandatory = $true)][string]$KubeContext, [Parameter(Mandatory = $true)][string]$Revision)
    $labels = [System.Collections.Generic.List[object]]::new()
    $statusRevisions = [System.Collections.Generic.List[string]]::new()
    $proxyImages = [System.Collections.Generic.List[string]]::new()
    $proxyImageIDs = [System.Collections.Generic.List[string]]::new()
    $creationLabels = [System.Collections.Generic.List[object]]::new()
    $allPods = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
        -Arguments @('get', 'pods', '-n', 'pandora', '-o', 'json') -Action '读取 pandora 全量 Pods 审计受保护 SA'
    $saToApp = @{}
    foreach ($identity in (Get-PandoraInventoryMeshWorkloads).GetEnumerator()) { $saToApp[[string]$identity.Value] = [string]$identity.Key }
    foreach ($pod in @($allPods.items)) {
        $saName = [string]$pod.spec.serviceAccountName
        if (-not $saToApp.ContainsKey($saName)) { continue }
        if ($null -ne (Get-PandoraInventoryObjectProperty $pod.metadata 'deletionTimestamp')) {
            throw "Pod/$($pod.metadata.name) 正在 terminating 但仍持有受保护 SA=$saName；须等 sidecar/容器彻底消失。"
        }
        $expectedApp = [string]$saToApp[$saName]
        if ([string](Get-PandoraInventoryObjectProperty $pod.metadata.labels 'app') -cne $expectedApp) {
            throw "Pod/$($pod.metadata.name) 冒用受保护 SA=$saName，app label 未绑定 $expectedApp。"
        }
    }
    foreach ($entry in (Get-PandoraInventoryMeshWorkloads).GetEnumerator()) {
        $name = [string]$entry.Key
        if ($name -ceq 'battle-result') {
            $deploymentList = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
                -Arguments @('get', 'deployments', '-n', 'pandora', '-l', 'app=battle-result', '-o', 'json') `
                -Action '读取 battle-result blue/green Deployments'
            $blue = @($deploymentList.items | Where-Object { [string]$_.metadata.name -ceq 'battle-result' })
            $green = @($deploymentList.items | Where-Object { [string]$_.metadata.name -ceq 'battle-result-ds-auth-green' })
            if ($blue.Count -ne 1 -or $green.Count -gt 1 -or @($deploymentList.items).Count -ne (1 + $green.Count)) {
                throw 'battle-result app selector 只能对应唯一 blue 与可选唯一 canonical green Deployment。'
            }
            Assert-PandoraInventoryMeshWorkload -Workload $blue[0] -Revision $Revision -LogicalName battle-result
            if ($green.Count -eq 1) {
                if ([int]$blue[0].spec.replicas -ne 0 -or [int](Get-PandoraInventoryObjectProperty $blue[0].status 'readyReplicas') -ne 0) {
                    throw 'canonical green 存在时 battle-result blue 必须 replicas=0/Ready=0。'
                }
                Assert-PandoraInventoryMeshWorkload -Workload $green[0] -Revision $Revision -LogicalName battle-result
                $deployment = $green[0]
            } else {
                $deployment = $blue[0]
            }
        } else {
            $deployment = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
                -Arguments @('get', "deployment/$name", '-n', 'pandora', '-o', 'json') -Action "读取 Deployment/$name"
            Assert-PandoraInventoryMeshWorkload -Workload $deployment -Revision $Revision -LogicalName $name
        }
        $want = [int]$deployment.spec.replicas
        if ($want -lt 1 -or [int]$deployment.status.readyReplicas -ne $want -or [int]$deployment.status.updatedReplicas -ne $want -or
            [int](Get-PandoraInventoryObjectProperty $deployment.status 'unavailableReplicas') -ne 0) {
            throw "Deployment/$name 未完整 Ready/updated。"
        }
        $pods = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
            -Arguments @('get', 'pods', '-n', 'pandora', '-l', "app=$name", '-o', 'json') -Action "读取 $name Pods"
        $live = @($pods.items | Where-Object { $null -eq (Get-PandoraInventoryObjectProperty $_.metadata 'deletionTimestamp') })
        if ($live.Count -ne $want) { throw "$name live Pod 数=$($live.Count)，expected=$want。" }
        foreach ($pod in $live) {
            Assert-PandoraInventoryMeshWorkload -Workload $pod -Revision $Revision -LivePod -LogicalName $name
            $creationLabels.Add((Get-PandoraInventoryPodCreationLabels -KubeContext $KubeContext -Pod $pod))
            if (-not (Test-PandoraInventoryPodReady $pod)) { throw "Pod/$($pod.metadata.name) 非 Ready。" }
            foreach ($container in @($name, 'istio-proxy')) {
                $statuses = @($pod.status.containerStatuses | Where-Object name -CEQ $container)
                if ($statuses.Count -ne 1 -or $statuses[0].ready -ne $true) { throw "Pod/$($pod.metadata.name) $container 未 Ready。" }
            }
            $labels.Add($pod.metadata.labels)
            $sidecarStatus = ([string](Get-PandoraInventoryObjectProperty $pod.metadata.annotations 'sidecar.istio.io/status')) | ConvertFrom-Json -Depth 20
            $statusRevisions.Add([string]$sidecarStatus.revision)
            $proxySpec = @($pod.spec.containers | Where-Object name -CEQ 'istio-proxy')
            $proxyStatus = @($pod.status.containerStatuses | Where-Object name -CEQ 'istio-proxy')
            if ($proxySpec.Count -ne 1 -or $proxyStatus.Count -ne 1 -or [string]::IsNullOrWhiteSpace([string]$proxyStatus[0].imageID)) {
                throw "Pod/$($pod.metadata.name) proxy image/imageID 不完整。"
            }
            $proxyImages.Add([string]$proxySpec[0].image); $proxyImageIDs.Add([string]$proxyStatus[0].imageID)
        }
    }
    return [pscustomobject]@{ Labels = @($labels); CreationLabels = @($creationLabels); StatusRevisions = @($statusRevisions); ProxyImages = @($proxyImages); ProxyImageIDs = @($proxyImageIDs) }
}

function Assert-PandoraInventoryEdgeCaptureMetadata {
    param([Parameter(Mandatory = $true)]$Metadata, [Parameter(Mandatory = $true)][string]$ObjectName)
    $annotations = Get-PandoraInventoryObjectProperty $Metadata 'annotations'
    if ([string](Get-PandoraInventoryObjectProperty $Metadata.labels 'istio.io/dataplane-mode') -ceq 'none' -or
        -not [string]::IsNullOrWhiteSpace([string](Get-PandoraInventoryObjectProperty $Metadata.labels 'sidecar.istio.io/inject')) -or
        -not [string]::IsNullOrWhiteSpace([string](Get-PandoraInventoryObjectProperty $annotations 'sidecar.istio.io/inject'))) {
        throw "$ObjectName 禁止 dataplane none 与额外 inject label/annotation。"
    }
    foreach ($property in @($(if ($null -ne $annotations) { $annotations.PSObject.Properties }))) {
        $name = [string]$property.Name
        if ($name -like 'traffic.sidecar.istio.io/excludeOutbound*' -or
            $name -like 'traffic.sidecar.istio.io/includeOutbound*' -or
            $name -ceq 'proxy.istio.io/config' -or $name -ceq 'sidecar.istio.io/interceptionMode') {
            throw "$ObjectName 禁止自定义到 Inventory 的 outbound/interception capture:$name。"
        }
        if ($name -like 'traffic.sidecar.istio.io/*' -and
            ($name -cne 'traffic.sidecar.istio.io/excludeInboundPorts' -or
             ((@(([string]$property.Value).Split(',') | ForEach-Object { $_.Trim() } | Sort-Object -Unique) -join ',') -cne '8443,8444,9901'))) {
            throw "$ObjectName 只允许精确排除自有 inbound 8443/8444/9901:$name。"
        }
    }
}

function Assert-PandoraInventoryEdgeApplicationSpec {
    param(
        [Parameter(Mandatory = $true)]$Spec,
        [Parameter(Mandatory = $true)][string]$ObjectName,
        [switch]$LivePod
    )
    $app = @($Spec.containers | Where-Object name -CEQ 'pandora-edge-envoy')
    $proxy = @($Spec.containers | Where-Object name -CEQ 'istio-proxy')
    if ((Get-PandoraInventoryObjectProperty $Spec 'automountServiceAccountToken') -ne $false -or
        $app.Count -ne 1 -or @($Spec.containers).Count -ne $(if ($LivePod) { 2 } else { 1 }) -or
        ($LivePod -and $proxy.Count -ne 1)) {
        throw "$ObjectName 必须 automountServiceAccountToken=false；模板仅单 edge 容器，live 仅 edge+proxy。"
    }
    $security = Get-PandoraInventoryObjectProperty $app[0] 'securityContext'
    $caps = Get-PandoraInventoryObjectProperty $security 'capabilities'
    $adds = @((Get-PandoraInventoryObjectProperty $caps 'add') | Where-Object { $null -ne $_ })
    $drops = @((Get-PandoraInventoryObjectProperty $caps 'drop') | Where-Object { $null -ne $_ } | ForEach-Object { [string]$_ })
    if ((Get-PandoraInventoryObjectProperty $security 'allowPrivilegeEscalation') -ne $false -or
        (Get-PandoraInventoryObjectProperty $security 'privileged') -ne $false -or
        $adds.Count -ne 0 -or $drops.Count -ne 1 -or $drops[0] -cne 'ALL' -or
        @((Get-PandoraInventoryObjectProperty $app[0] 'volumeMounts') | Where-Object name -CEQ 'istio-token').Count -ne 0) {
        throw "$ObjectName edge 应用容器必须无提权、drop ALL、且不得挂载 istio-token。"
    }
    $ephemeral = @((Get-PandoraInventoryObjectProperty $Spec 'ephemeralContainers') | Where-Object { $null -ne $_ })
    $init = @((Get-PandoraInventoryObjectProperty $Spec 'initContainers') | Where-Object { $null -ne $_ })
    if ($ephemeral.Count -ne 0 -or
        ($LivePod -and ($init.Count -gt 1 -or ($init.Count -eq 1 -and [string]$init[0].name -notin @('istio-init', 'istio-validation')))) -or
        (-not $LivePod -and ($init.Count -ne 0 -or @($Spec.volumes | Where-Object name -CEQ 'istio-token').Count -ne 0))) {
        throw "$ObjectName 禁止旁加载体；模板无 init/token，live 只允许唯一 Istio init。"
    }
}

function Get-PandoraInventoryEdgeState {
    param([Parameter(Mandatory = $true)][string]$KubeContext, [Parameter(Mandatory = $true)][string]$Revision)
    $namespace = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext -Arguments @('get', 'namespace/pandora-ingress', '-o', 'json') -Action '读取 pandora-ingress namespace'
    if ([string](Get-PandoraInventoryObjectProperty $namespace.metadata.labels 'pandora.dev/role') -cne 'ingress-gateway') {
        throw 'pandora-ingress namespace 缺 pandora.dev/role=ingress-gateway。'
    }
    $sa = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext -Arguments @('get', 'serviceaccount/pandora-edge-envoy', '-n', 'pandora-ingress', '-o', 'json') -Action '读取 edge SA'
    if ([string]$sa.metadata.name -cne 'pandora-edge-envoy' -or $sa.automountServiceAccountToken -ne $false) {
        throw '生产 edge SA 必须 automountServiceAccountToken=false。'
    }
    $deployment = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext -Arguments @('get', 'deployment/pandora-edge-envoy', '-n', 'pandora-ingress', '-o', 'json') -Action '读取 edge Deployment'
    $template = $deployment.spec.template
    $templateAnnotations = Get-PandoraInventoryObjectProperty $template.metadata 'annotations'
    if ([string]$deployment.spec.selector.matchLabels.'app.kubernetes.io/name' -cne 'pandora-edge-envoy' -or
        @($deployment.spec.selector.matchLabels.PSObject.Properties).Count -ne 1 -or
        [string](Get-PandoraInventoryObjectProperty $template.metadata.labels 'app.kubernetes.io/name') -cne 'pandora-edge-envoy' -or
        [string]$template.spec.serviceAccountName -cne 'pandora-edge-envoy' -or
        [string](Get-PandoraInventoryObjectProperty $template.metadata.labels 'istio.io/rev') -cne $Revision -or
        -not [string]::IsNullOrWhiteSpace([string](Get-PandoraInventoryObjectProperty $template.metadata.labels 'sidecar.istio.io/inject')) -or
        -not [string]::IsNullOrWhiteSpace([string](Get-PandoraInventoryObjectProperty $templateAnnotations 'sidecar.istio.io/inject'))) {
        throw '生产 edge 必须独立 SA、精确 revision，且禁止额外 inject label/annotation。'
    }
    if ((Get-PandoraInventoryObjectProperty $template.spec 'hostNetwork') -eq $true -or
        [string](Get-PandoraInventoryObjectProperty $template.metadata.labels 'istio.io/dataplane-mode') -ceq 'none') {
        throw '生产 edge 禁止 hostNetwork/dataplane none。'
    }
    Assert-PandoraInventoryEdgeCaptureMetadata -Metadata $template.metadata -ObjectName '生产 edge Deployment template'
    Assert-PandoraInventoryEdgeApplicationSpec -Spec $template.spec -ObjectName '生产 edge Deployment template'
    $want = [int]$deployment.spec.replicas
    if ($want -lt 1 -or [int]$deployment.status.readyReplicas -ne $want -or [int]$deployment.status.updatedReplicas -ne $want) {
        throw '生产 edge Deployment 未完整 Ready/updated。'
    }
    $pods = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext -Arguments @('get', 'pods', '-n', 'pandora-ingress', '-o', 'json') -Action '读取 pandora-ingress 全量 Pods 审计 edge SA'
    $edgeIdentityPods = @($pods.items | Where-Object {
            [string]$_.spec.serviceAccountName -ceq 'pandora-edge-envoy' -or
            [string](Get-PandoraInventoryObjectProperty $_.metadata.labels 'app.kubernetes.io/name') -ceq 'pandora-edge-envoy'
        })
    foreach ($pod in $edgeIdentityPods) {
        if ($null -ne (Get-PandoraInventoryObjectProperty $pod.metadata 'deletionTimestamp')) {
            throw "edge Pod/$($pod.metadata.name) terminating 期间仍可能持有高权限 mTLS 身份；须等对象彻底消失。"
        }
    }
    $live = @($edgeIdentityPods)
    if ($live.Count -ne $want) { throw "edge live Pod 数=$($live.Count)，expected=$want。" }
    $labels = [System.Collections.Generic.List[object]]::new()
    $revisions = [System.Collections.Generic.List[string]]::new()
    $proxyImages = [System.Collections.Generic.List[string]]::new()
    $proxyImageIDs = [System.Collections.Generic.List[string]]::new()
    $creationLabels = [System.Collections.Generic.List[object]]::new()
    $podNames = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($pod in $live) {
        if ([string]$pod.spec.serviceAccountName -cne 'pandora-edge-envoy' -or
            [string](Get-PandoraInventoryObjectProperty $pod.metadata.labels 'app.kubernetes.io/name') -cne 'pandora-edge-envoy') {
            throw "edge Pod/$($pod.metadata.name) 的高权限 SA 与 NetworkPolicy label 未双向绑定。"
        }
        Assert-PandoraInventoryEdgeCaptureMetadata -Metadata $pod.metadata -ObjectName "生产 edge Pod/$($pod.metadata.name)"
        Assert-PandoraInventoryEdgeApplicationSpec -Spec $pod.spec -ObjectName "生产 edge Pod/$($pod.metadata.name)" -LivePod
        if (-not (Test-PandoraInventoryPodReady $pod) -or [string]$pod.spec.serviceAccountName -cne 'pandora-edge-envoy' -or
            [string](Get-PandoraInventoryObjectProperty $pod.metadata.labels 'istio.io/rev') -cne $Revision -or
            (Get-PandoraInventoryObjectProperty $pod.spec 'hostNetwork') -eq $true -or
            @($pod.spec.containers | Where-Object name -CEQ 'istio-proxy').Count -ne 1) { throw "edge Pod/$($pod.metadata.name) 身份/sidecar/Ready 不闭合。" }
        $podOwner = @($pod.metadata.ownerReferences | Where-Object {
                [string]$_.kind -ceq 'ReplicaSet' -and [string]$_.apiVersion -ceq 'apps/v1' -and $_.controller -eq $true
            })
        if ($podOwner.Count -ne 1) { throw "edge Pod/$($pod.metadata.name) 缺唯一 ReplicaSet owner。" }
        $edgeReplicaSet = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
            -Arguments @('get', "replicaset/$($podOwner[0].name)", '-n', 'pandora-ingress', '-o', 'json') `
            -Action "读取 edge Pod/$($pod.metadata.name) ReplicaSet"
        $edgeDeploymentOwner = @($edgeReplicaSet.metadata.ownerReferences | Where-Object {
                [string]$_.kind -ceq 'Deployment' -and [string]$_.apiVersion -ceq 'apps/v1' -and $_.controller -eq $true
            })
        if ([string]$edgeReplicaSet.metadata.uid -cne [string]$podOwner[0].uid -or
            $edgeDeploymentOwner.Count -ne 1 -or [string]$edgeDeploymentOwner[0].name -cne 'pandora-edge-envoy' -or
            [string]$edgeDeploymentOwner[0].uid -cne [string]$deployment.metadata.uid -or
            [string]$edgeReplicaSet.spec.selector.matchLabels.'app.kubernetes.io/name' -cne 'pandora-edge-envoy' -or
            [string]$edgeReplicaSet.spec.selector.matchLabels.'pod-template-hash' -cne
              [string](Get-PandoraInventoryObjectProperty $pod.metadata.labels 'pod-template-hash') -or
            @($edgeReplicaSet.spec.selector.matchLabels.PSObject.Properties).Count -ne 2) {
            throw "edge Pod/$($pod.metadata.name) Deployment->ReplicaSet->Pod owner/selector 链不精确。"
        }
        Assert-PandoraInventoryEdgeCaptureMetadata -Metadata $edgeReplicaSet.spec.template.metadata `
            -ObjectName "生产 edge ReplicaSet/$($edgeReplicaSet.metadata.name) template"
        Assert-PandoraInventoryEdgeApplicationSpec -Spec $edgeReplicaSet.spec.template.spec `
            -ObjectName "生产 edge ReplicaSet/$($edgeReplicaSet.metadata.name) template"
        $deploymentApp = @($template.spec.containers | Where-Object name -CEQ 'pandora-edge-envoy')
        $replicaSetApp = @($edgeReplicaSet.spec.template.spec.containers | Where-Object name -CEQ 'pandora-edge-envoy')
        $podApp = @($pod.spec.containers | Where-Object name -CEQ 'pandora-edge-envoy')
        if ([string]$deploymentApp[0].image -cne [string]$replicaSetApp[0].image -or
            [string]$replicaSetApp[0].image -cne [string]$podApp[0].image) {
            throw "edge Pod/$($pod.metadata.name) 应用 image 未绑定 Ready updated Deployment/ReplicaSet。"
        }
        $statusText = [string](Get-PandoraInventoryObjectProperty $pod.metadata.annotations 'sidecar.istio.io/status')
        try { $status = $statusText | ConvertFrom-Json -Depth 20 } catch { throw "edge Pod/$($pod.metadata.name) sidecar status 非法。" }
        if ([string]::IsNullOrWhiteSpace([string]$status.revision) -or
            @($status.containers | ForEach-Object { [string]$_ }) -cnotcontains 'istio-proxy') { throw "edge Pod/$($pod.metadata.name) 缺 actual revision/proxy status。" }
        $proxySpec = @($pod.spec.containers | Where-Object name -CEQ 'istio-proxy')
        $proxyStatus = @($pod.status.containerStatuses | Where-Object name -CEQ 'istio-proxy')
        if ($proxySpec.Count -ne 1 -or $proxyStatus.Count -ne 1 -or [string]::IsNullOrWhiteSpace([string]$proxyStatus[0].imageID)) {
            throw "edge Pod/$($pod.metadata.name) proxy image/imageID 不完整。"
        }
        foreach ($containerStatus in @($pod.status.containerStatuses)) {
            if ($containerStatus.ready -ne $true) { throw "edge Pod/$($pod.metadata.name) container/$($containerStatus.name) 未 Ready。" }
        }
        $tokenVolumes = @($pod.spec.volumes | Where-Object name -CEQ 'istio-token')
        $tokenMounts = @($proxySpec[0].volumeMounts | Where-Object name -CEQ 'istio-token')
        if ($tokenVolumes.Count -ne 1 -or $tokenMounts.Count -ne 1 -or $tokenMounts[0].readOnly -ne $true -or
            @($tokenVolumes[0].projected.sources | Where-Object { $null -ne (Get-PandoraInventoryObjectProperty $_ 'serviceAccountToken') }).Count -ne 1) {
            throw "edge Pod/$($pod.metadata.name) istio-token projected SA token/mount 不完整。"
        }
        $proxyImages.Add([string]$proxySpec[0].image); $proxyImageIDs.Add([string]$proxyStatus[0].imageID)
        $creationLabels.Add((Get-PandoraInventoryPodCreationLabels -KubeContext $KubeContext -Pod $pod))
        $labels.Add($pod.metadata.labels); $revisions.Add([string]$status.revision); $null = $podNames.Add([string]$pod.metadata.name)
    }
    $slices = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext -Arguments @('get', 'endpointslices.discovery.k8s.io', '-n', 'pandora-ingress', '-l', 'kubernetes.io/service-name=pandora-edge-envoy', '-o', 'json') -Action '读取 edge EndpointSlice'
    $readyTargets = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach ($slice in @($slices.items)) { foreach ($endpoint in @($slice.endpoints)) {
        $target = Get-PandoraInventoryObjectProperty $endpoint 'targetRef'
        if ((Get-PandoraInventoryObjectProperty $endpoint.conditions 'ready') -eq $true -and $null -ne $target -and [string]$target.kind -ceq 'Pod') {
            $null = $readyTargets.Add([string]$target.name)
        }
    }}
    foreach ($name in $podNames) { if (-not $readyTargets.Contains($name)) { throw "edge Pod/$name 不在 Ready EndpointSlice。" } }
    return [pscustomobject]@{ Labels = @($labels); CreationLabels = @($creationLabels); StatusRevisions = @($revisions); ProxyImages = @($proxyImages); ProxyImageIDs = @($proxyImageIDs) }
}

function Assert-PandoraInventoryNetworkPolicyState {
    param(
        [Parameter(Mandatory = $true)][string]$KubeContext,
        [Parameter(Mandatory = $true)][object[]]$InventoryPodLabels
    )
    if ($InventoryPodLabels.Count -eq 0) { throw '缺 live Inventory Pod 完整 labels，无法审查 additive NetworkPolicy。' }
    $list = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext -Arguments @('get', 'networkpolicies', '-n', 'pandora', '-o', 'json') -Action '读取全部 pandora NetworkPolicy'
    foreach ($labels in $InventoryPodLabels) {
        $selectedForPod = @($list.items | Where-Object {
                Test-PandoraInventoryLabelSelectorMatch -Selector $_.spec.podSelector -Labels $labels
            })
        $namesForPod = @($selectedForPod | ForEach-Object { [string]$_.metadata.name } | Sort-Object -CaseSensitive)
        if (($namesForPod -join ',') -cne 'allow-inventory-grpc,default-deny-ingress') {
            throw "完整 live labels 命中 Inventory 的 additive NetworkPolicy 集合不精确:$($namesForPod -join ',')。"
        }
    }
    $selected = @($list.items | Where-Object { [string]$_.metadata.name -cin @('allow-inventory-grpc', 'default-deny-ingress') })
    $defaultDeny = @($selected | Where-Object { [string]$_.metadata.name -ceq 'default-deny-ingress' })[0]
    if (@((Get-PandoraInventoryObjectProperty $defaultDeny.spec 'ingress') | Where-Object { $null -ne $_ }).Count -ne 0 -or
        (@($defaultDeny.spec.policyTypes | ForEach-Object { [string]$_ }) -join ',') -cne 'Ingress') {
        throw 'default-deny-ingress 必须无任何 ingress allow。'
    }
    $policy = @($selected | Where-Object { [string]$_.metadata.name -ceq 'allow-inventory-grpc' })[0]
    if ([string]$policy.spec.podSelector.matchLabels.app -cne 'inventory' -or
        @($policy.spec.podSelector.matchLabels.PSObject.Properties).Count -ne 1 -or
        @($policy.spec.ingress).Count -ne 1) { throw 'Inventory 专用 NetPol selector/ingress 漂移。' }
    $ports = @($policy.spec.ingress[0].ports)
    if ($ports.Count -ne 1 -or [string]$ports[0].protocol -cne 'TCP' -or [int]$ports[0].port -ne 20015 -or
        $null -ne (Get-PandoraInventoryObjectProperty $ports[0] 'endPort')) { throw 'Inventory 专用 NetPol 只允许 TCP/20015。' }
    $from = @($policy.spec.ingress[0].from)
    if ($from.Count -ne 2) { throw 'Inventory 专用 NetPol 必须恰好两个来源 peer。' }
    $sameNamespace = @($from | Where-Object {
            $null -ne (Get-PandoraInventoryObjectProperty $_ 'podSelector') -and
            $null -eq (Get-PandoraInventoryObjectProperty $_ 'namespaceSelector') -and
            $null -eq (Get-PandoraInventoryObjectProperty $_ 'ipBlock')
        })
    $edge = @($from | Where-Object {
            $null -ne (Get-PandoraInventoryObjectProperty $_ 'podSelector') -and
            $null -ne (Get-PandoraInventoryObjectProperty $_ 'namespaceSelector') -and
            $null -eq (Get-PandoraInventoryObjectProperty $_ 'ipBlock')
        })
    if ($sameNamespace.Count -ne 1 -or $edge.Count -ne 1) { throw 'Inventory NetPol 来源必须是同ns调用方 + 标记edge ns，禁止 ipBlock/空 peer。' }
    $expressions = @($sameNamespace[0].podSelector.matchExpressions)
    $appIn = @($expressions | Where-Object { [string]$_.key -ceq 'app' -and [string]$_.operator -ceq 'In' })
    $expectedApps = @('auction', 'battle-result', 'leaderboard', 'mail', 'trade')
    if ($expressions.Count -ne 1 -or $appIn.Count -ne 1 -or
        ((@($appIn[0].values | ForEach-Object { [string]$_ } | Sort-Object -CaseSensitive) -join ',') -cne ($expectedApps -join ','))) {
        throw 'Inventory NetPol 同ns来源必须精确为五个真实调用方。'
    }
    if ([string]$edge[0].namespaceSelector.matchLabels.'pandora.dev/role' -cne 'ingress-gateway' -or
        @($edge[0].namespaceSelector.matchLabels.PSObject.Properties).Count -ne 1 -or
        [string]$edge[0].podSelector.matchLabels.'app.kubernetes.io/name' -cne 'pandora-edge-envoy' -or
        @($edge[0].podSelector.matchLabels.PSObject.Properties).Count -ne 1) {
        throw 'Inventory NetPol edge 来源 namespace+Pod identity 不精确。'
    }
}

function ConvertTo-PandoraInventoryCanonicalValue {
    param($Value)
    if ($null -eq $Value) { return $null }
    if ($Value -is [string] -or $Value -is [ValueType]) { return $Value }
    if ($Value -is [System.Collections.IDictionary]) {
        $ordered = [ordered]@{}
        foreach ($key in @($Value.Keys | ForEach-Object { [string]$_ } | Sort-Object -CaseSensitive)) {
            $ordered[$key] = ConvertTo-PandoraInventoryCanonicalValue $Value[$key]
        }
        return $ordered
    }
    if ($Value -is [System.Collections.IEnumerable]) {
        return @($Value | ForEach-Object { ConvertTo-PandoraInventoryCanonicalValue $_ })
    }
    $result = [ordered]@{}
    foreach ($property in @($Value.PSObject.Properties | Sort-Object Name -CaseSensitive)) {
        $result[[string]$property.Name] = ConvertTo-PandoraInventoryCanonicalValue $property.Value
    }
    return $result
}

function Get-PandoraInventoryLocalAdmissionObjects {
    $manifest = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../../../deploy/k8s/overlays/online/inventory-mesh/gate/admission.yaml'))
    if (-not (Test-Path -LiteralPath $manifest -PathType Leaf)) { throw "缺本地 Inventory VAP manifest:$manifest" }
    $documents = @([regex]::Split((Get-Content -LiteralPath $manifest -Raw), '(?m)^---\s*$') | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $objects = [System.Collections.Generic.List[object]]::new()
    foreach ($document in $documents) {
        if ($document.TrimStart().StartsWith('#', [StringComparison]::Ordinal) -and $document -notmatch '(?m)^apiVersion:') { continue }
        $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-inventory-vap-' + [guid]::NewGuid().ToString('N') + '.yaml')
        try {
            [System.IO.File]::WriteAllText($tmp, $document, [System.Text.UTF8Encoding]::new($false))
            $lines = @(& kubectl create --dry-run=client --validate=false -f $tmp -o json 2>&1)
            if ($LASTEXITCODE -ne 0) { throw "本地 VAP manifest 解析失败:$($lines -join [Environment]::NewLine)" }
            $objects.Add((($lines | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine | ConvertFrom-Json -Depth 100))
        } finally { Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue }
    }
    if ($objects.Count -ne 14) { throw "本地 Inventory admission 对象数=$($objects.Count)，expected=14。" }
    return @($objects)
}

function Assert-PandoraInventoryAdmissionGateState {
    param([Parameter(Mandatory = $true)][string]$KubeContext)
    foreach ($expected in (Get-PandoraInventoryLocalAdmissionObjects)) {
        $resource = switch ([string]$expected.kind) {
            'ValidatingAdmissionPolicy' { 'validatingadmissionpolicy' }
            'ValidatingAdmissionPolicyBinding' { 'validatingadmissionpolicybinding' }
            default { throw "本地 admission kind 非法:$($expected.kind)" }
        }
        $live = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext `
            -Arguments @('get', "$resource/$($expected.metadata.name)", '-o', 'json') -Action "读取 $resource/$($expected.metadata.name)"
        if ([string]$live.apiVersion -cne [string]$expected.apiVersion -or [string]$live.kind -cne [string]$expected.kind) {
            throw "$resource/$($expected.metadata.name) apiVersion/kind 漂移。"
        }
        $want = (ConvertTo-PandoraInventoryCanonicalValue $expected.spec | ConvertTo-Json -Compress -Depth 100)
        $actual = (ConvertTo-PandoraInventoryCanonicalValue $live.spec | ConvertTo-Json -Compress -Depth 100)
        if ($actual -cne $want) { throw "$resource/$($expected.metadata.name) spec 与仓库 exact gate 不一致。" }
        if ([string]$expected.kind -ceq 'ValidatingAdmissionPolicy') {
            $status = Get-PandoraInventoryObjectProperty $live 'status'
            if ($null -eq $status -or [long](Get-PandoraInventoryObjectProperty $status 'observedGeneration') -ne [long]$live.metadata.generation) {
                throw "$resource/$($expected.metadata.name) controller 尚未 observed 当前 generation。"
            }
            $typeChecking = Get-PandoraInventoryObjectProperty $status 'typeChecking'
            $warnings = @((Get-PandoraInventoryObjectProperty $typeChecking 'expressionWarnings') | Where-Object { $null -ne $_ })
            if ($warnings.Count -ne 0) { throw "$resource/$($expected.metadata.name) 存在 CEL expressionWarnings。" }
            foreach ($condition in @((Get-PandoraInventoryObjectProperty $status 'conditions') | Where-Object { $null -ne $_ })) {
                if ([string]$condition.status -ceq 'False' -or [string]$condition.reason -cmatch '(?i)error|fail' -or
                    [string]$condition.message -cmatch '(?i)error|fail') {
                    throw "$resource/$($expected.metadata.name) controller condition 未收敛:$($condition.type)/$($condition.reason)。"
                }
            }
        }
    }
}

function Assert-OnlineInventoryMeshPreflight {
    param([Parameter(Mandatory = $true)][string]$KubeContext, [Parameter(Mandatory = $true)][string]$Revision)
    Assert-PandoraIstioRevision -Revision $Revision
    Assert-PandoraInventoryAdmissionApiPrerequisite -KubeContext $KubeContext
    Assert-PandoraInventoryMeshCRDs -KubeContext $KubeContext
    $namespace = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext -Arguments @('get', 'namespace/pandora', '-o', 'json') -Action '读取 pandora namespace gate'
    if ([string](Get-PandoraInventoryObjectProperty $namespace.metadata.labels 'pandora.dev/inventory-mesh-revision') -cne $Revision) {
        throw 'Namespace/pandora Inventory mesh revision gate 未激活或与请求 revision 不一致。'
    }
    foreach ($entry in (Get-PandoraInventoryMeshWorkloads).GetEnumerator()) {
        $sa = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext -Arguments @('get', "serviceaccount/$($entry.Value)", '-n', 'pandora', '-o', 'json') -Action "读取 SA/$($entry.Value)"
        if ([string]$sa.metadata.name -cne [string]$entry.Value -or $sa.automountServiceAccountToken -ne $false) { throw "SA/$($entry.Value) 漂移。" }
    }
    $workloadState = Get-PandoraInventoryLiveWorkloadState -KubeContext $KubeContext -Revision $Revision
    $edgeState = Get-PandoraInventoryEdgeState -KubeContext $KubeContext -Revision $Revision
    $controlPlane = Resolve-PandoraInventoryIstioControlPlane -KubeContext $KubeContext -RequestedRevision $Revision `
        -PodLabels @($workloadState.CreationLabels) -NamespaceName pandora
    $edgeControlPlane = Resolve-PandoraInventoryIstioControlPlane -KubeContext $KubeContext -RequestedRevision $Revision `
        -PodLabels @($edgeState.CreationLabels) -NamespaceName pandora-ingress
    if ($edgeControlPlane.ActualRevision -cne $controlPlane.ActualRevision -or
        $edgeControlPlane.RootNamespace -cne $controlPlane.RootNamespace -or
        $edgeControlPlane.TrustDomain -cne $controlPlane.TrustDomain) {
        throw '生产 edge 与 Inventory 六 workload 未绑定同一 actual revision/rootNamespace/trustDomain。'
    }
    foreach ($actual in @($workloadState.StatusRevisions) + @($edgeState.StatusRevisions)) {
        if ([string]$actual -cne [string]$controlPlane.ActualRevision) { throw "sidecar status actual revision=$actual 与 selected istiod=$($controlPlane.ActualRevision) 不一致。" }
    }
    $proxyImages = @(@($workloadState.ProxyImages) + @($edgeState.ProxyImages) | Sort-Object -CaseSensitive -Unique)
    $proxyImageIDs = @(@($workloadState.ProxyImageIDs) + @($edgeState.ProxyImageIDs) | Sort-Object -CaseSensitive -Unique)
    if ($proxyImages.Count -ne 1 -or $proxyImageIDs.Count -ne 1) {
        throw '同一 selected injector 生成的 Inventory/调用方/edge proxy image 或运行 imageID 不一致。'
    }
    $inventoryLabels = @($workloadState.Labels | Where-Object { [string](Get-PandoraInventoryObjectProperty $_ 'app') -ceq 'inventory' })
    $policies = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext -Arguments @('get', 'authorizationpolicies.security.istio.io', '-A', '-o', 'json') -Action '读取全部 AuthorizationPolicy'
    Assert-PandoraInventoryAuthorizationPolicySet -Policies @($policies.items) -InventoryPodLabels $inventoryLabels -Phase enforce -RootNamespace $controlPlane.RootNamespace
    $peers = Get-PandoraInventoryMeshKubectlJson -KubeContext $KubeContext -Arguments @('get', 'peerauthentications.security.istio.io', '-A', '-o', 'json') -Action '读取全部 PeerAuthentication'
    Assert-PandoraInventoryPeerAuthenticationSet -PeerAuthentications @($peers.items) -InventoryPodLabels $inventoryLabels -Mode STRICT -RootNamespace $controlPlane.RootNamespace
    Assert-PandoraInventoryAdmissionGateState -KubeContext $KubeContext
    Assert-PandoraInventoryNetworkPolicyState -KubeContext $KubeContext -InventoryPodLabels $inventoryLabels
    return $controlPlane
}
