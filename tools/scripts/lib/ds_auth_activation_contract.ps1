# DS callback auth 一次性 epoch 激活的纯契约。
#
# 本文件只验证对象/构造只读客户端参数，不创建 CA、证书、密码、Secret、Job 或 Deployment。
# 生产信任材料和固定 synthetic Job 必须由平台在仓库外预置；证据缺失一律失败。
Set-StrictMode -Version Latest

function Assert-PandoraRedisTopologyChangeLockProvider(
    [string]$Checkpoint,
    [string]$RedisTopology,
    [string]$RedisConfigIdentity
) {
    if ([string]::IsNullOrWhiteSpace($Checkpoint) -or [string]::IsNullOrWhiteSpace($RedisTopology) -or
        [string]::IsNullOrWhiteSpace($RedisConfigIdentity)) {
        throw 'Redis topology-change lock provider gate 缺绑定上下文。'
    }
    # Deliberate shared fail-closed hook. A future implementation must verify
    # the same provider-signed/online lease here for both activation and
    # ordinary release; callers may not replace it with a ConfigMap marker.
    throw ("RELEASE BLOCKED: authoritative Redis topology-change lock provider 未接线；" +
        "checkpoint=$Checkpoint topology=$RedisTopology identity=$RedisConfigIdentity。")
}

function Assert-PandoraPodUIDACLCleanupTimeline(
    [datetimeoffset]$RequiredCreated,
    [datetimeoffset]$SwitchCreated,
    [datetimeoffset]$ProofCompleted,
    [datetimeoffset]$CompleteCreated
) {
    if ($RequiredCreated -gt $SwitchCreated -or $SwitchCreated -gt $ProofCompleted -or
        $ProofCompleted -gt $CompleteCreated) {
        throw 'pod_uid ACL cleanup 时间链 required<=switch<=proof<=complete 非法。'
    }
}

$script:PandoraDsAuthWriterApps = @('login', 'player-locator', 'ds-allocator', 'hub-allocator', 'battle-result')
$script:PandoraDsAuthRequiredFeaturesV2 =
    'hub_allocator=hub-reservation-ledger-v1|hub-heartbeat-capacity-v1|hub-owner-cleanup-v1|hub-physical-eviction-v1,' +
    'ds_allocator=battle-release-expected-tuple-v1|battle-storage-pod-uid-write-invariant-v1,' +
    'battle_result=battle-terminal-outbox-v1'
$script:PandoraDsAuthRequiredFeaturesV3 =
    'hub_allocator=hub-reservation-ledger-v1|hub-heartbeat-capacity-v1|hub-owner-cleanup-v1|hub-physical-eviction-v1|hub-successor-lease-v1,' +
    'ds_allocator=battle-release-expected-tuple-v1|battle-storage-pod-uid-write-invariant-v1,' +
    'battle_result=battle-terminal-outbox-v1'
# Compatibility for the immutable V1->V2 activation script. Never mutate this
# alias to V3: the V2 policy ID must retain its original exact feature meaning.
$script:PandoraDsAuthRequiredFeatures = $script:PandoraDsAuthRequiredFeaturesV2
$script:PandoraDsAuthPolicyV3EvidenceName = 'pandora-ds-auth-policy-v3-evidence'
$script:PandoraDsAuthIdentityMountPath = '/run/secrets/pandora/ds-auth-etcd'
$script:PandoraPodUIDReleasePreflightBaseArgs = @(
    '-conf', 'etc/pod-uid-preflight.yaml', '-pod-uid-release-preflight',
    '-pod-uid-release-preflight-timeout=10m', '-pod-uid-release-preflight-scan-count=1000'
)

function Assert-PandoraDsAuthPolicyV3EvidenceContract(
    $Marker,
    [string]$ExpectedServices,
    [string]$ExpectedInstances,
    [string]$ExpectedImageDigests,
    [string]$ExpectedKubeContext = '',
    [string]$ExpectedNamespace = ''
) {
    if ($null -eq $Marker -or $Marker.immutable -ne $true) {
        throw 'DS auth V3 policy evidence ConfigMap must exist and be immutable.'
    }
	if ([string]$Marker.metadata.uid -cnotmatch '^[0-9a-f][0-9a-f-]{7,127}$' -or
		[string]$Marker.metadata.resourceVersion -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') {
		throw 'DS auth V3 policy evidence metadata identity is not canonical.'
	}
    $deletingProperty = $Marker.metadata.PSObject.Properties['deletionTimestamp']
    if ($null -ne $deletingProperty -and -not [string]::IsNullOrWhiteSpace([string]$deletingProperty.Value)) {
        throw 'DS auth V3 policy evidence ConfigMap is deleting.'
    }
    $keys = @('contract', 'run_id', 'kube_context', 'namespace',
        'from_policy_generation', 'to_policy_generation',
        'from_required_value', 'to_required_value', 'required_policy_id',
        'staging_contract', 'expected_services', 'expected_instances',
        'expected_image_digests', 'required_features', 'evidence_sha256',
        'completed_at_unix_ms')
    if ((@($Marker.data.PSObject.Properties.Name | Sort-Object) -join ',') -cne
        (@($keys | Sort-Object) -join ',')) {
        throw 'DS auth V3 policy evidence data keys are not the exact contract.'
    }
    $completionMS = [int64]0
    if ([string]$Marker.data.contract -cne 'ds-auth-policy-v3-activation-evidence-v1' -or
        [string]$Marker.data.run_id -cnotmatch '^[a-z0-9][a-z0-9-]{7,31}$' -or
        [string]$Marker.data.kube_context -cnotmatch '^[A-Za-z0-9._:-]{1,128}$' -or
        [string]$Marker.data.namespace -cnotmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$' -or
        (-not [string]::IsNullOrWhiteSpace($ExpectedKubeContext) -and
            [string]$Marker.data.kube_context -cne $ExpectedKubeContext) -or
        (-not [string]::IsNullOrWhiteSpace($ExpectedNamespace) -and
            [string]$Marker.data.namespace -cne $ExpectedNamespace) -or
        [string]$Marker.data.from_policy_generation -cne '2' -or
        [string]$Marker.data.to_policy_generation -cne '3' -or
        [string]$Marker.data.from_required_value -cne '2@ds-auth-v2-pod-uid-write-invariant-v1' -or
        [string]$Marker.data.to_required_value -cne '2@ds-auth-v2-hub-successor-lease-v1' -or
        [string]$Marker.data.required_policy_id -cne 'ds-auth-v2-hub-successor-lease-v1' -or
        [string]$Marker.data.staging_contract -cne 'capability-lease-not-service-endpoint-v1' -or
        [string]$Marker.data.required_features -cne $script:PandoraDsAuthRequiredFeaturesV3 -or
        [string]$Marker.data.expected_services -cne $ExpectedServices -or
        [string]$Marker.data.expected_instances -cne $ExpectedInstances -or
        [string]$Marker.data.expected_image_digests -cne $ExpectedImageDigests -or
        [string]$Marker.data.evidence_sha256 -cnotmatch '^sha256:[0-9a-f]{64}$' -or
        -not [int64]::TryParse([string]$Marker.data.completed_at_unix_ms, [ref]$completionMS) -or
        $completionMS -le 0) {
        throw 'DS auth V3 policy evidence does not bind the exact staged capability/policy contract.'
    }
    $calculatedEvidence = Get-PandoraDsAuthPolicyV3EvidenceSHA256 $Marker.data
    if ([string]$Marker.data.evidence_sha256 -cne $calculatedEvidence) {
        throw 'DS auth V3 policy evidence digest does not match its canonical exact payload.'
    }
    $created = [datetimeoffset]::Parse([string]$Marker.metadata.creationTimestamp)
    if ([datetimeoffset]::FromUnixTimeMilliseconds($completionMS) -gt $created) {
        throw 'DS auth V3 policy evidence completion time is after immutable marker creation.'
    }
    return [pscustomobject][ordered]@{
        UID = [string]$Marker.metadata.uid
        ResourceVersion = [string]$Marker.metadata.resourceVersion
        EvidenceSHA256 = [string]$Marker.data.evidence_sha256
        CompletedAtUnixMS = $completionMS
        ExpectedServices = [string]$Marker.data.expected_services
        ExpectedInstances = [string]$Marker.data.expected_instances
        ExpectedImageDigests = [string]$Marker.data.expected_image_digests
        RequiredFeatures = [string]$Marker.data.required_features
        RunID = [string]$Marker.data.run_id
        KubeContext = [string]$Marker.data.kube_context
        Namespace = [string]$Marker.data.namespace
    }
}

function Get-PandoraDsAuthPolicyV3EvidenceSHA256($Data) {
    $fields = @('contract', 'run_id', 'kube_context', 'namespace', 'from_policy_generation',
        'to_policy_generation', 'from_required_value', 'to_required_value', 'required_policy_id',
        'staging_contract', 'expected_services', 'expected_instances', 'expected_image_digests',
        'required_features', 'completed_at_unix_ms')
    $lines = [System.Collections.Generic.List[string]]::new()
    foreach ($field in $fields) {
        $value = [string]$Data.$field
        if ([string]::IsNullOrWhiteSpace($value) -or $value.Contains("`r") -or $value.Contains("`n")) {
            throw "DS auth V3 evidence canonical field=$field is empty or multiline."
        }
        $lines.Add("$field=$value")
    }
    $sha = [System.Security.Cryptography.SHA256]::Create()
    try {
        $bytes = [Text.Encoding]::UTF8.GetBytes(($lines -join "`n"))
        $hex = ([BitConverter]::ToString($sha.ComputeHash($bytes))).Replace('-', '').ToLowerInvariant()
        return "sha256:$hex"
    } finally { $sha.Dispose() }
}

function Assert-PandoraDsAuthV3CompletionContract(
    $Marker,
    [string]$ExpectedName,
    [string]$ExpectedNamespace,
    $PolicyEvidence,
    [hashtable]$ExpectedCounts
) {
    $keys = @('contract', 'run_id', 'required_value', 'policy_generation', 'evidence_uid',
        'evidence_resource_version', 'evidence_sha256', 'final_instances', 'completed_at_unix_ms')
    $deleting = $Marker.metadata.PSObject.Properties['deletionTimestamp']
    $completedMS = [int64]0
    if ($Marker.immutable -ne $true -or [string]$Marker.metadata.name -cne $ExpectedName -or
        [string]$Marker.metadata.namespace -cne $ExpectedNamespace -or
        [string]$Marker.metadata.uid -cnotmatch '^[0-9a-f][0-9a-f-]{7,127}$' -or
        [string]$Marker.metadata.resourceVersion -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$' -or
        [string]::IsNullOrWhiteSpace([string]$Marker.metadata.creationTimestamp) -or
        ($null -ne $deleting -and -not [string]::IsNullOrWhiteSpace([string]$deleting.Value)) -or
        (@($Marker.data.PSObject.Properties.Name | Sort-Object) -join ',') -cne (@($keys | Sort-Object) -join ',') -or
        [string]$Marker.data.contract -cne 'ds-auth-policy-v3-completion-v1' -or
        [string]$Marker.data.run_id -cne [string]$PolicyEvidence.RunID -or
        [string]$Marker.data.required_value -cne '2@ds-auth-v2-hub-successor-lease-v1' -or
        [string]$Marker.data.policy_generation -cne '3' -or
        [string]$Marker.data.evidence_uid -cne [string]$PolicyEvidence.UID -or
        [string]$Marker.data.evidence_resource_version -cne [string]$PolicyEvidence.ResourceVersion -or
        [string]$Marker.data.evidence_sha256 -cne [string]$PolicyEvidence.EvidenceSHA256 -or
        -not [int64]::TryParse([string]$Marker.data.completed_at_unix_ms, [ref]$completedMS) -or
        $completedMS -le 0) {
        throw 'V3 completion marker metadata/data is not the exact immutable activation contract.'
    }
    $created = [datetimeoffset]::Parse([string]$Marker.metadata.creationTimestamp)
    if ([datetimeoffset]::FromUnixTimeMilliseconds($completedMS) -gt $created) {
        throw 'V3 completion marker completion time is after marker creation.'
    }
    $instances = @{}
    $allUIDs = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
    foreach ($item in ([string]$Marker.data.final_instances).Split(',')) {
        $parts = $item.Split('=', 2)
        if ($parts.Count -ne 2 -or -not $ExpectedCounts.ContainsKey($parts[0]) -or $instances.ContainsKey($parts[0])) {
            throw 'V3 completion final_instances contains an unknown, duplicate, or malformed service.'
        }
        $rawUIDs = @($parts[1].Split('|'))
        $uids = @($rawUIDs | Sort-Object -Unique)
        if ($rawUIDs.Count -ne $uids.Count -or $uids.Count -ne [int]$ExpectedCounts[$parts[0]] -or
            @($uids | Where-Object { $_ -cnotmatch '^[0-9A-Za-z][0-9A-Za-z._:-]{0,127}$' }).Count -ne 0) {
            throw "V3 completion final_instances count/UID for $($parts[0]) is invalid."
        }
        foreach ($uid in $uids) {
            if (-not $allUIDs.Add([string]$uid)) {
                throw 'V3 completion final_instances contains a duplicate UID across services.'
            }
        }
        $instances[$parts[0]] = $true
    }
    if ($instances.Count -ne $ExpectedCounts.Count -or [int]$ExpectedCounts['hub_allocator'] -ne 1) {
        throw 'V3 completion final_instances is not the exact five-service/single-Hub structure.'
    }
    return [pscustomobject][ordered]@{
        Name = [string]$Marker.metadata.name
        Namespace = [string]$Marker.metadata.namespace
        UID = [string]$Marker.metadata.uid
        ResourceVersion = [string]$Marker.metadata.resourceVersion
        FinalInstances = [string]$Marker.data.final_instances
        CompletedAtUnixMS = $completedMS
    }
}

function Resolve-PandoraDsAuthV3CanonicalUIDSet(
    [string]$Service,
    [string[]]$CurrentUIDs,
    [int]$Desired,
    [string[]]$PreCASExpectedUIDs = @(),
    [switch]$PreCAS
) {
    $current = @($CurrentUIDs | Sort-Object -Unique)
    if ($Desired -le 0 -or $current.Count -ne $Desired -or
        @($CurrentUIDs).Count -ne $current.Count -or
        @($current | Where-Object { $_ -cnotmatch '^[0-9A-Za-z][0-9A-Za-z._:-]{0,127}$' }).Count -ne 0) {
        throw "Canonical non-deleting Pod UID set for $Service is not exact desired=$Desired."
    }
    if ($PreCAS) {
        $expected = @($PreCASExpectedUIDs | Sort-Object -Unique)
        if ($expected.Count -ne $Desired -or ($current -join '|') -cne ($expected -join '|')) {
            throw "Pre-CAS canonical Pod UID set for $Service differs from immutable evidence."
        }
    }
    return ($current -join '|')
}

function Assert-PandoraDsAuthEtcdRevision([string]$Revision) {
    if ($Revision -cnotmatch '^r[1-9][0-9]*$') {
        throw "DS auth etcd identity revision 必须是 canonical rN，实际='$Revision'。"
    }
}

function Assert-PandoraDsAuthHttpsEndpoints([string]$Endpoints) {
    $items = @($Endpoints.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    if ($items.Count -eq 0) { throw 'DS auth etcd endpoints 不能为空。' }
    foreach ($endpoint in $items) {
        $uri = $null
        if (-not [Uri]::TryCreate($endpoint, [UriKind]::Absolute, [ref]$uri) -or
            $uri.Scheme -cne 'https' -or [string]::IsNullOrWhiteSpace($uri.Host) -or
            -not [string]::IsNullOrEmpty($uri.UserInfo) -or $uri.AbsolutePath -cne '/' -or
            -not [string]::IsNullOrEmpty($uri.Query) -or -not [string]::IsNullOrEmpty($uri.Fragment) -or
            $endpoint -cnotmatch '^https://(?:\[[0-9A-Fa-f:]+\]|[A-Za-z0-9._-]+):[1-9][0-9]{0,4}$' -or
            $uri.Port -lt 1 -or $uri.Port -gt 65535) {
            throw "生产 DS auth etcd endpoint 必须是 canonical https://host:port，实际='$endpoint'。"
        }
    }
    return $items
}

function Get-PandoraDsAuthIdentitySecretName([string]$App, [string]$Revision) {
    Assert-PandoraDsAuthEtcdRevision $Revision
    if ($script:PandoraDsAuthWriterApps -cnotcontains $App) { throw "未知 DS auth writer app='$App'。" }
    return "pandora-ds-auth-etcd-$App-$Revision"
}

function Get-PandoraDsAuthClientIdentity([string]$App) {
    if ($script:PandoraDsAuthWriterApps -cnotcontains $App) { throw "未知 DS auth writer app='$App'。" }
    return "pandora-$App"
}

function New-PandoraDsAuthEtcdIdentityPatch([string]$App, [string]$Revision,
    [string]$ServerName, [string]$ForbiddenReadPrefix, [bool]$UsesPasswordAuth,
    [string]$DeploymentName = '') {
    $secretName = Get-PandoraDsAuthIdentitySecretName $App $Revision
    $identity = Get-PandoraDsAuthClientIdentity $App
    if ([string]::IsNullOrWhiteSpace($DeploymentName)) { $DeploymentName = $App }
    if ($DeploymentName -cnotmatch '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$') { throw 'etcd identity patch Deployment name 非法。' }
    if ($ServerName -cnotmatch '^[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$') {
        throw 'etcd TLS server name 不能安全写入 runtime patch。'
    }
    if ($ForbiddenReadPrefix -cnotmatch '^/[A-Za-z0-9._/-]{1,240}/$') {
        throw 'etcd forbidden read prefix 必须是 canonical absolute prefix。'
    }
    $passwordEnv = if ($UsesPasswordAuth) {
@"
            - { name: PANDORA_DS_AUTH_ETCD_USERNAME_FILE, value: $script:PandoraDsAuthIdentityMountPath/username }
            - { name: PANDORA_DS_AUTH_ETCD_PASSWORD_FILE, value: $script:PandoraDsAuthIdentityMountPath/password }
"@
    } else { '' }
    return @"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $DeploymentName
  namespace: pandora
spec:
  template:
    spec:
      securityContext: { runAsNonRoot: true, runAsUser: 10001, runAsGroup: 10001, fsGroup: 10001, fsGroupChangePolicy: OnRootMismatch }
      containers:
        - name: $App
          env:
            - { name: PANDORA_DS_AUTH_ETCD_REQUIRE_MTLS, value: "1" }
            - { name: PANDORA_DS_AUTH_ETCD_CA_FILE, value: $script:PandoraDsAuthIdentityMountPath/ca.crt }
            - { name: PANDORA_DS_AUTH_ETCD_CERT_FILE, value: $script:PandoraDsAuthIdentityMountPath/tls.crt }
            - { name: PANDORA_DS_AUTH_ETCD_KEY_FILE, value: $script:PandoraDsAuthIdentityMountPath/tls.key }
            - { name: PANDORA_DS_AUTH_ETCD_SERVER_NAME, value: $ServerName }
            - { name: PANDORA_DS_AUTH_ETCD_CLIENT_IDENTITY, value: $identity }
            - { name: PANDORA_DS_AUTH_ETCD_IDENTITY_REVISION, value: $Revision }
            - { name: PANDORA_DS_AUTH_ETCD_REQUIRE_AUTH, value: "1" }
            - { name: PANDORA_DS_AUTH_ETCD_FORBIDDEN_READ_PREFIX, value: $ForbiddenReadPrefix }
$passwordEnv          volumeMounts:
            - { name: ds-auth-etcd-identity, mountPath: $script:PandoraDsAuthIdentityMountPath, readOnly: true }
      volumes:
        - name: ds-auth-etcd-identity
          secret: { secretName: $secretName, defaultMode: 0440 }
"@
}

function New-PandoraDsAuthDormantBluePatch([string]$App) {
    if ($script:PandoraDsAuthWriterApps -cnotcontains $App) { throw "未知 DS auth writer app='$App'。" }
    return @"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $App
  namespace: pandora
spec:
  replicas: 0
  selector:
    matchLabels: { app: $App, pandora.dev/ds-auth-writer-set: blue, pandora.dev/ds-auth-writer-epoch: "1" }
  template:
    metadata:
      labels: { app: $App, pandora.dev/ds-auth-writer-set: blue, pandora.dev/ds-auth-writer-epoch: "1" }
"@
}

function New-PandoraDsAuthGreenServicePatch([string]$App) {
    if ($script:PandoraDsAuthWriterApps -cnotcontains $App) { throw "未知 DS auth writer app='$App'。" }
    return @"
apiVersion: v1
kind: Service
metadata:
  name: $App
  namespace: pandora
spec:
  selector: { app: $App, pandora.dev/ds-auth-writer-set: green, pandora.dev/ds-auth-writer-epoch: "2" }
"@
}

function New-PandoraDsAuthCanonicalGreenObject($LiveDeployment, [string]$App, [string]$Revision,
    [string]$ServerName, [string]$ForbiddenReadPrefix, [bool]$UsesPasswordAuth,
    [ValidateRange(1, 99)][int]$DesiredReplicas, [string]$PinnedImage, [string]$Digest) {
    if ($PinnedImage -cnotmatch ('@' + [regex]::Escape($Digest) + '$') -or $Digest -cnotmatch '^sha256:[0-9a-f]{64}$') {
        throw 'canonical green image 必须固定到同一 immutable digest。'
    }
    $greenName = "$App-ds-auth-green"
    if ([string]$LiveDeployment.apiVersion -cne 'apps/v1' -or [string]$LiveDeployment.kind -cne 'Deployment' -or
        [string]$LiveDeployment.metadata.name -cne $greenName -or [string]$LiveDeployment.metadata.namespace -cne 'pandora' -or
        [string]$LiveDeployment.metadata.resourceVersion -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$') {
        throw 'canonical green live Deployment 身份/resourceVersion 非法。'
    }
    if ([string]$LiveDeployment.metadata.annotations.'pandora.dev/ds-auth-green-desired-replicas' -cne [string]$DesiredReplicas -or
        [int]$LiveDeployment.spec.replicas -ne $DesiredReplicas -or
        [string]$LiveDeployment.spec.selector.matchLabels.app -cne $App -or
        [string]$LiveDeployment.spec.selector.matchLabels.'pandora.dev/ds-auth-writer-set' -cne 'green' -or
        [string]$LiveDeployment.spec.selector.matchLabels.'pandora.dev/ds-auth-writer-epoch' -cne '2' -or
        @($LiveDeployment.spec.selector.matchLabels.PSObject.Properties).Count -ne 3) {
        throw 'canonical green live Deployment selector/desired 非 exact contract。'
    }
    $templatePod = [pscustomobject]@{ metadata = $LiveDeployment.spec.template.metadata; spec = $LiveDeployment.spec.template.spec }
    Assert-PandoraDsAuthIdentityPodContract $templatePod $App $Revision $ServerName $ForbiddenReadPrefix $UsesPasswordAuth
    if ($App -in @('battle-result', 'ds-allocator')) { Assert-PandoraDsTerminalMeshTemplateContract $templatePod $App }

    # 从已审计 live 对象构造完整 replace 对象，保留 args/ports/probes/resources/全部 volume、SA、
    # terminal-mesh labels 与 strategy；只替换本次 immutable image/digest。resourceVersion 提供 CAS。
    $spec = (($LiveDeployment.spec | ConvertTo-Json -Depth 40) | ConvertFrom-Json)
    $containers = @($spec.template.spec.containers | Where-Object { [string]$_.name -ceq $App })
    if ($containers.Count -ne 1) { throw 'canonical green live Deployment 缺唯一业务 container。' }
    $containers[0].image = $PinnedImage
    $spec.template.metadata.annotations.'pandora.dev/image-digest' = $Digest

    # Hub owns the single-writer successor/placement lease.  An ordinary
    # canonical-green replacement must never inherit a live RollingUpdate
    # strategy, because maxSurge would briefly create two Hub writers.
    if ($App -ceq 'hub-allocator') {
        if ($DesiredReplicas -ne 1) {
            throw 'canonical green hub-allocator desired replicas must be exactly 1.'
        }
        $spec.replicas = 1
        $spec.strategy = [pscustomobject]@{ type = 'Recreate' }

        # PANDORA_DEPLOY_STRATEGY 由 Pod annotation 经 downward API 注入进程。
        # 只改 spec.strategy 而继承旧模板的 RollingUpdate annotation，会让
        # Recreate 先删旧 Pod 后，新 Pod 因策略门禁 fail-closed，形成全量中断。
        $templateAnnotations = $spec.template.metadata.annotations
        if ($null -eq $templateAnnotations) {
            $templateAnnotations = [pscustomobject]@{}
            $spec.template.metadata.annotations = $templateAnnotations
        }
        if ($null -eq $templateAnnotations.PSObject.Properties['pandora.dev/deploy-strategy']) {
            $templateAnnotations | Add-Member -NotePropertyName 'pandora.dev/deploy-strategy' -NotePropertyValue 'Recreate'
        } else {
            $templateAnnotations.'pandora.dev/deploy-strategy' = 'Recreate'
        }

        $deployStrategyEnv = @($containers[0].env | Where-Object { [string]$_.name -ceq 'PANDORA_DEPLOY_STRATEGY' })
        if ($deployStrategyEnv.Count -gt 1) {
            throw 'canonical green hub-allocator 存在重复 PANDORA_DEPLOY_STRATEGY env。'
        }
        $canonicalDeployStrategyEnv = [pscustomobject]@{
            name = 'PANDORA_DEPLOY_STRATEGY'
            valueFrom = [pscustomobject]@{
                fieldRef = [pscustomobject]@{
                    fieldPath = "metadata.annotations['pandora.dev/deploy-strategy']"
                }
            }
        }
        if ($deployStrategyEnv.Count -eq 0) {
            $containers[0].env = @($containers[0].env) + @($canonicalDeployStrategyEnv)
        } else {
            $envIndex = [array]::IndexOf(@($containers[0].env), $deployStrategyEnv[0])
            $containers[0].env[$envIndex] = $canonicalDeployStrategyEnv
        }
    }

    # The one-time epoch activation temporarily pins ds-allocator green to a
    # per-RunId immutable raw snapshot.  A later ordinary epoch-2 release must
    # deliberately return to the current fixed pandora-config Secret; otherwise
    # rollout restart would silently keep serving the historical snapshot.
    if ($App -ceq 'ds-allocator') {
        $confVolumes = @($spec.template.spec.volumes | Where-Object { [string]$_.name -ceq 'conf' })
        if ($confVolumes.Count -ne 1 -or $null -eq $confVolumes[0].secret) {
            throw 'canonical green ds-allocator 缺唯一 Secret-backed conf volume。'
        }
        $confVolumes[0].secret.secretName = 'pandora-config'
    }

    # Production serves the epoch=2 canonical green Deployment, while the base
    # blue Deployment is deliberately dormant. Therefore putting Recreate +
    # preflight only in services.yaml would be a false gate: an ordinary prod
    # release replaces/restarts green and could still overlap old/new locator
    # writers. Build the exact gate into the CAS replacement object itself.
    if ($App -ceq 'player-locator') {
        $confVolumes = @($spec.template.spec.volumes | Where-Object {
            [string]$_.name -ceq 'conf' -and [string]$_.secret.secretName -ceq 'pandora-config'
        })
        if ($confVolumes.Count -ne 1) {
            throw 'canonical green player-locator 缺唯一 pandora-config volume，无法执行 placement preflight。'
        }
        $strategy = [pscustomobject]@{ type = 'Recreate' }
        if ($null -eq $spec.PSObject.Properties['strategy']) {
            $spec | Add-Member -NotePropertyName strategy -NotePropertyValue $strategy
        } else {
            $spec.strategy = $strategy
        }
        $preflight = [pscustomobject]@{
            name = 'placement-preflight'
            image = $PinnedImage
            imagePullPolicy = 'IfNotPresent'
            args = @('-conf', 'etc/cluster.yaml', '-placement-preflight',
                '-placement-preflight-timeout=10m', '-placement-preflight-scan-count=1000')
            volumeMounts = @([pscustomobject]@{
                name = 'conf'; mountPath = '/app/etc/cluster.yaml'; subPath = 'player-locator.yaml'; readOnly = $true
            })
            resources = [pscustomobject]@{
                requests = [pscustomobject]@{ cpu = '25m'; memory = '32Mi' }
                limits = [pscustomobject]@{ cpu = '1'; memory = '256Mi' }
            }
        }
        if ($null -eq $spec.template.spec.PSObject.Properties['initContainers']) {
            $spec.template.spec | Add-Member -NotePropertyName initContainers -NotePropertyValue @($preflight)
        } else {
            $spec.template.spec.initContainers = @($preflight)
        }
    }
    $candidate = [pscustomobject]@{
        apiVersion = 'apps/v1'
        kind = 'Deployment'
        metadata = [pscustomobject]@{
            name = $greenName
            namespace = 'pandora'
            resourceVersion = [string]$LiveDeployment.metadata.resourceVersion
            labels = $LiveDeployment.metadata.labels
            annotations = $LiveDeployment.metadata.annotations
        }
        spec = $spec
    }
    if ($App -ceq 'player-locator') {
        Assert-PandoraPlayerLocatorPlacementPreflightObjectContract $candidate $PinnedImage
    }
    if ($App -ceq 'hub-allocator') {
        Assert-PandoraHubAllocatorSingleWriterDeploymentContract $candidate
    }
    return $candidate
}

function Assert-PandoraHubAllocatorSingleWriterDeploymentContract($Deployment) {
    $containers = @($Deployment.spec.template.spec.containers | Where-Object { [string]$_.name -ceq 'hub-allocator' })
    $strategyEnv = if ($containers.Count -eq 1) {
        @($containers[0].env | Where-Object { [string]$_.name -ceq 'PANDORA_DEPLOY_STRATEGY' })
    } else {
        @()
    }
    $strategyEnvHasLiteralValue = $false
    if ($strategyEnv.Count -eq 1 -and $null -ne $strategyEnv[0].PSObject.Properties['value']) {
        $strategyEnvHasLiteralValue = -not [string]::IsNullOrEmpty([string]$strategyEnv[0].value)
    }
    if ([string]$Deployment.apiVersion -cne 'apps/v1' -or [string]$Deployment.kind -cne 'Deployment' -or
        [string]$Deployment.metadata.name -cnotin @('hub-allocator', 'hub-allocator-ds-auth-green') -or
        [int]$Deployment.spec.replicas -ne 1 -or [string]$Deployment.spec.strategy.type -cne 'Recreate' -or
        $null -ne $Deployment.spec.strategy.PSObject.Properties['rollingUpdate'] -or
        [string]$Deployment.spec.template.metadata.annotations.'pandora.dev/deploy-strategy' -cne 'Recreate' -or
        $containers.Count -ne 1 -or $strategyEnv.Count -ne 1 -or
        $strategyEnvHasLiteralValue -or
        [string]$strategyEnv[0].valueFrom.fieldRef.fieldPath -cne "metadata.annotations['pandora.dev/deploy-strategy']") {
        throw 'hub-allocator placement/successor writer must be exact replicas=1 with Recreate, matching Pod annotation/downward-API env, and no rollingUpdate.'
    }
}

function Assert-PandoraHubAllocatorGreenServiceContract($Service, [string]$Namespace = 'pandora') {
    $selector = $Service.spec.selector
    $ports = @($Service.spec.ports)
    $publishNotReady = $Service.spec.PSObject.Properties['publishNotReadyAddresses']
    if ([string]$Service.apiVersion -cne 'v1' -or [string]$Service.kind -cne 'Service' -or
        [string]$Service.metadata.name -cne 'hub-allocator' -or
        [string]$Service.metadata.namespace -cne $Namespace -or
        [string]$Service.metadata.uid -cnotmatch '^[0-9a-f][0-9a-f-]{7,127}$' -or
        (Test-PandoraKubernetesObjectDeleting $Service) -or
        [string]$Service.spec.type -cne 'ClusterIP' -or
        [string]::IsNullOrWhiteSpace([string]$Service.spec.clusterIP) -or
        [string]$Service.spec.clusterIP -ceq 'None' -or
        ($null -ne $publishNotReady -and $publishNotReady.Value -eq $true) -or
        [string]$selector.app -cne 'hub-allocator' -or
        [string]$selector.'pandora.dev/ds-auth-writer-set' -cne 'green' -or
        [string]$selector.'pandora.dev/ds-auth-writer-epoch' -cne '2' -or
        @($selector.PSObject.Properties).Count -ne 3 -or
        $ports.Count -ne 1 -or [int]$ports[0].port -ne 20021 -or
        [string]$ports[0].targetPort -cne '20021' -or [string]$ports[0].protocol -cne 'TCP') {
        throw 'Service/hub-allocator must be the exact canonical green ClusterIP/20021 selector contract.'
    }
}

function Assert-PandoraDsAuthV3GreenStageDeploymentContract(
    $Deployment,
    [string]$App,
    [string]$Revision,
    [string]$ServerName,
    [string]$ForbiddenReadPrefix,
    [bool]$UsesPasswordAuth
) {
    if ($script:PandoraDsAuthWriterApps -cnotcontains $App) {
        throw "Unknown V3 staged DS-auth writer app=$App."
    }
    $name = "$App-ds-auth-green"
    $desiredRaw = [string]$Deployment.metadata.annotations.'pandora.dev/ds-auth-green-desired-replicas'
    $selector = $Deployment.spec.selector.matchLabels
    if ([string]$Deployment.apiVersion -cne 'apps/v1' -or [string]$Deployment.kind -cne 'Deployment' -or
        [string]$Deployment.metadata.name -cne $name -or [string]$Deployment.metadata.namespace -cne 'pandora' -or
        [string]$Deployment.metadata.labels.app -cne $App -or
        (Test-PandoraKubernetesObjectDeleting $Deployment) -or $desiredRaw -cnotmatch '^[1-9][0-9]?$' -or
        [int]$Deployment.spec.replicas -ne [int]$desiredRaw -or
        [string]$selector.app -cne $App -or
        [string]$selector.'pandora.dev/ds-auth-writer-set' -cne 'green' -or
        [string]$selector.'pandora.dev/ds-auth-writer-epoch' -cne '2' -or
        @($selector.PSObject.Properties).Count -ne 3) {
        throw "Deployment/$name is not an exact canonical staged V3 writer."
    }
    foreach ($label in @($selector.PSObject.Properties)) {
        if ([string]$Deployment.spec.template.metadata.labels.($label.Name) -cne [string]$label.Value) {
            throw "Deployment/$name template does not carry its exact immutable selector label=$($label.Name)."
        }
    }
    $template = [pscustomobject]@{
        metadata = $Deployment.spec.template.metadata
        spec = $Deployment.spec.template.spec
    }
    Assert-PandoraDsAuthIdentityPodContract $template $App $Revision $ServerName `
        $ForbiddenReadPrefix $UsesPasswordAuth
    $containers = @($Deployment.spec.template.spec.containers | Where-Object { [string]$_.name -ceq $App })
    if ($containers.Count -ne 1 -or [string]$containers[0].image -cnotmatch '@(sha256:[0-9a-f]{64})$') {
        throw "Deployment/$name target writer image/template digest is not one exact immutable pin."
    }
    $digest = [string]$Matches[1]
    if ([string]$Deployment.spec.template.metadata.annotations.'pandora.dev/image-digest' -cne $digest) {
        throw "Deployment/$name target writer image/template digest is not one exact immutable pin."
    }
    $pinnedImage = [string]$containers[0].image
    if ($App -ceq 'hub-allocator') {
        Assert-PandoraHubAllocatorSingleWriterDeploymentContract $Deployment
    }
    if ($App -ceq 'player-locator') {
        Assert-PandoraPlayerLocatorPlacementPreflightObjectContract $Deployment $pinnedImage
    }
    if ($App -in @('battle-result', 'ds-allocator')) {
        Assert-PandoraDsTerminalMeshTemplateContract $template $App
    }
    return [pscustomobject][ordered]@{
        DesiredReplicas = [int]$desiredRaw
        Digest = $digest
        PinnedImage = $pinnedImage
    }
}

function Assert-PandoraPlayerLocatorPlacementPreflightObjectContract($Deployment, [string]$ExpectedPinnedImage = '') {
    if ([string]$Deployment.apiVersion -cne 'apps/v1' -or [string]$Deployment.kind -cne 'Deployment' -or
        [string]$Deployment.metadata.name -cnotin @('player-locator', 'player-locator-ds-auth-green')) {
        throw 'placement preflight Deployment 身份非法。'
    }
    if ([string]$Deployment.spec.strategy.type -cne 'Recreate' -or
        $null -ne $Deployment.spec.strategy.PSObject.Properties['rollingUpdate']) {
        throw 'player-locator placement writer 必须使用 exact Recreate strategy。'
    }
    $main = @($Deployment.spec.template.spec.containers | Where-Object { [string]$_.name -ceq 'player-locator' })
    $init = @($Deployment.spec.template.spec.initContainers)
    if ($main.Count -ne 1 -or $init.Count -ne 1 -or [string]$init[0].name -cne 'placement-preflight') {
        throw 'player-locator 必须有唯一主容器和唯一 placement-preflight initContainer。'
    }
    $mainImage = [string]$main[0].image
    if ([string]::IsNullOrWhiteSpace($ExpectedPinnedImage)) { $ExpectedPinnedImage = $mainImage }
    if ($ExpectedPinnedImage -cnotmatch '@sha256:[0-9a-f]{64}$' -or $mainImage -cne $ExpectedPinnedImage -or
        [string]$init[0].image -cne $ExpectedPinnedImage) {
        throw 'player-locator 主容器与 placement-preflight 必须使用同一 immutable digest。'
    }
    $commands = @()
    if ($null -ne $init[0].PSObject.Properties['command']) { $commands = @($init[0].command) }
    if ([string]$init[0].imagePullPolicy -cne 'IfNotPresent' -or
        (@($init[0].args) -join ' ') -cne
            '-conf etc/cluster.yaml -placement-preflight -placement-preflight-timeout=10m -placement-preflight-scan-count=1000' -or
        $commands.Count -ne 0) {
        throw 'player-locator placement-preflight image policy/args/ENTRYPOINT 非 canonical。'
    }
    $mounts = @($init[0].volumeMounts)
    if ($mounts.Count -ne 1 -or [string]$mounts[0].name -cne 'conf' -or
        [string]$mounts[0].mountPath -cne '/app/etc/cluster.yaml' -or
        [string]$mounts[0].subPath -cne 'player-locator.yaml' -or $mounts[0].readOnly -ne $true) {
        throw 'player-locator placement-preflight 配置挂载非 canonical read-only contract。'
    }
    $confVolumes = @($Deployment.spec.template.spec.volumes | Where-Object {
        [string]$_.name -ceq 'conf' -and [string]$_.secret.secretName -ceq 'pandora-config'
    })
    if ($confVolumes.Count -ne 1) {
        throw 'player-locator placement-preflight 配置源必须是唯一 pandora-config Secret。'
    }
}

# Construct the explicit, one-shot Model-B legacy pod_uid release gate from the
# exact dormant green ds-allocator image. This is a Job, never an initContainer:
# the same additive binary must first be rolled out as the epoch=1/blue writer
# in an earlier release phase so it can backfill exact legacy GameServer
# identities. This Job only audits; it cannot repair records whose exact K8s
# objects are already gone. Activation then runs prepare + post-drain proofs.
function Assert-PandoraPodUIDReleaseConfigEvidence($ConfigEvidence,
    [ValidateSet('prepare', 'drained', 'final')][string]$Phase) {
    if ($null -eq $ConfigEvidence -or
        [string]$ConfigEvidence.SourceSecretName -cne 'pandora-config' -or
        [string]$ConfigEvidence.SourceSecretUID -cnotmatch '^[0-9a-f][0-9a-f-]{7,127}$' -or
        [string]$ConfigEvidence.SourceSecretResourceVersion -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$' -or
        [string]$ConfigEvidence.RawConfigSHA256 -cnotmatch '^sha256:[0-9a-f]{64}$' -or
        [string]$ConfigEvidence.RawSnapshotName -cnotmatch '^pandora-dsa-cfg-v2-[a-z0-9][a-z0-9-]{7,23}-[0-9a-f]{12}$' -or
        [string]$ConfigEvidence.RawSnapshotUID -cnotmatch '^[0-9a-f][0-9a-f-]{7,127}$' -or
        [string]$ConfigEvidence.RawSnapshotResourceVersion -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$' -or
        [string]$ConfigEvidence.RawSnapshotSHA256 -cne [string]$ConfigEvidence.RawConfigSHA256 -or
        [string]$ConfigEvidence.ROSecretName -cnotmatch '^pandora-pod-uid-preflight-redis-ro-r[1-9][0-9]*$' -or
        [string]$ConfigEvidence.ROSecretUID -cnotmatch '^[0-9a-f][0-9a-f-]{7,127}$' -or
        [string]$ConfigEvidence.ROSecretResourceVersion -cnotmatch '^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$' -or
        [string]$ConfigEvidence.ROConfigSHA256 -cnotmatch '^sha256:[0-9a-f]{64}$' -or
        [string]$ConfigEvidence.RedisConfigIdentity -cnotmatch '^sha256:[0-9a-f]{64}$' -or
        [string]$ConfigEvidence.RedisConfigTopology -cnotin @('standalone', 'sentinel', 'cluster') -or
        [string]$ConfigEvidence.HelperSourceSHA256 -cnotmatch '^sha256:[0-9a-f]{64}$' -or
        ($Phase -ceq 'prepare' -and [string]$ConfigEvidence.RedisTargetIdentity -cne 'pending-prepare') -or
        ($Phase -cne 'prepare' -and [string]$ConfigEvidence.RedisTargetIdentity -cnotmatch '^sha256:[0-9a-f]{64}$')) {
        throw 'pod_uid preflight config evidence 必须绑定 source/raw snapshot/RO Secret UID+resourceVersion+raw SHA256 与阶段 Redis identity。'
    }
}

function New-PandoraPodUIDReleasePreflightJobObject($GreenDeployment, [string]$ActivationRunId,
    [ValidateSet('prepare', 'drained', 'final')][string]$Phase, [uint32]$TargetEpoch = 2,
    $ConfigEvidence) {
    Assert-PandoraPodUIDReleaseConfigEvidence $ConfigEvidence $Phase
    if ($ActivationRunId -cnotmatch '^[a-z0-9][a-z0-9-]{7,23}$') {
        throw 'pod_uid preflight ActivationRunId 必须为 8..24 位 canonical 小写身份。'
    }
    if ($TargetEpoch -ne 2 -or [string]$GreenDeployment.apiVersion -cne 'apps/v1' -or
        [string]$GreenDeployment.kind -cne 'Deployment' -or
        [string]$GreenDeployment.metadata.name -cne 'ds-allocator-ds-auth-green' -or
        [string]$GreenDeployment.metadata.namespace -cne 'pandora') {
        throw 'pod_uid preflight 必须从 exact epoch=2 ds-allocator green Deployment 构造。'
    }
    $main = @($GreenDeployment.spec.template.spec.containers | Where-Object {
        [string]$_.name -ceq 'ds-allocator'
    })
    if ($main.Count -ne 1) { throw 'pod_uid preflight green template 缺唯一 ds-allocator container。' }
    $image = [string]$main[0].image
    $digest = [string]$GreenDeployment.spec.template.metadata.annotations.'pandora.dev/image-digest'
    if ($digest -cnotmatch '^sha256:[0-9a-f]{64}$' -or
        $image -cnotmatch ('@' + [regex]::Escape($digest) + '$')) {
        throw 'pod_uid preflight image 必须绑定 green template immutable digest。'
    }
    $confVolumes = @($GreenDeployment.spec.template.spec.volumes | Where-Object {
        [string]$_.name -ceq 'conf' -and [string]$_.secret.secretName -cin @(
            'pandora-config', [string]$ConfigEvidence.RawSnapshotName)
    })
    $confMounts = @($main[0].volumeMounts | Where-Object {
        [string]$_.name -ceq 'conf' -and [string]$_.mountPath -ceq '/app/etc/cluster.yaml' -and
        [string]$_.subPath -ceq 'ds-allocator.yaml' -and $_.readOnly -eq $true
    })
    if ($confVolumes.Count -ne 1 -or $confMounts.Count -ne 1) {
        throw 'pod_uid preflight 缺 canonical read-only ds-allocator 配置源。'
    }
    if ($Phase -cne 'prepare' -and
        [string]$confVolumes[0].secret.secretName -cne [string]$ConfigEvidence.RawSnapshotName) {
        throw 'pod_uid drained/final preflight 要求 green 已绑定 immutable raw config snapshot。'
    }
    if ([string]$GreenDeployment.spec.template.spec.serviceAccountName -cne 'pandora-allocator') {
        throw 'pod_uid preflight 必须沿用受审 pandora-allocator 网络身份。'
    }
    if ($null -eq $GreenDeployment.spec.template.spec.PSObject.Properties['securityContext']) {
        throw 'pod_uid preflight green template 缺 Pod securityContext。'
    }
    $podSecurityContext = (($GreenDeployment.spec.template.spec.securityContext |
        ConvertTo-Json -Depth 20) | ConvertFrom-Json)
    $imagePullSecrets = @()
    if ($null -ne $GreenDeployment.spec.template.spec.PSObject.Properties['imagePullSecrets']) {
        $imagePullSecrets = @(($GreenDeployment.spec.template.spec.imagePullSecrets |
            ConvertTo-Json -Depth 20) | ConvertFrom-Json)
    }
    $podSpec = [ordered]@{
        serviceAccountName = 'pandora-allocator'
        automountServiceAccountToken = $false
        enableServiceLinks = $false
        restartPolicy = 'Never'
        securityContext = $podSecurityContext
        containers = @([ordered]@{
            name = 'pod-uid-release-preflight'
            image = $image
            imagePullPolicy = 'IfNotPresent'
            args = @($script:PandoraPodUIDReleasePreflightBaseArgs) + @(
                "-pod-uid-release-preflight-run-id=$ActivationRunId"
                "-pod-uid-release-preflight-phase=$Phase"
                "-pod-uid-release-preflight-image-digest=$digest"
            ) + $(if ($Phase -cne 'prepare') {
                @("-pod-uid-release-preflight-expected-target-identity=$($ConfigEvidence.RedisTargetIdentity)")
            } else { @() })
            env = @(
                [ordered]@{
                    name = 'PANDORA_POD_UID_PREFLIGHT_REDIS_USERNAME'
                    valueFrom = [ordered]@{ secretKeyRef = [ordered]@{
                        name = [string]$ConfigEvidence.ROSecretName; key = 'username'; optional = $false
                    } }
                }
                [ordered]@{
                    name = 'PANDORA_POD_UID_PREFLIGHT_REDIS_PASSWORD'
                    valueFrom = [ordered]@{ secretKeyRef = [ordered]@{
                        name = [string]$ConfigEvidence.ROSecretName; key = 'password'; optional = $false
                    } }
                }
            )
            volumeMounts = @([ordered]@{
                name = 'conf'; mountPath = '/app/etc/pod-uid-preflight.yaml'
                subPath = 'ds-allocator-preflight.yaml'; readOnly = $true
            })
            resources = [ordered]@{
                requests = [ordered]@{ cpu = '25m'; memory = '32Mi' }
                limits = [ordered]@{ cpu = '1'; memory = '256Mi' }
            }
            securityContext = [ordered]@{
                allowPrivilegeEscalation = $false
                readOnlyRootFilesystem = $true
                runAsNonRoot = $true
                capabilities = [ordered]@{ drop = @('ALL') }
            }
        })
        volumes = @([ordered]@{
            name = 'conf'; secret = [ordered]@{
                secretName = [string]$ConfigEvidence.ROSecretName; optional = $false; defaultMode = 288
                items = @([ordered]@{ key = 'ds-allocator-preflight.yaml'; path = 'ds-allocator-preflight.yaml' })
            }
        })
    }
    if ($imagePullSecrets.Count -gt 0) { $podSpec.imagePullSecrets = $imagePullSecrets }
    $job = [pscustomobject][ordered]@{
        apiVersion = 'batch/v1'
        kind = 'Job'
        metadata = [ordered]@{
            name = "pandora-pod-uid-preflight-v$TargetEpoch-$ActivationRunId-$Phase"
            namespace = 'pandora'
            labels = [ordered]@{
                'pandora.dev/ds-auth-activation' = [string]$TargetEpoch
                'pandora.dev/preflight' = 'pod-uid-release'
                'pandora.dev/preflight-phase' = $Phase
            }
            annotations = [ordered]@{
                'pandora.dev/image-digest' = $digest
                'pandora.dev/activation-run-id' = $ActivationRunId
                'pandora.dev/preflight-phase' = $Phase
                'pandora.dev/source-config-secret-uid' = [string]$ConfigEvidence.SourceSecretUID
                'pandora.dev/source-config-resource-version' = [string]$ConfigEvidence.SourceSecretResourceVersion
                'pandora.dev/source-ds-allocator-sha256' = [string]$ConfigEvidence.RawConfigSHA256
                'pandora.dev/raw-snapshot-name' = [string]$ConfigEvidence.RawSnapshotName
                'pandora.dev/raw-snapshot-uid' = [string]$ConfigEvidence.RawSnapshotUID
                'pandora.dev/raw-snapshot-resource-version' = [string]$ConfigEvidence.RawSnapshotResourceVersion
                'pandora.dev/ro-secret-name' = [string]$ConfigEvidence.ROSecretName
                'pandora.dev/ro-secret-uid' = [string]$ConfigEvidence.ROSecretUID
                'pandora.dev/ro-secret-resource-version' = [string]$ConfigEvidence.ROSecretResourceVersion
                'pandora.dev/ro-config-sha256' = [string]$ConfigEvidence.ROConfigSHA256
                'pandora.dev/redis-config-identity' = [string]$ConfigEvidence.RedisConfigIdentity
                'pandora.dev/redis-config-topology' = [string]$ConfigEvidence.RedisConfigTopology
                'pandora.dev/config-helper-source-sha256' = [string]$ConfigEvidence.HelperSourceSHA256
                'pandora.dev/redis-target-identity' = [string]$ConfigEvidence.RedisTargetIdentity
            }
        }
        spec = [ordered]@{
            backoffLimit = 0
            completions = 1
            parallelism = 1
            activeDeadlineSeconds = 660
            template = [ordered]@{
                metadata = [ordered]@{
                    labels = [ordered]@{
                        app = 'pandora-pod-uid-release-preflight'
                        'pandora.dev/ds-auth-activation' = [string]$TargetEpoch
                    }
                    annotations = [ordered]@{
                        'sidecar.istio.io/inject' = 'false'
                        'pandora.dev/raw-snapshot-uid' = [string]$ConfigEvidence.RawSnapshotUID
                        'pandora.dev/ro-secret-uid' = [string]$ConfigEvidence.ROSecretUID
                        'pandora.dev/redis-target-identity' = [string]$ConfigEvidence.RedisTargetIdentity
                    }
                }
                spec = $podSpec
            }
        }
    }
    Assert-PandoraPodUIDReleasePreflightJobContract $job $image $ActivationRunId $Phase $TargetEpoch $ConfigEvidence
    return $job
}

function Assert-PandoraPodUIDReleasePreflightJobContract($Job, [string]$ExpectedPinnedImage,
    [string]$ActivationRunId, [ValidateSet('prepare', 'drained', 'final')][string]$Phase,
    [uint32]$TargetEpoch = 2, $ConfigEvidence) {
    Assert-PandoraPodUIDReleaseConfigEvidence $ConfigEvidence $Phase
    $expectedName = "pandora-pod-uid-preflight-v$TargetEpoch-$ActivationRunId-$Phase"
    if ([string]$Job.apiVersion -cne 'batch/v1' -or [string]$Job.kind -cne 'Job' -or
        [string]$Job.metadata.name -cne $expectedName -or [string]$Job.metadata.namespace -cne 'pandora' -or
        [string]$Job.metadata.labels.'pandora.dev/ds-auth-activation' -cne [string]$TargetEpoch -or
        [string]$Job.metadata.labels.'pandora.dev/preflight' -cne 'pod-uid-release' -or
        [string]$Job.metadata.labels.'pandora.dev/preflight-phase' -cne $Phase -or
        [string]$Job.metadata.annotations.'pandora.dev/activation-run-id' -cne $ActivationRunId -or
        [string]$Job.metadata.annotations.'pandora.dev/preflight-phase' -cne $Phase -or
        [string]$Job.metadata.annotations.'pandora.dev/source-config-secret-uid' -cne [string]$ConfigEvidence.SourceSecretUID -or
        [string]$Job.metadata.annotations.'pandora.dev/source-config-resource-version' -cne [string]$ConfigEvidence.SourceSecretResourceVersion -or
        [string]$Job.metadata.annotations.'pandora.dev/source-ds-allocator-sha256' -cne [string]$ConfigEvidence.RawConfigSHA256 -or
        [string]$Job.metadata.annotations.'pandora.dev/raw-snapshot-name' -cne [string]$ConfigEvidence.RawSnapshotName -or
        [string]$Job.metadata.annotations.'pandora.dev/raw-snapshot-uid' -cne [string]$ConfigEvidence.RawSnapshotUID -or
        [string]$Job.metadata.annotations.'pandora.dev/raw-snapshot-resource-version' -cne [string]$ConfigEvidence.RawSnapshotResourceVersion -or
        [string]$Job.metadata.annotations.'pandora.dev/ro-secret-name' -cne [string]$ConfigEvidence.ROSecretName -or
        [string]$Job.metadata.annotations.'pandora.dev/ro-secret-uid' -cne [string]$ConfigEvidence.ROSecretUID -or
        [string]$Job.metadata.annotations.'pandora.dev/ro-secret-resource-version' -cne [string]$ConfigEvidence.ROSecretResourceVersion -or
        [string]$Job.metadata.annotations.'pandora.dev/ro-config-sha256' -cne [string]$ConfigEvidence.ROConfigSHA256 -or
        [string]$Job.metadata.annotations.'pandora.dev/redis-config-identity' -cne [string]$ConfigEvidence.RedisConfigIdentity -or
        [string]$Job.metadata.annotations.'pandora.dev/redis-config-topology' -cne [string]$ConfigEvidence.RedisConfigTopology -or
        [string]$Job.metadata.annotations.'pandora.dev/config-helper-source-sha256' -cne [string]$ConfigEvidence.HelperSourceSHA256 -or
        [string]$Job.metadata.annotations.'pandora.dev/redis-target-identity' -cne [string]$ConfigEvidence.RedisTargetIdentity -or
        [string]$Job.spec.template.metadata.annotations.'pandora.dev/raw-snapshot-uid' -cne [string]$ConfigEvidence.RawSnapshotUID -or
        [string]$Job.spec.template.metadata.annotations.'pandora.dev/ro-secret-uid' -cne [string]$ConfigEvidence.ROSecretUID -or
        [string]$Job.spec.template.metadata.annotations.'pandora.dev/redis-target-identity' -cne [string]$ConfigEvidence.RedisTargetIdentity) {
        throw 'pod_uid preflight Job 身份/activation binding 非 canonical。'
    }
    $digest = [string]$Job.metadata.annotations.'pandora.dev/image-digest'
    if ($digest -cnotmatch '^sha256:[0-9a-f]{64}$' -or
        $ExpectedPinnedImage -cnotmatch ('@' + [regex]::Escape($digest) + '$')) {
        throw 'pod_uid preflight Job 未固定到受审 green digest。'
    }
    if ([int]$Job.spec.backoffLimit -ne 0 -or [int]$Job.spec.completions -ne 1 -or
        [int]$Job.spec.parallelism -ne 1 -or [int]$Job.spec.activeDeadlineSeconds -ne 660) {
        throw 'pod_uid preflight Job 重试/并发/deadline 非 canonical。'
    }
    $jobUID = ''
    if ($null -ne $Job.metadata.PSObject.Properties['uid']) { $jobUID = [string]$Job.metadata.uid }
    if ([string]::IsNullOrWhiteSpace($jobUID)) {
        # Create request 必须让 apiserver 生成 selector；客户端预置即可逃逸
        # controller UID 绑定。
        foreach ($forbidden in @('manualSelector', 'selector', 'suspend')) {
            if ($null -ne $Job.spec.PSObject.Properties[$forbidden]) {
                throw "pod_uid preflight Job create 请求禁止声明 spec.$forbidden。"
            }
        }
    } else {
        # batch/v1 apiserver 在存储后会生成 selector，并可能显式回读
        # manualSelector=false/suspend=false。回读门只接受精确绑定本 Job UID
        # 的 controller selector，不能继续要求字段 absent，否则真实 K8s 永远失败。
        if ($jobUID -cnotmatch '^[0-9a-f][0-9a-f-]{7,127}$' -or
            ($null -ne $Job.spec.PSObject.Properties['manualSelector'] -and
                [bool]$Job.spec.manualSelector) -or
            ($null -ne $Job.spec.PSObject.Properties['suspend'] -and [bool]$Job.spec.suspend) -or
            $null -eq $Job.spec.PSObject.Properties['selector']) {
            throw 'pod_uid preflight stored Job UID/manualSelector/suspend/selector 非 canonical。'
        }
        $selector = $Job.spec.selector
        $matchExpressions = @()
        if ($null -ne $selector.PSObject.Properties['matchExpressions']) {
            $matchExpressions = @($selector.matchExpressions)
        }
        $matchLabels = @()
        if ($null -ne $selector.PSObject.Properties['matchLabels']) {
            $matchLabels = @($selector.matchLabels.PSObject.Properties)
        }
        $allowedControllerLabels = @('batch.kubernetes.io/controller-uid', 'controller-uid')
        if ($matchExpressions.Count -ne 0 -or $matchLabels.Count -lt 1 -or $matchLabels.Count -gt 2) {
            throw 'pod_uid preflight stored Job selector 不是 exact controller UID matchLabels。'
        }
        foreach ($label in $matchLabels) {
            if ([string]$label.Name -cnotin $allowedControllerLabels -or
                [string]$label.Value -cne $jobUID) {
                throw 'pod_uid preflight stored Job selector 未绑定 exact Job UID。'
            }
        }
    }
    $podSpec = $Job.spec.template.spec
    if ([string]$podSpec.restartPolicy -cne 'Never' -or
        [string]$podSpec.serviceAccountName -cne 'pandora-allocator' -or
        $podSpec.automountServiceAccountToken -ne $false -or
        $podSpec.enableServiceLinks -ne $false -or
        [string]$Job.spec.template.metadata.annotations.'sidecar.istio.io/inject' -cne 'false') {
        throw 'pod_uid preflight Job Pod 生命周期/身份/sidecar contract 非 canonical。'
    }
    $podSecurity = $podSpec.securityContext
    if ($null -eq $podSecurity -or $podSecurity.runAsNonRoot -ne $true -or
        [int64]$podSecurity.runAsUser -ne 10001 -or [int64]$podSecurity.runAsGroup -ne 10001 -or
        [int64]$podSecurity.fsGroup -ne 10001 -or
        ($null -ne $podSecurity.PSObject.Properties['fsGroupChangePolicy'] -and
            [string]$podSecurity.fsGroupChangePolicy -cne 'OnRootMismatch')) {
        throw 'pod_uid preflight Job Pod securityContext 未固定 non-root uid/gid/fsGroup=10001。'
    }
    foreach ($hostEscape in @('hostNetwork', 'hostPID', 'hostIPC', 'shareProcessNamespace')) {
        if ($null -ne $podSpec.PSObject.Properties[$hostEscape] -and [bool]$podSpec.$hostEscape) {
            throw "pod_uid preflight Job 禁止 $hostEscape=true。"
        }
    }
    foreach ($forbiddenContainers in @('initContainers', 'ephemeralContainers')) {
        if ($null -ne $podSpec.PSObject.Properties[$forbiddenContainers] -and
            @($podSpec.$forbiddenContainers).Count -ne 0) {
            throw "pod_uid release preflight 禁止 $forbiddenContainers。"
        }
    }
    $containers = @($podSpec.containers)
    $expectedArgs = @($script:PandoraPodUIDReleasePreflightBaseArgs) + @(
        "-pod-uid-release-preflight-run-id=$ActivationRunId"
        "-pod-uid-release-preflight-phase=$Phase"
        "-pod-uid-release-preflight-image-digest=$digest"
    ) + $(if ($Phase -cne 'prepare') {
        @("-pod-uid-release-preflight-expected-target-identity=$($ConfigEvidence.RedisTargetIdentity)")
    } else { @() })
    if ($containers.Count -ne 1 -or [string]$containers[0].name -cne 'pod-uid-release-preflight' -or
        [string]$containers[0].image -cne $ExpectedPinnedImage -or
        [string]$containers[0].imagePullPolicy -cne 'IfNotPresent' -or
        (@($containers[0].args) -join ' ') -cne (@($expectedArgs) -join ' ')) {
        throw 'pod_uid preflight Job image/args 非 canonical。'
    }
    $containerSecurity = $containers[0].securityContext
    if ($containerSecurity.allowPrivilegeEscalation -ne $false -or
        $containerSecurity.readOnlyRootFilesystem -ne $true -or
        $containerSecurity.runAsNonRoot -ne $true -or
        ($null -ne $containerSecurity.PSObject.Properties['privileged'] -and
            [bool]$containerSecurity.privileged) -or
        ($null -ne $containerSecurity.capabilities.PSObject.Properties['add'] -and
            @($containerSecurity.capabilities.add).Count -ne 0) -or
        (@($containerSecurity.capabilities.drop) -join ',') -cne 'ALL') {
        throw 'pod_uid preflight Job container securityContext 非 canonical。'
    }
    if ($null -ne $containers[0].PSObject.Properties['envFrom'] -and @($containers[0].envFrom).Count -ne 0) {
        throw 'pod_uid preflight Job 禁止 envFrom。'
    }
    $env = @($containers[0].env)
    $expectedEnv = @{
        'PANDORA_POD_UID_PREFLIGHT_REDIS_USERNAME' = 'username'
        'PANDORA_POD_UID_PREFLIGHT_REDIS_PASSWORD' = 'password'
    }
    if ($env.Count -ne 2) { throw 'pod_uid preflight Job 必须且只能注入 RO Redis username/password。' }
    foreach ($entry in $env) {
        $key = [string]$entry.name
        if (-not $expectedEnv.ContainsKey($key) -or $null -ne $entry.PSObject.Properties['value'] -or
            [string]$entry.valueFrom.secretKeyRef.name -cne [string]$ConfigEvidence.ROSecretName -or
            [string]$entry.valueFrom.secretKeyRef.key -cne [string]$expectedEnv[$key] -or
            $entry.valueFrom.secretKeyRef.optional -ne $false) {
            throw 'pod_uid preflight Job Redis credential env 非 exact RO Secret keyRef。'
        }
    }
    if ($null -ne $containers[0].PSObject.Properties['command'] -and
        @($containers[0].command).Count -ne 0) {
        throw 'pod_uid preflight Job 禁止覆盖 serving image ENTRYPOINT。'
    }
    $mounts = @($containers[0].volumeMounts)
    $volumes = @($podSpec.volumes)
    if ($mounts.Count -ne 1 -or [string]$mounts[0].name -cne 'conf' -or
        [string]$mounts[0].mountPath -cne '/app/etc/pod-uid-preflight.yaml' -or
        [string]$mounts[0].subPath -cne 'ds-allocator-preflight.yaml' -or $mounts[0].readOnly -ne $true -or
        $volumes.Count -ne 1 -or [string]$volumes[0].name -cne 'conf' -or
        [string]$volumes[0].secret.secretName -cne [string]$ConfigEvidence.ROSecretName -or
        $volumes[0].secret.optional -ne $false -or [int]$volumes[0].secret.defaultMode -ne 288 -or
        @($volumes[0].secret.items).Count -ne 1 -or
        [string]$volumes[0].secret.items[0].key -cne 'ds-allocator-preflight.yaml' -or
        [string]$volumes[0].secret.items[0].path -cne 'ds-allocator-preflight.yaml') {
        throw 'pod_uid preflight Job config mount/source 非 canonical read-only contract。'
    }
}

function Assert-PandoraPodUIDReleasePreflightRuntimeContract($Job, $Pod,
    [string]$ExpectedPinnedImage, [string]$ActivationRunId,
    [ValidateSet('prepare', 'drained', 'final')][string]$Phase, [uint32]$TargetEpoch,
    $ConfigEvidence, [datetimeoffset]$NotBefore = [datetimeoffset]::MinValue,
    [datetimeoffset]$NotAfter = [datetimeoffset]::MaxValue) {
    Assert-PandoraPodUIDReleasePreflightJobContract $Job $ExpectedPinnedImage `
        $ActivationRunId $Phase $TargetEpoch $ConfigEvidence
    if (Test-PandoraKubernetesObjectDeleting $Job) {
        throw 'pod_uid preflight Job 正在删除，不能作为不可变证据。'
    }
    $jobUID = [string]$Job.metadata.uid
    $podUID = [string]$Pod.metadata.uid
    if ($jobUID -cnotmatch '^[0-9a-f][0-9a-f-]{7,127}$' -or
        $podUID -cnotmatch '^[0-9a-f][0-9a-f-]{7,127}$' -or
        [string]$Pod.metadata.namespace -cne 'pandora' -or
        (Test-PandoraKubernetesObjectDeleting $Pod)) {
        throw 'pod_uid preflight Job/Pod UID、namespace 或删除状态非法。'
    }
    $owners = @($Pod.metadata.ownerReferences)
    if ($owners.Count -ne 1 -or [string]$owners[0].apiVersion -cne 'batch/v1' -or
        [string]$owners[0].kind -cne 'Job' -or [string]$owners[0].name -cne [string]$Job.metadata.name -or
        [string]$owners[0].uid -cne $jobUID -or $owners[0].controller -ne $true) {
        throw 'pod_uid preflight Pod 未由 exact Job UID 以 controller=true 唯一拥有。'
    }

    # Re-run the canonical Job Pod contract over the actual admitted Pod spec.
    # Kubernetes may add unrelated defaulted Pod fields, but every executable,
    # credential, image, argument, mount and security field remains exact.
    $podCarrier = (($Job | ConvertTo-Json -Depth 40) | ConvertFrom-Json)
    $podCarrier.spec.template.spec = $Pod.spec
    $podCarrier.spec.template.metadata.annotations = $Pod.metadata.annotations
    Assert-PandoraPodUIDReleasePreflightJobContract $podCarrier $ExpectedPinnedImage `
        $ActivationRunId $Phase $TargetEpoch $ConfigEvidence

    $conditions = @($Job.status.conditions)
    $complete = @($conditions | Where-Object { [string]$_.type -ceq 'Complete' -and [string]$_.status -ceq 'True' })
    $failed = @($conditions | Where-Object { [string]$_.type -ceq 'Failed' -and [string]$_.status -ceq 'True' })
    $active = if ($null -eq $Job.status.PSObject.Properties['active']) { 0 } else { [int]$Job.status.active }
    $failedCount = if ($null -eq $Job.status.PSObject.Properties['failed']) { 0 } else { [int]$Job.status.failed }
    if ($complete.Count -ne 1 -or $failed.Count -ne 0 -or [int]$Job.status.succeeded -ne 1 -or
        $active -ne 0 -or $failedCount -ne 0 -or [string]$Pod.status.phase -cne 'Succeeded') {
        throw 'pod_uid preflight Job/Pod 未以唯一 Succeeded/Complete 状态终止。'
    }
    $statuses = @($Pod.status.containerStatuses)
    if ($statuses.Count -ne 1 -or [string]$statuses[0].name -cne 'pod-uid-release-preflight' -or
        [string]$statuses[0].image -cne $ExpectedPinnedImage -or [int]$statuses[0].restartCount -ne 0) {
        throw 'pod_uid preflight Pod containerStatuses 数量/image/restart 非 canonical。'
    }
    $digest = [string]$Job.metadata.annotations.'pandora.dev/image-digest'
    if ([string]$statuses[0].imageID -cnotmatch ([regex]::Escape($digest) + '$')) {
        throw 'pod_uid preflight Pod imageID 未绑定受审 immutable digest。'
    }
    $terminated = $statuses[0].state.terminated
    if ($null -eq $terminated -or [int]$terminated.exitCode -ne 0 -or
        [string]$terminated.reason -cne 'Completed') {
        throw 'pod_uid preflight 唯一容器未以 exit=0/reason=Completed 终止。'
    }
    $jobCreated = [datetimeoffset]::MinValue
    $podCreated = [datetimeoffset]::MinValue
    $started = [datetimeoffset]::MinValue
    $finished = [datetimeoffset]::MinValue
    $completed = [datetimeoffset]::MinValue
    if (-not [datetimeoffset]::TryParse([string]$Job.metadata.creationTimestamp, [ref]$jobCreated) -or
        -not [datetimeoffset]::TryParse([string]$Pod.metadata.creationTimestamp, [ref]$podCreated) -or
        -not [datetimeoffset]::TryParse([string]$terminated.startedAt, [ref]$started) -or
        -not [datetimeoffset]::TryParse([string]$terminated.finishedAt, [ref]$finished) -or
        -not [datetimeoffset]::TryParse([string]$Job.status.completionTime, [ref]$completed) -or
        $jobCreated -lt $NotBefore -or $jobCreated -gt $podCreated -or
        $podCreated -gt $started -or $started -gt $finished -or $finished -gt $completed -or
        $completed -gt $NotAfter) {
        throw 'pod_uid preflight Job/Pod/container/completion 时间链非法或越过 activation 窗口。'
    }
    return [pscustomobject][ordered]@{
        JobName = [string]$Job.metadata.name
        JobUID = $jobUID
        PodName = [string]$Pod.metadata.name
        PodUID = $podUID
        CompletionTime = $completed.ToUniversalTime().ToString('o')
        CompletionTimeUnixMS = $completed.ToUnixTimeMilliseconds()
        ImageDigest = $digest
        ImageID = [string]$statuses[0].imageID
    }
}

function Assert-PandoraDsAuthIdentitySecretContract($Secret, [string]$App, [string]$Revision) {
    $expectedName = Get-PandoraDsAuthIdentitySecretName $App $Revision
    $expectedIdentity = Get-PandoraDsAuthClientIdentity $App
    if ([string]$Secret.metadata.name -cne $expectedName) { throw "etcd identity Secret 名称不是 $expectedName。" }
    if ($Secret.immutable -ne $true) { throw "Secret/$expectedName 必须 immutable=true。" }
    if ([string]$Secret.metadata.labels.'pandora.dev/ds-auth-etcd-identity-revision' -cne $Revision) {
        throw "Secret/$expectedName 缺精确 identity revision label。"
    }
    if ([string]$Secret.metadata.labels.'pandora.dev/ds-auth-etcd-client-identity' -cne $expectedIdentity) {
        throw "Secret/$expectedName 缺精确 client identity label。"
    }
    $keys = @($Secret.data.PSObject.Properties.Name | Sort-Object)
    $required = @('ca.crt', 'tls.crt', 'tls.key')
    foreach ($key in $required) {
        if ($keys -cnotcontains $key -or [string]::IsNullOrWhiteSpace([string]$Secret.data.$key)) {
            throw "Secret/$expectedName 缺非空 key=$key。"
        }
    }
    $hasUsername = $keys -ccontains 'username'
    $hasPassword = $keys -ccontains 'password'
    if ($hasUsername -ne $hasPassword) { throw "Secret/$expectedName username/password 必须同时存在或同时缺失。" }
    $allowed = @($required + @('username', 'password'))
    foreach ($key in $keys) {
        if ($allowed -cnotcontains $key) { throw "Secret/$expectedName 含未批准 key=$key。" }
    }
    return [pscustomobject]@{ Name = $expectedName; ClientIdentity = $expectedIdentity; UsesPasswordAuth = $hasUsername }
}

function Get-PandoraNamedObject($Items, [string]$Name, [string]$Kind) {
    $matches = @($Items | Where-Object { [string]$_.name -ceq $Name })
    if ($matches.Count -ne 1) { throw "$Kind name=$Name 必须且只能出现一次，实际=$($matches.Count)。" }
    return $matches[0]
}

function Test-PandoraKubernetesObjectDeleting($Object) {
    if ($null -eq $Object -or $null -eq $Object.metadata -or
        $null -eq $Object.metadata.PSObject.Properties['deletionTimestamp']) {
        return $false
    }
    return -not [string]::IsNullOrWhiteSpace([string]$Object.metadata.deletionTimestamp)
}

function Assert-PandoraDsAuthIdentityPodContract($Pod, [string]$App, [string]$Revision,
    [string]$ServerName, [string]$ForbiddenReadPrefix, [bool]$UsesPasswordAuth) {
    $secretName = Get-PandoraDsAuthIdentitySecretName $App $Revision
    $identity = Get-PandoraDsAuthClientIdentity $App
    if ([string]::IsNullOrWhiteSpace($ServerName)) { throw 'etcd TLS server name 不能为空。' }
    if ([string]::IsNullOrWhiteSpace($ForbiddenReadPrefix)) { throw 'ACL forbidden read prefix 不能为空。' }

    $container = Get-PandoraNamedObject @($Pod.spec.containers) $App 'container'
    $volume = Get-PandoraNamedObject @($Pod.spec.volumes) 'ds-auth-etcd-identity' 'volume'
    if ([string]$volume.secret.secretName -cne $secretName -or [int]$volume.secret.defaultMode -ne 288) {
        throw "$App Pod 未以 defaultMode=0440 挂载精确 Secret/$secretName。"
    }
    $mount = Get-PandoraNamedObject @($container.volumeMounts) 'ds-auth-etcd-identity' 'volumeMount'
    if ([string]$mount.mountPath -cne $script:PandoraDsAuthIdentityMountPath -or $mount.readOnly -ne $true) {
        throw "$App Pod 的 etcd identity volume 必须只读挂载到固定路径。"
    }

    $expected = [ordered]@{
        'PANDORA_DS_AUTH_ETCD_REQUIRE_MTLS'          = '1'
        'PANDORA_DS_AUTH_ETCD_CA_FILE'               = "$script:PandoraDsAuthIdentityMountPath/ca.crt"
        'PANDORA_DS_AUTH_ETCD_CERT_FILE'             = "$script:PandoraDsAuthIdentityMountPath/tls.crt"
        'PANDORA_DS_AUTH_ETCD_KEY_FILE'              = "$script:PandoraDsAuthIdentityMountPath/tls.key"
        'PANDORA_DS_AUTH_ETCD_SERVER_NAME'           = $ServerName
        'PANDORA_DS_AUTH_ETCD_CLIENT_IDENTITY'       = $identity
        'PANDORA_DS_AUTH_ETCD_IDENTITY_REVISION'     = $Revision
        'PANDORA_DS_AUTH_ETCD_REQUIRE_AUTH'           = '1'
        'PANDORA_DS_AUTH_ETCD_FORBIDDEN_READ_PREFIX' = $ForbiddenReadPrefix
    }
    if ($UsesPasswordAuth) {
        $expected['PANDORA_DS_AUTH_ETCD_USERNAME_FILE'] = "$script:PandoraDsAuthIdentityMountPath/username"
        $expected['PANDORA_DS_AUTH_ETCD_PASSWORD_FILE'] = "$script:PandoraDsAuthIdentityMountPath/password"
    }
    foreach ($pair in $expected.GetEnumerator()) {
        $entry = Get-PandoraNamedObject @($container.env) $pair.Key 'env'
        if ([string]$entry.value -cne [string]$pair.Value -or $null -ne $entry.valueFrom) {
            throw "$App Pod env $($pair.Key) 不是固定安全值。"
        }
    }
    foreach ($name in @('PANDORA_DS_AUTH_ETCD_USERNAME_FILE', 'PANDORA_DS_AUTH_ETCD_PASSWORD_FILE')) {
        $entries = @($container.env | Where-Object { [string]$_.name -ceq $name })
        if (-not $UsesPasswordAuth -and $entries.Count -ne 0) { throw "$App Pod 不应声明 $name。" }
    }
}

function Assert-PandoraDsTerminalMeshTemplateContract($Pod, [string]$App) {
    $expectedServiceAccount = switch ($App) {
        'battle-result' { 'pandora-battle-result' }
        'ds-allocator' { 'pandora-allocator' }
        default { throw "terminal mesh 不接受 app=$App。" }
    }
    $labelProperties = @($Pod.metadata.labels.PSObject.Properties)
    $annotationProperties = if ($null -eq $Pod.metadata.annotations) { @() } else { @($Pod.metadata.annotations.PSObject.Properties) }
    $revisionEntries = @($labelProperties | Where-Object Name -CEQ 'istio.io/rev')
    $injectEntries = @($labelProperties + $annotationProperties | Where-Object Name -CEQ 'sidecar.istio.io/inject')
    $rewriteEntries = @($annotationProperties | Where-Object Name -CEQ 'sidecar.istio.io/rewriteAppHTTPProbers')
    if ([string]$Pod.spec.serviceAccountName -cne $expectedServiceAccount -or
        $revisionEntries.Count -ne 1 -or [string]::IsNullOrWhiteSpace([string]$revisionEntries[0].Value) -or
        [string]$revisionEntries[0].Value -ceq 'PANDORA_ISTIO_REVISION_REQUIRED' -or
        $injectEntries.Count -ne 0 -or $rewriteEntries.Count -ne 1 -or
        [string]$rewriteEntries[0].Value -cne 'true') {
        throw "$App Pod 未绑定固定 ServiceAccount/唯一真实 Istio revision/probe rewrite，或含双 injector inject 标记。"
    }
}

function Assert-PandoraDsTerminalMeshPodContract($Pod, [string]$App) {
    Assert-PandoraDsTerminalMeshTemplateContract $Pod $App
    $proxy = Get-PandoraNamedObject @($Pod.status.containerStatuses) 'istio-proxy' 'terminal mesh sidecar'
    if ($proxy.ready -ne $true -or [int]$proxy.restartCount -ne 0) { throw "$App Pod istio-proxy 非稳定 Ready。" }
}

function Assert-PandoraDsTerminalMeshPolicyContract($PeerAuthentication, $AuthorizationPolicy, $Service) {
    if ([string]$PeerAuthentication.metadata.name -cne 'pandora-ds-allocator-terminal-permissive' -or
        [string]$PeerAuthentication.metadata.namespace -cne 'pandora' -or
        [string]$PeerAuthentication.spec.selector.matchLabels.app -cne 'ds-allocator' -or
        [string]$PeerAuthentication.spec.mtls.mode -cne 'PERMISSIVE') {
        throw 'terminal ReleaseBattle PeerAuthentication 实体不匹配。'
    }
    if ([string]$AuthorizationPolicy.metadata.name -cne 'pandora-ds-terminal-release-exact-deny' -or
        [string]$AuthorizationPolicy.metadata.namespace -cne 'pandora' -or
        [string]$AuthorizationPolicy.spec.selector.matchLabels.app -cne 'ds-allocator' -or
        [string]$AuthorizationPolicy.spec.action -cne 'DENY') {
        throw 'terminal ReleaseBattle AuthorizationPolicy 实体不匹配。'
    }
    $rules = @($AuthorizationPolicy.spec.rules)
    if ($rules.Count -ne 2) { throw 'terminal ReleaseBattle DENY 必须恰有两条规则。' }
    $expectedPrincipals = @('*', 'cluster.local/ns/pandora/sa/pandora-battle-result')
    $seen = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
    foreach ($rule in $rules) {
        $from = @($rule.from)
        $to = @($rule.to)
        if ($from.Count -ne 1 -or $to.Count -ne 1 -or @($from[0].source.notPrincipals).Count -ne 1 -or
            @($to[0].operation.methods).Count -ne 1 -or [string]$to[0].operation.methods[0] -cne 'POST' -or
            @($to[0].operation.paths).Count -ne 1 -or [string]$to[0].operation.paths[0] -cne '/pandora.ds.v1.DSAllocatorService/ReleaseBattle') {
            throw 'terminal ReleaseBattle DENY principal/method/path 不是 exact contract。'
        }
        $null = $seen.Add([string]$from[0].source.notPrincipals[0])
    }
    if ($seen.Count -ne 2) { throw 'terminal ReleaseBattle DENY principal 集不完整。' }
    foreach ($principal in $expectedPrincipals) {
        if (-not $seen.Contains($principal)) { throw "terminal ReleaseBattle 缺 DENY principal=$principal。" }
    }
    $ports = @($Service.spec.ports | Where-Object {
        [string]$_.name -ceq 'grpc' -and [string]$_.appProtocol -ceq 'grpc' -and [int]$_.port -eq 20020
    })
    if ([string]$Service.metadata.name -cne 'ds-allocator' -or [string]$Service.metadata.namespace -cne 'pandora' -or $ports.Count -ne 1) {
        throw 'ds-allocator Service 未暴露 exact grpc/appProtocol=grpc/20020。'
    }
}

function Get-PandoraDsAuthSecureGoArgs([string]$CAFile, [string]$CertFile, [string]$KeyFile,
    [string]$ServerName, [string]$ClientIdentity, [string]$IdentityRevision,
    [string]$ForbiddenReadPrefix, [string]$UsernameFile = '', [string]$PasswordFile = '') {
    Assert-PandoraDsAuthEtcdRevision $IdentityRevision
    foreach ($file in @($CAFile, $CertFile, $KeyFile)) {
        if (-not (Test-Path -LiteralPath $file -PathType Leaf)) { throw 'etcd auditor mTLS 文件缺失。' }
    }
    if ([string]::IsNullOrWhiteSpace($ServerName) -or [string]::IsNullOrWhiteSpace($ClientIdentity) -or
        [string]::IsNullOrWhiteSpace($ForbiddenReadPrefix)) { throw 'etcd auditor server-name/client-identity/forbidden-prefix 均必填。' }
    if ([string]::IsNullOrWhiteSpace($UsernameFile) -ne [string]::IsNullOrWhiteSpace($PasswordFile)) {
        throw 'etcd auditor username/password file 必须同时提供或同时缺失。'
    }
    $args = @('--require-mtls', '--ca-file', (Resolve-Path -LiteralPath $CAFile).Path,
        '--cert-file', (Resolve-Path -LiteralPath $CertFile).Path,
        '--key-file', (Resolve-Path -LiteralPath $KeyFile).Path,
        '--server-name', $ServerName, '--client-identity', $ClientIdentity,
        '--etcd-identity-revision', $IdentityRevision, '--require-auth',
        '--forbidden-read-prefix', $ForbiddenReadPrefix)
    if (-not [string]::IsNullOrWhiteSpace($UsernameFile)) {
        foreach ($file in @($UsernameFile, $PasswordFile)) {
            if (-not (Test-Path -LiteralPath $file -PathType Leaf)) { throw 'etcd auditor username/password 文件缺失。' }
        }
        $args += @('--username-file', (Resolve-Path -LiteralPath $UsernameFile).Path,
            '--password-file', (Resolve-Path -LiteralPath $PasswordFile).Path)
    }
    return $args
}

function Assert-PandoraExactStringList($Actual, [string[]]$Expected, [string]$Where) {
    $items = @($Actual)
    if ($items.Count -ne $Expected.Count) { throw "$Where 数量不匹配。" }
    for ($index = 0; $index -lt $Expected.Count; $index++) {
        if ([string]$items[$index] -cne $Expected[$index]) { throw "$Where 顺序或内容不匹配。" }
    }
}

function Get-PandoraDsAuthVerifiedEndpointUIDSet($SliceList, $Service, $ExpectedPods, [string]$Namespace) {
    if ([string]$Service.metadata.namespace -cne $Namespace -or [string]::IsNullOrWhiteSpace([string]$Service.metadata.name) -or
        [string]::IsNullOrWhiteSpace([string]$Service.metadata.uid)) {
        throw 'Service identity 非 canonical，不能验证 EndpointSlice。'
    }
    $serviceName = [string]$Service.metadata.name
    $expectedByUID = @{}
    $expectedAddressPairs = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
    foreach ($pod in @($ExpectedPods)) {
        $uid = [string]$pod.metadata.uid
        $name = [string]$pod.metadata.name
        if ([string]$pod.metadata.namespace -cne $Namespace -or [string]::IsNullOrWhiteSpace($uid) -or
            [string]::IsNullOrWhiteSpace($name) -or $expectedByUID.ContainsKey($uid)) {
            throw "Service/$serviceName expected Pod identity 非 canonical/重复。"
        }
        $deleting = $null
        if ($null -ne $pod.metadata.PSObject.Properties['deletionTimestamp']) { $deleting = $pod.metadata.deletionTimestamp }
        if ($null -ne $deleting) { throw "Service/$serviceName expected Pod 正在删除。" }
        $podIPs = @()
        if ($null -ne $pod.status.PSObject.Properties['podIPs']) {
            $podIPs = @($pod.status.podIPs | ForEach-Object { [string]$_.ip })
        } elseif ($null -ne $pod.status.PSObject.Properties['podIP']) {
            $podIPs = @([string]$pod.status.podIP)
        }
        if ($podIPs.Count -eq 0) { throw "Service/$serviceName expected Pod 缺 podIP。" }
        $expectedByUID[$uid] = [pscustomobject]@{ Name = $name; IPs = $podIPs }
        foreach ($ip in $podIPs) { $null = $expectedAddressPairs.Add("$uid|$ip") }
    }

    $expectedPorts = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
    foreach ($port in @($Service.spec.ports)) {
        $name = ''
        if ($null -ne $port.PSObject.Properties['name']) { $name = [string]$port.name }
        $protocol = if ($null -ne $port.PSObject.Properties['protocol']) { [string]$port.protocol } else { 'TCP' }
        $target = [string]$port.targetPort
        if ($target -cnotmatch '^[1-9][0-9]{0,4}$' -or [int]$target -gt 65535 -or
            -not $expectedPorts.Add("$name|$protocol|$target")) {
            throw "Service/$serviceName port contract 非 canonical/重复。"
        }
    }
    if ($expectedPorts.Count -eq 0) { throw "Service/$serviceName 无可验证端口。" }

    $seenUIDs = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
    $seenAddressPairs = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
    foreach ($slice in @($SliceList.items)) {
        if ([string]$slice.metadata.namespace -cne $Namespace -or
            [string]$slice.metadata.labels.'kubernetes.io/service-name' -cne $serviceName -or
            [string]$slice.metadata.labels.'endpointslice.kubernetes.io/managed-by' -cne 'endpointslice-controller.k8s.io' -or
            [string]$slice.addressType -notin @('IPv4', 'IPv6')) {
            throw "Service/$serviceName EndpointSlice identity/manager/addressType 非 canonical。"
        }
        $owners = @($slice.metadata.ownerReferences | Where-Object {
            [string]$_.kind -ceq 'Service' -and [string]$_.name -ceq $serviceName -and
            [string]$_.uid -ceq [string]$Service.metadata.uid -and $_.controller -eq $true
        })
        if ($owners.Count -ne 1 -or @($slice.metadata.ownerReferences).Count -ne 1) {
            throw "Service/$serviceName EndpointSlice owner 非唯一真实 Service。"
        }
        $slicePorts = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
        foreach ($port in @($slice.ports)) {
            $name = ''
            if ($null -ne $port.PSObject.Properties['name']) { $name = [string]$port.name }
            $protocol = if ($null -ne $port.PSObject.Properties['protocol']) { [string]$port.protocol } else { 'TCP' }
            $value = [string]$port.port
            if (-not $slicePorts.Add("$name|$protocol|$value")) { throw "Service/$serviceName EndpointSlice port 重复。" }
        }
        if ($slicePorts.Count -ne $expectedPorts.Count) { throw "Service/$serviceName EndpointSlice port 数不匹配。" }
        foreach ($portKey in $expectedPorts) {
            if (-not $slicePorts.Contains($portKey)) { throw "Service/$serviceName EndpointSlice port 被篡改。" }
        }
        foreach ($endpoint in @($slice.endpoints)) {
            $serving = $true
            if ($null -ne $endpoint.conditions.PSObject.Properties['serving']) { $serving = [bool]$endpoint.conditions.serving }
            $terminating = $false
            if ($null -ne $endpoint.conditions.PSObject.Properties['terminating']) { $terminating = [bool]$endpoint.conditions.terminating }
            if ($endpoint.conditions.ready -ne $true -or -not $serving -or $terminating -or $null -eq $endpoint.targetRef) {
                throw "Service/$serviceName 含非 Ready/Serving 或无 Pod targetRef 的 Endpoint。"
            }
            $uid = [string]$endpoint.targetRef.uid
            $expectedPod = $expectedByUID[$uid]
            if ($null -eq $expectedPod -or [string]$endpoint.targetRef.kind -cne 'Pod' -or
                [string]$endpoint.targetRef.namespace -cne $Namespace -or
                [string]$endpoint.targetRef.name -cne [string]$expectedPod.Name) {
                throw "Service/$serviceName Endpoint targetRef 不属于 exact green Pod。"
            }
            $addresses = @($endpoint.addresses)
            if ($addresses.Count -eq 0) { throw "Service/$serviceName Endpoint 缺地址。" }
            foreach ($address in $addresses) {
                $pair = "$uid|$([string]$address)"
                if (-not $expectedAddressPairs.Contains($pair) -or -not $seenAddressPairs.Add($pair)) {
                    throw "Service/$serviceName Endpoint 地址不属于 Pod 或重复。"
                }
            }
            $null = $seenUIDs.Add($uid)
        }
    }
    if ($seenUIDs.Count -ne $expectedByUID.Count) { throw "Service/$serviceName Endpoint UID 数不匹配。" }
    foreach ($uid in $expectedByUID.Keys) {
        if (-not $seenUIDs.Contains([string]$uid)) { throw "Service/$serviceName 缺 expected Pod Endpoint。" }
    }
    return ,$seenUIDs
}

function Assert-PandoraDsAuthSyntheticProbeContainer($Container, [string]$Namespace,
    [string]$RunId, [string]$Phase, [string]$Mode, [uint32]$TargetEpoch,
    [string]$KeysetRevision, [string]$EtcdIdentityRevision) {
    Assert-PandoraExactStringList @($Container.command) @('/pandora/bin/dsauth-synthetic-v1') 'synthetic command'
    $expectedArgs = @(
        '--contract=v1', "--namespace=$Namespace", "--run-id=$RunId", "--phase=$Phase", "--mode=$Mode",
        "--target-epoch=$TargetEpoch", "--keyset-revision=$KeysetRevision",
        "--etcd-identity-revision=$EtcdIdentityRevision", '--result-file=/dev/termination-log'
    )
    Assert-PandoraExactStringList @($Container.args) $expectedArgs 'synthetic args'
    if ([string]$Container.terminationMessagePath -cne '/dev/termination-log' -or
        [string]$Container.terminationMessagePolicy -cne 'File') {
        throw 'synthetic termination result 必须来自固定 /dev/termination-log 文件。'
    }
    $expectedEnv = [ordered]@{
        'PANDORA_SYNTHETIC_CONTRACT' = 'v1'
        'PANDORA_ACTIVATION_RUN_ID' = $RunId
        'PANDORA_SYNTHETIC_PHASE' = $Phase
        'PANDORA_SYNTHETIC_MODE' = $Mode
        'PANDORA_TARGET_WRITER_EPOCH' = [string]$TargetEpoch
        'PANDORA_KEYSET_REVISION' = $KeysetRevision
        'PANDORA_ETCD_IDENTITY_REVISION' = $EtcdIdentityRevision
        'PANDORA_NAMESPACE' = $Namespace
    }
    $envItems = @($Container.env)
    if ($envItems.Count -ne $expectedEnv.Count) { throw 'synthetic env 必须是 exact contract 集。' }
    foreach ($pair in $expectedEnv.GetEnumerator()) {
        $matches = @($envItems | Where-Object { [string]$_.name -ceq [string]$pair.Key })
        if ($matches.Count -ne 1 -or [string]$matches[0].value -cne [string]$pair.Value -or
            $null -ne $matches[0].PSObject.Properties['valueFrom']) {
            throw "synthetic env $($pair.Key) 不匹配或不是固定 literal。"
        }
    }
    $mounts = @()
    if ($null -ne $Container.PSObject.Properties['volumeMounts']) { $mounts = @($Container.volumeMounts) }
    if ($mounts.Count -ne 0) { throw 'synthetic probe 不允许任何 volumeMount。' }
    $envFrom = @()
    if ($null -ne $Container.PSObject.Properties['envFrom']) { $envFrom = @($Container.envFrom) }
    if ($envFrom.Count -ne 0) { throw 'synthetic probe 不允许 envFrom 注入。' }
    if ($null -ne $Container.PSObject.Properties['lifecycle']) { throw 'synthetic probe 不允许 lifecycle hook。' }
    $security = $Container.securityContext
    $drop = @($security.capabilities.drop)
    $add = @()
    if ($null -ne $security.capabilities.PSObject.Properties['add']) { $add = @($security.capabilities.add) }
    $privileged = $false
    if ($null -ne $security.PSObject.Properties['privileged']) { $privileged = [bool]$security.privileged }
    if (@($security.PSObject.Properties).Count -ne 4 -or
        @($security.capabilities.PSObject.Properties).Count -ne 1 -or
        $security.allowPrivilegeEscalation -ne $false -or $privileged -or
        $security.readOnlyRootFilesystem -ne $true -or $security.runAsNonRoot -ne $true -or
        $drop.Count -ne 1 -or [string]$drop[0] -cne 'ALL' -or $add.Count -ne 0) {
        throw 'synthetic probe 必须 non-root/read-only/no-escalation/drop ALL。'
    }
}

function Assert-PandoraDsAuthSyntheticPodSpec($Spec, [string]$Where) {
    $containers = @($Spec.containers)
    $initContainers = @()
    if ($null -ne $Spec.PSObject.Properties['initContainers']) { $initContainers = @($Spec.initContainers) }
    $ephemeralContainers = @()
    if ($null -ne $Spec.PSObject.Properties['ephemeralContainers']) { $ephemeralContainers = @($Spec.ephemeralContainers) }
    $volumes = @()
    if ($null -ne $Spec.PSObject.Properties['volumes']) { $volumes = @($Spec.volumes) }
    $hostAliases = @()
    if ($null -ne $Spec.PSObject.Properties['hostAliases']) { $hostAliases = @($Spec.hostAliases) }
    foreach ($flag in @('hostNetwork', 'hostPID', 'hostIPC', 'shareProcessNamespace', 'setHostnameAsFQDN')) {
        if ($null -ne $Spec.PSObject.Properties[$flag] -and [bool]$Spec.$flag) {
            throw "$Where 不允许 $flag。"
        }
    }
    foreach ($field in @('dnsConfig', 'hostname', 'subdomain')) {
        if ($null -ne $Spec.PSObject.Properties[$field]) { throw "$Where 不允许 $field。" }
    }
    if ($containers.Count -ne 1 -or [string]$containers[0].name -cne 'probe' -or
        $initContainers.Count -ne 0 -or $ephemeralContainers.Count -ne 0 -or $volumes.Count -ne 0 -or
        $hostAliases.Count -ne 0 -or [string]$Spec.serviceAccountName -cne 'pandora-ds-auth-synthetic-v1' -or
        [string]$Spec.restartPolicy -cne 'Never' -or [string]$Spec.dnsPolicy -cne 'ClusterFirst' -or
        $Spec.automountServiceAccountToken -ne $false -or $Spec.enableServiceLinks -ne $false -or
        @($Spec.securityContext.PSObject.Properties).Count -ne 2 -or
        $Spec.securityContext.runAsNonRoot -ne $true -or
        [string]$Spec.securityContext.seccompProfile.type -cne 'RuntimeDefault' -or
        @($Spec.securityContext.seccompProfile.PSObject.Properties).Count -ne 1) {
        throw "$Where 必须是无注入容器/卷/host 绕过的 exact isolated Pod spec。"
    }
}

function Assert-PandoraDsAuthSyntheticContract($Job, $Pod, [string]$Namespace,
    [string]$RunId, [ValidateSet('prepare', 'final')][string]$Phase,
    [string]$ImageDigest, [uint32]$TargetEpoch,
    [string]$KeysetRevision, [string]$EtcdIdentityRevision, [timespan]$MaxAge,
    [datetimeoffset]$NotBefore = [datetimeoffset]::MinValue,
    [datetimeoffset]$Now = [datetimeoffset]::UtcNow) {
    if ($RunId -cnotmatch '^[a-z0-9][a-z0-9-]{7,23}$') { throw 'synthetic activation run id 非法。' }
    if ($ImageDigest -cnotmatch '^sha256:[0-9a-f]{64}$') { throw 'synthetic image 必须是 immutable sha256 digest。' }
    Assert-PandoraDsAuthEtcdRevision $EtcdIdentityRevision
    if ($MaxAge -le [timespan]::Zero -or $MaxAge -gt [timespan]::FromHours(1)) { throw 'synthetic MaxAge 必须在 (0,1h]。' }
    $expectedJobName = "pandora-ds-auth-synthetic-v1-$RunId-$Phase"
    if ([string]$Job.metadata.name -cne $expectedJobName -or [string]$Job.metadata.namespace -cne $Namespace) {
        throw "只接受固定 namespace 内 Job/$expectedJobName。"
    }
    $annotations = $Job.metadata.annotations
    $syntheticMode = if ($Phase -ceq 'prepare') { 'isolated-no-writes-v1' } else { 'live-final-v1' }
    $expectedAnnotations = [ordered]@{
        'pandora.dev/ds-auth-synthetic-contract'      = 'v1'
        'pandora.dev/ds-auth-activation-run'          = $RunId
        'pandora.dev/ds-auth-synthetic-phase'         = $Phase
        'pandora.dev/ds-auth-synthetic-mode'          = $syntheticMode
        'pandora.dev/image-digest'                    = $ImageDigest
        'pandora.dev/ds-auth-target-epoch'            = [string]$TargetEpoch
        'pandora.dev/ds-auth-keyset-revision'         = $KeysetRevision
        'pandora.dev/ds-auth-etcd-identity-revision'  = $EtcdIdentityRevision
        'sidecar.istio.io/inject'                    = 'false'
    }
    foreach ($pair in $expectedAnnotations.GetEnumerator()) {
        if ([string]$annotations.($pair.Key) -cne [string]$pair.Value) { throw "synthetic Job annotation $($pair.Key) 不匹配。" }
        if ([string]$Job.spec.template.metadata.annotations.($pair.Key) -cne [string]$pair.Value -or
            [string]$Pod.metadata.annotations.($pair.Key) -cne [string]$pair.Value) {
            throw "synthetic template/Pod annotation $($pair.Key) 不匹配。"
        }
    }
    $expectedAnnotationNames = @($expectedAnnotations.Keys)
    foreach ($source in @(
        [pscustomobject]@{ Value = $Job.metadata.annotations; Where = 'Job'; AllowRuntime = $false },
        [pscustomobject]@{ Value = $Job.spec.template.metadata.annotations; Where = 'Job template'; AllowRuntime = $false },
        [pscustomobject]@{ Value = $Pod.metadata.annotations; Where = 'observed Pod'; AllowRuntime = $true }
    )) {
        foreach ($property in @($source.Value.PSObject.Properties)) {
            if ($expectedAnnotationNames -ccontains [string]$property.Name) { continue }
            $runtimeCNI = @('cni.projectcalico.org/containerID', 'cni.projectcalico.org/podIP',
                'cni.projectcalico.org/podIPs', 'k8s.v1.cni.cncf.io/network-status')
            if ($source.AllowRuntime -and $runtimeCNI -ccontains [string]$property.Name) { continue }
            throw "synthetic $($source.Where) 含未批准 annotation=$($property.Name)。"
        }
        foreach ($expectedName in $expectedAnnotationNames) {
            if (@($source.Value.PSObject.Properties.Name) -cnotcontains [string]$expectedName) {
                throw "synthetic $($source.Where) annotation 大小写/字段集不精确。"
            }
        }
    }
    $jobPodSpec = $Job.spec.template.spec
    Assert-PandoraDsAuthSyntheticPodSpec $jobPodSpec 'synthetic Job template'
    Assert-PandoraDsAuthSyntheticPodSpec $Pod.spec 'synthetic observed Pod'
    $parallelism = if ($null -ne $Job.spec.PSObject.Properties['parallelism']) { [int]$Job.spec.parallelism } else { 1 }
    $completions = if ($null -ne $Job.spec.PSObject.Properties['completions']) { [int]$Job.spec.completions } else { 1 }
    $completionMode = if ($null -ne $Job.spec.PSObject.Properties['completionMode']) { [string]$Job.spec.completionMode } else { 'NonIndexed' }
    $manualSelector = $false
    if ($null -ne $Job.spec.PSObject.Properties['manualSelector']) { $manualSelector = [bool]$Job.spec.manualSelector }
    $suspend = $false
    if ($null -ne $Job.spec.PSObject.Properties['suspend']) { $suspend = [bool]$Job.spec.suspend }
    if ([int]$Job.spec.backoffLimit -ne 0 -or [int]$Job.spec.activeDeadlineSeconds -lt 1 -or
        $parallelism -ne 1 -or $completions -ne 1 -or $completionMode -cne 'NonIndexed' -or
        $manualSelector -or $suspend -or $null -ne $Job.spec.PSObject.Properties['successPolicy'] -or
        $null -ne $Job.spec.PSObject.Properties['podFailurePolicy'] -or
        [int]$Job.spec.activeDeadlineSeconds -gt 300) {
        throw 'synthetic Job 必须使用固定 SA、Never、backoff=0、deadline<=300s。'
    }
    $complete = @($Job.status.conditions | Where-Object { $_.type -ceq 'Complete' -and $_.status -ceq 'True' })
    $failed = @($Job.status.conditions | Where-Object { $_.type -ceq 'Failed' -and $_.status -ceq 'True' })
    $failedCount = 0
    if ($null -ne $Job.status.PSObject.Properties['failed']) { $failedCount = [int]$Job.status.failed }
    if ($complete.Count -ne 1 -or $failed.Count -ne 0 -or [int]$Job.status.succeeded -ne 1 -or $failedCount -ne 0) {
        throw 'synthetic Job 未唯一成功或曾失败。'
    }
    $completion = [datetimeoffset]::Parse([string]$Job.status.completionTime)
    if ($completion -lt $NotBefore -or $completion -gt $Now -or ($Now - $completion) -gt $MaxAge) {
        throw 'synthetic Job 成功证据早于门槛、已过期或来自未来。'
    }

    $deletionTimestamp = $null
    if ($null -ne $Pod.metadata.PSObject.Properties['deletionTimestamp']) { $deletionTimestamp = $Pod.metadata.deletionTimestamp }
    if ([string]$Pod.metadata.namespace -cne $Namespace -or [string]$Pod.status.phase -cne 'Succeeded' -or
        $null -ne $deletionTimestamp) { throw 'synthetic Pod 非固定 namespace 的稳定 Succeeded Pod。' }
    $owners = @($Pod.metadata.ownerReferences | Where-Object { $_.kind -ceq 'Job' -and $_.uid -ceq $Job.metadata.uid -and $_.controller -eq $true })
    if ($owners.Count -ne 1) { throw 'synthetic Pod owner UID 与固定 Job 不一致。' }
    $jobContainers = @($Job.spec.template.spec.containers)
    if ($jobContainers.Count -ne 1) { throw 'synthetic Job template 必须且只能声明 probe container。' }
    $jobContainer = Get-PandoraNamedObject $jobContainers 'probe' 'synthetic Job container'
    $container = Get-PandoraNamedObject @($Pod.spec.containers) 'probe' 'synthetic Pod container'
    foreach ($probe in @($jobContainer, $container)) {
        if ([string]$probe.image -cnotmatch ('@' + [regex]::Escape($ImageDigest) + '$')) { throw 'synthetic probe image 未固定到批准 digest。' }
        Assert-PandoraDsAuthSyntheticProbeContainer $probe $Namespace $RunId $Phase $syntheticMode `
            $TargetEpoch $KeysetRevision $EtcdIdentityRevision
    }
    $status = Get-PandoraNamedObject @($Pod.status.containerStatuses) 'probe' 'synthetic containerStatus'
    if (@($Pod.status.containerStatuses).Count -ne 1) { throw 'synthetic Pod 不允许额外 containerStatus。' }
    if ([int]$status.restartCount -ne 0 -or -not ([string]$status.imageID).EndsWith($ImageDigest, [StringComparison]::Ordinal)) {
        throw 'synthetic probe imageID/restartCount 不满足固定成功证据。'
    }
    $terminated = $status.state.terminated
    $signal = 0
    if ($null -ne $terminated -and $null -ne $terminated.PSObject.Properties['signal']) { $signal = [int]$terminated.signal }
    if ($null -eq $terminated -or [int]$terminated.exitCode -ne 0 -or $signal -ne 0 -or
        [string]$terminated.reason -cne 'Completed') {
        throw 'synthetic probe 没有固定的 exitCode=0/Completed 终态。'
    }
    $startedAt = [datetimeoffset]::Parse([string]$terminated.startedAt)
    $finishedAt = [datetimeoffset]::Parse([string]$terminated.finishedAt)
    if ($startedAt -gt $finishedAt -or $finishedAt -gt $completion -or ($completion - $finishedAt) -gt [timespan]::FromMinutes(2)) {
        throw 'synthetic probe 终态时间与 Job completion 不一致。'
    }
    $terminationMessage = ''
    if ($null -ne $terminated.PSObject.Properties['message']) { $terminationMessage = [string]$terminated.message }
    try { $result = $terminationMessage | ConvertFrom-Json -ErrorAction Stop }
    catch { throw 'synthetic probe 未返回结构化 termination result。' }
    $expectedResult = [ordered]@{
        contract = 'v1'; run_id = $RunId; phase = $Phase; mode = $syntheticMode
        target_epoch = [string]$TargetEpoch; keyset_revision = $KeysetRevision
        etcd_identity_revision = $EtcdIdentityRevision; result = 'pass'
    }
    if (@($result.PSObject.Properties).Count -ne $expectedResult.Count) { throw 'synthetic termination result 字段集不精确。' }
    foreach ($pair in $expectedResult.GetEnumerator()) {
        if (@($result.PSObject.Properties.Name) -cnotcontains [string]$pair.Key -or
            [string]$result.($pair.Key) -cne [string]$pair.Value) {
            throw "synthetic termination result $($pair.Key) 大小写或值不匹配。"
        }
    }
}
