$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
. (Join-Path $ProjectRoot 'tools/scripts/lib/ds_auth_activation_contract.ps1')

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED: $Message" }
}

function Assert-Throws([scriptblock]$Action, [string]$Message) {
    $threw = $false
    try { & $Action } catch { $threw = $true }
    if (-not $threw) { throw "ASSERT FAILED (expected throw): $Message" }
}

function Copy-Object($Value) { return (($Value | ConvertTo-Json -Depth 30) | ConvertFrom-Json) }

Assert-PandoraPodUIDACLCleanupTimeline `
    ([datetimeoffset]'2026-07-15T12:00:00Z') ([datetimeoffset]'2026-07-15T12:01:00Z') `
    ([datetimeoffset]'2026-07-15T12:02:00Z') ([datetimeoffset]'2026-07-15T12:03:00Z')
Assert-Throws {
    Assert-PandoraPodUIDACLCleanupTimeline `
        ([datetimeoffset]'2026-07-15T12:00:00Z') ([datetimeoffset]'2026-07-15T12:02:00Z') `
        ([datetimeoffset]'2026-07-15T12:01:00Z') ([datetimeoffset]'2026-07-15T12:03:00Z')
} 'pre-switch ACL cleanup proof must never satisfy post-CAS completion'
$providerContinuation = [System.Collections.Generic.List[string]]::new()
Assert-Throws {
    Assert-PandoraRedisTopologyChangeLockProvider 'test-before-write' 'cluster' ('sha256:' + ('a' * 64))
    $providerContinuation.Add('kubectl-create-patch-scale')
} 'missing topology provider must throw before any simulated external write'
Assert-True ($providerContinuation.Count -eq 0) `
    'topology provider fail-closed hook must prevent the next create/patch/scale action'

$revision = 'r7'
$digest = 'sha256:' + ('a' * 64)
$policyV3Services = 'login=1,player_locator=1,ds_allocator=1,hub_allocator=1,battle_result=1'
$policyV3Instances = 'login=l1,player_locator=p1,ds_allocator=d1,hub_allocator=h1,battle_result=b1'
$policyV3Digests = 'login=' + $digest + ',player_locator=' + $digest + ',ds_allocator=' + $digest +
    ',hub_allocator=' + $digest + ',battle_result=' + $digest
$policyV3Marker = [pscustomobject]@{
    immutable = $true
    metadata = [pscustomobject]@{ uid = '12345678-abcd'; resourceVersion = '42'; creationTimestamp = '2026-07-15T12:01:00Z' }
    data = [pscustomobject][ordered]@{
        contract = 'ds-auth-policy-v3-activation-evidence-v1'
        run_id = 'successor-v3-test'; kube_context = 'prod-us-east'; namespace = 'pandora'
        from_policy_generation = '2'; to_policy_generation = '3'
        from_required_value = '2@ds-auth-v2-pod-uid-write-invariant-v1'
        to_required_value = '2@ds-auth-v2-hub-successor-lease-v1'
        required_policy_id = 'ds-auth-v2-hub-successor-lease-v1'
        staging_contract = 'capability-lease-not-service-endpoint-v1'
        expected_services = $policyV3Services; expected_instances = $policyV3Instances
        expected_image_digests = $policyV3Digests
        required_features = $script:PandoraDsAuthRequiredFeaturesV3
        evidence_sha256 = ('sha256:' + ('0' * 64)); completed_at_unix_ms = '1784116800000'
    }
}
$policyV3Marker.data.evidence_sha256 = Get-PandoraDsAuthPolicyV3EvidenceSHA256 $policyV3Marker.data
$policyV3Proof = Assert-PandoraDsAuthPolicyV3EvidenceContract $policyV3Marker `
    $policyV3Services $policyV3Instances $policyV3Digests
Assert-True ($policyV3Proof.EvidenceSHA256 -ceq $policyV3Marker.data.evidence_sha256) 'canonical V3 evidence marker rejected'
$badPolicyV3Marker = Copy-Object $policyV3Marker
$badPolicyV3Marker.data.staging_contract = 'service-endpoint-ready'
Assert-Throws {
    Assert-PandoraDsAuthPolicyV3EvidenceContract $badPolicyV3Marker `
        $policyV3Services $policyV3Instances $policyV3Digests
} 'V3 evidence must bind leased staging capabilities, not Hub readiness'
Assert-Throws {
    Resolve-PandoraDsAuthV3CanonicalUIDSet 'hub_allocator' @('uid-new', 'uid-extra') 1 @('uid-old') -PreCAS
} 'pre-CAS extra canonical Pod must be rejected even when the expected Pod is present elsewhere'
Assert-Throws {
    Resolve-PandoraDsAuthV3CanonicalUIDSet 'hub_allocator' @('uid-new') 1 @('uid-old') -PreCAS
} 'pre-CAS replacement UID must not satisfy immutable staging evidence'
$rescheduledUID = Resolve-PandoraDsAuthV3CanonicalUIDSet 'hub_allocator' @('uid-new') 1
Assert-True ($rescheduledUID -ceq 'uid-new') `
    'post-CAS finalize must derive and accept the current canonical replacement UID instead of marker UID'
$completionCounts = @{ login = 1; player_locator = 1; ds_allocator = 1; hub_allocator = 1; battle_result = 1 }
$completionMarker = [pscustomobject]@{
    immutable = $true
    metadata = [pscustomobject]@{ name = 'pandora-ds-auth-policy-v3-complete-successor-v3-test'; namespace = 'pandora';
        uid = 'abcdef12-3456'; resourceVersion = '77'; creationTimestamp = '2026-07-15T12:02:00Z' }
    data = [pscustomobject][ordered]@{
        contract = 'ds-auth-policy-v3-completion-v1'; run_id = $policyV3Proof.RunID
        required_value = '2@ds-auth-v2-hub-successor-lease-v1'; policy_generation = '3'
        evidence_uid = $policyV3Proof.UID; evidence_resource_version = $policyV3Proof.ResourceVersion
        evidence_sha256 = $policyV3Proof.EvidenceSHA256
        final_instances = 'login=l2,player_locator=p2,ds_allocator=d2,hub_allocator=h2,battle_result=b2'
        completed_at_unix_ms = '1784116860000'
    }
}
$null = Assert-PandoraDsAuthV3CompletionContract $completionMarker $completionMarker.metadata.name `
    'pandora' $policyV3Proof $completionCounts
$badCompletion = Copy-Object $completionMarker
$badCompletion.metadata.namespace = 'other'
Assert-Throws {
    Assert-PandoraDsAuthV3CompletionContract $badCompletion $completionMarker.metadata.name `
        'pandora' $policyV3Proof $completionCounts
} 'completion marker namespace drift must fail'
$badCompletion = Copy-Object $completionMarker
$badCompletion.data.final_instances += ',unknown=u1'
Assert-Throws {
    Assert-PandoraDsAuthV3CompletionContract $badCompletion $completionMarker.metadata.name `
        'pandora' $policyV3Proof $completionCounts
} 'completion marker extra service must fail'
$duplicateCompletion = Copy-Object $completionMarker
$duplicateCompletion.data.final_instances = 'login=l2,player_locator=p2,ds_allocator=d2,hub_allocator=h2|h2,battle_result=b2'
Assert-Throws {
    Assert-PandoraDsAuthV3CompletionContract $duplicateCompletion $completionMarker.metadata.name `
        'pandora' $policyV3Proof $completionCounts
} 'completion marker duplicate UID must not collapse into the expected count'
$crossServiceDuplicate = Copy-Object $completionMarker
$crossServiceDuplicate.data.final_instances = 'login=shared,player_locator=shared,ds_allocator=d2,hub_allocator=h2,battle_result=b2'
Assert-Throws {
    Assert-PandoraDsAuthV3CompletionContract $crossServiceDuplicate $completionMarker.metadata.name `
        'pandora' $policyV3Proof $completionCounts
} 'completion marker must reject a Pod UID reused by two services'
$secret = [pscustomobject]@{
    metadata = [pscustomobject]@{
        name = 'pandora-ds-auth-etcd-ds-allocator-r7'
        labels = [pscustomobject]@{
            'pandora.dev/ds-auth-etcd-identity-revision' = 'r7'
            'pandora.dev/ds-auth-etcd-client-identity' = 'pandora-ds-allocator'
        }
    }
    immutable = $true
    data = [pscustomobject]@{ 'ca.crt' = 'Y2E='; 'tls.crt' = 'Y2VydA=='; 'tls.key' = 'a2V5' }
}
$secretContract = Assert-PandoraDsAuthIdentitySecretContract $secret 'ds-allocator' $revision
Assert-True (-not $secretContract.UsesPasswordAuth) 'CN-only identity should be accepted'

$badSecret = Copy-Object $secret
$badSecret.immutable = $false
Assert-Throws { Assert-PandoraDsAuthIdentitySecretContract $badSecret 'ds-allocator' $revision } 'mutable identity Secret'
$badSecret = Copy-Object $secret
$badSecret.data | Add-Member -NotePropertyName username -NotePropertyValue 'dXNlcg=='
Assert-Throws { Assert-PandoraDsAuthIdentitySecretContract $badSecret 'ds-allocator' $revision } 'half username/password identity'
$badSecret = Copy-Object $secret
$badSecret.metadata.labels.'pandora.dev/ds-auth-etcd-client-identity' = 'pandora-login'
Assert-Throws { Assert-PandoraDsAuthIdentitySecretContract $badSecret 'ds-allocator' $revision } 'wrong certificate identity label'

$envValues = [ordered]@{
    'PANDORA_DS_AUTH_ETCD_REQUIRE_MTLS' = '1'
    'PANDORA_DS_AUTH_ETCD_CA_FILE' = '/run/secrets/pandora/ds-auth-etcd/ca.crt'
    'PANDORA_DS_AUTH_ETCD_CERT_FILE' = '/run/secrets/pandora/ds-auth-etcd/tls.crt'
    'PANDORA_DS_AUTH_ETCD_KEY_FILE' = '/run/secrets/pandora/ds-auth-etcd/tls.key'
    'PANDORA_DS_AUTH_ETCD_SERVER_NAME' = 'etcd.pandora.internal'
    'PANDORA_DS_AUTH_ETCD_CLIENT_IDENTITY' = 'pandora-ds-allocator'
    'PANDORA_DS_AUTH_ETCD_IDENTITY_REVISION' = 'r7'
    'PANDORA_DS_AUTH_ETCD_REQUIRE_AUTH' = '1'
    'PANDORA_DS_AUTH_ETCD_FORBIDDEN_READ_PREFIX' = '/pandora/acl-negative/'
}
$envList = @($envValues.GetEnumerator() | ForEach-Object {
    [pscustomobject]@{ name = $_.Key; value = $_.Value; valueFrom = $null }
})
$pod = [pscustomobject]@{
    metadata = [pscustomobject]@{
        name = 'ds-allocator-green-1'; namespace = 'pandora'; uid = 'pod-1'; deletionTimestamp = $null
        labels = [pscustomobject]@{ 'istio.io/rev' = 'asm-1-22' }
        annotations = [pscustomobject]@{ 'sidecar.istio.io/rewriteAppHTTPProbers' = 'true' }
    }
    spec = [pscustomobject]@{
        serviceAccountName = 'pandora-allocator'
        containers = @([pscustomobject]@{
            name = 'ds-allocator'; env = $envList
            volumeMounts = @([pscustomobject]@{ name = 'ds-auth-etcd-identity'; mountPath = '/run/secrets/pandora/ds-auth-etcd'; readOnly = $true })
        })
        volumes = @([pscustomobject]@{ name = 'ds-auth-etcd-identity'; secret = [pscustomobject]@{ secretName = 'pandora-ds-auth-etcd-ds-allocator-r7'; defaultMode = 288 } })
    }
    status = [pscustomobject]@{
        containerStatuses = @([pscustomobject]@{ name = 'istio-proxy'; ready = $true; restartCount = 0 })
    }
}
Assert-PandoraDsAuthIdentityPodContract $pod 'ds-allocator' $revision 'etcd.pandora.internal' '/pandora/acl-negative/' $false
Assert-PandoraDsTerminalMeshPodContract $pod 'ds-allocator'
$badPod = Copy-Object $pod
($badPod.spec.containers[0].env | Where-Object name -ceq 'PANDORA_DS_AUTH_ETCD_REQUIRE_AUTH').value = '0'
Assert-Throws { Assert-PandoraDsAuthIdentityPodContract $badPod 'ds-allocator' $revision 'etcd.pandora.internal' '/pandora/acl-negative/' $false } 'auth disabled in Pod'
$badPod = Copy-Object $pod
$badPod.status.containerStatuses[0].ready = $false
Assert-Throws { Assert-PandoraDsTerminalMeshPodContract $badPod 'ds-allocator' } 'terminal mesh sidecar not ready'

$peer = [pscustomobject]@{
    metadata = [pscustomobject]@{ name = 'pandora-ds-allocator-terminal-permissive'; namespace = 'pandora' }
    spec = [pscustomobject]@{ selector = [pscustomobject]@{ matchLabels = [pscustomobject]@{ app = 'ds-allocator' } }; mtls = [pscustomobject]@{ mode = 'PERMISSIVE' } }
}
$releasePath = '/pandora.ds.v1.DSAllocatorService/ReleaseBattle'
function New-DenyRule([string]$Principal) {
    return [pscustomobject]@{
        from = @([pscustomobject]@{ source = [pscustomobject]@{ notPrincipals = @($Principal) } })
        to = @([pscustomobject]@{ operation = [pscustomobject]@{ methods = @('POST'); paths = @($releasePath) } })
    }
}
$policy = [pscustomobject]@{
    metadata = [pscustomobject]@{ name = 'pandora-ds-terminal-release-exact-deny'; namespace = 'pandora' }
    spec = [pscustomobject]@{
        selector = [pscustomobject]@{ matchLabels = [pscustomobject]@{ app = 'ds-allocator' } }
        action = 'DENY'
        rules = @((New-DenyRule '*'), (New-DenyRule 'cluster.local/ns/pandora/sa/pandora-battle-result'))
    }
}
$service = [pscustomobject]@{
    metadata = [pscustomobject]@{ name = 'ds-allocator'; namespace = 'pandora' }
    spec = [pscustomobject]@{ ports = @([pscustomobject]@{ name = 'grpc'; appProtocol = 'grpc'; port = 50020 }) }
}
Assert-PandoraDsTerminalMeshPolicyContract $peer $policy $service
$badPolicy = Copy-Object $policy
$badPolicy.spec.rules[1].to[0].operation.paths[0] = '/pandora.ds.v1.DSAllocatorService/Heartbeat'
Assert-Throws { Assert-PandoraDsTerminalMeshPolicyContract $peer $badPolicy $service } 'broadened terminal RPC path'

$runId = 'release-20260713'
$now = [datetimeoffset]::Parse('2026-07-13T03:00:00Z')
$completion = $now.AddMinutes(-1).ToString('o')
$jobName = "pandora-ds-auth-synthetic-v1-$runId-prepare"
$syntheticAnnotations = [pscustomobject]@{
    'pandora.dev/ds-auth-synthetic-contract' = 'v1'
    'pandora.dev/ds-auth-activation-run' = $runId
    'pandora.dev/ds-auth-synthetic-phase' = 'prepare'
    'pandora.dev/ds-auth-synthetic-mode' = 'isolated-no-writes-v1'
    'pandora.dev/image-digest' = $digest
    'pandora.dev/ds-auth-target-epoch' = '2'
    'pandora.dev/ds-auth-keyset-revision' = 'callback-r9'
    'pandora.dev/ds-auth-etcd-identity-revision' = 'r7'
    'sidecar.istio.io/inject' = 'false'
}
$syntheticCommand = @('/pandora/bin/dsauth-synthetic-v1')
$syntheticArgs = @('--contract=v1', '--namespace=pandora', "--run-id=$runId", '--phase=prepare',
    '--mode=isolated-no-writes-v1', '--target-epoch=2', '--keyset-revision=callback-r9',
    '--etcd-identity-revision=r7', '--result-file=/dev/termination-log')
$syntheticEnv = @(
    [pscustomobject]@{ name = 'PANDORA_SYNTHETIC_CONTRACT'; value = 'v1' },
    [pscustomobject]@{ name = 'PANDORA_ACTIVATION_RUN_ID'; value = $runId },
    [pscustomobject]@{ name = 'PANDORA_SYNTHETIC_PHASE'; value = 'prepare' },
    [pscustomobject]@{ name = 'PANDORA_SYNTHETIC_MODE'; value = 'isolated-no-writes-v1' },
    [pscustomobject]@{ name = 'PANDORA_TARGET_WRITER_EPOCH'; value = '2' },
    [pscustomobject]@{ name = 'PANDORA_KEYSET_REVISION'; value = 'callback-r9' },
    [pscustomobject]@{ name = 'PANDORA_ETCD_IDENTITY_REVISION'; value = 'r7' },
    [pscustomobject]@{ name = 'PANDORA_NAMESPACE'; value = 'pandora' }
)
$syntheticContainer = [pscustomobject]@{
    name = 'probe'; image = "registry/pandora/dsauth-synthetic@$digest"
    command = $syntheticCommand; args = $syntheticArgs; env = $syntheticEnv; volumeMounts = @()
    terminationMessagePath = '/dev/termination-log'; terminationMessagePolicy = 'File'
    securityContext = [pscustomobject]@{
        allowPrivilegeEscalation = $false; readOnlyRootFilesystem = $true; runAsNonRoot = $true
        capabilities = [pscustomobject]@{ drop = @('ALL') }
    }
}
$job = [pscustomobject]@{
    metadata = [pscustomobject]@{
        name = $jobName; namespace = 'pandora'; uid = 'job-uid'
        annotations = $syntheticAnnotations
    }
    spec = [pscustomobject]@{
        backoffLimit = 0; activeDeadlineSeconds = 120
        template = [pscustomobject]@{
            metadata = [pscustomobject]@{ annotations = $syntheticAnnotations }
            spec = [pscustomobject]@{
                serviceAccountName = 'pandora-ds-auth-synthetic-v1'; restartPolicy = 'Never'; containers = @($syntheticContainer)
                dnsPolicy = 'ClusterFirst'; automountServiceAccountToken = $false; enableServiceLinks = $false
                securityContext = [pscustomobject]@{ runAsNonRoot = $true; seccompProfile = [pscustomobject]@{ type = 'RuntimeDefault' } }
            }
        }
    }
    status = [pscustomobject]@{
        conditions = @([pscustomobject]@{ type = 'Complete'; status = 'True' })
        succeeded = 1; failed = 0; completionTime = $completion
    }
}
$syntheticPodSpec = Copy-Object $job.spec.template.spec
$syntheticPod = [pscustomobject]@{
    metadata = [pscustomobject]@{
        namespace = 'pandora'; deletionTimestamp = $null; annotations = $syntheticAnnotations
        ownerReferences = @([pscustomobject]@{ kind = 'Job'; uid = 'job-uid'; controller = $true })
    }
    spec = $syntheticPodSpec
    status = [pscustomobject]@{
        phase = 'Succeeded'
        containerStatuses = @([pscustomobject]@{
            name = 'probe'; imageID = "docker-pullable://registry/pandora/dsauth-synthetic@$digest"; restartCount = 0
            state = [pscustomobject]@{ terminated = [pscustomobject]@{
                exitCode = 0; signal = 0; reason = 'Completed'; startedAt = $now.AddMinutes(-2).ToString('o'); finishedAt = $now.AddMinutes(-1).AddSeconds(-5).ToString('o')
                message = (@{ contract = 'v1'; run_id = $runId; phase = 'prepare'; mode = 'isolated-no-writes-v1'; target_epoch = 2; keyset_revision = 'callback-r9'; etcd_identity_revision = 'r7'; result = 'pass' } | ConvertTo-Json -Compress)
            } }
        })
    }
}
Assert-PandoraDsAuthSyntheticContract $job $syntheticPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now
$oldJob = Copy-Object $job
$oldJob.status.completionTime = $now.AddHours(-2).ToString('o')
Assert-Throws {
    Assert-PandoraDsAuthSyntheticContract $oldJob $syntheticPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddHours(-3) $now
} 'stale synthetic evidence'
$badJob = Copy-Object $job
$badJob.metadata.annotations.'pandora.dev/ds-auth-synthetic-phase' = 'final'
Assert-Throws {
    Assert-PandoraDsAuthSyntheticContract $badJob $syntheticPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now
} 'synthetic phase mismatch'
$badPod = Copy-Object $syntheticPod
$badPod.spec.containers[0].command = @('/bin/true')
Assert-Throws {
    Assert-PandoraDsAuthSyntheticContract $job $badPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now
} 'synthetic command bypass'
$badPod = Copy-Object $syntheticPod
$badPod.status.containerStatuses[0].state.terminated.message = '{"contract":"v1","result":"pass"}'
Assert-Throws {
    Assert-PandoraDsAuthSyntheticContract $job $badPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now
} 'incomplete synthetic termination result'
$badPod = Copy-Object $syntheticPod
$badPod.status.containerStatuses[0].state.terminated.message = (@{ CONTRACT = 'v1'; RUN_ID = $runId; PHASE = 'prepare'; MODE = 'isolated-no-writes-v1'; TARGET_EPOCH = 2; KEYSET_REVISION = 'callback-r9'; ETCD_IDENTITY_REVISION = 'r7'; RESULT = 'pass' } | ConvertTo-Json -Compress)
Assert-Throws {
    Assert-PandoraDsAuthSyntheticContract $job $badPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now
} 'termination result property names must be canonical case'
$badJob = Copy-Object $job
$badJob.spec.template.spec.containers[0].volumeMounts = @([pscustomobject]@{ name = 'replace-root'; mountPath = '/' })
Assert-Throws {
    Assert-PandoraDsAuthSyntheticContract $badJob $syntheticPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now
} 'root volume can replace immutable probe binary'
$badJob = Copy-Object $job
$badJob.spec.template.spec | Add-Member -NotePropertyName initContainers -NotePropertyValue @([pscustomobject]@{ name = 'replace-probe' })
Assert-Throws {
    Assert-PandoraDsAuthSyntheticContract $badJob $syntheticPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now
} 'synthetic init container bypass'
$badPod = Copy-Object $syntheticPod
$badPod.spec.containers += [pscustomobject]@{ name = 'attacker-sidecar'; image = 'attacker@sha256:' + ('f' * 64) }
Assert-Throws {
    Assert-PandoraDsAuthSyntheticContract $job $badPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now
} 'admission-injected sidecar bypass'
$badPod = Copy-Object $syntheticPod
$badPod.spec | Add-Member -NotePropertyName initContainers -NotePropertyValue @([pscustomobject]@{ name = 'attacker-init' })
$badPod.spec | Add-Member -NotePropertyName hostNetwork -NotePropertyValue $true
Assert-Throws {
    Assert-PandoraDsAuthSyntheticContract $job $badPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now
} 'observed Pod init/host network bypass'
$badPod = Copy-Object $syntheticPod
$badPod.spec.containers[0].volumeMounts = @([pscustomobject]@{ name = 'attacker-config'; mountPath = '/etc' })
$badPod.spec | Add-Member -NotePropertyName volumes -NotePropertyValue @([pscustomobject]@{ name = 'attacker-config'; configMap = [pscustomobject]@{ name = 'attacker' } })
Assert-Throws {
    Assert-PandoraDsAuthSyntheticContract $job $badPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now
} 'observed Pod /etc ConfigMap bypass'
$badPod = Copy-Object $syntheticPod
$badPod.metadata.annotations | Add-Member -NotePropertyName 'k8s.v1.cni.cncf.io/networks' -NotePropertyValue 'attacker/default-route'
Assert-Throws {
    Assert-PandoraDsAuthSyntheticContract $job $badPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now
} 'observed Pod network attachment annotation bypass'
$badJob = Copy-Object $job
$badJob.spec | Add-Member -NotePropertyName parallelism -NotePropertyValue 100
$badJob.spec | Add-Member -NotePropertyName completions -NotePropertyValue 1
Assert-Throws {
    Assert-PandoraDsAuthSyntheticContract $badJob $syntheticPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now
} 'parallel synthetic winner bypass'
$omitemptyJob = Copy-Object $job
$omitemptyPod = Copy-Object $syntheticPod
$omitemptyJob.status.PSObject.Properties.Remove('failed')
$omitemptyPod.metadata.PSObject.Properties.Remove('deletionTimestamp')
$omitemptyPod.spec.containers[0].PSObject.Properties.Remove('volumeMounts')
$omitemptyPod.status.containerStatuses[0].state.terminated.PSObject.Properties.Remove('signal')
Assert-PandoraDsAuthSyntheticContract $omitemptyJob $omitemptyPod 'pandora' $runId 'prepare' $digest 2 'callback-r9' 'r7' ([timespan]::FromMinutes(30)) $now.AddMinutes(-5) $now

$endpointService = [pscustomobject]@{
    metadata = [pscustomobject]@{ name = 'login'; namespace = 'pandora'; uid = 'service-uid' }
    spec = [pscustomobject]@{ ports = @([pscustomobject]@{ port = 50001; protocol = 'TCP'; targetPort = 50001 }) }
}
$endpointPod = [pscustomobject]@{
    metadata = [pscustomobject]@{ name = 'login-green-abc'; namespace = 'pandora'; uid = 'pod-uid' }
    status = [pscustomobject]@{ podIP = '10.0.0.8'; podIPs = @([pscustomobject]@{ ip = '10.0.0.8' }) }
}
$endpointSlice = [pscustomobject]@{
    metadata = [pscustomobject]@{
        namespace = 'pandora'
        labels = [pscustomobject]@{
            'kubernetes.io/service-name' = 'login'
            'endpointslice.kubernetes.io/managed-by' = 'endpointslice-controller.k8s.io'
        }
        ownerReferences = @([pscustomobject]@{ kind = 'Service'; name = 'login'; uid = 'service-uid'; controller = $true })
    }
    addressType = 'IPv4'
    ports = @([pscustomobject]@{ name = ''; protocol = 'TCP'; port = 50001 })
    endpoints = @([pscustomobject]@{
        addresses = @('10.0.0.8')
        conditions = [pscustomobject]@{ ready = $true; serving = $true; terminating = $false }
        targetRef = [pscustomobject]@{ kind = 'Pod'; namespace = 'pandora'; name = 'login-green-abc'; uid = 'pod-uid' }
    })
}
$sliceList = [pscustomobject]@{ items = @($endpointSlice) }
$verifiedUIDs = Get-PandoraDsAuthVerifiedEndpointUIDSet $sliceList $endpointService @($endpointPod) 'pandora'
Assert-True ($verifiedUIDs.Count -eq 1 -and $verifiedUIDs.Contains('pod-uid')) 'exact controller EndpointSlice maps to exact Pod UID/IP/port'
$badSlices = Copy-Object $sliceList
$badSlices.items[0].endpoints[0].targetRef = $null
Assert-Throws { Get-PandoraDsAuthVerifiedEndpointUIDSet $badSlices $endpointService @($endpointPod) 'pandora' } 'ready endpoint without targetRef'
$badSlices = Copy-Object $sliceList
$badSlices.items[0].endpoints[0].addresses[0] = '203.0.113.8'
Assert-Throws { Get-PandoraDsAuthVerifiedEndpointUIDSet $badSlices $endpointService @($endpointPod) 'pandora' } 'endpoint address outside exact Pod IPs'
$badSlices = Copy-Object $sliceList
$badSlices.items[0].endpoints += Copy-Object $badSlices.items[0].endpoints[0]
Assert-Throws { Get-PandoraDsAuthVerifiedEndpointUIDSet $badSlices $endpointService @($endpointPod) 'pandora' } 'duplicate endpoint address cannot collapse through UID set'
$badSlices = Copy-Object $sliceList
$badSlices.items[0].ports[0].port = 50099
Assert-Throws { Get-PandoraDsAuthVerifiedEndpointUIDSet $badSlices $endpointService @($endpointPod) 'pandora' } 'EndpointSlice target port drift'

$hubGreenService = [pscustomobject]@{
    apiVersion = 'v1'; kind = 'Service'
    metadata = [pscustomobject]@{ name = 'hub-allocator'; namespace = 'pandora'; uid = 'abcdef12-3456' }
    spec = [pscustomobject]@{
        type = 'ClusterIP'; clusterIP = '10.96.0.21'
        selector = [pscustomobject]@{
            app = 'hub-allocator'; 'pandora.dev/ds-auth-writer-set' = 'green';
            'pandora.dev/ds-auth-writer-epoch' = '2'
        }
        ports = @([pscustomobject]@{ port = 50021; targetPort = 50021; protocol = 'TCP' })
    }
}
Assert-PandoraHubAllocatorGreenServiceContract $hubGreenService 'pandora'
$badHubService = Copy-Object $hubGreenService
$badHubService.spec.selector | Add-Member -NotePropertyName attacker -NotePropertyValue route
Assert-Throws { Assert-PandoraHubAllocatorGreenServiceContract $badHubService 'pandora' } `
    'Hub zero-endpoint evidence must reject a Service with an extra/drifted selector'
$badHubService = Copy-Object $hubGreenService
$badHubService.spec | Add-Member -NotePropertyName publishNotReadyAddresses -NotePropertyValue $true
Assert-Throws { Assert-PandoraHubAllocatorGreenServiceContract $badHubService 'pandora' } `
    'Hub Service must not publish NotReady staged candidates'

Assert-True (@(Assert-PandoraDsAuthHttpsEndpoints 'https://etcd-a.internal:2379,https://etcd-b.internal:2379').Count -eq 2) 'canonical etcd endpoints'
Assert-Throws { Assert-PandoraDsAuthHttpsEndpoints 'http://etcd.internal:2379' } 'plaintext etcd endpoint'
Assert-Throws { Assert-PandoraDsAuthHttpsEndpoints 'https://etcd.internal' } 'etcd endpoint without explicit port'
Assert-Throws { Assert-PandoraDsAuthHttpsEndpoints 'https://etcd.internal:0' } 'zero etcd port'
Assert-Throws { Assert-PandoraDsAuthHttpsEndpoints 'https://etcd.internal:02379' } 'non-canonical etcd port'
Assert-Throws { Assert-PandoraDsAuthHttpsEndpoints 'https://etcd.internal:65536' } 'out-of-range etcd port'
Assert-Throws { Assert-PandoraDsAuthEtcdRevision '7' } 'non-canonical identity revision'
$identityPatch = New-PandoraDsAuthEtcdIdentityPatch 'ds-allocator' 'r7' 'etcd.pandora.internal' '/pandora/acl-negative/' $false
Assert-True ($identityPatch.Contains('secretName: pandora-ds-auth-etcd-ds-allocator-r7')) 'runtime patch exact immutable Secret name'
Assert-True ($identityPatch.Contains('PANDORA_DS_AUTH_ETCD_CLIENT_IDENTITY, value: pandora-ds-allocator')) 'runtime patch exact client identity'
Assert-True ($identityPatch.Contains('PANDORA_DS_AUTH_ETCD_REQUIRE_AUTH, value: "1"')) 'runtime patch auth enabled'
Assert-True (-not $identityPatch.Contains('PANDORA_DS_AUTH_ETCD_PASSWORD_FILE')) 'CN-only runtime patch has no password path'
$passwordPatch = New-PandoraDsAuthEtcdIdentityPatch 'login' 'r7' 'etcd.pandora.internal' '/pandora/acl-negative/' $true
Assert-True ($passwordPatch.Contains('PANDORA_DS_AUTH_ETCD_USERNAME_FILE')) 'optional username/password runtime patch'
Assert-Throws { New-PandoraDsAuthEtcdIdentityPatch 'login' 'r7' 'bad:2379' '/pandora/acl-negative/' $false } 'unsafe TLS server name in YAML patch'
$greenTemplateSpec = Copy-Object $pod.spec
$greenTemplateSpec.containers[0] | Add-Member -NotePropertyName image -NotePropertyValue ('registry/pandora/ds-allocator@' + $digest)
$greenTemplateSpec.containers[0] | Add-Member -NotePropertyName args -NotePropertyValue @('-conf', 'etc/cluster.yaml')
$greenTemplateSpec.containers[0] | Add-Member -NotePropertyName ports -NotePropertyValue @([pscustomobject]@{ name = 'grpc'; containerPort = 50020 })
$greenTemplateSpec.containers[0] | Add-Member -NotePropertyName readinessProbe -NotePropertyValue ([pscustomobject]@{ grpc = [pscustomobject]@{ port = 50020 } })
$greenTemplateSpec.containers[0] | Add-Member -NotePropertyName resources -NotePropertyValue ([pscustomobject]@{ requests = [pscustomobject]@{ cpu = '25m' } })
$greenTemplateSpec.volumes += [pscustomobject]@{ name = 'conf'; secret = [pscustomobject]@{ secretName = 'pandora-config' } }
$greenTemplateSpec.containers[0].volumeMounts += [pscustomobject]@{
    name = 'conf'; mountPath = '/app/etc/cluster.yaml'; subPath = 'ds-allocator.yaml'; readOnly = $true
}
$liveGreen = [pscustomobject]@{
    apiVersion = 'apps/v1'; kind = 'Deployment'
    metadata = [pscustomobject]@{
        name = 'ds-allocator-ds-auth-green'; namespace = 'pandora'; resourceVersion = '123'
        labels = [pscustomobject]@{ app = 'ds-allocator' }
        annotations = [pscustomobject]@{ 'pandora.dev/ds-auth-green-desired-replicas' = '2' }
    }
    spec = [pscustomobject]@{
        replicas = 2
        selector = [pscustomobject]@{ matchLabels = [pscustomobject]@{
            app = 'ds-allocator'; 'pandora.dev/ds-auth-writer-set' = 'green'; 'pandora.dev/ds-auth-writer-epoch' = '2'
        } }
        strategy = [pscustomobject]@{ type = 'RollingUpdate'; rollingUpdate = [pscustomobject]@{ maxUnavailable = 0; maxSurge = 1 } }
        template = [pscustomobject]@{
            metadata = [pscustomobject]@{
                labels = [pscustomobject]@{
                    app = 'ds-allocator'; 'pandora.dev/ds-auth-writer-set' = 'green'; 'pandora.dev/ds-auth-writer-epoch' = '2'
                    'istio.io/rev' = 'asm-1-22'
                }
                annotations = [pscustomobject]@{
                    'pandora.dev/image-digest' = $digest
                    'sidecar.istio.io/rewriteAppHTTPProbers' = 'true'
                }
            }
            spec = $greenTemplateSpec
        }
    }
}
$newDigest = 'sha256:' + ('b' * 64)
$greenObject = New-PandoraDsAuthCanonicalGreenObject $liveGreen 'ds-allocator' 'r7' `
    'etcd.pandora.internal' '/pandora/acl-negative/' $false 2 ('registry/pandora/ds-allocator@' + $newDigest) $newDigest
Assert-True ($greenObject.metadata.resourceVersion -ceq '123') 'canonical green full object keeps CAS resourceVersion'
$opaqueRVGreen = Copy-Object $liveGreen
$opaqueRVGreen.metadata.resourceVersion = 'rv:opaque-1'
$opaqueRVObject = New-PandoraDsAuthCanonicalGreenObject $opaqueRVGreen 'ds-allocator' 'r7' `
    'etcd.pandora.internal' '/pandora/acl-negative/' $false 2 `
    ('registry/pandora/ds-allocator@' + $newDigest) $newDigest
Assert-True ($opaqueRVObject.metadata.resourceVersion -ceq 'rv:opaque-1') `
    'Kubernetes resourceVersion is opaque and must not be interpreted as decimal'
Assert-True ($greenObject.spec.replicas -eq 2 -and $greenObject.spec.selector.matchLabels.'pandora.dev/ds-auth-writer-set' -ceq 'green') 'canonical green keeps desired/immutable selector'
$greenContainer = @($greenObject.spec.template.spec.containers | Where-Object name -ceq 'ds-allocator')[0]
Assert-True ($greenContainer.args[0] -ceq '-conf' -and $greenContainer.ports[0].containerPort -eq 50020 -and
    $greenContainer.readinessProbe.grpc.port -eq 50020 -and $greenContainer.resources.requests.cpu -ceq '25m') `
    'canonical green full object keeps args/ports/probes/resources'
Assert-True ($greenObject.spec.template.spec.serviceAccountName -ceq 'pandora-allocator' -and
    $greenObject.spec.template.metadata.labels.'istio.io/rev' -ceq 'asm-1-22' -and
    @($greenObject.spec.template.metadata.labels.PSObject.Properties | Where-Object Name -CEQ 'sidecar.istio.io/inject').Count -eq 0) `
    'canonical green full object keeps SA/single revision selector without dual injector marker'
$snapshotLive = Copy-Object $liveGreen
@($snapshotLive.spec.template.spec.volumes | Where-Object name -ceq 'conf')[0].secret.secretName = `
    'pandora-dsa-cfg-v2-release-run-01-aaaaaaaaaaaa'
$ordinaryGreen = New-PandoraDsAuthCanonicalGreenObject $snapshotLive 'ds-allocator' 'r7' `
    'etcd.pandora.internal' '/pandora/acl-negative/' $false 2 `
    ('registry/pandora/ds-allocator@' + $newDigest) $newDigest
Assert-True ([string](@($ordinaryGreen.spec.template.spec.volumes | Where-Object name -ceq 'conf')[0].secret.secretName) -ceq
    'pandora-config') 'ordinary epoch2 ds-allocator release must leave activation snapshot and return to current pandora-config'

$liveHubGreen = Copy-Object $liveGreen
$liveHubGreen.metadata.name = 'hub-allocator-ds-auth-green'
$liveHubGreen.metadata.labels.app = 'hub-allocator'
$liveHubGreen.metadata.annotations.'pandora.dev/ds-auth-green-desired-replicas' = '1'
$liveHubGreen.spec.replicas = 1
$liveHubGreen.spec.selector.matchLabels.app = 'hub-allocator'
$liveHubGreen.spec.template.metadata.labels.app = 'hub-allocator'
$hubMain = @($liveHubGreen.spec.template.spec.containers | Where-Object name -ceq 'ds-allocator')[0]
$hubMain.name = 'hub-allocator'
$hubMain.image = 'registry/pandora/hub-allocator@' + $digest
@($hubMain.env | Where-Object name -ceq 'PANDORA_DS_AUTH_ETCD_CLIENT_IDENTITY')[0].value = 'pandora-hub-allocator'
# 复现真实 live green：strategy 将被 builder 改成 Recreate，但模板仍携带旧
# RollingUpdate annotation/env；候选对象必须原子收敛三者，不能让新 Pod 启动后自杀。
$liveHubGreen.spec.template.metadata.annotations | Add-Member `
    -NotePropertyName 'pandora.dev/deploy-strategy' -NotePropertyValue 'RollingUpdate'
$hubMain.env = @($hubMain.env) + @([pscustomobject]@{
    name = 'PANDORA_DEPLOY_STRATEGY'; value = 'RollingUpdate'
})
@($liveHubGreen.spec.template.spec.volumes | Where-Object name -ceq 'ds-auth-etcd-identity')[0].secret.secretName = `
    'pandora-ds-auth-etcd-hub-allocator-r7'
$hubGreenObject = New-PandoraDsAuthCanonicalGreenObject $liveHubGreen 'hub-allocator' 'r7' `
    'etcd.pandora.internal' '/pandora/acl-negative/' $false 1 `
    ('registry/pandora/hub-allocator@' + $newDigest) $newDigest
Assert-PandoraHubAllocatorSingleWriterDeploymentContract $hubGreenObject
$hubStageProof = Assert-PandoraDsAuthV3GreenStageDeploymentContract $hubGreenObject 'hub-allocator' 'r7' `
    'etcd.pandora.internal' '/pandora/acl-negative/' $false
Assert-True ($hubStageProof.DesiredReplicas -eq 1 -and $hubStageProof.Digest -ceq $newDigest) `
    'server-preview V3 Hub stage contract must bind exact single writer and immutable image digest'
$hubGreenMain = @($hubGreenObject.spec.template.spec.containers | Where-Object name -ceq 'hub-allocator')[0]
Assert-True ([int]$hubGreenObject.spec.replicas -eq 1 -and
    [string]$hubGreenObject.spec.strategy.type -ceq 'Recreate' -and
    $null -eq $hubGreenObject.spec.strategy.PSObject.Properties['rollingUpdate'] -and
    [string]$hubGreenObject.spec.template.metadata.annotations.'pandora.dev/deploy-strategy' -ceq 'Recreate' -and
    @($hubGreenMain.env | Where-Object name -ceq 'PANDORA_DEPLOY_STRATEGY').Count -eq 1 -and
    [string](@($hubGreenMain.env | Where-Object name -ceq 'PANDORA_DEPLOY_STRATEGY')[0].valueFrom.fieldRef.fieldPath) -ceq `
        "metadata.annotations['pandora.dev/deploy-strategy']") `
    'ordinary canonical green Hub replacement must keep strategy, annotation and downward-API env on exact Recreate'
$badHubGreen = Copy-Object $hubGreenObject
$badHubGreen.spec.strategy = [pscustomobject]@{ type = 'RollingUpdate'; rollingUpdate = [pscustomobject]@{ maxSurge = 1 } }
Assert-Throws { Assert-PandoraHubAllocatorSingleWriterDeploymentContract $badHubGreen } `
    'live canonical green Hub RollingUpdate must fail the single-writer contract'
$badHubAnnotation = Copy-Object $hubGreenObject
$badHubAnnotation.spec.template.metadata.annotations.'pandora.dev/deploy-strategy' = 'RollingUpdate'
Assert-Throws { Assert-PandoraHubAllocatorSingleWriterDeploymentContract $badHubAnnotation } `
    'live canonical green Hub strategy annotation drift must fail before Recreate removes the old Pod'
$badHubEnv = Copy-Object $hubGreenObject
@($badHubEnv.spec.template.spec.containers | Where-Object name -ceq 'hub-allocator')[0].env = `
    @(@($badHubEnv.spec.template.spec.containers | Where-Object name -ceq 'hub-allocator')[0].env | Where-Object name -cne 'PANDORA_DEPLOY_STRATEGY')
Assert-Throws { Assert-PandoraHubAllocatorSingleWriterDeploymentContract $badHubEnv } `
    'live canonical green Hub missing strategy downward-API env must fail before mutation'
Assert-Throws {
    Assert-PandoraDsAuthV3GreenStageDeploymentContract $badHubGreen 'hub-allocator' 'r7' `
        'etcd.pandora.internal' '/pandora/acl-negative/' $false
} 'exact-name Hub server-preview with RollingUpdate must fail before mutating apply'

$podUIDGreen = Copy-Object $greenObject
$podUIDGreen.spec.template.spec | Add-Member -NotePropertyName securityContext -NotePropertyValue ([pscustomobject]@{
    runAsNonRoot = $true; runAsUser = 10001; runAsGroup = 10001; fsGroup = 10001
})
$podUIDMain = @($podUIDGreen.spec.template.spec.containers | Where-Object name -ceq 'ds-allocator')[0]
$podUIDPrepareConfig = [pscustomobject]@{
    SourceSecretName = 'pandora-config'
    SourceSecretUID = '11111111-1111-1111-1111-111111111111'
    SourceSecretResourceVersion = '101'
    RawConfigSHA256 = 'sha256:' + ('a' * 64)
    RawSnapshotName = 'pandora-dsa-cfg-v2-release-run-01-' + ('a' * 12)
    RawSnapshotUID = '22222222-2222-2222-2222-222222222222'
    RawSnapshotResourceVersion = '102'
    RawSnapshotSHA256 = 'sha256:' + ('a' * 64)
    ROSecretName = 'pandora-pod-uid-preflight-redis-ro-r7'
    ROSecretUID = '33333333-3333-3333-3333-333333333333'
    ROSecretResourceVersion = '103'
    ROConfigSHA256 = 'sha256:' + ('c' * 64)
    RedisConfigIdentity = 'sha256:' + ('e' * 64)
    RedisConfigTopology = 'standalone'
    HelperSourceSHA256 = 'sha256:' + ('f' * 64)
    RedisTargetIdentity = 'pending-prepare'
}
$podUIDJob = New-PandoraPodUIDReleasePreflightJobObject $podUIDGreen 'release-run-01' 'prepare' 2 $podUIDPrepareConfig
Assert-PandoraPodUIDReleasePreflightJobContract $podUIDJob `
    ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'prepare' 2 $podUIDPrepareConfig
Assert-True (@($podUIDJob.spec.template.spec.containers).Count -eq 1 -and
    $null -eq $podUIDJob.spec.template.spec.PSObject.Properties['initContainers'] -and
    $podUIDJob.spec.template.spec.automountServiceAccountToken -eq $false) `
    'pod_uid release preflight is an explicit one-shot Job, never an init container'
$badPodUIDJob = Copy-Object $podUIDJob
$badPodUIDJob.spec.template.spec.containers[0].image = 'registry/pandora/ds-allocator@sha256:' + ('c' * 64)
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightJobContract $badPodUIDJob `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'prepare' 2 $podUIDPrepareConfig
} 'pod_uid Job image drift'
$badPodUIDJob = Copy-Object $podUIDJob
$badPodUIDJob.spec.template.spec.containers[0].args[2] = '-serve'
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightJobContract $badPodUIDJob `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'prepare' 2 $podUIDPrepareConfig
} 'pod_uid Job one-shot args bypass'
$badPodUIDJob = Copy-Object $podUIDJob
$badPodUIDJob.spec.template.spec | Add-Member -NotePropertyName initContainers -NotePropertyValue @(
    [pscustomobject]@{ name = 'smuggled-writer' }
)
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightJobContract $badPodUIDJob `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'prepare' 2 $podUIDPrepareConfig
} 'pod_uid Job init container bypass'
$badPodUIDJob = Copy-Object $podUIDJob
$badPodUIDJob.spec.template.spec.containers[0].volumeMounts[0].readOnly = $false
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightJobContract $badPodUIDJob `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'prepare' 2 $podUIDPrepareConfig
} 'pod_uid Job writable config bypass'
$podUIDDrainedConfig = Copy-Object $podUIDPrepareConfig
$podUIDDrainedConfig.RedisTargetIdentity = 'sha256:' + ('d' * 64)
@($podUIDGreen.spec.template.spec.volumes | Where-Object name -ceq 'conf')[0].secret.secretName = `
    $podUIDDrainedConfig.RawSnapshotName
$drainedPodUIDJob = New-PandoraPodUIDReleasePreflightJobObject $podUIDGreen 'release-run-01' 'drained' 2 $podUIDDrainedConfig
Assert-PandoraPodUIDReleasePreflightJobContract $drainedPodUIDJob `
    ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig
Assert-True ([string]$podUIDJob.metadata.name -cne [string]$drainedPodUIDJob.metadata.name -and
    [string]$podUIDJob.metadata.labels.'pandora.dev/preflight-phase' -ceq 'prepare' -and
    [string]$drainedPodUIDJob.metadata.labels.'pandora.dev/preflight-phase' -ceq 'drained' -and
    (@($drainedPodUIDJob.spec.template.spec.containers[0].args) -join ' ').Contains(
        '-pod-uid-release-preflight-phase=drained')) `
    'prepare/drained Jobs are immutable distinct phase-bound evidence'
$badDrainedJob = Copy-Object $drainedPodUIDJob
$badDrainedJob.metadata.labels.'pandora.dev/preflight-phase' = 'prepare'
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightJobContract $badDrainedJob `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig
} 'prepare evidence cannot satisfy drained gate'
$badSelectorJob = Copy-Object $drainedPodUIDJob
$badSelectorJob.spec | Add-Member -NotePropertyName manualSelector -NotePropertyValue $false
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightJobContract $badSelectorJob `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig
} 'manualSelector must be absent even when false'
$badSelectorJob = Copy-Object $drainedPodUIDJob
$badSelectorJob.spec | Add-Member -NotePropertyName selector -NotePropertyValue ([pscustomobject]@{})
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightJobContract $badSelectorJob `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig
} 'selector must be absent'
$badSelectorJob = Copy-Object $drainedPodUIDJob
$badSelectorJob.spec | Add-Member -NotePropertyName suspend -NotePropertyValue $false
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightJobContract $badSelectorJob `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig
} 'suspend must be absent even when false'

$completedJob = Copy-Object $drainedPodUIDJob
$completedJob.metadata | Add-Member -NotePropertyName uid -NotePropertyValue '44444444-4444-4444-4444-444444444444'
$completedJob.metadata | Add-Member -NotePropertyName resourceVersion -NotePropertyValue '104'
$completedJob.metadata | Add-Member -NotePropertyName creationTimestamp -NotePropertyValue '2026-07-15T12:00:01Z'
$completedJob.spec | Add-Member -NotePropertyName manualSelector -NotePropertyValue $false
$completedJob.spec | Add-Member -NotePropertyName suspend -NotePropertyValue $false
$completedJob.spec | Add-Member -NotePropertyName selector -NotePropertyValue ([pscustomobject]@{
    matchLabels = [pscustomobject]@{
        'batch.kubernetes.io/controller-uid' = $completedJob.metadata.uid
    }
})
$completedJob | Add-Member -NotePropertyName status -NotePropertyValue ([pscustomobject]@{
    succeeded = 1; completionTime = '2026-07-15T12:00:10Z'
    conditions = @([pscustomobject]@{ type = 'Complete'; status = 'True' })
})
$completedPod = [pscustomobject]@{
    metadata = [pscustomobject]@{
        name = 'pandora-pod-uid-preflight-pod'; namespace = 'pandora'
        uid = '55555555-5555-5555-5555-555555555555'
        creationTimestamp = '2026-07-15T12:00:02Z'
        annotations = Copy-Object $completedJob.spec.template.metadata.annotations
        ownerReferences = @([pscustomobject]@{
            apiVersion = 'batch/v1'; kind = 'Job'; name = $completedJob.metadata.name
            uid = $completedJob.metadata.uid; controller = $true
        })
    }
    spec = Copy-Object $completedJob.spec.template.spec
    status = [pscustomobject]@{
        phase = 'Succeeded'
        containerStatuses = @([pscustomobject]@{
            name = 'pod-uid-release-preflight'
            image = 'registry/pandora/ds-allocator@' + $newDigest
            imageID = 'docker-pullable://registry/pandora/ds-allocator@' + $newDigest
            restartCount = 0
            state = [pscustomobject]@{ terminated = [pscustomobject]@{
                exitCode = 0; signal = 0; reason = 'Completed'
                startedAt = '2026-07-15T12:00:03Z'; finishedAt = '2026-07-15T12:00:09Z'
            } }
        })
    }
}
$runtimeEvidence = Assert-PandoraPodUIDReleasePreflightRuntimeContract $completedJob $completedPod `
    ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig `
    ([datetimeoffset]'2026-07-15T12:00:00Z') ([datetimeoffset]'2026-07-15T12:00:20Z')
Assert-True ($runtimeEvidence.JobUID -ceq $completedJob.metadata.uid -and
    $runtimeEvidence.PodUID -ceq $completedPod.metadata.uid -and
    $runtimeEvidence.CompletionTimeUnixMS -gt 0) 'runtime evidence binds exact Job/Pod UID and completion'
$badStoredJob = Copy-Object $completedJob
$badStoredJob.spec.selector.matchLabels.'batch.kubernetes.io/controller-uid' = `
    '66666666-6666-6666-6666-666666666666'
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightJobContract $badStoredJob `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig
} 'stored Job generated selector must bind exact Job UID'
$badStoredJob = Copy-Object $completedJob
$badStoredJob.spec.manualSelector = $true
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightJobContract $badStoredJob `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig
} 'stored Job cannot enable manualSelector'
$badRuntimePod = Copy-Object $completedPod
$badRuntimePod.metadata.ownerReferences[0].uid = '66666666-6666-6666-6666-666666666666'
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightRuntimeContract $completedJob $badRuntimePod `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig
} 'Pod ownerReference must bind exact Job UID'
$badRuntimePod = Copy-Object $completedPod
$badRuntimePod.status.containerStatuses[0].imageID = 'containerd://sha256:' + ('e' * 64)
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightRuntimeContract $completedJob $badRuntimePod `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig
} 'Pod imageID must match pinned digest'
$badRuntimePod = Copy-Object $completedPod
$badRuntimePod.status.containerStatuses[0].restartCount = 1
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightRuntimeContract $completedJob $badRuntimePod `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig
} 'Pod restart count must remain zero'
$badRuntimePod = Copy-Object $completedPod
$badRuntimePod.spec | Add-Member -NotePropertyName ephemeralContainers -NotePropertyValue @(
    [pscustomobject]@{ name = 'smuggled-debugger' }
)
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightRuntimeContract $completedJob $badRuntimePod `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig
} 'actual Pod cannot contain an injected ephemeral container'
Assert-Throws {
    Assert-PandoraPodUIDReleasePreflightRuntimeContract $completedJob $completedPod `
        ('registry/pandora/ds-allocator@' + $newDigest) 'release-run-01' 'drained' 2 $podUIDDrainedConfig `
        ([datetimeoffset]'2026-07-15T12:00:00Z') ([datetimeoffset]'2026-07-15T12:00:05Z')
} 'Job completion cannot be after switch/CAS bound'

$locatorLive = (($liveGreen | ConvertTo-Json -Depth 40).Replace('ds-allocator', 'player-locator') | ConvertFrom-Json)
$locatorObject = New-PandoraDsAuthCanonicalGreenObject $locatorLive 'player-locator' 'r7' `
    'etcd.pandora.internal' '/pandora/acl-negative/' $false 2 `
    ('registry/pandora/player-locator@' + $newDigest) $newDigest
Assert-PandoraPlayerLocatorPlacementPreflightObjectContract $locatorObject `
    ('registry/pandora/player-locator@' + $newDigest)
$null = Assert-PandoraDsAuthV3GreenStageDeploymentContract $locatorObject 'player-locator' 'r7' `
    'etcd.pandora.internal' '/pandora/acl-negative/' $false
Assert-True ([string]$locatorObject.spec.strategy.type -ceq 'Recreate' -and
    $null -eq $locatorObject.spec.strategy.PSObject.Properties['rollingUpdate'] -and
    @($locatorObject.spec.template.spec.initContainers).Count -eq 1) `
    'prod canonical green player-locator replacement must carry exact Recreate + preflight gate'
$badLocator = Copy-Object $locatorObject
$badLocator.spec.template.spec.initContainers[0].image = 'registry/pandora/player-locator@sha256:' + ('c' * 64)
Assert-Throws {
    Assert-PandoraPlayerLocatorPlacementPreflightObjectContract $badLocator `
        ('registry/pandora/player-locator@' + $newDigest)
} 'canonical green rejects placement preflight digest drift'
$badLocatorLive = Copy-Object $locatorLive
$badLocatorLive.spec.template.spec.volumes = @($badLocatorLive.spec.template.spec.volumes | Where-Object name -cne 'conf')
Assert-Throws {
    New-PandoraDsAuthCanonicalGreenObject $badLocatorLive 'player-locator' 'r7' `
        'etcd.pandora.internal' '/pandora/acl-negative/' $false 2 `
        ('registry/pandora/player-locator@' + $newDigest) $newDigest
} 'canonical green refuses preflight without canonical config source'
$badGreen = Copy-Object $liveGreen
$badGreen.spec.selector.matchLabels | Add-Member -NotePropertyName extra -NotePropertyValue smuggled
Assert-Throws {
    New-PandoraDsAuthCanonicalGreenObject $badGreen 'ds-allocator' 'r7' 'etcd.pandora.internal' '/pandora/acl-negative/' $false 2 ('registry/pandora/ds-allocator@' + $newDigest) $newDigest
} 'canonical green rejects broadened immutable selector'
$dormantPatch = New-PandoraDsAuthDormantBluePatch 'ds-allocator'
$greenServicePatch = New-PandoraDsAuthGreenServicePatch 'ds-allocator'
Assert-True ($dormantPatch.Contains('replicas: 0') -and $dormantPatch.Contains('pandora.dev/ds-auth-writer-epoch: "1"')) 'dormant blue fixed epoch=1/zero'
Assert-True ($greenServicePatch.Contains('pandora.dev/ds-auth-writer-set: green')) 'canonical Service fixed green selector'

$activationSource = Get-Content -LiteralPath (Join-Path $ProjectRoot 'tools/scripts/activate_ds_auth.ps1') -Raw
$policyV3ActivationSource = Get-Content -LiteralPath (Join-Path $ProjectRoot `
    'tools/scripts/activate_hub_successor_policy.ps1') -Raw
$activationContractSource = Get-Content -LiteralPath (Join-Path $ProjectRoot `
    'tools/scripts/lib/ds_auth_activation_contract.ps1') -Raw
Assert-True ($script:PandoraDsAuthRequiredFeatures.Contains(
    'ds_allocator=battle-release-expected-tuple-v1|battle-storage-pod-uid-write-invariant-v1')) `
    'epoch2 activation and ordinary release require the continuous battle-storage pod_uid write invariant capability'
Assert-True ($script:PandoraDsAuthRequiredFeaturesV2 -ceq $script:PandoraDsAuthRequiredFeatures -and
    -not $script:PandoraDsAuthRequiredFeaturesV2.Contains('hub-successor-lease-v1') -and
    $script:PandoraDsAuthRequiredFeaturesV3.Contains('hub-successor-lease-v1')) `
    'V2 feature policy must remain immutable while V3 gets the successor lease feature'
Assert-True ($policyV3ActivationSource.Contains("--policy-v3") -and
    $policyV3ActivationSource.Contains('$script:PandoraDsAuthRequiredFeaturesV3') -and
    $policyV3ActivationSource.Contains('--expected-instances') -and
    $policyV3ActivationSource.Contains('if ($ApplyPolicy) { $result += ''--apply'' }') -and
    $policyV3ActivationSource.Contains('Pre-CAS staged Hub must be Running with a capability lease but NotReady') -and
    $policyV3ActivationSource.IndexOf('Get-CanonicalWriterInstances $true $false', [StringComparison]::Ordinal) -lt
        $policyV3ActivationSource.IndexOf('& go @preCASArgs', [StringComparison]::Ordinal) -and
    $policyV3ActivationSource.IndexOf('--require-v3-activation-record', [StringComparison]::Ordinal) -lt
        $policyV3ActivationSource.IndexOf('$deadline =', [StringComparison]::Ordinal) -and
    $policyV3ActivationSource.IndexOf('$preCASArgs =', [StringComparison]::Ordinal) -lt
        $policyV3ActivationSource.IndexOf('rollout status', [StringComparison]::Ordinal) -and
    $policyV3ActivationSource.LastIndexOf('Get-CanonicalWriterInstances $false $true', [StringComparison]::Ordinal) -gt
        $policyV3ActivationSource.IndexOf('rollout status', [StringComparison]::Ordinal)) `
    'V2->V3 proves pre-CAS Hub endpoint absence, verifies durable record first, then acquired-V3 capabilities and post-CAS readiness'
Assert-True (-not $policyV3ActivationSource.Contains('[string[]]$WriterDeployments') -and
    @('login-ds-auth-green', 'player-locator-ds-auth-green', 'ds-allocator-ds-auth-green',
        'hub-allocator-ds-auth-green', 'battle-result-ds-auth-green').Where({
            $policyV3ActivationSource.Contains($_) }).Count -eq 5 -and
    $policyV3ActivationSource.Contains("foreach (`$service in `$writerSpecs.Keys)")) `
    'V3 orchestrator writer Deployment set must be a fixed exact five and cannot be caller-overridden or empty'
Assert-True ($policyV3ActivationSource.Contains("[uint32]`$snapshot.policy_generation -eq 2") -and
    $policyV3ActivationSource.Contains("[uint32]`$snapshot.policy_generation -eq 3") -and
    $policyV3ActivationSource.Contains('Crash/retry after CAS: never rerun the V2-only NotReady/no-endpoint gate') -and
    $policyV3ActivationSource.Contains("`$statuses.Count -ne 1") -and
    $policyV3ActivationSource.Contains("[string]`$_.name -ceq `$spec.Container") -and
    $policyV3ActivationSource.Contains('readyEndpoints.Count -ne 0') -and
    $policyV3ActivationSource.Contains('readyEndpoints.Count -ne 1') -and
    $policyV3ActivationSource.IndexOf('$snapshotLines =', [StringComparison]::Ordinal) -lt
        $policyV3ActivationSource.IndexOf('$markerJSON =', [StringComparison]::Ordinal) -and
    $policyV3ActivationSource.Contains('$currentInstances = Get-CanonicalWriterInstances $false $false') -and
    $policyV3ActivationSource.Contains('$finalInstances = Get-CanonicalWriterInstances $false $true') -and
    $policyV3ActivationSource.LastIndexOf('& go @finalArgs', [StringComparison]::Ordinal) -gt
        $policyV3ActivationSource.LastIndexOf('$finalInstances =', [StringComparison]::Ordinal) -and
    $policyV3ActivationSource.LastIndexOf('Assert-OrCreateV3CompletionMarker $finalInstances $true', [StringComparison]::Ordinal) -gt
        $policyV3ActivationSource.LastIndexOf('& go @finalArgs', [StringComparison]::Ordinal)) `
    'V3 retry, exact writer-container imageID, endpoint 0/1 cardinality, and final post-readiness acquired audit are hard gates'
Assert-True ($activationSource.Contains("'--min-policy-generation', '1', '--max-policy-generation', '2'") -and
    $activationSource.Contains("@('epoch', 'policy_generation', 'policy_id', 'raw_value', 'mod_revision')") -and
    $activationSource.Contains('[uint32]$snapshot.policy_generation -ne 1') -and
    $activationSource.Contains('[uint32]$snapshot.policy_generation -ne 2')) `
    'legacy V1->V2 script must consume the extended required snapshot contract and reject policy V3'
Assert-True ($activationSource.Contains("[ValidateSet('Audit', 'Activate')][string]`$Phase = 'Audit'")) 'Audit must be default'
Assert-True ($activationSource.Contains('[Parameter(Mandatory = $true)][string]$PodUIDPreflightRedisSecretRevision') -and
    $activationSource.Contains('pandora-pod-uid-preflight-redis-ro-$PodUIDPreflightRedisSecretRevision')) `
    'activation requires an explicit versioned immutable RO Redis Secret revision'
Assert-True (-not $activationSource.Contains('SyntheticProbeScript')) 'arbitrary synthetic script entry removed'
Assert-True ($activationSource.Contains('Ensure-PodUIDReleasePreflightJob') -and
    $activationSource.Contains('Ensure-DrainedPodUIDReleasePreflightJob') -and
    $activationSource.Contains('Ensure-FinalPodUIDReleasePreflightJob') -and
    $activationSource.Contains('Ensure-PodUIDActivationEvidenceMarker') -and
    $activationSource.Contains('Assert-ExistingPodUIDReleasePreflightJob') -and
    $activationSource.Contains('pod_uid release preflight PASSED: run_id=') -and
    $activationSource.Contains('findings=0; no data was modified') -and
    $activationSource.Contains('preflight Job failed closed') -and
    $activationSource.Contains('preflight Job 等待超时')) `
    'strict activation must create/wait/read exact pod_uid Job PASS evidence'
Assert-True ($activationSource.Contains("`$env:GOPROXY = 'off'") -and
    $activationSource.Contains("`$env:GOTOOLCHAIN = 'local'") -and
    $activationSource.Contains('go run -mod=readonly ./cmd/ds_allocator') -and
    $activationSource.Contains('Get-PodUIDConfigCompareSourceSHA256') -and
    $activationSource.Contains('config_helper_source_sha256')) `
    'config comparison helper must run dependency-offline and bind the exact release source hash into evidence'
$preflightCall = $activationSource.LastIndexOf('Ensure-PodUIDReleasePreflightJob', [StringComparison]::Ordinal)
$activationLock = $activationSource.LastIndexOf("Ensure-ActivationMarker `$ActivationLockName", [StringComparison]::Ordinal)
$namespaceFence = $activationSource.LastIndexOf('Set-RequiredNamespaceLabels', [StringComparison]::Ordinal)
Assert-True ($preflightCall -ge 0 -and $preflightCall -lt $activationLock -and $preflightCall -lt $namespaceFence) `
    'prepare pod_uid Job must hard-block before activation lock and namespace/writer epoch mutations'
$emptyCapability = $activationSource.LastIndexOf('Invoke-EmptyCapabilityAudit $candidate', [StringComparison]::Ordinal)
$drainedMarker = $activationSource.LastIndexOf("Ensure-ActivationMarker `$DrainMarkerName", [StringComparison]::Ordinal)
$drainedPreflight = $activationSource.LastIndexOf('Ensure-DrainedPodUIDReleasePreflightJob $drainNotBefore', [StringComparison]::Ordinal)
$greenScale = $activationSource.LastIndexOf('Scale-GreenDeploymentsToDesired $candidate', [StringComparison]::Ordinal)
$epochCAS = $activationSource.LastIndexOf('Invoke-CapabilityAudit $audit -ApplyEpoch', [StringComparison]::Ordinal)
$greenReady = $activationSource.LastIndexOf("Ensure-ActivationMarker `$GreenReadyMarkerName", [StringComparison]::Ordinal)
$finalPreflight = $activationSource.LastIndexOf('Ensure-FinalPodUIDReleasePreflightJob $greenReadyNotBefore', [StringComparison]::Ordinal)
$immutableEvidence = $activationSource.LastIndexOf('Ensure-PodUIDActivationEvidenceMarker $prepareEvidence', [StringComparison]::Ordinal)
$switchWriter = $activationSource.LastIndexOf('Switch-LiveServicesToGreen', [StringComparison]::Ordinal)
$finalEvidenceRead = $activationSource.LastIndexOf('Get-PodUIDActivationEvidenceChain $lockMarker', [StringComparison]::Ordinal)
Assert-True ($emptyCapability -lt $drainedMarker -and $drainedMarker -lt $drainedPreflight -and
    $drainedPreflight -lt $greenScale -and $greenScale -lt $greenReady -and
    $greenReady -lt $finalPreflight -and $finalPreflight -lt $immutableEvidence -and
    $immutableEvidence -lt $switchWriter -and $switchWriter -lt $finalEvidenceRead -and
    $finalEvidenceRead -lt $epochCAS) `
    'prepare/drained/final evidence must linearize around zero-writer/green-ready/switch/CAS'
$idempotentStart = $activationSource.LastIndexOf(
    'if ([uint32]$snapshot.epoch -eq $TargetEpoch) {', [StringComparison]::Ordinal)
$idempotentEnd = $activationSource.IndexOf(
    'if ([uint32]$snapshot.epoch -ne $ExpectedEpoch)', $idempotentStart, [StringComparison]::Ordinal)
$idempotentBody = $activationSource.Substring($idempotentStart, $idempotentEnd - $idempotentStart)
Assert-True ($idempotentBody.Contains('Get-PodUIDActivationEvidenceChain') -and
    -not $idempotentBody.Contains('Ensure-PodUIDReleasePreflightJob') -and
    -not $idempotentBody.Contains('Ensure-DrainedPodUIDReleasePreflightJob') -and
    -not $idempotentBody.Contains('Ensure-FinalPodUIDReleasePreflightJob') -and
    -not $idempotentBody.Contains('Ensure-PodUIDActivationEvidenceMarker')) `
    'required epoch=2 idempotent branch may only read original triple-phase evidence, never recreate it retroactively'
Assert-True ($activationSource.Contains(
        "`$prepareEvidence = Assert-ExistingPodUIDReleasePreflightJob 'prepare'") -and
    $activationSource.Contains('([datetimeoffset]::MinValue) $lockNotAfter') -and
    $activationSource.Contains(
        "`$drainedEvidence = Assert-ExistingPodUIDReleasePreflightJob 'drained'") -and
    $activationSource.Contains(
        "`$finalEvidence = Assert-ExistingPodUIDReleasePreflightJob 'final'") -and
    $activationSource.Contains('switch marker 已存在但 immutable evidence marker 缺失') -and
    $activationSource.Contains('Assert-LiveServiceTrack blue')) `
    'in-progress retries must consume original bounded phase evidence and refuse post-switch evidence fabrication'
$existingLockBranch = $activationSource.IndexOf('if ($null -eq $existingLock) {',
    $activationSource.IndexOf('Get-SyntheticEvidence prepare', [StringComparison]::Ordinal),
    [StringComparison]::Ordinal)
$prepareEvidenceEnd = $activationSource.IndexOf('$configEvidence =', $existingLockBranch,
    [StringComparison]::Ordinal)
$prepareRetryBody = $activationSource.Substring($existingLockBranch,
    $prepareEvidenceEnd - $existingLockBranch)
Assert-True ($prepareRetryBody.Contains('Ensure-PodUIDReleasePreflightJob') -and
    $prepareRetryBody.Contains("Assert-ExistingPodUIDReleasePreflightJob 'prepare'") -and
    $prepareRetryBody.IndexOf('Ensure-PodUIDReleasePreflightJob', [StringComparison]::Ordinal) -lt
        $prepareRetryBody.IndexOf("Assert-ExistingPodUIDReleasePreflightJob 'prepare'", [StringComparison]::Ordinal)) `
    'fresh activation may create prepare evidence, but an existing activation lock forces read-only prepare verification'
$allocatorMainSource = Get-Content -LiteralPath (Join-Path $ProjectRoot `
    'services/battle/ds_allocator/cmd/ds_allocator/main.go') -Raw
$allocatorPreflightSource = Get-Content -LiteralPath (Join-Path $ProjectRoot `
    'services/battle/ds_allocator/cmd/ds_allocator/pod_uid_preflight.go') -Raw
Assert-True ($allocatorMainSource.Contains('if flagPodUIDReleasePreflight {') -and
    $allocatorMainSource.IndexOf('if flagPodUIDReleasePreflight {', [StringComparison]::Ordinal) -lt
        $allocatorMainSource.IndexOf('repo := data.NewRedisBattleRepo', [StringComparison]::Ordinal) -and
    $allocatorPreflightSource.Contains('"pod-uid-release-preflight"') -and
    $allocatorPreflightSource.Contains('pod_uid release preflight PASSED:')) `
    'one-shot ds_allocator command must exit before repo/writer/listener wiring'
Assert-True ($activationSource.Contains("'--required-features', `$script:PandoraDsAuthRequiredFeatures")) 'required features must have one library source'
Assert-True ($activationSource.Contains("'--expected-image-digests', `$Audit.ServiceDigests")) `
    'capability audit must bind each service to its own live template digest'
Assert-True ($activationSource.Contains('ServiceDigests = (($serviceDigests.Keys')) `
    'green contract must emit exact service-to-digest mapping'
$startSource = Get-Content -LiteralPath (Join-Path $ProjectRoot 'tools/scripts/start.ps1') -Raw
Assert-True ($startSource.Contains('--expected-image-digests $State.ExpectedDigests')) `
    'ordinary online release final audit must preserve service-level digest binding'
Assert-True ($startSource.Contains('Get-OnlineDsAuthActivationEvidenceState') -and
    $startSource.Contains('Assert-OnlineDsAuthActivationEvidenceUnchanged') -and
    $startSource.Contains('--activation-evidence-sha256 $ActivationEvidence.PolicyV3EvidenceSHA256') -and
    $startSource.Contains('--activation-evidence-completed-at-ms $ActivationEvidence.PolicyV3CompletedAtUnixMS') -and
    $startSource.Contains('pandora-ds-auth-activation-v2') -and
    $startSource.Contains('$script:PandoraDsAuthPolicyV3EvidenceName') -and
    $startSource.Contains('pandora-ds-auth-policy-v3-complete-$($policyV3Evidence.RunID)') -and
    $startSource.Contains('Assert-PandoraDsAuthV3CompletionContract') -and
    $startSource.Contains('PolicyV3CompletionResourceVersion')) `
    'ordinary release must preserve V2 proof and bind immutable V3 evidence plus completion into its unchanged baseline'
Assert-True ($startSource.Contains("if (`$writer -ceq 'hub-allocator')") -and
    $startSource.Contains('Assert-PandoraHubAllocatorSingleWriterDeploymentContract $deployment')) `
    'ordinary online canonical-state audit must reject a green Hub that is not replicas=1/Recreate'
Assert-True (-not $startSource.Contains('State.ExpectedInstances -cne [string]$ActivationEvidence.PolicyV3ExpectedInstances') -and
    -not $startSource.Contains('State.ExpectedDigests -cne [string]$ActivationEvidence.PolicyV3ExpectedImageDigests')) `
    'one-time V3 staging UIDs/digests are provenance, not a permanent allowlist for rescheduled Pods or later pinned releases'
$initialActivationEvidence = $startSource.IndexOf(
    '$onlineDsAuthActivationEvidence = Get-OnlineDsAuthActivationEvidenceState',
    [StringComparison]::Ordinal)
$initialCapabilityAudit = $startSource.IndexOf(
    '$onlineDsAuthState = Assert-OnlineDsAuthRuntimeAndCapabilities',
    [StringComparison]::Ordinal)
$lockedActivationEvidence = $startSource.IndexOf(
    '$lockedActivationEvidence = Get-OnlineDsAuthActivationEvidenceState',
    [StringComparison]::Ordinal)
$preConfigActivationEvidence = $startSource.IndexOf(
    '$preConfigActivationEvidence = Get-OnlineDsAuthActivationEvidenceState',
    [StringComparison]::Ordinal)
Assert-True ($initialActivationEvidence -ge 0 -and
    $initialActivationEvidence -lt $initialCapabilityAudit -and
    $initialCapabilityAudit -lt $lockedActivationEvidence -and
    $lockedActivationEvidence -lt $preConfigActivationEvidence) `
    'ordinary release must establish activation evidence before first capability audit and never reset the baseline'
$onlineEvidenceFunctionStart = $startSource.IndexOf(
    'function Get-OnlineDsAuthActivationEvidenceState', [StringComparison]::Ordinal)
$onlineEvidenceFunctionEnd = $startSource.IndexOf(
    'function Assert-OnlineDsAuthActivationEvidenceUnchanged', $onlineEvidenceFunctionStart,
    [StringComparison]::Ordinal)
$onlineEvidenceFunction = $startSource.Substring($onlineEvidenceFunctionStart,
    $onlineEvidenceFunctionEnd - $onlineEvidenceFunctionStart)
Assert-True ($onlineEvidenceFunction.Contains('Test-PandoraKubernetesObjectDeleting') -and
    -not $onlineEvidenceFunction.Contains('.metadata.deletionTimestamp')) `
    'healthy immutable markers omit deletionTimestamp and must be checked through the strict-safe deletion helper'
$successorActivationSource = Get-Content -LiteralPath (Join-Path $ProjectRoot `
    'tools/scripts/activate_hub_successor_policy.ps1') -Raw
$successorPrepareSource = Get-Content -LiteralPath (Join-Path $ProjectRoot `
    'tools/scripts/prepare_hub_successor_policy.ps1') -Raw
foreach ($source in @($successorActivationSource, $successorPrepareSource)) {
    Assert-True ($source.Contains('Assert-PandoraDsAuthHttpsEndpoints $EtcdEndpoints') -and
        $source.Contains('Get-PandoraDsAuthSecureGoArgs $CAFile $CertFile $KeyFile') -and
        -not $source.Contains('[switch]$RequireMTLS') -and -not $source.Contains('[switch]$RequireAuth')) `
        'dedicated V2-to-V3 producer/consumer must make HTTPS+mTLS+auth mandatory rather than optional switches'
}
Assert-True ($successorActivationSource.Contains('Assert-PandoraHubAllocatorGreenServiceContract $hubService $Namespace') -and
    $successorPrepareSource.Contains('Assert-PandoraHubAllocatorGreenServiceContract $preStageHubService $Namespace') -and
    $successorPrepareSource.Contains('Assert-PandoraHubAllocatorGreenServiceContract $hubService $Namespace')) `
    'producer and consumer must validate the exact canonical green Hub Service before trusting endpoint cardinality'
$stagePreviewGate = $successorPrepareSource.IndexOf(
    '$null = Assert-PandoraDsAuthV3GreenStageDeploymentContract', [StringComparison]::Ordinal)
$mutatingStageApply = $successorPrepareSource.IndexOf(
    '    & kubectl --context $KubeContext -n $Namespace apply --server-side `', [StringComparison]::Ordinal)
Assert-True ($successorPrepareSource.Contains("'--dry-run=server'") -and
    $successorPrepareSource.Contains("'-o', 'json'") -and
    $stagePreviewGate -ge 0 -and $mutatingStageApply -gt $stagePreviewGate) `
    'producer must validate each server-side merged JSON candidate before the first mutating stage apply'
Assert-True ($successorActivationSource.Contains('Assert-OrCreateV3CompletionMarker $currentInstances $false')) `
    'Audit of an already-required V3 cluster must require and verify the immutable completion marker read-only'
Assert-True ($startSource.Contains(
    'Assert-PandoraPlayerLocatorPlacementPreflightObjectContract $placementGreen ([string]$goPins[''player-locator''])')) `
    'prod ordinary release must read back and verify canonical green placement preflight after rollout'
Assert-True (-not $startSource.Contains('--allowed-image-digests $devDigest')) `
    'fresh local bootstrap must not fabricate capability digest evidence'
Assert-True ($startSource.Contains('--zero-writer-genesis-v3 --apply') -and
    $startSource.Contains('--min-policy-generation 3 --max-policy-generation 3') -and
    $startSource.Contains('--require-v3-activation-record') -and
    $startSource.Contains('zero-writer genesis 单事务 CAS') -and
    $startSource.Contains('--required-features $script:PandoraDsAuthRequiredFeaturesV3 --policy-v3')) `
    'new local/ordinary release paths must require V3 and fresh local must use one crash-safe zero-writer genesis transaction'
Assert-True ($startSource.Contains('function Get-LocalMinikubeImageDigest') -and
    $startSource.Contains("minikube -p `$MinikubeProfile ssh -- docker image inspect -f '{{.Id}}' `$Image") -and
    $startSource.Contains('function Set-LocalDsAuthImageDigestAnnotations') -and
    $startSource.Contains('function Assert-LocalDsAuthImageDigestAnnotations') -and
    $startSource.Contains("`$writers = @('login', 'player-locator', 'ds-allocator', 'hub-allocator', 'battle-result')") -and
    $startSource.Contains("'pandora.dev/image-digest' = `$digest") -and
    $startSource.Contains("Where-Object { [string]`$_.name -ceq `$writer }") -and
    $startSource.Contains("[string]`$statuses[0].imageID).EndsWith(`$digest, [StringComparison]::Ordinal")) `
    'local V3 capability provenance must use the target minikube image config digest and exact writer container'
$localK8sStart = $startSource.IndexOf('function Invoke-K8s {', [StringComparison]::Ordinal)
$localK8sEnd = $startSource.IndexOf('function Invoke-Online {', $localK8sStart, [StringComparison]::Ordinal)
$localK8sBody = $startSource.Substring($localK8sStart, $localK8sEnd - $localK8sStart)
$localApply = $localK8sBody.IndexOf('kubectl @kubectlContextArgs apply -k $servicesDir', [StringComparison]::Ordinal)
$localDigestPatch = $localK8sBody.IndexOf('Set-LocalDsAuthImageDigestAnnotations', [StringComparison]::Ordinal)
$localRollout = $localK8sBody.IndexOf('rollout restart deploy/$($svc.Name)', [StringComparison]::Ordinal)
$localDigestFinal = $localK8sBody.LastIndexOf('Assert-LocalDsAuthImageDigestAnnotations', [StringComparison]::Ordinal)
Assert-True ($localApply -ge 0 -and $localApply -lt $localDigestPatch -and
    $localDigestPatch -lt $localRollout -and $localRollout -lt $localDigestFinal) `
    'local k8s release must patch node-derived writer digests before rollout and verify every running Pod afterward'
$resumeStart = $startSource.IndexOf('function Resume-K8s {', [StringComparison]::Ordinal)
$resumeEnd = $startSource.IndexOf('function Invoke-Resume {', $resumeStart, [StringComparison]::Ordinal)
$resumeBody = $startSource.Substring($resumeStart, $resumeEnd - $resumeStart)
$resumeEtcdReady = $resumeBody.IndexOf("rollout status deploy/etcd", [StringComparison]::Ordinal)
$resumeBaseline = $resumeBody.IndexOf('Assert-LocalDsAuthBaseline', [StringComparison]::Ordinal)
$resumeApply = $resumeBody.IndexOf('kubectl --context $mkCtx apply -k $resumeServicesDir', [StringComparison]::Ordinal)
$resumeDigestPatch = $resumeBody.IndexOf('Set-LocalDsAuthImageDigestAnnotations', [StringComparison]::Ordinal)
$resumeRollout = $resumeBody.IndexOf('rollout restart deploy/$($svc.Name)', [StringComparison]::Ordinal)
Assert-True ($resumeEtcdReady -ge 0 -and $resumeEtcdReady -lt $resumeBaseline -and
    $resumeBaseline -lt $resumeApply -and $resumeApply -lt $resumeDigestPatch -and
    $resumeDigestPatch -lt $resumeRollout -and
    [regex]::Matches($resumeBody, 'Set-LocalDsAuthImageDigestAnnotations').Count -eq 1 -and
    $resumeBody.Contains('Assert-LocalDsAuthImageDigestAnnotations')) `
    'Resume must wait etcd before baseline, apply Recreate before repairing node-derived writer provenance, then rollout and verify final Pods'
Assert-True ($startSource.Contains("get deployment/hub-allocator -o json") -and
    $startSource.Contains("[string]`$hubDeployment.spec.strategy.type -cne 'Recreate'") -and
    $startSource.Contains("`$hubDeployment.spec.strategy.PSObject.Properties['rollingUpdate']")) `
    'local digest template patches must refuse Hub unless replicas=1 Recreate with no rollingUpdate'
$auditBranch = $activationSource.IndexOf("if (`$Phase -ceq 'Audit')", [StringComparison]::Ordinal)
$firstApply = $activationSource.IndexOf('Invoke-CapabilityAudit $audit -ApplyEpoch', [StringComparison]::Ordinal)
Assert-True ($auditBranch -ge 0 -and $firstApply -gt $auditBranch) 'CAS apply must be after Audit early exit'
$drain = $activationSource.LastIndexOf('Scale-BlueDeploymentsToZero', [StringComparison]::Ordinal)
$empty = $activationSource.LastIndexOf('Invoke-EmptyCapabilityAudit $candidate', [StringComparison]::Ordinal)
$green = $activationSource.LastIndexOf('Scale-GreenDeploymentsToDesired $candidate', [StringComparison]::Ordinal)
$switch = $activationSource.LastIndexOf('Switch-LiveServicesToGreen', [StringComparison]::Ordinal)
Assert-True ($drain -lt $empty -and $empty -lt $green -and $green -lt $switch -and $switch -lt $firstApply) 'activation must be zero-overlap blue drain -> empty capability -> green -> service -> CAS'
Assert-True ([regex]::Matches($activationSource,
    '@\(\$service\.spec\.selector\.PSObject\.Properties\)\.Count\s+-ne\s+3').Count -ge 3) `
    'preview/live/switch Service gates must reject extra selectors'
Assert-True ($activationSource.Contains("op = 'replace'; path = '/spec/selector'") -and
    $activationSource.Contains("op = 'test'; path = '/metadata/resourceVersion'") -and
    $activationSource.Contains("'--type=json'")) `
    'live Service switch must CAS-replace the complete selector map'
Assert-True (-not $activationSource.Contains("`$selectorPatch = @{ `$WriterSetLabel = 'green'")) `
    'live Service switch must not merge a partial selector'

Assert-True ($activationSource.Contains("`$RequiredPolicyV2 = 'ds-auth-v2-pod-uid-write-invariant-v1'") -and
    $activationSource.Contains('$TargetRequiredValue = "2@$RequiredPolicyV2"') -and
    $activationSource.Contains("[string]`$snapshot.raw_value -cne `$TargetRequiredValue") -and
    $activationSource.Contains("[string]`$snapshot.policy_id -cne `$RequiredPolicyV2")) `
    'PowerShell activation must bind and exact-check the versioned etcd required policy value'
$topologyBeforePrepare = $activationSource.LastIndexOf(
    "Assert-RedisTopologyChangeLockProvider `$prepareConfigEvidence 'before-prepare'", [StringComparison]::Ordinal)
$freshTopologyGate = $activationSource.LastIndexOf(
    "Assert-RedisTopologyChangeLockProvider `$topologyGateEvidence 'before-prepare'", [StringComparison]::Ordinal)
$firstActivationWrite = $activationSource.LastIndexOf(
    'Ensure-RawPodUIDConfigSnapshot $sourceEvidence', [StringComparison]::Ordinal)
$topologyBeforeCAS = $activationSource.LastIndexOf(
    "Assert-RedisTopologyChangeLockProvider `$activationEvidence.Config 'before-cas'", [StringComparison]::Ordinal)
Assert-True ($activationContractSource.Contains('callers may not replace it with a ConfigMap marker') -and
    $activationSource.Contains('Assert-PandoraRedisTopologyChangeLockProvider $Checkpoint') -and
    $topologyBeforePrepare -ge 0 -and $topologyBeforePrepare -lt $preflightCall -and
    $freshTopologyGate -ge 0 -and $freshTopologyGate -lt $firstActivationWrite -and
    $topologyBeforeCAS -gt $finalEvidenceRead -and $topologyBeforeCAS -lt $epochCAS) `
    'missing authoritative Redis topology lock provider must fail closed before any fresh write/prepare and immediately before CAS'
$cleanupRequired = $activationSource.LastIndexOf(
    'Ensure-PodUIDACLCleanupRequiredMarker $preSwitchActivationEvidence', [StringComparison]::Ordinal)
$cleanupAfterCAS = $activationSource.LastIndexOf(
    'Complete-PodUIDACLPostCASCleanup $activationEvidence $switchMarker', [StringComparison]::Ordinal)
Assert-True ($cleanupRequired -gt $immutableEvidence -and $cleanupRequired -lt $switchWriter -and
    $cleanupAfterCAS -gt $epochCAS -and
    $activationSource.Contains('--admin-username-env $RedisACLAdminUsernameEnv') -and
    $activationSource.Contains('--admin-password-env $RedisACLAdminPasswordEnv') -and
    $activationSource.Contains('ACL cleanup 仍为 PENDING') -and
    $activationSource.Contains('Assert-PandoraPodUIDACLCleanupTimeline')) `
    'temporary Redis ACL user lifecycle must be required pre-switch, deleted/read-back post-CAS, and recoverably pending'
Assert-True ($idempotentBody.Contains('Complete-PodUIDACLPostCASCleanup') -and
    $idempotentBody.Contains('RECOVERY COMPLETE, RELEASE STILL BLOCKED')) `
    'epoch2 Activate retry may perform only the idempotent ACL cleanup recovery and must retain topology release blocker'
Assert-True ($startSource.Contains('pandora-pod-uid-acl-cleanup-required-v2-$runID') -and
    $startSource.Contains('pandora-pod-uid-acl-cleanup-complete-v2-$runID') -and
    $startSource.Contains('CleanupRequiredResourceVersion') -and
    $startSource.Contains('CleanupCompleteResourceVersion') -and
    $startSource.Contains('CleanupProofSHA256')) `
    'ordinary release must lock both post-CAS cleanup markers and their immutable UID/resourceVersion/proof state'
$onlineFunctionStart = $startSource.IndexOf('function Invoke-Online {', [StringComparison]::Ordinal)
$onlineFunctionEnd = $startSource.IndexOf('function Build-AllImages', $onlineFunctionStart, [StringComparison]::Ordinal)
$onlineFunction = $startSource.Substring($onlineFunctionStart, $onlineFunctionEnd - $onlineFunctionStart)
$onlineTopologyBlock = $onlineFunction.IndexOf(
    'Assert-PandoraRedisTopologyChangeLockProvider', [StringComparison]::Ordinal)
$onlineFirstBuild = $onlineFunction.IndexOf('Build-AllImages', [StringComparison]::Ordinal)
$onlineFirstApply = $onlineFunction.IndexOf('kubectl @kubectlContextArgs apply', [StringComparison]::Ordinal)
Assert-True ($onlineTopologyBlock -ge 0 -and
    ($onlineFirstBuild -lt 0 -or $onlineTopologyBlock -lt $onlineFirstBuild) -and
    ($onlineFirstApply -lt 0 -or $onlineTopologyBlock -lt $onlineFirstApply) -and
    $onlineFunction.Contains('online-before-build-push-apply')) `
    'ordinary online release must share the topology provider verifier and block before build/push/apply'

Write-Host 'ds_auth_activation_contract_test: PASS'
