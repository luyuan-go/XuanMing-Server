[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
. (Join-Path $ProjectRoot 'tools/scripts/lib/online_manifest_contract.ps1')
. (Join-Path $ProjectRoot 'tools/scripts/lib/dsticket_rotation_contract.ps1')
Set-StrictMode -Off

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

function Assert-Throws([scriptblock]$Action, [string]$Message) {
    try { & $Action } catch { return }
    throw "ASSERT FAILED:应抛错但成功:$Message"
}

function Assert-ThrowsMatching([scriptblock]$Action, [string]$Pattern, [string]$Message) {
    try { & $Action } catch {
        if ($_.Exception.Message -notmatch $Pattern) {
            throw "ASSERT FAILED:异常未命中 '$Pattern':$Message；actual=$($_.Exception.Message)"
        }
        return
    }
    throw "ASSERT FAILED:应抛错但成功:$Message"
}

function Assert-ThrowsWithoutValue([scriptblock]$Action, [string[]]$ForbiddenValues, [string]$Message) {
    try { & $Action } catch {
        $errorText = $_.Exception.Message
        foreach ($value in $ForbiddenValues) {
            if (-not [string]::IsNullOrEmpty($value) -and $errorText.Contains($value)) {
                throw "ASSERT FAILED:异常泄漏密钥值:$Message"
            }
        }
        return
    }
    throw "ASSERT FAILED:应抛错但成功:$Message"
}

function New-TestHmacConfigs {
    param(
        [string]$PlayerSecret,
        [string]$DsSecret,
        [string]$PlayerAdditional = '',
        [string]$DsAdditional = ''
    )
    $playerAdditionalLine = if ([string]::IsNullOrEmpty($PlayerAdditional)) { '' } else {
        "`n  additional_secrets: [`"$PlayerAdditional`"]"
    }
    $dsAdditionalLine = if ([string]::IsNullOrEmpty($DsAdditional)) { '' } else {
        "`n  additional_secrets: [`"$DsAdditional`"]"
    }
    return @{
        login = "login:`n  jwt:`n    secret: `"$PlayerSecret`"$($playerAdditionalLine.Replace("`n  ", "`n    "))`n  ds_auth:`n    secret: `"$DsSecret`"$($dsAdditionalLine.Replace("`n  ", "`n    "))"
        matchmaker = "jwt:`n  secret: `"$PlayerSecret`"$playerAdditionalLine"
        'matchmaker-pve' = "jwt:`n  secret: `"$PlayerSecret`"$playerAdditionalLine"
        'hub-allocator' = "jwt:`n  secret: `"$PlayerSecret`"$playerAdditionalLine`nds_auth:`n  secret: `"$DsSecret`"$dsAdditionalLine"
        'ds-allocator' = "ds_auth:`n  secret: `"$DsSecret`"$dsAdditionalLine"
        'battle-result' = "ds_auth:`n  secret: `"$DsSecret`"$dsAdditionalLine"
        'player-locator' = "ds_auth:`n  secret: `"$DsSecret`"$dsAdditionalLine"
        # player 2026-08-13 入列(verify-only:GetLoadout 经 :8444 暴露,须验 DS 回调令牌)。
        player = "ds_auth:`n  secret: `"$DsSecret`"$dsAdditionalLine"
        # team/guild 2026-08-17 同因入列(verify-only:GetPlayerTeam / 公会归属反查暴露在 DS 面)。
        team = "ds_auth:`n  secret: `"$DsSecret`"$dsAdditionalLine"
        guild = "ds_auth:`n  secret: `"$DsSecret`"$dsAdditionalLine"
    }
}

function New-TestOrdinaryFixedConfig([string]$Kid, [int]$Revision) {
    $data = [ordered]@{}
    foreach ($service in @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')) {
        $text = if ($service -ceq 'login') {
            "login:`n  ds_ticket:`n    private_key_file: `"/run/secrets/pandora-dsticket/private.pem`"`n" +
            "    active_kid: `"$Kid`"`n    ttl: `"120s`"`n" +
            "    jwks_file: `"/run/config/pandora-dsticket/jwks.json`"`n" +
            "    keyset_revision: `"$Revision`"`nserver:`n  addr: 0.0.0.0"
        } else {
            "ds_ticket:`n  private_key_file: `"/run/secrets/pandora-dsticket/private.pem`"`n" +
            "  active_kid: `"$Kid`"`n  ttl: `"120s`"`nserver:`n  addr: 0.0.0.0"
        }
        $data["$service.yaml"] = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($text))
    }
    $data['unrelated.yaml'] = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('keep: true'))
    return [pscustomobject]@{
        kind = 'Secret'
        metadata = [pscustomobject]@{ name = 'pandora-config'; namespace = 'pandora'; resourceVersion = '77' }
        data = [pscustomobject]$data
    }
}

function New-TestOrdinaryParentDeployment {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [string]$DirectSecret = '',
        [string]$ProjectedSecret = '',
        [string]$InitEnvSecret = '',
        [string]$ConfigSecret = '',
        [switch]$Paused
    )
    $volumes = [System.Collections.Generic.List[object]]::new()
    if (-not [string]::IsNullOrWhiteSpace($DirectSecret)) {
        $volumes.Add([pscustomobject]@{ name = 'dsticket'; secret = [pscustomobject]@{ secretName = $DirectSecret } })
    }
    if (-not [string]::IsNullOrWhiteSpace($ProjectedSecret)) {
        $volumes.Add([pscustomobject]@{
            name = 'projected-material'
            projected = [pscustomobject]@{ sources = @([pscustomobject]@{
                secret = [pscustomobject]@{ name = $ProjectedSecret }
            }) }
        })
    }
    if (-not [string]::IsNullOrWhiteSpace($ConfigSecret)) {
        $volumes.Add([pscustomobject]@{ name = 'conf'; secret = [pscustomobject]@{ secretName = $ConfigSecret } })
    }
    $initContainers = @()
    if (-not [string]::IsNullOrWhiteSpace($InitEnvSecret)) {
        $initContainers = @([pscustomobject]@{
            name = 'material-loader'
            env = @([pscustomobject]@{
                name = 'PRIVATE_KEY'
                valueFrom = [pscustomobject]@{
                    secretKeyRef = [pscustomobject]@{ name = $InitEnvSecret; key = 'private.pem' }
                }
            })
        })
    }
    return [pscustomobject]@{
        kind = 'Deployment'
        metadata = [pscustomobject]@{ name = $Name; namespace = 'pandora'; labels = [pscustomobject]@{} }
        spec = [pscustomobject]@{
            replicas = 0
            paused = [bool]$Paused
            template = [pscustomobject]@{
                metadata = [pscustomobject]@{ labels = [pscustomobject]@{} }
                spec = [pscustomobject]@{
                    containers = @([pscustomobject]@{ name = 'unrelated-worker'; env = @(); volumeMounts = @() })
                    initContainers = $initContainers
                    volumes = $volumes.ToArray()
                }
            }
        }
    }
}

function New-TestOrdinaryParentFleet {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [string]$ContainerName = 'third-party-ds',
        [switch]$DSTicketEnv,
        [switch]$JWKSVolume
    )
    $env = if ($DSTicketEnv) {
        @([pscustomobject]@{ name = 'PANDORA_DSTICKET_KEYSET_REVISION'; value = '7' })
    } else { @() }
    $volumes = if ($JWKSVolume) {
        @([pscustomobject]@{
            name = 'dsticket-jwks'
            configMap = [pscustomobject]@{ name = 'pandora-dsticket-jwks-r7' }
        })
    } else { @() }
    return [pscustomobject]@{
        apiVersion = 'agones.dev/v1'
        kind = 'Fleet'
        metadata = [pscustomobject]@{ name = $Name; namespace = 'default'; labels = [pscustomobject]@{} }
        spec = [pscustomobject]@{
            replicas = 0
            template = [pscustomobject]@{
                metadata = [pscustomobject]@{ labels = [pscustomobject]@{} }
                spec = [pscustomobject]@{
                    template = [pscustomobject]@{
                        metadata = [pscustomobject]@{ labels = [pscustomobject]@{} }
                        spec = [pscustomobject]@{
                            containers = @([pscustomobject]@{
                                name = $ContainerName; env = $env; volumeMounts = @()
                            })
                            volumes = $volumes
                        }
                    }
                }
            }
        }
    }
}

function New-TestSignerChildSpec {
    param([Parameter(Mandatory = $true)][ValidateSet(
        'None', 'Projected', 'EnvFrom', 'InitEnv', 'EphemeralEnv', 'ForbiddenInitEnv', 'ForbiddenEphemeralOct'
    )][string]$ReferenceKind)
    $containers = @([pscustomobject]@{ name = 'renamed-worker'; env = @(); envFrom = @(); volumeMounts = @() })
    $initContainers = @()
    $ephemeralContainers = @()
    $volumes = @()
    switch ($ReferenceKind) {
        'Projected' {
            $volumes = @([pscustomobject]@{
                name = 'renamed-material'
                projected = [pscustomobject]@{ sources = @([pscustomobject]@{
                    secret = [pscustomobject]@{ name = 'pandora-dsticket-signer-r6' }
                }) }
            })
        }
        'EnvFrom' {
            $containers[0].envFrom = @([pscustomobject]@{
                secretRef = [pscustomobject]@{ name = 'pandora-dsticket-signer-r6' }
            })
        }
        'InitEnv' {
            $initContainers = @([pscustomobject]@{
                name = 'renamed-init'
                env = @([pscustomobject]@{
                    name = 'PRIVATE_KEY'
                    valueFrom = [pscustomobject]@{
                        secretKeyRef = [pscustomobject]@{ name = 'pandora-dsticket-signer-r6'; key = 'private.pem' }
                    }
                })
                envFrom = @()
                volumeMounts = @()
            })
        }
        'EphemeralEnv' {
            $ephemeralContainers = @([pscustomobject]@{
                name = 'renamed-debugger'
                env = @([pscustomobject]@{
                    name = 'PRIVATE_KEY'
                    valueFrom = [pscustomobject]@{
                        secretKeyRef = [pscustomobject]@{ name = 'pandora-dsticket-signer-r6'; key = 'private.pem' }
                    }
                })
                envFrom = @()
                volumeMounts = @()
            })
        }
        'ForbiddenInitEnv' {
            $initContainers = @([pscustomobject]@{
                name = 'renamed-init'
                env = @([pscustomobject]@{ name = 'PANDORA_PLAYER_JWT_SECRET'; value = 'forbidden-test-value' })
                envFrom = @()
                volumeMounts = @()
            })
        }
        'ForbiddenEphemeralOct' {
            $ephemeralContainers = @([pscustomobject]@{
                name = 'renamed-debugger'
                env = @([pscustomobject]@{ name = 'UNRELATED'; value = '{"kty":"oct"}' })
                envFrom = @()
                volumeMounts = @([pscustomobject]@{ name = 'renamed-key'; mountPath = '/etc/keys/private.pem' })
            })
        }
    }
    return [pscustomobject]@{
        containers = $containers
        initContainers = $initContainers
        ephemeralContainers = $ephemeralContainers
        volumes = $volumes
    }
}

function New-TestVerifierChildSpec {
    param([Parameter(Mandatory = $true)][ValidateSet('None', 'Projected', 'Env', 'ConfigMapKeyRef', 'EnvFrom')][string]$ReferenceKind)
    $containers = @([pscustomobject]@{ name = 'renamed-verifier'; env = @(); envFrom = @(); volumeMounts = @() })
    $volumes = @()
    switch ($ReferenceKind) {
        'Projected' {
            $volumes = @([pscustomobject]@{
                name = 'renamed-public-material'
                projected = [pscustomobject]@{ sources = @([pscustomobject]@{
                    configMap = [pscustomobject]@{ name = 'pandora-dsticket-jwks-r6' }
                }) }
            })
        }
        'Env' {
            $containers[0].env = @([pscustomobject]@{ name = 'PANDORA_DSTICKET_KEYSET_REVISION'; value = '6' })
        }
        'ConfigMapKeyRef' {
            $containers[0].env = @([pscustomobject]@{
                name = 'PUBLIC_JWKS'
                valueFrom = [pscustomobject]@{
                    configMapKeyRef = [pscustomobject]@{ name = 'pandora-dsticket-jwks-r6'; key = 'jwks.json' }
                }
            })
        }
        'EnvFrom' {
            $containers[0].envFrom = @([pscustomobject]@{
                configMapRef = [pscustomobject]@{ name = 'pandora-dsticket-jwks-r6' }
            })
        }
    }
    return [pscustomobject]@{ containers = $containers; initContainers = @(); ephemeralContainers = @(); volumes = $volumes }
}

function New-TestChildReplicaSet([string]$Name, $PodSpec) {
    return [pscustomobject]@{
        kind = 'ReplicaSet'
        metadata = [pscustomobject]@{
            name = $Name; namespace = 'pandora'; uid = "uid-$Name"; labels = [pscustomobject]@{}; ownerReferences = @()
        }
        spec = [pscustomobject]@{
            replicas = 0
            template = [pscustomobject]@{ metadata = [pscustomobject]@{ labels = [pscustomobject]@{} }; spec = $PodSpec }
        }
        status = [pscustomobject]@{ replicas = 0; readyReplicas = 0 }
    }
}

function New-TestChildGameServerSet {
    param([string]$Name, [int]$Replicas, $PodSpec, [switch]$DirectSchema)
    $template = if ($DirectSchema) {
        [pscustomobject]@{ spec = $PodSpec }
    } else {
        [pscustomobject]@{
            spec = [pscustomobject]@{ template = [pscustomobject]@{ spec = $PodSpec } }
        }
    }
    return [pscustomobject]@{
        kind = 'GameServerSet'
        metadata = [pscustomobject]@{
            name = $Name; namespace = 'default'; uid = "uid-$Name"; labels = [pscustomobject]@{}; ownerReferences = @()
        }
        spec = [pscustomobject]@{
            replicas = $Replicas
            template = $template
        }
        status = [pscustomobject]@{ replicas = $Replicas; readyReplicas = 0 }
    }
}

function New-TestChildPod([string]$Name, [string]$Namespace, [string]$App, $PodSpec) {
    return [pscustomobject]@{
        kind = 'Pod'
        metadata = [pscustomobject]@{
            name = $Name; namespace = $Namespace; uid = "uid-$Name"
            labels = [pscustomobject]@{ app = $App }; ownerReferences = @()
        }
        spec = $PodSpec
    }
}

function Assert-OnlineHmacGateOrdering([string]$OnlineSource) {
    $liveRead = $OnlineSource.IndexOf('get secret/pandora-config', [StringComparison]::Ordinal)
    $generate = $OnlineSource.IndexOf('& "$ScriptDir/gen_cluster_config.ps1" @genArgs', [StringComparison]::Ordinal)
    $continuity = $OnlineSource.IndexOf('Assert-PandoraOnlineHmacContinuity', [StringComparison]::Ordinal)
    $abortContinuity = $OnlineSource.IndexOf('Assert-PandoraOnlineAllocationAbortAuthContinuity', [StringComparison]::Ordinal)
    $teamContinuity = $OnlineSource.IndexOf('Assert-PandoraOnlineTeamResumeAuthContinuity', [StringComparison]::Ordinal)
    $buildPush = $OnlineSource.IndexOf('if ($BuildPush)', [StringComparison]::Ordinal)
    $runtimeOverlay = $OnlineSource.IndexOf('New-OnlineRuntimeOverlay', [StringComparison]::Ordinal)
    $configApply = $OnlineSource.IndexOf('Apply-PandoraConfigSecret', [StringComparison]::Ordinal)
    foreach ($position in @($liveRead, $generate, $continuity, $abortContinuity, $teamContinuity, $buildPush, $runtimeOverlay, $configApply)) {
        Assert-True ($position -ge 0) 'online HMAC 门禁顺序所需 marker 必须存在'
    }
    Assert-True ($liveRead -lt $generate) '必须先读取 live pandora-config 再生成候选配置'
    Assert-True ($generate -lt $continuity) '必须比较实际生成的候选配置'
    Assert-True ($continuity -lt $buildPush) 'HMAC 连续性门禁必须早于 BuildPush'
    Assert-True ($abortContinuity -gt $generate -and $abortContinuity -lt $buildPush) `
        'allocation abort service key 连续性门禁必须在候选生成后、BuildPush 前执行'
    Assert-True ($teamContinuity -gt $generate -and $teamContinuity -lt $buildPush) `
        'Team resume service key 连续性门禁必须在候选生成后、BuildPush 前执行'
    Assert-True ($continuity -lt $runtimeOverlay -and $continuity -lt $configApply) `
        'HMAC 连续性门禁必须早于 runtime overlay 与集群配置写入'
}

function Get-TestContractRows([string]$Manifest, [switch]$Fleet, [switch]$DSTicket,
    [switch]$HubAllocatorSingleWriter) {
    $tmp = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-contract-test-' + [guid]::NewGuid().ToString('N') + '.yaml')
    try {
        # 只把本次 jsonpath 需要的 Kind 交给 kubectl client parser；online render 还包含
        # Istio CRD，测试机未安装 CRD 时不应让无关 discovery 阻断 Deployment/Fleet 纯结构契约。
        $wantedKind = if ($Fleet) { 'Fleet' } else { 'Deployment' }
        $documents = @([regex]::Split($Manifest, '(?m)^---\s*$') | Where-Object {
            $_ -cmatch ('(?m)^kind:\s*' + [regex]::Escape($wantedKind) + '\s*$')
        })
        if ($documents.Count -eq 0) { throw "测试 manifest 缺 Kind=$wantedKind。" }
        [System.IO.File]::WriteAllText($tmp, ($documents -join "`n---`n"), [System.Text.UTF8Encoding]::new($false))
        $jsonPath = if ($Fleet) {
            '{.kind}{"\t"}{.metadata.name}{"\t"}{.spec.template.spec.template.spec.containers[*].name}{"\t"}{.spec.template.spec.template.spec.containers[*].image}{"\t"}{.spec.template.spec.template.spec.containers[*].imagePullPolicy}{"\t"}{.spec.template.metadata.annotations.pandora\.dev/image-digest}{"\t"}{.spec.template.spec.template.metadata.annotations.pandora\.dev/image-digest}{"\t"}{.metadata.labels.pandora\.dev/release-track}{"\t"}{.spec.template.metadata.labels.pandora\.dev/release-track}{"\t"}{.spec.template.metadata.annotations.pandora\.dev/release-track}{"\t"}{.spec.template.spec.template.metadata.labels.pandora\.dev/release-track}{"\t"}{.spec.template.spec.template.metadata.annotations.pandora\.dev/release-track}{"\n"}'
        } elseif ($DSTicket) {
            '{.kind}{"\t"}{.metadata.name}{"\t"}{.spec.template.spec.volumes[?(@.name=="dsticket")].secret.secretName}{"\t"}{.spec.template.spec.volumes[?(@.name=="dsticket-jwks")].configMap.name}{"\n"}'
        } elseif ($HubAllocatorSingleWriter) {
            '{.kind}{"\t"}{.metadata.name}{"\t"}{.spec.replicas}{"\t"}{.spec.strategy.type}{"\t"}{.spec.strategy.rollingUpdate}{"\n"}'
        } else {
            '{.kind}{"\t"}{.metadata.name}{"\t"}{.spec.template.spec.containers[*].name}{"\t"}{.spec.template.spec.containers[*].image}{"\t"}{.spec.template.metadata.annotations.pandora\.dev/image-digest}{"\n"}'
        }
        $lines = @(& kubectl create --dry-run=client --validate=false -f $tmp -o "jsonpath=$jsonPath" 2>&1)
        if ($LASTEXITCODE -ne 0) { throw "kubectl client parse 失败:$($lines -join [Environment]::NewLine)" }
        return @($lines | ForEach-Object { $_.ToString() })
    } finally {
        Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
    }
}

$services = @(
    'login', 'player', 'data-service', 'friend', 'chat', 'guild', 'mail', 'player-locator',
    'leaderboard', 'owner', 'team', 'matchmaker', 'matchmaker-pve', 'trade', 'dialogue', 'mission', 'push',
    'inventory', 'auction', 'ds-allocator', 'hub-allocator', 'battle-result')
$writers = @('login', 'player-locator', 'ds-allocator', 'hub-allocator', 'battle-result')
$digests = @{}
for ($i = 0; $i -lt $services.Count; $i++) {
    $digests[$services[$i]] = 'sha256:' + ($i + 1).ToString('x').PadLeft(64, '0')
}

Assert-True ((Get-PandoraImageRepository 'registry.example.com:5000/pandora/login:v1') -ceq 'registry.example.com:5000/pandora/login') '带端口 registry 去 tag'
$pin = New-PandoraPinnedImageReference 'registry.example.com:5000/pandora/login:v1' $digests.login
Assert-True ($pin -ceq ('registry.example.com:5000/pandora/login@' + $digests.login)) '生成 digest pin'
Assert-PandoraImmutableReleaseTag 'v1.2.3-b5a5a95'
Assert-Throws { Assert-PandoraImmutableReleaseTag 'latest' } '拒绝 latest'
Assert-Throws { Assert-PandoraImmutableReleaseTag 'v1.2.3' } 'tag 必须含 git SHA'
Assert-PandoraImmutableReleaseTag 'release-b5a5a95' -CurrentCommit 'b5a5a95' -RequireCurrentCommit
Assert-Throws { Assert-PandoraImmutableReleaseTag 'release-aaaaaaa' -CurrentCommit 'b5a5a95' -RequireCurrentCommit } 'BuildPush 必须绑定当前 commit'

$inspectDigest = 'sha256:' + ('d' * 64)
$inspect = @"
Name: registry.example.com/pandora/login:v1
MediaType: application/vnd.oci.image.manifest.v1+json
Digest: $inspectDigest
"@
$descriptor = ConvertFrom-PandoraImagetoolsInspect -Reference 'registry.example.com/pandora/login:v1' -Output $inspect
Assert-True ($descriptor.Digest -ceq $inspectDigest) '解析 registry 单平台 digest'
Assert-True ($descriptor.Pinned -ceq ('registry.example.com/pandora/login@' + $inspectDigest)) 'registry digest 转 pin'
Assert-Throws {
    ConvertFrom-PandoraImagetoolsInspect -Reference 'registry.example.com/pandora/login:v1' -Output ($inspect.Replace(
        'application/vnd.oci.image.manifest.v1+json', 'application/vnd.oci.image.index.v1+json'))
} '拒绝多平台 image index'
Assert-Throws {
    ConvertFrom-PandoraImagetoolsInspect -Reference 'registry.example.com/pandora/login:v1' -Output ($inspect + "`nDigest: $($digests.player)")
} '拒绝多个顶层 digest'
Assert-True (Test-PandoraManifestNotFoundOutput -Output 'ERROR: manifest unknown') '识别 manifest 不存在'
Assert-True (-not (Test-PandoraManifestNotFoundOutput -Output 'ERROR: unauthorized: authentication required')) '鉴权失败不能当作 tag 不存在'
Assert-True (-not (Test-PandoraManifestNotFoundOutput -Output 'ERROR: unauthorized; manifest unknown')) '混合鉴权错误不能被 manifest unknown 覆盖'
Assert-True ((Get-PandoraPushDigest -Output "pushed`ndigest: $inspectDigest size: 123") -ceq $inspectDigest) '解析 push digest'
Assert-Throws { Get-PandoraPushDigest -Output 'push completed without digest' } 'push 缺 digest'
Assert-Throws {
    Get-PandoraPushDigest -Output "digest: $inspectDigest`ndigest: $($digests.player)"
} 'push 返回不同 digest'
Assert-PandoraCleanGitStatus -Output ''
Assert-Throws { Assert-PandoraCleanGitStatus -Output ' M services/account/login/main.go' } '脏 worktree 禁止发布'
Assert-PandoraImageRevision -Reference 'pandora/login:dev' -Actual 'b5a5a95' -Expected 'b5a5a95'
Assert-Throws {
    Assert-PandoraImageRevision -Reference 'pandora/login:dev' -Actual 'aaaaaaa' -Expected 'b5a5a95'
} '旧镜像 revision 禁止冒充当前 commit'
$loginRevisionPatch = New-PandoraLoginDSTicketJWKSRevisionPatch -Revision 7
Assert-True ($loginRevisionPatch.Contains('name: pandora-dsticket-jwks-r7')) `
    'online runtime overlay 必须把 Login 公钥卷指向本次 DSTicket revision'
$signerRevisionPatch = New-PandoraDSTicketSignerSecretRevisionPatch -Service matchmaker -Revision 7
Assert-True ($signerRevisionPatch.Contains('secretName: pandora-dsticket-signer-r7')) `
    'online runtime overlay 必须把四个 signer 私钥卷指向本次 revisioned Secret'
$liveDeploymentsR7 = @(
    "login`tpandora-config`tpandora-dsticket-signer-r7`tpandora-dsticket-jwks-r7",
    "matchmaker`tpandora-config`tpandora-dsticket-signer-r7`t",
    "matchmaker-pve`tpandora-config`tpandora-dsticket-signer-r7`t",
    "hub-allocator`tpandora-config`tpandora-dsticket-signer-r7`t"
)
$liveFleetsR7 = @(
    "pandora-battle-stable`t7`tpandora-dsticket-jwks-r7",
    "pandora-battle-canary`t7`tpandora-dsticket-jwks-r7",
    "pandora-hub-stable`t7`tpandora-dsticket-jwks-r7",
    "pandora-hub-canary`t7`tpandora-dsticket-jwks-r7"
)
Assert-PandoraOrdinaryReleaseDSTicketRevision -DeploymentRows @() -FleetRows @() -RequestedRevision 7
Assert-PandoraOrdinaryReleaseDSTicketRevision -DeploymentRows $liveDeploymentsR7 -FleetRows $liveFleetsR7 `
    -RequestedRevision 7
Assert-Throws {
    Assert-PandoraOrdinaryReleaseDSTicketRevision -DeploymentRows $liveDeploymentsR7 -FleetRows $liveFleetsR7 `
        -RequestedRevision 8
} '普通发布不得冒充分阶段轮换切 revision'
Assert-Throws {
    Assert-PandoraOrdinaryReleaseDSTicketRevision -DeploymentRows $liveDeploymentsR7 -FleetRows @() `
        -RequestedRevision 7
} '部分存在的 signer/Fleet 运行态必须阻断'
Assert-Throws {
    $split = @($liveFleetsR7)
    $split[0] = "pandora-battle-stable`t7`tpandora-dsticket-jwks-r6"
    Assert-PandoraOrdinaryReleaseDSTicketRevision -DeploymentRows $liveDeploymentsR7 -FleetRows $split `
        -RequestedRevision 7
} 'Fleet env/JWKS revision 分裂必须阻断'
Assert-Throws {
    $mixed = @($liveDeploymentsR7)
    $mixed[1] = "matchmaker`tpandora-config`tpandora-dsticket-signer-r6`t"
    Assert-PandoraOrdinaryReleaseDSTicketRevision -DeploymentRows $mixed -FleetRows $liveFleetsR7 `
        -RequestedRevision 7
} '运行态混用多个 revision 必须阻断'
Assert-Throws {
    $phaseConfig = @($liveDeploymentsR7)
    $phaseConfig[0] = "login`tpandora-config-dsticket-r7`tpandora-dsticket-signer-r7`tpandora-dsticket-jwks-r7"
    Assert-PandoraOrdinaryReleaseDSTicketRevision -DeploymentRows $phaseConfig -FleetRows $liveFleetsR7 `
        -RequestedRevision 7
} '普通发布不得误闯 revisioned config Secret 轮换中间态'

# 全量 parent scope 必须在 RS/GSS/Pod 尚未生成前阻断异步补出窗口；replicas=0
# 或 paused 都不是 unknown root controller 的安全历史豁免。
$unrelatedParentScope = Assert-PandoraOrdinaryDSTicketControllerScope `
    -DeploymentObjects @((New-TestOrdinaryParentDeployment -Name 'metrics-exporter')) `
    -FleetObjects @((New-TestOrdinaryParentFleet -Name 'third-party-fleet'))
Assert-True ($unrelatedParentScope.DeploymentObjects.Count -eq 0 -and
    $unrelatedParentScope.FleetObjects.Count -eq 0 -and
    $unrelatedParentScope.LegacyControllerObjects.Count -eq 0) `
    '不带 signer/DS/DSTicket 线索的无关 parent 不应误报'
Assert-True (Test-PandoraOnlineDSTicketReferenceName 'pandora-dsticket-signer-r01') `
    '畸形 signer 保留前缀也必须进入 ordinary parent clue'
Assert-True (Test-PandoraOnlineDSTicketReferenceName 'pandora-dsticket-jwks-r0') `
    '畸形 JWKS 保留前缀也必须进入 ordinary parent clue'
Assert-Throws {
    Assert-PandoraOrdinaryDSTicketControllerScope `
        -DeploymentObjects @((New-TestOrdinaryParentDeployment -Name 'rogue-malformed-prefix' `
            -DirectSecret 'pandora-dsticket-signer-r01')) -FleetObjects @()
} 'unknown Deployment 使用畸形保留 signer 前缀仍必须阻断'
Assert-Throws {
    Assert-PandoraOrdinaryDSTicketControllerScope `
        -DeploymentObjects @((New-TestOrdinaryParentDeployment -Name 'login-old' `
            -DirectSecret 'pandora-dsticket-signer-r6' -Paused)) -FleetObjects @()
} 'paused/zero 的历史 signer Deployment 即使尚无 RS 也必须阻断'
Assert-Throws {
    Assert-PandoraOrdinaryDSTicketControllerScope `
        -DeploymentObjects @((New-TestOrdinaryParentDeployment -Name 'rogue-projected-signer' `
            -ProjectedSecret 'pandora-dsticket-signer-r6')) -FleetObjects @()
} 'unknown Deployment 通过 projected source 消费 signer Secret 必须阻断'
Assert-Throws {
    Assert-PandoraOrdinaryDSTicketControllerScope `
        -DeploymentObjects @((New-TestOrdinaryParentDeployment -Name 'rogue-init-signer' `
            -InitEnvSecret 'pandora-dsticket-signer-r6')) -FleetObjects @()
} 'unknown Deployment 的 initContainer env secretKeyRef signer 必须阻断'
Assert-Throws {
    Assert-PandoraOrdinaryDSTicketControllerScope `
        -DeploymentObjects @((New-TestOrdinaryParentDeployment -Name 'rogue-phase-config' `
            -ConfigSecret 'pandora-config-dsticket-r7')) -FleetObjects @()
} 'unknown Deployment 引用 phase config 即使尚无 RS 也必须阻断'
Assert-Throws {
    Assert-PandoraOrdinaryDSTicketControllerScope -DeploymentObjects @() `
        -FleetObjects @((New-TestOrdinaryParentFleet -Name 'pandora-battle' `
            -ContainerName 'pandora-battle-ds'))
} 'zero 的历史 Fleet 即使尚无 GSS/GS 也必须阻断'
Assert-Throws {
    Assert-PandoraOrdinaryDSTicketControllerScope -DeploymentObjects @() `
        -FleetObjects @((New-TestOrdinaryParentFleet -Name 'archived-verifier' -DSTicketEnv -JWKSVolume))
} 'unknown Fleet 的 DSTicket env/JWKS tuple 即使尚无 GSS/GS 也必须阻断'

# child scope 复用共享 signer/verifier clue，必须在 direct volume/标准容器名之外
# 纳入 projected、envFrom、init、ephemeral 与 legacy private/oct 输入。
$rogueInitPod = New-TestChildPod -Name 'renamed-init-signer-pod' -Namespace pandora -App 'rogue-app' `
    -PodSpec (New-TestSignerChildSpec -ReferenceKind InitEnv)
$rogueEphemeralPod = New-TestChildPod -Name 'renamed-ephemeral-signer-pod' -Namespace pandora -App 'rogue-app' `
    -PodSpec (New-TestSignerChildSpec -ReferenceKind EphemeralEnv)
$forbiddenInitPod = New-TestChildPod -Name 'renamed-forbidden-init-pod' -Namespace pandora -App 'rogue-app' `
    -PodSpec (New-TestSignerChildSpec -ReferenceKind ForbiddenInitEnv)
$forbiddenEphemeralPod = New-TestChildPod -Name 'renamed-forbidden-ephemeral-pod' -Namespace pandora -App 'rogue-app' `
    -PodSpec (New-TestSignerChildSpec -ReferenceKind ForbiddenEphemeralOct)
$unrelatedPandoraPod = New-TestChildPod -Name 'metrics-pod' -Namespace pandora -App 'metrics' `
    -PodSpec (New-TestSignerChildSpec -ReferenceKind None)
$projectedRS = New-TestChildReplicaSet -Name 'renamed-projected-rs' `
    -PodSpec (New-TestSignerChildSpec -ReferenceKind Projected)
$envFromRS = New-TestChildReplicaSet -Name 'renamed-envfrom-rs' `
    -PodSpec (New-TestSignerChildSpec -ReferenceKind EnvFrom)
$unrelatedRS = New-TestChildReplicaSet -Name 'metrics-rs' `
    -PodSpec (New-TestSignerChildSpec -ReferenceKind None)
$zeroProjectedGSS = New-TestChildGameServerSet -Name 'arbitrary-zero-gss' -Replicas 0 `
    -PodSpec (New-TestVerifierChildSpec -ReferenceKind Projected) -DirectSchema
$nonzeroEnvGSS = New-TestChildGameServerSet -Name 'arbitrary-active-gss' -Replicas 1 `
    -PodSpec (New-TestVerifierChildSpec -ReferenceKind Env) -DirectSchema
$unrelatedGSS = New-TestChildGameServerSet -Name 'third-party-gss' -Replicas 0 `
    -PodSpec (New-TestVerifierChildSpec -ReferenceKind None) -DirectSchema
$projectedDSPod = New-TestChildPod -Name 'renamed-projected-ds-pod' -Namespace default -App 'rogue-ds' `
    -PodSpec (New-TestVerifierChildSpec -ReferenceKind Projected)
$keyRefDSPod = New-TestChildPod -Name 'renamed-keyref-ds-pod' -Namespace default -App 'rogue-ds' `
    -PodSpec (New-TestVerifierChildSpec -ReferenceKind ConfigMapKeyRef)
$envFromDSPod = New-TestChildPod -Name 'renamed-envfrom-ds-pod' -Namespace default -App 'rogue-ds' `
    -PodSpec (New-TestVerifierChildSpec -ReferenceKind EnvFrom)
$unrelatedDefaultPod = New-TestChildPod -Name 'third-party-pod' -Namespace default -App 'third-party' `
    -PodSpec (New-TestVerifierChildSpec -ReferenceKind None)
$childScope = Get-PandoraOrdinaryDSTicketChildScope `
    -PandoraPodObjects @($rogueInitPod, $rogueEphemeralPod, $forbiddenInitPod, $forbiddenEphemeralPod, $unrelatedPandoraPod) `
    -ReplicaSetObjects @($projectedRS, $envFromRS, $unrelatedRS) -GameServerObjects @() `
    -GameServerSetObjects @($zeroProjectedGSS, $nonzeroEnvGSS, $unrelatedGSS) `
    -DefaultPodObjects @($projectedDSPod, $keyRefDSPod, $envFromDSPod, $unrelatedDefaultPod)
Assert-True ($childScope.SignerPodObjects.Count -eq 4) `
    'rogue app+missing owner 的 init/ephemeral signer 与 private/oct Pod 必须进入 signer gate'
Assert-True ($childScope.ReplicaSetObjects.Count -eq 2) `
    'unknown RS projected/envFrom signer Secret 必须进入 ReplicaSet gate'
Assert-True ($childScope.GameServerSetObjects.Count -eq 2) `
    'arbitrary-name direct-schema GSS 的 zero/projected 与 nonzero/env clue 必须进入 GSS gate'
Assert-True ($childScope.DSPodObjects.Count -eq 3) `
    '改名 orphan DS Pod 的 projected/configMapKeyRef/envFrom clue 必须进入 DS gate'
Assert-True ($childScope.LegacyControllerObjects.Count -eq 11) `
    '无法归属白名单 owner/app/container 的 child 必须同时进入 legacy 二次防线'
Assert-ThrowsMatching {
    $malformedGSS = [pscustomobject]@{
        kind = 'GameServerSet'
        metadata = [pscustomobject]@{ name = 'arbitrary-malformed-gss'; namespace = 'default'; labels = [pscustomobject]@{} }
        spec = [pscustomobject]@{ replicas = 0; template = [pscustomobject]@{ spec = [pscustomobject]@{} } }
    }
    Get-PandoraOrdinaryDSTicketChildScope -PandoraPodObjects @() -ReplicaSetObjects @() `
        -GameServerObjects @() -GameServerSetObjects @($malformedGSS) -DefaultPodObjects @()
} '缺 GameServer/Pod template spec' '全量 GSS 任一 schema 无法解析必须 fail-closed，不能按无 clue 忽略'
Assert-ThrowsMatching {
    Assert-PandoraDSTicketOrdinaryState -RequestedRevision 7 `
        -DeploymentObjects @([pscustomobject]@{ kind = 'Deployment' }) `
        -FleetObjects @([pscustomobject]@{ kind = 'Fleet' }) -SignerPodObjects @() `
        -ReplicaSetObjects @() -GameServerObjects @() -GameServerSetObjects @() -DSPodObjects @() `
        -ActivationMarkers @() -TerminalMarkers @() -FixedConfigSecret $null `
        -LegacyControllerObjects @([pscustomobject]@{ kind = 'ReplicaSet' })
} 'legacy controller' 'live ordinary state 必须在 marker/fixed gate 前二次阻断 nonempty LegacyControllerObjects'

# 发布/轮换共享的 create-only 互斥锁纯契约：holder、operation、labels、
# apiserver identity 或 deletionTimestamp 任一漂移都必须 fail-closed。
$lockHolder = '0123456789abcdef0123456789abcdef'
$lockObject = New-PandoraDSTicketOperationLockObject -HolderId $lockHolder -Operation ordinary-online
$liveLock = $lockObject | ConvertTo-Json -Depth 20 | ConvertFrom-Json
$liveLock.metadata | Add-Member -NotePropertyName uid -NotePropertyValue 'lock-uid-1'
$liveLock.metadata | Add-Member -NotePropertyName resourceVersion -NotePropertyValue '101'
$liveLock.metadata | Add-Member -NotePropertyName creationTimestamp -NotePropertyValue '2026-07-13T12:00:00Z'
$lockContract = Assert-PandoraDSTicketOperationLockContract -LockObject $liveLock `
    -HolderId $lockHolder -Operation ordinary-online -RequireLiveIdentity
Assert-True ($lockContract.Uid -ceq 'lock-uid-1' -and $lockContract.ResourceVersion -ceq '101') `
    '操作锁回读 UID/resourceVersion 闭合'
$wrongHolderLock = $liveLock | ConvertTo-Json -Depth 20 | ConvertFrom-Json
$wrongHolderLock.metadata.annotations.'pandora.dev/dsticket-lock-holder-id' = 'fedcba9876543210fedcba9876543210'
Assert-Throws {
    Assert-PandoraDSTicketOperationLockContract -LockObject $wrongHolderLock `
        -HolderId $lockHolder -Operation ordinary-online -RequireLiveIdentity
} '锁 holder 漂移必须阻断'
$deletingLock = $liveLock | ConvertTo-Json -Depth 20 | ConvertFrom-Json
$deletingLock.metadata | Add-Member -NotePropertyName deletionTimestamp -NotePropertyValue '2026-07-13T12:01:00Z'
Assert-Throws {
    Assert-PandoraDSTicketOperationLockContract -LockObject $deletingLock `
        -HolderId $lockHolder -Operation ordinary-online -RequireLiveIdentity
} 'terminating 操作锁不得继续写'
$labelDriftLock = $liveLock | ConvertTo-Json -Depth 20 | ConvertFrom-Json
$labelDriftLock.metadata.labels.'app.kubernetes.io/component' = 'dsticket-rotation-audit'
Assert-Throws {
    Assert-PandoraDSTicketOperationLockContract -LockObject $labelDriftLock `
        -HolderId $lockHolder -Operation ordinary-online -RequireLiveIdentity
} '锁 label 漂移必须阻断'

$bootstrapState = Assert-PandoraOrdinaryReleaseDSTicketState -DeploymentRows @() -FleetRows @() `
    -RequestedRevision 7 -DeploymentObjects @() -FleetObjects @() -SignerPodObjects @() `
    -ReplicaSetObjects @() -GameServerObjects @() -GameServerSetObjects @() -DSPodObjects @() `
    -ActivationMarkers @() -TerminalMarkers @() -FixedConfigSecret $null
Assert-True ($bootstrapState.State -ceq 'bootstrap-empty') '真空集群允许 DSTicket bootstrap'
Assert-Throws {
    Assert-PandoraOrdinaryReleaseDSTicketState -DeploymentRows @() -FleetRows @() `
        -RequestedRevision 7 -DeploymentObjects @() -FleetObjects @() -SignerPodObjects @() `
        -ReplicaSetObjects @([pscustomobject]@{ kind = 'ReplicaSet' }) -GameServerObjects @() `
        -GameServerSetObjects @() -DSPodObjects @() -ActivationMarkers @() -TerminalMarkers @() `
        -FixedConfigSecret $null
} '双空 controller 下任一相关 ReplicaSet 残留必须阻断 bootstrap'
$configOnlyKid = 'A' * 43
$configOnlyFixed = New-TestOrdinaryFixedConfig -Kid $configOnlyKid -Revision 7
$configOnlyState = Assert-PandoraOrdinaryReleaseDSTicketState -DeploymentRows @() -FleetRows @() `
    -RequestedRevision 7 -DeploymentObjects @() -FleetObjects @() -SignerPodObjects @() `
    -ReplicaSetObjects @() -GameServerObjects @() -GameServerSetObjects @() -DSPodObjects @() `
    -ActivationMarkers @() -TerminalMarkers @() -FixedConfigSecret $configOnlyFixed
Assert-True ($configOnlyState.State -ceq 'config-only-recovery' -and
    $configOnlyState.ActiveKid -ceq $configOnlyKid) `
    '双空 controller + 同 revision fixed config 只允许带 active kid 的可续跑恢复态'
Assert-Throws {
    Assert-PandoraOrdinaryReleaseDSTicketState -DeploymentRows @() -FleetRows @() `
        -RequestedRevision 8 -DeploymentObjects @() -FleetObjects @() -SignerPodObjects @() `
        -ReplicaSetObjects @() -GameServerObjects @() -GameServerSetObjects @() -DSPodObjects @() `
        -ActivationMarkers @() -TerminalMarkers @() -FixedConfigSecret $configOnlyFixed
} 'config-only recovery 不得借普通发布换 revision'
$canaryState = Get-PandoraCanaryConfigContract `
    -BattleConfig "agones:`n  canary_percent: 17`n  canary_seed: `"stable-seed-001`"" `
    -HubConfig "agones:`n  canary_percent: 23`n  canary_seed: `"stable-seed-001`""
Assert-True ($canaryState.BattlePercent -eq 17 -and $canaryState.HubPercent -eq 23 -and
    $canaryState.Seed -ceq 'stable-seed-001') 'Canary 现网 cohort 状态可机械回读'
$legacyCanary = Get-PandoraCanaryConfigContract -BattleConfig 'agones: {}' -HubConfig 'agones: {}'
Assert-True ($legacyCanary.BattlePercent -eq 0 -and $legacyCanary.Seed -ceq '') '旧单轨配置只能迁移为 weight=0'
Assert-Throws {
    Get-PandoraCanaryConfigContract `
        -BattleConfig "agones:`n  canary_percent: 1`n  canary_seed: `"seed-0001`"" `
        -HubConfig "agones:`n  canary_percent: 1`n  canary_seed: `"seed-0002`""
} 'Battle/Hub cohort seed 分裂必须阻断'

# 普通 online 发布只能沿用 live HMAC keyset。契约解析实际 YAML，输出对象只含 SHA256，
# primary/additional 任一变化、服务间分裂或玩家/DS 跨域复用都必须 fail-closed。
$playerHmac = 'test-player-session-hmac-abcdefghijklmnopqrstuvwxyz'
$dsHmac = 'test-ds-callback-hmac-abcdefghijklmnopqrstuvwxyz'
$playerHmac2 = 'test-player-session-hmac-rotated-abcdefghijklmnop'
$dsHmac2 = 'test-ds-callback-hmac-rotated-abcdefghijklmnop'
$playerAdditional = 'test-player-additional-hmac-abcdefghijklmnopqrstuvwxyz'
$liveHmacConfigs = New-TestHmacConfigs -PlayerSecret $playerHmac -DsSecret $dsHmac
$hmacContract = Get-PandoraOnlineHmacContract -Configs $liveHmacConfigs
Assert-True ($hmacContract.Player.PrimarySha256 -cmatch '^[0-9a-f]{64}$') '玩家 HMAC 只暴露完整 SHA256 指纹'
Assert-True ($hmacContract.DsCallback.PrimarySha256 -cmatch '^[0-9a-f]{64}$') 'DS HMAC 只暴露完整 SHA256 指纹'
Assert-PandoraOnlineHmacContinuity -LiveConfigs $liveHmacConfigs -CandidateConfigs $liveHmacConfigs | Out-Null

$playerMutant = New-TestHmacConfigs -PlayerSecret $playerHmac2 -DsSecret $dsHmac
Assert-ThrowsWithoutValue {
    Assert-PandoraOnlineHmacContinuity -LiveConfigs $liveHmacConfigs -CandidateConfigs $playerMutant
} @($playerHmac, $playerHmac2) '普通发布拒绝玩家 Session HMAC 变化且不泄漏值'
$dsMutant = New-TestHmacConfigs -PlayerSecret $playerHmac -DsSecret $dsHmac2
Assert-ThrowsWithoutValue {
    Assert-PandoraOnlineHmacContinuity -LiveConfigs $liveHmacConfigs -CandidateConfigs $dsMutant
} @($dsHmac, $dsHmac2) '普通发布拒绝 DS callback HMAC 变化且不泄漏值'
$additionalMutant = New-TestHmacConfigs -PlayerSecret $playerHmac -DsSecret $dsHmac `
    -PlayerAdditional $playerAdditional
Assert-ThrowsWithoutValue {
    Assert-PandoraOnlineHmacContinuity -LiveConfigs $liveHmacConfigs -CandidateConfigs $additionalMutant
} @($playerHmac, $playerAdditional) '普通发布拒绝 additional keyset 变化且不泄漏值'
$splitMutant = New-TestHmacConfigs -PlayerSecret $playerHmac -DsSecret $dsHmac
$splitMutant.matchmaker = $splitMutant.matchmaker.Replace($playerHmac, $playerHmac2)
Assert-ThrowsWithoutValue {
    Get-PandoraOnlineHmacContract -Configs $splitMutant
} @($playerHmac, $playerHmac2) '玩家服务间 HMAC 分裂必须阻断且不泄漏值'
$crossDomainMutant = New-TestHmacConfigs -PlayerSecret $playerHmac -DsSecret $playerHmac
Assert-ThrowsWithoutValue {
    Get-PandoraOnlineHmacContract -Configs $crossDomainMutant
} @($playerHmac) '玩家与 DS HMAC 跨域复用必须阻断且不泄漏值'

$hmacServiceNames = @($liveHmacConfigs.Keys | Sort-Object)
$secretData = [ordered]@{}
foreach ($service in $hmacServiceNames) {
    $secretData["$service.yaml"] = [Convert]::ToBase64String(
        [System.Text.Encoding]::UTF8.GetBytes([string]$liveHmacConfigs[$service]))
}
$configSecret = [pscustomobject]@{
    kind = 'Secret'
    metadata = [pscustomobject]@{ name = 'pandora-config'; namespace = 'pandora' }
    data = [pscustomobject]$secretData
}
$decodedLive = ConvertFrom-PandoraConfigSecretObject -SecretObject $configSecret `
    -ExpectedServiceNames $hmacServiceNames
Assert-PandoraOnlineHmacContinuity -LiveConfigs $decodedLive -CandidateConfigs $liveHmacConfigs | Out-Null
$configSecret.data | Add-Member -NotePropertyName 'unexpected.yaml' -NotePropertyValue 'eA=='
Assert-Throws {
    ConvertFrom-PandoraConfigSecretObject -SecretObject $configSecret -ExpectedServiceNames $hmacServiceNames
} 'live pandora-config 多余 key 必须阻断'

$hmacConfigDir = Join-Path ([System.IO.Path]::GetTempPath()) `
    ('pandora-hmac-config-contract-' + [guid]::NewGuid().ToString('N'))
try {
    New-Item -ItemType Directory -Path $hmacConfigDir | Out-Null
    foreach ($service in $hmacServiceNames) {
        [System.IO.File]::WriteAllText((Join-Path $hmacConfigDir "$service.yaml"),
            [string]$liveHmacConfigs[$service], [System.Text.UTF8Encoding]::new($false))
    }
    $candidateFromDisk = Get-PandoraGeneratedConfigObject -ConfigDir $hmacConfigDir `
        -ExpectedServiceNames $hmacServiceNames
    Assert-PandoraOnlineHmacContinuity -LiveConfigs $liveHmacConfigs `
        -CandidateConfigs $candidateFromDisk | Out-Null
    [System.IO.File]::WriteAllText((Join-Path $hmacConfigDir 'unexpected.yaml'), 'x: y')
    Assert-Throws {
        Get-PandoraGeneratedConfigObject -ConfigDir $hmacConfigDir -ExpectedServiceNames $hmacServiceNames
    } '候选配置多余 YAML 必须阻断'
} finally {
    if (Test-Path -LiteralPath $hmacConfigDir) {
        $resolvedHmacDir = [System.IO.Path]::GetFullPath($hmacConfigDir)
        if ((Split-Path -Leaf $resolvedHmacDir) -notmatch '^pandora-hmac-config-contract-[0-9a-f]{32}$' -or
            (Split-Path -Parent $resolvedHmacDir) -cne
                [System.IO.Path]::GetFullPath([System.IO.Path]::GetTempPath()).TrimEnd(
                    [System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar)) {
            throw "拒绝清理未验证 HMAC 测试目录:$resolvedHmacDir"
        }
        Remove-Item -LiteralPath $resolvedHmacDir -Recurse -Force
    }
}

# start.ps1 的 runtime overlay 必须实际生成上面登记进 kustomization 的 patch 文件。
# 同时防止写文件语句再次误落进 Fleet apply 的局部作用域（该处没有 $runtime）。
$startTokens = $null
$startErrors = $null
$startAst = [System.Management.Automation.Language.Parser]::ParseFile(
    (Join-Path $ProjectRoot 'tools/scripts/start.ps1'), [ref]$startTokens, [ref]$startErrors)
Assert-True ($startErrors.Count -eq 0) 'start.ps1 AST 必须可解析'
$startFunctions = @($startAst.FindAll({
    param($node)
    $node -is [System.Management.Automation.Language.FunctionDefinitionAst]
}, $true))
$runtimeFunction = @($startFunctions | Where-Object Name -CEQ 'New-OnlineRuntimeOverlay')
$fleetApplyFunction = @($startFunctions | Where-Object Name -CEQ 'Apply-FleetManifest')
$onlineFunction = @($startFunctions | Where-Object Name -CEQ 'Invoke-Online')
$ordinaryStateFunction = @($startFunctions | Where-Object Name -CEQ 'Assert-OnlineOrdinaryDSTicketState')
$snapshotFunction = @($startFunctions | Where-Object Name -CEQ 'Get-OnlineLiveDSTicketRevisionRows')
$lockEnterFunction = @($startFunctions | Where-Object Name -CEQ 'Enter-OnlineDSTicketOperationLock')
$lockHeldFunction = @($startFunctions | Where-Object Name -CEQ 'Assert-OnlineDSTicketOperationLockHeld')
$lockExitFunction = @($startFunctions | Where-Object Name -CEQ 'Exit-OnlineDSTicketOperationLock')
$legacyConfigScanFunction = @($startFunctions | Where-Object Name -CEQ 'Test-HasLegacyPandoraConfigMapRef')
Assert-True ($runtimeFunction.Count -eq 1) '必须有唯一 New-OnlineRuntimeOverlay'
Assert-True ($fleetApplyFunction.Count -eq 1) '必须有唯一 Apply-FleetManifest'
Assert-True ($onlineFunction.Count -eq 1) '必须有唯一 Invoke-Online'
Assert-True ($ordinaryStateFunction.Count -eq 1) '必须有唯一 Assert-OnlineOrdinaryDSTicketState'
Assert-True ($snapshotFunction.Count -eq 1) '必须有唯一完整 DSTicket 快照函数'
Assert-True ($lockEnterFunction.Count -eq 1 -and $lockHeldFunction.Count -eq 1 -and $lockExitFunction.Count -eq 1) `
    '普通发布必须有唯一锁获取/持有权复查/释放入口'
Assert-True ($legacyConfigScanFunction.Count -eq 1) '必须有唯一旧 pandora-config 引用扫描函数'
Assert-True ($legacyConfigScanFunction[0].Extent.Text.Contains(
        'if ($Node -is [System.Collections.IEnumerable])')) `
    '旧 ConfigMap 扫描必须先逐项遍历 JSON 数组，不能递归数组的 SyncRoot'
Assert-True (-not $legacyConfigScanFunction[0].Extent.Text.Contains(
        '$Node -isnot [pscustomobject]')) `
    '[pscustomobject] 类型加速器也匹配 Object[]，不得用它排除 JSON 数组'
Assert-True ($legacyConfigScanFunction[0].Extent.Text.Contains('$Node.GetType().IsValueType')) `
    '旧 ConfigMap 扫描必须把 DateTime/TimeSpan/enum 等值类型视为叶子'
Invoke-Expression $legacyConfigScanFunction[0].Extent.Text
$legacyArrayFixture = [pscustomobject]@{
    restartedAt = [datetime]'2026-07-13T13:15:00Z'
    template = [pscustomobject]@{
        spec = [pscustomobject]@{
            volumes = @(
                [pscustomobject]@{ secret = [pscustomobject]@{ secretName = 'pandora-config' } },
                [pscustomobject]@{ configMap = [pscustomobject]@{ name = 'pandora-config' } }
            )
        }
    }
}
$secretOnlyArrayFixture = [pscustomobject]@{
    restartedAt = [datetime]'2026-07-13T13:15:00Z'
    template = [pscustomobject]@{
        spec = [pscustomobject]@{
            volumes = @([pscustomobject]@{ secret = [pscustomobject]@{ secretName = 'pandora-config' } })
        }
    }
}
Assert-True (Test-HasLegacyPandoraConfigMapRef $legacyArrayFixture) `
    '数组中的旧 ConfigMap 引用必须被发现'
Assert-True (-not (Test-HasLegacyPandoraConfigMapRef $secretOnlyArrayFixture)) `
    '只引用 Secret 的数组不得误报旧 ConfigMap'
Assert-True ($runtimeFunction[0].Extent.Text.Contains(
        '[System.IO.File]::WriteAllText((Join-Path $runtime $dsticketPatchName), $dsticketPatch')) `
    'runtime overlay 必须在 kubectl kustomize 前写出 Login DSTicket revision patch'
Assert-True ($runtimeFunction[0].Extent.Text.Contains('New-PandoraDSTicketSignerSecretRevisionPatch')) `
    'runtime overlay 必须原子生成四个 DSTicket signer Secret revision patch'
Assert-True (-not $fleetApplyFunction[0].Extent.Text.Contains('$dsticketPatchName')) `
    'Fleet apply 不得引用 runtime overlay 的局部 patch 变量'
$onlineSource = $onlineFunction[0].Extent.Text
Assert-True ($onlineSource.Contains('Assert-OnlineOrdinaryDSTicketState') -and
    $ordinaryStateFunction[0].Extent.Text.Contains('Assert-PandoraOrdinaryReleaseDSTicketState')) `
    '普通 online 发布必须调用完整 DSTicket 普通态门禁（含 revision/fixed config/marker/live DS）'
$snapshotSource = $snapshotFunction[0].Extent.Text
foreach ($requiredRead in @('deployments', 'replicasets', 'configmaps', 'fleets.agones.dev',
    'gameservers.agones.dev', 'gameserversets.agones.dev', "'pods'")) {
    Assert-True ($snapshotSource.Contains($requiredRead)) "DSTicket 快照必须读取 $requiredRead"
}
Assert-True ($snapshotSource.Contains('ReplicaSetObjects') -and $snapshotSource.Contains('GameServerSetObjects') -and
    $snapshotSource.Contains('$configRefs') -and
    $snapshotSource.Contains('Get-PandoraOrdinaryDSTicketChildScope') -and
    $snapshotSource.Contains('-PandoraPodObjects $pandoraPods -ReplicaSetObjects $allReplicaSets') -and
    $snapshotSource.Contains('-GameServerSetObjects @($gameServerSets.items)') -and
    $snapshotSource.Contains('@($childScope.LegacyControllerObjects)')) `
    '快照必须把全量 Pod/RS/GSS 交给 child scope，并把无法归属对象送入 legacy 二次防线'
Assert-True ($snapshotSource.Contains('Assert-PandoraOrdinaryDSTicketControllerScope') -and
    $snapshotSource.Contains('-DeploymentObjects $allDeployments -FleetObjects @($fleetList.items)')) `
    '快照必须把全量 Deployment/Fleet 交给 parent scope，不能等 RS/GSS/Pod 出现后才发现 unknown controller'
$onlineContractSource = Get-Content -LiteralPath `
    (Join-Path $ProjectRoot 'tools/scripts/lib/online_manifest_contract.ps1') -Raw
Assert-True ($onlineContractSource.Contains('Get-PandoraDSTicketSignerSecretReferences') -and
    $onlineContractSource.Contains('Test-PandoraDSTicketDSPodSpecClue') -and
    $onlineContractSource.Contains('Get-PandoraGameServerSetPodSpec') -and
    $onlineContractSource.Contains("'containers', 'initContainers', 'ephemeralContainers'") -and
    $onlineContractSource.Contains('PANDORA_(?:DS_TICKET_SECRET|JWT_SECRET|PLAYER_JWT_SECRET|DSTICKET_')) `
    'child scope 必须复用共享全引用 clue/GSS 双 schema parser，并覆盖 init/ephemeral forbidden private env'
$rotationContractSource = Get-Content -LiteralPath `
    (Join-Path $ProjectRoot 'tools/scripts/lib/dsticket_rotation_contract.ps1') -Raw
Assert-True ($rotationContractSource.Contains('if ($LegacyControllerObjects.Count -ne 0)') -and
    $rotationContractSource.Contains('ordinary live state 检测到未分类 legacy controller')) `
    '共享 ordinary live state 必须对 nonempty LegacyControllerObjects 二次 fail-closed'
Assert-True (-not $snapshotSource.Contains('Where-Object { $null -eq $_.metadata.deletionTimestamp }')) `
    '完整 DSTicket 快照不得过滤 terminating 对象'

$secureRequired = $onlineSource.IndexOf('Assert-PandoraDsAuthHttpsEndpoints $DsFenceEtcdEndpoints', [StringComparison]::Ordinal)
$canonicalPreflight = $onlineSource.IndexOf("任何 registry/K8s 写前只读验证 canonical green", [StringComparison]::Ordinal)
$lockAcquire = $onlineSource.IndexOf('$dsticketOperationLock = Enter-OnlineDSTicketOperationLock', [StringComparison]::Ordinal)
$lockedState = $onlineSource.IndexOf('$lockedOrdinaryState = Assert-OnlineOrdinaryDSTicketState', [StringComparison]::Ordinal)
$configWrite = $onlineSource.IndexOf('Apply-PandoraConfigSecret -KubeContext $ctx', [StringComparison]::Ordinal)
$fleetWrite = $onlineSource.IndexOf('Apply-AgonesManifests -BattleDsImage', [StringComparison]::Ordinal)
$deploymentWrite = $onlineSource.IndexOf('kubectl @kubectlContextArgs apply -k $runtimeOverlayDir', [StringComparison]::Ordinal)
$finalState = $onlineSource.IndexOf('$finalOrdinaryState = Assert-OnlineOrdinaryDSTicketState', [StringComparison]::Ordinal)
$lockRelease = $onlineSource.IndexOf('Exit-OnlineDSTicketOperationLock', [StringComparison]::Ordinal)
foreach ($position in @($secureRequired, $canonicalPreflight, $lockAcquire, $lockedState, $configWrite, $fleetWrite, $deploymentWrite, $finalState, $lockRelease)) {
    Assert-True ($position -ge 0) '互斥锁顺序契约 marker 必须存在'
}
Assert-True ($secureRequired -lt $canonicalPreflight -and $canonicalPreflight -lt $lockAcquire -and
    $lockAcquire -lt $lockedState -and $lockedState -lt $configWrite -and
    $configWrite -lt $fleetWrite -and $fleetWrite -lt $deploymentWrite -and $deploymentWrite -lt $finalState -and
    $finalState -lt $lockRelease) `
    'secure required=2 + canonical green 前置审计后才获取锁；锁内重验早于所有 DSTicket 写，终验早于释放'
Assert-True (-not $onlineSource.Contains('online 生产验票阻断')) '已完成真 Agones/UE E2E 后不得保留旧 DSTicket 假阻断'
Assert-True ($onlineSource.Contains('replace -f $greenPatchPath') -and
    $onlineSource.Contains('"$($svc.Name)-ds-auth-green"') -and
    $onlineSource.Contains('-CanonicalGreen:($Env -eq ''prod'')')) `
    'prod 普通发布必须 CAS replace/restart/status canonical green，不能重启 dormant blue'
Assert-True ($lockExitFunction[0].Extent.Text.Contains("kind = 'DeleteOptions'") -and
    $lockExitFunction[0].Extent.Text.Contains('preconditions') -and
    $lockExitFunction[0].Extent.Text.Contains('uid = $held.Uid') -and
    $lockExitFunction[0].Extent.Text.Contains('resourceVersion = $held.ResourceVersion') -and
    $lockExitFunction[0].Extent.Text.Contains('delete "--raw=$deletePath" -f -')) `
    '锁释放必须使用 raw DELETE DeleteOptions UID+resourceVersion 双前置条件'
Assert-True ($onlineSource.Contains('$lockedOrdinaryState.ActiveKid') -and
    $onlineSource.Contains('$finalOrdinaryState.ActiveKid')) `
    'fixed/terminal ordinary state active kid 必须在锁内与终验对账 immutable key material'
Assert-True ($onlineSource.Contains('$lockedOrdinaryState.State -cne [string]$ordinaryDSTicketState.State') -and
    $onlineSource.Contains('$earlyFixedRV -cne $lockedFixedRV') -and
    $onlineSource.Contains('Assert-PandoraOnlineHmacContinuity -LiveConfigs $lockedHmacConfigs')) `
    '长构建窗口后必须在锁内对账 ordinary state/fixed resourceVersion 并重跑 HMAC 连续性'
Assert-OnlineHmacGateOrdering -OnlineSource $onlineSource
Assert-True ($onlineSource.Contains('online 暂不允许 PANDORA_*_SECRET_ADDITIONAL')) `
    'online additional_secrets 无条件硬门必须保留'
$lateGateMutant = $onlineSource.Replace('Assert-PandoraOnlineHmacContinuity',
    'Deferred-PandoraOnlineHmacContinuity') + "`nAssert-PandoraOnlineHmacContinuity"
Assert-Throws {
    Assert-OnlineHmacGateOrdering -OnlineSource $lateGateMutant
} 'HMAC 连续性检查移到 push/apply 后必须被顺序契约阻断'

$sourceOverlay = Join-Path $ProjectRoot 'deploy/k8s/overlays/online'
$overlayParent = Split-Path -Parent $sourceOverlay
$runtimeOverlay = Join-Path $overlayParent ('.online-contract-test-' + [guid]::NewGuid().ToString('N'))
try {
    Copy-Item -LiteralPath $sourceOverlay -Destination $runtimeOverlay -Recurse
    $kustomizationPath = Join-Path $runtimeOverlay 'kustomization.yaml'
    $kustomization = Get-Content -LiteralPath $kustomizationPath -Raw
    $kustomization = ConvertTo-PandoraDigestKustomization -Template $kustomization -Registry 'registry.example.com:5000' -Digests $digests -ServiceNames $services
    $signers = @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')
    $signerPatchNames = @($signers | ForEach-Object { "dsticket-signer-$_.yaml" })
    $kustomization = Add-PandoraWriterPatchEntries -Kustomization $kustomization -WriterServices $writers `
        -AdditionalPatchPaths (@('dsticket-keyset-login.yaml') + $signerPatchNames)
    [System.IO.File]::WriteAllText($kustomizationPath, $kustomization, [System.Text.UTF8Encoding]::new($false))
    foreach ($writer in $writers) {
        $patchText = New-PandoraWriterDigestPatch -Service $writer -Digest $digests[$writer]
        [System.IO.File]::WriteAllText((Join-Path $runtimeOverlay "writer-digest-$writer.yaml"), $patchText, [System.Text.UTF8Encoding]::new($false))
    }
    [System.IO.File]::WriteAllText((Join-Path $runtimeOverlay 'dsticket-keyset-login.yaml'), $loginRevisionPatch, [System.Text.UTF8Encoding]::new($false))
    foreach ($signer in $signers) {
        $patchText = New-PandoraDSTicketSignerSecretRevisionPatch -Service $signer -Revision 7
        [System.IO.File]::WriteAllText((Join-Path $runtimeOverlay "dsticket-signer-$signer.yaml"), $patchText, [System.Text.UTF8Encoding]::new($false))
    }

    $renderedLines = @(& kubectl kustomize $runtimeOverlay 2>&1)
    if ($LASTEXITCODE -ne 0) { throw "kubectl kustomize 测试失败:$($renderedLines -join [Environment]::NewLine)" }
    $rendered = $renderedLines -join [Environment]::NewLine
    Assert-True ([regex]::Matches($rendered, '(?m)^\s*name:\s*pandora-dsticket-jwks-r7\s*$').Count -eq 1) `
        'online 渲染后 Login 必须精确挂本次 revision 公钥 ConfigMap'
    Assert-True ([regex]::Matches($rendered, '(?m)^\s*secretName:\s*pandora-dsticket-signer-r7\s*$').Count -eq 4) `
        'online 渲染后四个 signer 必须精确挂同 revision 私钥 Secret'
    $pins = @{}
    foreach ($name in $services) { $pins[$name] = "registry.example.com:5000/pandora/$name@$($digests[$name])" }
    $contractRows = Get-TestContractRows -Manifest $rendered
    Assert-PandoraRenderedOnlineContract -ContractRows $contractRows -Pins $pins -Digests $digests -ServiceNames $services -WriterServices $writers
    $hubSingleWriterRows = Get-TestContractRows -Manifest $rendered -HubAllocatorSingleWriter
    Assert-PandoraHubAllocatorSingleWriterContract -ContractRows $hubSingleWriterRows
    $dsticketRows = Get-TestContractRows -Manifest $rendered -DSTicket
    Assert-PandoraDSTicketSignerRevisionContract -ContractRows $dsticketRows -Revision 7

    $wrongSignerRevision = [regex]::Replace(
        $rendered, 'pandora-dsticket-signer-r7', 'pandora-dsticket-signer-r6', 1)
    Assert-Throws {
        $rows = Get-TestContractRows -Manifest $wrongSignerRevision -DSTicket
        Assert-PandoraDSTicketSignerRevisionContract -ContractRows $rows -Revision 7
    } '任一 signer Secret revision 漂移必须阻断'

    $missingAnnotation = [regex]::Replace($rendered,
        '(?m)^\s*pandora\.dev/image-digest:\s*["'']?' + [regex]::Escape($digests.login) + '["'']?\s*$',
        '', 1)
    Assert-Throws {
        $rows = Get-TestContractRows -Manifest $missingAnnotation
        Assert-PandoraRenderedOnlineContract -ContractRows $rows -Pins $pins -Digests $digests -ServiceNames $services -WriterServices $writers
    } '缺 writer annotation'

    $mutableImage = $rendered.Replace('@' + $digests.player, ':mutable')
    Assert-Throws {
        $rows = Get-TestContractRows -Manifest $mutableImage
        Assert-PandoraRenderedOnlineContract -ContractRows $rows -Pins $pins -Digests $digests -ServiceNames $services -WriterServices $writers
    } 'Deployment 回退 tag'

    $initContainerSmuggle = $mutableImage -replace '(?m)^(\s*)containers:\s*$', ('$1initContainers:' + "`n" + '$1- name: digest-decoy' + "`n" + '$1  image: registry.example.com:5000/pandora/player@' + $digests.player + "`n" + '$1containers:')
    Assert-Throws {
        $rows = Get-TestContractRows -Manifest $initContainerSmuggle
        Assert-PandoraRenderedOnlineContract -ContractRows $rows -Pins $pins -Digests $digests -ServiceNames $services -WriterServices $writers
    } '期望 digest 只在 initContainer 不能掩护 mutable 主容器'

    $hubSingleWriterIndex = -1
    for ($i = 0; $i -lt $hubSingleWriterRows.Count; $i++) {
        if ($hubSingleWriterRows[$i] -cmatch '^Deployment\thub-allocator\t') {
            $hubSingleWriterIndex = $i
            break
        }
    }
    Assert-True ($hubSingleWriterIndex -ge 0) 'hub-allocator single-writer 结构行必须存在'
    # 变异矩阵对齐当前权威设计(R9 P0-7:单写者由 writerlease 保证,部署策略是
    # RollingUpdate{maxSurge:1,maxUnavailable:0};见 online_manifest_contract.ps1 里
    # Assert-PandoraHubAllocatorSingleWriterContract 的说明)。此前三条变异押的是
    # 已被审计否决的 Recreate 形态,manifest 早已不长那样。
    foreach ($mutation in @(
        @{ Name = 'hub multi-replica writer'; Field = 2; Value = '2' },
        @{ Name = 'hub 回退 Recreate(全量断流,违反不停服)'; Field = 3; Value = 'Recreate' },
        @{ Name = 'hub maxUnavailable 放开(升级期可能零副本在服务)'; Field = 4; Value = 'map[maxSurge:1 maxUnavailable:1]' },
        @{ Name = 'hub maxSurge 放开(重叠窗口超两副本)'; Field = 4; Value = 'map[maxSurge:2 maxUnavailable:0]' }
    )) {
        $mutantRows = @($hubSingleWriterRows)
        $mutantFields = @([regex]::Split($mutantRows[$hubSingleWriterIndex], "`t"))
        $mutantFields[[int]$mutation.Field] = [string]$mutation.Value
        $mutantRows[$hubSingleWriterIndex] = $mutantFields -join "`t"
        Assert-Throws {
            Assert-PandoraHubAllocatorSingleWriterContract -ContractRows $mutantRows
        } ([string]$mutation.Name)
    }
}
finally {
    if (Test-Path -LiteralPath $runtimeOverlay) {
        $resolved = [System.IO.Path]::GetFullPath($runtimeOverlay)
        if ((Split-Path -Parent $resolved) -cne [System.IO.Path]::GetFullPath($overlayParent) -or
            (Split-Path -Leaf $resolved) -notmatch '^\.online-contract-test-[0-9a-f]{32}$') {
            throw "拒绝清理未验证测试目录:$resolved"
        }
        Remove-Item -LiteralPath $resolved -Recurse -Force
    }
}

$fleetCases = @(
    @{ File = '20-fleet-battle.yaml'; Track = 'stable'; Name = 'pandora-battle-stable' },
    @{ File = '21-fleet-battle-canary.yaml'; Track = 'canary'; Name = 'pandora-battle-canary' },
    @{ File = '30-fleet-hub.yaml'; Track = 'stable'; Name = 'pandora-hub-stable' },
    @{ File = '31-fleet-hub-canary.yaml'; Track = 'canary'; Name = 'pandora-hub-canary' }
)
foreach ($fleetCase in $fleetCases) {
    $fleetName = [string]$fleetCase.File
    $src = Join-Path $ProjectRoot "deploy/k8s/agones/$fleetName"
    $digest = if ($fleetName -like '*battle*') { 'sha256:' + ('b' * 64) } else { 'sha256:' + ('c' * 64) }
    $repo = if ($fleetName -like '*battle*') { 'registry.example.com/pandora/battle-ds:v1' } else { 'registry.example.com/pandora/hub-ds:v1' }
    $fleetPin = New-PandoraPinnedImageReference $repo $digest
    $fleet = Set-PandoraFleetImagePin -Manifest (Get-Content -LiteralPath $src -Raw) -PinnedImage $fleetPin
    $containerName = if ($fleetName -like '*battle*') { 'pandora-battle-ds' } else { 'pandora-hub-ds' }
    $fleetRows = Get-TestContractRows -Manifest $fleet -Fleet
    Assert-PandoraFleetManifestContract -Manifest $fleet -ContractRows $fleetRows -PinnedImage $fleetPin `
        -ContainerName $containerName -ExpectedTrack $fleetCase.Track -ExpectedFleetName $fleetCase.Name
    $maxPlayersEnv = @([regex]::Matches($fleet, '(?ms)^\s*-\s*name:\s*PANDORA_DS_MAX_PLAYERS\s*\r?\n\s*value:\s*"([0-9]+)"\s*$'))
    if ($fleetName -like '*hub*') {
        Assert-True ($maxPlayersEnv.Count -eq 1 -and $maxPlayersEnv[0].Groups[1].Value -eq '500') `
            'Hub stable/canary Fleet 必须且只能注入 PANDORA_DS_MAX_PLAYERS=500'
    }
    else {
        Assert-True ($maxPlayersEnv.Count -eq 0) `
            'Battle Fleet 不得继承 Hub 的 PANDORA_DS_MAX_PLAYERS'
    }

    $oneLayerMissing = [regex]::Replace($fleet,
        '(?m)^\s*pandora\.dev/image-digest:\s*["'']?' + [regex]::Escape($digest) + '["'']?\s*$',
        '', 1)
    Assert-Throws {
        $rows = Get-TestContractRows -Manifest $oneLayerMissing -Fleet
        Assert-PandoraFleetManifestContract -Manifest $oneLayerMissing -ContractRows $rows -PinnedImage $fleetPin `
            -ContainerName $containerName -ExpectedTrack $fleetCase.Track -ExpectedFleetName $fleetCase.Name
    } 'Fleet 少一层 annotation'
    $trackMetadataLines = @([regex]::Matches($fleet,
            '(?m)^\s*pandora\.dev/release-track:\s*(?:stable|canary)\s*$'))
    Assert-True ($trackMetadataLines.Count -eq 5) `
        'Fleet 必须有 Fleet label、GameServer label/annotation、Pod label/annotation 五处 release-track'
    foreach ($annotationIndex in @(2, 4)) {
        $missingTrackAnnotation = $fleet.Remove(
            $trackMetadataLines[$annotationIndex].Index, $trackMetadataLines[$annotationIndex].Length)
        Assert-Throws {
            $rows = Get-TestContractRows -Manifest $missingTrackAnnotation -Fleet
            Assert-PandoraFleetManifestContract -Manifest $missingTrackAnnotation -ContractRows $rows `
                -PinnedImage $fleetPin -ContainerName $containerName -ExpectedTrack $fleetCase.Track `
                -ExpectedFleetName $fleetCase.Name
        } "Fleet 少 release-track annotation(index=$annotationIndex)"
    }
    Assert-Throws {
        Assert-PandoraFleetManifestContract -Manifest ($fleet + "`nenv:`n- name: PANDORA_DS_TICKET_SECRET") -ContractRows $fleetRows -PinnedImage $fleetPin `
            -ContainerName $containerName -ExpectedTrack $fleetCase.Track -ExpectedFleetName $fleetCase.Name
    } 'Fleet 注入玩家 signing secret'
    # DSTicket v2(方案 B):缺 JWKS env / 缺 ConfigMap 卷 / revision 漂移都必须阻断。
    Assert-Throws {
        $noJwksEnv = [regex]::Replace($fleet, '(?m)^\s*-\s*name:\s*PANDORA_DSTICKET_JWKS_FILE\s*\r?\n\s*value:\s*"[^"]+"\s*\r?$', '', 1)
        Assert-PandoraFleetManifestContract -Manifest $noJwksEnv -ContractRows $fleetRows -PinnedImage $fleetPin `
            -ContainerName $containerName -ExpectedTrack $fleetCase.Track -ExpectedFleetName $fleetCase.Name
    } 'Fleet 缺 DSTicket v2 JWKS env'
    Assert-Throws {
        $noRevEnv = [regex]::Replace($fleet, '(?m)^\s*-\s*name:\s*PANDORA_DSTICKET_KEYSET_REVISION\s*\r?\n\s*value:\s*"[0-9]+"\s*\r?$', '', 1)
        Assert-PandoraFleetManifestContract -Manifest $noRevEnv -ContractRows $fleetRows -PinnedImage $fleetPin `
            -ContainerName $containerName -ExpectedTrack $fleetCase.Track -ExpectedFleetName $fleetCase.Name
    } 'Fleet 缺 DSTicket v2 keyset revision env'
    Assert-Throws {
        $noJwksVol = $fleet.Replace('pandora-dsticket-jwks-r1', 'some-other-configmap')
        Assert-PandoraFleetManifestContract -Manifest $noJwksVol -ContractRows $fleetRows -PinnedImage $fleetPin `
            -ContainerName $containerName -ExpectedTrack $fleetCase.Track -ExpectedFleetName $fleetCase.Name
    } 'Fleet 缺 DSTicket v2 JWKS ConfigMap 卷'
    Assert-Throws {
        $revDrift = $fleet.Replace('pandora-dsticket-jwks-r1', 'pandora-dsticket-jwks-r2')
        Assert-PandoraFleetManifestContract -Manifest $revDrift -ContractRows $fleetRows -PinnedImage $fleetPin `
            -ContainerName $containerName -ExpectedTrack $fleetCase.Track -ExpectedFleetName $fleetCase.Name
    } 'Fleet DSTicket revision env 与 ConfigMap 卷不一致'
    $revision7 = Set-PandoraFleetDSTicketKeysetRevision -Manifest $fleet -Revision 7
    Assert-True ($revision7.Contains('value: "7"') -and $revision7.Contains('pandora-dsticket-jwks-r7')) `
        'Fleet revision 必须同时改 env 与 ConfigMap'

    foreach ($smuggle in @(
        ($fleet + "`nsecretName: pandora-dsticket"),
        ($fleet + "`nsecretName: pandora-dsticket-signer-r7"),
        ($fleet + "`nvolumes: [ { name: leak, secret: { secretName: pandora-dsticket } } ]"),
        ($fleet + "`n- name: PANDORA_DSTICKET_HMAC`n  value: leak"),
        ($fleet + "`nenv: [ { name: PANDORA_JWT_SECRET, value: leak } ]"),
        ($fleet + "`nmountPath: /run/secrets/pandora-dsticket"),
        ($fleet + "`njwks: { kty: oct }"),
        ($fleet + "`nkey: private.pem")
    )) {
        Assert-Throws { Assert-PandoraFleetNoPlayerSigningMaterial -Manifest $smuggle } `
            'Fleet 私钥/HMAC 材料变体必须机械阻断'
    }
}

# Inventory/DS terminal 仍是独立静态候选；真实 E2E 与激活状态机未闭合前，普通 online 不得默认 apply。
$onlineKustomizationRaw = Get-Content -LiteralPath (Join-Path $ProjectRoot 'deploy/k8s/overlays/online/kustomization.yaml') -Raw
foreach ($component in @('mesh-shared-identity', 'inventory-mesh/identity', 'inventory-mesh/gate',
        'inventory-mesh/enforce', 'ds-terminal-mesh')) {
    Assert-True (-not $onlineKustomizationRaw.Contains($component)) "ordinary online 不得引用静态候选 component=$component"
}
$onlineRenderLines = @(& kubectl kustomize (Join-Path $ProjectRoot 'deploy/k8s/overlays/online') 2>&1)
Assert-True ($LASTEXITCODE -eq 0) "online kustomize render 必须成功:$($onlineRenderLines -join [Environment]::NewLine)"
$onlineRender = $onlineRenderLines -join "`n"
foreach ($marker in @(
    'pandora-inventory-exact-allow', 'pandora-inventory-mtls', 'pandora-inventory-mesh-deployments',
    'allow-inventory-grpc', 'pandora-ds-terminal-release-exact-deny',
    'pandora-ds-allocator-terminal-permissive', 'PANDORA_ISTIO_REVISION_REQUIRED')) {
    Assert-True (-not $onlineRender.Contains($marker)) "ordinary online render 不得包含未激活静态候选 marker=$marker"
}

Write-Host 'online_manifest_contract_test: PASS' -ForegroundColor Green
