# Pandora online 发布清单的纯函数与静态契约。
#
# 本文件只处理字符串/对象，不调用 docker、kubectl，也不修改集群。start.ps1 负责外部命令，
# 这里负责把最终发布引用固定到 registry digest，并验证 Downward API / annotation 闭环。

$script:PandoraDigestPattern = '^sha256:[0-9a-f]{64}$'
$script:PandoraImagePinPattern = '^(?<repository>[^\s@]+)@(?<digest>sha256:[0-9a-f]{64})$'
$script:PandoraWriterServices = @('login', 'player-locator', 'ds-allocator', 'hub-allocator', 'battle-result')
$script:PandoraPlayerHmacServices = @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')
# player 2026-08-13 入列(与 gen_cluster_config.ps1 的 $DsSecretServiceNames 必须同增同减):
# GetLoadout 经 Envoy DS 面暴露,服务端要验 DS 回调令牌,故 player 也持 DS callback HMAC 校验材料。
$script:PandoraDsCallbackHmacServices = @('login', 'ds-allocator', 'hub-allocator', 'battle-result', 'player-locator', 'player', 'team', 'guild')
$script:PandoraPlacementDomains = [ordered]@{}
$script:PandoraMatchResumeAuthBindings = @(
    @{ Service = 'matchmaker'; Section = 'match'; Child = 'match_resume_auth_secret' },
    @{ Service = 'matchmaker-pve'; Section = 'match'; Child = 'match_resume_auth_secret' })
# Team→Matchmaker 是 ResolvePlayerMatchContext 的第二个合法内部调用方，持**独立**于 login 的密钥。
# 验签端 matchmaker.match.team_resume_auth_secret 与签名端 team.team.match_resume_auth_secret 必须同值：
# 只改一端不会报错，只会让 matchmaker 拒掉每一次 team 调用（招募列表恒空 + 入队恒失败），两边进程都健康。
# matchmaker-pve 不参与：team 只连 PVP matchmaker，PVE 模板也没有该节点。
$script:PandoraTeamResumeAuthBindings = @(
    @{ Service = 'matchmaker'; Section = 'match'; Child = 'team_resume_auth_secret' },
    @{ Service = 'team'; Section = 'team'; Child = 'match_resume_auth_secret' })
$script:PandoraAllocationAbortAuthBindings = @(
    @{ Service = 'matchmaker'; Section = 'match'; Child = 'allocation_abort_auth_secret' },
    @{ Service = 'matchmaker-pve'; Section = 'match'; Child = 'allocation_abort_auth_secret' },
    @{ Service = 'ds-allocator'; Section = 'allocator'; Child = 'allocation_abort_auth_secret' })

# 普通 online 发布的 HMAC 连续性契约。所有函数只返回 SHA256 指纹，不把解析出的密钥
# 放进异常或日志；密钥轮换必须由独立流程完成，不能借普通 Stable/Canary 发布切换。
function Get-PandoraSecretSha256 {
    param([Parameter(Mandatory = $true)][AllowEmptyString()][string]$Value)
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Value)
    $hash = [System.Security.Cryptography.SHA256]::HashData($bytes)
    return ([System.BitConverter]::ToString($hash) -replace '-', '').ToLowerInvariant()
}

function ConvertFrom-PandoraJsonStringScalar {
    param([Parameter(Mandatory = $true)][string]$Json, [Parameter(Mandatory = $true)][string]$Where)
    $document = $null
    try {
        $document = [System.Text.Json.JsonDocument]::Parse($Json)
        if ($document.RootElement.ValueKind -ne [System.Text.Json.JsonValueKind]::String) {
            throw 'not-string'
        }
        return $document.RootElement.GetString()
    } catch {
        throw "$Where 必须是严格 JSON/YAML 双引号字符串；拒绝普通发布（错误不回显密钥）。"
    } finally {
        if ($null -ne $document) { $document.Dispose() }
    }
}

function ConvertFrom-PandoraJsonStringArray {
    param([Parameter(Mandatory = $true)][string]$Json, [Parameter(Mandatory = $true)][string]$Where)
    $document = $null
    try {
        $document = [System.Text.Json.JsonDocument]::Parse($Json)
        if ($document.RootElement.ValueKind -ne [System.Text.Json.JsonValueKind]::Array) {
            throw 'not-array'
        }
        $values = [System.Collections.Generic.List[string]]::new()
        foreach ($item in $document.RootElement.EnumerateArray()) {
            if ($item.ValueKind -ne [System.Text.Json.JsonValueKind]::String) { throw 'not-string-item' }
            $values.Add($item.GetString())
        }
        return @($values)
    } catch {
        throw "$Where 必须是严格的字符串数组；拒绝普通发布（错误不回显密钥）。"
    } finally {
        if ($null -ne $document) { $document.Dispose() }
    }
}

function Get-PandoraYamlHmacKeysetContract {
    param(
        [Parameter(Mandatory = $true)][string]$ServiceName,
        [Parameter(Mandatory = $true)][string]$Text,
        [Parameter(Mandatory = $true)][ValidateSet('jwt', 'ds_auth')][string]$SectionName
    )
    [string[]]$lines = [regex]::Split($Text, '\r?\n')
    $sectionPattern = '^([ ]*)' + [regex]::Escape($SectionName) + ':[ \t]*(?:#.*)?$'
    $sectionIndexes = @(
        for ($i = 0; $i -lt $lines.Count; $i++) {
            if ($lines[$i] -cmatch $sectionPattern) { $i }
        }
    )
    if ($sectionIndexes.Count -ne 1) {
        throw "$ServiceName.$SectionName 必须恰好有一个节点；拒绝普通发布。"
    }
    $sectionIndex = [int]$sectionIndexes[0]
    $sectionIndent = [regex]::Match($lines[$sectionIndex], $sectionPattern).Groups[1].Value.Length
    $sectionEnd = $lines.Count
    for ($i = $sectionIndex + 1; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -cmatch '^\s*$' -or $lines[$i] -cmatch '^[ ]*#') { continue }
        $indent = [regex]::Match($lines[$i], '^([ ]*)').Groups[1].Value.Length
        if ($indent -le $sectionIndent) { $sectionEnd = $i; break }
    }
    $directIndent = [int]::MaxValue
    for ($i = $sectionIndex + 1; $i -lt $sectionEnd; $i++) {
        if ($lines[$i] -cmatch '^\s*$' -or $lines[$i] -cmatch '^[ ]*#') { continue }
        $indent = [regex]::Match($lines[$i], '^([ ]*)').Groups[1].Value.Length
        if ($indent -gt $sectionIndent -and $indent -lt $directIndent) { $directIndent = $indent }
    }
    if ($directIndent -eq [int]::MaxValue) { throw "$ServiceName.$SectionName 是空节点；拒绝普通发布。" }

    $prefix = '^' + (' ' * $directIndent)
    $secretPattern = $prefix + 'secret[ \t]*:[ \t]*(?<json>"(?:\\.|[^"\\])*")[ \t]*(?:#.*)?$'
    $secretMatches = @(
        for ($i = $sectionIndex + 1; $i -lt $sectionEnd; $i++) {
            $match = [regex]::Match($lines[$i], $secretPattern)
            if ($match.Success) { $match }
        }
    )
    if ($secretMatches.Count -ne 1) {
        throw "$ServiceName.$SectionName.secret 必须是唯一直接子项；拒绝普通发布。"
    }
    $primary = ConvertFrom-PandoraJsonStringScalar -Json $secretMatches[0].Groups['json'].Value `
        -Where "$ServiceName.$SectionName.secret"
    if ([string]::IsNullOrEmpty($primary)) { throw "$ServiceName.$SectionName.secret 为空；拒绝普通发布。" }

    $additionalPrefix = $prefix + 'additional_secrets[ \t]*:'
    $additionalLines = @(
        for ($i = $sectionIndex + 1; $i -lt $sectionEnd; $i++) {
            if ($lines[$i] -cmatch $additionalPrefix) { $lines[$i] }
        }
    )
    if ($additionalLines.Count -gt 1) {
        throw "$ServiceName.$SectionName.additional_secrets 重复；拒绝普通发布。"
    }
    $additional = @()
    if ($additionalLines.Count -eq 1) {
        $additionalMatch = [regex]::Match($additionalLines[0],
            ($additionalPrefix + '[ \t]*(?<json>\[.*\])[ \t]*(?:#.*)?$'))
        if (-not $additionalMatch.Success) {
            throw "$ServiceName.$SectionName.additional_secrets 格式不受支持；拒绝普通发布。"
        }
        $additional = @(ConvertFrom-PandoraJsonStringArray -Json $additionalMatch.Groups['json'].Value `
            -Where "$ServiceName.$SectionName.additional_secrets")
        if ($additional.Count -eq 0) {
            throw "$ServiceName.$SectionName.additional_secrets 为空数组时必须省略；拒绝普通发布。"
        }
        if (@($additional | Where-Object { [string]::IsNullOrEmpty($_) }).Count -gt 0) {
            throw "$ServiceName.$SectionName.additional_secrets 含空值；拒绝普通发布。"
        }
    }

    $primarySha = Get-PandoraSecretSha256 -Value $primary
    $additionalSha = @($additional | ForEach-Object { Get-PandoraSecretSha256 -Value $_ })
    if ($additionalSha -contains $primarySha -or
        @($additionalSha | Sort-Object -Unique).Count -ne $additionalSha.Count) {
        throw "$ServiceName.$SectionName keyset 含重复密钥；拒绝普通发布。"
    }
    return [pscustomobject]@{
        PrimarySha256 = $primarySha
        AdditionalSha256 = $additionalSha
    }
}

function Test-PandoraStringSequenceEqual {
    param([AllowEmptyCollection()][string[]]$Left, [AllowEmptyCollection()][string[]]$Right)
    $leftValues = @($Left)
    $rightValues = @($Right)
    if ($leftValues.Count -ne $rightValues.Count) { return $false }
    for ($i = 0; $i -lt $leftValues.Count; $i++) {
        if ($leftValues[$i] -cne $rightValues[$i]) { return $false }
    }
    return $true
}

function Get-PandoraHmacDomainContract {
    param(
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$Configs,
        [Parameter(Mandatory = $true)][string[]]$ServiceNames,
        [Parameter(Mandatory = $true)][ValidateSet('jwt', 'ds_auth')][string]$SectionName,
        [Parameter(Mandatory = $true)][string]$DomainName
    )
    $baseline = $null
    foreach ($service in $ServiceNames) {
        if (-not $Configs.Contains($service) -or [string]::IsNullOrWhiteSpace([string]$Configs[$service])) {
            throw "$DomainName HMAC 连续性检查缺 $service 配置；拒绝普通发布。"
        }
        $current = Get-PandoraYamlHmacKeysetContract -ServiceName $service `
            -Text ([string]$Configs[$service]) -SectionName $SectionName
        if ($null -eq $baseline) { $baseline = $current; continue }
        if ($current.PrimarySha256 -cne $baseline.PrimarySha256 -or
            -not (Test-PandoraStringSequenceEqual $current.AdditionalSha256 $baseline.AdditionalSha256)) {
            throw "$DomainName HMAC keyset 在签发/验签服务间不一致；拒绝普通发布。"
        }
    }
    return $baseline
}

function Get-PandoraOnlineHmacContract {
    param([Parameter(Mandatory = $true)][System.Collections.IDictionary]$Configs)
    $player = Get-PandoraHmacDomainContract -Configs $Configs `
        -ServiceNames $script:PandoraPlayerHmacServices -SectionName jwt -DomainName '玩家 Session'
    $ds = Get-PandoraHmacDomainContract -Configs $Configs `
        -ServiceNames $script:PandoraDsCallbackHmacServices -SectionName ds_auth -DomainName 'DS callback'
    $playerSet = @($player.PrimarySha256) + @($player.AdditionalSha256)
    $dsSet = @($ds.PrimarySha256) + @($ds.AdditionalSha256)
    if (@($playerSet | Where-Object { $dsSet -contains $_ }).Count -gt 0) {
        throw '玩家 Session 与 DS callback HMAC keyset 不得相交；拒绝普通发布。'
    }
    return [pscustomobject]@{ Player = $player; DsCallback = $ds }
}

function Assert-PandoraOnlineHmacContinuity {
    param(
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$LiveConfigs,
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$CandidateConfigs
    )
    $live = Get-PandoraOnlineHmacContract -Configs $LiveConfigs
    $candidate = Get-PandoraOnlineHmacContract -Configs $CandidateConfigs
    if ($live.Player.PrimarySha256 -cne $candidate.Player.PrimarySha256) {
        throw '普通 online 发布检测到玩家 Session HMAC 变化；必须走独立换钥流程，本次未 push/apply。'
    }
    if (-not (Test-PandoraStringSequenceEqual $live.Player.AdditionalSha256 $candidate.Player.AdditionalSha256)) {
        throw '普通 online 发布检测到玩家 Session additional keyset 变化；必须走独立换钥流程，本次未 push/apply。'
    }
    if ($live.DsCallback.PrimarySha256 -cne $candidate.DsCallback.PrimarySha256) {
        throw '普通 online 发布检测到 DS callback HMAC 变化；必须走独立换钥流程，本次未 push/apply。'
    }
    if (-not (Test-PandoraStringSequenceEqual $live.DsCallback.AdditionalSha256 $candidate.DsCallback.AdditionalSha256)) {
        throw '普通 online 发布检测到 DS callback additional keyset 变化；必须走独立换钥流程，本次未 push/apply。'
    }
    return $candidate
}

# Placement proof 是已持久化 transition/outbox 的验签根。普通发布若误换任意一把 key，
# 冷启动恢复会把原本合法的 terminal/start proof 判成 UNKNOWN。这里严格解析每个 writer 与
# locator 的直接 YAML 子项，只返回 SHA256 指纹，并要求四个权限域彼此及与两套 HMAC 隔离。
function Get-PandoraYamlDirectStringSha256 {
    param(
        [Parameter(Mandatory = $true)][string]$ServiceName,
        [Parameter(Mandatory = $true)][string]$Text,
        [Parameter(Mandatory = $true)][string]$SectionName,
        [Parameter(Mandatory = $true)][string]$ChildName
    )
    [string[]]$lines = [regex]::Split($Text, '\r?\n')
    $sectionPattern = '^([ ]*)' + [regex]::Escape($SectionName) + ':[ \t]*(?:#.*)?$'
    $sectionIndexes = @(
        for ($i = 0; $i -lt $lines.Count; $i++) {
            if ($lines[$i] -cmatch $sectionPattern) { $i }
        }
    )
    if ($sectionIndexes.Count -ne 1) {
        throw "$ServiceName.$SectionName 必须恰好有一个节点；拒绝普通发布。"
    }
    $sectionIndex = [int]$sectionIndexes[0]
    $sectionIndent = [regex]::Match($lines[$sectionIndex], $sectionPattern).Groups[1].Value.Length
    $sectionEnd = $lines.Count
    for ($i = $sectionIndex + 1; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -cmatch '^\s*$' -or $lines[$i] -cmatch '^[ ]*#') { continue }
        $indent = [regex]::Match($lines[$i], '^([ ]*)').Groups[1].Value.Length
        if ($indent -le $sectionIndent) { $sectionEnd = $i; break }
    }
    $directIndent = [int]::MaxValue
    for ($i = $sectionIndex + 1; $i -lt $sectionEnd; $i++) {
        if ($lines[$i] -cmatch '^\s*$' -or $lines[$i] -cmatch '^[ ]*#') { continue }
        $indent = [regex]::Match($lines[$i], '^([ ]*)').Groups[1].Value.Length
        if ($indent -gt $sectionIndent -and $indent -lt $directIndent) { $directIndent = $indent }
    }
    if ($directIndent -eq [int]::MaxValue) { throw "$ServiceName.$SectionName 是空节点；拒绝普通发布。" }

    $childPattern = '^' + (' ' * $directIndent) + [regex]::Escape($ChildName) +
        '[ \t]*:[ \t]*(?<json>"(?:\\.|[^"\\])*")[ \t]*(?:#.*)?$'
    $matches = @(
        for ($i = $sectionIndex + 1; $i -lt $sectionEnd; $i++) {
            $match = [regex]::Match($lines[$i], $childPattern)
            if ($match.Success) { $match }
        }
    )
    if ($matches.Count -ne 1) {
        throw "$ServiceName.$SectionName.$ChildName 必须是唯一直接双引号字符串子项；拒绝普通发布。"
    }
    $value = ConvertFrom-PandoraJsonStringScalar -Json $matches[0].Groups['json'].Value `
        -Where "$ServiceName.$SectionName.$ChildName"
    if ([string]::IsNullOrEmpty($value)) {
        throw "$ServiceName.$SectionName.$ChildName 为空；拒绝普通发布。"
    }
    return Get-PandoraSecretSha256 -Value $value
}

function Get-PandoraOnlinePlacementContract {
    param([Parameter(Mandatory = $true)][System.Collections.IDictionary]$Configs)
    $contract = [ordered]@{}
    foreach ($domain in $script:PandoraPlacementDomains.GetEnumerator()) {
        $baseline = ''
        foreach ($binding in @($domain.Value)) {
            $service = [string]$binding.Service
            if (-not $Configs.Contains($service) -or [string]::IsNullOrWhiteSpace([string]$Configs[$service])) {
                throw "placement $($domain.Key) 连续性检查缺 $service 配置；拒绝普通发布。"
            }
            $current = Get-PandoraYamlDirectStringSha256 -ServiceName $service `
                -Text ([string]$Configs[$service]) -SectionName ([string]$binding.Section) `
                -ChildName ([string]$binding.Child)
            if ([string]::IsNullOrEmpty($baseline)) { $baseline = $current; continue }
            if ($current -cne $baseline) {
                throw "placement $($domain.Key) proof key 在 writer/locator 间不一致；拒绝普通发布。"
            }
        }
        $contract[$domain.Key] = $baseline
    }
    $placementSet = @($contract.Values)
    if (@($placementSet | Sort-Object -Unique).Count -ne $placementSet.Count) {
        throw 'placement proof 权限域不得复用 key；拒绝普通发布。'
    }
    $hmac = Get-PandoraOnlineHmacContract -Configs $Configs
    $hmacSet = @($hmac.Player.PrimarySha256) + @($hmac.Player.AdditionalSha256) +
        @($hmac.DsCallback.PrimarySha256) + @($hmac.DsCallback.AdditionalSha256)
    if (@($placementSet | Where-Object { $hmacSet -contains $_ }).Count -gt 0) {
        throw 'placement proof key 不得与玩家 Session/DS callback HMAC keyset 相交；拒绝普通发布。'
    }
    $matchResumeAuth = Get-PandoraOnlineMatchResumeAuthContract -Configs $Configs
    if ($placementSet -contains $matchResumeAuth) {
        throw 'Match resume service identity key 不得与 placement proof key 相交；拒绝普通发布。'
    }
    $allocationAbortAuth = Get-PandoraOnlineAllocationAbortAuthContract -Configs $Configs
    if ($placementSet -contains $allocationAbortAuth) {
        throw 'allocation abort service identity key 不得与 placement proof key 相交；拒绝普通发布。'
    }
    return [pscustomobject]$contract
}

function Get-PandoraOnlineMatchResumeAuthContract {
    param([Parameter(Mandatory = $true)][System.Collections.IDictionary]$Configs)
    $baseline = ''
    foreach ($binding in $script:PandoraMatchResumeAuthBindings) {
        $service = [string]$binding.Service
        if (-not $Configs.Contains($service) -or [string]::IsNullOrWhiteSpace([string]$Configs[$service])) {
            throw "Match resume service identity 连续性检查缺 $service 配置；拒绝普通发布。"
        }
        $current = Get-PandoraYamlDirectStringSha256 -ServiceName $service `
            -Text ([string]$Configs[$service]) -SectionName ([string]$binding.Section) `
            -ChildName ([string]$binding.Child)
        if ([string]::IsNullOrEmpty($baseline)) { $baseline = $current; continue }
        if ($current -cne $baseline) {
            throw 'Login/Matchmaker 的 Match resume service identity key 不一致；拒绝普通发布。'
        }
    }
    $hmac = Get-PandoraOnlineHmacContract -Configs $Configs
    $hmacSet = @($hmac.Player.PrimarySha256) + @($hmac.Player.AdditionalSha256) +
        @($hmac.DsCallback.PrimarySha256) + @($hmac.DsCallback.AdditionalSha256)
    if ($hmacSet -contains $baseline) {
        throw 'Match resume service identity key 不得与玩家 Session/DS callback HMAC keyset 相交；拒绝普通发布。'
    }
    return $baseline
}

function Assert-PandoraOnlineMatchResumeAuthContinuity {
    param(
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$LiveConfigs,
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$CandidateConfigs
    )
    $live = Get-PandoraOnlineMatchResumeAuthContract -Configs $LiveConfigs
    $candidate = Get-PandoraOnlineMatchResumeAuthContract -Configs $CandidateConfigs
    if ($live -cne $candidate) {
        throw '普通 online 发布检测到 Match resume service identity key 变化；必须走独立换钥流程，本次未 push/apply。'
    }
    return $candidate
}

function Get-PandoraOnlineAllocationAbortAuthContract {
    param([Parameter(Mandatory = $true)][System.Collections.IDictionary]$Configs)
    $baseline = ''
    foreach ($binding in $script:PandoraAllocationAbortAuthBindings) {
        $service = [string]$binding.Service
        if (-not $Configs.Contains($service) -or [string]::IsNullOrWhiteSpace([string]$Configs[$service])) {
            throw "allocation abort service identity 连续性检查缺 $service 配置；拒绝普通发布。"
        }
        $current = Get-PandoraYamlDirectStringSha256 -ServiceName $service `
            -Text ([string]$Configs[$service]) -SectionName ([string]$binding.Section) `
            -ChildName ([string]$binding.Child)
        if ([string]::IsNullOrEmpty($baseline)) { $baseline = $current; continue }
        if ($current -cne $baseline) {
            throw 'PVP/PVE Matchmaker 与 DS allocator 的 allocation abort service key 不一致；拒绝普通发布。'
        }
    }
    $hmac = Get-PandoraOnlineHmacContract -Configs $Configs
    $hmacSet = @($hmac.Player.PrimarySha256) + @($hmac.Player.AdditionalSha256) +
        @($hmac.DsCallback.PrimarySha256) + @($hmac.DsCallback.AdditionalSha256)
    if ($hmacSet -contains $baseline) {
        throw 'allocation abort service key 不得与玩家 Session/DS callback HMAC keyset 相交；拒绝普通发布。'
    }
    $matchResume = Get-PandoraOnlineMatchResumeAuthContract -Configs $Configs
    if ($matchResume -ceq $baseline) {
        throw 'allocation abort service key 不得与 Match resume service identity key 复用；拒绝普通发布。'
    }
    # Avoid recursive calls through Get-PandoraOnlinePlacementContract while
    # still checking every placement authority domain against this key.
    foreach ($domain in $script:PandoraPlacementDomains.GetEnumerator()) {
        $domainHash = ''
        foreach ($binding in $domain.Value) {
            $service = [string]$binding.Service
            if (-not $Configs.Contains($service) -or [string]::IsNullOrWhiteSpace([string]$Configs[$service])) {
                throw "allocation abort/placement distinctness 检查缺 $service 配置；拒绝普通发布。"
            }
            $current = Get-PandoraYamlDirectStringSha256 -ServiceName $service `
                -Text ([string]$Configs[$service]) -SectionName ([string]$binding.Section) `
                -ChildName ([string]$binding.Child)
            if ([string]::IsNullOrEmpty($domainHash)) { $domainHash = $current }
            elseif ($current -cne $domainHash) {
                throw "placement $($domain.Key) proof key 在 writer/verifier 间不一致；拒绝普通发布。"
            }
        }
        if ($domainHash -ceq $baseline) {
            throw "allocation abort service key 不得与 placement $($domain.Key) proof key 复用；拒绝普通发布。"
        }
    }
    return $baseline
}

function Assert-PandoraOnlineAllocationAbortAuthContinuity {
    param(
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$LiveConfigs,
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$CandidateConfigs
    )
    $live = Get-PandoraOnlineAllocationAbortAuthContract -Configs $LiveConfigs
    $candidate = Get-PandoraOnlineAllocationAbortAuthContract -Configs $CandidateConfigs
    if ($live -cne $candidate) {
        throw '普通 online 发布检测到 allocation abort service identity key 变化；必须走独立先 verifier 后 emitter 换钥流程，本次未 push/apply。'
    }
    return $candidate
}

function Get-PandoraOnlineTeamResumeAuthContract {
    param([Parameter(Mandatory = $true)][System.Collections.IDictionary]$Configs)
    $baseline = ''
    foreach ($binding in $script:PandoraTeamResumeAuthBindings) {
        $service = [string]$binding.Service
        if (-not $Configs.Contains($service) -or [string]::IsNullOrWhiteSpace([string]$Configs[$service])) {
            throw "Team resume service identity 连续性检查缺 $service 配置；拒绝普通发布。"
        }
        $current = Get-PandoraYamlDirectStringSha256 -ServiceName $service `
            -Text ([string]$Configs[$service]) -SectionName ([string]$binding.Section) `
            -ChildName ([string]$binding.Child)
        if ([string]::IsNullOrEmpty($baseline)) { $baseline = $current; continue }
        if ($current -cne $baseline) {
            throw 'Team 签名端与 Matchmaker 验签端的 Team resume service identity key 不一致；拒绝普通发布。'
        }
    }
    $hmac = Get-PandoraOnlineHmacContract -Configs $Configs
    $hmacSet = @($hmac.Player.PrimarySha256) + @($hmac.Player.AdditionalSha256) +
        @($hmac.DsCallback.PrimarySha256) + @($hmac.DsCallback.AdditionalSha256)
    if ($hmacSet -contains $baseline) {
        throw 'Team resume service identity key 不得与玩家 Session/DS callback HMAC keyset 相交；拒绝普通发布。'
    }
    # 与 login 那把复用 = 两个服务的信任域合并（任一方可冒充另一方），且轮换变成全有全无。
    $matchResume = Get-PandoraOnlineMatchResumeAuthContract -Configs $Configs
    if ($matchResume -ceq $baseline) {
        throw 'Team resume service identity key 不得与 Login→Matchmaker 的 Match resume key 复用；拒绝普通发布。'
    }
    $allocationAbort = Get-PandoraOnlineAllocationAbortAuthContract -Configs $Configs
    if ($allocationAbort -ceq $baseline) {
        throw 'Team resume service identity key 不得与 allocation abort service key 复用；拒绝普通发布。'
    }
    # 与 allocation abort 同样的理由：内联遍历 placement 权限域，避免经
    # Get-PandoraOnlinePlacementContract 递归回本函数。
    foreach ($domain in $script:PandoraPlacementDomains.GetEnumerator()) {
        $domainHash = ''
        foreach ($binding in $domain.Value) {
            $service = [string]$binding.Service
            if (-not $Configs.Contains($service) -or [string]::IsNullOrWhiteSpace([string]$Configs[$service])) {
                throw "team resume/placement distinctness 检查缺 $service 配置；拒绝普通发布。"
            }
            $current = Get-PandoraYamlDirectStringSha256 -ServiceName $service `
                -Text ([string]$Configs[$service]) -SectionName ([string]$binding.Section) `
                -ChildName ([string]$binding.Child)
            if ([string]::IsNullOrEmpty($domainHash)) { $domainHash = $current }
            elseif ($current -cne $domainHash) {
                throw "placement $($domain.Key) proof key 在 writer/verifier 间不一致；拒绝普通发布。"
            }
        }
        if ($domainHash -ceq $baseline) {
            throw "Team resume service identity key 不得与 placement $($domain.Key) proof key 复用；拒绝普通发布。"
        }
    }
    return $baseline
}

function Assert-PandoraOnlineTeamResumeAuthContinuity {
    param(
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$LiveConfigs,
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$CandidateConfigs
    )
    $live = Get-PandoraOnlineTeamResumeAuthContract -Configs $LiveConfigs
    $candidate = Get-PandoraOnlineTeamResumeAuthContract -Configs $CandidateConfigs
    if ($live -cne $candidate) {
        throw '普通 online 发布检测到 Team resume service identity key 变化；必须走独立先 verifier 后 signer 换钥流程，本次未 push/apply。'
    }
    return $candidate
}

function Assert-PandoraOnlinePlacementContinuity {
    param(
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$LiveConfigs,
        [Parameter(Mandatory = $true)][System.Collections.IDictionary]$CandidateConfigs
    )
    $live = Get-PandoraOnlinePlacementContract -Configs $LiveConfigs
    $candidate = Get-PandoraOnlinePlacementContract -Configs $CandidateConfigs
    foreach ($domain in $script:PandoraPlacementDomains.Keys) {
        if ([string]$live.$domain -cne [string]$candidate.$domain) {
            throw "普通 online 发布检测到 placement $domain proof key 变化；必须走独立换钥/证明迁移流程，本次未 push/apply。"
        }
    }
    return $candidate
}

function ConvertFrom-PandoraConfigSecretObject {
    param(
        [Parameter(Mandatory = $true)][object]$SecretObject,
        [Parameter(Mandatory = $true)][string[]]$ExpectedServiceNames
    )
    if ([string]$SecretObject.kind -cne 'Secret' -or
        [string]$SecretObject.metadata.name -cne 'pandora-config' -or $null -eq $SecretObject.data) {
        throw 'live pandora-config 不是预期 Secret；拒绝普通发布。'
    }
    $expectedFiles = @($ExpectedServiceNames | ForEach-Object { "$_.yaml" } | Sort-Object)
    $actualFiles = @($SecretObject.data.PSObject.Properties.Name | Sort-Object)
    if (-not (Test-PandoraStringSequenceEqual $actualFiles $expectedFiles)) {
        throw 'live Secret/pandora-config 的配置文件 key 集不精确；拒绝普通发布。'
    }
    $utf8 = [System.Text.UTF8Encoding]::new($false, $true)
    $configs = @{}
    foreach ($service in $ExpectedServiceNames) {
        $file = "$service.yaml"
        try {
            $encoded = [string]$SecretObject.data.PSObject.Properties[$file].Value
            $configs[$service] = $utf8.GetString([Convert]::FromBase64String($encoded))
        } catch {
            throw "live Secret/pandora-config 的 $file 不是合法 base64 UTF-8；拒绝普通发布（不回显内容）。"
        }
    }
    return $configs
}

function Get-PandoraGeneratedConfigObject {
    param(
        [Parameter(Mandatory = $true)][string]$ConfigDir,
        [Parameter(Mandatory = $true)][string[]]$ExpectedServiceNames
    )
    if (-not (Test-Path -LiteralPath $ConfigDir -PathType Container)) {
        throw 'online 候选配置目录不存在；拒绝发布。'
    }
    $expectedFiles = @($ExpectedServiceNames | ForEach-Object { "$_.yaml" } | Sort-Object)
    $actualFiles = @(Get-ChildItem -LiteralPath $ConfigDir -File -Filter '*.yaml' |
        ForEach-Object Name | Sort-Object)
    if (-not (Test-PandoraStringSequenceEqual $actualFiles $expectedFiles)) {
        throw 'online 候选配置 YAML 文件集不精确；拒绝发布。'
    }
    $configs = @{}
    foreach ($service in $ExpectedServiceNames) {
        $configs[$service] = Get-Content -LiteralPath (Join-Path $ConfigDir "$service.yaml") -Raw
    }
    return $configs
}

function Assert-PandoraDigest {
    param([Parameter(Mandatory = $true)][string]$Digest, [string]$Where = 'digest')
    if ($Digest -cnotmatch $script:PandoraDigestPattern) {
        throw "$Where 必须是 sha256:<64 个小写十六进制字符>，当前='$Digest'。"
    }
}

function Get-PandoraImageDigestFromReference {
    param([Parameter(Mandatory = $true)][string]$Reference)
    $m = [regex]::Match($Reference.Trim(), $script:PandoraImagePinPattern)
    if (-not $m.Success) { return '' }
    return $m.Groups['digest'].Value
}

function Get-PandoraImageRepository {
    param([Parameter(Mandatory = $true)][string]$Reference)
    $value = $Reference.Trim()
    if ([string]::IsNullOrWhiteSpace($value) -or $value -match '\s') {
        throw "镜像引用为空或含空白:$Reference"
    }
    $at = $value.IndexOf('@')
    if ($at -ge 0) { return $value.Substring(0, $at) }

    # registry 可带端口；只有最后一个 '/' 之后的 ':' 才是 tag 分隔符。
    $slash = $value.LastIndexOf('/')
    $colon = $value.LastIndexOf(':')
    if ($colon -gt $slash) { return $value.Substring(0, $colon) }
    return $value
}

function New-PandoraPinnedImageReference {
    param(
        [Parameter(Mandatory = $true)][string]$Reference,
        [Parameter(Mandatory = $true)][string]$Digest
    )
    Assert-PandoraDigest -Digest $Digest -Where "镜像 $Reference 的 digest"
    return (Get-PandoraImageRepository $Reference) + '@' + $Digest
}

function ConvertFrom-PandoraImagetoolsInspect {
    param(
        [Parameter(Mandatory = $true)][string]$Reference,
        [Parameter(Mandatory = $true)][string]$Output
    )
    $digestMatches = [regex]::Matches($Output, '(?im)^Digest:\s*(sha256:[0-9a-f]{64})\s*$')
    if ($digestMatches.Count -ne 1) {
        throw "registry inspect 未返回唯一顶层 digest:$Reference;count=$($digestMatches.Count)"
    }
    $media = [regex]::Match($Output, '(?im)^MediaType:\s*(\S+)\s*$')
    if (-not $media.Success) { throw "registry inspect 缺 MediaType:$Reference" }
    if ($media.Groups[1].Value -match '(?i)(image\.index|manifest\.list)') {
        throw "online 当前只接受单平台 manifest，拒绝 index/list:$Reference media=$($media.Groups[1].Value)。"
    }
    $digest = $digestMatches[0].Groups[1].Value.ToLowerInvariant()
    return [pscustomobject]@{
        Reference = $Reference
        Digest = $digest
        Pinned = New-PandoraPinnedImageReference -Reference $Reference -Digest $digest
        MediaType = $media.Groups[1].Value
    }
}

function Test-PandoraManifestNotFoundOutput {
    param([Parameter(Mandatory = $true)][string]$Output)
    $value = $Output.ToLowerInvariant()
    # 混合输出中只要出现鉴权/TLS/网络失败，就不能用同一段里的 manifest unknown 推断“不存在”。
    if ($value -match '(unauthori[sz]ed|authentication required|denied|forbidden|tls|x509|certificate|timeout|timed out|connection|connect:|dial |dns|network|unexpected eof)') {
        return $false
    }
    return $value -match '(manifest unknown|no such manifest|manifest[^\r\n]*not found|not found[^\r\n]*manifest)'
}

function Get-PandoraPushDigest {
    param([Parameter(Mandatory = $true)][string]$Output)
    $digests = @([regex]::Matches($Output, '(?i)digest:\s*(sha256:[0-9a-f]{64})') |
        ForEach-Object { $_.Groups[1].Value.ToLowerInvariant() } | Sort-Object -Unique)
    if ($digests.Count -ne 1) {
        throw "docker push 未返回唯一 digest;count=$($digests.Count)"
    }
    return $digests[0]
}

function Assert-PandoraCleanGitStatus {
    param([AllowEmptyString()][string]$Output)
    if (-not [string]::IsNullOrWhiteSpace($Output)) {
        throw 'online BuildPush 只允许从 clean worktree 发布；请先由人/Codex 按项目规则审核并提交全部变更。'
    }
}

function Assert-PandoraImageRevision {
    param(
        [Parameter(Mandatory = $true)][string]$Reference,
        [AllowEmptyString()][string]$Actual,
        [Parameter(Mandatory = $true)][string]$Expected
    )
    $want = $Expected.Trim().ToLowerInvariant()
    $got = $Actual.Trim().ToLowerInvariant()
    if ($want -cnotmatch '^[0-9a-f]{7,40}$') { throw "期望镜像 revision 非法:$Expected" }
    if ($got -cne $want) {
        throw "本地镜像 provenance 不匹配:$Reference revision='$got' expected='$want'。禁止把旧镜像冒充当前提交发布。"
    }
}

function Assert-PandoraImmutableReleaseTag {
    param(
        [Parameter(Mandatory = $true)][string]$Tag,
        [string]$CurrentCommit = '',
        [switch]$RequireCurrentCommit
    )
    $value = $Tag.Trim()
    if ($value -cnotmatch '^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$') {
        throw "online tag 不符合 OCI/Docker tag 语法:$Tag"
    }
    if ($value -match '^(?i:dev|latest|stable|prod|production|test)$') {
        throw "online 禁止复用可变 tag '$value'；必须使用含 git SHA 的不可变 tag。"
    }
    if ($value -cnotmatch '(^|[._-])[0-9a-f]{7,40}($|[._-])') {
        throw "online tag '$value' 必须包含独立的 7~40 位小写 git SHA 段。"
    }
    if ($RequireCurrentCommit) {
        $commit = $CurrentCommit.Trim().ToLowerInvariant()
        if ($commit -cnotmatch '^[0-9a-f]{7,40}$') {
            throw "无法校验当前 git commit:$CurrentCommit"
        }
        if ($value.ToLowerInvariant() -notmatch ('(^|[._-])' + [regex]::Escape($commit) + '($|[._-])')) {
            throw "BuildPush tag '$value' 必须包含当前 commit '$commit'，防止把当前源码覆盖到别的版本 tag。"
        }
    }
}

function ConvertTo-PandoraDigestKustomization {
    param(
        [Parameter(Mandatory = $true)][string]$Template,
        [Parameter(Mandatory = $true)][string]$Registry,
        [Parameter(Mandatory = $true)][hashtable]$Digests,
        [Parameter(Mandatory = $true)][string[]]$ServiceNames
    )
    $out = $Template
    $registryRoot = $Registry.Trim().TrimEnd('/')
    if ([string]::IsNullOrWhiteSpace($registryRoot)) { throw 'Registry 不能为空。' }

    foreach ($name in $ServiceNames) {
        if (-not $Digests.ContainsKey($name)) { throw "缺少服务 $name 的 registry digest。" }
        $digest = [string]$Digests[$name]
        Assert-PandoraDigest -Digest $digest -Where "服务 $name"
        $pattern = '(?m)^\s*-\s*\{\s*name:\s*pandora/' + [regex]::Escape($name) + '\s*,[^\r\n]*\}\s*$'
        $matches = [regex]::Matches($out, $pattern)
        if ($matches.Count -ne 1) {
            throw "online kustomization 中 pandora/$name 镜像条目数量=$($matches.Count)，应为 1。"
        }
        $replacement = "  - { name: pandora/$name, newName: $registryRoot/pandora/$name, digest: $digest }"
        $out = [regex]::Replace($out, $pattern, $replacement)
    }
    if ($out -match '(?m)^\s*newTag:\s*|newTag:\s*[^,}\r\n]+') {
        throw 'online 最终 kustomization 仍含 newTag，拒绝 tag 发布。'
    }
    return $out
}

function Add-PandoraWriterPatchEntries {
    param(
        [Parameter(Mandatory = $true)][string]$Kustomization,
        [Parameter(Mandatory = $true)][string[]]$WriterServices = $script:PandoraWriterServices,
        [string[]]$AdditionalPatchPaths = @()
    )
    if ($Kustomization -match '(?m)^patches\s*:') {
        throw 'online runtime kustomization 已含 patches；当前生成器拒绝隐式合并，避免覆盖人工补丁。'
    }
    $lines = [System.Collections.Generic.List[string]]::new()
    $lines.Add($Kustomization.TrimEnd())
    $lines.Add('')
    $lines.Add('patches:')
    foreach ($name in $WriterServices) {
        $lines.Add("  - path: writer-digest-$name.yaml")
    }
    foreach ($path in $AdditionalPatchPaths) {
        if ($path -cnotmatch '^[A-Za-z0-9._-]+\.yaml$') { throw "online runtime patch 文件名非法:$path" }
        $lines.Add("  - path: $path")
    }
    return ($lines -join [Environment]::NewLine) + [Environment]::NewLine
}

function New-PandoraLoginDSTicketJWKSRevisionPatch {
    param([Parameter(Mandatory = $true)][ValidateRange(1, 2147483647)][int]$Revision)
    return @"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: login
  namespace: pandora
spec:
  template:
    spec:
      volumes:
        - name: dsticket-jwks
          configMap:
            name: pandora-dsticket-jwks-r$Revision
            defaultMode: 0444
"@
}

function New-PandoraDSTicketSignerSecretRevisionPatch {
    param(
        [Parameter(Mandatory = $true)][ValidateSet('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')][string]$Service,
        [Parameter(Mandatory = $true)][ValidateRange(1, 2147483647)][int]$Revision
    )
    return @"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $Service
  namespace: pandora
spec:
  template:
    spec:
      volumes:
        - name: dsticket
          secret:
            secretName: pandora-dsticket-signer-r$Revision
            defaultMode: 0440
"@
}

function Assert-PandoraDSTicketSignerRevisionContract {
    param(
        [Parameter(Mandatory = $true)][string[]]$ContractRows,
        [Parameter(Mandatory = $true)][ValidateRange(1, 2147483647)][int]$Revision
    )
    $signers = @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')
    $expectedSecret = "pandora-dsticket-signer-r$Revision"
    $expectedJWKS = "pandora-dsticket-jwks-r$Revision"
    $seen = @{}
    foreach ($row in $ContractRows) {
        if ([string]::IsNullOrWhiteSpace($row)) { continue }
        $fields = @([regex]::Split($row, "`t"))
        if ($fields.Count -ne 4) { throw "DSTicket runtime contract 列数=$($fields.Count)，应为 4:$row" }
        if ([string]$fields[0] -cne 'Deployment') { continue }
        $name = [string]$fields[1]
        $secretName = [string]$fields[2]
        $jwksName = [string]$fields[3]
        if ($signers -contains $name) {
            if ($seen.ContainsKey($name)) { throw "DSTicket runtime contract 重复 Deployment/$name。" }
            $seen[$name] = $true
            if ($secretName -cne $expectedSecret) {
                throw "Deployment/$name DSTicket signer Secret=$secretName，expected=$expectedSecret。"
            }
        } elseif (-not [string]::IsNullOrWhiteSpace($secretName)) {
            throw "非签发方 Deployment/$name 不得挂 DSTicket signer Secret:$secretName。"
        }
        if ($name -ceq 'login') {
            if ($jwksName -cne $expectedJWKS) {
                throw "Deployment/login DSTicket JWKS=$jwksName，expected=$expectedJWKS。"
            }
        } elseif (-not [string]::IsNullOrWhiteSpace($jwksName)) {
            throw "Deployment/$name 不得挂 Login-only DSTicket JWKS:$jwksName。"
        }
    }
    foreach ($service in $signers) {
        if (-not $seen.ContainsKey($service)) { throw "DSTicket runtime contract 缺 Deployment/$service。" }
    }
}

# 读取 kubectl JSON/纯测试对象的可选属性。普通发布的父控制器扫描会面对不同
# Kubernetes 版本生成的对象，缺可选字段时必须按“没有该线索”处理，不能被
# StrictMode 的 PropertyNotFoundException 打断后误走空快照分支。
function Get-PandoraOnlineObjectProperty {
    param(
        [AllowNull()]$Object,
        [Parameter(Mandatory = $true)][string]$Name,
        $Default = $null
    )
    if ($null -eq $Object) { return $Default }
    if ($Object -is [System.Collections.IDictionary] -and $Object.Contains($Name)) {
        return $Object[$Name]
    }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) { return $Default }
    return $property.Value
}

function Test-PandoraOnlineDSTicketReferenceName {
    param([AllowEmptyString()][string]$Name)
    # pandora-dsticket* 全部是保留 material 前缀；r0/r01/backup 等畸形名也必须进入 parent clue。
    return $Name -cmatch '^pandora-dsticket(?:-|$)' -or
        $Name -cmatch '^pandora-config-dsticket(?:-|$)'
}

# 找出 Pod template 中所有能消费 DSTicket material 的结构，不只看 direct Secret
# volume：projected source、env/valueFrom、envFrom、init/ephemeral container 同样能在
# parent 刚创建而尚未生成 RS/GSS/Pod 时异步拉起旧 signer/verifier。
function Test-PandoraOnlineDSTicketPodSpecClue {
    param(
        [AllowNull()]$PodSpec,
        [Parameter(Mandatory = $true)][ValidateSet('Signer', 'DS')][string]$ControllerKind
    )
    if ($null -eq $PodSpec) { return $false }

    foreach ($volume in @(Get-PandoraOnlineObjectProperty $PodSpec 'volumes' @())) {
        $volumeName = [string](Get-PandoraOnlineObjectProperty $volume 'name')
        if ($volumeName -cin @('dsticket', 'dsticket-jwks')) { return $true }
        $secret = Get-PandoraOnlineObjectProperty $volume 'secret'
        $configMap = Get-PandoraOnlineObjectProperty $volume 'configMap'
        if (Test-PandoraOnlineDSTicketReferenceName ([string](Get-PandoraOnlineObjectProperty $secret 'secretName'))) {
            return $true
        }
        foreach ($item in @(Get-PandoraOnlineObjectProperty $secret 'items' @())) {
            if ([string](Get-PandoraOnlineObjectProperty $item 'key') -imatch '(?:^|[-_.])private(?:[-_.]|$)|\.pem$' -or
                [string](Get-PandoraOnlineObjectProperty $item 'path') -imatch '(?:^|/)private(?:[-_.]|/|$)|\.pem$') {
                return $true
            }
        }
        if (Test-PandoraOnlineDSTicketReferenceName ([string](Get-PandoraOnlineObjectProperty $configMap 'name'))) {
            return $true
        }
        $projected = Get-PandoraOnlineObjectProperty $volume 'projected'
        foreach ($source in @(Get-PandoraOnlineObjectProperty $projected 'sources' @())) {
            $projectedSecret = Get-PandoraOnlineObjectProperty $source 'secret'
            $projectedConfigMap = Get-PandoraOnlineObjectProperty $source 'configMap'
            if (Test-PandoraOnlineDSTicketReferenceName ([string](Get-PandoraOnlineObjectProperty $projectedSecret 'name'))) {
                return $true
            }
            foreach ($item in @(Get-PandoraOnlineObjectProperty $projectedSecret 'items' @())) {
                if ([string](Get-PandoraOnlineObjectProperty $item 'key') -imatch '(?:^|[-_.])private(?:[-_.]|$)|\.pem$' -or
                    [string](Get-PandoraOnlineObjectProperty $item 'path') -imatch '(?:^|/)private(?:[-_.]|/|$)|\.pem$') {
                    return $true
                }
            }
            if (Test-PandoraOnlineDSTicketReferenceName ([string](Get-PandoraOnlineObjectProperty $projectedConfigMap 'name'))) {
                return $true
            }
        }
        $csi = Get-PandoraOnlineObjectProperty $volume 'csi'
        $csiAttributes = Get-PandoraOnlineObjectProperty $csi 'volumeAttributes'
        if ([string](Get-PandoraOnlineObjectProperty $csiAttributes 'secretProviderClass') -cmatch '(?i)dsticket') {
            return $true
        }
    }

    $containers = @()
    foreach ($field in @('containers', 'initContainers', 'ephemeralContainers')) {
        $containers += @(Get-PandoraOnlineObjectProperty $PodSpec $field @())
    }
    foreach ($container in $containers) {
        $containerName = [string](Get-PandoraOnlineObjectProperty $container 'name')
        if ($ControllerKind -ceq 'Signer' -and
            $containerName -cin @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')) {
            return $true
        }
        if ($ControllerKind -ceq 'DS' -and
            $containerName -cin @('pandora-battle-ds', 'pandora-hub-ds')) {
            return $true
        }
        foreach ($env in @(Get-PandoraOnlineObjectProperty $container 'env' @())) {
            $envName = [string](Get-PandoraOnlineObjectProperty $env 'name')
            $envValue = [string](Get-PandoraOnlineObjectProperty $env 'value')
            if ($envName -imatch '^PANDORA_(?:DS_TICKET_SECRET|JWT_SECRET|PLAYER_JWT_SECRET|DSTICKET_(?:.*))$' -or
                $envValue -imatch 'BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY|["'']?kty["'']?\s*:\s*["'']?oct["'']?|/run/secrets/pandora-dsticket|/etc/pandora/dsticket/private|private\.pem') {
                return $true
            }
            $valueFrom = Get-PandoraOnlineObjectProperty $env 'valueFrom'
            foreach ($referenceKind in @('secretKeyRef', 'configMapKeyRef')) {
                $reference = Get-PandoraOnlineObjectProperty $valueFrom $referenceKind
                if (Test-PandoraOnlineDSTicketReferenceName ([string](Get-PandoraOnlineObjectProperty $reference 'name'))) {
                    return $true
                }
                if ($referenceKind -ceq 'secretKeyRef' -and
                    [string](Get-PandoraOnlineObjectProperty $reference 'key') -imatch '(?:^|[-_.])private(?:[-_.]|$)|\.pem$') {
                    return $true
                }
            }
        }
        foreach ($envFrom in @(Get-PandoraOnlineObjectProperty $container 'envFrom' @())) {
            foreach ($referenceKind in @('secretRef', 'configMapRef')) {
                $reference = Get-PandoraOnlineObjectProperty $envFrom $referenceKind
                if (Test-PandoraOnlineDSTicketReferenceName ([string](Get-PandoraOnlineObjectProperty $reference 'name'))) {
                    return $true
                }
            }
        }
        foreach ($mount in @(Get-PandoraOnlineObjectProperty $container 'volumeMounts' @())) {
            $mountName = [string](Get-PandoraOnlineObjectProperty $mount 'name')
            $mountPath = [string](Get-PandoraOnlineObjectProperty $mount 'mountPath')
            $subPath = [string](Get-PandoraOnlineObjectProperty $mount 'subPath')
            if ($mountName -cin @('dsticket', 'dsticket-jwks') -or
                $mountPath -imatch '/run/secrets/pandora-dsticket|/etc/pandora/dsticket/private|(?:^|/)private(?:[-_.]|/|$)|\.pem(?:/|$)|(?:^|/)oct(?:/|$)' -or
                $subPath -imatch '(?:^|[-_.])private(?:[-_.]|$)|\.pem$') {
                return $true
            }
        }
    }
    return $false
}

# 普通发布只管理四个 signer Deployment 与四个 Stable/Canary Fleet。这里扫描全量
# parent controller，而不是等子控制器出现后才发现：相关但不在 4+4 白名单中的
# paused/replicas=0 对象也能稍后异步补出旧 Pod，因此一律 fail-closed。
function Assert-PandoraOrdinaryDSTicketControllerScope {
    param(
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$DeploymentObjects,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$FleetObjects
    )
    $expectedSigners = @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')
    $expectedFleets = @('pandora-battle-stable', 'pandora-battle-canary', 'pandora-hub-stable', 'pandora-hub-canary')
    $managedDeployments = [System.Collections.Generic.List[object]]::new()
    $managedFleets = [System.Collections.Generic.List[object]]::new()

    foreach ($deployment in @($DeploymentObjects)) {
        $metadata = Get-PandoraOnlineObjectProperty $deployment 'metadata'
        $name = [string](Get-PandoraOnlineObjectProperty $metadata 'name')
        if ($expectedSigners -ccontains $name) {
            $managedDeployments.Add($deployment)
            continue
        }
        $labels = Get-PandoraOnlineObjectProperty $metadata 'labels'
        $template = Get-PandoraOnlineObjectProperty (Get-PandoraOnlineObjectProperty $deployment 'spec') 'template'
        $templateMetadata = Get-PandoraOnlineObjectProperty $template 'metadata'
        $templateLabels = Get-PandoraOnlineObjectProperty $templateMetadata 'labels'
        $podSpec = Get-PandoraOnlineObjectProperty $template 'spec'
        $labelValues = @(
            [string](Get-PandoraOnlineObjectProperty $labels 'app'),
            [string](Get-PandoraOnlineObjectProperty $labels 'app.kubernetes.io/name'),
            [string](Get-PandoraOnlineObjectProperty $labels 'app.kubernetes.io/component'),
            [string](Get-PandoraOnlineObjectProperty $templateLabels 'app'),
            [string](Get-PandoraOnlineObjectProperty $templateLabels 'app.kubernetes.io/name'),
            [string](Get-PandoraOnlineObjectProperty $templateLabels 'app.kubernetes.io/component')
        )
        $ownerNames = @(@(Get-PandoraOnlineObjectProperty $metadata 'ownerReferences' @()) |
            ForEach-Object { [string](Get-PandoraOnlineObjectProperty $_ 'name') })
        $historicalName = $name -cmatch '^(?:pandora-)?(?:login|matchmaker(?:-pve)?|hub-allocator)(?:-|$)'
        $labelClue = @($labelValues | Where-Object {
            $expectedSigners -ccontains $_ -or
            $_ -cmatch '^(?:pandora-)?(?:login|matchmaker(?:-pve)?|hub-allocator)(?:-|$)'
        }).Count -gt 0
        $ownerClue = @($ownerNames | Where-Object {
            $_ -cmatch '^(?:pandora-)?(?:login|matchmaker(?:-pve)?|hub-allocator)(?:-|$)'
        }).Count -gt 0
        if ($historicalName -or $labelClue -or $ownerClue -or
            (Test-PandoraOnlineDSTicketPodSpecClue -PodSpec $podSpec -ControllerKind Signer)) {
            throw "检测到普通发布白名单外的 DSTicket signer parent Deployment/$name；即使 paused/replicas=0 也可能异步补出旧 signer。"
        }
    }

    foreach ($fleet in @($FleetObjects)) {
        $metadata = Get-PandoraOnlineObjectProperty $fleet 'metadata'
        $name = [string](Get-PandoraOnlineObjectProperty $metadata 'name')
        if ($expectedFleets -ccontains $name) {
            $managedFleets.Add($fleet)
            continue
        }
        $labels = Get-PandoraOnlineObjectProperty $metadata 'labels'
        $fleetTemplate = Get-PandoraOnlineObjectProperty (Get-PandoraOnlineObjectProperty $fleet 'spec') 'template'
        $fleetTemplateMetadata = Get-PandoraOnlineObjectProperty $fleetTemplate 'metadata'
        $gameServerSpec = Get-PandoraOnlineObjectProperty $fleetTemplate 'spec'
        $podTemplate = Get-PandoraOnlineObjectProperty $gameServerSpec 'template'
        $podTemplateMetadata = Get-PandoraOnlineObjectProperty $podTemplate 'metadata'
        $podSpec = Get-PandoraOnlineObjectProperty $podTemplate 'spec'
        $fleetTemplateLabels = Get-PandoraOnlineObjectProperty $fleetTemplateMetadata 'labels'
        $podTemplateLabels = Get-PandoraOnlineObjectProperty $podTemplateMetadata 'labels'
        $labelValues = @(
            [string](Get-PandoraOnlineObjectProperty $labels 'app'),
            [string](Get-PandoraOnlineObjectProperty $labels 'app.kubernetes.io/name'),
            [string](Get-PandoraOnlineObjectProperty $labels 'agones.dev/fleet'),
            [string](Get-PandoraOnlineObjectProperty $fleetTemplateLabels 'agones.dev/fleet'),
            [string](Get-PandoraOnlineObjectProperty $podTemplateLabels 'agones.dev/fleet')
        )
        $ownerNames = @(@(Get-PandoraOnlineObjectProperty $metadata 'ownerReferences' @()) |
            ForEach-Object { [string](Get-PandoraOnlineObjectProperty $_ 'name') })
        $historicalName = $name -cmatch '^(?:pandora-)?(?:battle|hub)(?:-|$)'
        $labelClue = @($labelValues | Where-Object {
            $_ -cmatch '^(?:pandora-)?(?:battle|hub)(?:-|$)' -or $_ -cin @('pandora-battle-ds', 'pandora-hub-ds')
        }).Count -gt 0
        $ownerClue = @($ownerNames | Where-Object {
            $_ -cmatch '^(?:pandora-)?(?:battle|hub)(?:-|$)'
        }).Count -gt 0
        if ($historicalName -or $labelClue -or $ownerClue -or
            (Test-PandoraOnlineDSTicketPodSpecClue -PodSpec $podSpec -ControllerKind DS)) {
            throw "检测到普通发布白名单外的 DSTicket/DS parent Fleet/$name；即使 replicas=0 也可能异步补出旧 verifier。"
        }
    }

    return [pscustomobject]@{
        DeploymentObjects = @($managedDeployments)
        FleetObjects = @($managedFleets)
        LegacyControllerObjects = @()
    }
}

# 从全量 namespace Pod/ReplicaSet/GameServerSet/Pod 中闭合 DSTicket child scope。
# 不能只看 direct volume 或标准容器名：projected、env/valueFrom、envFrom 以及
# init/ephemeral container 都可能在 parent 快照完成后继续消费旧 material。
function Get-PandoraOrdinaryDSTicketChildScope {
    param(
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$PandoraPodObjects,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$ReplicaSetObjects,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$GameServerObjects,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$GameServerSetObjects,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$DefaultPodObjects
    )
    foreach ($helper in @(
        'Get-PandoraDSTicketSignerSecretReferences',
        'Test-PandoraDSTicketDSPodSpecClue',
        'Get-PandoraGameServerSetPodSpec'
    )) {
        if (-not (Get-Command $helper -ErrorAction SilentlyContinue)) {
            throw "未加载 dsticket_rotation_contract.ps1 helper/$helper，无法闭合 ordinary child scope。"
        }
    }
    $expectedSigners = @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')
    $expectedFleets = @('pandora-battle-stable', 'pandora-battle-canary', 'pandora-hub-stable', 'pandora-hub-canary')
    $relatedReplicaSets = [System.Collections.Generic.List[object]]::new()
    $relatedSignerPods = [System.Collections.Generic.List[object]]::new()
    $relatedGameServerSets = [System.Collections.Generic.List[object]]::new()
    $relatedDSPods = [System.Collections.Generic.List[object]]::new()
    $legacyObjects = [System.Collections.Generic.List[object]]::new()
    $relatedRSNames = @{}
    $relatedRSUids = @{}

    foreach ($rs in @($ReplicaSetObjects)) {
        $metadata = Get-PandoraOnlineObjectProperty $rs 'metadata'
        $name = [string](Get-PandoraOnlineObjectProperty $metadata 'name')
        $uid = [string](Get-PandoraOnlineObjectProperty $metadata 'uid')
        $labels = Get-PandoraOnlineObjectProperty $metadata 'labels'
        $template = Get-PandoraOnlineObjectProperty (Get-PandoraOnlineObjectProperty $rs 'spec') 'template'
        $templateLabels = Get-PandoraOnlineObjectProperty (Get-PandoraOnlineObjectProperty $template 'metadata') 'labels'
        $podSpec = Get-PandoraOnlineObjectProperty $template 'spec'
        $apps = @(
            [string](Get-PandoraOnlineObjectProperty $labels 'app'),
            [string](Get-PandoraOnlineObjectProperty $templateLabels 'app')
        )
        $owners = @(Get-PandoraOnlineObjectProperty $metadata 'ownerReferences' @())
        $ownerNames = @($owners | ForEach-Object { [string](Get-PandoraOnlineObjectProperty $_ 'name') })
        $expectedOwner = @($ownerNames | Where-Object { $expectedSigners -ccontains $_ }).Count -gt 0
        $historicalOwner = @($ownerNames | Where-Object {
            $_ -cmatch '^(?:pandora-)?(?:login|matchmaker(?:-pve)?|hub-allocator)(?:-|$)'
        }).Count -gt 0
        $appClue = @($apps | Where-Object { $expectedSigners -ccontains $_ }).Count -gt 0
        $nameClue = $name -cmatch '^(?:pandora-)?(?:login|matchmaker(?:-pve)?|hub-allocator)(?:-|$)'
        $referenceClue = @(Get-PandoraDSTicketSignerSecretReferences $podSpec).Count -gt 0
        $broaderClue = Test-PandoraOnlineDSTicketPodSpecClue -PodSpec $podSpec -ControllerKind Signer
        if (-not ($expectedOwner -or $historicalOwner -or $appClue -or $nameClue -or $referenceClue -or $broaderClue)) {
            continue
        }
        $relatedReplicaSets.Add($rs)
        if (-not [string]::IsNullOrWhiteSpace($name)) { $relatedRSNames[$name] = $true }
        if (-not [string]::IsNullOrWhiteSpace($uid)) { $relatedRSUids[$uid] = $true }
        if (-not $expectedOwner -and ($referenceClue -or $broaderClue)) { $legacyObjects.Add($rs) }
    }

    foreach ($pod in @($PandoraPodObjects)) {
        $metadata = Get-PandoraOnlineObjectProperty $pod 'metadata'
        $name = [string](Get-PandoraOnlineObjectProperty $metadata 'name')
        $labels = Get-PandoraOnlineObjectProperty $metadata 'labels'
        $app = [string](Get-PandoraOnlineObjectProperty $labels 'app')
        $owners = @(Get-PandoraOnlineObjectProperty $metadata 'ownerReferences' @())
        $ownerNames = @($owners | ForEach-Object { [string](Get-PandoraOnlineObjectProperty $_ 'name') })
        $ownerUids = @($owners | ForEach-Object { [string](Get-PandoraOnlineObjectProperty $_ 'uid') })
        $relatedOwner = @($ownerNames | Where-Object { $relatedRSNames.ContainsKey($_) }).Count -gt 0 -or
            @($ownerUids | Where-Object { $relatedRSUids.ContainsKey($_) }).Count -gt 0
        $historicalOwner = @($ownerNames | Where-Object {
            $_ -cmatch '^(?:pandora-)?(?:login|matchmaker(?:-pve)?|hub-allocator)(?:-|$)'
        }).Count -gt 0
        $appClue = $expectedSigners -ccontains $app
        $nameClue = $name -cmatch '^(?:pandora-)?(?:login|matchmaker(?:-pve)?|hub-allocator)(?:-|$)'
        $podSpec = Get-PandoraOnlineObjectProperty $pod 'spec'
        $referenceClue = @(Get-PandoraDSTicketSignerSecretReferences $podSpec).Count -gt 0
        $broaderClue = Test-PandoraOnlineDSTicketPodSpecClue -PodSpec $podSpec -ControllerKind Signer
        if (-not ($relatedOwner -or $historicalOwner -or $appClue -or $nameClue -or $referenceClue -or $broaderClue)) {
            continue
        }
        $relatedSignerPods.Add($pod)
        # shared signer gate 以受管 app 或 signer ref 识别 Pod；owner/name-only 且 app
        # 错标的 child 也必须由 legacy 二次防线兜住，不能因缺 material ref 被忽略。
        if (-not $appClue) { $legacyObjects.Add($pod) }
    }

    foreach ($gss in @($GameServerSetObjects)) {
        $metadata = Get-PandoraOnlineObjectProperty $gss 'metadata'
        $name = [string](Get-PandoraOnlineObjectProperty $metadata 'name')
        $labels = Get-PandoraOnlineObjectProperty $metadata 'labels'
        $fleetLabel = [string](Get-PandoraOnlineObjectProperty $labels 'agones.dev/fleet')
        $owners = @(Get-PandoraOnlineObjectProperty $metadata 'ownerReferences' @())
        $ownerNames = @($owners | ForEach-Object { [string](Get-PandoraOnlineObjectProperty $_ 'name') })
        $expectedOwner = $expectedFleets -ccontains $fleetLabel -or
            @($ownerNames | Where-Object { $expectedFleets -ccontains $_ }).Count -gt 0
        $historicalOwner = $fleetLabel -cmatch '^(?:pandora-)?(?:battle|hub)(?:-|$)' -or
            @($ownerNames | Where-Object { $_ -cmatch '^(?:pandora-)?(?:battle|hub)(?:-|$)' }).Count -gt 0
        $nameClue = $name -cmatch '^(?:pandora-)?(?:battle|hub)(?:-|$)'
        # Agones 历史对象既可能是 nested spec.template.spec.template.spec，也可能是
        # legacy/direct spec.template.spec；共享 parser 对两种 schema 做互斥闭包校验。
        $podSpec = Get-PandoraGameServerSetPodSpec $gss "GameServerSet/$name"
        $specClue = Test-PandoraDSTicketDSPodSpecClue $podSpec
        if (-not ($expectedOwner -or $historicalOwner -or $nameClue -or $specClue)) { continue }
        $relatedGameServerSets.Add($gss)
        $containerNames = @(@(Get-PandoraOnlineObjectProperty $podSpec 'containers' @()) |
            ForEach-Object { [string](Get-PandoraOnlineObjectProperty $_ 'name') })
        $standardContainer = @($containerNames | Where-Object {
            $_ -cin @('pandora-battle-ds', 'pandora-hub-ds')
        }).Count -gt 0
        if ((-not $expectedOwner -and $specClue) -or (-not $standardContainer -and $specClue)) {
            $legacyObjects.Add($gss)
        }
    }

    $gameServerNames = @{}
    $gameServerUids = @{}
    foreach ($gameServer in @($GameServerObjects)) {
        $metadata = Get-PandoraOnlineObjectProperty $gameServer 'metadata'
        $name = [string](Get-PandoraOnlineObjectProperty $metadata 'name')
        $uid = [string](Get-PandoraOnlineObjectProperty $metadata 'uid')
        if (-not [string]::IsNullOrWhiteSpace($name)) { $gameServerNames[$name] = $true }
        if (-not [string]::IsNullOrWhiteSpace($uid)) { $gameServerUids[$uid] = $true }
    }
    foreach ($pod in @($DefaultPodObjects)) {
        $metadata = Get-PandoraOnlineObjectProperty $pod 'metadata'
        $name = [string](Get-PandoraOnlineObjectProperty $metadata 'name')
        $labels = Get-PandoraOnlineObjectProperty $metadata 'labels'
        $fleetLabel = [string](Get-PandoraOnlineObjectProperty $labels 'agones.dev/fleet')
        $owners = @(Get-PandoraOnlineObjectProperty $metadata 'ownerReferences' @())
        $ownerNames = @($owners | ForEach-Object { [string](Get-PandoraOnlineObjectProperty $_ 'name') })
        $ownerUids = @($owners | ForEach-Object { [string](Get-PandoraOnlineObjectProperty $_ 'uid') })
        $relatedOwner = @($ownerNames | Where-Object { $gameServerNames.ContainsKey($_) }).Count -gt 0 -or
            @($ownerUids | Where-Object { $gameServerUids.ContainsKey($_) }).Count -gt 0
        $historicalOwner = @($ownerNames | Where-Object {
            $_ -cmatch '^(?:pandora-)?(?:battle|hub)(?:-|$)'
        }).Count -gt 0
        $nameClue = $name -cmatch '^(?:pandora-)?(?:battle|hub)(?:-|$)'
        $labelClue = $expectedFleets -ccontains $fleetLabel -or
            $fleetLabel -cmatch '^(?:pandora-)?(?:battle|hub)(?:-|$)'
        $podSpec = Get-PandoraOnlineObjectProperty $pod 'spec'
        $specClue = Test-PandoraDSTicketDSPodSpecClue $podSpec
        if (-not ($relatedOwner -or $historicalOwner -or $nameClue -or $labelClue -or $specClue)) { continue }
        $relatedDSPods.Add($pod)
        $containerNames = @(@(Get-PandoraOnlineObjectProperty $podSpec 'containers' @()) |
            ForEach-Object { [string](Get-PandoraOnlineObjectProperty $_ 'name') })
        $standardContainer = @($containerNames | Where-Object {
            $_ -cin @('pandora-battle-ds', 'pandora-hub-ds')
        }).Count -gt 0
        if ((-not $standardContainer -and $specClue) -or (-not $relatedOwner -and $specClue)) {
            $legacyObjects.Add($pod)
        }
    }

    return [pscustomobject]@{
        SignerPodObjects = @($relatedSignerPods)
        ReplicaSetObjects = @($relatedReplicaSets)
        GameServerSetObjects = @($relatedGameServerSets)
        DSPodObjects = @($relatedDSPods)
        LegacyControllerObjects = @($legacyObjects)
    }
}

# 普通 online 发布只允许沿用当前集群已经统一使用的 DSTicket revision；任何 revision 变化都属于
# 独立密钥轮换，必须走有全量 GameServer 门禁与 TTL 驻留的 dsticket_rotate.ps1。
# rows 格式：
#   DeploymentRows: <name> TAB <config-secret> TAB <signer-secret> TAB <login-jwks-or-empty>
#   FleetRows:      <name> TAB <env-revision> TAB <jwks-configmap>
# 两组都空表示首次 bootstrap；只存在一部分或任一引用漂移均 fail-closed。
function Assert-PandoraOrdinaryReleaseDSTicketRevision {
    param(
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$DeploymentRows,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$FleetRows,
        [Parameter(Mandatory = $true)][ValidateRange(1, 2147483647)][int]$RequestedRevision
    )
    $deploymentRows = @($DeploymentRows | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $fleetRows = @($FleetRows | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($deploymentRows.Count -eq 0 -and $fleetRows.Count -eq 0) { return }

    $expectedSigners = @('login', 'matchmaker', 'matchmaker-pve', 'hub-allocator')
    $expectedFleets = @('pandora-battle-stable', 'pandora-battle-canary', 'pandora-hub-stable', 'pandora-hub-canary')
    if ($deploymentRows.Count -ne $expectedSigners.Count -or $fleetRows.Count -ne $expectedFleets.Count) {
        throw "普通发布发现不完整的 DSTicket 运行态:signers=$($deploymentRows.Count)/4 fleets=$($fleetRows.Count)/4；拒绝在未知阶段继续。"
    }

    $observedRevisions = [System.Collections.Generic.List[int]]::new()
    $seenSigners = @{}
    foreach ($row in $deploymentRows) {
        $fields = @([regex]::Split($row, "`t", [System.Text.RegularExpressions.RegexOptions]::None))
        if ($fields.Count -ne 4) { throw "DSTicket live Deployment row 列数=$($fields.Count)，应为 4:$row" }
        $name, $configName, $secretName, $jwksName =
            [string]$fields[0], [string]$fields[1], [string]$fields[2], [string]$fields[3]
        if ($name -cnotin $expectedSigners -or $seenSigners.ContainsKey($name)) {
            throw "DSTicket live signer Deployment 非预期或重复:$name。"
        }
        $seenSigners[$name] = $true
        if ($configName -cne 'pandora-config') {
            throw "Deployment/$name 配置 Secret=$configName；普通发布只接受 fixed pandora-config，轮换 phase config 必须由专用流程收敛。"
        }
        if ($secretName -cnotmatch '^pandora-dsticket-signer-r([1-9][0-9]*)$') {
            throw "Deployment/$name signer Secret 非 revisioned:$secretName。"
        }
        $secretRevision = [int64]$Matches[1]
        if ($secretRevision -gt [int]::MaxValue) { throw "Deployment/$name signer revision 超范围:$secretRevision。" }
        $observedRevisions.Add([int]$secretRevision)
        if ($name -ceq 'login') {
            if ($jwksName -cnotmatch '^pandora-dsticket-jwks-r([1-9][0-9]*)$') {
                throw "Deployment/login JWKS ConfigMap 非 revisioned:$jwksName。"
            }
            if ([int64]$Matches[1] -ne $secretRevision) {
                throw "Deployment/login signer/JWKS revision 分裂:signer=r$secretRevision jwks=$jwksName。"
            }
        } elseif (-not [string]::IsNullOrWhiteSpace($jwksName)) {
            throw "Deployment/$name 不得挂 Login-only JWKS:$jwksName。"
        }
    }

    $seenFleets = @{}
    foreach ($row in $fleetRows) {
        $fields = @([regex]::Split($row, "`t", [System.Text.RegularExpressions.RegexOptions]::None))
        if ($fields.Count -ne 3) { throw "DSTicket live Fleet row 列数=$($fields.Count)，应为 3:$row" }
        $name, $envRevisionText, $jwksName = [string]$fields[0], [string]$fields[1], [string]$fields[2]
        if ($name -cnotin $expectedFleets -or $seenFleets.ContainsKey($name)) {
            throw "DSTicket live Fleet 非预期或重复:$name。"
        }
        $seenFleets[$name] = $true
        if ($envRevisionText -cnotmatch '^[1-9][0-9]*$' -or [int64]$envRevisionText -gt [int]::MaxValue) {
            throw "Fleet/$name PANDORA_DSTICKET_KEYSET_REVISION 非法:$envRevisionText。"
        }
        $envRevision = [int]$envRevisionText
        if ($jwksName -cnotmatch '^pandora-dsticket-jwks-r([1-9][0-9]*)$' -or [int64]$Matches[1] -ne $envRevision) {
            throw "Fleet/$name env/JWKS revision 分裂:env=r$envRevision jwks=$jwksName。"
        }
        $observedRevisions.Add($envRevision)
    }

    $distinct = @($observedRevisions | Sort-Object -Unique)
    if ($distinct.Count -ne 1) {
        throw "普通发布发现 DSTicket revision 混用:$($distinct -join ',')；须先由专用轮换流程收敛。"
    }
    if ($distinct[0] -ne $RequestedRevision) {
        throw "普通发布禁止把 DSTicket revision 从 r$($distinct[0]) 切到 r$RequestedRevision；请使用 tools/scripts/dsticket_rotate.ps1。"
    }
}

# 普通发布的完整 DSTicket 状态机入口。行契约先做最小引用闭包检查，
# 再由轮换工具共享的纯契约校验 marker 历史配对、fixed 子契约、终态
# signer/Fleet 与全部 live DS。两层都不访问集群，便于 mutant 测试。
function Assert-PandoraOrdinaryReleaseDSTicketState {
    param(
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$DeploymentRows,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][string[]]$FleetRows,
        [Parameter(Mandatory = $true)][ValidateRange(1, 2147483647)][int]$RequestedRevision,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$DeploymentObjects,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$FleetObjects,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$SignerPodObjects,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$ReplicaSetObjects,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$GameServerObjects,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$GameServerSetObjects,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$DSPodObjects,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$ActivationMarkers,
        [Parameter(Mandatory = $true)][AllowEmptyCollection()][object[]]$TerminalMarkers,
        [AllowNull()]$FixedConfigSecret,
        [AllowEmptyCollection()][object[]]$LegacyControllerObjects = @()
    )
    if (-not (Get-Command Assert-PandoraDSTicketOrdinaryState -ErrorAction SilentlyContinue)) {
        throw '未加载 dsticket_rotation_contract.ps1，无法安全判定普通发布状态。'
    }
    Assert-PandoraOrdinaryReleaseDSTicketRevision -DeploymentRows $DeploymentRows `
        -FleetRows $FleetRows -RequestedRevision $RequestedRevision
    return Assert-PandoraDSTicketOrdinaryState -RequestedRevision $RequestedRevision `
        -DeploymentObjects $DeploymentObjects -FleetObjects $FleetObjects `
        -SignerPodObjects $SignerPodObjects -ReplicaSetObjects $ReplicaSetObjects `
        -GameServerObjects $GameServerObjects -GameServerSetObjects $GameServerSetObjects `
        -DSPodObjects $DSPodObjects -ActivationMarkers $ActivationMarkers `
        -TerminalMarkers $TerminalMarkers -FixedConfigSecret $FixedConfigSecret `
        -LegacyControllerObjects $LegacyControllerObjects
}

function Set-PandoraFleetDSTicketKeysetRevision {
    param(
        [Parameter(Mandatory = $true)][string]$Manifest,
        [Parameter(Mandatory = $true)][ValidateRange(1, 2147483647)][int]$Revision
    )
    $envPattern = '(?ms)(- name:\s*PANDORA_DSTICKET_KEYSET_REVISION\r?\n\s*value:\s*")[0-9]+(")'
    if ([regex]::Matches($Manifest, $envPattern).Count -ne 1) {
        throw 'Fleet PANDORA_DSTICKET_KEYSET_REVISION 字段数量不是 1。'
    }
    $out = [regex]::Replace($Manifest, $envPattern, ('${1}' + $Revision + '${2}'), 1)
    $cmPattern = '(?m)^(\s*name:\s*)pandora-dsticket-jwks-r[0-9]+\s*$'
    if ([regex]::Matches($out, $cmPattern).Count -ne 1) {
        throw 'Fleet DSTicket ConfigMap 引用数量不是 1。'
    }
    return [regex]::Replace($out, $cmPattern, ('${1}pandora-dsticket-jwks-r' + $Revision), 1)
}

function Get-PandoraCanaryConfigContract {
    param(
        [Parameter(Mandatory = $true)][string]$BattleConfig,
        [Parameter(Mandatory = $true)][string]$HubConfig
    )
    $presence = @(
        [regex]::Matches($BattleConfig, '(?m)^\s{2}canary_percent:'),
        [regex]::Matches($BattleConfig, '(?m)^\s{2}canary_seed:'),
        [regex]::Matches($HubConfig, '(?m)^\s{2}canary_percent:'),
        [regex]::Matches($HubConfig, '(?m)^\s{2}canary_seed:')
    ) | ForEach-Object { $_.Count }
    if (($presence | Measure-Object -Sum).Sum -eq 0) {
        # 从旧单轨配置首次迁移：完全没有 Canary 字段等价于两轨权重 0。
        return [pscustomobject]@{ BattlePercent = 0; HubPercent = 0; Seed = '' }
    }
    if (@($presence | Where-Object { $_ -ne 1 }).Count -gt 0) {
        throw 'Battle/Hub Canary 字段只出现了一部分或重复，拒绝推测。'
    }
    function Read-ExactCanaryField([string]$Text, [string]$Name, [string]$Pattern, [string]$Service) {
        $matches = [regex]::Matches($Text, $Pattern)
        if ($matches.Count -ne 1) { throw "$Service $Name 字段数量=$($matches.Count)，应为 1。" }
        return $matches[0].Groups[1].Value
    }
    $battlePercent = Read-ExactCanaryField $BattleConfig 'canary_percent' '(?m)^\s{2}canary_percent:\s*([0-9]{1,3})\s*$' 'ds-allocator'
    $hubPercent = Read-ExactCanaryField $HubConfig 'canary_percent' '(?m)^\s{2}canary_percent:\s*([0-9]{1,3})\s*$' 'hub-allocator'
    $battleSeed = Read-ExactCanaryField $BattleConfig 'canary_seed' '(?m)^\s{2}canary_seed:\s*"([A-Za-z0-9._-]*)"\s*$' 'ds-allocator'
    $hubSeed = Read-ExactCanaryField $HubConfig 'canary_seed' '(?m)^\s{2}canary_seed:\s*"([A-Za-z0-9._-]*)"\s*$' 'hub-allocator'
    if ([int]$battlePercent -gt 100 -or [int]$hubPercent -gt 100) { throw 'canary_percent 超出 0..100。' }
    if ($battleSeed -cne $hubSeed) { throw 'Battle/Hub canary_seed 必须来自同一份发布参数。' }
    return [pscustomobject]@{
        BattlePercent = [int]$battlePercent
        HubPercent = [int]$hubPercent
        Seed = $battleSeed
    }
}

function New-PandoraWriterDigestPatch {
    param(
        [Parameter(Mandatory = $true)][string]$Service,
        [Parameter(Mandatory = $true)][string]$Digest
    )
    Assert-PandoraDigest -Digest $Digest -Where "writer $Service"
    return @"
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $Service
  namespace: pandora
spec:
  template:
    metadata:
      annotations:
        pandora.dev/image-digest: "$Digest"
"@
}

function Assert-PandoraFleetNoPlayerSigningMaterial {
    param([Parameter(Mandatory = $true)][string]$Manifest)
    # 先去掉纯注释，避免“私钥绝不进 Fleet”之类安全说明触发自身；实际 YAML token 仍全部扫描。
    $yaml = [regex]::Replace($Manifest, '(?m)^\s*#.*(?:\r?\n|$)', '')
    $forbidden = @(
        @{ Pattern = '(?i)(?:^|[\s{,])-?\s*name\s*:\s*PANDORA_(?:DS_TICKET_SECRET|JWT_SECRET|PLAYER_JWT_SECRET|DSTICKET_(?:SECRET|HMAC|PRIVATE(?:_KEY)?|SIGNING(?:_KEY)?))(?=[\s,}]|$)'; Reason = '玩家 JWT/DSTicket HMAC 或私钥 env' },
        @{ Pattern = '(?i)(?:^|[\s{,])secretName\s*:\s*pandora-dsticket(?:-signer-r[1-9][0-9]*)?(?=[\s,}]|$)'; Reason = 'DSTicket 签发 Secret 卷' },
        @{ Pattern = '(?i)(?:^|[\s{,])key\s*:\s*private\.pem(?=[\s,}]|$)'; Reason = 'private.pem Secret keyRef' },
        @{ Pattern = '(?i)/run/secrets/pandora-dsticket|/etc/pandora/dsticket/private'; Reason = 'DSTicket 私钥挂载路径' },
        @{ Pattern = '(?i)-----BEGIN (?:RSA )?PRIVATE KEY-----'; Reason = '内联私钥 PEM' },
        @{ Pattern = '(?i)["'']?kty["'']?\s*:\s*["'']?oct["'']?'; Reason = 'kty=oct 对称签名 key' }
    )
    foreach ($rule in $forbidden) {
        if ($yaml -match $rule.Pattern) {
            throw "Fleet 检测到 $($rule.Reason)；DS 只能持公开 RS256 JWKS，拒绝发布。"
        }
    }
}

function Set-PandoraFleetImagePin {
    param(
        [Parameter(Mandatory = $true)][string]$Manifest,
        [Parameter(Mandatory = $true)][string]$PinnedImage
    )
    $digest = Get-PandoraImageDigestFromReference $PinnedImage
    if ([string]::IsNullOrWhiteSpace($digest)) {
        throw "Fleet 镜像必须已固定为 repo@sha256:<digest>:$PinnedImage"
    }
    Assert-PandoraDigest -Digest $digest -Where 'Fleet image'
    Assert-PandoraFleetNoPlayerSigningMaterial -Manifest $Manifest

    $imagePattern = '(?m)^(?<indent>\s*)image:\s*\S+\s*$'
    $imageMatches = [regex]::Matches($Manifest, $imagePattern)
    if ($imageMatches.Count -ne 1) {
        throw "Fleet 主容器 image 字段数量=$($imageMatches.Count)，应为 1。"
    }
    $out = [regex]::Replace($Manifest, $imagePattern, '${indent}image: ' + $PinnedImage)

    # GameServer template metadata 与底层 Pod template metadata 都带 writer label；在各自完整
    # labels mapping 之后加同级 annotations。不能紧跟 writer 这一行插入：Hub 在它后面
    # 还有 region/capacity，那样会把后续 label 静默变成 annotation。
    $nl = if ($out.Contains("`r`n")) { "`r`n" } else { "`n" }
    $lines = [System.Collections.Generic.List[string]]::new([string[]]([regex]::Split($out, '\r?\n')))
    $writerIndexes = @(for ($i = 0; $i -lt $lines.Count; $i++) {
        if ($lines[$i] -cmatch '^([ ]*)pandora\.dev/ds-auth-writer-epoch:\s*"2"\s*$') { $i }
    })
    if ($writerIndexes.Count -ne 2) {
        throw "Fleet writer label 数量=$($writerIndexes.Count)，应为 GameServer/Pod 两处。"
    }
    $blocks = @()
    foreach ($writerIndex in $writerIndexes) {
        $writerIndent = [regex]::Match($lines[$writerIndex], '^([ ]*)').Groups[1].Value.Length
        if ($writerIndent -lt 2) { throw 'Fleet writer label 缩进异常。' }
        $labelIndent = $writerIndent - 2
        $labelsIndex = -1
        for ($j = $writerIndex - 1; $j -ge 0; $j--) {
            $candidateIndent = [regex]::Match($lines[$j], '^([ ]*)').Groups[1].Value.Length
            if ($candidateIndent -lt $labelIndent) { break }
            if ($candidateIndent -eq $labelIndent -and $lines[$j].Trim() -ceq 'labels:') { $labelsIndex = $j; break }
        }
        if ($labelsIndex -lt 0) { throw 'Fleet writer label 找不到所属 labels mapping。' }
        $end = $labelsIndex + 1
        while ($end -lt $lines.Count) {
            if ([string]::IsNullOrWhiteSpace($lines[$end])) { $end++; continue }
            $indent = [regex]::Match($lines[$end], '^([ ]*)').Groups[1].Value.Length
            if ($indent -le $labelIndent) { break }
            $end++
        }
        $blocks += [pscustomobject]@{ End = $end; LabelIndent = $labelIndent; EntryIndent = $writerIndent }
    }
    foreach ($block in @($blocks | Sort-Object End -Descending)) {
        $annotationHeader = [int]$block.End
        $headerIsAnnotations = $annotationHeader -lt $lines.Count -and
            [regex]::Match($lines[$annotationHeader], '^([ ]*)').Groups[1].Value.Length -eq [int]$block.LabelIndent -and
            $lines[$annotationHeader].Trim() -ceq 'annotations:'
        $digestLine = ((' ' * [int]$block.EntryIndent) + 'pandora.dev/image-digest: "' + $digest + '"')
        if (-not $headerIsAnnotations) {
            $lines.Insert($annotationHeader, ((' ' * [int]$block.LabelIndent) + 'annotations:'))
            $lines.Insert($annotationHeader + 1, $digestLine)
            continue
        }

        $annotationEnd = $annotationHeader + 1
        $digestIndexes = @()
        while ($annotationEnd -lt $lines.Count) {
            if ([string]::IsNullOrWhiteSpace($lines[$annotationEnd])) { $annotationEnd++; continue }
            $indent = [regex]::Match($lines[$annotationEnd], '^([ ]*)').Groups[1].Value.Length
            if ($indent -le [int]$block.LabelIndent) { break }
            if ($indent -eq [int]$block.EntryIndent -and
                $lines[$annotationEnd].Trim() -cmatch '^pandora\.dev/image-digest\s*:') {
                $digestIndexes += $annotationEnd
            }
            $annotationEnd++
        }
        if ($digestIndexes.Count -gt 1) { throw 'Fleet annotations 中 image-digest 重复。' }
        if ($digestIndexes.Count -eq 1) {
            $lines[[int]$digestIndexes[0]] = $digestLine
        } else {
            $lines.Insert($annotationHeader + 1, $digestLine)
        }
    }
    return ($lines -join $nl)
}

function Assert-PandoraRenderedOnlineContract {
    param(
        [Parameter(Mandatory = $true)][string[]]$ContractRows,
        [Parameter(Mandatory = $true)][hashtable]$Pins,
        [Parameter(Mandatory = $true)][hashtable]$Digests,
        [Parameter(Mandatory = $true)][string[]]$ServiceNames,
        [Parameter(Mandatory = $true)][string[]]$WriterServices = $script:PandoraWriterServices
    )
    $deployments = @{}
    foreach ($row in $ContractRows) {
        if ([string]::IsNullOrWhiteSpace($row)) { continue }
        $fields = @([regex]::Split($row, "`t"))
        if ($fields.Count -ne 5) { throw "kubectl workload contract 列数=$($fields.Count)，应为 5:$row" }
        if ($fields[0] -cne 'Deployment') { continue }
        $name = $fields[1]
        if ($deployments.ContainsKey($name)) { throw "online 渲染重复 Deployment/$name。" }
        $containerNames = @($fields[2].Split(' ', [System.StringSplitOptions]::RemoveEmptyEntries))
        $images = @($fields[3].Split(' ', [System.StringSplitOptions]::RemoveEmptyEntries))
        if ($containerNames.Count -ne $images.Count) {
            throw "Deployment/$name 容器名与镜像数量不一致:names=$($containerNames.Count) images=$($images.Count)。"
        }
        $targetIndexes = @(for ($i = 0; $i -lt $containerNames.Count; $i++) { if ($containerNames[$i] -ceq $name) { $i } })
        if ($targetIndexes.Count -ne 1) { throw "Deployment/$name 目标主容器数量=$($targetIndexes.Count)，应为 1。" }
        $deployments[$name] = [pscustomobject]@{
            Image = [string]$images[$targetIndexes[0]]
            Annotation = [string]$fields[4]
        }
    }
    if ($deployments.Count -ne $ServiceNames.Count) {
        throw "online 渲染 Deployment 数=$($deployments.Count)，应为 $($ServiceNames.Count)。"
    }
    foreach ($name in $ServiceNames) {
        if (-not $deployments.ContainsKey($name)) { throw "online 渲染缺 Deployment/$name。" }
        if (-not $Pins.ContainsKey($name)) { throw "缺 Deployment/$name 的期望 image pin。" }
        $digest = [string]$Digests[$name]
        Assert-PandoraDigest -Digest $digest -Where "Deployment/$name"
        if ([string]$deployments[$name].Image -cne [string]$Pins[$name]) {
            throw "Deployment/$name 主容器 image=$($deployments[$name].Image)，expected=$($Pins[$name])。"
        }
        if ($WriterServices -contains $name) {
            if ([string]$deployments[$name].Annotation -cne $digest) {
                throw "writer Deployment/$name Pod annotation=$($deployments[$name].Annotation)，expected=$digest。"
            }
        }
    }
}


function Assert-PandoraHubAllocatorSingleWriterContract {
    param([Parameter(Mandatory = $true)][string[]]$ContractRows)
    $targets = @()
    foreach ($row in $ContractRows) {
        if ([string]::IsNullOrWhiteSpace($row)) { continue }
        $fields = @([regex]::Split($row, "`t"))
        if ($fields.Count -lt 2 -or $fields[0] -cne 'Deployment' -or $fields[1] -cne 'hub-allocator') {
            continue
        }
        if ($fields.Count -ne 5) {
            throw "hub-allocator single-writer kubectl contract 列数=$($fields.Count)，应为 5:$row"
        }
        $targets += ,$fields
    }
    if ($targets.Count -ne 1) {
        throw "online 渲染 hub-allocator Deployment 数=$($targets.Count)，应为 1。"
    }
    $fields = $targets[0]
    # 2026-08-11 对齐当前权威设计:本断言原先要求 `Recreate + 无 rollingUpdate`,但
    # R9 P0-7 起 hub-allocator 的单写者约束**不再依赖部署策略**,改由运行时写者继任租约
    # 保证(pkg/dsauthfence/writerlease:etcd 选举 + 单调 fencing token + Redis 同事务
    # 存储级 fencing;未当选副本对写请求回可重试 UNAVAILABLE,失租旧 leader 永久不能补写)。
    # services.yaml 因此改成 `RollingUpdate{maxSurge:1,maxUnavailable:0}`(§9.16/§9.21
    # 不停服硬要求,Recreate 全量断流已被审计否决),并用 `pandora.dev/deploy-strategy`
    # 注解 + 进程启动 fail-closed 校验把"RollingUpdate × writer_lease_mode≠enforce"挡死。
    # 于是这条门禁若继续要求 Recreate,就变成一条与 manifest 永远对不上的死断言,
    # 硬阻断每一次 online 发布 —— 改为断言当前真正的不变量:
    #   ① replicas 恒为 1(多副本才是单写者风险的真实入口);
    #   ② 策略必须是 RollingUpdate 且 **maxUnavailable=0**(升级全程始终有副本在服务);
    #   ③ maxSurge 至多 1(重叠窗口内最多两个副本,与租约接任的时间预算匹配)。
    if ([string]$fields[2] -cne '1') {
        throw "Deployment/hub-allocator replicas=$($fields[2])，必须恒为 1(单写者;并发由写者租约而非副本数保证)。"
    }
    if ([string]$fields[3] -cne 'RollingUpdate') {
        throw "Deployment/hub-allocator strategy=$($fields[3])，必须为 RollingUpdate(§9.16/§9.21 不停服;单写者由 writerlease 保证)。"
    }
    $rollingUpdate = [string]$fields[4]
    if ($rollingUpdate -cnotmatch 'maxUnavailable:\s*0' -and $rollingUpdate -cnotmatch '"maxUnavailable"\s*:\s*0') {
        throw "Deployment/hub-allocator rollingUpdate 必须显式 maxUnavailable=0(升级全程始终有副本在服务)；actual=$rollingUpdate。"
    }
    if ($rollingUpdate -cnotmatch 'maxSurge:\s*1' -and $rollingUpdate -cnotmatch '"maxSurge"\s*:\s*1') {
        throw "Deployment/hub-allocator rollingUpdate 必须显式 maxSurge=1(重叠窗口最多两副本)；actual=$rollingUpdate。"
    }
}

function Assert-PandoraFleetManifestContract {
    param(
        [Parameter(Mandatory = $true)][string]$Manifest,
        [Parameter(Mandatory = $true)][string[]]$ContractRows,
        [Parameter(Mandatory = $true)][string]$PinnedImage,
        [Parameter(Mandatory = $true)][string]$ContainerName,
        [Parameter(Mandatory = $true)][ValidateSet('stable', 'canary')][string]$ExpectedTrack,
        [Parameter(Mandatory = $true)][string]$ExpectedFleetName
    )
    $digest = Get-PandoraImageDigestFromReference $PinnedImage
    if ([string]::IsNullOrWhiteSpace($digest)) { throw "Fleet 不是 digest pin:$PinnedImage" }
    Assert-PandoraFleetNoPlayerSigningMaterial -Manifest $Manifest
    # DSTicket v2(方案 B,已拍板):Fleet 必须携带公钥 JWKS 配置(env 对 + ConfigMap 卷)。
    # 缺任一项,DS 的 Agones Ready 门(FPandoraDSTicketReadyGatePolicy)会拒绝 Ready,发布必败 ——
    # 提前在第一次集群写之前阻断;env 里的 revision 必须与 ConfigMap 名后缀一致(防换钥时改了一处漏一处)。
    if ($Manifest -notmatch '(?m)^\s*-\s*name:\s*PANDORA_DSTICKET_JWKS_FILE\s*\r?\n\s*value:\s*"/etc/pandora/dsticket/jwks\.json"\s*$') {
        throw 'Fleet 缺 PANDORA_DSTICKET_JWKS_FILE="/etc/pandora/dsticket/jwks.json"(DSTicket v2 公钥验票必需)，拒绝发布。'
    }
    $dstRevMatch = [regex]::Match($Manifest, '(?m)^\s*-\s*name:\s*PANDORA_DSTICKET_KEYSET_REVISION\s*\r?\n\s*value:\s*"([0-9]+)"\s*$')
    if (-not $dstRevMatch.Success) {
        throw 'Fleet 缺 PANDORA_DSTICKET_KEYSET_REVISION(DSTicket v2 keyset 对账必需)，拒绝发布。'
    }
    $dstCmMatch = [regex]::Match($Manifest, '(?m)^\s*name:\s*pandora-dsticket-jwks-r([0-9]+)\s*$')
    if (-not $dstCmMatch.Success) {
        throw 'Fleet 缺 pandora-dsticket-jwks-r<revision> ConfigMap 卷(DSTicket v2 公钥来源)，拒绝发布。'
    }
    if ($dstCmMatch.Groups[1].Value -cne $dstRevMatch.Groups[1].Value) {
        throw "Fleet DSTicket keyset revision 不一致:env=$($dstRevMatch.Groups[1].Value) vs ConfigMap 卷=r$($dstCmMatch.Groups[1].Value)，拒绝发布。"
    }
    $rows = @($ContractRows | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    if ($rows.Count -ne 1) { throw "Fleet kubectl contract 行数=$($rows.Count)，应为 1。" }
    $fields = @([regex]::Split($rows[0], "`t"))
    if ($fields.Count -ne 12 -or $fields[0] -cne 'Fleet') { throw "Fleet kubectl contract 非法:$($rows[0])" }
    if ([string]$fields[1] -cne $ExpectedFleetName) { throw "Fleet 名=$($fields[1])，expected=$ExpectedFleetName。" }
    $names = @($fields[2].Split(' ', [System.StringSplitOptions]::RemoveEmptyEntries))
    $images = @($fields[3].Split(' ', [System.StringSplitOptions]::RemoveEmptyEntries))
    $policies = @($fields[4].Split(' ', [System.StringSplitOptions]::RemoveEmptyEntries))
    if ($names.Count -ne $images.Count -or $names.Count -ne $policies.Count) {
        throw 'Fleet 容器名/image/imagePullPolicy 数量不一致。'
    }
    $indexes = @(for ($i = 0; $i -lt $names.Count; $i++) { if ($names[$i] -ceq $ContainerName) { $i } })
    if ($indexes.Count -ne 1) { throw "Fleet 目标容器 $ContainerName 数量=$($indexes.Count)，应为 1。" }
    $idx = $indexes[0]
    if ([string]$images[$idx] -cne $PinnedImage) { throw "Fleet 主容器 image=$($images[$idx])，expected=$PinnedImage。" }
    if ([string]$policies[$idx] -cne 'IfNotPresent') { throw "Fleet 主容器 imagePullPolicy=$($policies[$idx])，expected=IfNotPresent。" }
    if ([string]$fields[5] -cne $digest -or [string]$fields[6] -cne $digest) {
        throw "Fleet 的 GameServer/Pod 两层 digest annotation 未闭合:gs=$($fields[5]) pod=$($fields[6])。"
    }
    if ([string]$fields[7] -cne $ExpectedTrack -or [string]$fields[8] -cne $ExpectedTrack -or
        [string]$fields[9] -cne $ExpectedTrack -or [string]$fields[10] -cne $ExpectedTrack -or
        [string]$fields[11] -cne $ExpectedTrack) {
        throw "Fleet release-track metadata 未闭合:fleet-label=$($fields[7]) gs-label=$($fields[8]) " +
              "gs-annotation=$($fields[9]) pod-label=$($fields[10]) pod-annotation=$($fields[11]) expected=$ExpectedTrack。"
    }
}
