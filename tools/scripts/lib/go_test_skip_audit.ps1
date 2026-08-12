<#
.SYNOPSIS
  解析 `go test -json`，审计因外部依赖未配置而跳过的用例。

.DESCRIPTION
  Go 1.26 会在 TestEvent 之外输出 build-output/build-fail BuildEvent。本文件同时还原
  两类事件，避免编译错误在 JSON 模式下从 Jenkins 控制台消失。

  依赖分两组：
    - database：MySQL/TiDB，Jenkins 使用 -RequireDbTests 强制执行；
    - optional：Redis/Kafka/etcd，未提供环境时明确告警，但不阻断数据库门禁。

  本文件只提供纯函数，不启动测试或外部服务。
#>

$script:PandoraGatedEnvRegistry = @(
    [pscustomobject]@{ Name = 'PANDORA_TEST_MYSQL_DSN'; Group = 'database' }
    [pscustomobject]@{ Name = 'PANDORA_TEST_TIDB_DSN'; Group = 'database' }
    # pkg/mysqlx 的真实 TiDB 行为探针沿用这一个历史名称，不能漏掉。
    [pscustomobject]@{ Name = 'PANDORA_TIDB_TEST_DSN'; Group = 'database' }

    [pscustomobject]@{ Name = 'PANDORA_TEST_REDIS_ADDR'; Group = 'optional' }
    [pscustomobject]@{ Name = 'PANDORA_TEST_REDIS'; Group = 'optional' }
    [pscustomobject]@{ Name = 'PANDORA_TEST_REDIS8_ADDR'; Group = 'optional' }
    [pscustomobject]@{ Name = 'PANDORA_TEST_REDIS8_PASSWORD'; Group = 'optional' }
    [pscustomobject]@{ Name = 'PANDORA_TEST_REDIS8_CLUSTER_ADDRS'; Group = 'optional' }
    [pscustomobject]@{ Name = 'PANDORA_TEST_REDIS8_CLUSTER_PASSWORD'; Group = 'optional' }
    [pscustomobject]@{ Name = 'PANDORA_TEST_REDIS8_SENTINEL_ADDRS'; Group = 'optional' }
    [pscustomobject]@{ Name = 'PANDORA_TEST_REDIS8_SENTINEL_MASTER_NAME'; Group = 'optional' }
    [pscustomobject]@{ Name = 'PANDORA_TEST_REDIS8_SENTINEL_PASSWORD'; Group = 'optional' }
    [pscustomobject]@{ Name = 'PANDORA_TEST_KAFKA_BROKERS'; Group = 'optional' }
    [pscustomobject]@{ Name = 'PANDORA_TEST_ETCD_ENDPOINTS'; Group = 'optional' }
)

function Get-PandoraGatedEnvRegistry {
    return $script:PandoraGatedEnvRegistry
}

function Get-PandoraGatedEnvNames {
    return @($script:PandoraGatedEnvRegistry | ForEach-Object { $_.Name })
}

# 兼容已有调用方；名字保留，但返回的是全部受审计依赖，不再暗示全部都是 DB 必选。
function Get-PandoraDbGatedEnvNames {
    return (Get-PandoraGatedEnvNames)
}

function Get-PandoraEnvGroup([string]$Name) {
    $entry = $script:PandoraGatedEnvRegistry | Where-Object { $_.Name -eq $Name } | Select-Object -First 1
    if ($null -eq $entry) { return 'optional' }
    return $entry.Group
}

function Add-PandoraUniqueName(
    [System.Collections.Generic.List[string]]$Names,
    [string]$Name
) {
    if (-not $Names.Contains($Name)) { $Names.Add($Name) }
}

# 环境变量名按 ASCII token 边界匹配，不能让 PANDORA_TEST_REDIS 吞掉 REDIS8_ADDR。
# 三条 Redis8 用例的既有 skip 文案使用 ADDR/PASSWORD 简写或自然语言；这里仅为这些
# 已知、完整短语补回同一个 gate 的全部前置变量，不做模糊子串猜测。
function Get-PandoraGateEnvNamesFromReason([string]$Reason) {
    $names = [System.Collections.Generic.List[string]]::new()
    foreach ($entry in $script:PandoraGatedEnvRegistry) {
        $escaped = [regex]::Escape($entry.Name)
        if ($Reason -match "(?<![A-Z0-9_])$escaped(?![A-Z0-9_])") {
            Add-PandoraUniqueName $names $entry.Name
        }
    }

    if ($Reason -match '(?<![A-Z0-9_])PANDORA_TEST_REDIS8_ADDR/PASSWORD(?![A-Z0-9_])') {
        Add-PandoraUniqueName $names 'PANDORA_TEST_REDIS8_ADDR'
        Add-PandoraUniqueName $names 'PANDORA_TEST_REDIS8_PASSWORD'
    }
    if ($Reason -match '(?<![A-Z0-9_])PANDORA_TEST_REDIS8_CLUSTER_ADDRS/PASSWORD(?![A-Z0-9_])') {
        Add-PandoraUniqueName $names 'PANDORA_TEST_REDIS8_CLUSTER_ADDRS'
        Add-PandoraUniqueName $names 'PANDORA_TEST_REDIS8_CLUSTER_PASSWORD'
    }
    if ($Reason -match '(?i)(?<![A-Z0-9_])Redis 8 sentinel integration addresses/master/password(?![A-Z0-9_])') {
        Add-PandoraUniqueName $names 'PANDORA_TEST_REDIS8_SENTINEL_ADDRS'
        Add-PandoraUniqueName $names 'PANDORA_TEST_REDIS8_SENTINEL_MASTER_NAME'
        Add-PandoraUniqueName $names 'PANDORA_TEST_REDIS8_SENTINEL_PASSWORD'
    }
    return $names.ToArray()
}

<#
.OUTPUTS
  [pscustomobject] @{
    Console       = [string[]]  TestEvent.Output + Go 1.26 BuildEvent.Output
    Passed        = [int]
    Failed        = [int]
    Skipped       = [int]
    GatedSkips    = [pscustomobject[]] @{ Package; Test; Envs; Env }
    ParseErrors   = [string[]]
    BuildFailures = [string[]]  build-fail ImportPath / TestEvent.FailedBuild 去重集合
  }
#>
function Get-GoTestSkipAudit {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][AllowEmptyCollection()][AllowNull()]
        [string[]]$JsonLines
    )

    $console = [System.Collections.Generic.List[string]]::new()
    $parseErrors = [System.Collections.Generic.List[string]]::new()
    $gated = [System.Collections.Generic.List[pscustomobject]]::new()
    $buildFailures = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::Ordinal)
    $outputByTest = @{}
    $passed = 0
    $failed = 0
    $skipped = 0

    foreach ($line in ($JsonLines | Where-Object { $null -ne $_ })) {
        $trimmed = $line.Trim()
        if (-not $trimmed) { continue }
        if (-not $trimmed.StartsWith('{')) {
            $parseErrors.Add($line)
            $console.Add($line)
            continue
        }
        try { $ev = $trimmed | ConvertFrom-Json -ErrorAction Stop }
        catch {
            $parseErrors.Add($line)
            $console.Add($line)
            continue
        }

        $key = "$($ev.Package)::$($ev.Test)"
        switch ($ev.Action) {
            'build-output' {
                if ($ev.Output) { $console.Add($ev.Output.TrimEnd("`r", "`n")) }
            }
            'build-fail' {
                if (-not [string]::IsNullOrWhiteSpace([string]$ev.ImportPath)) {
                    [void]$buildFailures.Add([string]$ev.ImportPath)
                }
            }
            'output' {
                if ($ev.Output) { $console.Add($ev.Output.TrimEnd("`r", "`n")) }
                if ($ev.Test) {
                    if (-not $outputByTest.ContainsKey($key)) { $outputByTest[$key] = '' }
                    $outputByTest[$key] += $ev.Output
                }
            }
            'pass' {
                if ($ev.Test) { $passed++ }
            }
            'fail' {
                if ($ev.Test) { $failed++ }
                if (-not [string]::IsNullOrWhiteSpace([string]$ev.FailedBuild)) {
                    [void]$buildFailures.Add([string]$ev.FailedBuild)
                }
            }
            'skip' {
                if (-not $ev.Test) { break }
                $skipped++
                $reason = if ($outputByTest.ContainsKey($key)) { $outputByTest[$key] } else { '' }
                $envs = @(Get-PandoraGateEnvNamesFromReason $reason)
                if ($envs.Count -gt 0) {
                    $gated.Add([pscustomobject]@{
                            Package = $ev.Package
                            Test    = $ev.Test
                            Envs    = $envs
                            # 兼容旧的单变量消费方；多前置 gate 必须读取 Envs。
                            Env     = if ($envs.Count -eq 1) { $envs[0] } else { $null }
                        })
                }
            }
        }
    }

    return [pscustomobject]@{
        Console       = $console.ToArray()
        Passed        = $passed
        Failed        = $failed
        Skipped       = $skipped
        GatedSkips    = $gated.ToArray()
        ParseErrors   = $parseErrors.ToArray()
        BuildFailures = @($buildFailures | Sort-Object)
    }
}

function Test-PandoraGatedSkipPolicy {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][AllowEmptyCollection()][AllowNull()]
        [pscustomobject[]]$GatedSkips,
        [switch]$RequireDbTests,
        # 兼容旧调用；新代码应使用语义准确的 -RequireDbTests。
        [Alias('Require')][switch]$LegacyRequireDbTests
    )

    if ($LegacyRequireDbTests) { $RequireDbTests = $true }
    $violations = [System.Collections.Generic.List[string]]::new()
    $warnings = [System.Collections.Generic.List[string]]::new()
    $skips = @($GatedSkips | Where-Object { $null -ne $_ })

    $samplesByEnv = @{}
    foreach ($skip in $skips) {
        $envs = if ($skip.PSObject.Properties.Name -contains 'Envs') { @($skip.Envs) } else { @($skip.Env) }
        foreach ($name in ($envs | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })) {
            if (-not $samplesByEnv.ContainsKey($name)) {
                $samplesByEnv[$name] = [System.Collections.Generic.List[string]]::new()
            }
            $samplesByEnv[$name].Add("$($skip.Package) $($skip.Test)")
        }
    }

    # 数据库门禁先检查环境本身，避免“解析器漏掉某条 Skip”反而让缺失 DSN 假绿。
    if ($RequireDbTests) {
        foreach ($entry in ($script:PandoraGatedEnvRegistry | Where-Object { $_.Group -eq 'database' })) {
            $name = $entry.Name
            $isSet = -not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($name))
            if (-not $isSet) {
                $sample = if ($samplesByEnv.ContainsKey($name)) {
                    ($samplesByEnv[$name] | Select-Object -First 3) -join '; '
                } else { '未观察到对应 Skip 事件（仍须 fail-closed 校验 CI 环境）' }
                $violations.Add("$name 未设置，数据库必选门禁未满足。例:$sample")
            }
        }
    }

    $missingOptional = @{}
    foreach ($skip in $skips) {
        $envs = if ($skip.PSObject.Properties.Name -contains 'Envs') { @($skip.Envs) } else { @($skip.Env) }
        $envs = @($envs | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Select-Object -Unique)
        if ($envs.Count -eq 0) { continue }
        $missing = @($envs | Where-Object {
                [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($_))
            })

        if ($missing.Count -eq 0) {
            $violations.Add(("依赖变量均已设置，却仍跳过 {0} {1}（变量未透传或用例门控失效？）：{2}" -f `
                        $skip.Package, $skip.Test, ($envs -join ', ')))
            continue
        }

        foreach ($name in $missing) {
            if ($RequireDbTests -and (Get-PandoraEnvGroup $name) -eq 'database') {
                # 已由上面的“必选环境”检查统一报告，避免同一缺口重复两条 violation。
                continue
            }
            if (-not $missingOptional.ContainsKey($name)) {
                $missingOptional[$name] = [System.Collections.Generic.List[string]]::new()
            }
            $missingOptional[$name].Add("$($skip.Package) $($skip.Test)")
        }
    }

    foreach ($name in ($missingOptional.Keys | Sort-Object)) {
        $items = $missingOptional[$name]
        $sample = ($items | Select-Object -First 3) -join '; '
        $warnings.Add("$name 未设置 → $($items.Count) 个依赖门控用例被跳过，**本轮未验证**。例:$sample")
    }

    return [pscustomobject]@{
        Violations = $violations.ToArray()
        Warnings   = $warnings.ToArray()
    }
}
