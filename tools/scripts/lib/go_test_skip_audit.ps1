<#
.SYNOPSIS
  解析 `go test -json` 事件流,把「因未设置依赖 DSN 而跳过」的用例单独统计出来。

.DESCRIPTION
  存在的理由(2026-08-11):`go test` 对「全部用例都 Skip 的包」输出的是 `ok`,与真跑过
  完全无法区分。本仓库有 26 个文件、上百个用例由 PANDORA_TEST_* 环境变量门控;CI
  (ci_backend.ps1)从不设置这些变量,于是这些用例一次都没跑过,而流水线报告一路绿。

  真实代价已经发生:friend 域 `CreateRequest` 在真 MySQL 上是**确定性 1213 死锁**
  (3/3 复现),mission 域 `ApplyFactsTx` 同样,两条都被这层"SKIP 显示成 ok"盖了很久;
  它们在 TiDB 上不复现,而恰恰只有 TiDB 侧偶尔被人手工跑过。

  本文件只提供**纯函数**(输入 JSON 行、输出统计),不执行 go test,因此可以用合成输入
  做契约测试(tools/scripts/tests/go_test_skip_audit_contract_test.ps1),不需要数据库。
#>

# PandoraDbGatedEnvNames 是「跳过原因里出现即判定为依赖门控」的环境变量名。
# 新增依赖门控变量时必须同步补进来,否则新增的门控用例又会隐身。
$script:PandoraDbGatedEnvNames = @(
    'PANDORA_TEST_MYSQL_DSN'
    'PANDORA_TEST_TIDB_DSN'
    'PANDORA_TEST_REDIS_ADDR'
    'PANDORA_TEST_REDIS'
    'PANDORA_TEST_KAFKA_BROKERS'
    'PANDORA_TEST_ETCD_ENDPOINTS'
)

function Get-PandoraDbGatedEnvNames {
    return , $script:PandoraDbGatedEnvNames
}

<#
.SYNOPSIS
  把 `go test -json` 的行数组归并成统计结果。

.OUTPUTS
  [pscustomobject] @{
    Console      = [string[]] 逐字还原的人类可读输出(与不加 -json 时一致)
    Passed       = [int] 通过的用例数(顶层 + 子测试)
    Failed       = [int] 失败的用例数
    Skipped      = [int] 跳过的用例数
    GatedSkips   = [pscustomobject[]] @{ Package; Test; Env }  依赖门控导致的跳过
    ParseErrors  = [string[]] 无法解析为 JSON 的行(构建错误等,原样保留)
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
    # 每个 (包, 用例) 的累计输出:Skip 事件本身不带原因,原因在它之前的 output 事件里。
    $outputByTest = @{}
    $passed = 0; $failed = 0; $skipped = 0

    foreach ($line in ($JsonLines | Where-Object { $null -ne $_ })) {
        $trimmed = $line.Trim()
        if (-not $trimmed) { continue }
        if (-not $trimmed.StartsWith('{')) {
            # 构建失败等非 JSON 行必须原样透出,否则 CI 日志里看不到编译错误。
            $parseErrors.Add($line)
            $console.Add($line)
            continue
        }
        try { $ev = $trimmed | ConvertFrom-Json -ErrorAction Stop }
        catch { $parseErrors.Add($line); $console.Add($line); continue }

        $key = "$($ev.Package)::$($ev.Test)"
        switch ($ev.Action) {
            'output' {
                if ($ev.Output) { $console.Add($ev.Output.TrimEnd("`r", "`n")) }
                if ($ev.Test) {
                    if (-not $outputByTest.ContainsKey($key)) { $outputByTest[$key] = '' }
                    $outputByTest[$key] += $ev.Output
                }
            }
            'pass' { if ($ev.Test) { $passed++ } }
            'fail' { if ($ev.Test) { $failed++ } }
            'skip' {
                if (-not $ev.Test) { break }   # 包级 skip(无测试文件)不计
                $skipped++
                $reason = if ($outputByTest.ContainsKey($key)) { $outputByTest[$key] } else { '' }
                foreach ($envName in $script:PandoraDbGatedEnvNames) {
                    if ($reason -like "*$envName*") {
                        $gated.Add([pscustomobject]@{
                                Package = $ev.Package
                                Test    = $ev.Test
                                Env     = $envName
                            })
                        break
                    }
                }
            }
        }
    }

    return [pscustomobject]@{
        Console     = $console.ToArray()
        Passed      = $passed
        Failed      = $failed
        Skipped     = $skipped
        GatedSkips  = $gated.ToArray()
        ParseErrors = $parseErrors.ToArray()
    }
}

<#
.SYNOPSIS
  按「哪些 DSN 已设置」判定本次跳过是否构成门禁失败。

.DESCRIPTION
  两条判据,性质不同,不能合并:
    ① **配置失效**(硬失败):某个 DSN 明明设置了,却仍有用例因它而跳过 —— 说明变量没传到
       go test 进程(名字拼错 / Jenkins 没透传 / 在子 shell 里丢了)。这种情况下报告显示的
       "全绿"是假的,必须让流水线红。
    ② **覆盖缺口**(默认告警,-Require 时硬失败):DSN 根本没设置。这是环境能力问题而不是
       配置错误,默认只响亮告警并打印缺口清单;等流水线真正挂上测试库后用 -Require 转成门禁。

.OUTPUTS
  [pscustomobject] @{ Violations = [string[]]; Warnings = [string[]] }
#>
function Test-PandoraGatedSkipPolicy {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][AllowEmptyCollection()][AllowNull()]
        [pscustomobject[]]$GatedSkips,
        [switch]$Require
    )

    $violations = [System.Collections.Generic.List[string]]::new()
    $warnings = [System.Collections.Generic.List[string]]::new()

    $byEnv = @{}
    foreach ($s in ($GatedSkips | Where-Object { $null -ne $_ })) {
        if (-not $byEnv.ContainsKey($s.Env)) { $byEnv[$s.Env] = [System.Collections.Generic.List[string]]::new() }
        $byEnv[$s.Env].Add("$($s.Package) $($s.Test)")
    }

    foreach ($envName in ($byEnv.Keys | Sort-Object)) {
        $count = $byEnv[$envName].Count
        $isSet = -not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($envName))
        $sample = ($byEnv[$envName] | Select-Object -First 3) -join '; '
        if ($isSet) {
            $violations.Add("$envName 已设置,却仍有 $count 个用例因它跳过(变量没传到 go test 进程?)。例:$sample")
        }
        elseif ($Require) {
            $violations.Add("$envName 未设置,$count 个依赖门控用例未执行(-Require 下视为门禁失败)。例:$sample")
        }
        else {
            $warnings.Add("$envName 未设置 → $count 个用例被跳过,**本轮未验证**。例:$sample")
        }
    }

    return [pscustomobject]@{
        Violations = $violations.ToArray()
        Warnings   = $warnings.ToArray()
    }
}
