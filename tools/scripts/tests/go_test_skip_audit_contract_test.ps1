<#
.SYNOPSIS
  lib/go_test_skip_audit.ps1 的契约测试:证明「跳过被当成通过」这条洞真的被堵上了。

.DESCRIPTION
  被测性质本身就是"门禁会不会失灵",所以断言必须双向:
    - 门控用例被跳过 → 必须被识别、计数、并按 DSN 是否设置分流成 告警 / 门禁失败;
    - 普通 Skip(t.Skip("windows 不支持"))**不得**被误判成门控跳过,否则门禁天天误报,
      很快就会被人加白名单绕过 —— 那等于回到原点。

  用合成的 `go test -json` 行做输入,不需要数据库、不需要跑 go test。

.EXAMPLE
  pwsh tools/scripts/tests/go_test_skip_audit_contract_test.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot '../lib/go_test_skip_audit.ps1')

$script:Failures = @()
function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { $script:Failures += $Message; Write-Host "  [FAIL] $Message" -ForegroundColor Red }
    else { Write-Host "  [ ok ] $Message" -ForegroundColor DarkGray }
}

function New-Event([string]$Action, [string]$Package, [string]$Test, [string]$Output) {
    $o = [ordered]@{ Action = $Action; Package = $Package }
    if ($Test) { $o.Test = $Test }
    if ($null -ne $Output) { $o.Output = $Output }
    return ([pscustomobject]$o | ConvertTo-Json -Compress)
}

# ── 合成一段典型的 go test -json 流 ────────────────────────────────────────────
$pkgFriend = 'github.com/luyuancpp/pandora/services/social/friend/internal/data'
$pkgUtil = 'github.com/luyuancpp/pandora/pkg/util'

$lines = @(
    New-Event 'run' $pkgFriend 'TestFriendCreateRequestGuardBeforeGapLocks' $null
    New-Event 'output' $pkgFriend 'TestFriendCreateRequestGuardBeforeGapLocks' "    跳过 mysql 好友申请容量集成测试:未设置 PANDORA_TEST_MYSQL_DSN`n"
    New-Event 'skip' $pkgFriend 'TestFriendCreateRequestGuardBeforeGapLocks' $null

    New-Event 'run' $pkgFriend 'TestFriendRepoIncomingLimitConcurrencyMySQLAndTiDB' $null
    New-Event 'output' $pkgFriend 'TestFriendRepoIncomingLimitConcurrencyMySQLAndTiDB' "    跳过 tidb:未设置 PANDORA_TEST_TIDB_DSN`n"
    New-Event 'skip' $pkgFriend 'TestFriendRepoIncomingLimitConcurrencyMySQLAndTiDB' $null

    # 普通跳过:与任何 DSN 无关,绝不能被算成门控跳过
    New-Event 'run' $pkgUtil 'TestOnlyOnLinux' $null
    New-Event 'output' $pkgUtil 'TestOnlyOnLinux' "    windows 下不适用,跳过`n"
    New-Event 'skip' $pkgUtil 'TestOnlyOnLinux' $null

    New-Event 'run' $pkgUtil 'TestNormalize' $null
    New-Event 'pass' $pkgUtil 'TestNormalize' $null

    New-Event 'output' $pkgUtil $null "ok  	$pkgUtil	0.012s`n"
)

Write-Host '[1] 解析与分类' -ForegroundColor Cyan
$audit = Get-GoTestSkipAudit -JsonLines $lines
Assert-True ($audit.Passed -eq 1) "通过计数 = 1(实为 $($audit.Passed))"
Assert-True ($audit.Skipped -eq 3) "跳过计数 = 3(实为 $($audit.Skipped))"
Assert-True ($audit.GatedSkips.Count -eq 2) "依赖门控跳过 = 2(实为 $($audit.GatedSkips.Count))"
Assert-True (($audit.GatedSkips | Where-Object { $_.Env -eq 'PANDORA_TEST_MYSQL_DSN' }).Count -eq 1) 'MySQL 门控跳过被识别'
Assert-True (($audit.GatedSkips | Where-Object { $_.Env -eq 'PANDORA_TEST_TIDB_DSN' }).Count -eq 1) 'TiDB 门控跳过被识别'
Assert-True (($audit.GatedSkips | Where-Object { $_.Test -eq 'TestOnlyOnLinux' }).Count -eq 0) '普通 Skip 未被误判成门控跳过'
Assert-True (($audit.Console -join "`n") -match 'ok\s') '人类可读输出被逐字还原(日志观感不变)'

Write-Host '[2] 未设置 DSN:默认告警,不阻断' -ForegroundColor Cyan
$saveMy = $env:PANDORA_TEST_MYSQL_DSN
$saveTi = $env:PANDORA_TEST_TIDB_DSN
try {
    $env:PANDORA_TEST_MYSQL_DSN = ''
    $env:PANDORA_TEST_TIDB_DSN = ''
    $policy = Test-PandoraGatedSkipPolicy -GatedSkips $audit.GatedSkips
    Assert-True ($policy.Violations.Count -eq 0) '未设置 DSN 时不阻断流水线'
    Assert-True ($policy.Warnings.Count -eq 2) "两个变量各出一条告警(实为 $($policy.Warnings.Count))"

    Write-Host '[3] -Require:同样的输入必须转成门禁失败' -ForegroundColor Cyan
    $policyReq = Test-PandoraGatedSkipPolicy -GatedSkips $audit.GatedSkips -Require
    Assert-True ($policyReq.Violations.Count -eq 2) "-Require 下 2 条门禁失败(实为 $($policyReq.Violations.Count))"

    Write-Host '[4] DSN 已设置却仍跳过 = 配置失效,必须硬失败' -ForegroundColor Cyan
    # 这是最要命的一种:Jenkins 里写了变量但没透传到 go test 进程,报告会显示"全绿"。
    $env:PANDORA_TEST_MYSQL_DSN = 'root:x@tcp(127.0.0.1:3307)/'
    $policySet = Test-PandoraGatedSkipPolicy -GatedSkips $audit.GatedSkips
    $mysqlViolation = $policySet.Violations | Where-Object { $_ -like '*PANDORA_TEST_MYSQL_DSN*' }
    Assert-True ($null -ne $mysqlViolation) 'DSN 已设置却仍跳过 → 门禁失败'
    Assert-True (($policySet.Violations | Where-Object { $_ -like '*TIDB*' }).Count -eq 0) '未设置的 TiDB 仍只是告警(两条判据不混淆)'

    Write-Host '[5] 全部执行(零门控跳过)时既不告警也不失败' -ForegroundColor Cyan
    $policyClean = Test-PandoraGatedSkipPolicy -GatedSkips @() -Require
    Assert-True ($policyClean.Violations.Count -eq 0 -and $policyClean.Warnings.Count -eq 0) '零门控跳过 → 干净通过'
}
finally {
    $env:PANDORA_TEST_MYSQL_DSN = $saveMy
    $env:PANDORA_TEST_TIDB_DSN = $saveTi
}

Write-Host '[6] 非 JSON 行(构建错误)必须原样透出,不得被吞' -ForegroundColor Cyan
$buildFail = @(
    './internal/data/foo.go:12:2: undefined: Bar'
    (New-Event 'fail' $pkgFriend $null $null)
)
$auditBuild = Get-GoTestSkipAudit -JsonLines $buildFail
Assert-True ($auditBuild.ParseErrors.Count -eq 1) '非 JSON 行被记入 ParseErrors'
Assert-True (($auditBuild.Console -join "`n") -match 'undefined: Bar') '编译错误仍出现在控制台输出里'

if ($script:Failures.Count -gt 0) {
    Write-Host "`n[FAIL] $($script:Failures.Count) 条断言未通过。" -ForegroundColor Red
    exit 1
}
Write-Host "`n[PASS] go_test_skip_audit 契约测试全部通过。" -ForegroundColor Green
