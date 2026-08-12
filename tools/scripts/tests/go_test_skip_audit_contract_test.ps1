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

function New-BuildEvent([string]$Action, [string]$ImportPath, [string]$Output) {
    $o = [ordered]@{ ImportPath = $ImportPath; Action = $Action }
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
# Test-PandoraGatedSkipPolicy 会**读进程环境变量**,所以本段必须先把受审计的门控变量
# 全部清空,再由 [4]/[5] 按需逐个设回去 —— 否则断言结果取决于"谁在跑这个脚本"。
# 这不是假设性风险:ci_backend.ps1(2026-08-12 起)会先把 CI DB 的三个 DSN 导入本进程
# 环境,再在同一进程里跑本契约测试;漏清任何一个,本地全绿而 Jenkins 必红。
# 用注册表遍历而不是逐个写死:将来再加一个数据库门控变量时不会重蹈覆辙。
$savedBlockEnv = @{}
foreach ($name in (Get-PandoraGatedEnvNames)) {
    $savedBlockEnv[$name] = [Environment]::GetEnvironmentVariable($name)
    [Environment]::SetEnvironmentVariable($name, '')
}
try {
    $policy = Test-PandoraGatedSkipPolicy -GatedSkips $audit.GatedSkips
    Assert-True ($policy.Violations.Count -eq 0) '未设置 DSN 时不阻断流水线'
    Assert-True ($policy.Warnings.Count -eq 2) "两个变量各出一条告警(实为 $($policy.Warnings.Count))"

    Write-Host '[3] -Require:同样的输入必须转成门禁失败' -ForegroundColor Cyan
    $policyReq = Test-PandoraGatedSkipPolicy -GatedSkips $audit.GatedSkips -RequireDbTests
    Assert-True ($policyReq.Violations.Count -eq 3) "-RequireDbTests 下三个数据库变量均为门禁(实为 $($policyReq.Violations.Count))"

    Write-Host '[4] DSN 已设置却仍跳过 = 配置失效,必须硬失败' -ForegroundColor Cyan
    # 这是最要命的一种:Jenkins 里写了变量但没透传到 go test 进程,报告会显示"全绿"。
    $env:PANDORA_TEST_MYSQL_DSN = 'root:x@tcp(127.0.0.1:3307)/'
    $policySet = Test-PandoraGatedSkipPolicy -GatedSkips $audit.GatedSkips
    $mysqlViolation = $policySet.Violations | Where-Object { $_ -like '*PANDORA_TEST_MYSQL_DSN*' }
    Assert-True ($null -ne $mysqlViolation) 'DSN 已设置却仍跳过 → 门禁失败'
    Assert-True (($policySet.Violations | Where-Object { $_ -like '*TIDB*' }).Count -eq 0) '未设置的 TiDB 仍只是告警(两条判据不混淆)'

    Write-Host '[5] 全部执行(零门控跳过)时既不告警也不失败' -ForegroundColor Cyan
    $env:PANDORA_TEST_TIDB_DSN = 'root@tcp(127.0.0.1:4000)/'
    $env:PANDORA_TIDB_TEST_DSN = 'root@tcp(127.0.0.1:4000)/pandora_account'
    $policyClean = Test-PandoraGatedSkipPolicy -GatedSkips @() -RequireDbTests
    Assert-True ($policyClean.Violations.Count -eq 0 -and $policyClean.Warnings.Count -eq 0) '数据库变量齐全且零门控跳过 → 干净通过'
}
finally {
    foreach ($name in $savedBlockEnv.Keys) {
        [Environment]::SetEnvironmentVariable($name, $savedBlockEnv[$name])
    }
}

Write-Host '[6] 非 JSON 行(构建错误)必须原样透出,不得被吞' -ForegroundColor Cyan
$buildFail = @(
    './internal/data/foo.go:12:2: undefined: Bar'
    (New-Event 'fail' $pkgFriend $null $null)
)
$auditBuild = Get-GoTestSkipAudit -JsonLines $buildFail
Assert-True ($auditBuild.ParseErrors.Count -eq 1) '非 JSON 行被记入 ParseErrors'
Assert-True (($auditBuild.Console -join "`n") -match 'undefined: Bar') '编译错误仍出现在控制台输出里'

Write-Host '[7] 数据库必选与 Redis/Kafka/etcd 可选必须分组' -ForegroundColor Cyan
$groupLines = @(
    New-Event 'output' $pkgFriend 'TestMySQL' "skip: PANDORA_TEST_MYSQL_DSN`n"
    New-Event 'skip' $pkgFriend 'TestMySQL' $null
    New-Event 'output' $pkgFriend 'TestTiDB' "skip: PANDORA_TEST_TIDB_DSN`n"
    New-Event 'skip' $pkgFriend 'TestTiDB' $null
    New-Event 'output' $pkgFriend 'TestTiDBBackend' "skip: PANDORA_TIDB_TEST_DSN`n"
    New-Event 'skip' $pkgFriend 'TestTiDBBackend' $null
    New-Event 'output' $pkgFriend 'TestRedis' "skip: PANDORA_TEST_REDIS_ADDR`n"
    New-Event 'skip' $pkgFriend 'TestRedis' $null
    New-Event 'output' $pkgFriend 'TestKafka' "skip: PANDORA_TEST_KAFKA_BROKERS`n"
    New-Event 'skip' $pkgFriend 'TestKafka' $null
    New-Event 'output' $pkgFriend 'TestEtcd' "skip: PANDORA_TEST_ETCD_ENDPOINTS`n"
    New-Event 'skip' $pkgFriend 'TestEtcd' $null
)
$groupAudit = Get-GoTestSkipAudit -JsonLines $groupLines
$savedGateEnv = @{}
foreach ($name in (Get-PandoraGatedEnvNames)) {
    $savedGateEnv[$name] = [Environment]::GetEnvironmentVariable($name)
    [Environment]::SetEnvironmentVariable($name, '')
}
try {
    $groupPolicy = Test-PandoraGatedSkipPolicy -GatedSkips $groupAudit.GatedSkips -RequireDbTests
    $dbViolations = @($groupPolicy.Violations | Where-Object { $_ -match 'MYSQL_DSN|TIDB.*DSN' })
    Assert-True ($dbViolations.Count -eq 3) "-RequireDbTests 只强制三个数据库门(实为 $($dbViolations.Count))"
    Assert-True (($groupPolicy.Violations | Where-Object { $_ -match 'REDIS|KAFKA|ETCD' }).Count -eq 0) 'Redis/Kafka/etcd 缺失不升级为失败'
    Assert-True (($groupPolicy.Warnings | Where-Object { $_ -match 'REDIS|KAFKA|ETCD' }).Count -eq 3) 'Redis/Kafka/etcd 缺失保持三条未验证告警'
}
finally {
    foreach ($name in $savedGateEnv.Keys) {
        [Environment]::SetEnvironmentVariable($name, $savedGateEnv[$name])
    }
}

Write-Host '[8] 环境变量必须按完整 token 精确识别' -ForegroundColor Cyan
$redis8Names = @(
    'PANDORA_TEST_REDIS8_ADDR'
    'PANDORA_TEST_REDIS8_PASSWORD'
    'PANDORA_TEST_REDIS8_CLUSTER_ADDRS'
    'PANDORA_TEST_REDIS8_CLUSTER_PASSWORD'
    'PANDORA_TEST_REDIS8_SENTINEL_ADDRS'
    'PANDORA_TEST_REDIS8_SENTINEL_MASTER_NAME'
    'PANDORA_TEST_REDIS8_SENTINEL_PASSWORD'
)
$exactLines = @()
foreach ($name in $redis8Names) {
    $testName = "TestExact_$name"
    $exactLines += New-Event 'output' $pkgUtil $testName "missing $name`n"
    $exactLines += New-Event 'skip' $pkgUtil $testName $null
}
$exactLines += New-Event 'output' $pkgUtil 'TestPrefixSuffix' 'not gates: XPANDORA_TEST_REDIS8_ADDR PANDORA_TEST_REDIS8_ADDR_SUFFIX'
$exactLines += New-Event 'skip' $pkgUtil 'TestPrefixSuffix' $null
$exactAudit = Get-GoTestSkipAudit -JsonLines $exactLines
foreach ($name in $redis8Names) {
    Assert-True (($exactAudit.GatedSkips | Where-Object { $_.Envs -contains $name }).Count -eq 1) "$name 被精确识别一次"
}
Assert-True (($exactAudit.GatedSkips | Where-Object { $_.Test -eq 'TestPrefixSuffix' }).Count -eq 0) '前后粘连的相似变量名不得命中'
Assert-True (($exactAudit.GatedSkips | Where-Object { $_.Envs -contains 'PANDORA_TEST_REDIS' }).Count -eq 0) 'Redis8 变量不得误归类成旧 PANDORA_TEST_REDIS'

Write-Host '[9] 多变量 gate 按整组判断,不得把已设置成员误判为透传失败' -ForegroundColor Cyan
$multiLines = @(
    New-Event 'output' $pkgUtil 'TestRedis8ACL' "set PANDORA_TEST_REDIS8_ADDR/PASSWORD for real Redis 8 ACL integration`n"
    New-Event 'skip' $pkgUtil 'TestRedis8ACL' $null
)
$multiAudit = Get-GoTestSkipAudit -JsonLines $multiLines
$multiGate = @($multiAudit.GatedSkips | Where-Object { $_.Test -eq 'TestRedis8ACL' })
Assert-True ($multiGate.Count -eq 1) '一个测试只生成一个 gate 记录'
Assert-True ($multiGate[0].Envs.Count -eq 2) 'ADDR/PASSWORD 简写还原成两个完整前置变量'
$savedAddr = $env:PANDORA_TEST_REDIS8_ADDR
$savedPassword = $env:PANDORA_TEST_REDIS8_PASSWORD
try {
    $env:PANDORA_TEST_REDIS8_ADDR = '127.0.0.1:6379'
    $env:PANDORA_TEST_REDIS8_PASSWORD = ''
    $multiPolicy = Test-PandoraGatedSkipPolicy -GatedSkips $multiAudit.GatedSkips -RequireDbTests:$false
    Assert-True (($multiPolicy.Violations | Where-Object { $_ -match 'REDIS8_ADDR' }).Count -eq 0) '地址已设置、密码缺失时不得诬告地址透传失败'
    Assert-True (($multiPolicy.Warnings | Where-Object { $_ -match 'REDIS8_PASSWORD' }).Count -eq 1) '只告警真正缺失的密码'

    $env:PANDORA_TEST_REDIS8_PASSWORD = 'ephemeral-test-only'
    $allSetPolicy = Test-PandoraGatedSkipPolicy -GatedSkips $multiAudit.GatedSkips
    Assert-True ($allSetPolicy.Violations.Count -eq 1) '整组变量全设置仍 Skip 才判配置失效'
}
finally {
    $env:PANDORA_TEST_REDIS8_ADDR = $savedAddr
    $env:PANDORA_TEST_REDIS8_PASSWORD = $savedPassword
}

Write-Host '[10] Go 1.26 BuildEvent 必须还原编译输出并记录失败根包' -ForegroundColor Cyan
$buildImport = 'github.com/luyuancpp/pandora/example [github.com/luyuancpp/pandora/example.test]'
$go126Lines = @(
    New-BuildEvent 'build-output' $buildImport "# github.com/luyuancpp/pandora/example`n"
    New-BuildEvent 'build-output' $buildImport "example_test.go:3:11: undefined: missingSymbol`n"
    New-BuildEvent 'build-fail' $buildImport $null
    (([ordered]@{ Time = '2026-08-12T00:00:00Z'; Action = 'fail'; Package = 'github.com/luyuancpp/pandora/example'; Elapsed = 0; FailedBuild = $buildImport }) | ConvertTo-Json -Compress)
)
$go126Audit = Get-GoTestSkipAudit -JsonLines $go126Lines
Assert-True (($go126Audit.Console -join "`n") -match 'undefined: missingSymbol') 'build-output 编译错误进入 Console'
Assert-True ($go126Audit.ParseErrors.Count -eq 0) '合法 BuildEvent 不得伪装成 ParseError'
Assert-True ($go126Audit.BuildFailures.Count -eq 1) 'build-fail 与 FailedBuild 去重为一个失败根包'
Assert-True ($go126Audit.BuildFailures[0] -eq $buildImport) '失败根包使用 Go 1.26 ImportPath/FailedBuild 精确值'

if ($script:Failures.Count -gt 0) {
    Write-Host "`n[FAIL] $($script:Failures.Count) 条断言未通过。" -ForegroundColor Red
    exit 1
}
Write-Host "`n[PASS] go_test_skip_audit 契约测试全部通过。" -ForegroundColor Green
