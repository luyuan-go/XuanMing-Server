<#
.SYNOPSIS
  后端 CI 构建入口:按 go.work 的 use 清单逐模块 go build + go test。

.DESCRIPTION
  供 Jenkins(仓库根 Jenkinsfile)或本机手工调用。不做镜像构建/发布 —— 那是
  publish_offline_images.ps1 的职责,由流水线在测试全绿后单独调用。
  任何模块失败立即整体失败,不吞错(AGENTS.md §8)。

.PARAMETER RequireDbTests
  把「依赖门控用例被跳过」从告警提升为门禁失败。等流水线真正挂上测试 MySQL / TiDB
  之后打开(或设环境变量 PANDORA_CI_REQUIRE_DB_TESTS=1),此后再有人把 DSN 摘掉就会红。

.EXAMPLE
  pwsh tools/scripts/ci_backend.ps1

.EXAMPLE
  # 带真实数据库跑(依赖门控用例才会真正执行):
  $env:PANDORA_TEST_MYSQL_DSN = 'root:pandora_dev_root@tcp(127.0.0.1:3307)/'
  $env:PANDORA_TEST_TIDB_DSN  = 'root:@tcp(127.0.0.1:4000)/'
  pwsh tools/scripts/ci_backend.ps1 -RequireDbTests
#>
[CmdletBinding()]
param(
    [switch]$RequireDbTests
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../..").Path

. (Join-Path $PSScriptRoot 'lib/go_test_skip_audit.ps1')

if (-not $RequireDbTests -and $env:PANDORA_CI_REQUIRE_DB_TESTS -in @('1', 'true', 'True', 'yes')) {
    $RequireDbTests = $true
}

# ---- 解析 go.work 的 use 清单(支持单行 use 与 use ( ... ) 块) ----
$goWork = Join-Path $ProjectRoot 'go.work'
if (-not (Test-Path -LiteralPath $goWork)) { throw "找不到 go.work:$goWork" }
$modules = @()
$inBlock = $false
foreach ($line in Get-Content -LiteralPath $goWork) {
    $t = ($line -replace '//.*$', '').Trim()
    if (-not $t) { continue }
    if ($t -match '^use\s*\($') { $inBlock = $true; continue }
    if ($inBlock) {
        if ($t -eq ')') { $inBlock = $false; continue }
        $modules += $t
        continue
    }
    if ($t -match '^use\s+(\S+)$') { $modules += $Matches[1] }
}
if ($modules.Count -eq 0) { throw 'go.work 未解析到任何 use 模块。' }

Write-Host "[INFO] go.work 模块数:$($modules.Count)" -ForegroundColor Cyan
$goVersion = (go env GOVERSION 2>$null | Out-String).Trim()
Write-Host "[INFO] Go:$goVersion" -ForegroundColor Cyan

# 依赖门控 DSN 一览:先打出来,让日志顶部就能看清本轮到底具备哪些验证能力。
Write-Host '[INFO] 依赖门控环境变量:' -ForegroundColor Cyan
foreach ($envName in (Get-PandoraDbGatedEnvNames)) {
    $isSet = -not [string]::IsNullOrWhiteSpace([Environment]::GetEnvironmentVariable($envName))
    $mark = if ($isSet) { '已设置' } else { '未设置(相关用例将被跳过)' }
    $color = if ($isSet) { 'Green' } else { 'DarkYellow' }
    Write-Host ("       {0,-30} {1}" -f $envName, $mark) -ForegroundColor $color
}

$failed = @()
$allGatedSkips = [System.Collections.Generic.List[pscustomobject]]::new()
$totalPassed = 0
$totalSkipped = 0

foreach ($m in $modules) {
    $dir = Join-Path $ProjectRoot ($m -replace '^\./', '' -replace '/', '\')
    if (-not (Test-Path -LiteralPath $dir)) { $failed += "$m(目录不存在)"; continue }
    Write-Host "`n===== $m =====" -ForegroundColor Magenta
    Push-Location $dir
    try {
        go build ./...
        if ($LASTEXITCODE -ne 0) { $failed += "$m(build)"; continue }

        # -json 而非裸 go test:裸输出对「全部用例都 Skip 的包」只会打一个 ok,
        # 与真跑过无法区分(见 lib/go_test_skip_audit.ps1 头注释)。
        # Console 字段把人类可读输出逐字还原,所以日志观感与改造前一致。
        $raw = & go test ./... -count=1 -json 2>&1 | ForEach-Object {
            if ($_ -is [System.Management.Automation.ErrorRecord]) { $_.ToString() } else { [string]$_ }
        }
        $testExit = $LASTEXITCODE
        $audit = Get-GoTestSkipAudit -JsonLines $raw
        $audit.Console | ForEach-Object { Write-Host $_ }
        $totalPassed += $audit.Passed
        $totalSkipped += $audit.Skipped
        foreach ($g in $audit.GatedSkips) { $allGatedSkips.Add($g) }
        if ($testExit -ne 0) { $failed += "$m(test)"; continue }
    } finally { Pop-Location }
}

if ($failed.Count -gt 0) {
    Write-Host "`n[ERR ] 以下模块未通过:" -ForegroundColor Red
    $failed | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}

# ---- 依赖门控跳过审计(2026-08-11)----
#
# 为什么这是门禁而不是日志:friend/mission 两条**确定性** 1213 死锁在真 MySQL 上必现、
# 在 TiDB 上不现,而 CI 从不设 DSN → 相关用例全 Skip → `go test` 打 ok → 流水线长期绿。
# 缺陷不是没被测试覆盖,是覆盖被"跳过等于通过"吃掉了。
$policy = Test-PandoraGatedSkipPolicy -GatedSkips $allGatedSkips.ToArray() -Require:$RequireDbTests
if ($policy.Warnings.Count -gt 0) {
    Write-Host "`n[WARN] 依赖门控用例未执行 —— 本轮绿灯**不覆盖**下列范围:" -ForegroundColor Yellow
    $policy.Warnings | ForEach-Object { Write-Host "  ! $_" -ForegroundColor Yellow }
    Write-Host '       挂上测试库后用 -RequireDbTests(或 PANDORA_CI_REQUIRE_DB_TESTS=1)把它转成门禁。' -ForegroundColor Yellow
}
if ($policy.Violations.Count -gt 0) {
    Write-Host "`n[ERR ] 依赖门控门禁失败:" -ForegroundColor Red
    $policy.Violations | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}

$gatedCount = $allGatedSkips.Count
Write-Host ("`n[ OK ] 全部 {0} 个模块 build + test 通过(用例 通过={1} 跳过={2},其中依赖门控跳过={3})。" -f `
        $modules.Count, $totalPassed, $totalSkipped, $gatedCount) -ForegroundColor Green

# ---- 集群配置生成器契约测试(R11 复审 P0-3)----
#
# 为什么必须进 CI:这些 ps1 断言的是**生成器产物**的方向性契约(如 -Prod 必须把 login 的
# hub_allocator 地址改写成 dns:///hub-allocator-headless FQDN、非 -Prod 必须保留短名)。
# 生成的集群配置 **不入版本库**(.gitignore 的 run/),所以除了这些断言之外没有任何东西
# 能挡住生成器回归 —— 而在此之前 CI 只跑 go test,这些脚本一次都没被执行过,
# 等于"有测试但不是门禁"。
#
# 只登记**当前绿**的脚本。基线即红的(gen_cluster_b1_contract_test 卡在未实现的
# placement 分权 key 注入)不登记:把已知红的脚本塞进 CI 只会让整条流水线长期红,
# 从而掩盖真实回归。要登记它必须先把那条特性做完或明确退役。
# 2026-07-27:补登记三个此前一直绿却没进门禁的脚本 + 新增的 account 契约测试。
# 只写进 tools/scripts/README.md 的表格不算门禁 —— 那是文档,这个数组才是 CI 实际执行的清单。
$contractTests = @(
    'tools/scripts/tests/gen_cluster_prod_progress_contract_test.ps1'
    'tools/scripts/tests/gen_cluster_prod_owner_contract_test.ps1'
    'tools/scripts/tests/gen_cluster_prod_ratelimit_contract_test.ps1'
    'tools/scripts/tests/gen_cluster_session_gate_contract_test.ps1'
    # -Prod 账号库 TiDB DSN + login 开发后门关断(免密登录曾随 -Prod 产物出厂,见脚本头注释)。
    'tools/scripts/tests/gen_cluster_prod_account_contract_test.ps1'
    # Team→Matchmaker 服务身份 key 必须两端成对且与 login 那把不同(漏配只表现为
    # 「招募列表恒空 + 入队被拒」,两端进程都启动成功,人工 review 抓不住)。
    'tools/scripts/tests/gen_cluster_team_resume_auth_contract_test.ps1'
    # 策划一键导表失败时的 SVN 归因(取版本号 / 判未提交)。判错方向 = 把人指到错的地方。
    'tools/scripts/tests/configtable_gen_svn_status_test.ps1'
    # 依赖门控跳过审计本身的门禁(2026-08-11)。它守的是**本脚本上面那段审计会不会失灵**:
    # 误判成"全跑过"就等于把 friend/mission 那类只在真库复现的缺陷重新放回黑箱。
    'tools/scripts/tests/go_test_skip_audit_contract_test.ps1'
)
$contractFailed = @()
foreach ($rel in $contractTests) {
    $path = Join-Path $ProjectRoot ($rel -replace '/', '\')
    if (-not (Test-Path -LiteralPath $path)) { $contractFailed += "$rel(缺文件)"; continue }
    Write-Host "`n===== 契约测试 $rel =====" -ForegroundColor Magenta
    & pwsh -NoProfile -File $path
    if ($LASTEXITCODE -ne 0) { $contractFailed += $rel }
}
if ($contractFailed.Count -gt 0) {
    Write-Host "`n[ERR ] 以下契约测试未通过:" -ForegroundColor Red
    $contractFailed | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}
Write-Host "[ OK ] 契约测试全部通过($($contractTests.Count) 个)。" -ForegroundColor Green
