<#
.SYNOPSIS
  CI 数据库生命周期与 Jenkins 接线的静态契约测试，不启动容器。
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$script:Failures = @()

function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) { Write-Host "  [ ok ] $Message" -ForegroundColor DarkGray }
    else { $script:Failures += $Message; Write-Host "  [FAIL] $Message" -ForegroundColor Red }
}

function Parse-PowerShell([string]$RelativePath) {
    $tokens = $null
    $errors = $null
    [System.Management.Automation.Language.Parser]::ParseFile(
        (Join-Path $ProjectRoot $RelativePath), [ref]$tokens, [ref]$errors) | Out-Null
    return @($errors)
}

$composePath = Join-Path $ProjectRoot 'deploy/docker-compose.ci-db.yml'
$scriptPath = Join-Path $ProjectRoot 'tools/scripts/ci_db.ps1'
$backendPath = Join-Path $ProjectRoot 'tools/scripts/ci_backend.ps1'
$jenkinsPath = Join-Path $ProjectRoot 'Jenkinsfile'

Write-Host '[1] PowerShell 语法' -ForegroundColor Cyan
Assert-True ((Parse-PowerShell 'tools/scripts/ci_db.ps1').Count -eq 0) 'ci_db.ps1 Parser 无错误'
Assert-True ((Parse-PowerShell 'tools/scripts/ci_backend.ps1').Count -eq 0) 'ci_backend.ps1 Parser 无错误'

Write-Host '[2] Compose 隔离与固定版本' -ForegroundColor Cyan
$compose = Get-Content -Raw -LiteralPath $composePath
Assert-True ($compose -match '(?m)^\s*image:\s*mysql:8\.4\s*$') 'MySQL 固定 8.4'
Assert-True (($compose | Select-String -Pattern 'pingcap/(pd|tikv|tidb):v8\.5\.1' -AllMatches).Matches.Count -eq 3) 'PD/TiKV/TiDB 均固定 v8.5.1'
Assert-True ($compose -match '127\.0\.0\.1:\$\{PANDORA_CI_MYSQL_PORT') 'MySQL 只发布到动态回环端口'
Assert-True ($compose -match '127\.0\.0\.1:\$\{PANDORA_CI_TIDB_PORT') 'TiDB 只发布到动态回环端口'
Assert-True ($compose -notmatch '(?m)^\s*container_name:|^volumes:\s*$|mysql-data:|tidb-pd-data:|tidb-tikv-data:') '不固定容器名且不声明持久卷'

Write-Host '[3] 生命周期与无库名 DSN' -ForegroundColor Cyan
$scriptText = Get-Content -Raw -LiteralPath $scriptPath
Assert-True ($scriptText -match "ValidateSet\('Up', 'Down'\)") '生命周期只有 Up/Down'
Assert-True ($scriptText -match "down', '--volumes', '--remove-orphans") '清理包含 volumes 与 orphans'
Assert-True ($scriptText -match 'PANDORA_TEST_MYSQL_DSN\s*=\s*"root@tcp\(127\.0\.0\.1:\$mysqlPort\)/\?') 'MySQL DSN 明确无库名'
Assert-True ($scriptText -match 'PANDORA_TEST_TIDB_DSN\s*=\s*"root@tcp\(127\.0\.0\.1:\$tidbPort\)/\?') 'TiDB 通用 DSN 明确无库名'
Assert-True ($scriptText -match 'PANDORA_TIDB_TEST_DSN\s*=.*pandora_account') 'pkg/mysqlx 历史探针获得初始化库 DSN'
Assert-True ($scriptText -match "StartsWith\('pandora-ci-db-'") 'Down 校验 CI project 前缀后再清理'

Write-Host '[4] Jenkins 强门禁和最终清理' -ForegroundColor Cyan
$jenkins = Get-Content -Raw -LiteralPath $jenkinsPath
Assert-True ($jenkins -match 'ci_db\.ps1\s+-Action\s+Up') 'Build & Test 前启动 CI DB'
Assert-True ($jenkins -match 'ci_backend\.ps1[^\r\n]*-RequireDbTests') 'Jenkins 明确开启数据库必选门禁'
Assert-True ($jenkins -match 'ci_backend\.ps1[^\r\n]*-CiDbStateFile') 'Jenkins 将状态文件显式传给测试进程'
Assert-True ($jenkins -match '(?s)post\s*\{\s*always\s*\{.*ci_db\.ps1\s+-Action\s+Down') 'pipeline post always 清理本轮容器'

Write-Host '[5] ci_backend 只导入数据库白名单' -ForegroundColor Cyan
$backend = Get-Content -Raw -LiteralPath $backendPath
Assert-True ($backend -match '\[string\]\$CiDbStateFile') 'ci_backend 接收状态文件'
Assert-True ($backend -match 'PANDORA_TIDB_TEST_DSN') 'ci_backend 覆盖 pkg/mysqlx TiDB 历史门名'
Assert-True ($backend -notmatch "allowedDbEnv\s*=\s*@\([^\)]*REDIS") '状态文件不能注入 Redis 等非数据库变量'

if ($script:Failures.Count -gt 0) {
    Write-Host "`n[FAIL] $($script:Failures.Count) 条断言未通过。" -ForegroundColor Red
    exit 1
}
Write-Host "`n[PASS] CI DB 契约测试全部通过。" -ForegroundColor Green
