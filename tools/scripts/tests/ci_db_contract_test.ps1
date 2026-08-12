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

Write-Host '[5] ci_backend 按白名单导入,且两组语义不混' -ForegroundColor Cyan
$backend = Get-Content -Raw -LiteralPath $backendPath
Assert-True ($backend -match '\[string\]\$CiDbStateFile') 'ci_backend 接收状态文件'
Assert-True ($backend -match 'PANDORA_TIDB_TEST_DSN') 'ci_backend 覆盖 pkg/mysqlx TiDB 历史门名'
# 数据库组仍必须只含三个 DSN:它们是 -RequireDbTests 的硬门禁对象,混进可选依赖
# 会让"缺 Redis"也变成阻断,与 go_test_skip_audit 的分组语义冲突。
Assert-True ($backend -notmatch "allowedDbEnv\s*=\s*@\([^\)]*(REDIS|KAFKA|ETCD)") '数据库必选组不得混入 Redis/Kafka/etcd'
Assert-True ($backend -match 'allowedOptionalEnv\s*=\s*@\(') '可选依赖组有独立白名单'
foreach ($name in @('PANDORA_TEST_REDIS_ADDR', 'PANDORA_TEST_REDIS8_ADDR',
        'PANDORA_TEST_REDIS8_PASSWORD', 'PANDORA_TEST_ETCD_ENDPOINTS', 'PANDORA_TEST_KAFKA_BROKERS')) {
    Assert-True ($backend -match [regex]::Escape($name)) "可选依赖白名单含 $name"
}
# 导入必须由白名单驱动(foreach 白名单),而不是遍历状态文件的键 —— 否则状态文件
# 被篡改就能注入任意环境变量。
Assert-True ($backend -notmatch 'state\.environment\.PSObject\.Properties\s*\)') '导入按白名单遍历,不遍历状态文件键集'

Write-Host '[6] 可选依赖容器与端口' -ForegroundColor Cyan
Assert-True ($compose -match '(?m)^\s*image:\s*redis:8\.8\.0-alpine\s*$') 'Redis 固定 8.8.0-alpine'
Assert-True ($compose -match 'quay\.io/coreos/etcd:v3\.6\.12') 'etcd 固定 v3.6.12'
Assert-True ($compose -match 'confluentinc/cp-kafka:7\.9\.7') 'Kafka 固定 7.9.7'
foreach ($portVar in @('PANDORA_CI_REDIS_PORT', 'PANDORA_CI_REDIS8_PORT',
        'PANDORA_CI_ETCD_PORT', 'PANDORA_CI_KAFKA_PORT')) {
    Assert-True ($compose -match ('127\.0\.0\.1:\$\{' + $portVar)) "$portVar 只发布到动态回环端口"
}
# Kafka 的 advertised listener 会原样发回客户端,容器内外端口必须同号,
# 否则 metadata 给出的地址在宿主上连不通(表现为消费者永久超时,不是连接拒绝)。
Assert-True ($compose -match 'EXTERNAL://0\.0\.0\.0:\$\{PANDORA_CI_KAFKA_PORT') 'Kafka EXTERNAL 监听动态端口本身'
Assert-True ($compose -match 'EXTERNAL://127\.0\.0\.1:\$\{PANDORA_CI_KAFKA_PORT') 'Kafka advertised 用同一动态端口'
# Redis 8 只读 ACL 必须与 poduidpreflight 的精确契约一致,少一条命令就整批失败。
Assert-True ($compose -match 'pandora-pod-uid-release-preflight-ro') 'Redis8 提供专用只读 ACL 身份'
Assert-True ($compose -match '%R~pandora:ds:battle:\*') 'Redis8 ACL 键规则与只读命名空间一致'
Assert-True ($compose -match 'sanitize-payload') 'Redis8 ACL 带 sanitize-payload 标志'
foreach ($cmd in @('-@all', '\+acl\|getuser', '\+cluster\|shards', '\+scan')) {
    Assert-True ($compose -match $cmd) "Redis8 ACL 命令白名单含 $($cmd -replace '\\','')"
}
Assert-True ($scriptText -match 'New-CiSecret') 'Redis8 口令每轮现生成,不落仓库'
Assert-True ($scriptText -match "contract\s*=\s*'pandora-ci-db-v2'") '状态契约升到 v2'

if ($script:Failures.Count -gt 0) {
    Write-Host "`n[FAIL] $($script:Failures.Count) 条断言未通过。" -ForegroundColor Red
    exit 1
}
Write-Host "`n[PASS] CI DB 契约测试全部通过。" -ForegroundColor Green
