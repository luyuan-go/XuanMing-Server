# Pandora 本机 dev 数据库结构自动升级(local 模式一键启动的一环)
#
# 解决的问题:deploy/mysql-init/*.sql 只在 **MySQL 数据卷第一次创建** 时由容器 entrypoint
# 执行。卷一旦建好,后续新增的结构改动(tools/migrate/migrations/<set>/0000N_*.up.sql)
# 就再也不会自动进库 —— 于是「一键启动」拉起的服务连的是旧 schema,表现为启动即崩,
# 例:player 报 `Unknown column 'exp' in 'field list'`(000002_experience 没执行),
# 而人只看到「一键启动失败」,完全不知道要手工补迁移。本脚本把这一步补进启动链。
#
# 做法:直接复用生产同一个迁移器(tools/migrate),它以库内 `schema_migrations` 为准,
# 只执行缺的版本,天然幂等 —— 不自己写一套版本管理,避免与生产语义分叉。
# 各库的 000001_baseline 就是从 deploy/mysql-init/*.sql 生成的(全 CREATE TABLE IF NOT
# EXISTS),所以对「已由 mysql-init 建好、但没有 schema_migrations」的老库也安全:
# baseline 空跑记版本,之后的增量照常补上。
#
# 用法:
#   pwsh tools/scripts/dev_migrate.ps1            # 升级本机 dev MySQL 到最新结构
#   pwsh tools/scripts/dev_migrate.ps1 -WhatIfOnly # 只列出将要处理的库,不执行
#
# ⚠️ 仅用于本机 dev(127.0.0.1:3307 容器 MySQL,dev 弱口令)。线上迁移走
#    deploy/k8s/migrate/job.yaml + 外部 Secret,与本脚本无关。

[CmdletBinding()]
param(
    # dev MySQL 容器名(docker-compose.dev.yml 的 mysql 服务)
    [string]$Container = 'pandora-mysql',
    # 宿主上 dev MySQL 的地址(docker-compose.dev.yml 发布的端口)
    [string]$MysqlHost = '127.0.0.1',
    [int]$MysqlPort = 3307,
    [string]$MysqlUser = 'pandora',
    # dev 弱口令,与 deploy/env/dev.env / 各 *-dev.yaml 一致;不是密钥。
    [string]$MysqlPassword = 'pandora_dev_pwd',
    # 重放 mysql-init 要建库 + 授权,必须 root(deploy/env/dev.env MYSQL_ROOT_PASSWORD)。
    [string]$MysqlRootPassword = 'pandora_dev_root',
    [switch]$WhatIfOnly
)

$ErrorActionPreference = 'Stop'
$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../..").Path
$MigrationsRoot = Join-Path $ProjectRoot 'tools/migrate/migrations'

function Write-MigInfo($m) { Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Write-MigOk($m)   { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Write-MigWarn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }

Write-Host "===== 数据库结构升级(dev) =====" -ForegroundColor Cyan

# 先确认 MySQL 容器能连。连不上就整个跳过(而不是报错中断):可能是没用 MySQL 的场景。
$dbListRaw = & docker exec $Container sh -c "MYSQL_PWD='$MysqlPassword' mysql -u'$MysqlUser' --batch --skip-column-names -e 'SHOW DATABASES;'" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-MigWarn "连不上 dev MySQL 容器『$Container』,跳过结构升级(基础设施可能还没起完)。"
    Write-MigWarn "  详情:$($dbListRaw | Select-Object -First 3)"
    exit 0
}

# ---------------------------------------------------------------------------
# 第 0 步:重放 deploy/mysql-init/*.sql
#
# 为什么必须有这一步:tools/migrate 只覆盖「有迁移集的库」(migrations/<set>/),而
# mysql-init 里后来新增的库/表根本没有对应迁移集 —— 比如 14-bag-tables.sql(pandora_bag)、
# 15-owner-tables.sql(pandora_owner)。老卷上这两个文件从没执行过,现象:
#   inventory: 缺少数据库表 [bag_migration bag_capacity]
#   owner    : Access denied ... to database 'pandora_owner'(库压根不存在)
# 服务自己的报错甚至直接写了「请对当前库重放 deploy/mysql-init/xx.sql」—— 那就别让人手工做。
#
# 安全性:这批文件全是 CREATE DATABASE / TABLE IF NOT EXISTS(已用正则核过,无 DROP/ALTER/
# INSERT),对已有库重放是空操作,不会动数据。需要 root 是因为 01 里有 CREATE DATABASE + GRANT。
# ---------------------------------------------------------------------------
$initDir = Join-Path $ProjectRoot 'deploy/mysql-init'
$initFiles = @(Get-ChildItem -LiteralPath $initDir -Filter '*.sql' -ErrorAction SilentlyContinue | Sort-Object Name)
if ($initFiles.Count -gt 0) {
    Write-MigInfo "重放 mysql-init 建库建表脚本($($initFiles.Count) 个,全 IF NOT EXISTS,已存在则空跑)..."
    foreach ($f in $initFiles) {
        $sqlOut = Get-Content -LiteralPath $f.FullName -Raw -Encoding utf8 |
            & docker exec -i $Container sh -c "MYSQL_PWD='$MysqlRootPassword' mysql -uroot" 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Host "[ERR] 重放 $($f.Name) 失败:" -ForegroundColor Red
            $sqlOut | ForEach-Object { Write-Host "      $_" -ForegroundColor Red }
            exit 1
        }
    }
    Write-MigOk "mysql-init 脚本已全部重放(建库 / 建表 / 授权已对齐仓库当前状态)。"
}

# ---------------------------------------------------------------------------
# 第 1 步:跑增量迁移(列改动这类 IF NOT EXISTS 表达不了的结构变更)
# ---------------------------------------------------------------------------
if (-not (Test-Path -LiteralPath $MigrationsRoot)) {
    Write-MigWarn "找不到 $MigrationsRoot,跳过增量迁移。"
    exit 0
}

# 1) 本仓库有哪些 migration set(= 目录名 = 库名)
$sets = @(Get-ChildItem -LiteralPath $MigrationsRoot -Directory -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty Name | Sort-Object)
if ($sets.Count -eq 0) {
    Write-MigWarn "$MigrationsRoot 下没有 migration set,跳过。"
    exit 0
}

# 2) 本机 dev MySQL 里实际存在哪些库。只升级「已存在」的库:建库是上面重放 mysql-init
#    的职责,本步不越权建库,免得把拼错的库名凭空建出来。
#    重新查一次 —— 重放可能刚建出了新库(如 pandora_owner)。
$dbListRaw = & docker exec $Container sh -c "MYSQL_PWD='$MysqlPassword' mysql -u'$MysqlUser' --batch --skip-column-names -e 'SHOW DATABASES;'" 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-MigWarn "重放后重查库列表失败,跳过增量迁移。"
    exit 0
}
$existingDbs = @($dbListRaw | ForEach-Object { "$_".Trim() } | Where-Object { $_ })

$targets = @($sets | Where-Object { $existingDbs -contains $_ })
$missing = @($sets | Where-Object { $existingDbs -notcontains $_ })
if ($missing.Count -gt 0) {
    Write-MigInfo "本机没有这些库,跳过(通常是该服务在本机不启用):$($missing -join ', ')"
}
if ($targets.Count -eq 0) {
    Write-MigWarn '本机 dev MySQL 里没有任何可升级的库,跳过。'
    exit 0
}
Write-MigInfo "待检查的库($($targets.Count)):$($targets -join ', ')"
if ($WhatIfOnly) { exit 0 }

# 3) 选迁移器:优先预编译产物(策划机没 Go),否则现场 go run。
$migrateExe = Join-Path $ProjectRoot 'run/artifacts/windows/bin/pandora-migrate.exe'
$hasGo = [bool](Get-Command go -ErrorAction SilentlyContinue)
if (-not (Test-Path -LiteralPath $migrateExe) -and -not $hasGo) {
    # 不阻断启动:结构可能本来就是最新的。但必须把话说清楚,不能让人再对着
    # "Unknown column" 猜半天 —— 这正是本脚本存在的理由。
    Write-MigWarn '本机既没有 Go,也没有预编译的迁移器,无法自动升级数据库结构。'
    Write-MigWarn '  若稍后有服务报 Unknown column / Table doesn''t exist,就是结构没跟上,'
    Write-MigWarn '  请找后端同学跑一次 pwsh tools/scripts/dev_migrate.ps1(需要 Go)。'
    exit 0
}

# 4) 生成迁移器要的 targets 清单 + DSN 文件(它只接受文件形式的 DSN,且必须在清单同目录下)。
#    放临时目录,用完即删。
$tmpDir = Join-Path ([System.IO.Path]::GetTempPath()) "pandora-dev-migrate-$PID"
New-Item -ItemType Directory -Path $tmpDir -Force | Out-Null
try {
    $targetEntries = @()
    $expected = @()
    foreach ($db in $targets) {
        $dsnFile = "$db.dsn"
        # 迁移器的目标名只允许 ^[a-z][a-z0-9-]{0,62}$(不能有下划线),而库名是
        # pandora_player 这种带下划线的 —— 转成连字符。migration_set / database 仍用真库名。
        $name = ($db -replace '_', '-') + '-dev'
        # parseTime/loc 与各服务 DSN 对齐;multiStatements 供 baseline 那种多语句脚本执行。
        $dsn = "${MysqlUser}:${MysqlPassword}@tcp(${MysqlHost}:${MysqlPort})/${db}?parseTime=true&loc=UTC&multiStatements=true"
        Set-Content -LiteralPath (Join-Path $tmpDir $dsnFile) -Value $dsn -Encoding ascii -NoNewline
        $targetEntries += [ordered]@{
            name                      = $name
            migration_set             = $db
            database                  = $db
            dsn_file                  = $dsnFile
            timeout_seconds           = 300
            lock_wait_timeout_seconds = 15
        }
        $expected += "${name}:${db}:${db}"
    }
    $targetsFile = Join-Path $tmpDir 'targets.json'
    Set-Content -LiteralPath $targetsFile -Encoding utf8NoBOM `
        -Value (@{ targets = $targetEntries } | ConvertTo-Json -Depth 5)

    # -environment=dev:免掉迁移器对 TLS 的强制要求(本机容器 MySQL 没有 TLS)。
    # -expected-targets 在生产是「发布侧独立审核清单」,本机 dev 没有第二方,这里与 targets
    # 同源生成 —— 它在本场景只起格式校验作用,不构成额外保证(生产的独立审核仍在 k8s Job 里)。
    $migArgs = @(
        "-targets-file=$targetsFile"
        "-expected-targets=$($expected -join ',')"
        '-environment=dev'
    )

    Write-MigInfo '执行迁移器(只补缺失版本,已是最新则空跑)...'
    # 迁移器的进度日志走 stderr(Go log 默认)。不合并的话 PowerShell 会把每行都渲染成
    # 红色 NativeCommandError,看着像出错了其实一切正常 —— 2>&1 合并成普通输出行。
    # 真正的成败只看退出码。
    if (Test-Path -LiteralPath $migrateExe) {
        & $migrateExe @migArgs 2>&1 | ForEach-Object { "  $_" }
    } else {
        Push-Location (Join-Path $ProjectRoot 'tools/migrate')
        try { & go run . @migArgs 2>&1 | ForEach-Object { "  $_" } } finally { Pop-Location }
    }
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERR] 数据库结构升级失败(exit=$LASTEXITCODE)。" -ForegroundColor Red
        Write-Host "      业务服务连上旧结构只会启动即崩(如 Unknown column),故此处中止。" -ForegroundColor Red
        exit 1
    }
    Write-MigOk '数据库结构已是最新。'
}
finally {
    Remove-Item -LiteralPath $tmpDir -Recurse -Force -ErrorAction SilentlyContinue
}
