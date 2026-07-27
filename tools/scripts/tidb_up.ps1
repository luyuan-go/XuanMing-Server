# Pandora TiDB 集群一键起服 + 装载全部 tidb-init schema(2026-06-18;2026-07-27 改为遍历目录)
#
# 本脚本把"起 TiDB 集群 + 建账号 + 装载 deploy/tidb-init/*.sql"收敛成一条命令。
# 2026-07-27 前只硬编码装载 01-social-tidb.sql,02-owner-tidb.sql 从未被本链路装载过;
# 改为按文件名排序遍历目录,新增 DDL 文件自动纳入(库名白名单见 $ExpectedDatabases)。
#
# 覆盖的库(各自对应一份自包含 DDL):
#   pandora_social   friend / chat / guild / mail(好友图迁 TiDB,friend-distributed-scaling.md §8/§14)
#   pandora_owner    owner 权威(§9.22:线性一致 + 确认写不回滚,生产强制 TiDB)
#   pandora_account  login 账号 / 会话代际 / 选角(全服单点扩容:写 QPS = 全服登录 QPS)
#
# 用法:
#   pwsh tools/scripts/tidb_up.ps1                  # 起集群 + 建账号 + 装载全部 DDL
#   pwsh tools/scripts/tidb_up.ps1 -Pull            # 先拉最新镜像再起
#   pwsh tools/scripts/tidb_up.ps1 -Down            # 停集群(保留数据卷)
#   pwsh tools/scripts/tidb_up.ps1 -Down -Volumes   # 停 + 删数据卷(彻底重来)
#
# 前置:本机已装 Docker;首次起会拉 pingcap/{pd,tikv,tidb} 镜像(需联网)。
# ⚠️ 生产用 TiUP / Operator 建集群时必须显式设 new_collations_enabled_on_first_bootstrap=true
#    (该参数**只在首次 bootstrap 生效、之后不可更改**)。guilds.name / chat_groups.name /
#    accounts.account 的列级 utf8mb4_0900_ai_ci 依赖它;关掉的话 TiDB 只在语法上接受 _ci、
#    语义上按 binary 比较且不报错 —— 详见 deploy/tidb-init/README.md「生产前置条件」。
#
# 注:按 AGENTS.md §11.1,本脚本由 Codex / 人执行(起重服务 / 拉镜像属环境动作)。

param(
    [switch]$Pull,
    [switch]$Down,
    [switch]$Volumes
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot/../.."
$ComposeFile = "$ProjectRoot/deploy/docker-compose.tidb.yml"
$DdlDir      = "$ProjectRoot/deploy/tidb-init"
$Network     = "pandora-tidb-net"
$ProjectName = "pandora-tidb"

# *-dev-tidb.yaml DSN 里的账号(改 DSN 时同步改这里)
$DbUser = "pandora"
$DbPwd  = "pandora_dev_pwd"

# 期望被装载的库白名单。与 deploy/tidb-init/*.sql 里 CREATE DATABASE 出现的库对账,
# 漂移即 throw —— 否则新增一个 DDL 文件时"库建出来了但 pandora 账号没授权",现象是
# 「schema 装载成功、服务连不上」,极难排查。新增库时同步加到这里。
$ExpectedDatabases = @('pandora_social', 'pandora_owner', 'pandora_account')

if (-not (Test-Path $ComposeFile)) {
    Write-Host "[ERR] compose file not found: $ComposeFile" -ForegroundColor Red
    exit 1
}

# ---- 停集群 ----
if ($Down) {
    Write-Host "===== Pandora TiDB down =====" -ForegroundColor Cyan
    if ($Volumes) {
        docker compose -p $ProjectName -f $ComposeFile down -v
        Write-Host "[OK] TiDB stopped, volumes removed." -ForegroundColor Green
    } else {
        docker compose -p $ProjectName -f $ComposeFile down
        Write-Host "[OK] TiDB stopped, volumes kept." -ForegroundColor Green
    }
    exit $LASTEXITCODE
}

Write-Host "===== Pandora TiDB up =====" -ForegroundColor Cyan
Write-Host "Compose: $ComposeFile"
Write-Host "DDL dir: $DdlDir"
Write-Host ""

# ---- 1. validate ----
Write-Host "[1/5] Validating compose file..." -ForegroundColor Yellow
docker compose -p $ProjectName -f $ComposeFile config --quiet
if ($LASTEXITCODE -ne 0) { Write-Host "[ERR] compose invalid" -ForegroundColor Red; exit 1 }

# ---- 2. pull (可选) ----
if ($Pull) {
    Write-Host "[2/5] Pulling images..." -ForegroundColor Yellow
    docker compose -p $ProjectName -f $ComposeFile pull
    if ($LASTEXITCODE -ne 0) { Write-Host "[ERR] pull failed" -ForegroundColor Red; exit 1 }
} else {
    Write-Host "[2/5] Skipping pull (use -Pull to refresh)" -ForegroundColor Yellow
}

# ---- 3. up ----
Write-Host "[3/5] Starting TiDB cluster..." -ForegroundColor Yellow
docker compose -p $ProjectName -f $ComposeFile up -d
if ($LASTEXITCODE -ne 0) { Write-Host "[ERR] compose up failed" -ForegroundColor Red; exit 1 }

# ---- 4. 等 TiDB :4000 可连(用一次性 mysql client 容器 ping)----
Write-Host "[4/5] Waiting for TiDB SQL layer (:4000)..." -ForegroundColor Yellow
$ready = $false
for ($i = 1; $i -le 30; $i++) {
    docker run --rm --network $Network mysql:8.4 `
        mysqladmin ping -h tidb -P 4000 -u root --silent 2>$null
    if ($LASTEXITCODE -eq 0) { $ready = $true; break }
    Start-Sleep -Seconds 3
    Write-Host "  ...still waiting ($i/30)"
}
if (-not $ready) {
    Write-Host "[ERR] TiDB not ready after ~90s. Check: docker compose -p $ProjectName -f $ComposeFile logs tidb" -ForegroundColor Red
    exit 1
}
Write-Host "  TiDB is up." -ForegroundColor Green

# ---- 5. 建账号 + 装载 schema ----
# 遍历 deploy/tidb-init/*.sql 按文件名排序逐个装载(此前硬编码只装 01-social-tidb.sql,
# 导致 02-owner-tidb.sql 从未被本链路装载过)。
# 可重复执行的依据:三份 DDL 都是**自包含**的(各自 CREATE DATABASE + USE +
# CREATE TABLE IF NOT EXISTS),对已有数据卷再跑一次只会补建缺失的库表,不破坏既有数据。
Write-Host "[5/5] Creating user + loading schema..." -ForegroundColor Yellow

$ddlFiles = @(Get-ChildItem -LiteralPath $DdlDir -Filter '*.sql' | Sort-Object Name)
if ($ddlFiles.Count -eq 0) {
    Write-Host "[ERR] no *.sql found in $DdlDir" -ForegroundColor Red; exit 1
}

# 5a. 从 DDL 文件里提取库名作为唯一来源,再与白名单对账(不手写第二份清单)。
$declared = [System.Collections.Generic.HashSet[string]]::new()
foreach ($f in $ddlFiles) {
    $content = Get-Content -Raw -LiteralPath $f.FullName
    foreach ($m in [regex]::Matches($content, '(?im)^\s*CREATE\s+DATABASE\s+(?:IF\s+NOT\s+EXISTS\s+)?`(pandora_\w+)`')) {
        [void]$declared.Add($m.Groups[1].Value)
    }
}
$missing = @($ExpectedDatabases | Where-Object { -not $declared.Contains($_) })
$extra = @($declared | Where-Object { $ExpectedDatabases -notcontains $_ })
if ($missing.Count -gt 0 -or $extra.Count -gt 0) {
    Write-Host "[ERR] tidb-init 库清单与白名单漂移。缺少:$($missing -join ', ');多出:$($extra -join ', ')" -ForegroundColor Red
    Write-Host "      新增 DDL 文件时请同步更新本脚本的 \$ExpectedDatabases(否则库建出来但账号无权限)。" -ForegroundColor Red
    exit 1
}

# 5b. 建 pandora 账号并对全部库授权(TiDB root 默认无密码)。
#     ⚠️ pandora_social 的 GRANT 必须保留,否则 friend/chat/guild/mail 全断。
$grantLines = ($ExpectedDatabases | ForEach-Object { "GRANT ALL PRIVILEGES ON ``$_``.* TO '$DbUser'@'%';" }) -join "`n"
$bootstrapSql = @"
CREATE USER IF NOT EXISTS '$DbUser'@'%' IDENTIFIED BY '$DbPwd';
$grantLines
FLUSH PRIVILEGES;
"@
$bootstrapSql | docker run --rm -i --network $Network mysql:8.4 `
    mysql -h tidb -P 4000 -u root
if ($LASTEXITCODE -ne 0) { Write-Host "[ERR] bootstrap user/grants failed" -ForegroundColor Red; exit 1 }

# 5c. 逐个装载 DDL。$LASTEXITCODE 必须**在循环内**判 —— 只在循环外判的话,前面文件的
#     失败码会被后一次 docker run 覆盖,变成静默吞错。
foreach ($f in $ddlFiles) {
    Write-Host "  loading $($f.Name)..." -ForegroundColor Gray
    Get-Content -Raw -LiteralPath $f.FullName | docker run --rm -i --network $Network mysql:8.4 `
        mysql -h tidb -P 4000 -u root
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERR] load schema failed: $($f.Name)" -ForegroundColor Red; exit 1
    }
}

# 5d. 回读断言:确认期望的库都真的存在(装载"成功"不等于库建出来了)。
$actual = (@"
SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME LIKE 'pandora\_%';
"@ | docker run --rm -i --network $Network mysql:8.4 mysql -h tidb -P 4000 -u root -N 2>$null) -split "`r?`n" |
    ForEach-Object { $_.Trim() } | Where-Object { $_ }
$notCreated = @($ExpectedDatabases | Where-Object { $actual -notcontains $_ })
if ($notCreated.Count -gt 0) {
    Write-Host "[ERR] 装载后仍缺库:$($notCreated -join ', ')" -ForegroundColor Red; exit 1
}

Write-Host ""
Write-Host "===== TiDB ready =====" -ForegroundColor Green
Write-Host "  SQL:    127.0.0.1:4000 (user=$DbUser)"
Write-Host "  DBs:    $($ExpectedDatabases -join ', ')"
Write-Host "  DDL:    $(($ddlFiles | ForEach-Object { $_.Name }) -join ', ')"
Write-Host "  Status: http://127.0.0.1:10080/status"
Write-Host ""
Write-Host "Start services against TiDB:" -ForegroundColor Cyan
Write-Host "  login   --conf services/account/login/etc/login-dev-tidb.yaml"
Write-Host "  friend  --conf services/social/friend/etc/friend-dev-tidb.yaml"
Write-Host "  guild   --conf services/social/guild/etc/guild-dev-tidb.yaml"
Write-Host "  chat    --conf services/social/chat/etc/chat-dev-tidb.yaml"
Write-Host "  mail    --conf services/social/mail/etc/mail-dev-tidb.yaml"
