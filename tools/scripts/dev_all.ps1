# Pandora 一键开发环境(基础设施 + 全部业务服务)
#
# 这是给"UE 联调"用的一条命令:先拉起 docker 基础设施(MySQL/Redis/Kafka/etcd/Envoy),
# 等容器 healthy 后,再按依赖顺序拉起 Go 业务服务。
#
# 用法:
#   # 起全部业务服务(默认)
#   pwsh tools/scripts/dev_all.ps1
#
#   # 全起但排除某个服务(留给 VS Code 断点调试)
#   pwsh tools/scripts/dev_all.ps1 -Exclude team
#
#   # 全停(基础设施 + 业务服务)
#   pwsh tools/scripts/dev_all.ps1 -Down

[CmdletBinding()]
param(
    [string[]]$Exclude = @(),
    [switch]$Pull,
    [switch]$Down,

    # 免 Docker 模式(策划机):基础设施改用本机原生进程(local_infra.ps1),
    # 不起 TiDB(社交四服改连本机 MySQL 的 pandora_social)。
    # 默认关 = 完全保持原有 docker 行为(CLAUDE.md §14.2)。
    [switch]$NoDocker
)

$ErrorActionPreference = 'Stop'
$ScriptDir = $PSScriptRoot
. (Join-Path $ScriptDir 'lib/local_infra_state.ps1')
$projectRoot = (Resolve-Path "$ScriptDir/../..").Path

Enter-PandoraOrchestrationLock -ProjectRoot $projectRoot -Operation $(if ($Down) { '完整停止' } else { '完整启动' })
try {
if ($Down) {
    Write-Host "===== 停止业务服务 =====" -ForegroundColor Cyan
    & "$ScriptDir/run_services.ps1" -Action down
    if ($LASTEXITCODE -ne 0) {
        Write-Host '[ERR] 业务服务未能全部停止；保留基础设施，避免仍存活服务被突然断库。' -ForegroundColor Red
        exit 1
    }
    Write-Host ""
    Write-Host "===== 停止基础设施 =====" -ForegroundColor Cyan
    if ($NoDocker) { & "$ScriptDir/local_infra.ps1" -Action down } else { & "$ScriptDir/dev_down.ps1" }
    exit $LASTEXITCODE
}

if ($NoDocker) {
    # ===== 免 Docker 路线 =====
    # 与 docker 路线的差异:基础设施换成本机进程、不起 TiDB、迁移器用本机 mysql.exe；
    # MySQL 使用独立动态端口，并在 run/localinfra 下派生服务运行态配置，仓库 yaml 不分叉。
    Write-Host "===== [1/3] 本机基础设施(免 Docker) =====" -ForegroundColor Cyan
    # 端口权威与“业务服务已经应用的端口”是两件事。只有完整服务启动成功才更新后者；
    # 即使上轮在基础设施启动后半途失败，下轮也仍会强制刷新旧 DSN 进程。
    $appliedMysql = Get-PandoraServiceAppliedMysqlState $projectRoot
    & "$ScriptDir/local_infra.ps1" -Action up
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERR] 本机基础设施启动失败,中止" -ForegroundColor Red
        exit 1
    }
    $mysqlPort = Get-PandoraLocalMysqlPort $projectRoot -Required

    if (-not $appliedMysql -or $appliedMysql.Mode -ne 'nodocker' -or
        [int]$appliedMysql.MysqlPort -ne $mysqlPort -or -not [bool]$appliedMysql.SocialOnMysql) {
        $fromText = if ($appliedMysql) { "$($appliedMysql.Mode)/:$($appliedMysql.MysqlPort)/social_mysql=$($appliedMysql.SocialOnMysql)" } else { '未登记/旧版' }
        Write-Host "[INFO] 业务服务已应用运行态为 $fromText，当前为 nodocker/:$mysqlPort/social_mysql=True；完整停止本项目业务服务以刷新 DSN。" -ForegroundColor Cyan
        & "$ScriptDir/run_services.ps1" -Action down
        if ($LASTEXITCODE -ne 0) {
            Write-Host '[ERR] 旧业务服务停止失败，不能带着旧 MySQL DSN 继续启动。' -ForegroundColor Red
            exit 1
        }
    }

    Write-Host ""
    Write-Host "===== [2/3] 数据库结构 =====" -ForegroundColor Cyan
    # 免 Docker 模式用 local_infra 备料的 mysql.exe 作客户端(docker exec 用不了)。
    $mysqlClient = Get-ChildItem -Path (Join-Path $ScriptDir '../../run/localinfra/dist/mysql') `
        -Recurse -File -Filter 'mysql.exe' -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $mysqlClient) {
        Write-Host "[ERR] 找不到本机 mysql.exe(备料应由 local_infra.ps1 完成),中止" -ForegroundColor Red
        exit 1
    }
    & "$ScriptDir/dev_migrate.ps1" -MysqlClient $mysqlClient.FullName -MysqlPort $mysqlPort -RequireMysql
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERR] 数据库结构升级失败,中止(继续启动只会让服务连着旧结构崩溃)" -ForegroundColor Red
        exit 1
    }

    Write-Host ""
    Write-Host "===== [3/3] 业务服务 =====" -ForegroundColor Cyan
    & "$ScriptDir/run_services.ps1" -Exclude $Exclude -SocialOnMysql -NoDocker -MysqlPort $mysqlPort
    exit $LASTEXITCODE
}

# 1) 基础设施
Write-Host "===== [1/4] 基础设施 =====" -ForegroundColor Cyan
if ($Pull) { & "$ScriptDir/dev_up.ps1" -Pull } else { & "$ScriptDir/dev_up.ps1" }
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERR] 基础设施启动失败,中止" -ForegroundColor Red
    exit 1
}

# 2) TiDB 集群。friend / chat / guild / mail 四个服务在 run_services.ps1 里被钉死用
# *-dev-tidb.yaml(DSN 指向 127.0.0.1:4000),不起 TiDB 它们必然启动即崩
# (panic: ping mysql: dial tcp 127.0.0.1:4000 ... 拒绝)。tidb_up.ps1 幂等,已在跑则快速返回。
# ⚠️ 首次会拉 pingcap/{pd,tikv,tidb} 镜像(数百 MB,需联网)。
Write-Host ""
Write-Host "===== [2/4] TiDB 集群(社交库) =====" -ForegroundColor Cyan
& "$ScriptDir/tidb_up.ps1"
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERR] TiDB 启动失败,中止(friend/chat/guild/mail 连的就是它)" -ForegroundColor Red
    exit 1
}

# 3) 数据库结构升级。必须夹在「基础设施起好」与「业务服务启动」之间:
# mysql-init 只在数据卷首次创建时跑一次,之后新增的迁移不会自动进库,服务连上旧
# 结构会启动即崩(实测 player: Unknown column 'exp')。幂等,已最新则空跑。
Write-Host ""
Write-Host "===== [3/4] 数据库结构 =====" -ForegroundColor Cyan
& "$ScriptDir/dev_migrate.ps1" -RequireMysql
if ($LASTEXITCODE -ne 0) {
    Write-Host "[ERR] 数据库结构升级失败,中止(继续启动只会让服务连着旧结构崩溃)" -ForegroundColor Red
    exit 1
}

# 4) 业务服务
Write-Host ""
Write-Host "===== [4/4] 业务服务 =====" -ForegroundColor Cyan
& "$ScriptDir/run_services.ps1" -Exclude $Exclude
exit $LASTEXITCODE
} finally {
    Exit-PandoraOrchestrationLock
}
