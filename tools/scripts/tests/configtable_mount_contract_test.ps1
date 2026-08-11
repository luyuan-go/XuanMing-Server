# configtable_mount_contract_test — 配置表挂载三处一致性门禁(2026-08-11)。
#
# 背景:凡 dev 模板里配了 `config_table` 的服务,gen_cluster_config.ps1 都会把它的
# `config_table.dir` 改写成容器内路径 `/app/configtable/active`(生成器已有机械闸:
# 产物里残留宿主相对路径就报错)。但**"改了路径"和"容器里真有这个目录"是两件事** ——
# 目录得由编排层挂进去,而挂载点分散在两个文件里:
#
#   deploy/docker-compose.services.yml   docker 模式:bind mount ../configtable/dist
#   deploy/k8s/services/services.yaml    k8s 模式  :configMap pandora-configtable
#
# 漏挂的后果是**启动即死**:这些服务把配置表当强依赖,加载失败一律 fail-closed 退出
# (mission/inventory 的 main.go 都是 os.Exit(1)),表现为容器起不来 / CrashLoopBackOff,
# 而生成、构建、单测全绿 —— 只有真去跑一键启动才会撞上。
#
# 实锤:2026-08-11 复核发现 docker-compose 里 **mission / inventory / ds-allocator /
# battle-result 四个服务全都漏挂**(k8s 侧七个都齐)。k8s 齐而 compose 缺,正是"两处
# 手工维护、只改了想起来的那一处"的典型。本测试把三处清单绑死,漂移即失败。
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Off
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$GeneratorPath = Join-Path $ProjectRoot 'tools/scripts/gen_cluster_config.ps1'
$ComposePath = Join-Path $ProjectRoot 'deploy/docker-compose.services.yml'
$K8sPath = Join-Path $ProjectRoot 'deploy/k8s/services/services.yaml'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

function Format-Diff([object[]]$Diff) {
    $missing = @($Diff | Where-Object SideIndicator -eq '<=' | ForEach-Object InputObject)
    $extra = @($Diff | Where-Object SideIndicator -eq '=>' | ForEach-Object InputObject)
    return "缺=[$($missing -join ', ')] 多=[$($extra -join ', ')]"
}

# ── 1) 权威清单:生成器里 Set-ServiceClusterConfigTableDir 的服务白名单 ──
$generator = Get-Content -LiteralPath $GeneratorPath -Raw
$whitelistMatch = [regex]::Match(
    $generator,
    "if \(\`$s\.Name -in @\((?<list>[^)]*)\)\) \{\s*\r?\n\s*\`$out = Set-ServiceClusterConfigTableDir")
Assert-True $whitelistMatch.Success `
    'gen_cluster_config.ps1 里未找到 Set-ServiceClusterConfigTableDir 的服务白名单(正则与写法漂移)'
$expected = @([regex]::Matches($whitelistMatch.Groups['list'].Value, "'(?<name>[a-z0-9-]+)'") |
    ForEach-Object { $_.Groups['name'].Value } | Sort-Object -Unique)
Assert-True ($expected.Count -ge 5) "白名单解析结果异常(实际=$($expected.Count) 项)"

# 白名单本身必须与"dev 模板里真配了 config_table 的服务"一致 —— 否则生成器的
# 宿主相对路径闸会在生成期直接抛错,那是另一道门,这里只做正向核对以定位漏登记。
$serviceEntries = @([regex]::Matches(
    $generator,
    "@\{\s*Name\s*=\s*'(?<name>[^']+)';\s*Conf\s*=\s*'(?<conf>[^']+)';\s*Port\s*=\s*(?<port>\d+)\s*\}"))
Assert-True ($serviceEntries.Count -ge 20) "服务清单解析失败(实际=$($serviceEntries.Count) 项)"
$declaring = @(
    foreach ($entry in $serviceEntries) {
        $confPath = Join-Path $ProjectRoot $entry.Groups['conf'].Value
        if (-not (Test-Path -LiteralPath $confPath -PathType Leaf)) {
            throw "服务清单指向不存在的 dev 配置:$confPath"
        }
        if ([regex]::IsMatch((Get-Content -LiteralPath $confPath -Raw), '(?m)^config_table:')) {
            $entry.Groups['name'].Value
        }
    }
) | Sort-Object -Unique
$declareDiff = @(Compare-Object -ReferenceObject $expected -DifferenceObject $declaring -CaseSensitive)
Assert-True ($declareDiff.Count -eq 0) `
    ("生成器 config_table 白名单与'dev 模板真配了 config_table'的服务集不一致:" + (Format-Diff $declareDiff))

# ── 2) docker-compose:每个白名单服务必须 bind mount configtable ──
$compose = Get-Content -LiteralPath $ComposePath -Raw
$composeMounted = @(
    foreach ($name in $expected) {
        # 取该服务块(到下一个顶层两空格缩进的服务名为止),在块内找 configtable bind mount。
        $blockMatch = [regex]::Match($compose, "(?ms)^  $([regex]::Escape($name)):\r?\n(?<body>.*?)(?=^  [a-z0-9-]+:\r?\n|\z)")
        if (-not $blockMatch.Success) { throw "docker-compose.services.yml 缺服务块:$name" }
        if ($blockMatch.Groups['body'].Value -match '\.\./configtable/dist:/app/configtable/active:ro') {
            $name
        }
    }
) | Sort-Object -Unique
$composeDiff = @(Compare-Object -ReferenceObject $expected -DifferenceObject $composeMounted -CaseSensitive)
Assert-True ($composeDiff.Count -eq 0) `
    ("docker-compose.services.yml 的 configtable 挂载与生成器白名单不一致(漏挂 = 容器启动即 fail-closed 退出):" +
     (Format-Diff $composeDiff))

# ── 3) k8s services.yaml:每个白名单服务的 Deployment 必须挂 pandora-configtable ──
$k8sRaw = Get-Content -LiteralPath $K8sPath -Raw
$k8sDocs = @([regex]::Split($k8sRaw, "(?m)^---\r?$"))
$k8sMounted = @(
    foreach ($doc in $k8sDocs) {
        if ($doc -notmatch '(?m)^kind:\s*Deployment\s*$') { continue }
        $nameMatch = [regex]::Match($doc, '(?m)^metadata:\s*\{\s*name:\s*(?<name>[a-z0-9-]+)')
        if (-not $nameMatch.Success) { continue }
        if ($doc -match 'configMap:\s*\{\s*name:\s*pandora-configtable') { $nameMatch.Groups['name'].Value }
    }
) | Sort-Object -Unique
$k8sDiff = @(Compare-Object -ReferenceObject $expected -DifferenceObject $k8sMounted -CaseSensitive)
Assert-True ($k8sDiff.Count -eq 0) `
    ("deploy/k8s/services/services.yaml 的 pandora-configtable 挂载与生成器白名单不一致:" + (Format-Diff $k8sDiff))

Write-Host "configtable_mount_contract_test: PASS($($expected.Count) 个服务三处一致:$($expected -join ', '))" -ForegroundColor Green
