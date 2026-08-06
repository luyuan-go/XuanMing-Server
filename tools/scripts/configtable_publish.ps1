# Pandora 配置表发布脚本(docs/design/config-table-hotreload.md §4/§6/§10.5-6)
#
# 职责:把 configtable/dist 的一批产物发布到服务端运行态目录:
#   dist → <DeployRoot>\configtable\staging(先落地,不碰线上)
#        → 逐表 sha256 校验 + 版本单调检查
#        → 旧 active 归档 history\v<版本> → staging 原子改名为 active
#   服务端进程随后经 ConfigTableAdminService.ReloadConfigTable 热加载 active
#   (加载成功才切内存指针,失败保留旧表;本脚本只负责文件面)。
#
# 用法:
#   pwsh tools/scripts/configtable_publish.ps1 -DeployRoot D:\pandora-deploy
#   pwsh tools/scripts/configtable_publish.ps1 -DeployRoot D:\pandora-deploy -ReloadAddr 127.0.0.1:20011
#
# 幂等 / 容错:
#   - dist 版本 == active 版本 → 文件面 no-op,但 -ReloadAddr 仍会触发 reload
#     (崩溃续跑 / 服务重启后内存落后磁盘 / 新副本上线,都靠幂等 reload 收敛,审计 P1);
#   - staging 版本 <  active 版本 → 拒绝(防回退;回滚 = 重新生成更高版本);
#   - 两次改名之间进程崩溃:重跑本脚本即可恢复(active 缺失时 staging 直接补位)。
#
# ⚠️ 定位与边界(2026-07-22 审计钉死):本脚本是**单实例 / dev 联调**发布工具:
#   - 同一 DeployRoot **禁止并发发布**:staging / history 是共用固定目录,无发布锁,
#     并发跑会互删对方中间产物;
#   - -ReloadAddr 只支持单个服务地址,不能替代"全 fleet 原子切换";
#   - 多副本生产发布走 etcd version 键 watch 方案(config-table-hotreload.md 待排期),
#     落地前不得把本脚本包装成生产发布门禁。

param(
    [Parameter(Mandatory = $true)][string]$DeployRoot,
    [string]$DistDir = "",
    [string]$ReloadAddr = ""   # 可选:matchmaker 等服务的 gRPC 地址,发布后用 grpcurl 触发热更
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot/../.."
if ($DistDir -eq "") { $DistDir = Join-Path $ProjectRoot "configtable/dist" }

$ctRoot   = Join-Path $DeployRoot "configtable"
$staging  = Join-Path $ctRoot "staging"
$active   = Join-Path $ctRoot "active"
$history  = Join-Path $ctRoot "history"

function Read-Manifest([string]$dir) {
    $path = Join-Path $dir "manifest.json"
    if (-not (Test-Path $path)) { return $null }
    return Get-Content $path -Raw -Encoding UTF8 | ConvertFrom-Json
}

# 严格非负整数校验(R5 复审 P1-9):ConvertFrom-Json 把 JSON 整数解析成 [long]、小数解析
# 成 [double];PowerShell 的 [uint64] 强转会把 1.5 四舍五入成 2、"2"(字符串)也能转过。
# Go 运行时 json.Unmarshal 到 uint64 对 1.5 / 2.0 / "2" 一律拒载 —— 发布器接受运行时必拒
# 的 manifest 就是"发布成功、重启拒载"。此处按运行时同等严格度把关:只认整数字面量。
function Assert-UInt64Field($value, [string]$field) {
    if ($value -isnot [long] -and $value -isnot [int] -and $value -isnot [int64] -and $value -isnot [uint64]) {
        Write-Host "[ERR] manifest $field='$value'(类型 $($null -ne $value ? $value.GetType().Name : 'null'))非法:必须是 JSON 非负整数字面量(不得带小数/引号),Go 运行时会拒载该形态。" -ForegroundColor Red
        exit 1
    }
    if ([int64]$value -lt 0) {
        Write-Host "[ERR] manifest $field=$value 非法:必须 >= 0。" -ForegroundColor Red
        exit 1
    }
    return [uint64]$value
}

# 比较两份 manifest 表清单的**运行语义字段**(name/file/checksum/proto/rows),返回差异
# 描述列表(空 = 语义一致)。R4 复审 P1-8:同版本门禁只比表文件 hash 时,同字节文件、
# 不同 proto/rows 的 manifest 也能冒充同批次——proto 决定服务端解析容器,rows 参与
# 加载校验,都是会改变运行行为的语义,必须一并入比。
function Compare-ManifestTables($aTables, $bTables) {
    $diff = @()
    if ($null -eq $aTables) { $aTables = @() }
    if ($null -eq $bTables) { $bTables = @() }
    # 语义字段一律**大小写敏感**比对(R5 复审 P1-9):PowerShell -ne 默认大小写不敏感,
    # 大写 checksum/proto 名会被当成相同;Go 运行时是精确比较,发布器必须同等严格。
    $byName = @{}
    foreach ($t in $aTables) { $byName[[string]$t.name] = $t }
    foreach ($t in $bTables) {
        $name = [string]$t.name
        if (-not $byName.ContainsKey($name)) { $diff += "$name(对侧缺失)"; continue }
        $p = $byName[$name]
        if ([string]$p.file -cne [string]$t.file) { $diff += "$name(file)" }
        elseif ([string]$p.checksum -cne [string]$t.checksum) { $diff += "$name(checksum)" }
        elseif ([string]$p.proto -cne [string]$t.proto) { $diff += "$name(proto)" }
        elseif ([string]$p.rows -cne [string]$t.rows) { $diff += "$name(rows)" }
        $byName.Remove($name)
    }
    foreach ($name in $byName.Keys) { $diff += "$name(本侧独有)" }
    return $diff
}

# 0. 前置依赖检查(审计 P1:grpcurl 检查原在切换 active 之后,明确无法发 RPC 时
# 磁盘已被改动;所有依赖必须在动盘之前验清)。
if ($ReloadAddr -ne "") {
    $grpcurl = Get-Command grpcurl -ErrorAction SilentlyContinue
    if ($null -eq $grpcurl) {
        Write-Host "[ERR] 已请求 -ReloadAddr 但未安装 grpcurl:发布未开始,磁盘未改动。请安装 grpcurl 后重跑,或去掉 -ReloadAddr 只做文件面发布并手动触发 reload。" -ForegroundColor Red
        exit 1
    }
}

# 1. dist 检查(含 source_rev 溯源门禁,审计 P1:生成器已拒 unknown/空白,但历史
# 产物可能带着坏值,发布链必须自己把关——不可追溯批次不得进入 active)。
$distManifest = Read-Manifest $DistDir
if ($null -eq $distManifest) { Write-Host "[ERR] $DistDir 缺少 manifest.json(先跑 go run ./tools/configtable-gen)" -ForegroundColor Red; exit 1 }
$newVersion = Assert-UInt64Field $distManifest.version 'version'
$srcRev = ([string]$distManifest.source_rev).Trim()
if ($srcRev -eq "" -or $srcRev -ieq "unknown") {
    Write-Host "[ERR] dist manifest source_rev='$srcRev' 不可追溯:用真实 -source-rev(如 svn-r123)重跑 configtable-gen 后再发布(同内容重跑会原地纠正 manifest,版本不变)。" -ForegroundColor Red
    exit 1
}
Write-Host "dist 批次 version = $newVersion($($distManifest.tables.Count) 张表,source_rev=$srcRev)"

# 2. dist → staging(先清后拷,staging 永远是完整一批)
New-Item -ItemType Directory -Force $ctRoot | Out-Null
if (Test-Path $staging) { Remove-Item -Recurse -Force $staging }
Copy-Item -Recurse $DistDir $staging

# 2.1 快照边界校验(R5 复审 P1-10):generator 是「逐表写入 → 最后换 manifest」,与本脚本
# 无共享锁;第 1 步读 manifest 与第 2 步递归复制之间 generator 可能正在重写 dist。
# 复制完成后把 staging manifest 与第 1 步读到的快照比对(version/source_rev/表语义),
# 不一致 = 撞上并发生成,发布决策($newVersion 日志、版本单调判断、reload expect_version)
# 已失去一致基准,fail-fast 拒绝,等 generator 结束后重跑。撕裂拷贝(manifest 与部分表
# 文件来自不同批次)则由下方第 3 步逐表 sha256 拦截。
$stagingManifest = Read-Manifest $staging
if ($null -eq $stagingManifest) { Write-Host "[ERR] staging 缺 manifest.json" -ForegroundColor Red; exit 1 }
$stagingVersion = Assert-UInt64Field $stagingManifest.version 'version(staging)'
$stagingSrcRev = ([string]$stagingManifest.source_rev).Trim()
$snapshotDiff = @(Compare-ManifestTables $distManifest.tables $stagingManifest.tables)
if ($stagingVersion -ne $newVersion -or $stagingSrcRev -cne $srcRev -or $snapshotDiff.Count -gt 0) {
    Write-Host "[ERR] dist 在读取与复制之间被并发改写(version $newVersion→$stagingVersion, source_rev '$srcRev'→'$stagingSrcRev', 表差异: $($snapshotDiff -join ', ')):configtable-gen 可能正在运行。等生成结束后重跑发布;同一 dist 目录不要与生成器并发使用。" -ForegroundColor Red
    Remove-Item -Recurse -Force $staging
    exit 1
}

# 3. staging 结构 + 逐表 sha256 校验(审计 P1:只验 checksum 不验结构,
# version=42/tables=[] 的非法 manifest 也能落 active,服务重启才 fail-fast)。
# 校验一律大小写敏感(R5 复审 P1-9):-ne/-notmatch 默认大小写不敏感,大写 checksum /
# 大写表名会被放行,而 Go 运行时精确比较 + 只认小写表名,重启即拒载。
if ($newVersion -le 0) { Write-Host "[ERR] manifest version=$newVersion 非法(必须 > 0)" -ForegroundColor Red; exit 1 }
if ($null -eq $stagingManifest.tables -or $stagingManifest.tables.Count -eq 0) {
    Write-Host "[ERR] manifest 表清单为空:空批次不可发布" -ForegroundColor Red
    exit 1
}
$seenNames = @{}
foreach ($t in $stagingManifest.tables) {
    $name = [string]$t.name
    if ($name -cnotmatch '^[a-z0-9_]+$') {
        Write-Host "[ERR] 非法表名 '$name'(只允许小写 [a-z0-9_])" -ForegroundColor Red
        exit 1
    }
    if ($seenNames.ContainsKey($name)) {
        Write-Host "[ERR] 表名重复 '$name'" -ForegroundColor Red
        exit 1
    }
    $seenNames[$name] = $true
    if ([string]$t.file -cne "$name.json") {
        Write-Host "[ERR] 表 '$name' 的 file='$($t.file)' 非法(必须为 <name>.json,防路径逃逸)" -ForegroundColor Red
        exit 1
    }
    $null = Assert-UInt64Field $t.rows "tables[$name].rows"
    $f = Join-Path $staging $t.file
    if (-not (Test-Path $f)) { Write-Host "[ERR] staging 缺文件 $($t.file)" -ForegroundColor Red; exit 1 }
    $got = "sha256:" + (Get-FileHash $f -Algorithm SHA256).Hash.ToLower()
    if ($got -cne [string]$t.checksum) {
        Write-Host "[ERR] $($t.file) checksum 不匹配(大小写敏感,Go 运行时同标准)`n  声明 $($t.checksum)`n  实际 $got" -ForegroundColor Red
        exit 1
    }
}
Write-Host "staging 校验通过($($stagingManifest.tables.Count) 张表)"

# 4. 版本单调检查
$prevSlot = $null
$sameVersion = $false
$activeManifest = Read-Manifest $active
# 残缺 active(目录在、manifest.json 缺失,R4 复审 P1-6):不得落入下方"active 缺失"
# 恢复分支——那条路最终 Move-Item $staging $active 会因目标目录已存在而把 staging
# 挪成 active\staging 嵌套子目录,脚本却退出 0 误报发布成功(根目录无 manifest,
# 服务端加载必失败)。该形态只能来自外部篡改/手工误删(归档与补位均为原子改名,
# 崩溃续跑不会留下无 manifest 的 active),fail-fast 留给人工核对,不自动清场。
if ((Test-Path $active) -and ($null -eq $activeManifest)) {
    Write-Host "[ERR] active 目录存在但 manifest.json 缺失:active 已残缺(非崩溃续跑形态)。人工核对内容后把整个 active 目录移走留证(如 history\corrupt-active),再重跑发布。" -ForegroundColor Red
    Remove-Item -Recurse -Force $staging
    exit 1
}
if ($null -ne $activeManifest) {
    $activeVersion = Assert-UInt64Field $activeManifest.version 'version(active)'
    if ($newVersion -eq $activeVersion) {
        # 同版本必须先证明**同批次**(审计 P1:版本相同但表 checksum 不同 = 生成纪律
        # 破坏或 active 被篡改,此前会静默"成功"而 active 内容与 dist 不一致)。
        # 比对**重算 active 磁盘实际 hash**,不信 active manifest 的自述(审计 R4 #13:
        # active 表文件被篡改/损坏而 manifest 未变时,声明比对静默放行,脚本声称
        # no-op 而 active 实际内容 ≠ dist)。staging 侧 hash 已在第 3 步重算验证。
        $mismatch = @()
        foreach ($t in $stagingManifest.tables) {
            $name = [string]$t.name
            $af = Join-Path $active ([string]$t.file)
            if (-not (Test-Path $af)) { $mismatch += $name; continue }
            $got = "sha256:" + (Get-FileHash $af -Algorithm SHA256).Hash.ToLower()
            if ($got -cne [string]$t.checksum) { $mismatch += $name }
        }
        # manifest 运行语义比对(R4 复审 P1-8):文件字节相同但 active manifest 声明的
        # proto/rows 与 dist 不同,同样不是同批次(no-op 路径会保留 active 旧 manifest,
        # 语义漂移被静默放行)。
        $mismatch += Compare-ManifestTables $activeManifest.tables $stagingManifest.tables
        if ($activeManifest.tables.Count -ne $stagingManifest.tables.Count -or $mismatch.Count -gt 0) {
            Write-Host "[ERR] dist 与 active 版本同为 $newVersion 但内容/语义不同(差异: $($mismatch -join ', '));同版本必须同批次——active 被篡改/损坏或版本号被复用:重新生成更高版本发布,并排查来源。" -ForegroundColor Red
            Remove-Item -Recurse -Force $staging
            exit 1
        }
        # 文件面 no-op,但**不能跳过 reload**(审计 P1):上次发布在 reload 前崩溃、
        # 服务重启后内存落后、或另一副本尚未加载时,active 同版本 ≠ 服务端已加载。
        # reload 幂等(服务端已是该版本时返回 code=OK + activeVersion),放心重发。
        # 同版本同批次但 source_rev 不同(生成器同内容原地纠正过溯源):把纠正后的
        # manifest 同步进 active(表内容 checksum 相同,只换 manifest)。
        $activeSrcRev = ([string]$activeManifest.source_rev).Trim()
        if ($activeSrcRev -cne $srcRev) {
            Copy-Item (Join-Path $staging "manifest.json") (Join-Path $active "manifest.json") -Force
            Write-Host "active manifest source_rev 已纠正:'$activeSrcRev' → '$srcRev'(版本与表内容不变)" -ForegroundColor Yellow
        }
        Write-Host "active 已是 version $activeVersion,文件面无需发布;继续确认服务端已加载" -ForegroundColor Yellow
        Remove-Item -Recurse -Force $staging
        $sameVersion = $true
    }
    if (-not $sameVersion) {
        if ($newVersion -lt $activeVersion) {
            Write-Host "[ERR] dist 版本 $newVersion 低于 active $activeVersion,拒绝回退(回滚请重新生成更高版本)" -ForegroundColor Red
            exit 1
        }
        # 5. 归档旧 active → history\v<版本>
        New-Item -ItemType Directory -Force $history | Out-Null
        $slot = Join-Path $history "v$activeVersion"
        if (Test-Path $slot) { Remove-Item -Recurse -Force $slot }
        Move-Item $active $slot
        Write-Host "旧批次 v$activeVersion 已归档 → $slot"
        $prevSlot = $slot   # reload 校验失败时用于回滚磁盘 active
    }
} else {
    # active 缺失(全新部署,或上次崩在"归档旧 active"与"staging 补位"之间):
    # 不能只信 dist 版本——history 里可能躺着更高版本(审计 P1:否则崩溃续跑可以把
    # 比 history 最高版更旧的 dist 装成 active,真实降级)。
    $histTop = [uint64]0
    if (Test-Path $history) {
        foreach ($d in Get-ChildItem -Directory $history) {
            if ($d.Name -match '^v(\d+)$') {
                $v = [uint64]$Matches[1]
                if ($v -gt $histTop) { $histTop = $v }
            }
        }
    }
    if ($histTop -gt 0 -and $newVersion -lt $histTop) {
        Write-Host "[ERR] active 缺失且 dist 版本 $newVersion 低于 history 最高版 v$histTop:拒绝降级装载。恢复 = 把 history\v$histTop 复制回 active,或用更高版本重新生成发布。" -ForegroundColor Red
        exit 1
    }
    if ($histTop -gt 0 -and $newVersion -eq $histTop) {
        # 同版本必须同批次(审计 R4 #13):active 缺失续跑的合法场景只有"同一批次补位"
        # (上次崩在归档与补位之间)。版本号被复用的**不同批次**不得借 active 缺失窗口
        # 装载成 active——与 history 槽位的磁盘实际 hash 比对(不信槽内 manifest 自述)。
        $slotDir = Join-Path $history "v$histTop"
        $mismatch = @()
        foreach ($t in $stagingManifest.tables) {
            $hf = Join-Path $slotDir ([string]$t.file)
            if (-not (Test-Path $hf)) { $mismatch += [string]$t.name; continue }
            $got = "sha256:" + (Get-FileHash $hf -Algorithm SHA256).Hash.ToLower()
            if ($got -cne [string]$t.checksum) { $mismatch += [string]$t.name }
        }
        # manifest 运行语义比对(R4 复审 P1-8):槽位 manifest 与 dist 的 proto/rows 声明
        # 必须一致,同字节文件不同语义同样拒绝借"active 缺失"窗口装载。
        $slotManifest = Read-Manifest $slotDir
        if ($null -eq $slotManifest) { $mismatch += 'manifest(槽位缺失)' }
        else { $mismatch += Compare-ManifestTables $slotManifest.tables $stagingManifest.tables }
        $histJson = @(Get-ChildItem -File $slotDir -Filter '*.json' -ErrorAction SilentlyContinue |
            Where-Object Name -ne 'manifest.json')
        if ($mismatch.Count -gt 0 -or $histJson.Count -ne $stagingManifest.tables.Count) {
            Write-Host "[ERR] active 缺失且 dist 版本 $newVersion 等于 history 最高版 v$histTop,但内容/语义与该槽位不同(差异: $($mismatch -join ', '));同版本必须同批次——版本号复用的不同批次拒绝装载,用更高版本重新生成发布。" -ForegroundColor Red
            Remove-Item -Recurse -Force $staging
            exit 1
        }
        Write-Host "active 缺失恢复:dist 与 history v$histTop 磁盘实际内容一致(同批次补位),继续装载" -ForegroundColor Yellow
    }
}

if (-not $sameVersion) {
    # 6. staging → active(同卷改名,近原子;两步间崩溃重跑本脚本即恢复)。
    # 目标必须不存在(R4 复审 P1-6):Move-Item 到已存在目录会生成 active\staging 嵌套,
    # 上方已对"残缺 active"fail-fast,此处为最后一道内部不变量护栏。
    if (Test-Path $active) {
        Write-Host "[ERR] 内部不变量被破坏:切换前 active 目录仍存在(应已归档或缺失),拒绝生成嵌套目录;staging 保留于 $staging 供排查。" -ForegroundColor Red
        exit 1
    }
    Move-Item $staging $active
    Write-Host "发布完成:active = version $newVersion" -ForegroundColor Green
}

# 回滚磁盘 active:reload 被服务端拒绝(校验失败)时,坏批次不能留在 active——
# 服务端内存虽保留旧表,但下次进程重启会 fail-closed 加载失败(启动强依赖)。
# 坏批次移到 history\failed-v<版本> 留证,再恢复旧批次。
# 恢复目标(R4 复审 P1-7):**优先精确恢复服务端上报的 activeVersion**——服务内存 v7、
# 磁盘曾发过 v9 时,恢复 v9 仍是"内存 v7/磁盘 v9"劈叉(下次重启静默换表);只有磁盘
# 收敛到 v<服务端版本> 才与运行态一致。该槽位缺失时才退回:本次归档的 $prevSlot →
# history 最高且 < 候选版本的 v* 槽位,并明示磁盘与服务端内存仍劈叉、需人工收敛。
function Restore-PreviousActive([string]$Reason, [uint64]$ServiceActiveVersion = 0) {
    $failedSlot = Join-Path $history "failed-v$newVersion"
    New-Item -ItemType Directory -Force $history | Out-Null
    if (Test-Path $failedSlot) { Remove-Item -Recurse -Force $failedSlot }
    Move-Item $active $failedSlot

    $restoreSlot = $null
    if ($ServiceActiveVersion -gt 0) {
        $exact = Join-Path $history "v$ServiceActiveVersion"
        if (Test-Path $exact) {
            $restoreSlot = $exact
        } else {
            Write-Host "[WARN] history 缺服务端当前版本 v$ServiceActiveVersion 的槽位,无法精确恢复;退回可用旧批次后磁盘与服务端内存仍劈叉,需人工收敛(用服务端版本重发或生成更高版本)。" -ForegroundColor Yellow
        }
    }
    if ($null -eq $restoreSlot -and $null -ne $prevSlot -and (Test-Path $prevSlot)) {
        $restoreSlot = $prevSlot
    }
    if ($null -eq $restoreSlot) {
        $best = [uint64]0
        if (Test-Path $history) {
            foreach ($d in Get-ChildItem -Directory $history) {
                if ($d.Name -match '^v(\d+)$') {
                    $v = [uint64]$Matches[1]
                    if ($v -lt $newVersion -and $v -gt $best) {
                        $best = $v
                        $restoreSlot = $d.FullName
                    }
                }
            }
        }
    }
    if ($null -ne $restoreSlot -and (Test-Path $restoreSlot)) {
        # 回滚安装(R5 复审 P2-9):不直接递归复制进 active——槽位可能已损坏(manifest 缺失/
        # 表被篡改),复制中崩溃也会留下残缺 active(下次发布按"残缺 active"fail-fast,
        # 服务重启拒载)。改为:复制到临时目录 → 按槽位 manifest 逐表 sha256 复验 → 原子
        # 改名补位;复验失败不安装,active 保持缺失并明示人工恢复路径。
        $restoreStaging = Join-Path $ctRoot "restore-staging"
        if (Test-Path $restoreStaging) { Remove-Item -Recurse -Force $restoreStaging }
        Copy-Item -Recurse $restoreSlot $restoreStaging
        $slotName = Split-Path -Leaf $restoreSlot
        $restoreManifest = Read-Manifest $restoreStaging
        $badTables = @()
        if ($null -eq $restoreManifest -or $null -eq $restoreManifest.tables -or $restoreManifest.tables.Count -eq 0) {
            $badTables += 'manifest(缺失/空表清单)'
        } else {
            foreach ($t in $restoreManifest.tables) {
                $rf = Join-Path $restoreStaging ([string]$t.file)
                if (-not (Test-Path $rf)) { $badTables += [string]$t.name; continue }
                $got = "sha256:" + (Get-FileHash $rf -Algorithm SHA256).Hash.ToLower()
                if ($got -cne [string]$t.checksum) { $badTables += [string]$t.name }
            }
        }
        if ($badTables.Count -gt 0) {
            Remove-Item -Recurse -Force $restoreStaging
            Write-Host "[ERR] $Reason;回滚槽位 $slotName 复验失败(差异: $($badTables -join ', ')),拒绝把损坏批次装成 active。active 保持缺失,坏批次留证 → $failedSlot;请人工用完好槽位恢复或重新生成发布。" -ForegroundColor Red
            return
        }
        Move-Item $restoreStaging $active
        Write-Host "[ERR] $Reason;磁盘 active 已回滚到旧批次($slotName,已逐表复验),坏批次留证 → $failedSlot" -ForegroundColor Red
    } else {
        Write-Host "[ERR] $Reason;history 无可用旧批次,active 已移除,坏批次留证 → $failedSlot" -ForegroundColor Red
    }
}

# 7. 触发热更(可选;也可由运维手动调 ReloadConfigTable)。
# grpcurl 退出码 0 只代表 RPC 传输成功;服务端校验失败通过 payload 的 code/detail 返回
# (加载失败保留旧表,hotreload doc §6),必须解析响应判定,否则坏批次留在磁盘 active
# 且脚本误报成功(审计 P1)。
if ($ReloadAddr -ne "") {
    $grpcurl = Get-Command grpcurl -ErrorAction SilentlyContinue
    if ($null -eq $grpcurl) {
        # 要求触发 reload 但无法执行 = 发布未完成,必须非 0 退出(审计:退出 0 会被
        # 上层当成"已发布已加载",服务端实际从未加载新批次)。
        Write-Host "[ERR] 已请求 -ReloadAddr 但未安装 grpcurl,reload 未触发;请安装 grpcurl 或手动触发并核对响应 code=OK 且 activeVersion=${newVersion}:" -ForegroundColor Red
        Write-Host "  grpcurl -plaintext -d '{\"expect_version\": $newVersion}' $ReloadAddr pandora.config.v1.ConfigTableAdminService/ReloadConfigTable"
        exit 1
    }
    # -max-time:reload 内含全表加载校验,给足预算但必须有界(挂死的调用会让发布卡住)。
    $respRaw = & grpcurl -plaintext -max-time 30 -d "{`"expect_version`": $newVersion}" $ReloadAddr pandora.config.v1.ConfigTableAdminService/ReloadConfigTable 2>&1 | Out-String
    if ($LASTEXITCODE -ne 0) {
        # 传输失败(服务不可达 / 超时):磁盘 active 保持新批次(服务端内存未动或未知),
        # 人工重试 reload;不回滚磁盘——回滚只对"服务端明确拒绝"成立。
        Write-Host "[ERR] reload RPC 调用失败(exit=$LASTEXITCODE):`n$respRaw" -ForegroundColor Red
        Write-Host "  磁盘 active 已是 v$newVersion,服务端加载状态未知;请排查后手动触发:" -ForegroundColor Yellow
        Write-Host "  grpcurl -plaintext -d '{\"expect_version\": $newVersion}' $ReloadAddr pandora.config.v1.ConfigTableAdminService/ReloadConfigTable"
        exit 1
    }
    try { $resp = $respRaw | ConvertFrom-Json }
    catch {
        # UNKNOWN ≠ 拒绝(审计 P1):RPC 已送达,reload 可能已经成功,只是响应解析不了。
        # 此时回滚磁盘会造成"内存新版本、磁盘旧版本"劈叉(下次重启加载旧表,静默降级)。
        # 保持磁盘 active = 新批次,非 0 退出,人工重查服务端 activeVersion 收敛。
        Write-Host "[ERR] reload 响应无法解析(UNKNOWN,不回滚磁盘):$respRaw" -ForegroundColor Red
        Write-Host "  磁盘 active 保持 v$newVersion;请人工核对服务端 activeVersion:" -ForegroundColor Yellow
        Write-Host "  grpcurl -plaintext -d '{\"expect_version\": $newVersion}' $ReloadAddr pandora.config.v1.ConfigTableAdminService/ReloadConfigTable"
        exit 1
    }
    # proto3 JSON:code=OK(0)/reloaded=false 等零值字段会被省略。code 缺省 = OK。
    $codeText = if ($null -ne $resp.code) { [string]$resp.code } else { 'OK' }
    $gotVersion = if ($null -ne $resp.activeVersion) { [uint64]$resp.activeVersion } else { [uint64]0 }
    if ($codeText -cne 'OK' -or $gotVersion -ne $newVersion) {
        # 回滚分类(审计 P1:所有非 OK 一律回滚会把磁盘错误回退——服务端已领先候选
        # 版本时回滚是真实降级;鉴权/协议错误也不是"坏批次"):
        #   a) 服务端已领先(gotVersion > newVersion):别的发布已上更高版本,磁盘不动,
        #      指引用更高版本重新生成;
        #   b) 服务端仍在旧版本且明确拒绝候选(code!=OK 且 0<gotVersion<newVersion):
        #      坏批次,回滚磁盘(移 failed-v 留证 + 从 prevSlot/history 恢复);
        #   c) 其它(gotVersion=0 = 鉴权/协议/未知形态):磁盘不动,人工排查后重试 reload。
        if ($gotVersion -gt $newVersion) {
            Write-Host "[ERR] 服务端已在更高版本 v$gotVersion(候选 v$newVersion 过旧):磁盘不回滚;请基于最新源表生成更高版本重新发布。" -ForegroundColor Red
        } elseif ($codeText -cne 'OK' -and $gotVersion -gt 0 -and $gotVersion -lt $newVersion) {
            Restore-PreviousActive -Reason "服务端拒绝批次 v$newVersion(code=$codeText activeVersion=$gotVersion detail=$($resp.detail);sameVersion=$sameVersion);服务端内存保留旧表" -ServiceActiveVersion $gotVersion
        } else {
            Write-Host "[ERR] reload 未确认(code=$codeText activeVersion=$gotVersion detail=$($resp.detail)):形态不像'坏批次被拒'(可能鉴权/协议问题),磁盘保持 v$newVersion 不回滚;排查后手动重试:" -ForegroundColor Red
            Write-Host "  grpcurl -plaintext -d '{\"expect_version\": $newVersion}' $ReloadAddr pandora.config.v1.ConfigTableAdminService/ReloadConfigTable"
        }
        exit 1
    }
    Write-Host "reload 生效:服务端 activeVersion=$gotVersion(detail=$($resp.detail))" -ForegroundColor Green
    exit 0
}

if ($sameVersion) {
    Write-Host "[WARN] 文件面同版本 no-op 且未指定 -ReloadAddr:无法确认服务端已加载 v$newVersion(崩溃续跑 / 服务重启场景请带 -ReloadAddr 重跑)" -ForegroundColor Yellow
}
