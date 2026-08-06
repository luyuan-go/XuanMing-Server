<#
.SYNOPSIS
  策划一键导表:把客户端 SVN 的策划 xlsx 生成为服务端配置表(configtable/dist + manifest.json)。

.DESCRIPTION
  包装 tools/configtable-gen,把三件策划做不了的事替掉:
    1. 自动定位客户端 Table 根目录(不用记路径);
    2. 自动从 SVN 取版本号填 -source-rev(生成器必填且拒 unknown/空白);
    3. 没装 Go 也能跑(优先用 run/artifacts/windows/bin/configtable-gen.exe)。
  并把生成器面向程序的报错翻译成"该找谁、该改什么"。

  产物是**整批**的:configtable/dist 下全部 json + manifest.json 必须一起提交,
  只提交其中一个会让服务端 Store.Load 校验失败、服务起不来。

.PARAMETER TableRoot
  客户端策划表根目录。留空按顺序自动探测:
    -TableRoot 参数 → $env:PANDORA_CLIENT_TABLE_ROOT → <服务端仓库>/../Pandora-Client-SVN/Table
    → F:\work\Pandora-Client-SVN\Table

.PARAMETER SourceRev
  产物溯源标注。留空则从 SVN 读 Table 目录的最后改动版本,填成 svn-r<N>。

.PARAMETER Check
  只探测环境并报告将要用的参数,不真的生成。

.EXAMPLE
  pwsh tools/scripts/configtable_gen.ps1
  pwsh tools/scripts/configtable_gen.ps1 -Check
  pwsh tools/scripts/configtable_gen.ps1 -TableRoot D:\work\Pandora-Client-SVN\Table
#>
param(
    [string]$TableRoot = '',
    [string]$SourceRev = '',
    [switch]$Check
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# 控制台默认按本地代码页解码子进程输出,中文表名/报错会变乱码;固定 UTF-8(与 start.ps1 同款处理)。
try { [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false) } catch { }

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = [System.IO.Path]::GetFullPath((Join-Path $ScriptDir '..\..'))
$DistDir = Join-Path $ProjectRoot 'configtable\dist'

function Write-Ok([string]$m) { Write-Host "[OK]  $m" -ForegroundColor Green }
function Write-Info([string]$m) { Write-Host "[..]  $m" -ForegroundColor Cyan }
function Write-Warn2([string]$m) { Write-Host "[!!]  $m" -ForegroundColor Yellow }
function Write-Err([string]$m) { Write-Host "[ERR] $m" -ForegroundColor Red }

# ---------------------------------------------------------------------------
# 1. 定位客户端策划表根目录
# ---------------------------------------------------------------------------
function Resolve-TableRoot([string]$Explicit) {
    $candidates = New-Object System.Collections.Generic.List[string]
    if ($Explicit) { $candidates.Add($Explicit) }
    if ($env:PANDORA_CLIENT_TABLE_ROOT) { $candidates.Add($env:PANDORA_CLIENT_TABLE_ROOT) }
    $candidates.Add((Join-Path $ProjectRoot '..\Pandora-Client-SVN\Table'))
    $candidates.Add('F:\work\Pandora-Client-SVN\Table')

    foreach ($c in $candidates) {
        if ([string]::IsNullOrWhiteSpace($c)) { continue }
        try { $full = [System.IO.Path]::GetFullPath($c) } catch { continue }
        if (-not (Test-Path -LiteralPath $full -PathType Container)) { continue }
        # 目录存在还不够:指错到某个空目录会让生成器报一堆"读 xlsx 失败",
        # 不如在这里就确认它确实是策划表根(至少有一个 xlsx)。
        $anyXlsx = Get-ChildItem -LiteralPath $full -Filter '*.xlsx' -Recurse -File -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if ($null -eq $anyXlsx) { continue }
        return $full
    }
    return ''
}

$TableRoot = Resolve-TableRoot $TableRoot
if (-not $TableRoot) {
    Write-Err '找不到客户端策划表根目录(Table)。'
    Write-Host '      按顺序找过:-TableRoot 参数 / 环境变量 PANDORA_CLIENT_TABLE_ROOT /'
    Write-Host '      <服务端仓库>\..\Pandora-Client-SVN\Table / F:\work\Pandora-Client-SVN\Table'
    Write-Host '      解决:显式指定,例如'
    Write-Host '        pwsh tools\scripts\configtable_gen.ps1 -TableRoot D:\work\Pandora-Client-SVN\Table'
    exit 1
}
Write-Ok "策划表目录: $TableRoot"

# ---------------------------------------------------------------------------
# 2. 取 SVN 版本号当 -source-rev(生成器必填,且拒 unknown / 空白)
# ---------------------------------------------------------------------------
function Resolve-SourceRev([string]$Explicit, [string]$Root) {
    if (-not [string]::IsNullOrWhiteSpace($Explicit)) { return $Explicit.Trim() }
    $svn = Get-Command svn -ErrorAction SilentlyContinue
    if ($null -eq $svn) { return '' }
    # Table 根目录是纯 ASCII 路径,svn.exe 能吃;不要往下传中文子目录(会 E155010)。
    $rev = (& svn info --show-item last-changed-revision $Root 2>$null | Out-String).Trim()
    if ($rev -notmatch '^\d+$') { return '' }
    return "svn-r$rev"
}

$SourceRev = Resolve-SourceRev $SourceRev $TableRoot
if (-not $SourceRev) {
    Write-Err '取不到 SVN 版本号,无法填写 -source-rev(生成器拒绝不可追溯的批次)。'
    Write-Host '      要么装好 svn 命令行并保证 Table 是 SVN 工作副本,'
    Write-Host '      要么手动指定,例如:-SourceRev svn-r1774'
    exit 1
}
Write-Ok "源表版本: $SourceRev"

# ---------------------------------------------------------------------------
# 3. 选生成器:优先预编译 exe(策划机通常没装 Go),否则回退 go run
# ---------------------------------------------------------------------------
$GenExe = Join-Path $ProjectRoot 'run\artifacts\windows\bin\configtable-gen.exe'
$UseExe = Test-Path -LiteralPath $GenExe -PathType Leaf
$GoCmd = Get-Command go -ErrorAction SilentlyContinue
if (-not $UseExe -and $null -eq $GoCmd) {
    Write-Err '既没有预编译的 configtable-gen.exe,本机也没装 Go,无法导表。'
    Write-Host "      预期位置: $GenExe"
    Write-Host '      找程序出一份:'
    Write-Host '        go build -o run\artifacts\windows\bin\configtable-gen.exe .\tools\configtable-gen'
    exit 1
}
Write-Ok $(if ($UseExe) { "生成器: 预编译 exe($GenExe)" } else { "生成器: go run ./tools/configtable-gen(本机有 Go)" })

if ($Check) {
    Write-Info '-Check 模式:环境探测通过,未执行生成。'
    exit 0
}

# ---------------------------------------------------------------------------
# 4. 记录旧批次行数,生成后做增减对比
# ---------------------------------------------------------------------------
function Read-ManifestRows([string]$Dir) {
    $p = Join-Path $Dir 'manifest.json'
    if (-not (Test-Path -LiteralPath $p -PathType Leaf)) { return $null }
    try {
        $m = [System.Text.UTF8Encoding]::new($false).GetString([System.IO.File]::ReadAllBytes($p)) | ConvertFrom-Json
    } catch { return $null }
    $map = @{}
    foreach ($t in $m.tables) { $map[[string]$t.name] = [int]$t.rows }
    return [pscustomobject]@{ Version = [uint64]$m.version; Rows = $map }
}
$before = Read-ManifestRows $DistDir

# ---------------------------------------------------------------------------
# 5. 跑生成器
# ---------------------------------------------------------------------------
Write-Host ''
Write-Info '开始导表(全批原子:任一张表校验不过就整批不产出,旧表保持不变)...'
Write-Host ''

Push-Location $ProjectRoot
try {
    $genArgs = @('-tables', $TableRoot, '-source-rev', $SourceRev)
    if ($UseExe) {
        $out = & $GenExe @genArgs 2>&1 | ForEach-Object { $_.ToString() }
    } else {
        $out = & go run ./tools/configtable-gen @genArgs 2>&1 | ForEach-Object { $_.ToString() }
    }
    $genExit = $LASTEXITCODE
} finally {
    Pop-Location
}

$out | ForEach-Object { Write-Host $_ }
$outText = ($out -join "`n")

# ---------------------------------------------------------------------------
# 6. 失败:把面向程序的报错翻译成"找谁、改什么"
# ---------------------------------------------------------------------------
if ($genExit -ne 0) {
    Write-Host ''
    Write-Err "导表失败(退出码 $genExit),服务端配置表**没有任何改动**,原来的表还能用。"
    Write-Host ''
    if ($outText -match '未登记的第\s*(\S+)\s*列\s*"([^"]*)"') {
        $col = $Matches[1]; $name = $Matches[2]
        Write-Warn2 "原因:策划在表里新增了第 $col 列「$name」,但服务端还不认识这一列。"
        Write-Host '      这**不是策划能修的**:需要程序在 proto/pandora/config/v1/<表>.proto 里'
        Write-Host '      加对应字段 + (excel_col) 注解,重生 pb 后才能导。'
        Write-Host '      做法:把上面那行报错原文发给程序。临时想继续导表,可先把该列从 xlsx 撤掉。'
    } elseif ($outText -match '外键校验失败') {
        Write-Warn2 '原因:有表引用了另一张表里不存在的 id(比如掉落表引用了已删除的道具 id)。'
        Write-Host '      看上面报错里点名的「表名 / 主键 / 字段」,把那一行的引用改成存在的 id,或把被引用的行加回来。'
    } elseif ($outText -match '位序|bit_index|bitindex') {
        Write-Warn2 '原因:位序状态(configtable/bitindex_state)对不上。这个必须找程序,'
        Write-Host '      **不要**自己删或改 bitindex_state 下的文件——那会让已存档的玩家进度位图整体错位。'
    } elseif ($outText -match '读\s+.*失败|xlsx') {
        Write-Warn2 '原因:某张 xlsx 读不出来。常见是文件正被 Excel 打开、或 SVN 更新到一半。'
        Write-Host '      做法:关掉 Excel,svn update 后重试。'
    } else {
        Write-Warn2 '未识别的错误,把上面整段输出发给程序。'
    }
    exit $genExit
}

# ---------------------------------------------------------------------------
# 7. 成功:报告行数增减
# ---------------------------------------------------------------------------
$after = Read-ManifestRows $DistDir
if ($null -eq $after) {
    Write-Err "导表报成功,但 $DistDir 下读不到 manifest.json,请找程序看。"
    exit 1
}

Write-Host ''
if ($null -ne $before -and $before.Version -eq $after.Version) {
    Write-Ok "内容与上一批完全相同,批次号未变(v$($after.Version)),没有需要提交的改动。"
} else {
    $oldV = if ($null -eq $before) { '(无)' } else { "v$($before.Version)" }
    Write-Ok "导表成功:批次 $oldV -> v$($after.Version)"
    if ($null -ne $before) {
        $shrunk = New-Object System.Collections.Generic.List[string]
        foreach ($name in ($after.Rows.Keys | Sort-Object)) {
            $newRows = $after.Rows[$name]
            $oldRows = if ($before.Rows.ContainsKey($name)) { $before.Rows[$name] } else { $null }
            if ($null -eq $oldRows) {
                Write-Host ("      + {0,-20} 新表 {1} 行" -f $name, $newRows) -ForegroundColor Green
            } elseif ($newRows -ne $oldRows) {
                $delta = $newRows - $oldRows
                $sign = if ($delta -gt 0) { '+' } else { '' }
                $color = if ($delta -lt 0) { 'Yellow' } else { 'Green' }
                Write-Host ("        {0,-20} {1} -> {2} 行 ({3}{4})" -f $name, $oldRows, $newRows, $sign, $delta) -ForegroundColor $color
                if ($delta -lt 0) { $shrunk.Add($name) }
            }
        }
        if ($shrunk.Count -gt 0) {
            Write-Host ''
            # 刻意只提醒不拦截:2026-08-05 道具表 100→65 就是"占位表换真表",行数变少完全正当。
            # 行数是信号不是判据,真正要看的是内容。
            Write-Warn2 ("这些表行数变少了:{0}" -f ($shrunk -join ', '))
            Write-Host '      行数变少不一定是错(删占位、删废弃行都正常),但请顺手确认一下是有意的:'
            Write-Host '      如果是 SVN 更新时把别人的改动覆盖掉了,现在导出去就会固化到服务端。'
        }
    }
}

Write-Host ''
Write-Info '产物在: ' -NoNewline; Write-Host $DistDir
Write-Host '      这批文件必须**整批一起**提交(dist 下全部 json + manifest.json),'
Write-Host '      只提交一部分会让服务端启动时校验失败。'
Write-Host '      本机重启后端即可生效;要让其他人也拿到,需要把服务端仓库这批改动提交上去。'
Write-Host ''
exit 0
