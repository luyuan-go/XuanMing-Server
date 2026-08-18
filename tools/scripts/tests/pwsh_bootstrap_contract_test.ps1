<#
.SYNOPSIS
  「没装 PowerShell 7 的机器也能双击免 Docker 一键入口」的契约测试。

.DESCRIPTION
  背景:免 Docker 那三个入口是给策划机用的,而策划机以前必须先自己装 PowerShell 7 ——
  入口里只有一段 `where pwsh` + 报错退出。2026-08-18 改成由 tools\scripts\bootstrap_pwsh.cmd
  在 cmd.exe 里自举一份官方免安装 pwsh(不装机、不要 UAC、卸载 = 删目录)。

  这里守的是四类会**静默**坏掉、坏了又只在别人机器上才发现的东西:

  ① 供应链:自举包会被直接执行,而取包路径有三条(本机 cache / 任意可写共享盘 / 公网),
     后两条完全不受 HTTPS 保护。所以 sha256 必须钉死、必须真的拦得住 —— [4] 拿一个假包
     真跑一遍 bootstrap_pwsh.cmd,断言它拒绝并且**没有**解包。
  ② 单一实现:版本 / 校验和只允许写在 lib\pwsh_bootstrap.pin 一处。抄第二份必然漂,
     漂了就是「本机自举出 7.6.5、共享盘上备的是 7.6.4」这种查半天的问题。
  ③ ASCII-only:.cmd 入口里出现任何非 ASCII 字节,cmd.exe 会按当前代码页重读文件、
     算错偏移,然后去执行注释行的碎片(2026-08-06 现场)。这条铁律此前没有任何测试。
  ④ 接线:三个免 Docker 入口都得真的接上自举,并且用引号包住解释器路径 —— 自举出来的
     是全路径,仓库放在带空格的目录里就会炸。

.EXAMPLE
  pwsh tools/scripts/tests/pwsh_bootstrap_contract_test.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ScriptsDir = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path

$script:Failures = @()
function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { $script:Failures += $Message; Write-Host "  [FAIL] $Message" -ForegroundColor Red }
    else { Write-Host "  [ ok ] $Message" -ForegroundColor DarkGray }
}

$BootstrapCmd = Join-Path $ScriptsDir 'bootstrap_pwsh.cmd'
$PinFile = Join-Path $ScriptsDir 'lib/pwsh_bootstrap.pin'

# 免 Docker 三入口 = 本次自举的服务对象。其余入口(k8s / 出包 / 导表)面向程序,
# 保持原来的「没 pwsh 就明确报错」,不在本契约范围内。
$NoDockerEntries = @(
    '策划一键启动-免Docker-测试版.cmd'
    '策划一键停止-免Docker-测试版.cmd'
    '策划一键重启DS-免Docker-测试版.cmd'
)

# ── [1] pin 文件本身 ────────────────────────────────────────────────────────
Write-Host '[1] 版本 pin' -ForegroundColor Cyan
Assert-True (Test-Path -LiteralPath $PinFile) 'lib/pwsh_bootstrap.pin 存在'
$pin = @{}
if (Test-Path -LiteralPath $PinFile) {
    foreach ($line in [System.IO.File]::ReadAllLines($PinFile)) {
        $t = $line.Trim()
        if (-not $t -or $t.StartsWith('#')) { continue }
        $i = $t.IndexOf('=')
        if ($i -ge 1) { $pin[$t.Substring(0, $i).Trim()] = $t.Substring($i + 1).Trim() }
    }
}
foreach ($k in 'PWSH_VERSION', 'PWSH_FILE', 'PWSH_SHA256', 'PWSH_URL') {
    Assert-True ([bool]$pin[$k]) "pin 里有 $k"
}
Assert-True ($pin.PWSH_SHA256 -match '^[0-9a-f]{64}$') 'PWSH_SHA256 是 64 位小写十六进制'
# 版本号必须同时出现在文件名和 URL 里:换版时只改了一处(最典型的是改了 URL 忘了改 sha)
# 会在这里当场撞红,而不是等某台机器下到对不上号的包。
Assert-True ($pin.PWSH_FILE -like "*$($pin.PWSH_VERSION)*") 'PWSH_FILE 里含 PWSH_VERSION'
Assert-True ($pin.PWSH_URL -like "*$($pin.PWSH_VERSION)*") 'PWSH_URL 里含 PWSH_VERSION'
Assert-True ($pin.PWSH_URL.EndsWith('/' + $pin.PWSH_FILE)) 'PWSH_URL 结尾就是 PWSH_FILE(URL 与文件名不许各说各话)'
# 免安装 zip,不是 msi:msi 是按机器安装,要本地管理员 + UAC,策划机常常没有,
# 而且会把 Web 管理台的无人值守运行卡在 UAC 弹窗上。
Assert-True ($pin.PWSH_FILE -like '*.zip') 'pin 的是免安装 zip(不是需要管理员 + UAC 的 msi)'

# ── [2] 唯一实现:版本 / 校验和不许在别处再抄一份 ────────────────────────────
Write-Host '[2] 单一事实来源' -ForegroundColor Cyan
$sha = $pin.PWSH_SHA256
$dupes = @()
if ($sha) {
    $scan = @(Get-ChildItem -LiteralPath $ScriptsDir -Recurse -File -Include '*.ps1', '*.cmd' -ErrorAction SilentlyContinue) +
            @(Get-ChildItem -LiteralPath $ProjectRoot -File -Filter '*.cmd' -ErrorAction SilentlyContinue)
    $dupes = @($scan | Where-Object { $_.FullName -ne $PinFile -and $_.FullName -ne $PSCommandPath } |
        Where-Object { [System.IO.File]::ReadAllText($_.FullName) -match [regex]::Escape($sha) })
}
Assert-True ($dupes.Count -eq 0) ('sha256 只写在 pin 里' + $(if ($dupes.Count) { ',还出现在:' + (($dupes.Name) -join ', ') }))

# local_infra.ps1 必须是**读** pin,而不是自己抄一份常量。
$infra = [System.IO.File]::ReadAllText((Join-Path $ScriptsDir 'local_infra.ps1'))
Assert-True ($infra -match 'pwsh_bootstrap\.pin') 'local_infra.ps1 读 pin 文件'
Assert-True ($infra -match 'Save-PwshBootstrapArchive') 'local_infra.ps1 有 Save-PwshBootstrapArchive'
# 只在 provision 备料:能跑到 up 的机器必然已经有 pwsh,再下 100MB 是白下。
Assert-True ($infra -match "'provision'\s*\{[^}]*Save-PwshBootstrapArchive") 'provision 动作才备 pwsh 包'
Assert-True ($infra -notmatch "'up'\s*\{[^}]*Save-PwshBootstrapArchive") 'up 动作不备 pwsh 包'

# ── [3] ASCII-only 铁律(2026-08-06:非 ASCII 字节会让 cmd 执行注释行碎片)────
Write-Host '[3] 入口 .cmd 必须是纯 ASCII' -ForegroundColor Cyan
$asciiTargets = @($BootstrapCmd) + ($NoDockerEntries | ForEach-Object { Join-Path $ProjectRoot $_ })
foreach ($f in $asciiTargets) {
    $name = Split-Path -Leaf $f
    if (-not (Test-Path -LiteralPath $f)) { Assert-True $false "$name 存在"; continue }
    $bytes = [System.IO.File]::ReadAllBytes($f)
    $bad = @($bytes | Where-Object { $_ -ge 0x80 }).Count
    Assert-True ($bad -eq 0) "$name 无非 ASCII 字节(发现 $bad)"
    Assert-True ($bytes.Length -lt 3 -or -not ($bytes[0] -eq 0xEF -and $bytes[1] -eq 0xBB -and $bytes[2] -eq 0xBF)) "$name 无 BOM"
    # chcp 会换掉控制台代码页,正是那个 bug 的另一半触发条件。
    Assert-True ([System.IO.File]::ReadAllText($f) -notmatch '(?im)^\s*chcp\b') "$name 不含 chcp"
}
# pin 文件也归 cmd.exe 的 for /f 读,同样只能是 ASCII。
if (Test-Path -LiteralPath $PinFile) {
    $pinBytes = [System.IO.File]::ReadAllBytes($PinFile)
    Assert-True (@($pinBytes | Where-Object { $_ -ge 0x80 }).Count -eq 0) 'pwsh_bootstrap.pin 无非 ASCII 字节'
}

# ── [4] 三个免 Docker 入口真的接上了自举 ────────────────────────────────────
Write-Host '[4] 入口接线' -ForegroundColor Cyan
foreach ($name in $NoDockerEntries) {
    $path = Join-Path $ProjectRoot $name
    if (-not (Test-Path -LiteralPath $path)) { Assert-True $false "$name 存在"; continue }
    $text = [System.IO.File]::ReadAllText($path)
    Assert-True ($text -match 'call "%~dp0tools\\scripts\\bootstrap_pwsh\.cmd"') "$name 调用 bootstrap_pwsh.cmd"
    # 不许再留自己那份 where pwsh 判定:留着就会和自举的结论打架(自举成功了它还报缺)。
    Assert-True ($text -notmatch '(?m)^where pwsh') "$name 不再内联 where pwsh 判定"
    Assert-True ($text -match 'PANDORA_PWSH') "$name 用 bootstrap 给出的 PANDORA_PWSH"
    # 自举出来的是全路径,仓库放在带空格的目录下不加引号必炸。
    Assert-True ($text -match '"%PS%" -NoProfile') "$name 用引号包住解释器路径"
    Assert-True ($text -notmatch '(?m)^\s*%PS% ') "$name 没有裸 %PS% 调用"
}

# ── [5] 供应链闸门:假包必须被拒,且绝不解包 ────────────────────────────────
# 在临时目录里真跑一遍 bootstrap_pwsh.cmd:PATH 收窄到系统目录(制造「本机没有 pwsh」),
# PANDORA_LOCALINFRA_MIRROR 指向一个放了同名假包的目录。这条路径是整套机制里唯一
# 「错了会真出事」的地方,所以必须真跑,不能只做静态断言。
Write-Host '[5] sha256 闸门(假包真跑一遍)' -ForegroundColor Cyan
$tmpRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('pandora-pwshboot-' + [System.IO.Path]::GetRandomFileName())
try {
    $fakeScripts = Join-Path $tmpRoot 'tools/scripts'
    New-Item -ItemType Directory -Force -Path (Join-Path $fakeScripts 'lib') | Out-Null
    Copy-Item $BootstrapCmd (Join-Path $fakeScripts 'bootstrap_pwsh.cmd')
    Copy-Item $PinFile (Join-Path $fakeScripts 'lib/pwsh_bootstrap.pin')

    $mirror = Join-Path $tmpRoot 'mirror'
    New-Item -ItemType Directory -Force -Path $mirror | Out-Null
    [System.IO.File]::WriteAllText((Join-Path $mirror $pin.PWSH_FILE), 'not a powershell release')

    $sysRoot = [Environment]::GetFolderPath('Windows')
    $minimalPath = "$sysRoot\system32;$sysRoot"
    $runner = Join-Path $tmpRoot 'run.cmd'
    [System.IO.File]::WriteAllText($runner, (@"
@echo off
setlocal
set "PATH=$minimalPath"
set "PANDORA_LOCALINFRA_MIRROR=$mirror"
call "$fakeScripts\bootstrap_pwsh.cmd"
exit /b %ERRORLEVEL%
"@ -replace "`r?`n", "`r`n"))

    $out = & cmd.exe /c "`"$runner`"" 2>&1 | Out-String
    $rc = $LASTEXITCODE
    Assert-True ($rc -ne 0) "假包时以非零码退出(实际 $rc)"
    Assert-True ($out -match 'SHA256 mismatch') '报的是 sha256 不匹配'
    Assert-True (-not (Test-Path -LiteralPath (Join-Path $tmpRoot 'run/localinfra/dist/pwsh'))) '校验不过就绝不解包'
    Assert-True (-not (Test-Path -LiteralPath (Join-Path $tmpRoot 'run/localinfra/dist/pwsh.tmp'))) '不留半截解包目录'
    # 坏包必须从 cache 删掉,否则下次跑还会拿它当缓存再撞一次。
    Assert-True (-not (Test-Path -LiteralPath (Join-Path $tmpRoot ('run/localinfra/cache/' + $pin.PWSH_FILE)))) '坏包不留在 cache'

    # ── [6] 全链路(仅在本机已备料时跑;没备料就跳过,不假装绿)──────────────
    Write-Host '[6] 全链路自举' -ForegroundColor Cyan
    $realZip = Join-Path $ProjectRoot ('run/localinfra/cache/' + $pin.PWSH_FILE)
    if (-not (Test-Path -LiteralPath $realZip)) {
        Write-Host "  [skip] 本机 cache 里没有 $($pin.PWSH_FILE),跳过全链路(先跑 local_infra.ps1 -Action provision)" -ForegroundColor Yellow
    }
    else {
        $goodMirror = Join-Path $tmpRoot 'mirror-good'
        New-Item -ItemType Directory -Force -Path $goodMirror | Out-Null
        Copy-Item $realZip (Join-Path $goodMirror $pin.PWSH_FILE)
        $runner2 = Join-Path $tmpRoot 'run2.cmd'
        [System.IO.File]::WriteAllText($runner2, (@"
@echo off
setlocal
set "PATH=$minimalPath"
set "PANDORA_LOCALINFRA_MIRROR=$goodMirror"
call "$fakeScripts\bootstrap_pwsh.cmd"
if errorlevel 1 exit /b 1
echo PANDORA_PWSH=%PANDORA_PWSH%
"%PANDORA_PWSH%" -NoProfile -Command "`$PSVersionTable.PSVersion.ToString()"
exit /b %ERRORLEVEL%
"@ -replace "`r?`n", "`r`n"))
        $out2 = & cmd.exe /c "`"$runner2`"" 2>&1 | Out-String
        $rc2 = $LASTEXITCODE
        Assert-True ($rc2 -eq 0) "好包时自举成功(实际退出码 $rc2)"
        Assert-True (Test-Path -LiteralPath (Join-Path $tmpRoot 'run/localinfra/dist/pwsh/pwsh.exe')) '解包到 run/localinfra/dist/pwsh'
        Assert-True ($out2 -match [regex]::Escape($pin.PWSH_VERSION)) "自举出来的解释器报告版本 $($pin.PWSH_VERSION)"
        Assert-True ($out2 -match 'PANDORA_PWSH=.*pwsh\.exe') 'PANDORA_PWSH 指向解包出的 pwsh.exe'
    }
}
finally {
    if (Test-Path -LiteralPath $tmpRoot) {
        try { [System.IO.Directory]::Delete($tmpRoot, $true) } catch { }
    }
}

Write-Host ''
if ($script:Failures.Count -gt 0) {
    Write-Host "[ERR ] $($script:Failures.Count) 项契约未满足:" -ForegroundColor Red
    $script:Failures | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}
Write-Host '[ OK ] PowerShell 7 自举契约全部满足。' -ForegroundColor Green
