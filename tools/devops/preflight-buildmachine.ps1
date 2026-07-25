#!/usr/bin/env pwsh
<#
.SYNOPSIS
  构建/发布机开工前自检：确认这台机器能不能完成「UE 打包 → 发布制品库 → 打 DS 镜像」全链。

.DESCRIPTION
  换机器（如迁到发布机 infinity-engine）后先跑这个，避免打包跑了半小时才发现缺前置。
  检查项与真实脚本的硬门禁一一对应：
    - Tool\Build\PackageSet.ps1  : 编辑器进程 / LINUX_MULTIARCH_ROOT / 源码引擎
    - Tool\Build\PublishPackages.ps1 : svn 可用（版本戳要 svnversion）/ 制品根
    - deploy\ds\build-image-minikube.ps1 : docker / 制品根

  只读检查，不改任何配置、不写文件、不联网认证。

.EXAMPLE
  pwsh tools\devops\preflight-buildmachine.ps1
  pwsh tools\devops\preflight-buildmachine.ps1 -ClientRepo D:\Pandora-Client-SVN
#>
[CmdletBinding()]
param(
    [string]$ClientRepo,          # UE 客户端 SVN 工作副本；留空则只跳过与之相关的检查
    [int]$MinFreeGB = 100         # UE 打包 + 制品保留的最低空闲空间建议值
)

$ErrorActionPreference = 'Continue'
$script:fail = 0
$script:warn = 0

function Ok   ($m) { Write-Host "  [ OK ] $m" -ForegroundColor Green }
function Bad  ($m) { Write-Host "  [FAIL] $m" -ForegroundColor Red;    $script:fail++ }
function Warn ($m) { Write-Host "  [WARN] $m" -ForegroundColor Yellow; $script:warn++ }
function Section($t) { Write-Host "`n== $t ==" -ForegroundColor Cyan }

Write-Host "构建/发布机自检  $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')  $env:COMPUTERNAME" -ForegroundColor White

# ── 1. 基础工具链 ──
Section '基础工具'
if ($PSVersionTable.PSVersion.Major -ge 7) { Ok "PowerShell $($PSVersionTable.PSVersion)" }
else { Bad "PowerShell $($PSVersionTable.PSVersion) —— 项目要求 PowerShell 7+，装 pwsh 后用 pwsh 跑" }

foreach ($t in @(
    @{ n = 'svn';    why = 'PublishPackages 用 svnversion 取版本戳，没有它无法发布' },
    @{ n = 'git';    why = '拉后端仓库' },
    @{ n = 'docker'; why = '打 DS 镜像' }
)) {
    $c = Get-Command $t.n -ErrorAction SilentlyContinue
    if ($c) { Ok "$($t.n)  ($($c.Source))" } else { Bad "缺 $($t.n) —— $($t.why)" }
}
$goCmd = Get-Command go -ErrorAction SilentlyContinue
if ($goCmd) { Ok "go  ($(& go version 2>$null))" } else { Warn '缺 go —— 只打 UE 包可不用；要编后端二进制/镜像则需要' }

# ── 2. UE 打包前置（PackageSet.ps1 的硬门禁）──
Section 'UE 打包前置'

$editors = Get-Process -Name UnrealEditor, UE4Editor -ErrorAction SilentlyContinue
if ($editors) { Bad "UE 编辑器正在运行（PID $($editors.Id -join ',')）—— 会阻塞 UBT，打包前必须关闭" }
else { Ok 'UE 编辑器未运行' }

if ($env:LINUX_MULTIARCH_ROOT -and (Test-Path -LiteralPath $env:LINUX_MULTIARCH_ROOT)) {
    Ok "LINUX_MULTIARCH_ROOT = $env:LINUX_MULTIARCH_ROOT"
} elseif ($env:LINUX_MULTIARCH_ROOT) {
    Bad "LINUX_MULTIARCH_ROOT 已设但路径不存在：$env:LINUX_MULTIARCH_ROOT"
} else {
    Bad 'LINUX_MULTIARCH_ROOT 未设 —— 打 Linux DS 必需（装 UE 官方 Linux 交叉编译工具链后设置）'
}

# 源码引擎：Server 目标只有自制引擎能编，Epic launcher 发行版会报
# "Server targets are not currently supported from this engine distribution"
$bk = 'HKCU:\SOFTWARE\Epic Games\Unreal Engine\Builds'
$builds = $null
if (Test-Path $bk) { $builds = Get-Item $bk | ForEach-Object { $_.Property | ForEach-Object { [pscustomobject]@{ Guid = $_; Path = (Get-ItemProperty $bk).$_ } } } }
if ($builds) {
    $alive = $builds | Where-Object { $_.Path -and (Test-Path -LiteralPath $_.Path) }
    if ($alive) {
        Ok "已注册源码引擎 $($alive.Count) 个："
        $alive | ForEach-Object { Write-Host "         $($_.Path)" -ForegroundColor DarkGray }
    } else {
        Bad "HKCU Builds 里有注册项但路径都不存在 —— 源码引擎没装好"
    }
} else {
    Bad '未注册任何源码引擎（HKCU\SOFTWARE\Epic Games\Unreal Engine\Builds 为空）—— Server/Linux 目标无法编译；Epic 发行版不行，需自建引擎并 RegisterEngine'
}

# ── 3. 客户端工作副本 ──
Section '客户端 SVN 工作副本'
if ($ClientRepo) {
    if (Test-Path -LiteralPath (Join-Path $ClientRepo '.svn')) {
        Ok "SVN 工作副本：$ClientRepo"
        foreach ($s in @('Tool\Build\PackageSet.ps1', 'Tool\Build\Package.bat', 'Tool\Build\PublishPackages.ps1')) {
            if (Test-Path -LiteralPath (Join-Path $ClientRepo $s)) { Ok "  $s" } else { Bad "  缺 $s（svn update 拿全）" }
        }
    } else { Bad "不是 SVN 工作副本（没有 .svn）：$ClientRepo" }
} else {
    Warn '未传 -ClientRepo，跳过客户端检查。打包必须有客户端 SVN 工作副本'
}

# ── 4. 制品库 ──
Section '制品库'
if ($env:PANDORA_ARTIFACT_ROOT) {
    if ($env:PANDORA_ARTIFACT_ROOT -match '^[a-z]+://') {
        # URL 形态直接判死，不再做存在性检查（那只会输出误导性的"发布时会自动创建"）
        Bad "制品根被设成了 URL：$env:PANDORA_ARTIFACT_ROOT"
        Write-Host "         发布/解析脚本用的是文件系统 API（robocopy / Test-Path / Move-Item），" -ForegroundColor Red
        Write-Host "         URL 不是文件系统路径：Test-Path 直接返回 False，结果是'找不到任何制品'而非报错，极难排查。" -ForegroundColor Red
        Write-Host "         请改用本地路径，或 SMB 共享的 UNC 路径（\\主机\共享\artifacts）。" -ForegroundColor Red
    } elseif (Test-Path -LiteralPath $env:PANDORA_ARTIFACT_ROOT) {
        Ok "PANDORA_ARTIFACT_ROOT = $env:PANDORA_ARTIFACT_ROOT（存在）"
    } else {
        Warn "PANDORA_ARTIFACT_ROOT = $env:PANDORA_ARTIFACT_ROOT（尚不存在，发布时会自动创建）"
    }
} else {
    # 未设时两个脚本都退到硬编码默认值 F:\work\artifacts。
    # 该路径存在 => 能用（但绑死在这台机器的盘符上）；不存在 => 发布落空、DS 包解析不到，是真阻断。
    $defaultRoot = 'F:\work\artifacts'
    if (Test-Path -LiteralPath $defaultRoot) {
        Warn "未设 PANDORA_ARTIFACT_ROOT，将使用默认值 $defaultRoot（存在，可用）。换机器/换盘符前记得显式设置，别依赖这个默认值"
    } else {
        Bad "未设 PANDORA_ARTIFACT_ROOT，且默认值 $defaultRoot 不存在 —— 发布会落空、DS 包解析不到。先 setx PANDORA_ARTIFACT_ROOT <本机路径>（设完需重开终端）"
    }
}

# ── 5. 磁盘空间 ──
Section '磁盘空间'
$targets = @($PWD.Path)
if ($ClientRepo) { $targets += $ClientRepo }
if ($env:PANDORA_ARTIFACT_ROOT -and $env:PANDORA_ARTIFACT_ROOT -notmatch '^[a-z]+://') { $targets += $env:PANDORA_ARTIFACT_ROOT }
$targets | ForEach-Object { try { [System.IO.Path]::GetPathRoot((Resolve-Path -LiteralPath $_ -ErrorAction SilentlyContinue).Path ?? $_) } catch { $null } } |
    Where-Object { $_ } | Select-Object -Unique | ForEach-Object {
        $root = $_
        $d = Get-PSDrive -Name $root.TrimEnd(':\/') -ErrorAction SilentlyContinue
        if ($d) {
            $freeGB = [math]::Round($d.Free / 1GB, 1)
            if ($freeGB -ge $MinFreeGB) { Ok "$root 空闲 $freeGB GB" }
            else { Warn "$root 只剩 $freeGB GB（建议 ≥ $MinFreeGB GB：UE 打包 + 制品保留很吃空间）" }
        }
    }

# ── 汇总 ──
Write-Host ""
if ($script:fail -eq 0 -and $script:warn -eq 0) {
    Write-Host "全部通过 —— 这台机器可以完成打包+发布全链。" -ForegroundColor Green
} elseif ($script:fail -eq 0) {
    Write-Host "无阻断项，但有 $($script:warn) 条提醒 —— 可以开工，注意上面 WARN。" -ForegroundColor Yellow
} else {
    Write-Host "有 $($script:fail) 项阻断（另有 $($script:warn) 条提醒）—— 先补齐 FAIL 项再打包，否则会在中途失败。" -ForegroundColor Red
}
Write-Host @"

通过后的标准流程（客户端仓库根 / 后端仓库根）：
  1) pwsh Tool\Build\PackageSet.ps1 -Flavors 'Server/Linux/Development'
  2) pwsh Tool\Build\PublishPackages.ps1 -AllowDirty
  3) pwsh deploy\ds\build-image-minikube.ps1 -BuildOnHost            # 想先看来源加 -ResolveOnly
"@ -ForegroundColor DarkGray

exit ([int]($script:fail -gt 0))
