<#
.SYNOPSIS
  「策划一键启动」两条护栏的契约测试:本机 dev.env 自举 + 启动失败必须带出非零退出码。

.DESCRIPTION
  两条都来自 2026-08-12 现场:另一台机器刚更新完双击一键启动,断在 [1/4] 基础设施,
  而窗口最后一行报的是「完成(退出码 0)」。

  ① dev.env 命中 .gitignore 的 `*.env`,受版本控制的只有 dev.env.example ——
     任何新机器 / 新克隆第一次跑都必然缺它,而 docker compose 的 --env-file 指到不存在
     的路径直接失败。让人先手工 Copy-Item 就不叫「一键」了,所以入口必须自举。
     反向同样重要:已经存在的 dev.env 里可能填了本机真实 webhook/token,绝不许被覆盖。
  ② `&` 调子脚本**不会**让父脚本失败。dev_all.ps1 每步失败都 exit 1,但 Invoke-Local
     不透传的话 start.ps1 走完 switch 就正常结束 → 双击窗口 / Web 管理台看到绿色的 0。
     一键启动的绿灯必须等于「真的起来了」,否则策划只会掉头去查客户端。

  不需要 docker,不起任何容器:自举逻辑在临时目录里真实执行,透传用 AST 静态断言。

.EXAMPLE
  pwsh tools/scripts/tests/oneclick_devenv_exitcode_contract_test.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ScriptsDir  = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path

$script:Failures = @()
function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { $script:Failures += $Message; Write-Host "  [FAIL] $Message" -ForegroundColor Red }
    else { Write-Host "  [ ok ] $Message" -ForegroundColor DarkGray }
}

# ── [1] 自举行为:真实执行 dev_env_file.ps1,全程在临时目录,不碰仓库里的 dev.env ──
Write-Host '[1] dev.env 自举三态' -ForegroundColor Cyan
. (Join-Path $ScriptsDir 'dev_env_file.ps1')

$tmpRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("pandora-devenv-" + [System.IO.Path]::GetRandomFileName())
try {
    $envDir = Join-Path $tmpRoot 'deploy/env'
    New-Item -ItemType Directory -Force -Path $envDir | Out-Null
    $example = Join-Path $envDir 'dev.env.example'
    $target  = Join-Path $envDir 'dev.env'
    Copy-Item (Join-Path $ProjectRoot 'deploy/env/dev.env.example') $example

    # 1a 缺失 → 按 example 建出来
    Confirm-DevEnvFile -ProjectRoot $tmpRoot | Out-Null
    Assert-True (Test-Path -LiteralPath $target) '缺 dev.env 时自动建出'
    Assert-True ((Get-FileHash $target).Hash -eq (Get-FileHash $example).Hash) '内容与 dev.env.example 逐字节一致'
    Assert-True (@(Get-ChildItem -LiteralPath $envDir -Filter '*.tmp').Count -eq 0) '不留临时文件(并发双击靠原子改名)'

    # 1b 已存在 → 绝不覆盖、绝不出声(本机填的真实 webhook 不能被冲掉)
    $mine = "MYSQL_ROOT_PASSWORD=my_real_pwd`nPANDORA_ALERT_WECOM_URL=https://qyapi.example/real`n"
    [System.IO.File]::WriteAllText($target, $mine)
    $out = @(Confirm-DevEnvFile -ProjectRoot $tmpRoot *>&1)
    Assert-True ([System.IO.File]::ReadAllText($target) -eq $mine) '已存在的 dev.env 不被覆盖(本机 secret 保住)'
    Assert-True ($out.Count -eq 0) '已存在时静默,不刷屏'

    # 1c 连 example 都没有 = 工作区不完整,必须硬失败而不是静默造一个空文件
    $bareRoot = Join-Path $tmpRoot 'bare'
    New-Item -ItemType Directory -Force -Path (Join-Path $bareRoot 'deploy/env') | Out-Null
    $threw = $false
    try { Confirm-DevEnvFile -ProjectRoot $bareRoot | Out-Null } catch { $threw = $true }
    Assert-True $threw 'example 也缺时抛错(工作区不完整,不该假装成功)'
} finally {
    if (Test-Path -LiteralPath $tmpRoot) { [System.IO.Directory]::Delete($tmpRoot, $true) }
}

# ── [2] 一键链上每个吃 --env-file 的入口都必须接自举 ─────────────────────────
Write-Host '[2] 入口脚本接线' -ForegroundColor Cyan
$entries = @('start.ps1', 'dev_up.ps1', 'dev_down.ps1', 'dev_status.ps1', 'k8s_envoy_bridge.ps1')
foreach ($name in $entries) {
    $text = [System.IO.File]::ReadAllText((Join-Path $ScriptsDir $name))
    Assert-True ($text -match 'dev_env_file\.ps1')  "$name 载入 dev_env_file.ps1"
    Assert-True ($text -match 'Confirm-DevEnvFile') "$name 调用 Confirm-DevEnvFile"
}

# ── [3] 不许再出现第二份内联实现(会各自漂移;k8s 分支就曾独有一份)────────────
# tests/ 除外:契约测试自己要造「缺 dev.env」的场景,必然要复制 example。
Write-Host '[3] 自举实现唯一' -ForegroundColor Cyan
$testsDir = $PSScriptRoot
$inline = @(Get-ChildItem -LiteralPath $ScriptsDir -Filter '*.ps1' -Recurse |
    Where-Object { $_.Name -ne 'dev_env_file.ps1' -and $_.DirectoryName -ne $testsDir } |
    Where-Object { [System.IO.File]::ReadAllText($_.FullName) -match 'Copy-Item[^\r\n]*dev\.env\.example' })
Assert-True ($inline.Count -eq 0) ('没有内联的 Copy-Item dev.env.example 副本' + $(if ($inline.Count) { ':' + (($inline.Name) -join ', ') }))

# ── [4] Invoke-Local 必须透传 dev_all.ps1 的退出码 ──────────────────────────
Write-Host '[4] 退出码透传' -ForegroundColor Cyan
$perrs = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile((Join-Path $ScriptsDir 'start.ps1'), [ref]$null, [ref]$perrs)
Assert-True (-not ($perrs -and $perrs.Count)) 'start.ps1 语法可解析'
$fn = $ast.Find({ param($n)
    $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'Invoke-Local' }, $true)
Assert-True ($null -ne $fn) 'start.ps1 里找得到 Invoke-Local'
if ($fn) {
    $lines = $fn.Extent.Text -split "`r?`n"
    # 只认真正的调用行:注释里也会提 dev_all.ps1(本次修复就写了一段),把注释算进来会让
    # 「到下一次调用为止」的窗口被截成空,测试自己造出假红。
    $callIdx = @(0..($lines.Count - 1) | Where-Object {
        $lines[$_] -match 'dev_all\.ps1' -and $lines[$_] -notmatch '^\s*#' })
    Assert-True ($callIdx.Count -ge 2) "Invoke-Local 里有 $($callIdx.Count) 处 dev_all.ps1 调用(-Down 与正常启动各一)"
    foreach ($i in $callIdx) {
        # 窗口 = 到下一次 dev_all 调用为止,最多 12 行:够放注释,又不会借到别处的检查充数
        $end = [Math]::Min($i + 12, $lines.Count - 1)
        foreach ($j in $callIdx) { if ($j -gt $i -and ($j - 1) -lt $end) { $end = $j - 1 } }
        $window = if ($end -gt $i) { ($lines[($i + 1)..$end]) -join "`n" } else { '' }
        Assert-True (($window -match '\$LASTEXITCODE') -and ($window -match '\bexit\b')) `
            "第 $($i + 1) 行 dev_all.ps1 调用后有 LASTEXITCODE 判定并 exit 带出"
    }
}

Write-Host ''
if ($script:Failures.Count -gt 0) {
    Write-Host "[ERR ] $($script:Failures.Count) 项契约未满足:" -ForegroundColor Red
    $script:Failures | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}
Write-Host '[ OK ] 一键启动 dev.env 自举 + 退出码透传契约全部满足。' -ForegroundColor Green
