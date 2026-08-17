# configtable_gen_svn_status_test — configtable_gen.ps1 里两个 SVN 判定函数的**行为**回归。
#
# 为什么要单独一份:这两个函数决定的是「策划一键导表」失败时给出的指引方向,
# 判错方向的代价不是报错更难看,而是**把人指到错的地方**:
#
#   · Resolve-SourceRev 一旦把非数字输出(svn 报错文本、空、E155010)当成版本号,
#     生成器就会收下一个不可追溯的 -source-rev,批次事后无法定位源表版本。
#   · Get-TableWorkingCopyChanges 用 `return , $changed` 包了一层逗号,正是因为
#     PowerShell 会在 return 时展开集合:空 List 会退化成 $null,于是
#     「工作副本干净」和「本机没装 svn」撞成同一个返回值 —— 前者该说"程序 update 就能看到",
#     后者该说"没法判断,请把原文发给程序"。这层逗号被谁顺手删掉,静态检查是看不出来的。
#
# 所以这里把函数从 AST 里取出来真跑一遍,用 svn 桩喂各种真实输出。
$ErrorActionPreference = 'Stop'

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$TargetScript = Join-Path (Split-Path -Parent $ScriptDir) 'configtable_gen.ps1'
if (-not (Test-Path -LiteralPath $TargetScript -PathType Leaf)) { throw "找不到 configtable_gen.ps1:$TargetScript" }

$failed = 0
function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) { Write-Host "  [PASS] $Message" -ForegroundColor DarkGray }
    else { Write-Host "  [FAIL] $Message" -ForegroundColor Red; $script:failed++ }
}

# ---- 从 configtable_gen.ps1 取出被测函数(只取函数体,不执行脚本主流程)----
$tokens = $null; $errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($TargetScript, [ref]$tokens, [ref]$errors)
if ($errors.Count -gt 0) { throw "configtable_gen.ps1 解析失败:$($errors[0].Message)" }
foreach ($name in @('Resolve-SourceRev', 'Get-TableWorkingCopyChanges')) {
    $fn = @($ast.FindAll({ param($n)
        $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name }, $true))
    if ($fn.Count -ne 1) { throw "configtable_gen.ps1 中未找到唯一的 $name" }
    . ([scriptblock]::Create($fn[0].Extent.Text))
}

# ---- svn 桩 ----
# 被测函数先 Get-Command svn 探在不在,再 & svn 取输出;两者都要能按用例摆布。
$script:SvnPresent = $true
$script:SvnOutput = ''
$script:SvnCalls = 0

function Get-Command {
    [CmdletBinding()]
    param(
        [Parameter(Position = 0)][string]$Name,
        [Parameter(ValueFromRemainingArguments = $true)]$Rest
    )
    if ($Name -ceq 'svn') {
        if (-not $script:SvnPresent) { return $null }
        return [pscustomobject]@{ Name = 'svn' }
    }
    throw "测试桩未预期的 Get-Command 调用:$Name"
}

function svn {
    $script:SvnCalls++
    return $script:SvnOutput
}

$TableRoot = 'F:\work\Pandora-Client-SVN\Table'

# ---- Resolve-SourceRev ----
Write-Host 'Resolve-SourceRev'

$script:SvnCalls = 0
$explicit = Resolve-SourceRev '  svn-r1774  ' $TableRoot
Assert-True ($explicit -ceq 'svn-r1774') '显式 -SourceRev 原样保留(去空白)'
Assert-True ($script:SvnCalls -eq 0) '显式 -SourceRev 时不应再调 svn'

$script:SvnPresent = $false
Assert-True ((Resolve-SourceRev '' $TableRoot) -ceq '') '本机没装 svn 时返回空(由调用方拒绝生成)'

$script:SvnPresent = $true
$script:SvnOutput = '1844'
Assert-True ((Resolve-SourceRev '' $TableRoot) -ceq 'svn-r1844') '纯数字版本号转成 svn-r<N>'

$script:SvnOutput = "1844`n"
Assert-True ((Resolve-SourceRev '' $TableRoot) -ceq 'svn-r1844') '版本号带换行也要能解析(svn 输出总带换行)'

$script:SvnOutput = "2064 F:\work\Pandora-Client-SVN\Table`n2066 F:\work\Pandora-Client-SVN\Table\role.xlsx"
Assert-True ((Resolve-SourceRev '' $TableRoot) -ceq 'svn-r2066') '混合版本工作副本必须取子节点最大 revision'

# 非工作副本 / 中文路径 E155010 / svn 把错误写到 stdout:全都不是版本号,必须拒。
foreach ($bad in @('', '   ', 'Exit 1', "svn: E155010: The node '...' was not found.", 'Unversioned directory', '18a4')) {
    $script:SvnOutput = $bad
    Assert-True ((Resolve-SourceRev '' $TableRoot) -ceq '') "非数字输出必须判为取不到版本号:'$bad'"
}

# ---- Get-TableWorkingCopyChanges ----
Write-Host 'Get-TableWorkingCopyChanges'

$script:SvnPresent = $false
Assert-True ($null -eq (Get-TableWorkingCopyChanges $TableRoot)) '本机没装 svn 返回 $null(= 无法判断)'

$script:SvnPresent = $true
foreach ($bad in @('', '   ', 'not xml at all')) {
    $script:SvnOutput = $bad
    Assert-True ($null -eq (Get-TableWorkingCopyChanges $TableRoot)) "svn status 输出不可解析时返回 `$null:'$bad'"
}

# 干净工作副本:必须是**空集合**而不是 $null —— 这正是那层 `return , $changed` 的意义。
# 退化成 $null 会让「表已提交、程序 update 就能看到」被误报成「本机没法判断」。
$script:SvnOutput = @'
<?xml version="1.0" encoding="UTF-8"?>
<status>
  <target path=".">
    <entry path="Table\角色\j_角色等级.xlsx">
      <wc-status item="normal" props="none" />
    </entry>
    <entry path="Table\externals">
      <wc-status item="external" props="none" />
    </entry>
    <entry path="Table\~$tmp.xlsx">
      <wc-status item="ignored" props="none" />
    </entry>
  </target>
</status>
'@
$clean = Get-TableWorkingCopyChanges $TableRoot
Assert-True ($null -ne $clean) '工作副本干净时不得返回 $null(会与"没装 svn"撞成同一个返回值)'
Assert-True ($clean.Count -eq 0) 'normal/external/ignored 都不算改动'

# 有未提交改动:要能带出路径与状态,供调用方按表名过滤。
$script:SvnOutput = @'
<?xml version="1.0" encoding="UTF-8"?>
<status>
  <target path=".">
    <entry path="Table\角色\j_角色等级.xlsx">
      <wc-status item="modified" props="none" />
    </entry>
    <entry path="Table\道具\j_新道具.xlsx">
      <wc-status item="unversioned" props="none" />
    </entry>
    <entry path="Table\战斗\j_战斗.xlsx">
      <wc-status item="normal" props="none" />
    </entry>
  </target>
</status>
'@
# 按调用方（configtable_gen.ps1）的真实用法消费：整个集合直接进管道。
# 切勿再套一层 @()：那会把 `return , $changed` 保护起来的集合重新嵌成
# 「只含一个数组的数组」（k8s_down_preserve_predicate_test 里那起真故障就是这么来的）。
$dirty = Get-TableWorkingCopyChanges $TableRoot
Assert-True ($dirty.Count -eq 2) '只报非 normal/external/ignored 的条目'
Assert-True ((@($dirty | Where-Object { $_.Path -like '*j_角色等级.xlsx' })[0]).Item -ceq 'modified') '本地已改的表标为 modified'
Assert-True ((@($dirty | Where-Object { $_.Path -like '*j_新道具.xlsx' })[0]).Item -ceq 'unversioned') '新增未加入版本库的表标为 unversioned'
# 中文表名必须原样带出:调用方用 `-like "*$leaf"` 按表名过滤,糊了就永远匹配不上,
# 于是「未提交」被误报成「已与 SVN 一致」,程序被指去 svn update 一列根本不存在的字段。
Assert-True (($dirty | Where-Object { $_.Path -like '*j_角色等级.xlsx' }).Count -eq 1) '中文表名不得被编码糟蹋(否则按表名过滤恒不命中)'

if ($failed -gt 0) {
    Write-Host "configtable_gen_svn_status_test: FAIL($failed 条)" -ForegroundColor Red
    exit 1
}
Write-Host 'configtable_gen_svn_status_test: PASS' -ForegroundColor Green
