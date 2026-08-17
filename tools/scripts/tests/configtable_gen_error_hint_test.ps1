$ErrorActionPreference = 'Stop'

$scriptPath = Join-Path (Split-Path -Parent $PSScriptRoot) 'configtable_gen.ps1'
$source = Get-Content -LiteralPath $scriptPath -Raw

$requiredHint = "elseif (`$outText -match '.*必填列为空')"
$readHint = "elseif (`$outText -match '读\s+.*\.xlsx\s+失败|打开 xlsx 失败')"
$requiredIndex = $source.IndexOf($requiredHint, [StringComparison]::Ordinal)
$readIndex = $source.IndexOf($readHint, [StringComparison]::Ordinal)

if ($requiredIndex -lt 0) {
    throw '缺少「必填列为空」专用错误分流。'
}
if ($readIndex -lt 0) {
    throw '缺少仅匹配真实 xlsx 读取失败的错误分流。'
}
if ($requiredIndex -gt $readIndex) {
    throw '必填列错误必须先于 xlsx 读取错误匹配。'
}
if ($source -match "读\\s\+\.\*失败\|xlsx") {
    throw 'xlsx 读取错误分流仍会吞掉所有带 .xlsx 文件名的校验错误。'
}

Write-Host '[PASS] configtable_gen 错误提示分类契约'
