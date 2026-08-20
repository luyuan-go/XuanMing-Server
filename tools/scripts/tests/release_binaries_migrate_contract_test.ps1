<#
.SYNOPSIS
  策划免 Docker Windows 发布包必须携带数据库迁移器。

.DESCRIPTION
  只读检查构建、manifest、zip 与消费路径的接线；不执行 go build，不创建制品，
  也不启动 Docker、数据库或服务。

.EXAMPLE
  pwsh tools/scripts/tests/release_binaries_migrate_contract_test.ps1
#>
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot '../../..')).Path
$BuildScript = Join-Path $ProjectRoot 'tools/scripts/build_release_binaries.ps1'
$MigrateScript = Join-Path $ProjectRoot 'tools/scripts/dev_migrate.ps1'
$BuildSource = [System.IO.File]::ReadAllText($BuildScript)
$MigrateSource = [System.IO.File]::ReadAllText($MigrateScript)

$script:Failures = [System.Collections.Generic.List[string]]::new()
function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) {
        Write-Host "  [ ok ] $Message" -ForegroundColor DarkGray
    } else {
        $script:Failures.Add($Message)
        Write-Host "  [FAIL] $Message" -ForegroundColor Red
    }
}

Write-Host '[1] 发布构建接入 pandora-migrate.exe' -ForegroundColor Cyan
$parseErrors = $null
$buildAst = [System.Management.Automation.Language.Parser]::ParseFile(
    $BuildScript, [ref]$null, [ref]$parseErrors)
Assert-True (-not ($parseErrors -and $parseErrors.Count -gt 0)) 'build_release_binaries.ps1 语法可解析'

$fullBuildBlocks = @($buildAst.FindAll({
            param($node)
            $node -is [System.Management.Automation.Language.IfStatementAst] -and
            $node.Extent.Text -match 'if\s*\(\s*-not\s+\$Service\s*\)'
        }, $true))
Assert-True ($fullBuildBlocks.Count -eq 1) '存在唯一的整批构建工具分支 if (-not $Service)'

$fullBuildText = if ($fullBuildBlocks.Count -eq 1) { $fullBuildBlocks[0].Extent.Text } else { '' }
Assert-True ($fullBuildText -match "Join-Path\s+\`$ProjectRoot\s+'tools/migrate'") `
    '整批发布从 tools/migrate 模块构建迁移器'
Assert-True ($fullBuildText -match "Join-Path\s+\`$ArtifactDir\s+'pandora-migrate\.exe'") `
    '迁移器输出到 windows/bin/pandora-migrate.exe'
Assert-True ($fullBuildText -match '&\s+go\s+build\s+-o\s+\$migrateExePath\s+\.') `
    '迁移器使用 go build -o 写入约定产物路径'

Write-Host '[2] manifest、zip 与运行时消费路径闭环' -ForegroundColor Cyan
$migrateBuildIndex = $BuildSource.IndexOf('go build -o $migrateExePath', [StringComparison]::Ordinal)
$manifestScanIndex = $BuildSource.IndexOf('$exes = @(', [StringComparison]::Ordinal)
Assert-True ($migrateBuildIndex -ge 0 -and $manifestScanIndex -gt $migrateBuildIndex) `
    '迁移器在 manifest 扫描 bin/*.exe 之前完成构建，清单会记录其 hash'
Assert-True ($BuildSource -match "Compress-Archive\s+-Path\s+\(Join-Path\s+\(Split-Path\s+-Parent\s+\`$ArtifactDir\)\s+'\*'\)") `
    'zip 打包 windows 产物根，包含 bin/pandora-migrate.exe 与 manifest.json'
Assert-True ($MigrateSource -match "run/artifacts/windows/bin/pandora-migrate\.exe") `
    'dev_migrate.ps1 消费同一个 pandora-migrate.exe 路径'

Write-Host ''
if ($script:Failures.Count -gt 0) {
    Write-Host "[ERR ] $($script:Failures.Count) 项发布迁移器契约未满足:" -ForegroundColor Red
    $script:Failures | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}

Write-Host '[ OK ] 策划免 Docker 发布包迁移器契约全部满足。' -ForegroundColor Green
