#!/usr/bin/env pwsh
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$scriptUnderTest = (Resolve-Path (Join-Path $PSScriptRoot '..\..\devops\publish-to-minio.ps1')).Path
$testRoot = Join-Path ([IO.Path]::GetTempPath()) ("pandora-minio-contract-{0}" -f [guid]::NewGuid().ToString('N'))
$binDir = Join-Path $testRoot 'bin'
$artifactRoot = Join-Path $testRoot 'artifacts'
$sourceDir = Join-Path $artifactRoot 'snapshots\images'
$fakeDocker = Join-Path $binDir 'docker.cmd'
$callLog = Join-Path $testRoot 'docker-calls.log'
$pointerFile = Join-Path $sourceDir 'latest.json'
$oldPath = $env:PATH

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "[FAIL] $Message" }
}

function Invoke-Scenario([string]$Scenario, [string]$ScenarioSource = $sourceDir) {
    $env:PANDORA_FAKE_SCENARIO = $Scenario
    $env:PANDORA_FAKE_LOG = $callLog
    $env:PANDORA_FAKE_POINTER_FILE = $pointerFile
    $env:PANDORA_FAKE_POINTER_SIZE = (Get-Item -LiteralPath $pointerFile).Length
    $env:PANDORA_FAKE_DATA_SIZE = if ($Scenario -eq 'size-mismatch') { 5 } else { 4 }
    $env:PANDORA_FAKE_DATA_KEY = if ($Scenario -eq 'case-mismatch') { 'VERSION/data.bin' } else { 'version/data.bin' }
    Remove-Item -LiteralPath $callLog -Force -ErrorAction SilentlyContinue

    $output = @(& pwsh -NoProfile -ExecutionPolicy Bypass -File $scriptUnderTest `
        -SourceDir $ScenarioSource `
        -ArtifactRoot $artifactRoot `
        -AccessKey test-access `
        -SecretKey test-secret `
        -Bucket test-bucket `
        -MirrorRetries 1 2>&1)
    [PSCustomObject]@{ ExitCode = $LASTEXITCODE; Output = $output }
}

try {
    New-Item -ItemType Directory -Path (Join-Path $sourceDir 'version'), $binDir -Force | Out-Null
    [IO.File]::WriteAllBytes((Join-Path $sourceDir 'version\data.bin'), [byte[]](1, 2, 3, 4))
    [IO.File]::WriteAllText($pointerFile, "{`n  `"version`": `"new`"`n}`n", [Text.UTF8Encoding]::new($false))

    [IO.File]::WriteAllText($fakeDocker, @'
@echo off
echo %*>>"%PANDORA_FAKE_LOG%"
echo %*| findstr /c:" mirror " >nul
if not errorlevel 1 (
    if /i "%PANDORA_FAKE_SCENARIO%"=="mirror-error" (
        echo {"status":"error","error":{"message":"Overwrite not allowed"}}
    ) else (
        if /i "%PANDORA_FAKE_SCENARIO%"=="pointer-race" (
            >"%PANDORA_FAKE_POINTER_FILE%" echo {
            >>"%PANDORA_FAKE_POINTER_FILE%" echo   "version": "raced"
            >>"%PANDORA_FAKE_POINTER_FILE%" echo }
        )
        echo {"status":"success","total":1,"transferred":1}
    )
    exit /b 0
)
echo %*| findstr /c:" cp " >nul
if not errorlevel 1 (
    echo {"status":"success"}
    exit /b 0
)
echo %*| findstr /c:" ls " >nul
if not errorlevel 1 (
    echo {"status":"success","type":"file","size":%PANDORA_FAKE_DATA_SIZE%,"key":"%PANDORA_FAKE_DATA_KEY%"}
    echo {"status":"success","type":"file","size":%PANDORA_FAKE_POINTER_SIZE%,"key":"latest.json"}
    echo {"status":"success","type":"file","size":9,"key":"historical/extra.bin"}
    exit /b 0
)
echo %*| findstr /c:" cat " >nul
if not errorlevel 1 (
    if /i "%PANDORA_FAKE_SCENARIO%"=="pointer-mismatch" (
        echo {
        echo   "version": "old"
        echo }
    ) else if /i "%PANDORA_FAKE_SCENARIO%"=="pointer-race" (
        echo {
        echo   "version": "new"
        echo }
    ) else (
        type "%PANDORA_FAKE_POINTER_FILE%"
    )
    exit /b 0
)
echo {"status":"error","error":{"message":"unexpected fake docker call"}}
exit /b 0
'@, [Text.ASCIIEncoding]::new())

    $env:PATH = "$binDir;$oldPath"

    $result = Invoke-Scenario 'mirror-error'
    Assert-True ($result.ExitCode -ne 0) 'mc JSON status=error 且 native exit=0 时必须失败'
    Assert-True (Test-Path -LiteralPath $callLog) "fake docker 未被调用：$($result.Output -join ' | ')"
    Assert-True ((Get-Content -Raw -LiteralPath $callLog) -match '(?:^|\s)mirror(?:\s|$)') '错误场景必须实际调用 mirror'

    $result = Invoke-Scenario 'success-with-history'
    Assert-True ($result.ExitCode -eq 0) "远端多历史对象时，本地对象完整应通过：$($result.Output -join ' | ')"
    $calls = Get-Content -LiteralPath $callLog
    $mirrorIndex = [Array]::FindIndex([string[]]$calls, [Predicate[string]]{ param($line) $line -match '(?:^|\s)mirror(?:\s|$)' })
    $copyIndex = [Array]::FindIndex([string[]]$calls, [Predicate[string]]{ param($line) $line -match '(?:^|\s)cp(?:\s|$)' })
    Assert-True ($mirrorIndex -ge 0 -and $copyIndex -gt $mirrorIndex) '必须先 mirror 不可变内容，再 cp latest.json'
    Assert-True ($calls[$mirrorIndex] -match '--exclude latest\.json') 'immutable mirror 必须精确排除 latest.json'
    Assert-True (-not (($calls -join "`n") -match 'test-secret')) 'MinIO 密码不得进入 docker 命令行'
    Assert-True (($calls -join "`n") -match '-e MC_HOST_pandora(?:\s|$)') 'docker 只能继承 MC_HOST_pandora 变量名'

    $result = Invoke-Scenario 'size-mismatch'
    Assert-True ($result.ExitCode -ne 0) '任一本地对象远端大小不一致时必须失败'

    $result = Invoke-Scenario 'pointer-mismatch'
    Assert-True ($result.ExitCode -ne 0) 'latest.json 远端内容不一致时必须失败'

    $result = Invoke-Scenario 'case-mismatch'
    Assert-True ($result.ExitCode -ne 0) 'S3 key 只有大小写不同时必须视为缺失并失败'

    $result = Invoke-Scenario 'pointer-race'
    Assert-True ($result.ExitCode -eq 0) "mirror 期间活指针变化时必须仍发布冻结快照：$($result.Output -join ' | ')"
    $calls = Get-Content -LiteralPath $callLog
    $copyCall = $calls | Where-Object { $_ -match '(?:^|\s)cp(?:\s|$)' } | Select-Object -First 1
    Assert-True ($copyCall -match '/pointer-snapshots/\d+-latest\.json') 'mc cp 必须读取冻结指针，而不是活的 artifact latest.json'
    Assert-True ((Get-Content -Raw -LiteralPath $pointerFile) -match 'raced') '竞态测试必须在 mirror 中真实改写活指针'

    $emptySource = Join-Path $artifactRoot 'snapshots\empty'
    New-Item -ItemType Directory -Path $emptySource -Force | Out-Null
    $result = Invoke-Scenario 'empty-source' $emptySource
    Assert-True ($result.ExitCode -ne 0) '空 SourceDir 必须失败，不能产生 0 文件成功'

    Write-Host '[PASS] publish_to_minio contract: error propagation, pointer ordering, one-way verification'
} finally {
    $env:PATH = $oldPath
    Remove-Item Env:PANDORA_FAKE_SCENARIO, Env:PANDORA_FAKE_LOG, Env:PANDORA_FAKE_POINTER_FILE, Env:PANDORA_FAKE_POINTER_SIZE, Env:PANDORA_FAKE_DATA_SIZE, Env:PANDORA_FAKE_DATA_KEY -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $testRoot -Recurse -Force -ErrorAction SilentlyContinue
}
