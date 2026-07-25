#!/usr/bin/env pwsh
<#
.SYNOPSIS
  把 Jenkins 从「空实例」接线到「三条流水线可跑」。up.ps1 之后跑这一条即可。

.DESCRIPTION
  up.ps1 只负责把容器起起来；本脚本负责 Jenkins 内部的接线，这部分不在容器镜像里：
    1. 确认 Java（宿主机 agent 的硬前提）
    2. 创建/更新构建节点 windows-host（含 UE 打包所需的环境变量）
    3. 下载 agent.jar 并把 agent 跑起来（已连上则跳过）
    4. 创建/更新 client-dev / backend-dev / artifacts-sync 三个任务
    5. 报告仍需人工完成的事项（SVN 凭据）

  **幂等**：可反复执行。已存在的节点/任务走更新而不是报错；agent 已在线则不重复拉起。

  **不碰密码**：SVN 凭据必须由你在 Jenkins 界面自行创建，本脚本只检查它在不在、
  并把任务指向它的 ID。MinIO 凭据留在机器本地 .env，同样不进 Jenkins。

  配置来自同目录 .env（见 .env.example 的「Jenkins 接线」段）。

.EXAMPLE
  pwsh tools/devops/setup-jenkins.ps1
  pwsh tools/devops/setup-jenkins.ps1 -SkipAgent     # 只建/更新任务，不动 agent
  pwsh tools/devops/setup-jenkins.ps1 -WhatIfOnly    # 只体检，不做任何修改
#>
[CmdletBinding()]
param(
    [switch]$SkipAgent,
    [switch]$WhatIfOnly
)

$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot

function Info($m) { Write-Host "[setup] $m" -ForegroundColor Cyan }
function Ok($m)   { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Fail($m) { Write-Host "[FAIL] $m" -ForegroundColor Red }

# ── 读 .env ──
$envFile = Join-Path $here '.env'
if (-not (Test-Path -LiteralPath $envFile)) {
    throw "缺少 $envFile。先跑 up.ps1（它会从 .env.example 生成），或手工复制一份。"
}
$cfg = @{}
Get-Content -LiteralPath $envFile | ForEach-Object {
    if ($_ -match '^\s*([^#=]+)=(.*)$') { $cfg[$Matches[1].Trim()] = $Matches[2].Trim() }
}
function Cfg($k, $default) { if ($cfg.ContainsKey($k) -and $cfg[$k]) { $cfg[$k] } else { $default } }

$jUser   = Cfg 'JENKINS_ADMIN_ID' 'admin'
$jPass   = Cfg 'JENKINS_ADMIN_PASSWORD' 'change-me-admin'
$jPort   = Cfg 'JENKINS_HTTP_PORT' '8080'
$jUrl    = "http://localhost:$jPort"

# 接线参数（占位符 -> 实际值）
$repoRoot = (Resolve-Path (Join-Path $here '..\..')).Path
$subst = @{
    '@@BACKEND_GIT_URL@@'           = Cfg 'BACKEND_GIT_URL' 'https://github.com/luyuan-go/XuanMing-Server.git'
    '@@SVN_CLIENT_URL@@'            = Cfg 'SVN_CLIENT_URL' 'http://infinity-svn/svn/Pandora-Moba/trunk/Client'
    '@@SVN_CREDENTIALS_ID@@'        = Cfg 'SVN_CREDENTIALS_ID' 'svn-cred'
    '@@AGENT_WORKDIR@@'             = Cfg 'AGENT_WORKDIR' 'F:\jenkins-agent'
    '@@PANDORA_PROTO_SERVER_ROOT@@' = Cfg 'PANDORA_PROTO_SERVER_ROOT' $repoRoot
    '@@PANDORA_ARTIFACT_ROOT@@'     = Cfg 'PANDORA_ARTIFACT_ROOT' 'F:\work\artifacts'
    '@@LINUX_MULTIARCH_ROOT@@'      = Cfg 'LINUX_MULTIARCH_ROOT' $env:LINUX_MULTIARCH_ROOT
}
$agentName = 'windows-host'

Write-Host ""
Info "Jenkins   : $jUrl"
Info "接线参数  :"
$subst.GetEnumerator() | Sort-Object Name | ForEach-Object {
    Write-Host ("           {0,-32} = {1}" -f ($_.Key -replace '@@',''), $_.Value) -ForegroundColor DarkGray
}
Write-Host ""

# ── HTTP 助手 ──
$authHeader = @{ Authorization = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${jUser}:${jPass}")) }
# 超时给足：机器上同时跑着开发用的一整套容器时 Jenkins 会明显变慢，
# 超时太短会让本脚本在体检阶段就误报连不上。
function JGet($path) { Invoke-RestMethod -Uri "$jUrl$path" -Headers $authHeader -TimeoutSec 180 }

# ── 0. Jenkins 可达 + 声明式插件 ──
Info '检查 Jenkins ...'
try { $null = JGet '/api/json' } catch { throw "连不上 $jUrl（$($_.Exception.Message)）。先跑 up.ps1。" }
$plugins = JGet '/pluginManager/api/json?depth=1'
$decl = $plugins.plugins | Where-Object { $_.shortName -eq 'pipeline-model-definition' -and $_.active }
if ($decl) { Ok "声明式 Pipeline 插件已激活（共 $($plugins.plugins.Count) 个插件）" }
else {
    Fail '声明式 Pipeline 插件未激活 —— 此时 Jenkinsfile 的 pipeline{} 块会被静默忽略，构建"成功"但零 stage。'
    Fail '处理：docker compose -f docker-compose.stack.yml restart jenkins；仍不行则 up.ps1 -Pull 重建镜像。'
    throw '前置未满足，中止。'
}

# ── jenkins-cli.jar（建/改任务与节点都靠它；REST 直接 POST 会被 CSRF 挡 403）──
$cliJar = Join-Path $here 'jenkins\jenkins-cli.jar'
function Get-JavaExe {
    $c = Get-Command java -ErrorAction SilentlyContinue
    if ($c) { return $c.Source }
    $jdk = Get-ChildItem 'C:\Program Files\Microsoft\jdk*', 'C:\Program Files\Eclipse Adoptium\jdk*' -Directory -ErrorAction SilentlyContinue |
           Sort-Object Name -Descending | Select-Object -First 1
    if ($jdk) { return (Join-Path $jdk.FullName 'bin\java.exe') }
    return $null
}

# ── 1. Java ──
Info '检查 Java（宿主机 agent 的硬前提）...'
$java = Get-JavaExe
if (-not $java) {
    if ($WhatIfOnly) { Fail 'Java 未安装（-WhatIfOnly 不执行安装）' }
    else {
        Warn 'Java 未安装，用 winget 安装 Microsoft.OpenJDK.21 ...'
        if (-not (Get-Command winget -ErrorAction SilentlyContinue)) { throw '缺 Java 且无 winget，请手工安装 JDK 21 后重跑。' }
        & winget install --id Microsoft.OpenJDK.21 --silent --accept-source-agreements --accept-package-agreements | Out-Null
        $java = Get-JavaExe
        if (-not $java) { throw 'Java 安装后仍找不到，请手工确认。' }
    }
}
if ($java) { Ok "Java: $java" }

if (-not $WhatIfOnly -and -not (Test-Path -LiteralPath $cliJar)) {
    Info '下载 jenkins-cli.jar ...'
    Invoke-WebRequest -Uri "$jUrl/jnlpJars/jenkins-cli.jar" -OutFile $cliJar -Headers $authHeader -UseBasicParsing
}
function JCli { param([string[]]$CliArgs, [string]$StdIn)
    $a = @('-jar', $cliJar, '-s', $jUrl, '-auth', "${jUser}:${jPass}") + $CliArgs
    if ($StdIn) { $StdIn | & $java @a 2>&1 } else { & $java @a 2>&1 }
}

# ── 模板渲染 ──
function Render($templatePath) {
    $t = [IO.File]::ReadAllText($templatePath)
    foreach ($k in $subst.Keys) { $t = $t.Replace($k, [string]$subst[$k]) }
    $left = [regex]::Matches($t, '@@[A-Z_]+@@')
    if ($left.Count) { throw "模板 $([IO.Path]::GetFileName($templatePath)) 仍有未替换占位符：$(($left | ForEach-Object { $_.Value } | Select-Object -Unique) -join ', ')" }
    return $t
}

# ── 2. 节点 ──
$existingNodes = (JGet '/computer/api/json').computer | ForEach-Object { $_.displayName }
$nodeTemplate = Join-Path $here 'jenkins\node-windows-host.xml'
if ($WhatIfOnly) {
    if ($existingNodes -contains $agentName) { Ok "节点 $agentName 已存在（-WhatIfOnly 不修改）" } else { Warn "节点 $agentName 不存在（-WhatIfOnly 不创建）" }
} else {
    $xml = Render $nodeTemplate
    if ($existingNodes -contains $agentName) {
        Info "节点 $agentName 已存在 → 更新配置"
        $r = JCli -CliArgs @('update-node', $agentName) -StdIn $xml
    } else {
        Info "创建节点 $agentName"
        $r = JCli -CliArgs @('create-node', $agentName) -StdIn $xml
    }
    if ($LASTEXITCODE -ne 0) { Fail "节点操作失败：$r" } else { Ok "节点 $agentName 就绪" }
}

# ── 3. agent 进程 ──
if ($SkipAgent -or $WhatIfOnly) {
    Info 'agent 拉起：已跳过'
} else {
    $node = JGet "/computer/$agentName/api/json"
    if (-not $node.offline) { Ok "agent 已在线，跳过拉起" }
    else {
        $workDir = [string]$subst['@@AGENT_WORKDIR@@']
        New-Item -ItemType Directory -Path $workDir -Force | Out-Null
        $agentJar = Join-Path $workDir 'agent.jar'
        Info '下载 agent.jar ...'
        Invoke-WebRequest -Uri "$jUrl/jnlpJars/agent.jar" -OutFile $agentJar -Headers $authHeader -UseBasicParsing
        # 从 JNLP 里取密钥（每个 Jenkins 实例独有，不能预置进仓库）
        $r = Invoke-WebRequest -Uri "$jUrl/computer/$agentName/jenkins-agent.jnlp" -Headers $authHeader -UseBasicParsing
        $jnlp = if ($r.Content -is [byte[]]) { [Text.Encoding]::UTF8.GetString($r.Content) } else { $r.Content }
        $secret = ([regex]'<argument>([a-f0-9]{64})</argument>').Match($jnlp).Groups[1].Value
        if (-not $secret) { throw "取不到 agent 密钥（$jUrl/computer/$agentName/jenkins-agent.jnlp）" }
        Info '启动 agent（后台进程，日志在工作目录）...'
        $p = Start-Process -FilePath $java -WindowStyle Hidden -PassThru `
             -ArgumentList @('-jar', $agentJar, '-url', "$jUrl/", '-secret', $secret, '-name', $agentName, '-webSocket', '-workDir', $workDir) `
             -RedirectStandardOutput (Join-Path $workDir 'agent-out.log') `
             -RedirectStandardError  (Join-Path $workDir 'agent-err.log')
        Info "agent PID=$($p.Id)，等待接入 ..."
        $online = $false
        for ($i = 0; $i -lt 24; $i++) {
            Start-Sleep -Seconds 5
            try { if (-not (JGet "/computer/$agentName/api/json").offline) { $online = $true; break } } catch {}
        }
        if ($online) { Ok "agent $agentName 已上线" }
        else { Fail "agent 未在 2 分钟内接入，查看 $workDir\agent-err.log" }
    }
}

# ── 4. 任务 ──
$jobDir = Join-Path $here 'jenkins\jobs'
$existingJobs = (JGet '/api/json?tree=jobs[name]').jobs | ForEach-Object { $_.name }
foreach ($f in Get-ChildItem "$jobDir\*.xml" | Sort-Object Name) {
    $name = [IO.Path]::GetFileNameWithoutExtension($f.Name)
    if ($WhatIfOnly) {
        if ($existingJobs -contains $name) { Ok "任务 $name 已存在（-WhatIfOnly 不修改）" } else { Warn "任务 $name 不存在（-WhatIfOnly 不创建）" }
        continue
    }
    $xml = Render $f.FullName
    if ($existingJobs -contains $name) {
        $r = JCli -CliArgs @('update-job', $name) -StdIn $xml
        if ($LASTEXITCODE -ne 0) { Fail "更新任务 $name 失败：$r" } else { Ok "任务 $name 已更新" }
    } else {
        $r = JCli -CliArgs @('create-job', $name) -StdIn $xml
        if ($LASTEXITCODE -ne 0) { Fail "创建任务 $name 失败：$r" } else { Ok "任务 $name 已创建" }
    }
}

# ── 5. 仍需人工的事 ──
Write-Host ""
Write-Host '========== 仍需人工完成 ==========' -ForegroundColor Yellow
$credId = [string]$subst['@@SVN_CREDENTIALS_ID@@']
$hasCred = $false
try {
    $creds = JGet '/credentials/store/system/domain/_/api/json?tree=credentials[id]'
    $hasCred = @($creds.credentials | Where-Object { $_.id -eq $credId }).Count -gt 0
} catch {}
if ($hasCred) {
    Ok "SVN 凭据 '$credId' 已存在"
} else {
    Warn "SVN 凭据 '$credId' 不存在 —— client-dev 会因此检出失败。"
    Write-Host "  请自行创建（密码不经他人之手）：" -ForegroundColor Yellow
    Write-Host "    $jUrl/credentials/store/system/domain/_/newCredentials" -ForegroundColor Yellow
    Write-Host "    Kind=Username with password   ID 必须精确等于：$credId" -ForegroundColor Yellow
    Write-Host "  若你已用别的 ID，改 .env 的 SVN_CREDENTIALS_ID 后重跑本脚本即可。" -ForegroundColor Yellow
}
Write-Host ""
Write-Host '打包前置（客户端流水线需要，可用 preflight 自检）：' -ForegroundColor DarkGray
Write-Host '  pwsh tools/devops/preflight-buildmachine.ps1 -ClientRepo <客户端工作副本>' -ForegroundColor DarkGray
Write-Host ''
Write-Host "Jenkins: $jUrl  （$jUser / 见 .env）" -ForegroundColor Green
