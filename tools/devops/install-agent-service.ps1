#!/usr/bin/env pwsh
<#
.SYNOPSIS
  把 Jenkins 构建 agent 注册成开机自启的计划任务，让它在重启/掉线后自动恢复。

.DESCRIPTION
  为什么需要：直接用 Start-Process 拉起的 agent 是普通进程，
  会话结束、注销或重启后就没了，Jenkins 端表现为节点 offline、
  构建卡在 "Timeout waiting for agent to come back"（实际发生过）。

  本脚本注册一个计划任务：
    - 开机时自动启动（不需要有人登录）
    - 以当前用户身份运行 —— **这点很关键**：UE 引擎注册在 HKCU，
      agent 必须以注册过引擎的那个账号运行，否则打包时挑不到源码引擎，
      Server 目标会报 "Server targets are not currently supported"。
    - 进程意外退出后自动重启

  agent 密钥每个 Jenkins 实例独有，每次启动时从 Jenkins 现取，不落盘。

.EXAMPLE
  pwsh tools/devops/install-agent-service.ps1
  pwsh tools/devops/install-agent-service.ps1 -Uninstall
#>
[CmdletBinding()]
param(
    [string]$TaskName = 'PandoraJenkinsAgent',
    [switch]$Uninstall
)

$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot

function Info($m) { Write-Host "[agent] $m" -ForegroundColor Cyan }
function Ok($m)   { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }

if ($Uninstall) {
    if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
        Ok "已移除计划任务 $TaskName"
    } else { Warn "计划任务 $TaskName 不存在" }
    return
}

# ── 读配置 ──
$envFile = Join-Path $here '.env'
if (-not (Test-Path -LiteralPath $envFile)) { throw "缺少 $envFile，先跑 up.ps1。" }
$cfg = @{}
Get-Content -LiteralPath $envFile | ForEach-Object {
    if ($_ -match '^\s*([^#=]+)=(.*)$') { $cfg[$Matches[1].Trim()] = $Matches[2].Trim() }
}
function Cfg($k, $d) { if ($cfg.ContainsKey($k) -and $cfg[$k]) { $cfg[$k] } else { $d } }
$workDir = Cfg 'AGENT_WORKDIR' 'F:\jenkins-agent'
$jPort   = Cfg 'JENKINS_HTTP_PORT' '8080'

# ── 生成常驻启动脚本（每次启动现取密钥，不把密钥写死）──
$runner = Join-Path $workDir 'run-agent.ps1'
New-Item -ItemType Directory -Path $workDir -Force | Out-Null
$runnerBody = @"
# 由 install-agent-service.ps1 生成。每次启动时从 Jenkins 现取 agent 密钥。
# 密钥不写进本文件，也不进版本库。
`$ErrorActionPreference = 'Stop'
`$envFile = '$((Join-Path $here '.env'))'
`$cfg = @{}
Get-Content -LiteralPath `$envFile | ForEach-Object {
    if (`$_ -match '^\s*([^#=]+)=(.*)`$') { `$cfg[`$Matches[1].Trim()] = `$Matches[2].Trim() }
}
`$jUrl  = 'http://localhost:' + `$cfg['JENKINS_HTTP_PORT']
`$user  = `$cfg['JENKINS_ADMIN_ID']
`$pass  = `$cfg['JENKINS_ADMIN_PASSWORD']
`$work  = '$workDir'
`$name  = 'windows-host'
# 用 `${} 明确界定变量名：写成 "`$user:`$pass" 时 PowerShell 会把 user: 当作作用域限定符而解析失败
`$H = @{ Authorization = 'Basic ' + [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("`${user}:`${pass}")) }

# 等 Jenkins 起来（开机时容器可能还没就绪）
for (`$i = 0; `$i -lt 120; `$i++) {
    try { Invoke-WebRequest -Uri "`$jUrl/login" -UseBasicParsing -TimeoutSec 10 | Out-Null; break } catch { Start-Sleep -Seconds 10 }
}
Invoke-WebRequest -Uri "`$jUrl/jnlpJars/agent.jar" -OutFile "`$work\agent.jar" -Headers `$H -UseBasicParsing
`$r = Invoke-WebRequest -Uri "`$jUrl/computer/`$name/jenkins-agent.jnlp" -Headers `$H -UseBasicParsing
`$jnlp = if (`$r.Content -is [byte[]]) { [Text.Encoding]::UTF8.GetString(`$r.Content) } else { `$r.Content }
`$secret = ([regex]'<argument>([a-f0-9]{64})</argument>').Match(`$jnlp).Groups[1].Value
if (-not `$secret) { throw '取不到 agent 密钥' }

`$java = (Get-ChildItem 'C:\Program Files\Microsoft\jdk*','C:\Program Files\Eclipse Adoptium\jdk*' -Directory -ErrorAction SilentlyContinue |
          Sort-Object Name -Descending | Select-Object -First 1)
if (-not `$java) { throw '找不到 JDK' }
`$javaExe = Join-Path `$java.FullName 'bin\java.exe'

# 前台运行：进程退出即任务结束，由计划任务的重启策略拉起
& `$javaExe -jar "`$work\agent.jar" -url "`$jUrl/" -secret `$secret -name `$name -webSocket -workDir `$work
"@
[IO.File]::WriteAllText($runner, $runnerBody, (New-Object Text.UTF8Encoding($false)))
Ok "已生成启动脚本：$runner"

# ── 注册计划任务 ──
$pwshExe = (Get-Process -Id $PID).Path
$action  = New-ScheduledTaskAction -Execute $pwshExe -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$runner`""
$trigger = New-ScheduledTaskTrigger -AtStartup
# 以当前用户身份、最高权限运行；S4U 使得无人登录时也能跑，且保留 HKCU（引擎注册表在那）
$principal = New-ScheduledTaskPrincipal -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) -LogonType S4U -RunLevel Highest
$settings  = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
             -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew

if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Info "计划任务 $TaskName 已存在 → 更新"
    Set-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings | Out-Null
} else {
    Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings `
        -Description 'Pandora Jenkins 构建 agent：开机自启，掉线自动重启。以注册过 UE 引擎的账号运行（引擎在 HKCU）。' | Out-Null
    Ok "已注册计划任务 $TaskName"
}

Write-Host ""
Ok "配置完成。agent 将在开机时自动启动，异常退出后 1 分钟内自动重启。"
Write-Host "  立即启动： Start-ScheduledTask -TaskName $TaskName" -ForegroundColor DarkGray
Write-Host "  查看状态： Get-ScheduledTask -TaskName $TaskName | Get-ScheduledTaskInfo" -ForegroundColor DarkGray
Write-Host "  卸载：     pwsh tools/devops/install-agent-service.ps1 -Uninstall" -ForegroundColor DarkGray
