# Pandora 压测 prom 快照批量抓取(stress_snap.ps1)
#
# 用途(对齐 docs/design/stress-discipline.md §4.2):压测过程中,在指定的若干"分钟时刻"
#   并行拉各服务 /metrics 端口快照,落到 <RunDir>/prom-snapshots/t<N>m_<svc>.txt,
#   供 stress_summarize.ps1 出二维表。替代手 curl 单端口的临时抓取。
#
# 设计:
#   - 至少 3 次快照(ramp 完成 / 稳态中段 / 稳态末),由 -Stages 指定分钟数。
#   - StartTime = 压测稳态计时起点;脚本按 StartTime + N 分钟在每个时刻抓一轮。
#   - 只读 HTTP GET /metrics,不修改任何状态;纯观测,安全可重复。
#
# 用法:
#   pwsh tools/scripts/stress_snap.ps1 `
#     -RunDir robot/logs/stress-single-cell-40w-20260610 `
#     -StartTime '2026-06-10 14:00:00' `
#     -Stages 2,5,10,15,18
#
#   # 立即抓一次(调试 / 冒烟):
#   pwsh tools/scripts/stress_snap.ps1 -RunDir robot/logs/smoke -Stages 0

param(
    [Parameter(Mandatory = $true)]
    [string]$RunDir,

    # 稳态计时起点;省略则以脚本启动时刻为基准。
    [string]$StartTime,

    # 抓取时刻(分钟,相对 StartTime)。0 表示立即抓一次。
    # 兼容两种调用:
    #   -Stages 0,1,2      (同一 PowerShell 进程里通常绑定为数组)
    #   -Stages "0,1,2"    (经 pwsh -File 外层转发时常见)
    [object[]]$Stages = @(2, 5, 10, 15, 18),

    # 服务端口表:后缀 => metrics 端口(对齐 stress-discipline.md §3 + infra.md §6.2)。
    [hashtable]$Ports = @{ login = 21001; match = 21011; ds = 21020; battle = 21022 }
)

$ErrorActionPreference = "Stop"

$SnapDir = Join-Path $RunDir "prom-snapshots"
New-Item -ItemType Directory -Force -Path $SnapDir | Out-Null

$StageMinutes = @()
foreach ($stage in $Stages) {
    if ($null -eq $stage) { continue }
    foreach ($part in ($stage.ToString() -split ",")) {
        $trimmed = $part.Trim()
        if ($trimmed -eq "") { continue }
        $StageMinutes += [int]$trimmed
    }
}
if ($StageMinutes.Count -eq 0) {
    throw "Stages 不能为空"
}

if ($StartTime) {
    $base = [datetime]::Parse($StartTime)
} else {
    $base = Get-Date
}

function Invoke-Snapshot([int]$minute) {
    Write-Host ("[t{0}m] 抓取 prom 快照 -> {1}" -f $minute, $SnapDir) -ForegroundColor Cyan
    $jobs = @()
    foreach ($svc in $Ports.Keys) {
        $port = $Ports[$svc]
        $outFile = Join-Path $SnapDir ("t{0}m_{1}.txt" -f $minute, $svc)
        $jobs += Start-Job -ScriptBlock {
            param($url, $out, $svcName, $minuteLabel)
            try {
                $resp = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 10
                Set-Content -Path $out -Value $resp.Content -Encoding UTF8
                "OK $svcName"
            } catch {
                # 端口不可达也落一个标记文件,summarize 据此显式区分"没抓到"与"指标为 0"。
                Set-Content -Path $out -Value "# SNAPSHOT_FAILED t$minuteLabel $svcName $($_.Exception.Message)" -Encoding UTF8
                "FAIL $svcName"
            }
        } -ArgumentList ("http://127.0.0.1:$port/metrics"), $outFile, $svc, $minute
    }
    $results = $jobs | Wait-Job | Receive-Job
    $jobs | Remove-Job
    foreach ($r in $results) {
        if ($r -like "FAIL*") {
            Write-Host ("  [--] {0}" -f $r) -ForegroundColor Yellow
        } else {
            Write-Host ("  [OK] {0}" -f $r) -ForegroundColor Green
        }
    }
}

# 立即抓一次模式。
if ($StageMinutes.Count -eq 1 -and $StageMinutes[0] -eq 0) {
    Invoke-Snapshot 0
    Write-Host "完成:单次快照已写入 $SnapDir" -ForegroundColor Green
    return
}

foreach ($m in ($StageMinutes | Sort-Object)) {
    $target = $base.AddMinutes($m)
    $wait = ($target - (Get-Date)).TotalSeconds
    if ($wait -gt 0) {
        Write-Host ("等待到 t{0}m({1:HH:mm:ss}),还有 {2:N0}s ..." -f $m, $target, $wait) -ForegroundColor DarkGray
        Start-Sleep -Seconds $wait
    }
    Invoke-Snapshot $m
}

Write-Host ""
Write-Host "全部快照完成。下一步:pwsh tools/scripts/stress_summarize.ps1 -RunDir $RunDir" -ForegroundColor Green
