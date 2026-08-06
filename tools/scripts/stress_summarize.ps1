# Pandora 压测单轮汇总(stress_summarize.ps1)
#
# 用途(对齐 docs/design/stress-discipline.md §5):读 <RunDir> 下的 prom 快照与
#   robot-stats.jsonl,输出五段二维表到 <RunDir>/summary.txt 并打印。
#   纯读不改;不许手 grep raw prom —— 一律走本脚本出表。
#
# 五段表:
#   1. robot 每分钟 stats        (robot-stats.jsonl)
#   2. matchmaker 关键阶段        (t<N>m_match.txt)
#   3. ds_allocator 子阶段        (t<N>m_ds.txt)
#   4. battle_result 子阶段       (t<N>m_battle.txt)
#   5. 大厅 DS Replication         (DS prom;阶段 1 stub 模式无 DS → 标 N/A)
#
# 用法:
#   pwsh tools/scripts/stress_summarize.ps1 -RunDir robot/logs/stress-single-cell-40w-20260610
#   pwsh tools/scripts/stress_summarize.ps1 -RunDir <dir> -StatsFile robot/logs/robot-stats.jsonl

param(
    [Parameter(Mandatory = $true)]
    [string]$RunDir,

    # robot-stats.jsonl 路径;默认取 <RunDir>/robot-stats.jsonl,回退 robot/logs/robot-stats.jsonl。
    [string]$StatsFile
)

$ErrorActionPreference = "Stop"

$SnapDir = Join-Path $RunDir "prom-snapshots"
$OutFile = Join-Path $RunDir "summary.txt"
$lines = New-Object System.Collections.Generic.List[string]

function Emit([string]$s) { $lines.Add($s) | Out-Null; Write-Host $s }

# ---- prometheus 文本解析:histogram 分位 + counter 取值 ----
# 返回:@{ buckets=@{le=>cum}; count=N; sum=S } 按 metric 名归并(忽略 label 维度,聚合)。
function Parse-Prom([string]$path) {
    $fams = @{}
    if (-not (Test-Path $path)) { return $fams }
    $first = (Get-Content $path -TotalCount 1)
    if ($first -like "# SNAPSHOT_FAILED*") { return $null }  # 显式区分"没抓到"
    foreach ($raw in Get-Content $path) {
        $line = $raw.Trim()
        if ($line -eq "" -or $line.StartsWith("#")) { continue }
        # 形如:  name{labels} value   或   name value
        if ($line -notmatch '^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9eE\.\+\-]+)') { continue }
        $name = $Matches[1]
        $labels = $Matches[2]
        $val = [double]$Matches[3]

        if ($name.EndsWith("_bucket")) {
            $base = $name.Substring(0, $name.Length - "_bucket".Length)
            if (-not $fams.ContainsKey($base)) { $fams[$base] = @{ buckets = @{}; count = 0.0; sum = 0.0 } }
            if ($labels -match 'le="([^"]+)"') {
                $le = $Matches[1]
                if (-not $fams[$base].buckets.ContainsKey($le)) { $fams[$base].buckets[$le] = 0.0 }
                $fams[$base].buckets[$le] += $val
            }
        } elseif ($name.EndsWith("_count")) {
            $base = $name.Substring(0, $name.Length - "_count".Length)
            if (-not $fams.ContainsKey($base)) { $fams[$base] = @{ buckets = @{}; count = 0.0; sum = 0.0 } }
            $fams[$base].count += $val
        } elseif ($name.EndsWith("_sum")) {
            $base = $name.Substring(0, $name.Length - "_sum".Length)
            if (-not $fams.ContainsKey($base)) { $fams[$base] = @{ buckets = @{}; count = 0.0; sum = 0.0 } }
            $fams[$base].sum += $val
        } else {
            # 普通 gauge / counter:存到 _scalar 命名空间。
            if (-not $fams.ContainsKey($name)) { $fams[$name] = @{ buckets = @{}; count = 0.0; sum = 0.0; scalar = 0.0 } }
            $fams[$name].scalar += $val
        }
    }
    return $fams
}

# histogram 分位估计(累积桶定位)。
# 返回:有限桶边界 [double];若真分位落到 +Inf 溢出桶(超过最大有限桶),返回字符串 ">{top}"。
function Get-Quantile($fam, [double]$q) {
    if ($null -eq $fam -or $fam.buckets.Count -eq 0) { return $null }
    $total = $fam.count
    if ($total -le 0) { return $null }
    $target = $q * $total
    $finite = $fam.buckets.GetEnumerator() | Where-Object { $_.Key -ne "+Inf" } |
        Sort-Object { [double]$_.Key }
    foreach ($b in $finite) {
        if ($b.Value -ge $target) { return [double]$b.Key }
    }
    # 落到 +Inf 溢出桶:真分位 > 最大有限桶边界,histogram 没有更大桶,只能给下界。
    $top = ($finite | Select-Object -Last 1)
    if ($top) { return (">{0}" -f $top.Key) }
    return $null
}

function Format-Latency($fam) {
    if ($null -eq $fam) { return "  (未抓到)" }
    $avg = if ($fam.count -gt 0) { ($fam.sum / $fam.count) } else { 0 }
    $p50 = Get-Quantile $fam 0.50
    $p99 = Get-Quantile $fam 0.99
    $note = ""
    $finite = $fam.buckets.GetEnumerator() | Where-Object { $_.Key -ne "+Inf" } |
        Sort-Object { [double]$_.Key }
    $top = ($finite | Select-Object -Last 1)
    if ($top -and (($fam.count - $top.Value) -gt 0)) {
        # 顶桶之上有样本:说明 p99 只能给下界,显式标注溢出样本数(非超时/非错误,仅超出桶配置)。
        $note = ("  [顶桶 le={0}s 之上溢出 {1:N0} 样本]" -f $top.Key, ($fam.count - $top.Value))
    }
    return ("  count={0:N0}  avg={1:N4}s  p50={2}s  p99={3}s{4}" -f $fam.count, $avg, $p50, $p99, $note)
}

# Get-GrpcHandling 取聚合的全 method 时延 family;Parse-PromByMethod 拆 method。
function Get-GrpcHandling($fams) {
    if ($null -eq $fams) { return $null }
    foreach ($k in $fams.Keys) {
        if ($k -eq 'pandora_rpc_duration_seconds') { return $fams[$k] }
        if ($k -match '(grpc|rpc).*(handling|duration|latency)') { return $fams[$k] }
    }
    return $null
}

# Parse-PromByMethod 按 service/method 拆 pandora_rpc_duration_seconds histogram,
# 避免把「快的高频 RPC」和「慢的低频 RPC」混进一个聚合分位(否则 AllocateBattle 的
# 慢尾会被 GetMatchProgress 的海量快样本淹没,或反过来污染整体 p99)。
function Parse-PromByMethod([string]$path) {
    $m = @{}
    if (-not (Test-Path $path)) { return $m }
    $first = (Get-Content $path -TotalCount 1)
    if ($first -like "# SNAPSHOT_FAILED*") { return $null }
    foreach ($raw in Get-Content $path) {
        $line = $raw.Trim()
        if ($line -eq "" -or $line.StartsWith("#")) { continue }
        if ($line -notmatch '^pandora_rpc_duration_seconds(_bucket|_count|_sum)\{([^}]*)\}\s+([0-9eE\.\+\-]+)') { continue }
        $suffix = $Matches[1]
        $labels = $Matches[2]
        $val = [double]$Matches[3]
        $svc = if ($labels -match 'service="([^"]+)"') { $Matches[1] } else { "?" }
        $meth = if ($labels -match 'method="([^"]+)"') { $Matches[1] } else { "?" }
        $key = "$svc/$meth"
        if (-not $m.ContainsKey($key)) { $m[$key] = @{ buckets = @{}; count = 0.0; sum = 0.0 } }
        switch ($suffix) {
            "_bucket" {
                if ($labels -match 'le="([^"]+)"') {
                    $le = $Matches[1]
                    if (-not $m[$key].buckets.ContainsKey($le)) { $m[$key].buckets[$le] = 0.0 }
                    $m[$key].buckets[$le] += $val
                }
            }
            "_count" { $m[$key].count += $val }
            "_sum" { $m[$key].sum += $val }
        }
    }
    return $m
}

Emit "================ Pandora 压测汇总 (summary.txt) ================"
Emit ("RunDir : {0}" -f $RunDir)
Emit ("生成时间: {0}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"))
Emit ""

# ---------- 段 1:robot 每分钟 stats ----------
Emit "----- 段 1. robot 每分钟 stats (robot-stats.jsonl) -----"
if (-not $StatsFile) {
    $cand = Join-Path $RunDir "robot-stats.jsonl"
    $StatsFile = if (Test-Path $cand) { $cand } else { "robot/logs/robot-stats.jsonl" }
}
if (Test-Path $StatsFile) {
    Emit ("{0,-20} {1,8} {2,8} {3,8} {4,8} {5,8} {6,8} {7,8} {8,8} {9,7}" -f `
        "ts", "online", "loginOK", "loginF", "enq", "conf", "disp", "battle", "p99ms", "err")
    foreach ($l in Get-Content $StatsFile) {
        if ($l.Trim() -eq "") { continue }
        try { $r = $l | ConvertFrom-Json } catch { continue }
        Emit ("{0,-20} {1,8} {2,8} {3,8} {4,8} {5,8} {6,8} {7,8} {8,8:N1} {9,7}" -f `
            $r.ts, $r.vu_online, $r.login_ok, $r.login_fail, `
            $r.match_enqueue, $r.match_confirmed, $r.match_dispatched, `
            $r.battle_reported, $r.rpc_p99_ms, $r.errors)
    }
} else {
    Emit ("  (未找到 {0};robot 跑测后才会生成)" -f $StatsFile)
}
Emit ""

# ---------- 段 2-4:prom 快照各阶段 ----------
$sections = @(
    @{ Title = "段 2. matchmaker 关键阶段 (:21011)"; Svc = "match" },
    @{ Title = "段 3. ds_allocator 子阶段 (:21020)"; Svc = "ds" },
    @{ Title = "段 4. battle_result 子阶段 (:21022)"; Svc = "battle" }
)

foreach ($sec in $sections) {
    Emit ("----- {0} -----" -f $sec.Title)
    $snaps = @()
    if (Test-Path $SnapDir) {
        $snaps = Get-ChildItem $SnapDir -Filter ("t*m_{0}.txt" -f $sec.Svc) -ErrorAction SilentlyContinue |
            Sort-Object { [int]($_.BaseName -replace '^t(\d+)m_.*$', '$1') }
    }
    if ($snaps.Count -eq 0) {
        Emit "  (无快照;stress_snap.ps1 抓取后才有)"
        Emit ""
        continue
    }
    foreach ($snap in $snaps) {
        $minute = ($snap.BaseName -replace '^t(\d+)m_.*$', '$1')
        $fams = Parse-Prom $snap.FullName
        if ($null -eq $fams) {
            Emit ("  [t{0}m] 快照抓取失败(端口不可达)" -f $minute)
            continue
        }
        $grpc = Get-GrpcHandling $fams
        Emit ("  [t{0}m] 全 method 聚合:{1}" -f $minute, (Format-Latency $grpc))
        # 按 method 拆分(avg 降序),把慢 RPC 单独暴露,避免聚合分位误导。
        $byMethod = Parse-PromByMethod $snap.FullName
        if ($byMethod -and $byMethod.Count -gt 0) {
            $byMethod.GetEnumerator() |
                Sort-Object { -($_.Value.sum / [math]::Max($_.Value.count, 1)) } |
                ForEach-Object {
                    Emit ("      - {0,-42}{1}" -f $_.Key, (Format-Latency $_.Value))
                }
        }
    }
    Emit ""
}

# ---------- 段 5:大厅 DS Replication ----------
Emit "----- 段 5. 大厅 DS Replication (DS prom) -----"
$dsSnaps = @()
if (Test-Path $SnapDir) {
    $dsSnaps = Get-ChildItem $SnapDir -Filter "t*m_hubds.txt" -ErrorAction SilentlyContinue
}
if ($dsSnaps.Count -eq 0) {
    Emit "  N/A —— 阶段 1 stub 模式不起真 DS(ds_mode=stub),无 hub DS prom 端口。"
    Emit "       接真 DS(ds_mode=real)后抓 t<N>m_hubds.txt 再汇总本段。"
}
Emit ""

Emit "================ 汇总结束 ================"
Emit "提示:与 prev-summary.txt 二维对比后写 round-N-vs-N-1.md;没有 prev-summary.txt 不许声明性能提升。"

# 落盘。
New-Item -ItemType Directory -Force -Path $RunDir | Out-Null
Set-Content -Path $OutFile -Value ($lines -join "`n") -Encoding UTF8
Write-Host ""
Write-Host ("已写入 {0}" -f $OutFile) -ForegroundColor Green
