#requires -Version 7.0
<#
.SYNOPSIS
    策划机免 Docker 本机基础设施(MySQL / Redis / Kafka / Envoy)。

.DESCRIPTION
    背景:策划机不装 Docker Desktop(装不动、要 WSL2、开机慢、IT 不给管理员),
    但本机跑一整套后端需要 MySQL / Redis / Kafka / Envoy 四件套。本脚本用**免安装**
    的原生 Windows 二进制把这四件套跑成宿主进程,端口 / 账号 / 配置与
    deploy/docker-compose.dev.yml 完全一致,因此 services/*/etc/*-dev.yaml 一个字都不用改。

    与 docker 模式的对应关系(端口 / 账号必须一致,否则服务配置就得分叉):
      MySQL  127.0.0.1:3307   root/pandora_dev_root   pandora/pandora_dev_pwd
      Redis  127.0.0.1:6380   无密码,appendonly yes,maxmemory 1gb noeviction
      Kafka  127.0.0.1:9093   KRaft 单节点(不需要 ZooKeeper),自动建 topic,4 分区
      Envoy  0.0.0.0:8443(客户端面) / 127.0.0.1:8444(DS 面) / 127.0.0.1:9901(admin)

    **不含** TiDB / Prometheus / Grafana / Loki / etcd:
      - TiDB   :TiKV 没有可用的 Windows 原生部署;social 四服(friend/chat/guild/mail)
                 本模式改走 etc/*-dev.yaml 直连本机 MySQL 的 pandora_social 库
                 (run_services.ps1 -SocialOnMysql)。
      - 观测栈 :策划不看 Grafana,省 1GB 内存和一堆磁盘。
      - etcd   :dev 配置里 etcd_endpoints 全是注释状态(snowflake 走 static、
                 authority_mode 非 redis),本机单副本用不上。

    ⚠️ Envoy 用的是 1.28.0(2023 年最后一版官方 Windows 构建;上游 2023-08 关闭了
       Windows CI,之后没有官方 Windows 二进制)。**只允许用于本机 127.0.0.1 开发边缘**,
       内网 / k8s / 线上一律继续用 v1.38(deploy/k8s/infra/edge-envoy.yaml)。
       生产 envoy.yaml 里只有 1 个字段是 1.28 不认识的(见 $EnvoyDropFields),
       派生配置时精确剔除并跑 --mode validate 卡关:出现白名单以外的未知字段 → 直接失败,
       绝不"自动跳过不认识的字段"(那会让策划机静默跑在比线上更弱的鉴权上)。

.PARAMETER Action
    up        : 备料(缺什么下什么)+ 启动 + 健康检查(默认)
    down      : 停止所有本机基础设施进程(保留数据)
    status    : 打印各组件端口 / 进程状态
    provision : 只备料不启动(适合提前在共享盘上做好离线包)
    reset     : 停止并删除 data 目录(MySQL / Kafka / Redis 数据全清,下次 up 会重新初始化)

.PARAMETER Force
    provision / up 时强制重新下载并解包(默认已就位则跳过)。

.NOTES
    离线 / 内网分发:设置 PANDORA_LOCALINFRA_MIRROR 指向一个已放好压缩包的目录
    (本地目录或 UNC 共享),脚本会优先从那里拷,不走公网。文件名见 $Components 的 File 字段。
#>

[CmdletBinding()]
param(
    [ValidateSet('up', 'down', 'status', 'provision', 'reset')]
    [string]$Action = 'up',

    [switch]$Force
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'   # 不关的话 Invoke-WebRequest 下大文件慢十倍

$ProjectRoot = (Resolve-Path "$PSScriptRoot/../..").Path
$Root = Join-Path $ProjectRoot 'run/localinfra'
$DistDir = Join-Path $Root 'dist'      # 解包后的二进制
$DataDir = Join-Path $Root 'data'      # 各组件数据目录
$LogDir = Join-Path $Root 'logs'
$PidDir = Join-Path $Root 'pids'
$CfgDir = Join-Path $Root 'cfg'
$CacheDir = Join-Path $Root 'cache'     # 下载的压缩包

# ===== 与 docker-compose.dev.yml 对齐的端口 / 账号(改这里就得同步改 compose)=====
$MysqlPort = 3307
$RedisPort = 6380
$KafkaPort = 9093
$KafkaCtrlPort = 9094          # KRaft controller,仅本机内部
# Envoy admin 9901 由 deploy/envoy/envoy.yaml 自己声明,这里只负责把它的监听地址收敛到 127.0.0.1
$MysqlRootPwd = 'pandora_dev_root'
$MysqlUser = 'pandora'
$MysqlUserPwd = 'pandora_dev_pwd'

# ===== 组件清单 =====
#
# 每个组件都必须钉死【确定版本 + Sha256】,没有例外。原因不是"防下载出错"(那有 Content-Length
# 和解包失败兜着),而是**供应链**:这些包会在 100 台策划机上解开直接执行,而取包路径有三条 ——
# 公网 URL、任意可写的共享盘(PANDORA_LOCALINFRA_MIRROR)、本机 cache 目录。后两条完全不受
# HTTPS 保护,谁能写那个目录谁就能换掉一个必然被执行的二进制。所以校验放在 Get-Archive 里,
# 对三条路径一视同仁,校验不过绝不解包。
#
# Sha256 的来源必须是**上游权威值**,不能是"我下下来自己算的"(那样只是把第一次下到的东西
# 当成标准,中间人在第一次就成功的话照样写进常量)。2026-08-15 逐个核对如下:
#   mysql : 官方 https://cdn.mysql.com/archives/mysql-8.4/mysql-8.4.6-winx64.zip.md5
#           = 73022866eb641b8ea8b22b81f8be1694,与本机文件 MD5 一致(MySQL 归档只发 MD5 和
#           GPG .asc;这里用它作上游锚点,实际校验仍用下面的 SHA256)
#   redis : 官方 release notes 公布 SHA256 1A0741A8...980460,与下面一致
#   kafka : 官方 https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz.sha512
#           与本机文件 SHA512 逐位一致
#   jre   : Adoptium assets API 的 package.checksum = b8aa18fe...73ba,与下面一致
# 换版本时必须重新走一遍上述核对,不许直接把新算出来的哈希填进来。
$Components = [ordered]@{
    mysql = @{
        Version = '8.4.6'
        File    = 'mysql-8.4.6-winx64.zip'
        Urls    = @(
            'https://cdn.mysql.com/archives/mysql-8.4/mysql-8.4.6-winx64.zip'
            'https://dev.mysql.com/get/Downloads/MySQL-8.4/mysql-8.4.6-winx64.zip'
        )
        Sha256  = 'b6c152f9f3aaa7294eb47db698e47974d37b261bf3cab4f90dc1243bb5ecd204'
        Probe   = 'mysqld.exe'
    }
    redis = @{
        # redis-windows/redis-windows:上游 Redis 源码的 msys2 原生 Windows 构建,
        # 免安装、无服务、无 UAC。选 8.8.x 对齐 compose 的 redis:8.8.0-alpine —— 版本要紧:
        # 项目用到 PEXPIRE LT / ZADD GT|XX 这类 6.2+ 语义,老的 Redis 3.x Windows 移植版跑不了。
        Version = '8.8.1'
        File    = 'Redis-8.8.1-Windows-x64-msys2.zip'
        Urls    = @(
            'https://github.com/redis-windows/redis-windows/releases/download/8.8.1/Redis-8.8.1-Windows-x64-msys2.zip'
        )
        Sha256  = '1a0741a8f997a50ad7a32370e9ddf719ed3d5d87701324c57b7b34518b980460'
        Probe   = 'redis-server.exe'
    }
    kafka = @{
        # 3.9.x 对齐 compose 的 confluentinc/cp-kafka:7.9(= Kafka 3.9)。
        # 用 KRaft 模式,不起 ZooKeeper(compose 里那个 zookeeper 容器在本模式下不需要)。
        Version = '3.9.1'
        File    = 'kafka_2.13-3.9.1.tgz'
        Urls    = @(
            # 华为云 apache 镜像放国内,实测可用且带 Range;上游 archive.apache.org 作兜底。
            # 镜像站是第三方,正因如此下面的 Sha256 才是必须的 —— 它取自 apache 官方 .sha512
            # 对应的同一个文件,镜像站给的包对不上就会被拒。
            'https://mirrors.huaweicloud.com/apache/kafka/3.9.1/kafka_2.13-3.9.1.tgz'
            'https://archive.apache.org/dist/kafka/3.9.1/kafka_2.13-3.9.1.tgz'
        )
        Sha256  = 'dd4399816e678946cab76e3bd1686103555e69bc8f2ab8686cda71aa15bc31a3'
        Probe   = 'kafka-server-start.bat'
    }
    jre   = @{
        # Kafka 要 JVM。策划机不一定装 Java,也不能假设装了就是 17+,所以自带一份免安装 JRE21。
        # 地址必须钉到具体 release(jdk-21.0.12+8),**不能**用 /v3/binary/latest/... ——
        # latest 是会漂的:Adoptium 一发新补丁版,同一个 URL 就换了内容,校验和当场失效,
        # 而且各台机器按备料时间不同装到不同 JVM,出问题无法复现。
        Version = 'temurin-21.0.12+8'
        File    = 'OpenJDK21U-jre_x64_windows_hotspot_21.0.12_8.zip'
        Urls    = @(
            'https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.12%2B8/OpenJDK21U-jre_x64_windows_hotspot_21.0.12_8.zip'
        )
        Sha256  = 'b8aa18fef5edb69bee8618f99677d66d0873d22cb40d974c15ac9ffcdecf73ba'
        Probe   = 'java.exe'
    }
}

# mkcert 单独处理:上游发布的是**裸 exe**(不是压缩包),所以不走 $Components 的解包流程。
# 为什么要自动备料而不是让策划 winget install:Envoy 的 TLS 叶子证书必须本机签(SAN 要含本机
# 局域网 IP),这是免 Docker 模式唯一还剩的外部工具依赖。它就是个 4.7MB 单文件、不写注册表、
# 不装服务、不要 UAC(只要不跑 `mkcert -install`)—— 完全没有理由让 100 台策划机各装一遍。
# 备料到 run/localinfra/dist/mkcert 后由 Register-LocalToolPath 挂进**本进程** PATH,
# 不改机器的用户/系统 PATH(策划机环境保持干净,卸载 = 删目录)。
$MkcertVersion = 'v1.4.4'
$MkcertFile = 'mkcert-v1.4.4-windows-amd64.exe'
$MkcertUrls = @(
    'https://github.com/FiloSottile/mkcert/releases/download/v1.4.4/mkcert-v1.4.4-windows-amd64.exe'
)
# 钉死 sha256:裸 exe 没有 registry digest 那种内容寻址,又允许走 PANDORA_LOCALINFRA_MIRROR
# 共享盘,共享盘上放了什么本脚本无从判断 —— 不校验就等于让任何能写共享盘的人换掉一个
# 会被执行、且专门用来签 TLS 证书的二进制。取值:2026-08-15 实测下载 4,896,256 字节,
# 运行 `-version` 输出 v1.4.4。换版本时必须同步更新这三个常量。
$MkcertSha256 = 'd2660b50a9ed59eada480750561c96abc2ed4c9a38c6a24d93e30e0977631398'

# Envoy 单独处理:没有官方 Windows 压缩包,只能从 2023 年最后一版官方 Windows 镜像里取 exe。
# 走纯 HTTPS 的 registry API(不需要本机有 docker),digest 固定 = 内容寻址,下载后校验 sha256。
$EnvoyImageRepo = 'envoyproxy/envoy-windows'
$EnvoyImageTag = 'v1.28.0'
$EnvoyLayerDigest = 'sha256:bbfb444bc8bd3ee4d1e11cb10b82fbc9f101a57fb223b0315ce14abb4c1c5b7d'
$EnvoyExeInLayer = 'Files/Program Files/envoy/envoy.exe'

# Envoy 1.28 不认识、但对本机开发无功能影响的字段白名单。
# 每一条都必须写清「线上什么行为 / 本机退化成什么」,不写清楚的不许加。
$EnvoyDropFields = @{
    # local_ratelimit 的「超限时回 gRPC RESOURCE_EXHAUSTED 而不是 HTTP 429」开关(1.30+ 才有)。
    # 影响面:只有 Login 接口的限流响应码(阈值 50rps / burst 100),本机单人压根触发不到。
    'rate_limited_as_resource_exhausted' = '1.30+ 字段;本机退化为超限回 HTTP 429(线上仍是 RESOURCE_EXHAUSTED)'
}

# ===== 输出 =====
function Write-Step([string]$m) { Write-Host "[infra] $m" -ForegroundColor Cyan }
function Write-Ok([string]$m) { Write-Host "  [ OK ] $m" -ForegroundColor Green }
function Write-Warn2([string]$m) { Write-Host "  [WARN] $m" -ForegroundColor Yellow }
function Write-Err([string]$m) { Write-Host "  [ERR ] $m" -ForegroundColor Red }

function Fail([string]$m) {
    Write-Err $m
    exit 1
}

# ===== 通用工具 =====

function Test-PortOpen([int]$Port) {
    $c = [System.Net.Sockets.TcpClient]::new()
    try {
        $iar = $c.BeginConnect('127.0.0.1', $Port, $null, $null)
        if (-not $iar.AsyncWaitHandle.WaitOne(300)) { return $false }
        $c.EndConnect($iar)
        return $true
    } catch { return $false } finally { $c.Dispose() }
}

function Get-PidFile([string]$Name) { Join-Path $PidDir "$Name.pid" }

function Get-RunningProcess([string]$Name) {
    $f = Get-PidFile $Name
    if (-not (Test-Path -LiteralPath $f)) { return $null }
    $procId = 0
    if (-not [int]::TryParse((Get-Content -LiteralPath $f -Raw).Trim(), [ref]$procId)) { return $null }
    return Get-Process -Id $procId -ErrorAction SilentlyContinue
}

function Stop-Component([string]$Name) {
    $p = Get-RunningProcess $Name
    if ($p) {
        # /T:kafka 是 cmd.exe 拉起 java 的父子结构,只杀父进程会留下孤儿 java 占着 9093。
        & taskkill.exe /PID $p.Id /T /F 2>&1 | Out-Null
        Write-Ok "$Name 已停止 (PID $($p.Id))"
    }
    Remove-Item -LiteralPath (Get-PidFile $Name) -Force -ErrorAction SilentlyContinue
}

function Save-Pid([string]$Name, [int]$ProcessId) {
    Set-Content -LiteralPath (Get-PidFile $Name) -Value $ProcessId -Encoding ascii
}

function Wait-Port([string]$Name, [int]$Port, [int]$TimeoutSec, [System.Diagnostics.Process]$Proc) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if ($Proc -and $Proc.HasExited) {
            Fail "$Name 启动后立即退出 (exit $($Proc.ExitCode))。日志: $(Join-Path $LogDir "$Name.log")"
        }
        if (Test-PortOpen $Port) { return }
        Start-Sleep -Milliseconds 500
    }
    Fail "$Name 在 ${TimeoutSec}s 内没有监听 :$Port。日志: $(Join-Path $LogDir "$Name.log")"
}

function Get-RemoteFile {
    <#
      断点续传下载。这不是"以防万一"的复杂化 —— 实测从国内拉 260MB 的 MySQL 包,
      连接会在中途被掐(Received an unexpected EOF),不续传就等于永远下不完。
      另外必须带 User-Agent:dev.mysql.com 对空 UA 直接回 403。
    #>
    param(
        [Parameter(Mandatory)][string]$Uri,
        [Parameter(Mandatory)][string]$OutFile,
        [hashtable]$Headers = @{},
        [int]$MaxAttempts = 12
    )
    $tmp = "$OutFile.part"
    $client = [System.Net.Http.HttpClient]::new()
    try {
        # 单次尝试封顶 5 分钟:HttpClient.Timeout 覆盖整个读流过程,设成"够大"(如 30 分钟)
        # 意味着链路半死不活时会静默卡半小时,策划只会看到一个不动的窗口。因为有断点续传,
        # 到点掐掉再续上没有任何损失 —— 这是把无界等待收敛成有界,不是靠 sleep 掩盖时序。
        $client.Timeout = [TimeSpan]::FromMinutes(5)
        $attempt = 0
        $lastPct = -1
        while ($true) {
            $attempt++
            $have = if (Test-Path -LiteralPath $tmp) { (Get-Item -LiteralPath $tmp).Length } else { 0L }
            try {
                $req = [System.Net.Http.HttpRequestMessage]::new('GET', $Uri)
                $req.Headers.TryAddWithoutValidation('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)') | Out-Null
                foreach ($k in $Headers.Keys) { $req.Headers.TryAddWithoutValidation($k, $Headers[$k]) | Out-Null }
                if ($have -gt 0) { $req.Headers.Range = [System.Net.Http.Headers.RangeHeaderValue]::new($have, $null) }

                $resp = $client.SendAsync($req, [System.Net.Http.HttpCompletionOption]::ResponseHeadersRead).GetAwaiter().GetResult()
                if (-not $resp.IsSuccessStatusCode) { throw "HTTP $([int]$resp.StatusCode) $($resp.ReasonPhrase)" }

                # 服务端不支持 Range(回 200 而不是 206)时只能从头下,否则会把整包又追加一遍。
                $resume = ($have -gt 0 -and [int]$resp.StatusCode -eq 206)
                if (-not $resume) { $have = 0L; Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue }

                $total = if ($resp.Content.Headers.ContentLength) { $have + $resp.Content.Headers.ContentLength } else { 0L }
                $src = $resp.Content.ReadAsStreamAsync().GetAwaiter().GetResult()
                $mode = if ($resume) { [System.IO.FileMode]::Append } else { [System.IO.FileMode]::Create }
                $dst = [System.IO.FileStream]::new($tmp, $mode, [System.IO.FileAccess]::Write)
                try {
                    $buf = New-Object byte[] 1048576
                    $done = $have
                    while (($n = $src.Read($buf, 0, $buf.Length)) -gt 0) {
                        $dst.Write($buf, 0, $n)
                        $done += $n
                        if ($total -gt 0) {
                            $pct = [int](($done * 100) / $total)
                            if ($pct -ge $lastPct + 10) {
                                $lastPct = $pct
                                Write-Host ("    下载中 {0}%  ({1:N0}/{2:N0} MB)" -f $pct, ($done / 1MB), ($total / 1MB))
                            }
                        }
                    }
                } finally { $dst.Dispose(); $src.Dispose() }

                Move-Item -LiteralPath $tmp -Destination $OutFile -Force
                return
            } catch {
                $now = if (Test-Path -LiteralPath $tmp) { (Get-Item -LiteralPath $tmp).Length } else { 0L }
                if ($attempt -ge $MaxAttempts) { throw }
                # 本轮拿到新字节就不算一次"失败尝试",避免慢但可用的链路被误判成不可用。
                if ($now -gt $have) { $attempt-- }
                Write-Warn2 "下载中断($($_.Exception.Message)),已拿到 $([math]::Round($now/1MB))MB,续传重试(剩余 $($MaxAttempts - $attempt) 次)"
                Start-Sleep -Seconds 3
            }
        }
    } finally { $client.Dispose() }
}

function Test-FileSha256 {
    <# 文件哈希是否等于期望值(大小写无关)。文件不存在返回 $false。#>
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Expected
    )
    if (-not (Test-Path -LiteralPath $Path)) { return $false }
    $actual = (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
    return ($actual -eq $Expected.ToLowerInvariant())
}

function Get-Archive {
    <# 备料一个压缩包到 cache,并保证返回的文件 sha256 == $Sha256。

       三条取包路径(本机 cache / 离线镜像共享盘 / 公网 URL)统一在这里校验,少校验任何一条
       都等于留了个后门:cache 和共享盘目录都是普通可写目录,不受 HTTPS 保护,而这些包解开
       就直接执行。校验不过的文件一律删掉,绝不返回给调用方解包。 #>
    param(
        [Parameter(Mandatory)][string]$File,
        [Parameter(Mandatory)][string[]]$Urls,
        [Parameter(Mandatory)][string]$Sha256,
        [hashtable]$Headers = @{}
    )
    $dest = Join-Path $CacheDir $File

    # ① 本机 cache:命中也要校验。上次下到一半、磁盘坏块、或者有人手工往 cache 里塞了个包,
    #    都会在这里被挡下;删掉重新走正常流程,不要求人工介入。
    if (Test-Path -LiteralPath $dest) {
        if (-not $Force -and (Test-FileSha256 -Path $dest -Expected $Sha256)) { return $dest }
        if (-not $Force) { Write-Warn2 "缓存的 $File 校验不通过(已损坏或被替换),删除后重新获取。" }
        Remove-Item -LiteralPath $dest -Force -ErrorAction SilentlyContinue
    }

    # ② 离线镜像共享盘。校验不过**直接失败,不静默回退公网** —— 共享盘上的包对不上号是必须
    #    有人去看一眼的事(放错版本 or 被人换了),偷偷改从公网下会让 100 台机器各自默默绕过它,
    #    既掩盖了问题又白费了共享盘的意义。
    $mirror = $env:PANDORA_LOCALINFRA_MIRROR
    if ($mirror) {
        $src = Join-Path $mirror $File
        if (Test-Path -LiteralPath $src) {
            Write-Host "    从离线镜像拷贝: $src"
            Copy-Item -LiteralPath $src -Destination $dest -Force
            if (Test-FileSha256 -Path $dest -Expected $Sha256) { return $dest }
            Remove-Item -LiteralPath $dest -Force -ErrorAction SilentlyContinue
            Fail "离线镜像里的 $File 校验不通过(期望 sha256 $Sha256)。请找后端同学确认 $mirror 上这个文件,确认前不要绕过。"
        }
    }

    # ③ 公网,按顺序试。某个源给的包对不上就换下一个源,全都不行才报错。
    $lastErr = $null
    foreach ($u in $Urls) {
        Write-Host "    下载 $File  <- $u"
        try {
            Get-RemoteFile -Uri $u -OutFile $dest -Headers $Headers
            if (Test-FileSha256 -Path $dest -Expected $Sha256) { return $dest }
            $actual = (Get-FileHash -LiteralPath $dest -Algorithm SHA256).Hash.ToLowerInvariant()
            Write-Warn2 "该地址下到的 $File 校验不通过(期望 $Sha256,实际 $actual),换下一个源。"
            Remove-Item -LiteralPath $dest -Force -ErrorAction SilentlyContinue
            $lastErr = [Exception]::new("sha256 不匹配($u)")
        } catch {
            $lastErr = $_.Exception
            Write-Warn2 "该地址不可用:$($_.Exception.Message)"
            # 换源必须丢掉半截文件:不同源的字节偏移不保证一致,续传会拼出坏包。
            Remove-Item -LiteralPath "$dest.part" -Force -ErrorAction SilentlyContinue
        }
    }
    throw "所有下载地址都失败或校验不通过($File):$($lastErr.Message)。可让后端同学把压缩包放到共享盘,再设 PANDORA_LOCALINFRA_MIRROR 指过去。"
}

function Expand-Archive2 {
    <# 用 Windows 自带 bsdtar 解包(zip / tgz 通吃,比 Expand-Archive 快一个数量级)。#>
    param(
        [Parameter(Mandatory)][string]$Archive,
        [Parameter(Mandatory)][string]$Destination
    )
    New-Item -ItemType Directory -Force -Path $Destination | Out-Null
    & tar.exe -x -f $Archive -C $Destination
    if ($LASTEXITCODE -ne 0) { throw "解包失败: $Archive (tar exit=$LASTEXITCODE)" }
}

function Find-Tool([string]$Component, [string]$Exe) {
    $dir = Join-Path $DistDir $Component
    if (-not (Test-Path -LiteralPath $dir)) { return $null }
    $hit = Get-ChildItem -LiteralPath $dir -Recurse -File -Filter $Exe -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($hit) { return $hit.FullName }
    return $null
}

function Register-LocalToolPath {
    <# 把自带工具目录挂进**本进程** PATH,让 envoy_cert.ps1 里的 `Get-Command mkcert` 能命中。
       只改 $env:PATH,不碰用户 / 系统环境变量 —— 策划机环境保持干净,卸载 = 删 run/localinfra。
       放在前面(prepend)是刻意的:本机若真装过老版本 mkcert,也以我们钉死版本的这份为准。 #>
    $mkcertDir = Join-Path $DistDir 'mkcert'
    if (-not (Test-Path -LiteralPath $mkcertDir)) { return }
    $parts = $env:PATH -split ';'
    if ($parts -notcontains $mkcertDir) { $env:PATH = "$mkcertDir;$env:PATH" }
}

# ===== 备料 =====

function Invoke-ProvisionAll {
    New-Item -ItemType Directory -Force -Path $DistDir, $DataDir, $LogDir, $PidDir, $CfgDir, $CacheDir | Out-Null

    if (-not (Get-Command tar.exe -ErrorAction SilentlyContinue)) {
        Fail ' 本机没有 tar.exe(Windows 10 1803+ 自带)。请升级系统或手工解包到 run/localinfra/dist/。'
    }

    foreach ($name in $Components.Keys) {
        $c = $Components[$name]
        $have = Find-Tool $name $c.Probe
        if ($have -and -not $Force) {
            # 已解包就跳过。校验的边界划在「取包」这一步:凡是从 cache / 共享盘 / 公网拿进来的
            # 压缩包,解包前一定过 sha256。已经解开的 dist 目录不再逐文件复验 —— 能改那里的人
            # 同样能改本脚本自身,再验一遍并不提高安全性,只会让每次启动多跑几秒哈希。
            Write-Ok "$name $($c.Version) 已就位"
            continue
        }
        Write-Step "备料 $name $($c.Version)"
        $ar = Get-Archive -File $c.File -Urls $c.Urls -Sha256 $c.Sha256
        $target = Join-Path $DistDir $name
        Remove-Item -LiteralPath $target -Recurse -Force -ErrorAction SilentlyContinue
        Expand-Archive2 -Archive $ar -Destination $target
        if (-not (Find-Tool $name $c.Probe)) {
            Fail "$name 解包后找不到 $($c.Probe),压缩包结构可能变了: $ar"
        }
        Write-Ok "$name $($c.Version) 备料完成"
    }

    Confirm-MkcertBinary
    Confirm-EnvoyBinary
}

function Confirm-MkcertBinary {
    <# 备料 mkcert(裸 exe)。已装了系统级 mkcert 也照样备一份自己的:版本钉死,
       免得各机器 mkcert 版本不一导致签出来的证书行为有差。 #>
    $dir = Join-Path $DistDir 'mkcert'
    $exe = Join-Path $dir 'mkcert.exe'
    if ((Test-Path -LiteralPath $exe) -and -not $Force) {
        Register-LocalToolPath
        Write-Ok "mkcert $MkcertVersion 已就位"
        return
    }
    Write-Step "备料 mkcert $MkcertVersion(签 Envoy 本机 TLS 证书用,策划机不用自己装)"
    # 校验在 Get-Archive 里做(cache / 共享盘 / 公网三条路径统一过一道),拿到就是对的。
    $src = Get-Archive -File $MkcertFile -Urls $MkcertUrls -Sha256 $MkcertSha256
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    Copy-Item -LiteralPath $src -Destination $exe -Force
    Register-LocalToolPath
    Write-Ok "mkcert $MkcertVersion 备料完成"
}

function Confirm-EnvoyBinary {
    $exe = Join-Path $DistDir 'envoy/envoy.exe'
    if ((Test-Path -LiteralPath $exe) -and -not $Force) {
        Write-Ok "envoy $EnvoyImageTag 已就位"
        return
    }
    Write-Step "备料 envoy $EnvoyImageTag(从官方 Windows 镜像层取 exe,不需要本机装 docker)"

    # registry 的 blob digest 就是内容的 sha256,直接当期望值交给 Get-Archive 统一校验。
    # 拿 token 要先看 cache / 共享盘有没有:已经有包的机器没必要再跑一趟 docker registry。
    $blobName = "envoy-windows-$EnvoyImageTag-layer.tar.gz"
    $blobFile = Join-Path $CacheDir $blobName
    $wantSha = ($EnvoyLayerDigest -replace '^sha256:', '')
    $headers = @{}
    $needNetwork = $Force -or -not (Test-FileSha256 -Path $blobFile -Expected $wantSha)
    if ($needNetwork -and -not ($env:PANDORA_LOCALINFRA_MIRROR -and (Test-Path -LiteralPath (Join-Path $env:PANDORA_LOCALINFRA_MIRROR $blobName)))) {
        $tokUri = "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${EnvoyImageRepo}:pull"
        $tok = (Invoke-RestMethod -Uri $tokUri -TimeoutSec 60).token
        if (-not $tok) { Fail '拿不到 docker registry 匿名 token,检查网络 / 代理。' }
        $headers = @{ Authorization = "Bearer $tok" }
    }
    $blobFile = Get-Archive -File $blobName `
        -Urls @("https://registry-1.docker.io/v2/$EnvoyImageRepo/blobs/$EnvoyLayerDigest") `
        -Sha256 $wantSha -Headers $headers

    $stage = Join-Path $CacheDir 'envoy-stage'
    Remove-Item -LiteralPath $stage -Recurse -Force -ErrorAction SilentlyContinue
    New-Item -ItemType Directory -Force -Path $stage | Out-Null
    & tar.exe -x -z -f $blobFile -C $stage $EnvoyExeInLayer
    if ($LASTEXITCODE -ne 0) { Fail "从镜像层解出 envoy.exe 失败 (tar exit=$LASTEXITCODE)" }

    $src = Join-Path $stage $EnvoyExeInLayer
    if (-not (Test-Path -LiteralPath $src)) { Fail "镜像层里没有 $EnvoyExeInLayer" }
    New-Item -ItemType Directory -Force -Path (Join-Path $DistDir 'envoy') | Out-Null
    Move-Item -LiteralPath $src -Destination $exe -Force
    Remove-Item -LiteralPath $stage -Recurse -Force -ErrorAction SilentlyContinue
    Write-Ok "envoy $EnvoyImageTag 备料完成"
}

# ===== MySQL =====

function Get-MysqlIniPath { Join-Path $CfgDir 'my.ini' }

function New-MysqlIni([string]$BaseDir) {
    $data = (Join-Path $DataDir 'mysql') -replace '\\', '/'
    $base = $BaseDir -replace '\\', '/'
    $errlog = (Join-Path $LogDir 'mysql.log') -replace '\\', '/'
    # sql_mode 必须含 STRICT_TRANS_TABLES:pkg/dbguard.AssertStrictMode 启动即断言,
    # 非严格模式下超长写入会被静默截断(CLAUDE.md §9.24)。8.4 默认就是严格,这里显式钉死。
    # mysqlx=OFF:免安装版默认还会开 33060,本机没人用,省一个端口和一份内存。
    @"
# 由 tools/scripts/local_infra.ps1 生成,勿手改(改了下次 up 会被覆盖)。
[mysqld]
basedir=$base
datadir=$data
port=$MysqlPort
bind-address=127.0.0.1
mysqlx=OFF
log-error=$errlog
skip-name-resolve
character-set-server=utf8mb4
collation-server=utf8mb4_0900_ai_ci
max_connections=500
sql_mode=ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION
innodb_buffer_pool_size=512M
innodb_flush_log_at_trx_commit=2
"@ | Set-Content -LiteralPath (Get-MysqlIniPath) -Encoding utf8NoBOM
}

function Invoke-MysqlSql {
    <# 用免安装版自带的 mysql.exe 执行 SQL(替代 docker exec)。#>
    param(
        [Parameter(Mandatory)][string]$Sql,
        [string]$User = 'root',
        [string]$Password = '',
        [string]$Database = ''
    )
    $mysql = Find-Tool 'mysql' 'mysql.exe'
    if (-not $mysql) { Fail '找不到 mysql.exe,先跑 -Action provision。' }
    $cliArgs = @('--protocol=TCP', '--host=127.0.0.1', "--port=$MysqlPort", "--user=$User", '--default-character-set=utf8mb4', '--batch', '--silent')
    if ($Database) { $cliArgs += "--database=$Database" }
    # 密码走 MYSQL_PWD 环境变量,不进命令行(命令行密码会出现在进程列表里)。
    $old = $env:MYSQL_PWD
    try {
        $env:MYSQL_PWD = $Password
        $out = $Sql | & $mysql @cliArgs 2>&1
        if ($LASTEXITCODE -ne 0) { throw "mysql 执行失败: $($out -join "`n")" }
        return $out
    } finally { $env:MYSQL_PWD = $old }
}

function Start-LocalMysql {
    $mysqld = Find-Tool 'mysql' 'mysqld.exe'
    if (-not $mysqld) { Fail '找不到 mysqld.exe,先跑 -Action provision。' }
    $baseDir = Split-Path -Parent (Split-Path -Parent $mysqld)   # <dist>/mysql/mysql-8.4.6-winx64
    $dataMysql = Join-Path $DataDir 'mysql'

    if (Test-PortOpen $MysqlPort) {
        Write-Ok "MySQL :$MysqlPort 已在运行"
        return
    }

    New-MysqlIni -BaseDir $baseDir

    $firstRun = -not (Test-Path -LiteralPath (Join-Path $dataMysql 'mysql'))
    if ($firstRun) {
        Write-Step "首次初始化 MySQL 数据目录(约 30s,只有第一次)"
        Remove-Item -LiteralPath $dataMysql -Recurse -Force -ErrorAction SilentlyContinue
        New-Item -ItemType Directory -Force -Path $dataMysql | Out-Null
        # --initialize-insecure:建出空密码的 root@localhost(不是过期密码),后面立刻改掉。
        $p = Start-Process -FilePath $mysqld -ArgumentList "--defaults-file=$(Get-MysqlIniPath)", '--initialize-insecure' `
            -WindowStyle Hidden -PassThru -Wait
        if ($p.ExitCode -ne 0) {
            Fail "MySQL 初始化失败 (exit $($p.ExitCode))。日志: $(Join-Path $LogDir 'mysql.log')"
        }
    }

    Write-Step "启动 MySQL :$MysqlPort"
    $proc = Start-Process -FilePath $mysqld -ArgumentList "--defaults-file=$(Get-MysqlIniPath)" `
        -WindowStyle Hidden -PassThru
    Save-Pid 'mysql' $proc.Id
    Wait-Port -Name 'mysql' -Port $MysqlPort -TimeoutSec 90 -Proc $proc

    if ($firstRun) {
        Write-Step '创建 root 密码与 pandora 账号(对齐 compose 的 MYSQL_* 环境变量)'
        # compose 里 mysql 镜像用 MYSQL_ROOT_PASSWORD / MYSQL_USER / MYSQL_PASSWORD 建号;
        # 免安装版没有这套 entrypoint,这里手工等价补齐。库和授权仍由 mysql-init/01 负责。
        $sql = @"
ALTER USER 'root'@'localhost' IDENTIFIED BY '$MysqlRootPwd';
CREATE USER IF NOT EXISTS '$MysqlUser'@'%' IDENTIFIED BY '$MysqlUserPwd';
CREATE USER IF NOT EXISTS '$MysqlUser'@'localhost' IDENTIFIED BY '$MysqlUserPwd';
FLUSH PRIVILEGES;
"@
        Invoke-MysqlSql -Sql $sql -User 'root' -Password '' | Out-Null
        Write-Ok 'MySQL 账号就绪'
    }
    Write-Ok "MySQL :$MysqlPort"
}

# ===== Redis =====

function Start-LocalRedis {
    if (Test-PortOpen $RedisPort) {
        Write-Ok "Redis :$RedisPort 已在运行"
        return
    }
    $exe = Find-Tool 'redis' 'redis-server.exe'
    if (-not $exe) { Fail '找不到 redis-server.exe,先跑 -Action provision。' }
    $dataRedis = Join-Path $DataDir 'redis'
    New-Item -ItemType Directory -Force -Path $dataRedis | Out-Null

    Write-Step "启动 Redis :$RedisPort"
    # 参数逐条对齐 compose 的 redis command:appendonly yes / maxmemory 1gb /
    # maxmemory-policy noeviction(本实例承载会话权威,LRU 驱逐 = 静默放行旧会话)。
    $cliArgs = @(
        '--port', "$RedisPort"
        '--bind', '127.0.0.1'
        '--appendonly', 'yes'
        '--dir', $dataRedis
        '--maxmemory', '1gb'
        '--maxmemory-policy', 'noeviction'
        '--logfile', (Join-Path $LogDir 'redis.log')
    )
    $proc = Start-Process -FilePath $exe -ArgumentList $cliArgs -WorkingDirectory (Split-Path -Parent $exe) `
        -WindowStyle Hidden -PassThru
    Save-Pid 'redis' $proc.Id
    Wait-Port -Name 'redis' -Port $RedisPort -TimeoutSec 30 -Proc $proc
    Write-Ok "Redis :$RedisPort"
}

# ===== Kafka(KRaft 单节点,不需要 ZooKeeper)=====

function Get-KafkaHome {
    $bat = Find-Tool 'kafka' 'kafka-server-start.bat'
    if (-not $bat) { return $null }
    return (Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $bat)))  # <...>/kafka_2.13-3.9.1
}

function New-KafkaProperties {
    $logs = (Join-Path $DataDir 'kafka') -replace '\\', '/'
    New-Item -ItemType Directory -Force -Path (Join-Path $DataDir 'kafka') | Out-Null
    # 单节点 KRaft:一个进程同时当 broker 和 controller。分区数 / 副本因子对齐 compose
    # (KAFKA_NUM_PARTITIONS=4,单副本),auto-create 打开 —— pkg/kafkax 的 topic 名是硬编码常量,
    # 靠自动建 topic 免掉一步初始化。
    @"
# 由 tools/scripts/local_infra.ps1 生成,勿手改。
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:$KafkaCtrlPort
listeners=PLAINTEXT://127.0.0.1:$KafkaPort,CONTROLLER://127.0.0.1:$KafkaCtrlPort
advertised.listeners=PLAINTEXT://127.0.0.1:$KafkaPort
inter.broker.listener.name=PLAINTEXT
controller.listener.names=CONTROLLER
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs=$logs
num.partitions=4
default.replication.factor=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=true
group.initial.rebalance.delay.ms=0
num.network.threads=3
num.io.threads=8
log.retention.hours=48
"@ | Set-Content -LiteralPath (Join-Path $CfgDir 'kafka.properties') -Encoding utf8NoBOM
}

function Start-LocalKafka {
    if (Test-PortOpen $KafkaPort) {
        Write-Ok "Kafka :$KafkaPort 已在运行"
        return
    }
    $home2 = Get-KafkaHome
    if (-not $home2) { Fail '找不到 kafka 发行包,先跑 -Action provision。' }
    $java = Find-Tool 'jre' 'java.exe'
    if (-not $java) { Fail '找不到自带 JRE,先跑 -Action provision。' }
    $javaHome = Split-Path -Parent (Split-Path -Parent $java)

    New-KafkaProperties
    $props = Join-Path $CfgDir 'kafka.properties'
    $meta = Join-Path $DataDir 'kafka/meta.properties'

    if (-not (Test-Path -LiteralPath $meta)) {
        Write-Step 'Kafka 首次格式化存储(KRaft)'
        $env:JAVA_HOME = $javaHome
        $uuid = & (Join-Path $home2 'bin/windows/kafka-storage.bat') random-uuid
        if ($LASTEXITCODE -ne 0 -or -not $uuid) { Fail 'kafka-storage random-uuid 失败' }
        $uuid = ($uuid | Select-Object -Last 1).Trim()
        $out = & (Join-Path $home2 'bin/windows/kafka-storage.bat') format -t $uuid -c $props 2>&1
        if ($LASTEXITCODE -ne 0) { Fail "kafka-storage format 失败: $($out -join "`n")" }
    }

    Write-Step "启动 Kafka :$KafkaPort"
    # 用 cmd /c 包一层:kafka-server-start.bat 自己会 spawn java。停的时候必须 taskkill /T,
    # 否则 java 变孤儿继续占着 9093(Stop-Component 已经带 /T)。
    $logFile = Join-Path $LogDir 'kafka.log'
    $psi = @{
        FilePath               = 'cmd.exe'
        ArgumentList           = @('/c', "`"$(Join-Path $home2 'bin/windows/kafka-server-start.bat')`"", "`"$props`"")
        WorkingDirectory       = $home2
        WindowStyle            = 'Hidden'
        PassThru               = $true
        RedirectStandardOutput = $logFile
        RedirectStandardError  = (Join-Path $LogDir 'kafka.err.log')
    }
    $env:JAVA_HOME = $javaHome
    $env:KAFKA_HEAP_OPTS = '-Xmx512M -Xms256M'   # 默认 1G,策划机省点内存
    $env:LOG_DIR = $LogDir
    $proc = Start-Process @psi
    Save-Pid 'kafka' $proc.Id
    Wait-Port -Name 'kafka' -Port $KafkaPort -TimeoutSec 120 -Proc $proc
    Write-Ok "Kafka :$KafkaPort"
}

# ===== Envoy =====

function New-LocalEnvoyConfig {
    <#
      从 deploy/envoy/envoy.yaml **派生**出本机版(唯一权威仍是 deploy/envoy/envoy.yaml,
      不许在仓库里再放第二份 yaml —— 两份必然漂移)。派生只做四件事:
        1. host.docker.internal -> 127.0.0.1(容器里才需要那个特殊域名)
        2. /etc/envoy/*.pem     -> deploy/envoy/ 下的真实路径
        3. 监听地址按 PANDORA_*_BIND_HOST 收敛(compose 是靠端口映射限制的,原生进程得自己限)
        4. 剔除 $EnvoyDropFields 里登记的、1.28 不认识的字段
      然后 --mode validate 卡关:任何白名单以外的报错一律硬失败。
    #>
    $srcPath = Join-Path $ProjectRoot 'deploy/envoy/envoy.yaml'
    if (-not (Test-Path -LiteralPath $srcPath)) { Fail "找不到 $srcPath" }

    $certPath = (Join-Path $ProjectRoot 'deploy/envoy/cert.pem') -replace '\\', '/'
    $keyPath = (Join-Path $ProjectRoot 'deploy/envoy/key.pem') -replace '\\', '/'
    foreach ($p in @($certPath, $keyPath)) {
        if (-not (Test-Path -LiteralPath $p)) { Fail "缺少 Envoy 证书 $p(应由 Confirm-EnvoyDevCert 自动签发,检查 mkcert 是否可用)。" }
    }

    # 客户端面默认 127.0.0.1;start.ps1 的 local 模式会导出 0.0.0.0 让手机 / 同事连。
    # DS 面 8444 没有玩家鉴权,默认恒绑本机(与 compose 的 PANDORA_DS_EDGE_BIND_HOST 语义一致)。
    $edgeHost = if ($env:PANDORA_EDGE_BIND_HOST) { $env:PANDORA_EDGE_BIND_HOST } else { '127.0.0.1' }
    $dsEdgeHost = if ($env:PANDORA_DS_EDGE_BIND_HOST) { $env:PANDORA_DS_EDGE_BIND_HOST } else { '127.0.0.1' }

    $lines = Get-Content -LiteralPath $srcPath
    $out = New-Object System.Collections.Generic.List[string]
    $dropped = @{}
    # 监听器区分:admin / pandora_listener / pandora_ds_listener 的 address 各自替换。
    # 状态机只吃「离我最近的一个 name/admin 标记」,yaml 里三块的 address 各只出现一次。
    $pending = 'admin'
    foreach ($line in $lines) {
        if ($line -match '^\s*-?\s*name:\s*pandora_listener\s*$') { $pending = 'edge' }
        elseif ($line -match '^\s*-?\s*name:\s*pandora_ds_listener\s*$') { $pending = 'ds' }

        $m = [regex]::Match($line, '^(?<i>\s*)address:\s*0\.0\.0\.0\s*$')
        if ($m.Success -and $pending) {
            $repl = switch ($pending) {
                'admin' { '127.0.0.1' }   # compose 里 9901 没有映射到宿主,原生进程必须自己收敛
                'edge' { $edgeHost }
                'ds' { $dsEdgeHost }
            }
            $out.Add("$($m.Groups['i'].Value)address: $repl")
            $pending = $null
            continue
        }

        # 1.28 不认识的字段:只剔白名单里的,别的一律留着让 validate 去炸。
        $isDropped = $false
        foreach ($f in $EnvoyDropFields.Keys) {
            if ($line -match "^\s*$([regex]::Escape($f))\s*:") {
                $dropped[$f] = $true
                $isDropped = $true
                break
            }
        }
        if ($isDropped) { continue }

        $line = $line -replace 'host\.docker\.internal', '127.0.0.1'
        $line = $line -replace '/etc/envoy/cert\.pem', $certPath
        $line = $line -replace '/etc/envoy/key\.pem', $keyPath
        $out.Add($line)
    }

    $dst = Join-Path $CfgDir 'envoy.yaml'
    $header = @(
        '# 【自动生成,勿手改】由 tools/scripts/local_infra.ps1 从 deploy/envoy/envoy.yaml 派生。'
        '# 唯一权威是 deploy/envoy/envoy.yaml;要改路由 / 鉴权请改那一份,本文件每次启动重新生成。'
        "# 本机 Envoy 版本 $EnvoyImageTag(官方最后一版 Windows 构建),剔除的字段见下:"
    )
    foreach ($f in $EnvoyDropFields.Keys) {
        $mark = if ($dropped.ContainsKey($f)) { '已剔除' } else { '未出现' }
        $header += "#   - $f  [$mark]  $($EnvoyDropFields[$f])"
    }
    Set-Content -LiteralPath $dst -Value (($header + $out) -join "`n") -Encoding utf8NoBOM
    return $dst
}

function Start-LocalEnvoy {
    $exe = Join-Path $DistDir 'envoy/envoy.exe'
    if (-not (Test-Path -LiteralPath $exe)) { Fail ' 找不到 envoy.exe,先跑 -Action provision。' }

    # 证书自愈:与 docker 模式共用 deploy/envoy/cert.pem —— 复用 dev_up.ps1 的同一套函数,
    # 不另写一份签发逻辑(签发规则、SAN 列表、共享 dev CA 只能有一处权威)。
    . "$PSScriptRoot/envoy_cert.ps1"
    Confirm-SharedDevCa -ProjectRoot $ProjectRoot | Out-Null
    Confirm-EnvoyDevCert -EnvoyDir (Join-Path $ProjectRoot 'deploy/envoy')

    Stop-Component 'envoy'   # 配置每次都重生成,必须重启才生效(等价 compose 的 --force-recreate)
    $cfg = New-LocalEnvoyConfig

    Write-Step ' 校验派生的 Envoy 配置'
    $val = & $exe --mode validate -c $cfg 2>&1
    if ($LASTEXITCODE -ne 0) {
        $text = $val -join "`n"
        Write-Err '派生配置在本机 Envoy 上校验不通过 —— 拒绝启动(fail-closed)。'
        $unknown = [regex]::Match($text, "no such field: '(?<f>[^']+)'")
        if ($unknown.Success) {
            Write-Err "未知字段: $($unknown.Groups['f'].Value)"
            Write-Host @"
      deploy/envoy/envoy.yaml 用到了本机 Envoy $EnvoyImageTag 不支持的新字段。
      **不要**顺手把它加进 `$EnvoyDropFields 就完事 —— 先确认剔掉它之后本机的鉴权 / 路由
      不会比线上更宽松;确认无害再登记进白名单并写清退化说明,否则策划机会跑在一套
      看起来正常、实际少了一道门的配置上。
"@ -ForegroundColor Yellow
        } else {
            Write-Host $text -ForegroundColor DarkGray
        }
        exit 1
    }
    Write-Ok '配置校验通过'

    Write-Step " 启动 Envoy :8443(客户端面) / :8444(DS 面)"
    $proc = Start-Process -FilePath $exe `
        -ArgumentList '-c', "`"$cfg`"", '--log-level', 'warn', '--log-path', "`"$(Join-Path $LogDir 'envoy.log')`"" `
        -WorkingDirectory $CfgDir -WindowStyle Hidden -PassThru
    Save-Pid 'envoy' $proc.Id
    Wait-Port -Name 'envoy' -Port 8443 -TimeoutSec 30 -Proc $proc
    Write-Ok 'Envoy :8443 / :8444'
}

# ===== 动作 =====

function Invoke-Up {
    Invoke-ProvisionAll
    Start-LocalMysql
    Start-LocalRedis
    Start-LocalKafka
    Start-LocalEnvoy
    Write-Host ''
    Write-Host '  本机基础设施已就绪(免 Docker)' -ForegroundColor Green
    Invoke-Status
}

function Invoke-Down {
    foreach ($n in @('envoy', 'kafka', 'redis', 'mysql')) { Stop-Component $n }
    Write-Ok '本机基础设施已停止(数据保留)'
}

function Invoke-Status {
    $rows = @(
        @{ Name = 'mysql'; Port = $MysqlPort }
        @{ Name = 'redis'; Port = $RedisPort }
        @{ Name = 'kafka'; Port = $KafkaPort }
        @{ Name = 'envoy'; Port = 8443 }
        @{ Name = 'envoy-ds'; Port = 8444 }
    )
    Write-Host ''
    Write-Host '  组件      端口   状态' -ForegroundColor Gray
    foreach ($r in $rows) {
        $ok = Test-PortOpen $r.Port
        $txt = if ($ok) { 'UP  ' } else { 'DOWN' }
        $color = if ($ok) { 'Green' } else { 'Red' }
        Write-Host ("  {0,-9} {1,-6} {2}" -f $r.Name, $r.Port, $txt) -ForegroundColor $color
    }
    Write-Host ''
}

function Invoke-Reset {
    Invoke-Down
    Write-Step '删除本机基础设施数据目录'
    Remove-Item -LiteralPath $DataDir -Recurse -Force -ErrorAction SilentlyContinue
    Write-Ok "已删除 $DataDir(下次 up 会重新初始化)"
}

New-Item -ItemType Directory -Force -Path $Root, $LogDir, $PidDir, $CfgDir | Out-Null
# 已备料过就先挂 PATH:down / status 这类不走 provision 的动作也能用上自带工具。
Register-LocalToolPath

switch ($Action) {
    'up' { Invoke-Up }
    'down' { Invoke-Down }
    'status' { Invoke-Status }
    'provision' { Invoke-ProvisionAll }
    'reset' { Invoke-Reset }
}
