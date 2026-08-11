<#
.SYNOPSIS
  构建业务镜像离线包并发布到制品目录(版本库外),替代旧的"tar 入库随仓库同步"。

.DESCRIPTION
  流程:
    1) git 取版本戳(短 sha;工作区脏则默认拒绝,-AllowDirty 放行并带时间戳后缀)
    2) 调用 export_images.ps1 生成 deploy/offline-images/pandora-images.tar
       (位置不变:本机 start.ps1 的离线导入短路继续可用;该 tar 已不入库)
    3) 从 tar 的 manifest.json 提取每个镜像的 RepoTags + Config 摘要(镜像 ID),
       生成 images-manifest.json —— 部署侧可据此核验导入的镜像身份
    4) 原子发布到 <制品根>\images\<版本>\,附 sha256sums.txt + build-info.json,
       并更新 <制品根>\images\latest.json 指针

  制品根目录:-ArtifactRoot > 环境变量 PANDORA_ARTIFACT_ROOT > F:\work\artifacts

.EXAMPLE
  # 从当前源码重建并发布(推荐;宿主 Go 交叉编译)
  pwsh tools/scripts/publish_offline_images.ps1

  # 镜像/包已是最新(export 过期守卫会核验),只发布不重建
  pwsh tools/scripts/publish_offline_images.ps1 -SkipBuild
#>
[CmdletBinding()]
param(
    [switch]$SkipBuild,     # 不重建,复用现有 tar(export_images.ps1 的过期守卫仍会核验新旧)
    [switch]$AllowDirty,    # git 工作区脏时仍发布(版本号带 -dirty-时间戳;仅本机联调用,CI 禁用)
    [switch]$SkipIfExists,  # 目标版本已发布时静默成功(CI 幂等重跑用)
    [string]$ArtifactRoot,  # 制品根目录覆盖
    [string]$Version,       # 发布版本号(如 v0.1.0):显式覆盖 git describe 注入镜像,免打 tag;记入 build-info.app_version
    [switch]$PushRegistry,  # 同时把业务镜像推到 registry —— k8s 只能从 registry 拉,离线 tar 集群用不了
    [string]$RegistryHost = 'localhost:5000'  # -PushRegistry 的目标 registry(deploy/devops .env 的 REGISTRY_PORT)
)

$ErrorActionPreference = 'Stop'
$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path "$ScriptDir/../..").Path
. (Join-Path $ScriptDir 'artifacts_lib.ps1')

function Write-Info($m) { Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Write-Ok($m)   { Write-Host "[ OK ] $m" -ForegroundColor Green }

# ---- 1) 版本戳(自适应 SVN / git) ----
# 后端代码同时存在于团队 SVN(^/trunk/Server,与 ^/trunk/Client 同级)和个人 git 仓库。
# CI 构建的是 SVN 权威源,开发者本机可能在 git 工作副本里跑,故两种都要支持。
# SVN 优先:它是团队权威源;且 r<rev> 与客户端包命名一致,前后端版本戳统一。
Push-Location $ProjectRoot
try {
    # 判定用**命令退出码**,不探测 .svn / .git 目录是否存在:
    # SVN 1.7+ 只在工作副本**根**保留一个 .svn 目录。CI 若检出 ^/trunk(Server 与 Client 同级)
    # 而不是 ^/trunk/Server,$ProjectRoot 下就没有 .svn,按目录探测会误判成"不是 SVN 工作副本"
    # 并落到最后的 throw。svnversion 在工作副本的任意子目录都能正确解析,没有这个盲区。
    $svnAvailable = [bool](Get-Command svnversion -ErrorAction SilentlyContinue)
    $svnver = $null
    if ($svnAvailable) {
        $svnver = (svnversion . 2>$null | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or -not $svnver -or $svnver -match 'Unversioned|exported') { $svnver = $null }
    }
    $gitAvailable = [bool](Get-Command git -ErrorAction SilentlyContinue)
    $gitSha = $null
    if (-not $svnver -and $gitAvailable) {
        $gitSha = (git rev-parse --short=12 HEAD 2>$null | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or -not $gitSha) { $gitSha = $null }
    }

    if ($svnver) {
        $vcs = 'svn'
        if ($svnver -notmatch '^(\d+)(:(\d+))?([MSP]*)$') { throw "svnversion 输出无法解析:'$svnver'" }
        # 混合版本取较大者;M(本地改动)/S(切换)/P(部分检出)任一出现都算 dirty
        $srcId = 'r' + $(if ($Matches[3]) { $Matches[3] } else { $Matches[1] })
        $dirty = [bool]$Matches[2] -or [bool]$Matches[4]
    }
    elseif ($gitSha) {
        $vcs   = 'git'
        $srcId = "g$gitSha"
        $dirty = @(git status --porcelain 2>$null | Where-Object { $_ }).Count -gt 0
    }
    else {
        # 把"客户端没装"和"这里确实不是工作副本"分开报,否则 CI 上二者症状一样、极难定位。
        throw @"
无法取得可追溯的源码版本,拒绝发布。$ProjectRoot 既不是可识别的 SVN 工作副本,也不是 git 仓库。
  svnversion 可用: $svnAvailable$(if (-not $svnAvailable) { '   ← SVN 命令行客户端未安装或不在 PATH;Jenkins 的 Subversion 插件不提供它' })
  git 可用       : $gitAvailable
"@
    }
} finally { Pop-Location }

if ($dirty -and -not $AllowDirty) {
    throw "$vcs 工作区有未提交改动($srcId),发布的镜像无法追溯到确定版本。先提交,或本机联调明确加 -AllowDirty。"
}
# 注意:git 快照版本变量名不能用 $version —— PowerShell 变量名大小写不敏感,$version 与参数
# $Version 是同一变量,会覆盖用户传入的发布版本号(v0.1.0 → g<sha>),连带污染下面的 channel /
# folderVer / app_version 判定。用独立名 $snapshotVer 隔离。
$snapshotVer = if ($dirty) { "$srcId-dirty-" + (Get-Date -Format 'yyyyMMdd-HHmmss') } else { $srcId }

# 频道:有 -Version = 发布版本轨(releases\,目录名=版本号);否则 dev 快照轨(snapshots\,目录名=SVN r<rev> 或 git g<sha>)
$channel   = if ($Version) { 'release' } else { 'snapshot' }
$folderVer = if ($Version) { ($Version -replace '[^A-Za-z0-9._-]', '_') } else { $snapshotVer }

# ---- 2) 生成/校验离线 tar(复用既有脚本与其过期守卫,不另造构建逻辑) ----
# 发布版本号覆盖:设 PANDORA_RELEASE_VERSION 后,start.ps1 的 Get-VersionInfo 用它注入 -ldflags,
# 免打 git tag(标准 CI 做法:显式 VERSION 优先于 git describe)。Commit 字段仍记 git sha 作溯源。
if ($Version) {
    $env:PANDORA_RELEASE_VERSION = $Version
    Write-Info "镜像版本注入为 $Version(覆盖默认版本;源码版本 $srcId 仍作溯源)"
}
# 源码版本下传给 start.ps1 的 Get-VersionInfo。必须这样传,不能让 start.ps1 自己再探一次 VCS:
# 它原本只会 git describe / git rev-parse,在 SVN 检出下是**软回退**(不报错),结果 22 个业务镜像
# 被静默烙上 version=dev / commit=unknown —— 编译期注入,事后无法从镜像追回是哪次提交。
# 版本来源收敛在本脚本一处(§15.5 代码路径直达),start.ps1 只消费。
$env:PANDORA_RELEASE_COMMIT = $srcId
# 显式传参:数组 splat 传 switch 参数(-Build)会被 PowerShell 误绑成 -BuildMode 的值,必须显式写。
$exportScript = Join-Path $ScriptDir 'export_images.ps1'
if ($SkipBuild) { & $exportScript } else { & $exportScript -Build -BuildMode host }
if ($LASTEXITCODE -ne 0) { throw "export_images.ps1 失败(exit=$LASTEXITCODE),不发布。" }

$tar = Join-Path $ProjectRoot 'deploy/offline-images/pandora-images.tar'
if (-not (Test-Path -LiteralPath $tar)) { throw "找不到离线包:$tar" }

# ---- 3) 从 tar manifest 提取镜像身份(tag + 镜像 ID),不重复维护镜像清单 ----
$manifestJson = tar -xOf $tar manifest.json
if ($LASTEXITCODE -ne 0) { throw "无法读取离线包 manifest.json:$tar" }
$entries = $manifestJson | ConvertFrom-Json
$imagesManifest = @(foreach ($e in @($entries)) {
    [pscustomobject]@{
        repo_tags = @($e.RepoTags)
        # docker archive 的 Config 即镜像 ID blob(如 blobs/sha256/<id>),据此核验导入结果
        image_id  = ($e.Config -replace '^.*/', '' -replace '\.json$', '')
    }
})
if ($imagesManifest.Count -eq 0) { throw '离线包 manifest 为空,拒绝发布。' }

# ---- 3.5) 推送业务镜像到 registry(-PushRegistry) ----
# k8s 只能从 registry 拉镜像,离线 tar 集群用不了 —— 这一步是 GitOps 链上 CI 侧的终点。
#
# 【顺序:必须在下面的原子发布之前】版本目录一旦落地就不可变,-SkipIfExists 会让重跑直接 exit 0,
# 那时 push 永远补不上,形成"制品目录有、registry 没有"的静默缺口(k8s ImagePullBackOff 却查不到源头)。
# 放在前面则 push 失败可无损重跑:docker push 同 tag 同内容天然幂等(重推全是 Layer already exists)。
#
# 【tag 用 $folderVer 而非 :dev】:dev 是可变 tag,在 GitOps 下是硬伤 —— 镜像换了但 manifest 一个字节
# 没变,Argo CD 永远显示 Synced、没法回滚到具体版本、也无法审计线上到底跑的哪次构建。
# 不可变 tag 是 §9.21 金丝雀(stable/canary 必须能同时指定两个确定版本)的前提。
#
# 【适用范围:只限 dev registry,不是线上发布通道】
# start.ps1 -Mode online 的 -BuildPush 路径被主动 throw 阻断,理由是:"先检查 tag 不存在再 push"
# (Assert-RemoteImageTagAbsent)存在 TOCTOU —— 检查与 push 之间他人可抢占同一 tag,客户端预检
# 证明不了不可变,必须由 registry 原生 immutable-tag / create-only 策略 + 发布锁保证。
# 本开关同样绕不过那个论证:它只补"dev 集群拉不到镜像"这个缺口,不是发布通道。
# 线上仍走 start.ps1 -Mode online —— 它按 digest 部署(不按 tag),保证比 tag 更强。
$pushedImages = @()
if ($PushRegistry) {
    $devRegistryPattern = '^(localhost|127\.0\.0\.1|host\.docker\.internal)(:\d+)?$'
    if ($RegistryHost -notmatch $devRegistryPattern) {
        throw @"
-PushRegistry 只允许推本机 dev registry(localhost / 127.0.0.1 / host.docker.internal),当前:$RegistryHost
线上发布不能走这里 —— start.ps1 -Mode online 的 -BuildPush 已因 TOCTOU 主动阻断:
客户端"预检 tag 不存在"证明不了不可变,必须先在目标 registry 启用 native immutable-tag /
create-only 策略与发布锁。绕开它等于把那个论证作废。
线上路径:先配好 registry 不可变策略 → 解除 start.ps1 的 BuildPush 阻断 → 按 digest 部署。
"@
    }
    Write-Info "推送业务镜像 → $RegistryHost(tag=$folderVer)"
    # 镜像清单取自离线包 manifest,与 tar 同源;不另抄一份服务清单(§15.5,避免与 Get-ServiceList 漂移)
    $bizTags = @($imagesManifest | ForEach-Object { $_.repo_tags } | Where-Object { $_ -like 'pandora/*' })
    if ($bizTags.Count -eq 0) { throw "离线包里没有 pandora/* 业务镜像,拒绝推送(export_images.ps1 是否构建成功?)" }
    foreach ($src in $bizTags) {
        # pandora/login:dev → <registry>/pandora/login:<folderVer>
        # ${repo} 的花括号不能省:PowerShell 会把 "$repo:" 解析成 drive qualifier
        $repo = ($src -split ':')[0]
        $dst  = "$RegistryHost/${repo}:$folderVer"
        & docker tag $src $dst
        if ($LASTEXITCODE -ne 0) { throw "docker tag 失败:$src → $dst" }
        & docker push $dst
        if ($LASTEXITCODE -ne 0) { throw "docker push 失败:$dst(registry 活着吗?curl http://$RegistryHost/v2/)" }
        $pushedImages += $dst
    }
    Write-Ok "已推送 $($pushedImages.Count) 个镜像 → $RegistryHost(tag=$folderVer)"
}

# ---- 4) 原子发布(按频道分仓) ----
$channelRoot = Get-ChannelRoot -Override $ArtifactRoot -Channel $channel
$finalDir = Join-Path $channelRoot "images\$folderVer"
if (Test-Path -LiteralPath $finalDir) {
    if ($SkipIfExists) { Write-Ok "版本已发布,跳过(不可变):$finalDir"; exit 0 }
    throw "版本已发布且不可覆盖:$finalDir($channel 轨制品不可变)"
}

Write-Info "发布 [$channel] $folderVer → $finalDir"
$staging = New-AtomicStaging -FinalDir $finalDir
try {
    Copy-Item -LiteralPath $tar -Destination (Join-Path $staging 'pandora-images.tar')
    $imagesManifest | ConvertTo-Json -Depth 5 | Set-Content (Join-Path $staging 'images-manifest.json') -Encoding utf8NoBOM
    [pscustomobject]@{
        version      = $folderVer
        channel      = $channel     # snapshot(dev) / release(发布版本)
        app_version  = $Version    # 注入镜像的发布版本号(v0.1.0);空=未指定,走 git describe 默认
        vcs          = $vcs      # svn(团队权威源 ^/trunk/Server) / git(个人仓库)
        source_rev   = $srcId    # SVN 为 r<rev>,git 为 g<sha>;与客户端包命名一致
        dirty        = $dirty
        image_count  = $imagesManifest.Count
        # registry 落点:空=本次只发离线 tar,没推 registry(k8s 拉不到这个版本)
        registry       = if ($PushRegistry) { $RegistryHost } else { $null }
        pushed_images  = $pushedImages
        published_at = (Get-Date -Format 'o')
        machine      = $env:COMPUTERNAME
        publisher    = $env:USERNAME
    } | ConvertTo-Json | Set-Content (Join-Path $staging 'build-info.json') -Encoding utf8NoBOM
    New-Sha256Sums -Dir $staging | Out-Null
    Complete-AtomicDir -Staging $staging -FinalDir $finalDir
} catch {
    if (Test-Path -LiteralPath $staging) { Remove-Item -LiteralPath $staging -Recurse -Force }
    throw
}

# latest.json 是唯一可变文件(指针,类比 registry 的 latest tag);版本目录本身不可变。每个频道各一份。
[pscustomobject]@{ version = $folderVer; channel = $channel; published_at = (Get-Date -Format 'o') } |
    ConvertTo-Json | Set-Content (Join-Path $channelRoot 'images\latest.json') -Encoding utf8NoBOM

Write-Ok "镜像离线包已发布[$channel]:$finalDir($($imagesManifest.Count) 个镜像)"
Write-Info '目标机拉取:pwsh tools/scripts/fetch_offline_images.ps1(之后一键启动 cmd 照常)'
