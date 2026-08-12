<#
.SYNOPSIS
  在「能联网」的机器上把 Pandora 业务镜像(+可选基础设施镜像)打包成 tar,
  拷到「拉不到镜像」的机器上用 import_images.ps1 离线导入。

.DESCRIPTION
  目标机连不上 Docker Hub / 国内加速站时,不必在目标机联网构建。流程:
    1) 本机(能联网)跑本脚本 → 构建 22 个 pandora/*:dev + 打包成 pandora-images.tar
    2) U 盘 / 共享盘 把 tar 拷到目标机
    3) 目标机跑 import_images.ps1 → docker load 进本地
    4) 目标机双击一键启动 .cmd 即可(镜像已在本地,不再联网拉)

  默认只打包 22 个业务镜像(pandora/*:dev)。基础设施(mysql/redis/kafka/etcd/
  prometheus/grafana/loki/alloy/envoy)一般目标机已经拉到过并在跑;若目标机是全新环境、
  基础设施也拉不下来,加 -IncludeInfra 并显式指定独立 -Out 一并打包。

  过期守卫:先比较现有 tar 与源码/构建脚本的最新 mtime;包未过期且 tag 完整时直接退出,
  不重复构建/导出。需要重出且未带 -Build 时,再比较每个业务镜像 Created 时间与镜像源,
  发现镜像更旧就拒绝并提示先 -Build,不提供绕过开关。

.EXAMPLE
  # 本机:先构建再打包(推荐,保证镜像最新)
  pwsh tools/scripts/export_images.ps1 -Build

  # 本机:宿主编译再打包(快,秒级增量,需装 Go)
  pwsh tools/scripts/export_images.ps1 -Build -BuildMode host

  # 本机:只打包(镜像已构建好,不想重建)
  pwsh tools/scripts/export_images.ps1

  # 本机:业务 + 基础设施一起打包(目标机是全新环境;必须用独立包,不覆盖仓库受管包)
  pwsh tools/scripts/export_images.ps1 -Build -IncludeInfra -Out D:\pandora-full-images.tar

  # 指定输出路径
  pwsh tools/scripts/export_images.ps1 -Build -Out D:\pandora-images.tar
#>
[CmdletBinding()]
param(
    [switch]$Build,          # 打包前先构建业务镜像(复用 start.ps1 的 Build-AllImages,强制 -Rebuild 不走离线短路)
    [switch]$IncludeInfra,   # 连基础设施镜像一起打包;必须显式 -Out 且不能覆盖仓库受管包
    [ValidateSet('incontainer', 'host')]
    [string]$BuildMode = 'host', # 配合 -Build:host=宿主交叉编译再打包(默认,AGENTS §4 优先宿主方案)/ incontainer=容器内编译(宿主无 Go 时用)
    [string]$Out             # 输出 tar 路径(默认 <仓库根>/deploy/offline-images/pandora-images.tar)
)

$ErrorActionPreference = 'Stop'
$ScriptDir   = $PSScriptRoot
$ProjectRoot = (Resolve-Path "$ScriptDir/../..").Path

function Write-Info($m) { Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Write-Ok($m)   { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Write-Warn($m) { Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Write-Err($m)  { Write-Host "[ERR ] $m" -ForegroundColor Red }
function Write-Step($m) { Write-Host "`n===== $m =====" -ForegroundColor Magenta }

function Get-ArchiveRepoTags([string]$Archive) {
    $json = tar -xOf $Archive manifest.json
    if ($LASTEXITCODE -ne 0) { throw "无法读取镜像包 manifest.json:$Archive" }
    $manifest = $json | ConvertFrom-Json
    $tags = @()
    foreach ($entry in @($manifest)) {
        foreach ($tag in @($entry.RepoTags)) {
            if ($tag) { $tags += $tag }
        }
    }
    return $tags
}

function Copy-ArchivePayload([string]$ExtractDir, [string]$MergeDir) {
    Get-ChildItem -LiteralPath $ExtractDir -Recurse -File | ForEach-Object {
        $rel = [System.IO.Path]::GetRelativePath($ExtractDir, $_.FullName)
        if ($rel -in @('manifest.json', 'repositories', 'index.json', 'oci-layout')) { return }

        $target = Join-Path $MergeDir $rel
        $targetDir = Split-Path -Parent $target
        if (-not (Test-Path -LiteralPath $targetDir)) {
            New-Item -ItemType Directory -Path $targetDir -Force | Out-Null
        }
        if (-not (Test-Path -LiteralPath $target)) {
            Copy-Item -LiteralPath $_.FullName -Destination $target
        }
    }
}

function Save-MergedDockerArchive([string[]]$Images, [string]$OutPath) {
    $tmpRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("pandora-docker-save-" + [System.Guid]::NewGuid().ToString('N'))
    $mergeDir = Join-Path $tmpRoot 'merged'
    New-Item -ItemType Directory -Path $mergeDir -Force | Out-Null

    try {
        $allManifest = @()
        $seenTags = New-Object 'System.Collections.Generic.HashSet[string]'
        $index = 0

        foreach ($img in $Images) {
            $singleTar = Join-Path $tmpRoot ("image-$index.tar")
            $extractDir = Join-Path $tmpRoot ("image-$index")
            New-Item -ItemType Directory -Path $extractDir -Force | Out-Null

            docker save -o $singleTar $img
            if ($LASTEXITCODE -ne 0) { throw "docker save 单镜像失败:$img" }

            tar -xf $singleTar -C $extractDir
            if ($LASTEXITCODE -ne 0) { throw "解包单镜像 archive 失败:$img" }

            $manifestPath = Join-Path $extractDir 'manifest.json'
            $entries = Get-Content -LiteralPath $manifestPath -Raw | ConvertFrom-Json
            foreach ($entry in @($entries)) {
                $newTags = @()
                foreach ($tag in @($entry.RepoTags)) {
                    if ($tag -and $seenTags.Add($tag)) { $newTags += $tag }
                }
                if ($newTags.Count -gt 0) {
                    $entry.RepoTags = [string[]]$newTags
                    $allManifest += $entry
                }
            }

            Copy-ArchivePayload -ExtractDir $extractDir -MergeDir $mergeDir
            $index++
        }

        $allManifest | ConvertTo-Json -Depth 20 | Set-Content -LiteralPath (Join-Path $mergeDir 'manifest.json') -Encoding UTF8
        $items = @(Get-ChildItem -LiteralPath $mergeDir | ForEach-Object { $_.Name })
        tar -cf $OutPath -C $mergeDir @items
        if ($LASTEXITCODE -ne 0) { throw "合并镜像 archive 失败。" }
    } finally {
        if (Test-Path -LiteralPath $tmpRoot) {
            Remove-Item -LiteralPath $tmpRoot -Recurse -Force
        }
    }
}

function Get-NewestFileTimeUtc([string[]]$Paths) {
    $newest = $null
    foreach ($path in $Paths) {
        if (-not (Test-Path -LiteralPath $path)) { continue }
        if (Test-Path -LiteralPath $path -PathType Leaf) {
            $item = Get-Item -LiteralPath $path
            if ($null -eq $newest -or $item.LastWriteTimeUtc -gt $newest) { $newest = $item.LastWriteTimeUtc }
            continue
        }
        $latest = Get-ChildItem -LiteralPath $path -Recurse -File -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTimeUtc -Descending | Select-Object -First 1
        if ($latest -and ($null -eq $newest -or $latest.LastWriteTimeUtc -gt $newest)) {
            $newest = $latest.LastWriteTimeUtc
        }
    }
    return $newest
}

# 22 个业务服务镜像名(与 start.ps1 的 Get-ServiceList 一致)
$BusinessImages = @(
    'pandora/login:dev','pandora/player:dev','pandora/data-service:dev',
    'pandora/friend:dev','pandora/chat:dev','pandora/guild:dev','pandora/mail:dev',
    'pandora/player-locator:dev','pandora/leaderboard:dev','pandora/owner:dev','pandora/team:dev',
    'pandora/matchmaker:dev','pandora/matchmaker-pve:dev','pandora/trade:dev','pandora/dialogue:dev',
    'pandora/mission:dev',
    'pandora/push:dev','pandora/inventory:dev','pandora/auction:dev',
    'pandora/ds-allocator:dev','pandora/hub-allocator:dev','pandora/battle-result:dev'
)

# 基础设施镜像(与 deploy/docker-compose.dev.yml 的 image: 一致)
$InfraImages = @(
    'mysql:8.4','redis:8.8.0-alpine',
    'confluentinc/cp-zookeeper:7.9.7','confluentinc/cp-kafka:7.9.7',
    'quay.io/coreos/etcd:v3.6.12','prom/prometheus:v2.55.1',
    'grafana/grafana:11.3.1','grafana/loki:3.4.1','grafana/alloy:v1.7.1',
    'envoyproxy/envoy:v1.38-latest','envoyproxy/envoy:v1.38.1',
    # TiDB 三件套(2026-07-28 进集群,deploy/k8s/infra/tidb.yaml;清单须与 infra yaml 同步)
    'pingcap/pd:v8.5.1','pingcap/tikv:v8.5.1','pingcap/tidb:v8.5.1'
)

# ---- 输出隔离 + 现有包新旧判断(必须早于构建,避免包未过期时浪费时间) ----
$managedOut = [System.IO.Path]::GetFullPath((Join-Path $ProjectRoot 'deploy/offline-images/pandora-images.tar'))
$projectRootPrefix = $ProjectRoot.TrimEnd([System.IO.Path]::DirectorySeparatorChar, [System.IO.Path]::AltDirectorySeparatorChar) + [System.IO.Path]::DirectorySeparatorChar
if (-not $Out) {
    if ($IncludeInfra) {
        throw '-IncludeInfra 必须显式指定独立 -Out,禁止覆盖仓库受管的 20 业务镜像包。'
    }
    $outDir = Split-Path -Parent $managedOut
    if (-not (Test-Path -LiteralPath $outDir)) { New-Item -ItemType Directory -Path $outDir -Force | Out-Null }
    $Out = $managedOut
} else {
    $Out = [System.IO.Path]::GetFullPath($Out)
}
if ($IncludeInfra -and $Out.StartsWith($projectRootPrefix, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "-IncludeInfra 完整大包不得写入源码仓库:$ProjectRoot。请用 -Out 指定仓库外路径。"
}
$outParent = Split-Path -Parent $Out
if (-not (Test-Path -LiteralPath $outParent)) { New-Item -ItemType Directory -Path $outParent -Force | Out-Null }

$images = @($BusinessImages)
if ($IncludeInfra) { $images += $InfraImages }

# 镜像内容源与打包源分开:export/wrapper 变化要求重出 tar,但不会让既有镜像本身过期。
$imageSourcePaths = @(
    (Join-Path $ProjectRoot 'services'),
    (Join-Path $ProjectRoot 'pkg'),
    (Join-Path $ProjectRoot 'proto'),
    (Join-Path $ProjectRoot 'go.work'),
    (Join-Path $ProjectRoot 'go.work.sum'),
    (Join-Path $ProjectRoot '.dockerignore'),
    (Join-Path $ProjectRoot 'deploy/services'),
    (Join-Path $ProjectRoot 'tools/scripts/start.ps1')
)
$packageSourcePaths = @($imageSourcePaths) + @(
    (Join-Path $ProjectRoot 'tools/scripts/export_images.ps1'),
    (Join-Path $ProjectRoot '出离线镜像包.cmd')
)
if ($IncludeInfra) {
    $packageSourcePaths += (Join-Path $ProjectRoot 'deploy/docker-compose.dev.yml')
}
$newestImageSource = Get-NewestFileTimeUtc $imageSourcePaths
$newestPackageSource = Get-NewestFileTimeUtc $packageSourcePaths
if ($null -eq $newestImageSource -or $null -eq $newestPackageSource) {
    throw '未找到镜像/打包源文件,无法可靠判断离线包是否过期。'
}

if (Test-Path -LiteralPath $Out -PathType Leaf) {
    $archive = Get-Item -LiteralPath $Out
    if ($archive.LastWriteTimeUtc -ge $newestPackageSource) {
        try {
            $existingTags = @(Get-ArchiveRepoTags -Archive $Out)
            $missingExistingTags = @($images | Where-Object { $existingTags -notcontains $_ })
            $unexpectedExistingTags = @($existingTags | Where-Object { $images -notcontains $_ })
            if ($missingExistingTags.Count -eq 0 -and $unexpectedExistingTags.Count -eq 0 -and $existingTags.Count -eq $images.Count) {
                Write-Ok "现有镜像包未过期且 tag 完整($($existingTags.Count) 个):$Out"
                Write-Info '无需重复构建/导出。源码或镜像相关脚本有新改动后再重出。'
                exit 0
            }
            Write-Warn "现有包 tag 集合不精确(缺少=$($missingExistingTags.Count),额外=$($unexpectedExistingTags.Count),总数=$($existingTags.Count),期望=$($images.Count)),将重新生成。"
        } catch {
            Write-Warn "现有包无法通过 manifest 校验($($_.Exception.Message)),将重新生成。"
        }
    } else {
        Write-Info "现有包已过期(包 UTC $($archive.LastWriteTimeUtc.ToString('yyyy-MM-dd HH:mm:ss'));源 UTC $($newestPackageSource.ToString('yyyy-MM-dd HH:mm:ss'))),需要重出。"
    }
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Write-Err "未找到 docker,请先安装 Docker Desktop 并启动。"
    exit 1
}

# ---- 可选:先构建业务镜像 ----
if ($Build) {
    Write-Step "构建 22 个业务镜像(BuildMode=$BuildMode;离线优先:本地基础镜像 + docker.io 源)"

    # incontainer(方案 A):容器内跑 `go build`,基础镜像必须实测为 go1.26.5。
    # host(方案 B):start.ps1 会独立校验宿主 `go env GOVERSION` 精确等于 go1.26.5；
    # Dockerfile.prebuilt 的 runtime_assets 只提供 CA/时区，不参与 Go 编译。
    if ($BuildMode -eq 'incontainer') {
        $wantGo = 'golang:1.26.5-bookworm'
        $wantGoversion = 'go1.26.5'

        function Get-GoImageGoversion([string]$image) {
            $out = (docker run --rm --entrypoint go $image env GOVERSION 2>$null | Out-String).Trim()
            if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($out)) { return $null }
            return $out
        }

        docker image inspect $wantGo *> $null
        $wantActual = if ($LASTEXITCODE -eq 0) { Get-GoImageGoversion $wantGo } else { $null }
        if ($wantActual -ne $wantGoversion) {
            if ($wantActual) { Write-Warn "本地 $wantGo 实际为 $wantActual，拒绝冒充 $wantGoversion。" }
            # 逐个候选 golang 镜像实测，只接受精确的 go1.26.5。
            $candidates = @(docker images --format '{{.Repository}}:{{.Tag}}' | Select-String '(^|/)golang:1\.26\.5-bookworm$' | ForEach-Object { "$_".Trim() })
            $picked = $null
            foreach ($cand in $candidates) {
                $actual = Get-GoImageGoversion $cand
                if ($actual -eq $wantGoversion) { $picked = $cand; break }
                elseif ($actual) { Write-Warn "本地 $cand 的 GOVERSION=$actual，跳过。" }
            }
            if ($picked) {
                Write-Info "发现 $picked 的 GOVERSION=$wantGoversion，打标 $wantGo 复用。"
                docker tag $picked $wantGo
                docker tag $picked "docker.io/library/golang:1.26.5-bookworm"
            } else {
                Write-Err "本地没有任何 GOVERSION=$wantGoversion 的 golang 镜像可复用。"
                Write-Err "请先 docker pull golang:1.26.5-bookworm(或在能联网的机器上 pull 后 docker save/load 过来)。"
                Write-Err "提示:本机已装 go 1.26.5 时,可改用 -BuildMode host(宿主编译,不需要容器内 golang 1.26.5)。"
                exit 1
            }
        } else {
            docker tag $wantGo "docker.io/library/golang:1.26.5-bookworm" 2>$null
        }
    }

    # 基础镜像仓库 + go 模块代理(host / incontainer 都用):本地已有镜像不会真的联网。
    # GOPROXY 分隔符必须是 `|`:逗号只在 404/410 回退,网络层错误直接失败(详见 deploy/services/Dockerfile)。
    if (-not $env:PANDORA_BASE_REGISTRY) { $env:PANDORA_BASE_REGISTRY = 'docker.io' }
    if (-not $env:PANDORA_GOPROXY)       { $env:PANDORA_GOPROXY       = 'https://goproxy.cn|https://proxy.golang.org|direct' }

    $buildOnlyServices = @($BusinessImages | ForEach-Object { ($_ -replace '^pandora/', '') -replace ':dev$', '' })
    # ⚠️ 必须带 -Rebuild:不带时 start.ps1 的离线短路(PANDORA_OFFLINE=1 / 本机无构建能力)会
    # 直接 docker load 旧离线包当作「构建成功」,本脚本随后把旧镜像重新打包 = 新包装旧酒
    # (回应审核 P1:-Build 未传 -Rebuild)。-Build 语义就是「从当前源码重新构建」,无条件绕过离线短路。
    & (Join-Path $ScriptDir 'start.ps1') -Mode docker -BuildOnly -Rebuild -BuildMode $BuildMode -Only $buildOnlyServices
    if ($LASTEXITCODE -ne 0) { throw "业务镜像构建失败,先解决构建问题再打包。" }
}

Write-Step "校验本地镜像是否齐全"
$missing = @()
foreach ($img in $images) {
    docker image inspect $img *> $null
    if ($LASTEXITCODE -ne 0) { $missing += $img } else { Write-Ok "已存在:$img" }
}
if ($missing.Count -gt 0) {
    Write-Err "以下镜像本地不存在,无法打包:"
    $missing | ForEach-Object { Write-Err "  - $_" }
    if (-not $Build) { Write-Warn "提示:加 -Build 先构建业务镜像;基础设施缺失请先 docker compose 起一次或加 -IncludeInfra 前先拉取。" }
    exit 1
}

# ---- 过期守卫:业务镜像固定用 :dev tag,「存在」不代表「最新」 ----
# 若本次不 -Build,现有 pandora/*:dev 可能是上次编译的旧镜像;而 services/ pkg/ proto 生成代码
# 或 Dockerfile 之后有改动时,直接打包会产出「过期离线包」(AGENTS.md §4:出包前判断是否过期)。
if (-not $Build) {
    Write-Step "过期守卫:校验业务镜像是否比源码新"
    $staleImages = @()
    foreach ($img in $BusinessImages) {
        $createdRaw = (docker image inspect -f '{{.Created}}' $img 2>$null | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($createdRaw)) {
            # 存在性检查已通过却读不到 Created,保守视为过期。
            $staleImages += $img
            continue
        }
        try { $createdUtc = ([datetimeoffset]::Parse($createdRaw)).UtcDateTime } catch { $staleImages += $img; continue }
        if ($createdUtc -lt $newestImageSource) { $staleImages += $img }
    }

    if ($staleImages.Count -gt 0) {
        Write-Err "以下业务镜像比镜像源旧(源最新改动 UTC $($newestImageSource.ToString('yyyy-MM-dd HH:mm:ss'))),离线包会过期:"
        $staleImages | ForEach-Object { Write-Err "  - $_" }
        Write-Err '请按项目标准用宿主方案重建再打包:pwsh tools/scripts/export_images.ps1 -Build -BuildMode host'
        exit 1
    }
    Write-Ok '全部业务镜像均不早于镜像源最新改动,未过期。'
}

Write-Step "导出 $($images.Count) 个镜像 → $Out(可能几分钟,镜像较大)"
docker save -o $Out @images
if ($LASTEXITCODE -ne 0) { throw "docker save 失败。" }

$exportedTags = @(Get-ArchiveRepoTags -Archive $Out)
$missingTags = @($images | Where-Object { $exportedTags -notcontains $_ })
$unexpectedTags = @($exportedTags | Where-Object { $images -notcontains $_ })
if ($missingTags.Count -gt 0 -or $unexpectedTags.Count -gt 0 -or $exportedTags.Count -ne $images.Count) {
    Write-Warn "docker save 产物 tag 集合不精确(缺少=$($missingTags.Count),额外=$($unexpectedTags.Count)),改用逐镜像合并 archive:"
    $missingTags | ForEach-Object { Write-Warn "  - $_" }
    Save-MergedDockerArchive -Images $images -OutPath $Out

    $exportedTags = @(Get-ArchiveRepoTags -Archive $Out)
    $missingTags = @($images | Where-Object { $exportedTags -notcontains $_ })
    $unexpectedTags = @($exportedTags | Where-Object { $images -notcontains $_ })
    if ($missingTags.Count -gt 0 -or $unexpectedTags.Count -gt 0 -or $exportedTags.Count -ne $images.Count) {
        Write-Err "合并后的镜像包 tag 集合仍不精确(缺少=$($missingTags.Count),额外=$($unexpectedTags.Count),总数=$($exportedTags.Count),期望=$($images.Count)):"
        $missingTags | ForEach-Object { Write-Err "  - $_" }
        $unexpectedTags | ForEach-Object { Write-Err "  + $_" }
        throw "离线镜像包校验失败。"
    }
}
Write-Ok "镜像包 manifest 校验通过($($exportedTags.Count) 个 tag)。"

$sizeMB = [math]::Round((Get-Item $Out).Length / 1MB, 1)
Write-Ok "打包完成:$Out(${sizeMB} MB)"
Write-Host ""
Write-Info "下一步(拷到目标机后,在目标机上跑):"
Write-Info "  pwsh tools/scripts/import_images.ps1 -In <拷过去的 pandora-images.tar 路径>"
Write-Info "导入后目标机双击一键启动 .cmd 即可(镜像已在本地,不再联网拉)。"
