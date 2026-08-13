# go_test_race.ps1 —— 在 Linux 容器里跑 `go test -race`。
#
# 为什么需要这个脚本:
#   `-race` 依赖 CGO,而本仓库的开发机是 Windows 且通常没装 gcc,直接跑会得到
#       go: -race requires cgo; enable cgo by setting CGO_ENABLED=1
#   于是 `-race` 被当成"环境不支持"写进了多份事故档案的阻断项(INC-20260811-001、
#   INC-20260811-002、INC-20260812-002 …)。实际上它只是**需要一个带 CGO 的 Linux**,
#   而本机已有 golang 镜像和完整模块缓存 —— 挂进去就能跑,不需要 CI、不需要装 gcc。
#
# 两个必须踩对的点(踩错会得到误导性的失败):
#   1. **保持 workspace 模式**。本仓库是 go.work 多模块;设 GOWORK=off 会让
#      `./services/...` 报 "directory prefix does not contain main module"。
#   2. **不要设 GOFLAGS=-mod=mod**。workspace 模式下 `-mod` 只接受 readonly/vendor,
#      设成 mod 会直接报错退出。
#
# 用法:
#   pwsh tools/scripts/go_test_race.ps1                          # 全仓
#   pwsh tools/scripts/go_test_race.ps1 -Pattern ./services/runtime/owner/...
#   pwsh tools/scripts/go_test_race.ps1 -Pattern ./pkg/... -Timeout 30m
[CmdletBinding()]
param(
    # 要测的包模式;默认全仓。注意全仓 -race 很慢(编译器要插桩每一次内存访问)。
    [string]$Pattern = './...',
    # 单个测试二进制的超时;-race 下比平时慢 2~20 倍,默认放宽到 20 分钟。
    [string]$Timeout = '20m',
    # golang 镜像 tag。默认跟随 go.mod 的工具链版本,避免"本地过、容器不过"的版本差。
    [string]$Image = 'golang:1.26.5'
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $repoRoot

# 模块缓存必须挂进去:5.7G 依赖,不挂就得在容器里重新下载一遍(且离线环境直接失败)。
$modCache = (& go env GOMODCACHE)
if (-not $modCache -or -not (Test-Path $modCache)) {
    throw "拿不到 GOMODCACHE($modCache);先在宿主装好 Go 工具链再跑本脚本。"
}

Write-Host "[race] 镜像=$Image  包=$Pattern  超时=$Timeout"
Write-Host "[race] 仓库=$repoRoot"
Write-Host "[race] 模块缓存=$modCache"

# -e CGO_ENABLED=1 是唯一必需的 env;刻意不设 GOWORK / GOFLAGS(理由见文件头)。
& docker run --rm `
    -v "${repoRoot}:/src" `
    -v "${modCache}:/go/pkg/mod" `
    -w /src `
    -e CGO_ENABLED=1 `
    $Image `
    sh -c "go test -race -count=1 -timeout $Timeout $Pattern"

$code = $LASTEXITCODE
if ($code -ne 0) {
    Write-Host "[race] 失败(exit=$code)。data race 会打印 'WARNING: DATA RACE' 与双方栈,请完整保留该段贴进事故档案。" -ForegroundColor Red
    exit $code
}
Write-Host "[race] 通过。" -ForegroundColor Green
