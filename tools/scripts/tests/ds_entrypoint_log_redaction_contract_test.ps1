[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$entrypoint = Get-Content -LiteralPath (Join-Path $projectRoot 'deploy/ds/entrypoint.sh') -Raw
$overlayDockerfile = Get-Content -LiteralPath `
    (Join-Path $projectRoot 'deploy/ds/Dockerfile.entrypoint-overlay') -Raw

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

# UE LogNet 的 Login/Join 日志会包含完整连接 URL（包括短期 DSTicket）。入口必须在
# 用户扩展参数之后再次固定分类级别，且禁止通过 LogCmds/ExecCmds/Core.Log 覆盖。
#
# 2026-08-11 修正死契约:本断言原先写死 `exec "${SERVER_SH}"`,而 c37e8660(2026-07-30)
# 为 stdbuf 行缓冲把启动命令重构成数组 `exec "${FINAL_LAUNCH[@]}"`,契约测试被落下 ——
# 它从此断言一个**已不存在的形态**,恒红,而它要守的性质其实一直满足。
# 修法是**重新指向当前不变量**而不是删掉:这里要守的从来不是"用哪个变量名启动",
# 而是**两条强制日志参数必须排在 ${EXTRA_ARGS} 之后**(排在前面会被用户参数覆盖回
# Display 级,含 DSTicket 的 Login/Join URL 就会重新写进 stdout)。
# 故意不再匹配启动变量名 —— 那正是上次把契约写死的地方。
$execPattern = '(?sm)^exec\s+.*?\$\{EXTRA_ARGS\}.*?Core\.Log.*?LogNet=Warning.*?LogCmds=LogNet Warning'
Assert-True ([regex]::IsMatch($entrypoint, $execPattern)) `
    'DS exec 必须在 EXTRA_ARGS 之后强制 Core.Log/LogCmds 的 LogNet=Warning'
foreach ($forbiddenOverride in @('logcmds', 'execcmds', 'ini:engine:[core.log]')) {
    Assert-True $entrypoint.Contains('"' + '${EXTRA_ARGS_LOWER}' + '" == *"' + $forbiddenOverride + '"*') `
        "DS 入口必须拒绝 EXTRA_ARGS 覆盖 $forbiddenOverride"
}
Assert-True (-not [regex]::IsMatch($entrypoint, 'echo[^\r\n]*\$\{EXTRA_ARGS\}')) `
    '启动日志不得回显 PANDORA_DS_EXTRA_ARGS'

# Hub 容量必须通过 URL option 真正进入 UE GameSession，且入口拒绝 0/负数/非数字/溢出；
# battle Fleet 不设置该 env 时 MAP_URL 行为保持原样。
Assert-True $entrypoint.Contains('[[ -n "${MAX_PLAYERS}" && ! "${MAX_PLAYERS}" =~ ^[1-9][0-9]*$ ]]') `
    'DS 入口必须严格校验 PANDORA_DS_MAX_PLAYERS 正整数'
Assert-True $entrypoint.Contains('${#MAX_PLAYERS} > 10') `
    'DS 入口必须在数值比较前按字符串长度拒绝超长十进制'
Assert-True ($entrypoint.Contains('${#MAX_PLAYERS} == 10') -and $entrypoint.Contains('"${MAX_PLAYERS}" > "2147483647"')) `
    'DS 入口必须对 10 位十进制做 canonical lexical int32 上限比较'
Assert-True $entrypoint.Contains('MAP_URL="${MAP_URL}?MaxPlayers=${MAX_PLAYERS}"') `
    'DS 入口必须把 MaxPlayers 作为 UE FURL option 追加到最终 MAP_URL'

function Test-MaxPlayersContract([string]$Value) {
    if ($Value -notmatch '^[1-9][0-9]*$') { return $false }
    if ($Value.Length -gt 10) { return $false }
    if ($Value.Length -eq 10 -and [string]::CompareOrdinal($Value, '2147483647') -gt 0) { return $false }
    return $true
}
Assert-True (Test-MaxPlayersContract '2147483647') 'int32 max 必须通过 MaxPlayers 契约'
Assert-True (-not (Test-MaxPlayersContract '2147483648')) 'int32 max+1 必须被拒'
Assert-True (-not (Test-MaxPlayersContract ('9' * 100))) '超长全数字必须在机器整数比较前被拒'
Assert-True ($overlayDockerfile -match '(?m)^ARG BASE_IMAGE\s*$' -and
    $overlayDockerfile -match '(?m)^FROM \$\{BASE_IMAGE\}\s*$' -and
    $overlayDockerfile -match '(?m)^COPY --chown=10001:10001 --chmod=0755 entrypoint\.sh /home/pandora/entrypoint\.sh\s*$') `
    '断网验收覆盖层必须只从显式本地 BASE_IMAGE 覆盖非 root 可执行 entrypoint'
Assert-True ($overlayDockerfile -notmatch '(?im)^\s*(RUN|ADD)\b') `
    'entrypoint 覆盖层禁止执行联网/包安装步骤或引入其它内容'

Write-Host 'PASS:DS entrypoint ticket 日志脱敏契约。'
