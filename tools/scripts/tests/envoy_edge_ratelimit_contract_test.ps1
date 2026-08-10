[CmdletBinding()]
param()

# envoy_edge_ratelimit_contract_test —— 边缘限流与受信客户端 IP 头的结构契约
# (anti-abuse-scene-entry.md §4.1/§6 第 4/5 项,2026-08-10)。
# 断言的是「配置形状」,不替代压测验收(登录洪峰 429/RESOURCE_EXHAUSTED 削平仍须实测)。

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$EnvoyPath = Join-Path $ProjectRoot 'deploy/envoy/envoy.yaml'
$manifest = Get-Content -LiteralPath $EnvoyPath -Raw

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

# 1. local_ratelimit filter 必须存在,且位于 jwt_authn 之前(在验签计算之前丢弃洪水)。
$lrlFilter = 'name: envoy.filters.http.local_ratelimit'
$lrlIndex = $manifest.IndexOf($lrlFilter, [StringComparison]::Ordinal)
Assert-True ($lrlIndex -ge 0) '缺 envoy.filters.http.local_ratelimit filter'
$jwtIndex = $manifest.IndexOf('name: envoy.filters.http.jwt_authn', [StringComparison]::Ordinal)
Assert-True ($jwtIndex -ge 0) '缺 jwt_authn filter(前提断言)'
Assert-True ($lrlIndex -lt $jwtIndex) 'local_ratelimit 必须排在 jwt_authn 之前'

# 2. filter 级不得配 token_bucket(默认不限流,只有显式 route 覆写才启用)。
$filterBlock = $manifest.Substring($lrlIndex, [Math]::Min(400, $manifest.Length - $lrlIndex))
Assert-True (-not ($filterBlock -match 'token_bucket')) 'filter 级 local_ratelimit 不得带默认 token_bucket(会波及全部路由,含 push 长连与退出路径)'

# 3. Login 必须有 exact 路由挂 local_ratelimit 覆写,且位于 login prefix catch-all 之前。
$loginExact = 'path: "/pandora.login.v1.LoginService/Login"'
$loginExactIndex = $manifest.IndexOf($loginExact, [StringComparison]::Ordinal)
Assert-True ($loginExactIndex -ge 0) '缺 login.Login exact 路由'
$loginPrefix = 'prefix: "/pandora.login.v1.LoginService/"'
$loginPrefixIndex = $manifest.LastIndexOf($loginPrefix, [StringComparison]::Ordinal)
Assert-True ($loginPrefixIndex -ge 0) '缺 login prefix catch-all'
Assert-True ($loginExactIndex -lt $loginPrefixIndex) 'Login exact 路由必须位于 login prefix catch-all 之前(否则覆写永不命中)'
$loginBlock = $manifest.Substring($loginExactIndex, [Math]::Min(1600, $manifest.Length - $loginExactIndex))
Assert-True ($loginBlock -match 'envoy\.filters\.http\.local_ratelimit') 'Login exact 路由必须挂 local_ratelimit 覆写'
Assert-True ($loginBlock -match 'token_bucket') 'Login 覆写必须配 token_bucket'
Assert-True ($loginBlock -match 'rate_limited_as_resource_exhausted:\s*true') 'Login 限流必须映射 gRPC RESOURCE_EXHAUSTED(UE 可重试语义)'

# 4. 退出/进场关键路径不得被限流覆写波及:Logout / IssueDSTicket / SelectRole / push
#    的 route 块内不得出现 local_ratelimit 覆写(验收底线 4)。
foreach ($method in @('Logout', 'IssueDSTicket', 'SelectRole')) {
    $p = 'path: "/pandora.login.v1.LoginService/' + $method + '"'
    $idx = $manifest.IndexOf($p, [StringComparison]::Ordinal)
    if ($idx -ge 0) {
        # 该方法在 jwt rules 与(可能的)路由两处出现;逐处检查其后 600 字符内无限流覆写。
        $cursor = 0
        while (($idx = $manifest.IndexOf($p, $cursor, [StringComparison]::Ordinal)) -ge 0) {
            $block = $manifest.Substring($idx, [Math]::Min(600, $manifest.Length - $idx))
            Assert-True (-not ($block -match 'local_ratelimit')) "$method 不得被边缘限流波及(退出/进场关键路径)"
            $cursor = $idx + $p.Length
        }
    }
}

# 5. 受信客户端 IP 链:入站剥离 + Envoy 权威重写,两半缺一即失去不可伪造性。
Assert-True ($manifest -match '-\s*remove:\s*"x-pandora-client-ip"') 'header_mutation 必须剥离入站 x-pandora-client-ip(防伪造)'
Assert-True ($manifest -match 'key:\s*"x-pandora-client-ip"') 'virtual_host 必须注入 x-pandora-client-ip'
Assert-True ($manifest -match 'DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT') 'x-pandora-client-ip 值必须取 Envoy 直连观测的下游地址'

Write-Host 'envoy_edge_ratelimit_contract_test: PASS' -ForegroundColor Green
