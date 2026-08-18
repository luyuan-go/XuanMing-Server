[CmdletBinding()]
param()

# envoy_ds_identity_header_strip_contract_test —— DS 面身份头剥离清单的结构契约
# (账号 / 角色分离 2026-08-18)。
#
# 守的是什么:DS 面(:8444 / k8s ds-envoy)不挂 jwt_authn,后端 pmw.AuthOptional 直接读
# x-pandora-* 身份头。这些头在 DS 面**没有任何合法来源**,必须无条件剥离,否则任何能连上
# DS 面的调用方都能自带一个身份头冒充别人 —— 例如自带 x-pandora-account-id 打
# ListAccountRoles / EnterRole,列出并进入别人账号下的角色。
#
# 为什么需要这道门:同一份剥离清单被**两份互不引用的 yaml** 各写一遍
#   - deploy/envoy/envoy.yaml            的 :8444 listener(dev / compose 形态)
#   - deploy/k8s/agones/16-ds-envoy.yaml 的 ds sidecar(集群形态)
# 加新身份头的人只会想到改自己在用的那一份。2026-08-18 实测:主 envoy 已经剥了
# x-pandora-account-id 与 x-pandora-client-ip,k8s 那份两个都漏了,而集群形态才是生产。
# 漏剥离不会让任何测试变红、不会让 Envoy 启动失败,只在被人利用时才暴露。
#
# 断言两条:① 集群那份 ⊇ 主 envoy DS 面那份(新增头必须两边一起加);
#          ② 三个身份头显式必存(防止「两边一起删」把 ① 变成永真式)。

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$MainEnvoy = Join-Path $ProjectRoot 'deploy/envoy/envoy.yaml'
$K8sEnvoy = Join-Path $ProjectRoot 'deploy/k8s/agones/16-ds-envoy.yaml'

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

function Get-RemovedHeaders([string]$Text) {
    $set = [System.Collections.Generic.HashSet[string]]::new()
    foreach ($m in [regex]::Matches($Text, '-\s*remove:\s*"([^"]+)"')) {
        [void]$set.Add($m.Groups[1].Value)
    }
    return $set
}

# 主 envoy 有两个 header_mutation 块(:8443 客户端面 / :8444 DS 面),这里只取 DS 面那块。
# 锚点是 DS 面注释里那句话;它变了本测试要红,那是对的 —— 说明该 listener 被改动过。
$mainRaw = Get-Content -LiteralPath $MainEnvoy -Raw
$dsAnchor = 'DS 面不挂 jwt_authn'
$dsAnchorIndex = $mainRaw.IndexOf($dsAnchor, [StringComparison]::Ordinal)
Assert-True ($dsAnchorIndex -ge 0) "主 envoy 里找不到 DS 面锚点注释「$dsAnchor」(listener 结构变了,请同步本测试)"
$routeIndex = $mainRaw.IndexOf('route_config:', $dsAnchorIndex, [StringComparison]::Ordinal)
Assert-True ($routeIndex -gt $dsAnchorIndex) '主 envoy DS 面 header_mutation 之后找不到 route_config,无法界定块范围'
$mainDsBlock = $mainRaw.Substring($dsAnchorIndex, $routeIndex - $dsAnchorIndex)

$mainRemoved = Get-RemovedHeaders $mainDsBlock
$k8sRemoved = Get-RemovedHeaders (Get-Content -LiteralPath $K8sEnvoy -Raw)

Assert-True ($mainRemoved.Count -gt 0) '主 envoy DS 面一个 remove 都没解析出来(正则或块界定失效)'
Assert-True ($k8sRemoved.Count -gt 0) 'k8s ds-envoy 一个 remove 都没解析出来'

# ① 集群那份必须覆盖主 envoy DS 面的每一项。
$missing = @($mainRemoved | Where-Object { -not $k8sRemoved.Contains($_) })
Assert-True ($missing.Count -eq 0) ("k8s ds-envoy 漏剥离了主 envoy DS 面已剥离的头:" + ($missing -join ', ') +
    " —— 集群形态才是生产,漏一个就等于该头在生产上可被伪造")

# ② 身份头显式必存,防止两边一起删。
$mustStrip = @('x-pandora-player-id', 'x-pandora-account-id', 'x-pandora-jwt-payload')
foreach ($h in $mustStrip) {
    Assert-True ($mainRemoved.Contains($h)) "主 envoy DS 面必须剥离身份头 $h"
    Assert-True ($k8sRemoved.Contains($h)) "k8s ds-envoy 必须剥离身份头 $h"
}

Write-Output ("[PASS] envoy_ds_identity_header_strip_contract_test:两份 DS 面剥离清单一致(" +
    ($k8sRemoved.Count) + " 项),三个身份头均已剥离")
