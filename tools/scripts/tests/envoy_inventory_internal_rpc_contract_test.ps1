[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$ProjectRoot = (Resolve-Path "$PSScriptRoot/../../..").Path
$EnvoyPath = Join-Path $ProjectRoot 'deploy/envoy/envoy.yaml'
$manifest = Get-Content -LiteralPath $EnvoyPath -Raw

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "ASSERT FAILED:$Message" }
}

$systemMethods = @(
    'GrantItems',
    'GrantInstances',
    'FreezeForOrder',
    'EnsureAuctionEscrow',
    'SettleAuctionMatch',
    'SettlePlayerTrade',
    'ReleaseEscrow',
    # 2026-07-25:player.SetEquipment 拥有权校验用的系统查询,玩家可调即可探测他人背包。
    'CheckItemsOwned'
)
$catchAll = 'prefix: "/pandora.inventory.v1.InventoryService/"'
$catchAllIndex = $manifest.LastIndexOf($catchAll, [StringComparison]::Ordinal)
Assert-True ($catchAllIndex -ge 0) '缺 InventoryService 客户端路由 catch-all'

foreach ($method in $systemMethods) {
    $path = "/pandora.inventory.v1.InventoryService/$method"
    $pattern = '(?ms)^\s*-\s+match:\s*\r?\n\s*path:\s*"' + [regex]::Escape($path) +
        '"\s*\r?\n\s*direct_response:\s*\{\s*status:\s*403\b'
    $matches = [regex]::Matches($manifest, $pattern)
    Assert-True ($matches.Count -eq 1) "$method 必须且只能有一条 exact-path 403"
    Assert-True ($matches[0].Index -lt $catchAllIndex) "$method 的 403 必须位于 Inventory catch-all 之前"
}

# 玩家自助方法继续由 JWT + Inventory 业务权限校验，不得被本组内部 RPC 规则误封。
foreach ($method in @('GetInventory', 'UseItem', 'SellItem', 'IdentifyItem', 'DiscardInstance', 'MoveInstance')) {
    $path = "/pandora.inventory.v1.InventoryService/$method"
    Assert-True (-not [regex]::IsMatch($manifest,
        '(?ms)path:\s*"' + [regex]::Escape($path) + '"\s*\r?\n\s*direct_response:\s*\{\s*status:\s*403\b')) `
        "$method 不得被标成内部 RPC 403"
}

Write-Host 'envoy_inventory_internal_rpc_contract_test: PASS' -ForegroundColor Green
