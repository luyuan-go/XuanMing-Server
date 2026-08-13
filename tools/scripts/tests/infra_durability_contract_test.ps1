# infra_durability_contract_test.ps1 —— INC-20260812-002 的回归门禁。
#
# 这份测试守两条**配置级**约束。它们没法用普通单测覆盖(修复本身就是 YAML 与生成脚本,
# 不是 Go 代码),但一旦回退就会重现 P0,所以必须有机械断言:
#
#   ① 本地 MySQL 必须放宽双 fsync。回退到 trx_commit=1 + sync_binlog=1 会让每次事务提交
#      在 Docker Desktop 虚拟磁盘上做两次 fsync(实测长尾 19.4 秒)→ owner.QueryOwner 超时
#      → hub-allocator fail-closed → **玩家在大厅被踢**。
#   ② edge envoy 的上游必须是全限定名。写成短名时 DNS 搜索域一旦失效,解析会落到宿主 DNS,
#      而 Docker Desktop 对任何未知名字都返回自己的网关 192.168.65.254 → 18 个上游同时废掉
#      → **登录 503**。
#
# 刻意**不**断言"延迟小于某个阈值":那种断言依赖跑测机器的磁盘,必然 flaky,
# 而真正会回退的是配置本身。这里守配置,延迟由 §8 的人工实测与 A-6 观察窗口负责。
#
# 用法:pwsh tools/scripts/tests/infra_durability_contract_test.ps1
[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $PSScriptRoot))
$failures = New-Object 'System.Collections.Generic.List[string]'

function Assert-True([bool]$Cond, [string]$What) {
    if ($Cond) { Write-Host "  [PASS] $What" }
    else { Write-Host "  [FAIL] $What" -ForegroundColor Red; $script:failures.Add($What) }
}

Write-Host '=== ① 本地 MySQL 持久化(INC-20260812-002 §5.1) ==='
$infra = Get-Content (Join-Path $repoRoot 'deploy/k8s/infra/infra.yaml') -Raw
Assert-True ($infra -match '--innodb-flush-log-at-trx-commit=2') `
    'infra.yaml 声明 innodb-flush-log-at-trx-commit=2'
Assert-True ($infra -match '--sync-binlog=0') `
    'infra.yaml 声明 sync-binlog=0'
# 反向断言:确认没有把生产值写回来。=1 出现即回退。
Assert-True (-not ($infra -match '--innodb-flush-log-at-trx-commit=1')) `
    'infra.yaml 未出现 trx_commit=1(回退哨兵)'

# 这份放宽只有在"线上不引用 infra/"的前提下才是安全的。前提被破坏时必须立刻失败,
# 否则会把开发期的弱持久化带上生产。
Write-Host '=== ② 线上 overlay 不得引用 infra/(放宽的前提) ==='
$onlineKust = Get-Content (Join-Path $repoRoot 'deploy/k8s/overlays/online/kustomization.yaml') -Raw
Assert-True (-not ($onlineKust -match 'infra')) `
    'overlays/online 未引用 infra/(否则弱持久化会上生产)'

Write-Host '=== ③ edge envoy 上游必须全限定名(INC-20260812-002 §5.5) ==='
$startPs1 = Get-Content (Join-Path $repoRoot 'tools/scripts/start.ps1') -Raw
Assert-True ($startPs1 -match 'svc\.cluster\.local') `
    'start.ps1 的 edge envoy 改写产出全限定名'

# 行为级断言:真正载入函数跑一遍,不只是 grep 文本。
# grep 只能证明"字符串在文件里",证明不了"改写结果正确"。
$m = [regex]::Match($startPs1, '(?s)function Convert-EdgeEnvoyConfigForCluster.*?\n}')
Assert-True $m.Success 'start.ps1 能定位到 Convert-EdgeEnvoyConfigForCluster'
if ($m.Success) {
    Invoke-Expression $m.Value
    $sample = @'
admin:
  address:
    socket_address:
      address: 0.0.0.0
static_resources:
  clusters:
  - name: login_cluster
    load_assignment:
      endpoints:
      - lb_endpoints:
        - endpoint:
            address:
              socket_address:
                address: host.docker.internal
                port_value: 20001
'@
    $out = Convert-EdgeEnvoyConfigForCluster -Text $sample -Namespace 'pandora'
    Assert-True ($out -match 'address: login\.pandora\.svc\.cluster\.local') `
        '改写产出 login.pandora.svc.cluster.local'
    # 短名哨兵:出现"孤零零的 address: login"就是回退了。
    Assert-True (-not ($out -match '(?m)^\s*address: login\s*$')) `
        '改写产出不含短名 address: login(回退哨兵)'
    # admin 必须锁回环:改写不能把 admin 也一起改到集群地址上。
    Assert-True ($out -match 'address: 127\.0\.0\.1') `
        'admin 仍锁在 127.0.0.1'
}

Write-Host ''
if ($failures.Count -gt 0) {
    Write-Host "FAILED: $($failures.Count) 条断言未通过" -ForegroundColor Red
    $failures | ForEach-Object { Write-Host "  - $_" -ForegroundColor Red }
    exit 1
}
Write-Host 'PASSED: infra 持久化与 edge envoy 全限定名契约全部满足' -ForegroundColor Green
