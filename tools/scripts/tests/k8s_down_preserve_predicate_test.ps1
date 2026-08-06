# k8s -Down 保留谓词的**行为**回归测试(不连集群)。
#
# 为什么要单独一份:infra_etcd_persistence_contract_test.ps1 只做静态文本断言
# (断言源码里存在 "Kind -ceq 'Namespace'" 这句),而 2026-08-06 的真实故障恰恰是
# 那句谓词**一次都没被求值到**——Remove-K8sManifestObjectsPreserving 里
#     $identities = @(Get-K8sManifestObjectIdentities ...)
# 给一个 `return , $array` 的函数外面又套了 @(),拿到的是「只含一个数组的数组」,
# 管道只喂给 Where-Object 一个对象,[string]$o.Kind 变成 "Namespace Service ..." 空格串,
# 谓词恒为 false → payload = 整份清单 → namespace/pandora 与 PVC/etcd-data 一起被删。
# 静态断言全绿,Down 每次都炸在「namespace 缺失」。
#
# 因此本测试把两个函数从 start.ps1 的 AST 里取出来真跑一遍,用 kubectl 桩截获
# 最终发给 apiserver 的 payload,断言被保留的对象**不在**删除清单里。
$ErrorActionPreference = 'Stop'

$ScriptDir   = Split-Path -Parent $MyInvocation.MyCommand.Path
$StartScript = Join-Path (Split-Path -Parent $ScriptDir) 'start.ps1'
if (-not (Test-Path -LiteralPath $StartScript -PathType Leaf)) { throw "找不到 start.ps1:$StartScript" }

$failed = 0
function Assert-True([bool]$Condition, [string]$Message) {
    if ($Condition) { Write-Host "  [PASS] $Message" -ForegroundColor DarkGray }
    else { Write-Host "  [FAIL] $Message" -ForegroundColor Red; $script:failed++ }
}

# ---- 从 start.ps1 取出被测函数(只取函数体,不执行脚本主流程)----
$tokens = $null; $errors = $null
$ast = [System.Management.Automation.Language.Parser]::ParseFile($StartScript, [ref]$tokens, [ref]$errors)
if ($errors.Count -gt 0) { throw "start.ps1 解析失败:$($errors[0].Message)" }
foreach ($name in @('Get-K8sManifestObjectIdentities', 'Remove-K8sManifestObjectsPreserving')) {
    $fn = @($ast.FindAll({ param($n)
        $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -ceq $name }, $true))
    if ($fn.Count -ne 1) { throw "start.ps1 中未找到唯一的 $name" }
    . ([scriptblock]::Create($fn[0].Extent.Text))
}

# ---- 被测函数的最小依赖桩 ----
$script:K8sDownApiserverGone = $false
function Write-Skip([string]$m) { }
function Test-K8sApiserverUnreachableText([string]$Text) { return $false }
function Test-K8sDownBenignMissingText([string]$Text) { return $false }

# kubectl 桩:create --dry-run 按文档文本回一个身份三元组;delete -f - 记下 payload。
$script:CapturedDeletePayload = $null
function kubectl {
    $stdin = ($input | Out-String)
    $global:LASTEXITCODE = 0
    if ($args -contains 'create') {
        $kind = ([regex]::Match($stdin, '(?m)^\s*kind:\s*(\S+)\s*$')).Groups[1].Value
        $name = ([regex]::Match($stdin, '(?m)^\s*name:\s*(\S+)\s*$')).Groups[1].Value
        $ns   = ([regex]::Match($stdin, '(?m)^\s*namespace:\s*(\S+)\s*$')).Groups[1].Value
        return "$kind|$name|$ns"
    }
    if ($args -contains 'delete') { $script:CapturedDeletePayload = $stdin; return @() }
    throw "测试桩未预期的 kubectl 调用:$($args -join ' ')"
}

# ---- 用例 1:services 形态——保留 Namespace/pandora ----
$K8sNamespace = 'pandora'
$servicesManifest = @'
apiVersion: v1
kind: Namespace
metadata:
  name: pandora
---
apiVersion: v1
kind: Service
metadata:
  name: auction
  namespace: pandora
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: player
  namespace: pandora
'@

Write-Host 'services:保留 namespace/pandora'
$script:CapturedDeletePayload = $null
$ids = Remove-K8sManifestObjectsPreserving -KubeContext 'stub' -ManifestText $servicesManifest `
    -What 'test services' -ShouldPreserve {
        param($o)
        ([string]$o.Kind -ceq 'Namespace') -and ([string]$o.Name -ceq $K8sNamespace)
    }
Assert-True ($ids.Count -eq 3) "身份清单必须逐对象展开(期望 3,实际 $($ids.Count))"
Assert-True ($null -ne $script:CapturedDeletePayload) '必须真的发出了一次 delete'
Assert-True ($script:CapturedDeletePayload -notmatch '(?m)^\s*kind:\s*Namespace\s*$') `
    'delete payload 中不得包含 Namespace 对象'
Assert-True ($script:CapturedDeletePayload -match '(?m)^\s*name:\s*auction\s*$' -and
             $script:CapturedDeletePayload -match '(?m)^\s*name:\s*player\s*$') `
    '除保留对象外的其余对象仍必须被删除'

# ---- 用例 2:infra 形态——保留 PVC/etcd-data ----
$infraManifest = @'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mysql
  namespace: pandora
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: etcd-data
  namespace: pandora
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: etcd
  namespace: pandora
'@

Write-Host 'infra:保留 PVC/etcd-data'
$script:CapturedDeletePayload = $null
$ids2 = Remove-K8sManifestObjectsPreserving -KubeContext 'stub' -ManifestText $infraManifest `
    -What 'test infra' -ShouldPreserve {
        param($o)
        ([string]$o.Kind -ceq 'PersistentVolumeClaim') -and
        ([string]$o.Name -ceq 'etcd-data') -and
        ([string]$o.Namespace -ceq $K8sNamespace)
    }
Assert-True ($ids2.Count -eq 3) "身份清单必须逐对象展开(期望 3,实际 $($ids2.Count))"
Assert-True ($script:CapturedDeletePayload -notmatch '(?m)^\s*name:\s*etcd-data\s*$') `
    'delete payload 中不得包含 PVC/etcd-data'
Assert-True ($script:CapturedDeletePayload -match '(?m)^\s*name:\s*mysql\s*$') `
    '其余 infra 对象仍必须被删除'

# ---- 用例 3:空清单必须 fail closed(旧写法下 Count 恒为 1,这道守卫是死的)----
Write-Host '空清单:必须拒绝继续 Down'
$threw = $false
try {
    $null = Remove-K8sManifestObjectsPreserving -KubeContext 'stub' -ManifestText "# 只有注释`n" `
        -What 'test empty' -ShouldPreserve { param($o) $false }
} catch { $threw = $true }
Assert-True $threw '清单为空时必须抛错,不得在未知状态下继续删除'

if ($failed -gt 0) {
    Write-Host "k8s_down_preserve_predicate_test: FAIL($failed 项)" -ForegroundColor Red
    exit 1
}
Write-Host 'k8s_down_preserve_predicate_test: PASS' -ForegroundColor Green
