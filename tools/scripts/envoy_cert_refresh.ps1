<#
.SYNOPSIS
  重签本机 Envoy dev TLS 证书,并把新证书推给集群内边缘 Envoy(pandora-edge-envoy)。

.DESCRIPTION
  本机局域网 IP 随 DHCP 变化后,旧证书 SAN 仍写着老地址:文件俱在、Envoy 照常加载、
  TLS 握手也成功,但客户端用新 IP 连时主机名校验失败 ——
      SSL: no alternative certificate subject name matches target ipv4 address
  表现极像"登录服务挂了"。修它需要三步,少一步都不生效:
      1. 重签叶子证书(SAN 含本机当前 IP)
      2. 更新 k8s Secret pandora-edge-envoy-certs
      3. 滚动 deploy/pandora-edge-envoy(证书是挂载进 Pod 的,改文件不会自动重载)
  这三步原本只内嵌在 start.ps1 的 [7.5/8],单独跑不了,只能整跑一遍一键启动。
  本脚本把它们抽成独立入口,幂等,可随时单跑。

  签发用本机 mkcert CAROOT 里的根 CA。若该 CA 是全队共享 dev CA,客户端已导入的 CA 不变,
  **无需重新导入、无需重启 UE 编辑器**(客户端信任的是根 CA,不是叶子证书)。

  ⚠️ 仅限本地开发。生产 Envoy 用公网 CA 签真实域名证书,与本脚本无关。
     为防误操作,目标 context 必须指向本机 minikube,否则 fail-fast。

.PARAMETER KubeContext
  目标 kube-context。留空则用 kubectl current-context。

.PARAMETER Namespace
  边缘 Envoy 所在 namespace,默认 pandora。

.PARAMETER SkipRollout
  只重签证书,不碰集群(用于 docker-compose 模式,或想自己决定何时滚动)。

.EXAMPLE
  pwsh tools/scripts/envoy_cert_refresh.ps1

.EXAMPLE
  pwsh tools/scripts/envoy_cert_refresh.ps1 -SkipRollout
#>
[CmdletBinding()]
param(
    [string]$KubeContext = '',
    [string]$Namespace = 'pandora',
    [switch]$SkipRollout
)

$ErrorActionPreference = 'Stop'
$ScriptDir = $PSScriptRoot
$ProjectRoot = (Resolve-Path "$ScriptDir/../..").Path
$EnvoyDir = Join-Path $ProjectRoot 'deploy/envoy'
$CertPem = Join-Path $EnvoyDir 'cert.pem'
$KeyPem = Join-Path $EnvoyDir 'key.pem'

function Write-Info($m) { Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Write-Ok($m) { Write-Host "[ OK ] $m" -ForegroundColor Green }
function Write-Step($m) { Write-Host "`n=== $m ===" -ForegroundColor Magenta }

# envoy_cert.ps1 是函数库,必须 dot-source 才有 Confirm-EnvoyDevCert(以 -File 直跑是空操作)。
. (Join-Path $ScriptDir 'envoy_cert.ps1')

Write-Step '[1/3] 校验 / 重签本机 Envoy dev 证书'
# 先确保本机 mkcert 用的是全队共享 dev CA:私钥就位则自动装,否则仅提示、退回本机独立 CA。
# 放在重签之前,因为换 CA 会作废旧叶子证书,顺序反了会白签一次。
Confirm-SharedDevCa -ProjectRoot $ProjectRoot | Out-Null
# 幂等:SAN 已覆盖本机所有地址则静默通过,不会无谓重签。
Confirm-EnvoyDevCert -EnvoyDir $EnvoyDir
if (-not (Test-Path $CertPem) -or -not (Test-Path $KeyPem)) {
    throw "证书重签后仍缺失($CertPem / $KeyPem);多半是 mkcert 不可用:winget install FiloSottile.mkcert 后重跑。"
}

if ($SkipRollout) {
    Write-Ok '已跳过集群下发(-SkipRollout)。docker-compose 模式请重建 envoy 容器让新证书生效。'
    return
}

Write-Step '[2/3] 下发到 k8s Secret pandora-edge-envoy-certs'
if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    throw 'kubectl 不可用,无法下发证书。只想重签本机文件请加 -SkipRollout。'
}
if ([string]::IsNullOrWhiteSpace($KubeContext)) {
    $KubeContext = (kubectl config current-context 2>$null)
    if ($LASTEXITCODE -ne 0) { $KubeContext = '' }
    # current-context 没设是常态(一键启动全程用 kubectl --context 显式钉住,从不 use-context)。
    # 此时不该直接报错:kubeconfig 里若只有一个 context,它就是唯一可能的目标;
    # 多个则退回 minikube 同名 context。两者都不成立才要求显式指定。
    if ([string]::IsNullOrWhiteSpace($KubeContext)) {
        $all = @(kubectl config get-contexts -o name 2>$null | Where-Object { $_ })
        if ($all.Count -eq 1) {
            $KubeContext = $all[0]
            Write-Info "kubeconfig 未设 current-context,采用唯一 context『$KubeContext』。"
        } elseif ($all -contains 'minikube') {
            $KubeContext = 'minikube'
            Write-Info "kubeconfig 未设 current-context,采用本地『minikube』。"
        } else {
            throw "kubeconfig 未设 current-context 且无法唯一确定目标(现有:$($all -join ', '));请用 -KubeContext 显式指定。"
        }
    }
    $KubeContext = $KubeContext.Trim()
}

# 只比对 context 名不足以保证安全:别人 kubeconfig 里完全可能有同名 context 却指向线上集群。
# 这里取 apiserver 的 host,必须是回环或 minikube 节点 IP 才继续 —— dev 自签证书绝不能推上生产。
$cluster = (kubectl config view -o jsonpath="{.contexts[?(@.name==`"$KubeContext`")].context.cluster}" 2>$null)
$server = if ($cluster) { kubectl config view -o jsonpath="{.clusters[?(@.name==`"$cluster`")].cluster.server}" 2>$null } else { '' }
$apiHost = ''
if (-not [string]::IsNullOrWhiteSpace($server)) {
    try { $apiHost = ([System.Uri]$server).Host } catch { $apiHost = '' }
}
$isLocal = $apiHost -in @('127.0.0.1', 'localhost', '::1', '[::1]')
if (-not $isLocal -and -not [string]::IsNullOrWhiteSpace($apiHost)) {
    $mkIp = (& minikube -p $KubeContext ip 2>$null)
    if ($LASTEXITCODE -eq 0 -and $mkIp -and $apiHost -eq $mkIp.Trim()) { $isLocal = $true }
}
if (-not $isLocal) {
    throw "kube-context『$KubeContext』的 apiserver($apiHost)不是本机 minikube。mkcert 自签证书只用于本地开发,拒绝下发。"
}
Write-Info "目标:context=$KubeContext namespace=$Namespace"

$ctxArgs = @('--context', $KubeContext)
kubectl @ctxArgs create secret generic pandora-edge-envoy-certs `
    --from-file=cert.pem=$CertPem --from-file=key.pem=$KeyPem -n $Namespace `
    --dry-run=client -o yaml | kubectl @ctxArgs apply -f -
if ($LASTEXITCODE -ne 0) { throw 'kubectl apply secret pandora-edge-envoy-certs 失败' }
Write-Ok 'Secret 已更新'

Write-Step '[3/3] 滚动 deploy/pandora-edge-envoy'
# Secret 变更不会触发 Pod 重建,也不会被运行中的 Envoy 进程重读 —— 必须显式滚动并等就绪,
# 否则脚本"成功"了但线上跑的还是旧证书,排障时极具误导性。
kubectl @ctxArgs rollout restart deploy/pandora-edge-envoy -n $Namespace
if ($LASTEXITCODE -ne 0) { throw 'rollout restart pandora-edge-envoy 失败(该 Deployment 存在吗?先跑一键启动部署集群)' }
kubectl @ctxArgs rollout status deploy/pandora-edge-envoy -n $Namespace --timeout=120s
if ($LASTEXITCODE -ne 0) {
    throw "pandora-edge-envoy 未在 120s 内就绪;排障:kubectl --context $KubeContext -n $Namespace describe deploy/pandora-edge-envoy"
}

Write-Host ''
Write-Ok '证书已重签并生效。客户端无需重新导入 CA(根 CA 未变),直接重连即可。'
Write-Info "自检:curl.exe -sv https://<本机IP>:8443/ --cacert `"$ProjectRoot\deploy\dev-ca\pandora-dev-rootCA.pem`""
