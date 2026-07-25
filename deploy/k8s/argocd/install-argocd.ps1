#!/usr/bin/env pwsh
# 安装 Argo CD（发布线④部署层 GitOps）到当前 kubectl context。
#
# 用法：
#   pwsh deploy/k8s/argocd/install-argocd.ps1                    # 装 + 打印访问方式
#   pwsh deploy/k8s/argocd/install-argocd.ps1 -Version v3.4.5    # 指定版本（默认见下）
#   pwsh deploy/k8s/argocd/install-argocd.ps1 -PrintPasswordOnly # 只取初始 admin 密码
#
# 设计要点：
#   - **版本钉死**：不用 stable/latest，保证另一台机器装出同一版本（可复现）。
#   - **server-side apply**：applicationsets CRD 超过 kubectl 客户端 262144 字节注解上限，
#     普通 apply 会报 "metadata.annotations: Too long"。server-side 是官方修法。
#   - **幂等**：重复跑安全。
#
# 镜像来源：quay.io（argocd）+ ghcr.io（dex）+ public.ecr.aws（redis）。
# 都不是被墙的 docker.io CDN，墙内可直接拉（本机实测三个源均可达）。
[CmdletBinding()]
param(
    [string]$Version = 'v3.4.5',
    [string]$Namespace = 'argocd',
    [switch]$PrintPasswordOnly
)

$ErrorActionPreference = 'Stop'

if (-not (Get-Command kubectl -ErrorAction SilentlyContinue)) {
    throw "找不到 kubectl。"
}

$ctx = (& kubectl config current-context 2>$null)
if ([string]::IsNullOrWhiteSpace($ctx)) { throw "kubectl 没有当前 context。" }

function Get-AdminPassword {
    param([string]$Ns)
    $b64 = & kubectl -n $Ns get secret argocd-initial-admin-secret -o jsonpath='{.data.password}' 2>$null
    if ([string]::IsNullOrWhiteSpace($b64)) { return $null }
    return [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($b64))
}

if ($PrintPasswordOnly) {
    $pw = Get-AdminPassword -Ns $Namespace
    if ($pw) { Write-Host $pw } else { Write-Host "(取不到 argocd-initial-admin-secret；可能已被删除或改过密码)" -ForegroundColor Yellow }
    return
}

Write-Host "[argocd] 目标 context = $ctx，namespace = $Namespace，版本 = $Version" -ForegroundColor Cyan

# 1) namespace（幂等）
& kubectl create namespace $Namespace --dry-run=client -o yaml | & kubectl apply -f - | Out-Null

# 2) 官方安装清单（钉版本）
$url = "https://raw.githubusercontent.com/argoproj/argo-cd/$Version/manifests/install.yaml"
Write-Host "[argocd] apply（server-side）：$url" -ForegroundColor Cyan
& kubectl apply --server-side --force-conflicts -n $Namespace -f $url
if ($LASTEXITCODE -ne 0) {
    throw "argocd 清单 apply 失败。墙内网络较慢时可先手动下载 install.yaml 再 kubectl apply --server-side -f <本地文件>。"
}

# 3) 等就绪（首次要拉 quay/ghcr/ecr 镜像，慢）
Write-Host "[argocd] 等待 Pod Ready（首次拉镜像可能数分钟）..." -ForegroundColor Cyan
& kubectl -n $Namespace wait --for=condition=Ready pod --all --timeout=600s
if ($LASTEXITCODE -ne 0) {
    Write-Host "[argocd] 等待超时。排查：kubectl -n $Namespace get pods / describe pod <name>" -ForegroundColor Yellow
}

$pw = Get-AdminPassword -Ns $Namespace

Write-Host ""
Write-Host "========== Argo CD 就绪 ==========" -ForegroundColor Green
Write-Host "  访问（前台占用当前终端）：kubectl -n $Namespace port-forward svc/argocd-server 8090:443"
Write-Host "  然后打开：https://localhost:8090   （自签证书，浏览器需手动信任）"
Write-Host "  用户名：admin"
if ($pw) { Write-Host "  初始密码：$pw" } else { Write-Host "  初始密码：取不到（可能已改）" -ForegroundColor Yellow }
Write-Host "==================================" -ForegroundColor Green
Write-Host ""
Write-Host "下一步（注意先读 application-services.yaml 顶部的 prune 警告）：" -ForegroundColor Yellow
Write-Host "  kubectl apply -f deploy/k8s/argocd/application-services.yaml"
