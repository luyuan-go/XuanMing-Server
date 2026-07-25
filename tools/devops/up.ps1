#!/usr/bin/env pwsh
# Pandora DevOps 栈 —— 一键起。可原样搬到另一台机器（无绝对路径/无机器 IP）。
#   pwsh ./up.ps1            起全栈
#   pwsh ./up.ps1 -Pull      起前先拉最新基础镜像
[CmdletBinding()]
param(
    [switch]$Pull
)

$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    $compose = 'docker-compose.stack.yml'

    # 1) 前置检查：Docker + Compose v2
    Write-Host '==> 检查前置...' -ForegroundColor Cyan
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'Docker 未安装或不在 PATH。请先装 Docker Desktop（Windows）/ Docker Engine（Linux）。'
    }
    try { docker info *> $null } catch { throw 'Docker 守护进程未运行，请先启动 Docker Desktop。' }
    $composeVersion = (docker compose version) 2>$null
    if (-not $composeVersion) { throw 'docker compose (v2) 不可用。请升级 Docker 到自带 compose v2 的版本。' }

    # 2) 无 .env 则从模板生成（首启即可跑；生产务必改口令）
    if (-not (Test-Path '.env')) {
        Copy-Item '.env.example' '.env'
        Write-Host '==> 已从 .env.example 生成 .env —— 首启用默认口令，正式环境请改后重跑。' -ForegroundColor Yellow
    }

    if ($Pull) {
        Write-Host '==> 拉取基础镜像...' -ForegroundColor Cyan
        docker compose -f $compose pull registry registry-ui minio minio-init
    }

    # 3) 构建 Jenkins 镜像并起栈
    Write-Host '==> 构建并启动全栈（首次会拉镜像+装插件，约几分钟）...' -ForegroundColor Cyan
    docker compose -f $compose up -d --build

    # 4) 读回实际端口/口令用于展示
    $env:DUMMY = $null
    $envMap = @{}
    Get-Content '.env' | ForEach-Object {
        if ($_ -match '^\s*([^#=]+)=(.*)$') { $envMap[$Matches[1].Trim()] = $Matches[2].Trim() }
    }
    function Val($k, $d) { if ($envMap.ContainsKey($k) -and $envMap[$k]) { $envMap[$k] } else { $d } }

    $jPort  = Val 'JENKINS_HTTP_PORT' '8080'
    $rPort  = Val 'REGISTRY_PORT' '5000'
    $ruPort = Val 'REGISTRY_UI_PORT' '8082'
    $mApi   = Val 'MINIO_API_PORT' '9000'
    $mCon   = Val 'MINIO_CONSOLE_PORT' '9001'
    $jUser  = Val 'JENKINS_ADMIN_ID' 'admin'
    $jPass  = Val 'JENKINS_ADMIN_PASSWORD' 'change-me-admin'
    $mUser  = Val 'MINIO_ROOT_USER' 'pandora'
    $mPass  = Val 'MINIO_ROOT_PASSWORD' 'change-me-minio'

    Write-Host ''
    Write-Host '========== Pandora DevOps 已启动 ==========' -ForegroundColor Green
    Write-Host ("  Jenkins        http://localhost:{0}      账号 {1} / {2}" -f $jPort, $jUser, $jPass)
    Write-Host ("  Registry API   http://localhost:{0}      (docker push localhost:{0}/pandora/<svc>:<tag>)" -f $rPort)
    Write-Host ("  Registry UI    http://localhost:{0}" -f $ruPort)
    Write-Host ("  MinIO 控制台   http://localhost:{0}      账号 {1} / {2}" -f $mCon, $mUser, $mPass)
    Write-Host ("  MinIO S3 API   http://localhost:{0}" -f $mApi)
    Write-Host '==========================================' -ForegroundColor Green
    Write-Host 'Jenkins 首启装插件需 1-3 分钟，登录 502/未就绪属正常，稍等重刷。' -ForegroundColor DarkGray
    Write-Host '状态: docker compose -f docker-compose.stack.yml ps   日志: ... logs -f jenkins' -ForegroundColor DarkGray
}
finally {
    Pop-Location
}
