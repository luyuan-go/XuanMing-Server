#!/usr/bin/env pwsh
# 停 Pandora DevOps 栈。
#   pwsh ./down.ps1            停容器，保留数据卷（Jenkins/镜像/制品都还在）
#   pwsh ./down.ps1 -Volumes   连数据卷一起删（彻底清空，慎用）
[CmdletBinding()]
param(
    [switch]$Volumes
)

$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    $compose = 'docker-compose.stack.yml'
    if ($Volumes) {
        Write-Host '==> 停栈并删除数据卷（Jenkins 配置 / 镜像 / 制品全部清空）...' -ForegroundColor Yellow
        docker compose -f $compose down -v
    } else {
        Write-Host '==> 停栈（保留数据卷）...' -ForegroundColor Cyan
        docker compose -f $compose down
    }
    Write-Host '已停止。' -ForegroundColor Green
}
finally {
    Pop-Location
}
