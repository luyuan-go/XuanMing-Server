# Pandora 开发环境状态查看
#
# 用法:
#   pwsh tools/scripts/dev_status.ps1

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path "$PSScriptRoot/../.."
$ComposeFile = "$ProjectRoot/deploy/docker-compose.dev.yml"
$EnvFile     = "$ProjectRoot/deploy/env/dev.env"

# 同 dev_up / dev_down:缺 dev.env 时 compose ps 会直接报错,状态一行都看不到。
. "$PSScriptRoot/dev_env_file.ps1"
Confirm-DevEnvFile -ProjectRoot $ProjectRoot

Write-Host "===== Pandora dev infra status =====" -ForegroundColor Cyan
docker compose -f $ComposeFile --env-file $EnvFile ps

Write-Host ""
Write-Host "===== 端口监听 =====" -ForegroundColor Cyan
# 3307 mysql / 6380 redis / 9093 kafka / 2380 etcd-client / 2381 etcd-peer
# 9091 prometheus / 3001 grafana / 8443 envoy(客户端入口) / 9901 envoy-admin
$ports = @(3307, 6380, 9093, 2380, 2381, 9091, 3001, 8443, 9901)
foreach ($p in $ports) {
    $client = [System.Net.Sockets.TcpClient]::new()
    $connect = $client.BeginConnect("127.0.0.1", $p, $null, $null)
    $ok = $connect.AsyncWaitHandle.WaitOne(500, $false)
    if ($ok) {
        try { $client.EndConnect($connect) } catch { $ok = $false }
    }
    $client.Close()

    if ($ok) {
        Write-Host "  [OK] :$p" -ForegroundColor Green
    } else {
        Write-Host "  [--] :$p" -ForegroundColor DarkGray
    }
}
