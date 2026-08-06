# 登录调试手册

> 用途:本机用 VS Code / PowerShell 调试 Pandora login 服务。
> 当前阶段先直连 login 的本地 HTTP / gRPC 端口,不走 UE 客户端,也不走 Envoy gRPC-Web。

## 1. 启动顺序

### 1.1 基础设施

在仓库根目录 `E:\work\Pandora` 执行:

```powershell
docker compose -f deploy/docker-compose.dev.yml --env-file deploy/env/dev.env up -d mysql redis
```

检查:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

至少应看到:

- `pandora-mysql` healthy,端口 `3307`
- `pandora-redis` healthy,端口 `6380`

### 1.2 登录依赖服务

login 当前开发配置会拨 `player_locator` 和 `hub_allocator`。分别开两个终端:

```powershell
go run ./services/runtime/player_locator/cmd/locator -conf services/runtime/player_locator/etc/locator-dev.yaml
```

```powershell
go run ./services/battle/hub_allocator/cmd/hub_allocator -conf services/battle/hub_allocator/etc/hub_allocator-dev.yaml
```

端口:

- `player_locator`:gRPC `20006`,HTTP `21006`
- `hub_allocator`:gRPC `20021`,HTTP `21021`

### 1.3 VS Code 启 login

VS Code 使用 [.vscode/launch.json](../../.vscode/launch.json) 里的 `Debug login`:

```json
{
  "name": "Debug login",
  "type": "go",
  "request": "launch",
  "mode": "auto",
  "program": "${workspaceFolder}/services/account/login/cmd/login",
  "cwd": "${workspaceFolder}",
  "args": [
    "-conf",
    "${workspaceFolder}/services/account/login/etc/login-dev.yaml"
  ]
}
```

启动日志里应看到:

```text
account_seed_done account=test ... created=false
account_repo_mysql ...
redis_connected addr=127.0.0.1:6380
locator_dial_ok addr=127.0.0.1:20006
hub_allocator_dial_ok addr=127.0.0.1:20021
service_ready ... account_repo=mysql session_repo=redis hub_assigner=grpc
```

## 2. 测登录

PowerShell 推荐用 `Invoke-RestMethod`,避免 `curl.exe` 的 JSON 引号坑:

```powershell
$body = @{
  account = "test1"
  password_hash = "abc"
  device_id = "login-debug"
} | ConvertTo-Json -Compress

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:21001/v1/login" `
  -ContentType "application/json" `
  -Body $body
```

成功返回应包含:

- `code = OK`
- `playerId`
- `sessionToken`
- `hubDsAddr`,例如 `127.0.0.1:7778`
- `hubTicket`

也可以用 `curl.exe`,但建议用临时变量:

```powershell
$body = '{"account":"test1","password_hash":"abc","device_id":"login-debug"}'
curl.exe -X POST "http://127.0.0.1:21001/v1/login" `
  -H "Content-Type: application/json" `
  --data-raw $body
```

## 3. 账号数据

开发配置会在 login 启动时确认 / 创建种子账号:

- 账号:`test`
- 密码:`abc`
- 配置位置:`services/account/login/etc/login-dev.yaml`
- 创建逻辑:`services/account/login/cmd/login/main.go`
- 表:`pandora_account.accounts`

本机当前额外插了 demo 账号:

- `test1` 到 `test10000`
- 密码均为 `abc`

检查账号数量:

```powershell
docker exec pandora-mysql mysql -upandora -ppandora_dev_pwd -D pandora_account -N -e "SELECT COUNT(*) FROM accounts; SELECT account, player_id FROM accounts WHERE account IN ('test','test1','test10000') ORDER BY account;"
```

## 4. 建议断点

RPC / HTTP 入口:

- `services/account/login/internal/service/login.go`
- `func (s *LoginService) Login(...)`

业务流程:

- `services/account/login/internal/biz/login.go`
- `func (u *LoginUsecase) Login(...)`
- `resolveHub(...)`

MySQL 查询:

- `services/account/login/internal/data/account.go`
- `FindByAccount(...)`
- `CheckBanned(...)`

注意:HTTP 请求有 deadline。断点停太久后继续执行,可能看到:

```text
mysql find account: context deadline exceeded
mysql check banned: context deadline exceeded
```

这不是账号不存在,而是这次请求的 `ctx` 已经过期。慢慢单步时建议重新发请求,或用 gRPC 加长超时:

```powershell
grpcurl -plaintext -max-time 300 `
  -d '{"account":"test1","password_hash":"abc","device_id":"login-debug"}' `
  127.0.0.1:20001 pandora.login.v1.LoginService/Login
```

## 5. 常见问题

### 5.1 CODEC invalid value account

典型返回:

```json
{"code":400,"reason":"CODEC","message":"body unmarshal proto: syntax error (line 1:2): invalid value account"}
```

原因通常是 PowerShell + `curl.exe` 把 JSON 双引号吃掉了。用 `Invoke-RestMethod` 或先把 JSON 放到 `$body` 变量。

### 5.2 passwordHash / deviceId 不生效

当前 HTTP 入口使用 proto 原字段名,请求体用:

```json
{"account":"test1","password_hash":"abc","device_id":"login-debug"}
```

不要写:

```json
{"account":"test1","passwordHash":"abc","deviceId":"login-debug"}
```

### 5.3 UE 客户端连 Envoy :8443 报 SSL 错误(`libcurl error 35 / SSL_ERROR_SYSCALL`)

典型 UE 日志:

```text
LogHttp: Warning: ... request failed, libcurl error: 35 (SSL connect error)
LogHttp: Warning: ... TLS connect error: error:00000000:lib(0):func(0):reason(0)
LogHttp: Warning: ... OpenSSL SSL_connect: SSL_ERROR_SYSCALL in connection to 127.0.0.1:8443
LogPandoraLogin: Warning: Login failed: ... err=HTTP request failed
```

**根因**:这不是 login 业务问题,也不是 Envoy 挂了,而是 **UE 自带 OpenSSL 不信任本机 mkcert 自签证书**。
UE 在 Windows 上用引擎自带的 libcurl + OpenSSL(读引擎 `cacert.pem`,**不读 Windows 系统根证书库**),
所以 `mkcert -install` 把 CA 装进系统库对 UE 无效。`SSL_ERROR_SYSCALL / error:00000000` 是
libcurl 证书校验回调失败后主动中止握手的表象(OpenSSL 错误队列为空 → 归类成 SYSCALL)。

**快速判定**(用与 UE 同后端的 OpenSSL,别用 `curl -k` 或 schannel 误判):

```powershell
# 与 UE 同后端：openssl 复现
& "<git>\usr\bin\openssl.exe" s_client -connect 127.0.0.1:8443 -servername localhost
#   → Verify return code: 21 (unable to verify the first certificate) = 证书不被信任（就是本问题）
#   → 握手 has read ... bytes 说明 TLS 协商本身是通的，Envoy/证书没问题

# 用 mkcert rootCA 验证应通过：
& "<git>\usr\bin\openssl.exe" s_client -connect 127.0.0.1:8443 -servername localhost -CAfile (Join-Path (mkcert -CAROOT) 'rootCA.pem')
#   → Verify return code: 0 (ok)
```

**修复(dev 方案 A,由 Codex/人执行,项目侧配置)**:用 UE `[SSL] DebuggingCertificatePath`
叠加 dev CA,不修改引擎:

```powershell
pwsh E:\work\Pandora\tools\scripts\import_dev_ca.ps1
# 重启 UE 编辑器
```

脚本会把公开 dev CA 放到 UE 客户端工程 `Config/Certificates/`,并在 `Config/DefaultEngine.ini`
写入 `[SSL] DebuggingCertificatePath`。不要改 `D:\UnrealEngine` 的 `cacert.pem`。

**说明**:此问题**仅 dev 自签证书存在**;生产用公网 CA(Let's Encrypt)+ 真实域名后,玩家零配置直接连。
完整证书策略见 [`docs/design/gateway-decision.md`](../design/gateway-decision.md) §14。

### 5.4 Redis maint_notifications 历史警告

旧版本 go-redis 启动时可能出现:

```text
maintnotifications disabled due to handshake error
```

这是 go-redis 客户端能力探测警告,不影响登录调试。2026-06-08 起项目已升级
`github.com/redis/go-redis/v9` 到 `v9.20.0`,并通过 `pkg/redisx.NewClient` 统一禁用
维护通知探测;正常启动日志里不应再出现这条噪音。

默认行为可经配置覆盖:在服务 yaml 的 `node.redis_client.maint_notifications` 写
`auto` / `enabled`(接 Redis Cloud / Enterprise 时),留空或写 `disabled` = 关闭探测。

## 6. 这次登录测的是什么连接

当前命令直连:

```text
http://127.0.0.1:21001/v1/login
```

这是本地 HTTP 短请求,不是 HTTPS,也不是长连接。

完整架构中:

- `Login` 是短请求
- 后续业务短请求用 `sessionToken` 放在 header / metadata 鉴权
- 业务推送走 push 服务的 gRPC server stream 长流
- 游戏同步走 UE NetDriver 到 Hub / Battle DS 的长连接
