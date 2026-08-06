# 组队调试手册

> 用途:本机用 VS Code / grpcurl 调试 Pandora team 服务。
> 当前 team 服务没有 RESTful HTTP 路由,`21010` 只提供 `/metrics`;组队 RPC 直连 gRPC `20010`。

## 1. 启动顺序

### 1.1 基础设施

默认配置下最小需要 Redis + Zookeeper + Kafka。team 的邀请通知经
`pandora.team.update` 投递；只要 `kafka.brokers` 非空，Kafka producer 就是启动强依赖：
连不上时 team 会在 gRPC Ready 前退出，由编排器在 Kafka 恢复后重试，不能接受 Invite 后静默丢通知。

```powershell
docker compose -f deploy/docker-compose.dev.yml --env-file deploy/env/dev.env up -d zookeeper kafka redis
```

要观察客户端/stream 收到邀请，再启动 push：

```powershell
go run ./services/runtime/push/cmd/push -conf services/runtime/push/etc/push-dev.yaml
```

如果只做不涉及玩家通知的纯 RPC 本地调试，可以复制一份临时 team 配置并显式设
`kafka.brokers: []`。启动日志会标记 `kafka_producer_disabled_dev_only`；该模式不能用于验证邀请，
也不能用于集群部署。

### 1.2 VS Code 启 team

VS Code 使用 [.vscode/launch.json](../../.vscode/launch.json) 里的 `Debug team`:

```json
{
  "name": "Debug team",
  "type": "go",
  "request": "launch",
  "mode": "auto",
  "program": "${workspaceFolder}/services/matchmaking/team/cmd/team",
  "cwd": "${workspaceFolder}",
  "args": [
    "-conf",
    "${workspaceFolder}/services/matchmaking/team/etc/team-dev.yaml"
  ]
}
```

启动日志应看到:

```text
redis_connected addr=127.0.0.1:6380
kafka_producer_ready topic=pandora.team.update required=true
service_ready grpc=:20010 http=:21010 ...
```

如果 Kafka 没起，会看到下面日志并退出；先恢复 Kafka，再重启 team：

```text
kafka_producer_required_but_unavailable ... team service exits before Ready ...
```

## 2. 身份怎么传

team 的写 RPC 不信 proto body 里的 `player_id`,而是从 ctx 里的 `player_id` 取调用者身份。

本地直连 gRPC 时,用 metadata 模拟 Envoy 鉴权后的注入:

```powershell
-H "x-pandora-player-id: 30907585389428737"
```

常用 demo 账号:

| 账号 | player_id |
|---|---:|
| test1 | 30907585389428737 |
| test2 | 30907585389428738 |
| test3 | 30907585389428739 |
| test4 | 30907585389428740 |
| test5 | 30907585389428741 |

## 3. 基本流程

PowerShell 里 `grpcurl -d '{"teamId":"..."}'` 或 `grpcurl -d $body` 都容易被
native exe 参数传递吃掉双引号。下面统一用 `ConvertTo-Json -Compress` 生成请求体,
再通过管道配合 `grpcurl -d '@'` 从 stdin 读取 JSON。

### 3.1 创建队伍(test1)

```powershell
$body = @{} | ConvertTo-Json -Compress

$body | grpcurl -plaintext `
  -H "x-pandora-player-id: 30907585389428737" `
  -d '@' `
  127.0.0.1:20010 pandora.team.v1.TeamService/CreateTeam
```

返回里记录:

- `teamId`
- `team.teamId`
- `team.captainId`

### 3.2 邀请 test2

把 `<TEAM_ID>` 换成上一步返回的 `teamId`:

```powershell
$teamId = "这里填CreateTeam返回的teamId"
$body = @{
  teamId = $teamId
  targetPlayerId = "30907585389428738"
} | ConvertTo-Json -Compress

$body | grpcurl -plaintext `
  -H "x-pandora-player-id: 30907585389428737" `
  -d '@' `
  127.0.0.1:20010 pandora.team.v1.TeamService/Invite
```

返回里记录:

- `inviteId`
- `expiresAtMs`

### 3.3 test2 接受邀请

```powershell
$teamId = "这里填CreateTeam返回的teamId"
$inviteId = "这里填Invite返回的inviteId"
$body = @{
  teamId = $teamId
  inviteId = $inviteId
} | ConvertTo-Json -Compress

$body | grpcurl -plaintext `
  -H "x-pandora-player-id: 30907585389428738" `
  -d '@' `
  127.0.0.1:20010 pandora.team.v1.TeamService/AcceptInvite
```

成功后 `team.members` 应包含 test1 / test2 两个 player_id。

### 3.4 查询队伍

GetTeam 是只读 RPC,不要求 metadata:

```powershell
$teamId = "这里填CreateTeam返回的teamId"
$body = @{
  teamId = $teamId
} | ConvertTo-Json -Compress

$body | grpcurl -plaintext `
  -d '@' `
  127.0.0.1:20010 pandora.team.v1.TeamService/GetTeam
```

### 3.5 设置准备

test1 准备:

```powershell
$teamId = "这里填CreateTeam返回的teamId"
$body = @{
  teamId = $teamId
  ready = $true
  heroId = 1
} | ConvertTo-Json -Compress

$body | grpcurl -plaintext `
  -H "x-pandora-player-id: 30907585389428737" `
  -d '@' `
  127.0.0.1:20010 pandora.team.v1.TeamService/SetReady
```

test2 准备:

```powershell
$teamId = "这里填CreateTeam返回的teamId"
$body = @{
  teamId = $teamId
  ready = $true
  heroId = 2
} | ConvertTo-Json -Compress

$body | grpcurl -plaintext `
  -H "x-pandora-player-id: 30907585389428738" `
  -d '@' `
  127.0.0.1:20010 pandora.team.v1.TeamService/SetReady
```

当前队伍未满 5 人时,state 通常仍是 `TEAM_STATE_FORMING`;满 5 人且全 ready 后才会自动进入 `TEAM_STATE_READY`。

## 4. PowerShell 变量版

可以用变量减少手抄:

```powershell
$p1 = "30907585389428737"
$p2 = "30907585389428738"

$body = @{} | ConvertTo-Json -Compress
$create = $body | grpcurl -plaintext -H "x-pandora-player-id: $p1" -d '@' `
  127.0.0.1:20010 pandora.team.v1.TeamService/CreateTeam | ConvertFrom-Json

$teamId = $create.teamId

$body = @{
  teamId = $teamId
  targetPlayerId = $p2
} | ConvertTo-Json -Compress

$invite = $body | grpcurl -plaintext -H "x-pandora-player-id: $p1" `
  -d '@' `
  127.0.0.1:20010 pandora.team.v1.TeamService/Invite | ConvertFrom-Json

$inviteId = $invite.inviteId

$body = @{
  teamId = $teamId
  inviteId = $inviteId
} | ConvertTo-Json -Compress

$body | grpcurl -plaintext -H "x-pandora-player-id: $p2" `
  -d '@' `
  127.0.0.1:20010 pandora.team.v1.TeamService/AcceptInvite
```

## 5. 清理 Redis 组队状态

如果重复调试时遇到:

- 玩家已在队伍
- 邀请已存在 / 已过期
- 队伍状态残留

可以只清 team 相关 Redis key:

```powershell
docker exec pandora-redis redis-cli --scan --pattern "pandora:team:*" `
  | ForEach-Object { docker exec pandora-redis redis-cli UNLINK $_ }
```

注意:这会清空本地所有 team 调试状态,不要在联调别人流程时随手执行。

## 6. 建议断点

service 层:

- `services/matchmaking/team/internal/service/team.go`
- `CreateTeam`
- `Invite`
- `AcceptInvite`
- `SetReady`

biz 层:

- `services/matchmaking/team/internal/biz/team.go`
- `CreateTeam`
- `Invite`
- `AcceptInvite`
- `SetReady`
- `publishUpdate`

data 层:

- `services/matchmaking/team/internal/data/team.go`
- Redis `WATCH/MULTI/EXEC` 写队伍状态
- `SetInvite` / `GetInvite`

## 7. 常见问题

### 7.1 返回 ERR_UNAUTHORIZED

写 RPC 没带 metadata:

```powershell
-H "x-pandora-player-id: <player_id>"
```

直连 `20010` 时,业务服务不会自己验 JWT,只读取这个 header / metadata。

### 7.2 端口 21010 不能调 RPC

正常。team 的 HTTP server 只挂 `/metrics`,没有 RESTful RPC。

组队 RPC 用:

```text
127.0.0.1:20010
```

### 7.3 Kafka 没起

默认配置下 team 会在 Ready 前退出，这是邀请通知不静默丢失的启动门禁。先启动 Kafka，确认
broker 可连接，再启动/重启 team；Kubernetes 部署会自动重试未成功启动的新 Pod。

只有显式 `kafka.brokers: []` 的纯 RPC 本地配置允许无 Kafka 启动，并会打印
`kafka_producer_disabled_dev_only`。该模式没有玩家可见的邀请通知。

### 7.4 字段名

grpcurl 使用 proto JSON 名称,写:

```json
{"teamId":"...","targetPlayerId":"...","inviteId":"...","heroId":1}
```

不是:

```json
{"team_id":"...","target_player_id":"...","invite_id":"...","hero_id":1}
```

PowerShell 调试时优先用 `ConvertTo-Json -Compress` 生成 `$body`,再用
`$body | grpcurl ... -d '@'` 从 stdin 读取;不要直接手写 `-d '{"teamId":"..."}'`,
也不要用 `-d $body`,否则很容易变成无引号 JSON。
