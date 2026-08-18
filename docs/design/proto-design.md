# Pandora Proto 设计

> Pandora 协议设计。本文档是 D3 写 .proto 文件的执行依据。
> 协议兼容阶段由 [`CLAUDE.md §3.1`](../../CLAUDE.md) 的“首次生产上线日期”唯一决定：日期未填写时允许
> 有意 breaking，但必须整批刷新所有读写者、数据与生成物；日期填写后，本文兼容条款全部作为硬门禁。

## 1. 设计原则

1. **Pandora 协议独立设计**,不做旧协议兼容
2. **按服务分包**,每个 go 服务一个 .proto 文件
3. **gRPC 双向**:同步走 unary RPC,推送走 server stream 或 Kafka
4. **kafka 消息走 envelope**:`KafkaEnvelope { topic, key, payload(bytes), trace_id, ts }`
5. **字段编号预留**:每个消息预留 50~99 段给后续扩展
6. **向前兼容**:首次生产上线日期填写后不删字段、不改类型,只 `reserved <num>; // <reason>`；日期未填写时按 `CLAUDE.md §3.1` 的刷新闭环执行

## 2. proto 目录结构

```
Pandora/proto/
├── buf.yaml
├── buf.gen.go.yaml          # 生成 go pb 到 Pandora/proto/gen/go/
├── buf.gen.cpp.yaml         # 生成 cpp pb 到 C:/work/Pandora/Source/Pandora/Generated/Proto/
├── common/
│   ├── errcode.proto        # 错误码枚举
│   ├── pagination.proto     # 通用分页
│   ├── timestamp.proto      # 时间封装
│   └── kafka_envelope.proto # Kafka 消息信封
├── login/
│   └── login.proto          # LoginService(账号 + 票据)
├── player/
│   └── player.proto         # PlayerService(玩家档案 + 段位 + 英雄池)
├── team/
│   └── team.proto           # TeamService(组队状态机)
├── match/
│   └── match.proto          # MatchService(匹配)
├── ds/
│   └── allocator.proto      # DSAllocatorService(战斗 DS 调度)
├── hub/
│   └── allocator.proto      # HubAllocatorService(大厅 DS 调度)
├── battle/
│   └── battle.proto         # BattleResultService(结算上报)
├── trade/
│   └── trade.proto          # TradeService(两阶段交易)
├── dialogue/
│   └── dialogue.proto       # DialogueService(NPC 对话树)
├── chat/
│   └── chat.proto           # ChatService(频道)
├── friend/
│   └── friend.proto         # FriendService
├── locator/
│   └── locator.proto        # PlayerLocatorService
├── data_service/
│   └── data_service.proto   # DataService(玩家数据读写网关)
└── ds_runtime/              # ⭐ DS 运行时协议(UE 客户端 ↔ UE DS 之间)
    ├── hub.proto            # 大厅 DS RPC(NPC 触发、商店、跨 hub 切换)
    └── battle.proto         # 战斗 DS RPC(出装、投降、战绩)
```

## 3. 核心消息设计(关键服务)

### 3.1 login.proto

```proto
service LoginService {
  rpc Login(LoginRequest) returns (LoginResponse);
  rpc Logout(LogoutRequest) returns (LogoutResponse);
  rpc IssueDSTicket(IssueDSTicketRequest) returns (IssueDSTicketResponse);
  rpc VerifyDSTicket(VerifyDSTicketRequest) returns (VerifyDSTicketResponse);
}

message LoginRequest {
  string account = 1;
  string password_hash = 2;     // 客户端先 sha256
  string device_id = 3;
  string client_version = 4;
}

message LoginResponse {
  ErrCode code = 1;
  uint64 player_id = 2;
  string session_token = 3;     // 连后端用
  string hub_ds_addr = 4;       // 直连大厅 DS 的 IP:port
  string hub_ticket = 5;        // hub DS 票据(JWT,含 player_id + exp)
}

message DSTicket {              // JWT payload
  uint64 player_id = 1;
  uint64 match_id = 2;          // 战斗 DS 才有,hub 为 0
  int64 issued_at_ms = 3;
  int64 expires_at_ms = 4;
  string ds_type = 5;           // "hub" | "battle"
  string jti = 6;               // JWT ID,防重放
}
```

### 3.2 team.proto

队伍状态变更走 kafka topic `pandora.team.update`(key=player_id)→ push 服务 server stream,**不提供** `StreamTeamUpdates` RPC。
队伍主体存 Redis value `pandora:team:{team_id}` = `proto.Marshal(TeamStorageRecord)`(非 hash);`pandora:team:player:<player_id>` 用 SETNX string 保一人一队;`pandora:team:invite:<invite_id>` 是短 TTL 小令牌,继续用 hash。

```proto
service TeamService {
  rpc CreateTeam(CreateTeamRequest) returns (CreateTeamResponse);
  rpc Invite(InviteRequest) returns (InviteResponse);
  rpc AcceptInvite(AcceptInviteRequest) returns (AcceptInviteResponse);
  rpc LeaveTeam(LeaveTeamRequest) returns (LeaveTeamResponse);
  rpc Kick(KickRequest) returns (KickResponse);
  rpc SetReady(SetReadyRequest) returns (SetReadyResponse);
  rpc GetTeam(GetTeamRequest) returns (GetTeamResponse);
}

enum TeamState {
  TEAM_STATE_UNSPECIFIED = 0;
  TEAM_STATE_FORMING = 1;
  TEAM_STATE_READY = 2;
  TEAM_STATE_MATCHING = 3;
  TEAM_STATE_IN_BATTLE = 4;
  TEAM_STATE_DISBANDED = 5;
}

// 客户端可见结构(RPC response / push payload)
message Team {
  uint64 team_id = 1;
  uint64 captain_id = 2;
  repeated TeamMember members = 3;
  TeamState state = 4;
  int64 created_at_ms = 5;
  int32 max_size = 6;           // MOBA 5 人队
}

message TeamMember {
  uint64 player_id = 1;
  string nickname = 2;
  int32 mmr = 3;
  bool ready = 4;
  uint32 hero_id = 5;
}

// 服务端存储快照(Redis value protobuf bytes;独有 updated_at_ms,不外泄客户端)
message TeamStorageRecord {
  uint64 team_id = 1;
  uint64 captain_id = 2;
  TeamState state = 3;
  repeated TeamMemberStorageRecord members = 4;
  int64 created_at_ms = 5;
  int64 updated_at_ms = 6;
  int32 max_size = 7;
}

message TeamMemberStorageRecord {
  uint64 player_id = 1;
  string nickname = 2;
  int32 mmr = 3;
  bool ready = 4;
  uint32 hero_id = 5;
}
```

### 3.3 match.proto

```proto
service MatchService {
  rpc StartMatch(StartMatchRequest) returns (StartMatchResponse);
  rpc CancelMatch(CancelMatchRequest) returns (CancelMatchResponse);
  rpc StreamMatchProgress(StreamMatchProgressRequest) returns (stream MatchProgress);
  rpc ConfirmMatch(ConfirmMatchRequest) returns (ConfirmMatchResponse);
}

enum MatchStage {
  MATCH_STAGE_QUEUEING = 0;
  MATCH_STAGE_FOUND = 1;
  MATCH_STAGE_CONFIRM = 2;
  MATCH_STAGE_ALLOCATING = 3;
  MATCH_STAGE_READY = 4;
  MATCH_STAGE_FAILED = 5;
}

message MatchProgress {
  uint64 match_id = 1;
  MatchStage stage = 2;
  int32 queue_seconds = 3;
  int32 estimated_wait_seconds = 4;
  string battle_ds_addr = 5;
  string battle_ticket = 6;
  repeated uint64 team_a = 7;
  repeated uint64 team_b = 8;
}
```

### 3.4 ds/allocator.proto

```proto
service DSAllocatorService {
  rpc AllocateBattle(AllocateBattleRequest) returns (AllocateBattleResponse);
  rpc ReleaseBattle(ReleaseBattleRequest) returns (ReleaseBattleResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse); // DS 每 5s 单向 unary 主动调
  rpc ListBattles(ListBattlesRequest) returns (ListBattlesResponse);
}

message AllocateBattleRequest {
  uint64 match_id = 1;
  repeated uint64 player_ids = 2;
  uint32 map_id = 3;
  string game_mode = 4;
}

message AllocateBattleResponse {
  ErrCode code = 1;
  string ds_addr = 2;
  string ds_pod_name = 3;
  int64 allocated_at_ms = 4;
}

message ReleaseBattleRequest {
  uint64 match_id = 1;
  string reason = 2; // "completed" | "abandoned" | "crash"
}

message HeartbeatRequest {
  string ds_pod_name = 1;
  uint64 match_id = 2;
  int32 player_count = 3;
  float cpu_pct = 4;
  float mem_mb = 5;
  string state = 6;             // "warming" | "ready" | "running" | "ended"
  int64 ts_ms = 7;
}

message HeartbeatResponse {
  ErrCode code = 1;
  string command = 2;           // "" | "stop" | "drain" | "reload_config"
}
```

> 职责切分: `ds_allocator` 只分配/回收 DS,不签 battle ticket;ticket 由 matchmaker 签。当前正常结算
> 不依赖 `ReleaseBattle` 生产调用,而是 Battle DS 发 `ended` 心跳后自行 `Agones->Shutdown()`。

### 3.5 battle.proto

```proto
service BattleResultService {
  rpc ReportResult(ReportResultRequest) returns (ReportResultResponse);
  rpc QueryHistory(QueryHistoryRequest) returns (QueryHistoryResponse);
}

message BattleResult {
  uint64 match_id = 1;          // 幂等 key
  int64 started_at_ms = 2;
  int64 ended_at_ms = 3;
  int32 winner_team = 4;        // 0=A win, 1=B win, 2=draw
  repeated PlayerStats stats = 5;
  string ds_pod_name = 6;
}

message PlayerStats {
  uint64 player_id = 1;
  int32 hero_id = 2;
  int32 team = 3;
  int32 kills = 4;
  int32 deaths = 5;
  int32 assists = 6;
  int64 damage_dealt = 7;
  int64 damage_taken = 8;
  int64 healing = 9;
  int64 gold = 10;
  int32 mmr_delta = 11;
}
```

### 3.6 ds_runtime/hub.proto(UE 客户端 ↔ Hub DS)

```proto
service HubRuntimeService {
  rpc TriggerNPC(TriggerNPCRequest) returns (TriggerNPCResponse);
  rpc OpenShop(OpenShopRequest) returns (OpenShopResponse);
  rpc TransferToHub(TransferToHubRequest) returns (TransferToHubResponse);
  rpc EnterBattle(EnterBattleRequest) returns (EnterBattleResponse);
}

message TransferToHubRequest {
  string target_hub_id = 1;
  Vector3 spawn_pos = 2;
}

message Vector3 { float x = 1; float y = 2; float z = 3; }
```

## 4. 错误码段规划(避免冲突)

```
0           = OK
1-999       = 公共错(网络、超时、参数、权限)
1000-1999   = login
2000-2999   = player
3000-3999   = team
4000-4999   = match
5000-5999   = ds_allocator
6000-6999   = battle_result
7000-7999   = trade
8000-8999   = dialogue
9000-9999   = chat / friend / locator
10000-10999 = data_service
11000-11999 = mission(任务)
12000-12999 = auction(全服拍卖行 / 撮合)
```

详细错误码清单写到 `pkg/errcode/errors.go`(D2 实现)。

## 5. Kafka topic 命名规范

```
pandora.<domain>.<event>     格式
pandora.dlq.<original_topic> 死信队列
```

| Topic | 用途 |
|---|---|
| `pandora.login.event` | 登录登出事件 |
| `pandora.match.found` | 匹配成功通知 |
| `pandora.match.failed` | 匹配失败 |
| `pandora.ds.lifecycle` | DS 拉起/回收事件 |
| `pandora.battle.result` | 战斗结算(at-least-once,消费者幂等落库) |
| `pandora.player.update` | 玩家档案变更 |
| `pandora.trade.audit` | 交易审计日志 |
| `pandora.auction.match` | 拍卖成交事件(key=match_id) |
| `pandora.auction.audit` | 拍卖挂单流转审计(key=order_id,append-only) |
| `pandora.chat.world` | 世界聊天 |
| `pandora.locator.update` | 玩家位置变更 |
| `pandora.dlq.<topic>` | 死信队列 |

详细 partition / retention 设计见 `infra.md` §4。

## 6. proto 工具链选型(D3 决策)

**候选**:
- A. **buf**(推荐):标准化 lint + breaking change 检测 + 多语言代码生成
- B. **protoc + Makefile**:轻量,但要手维护版本
- C. **bazel + rules_proto**:大型项目最佳,但学习曲线陡

**建议 A(buf)**,理由:
- buf 内置 breaking change 检测；首次生产上线日期填写后以其作为“不复用编号”的硬门禁，日期未填写且本次有意 breaking 时用它列出变化，并按 `CLAUDE.md §3.1` 完成全量生成、数据刷新和模块验证
- 双仓库场景下,buf 生成器可以一次配置 go + cpp 两种产物
- 社区主流,UE 项目对接成熟

## 7. 字段命名约定

- 时间戳:`<name>_at_ms`(明确单位为毫秒,int64,Unix epoch)
- 运行时 / 业务 ID:`<entity>_id`(`player_id` / `team_id` / `match_id` / `order_id` / `message_id` / `dialogue_id` / `hub_id` / `invite_id` 等 Snowflake ID 一律用 `uint64`;未知 / 空值用 `0`,需要 presence 时用 `optional uint64`)
- 配置表 ID:`<entity>_id` 或 `<entity>_config_id`(`npc_id` / `hero_id` / `skill_id` / `item_config_id` / `map_id` 等默认用 `uint32`;容易和运行时实体混淆时优先用 `<entity>_config_id`)
- UUID:只有外部系统已定义为 UUID 时才用 `string`
- 枚举:`<TYPE>_<NAME>`(`TEAM_STATE_FORMING`),并以 `<TYPE>_UNSPECIFIED = 0` 兜底;状态 / 类型 / 原因等 enum value 和 Go mirror 常量保持生成 enum 类型或 `int32` 语义,不因取值非负改成 `uint32`
- message 命名(四类各司其职,**不手写与 proto 重复的并行 struct**):
  - RPC 请求/响应 → `<Verb><Domain>Request` / `<Verb><Domain>Response`
  - 客户端可见结构 → `<Domain>` / `<Domain><Part>`(短名,如 `Team` / `TeamMember`)
  - 服务端存储快照 → `<Domain>StorageRecord` + 子结构 `<Domain><Part>StorageRecord`(如 `TeamStorageRecord` / `TeamMemberStorageRecord`);存储结构一律以 `StorageRecord` 结尾
  - 服务间事件 → `<Domain><Action>Event`(如 `TeamUpdateEvent`);可内嵌客户端可见结构,但它本身是服务内部消息,不是存储快照
  - 不用"客户端 proto / 服务器 proto"这种叫法;统一说"客户端可见结构 / 存储快照结构"
  - **proto bytes 只用于快照/blob**(Redis value、Kafka payload、MySQL blob 列);关系型 MySQL 表的结构化列直接映射 proto 字段,临时小令牌(如 invite)可继续用 redis hash,都不强制序列化成 bytes
  - proto message 直接当存储 record 时禁止值拷贝(`a := *rec` 会复制内部 state/mu/sizeCache),克隆一律用 `proto.Clone`
  - **禁止把存储快照直接返回 / 推送给客户端**;response / push 只使用客户端可见结构,由服务端从存储 record / DB / Redis 中按当前客户端需求的最小字段集填充,必要时计算派生字段
- 布尔:`is_<adj>` / `has_<noun>`(避免 `<name>_flag`)
- 集合:复数(`player_ids` / `members`)

## 8. D3 执行清单

1. 写 `buf.yaml` / `buf.gen.go.yaml` / `buf.gen.cpp.yaml`
2. 写 14 个 service 的 .proto 骨架(只声明 RPC 和核心消息,不写所有字段)
3. 跑 `buf lint` 通过
4. 跑 `buf generate` 产出 go pb 到 `proto/gen/go/`
5. 写 `tools/scripts/proto_gen.ps1` 包装 buf 命令
6. 在 `PROGRESS.md` 追加 D3 完成记录
