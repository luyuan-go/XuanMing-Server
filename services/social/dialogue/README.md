# dialogue

> NPC 对话树运行时:按 `npc_id` 取服务端权威对话树,`StartDialogue` 建会话 → `ChooseOption`
> 按 `option_id` 推进节点 → `EndDialogue` 关闭;客户端只渲染 `DialogueState`、只回传选择。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见
> `docs/design`:服务要约见 [`go-services.md §2.10`](../../../docs/design/go-services.md);
> 会话 owner cell 锚定见 [`scale-cellular-20m.md §4.2`](../../../docs/design/scale-cellular-20m.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:运行 NPC 对话树。三个 unary RPC:开启对话 / 选择选项推进 / 结束对话。
- **对话树权威**:对话树是**服务端权威配置**,当前最小版本内联在 `dialogue-dev.yaml`
  (`ConfigTreeProvider` 内存只读,进程启动构建后不再变更)。客户端只拿渲染用的
  `DialogueState`,选择只回传 `option_id`,不能伪造节点 / 文本 / 可见性。
- **会话权属**:`dialogue_id` 由服务端 snowflake 生成;会话状态(当前节点)由服务端持有,
  **当前为单实例进程内存**(`MemorySessionStore`),**不跨实例、进程重启即丢**。
  会话归属用 JWT `player_id` 校验,非本人会话一律按「不存在」处理(R5,不泄露他人会话)。
- **不做的事**:选项当前**无副作用**(领奖励 / 改任务进度等留后续接 trade / player 服务);
  可见性是**静态配置**判定(基于玩家等级 / 任务进度的前置条件判定留后续);
  无 MySQL / 无 Redis / 无 Kafka(不消费也不生产事件)。

## 端口(`docs/design/infra.md`)

端口取自 `internal/conf/conf.go` 的 `Defaults()`。

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20013` | 客户端 RPC(经 Envoy)|
| HTTP | `:21013` | 仅 `/metrics`(`dialogue.proto` 无 `google.api.http` 注解,无 RESTful RPC)|

## 对外接口

代码入口:`internal/service/dialogue.go`(gRPC service 层,实现 `dialoguev1.DialogueServiceServer`)。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `StartDialogue(npc_id)` | 客户端 | 按 `npc_id` 建会话,返回起始节点 `DialogueState`(`dialogue_id` 服务端生成)| JWT `player_id` |
| `ChooseOption(dialogue_id, option_id)` | 客户端 | 校验选项合法 + 可见后推进到下一节点,返回新 `DialogueState` | JWT `player_id` |
| `EndDialogue(dialogue_id)` | 客户端 | 结束并回收会话(幂等)| JWT `player_id` |

**鉴权模型(R5,`service/dialogue.go:9` 顶部注释)**:三个 RPC **全部只认 ctx 中 JWT 注入的
`player_id`**,**忽略请求体里的任何 `player_id` 字段**——请求 proto 里根本不接收 `player_id`,
身份完全由服务端从 ctx 取(`callerID`,`service/dialogue.go:94`)。链路:Envoy `jwt_authn` 在路由层
require JWT → `pmw.AuthOptional()`(`server/grpc.go:21`)从 Envoy 注入的 `x-pandora-player-id`
header 读出 `player_id` 注入 ctx → service 层再做一次 `callerID==0 → ERR_UNAUTHORIZED` 兜底拦截。

> **本服务无内部专用 RPC**:三个 RPC 全是面向客户端、经 Envoy、由玩家 JWT 鉴权;不存在
> `callerID==0` 的内部直连路径,也没有内部 HMAC 信任域(区别于 matchmaker 的 `ReleaseMatch` /
> `ResolvePlayerMatchContext`)。

## 目录结构(Kratos 标准分层,对齐 login / matchmaker)

```
cmd/dialogue/main.go            启动入口(装配 snowflake + 对话树 + 内存会话 + 会话过期清理 goroutine)
etc/dialogue-dev.yaml           开发期配置(内联对话树 + session_ttl)
etc/dialogue-prod.yaml.example  prod 配置样例
internal/
  conf/conf.go                  配置结构(嵌入 config.Base + DialogueConf{SessionTTL, Trees};Defaults 填端口/TTL)
  service/
    dialogue.go                 RPC 入口(实现 DialogueServiceServer;JWT player_id 下沉,errcode→proto 映射)
  biz/
    dialogue.go                 DialogueUsecase 核心(StartDialogue / ChooseOption / EndDialogue + 节点渲染)
    dialogue_sharding.go        会话 owner cell 锚定纯逻辑(SessionShardKey=player_id;router 注入后打落点观测)
  data/
    tree.go                     DialogueTree/Node/Option 领域类型 + ConfigTreeProvider(按 npc_id 只读查树)
    session.go                  Session + SessionStore 抽象 + MemorySessionStore(进程内存,惰性+主动过期)
  server/
    grpc.go                     gRPC server 注册(AuthOptional 中间件)
    http.go                     HTTP server 注册(仅 /metrics)
```

## 核心调用链

三个 RPC 都是**同步 request/response**:service 层取 JWT `player_id` + 校验非空参数 → 调 biz
Usecase → biz 读对话树(`TreeProvider`)+ 读写会话(`SessionStore`)→ 渲染成客户端可见的
`DialogueState` 返回。无 push、无异步 saga、无后台推进循环(唯一后台 goroutine 是会话过期清理)。

```
客户端 ──gRPC──► service/dialogue.go            biz/dialogue.go               data/
                 (JWT player_id 下沉 + 参数校验)   (对话树运行时)               (树只读 + 会话读写)

StartDialogue(npc_id)
  StartDialogue:42 ──► uc.StartDialogue:61 ──┬─► trees.GetTree(npcID)         tree.go GetTree:57
   sf.Generate()=dialogue_id                 ├─► sessions.Create(session)     session.go Create:47
                                             ├─► logSessionPlacement          sharding.go:63(router!=nil 才打)
                                             └─► buildState(起始节点)          biz/dialogue.go buildState:190
                                                  起始即终止节点→Delete 回收

ChooseOption(dialogue_id, option_id)
  ChooseOption:59 ──► uc.ChooseOption:112 ──┬─► sessions.Get                  session.go Get:59(惰性过期)
                                            │    └─ 非本人/不存在→ErrDialogueNotFound(R5)
                                            ├─► trees.GetTree(s.NpcID)
                                            ├─► findVisibleOption             biz/dialogue.go findVisibleOption:225
                                            │    └─ 选项缺失/不可见→ErrDialogueOptionInvalid
                                            ├─► NextNode 空或不存在 → Delete + endedState:215(对话结束)
                                            └─► sessions.Update(推进节点)     session.go Update:75
                                                 → buildState(下一节点);跳到终止节点则 Delete 回收

EndDialogue(dialogue_id)
  EndDialogue:76 ──► uc.EndDialogue:168 ────► sessions.Get → 仅本人会话 sessions.Delete(幂等)
                                                                            session.go Delete:84

后台(main.go):runSessionSweep:190 每 1min ──► MemorySessionStore.SweepExpired  session.go SweepExpired:93
                                                (主动清被遗弃的过期会话,防堆积)
```

**errcode 映射**:biz 用 `pkg/errcode`(`ErrDialogueNotFound=8001` / `ErrDialogueOptionInvalid=8002` /
`ErrInvalidArg` / `ErrUnauthorized`),service 层 `toProtoCode`(`service/dialogue.go:100`)按数值 1:1
映射成 `commonv1.ErrCode` 塞进 response 的 `code` 字段;RPC 本身返回 `err=nil`(业务失败走 code,不走 gRPC error)。

## 会话生命周期与不变量

会话是一个小状态机:`Create → (节点间推进) → Delete`。删除的触发点有三处,三者语义一致(会话消失后
后续 RPC 一律按「不存在」处理):

```
StartDialogue ──► [会话存活: 当前 NodeID] ──ChooseOption(有后续节点)──► [推进到下一 NodeID]
                        │                                                      │
                        │  到达终止节点(无可见选项, ended=true) / 选项无 next_node
                        ├──────────────────────────────────────────────────────┤
                        ▼                                                        ▼
                   Delete 回收 ◄── EndDialogue(幂等) ◄── TTL 过期(惰性 Get / 主动 Sweep)
```

守卫与不变量(均在 `biz/dialogue.go`,以真实代码为准):

| 主题 | 约束 | 代码锚点 |
|---|---|---|
| 会话归属 | `ChooseOption` 取到的会话 `PlayerID != playerID` → 按「不存在」返回 `ErrDialogueNotFound`,不泄露他人会话 | `ChooseOption:128` |
| 选项服务端判定 | 只接受**存在且 `Visible==true`** 的选项;不可见选项即使客户端回传 `option_id` 也拒绝 | `findVisibleOption:225` |
| 终止节点 | `DialogueState.Ended = 无可见选项`;起始节点即终止节点则 `StartDialogue` 建后立即 `Delete` 回收 | `buildState:190` / `StartDialogue:105` |
| 客户端可见结构 | 只输出可见选项到 `DialogueState`(§9.14);存储侧 `Session` 不外露,渲染字段来自权威对话树 | `buildState:190` |
| 会话过期 | 绝对过期时间戳 `ExpiresMs`;`Get` 命中过期即删并视为不存在(惰性),另有 1min ticker 主动 `SweepExpired` | `session.go Get:59` / `SweepExpired:93` |
| 结束幂等 | `EndDialogue` 对不存在 / 非本人会话均返回成功(幂等结束语义) | `EndDialogue:180` |
| 会话 owner cell | 会话是玩家 owner 数据,分片键口径统一为 `player_id`(**非** `dialogue_id`);单 Cell(`router==nil`)不做落点观测 | `sharding.go SessionShardKey:34` |

> **分片现状**:`SetCellRouter`(`biz/dialogue.go:56`)可注入确定性 `cellroute.Router`,注入后
> `StartDialogue` 会额外打一条会话落点观测日志(`dialogue_session_placement`,`sharding.go:63`),
> 供分片上线核对「会话落点 == 玩家 owner cell」。`router==nil`(单 Cell / dev)时该路径不执行,
> 行为与历史一致。会话存储真正按 owner cell 分片属基础设施,由 Codex / 人接(见 `dialogue_sharding.go`
> 顶部注释)。

## 配置项(`internal/conf/conf.go`)

| 键 | 默认 | 说明 |
|---|---|---|
| `server.grpc.addr` | `:20013` | gRPC 监听(`Defaults()` 填)|
| `server.http.addr` | `:21013` | HTTP 监听,仅 `/metrics`(`Defaults()` 填)|
| `dialogue.session_ttl` | `5m` | 单次对话会话空闲存活时间;超时被惰性 / 主动清理 |
| `dialogue.trees[]` | 无 | 内联对话树:`npc_id` / `speaker` / `start_node` / `nodes[]`;`node` 含 `node_id` / `text` / `options[]`;`option` 含 `option_id` / `text` / `visible`(省略=可见)/ `next_node`(空或指向不存在节点=结束对话)。启动期 `buildTrees`(`main.go:139`)做去重 + 起始节点存在性校验,不合法直接 fatal |
| `node.node_id` | `1` | snowflake 发号器 node 段(`dialogue_id` 生成);多副本须各自唯一 |
| `snowflake.node_id_source` | `static` | `""`/`static` 静态(默认,单副本 / dev);`etcd` 走 etcd 自动抢占(多副本,失租自动退出)|
| `cell_route.mode` | 空 | 空=单 Cell 不路由;`static`/`etcd`=多 Cell,经 `etcdtable.WireRouter` 注入 `SetCellRouter` |

> `config.Base` 的其余通用字段(`server.grpc.timeout` / `enable_reflection` / `max_conn_age` /
> `killswitch` / `session_gate` 等)按需在 yaml 覆盖,dev 示例见 `etc/dialogue-dev.yaml`;
> 本服务**不接** Redis / MySQL / Kafka / ConfigTable。

## 本地启动

```powershell
# dialogue 无外部依赖(内联对话树 + 内存会话),直接起即可
go run ./services/social/dialogue/cmd/dialogue -conf services/social/dialogue/etc/dialogue-dev.yaml
```

`dialogue-dev.yaml` 开了 `enable_reflection: true`,可用 grpcurl 直连 `:20013` 联调:

```powershell
# 开启对话(npc_id=1001「商店老板」;需带 JWT,player_id 从 ctx 取)
grpcurl -plaintext -d '{\"npc_id\":1001}' 127.0.0.1:20013 pandora.dialogue.v1.DialogueService/StartDialogue

# Prometheus 抓 metrics
curl http://127.0.0.1:21013/metrics | Select-String pandora
```

> 直连无 Envoy 时不会注入 `x-pandora-player-id`,`callerID==0` → 返回 `ERR_UNAUTHORIZED`;
> 端到端联调需经 Envoy 带 JWT,或按各服务约定注入 header。

## 关联文档

- [`go-services.md §2.10`](../../../docs/design/go-services.md) — dialogue 服务要约(RPC / 对话树存储 / 会话状态机 / MOBA 早期范围)
- [`scale-cellular-20m.md §4.2`](../../../docs/design/scale-cellular-20m.md) — owner cell 不变量与会话分片键口径(`SessionShardKey=player_id`)
- [`infra.md`](../../../docs/design/infra.md) — 服务端口规划(dialogue = 20013 / 21013)与 topic 规范
