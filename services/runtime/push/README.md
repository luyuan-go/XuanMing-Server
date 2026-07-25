# push

> 集中式长连接推送服务:客户端登录后调 `Subscribe`(gRPC server stream)维持长连,服务端消费各业务域
> kafka 事件,按 `player_id` 路由成 `PushFrame` 下发。**不是** WebSocket 服务、**不是** HTTP 网关——
> 客户端走 gRPC-Web over HTTP/2 TLS 连 Envoy,Envoy 转标准 gRPC 到本服务。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`
> 的 [`go-services.md §5`](../../../docs/design/go-services.md)(push 服务详细契约)、
> [`gateway-decision.md §6`](../../../docs/design/gateway-decision.md)(为何 server stream 而非自研 WebSocket);
> 协议时序见 [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:持有全部在线客户端 stream,消费各业务域 push topic,按 `player_id` 路由成 `PushFrame` 转发;
  在线实时投递 + 断线/跨 Pod 补推,**每玩家有序、不漏**。
- **投递权威**:定向帧的定序与投递权威全在 **Redis 投递缓冲**(`data/offline.go`);进程内 `ConnectionManager`
  只是 `player_id → stream` 的连接索引 + 唤醒面,**没有**「内存索引直发」路径(2026-07-22 审计 v2 起,
  所有帧先入缓冲、后唤醒拉取)。
- **架构边界**(`gateway-decision.md §6`):
  - **不是 WebSocket 服务**(2026-06-03 自研 WebSocket 已被否决);
  - **不是 HTTP 网关**(那是 Envoy 的职责,HTTP :51014 仅 `/metrics`);
  - 客户端走 gRPC-Web over HTTP/2 TLS 连 Envoy,Envoy 转标准 gRPC 给本服务;
  - 业务服推送事件**全部走 kafka**,push 消费转 stream,**不接**业务服直接 gRPC 调用。
- **不做的事**:不产生业务事件(只转发)、不算派生数值、不持久化业务态(缓冲只是有界投递窗口,§9.24 外的
  滚动窗口,非权威存储)。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:50014` | `Subscribe` server stream(客户端 → Envoy gRPC-Web → 本服) + 将来 unary RPC 预留 |
| HTTP | `:51014` | 仅 `/metrics`(`push.proto` 无 `google.api.http` 注解,buf 不生成 HTTP handler,无 RESTful RPC) |

取值来自 `conf.Defaults()`(`internal/conf/conf.go`:`Server.Grpc.Addr` / `Server.Http.Addr`)。

## 对外接口

代码入口:`internal/service/push.go`。唯一 RPC 是 server stream,`player_id` 由 Envoy `jwt_authn` 注入
`x-pandora-player-id` 头,service 层用 `pmw.PlayerIDFromContext` 从 Kratos transport 直接读(**stream 不跑
Kratos unary 中间件链**,不能靠 `ctx.Value`,否则恒为 0)。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `Subscribe(session_token, last_seen_ms)` → `stream PushFrame` | 客户端(经 Envoy gRPC-Web) | 已受理 + 长连:补推 `> last_seen_ms` 的缓冲帧,随后阻塞等新帧,直到 client 关闭或会话失效 | Envoy `jwt_authn`(注入 player_id)+ 服务层**会话现行性门**(jti 必须为 login 当前一代,`AuthorizeAndRegister`) |

> **无独立 unary RPC**:业务事件全走 kafka,push 只有这一个 stream 入口。`SubscribeRequest.session_token`
> 是冗余字段(Envoy 已验签),现行性由服务端向 login 会话权威 `pandora:sess` 再查一次(见「会话现行性门」)。

## 目录结构(Kratos 标准分层,对齐 login / matchmaker)

```
cmd/push/main.go               启动入口(redis + 每 topic 一个 KafkaConsumer + wake 订阅 + cell 路由装配)
etc/push-dev.yaml              开发配置(reflection 开、require_session_gate 关、内网直连联调)
etc/push-prod.yaml.example     生产配置模板(reflection 关、require_session_gate=true、真值占位)
internal/
  conf/conf.go                 配置结构(config.Base + PushConf) + Defaults()
  service/
    push.go                    RPC 入口(实现 pushv1.PushServiceServer;Subscribe 建流门 + 反注册)
  biz/
    push.go                    PushUsecase:AuthorizeAndRegister / RunSubscribeStream(唯一写者)/ drainBuffer / 会话 fence
    connection.go              ConnectionManager:player_id→StreamSlot 索引(顶号语义)+ 唤醒 + 广播箱
    consumer.go                KafkaConsumer:每 topic 一个,kafka 消息 → 缓冲 + 唤醒;毒丸投 DLQ
    consumer_sharding.go       蜂窝 cell 归属判定(ownsPlayer;单 Cell nil-safe no-op)
    metrics.go                 pandora_push_* prometheus 指标(offline_append_failed_total)
  data/
    offline.go                 RedisOfflineCacheRepo:投递缓冲(ZSET + 单 key Lua 游标分配/修剪/gap)
    session_gate.go            SessionGate 只读端口(现行性由共享 pkg/sessiongate 实现)
    wake.go                    RedisWakeSignal:跨 Pod pub/sub 唤醒(best-effort 加速器)
  server/
    grpc.go                    gRPC server 注册(unary 链挂 SessionCurrent;Subscribe 门在 service 层)
    http.go                    HTTP server 注册(仅 /metrics)
```

## 核心调用链

两条独立数据流:**(A) 入站 kafka 事件 → 投递缓冲 → 唤醒**;**(B) 客户端 Subscribe → 补推 + 拉取投递**。
连接写者(`RunSubscribeStream` 所在 goroutine)是每条 stream 的**唯一 `stream.Send` 调用者**——kafka
handler 只写缓冲 + 发唤醒信号,绝不直接碰 stream(单写者不变量,防 HTTP/2 帧撕裂)。

### A. 生产侧:kafka 事件 → 投递缓冲

```
业务服 producer
  └─ kafkax.PushToPlayers(ctx, callerPID, toPIDs, payload)   排除发起方(原则 2);key = player_id
       ↓ kafka pandora.<域>.<事件>(header: trace_id / event_type)
KafkaConsumer.handle                          (consumer.go:165,每 topic 一个 goroutine)
  ├─ 广播类 topic? → cm.Broadcast(frame)       (connection.go:110)本 Pod 全在线玩家,满箱即丢
  ├─ ParseUint(key) → player_id                非数字 / =0 → kafkax.Poison 投 DLQ 留证
  ├─ ownsPlayer(player_id)                      (consumer_sharding.go:44)非本 cell → Poison 投 DLQ(单 Cell no-op)
  ├─ parseEventTypeHeader(headers)             (consumer.go:298)header 非法 → Poison(不降级 legacy 0)
  ├─ offline.AssignAndBuffer(player_id, frame) (offline.go:209)单 key Lua 原子:分配游标 + ZADD 帧 + 双修剪 + 记 fl
  │      失败 → 返 errcode 9301 → kafkax 按 RetryPolicy 重试,耗尽投 DLQ(绝不「首败即 ack 丢帧」)
  ├─ cm.SendTo(player_id)                       (connection.go:95)本地唤醒(仅信号,帧体已在缓冲)
  └─ wake.PublishWake(player_id)                (wake.go:44)**无条件**跨 Pod pub/sub 唤醒(best-effort)
```

### B. 消费侧:Subscribe → 补推 + 拉取投递

```
客户端 Subscribe(session_token, last_seen_ms)   经 Envoy jwt_authn 注入 x-pandora-player-id
PushService.Subscribe                           (service/push.go:58)
  ├─ pmw.KillSwitchStreamCheck                  stream 不跑 unary 链,手动查关停开关
  ├─ AuthorizeAndRegister(playerID, sess, ...)  (biz/push.go:125)同玩家条带锁内原子完成:
  │     ├─ AuthorizeSubscribe                    (biz/push.go:146)jti == 当前一代会话?
  │     │       顶号 → ErrSessionSuperseded(ABORTED)  权威不可达 → ErrUnavailable(fail-closed)
  │     └─ Conns().Register                      (connection.go:64)顶号:旧 slot.cancel() 踢线
  └─ RunSubscribeStream(slot, playerID, ...)     (biz/push.go:389)本 goroutine = 该 stream 唯一写者
        ├─ 看门狗 goroutine                       每 30s recheckSession(顶号/登出/到期 → cancelStream 关流)
        ├─ 首轮 drainBuffer(cursor=last_seen_ms) 重连补推
        └─ for { <-notify(本地唤醒) / <-poll.C(30s 兜底) / <-bcast(广播箱) → drainBuffer }
             drainBuffer                          (biz/push.go:282)
               ├─ offline.Range(> cursor)         (offline.go:237)拉游标之后的**完整前缀**(单点定序保证不跳)
               ├─ 每页发送前 LostSince 预检        (offline.go:342)缺口 → 先发 resync 信号帧,再发幸存帧
               ├─ 逐帧 sessionFenceDelivery        (biz/push.go:228)轮换后产生的帧零交付(跨 Pod 顶号 fencing)
               ├─ slot.stream.Send(frame)         单写者
               └─ 拉空后 LostSince 终检            fail-closed:查不了 ≠ 无丢失,游标不越过缺口
```

## 投递缓冲与游标契约(核心特有节)

`data/offline.go` 的 Redis ZSET 是**唯一定序与投递权威**;老版 README 里「在线 `SendTo(frame)` 直发内存、
离线才 `Append`」的双路径已在 2026-07-22 审计 v2 合并为**单一拉取式**:所有定向帧先入缓冲,再唤醒连接
写者按游标拉取。

- **key**:`pandora:push:offline:<player_id>`(ZSET;key 名沿用 dev 环境既有名)。
- **score = 投递游标**,不是事件时间:`AssignAndBuffer` 用**单 key 单 Lua** 原子完成
  「读当前最大游标 → `cursor = max(基线+1, 服务端 now)` → ZADD 帧 → 更新哨兵 → 双修剪 → 续期」。
  游标基线用**服务端 now** 而非 kafka ts(审计 R4 P1-1:重投/积压帧的旧 ts 会落在保留窗下界外,被同一
  Lua 的修剪立即删掉后 ack = 静默丢帧)。单 Lua 保证游标分配与入缓冲不可分割,多 Pod / 多 topic consumer
  并发写同一玩家时缓冲恒等于「已分配游标全集」,`Range(>X)` 永远返回 X 之后的完整前缀——**跨 Pod 顺序由
  Redis 单点定序,不依赖进程内锁**。
- **哨兵 member**:`wm`(游标基线,修剪/重启不丢基线)、`fl`(修剪线,gap 检测权威);整 key TTL 7 天
  (`cursorKeyTTL`,≥ 客户端游标寿命),帧 member 按 `offline_cache_ttl` 窗口 + `offline_cache_max_frames`
  条数**双修剪**(§9.18 有界纪律),哨兵不受条数修剪影响。
- **`PushFrame.ts_ms` 语义变更**(见 `push.proto`):定向帧 = 服务端投递游标(每玩家严格递增且唯一);
  **广播帧恒为 0**(不参与游标)。客户端**按玩家隔离**保存最大 `ts_ms` 作断点续传游标,切账号/角色必须
  切游标存储。`ts_ms` 不能作业务去重键。
- **交付语义 = at-least-once,不承诺不重**:kafka 重投 / redis 结果不确定时重试会给同一业务事件分配新游标
  → 客户端可能重复收到,业务事件必须幂等或按业务 ID 判重(chat 有 `message_id`,状态类推送以最新为准天然
  幂等)。游标保证的是**不漏 + 每玩家全序**,不是 exactly-once。

## 会话现行性门与顶号 fencing(P0,INC-20260722-004)

JWT 验签只证明「曾经登录过」,旧 / 被顶号 token 在 `exp` 前仍能过 Envoy `jwt_authn`。push 必须再过现行性门
(§9.23 会话 fencing):向 login 会话权威 `pandora:sess:<player_id>`(`data/session_gate.go` 只读,实现复用
共享 `pkg/sessiongate`)核对请求 jti == 当前一代。

- **建流门 + 注册同锁原子**(`AuthorizeAndRegister`,`biz/push.go:125`,R4 复审①):校验与注册分离存在
  TOCTOU——旧会话校验通过后暂停、新会话注册、旧会话再注册会反过来顶掉新设备。同玩家 64 条带锁串行化,
  两种交错都收敛到「新会话持有连接槽」。
- **流内看门狗**(`recheckSession`,`biz/push.go:190`,R4 复审②):独立 goroutine 每 30s 复查,不受写者
  `stream.Send` 阻塞影响;失效后 `cancelStream`,写者下一次 Send 前必然观察到取消。**先判代际后判到期**
  (R5 复审 P0-2):「已过期且已被顶」必须回 `ErrSessionSuperseded`(→ ABORTED)而非 UNAUTHENTICATED,否则
  被顶设备自动重登反顶新设备(互踢循环)。
- **逐帧投递 fence**(`sessionFenceDelivery`,`biz/push.go:228`,R5 P0-4 → R6 逐帧):进程内条带锁只覆盖单
  Pod,跨 Pod 顶号靠 Redis session key 单点串行——每帧 Send 前复核,轮换后产生的帧**零交付**;检查与 Send
  跨存储不可原子,在途暴露 ≤1 帧(诚实契约,不宣称零瞬时窗口)。
- **权威不可达一律 fail-closed**:建流拒绝(客户端退避重连);流内连续 `sessionFailClose`(3)次查询失败后
  关流。短抖动不误杀,持续故障不裸奔。
- **顶号语义**:`ConnectionManager.Register`(`connection.go:64`)发现同 `player_id` 已有 slot 时调
  `old.cancel()` 踢旧流;`Unregister` 仅当当前 slot == 传入 slot 才删,避免顶号后新流删掉自己。

## gap 检测与 resync 信号

修剪 = 丢失,丢失必须留痕(§9.16)。缓冲修剪(窗口/条数)与坏 member 都把被删帧最高游标记入 `fl` 哨兵;
补推时若客户端游标之后已有帧被修剪/滑出保留窗,`drainBuffer` 检出 `LostSince > baseline` 并下发一条合成帧
`topic = pandora.push.resync`(`ResyncTopic`,`biz/push.go:72`;payload 空、`ts_ms=0`、不推进客户端游标)。

- **两层检测**(R7 复审 P1-1):**每页发送前预检**(主防线,resync 信号必须先于任何越过缺口的幸存帧到达)
  + **拉空后终检**(fail-closed 兜底,`offline.go:342` `LostSince`)。同一段丢失只信号一次(基线跳到丢失上界)。
- **首连契约**(R9 复审 P1-e):`last_seen_ms=0` 且缓冲拉空无帧时**不做**终检——新客户端无增量历史,交付契约
  从订阅时刻开始。其正确性依赖客户端时序契约:登录成功提交点**先订阅 push、后拉业务快照**(UE 侧
  `MyAccountModel` 是唯一订阅点),订阅前被修剪的事件必被订阅后首次全量快照覆盖。
- **客户端契约**:收到 `pandora.push.resync` 信号帧 → 对推送驱动的各业务域回源全量拉取权威态(team / match /
  friend / DS recovery 已接;新增推送消费域必须同步接入)。resync 不是 kafka topic,只存在于 Subscribe 下行;
  无需 ACK(信号帧丢失时重连补推会重新检出同一缺口再次发信号)。

## 广播 topic 与跨 Pod 唤醒

- **广播类 topic**(`kafkax.IsBroadcastTopic`,如 `pandora.chat.world` / `pandora.system.notify`):无 per-player
  归属,走 `cm.Broadcast`(`connection.go:110`)投给**本 Pod** 全部在线玩家(满箱即丢,广播丢失容忍)。
  **每 Pod 独立 consumer group**(`groupID + "-bcast-" + hostname`)且 `initialOffset = OffsetNewest` +
  `DisableOffsetCommit`(`consumer.go:112`)——共享 group 只有一个 Pod 消费到广播;不留位点保证 Pod 重启不把
  停机窗口积压的广播整段重放刷屏。广播帧 `ts_ms=0`,不参与玩家游标。
- **跨 Pod 唤醒**(`data/wake.go`,R5 P2-10 + 复审 P1-5):投递缓冲写者与连接可能不在同一 Pod(滚动/多副本)。
  消费侧写完缓冲后**无条件** `PublishWake`(不以「本地有 slot」抑制——本地 slot 可能是被顶号的陈旧残留);各
  Pod 订阅 `pandora:push:wake` channel,收到后对本地连接做一次 `SendTo`(本地无此玩家 = 廉价 no-op)。信号是
  **best-effort 加速器**:丢失由 30s 兜底轮询(`pollFallbackInterval`)收敛,交付正确性只依赖缓冲。

## Cell 归属定向路由(蜂窝扩容)

`consumer_sharding.go` 是 nil-safe 接线:单 Cell / dev(`router == nil`)时本实例拥有全部玩家,`ownsPlayer`
返回 `known=false`,`handle` 不做归属判定,行为与历史一致(当前唯一部署形态)。多 Cell 时 main 经
`SetCellOwnership` 注入 `cellroute.Router` + 本实例 region/cell;非本 cell 玩家的消息**毒丸投 DLQ**
(`consumer.go:226`)——本 cell Redis 对连接所在 cell 不可见,「照常交付」实为写错缓存 + ACK = 静默丢。
诚实标注:当前**没有自动转投**,业务生产者按 `player_id` 路由到正确 cell 是**部署面契约**,本判定只是错配的
兜底暴露(见 `scale-cellular-20m.md §4.2`)。

## 域内多事件类型路由(`event_type`,2026-07-17)

一个 `topic`(= 业务域)下可承载多种**结构不同**的事件 message,用 `PushFrame.event_type` 做**域内细路由**,
避免「一事件一 topic」导致 topic 爆炸。客户端按 `(topic, event_type)` 定位该反序列化哪个 message。

- **每域独立编号**:各域自定义 `XxxPushEventType` enum(如 `pandora.team.v1.TeamPushEventType`);`0` 永远 =
  该 topic 现有旧事件,枚举值只增不复用。设计详见 [`go-services.md §5`](../../../docs/design/go-services.md)
  「域内多事件类型路由」。
- **传递路径**:与 `trace_id` 一致走 kafka header(`kafkax.HeaderEventType`);consumer `parseEventTypeHeader`
  (`consumer.go:298`)把 header 透传进 `PushFrame.EventType`。
- **push 只做形态校验、不解释语义**(R5 注释纠偏):header 缺失/空 = legacy `0`(显式兼容);header 存在但非法
  (非十进制 / 越界)= producer bug,**毒丸进 DLQ,不降级为 0**(防新事件被按旧 message 错误路由);合法值原样
  透传,payload 仍由客户端按 `(topic, event_type)` 解释。
- **当前接入域**:`TeamPushEventType.INVITE=1`(team 兼容期双发专属 `TeamInviteEvent(event_type=1)` 与 legacy
  `TeamUpdateEvent(event_type=0)`);match / friend 目前仍只有 `UNSPECIFIED=0`。
- **发布顺序**:必须先升级 push reader,再升级 team dual producer,最后发布只认专属邀请事件的新客户端;
  回滚同样保持 **push → team**,避免新客户端命中旧 writer 后漏邀请。

## 协议铁律(对齐 [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md))

- **原则 2**:发起方不收自己触发的 push——业务服 produce kafka 时**必须用 `pkg/kafkax.PushToPlayers` helper**,
  helper 自动排除 `caller_player_id`。
- **原则 3**:已受理型 RPC(`match.StartMatch` / `ConfirmMatch`)是例外,传 `callerPlayerID=0` 让 helper 跳过
  排除,push 给所有人含发起方(key=`player_id` 保证同玩家有序)。

## 关键设计点 / 不变量

| 主题 | 约束 | 代码锚点 |
|---|---|---|
| 单写者 | 每条 stream 只有 `RunSubscribeStream` goroutine 调 `stream.Send`;consumer 只写缓冲 + 发唤醒 | `RunSubscribeStream` / `consumer.handle` |
| 缓冲为投递权威 | 所有定向帧先入 Redis 缓冲、后唤醒拉取;无「内存索引直发」路径 | `AssignAndBuffer` / `drainBuffer` |
| 游标单点定序 | 单 key Lua 分配严格递增游标,跨 Pod / 跨 topic 全序,`Range(>X)` 返回完整前缀 | `assignAndBufferScript` |
| at-least-once | 重投分配新游标 → 可能重复,业务按 ID 判重;保证不漏 + 每玩家有序 | `AssignAndBuffer` / `Range` |
| 会话现行性门 | 建流校验 jti + 注册同锁;流内看门狗复查;逐帧投递 fence;权威不可达 fail-closed | `AuthorizeAndRegister` / `recheckSession` / `sessionFenceDelivery` |
| 顶号语义 | `Register` 顶掉旧 slot;`Unregister` 仅删自身 slot | `connection.go` |
| gap 留痕 | 修剪/坏帧记 `fl` 哨兵;补推检出缺口发 resync,查不了 fail-closed 不越缺口 | `drainBuffer` / `LostSince` |
| 毒丸不静默 | 非法 key / player_id=0 / 非法 event_type / 非本 cell → Poison 投 DLQ 留证 | `consumer.handle` |
| 不首败即 ack | `AssignAndBuffer` 失败拒 ack,kafkax 重试→DLQ;metric 告警 | `consumer.handle` / `metrics.go` |
| Redis 驱逐门 | 启动核验全拓扑 `maxmemory-policy=noeviction`,查不了缺省 fail-closed 拒启动 | `verifyEvictionPolicy` |

## 配置项(`internal/conf/conf.go`)

| 键 | 默认 | 说明 |
|---|---|---|
| `push.topics` | `kafkax.PushTopics` 全量集 | 订阅的 kafka topic 列表,每 topic 一个 consumer,共享 `kafka.group_id`;空则走权威默认集(prod 模板建议不列,避免漏项) |
| `push.offline_cache_ttl` | `5m` | 投递缓冲帧 member 保留窗口(修剪下界 = `now - ttl`;整 key 另有 7 天基线保活) |
| `push.offline_cache_max_frames` | `512` | 单玩家缓冲条数硬上限(§9.18);写侧保留最新 N + 读侧同值兜底 LIMIT |
| `push.require_session_gate` | `false`(dev)/ `true`(prod 机械置) | 会话现行性门强制档:true = 建流必须带当前一代 jti,权威不可达 fail-closed;false = 有 jti 仍校验、无 jti 放行(dev 内网直连联调) |
| `push.allow_unverified_eviction_policy` | `false` | 托管 Redis 禁用 `CONFIG` 时,是否放行「无法核验 maxmemory-policy」的启动;缺省 fail-closed 拒启动,仅人工确认全拓扑 noeviction 后显式置 true |
| `server.grpc.addr` | `:50014` | gRPC server stream 端口 |
| `server.http.addr` | `:51014` | HTTP `/metrics` 端口 |
| `kafka.group_id` | `pandora-push` | 定向 topic 共享消费组(广播 topic 另派生 `-bcast-<hostname>` 独立组) |
| `kafka.brokers` / `partition_cnt` | — | broker 列表(dev `9093`)/ 分区数(需与业务 producer 一致,保证同 player_id 同 partition) |

DLQ 重试常量在 `cmd/push/main.go`:`dlqMaxRetries=3` / `dlqRetryBackoff=500ms`(对齐 battle_result;offline
写入瞬时抖动进程内重试,耗尽投 `pandora.dlq.<topic>` 可回放)。`max_conn_age`(dev `15m`)达龄 GOAWAY 重拨,
滚动更新时流量能滚到新副本(`zero-downtime-update.md §6.2`)。

## 本地启动

```powershell
# 1. 基础设施(redis + kafka;maxmemory-policy=noeviction,否则启动被驱逐门拒)
pwsh tools/scripts/dev_up.ps1

# 2. 启 push(dev 配置:reflection 开、会话门宽松)
go run ./services/runtime/push/cmd/push -conf services/runtime/push/etc/push-dev.yaml
```

联调验证(需 `grpcurl` + kafka console producer + `redis-cli`):

```powershell
# 1) 客户端 subscribe(dev 直连,无 token;require_session_gate=false 放行匿名/无 jti)
grpcurl -plaintext -d '{\"session_token\":\"\",\"last_seen_ms\":0}' `
  127.0.0.1:50014 pandora.push.v1.PushService/Subscribe

# 2) 另起 terminal,往 kafka 写一条 key=42 的定向消息(进 kafka 容器)
docker exec -it pandora-kafka kafka-console-producer.sh `
  --bootstrap-server 127.0.0.1:9093 `
  --topic pandora.team.update `
  --property "parse.key=true" --property "key.separator=:"
# 输入:42:dummy-payload
# 客户端 1) 那边应立即收到一帧 PushFrame{topic=pandora.team.update}

# 3) 断开 grpcurl 后再 produce,redis 投递缓冲应见帧(score = 投递游标,非 kafka ts)
redis-cli -p 6380 ZRANGE pandora:push:offline:42 0 -1 WITHSCORES

# 4) Prometheus 抓 metrics(pandora_push_offline_append_failed_total 应恒为 0)
curl http://127.0.0.1:51014/metrics | Select-String pandora
```

> 生产用 `etc/push-prod.yaml.example` 为模板(`cp` 后填真值):删 `enable_reflection`、redis 加密码、kafka 真实
> brokers、`require_session_gate: true`。真实 `push-prod.yaml` 被 `services/.gitignore` 忽略,只提交 `.example`。

## 关联文档

- [`go-services.md §5`](../../../docs/design/go-services.md) — push 服务详细契约(server stream / PushFrame / 域内多事件类型路由)
- [`gateway-decision.md §6`](../../../docs/design/gateway-decision.md) — 为何 server stream 而非自研 WebSocket;Envoy gRPC-Web 转发
- [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md) — 推送协议原则 2(排除发起方)/ 原则 3(已受理型例外)
- [`infra.md`](../../../docs/design/infra.md) — 端口(50014 / 51014)/ redis key / kafka topic / metrics 命名登记
- [`session-generation-rollout.md`](../../../docs/design/session-generation-rollout.md) — 会话代际门 / 顶号 fencing(INC-20260722-004)
- [`scale-cellular-20m.md`](../../../docs/design/scale-cellular-20m.md) §4.2 — 蜂窝 cell 归属与跨 cell 弱实时事件桥
- [`zero-downtime-update.md`](../../../docs/design/zero-downtime-update.md) §6.2 — max_conn_age GOAWAY 与滚动更新
