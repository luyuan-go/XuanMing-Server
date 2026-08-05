# Pandora 协议顺序规则

> **状态**:已决策(2026-06-03 立;2026-08-05 补入原则 5)
> **问题来源**:用户在 D3 末尾发现 RPC response 与 kafka push 乱序问题;2026-08-05 用户追问"状态刷新该不该客户端主动拉"
> **作用**:固化 5 个协议设计原则,所有业务 proto + 客户端代码必须遵守
> **两类失效模式别混**:原则 1~4 治**乱序**(response 与 push 谁先到),原则 5 治**丢失**(push 整段不来)。做完原则 1~4 不代表推送可靠。

## 1. 乱序问题(必须先理解)

### 1.1 时序示例

```
Client → gateway(WebSocket)→ matchmaker.ConfirmMatch
                                  │
                                  │ ① 处理(几 ms)
                                  │ ② 发 kafka pandora.match.progress { stage=ALLOCATING }
                                  │ ③ 返回 gRPC response { ok }
                                  │
                          ┌───────┴────────┐
                          │                │
                          ▼                ▼
            gateway 进程内部:        kafka broker
              业务 goroutine             │
              收到 gRPC response         │ ④ 持久化 + 推到消费者(几 ms ~ 几十 ms)
              立刻 ws.Send(response)     │
                          │              ▼
                          │        gateway 进程内部:
                          │          kafka 消费 goroutine
                          │          收到消息
                          │          ws.Send(push)
                          ▼              │
                   client 收到 RESPONSE  ▼
                   (~50ms)         client 收到 PUSH
                                   (~100ms)
```

**结果**:Client 先收到 Response,后收到 Push。

### 1.2 为什么会这样

- gRPC response 走的是 **业务 goroutine → ws** 路径,几 ms 完成
- kafka push 走的是 **业务 goroutine → kafka broker → 消费 goroutine → ws** 路径,几十 ms 完成
- 这两条路径在 gateway 进程里是**两个独立 goroutine**,Go runtime 调度不保证顺序
- TCP 单连接也救不了:**gateway 把 Response 写进 ws 流的瞬间,Push 还在 kafka broker 里没出来**

### 1.3 单 WebSocket(B1)能不能解决?

**不能**。这是**应用层语义问题**,不是 TCP 顺序问题:
- TCP 保证字节流顺序 ✅
- 但 gateway **写 ws 的顺序** = "Response 先写 + Push 后写"
- 单连接只能保证"写进去什么顺序客户端就按什么顺序收"
- 不能保证"两个独立异步通道的写入操作顺序"

**B0 三连接 vs B1 单连接**:
- B0 多个 push 之间的顺序:WebSocket TCP 保证 ✅
- B1 多个 push 之间的顺序:WebSocket TCP 保证 ✅
- B0 response 与 push 之间的顺序:无保证 ❌
- B1 response 与 push 之间的顺序:无保证 ❌

**所以单 WebSocket 在乱序问题上不比三连接更优**(B1 的优势是资源、状态清晰,不是顺序)。

## 2. 真正的 bug 案例

### 2.1 案例 A:快速连点 + UI 状态机错乱

```
Client 玩家 A 快速点击 3 次:
  1. CreateTeam(队伍 1)
  2. LeaveTeam(队伍 1)
  3. CreateTeam(队伍 2)

如果 team 服务**违反原则 2**(发 push 给发起方自己):

理想顺序事件流:
  Response 1: ok, team_id=T1
  Push 1:     team.update, T1 created
  Response 2: ok
  Push 2:     team.update, T1 disbanded
  Response 3: ok, team_id=T2
  Push 3:     team.update, T2 created

实际事件流(因为 gRPC 比 kafka 快):
  Response 1: ok, team_id=T1                  ← 立刻
  Response 2: ok                              ← 立刻
  Response 3: ok, team_id=T2                  ← 立刻
  Push 1:     team.update, T1 created         ← 几十 ms 后
  Push 2:     team.update, T1 disbanded       ← 同上
  Push 3:     team.update, T2 created         ← 同上

UI 状态机:
- 收到 Response 1:UI 显示 T1
- 收到 Response 2:UI 清空
- 收到 Response 3:UI 显示 T2
- 收到 Push 1:UI 又显示 T1(回退!闪烁)
- 收到 Push 2:UI 又清空(回退!闪烁)
- 收到 Push 3:UI 又显示 T2
```

**结果**:界面闪烁,过渡态可能引发其它逻辑误触发。

### 2.2 案例 B:ConfirmMatch UI 状态错乱

```
玩家点"确认参战"按钮 → 客户端发 ConfirmMatch 到 gateway
↓
matchmaker:
  - 写 redis 记录玩家已确认
  - 检查是否所有人都已确认(假设是)
  - 发 kafka pandora.match.progress { stage=ALLOCATING, key=玩家 A }
  - 返回 RPC response { ok }

如果客户端**违反原则 3**(根据 RPC response 切 UI 状态):

错误的客户端代码:
  OnConfirmMatchResponse(resp) {
    UI.ShowText("匹配成功!");  ← 错!
  }
  OnPushMatchProgress(push) {
    UI.UpdateStage(push.stage);
  }

实际表现:
  - Response 来:UI 显示"匹配成功!"(50ms)
  - Push 来:  UI 显示"正在拉起战斗服..."(100ms)← 比"成功"还后!

玩家看到:
  "确认中..." → "匹配成功!" → "正在拉起战斗服..." → "准备进入战斗..."
                ↑ 这一步是 bug,本应跳过
```

### 2.3 案例 C:第三方玩家收到错误顺序事件

```
玩家 A:Invite(B) → response ok
玩家 A:CancelInvite(B) → response ok
↓ 但 push 到 B 的延迟可能不同

B 看到:
  Push: A 邀请你
  Push: A 撤销了邀请

但如果两个 push 走不同 kafka partition(不同 key 设计错误):
  Push: A 撤销了邀请   ← B 一脸懵:"撤销什么?"
  Push: A 邀请你       ← B 看到邀请

⚠️ 这里同 key=B,kafka 保证 partition 内有序,所以这种情况**不会**发生
   除非 partition key 设计错(如用 team_id),才会乱。
```

**结论**:多个 push 之间在 kafka 同 partition 内有序,只要 key 设计正确(`key=收件人 player_id`),不会乱。

## 3. 4 个协议设计原则

### 原则 1:**Response 必须同步返回完整业务结果**

⭐ 适用范围:**立即完成**型 RPC(如 CreateTeam / GetProfile / GetMMR)

这类 RPC 的语义是"立刻办完",response 必须含完整结果,**客户端不需要等 push 才能渲染 UI**。

```proto
// ✅ 正确
service TeamService {
  rpc CreateTeam(CreateTeamRequest) returns (CreateTeamResponse);
}
message CreateTeamResponse {
  ErrCode code = 1;
  Team    team = 2;   // ⭐ 完整 Team,客户端拿到 response 就能渲染
}

// ❌ 错误(违反原则 1)
message CreateTeamResponse {
  ErrCode code    = 1;
  uint64  team_id = 2;   // 只返 ID,客户端要等 push 才能拿完整数据
}
```

#### 3.1 "设计 smell" 详解(为什么发起方不该收自己的 push)

"smell" 是工程术语,意思是**代码看起来不优雅,虽然能跑但暴露设计有问题**。

具体到 Pandora:**发起方既看 RPC response 又收自己的 push,意味着同一份信息走了两条路给同一个人**。

**反例**(违反原则 2):

```
玩家 A 点 CreateTeam:

✅ 干净设计
A 收 RPC response: { team: Team{id=T1, members=[A]} }
A 没收 push(因为他是发起方,他自己已经知道结果了)

❌ 设计 smell
A 收 RPC response: { team: Team{id=T1, members=[A]} }
A 同时收 push: team.update { Team{id=T1, members=[A]} }  ← 一模一样的信息

A 的客户端代码要写:
  收到 response → UI 显示 T1
  收到 push → 检查"是不是我自己刚发的请求引起的?是的话忽略,不是的话再处理"
                                            ↑ 这句话就是 smell
```

**5 条 smell 表现**:

1. **冗余**:同一份数据走两条路(response + push),浪费协议设计
2. **去重逻辑复杂**:客户端要判"这 push 是不是我自己引起的"
3. **状态机难推理**:同一事件触发两次回调,顺序还不保证
4. **流量浪费**:多发一次 kafka 消息 + 多走一次 stream 帧
5. **测试难**:要专门测"重复事件不引起 UI 错乱",回归用例多

**性能数据 vs 卫生数据**:

即使 Pandora 切到 Kratos + gRPC server stream(2026-06-04 决策),延迟差从"kafka 几十 ms"降到"stream 几 ms",**视觉上看不出闪烁了**,但这些 smell 仍存在 — 代码维护痛、测试用例膨胀、新人接手难。

**所以原则 2 是架构卫生(architectural hygiene),不是性能优化**。

**正确做法对照表**:

| RPC | response 给 caller | push 给谁 | 是否 push 给 caller |
|---|---|---|---|
| CreateTeam(单人队伍)| 完整 Team | (无第三方)| ❌ 不发 |
| Invite(A→B)| ok + invite_id | B(被邀请方)| ❌ 不发给 A |
| LeaveTeam(A 离开)| ok | 剩余队员 C/D/E | ❌ 不发给 A |
| Kick(A 踢 B)| ok | B + 剩余队员 | ❌ 不发给 A |
| StartMatch(已受理型) | match_id | 队伍所有人 | ⚠️ **例外**:发(原则 3) |
| ConfirmMatch(已受理型)| ok | 队伍所有人 | ⚠️ 同上例外 |
| SendMessage(私聊)| message_id | 接收方 B | ❌ 不发给 A |

**例外的合法性**:已受理型 RPC 的 stage 变化必须靠 push,发起方也必须收(否则他不知道 stage 已变),这个例外在原则 3 显式标注。

### 原则 2:**kafka push 不发给请求发起方,只发给"第三方玩家"**

⭐ 这是**最重要**的原则,违反它必出 bug。

| RPC | 谁会收到 push | 谁不会收到 push |
|---|---|---|
| Invite(A 邀请 B) | B(被邀请方)| A(发起方,看 response)|
| LeaveTeam(A 离开)| 其它队员 C/D/E | A(发起方,看 response)|
| ConfirmMatch(A 确认)| 其它 9 个匹配玩家 | A(发起方,看 response;但 stage 异步变化是例外,见原则 3)|
| SendMessage(A 发聊天)| 接收方 B / 频道订阅者 | A(发起方,看 response)|
| AddFriend(A 加 B)| B(接收方)| A(发起方,看 response)|

**实现要点**:
- 业务服务发 kafka 时,**循环排除 caller_player_id**
- ctx 里必须能拿到 caller_player_id(gateway 鉴权时注入)
- code review 时强制盯一下 "发 kafka 的循环里有没有排除 caller"

```go
// ✅ 正确
func (s *TeamService) Invite(ctx, req *InviteRequest) (*InviteResponse, error) {
    caller := ctx.Value("player_id").(uint64)  // = req.captain_id 通常
    
    // ... 写 redis 记录邀请 ...
    
    // 只 push 给被邀请的 B,不 push caller
    kafka.Send("pandora.team.update", key=req.target_player_id, payload=...)
    
    return &InviteResponse{Ok: true}, nil
}

// ❌ 错误
func (s *TeamService) Invite(ctx, req *InviteRequest) (*InviteResponse, error) {
    // 错!给所有相关玩家都发,包括 caller
    for _, p := range []uint64{req.captain_id, req.target_player_id} {
        kafka.Send("pandora.team.update", key=p, payload=...)
    }
    return &InviteResponse{Ok: true}, nil
}
```

### 原则 3:**异步状态机变化必须走 push,RPC response 只表示"已受理"**

⭐ 适用范围:**已受理**型 RPC(如 StartMatch / ConfirmMatch / CreateOrder)

某些业务**本质就是异步**:
- 匹配:玩家发 StartMatch 后,撮合是几秒~几分钟的过程,中间状态(QUEUEING / FOUND / CONFIRM / ALLOCATING / READY)**只能走 push**
- 战斗结算:DS 战斗完后才发 result,客户端不能"调一个 RPC 等结果"

这种 RPC 的语义是:
- **Response 只表示"已收到请求,会处理"**(不能驱动 UI 状态)
- **真正的状态变化全靠 push**(包括发起方自己也收 push)

```proto
// ✅ 正确(StartMatch 是已受理型)
rpc StartMatch(StartMatchRequest) returns (StartMatchResponse);
message StartMatchResponse {
  ErrCode code     = 1;
  uint64  match_id = 2;   // 已入队,后续 stage 走 push
}
```

⚠️ **原则 3 跟原则 2 冲突的特例**:匹配进度 push **必须**给发起方自己也发,因为他没有别的方式知道 stage 变化。

**这是协议设计上的"已知例外"**,要在 proto 注释里显式标注。

### 原则 4:**Proto 注释必须显式标注 RPC 语义**

每个 RPC 必须在 proto 注释里写明是"立即完成"还是"已受理":

```proto
// CreateTeam: 立即完成(synchronous),response 含完整 Team
// kafka push 不发给发起方(他看 RPC response 即可)
rpc CreateTeam(CreateTeamRequest) returns (CreateTeamResponse);

// StartMatch: 已受理(accepted),后续 stage 变化走 push
// ⚠️ 例外:matchmaker 的 push 给所有队员包括发起方自己(原则 3 例外)
rpc StartMatch(StartMatchRequest) returns (StartMatchResponse);
```

让客户端开发者一看注释就知道:
- "立即完成" → 客户端 OnResponse 里直接切 UI 状态
- "已受理" → 客户端 OnResponse 里只显示 loading,等 push 切状态

### 原则 5:**推送不承担正确性;每个客户端状态必须有权威查询接口**

> 补入 2026-08-05。原则 1~4 解决的是**顺序**(response 与 push 谁先到),原则 5 解决的是**丢失**(push 整段不来)。两者是不同的失效模式,不能互相替代。

⭐ 适用范围:**所有走 push 下发的状态**(匹配进度、队伍、好友、公会、经验、hub 迁移……)

分工必须是这样,不能二选一:

- **push = 变更提示 + 低延迟**,它**不是**真相源;
- **pull(权威查询 RPC)= 真相源 + 兜底**,任何有生命周期的状态必须有一个**幂等的「查当前权威态」接口**(`GetMatchProgress` / `GetTeam` / `ListFriends` / `GetGuild` / `GetProfile` …);
- 客户端不允许存在「只能靠 push 才能知道结果」的状态。

**评审新 push 通道先问一句:「接收方错过这条推送,还有没有别的途径得知结果?」** 答案是"没有" → 缺 pull 接口,先补;补不了就必须上"启动强依赖 + 服务端可重放补推 + 客户端有界轮询"三层(见 §12.1 的真实事故)。

#### 5.1 两种合法的 apply 模型(二选一,不准混用)

| | **A:push 只当信号**(默认选它) | **B:push 带态 + 单调序** |
|---|---|---|
| push 到达时 | **不写状态**,只触发一次 pull | 直接 apply,但帧必须带单调序(stage / revision / 版本号) |
| 谁写状态 | 只有 pull 的返回 | pull 与 push **走同一个 apply 函数**,只接受更新的版本 |
| 代价 | 多一个 RTT | 服务端必须保证该状态的版本单调 |
| 适用 | 低频、状态小、正确性敏感(匹配、队伍、公会) | 高频、payload 即全部、天然以最新为准(经验条、聊天) |

⚠️ **模型 B 里"同一个 apply 函数"是硬要求,不是建议**。两条通道各写一份状态、再用 revision / 世代守卫去弥合顺序,是 `CLAUDE.md §9.22`(唯一权威,不重复影子状态)与 `§11.7`(客户端单一事实通道)明令禁止的形态——2026-07-28 宝箱读条就是踩了这个坑后整块删掉守卫改单通道的。

#### 5.2 push 是 at-least-once,判重不能只看 ts_ms

`PushFrame.ts_ms` 是**服务端每玩家严格递增的投递游标**,不是事件时间。同一业务事件被 kafka 重投时会拿到**新的更大游标**再投一次,所以"ts_ms 比上次大就当新事件"挡不住重复(§5.3 的旧写法只挡乱序,不挡重投)。契约以 `proto/pandora/push/v1/push.proto` 为准:

- **游标保证不漏 + 每玩家有序,不保证不重**;
- 判重按**业务 ID**(chat 用 `message_id`;状态类推送以最新为准天然幂等);
- **不得把 `ts_ms` 当事件时间显示**;
- 游标必须按 player_id 隔离存储,切账号 / 切角色要换游标,不能用进程级单值。

## 4. Pandora 现有 RPC 的语义分类

### 4.1 立即完成型(原则 1)

| RPC | response 内容 | push? |
|---|---|---|
| login.Login | session_token + hub_ds_addr + hub_ticket | 不发(发起方看 response)|
| login.IssueDSTicket | ticket | 同上 |
| login.VerifyDSTicket | claims | 同上 |
| player.GetProfile | PlayerProfile | 不发 |
| player.GetMMR | mmr | 不发 |
| player.UpdateNickname | ok | 可能给好友 push |
| player.UnlockHero | ok | 不发 |
| team.CreateTeam | Team | 不发(单人队伍无第三方)|
| team.GetTeam | Team | 不发 |
| friend.ListFriends | []FriendInfo | 不发 |
| trade.ListMyOrders | []Order | 不发 |
| dialogue.StartDialogue | DialogueState | 不发 |
| chat.PullHistory | []ChatMessage | 不发 |
| ds.AllocateBattle | ds_addr + ds_pod_name | 不发(matchmaker 内部调;battle_ticket 由 matchmaker 签后随 match.progress 推送) |
| hub.AssignHub | hub_ds_addr + ticket | 不发 |
| Heartbeat(各)| command | 不发(DS 内部用)|

### 4.2 已受理型(原则 3)

| RPC | response 内容 | push 给谁 |
|---|---|---|
| match.StartMatch | match_id | 队伍所有人(含发起方,例外)|
| match.CancelMatch | ok | 队伍所有人(含发起方,例外)|
| match.ConfirmMatch | ok | 队伍所有人(含发起方,例外)|
| trade.CreateOrder | order_id | 双方(发起方 + 对方,因状态机要 push 推进)|
| trade.ConfirmOrder | ok | 同上 |

### 4.3 涉及第三方的立即完成型(原则 2 严格执行)

| RPC | response 给发起方 | push 给第三方(不含发起方)|
|---|---|---|
| team.Invite | ok | B(被邀请方)|
| team.AcceptInvite | Team(完整队伍)| 其它队员 |
| team.LeaveTeam | ok | 剩余队员 |
| team.Kick | ok | 被踢者 + 剩余队员 |
| team.SetReady | ok | 其它队员 |
| friend.AddFriend | request_id | B(被加方)|
| friend.AcceptFriend | ok | 申请方 |
| chat.SendMessage | message_id | 接收方 / 频道订阅者 |
| dialogue.ChooseOption | DialogueState | 不发(单玩家会话)|

## 5. 客户端代码强制约定

### 5.1 OnResponse 处理规则

**立即完成型 RPC**:
```cpp
OnCreateTeamResponse(resp) {
    if (resp.code == OK) {
        UI.ShowTeam(resp.team);   // ✅ 直接渲染
    } else {
        UI.ShowError(resp.code);
    }
}
```

**已受理型 RPC**:
```cpp
OnStartMatchResponse(resp) {
    if (resp.code == OK) {
        UI.ShowText("匹配中...");   // ✅ 只显示 loading,不切状态
        currentMatchId = resp.match_id;
    } else {
        UI.ShowError(resp.code);
    }
    // ⚠️ 不要根据 response 切"匹配成功"UI
}

OnPushMatchProgress(push) {
    UI.UpdateStage(push.stage);   // ✅ stage 由 push 驱动
    if (push.stage == READY) {
        ConnectToBattleDS(push.battle_ds_addr, push.battle_ticket);
    }
}
```

### 5.2 UI 状态机原则

- **立即完成型**:Response 直接驱动 UI 状态切换
- **已受理型**:Response 只表示"已发出",UI 进入"等待"态;状态切换全靠 push
- **永远不要**:同时根据 Response 和 Push 切 UI(必出乱序 bug)

### 5.3 客户端去重(应对 at-least-once)

⚠️ **2026-08-05 修正**:下面这段只挡「旧帧 / 乱序帧」,**挡不住重投**——同一业务事件被重投会拿到更大的新游标(见 §3 原则 5.2)。真正的判重必须按**业务 ID**;`ts_ms` 只是投递游标,用途是断线重连时回传 `last_seen_ms` 做断点续传,且必须按 player_id 隔离存储。

kafka 是 at-least-once 推送,push 可能重复。客户端按 envelope 时间戳挡旧帧:

```cpp
OnPushReceived(envelope) {
    // 去重检查
    if (envelope.ts_ms <= lastSeenTs[envelope.topic]) {
        // 比上次看到的还旧,丢弃
        return;
    }
    lastSeenTs[envelope.topic] = envelope.ts_ms;
    
    Dispatch(envelope);
}
```

### 5.4 客户端什么时候主动拉(原则 5 的落地)

**拉的时机不是"定时轮询",是「入口点 + 有界 watchdog」。** 五个必接触发点:

| # | 触发点 | 为什么必须拉 |
|---|---|---|
| 1 | 进入 / 返回该界面 | 界面不在时的推送可能已被忽略或从未消费 |
| 2 | push 流(重)连成功后 | 断点续传只覆盖缓冲窗口(默认 5min / 512 帧),窗口外的帧已被修剪 |
| 3 | 切回前台 | 后台期可能断流、可能超窗 |
| 4 | 收到 `pandora.push.resync` | 服务端**已确证**你的增量有缺口,这是最精确的回源信号 |
| 5 | watchdog 到期 | 处于"必须等 push 才能推进"的等待态且超时无进展 |

三条硬约束:

1. **常驻轮询只允许存在于有界等待态**。匹配中可以按固定间隔轮 `GetMatchProgress`,但状态一旦落地(拿到 `battle_ds_addr` / 无活跃 match)**必须停表**。常驻短周期轮询代替 push 违反 `CLAUDE.md §16.10`(禁止用定时器掩盖时序问题),而且会把服务端打成筛子。
2. **watchdog 到期后只能"重查权威并重试",不准"假设已成功往下走"**。判别口诀见 `§16.10`:到期后假设成功 = 掩盖时序;到期后重查权威 = 合法兜底。
3. **拉取必须发生在 subscribe 之后**。push 的首连契约(`push.proto`)明确要求:登录成功后**先订阅 push、再发起任何业务域快照拉取**;先拉快照再订阅会在两者之间开一个永久丢失窗口,且服务端的"首连不做缺口终检"跳过将不再安全。UE 侧唯一订阅点是 `MyAccountModel` 登录完成回调。

## 6. 服务端代码强制约定

### 6.1 发 kafka 时排除 caller

每个发 push 的业务服务,**强制使用以下模板**:

```go
// pkg/push/helper.go(W2 时实现)
func PushToPlayers(ctx context.Context, topic string, recipients []uint64, payload proto.Message) error {
    callerID := GetCallerPlayerID(ctx)  // 从 ctx 拿,没有就是 0
    
    for _, recipientID := range recipients {
        if recipientID == callerID {
            continue   // ⭐ 强制排除 caller
        }
        kafka.Send(topic, recipientID, payload)
    }
    return nil
}
```

### 6.2 异步业务必须显式声明"我要给 caller 也 push"

如果是 match / trade 这种"已受理"型 RPC,需要给发起方也 push,**必须用单独函数**:

```go
// pkg/push/helper.go
func PushToAllIncludingCaller(ctx context.Context, topic string, recipients []uint64, payload proto.Message) error {
    // ⚠️ 这个函数仅用于已受理型 RPC 的 stage 推进
    // 使用前必须确认 RPC 是"已受理"语义
    for _, recipientID := range recipients {
        kafka.Send(topic, recipientID, payload)
    }
    return nil
}
```

代码 review 强制要求:**调 PushToAllIncludingCaller 必须在注释里说明对应的 RPC 是已受理型**。

## 7. 反模式禁令

- ❌ **不要**让发起方既看 RPC response 又收自己触发的 push(原则 2)
- ❌ **不要**根据"已受理"型 RPC 的 response 切 UI 状态机(原则 3)
- ❌ **不要**省略 RPC 的语义注释(原则 4)
- ❌ **不要**让 RPC response 只返 ID 不返完整数据(立即完成型应该返完整数据)
- ❌ **不要**用 stream RPC 解决推送问题(go-zero 不支持,改 kafka push)
- ❌ **不要**指望 TCP 单连接能解决 RPC response 和 kafka push 的乱序(它们是两个 goroutine 写 ws,Go 调度不保证顺序)
- ❌ **不要**写"等 response 和 push 都到了再处理 UI"的复杂同步逻辑(直接选对一种语义即可)
- ❌ **不要**设计"只能靠 push 才能得知结果"的状态 — 必须同时有权威查询 RPC(原则 5)
- ❌ **不要**让 push 和 pull 各写一份状态再用 revision / 世代守卫弥合 — 选模型 A 或 B,只能有一个写入路径(原则 5.1)
- ❌ **不要**用常驻短周期轮询代替 push — 轮询只能是**有界等待态**里的兜底,状态落地即停表(§5.4)
- ❌ **不要**只按 `ts_ms` 判重就声称幂等 — 重投会拿到更大的新游标,判重必须按业务 ID(原则 5.2)
- ❌ **不要**先拉业务快照再订阅 push — 顺序反了会开一个永久丢失窗口(§5.4 第 3 条)

## 8. 工程检查清单

### 8.1 写新 RPC 前

- [ ] 确定 RPC 语义:立即完成 / 已受理(原则 4)
- [ ] proto 注释里写明语义
- [ ] response message 是否符合原则 1(立即完成型必须返完整数据)
- [ ] 是否需要 push?发给谁?是否含 caller(原则 2)?

### 8.2 服务端代码 review

- [ ] 发 kafka 的循环里**显式排除 caller_player_id**(或调用 `PushToPlayers` 帮助函数)
- [ ] 已受理型 RPC 调用 `PushToAllIncludingCaller` 时,代码注释写明"已受理型 RPC,对应 §4.2"

### 8.3 客户端代码 review

- [ ] 立即完成型 RPC:OnResponse 直接更新 UI
- [ ] 已受理型 RPC:OnResponse 只显示 loading,UI 状态机由 push 驱动
- [ ] OnPushReceived 有时间戳去重(挡旧帧)**且**有业务 ID 判重(挡重投,原则 5.2)
- [ ] 该状态有权威查询 RPC,且 §5.4 五个触发点(界面进入 / push 重连 / 切前台 / resync / watchdog)都接了刷新
- [ ] push 与 pull 只有一个写入路径:push 只触发 pull(模型 A),或两者共用同一 apply 函数(模型 B)
- [ ] 消费 push 的 Model **显式处理 `pandora.push.resync`**(不能落到"其余 topic 原样忽略"分支)
- [ ] 有界等待态里的轮询在状态落地后确实停表(无常驻定时器泄漏)

### 8.4 测试

- [ ] 单元测试:覆盖"快速连点同一 RPC"场景,验证 UI 状态收敛
- [ ] 集成测试:验证 push 到达延迟(p99 < 200ms)
- [ ] 故障注入:kafka 延迟升到 1s 时验证 UI 不乱

## 9. 与 gateway 设计的关系

`gateway-decision.md` 描述了客户端连接架构(B1 单 WebSocket / 还是 B0 三连接),**那是基础设施层**;
本文档描述协议语义层 — **乱序问题靠协议规则解决,不靠基础设施**。

无论选 B0 还是 B1,**4 个原则都必须遵守**。

## 10. 历史演化

| 日期 | 事件 |
|---|---|
| 2026-06-03 上午 | 用户问"go-zero 不支持 stream 怎么推送" |
| 2026-06-03 中午 | 走错严格 A 路线,被否决(`architecture-rejected-strict-ds-only.md`)|
| 2026-06-03 下午 | 选 B0 三连接,设计 push 服务 |
| 2026-06-03 傍晚 | 用户提出"WebSocket 双工合并",改 B1 单 WebSocket(`gateway-decision.md`)|
| 2026-06-03 晚上 | 用户提出**乱序问题** | 发现这是协议设计问题不是架构问题 |
| 2026-06-03 晚上 | **本文档落地**,固化 4 个原则 |
| 2026-08-05 | 用户提出"刷新状态(如匹配状态)该不该客户端主动拉";补入**原则 5**(推送不承担正确性)+ §5.4 五个刷新触发点 + §12 落地现状与缺口 |

## 11. 决策行(写入 pandora-arch.md §11)

- 2026-06-03:RPC response 与 kafka push 乱序问题确认 = 协议设计问题(非架构问题)
- 2026-06-03:固化 4 个协议原则 — Response 完整 / 不发 push 给 caller / 已受理显式 / proto 注释标注
- 2026-06-03:服务端 `PushToPlayers` / `PushToAllIncludingCaller` helper 必须强制使用(W2 时实现)
- 2026-06-03:客户端 UI 状态机原则 — 立即完成型按 response,已受理型按 push
- 2026-08-05:补入**原则 5** — push 不承担正确性,每个客户端状态必须有权威查询 RPC;push 与 pull 只能有一个写入路径(模型 A / B 二选一)
- 2026-08-05:客户端刷新触发点固化为「界面进入 / push 重连 / 切前台 / resync / watchdog」五点;常驻轮询只允许存在于有界等待态,状态落地即停表
- 2026-08-05:修正 §5.3 —— `ts_ms` 是投递游标不是事件时间,只挡旧帧不挡重投;判重必须按业务 ID

## 12. 权威态刷新的落地现状与缺口(2026-08-05 核查)

### 12.1 为什么有原则 5:2026-07-20 的真实事故

matchmaker-pve 启动时 Kafka 未就绪,producer 一次性初始化失败后 pusher **永久 nil**,`pandora.match.progress` 全程静默丢弃。组队里的**非队长成员**没有 match_id、只能靠推送获知 READY,于是永远停在 Hub。

三层修复(同日完成):

1. **启动门禁** — brokers 配置时 producer 是启动强依赖,失败在 Ready 前 exit(`services/matchmaking/matchmaker/cmd/matchmaker/main.go` `initializeMatchPublication`),与 team 同口径。
2. **READY at-least-once 补推** — 机械不变量「READY ∈ active ZSET ⟺ 推送交付未确认」,全员推送成功才 RemoveActive;崩溃窗口 / Kafka 中断由撮合循环幂等补推(重签新 jti),`expireOnce` / reconcile 不清该表项。回归测试 `ready_push_saga_test.go`。
3. **客户端 watchdog + 幂等 no-op 守卫** — 见 §12.3。

一句话教训:**推送是部分玩家获知权威状态的唯一通道时,任何一环(启动、运行中、客户端)都不允许静默丢失。** 弱依赖 warn-继续只适用于有独立兜底的通道。

### 12.2 push 通道自身已提供的兜底能力(别重复造)

以 `proto/pandora/push/v1/push.proto` 为准:

- 每玩家**严格递增投递游标** + 投递缓冲(默认 5min / 512 帧),重连回传 `last_seen_ms` 做断点续传;
- 服务端确证客户端游标之后的帧已被修剪 / 滑出保留窗(补推无法闭合)时,下发合成帧 `pandora.push.resync`(payload 空、`ts_ms=0`、不推进游标);两层检测 = 每页发送前预检(主防线,信号先于任何越过缺口的幸存帧)+ 拉空后终检(fail-closed 兜底);同一段丢失只信号一次;
- 首连(`last_seen_ms=0` 且缓冲拉空)**不做**缺口终检 —— 其正确性依赖客户端"先订阅后拉快照"(§5.4 第 3 条)。

所以客户端**不需要自己猜有没有漏**:resync 就是服务端在说"你该回源了",比裸 watchdog 精确得多,是性价比最高的兜底。

### 12.3 参考实现 = 匹配域(新域照抄这三件)

`Pandora-Client-SVN/Pandora/Source/Pandora/Private/Module/Match/Model/MyMatchModel.cpp`:

1. **resync 回源** — `HandlePushFrame` 精确匹配 `pandora.push.resync`,有活跃 match 时立即 `RequestMatchProgress(CurrentMatchId)`;
2. **有界轮询** — `MatchProgressPollTimer` 在活跃匹配期周期拉 `GetMatchProgress`,**拿到 `battle_ds_addr` 或已无 match 即停表**(间隔配 `<=0` 表示不自动轮询,交蓝图自行拉)。它同时是 resync 单次回源失败的自然补偿,所以匹配域没另做重试脏标记;Team / Friend 没有这条常驻轮询,才各自加了有限重试;
3. **watchdog** — `TeamMatchStandbyTimer`:本队 `TEAM_STATE_MATCHING` 且本地无匹配归属时周期检查,Coordinator 空闲(Idle)才触发权威恢复;首次检查等满一个完整间隔,给推送主驱动留窗口。

配套:`UMyDsRecoveryCoordinator::TryDriveTravel` 的**幂等 no-op 守卫**(目标为 Battle 且当前 live connection 端点精确一致时不重复 `ClientTravel`)是 at-least-once 补推的前提 —— 否则战斗内重复收到 READY 会把玩家拽回去重载地图。这个缺口先于补推存在,补推使之常态化后才被发现。

### 12.4 缺口清单(待办)

| # | 缺口 | 位置 | 修法 |
|---|---|---|---|
| 1 | **经验域没接 resync** | `MyPlayerProgressionModel::HandlePushFrame` 只认 `pandora.player.experience`,其余 topic 原样忽略 | 收到 resync 时回源拉一次经验快照(`GetProfile` 的经验字段与推送共用同一形态),走与推送**同一个** apply 路径。漏帧现症:等级 / 经验条停在旧值,直到下次登录拉快照 |
| 2 | **五个触发点未逐域核对** | 目前只确证匹配域有(有界)常驻轮询兜底;Team / Friend / Guild 是 resync + 有限重试 | 按 §5.4 表格逐域过「界面进入 / push 重连 / 切前台」三点是否真的接了刷新,缺哪个补哪个 |
| 3 | **聊天域尚无客户端 push 消费者** | 本次核查未在客户端发现 `pandora.chat.*` 的消费者(仅注释提及) | 接入时必须同时接 resync + `PullHistory` 回源,并按 `message_id` 判重(chat 是最典型的"重投即重复"域) |
| 4 | **presence / system.notify 未接** | `pandora.presence.update` 客户端无消费者;`pandora.system.notify` 后端无 proto、无 producer(push.proto 注释已写明) | 接入前不算缺口;接入时照 §8.3 客户端清单全过一遍 |

⚠️ 本节是 2026-08-05 对 HEAD 的**静态核查**结论,未经编译与真机验证(UE 编译归用户,见 `CLAUDE.md §11.6`)。动手前先复核对应文件的当前状态。
