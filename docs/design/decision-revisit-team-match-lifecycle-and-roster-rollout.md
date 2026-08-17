# decision-revisit:赛后准备复位的代际幂等与花名册期限的滚动激活

- 状态:**已拍板方案 A 并落码(2026-08-17),发布阻断解除**。拍板人:用户(2026-08-13
  「按最标准方案做」+ 2026-08-17 复确认);落码与复核记录见 §9(新增)。
- 历史状态:待人拍板/当前 v2 禁止部署(2026-08-13 Codex 复核提出)
- 触发:INC-20260813-001 v2 把 `EndTeamMatch` 挂到 `ReleaseMatch`，并给
  ds_allocator 增加 45s 花名册到齐期限。常规测试通过后，按 `AGENTS.md §4/§10` 复核
  ACK 丢失、跨代重试、旧新副本共存与不停服激活，发现当前方案尚不满足这些硬约束。
- 影响面:`TeamService.BeginTeamMatch/EndTeamMatch`、team ready 存储、matchmaker 的
  Start/Release saga、battle_result release outbox、ds_allocator battle 存储、发布器、UE proto。
- 规范依据:`AGENTS.md §4`（重复、乱序、进程重启、滚动升级必须闭环）、`§7`（推翻既有
  决策先写 decision-revisit）、`§10`（不得以停服或误伤在场玩家换取升级）。

## 1. 旧方案与这次复核推翻了什么

v2 的直接方案是:

1. `BeginTeamMatch` 只冻结 roster 并写 5s 自净租约，不改变 ready；
2. `ReleaseMatch` 先释放玩家 claim，再按 canonical match 的 roster 调 `EndTeamMatch`；
3. team 清本局成员 ready，并把 READY 队伍打回 FORMING；
4. 失败保留 canonical，由 battle_result outbox 重投。

这里“会一直重投”成立，但“天然获得幂等与同一份可靠性”不成立。RPC 只有
`team_id + player_ids`，没有 match generation、成员 generation 或 ready revision。
传输层 at-least-once 只保证旧请求会再来，不能证明旧请求仍有权改当前代状态。

本文件不推翻事故的第一根因事实：team 的确缺少赛后 ready 生命周期。推翻的是“无代际
fence 的 `EndTeamMatch` 可以直接作为最终修法”和“只规定 team 先发布就能安全上线”。

## 2. 当前方案的发布阻断证据

### 2.1 ACK 丢失后会抹掉新一轮玩家意图

可复现时序:

1. 旧局 `M1` 的 `EndTeamMatch` 已在 team 清 ready，但 ACK 丢失；
2. 玩家回大厅重新点 ready；
3. `M1` outbox 重投相同 `team_id + player_ids`；
4. team 看见当前 READY/ready，再次清除玩家刚表达的新意图。

若第一次 RPC 在写入前失败，同样会在玩家 re-ready 后发生。当前“连续调用三次”的单测只覆盖
状态没有演进的重复调用，覆盖不了这条跨代时序。

### 2.2 离队重入是 player_id 级 ABA

旧局成员离队、重新加入同一队并再次 ready 后，player_id 仍相同。旧请求没有 membership
generation，无法区分“旧局里的这个人”和“新一代成员里的同一个 player_id”。

`readyNeedsClearing` 还有一个独立边界：targets 已全部离队时，只要当前 team.State 仍是
READY，就会把由全新 roster 组成的队伍打回 FORMING。这与“成员已离队一律 no-op”和
“只复位本局 roster”冲突。

### 2.3 claim 先释放保留了原事故窗口

claim 全部释放到 `EndTeamMatch` 成功之间，队伍仍是 READY，队长可以再次调用 StartMatch。
5s Begin 租约只挡当前瞬间；租约过后旧局 End 仍可能清新局状态。因此“claim 已释放，玩家
不受影响”只能描述“不被旧 claim/4002 卡住”，不能描述业务正确性。

### 2.4 只做 team → matchmaker 顺序仍会漏局

matchmaker 单副本使用 RollingUpdate，更新时旧新 Pod 会短暂共存。battle_result 通过普通
ClusterIP 的长期 gRPC 连接可能持续钉在旧 Pod。旧 matchmaker 会按旧语义 ACK ReleaseMatch，
并让 battle_result 删除 outbox，却从不调用 EndTeamMatch。这些局没有第二次补偿机会。

当前 `tools/scripts/start.ps1` 还是整 overlay 一次 apply、全部 Deployment 先 restart、之后才
逐个 wait；release checklist 的文字顺序不是机械门禁。正向升级之外，回滚也必须反向:
先让 matchmaker 全量停止调用新 RPC，再回退 team。

### 2.5 “systemOnly”不是 matchmaker 身份

`systemOnly` 只证明请求没有玩家 JWT，能挡公网玩家；online NetworkPolicy 允许业务 Pod mesh
互通，任一可直连 team 的业务 Pod 都满足 callerID=0。对“可把任意队伍打回 FORMING”的写
RPC，这不是 matchmaker-only。若保留该 RPC，须复用 `pkg/internalrpcauth`，签名至少绑定
full method、team_id、match generation 与 payload，并共享防重放存储。

### 2.6 `team_addr` 是未声明的状态机开关

matchmaker 允许 `team_addr` 为空；main 只 Warn，`endTeamMatches` 在 reader=nil 时返回成功，
随后仍 DeleteMatch。即使没有新增专属开关，生产漏配 team_addr 仍会静默退回修复前行为。

### 2.7 allocator 的加字段兼容不等于语义兼容

ds_allocator 是 2 副本 RollingUpdate。旧副本会保留未知 protobuf 字段，但不会写
`roster_ever_complete`。若全员到齐只被旧副本看见，切到新副本时恰好已有局中掉线，新逻辑
会把该存量局当“从未到齐”，45s 后误判弃一场正在打的对局。

缺配置时默认立即启用 45s；先配 `-1` 也不够，因为当前关闭分支完全不记录“曾到齐”。

## 3. 需要收口的 module 与 seam

真正需要设计的是 team 的“ready 一次性授权”module，而不是单独给 ReleaseMatch 再挂一个
浅 RPC。它的 interface 必须同时包含以下不变量、顺序与错误模式:

- 一次 ready 意图最多授权一次 StartMatch；
- 旧 match / 旧成员代 / 旧 ready revision 永远不能改新意图；
- ACK 丢失、重复、乱序、进程重启后结果相同；
- matchmaker/team 旧新副本可共存；
- 缺配置或身份不明时 fail-closed，不得静默跳过；
- 玩家永远不会因远端服务故障永久卡在不可恢复状态。

matchmaker→team 是“remote but owned”依赖。interface 放在 team 生命周期 seam；gRPC 是生产
adapter，内存 fake 是测试 adapter。代际判断必须藏在这个深 module 的实现里，不能让每个
Release/Cancel/Retry 调用方各自猜一次。

## 4. 候选方案

### 方案 A（推荐）：ready 在 Begin 时一次性消费，不再以 End 作为正确性路径

把 ready 解释为“一次 StartMatch 的单次授权”，在 `BeginTeamMatch` 同一把乐观锁内:

1. 校验队长、READY、roster 与唯一 attempt_id；
2. 克隆并返回消费前的 roster 快照；
3. 原子清除本次成员 ready，并把队伍转 FORMING；
4. 写稳定 receipt，供同一 attempt 的短时重入识别。

matchmaker 崩溃最多让玩家重新点 ready，不会把队伍卡在 MATCHING，也没有旧 End 跨代清理
新意图的问题。旧 matchmaker 本来就调用 Begin，因此 team 全量升级后旧 caller 也自动获得
新语义；ReleaseMatch 是否命中旧 Pod 不再影响 ready 正确性。

必须同时修正两点:

- 当前 `rosterLockOperationID(teamID,captainID)` 对同一队长的每一局都相同，不是唯一
  attempt。单改 ticket_id 也不够：StartMatch RPC 重试会生成新 ticket_id。须先建立一个
  **跨客户端重试稳定、跨真实点击递增**的 attempt_id（客户端按钮动作 UUID，或 matchmaker
  在调用 Begin 前持久化并可按 captain/team 恢复的 PREPARING operation），再让 receipt 绑定它。
- legacy ready 没有“是否已被旧局消费”的证据。新 team 首次看到 legacy READY 时应保守地
  原子转 FORMING 并要求重按，不能让旧 ready 再授权一局。

代价:A-12 的产品语义会变成“点开始/进入排队就消费 ready”，所以排队取消、准入后失败也要
重新点。它比“只在战斗结算后复位”更保守，但实现最深、调用 interface 最小、失败方向安全。

### 方案 B：保留 End，但做完整代际协议

如果产品明确要求 ready 在排队/开战期间一直保留，End 请求至少要携带 Begin 返回并持久化的
fence:

- team match generation；
- team 全局单调 mutation revision；
- 每个 roster 成员的 membership/ready revision；
- 唯一 operation_id / match_id。

team 只在当前 generation 与成员 revision 精确匹配时清理；已应用 generation 重投 no-op，
更老 generation 无条件 no-op，队伍 State 由当前成员重新计算，不能盲打 FORMING。fence 必须
从 Begin receipt 一路落进 ticket、match canonical 与 release outbox，不能只存在内存。

但这仍不足以阻止 claim→End 间用旧 ready 开下一局。要保留 ready，就还需 Commit/Abort/End
完整生命周期，且排队取消、成局失败、allocation 失败、正常结算、abandoned 每条终态都必须
收口；远端状态还需可恢复 reconcile，不能靠永久 MATCHING。interface 和实现明显更重。

### 方案 C：把 End 改为 Kafka 事件

事件能解耦发布时序，却不能解决代际幂等、ABA 或旧 consumer 修改新状态；仍须方案 B 的全部
fence，且多一个 topic/consumer/outbox。当前没有第二个独立消费者收益，不推荐。

## 5. allocator 推荐激活协议

无论 team 选哪一案，花名册期限都必须单独采用 expand → observe → enforce:

1. 增加 `off/observe/enforce` 语义；新 binary 在 observe 时也持续记录到齐事实，但不判弃；
2. battle 在 Allocate 时冻结 `roster_policy_generation`，只有**全量新副本之后创建**、且 generation
   属于当前 enforce 代的 battle 才允许执行 45s；
3. 默认/legacy generation=0 永不执行新期限，存量局自然排空；
4. 回滚 enforce→observe 立即停止判弃但继续采证；再次激活须升 generation，不能复用旧计时器；
5. 发布器机械验证旧 ReplicaSet Ready=0、所有 endpoint 为目标 digest 后，才允许切 enforce。

这样旧副本是否看见过“全员到齐”不再影响任何被 enforcement 覆盖的局。

## 6. 迁移与发布顺序

在拍板和实现完成前，禁止构建/部署当前 v2，也不应先同步 UE lock（协议还会变化）。

若选方案 A，不能直接 team 先行：旧 matchmaker 的 operation id 跨真实对局复用，新 team 若据此
消费 ready 会把多局误认成一次。须拆三阶段:

1. matchmaker **expand 版本**先落稳定 attempt_id + PREPARING 恢复，但 team 仍按旧 Begin 语义；
   该版本须先全量、旧 RS Ready=0；
2. team **migrate 版本**再落 receipt + Begin 原子消费，并只接受可证明唯一的 attempt_id；
   必须用响应丢失/客户端重试验证能返回同一 receipt；
3. matchmaker **contract 版本**移除 Release→End 的正确性依赖与临时兼容逻辑；
4. allocator 先 observe 全量，再仅对新 policy generation 启 enforce；
5. UE 只同步最终 proto，并接 4011 的确定性拒绝 UI；
6. 回滚逐阶段反向执行；allocator enforce 先退 observe。

若选方案 B，team 的 fenced End + internal auth 必须先全量；matchmaker 切换阶段必须采用
blue-green/versioned endpoint 或短暂停 ReleaseMatch，保证旧 MM 不会 ACK 掉需要新 End 的局。
禁止把“replicas=1”误当成没有旧新共存窗口。

## 7. 验收标准

1. 故障注入：End/Begin 写成功但 ACK 丢失，随后 re-ready，旧重投不得改新 ready。
2. 离队→同 player_id 重入→ready，旧局终态不得改新成员代。
3. 本局 targets 已全离队且新 roster READY，旧终态必须零写零推送。
4. claim 释放与 team 依赖故障并发时，不得用旧 ready 开出下一局。
5. team_addr/鉴权缺失必须启动失败或 RPC fail-closed；任意其他业务 Pod 伪调用被拒。
6. 旧新 team/MM 真实双版本矩阵，含长期 gRPC 连接钉旧 Pod；不得丢补偿或积压无界。
7. allocator 旧局在 rollout 前已到齐、rollout 时局中掉线，永不触发 45s 判弃。
8. allocator 只对全量新副本后创建的新 generation 局 enforce；回滚/再激活不复用旧计时器。
9. 相关 module 普通回归、Linux `-race`、Redis 真后端、Model B 真集群与玩家 E2E 全部留证。
10. release 脚本有 fail-closed 的 digest/旧 RS/capability/阶段门；人工清单不能替代机械门。

## 8. 待拍板

需要产品与架构共同决定:

1. 是否接受方案 A 的“一点开始就消费 ready；失败/取消也要重按”？
2. 若不接受，是否愿意承担方案 B 的完整 Commit/Abort/End 代际状态机与 blue-green 成本？
3. 4011 只显示固定“有队员不在大厅”，还是给 StartMatchResponse 加结构化缺席成员 ID？

个人建议:**选方案 A**。它把复杂度收回 ready 生命周期 module 内，删除 ReleaseMatch 调用方
必须理解的时序知识；失败只要求玩家重按，不会静默复用或抹掉意图。产品若明确拒绝这个手感，
再选方案 B，不能继续部署当前无 fence 的折中版。

## 9. 拍板与落码记录(2026-08-17)

**拍板:方案 A**(ready 在 `BeginTeamMatch` 一次性消费),4011 加结构化缺席字段。

### 9.1 相对 §4/§6 的一处重要简化:不需要三阶段发布

§6 认为方案 A 须拆三阶段,理由是旧 matchmaker 的 operation_id 跨真实对局复用,新 team
按它消费 ready 会把多局误认成一次。落码时发现更简单的解法:**收据绑定「消费后代际」
(`MatchStartReceipt.post_ready_generation`)**。任何 ready 意图变更都会推进代际,于是:

- 真·重试(玩家什么都没动)→ 当前代际 == 收据 post 代 → 判重入,返回同一份消费前快照;
- 第二次真实点击(全队重新点过准备)→ 代际已前进 → 不是重入 → 正常消费新一代。

跨局区分由代际完成、不靠 operation_id,旧 matchmaker 打到新 team 因此天然安全,
「谁必须先上线」的顺序约束消失(§9.21 达成)。另有 60s 收据重入窗兜底
「消费后长期无人操作,久后的真实点击被误判成重试」(失败方向安全:多按一次准备)。

### 9.2 落码清单

- team `BeginTeamMatch` 三路径:收据重入 / legacy 零代际 READY 保守作废(要求重按)/
  正常消费(锁内:留消费前快照 → 清 ready → 转 FORMING → 上租约 → 盖收据);
  收据盖章在代际推进之后(`updateTeamThenStamp`)。锁冲突检查前移到 READY 判定之前
  (竞争必须报可重试的 3007,不得报终态 3006)。
- `EndTeamMatch` 降级为共存窗口兼容路径:新 team 下代际 CAS 几乎恒落空 → no-op;
  只在「新 matchmaker + 旧 team」组合真正动手。契约测试钉死零写。
- §2.1(ACK 丢失抹新意图)、§2.2(离队重入 ABA)、§2.3(claim→End 窗口用旧 ready
  开局)、§2.4(发布顺序漏局)由消费语义整体消除;§2.5 由 verifyMatchCall 三档验签
  收口;§2.6 由 matchmaker `team_addr` fail-closed(`allow_missing_team` 显式开关)收口。
- §5 allocator 激活协议落码:`roster_join_deadline_mode`(off/observe/**observe 默认**/
  enforce)+ `roster_policy_generation`(Allocate 冻结进 battle 记录,proto 字段 26);
  判弃唯一谓词 `conf.RosterDeadlineShouldAbandon`(biz legacy 与 data Model B 共用);
  observe/代不匹配到点只打 `roster_incomplete_would_abandon` 采证 WARN;
  enforce+generation=0 启动拒。激活/回滚步骤见 yaml 注释(observe+0 全量 → 代改 1 滚动 →
  确认全量后翻 enforce;回滚只翻档;再激活代 +1,旧代局连同计时整体豁免)。
- §8-3 拍板「加结构化字段」:`StartMatchResponse.absent_player_ids`(4011 时点名缺席者),
  biz 用 `MemberOfflineError`(Unwrap 链保 4011 语义)携带,service 层 errors.As 填充。
- 同批审计修复:①`formSoloMatch`/`formMatch` 装配 match 成员时丢 `team_ready_generation`
  (主路径 End CAS 恒退化 gen=0,迟到重投可抹新 ready;已补传递 + 穿真实装配的回归测试);
  ②`team_call_auth_secret` 纳入 matchmaker Validate 的跨信任域同钥拒绝(此前注释里的
  「必须不同」无机械 enforcement)。

### 9.3 §7 验收状态

1/2/4(重试不重复消费/跨代区分/旧 ready 不开二局)= 契约测试绿(begin_consume_ready_test.go);
3(全离队零写)= 消费语义下不可达,End no-op 测试覆盖;5 = conf 校验 + 验签测试绿;
7/8(allocator 滚动矩阵)= 分代豁免为机制保证 + 单测绿,真实双版本矩阵留待集群演练;
6/9/10(真实双版本矩阵、Linux -race、真集群 E2E、发布器机械门)**未做**,仍是发布前置项。
