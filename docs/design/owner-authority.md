# Owner Authority(每玩家 owner_epoch 线性一致权威,CLAUDE.md §9.22 落地)

> 状态:**设计定稿 + 权威本体及 login/Hub allocator/DS allocator Admission 链已接线；
> 当前仍属 migrate 阶段，生产滚动兼容与旧门退役未完成**。本文档是 §9.22 的实现设计;它是背包域 phase 2(DS 写权威切换)
> 与 §9.23 幂等进场链的硬前置。现状地基:pkg/placement 的 fence-lease 常量(20/7/27s)与
> `Target` 实例身份元组、battle-reconnect.md §8 的心跳再入屏障子集。

## 0. 目标与非目标

**目标**:为"当前哪个 DS 有权控制和修改玩家"建立唯一权威:每玩家单调 `owner_epoch`、
owner 类型、exact DS 实例身份、稳定 `operation_id`、短 lease 绑定、`admit_not_before`
迁移屏障、最多 PENDING/ADMITTED 两阶段——全部处于**同一线性一致事务域**。

**非目标**(§9.22 明令禁止的膨胀):不做通用 placement saga、不做多套 proof、不做影子状态;
player_locator 继续只作 presence 投影;匹配阶段状态仍归 matchmaker;角色权威仍归 player。

## 1. 宿主与存储选型

- **宿主:新独立服务 `owner`**(runtime 域,端口 20017/21017)。§9.22 要求"只有唯一 owner
  authority 能通过受控 transition API 原子修改;Login/matchmaker/allocator/DS 只能查询或
  提交绑定 operation_id 的命令,不得各自 raw CAS"——库形态会让每个调用方都变成写者,
  服务形态才是"唯一权威"。服务本身无状态可水平扩展(原子性在存储 CAS,副本只是通道)。
- **存储:生产 TiDB,dev 允许单机 MySQL**。§9.22 硬性要求线性一致读/CAS、法定多数侧单写、
  已确认写故障切换不回滚——TiDB(raft 多数派提交)满足;MySQL 异步复制主从切换会回滚已确认
  写,**生产禁用**;dev 单机 MySQL 无复制、天然线性一致,允许联调(配置 DSN 决定,文档强制
  生产形态)。owner 记录 + DS 实例租约 + 审计流水**三表同库**,epoch/lease/admit_not_before/
  phase 的读改写全部单事务完成——不存在"owner 放 A 库、lease 放 B 库再跨存储先查后写"。
- **SQL 写法 TiDB 安全**(公会迁 TiDB 的既有教训):单行悲观锁(SELECT ... FOR UPDATE 锁
  存在行)+ 条件 UPDATE,不依赖间隙锁;`owner_record` 雪花 player_id 主键在 TiDB 侧用
  NONCLUSTERED + SHARD_ROW_ID_BITS 打散写热点。

## 2. 数据模型(`pandora_owner` 库)

```text
owner_record  player_id PK | owner_epoch | owner_type(0 none/1 hub/2 battle) | phase(1 PENDING/2 ADMITTED)
              | pod_name | instance_uid | instance_epoch | assignment_or_allocation_id | release_track
              | operation_id | admit_not_before_ms | updated_at_ms
              (owner 记录永不因 TTL 消失;显式 Release 置 owner_type=none 但 epoch 保留继续单调)
ds_instance_lease  instance_uid PK | pod_name | instance_epoch | release_track
              | lease_deadline_ms | updated_at_ms
              (实例级租约,allocator 心跳驱动 Renew,deadline 只前进;与 owner_record 同库同事务域)
owner_transition_log  id PK | player_id | from_epoch | to_epoch | op(1 begin/2 admit/3 release)
              | operation_id | detail | created_at
              (审计 append;90 天 sweep,§9.24)
```

**玩家可操作判定**(单事务内两读,同一线性一致域):
`record.phase == ADMITTED ∧ 调用方 epoch == record.owner_epoch ∧
 record 指向的 ds_instance_lease.lease_deadline_ms > now ∧ 实例身份 exact 匹配`。

**租约分层**(把续租 QPS 钉在实例粒度):lease 是**实例级**的(几百台 DS,每 ~5s 一续),
不是玩家级(60 万玩家级续租会压垮任何权威存储)。玩家的"owner lease"派生自其 owner 实例的
租约行。owner 切换 CAS 后,旧实例照常为它上面的**其他**玩家续租——对被迁走的玩家无效,
因为其权限判定的 epoch 已变(§9.22"CAS 后旧 epoch 的续租一律失败"的实现形态)。

## 3. Transition 状态机与 API(pandora.owner.v1)

```text
(none/E) --BeginTransition--> (E+1, PENDING, new target, admit_not_before)
(E+1, PENDING) --Admit(屏障开 + exact target 五字段)--> (E+1, ADMITTED)
(E+1, *) --BeginTransition(下一次迁移)--> (E+2, PENDING, ...)
(E, ADMITTED) --Release(epoch 匹配)--> (E, none)   // epoch 不回退,record 不删
```

- **QueryOwner(player_id)** → 当前记录(none 也返回 epoch;查询失败调用方按 UNKNOWN 处理,
  §9.22 禁冒充 OFFLINE)。
- **BeginTransition(player_id, expect_epoch, operation_id, owner_type, target)**:
  单事务:锁 owner_record 行(无行则建 epoch=0/none)→ `expect_epoch != 当前` → 返回
  ERR_OWNER_EPOCH_CONFLICT + 当前记录(调用方重查再决策,禁盲重试)→ 读旧 target 的
  `ds_instance_lease`,`admit_not_before = max(now, 旧 lease_deadline) +
  DSFenceSkewMarginSeconds`(常量单一来源 pkg/placement;旧实例无租约行/已过期 →
  `now + margin`)→ 写 `E+1/PENDING/new target/operation_id/admit_not_before` + 审计。
  **幂等**:同 (player_id, operation_id) 重试且记录仍是本次结果 → 原样返回(响应丢失安全);
  operation_id 必须是 UUIDv4(placement.ValidOperationID)。
  **同 target 幂等**:只有 owner type 与完整 target
  (`pod_name + instance_uid + instance_epoch + assignment_or_allocation_id + release_track`)
  全等且 phase 有效时,才原样返回既有记录,不推进 epoch、不改 phase、不覆盖 operation_id。
  同一物理实例上换发 assignment/allocation 或 release track 不是“字段刷新”,而是一次真实
  owner 身份迁移:走正常 CAS 写 `E+1/PENDING/new operation`。assignment UUID 没有单调次序；
  若在旧 epoch 内原地覆盖,迟到旧 Begin 能把新值回写成旧值,Battle allocation/track 也会继承
  旧 epoch/ADMITTED 权限。新 epoch 使旧票、旧 ACK 与迟到写统一失效。
- **Hub assignment 发布顺序**:Redis assignment CAS 是本次路由的线性化点。签名可在 CAS 前
  纯计算,但 owner Begin 只能由 CAS winner 在发布后执行；Begin 前后都重读 Redis 并校验完整
  ticket binding,任一漂移就扣票并让外层重读。Begin 遇 epoch conflict 只允许完整
  重跑一次 `Query owner -> 复核 writer lease + Redis exact assignment -> Begin`；第二轮
  仍 current 才能写,绝不得只换新 epoch 盲写原 target。这一次受保护重试用于收敛
  “旧请求已过 guard、最终 winner 随后发布”的窗口；通用/DS helper 无当前意图 guard,
  遇 conflict 仍单次 fail-closed。
  该证明以所有写者均执行 guard 为前提；旧 binary 的 sign-before-CAS/blind retry 不能与新版本
  普通滚动共存。生产滚动所需 source revision/Owner high-water 见
  [INC-20260818-003](../incidents/2026-08-18-p1-hub-assignment-source-revision-rollout.md)。
  Login 在同一响应交付前还必须验证 Hub ticket 与 owner Resume exact target 全等。
- **Battle READY 交付顺序**:allocation claim winner 在技术 READY 后、对外交付前逐玩家
  Begin(BATTLE)。DS helper 没有 Hub Redis-current guard，`EPOCH_CONFLICT` 必须单次
  fail-closed，由外层重读 allocation，禁止盲重试旧 target。批量补偿所需的
  每玩家 operation 由 DS 显式铸 UUIDv4；只有成功回包的 operation 等于请求值
  才是本批 grant，same-target no-op 不可回滚。非 conflict 回包错误必须 Query
  read-back；仍不可判定时扣 READY 并保留 allocation/Pod，不得删除可能已被
  Owner 引用的实例。claim loser 只读确认 roster 全员 Owner full-exact，再 one-shot
  复核同一 READY allocation；不调 Begin/Release/cleanup。持久 outcome-unknown 恢复
  见 [INC-20260818-002](../incidents/2026-08-18-p1-ds-owner-bind-outcome-unknown.md)。
- **Admit(player_id, owner_epoch, operation_id, target)**:单事务:锁行 → epoch/operation/
  exact target 五字段全等校验 → `now < admit_not_before` → ERR_OWNER_BARRIER_NOT_OPEN
  (带剩余毫秒,调用方退避重试,§9.23 WAIT 语义)→ PENDING→ADMITTED CAS + 审计。
  **幂等**:已 ADMITTED 且 exact target 五字段一致 → 原样返回 ADMITTED(Admission 回包丢失不再分配、
  不创建第二 owner)。新 DS 在 Admit 成功前只能预载资源,不得建可操作 Pawn(§9.22,DS 侧约束)。
- **RenewInstanceLease(target, lease_seconds)**:upsert 实例租约行,deadline 只前进
  (旧实例纪元/UID 不匹配 → 拒);lease_seconds 服务端钳制 ≤ DSFenceLeaseMaxSeconds。
  由 hub_allocator / ds_allocator 在处理 DS 业务心跳时代写(DS 不直连 owner 服务续租)。
- **ReleaseOwner(player_id, owner_epoch, operation_id)**:epoch+operation 匹配才置 none;
  迟到 Release(旧 epoch)幂等 no-op 返回当前记录——"迟到 Logout 只能 compare-delete 自己"。

## 4. 与既有机制的关系(expand → migrate → contract)

| 阶段 | 内容 |
|---|---|
| expand(已完成) | owner 服务 + 三表 + transition API 落地;无调用方,现网行为零变化 |
| migrate(当前) | ①login §23 入口 query-first 接 QueryOwner,需要新归属时 BeginTransition(hub 进场/选角后首 hub);②ds_allocator READY 交付前 BeginTransition(battle);③DS Admission 链提交 Admit;④battle_result 终局 → Battle→Hub 新 operation;⑤logout → Release;⑥**allocator 心跳双写 RenewInstanceLease(已落码 2026-07-22)**:hub/ds 两 allocator 的 Model B 授权心跳成功后、响应返回前经 `renewOwnerLeaseGate` 双写(必须先于响应:DS 收到响应才延长本地租约,权威侧须先覆盖该认知);`owner_addr` 空=不启用,`owner_lease_required` 默认 false=弱依赖(失败告警放行,旧门兜底),contract 阶段置 true 转强依赖(失败即心跳失败→DS 自我 fencing,时序闭合);hub 凭据无实例纪元→epoch 传 0(owner 侧仅双方非零且不同才拒)。窗口内**新旧双门并行**:既有 last_heartbeat_ms 再入屏障照跑,admit_not_before 只增不减安全性 |
| contract | 全链路验证后,last_heartbeat_ms 再入门退役,§9.22"尚未实现"注记删除;背包域 phase 2 / §9.23 幂等进场链解锁 |

- fence 常量沿用 `pkg/placement`(单一权威计算入口):skew margin 就是 admit_not_before 的
  余量项;DS 侧自我 fencing 契约(battle-reconnect.md §8)不变。
- hub_allocator 的 assignment / ds_allocator 的 allocation 记录保留(容量、座位、回收等
  运营细节),但"谁 own 谁"以 owner_record 为准——它们降级为执行细节,不再充当归属权威。

## 5. 失败模式与验证矩阵(§16)

| 风险 | 防线 | 验证 |
|---|---|---|
| 并发双迁移 | 行锁 + expect_epoch CAS,一胜一 CONFLICT | 并发 Begin 集成测试 |
| 屏障未开抢跑 | Admit 校验 now ≥ admit_not_before | 时钟注入:早到 Admit 必拒 |
| Admission 回包丢失 | Admit 幂等重放返回原 ADMITTED | 重放测试 |
| Begin 响应丢失 | (player, operation_id) 幂等原样返回 | 重放测试 |
| 旧 epoch 迟到写 | epoch 全等校验;Release 旧 epoch no-op | 旧 epoch Admit/Release 测试 |
| 实例/归属替换伪装 | exact target 五字段(pod/uid/epoch/assignment-or-allocation/track)全等 | 同名换 UID、旧 assignment、错误 track 的 Admit 必拒 |
| 租约回退 | Renew deadline 只前进 + 实例身份校验 | 并发 Renew 单调测试 |
| 旧实例租约撑大屏障 | admit_not_before 取 CAS 时点观察值,后续续租不影响已算屏障 | Begin 后续租再 Admit 测试 |
| 同实例 assignment 换发分叉(resume↔票据死循环) | 完整 target 不同即 epoch+1/PENDING；Hub assignment CAS winner 才能 Begin，Begin 前后复核 Redis；Login/ACK exact gate | 换发后 Query 收敛、新 assignment Admit 放行、旧 epoch/assignment 拒；CAS loser 零 owner 副作用；票与 Resume 不一致即 WAIT |
| 审计增长 | transition_log 90 天 sweep | 既有 sweep 模式 |

## 6. 复杂度举证(§15.4)

简单方案"继续用 locator TTL + 心跳再入屏障"无法满足已确认约束:§9.22 明确 TTL 投影不能
证明离开、不能授权进入;§9.23 需要 PENDING/ADMITTED + operation 幂等;背包域 phase 2 需要
可校验的"该 DS own 该玩家"。新增组件 = 一个小服务 + 三张表 + 五个 RPC,记录形态被 §9.22
明文限定(最小权威事实 + 必要租约),无预设扩展。回退:migrate 阶段双门并行,owner 服务
不可用时调用方按 UNKNOWN/WAIT 处理(fail-closed,不降级为旧门单独放行新归属)。
