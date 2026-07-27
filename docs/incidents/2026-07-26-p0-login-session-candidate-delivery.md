# [INC-20260726-002][P0] Login 未交付候选会话可覆盖已交付会话

> **状态**：补偿路径已修复，仍有相邻交付缺口（未关闭）  
> **类型**：`session-fencing` / `data` / `availability` / `near-miss`  
> **环境**：上线前静态审计 + 确定性交错验证；未在生产发生  
> **首次发生时间（UTC）**：不适用（上线前 near-miss）  
> **首次发现时间（UTC）**：2026-07-26 12:00:00  
> **负责人**：待指定  
> **受影响服务/版本**：`services/account/login`，基线 `37e4dc4d9599a65c1357d3efa606a3bbb3189a0b`  
> **最后更新**：2026-07-26

## 0. 一句话结论

R13 已删除“恢复即时前代”机制：Redis 写结果不确定时，MySQL 对本次 `(jti,generation)` 条件写无能力墓碑，Redis 对 `current.gen <= failedGen` 清能力并推进单调水位；更高代际赢家不受影响。A→B→C 确定性交错不再恢复未交付 B，下一次可重试 Login 以 `gen+1` 自愈。相邻缺口仍存在：`sessions.Set` 成功后 placement 依赖失败会只返回错误、不返回已建立 session；本事故因此保持未关闭。

## 1. 影响与范围

- 玩家影响：若缺陷上线，登录响应丢失、超时或 placement 依赖失败后，玩家可能持有的最后一个已成功会话被失效；表现为突然掉线、旧设备被错误顶号或被迫重新登录。
- 数据影响：不会直接回档业务数据，但 Redis/MySQL 的“当前会话”可指向从未交付的 token，违反会话权威完整性。
- 触发范围：同一账号有至少两个并发或重试中的 Login，且其中一个已提交会话写但响应/后续 placement 失败。
- 实际事故：无；本次为上线前 near-miss。
- 严重度理由：影响登录可用性、顶号安全边界和旧 JTI 全服务吊销，按 P0 建档。

## 2. 第一现场与确定性交错

### 2.1 可复现交错

```text
初始：客户端持有并已使用 A/gen1；Redis/MySQL 当前会话均为 A。

Login B：写入 B/gen2 成功，但响应丢失，B 从未交付。
Login C：写入 C/gen3；它保存的“前一代”快照是 B。
Login C：后续失败，补偿把两处恢复为 B/gen3。
Login B：迟到补偿只允许 gen2，看到 gen3 后 no-op。

最终：当前会话是从未交付的 B；最后一个已交付的 A 已失效。
```

独立审计用生产 Lua 构造过该交错，失败证据为：

```text
last delivered session A was replaced by unacknowledged B: jti="jti-B" found=true
```

临时审计用例未保留为仓库常驻红灯；在协议和状态模型补齐前，它应作为永久修复的验收用例恢复。

### 2.2 另一个确定性出口

`sessions.Set` 成功后，Login 仍会调用 locator、battle、owner、Hub 等依赖。任一依赖失败时，现有 RPC 只返回错误码，不返回刚写入的新 session，也没有可重复取得同一结果的稳定 `login_attempt_id`。因此客户端可能从未拿到新 token，而旧 token 已被服务端吊销。

## 3. 调用链与关键变量

以下是触发审计的 R12 修复前调用链，仅保留为第一现场；R13 已删除
`rollback snapshot` 与 `clearRollbackIfJTI`：

```text
Login request
  -> PersistSessionJTI(MySQL generation/JTI)
  -> sessions.Set(Redis current session + rollback snapshot)
  -> clearRollbackIfJTI                 # 早于 Login OK 交付点
  -> locator / match / owner / Hub route
       -> 成功：响应携带 session
       -> 暂时失败：只返回错误，新 session 未交付
```

| 变量/对象 | R12 修复前语义 | 缺口 |
|---|---|---|
| `generation` | 单调 fencing 水位 | 不能证明该代际曾交付或激活 |
| `_rollback_*` | 本次覆盖前的即时快照 | 三个以上交错时可能指向未交付候选 |
| `jti` | 每次 Login 新 UUID | 没有稳定 attempt 关联重试与结果重放 |
| Login response | 唯一 token 交付面 | 回包丢失后服务端没有幂等取回同一结果的入口 |

## 4. 根因

### 4.1 直接根因

1. 会话只有“当前”状态，没有区分 `candidate` 与 `active/delivered`。
2. 客户端请求没有稳定 `login_attempt_id`，网络重试会创建新的候选会话，服务端也无法幂等重放原结果。
3. 补偿以“最近覆盖值”为恢复目标，而非“最后一个已证明交付/激活的会话”。
4. Redis 与 MySQL 分属两个事务域；代际和即时前代快照只能 fail-closed，不能证明跨存储交付原子性。
5. placement 暂时失败发生在会话写之后；现有字段虽能表达 `WAIT + retry_after`，但当前
   Login 流程没有稳定 `operation_id` 和可重放结果，不能安全返回该状态。

### 4.2 为什么现有保护没有挡住

- `(jti,generation)` CAS 能阻止低代际补偿覆盖新赢家，但无法判断新赢家是否曾交付。
- Redis `_rollback_*` 能修复两方 A->B 的响应丢失，却不能在 A->B->C 中区分 B 是 active 还是未交付 candidate。
- `clearRollbackIfJTI` 在 Redis Set 响应后执行，早于真正的客户端 `Login OK`，不是交付 ACK。
- 24h token TTL、客户端重试和 session gate 都只消费“当前 JTI”，不会恢复最后已交付会话。

## 5. R13 已落地修复

- Redis `Set` 同一 `(jti,generation)` 重试改为幂等成功；同 generation 不同 JTI fail-closed 返回可重试错误。
- Redis 依赖/I/O 失败统一映射 `ErrUnavailable`，客户端进入有界退避而非终态内部错误。
- 删除 Redis `_rollback_*` 前代快照与弱清理步骤；新写路径只保留当前能力与单调 `gen`（脚本仍删除滚动窗口遗留字段）。
- MySQL 失败补偿改为 `TombstoneFailedSessionJTI`：只命中本次 `(jti,gen)`，不恢复即时前代。
- Redis 失败补偿改为 `FenceFailedSet`：`current.gen <= failedGen` 时清能力并推进水位，`current.gen > failedGen` 时 no-op。该范围条件是必须的：C 的 Redis 写完全没落地时，key 可能仍停在未交付 B；只匹配 C 会漏掉 B。
- 对于已确认 MySQL COMMIT 后的 Redis Set 失败，两侧补偿使用各自 detached 有界
  context；一侧 error/no-op 不跳过另一侧。
- MySQL COMMIT 结果不确定且读回失败是例外：Redis Set 尚未执行，事务内读到的
  generation 可能在事务未落地后被赢家复用。只有 MySQL 条件墓碑明确命中，才证明
  generation 已持久占用并允许继续 fence Redis；MySQL no-op/error 时禁止拿未证实
  generation 清 Redis。常驻交错覆盖 B 未落地、C 复用同 generation 并交付后，B
  的迟到消歧不得清掉 C。
- 常驻回归 `TestRedisSessionFailedABCInterleavingNeverRestoresUndeliveredB` 覆盖 A 已交付、B/C 未交付、C→B 旧缺陷，并验证 D/gen4 自愈。
- Login module `go test ./... -count=1` 通过。

该策略选择安全优先：若失败代际的 Redis 写实际未落地，Redis fence 也可能撤销仍在线的旧 A；客户端收到 `ErrUnavailable` 后自动重试并用更高代际恢复。它不承诺保留 A，但保证不会把无法证明已交付的 B/C 留成当前能力。

## 6. 仍未关闭的相邻交付缺口

`sessions.Set` 之后仍有 battle locator/query、LOGIN_PENDING、owner/Hub 分配和票据签发。它们的可重试失败目前使 RPC 只返回错误码，新 session 不返回但旧 session 已被轮换。现有 `LoginResponse + ResumeContext(WAIT)` 在字段层能够表达“认证成功、进场等待”，但直接改为成功返回仍不安全：WAIT 必须携带稳定 `operation_id`，并由同一 query-first owner 链持续驱动；部分旧路径尚会默认 Hub/自签 fallback。该项必须与 owner-authority 完整接线一起处理，本轮不做第二套无 operation 的 WAIT 半成品。

## 7. 验证矩阵

| 验证 | 当前状态 | 关闭要求 |
|---|---|---|
| 同 `(jti,gen)` Redis lost-reply 重试 | PASS | 保留 |
| 同 gen 不同 JTI 冲突 | PASS | 保留 |
| A 已交付、B/C 未交付、C/B 迟到补偿 | PASS（常驻确定性交错） | 真 Redis 响应丢失注入复验 |
| B COMMIT 未落地、C 复用同 generation、B 迟到消歧 | PASS（常驻确定性交错） | 真 MySQL COMMIT 模糊注入复验 |
| session 写成功后 placement 失败 | 当前只返回错误，token 未交付 | 返回 session+WAIT，重试同 attempt |
| 真 MySQL COMMIT 模糊结果 | 未执行 | 故障注入 PASS |
| 真 Redis 响应丢失/主从切换 | 未执行 | 故障注入 PASS |
| 双设备 + Envoy + 共享 Redis E2E | 未执行 | PASS |
| Linux `go test -race` | 未执行 | PASS |
| 目标镜像与观察窗 | 未部署 | 可追溯产物 + 无复发 |

## 8. 部署、回滚与关闭

- 修复 commit：无，本轮工作树未提交。
- 部署产物：无。
- 当前止血：失败会话代际统一无能力墓碑；保持新 owner/placement contract 默认关闭，post-Set placement 失败仍按 fail-closed 处理。
- 回滚：未部署，无线上回滚动作。
- 关闭条件：第 6 节 post-Set WAIT/owner 幂等链闭环，验证矩阵全绿，目标环境部署并完成观察窗。

**关闭结论与审批人**：未关闭；等待跨端协议与状态模型批次。
