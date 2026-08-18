# [INC-20260818-003][P1] Hub assignment 缺来源版本，旧新 allocator 不能安全滚动共存

> **状态**：生产发布阻断（方案待拍板，未关闭）
> **类型**：`release-safety` / `backward-compatibility` / `split-brain`
> **环境**：代码对抗审计；INC-20260818-001 本机采用无在场玩家的 quiescent cutover
> **发现时间（UTC）**：2026-08-18 05:25
> **关联事故**：[INC-20260818-001](2026-08-18-p0-owner-hub-assignment-divergence.md)

## 0. 结论

INC-20260818-001 的全新 Hub Allocator 通过
`Query owner -> writer/assignment guard -> Begin` 的一次受保护 conflict 重跑，能在全新
版本副本集合中收敛最终 Redis winner。但旧 binary 会在 assignment CAS 前执行 Owner Begin，
并在 conflict 后不复核当前 assignment 就拿新 epoch 盲写旧 target。滚动升级期间只要仍有两个
旧请求在途，就能依次插入新 winner 的两轮 Begin，使 Redis=B、Owner=A2。

本机没有在场玩家，因此本次用“Hub/DS 同时缩零、旧 Pod/lease 消失、再静默 10.4s、启动
全新 allocator、Owner 最后”的停写切换安全部署。这不能作为生产滚动发布证明；仓库标准要求
新旧副本共存，不能依赖先停再起。

## 1. 确定性混版反例

```text
旧 R1(A1)、R2(A2) 在仍持 writer 时进入并暂停
新 writer 发布 B
B:  Query(E) -> guard(B)
R1: Query(E) -> Begin(A1) 成功，Owner=E+1/A1
B:  Begin(E,B) conflict；Query(E+1) -> guard(B)
R2: Query(E+1) -> Begin(A2) 成功，Owner=E+2/A2
B:  第二次 Begin conflict 后扣票
R1/R2 随后的 assignment CAS 都输给 B
最终 Redis=B、Owner=A2
```

新版本的 writer guard 无法撤销旧 binary 已越过入口的 RPC。增加固定次数重试只能提高
liveness，不能建立无上界并发下的不变量。

## 2. 为什么现有字段不能充当版本

- `assignment_id`、`auth_jti` 是随机 UUID，仅唯一不可排序。
- `assigned_at_ms` 受时钟回拨和同毫秒碰撞影响。
- `auth_epoch/auth_gen` 是 per-pod 凭据版本，跨 Pod 无序。
- `writer_token` 只按 allocator 任期递增，同一任期的多次 assignment 相同。
- `placement_version` 未接入普通 Assign/Transfer，且 Owner target 不携带。
- `owner_epoch` 是目标端提交后的版本，不能证明 Redis assignment 的来源新旧。

因此不能在不扩展持久协议的前提下，让 Owner 原子拒绝一个“看起来 target 不同但实际更旧”
的 assignment。

## 3. 建议永久方案

- Hub assignment 增加持久、严格单调的 source revision；仅真实 target-changing CAS 领取，
  TTL 刷新、凭据轮换和 cleanup-only CAS 不推进。
- revision 必须跨 assignment TTL、释放和 writer 换届保留；可用 writer 任期高位 + 任期内严格
  递增低位，或独立持久序列，但不能用墙钟或 UUID 排序。
- Owner 持久保存 per-player Hub high-water，并在 owner 行锁事务内与 target/epoch 一起比较和提交：
  同 revision+同 target 幂等，低 revision 或同 revision 不同 target 拒绝，高 revision 才可迁移。
- 一旦某玩家见过非零 revision，永久拒绝 legacy revision=0；Release/Battle transition 不清 high-water。
- 分阶段发布：expand DDL/proto -> 可兼容 legacy=0 的新 Owner 全量 -> 新 Hub writer 滚动并写非零
  revision -> 证明旧 writer 排空 -> 激活全局 legacy 拒绝门。每阶段必须可回滚且 fail-closed。

## 4. 关闭条件

- [ ] source revision 的领域语义、持久生成器和高水位 DDL/proto 已定稿
- [ ] Owner 低版本拒绝、同版本冲突、Release 后旧写、writer 换届与 TTL 后旧写测试通过
- [ ] Hub Assign/Transfer/drain 的 revision 领取与 CAS loser 故障注入通过
- [ ] 旧 Owner/新 Owner、旧 Hub/新 Hub 的滚动兼容矩阵通过
- [ ] 生产式 RollingUpdate 演练中零停写、零 ticket/Owner 分叉

**关闭结论**：未关闭；在完成上述协议前，INC-20260818-001 的修复只允许用于明确排空旧
allocator 的本机/维护窗切换，不得标记 production rolling-ready。
