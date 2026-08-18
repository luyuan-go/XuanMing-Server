# [INC-20260818-003][P1] Hub assignment 缺来源版本，旧新 allocator 不能安全滚动共存

> **状态**：协议已落码，等待分阶段发布与生产演练（未关闭）
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

## 4. 已落码的协议实现（2026-08-18）

§3 的永久方案已按下述形态落码。**这不等于事故可关闭** —— 关闭条件里的生产演练部分本机
做不了，见 §5。

**编码**（`pkg/placement/source_revision.go`）

`source revision = 写者任期(高 40 位) × 2^24 + 任期内序号(低 24 位)`。高位取 writerlease 的
token（= 本届 leader key 的 etcd `CreateRevision`，历届严格递增且持久），低位是同一任期内
唯一写者进程的原子自增。**不需要额外的持久发号器**：任期号本身持久，进程崩溃重启必然拿到
更大的任期号，低位从 0 重来也不会与旧任期的号相撞。两侧越界一律 `fail-closed` 报错，
绝不回绕 —— 回绕会让更旧的来源铸出更大的号，那是比没有门更坏的失效形态。

**领号**（`hub_allocator`）

只有两个真实置换点领号：`replaceAssignmentSaga`（AssignHub / TransferHub 共用）与
`migratePlayer`（drain 迁移，是前者的既定变体）。TTL 刷新、凭据轮换、cleanup-only 标记、
墓碑删除都原样带走旧号。铸不出号（失租 / 号段耗尽）即整笔放弃并补偿座位。
未启用 writerFence 的部署（dev / mock / 单副本 Recreate）返回 legacy=0：那里不存在两个写者
并存，本门无事可做。号写进 `HubAssignmentStorageRecord.source_revision`（allocator.proto 32），
Owner Begin 时**从已发布的 assignment 记录里取**而不是现铸 —— 现铸会让迟到的 Begin 拿到比它
所绑定的 assignment 更新的号，恰好绕过本门。

**比较与持久化**（`owner`）

`owner_record` 新增 `hub_source_revision`（每玩家高水位）。`BeginTransition` 在**行锁事务内、
所有其它分支之前**比较（`classifySourceRevision`）：

| 情形 | 处置 |
|---|---|
| incoming=0 且全局门开 | 拒 `legacy_rejected_globally` |
| incoming=0 且高水位>0 | 拒 `legacy_after_versioned`（逐玩家自动生效，无需开关） |
| incoming=0 且高水位=0 | 放行、不建水位（兼容窗） |
| incoming < 高水位 | 拒 `older_than_high_water`（§1 反例的 R1/R2 落在这） |
| incoming = 高水位，同 target | 放行、水位不动（幂等） |
| incoming = 高水位，异 target | 拒 `same_revision_different_target`（铸号被复制） |
| incoming > 高水位 | 放行并推进 |

三条必须一起成立的细节：①闸门**只对 HUB 生效**，BATTLE 不带号也不动水位（对 BATTLE 也比较
会让 battle 的 0 被「见过非零就拒 legacy」挡下，玩家永远进不了战斗）；②写入侧取
`max(旧水位, incoming)` 而不是直接赋值（直接赋值会让一次合法的 legacy 写把水位打回 0）；
③`Release` 的 UPDATE 列清单刻意不含本列（清掉等于「打完一局回大厅」就把门重新敞开）。

新增错误码 `ERR_OWNER_SOURCE_REVISION_STALE = 15006`；全局 legacy 拒绝门是
`owner.reject_legacy_source_revision`，默认关。

**已通过的验证**

- `pkg/placement`：跨任期全序、越界 fail-closed、legacy 哨兵不与真实铸号相交。
- `owner`：判定矩阵 8 格逐格（含放行与拒绝两个方向）+ legacy 永不推进水位。
- `hub_allocator`：同任期严格递增、跨任期不回退、失租 fail-closed、未启用 fence 回 legacy。
- 受影响 5 个 module 全量回归通过（pkg / owner / hub_allocator / ds_allocator / login）。

## 5. 关闭条件

- [x] source revision 的领域语义、持久生成器和高水位 DDL/proto 已定稿
- [x] Owner 低版本拒绝、同版本冲突、legacy 兼容窗与永久拒 legacy 的判定矩阵测试通过
- [ ] **Release 后旧写、writer 换届与 TTL 后旧写的真库测试**（判定矩阵已覆盖纯函数，
      但事务级行为需连真实 MySQL/TiDB 跑 `owner_repo_mysql_test.go`，本机未执行）
- [ ] Hub Assign/Transfer/drain 的 revision 领取与 CAS loser 故障注入通过
- [ ] 旧 Owner/新 Owner、旧 Hub/新 Hub 的滚动兼容矩阵通过
- [ ] 生产式 RollingUpdate 演练中零停写、零 ticket/Owner 分叉

**分阶段发布顺序（每阶段可回滚且 fail-closed）**

1. **expand DDL**：`owner_record` 加 `hub_source_revision`（mysql-init / tidb-init 已含建表列，
   既有库按两份 init 脚本里的 ALTER 注释手工补）。此时无人读写该列。
2. **新 Owner 全量**：`reject_legacy_source_revision` 保持 **false**。此阶段能容忍 legacy=0，
   旧 hub_allocator 不受影响。
3. **新 Hub writer 滚动**：开始写非零 revision。**逐玩家的永久拒 legacy 从这一刻自动生效**
   —— 某玩家被新写者服务过一次之后，旧写者就再也碰不到他。
4. **证明旧 writer 排空**：确认没有旧版本 hub_allocator Pod 在跑。
5. **打开全局门**：`reject_legacy_source_revision: true`。提前打开会让仍在跑的旧副本全部
   写失败 = 大厅分配停摆。

回滚：从后往前逐阶段回退。⚠️ **DROP COLUMN 必须等到全部 hub_allocator 副本退回不写 revision
的版本之后** —— 列还在而新写者已在写非零值时 DROP，会把水位抹成 0 = 门重新对 legacy 敞开。

**关闭结论**：未关闭。协议本体已落码并通过本机可做的全部验证，但阶段 3~5 的滚动兼容矩阵与
生产 RollingUpdate 演练尚未执行；在演练通过前，INC-20260818-001 的修复仍不得标记
production rolling-ready。
