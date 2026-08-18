# [INC-20260818-002][P1] Battle Owner Begin 不确定提交缺持久恢复阶段

> **状态**：安全缓解已部署本机，持久自动恢复待实施（未关闭）
> **类型**：`availability` / `distributed-commit uncertainty`
> **环境**：代码审计、确定性故障注入与本机 `pandora-agones` 部署；未在真实玩家对局触发
> **发现时间（UTC）**：2026-08-18 05:10
> **关联事故**：[INC-20260818-001](2026-08-18-p0-owner-hub-assignment-divergence.md)

## 0. 结论

DS Allocator 在 Battle allocation 已技术 READY 后逐玩家调用 Owner Begin。若 Owner
事务已提交、但 gRPC 回包丢失，旧实现会把该次当失败并回收 Pod，留下
`Owner PENDING -> 已删除 Battle Pod` 的死归属。当前修复用显式 UUIDv4 operation
和独立 Query read-back 判定结果；仍不可判定时拒绝 READY 交付，但保留
allocation/Pod 与已写 grants，不再主动制造死归属。

剩余可用性风险是：若 unknown 实际未提交，当前没有持久 `owner-binding`
计划/阶段让后续 claim loser 重放缺失玩家。loser 只读 exact 门会持续 fail-closed，
直到 no-show/空场回收链结束本 allocation。默认配置下该状态有界，但仍应
用持久 bind plan + reconciler 替代。

## 1. 已落地的安全缓解

- DS Owner helper 只做一次 `Query + Begin`；`EPOCH_CONFLICT` 原样 fail-closed，禁止
  拿旧 allocation 盲重试。
- 每玩家 Begin 显式携带 UUIDv4 operation；只有回包 operation 等于本请求才记为
  本批 grant，same-target no-op 不会被后续失败误 Release。
- 非 conflict 错误后用独立 2s 预算 Query read-back：exact+本 operation 视为已提交；
  exact+其他 operation 视为已有幂等结果；明确 non-exact 才允许普通失败补偿。
- Begin 与 read-back 都不可判定时返回 outcome-unknown sentinel：winner 不交付 READY、
  不 rollback grants、不 cleanup allocation/Pod。
- claim loser 只 Query roster 全员 Owner full-exact，再 one-shot 复核同一 READY
  allocation/Pod/UID/epoch/track；不 Begin、Release 或 cleanup。

## 2. 验证

```text
go test -p 1 -count=1 ./services/battle/ds_allocator/...
PASS

tools/scripts/go_test_race.ps1 -Pattern ./services/battle/ds_allocator/internal/biz -Timeout 20m
PASS (Linux golang:1.26.5, CGO/race)
```

确定性用例覆盖 commit-then-response-loss、read-back 也失败、批内 grants 保留、
same-target no-op 不误回滚、claim loser 在 winner Begin 前拒绝 READY，以及 Owner 完整
exact 后只读恢复交付。

安全缓解随 `pandora/ds-allocator:g796da364-dirty-20260818-014129-inc20260818`
于 2026-08-18 05:48:24 UTC 部署到本机；两个 Pod 的实际 imageID 均为
`sha256:569e8abb4a38…`、Ready 且 restart=0。该证据只证明缓解产物已运行，不替代
网络丢包注入、Battle 玩家 E2E 或持久 reconciler 的关闭条件。

## 3. 待实施的永久恢复

- 在 Battle 持久状态中记录 roster 对齐的 owner operation plan 和
  `OWNER_BINDING/OWNER_BOUND` 阶段；计划必须在首个 Begin 前定案。
- winner 与后续 reconciler 只重放持久 operation，不自铸新 operation；全员 exact 后
  CAS 推进 `OWNER_BOUND`。
- READY 交付必须同时证明 allocation exact + `OWNER_BOUND`；回收必须识别
  binding outcome unknown，避免与 reconciler 并发。
- 补持久阶段崩溃点、两个 reconciler 并发和部分 roster 故障注入，再安排上线。

## 4. 关闭条件

- [x] 旧实现的死归属路径已用确定性用例锁定
- [x] 当前版本在不可判定时安全 fail-closed，不删可能已被 Owner 引用的 Pod
- [ ] 持久 owner-binding plan/phase 与 reconciler 已实施
- [ ] 真 Redis/Owner 网络丢包故障注入已通过
- [ ] 部署后 Battle 玩家 E2E 与观察窗口已通过

**关闭结论**：未关闭；当前已随 INC-20260818-001 的 allocator 安全修复部署到本机，
但不得宣称 outcome-unknown 已具备无界自愈。
