# 崩溃与 P0 事故档案

本目录是 Pandora 服务端统一事故事实源，记录“具体一次事故发生了什么、为什么发生、如何恢复、修复是否真正上线并验证”。稳定架构规则写入 `docs/design/`，取证与处置方法写入 `docs/ops/`，静态审核与交接写入 `docs/reviews/`；这些文档可以链接事故档案，但不能替代事故档案。

从 `INC-20260721-001` 开始执行本规范。历史上散落在设计文档或审核文档中的事故说明暂不批量迁移，避免链接漂移；后续触及时可补索引或按新模板回填。

## 1. 强制建档范围

下列任一条件成立，必须在发现当次从 [`template.md`](template.md) 复制新文档并登记到本页：

- 任意 Go 服务、UE DS、sidecar 或关键工具发生 runtime fatal、未恢复 panic、SIGSEGV、abort/assert、OOMKilled、CrashLoopBackOff、非预期进程退出或无法解释的容器重启。
- 任意故障导致玩家掉线、被踢、无法登录/匹配/进场、永久中间态、数据错误/丢失、安全边界突破、脑裂、关键服务大面积不可用，并判定为 P0。
- 上线前发现但若上线会造成上述 P0 后果的问题，按 `near-miss` 建档；必须明确“未在线上发生”，不得伪装成线上事故。
- 一次事故即使由多个 bug 共同造成，也使用一个 Incident ID；独立时间、独立影响或独立根因的事件分别建档并互相链接。

这里的 `P0` 指事故严重级别，不是 `stress-p0` 压测阶段、开发优先级或普通代码审查标签。未形成事故/near-miss 的低风险静态问题继续记录在 `docs/reviews/`。

## 2. 命名与状态

- Incident ID：`INC-YYYYMMDD-NNN`，创建后永久不变。
- 文件名：`YYYY-MM-DD-p0-<service>-<short-slug>.md`；同日同服务多起事故时增加两位序号。
- 时间线以 UTC 为主；同时引用客户端或本地时间时必须写明时区和换算关系。
- 文档只能追加更正和状态变化，不得删除已确认的原始时间线、旧结论或失败验证；结论被推翻时写明证据和更正日期。

统一状态：

| 状态 | 含义 |
|---|---|
| 调查中 | 已确认事故/P0，但根因尚未闭合 |
| 根因确认 | 直接根因、触发条件和放大因素均有证据 |
| 已止血 | 当前影响停止，但永久修复尚未完成 |
| 已修复待部署 | 代码/配置已完成并验证，目标环境尚未加载新产物 |
| 已部署待验证 | 新产物已加载，仍需故障路径和观察窗口验证 |
| 已关闭 | 关闭门槛全部满足，且没有未披露的 P0 剩余项 |

## 3. 建档与关闭规则

事故一经确认，先填写最小事实：Incident ID、严重级别、类型、状态、环境、发现时间、受影响服务/版本、玩家影响、第一现场证据位置和当前负责人。根因尚不明确时必须写“未知”，不能用猜测填满文档。

每篇事故文档必须持续补齐：

- 原始症状与无关噪声排除；
- 完整时间线、调用链、关键变量及所有权/生命周期；
- 直接根因、必要触发条件、故障放大因素，以及 Recovery/重试/租约为何没有挡住；
- 全仓同类模式扫描范围、命中项、排除项和未扫描边界；
- 临时止血、永久修复、测试、部署、回滚、故障注入、观察窗口及剩余风险；
- 修复 commit、镜像 digest、Pod/GameServer UID、部署时间等可追溯产物证据。

P0 不能仅凭“代码已改”“普通单测通过”或“服务重新 Ready”关闭。至少需要：

1. 修复前失败、修复后通过的针对性回归；
2. 并发问题在支持环境完成 `go test -race`，或明确记录阻断且状态不得关闭；
3. 对应的集成/故障注入验证，覆盖进程 fatal、OOM、SIGKILL、重启或该事故的真实失败模式；
4. 目标环境实际加载的新 commit/镜像 digest/配置可追溯；
5. 同类代码扫描完成，相关 P0 一并修复或另建 Incident；
6. 观察窗口无复发，玩家路径和补偿/恢复路径均验证；
7. 所有未完成项都有负责人、状态和后续 Incident/任务链接，不把风险包装成已关闭。

## 4. 证据与脱敏

- 可以记录 trace ID、match ID、Pod/GameServer 名称和 UID，但不得写入 JWT、DSTicket、密码、私钥、Secret 原值、Authorization header 或完整个人数据。
- 含凭证的原始日志只记录受控存储位置、时间范围和哈希/查询条件；事故文档只摘录完成根因证明所需的脱敏片段。
- 日志时钟、集群时钟和客户端时钟不一致时，必须明确时区，不能按肉眼相近直接拼时间线。

## 5. 事故索引

| Incident ID | 日期 | 严重级别 | 类型 | 状态 | 服务/主题 | 文档 |
|---|---|---|---|---|---|---|
| INC-20260727-002 | 2026-07-27 | P0 | crash / availability | 正式限额已定 14Gi（待部署验证，未关闭） | Battle DS 加载 Artic01 anon 内存顶死旧 limits 2Gi 被 memcg OOM 杀（dmesg 9 例）；实测 peak=10.43GiB/12Gi 围栏 0 OOM，limits=requests=14Gi（×1.34）+ autoscaler 上限 3 已落配置 | [事故报告](2026-07-27-p0-battle-ds-artic01-memcg-oom.md) |
| INC-20260727-001 | 2026-07-27 | P0 | availability | 已部署但验证失败×2（第三 P0 已修待重部署，未关闭） | Artic01 冷加载期 warming DS 被单阈值 sweep 误回收；第二 P0=BeginPlay 过早宣告 running（心跳移 PostLoadMapWithWorld）；第三 P0=PostLoadMap 回调内首拍即激活/放行 ds_addr，回调后线程续阻塞 17s 被 ACTIVE 15s 回收（实测两例）——已下沉为 allocator 两阶段激活门（≥3 次实收心跳且跨度 ≥10s 才提升，期间保持 warming）+ DS 纯周期心跳 + NetDriver fail-closed 门；验收门 A/B/C、pinger 硬门与节点级抖动定谳（A10）OPEN | [事故报告](2026-07-27-p0-ds-allocator-warming-coldload-reclaim.md) |
| INC-20260726-003 | 2026-07-26 | P0 | availability / client-state / near-miss | 已修复待验证（未关闭） | UE SelectRole 旧请求被权威换代后门闩未释放；ROLE_REQUIRED 重进选角会永久吞掉后续确认 | [事故报告](2026-07-26-p0-client-select-role-stale-singleflight.md) |
| INC-20260726-002 | 2026-07-26 | P0 | session-fencing / data / availability / near-miss | 补偿已修复，仍有相邻交付缺口（未关闭） | A/B/C 未交付前代恢复已改无能力墓碑；post-Set placement 失败仍会扣留新 session | [事故报告](2026-07-26-p0-login-session-candidate-delivery.md) |
| INC-20260726-001 | 2026-07-26 | P0 | split-brain / data / availability / near-miss | 修复实施中（未关闭） | hub_allocator writer lease TTL 盲续/复活；assignment 补偿丢 operation identity/PTTL 与 tombstone ABA；CreateShard 绕 fence；Redis HA 已确认写回档仍 OPEN；canonical green strategy 漂移 | [事故报告](2026-07-26-p0-hub-writer-fencing-near-miss.md) |
| INC-20260724-001 | 2026-07-24 | P0 | availability | 修复实施中(未关闭) | 战斗中退出后进不去：DS 分配不可用（fleet churn 至 ready=0 + 控制面超时）叠加 matchmaker 成局最终门 **结构性 100% 假阳性**（MATCHING 投影零保活，30s TTL 后必然全员误判离线；实测 31.07s 判死）+ ALLOCATING 期玩家无出口；已落码 FIX-1（两道 presence 判死门回退关闭）+ FIX-2（pre-checkpoint 取消出口），单测绿、真集群未验证。原"孤儿 start-claim"与"travel churn 致 presence 失效"两假设均被推翻 | [事故报告](2026-07-24-p0-matchmaker-orphan-start-claim-freeze.md) |
| INC-20260722-004 | 2026-07-22 | P0 | security / session-fencing / near-miss | 修复实施中(未关闭) | push 旧/被顶号会话 token 仍能订阅私有推送流(建流无 jti 现行性校验,流寿命无界) | [事故报告](2026-07-22-p0-push-stale-session-subscribe.md) |
| INC-20260722-003 | 2026-07-22 | P0 | data / near-miss | 修复实施中(未关闭) | inventory Bag journal sweep 可删除 checkpoint 未覆盖的恢复尾部 | [事故报告](2026-07-22-p0-inventory-bag-journal-sweep.md) |
| INC-20260722-002 | 2026-07-22 | P0 | split-brain / near-miss | 修复实施中(未关闭) | locator 面已 fail-closed；Owner Authority 仍缺 Login+Hub+Battle 同批强 Begin/Admit/Release、稳定 operation/票据身份与 WAIT 恢复，禁止只启用 Hub contract | [事故报告](2026-07-22-p0-hub-allocator-locator-fail-open.md) |
| INC-20260722-001 | 2026-07-22 | P0 | data / near-miss | 修复实施中(未关闭) | trade 结算成功后 Redis 订单终态可被并发取消撕裂 | [事故报告](2026-07-22-p0-trade-settlement-state-race.md) |
| INC-20260721-001 | 2026-07-21 | P0 | crash | 根因确认 | ds_allocator Heartbeat 响应 metadata 并发写导致 fatal，租约恢复窗放大为玩家被踢 | [事故报告](2026-07-21-p0-ds-allocator-heartbeat-context-race.md) |
