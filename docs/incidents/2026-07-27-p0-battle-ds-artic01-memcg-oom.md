# [INC-20260727-002][P0] Battle DS 加载 Artic01 时被 memcg OOM 杀死（limits 2Gi 顶死）

> **状态**：正式限额 14Gi 已 apply 生效（2026-07-28 live 实测 limits=requests=14Gi、maxReplicas=2；完整一局回归与观察窗口未跑，未关闭）  
> **类型**：`crash` / `availability`  
> **环境**：本机 k8s（minikube + Agones，dev 全链路）  
> **首次发生时间（UTC）**：2026-07-27（精确时刻缺失，见 §2.2）  
> **首次发现时间（UTC）**：2026-07-27（排查 INC-20260727-001 期间从 dmesg 定位）  
> **负责人**：luhailong  
> **受影响服务/版本**：UE Battle DS（`pandora/battle-ds:dev`）、Fleet `pandora-battle-stable`（当时 limits 2Gi）  
> **最后更新**：2026-07-27

## 0. 一句话结论

Artic01（World Partition 83 cell，server cook 190.7MB）加载中段的 anon 内存冲破容器旧 memory limits 2Gi，内核 memcg OOM killer 杀死 DS 进程（dmesg 9 例，anon-rss≈2Gi 全部顶死限额，CONSTRAINT_MEMCG），玩家该局分配失败。已把 limits 临时抬到 12Gi 作为**量测用上界**止血；正式限额待完整跑通一局量出 `memory.peak` 后按 peak×1.3~1.5 确定。与同日 INC-20260727-001（allocator 单阈值误删 warming）是**独立根因**：本档的杀手是 kubelet/内核，另一档的杀手是 allocator sweep；两者曾在同一排查现场叠加出现。

## 1. 影响与范围

- 玩家影响：命中 OOM 的分配直接失败（DS 进程死亡→无心跳→按 abandoned 回收），玩家需重新匹配。
- 影响人数/对局/请求数：dev 单人测试；dmesg 记录 9 次 OOM kill。
- 服务影响：battle GameServer 被杀重建（fleet 补位），无跨局影响。
- 数据与安全影响：无（warming 期无已入账玩家数据）。
- 是否仍可复发：12Gi 量测围栏下未再观察到；**正式限额未定前不能宣布消除**（12Gi 是量测值不是设计值）。
- 严重级别判定理由：OOMKilled 属 index §1 强制建档范围；导致玩家进场失败。

## 2. 第一现场与证据

### 2.1 症状

- 服务端症状：DS 进程在 Artic01 加载中段消失，业务心跳从未启动。
- K8s 症状：容器 OOMKilled（内核 memcg），GameServer 随后不可用。

### 2.2 原始证据

- minikube 节点 `dmesg`：9 例 OOM kill，`anon-rss≈2Gi`、`CONSTRAINT_MEMCG`，进程为 DS 二进制。
- **证据缺口（明确标注缺失）**：dmesg 原始文本未归档、9 例的精确时间戳未记录、对应 Pod/GameServer 名称与 UID 未记录；minikube 节点重启后 dmesg 会丢失，**可能已不可回捞**。当时结论记录于 `deploy/k8s/agones/20-fleet-battle.yaml` limits 注释（同日写入，可信但非原始日志）。
- 参考量级：Artic01 Windows 编辑器加载峰值 ~13GB（含编辑器开销与 DDC，Linux cooked 无 DDC 构建应显著更小）。

### 2.3 已排除的噪声

- 与 allocator 删除 warming（INC-20260727-001）互为独立事件：OOM 例中进程被内核杀死在先；单阈值误删例中进程活到 BeginPlay、被 allocator 主动 DELETE。

## 3. 时间线

精确时间线缺失（见 §2.2 证据缺口）；已知顺序：旧 limits 2Gi 期间多次 OOM → 排查 INC-001 时从 dmesg 定位 → 同日把 limits 抬到 12Gi 量测围栏（当前集群已生效）。

## 4. 调用链与关键变量

| 变量/对象 | 位置 | 事故中的作用 |
|---|---|---|
| `resources.limits.memory: 2Gi`（旧值） | fleet yaml | Artic01 加载 anon 峰值即顶死该值 |
| `resources.limits.memory: 12Gi`（现值） | fleet yaml（集群已生效） | 量测用上界，非正式值 |
| `resources.requests.memory: 1Gi` | fleet yaml | 调度按 requests，不超卖节点；limits 超卖仅影响并发加载突发 |

## 5. 根因

### 5.1 直接根因

Artic01 服务端加载所需 anon 内存 > 2Gi 旧限额；memcg 硬限触发内核 OOM killer。证据：dmesg 9 例 CONSTRAINT_MEMCG、anon-rss≈2Gi 顶死（原始文本缺失，见 §2.2）。

### 5.2 触发条件

- 分配任何进入 Artic01 的对局（其余小图未复现）。

### 5.3 故障放大因素

- 大图内存量级从未量测过（此前所有图都在 2Gi 内），limits 是沿用值而非推导值。

### 5.4 为什么现有保护没有挡住

- OOM kill 无进程侧可拦截路径；allocator 侧的失败表现（心跳永不出现）当时又被 INC-001 的 15s 误删掩盖，两个故障互相遮蔽拖慢定位。

## 6. 全仓同类问题扫描

- Hub DS：城图常驻，长期运行在既有 limits 内，未见 OOM（dmesg 仅命中 battle DS 二进制）。
- 其余 Go 服务 limits 均远小于用量上界且有 metrics，不属同型。
- 未覆盖边界：未来新增大图（同 Artic01 量级）必须先量测再定 limits——已并入 A2 验收流程。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| limits 2Gi→12Gi（量测围栏） | 已生效（集群实测） | fleet yaml + `kubectl get fleet` | 节点 ~47Gi 可分配,约容 3 台并发加载;非正式值 |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| A1 完整加载量 `memory.peak`(cgroup v2) | **已完成**(2026-07-27 部署验证):最高 `memory.peak=11,200,929,792B≈10.43GiB`,12Gi 围栏下 **0 OOM**(仅一实例读数在案;逐实例分解未记录,标缺失) | — | 部署实测 |
| A2 按 peak×1.3~1.5 定正式 limits 并回调 yaml | **已 apply 生效**(2026-07-28 live 实测 limits=requests=14Gi、maxReplicas=2;当日 map8 真实客户端 E2E 一局进图+Admission 无 OOM——但该局未跑完整局,`memory.peak` 未读,不计入关闭回归):内存 limits=requests=**14Gi**(peak×1.34;CPU 仍 request<limit,QoS 为 Burstable);autoscaler `maxReplicas` 500→**2**(K8s 宣告 47Gi 但 minikube 外层 memory.max 实际 40Gi,3×14=42 已超;**"3 台并发"验收在外层扩容前不可执行,当前口径=2 台并发调度**) | `20/21-fleet-battle*.yaml`、`25-fleetautoscaler-battle.yaml` | 完整一局不 OOM + `memory.peak` 逐实例读数,OPEN |
| A3 资产减负后重测(与 INC-001 A5 同源) | 未排期 | — | — |

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| Artic01 完整加载不 OOM | 2Gi 下 9 例 OOM | 12Gi 围栏下 **0 OOM**,peak=10.43GiB(2026-07-27 部署验证;完整一局含真实客户端仍未跑,受 INC-001 验收门阻塞) | dev minikube | INC-001 §4.1 |
| 14Gi 正式值回归(一局不 OOM + 3 台并发调度) | — | 未执行 | 待 apply | OPEN |

## 9. 部署、回滚与观察

- 12Gi 已生效于当前集群（`kubectl get fleet` 实测）；正式值待 A2。
- 回滚条件：无（量测围栏无副作用；正式值确定后回调）。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A1 | P0-关闭门 | 量测 memory.peak（依赖 INC-20260727-001 部署链） | luhailong | 待执行 | INC-20260727-001 |
| A2 | P0-关闭门 | 定正式 limits 并回归验证 | luhailong | 待执行 | — |
| A3 | P2 | 归档/回捞 dmesg 原始证据；不可回捞则确认永久缺失 | luhailong | 待执行 | §2.2 |

## 11. 关闭审核

- [ ] 直接根因和放大因素均有证据（dmesg 原始文本待归档/确认缺失）
- [ ] 修复前失败、修复后通过的回归存在（完整一局量测未跑）
- [ ] 目标环境已加载可追溯的正式限额
- [ ] 观察窗口无复发
- [x] 文档已脱敏且时间线时区明确（缺失项已明示）

**关闭结论与审批人**：未关闭。
