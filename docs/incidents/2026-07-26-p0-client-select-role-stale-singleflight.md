# [INC-20260726-003][P0] SelectRole 旧请求门闩阻塞权威重进选角

> **状态**：已修复待验证（未关闭）  
> **类型**：`availability` / `client-state` / `near-miss`  
> **环境**：上线前静态审计；未在生产发生  
> **首次发生时间（UTC）**：不适用（上线前 near-miss）  
> **首次发现时间（UTC）**：2026-07-26 13:00:00  
> **负责人**：待指定  
> **受影响组件**：Pandora UE 客户端 `UMyAccountModel`，SVN r1502  
> **最后更新**：2026-07-26

## 0. 一句话结论

SelectRole 发出后若 Coordinator 因其它权威事件换代，旧回调会按 token 正确丢弃；但本地 `bSelectRoleInFlight` 不会释放。服务端随后明确返回 `ROLE_REQUIRED` 并重新打开选角界面时，玩家每次确认都会被旧门闩吞掉，不再发送 RPC，形成永久卡玩家。修复是在 `ROLE_REQUIRED` 当前权威结论处释放已失效旧请求的门闩；UE 尚未编译和运行验收，因此事故未关闭。

## 1. 确定性触发路径

```text
RequestEnterHubWithRole
  -> bSelectRoleInFlight = true
  -> SelectRoleScoped(token=A)

其它权威事件
  -> Coordinator generation/request 换代，A 失效
  -> 新 ResumeContext = ROLE_REQUIRED
  -> HandleAuthoritativeRoleRequired 重新打开选角

旧 SelectRole(A) 回调迟到
  -> IsCallbackCurrent(A) == false
  -> return，不清 bSelectRoleInFlight

玩家再次确认
  -> bSelectRoleInFlight == true
  -> return true，但没有发出新 SelectRole RPC
```

原实现只有当前有效回调、`ReturnToLogin` 或对象销毁会清门；权威换代后重新进入选角不属于这三种路径，因此会一直卡到玩家主动返回登录或重启。

## 2. 根因与修复

- 直接根因：single-flight 布尔门闩的生命周期长于其所属 Coordinator token；token 已失效，门闩仍被当作“请求在飞”。
- 放大因素：迟到回调正确地不得修改新代次状态，所以不能依赖它清理旧门闩。
- 修复线性化点：`HandleAuthoritativeRoleRequired` 是当前权威代次明确要求重新选角的结论；到达该点即可证明此前 SelectRole 已失效，安全释放门闩。
- 防误清：旧回调仍先过 token 门并直接丢弃；新请求在 `ROLE_REQUIRED` 之后才可能建立，旧回调不会清掉它。

改动：

```text
UMyAccountModel::HandleAuthoritativeRoleRequired
  -> bSelectRoleInFlight = false
  -> 重新打开权威选角
```

未引入新 timer、状态机或兼容层。

## 3. 验证矩阵

| 验证 | 当前状态 | 关闭要求 |
|---|---|---|
| 静态交错：A 失效 -> ROLE_REQUIRED -> A 迟到 -> 再次确认 | 代码路径已收口 | UE 自动化/手工路径确认第二次确认确实发送 RPC |
| 旧 A 回调不得影响新 B | token 门仍在，静态成立 | 自动化注入 A/B 回调乱序 |
| UE 全量编译 | **未执行（按用户要求）** | 编译通过 |
| 双端集成 | 未执行 | Login/SelectRole/ResumeContext 真实链路 PASS |
| 目标产物与观察窗 | 未部署 | 可追溯产物 + 无复发 |

## 4. 部署与关闭

- 服务端 Git commit：无，本轮未提交。
- 客户端 SVN：r1502 是外部并发提交；本次修复在其上形成新的本地修改，尚未提交。
- 部署产物：无。
- 回滚：未部署；若需回滚，仅撤销本次单行门闩释放及注释，不影响 wire。
- 关闭条件：UE 编译、确定性乱序验证、真实进场路径与目标产物观察全部通过。

**关闭结论与审批人**：未关闭。
