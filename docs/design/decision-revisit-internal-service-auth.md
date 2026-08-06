# 内部服务身份与 Inventory 系统 RPC 授权复议

> 状态：**方案 A 已获人明确批准；独立静态候选已交付，普通 online 发布路径未接线、未激活**（2026-07-13）
> 范围：Inventory 的 7 个系统写 RPC，以及后续内部系统 RPC 的统一身份基础。
> 关联规范：`CLAUDE.md` §9 不变量 7/8/16、`AGENTS.md` §7/§10、
> `gateway-decision.md` §16、`zero-downtime-update.md`。
> 2026-07-13 人已明确批准本文方案 A。该批准确定架构方向，但不等于已安装/激活 Istio，也不授权
> 写 secret、修改生产集群或放宽现有安全门。共享 identity、Inventory 与 DS-terminal 清单仅为静态候选，
> 默认 online kustomization、`start.ps1` 与普通 online NetworkPolicy 均未接入，不能由普通发布误激活。

## 1. 为什么必须复议

Inventory 当前同时承载玩家自助 RPC 与高权限系统写 RPC。玩家身份由边缘 Envoy 验证 SessionToken
后写入 `x-pandora-player-id`，Inventory 使用 `pmw.AuthOptional()` 读取；但系统 RPC 的现有判定是：

```text
有 player_id  → 玩家调用，拒绝
无 player_id  → 内部系统调用，放行
```

也就是说，`callerID == 0` 表示的只是“请求没有玩家身份”，并不能证明请求来自 auction、trade、mail、
leaderboard 或 battle-result。任何能连到 Inventory `:20015` 的匿名内部调用者，只要不携带
`x-pandora-player-id`，就会被当成系统服务。当前 online NetworkPolicy 的 `allow-app-mesh` 又允许业务 Pod
之间全通，因此单个低权限业务 Pod 被攻破后，可以直接伪造受害玩家和订单参数调用资产写接口。

最小复现序列：

1. 攻击者取得任一可访问 `inventory.pandora.svc:20015` 的业务 Pod 执行权限；
2. 直接建立明文 gRPC 连接，不携带玩家身份；
3. 调用 `EnsureAuctionEscrow` / `FreezeForOrder`，在请求体填入受害玩家、订单、物品和数量；
4. Inventory 看到 `callerID == 0` 后放行，在业务校验满足时冻结受害玩家资产。

同一问题覆盖全部 7 个系统 RPC，不是只修 `EnsureAuctionEscrow` 一处即可。相关入口：

- `services/economy/inventory/internal/server/grpc.go`
- `services/economy/inventory/internal/service/inventory.go`
- `pkg/middleware/auth.go`
- `pkg/grpcclient/grpcclient.go`

当前仓库清单与本地 `pandora-agones` 集群还存在以下事实：

- inventory、auction、trade、mail、leaderboard、battle-result 均使用 `default` ServiceAccount，无法形成
  可区分的 SPIFFE workload identity；
- 尚无 Istio / Linkerd CRD 与 sidecar；
- 五个真实调用方全部通过 `grpcclient.MustDialInsecure` 直连，只传播 trace，不传播服务身份；
- `deploy/envoy/envoy.yaml` 本轮已把 7 个 Inventory 系统方法全部 exact-path 403（含
  `EnsureAuctionEscrow`），并有静态契约锁定顺序；这只收紧客户端边缘纵深防御，不能证明东西向调用方身份，
  因而不消除本文漏洞；
- 生产客户端边缘 Envoy 不在本仓库中，采用 SPIFFE 后必须为它规定唯一、可审计的 workload principal。

## 2. 信任域边界

内部服务身份必须独立于以下既有信任域，密钥、issuer、audience 和用途均不得复用：

- 玩家 SessionToken；
- 玩家进入 Hub/Battle 的 DSTicket；
- DS → 后端回调凭据；
- 人工 GM / 运维凭据。

以下信息都**不能单独作为系统身份**：

- `callerID == 0`、请求体里的 `player_id` 或任意自报 header；
- Pod IP、源端口、DNS 名、同命名空间、NetworkPolicy 可达性；
- 玩家 JWT、DSTicket、DS callback token；
- 所有调用方共用的 HMAC secret。

NetworkPolicy 只能继续作为可达性收敛和纵深防御，不能替代工作负载身份与方法授权。

## 3. 7 方法 × 5 principal 精确矩阵

当前全仓真实调用方已穷举如下。`—` 表示必须拒绝；不得为了“以后可能调用”预授权。

| Inventory 系统 RPC | `pandora-auction` | `pandora-trade` | `pandora-mail` | `pandora-leaderboard` | `pandora-battle-result` |
|---|:---:|:---:|:---:|:---:|:---:|
| `GrantItems` | — | — | 允许 | 允许 | — |
| `GrantInstances` | — | — | 允许 | — | 允许 |
| `FreezeForOrder` | 允许 | — | — | — | — |
| `EnsureAuctionEscrow` | 允许 | — | — | — | — |
| `SettleAuctionMatch` | 允许 | — | — | — | — |
| `SettlePlayerTrade` | — | 允许 | — | — | — |
| `ReleaseEscrow` | 允许 | — | — | — | — |

对应真实调用点：

- auction：`services/economy/auction/internal/data/settlement_client.go`
- trade：`services/economy/trade/internal/data/settlement_client.go`
- mail：`services/social/mail/internal/data/inventory_client.go`
- leaderboard：`services/runtime/leaderboard/internal/data/reward_client.go`
- battle-result：`services/battle/battle_result/internal/data/inventory_client.go`

未来若 player 奖励领取或新服务需要调用 Inventory，必须先补设计、真实调用链和测试，再只增加对应
`principal × method` 单元格；禁止给某个 principal 放整个 `InventoryService/*` 前缀。

## 4. 方案 A：Istio STRICT mTLS + SPIFFE + 方法白名单（推荐）

### 4.1 最终形态

采用 revision 化 Istio sidecar mesh：

1. 为 inventory、五个调用方和生产客户端边缘 Envoy 分配互不复用的 Kubernetes ServiceAccount；
2. sidecar 为每个工作负载签发包含 SPIFFE SAN 的短期证书，自动完成双向 TLS、调用方身份和服务端身份；
3. Inventory workload 使用 `PeerAuthentication` 的 `STRICT` 模式；
4. Inventory 使用 `AuthorizationPolicy action: ALLOW`，按 §3 的 principal 和完整 gRPC path 精确授权；
5. 其它 principal、匿名明文连接、错误方法均在到达 Go handler 前 fail-closed；
6. Go 业务容器继续使用明文 h2c，TLS、证书轮换和授权全部由 sidecar 承担，符合
   `gateway-decision.md` §16 的既定方向。

建议身份名：

```text
spiffe://cluster.local/ns/pandora/sa/pandora-inventory
spiffe://cluster.local/ns/pandora/sa/pandora-auction
spiffe://cluster.local/ns/pandora/sa/pandora-trade
spiffe://cluster.local/ns/pandora/sa/pandora-mail
spiffe://cluster.local/ns/pandora/sa/pandora-leaderboard
spiffe://cluster.local/ns/pandora/sa/pandora-battle-result
```

生产客户端边缘 Envoy 必须使用独立 principal，例如
`spiffe://cluster.local/ns/pandora-ingress/sa/pandora-edge-envoy`。该 principal 只允许六个玩家 RPC：

- `GetInventory`
- `UseItem`
- `SellItem`
- `IdentifyItem`
- `DiscardInstance`
- `MoveInstance`

边缘 Envoy 不得获得 §3 任一系统方法；DS 面 `pandora-envoy` 也不是 Inventory 调用方，不得因名称相近
而误授权。

### 4.2 K8s / Envoy 硬约束

- Inventory Service `20015` 端口必须显式标记 `name: grpc` 和 `appProtocol: grpc`，避免 L7 方法规则因
  协议识别失败退化成 TCP 规则；
- gRPC 的 HTTP method 固定为 `POST`，path 必须是完整
  `/pandora.inventory.v1.InventoryService/<Method>`，禁止 service 前缀和通配符；
- namespace / workload 必须绑定明确 Istio revision，injection webhook 必须 `failurePolicy: Fail`；
- 禁止目标 workload 使用 `sidecar.istio.io/inject: "false"`，禁止排除 Inventory `20015` 入站或五个
  调用方到 Inventory 的出站；
- 发布门必须检查目标 Pod 的 ServiceAccount、`istio-proxy`、revision、STRICT 和完整白名单；仅检查
  YAML 中有 annotation 不够；
- custom 边缘 Envoy 可排除其公网 `8443`、DS `8444` 和 admin `9901` 的 sidecar 入站劫持，但其访问
  Inventory 的出站必须进入 mesh；需在测试环境反证不会产生双 Envoy 循环；
- 当前原生 gRPC readinessProbe 必须经过所选 Istio revision 的 probe rewrite 实测，不能为绕过探针
  问题而排除 `20015`；
- 能创建/更新 Pod 或 Deployment 的主体等价于能选择高权限 ServiceAccount，CD/RBAC 必须禁止普通
  workload 冒用 `pandora-auction` 等身份；
- mesh 注入不可只靠约定：admission 与 online 发布门都必须 fail-closed，缺 sidecar 的 Inventory Pod
  不得被创建或加入流量；
- 证书根和控制面按 revision 重叠轮换，不能原地切 CA 导致新旧 sidecar 互不信任。

### 4.3 优点与代价

优点：

- 同时提供调用方身份、服务端身份和链路机密性；
- 不把密钥和 TLS 生命周期压进六个 Go 服务；
- 方法授权集中可审计，未来可逐服务收敛现有 `allow-app-mesh`；
- 与 DS `:8444` 已登记的生产 mTLS 阻断项复用同一基础设施，不重复建设。

代价 / 风险：

- 需要人批准安装和维护 mesh 控制面、CA、sidecar 容量与升级流程；
- 当前生产客户端边缘 Envoy 不在仓库内，必须先纳入同一 trust domain 并确定唯一 principal；
- sidecar 注入、原生 gRPC probe、自定义 Envoy 与 mesh 的组合必须真集群验证；
- mesh-only 授权的安全边界在 sidecar。若发布系统允许无 sidecar 的 Inventory 裸跑，应用仍会接受
  匿名直连，因此 admission、RBAC 和 online 激活门是方案组成部分，不是可选优化。

## 5. 方案 B：独立 per-service RS256 JWT（备选）

仅在明确不批准现阶段引入 mesh 时采用。它必须是新的 `pandora-service-auth` 信任域，不能复用现有
Session / DSTicket / DS callback 代码配置或密钥。

### 5.1 最低安全规格

- 五个调用方各持有**独立** RS256 私钥；Inventory 只挂公钥 keyset；
- 每个 `kid` 在 Inventory 配置中固定映射到一个 service principal，不能只相信 token 自报的 `sub`；
- 每次 RPC 单独签 token，TTL 不超过 30 秒；
- 强制 claims：`iss`、`aud=pandora-inventory`、`sub`、`iat`、`nbf`、`exp`、`jti`、完整 `rpc`、
  `request_sha256`；
- `request_sha256` 对 deterministic protobuf bytes 计算，使被窃取 token 只能重放完全相同的请求，
  不能在有效期内替换 player/order/item/quantity；
- 服务端权限仍由 §3 固定映射裁决，token 中的 `rpc` 只是绑定项，不能自行扩权；
- 系统 metadata header 必须与玩家 Authorization 分域，边缘 Envoy 入站先无条件剥离；
- online 配置必须 enforce + fail-closed。观察模式只能用于迁移阶段，不能成为长期可绕过开关；
- 禁止所有服务共享一个 HS256 secret；否则任一服务泄露即可冒充 auction 调全部资产方法；
- 7 个方法已有业务幂等键，完全相同请求的重放应由业务幂等消化。若另建 JTI replay cache，必须有
  到期清理和硬容量上限，满载 fail-closed，遵守 `CLAUDE.md` §9 不变量 18。

### 5.2 需要修改的范围

- 新增独立 service-auth signer/verifier 与 server/client middleware；
- Inventory 按 `transport.Operation()` 执行 §3 方法授权；
- auction、trade、mail、leaderboard、battle-result 五个 client 全量接入签名；
- 五套 revisioned immutable 私钥 Secret、Inventory 公钥 keyset、K1/K2 轮换脚本和发布契约；
- 所有 dev/cluster 配置、生成脚本、K8s 挂载、权限、测试与文档同步；
- 客户端 Envoy 7 个系统方法 direct 403 契约持续保留（`EnsureAuctionEscrow` 已补齐）。

### 5.3 主要风险

- JWT 本身不提供链路机密性和服务端身份；当前东西向明文时，token 与业务参数仍可被观察；
- 若不绑定请求摘要，窃取的 Bearer 可在 TTL 内改写受害玩家和资产参数；
- 五套私钥、keyset 和不停服轮换会复制当前 DSTicket 已暴露的运维复杂度；
- 最终仍需为 DS Bearer、玩家票据和 GM 命令交付 mTLS，因此会形成“两套工作负载身份系统”；
- signer/verifier、时钟偏差、kid 映射或 permissive 配置出错会直接中断奖励、交易和拍卖补偿链。

## 6. 方案比较与推荐

| 维度 | 方案 A：STRICT mTLS/SPIFFE | 方案 B：per-service RS256 JWT |
|---|---|---|
| 调用方身份 | SPIFFE SAN 绑定 ServiceAccount | `kid → principal` 固定映射 |
| 链路机密性 | 有 | 无 |
| 服务端身份 | 有 | 无 |
| 方法授权 | sidecar L7 AuthorizationPolicy | Inventory middleware |
| Go 业务改动 | 基本无 | Inventory + 5 调用服务 |
| 轮换 | mesh 自动短证书 + revision CA | 5 套私钥与公钥 keyset |
| host/compose 开发 | online 专用；dev 明确非生产 | 可覆盖，但需开发密钥/配置 |
| 与既有架构 | 完全一致 | 临时替代，最终仍需 mTLS |

**推荐方案 A。** Pandora 已决定东西向加密与身份下沉基础设施层，且 DS `:8444` 已把 STRICT mTLS
登记为生产 Apply 前置。为 Inventory 单独再建应用 JWT，不能消除该前置，只会增加第二套密钥和轮换面。

## 7. 零停机迁移与回滚

### 7.1 方案 A 迁移顺序

每一步均为独立、可审计的发布，不允许把 prepare 与 enforce 压成一次不可观测切换：

1. **身份准备**：创建独立 ServiceAccount，给 inventory、五个调用方和生产边缘 Envoy 设置
   `serviceAccountName`；暂不启用拒绝策略。按 `maxUnavailable=0,maxSurge=1` 滚动并确认全部 Ready；
2. **mesh prepare**：安装/选择 revision 化 Istio 与 CA，sidecar 先以 `PERMISSIVE` 进入相关 workload；
   应用协议仍是明文 h2c，业务代码不变；
3. **dry-run 授权**：以 dry-run 部署 §3 精确策略，跑 mail 领奖、榜单发奖、战斗掉落、trade 结算、
   auction 冻结/补冻/结算/释放，确认所有真实 source principal，无未知调用方；
4. **清旧连接**：滚动五个调用方和边缘 Envoy，或至少等待现有 `max_conn_age=15m` 加 grace，确保旧明文
   HTTP/2 连接退出；
5. **先授权后 STRICT**：激活精确 ALLOW 策略，再将 Inventory workload 切到 `STRICT`；确认匿名、错误
   principal、错误方法全部被 sidecar 拒绝；
6. **永久发布门**：online 发布器在任何业务资源写入前，机械检查 mesh CRD/revision、admission、
   ServiceAccount、live sidecar、STRICT、完整白名单和边缘 principal；任一不满足即停止；
7. **扩大范围**：Inventory 验收完成后，才按相同模型处理其它系统 RPC，不在本轮顺手给全服务宽授权。

回滚规则：

- 只允许回滚到上一份已验证的精确 AuthorizationPolicy 和上一 mesh revision；
- 始终保持 STRICT，不得用 `PERMISSIVE` / `off` / 匿名放行救火；
- CA 轮换必须有新旧根重叠信任窗口；
- ServiceAccount 名保持稳定，不随应用版本回滚；
- 若新应用版本有问题，可在 mesh 身份与白名单不变的前提下回滚镜像；
- 如果授权配置误拦截，应修正/恢复上一份白名单，不得恢复 `callerID == 0` 网络信任。

### 7.2 方案 B 迁移顺序

1. Inventory 先发布能验 RS256 token、记录缺失/错误指标但暂不拒绝的兼容版本；
2. 先发布 K1 公钥 keyset，再逐个发布五个带 K1 signer 的调用方；
3. 覆盖所有前台与后台真实链路，确认无匿名系统调用；
4. Inventory 切 enforce，完成后删除长期 permissive 入口；
5. 轮换走 `K1 verifier → K1+K2 verifier → K2 signer → 等待最大 TTL → K2 verifier`；
6. 回滚只能恢复上一版仍 enforce 的 verifier/keyset，不能回到匿名放行版本。

RPC message 不变，service-auth 仅增加 metadata，因此新旧应用可在准备阶段共存；但 enforce 必须等五个
调用方全部完成接线后才启用。

## 8. 验收与 mutant 测试

### 8.1 静态 / 契约验收

- Kustomize 渲染后，相关 workload 均使用独立 ServiceAccount，不得出现 `default`；
- Inventory 只有一个 STRICT PeerAuthentication，selector 精确命中 `app=inventory`；
- AuthorizationPolicy 与 §3 矩阵逐单元格完全相等，不多不少；
- Inventory gRPC Service 端口名和 `appProtocol` 正确；
- 禁止 wildcard principal、service 前缀、`*` path、匿名系统方法和 `20015` mesh bypass；
- 客户端 Envoy 七个系统方法全部 direct 403，包含 `EnsureAuctionEscrow`；
- online 发布门位于第一次远端写之前，失败必须 fail-closed；
- 生产边缘 Envoy 的 principal、namespace、SA 与 live Endpoint 可机械核对。

### 8.2 真集群行为验收

- 每个合法 principal 调矩阵内方法成功；
- 同一合法 principal 调矩阵外方法返回 gRPC `PERMISSION_DENIED`，Inventory handler 不产生业务副作用；
- `default` SA、任意其它业务 SA、无 sidecar/plaintext Pod 均不能调用 7 个系统方法；
- 玩家 SessionToken、DSTicket、DS callback token、伪造 `x-pandora-*` header 均不能冒充系统身份；
- 边缘 Envoy 可调六个玩家 RPC，但调任一系统 RPC 必须被拒；
- readiness、gRPC health、指标抓取在 STRICT 下持续正常；
- mail、leaderboard、battle-result、trade、auction 前台和后台补偿链均执行成功；
- 两副本持续流量下滚动 SA、sidecar、policy、Inventory 镜像和 mesh revision，无 `Unavailable`、无重复
  资产变化、无漏发/漏结算；
- CA 重叠轮换期间旧连接、新连接和重拨均正常；
- 拒绝日志含 source principal / operation / trace_id，不记录 token、证书私钥或业务 secret。

### 8.3 必须失败的 mutants

至少机械注入并证明以下变体失败：

1. auction / trade / mail / leaderboard / battle-result 任一改回 `default` SA；
2. 删除 Inventory STRICT 或改成 PERMISSIVE；
3. 删除 Inventory / 调用方 sidecar 注入，或设置 `inject=false`；
4. 排除 Inventory `20015` 入站，或排除调用方到 Inventory 的出站；
5. 把某条完整 gRPC path 改成 service 前缀 / 通配符；
6. 给 auction 增加 `GrantItems`、给 mail 增加 `SettleAuctionMatch` 等越权单元格；
7. 漏掉 §3 某条合法边，证明 dry-run/真实链路能发现而不是静默丢业务；
8. 生产边缘 Envoy 使用 `default` SA、无 sidecar或不在同一 trust domain；
9. 删除 `EnsureAuctionEscrow` 的客户端 Envoy direct 403；
10. readiness probe rewrite 失效；禁止通过排除 `20015` 让 mutant 变绿；
11. injection webhook 不可用时仍创建出无 sidecar Inventory Pod；
12. online preflight 失败后脚本仍发生第一次远端写。

若选择方案 B，另加：错 iss/aud/kid/alg/sub、未知 key、过期/未来/超长 TTL、重复 header、方法不匹配、
请求摘要不匹配、请求体一字节变异、K1/K2 断档、共享私钥、空 keyset、permissive online 配置等 mutants。

## 9. 批准记录与当前交付边界

2026-07-13，人明确选择并批准**方案 A**：revision 化 Istio `STRICT mTLS + SPIFFE + exact
AuthorizationPolicy` 是 Pandora 内部系统 RPC 的统一最终身份层，Inventory 为第一条落地链。
方案 B 不再作为本轮实现路径；仍禁止共享 secret、IP/NetworkPolicy、来源 header、`callerID == 0`
或“只把 `EnsureAuctionEscrow` 藏在 Envoy 后面”等替代。

仓库已完成的独立静态候选包括：六个内部 workload 的独立 SA 与纯 revision 注入、Inventory
`grpc/appProtocol`、9 个系统 allow + 26 个 system deny 补集、edge 6 个玩家 allow + 7 个 system deny
补集、STRICT/PERMISSIVE 分层清单、Inventory 专用 NetworkPolicy、内部与 edge 的
Deployment→ReplicaSet→Pod 三层 VAP、完整 live labels/owner/sidecar/token/control-plane 审计，以及
candidate 写前 exact document hash。VAP 使用稳定 `admissionregistration.k8s.io/v1`，生产最低要求
**Kubernetes 1.30**；不提供未验证的 beta 兼容分支。Istio revision 由发布参数显式提供，禁止
`default/latest`、sentinel 与 `inject=true` 混用。共享静态 component 唯一拥有
`pandora-battle-result` ServiceAccount；Inventory 与 DS-terminal 候选组合时只引入一次，DS-terminal
自行声明两端 revision patch，不依赖 Inventory identity component。

本轮没有安装 Istio、没有执行 Kubernetes apply、没有写真实 secret，也没有跑真实 Kubernetes/外部
edge 的分阶段激活。默认 online kustomization 不引用 shared identity/Inventory/DS-terminal component，`start.ps1`
不加载 Inventory helper、不接收 Istio revision、不生成运行时 patch，也不执行 Inventory preflight；
普通 online NetworkPolicy 保持原样。旧 `pandora.inventory-mesh-audit/v1` 可伪造/重放，已永久 hard-fail。
未来重新接线前必须先完成以下 v2 证据、自动激活状态机与真实验收，并重新接受代码审核：

1. `observeEvidence`：明确为 dry-run shadow decision，精确枚举 9+26+6+7，绑定短时窗、context/API
   endpoint、namespace UID、requested+actual revision、webhook UID/RV、policy UID/generation/RV、
   live Deployment generation、Pod UID/imageID；
2. `activeAllowEvidence`：active ALLOW 新 generation/RV 传播后的独立实际证据，逐 proxy 证明 xDS 已接受，
   并重新跑合法/非法反证；不得复用 observe JSON 或用单一 `workflows_ok` 布尔冒充；
3. 在共享 DSTicket 操作锁内临写前复证上述 live 身份与证据，再按 PERMISSIVE→identity→gate→observe→
   active ALLOW→STRICT 分开写入；final NetworkPolicy 不得早于 edge/allow 传播；
4. 真实 Kubernetes 验收 sidecar injection、CNI/NetworkPolicy、外部 edge、probe rewrite、metrics merge、
   五条业务补偿链与滚动零停机。原生 sidecar restartable-init 模式本轮不支持；仅接受普通
   `istio-proxy` 容器与零个/唯一 `istio-init` 或 `istio-validation`。

普通 online 当前另有独立的 DSTicket K1→K2→K2-only 真实 Kubernetes/UE E2E 零写验收边界；该边界
不代表 Inventory 已接线或已完成 Istio 激活。本轮 post-redaction K1 尚未到达认证入口，K2 未执行。
