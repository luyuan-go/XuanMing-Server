# 不停服更新(零停机)与玩家数据在线演进

> 状态:**已拍板·标准规范**。整理人:GitHub Copilot / 2026-07-01
> 关联规范:`CLAUDE.md` §5(proto)/ §9(不变量)、`AGENTS.md` §10(红线)
> 关联代码:[pkg/redisx](../../pkg/redisx)、[services/data](../../services/data)、[proto/](../../proto)
> 关联设计:[player-data-actor-serial.md](./player-data-actor-serial.md)、[proto-design.md](./proto-design.md)、[config-table-hotreload.md](./config-table-hotreload.md)

---

## 0. 结论先行

Pandora 后端**目标就是不停服更新**,分两层保障,缺一不可:

1. **服务层**:14+ 个 Go 服务全部 **headless、无状态、可水平扩展**,靠 k8s **滚动更新(rolling update)** 逐副本替换,任意时刻新旧版本副本**同时在线**,对客户端零停机。
2. **数据层**:玩家等业务数据以 **二进制 protobuf** 存 Redis(必要时 write-behind 落 MySQL blob/列)。proto **只做兼容演进**,新版本能读旧数据、旧版本能读新数据,因此**加字段/加玩家数据不需要停服、不需要全量迁移**。

只要守住下面的规则,「上线新版本」= 逐副本重启,「给玩家加数据」= proto 加字段,**都不停服**。

---

## 1. 服务层:滚动更新的前提

| 前提 | 要求 |
|---|---|
| 无状态 | 进程内不存跨请求的权威状态;权威态在 Redis/MySQL/etcd。内存只做缓存/Actor 邮箱,副本可随时被杀 |
| 优雅退出 | 收到 `SIGTERM` 先摘流量(healthz 转 not-serving)→ 排空在途请求 → flush write-behind 脏数据 → 退出;别让滚动更新丢未落库的写 |
| 幂等 | 客户端/上游可重试;重试不产生重复副作用(战斗结算、扣费走幂等键,见 §9 不变量 2/7) |
| 前后兼容 | 新旧版本副本同时在线期间,**RPC 协议**与**存储 pb**都必须双向兼容(见 §2) |
| 有状态单例例外 | Snowflake nodeID、锁、Actor 归属靠 etcd Lease / 分片路由重新分配,不靠进程常驻(见 `snowflake` / `cellroute`) |

滚动更新期间「新版本写了新字段 → 旧版本副本读到并回写」是常态,所以**兼容性是滚动更新的硬约束**,不是可选项。

---

## 2. 数据层:Redis 二进制 pb 的 schema 演进规则(硬规则)

### 2.1 只做兼容变更(允许,不停服)

- ✅ **新增字段**:分配新的、从未用过的 field number。proto3 缺省即零值,旧数据读出来新字段就是零值,代码按「老玩家没这份数据」处理即可。
- ✅ **新增 message / 新增 enum 值**:enum 必须有 `*_UNSPECIFIED = 0`,消费侧对未知 enum 值有 fallback。
- ✅ **删字段**:先 `reserved <num>;` + `reserved "<name>";` 并注释原因,**永不复用编号**(开发期未上线的字段可复用,但须重生 proto 并全量编译,见 §9 不变量 5)。

### 2.2 禁止的破坏性变更(会读坏线上数据 / 破坏不停服)

- ❌ **改 field number**(等于删旧字段+加新字段,旧数据错位)
- ❌ **改字段类型**(`int32↔int64↔uint`、`string↔bytes` 之外的换型;`sint/fixed` 与 `int` wire type 不兼容)
- ❌ **改 `optional/repeated/map/oneof` 的基数或成员归属**
- ❌ **改字段语义**(编号类型不变但含义变,新旧副本各理解一套)
- ❌ **复用已 `reserved` 的编号**

破坏性变更**只能**走「加新字段 + 双写 + 灰度 + 下线旧字段」的多版本迁移,不允许原地改。

### 2.3 未知字段必须保留(最易踩的坑)

滚动更新期间**旧版本副本会读到新版本写入、含新字段的数据**。protobuf 默认**保留 unknown fields**并在再次序列化时原样写回。因此:

- ❌ 在 **read-modify-write** 路径(读 Redis → 改 → 写回 Redis)上**禁止** `proto.UnmarshalOptions{DiscardUnknown:true}` / `protojson` 丢弃未知字段 —— 否则旧副本回写时会把新字段**丢掉,造成静默数据丢失**。
- ❌ **禁止"重建式回写"**:read-modify-write 必须**原地修改**反序列化出的同一个 message 再 Marshal 回写;禁止 `&XxxStorageRecord{...}` 从旧 message 拷贝已知字段**重建一个新 message** 后写回同一 key —— 新 message 不携带旧 message 的 unknown fields,效果等同 DiscardUnknown,旧副本回写照样丢新字段(2026-07-21 审计在领奖记录路径发现此模式,见 §7.3)。新建记录(key 尚不存在)和构造派生值对象(如清理目标身份)不在此限。
- ✅ 只读、不回写的路径(纯展示、只 Marshal 给客户端最小视图)才可视需要 DiscardUnknown。
- ✅ 面向客户端的视图本来就要按最小数据单位重新组装(§9 不变量 14),天然不回写存储,不受影响。

---

## 3. 给玩家「加数据」的标准流程(不停服)

1. 在对应 `<Domain>StorageRecord` proto message **加新字段**(新编号),`pwsh tools/scripts/proto_gen.ps1` 重生 go pb(cpp pb 同步 UE 仓库,见 §5)。
2. 新代码写:填新字段并写回;读旧数据时新字段为零值,按缺省语义处理(懒初始化)。
3. 滚动发布:k8s 逐副本替换。期间旧副本读到新字段会**原样保留回写**(§2.3),不丢数据。
4. 老玩家的旧数据**无需批量刷库**:下次被加载改写时自然带上新字段(**懒迁移 lazy migration**)。真需要统一初始化的,写一次性后台 backfill job,不停服跑。

## 4. 需要真正「数据版本迁移」时

绝大多数「加数据」靠 §3 的懒迁移即可,不需要版本号。只有当**同一字段语义要变**或**结构要重组**时,才引入迁移:

- 存储 record 可选带 `uint32 schema_version`(或 `data_version`),加载时 `if version < N { 迁移并回写; version = N }`,**单调递增防回退**。
- 迁移在**加载路径懒执行**,或用**不停服后台 backfill**,绝不停服全量刷。
- 迁移过程保证**幂等**(重复执行结果一致),配合玩家数据单写者串行(见 `player-data-actor-serial.md`)避免并发迁移竞争。

---

## 5. 验收红线(违反即 PR 拒绝 / 立刻停止报告)

- 改动导致**旧数据反序列化出错**或**旧副本回写丢字段** → 拒。
- 在 read-modify-write 路径丢弃 unknown fields → 拒。
- 改 field number / 改类型 / 复用 reserved 编号 → 拒(见 §2.2)。
- 新版本上线要求「先停服再启动」才能读数据 → 拒(违背本文核心目标)。
- 服务持有不可重建的进程内权威状态,导致副本不可随意重启 → 拒。

---

## 6. 流量切换与灰度发布(新流量怎么走到新副本)

> 2026-07-07 补充。回答「灰度升级时,新的流量应该往新的服务上去吧?」——对,核心机制就是
> **新副本 Ready 才接流量、旧副本摘除后不再接新请求、在途请求排空后退出**。

### 6.1 滚动更新的流量切换时序(k8s 默认路径)

```
发布新版本 image
  → k8s 起 1 个新 Pod(旧的还在跑,继续服务)
  → 新 Pod readinessProbe 通过 → 加入 Service Endpoints → 开始接【新】流量
  → k8s 给 1 个旧 Pod 发 SIGTERM,同时把它从 Endpoints 摘除 → 不再接新请求
  → 旧 Pod 优雅退出:healthz 转 not-serving → 排空在途 → flush write-behind → exit
  → 重复,直到所有副本换完;任意时刻都有 Ready 副本在服务
```

关键点:

- **Ready 门槛**:新 Pod 必须 readinessProbe(gRPC health check)通过才进 Endpoints;
  起不来/配置错的新版本**不会接到任何流量**,发布自动卡住,旧版本继续服务 —— 这本身就是
  最基本的一层灰度保护(`maxUnavailable=0, maxSurge=1` 时严格「先起新、再杀旧」)。
- **摘流量先于杀进程**:SIGTERM 与 Endpoints 摘除同时发生,所以优雅退出期间只需处理
  「在途请求」,不会有新请求进来(§1 优雅退出行)。

### 6.2 gRPC 长连接的坑:L4 均衡切不动,必须 L7

gRPC 是 HTTP/2 **长连接多路复用**:连接建立后所有请求走同一条连接。k8s Service(kube-proxy)
是 **L4 按连接**均衡 —— 老连接不断,新版本 Pod 就分不到流量,滚动更新会出现「新 Pod 空闲、
旧 Pod 迟迟排不空」。Pandora 的对策(缺一不可):

1. **k8s Service 用 headless**(`clusterIP: None`)+ 客户端侧(服务间 grpcclient / Envoy)做
   **per-request L7 均衡**:Envoy 对 upstream cluster 本来就是按请求选后端,天然支持。
2. **服务端设置 `MAX_CONNECTION_AGE`(grpc keepalive)**:让长连接定期(如 5~10min)优雅
   GOAWAY 重建,旧连接自然滚到新副本;这是兜底,保证任何客户端行为下连接都会轮换。
3. **push 的 server-stream 长流**:滚动更新时旧副本发 GOAWAY,客户端(UE FHttpModule 通道)
   按重连协议重新订阅到新副本;离线补发窗口(Redis ZSET 5min)覆盖重连间隙,不丢推送。
4. **入口层**:UE 玩家客户端只连 Envoy `:8443`；`:8444` 是隔离的 DS 回调面。Envoy 与后端之间按请求路由 —— 客户端
   连接不需要因后端发布而断开。

### 6.3 灰度(金丝雀)发布:比滚动更新多一层「按比例/按人群」

滚动更新解决「不停服」;**灰度解决「先让一小撮流量验证新版本」**。普通 Go Deployment 的
通用做法如下；Dedicated Server 因为一次分配会承载完整会话，采用后面的独立 Fleet 模型：

**全局发布硬约束**：所有 Go 服务与 Hub / Battle DS 都必须具备金丝雀发布能力，发布新服务器版本
不得停服、踢掉在场玩家或打断对局。Go 服务让 Stable / Canary 新旧副本兼容共存并按比例 / 人群
分流；DS 只把新分配送入已 Ready 的 Canary，旧 Battle DS 跑完当前对局，旧 Hub DS 停止接新并
迁移 / 排空玩家后才退役。任何回滚均先把 Canary 权重归零，保留仍承载会话的旧 DS 继续服务。

| 阶段 | 做法 | 载体 |
|---|---|---|
| 1. 按比例 | stable / canary 两个 Deployment(同一 Service label,副本数 9:1)→ 天然 ~10% 流量进新版本 | 纯 k8s,零新组件 |
| 2. 按权重 | Envoy weighted_clusters:stable 95% / canary 5%,权重热调 | 已有 Envoy,加路由配置 |
| 3. 按人群 | Envoy 按 header 分流(内部账号/白名单玩家带 `x-pandora-canary: 1` 进 canary)| Envoy header 路由 + login 侧标记 |
| 4. 观测与回滚 | 盯 canary 的 error rate / P99(prometheus 按 version label 分组);异常 → 权重归零即回滚,秒级 | 已有 metrics 体系 |

前提约束与 §1/§2 完全一致:**新旧版本必须双向兼容**(RPC + 存储 pb),因为灰度期间新旧
副本长时间共存 —— 这正是 §2 硬规则存在的原因,灰度只是把「共存窗口」从分钟拉长到小时/天。

有状态语义的注意点:同一玩家的请求可能一会儿打到 stable、一会儿打到 canary(除非按人群
粘住)。所以**行为变更类灰度必须按人群(阶段 3)**,按比例灰度只适合纯技术升级(依赖库/
性能优化/重构)。

#### 6.3.1 Agones DS 的 Stable/Canary（已接线）

DS 不通过同一个 Service 随机分流，而是四个互斥 Fleet：`pandora-battle-stable`、
`pandora-battle-canary`、`pandora-hub-stable`、`pandora-hub-canary`。Fleet、GameServer template、
Pod template 三层都必须带相同的 `pandora.dev/release-track=stable|canary`；allocator 同时用
`agones.dev/fleet` 和 release-track 选择，防止命名/标签漂移时跨轨误分配。

- Battle 用稳定 seed 对 `match_id` 做确定性 cohort，整场玩家必在同一轨；Hub 用同一 seed 对
  `player_id` 做 cohort，并把**实际命中轨**写入 assignment 保持粘性。Canary 明确无 Ready 容量时
  可回退 Stable，但 Stable cohort 不会反向进入 Canary；网络错误/响应不确定不能当“无容量”降级。
- `canary_seed` 是 cohort 身份的一部分。灰度权重非 0 时禁止换 seed；未显式传值时发布器只读继承
  集群现值且不打印。Battle/Hub 可以用不同百分比，但必须共用同一稳定 seed。
- Stable/Canary 共用同一个 DSTicket 公钥 keyset revision。`release_track` 被写入 DS 实例身份与
  DSTicket claims；普通灰度/回滚只换镜像 digest 和百分比，**不创建新私钥、不轮换公钥**。
- DSTicket 轮换只能由独立 `dsticket_rotate.ps1` 分 `stage/promote/retire` 执行。普通 online 发布与轮换
  通过同一个 create-only operation-lock 线性互斥；崩溃遗留锁 fail-closed，不按本机时间自动抢锁。
  K2 激活后的 K1 清退窗固定以 activation marker 的 apiserver `creationTimestamp` 计算，当前下限为
  `180s 最大票据 TTL + 15s leeway + 30s buffer = 225s`。轮换不删除旧密钥、不杀 Allocated DS；
  完成前必须证明四 signer、Login、四 Fleet、所有存活 DS 及其 controller owner 链处于对应阶段。

安全发布顺序（每一步均为独立、可审计的 `start.ps1 -Mode online` 运行）：

1. 先让发布器只读对账 immutable `Secret/pandora-dsticket-signer-r<revision>` 与 `default`/`pandora` 两份同 hash JWKS，
   并完成 callback Model-B fence、镜像 digest、旧单轨 Fleet 等硬门禁。
2. 显式提供 Canary DS 镜像，但保持 `BattleCanaryPercent=0` / `HubCanaryPercent=0`；Canary Fleet 先拉
   至指定 replicas，等待 Ready 池全部命中新 digest/track。这是预热，不会接新玩家。
3. 用同一镜像、同一 seed 再发布小比例权重；按 release-track 观察分配成功率、Ready 缓冲、DS 崩溃、
   PreLogin 拒绝、心跳/结算错误和 P99，确认后逐级放量。不要一次从 0 跳 100。
4. 晋升 Stable 是另一轮发布：先把 Canary 权重归零，再把 Stable 镜像钉到已验证 digest；发布器等待
   Stable Ready 池全量命中新 digest。旧 Allocated GameServer 不强删，按原镜像把在场会话跑完。

异常回滚顺序：**先把权重归零**，立即停止新分配进入 Canary；不要删除 Fleet、不要杀 Allocated
GameServer、不要换 keyset。确认 Canary 已无新分配后，可把 Canary `replicas` 设为 0 清 Ready 池；
旧 Allocated Hub/Battle 继续排空。

**残留 Allocated 实例的清理（2026-08-03 设计复议，替代原「运维结合在线证据人工清理」条款）**：

- 旧版本条款要求「最终由运维结合对局/Hub 在线人数证据清理」。该路径已被证伪并废除：
  「无人」的反证责任在删除方，而人工可得的证据（日志窗口 grep、近段鉴权记录）受
  `--since` 窗口起点、容器日志轮转、级别静默三重失真，**永远构不成「无人」的证明**——
  2026-08-03 正是按此流程误删了载人 battle DS（INC-20260803-002，玩家被踢）。
- 现行规则：**人工与脚本一律不得直接 delete Allocated GameServer**（§9.21）。残留分两类：
  - **有权威分配记录引用**的 Allocated 实例：等对局/在线自然结束，由既有判弃链
    （心跳超时 abandoned → fenced release）与 Agones 生命周期回收；
  - **无任何权威记录引用**的孤儿实例：由 ds_allocator sweep 的孤儿对账清扫自动回收
    （`biz/orphan_gameserver.go`）——它能做到人工做不到的机械证明：无记录 ⇒ 签不出
    DSTicket ⇒ 不可能再有玩家进来（票据硬上限 180s）；「不可能已在内」由心跳停机契约
    （无记录心跳 → `commandStop` / DS 失联自我 fencing，§9.22）闭合。删除走服务端复核
    + UID+resourceVersion 双 precondition，连续观察超阈值（默认 10m）才执行；且必须由
    allocation 权威台账（`data/allocation_ledger.go`）证明该 GS 出身本权威——读到空/
    错配 Redis 的进程台账必空、一台都删不掉，防「权威视图分裂下机械化全量误删」。
- 「本仓库无法机械证明玩家已经全部离开」对**有记录引用**的实例（尤其旧 Hub Fleet）仍然
  成立，脚本仍不会自动删除旧 Hub Fleet；孤儿对账清扫只覆盖「无记录」这一可证明子集。

上述普通灰度回滚不得与独立 DSTicket 轮换交叉执行。若发现
`pandora/ConfigMap/pandora-dsticket-operation-lock` 遗留，只能先审计 holder、operation、UID、相关
immutable marker 与实际 controller/Pod 状态；不得直接依赖时间删除，也不得为了继续发布绕过门禁。

### 6.4 澄清:dev compose 的 restart 策略与本文无关

2026-07-07 把 `deploy/docker-compose.services.yml` 业务容器 `restart` 从 `unless-stopped`
改为 `"no"`(根治重启电脑后旧容器复活抢 500xx 端口、劫持 Envoy → k8s 流量的问题)。
**这不影响不停服更新**:

- `restart:` 只管 Docker daemon 的**崩溃自愈 / 开机自启**,不提供任何升级能力;
  docker compose 本身没有滚动更新(`compose up` 重建容器 = 单实例先停后起,必断)。
- 灰度/滚动更新的载体自始至终是 **k8s Deployment**(生产形态);compose 只是本地 dev
  联调便利环境。k8s 模式下 Pod `restartPolicy: Always` 照常自愈,不受影响。

### 6.5 现状差距清单(2026-07-08 审计;升多副本前必须补齐)

§6.1~§6.2 描述的是目标机制;对照当前代码/部署,**已兑现**与**未兑现**如下。
单副本 dev 不受影响,但**扩多副本 / 启用真滚动更新前必须先补齐未兑现项**,否则会
出现「新 Pod 没 listen 就接流量」「长连接粘死旧副本」。

| 机制 | 现状 | 状态 |
|---|---|---|
| SIGTERM 优雅退出 | Kratos `app.Run()` 默认拦 SIGTERM → GracefulStop | ✅ 已兑现 |
| Envoy 入口 per-request 路由 | Envoy upstream cluster 天然按请求选后端 | ✅ 已兑现 |
| readinessProbe 才进 Endpoints | 2026-07-08 已落地:`services.yaml` 20 个 Deployment 全部加原生 gRPC readinessProbe(k8s ≥1.24;Kratos 默认注册 grpc_health_v1,Stop 时自动转 NOT_SERVING) | ✅ 已兑现 |
| `MAX_CONNECTION_AGE` 连接轮换 | 2026-07-08 已落地:`pkg/config` 加 `max_conn_age`/`max_conn_age_grace`,`pkg/grpcserver` 按配置挂 `keepalive.ServerParameters`;20 个服务 dev yaml 全量开 15m(grace 默认 30s;ds_allocator 显式 90s 盖过 AllocateBattle 同步等 DS ready 的 ~60s);不配(零值)= 关 | ✅ 已兑现 |
| 服务间 L7 均衡 | Service 全是普通 ClusterIP,服务间 `grpcclient.MustDial` 直连 DNS 名 → kube-proxy L4 按连接 | ❌ 待补:Service 改 headless + client 走 dns resolver per-request 均衡(grpcclient 已有 WRR selector 底座),或服务间也过 Envoy。MaxConnAge 15m 已作为兜底(多副本时连接最迟 15m 重平衡) |
| RollingUpdate 策略显式化 | Deployment 未写 `strategy`(k8s 默认 25%/25%) | ⚠️ 建议:关键服务显式 `maxUnavailable=0, maxSurge=1` |
| Go 服务金丝雀(§6.3 通用四阶段) | 未搭 stable/canary 双 Deployment / Envoy weighted_clusters | ❌ 发布硬要求，正式上线新版本前必须补齐；不再是按需项 |
| Agones DS 金丝雀(§6.3.1) | Battle/Hub 已拆 Stable/Canary 四 Fleet，allocator 按确定性 cohort 选轨，online 发布器具备预热、权重归零、digest/track/Ready 门禁 | ✅ 已接线；仍须真实集群完成 mTLS、Model-B 激活与端到端证据后才可生产 Apply |

补齐顺序建议:~~readinessProbe → MAX_CONNECTION_AGE~~(已完成)→ headless/L7 →(需要时)金丝雀。

---

## 7. 金丝雀共存窗口「旧副本回写丢新字段」问题与全仓审计(2026-07-21)

### 7.1 问题定义

金丝雀 / 滚动更新期间,**同一玩家的请求会在新旧副本之间跳**(按比例分流时无人群粘性,§6.3
已说明):

```
T1 玩家请求打到旧副本 —— 旧副本二进制里没有新字段
T2 玩家请求打到新副本 —— 新副本写入了新字段(例如 proto 新编号字段)
T3 玩家请求又打到旧副本 —— 旧副本读出数据、修改、写回
     ⚠️ 若此路径丢弃 unknown fields 或重建 message,T2 写入的新字段被静默清掉
```

这不是理论风险:共存窗口在金丝雀模式下会持续**小时~天**级(§6.3),窗口内每一次
read-modify-write 都是一次丢字段机会。丢失是**静默**的——不报错、不留日志,只有玩家
数据"倒退"后才暴露。

### 7.2 按存储类别的防护结论(2026-07-21 代码审计)

| 存储类别 | 代表 | 旧副本回写行为 | 结论 |
|---|---|---|---|
| MySQL 结构化列 | players / 属性 / 装备 / 天赋 / 经验 / 库存 / 好友 / 公会 / 交易 / 拍卖 | `UPDATE ... SET` 只写**自己认识的列**;全仓无 `REPLACE INTO`、无"读整行→重写整行" | ✅ 安全:旧副本语句里根本没有新列,碰不到也清不掉 |
| data_service PlayerData(MySQL) | proto2mysql | `update_mask` 掩码列写,旧副本只写掩码内的列 | ✅ 安全 |
| data_service PlayerData(Redis 缓存) | `cache.go`(`PDC\x02` 格式) | 缓存值带**写入方字段号位图**;读取只信任「写入方字段集 ⊇ 本副本字段集」,旧副本写的残缺条目对新副本视为 miss 回落 MySQL | ✅ 安全:就是针对本场景设计的防投毒机制 |
| Redis proto blob RMW | team(`UpdateWithLock`)/ hub 权威记录(`hub_authoritative.go` / `hub_auth_repo.go`) | 默认 `proto.Unmarshal`(保留 unknown fields)+ **原地修改**读出的 message 后回写 | ✅ 安全:unknown fields 随 Marshal 原样带回 |
| 只读 DiscardUnknown | `pkg/configtable` 运行时加载 | 配置表由生成器整表重发,非玩家数据、纯只读不回写 | ✅ 合规(§2.3 允许) |
| 一次性写入 | 库存鉴定属性 JSON(写一次不再改)/ push 离线 ZSET(append-only)/ 经验推送出箱 | 无 read-modify-write | ✅ 不适用 |

**登录链路本身**(账号、selected_role_id、session 短时令牌)全部是 MySQL 结构化列或短 TTL
令牌,不涉及 blob 回写,不受本问题影响。

### 7.3 已修复:领奖记录「重建式回写」(2026-07-21 当日修复)

[services/account/player/internal/biz/reward.go](../../services/account/player/internal/biz/reward.go)
的 `saveRewardRecord` 原实现:

```go
// loadRewardRecord: proto.Unmarshal(raw, stored) 后只取走两张 map,stored 被丢弃
// saveRewardRecord: 用 Snapshot 的两张 map 重建全新 message 回写 ↓
raw, err := proto.Marshal(&playerv1.RewardClaimStorageRecord{Permanent: perm, Activity: act})
```

- **当时没有实际丢失**:`RewardClaimStorageRecord` 只有 `permanent` / `activity` 两个字段,
  新旧副本认知一致;但这是绕过 unknown fields 保护的模式——将来加任何新字段,金丝雀窗口内
  旧副本处理一次 `ClaimReward` 就会静默清掉它,乐观锁 version 挡不住(旧副本是"合法"写入)。
- **已修复**:`loadRewardRecord` 保留并返回 `stored` message;`saveRewardRecord` 原地
  `stored.Permanent, stored.Activity = rec.Snapshot()` 后 `proto.Marshal(stored)`,
  unknown fields 随 Marshal 原样带回。
- **回归测试**:`TestClaimReward_PreservesUnknownFields`(reward_test.go)——给存量记录挂上
  本副本不认识的 raw field(field 15 varint),ClaimReward 后断言 unknown fields 原样保留
  且业务位图正确;修复前该测试失败。
- §2.3 已增补「禁止重建式回写」为硬规则,后续 review 按此执行。

### 7.4 全仓落盘点排查(2026-07-21,与 §7.3 同批)

按「`proto.Marshal` 非测试落盘点 → 是否回写既有记录 → 回写对象来源」三步全量分类,
除 §7.3 外**未发现第二处重建式回写**:

| 服务 / 路径 | 模式 | 结论 |
|---|---|---|
| team `UpdateWithLock` / trade `UpdateWithLock` | unmarshal → 闭包**原地改同一对象** → marshal 回写 | ✅ |
| ds_allocator(battle / battle_auth / abort / lifecycle)、hub_allocator(assignment / auth / authoritative)、matchmaker(ticket / match / start_operation) | 全线 `proto.Clone(loaded)` + 原地改;`Clone` 保留 unknown fields | ✅ |
| hub_allocator capacity ledger(`writeHubCapacityLedger` 整表 Del+HSet 重写) | ledger map 里存的就是 `loadHubCapacityLedger` 反序列化出的**原对象**,改动原地或经 Clone;新条目才新建 | ✅ |
| mail 内容 record | `CreateMail` 建一次后不再改;领取 / 已读状态在独立 MySQL 表,读路径只 unmarshal 不回写 | ✅ |
| guild 信息缓存 / data_service PlayerData 缓存 | 均带「写入方字段号位图 + 超集判定」投毒防护(魔数 `PGC\x01` / `PDC\x02` 同款机制) | ✅ |
| friend / chat / dialogue / auction / leaderboard / player_locator / inventory | 无 proto blob 落盘(MySQL 结构化行 + Redis 索引 / ZSET),列写只触碰已知列 | ✅ 天然免疫 |
| Kafka 事件 payload / push 帧 / outbox 行 | 一次性新建写,无 read-modify-write | ✅ 不适用 |

### 7.5 数据兼容不能替代路由粘性

本节防护解决的是**数据不丢**;若新字段承载**行为语义变更**(新旧副本对同一数据做不同决策),
仍必须按 §6.3 用人群粘性(header 分流)把玩家粘在单一 release track 上。两层防护各管一半,
不能互相替代。
