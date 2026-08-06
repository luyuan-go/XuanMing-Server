# 本地 Agones dev 联调（minikube + Agones）

> W4 ⑬（2026-06-09）。把 ds_allocator / hub_allocator 从 mock 地址推进到本地 Agones 联调。
>
> ⚠️ **AGENTS.md §3 / §11.1**：本目录里所有「安装工具 / 起 minikube / helm 装 Agones / 拉镜像 /
> 启重服务」的命令**由 Codex / 用户执行**，Claude 只负责写清单 + 风险 + 验收标准。
> apply 业务 manifest（Fleet / RBAC）属本地 dev 集群操作，也由 Codex 执行。

---

## 🚀 真 DS 闭环·快速开始（无 mock，线上等价）

想跑「登录 → 大厅 Hub DS → 匹配进战斗 DS → 结算回大厅」的**真 DS 全链路**（用真 UE Linux DS 包，
不是 mock 假地址），一条命令：

```powershell
# 起 minikube + 装 Agones + apply RBAC/16-ds-envoy/Fleet/FleetAutoscaler + 部署 20 个后端服务(allocator=agones)
# 末尾 [8/8] 会自动跑 e2e_k8s.ps1(宿主 Envoy 桥接 + UDP 回程中继 + 验收清单),无需再手动跑
pwsh tools/scripts/start.ps1 -Mode k8s
```

只有宿主桥接/中继单独坏了(如手动杀了 port-forward)才需单跑修复：

```powershell
# 当前 allocator 向局域网客户端广播非回环 IP 时，必须显式开放宿主入口：
pwsh tools/scripts/e2e_k8s.ps1 -SkipImageLoad -MinikubeProfile pandora-agones -KubeContext pandora-agones -RelayBindHost 0.0.0.0
```

`e2e_k8s.ps1` 会先读取 live `Secret/pandora-config` 中 DS/Hub allocator 的
`agones.advertise_host`。两者漂移、地址不属于本机，或非回环广播地址与 relay 监听范围不匹配时，
脚本会在改动 Fleet、镜像、Envoy 或旧 relay 前停止；局域网开放必须显式传 `-RelayBindHost 0.0.0.0`。

`e2e_k8s.ps1` 自动完成：校验集群/Agones/Fleet 就绪 → 从 Fleet yaml 解析真 DS 镜像精确 tag 并
`minikube image load` → 起宿主 Envoy 桥接(`k8s_envoy_bridge.ps1`：对各业务 k8s Service 做
`kubectl port-forward` + 拉起 docker envoy `:8443`/`:8444`) → 轮询等
`pandora-battle-stable` / `pandora-hub-stable` Ready，并确认两个 Canary Fleet 以 0 Ready 休眠 →
docker driver 下拉起容器版 UDP 回程中继 → 打印端到端验收清单与
实时观察命令。常用开关：`-NoRelay`（自己起中继）、`-SkipImageLoad`（镜像已 load）、
`-TimeoutSec`（等 Fleet 超时）。

> **DS 回调为什么能通**：k8s 模式下 DS(Pod)走**集群内 Envoy「DS 面」网关**
> `pandora-envoy.pandora.svc.cluster.local:8444`(由 `16-ds-envoy.yaml` 部署,线上同款拓扑),
> 它把 grpc-web 转成 gRPC 后直连 `ds-allocator.pandora.svc:20020` 等集群内 Service。
> Pod 内不再写 `host.docker.internal`(minikube 的 Pod 解析不了该域名,DNS 层就失败)。
> 宿主 Envoy `:8443` 服务 UE 玩家客户端，宿主 `:8444` 仅保留回环调试面；`k8s_envoy_bridge.ps1`
> 的 port-forward 是给宿主 Envoy upstream 用的，与 GameServer DS 回调无关。

> **为什么不是 `-Mode docker`**：docker-compose 里 ds_allocator 跑在 Linux 容器内,既不能 exec
> Windows DS、又没有 Agones 可调,代码只有 local/agones/mock 三种 provider,故 docker 只能落 mock。
> 要真 DS 用 `-Mode k8s`(本机 Agones,线上等价)或 `-Mode local`(本机直接 exec Windows DS)。
>
> **前置**:真 UE Linux DS 镜像须先由 UE 侧打包到 `deploy/ds/stage/LinuxServer`。本地可用
> `deploy/ds/build-image-minikube.ps1` 直接构建到 minikube 内置 Docker daemon（然后跑
> `e2e_k8s.ps1 -SkipImageLoad`），也可用 `deploy/ds/build-image.sh` 在宿主构建
> `pandora/battle-ds:dev` / `pandora/hub-ds:dev` 后让 `e2e_k8s.ps1` 执行 `minikube image load`。
> 注意 `:dev` 只是本地构建链的中间 tag;**Fleet yaml 引用的是不可变 tag**,不要把 `:dev` 写回 Fleet。

详细环境准备 / 手测分配 / 心跳 stub 见下文各节。

---

## 🔁 电脑重启后 / 一键重置

**电脑重启后**(minikube 容器被停、宿主 go 进程/UDP 中继都没了,但集群状态和已 load 的镜像都还在磁盘上):

```powershell
pwsh tools/scripts/start.ps1 -Mode k8s -Resume   # minikube start + 等 Pod 恢复 + 自动重建宿主桥接/中继(末尾自动跑 e2e_k8s.ps1)
```

`-Resume` 是**快路径**:只 `minikube start`(集群/镜像都在磁盘,Pod 自动重建)再等关键 Pod 就绪,
**不重新 build/load 20 个镜像**,几十秒就回到上次状态。其它模式同理:

| 模式 | `-Resume` 做什么 |
|---|---|
| `k8s` | `minikube start` + 等 login/ds-allocator/hub-allocator 就绪 |
| `docker` / `intranet` | `docker compose up -d`(不加 `--build`,不重建镜像) |
| `local` | 基础设施容器随 Docker 自动恢复 + 重启宿主 go 服务 |
| `online` | 不适用(远端集群 Pod 自管) |

**环境乱了 / 想从头来**(`-Resume` 报错说找不到镜像或 Fleet,多半是之前 `minikube delete` 过):

```powershell
pwsh tools/scripts/start.ps1 -Mode k8s -Reset    # minikube delete 后全新部署(会重建+重 load 镜像,较慢;末尾同样自动跑 e2e_k8s.ps1)
```

`-Reset` = 彻底清掉旧状态再全新起(k8s 会 `minikube delete`;docker 会 `down -v` 清卷)。
线上 `online` 模式**禁用** `-Reset`(不对生产/测试集群做销毁式重置)。

> 经验法则:**正常重启用 `-Resume`(快),状态损坏才用 `-Reset`(慢但干净)。**

### 旧本地 etcd / 单轨 Fleet 的一次性迁移门禁

现在本地 etcd 用 `PVC/etcd-data` 持久化 Model-B 的 `required_writer_epoch` 与 capability，Deployment
固定 `strategy: Recreate`，避免单节点 RWO 数据盘同时挂给两个 Pod。`start.ps1` 在任何集群写入前检查：

- 现有 etcd 若仍是旧版非 PVC 布署，普通启动和 `-Resume` 都会 fail-closed。要保留现场，必须先停写、
  做并校验 etcd snapshot，restore 到 PVC 后，再核对 required epoch 和五类 writer capability；脚本不会
  把 missing 偷当成可自动初始化。不要直接 apply 新 Deployment 覆盖旧 Pod，否则会丢授权基线。
- 普通一键启动不再只看“本次运行前 profile 是否存在”。首次创建 namespace 时把 fresh anchor
  作为同一个 Namespace API 对象的 annotation 原子落盘，随后转成 create-only immutable intent，
  并绑定 profile、namespace UID 与随机 continuity token；同一宿主/profile 的 baseline 由跨进程命名
  Mutex 串行化（进程崩溃后可由下一次双击接管）。`preinfra/adopting` 只有在完整 authority prefix 为空时
  才能把同一 token create-only 写进 etcd 数据盘，紧邻 prepare 还会重读 marker state/token；精确回读
  sentinel 后才绑定 PVC UID 并推进 `pending`。`pending` 以后只读 sentinel、绝不重建，数据盘原地清空或
  替换会在任何 writer/CAS 前 fail-closed。只有 `pending` 才允许 missing→精确 V3 的单事务 CAS；CAS 同时
  比较 sentinel 先于 canonical genesis record、activation lock 以及除 lock 外完整 authority prefix 为空。
  CAS 前 evidence sha/time 已持久化，崩溃重跑会精确核对同一份 genesis record，verify 后才推进
  `complete`。因此首次下载/启动任一步中断后，再双击同一入口可安全续跑；`complete + missing` 一律按
  权威数据丢失阻断。
- 兼容旧启动器刚留下“仅 canonical 基础设施 + Bound PVC、但没有 marker”的半成品现场，仅开放一条
  **local-only 短时同批 cohort heuristic**：在本轮任何 apply 前，必须把 namespace、PVC/绑定 PV、
  mysql/redis/zookeeper/kafka/etcd 五个 Deployment 及各自唯一 current ReplicaSet/Ready Pod 共 18 个对象
  按 UID/owner/creationTimestamp 做 fingerprint；所有对象距同一采集时刻不得超过 2 小时、全体创建跨度
  不得超过 10 分钟。PV 还必须精确满足 `claimRef.uid=PVC UID`、`storageClass=standard`、
  `pv.kubernetes.io/provisioned-by=k8s.io/minikube-hostpath`、`reclaimPolicy=Delete`。baseline 确认 missing 后，
  apply 前即使 Pod 还在拉镜像，也只放宽动态 Ready status，UID/owner/spec/revision/generation 仍须完整；
  infra rollout 后会沿用同一采集时刻严格重算 Ready 并要求 fingerprint 完全相同，再两次只读证明 etcd
  `revision=1`、整个 `/pandora/ds-auth/` 前缀为空且全局没有 Pandora/Agones writer；create-only adopted marker
  会在同一次创建
  中原子绑定 `adopting + PVC UID + cohort fingerprint + continuity token`，随后按 sentinel→`pending`→CAS
  到 V3。这个 heuristic 只为兼容截图所示的旧本地启动器短时中断现场，**不能数学证明宿主目录/卷内容的
  历史连续性**，因此不能用于远端/生产恢复。
  V1/V2、超时或跨批对象、未知 workload、任一 Fleet/GameServer/GameServerSet/FleetAutoscaler/
  GameServerAllocation、发生过写入的 etcd、PV/PVC/owner/UID 漂移都会 fail-closed。最终线性化点由 Go CAS
  对 exact sentinel、required、目标 record、activation lock 与除 lock 外完整 DS-auth authority prefix 的原子
  compare 保证；revision/prefix 读和 cohort 都是收养历史门禁，不冒充 CAS 条件。已有精确 V3 即使没有
  marker 也只走只读验证，并建立绑定 namespace/PVC UID 的 immutable observed witness 后再次线性复查；以后
  witness + missing 会按数据丢失阻断，不再重新进入 cohort 收养。`-Resume` 不从 missing bootstrap；若精确
  回读到同一份 V3 evidence、marker 仅差最后
  一步，则只补 `pending→complete` 元数据后再启动 writers。现场不需保留时可显式 `-Reset`。
- 若发现旧 `pandora-battle` / `pandora-hub` 单轨 Fleet，本地要求 `-Reset`，线上则要求人工 drain +
  显式删除后重跑。四轨 Fleet 与旧单轨 Fleet 不允许静默共存，尤其不会自动删除可能仍有玩家的 Hub。

---

## ☁️ 线上真集群部署(online:测试服 / 生产 kbs)

线上 Fleet 跟本地有一处**必须换掉**、一处**默认已对齐**:
  1. DS 镜像:本地钉的是**不可变 tag**(制品库快照版本号,如 `pandora/battle-ds:r1833-dirty-20260805-222737`,镜像只在你机器上/制品库里),远端要换成 registry 可拉取的完整镜像名 —— 但两端都**不准用 `:dev` 滚动 tag**(§9.21 / INC-20260727-001 P1-4)
  2. DS 回调地址:Fleet 默认已是线上同款 `pandora-envoy.pandora.svc.cluster.local:8444`(本地由 `16-ds-envoy.yaml` 提供同名 Service,线上由边缘网关提供);若线上网关 DNS 不同,用 `-DsGatewayAddr` 覆盖

### 玩家 → GameServer 公网 UDP 硬门

线上玩家面**不经过**本地 `pandora-udp-relay`。`-Mode online` 默认不生成
`agones.advertise_host`，Battle / Hub allocator 直接返回 Agones
`status.address:status.ports[0].port`；四条 Fleet 当前均为 `portPolicy: Dynamic` + UDP。

因此本地这次“allocator 下发局域网 IP、relay 却只监听回环”的原样故障不会自动带到 online，
但以下任一配置仍会产生完全相同的玩家症状（登录成功、拿到 DS 地址、UDP 握手零回包）：

- Agones 上报的是 Pod IP、ClusterIP 或仅 VPC 可达的 Node 内网 IP；
- 云安全组、宿主防火墙没有放行实际动态 UDP 端口；
- 公网 NAT / LB 将端口映射到错误节点，或端口重写后与 allocator 返回值不一致；
- 人工给 online Secret 添加统一 `advertise_host`，却没有为每个动态端口证明到 exact GameServer 的路由。

上线前必须同时满足：

1. 回读所有可接流量的 Stable / Canary GameServer，确认 address、动态 port、Node、节点池、track 与 UID 完整且属于批准的公网路由方案；
2. 回读云安全组 / 节点防火墙 / NAT 或 LB 的实际规则，证明 `address:port → 所在 Node hostPort`，不能只看清单；
3. 从集群外、与玩家同路径对当前每个可接新玩家的 GameServer 做真实 UDP 握手；至少覆盖每个 distinct 公网 address、Node / 节点池、放量 track 和实际动态 port，并在 exact GameServer 日志中看到对应握手 / `PreLogin`；
4. 自动扩容产生的新 Node / 节点池或 GameServer 在接收玩家前重复等价外部验证；无法验证的容量保持不可分配；
5. 将 address、port、GameServer name/UID、Node、track、时间与结果作为发布证据；任一项 UNKNOWN / 超时即阻断，且不得记录 DSTicket 或密钥。

> 当前 `start.ps1 -Mode online` **尚未自动实现**云规则回读或集群外 UDP 探针；证据须由外部平台 /
> 运维提供，不能因为本节写了“硬门”就视为脚本已经 PASS。现有 online 独立零写阻断仍不得删除。

Fleet Ready、Agones SDK health、DS→allocator 心跳以及集群内 UDP 探针均**不能替代第 3 项**。完整勾选项见
[`docs/ops/release-checklist.md`](../../../docs/ops/release-checklist.md) §2.3。

所以 `-Mode online` **强制要求**镜像/网关、DSTicket keyset revision、Model-B fence 与环境对应的
kube-context 映射（缺一即 fail-fast，不会把测试部署误打到生产）。下面展示权重 0 的 Canary 预热
形态；当前生产 mTLS、真 UE DSTicket v2 E2E、registry immutable policy 三道硬门未解除时，脚本会在
push/apply 前正确停止，禁止注释门禁强行上线：

```powershell
# 测试服集群(-Env test)
pwsh tools/scripts/start.ps1 -Mode online -Env test -TestKubeContext pandora-test `
  -Registry registry.mycorp.com -Tag v1.2.3-b5a5a95 `
  -BattleDsImage registry.mycorp.com/pandora/battle-ds@sha256:<stable-battle-digest> `
  -HubDsImage registry.mycorp.com/pandora/hub-ds@sha256:<stable-hub-digest> `
  -CanaryBattleDsImage registry.mycorp.com/pandora/battle-ds@sha256:<canary-battle-digest> `
  -CanaryHubDsImage registry.mycorp.com/pandora/hub-ds@sha256:<canary-hub-digest> `
  -BattleCanaryPercent 0 -HubCanaryPercent 0 `
  -BattleCanaryReplicas 2 -HubCanaryReplicas 1 -CanarySeed ds-release-20260713 `
  -DsGatewayAddr pandora-envoy.pandora.svc:8444 -DsAuthMode enforce -DsAuthorityMode redis `
  -DsFenceEtcdEndpoints <mtls-etcd-endpoint> -DsFenceKeysetRevision <callback-revision> `
  -DsTicketKeysetRevision 1

# 生产 kbs 集群(-Env prod,会要求二次输入 kube-context + 大写 PROD 确认)
pwsh tools/scripts/start.ps1 -Mode online -Env prod -ProdKubeContext pandora-prod `
  -Registry registry.mycorp.com -Tag v1.2.3-b5a5a95 `
  -BattleDsImage registry.mycorp.com/pandora/battle-ds@sha256:<stable-battle-digest> `
  -HubDsImage registry.mycorp.com/pandora/hub-ds@sha256:<stable-hub-digest> `
  -DsGatewayAddr pandora-envoy.pandora.svc:8444 -DsAuthMode enforce -DsAuthorityMode redis `
  -DsFenceEtcdEndpoints <mtls-etcd-endpoint> -DsFenceKeysetRevision <callback-revision> `
  -DsTicketKeysetRevision 1
```

| 参数 | 作用 | 默认 |
|---|---|---|
| `-Registry` / `-Tag` | 20 个 Go 服务镜像来源；tag 必须含 git SHA，运行时再解析并固定 digest | 必填 |
| `-TestKubeContext` / `-ProdKubeContext` | 将 `-Env test/prod` 绑定到各自允许的 context；也可用 `PANDORA_K8S_TEST_CONTEXT/PROD_CONTEXT` | 对应环境必填 |
| `-BattleDsImage` / `-HubDsImage` | Stable DS 镜像；发布器从 registry 回读并固定 `repo@sha256:digest` | 必填 |
| `-CanaryBattleDsImage` / `-CanaryHubDsImage` | Canary 独立镜像；启用对应权重时必填，digest 必须与 Stable 不同 | 不启用时空 |
| `-BattleCanaryPercent` / `-HubCanaryPercent` | allocator 确定性 cohort 权重；0 可先预热 Canary | `0` |
| `-BattleCanaryReplicas` / `-HubCanaryReplicas` | 显式 Canary 镜像的预热池容量 | `1` |
| `-BattleMaxReplicas` | Battle FleetAutoscaler 同时最大局数护栏;设为节点池上限对应容量,真弹性由集群 Cluster Autoscaler 加节点提供 | `0`(用 yaml 本地值 500) |
| `-BattleBufferSize` | Battle Ready 预热量;大规模建议百分比(如 `10%`,随在跑局数自动放大) | 空(用 yaml 本地值 2) |
| `-CanarySeed` | Battle 按 match_id、Hub 按 player_id 的稳定 cohort seed；权重非 0 时禁止更换 | 权重>0 必填 |
| `-DsGatewayAddr` | 覆写四个 Fleet 的 DS 回调地址 env → 线上网关实际 DNS | 必填 |
| `-DsGatewayTls` | 改写 `PANDORA_DS_ALLOCATOR_TLS`；同集群 DS 面明文，只有集群外 TLS 边缘才设 `1` | `0` |
| `-DsAuthorityMode redis` / `-DsAuthMode enforce` | Model-B callback authority；与玩家 DSTicket keyset 独立 | online 固定 |
| `-DsFenceEtcdEndpoints` / `-DsFenceKeysetRevision` | callback required epoch 的 etcd 与身份 revision | 必填 |
| `-DsTicketKeysetRevision` | 已 bootstrap 的 immutable 玩家 DSTicket public keyset revision | 必填 |
| `-BuildPush` | 本地构建并 push 20 个 Go 服务镜像到 `-Registry`(发布动作,需人工授权) | 关 |

> Fleet 改写是**在临时文件里做再 apply**：Battle/Hub 的 Stable/Canary 四份 Fleet 仓库原文保持本地
> dev 值，git 不会脏。线上 Agones 须由集群管理员预装（脚本只 apply 业务 RBAC/Fleet，不 helm install）。
> 权重归零只停止新分配进入 Canary；旧 Allocated DS 保留到会话排空，不随 Ready 池缩容被强删。
>
> DS 镜像本身仍由 UE 侧 `deploy/ds/build-image.sh` 构建后,由人手动 `docker push` 到 registry
> (与 Go 服务镜像分开,脚本不替你 push DS 镜像)。
>
> 线上 DS 崩溃、`kubectl logs --previous`、Release 符号归档、Prometheus/Grafana 指标与
> profiler 排查见 [`docs/ops/linux-ds-observability.md`](../../../docs/ops/linux-ds-observability.md)。

---

## 0. 两种 DS 模型（先理解再联调）

| | 战斗 DS（ds_allocator） | 大厅 Hub DS（hub_allocator） |
|---|---|---|
| Agones 模型 | **按需分配** GameServerAllocation | **常驻分片** LIST GameServer |
| Fleet | `pandora-battle-stable` / `pandora-battle-canary` | `pandora-hub-stable` / `pandora-hub-canary`（带 `pandora.dev/region` 标签）|
| 触发 | matchmaker 全员确认 → `AllocateBattle` | login → `AssignHub`（lazy-seed 分片到 Redis）|
| 容量判定 | 一对局一个 GameServer | hub_allocator 自己在 Redis 维护 `player_count`（500/实例）|
| 后端代码 | `data/agones_allocator.go`（W4 ⑫）| `biz/agones_fleet.go`（W4 ⑬）|

两者都**不引入 agones/client-go 重依赖**，用标准库 `net/http` 直连 k8s apiserver REST，
provider 无关（minikube / 自建 / ACK 一致），所以 **D7（k8s 选型）不卡此代码，只卡真集群联调**。

---

## 1. 环境准备命令（Codex 执行）

> 假设本机已装 Docker Desktop。命令按 Windows PowerShell 给出，必要处标注。

```powershell
# 1.1 装 minikube + kubectl + helm（如未装）
winget install Kubernetes.minikube
winget install Kubernetes.kubectl
winget install Helm.Helm

# 1.2 起 minikube（Docker driver，给足资源跑 Agones + 几个 GameServer）
# Windows / 国内网络下必须禁用 preload，否则容易卡在 Google preload tarball 下载；
# kicbase 使用已验证可拉取的阿里云镜像。
& 'C:\Program Files\Kubernetes\Minikube\minikube.exe' start `
  --driver=docker `
  --cpus=4 `
  --memory=6144 `
  --kubernetes-version=v1.30.0 `
  --base-image=registry.cn-hangzhou.aliyuncs.com/google_containers/kicbase:v0.0.50 `
  --preload=false `
  --cache-images=false `
  --interactive=false

# 1.3 装 Agones（官方 helm chart，装到 agones-system 命名空间）
helm repo add agones https://agones.dev/chart/stable
helm repo update
kubectl create namespace agones-system
helm install agones agones/agones --namespace agones-system --wait

# 1.4 校验 Agones controller 起来了
kubectl get pods -n agones-system           # agones-controller / agones-allocator 应 Running
kubectl get crd | Select-String agones      # 看到 fleets/gameservers/gameserverallocations
```

## 2. apply Pandora manifest（Codex 执行）

```powershell
# 先建 pandora 命名空间(RBAC 的 ServiceAccount 建在 pandora ns,没它 apply 会失败),
# 再 RBAC → DS 面 Envoy 网关(本地 minikube 必需,DS 回调 :8444 靠它) → Fleet
$ctx = 'pandora-agones' # 改成你的本地 minikube context；所有变更显式钉住，不依赖 current-context
kubectl --context $ctx apply -f deploy/k8s/services/00-namespace.yaml
# 手工链路先 create-only 投递 dev K1；15-dsticket-jwks.yaml 只是 fail-closed 模板，禁止直接 apply。
# -Generate 仅在 run/cluster/dsticket 尚不存在时执行；脚本刻意拒绝覆盖已有材料。
pwsh tools/scripts/dsticket_keyset.ps1 -KeyDir run/cluster/dsticket -Revision 1 -Generate
pwsh tools/scripts/dsticket_keyset.ps1 -KeyDir run/cluster/dsticket -Revision 1 -Apply -KubeContext $ctx
kubectl --context $ctx apply -f deploy/k8s/agones/10-rbac-allocator.yaml
kubectl --context $ctx apply -f deploy/k8s/agones/16-ds-envoy.yaml
kubectl --context $ctx apply -f deploy/k8s/agones/20-fleet-battle.yaml
kubectl --context $ctx apply -f deploy/k8s/agones/21-fleet-battle-canary.yaml
kubectl --context $ctx apply -f deploy/k8s/agones/30-fleet-hub.yaml
kubectl --context $ctx apply -f deploy/k8s/agones/31-fleet-hub-canary.yaml

# 等真 Pandora DS Fleet 全部 Ready（镜像内已接 Agones SDK）
kubectl get fleet
kubectl get gameservers -L agones.dev/fleet,pandora.dev/region,pandora.dev/release-track
# 期望:两个 Stable Fleet 按 20/30 yaml 达到 Ready；两个 Canary Fleet 默认 replicas=0、Ready=0。
```

## 3. 让本机 allocator 连 minikube apiserver

> ⚠️ **提交规范**：两个 allocator 的 `*-dev.yaml` 基线一律保持 `mode: local`、`agones.enabled: false`,
> 且 `api_server` / `token_path` 用通用 in-cluster 默认值。**不要把本机 minikube 的临时 apiserver 地址、
> token 路径提交进仓库**。本地切到 Agones 链路靠 `start.ps1 -Mode k8s`(脚本生成 cluster 配置),
> 见 `tools/scripts/gen_cluster_config.ps1` 的 `-AllocatorMode agones`。

allocator 当前是**本机进程**（docker-compose dev 之外单独跑），需要把它指向 minikube
apiserver + 提供 `pandora-allocator` ServiceAccount 的 token。

```powershell
# 3.1 拿 minikube apiserver 地址
$apiServer = (kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
# 3.2 给 ServiceAccount 签一个短期 token（k8s >=1.24;SA 建在 pandora ns,见 10-rbac-allocator.yaml）
$token = (kubectl create token pandora-allocator -n pandora --duration=24h)
# 3.3 写到本机文件供 allocator 读(token_path 指向它)
Set-Content -Path "$env:TEMP\pandora-allocator.token" -Value $token -NoNewline
```

然后**临时**改两个 allocator 的 dev yaml 的 `agones` 段（**只在本机改，验完即还原，勿提交**）：

```yaml
# ds_allocator-dev.yaml / hub_allocator-dev.yaml
agones:
  enabled: true
  api_server: "<上面的 $apiServer>"        # 形如 https://127.0.0.1:xxxxx
  namespace: "default"
  fleet_name: "pandora-battle-stable"      # hub 填 "pandora-hub-stable"
  canary_fleet_name: "pandora-battle-canary" # hub 填 "pandora-hub-canary"
  canary_percent: 0                         # >0 时必须提供稳定 canary_seed
  canary_seed: ""
  token_path: "C:\\Users\\<you>\\AppData\\Local\\Temp\\pandora-allocator.token"
  insecure_skip_tls_verify: true           # minikube 自签证书,dev 临时开;或填 ca_path
  # ca_path: "<minikube ca.crt 路径>"      # 与 insecure_skip_tls_verify 二选一
  advertise_host: "127.0.0.1"              # docker-driver 必填,见 §3.1;真集群留空用 status.address
```

> 也可用 `kubectl proxy --port=8001` 起本地代理，然后 `api_server: http://127.0.0.1:8001`
> + `token_path: "-"`（不带 token，proxy 复用 kubeconfig 凭证），免去 token/TLS 配置。

### 3.1 Windows 客户端 → Linux DS 回程中继（docker driver 必读）

minikube 用 **docker driver** 时，GameServer Pod 的 `status.address` 是集群内网 IP，Windows
客户端**直连不到**。解决办法两段：

1. allocator 侧把返回地址改写为客户端可达的宿主地址。`start.ps1 -Mode k8s` 的优先级是
   `-AdvertiseHost` → `PANDORA_DS_ADVERTISE_HOST` → 自动解析局域网 IPv4；都不可用才回退
   `127.0.0.1`（仅本机客户端）。
2. 本机起 UDP 中继，把宿主 `<advertise_host>:<port>` 转发到 minikube 节点同端口。**docker driver 下推荐用
   `e2e_k8s.ps1` 自动起的容器版中继**（`--network pandora-agones`，直连 minikube 节点 IP）；
   只在调试时才用进程版 `udp_relay.ps1`：

```powershell
# 容器版(e2e_k8s.ps1 自动做;手动等价命令,挂 pandora-agones 网络):
docker run -d --name pandora-udp-relay --network pandora-agones `
  -p 127.0.0.1:7000-8000:7000-8000/udp `
  -e TARGET_HOST=$(minikube -p pandora-agones ip) -e PORT_RANGE=7000-8000 `
  pandora/udp-relay:dev

# 进程版(仅调试;默认 profile=pandora-agones,自动解析 minikube -p pandora-agones ip):
pwsh tools/scripts/udp_relay.ps1
# 链路:client --UDP--> 127.0.0.1:<port> --[tools/udp-relay]--> <minikube ip>:<port> --> GameServer
```

> ⚠️ **必须用当前 profile（`pandora-agones` / `192.168.58.x`）的 minikube IP 和 docker network**。
> 旧的默认 `minikube` profile（`192.168.49.x`）network 重启后可能残留：若用裸 `minikube ip`、
> `--network minikube` 或 `docker network inspect minikube`，relay 会挂到错误 Docker 网络，
> **`pandora-udp-relay` 看似启动成功，但 UDP 包进不了 Hub DS——表现为客户端登录成功、却卡在进不去大厅**。
> `e2e_k8s.ps1` 启动前会校验 `TARGET_HOST` 是否落在该 docker network 的 IPv4 subnet 内，不匹配直接 fail。

> 真集群 / 非 docker-driver 不需要本中继，`advertise_host` 留空直接用 `status.address`。

---

## 4. 分两步验证（重要：心跳 ≠ Agones SDK）

### 第一步：Agones 分配链路（真 Pandora DS 镜像，**现在就能做**）

真 Pandora DS 镜像已接 Agones SDK，Fleet Ready 后可完整验证「分配 → Allocated → 返回真实 addr」：

```powershell
# 4.1 手测 GameServerAllocation(不依赖 ds_allocator)
kubectl create -f deploy/k8s/agones/40-gameserverallocation-example.yaml -o yaml
#   看 status.state=Allocated + status.address + status.ports[0].port

# 4.2 起本机 ds_allocator(agones.enabled=true), grpcurl 调 AllocateBattle
#   期望返回真实 ds_addr(GameServer host:port), 不再是 mock 127.0.0.1:300xx
#   日志 allocator_mode=agones

# 4.3 起本机 hub_allocator(agones.enabled=true), grpcurl 调 AssignHub region=cn
#   期望 hub_ds_addr 为真实 GameServer host:port, 日志 fleet_mode=agones
```

### 第二步：DS 业务心跳上报（真 UE DS）

Fleet Ready 只证明真 DS 的 Agones SDK health 正常；还必须验证它向 ds_allocator / hub_allocator
发送 Pandora 业务 Heartbeat（gRPC unary 每 5s，与 Agones SDK health 是两条独立链路，详见
`docs/design/ds-arch.md` §0.2）。

- 真 UE Pandora Hub DS / Battle DS（Pandora-Client 独立仓库，DS 侧已接管）按
  `docs/design/agones-dev.md` 的「DS 心跳上报契约」实现，心跳链路 + locator HUB/BATTLE
  上报闭环端到端由真 UE DS 跑通。
- 后端侧需验证的闭环：
  - **心跳 / sweep / locator**：DS 周期调 Heartbeat + SetLocation；DS 心跳中断后观察
    hub/ds_allocator sweep 标 draining/abandoned；BATTLE→HUB 带 fence matchId 合法回流（W4 ⑪）。
  - **战斗结算 → 段位回滚补偿链（不变量 §4 第二段）**：DS 同步 ReportResult → 事务出箱 →
    `pandora.player.update` → player 段位回写；NORMAL 验 Elo 守恒、ABANDONED 验 mmr_delta 全 0、
    幂等复测 alreadyRecorded=true、outbox 清零。

---

## 5. 风险 / 注意

- **DS 端口**：Pandora Hub/Battle DS 与 Fleet 均使用 UDP `7777`；修改 DS 监听端口时必须同步清单。
- **minikube 资源**：默认 2 个 Battle + 1 个 Hub GameServer，再加 Agones controller；建议
  `--memory>=6144`，不足会 Pending。
- **token 时效**：`kubectl create token` 默认/指定时效到期后 allocator 调用会 401，需重签。
  in-cluster 部署用投影 token 自动轮转（allocator 代码每次请求重读 token 文件已支持）。
- **insecure_skip_tls_verify 仅 dev**：生产必须配 `ca_path`，禁用跳过校验。
- **GameServerAllocation 是一次性对象**：手测用 `kubectl create`（非 `apply`），每次触发一次分配。
- **关停**：`minikube stop` / `minikube delete`（删整个集群）由 Codex/用户执行。

---

## 6. 验收标准（Codex 跑完交 Claude 复核）

- [ ] `kubectl get fleet` 显示 `pandora-battle-stable`(2)、`pandora-hub-stable`(1) 全 Ready，两个 Canary 默认 0 Ready
- [ ] 四个 Fleet / GameServer / Pod 的 `pandora.dev/release-track` 与 Fleet 名一致
- [ ] DS 只挂 public `pandora-dsticket-jwks-r<revision>`；任何 Fleet 都没有玩家私钥/HMAC
- [ ] 手测 GameServerAllocation 返回 `state=Allocated` + 真实 address:port
- [ ] ds_allocator `agones.enabled=true` 下 `AllocateBattle` 返回真实 ds_addr，日志 `allocator_mode=agones`
- [ ] hub_allocator `agones.enabled=true` 下 `AssignHub region=cn` 返回真实 hub_ds_addr，日志 `fleet_mode=agones`
- [ ] 被分配的 GameServer 上能看到 `pandora.dev/match-id` 等业务标签
- [ ] （UE DS 就绪后）Heartbeat 链路 + locator HUB/BATTLE 上报闭环跑通
