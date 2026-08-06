# Agones 拉起 Pandora Linux DS —— 部署 & 交接（给 Codex）

> 本目录 = **Pandora DS 镜像构建 + Agones 安装**的运维资产（后端「部署单一事实源」）。
> UE C++（Server 目标 / `UPandoraAgonesSubsystem`）和 UE 打包脚本在独立**客户端仓库** `Pandora-Client`。
> **「启动 + 压测」由 Codex 在真机/集群上执行**（见下「交接给 Codex」）。

---

## 架构一句话

```
客户端匹配成功
      │  (match.v1 StartMatch/GetMatchProgress)
      ▼
matchmaker ──► ds_allocator ──POST GameServerAllocation──► Agones
                                                              │ 选一个 Ready 的 pandora-battle GameServer
                                                              ▼  转 Allocated + 打 pandora.dev/match-id label
                                                        Linux DS Pod
                                                        ├─ 容器: pandora/battle-ds  ← deploy/ds 构建
                                                        └─ sidecar: agones-sdk (自动注入)
                                                              ▲
                       UPandoraAgonesSubsystem ──HTTP(127.0.0.1:$AGONES_SDK_HTTP_PORT)──┘
                       Agones Ready / Health / GET gameserver(读 match-id) / Shutdown
                                                              │
                                                              ▼
                       DS 拿到 match_id 后才上报 Pandora ready 心跳
ds_allocator 收到该心跳才下发 address:port ──► matchmaker push match_ready/READY
                                                              │
                                                              ▼
                                                   客户端 ClientTravel 进 DS 打这局
```

---

## 文件分布（挪动后）

后端仓库 `E:\work\Pandora`（本目录 `deploy/ds/`）：

- `deploy/ds/Dockerfile` / `entrypoint.sh` / `.dockerignore` — Linux DS 容器化。
- `deploy/ds/build-image.sh` — docker build + 可选本地 retag（`docker push` 由人手动执行）。
- `deploy/ds/install-agones.sh` — helm 安装/升级 Agones（基线 v1.58.0）。
- `deploy/ds/stage/LinuxServer/` — UE 打包产物落点（**不入库**，由客户端脚本拷入）。
- `deploy/k8s/agones/*.yaml` — Fleet / RBAC / Allocation。四份 Fleet 的 `image:` 一律钉**不可变 tag**（tag = 制品库快照版本号，如 `pandora/battle-ds:r1833-dirty-20260805-222737`），**严禁回填 `:dev`**——理由与换版流程见 `20-fleet-battle.yaml` 该行注释。

客户端仓库 `Pandora-Client`（编进 DS 二进制，**不可挪**）：

- `Pandora/Source/PandoraServer.Target.cs` — Server 目标（打 Linux DS）。
- `Pandora/Source/Pandora/Public/Net/PandoraAgonesSubsystem.h` / `Private/Net/PandoraAgonesSubsystem.cpp`
  — DS↔Agones SDK 桥接子系统（仅 Dedicated Server 创建，走 sidecar HTTP REST，不引 grpc-cpp）。
- `Pandora/Source/Pandora/Pandora.Build.cs` — 加 `Json` 依赖。
- `Tool/Server/Agones/build-linux-ds.ps1` — RunUAT 打 Linux DS，产物拷到本目录 `deploy/ds/stage`。

---

## 交接给 Codex：启动

### 0. 前置
- Windows 机装好 UE5.8 + **Linux 跨平台工具链**（设 `LINUX_MULTIARCH_ROOT`/`UE_ENGINE_DIR`）。
- 一个能跑 Agones 的 K8s（minikube/kind/云）：`kubectl get crd | grep agones.dev` 能看到 CRD。

### 0.0 minikube 最新版（国内下载）
> 当前基线：**minikube v1.38.1**（GitHub latest）。国内网络优先走 npmmirror 下载并校验 sha256：
```powershell
# 后端仓库根目录，Windows PowerShell
powershell -ExecutionPolicy Bypass -File .\deploy\ds\install-minikube-windows.ps1
minikube version
```
> 注意：`minikube v1.38.1` 是 **minikube 程序版本**；`minikube start --kubernetes-version=...`
> 指的是集群里的 **Kubernetes 版本**。本仓库启动/压测基线统一用 **Kubernetes v1.35.1**，
> 不降级到 v1.34.x。

启动 Agones 专用 minikube profile：
```powershell
powershell -ExecutionPolicy Bypass -File .\deploy\ds\start-minikube-agones.ps1
```
> 实测：npmmirror 可下载 minikube v1.38.1；阿里 `kubernetes-release` 旧路径缺 v1.35.1
> 的 `kubeadm/kubectl/kubelet.sha256`，会 404。因此启动脚本把 Kubernetes 二进制
> `--binary-mirror` 固定到 `https://dl.k8s.io/release`，Docker 基础镜像仍可走国内 `kicbase`。
> 如果当前网络连不上 `dl.k8s.io` 或拉不到 v1.35.1 组件镜像，需要换完整镜像源或预加载缓存；
> 脚本会直接失败提示，不会自动降级。

### 0.1 装最新 Agones（基线 v1.58.0）
> 官方仓库已从 `googleforgames/agones` 迁到 **`agones-dev/agones`**；helm repo 地址不变。
> 撰写时最新稳定版 = **v1.58.0**；本仓库搭配 **Kubernetes v1.35.1** 启动/压测。升级 Agones 时把这里和 Fleet 一起过一遍。
```bash
./deploy/ds/install-agones.sh 1.58.0
# 等价于: helm repo add agones https://agones.dev/chart/stable
#         helm upgrade --install agones agones/agones --version 1.58.0 -n agones-system --create-namespace --wait
```
> 实测当前网络下 Helm repo 会跳到 Google Storage，可能失败；脚本会 fallback 到
> `https://raw.githubusercontent.com/agones-dev/agones/v1.58.0/install/yaml/install.yaml`，
> 并使用 `kubectl apply --server-side --force-conflicts` 安装，避开 CRD 过大导致的
> client-side apply annotation 限制。
> 我们的 DS 走 **sidecar 的 HTTP REST**（`/ready` `/health` `/gameserver` `/shutdown`），这套接口跨 Agones 版本稳定，
> 升级 Agones 一般无需改 C++。Fleet 用的 `agones.dev/v1` / `allocation.agones.dev/v1` 也都是 GA，v1.58 适用。

### 1. 打 Linux DS（Windows，客户端仓库）
```powershell
# 在客户端仓库根目录：
./Tool/Server/Agones/build-linux-ds.ps1 -EngineDir "D:\UE_5.8"
# 产物归档并自动拷到后端 E:\work\Pandora\deploy\ds\stage\LinuxServer
# 若后端在别处，加 -StageDir "<repo>\deploy\ds\stage\LinuxServer"
```

### 2. 构建镜像（后端仓库）

本地 minikube 联调用 PowerShell 直接构建到 minikube 内置 Docker daemon：

```powershell
pwsh deploy/ds/build-image-minikube.ps1
# 构建 pandora/battle-ds:dev 和 pandora/hub-ds:dev；之后跑 e2e_k8s.ps1 -SkipImageLoad
```

需要推到远端 registry 时，用 bash 脚本构建/打 tag，push 仍由人手动执行：

```bash
./deploy/ds/build-image.sh pandora/battle-ds:dev <你的registry>
./deploy/ds/build-image.sh pandora/hub-ds:dev    <你的registry>
# 脚本只 build/tag，不 push。docker push 由人手动执行。
# 若用私有 registry，推送后记得同步改 Fleet 的 image: 为 <registry>/battle-ds:dev
```

### 3. 部署 Agones 资源
```bash
# 先建 pandora 命名空间(RBAC 的 ServiceAccount 建在 pandora ns),再 RBAC → DS 面 Envoy → Fleet
$ctx=pandora-agones # 改成你的本地 minikube context
kubectl --context "$ctx" apply -f deploy/k8s/services/00-namespace.yaml
kubectl --context "$ctx" apply -f deploy/k8s/agones/10-rbac-allocator.yaml
kubectl --context "$ctx" apply -f deploy/k8s/agones/16-ds-envoy.yaml   # 本地 minikube 必需:DS 回调 :8444 网关
kubectl --context "$ctx" apply -f deploy/k8s/agones/20-fleet-battle.yaml
kubectl --context "$ctx" apply -f deploy/k8s/agones/30-fleet-hub.yaml
kubectl get gameservers -w     # 等到 STATE=Ready（DS 调了 /ready 才会 Ready）
```
> 排障：若 GameServer 卡在 `Scheduled/RequestReady`，看 DS 容器日志里 `UPandoraAgonesSubsystem`
> 是否打了「Agones Ready 成功」。没有就检查 `AGONES_SDK_HTTP_PORT` env 是否注入、关卡是否成功加载。

### 4. 验证「按需分配」一条链路
```bash
kubectl create -f deploy/k8s/agones/40-gameserverallocation-example.yaml -o yaml
# 看返回 status.state=Allocated + status.address + status.ports[0].port
# 进对应 DS Pod 日志，应看到 FetchGameServer 读到 match-id=123456
```

---

## 交接给 Codex：压测

目标指标：分配延迟 P50/P95、Fleet 扩容跟手度、单 DS 承载、DS Ready 成功率。

1. **分配压测（直接打 Agones）**：循环 `kubectl create -f 40-gameserverallocation-example.yaml`，
   或写脚本并发 N 次 GameServerAllocation，统计 Allocated 耗时与失败率；配合
   `FleetAutoscaler`（已建：`25-fleetautoscaler-battle.yaml`，Buffer 策略）观察 Ready buffer 是否被打穿。
2. **端到端压测（打 matchmaker）**：用后端 `E:\work\Pandora\robot/`（机器人）或 `run/` 脚本
   批量发 Team→StartMatch，验证 ds_allocator→Agones→回址→客户端可连 的整链 QPS。
3. **单 DS 承载**：往一个 Allocated DS 灌 N 个模拟客户端连接，看 CPU/内存/网络与 tick 稳定性，
   据此校准 Fleet `resources` 与单 Pod 人数上限。
4. 监控：`deploy/prometheus/prometheus.yml` + Agones 自带 metrics（`agones_gameservers_count` 等）。

> 压测产生的临时 GameServerAllocation/负载脚本不要提交；调出的 replicas/resources/autoscaler
> 阈值回写到 `deploy/k8s/agones/*.yaml` 并说明依据。

---

## 注意 / 红线

- DS 侧逻辑**仅在 Dedicated Server 进程生效**（`ShouldCreateSubsystem` → `IsRunningDedicatedServer()`），
  客户端/编辑器不实例化，不会把 DS 逻辑带进客户端包。
- 不引 `agones-cpp-sdk`/`grpc-cpp`：DS 用 sidecar **HTTP REST**（`/ready` `/health` `/allocate` `/shutdown` `/gameserver`）。
- `entrypoint.sh` 用 `exec` 让 DS 成为 PID 1，正确收 `SIGTERM`（Agones 回收 Pod 优雅退出）。
- Fleet 里 `image:` 钉的是**不可变 tag**（制品库快照版本号），不是 `:dev` 占位。本地 `:dev` 只是构建链的中间产物，Fleet 不引用它；推到私有 registry 时把镜像名换成可拉取地址，但**仍须保持不可变 tag**。
- `deploy/ds/stage/` 是 UE 打包大产物，**不要入库**（用 `.gitignore` 排除）。

---

## 备选：官方 Agones Unreal 插件

Agones 有**第一方 UE 插件**（v1.54 已重构成 subsystem，v1.51 起支持 List）。
我们没用它，而是自己写了 `UPandoraAgonesSubsystem` 走 sidecar HTTP，理由：
1. 与项目「客户端/DS 不引 grpc-cpp、零额外依赖」铁律一致；
2. sidecar REST 跨版本稳定，升级 Agones 不动 C++；
3. 只用到 Ready/Health/Allocate/Shutdown/GetGameServer，自写更轻。

若以后想要 Counters/Lists、Reserve、Watch 等完整能力，可改接官方插件（功能更全，但会引入其 SDK 依赖）。

---

## DS 进战斗的地图模型（A / B）与「地图阻挡」

> 记录于 2026-06-17：DS 业务心跳（`UPandoraDSBackendSubsystem` → ds_allocator.Heartbeat，
> 每 5s，防 `battle_abandoned_heartbeat_timeout`）已在客户端仓库实现，挂在
> `APandoraBattleGameMode::BeginPlay/EndPlay`。要让它真正触发，DS 加载的关卡必须用对 GameMode。

### 两种模型

- **模型 A（推荐，无需 ServerTravel）**：一个 Fleet / 一张图，Pod **启动即 boot 进战斗图**。
  `entrypoint.sh` 用 `PANDORA_DS_MAP` 指定战斗图；客户端 `open ip:port?ticket=` 时 UE 握手
  自动把客户端 travel 到该图，**不需要写任何 DS 侧切图 / ClientTravel 代码**。多图就「一图一 Fleet」
  或在 Fleet env 里参数化 `PANDORA_DS_MAP`。
- **模型 B（单 Fleet 多图动态切换，才需要）**：Pod 先 boot 进中立 holding 图 → Ready，被 Allocated 后
  从 Agones label `pandora.dev/map-id` 读 map_id，DS 调 `GetWorld()->ServerTravel("/Game/.../Map_X")`
  切到真正战斗图（需 `bUseSeamlessTravel`）。当前**不做**；真有需求再实现，并要在
  `FPandoraAgonesGameServer` 里补 `map-id` / `game-mode` label 解析。

**结论**：先用模型 A 跑通闭环。`UPandoraAgonesSubsystem` 当前只解析 `pandora.dev/match-id`，足够模型 A。

### 「地图阻挡」/ 碰撞由谁处理

- Linux DS 加载的是**完整关卡**（含 BlockingVolume、StaticMesh 碰撞、NavMesh、物理）。DS 是**碰撞 / 移动的权威端**：
  渲染被跳过（`-nullrhi` 等），但碰撞、物理、阻挡、寻路**照常在服务器跑**并校验客户端。
- 所以「每个 Fleet 用自己的 `PANDORA_DS_MAP` 启动，DS 会处理该图的碰撞阻挡」——**是的，DS 端权威处理**，
  客户端本地也有碰撞但以服务器为准。无需为碰撞额外写逻辑。

### ⚠️ 当前两个必须修的配置缺口（2026-06-17 核实，否则心跳不闭环）

> 均用 UE 标准机制 + 环境变量实现，**无需改 .umap/.uasset，也不动业务代码**
> （心跳仍由 `APandoraBattleGameMode::BeginPlay/EndPlay` 拥有，单一生命周期路径）。battle Fleet 已写好 env。

1. **战斗 DS 的 GameMode 不是 `APandoraBattleGameMode` → 心跳本来不会启动。**
   心跳挂在 `APandoraBattleGameMode::BeginPlay`；但 Fleet 的
   `PANDORA_DS_MAP=/Game/Entry/Level/Lvl_Server_Entry` → `GM_Server_Entry`（父类引擎 `GameModeBase`），
   不是 battle GameMode，`BeginPlay` 不触发。
   **已修（UE 标准做法：`?game=` URL option）**：DS 启动时用 GameMode URL 覆盖，优先级高于
   地图 WorldSettings 的 GameModeOverride，**无需改地图资产、无需额外业务代码**：
   - `20-fleet-battle.yaml` 已加 env `PANDORA_DS_GAMEMODE=/Script/Pandora.PandoraBattleGameMode`；
   - `entrypoint.sh` 把它拼成 `地图?game=/Script/Pandora.PandoraBattleGameMode` 传给 `PandoraServer.sh`；
   - 于是 DS 加载该图时用 `APandoraBattleGameMode`，`BeginPlay` 起心跳，`EndPlay` 停心跳——唯一路径。
   hub Fleet 不注这个 env（或指其他 GameMode）即不起战斗心跳。
   本地手动跑：`PandoraServer.sh 地图?game=/Script/Pandora.PandoraBattleGameMode -port=7777 -log`。
   备注：`Lvl_Server_Entry` 是 dev 占位入口图；正式战斗图就绪后改 `PANDORA_DS_MAP` 指向它即可，
   `?game=` 机制与具体地图解耦。

2. **`DSAllocatorEndpoint` 默认 `127.0.0.1:20020` 在集群里是错的（裸 gRPC 端口 + 同 Pod 假设）。**
   关键：DS 走 **grpc-web**（FHttpModule），**不能直连裸 gRPC `:20020`**，必须打到 **Envoy 的「DS 面」
   grpc-web 监听器 `:8444`**（集群内明文 grpc-web→gRPC 转换，并按精确 method 白名单
   路由到内部服务，见 `deploy/k8s/agones/16-ds-envoy.yaml`）。grpc-web 模型下 DS 的
   4 个 endpoint（hub/ds/locator/battle_result）其实都指向**同一个 Envoy `:8444`**，由 Envoy 按 gRPC 路径分发。
   **已修**：支持环境变量覆盖，镜像不变、只改 Fleet env：
   - `PANDORA_DS_ALLOCATOR_ADDR`（host:port，覆盖 endpoint）；
   - `PANDORA_DS_ALLOCATOR_TLS`（同集群默认 `0`；只有 DS 位于集群外并接入 TLS 边缘时才显式设 `1`）。
   `20-fleet-battle.yaml` 已填**线上同款集群内 Service DNS** `pandora-envoy.pandora.svc.cluster.local:8444` + TLS `0`
   （本地 minikube 由 `deploy/k8s/agones/16-ds-envoy.yaml` 在集群内起等价 Envoy「DS 面」网关;
   不再用 `host.docker.internal`——minikube 的 Pod 解析不了该域名）。
   ⚠️ **线上真集群**：同集群 `pandora-envoy` Service 继续使用 TLS `0`;DNS 不同则用
   `start.ps1 -Mode online -DsGatewayAddr` 覆盖。仅集群外 TLS 边缘配好证书与 DS 信任链后，
   才显式传 `-DsGatewayTls 1`。
   **DS 镜像/代码不变,只换 env 值。**
   未填则回退裸端口默认值，心跳会传输失败但不崩，日志可见 `DS call transport failed`。
