# Pandora DevOps 本地一键栈

把「制品库 + CI 执行器」这层从无到有搭起来。一条命令起，可原样搬到另一台机器。

> 定位：填补发布线四层里的 **②CI 执行器** 和 **③制品库** 两层落地。
> ①版本库退库 + 钩子、④发布 manifest（`make_release.ps1` / `artifacts/`）已在别处存在，本栈不重复。

---

## 这套起了什么

| 服务 | 端口(默认) | 作用 | 大厂对标 |
|---|---|---|---|
| **Jenkins** controller | 8080 / 50000 | CI 编排（调度流水线，不亲自编译） | Jenkins |
| **registry:2** | 5000 | 镜像库，`pandora-images.tar` 的正确归宿 | Harbor 的轻量替代 |
| registry-ui | 8082 | 镜像库 Web 界面 | Harbor UI |
| **MinIO** | 9000(API) / 9001(控制台) | UE 打包大 zip 的对象存储，带 30 天保留 | MinIO / S3 |

**刻意没塞进一键的**（见文末「升级路径」，需要额外环境，硬塞进来只会让脚本假装能跑）：
- **Harbor** —— 官方是独立 installer tar 包，Windows 上重；先用 registry:2 顶上，规模上来再换。
- **Argo CD** —— 必须先有 k8s 集群（你们的 minikube），属于部署层 GitOps，不是本地一键能覆盖的。
- **GoReleaser** —— 它是 CLI 不是常驻服务，用法见文末。

---

## 前置

- **Docker Desktop（Windows）/ Docker Engine（Linux）**，自带 `docker compose` v2
- **PowerShell 7**（`pwsh`）
- 本机已验证：Docker 29.5.3 + Compose v5.1.4 ✓

## ⚠️ 镜像获取（国内网络必读）

本机实测：**Docker Hub 的 CDN `production.cloudfront.docker.com` 被墙/限速** —— 小镜像
（`registry:2` 37MB）能挤过去，大镜像（`minio/minio`）blob 传输每次 EOF 断流。DaoCloud
之类**重定向型** mirror 无效（manifest 走了、blob 仍 302 回 cloudfront）。

**已用的绕行办法**（`up.ps1` 前先跑一次，把 MinIO 从 quay.io 拉回来再 retag）：

```bash
bash pull-quay.sh     # 从 quay.io 拉 minio/minio + minio/mc，retag 成规范名
```

Jenkins 基础镜像本机已有 `jenkins/jenkins:lts`（故 Dockerfile 用它，不用 `lts-jdk21` 触发大拉）；
Jenkins **插件**走 `updates.jenkins.io`，实测可达。

**另一台机器**若同样墙内，二选一根治（比逐个 quay 更省事）：
1. 配一个**能代理 blob（非重定向）**的 registry mirror —— 见下方「配阿里云加速器」；
2. 给 Docker 挂一个可用的 **HTTP 代理 / VPN**。

选定后 `up.ps1` 就能一把过，不必再手动 quay。

### 配阿里云镜像加速器（推荐，需你本人操作）

阿里云加速器是**真缓存代理**（自己存 blob），不像 DaoCloud 那样把 blob 302 回被墙的
cloudfront —— 这正是它可能有效而 DaoCloud 无效的原因。

**第 1 步：取你的专属地址**
登录 [阿里云容器镜像服务 ACR](https://cr.console.aliyun.com) → 左侧「镜像工具 → 镜像加速器」，
复制形如 `https://<你的ID>.mirror.aliyuncs.com` 的地址（每个账号不同，**必须自己登录获取**）。

**第 2 步：写进 daemon.json**
Docker Desktop → Settings → Docker Engine，把 `registry-mirrors` 一行**并入**现有 JSON
（本机现有配置已含 `builder` / `experimental`，不要整体替换掉）：

```json
{
  "builder": {
    "gc": {
      "defaultKeepStorage": "20GB",
      "enabled": true
    }
  },
  "experimental": false,
  "registry-mirrors": ["https://<你的ID>.mirror.aliyuncs.com"]
}
```

点「Apply & Restart」等 Docker 重启完成。

**第 3 步：验证真的生效**

```powershell
docker info | Select-String -Pattern "Registry Mirrors" -Context 0,2   # 应列出你的加速器地址
docker rmi minio/minio:latest -f; docker pull minio/minio:latest       # 之前必断的大镜像，现在应能拉完
```

**已知边界**：`registry-mirrors` **只对 Docker Hub（docker.io）镜像生效**，
对 `quay.io` / `gcr.io` / `ghcr.io` 无效——那些仍需直连或走代理。
本栈除 MinIO 外全部来自 Docker Hub，故配好加速器后 `pull-quay.sh` 可不再需要。

> 本文档未替你修改 Docker 配置：改 daemon 属系统设置，需你自己在 Docker Desktop 里确认应用。
> 上面第 3 步的验证在本机**尚未跑过**（缺你的加速器 ID），生效与否以你实测为准。

---

## 起 / 停

```powershell
cd XuanMing-Server/tools/devops
pwsh ./up.ps1          # 一键起（首次拉镜像+装 Jenkins 插件，约几分钟）
pwsh ./down.ps1        # 停，保留数据
pwsh ./down.ps1 -Volumes   # 停并清空所有数据卷（慎用）
```

`up.ps1` 会：检查 Docker → 缺 `.env` 就从 `.env.example` 生成 → `compose up -d --build` → 打印各服务地址和账号。

Jenkins 首启装插件需 1–3 分钟，其间登录页 502/未就绪是正常的，稍等重刷。

---

## 换一台机器怎么搭（你说的「另外一台机器」）

无绝对路径、无写死 IP，可平移：

1. 那台机器装好 **Docker Desktop + PowerShell 7**。
2. 把本仓库同步过去（SVN/git 拉，或直接拷 `tools/devops/` 整个目录）。
3. ```powershell
   cd XuanMing-Server/tools/devops
   cp .env.example .env      # 按需改端口/口令（生产务必改口令）
   pwsh ./up.ps1
   ```
4. 完事。因为 Jenkins 用 **JCasC** 声明式初始化（`jenkins/casc.yaml` + `jenkins/plugins.txt`），两台机器起出来的 Jenkins 配置/插件**完全一致**，不靠手点向导。

> 唯一「每台机器手动一次」的是 **Jenkins 里的 SCM 凭据**（SVN 账号 / git 凭据）—— 这是秘密，不进仓库。见下节。

---

## 接进现有流程

### 1. 镜像：DS 构建脚本已接 registry

`deploy/ds/build-image-minikube.ps1` 已支持推制品库（本次接线）：

```powershell
pwsh deploy/ds/build-image-minikube.ps1 -BuildOnHost -PushRegistry
```

推完会打印 **digest**，那才是该记进发布 manifest 的不可变引用（tag 会被覆盖，digest 不会）。

**为什么必须带 `-BuildOnHost`**：不带时脚本会把 docker CLI 切到 **minikube 内置 daemon**，
`localhost:5000` 就变成 minikube VM 内部而非宿主机的 registry 容器 —— 会造成
「push 看着成功、制品库里啥也没有」。脚本对这个组合 **fail-closed 直接报错**，不静默推错地方。
另 `-RegistryHost harbor.xxx:5000` 可推到别的 registry。

`localhost` 默认被 Docker 当 insecure-ok，push 不用改 daemon 配置。

**让 minikube 反过来从这个 registry 拉**是额外一步（minikube 在自己的 VM 里，要配
insecure-registry 指向宿主机地址）—— 需要时单独做，不在本栈范围；当前 DS 部署仍走
`minikube image load`。

### 2. UE 产物：从「本地目录」升级到「MinIO」
`make_release.ps1` 产出后，用 mc 或 aws-cli 上传：
```powershell
# 一次性配 alias
mc alias set pandora http://localhost:9000 <MINIO_ROOT_USER> <MINIO_ROOT_PASSWORD>
# 上传某个 release（按版本号分目录，不可变）
mc cp --recursive ..\..\..\artifacts\releases\ pandora/pandora-artifacts/releases/
```
桶 `pandora-artifacts` 已建好并挂了 30 天保留策略。

### 3. Jenkins 挂 4 条流水线
仓库里已有 4 条 Jenkinsfile：
- `Pandora-Client-SVN/Tool/Build/Jenkinsfile`(+`.release`)
- `XuanMing-Server/Jenkinsfile`(+`.release`)

登录 Jenkins → New Item → **Pipeline** → 「Pipeline script from SCM」→ 填仓库地址 + `Script Path` 指到对应 Jenkinsfile → 建 SVN/git 凭据。
（想开机自带这几个 job：`jenkins/casc.yaml` 末尾有 job-dsl 模板，补好凭据后取消注释即可。）

### 4. 构建 agent（关键）
Jenkins controller 在容器里，`numExecutors=0`，**不亲自编译**。真正的 UE cook / Go 编译要挂一个**宿主机 agent**——就是那台装了 UE 源码引擎 / Go 1.26.5 / svn / Docker 的构建机：
Jenkins → Manage Nodes → New Node → 用 50000 端口的 inbound agent 接入。
（UE 无法在 Linux 容器里 cook，这是为什么构建必须落到宿主机 agent，而非 controller 容器内。）

---

## 其余工具链（已落地 / 各自的边界）

| 工具 | 状态 | 入口 |
|---|---|---|
| **Argo CD** | ✅ 已装进 `pandora-agones` minikube（v3.4.5） | `deploy/k8s/argocd/` |
| **Harbor** | 📄 安装脚本就绪（**限 Linux 宿主机**） | `tools/devops/harbor/install-harbor.sh` |
| **Nexus** | ⚙️ compose 可选 profile（默认关，职责重叠） | `--profile nexus` |
| **GoReleaser** | 见 `docs/design/release-pipeline.md` 的 go.work 说明 | — |
| **Horde** | ❌ 刻意不做一键 | 见下方说明 |

### Argo CD（已部署）

```powershell
pwsh deploy/k8s/argocd/install-argocd.ps1                    # 装（幂等，版本钉死 v3.4.5）
pwsh deploy/k8s/argocd/install-argocd.ps1 -PrintPasswordOnly # 取初始 admin 密码
kubectl -n argocd port-forward svc/argocd-server 8090:443    # 访问 https://localhost:8090
```

**Application 默认手动同步、不 prune**，这是刻意的安全默认：`pandora` 命名空间是活环境，
且 Secret `pandora-config` 由 `start.ps1` 现场生成、**不在 git 里** —— 开 prune 会被删掉导致服务起不来。
细节见 `deploy/k8s/argocd/application-services.yaml` 顶部警告。

### Harbor（Linux 宿主机）

Harbor 官方**只支持 Linux**，Windows 开发机上继续用 registry:2。到 Linux 构建机/服务器上：

```bash
sudo HARBOR_ADMIN_PASSWORD='<强密码>' tools/devops/harbor/install-harbor.sh
```

**墙内关键**：脚本用 **offline installer**（自带全部镜像 tar，`docker load` 本地导入），
**完全不碰 Docker Hub** —— 正好绕开 cloudfront 被限速的问题。online installer 墙内必失败。

Harbor 与 registry:2 是**二选一替代**（Harbor 自带 registry 并占 80/443），不是叠加。

### Nexus（默认关）

与本栈已有组件职责重叠（镜像 → registry:2；大二进制 → MinIO），按 CLAUDE.md §15
「不做预设性复杂化」默认不起。确需 maven/npm/nuget 私服时：

```powershell
docker compose -f docker-compose.stack.yml --profile nexus up -d
# 初始密码：docker exec pandora-nexus cat /nexus-data/admin.password
```

### Horde（刻意不做一键）

Epic 官方 CI，专为 UE 大规模分布式编译设计。**不纳入本地一键栈**：它需要
UE 源码引擎 + .NET + SQL Server/MongoDB + 专用 agent 池，是一套独立基础设施，
成本远超当前团队规模的收益。Jenkins + 宿主机 agent 已覆盖你们的 UE 构建需求；
等"多台构建机分布式编译成为瓶颈"时再评估 Horde 才有意义。

### GitHub Actions / GitLab CI（不适用）

两者都要求代码托管在对应平台。你们客户端在 **SVN**、后端 git remote 是 GitHub 但
构建依赖本地 UE 引擎和大体积资产，跑 GitHub-hosted runner 不现实。
Jenkins + 自有构建机是当前形态下的正确选择。

---

## 目录

```
tools/devops/
├── docker-compose.stack.yml   # 栈定义
├── .env.example               # 每机一份配置（up.ps1 自动生成 .env）
├── up.ps1 / down.ps1          # 一键起/停
└── jenkins/
    ├── Dockerfile             # LTS + 预装插件（离线一致）
    ├── plugins.txt            # 插件清单
    └── casc.yaml              # JCasC 声明式初始化
```
