# 构建/发布机搭建手册

把一台新机器变成能完成「UE 打包 → 发布制品库 → 打 DS 镜像」全链的构建机。

> 配套自检脚本：`tools/devops/preflight-buildmachine.ps1`
> 每一步做完都可以跑它验证，全绿再往下走。

---

## 0. 要搬过去的东西

| 内容 | 来源 | 体积 | 备注 |
|---|---|---|---|
| **UE 引擎** | `E:\GitRepos\PandoraEngine.git`（裸仓库） | **51 GB** | 已编译的 installed build，**clone 完不用再编译** |
| **Linux 交叉编译工具链** | `C:\UnrealToolchains\v26_clang-20.1.8-rockylinux8\` | 3.3 GB | ⚠️ **不在引擎仓库里**，必须单独装或拷 |
| **客户端源码** | SVN `http://infinity-svn/svn/Pandora-Moba` | 大 | `svn checkout` |
| **后端源码** | `https://github.com/luyuan-go/XuanMing-Server.git` | 小 | 注意用 `luyuan-go`（仓库已从 `luyuan-cpp` 迁移） |

引擎那 51 GB 是大头，按内网带宽预留时间。

---

## 1. 引擎：clone + **注册**

```powershell
git clone E:\GitRepos\PandoraEngine.git D:\PandoraEngine
```

（路径按实际共享方式调整；也可以直接拷贝整个裸仓库过去再本地 clone。）

**注册这一步不能漏**：

```powershell
D:\PandoraEngine\Engine\Binaries\Win64\UnrealVersionSelector.exe /register
```

注册会往 `HKCU:\SOFTWARE\Epic Games\Unreal Engine\Builds` 写条目。
**不注册的后果**：`Tool\Build\PackageSet.ps1` 挑不到自制引擎，会退到 Epic Launcher 发行版，
然后 Server 目标直接失败：`Server targets are not currently supported from this engine distribution`。

---

## 2. Linux 工具链

装 UE 官方 Linux 交叉编译工具链（版本要与引擎匹配，本项目当前为 `v26_clang-20.1.8-rockylinux8`），
或直接从原机器拷贝整个目录过去。

---

## 3. 环境变量（**设完必须重开终端**）

```powershell
setx LINUX_MULTIARCH_ROOT "C:\UnrealToolchains\v26_clang-20.1.8-rockylinux8\"
```

```powershell
setx PANDORA_ARTIFACT_ROOT "D:\artifacts"
```

- `LINUX_MULTIARCH_ROOT`：不设或路径不存在 → 打 Linux DS 时 `PackageSet.ps1` 直接 throw。
- `PANDORA_ARTIFACT_ROOT`：制品库根目录，**按这台机器的实际盘符设**。
  不设会退到硬编码默认值 `F:\work\artifacts`，新机器上通常不存在 →
  发布落空、DS 包解析不到。

### ⚠️ 制品根必须是文件系统路径，不能是 URL

发布与解析脚本用的是 **文件系统 API**（`robocopy` / `Test-Path` / `Get-FileHash` / `Move-Item`）。
把制品根设成 URL **不会报错，只会静默失效**（`Test-Path` 直接返回 `False`，
表现为「找不到任何制品」），是最难排查的一类故障。

- ✅ 本地路径：`D:\artifacts`
- ✅ SMB 共享 UNC：`\\主机\共享\artifacts`（`robocopy` 原生支持）
- ❌ 任何 URL 形式

自检脚本会对 URL 形态直接判 FAIL。

> **团队怎么拿包**：制品根是本机的**权威制品库**，不直接对外。
> 分发走 MinIO —— `artifacts-sync` 流水线会在构建成功后自动把已发布产物同步上去，
> 详见 [`publish-to-minio.ps1`](./publish-to-minio.ps1) 与 [`Jenkinsfile.artifacts-sync`](./Jenkinsfile.artifacts-sync)。

---

## 4. 拉两个仓库

```powershell
git clone https://github.com/luyuan-go/XuanMing-Server.git
```

客户端按既有方式 `svn checkout`。

---

## 5. 自检

```powershell
pwsh tools\devops\preflight-buildmachine.ps1 -ClientRepo <客户端工作副本路径>
```

逐项检查：PowerShell 7 / svn / git / docker / go、UE 编辑器是否占用、
`LINUX_MULTIARCH_ROOT`、已注册的源码引擎、客户端三个打包脚本是否齐全、制品根、磁盘空间。

退出码 `0` = 无阻断，`1` = 有阻断。**全绿再往下走。**

---

## 6. 打包 → 发布 → 打镜像

```powershell
pwsh Tool\Build\PackageSet.ps1 -Flavors 'Server/Linux/Development'
```

```powershell
pwsh Tool\Build\PublishPackages.ps1 -AllowDirty
```

```powershell
pwsh deploy\ds\build-image-minikube.ps1 -BuildOnHost
```

- 第 1 步硬前提：**UE 编辑器必须关闭**（会阻塞 UBT）、源码引擎已注册、`LINUX_MULTIARCH_ROOT` 已设。
- 第 2 步 `-AllowDirty`：工作副本非纯净版本时放行，版本号会带 `-dirty-<时间戳>` 并进 `snapshots/` 快照轨。
  纯净版本且要发正式版时改用 `-Version vX.Y.Z`，进 `releases/` 轨。
  **只有 `Package.bat` / `PackageSet.ps1` 的产出带 `BUILD_INFO.txt` 才能发布**，
  缺它会被发布器判为「不是完整打包产物」拒收。
- 第 3 步想先确认包来源，加 `-ResolveOnly`：只打印解析到哪个包、不同步不构建。
  要把镜像推到制品 registry 再加 `-PushRegistry`（必须同时带 `-BuildOnHost`）。

---

## 已知坑（都是踩过的）

1. **引擎会翻回 Epic 发行版**：工程 `EngineAssociation` 若指向 `"5.8"` 而非注册的自制引擎 GUID，
   重启后尤其容易翻，导致 Server 目标编译失败。
2. **DS 包来源已改为制品库**（2026-07-25）：客户端 `Packages` 目录不再参与分发，
   `build-image-minikube.ps1` 优先从制品库解析。解析优先级：
   `-SourcePkg` > `PANDORA_DS_LINUX_PKG` > 制品库（两轨取最新）> 同级 `Packages`（已退役，仅兼容）。
3. **降级守卫**：解析到的包若比 `deploy/ds/stage` 里已有的更旧，构建会 **fail-closed 拒绝**，
   避免 `robocopy /MIR` 用旧二进制静默覆盖新的、让镜像退回老代码。
   确需回滚加 `-AllowOlderPackage`。
4. **快速迭代产物进不了制品库**：`Tool\Server\Agones\build-linux-ds.ps1` 输出到 `PandoraDSArchive`，
   **不写 `BUILD_INFO.txt`**，会被发布器拒收。目前只有全量 `PackageSet.ps1` 的产出可发布。
5. **PowerShell 数组 splat 传 switch 参数会误绑**：`& script @('-Build','-BuildMode','host')`
   会把 `-Build` 当成 `-BuildMode` 的值。要显式传参或用 hashtable splat。
