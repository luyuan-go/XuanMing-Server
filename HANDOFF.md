# Pandora 接班手册

## §0 项目一句话

**Pandora** = MOBA(5v5)+ 持续在线大厅(500 人/实例,全图自由 PvP)。
后端 Go + Kratos,UE 5.8 客户端 + DS,Envoy + gRPC-Web 网关,Kafka + MySQL + Redis + etcd 基础设施。

---

## §1 铁律

### 1.1 客户端连接(2 条,锁死)

```
Client(UE 5.8)
├── ① UE NetDriver → Hub/Battle DS         仅游戏内同步 / GAS / Replication
└── ② FHttpModule → Envoy(8443 HTTPS)     gRPC-Web over HTTP/2 TLS
                                            业务请求 unary + 推送 server stream
```

- **Client 不走 gRPC 原生**(走 gRPC-Web,UE 自研基于 FHttpModule)
- **客户端零额外依赖**(不拉 grpc-cpp 80MB,不装第三方 UE gRPC 插件)
- **2 条连接,不是 3 条**(2026-06-04 推翻 gateway+push 分离方案)

### 1.2 后端框架

| 项 | 锁定值 |
|---|---|
| Go 框架 | **Kratos v2.9.2** |
| Go 版本 | go1.26.5 |
| Log | Kratos log + **zap** 实现 |
| Config | yaml + file source(W3+ 接 etcd) |
| Edge Gateway | **Envoy v1.38.0**(grpc-web filter) |
| 服务发现 | k8s Service + DNS(W3 + Kratos registry/etcd 可选) |
| Kafka client | sarama v1.43.1 |
| Redis client | go-redis/v9 v9.16.0 |
| Proto 工具 | **buf v1.70.0** |

### 1.3 协议铁律

- **UE 有的功能 proto 里不写**(GAS / Replication / ServerRPC 都不写 proto)
- **proto 不写战斗 tick 字段**(那是 UE Replication 的事)
- **Heartbeat 用 unary 每 5s**
- **Client 不直连 gRPC 业务服**(走 Envoy → Kratos)
- **DS 不兼任业务网关**

### 1.4 RPC 顺序与 Response 语义(4 协议原则)

详见 `docs/design/protocol-ordering-rules.md`:

1. **原则 1**:立即完成型 RPC 的 response 必须返完整业务数据(客户端不需要等 push)
2. **原则 2**:kafka push 不发给请求发起方(发起方看 response,避免 smell)
3. **原则 3**:已受理型 RPC(StartMatch / ConfirmMatch)显式标注,客户端 UI 状态机由 push 驱动
4. **原则 4**:每个 RPC 在 proto 注释里标注"立即完成"或"已受理"语义

### 1.5 服务目录布局

```
F:/work/Pandora/
├── services/
│   ├── account/      (login, player)
│   ├── social/       (friend, chat, dialogue)
│   ├── matchmaking/  (team, matchmaker)
│   ├── battle/       (ds_allocator, hub_allocator, battle_result)
│   ├── economy/      (trade)
│   ├── data/         (data_service)
│   └── runtime/      (player_locator, push)
```

Module 路径:`github.com/luyuancpp/pandora/services/<域>/<服务>`

### 1.6 命名规则

- **目录布局**:`proto/pandora/<domain>/v1/<file>.proto`
- **RPC 请求/响应类型**:`XxxRequest` / `XxxResponse`(不用 Req/Resp 缩写)
- **Package**:`pandora.<domain>.v1`
- **Service**:`<Name>Service`(LoginService / TeamService)
- **字段**:`snake_case`(player_id / created_at_ms)

### 1.7 大小写规则

- **Pandora**(首字母大写):仓库名 / 路径 / 文档项目名引用 / Go module 顶级名
- **pandora**(全小写):kafka topic / mysql / redis key / docker / go module path
- **MOBA**:仅描述游戏类型(不指代项目)

---

## §2 当前进度

接班 AI 必须自己根据 `PROGRESS.md`、代码目录和最近提交确认当前状态。
不要依赖本文记录的服务完成情况。
当前下一步见 §3。

---

## §3 当前下一步

### Step 1:W4 ⑤ — hub_allocator 服务实现

目标:

1. 基于已生成的 `proto/pandora/hub/v1/allocator.proto` 与 `HubShardStorageRecord` / `HubAssignmentStorageRecord` 落地 Kratos 服务
2. 接 Redis 维护 hub 分片镜像与玩家归属,容量默认 500 人/实例
3. 实现 `AssignHub` / `ReleaseHub` / `TransferHub` / `ListHubs` / `Heartbeat`
4. login 调 hub_allocator 拿真实 `hub_ds_addr` + hub ticket
5. 当前无真实 Agones/Hub DS 时可先用 mock seed hub,但协议和存储边界必须按最终形态写

### Step 2:可靠补偿收口

- 修 W4 ③ 已记录的阶段限制:`ds.lifecycle` / `player.update` 仍是 best-effort,需要 outbox、待补偿队列或 battle_result 对账路径三选一
- 目标是让 `CLAUDE.md §9.4 DS 崩溃必有补偿` 从"Kafka 正常时成立"升级为可靠闭环

### Step 3:UE 主链路

- UE 客户端 grpc-web(FHttpModule 自研解析)接 Envoy
- UE Hub DS / Battle DS 骨架 + GAS / Iris / Agones 联调
- 打通登录 → 进大厅 → 匹配 → 进战斗 → 结算 → 回大厅

### 明确暂缓

`friend`(:20004) 和 `chat`(:20005) 现在不做;保留 proto / 端口 / topic 规划,等 UE 与核心链路全部完成后,再作为社交尾部功能实现。

---

## §4 接班 AI 工作守则

### 4.1 必读文档

1. `CLAUDE.md`
2. `AGENTS.md`
3. `docs/design/pandora-arch.md`
4. `docs/design/gateway-decision.md`
5. `docs/design/infra.md`
6. `PROGRESS.md` 最新 W2 段落

### 4.2 工作流

- Claude / Agent 默认直接执行,不再要求先走前置流程。
- 编码 / 配置任务直接按 `AGENTS.md` 和设计文档约束实现。
- 跑项目内验证命令。
- 不做 git 收尾,把验证结果交给 ChatGPT / Codex。
- ChatGPT / Codex 做完环境 / 文档 / git 收尾后,Claude 必须审核相关产物和验证结果。
- 非代码任务,或项目分析 / 逻辑细节任务中需要执行的辅助部分,由 Claude 生成执行操作信息,用户复制给 ChatGPT / Codex 执行。
- 涉及安装工具、改系统环境、写 secrets、生产集群、push / tag 等红线时必须停止并等人授权;大范围改动不设文件数硬上限(方向标准/正确/更优即可放手做,完成后列出改动范围与验证)。

### 4.3 跨 AI 分工

**Claude 系模型负责**:

- 深度分析
- Agent 直接执行
- 输出关键做法和验证结果
- 生成可直接粘贴给 ChatGPT / Codex 的非代码任务辅助执行操作信息
- 改代码 / proto / yaml / 脚本 / 文档
- 跑项目内验证
- 审核 ChatGPT / Codex 完成的环境配置、文档整理、git 收尾结果

**Claude 系模型不负责**:

- 安装工具
- 改系统环境
- 生成证书
- 拉 Docker 镜像
- 启停本机环境
- git status / diff / commit message / commit / push / tag

如果需要环境配置,Claude 只输出:

- 环境配置方案
- 命令
- 风险
- 验收标准

**ChatGPT / Codex 负责**:

- 本机工具和环境配置
- 执行 Claude 系模型生成的非代码任务辅助操作信息
- 证书 / Docker 镜像 / 本地环境启停
- 环境确认
- git status / diff / commit message 建议
- 用户明确授权后的 commit
- 完成后把改动范围、验证结果、剩余未处理项交给 Claude 系模型复查
- 不实现业务代码,不处理业务逻辑细节;只做审核、问题分析、辅助执行和收尾。发现问题时,生成可直接粘贴给 Claude 系模型的问题反馈。

### 4.4 失败和红线

- 不假装成功。
- 不跳过失败。
- 不绕过测试。
- 发现要写 secret、push 远端、规范冲突时立即停止报告。

---

## §5 当前未决项

- ✅ UE 客户端 + DS 独立仓库已确定；M0–M1.5 FPS PoC 已完成：DS 联机 / 角色 / EnhancedInput / 武器 / MVVM HUD / GAS。**UE 工程/模块/类命名统一为 Pandora**，以后 UE 侧一律用 Pandora 命名，不再用 Xuanming/Xm。
- ⏸️ k8s 选型:阿里云 ACK / 自建 / 先 minikube(D7 阻塞)
- ⏸️ Envoy 跑模式:k8s Ingress / 独立 Pod(D7 决定)
- ⏸️ JWT 鉴权细节(login 服务签发 + Envoy jwt_authn filter)(W3 写 login 时定)

---

## §6 关键文件索引

| 想了解什么 | 看哪个文件 |
|---|---|
| 当前进度 | `PROGRESS.md` |
| 项目规范 | `CLAUDE.md` / `AGENTS.md` |
| 总架构 | `docs/design/pandora-arch.md` |
| Envoy + gRPC-Web | `docs/design/gateway-decision.md` |
| 端口 / topic / 命名 | `docs/design/infra.md` |
| 服务清单 | `docs/design/go-services.md` |
| proto 源文件 | `proto/pandora/<domain>/v1/*.proto` |
| proto 生成脚本 | `tools/scripts/proto_gen.ps1` |
| docker compose | `deploy/docker-compose.dev.yml` |
| Prometheus 配置 | `deploy/prometheus/prometheus.yml` |

---

## §7 UE 引擎 / Dedicated Server 构建事实(2026-06-16 实测确认)

> ⚠️ **5.8 更新(2026-07-25)**:本节是 2026-06-16 在原开发机(`D:` / `C:\work` 布局)上的 5.7 时代实测记录,版本标签已改 5.8。两点变化:
> ① **联机不再靠 CL 对齐** —— 5.8 起客户端在 `FPandoraGameModule` 覆盖了 `FNetworkVersion`,NetCL 不含引擎构建 CL(见 Pandora-Client `Doc/客户端/框架/网络/客户端网络版本互通.md` 与本仓 `agones-dev.md §2.5`);故下方 §7.1 表与 §7.3 交接话术里的 `47537391` / `必须仍为 47537391` 均为 5.7 时代值,**5.8 不再适用**。
> ② **本机实测的 5.8.0 Launcher 在 `E:\Program Files\UE_5.8`**(不是 `D:\Program Files\Epic Games\...`):`CompatibleChangelist=0`(回退用 `Changelist`)、`Changelist=55116800`、有效 NetCL `55116800`、`BranchName=++UE5+Release-5.8`;源码版 `D:\UnrealEngine` 的 5.8 构建本机不存在,未核实。

### 7.1 已验证事实(新会话不要重复怀疑)

1. **Launcher 发行版打不了 Dedicated Server**:`E:\Program Files\UE_5.8` 的 `InstalledPlatforms` 只有 `PlatformType=Editor/Game`,**无 `Server`**;Epic 官方设计如此,勾任何 optional component 都补不回来。报错 `Server targets are not currently supported from this engine distribution` 即源于此。
2. **源码版能打 Server**:`D:\UnrealEngine`,UE 5.8,`BranchName=UE5`,Editor + UBT 已编译,是 source build(无 `Engine\Build\InstalledBuild.txt`)。**(源码版 5.8 本机不存在,此为原机记录,未在 5.8 重测。)**
3. **网络兼容(Launcher 侧 5.8.0 本机实测,源码版 5.8 待实测)**:
   | 字段 | Launcher(本机 5.8.0 实测) | 源码版(5.8 待实测) | 影响联机 |
   |---|---|---|---|
   | Major/Minor/Patch | 5.8.0 | 待实测 | 5.8 起由 override 判定 |
   | **CompatibleChangelist** | **0**(回退用 `Changelist`) | 待实测 | 5.8 已不参与判定 |
   | IsLicenseeVersion | 0 | 待实测 | — |
   | Changelist | 55116800 | 待实测 | — |
   | BranchName | `++UE5+Release-5.8` | 待实测 | — |
   - 5.8 起客户端覆盖 `FNetworkVersion`,联机不再看两端 CL 是否一致(见 §7 顶部说明与 `agones-dev.md §2.5`);5.7 时代记录为两端 `CompatibleChangelist` 均 `47537391`、版本 `5.7.4`,已失效。
4. **Linux 工具链已装**:机器级 `LINUX_MULTIARCH_ROOT = C:\UnrealToolchains\v26_clang-20.1.8-rockylinux8`。
5. **客户端工程**:`C:\work\Pandora-Client-SVN\Pandora\Pandora.uproject`,已有 `Pandora`(Game)/`PandoraEditor`/`PandoraServer`(`Type=TargetType.Server`)。
   当前本机约定:`F:\work\Pandora-Client-SVN\Pandora` 用源码引擎出 WindowsServer DS 包,`C:\work\Pandora-Client-SVN\Pandora` 用发行版 Editor/客户端登录、匹配、进战斗。
6. **DS 打包脚本**:`C:\work\Pandora-Client-SVN\Tool\Server\Agones\build-linux-ds.ps1`,已改成自动锁定源码版引擎(扫 HKCU `Builds` 里无 `InstalledBuild.txt` 的那个);**未改** `Pandora.uproject` 的 `EngineAssociation`(保持 `"5.8"`,不把本机 GUID 提交进 SVN)。
7. **Windows local DS**:`local` 模式不能用 `Pandora\Binaries\Win64\PandoraServer.exe` 裸二进制,必须用 cook/stage/pak 后的 WindowsServer 包(由 Pandora-Client / UE 侧打包流水线产出,后端仓库不再维护 DS 编译脚本),再把 hub/ds allocator 指到 staged `PandoraServer.exe` 与 staged 根目录;缺 cooked 内容会出现 AssetRegistry/BufferReader 崩溃。

### 7.2 引擎选型纪律

- **现阶段(个人打通 DS 链路)**:客户端用 Launcher 出 Win64 包,DS 用源码版 `D:\UnrealEngine` 打 Linux Server。已验证兼容,**唯一红线:不改 `D:\UnrealEngine` 引擎源码**(改了 `CompatibleChangelist` 不再可靠,必须客户端也同源出包)。
- **团队规模化**:用源码版产一个 `WithServer=true` 的 **Installed Build**(标准做法,大团队主流,经 CI/构建农场分发);届时**客户端也切到同一个 Installed Build 出包**,单引擎天然兼容。
- **Installed Build 只能用源码版 `D:\UnrealEngine` 产**,Launcher 版是成品、无源码无构建能力,不能当母机。

### 7.3 交接话术:用源码版产支持 Server 的 Installed Build

> 新会话(如 Claude Code)做 Installed Build 时,整段复制下面给它:

```
你接手 Pandora 项目的一个 UE 引擎构建任务:用源码版引擎产出一个支持 Dedicated Server 的 Installed Build,供团队和 CI 后续消费。开工前先读项目根的 AGENTS.md / CLAUDE.md(尤其 §11.1 跨 AI 分工:改本机环境、装工具、跑重型构建这类动作要先和用户确认,由用户/Codex 执行;你负责方案、脚本、项目内验证)。

## 背景(已由上一会话实测确认,不要重复怀疑)

1. 目标:Epic Launcher 发行版引擎(D:\Program Files\Epic Games\UE_5.8)官方设计上不支持构建 Dedicated Server target(InstalledPlatforms 里只有 PlatformType=Editor/Game,无 Server)。已确认无法靠勾 optional component 解决。
2. 源码版引擎:D:\UnrealEngine,UE 5.8,BranchName=UE5,Editor 和 UBT 已编译完成,是 source build(无 Engine\Build\InstalledBuild.txt 标记)。它支持 Server target。
3. 两个引擎 Build.version 已比对,网络兼容:
   - Launcher:CompatibleChangelist = 47537391
   - 源码版  :CompatibleChangelist = 47537391
   - 联机握手(FNetworkVersion)看 CompatibleChangelist + 5.8 + IsLicenseeVersion=0,三者一致 = 兼容。
4. Linux 交叉编译工具链已装好,机器级环境变量 LINUX_MULTIARCH_ROOT = C:\UnrealToolchains\v26_clang-20.1.8-rockylinux8。
5. 客户端工程:C:\work\Pandora-Client-SVN\Pandora\Pandora.uproject,已有 Target:Pandora(Game)、PandoraEditor、PandoraServer(Type=TargetType.Server)。
6. 后端 DS 打包脚本:C:\work\Pandora-Client-SVN\Tool\Server\Agones\build-linux-ds.ps1,已改成自动锁定源码版引擎(扫 HKCU Builds 里无 InstalledBuild.txt 的那个)。

## 本次任务

用源码版 D:\UnrealEngine 产出一个 Installed Build,要求:
- 含 Win64(Editor+Game)
- 含 Linux 平台支持
- 含 Server target 支持(关键:-set:WithServer=true)
- 关键纪律:产出的 Installed Build 的 Build.version 里 CompatibleChangelist 必须仍为 47537391,以便和现有 Launcher 客户端联机兼容。不要改引擎源码,不要 sync 到别的 changelist。

官方机制是跑 BuildGraph:
  D:\UnrealEngine\Engine\Build\BatchFiles\RunUAT.bat BuildGraph ^
    -Script=Engine\Build\InstalledEngineBuild.xml ^
    -Target="Make Installed Build Win64" ^
    -set:WithWin64=true -set:WithLinux=true -set:WithServer=true ^
    -set:WithClient=true -set:WithDDC=false
产物默认在 D:\UnrealEngine\LocalBuilds\Engine\Windows。

## 要你做的事(按顺序,每步先说明再执行)

1. 先只读核对:打印 D:\UnrealEngine\Engine\Build\InstalledEngineBuild.xml 里可用的 -set 选项(不同 5.8 版本开关名可能不同,如 WithServer / WithLinux / HostPlatformOnly 等),确认正确的参数名,不要照抄上面命令就跑。
2. 给出最终 BuildGraph 命令草案 + 预计耗时 + 产物大小 + 磁盘占用,交用户确认后再执行(这是改本机环境的重活,按 §11.1 需用户授权)。
3. 用户授权后执行 BuildGraph,全程留意失败立即停并报告(不要自动重试、不要绕过失败)。
4. 产出后校验:打印 LocalBuilds\Engine\Windows\Engine\Build\Build.version,确认 CompatibleChangelist == 47537391;确认 Engine\Config\BaseEngine.ini 的 InstalledPlatforms 里出现 PlatformType="Server"。
5. 用产出的 Installed Build 验证能编 Server target:
   <InstalledBuild>\Engine\Build\BatchFiles\Build.bat PandoraServer Linux Development -project="C:\work\Pandora-Client-SVN\Pandora\Pandora.uproject"
6. 汇报:产物路径、Build.version 校验结果、Server 平台是否出现、Server target 是否编过、磁盘占用、剩余风险。

## 红线(触发就停下问用户)

- 需要改引擎源码、sync 别的 CL、装/升级工具、改系统环境
- BuildGraph 失败
- 产出的 CompatibleChangelist != 47537391(会导致和 Launcher 客户端不兼容)
- 要动 Pandora.uproject 的 EngineAssociation(不要提交本机 GUID 进 SVN)

先读 AGENTS.md / CLAUDE.md,然后从第 1 步(只读核对 InstalledEngineBuild.xml 的开关)开始。
```
---

## §8 GM 指令链路交接(2026-07-07)

### 8.1 已完成代码范围

服务端 `XuanMing-Server`:

- 新增 `proto/pandora/gm/v1/gm.proto`,并同步生成 Go/C++ 产物到 `proto/gen/go/pandora/gm/` 与 `proto/gen/cpp/pandora/gm/`。
- 新增 `services/battle/ds_allocator/internal/gm/`,实现 GmService 与单测。
- 修改 `services/battle/ds_allocator/internal/server/grpc.go` 注册 GmService。
- 修改 `services/battle/ds_allocator/cmd/ds_allocator/main.go` 构造 gmSvc。
- 新增 `services/battle/ds_allocator/cmd/gmctl/`,作为 `SendCommand` 运维 CLI。
- 修改 `services/battle/ds_allocator/go.mod`,将 `github.com/google/uuid` 提为直接依赖。

UE `Pandora-Client-SVN`:

- 同步 `Source/PandoraProto/Public/Generated/Proto/pandora/gm/v1/gm.pb.h`。
- 同步 `Source/ThirdParty/PandoraProtoGenerated/pandora/gm/v1/gm.pb.cc`。
- 新增 `Source/PandoraProto/Private/Generated/Proto/PandoraGeneratedProto_0026.cpp`,聚合 include `gm.pb.cc`。
- 修改 `Source/PandoraProto/Public/Codec/PandoraWireTypes.h` 与 `PandoraMessageCodec.h`,增加 GM wire 结构与声明。
- 新增 `Source/PandoraProto/Private/Codec/PandoraMessageCodec_Gm.cpp`。
- 修改 `Source/Pandora/Public/Net/PandoraDSBackendSubsystem.h` 与 `Private/Net/PandoraDSBackendSubsystem.cpp`,实现轮询、执行与 Ack。
- 修改 `Source/Pandora/Public/Gameplay/Default/PandoraDSGmCVars.h` 与 `.cpp`,让 `AddItem` 返回 bool。

### 8.2 Codex 已完成验证

2026-07-07 已在 `services/battle/ds_allocator` 执行:

```powershell
go mod tidy
go build ./...
go vet ./...
go test ./internal/gm/...
go build ./cmd/gmctl
```

结果:全部通过。`go mod tidy` 后 `go.sum` 无新增变化;`go.mod` 的预期差异是 `github.com/google/uuid v1.6.0` 从 indirect 变为直接依赖。

### 8.3 剩余必须由 UE/编辑器侧完成

1. 重新生成 UE 工程文件,让 UBT 识别新增 `.cpp`。
2. 编译 `PandoraProto` 与 `Pandora` 两个 module。
3. 同一份源码同时重打 Client 与 Linux DS 包,避免 `NetChecksumMismatch`。
4. 按既有流程替换 `XuanMing-Server/run/ue-ds-archive*` 中的 DS 归档,并部署到 dev DS。
5. 起 ds_allocator 与一局战斗 DS 后,使用真实在线玩家执行端到端冒烟:

```powershell
cd f:\work\XuanMing-Server\services\battle\ds_allocator
go run ./cmd/gmctl additem --match <matchID> --player <playerID> --config <真实道具ID> --count 1
```

期望:

- gmctl 输出 `已入队:... idempotency_key=<UUID>`。
- ds_allocator 日志出现 `gm_command_enqueued` -> `gm_commands_delivered` -> `gm_command_acked`。
- DS 日志出现 `[DS][GM] 已添加道具...` 与 `GM command handled: ... ok=1`。
- 玩家背包实际到账。

边界验证:

- 错误 `--config` 应返回 Ack `ok=0` / `add-item-failed`,队列不阻塞。
- `gmctl` 每次都会生成新的 `idempotency_key`;重复运行命令会发两条真实指令。DS 去重只用于防止同一条指令被重复投递。

### 8.4 语义和部署约束

- GM 指令送达语义是 at-most-once:Redis `RPOP` 取出即出队,DS 拉取后宕机会丢,不自动重投。
- gmctl 是内网运维 CLI,直连 ds_allocator,不经 Envoy,不能暴露给玩家客户端。
- gmctl 默认地址是 `127.0.0.1:20020`;远程使用 `--addr host:20020` 或环境变量 `PANDORA_DS_ALLOCATOR_ADDR`。
- `PandoraProto` module 仍需保持 RTTI/异常/无 unity/NoPCHs 约束;protobuf 头只应在 codec `.cpp` 中出现,不要外泄到普通 UE 业务头。

---

## §9 排行榜榜外区间估算交接(2026-07-21)

### 9.1 已完成代码范围(Claude,测试全绿)

头部精确 + 尾部估算:精确榜由 `max_size` 截断(可配,如 10 万),被截断玩家 `GetRank` 回退分数直方图区间估算名次。设计见 `docs/design/decision-revisit-leaderboard.md` §3.4。

- `proto/pandora/leaderboard/v1/leaderboard.proto`:`BoardOptions.estimate_bucket_width=5`、`GetRankResponse.estimated=4` / `total_submitters=5`(纯加字段,双向兼容)**[proto]**
- `services/runtime/leaderboard/internal/data/board_store.go`:新增全员分 `:s` / 直方图 `:h` 两个 HASH(同 hashtag slot);submitLua 重写(旧分从 `:s` 读、直方图旧桶-1 新桶+1、截断只清 `:z`/`:t`);`Estimate` 方法;`Remove` 改 Lua 回扣直方图;`Clear`/`Delete`/TTL 覆盖新 key。**顺带修复原截断缺陷:出榜玩家 INCREMENT 累计分从 0 重算、SET_IF_HIGHER 可放进更低分**。
- biz `GetRank` 返回 `RankView`(精确 / 估算 / 未上报三态);conf 加 `default_estimate_bucket_width`(默认 25);service 层接线新 pb 字段。
- 测试:data 层 11 个新用例(截断保分、估算升降序、钳制、Remove/Clear/TTL、桶宽不可变、旧榜回退补记)+ biz `TestGetRank_EstimatedFallback`,`go test ./internal/data ./internal/biz` 全绿。

### 9.2 Codex 待办(阻塞整模块编译)——已完成

1. ~~跑 `pwsh tools/scripts/proto_gen.ps1` 重生 leaderboard pb~~ ✅ Codex 已重生(新字段 getter 已在 gen/go)。
2. ~~重生后确认全绿~~ ✅ Claude 复验(2026-07-21):leaderboard 模块 `go build ./... && go vet ./... && go test ./...` 全绿,proto 模块 build OK。
3. cpp pb 同步 UE 仓库(常规 [proto] 流程;客户端展示估算名次须做粗粒度文案,禁止假精度)——**如未同步仍待办**;UE 侧榜单 UI 接 `estimated` / `total_submitters` 时一并处理。

### 9.3 语义约束(review 红线)

- `:s`/`:h` 是非权威可重建派生状态(§22),禁止用于结算 / 发奖 / 任何权威写;SettleBoard 仍只读精确 ZSET Top-N。
- 桶宽建榜定死不可变;估算名次钳制到 ZCARD+1 之后,不与精确区打架。
- 直方图增量计数存在理论漂移,当前不加重建 job(§15.3);永久榜压测若漂移可感知,补「从 :s 重算 :h」低峰任务。

---

## §10 Battle DS 双阈值空场回收交接(2026-08-07)

### 10.1 背景

`docs/design/anti-abuse-scene-entry.md` §3.2.1:「进副本→退出→再进」能把一台 14Gi 的 Battle DS
押约 6 分钟(冷启动 + 5min 空场兜底),`maxReplicas` 个小号即可押死整个 Fleet,速率比正常玩家还低,
**任何频率闸都拦不住**。同一处还有个 §9.20 卡玩家 bug:正常玩家强退后被 `ensureNoneInBattle`
锁最多 5 分钟,只看到 `ErrMatchInBattle(4007)`。缩短「从未连入」局的持有时间**一个修同时解决两件事**。

### 10.2 已完成代码范围(Claude;2026-08-10 复检:`go build` + `go test ./services/battle/ds_allocator/... -count=1` 全绿)

把空场回收拆成两档:`ever_had_players=false`(no-show)走短阈值,其余走原 `empty_battle_timeout`。

- `proto/pandora/ds/v1/allocator.proto`:`BattleStorageRecord.ever_had_players = 21`(纯加字段,§9.17 双向兼容)**[proto]**
- `services/battle/ds_allocator/internal/conf/conf.go`:`no_show_battle_timeout` +
  `DefaultNoShowBattleTimeout=150s` / `NoShowTimeoutFloor=60s` + `ResolveNoShowTimeout()`
- `internal/biz/allocator.go`:`heartbeatLegacy` 按位选阈值;判弃日志分 `reason=no_show` / `all_disconnected`
- `internal/data/battle_auth.go`:`ActivateHeartbeat`(**Model B 生产路径**)同款双阈值 + `BattleHeartbeatInput.NoShowTimeout`
- 新增 6 个单测(biz 4 + conf 2),**均未运行**

### 10.3 Codex 待办(阻塞本模块编译)

1. ~~跑 `pwsh tools/scripts/proto_gen.ps1` 重生 ds pb~~ ✅ Codex 已重生并提交(`9e07b875`,
   `EverHadPlayers` / `GetEverHadPlayers()` 已在 gen/go),build 已验证不红。
2. 无需 `go mod tidy`(未引入新依赖)。
3. cpp pb 无需同步(存储侧字段,UE 不可见)。

### 10.4 语义约束(review 红线)

- **两条心跳路径必须同时改**:`heartbeatLegacy`(`!RedisAuthorityEnabled()`)和 `ActivateHeartbeat`(Model B)。
  只改一条 = 线上没生效。
- **阈值解析结果绝不允许是 0**:判定是 `timeout > 0 && ...`,0 会让 no-show 局**永不回收**,比不改还糟。
  fail-safe 方向统一为「宁可回收得晚,不可不回收」。
- **不要缩 `heartbeat_timeout`** 来达成类似效果:空场回收与失联回收是两条独立时钟,
  INC-20260727-001 的根因正是单阈值同时管启动与稳态。
- `ever_had_players` 一经置位**永不清零**,保证「9 人在打、1 人掉线」绝不会走 no-show 短档。
- 回滚开关:`no_show_battle_timeout: -1s` = 禁用差异化,退回改动前单阈值行为。

### 10.5 仍未验证(§6 第 0 项不算验收完成)

- ~~未编译、未跑测试~~ ✅ 2026-08-10 复检:ds_allocator 全包 build 绿,全套件测试绿,
  新增 6 个单测(biz 4 + conf 2,含 `StickyAcrossDisconnect` 防误伤护栏)逐个 PASS
- 未做故障注入验证真实断线玩家在重连窗内不被误判
- 未实测「DS 报 ready → 客户端完成 Admission」P99 来复核 150s 默认值(该值目前是**有推导依据的保守初值**,不是实测值)
- 未给出「单次进场占用 Pod·分钟」前后对比表

---

## §11 注册编号首次会话补拉交接(2026-08-10)

### 11.1 服务端状态

- 新增 `LoginService.GetRegisterNo`:空请求,玩家身份只取 JWT `sub` 经 Envoy 注入的
  `x-pandora-player-id`;请求体不接受自报身份。
- `code=OK, register_no=0` 是约 15s 补号窗口内的正常态;仓储查询错误返回非 OK,
  客户端不得把错误覆盖成 0。服务端刻意不做 sjti 现行性复核,因为该接口只读、只能读自己、
  零副作用且不签发凭据。
- `deploy/envoy/envoy.yaml` 已加入 `/pandora.login.v1.LoginService/GetRegisterNo` exact JWT rule。
  `jwt_authn.rules` 未命中的 path 默认放行但不注入玩家头,漏配会让接口一律未授权。
- `register_no_counter` 保留 `MaxRows: 8`,取消 `MaxAvgRowBytes`:单行 InnoDB 表的
  `information_schema` 平均行长受 16KiB 页分配影响,不代表两列约 9B 的逻辑载荷,此前会恒刷
  `db_capacity_budget_exceeded actual=16384` 噪声。
- 实现方报告 login `go build` / `go vet` / `go test ./... -count=1` 全绿;本次文档交接
  **未独立复跑**。设计见 `docs/design/register-no-and-login-surge.md` §3.7,进度见
  `PROGRESS.md` 2026-08-10 同日补充。

### 11.2 当前快照与提交边界

- 2026-08-10 08:45 并发提交 `11320853` 已纳入本轮新 RPC、Go pb、Envoy、业务实现和
  `login_register_no_rpc_test.go`;服务端 `proto/` 当前干净,可作为客户端生成输入。
- `11320853` 的标题是 `feat(server): 完成防滥用配额与注册编号补拉`,**遗漏仓库规范要求的
  `[proto]` 标记**。推送前须由人决定 amend 或明确记录例外;Codex 不得在未授权时改提交历史。
- 客户端已用官方 `Tool/Protobuf/GenClientProto.ps1` 对 `11320853` 执行 `-UpdateLock`,并以
  同参数 `-VerifyOnly` 复验通过。协议锁已推进到 `11320853`;实际只有 login `.pb.h`、
  login `.pb.cc`、`ClientProto.lock.json` 三个生成文件变化。
- 提交后工作树仍有并行的 configtable 生成物修改;后续不能 `git add -A`。

### 11.3 UE 客户端已落码与验证边界

- `PandoraLoginClient` 已接 `/pandora.login.v1.LoginService/GetRegisterNo`:请求体为空且
  `bWithAuth=true`;codec 保留 `OK+0` 正常态,响应缺 data frame、解析失败或业务/传输失败均显式
  非 OK,不会伪装成编号 0。
- `MyAccountModel` 在登录拿到 0 后立即发起并用 CoreTicker 每 3s + 最多 0.75s jitter 单飞补拉;
  `OK+0` 继续显示“生成中”,非 0 写回并停止。真实错误停止自动补拉并显示失败,由角色界面刷新按钮
  显式重试,不会无限空等。
- 写回通过 `PandoraBackendSubsystem::TrySetRegisterNo`,绑定发起时的 attempt、
  `SessionGeneration`、`PlayerId` 三重围栏;不推进会话世代。登出、切号、重连放弃、
  `Deinitialize` 后的迟到结果均无副作用。
- `MyRoleInfoView` 已展示非 0 编号、“注册编号 生成中”和“注册编号 获取失败，点击刷新”三态;
  Widget 重用时会重新绑定刷新按钮。
- 新增三类 Automation 覆盖 codec 契约、当前会话写回围栏、RPC/source 契约(含空请求、JWT auth、
  CoreTicker/三围栏、错误不重试和 Widget 重绑)。本任务修改的 C++ 源码已编译,
  `UnrealEditor-PandoraTests.dll` 已链接成功。
- **完整目标仍未全绿**:`PandoraEditor` 最终链接被无关并行改动 `MyMainView.cpp/.h` 阻断——
  其中已经声明并调用、但未实现 `EnsureClientPerfWidget()` / `RefreshClientPerf(float)`,触发
  LNK2019/LNK1120。本交接不越权修改该并行功能。随后 Editor 可加载旧 `Pandora.dll` /
  `PandoraProto`,但加载新链接的 `PandoraTests.dll` 失败(`GetLastError=127`,game module
  `PandoraTests` could not be loaded):新测试 DLL 引用了本轮新增导出,旧主 DLL 不具备。因此三组
  Automation **均未实际执行**,不是测试断言失败。
- 客户端本轮未 SVN commit、未 push。服务端 `11320853` 也未由 Codex amend;仍需人决定缺失
  `[proto]` 标记的处理方式。最终验收还需完整 UE 目标编译通过，以及真实 Envoy/JWT 新账号 E2E
  验证“生成中”无需重登收敛为非 0且不再请求。

### 11.4 部署与排障口径

- 当前 K8s dev 生成档 `run/cluster/etc/login.yaml` 连接集群 `mysql:3306/pandora_account`,
  不是本机 Docker TiDB `:4000`。宿主 `login-dev.yaml` 走 Docker MySQL `:3307`;
  只有显式使用 `login-dev-tidb.yaml` 才走 TiDB `:4000`。
- 查列或账号前先核对生效配置并执行 `SELECT VERSION(), DATABASE()`。实现方现场记录账号
  `test123` 在实际 MySQL 中 `register_no=1`;该值是排障样本,不是本轮重新验证的运行态断言。
- Envoy 源码 rule 已加,但仍需 `envoy --mode validate -c deploy/envoy/envoy.yaml`、重启/灌入
  ConfigMap 与真实 JWT 请求验收;Go 全绿不能证明网关运行态生效。

### 11.5 已知待复核边界

`AccountRepo.GetRegisterNo` 当前把 `sql.ErrNoRows` 和 `register_no IS NULL` 都映射成
`0,nil`。在“有效 JWT 对应账号行必然存在”的现有不变量下不影响正常路径;若账号行异常缺失,
客户端仍会 `OK+0` 空等。未来允许删号或排查到缺行时,必须把 no-row 改为非 OK,不能继续冒充补号中。

### 11.6 对抗性复审(2026-08-10,6 路 agent)结论与修复

复审横跨 matchmaker 不变量 / fail-open 全面审计 / no-show 记罚 / login+Envoy 安全链 /
配置与文档一致性(Envoy 配置有效性 agent 中途断线,其发现由其余 agent 交叉覆盖)。
**0 个 P0**;大量 clean 确认了核心 fail-open / 零副作用 / 退出路径零波及纪律正确。

**已修复(9 项)**:
1. **[P1] 守卫退队**(match.go / data/match.go):`RequeueTicketIfOwned` 镜像 ReserveTicket 的
   WATCH CAS——票据被并发 CancelMatch 删除后不再盲写复活(避免已取消玩家被重新凑局 + 误记
   no-show 罚)。failMatch 与 rollbackReservations 两个调用点已切换,3 个回归测试。
   注:此竞态**先于本轮存在**(failMatch 一直盲写),onMatchNoCapacity 放大了暴露面,一并修掉。
2. **[P1] login 账号大小写绕过**(login_ratelimit.go):hashAccount 前先 `ToLower+TrimSpace`
   归一化,对齐 utf8mb4_0900_ai_ci;否则 alice/Alice/ALICE 各享独立失败预算。回归测试锁定。
3. **[P2] §9.6 DS 自报 abandoned**(allocator service):新增 `sanitizeReportedState` 白名单——
   DS 只能报 warming/ready/running/ended,"abandoned"(后端专属判决)归一化为空串;堵住
   「DS 输入直接铸造 no-show 处罚」的将来陷阱,两条心跳路径语义对齐。
4. **[P2] login 部分故障放大**(LockRemaining):账号/IP 两维度独立读、各自 fail-open,
   一维读失败不再短路掉另一维已读到的锁(撞库时 IP 锁常是唯一防线)。
5. **[P2] login 续锁攻击**(RecordFailure):布锁时清零该维度计数——否则计数窗(15m)>锁窗(5m)
   时锁到期后单次失败即重锁。回归测试锁定「单次失败不重锁」。
6. **[P2] team_id 骚扰面**(entry_limiter.go):StartMatch 冷却只按 JWT 的 captain_id 计
   (自限),删掉按未校验 team_id 占坑的 per-队伍窗(那是「刷任意 team_id 压制他人队伍」原语)。
7. **[P2] 配置注释失实**(matchmaker/ds_allocator/login conf.go 共 6 字段):`<=0 关闭` 改为
   `负值关闭,0=用默认`,与 Defaults 实际语义(==0 用默认)一致。
8. **[P2] Envoy :8444 纵深**:DS 面 header_mutation 一并入站剥离 x-pandora-client-ip。
9. **[P2] 文档键名漂移**:anti-abuse §3.3 的 hub 冷却键 `transfer:cd:{}` 更正为代码实际的
   `transfer_cd:<>`。

**已评估、判定为可接受并文档化(不修,附依据)**:
- **onMatchNoCapacity 补偿重放会闪 FAILED**(match.go 重放路径走 plain failMatch):
  「不闪 FAILED」保证只在首攻补偿成功时成立;崩溃/Redis 抖动的重放路径会推一次 FAILED
  (无倒计时),客户端经 ResolvePlayerMatchContext 恢复,**不卡死(§9.20 不破)**。彻底修需在
  match 记录上持久化「容量耗尽 reason」——那要加 proto 字段,与本轮「零 proto 改动」冲突,留待
  下次 proto 批次。
- **no-show 采样窗口漏连**(battle_auth.go:两跳心跳<5s 间完成连入并崩溃 → EverHadPlayers 恒 false):
  窗口极窄 + 首次免罚兜底;彻底闭合需判 no_show 前只读核验 departure journal,改动更大,记为
  已知窄边界。
- **IPv6 轮换 / CGNAT 连坐**(IP 维度按精确 IP):IP 限流的固有取舍;主威胁由账号维度 + Envoy
  边缘桶 + 生产关自动注册(§7 真正杠杆)覆盖。/64 聚合属将来可配项。
- **FirstAbandon 排在可失败步骤后**(Model B 依赖错误会吞掉记罚):方向是 fail-open「少罚」,
  安全;不值得为它重排删除延迟收尾链。
- **no-show 拒绝在 BeginTeamMatch roster 租约之后**(组队路径非严格零副作用):租约按
  operationID 幂等、秒级自净,拒绝前无 DS 分配 / durable operation / 流水等真持久副作用。
- **anyTicketInFormCooldown 满载期 O(队列长) 串行 PTTL**:纯成本/节拍(fail-open、探测在
  CreateMatch 前无副作用);满载恢复瞬间的时延放大,可后续加「本 tick 探测记忆」优化,非正确性。
- **Envoy 桶值 100/50rps**:已标「待压测复核」。
