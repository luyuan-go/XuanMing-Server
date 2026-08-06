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
