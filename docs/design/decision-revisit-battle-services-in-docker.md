# decision-revisit:含战斗版把 go 业务服务搬进 docker(只留两个 allocator 在宿主)

> 触发:用户提出「策划不需要自己起 go 服务;含战斗版的 go 服务也想直接跑进 docker;
> 宿主(Windows)go 服务只在本机断点调试时才用」。本文档按 `AGENTS.md` §5/§7 给出
> 旧问题 / 硬约束 / 新方案 / 风险 / 迁移成本 / 验收,并记录方案 A 第一版编排落地边界。
> 决策级别:编排 / 运维链路(start.ps1 / run_services.ps1 / docker-compose),不改业务逻辑与不变量。
>
> 状态:**已被推翻(2026-07-14)** —— battle 混合模式退役:Windows DS 只保留给 `-Mode local`
> 断点调试,其他要真 DS 的场景一律 k8s + Agones(Linux DS)。含战斗双击入口已删,
> `-Mode battle` 仅剩 `-Down`/`-Status` 用于清理遗留环境。
> 见 `decision-revisit-retire-battle-mode.md`。以下为历史记录。
>
> 历史状态:已拍板,方案 A 第一版已落地,待真 DS 端到端验收(2026-07-01 分析,同日实施)。
> 决策:采用**混合模式(方案 1)** —— 17 个业务服务进 docker,`ds_allocator` + `hub_allocator`
> 留宿主(仍靠宿主 `go build` 拉起,不分发预编译 exe)。已改 5 个编排文件,见文末「落地记录」。

---

## 1. 旧问题(改造前)

改造前四个双击入口都走 `play.ps1 -Battle[ -Intranet]` → `start.ps1 -Mode local`,
`local` 模式把**全部 19 个 go 服务以宿主进程(host `go build` + 运行)**拉起:

| 模式 | go 服务跑哪 | 服务器机器要装 | 能进真实 DS |
|---|---|---|---|
| `docker` / `intranet` | 19 个全在容器(`docker-compose.services.yml`) | 只要 Docker | ❌ DS=mock |
| `local`(**改造前的含战斗**) | **19 个全跑宿主**(要 Go + build) | Docker + **Go** | ✅ 真 DS |
| `k8s` / `online` | 容器 / 集群 | — | ✅ Agones |

痛点:改造前含战斗那台「服务器机器」被迫装 Go 并每次 host `go build`。而 `local` 存在的**正当理由本是
「宿主进程可断点调试」**——含战斗版并不需要调试,只是被 DS 拉起方式**连累**跟着全宿主。

## 2. 硬约束(为什么不能 100% 全 docker)⚠️

- 战斗 DS / 大厅 DS 是 **Windows 的 `PandoraServer.exe`**,**跑不进 Linux 容器**。
- `ds_allocator`(mode=local)与 `hub_allocator` 要**在本机 `exec` 拉起这个 `.exe`**
  (`services/battle/ds_allocator/internal/data/local_allocator.go`;hub 侧懒拉起 UDP :7777)。
- Linux 容器里的进程**无法直接拉起宿主的 Windows 程序**。

**结论**:只要要「进真实战斗/大厅 DS」,就**必须有宿主侧进程**去 exec Windows DS。
能搬进 docker 的是**17 个纯业务服务**,`ds_allocator` + `hub_allocator` **必须留宿主**。

## 3. 关键事实:混合模式的跨边界通道已存在

- 容器版服务间发现:靠生成的 `run/cluster/etc/*.yaml`(`gen_cluster_config.ps1`),
  地址用 docker DNS 名(`ds-allocator:20020` / `hub-allocator:20021`)。
- 「容器 → 宿主」通道**仓库里已在用**:`docker-compose.dev.yml` 的 envoy 用
  `extra_hosts: "host.docker.internal:host-gateway"` + `host.docker.internal` 访问宿主服务
  (见 dev.yml line 179-199,注释明写「上游业务服跑在宿主,通过 host.docker.internal 访问」)。
- 基础设施(redis/mysql/kafka/etcd)已 `ports:` 发布到宿主 `localhost`,宿主 allocator 直接连得到。
- DS 返回给客户端的地址已由 `PANDORA_DS_ADVERTISE_HOST` 注入(local 战斗链路现成)。

## 4. 新方案(推荐:方案 A —— 混合模式 `battle-docker`)

新增一种编排:**17 个业务服务进 docker(复用 docker 模式镜像),`ds_allocator` + `hub_allocator`
留宿主进程**(它们负责 exec Windows DS)。

改动点(编排层,不动业务代码):

1. **`start.ps1` 新增 `battle` 模式**:含战斗时业务服务用 `docker-compose.services.yml` 起,
   但 `ds-allocator` / `hub-allocator` 两个 service **不进 compose**(profile 排除或 override),
   改由宿主进程启动这两个 allocator。`run_services.ps1` 已补 `-Only` 多服务筛选,
   `battle` 模式用 `-Only ds_allocator,hub_allocator` 只起宿主 allocator。
2. **容器服务指向宿主 allocator**:生成 cluster 配置时,把 matchmaker→ds_allocator 的地址、
   login→hub_allocator 的地址(`login.hub.addr`)改成 `host.docker.internal:20020/20021`;
   `docker-compose.services.yml` 相关容器补 `extra_hosts: host.docker.internal:host-gateway`
   (Windows/Mac 自带,Linux/WSL 需显式,同 envoy 现有写法)。
3. **宿主 allocator 的两类地址必须分开**:
   - allocator 自己连 redis/mysql/etcd 仍用宿主 `localhost:<published>`。
   - 容器里的 login/matchmaker 拨 allocator 时,配置 endpoint 必须是
     `host.docker.internal:20020/20021`(而非 `127.0.0.1`),避免容器拨到自己。
   - allocator 返回给 UE 客户端的 **Hub/Battle DS 地址**继续走
     `PANDORA_DS_ADVERTISE_HOST` / `local_hub.advertise_host` / `local_ds.advertise_host`,
     本机用 `127.0.0.1`,内网多人用服务器局域网 IP,不能误写成 `host.docker.internal`。
   当前服务间调用是直连配置,不是 allocator 自注册服务发现;若以后接注册中心,也必须把
   “容器可达的 allocator RPC 地址”和“玩家可达的 DS NetDriver 地址”拆成两个字段。
4. **服务器机器不再强制装 Go**(可选进阶):把两个 allocator **预编译成 exe** 随包分发,
   宿主免 Go;或维持 `go build`(仅这两个服务,构建量远小于 19 个)。见 §6。

> 实际落地模式名为 `battle`,并由 `play.ps1 -Battle[ -Intranet]` 调用。四个双击入口语义不变,
> 底层从「19 全宿主」变「17 容器 + 2 宿主 allocator」。

### 否决/备选
- **方案 B(维持现状)**:含战斗那台装 Go + build 一次即可。零改动、零风险,但没解决用户诉求。
- **方案 C(全 docker 放弃真实战斗)**:含战斗改 DS=mock,等于取消「含战斗」入口价值。**否决**。
- **host 侧 DS 启动代理**:容器 allocator 通过一个宿主 agent(监听端口→spawn `.exe`)拉 DS。
  比方案 A 更「纯」(allocator 也进容器),但要新增 agent + 协议 + 生命周期管理,复杂度/风险显著更高。
  **暂否决**,除非后续要求 allocator 也容器化。

## 5. 风险

| 风险 | 说明 | 缓解 |
|---|---|---|
| 跨边界网络回归 | 容器→宿主 allocator 拨号、宿主 allocator→容器基础设施访问都跨边界 | 复用 envoy 现成 `host.docker.internal` 模式;起后端到端跑一局验证 |
| 地址语义混淆 | allocator RPC endpoint 需要容器可达,DS advertise 地址需要玩家可达,两者误共用会导致容器拨号失败或 UE 客户端连不上 DS | 配置字段/生成逻辑拆开;启动自检分别校验 allocator endpoint 与 DS advertise host |
| 破坏已验证 local 调试链路 | 不能动 `-Mode local`(断点调试仍要 19 全宿主) | 新增独立模式,`local` 保持不变 |
| Intranet(局域网多人) | 内网 IP 注入需同时对容器服务与宿主 allocator 生效 | `PANDORA_DS_ADVERTISE_HOST` 已贯穿;分别验证 |
| Windows/WSL host-gateway 差异 | Linux 需显式 extra_hosts | 已有 envoy 先例,统一补齐 |

## 6. 迁移成本

- 已改 **编排脚本**:`start.ps1`(新增 battle 模式)、`run_services.ps1`(只起两个 allocator)、
  `gen_cluster_config.ps1`(allocator 地址改 host.docker.internal)、
  `docker-compose.services.yml`(两个 allocator service 可排除 + 相关容器补 extra_hosts)、
  `play.ps1`(含战斗入口切到新模式,提示语更新)。均属编排层,不碰业务/proto/不变量。
- 可选:两个 allocator 预编译分发(免宿主 Go)——另立打包步骤,单独一个小任务。
- **无法在本环境端到端验证**(需真 Windows DS 包 + Docker + UE 客户端),故必须人工到端验证后才算完成。

## 7. 验收标准(落地后验收对照)

1. 含战斗那台机器:`docker ps` 见 17 个业务容器 + 基础设施;宿主仅 `ds_allocator`/`hub_allocator` 两进程。
2. **不装 Go 也能起**(若选预编译分发路径)/ 或只 build 两个 allocator。
3. UE 客户端登录 → 进大厅 Hub DS(UDP :7777)→ 匹配 → 进 Battle DS 打完一局,battle_result 落库一次(不变量 §2 幂等)。
4. `-Intranet`:局域网另一台策划客户端能连进同一大厅 + 战斗(advertise 内网 IP 生效)。
5. `-Mode local`(纯调试)链路**不受影响**,仍 19 全宿主可断点。
6. 更新 `PROGRESS.md` 追加条目;必要时更新四个 .cmd 与 `play.ps1` 的说明文案。

---

## 附:三点结论回应用户

- 「策划不需要启动 go 服务」→ ✅ 正确,策划机永远只是客户端连服务器 IP。
- 「Windows 宿主 go 服务只在调试时用」→ ✅ 对**纯业务服务**成立;但 `ds_allocator`/`hub_allocator`
  因需 exec Windows DS 属**例外**,含战斗时仍须留宿主。
- 「go 服务也进 docker」→ ✅ 已落地 **17/19 进 docker**(方案 A),但**做不到 100% 全 docker**
  (硬约束见 §2)。下一步按 §7 做真 Docker + Windows DS + UE 客户端端到端验收。

---

## 落地记录(2026-07-01 实施,方案 A 混合模式)

已按方案 A 改 5 个编排文件,新增 `battle` 模式(19 全宿主的 `local` 保持不变,仍供断点调试):

| 文件 | 改动 |
|---|---|
| `tools/scripts/start.ps1` | `ValidateSet` 加 `battle`;新增 `Invoke-Battle`(先清残留→dev_up→`gen_cluster_config.ps1 -HostAllocators`→`Build-AllImages -Only $containerSvcs` 只构 17 个→`docker compose up -d` 17 容器→`run_services.ps1 -Only ds_allocator,hub_allocator` 起 2 宿主);`Build-AllImages` 加 `-Only` 过滤;Resolve-Prerequisites / 主 switch / Invoke-Resume / Invoke-Reset / Show-Status 均补 `battle` 分支 |
| `tools/scripts/run_services.ps1` | 新增 `-Only <names>` 参数,`Get-TargetServices` 支持只起指定服务(用于只拉 2 个 allocator) |
| `tools/scripts/gen_cluster_config.ps1` | 新增 `-HostAllocators` 开关:把 20020/20021 的容器地址改成 `host.docker.internal`,令容器内 matchmaker/login/battle_result 回连宿主 allocator |
| `deploy/docker-compose.services.yml` | login / matchmaker / battle-result 补 `extra_hosts: host.docker.internal:host-gateway`(Linux/WSL 需要,Windows/纯 docker 无害) |
| `tools/scripts/play.ps1` | 含战斗入口 3 处 `-Mode local` → `-Mode battle`;提示文案更新为「17 容器 + 2 宿主 allocator」 |

**地址语义(§4.3)落实**:allocator 自己连 redis/mysql/etcd 用宿主 `localhost:<published>`;容器拨 allocator RPC 走 `host.docker.internal:20020/20021`;allocator 返回给 UE 客户端的 DS 地址仍走 `PANDORA_DS_ADVERTISE_HOST`(本机 127.0.0.1 / 内网局域网 IP),由 `play.ps1` 注入、宿主 allocator 子进程继承。

**选型**:采用「宿主 `go build` 这 2 个 allocator」而非预编译 exe 分发(用户选「要最好的」= 方案 1),故 `battle` 的 Resolve-Prerequisites 仍需 Go(仅为这 2 个服务)。

**验证边界(AGENTS.md §11.1)**:已完成项目内校验 —— 5 个文件语法无新增错误(仅存量 PSScriptAnalyzer 未批准动词告警),无 go 代码改动。**端到端验证需人工执行**:`play.ps1 -Battle`(或 `-Battle -Intranet`),对照 §7 跑通一局(登录→Hub DS UDP:7777→匹配→Battle DS→battle_result 落库一次)。
