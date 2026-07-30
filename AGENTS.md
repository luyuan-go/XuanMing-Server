# Pandora AI 协作守则

> 给本项目所有 AI Agent(Claude Code / Cursor / Copilot 等)的工作守则,人类开发者同样遵守。

## 1. 第一原则

**AI 没有跨会话记忆**。每次新会话动手前必须按序读完:

1. `PROGRESS.md` —— 当前进度
2. `CLAUDE.md` —— 项目规范
3. `docs/design/pandora-arch.md` —— 架构总图
4. `docs/design/<相关服务>.md` —— 任务相关设计
5. `git log -20 --oneline` —— 最近改动
6. 当前打开的 PR / Issue

**没读懂就动手 = 失忆人改代码**,会出大问题。

## 2. AI 能做的

写代码(go / UE C++ / proto / yaml / shell / ps1)/ 文档 / 测试;跑本地 build/test/lint;跑本地 docker-compose / kubectl(apply 受限,见 §3);建议 commit message 与 PR 描述;代码审查 / 设计评审;分析 stress_summarize 输出表。

## 3. AI 不能做的

- ❌ `git push` / `git tag`(人手动推);`git commit`(除非用户明说"帮我 commit")
- ❌ **Claude 系模型不装工具 / 不改本机环境 / 不做 git 收尾**(见 §11.1)
- ❌ 登录任何远端账号(GitHub / k8s / 云厂商 / 注册表);改 CI 凭证 / secrets
- ❌ 写 secret / token / 密码到 git 跟踪文件
- ❌ `kubectl apply` 到生产(只能本地 minikube / 指定 dev 集群);`docker push` 到 registry

## 4. AI 执行方式

默认 **直接执行**:读 §1 → 改代码/proto/yaml/脚本/文档 → 跑 build/test/lint → 汇报改动范围+验证+剩余风险 → 需 commit 时等人发话再由 Codex 执行(分工见 §11.1)。

PowerShell 优先使用 PowerShell 7。

出离线镜像包(`deploy/offline-images/pandora-images.tar`)前先判断现有包是否过期:若包生成时间之后有 `services/` / `pkg/` / 镜像相关脚本或 Dockerfile 改动,必须重出;否则不要浪费时间重出。需要重出时**优先宿主编译方案**:

```powershell
pwsh tools/scripts/export_images.ps1 -Build -BuildMode host
```

该路线由宿主 Go 交叉编译业务二进制,再仅用 Docker 封装/`docker save` 成离线镜像包,不走容器内 `go build` 慢路径。只有宿主 Go 不可用或 host 构建明确失败时,才报告原因并经人确认后改用容器内构建。

**tar 不入库**(2026-07-23 起):离线包发布走 `publish_offline_images.ps1`(git sha 版本戳 + 制品目录不可变发布),目标机用 `fetch_offline_images.ps1` 拉取;禁止把任何 tar `git add`/`svn add`。发布线见 `docs/design/release-pipeline.md`。

遇 §3 禁令、§10 红线,或要装/升级工具、改系统环境、写 secrets、碰生产、push/tag → **立刻停止报告**,等授权。

**大范围改动不设文件数硬上限**:只要方向标准、正确、比原方案更好,可放手做大范围重构 / 批量改动,不必因文件数多而停手;但完成后须在汇报里如实列出改动范围、动机与验证结果,方便 review。若属于推翻既有设计决策,仍按 §7 先写 `decision-revisit` 文档等人拍板。

**设计与实现优先简单、标准、直达**(细则见 `CLAUDE.md §15`):优先采用语言 / 框架 / 协议 / 基础设施的官方能力和业界通行模式,在满足正确性、安全性、性能与不停服等硬约束的前提下,选择概念最少、依赖最少、调用链最短、维护成本最低的方案。不得为了“以后可能需要”提前引入多层抽象、自研框架、额外中间件、复杂状态机或曲折兼容层;确需增加复杂度时,必须说明简单方案为何不满足、复杂度解决了什么已确认问题,并给出验证依据。

**实现必须全面排查隐蔽 bug 与分布式边界**(细则见 `CLAUDE.md §16`):不能只跑通 happy path。设计、实现和 review 必须覆盖并发竞争、重复请求、重试、超时、乱序、消息重复 / 丢失、部分成功、网络分区、依赖降级、进程崩溃 / 重启、多副本、滚动升级与补偿恢复;共享状态必须明确权威源、幂等键、事务 / 原子边界、fencing 与最终一致性条件。发现的问题必须修到闭环并补针对性测试;没有证据不得声称“无 bug”或“分布式安全”。

**Go 请求上下文不得逃逸到后台任务**(细则见 `CLAUDE.md §16.7`):gRPC / HTTP handler 的请求 `ctx` 包含 Kratos transport、metadata、鉴权头等请求级对象,禁止直接保存到长生命周期结构、放入队列或传给 goroutine;`context.WithoutCancel(ctx)` 只去掉取消 / deadline,**不会**剥离这些 Value,不能当作异步 detached context。fire-and-forget 必须从干净 context 派生,只白名单复制 trace / player / match / team 等不可变日志字段;下游鉴权和业务参数显式传值。server / client 共用 middleware 必须按本次调用方向互斥处理,client hop 绝不能写继承的 server `ReplyHeader`。涉及异步下游 RPC 的改动必须补双 transport 回归并在可用环境运行 `go test -race`。

**任何崩溃与 P0 必须独立建档**(细则见 `CLAUDE.md §16.9` 和 `docs/incidents/index.md`):发现 runtime fatal、未恢复 panic、OOM、CrashLoop、非预期进程退出,或造成玩家掉线/被踢/无法进场、数据错误、安全突破、脑裂、大面积不可用的 P0,必须在当次任务创建或更新 `docs/incidents/YYYY-MM-DD-p0-*.md` 并登记索引。文档必须保留脱敏原始证据、UTC 时间线、调用链与关键变量、根因/放大因素、全仓同类扫描、修复/部署/回滚/验证和剩余风险。代码已改、普通单测通过或 Pod 重新 Ready 都不能关闭 P0;缺 race、故障注入、实际部署产物或玩家路径验证时状态必须保持未关闭。

**状态使用优先查询唯一权威,不在各服务重复存**(细则见 `CLAUDE.md` §9 不变量 22):只持久化跨请求 / 重启仍影响正确性且无法可靠重建的最小权威事实或必要租约;可从权威事实计算的展示 / 派生状态按需查询计算。其他服务不得保存并信任影子副本;缓存只能用于加速,必须可过期 / 可重建且不能参与权威写决策。关键状态迁移必须用事务 / CAS / 条件更新原子完成,禁止非原子的“先查后存”;查询失败返回 UNKNOWN / UNAVAILABLE,不得冒充 OFFLINE、空闲或默认状态。`LOCATION_STATE_HUB` / `BATTLE` 等 TTL 位置只作 presence / 最近活跃投影,key miss 不证明玩家已离开旧 DS;玩家归属只由唯一 owner authority 维护最小 `owner_epoch + exact owner + operation_id + lease` 权威。旧 epoch 不能影响当前 owner / 业务,但可精确幂等清理只属于自己的残留;有权写还必须满足 lease 未过期。相关设计若仍把 locator TTL 写成 Hub↔Battle 路由权威,即属规范冲突,必须停止实现并先报告 / 同步文档。

**脑裂下同一玩家最多只能在一个可玩 DS**(细则见 `CLAUDE.md` §9 不变量 22):唯一 owner authority 及底层必须提供线性一致 CAS、法定多数侧单写和已确认写不回滚;`owner_epoch`、lease、`admit_not_before`、Admission CAS 必须在同一事务域,禁止 MySQL + Redis / etcd 跨存储先查后写。切换目标时以稳定 `operation_id` 一次 CAS 到新 `owner_epoch/PENDING` 并写入覆盖旧 lease 最晚安全截止点的 `admit_not_before`;新 DS 在屏障前不得创建可操作玩家态、Admission、处理输入、业务写或确认 `PLAYABLE`,旧 DS 续租不确定 / 失租 / 到期必须先自 fencing 再 Kick / Despawn,进程恢复后也须在处理输入前重查。Redis TTL、locator key miss、客户端超时、Pod 存活和本机时钟都不能绕过屏障;Stable / Canary 必须共享同一 owner authority 与门禁。脑裂等待必须复用可见 `WAIT + retry_after + watchdog` 并保留 session / 原 operation;权威恢复后自动继续,不得用放行第二 DS 来“避免卡顿”。

**登录成功后只走一条幂等进场 / 恢复闭环**(细则见 `CLAUDE.md` §9 不变量 23):登录返回、选角、匹配各阶段、READY、切前后台、断线、Travel / Admission 失败和 Hub↔Battle 回流全部汇入同一无状态查询 / 恢复逻辑入口与客户端恢复协调器,不是新建有状态编排服务;禁止客户端自行猜路由或增加第二套 fallback。`Login OK` 后的暂时故障不得清 session 或要求重新输入密码;重复请求 / 回包丢失 / 重启必须沿稳定 `operation_id` 继续,不得重复占座、分配 DS 或产生第二 owner。未选角先返回 ROLE_REQUIRED,不提前分配 Hub;WAIT / UNKNOWN 的每次等待有 deadline,超时重查或展示真实重试 / 退出入口,整体可持续重试;只有该场景要求的玩家态完成 Admission 且客户端收到 exact 连接 ACK 才算本地 PLAYABLE。实现与 review 必须覆盖 locator 分区、MATCHING 恢复、地图加载无回调、旧 DS 恢复、迟到 Logout / Heartbeat 和 Stable / Canary 混跑。

**接线做最终版,不留半成品**(细则见 `CLAUDE.md §14`):新功能一次接到可上线版本,不准 TODO 占位 / 空实现;允许配置开关默认关闭(如 `snowflake.node_id_source` 默认 static),但开关打开后的分支必须是完整真实实现。引入隔离重依赖的独立 pkg module(如 `pkg/snowflake/etcdnode` 的 etcd client)到服务时,Claude 写代码 + 补 go.mod 的 require/replace,`go mod tidy` 生成 go.sum 由 Codex 执行(§11.1);接线后必须在交接里**列出需 tidy 的服务清单**,不准默声留着让下个 AI 撞 build 红。

## 5. 决策记录

- 大决策 → `docs/design/pandora-arch.md` §11
- 服务级 → `docs/design/<service>.md`
- 压测结果 → `docs/design/stress-<round>-*.md`
- 进度 → `PROGRESS.md`(每周追加,不删旧的)

**没写文档 = 没说过**(下个 AI 不会记得)。

## 6. proto 同步

以 `CLAUDE.md §5`(尤其 §5.8-§5.10、§5.12)和 `docs/design/proto-design.md` 为准,本文件不重复细则。**非负整型字段默认用无符号类型**(`uint32` / `uint64`),仅差值 / 增量、可能下溢的减法字段、枚举 / 状态例外——细则见 `CLAUDE.md §5` 第 12 条。

## 7. 跨 AI 冲突解决

- **新 AI 默认尊重旧的 PROGRESS.md / docs/design/ 决策**
- 有更优/更标准/更安全方案可提推翻,但须先写 `docs/design/decision-revisit-<topic>.md`(旧问题/新方案/风险/迁移成本/验收标准),人拍板后再改

## 8. 失败时怎么办

不"假装成功"(老实报错)、不自动重试 5 次(报错后等决策)、不绕过失败(注释断言/跳 test)、不擦屁股式 `git reset / checkout --` 销毁进度。

## 9. 报告 token / 工期

长任务开始时估完工时间;实际超 1.5 倍立刻汇报;不许"先干完再说"。

## 10. 触碰红线 → 立刻停止 + 报告

任务范围明显扩大或漏关键文件 / 规范文档自相矛盾 / 要写 secrets 进 git / 要 sudo / chmod / 关防火墙 / build 改坏别的服务 / 即将 push 远端 / **任何可能让玩家卡死、永久等待或进不去场景的代码**(所有登录、选角、匹配、传送、重连、切场景和进 Hub / Battle DS 路径必须有有界超时、持续驱动、可见错误与真实可用的重试 / 返回 / 恢复入口,见 `CLAUDE.md` §9 不变量 19/20/23) / **任何让 Go 服务或 Hub / Battle DS 无法金丝雀发布、必须停服升级或会强杀在场玩家的改动**(Go 新旧副本兼容共存,已有 RPC 不得原地禁用,同一逻辑任务 / 分片的单写者 Stable / Canary 共用 election + 单调 fencing token;DS Stable/Canary 双 Fleet,新版本只接新分配,旧版本停止接新并排空在场会话后退役,见 `CLAUDE.md` §9 不变量 21、`docs/design/zero-downtime-update.md` §6.3) / **破坏不停服更新(给 Redis pb 存储改字段编号·类型·语义、read-modify-write 路径丢弃 unknown fields、设计「必须停服才能上线/读数据」的方案)**(见 `CLAUDE.md` §9 不变量 16/17、`docs/design/zero-downtime-update.md`) / **新增客户端可写入的累积列表却没有写入侧总量上限和读取侧分页 / 单次返回上限**(好友、好友申请、公会申请、公会成员、组队 / 入群邀请、临时群成员、我所在的群、交易请求、黑名单、待处理队列等;必须在事务 / 原子写路径校验单玩家 / 单实体的 pending / active 数量,超限回明确业务错误,读取侧走 cursor 分页或 SQL LIMIT 兜底;登记到 `CLAUDE.md` §9 不变量 18 的「现存受管列表清单」) / **新增只增不删的持久化表却没有保留期和"待清理量报告"任务**(注意:保留期清理**默认只报告不删**,§9.24 + 2026-07-22 用户指令;真删须显式配 retention_mode=delete。业务性过期删除如道具过期/邮件失效不受此约束)(幂等流水、审计、托管、归档、领取记录、outbox 等会随时间 / 活跃度线性增长的表必须有界;玩家已失去 / 已终态的失效数据物理保留默认且最多 90 天,幂等行保留期须远大于重试窗口且删后重放 fail-closed / no-op;登记到 `CLAUDE.md` §9 不变量 24 的只增表清单**并同步 `tools/migrate/cmd/dbcheck` 内嵌清单**——该工具是上线前发布门禁与压测前后强制检查,未登记表 / 缺清理索引 / outbox 堆积直接 FAIL;无清理方案的只增表一律拒) / **新增 blob·JSON·CSV·位图这类集合序列化列却没有写入侧上限**(必须同时具备单元素上限 + 集合条目上限 + 整体字节上限三条,缺一即有洞;上限按设计期望定不按列类型上限定,列类型能用 `VARBINARY(N)` 就不用 `LONGBLOB`;登记进 dbcheck `bigFields` 与服务 `budgets.go`。排查手册见 `docs/design/db-capacity-guard.md`) / **新增连 MySQL 的服务却没在启动时断言 `sql_mode` 严格**(`pkg/dbguard.AssertStrictMode`;非严格模式超长写入静默截断 = 无声的数据损坏)。

以下同样属于立即停止的进场红线:把 locator TTL / key miss 当 Hub↔Battle 归属证明;新 DS 在旧 lease 安全屏障打开前 Admission、创建可操作玩家态或确认 `PLAYABLE`;旧 DS 续租不确定 / 到期后仍处理输入或业务写;`Login OK` 后因暂时失败清 session、要求重输密码或另走本地 fallback;重复进场产生第二 assignment / allocation / owner;旧 session / owner epoch 的 Logout、Heartbeat、Admission 或业务写能影响新会话;地图加载、Admission、旧连接 / Controller 清退只有无限轮询而没有总截止时间和可见恢复入口。

## 11. 合作分工

### 11.1 跨 AI 平台硬性分工

首版服务器主程序优先安全稳定,**不以省 token 为由降级模型**(业务代码不固定交给低一档模型)。

| 角色 | 负责 | 不负责 |
|---|---|---|
| **最高可用 Claude**(Opus 4.8+) | 实现+验证业务代码/proto/yaml/脚本/文档;深读代码与设计;架构/安全/跨服务一致性/核心战斗-匹配-交易/疑难 bug review;项目内验证(build/test/lint/compose);最终把关 | 装/升级/卸载工具;改系统环境(PATH/证书/Docker/防火墙);拉大镜像/生成证书/启停系统级服务;git status/diff/commit/push/tag |
| **ChatGPT / Codex** | 按 Claude 方案做环境配置/工具安装/证书/Docker/就绪确认/文档清理/调研归档;查版本/端口/容器/日志;git status/diff/commit message 建议;经授权执行 commit;纯 ops 直接做,回报交 Claude 审核 | 不实现业务逻辑(需写逻辑时只做审核/问题分析,反馈给 Claude) |
| **人** | 决策(架构/玩法/PvP/性能);UE 编辑器(蓝图/UMG/地图/动画/特效);美术;真机部署(k8s apply/docker push/上云);git push/PR 合并/release tag;环境改动与 commit 前授权 | — |

工作流:**Claude 实现+验证 → Claude review → Codex 环境执行+git 收尾(回报)→ Claude 复查**。装工具/改环境/信任证书/启重服务/push/tag/生产操作前等人批准。

## 12. 中文回复

继承 `CLAUDE.md §3`:所有对话产出、注释、commit、文档全中文。

## 13. 命名硬规则:UE 侧一律用 Pandora

**UE 工程 / 模块 / 类 / 文件 / 命名空间一律 `Pandora`,永久废弃 `Xuanming` / `Xm`**(2026-06-08 Codex 改名编译审核通过):

- 入口 `Pandora.uproject`、主模块 `Source/Pandora/`、类前缀 `Pandora*`
- 新建 UE 文件 / 类 / 模块不准再用 `Xuanming` / `Xm`;历史路径名仅作记录,不进代码
- 细则见 `CLAUDE.md §11`、`§13`
