# 复审残余架构项整改设计(2026-07-24)

> 本文覆盖 23 项复审(snapshot `86b15dbc` / 后续)中**无法作为一次性代码修复安全落地**的残余项。
> 可安全就地修的已落码测试绿(见 PROGRESS 与各文件注释);本文只收**必须先设计、分阶段验证**
> 的架构级改动。**在对应阶段实现并通过验证前,不得声称这些项已修复**(§9.22 / §16.6 / §16.8)。
> 排序原则:先做低爆炸半径、可本地验证的;owner authority / key 迁移类放最后并要求故障注入。

## 0. 为什么不能一次性照审核改(证据)

- **P0 assignment fencing(#6)**:`writer_fence.go:20-31` 已记录 per-player key `pandora:hub:player:<id>`
  **无 hashtag、与任何 {pod} slot 不可同事务**,结构上加不了存储级 fence;审核要的"CAS 里带持久
  fencing token"正是文档说明过"需迁移现网 key scheme、风险更大"的事。属**高风险在线迁移**,非编辑。
- **owner authority 类(P0#3/#5 部分、P1#1)**:§9.22 明写"每玩家 owner_epoch 线性一致 owner
  authority **尚未实现**,实现前不得宣称本条全量达成"。这是一个**未实现的权威子系统**,不是漏改的行。
- **两项外部阻塞**:writerlease 生产 etcd ACL 是 **etcd RBAC 运维工件**;team_size 在队固化需
  **加 proto 字段 → Codex 重生 pb**(§5)。代码侧我能写,授予/重生这一步不在仓库内。
- 教训:上轮把 P1#7 当"漏清 ticker"急改,反而引入回归(违反 1515 行明确注释)。分布式核心急改
  = 制造 P0(§16)。故本文全部要求"读全链→设计→落码→**故障注入/集成验证**"后才算闭合。

---

## 1. 阶段 A — 低爆炸半径、可本地验证(建议先做)

> **进度(2026-07-24)**:A2 已落码(build 绿,待集群冒烟);A1 判定为已被现有日志升级覆盖;A3 归 ops + 可选探针。详见下。
>
> **进度更新(2026-07-25,R10 复审整改)**:
> - **A1 落码**:`/healthz/writer` 已上线(`internal/server/http.go` `WriterHealthHolder`),
>   `Health()` 扩为 `HealthSnapshot`(held/token/竞选失败/**激活失败**/degraded);readiness 仍不挂钩,
>   并由 `main_test.go` 守护"不得把 /healthz/writer 接进 readinessProbe"。
> - **A2 收口**:此前只有 `login-prod.yaml.example` 手写 headless 地址,**标准生成链仍产出 ClusterIP 短名**
>   ——即生产实际配置绕过了整条 round_robin 修复。已加 `Set-ProdLoginHubHeadlessAddr`(-Prod 机械改写)
>   + 契约测试双向断言(prod=headless / dev=短名,compose 不可解析 FQDN)。集群冒烟仍是关闭条件。
> - **阶段 E 落码**:`hub.writer_lease_mode`(enforce|warmup|off)+ rollout §5.4 三跳发布合约。
> - **P0#6 的 assignment fencing 部分落码**(不等 owner authority):归属记录内嵌 `writer_token`
>   持久水位 + 继任推扫升级为**接流前硬门**。详见下方阶段 D 的更新。

### A1. writerlease 就绪可观测(P0#4)——**已落码(观测端点上线,readiness 仍不挂钩)**
- 复核后判定:`writerlease` 已有 `campaignErrEscalateAfter=15`(~30s 连续竞选失败即从 Warn 升 Error),
  日志即可驱动"持续无主"告警——这就是生产可观测路径。本仓库**无自定义 Prometheus 指标模式**,
  为此新起一套指标是新增机制(§15.3 不预设)。故 A1 不再单独落指标;若将来引入指标体系,再把
  `Health()`(consecutiveErrs/lastErr)接一个 gauge。**不改 readiness 流量门**(热备语义,理由见上)。
- **现状**:失主副本保持 Ready 是**有意热备**(拒写但可秒级接管);`Health()` 已在但未接探针。
  盲目把 readiness 门成"必须是 writer"会在全体无法当选时把"写降级"放大成"整服零端点"。
- **方案**:不改 K8s readiness(保持热备可服务);新增独立 `/healthz/writer`(或 metrics
  `hub_allocator_writer_held` + `hub_allocator_campaign_consecutive_errs`)暴露 `Health()`,供告警
  规则在"持续无主 > lease TTL"时报警。**只加观测,不改流量门**。
- **验证**:单测覆盖 Health() 快照;人工确认告警规则表达式。

### A2. 非-writer 路由(P0#5)——**已落码(build 绿,待集群冒烟)**
- **现状**:login→hub_allocator 经普通 ClusterIP 直连(`MustDialInsecure(静态addr)`),被 L4 钉在
  某一 Pod;落到非-writer 副本就一直非-writer,`AssignHub` 就地 3 次重试复用同连接=同 Pod=白重试。
- **落地方案(最标准的 gRPC-on-k8s 做法,非 ad-hoc 重连;本仓库无服务发现)**:
  - `pkg/grpcclient` 新增 `MustDialInsecureRoundRobin`,经 `kgrpc.WithOptions(grpc.WithDefaultServiceConfig)`
    注入 gRPC 官方 `round_robin` LB;
  - login `mustBuildHubAssigner` 改用它;`login-prod.yaml.example` 的 `hub.addr` 改为
    `dns:///hub-allocator-headless.pandora.svc.cluster.local:20021`;
  - `deploy/k8s/services/services.yaml` 新增 `hub-allocator-headless`(clusterIP:None,DNS 返全部
    Ready Pod IP),保留原 ClusterIP Service 供单发调用方。
  - 效果:`AssignHub` 现有就地重试每次 RPC 经 round_robin 轮到不同副本,滚动重叠(maxSurge 双 Pod)
    内数次命中 writer;dev 单静态 addr(passthrough)下退化单后端,行为不变(§14)。
- **验证**:pkg/login **build 绿 + login biz 测试绿**(装配正确);**LB 分发效果依赖 k8s headless
  DNS + gRPC dns 解析,只能集群内冒烟(观察 RPC 落到多个 Pod),本机无法验证**——列为交接项。
- **残留调参**:`assignHubMaxAttempts=3` 对 replicas:1+滚动双 Pod 足够;若 HA 增副本到 N,应把
  重试上限提到 ≥N 以保证 round_robin 一轮内必遍历到 writer(§15.3 现不预设)。

### A3. writerlease 生产 ACL(P1#2)——**代码 + 运维两半**
- **代码(可落)**:writerlease.Start dial 后做一次**正向能力探针**(在 `prefix+election` 下写一个
  `__acl_probe__` 短租约 key 再删),`PermissionDenied` 即 fail-fast,把"最小权限身份竞选永久
  被拒"从"~30s 后升 Error"提前到启动即炸。**需真实 etcd 验证**,本地无 etcd 只能 build。
- **运维(阻塞)**:在 etcd RBAC 授予该身份对 `/pandora/writerlease/` 前缀的读写;写入部署契约。

---

### A2 集群冒烟验收(P0#5 关闭条件,本机无法代跑)
前置:apply 新 `hub-allocator-headless` Service + login 用 `dns:///hub-allocator-headless...:20021` 配置。
1. **多后端解析**:`kubectl -n pandora scale deploy/hub-allocator --replicas=3`(或在滚动升级窗口),
   `kubectl -n pandora get endpoints hub-allocator-headless -o wide` 确认返回多个 Pod IP。
2. **确认 writer 唯一**:`kubectl -n pandora logs -l app=hub-allocator --prefix | grep 'writerlease.*elected'`
   —— 只有一个 Pod 打 `elected token=…`,其余为热备。
3. **分发生效**:驱动 ≥30 次登录(AssignHub);按 Pod 分别 `grep 'AssignHub'` 访问日志,确认请求
   **落到多个 Pod**(非钉在一个);非-writer Pod 打 `writer lease superseded`(可重试),login 侧
   `AssignHub transport/…retry` 后成功——即重试轮到了 writer。
4. **无主 fail-closed 不卡死**:临时封锁 etcd(全体无法当选),确认 AssignHub 有界重试耗尽返回
   Unavailable、login 不清会话、客户端稍后重登可恢复(§23);恢复 etcd 后自动收敛。
> 4 步全绿 = P0#5 可关闭;否则保持 OPEN。`assignHubMaxAttempts=3`,若 replicas>3 先调高再验第 3 步。

## 2. 阶段 B — 进场屏障(spawn 前复核,P0#1/#2)——**窄版已落码(2026-07-25),§22 全量屏障仍待阶段 D**

> **2026-07-25 落码(R10 复审 P0-6/P0-7)**:不等 owner authority,先把**既有**的会话复核从
> "spawn 之后补检"前移为"spawn 的前置条件"。这不是 §9.22 的 PENDING→ADMITTED 屏障,但确实
>消灭了审核指出的那个窗口:**复核未确证前不存在可操作 Pawn**。
> - **Hub**(`PandoraHubGameMode.cpp`):`TryOpenAdmissionSpawnGate` 在旧连接全部物理离场后
>   不再直接开门,而是先 `StartPreOpenAdmissionRecheck`(同 identity 幂等重发 ACK);确证通过才
>   `FinishOpenAdmissionSpawnGate`(flush 旧 Departure → 移除 gate → 补做唯一一次 starting flow →
>   客户端信号 + locator)。定性拒绝/预算耗尽 → `FailAdmission` 清退(此时尚无 Pawn);
>   本地/PIE 无 DSBackend → 直接开门(不留无人驱动的等待)。
> - **Battle**(`PandoraDSGameModeBase.cpp`):新增 `SessionRecheckSpawnGate`,`PostLogin` 在
>   `Super::PostLogin` **之前**把有远端权威挂账的连接入门;`HandleStartingNewPlayer_Implementation`
>   与 `SpawnDefaultPawnAtTransform_Implementation` 两道门在确证前一律不生成 Pawn;
>   复核确证 → `OpenSessionRecheckSpawnGate` 补做 starting flow 并登记 Pawn claims。
>   原 `PostSpawn*` 系列符号整体更名为 `Entry*`(语义已变,避免名不副实)。
> - **不卡死自证(§9.19/20)**:复核每条终局路径要么开门、要么 Kick——成功开门;定性会话失效
>   Kick;非会话语义的定性拒绝**也开门**(不误杀但必须放行);未知结果有界重试;预算耗尽
>   fail-closed Kick。无挂账/无后端子系统一律直接开门。
> - **诚实残余**:复核返回与开门之间仍有进程内 check-then-act 窗口(与 login `fenceLoginDelivery`
>   同构),但窗口内没有可操作 Pawn,且长度从"等旧连接物理离场的秒级"缩到一次调用返回。
>   要彻底消灭仍需下面的 §22 屏障(阶段 D 之后)。
> - **验证状态**:**UE 编译与 §23 验收矩阵均未跑**(本仓库规定 UE 编译由用户执行)。
>   未过编译与故障注入前不得声称 P0#1/#2 已关闭。

- **现状(改动前)**:Hub/Battle 均先开 gate/Spawn,再异步复核会话(有 R9 自愈:定性失效清退)。残余=
  开 gate 到复核确证之间的可玩窗口。要"消灭窗口"必须在 **spawn 前**完成 owner/session 确证,
  但纯前置又可能卡住进场(撞 §19/20 不卡死)。
- **方案(§22 admit barrier)**:落 §9.22 的 `PENDING→ADMITTED` 屏障——新 DS 在屏障打开前只
  预留 + 加载地图级资源,**不创建可操作 Pawn / 不处理输入 / 不产生业务写 / 不向客户端确认
  PLAYABLE**;屏障打开(owner authority CAS 确证)后才 Spawn 可操作 Pawn。既消灭"先 Spawn"窗口,
  又靠 watchdog + retry_after 的 WAIT 不卡死。
- **依赖**:owner authority(阶段 D)。**先做 D 再做 B**。
- **验证**:§23 验收矩阵相关项(Admission ACK 丢失、旧 Controller 不退、旧 DS 分区恢复)+ 故障注入。

## 3. 阶段 C — Login 交付顺序(P0 login 撕裂 #1/#3、P1#1)

- **P1#1(抢占早于路由)**:现在 sessions.Set / PersistSessionJTI 在 resolveHub/AssignHub **之前**,
  后续路由失败会既废旧会话又不交付新 token。**方案**:把"轮换会话代际"推迟到路由/签票成功后的
  单一交付点,或采用两阶段(先占位后提交),使"未能交付新凭据"时不废旧会话。需覆盖顶号语义
  (§23 会话 fencing:旧 session 不能再签票,迟到 Logout 只 compare-delete 自己)。
- **P0 login Redis/MySQL 撕裂(#1/#3)**:现有 R9 回读→条件回补是 fail-closed 自愈(generation
  单调、非双活),残余是"Redis=新未交付 / MySQL=旧"的短暂 lockout。**方案**:把两存储的会话代际
  写收敛到**单一线性化权威**(§22:owner/会话权威须线性一致,禁跨存储先查后写),Redis 仅作可
  重建投影。这与 owner authority 同属"线性一致权威"工作线。
- **验证**:并发双登录交错 + 部分失败(已提交/响应丢失)的确定性回归 + 真实 Redis/MySQL 集成。

## 4. 阶段 D — owner authority **contract 切换**(权威已实现,剩强依赖切换)+ assignment fencing(P0#6)

> **重大更正(2026-07-24,读 `services/runtime/owner` 后)**:§9.22 owner authority **已实现**,不是
> "尚未实现"。`services/runtime/owner` 有完整 `owner_record`(owner_epoch/phase/instance_epoch/
> admit_not_before…)、`ds_instance_lease`、`owner_transition_log`;`owner_repo.BeginTransition` 在
> **单个 SQL 事务(FOR UPDATE = 线性一致域,满足§9.22"禁跨存储先查后写")** 内做 epoch CAS +
> 幂等重放 + epoch 冲突 query-first + `admit_not_before=max(now,旧租约截止)+skew`;`Admit` 做全等
> fail-closed + 屏障 `wait_ms` + `PENDING→ADMITTED` 幂等。CLAUDE.md §9.22 的"尚未实现"注是 stale。

- **剩余 = contract 切换(不是建权威)**:当前 allocator/login/DS 把 owner Begin/Admit 当**弱依赖并行**
  (owner_authority.go:"全弱依赖,旧路由门照跑");Phase D = 把它切成**强依赖/权威路由**并**退役旧
  §8 lease-fencing 门**。分服务灰度:①login **query-first** 组合 owner(§23);②allocator 分配/出票
  以 owner epoch 为准;③DS Admission 直接提交 owner Admit(退役 census 近似,owner_authority.go 已注);
  ④确认新路径稳定后逐一退旧门。每步保留回退(弱→强开关),异常回落弱依赖。
- **assignment fencing(P0#6)已先行落码(2026-07-25,不依赖 owner authority)**:同样**不需要迁 key scheme**
  ——把水位记进**归属记录自身**(`HubAssignmentStorageRecord.writer_token`,additive 字段 31)。同一 key 的
  WATCH/MULTI/EXEC 天然原子,比较与写入在同一线性化点,于是 per-player 键也有了持久 fence:
  `current.writer_token > 本届 token` → 零写入 `ErrWriterSuperseded`,旧写者**既不能覆盖也不能删除**
  继任者写的归属。同时把继任推扫从"后台 tick 懒执行"升级为 `writerlease.Config.OnElected` 的
  **接流前硬门**(推扫成功才宣告持有领导权),消除"已接流、继任未完成"窗口。
  测试:`internal/data/writer_fence_test.go`(前任覆盖/删除双拒 + 零变更 + legacy 兼容)、
  `pkg/dsauthfence/writerlease/writerlease_test.go`(激活期间恒不持有 / 激活失败让位)。
  下方 owner-epoch 方案仍是 contract 阶段的终局形态,两者不冲突(水位是过渡期的存储级兜底)。
- **owner authority 化解路径(仍待 contract 切换)**:per-player `owner_record` 本身就是
  每玩家**线性一致 fence**(MySQL FOR UPDATE);assignment 分配 / 出票在 contract 阶段挂 owner epoch
  校验(Begin/Admit 全等),旧 writer 失租后其 epoch 必被新 owner 的 CAS 推进,迟到 CAS/出票在 owner
  层被 `ErrOwnerEpochConflict`/`ErrOwnerIdentityMismatch` 拒——即得 fencing。writer_fence.go 的
  Redis 四层组合退为"contract 前的过渡兜底",切换后可评估退役。
- **验证**:owner 服务的 epoch CAS / 屏障单测(应已有,待核)+ **强依赖切换后**的脑裂"一人一 DS"
  故障注入 + 故障切换不回滚(§9.22 硬要求,需真实 MySQL/TiDB + 分区注入,集成环境,非本机)。

## 5. 阶段 E — 首次 writerlease 引导升级(P0#7)——**已落码(2026-07-25);集群演练待跑**

- **问题**:从 writerlease-**无感**旧镜像首次升级,新旧并存=无 fence 双写;Recreate 又停唯一旧 Pod。
- **落地**:`hub.writer_lease_mode`(`enforce|warmup|off`,留空=enforce 保持现网行为)+
  `docs/design/session-generation-rollout.md` §5.4 三跳发布合约:
  跳 1 Recreate 只换镜像跑 `warmup`(**写路径与旧二进制逐字节同行为**,只竞选观测,可零代价回滚镜像);
  跳 2 Recreate 只改配置到 `enforce`(回滚 = 改回 warmup,不必回滚镜像);
  跳 3 单独 apply `RollingUpdate`(不重建 Pod,零中断),此后稳态不停服。
- **必须照实说的边界(不是遗漏,是原理)**:旧二进制既不竞选也不读 fence 键,新写者无法让它停手,
  所以这一次性迁移上"零写暂停"与"零双写"**不可兼得**。§5.4 选零双写:每跳窗口 = 一次 Pod 重启,
  等于迁移前每次发布的基线,不新增代价;玩家不掉线、对局不中断。原方案设想的"预热版本可在
  RollingUpdate 下与旧版并存"并不成立(两者都按旧路径写 = 仍是双写)。
- **验证**:§5.4 的三跳验收项(集群内,本机无法代跑)。未跑完前本阶段按 OPEN 记录。

## 6. 阶段 F — team_size 在队固化(P1#10)——**需 Codex**

- 给 `MatchTicketStorageRecord` 加 `uint32 team_size`(或 `config_version`)字段,入队时按当时配置固化;
  撮合用票据固化值而非当前配置,消除热更改变在队票成局语义。**proto 改 → Codex 重生 go/cpp pb**,
  再改撮合读取点 + 回归。字段编号新增、双向兼容(§9.17),滚动期旧票无字段回退当前配置。

---

## 落地纪律(所有阶段通用)
- 每阶段独立 PR,含动机/改动范围/测试方式/风险点(§4.3);分布式项必须故障注入 + 集成验证,
  `go test -race` 在 CGO/Linux CI 跑(§16.6/§16.7)。
- owner authority / key 迁移类改动**上生产前**须过脑裂故障注入 + dbcheck 发布门禁。
- 未过验证的阶段一律标 OPEN,事故档案 `2026-07-22-p0-push-stale-session-subscribe.md` 的 P0 面
  以本文阶段状态为准,不得据旧"只剩一个 P0"对外表述。
