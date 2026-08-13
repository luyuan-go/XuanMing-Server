# [INC-20260812-002][P0] owner 事务提交 fsync 长尾打断 Hub 进场链,玩家随机被踢

> **状态**：已修复待观察（未关闭）
> **类型**：`availability`
> **环境**：本机 k8s（minikube profile `pandora-agones` / Docker Desktop on Windows 11）
> **首次发生时间（UTC）**：2026-08-12 14:13 前后（owner 与 edge envoy 同批启动即存在，最早可证的失败日志为 15:17:33）
> **首次发现时间（UTC）**：2026-08-12 15:21（玩家侧表现为"队友血条不变色"，逐层排查后才定位到被踢）
> **负责人**：待指定
> **受影响服务/版本**：`owner`（image `pandora/owner:dev`）、`hub-allocator`、Hub DS `pandora/hub-ds:r1987-dirty-20260812-093733`
> **最后更新**：2026-08-12

## 0. 一句话结论

本地 MySQL 沿用生产级双 fsync 持久化（`innodb_flush_log_at_trx_commit=1` + `sync_binlog=1`），而数据目录落在 Docker Desktop 虚拟磁盘上，单次事务提交长尾达 **19.4 秒**；`owner` 的 `QueryOwner` 因此超时，`hub-allocator` 按 §9.22 fail-closed 返回 `ErrUnavailable(10)`，Hub DS 拒绝 SetLocation 并**踢出玩家**——玩家表现为随机掉线，以及重连后社交缓存被清导致队友识别失效。同一时段还并存一个**独立**故障（edge envoy 短名解析全部落到宿主网关导致登录 503），两者无因果关系，一并记录于本档 §5.5。

## 1. 影响与范围

- **玩家影响**：在大厅内随机被踢回登录；重连后队友血条颜色不生效（队友识别失效）。
- **影响人数/对局/请求数**：本机联调环境，2 名测试账号；可证的踢出事件 6 次（15:17:33、15:17:53、15:18:07、15:18:19、15:21:08、15:21:15）。
- **服务影响**：`owner` 未崩溃、未重启，仅延迟劣化；`hub-allocator` 与 Hub DS 行为均符合设计（fail-closed），非缺陷方。
- **数据与安全影响**：无。没有错误写入、没有越权、没有脑裂——fail-closed 恰恰保住了 §9.1「玩家同一时刻只在一个可操作 DS」。
- **开始/结束时间**：UTC 15:17:33 起可证；UTC 17:0x 改配置后消失。
- **是否仍可复发**：本机环境已修复；**任何未应用本次 `infra.yaml` 改动的开发机仍会复发**。
- **严重级别判定理由**：玩家被踢出可玩场景且无法自愈地重复发生，命中 `index.md` §1 第二条（导致玩家掉线/被踢）。虽仅本机环境，但根因是**共享的本地部署清单**，每个开发者都会遇到。

## 2. 第一现场与证据

### 2.1 症状

- **客户端症状**：站在大厅里被踢回登录页；重连成功后队友头顶血条仍是敌对色，自己的正常。
- **服务端症状**：Hub DS 反复打 `Hub Admission 失败，拒绝 SetLocation 并踢出`；`owner` 侧 RPC 延迟中位 1ms 但最大 19397ms。
- **K8s/Agones 状态**：全部 Pod `Running`，无重启、无 CrashLoop、无 OOMKilled、GameServer 全 `Ready`——**这正是本事故最难定位的原因：所有健康检查都是绿的**。

### 2.2 原始证据

采集位置：`kubectl --context pandora-agones logs -n pandora <pod>`；Hub DS 为 `kubectl logs <gameserver> -c pandora-hub-ds`。

```text
# Hub DS —— 踢人
[15:21:15.861] LogPandoraHubFlow: Warning: Hub Admission 失败，拒绝 SetLocation 并踢出：player=23135464110522368

# hub-allocator —— 上游返回不可用（transport 通、gRPC OK、业务码 10）
[14:56:20.032] LogPandoraDSBackend: Warning: DS call failed:
    /pandora.hub.v1.HubAllocatorService/AcknowledgeAdmission transport=1 grpc=0 code=8 err=

# owner —— 分段计时（本次为定位而加）：延迟 100% 在 commit
owner_renew_lease_slow  total_ms=759   begin_tx_ms=0 select_for_update_ms=0 commit_ms=758  pool_wait_ms=0
owner_renew_lease_slow  total_ms=1681  begin_tx_ms=0 select_for_update_ms=0 commit_ms=1680 pool_wait_ms=0
owner_renew_lease_slow  total_ms=1469  begin_tx_ms=0 select_for_update_ms=0 commit_ms=1468 pool_wait_ms=0

# owner —— 延迟分布（全量日志 3099 样本）
中位 1ms   P90 65ms   最大 19397ms
```

MySQL 侧配置取证：

```text
innodb_flush_log_at_trx_commit  1
sync_binlog                     1
innodb_flush_method             O_DIRECT
datadir                         /var/lib/mysql/     （Pod volume 类型 = emptyDir）
```

### 2.3 已排除的噪声

| 同时出现的报错 | 为什么不是本事故根因 |
|---|---|
| `owner_lease_renew_failed_weak ... DeadlineExceeded`（streak 22） | 该门是**弱依赖**，`required=false` 时 `return nil` 放行，不影响准入。日志里的 `streak` 是 `plog.Window` 的**限流计数**，不是连续失败数。排查中曾据此误判，已更正。 |
| `hub_presence_refresh_failed ... locator code=2` | player_locator 的 presence 投影刷新失败，按 §9.22 presence 不参与准入判定。 |
| `login` Pod `RESTARTS 1 (exitCode=1)` | 发生在 14:13 启动窗口，与 15:17 起的踢出无时间相关性；login 自身健康检查全程 `rpc_ok`。 |
| edge envoy 503 | **是独立的第二故障**（§5.5），影响的是"登不进去"而非"进去后被踢"，两者根因无交集。 |

## 3. 时间线

以 UTC 为主。客户端日志为本地时间（UTC+4），已换算。

| UTC 时间 | 组件 | 事件 | 证据 |
|---|---|---|---|
| 14:13:23 | mysql / owner / edge-envoy | 本轮环境启动 | `owner_store_connected dsn=…@tcp(mysql:3306)/pandora_owner` |
| 14:56:20 | hub-allocator | `AcknowledgeAdmission` 返回 `code=8` | DS 日志 |
| 15:17:33 | Hub DS | 首次可证的踢出 | `Hub Admission 失败…踢出：player=23135…` |
| 15:17:53 ~ 15:21:15 | Hub DS | 再踢 5 次，玩家反复重连 | 同上 |
| 15:21:25 | Hub DS | 该玩家最终被接纳（`InitNewPlayer accepted`），但社交缓存已被前次 Logout 清空 | `[SpawnPawn] player=23135… Camp=2` |
| ~16:5x | owner | 加分段计时日志并重新部署 | commit `65751670` 前的工作副本 |
| ~17:0x | owner | 分段计时给出 `commit_ms` 占满 `total_ms` | 见 §2.2 |
| ~17:1x | mysql | 应用 `trx_commit=2` + `sync_binlog=0` 并滚动重启 | `deployment.apps/mysql configured` |
| ~17:2x | owner | 60 秒窗口验证：慢续租 0 次 | 见 §8 |

## 4. 调用链与关键变量

```text
Hub DS PostLogin
  → hub-allocator.AcknowledgeAdmission
    → admitOwnerForAdmission
      → ownerAuth.QueryOwner(ctx, playerID)        ← 卡在这里
        → owner.QueryOwner
          → MySQL BEGIN → SELECT ... FOR UPDATE → COMMIT   ← COMMIT 双 fsync 阻塞 0.7~19.4s
      ← err → errcode.NewCause(ErrUnavailable, …)   ← code=10
  ← code!=OK → FailAdmission → GameSession->KickPlayer
```

| 变量/对象 | 创建位置 | 所有者与生命周期 | 是否共享/可变 | 事故中的作用 |
|---|---|---|---|---|
| MySQL redo/binlog fsync | InnoDB 提交路径 | 每次 COMMIT | 共享磁盘 | **直接根因**：阻塞点 |
| `owner` 事务 `ctx` | `QueryOwner` handler | 单次 RPC | 否 | 超时后向上传播为 `ErrUnavailable` |
| `FHubAdmissionState` | Hub DS PostLogin | 单次准入，含 `RetryTimer` | 否 | 重试预算耗尽后 `FailAdmission` |
| `PlayerSocialIds` | Hub DS GameMode | 玩家在场期间 | 否 | 被踢 → Logout 清空 → 队友识别失效 |

## 5. 根因

### 5.1 直接根因

**本地 MySQL 沿用生产级持久化配置，而底层磁盘无法支撑其 fsync 成本。**

最小必要条件（三者同时成立才发作）：

1. `innodb_flush_log_at_trx_commit=1` **且** `sync_binlog=1` → 每次事务提交做**两次** fsync；
2. `datadir` 位于 `emptyDir` → minikube 容器层 → Docker Desktop 虚拟磁盘（Windows 上 fsync 成本极高）；
3. `owner` 的 owner-lease 语义要求**每次续租/查询都落一次事务**（这是 §9.22 线性一致要求，不可省）。

确定证据：分段计时显示 `begin_tx=0 / select_for_update=0 / pool_wait=0`，**唯独 `commit_ms` 等于 `total_ms`**。这直接排除了连接池饱和、慢查询、行锁竞争三种常见猜测。

事实与推断的边界：「延迟在 commit」是**实测事实**；「commit 慢是因为 Docker Desktop 虚拟磁盘 fsync 慢」是**强推断**——依据是改配置后同一硬件上单次提交降到 0.83ms，若为其它原因（如 CPU、内存、网络）不会因放宽 fsync 而改善。未做磁盘级 `fio` 基准佐证，列为剩余风险 A-1。

### 5.2 触发条件

- 任意一次 Hub 准入（玩家登录进大厅、Battle 回流 Hub）恰好撞上 fsync 长尾；
- 长尾超过 `hub-allocator → owner` 的 RPC deadline 与 Hub DS 的准入重试预算之和。

### 5.3 故障放大因素

1. **重试放大**：被踢 → 客户端自动重连 → 再次准入 → 再次撞上长尾，形成"踢-重连"循环（15:17~15:21 共 6 次）。
2. **副作用扩散**：每次被踢触发 Logout，清空 `PlayerSocialIds` 缓存，导致"队友血条不变色"这个**看起来完全无关**的表象——排查从 UI 层一路倒查到磁盘，耗时数小时。
3. **全绿的健康检查**：没有崩溃、没有重启、没有 OOM，`kubectl get pods` 全 `Running`，常规巡检手段全部失效。

### 5.4 为什么现有保护没有挡住

| 保护 | 为何无效 |
|---|---|
| Hub DS 准入重试（`State.RetryTimer`） | 有界重试，预算耗尽即 `FailAdmission`。**这是正确设计**：拿不到 owner 权威就不能开门，否则违反 §9.1。不应为此放宽。 |
| `owner_lease` 弱依赖放行 | 只覆盖**租约双写**这条弱链路，`QueryOwner` 是准入的**强依赖**，无法弱化。 |
| 连接池 | `pool_wait_ms=0`——池子根本没饱和，问题在池子之外的磁盘。 |
| 健康检查 / readiness | 探针只验"进程活着、能应答"，不验延迟分布。owner 全程 `rpc_ok`。 |
| 既有可观测性 | `rpc_slow` 只打**总延迟**，无法区分是连接池、查询还是提交慢。**本事故的定位阻塞点正在于此**——分段计时是为查这个故障临时加的，此前无法回答"慢在哪一段"。 |

### 5.5 并存的独立故障：edge envoy 短名解析（同批修复，非同一根因）

`start.ps1` 的 `Convert-EdgeEnvoyConfigForCluster` 把宿主配置的 `host.docker.internal` 改写成**短名**（`login` / `team` / …）。短名依赖 Pod 的 DNS 搜索域；搜索域一旦未生效，解析落到宿主 DNS，而 Docker Desktop 的 DNS 对任何未知名字都返回**自己的网关** `192.168.65.254`。结果：18 个上游全部"解析成功"却指向没有这些服务的地址。

```text
login_cluster::192.168.65.254:20001  cx_connect_fail=19  rq_error=20
membership_healthy=1  upstream_cx_none_healthy=0     ← 假象：Envoy 认为上游是健康的
```

对照组：集群内 DS 版网关（`16-ds-envoy.yaml`）一直写**全限定名**，同一集群同一时刻解析正确（`login_cluster::10.105.158.133:20001`），从未出过此故障。

玩家表现为登录直接 503（`Login failed: HTTP 503 without gRPC trailer`）。与 §5.1 的踢出**无因果关系**，只是同一排查窗口内并存，按 `index.md` §1 第四条（独立根因分别建档）本应另建 Incident；因两者修复同批提交、且第二故障未独立造成玩家可玩性中断（登录直接失败，非进场后被踢），此处合并记录并在 §10 列为待拆分项 A-3。

## 6. 全仓同类问题扫描

- **扫描基线 commit**：`65751670`
- **扫描目录和文件类型**：`deploy/k8s/**/*.yaml`、`tools/scripts/*.ps1`、`services/**/internal/data/*.go`
- **搜索模式/工具**：`grep -rn "flush_log_at_trx_commit\|sync_binlog"`、`grep -rn "address: [a-z][a-z0-9-]*$"`、Envoy admin `/clusters` 实测比对
- **Confirmed 同型命中**：
  - `deploy/k8s/infra/infra.yaml` 的 MySQL（已修）。
  - `start.ps1` 生成的 edge envoy 18 个上游短名（已修）。
- **结构性隐患**：
  - **本地基础设施沿用生产级默认值**是一类通病，不止 MySQL。同目录的 `etcd` / `kafka` / `redis` 是否也有类似的持久化/刷盘成本，**本次未逐个核查**（剩余风险 A-2）。
  - **只打总延迟、不打分段**是全仓可观测性通病。`owner` 已补，其它服务的 `rpc_slow` 仍只有总数（剩余风险 A-4）。
- **已排除项及理由**：`tidb.yaml` 使用独立存储路径与 TiKV，不走本 emptyDir 链路，且压测域另有基线，本次不改。
- **未覆盖边界**：未在其它开发者机器上复现验证（不同磁盘/不同 Docker Desktop 版本表现可能不同）。

## 7. 处置与永久修复

### 7.1 临时止血

| 动作 | 状态 | 证据 | 风险/回滚 |
|---|---|---|---|
| 无 | — | 本次直接做了永久修复，未走临时手段 | — |

### 7.2 永久修复

| 项目 | 状态 | 代码/配置 | 验证 |
|---|---|---|---|
| MySQL 放宽本地持久化 | 已提交已部署 | `deploy/k8s/infra/infra.yaml`：`--innodb-flush-log-at-trx-commit=2` `--sync-binlog=0` | §8 |
| edge envoy 改全限定名 | 已提交**未生效** | `tools/scripts/start.ps1`：`Convert-EdgeEnvoyConfigForCluster` 追加 `.$Namespace.svc.cluster.local`，命名空间改为显式参数 | 函数级自测通过；**需下次跑 `start.ps1` 重建 ConfigMap 才落地** |
| owner 续租分段计时 | 已提交已部署 | `services/runtime/owner/internal/data/owner_repo.go` | 本事故即靠它定位 |
| owner pprof（含 block/mutex 采样） | 已提交已部署 | `services/runtime/owner/internal/server/http.go`，`PANDORA_PPROF=1` 开启 | 四端点实测 HTTP 200 |

修复 commit：`65751670`（8 files changed）。

### 7.3 防复发规则

- 本档案本身即规则载体：**本地基础设施不得直接沿用生产级持久化默认值**，放宽必须写明放宽了什么、丢什么、为什么本地可接受，并确认线上不引用该文件（本次已确认 `overlays/online/kustomization.yaml` 只引用 `../../services`，不含 `infra/`）。
- **集群内服务地址一律写全限定名**，不依赖 DNS 搜索域。已在 `start.ps1` 改写处以注释固化，附本次实测证据。
- 尚未落为机械门禁（剩余风险 A-5）。

## 8. 验证矩阵

| 验证 | 修复前结果 | 修复后结果 | 环境/命令 | 证据 |
|---|---|---|---|---|
| MySQL 单次提交耗时 | 秒级（长尾 19397ms） | **0.83ms** | Pod 内 `INSERT` + `TIMESTAMPDIFF` | §2.2 |
| owner RPC 延迟分布 | 中位 1ms / P90 65ms / 最大 19397ms（3099 样本） | 中位 0ms / **P90 2ms** / **最大 266ms**（24 样本，60s 窗口） | `kubectl logs -n pandora owner-* --since=60s` | — |
| 慢续租告警次数 | 持续出现 | **0 次** | 同上 | — |
| Hub 准入失败次数 | 6 次可证踢出 | **0 次** | Hub DS 日志 | — |
| 队伍解析（玩家侧终态） | 仅 1 个玩家有记录，另一个零记录 | **两个玩家 `team` 一致** | Hub DS `[SocialIds] 队伍已解析` | — |
| envoy 改写函数自测 | `address: login` | `address: login.pandora.svc.cluster.local`，admin 仍锁 `127.0.0.1` | `pwsh` 载入函数跑样本配置 | — |
| pprof 端点 | 不存在 | goroutine/block/mutex/heap 均 200 | port-forward + curl | — |
| `go test -race` | **未执行** | **未执行** | 需 CGO Linux 环境 | 阻断项 |
| fatal/OOM/SIGKILL 重启注入 | **未执行** | **未执行** | — | 未做 |
| 玩家 E2E | 被踢 | 登录进场正常、组队正常 | 双客户端手测 | 已完成（手测，非自动化） |
| edge envoy 端到端 | 登录 503 | **未验证**（改动未生效） | 需重跑 `start.ps1` | 阻断项 |

## 9. 部署、回滚与观察

- **修复 commit**：`65751670`（未推送）
- **构建产物/镜像 digest**：`pandora/owner:dev` = `sha256:c712e4e407dfaf20088ce00ae6b453e990694a2d4fee8fdba15685ba6fef28fa`（在 minikube 内构建，见下方"坑"）
- **部署时间与目标环境**：2026-08-12 UTC ~17:1x，本机 `pandora-agones`
- **实际 Pod `imageID`**：已核对与上述 digest 一致
- **回滚条件和步骤**：若发现数据一致性异常（本地开发不预期），把 `infra.yaml` 两行删掉并 `kubectl apply` + 滚动重启 MySQL 即可恢复原持久化强度
- **观察窗口、指标与结果**：仅 60 秒窗口 + 一次双客户端手测。**观察窗口严重不足**，列为未关闭主因

### 部署踩坑（供后人）

`minikube image load --overwrite=true` 对**同名 `:dev` tag 不会真正覆盖**——load 后节点上仍是旧 image ID，`image rm` 再 load 也无效。本次因此一度误判为"pprof 路由注册方式不对"（实际代码没问题，只是跑的是旧镜像）。**正解是直接在 minikube 的 docker daemon 内构建**（`minikube docker-env` 后 `docker build`）。`start.ps1` 中已有注释提及此坑。

## 10. 剩余风险与行动项

| ID | 严重级别 | 行动项 | 负责人 | 状态 | 目标/关联 Incident |
|---|---|---|---|---|---|
| A-1 | 低 | 直接测 MySQL datadir 的 fsync 成本，把 §5.1 的"强推断"升级为实测事实 | 待指定 | **进行中**（脚本已就绪，见 §12.1；测量时 `pandora` 命名空间恰被清空，待栈起来后执行） | 本档 |
| A-2 | 中 | 核查 `infra.yaml` 中 etcd / kafka / redis 是否也沿用了生产级刷盘默认值 | 待指定 | **已完成，结论见 §12.2**；派生出 A-8（etcd） | 本档 |
| A-3 | 中 | 把 §5.5 的 edge envoy 故障拆为独立 Incident（`index.md` §1 第四条要求独立根因分别建档） | 待指定 | 未开始 | 本档 |
| A-4 | 中 | 把分段计时推广到其它服务的关键写路径（当前只有 owner 有） | 待指定 | 未开始 | 本档 |
| A-5 | 中 | 为"集群内地址必须全限定名"加机械门禁（CI 扫 `deploy/**` 与生成脚本） | 待指定 | 未开始 | 本档 |
| A-6 | 高 | 补足观察窗口：至少一轮完整联调 + 一次压测，确认长尾不再打断准入 | 待指定 | 未开始（**需真实使用，无法由工具单独完成**） | 本档 |
| A-7 | 中 | 验证 edge envoy 修复：重跑 `start.ps1` 后核对 Envoy `/clusters` 解析结果为 ClusterIP | 待指定 | **已可自动核验**（`tools/scripts/tests/edge_envoy_fqdn_contract_test.ps1` 覆盖生成侧；运行期核对脚本见 §12.3） | 本档 |
| A-8 | 中 | **etcd 的 WAL fsync 是同类风险且无法配置放宽**（A-2 派生）：承载 snowflake 发号 / 配置表 watch / writer lease，同一慢盘上会出现同样的长尾，后果是租约抖动与 writer 交接。需实测 `etcd_disk_wal_fsync_duration_seconds` 并决定是换存储还是接受并监控 | 待指定 | 未开始 | 本档 |

## 11. 关闭审核

- [x] 直接根因和放大因素均有证据
- [ ] 修复前失败、修复后通过的回归存在（**无自动化回归**：本次是配置级修复，未写测试断言"提交延迟 < 阈值"）
- [ ] race/集成/故障注入达到本事故风险要求（`-race` 阻断、故障注入未做）
- [x] 同类代码扫描完成（结构性隐患已列 A-2/A-4）
- [x] 目标环境已加载可追溯的新产物（imageID 已核对）
- [x] 玩家路径、恢复和补偿路径验证通过（双客户端手测）
- [ ] 观察窗口无复发（**仅 60 秒 + 一次手测，严重不足**）
- [ ] 剩余风险已解决或另建 Incident/任务（A-1 ~ A-7 全部未开始）
- [x] 文档已脱敏且时间线时区明确

**关闭结论与审批人**：未关闭。主要缺口为观察窗口不足（A-6）、etcd 同类风险未评估（A-8）、A-1 待栈恢复后执行。

---

## 12. 跟进记录（2026-08-12 第二轮）

### 12.1 `-race` 阻断：**已解除**（此前三份档案都写错了）

`INC-20260811-001` / `-002` / 本档最初都把 `go test -race` 记为"环境不支持"的永久阻断。实测证明这是**误判**：

```text
# Windows 宿主(无 gcc)——确实跑不了，但这不是结论
go: -race requires cgo; enable cgo by setting CGO_ENABLED=1

# 挂进本地已有的 golang 镜像 + 模块缓存——直接就过
ok  .../owner/internal/biz      1.021s
ok  .../owner/internal/conf     1.014s
ok  .../owner/internal/data     1.021s
ok  .../owner/internal/service  1.529s
```

`-race` 只是需要一个带 CGO 的 Linux，本机 `golang:1.26.5` 镜像和 5.7G 模块缓存都现成。已固化为 [`tools/scripts/go_test_race.ps1`](../../tools/scripts/go_test_race.ps1)，并在脚本头写明两个必踩对的点（保持 workspace 模式、不要设 `GOFLAGS=-mod=mod`），避免下一个人重新发现。

**owner 全包 race 检测通过，零 data race。**

建议回填修订另两份档案的阻断项结论。

### 12.2 A-2 结论：三个组件风险各不相同

| 组件 | 刷盘行为 | 判定 |
|---|---|---|
| MySQL | 曾每次提交**双 fsync** | 已修（本档） |
| **etcd** | **每次提交 fsync WAL，且无等价放宽开关**（共识存储刻意不提供） | ⚠️ 同类风险，另立 **A-8** |
| Redis | `appendonly yes` + 默认 `appendfsync everysec`（后台线程每秒一次，非每写） | 可接受 |
| Kafka / ZK | 未配 flush 参数 → 默认交给 OS，非每条 fsync | 可接受 |

**etcd 是真正的残留风险**：它承载 snowflake 发号、配置表版本 watch、writer lease，同一块慢盘上会出现与 MySQL 同样的长尾，后果是租约抖动与 writer 交接。**不能靠配置解决**（etcd 无 `trx_commit=2` 等价物），只能换更快的存储或接受并监控 `etcd_disk_wal_fsync_duration_seconds`。

### 12.3 自动化回归：**已补**，且经变异测试验证

新增 [`tools/scripts/tests/infra_durability_contract_test.ps1`](../../tools/scripts/tests/infra_durability_contract_test.ps1)，9 条断言守两条配置级约束 + 一条前提：

1. MySQL 声明 `trx_commit=2` / `sync_binlog=0`，且**不得出现 `=1`**（回退哨兵）；
2. `overlays/online` **不得引用 `infra/`** —— 这是"本地放宽是安全的"这一论断的前提，前提被破坏必须立刻失败，否则弱持久化会上生产；
3. edge envoy 改写产出全限定名——不是 grep 文本，而是**真正载入函数跑样本配置**，断言产出 `login.pandora.svc.cluster.local`、不含短名、且 admin 仍锁 `127.0.0.1`。

**刻意不写"提交延迟 < N ms"的断言**：那种断言依赖跑测机器的磁盘，必然 flaky；而真正会回退的是**配置本身**。延迟由 §8 实测与 A-6 观察窗口负责。

变异验证（证明门禁是活的，不是摆设）：

| 变异 | 结果 |
|---|---|
| `trx_commit=2` → `=1` | **FAILED，2 条断言命中** |
| edge envoy 全限定名 → 短名 | **FAILED，3 条断言命中** |
| 还原 | PASSED |

### 12.4 本轮未完成

- **A-1**：fsync 直测脚本已就绪，但执行时 `pandora` 命名空间恰被清空（用户正在重跑 `start.ps1`），待栈恢复后执行。§5.1 的"强推断"标注维持不变。
- **A-6**：观察窗口需真实使用累积，工具无法单独完成。
- **A-8**：etcd WAL fsync 实测未做。
