# owner

> 每玩家 owner 权威:为「当前哪台 DS 有权控制和修改某玩家」维护唯一权威记录——
> 单调不回退的 `owner_epoch`、owner 类型、exact DS 实例身份、稳定 `operation_id`、
> `admit_not_before` 迁移屏障、`PENDING → ADMITTED` 两阶段准入(不变量 §9.22)。
> 全部 transition 是 `pandora_owner` 库上的**单行串行化 CAS**,进程内不缓存权威态。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见
> `docs/design` 的 [`owner-authority.md`](../../../docs/design/owner-authority.md)(权威本体 / 迁移
> 状态机 / expand→migrate→contract 演进);脑裂再入屏障契约见
> [`battle-reconnect.md §8`](../../../docs/design/battle-reconnect.md)。
>
> 代码行号锚点以**函数名**为准;文中所有 `文件:行号` 截至当前 HEAD,行号会随改动漂移。

## 职责与边界

- **职责**:唯一 owner authority。只有本服务能通过受控 transition API 原子修改 owner 记录;
  login / matchmaker / allocator / DS 等**只能查询或提交绑定 `operation_id` 的命令**,不得各自 raw
  CAS 同一记录(§9.22)。
- **权威态**:`owner_record`(每玩家一行,永不因 TTL 消失)、`ds_instance_lease`(实例级租约,
  deadline 只前进)、`owner_transition_log`(迁移审计)全在 **`pandora_owner` 库**;三表同库同事务域,
  `epoch / lease / admit_not_before / phase` 的读改写单事务完成。
- **强依赖存储**:owner CAS 不可降级。**生产必须连 TiDB**(线性一致 CAS + 法定多数侧单写 + 已确认写
  故障切换不回滚);MySQL 异步复制主从切换会回滚已确认写,一次回滚即可能双 owner 脑裂,故生产禁用
  裸 MySQL。dev 允许单机 MySQL(无复制,天然线性一致)。校验由 `require_tidb` 驱动(见「配置项」)。
- **不做的事**:不签发 DS 票据(签票是 hub_allocator / ds_allocator 的活)、不算派生数值、不维护
  presence(那是 player_locator 的短期投影)、不做 leader 选举或撮合。本服务是无状态 headless gRPC,
  权威全在库里,任意副本可随时被杀被替换。

## 端口(`docs/design/infra.md`)

端口取自 `internal/conf/conf.go` 的 `Defaults()`(`conf.go:38`)。

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20017` | 内部系统 RPC(login / allocator / DS 回调链直连) |
| HTTP | `:21017` | 仅 `/metrics`(`owner.proto` 无 `google.api.http` 注解,不对外挂业务路由) |

## 对外接口

代码入口:`internal/service/owner.go`(实现 `ownerv1.OwnerServiceServer`)。**全部 RPC 是内部系统接口**:
`rejectClientCaller`(`owner.go:31`)拒绝带玩家 JWT 的调用(`pmw.PlayerIDFromContext(ctx) != 0` →
`ERR_PERMISSION_DENY`);合法调用者是内网直连,无 JWT(`callerID == 0`,server 用 `pmw.AuthOptional()`,
`server/grpc.go:20`)。Envoy 侧对 `/pandora.owner.v1/` 前缀另有 403 拦截,双保险。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `QueryOwner(player_id)` | login / hub_allocator / ds_allocator | 读当前 owner 记录(附派生 lease 截止);查询失败调用方一律按 UNKNOWN 处理,不冒充 OFFLINE | **拒玩家 JWT**(callerID 必须 =0) |
| `BeginTransition(player_id, expect_epoch, operation_id, owner_type, target)` | hub_allocator / ds_allocator | 发起 owner 迁移:CAS `expect_epoch → epoch+1 / PENDING / newTarget`,同事务算 `admit_not_before`。EPOCH_CONFLICT 时响应仍带当前记录供 query-first 重查 | **拒玩家 JWT** |
| `Admit(player_id, owner_epoch, operation_id, target)` | hub_allocator / ds_allocator | 准入提交:屏障已开 + epoch/operation/实例全等 → `PENDING → ADMITTED`;屏障未开带 `retry_after_ms`;已 ADMITTED 幂等重放 | **拒玩家 JWT** |
| `RenewInstanceLease(target, lease_seconds)` | hub_allocator / ds_allocator | DS 业务心跳代写实例租约(DS 不直连本服务),deadline 只前进;秒数硬钳到 `placement.DSFenceLeaseMaxSeconds` | **拒玩家 JWT** |
| `ReleaseOwner(player_id, owner_epoch, operation_id)` | login(登出/终局) | epoch+operation 匹配才置 `owner_type=none`(epoch 保留继续单调);迟到调用 compare-delete 自己,幂等 no-op | **拒玩家 JWT** |

> **为什么全内部**:owner 是「一人一 DS」的最终仲裁点(§9.1 / §9.22)。玩家客户端永远经 login /
> allocator 间接影响 owner,不得直连——直连即绕过 fencing。`BeginTransition` / `Admit` 由 **allocator
> 统一出口**发起(hub_allocator 是 Hub 签票出口,ds_allocator 是 Battle READY 交付 / census 出口);
> `RenewInstanceLease` 由两个 allocator 在处理 DS 心跳时代写;`ReleaseOwner` 由 login 在登出链兜底。

## 目录结构(Kratos 标准分层,对齐 login / matchmaker)

```
cmd/owner/main.go            启动入口(yaml→conf / MySQL client + schema gate / require_tidb 校验 /
                             装配 Usecase→Service→gRPC/HTTP / 起审计 sweep goroutine)
etc/owner-dev.yaml           dev 配置(连本机 docker MySQL 的 pandora_owner 库,require_tidb: false)
internal/
  conf/conf.go               配置结构(OwnerConf + Defaults():端口 / sweep / 保留期)
  service/owner.go           RPC 入口(实现 OwnerServiceServer;rejectClientCaller 系统接口守卫;
                             proto ↔ data 结构互转)
  biz/owner.go               OwnerUsecase(入参形状校验:operation UUIDv4 / target 完整性 /
                             owner_type 合法性 / lease 秒数钳制 → 委托 repo)
  data/owner_repo.go         MySQLOwnerRepo(所有 CAS / 屏障 / 幂等在 database/sql 事务内;
                             锁序固定 owner_record → ds_instance_lease,无环无死锁)
  data/backend_check.go      AssertTiDBBackend(require_tidb=true 时校验 VERSION() 含 "-TiDB-")
  server/grpc.go             gRPC server 注册(AuthOptional)
  server/http.go             HTTP server 注册(仅 /metrics)
```

存储 DDL 不在本服务目录:`deploy/mysql-init/15-owner-tables.sql`(dev 单机 MySQL)/
`deploy/tidb-init/02-owner-tidb.sql`(生产 TiDB,同构 DDL)。

## 核心调用链

owner 服务是**被动权威**:没有后台撮合 / 选举循环(唯一后台 goroutine 是审计 sweep)。真实业务由
调用方按 `Begin → (加载资源) → Admit`、心跳 `Renew`、登出 `Release` 驱动,每个 RPC 落成
`pandora_owner` 上的一次单行事务。下图是一次 **Hub→Battle owner 迁移**的端到端链路(锚点见
`internal/data/owner_repo.go`):

```
allocator(hub/ds)                         owner 服务(每玩家 owner_record 单行串行化)
─────────────────────────                 ────────────────────────────────────────────────
BeginTransition(expect_epoch,      ─────► service.BeginTransition (owner.go:86)
  op, owner_type, target)                 └─► biz.BeginTransition 形状校验 (biz/owner.go:39)
                                              · ValidOperationID(canonical UUIDv4)
                                              · owner_type ∈ {HUB, BATTLE}
                                              · target.Complete()(pod/uid/epoch/allocId/track 全非空)
                                          └─► repo.BeginTransition 事务 (owner_repo.go:211)
                                              ├ lockRecordTx  INSERT IGNORE + SELECT..FOR UPDATE
                                              │                 (无行则建 epoch=0/none 再锁, :137)
                                              ├ 幂等重放? 同 op ∧ epoch==expect+1 ∧ 目标全等 → 原样返回
                                              ├ epoch != expect → ErrOwnerEpochConflict(附当前记录, :238)
                                              ├ admit_not_before = max(now, 旧实例 lease deadline)
                                              │                     + DSFenceSkewMarginSeconds(7s)
                                              │                 (readLeaseDeadline FOR UPDATE 挡在途续租, :254)
                                              ├ UPDATE owner_record → epoch+1 / PENDING / newTarget
                                              └ appendTransitionLog(op=begin)
   ── 新 DS 只加载地图级资源,屏障开前不建可操作 Pawn / 不处理输入 / 不 ADMITTED ──
Admit(owner_epoch, op, target)     ─────► service.Admit (owner.go:103)
                                          └─► repo.Admit 事务 (owner_repo.go:299)
                                              ├ SELECT..FOR UPDATE
                                              ├ 身份不符(epoch/op/target/owner_type)→
                                              │     ErrOwnerIdentityMismatch(fail-closed, :310)
                                              ├ 已 ADMITTED → 幂等重放(回包丢失重放同结果, :322)
                                              ├ now < admit_not_before →
                                              │     ErrOwnerBarrierNotOpen(+retry_after_ms, :330)
                                              └ UPDATE phase=ADMITTED + appendTransitionLog(op=admit)

DS 业务心跳(每 ~5s,经 allocator 代写)
RenewInstanceLease(target, secs)   ─────► repo.RenewInstanceLease 事务 (owner_repo.go:359)
                                              ├ SELECT..FOR UPDATE ds_instance_lease
                                              ├ 无行 → INSERT;实例纪元守卫(双方非零且不同 →
                                              │     ErrOwnerLeaseRegressed, :390);存量 0 → 补齐纪元
                                              └ deadline 只前进(乱序迟到续租幂等返回现值)

登出 / 终局(login)
ReleaseOwner(owner_epoch, op)      ─────► repo.Release 事务 (owner_repo.go:422)
                                              ├ SELECT..FOR UPDATE
                                              ├ epoch/op 不符(迟到)→ 幂等 no-op 返回当前(:433)
                                              └ UPDATE owner_type=none + 清 target(epoch 保留)+ log
```

**要点(§9.22 / §9.23)**:

1. **owner_record 行是每玩家的串行化锚点**——每个 transition 先 `SELECT ... FOR UPDATE` 锁该行,
   epoch 单调 CAS、`admit_not_before` 计算、`PENDING → ADMITTED` 推进全在同一事务内完成。锁序固定
   `owner_record → ds_instance_lease`,`Renew` 只锁 lease 行,无环无死锁(`data/owner_repo.go:1` 顶注)。
2. **admit_not_before 在 CAS 线性化点算定**:取该点观察到的旧实例租约最晚截止(`FOR UPDATE` 挡住在途
   续租)+ 安全余量,CAS 后旧 epoch 的续租一律失败。**核心时序**:旧 DS 最晚停止可玩时间 < 新 DS
   最早开始可玩时间(屏障 = 该分界)。无旧 owner → 无需屏障(`admit_not_before = now`)。
3. **端到端幂等**:一次真实进场 / owner 迁移用一个稳定 `operation_id` 作幂等键(`(player, operation)`)。
   响应丢失后原样重试 `Begin` 拿回同一结果、不再推进 epoch;`Admit` 回包丢失重放返回同一 `ADMITTED`,
   不再分配、不创建第二 owner。
4. **query-first 重查**:`BeginTransition` epoch 冲突时把当前记录随响应返回(`service/owner.go:95`),
   调用方据此重查再决策,**禁盲重试推进 epoch**。屏障未开返回 `retry_after_ms`,由调用方 watchdog
   到期重试(安全优先但不永久卡)。

## 存储(`pandora_owner` 库)

DDL:`deploy/mysql-init/15-owner-tables.sql`(dev)/ `deploy/tidb-init/02-owner-tidb.sql`(生产)。
`cmd/owner/main.go` 启动时对这三张表做 schema gate(缺表 fail-fast,`main.go:85`)。

| 表 | 主键 | 关键列 | 语义 |
|---|---|---|---|
| `owner_record` | `player_id` | `owner_epoch`(单调 +1)/ `owner_type`(0 none/1 hub/2 battle)/ `phase`(1 PENDING/2 ADMITTED)/ exact target 五元组 / `operation_id` / `admit_not_before_ms` | 每玩家 owner 权威;**永不因 TTL 消失**,Release 置 `owner_type=none` 但 epoch 保留继续单调 |
| `ds_instance_lease` | `instance_uid` | `pod_name` / `instance_epoch` / `lease_deadline_ms`(只前进) | DS **实例级**租约(非玩家级),allocator 心跳代写;玩家 owner lease 由此派生 |
| `owner_transition_log` | `id` | `player_id` / `from_epoch` / `to_epoch` / `op`(1 begin/2 admit/3 release)/ `operation_id` / `created_at` | 迁移审计流水(append);`idx_created_at` 供 90 天保留期 sweep(§9.24) |

- **exact target 五元组**:`pod_name` + `instance_uid` + `instance_epoch` + `assignment_or_allocation_id`
  + `release_track`,`OwnerTarget.Equal`(`owner_repo.go:53`)全等才认——**同名换实例不相等**(Pod 复用
  uid / epoch 回退都被 fencing 挡住)。
- **实例租约是实例级不是玩家级**:60 万玩家逐个续租会压垮任何权威库,故按 DS 实例续租,玩家 owner
  lease 从其 owner 实例的租约行派生(`QueryOwner` / `readLeaseDeadline` 同事务读出派生 `LeaseDeadlineMs`)。
- **审计保留期**:`owner_transition_log` 登记在 §9.24 只增表清单(90 天);由 `main.go` 的
  `runTransitionLogSweep` goroutine 周期 `DELETE ... LIMIT`(`SweepTransitionLog`,`owner_repo.go:462`)。

## 关键设计点 / 不变量

| 主题 | 约束 | 代码锚点 |
|---|---|---|
| 唯一 authority | 只有本服务能改 owner;调用方提交绑定 operation 的命令,不各自 raw CAS | `service/owner.go` 全体 RPC |
| 单行串行化 | 每 transition `SELECT ... FOR UPDATE` 锁 owner_record 行,读改写单事务 | `lockRecordTx` / `BeginTransition` / `Admit` / `Release` |
| epoch 单调 | CAS `expect → epoch+1`;冲突附当前记录返回,不盲重试推进 | `BeginTransition`(:238) |
| 迁移屏障 | `admit_not_before` 在 CAS 点算定 = max(now, 旧 lease deadline)+ skew;旧 epoch 续租随即失败 | `BeginTransition`(:254)/ `Admit`(:330) |
| fail-closed 准入 | Admit epoch/op/实例任一不符即拒(旧 epoch / 换代实例 / 伪造 operation 都进不来) | `Admit`(:310) |
| 端到端幂等 | 幂等键 =(player, operation);Begin/Admit/Release 响应丢失重放同结果 | `BeginTransition`(:225)/ `Admit`(:322)/ `Release`(:433) |
| 实例纪元 fencing | 换代实例(双方非零且不同)不得续旧租约行;deadline 只前进 | `RenewInstanceLease`(:390) |
| lease 秒数钳制 | 续租秒数硬钳 ≤ `placement.DSFenceLeaseMaxSeconds`(20s),配置无法放大脑裂窗口 | `biz.RenewInstanceLease`(:80) |
| 正确性常量单源 | fence / skew / lease 常量不在配置,单一来源 `pkg/placement`,禁调优 | `pkg/placement`(skew 7s / lease 20s) |
| TiDB 强依赖 | require_tidb=true 时校验后端确为 TiDB,否则 fail-fast 拒启(生产禁裸 MySQL) | `data.AssertTiDBBackend` |

## 配置项(`internal/conf/conf.go`)

`owner.*` 私有配置 + `Defaults()`(`conf.go:38`)。fence / lease 协议常量**不在配置里**(单一来源
`pkg/placement`,正确性常量禁调优);本配置只管端口、TiDB 门禁与审计清理节奏。

| 键 | 默认 | 说明 |
|---|---|---|
| `server.grpc.addr` | `:20017` | gRPC 监听(内部 RPC) |
| `server.http.addr` | `:21017` | HTTP 监听(仅 `/metrics`) |
| `owner.require_tidb` | `false` | 启动强校验权威库确为 TiDB(§9.22)。dev 保持 false(单机 MySQL 天然线性一致);`-Prod` 产物由 `gen_cluster_config.ps1` 机械注入 `true`,不允许线上产物继承 dev 宽松档 |
| `owner.sweep_interval` | `5m` | 审计流水清理轮询间隔(多副本各自跑,DELETE 幂等无需锁) |
| `owner.sweep_batch` | `500` | 每轮清理行数上限(有界批量,防长事务锁表) |
| `owner.log_retention_days` | `90` | `owner_transition_log` 保留天数(§9.24) |
| `node.mysql_client.dsn` | 无(必填) | `pandora_owner` 库 DSN;缺失 fail-fast(`main.go:75`)。生产必须指向 TiDB |

## 本地启动

```powershell
# 1. 基础设施:起本机 docker MySQL,并确保 pandora_owner 库已建、三张表已导入
#    (deploy/mysql-init/15-owner-tables.sql;首次或换 volume 后需手动跑,启动有 schema gate)
pwsh tools/scripts/dev_up.ps1

# 2. 启 owner(dev 配置:连 127.0.0.1:3307 的 pandora_owner,require_tidb: false)
go run ./services/runtime/owner/cmd/owner -conf services/runtime/owner/etc/owner-dev.yaml
```

> dev 模板 `require_tidb: false`,连单机 MySQL 即可联调;生产产物注入 `true` 并指向 TiDB DSN,
> 后端非 TiDB 时启动即 fail-fast。

## 关联文档

- [`owner-authority.md`](../../../docs/design/owner-authority.md) — 权威本体 / 数据模型 / transition
  状态机 / expand→migrate→contract 演进 / 失败模式验证矩阵(本服务设计权威)
- [`infra.md`](../../../docs/design/infra.md) — 服务端口 / key 命名规范(20017/21017)
- [`battle-reconnect.md`](../../../docs/design/battle-reconnect.md) §8 — DS 授权租约 fencing / 再入
  屏障契约(admit_not_before 时序的下游语义)
- [`zero-downtime-update.md`](../../../docs/design/zero-downtime-update.md) — 不停服滚动 / 金丝雀下的
  owner epoch 与 Admission 门(§9.16/§9.21)
- [`go-services.md`](../../../docs/design/go-services.md) §2.6 — player_locator presence 投影与 owner
  权威的边界(locator 只作 presence,不是归属权威)
