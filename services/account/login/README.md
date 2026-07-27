# login

> 账号登录 / 登出服务:校验账号密码 → 签发 Session Token(HS256 JWT)→ 解析大厅落点并颁发 Hub DS 票据,
> 兼做断线重连(直连原 Battle DS)、选角落库、DS 票据签发 / 校验(JWT + Redis 防重放)。
>
> 本 README 是**模块级说明**(职责 / 端口 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`
> 的 [`go-services.md §2.1`](../../../docs/design/go-services.md)、[`battle-reconnect.md`](../../../docs/design/battle-reconnect.md)、
> [`session-generation-rollout.md`](../../../docs/design/session-generation-rollout.md)、
> [`decision-revisit-ds-callback-auth.md`](../../../docs/design/decision-revisit-ds-callback-auth.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:账号登录 / 登出;签发 Session Token + Hub DS 票据;断线重连回原 Battle DS;选角落库(`player_roles` 权威);
  DS 票据签发 / 校验(JWT + Redis JTI 防重放)。
- **权威态**:login 是**账号数据 + 会话代际 + 已选角色**的权威(MySQL `pandora_account`);会话现行性(顶号 fencing)
  权威在 **Redis session**(`pandora:sess:<player_id>`)+ MySQL 单调 `generation`(登录定序)。
- **不是权威、只查询**(不变量 §22):玩家当前位置查 player_locator(presence 投影,30s TTL,不复制);玩家是否在活跃对局
  查 matchmaker(`ResolvePlayerMatchContext`,耐久 claim 才是"在局"证明);Hub 分片 / Hub 票据签发权威在 hub_allocator;
  owner 归属权威在 owner 服务(登出弱释放)。
- **不做的事**:不签 Hub v2 票(RS256 hub 票只能由 hub_allocator 签,login 只透传 role_id / sjti / 回流 match_id);
  不算 MMR / 经验 / 掉落;正式注册流程不属 login 职责(仅有 dev 假注册开关)。

## 端口(`docs/design/infra.md`)

取值见 `internal/conf/conf.go` 的 `Defaults()`(`Server.Grpc/Http.Addr`)。

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:50001` | 主流量(客户端 → Envoy `:8443` gRPC-Web → 本服)+ 内部 RPC |
| HTTP | `:51001` | `/metrics` Prometheus + Kratos RESTful(`/v1/login` 等,运营 / 联调低 QPS) |

> **VerifyDSTicket 网关例外**:Redis authority 下 UE DS 的在线 `VerifyDSTicket` 只走独立 Envoy `:8444` exact route
> (`:8443` 对该 path 精确 403);网络位置本身不构成身份,仍须过 DS Bearer + Redis active 门(见「Hub / Battle 票据签发与验证」)。

## 对外接口

代码入口:`internal/service/login.go`(实现 `loginv1.LoginServiceServer`;gRPC server 装 `pmw.AuthOptional()`,
把 Envoy jwt_authn 注入的 `x-pandora-player-id` / `x-pandora-jwt-payload` 解进 ctx——请求体自报 `player_id` 一律不信任)。
proto 见 `proto/pandora/login/v1/login.proto`。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `Login(account, password_hash, device_id)` | 客户端 | 校验密码 → 签 session + 解析 Hub/Battle 落点 + resume;**立即完成型**,response 含完整进场数据 | 账号密码自证(Envoy 对本 path 不强制 JWT) |
| `GetResumeContext(session_token)` | 客户端(冷启动 / 前台恢复) | 纯读当前应进 HUB / BATTLE 路由(不做任何 placement mutation) | session_token 验签 + 会话现行性门 |
| `SelectRole(role_id)` | 客户端 | 选角落库(`player_roles` 覆盖式)+ 重签当前有效 Hub 票 | Envoy JWT `player_id` + payload jti 现行性门 |
| `Logout(session_token)` | 客户端 | CAS 删本代 Redis session + MySQL 墓碑 + 弱释放 owner;容错(验签失败也返 OK) | session_token(容错) |
| `IssueDSTicket(ds_type, target_id, session_token)` | 客户端(结算回大厅 / 断线重连) | `hub`=复用 AssignHub 拿当前有效大厅地址 + 新票;`battle`=经 roster 权威门现签重连票 | Envoy JWT `player_id` + session token 现行性门 |
| `VerifyDSTicket(ticket, ds_pod_name, admission_id)` | UE DS(经 `:8444`) | 验签 + exp + JTI 防重放 +(redis authority)active/binding/admission 门 | off/legacy=内部信任;redis=DS Bearer Guard + Redis active |

> **立即完成型(协议原则 1,见 [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md))**:`Login` / `SelectRole` /
> `IssueDSTicket` 的 response 必须携带完整业务数据(session / 票据 / 直连地址 / resume),客户端**不等任何后续 push**。
> 错误一律翻译成 `LoginResponse.code`(`errcode.*` → proto enum,不抛 gRPC error),客户端永远看 `code` 字段。

## 目录结构(Kratos 标准分层,对齐 matchmaker)

```
cmd/login/main.go                 启动入口(config → snowflake → JWT signer/verifier → data/biz/service 三层装配 → gRPC/HTTP)
etc/login-dev.yaml                开发期配置(免密 / 假注册 / 全依赖 addr 已填)
etc/login-prod.yaml.example       生产配置样例
etc/killswitch.yaml               RPC 级临时关停开关(file source)
internal/
  conf/conf.go                    配置结构(嵌入 pkg/config.Base;Defaults() 填默认;Validate() 校验冲突)
  service/
    login.go                      RPC 入口(实现 LoginServiceServer,proto↔biz 互转,errcode→code)
  biz/
    login.go                      LoginUsecase:Login / SelectRole / Logout / Resolve*Endpoint / 会话现行性门
    ticket.go                     TicketUsecase:IssueDSTicket / VerifyDSTicket(签发 + roster 门 + admission 门)
    owner_release.go              OwnerReleaser 接口(登出弱释放 owner,owner-authority.md migrate ⑤)
  data/
    account.go                    MySQL accounts/devices/bans 仓储 + Redis session/ticket-JTI 仓储 + PurgeStaleDevices
    role.go                       player_roles 仓储(选角权威,SetRole 同事务代际 fencing)
    session_generation.go         player_session_generations 仓储(登录单调代际,定序权威)
    locator_client.go             player_locator gRPC client(LOGIN_PENDING 上报 / BATTLE 位置查询)
    hub_client.go                 hub_allocator gRPC client(AssignHub)
    match_client.go               matchmaker gRPC client(ResolvePlayerMatchContext,internalrpcauth 签名)
    owner_client.go               owner 服务 gRPC client(Query/Release)
    battle_ticket_authorizer.go   battle 票 roster 权威门 + 三态路由 inspector(RedisBattleTicketAuthorizer)
    hub_assignment_binding.go     Hub 归属绑定 Redis checker(CheckCurrent / CheckCurrentB1)
    ds_admission.go               Redis authority 在线入场 active checker
  server/
    grpc.go                       gRPC server 注册(AuthOptional middleware)
    http.go                       HTTP server 注册(/metrics + RESTful)
```

## 核心调用链

### 1. Login —— 密码校验 → 会话定序 → 断线重连兜底 → Hub 落点

`LoginUsecase.Login`(`biz/login.go:278`):

```
Login(account, password_hash, device_id)
├─ repo.FindByAccount              查 player_id + bcrypt 哈希(缺账号 + dev 开关 → ensureAccount 懒注册)
├─ passwd.Verify / devSkipPassword 校验密码(免密开关跳过)
├─ repo.CheckBanned                封禁拦截(account_bans)
├─ signer.SignSession              签 session JWT(sessJTI = uuid v4)
├─ sessionGen.PersistSessionJTI    ★ MySQL 原子 +1 分配单调 generation(登录定序权威,fail-closed)
├─ sessions.Set(..., gen)          ★ Redis「仅更高代际可覆盖」条件写(输掉定序 → ErrSessionSuperseded,不发凭据)
├─ routeRegionCell                 cellroute 算确定性 region/cell 落点(单 Cell/dev → 0/0)
├─ tryBattleReconnect              ★ locator BATTLE 租约 + match 三态门:Active→直连原局并 return;Terminal→带回流 fence 进 Hub
├─ loadSelectedRole                读 player_roles(弱依赖,失败按 0)
├─ notifier.NotifyLoginPending     B1 严格档:分配 Hub 前先写 LOGIN_PENDING 权威位置(失败 fail-closed)
├─ resolveHub                      ★ hub_allocator.AssignHub 拿真实地址 + 票(校验票据);未配/失败 → 自签回退(非严格档)
├─ repo.TouchDevice                记录设备(失败仅告警)
└─ fenceLoginDelivery              ★ 交付终检:sessJTI 仍是当前一代才返回凭据(并发新登录轮换 → 扣留)
```

- **断线重连**(`tryBattleReconnect`,`biz/login.go:714`):locator presence 命中 BATTLE,或 presence 蒸发但 matchmaker
  `ACTIVE+READY` claim 兜底(`resolveBattleAuthority`,`biz/login.go:651`),再经 `InspectBattleRoute` 三态分诊——
  `Active`=签重连票回原局;`Terminal`=locator 仅 TTL 残留,带原 `match_id` 回流 fence 进 Hub;`Unknown`=`Unavailable` 可重试
  (最长 ~30s 租约到期自愈,永不永久卡死,§9.19 no-freeze)。
- **会话定序 / 部分失败**(`biz/login.go:321` 起注释):先由 MySQL 分配单调
  generation，再由 Redis Lua 仅接受更高 generation；同 `(jti,generation)` 的
  lost-reply 重试幂等成功，同 generation 不同 jti 按完整性冲突 fail-closed。
  Redis 写结果不确定时绝不恢复“即时前代”——连续 A→B→C 登录中，B 也可能从未
  交付。`reconcileFailedSessionWrite` 分别用独立有界预算写两侧无能力墓碑：MySQL
  仅条件命中本次 `(jti,generation)`；Redis 在 `current.gen <= failedGen` 时清除
  token/jti/device/exp 并推进水位，更高代际赢家保持不变。遗留 `_rollback_*` 只在
  滚动兼容期被清理，不再参与恢复。失败 RPC 返回可重试错误；下一次 Login 用更高
  generation 原子推进两处权威自愈。

### 2. IssueDSTicket —— 结算回大厅 / 断线重连补票

`LoginService.IssueDSTicket`(`service/login.go:152`)→ 先 `RequireCurrentSessionToken`(会话现行性门)→ 按 `ds_type` 分流:

```
ds_type == "hub"    → LoginUsecase.ResolveHubEndpointFromMatch (biz/login.go:978)
                        └─ guardHubRouteAgainstActiveBattle  active-BATTLE 三态门(Active 拒/Terminal 放行带回流 fence/Unknown 可重试)
                        └─ resolveHub                        AssignHub 拿"当前有效"大厅地址 + 全新一次性票(旧地址可能已被 Agones 重建)
ds_type == "battle" → LoginUsecase.ResolveBattleEndpoint  (biz/login.go:575)
                        └─ battleTicketIssuer.IssueBattleDSTicketAtCell → roster 权威门现签(见下 §4 门链)
ds_type == 其它     → TicketUsecase.IssueDSTicket        (biz/ticket.go:211,legacy 直签,受 RS256 profile 限制)
```

每条分支签票后再过一次 `fenceTicketDelivery`(交付终检,同 Login):检查与签票之间被顶号则扣留票据(票据从未离开服务端 = 未取得)。

### 3. SelectRole —— 选角落库 + 重签 Hub 票

`LoginService.SelectRole`(`service/login.go:101`)→ ctx 取 `player_id` + payload jti → `RequireCurrentSessionJTI` 预检 →
`LoginUsecase.SelectRole`(`biz/login.go:1087`):

```
SelectRole(player_id, role_id, sessJTI)
├─ guardHubRouteAgainstActiveBattle  Hub 副作用入口先过 active-BATTLE 三态门
├─ allowedRoleIDs 白名单校验          非空=严格白名单;空 + !devAllowAnyRole → fail-closed 拒绝(防改包签任意 role_id)
├─ roleRepo.SetRole(expectedSessJTI, precommit)  同一 MySQL 事务内 UPSERT + FOR UPDATE 复核代际(precommit 读 Redis 纵深)
└─ resolveHub(role_id)               把 role_id 签进新 Hub 票 + 返回当前有效地址
```

落库后 service 层再做一次 `RequireCurrentSessionJTI` 交付终检(`service/login.go:125`)。

### 4. VerifyDSTicket —— UE DS 在线入场(redis authority 门链)

`LoginService.VerifyDSTicket`(`service/login.go:227`):

```
redis authority(SetRedisDSAdmissionAuthority 已装配):
  ① dsGuard.CheckCredential      DS Bearer 验签 + 请求 pod scope(ds_pod_name 空 → InvalidArg)
  ② admissionChecker.CheckActive Redis active/projection(DS 是否当前在岗)
  ③ VerifyDSTicketForAdmission   ticketUC 比对票内 binding/assignment/roster + 原子 MarkUsedByAdmission(30s 幂等窗)
  ④ RequireTicketSessionCurrent  票内 sjti 非空则复核会话现行性(签发→兑换间被顶的旧票在此作废)
off/legacy:ticketUC.VerifyDSTicket → 验签 + 单次 JTI SETNX 防重放(保留既有内部语义)
```

`TicketUsecase.verifyDSTicket`(`biz/ticket.go:403`)内部固定顺序:验签(`verifyDSTicketSignature`)→ admission binding 严格 /
重试校验 → **会话现行性门前置到 replay marker 之前**(R7 P2-1,被顶旧票不消耗 jti 名额)→ 写 replay marker。

### 5. Logout —— CAS 删本代会话

`LoginUsecase.Logout`(`biz/login.go:1233`):验签拿 `player_id` → `sessions.DeleteIfJTI`(仅删本 jti 那一代,顶号后旧设备迟到
Logout CAS 不命中 → no-op)→ `sessionGen.TombstoneSessionJTI`(MySQL 墓碑,弱)→ `ownerReleaser` Query+Release(弱)。
token 验签失败也返回 OK(客户端 fire-and-forget)。

## 鉴权模型与会话现行性门(顶号 fencing)

JWT 验签只证明"曾登录过",**顶号后旧 token 在 exp 前仍验得过**——两台设备可各自拿票造成双在场。login 用四道纵深封住:

| 门 | 位置 | 作用 |
|---|---|---|
| 会话现行性门 | `requireCurrentSession`(`biz/login.go:1350`) | Redis session `jti` 现行性判定;不匹配 → `ErrSessionSuperseded`(顶号专属码,→ gRPC ABORTED,区别于自然过期的 `ErrUnauthorized`,防互踢死循环) |
| 登录定序 | MySQL 单调 `generation` + Redis 条件写 | 并发登录确定性定序,输掉的登录不发凭据(`RedisSessionRepo.Set` Lua `setIfNewerGen`) |
| 交付终检 | `fenceLoginDelivery` / `fenceTicketDelivery` | 分配 / 签票等副作用后、返回凭据前复核 jti 仍现行,期间被顶则扣留(诚实边界:检查与写出间仍有进程内窗口,但旧票在下游各门被拒) |
| 票内 sjti 绑定 | `RequireTicketSessionCurrent`(`biz/login.go:1299`) | Hub/Battle v2 票携带签发方 `sjti`,VerifyDSTicket 兑换点复核;空 sjti 由 `require_ticket_sjti` 门控(默认兼容放行) |

- **SetRole 代际 fencing**:`session_generation_enforce=true` 时,`SetRole` 在同一 MySQL 事务内 `SELECT ... FOR UPDATE`
  复核 `player_session_generations`,与登录代际写串行化,确定性挡旧会话(主防线);关闭时仅 emit(双写)+ Redis precommit(纵深)。
- **激活顺序硬约束**(提前开会误拒 / 硬拒合法会话):两个强制门都以「Redis 会话权威存在」为前提,缺 Redis 时 main
  `fail-fast`(`cmd/login/main.go:211`)。发布阶段序见 [`session-generation-rollout.md`](../../../docs/design/session-generation-rollout.md)。

## Hub / Battle 票据签发与验证

- **Battle 票统一入口**:所有 login 侧 Battle 签票(公共 `IssueDSTicket(battle)` + 登录断线重连)都经
  `IssueBattleDSTicketAtCell`(`biz/ticket.go:249`)先过 `AuthorizeBattleTicket` **player↔match roster 权威门**——
  签名前必须从 Redis 证明玩家属于 live match roster;Redis / 坏 protobuf / 空 roster / 陈旧心跳 / Model-B 漂移均 **fail-closed**,
  不再直接相信 `target_id` 或 locator。重连地址用 authorizer 同一 Redis 快照里的 live `ds_addr`,不回退 locator 旧地址。
- **三态路由门**(`InspectBattleRoute`):`Active`(确属 live 对局)/ `Terminal`(权威记录显式 ended/abandoned,唯一允许进 Hub 的证明)/
  `Unknown`(roster 漂移 / 非成员 / 记录缺失 / stale / 错误 → fail-closed)。P0 修复:不得用通用 `ErrPermissionDeny` 冒充终态证明。
- **RS256 v2 profile**(方案 B):配置 `login.ds_ticket.private_key_file` 即启用;启用后 login 侧 battle 票全走 v2 实例绑定签发
  (绑定来自 roster 门同一 Redis 快照,缺失 fail-closed),**hub 票一律拒签**(v2 hub 票只能由 hub_allocator 签);
  legacy HS256 只留给完全未启用 v2 的 local/off。
- **Model B redis authority**:`ds_auth.authority_mode=redis`(须同时 `require_hub_assignment_binding=true` 且 fence 一致)时,
  VerifyDSTicket 走 Guard → Redis active/projection → binding/assignment/roster → admission JTI marker;相同小写 UUIDv4 `admission_id`
  仅在 30s 短窗内幂等恢复响应丢失。
- **dev 回退**:local/off 且未启用 v2 / binding 时,hub_allocator 不可用可回退自签 HS256 hub 票 + 静态 `mock_hub_ds_addr`
  (login 与 hub_allocator 共享同一 JWT secret/issuer/audience,便于本机不起 hub_allocator 也能联调)。

## Hub 归属绑定激活栅栏

`login.require_hub_assignment_binding` 默认 `false`(供新旧副本滚动兼容;带绑定的 Hub 票仍会严格查 Redis 当前归属)。切 `true` 后:

- 无完整 `(assignment_id, pod, uid, epoch, gen, credential_jti, writer_epoch)` 绑定的 Hub 票一律拒绝;
- login 禁止自签 Hub 票和静态地址回退,只接受 hub_allocator 权威签发结果;
- Redis 或 `login.hub.addr` / `login.locator.addr` / `hub_assignment_fence` 未配置会在启动期直接失败(`conf.Validate`);
- assignment missing/mismatch 不消费票据 jti,Redis 故障或坏 protobuf 返回 `Unavailable`。

> **生产阻断**:上述代码门**不等于** DS auth 已可 Apply。blue/green 行为激活、跨普通轮换旧 Hub 票据 grace、
> Battle terminal outbox、revisioned immutable Secret/keyset、digest 与集群内 synthetic 尚未闭环;详见
> [`decision-revisit-ds-callback-auth.md`](../../../docs/design/decision-revisit-ds-callback-auth.md) §7.15–§7.16。

## 存储布局

**MySQL `pandora_account`**(DSN 必填,为空启动失败;启动期 `CheckTables` / `CheckColumnSpecs` 校验 schema):

| 表 | 用途 | 关键约束 |
|---|---|---|
| `accounts` | 账号 → `player_id` + bcrypt 密码 | `uk_account` 唯一(同账号名稳定 `player_id`) |
| `account_devices` | 最近登录设备(`TouchDevice` upsert) | `last_login_at` 超期批删(§9.24,`PurgeStaleDevices`,默认 90 天) |
| `account_bans` | 封禁(`player_id` / `device_id`,`expires_at`) | §9.24 登记豁免(运营合规审计,不清理) |
| `player_roles` | 玩家当前已选角色(选角权威) | 覆盖式 upsert;`SetRole` 同事务代际 fencing |
| `player_session_generations` | 登录单调 `generation` + 当前 `sess_jti` | 登录定序权威;`000003` 迁移新增 `generation` 列 |

**Redis**(`node.redis_client`,可空 = dev 裸跑降级):

| key | 类型 / TTL | 用途 |
|---|---|---|
| `pandora:sess:<player_id>` | hash / session TTL(默认 24h) | 会话状态:`token`/`jti`/`device_id`/`exp_ms`/`gen`;现行性门 + 顶号 CAS |
| `pandora:ticket:<jti>` | string / `ds_ticket_ttl` | DSTicket 防重放:legacy SETNX;redis authority 为 `admission-v4` 版本化 marker(30s 幂等窗) |

## 配置项(`internal/conf/conf.go`)

`Config` 内嵌 `pkg/config.Base`(`Server`/`Node`/`Snowflake`/`Locker` 等公共字段);以下为 login 私有键。默认值来自 `Defaults()`。

| 键 | 默认 | 说明 |
|---|---|---|
| `login.session_token_ttl` | `24h` | session_token 有效期(Redis TTL + JWT exp) |
| `login.ds_ticket_ttl` | `5m` | DS 票据有效期(不变量 §3 短时效) |
| `login.device_retention_days` | `90` | `account_devices` 保留期(§9.24) |
| `login.mock_hub_ds_addr` | `127.0.0.1:7777` | hub_allocator 不可用时的本地回退大厅地址(仅 dev / 非严格档) |
| `login.require_hub_assignment_binding` | `false` | Hub 归属绑定激活栅栏(见上节);`true` 需 Redis + hub.addr + locator.addr + fence |
| `login.hub_assignment_fence` | — | 与 DS Redis authority 共用的 etcd capability lease(binding=true 时必填) |
| `login.session_generation_enforce` | `false`(dev yaml `true`) | SetRole 同事务 MySQL 代际强制门;前提=全 fleet emit + 旧版本排空 |
| `login.require_ticket_sjti` | `false`(dev yaml `true`) | VerifyDSTicket 空 sjti 硬拒门;前提=签发面必带 sjti + 等满票据最大 TTL |
| `login.owner_addr` | 空 | owner 服务地址;空=不接(登出弱 Query+Release,owner-authority.md migrate ⑤) |
| `login.dev_skip_password` | `false` | ⚠️ dev 免密:跳过 bcrypt + 未知账号懒注册(严禁生产) |
| `login.dev_auto_register` | `false` | ⚠️ dev 首登假注册:存本次密码 bcrypt(严禁生产) |
| `login.allowed_role_ids` | 空 | 选角白名单;非空=严格白名单,空=fail-closed 拒绝 SelectRole(除非 dev_allow_any_role) |
| `login.dev_allow_any_role` | `false` | ⚠️ dev 选角宽松:白名单空时只校 role_id 非 0(严禁生产) |
| `login.jwt.issuer` / `audience` / `secret` | `pandora-login` / `pandora-client` / dev 默认 | HS256;须与 `deploy/envoy/envoy.yaml` jwt_authn provider(base64url secret 填 JWKS)一致 |
| `login.jwt.additional_secrets` | 空 | 仅校验用的额外密钥,支持玩家 JWT 不停服三段式轮换 |
| `login.jwt.session_ttl` / `ds_ticket_ttl` | 同上两 TTL | JWT 侧 TTL(默认跟随 `session_token_ttl` / `ds_ticket_ttl`) |
| `login.ds_ticket` | 空=legacy HS256 | DSTicket v2(RS256,方案 B)签发 / 验证配置;`private_key_file` 非空即启用 v2 profile |
| `login.locator.addr` | `127.0.0.1:50006` | player_locator;空仅允许 local/off;binding=true 时为 Hub 分配前权威门 |
| `login.hub.addr` / `region` | `127.0.0.1:50021` / — | hub_allocator(round_robin LB,生产用 `dns:///` headless);空=回退自签 |
| `login.matchmaker.addr` / `auth_secret` / `auth_audience` | `127.0.0.1:50011` / — / — | matchmaker 只读权威兜底;`auth_*` 须与 matchmaker `match_resume_auth_*` 一致(独立随机密钥,≥32 字节,不复用玩家 JWT) |
| `ds_auth.*`(顶层) | `mode=off` / `authority_mode=legacy` | UE DS 在线 VerifyDSTicket 入场鉴权;`redis` 模式须 binding=true + fence 一致(`CapabilityFence`) |

> **`Validate` 强校验**:`ds_ticket` signer/verifier 必须显式 `active_kid`(+ verifier 的 `keyset_revision`);`authority_mode=redis`
> 要求 `require_hub_assignment_binding=true` 且 `ds_auth.fence` 与 `hub_assignment_fence` 完全一致(单一 capability lease,main 只 Acquire 一次)。

## 本地启动

```powershell
# 1. 基础设施(MySQL pandora_account / Redis;dev 一键还含 locator/hub_allocator/matchmaker/owner)
pwsh tools/scripts/dev_up.ps1

# 2. 启 login(dev 配置:免密 + 假注册 + 全依赖 addr 已填)
cd F:\work\XuanMing-Server
go run ./services/account/login/cmd/login -conf services/account/login/etc/login-dev.yaml
```

### 验证(需装 grpcurl)

```powershell
# 直连 gRPC(dev 免密:随便填账号名即进)
grpcurl -plaintext -d '{\"account\":\"test\",\"password_hash\":\"abc\",\"device_id\":\"d1\"}' `
  127.0.0.1:50001 pandora.login.v1.LoginService/Login

# 走 HTTP RESTful
curl -X POST http://127.0.0.1:51001/v1/login `
  -H "Content-Type: application/json" `
  -d '{"account":"test","password_hash":"abc","device_id":"d1"}'

# Prometheus 抓 metrics
curl http://127.0.0.1:51001/metrics | Select-String pandora
```

> `dev_skip_password` / `dev_auto_register` / `dev_allow_any_role` 是**纯 dev / 联调开关,默认 `false`,绝不能上生产**
> (否则任意账号名 + 任意密码可登入任意 `player_id`,或改包客户端签任意 role_id 进 hub 票);启动时各打 `*_ENABLED` 警告日志。
> login 不再支持未配 MySQL DSN 的内存 mock:DSN 为空启动失败。

`dev_auto_register` × `dev_skip_password` 正交组合(仅本机联调,生产两者留 `false`):

| `dev_auto_register` | `dev_skip_password` | 行为 |
|---|---|---|
| `false` | `false` | 正常:账号必须存在 + 密码必须匹配 |
| `true` | `false` | 假注册:未知账号首登即注册并存本次密码,后续用同密码走正常 bcrypt 校验(错密码仍拦) |
| `false` | `true` | 免密:已存在账号任意密码放行;未知账号也会被懒注册 |
| `true` | `true` | 最宽松:任意账号名 + 任意密码都能进 |

## 关联文档

- [`go-services.md §2.1`](../../../docs/design/go-services.md) — login 要约(RPC / 依赖 / 状态)
- [`infra.md`](../../../docs/design/infra.md) — 服务端口 / key 规范
- [`battle-reconnect.md`](../../../docs/design/battle-reconnect.md) — 断线重连 / no-freeze 契约 / §8 DS 授权租约 fencing 再入屏障
- [`session-generation-rollout.md`](../../../docs/design/session-generation-rollout.md) — 会话代际 / sjti 强制门分阶段发布顺序
- [`protocol-ordering-rules.md`](../../../docs/design/protocol-ordering-rules.md) — 立即完成型 / 已受理型协议原则
- [`decision-revisit-ds-callback-auth.md`](../../../docs/design/decision-revisit-ds-callback-auth.md) §7.15–§7.16 — Hub 归属绑定生产阻断项
- [`owner-authority.md`](../../../docs/design/owner-authority.md) — 每玩家 owner_epoch 权威 / migrate ⑤ 登出释放
- [`scale-cellular-20m.md`](../../../docs/design/scale-cellular-20m.md) §3.2/§3.3 — 确定性 region/cell 路由落点与票据绑定

## 待办

- [ ] 生产 `pandora.login.event` topic(登入 / 登出事件,给风控 / 审计)
