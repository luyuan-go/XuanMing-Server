# decision-revisit:DS→后端回调服务令牌认证(审核 P1 #1)

> 状态：**B 方案已获人批准并在当前工作树实施；生产行为激活仍 fail-closed**（更新于 2026-07-15）。
> 当前阻断原因不再是等待 §7.15 拍板，而是仓库尚未接入可执行、可验证的 Redis
> topology-change/failover/reshard lease provider 与信任根。`activate_ds_auth.ps1 -Phase Activate`、Go CLI/core
> CAS 和普通 online release 均在写入前拒绝；`-Phase Audit` 只做只读检查且不会给出可发布结论。
> 关联:`docs/design/agones-dev.md` §5.1(DS 面网关)、`deploy/envoy/envoy.yaml` :8444、
> `deploy/k8s/agones/16-ds-envoy.yaml`、CLAUDE.md §9 不变量 2/3/6/16/17。

## 1. 旧问题

Envoy :8444「DS 面」网关此前只有两道防线:

1. **方法白名单**(2026-07-10 上午收紧):只放行 7 个 DS 回调方法,deny-by-default;
2. **网络可达边界**(NetworkPolicy / 不暴露公网)+ 入站剥离 `x-pandora-player-id` 等身份头。

但**放行的方法本身完全不认证调用者**。任何能触达 :8444 的进程(伪 DS、被攻破的同网段
Pod、内网横向移动者)可以直接:

- `battle_result.ReportResult`:伪造任意 `match_id` 的结算 → 假战绩、假 MMR、假掉落
  (虽有白名单/幂等兜底,但首次上报即被污染);
- `locator.SetLocation` / `ReportDisconnect`:把任意玩家改到任意位置 / 踢缩 TTL,
  破坏不变量 §9.1「玩家在线只能在一个 DS」;
- `ds_allocator.Heartbeat` / GM `PollCommands` / `AckCommand`:伪造心跳阻止 DS 回收、
  偷看/吞掉 GM 指令;
- `hub_allocator.Heartbeat`:伪造 Hub 拓扑心跳。

对比:玩家面 :8443 有 jwt_authn,玩家→DS 有 DSTicket,唯独 **DS→后端这条边没有身份**。

## 2. 新方案(已实现)

标准做法:**由控制面(allocator)给每个 DS 实例签发短寿命、范围绑定(scope-bound)的
服务令牌,DS 回调时带 `authorization: Bearer <token>`,被调服务校验令牌 + 范围**。
DS 永远拿不到签名密钥,只持有对自己实例生效的令牌。

### 2.1 令牌格式(HS256 JWT,`pkg/auth`)

| | battle 令牌 | hub 令牌 |
|---|---|---|
| 签发方 | ds_allocator(分配对局时) | hub_allocator(ListShards 发现 ready 分片时) |
| `iss` | `pandora-ds-control` | 同左 |
| `aud` | `pandora-ds`(与玩家 `pandora-client` 严格分离) | 同左 |
| `sub` | 空 | **hub pod 名**(范围绑定) |
| `ds_type` | `battle` | `hub` |
| `match_id` | **必填**(范围绑定) | 必须为 0 |
| TTL | 默认 4h(覆盖最长对局,不续期) | 默认 24h,剩余 < TTL/3 自动续期 |

实现:`auth.Signer.SignDSCallback` / `auth.Verifier.VerifyDSCallback`(`pkg/auth/jwt.go`)。

### 2.2 下发通道

- **agones 模式**:battle 令牌放进 GameServerAllocation `metadata.annotations`
  (`pandora.dev/ds-token`);hub 令牌由 hub_allocator 发现分片时 merge-patch 写
  GameServer annotation(`pandora.dev/ds-token` + `pandora.dev/ds-token-exp-ms`
  过期戳)。**续期判断不只看 exp 戳**:要求 annotation 令牌**非空**、exp 未临近、且
  (注入了校验器时)**验签通过 + `ds_type=hub` + `sub=pod` 匹配**,任一不满足即重签
  (修:密钥轮转/空令牌/损坏令牌此前仅凭「有 annotation + 外部 exp」被误判为可用)。
  UE DS 用 Agones SDK `GameServer()/WatchGameServer()` 读取。
- **local 模式**:起进程时把令牌**一次性签发**后注入环境变量 `PANDORA_DS_TOKEN`
  (battle 随分配单次签发并透传给 `startProc`,启动时不再二次重签;hub 本地一次性下发,无续期,
  dev-only,24h TTL 足够)。`extra_env` **不得覆盖** `PANDORA_DS_TOKEN` 及 `AGONES_GAMESERVER_NAME`
  / `PANDORA_DS_TYPE` / `PANDORA_REGION` / `PANDORA_MATCH_ID` 等保留键(命中即忽略并告警,
  防配置注入伪造 DS 身份/范围)。UE DS 用 Agones SDK `GameServer()/WatchGameServer()` 读取。
- **签发失败按模式分档(enforce fail-closed)**:off/permissive 下签发失败 Warn + 继续
  (可用性优先,guard 侧观察);**enforce 下签发失败 = fail-closed**——battle 分配返回
  `ErrDSAllocationFailed` 不产生连不回来的对局,hub 候选标记 `TokenReady=false` 不接客也不入
  ready 镜像(详见 §2.5)。

### 2.3 校验(`pkg/middleware/dsauth.go` DSCallbackGuard)

- Envoy :8444 虚拟主机给所有经 DS 网关的请求盖 **`x-pandora-ds-gateway: "1"`** 标记头
  (入站先无条件剥离,不可伪造;:8443 客户端面也剥离,纵深防御)。
- 判定表:无标记头 + 无令牌 → **仅当 scope 未置 `RequireToken` 时**才按内部东西向直连放行
  (login/matchmaker/ds_allocator 内部调 SetLocation 等不受影响);**纯 DS 回调方法已置
  `RequireToken:true`(hub/ds_allocator Heartbeat、battle_result ReportResult、GM
  PollCommands/AckCommand、locator SetLocation(HUB)/ReportDisconnect),无令牌即使东西向也 401**;
  有标记头无令牌 → 401;令牌验签失败 → 401;`ds_type`/`match_id`/`pod` 与 handler 声明的 scope
  不符 → 403;`DenyDS` scope(如 SetLocation 写非 HUB 状态)→ 经网关或带令牌一律 403。
- 三档模式 `ds_auth.mode`:**off(默认,现状不变)** / permissive(只 Warn 不拒,灰度观察)
  / enforce(强制)。secret 未配 = 功能整体关闭。

### 2.4 各服务 handler 绑定

| 服务 | 方法 | scope |
|---|---|---|
| ds_allocator | Heartbeat、GM PollCommands/AckCommand | `{battle, match_id=req.match_id}` |
| hub_allocator | Heartbeat | `{hub, pod=req.hub_pod_name}` |
| player_locator | SetLocation(state==HUB) | `{hub, pod=loc.hub_pod}` |
| player_locator | SetLocation(其他 state) | `DenyDS`(仅内部服务可写) |
| player_locator | ReportDisconnect | `{hub, pod=req.hub_pod}` |
| battle_result | ReportResult | `{battle, match_id=result.match_id}` |

### 2.5 令牌不可用的 Hub 不接客(TokenReady 传播)

`ShardCandidate` 增 `TokenReady bool`。`ensureDSToken` 失败(enforce)时 `ListShards` **仍返回
该候选但标记 `TokenReady=false`**,而非直接从列表剔除。上层据此:

- `ensureShards`:不给 `TokenReady=false` 的分片播种 Redis `ready` 镜像;
- `reconcileShardTopology`:`TokenReady=false` 的 pod **不计入 `live` 集合**,被陈旧清理逻辑移除
  其 `ready` 镜像 → `AssignHub` 不会再把玩家分到它。

修的问题:此前令牌签发/patch 失败只是 `continue` 跳过候选,但拓扑 reconcile 因「Fleet 返回非空」
仍保留旧的 Redis `ready` 镜像,`AssignHub` 可能继续把玩家分到一个回调必被拒的 Hub。用「返回但标记」
既能区分「Fleet 有分片但全令牌死」(非空 → 陈旧清理移除镜像)与「Fleet 什么都没返回」(空 → 保留镜像,
避免瞬时抖动误删),又能让令牌死的 Hub 干净自愈下线。

## 3. 风险

- **UE DS 未接令牌前开 enforce 会打断所有 DS 回调** → 默认 off,上线顺序:UE 接令牌 →
  permissive 看日志 → enforce。
- hub 令牌续期依赖 hub_allocator 的 GameServer **patch** 权限(RBAC 已补);多副本
  hub_allocator 并发 patch 无害(两个令牌都有效,last-writer-wins)。
- HS256 对称密钥意味着所有校验服务都持有签名密钥;生产硬化方向:换 RS256 + Envoy :8444 挂
  jwt_authn 在网关层先验一道(本次不做,文档记录)。
- local 模式 hub 令牌不续期(env 一次性);仅 dev,24h TTL 覆盖。

## 4. 迁移成本

- **proto 零改动**(令牌走 gRPC metadata `authorization` 头)。
- 后端:pkg + 4 服务已全部接线并测试通过；默认 off 时该令牌子功能保持兼容。但这只说明 additive 代码路径，
  不能推出整套 DS recovery 可直接滚动上线；当前普通 online release 仍被 topology provider 硬门阻断。
- **UE DS 侧待办**(默认 off 期间不阻塞):读 annotation/env 拿令牌,7 个回调方法带
  `authorization: Bearer`;Hub DS 监听 annotation 变化换新令牌。
- 部署:重发 `10-rbac-allocator.yaml`(补 patch)、重滚 Envoy(16-ds-envoy.yaml /
  deploy/envoy/envoy.yaml,均已 `envoy --mode validate` 通过)。

## 5. 验收标准

- [x] pkg/auth 令牌签发/校验单测(错 aud、过期、跨类型、坏 claims 全拒);
- [x] pkg/middleware 判定表单测(off/permissive/enforce × 有无标记头 × 有无令牌 × scope 错配);
- [x] ds_allocator agones 分配 annotation 注入单测(含签发失败不阻断);
- [x] hub_allocator ListShards 签发/续期/未到期不 patch 单测(httptest 假 apiserver);
- [x] 5 个模块 `go build` + `go vet` + `go test` 全绿;
- [x] 两份 Envoy 配置 `envoy --mode validate` OK;
- [ ] UE DS 带令牌后,dev 集群 permissive 模式日志无误报 → enforce 冒烟(伪造请求 401/403,
  真 DS 正常)——待 UE 侧接入后执行。

## 6. 第三轮硬化(P0/P1 密钥面与端到端补漏)

本轮补掉「令牌机制正确但密钥/下发/存储链路仍有漏」的问题:

1. **玩家 JWT 与 DS 回调密钥物理分离(P0)**:`gen_cluster_config.ps1` 由单把 `-Secret` 拆成
   `-Secret`(玩家面:login/hub/matchmaker jwt + Envoy JWKS)与 `-DsSecret`(DS 回调面:ds_auth)。
   `-Prod` 强制两把各自非空 / ≠dev / ≥32B、**且彼此不同**(同值时泄露玩家面即可伪造 DS 回调令牌
   绕过范围绑定)。`Convert-Secret` 改为**按配置段**替换(`jwt:` 段注玩家密钥、`ds_auth:` 段注 DS
   密钥),因 hub_allocator 同文件含两段,不能整文件盲替换。`Sync-EnvoyJwks` 用玩家面密钥
   (Envoy :8443 校验的是玩家 SessionToken)。
2. **配置密钥用 Secret 而非明文 ConfigMap(P0)**:`start.ps1` 三处 `kubectl create configmap
   pandora-config` 全改 `create secret generic pandora-config`;`services.yaml` 20 个 Deployment
   的 conf 卷 `configMap`→`secret`(subPath 挂载行为一致)。
3. **online 部署密钥预检 + 边缘 JWKS fail-closed 门(P0)**:`start.ps1` online 路径在 `BuildPush`
   **之前**校验 `PANDORA_JWT_SECRET` + `PANDORA_DS_JWT_SECRET` 都在、各 ≥32B、≠dev、彼此不同
   (避免镜像已推、gen 才因缺密钥失败)。另设 `PANDORA_EDGE_JWKS_ACK=1` 确认门:本仓库不含生产
   边缘网关(外部自带同名 pandora-envoy),gen 产出的 `envoy-jwks.json` 需运维手动灌入边缘网关;
   未确认则拒绝部署,防 login 用新密钥签的 SessionToken 被边缘网关全拒。
4. **令牌 TTL 启动校验(P1)**:`config.DSAuthConf.Validate(enabled)`——启用鉴权时
   `BattleTokenTTL`/`HubTokenTTL` < 1min 直接 `os.Exit(1)`(ds_allocator / hub_allocator main
   接线)。防误配 0 / 极小 TTL 导致令牌瞬间过期、DS 回调全 401。
5. **Hub 令牌续期真校验 + battle 单次签发 + extra_env 保留键防注入 + Hub TokenReady 不接客**:
   见 §2.2 / §2.5。

### 6.1 本轮验收

- [x] `gen_cluster_config.ps1` 分段替换手测:hub-allocator jwt→玩家密钥 / ds_auth→DS 密钥,
  ds-allocator 仅 ds_auth→DS,login jwt→玩家;`-Prod` 同密钥→拒、缺密钥→拒、两把独立→OK;
- [x] 两个 PowerShell 脚本 AST 解析无错;
- [x] pkg(config/middleware/auth)+ ds_allocator + hub_allocator `go build`+`go test` 全绿;
- [ ] 运维在生产设两把独立真密钥 + 灌边缘 JWKS + `PANDORA_EDGE_JWKS_ACK=1` 后端到端冒烟(待活集群)。

### 6.2 第四轮补漏(再审 5 项残留阻断)

1. **保留键检查大小写不敏感(P1)**:Windows(local 模式 DS/Hub 宿主)环境变量名大小写不敏感,
   `pandora_ds_token` 与 `PANDORA_DS_TOKEN` 指向同一变量;原精确大写比对会漏放小写别名覆盖真令牌。
   `isReservedDSEnvKey` / `isReservedHubDSEnvKey` 改为 `strings.ToUpper(TrimSpace(k))` 比对。
2. **非续期令牌 TTL 下限提高(P1)**:原 `dsAuthMinTokenTTL=1min` 只防「签发即过期」,挡不住
   「运行中途过期」。战斗令牌**永不续期**、大厅令牌 **local 模式不续期**,故按各自不续期场景分设:
   `dsAuthMinBattleTokenTTL=30m`(覆盖一局对局+重连)、`dsAuthMinHubTokenTTL=1h`(覆盖一段常驻会话);
   低于下限启动即 fatal。
3. **陈旧 `envoy-jwks.json` 清理(P1)**:非生产/未注入玩家面真密钥时,`Sync-EnvoyJwks` 主动删除
   `<OutDir>/envoy-jwks.json`,防上一轮 `-Prod` 产出的旧生产密钥材料残留、被后续误当本次产物重新灌入。
4. **agones 链路强制显式声明生产/本地(P1)**:`-AllocatorMode agones` 指向真 Linux DS;不带 `-Prod`
   会写入公开 dev 密钥。改为:agones + 无 `-Prod` 且无 `-AllocatorAdvertiseHost` → **拒绝生成**
   (防 dev 密钥被带上真集群);本地 minikube 须带 `-AllocatorAdvertiseHost` 显式声明本地链路(仅告警)。
5. **边缘 JWKS 真实探测(P0)**:`PANDORA_EDGE_JWKS_ACK=1` 仅布尔承诺,不能证明当前 edge 在用当前密钥。
   新增 `PANDORA_EDGE_PROBE_URL`(edge 上受 `pandora_session` JWT 保护的路由):脚本用玩家面密钥现签
   60s 探测 JWT(iss=pandora-login / aud=pandora-client),无 token 应 401、带 token 若仍 401 =
   edge 用的是旧/别的密钥 → fail-closed 中止;带 token 过 401 = 当前 edge 确在用当前密钥 → 真实通过。
   未提供探测 URL 时退回 `PANDORA_EDGE_JWKS_ACK=1`(大声告警其为未验证承诺),两者皆无则拒绝部署。

#### 6.2 验收

- [x] `strings.ToUpper` 保留键比对;pkg config + ds_allocator + hub_allocator `go build`+`go test` 全绿;gofmt 干净;
- [x] `gen_cluster_config.ps1` 手测:agones 无 `-Prod`/`-AllocatorAdvertiseHost` → 退码 1 拒绝;带 advertise host → 告警放行;预置陈旧 `envoy-jwks.json` 经非生产 gen 后被删除;
- [x] 两个 PowerShell 脚本 AST 解析无错;
- [ ] 运维配 `PANDORA_EDGE_PROBE_URL` 后在活集群实测边缘探测(无 token→401 / 带当前密钥→非 401)。

### 6.3 第五轮补漏(再审 6 项残留阻断)

1. **战斗令牌 TTL 关联 `battle_ttl`(P1)**:第四轮 `dsAuthMinBattleTokenTTL=30m` 仍小于 `battle_ttl`
   默认 2h,一局长对局跑到中途令牌就过期、DS 回调被拒。改双闸:
   ①`pkg/config` 粗下限 30m→1h;②`ds_allocator/main` 精确校验 `BattleTokenTTL ≥ battle_ttl + 15m`
   (重连余量),低于即启动 fatal。默认 4h 令牌 ≥ 2h+15m 通过;运维压低令牌或抬高 battle_ttl 会被挡。
2. **Hub 分片先 warming 后 ready(P1)**:原 hub 分片种子即 `ready` 且 `LastHeartbeatMs=0`,从未心跳的
   Hub 立即可被分配玩家。对齐 battle allocator 既有 warming 模式:新增 `stateWarming`,
   `HubUsecase.requireHeartbeatReady`(仅 `cfg.Mode==agones` 真 DS 链路置 true;mock/local 保持种子 ready
   不破坏离线联调)时种子为 warming 不可分配;`hub_repo.HeartbeatShard` 加 `warming` 分支:**首个通过
   Guard(enforce 下即已鉴权)的心跳**把 warming→ready(除非 DS 报更高 drain rank),据「真实鉴权心跳」
   而非「PATCH/发现成功」才接客。
3. **local Hub 令牌不续期 TTL 下限(P1)**:local 模式 Hub 令牌由 env 注入、进程内无法续期,运行 >1h 即断。
   `hub_allocator/main` 在 `mode==local && enforce` 时强制 `HubTokenTTL ≥ 12h`,默认 24h 通过,仅挡显式压低。
4. **边缘探测反密钥 + 指纹 + 证书告警(P0/P1)**:探测新增**负向控制**——用错误密钥现签 token 打过去应
   401(证明 edge 真在**验签**而非只看 token 存在,消除「接受任意 token」假阳性);`PANDORA_EDGE_PROBE_INSECURE=1`
   跳证书校验时大声告警 MITM 风险;成功日志与 ACK 回退均打印玩家面密钥 SHA256 前 12 hex 指纹供审计对账。
5. **agones 生产/本地判定改显式开关(P1)**:废弃「有 advertise host 即本地」推断(生产也配 advertise
   host/DNS,会绕过 `-Prod` 写入 dev 密钥)。`gen_cluster_config.ps1` 新增 `-AllowDevSecrets`:agones 不带
   `-Prod` 时必须显式加本开关才放行 dev 密钥,否则 deny-by-default 拒绝;`-AllocatorAdvertiseHost` 只回归其
   udp-relay 回程用途。`start.ps1` minikube(944)/resume(1560)两处 dev 调用补 `-AllowDevSecrets`,online(1194)保持 `-Prod`。
6. **行尾空白 + Envoy 陈旧注释(P1)**:`start.ps1` 边缘探测块 `} catch {` 行尾空白随第 4 项重写清除
   (`git diff --check` 干净);`deploy/envoy/envoy.yaml`、`deploy/k8s/agones/16-ds-envoy.yaml` 的
   「:8444 仅靠网络信任 / SetLocation·ReportResult 仍依赖网络信任」注释改为双层防护(方法白名单 +
   enforce 下 DS Bearer 服务令牌鉴权),不再声称「仅靠网络信任」。

#### 6.3 验收

- [x] pkg config + ds_allocator + hub_allocator `go build`+`go test`(含 biz/data)全绿;gofmt 干净;
- [x] 两个 PowerShell 脚本 AST 解析无错;`start.ps1` 无行尾空白;
- [x] `gen_cluster_config.ps1` 手测:agones + advertise host 但无 `-Prod`/`-AllowDevSecrets` → 退码拒绝(FATAL);
  加 `-AllowDevSecrets` → WARN 后生成 exit 0;
- [ ] 运维在活集群实测:边缘探测无 token→401 / 错误密钥→401 / 当前密钥→非 401;agones 真 DS 首心跳后
  warming→ready、玩家才被路由;长对局(接近 battle_ttl)战斗令牌不中途过期。

### 6.4 2026-07-11 再审阻断:Hub ready 证明仅部分接线(仍待拍板)

§6.3 第 2 项只把“未心跳分片”留在 `warming`,但尚不能证明“当前令牌代际已通过真实鉴权”:

- `requireHeartbeatReady` 由 Agones 模式开启,`ds_auth.mode` 却可仍为默认 `off`;两者没有启动期耦合。
  这种组合下心跳不具备鉴权证明,却仍可触发 `warming→ready`。
- 当前工作树随后加入了 `current_token_exp_ms`、心跳验签 claims.exp 对比及重签后退回 `warming` 的
  部分接线，但它把 JWT `exp`(秒精度)当代际。同一秒内重签/换 key 可得到相同 exp，K1/K2 会发生
  代际碰撞，延迟 K1 心跳仍可能重新证明；它不是单调或随机唯一的 token generation。
- gate 必须继续放在所有旧选择器本来就读取的 `state` 中,不能另加 `ready_proven` 旁路字段；否则滚动
  期间旧副本和漏改的选择路径仍会把未证明分片当成 ready。

待人确认的完整方向是一次性完成以下不变量，未完成前不得宣称 Agones Hub ready 鉴权门已闭环:

1. `Agones/requireHeartbeatReady => ds_auth.mode=enforce`,不满足即启动失败；
2. shard 记录不会碰撞的当前令牌代际；PATCH/轮换令牌时产生新代际并把 `state` 退回 `warming`；
3. 只有 Guard 已真实验签、且心跳令牌代际等于当前代际时,仓储事务才允许 `warming→ready`；
4. 延迟旧代际心跳、重复 PATCH、多副本交错滚动和所有现有选择路径均有测试。

本节是对 §6.3 第 2 项“已修”表述的纠偏记录。当前工作树已有未经本决策拍板的 Hub/proto 部分实现；
本轮 Codex 未改这些业务文件，只在生产生成器中强制 `ds_auth.mode=enforce`，并继续把上述业务项列为阻塞。
### 6.5 2026-07-12 再审:生成器姿态改分级逃生 + enforce 启用门 + 网关探测处置

**姿态纠正(取代 §6.4 末段“生产强制 enforce”)**:UE DS(独立仓库)当前仍不读令牌、不发
`Bearer`,生产硬制 enforce 会让全部 7 类 DS 回调 401,且无法不停服切换。故生成器改为**分级逃生**
(见 `gen_cluster_config.ps1` / `decision-revisit-ds-key-rotation.md` 关联):

- `-Prod` + `off` → **抛错**(生产永不允许无鉴权);
- `-Prod` + `permissive` → 大声告警后注入 `permissive`(UE DS 接令牌前的迁移窗口逃生口);
- `-Prod` 缺省 / `enforce` → 注入 `enforce`(目标姿态,默认不静默削弱)。
- `start.ps1` online 增 `-DsAuthMode`('' / permissive / enforce) 透传;缺省 / enforce 时打印
  “UE DS 未发 Bearer 会全 401,迁移窗口请显式 permissive” 提醒。Go 侧 hub_allocator 对
  `agones + 非 enforce` 是 **warn 不 hard-fail** 且 `SetDSTokenGeneration(false)`,故 permissive
  能正确运行(代际门休眠)。

**permissive → enforce 启用门(必须全绿才允许切 enforce,缺一即不得切)**:

1. **UE DS 发 Bearer**:UE 客户端仓库读 annotation/env 令牌,7 类回调均带 `authorization: Bearer`;
   Hub DS 监听 annotation 变更换新令牌(独立仓库交付,后端无法代劳)。
2. **Agones annotation 注入闭环**:battle 令牌进 `GameServerAllocation.metadata.annotations`、hub 令牌由
   allocator merge-patch 写 GameServer annotation,dev 集群实测 DS 能取到并回带。
3. **令牌覆盖率门**:permissive 期观测**带有效令牌**的 DS 回调占比达 100%(或明确白名单)持续 N 分钟,
   日志无“无令牌 / 验签失败”误报,方可切。
4. **旧 Battle 排空**:切 enforce 前排空不带令牌的旧 Battle DS(§9 不变量 4 心跳超时补偿路径),
   避免存量对局回调在切换瞬间集体 401。
5. **旧 Hub drain 门**:先滚完全部 hub_allocator 到带令牌代际门控的新版(与 ds-key-rotation §2.3
   “先滞完 hub_allocator”硬规则同源),再切 enforce,防旧副本把持旧令牌的心跳绕过代际比对转 ready。
6. **代际门闭环(§6.4 的 4 条不变量)**:`Agones/requireHeartbeatReady ⇒ enforce` 启动耦合、
   不碰撞的单调/随机 token generation(非 exp 秒级)、Guard 验签 + 代际相等才 `warming→ready`、
   以及 **annotation PATCH 与 Redis 镜像写的原子性/补偿**(当前先 PATCH 后写 Redis、无 resourceVersion
   CAS,Redis 失败会留旧 ready、多副本 PATCH 可能互覆盖)——须一并硬化并补测。

上述 1–6 属跨子系统(含独立 UE 仓库)与 §6.4 待拍板的代际设计,**enforce 门在上线默认姿态下休眠**
(permissive),故本轮不在后端强行硬化休眠路径(硬化无上线收益且需未拍板的 token 格式变更),
留待真正启用 enforce 时按本门逐项落地 + 验收。

**Online DS gateway 可达性探测:明确不做(不是遗漏)**。宿主侧无法可靠验证目标集群内 DNS / TCP /
TLS / 真实 DS 心跳链路(网络分区、集群内 DNS 仅集群内可解、Envoy :8444 仅集群内可达),盲加宿主探测
只会给出**假信心**(探测通不一定代表集群内 DS 真能回调)。可达性验证的正确落点是**集群内**:
DS 回调路径的 readiness/synthetic probe + `pandora_ds_allocator_fleet_*` 容量指标 + 三审新增的
`pandora-ds-allocator-down` 可用性告警(见 `deploy/grafana/provisioning/alerting/rules.yaml`),
而非 online 发布脚本里的宿主侧一次性探测。故 online 只校验网关地址非空 + 面分离,不加宿主探测。

#### 6.5 处置

- [x] 生成器分级逃生 + `start.ps1 -DsAuthMode` 透传已实现(PowerShell AST 解析通过);
- [x] guild/group TOCTOU、reset fail-closed 等同轮自包含缺陷已修并验证(见对应文件);
- [ ] **待人拍板 / 待 UE 交付 / 待启用 enforce**:上列 permissive→enforce 启用门 1–6、代际门 4 不变量、
  key 轮换部署侧机制门(见 `decision-revisit-ds-key-rotation.md §2.4`)、TiDB 切换闭环
  (见 `decision-revisit-guild-scaling.md §5-6`)。

### 6.6 2026-07-12 落地:令牌代际改 Redis INCR 单调 generation(P1-6/P1-8 已实现,休眠)

按审核「标准修复」意见,把 Hub 令牌代际从**秒级 exp**改为 **Redis INCR 权威、独立、单调 generation**,
消除同秒重签碰撞(P1-6),并把代际校验从**仅 warming** 扩到**所有能置/复位 ready 的状态转移**(P1-8)。
改动全部 **additive + 休眠**(仅 `agones + ds_auth.mode=enforce` 生效,生产 permissive 下恒 0 不启用),
不影响现网行为,零停机滚动兼容(不变量 §17)。

**已实现(后端自包含,本地 buf 重生 go pb + build + go test 全绿)**:

1. `proto`:`HubShardStorageRecord` 新增 `uint64 current_token_gen = 12`(additive,field 11 `current_token_exp_ms`
   保留仅作兼容/调试,不再当代际);
2. `pkg/auth`:`DSCallbackClaims` 新增 `ds_gen`(omitempty)+ `Gen()`;新增 `SignDSCallbackWithGen`,
   `SignDSCallback` 委托 gen=0(battle DS 令牌路径不受影响);
3. `hub_allocator` 全链路:签发器经 `pandora:hub:tokengen:{pod}` INCR 领取严格递增 gen 并签进令牌 →
   agones 写 `pandora.dev/ds-token-gen` annotation + local 经 env 一次性下发 → `ShardCandidate.TokenGen` →
   拓扑对账写 `CurrentTokenGen` 并按 **gen 精确相等**复位 warming → 心跳侧 `HeartbeatShard` 在**所有状态**
   用 gen 精确相等拦「置/复位 ready」(含 draining→ready 存活恢复路径,堵旧代际令牌复活漏洞,P1-8);
4. DS 把令牌视作不透明串原样回显,gen「免费」随 `Authorization` 捎回,无需 UE 侧改动。

**仍待办(不在本次后端范围)**:

- **P1-7(annotation↔Redis 无 CAS)**:agones merge-patch 仍无 `resourceVersion` CAS、先 PATCH 后写 Redis;
  多副本交错 PATCH / Redis 写失败仍可能短暂不一致。代际改单调后**危害已降级**(旧 gen 心跳被全状态拦,
  不会误 ready),但严格原子化仍待启用 enforce 时按 §6.5 门 6 落地;
- **P1-9(新旧 allocator 混合滚动)**:靠**安全默认值**(gen 默认 0,旧副本忽略新字段/新 claim)+ **部署顺序**
  (先滚完全部 hub_allocator 到本版,再切 enforce,同 §6.5 门 5 / ds-key-rotation §2.3)兜住,属**运维顺序**
  依赖,非纯代码可闭环;
- **P1-4(online DS gateway 可达性探测)**:§6.5 已明确**不做**(宿主侧探测给假信心,正确落点在集群内 probe)。

**Codex 交接**:①**仓内 cpp pb 已重生**(见 §6.9,`proto/gen/cpp/pandora/hub/v1/allocator.pb.{h,cc}`
已含 field 11/12),剩余仅需把 `proto/gen/cpp/**` **拷贝**同步到 UE 仓 `Source/Pandora/Generated/Proto/`
(`[proto]`);②本轮未引入新 module,无需额外 `go mod tidy`;③上线切 enforce 前照 §6.5 门 5 / P1-9
先滚完全部 hub_allocator 到本版。

### 6.7 2026-07-12 再审驳回 → 心跳侧「代际门 fail-closed」硬化(P1 1/3/4/5-8 已实现)

上一轮把代际改单调后,再审仍驳回:代际**发号**单调不等于**当前生效代际**具备权威性,且心跳侧存在多处
**fail-open**——旧代际/无代际(gen=0)心跳仍能刷 `player_count`、刷 `LastHeartbeatMs`(保活)、进 active 索引、
触发 presence,permissive 副本还能把已生效的非零代际**清 0 回退**。本轮把这些 fail-open 全部改 **fail-closed**
(仍 additive + 休眠,仅 agones+enforce 生效):

**已实现(后端自包含,build + go test 全绿)**:

1. **stale 令牌零变更 fail-closed(P1 5/6/7/8)**:`HeartbeatShard` 把代际校验提到**任何镜像变更之前**——
   代际过期(镜像非零且不等)或缺失(`genRequired && gen==0`)时,`WATCH` 事务内直接返回
   `errShardTokenStale`(`ErrUnauthorized`),**不 EXEC**:`player_count`/`state`/`LastHeartbeatMs`/TTL 全不动,
   不 `SADD shards`、不 `ZADD active`。biz 透传该错误 → service 层因 `err!=nil` 提前返回、**不刷 presence**。
   旧代际 DS 从此既不能保活、不能占位、不能伪造在场;心跳超时后自然被 sweep 回收(严格 fail-closed);
2. **enforce 下要求 gen>0(P1 1/4)**:新增 `genRequired`(= `biz.dsTokenGeneration`)入参,enforce 下
   legacy gen0 心跳一律判 stale——即便镜像 `CurrentTokenGen` 尚未建立(0),也不放 gen0 通过关闭代际门;
3. **代际单调不回退(P1 3 + Redis 侧 12)**:拓扑对账 `UpdateShardWithLock` 改为
   **`gen > s.CurrentTokenGen` 才推进**,删除原「候选 0 → 清 0 自愈」分支;permissive 副本 / annotation 缺失
   (候选 gen=0)**保持镜像既有代际不变**,绝不清除或回退已生效的 enforce 代际(经 `WATCH` CAS 落 Redis,
   Redis 侧亦单调);
4. **enforce 强制补齐 annotation gen(P1 1/2)**:`ensureDSToken` 续期判定下,enforce 且 annotation 缺合法
   `ds-token-gen`(缺失/非法/legacy)时**强制重签**补齐单调代际,挡「寿命充足的 legacy gen0 令牌被当有效」;
5. **测试(P1 17/18/19)**:新增 stale-gen 零变更、enforce gen0 拒、匹配 gen 翻 ready、permissive gen0 向后兼容
   (`hub_repo_test.go`);enforce+缺 gen 强制重签且写 `ds-token-gen`(`agones_fleet_test.go`);`ds_gen`
   验签 round-trip + legacy gen=0(`jwt_dscallback_test.go`)。

**仍未闭环(诚实交代,非本次后端能单方消化)**:

- **P1 9-13(K8s↔Redis 权威分裂 + activation fence)**:本轮已按标准乐观并发落地,详见 §6.8;
- **P1 14/15(UE DS Bearer 传输)= enforce 启用硬阻塞**:UE 仓库 `Pandora-Client-SVN` 的
  `PandoraDSBackendSubsystem` / `PandoraAgonesSubsystem` 尚未读取 `pandora.dev/ds-token` annotation、
  未在 Hub 回调加 `Authorization: Bearer`。ds_gen 本身不需 UE 解析(不透明回显),但**没有 Bearer 传输,
  一开 enforce 所有 Hub 回调即全拒**。此为**独立 UE 仓库工作**,按治理归 UE 开发者 / 人,后端不可代改;
- **P1 16(cpp pb 同步)**:**仓内 cpp pb 已重生并落库**(见 §6.9,`proto/gen/cpp/pandora/hub/v1/`
  已含 field 11/12);剩余仅「把 `proto/gen/cpp/**` 拷贝进 UE 仓 `Source/Pandora/Generated/Proto/`」一步,
  归 Codex(`[proto]`,需 UE 构建集成,后端不可代做);
- **上一轮发布阻塞(密钥集合无交集门 / Secret 无 Lease·CAS·pre-image 回滚 / 只能 primary 硬切 /
  旧 ConfigMap 删除破坏旧 RS 回退 / mutable tag + IfNotPresent / 集群内 DS gateway synthetic gate)**:
  属 **ops + 人决策**(§11.1 registry/部署/生产 归 Codex+人),后端不触碰。

**结论**:本轮把「心跳侧代际门」由 fail-open 收敛为 fail-closed(P1 1/3/4/5-8 + 测试 17/18/19),
是自包含、可本地验证的正确硬化;但整链路要真正可启用 enforce,仍卡在 **UE Bearer 传输(14/15)**、
**K8s↔Redis CAS + activation epoch(9-13,架构级需拍板)**、**cpp pb 同步(16,Codex)** 与 **发布门(ops/人)**。
在这些闭环前,enforce **不得开启**,本代际门保持休眠。

### 6.8 2026-07-12 落地:K8s↔Redis 代际 resourceVersion CAS + read-after-write + 补偿(P1 9-13 已实现)

用户明示「最标准修复,不用拍板」,授权把 §6.7 遗留的架构级 P1 9-13 按标准乐观并发模式落地
(仍 additive + 休眠,仅 agones+enforce)。本轮实现(build+vet+go test 全绿):

1. **resourceVersion CAS(P1 9/10)**:`gsMetadata` 解析 `resourceVersion`/`uid`;`ensureDSToken` 重签
   PATCH 由无条件 merge-patch 改为**携带 `metadata.resourceVersion` 的乐观并发 merge-patch** ——
   apiserver 在 live 对象 rv 已变时回 `409 Conflict`,冲突方不再盲写;
2. **冲突重读 + 天然收敛(P1 10/12/20)**:PATCH 收到 409 → `getGameServer` 单对象重读拿最新 rv/annotation
   → 循环重判(`maxCAS=3`);重读结果若已含对方写好的**当前代际有效令牌**则 `tokenStillValid` 命中直接复用、
   **不再 INCR 发号**,消除「后到低代际 PATCH 覆盖高代际」;CAS 连续冲突耗尽 → 返回 error,enforce 下该 Hub
   本轮不计入可用(fail-closed),下轮对账重试;
3. **read-after-write 信任服务器终态(P1 11)**:PATCH 成功后解析**返回对象**的 annotation 代际/exp 为准
   (`genFromAnnotations`/`expFromAnnotations`),不盲信本地发号值;
4. **Redis 侧单调 CAS(P1 12)**:拓扑对账 `UpdateShardWithLock`(WATCH/MULTI/EXEC)已改
   `gen > s.CurrentTokenGen 才推进`,低代际永不覆盖高代际(§6.7 已落);K8s 侧 CAS + Redis 侧单调双保险,
   最终 gen 不再分裂;
5. **PATCH 成功 / Redis 失败补偿收敛(P1 12)**:annotation 持久持有新代际 → 下轮对账 `tokenStillValid`
   命中复用 → `UpdateShardWithLock` 重试写 Redis;代际推进复位分片 warming,Redis 追平前分片保持
   **不可分配(fail-closed)**,绝不把玩家路由到代际未落地的 Hub;
6. **activation fence 机械化(P1 13)**:关键在于 `HeartbeatShard` 的 stale 判据
   `rec.CurrentTokenGen != 0 && tokenGen != rec.CurrentTokenGen` **不依赖本副本的 enforce 开关**——
   一旦某分片记录代际非零,**所有新代码副本(含 permissive)都对非匹配代际心跳 fail-closed**,permissive
   副本也不能把已生效代际清 0 或放旧代际过关(§6.7 单调 + 记录驱动)。故 permissive↔enforce 混合(全新代码)
   窗口安全;残余唯一不安全 writer 是**旧代码副本**(本次改动之前的二进制),由标准**两阶段发布**兜底
   (先全量滚新代码、enforce 关 → 再翻 config 开 enforce;新代码永不与写代际记录的 enforce 副本并存旧二进制),
   这是**运维顺序不变量**,非新机制;
7. **测试(P1 20)**:新增 `TestAgonesEnsureDSToken_ResourceVersionConflictReread` —— 首个 PATCH 409 →
   重读命中对方 gen=7 有效令牌 → 复用不重签,仅 1 次 PATCH,分片 TokenGen=7。

**结论**:P1 9-13 的后端可实现部分已按标准乐观并发(K8s resourceVersion CAS + read-after-write + 冲突重读)
+ Redis 单调 CAS + fail-closed 补偿 + 记录驱动 activation fence 全部落地并测试。整链路要真正开 enforce,
仍**只**卡在 **UE Bearer 传输(P1 14/15,独立 UE 仓,归 UE/人)**、**cpp pb 拷贝进 UE 仓(P1 16 残余,
仓内已重生见 §6.9,Codex)** 与 **发布门(ops/人)**——这三类后端不可代做。在 UE Bearer 落地前 enforce
不得开启,本代际门保持休眠。

### 6.9 2026-07-12 落地:仓内 cpp pb 重生对齐 field 11/12(P1 16 后端侧闭环)

上一轮只重生了 go pb,仓内 tracked 的 cpp pb(`proto/gen/cpp/pandora/hub/v1/allocator.pb.{h,cc}`)
仍停在旧代际,连 field 11 `current_token_exp_ms` 都缺,与 `.proto` / go pb 不一致(仓库自身 tracked
产物漂移)。本轮 `cd proto && buf generate --template buf.gen.cpp.yaml` 重生对齐:

1. **内容对齐**:`HubShardStorageRecord` cpp 侧补齐 field 11 `current_token_exp_ms` + field 12
   `current_token_gen` 的 getter/setter/clear、`_impl_` 字段、TcParser 快表、`PROTOBUF_FIELD_OFFSET`
   偏移表、hasbit 索引、消息 size 表与内嵌 descriptor 字节串——均为加 2 字段的标准 codegen 变更;
2. **对齐仓库 editorconfig 约定**:`buf` 原始输出含少量行尾空格,而仓内既有 cpp 产物已按
   `.editorconfig`(`trim_trailing_whitespace=true`)裁过。故重生后对改动文件统一裁行尾空格,
   使 diff **仅剩 field 11/12 真实新增**,且顺带被 `buf` 重生波及的无关 `gm.pb.h`(仅行尾空格漂移)
   裁回基线零 diff——最终仅 `allocator.pb.{h,cc}` 两文件改动;
3. **当时仍属后端不可代做的残余**:把 `proto/gen/cpp/**` **拷贝**进独立 UE 仓并完成 UE 构建集成。
   此项后续已按 UE 实际 `PandoraProto` / `ThirdParty/PandoraProtoGenerated` 路径完成，当前状态见 §7.14；
   本段只保留当时的后端生成记录。

**结论（当时）**:P1 16 的**后端仓内侧已闭环**(cpp pb 与 `.proto`/go pb 一致)；
跨仓同步的后续实际状态以 §7.14 为准。
工作树未 commit/push(git 归 Codex/人)。

---

## 7. 2026-07-12 架构级再审：从 K8s-first 改为 Redis authority + active/pending（历史提案；后续已批准实施）

> 本节保留 2026-07-12 **PROPOSED / 待人拍板**时的发现、候选和验收原文，用于说明为何推翻
> §2.2 / §6.6–§6.8；其中“未实施/待拍板”描述的是当时状态，不是当前结论。后续实施状态与仍存在的
> 发布硬门以 §7.14~§7.18 和 `docs/reviews/ds-recovery-claude-audit.md` 为准。

### 7.0 先纠偏:§6.8「P1 9-13 已闭环」的结论不准确

§6.8 声称「P1 9-13 的后端可实现部分已按标准乐观并发全部落地并测试」。**这个「闭环」判断是错的**,
因为它把「K8s 内部丢失更新」当成了「K8s↔Redis 跨系统事务」来解决:

- `resourceVersion` 条件 PATCH **只能**保证同一个 GameServer 对象在 apiserver 内部不被并发覆盖
  (解决 P1 内部丢失更新)。它**无法**在「PATCH K8s」与「写 Redis」两个独立系统之间提供原子性。
  参见 Kubernetes API Concepts(resourceVersion 是**单对象**乐观并发,不是跨资源/跨系统事务)。
- 现有顺序仍是 **K8s-first**:`agones_fleet.go::ensureDSToken` 先 PATCH 令牌/gen annotation,
  `hub.go` 的拓扑对账后写 Redis `CurrentTokenGen`。**PATCH 成功而 Redis 写失败**这个半成功窗口里,
  旧 Redis 分片镜像可能仍 `ready` 且已在 `live`/active 索引中,`AssignHub` 仍可能选中它。
  §6.8 用「下一轮对账最终收敛 + 代际推进复位 warming」来兜底,但**「最终收敛」不是 fail-closed**:
  在收敛发生之前的窗口里,分配路径读的是 `shard.State==ready`,并不校验「该 ready 是否已被真实鉴权证明」。
- annotation 仍**间接参与授权推进**:候选令牌代际(来自 annotation `ds-token-gen`)经拓扑对账写进 Redis
  `CurrentTokenGen`。annotation 里独立填写的一个数字**不应该有能力推进 Redis 的权威授权状态**。

因此 §6.8 的正确表述应为:**「在 K8s-first 模型内已把能补的补到位,但该模型本身不能提供 K8s↔Redis
跨系统安全,残留半成功窗口 + annotation 参与授权推进两个结构性缺陷,不构成 P1 闭环。」**

### 7.1 独立分析:线性化点与权威来源

抛开现有实现,先问三个根本问题:

1. **谁是「当前哪张令牌能写数据 / 该分片能否被分配」的唯一权威?**
   候选:①K8s GameServer annotation;②Redis;③JWT 本身。
   - annotation 是**投递副本**(deliverable),会因多副本 PATCH、Pod 重建、手工改动而漂移,**不能**当权威。
   - JWT 是**凭证内容真值**(签名保证 claims 不可篡改),但 JWT **无状态**——它不知道「自己是否已被更高代际取代 /
     是否已吊销」。JWT 回答「你是谁」,不回答「你现在还算不算数」。
   - 只有 **Redis** 能持久保存「当前生效凭证 = 谁」并对读写做**线性化**(WATCH/MULTI/EXEC 或 Lua 单 slot 原子)。
   **结论:Redis 是唯一授权权威;annotation 只投递;JWT 只证明身份内容。**
2. **线性化点(linearization point)在哪?**
   现有 Scheme A 把线性化点**隐式**放在「拓扑对账写 Redis」这一步,但这步与「PATCH K8s」不原子,且
   由 allocator(旁路)驱动,不由持令牌的 DS 驱动。
   **正确的线性化点应是「DS 用 pending 令牌发出的首个合法心跳,在 Redis 单 slot 原子事务里把 pending→active」**
   ——即**由令牌的实际使用者、在权威存储内、一次原子操作**完成「授权生效」。投递(PATCH)与生效(promote)
   彻底分离:PATCH 成功 ≠ 授权生效,只有 DS 证明持有该令牌并被 Redis 原子采纳才生效。
3. **一次 generation 对应几个凭证身份?**
   现有实现只用 `gen` 排序,`gen` 由 Redis INCR 保证单调,但 **INCR 计数器有 TTL**(`pandora:hub:tokengen:{pod}`),
   failover/误删/过期后计数器可能重置 → **同一 gen 可能被复用签出两张不同 JWT**。gen 单调**不足以**唯一标识凭证身份。
   **凭证身份必须是元组** `(instance_uid, protocol_epoch, gen, jti)`:gen 排序、uid 防同名 Pod 重建复活、
   jti 防同 gen 不同 token 混淆、epoch 防旧协议/旧二进制旁路。

### 7.2 三方案对比(并发 / 半成功 / 崩溃 / 新旧混跑 / Pod 重建 / 密钥轮换)

| 维度 | A. 现状 K8s-first 最终一致(§6.6–6.8) | B. Redis 权威 active/pending 两阶段激活(**推荐**) | C. 更简方案(如「只信 JWT + 短 TTL 不留 Redis 授权状态」) |
|---|---|---|---|
| **并发多副本发号** | 靠 rv CAS + Redis `gen>cur` 单调兜。旧低 gen PATCH 会 409 重读,但**发号点在 allocator**,两副本仍可能各自 INCR 出 g5/g6 交错 | Redis `StagePending` CAS 唯一决定 pending 归属;PATCH 只投递。线性化点在 Redis,天然定序 | JWT 无状态,无法防「两张同时有效」,并发下多 token 并存 |
| **半成功(PATCH ok / Redis fail)** | ❌ 残留:旧 Redis ready 仍可被 AssignHub 选中,直到下轮对账收敛(非 fail-closed) | ✅ PATCH 只投递,不改 active;Redis 未 promote 前分片保持不可分配(fail-closed);无跨系统事务需求 | ✅ 无 Redis 授权态则无此窗口,但代价是无法吊销/无法定序 |
| **进程崩溃(任意阶段)** | 依赖下轮对账重放;窗口内可能误分配 | ✅ 任意阶段崩溃最多停在 BOOTSTRAP/ROTATING(不可分配);pending 未被证明前旧 active 继续服务或分片 warming | 崩溃无状态可恢复,但同样无授权语义 |
| **新旧二进制混跑** | 记录驱动 fence(gen!=0 则全副本拒非匹配)对**新代码**安全;**旧代码**副本仍能旁路 → 靠部署顺序 | ✅ 记录驱动 + `protocol_epoch` 机械门:旧 epoch writer/token 一律拒,不靠人工顺序 | ❌ 无 epoch,旧二进制可继续签发被接受 |
| **Pod 同名重建** | ❌ 现模型不校验 `metadata.uid`;同名新 Pod 可复用旧 annotation/旧 assignment | ✅ `instance_uid` 变更视为全新实例,旧 uid 令牌/assignment/心跳全失效 | ❌ 无 uid 绑定,同名复活 |
| **密钥轮换** | §6 三段式 additional_secrets 可平滑,但「紧急吊销」与「平滑续期」无区分 | ✅ pending 未证明前旧 active 继续服务(平滑);QUARANTINED 相位立即拒旧 active(紧急吊销)两条路径分离 | 仅靠 TTL 到期,无法即时吊销 |
| **实现复杂度 / 迁移成本** | 已实现(但不闭环) | 高:新 Redis auth record + 状态机 + DS 激活心跳 + UE staged token + 真集群测试 | 低,但**不满足**吊销/定序/防重建等硬不变量,**不可接受** |

**选择 B**。C 简单但结构性放弃了「吊销 / 定序 / 防同名重建 / 防旧协议旁路」这些硬安全不变量,直接出局。
A 在其模型内已尽力,但**结构上**不能提供 K8s↔Redis 安全,继续打补丁是给不存在的跨系统原子事务打补丁。
B 不试图制造跨系统原子事务,而是用**持久状态机**把「投递」和「授权生效」彻底分开,线性化点收敛到 Redis 单 slot,
一次解决并发、半成功、崩溃、混跑、Pod 重建、密钥轮换、UE 切 token 全部问题。

### 7.3 权威模型与存储(proto additive)

新增独立 Redis auth record,不再让 annotation candidate 直接推进 `CurrentTokenGen`。三键共用 `{pod}` hash tag
(同 slot,可跨键 WATCH/Lua 原子):`pandora:hub:auth:{pod}`(权威状态)、`pandora:hub:tokengen:{pod}`(发号)、
`pandora:hub:shard:{pod}`(镜像)。

新增 additive proto message(不改任何现有 field number,遵循不变量 §17 双向兼容):

```proto
message HubDSCredential {          // 一张凭证的完整身份
  uint64 gen            = 1;       // Redis INCR 单调发号(排序)
  string jti            = 2;       // crypto/rand 唯一(防同 gen 不同 token 混淆)
  uint64 exp_ms         = 3;
  string kid            = 4;       // 签名密钥指纹(轮换用)
  string instance_uid   = 5;       // GameServer metadata.uid(防同名 Pod 复活)
  uint32 protocol_epoch = 6;       // 协议纪元(防旧二进制/旧协议旁路)
  string token_sha256   = 7;       // 只存摘要,绝不存 JWT 明文
}
message HubShardAuthStorageRecord { // pandora:hub:auth:{pod} 权威记录
  string pod_name              = 1;
  string instance_uid          = 2;
  uint32 protocol_epoch        = 3;
  HubAuthPhase phase           = 4; // BOOTSTRAP/ACTIVE/ROTATING/QUARANTINED/TERMINATING
  HubDSCredential active       = 5;
  HubDSCredential pending      = 6;
  uint64 high_water_gen        = 7; // 烧号防重放
  uint64 pending_started_ms    = 8;
  string delivered_rv          = 9; // 已投递的 GameServer resourceVersion
}
```

`HubShardStorageRecord`(现 field 1-12)additive 增:`last_verified_gen`、`last_verified_jti`、
`gameserver_uid`、`auth_epoch`——使 `state==ready` **不再是唯一可分配证明**,分配门另查这些字段。
`HubAssignmentStorageRecord` additive 增 `hub_instance_uid` / `auth_epoch` / `auth_gen`,读旧 assignment 时
uid/epoch/gen 不一致即作废重分配(防把玩家送到同名新实例)。

**兼容性**:全部 additive field number,旧副本忽略未知字段;read-modify-write 路径**禁止 `DiscardUnknown`**
(不变量 §17)。默认相位/字段为零值时行为回退到现状(休眠),零停机滚动。

### 7.4 状态机与线性化点

相位:`BOOTSTRAP`(新实例无 active,不可分配)→ `ACTIVE`(有已证明 active)⇄ `ROTATING`(active 仍服务,
pending 待投递/证明)→ `TERMINATING`(排空);`QUARANTINED`(紧急吊销/异常,不可分配)可从任意相位进入。

阶段与**副作用点**:

| 阶段 | 执行方 | Redis 操作 | 是否改授权 |
|---|---|---|---|
| A. Discover/Init | allocator | `SET NX` / CAS 初始化 auth+shard;uid 变更→隔离旧实例 | 否(BOOTSTRAP,不可分配) |
| B. Prepare rotation | allocator | INCR 领 gen + crypto/rand jti + 签 JWT(绑 uid/epoch/gen/jti/kid);`StagePending` CAS(uid/epoch 匹配 & pending.gen > active/pending) | 否(仅写 pending) |
| C. Publish | allocator | —(PATCH K8s,带 uid+rv 条件;超时/2xx空body/坏JSON→GET 判定,绝不回退本地 gen) | 否(PATCH ok = 已投递 ≠ 生效) |
| D. UE staged | UE DS | —(收 annotation 存为 staged token,不解 JWT,不替换 active) | 否 |
| **E. Prove/Promote** | **DS 首个 pending 心跳** | **单 slot WATCH/MULTI/EXEC 或 Lua:credential==pending → pending→active + 清 pending + 更新 last_verified + warming→ready + 刷 count/state/hb/TTL/index** | **是(线性化点)** |
| F. Rotate/Revoke | allocator | 平滑:pending 证明前旧 active 继续;紧急:QUARANTINED 立即拒旧 active | 是 |

**唯一线性化点 = 阶段 E**:DS 用 pending 令牌的首个合法心跳,在 Redis 单 slot 原子事务里 pending→active。
promote 幂等(credential==active 时普通心跳成功);激活心跳响应经 additive proto 回显 accepted gen/jti,
UE 收到 ack 后才把 staged 原子切为 active。ack 丢失时 UE 重发 staged 激活心跳,服务端发现已是 active 仍返回成功。

### 7.5 核心安全不变量(实施时须逐条测试证明,不得只写注释)

1. Redis 是当前写权限与可分配状态的唯一权威;annotation 永不能直接 activate/降 gen/清 gate/使 ready。
2. 凭证身份 = `(instance_uid, protocol_epoch, gen, jti)`,一次 gen 只对应一个身份;发号丢失即烧号用更高 gen。
3. stale/missing/future/错 uid/错 jti/错 epoch 的令牌**在任何副作用前**失败,**零副作用**(shard bytes/count/state/
   last_hb/TTL/active ZSET/索引/locator presence 全不动,不产生 assignment,不下发会被误解为成功的 command)。
4. 同名 Pod 的 `metadata.uid` 变更 = 全新实例,旧 uid 令牌/assignment/心跳永不作用于新实例。
5. 半成功(PATCH ok / Redis 任意写 fail)最多导致不可用(warming/BOOTSTRAP),绝不误授权或继续分配。
6. 所有受保护 Hub 写方法(hub Heartbeat / locator SetLocation(HUB) / ReportDisconnect / 其他 Hub DS 回调)
   统一校验 Redis **active** 凭证元组;**pending 令牌只能用于激活心跳,不能用于 locator 或其他业务写**。
7. AssignHub / reserveSeat / 复用既有 assignment / TransferHub / ListHubLines 全部在 Redis CAS 内做最终门:
   phase==ACTIVE & active==last_verified & uid 一致 & 未过期 & 心跳新鲜 & state==ready & 非 draining & 容量未超。

### 7.6 逐失败点安全结果(实施后必须由故障注入测试硬断言)

- INCR ok/签名 fail:只烧号,不碰 K8s,不写 pending。
- StagePending fail:无 pending,不 PATCH,分片保持原相位。
- Stage ok/PATCH fail:pending 已在 Redis 但未投递,分片不可分配;重试投递。
- PATCH ok/客户端 timeout:结果未知 → GET 判定;只有返回 bundle 与 pending 完全一致才算已投递,否则保持 pending。
- PATCH ok/Redis 后续 fail:annotation 已投递,但 active 未变;下轮/心跳 promote 幂等收敛;窗口内分片不可分配。
- Promote ok/响应丢失:promote 幂等;UE 重发 staged 激活心跳,服务端返回已 active 成功,UE 再切换。
- allocator 任意阶段 crash:最多停在 BOOTSTRAP/ROTATING;pending 未证明前旧 active 服务或分片 warming。
- annotation 被删/改低/改高:annotation 无授权能力;promote 只认 Redis pending;不一致 token 一律 zero-effect 拒。
- Redis counter TTL 回退:凭证身份含 uid/epoch/jti,旧 JWT 的 (uid,epoch,jti) 与新 active 不符 → 拒,不复活。

### 7.7 permissive→enforce 机械 activation fence(不靠人工部署顺序)

引入单调 `required_auth_epoch`(etcd 线性 CAS,复用现有 Lease/CAS 能力;进程看到 epoch2 后 watch 断开也不降回;
启动读失败 fail-closed)。每副本注册带 TTL 的 capability(writer protocol version / supported epoch / image digest /
keyset revision)。Activation Job 持 Lease 机械核对(Deployment updated==desired / 旧 RS pod==0 / EndpointSlice 全 v2 /
capability 齐 / 全 live Hub DS 支持 Bearer / legacy DS==0 / pending 与 divergence==0 / would-deny==0 / 连续 2–3 心跳周期稳定 /
synthetic 通过)后一次 CAS 推进 epoch;激活后准入策略拒旧 allocator/旧 UE 镜像,禁 epoch 回退,break-glass 显式限时审计。
若新旧二进制无法安全共存,用 blue/green + 独立 Service selector,旧 writer scale 0 后才激活。

### 7.8 观测 / 密钥轮换 / 测试矩阵(实施时落地)

- **指标(低基数,pod/gen/jti 不作 label)**:`pandora_hub_token_rotation_total{stage,result}`、
  `..._failure_total{stage,reason}`、`..._divergence_shards{kind}`、`..._pending_shards`、`..._pending_max_age_seconds`、
  `..._activation_latency_seconds`、`pandora_hub_heartbeat_auth_total{result,reason}`、
  `pandora_hub_assignment_blocked_total{reason}`、`pandora_ds_auth_protocol_info{version}`、keyset revision。
- **告警**:divergence>0 连两轮 / pending 超 2 周期 / enforce 下 unsupported writer>0 / stale 拒升高 /
  active 将过期但 pending 未激活 / ready 分片无近期 accepted 心跳。
- **密钥**:独立评估 HS256→RS256(signer 持私钥、verifier 只持 JWKS、kid 白名单、unknown kid 立拒);
  本轮若不切非对称,须明确记录 HS256 verifier 可伪造 token 的残余风险,不称「最标准闭环」。轮换三阶段
  (发 K2 verifier → 确认后 signer 切 K2 触发 pending rotation → 等 max TTL+skew+in-flight 后移除 K1)。
- **测试(禁 sleep 靠 barrier/hooks;真实 Redis WATCH/Cluster slot;K8s 至少一次 envtest/minikube;UE Automation)**:
  JWT/middleware round-trip 与错配拒;K8s uid+rv 条件 PATCH 与 409/超时/空body/坏body/同名重建;Redis 状态机
  SET NX/StagePending 幂等/并发 g5/g6/g7/WATCH 冲突/counter 回退/uid 变更/promote 原子/ack 丢失幂等;
  跨系统 10 个故障注入点逐一断言「最多不可用 + AssignHub 从不选中不一致 shard + 2 周期内收敛」;
  心跳全链路 zero-effect(8 类坏 token 前后断言 bytes/PTTL/ZSCORE/index/locator/count/state/hb 不变);
  UE `Pandora.Net.DSAuth`(annotation/uid 解析、staged 切换、ack 丢失、100 并发只拿完整旧/新 token、7 类 RPC Bearer);
  旧新版本矩阵 allocator v1/v2 × UE v1/v2 × permissive/enforce/epoch。
  命令基线:`go test -race -count=100 ./internal/...`、`go test -count=1 ./...`、`go vet ./...`。

### 7.9 迁移成本与分批实施计划(拍板后)

1. **P0 proto additive**:加 §7.3 message/field(不改现有 number)+ buf 重生 go/cpp pb(`[proto]`,cpp 拷贝 UE 归 Codex)。
2. **P1 Redis auth record + 状态机**:pkg 新增 auth repo(SET NX / StagePending / Promote Lua)+ 单元/集成测试。
3. **P1 中间件 VerifiedCredential + CredentialStateChecker**:`pkg/middleware/dsauth.go` 扩展,所有 Hub 写方法查 active 元组;
   Redis 不可读返回 Unavailable(不当 401、不放行)。
4. **P1 allocator**:Discover/Init(uid 绑定)+ Prepare/Stage + Publish(uid+rv 条件 PATCH,GET 判定)。
5. **P1 心跳 promote**:hub_repo Promote 事务(线性化点)+ AssignHub 最终门。
6. **P2 activation fence**:etcd `required_auth_epoch` + capability lease + Activation Job(ops/人协同)。
7. **UE(独立仓,归 UE/人)**:staged token + Bearer 全 7 类回调 + Automation 测试。**enforce 硬阻塞项**。

**迁移期安全**:全部 additive + 默认相位/字段零值 = 休眠回退现状,零停机滚动(不变量 §16/§17)。
enforce 在 §7.7 机械门全绿前保持关闭。

### 7.10 主动反证(实施前须用测试回答,不得只声称安全)

- **PATCH ok / Redis 任意写 fail,旧 ready 为何不会继续被分配?** 因 active 未 promote,分片保持 warming/BOOTSTRAP;
  AssignHub 最终门查 phase==ACTIVE & active==last_verified,不满足即拒。annotation 无授权能力。
- **两 allocator 高低 gen 乱序,谁拥有线性化权?** Redis `StagePending` CAS(pending.gen 必须高于 active/pending)
  是唯一定序点;低 gen Stage 失败,PATCH 只投递不生效。
- **同 gen 不同 token 为何不能互换?** 凭证身份含 `jti`(crypto/rand 唯一);promote 比对整元组,jti 不符即拒。
- **Pod 同名重建旧 token 为何必然失效?** 身份含 `instance_uid`=metadata.uid;新 Pod uid 不同,旧 uid 令牌全链路拒。
- **pending token 为何不能提前写 locator?** 不变量 §7.5.6:pending 仅允许激活心跳;locator/业务写只认 active 元组。
- **旧 allocator 激活后为何不能旁路?** `protocol_epoch` 机械门 + capability lease;旧 epoch writer/token 一律拒,
  不靠人工顺序;必要时 blue/green scale 0。
- **Redis counter 回退旧 JWT 为何不复活?** 身份含 uid/epoch/jti,旧 JWT 元组与新 active 不符即拒。
- **UE 在 ack 丢失如何幂等恢复?** promote 幂等;UE 重发 staged 激活心跳,服务端返回已 active 成功,UE 再切换。
- **密钥泄露紧急吊销与平滑轮换如何区分?** 平滑:pending 证明前旧 active 继续服务;紧急:QUARANTINED 相位立即拒旧 active。

### 7.11 本轮实际产出与边界

- **只做了架构分析与设计定稿(本节 §7)**,**未改任何业务代码 / proto / yaml / 脚本**。
- 未 commit / push / tag,未碰生产,未写 secret,未改本机环境。
- **待人拍板**:是否按 §7.9 推翻 §6.6–§6.8 的 K8s-first 权威模型、实施 B 方案。拍板前 enforce 保持关闭,
  现有休眠代际门不动。UE Bearer 传输、cpp pb 跨仓拷贝、发布/密钥 ops 门仍归 UE/Codex/人。
- **2026-07-12 自主决策记录**:征询拍板时人不在线(授权「自主决策」)。经权衡仍**决定暂不实施 §7.9**——
  理由:①§7 方案实质推翻现有权威模型,AGENTS.md §7 / CLAUDE.md §7 明确要求此类改动**先经人拍板再改**,
  「人不在线」不等于授权推翻权威模型;②仅落 P0 proto additive 而不接全链路 = CLAUDE.md §14 禁止的
  半成品 / 空实现(新 message 无人消费);③完整实现跨独立 UE 仓(Bearer 传输,enforce 硬阻塞)+ 需真集群
  (真 Redis WATCH / envtest-minikube / UE Automation)验证,本地无法闭环。故本阶段**完整、非半成品的交付物
  就是本决策文档**,留待人 review 后再按 §7.9 分批实施。现网无任何行为变化。

### 7.12 后端全量实施记录(2026-07-12,人拍板「批准 B,一次实施后端全部 §7.9 步骤 1-5」)

人明确拍板批准 B 方案并要求**一次实施后端全部**。已按 §7.9 步骤 1-5 落地后端全链路,**默认休眠**
(仅 `ds_auth.authority_mode=redis` + agones + enforce 三者齐备才激活;默认 `legacy` 保持现有已验证
代际门,行为零变化)。UE Bearer 传输(步骤 7)与 ops 激活栅栏(步骤 6)仍归 UE 仓 / 人,不在本轮。

**已实现(全部 build/vet/test green,gofmt clean)**:

1. **proto(additive+休眠)** `proto/pandora/hub/v1/allocator.proto`:
   - 新增 `enum HubAuthPhase`(UNSPECIFIED/BOOTSTRAP/ACTIVE/ROTATING/QUARANTINED/TERMINATING)、
     `message HubDSCredential`(gen/jti/exp_ms/kid/instance_uid/protocol_epoch/token_sha256)、
     `message HubShardAuthStorageRecord`(pod/uid/epoch/phase/active/pending/high_water_gen/…)。
   - `HeartbeatResponse` +`accepted_token_gen`(激活确认回显);`HubShardStorageRecord`
     +`last_verified_gen/jti/gameserver_uid/auth_epoch`(授权投影镜像);`HubAssignmentStorageRecord`
     +`hub_instance_uid/auth_epoch/auth_gen`(分配绑定)。全部新编号,旧编号不动(不变量 §5/§17)。
   - go pb 已用 buf 本地重生解锁编译;**Codex 需按官方 `tools/scripts/proto_gen.ps1` 重跑一次并同步
     cpp pb 到 UE 仓 `Source/Pandora/Generated/Proto/`**(§5)。
2. **pkg/auth** `jwt.go`:`DSCallbackClaims` +`ds_uid`/`ds_epoch`;新增 `UID()/Epoch()/JTI()` 访问器、
   `SignHubCredential(pod,uid,epoch,gen,jti,ttl)`(签 Model B 凭据 + kid + token_sha256)。
   **顺带修复**:`SignDSCallbackWithGen` 遗漏 `t.Header["kid"]=…`(注释在、赋值缺,致 DS 回调令牌
   无 kid 头、轮换期校验侧无法按 kid 路由密钥),已补回(`TestRotationKidHeaderPresent` 恢复通过)。
3. **data** 新增 `internal/data/hub_auth_repo.go`:`HubAuthRepo` 接口 + `RedisHubAuthRepo`
   (key `pandora:hub:auth:{pod}` 同 `{pod}` hashtag 可事务;全 WATCH/MULTI/EXEC,plain `proto.Unmarshal`
   不丢 unknown,不变量 §17):`InitAuth`(建/换实例复位 epoch++/幂等)、`StagePending`(gen 严格高于
   high_water 单调门)、`PromoteOrValidate`(**线性化点**:首个合法 pending 心跳原子激活 / 重复幂等 /
   stale fail-closed)、`MarkDelivered`、`RemoveAuth`。
4. **pkg/middleware** `dsauth.go`:`VerifiedCredential` + `CheckHubCredential`(验签后抽凭据;legacy 令牌
   无 uid → 返回 nil credential 让 service 回退代际门;中间件保持无 Redis 依赖)。
5. **biz** `hub.go`:`HubUsecase` +`authRepo`/`SetAuthRepo`;`HeartbeatWithCredential`(promote→刷镜像→
   投影 last_verified,promote stale 早退 fail-closed,`Heartbeat` 6 参签名不变兼容旧测试);
   AssignHub / TransferHub **授权终态门** `authAllowsAssign`(记录须 ACTIVE/ROTATING,否则拒);分配时
   `bindAssignmentAuth` 钉实例身份/纪元/代际。`agones_fleet.go`:Model B 两阶段投递 `ensureHubCredential`
   (InitAuth→StagePending→annotation CAS 投递→MarkDelivered;legacy `ensureDSToken` 原样保留,
   `ensureDSTokenOrCredential` 分发)。
6. **service** `hub.go`:`Heartbeat` 改调 `CheckHubCredential`,Model B 令牌走 `HeartbeatWithCredential`
   并回显 `AcceptedTokenGen`,legacy 走原路径。
7. **config** `pkg/config/config.go`:`DSAuthConf` +`AuthorityMode`(`legacy` 默认 / `redis`)+
   `AuthorityModeRedis()`。**main** `cmd/hub_allocator/main.go`:`modelBAuthority = agones && enforce &&
   authority_mode==redis` 时构造 `RedisHubAuthRepo`、注入 `af.SetHubAuthority`(带 `issueHubCredential`:
   INCR gen + uuid jti + `SignHubCredential`)、`uc.SetAuthRepo` 并关闭 legacy `SetDSTokenGeneration`
   (Model B 取代);否则完全走 legacy。
8. **测试**:`hub_auth_repo_test.go`(init/reset/幂等、stage 单调门、promote 激活/幂等/stale、无记录)、
   `hub_modelb_test.go`(AssignHub 门 无记录拒 / ACTIVE 放行+绑定、Heartbeat promote 回显+投影、stale
   fail-closed)、`dsauth_test.go::TestCheckHubCredential`(Model B 令牌出凭据 / legacy 出 nil)。

**交接给 Codex / 人**:
- **Codex**:①按 `tools/scripts/proto_gen.ps1` 官方重生 go pb + 同步 cpp pb 到 UE 仓;②本轮**未引入新
  pkg module**,无需额外 `go mod tidy`(仅用既有 `github.com/google/uuid`、`redis/v9`、`miniredis`);
  ③git status/diff/commit 建议由 Codex 出,commit 由人授权后执行。
- **人 / UE 仓**:步骤 7(UE DS 拿 annotation `pandora.dev/ds-token` 作 `authorization: Bearer` 回调)与
  步骤 6(ops 激活栅栏 / 灰度切 `authority_mode=redis`)。默认 `legacy` 下现网**零行为变化**,可安全先合。

### 7.13 第二轮 reviewer 复审硬化(2026-07-12,人拍板批 B 全后端后的怀疑式复核,已实施)

reviewer 独立核实 §7.12 后端全量实施仍有 P0 **原子性 / legacy 旁路 / TTL** 反例(均在代码层证实,非空口)。
人 TOP 推荐:「将 `ActivateHeartbeat` 和 `ReserveRoutableSeat` 分别做成 auth+shard 同槽原子事务,并彻底
删除 Redis authority 下的 legacy fallback」。本轮按此实施后端核心(proto-free,全 miniredis 端到端可验证)。

**核实的反例(实施前证实)**:
- **CE1/CE2 legacy 旁路**:Model B 下 `dsTokenGeneration=false`,但 service `Heartbeat` 对 legacy 令牌
  返回 `cred=nil` → 走 `uc.Heartbeat` legacy 路径 → `HeartbeatShard` 能把 warming→ready / 保活,
  **绕过授权记录**;迁移期 shard 有旧代际时,Model B 心跳(tokenGen=0)反被代际门拒、legacy gen=5 却能刷
  ready,配合 auth ACTIVE → 误分配。
- **CE4/CE6/CE8 非原子**:旧 `heartbeat()` 先 `PromoteOrValidate`(原子改 auth)再 best-effort
  `projectShardAuth`+`HeartbeatShard`(两步);promote 后崩溃 = auth ACTIVE 但 shard 旧。AssignHub 的
  `assignAuth` 与 `reserveSeat` 是「先检查后占」分离(违反不变量 §9)。**CE8 确证 TTL bug**:把
  `shardTTL`(30m)传给 auth 仓,auth 记录 TTL 从 48h 缩成 30m。
- **CE7**:`CreateShard` 无条件 `SET` 覆盖并发心跳写入的 `last_verified`。
- **CE9-iii**:`QUARANTINED`/`TERMINATING` 未挡 `StagePending`/promote。

**本轮实施(全部 `go test ./services/battle/hub_allocator/...` green,gofmt/vet clean)**:
1. **auth TTL 独立**:`HubUsecase` 加 `authTTL`+`SetAuthTTL`;`ActivateHeartbeat` 入参 `AuthTTL`/`ShardTTL`
   分开传,auth 记录不再被 shardTTL 缩短(**CE8 修**)。
2. **phase 门**:`StagePending` 与原子 promote 在 `QUARANTINED`/`TERMINATING` 直接 `errAuthStale`(**CE9-iii 修**)。
3. **`ActivateHeartbeat`(新 `data/hub_authoritative.go`)= 唯一线性化点**:对 `authKey`+`shardKey`(同 `{pod}`
   hashtag,同 slot)单次 `WATCH/MULTI/EXEC`:校验凭据元组(uid/epoch/jti/gen)+ phase → promote
   pending→active → 一次 EXEC 写 shard(warming→ready / count / state / last_heartbeat + 投影
   last_verified 元组 + gameserver_uid/auth_epoch)。shard 不存在 → `ShardFound=false`**不 promote、不写**
   (biz 先 `reconcileShardTopology` 再重试一次,保证 promote 与 ready 同一事务,**CE4 修**)。
4. **`ReserveRoutableSeat`/`CheckRoutable`(同 slot 原子终态门)**:共用 `routable()`,8 项校验
   (phase∈{ACTIVE,ROTATING} / active 完整未过期 / shard 存在 ready / `last_verified==active` /
   uid+epoch 一致 / 非 draining / 容量 / 心跳新鲜)全过才 `seat++`(`reserve=true`)并返回绑定元组;
   `CheckRoutable` 只读不占。AssignHub/TransferHub 走 `reserveRoutableSeat`+`bindAssignmentAuth`;
   reuse 路径用只读 `CheckRoutable`+校验 `assignment` 元组==active(**CE6「先检查后占」修**)。
5. **`CreateShard` 改 `SETNX`**(init-only,不覆盖既有 heartbeat / last_verified,**CE7 修**)。
6. **删 Redis authority 下 legacy fallback**:service `SetModelBAuthority(true)`,`cred==nil && claims!=nil`
   → `ErrUnauthorized`;biz `heartbeat` 在 `authRepo!=nil && cred==nil` → 拒(**CE1/CE2 修**)。
   删 `PromoteOrValidate`/`PromoteResult`/`projectShardAuth`/`authAllowsAssign`/`assignAuth`。
7. **`applyHeartbeatToShard` 纯函数**提取,`HeartbeatShard`(legacy)与 `ActivateHeartbeat` 共用。

**主动反证测试(barrier/hook,非 sleep)**:
- `data/hub_authoritative_test.go`:promote 翻转与 shard 写入同事务、CE8 TTL(`mr.TTL(authKey/shardKey)`
  分别断言)、幂等重复不再 promote、quarantine 相位锁、无授权记录拒、reserve 8 门逐项、只读 CheckRoutable
  不占位、**8 goroutine + barrier channel 并发 promote 恰好 1 个 Accepted**。
- `biz/hub_modelb_test.go`**重写为真 miniredis 端到端**(废弃内存 `fakeAuthRepo`,`RedisHubRepo`+
  `RedisHubAuthRepo` 同 rdb,让 biz 心跳/分配真正跑原子事务):心跳激活 warming→ready+promote+投影、
  Model B 下 legacy 无凭据心跳拒且不翻 ready、stale 凭据 fail-closed 不刷镜像、未激活分片 AssignHub 拒、
  激活后放行且归属钉 active 元组、幂等复用不重复占位、**实例漂移(uid/epoch/gen 变)使旧归属失效并重分配**。

**关键回答(旧 token 为何无法通过任何写接口)**:①service 在 `modelBAuthority` 下 `cred==nil`(legacy token
无 uid)→ `ErrUnauthorized`;②biz `heartbeatModelB` 对 `cred==nil` → 拒;③`ActivateHeartbeat` 要求凭据元组
(uid+epoch+jti+gen)匹配 pending/active 且 phase 未锁,否则 `errAuthStale`;④`ReserveRoutableSeat` 要求
`active==shard.last_verified` 且 uid/epoch 一致,旧 token 造不出被 active 投影的 shard 镜像。

**当时仍未做(历史交接；后续已由 §7.14 部分或全部取代)**:UE C++ Bearer 传输(独立仓)、Battle token 元组绑定 + 7 个 DS RPC、
投递侧 409/PATCH UID precondition + MarkDelivered CAS 的真集群验证、locator 等跨服务 active checker、
activation fence(etcd required_auth_epoch + capability lease + Activation Job)、ACK 元组 additive proto
(与 UE 一起做)、envtest/minikube/真 Redis Cluster/`-race -count=100`/UE Automation 真基础设施测试。
**本轮无新依赖,无需 `go mod tidy`;无 proto 改动(仅复用 §7.12 additive 字段);未 commit/push/tag。**

### 7.14 P1 全链路收束与独立反证修复(2026-07-12,当前脏工作树)

在 §7.12–§7.13 基础上继续核实代码，而非相信“已闭环”自述。下列反例已落实现与测试：

1. **Battle 结算顺序线性化**：新增同 `{match_id}` slot 的
   `pandora:ds:result-receipt:{match_id}`。`battle_result` 只有在 MySQL 幂等落库成功后，才以
   auth+battle+receipt 同槽 WATCH 重新核验完整 `(allocation,UID,epoch,gen,jti,exp,kid,hash,writer)`
   并写 receipt；`ended` 心跳无匹配 receipt 时零写入返回 invalid-state，receipt 存在时才与
   `auth→TERMINATING / battle→ended` 同一 EXEC 消费。响应丢失重试先命中 DB 幂等，再补 receipt。
2. **Battle allocation 半失败**：GSA POST 前先把 `allocating + allocation_uncertain` 持久 fence
   写入 Redis；POST/Finalize/PATCH/GET 任一响应未知都保留 fence，禁止第二次 POST。只有已经取得
   `(allocation_id,pod,UID)` 的实例，经 UID precondition Release 明确成功并完成 Redis readback 后，
   才允许清理 projection/fence；Release 超时、失败或进程崩溃均保留永久 `TERMINATING` 墓碑。
   `allocation_uncertain` 目前是安全隔离，不是 sweep 猜测恢复；自动恢复仍属于 §7.16 待决策项。
3. **紧急吊销**：Hub/Battle 新增 `QuarantineExpected` 完整 tuple CAS。Hub 同槽锁 auth 并把 shard
   置 draining；Battle 同槽锁 auth、写 abandoned 并保留可靠补偿索引。另提供默认 audit-only、显式
   `-apply` 的受控 ops CLI；错误 UID/epoch/gen/jti/hash/writer 全部零变更。普通 ROTATING 不替代泄露吊销。
4. **Hub assignment 零停机兼容**：重分配、Transfer、drain migration 全部从旧 protobuf clone 后覆盖
   已知字段，保留未来 unknown fields；同一 UID+epoch 的凭据轮换只 CAS 重绑 assignment，不重复占座。
   UID/epoch 变化仍视为新实例，旧 assignment 不复用。
5. **跨服务写门**：locator HUB 写、battle_result、login ticket、Hub/Battle GM/heartbeat/assignment
   均在副作用前核验 Redis active 完整 tuple、服务端心跳与投影；Redis 失败返回 unavailable，不能降级放行。
   `battle_result` 在 Redis authority 下启动时还会机械拒绝订阅无凭据的 `pandora.battle.result`，只保留
   `pandora.ds.lifecycle`；结算唯一入口为受 Guard+active+receipt 保护的同步 `ReportResult`。
6. **控制面机械硬化(不等于已可激活)**：capability 注册同一 etcd Txn 比较 required value +
   `ModRevision` + activation lock，封住正常推进与 ABA；bootstrap 库/CLI 只允许 baseline=1；writer epoch
   必须由五服务显式声明且审计精确等于 target。所有后台 writer 在 capability 成功后才启动。
   Redis audit 双向扫描 live projection↔auth、至少三周期，并把 exp/kid/hash/high-water/required 纳入稳定身份。
7. **玩家在线准入**：`:8443` 精确拒绝 `VerifyDSTicket`，`:8444` 增加唯一 exact route；login 按
   `Guard → Redis active+projection → ticket binding/assignment/roster → JTI marker` 固定顺序执行。
   `admission_id` 只接受小写 RFC4122 UUIDv4；同一次尝试在服务端 30s 短窗内可幂等恢复响应丢失，
   不同尝试、legacy marker、UID/epoch/drain/quarantine 变化均 replay 拒绝，且失败不提前写 JTI。
8. **本机开发机械隔离**：Windows `mode=local + ds_auth.mode=off + authority=legacy` 才能启动
   `local-off-v1`。allocator 仍签发含 `(pod,随机 UID,epoch,gen,jti,exp,kid,writer=2)` 的 JWT，
   并计算 token hash 供凭据身份使用，
   并注入 `PANDORA_DS_LOCAL_PROFILE=local-off-v1`；UE 还须同时证明 Windows、非 Agones、本地 pod
   前缀和 token scope 才允许直接 active/关闭在线 admission。生产/Agones、permissive/enforce、Redis
   authority 或缺签发器均不能进入此 profile，legacy JWT 继续拒绝。Hub 凭据由 env 一次性下发，
   因此无论 Guard 是否为 off 都机械要求 `hub_token_ttl >= 12h`；不能等 UE 到 exp 清空 active 后才发现断路。
9. **Battle 票据签发归属门**：公共 `IssueDSTicket(battle,target_id)` 与登录断线重连共用唯一 issuer，
   不再分别信任客户端自报 match 或 locator。login 在签名前读取 Redis 权威 roster；legacy/local 要求
   projection 是 live `ready/running`、心跳新鲜、
   roster 非空且含当前 player，Redis authority 还以同槽 `MGET` 同时核验 auth/projection 的完整 active
   tuple、allocation、UID/epoch、writer/high-water、last_verified 与心跳。Redis 故障、坏 protobuf、
   空 roster、非成员或任一漂移均不签票；未装配 authorizer 也 fail-closed。这样在 Model-B 行为正式
   激活前的 legacy rollout 窗口，不能靠“知道 match_id”取得可入场票。若 locator 已明确玩家仍
   `InBattle`，此后 authorizer/Redis/签名失败必须让本次 Login 返回 unavailable；不得继续 AssignHub、
   占第二个 seat 或写 LOGIN_PENDING。locator 成功且明确 `!InBattle` 才正常走 Hub；查询不可判定时
   仍按既有待决策弱降级，具体生产阻断与候选见 §7.16.3。
   reconnect 地址也必须使用同一次 roster GET/MGET 返回的 live projection `ds_addr`；不能证明了重建后
   实例 B，却把 locator 中旧实例 A 的地址返回。空地址拒绝，同名/同 match UID 重建后只返回 B。
10. **本机票据验签密钥 fail-closed**：UE tracked dev 占位只允许精确 `local-off-v1` 使用。Agones、
    非本机 profile、profile/pod/scope 伪造、空值、短 secret 或占位 secret 均不得回退放行；非空环境
    变量一旦成为候选，坏值不得静默回退配置文件。此代码门只阻止误用，**不等于**已完成生产
    Secret/keyset 投递。
11. **UE JTI 防重放缓存有界**：永久 `TSet` 改为线程安全 `JTI→exp+leeway+1s` map；检查/消费前
    原子清过期，JTI UTF-8≤256 bytes，容量为
    `min(effective MaxPlayers×每玩家窗口预算, configured absolute max, 65536)`。满载/非法配置
    fail-closed，绝不驱逐未过期 JTI；`PreLoginAsync` 只检查且可清过期项、绝不新增 JTI，online authority 成功后的
    `InitNewPlayer` 才消费（local-off-v1 是显式离线例外）。因此长驻 Hub 不能靠反复取新票/连断让
    `ConsumedJtis` 永久无界增长，也不会因容量驱逐重新放开重放。
12. **UE unary trailer 严格解析**：`grpc-status` 改为手写 canonical 十进制有界累加，只接受唯一的
    `0..16`；空值、非数字、前导零（`0` 除外）、重复字段、`int32` 溢出和超出标准范围全部保持
    `status=-1` 并让整次 unary 失败。`4294967296` 等输入不能再经 `Atoi` 回绕为 0 冒充成功。
13. **UE 心跳响应命令绑定**：Hub/Battle 每次心跳固定捕获实际用于 Authorization 的 credential
    snapshot，成功响应的完整 ACK 必须先精确匹配该 snapshot。普通 active 心跳在回调时还必须证明
    snapshot 仍是 manager 当前 active；A 请求在 B 提升后迟到，即便自洽 ACK(A) 也会清空 command 并
    fail-closed。activation 响应则先由同锁 `TryPromote` 精确提升 staged，只有提升成功才处理命令；
    active=A/staged=B 时迟到 ACK(A) 不再被 active 幂等分支误认为 B 已激活。该 ACK 只防响应串线与
    乱序，不能替代 §7.15 的 mTLS。
14. **UE admission owner 真 UUIDv4**：Automation 实跑发现 `FGuid::ToString` 的平台字段布局/版本行为
    不能直接证明 RFC4122 v4，且 `FGuid::ParseExact` 会接受非 canonical 大写输入。生成器改为用 UE 已
    链接的 OpenSSL `RAND_bytes` 取得 128-bit CSPRNG，按网络字节序设置 version=4 与 variant=`10xx`
     后编码小写 4-2-2-2-6 字符串，保留 122-bit 随机性；随机源失败返回空并在任何网络请求前拒绝。
     校验器逐字符强制 36 长度、四个固定连字符、仅小写 hex、version 4 和 variant 8/9/a/b，不再把
     宽松 parser 当 canonical 证明。
15. **幂等复用刷新 assignment TTL**：旧快路径在 assignment bytes 完全相同时直接重签票，不刷新
    Redis TTL；归属只剩数秒时可签出 5min 新票，随后在线 admission 因 assignment 先到期而拒绝。
    现统一走完整 bytes CAS SET 刷新 30min TTL，仍不重复占座，也不放宽并发前置。

已验证的关键矩阵包括：真并发 claim/assignment、PATCH timeout-but-applied/409/坏响应、Redis failure、
UID 重建、stale/future/wrong tuple 零副作用、AssignHub 同实例轮换不增容量、assignment future wire field
跨重分配/Transfer/drain 保留、Battle receipt 前 ended 拒与 receipt 后原子终止、quarantine 错 tuple零变更，
以及 battle 签票 legacy/Model-B 的成员、空 roster、陈旧心跳、auth 漂移和 Redis bytes/PTTL 零变更；
Hub 幂等复用还覆盖 assignment 剩 1s 时重签并恢复完整 TTL；
UE replay cache 的过期边界、满载零新增、128 路同 JTI 单次消费与 128 路 unique 硬上限，
TryPromote 旧 ACK、grpc-status 溢出、Hub/Battle missing/wrong/stale ACK 的 command 零副作用，
以及 1000 个 UUIDv4 生成烟测与空/长短/大小写/version/variant/连字符/非法字符负例。

本地最终证据（不替代真集群验收）：UUID 修复后的 `PandoraEditor Win64 Development` UBT
`714/714`、`Result: Succeeded`（1164.39s）；headless `Pandora.Net` 11/11 与 Battle terminal 1/1，
合计 Automation 12/12；`PandoraServer Win64 Development` UBT `824/824`、`PandoraServer.exe`
链接成功、`Result: Succeeded`（1132.36s）。Automation 第一轮曾是 10/11，真实暴露 `FGuid` UUID
反例后才实施 §7.14-14 并全量重跑，不能把第一轮误作通过。服务端相关 Go test/vet、proto lint、
online/services Kustomize render、Envoy config validate、PowerShell AST 与 `git diff --check` 均已通过；
`-race` 因本机 `CGO_ENABLED=0` 未执行且未改环境。真 Redis Cluster/envtest/minikube、安全传输上的
集群内 synthetic 与真 Hub/Battle 往返仍未执行；当前发布边界以 §7.17.7、§7.18 和审核清单为准，
不能把“本地全绿”解释成允许上线。

### 7.15 行为激活子决策（历史候选；B 已批准并实施）

独立审计给出确定反例：配置滚成 `authority_mode=redis + enforce` 后，新 v2 Pod 在 etcd required 仍为1时
已经执行 Model B；五个 Service 激活前仍只按 `app` 选择 Pod，旧 legacy writer 与新 Model-B writer 会在
滚动窗口同时接流量/跑后台任务。最终 capability/Redis/synthetic 审计无法撤销此前混写。因此 §7.7
原“先滚完再 CAS”不满足核心缺口 #8，`required_writer_epoch` 目前只是进程存活 fence，不是行为开关。

候选方案：

- **A. 单进程 prepare/stage-only 门**：required=1 时 v2 只注册 capability、投递 pending、接受激活心跳；
  assignment/result/locator/GM/后台 writer 全禁，CAS 后开放。优点是 controller 少；缺点是每条路由和后台任务
  都要显式相位门，且旧 writer 的 quiesce/摘流仍需另一套线性证明，漏一条即旁路。
- **B. blue/green writer 集合 + prepare/quiesce/active(独立推荐)**：green 使用独立 Service selector，预热阶段
  只做 stage/activation synthetic；旧 writer 获得可证明的 quiesce lease、从 Service/后台 leader 集合摘除后，
  再以单一 Activation Job 持 etcd lock 同时核对最终 K8s GameServer CR UID、Endpoint、capability、Redis，切 green
  writer lease/selector 并单调推进 required。任一半失败只会无 writer/不可用。成本更高，但状态空间更小且可机械反证。
- **C. 停服切换**：实现简单，但违反零停机目标，拒绝。

另有四个必须一起批准/交付的 production blocker：

1. `decision-revisit-image-tag-immutability.md` 的 digest pin + admission allowlist + 全保留 RS/Fleet/GameServerSet
   回滚审计；当前 mutable tag 不能证明 label=2 就是 v2 二进制。
2. revisioned immutable Secret/keyset、runtime/activator 分离身份与 etcd mTLS/ACL、Redis TLS；当前 capability
   仍依赖网络边界，不能作为生产信任根。更具体地，当前 Battle/Hub Fleet 都未安全注入
   `PANDORA_DS_TICKET_SECRET`，而 UE 会在玩家联网前本地验 DSTicket：生产 signer 使用真实 key 时会全量
   拒票，若生产沿用 tracked dev 占位则失去安全边界。必须在 Secret revision/JWKS/RS256 方案获批后
   完成 immutable 投递与轮换；不得把占位值写进生产清单。更不能把现有玩家 HS256 签名 secret
   直接交给不可信 DS；应在 `decision-revisit-player-jwt-key-rotation.md` 中拍板 DSTicket 公钥验签或
   仅 online authority 后，投递 revisioned public keyset（或不投递玩家 key）。
3. 仓内固定 digest 的 K8s :8444 synthetic Job，验证 missing/bad/stale/active token 与 Redis/MySQL/locator
   零副作用；不接受任意外部 `exit 0` 脚本。
4. **:8444 传输身份与机密性**：当前 Fleet 明确 `TLS=0`，active Bearer、玩家 DSTicket、GM Poll 与
   stop/drain/reload 响应都走集群明文。NetworkPolicy/方法白名单只收敛可达性，不能阻止有东西向包
   可见性者窃取 token/重放写请求或伪造响应。须由人拍板并交付 service-mesh STRICT mTLS、SPIFFE/SAN
   绑定或等价的 Envoy 双向 TLS + revisioned CA/客户端身份/零停机轮换；synthetic 也必须走真实安全传输。
   response ACK 精确绑定是应用层防串线门，**不能**替代 TLS。

方案 B 后续已获批准并实施，当前实现与新增阻断见 §7.17~§7.18：脚本参数为
`-Phase Audit|Activate`，并不存在 `-Apply`；Go CLI 才使用 `--apply`。由于真实 Redis topology-change
lease provider 尚未接线，fresh/retry `-Phase Activate` 在任何 create/patch/scale 前 fail-closed，CAS 前
再次阻断，Audit 也只返回只读 BLOCKED 结论。不得以 ConfigMap、人工部署顺序、重试、下一轮自愈或
“最终审计绿”替代真实执行锁与行为激活证明。

### 7.16 新发现的状态机子决策（历史决策记录；当前状态以各小节末尾为准）

下列反例由真实代码与现有测试重新推导，均不是加重试能闭环。各候选与“待拍板”文字保留发现时的
决策历史；§7.16.2、§7.16.3、§7.16.4 的后续实施状态分别记录在各小节末尾，不能再用本节旧标题判断
当前工作树。未获批的 §7.16.1 仍不得暗中放宽授权条件。

#### 7.16.1 Hub 票据跨普通凭据轮换的首次准入 grace

**反例**：T0 login 用 active A 签出尚有 5min TTL 的 Hub DSTicket；T1 同 UID/epoch 的 pending B
激活并成为 current active；T2 玩家首次连接且 JTI marker 不存在。当前 UE 在网络请求前要求票据
`gen/jti == B`，login strict admission 也要求票据 A 等于 caller B，因此确定拒绝。响应丢失重试允许
同实例轮换并不能覆盖“尚未首次准入”的票据。故当前平滑轮换会制造最多 5min 的有效票断档。

候选：

- **A. 强制客户端重新取票**：安全但违反零停机/已有票据有效期契约，拒绝作为生产闭环。
- **B. 单个 `previous_admission` grace(独立推荐)**：promotion 时把旧 active 的完整 tuple 复制到
  additive `previous_admission`，并以 Redis TIME 写 `previous_admission_valid_until_ms = promote_time +
  DSTicketTTL + skew`。它**只供 login 首次准入核验**，绝不能通过 callback/locator/result/GM/
  assignment 的 active checker。login 只有在 caller 当前 B 仍 active+healthy、票据中可见的
  A `(pod,UID,epoch,gen,jti,writer)` 精确等于 previous、
  pod/UID/epoch/writer/assignment_id 稳定、shard ready 且未 drain/quarantine 时才接受；UE 本地前置只比较
  稳定 lineage，最终授权仍以在线结果为准。仅首次 A→B promotion 创建 previous；B 的幂等 active
  heartbeat 不得覆盖或续期。B→C promotion 必须在同一个 Redis 事务用 Redis TIME 证明 grace 已结束，
  否则拒绝。初次 BOOTSTRAP→A 不创建 previous，quarantine 原子清空 previous。还须给票据
  `exp-iat` 配置加集群级硬上限，并在 grace 核验 `ticket.iat <= retired_at+skew`，防 signer 配置漂移或
  退休竞态签出的票超出证明窗口；rotation lead 必须机械大于 `max DSTicketTTL+skew`。
- **C. 不存 previous、只比较 UID/epoch**：无法证明票据里的 gen/jti 曾经被授权，泄露/伪造旧 tuple
  可借稳定身份旁路，拒绝。

若批准 B，必须新增 proto additive 字段、promotion/login/UE 接线，并覆盖 A→B 票据、B→C 过快轮换、
grace 边界、UID 重建、drain/quarantine、assignment 变化以及所有失败零 JTI 写。

#### 7.16.2 Battle 结算终态不能依赖即将过期的 callback token

**决策状态（2026-07-13）**：已人工选择并实现候选 B。实现已通过本地/隔离 MySQL
测试，但上线仍须先执行 additive migration、接入 online mesh component，并通过 §7.15 的生产激活门；
本段状态不等于已对生产集群 Apply。

**反例**：ReportResult 已完成 MySQL 幂等落库并写 receipt，但响应或随后 `ended` ACK 持续丢失；
receipt 的保留期按 battle 生命周期延长，而 Guard/UE active snapshot 仍在 JWT exp 时刻失效。receipt 又会
阻止普通 credential promotion。到 exp 后，DS 既不能重发 ReportResult，也不能发送可被接受的 ended，
资源与客户端终态握手可能永久卡住。简单允许过期 JWT 或 receipt 充当所有 RPC 凭据会扩大重放面，拒绝。

候选：

- **A. terminal-only capability**：ReportResult 返回只允许 ended/ACK 的短时、单次能力；仍需解决响应
  丢失前能力未送达和后端资源回收，状态更多。
- **B. 服务端驱动终态 + 持久 release outbox（已选择）**：把 terminal-release outbox 行与
  `battles/player_stats` 放进现有 `SaveResult` 的**同一 MySQL 事务**，消除 MySQL commit→Redis 之间的
  崩溃窗；relay 再以该行携带的 `(allocation_id,pod,UID,epoch)` 幂等 CAS Redis terminal/receipt，随后
  用 UID precondition 回收，成功才确认 outbox。不得声称单个 Redis 同槽事务解决跨 MySQL/Redis 原子性；
  若不共事务，就必须给出从 DB 扫描重建 outbox 的等价证明。UE 的 ReportResult/ended 重试只影响通知
  体验，不再决定资源安全释放；还需明确客户端回大厅通知与强制断线策略。
- **C. 延长 token/无限重试**：只能缩小概率，不能消除 exp 边界，拒绝。

**落地状态机**：

1. `ReportResult` 先以服务端 Redis active snapshot 完整授权，把 battle/player_stats、业务 outbox 与
   `terminal_release_outbox` 的原始 proof 放进同一个 MySQL 事务。DB commit 失败绝不返回 OK；commit
   成功后即时 Redis receipt 写失败仍返回 OK，因为 durable outbox 已能恢复。幂等重试只读既有结果，
   不用新 credential 覆盖旧 proof，也不写第二行。
2. 宽限窗后，pending 行发 `ReleaseBattle(reason=completed)`，携带原始
   `(match,allocation,pod,UID,epoch,gen,jti,exp,kid,hash,writer,authorized_at)`。`ds_allocator` 只接受
   完整 proof 与当前稳定实例/receipt 精确匹配；允许 proof 此时已过 JWT exp 或 current gen 已普通轮换，
   但这不是通用过期凭据授权。它先原子写**无 TTL** terminal/receipt 墓碑，再做 Kubernetes UID
   precondition delete；allocation/UID/epoch 漂移零副作用且不 ACK。
3. phase1 明确成功后，worker 才以 `released_at_ms=0` 的 MySQL CAS 写入服务端时间。RPC 响应丢失或
   DB mark 失败都保留永久墓碑和 outbox；重启后按 DB 真值决定重放 phase1 还是进入 phase2。
4. released 行只发 `ReleaseBattle(reason=completed-finalize)`。该路径只校验同一 proof 并给三份墓碑
   恢复有界 TTL，**绝不再次删除 Kubernetes**；若 finalize 响应曾丢失且 DB delete 长期失败，TTL 后
   三键全不存在也按幂等成功。随后仅允许 SQL
   `DELETE ... WHERE id=? AND released_at_ms>0` 删除 outbox。
5. 启动 capability 前精确探测表列/索引、`ENGINE=InnoDB`、表 collation 与
   `released_at_ms NOT NULL DEFAULT 0`。内部 ReleaseBattle proof 本身不是签名票，因此 online 仅允许
   battle-result SPIFFE 身份经 mTLS 调 exact RPC；`:8444` DS 面不暴露该方法，match-id-only Model-B
   调用永久拒绝。

Hub quarantine assignment 迁移/UID 退役是否共用持久 outbox，以及 `allocation_uncertain` 的权威
reconciler 仍是独立子决策；在此前保持“安全隔离、可能不可用”，不得借本节声称自动恢复。
Redis authority 下无凭据的 `pandora.battle.result` consumer 已由启动校验禁止；终态方案须永久保持该
单入口，不能再让消息 handler 直接调用 `ReportResult` 绕过 active/receipt。若未来恢复 topic，消息本身
必须携带可核验 Model-B 身份并进入同一 MySQL outbox 状态机，需另行设计。

#### 7.16.3 locator 不可判定时不能先分配 Hub 再做后置通知(2026-07-15 已实施)

**历史反例**：玩家仍在 Battle X，locator 保存 `BATTLE(X)`，但 login 的三次
`GetBattleLocation` 都因 locator/网络故障返回 error。若 login 随后 `AssignHub`（占 seat 并签 Hub 票），
再 best-effort `NotifyLoginPending`,后者即使被 BATTLE fence 拒绝也撤不回 assignment/ticket；Hub admission
只核 DS active 与 assignment,不核玩家 placement。因此玩家可同时持有 Battle 与可用 Hub 归属。

**2026-07-14 Login 状态（历史）**：候选 A 已在 B1 模式实现。`require_hub_assignment_binding=true` 时,locator 未配置、
查询失败或 `NotifyLoginPending` 失败都返回 unavailable,且发生在 `AssignHub` 之前；Redis authority 的集群
配置生成器会开启此门。对应回归测试覆盖“未配置 locator”“locator 查询失败”“LOGIN_PENDING 失败”下
Hub assigner 零调用。本地/off 模式显式关闭该门时仍保留弱降级,不能拿该 profile 证明生产安全。

**2026-07-14 尚未闭环的旁路（历史反例）**：`IssueDSTicket(ds_type="hub")` 没有复用上述 B1/locator/LOGIN_PENDING 门,而是直接
`ResolveHubEndpoint → AssignHub`。客户端 Battle 直连失败和 30s 重连超时都会调用它,所以本节的原始
双归属反例仍能从该入口发生。另外,DS→locator 刷新是 best-effort；若连续失败到 key 正常过期,
`GetBattleLocation` 会成功返回非 BATTLE 而不是 error,B1 也不会查询 live roster,仍可能误走 Hub。
详见 `battle-reconnect.md` §6.2、§6.3.1、§7.3 A/J。

候选：

- **A. 未知即拒绝(已用于 B1 Login,仍须覆盖所有 Hub 签票入口)**：查询重试后仍不可判定,返回
  unavailable,不 AssignHub、不签 Hub 票、不写 LOGIN_PENDING。下一步必须把相同门下沉到
  `IssueDSTicket(hub)` 和 Hub 最终 admission,并能区分“权威证明不在 Battle”与“placement key 缺失”,
  避免新调用方或 TTL 缺口再次旁路。
- **B. 版本化 placement lease + admission 最终门(独立推荐的标准方案)**：把玩家 placement 作为单一
  权威记录；login 先以 `(player_id,operation_id,expected_version)` CAS 取得有界 `HUB_PENDING` lease，
  仅非 active Battle 才成功。随后才 reserve seat/签票，并把 `placement_version/operation_id` 绑定进
  assignment 与票据；Hub `VerifyDSTicket` 最终准入再次核验当前 placement。seat 成功但 placement finalize
  失败时，票据因 version 不匹配不可入场，持久补偿/outbox 释放 seat；响应丢失按 operation_id 幂等回读。
  玩家在签票后转入 Battle 会推进 placement version，使旧 Hub 票自然失效。跨 player/shard slot 不得
  假装单事务，必须显式 saga+补偿或重新设计同槽键。
- **C. 维持后置 Notify/Hub join 再对账**：assignment 与票已经产生，且对账故障本身不可判定；只能
  缩短错误窗口，不能证明一人一 DS，拒绝作为生产闭环。

候选 A 的 Login 负例已存在；仍须新增 `IssueDSTicket(hub)` 在 active/unknown placement 下
Hub assigner、seat、assignment、票据全零测试,以及 Hub admission placement/fence 负例。若批准 B，还需
additive proto/票据 claim、login/hub_allocator/player_locator/UE 接线，覆盖
两次并发 Login、Battle↔Hub 竞态、reserve 成功后 CAS/响应丢失、补偿崩溃重启、旧 placement version
入场、assignment future unknown fields 与 Redis Cluster 跨 slot；未完成前生产 Apply 继续禁止。

**决策状态（2026-07-14）**：已人工批准候选 B 为最终方案（版本化 placement lease + admission
最终门），按“先服务端后客户端”的分阶段程序推进；C 拒绝。同日先落地 P0 止血
（候选 A 下沉到全部 Hub 签票入口）：

- login `ResolveHubEndpoint`（即 `IssueDSTicket(hub)` 实现）与 `SelectRole` 前新增
  `guardHubRouteAgainstActiveBattle` 三态权威门（**2026-07-15 Codex 复审修正**：改为显式
  `InspectBattleRoute` 三态 ACTIVE/TERMINAL/UNKNOWN，不再把 `AuthorizeBattleTicket` 的通用
  `ErrPermissionDeny` 当终态证明——roster 漂移/非成员/记录缺失都会折叠进该码，误当放行证据
  仍是双归属）：locator BATTLE 且权威判 ACTIVE → `ErrInvalidState` 零副作用拒绝（不 AssignHub、
  不写 role、不签票）；locator BATTLE 且投影记录显式终态（state ∈ {ended, abandoned} 且
  match_id 一致）→ TERMINAL，唯一放行路径（正常结算回大厅不受影响）；其余一切
  （roster 漂移/非成员 PermissionDeny、记录缺失、stale 心跳、match 不符、Redis/网络错误、
  locator 查询失败、未接权威）→ UNKNOWN，locator 已明确 InBattle 时不分 profile 一律
  `ErrUnavailable` fail-closed；local/off 仅对“locator 查询失败”保留历史弱降级。
  负例测试见 `services/account/login/internal/biz/hub_route_gate_test.go`
  （含 roster 漂移拒绝、显式终局放行、SelectRole 零 role 落库、TOCTOU 并发终局切换）与
  `services/account/login/internal/data/battle_ticket_authorizer_test.go` 三态矩阵。
- 客户端同日移除 `FallbackToHubViaIssueDSTicket`：battle 直连失败/本地 30s 超时不再自切大厅，
  改为权威重查循环（退避重登，去向由 LoginResponse 决定），超窗只升级可取消 UI。
- **P0 已知取舍**：对局进行中“主动退出回大厅”入口（若有）会被本门拒绝——主动放弃必须先由
  服务端执行显式离局事务（roster 移除/placement 推进），属候选 B 阶段工作；正常结算路径不受影响
  （DS 上报终局后 roster 权威判非 live，门放行）。§7.3 J（locator TTL 缺口误判非 BATTLE）
  同样待候选 B 根治。

**实施状态（2026-07-15）**：候选 B 已按服务端先行原则落地，以上“待/尚未”文字仅保留决策历史，
当前代码硬门如下：

1. `player_locator` 保存无 presence TTL 的版本化 placement；记录缺失/解析失败/权威依赖错误均为
   UNKNOWN，不再解释成“可进 Hub”。每次 Hub↔Battle 推进都使用 CAS version 与 UUID operation。
2. Hub/Battle DSTicket additive 绑定 placement version、operation、目标 DS identity 与 source match；
   login、matchmaker 和 allocator 只从 canonical binding 签票。旧 version ticket 即使仍在 `exp` 内也不能准入。
3. Hub assignment 在 Commit/Admission ACK 前后都是持久 canonical owner（TTL=0）；Hub DS 在 PreLogin 后必须调用
   placement-aware `AcknowledgeAdmission`，只有 exact binding Commit 成功才开放 spawn gate。ACK 响应丢失
   可用相同 admission/operation 重放；稳定 owner 不再回退到 30m TTL。客户端只有收到本次
   `recovery_attempt` 对应的 Reliable Admission committed RPC 才可把该次 Hub travel 判成功。
4. BattleResult 先为 canonical roster 写无 TTL signed terminal tombstone，再由 outbox CAS 精确
   `BATTLE(match)`/`PENDING→BATTLE(match)` 到 Hub pending。Locator 的 Match Begin/Bind/Admission 与
   tombstone 同 slot 原子观察，旧局不能在终局后复活。
5. Login 的 `GetResumeContext` 覆盖 HUB/BATTLE/QUEUED/CONFIRMING/ALLOCATING/READY/RUNNING；内部
   match context RPC 使用独立 HMAC+timestamp+nonce，绑定 caller、PVP/PVE audience、exact method 与 player，
   Matchmaker 用共享 Redis SETNX 防跨副本重放，不转发玩家 authorization。
6. UE 只允许 `UMyDsRecoveryCoordinator` 发起 DS `ClientTravel`；本地超时、前后台、network/travel failure、
   session 过期均重新读取权威路由，不能自行推进 placement 或直接回 Hub。
7. Hub→Hub、drain 与 Release 共用 index-first durable owner-cleanup：source exact identity、target Bind
   与 phase 均持久化；assignment CAS/Bind/Departure/phase-clear 的未知结果由重启 reconciler 幂等恢复。
   target ticket、migration push、Admission 在 cleanup 完成前 fail-closed。
8. connected ledger 删除不是旧 Pawn 离场证明。source Hub 只从绑定当前 Model-B credential 的 Heartbeat
   接收 exact eviction order，核对本地 `(player,assignment,admission)` 后 Kick；标准 Logout 发
   `AcknowledgeDeparture` 才完成 physical proof。allocator 的 exact seat release 只回收 reservation；
   live connected owner 返回 `DepartureRequired`。stable shard-member index 同样持久到显式 cleanup，
   防止 30m 长连玩家从 drain 枚举消失。

发布判定从“代码阻断”改为“环境验收阻断”：相关单元/并发/故障注入测试与 UE 编译/Automation 通过后，
仍须跑真实移动端前后台、UDP Admission、Redis/K8s/Agones 故障矩阵。未跑真实矩阵不能宣称生产已验证，
但不得再把 presence/assignment/claim TTL 当恢复机制。对局中的主动离局仍需独立的服务端 terminal/leave
业务授权；客户端主动放弃只能回登录界面，不能绕过 active Battle placement。

#### 7.16.4 Hub 实报在线数不能覆盖尚未入场的 reservation（历史 P1 阻断；2026-07-15 已实施，真实环境待验）

**反例**：`ReserveRoutableSeat` 在 auth+shard 同槽事务内把 `player_count++`，随后 assignment 可保留
30min；但下一次已鉴权心跳会执行 `applyHeartbeatToShard`，用 DS 上报的“当前实际连接数”直接覆盖
`player_count`。若玩家尚未连接，刚占的 reservation 因此从容量账消失；下一批不同玩家又可各自取得
assignment 和有效票据。归属键到期没有对应的原子退座或 reservation ledger，故该过程可跨多轮心跳
重复，风险上界取决于请求速率、票据有效期和 assignment TTL，**不是**“最多一个心跳周期的一次
超额”。最终 admission 虽会核验 assignment/auth，却没有独立的容量 lease，因此不能阻止这些已签票
玩家随后集中连接并把单 Hub 推过容量。

按当前默认容量 500、票据 5min、assignment 30min、UE 心跳 5s，若实际连接数保持 0，理论上可同时
积累约 `500×300/5=30,000` 张未过期票和 `500×1800/5=180,000` 条活 assignment。后端未限制
合法 DS 的最小心跳间隔，故严格机械上界仍只受请求吞吐与 TTL 限制。另须机械证明 Fleet/allocator
capacity 与 UE `GameSession.MaxPlayers` 一致；当前 Fleet/allocator 写 500，而现有打包配置可见
`MaxPlayers=16`，即使 UE 最终以 `Server full` 拒绝，也只是把误分配转化为大规模登录失败，不是容量闭环。

该行为在 legacy 与 Model B 共用的 `applyHeartbeatToShard` 中均存在，不是本次 Redis 权威改造新引入；
但 Model B 的“最终分配门原子核验容量”证明因此不成立，仍属于生产激活阻断，不能以“心跳会自纠正”
降级为纯观测项。

候选：

- **A. 独立 reservation lease ledger（独立推荐）**：同 `{pod}` slot 保存有界、可回收的 reservation
  ledger，身份至少绑定 `(player_id, assignment_id, GameServer UID, auth epoch)`，每项有服务端绝对到期时刻。
  `ReserveRoutableSeat` 在同一 Redis 事务清理已到期项、核验
  `connected_count + live_reservations < capacity` 后创建 reservation；不得再修改 DS 实报计数。
  admission 成功以 admission/assignment 幂等键原子消费 reservation，转入 connected ownership；
  Release/Transfer/assignment CAS 失败按精确 identity 幂等删除 reservation。心跳只更新
  `reported_connected_count` 和存活证明，不能覆盖 ledger。若 connected ownership 与 UE 实报可能漂移，
  须定义以 admission/session ledger 为容量权威、UE 实报只审计，或设计带 session identity 的严格对账；
  不能再次把两个含义压回一个整数。
- **B. 仅拆 `reported_connected_count + reserved_count` 两个整数**：比现状好，但 assignment 到期、响应
  丢失、重复 admission、Transfer 补偿和进程崩溃都可能使 `reserved_count` 漂移；若没有逐 reservation
  identity、TTL 与幂等消费/释放，仍不能形成可证明的容量上限，拒绝作为最终方案。
- **C. 心跳写 `max(old_count, reported_count)`、缩短 TTL 或限流**：会制造永久虚高、无法区分连接与预留，
  且只能降低复现概率，拒绝。

若批准 A，需先决定“容量权威是 session/admission ledger，还是 UE 连接清单的带身份对账”，再改 additive
proto/Redis record 与 login/Hub UE admission ACK。最低测试矩阵必须覆盖：reservation 后心跳 0、跨多轮
心跳累计、票据/assignment 到期、同 player/assignment 并发、Reserve 成功但 assignment CAS 失败、
admission 响应丢失重认、Release/Transfer、UID 重建、quarantine/drain，以及所有失败路径对 ledger/
assignment/票据副作用为零。以上是实施前的候选与验收要求，后续实际状态如下。

**实施状态（2026-07-15，当前工作树）**：已按候选 A 落地逐 identity 容量账本，容量权威选择为
reservation + admission/session ownership，UE 心跳连接数仅作审计，不再拥有删除/覆盖授权。

1. 同 `{pod}` slot 保存 reservation HASH/expiry ZSET 与 connected ownership HASH；reservation 精确绑定
   `(player_id,assignment_id,GameServer UID,auth epoch,writer epoch)` 和服务端绝对到期。整键 retention 覆盖
   最晚单项到期 + guard，不会被较短 shard TTL 提前删除。
2. `ReserveAssignment` 在 auth、shard、teardown proof 与四把 ledger key 的同一 WATCH/MULTI/EXEC 中清理
   已过期 reservation，核验 active credential、heartbeat、ready、drain、`MaxPlayers` 与容量，再幂等创建
   exact reservation。容量满时不产生 assignment/ticket；同 assignment 不同 identity 冲突零变更。
3. `AcknowledgeAdmission` 原子把 reservation 消费成 connected owner；相同 admission/sequence 重放幂等，
   更高 sequence 可接管，迟到 admission/Logout 不能删新 owner。connected 新格式没有时间 TTL，heartbeat
   报 0 也不能推断 Pawn 已离场。
4. `ReleaseAssignmentSeatExact` 只删除未接纳 reservation；live connected owner 返回
   `DepartureRequired`。只有 exact `AcknowledgeDeparture` 或按 immutable GameServer UID 记录的 teardown proof
   才能清 owner。Heartbeat 的 `reported_connected_count`/player list 只更新审计投影，
   `player_count=reserved_count+connected_ownership_count` 始终从 ledger 派生。
5. 路由门要求 `reported_max_players == shard.capacity`；旧连接尚未进入 ledger 且实报人数大于 ownership 时，
   返回 `untracked-connected-players` 阻断新分配。这样迁移期宁可暂时不可用，也不把未知连接当空位。

当前已有单元/并发反例覆盖 reservation 经多次 0-player heartbeat 仍保留、绝对过期回收、Admission ACK
丢失重放、旧 Logout/旧 admission sequence、capacity=1 并发只一赢家、exact release、connected
DepartureRequired、UID teardown 与 MaxPlayers/未跟踪连接门。最终合并快照的命令结果统一记录在
`docs/reviews/ds-recovery-claude-audit.md`；真 Hub UE 连接清单、真实 Redis Cluster 与 K8s UID teardown
仍须发布前故障注入，不能据单元测试宣称生产容量已验证。

### 7.17 Placement physical source-departure 与恢复发布门（2026-07-15 已实施，环境验收待完成）

§7.16.3 的版本化 placement 能阻止旧票准入，但原实现仍存在一个独立反例：`Begin` 已把玩家逻辑目标改为
PENDING，并不证明 source DS 的 PlayerController/Pawn 已经离开。如果 target Admission 只看 version/op，
旧 Pawn 尚存时仍可能形成双物理 owner。本轮把 source departure 变成 locator Commit 之前的第二份、独立
权威证明。

#### 7.17.1 additive 存储与唯一 Commit 门

- locator proto 新增 `ConfirmPlacementSourceDeparture`、proof type `HUB_DEPARTURE=101` /
  `BATTLE_DEPARTURE=102`，以及 `PlayerPlacementStorageRecord` field 24~38。字段只追加，不复用 tag。
- `Begin` 在同一 placement CAS 内保存 `source_placement_version/source_operation_id` 与 source exact target
  `(pod,UID,epoch,assignment|allocation,release_track)`；逻辑 terminal proof 只授权发起 transition，不能替代
  物理 source departure。
- 非 account-bootstrap PENDING 的 `CommitPlacementAdmission` 必须看到当前 exact
  `(player,version,operation,target route/match,source binding,source target)` 对应的 active confirmation。
  source 缺失、确认类型错、proof id 空、Retarget 后旧确认或任何 tuple 漂移全部 fail-closed。
- Commit 成功把 proof identity 复制到 `last_source_departure_*` 仅供审计，再清活动 source；下一次 Begin/
  Retarget 重新捕获 source 并清确认，旧 proof 不能跨 version 复用。

#### 7.17.2 两个物理 authority 与 ACK-loss 语义

- **Hub source**：Hub allocator 从持久 assignment cleanup journal 取得 expected source；只有逐 assignment
  reservation/connected ledger 已证明 exact owner absent，或 immutable GameServer UID teardown 已确认，
  才生成稳定 `hub-departure:<sha256 exact tuple>`。它重读 locator 当前 source、再次比对 ledger 后签名确认。
  Redis seat 删除本身不是 proof，live connected owner 必须等待 UE Logout Departure ACK。
- **Battle source**：ds_allocator 先核 locator 正处于 exact PENDING Hub，再由 durable departure journal 下发
  exact eviction。Battle DS 的完整 world census 必须证明 exact owner 不再出现在 pending admission、admitted
  claims、Controller、Pawn weak ledger 或 World；之后以稳定 `departure_id` 用独立 Battle authority 签名确认。
- 两条路径都把 locator confirmation 视为成功链的组成部分。物理 journal 已提交但 locator ACK 丢失时，
  调用方仍收到 retryable；重试使用同一 proof id，locator exact-CAS 幂等返回。绝不能先给 target ticket/
  READY/Admission，再异步补确认。

#### 7.17.3 五把 placement authority key 的启动硬门

生产配置包含五把相互独立的 authority secret：`account-bootstrap`、`match-start`、`battle-exit`、
`hub-transfer`、`battle-departure`。Hub logical transfer 与 Hub physical departure 共用 Hub owner 的一把 key，
但 HMAC canonical domain 分离；Hub allocator永远不取得 Battle departure key。player-locator 启动时机械要求：

1. 五把 key 全部存在且每把至少 32 bytes；
2. 五把 pairwise 不同；
3. 任一 placement key 不得复用 DS callback secret；
4. 环境变量显式值优先，坏值不能静默回退配置文件。

任一条件失败 locator 直接退出，不以“空 keyring 导致所有 transition 卡住”的方式带病运行。集群配置生成器
与 online manifest contract 已加入 BattleDeparture binding：该 key 只投递给 ds_allocator 与 player-locator；
Hub departure 使用既有 HubTransfer authority，并靠独立 canonical domain 防类型混用。

#### 7.17.4 旧 writer 与遗留记录迁移

旧 locator Commit 不认识 source-departure gate；若与新副本滚动重叠，会绕过新不变量。因此本协议迁移期间
player-locator Deployment 使用 `strategy.type=Recreate`，明确接受短时不可用来换取 writer 集合唯一，不能
使用默认 RollingUpdate。online prod 的实际 serving writer 是 `player-locator-ds-auth-green`，所以普通发布的
CAS replacement object 也必须机械改成 Recreate，并注入与主容器**同一 immutable digest** 的 preflight
initContainer；只改 dormant blue `player-locator` 等于没有门。发布终态会回读 green 再核 strategy/image/args/
ENTRYPOINT/只读配置挂载。

Recreate 前必须运行只读 `services/runtime/player_locator/cmd/placement_preflight`。它对 Redis Cluster 每个
master 执行 SCAN，逐条验证：STABLE 有完整 exact current target 且无活动 source marker；bootstrap PENDING
形状严格合法；其他 PENDING 有完整 immutable source、与 current route 一致的 exact source target，且活动
departure marker 要么全空（合法 Begin-before-Confirm），要么是 type/id 与 physical source 完全匹配的已确认
proof。initContainer 在每次 Pod restart 都会执行，因此必须放行前一种 canonical 未确认态，让新 locator 启动后
继续接收 Confirm；partial marker 才是 malformed。坏 key shape、protobuf 损坏、扫描/GET 错误或 key 在审计中
消失都非零退出；工具不写 Redis。
STABLE 的 `last_source_departure_*` 是 audit-only：pre-source-departure 的 legacy STABLE 四字段全空仍可作为下一次
Begin 的安全 exact source，必须允许启动；只要四字段部分存在，或完整 tuple 的 version/op/type/id 不匹配当前
committed transition，才判 malformed。旧 admission/proof/timestamp 缺失本身也不能取代 exact current target
成为永久启动门。
审计失败必须先让原 operation 正常完成/补偿，不能手工制造 confirmation。

#### 7.17.5 durable worker/index/release 的新增反例修复

- ds_allocator 的 active ZSET 是派生索引。新 reconciler 在启动立即、之后周期性扫描 Redis Cluster **每个
  master** 的 canonical `pandora:ds:battle:{*}`；`ZADD NX` 只补丢失成员，不覆盖 score=0 的立即恢复唤醒。
  allocation-uncertain/release-pending/empty-tombstone/preactive-release 与永久 abandoned handoff 会被重建；
  ended 和已有 TTL 的已完成 audit 不会复活；未知状态/坏 protobuf fail-closed。
- Matchmaker auto-confirm/solo 不再直接创建 ALLOCATING：先持久 fully-accepted CONFIRM，验证 ticket union
  精确等于 canonical roster 且所有 player claim durable 后才 CAS ALLOCATING。allocation worker 外调前再次
  执行 exact discovery；`ERR_UNAVAILABLE` 保持未知结果并重试，不改成 FAILED/requeue。
- Release 对 ticket 与 player claim 使用 exact compare-delete。canonical match 已缺失时，fallback roster
  只有在 claim→ticket、ticket.match_id、ticket member 三条边均可证明时才清理；缺 ticket/损坏/歧义返回
  unavailable，不能误删玩家已建立的新局 claim。旧 ticket record 已删但派生 queue ZSET 的前次 `ZREM`
  失败时，幂等 `DeleteTicketIfMatch` 仍清 stale member。
- BattleResult 的 match-release outbox 对 downstream commit-but-ACK-lost 保留行并重放；幂等结果 replay 会
  恢复缺失 outbox，或把未来 `next_attempt_at` 拉回立即 due，同时保持 operation identity 与 attempt count。

#### 7.17.6 UE 单写者与物理 census

`UMyDsRecoveryCoordinator` 仍是唯一 DS `ClientTravel` writer；Account/Match model 只提交 intent/权威结果。
回调同时核 generation、request sequence、expected phase；foreground、World BeginPlay、network/travel failure
使旧 generation 失效并重查 `ResumeContext`。30s 只改变 UI/退避，不改变 route。

Battle DS 另外加入 World 级物理 census 与 exact eviction：任何没有完整 v2 claims 的 Controller/Pawn、Pawn
owner 不一致或 weak Pawn ledger tuple 漂移，都让 census 整体 fail-closed；Logout map 清理早于 Pawn destroy
时仍报告 active。eviction 先完整扫描 ABA，再取消 pending、Kick Controller 或 Destroy 无 Controller Pawn；
旧 placement version/operation/UID/allocation 不能作用于新 admission。该静态/Automation 证明仍不能替代
真 Dedicated Server 上的网络断开、GC/Destroy 时序和长后台测试。

#### 7.17.7 发布与声明边界

严格发布顺序见 `docs/reviews/ds-recovery-claude-audit.md`。最低顺序不变量是：先发布 additive reader 与新
DS census/eviction 能力并排空旧 Hub/Battle DS；再停住旧 locator writer、全 master preflight 通过后以
Recreate 启动新 locator；之后才开启 strict source-departure/ticket/Admission gate并灰度客户端 Coordinator。
Match durable worker/outbox 的 writer ownership 也必须唯一，不能新旧推进器同时写同一 operation。

当前工作树和本地测试能审查“失败不会被解释为成功”的代码路径，不能证明真实 Redis Cluster、K8s/Agones、
UDP Admission、移动端 OS suspend 或生产 Secret 投递已经通过。最终可接受结论仅为：**任意故障后要么只有
一个权威 DS 可准入，要么停在明确 retryable/登录态；不得双 Admission，不得静默等 TTL。** 在真环境矩阵
完成前，不得宣称“生产任意时间点已无卡死”。

### 7.18 Allocation abort 与 Model-B 回收证明（2026-07-15 已实施，升级证据待环境验收）

Matchmaker 的 allocation phase 新增 additive `ABORTING`。发生 placement 竞争、取消或 READY 不再允许时，
唯一合法顺序为：

```text
REQUESTING(exact operation + target)
  --CAS--> ABORTING(same operation + target)
  --AbortPreactiveBattle durable/physical confirmation--> FAILED
```

READY CAS 只接受 REQUESTING，并在 Placement、Hub Departure 和签 Battle ticket 之前各自重新检查 exact
phase/op/target。`AbortPreactiveBattle` 用独立 `PANDORA_ALLOCATION_ABORT_AUTH_SECRET`，HMAC canonical 使用
长度前缀绑定全 payload，metadata digest 不可降级，nonce 在 payload 比对后才消费。生产启动拒绝
缺失、短值、控制字符、公开 dev 值，也拒绝与 DS callback、login resume 及五把 placement authority
密钥复用。

Allocator 对 abort 使用持久 full-target journal/fence。物理回收完成后写 teardown proof；ABANDONED
消息被 Kafka 确认后写 full-target lifecycle marker。旧 abort RPC 响应丢失或服务重启时，只有两份证明
同时精确匹配才能合成已完成；任意一份缺失均保留 retryable ABORTING。`RELEASED` 只表示 K8s
物理回收已被不可变身份证明，不表示 Redis auth/battle cleanup ACK 已收到；Complete 重放会重读
剩余 authority，仅 exact 匹配才恢复 retention/移出 active，任何 ABA 漂移都拒绝。

新 Model-B allocation 在业务层和 Redis finalize 层都必须带真实 Pod UID。存量空 `pod_uid` 只能从
exact GameServer UID+allocation_id 与 owned Pod 读回，然后 KEEPTTL CAS 持久并重读验证。正常结算、
empty timeout、stale sweep、abort 与 preactive crash 都在 terminal/permanent transition 前做该预检。K8s
缺失、重建或 GET 未知时零 terminal/删除副作用，并保留 active/fence 重试。

pre-Prepare 时崩溃的 allocation 尚无 credential epoch，即 `instance_epoch=0`。它不能伪造 epoch 去通过
通用 player-battle release；专用 `releaseFencedPreactiveGameServer` 只在 exact preactive fence 已持久后，
使用真实 GameServer UID + Pod UID + allocation_id 执行 UID-conditioned release。unknown 保留永久 fence，
明确成功才 purge，不写玩家 teardown proof。

该升级策略的安全边界是：对空 `pod_uid` 的历史 active record，如果对应 K8s 不可变对象在升级前
已消失，新 writer 不猜 UID，因而会停在明确可重试 fence。这保证不误删/不双准入，但不能写成
自动收敛。生产发布前必须完成这类记录的迁移审计、证据保留/安全处置并清零；该记录未清零时
严格门不可放量。

迁移审计已机械接入 DS auth activation，而不是文档约定。新 ds_allocator 的 one-shot
`-pod-uid-release-preflight` 在任何 repo/worker/listener 创建前退出，使用独立高熵只读 ACL 身份和
credential-free immutable 配置。Cluster 必须证明 exact 16384-slot master 拓扑并在每个 master 做
PING/SCAN/GET；standalone 必须证明非 Cluster 且为 master；Sentinel/Cluster membership、ACL、配置或
同 key 身份有任何漂移都 fail-closed。产线激活需要同 RunId、同 green immutable image/config 与同 Redis
config/target identity 的三份不可变 Job：

```text
epoch=1 additive writer 先回填可证明的旧记录
→ prepare preflight PASS
→ activation lock / namespace fence
→ blue writer=0 + capability empty + drain marker
→ drained preflight creation/completion >= drain marker
→ green scale + exact capability/strict writer audit
→ final preflight PASS（Service 仍全部指向 blue）
→ immutable evidence + cleanup-required marker
→ Service switch / final synthetic
→ required_writer_epoch 1→2 CAS
→ 独立 Redis 控制身份 DELUSER 临时只读用户 + WHOAMI/GETUSER absent 双遍回读
→ immutable cleanup-complete marker
```

只执行 prepare 存在 TOCTOU：它通过后旧 writer 仍可写新旧格式 record，所以不能作线性化证据。
drained Job 发生在旧 writer 完全归零后；final Job 又发生在新 digest 的 exact capability 与 strict writer
启动后，能反证新进程是否仍有绕过写点。新 allocation 的 usecase+finalize 双门与 `BattleStorageRecord`
全部 23 个生产 content write 的不可逆 mutation gate 共同关闭 Model-A 回写窗口：新/重写必须带 PodUID，
legacy 只允许 exact backfill，PodUID 不可变且未知 protobuf bytes 保留。epoch=2 的 Audit/幂等路径只能验证
原 RunId 下已完成的 prepare/drained/final Job、exact PASSED 日志和 cleanup-complete，缺失时直接拒绝，
禁止激活后创建追认证据。

required writer 状态同时升级为固定 byte-level policy fence：target 只能是
`2@ds-auth-v2-pod-uid-write-invariant-v1`，并绑定五个生产 writer 的 exact feature 集合。初读、capability
注册事务、watch 和 activation record 都比较同一 raw value/policy；旧 numeric epoch=2 binary 不能解析，
新 binary 也拒绝裸 `2`，因此旧 writer 不能借 feature-only rollback 重入。

但是当前仓库没有可执行 Redis topology-change/failover/reshard lease provider 或其信任根，无法排除
SCAN 窗口中 master/slot 原子迁移后又恢复的 ABA。ConfigMap/一次 topology digest 不是执行锁。为防假绿，
fresh/retry Activate 在任何 create/patch/scale 前阻断，CAS 前二次阻断；Go CLI/core CAS 和普通 online
build/push/apply 也独立 fail-closed。该缺口属于外部控制面能力尚未接线的发布硬阻断，不是可标为
`BLOCKED-ENV` 后跳过的测试；真实 provider 接入并完成 Redis/K8s/Agones 故障注入前不得启用 epoch=2。

## 8. 2026-08-13:player 入列(GetLoadout 上 DS 面)与灰度启用策略

### 8.1 为什么 player 必须入列

`player.GetLoadout` 这天被挂到 Envoy DS 面(`:8444`)的精确路由上,供 Hub/Battle DS 在
`PlayerState` 初始化时拉专精与预设装备。该监听器**没有 `jwt_authn`**,经它进来的请求
`callerID` 恒为 0;而 `resolvePlayerID` 的既有语义是「`callerID==0` ⇒ 后端内部可信 ⇒ 信请求体
`player_id`」。两者相乘的结果是:**任何能连到 `:8444`、或绕过 Envoy 直连业务端口 20002 的进程,
都能读任意玩家的出战快照**(装备实例 ID、词条、天赋)。`systemOnly` 在这里帮不上忙 ——
它挡的是「带玩家 JWT 的客户端」,而这里的问题恰恰是「什么都不带」。

补法即本文件 §2 的既有机制:handler 顶部过 `DSCallbackGuard`,`scope` 取
`{RequireToken: true}`,不绑 `Type` / `Pod` / `MatchID`。

- `RequireToken`:全仓 grep 确认 `GetLoadout` **没有**任何内部 Go 调用方,因此「无标记无令牌的
  东西向调用」不存在合法形态,直连业务端口也一并拒 —— 这正是堵住 20002 旁路的那一半。
- 不绑类型:Hub 与 Battle DS 都在进场时查这一次,与哪台 DS、哪一局无关。这与 UE 侧既有契约一致 ——
  `IsPlayerLoadoutMethod` 早已归在 `hub|battle` 双类型 + `Active` 凭据分支里,**DS 本来就带着
  Bearer 令牌发这条请求**,服务端只是此前没验。切 enforce 不需要 UE 侧任何改动。
- 只在 `callerID==0` 时过门:客户端自助查自己的出战快照走 `:8443`,其 `authorization` 头里是
  **玩家 JWT**,拿去 `VerifyDSCallback` 必然验签失败。无条件过门会把整个客户端出战面板拒成
  `ERR_UNAUTHORIZED`,这条有专门的回归用例钉住。

### 8.2 三处同增同减

player 是 **verify-only**(只验签,不签发;签发方仍只有 `ds_allocator` / `hub_allocator`),
但持有校验材料同样扩大了 DS 回调密钥的分发面,因此按既有纪律登记:

| 位置 | 内容 |
|---|---|
| `services/account/player/etc/player-dev.yaml` | 新增 `ds_auth` 节点(生成器双向断言:节点存在 ⟺ 在清单里) |
| `services/account/player/etc/player-prod.yaml.example` | 同上,生产底稿 `mode: "enforce"` |
| `tools/scripts/gen_cluster_config.ps1` | `$DsSecretServiceNames` += `'player'` |
| `tools/scripts/lib/online_manifest_contract.ps1` | `$PandoraDsCallbackHmacServices` += `'player'` |

四处漏改任意一处,`gen_cluster_config.ps1` 会以
`[FATAL] player 的 ds_auth 节点与权威服务清单不一致` 拒绝生成,或发布契约以
`DS callback HMAC 连续性检查缺 player 配置` 拒绝发布。`ds_auth_conf_test.go` 把这条一致性
提前到 `go test`,不必等到真实配置生成才炸。

### 8.3 档位与启用顺序

| 环境 | 档位 | 来源 | 理由 |
|---|---|---|---|
| local / dev | `permissive` | `player-dev.yaml` 直接写死 | 跑与 enforce **完全相同**的验签 + 范围校验路径,失败只打 warn 仍放行 —— 不改变任何现有行为(§14.2,本地联调与一键启动不会被这次加固弄坏),同时把「令牌到底有没有到、验不验得过」变成可查证据 |
| prod / agones | `enforce` | `gen_cluster_config.ps1 -DsAuthMode enforce` 机械注入 | 已实测:B1 档生成的 `player.yaml` 得到 `mode: enforce` + `authority_mode: redis` + 完整 fence,与其余四个 DS 密钥服务逐字同款 |

**dev 刻意不写 `off`。** 写 off 就退回「执行点在位但恒放行」的纸面门 —— 那正是 2026-08-13
评审打回的状态:代码可编译、测试全绿、漏洞照旧。permissive 是能同时满足「不破坏现有行为」与
「持续产出真实证据」的唯一档位。

**切 enforce 的判据**:dev 跑一局双人 PVE,player 日志里**没有**任何
`ds_callback_auth` 相关 warn(令牌缺失 / 验签失败 / 范围不匹配),即证明 DS 侧令牌链路完整。
判据不成立就先查 UE 的 `EPandoraDSCredentialUse::Active` 凭据是否完整,**不要**先降档位。

**回滚**:把生成参数 `-DsAuthMode` 退回 `permissive` 即可,不需要改代码、不需要重出 DS 包;
player 只验不签,退档不影响任何其它服务的令牌签发。

`authority_mode: redis` 与 `fence` 会被生成器一并注入 player.yaml,但 player **不读**它们
(只有 `hub_allocator` 读;player 侧只调 `NewDSCallbackGuardFromConf`,仅消费 `mode` / `secret` /
`additional_secrets`)。属惰性字段,不构成启动依赖。

### 8.4 剩余风险(本次未覆盖)

`team.GetPlayerTeam` 与 `guild.GetPlayerGuild` 同样挂在 DS 面、同样已接 `DSCallbackGuard`,
但这两个服务**至今不在上述两份权威清单里** —— 它们的 `ds_auth.mode` 恒为 off、守卫恒 nil,
那两道门目前仍是纸面门。泄露面小于 loadout(只有队伍 / 公会编号),但性质与本条完全相同。
要闭合需按 §8.2 同样的四处登记把 team / guild 也纳入,属独立决策,本次未做。

### 8.5 ⚠️ 首次 online 发布会被连续性门挡下(必须先走换钥流程)

`Assert-PandoraOnlineHmacContinuity` 会对**线上现存配置**和候选配置各算一次 DS callback keyset
基线,而基线是按 `$PandoraDsCallbackHmacServices` **逐个服务**算的。player 刚进这份清单,
但线上正在跑的 `player.yaml` 是本次改动之前生成的、**没有 `ds_auth` 节点** ——
于是算 live 基线时就会在 player 上抛错(`player.ds_auth 是空节点` / `缺 player 配置`),
发布在 push/apply 之前被拒。

这不是 bug,是这套门禁的既定语义:**DS callback 密钥的分发面变化必须走独立换钥流程,
不能混在普通 online 发布里**。往一个域里新增持钥服务与轮换密钥同属该类事件。

因此上线顺序是:

1. 先按换钥流程把 player 纳入 DS callback 域(让线上 `player.yaml` 先带上 `ds_auth` 节点,
   secret 与现网 DS callback 主钥同值 —— 这一步不改任何密钥值,只是把同一把钥匙多发一份);
2. 确认线上 player.yaml 已有 `ds_auth` 后,后续普通 online 发布的连续性门即自然通过;
3. 再按 §8.3 的判据从 permissive 推到 enforce。

把 1 和 3 合成一次发布会同时触发「分发面变化」与「档位变化」,出问题时无法二分定位,
不要这么做。
