# decision-revisit:Hub DS 进场票据能否取消(2026-07-29 复核)

> 状态:**已复核,维持现状**(2026-07-29 提出「Hub 进场不要票据,登录验证过就能进」的简化设想,
> 逐条论证后否决;本次复核**代码零改动**,只补本文档与 CLAUDE.md §9 不变量 3 的表述)。
> 关联:`pkg/auth/dsticket.go`(DSTicket v2 签发 / 验签本体)、
> `pkg/auth/dsticket_conf.go`(三个签发点共享的加载与校验)、
> `services/battle/hub_allocator/internal/biz/hub.go`(Hub 签票权威 + 分片容量)、
> `services/account/login/internal/biz/login.go`(query-first placement + 三态战斗门)、
> `proto/pandora/hub/v1/allocator.proto`、
> UE 侧 `Private/Auth/PandoraDSTicket.cpp`、`Private/Gameplay/Default/PandoraDSGameModeBase.cpp`、
> `Private/Gameplay/Default/PandoraHubGameMode.cpp`、
> `docs/design/decision-revisit-player-jwt-key-rotation.md` §7(2026-07-13 方案 B 拍板)、
> CLAUDE.md §9 不变量 1 / 3 / 6 / 21 / 22 / 23、§15 设计简单性、
> 工作区 `F:\work\CLAUDE.md`「验收底线七条」。

## 1. 被提出的简化设想

> 「Hub DS 进入不需要 ticket 可以吗?只要我登录验证过了进去了,不在战斗,只在一个 DS 就行。
> 这样会不会简化一点、简单一点,这样不会卡玩家?」

动机拆成三条,逐条复核:

1. **少一套机制**(简单)——删掉 RS256 密钥体系、JWKS 投递、kid 轮换、JTI replay cache;
2. **少一个失败点**(不卡)——票据签发 / 验签失败不再是进场链上的一环;
3. **前置条件看起来已经够**——"登录过 + 不在战斗 + 只在一个 DS"三条成立即可放行。

## 2. 结论

**否决,维持现状。** 三条动机全部不成立,且第 3 条把最贵的保证误当成了免费前提。

1. **票据不是一套独立鉴权机制,而是把已经算好的权威判定结果搬运到 DS 的唯一不可伪造通道。**
   删掉通道并不减少要搬的东西:`player_id` / `role_id` / `hub_assignment_id` / exact 实例三元组 /
   `release_track` / `owner_epoch` 语义,DS 一样都不能少。
2. **唯一自洽的替代是 DS 每次 PreLogin 同步查后端** —— 即代码里已存在的
   `EPandoraDSTicketVerifierProfile::OnlineAuthority` 档。那是把「离线可验的短时签名」换成
   「进场路径上的在途同步依赖」,故障点更多、延迟更长、更容易卡。按 §15.2(能同步不异步、
   能复用不新增)与 §15.4(复杂度必须举证),它比现状**更复杂**,不是更简单。
3. **全部已归档卡死事故的根因没有一次在票据**(见 §5)。删票不降低卡的概率,只减少一层
   fail-closed,反而更容易出「进错 DS / 双 DS」这类比卡死更严重的问题
   (验收底线第 3 条:宁可 fail-closed 拒一次,也不要写出一份不自洽的数据)。
4. **「只在一个 DS」不是前提,是结论。** 它由 §9.22 的每玩家 `owner_epoch` 线性一致 owner
   authority + 短 owner lease fencing + Admission 交接屏障保证;票据是把该结论带到 DS 的信封。
   把信封扔掉,结论并不会自己到达 DS。

## 3. 票据实际承载什么(逐 claim → 消费点 → 取消后的后果)

claims 定义见 `pkg/auth/dsticket.go` `DSTicketClaimsV2`(§L74)。

| claim | 消费点 | 取消后 |
|---|---|---|
| `sub`(player_id)/ `role_id` | UE `GetTrustedPlayerId` / `GetTrustedDSTicketClaims`;`PandoraHubGameMode::PostLogin` 注释原文:**"不读取客户端自报字段"** | 客户端连 DS 时只能自报 `?player_id=123`。大厅是全图自由 PvP 且 DS 是背包受信写者(§9.6 五要件①身份),身份可伪造即可冒充任意玩家并写其权威数据 |
| `hub_assignment_id` | 玩家归属版本。`allocator.proto` 原文:Transfer / Release / 同名 Pod 重建后**旧票因 assignment_id 不再匹配而失效** | 500 人/实例的容量判定(`hub.go` `PlayerCount >= Capacity`)失去执行手段;玩家可自行挑分片连接,也能连上正在 draining 的分片;`TransferHub` 强制整合迁移没有生效手段 |
| `ds_pod` / `ds_uid` / `ds_instance_epoch` | `ValidateV2InstanceBindingAndGates` exact 实例绑定 | 可连上**同名重建但已换代**的 Pod。这正是 §9.22 要求 exact DS identity 而非 Pod 名的原因 |
| `release_track` | `FPandoraDSTicketV2LocalBindingPolicy::ValidateReleaseTrack` | 违反 §9.21 / 验收底线第 7 条:同一玩家无法固定 Stable / Canary 轨道,灰度失效,异常时无法把 Canary 权重归零 |
| `jti` + 短 exp | DS 侧一次性消费 cache(`FPandoraDSTicketReplayCache`,§9.18 有界纪律) | 这是 B1 纯本地验票下**唯一的吊销手段**(吊销时延上界 = TTL + leeway)。没有票即没有任何吊销窗口:封禁 / 顶号后玩家照样连着 DS |
| `sjti`(签发方会话 jti) | §9.23 会话 fencing;`AcknowledgeAdmission` 复核现行性 | 顶号后旧会话仍能进 DS,违反 §9.23「旧 session 不能再签 DS 票」 |
| `source_match_id` | Battle→Hub 回流 fence,过 locator 的 BATTLE→HUB guard | 终局 TTL 残留会重新导致 4007「玩家正在战斗中」 |

DS 侧的门是 fail-closed 的:`PandoraHubGameMode::PostLogin` 在缺可信 player / assignment / claims 时
**先加 spawn gate、再 Kick,且不生成 Pawn**;`ShouldEnforceTicketForNetMode` 对 `NM_DedicatedServer`
**恒返回 true**(不可配置关闭)。取消票据等于把这道门整体拆掉。

## 4. 取消后的失效矩阵

| 约束 | 失效方式 |
|---|---|
| 验收底线 3(数据完整性) | 身份可伪造 → §9.6 五要件①身份 / ②owner 授权 / ③fencing 同时断裂,DS 可写出别人的背包 |
| 验收底线 7 / §9.21(金丝雀) | `release_track` 与 exact 实例绑定消失,共存窗口内玩家无法钉在一条轨道 |
| §9.1 / §9.22(一人一 DS) | owner 判定结果无法可信抵达 DS;DS 只能自查,回到跨存储「先查后写」TOCTOU |
| §9.3(票据短时效) | 条款本身消失,吊销能力归零 |
| §15.2(最少复杂度) | 表面删掉一套机制,实则新增每次进场一次同步 RPC 依赖,净复杂度上升 |

## 5. 「不卡玩家」动机的事实核查

复核全部已归档 P0,**没有一次**根因落在票据:

| 事故 | 真实根因 | 与票据的关系 |
|---|---|---|
| INC-20260724-001(`2026-07-24-p0-matchmaker-orphan-start-claim-freeze.md`) | Battle DS 分配不可用(fleet churn → ready=0 + k8s / Agones 控制面 context deadline);客户端恢复循环重复发 StartMatch 自撞 4002 | 无 |
| INC-20260727-001(`2026-07-27-p0-ds-allocator-warming-coldload-reclaim.md`) | Artic01 冷加载期 warming DS 被单阈值 sweep 误回收;BeginPlay 过早宣告 running | 无 |
| INC-20260727-002(`2026-07-27-p0-battle-ds-artic01-memcg-oom.md`) | Battle DS anon 内存顶死旧 limits 2Gi 被 memcg OOM | 无 |
| 松林镇 4002(2026-07-27) | 镜像关卡表漂移(7 → Artic01)→ `BlockTillLevelStreamingCompleted` 死锁 → 零心跳 → 退队循环 | 无 |
| ACK 等待静默失效 | deadline 挂在会被 generation 验身作废的 `TravelWatchdog` 上,换代后约 1 秒即失效 | 无 |

卡的来源集中在**分配 / 容量 / Admission 有界驱动 / 关卡表一致性**四处。

反证方向同样成立:签票在进场链上是**零副作用可重签**的一步 —— §9.23 明文
「单次票据可以安全重签新 JTI,但票据本身不是幂等键」。它既不占座、不推进 owner_epoch,
也不是幂等键,失败时客户端退避重查权威即可,不构成无出口等待。**票据不在卡死的因果链上。**

## 6. 真正可用的简化口:已存在的三档 verifier profile

`FPandoraDSTicketVerifierProfilePolicy::Select` 已经提供三档,**"不做本地验票"不是新设计**:

| profile | 用途 | 代价 |
|---|---|---|
| `RS256Local` | 生产。DS 只持公钥 JWKS,永不持私钥 | 需投递 JWKS |
| `HS256LocalOff` | dev / 内网联调。省掉整套 JWKS 投递 | 仅限 local-off 档,`FPandoraDSTicketSecretPolicy::Resolve` 对非 local-off 硬拒 |
| `OnlineAuthority` | DS 在线核销 | 进场路径新增同步依赖 |

因此:

- 若目标是**开发期少折腾密钥** → 内网 / dev 切 `local-off` 档,一行配置,架构不动。这是当初就留好的口子。
- 若目标是**少卡** → 该动的是 hub / battle DS 的分配容量,与 `UMyDsRecoveryCoordinator` 各阶段
  deadline(挂同一个共享 watchdog、用不随 generation 失效的绝对时间戳),与票据无关。
- **生产档保持 `RS256Local` 不变。**

## 7. 重开此议题的举证门槛

按 §15.4「复杂度必须举证」的对称要求,**删机制同样要举证**。再次提出取消 Hub 票据时,必须先给出:

1. `player_id` / `role_id` / `hub_assignment_id` / exact 实例三元组 / `release_track` 的**替代不可伪造通道**,
   且该通道不得是「客户端自报 + DS 信任」;
2. 替代方案下的**吊销时延上界**测算(现状 = TTL + leeway ≤ 195s);
3. 替代方案在 §9.21 共存窗口(Stable↔Canary、旧 DS↔新 Go、新 DS↔旧 Go)的验证结论;
4. 证明新增的同步依赖满足 §9.19 有界驱动,且不引入新的无出口等待。

四条缺一不可。仅以"看起来更简单"为由的取消提案,按本文档直接驳回。

## 8. 顺带订正:票据 TTL 口径

CLAUDE.md §9 不变量 3 历史表述为 `JWT exp 5min`,该数值**只对 legacy HS256 路径成立**。
生产 v2 RS256 的真实口径:

- `DSTicketDefaultTTL = 2 * time.Minute`、`DSTicketMaxTTL = 3 * time.Minute`(`pkg/auth/dsticket.go`);
- 签发侧 `NewDSTicketSigner` 对 `ttl > DSTicketMaxTTL` **启动即拒**;
- 验签侧 `Verify` 对 `exp - iat > DSTicketMaxTTL` 同样拒(双向防「长票 + 本地验签 = 吊销失效」);
- `login-dev.yaml` 的 `ds_ticket_ttl: "5m"` 喂的是 legacy signer
  (`hub_allocator/cmd/hub_allocator/main.go` 日志名 `hub_ticket_legacy_signer_ready` 可证),
  v2 TTL 走 `config.DSTicketConf.TTL` 经 `NewDSTicketSignerFromConf` 注入;
- `login/internal/conf/conf.go` 注释已记录该分野:「v2 RS256 上限 180s;legacy HS256 为 ds_ticket_ttl,默认 5min」。

差异达 2.8 倍,而「吊销时延上界 = TTL + DS 侧 leeway」是选择 B1 纯本地验票时明确接受的取舍论据 ——
按 5min 计算安全窗口会得出偏保守的错误结论。CLAUDE.md §9 不变量 3 已按本节订正。
