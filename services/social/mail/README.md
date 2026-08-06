# mail

> 游戏内邮件服务:系统 / 公会 / 个人三类邮件统一收件箱视图,附件领取走 inventory 幂等入账,
> 各表增长有界(TTL + 周期 sweep)。系统 / 公会邮件 = channel + watermark 拉取(零写扩散),
> 个人邮件 = 写扩散(离线可达)。
>
> 本 README 是**模块级说明**(职责 / RPC / 存储 / 调用链 / 起动)。**设计判断 / 决策记录**见 `docs/design`
> 的 [`mail.md`](../../../docs/design/mail.md);附件领取三段式见 [`bag-domain.md §7`](../../../docs/design/bag-domain.md)、
> [`decision-revisit-bag-replay-semantics.md`](../../../docs/design/decision-revisit-bag-replay-semantics.md);
> 分页上限见 [`decision-revisit-list-pagination.md`](../../../docs/design/decision-revisit-list-pagination.md)。
>
> 代码行号锚点截至当前 HEAD,以**函数名**为准(行号会随改动漂移)。

## 职责与边界

- **职责**:三类邮件的收发 / 拉取 / 已读 / 领取 / 删除;附件领取幂等交付到 inventory;过期数据周期回收。
- **权威态**:邮件本体 + 领取记录全在 **MySQL**(`pandora_social` 库),进程内无缓存,无 leader,水平可扩。
- **两种投递模型**:
  - **系统 / 公会邮件**:全服(或全公会)只存**一行**,靠 `player_mail_cursor` 的 watermark 游标拉取,
    僵尸 / 退游玩家不登录即零成本(不写扩散)。
  - **个人邮件**:发送时直接**写扩散**进收件人 `player_mail`,离线可达,上线拉取。
- **不做的事**:不算掉落 / 数值 / 派生结果(那是 battle_result 的活,§9.6);**不依赖 kafka**
  (拉取式,个人邮件落库即达,红点推送复用运营侧 `system.notify`);附件不在 mail 内落袋——
  统一委托 inventory 服务按幂等键入账(资产不变量 §7)。

## 端口(`docs/design/infra.md`)

| 协议 | 端口 | 用途 |
|---|---|---|
| gRPC | `:20009` | 客户端 RPC(经 Envoy)+ 内网运营 / DS RPC |
| HTTP | `:21009` | 仅 `/metrics`(`mail.proto` 无 `google.api.http` 注解,无 RESTful RPC) |

端口默认值在 `internal/conf/conf.go` 的 `Defaults()`(`Server.Grpc.Addr` / `Server.Http.Addr`)。

## 对外接口

代码入口:`internal/service/mail.go`(gRPC service 层)。玩家 RPC 从 JWT ctx 取 `player_id`
(`callerID`,`service/mail.go:163`),`0` 直接返 `ERR_UNAUTHORIZED`,不认 request 里的 id(防伪造);
另经 `pmw.SessionCurrent` 校验请求 jti == login 会话权威当前一代(顶号后旧 JWT 立即失效)。

| RPC | 调用方 | 语义 | 鉴权 |
|---|---|---|---|
| `ListMail(cursor, limit)` | 客户端 | 拉收件箱:分页个人邮件 + 首页拼系统 / 公会 watermark 增量 | JWT `player_id` + 会话门 |
| `ReadMail(mail_id)` | 客户端 | 个人邮件置已读(系统 / 公会靠游标,幂等) | JWT `player_id` + 会话门 |
| `ClaimMail(mail_id)` | 客户端 | 领附件(直连链,inventory 幂等),返回实发清单 | JWT `player_id` + 会话门 |
| `DeleteMail(mail_id)` | 客户端 | 删个人邮件(系统 / 公会不可删,游标天然跳过) | JWT `player_id` + 会话门 |
| `SendSystemMail(...)` | 运营 / 内网 | 系统邮件插一行(零写扩散);transfer 附件拒 | **拒玩家 JWT**(`callerID` 必须 =0) |
| `SendGuildMail(guild_id, ...)` | 运营 / 内网 | 公会邮件插一行;transfer 附件拒 | **拒玩家 JWT**(`callerID` 必须 =0) |
| `SendPersonalMail(to_player_id, ...)` | 运营 / battle_result(掉落背包满转邮件) | 定点发个人邮件(离线可达),可携 transfer 托管附件 | **拒玩家 JWT**(`callerID` 必须 =0) |
| `GetClaimableAttachments(player_id, mail_id)` | owner DS(bag phase 2) | 取 / 幂等重取领取意图(展开 `BagItem` + `claim_key`) | **拒玩家 JWT**(`callerID` 必须 =0) |
| `MarkMailClaimed(player_id, mail_id)` | owner DS(bag phase 2) | journal ACK 后终结领取(消托管行 + 置终态,幂等) | **拒玩家 JWT**(`callerID` 必须 =0) |

> **内网专用 RPC 的鉴权**:后 5 个是**系统接口**(`systemOnly`,`service/mail.go:171`)——合法调用者是后端
> 内部直连(运营工具 / battle_result / owner DS,无 JWT 注入 → `callerID==0`);经 Envoy 的客户端
> (`callerID>0`)一律返 `ERR_PERMISSION_DENY`。Envoy 侧对三个 `Send*` 精确 path 直接 403,`systemOnly`
> 是服务层兜底(双保险);内网调用不带 `x-pandora-jwt-payload`,`SessionCurrent` 会话门对其天然放行。

## 目录结构(Kratos 标准分层,对齐 login / matchmaker)

```
cmd/mail/main.go               启动入口(MySQL 强依赖 + Snowflake + inventory client + sweep goroutine + 会话门装配)
etc/mail-dev.yaml              MySQL 开发配置
etc/mail-dev-tidb.yaml         TiDB 版(唯一差异:DSN 指向 TiDB:4000 + collation utf8mb4_bin)
internal/
  conf/conf.go                 配置结构(MailConf + Defaults())
  service/
    mail.go                    RPC 入口(实现 mailv1.MailServiceServer;callerID / systemOnly 鉴权下沉)
  biz/
    mail.go                    MailUsecase 核心(ListMail / ClaimMail / DS 三段式 / Send* / 附件分形态发放)
    sweep.go                   SweepExpired 周期清理(过期归档 / 直删 / 领取记录截断)
  data/
    mail_repo.go               MailRepo(MySQL:游标 / 写扩散 / 领取幂等 / 收件箱上限事务 / sweep 批删)
    inventory_client.go        inventory gRPC client(GrantItems / GrantInstances / ClaimTransferInstances / ConsumeTransferEscrow)
  server/
    grpc.go                    gRPC server 注册(AuthOptional + SessionCurrent 中间件)
    http.go                    HTTP server(仅 /metrics)
```

## 核心调用链

### 1. ListMail —— 三类邮件合并视图 + 游标推进

`ListMail`(`internal/biz/mail.go:127`)把个人邮件(写扩散)与系统 / 公会邮件(watermark 拉取)合并成一个视图:

```
ListMail(player_id, cursor, limit)
├─ GetCursor            读 player_mail_cursor(last_sys / last_guild)          data/mail_repo.go:120
├─ ListPersonal         倒序拉个人邮件(mail_id < cursor,LIMIT limit)         data/mail_repo.go:147
│                       末条 mail_id = nextCursor(不足一页则 0)
├─ (cursor != 0 → 到此返回:翻页只走个人邮件,系统/公会仅首页拼)
├─ ListSysSince         拉 mail_id > last_sys 且当前生效的系统邮件            data/mail_repo.go:173
├─ GetPlayerGuild →     若属公会:ListGuildSince 拉该公会新邮件                data/mail_repo.go:181
└─ AdvanceCursor        把 last_sys / last_guild 推进到本次拉取的最大 mail_id   data/mail_repo.go:214
                        (GREATEST 单调不回退 → 看过的不重复拉、过期的不拉)
```

- **游标语义**:系统 / 公会邮件靠 `(mail_id > cursor) AND now ∈ [start_ms, end_ms)` 天然有界,拉过即推进游标,
  过期邮件永远落在游标之后不再出现。分页 `limit` 经 `clampLimit`(`biz/mail.go:115`)钳到 `[1,100]`,默认 50。
- **客户端可见结构**(§14):payload blob 反序列化为 `MailContentStorageRecord` 后,`toMail` / `toChannelMail`
  (`biz/mail.go:623`/`639`)只填 `Mail` / `MailAttachment` 最小视图,不外露存储字段。

### 2. ClaimMail —— 直连领取链(inventory 幂等)

`ClaimMail`(`biz/mail.go:199`)是玩家在线时的旧直连领取路径,顺序**先入库再记 claim**(crash 不丢奖):

```
ClaimMail(player_id, mail_id)
├─ GetClaimablePayload   按 channel 校验领取人权限 + 生效区间           data/mail_repo.go:250
│                        个人=收件人本人 / 系统=任意 / 公会=当前会员;越权→NotFound
├─ GetClaimState         已终态→ErrMailAlreadyClaimed;意图进行中→ErrMailClaimInProgress(与 DS 链互斥)
├─ partitionAttachments  附件按 oneof 分三类;unknown>0 → 整封 fail-closed(9606,不静默跳过)
├─ stack  → granter.Grant(GrantItems)              幂等键 mail:{mail}:{player}
├─ inst   → instGranter.GrantInstances(逐件铸实例) 幂等键 InstanceGrantKey 或 mail_inst:{mail}:{player}
├─ xfer   → xferClaimer.ClaimTransfers(托管行改归属) 幂等键 mail_xfer:{mail}:{player}(无空领豁免)
├─ RecordClaim           写 player_mail_claim 终态标记(INSERT IGNORE)
└─ SetPersonalStatus     个人邮件置 claimed(系统/公会靠 claim 表)
```

- **附件三形态**(oneof,`partitionAttachments`,`biz/mail.go:510`):`stack`(可堆叠)走 `GrantItems`;
  `instance`(唯一实例 / 装备)走 `GrantInstances` 逐件铸造;`transfer`(既存实例托管转移)走
  `ClaimTransferInstances` 只改归属(bag-domain.md §7.1)。三者各用独立幂等键,重领 / 重试各自去重。
- **transfer 无空领豁免**:`AllowNoopGrant` 也不放行 transfer——空领会把邮件标已领而托管行原地滞留,
  实例资产静默丢失;宁可领取报错保持可重领。

### 3. DS 三段式领取 —— 恰好一次(bag phase 2)

玩家在线时领取由 owner DS 驱动,经 bag journal 入包,取代直连链(两者对同一封邮件**互斥**):

```
① GetClaimableAttachments   意图落库(稳定展开)         biz/mail.go:298
     ├─ buildClaimIntent     附件展开为 BagItem;instance 形态在此一次性铸 ID   biz/mail.go:370
     └─ CreateClaimIntent    INSERT IGNORE 写意图行(claimed=0 + intent_payload)  data/mail_repo.go:340
          └─ 已有行 → 重读为准(绝不覆盖,覆盖会换 instance ID 破坏 journal 指纹)
② DS 预留容量 + bag.AppendJournal(op=mail_claim,幂等键=claim_key,单条批)  ← DS 侧,非本服务
③ MarkMailClaimed           终结领取                     biz/mail.go:404
     ├─ transfer → escrowConsumer.ConsumeTransferEscrow(消托管行,资产已 journal 入包)
     └─ MarkClaimed          意图行置终态(claimed=1,幂等)  data/mail_repo.go:352
```

- **恰好一次**:意图内容持久化 → 重放逐字节一致 → journal 指纹去重命中即已入包;Mark 前任意点崩溃
  → 重走 ①(返回同内容)→ journal 重放去重 → Mark 幂等(bag-domain.md §7)。
- **行复用 `player_mail_claim`**:`claimed=0 + intent_payload` = 意图进行中;`claimed=1` = 终态。
  `HasClaimed`(`data/mail_repo.go:292`)只认终态,DS 意图行不算已领,由 `GetClaimState` 单独判定。
- **nil-safe 门控**:未注入 `idGen` → 含 instance 的意图拒创建;未注入 `escrowConsumer` → 含 transfer 的意图拒终结
  (防托管行残留双持)。

### 4. Send* —— 发送(零写扩散 vs 写扩散 + 上限)

- `SendSystemMail`(`biz/mail.go:444`)/ `SendGuildMail`(`biz/mail.go:464`):`buildPayload` 校验标题 / 正文 / 附件形态
  → `defaultEnd` 补默认 TTL 并钳窗口 → `InsertSysMail` / `InsertGuildMail` **只插一行**(零写扩散);
  transfer 附件拒(多人可领与单实例矛盾)。
- `SendPersonalMail`(`biz/mail.go:489`):`InsertPersonalMail`(`data/mail_repo.go:404`)在**事务内**原子校验
  收件箱上限(§9 不变量 18,防 TOCTOU):`COUNT(*) ... FOR UPDATE` 锁玩家索引范围,满时驱逐最旧的**已领**邮件
  (附件已落袋,删除无损),仍满回滚返 `ERR_MAIL_BOX_FULL`;调用方(battle_result 掉落出箱)靠补扫重试,
  旧邮件过期被 sweep 清后自然成功。个人邮件可携 transfer 附件(收件人唯一,与托管行一一对应)。

### 5. SweepExpired —— 增长有界

`SweepExpired`(`internal/biz/sweep.go:33`)由 `main.go` 的 `runMailSweep` ticker 每 `SweepInterval`(默认 5m)驱动一轮,
每表至多一批(`SweepBatch`,默认 500,小批量防长事务锁表),任一步失败只记日志继续(彼此独立幂等,下轮重试):

```
SweepExpired (每 5m)
├─ ListExpiredPersonal → partitionExpired → ArchiveAndDeletePersonal
│     过期 + expired_retention 缓冲后:已领/无附件直删;带未领附件的先归档 player_mail_archive 再删
├─ DeleteSysMailEndedBefore / DeleteGuildMailEndedBefore   失效 + 缓冲后直删
├─ DeleteClaimsBefore   雪花 mail_id < MinIDAt(now - claim_retention) 的领取记录截断
└─ PurgeArchiveBefore   归档超 archive_retention_days 清除(归档表自身有界)
```

## 领取记录状态(`player_mail_claim` 行复用)

一行 `(player_id, mail_id)` 同时承载「直连已领终态」与「DS 三段式意图进行中」两态:

```
(无行)  ──ClaimMail──────────────► claimed=1              (直连:先入库后 RecordClaim)
   │                                    ▲
   ├──GetClaimableAttachments──► claimed=0 + intent_payload ──MarkMailClaimed──► claimed=1
   │      (DS 意图落库)               (意图进行中)              (journal ACK 后终结)
   └── 两条链互斥:意图开启后 ClaimMail 返 ERR_MAIL_CLAIM_IN_PROGRESS(9607),防 journal 与直连双发
```

`GetClaimState`(`data/mail_repo.go:308`)返回 `(claimed, intentOpen)` 二态;`CreateClaimIntent` 用 `INSERT IGNORE`
保证并发 / 重放不覆盖既有展开(换 instance ID 会破坏 journal 指纹一致性)。

## 存储(`pandora_social` 库)

DDL:`deploy/mysql-init/12-mail-tables.sql`(TiDB:`deploy/tidb-init/01-social-tidb.sql`)。邮件正文 + 附件序列化为
`MailContentStorageRecord` proto bytes 存 `payload` blob(§5.8)。

| 表 | 主键 / 索引 | 用途 |
|---|---|---|
| `sys_mail` | PK `mail_id`(雪花,channel 内递增) | 系统邮件一份(零写扩散) |
| `guild_mail` | PK `mail_id`;idx `guild_id` | 公会邮件一份 |
| `player_mail` | PK `mail_id`;idx `player_id+status` | 个人收件箱(写扩散) |
| `player_mail_cursor` | PK `player_id` | 系统 / 公会拉取游标(`last_sys_mail_id` / `last_guild_mail_id`) |
| `player_mail_claim` | PK `player_id+mail_id` | 领取幂等:`claimed` + `intent_payload`(DS 意图) |
| `player_mail_archive` | `mail_id` | 过期带未领附件邮件的补偿归档(sweep 迁入,超 `archive_retention_days` 清) |
| `guild_members` | —— | **只读**:判玩家当前所属公会(权威属 guild 服务,同库) |

> **`claim_retention_days=180` 是 §9.24「失效数据最多 90 天」的登记例外**:`defaultEnd`(`biz/mail.go:554`)把一切邮件
> `end_ms` 钳到「创建时刻 + `ClaimRetentionDays`」以内,保证 claim 行存活 ≥ 邮件可领窗口——重复领取永远先被 claim 行挡住,
> 不依赖 inventory 幂等流水兜底(其自身仅保留 90 天)。

## 配置项(`internal/conf/conf.go`)

| 键(`mail.*`) | 默认 | 说明 |
|---|---|---|
| `default_sys_ttl_days` | `7` | 系统 / 公会邮件 `end_ms=0` 时的默认有效期天数 |
| `default_personal_ttl_days` | `30` | 个人邮件 `expire_ms=0` 时的默认有效期天数(一切邮件生命有限,sweep 前提) |
| `max_inbox_size` | `200` | 单玩家收件箱行数上限(§9 不变量 18);满时驱逐最旧已领,仍满 `ERR_MAIL_BOX_FULL` |
| `sweep_interval` | `5m` | 过期清理轮询间隔(多副本各自跑,删除幂等无需锁) |
| `sweep_batch` | `500` | 每轮每表清理行数上限(小批量防长事务锁表) |
| `expired_retention_days` | `7` | 过期后延迟清理的缓冲天数(留客诉排查窗口 + 吸收时钟偏差) |
| `archive_retention_days` | `90` | 归档表保留天数(归档表自身有界) |
| `claim_retention_days` | `180` | 领取记录保留天数(§9.24 登记例外,须覆盖邮件最长寿命) |
| `max_title_len` | `64` | 标题最大长度(utf8 rune) |
| `max_body_len` | `2048` | 正文最大长度(utf8 rune) |
| `max_attachments` | `16` | 单封邮件附件上限 |
| `inventory_addr` | `""` | inventory 服务 gRPC 地址(领附件入库);缺省且非 `allow_noop_grant` → 拒启,防裸奔丢奖 |
| `allow_noop_grant` | `false` | inventory 不可用时允许空领(只标记不真发,**仅测试**;transfer 附件仍拒空领) |

顶层 `session_gate.require`(`false`,dev 宽松档)控制会话现行性门:`true` 时漏配端点拒启,`-Prod` 产物机械置 `true`。

## 本地启动

```powershell
# 1. 基础设施(MySQL + Redis;起 inventory 服务后可跑全链领取,否则设 allow_noop_grant=true 走空领)
pwsh tools/scripts/dev_up.ps1

# 2. 启 mail(MySQL dev 配置;强依赖 node.mysql_client.dsn = pandora_social 库)
go run ./services/social/mail/cmd/mail -conf services/social/mail/etc/mail-dev.yaml
```

> MySQL 是**强依赖**(DSN 为空直接启动失败);inventory 地址缺省时必须显式 `mail.allow_noop_grant: true`
> 才能起(否则拒启),空领只标记 claim 不真发物品,**仅测试用**。TiDB 部署改用 `etc/mail-dev-tidb.yaml`。

## 关联文档

- [`mail.md`](../../../docs/design/mail.md) — 邮件系统设计(channel + 游标模型 / 表结构 / 过期清理 / 不变量)
- [`bag-domain.md`](../../../docs/design/bag-domain.md) §7 — DS 三段式领取与 transfer 托管转移
- [`decision-revisit-bag-replay-semantics.md`](../../../docs/design/decision-revisit-bag-replay-semantics.md) — journal 重放 / 指纹去重语义
- [`decision-revisit-list-pagination.md`](../../../docs/design/decision-revisit-list-pagination.md) — 列表分页上限(`clampLimit` 依据)
- [`go-services.md`](../../../docs/design/go-services.md) — 服务清册(mail 端口 / 依赖 / 状态)
- [`infra.md`](../../../docs/design/infra.md) — 端口规划
- [`session-generation-rollout.md`](../../../docs/design/session-generation-rollout.md) — 会话现行性门(顶号失效)
- [`zero-downtime-update.md`](../../../docs/design/zero-downtime-update.md) §6.2 — `max_conn_age` GOAWAY 滚动更新
