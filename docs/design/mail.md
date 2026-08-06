# Pandora 邮件系统设计(mail 服务)

> 状态:设计待拍板(2026-06-29)。社交域(social),gRPC 20009 / metrics 21009。
>
> 依赖前置:`go-services.md`(服务清单)、`infra.md`(端口 / topic)、`CLAUDE.md §5/§9`(proto / 不变量)。
> 复用:inventory 的附件幂等发奖(W5 ③)、push 的红点推送、`pandora.system.notify` topic。

## 1. 为什么自研

GitHub 上的 "mail" 项目基本都是 SMTP / 电子邮件,跟"游戏内邮件"无关。游戏邮件强耦合玩家 ID、道具/货币发放、附件领取幂等、过期清理、运营群发,业界一律自研。本服务不引第三方库,只拼现有件:存储 = MySQL(pandora_social)、群发红点 = push、附件 = inventory。

## 2. 核心模型:邮箱频道(channel)+ 游标

拉取式**不为系统/公会各写一套**,抽象为通用 channel:任何"全员共享、按发布顺序、谁来谁拉"的邮件源都是一个 channel,只差一个标识。邮件分三类,前两类是 channel 实例:

| channel | 标识 | 拉取条件 | 游标 | 用途 |
|---|---|---|---|---|
| 系统 | `sys` | 全服(或定向) | last_sys_mail_id | 公告 / 全服补偿 / 活动 |
| 公会 | `guild:{guild_id}` | 所属公会 | last_guild_mail_id | 公会群发 / 公会奖励 |
| (扩展) | `activity:{id}` 等 | 命中目标 | 同模式 | 赛季 / 军团 … 不动核心 |
| 个人 | — | 写收件箱 | —(写扩散) | 互发 / 定点补偿 |

核心逻辑唯一:`(channel_id, mail_id > cursor, now ∈ [start,end]) → 推进游标`。加新 channel = 注册一行游标,不改拉取代码。发邮件永远 O(1),僵尸/退游不登录就不拉。

**系统邮件就是一个列表**:`sys_mail` 一行一封,mail_id 递增天然有序,合起来是全服共享列表;ListMail 返回"个人收件箱 + 各 channel 未读且有效"合并去重后的统一视图,客户端不感知背后是拉取还是扩散。

### 2.1 系统邮件 = 一份数据 + 玩家游标

- 系统邮件**只存一份**,每封带生效区间 `[start_ms, end_ms]` 与目标条件(全服 / 段位 / 区服)。
- 玩家身上只存一个游标 `last_sys_mail_id`(uint64,已读到的最大系统邮件 id)。
- 发系统邮件 = 插 1 行,**绝不遍历玩家、不写扩散**。

登录 / 主动刷新时拉取:

```
SELECT * FROM sys_mail
WHERE mail_id > :last_sys_mail_id
  AND :now BETWEEN start_ms AND end_ms
  AND (audience = ALL OR <定向条件命中>)
ORDER BY mail_id
```

读完把 `last_sys_mail_id` 推进到本次最大 id。新玩家 `last_sys_mail_id=0`,首登靠 `now ∈ [start,end]` 自动过滤掉早过期的历史邮件,不会拉一堆垃圾。

### 2.2 领取状态独立存

游标只管"看过没",**领没领附件是 per-player 状态**,单独存 `player_mail_claim(player_id, mail_id)`。原因:游标推进后无法反推哪封领过,所以领取记录必须落表,并复用 inventory ledger 幂等键(`mail:{mail_id}:{player_id}`)防重复发奖。

### 2.3 公会群发:同样拉取式,僵尸成员零成本

公会群发**绝不写扩散**(逐成员插邮件)。退游玩家可能占大半,写扩散既费空间又给死号发垃圾。做法同系统邮件:公会邮件每公会插一行(`guild_mail`),成员存 `last_guild_mail_id` 游标,登录时按当前所属 `guild_id` 拉取。退游/僵尸成员根本不登录 → 不拉 → 零成本。发邮件永远 O(1)。

额外约束:拉取下界取 `max(last_guild_mail_id, 入会时间对应的 mail_id)`,新成员不会看到入会前的旧群发;退会后游标作废,不再拉该公会邮件。

### 2.4 过期清理与增长有界(2026-07-21 落地,biz/sweep.go)

邮件表天然只增不减(写扩散的 `player_mail` 与"领取状态写扩散"的 `player_mail_claim` 尤甚),按三层保证增长有界:

1. **一切邮件生命有限**:`end_ms`/`expire_ms` 为 0 时发送侧补默认 TTL(系统/公会 `default_sys_ttl_days` 默认 7 天,个人 `default_personal_ttl_days` 默认 30 天)。没有默认 TTL,后面所有清理都清不干净。
2. **收件箱写入侧上限**(§9 不变量 18):`InsertPersonalMail` 事务内 `COUNT(*) FOR UPDATE` 原子校验单玩家行数(`max_inbox_size` 默认 200,防 TOCTOU);满时驱逐最旧的已领邮件(附件已落袋,删除无损),仍满返回 `ERR_MAIL_BOX_FULL`。调用方(battle_result 掉落出箱)靠补扫重试,旧邮件过期被清后发送自然成功。读侧 `ListMail` 已有 cursor 分页。
3. **sweep worker 周期回收**(每 `sweep_interval` 默认 5m 一轮,每表单批 `sweep_batch` 默认 500 行;多副本各自跑、无锁,删除/INSERT IGNORE 幂等,对齐 leaderboard 补扫模式):
   - `player_mail`:过期 + `expired_retention_days`(默认 7 天)缓冲后,已领/无附件直删;**带未领附件的先移入 `player_mail_archive` 再删**(§7.4 "不静默丢失",归档行是客诉补偿凭据);
   - `sys_mail`/`guild_mail`:`end_ms` 过 + 缓冲后直删(玩家游标天然跳过,不参与拉取);
   - `player_mail_claim`:雪花 `mail_id` 时间段单调,按 `mail_id < snowflake.MinIDAt(now - claim_retention_days)`(默认 180 天,须大于一切邮件最长有效期)范围删,走 `idx_mail` 索引;即便运营例外邮件寿命超长导致 claim 提前删,重复发奖也被 inventory 幂等键兜住,只影响 UI 已领标记;
   - `player_mail_archive`:超 `archive_retention_days`(默认 90 天)后清除,归档表自身有界。

清理走 `idx_expire`(player_mail)/`idx_end`(sys/guild)/`idx_mail`(claim)索引,小批量防长事务。InnoDB DELETE 不还磁盘空间(空间内部复用);若上线后量级证明批量删追不上写入,升级路径是按 mail_id RANGE 分区 + DROP PARTITION(两表 PK 均含 mail_id)。

## 3. 对外 RPC

```
# 玩家
ListMail(player_id) → []Mail            # 个人邮件 + 命中的有效系统邮件(已合并去重)
ReadMail(player_id, mail_id) → ok        # 个人邮件标记已读 / 推进系统游标
ClaimMail(player_id, mail_id) → ItemDelta # 领附件,走 inventory 幂等
DeleteMail(player_id, mail_id) → ok
# 运营 / 内网(系统接口,绝不对客户端开放:Envoy 精确 path 403 + 服务层 systemOnly 双保险,2026-07-08)
SendSystemMail(SystemMail) → mail_id     # 插一行,定向条件 + 生效区间
SendPersonalMail(player_id, Mail) → mail_id
```

## 4. 数据结构(proto,CLAUDE.md §5.8 四类)

- 客户端可见:`Mail` / `MailAttachment`(只回最小视图,不外露 storage record)。
- 存储快照:`SysMailStorageRecord` / `PersonalMailStorageRecord`(blob 列序列化,带 created_at_ms 等内部字段)。
- 事件:`SystemMailPublishedEvent`(走 `pandora.system.notify`,push 推全服红点)。
- ID:`mail_id` uint64 雪花(`pkg/snowflake`,单节点严格递增,32768/s);`item_config_id` uint32。
- 个人邮件直接用 snowflake,全局唯一即可。系统/公会邮件参与游标比较,要求 channel 内单调递增:由**单 worker(同一 node)生成**,避免跨节点同秒乱序导致漏拉。

## 5. 表结构(pandora_social)

- `sys_mail(mail_id PK, audience, start_ms, end_ms, payload BLOB, created_at)` + idx(end_ms) — 系统邮件一份。
- `guild_mail(mail_id PK, guild_id, start_ms, end_ms, payload BLOB, created_at)` + idx(guild_id) + idx(end_ms) — 公会邮件一份。
- `player_mail(mail_id PK, player_id, status, payload BLOB, expire_ms)` + idx(player_id,status) + idx(expire_ms) — 个人收件箱。
- `player_mail_cursor(player_id PK, last_sys_mail_id, last_guild_mail_id)` — 系统/公会邮件游标。
- `player_mail_claim(player_id, mail_id) PK` + idx(mail_id) — 领取幂等(idx 供 sweep 按雪花 cutoff 范围删)。
- `player_mail_archive(mail_id PK, player_id, status, expire_ms, created_ms, payload, archived_at)` + idx(player_id) + idx(archived_at) — 过期未领附件归档(§2.4)。

## 6. kafka

- 复用 `pandora.system.notify`:系统邮件发布只推一条红点广播,客户端收到后调 ListMail 拉取(系统邮件不写扩散)。
- 个人邮件走 push(key=收件人 player_id)实时红点。

## 7. 不变量

1. 附件领取幂等(同一 mail+player 只发一次,复用 inventory ledger)。
2. 系统邮件零写扩散(发邮件 O(1),不随玩家数增长)。
3. 客户端只拿 `Mail` 视图,不回 StorageRecord。
4. 过期附件清理后必有补偿或归档,不静默丢失。
5. 附件三形态各走各的交付链(2026-07-22,bag-domain.md §7.1):stack → GrantItems、
   instance(铸造凭证)→ GrantInstances、transfer(既存实例托管转移,只改归属)→
   ClaimTransferInstances(领取只认 inventory 托管行,伪造附件必 fail-closed)。
   transfer 仅个人邮件可携带;发送方必须先 EscrowOutInstances 托管(saga,失败补偿
   ReleaseTransferEscrow);任一形态交付失败整封保持未领取,重领靠各自幂等键去重。

## 8. 待人拍板

- 端口 20009 占用确认。
- 是否本期做"定向系统邮件"(段位/区服)还是只全服。
- 过期未领是否自动补发到个人邮件。**2026-07-21 部分落地**:清理侧已用归档兜底
  (`player_mail_archive` 保留 90 天,不静默丢失,§2.4);"自动补发"仍待拍板——若做,
  从归档表捞行补发即可,发放幂等由 inventory 键保证,不需改清理链路。
