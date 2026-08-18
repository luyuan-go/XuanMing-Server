# 玩家全链路日志排障手册(登录 → 匹配 → 进战 → 断线 → 结算退出 → 重连)

> 用途:值班/客服/AI 拿着一个 player_id,在生产 **info 级**日志里查清「玩家卡在哪一步、为什么」。
> 验收标准原文见 `docs/design/infra.md §11.3`(R1-R4 分级纪律与 join key 表),本手册是它的**使用侧地图**。
> 基线:2026-08-17 全链路日志审计(6 条链取证 + 对抗复核 + 当日修复);维护口径:改动本手册涉及的
> msg 名/字段前先看 §11.3「msg 命名契约」——改名会打断看板与告警。

## 0. 值班速查:拿到 player_id 后的标准动作

1. **拉玩家时间线**(Grafana → Loki):

   ```
   {service=~".+"} | json | player_id="<PID>"
   ```

2. **找最后一个里程碑**(§2 各阶段表的 INFO 行),它之后的空白就是卡点所在阶段。
3. **两跳补链**:内部服务间 RPC 不透传 player_id、只透传 trace_id;match 级日志(结算/分配/判弃)不带
   roster。从最后一条带 player_id 的日志取 `trace_id` 或 `match_id` 再查:

   ```
   {service=~".+"} | json | trace_id="<TID>"
   {service=~".+"} | json | match_id="<MID>"
   ```

4. **查该阶段的拒绝面**:每个业务拒绝点都有显式 WARN + 枚举 `reason`(access log 兜不住,见 §1)。
5. **DS 段**(文本日志,不能 `| json`)。⚠️ GameServer Pod 无 `app` label → `service` label 为空
   (deploy/k8s/infra/loki.yaml 注释明示),按 `pod` label 或全文匹配查:

   ```
   {source="k8s"} |= "player=<PID>"                      # DS 关键行约定 key=value 文本
   {source="k8s", pod=~"pandora-battle.*"} |= "match=<MID>"
   ```

   另注:`level` label 只对 JSON 行提取,DS 文本行没有 level label,按级别过滤只对 Go 服务有效。

6. **慢**:`| json | msg="rpc_slow"`;分段耗时看各里程碑的 `elapsed_ms` / `barrier_wait_ms` / `dur_total_ms`。

**join key 表**(infra.md §11.3 R3):登录=`player_id/account/device_id/sess_jti`;匹配=`player_id/ticket_id/
match_id/team_id`;进出场=`player_id/hub_assignment_id/ds_pod/admission_id/allocation_id`;结算=`match_id/
player_id/ds_pod/ds_instance_epoch`;重连=`player_id/match_id/owner_epoch/jti/ticket_sha`。

## 1. 日志体系事实(AI 接入必读)

- **两条产生源,一条收集管道**:Go 服务用 `pkg/log`(Kratos+zap)输出 **JSON** 到 stdout;UE DS/客户端
  是 UE 文本行。两者都经 **Grafana Alloy → Loki**(`deploy/alloy`、`deploy/loki`),同一界面查询。
  **Prometheus 只管指标不管日志**(`deploy/prometheus`,`pandora_rpc_total` 等)。
- **JSON 字段**:`ts / level / caller / service / msg / trace_id / player_id / match_id / team_id + 业务字段`。
  ⚠️ zap 的 Message 恒为空串,真正的消息名在 **`msg` 字段**——解析器按 `msg` 过滤,不要按 message。
- **级别语义**(§11.3 R1-R4):INFO=不可逆里程碑(每玩家每链路每阶段至多一条);WARN=拒绝/降级(必带枚举
  `reason`);ERROR=服务端故障/数据丢失风险;DEBUG=高频常态(心跳/续期/逐帧,`LOG_LEVEL=debug` 可线上临时全开)。
- **access log 兜不住业务拒绝**:`pkg/middleware/logging.go` 只有 transport 失败(`rpc_failed`)、慢
  (`rpc_slow`)、in-band 服务器故障域码(`rpc_inband_error`,仅 Unknown/Internal/Timeout/Unavailable)不是
  DEBUG;**ErrUnauthorized/ErrInvalidArg/ErrPermissionDeny 及全部业务码(>999)都落 `rpc_ok`=DEBUG**,生产
  info 级不可见——所以「玩家被拒」必须靠各服务的显式 WARN,别指望 access log。
- **player_id 注入边界**:玩家 JWT 面(:8443)由中间件自动进 ctx;**login 未鉴权面、DS 回调面(:8444)、
  kafka 消费循环、后台 ticker 不会自动带**,这些位置的日志靠代码手写 `player_id` 字段(本轮已补齐关键点,
  新增点位必须延续)。
- **trace_id 跨进程**:后端采纳入站 `x-pandora-trace-id`;UE 客户端前缀 `ue`、UE DS 前缀 `ds`——看前缀即知
  链路发起端。DS 自己的本地日志行没有 trace_id 字段,靠 `player=/match=` 文本对齐时间窗。
- **DS 文本行检索约定**:关键行必须含 `player=<id>`(部分历史行是中文语句,检索词见 §2.7)。
- **能查多久 / 存在哪**(决定"这单还查不查得动"):日志存 **Loki**,不在 MySQL/TiDB 里,别去翻库。
  保留 **7 天**(compactor 到期真删),超期即查不到。落盘位置:compose=docker volume `loki-data`;
  k8s=PVC `loki-data`(2026-08-18 前是 emptyDir,**Loki Pod 一重建历史全零**,老工单查无结果可能是这个原因
  而不是没打日志)。`start.ps1 -Down` 已改为保留 PVC。生产切 MinIO 的路径见 infra.md §11.2。
- **短命 DS 也查得到**:Battle DS 每局一个 Pod,但结算后约 15~17s(异常 15~20s)才回收,Alloy 秒级采集
  来得及;Pod 删除后仍能在 Loki 查到(INC-20260803 即从 Loki 捞回已删 Pod 的完整退出序列)。
  唯一例外是 **Alloy 自身重启的窗口**——那一局的 Pod 与 kubelet 日志目录已消失,无第二份可回补,
  故 Alloy 的 positions 也已落 PVC。

## 2. 六段链路:里程碑、玩家问题判据与陷阱

### 2.1 登录 / 选角(login + player + data_service)

| 里程碑(INFO) | 说明 |
|---|---|
| `service_ready`(启动链尾) | login 依赖自检:`account_seed_done→account_repo_mysql→redis_connected→locator_dial_ok→hub_allocator_dial_ok→service_ready`,缺哪条=对应依赖没通 |
| `login_ok` / `login_wait_returned` | **全链唯一 account↔player_id 映射点**;带 device_id/sess_jti/battle_ds_addr/match_id/dur_total_ms;WAIT 降级走 WARN+wait_reason |
| `session_generation_rotated`(2026-08-17 新增) | 顶号/重登轮换的**赢家侧**记录:prev_sess_jti 可 join 回上一条 login_ok 拿旧设备 |
| `select_role_ok` | 带 role_id/hub_ds_addr/source_match_id/sess_jti |
| `battle_authority_resolved` | 重连分水岭,decision 7 枚举(presence_in_battle/recovered_from_ready_claim/match_none…) |

**玩家问题 → 判据**
- 「卡登录界面」:`login_ok` 缺失 → 看 `login_account_not_found / login_password_mismatch / login_account_banned / ban_check_failed / session_set_failed(reason=superseded|infra) / session_generation_persist_failed`。
- 「点选角被弹回登录」:`session_gate_rejected`(2026-08-17 新增,reason=session_not_found/jti_evidence_missing/jwt_payload_header_missing/session_token_*)——**全服集中报 jwt_payload_header_missing = 网关 jwt_authn 配置漂移**。
- 「进不了世界/卡加载」:`get_profile_failed` / `get_loadout_failed`(2026-08-17 新增,player 服务,带 code+err;DS 面拉档失败此前完全无 player_id 可查)。`player_cache_corrupt_entry`(data_service,新增)= 缓存坏档/串号。
- 「被顶号」:被顶侧 `session_superseded_rejected`(它下次发请求才出现);赢家侧 `session_generation_rotated`。

**陷阱**:①login 未鉴权面日志没有自动 player_id,按 account 补查;②`mysql find account: context deadline exceeded` 是 ctx 过期不是账号不存在;③本机多套并存库,先核对生效 DSN(login-debug.md §4)。

### 2.2 组队与匹配(team + matchmaker + player_locator)

| 里程碑(INFO) | 说明 |
|---|---|
| `team_created / team_invite_sent / team_member_joined / team_set_ready` | 全带 player_id;拒绝面各有 `*_rejected` WARN + reason |
| `match_start_accepted` | 带 **member_ids 全列表** + operation_id + ticket_id(player→ticket 的 join 点) |
| `team_match_roster_locked / team_match_roster_frozen` | 名单冻结(roster 数组含每人 player_id:ready:hero) |
| `match_found / solo_match_found` | 带 ticket_ids + 票龄(>10min 另报 `stale_ticket_matched`);无 player_ids,经 accepted 两跳 |
| `match_ready` | 签票+READY 下发;`push_match_progress_buffered`(2026-08-17 新增,push 侧)证实 READY 已入该玩家投递缓冲 |
| `location_state_changed` | locator 状态迁移(player_id + prev/new state),`locator_guard_rejected` WARN 是状态机拒绝面 |
| `match_released / team_match_ended_unready` | 打完一局复位链(INC-20260813-001) |
| `resolve_match_context_ok` | 重连恢复入口 |

**玩家问题 → 判据**
- 「点了匹配没反应」:有 `match_start_accepted` 但之后静默 → **当前已知盲区**(saga 补偿零日志,见 §4 待办 M1);没有 accepted → 看 `match_start_rejected`(gate+reason 七道闸)/`match_start_member_offline`(带 offline_players)。
- 「一直匹配中」:队列侧 `queue_absence_reaped_ticket`(带 absent_players)/`match_no_capacity_requeued`;分配卡死看连续 `ds_allocate_failed` ERROR(§4 M5:尚无老化告警)。
- 「反复 4002」:先看之前有没有 `match_start_accepted`——有=客户端恢复循环自撞;没有且 start-claim 残留=孤儿 claim(INC-20260724-001 §5.1,两种处置完全不同)。
- 「队友掉了打成残局」:`team_match_ended_unready` 频次应与结算局数同量级,偏低=复位链没接上;`match_start_rejected gate=member_offline` 异常高=grace 配小。
- 「谁没点确认导致这局黄了」:**当前盲区**(确认超时不点名,§4 M2)。

**陷阱**:①`match_confirm_timeout` 只有 match_id;②玩家主动取消匹配成功目前仅 DEBUG(§4 M3);③MATCHING 投影只在成局那一刻写、永不续期,不能拿「locator 查不到」判离线(判死统一走 absentBeyond)。

### 2.3 进入战斗(ds_allocator + hub_allocator + owner + push)

| 里程碑(INFO) | 说明 |
|---|---|
| `battle_allocate_ok` | 受理放行(match 级,elapsed_ms) |
| `gameserver_allocated / gameserver_allocate_bound` | Agones 分配 + exact 实例定格;失败拆 `no_available_gameserver`(扩容)vs `control_plane_failed`(查 k8s);`ds_fleet_capacity_exhausted` ERROR |
| `battle_warming → battle_ds_heartbeat_ready / battle_ready_after_heartbeat` | 成对可算 DS 冷启动耗时;卡冷启动=有头无尾+`battle_ready_wait_timeout` |
| `owner_transition_begun` | **player↔pod/allocation 的第一处强 join 点**(owner 服务,带 barrier_wait_ms) |
| `hub_departed` | 离开大厅(与 hub_admitted 成对) |
| `owner_admitted / owner_admit_barrier_wait` | 进场确认;屏障实际等待可还原 |
| `battle_target_resolved` | 重连回场解算(拒绝分 4 档带 ready_reason) |

**玩家问题 → 判据**
- 「匹配到了但黑屏/卡传送」:按 match_id 看链走到哪:分配失败(`gameserver_allocate_failed`)→ 冷启动超时(`battle_ready_wait_timeout`)→ owner 拒(`owner_begin_failed`/`battle_ready_refused_owner_begin_failed`)→ READY 没送到(`push_match_progress_buffered` 缺失 + `match_push_failed`)。
- 「进图失败被弹回/没到齐」:`battle_abandoned_empty_timeout`(带 absentees)/`roster_incomplete_would_abandon`/`noshow_penalty_armed`(逐玩家)。
- 「大厅随机被踢」:owner 侧 `owner_renew_lease_slow`(commit_ms≈total_ms=磁盘 fsync,INC-20260812-002)+ DS 侧「Hub Admission 失败,拒绝 SetLocation 并踢出」。

**陷阱**:①判弃 `battle_abandoned_heartbeat_timeout` 目前不带 player_ids(§4 D1,按 player 查经 locator 两跳);②canary fleet 0/0/0 在无灰度时是常态;③绝不删 Allocated GameServer。

### 2.4 战斗中断线(locator + team + offlinewatch + ds_allocator)

| 里程碑 | 说明 |
|---|---|
| `location_disconnect_reported`(INFO) | 干净断线 10s 内可查(shrunk/grace_ms) |
| `location_refresh_projection_missing / location_refresh_pod_mismatch`(2026-08-17 新增 WARN) | 「人连在 Hub 上但投影已蒸发/指向别台」——此前 TTL 蒸发零痕迹;恢复有 `location_refresh_projection_recovered` |
| `team_presence_lost_unready`(INFO) | 软化档:断线取消准备(since_ms/ready_generation) |
| `team_offline_leave`(INFO) | 硬档:满 180s 摘人(threshold/captain_transferred);`offline_leave_disabled`(新增)=功能没开 |
| `offlinewatch_deferred_stuck`(2026-08-17 新增 WARN) | 「离线很久还摘不掉」点名(此前推迟循环与正常对局同形不可见) |
| `battle_eviction_order_first_issued / battle_eviction_order_stalled`(2026-08-17 新增) | 驱逐单首发台账 + 超龄未 ack 告警(「点了退出没反应」) |

**陷阱**:①崩溃/停报场景「变离线」靠 TTL 静默过期,机制上无日志——用新增的 refresh 差集 WARN + team 摘人日志夹逼时间窗;②战斗中掉线者 BATTLE presence 按 roster 刻意续期(保席重连,battle-reconnect.md §2.2),不是泄漏;③gate 崩溃→会话永久 ONLINE 是已知待拍板项,此时「还显示在线」无矛盾痕迹。

### 2.5 结算与退出(battle_result + inventory + hub_allocator + player)

| 里程碑(INFO) | 说明 |
|---|---|
| `battle_result_received → battle_result_recorded` | match 级;幂等命中区分同 pod 重试 vs 僵尸 DS;`battle_result_persist_failed` ERROR |
| `ds_lifecycle_abandoned_received → battle_abandoned_recorded` | DS 崩溃补偿路径(「打完没结算」的另一半答案) |
| `drop_grant_delivered / drop_overflow_mailed` | **奖励入包逐玩家台账**,player_id 直接 grep;失败 `drop_grant_failed` WARN |
| `progress_grant_delivered / progress_facts_skipped` | 经验/物品实时进度;漏配白名单在 skipped 里(sample_player_id) |
| `mission_fact_delivered` | 任务事实转发逐玩家台账 |
| `player_update_delivered`(2026-08-17 新增)→ `update_mmr_applied`(升 INFO) | **段位链出箱→入账端到端正查**(此前两端全 DEBUG,「打完段位没变」零痕迹);`outbox_pending_without_pusher`(新增)=部署缺 kafka 的积压告警 |
| `match_release_published` | 不发则回大厅再匹配撞 4002 |
| `location_state_changed`(BATTLE→HUB)→ `hub_assign_ok / hub_assigned` → `hub_admitted` | 回城链全带 player_id |

**玩家问题 → 判据**
- 「结算转圈」:match_id 两跳查 received/recorded/persist_failed/ds_auth_rejected。
- 「奖励没到账」:按 player_id 直接 grep 对应 `*_delivered/*_failed`;段位看 `player_update_delivered`+`update_mmr_applied`。
- 「退出卡 40 秒」:`hub_admission_barrier_not_open` WARN(retry_after_ms)——**27s ADMIT_BARRIER 是刻意的防脑裂等待,不是 bug**(pkg/placement.DSFenceReentryBarrier)。
- 「退出回到登录界面」:`hub_assign_failed / sign_hub_ticket_failed / hub_admission_rejected` 全带 player_id+reason。

### 2.6 断线重连(login + hub_allocator + ds_allocator + push)

重连 = 完整重登,先走 §2.1,再分诊:

| 里程碑(INFO) | 说明 |
|---|---|
| `battle_authority_resolved` | decision 枚举:「登录后被丢回大厅、对局没了」直接对账 |
| `battle_reconnect_skipped_terminal_match` / `battle_reconnect_route_unknown_retryable` | 三态门:Terminal 回大厅 / UNKNOWN 退避(**27s 再入屏障内 UNKNOWN 是设计内**,不是故障) |
| `ds_ticket_v2_issued / ds_ticket_issued`(+`ticket_sha` 2026-08-17 新增) | 签发侧;`login_battle_reconnect` |
| `verify_ds_ticket_rejected`(2026-08-17 新增 WARN) | **兑换点前置门**:reason=ds_credential_rejected/ds_admission_not_active…——一台 DS 凭据漂移=它上面所有玩家重连全挂,此前零痕迹(P0) |
| `ds_ticket_admission_rejected reason=hub_assignment_not_current`(新增) | 断线期间 assignment 被 Transfer/重建,旧票撞归属变更(重连最常见拒因,此前零痕迹,P0) |
| `authorize_battle_reconnect_ticket_failed` | 现在 err 带**首个不匹配条件名**(player_not_in_roster/heartbeat_stale/instance_epoch_mismatch…共 30+ 枚举,此前 25 个条件塌成一句话) |
| `ds_ticket_verified`(升 INFO) | **battle 重入的唯一成功里程碑**:票签出后静默=DS 没来核销,可与签发侧按 jti/ticket_sha 成对对账 |
| `push_stream_open → push_replayed(升 INFO) → push_stream_closed(新增)` | 通知流生命周期成对 + 补投推进量(「重连丢通知」可证实) |
| `owner_placement_resolved` | 冷启动恢复五态(admit_barrier_remain_ms=「还要等多久」) |

`ds_callback_auth_rejected`(2026-08-17 新增,pkg/middleware/dsauth.go):**全服务 DS 回调鉴权拒绝的统一收口**(enforce 档此前静默)。

### 2.7 DS 侧判据(UE 文本日志,Loki 全文匹配)

- **进没进这局 DS**:`InitNewPlayer accepted` → `Join succeeded` → `[SpawnPawn] player=... Camp=...`;
  `NotifyAcceptedConnection` 条数对 `roster_count`。认端口分文件:日志头 `Command Line:` 里 `-port=`。
- **集体被踢**:「授权租约超窗…自我 fencing」——先查 ds_allocator 部署/重启事件(INC-20260729-001),不是 DS bug。
- **PVE 退出链**:`BattleEnded{result, hub_ds_addr, hub_ticket}` 下发后由客户端自行回 Hub;客户端真判据是
  `armed_source_disconnect`,**两条 LogNet Error 是引擎无条件打的,不算判据**。
- **崩溃取证**:退出码 139=SIGSEGV/134=assert/137=OOM 或强杀/143=SIGTERM;崩溃前日志必须 `kubectl logs --previous`;
  「某窗口 DS 没打日志」先确认 entrypoint 有 `stdout 缓冲模式=stdbuf` 再做否定推断。
- **双重语义陷阱**:「玩家离开大厅,上报 player_locator 断线」对退出客户端与 travel 去战斗**同文本**(A-10 未修),
  判别法=去 battle DS 日志(**含 backup 文件**)找同时刻 `InitNewPlayer accepted`。
- mode=local:Go 业务日志在 `run/dev/logs/*.err.log`;账号→player_id 映射查 `login_dev_skip_password`。

**Hub DS 进出场链里程碑**(2026-08-17 审计,整体扎实;类别 LogPandoraDSAuth / LogPandoraHubFlow):

| 里程碑(Log 级,带 player_id=) | 说明 |
|---|---|
| `ds_prelogin_ticket_ok` → `InitNewPlayer accepted player=` | 验票通过→接纳;拒绝面 `ds_ticket_verify/gate/claims_rejected` 逐项 reason(唯一合法盲档:验签失败时 player_id=0) |
| `hub_admission_ack_sent → hub_admission_admitted`(ack_elapsed_ms) | Admission ACK 链,与 hub_allocator 按 admission_id/seq 对账 |
| `hub_entry_confirmed`(entry_elapsed_ms) → `[SpawnPawn] player=` | 进场完成;拦截面 `hub_spawn_blocked` 带 reason |
| `hub_logout`(was_admitted/had_pawn/departure_action) → `hub_disconnect_reported` | 离场;失败 `hub_disconnect_report_failed` |
| `hub_departure_queued → hub_departure_acked`(2026-08-17 新增) | 离场证明入队→后端确认出队;持续失败 `hub_departure_retry_failing`(首败+每 30 次,此前重试风暴零痕迹) |

**Battle DS 结算/驱逐链**(2026-08-17 窄审;类别 LogPandoraDSBackend / GameMode 中文行):

| 判据 | 说明 |
|---|---|
| 「上报战斗正常结算:match_id=」→「战斗结算 receipt 已确认:match_id=」 | ReportResult 发送→回执成对;失败「战斗结算上报失败:reason=result_report_rejected」WARN,长期未确认升 Error(`result_report_unacked`);全链只有 match_id 无 player(按玩家查经 locator 两跳) |
| `battle_eviction_orders_received / battle_eviction_orders_dropped`(2026-08-17 新增) | 驱逐单收到条数/整批丢弃原因——此前后端下没下过单在 DS 侧无法证明 |
| 「Battle exact eviction order 已处理:id= player= match=」 | 执行(kick_or_absent);拒绝两条 WARN 已补 match= |
| `battle_departure_ack_consumed`(2026-08-17 新增) | 离场 ack 被后端消费的终局痕迹,与后端 `battle_eviction_order_first_issued/stalled` 按 departure_id 对账 |

**「退出客户端 vs travel 去战斗」判别法**(A-10 去向字段未落地前的标准动作):`hub_logout` 文本对两者相同,
去**后端** locator 日志看同一秒:真退出=`location_disconnect_reported`(缩 TTL 被接受);travel/对局中=
`locator_disconnect_rejected reason=hub_fence_or_state_mismatch`(state 守卫只认 HUB);再用 battle DS 日志
(含 backup 文件)的 `InitNewPlayer accepted` 终证。

## 3. 2026-08-17 本轮落码清单(新增/升级,全部编译+单测绿)

| # | msg | 位置 | 级别 | 解决 |
|---|---|---|---|---|
| 1 | `verify_ds_ticket_rejected`(4 分支) | login service | WARN/ERROR | P0:兑换点前置门拒绝零日志 |
| 2 | `ds_ticket_admission_rejected reason=hub_assignment_not_current` | login biz/ticket.go | WARN | P0:hub 票核销拒绝零日志 |
| 3 | `session_gate_rejected`(4 分支) | login biz/login.go | WARN | P0:选角/签票会话门零日志(网关漂移可告警) |
| 4 | `get_profile_failed` / `get_loadout_failed` + ctx 注入 player_id | player service | WARN | P0:DS 拉档失败无 player_id/无原因 |
| 5 | `liveRosterDenyReason / modelBDenyReason` 拆分 | login data/battle_ticket_authorizer.go | — | P1:拒签 25 条件塌缩一句话 |
| 6 | `ds_ticket_verified` 升 INFO | login biz/ticket.go | INFO | P1:battle 重入无成功里程碑 |
| 7 | `session_generation_rotated` | login data/session_generation.go | INFO | P1:顶号赢家侧零记录 |
| 8 | `player_update_delivered` + `update_mmr_applied/idempotent_hit` 升 INFO + 消费 skip 带 key | battle_result + player | INFO | P1:段位链端到端仅 DEBUG |
| 9 | `location_refresh_projection_missing / pod_mismatch / recovered` | player_locator data | WARN | P1:presence 蒸发零名单 |
| 10 | `ds_callback_auth_rejected` | pkg/middleware/dsauth.go | WARN | 系统性:enforce 档 DS 回调拒绝全服静默 |
| 11 | `push_stream_closed` + `push_match_progress_buffered` + `push_replayed` 升 INFO | push | INFO | P2:流单边可见/READY 交付不可证实/补投不可证实 |
| 12 | `battle_eviction_order_first_issued / stalled` | ds_allocator data/battle_departure.go | INFO/WARN | P2:驱逐单发没发查不到 |
| 13 | `offlinewatch_deferred_stuck` | pkg/offlinewatch | WARN | P2:病理性推迟不可见 |
| 14 | `offline_leave_disabled` | team cmd/main.go | INFO | P2:关闭态零痕迹 |
| 15 | `player_cache_corrupt_entry` | data_service data/cache.go | WARN | P1:缓存坏档/串号静默当 miss |
| 16 | `ticket_sha` 字段(签发+验签失败) | login biz/ticket.go | — | P2:坏票无法两侧对账 |
| 17 | `outbox_pending_without_pusher` | battle_result | WARN | P2:缺 kafka 时段位出箱静默积压 |

**DS 侧(UE 仓库,待用户编译验证)**:

| # | 日志 | 位置 | 解决 |
|---|---|---|---|
| D1 | `hub_departure_acked` / `hub_departure_retry_failing` | PandoraDSBackendSubsystem.cpp | P1:离场证明重试队列三种终局零日志(local 档重试风暴曾完全不可见) |
| D2 | `InitNewPlayer rejected` 补 `player=`+err | PandoraDSGameModeBase.cpp | P2:battle 档拒绝按玩家检索零痕 |
| D3 | `hub_locator_write_ok/_failed` 补 player_id/admission_id | PandoraHubGameMode.cpp(+.h 回调签名) | P2:并发入场失败回调无法归属到人 |
| D4 | spawn 复核拦截行补 `player=` | PandoraDSGameModeBase.cpp | P2:battle 侧屏障命中查不到是谁 |
| D5 | `[SpawnPawn]` 两处 UE_LOG→MY_LOG | MyGameMode.cpp | 仓库日志宏纪律 |
| D6 | `hub_ticket_issue_requested / hub_ticket_issue_failed / hub_ticket_result_stale_dropped` | MyDsRecoveryCoordinator.cpp | P1:AwaitingTicket 阶段全程零日志(持续拒签=客户端静默 30s) |
| D7 | `ignoring stale/mismatched Hub admission signal` 补 reason 首失败项 | MyDsRecoveryCoordinator.cpp | P2:ACK 永失配空转只能靠 generation churn 反推 |
| D8 | HUB/BATTLE 终态确认行补 hub_assignment_id/admission_attempt/owner_epoch/generation | MyDsRecoveryCoordinator.cpp | P2:与 DS `hub_entry_confirmed`、后端 `hub_admitted` 三方对账 |
| D9 | `battle_eviction_orders_received/_dropped` + 四条驱逐行补 match= + `battle_departure_ack_consumed` | PandoraDSBackendSubsystem.cpp | P1:驱逐单收到/丢弃/ack 三段零日志(与后端首发台账双侧对账) |

## 4. 未闭环缺口(按优先级;M=matchmaker,D=ds_allocator,L=login/player)

**避让并发在途改动而未修**(matchmaker/biz/match.go、ds_allocator/biz/allocator.go 当时有其他会话在途;
接手前先 `git diff` 确认归属):

- **M1(P1)** StartMatch saga 异步作废零日志:`compensateStartOperation`(match.go≈1310-1340)与两处
  COMPENSATING 写点零 plog——「受理成功后玩家收到 FAILED」连 DEBUG 都没有。补 `match_start_compensated`
  WARN(ticket_id/operation_id/member_ids/reason)。
- **M2(P1)** 确认超时不点名:`match_confirm_timeout`(≈4098)只有 match_id;判责删票/退票零日志
  (≈2299-2367)。补 unconfirmed_player_ids/faulty_ticket_ids/requeued_ticket_ids;accept=false 的
  `match_confirm` 升 INFO。
- **M3(P1)** 玩家主动取消匹配成功仅 DEBUG(≈1605/1682):整票撤销、全队被推 FAILED 在 info 级零痕迹。升 INFO。
- **M4(P2)** `match_found/match_ready/match_released/team_match_ended_unready` 补 member player_ids(≤10 有界)。
- **M5(P2)** ALLOCATING 无老化告警、matchmaker 零业务指标:超 5min 未分配打限流 WARN(挂既有 tick)。
- **D1(P1)** `battle_abandoned_heartbeat_timeout`(≈3745/3842)补 player_ids(闭包里 playerIDs 就在手边)。
- **D2(P2)** Model B `roster_incomplete_would_abandon`(≈2460)补缺席名单(legacy 2918 已有,勿动)。
- **D3(P2)** `battle_heartbeat_refused`(departure reconcile 族)补结构化 player_id;顺带清理
  battle_departure.go 的 departureRejected 死代码。
- **D4(P2)** owner_addr 未配置时启动 WARN(ds_allocator + hub_allocator 同型):否则 per-player 归属日志
  整条静默消失且无降级提示。
- **L1(P1)** 补号(player_no)链:停用后无周期复报/metric;`data/player_no.go` 整文件零日志(含「第二写者
  整批回滚」不变量违反);`player_no_assigned` 只有 rows 无号段。
- **L2(P2)** `select_role_ok` 无 prev_role_id(幂等重选与换角同形);重名去重路径零痕;顶号不主动踢线的
  空窗(功能项,拍板后一并落日志)。
- **DS/客户端侧待办**:A-10 去向字段(`hub_logout` 补 departure_kind)——涉及信任边界选型(客户端
  travel intent 不可信,需标注 untrusted 仅供日志;或后端在 READY 派发时预告),待拍板,落地前用
  §2.7 的后端交叉判别法;恢复协调器 phase 跃迁行(phase 目前以枚举数字输出、成功路径无阶段耗时,
  建议 phase_transition 行 + UEnum 名字 + elapsed_in_prev_ms);终态行补从 BeginRequest 起算的
  recovery_elapsed_ms(需 Operation 加时间戳字段);battle DS 剩余:`RequestExactPlayerEviction` 整函数
  零日志(20+ fail-closed 分支同文案不可分诊,P1)、ReportResult 三条关键行补 player_ids(P1,现靠
  locator 两跳)、四条前置失败行(归一队伍/战绩生成/CacheResultOnce/首发权)补 match_id+ds_pod(P2)。
- **设计待拍板**:gate 崩溃→会话永久 ONLINE(会话清扫方案定时,同步要求「变离线」INFO);
  实例背包未启用无启动日志(inventory)。

## 5. 已证伪结论(别再立项)

- 「战斗中掉线者 presence 泄漏」——按 roster 续期是文档化的保席设计(battle-reconnect.md §2.2)。
- 「push 应订阅 player.update 推段位」——infra.md topic 登记表明文「服务间事件,push 不订阅」,客户端走查询。
- 「进战前分配失败按 player 查不到=链断」——matchmaker `match_start_accepted` 带 member_ids,两跳可查(工效
  问题记 M4,不是断链)。
- 「ADMIT_BARRIER 27s / 重登 27s 内 UNKNOWN」——刻意的防脑裂等待,不是卡死。
- 2026-07-24 遗留的「hub CAS 耗尽无日志」「ds_allocator sweep 无聚合」——已闭环,勿重报。
