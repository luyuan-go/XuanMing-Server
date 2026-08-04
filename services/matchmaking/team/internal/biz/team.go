// Package biz 是 team 服务的业务逻辑层(W3 ⑦ Phase 3,2026-06-05)。
//
// 设计原则(协议铁律 4 原则):
//  1. 立即完成型:7 个 RPC 在 biz 内完成状态机迁移 + redis 写 + kafka push 后立即返回
//  2. push 不发 caller:PushTeamUpdate callerPlayerID != 0 时不发给发起者自身
//  3. kafka key = player_id(不变量 §9):PushToPlayers 已保证
//  4. WATCH/MULTI/EXEC 乐观锁:所有写路径走 UpdateWithLock,冲突重试 OptimisticRetry 次
//
// 状态机合法迁移(见 proto/pandora/team/v1/team.proto):
//
//	FORMING  → READY(全员 ready)
//	READY    → FORMING(任一成员 leave/kick)
//	DISBANDED → 任何写操作都拒绝(ErrTeamWrongState)
package biz

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"

	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/team/internal/data"
)

// TeamEventPusher 是 kafka push 的抽象接口。
// 实现由 main 装配时注入(kafkax.KeyOrderedProducer.PushToPlayers 包装)。
type TeamEventPusher interface {
	// PushTeamUpdate 向 toPlayerIDs 广播队伍变更事件字节(不发给 callerPlayerID)。
	// payload 是 proto.Marshal(teamv1.TeamUpdateEvent) 的结果,event_type 缺省 0(旧事件)。
	PushTeamUpdate(ctx context.Context, callerPlayerID uint64, toPlayerIDs []uint64, payload []byte) (sent int, err error)

	// PushTeamEvent 与 PushTeamUpdate 相同,额外指定 push 域内事件类型判别键 event_type
	// (填入 PushFrame.event_type),供客户端按 (topic, event_type) 定位反序列化哪个 message。
	// eventType=0 时等价 PushTeamUpdate。用于邀请等「payload 是各自独立 proto」的专属事件。
	PushTeamEvent(ctx context.Context, callerPlayerID uint64, toPlayerIDs []uint64, payload []byte, eventType uint32) (sent int, err error)
}

// MatchCanceler 是“离队/踢人 → 撤销 matchmaker 匹配票据”联动的抽象接口。
// 实现由 main 装配时注入(data.GrpcMatchCanceler,直连 matchmaker 内网 gRPC)。
// 可为 nil:本机不起 matchmaker 的骨架联调 / 未配 matchmaker_addr 时跳过联动。
type MatchCanceler interface {
	// CancelMatch 撤销 playerID 当前所在的匹配票据(整张票据,含全体队友)。
	// 玩家未在排队时返回 ErrMatchNotFound(4004),调用方按常态忽略。
	CancelMatch(ctx context.Context, playerID uint64) error
}

// ── 常量 ─────────────────────────────────────────────────────────────────────

const (
	stateForming   = teamv1.TeamState_TEAM_STATE_FORMING
	stateReady     = teamv1.TeamState_TEAM_STATE_READY
	stateMatching  = teamv1.TeamState_TEAM_STATE_MATCHING
	stateInBattle  = teamv1.TeamState_TEAM_STATE_IN_BATTLE
	stateDisbanded = teamv1.TeamState_TEAM_STATE_DISBANDED
)

// ── TeamUsecase ───────────────────────────────────────────────────────────────

// TeamUsecase 是 team 业务逻辑的核心。
type TeamUsecase struct {
	repo   data.TeamRepo
	pusher TeamEventPusher
	cfg    conf.TeamConf

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分片,队伍 region 分布观测退化为不打日志(行为不变)。
	// 分片部署时由 main 经 SetCellRouter 注入,成员变更(建队 / 入队)后额外打一条队伍
	// 跨 region 组队观测(供撮合 / battle 放置评估跨 region 组队占比)。nil-safe。
	router *cellroute.Router

	// matchCanceler 是“离队/踢人 → 撤销 matchmaker 票据”联动。可为 nil(未配
	// matchmaker_addr / 骨架联调)→ 不联动,行为与历史一致。nil-safe。
	matchCanceler MatchCanceler

	// lastTouch 记录每个玩家上次 GetMyTeam 续期队伍 TTL 的时刻(节流,避免每次
	// 轮询都敲 Redis EXPIRE)。key=playerID(uint64) value=time.Time。
	// 多实例部署下各实例独立节流,最坏情况多几次 EXPIRE,无正确性影响。
	// 内存上限:maybeSweepLastTouch 每 touchInterval 清一次过期条目(见下),
	// 常驻规模 ≈ 最近 2×touchInterval 内轮询过 GetMyTeam 的活跃玩家数,不随 DAU 永久增长。
	lastTouch sync.Map

	// lastTouchSweepAtNs 是上次清扫 lastTouch 的时刻(UnixNano)。CAS 抢占保证
	// 同一时刻至多一个 goroutine 执行清扫,其余直接返回。
	lastTouchSweepAtNs atomic.Int64
}

// NewTeamUsecase 构造 TeamUsecase。
func NewTeamUsecase(repo data.TeamRepo, pusher TeamEventPusher, cfg conf.TeamConf) *TeamUsecase {
	return &TeamUsecase{repo: repo, pusher: pusher, cfg: cfg}
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),不做队伍 region 分布观测,行为与历史
// 一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 matchmaker / auction /
// battle_result / friend / chat / trade / dialogue / inventory / locator / push 一致)。Router 内部读路径无锁,并发安全。
func (u *TeamUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// SetMatchCanceler 注入“离队/踢人 → 撤销 matchmaker 匹配票据”联动。
//
// nil-safe:不调用 / 传 nil 时(未配 matchmaker_addr / 骨架联调),离队不联动撤票,
// 行为与历史一致。用 setter 而非构造参数,避免现有调用点/测试被迫改签名(与
// SetCellRouter 一致)。
func (u *TeamUsecase) SetMatchCanceler(c MatchCanceler) {
	u.matchCanceler = c
}

// InviteTTLMs 返回邀请令牌 TTL 的毫秒数,供 service 层计算 expires_at_ms。
func (u *TeamUsecase) InviteTTLMs() int64 {
	return u.cfg.InviteTTL.Std().Milliseconds()
}

// activeTTL 返回活跃队伍 Redis key 的生命周期。
func (u *TeamUsecase) activeTTL() time.Duration {
	return u.cfg.ActiveTTL.Std()
}

// touchInterval 是 GetMyTeam 在线续期的节流间隔:同一玩家至多每 15 分钟续一次。
// 客户端轮询周期(秒级)远小于它,active_ttl(60 分钟)远大于它,续期不会断流。
const touchInterval = 15 * time.Minute

// maybeTouchTeam 在线心跳保活:玩家仍在轮询自己的队伍 → 续期队伍与索引 TTL,
// 避免在线队伍被 active_ttl 误回收;停止轮询后 TTL 自然到期,僵尸队伍 GC 仍在。
// 15 分钟节流 + best-effort:失败只告警,不影响读返回。
//
// 同时刷新开放队伍索引的 score:索引成员的 score 是"索引项最晚存活时刻",与队伍 key 的
// TTL 同源。只续队伍 key 而不续索引,会让一支持续在线、持续招募的队伍在 active_ttl 后
// 被 ZREMRANGEBYSCORE 当成过期项清掉,从"获取队伍"列表里静默消失(队伍还活着但没人找得到)。
// 节流间隔(15min)远小于 active_ttl(60min),续期不会断流。
func (u *TeamUsecase) maybeTouchTeam(ctx context.Context, team *teamv1.TeamStorageRecord, playerID uint64) {
	now := time.Now()
	if v, ok := u.lastTouch.Load(playerID); ok {
		if last, ok2 := v.(time.Time); ok2 && now.Sub(last) < touchInterval {
			return
		}
	}
	u.lastTouch.Store(playerID, now)
	u.maybeSweepLastTouch(now)
	if err := u.repo.TouchTeam(ctx, team.GetTeamId(), playerID, u.activeTTL()); err != nil {
		plog.With(ctx).Warnw("msg", "team_touch_failed",
			"player_id", playerID, "team_id", team.GetTeamId(), "err", err)
	}
	u.syncOpenIndex(ctx, team, team.GetMapId())
}

// maybeSweepLastTouch 惰性清扫 lastTouch 里已过节流窗口的条目,防止长跑进程内存
// 随历史活跃玩家数无界增长。清扫间隔 = touchInterval;删除“距上次续期 ≥ touchInterval”
// 的条目与直接不存在等价(下次 Load 反正会放行续期),行为不变。CAS 抢占单 goroutine
// 执行,Range 全量扫描 O(活跃玩家数),每 15 分钟一次可忽略。
func (u *TeamUsecase) maybeSweepLastTouch(now time.Time) {
	last := u.lastTouchSweepAtNs.Load()
	if now.UnixNano()-last < int64(touchInterval) {
		return
	}
	if !u.lastTouchSweepAtNs.CompareAndSwap(last, now.UnixNano()) {
		return // 其他 goroutine 已在清扫
	}
	u.lastTouch.Range(func(k, v any) bool {
		if t, ok := v.(time.Time); !ok || now.Sub(t) >= touchInterval {
			u.lastTouch.Delete(k)
		}
		return true
	})
}

// ── 9 RPC ──────────────────────────────────────────────────────────────────

// CreateTeam 创建队伍,playerID 为队长。
// 前置条件:playerID 不在任何队伍中。
//
// 写序铁律:**先写队伍主体,后 ClaimPlayer 声明归属**。主体先落地时索引尚未指向它
// (teamID 是 Snowflake 新发,返回前无人可见),故不存在「索引已指向、主体还没写」的
// in-flight 窗口 —— 这是 claimPlayerHealingOrphan 把「主体不存在」判为真孤儿的安全前提。
// 若倒过来先 claim 后写主体,并发的 heal 会把 in-flight claim 误判孤儿并 CAS 删掉,
// 同一玩家可能同时出现在两支队伍(违反不变量 §1)。
// claim 失败 → 回滚删掉自己刚写的主体;中途崩溃残留的无主主体(无索引指向)由 TTL 自然回收。
func (u *TeamUsecase) CreateTeam(ctx context.Context, teamID, playerID uint64) (*teamv1.TeamStorageRecord, error) {
	ttl := u.activeTTL()

	now := time.Now().UnixMilli()
	team := &teamv1.TeamStorageRecord{
		TeamId:      teamID,
		CaptainId:   playerID,
		State:       stateForming,
		Members:     []*teamv1.TeamMemberStorageRecord{{PlayerId: playerID}},
		CreatedAtMs: now,
		UpdatedAtMs: now,
		MaxSize:     int32(u.cfg.MaxMembers),
	}

	// 1. 先写队伍主体(此时索引不指向它,对全世界不可见)。
	if err := u.repo.Create(ctx, team, ttl); err != nil {
		return nil, err
	}

	// 2. 原子声明玩家归属(SETNX),保证不变量 §1:一人只能在一个队。
	//    孤儿索引(索引指向的队伍主体已过期/解散)会自愈,不误拦成 3004。
	if err := u.claimPlayerHealingOrphan(ctx, playerID, teamID, ttl); err != nil {
		// 声明失败(玩家真在其他队) → 回滚删掉自己刚写的主体,避免残留无主队伍。
		_ = u.repo.DeleteTeam(ctx, teamID)
		return nil, err
	}

	// 3. push 给队长自己(创建者收到快照确认)
	u.pushUpdate(ctx, 0, []uint64{playerID}, team,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_JOINED, 0)

	// 新建队伍只有队长一人、状态 FORMING → 立即进入"开放招募"索引,让别人能找到它。
	// prevMapID 与 MapId 同为 0(新建队尚未选图),不产生换桶操作。
	u.syncOpenIndex(ctx, team, team.MapId)

	plog.With(ctx).Debugw("msg", "team_created", "team_id", teamID, "captain_id", playerID)
	// 分片:队伍锁定队长 owner cell(TeamShardKey=captain_id);新建队仅队长一人,region 分布
	// 为单一,但统一打点便于后续成员加入后对比。router 为 nil(单 Cell)→ 不打。
	u.logTeamComposition(ctx, team)
	return team, nil
}

// Invite 邀请目标玩家加入队伍。inviterID 必须在该队伍中。
func (u *TeamUsecase) Invite(ctx context.Context, inviteID, teamID, inviterID, targetPlayerID uint64) (*teamv1.TeamStorageRecord, error) {
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errcode.New(errcode.ErrTeamNotFound, "team %d not found", teamID)
	}
	if team.State == stateDisbanded {
		return nil, errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
	}
	if !hasMember(team, inviterID) {
		return nil, errcode.New(errcode.ErrTeamNotFound, "player %d not in team %d", inviterID, teamID)
	}
	if len(team.Members) >= int(team.MaxSize) {
		return nil, errcode.New(errcode.ErrTeamFull, "team %d is full (%d/%d)", teamID, len(team.Members), team.MaxSize)
	}

	// 存储邀请令牌。同一被邀请人 pending 邀请数超 MaxPendingInvites 时返
	// ErrTeamInvitePendingLimit(3008)——不变量 §9-18 写入侧上限,限流+占位在
	// data 层 Lua 内原子完成。
	if err := u.repo.SetInvite(ctx, inviteID, teamID, inviterID, targetPlayerID, u.cfg.InviteTTL.Std(), u.cfg.MaxPendingInvites); err != nil {
		return nil, err
	}

	// push 邀请给 target(不发给 inviter — 原则 2)。
	// 邀请是「payload 各自独立 proto」的专属事件。老客户端只认 TeamUpdateEvent(reason=INVITE_SENT),
	// 新客户端只认独立 TeamInviteEvent(已不再从 TeamUpdateEvent 读邀请)。灰度共存期靠"双发"喂饱两代:
	//   - dual(默认):两条都发。老客户端 legacy 弹框、把独立事件误解成 TeamUpdateEvent→护栏(InviteId/
	//     TeamId>0)不过→只多一次无害快照不误弹;新客户端忽略 legacy、只在独立事件弹框。各弹一次不双弹。
	//   - dedicated:只发独立事件(全量铺完新客户端后用)。
	//   - legacy:只发旧事件(回退用)。
	switch u.cfg.InvitePushMode {
	case "legacy":
		// 回退模式只发旧 TeamUpdateEvent,继续服务尚未识别 event_type 的客户端。
		u.pushUpdate(ctx, inviterID, []uint64{targetPlayerID}, team,
			teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_INVITE_SENT, inviteID)
	case "dedicated":
		// 新客户端全量铺开后只发精简的独立邀请事件,停止冗余旧快照。
		u.pushInvite(ctx, inviterID, targetPlayerID, teamID, inviteID)
	default: // "dual"(含空串):共存期双发,金丝雀安全
		// 金丝雀共存期同时照顾新旧客户端;两代各认一种 payload,不会重复弹邀请框。
		u.pushInvite(ctx, inviterID, targetPlayerID, teamID, inviteID)
		u.pushUpdate(ctx, inviterID, []uint64{targetPlayerID}, team,
			teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_INVITE_SENT, inviteID)
	}

	plog.With(ctx).Debugw("msg", "team_invite_sent",
		"team_id", teamID, "inviter_id", inviterID,
		"target_player_id", targetPlayerID, "invite_id", inviteID)
	return team, nil
}

// AcceptInvite 目标玩家接受邀请加入队伍。
func (u *TeamUsecase) AcceptInvite(ctx context.Context, inviteID, teamID, playerID uint64) (*teamv1.TeamStorageRecord, error) {
	// 1. 若提供 inviteID,校验令牌
	if inviteID != 0 {
		inv, found, err := u.repo.GetInvite(ctx, inviteID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errcode.New(errcode.ErrTeamInviteExpired, "invite %d expired or not found", inviteID)
		}
		if inv.TargetPlayerID != playerID {
			return nil, errcode.New(errcode.ErrTeamInviteExpired, "invite %d target mismatch", inviteID)
		}
		if inv.TeamID != teamID {
			return nil, errcode.New(errcode.ErrTeamInviteExpired, "invite %d team mismatch", inviteID)
		}
	}

	// 2. 走共用入队事务(内部先 ClaimPlayer 保不变量 §1,再改成员表)。
	result, err := u.joinTeam(ctx, teamID, playerID)
	if err != nil {
		return nil, err
	}

	// 删 invite 令牌(同时释放被邀请人 pending 索引配额)
	if inviteID != 0 {
		_ = u.repo.DeleteInvite(ctx, inviteID, playerID)
	}

	// push MEMBER_JOINED 给所有成员(不发给 playerID — 原则 2)
	u.pushUpdate(ctx, playerID, memberIDs(result), result,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_JOINED, 0)

	// 人数变了 → 可能从"招募中"变成"已满",同步开放队伍索引(非权威投影)。
	u.syncOpenIndex(ctx, result, result.MapId)

	plog.With(ctx).Debugw("msg", "team_accept_invite", "team_id", teamID, "player_id", playerID)
	// 分片:成员加入后队伍 region 分布可能变跨 region(影响 §4.4 battle DS 放置)。router 为 nil → 不打。
	u.logTeamComposition(ctx, result)
	return result, nil
}

// joinTeam 是"把 playerID 加进 teamID"的唯一入队事务,由三条路径共用:
// AcceptInvite(接受邀请)、ApplyToTeam(open 策略直接入队)、HandleTeamApplication(队长同意)。
//
// 为什么必须共用一份:入队是不变量 §1(一人只能在一个可操作队伍)的关键写路径,
// 顺序铁律是**先 ClaimPlayer 原子声明归属,后改成员表**——两个并发入队路径若各写一份,
// 迟早有一条忘了先声明,同一玩家就会同时出现在两支队伍。共用后新增入队入口不可能绕过它。
//
// 失败时用 CAS 回滚 claim(仅当索引仍指向本队才删),防误删并发路径刚写入的新归属;
// 回滚失败会把玩家锁在"claim 指向一支没真进的队"的状态(靠 claimPlayerHealingOrphan
// 下次自愈),因此必须留 Warn 可观测。
func (u *TeamUsecase) joinTeam(ctx context.Context, teamID, playerID uint64) (*teamv1.TeamStorageRecord, error) {
	ttl := u.activeTTL()
	if err := u.claimPlayerHealingOrphan(ctx, playerID, teamID, ttl); err != nil {
		return nil, err
	}

	var result *teamv1.TeamStorageRecord

	if err := u.repo.UpdateWithLock(ctx, teamID, u.cfg.OptimisticRetry, func(team *teamv1.TeamStorageRecord) error {
		if team.State == stateDisbanded {
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if len(team.Members) >= int(team.MaxSize) {
			return errcode.New(errcode.ErrTeamFull, "team %d full", teamID)
		}
		if hasMember(team, playerID) {
			return errcode.New(errcode.ErrTeamAlreadyInTeam, "player %d already in team %d", playerID, teamID)
		}

		team.Members = append(team.Members, &teamv1.TeamMemberStorageRecord{PlayerId: playerID})
		team.UpdatedAtMs = time.Now().UnixMilli()

		// 全员 ready → READY
		if team.State == stateForming && allReady(team.Members) {
			team.State = stateReady
		}
		result = cloneTeam(team)
		return nil
	}, ttl); err != nil {
		if derr := u.repo.DeletePlayerIndexIfMatches(ctx, playerID, teamID); derr != nil {
			plog.With(ctx).Warnw("msg", "team_join_rollback_index_delete_failed",
				"player_id", playerID, "team_id", teamID, "err", derr)
		}
		return nil, err
	}

	// player index 已由 ClaimPlayer 在锁前原子写入,此处无需再写。
	return result, nil
}

// LeaveTeam 玩家主动离队。
//
// 匹配联动:若该成员正在排队/确认期(matchmaker 持有其 claim),离队后 best-effort
// 撤销整张匹配票据(队伍人数已变,票据快照不再成立):排队中 → 全队退出队列;
// 确认期 → 等价该玩家拒绝确认(match 失败,其余票据退回队列)。见 cancelMatchmaking。
func (u *TeamUsecase) LeaveTeam(ctx context.Context, teamID, playerID uint64) (*teamv1.TeamStorageRecord, error) {
	ttl := u.activeTTL()
	disbandedTTL := u.cfg.DisbandedRetention.Std()
	var result *teamv1.TeamStorageRecord

	if err := u.repo.UpdateWithLock(ctx, teamID, u.cfg.OptimisticRetry, func(team *teamv1.TeamStorageRecord) error {
		if team.State == stateDisbanded {
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if !hasMember(team, playerID) {
			return errcode.New(errcode.ErrTeamNotFound, "player %d not in team %d", playerID, teamID)
		}

		team.Members = removeMember(team.Members, playerID)
		team.UpdatedAtMs = time.Now().UnixMilli()

		if len(team.Members) == 0 {
			// 队伍空 → 解散
			team.State = stateDisbanded
		} else {
			// 队长离队 → 转移给第一个成员
			if team.CaptainId == playerID {
				team.CaptainId = team.Members[0].PlayerId
			}
			// READY 状态下有人离开 → 回 FORMING
			if team.State == stateReady {
				team.State = stateForming
			}
		}
		result = cloneTeam(team)
		return nil
	}, ttl); err != nil {
		return nil, err
	}

	// 删 player index。CAS:仅当索引仍指向本队才删,防误删玩家并发加入新队的归属。
	if err := u.repo.DeletePlayerIndexIfMatches(ctx, playerID, teamID); err != nil {
		plog.With(ctx).Warnw("msg", "team_leave_delete_player_index_failed", "player_id", playerID, "team_id", teamID, "err", err)
	}

	// 匹配联动:离队成员若正在排队/确认期 → 撤销整张票据(best-effort,不阻断离队)
	u.cancelMatchmaking(ctx, teamID, playerID)

	// 人数/状态变了 → 同步开放队伍索引(解散或满员时会被摘掉)。
	u.syncOpenIndex(ctx, result, result.MapId)

	// 解散时用短 TTL 刷新 key
	if result.State == stateDisbanded {
		u.refreshDisbandedTTL(ctx, teamID, disbandedTTL)
		// 队伍没了,残留的入队申请再无人能处理 → 顺手清掉,不留到 TTL(best-effort)。
		if err := u.repo.DeleteApplications(ctx, teamID); err != nil {
			plog.With(ctx).Warnw("msg", "team_disband_delete_applications_failed",
				"team_id", teamID, "err", err)
		}
		u.pushUpdate(ctx, playerID, memberIDs(result), result,
			teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_DISBANDED, 0)
	} else {
		u.pushUpdate(ctx, playerID, memberIDs(result), result,
			teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_LEFT, 0)
	}

	plog.With(ctx).Debugw("msg", "team_leave", "team_id", teamID, "player_id", playerID,
		"new_state", result.State)
	return result, nil
}

// Kick 队长踢人。
//
// 匹配联动:同 LeaveTeam——被踢成员若正在排队/确认期,踢人后 best-effort 撤销其所在
// 的整张匹配票据。见 cancelMatchmaking。
func (u *TeamUsecase) Kick(ctx context.Context, teamID, captainID, targetPlayerID uint64) (*teamv1.TeamStorageRecord, error) {
	ttl := u.activeTTL()
	var result *teamv1.TeamStorageRecord

	if err := u.repo.UpdateWithLock(ctx, teamID, u.cfg.OptimisticRetry, func(team *teamv1.TeamStorageRecord) error {
		if team.State == stateDisbanded {
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if team.CaptainId != captainID {
			return errcode.New(errcode.ErrTeamNotCaptain, "player %d is not captain of team %d", captainID, teamID)
		}
		if captainID == targetPlayerID {
			return errcode.New(errcode.ErrInvalidArg, "captain cannot kick themselves")
		}
		if !hasMember(team, targetPlayerID) {
			return errcode.New(errcode.ErrTeamNotFound, "player %d not in team %d", targetPlayerID, teamID)
		}

		team.Members = removeMember(team.Members, targetPlayerID)
		team.UpdatedAtMs = time.Now().UnixMilli()

		// READY 状态下踢人 → 回 FORMING
		if team.State == stateReady {
			team.State = stateForming
		}
		result = cloneTeam(team)
		return nil
	}, ttl); err != nil {
		return nil, err
	}

	// 删 target player index。CAS:仅当索引仍指向本队才删,防误删被踢者并发加入新队的归属。
	if err := u.repo.DeletePlayerIndexIfMatches(ctx, targetPlayerID, teamID); err != nil {
		plog.With(ctx).Warnw("msg", "team_kick_delete_player_index_failed", "player_id", targetPlayerID, "team_id", teamID, "err", err)
	}

	// 匹配联动:被踢成员若正在排队/确认期 → 撤销整张票据(best-effort,不阻断踢人)
	u.cancelMatchmaking(ctx, teamID, targetPlayerID)

	// 人数变了(满员队踢掉一人后重新开放招募)→ 同步开放队伍索引。
	u.syncOpenIndex(ctx, result, result.MapId)

	// push 给剩余成员 + 被踢者(不发给 captain — 原则 2)
	recipients := append(memberIDs(result), targetPlayerID)
	u.pushUpdate(ctx, captainID, recipients, result,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_KICKED, 0)

	plog.With(ctx).Debugw("msg", "team_kick", "team_id", teamID, "captain_id", captainID,
		"target_player_id", targetPlayerID)
	return result, nil
}

// SetReady 设置玩家 ready 状态,并可选更换英雄。
func (u *TeamUsecase) SetReady(ctx context.Context, teamID, playerID uint64, ready bool, heroID uint32) (*teamv1.TeamStorageRecord, error) {
	ttl := u.activeTTL()
	var result *teamv1.TeamStorageRecord

	if err := u.repo.UpdateWithLock(ctx, teamID, u.cfg.OptimisticRetry, func(team *teamv1.TeamStorageRecord) error {
		if team.State == stateDisbanded {
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if team.State != stateForming && team.State != stateReady {
			return errcode.New(errcode.ErrTeamWrongState, "team %d state %d not allows set_ready", teamID, team.State)
		}

		idx := memberIndex(team.Members, playerID)
		if idx < 0 {
			return errcode.New(errcode.ErrTeamNotFound, "player %d not in team %d", playerID, teamID)
		}

		team.Members[idx].Ready = ready
		if heroID > 0 {
			team.Members[idx].HeroId = heroID
		}
		team.UpdatedAtMs = time.Now().UnixMilli()

		// 全员 ready → 切 READY
		if ready && allReady(team.Members) {
			team.State = stateReady
		} else if !ready && team.State == stateReady {
			// 任一成员取消 ready → 回 FORMING
			team.State = stateForming
		}

		result = cloneTeam(team)
		return nil
	}, ttl); err != nil {
		return nil, err
	}

	reason := teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_READY
	if heroID > 0 {
		reason = teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_HERO_CHANGED
	}
	// push 给其他成员(不发给自己 — 原则 2)
	u.pushUpdate(ctx, playerID, memberIDs(result), result, reason, 0)

	// FORMING ↔ READY 会改变"是否还在招募"(只有 FORMING 才进列表)→ 同步索引。
	u.syncOpenIndex(ctx, result, result.MapId)

	plog.With(ctx).Debugw("msg", "team_set_ready", "team_id", teamID, "player_id", playerID,
		"ready", ready, "new_state", result.State)
	return result, nil
}

// GetTeam 读取队伍快照(只读,不走 WATCH)。
func (u *TeamUsecase) GetTeam(ctx context.Context, teamID uint64) (*teamv1.TeamStorageRecord, error) {
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errcode.New(errcode.ErrTeamNotFound, "team %d not found", teamID)
	}
	return team, nil
}

// GetMyTeam 查询玩家当前所在队伍(只读,登录后进大厅时调用)。
// 返回 (record, hasTeam, err):没队伍是正常态,hasTeam=false 且 err=nil。
// 索引命中但队伍记录已过期/已解散时,顺手清掉脏索引(否则玩家会被
// ClaimPlayer SETNX 挡住无法再建队,不变量 §1 的残留侧漏洞)。
func (u *TeamUsecase) GetMyTeam(ctx context.Context, playerID uint64) (*teamv1.TeamStorageRecord, bool, error) {
	teamID, found, err := u.repo.GetPlayerTeamID(ctx, playerID)
	if err != nil {
		return nil, false, err
	}
	if !found {
		return nil, false, nil
	}

	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		return nil, false, err
	}
	if !found || team.State == stateDisbanded {
		// TTL 竞态残留:索引还在但队伍已没/已解散 → 按无队伍处理并清索引。
		// CAS:仅当索引仍指向该孤儿 teamID 才删,防误删玩家并发建队/入队刚写入的新归属。
		if err := u.repo.DeletePlayerIndexIfMatches(ctx, playerID, teamID); err != nil {
			plog.With(ctx).Warnw("msg", "team_stale_player_index_cleanup_failed",
				"player_id", playerID, "team_id", teamID, "err", err)
		}
		return nil, false, nil
	}
	// 在线心跳:玩家仍在轮询自己的队伍 → 续期(15s 节流,best-effort)。
	// 只在 GetMyTeam(本人+索引校验过)续,GetTeam(任意 teamID)绝不续,
	// 防旁人反复读把已抛弃队伍永久续命;disbanded 分支已在上方 return,不续。
	u.maybeTouchTeam(ctx, team, playerID)
	return team, true, nil
}

// ListPendingInvites 查询发给 playerID 的未过期 pending 邀请(拉取兜底,只读)。
//
// 为什么存在:邀请令牌的唯一权威在 Redis,kafka→push 推送只是投影(不变量
// §9-22)。此前 invite_id 只能从推送获得,推送链路任一环丢帧(producer 故障窗口/
// push 副本崩溃/客户端断线)邀请就静默失效到 TTL 过期。客户端在登录、回前台、
// 打开组队 UI 时调本接口兜底,推送从「唯一通道」降级为「加速器」。
// 读取侧上限 = MaxPendingInvites(写入侧硬上限已兜住总量,单次全量返回即达标)。
func (u *TeamUsecase) ListPendingInvites(ctx context.Context, playerID uint64) ([]*data.InviteRecord, error) {
	return u.repo.ListPendingInvites(ctx, playerID, u.cfg.MaxPendingInvites)
}

// ── 找队伍:列表 / 申请 / 审批 ─────────────────────────────────────────────────
//
// 玩家没有队伍时的入口:ListOpenTeams 拉一批正在招募的队伍(全部 / 指定 map_id,上限 10),
// ApplyToTeam 申请其中一支。ApplyToTeam 按服务端配置 join_policy 走两条路径之一,
// 客户端不分叉、也不自己判定能不能进(§17.3 准入条件只有服务端一份权威判定)。

const (
	// openCandidateFactor 是候选超取倍数。索引是非权威投影,候选里可能混着已满 / 已开打 /
	// 已解散的队伍,复核会刷掉一部分;只取 limit 条会导致"明明有队伍却返回不足 limit 条"。
	openCandidateFactor = 3
	// openCandidateMax 是单次候选读取的硬上限,保证单次 Redis 读与复核开销有界
	// (limit 已被 max_open_teams_per_query 钳住,这里是第二道闸)。
	openCandidateMax = 64
)

// joinPolicy 返回当前生效的入队策略。
//
// 启动时 ValidateJoinPolicy 已 fail-fast,正常永远解析成功;万一运行期配置被改坏,
// 一律退回最保守的 approval——绝不因为解析失败就把全服队伍对陌生人敞开(权限放大是
// 不可接受的失败模式,同 conf.ParseJoinPolicy 的口径)。
func (u *TeamUsecase) joinPolicy() string {
	policy, err := conf.ParseJoinPolicy(u.cfg.JoinPolicy)
	if err != nil {
		return conf.JoinPolicyApproval
	}
	return policy
}

// joinPolicyProto 把生效策略映射成客户端可见枚举(填进每份 Team 快照)。
func (u *TeamUsecase) joinPolicyProto() teamv1.TeamJoinPolicy {
	if u.joinPolicy() == conf.JoinPolicyOpen {
		return teamv1.TeamJoinPolicy_TEAM_JOIN_POLICY_OPEN
	}
	return teamv1.TeamJoinPolicy_TEAM_JOIN_POLICY_APPROVAL
}

// maxOpenTeams 返回单次列表的返回上限(读取侧上限,不变量 §9-18)。
func (u *TeamUsecase) maxOpenTeams() int {
	if u.cfg.MaxOpenTeamsPerQuery > 0 {
		return u.cfg.MaxOpenTeamsPerQuery
	}
	return 10
}

// maxApplications 返回单队 pending 申请上限(写入侧 + 读取侧共用,不变量 §9-18)。
func (u *TeamUsecase) maxApplications() int {
	if u.cfg.MaxApplicationsPerTeam > 0 {
		return u.cfg.MaxApplicationsPerTeam
	}
	return 10
}

// isOpenForRecruit 判定队伍是否"正在招募"(会出现在 ListOpenTeams 结果里)。
//
// 只认 FORMING:READY 表示全员已准备、下一步就是开局,MATCHING 已进撮合队列,
// IN_BATTLE 已在打,DISBANDED 已解散——这几种状态下把陌生人放进来都会打断队伍已有进程。
// 这是**唯一**判定口径:写索引和读复核都调它,避免"写进去的和读出来的不是同一套标准"。
func isOpenForRecruit(team *teamv1.TeamStorageRecord) bool {
	if team == nil || team.State != stateForming {
		return false
	}
	count := len(team.Members)
	return count > 0 && count < int(team.MaxSize)
}

// syncOpenIndex 把队伍在开放招募索引里的存在性同步为当前权威状态。
//
// 索引是非权威投影(不变量 §9.22):写失败只告警,**绝不回滚已提交的队伍状态机迁移**——
// 为了一个用来"找候选"的加速结构而回退已经落地的入队/离队,才是真的破坏正确性。
// score 取"现在 + active_ttl",与队伍 key 的 TTL 同源:队伍 key 到期消失时索引成员恰好
// 也过期,ZREMRANGEBYSCORE 一扫即净,不留悬挂。
func (u *TeamUsecase) syncOpenIndex(ctx context.Context, team *teamv1.TeamStorageRecord, prevMapID uint32) {
	if team == nil || team.TeamId == 0 {
		return
	}
	ttl := u.activeTTL()
	expiresAtMs := time.Now().Add(ttl).UnixMilli()
	if err := u.repo.SyncOpenTeam(ctx, team.TeamId, team.MapId, prevMapID, isOpenForRecruit(team), expiresAtMs, ttl); err != nil {
		plog.With(ctx).Warnw("msg", "team_open_index_sync_failed",
			"team_id", team.TeamId, "map_id", team.MapId, "prev_map_id", prevMapID, "err", err)
	}
}

// SetTeamMap 队长设置本队目标关卡(招募展示 + ListOpenTeams 的 map_id 筛选依据)。
//
// 不校验 map_id 是否在关卡表内:本字段只是招募标签,真正的准入判定在进入链
// (MatchService.StartMatch 已有 ERR_MATCH_INVALID_MAP),在这里再判一次就是第二份判定
// (§17.3 明确禁止)。填了非法值的后果仅限于"这支队伍出现在一个没人筛的分桶里",
// 且换图会把它从旧分桶摘掉,分桶数因此被在线队伍数有界(空 ZSET 被 Redis 自动删除)。
func (u *TeamUsecase) SetTeamMap(ctx context.Context, teamID, captainID uint64, mapID uint32) (*teamv1.TeamStorageRecord, error) {
	ttl := u.activeTTL()
	var result *teamv1.TeamStorageRecord
	var prevMapID uint32

	if err := u.repo.UpdateWithLock(ctx, teamID, u.cfg.OptimisticRetry, func(team *teamv1.TeamStorageRecord) error {
		if team.State == stateDisbanded {
			return errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
		}
		if team.CaptainId != captainID {
			return errcode.New(errcode.ErrTeamNotCaptain, "player %d is not captain of team %d", captainID, teamID)
		}
		// 已进撮合/战斗的队伍改目标关卡没有意义(本次对局的图早已定死),拒绝以免误导队员。
		if team.State != stateForming && team.State != stateReady {
			return errcode.New(errcode.ErrTeamWrongState, "team %d state %d not allows set_map", teamID, team.State)
		}

		prevMapID = team.MapId
		team.MapId = mapID
		team.UpdatedAtMs = time.Now().UnixMilli()
		result = cloneTeam(team)
		return nil
	}, ttl); err != nil {
		return nil, err
	}

	// 换桶:先摘旧 map 分桶再写新分桶(由 repo.SyncOpenTeam 内部按 prevMapID 处理)。
	u.syncOpenIndex(ctx, result, prevMapID)

	u.pushUpdate(ctx, captainID, memberIDs(result), result,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MAP_CHANGED, 0)

	plog.With(ctx).Debugw("msg", "team_set_map", "team_id", teamID, "captain_id", captainID,
		"prev_map_id", prevMapID, "map_id", mapID)
	return result, nil
}

// ListOpenTeams 列出正在招募的队伍(只读)。
//
//	mapID = 0 → 全部;mapID > 0 → 只要目标关卡等于该值的。
//	limit ≤ 0 或超过 max_open_teams_per_query → 钳到 max_open_teams_per_query(默认 10)。
//
// 两段式:先从非权威索引取候选,再逐条回权威队伍记录复核。复核不通过(已满 / 已开打 /
// 已解散 / 记录已没 / map 已改)的候选顺手从索引剔除(best-effort 自愈)。
// 因此索引脏不会让玩家看到一支实际进不去的队伍,最坏只是这一次少返几条。
//
// 单条 Get 失败(网络抖 / 该条记录 proto 损坏)只跳过该候选并计数告警,不整单失败:
// 这里返回的是候选展示列表,跳过一条不授权任何东西;真正的准入判定在 ApplyToTeam,
// 那条路径是 fail-closed 的。索引本身读失败则照常返回错误,由客户端退避重试。
func (u *TeamUsecase) ListOpenTeams(ctx context.Context, mapID uint32, limit int) ([]*teamv1.OpenTeamBrief, error) {
	maxTeams := u.maxOpenTeams()
	if limit <= 0 || limit > maxTeams {
		limit = maxTeams
	}

	candidateLimit := limit * openCandidateFactor
	if candidateLimit > openCandidateMax {
		candidateLimit = openCandidateMax
	}
	candidates, err := u.repo.ListOpenTeamIDs(ctx, mapID, candidateLimit)
	if err != nil {
		return nil, err
	}

	policy := u.joinPolicyProto()
	out := make([]*teamv1.OpenTeamBrief, 0, limit)
	skipped := 0

	for _, teamID := range candidates {
		if len(out) >= limit {
			break
		}
		team, found, gerr := u.repo.Get(ctx, teamID)
		if gerr != nil {
			skipped++
			continue
		}
		if !found || !isOpenForRecruit(team) || (mapID > 0 && team.MapId != mapID) {
			// 索引脏了。剔除时用权威记录里的 map_id(记录已没就用查询用的 mapID),
			// 保证摘的是它真正挂着的那个分桶。
			bucket := mapID
			if found {
				bucket = team.MapId
			}
			if rerr := u.repo.RemoveOpenTeamCandidate(ctx, teamID, bucket); rerr != nil {
				plog.With(ctx).Warnw("msg", "team_open_index_prune_failed",
					"team_id", teamID, "map_id", bucket, "err", rerr)
			}
			continue
		}

		out = append(out, &teamv1.OpenTeamBrief{
			TeamId:      team.TeamId,
			CaptainId:   team.CaptainId,
			MemberCount: uint32(len(team.Members)),
			MaxSize:     uint32(team.MaxSize),
			MapId:       team.MapId,
			CreatedAtMs: team.CreatedAtMs,
			JoinPolicy:  policy,
		})
	}

	if skipped > 0 {
		plog.With(ctx).Warnw("msg", "team_open_list_candidates_skipped",
			"map_id", mapID, "skipped", skipped, "returned", len(out))
	}
	return out, nil
}

// ApplyToTeam 申请加入队伍。返回 (joined, team, expiresAtMs, err):
//   - joined=true  → open 策略下已当场入队,team 为入队后完整快照;
//   - joined=false → approval 策略下已写入申请令牌,expiresAtMs 为其过期时刻。
//
// 前置校验读的是权威队伍记录(不是索引):队伍存在、未解散、正在招募、申请人不在队内。
// open 路径复用 joinTeam(与接受邀请同一入队事务,保不变量 §1)。
// approval 路径的上限校验与占位在 data 层 Lua 内原子完成,无 TOCTOU;重复申请同一队伍
// 幂等(只刷新自己那条的过期时间,不再占新名额)。
func (u *TeamUsecase) ApplyToTeam(ctx context.Context, teamID, applicantID uint64) (bool, *teamv1.TeamStorageRecord, int64, error) {
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		return false, nil, 0, err
	}
	if !found {
		return false, nil, 0, errcode.New(errcode.ErrTeamNotFound, "team %d not found", teamID)
	}
	if team.State == stateDisbanded {
		return false, nil, 0, errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
	}
	if hasMember(team, applicantID) {
		return false, nil, 0, errcode.New(errcode.ErrTeamAlreadyInTeam, "player %d already in team %d", applicantID, teamID)
	}
	if !isOpenForRecruit(team) {
		// 满员与状态不对分开报,客户端才能给出正确提示(而不是笼统的"进不去")。
		if len(team.Members) >= int(team.MaxSize) {
			return false, nil, 0, errcode.New(errcode.ErrTeamFull,
				"team %d is full (%d/%d)", teamID, len(team.Members), team.MaxSize)
		}
		return false, nil, 0, errcode.New(errcode.ErrTeamWrongState,
			"team %d state %d not recruiting", teamID, team.State)
	}

	// open 策略:当场入队。上面的读只是快速失败,真正的满员/重复入队判定在 joinTeam 的
	// WATCH/MULTI/EXEC 事务内重做一遍(读到写之间队伍可能已被别人填满)。
	if u.joinPolicy() == conf.JoinPolicyOpen {
		result, jerr := u.joinTeam(ctx, teamID, applicantID)
		if jerr != nil {
			return false, nil, 0, jerr
		}
		u.pushUpdate(ctx, applicantID, memberIDs(result), result,
			teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_JOINED, 0)
		u.syncOpenIndex(ctx, result, result.MapId)
		plog.With(ctx).Debugw("msg", "team_apply_joined_open",
			"team_id", teamID, "player_id", applicantID)
		u.logTeamComposition(ctx, result)
		return true, result, 0, nil
	}

	// approval 策略:写申请令牌,等队长审批。
	expiresAtMs, err := u.repo.ClaimApplication(ctx, teamID, applicantID, u.cfg.ApplyTTL.Std(), u.maxApplications())
	if err != nil {
		return false, nil, 0, err
	}

	// 推送只是"去重查申请列表"的提示(§9.22 权威是 ListTeamApplications),丢帧最多延迟
	// 队长看到申请,不丢申请。只发队长,不打扰其他队员。
	u.pushUpdate(ctx, applicantID, []uint64{team.CaptainId}, team,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_APPLICATION_RECEIVED, 0)

	plog.With(ctx).Debugw("msg", "team_apply_pending",
		"team_id", teamID, "player_id", applicantID, "expires_at_ms", expiresAtMs)
	return false, nil, expiresAtMs, nil
}

// ListTeamApplications 队长查本队待处理入队申请(只读,拉取兜底)。
// 申请人名单不对普通成员开放:非队长返回 ErrTeamNotCaptain(3003)。
func (u *TeamUsecase) ListTeamApplications(ctx context.Context, teamID, captainID uint64) ([]*data.ApplicationRecord, error) {
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errcode.New(errcode.ErrTeamNotFound, "team %d not found", teamID)
	}
	if team.CaptainId != captainID {
		return nil, errcode.New(errcode.ErrTeamNotCaptain, "player %d is not captain of team %d", captainID, teamID)
	}
	return u.repo.ListApplications(ctx, teamID, u.maxApplications())
}

// HandleTeamApplication 队长同意 / 拒绝一份入队申请。
//
// 定序:先用 TakeApplication 原子取走令牌,再决定做什么。这保证同一份申请只被处理一次
// (队长连点两次"同意"→ 第二次拿不到令牌,返回 3010,不会重复入队);也保证"同意"与
// "拒绝"竞争时只有一方生效。
//
// 已知取舍:accept 路径若在取走令牌后入队失败(期间队伍被填满 / 申请人已加入别队),
// 令牌不会被放回。理由是"放回一份队长已经处理过的申请"会让队长再看到一次幽灵申请,
// 比让申请人重新申请更糟(§3 宁可 fail-closed 拒一次,也不写出不自洽的状态)。
// 队长收到明确错误码,申请人的本地待处理态到期后按钮自动恢复可点。
func (u *TeamUsecase) HandleTeamApplication(ctx context.Context, teamID, captainID, applicantID uint64, accept bool) (*teamv1.TeamStorageRecord, error) {
	team, found, err := u.repo.Get(ctx, teamID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errcode.New(errcode.ErrTeamNotFound, "team %d not found", teamID)
	}
	if team.State == stateDisbanded {
		return nil, errcode.New(errcode.ErrTeamWrongState, "team %d disbanded", teamID)
	}
	if team.CaptainId != captainID {
		return nil, errcode.New(errcode.ErrTeamNotCaptain, "player %d is not captain of team %d", captainID, teamID)
	}

	taken, err := u.repo.TakeApplication(ctx, teamID, applicantID)
	if err != nil {
		return nil, err
	}
	if !taken {
		return nil, errcode.New(errcode.ErrTeamApplyNotFound,
			"application of player %d to team %d not found or expired", applicantID, teamID)
	}

	if !accept {
		// 拒绝:令牌已消耗、配额已释放,队伍状态不变。不给申请人发推送——
		// 申请人的等待本来就是有界的(令牌 TTL),到期即恢复可申请,不需要为"被拒"
		// 单开一条推送通道(§15.3 不为可能的将来预留机制)。
		plog.With(ctx).Debugw("msg", "team_application_rejected",
			"team_id", teamID, "captain_id", captainID, "applicant_id", applicantID)
		return team, nil
	}

	result, err := u.joinTeam(ctx, teamID, applicantID)
	if err != nil {
		return nil, err
	}

	// push 给全体成员(含刚入队的申请人;不发给队长自己 — 原则 2)。
	u.pushUpdate(ctx, captainID, memberIDs(result), result,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_JOINED, 0)
	u.syncOpenIndex(ctx, result, result.MapId)

	plog.With(ctx).Debugw("msg", "team_application_accepted",
		"team_id", teamID, "captain_id", captainID, "applicant_id", applicantID)
	u.logTeamComposition(ctx, result)
	return result, nil
}

// claimPlayerHealingOrphan 原子声明 player→teamID 归属(SETNX,不变量 §1),并对
// 孤儿索引自愈:索引虽在但其指向的队伍主体已过期/解散(TTL 竞态 / 解散后删索引
// 失败的悬挂残留)时,不该把玩家永久锁在不存在的队伍里,而应清掉脏索引后重新声明。
//
// 判孤儿安全前提(缺一不可):**所有写路径都先写队伍主体、后写/改索引**——
//   - CreateTeam:先 Create 主体再 claim(本函数),因此「索引指向 X 但 X 主体不在」
//     永远不会是另一个 CreateTeam 的 in-flight 中间态;
//   - AcceptInvite:claim 时目标队伍主体必已存在(邀请的前提)。
//
// 若有人改成「先 claim 后写主体」,本函数会把 in-flight claim 误判孤儿并删掉,
// 造成同一玩家进两支队伍 —— 违反不变量 §1,绝对禁止。
//
// 并发安全:
//   - SETNX 成功 → 直接返回(常态)。
//   - 声明失败且现有队伍真实存在且未解散 → 真冲突,返回 3004。
//   - 声明失败但现有队伍主体已没/已解散 → 孤儿:用 DeletePlayerIndexIfMatches(CAS)
//     仅当索引仍指向该孤儿 teamID 时才删(防误删其他请求刚写入的新 claim),再重试一次
//     SETNX;若重试仍撞占用(他人抢先真建队)→ 诚实返回 3004。
func (u *TeamUsecase) claimPlayerHealingOrphan(ctx context.Context, playerID, teamID uint64, ttl time.Duration) error {
	existTeamID, claimed, err := u.repo.ClaimPlayer(ctx, playerID, teamID, ttl)
	if err != nil {
		return err
	}
	if claimed {
		return nil
	}

	// 声明失败:核对现有队伍是否真实存在。存在且未解散 = 真冲突。
	existTeam, found, err := u.repo.Get(ctx, existTeamID)
	if err != nil {
		return err
	}
	if found && existTeam.State != stateDisbanded {
		// 真冲突(玩家确在他队):排查「为什么玩家进不去队」时需要知道他当前卡在哪支队,
		// 而 access log 只有错误码 3004。DEBUG 级即可(正常业务拒绝,量不大)。
		plog.With(ctx).Debugw("msg", "team_claim_conflict",
			"player_id", playerID, "existing_team_id", existTeamID, "existing_state", existTeam.State)
		return errcode.New(errcode.ErrTeamAlreadyInTeam, "player %d already in team %d", playerID, existTeamID)
	}

	// 孤儿索引:队伍主体已没/已解散。CAS 清掉脏索引(仅当仍指向该 teamID)后重试一次声明。
	if err := u.repo.DeletePlayerIndexIfMatches(ctx, playerID, existTeamID); err != nil {
		plog.With(ctx).Warnw("msg", "team_orphan_player_index_cleanup_failed",
			"player_id", playerID, "team_id", existTeamID, "err", err)
		return err
	}
	plog.With(ctx).Infow("msg", "team_orphan_player_index_healed",
		"player_id", playerID, "stale_team_id", existTeamID)

	retryTeamID, claimed, err := u.repo.ClaimPlayer(ctx, playerID, teamID, ttl)
	if err != nil {
		return err
	}
	if !claimed {
		// 清理与重试之间有人抢先真建队 → 诚实报冲突(自愈后仍撞上,属并发竞争)。
		plog.With(ctx).Debugw("msg", "team_claim_conflict_after_heal",
			"player_id", playerID, "existing_team_id", retryTeamID)
		return errcode.New(errcode.ErrTeamAlreadyInTeam, "player %d already in team %d", playerID, retryTeamID)
	}
	return nil
}

// ── 匹配联动辅助 ──────────────────────────────────────────────────────────────

// cancelMatchmaking 成员离开队伍(主动离队 / 被踢)后,best-effort 撤销其所在的
// matchmaker 匹配票据。修复原 TODO"排队中离队不取消票据"的跨服务不一致:
// 不撤销时票据里仍含已离队成员,成局会把他拉进战斗;其残留 claim 也会阻塞他加入的
// 新队伍 StartMatch(4002)。
//
// 弱依赖语义:
//   - matchCanceler 为 nil(未配 matchmaker_addr / 骨架联调)→ 跳过,行为与历史一致;
//   - ErrMatchNotFound(4004)= 该成员本就没在排队,常态,静默;
//   - 其余错误仅 Warn 不阻断离队(残留票据由确认期超时 / TTL 兜底回收)。
func (u *TeamUsecase) cancelMatchmaking(ctx context.Context, teamID, playerID uint64) {
	if u.matchCanceler == nil {
		return
	}
	if err := u.matchCanceler.CancelMatch(ctx, playerID); err != nil {
		if errcode.As(err) == errcode.ErrMatchNotFound {
			return // 未在排队,常态
		}
		plog.With(ctx).Warnw("msg", "team_cancel_matchmaking_failed",
			"team_id", teamID, "player_id", playerID, "err", err)
		return
	}
	plog.With(ctx).Debugw("msg", "team_matchmaking_cancelled_on_leave",
		"team_id", teamID, "player_id", playerID)
}

// ── push 辅助 ─────────────────────────────────────────────────────────────────

// pushUpdate 把 TeamUpdateEvent marshal 后调 pusher.PushTeamUpdate。
// pusher 为 nil 时(Phase 2 骨架阶段)直接跳过。
//
// 每个接收方单独序列化一条 TeamUpdateEvent,使 to_player_id 字段精确标识接收方。
// kafka key = player_id(不变量 §9)由 PushToPlayers 内部保证;
// PushToPlayers 内部同时排除 callerPlayerID(原则 2)。
func (u *TeamUsecase) pushUpdate(
	ctx context.Context,
	callerPlayerID uint64,
	toPlayerIDs []uint64,
	team *teamv1.TeamStorageRecord,
	reason teamv1.TeamUpdateReason,
	inviteID uint64,
) {
	if u.pusher == nil || len(toPlayerIDs) == 0 {
		return
	}

	now := time.Now().UnixMilli()
	protoTeam := u.teamToProto(team)

	for _, pid := range toPlayerIDs {
		event := &teamv1.TeamUpdateEvent{
			Team:       protoTeam,
			ByPlayerId: callerPlayerID,
			ToPlayerId: pid, // 每条消息精确标识接收方,客户端可直接读取
			TsMs:       now,
			Reason:     reason,
			InviteId:   inviteID,
		}
		payload, err := proto.Marshal(event)
		if err != nil {
			plog.With(ctx).Warnw("msg", "team_push_marshal_failed",
				"team_id", team.GetTeamId(), "to_player_id", pid, "reason", reason.String(), "err", err)
			continue
		}
		// PushToPlayers 内部跳过 callerPlayerID == pid 的情况(原则 2)
		if _, err := u.pusher.PushTeamUpdate(ctx, callerPlayerID, []uint64{pid}, payload); err != nil {
			if inviteID != 0 {
				// legacy 路径承载邀请的推送丢了 → 计入邀请推送失败指标(可告警,
				// 不再只靠 Warn 日志;被邀请人靠 ListMyPendingInvites 拉取兜底)。
				InvitePushFailed.WithLabelValues("legacy").Inc()
			}
			plog.With(ctx).Warnw("msg", "team_push_failed",
				"team_id", team.GetTeamId(), "to_player_id", pid, "reason", reason.String(), "err", err)
		}
	}
}

// pushInvite 构造独立的 TeamInviteEvent 并以 event_type=INVITE(=1)推送给被邀请人。
// 由 Invite 按 InvitePushMode 调用(dual/dedicated 会调,legacy 不调)。
func (u *TeamUsecase) pushInvite(ctx context.Context, inviterID, targetPlayerID, teamID, inviteID uint64) {
	// 未装配推送器或接收方无效时无法投递,直接保持邀请主流程成功。
	if u.pusher == nil || targetPlayerID == 0 {
		return
	}
	// now 统一作为事件产生时间和过期时间的计算基准,避免两次取时产生偏差。
	now := time.Now().UnixMilli()
	// event 只携带邀请弹框所需的最小字段,不再附带完整队伍快照。
	event := &teamv1.TeamInviteEvent{
		TeamId:      teamID,
		InviteId:    inviteID,
		InviterId:   inviterID,
		ToPlayerId:  targetPlayerID,
		TsMs:        now,
		ExpiresAtMs: now + u.cfg.InviteTTL.Std().Milliseconds(),
	}
	// payload 是写入 Kafka 的 TeamInviteEvent protobuf 字节;err 表示序列化失败。
	payload, err := proto.Marshal(event)
	if err != nil {
		// 序列化失败只记录告警;邀请令牌已落库,不能把推送弱依赖反向变成业务失败。
		InvitePushFailed.WithLabelValues("dedicated").Inc()
		plog.With(ctx).Warnw("msg", "team_invite_marshal_failed",
			"team_id", teamID, "to_player_id", targetPlayerID, "invite_id", inviteID, "err", err)
		return
	}
	// eventTypeInvite 是客户端选择 TeamInviteEvent 反序列化器的域内判别值。
	const eventTypeInvite = uint32(teamv1.TeamPushEventType_TEAM_PUSH_EVENT_TYPE_INVITE)
	// 推送失败沿用弱依赖策略只记告警 + 计数(可告警),不改变已经落库的邀请主流程结果;
	// 丢帧由被邀请人 ListMyPendingInvites 拉取兜底。
	if _, err := u.pusher.PushTeamEvent(ctx, inviterID, []uint64{targetPlayerID}, payload, eventTypeInvite); err != nil {
		InvitePushFailed.WithLabelValues("dedicated").Inc()
		plog.With(ctx).Warnw("msg", "team_invite_push_failed",
			"team_id", teamID, "to_player_id", targetPlayerID, "invite_id", inviteID, "err", err)
	}
}

// refreshDisbandedTTL 用短 TTL 刷新已解散队伍的 key。
// 单条 EXPIRE 即可,无需再走一轮 WATCH/MULTI/EXEC 空写。
func (u *TeamUsecase) refreshDisbandedTTL(ctx context.Context, teamID uint64, ttl time.Duration) {
	if err := u.repo.ExpireTeam(ctx, teamID, ttl); err != nil {
		plog.With(ctx).Warnw("msg", "team_refresh_disbanded_ttl_failed", "team_id", teamID, "err", err)
	}
}

// ── 类型转换 ──────────────────────────────────────────────────────────────────

// teamToProto 把存储快照 TeamStorageRecord 转成客户端可见结构 Team(不变量 §9.14)。
//
// join_policy 不是存储字段,而是**每次组装时从服务端配置派生**(§9.11 派生字段服务端重算 /
// §9.22 不重复影子状态):改配置即时对全服生效,也不存在"队伍里存的策略"与配置漂移的问题。
// 因此本转换必须是 TeamUsecase 的方法而不是自由函数——离开 usecase 就拿不到权威配置,
// 只能填 UNSPECIFIED,客户端就会一直按保守策略渲染。
func (u *TeamUsecase) teamToProto(r *teamv1.TeamStorageRecord) *teamv1.Team {
	if r == nil {
		return nil
	}
	members := make([]*teamv1.TeamMember, 0, len(r.Members))
	for _, m := range r.Members {
		members = append(members, &teamv1.TeamMember{
			PlayerId: m.PlayerId,
			Nickname: m.Nickname,
			Mmr:      m.Mmr,
			Ready:    m.Ready,
			HeroId:   m.HeroId,
		})
	}
	return &teamv1.Team{
		TeamId:      r.TeamId,
		CaptainId:   r.CaptainId,
		Members:     members,
		State:       r.State,
		CreatedAtMs: r.CreatedAtMs,
		MaxSize:     r.MaxSize,
		MapId:       r.MapId,
		JoinPolicy:  u.joinPolicyProto(),
	}
}

// TeamToProto 导出供 service 层使用。
func (u *TeamUsecase) TeamToProto(r *teamv1.TeamStorageRecord) *teamv1.Team {
	return u.teamToProto(r)
}

// ── 成员辅助函数 ──────────────────────────────────────────────────────────────

func hasMember(team *teamv1.TeamStorageRecord, playerID uint64) bool {
	for _, m := range team.Members {
		if m.PlayerId == playerID {
			return true
		}
	}
	return false
}

func memberIndex(members []*teamv1.TeamMemberStorageRecord, playerID uint64) int {
	for i, m := range members {
		if m.PlayerId == playerID {
			return i
		}
	}
	return -1
}

func removeMember(members []*teamv1.TeamMemberStorageRecord, playerID uint64) []*teamv1.TeamMemberStorageRecord {
	out := make([]*teamv1.TeamMemberStorageRecord, 0, len(members))
	for _, m := range members {
		if m.PlayerId != playerID {
			out = append(out, m)
		}
	}
	return out
}

func allReady(members []*teamv1.TeamMemberStorageRecord) bool {
	if len(members) == 0 {
		return false
	}
	for _, m := range members {
		if !m.Ready {
			return false
		}
	}
	return true
}

func memberIDs(team *teamv1.TeamStorageRecord) []uint64 {
	ids := make([]uint64, 0, len(team.Members))
	for _, m := range team.Members {
		ids = append(ids, m.PlayerId)
	}
	return ids
}

func cloneTeam(team *teamv1.TeamStorageRecord) *teamv1.TeamStorageRecord {
	return proto.Clone(team).(*teamv1.TeamStorageRecord)
}
