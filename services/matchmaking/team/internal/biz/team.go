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
func (u *TeamUsecase) maybeTouchTeam(ctx context.Context, teamID, playerID uint64) {
	now := time.Now()
	if v, ok := u.lastTouch.Load(playerID); ok {
		if last, ok2 := v.(time.Time); ok2 && now.Sub(last) < touchInterval {
			return
		}
	}
	u.lastTouch.Store(playerID, now)
	u.maybeSweepLastTouch(now)
	if err := u.repo.TouchTeam(ctx, teamID, playerID, u.activeTTL()); err != nil {
		plog.With(ctx).Warnw("msg", "team_touch_failed",
			"player_id", playerID, "team_id", teamID, "err", err)
	}
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

	// 2. 原子声明 playerID 归属(SETNX),保证不变量 §1:一人只能在一个队。
	//    必须在改成员列表前声明,杜绝两个并发 AcceptInvite 把同一玩家加进两个队的 TOCTOU。
	//    孤儿索引(索引指向的队伍主体已过期/解散)会自愈,不误拦成 3004。
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
		// 入队失败(满员/解散/冲突),回滚 claim 释放玩家。CAS:仅当索引仍指向本队才删,
		// 防误删并发路径刚写入的新归属。
		_ = u.repo.DeletePlayerIndexIfMatches(ctx, playerID, teamID)
		return nil, err
	}

	// player index 已由 ClaimPlayer 在锁前原子写入,此处无需再写。

	// 删 invite 令牌(同时释放被邀请人 pending 索引配额)
	if inviteID != 0 {
		_ = u.repo.DeleteInvite(ctx, inviteID, playerID)
	}

	// push MEMBER_JOINED 给所有成员(不发给 playerID — 原则 2)
	u.pushUpdate(ctx, playerID, memberIDs(result), result,
		teamv1.TeamUpdateReason_TEAM_UPDATE_REASON_MEMBER_JOINED, 0)

	plog.With(ctx).Debugw("msg", "team_accept_invite", "team_id", teamID, "player_id", playerID)
	// 分片:成员加入后队伍 region 分布可能变跨 region(影响 §4.4 battle DS 放置)。router 为 nil → 不打。
	u.logTeamComposition(ctx, result)
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
		plog.With(ctx).Warnw("msg", "team_leave_delete_player_index_failed", "player_id", playerID, "err", err)
	}

	// 匹配联动:离队成员若正在排队/确认期 → 撤销整张票据(best-effort,不阻断离队)
	u.cancelMatchmaking(ctx, teamID, playerID)

	// 解散时用短 TTL 刷新 key
	if result.State == stateDisbanded {
		u.refreshDisbandedTTL(ctx, teamID, disbandedTTL)
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
		plog.With(ctx).Warnw("msg", "team_kick_delete_player_index_failed", "player_id", targetPlayerID, "err", err)
	}

	// 匹配联动:被踢成员若正在排队/确认期 → 撤销整张票据(best-effort,不阻断踢人)
	u.cancelMatchmaking(ctx, teamID, targetPlayerID)

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
	u.maybeTouchTeam(ctx, teamID, playerID)
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
		// 清理与重试之间有人抢先真建队 → 诚实报冲突。
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
	protoTeam := recordToProto(team)

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

// recordToProto 把 teamv1.TeamStorageRecord 转成 proto Team。
func recordToProto(r *teamv1.TeamStorageRecord) *teamv1.Team {
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
	}
}

// RecordToProto 导出供 service 层使用。
func RecordToProto(r *teamv1.TeamStorageRecord) *teamv1.Team {
	return recordToProto(r)
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
