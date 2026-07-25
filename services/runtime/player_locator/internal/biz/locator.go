// Package biz 是 player_locator 服务的业务用例层。
//
// W3 ⑤(2026-06-05):
//   - SetLocation 输入校验 + 调 redis 覆盖式写
//   - GetLocation 返回 OFFLINE 状态当 key miss(state=LOCATION_STATE_OFFLINE=1)
//   - ClearLocation 直接 Delete
//
// 不变量 §1(CLAUDE.md §9.1)"玩家只能在一个 Location":
//
//	redis hash 是单写者(SetLocation),覆盖语义 = 自动顶号;
//	W4 ⑩(2026-06-06):接 hub DS 上报后,加状态机守卫(guardTransition):
//	只有 HUB 上报来自数据面(hub DS),可能 stale;LOGIN_PENDING / MATCHING / BATTLE
//	来自可信控制面(login / matchmaker),直接顶号。HUB 上报覆盖控制面 MATCHING 时
//	返回 ErrLocatorConflict(玩家在确认期仍连 hub DS,hub DS 会持续上报 HUB,
//	必须挡住以免顶掉 matchmaker 刚写的 MATCHING)。
//	W4 ⑪(2026-06-06)BATTLE fence:补齐 W4 ⑩ 留下的 stale hub 顶掉 active BATTLE 缺口。
//	HUB 报文复用 match_id 字段作为 fence 令牌(无需改 proto):hub DS 在玩家打完一场
//	战斗、回到大厅时上报 HUB,须携带该玩家刚结束那场战斗的 match_id(从 battle DSTicket 取得)。
//	守卫在 cur.State==BATTLE 时:仅当 HUB 报文 match_id == cur.MatchID(且 != 0)才放行
//	(合法回流);match_id 不匹配 / 为 0 = 不知道 active BATTLE 的 stale hub DS,拒 ErrLocatorConflict。
//	BATTLE fence 加固(2026-07-02,docs/design/battle-reconnect.md §5):原守卫只拦 HUB 上报,
//	使得 login 断线重登降级写的 LOGIN_PENDING 能无条件顶掉 active BATTLE → matchmaker 误判
//	空闲 → 一人两处(破 §1)。现改为:cur.State==BATTLE 时只接受对局写(BATTLE 同 match
//	续期/推进、MATCHING 下一局撮合、带令牌 HUB 回流),其余写(LOGIN_PENDING 等裸登录)一律拒 ErrLocatorConflict。
package biz

import (
	"context"
	"time"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/placement"

	"github.com/luyuancpp/pandora/services/runtime/player_locator/internal/data"
)

// LocationState 是 biz 层的 location state(跟 proto enum 数值 1:1)。
const (
	LocationStateUnspecified  int32 = 0
	LocationStateOffline      int32 = 1
	LocationStateLoginPending int32 = 2
	LocationStateHub          int32 = 3
	LocationStateMatching     int32 = 4
	LocationStateBattle       int32 = 5
)

// optimisticRetry 是 SetGuarded WATCH/MULTI/EXEC 的 CAS 冲突重试次数。
const optimisticRetry = 3

// LocationInput 是 SetLocation 的入参(从 service 层 proto 翻译)。
type LocationInput struct {
	PlayerID  uint64
	State     int32
	HubPod    string
	ShardID   uint32
	MatchID   uint64
	BattlePod string
}

// LocationOutput 是 GetLocation 的出参。
type LocationOutput struct {
	State       int32
	HubPod      string
	ShardID     uint32
	MatchID     uint64
	BattlePod   string
	UpdatedAtMs int64
}

// LocatorUsecase 实现 SetLocation / GetLocation / ClearLocation。
type LocatorUsecase struct {
	repo     data.LocationRepo
	ttl      time.Duration
	presence PresenceNotifier // 可为 nil(presence 订阅推送未开启 → 纯拉模式)

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分片,位置 owner 落点观测退化为不打日志(行为不变)。
	// 分片部署时由 main 经 SetCellRouter 注入,SetLocation 写成功后额外打一条位置 owner 落点
	// 观测(核对位置落点 == 玩家 owner cell,防 §1 单写者须同 cell)。nil-safe。
	router *cellroute.Router
}

// PresenceNotifier 是 presence fan-out 入口(由 PresenceHub 实现;nil 表示未启用)。
// 见 docs/design/friend-distributed-scaling.md §13.4。
type PresenceNotifier interface {
	Notify(playerID uint64, state int32)
	Subscribe(subscriberID uint64, watchedIDs []uint64)
	Unsubscribe(subscriberID uint64)
}

// NewLocatorUsecase 构造用例。presence 可选(不传 = 未开启订阅推送,走纯拉)。
func NewLocatorUsecase(repo data.LocationRepo, ttl time.Duration, presence ...PresenceNotifier) *LocatorUsecase {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	// 脑裂再入屏障机械下限(pkg/placement 契约,2026-07-16):BATTLE presence 是
	// login/matchmaker 再入门(tryBattleReconnect/ensureNoneInBattle)的第一道信号,
	// 其 TTL 必须 ≥ DS 授权租约上限 + 偏差余量(27s),保证 presence 蒸发、各门放行时
	// 分区的旧 DS 已对存量玩家完成自我 fencing。正确性下限,配置调低机械抬回。
	if ttl < placement.DSFenceReentryBarrier {
		ttl = placement.DSFenceReentryBarrier
	}
	// u 保存钳制后的有效 TTL，后续写入不能再使用调用方传入的过短原值。
	u := &LocatorUsecase{repo: repo, ttl: ttl}
	// presence 是可选弱依赖；仅在调用方显式提供时记录首个通知器，未提供则保持纯拉模式。
	if len(presence) > 0 {
		u.presence = presence[0]
	}
	return u
}

// SetCellRouter 注入确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级架构)。
//
// nil-safe:不调用 / 传 nil 时(单 Cell / dev / 阶段 1~2),SetLocation 不做位置 owner 落点观测,
// 行为与历史一致。用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 matchmaker /
// auction / battle_result / friend / chat / trade / dialogue / inventory 一致)。Router 内部读路径无锁,并发安全。
func (u *LocatorUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// SetLocation 写入 redis hash。
//
// 校验:
//   - playerID > 0
//   - state 在合法枚举范围(0-5)
//   - state=HUB → hub_pod 非空
//   - state=MATCHING / BATTLE → match_id 非空
//   - state=BATTLE → battle_pod 非空
func (u *LocatorUsecase) SetLocation(ctx context.Context, in LocationInput) error {
	if in.PlayerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id must > 0")
	}
	if in.State < LocationStateUnspecified || in.State > LocationStateBattle {
		return errcode.New(errcode.ErrInvalidArg, "invalid state=%d", in.State)
	}
	switch in.State {
	case LocationStateHub:
		if in.HubPod == "" {
			return errcode.New(errcode.ErrInvalidArg, "HUB state requires hub_pod")
		}
	case LocationStateMatching:
		if in.MatchID == 0 {
			return errcode.New(errcode.ErrInvalidArg, "MATCHING state requires match_id")
		}
	case LocationStateBattle:
		if in.MatchID == 0 || in.BattlePod == "" {
			return errcode.New(errcode.ErrInvalidArg, "BATTLE state requires match_id + battle_pod")
		}
	}

	rec := data.LocationRecord{
		State:       in.State,
		HubPod:      in.HubPod,
		ShardID:     in.ShardID,
		MatchID:     in.MatchID,
		BattlePod:   in.BattlePod,
		UpdatedAtMs: time.Now().UnixMilli(),
	}
	// W4 ⑪:HUB 报文里的 match_id 仅作 BATTLE fence 令牌(供 guardTransition 判定),
	// 玩家进入 HUB 后已无活跃对局,不持久化 match_id/battle_pod,免其它服务误读。
	if in.State == LocationStateHub {
		rec.MatchID = 0
		rec.BattlePod = ""
	}
	if err := u.repo.SetGuarded(ctx, in.PlayerID, rec, u.ttl, optimisticRetry, guardTransition(ctx, in)); err != nil {
		return err
	}
	// presence fan-out(§13.4):写成功后通知 hub,内部转粗粒度 + 去抖 + 合并 + 只推订阅者。
	if u.presence != nil {
		u.presence.Notify(in.PlayerID, in.State)
	}
	plog.With(ctx).Infow("msg", "location_set",
		"player_id", in.PlayerID, "state", in.State,
		"hub_pod", in.HubPod, "match_id", in.MatchID, "battle_pod", in.BattlePod,
		"ttl_ms", u.ttl.Milliseconds())
	// 分片:位置写成功后观测本玩家位置锁定的 owner 落点(位置是 owner 数据,须锁定
	// 玩家 owner cell 以保单写者须号,不变量 §1)。router 为 nil(单 Cell)→ 不打。
	u.logLocationPlacement(ctx, in.PlayerID, in.State)
	return nil
}

// guardTransition 返回 SetGuarded 的状态机守卫闭包,实现不变量 §1。
//
// 守卫只在当前状态是 MATCHING / BATTLE(对局相关、需保护的状态)时介入:
//
// 当前 MATCHING(撮合确认期):
//   - 拒 HUB 上报 → ErrLocatorConflict。玩家在确认期物理上仍连 hub DS,hub DS 会持续上报 HUB,
//     若放行会顶掉 matchmaker 刚写的 MATCHING。其余写放行(顶号语义)。
//
// 当前 BATTLE(active 战斗,docs/design/battle-reconnect.md §5):只接受两类写,其余一律拒:
//   - BATTLE 且 match_id 相同:同局心跳续期 / 推进 → 放行。
//     不同 match_id = 旧 DS / 旧 allocator 的迟到心跳,拒 ErrLocatorConflict,
//     否则会把当前对局位置覆盖成旧对局(指向已死的旧 battle DS,破 §1)。
//   - MATCHING:对局生命周期控制面写(下一局撮合决策)→ 放行。
//     (新对局的首个 BATTLE 恒经 MATCHING 过渡,故不存在合法的 BATTLE→BATTLE 跨 match 直转。)
//   - HUB 带正确 match_id 令牌(== cur.MatchID 且 != 0):玩家打完回大厅的合法回流(W4⑪)→ 放行。
//   - 其余写(LOGIN_PENDING 裸登录/断线重登降级、无令牌 HUB)→ 拒 ErrLocatorConflict。
//     否则客户端反复重登会把 BATTLE 冲成 LOGIN_PENDING,形成抖动窗口,matchmaker 读到
//     误判空闲 → 一人两处(破 §1)。一次裸登录本就不该有权终止一场进行中的战斗。
//
// 控制面写(LOGIN_PENDING / MATCHING / BATTLE 来自 login / matchmaker)在 cur 非 MATCHING/BATTLE 时一律放行。
//
// ── 一句话速记(判断分三层)────────────────────────────────────────────
//   - 玩家原本没记录(!found)→ 首次上线,放行。
//   - 旧状态不是对局态(OFFLINE / LOGIN_PENDING / HUB)→ switch 不命中,直接放行(普通顶号覆盖)。
//   - 旧状态是对局态才拦:
//   - 旧 = MATCHING:只拦 HUB 上报(防 hub DS 把 matchmaker 刚写的确认期冲掉),其余放行。
//   - 旧 = BATTLE(最严):内层按新状态分类——
//   - 新 BATTLE 且同 match_id → 放行(心跳续期);不同 match_id → 拒(旧 DS 迟到心跳)。
//   - 新 MATCHING → 放行(下一局撮合)。
//   - 新 HUB → 带对的 match_id 令牌才放行(打完回大厅),否则拒(stale hub)。
//   - 其余(LOGIN_PENDING 等裸登录)→ 一律拒。这就是 §5 修的核心洞:防止断线重登把人从
//     战斗里顶出去,导致 matchmaker 误判空闲、一人两处。
//
// 核心心法:旧状态越"重要"(BATTLE 最重),门卫越挑剔,只放跟这局有关的写进来。
func guardTransition(ctx context.Context, in LocationInput) func(cur data.LocationRecord, found bool) error {
	// reject 统一记 WARN:这些拒绝正是不变量 §1(一人一处可操作 DS)的 fencing 事件
	// ——stale hub DS 顶掉 MATCHING、旧 DS 迟到心跳跨 match、裸登录顶掉 active BATTLE。
	// service handler 把 ErrLocatorConflict 转成 in-band Code 后返回 nil error,access log
	// 只会记 rpc_ok(DEBUG),故必须在拒绝点显式留证,否则线上出「玩家被莫名踢出战斗 /
	// 顶号」类问题时无日志可查(本服务最大盲点)。
	reject := func(reason string, cur data.LocationRecord) {
		plog.With(ctx).Warnw("msg", "locator_guard_rejected",
			"reason", reason, "player_id", in.PlayerID,
			"cur_state", cur.State, "cur_match_id", cur.MatchID,
			"new_state", in.State, "fence_match_id", in.MatchID, "hub_pod", in.HubPod)
	}
	return func(cur data.LocationRecord, found bool) error {
		if !found {
			return nil
		}
		switch cur.State {
		case LocationStateMatching:
			// 撮合确认期只拦可能 stale 的 hub DS 上报。
			if in.State == LocationStateHub {
				reject("stale_hub_during_matching", cur)
				return errcode.New(errcode.ErrLocatorConflict,
					"player %d in MATCHING(match_id=%d), reject stale HUB report pod=%s",
					in.PlayerID, cur.MatchID, in.HubPod)
			}
		case LocationStateBattle:
			switch in.State {
			case LocationStateBattle:
				// 同局心跳续期放行;不同 match_id = 旧 DS / 旧 allocator 的迟到心跳,
				// 拒之以免把当前对局位置覆盖成旧对局(指向已死旧 DS,破 §1)。
				if in.MatchID != cur.MatchID {
					reject("battle_write_different_match", cur)
					return errcode.New(errcode.ErrLocatorConflict,
						"player %d in BATTLE(match_id=%d), reject BATTLE write for different match_id=%d",
						in.PlayerID, cur.MatchID, in.MatchID)
				}
			case LocationStateMatching:
				// matchmaker 控制面写下一局撮合,放行。
			case LocationStateHub:
				// hub 回流必须带当前战斗的 match_id 令牌。
				if in.MatchID == 0 || in.MatchID != cur.MatchID {
					reject("stale_hub_during_battle", cur)
					return errcode.New(errcode.ErrLocatorConflict,
						"player %d in BATTLE(match_id=%d), reject stale HUB report pod=%s fence_match_id=%d",
						in.PlayerID, cur.MatchID, in.HubPod, in.MatchID)
				}
			default:
				// LOGIN_PENDING 等裸写无对局上下文,不得顶掉 active BATTLE。
				reject("bare_write_evicts_active_battle", cur)
				return errcode.New(errcode.ErrLocatorConflict,
					"player %d in BATTLE(match_id=%d), reject non-battle write state=%d (bare login/reconnect cannot evict active battle)",
					in.PlayerID, cur.MatchID, in.State)
			}
		}
		return nil
	}
}

// GetLocation 读 redis hash;key 不存在返回 OFFLINE 占位记录(不报错)。
func (u *LocatorUsecase) GetLocation(ctx context.Context, playerID uint64) (LocationOutput, error) {
	if playerID == 0 {
		return LocationOutput{}, errcode.New(errcode.ErrInvalidArg, "player_id must > 0")
	}
	rec, found, err := u.repo.Get(ctx, playerID)
	if err != nil {
		return LocationOutput{}, err
	}
	if !found {
		// 不变量 §1:不存在等价 OFFLINE,客户端 / DS 看到这个就知道"玩家不在线"
		return LocationOutput{State: LocationStateOffline}, nil
	}
	return LocationOutput{
		State:       rec.State,
		HubPod:      rec.HubPod,
		ShardID:     rec.ShardID,
		MatchID:     rec.MatchID,
		BattlePod:   rec.BattlePod,
		UpdatedAtMs: rec.UpdatedAtMs,
	}, nil
}

// BatchGetLocation 一次查多个玩家位置(好友列表在线态批量拉,
// 见 docs/design/friend-distributed-scaling.md §13.3 BatchGetPresence)。
//
// 语义与 GetLocation 一致但不给 miss 回填 OFFLINE 占位:返回 map 只含在 redis
// 查到的玩家;未在线 / 不存在的 player_id 不出现在 map 里(调用方按缺席判离线,
// 避免响应被大量离线占位撞胀)。player_id==0 与重复 id 由 data 层跳过 / 去重。
func (u *LocatorUsecase) BatchGetLocation(ctx context.Context, playerIDs []uint64) (map[uint64]LocationOutput, error) {
	if len(playerIDs) == 0 {
		return map[uint64]LocationOutput{}, nil
	}
	recs, err := u.repo.BatchGet(ctx, playerIDs)
	if err != nil {
		return nil, err
	}
	out := make(map[uint64]LocationOutput, len(recs))
	for pid, rec := range recs {
		out[pid] = LocationOutput{
			State:       rec.State,
			HubPod:      rec.HubPod,
			ShardID:     rec.ShardID,
			MatchID:     rec.MatchID,
			BattlePod:   rec.BattlePod,
			UpdatedAtMs: rec.UpdatedAtMs,
		}
	}
	return out, nil
}

// RefreshHubLocations 批量续期一批玩家的 HUB 位置 TTL(在线保活)。
//
// 调用链:Hub DS Heartbeat(每 5s,捎带在场 player_ids)→ hub_allocator → 本方法。
// 只有「state==HUB 且 hub_pod==本次上报 pod」的记录才续期(data 层校验),
// MATCHING/BATTLE/其它 pod 的记录一律不动(不变量 §1,对局态由战斗链路权威维护)。
// 玩家掉线/拔线 → Hub DS 停报该 id → key 30s 自然过期 = 好友视角离线。
// 不触发 presence 通知(续期不是状态变更)。
func (u *LocatorUsecase) RefreshHubLocations(ctx context.Context, hubPod string, playerIDs []uint64) (int, error) {
	if hubPod == "" {
		return 0, errcode.New(errcode.ErrInvalidArg, "hub_pod must not be empty")
	}
	if len(playerIDs) == 0 {
		return 0, nil
	}
	refreshed, err := u.repo.RefreshHubLocations(ctx, hubPod, playerIDs, u.ttl)
	if err != nil {
		return 0, err
	}
	return refreshed, nil
}

// disconnectGrace 是快速断线上报后的宽限期:真退出的玩家 ~10s 内判离线(不等满 30s
// 心跳 TTL);窗口内重连 → PostLogin SetLocationHub 重写记录,状态自愈。
// 绝不即时置 OFFLINE:玩家 travel 去战斗也触发 Hub Logout,靠 grace + 守卫免疫误判。
const disconnectGrace = 10 * time.Second

// ReportDisconnect 快速断线上报:Hub DS 在玩家 Logout / 连接超时断开时调用,
// 把该玩家 HUB 位置的 TTL 缩短到 disconnectGrace。守卫在 data 层:只缩
// 「state==HUB 且 hub_pod 匹配」且只缩不涨(EXPIRE LT)。
// 不触发 presence 通知——缩 TTL 不是状态变更,真离线由 key 过期体现。
func (u *LocatorUsecase) ReportDisconnect(ctx context.Context, hubPod string, playerID uint64) (bool, error) {
	if hubPod == "" {
		return false, errcode.New(errcode.ErrInvalidArg, "hub_pod must not be empty")
	}
	if playerID == 0 {
		return false, errcode.New(errcode.ErrInvalidArg, "player_id must > 0")
	}
	shrunk, err := u.repo.ShrinkHubTTL(ctx, hubPod, playerID, disconnectGrace)
	if err != nil {
		return false, err
	}
	plog.With(ctx).Infow("msg", "location_disconnect_reported",
		"player_id", playerID, "hub_pod", hubPod, "shrunk", shrunk,
		"grace_ms", disconnectGrace.Milliseconds())
	return shrunk, nil
}

// ClearLocation Unlink redis hash。
func (u *LocatorUsecase) ClearLocation(ctx context.Context, playerID uint64) error {
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id must > 0")
	}
	if err := u.repo.Delete(ctx, playerID); err != nil {
		return err
	}
	// presence fan-out(§13.4):清位置等价离线,通知 hub。
	if u.presence != nil {
		u.presence.Notify(playerID, LocationStateOffline)
	}
	plog.With(ctx).Infow("msg", "location_cleared", "player_id", playerID)
	return nil
}

// SubscribePresence 注册订阅者关注的一批好友在线态(§13.4.1)。
// presence 未启用时为 no-op(纯拉模式),不报错。
func (u *LocatorUsecase) SubscribePresence(subscriberID uint64, watchedIDs []uint64) error {
	if subscriberID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "subscriber_id must > 0")
	}
	if u.presence != nil {
		u.presence.Subscribe(subscriberID, watchedIDs)
	}
	return nil
}

// UnsubscribePresence 退订(关闭好友面板时)。presence 未启用时为 no-op。
func (u *LocatorUsecase) UnsubscribePresence(subscriberID uint64) error {
	if subscriberID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "subscriber_id must > 0")
	}
	if u.presence != nil {
		u.presence.Unsubscribe(subscriberID)
	}
	return nil
}
