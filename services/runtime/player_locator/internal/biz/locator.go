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

// DepartureNotifier 把「Hub DS 观测到某玩家离开大厅」发成服务间事件
// (topic pandora.player.presence,payload=PlayerLeftHubEvent)。
//
// nil = 未开启(departure_event.enabled=false / 未配 kafka):此时 last-seen 时刻照常
// 记录,只是没有实时触发器,消费方退化为「下次读到该实体时顺手复查」的兜底路径。
// 这条降级是刻意保留的:事件流是加速器,权威始终是 locator 的查询接口。
type DepartureNotifier interface {
	// NotifyLeftHub best-effort 投递;返回 error 只用于打日志,绝不阻断 ReportDisconnect
	// (断线上报本身是尽力而为的在线态优化,不能因为 kafka 抖动就把它变成失败)。
	NotifyLeftHub(ctx context.Context, playerID uint64, leftAtMs int64, hubPod string) error
}

// LocatorUsecase 实现 SetLocation / GetLocation / ClearLocation。
type LocatorUsecase struct {
	repo     data.LocationRepo
	ttl      time.Duration
	presence PresenceNotifier // 可为 nil(presence 订阅推送未开启 → 纯拉模式)

	// lastSeenRetention 是 last-seen 时刻的保留时长,必须**远大于**所有消费方的离线阈值
	// (当前最长是 team 的 offline_leave.threshold=180s),否则玩家还没到阈值、时刻先过期了,
	// 消费方查到 UNKNOWN 就永远不会动作。默认 1h,由 conf 注入。
	// 容量有界(§9.18):一次断线一个小 key,retention 到期自动消失,不随 DAU 累积。
	lastSeenRetention time.Duration

	// departure 是离场事件出口,可为 nil(见 DepartureNotifier 注释)。nil-safe。
	departure DepartureNotifier

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
	u := &LocatorUsecase{repo: repo, ttl: ttl, lastSeenRetention: defaultLastSeenRetention}
	// presence 是可选弱依赖；仅在调用方显式提供时记录首个通知器，未提供则保持纯拉模式。
	if len(presence) > 0 {
		u.presence = presence[0]
	}
	return u
}

// SetLastSeenRetention 覆盖 last-seen 时刻的保留时长(conf 注入)。
// <=0 视为不改(保持默认),避免配置缺字段时把保留期设成 0 让整条链静默失效。
func (u *LocatorUsecase) SetLastSeenRetention(d time.Duration) {
	if d > 0 {
		u.lastSeenRetention = d
	}
}

// SetDepartureNotifier 注入离场事件出口(nil = 不发事件,见 DepartureNotifier 注释)。
// 用 setter 而非构造参数,与 SetCellRouter / SetMatchCanceler 等既有形态一致。
func (u *LocatorUsecase) SetDepartureNotifier(n DepartureNotifier) {
	u.departure = n
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
	// 玩家回到 Hub → 清掉上一次的离开时刻,维持不变量:
	//   **last-seen 存在 ⟺ 离开 Hub 后还没回来过**。
	//
	// 为什么必须清(「断线→秒重连」的真实故障链):留着旧时刻本身不会立刻出错,因为消费方
	// 永远先看「此刻是否在线」;但等到下一次掉线**恰好没能写新时刻**时就会出事 ——
	// Hub DS 整台挂掉时压根不会调 ReportDisconnect,此时玩家位置 key 自然过期(离线),
	// 而权威侧还留着半小时前那次重连前的旧时刻 → 消费方算出「已离线半小时」→
	// 把刚掉线 10 秒的玩家立刻踢出队伍,180s 宽限形同虚设。
	//
	// 只在 HUB 分支清,不是图省事:last-seen 只可能由「离开 HUB」写出(ShrinkHubTTL 只缩
	// HUB 记录),而玩家要再次离开就必须先回到 HUB,所以 HUB 这一处就覆盖了全部路径。
	// 反过来若无条件清,会给 BATTLE 心跳链路(ds_allocator 每 5s 每人一次 SetLocation)
	// 平白加一次 Redis 往返,纯浪费。
	//
	// best-effort:清失败只告警。失效方向是「退回本次修复前的行为」,而不是引入新错误。
	if in.State == LocationStateHub {
		if cerr := u.repo.ClearLastSeen(ctx, in.PlayerID); cerr != nil {
			plog.With(ctx).Warnw("msg", "location_last_seen_clear_failed",
				"player_id", in.PlayerID, "hub_pod", in.HubPod, "err", cerr)
		}
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

// defaultLastSeenRetention 是 last-seen 时刻的默认保留时长。
// 取值依据:必须远大于所有消费方的离线阈值(当前最长 team 的 180s),留足一个数量级的
// 余量以容纳消费方重启 / 积压补跑;同时不能大到让 key 无谓堆积(一次断线一个小 key)。
// 1h 同时也大于 team active_ttl 之外任何合理阈值,新增消费方若要更长阈值,
// 必须同步调大 locator.last_seen_retention,否则会查到 UNKNOWN 而永不动作。
const defaultLastSeenRetention = time.Hour

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
	// 顺序铁律:**先守卫缩 TTL,成功后才记时刻 / 发事件**,两步刻意不做成一个原子操作。
	//
	// ① 只在 shrunk==true 时才记:ShrinkHubTTL 的 Lua 守卫(state==HUB 且 pod 匹配)
	//    才是「他确实离开了大厅」的唯一判据。守卫没过意味着这是 travel 去战斗、切线后
	//    旧 pod 的迟到报文、或记录早已不在——那些都不该在权威侧留下「离开时刻」,
	//    否则会让一个其实在线的玩家在下次真离线时被算成已离线很久,提前踢人。
	// ② 顺序不能反:先记时刻再缩 TTL,进程恰好在中间挂掉就会留下无守卫背书的时刻。
	//    按现在的顺序,中间挂掉最多是「缩了 TTL 但没时刻」——消费方查到 UNKNOWN 一律
	//    不动作(§9.22),失效方向是安全的那一侧。
	// ③ 两步失败都不回滚、也不阻断本 RPC:断线上报本身是尽力而为的在线态优化,
	//    不能因为 Redis / kafka 抖动就把它变成失败(那会让 Hub DS 反复重试真正重要的
	//    TTL 收缩)。丢了的后果是这一次掉线少一个加速信号,兜底复查路径仍然成立。
	nowMs := time.Now().UnixMilli()
	if shrunk {
		if serr := u.repo.SetLastSeen(ctx, playerID, nowMs, u.lastSeenRetention); serr != nil {
			plog.With(ctx).Warnw("msg", "location_last_seen_write_failed",
				"player_id", playerID, "hub_pod", hubPod, "err", serr)
		}
		if u.departure != nil {
			if nerr := u.departure.NotifyLeftHub(ctx, playerID, nowMs, hubPod); nerr != nil {
				plog.With(ctx).Warnw("msg", "location_departure_event_failed",
					"player_id", playerID, "hub_pod", hubPod, "err", nerr)
			}
		}
	}
	plog.With(ctx).Infow("msg", "location_disconnect_reported",
		"player_id", playerID, "hub_pod", hubPod, "shrunk", shrunk,
		"grace_ms", disconnectGrace.Milliseconds())
	return shrunk, nil
}

// BatchGetLastSeen 批量查「最后一次被观测到离开 Hub 的时刻」(unix ms)。
//
// 返回 map 只含有记录的玩家;缺席 = UNKNOWN。调用方必须与 BatchGetLocation 合用:
// 先确认此刻查不到位置(离线),再看 last-seen 判断离开了多久;单看本接口不能判离线
// (玩家可能已经回来了,而上一次的离开时刻还在保留期内没过期)。
func (u *LocatorUsecase) BatchGetLastSeen(ctx context.Context, playerIDs []uint64) (map[uint64]int64, error) {
	if len(playerIDs) == 0 {
		return map[uint64]int64{}, nil
	}
	return u.repo.BatchGetLastSeen(ctx, playerIDs)
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
