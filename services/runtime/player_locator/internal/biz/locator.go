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

// 拒绝 reason 枚举(§11.3 R2:reason 必须是固定 snake_case 枚举串,不是自由文本)。
//
// 为什么要收成常量:这些串是日志系统按 reason 聚合告警的维度,散落字面量必然漂移
// (同一个原因两处写法不同 → 看板漏计)。取值口径 = 「触发这次拒绝的那个具体条件」,
// 一个 if 收敛 N 个条件时必须拆成 N 个(否则"被拒了"查得到、"为什么被拒"查不到)。
const (
	// SetLocation 入参形状校验。
	reasonSetPlayerIDZero        = "player_id_zero"
	reasonSetStateOutOfRange     = "state_out_of_range"
	reasonSetHubPodMissing       = "hub_pod_missing"
	reasonSetHubFenceIncomplete  = "hub_fence_incomplete"
	reasonSetMatchIDMissing      = "match_id_missing"
	reasonSetBattleTargetMissing = "battle_match_or_pod_missing"
	reasonSetHubFenceOnNonHub    = "hub_fence_on_non_hub_state"

	// HUB presence 代际闸(长 TTL meta,与 location CAS 分两步)。
	reasonSetPresenceStale      = "stale_hub_presence"
	reasonSetPresenceSuperseded = "hub_presence_superseded_before_commit"

	// ReportDisconnect / RefreshHubLocations / 查询面入参与 fence 拒绝。
	reasonDisconnectHubPodMissing  = "hub_pod_missing"
	reasonDisconnectPlayerIDZero   = "player_id_zero"
	reasonDisconnectFenceIncmplete = "hub_fence_incomplete"
	reasonDisconnectFenceMismatch  = "hub_fence_or_state_mismatch"
	reasonRefreshHubPodMissing     = "hub_pod_missing"
	reasonQueryPlayerIDZero        = "player_id_zero"
	reasonSubscriberIDZero         = "subscriber_id_zero"
)

// LocationInput 是 SetLocation 的入参(从 service 层 proto 翻译)。
type LocationInput struct {
	PlayerID         uint64
	State            int32
	HubPod           string
	ShardID          uint32
	MatchID          uint64
	BattlePod        string
	HubPresenceFence HubPresenceFence
}

// HubPresenceFence 是 Hub DS 已完成 Admission 的连接级 identity。
type HubPresenceFence struct {
	AssignmentID string
	AdmissionID  string
	AdmissionSeq uint64
}

func (f HubPresenceFence) isZero() bool {
	return f.AssignmentID == "" && f.AdmissionID == "" && f.AdmissionSeq == 0
}

func (f HubPresenceFence) isComplete() bool {
	return f.AssignmentID != "" && f.AdmissionID != "" && f.AdmissionSeq > 0
}

func (f HubPresenceFence) toData() data.HubPresenceFence {
	return data.HubPresenceFence{
		AssignmentID: f.AssignmentID,
		AdmissionID:  f.AdmissionID,
		AdmissionSeq: f.AdmissionSeq,
	}
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

// logSetRejected 记录一次 SetLocation 的提前拒绝(§11.3 R2)。
//
// 为什么必须显式打:service handler 把这些 errcode 转成 in-band Code 后返回 nil error,
// access log 只会记 rpc_ok(DEBUG);而 ErrInvalidArg / ErrLocatorConflict 都不在
// errcode.IsServerFault 里,线上默认 info 级下**一条都看不到**——玩家 presence 写不进去
// 的现场因此完全无痕。频次 = 每次被拒一条,正常链路恒 0。
func logSetRejected(ctx context.Context, reason string, in LocationInput, extra ...any) {
	kvs := []any{"msg", "locator_set_rejected",
		"reason", reason,
		"player_id", in.PlayerID,
		"presence_state", in.State,
		"hub_pod", in.HubPod, "battle_pod", in.BattlePod,
		"fence_match_id", in.MatchID,
		"assignment_id", in.HubPresenceFence.AssignmentID,
		"admission_id", in.HubPresenceFence.AdmissionID,
		"admission_seq", in.HubPresenceFence.AdmissionSeq,
	}
	plog.With(ctx).Warnw(append(kvs, extra...)...)
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
		logSetRejected(ctx, reasonSetPlayerIDZero, in)
		return errcode.New(errcode.ErrInvalidArg, "player_id must > 0")
	}
	if in.State < LocationStateUnspecified || in.State > LocationStateBattle {
		logSetRejected(ctx, reasonSetStateOutOfRange, in,
			"min_state", LocationStateUnspecified, "max_state", LocationStateBattle)
		return errcode.New(errcode.ErrInvalidArg, "invalid state=%d", in.State)
	}
	switch in.State {
	case LocationStateHub:
		if in.HubPod == "" {
			logSetRejected(ctx, reasonSetHubPodMissing, in)
			return errcode.New(errcode.ErrInvalidArg, "HUB state requires hub_pod")
		}
		if !in.HubPresenceFence.isZero() && !in.HubPresenceFence.isComplete() {
			logSetRejected(ctx, reasonSetHubFenceIncomplete, in)
			return errcode.New(errcode.ErrInvalidArg, "hub presence fence must be complete or empty")
		}
	case LocationStateMatching:
		if in.MatchID == 0 {
			logSetRejected(ctx, reasonSetMatchIDMissing, in)
			return errcode.New(errcode.ErrInvalidArg, "MATCHING state requires match_id")
		}
	case LocationStateBattle:
		if in.MatchID == 0 || in.BattlePod == "" {
			logSetRejected(ctx, reasonSetBattleTargetMissing, in)
			return errcode.New(errcode.ErrInvalidArg, "BATTLE state requires match_id + battle_pod")
		}
	}
	if in.State != LocationStateHub && !in.HubPresenceFence.isZero() {
		logSetRejected(ctx, reasonSetHubFenceOnNonHub, in)
		return errcode.New(errcode.ErrInvalidArg, "hub presence fence is only valid for HUB state")
	}

	resolvedHubFence := in.HubPresenceFence.toData()
	if in.State == LocationStateHub && in.HubPresenceFence.isZero() {
		// 旧 Hub DS 没带连接级 fence:HUB 写仍然放行(滚动升级必须能跑),但要留计数。
		// 这里只计数不打 Error —— 与 ReportDisconnect 的降级不同,写 HUB 走 legacy
		// 不会让任何功能失效,只是失去「同 pod 旧连接迟到写」的防护。真正会静默吃掉
		// 功能的是 ReportDisconnect 那一侧(见那里的说明),两条用同一个指标的不同 label
		// 便于对照:稳态下两者应同时归零,只有 set_location 有值说明协议只接了一半。
		HubPresenceLegacyDegraded.WithLabelValues(legacyOpSetLocation).Inc()
	}

	// 先只读校验长 TTL meta，再 CAS 写 location，最后 commit meta。这样 location 已过期时
	// 仍可用长 TTL meta 挡住同 assignment 的旧 admission；而 MATCHING/BATTLE guard 拒绝时 meta 零副作用。
	if in.State == LocationStateHub {
		accepted, err := u.repo.ValidateHubPresence(ctx, in.PlayerID, resolvedHubFence)
		if err != nil {
			return err
		}
		if !accepted {
			// 长 TTL meta 判定本次 HUB 写来自旧代连接(同 assignment 内 seq 回退 / 同序 ABA /
			// 已离开的 admission 重放 / fenced 当前代下的 legacy 降级写)。这是本服务
			// 「玩家秒重连后被旧连接迟到写顶回去」的唯一拦截点,必须留证。
			// 期望值(当前代)在 Lua 内比对,取不到,故并排打出本次 incoming 全量身份 +
			// 已知的当前 hub_pod,供与 locator_guard_rejected / hub_allocator 侧对账。
			logSetRejected(ctx, reasonSetPresenceStale, in)
			return errcode.New(errcode.ErrLocatorConflict,
				"player %d reject stale HUB presence assignment=%s admission_seq=%d",
				in.PlayerID, in.HubPresenceFence.AssignmentID, in.HubPresenceFence.AdmissionSeq)
		}
	}

	rec := data.LocationRecord{
		State:            in.State,
		HubPod:           in.HubPod,
		ShardID:          in.ShardID,
		MatchID:          in.MatchID,
		BattlePod:        in.BattlePod,
		UpdatedAtMs:      time.Now().UnixMilli(),
		HubPresenceFence: resolvedHubFence,
	}
	// W4 ⑪:HUB 报文里的 match_id 仅作 BATTLE fence 令牌(供 guardTransition 判定),
	// 玩家进入 HUB 后已无活跃对局,不持久化 match_id/battle_pod,免其它服务误读。
	if in.State == LocationStateHub {
		rec.MatchID = 0
		rec.BattlePod = ""
	}
	// prev* 只做观测:守卫闭包本就拿到了「写之前的当前记录」,把它记下来是为了在写成功后
	// 判断这次到底是**状态迁移**(HUB↔BATTLE↔MATCHING,低频、必须 Info)还是**同态续期**
	// (每次心跳一条、只能 Debug)。CAS 重试时守卫会被多调几次,最后一次即实际提交所依据的
	// 那份快照。纯赋值,不参与任何判定,行为与原先一字不差。
	var (
		prevFound   bool
		prevState   int32
		prevMatchID uint64
		prevHubPod  string
	)
	guard := guardTransition(ctx, in, resolvedHubFence)
	observedGuard := func(cur data.LocationRecord, found bool) error {
		prevFound, prevState, prevMatchID, prevHubPod = found, cur.State, cur.MatchID, cur.HubPod
		return guard(cur, found)
	}
	if err := u.repo.SetGuarded(ctx, in.PlayerID, rec, u.ttl, optimisticRetry, observedGuard); err != nil {
		return err
	}
	if in.State == LocationStateHub {
		committed, err := u.repo.ActivateHubPresence(ctx, in.PlayerID, resolvedHubFence, u.lastSeenRetention)
		if err != nil {
			return err
		}
		if !committed {
			// validate 后 exact Disconnect 可能先缩 TTL/写 left_at，随后本 SetGuarded 又把
			// 同 fence TTL 刷长；commit 会因 left_at 拒绝。此时必须再做一次 exact 收缩
			// 补偿。若位置已经被更新代或对局态替换，data 层 exact guard 会零副作用。
			compensated := false
			if resolvedHubFence.IsComplete() {
				if _, _, shrinkErr := u.repo.ShrinkHubTTL(
					ctx, in.HubPod, in.PlayerID, resolvedHubFence, disconnectGrace); shrinkErr != nil {
					// 补偿失败 = location TTL 被刷长了但 meta 拒绝确认:玩家的 presence 会比
					// 真实在场多活一个 TTL。这条是 ErrUnavailable(access log 会记 ERROR),
					// 但那里没有 player_id / fence,单看 access log 无法定位到人。
					logSetRejected(ctx, reasonSetPresenceSuperseded, in,
						"compensated", false, "compensate_err", shrinkErr)
					return errcode.NewCause(errcode.ErrUnavailable, shrinkErr,
						"hub presence meta commit rejected and TTL compensation failed player=%d", in.PlayerID)
				}
				compensated = true
			}
			logSetRejected(ctx, reasonSetPresenceSuperseded, in, "compensated", compensated)
			return errcode.New(errcode.ErrLocatorConflict,
				"player %d hub presence superseded before meta commit", in.PlayerID)
		}
	}
	// 非 HUB 的在线状态(MATCHING / BATTLE)走的是本路径而不是 RefreshHubLocations,
	// 必须在这里推 last_alive_ms —— 否则玩家一进战斗 meta 就不再更新,
	// 「打完一局直接退游戏」(最常见的退出方式)的离线时刻会停在很早的 Hub 阶段。
	// 已带 30s 节流(见 data.AliveTouchThrottle),BATTLE 心跳的绝大多数调用只读不写。
	// best-effort:失败只告警,退化成「时刻偏早」,方向安全(见 BatchGetLastSeen 注释)。
	if in.State == LocationStateMatching || in.State == LocationStateBattle {
		if terr := u.repo.TouchAlive(ctx, in.PlayerID, time.Now().UnixMilli(), u.lastSeenRetention); terr != nil {
			plog.With(ctx).Warnw("msg", "location_touch_alive_failed",
				"player_id", in.PlayerID, "state", in.State, "err", terr)
		}
	}

	// presence fan-out(§13.4):写成功后通知 hub,内部转粗粒度 + 去抖 + 合并 + 只推订阅者。
	if u.presence != nil {
		u.presence.Notify(in.PlayerID, in.State)
	}
	// §11.3 R1 vs R4 的分界就在这里:
	//   - **状态迁移**(首次上线 / HUB↔MATCHING↔BATTLE 互切)是低频的链路阶段推进,
	//     「presence 什么时候从 HUB 变 BATTLE」只能靠它回答 → Info,每次真实迁移一条;
	//   - **同态续期**(下面的 location_set)挂在每玩家每次心跳上 → Debug。
	// 两条分开打而不是一条带 changed 标志:前者要在生产默认 info 级下可见,后者绝不能。
	if !prevFound || prevState != in.State {
		plog.With(ctx).Infow("msg", "location_state_changed",
			"player_id", in.PlayerID,
			"prev_found", prevFound,
			"prev_presence_state", prevState, "presence_state", in.State,
			"prev_hub_pod", prevHubPod, "hub_pod", in.HubPod,
			"prev_match_id", prevMatchID, "battle_pod", in.BattlePod,
			"assignment_id", in.HubPresenceFence.AssignmentID,
			"admission_seq", in.HubPresenceFence.AdmissionSeq,
			"ttl_ms", u.ttl.Milliseconds())
	}
	// §11.3 R4:这条挂在**每玩家每次心跳**上(Hub 上报 / 对局位置续期),500 人/hub 下
	// 按 Info 打会把同文件里的 locator_guard_rejected / locator_set_rejected 彻底冲走
	// ——那些才是排障要看的行。降 Debug 是安全的:排障时对单个 pod 临时 LOG_LEVEL=debug 即可。
	// (2026-08-15 级别修正:原为 Infow。)
	// match_id 由 service 层写进 ctx(plog.WithMatchID),此处不再重复带,避免同名字段打两遍。
	plog.With(ctx).Debugw("msg", "location_set",
		"player_id", in.PlayerID, "presence_state", in.State,
		"hub_pod", in.HubPod, "battle_pod", in.BattlePod,
		"assignment_id", in.HubPresenceFence.AssignmentID,
		"admission_seq", in.HubPresenceFence.AdmissionSeq,
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
func guardTransition(ctx context.Context, in LocationInput, incomingHubFence data.HubPresenceFence) func(cur data.LocationRecord, found bool) error {
	// reject 统一记 WARN:这些拒绝正是不变量 §1(一人一处可操作 DS)的 fencing 事件
	// ——stale hub DS 顶掉 MATCHING、旧 DS 迟到心跳跨 match、裸登录顶掉 active BATTLE。
	// service handler 把 ErrLocatorConflict 转成 in-band Code 后返回 nil error,access log
	// 只会记 rpc_ok(DEBUG),故必须在拒绝点显式留证,否则线上出「玩家被莫名踢出战斗 /
	// 顶号」类问题时无日志可查(本服务最大盲点)。
	reject := func(reason string, cur data.LocationRecord, extra ...any) {
		kvs := []any{"msg", "locator_guard_rejected",
			"reason", reason, "player_id", in.PlayerID,
			"cur_state", cur.State, "presence_state", cur.State, "cur_match_id", cur.MatchID,
			"cur_hub_pod", cur.HubPod, "cur_battle_pod", cur.BattlePod,
			"new_state", in.State, "fence_match_id", in.MatchID, "hub_pod", in.HubPod,
			"cur_updated_at_ms", cur.UpdatedAtMs}
		plog.With(ctx).Warnw(append(kvs, extra...)...)
	}
	return func(cur data.LocationRecord, found bool) error {
		if !found {
			return nil
		}
		// HUB→HUB 也必须按连接代际守卫。同 Pod 秒重连时 hub_pod 不变，只有
		// assignment + admission seq/id 能阻止旧 SetLocation 迟到反向夺回投影。
		if cur.State == LocationStateHub && in.State == LocationStateHub {
			currentFence := cur.HubPresenceFence
			incomingFence := incomingHubFence
			if currentFence.IsComplete() {
				// 已经有带 fence 的当前连接,就不再接受不带 fence 的写覆盖它 ——
				// 否则一台还没升级的旧 DS 能把新连接的投影降级回「谁也说不清是哪条连接」。
				if incomingFence.IsZero() {
					reject("legacy_hub_write_downgrades_fenced_presence", cur,
						"cur_assignment_id", currentFence.AssignmentID,
						"cur_admission_id", currentFence.AdmissionID,
						"cur_admission_seq", currentFence.AdmissionSeq,
						"req_assignment_id", "", "req_admission_id", "", "req_admission_seq", uint64(0))
					return errcode.New(errcode.ErrLocatorConflict,
						"player %d reject legacy HUB write over fenced current presence", in.PlayerID)
				}
				// **只在同 assignment 内定序**:秒重连落回同一 assignment 时,靠 seq 单调
				// + 同序 admission_id 防 ABA,挡住旧连接迟到的 SetLocation 反向夺回投影。
				// 跨 assignment 不在这里判 —— 那是 hub_allocator 的归属权威说了算,
				// locator 是投影不是 owner authority(§9.22)。
				if !incomingFence.IsComplete() {
					reject("incomplete_hub_presence_fence", cur,
						"cur_assignment_id", currentFence.AssignmentID,
						"cur_admission_id", currentFence.AdmissionID,
						"cur_admission_seq", currentFence.AdmissionSeq,
						"req_assignment_id", incomingFence.AssignmentID,
						"req_admission_id", incomingFence.AdmissionID,
						"req_admission_seq", incomingFence.AdmissionSeq)
					return errcode.New(errcode.ErrLocatorConflict,
						"player %d reject incomplete HUB presence fence", in.PlayerID)
				}
				if incomingFence.AssignmentID == currentFence.AssignmentID &&
					(incomingFence.AdmissionSeq < currentFence.AdmissionSeq ||
						(incomingFence.AdmissionSeq == currentFence.AdmissionSeq &&
							incomingFence.AdmissionID != currentFence.AdmissionID)) {
					// 期望值 vs 实际值并排(§11.3 R2):只报「被拒了」查不出是哪条旧连接在迟到夺回,
					// 而这正是「玩家秒重连后又被顶回旧连接」现场的唯一判据。
					reject("stale_hub_presence_generation", cur,
						"cur_assignment_id", currentFence.AssignmentID,
						"cur_admission_id", currentFence.AdmissionID,
						"cur_admission_seq", currentFence.AdmissionSeq,
						"req_assignment_id", incomingFence.AssignmentID,
						"req_admission_id", incomingFence.AdmissionID,
						"req_admission_seq", incomingFence.AdmissionSeq)
					return errcode.New(errcode.ErrLocatorConflict,
						"player %d reject stale HUB admission assignment=%s current_seq=%d incoming_seq=%d",
						in.PlayerID, currentFence.AssignmentID,
						currentFence.AdmissionSeq, incomingFence.AdmissionSeq)
				}
			}
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
		plog.With(ctx).Warnw("msg", "locator_query_rejected",
			"reason", reasonQueryPlayerIDZero, "rpc", "GetLocation", "player_id", playerID)
		return LocationOutput{}, errcode.New(errcode.ErrInvalidArg, "player_id must > 0")
	}
	rec, found, err := u.repo.Get(ctx, playerID)
	if err != nil {
		return LocationOutput{}, err
	}
	if !found {
		// 不变量 §1:不存在等价 OFFLINE,客户端 / DS 看到这个就知道"玩家不在线"
		//
		// §11.3 R4:查询是高频路径(好友面板 / 登录路由 / 重连判定都在反复拉),只能 Debug。
		// 但必须有 —— 「重连时 locator 到底告诉调用方什么」是 key miss 被误当成「已离开旧 DS」
		// 这类事故(§9.22)的第一手证据；LOG_LEVEL=debug 单 pod 临时开即可取证。
		plog.With(ctx).Debugw("msg", "location_queried",
			"player_id", playerID, "found", false, "presence_state", LocationStateOffline)
		return LocationOutput{State: LocationStateOffline}, nil
	}
	plog.With(ctx).Debugw("msg", "location_queried",
		"player_id", playerID, "found", true, "presence_state", rec.State,
		"hub_pod", rec.HubPod, "battle_pod", rec.BattlePod,
		"loc_match_id", rec.MatchID, "updated_at_ms", rec.UpdatedAtMs)
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
		plog.With(ctx).Warnw("msg", "locator_refresh_rejected",
			"reason", reasonRefreshHubPodMissing, "requested", len(playerIDs))
		return 0, errcode.New(errcode.ErrInvalidArg, "hub_pod must not be empty")
	}
	if len(playerIDs) == 0 {
		return 0, nil
	}
	refreshed, err := u.repo.RefreshHubLocations(ctx, hubPod, playerIDs, u.ttl, u.lastSeenRetention)
	if err != nil {
		return 0, err
	}
	// §11.3 R4:Hub DS 心跳携带(每 5s 一次、一次带全场百人),只能 Debug。
	// 差集(census 在场却没被续上的名单)由 data 层按原因分类点名:
	// location_refresh_projection_missing / location_refresh_pod_mismatch(WARN,带样本)。
	plog.With(ctx).Debugw("msg", "location_hub_refreshed",
		"hub_pod", hubPod, "requested", len(playerIDs), "refreshed", refreshed,
		"ttl_ms", u.ttl.Milliseconds())
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
// 把该玩家 HUB 位置的 TTL 缩短到 disconnectGrace。守卫在 data 层同时核对
// state、pod 与 exact connection fence；同 Pod 旧连接迟到也严格零副作用。
// 不触发 presence 通知——缩 TTL 不是状态变更,真离线由 key 过期体现。
func (u *LocatorUsecase) ReportDisconnect(ctx context.Context, hubPod string, playerID uint64, fence HubPresenceFence) (bool, error) {
	if hubPod == "" {
		plog.With(ctx).Warnw("msg", "locator_disconnect_rejected",
			"reason", reasonDisconnectHubPodMissing, "player_id", playerID)
		return false, errcode.New(errcode.ErrInvalidArg, "hub_pod must not be empty")
	}
	if playerID == 0 {
		plog.With(ctx).Warnw("msg", "locator_disconnect_rejected",
			"reason", reasonDisconnectPlayerIDZero, "hub_pod", hubPod)
		return false, errcode.New(errcode.ErrInvalidArg, "player_id must > 0")
	}
	if fence.isZero() {
		// 滚动升级中的旧 Hub DS 无法证明具体连接代际。安全降级为等正常 presence
		// TTL，自身 RPC 仍返回 OK，不能再按同 pod 猜测并误伤秒重连的新连接。
		//
		// ⚠️ 这条降级**不是无害的**,必须能被发现:走到这里意味着本次断线上报被整条跳过 ——
		// 不缩 TTL(10s 快速离线判定退回 30s 心跳 TTL)、不记 last-seen、不发离场事件,
		// 于是所有按「离线满 N 秒」做决策的下游(组队自动退队等)**一个都不会触发**,
		// 而链路上每一环都返回 OK、看起来健康。这是「测试全绿但功能什么都没发生」那一类故障。
		//
		// 因此这里打 Error + 计数,而不是 Info:滚动升级窗口内偶发是预期的,
		// 稳态下持续出现 = Hub DS 根本没接这个协议,必须有人来看。
		HubPresenceLegacyDegraded.WithLabelValues(legacyOpReportDisconnect).Inc()
		plog.With(ctx).Errorw("msg", "location_disconnect_legacy_noop",
			"player_id", playerID, "hub_pod", hubPod,
			"impact", "no ttl shrink / no last-seen / no departure event: offline-duration features stay silent",
			"hint", "Hub DS must send hub_presence_fence (assignment_id + admission_id + admission_seq)")
		return false, nil
	}
	if !fence.isComplete() {
		plog.With(ctx).Warnw("msg", "locator_disconnect_rejected",
			"reason", reasonDisconnectFenceIncmplete, "player_id", playerID, "hub_pod", hubPod,
			"req_assignment_id", fence.AssignmentID, "req_admission_id", fence.AdmissionID,
			"req_admission_seq", fence.AdmissionSeq)
		return false, errcode.New(errcode.ErrInvalidArg, "hub presence fence must be complete or empty")
	}

	accepted, shrunk, err := u.repo.ShrinkHubTTL(ctx, hubPod, playerID, fence.toData(), disconnectGrace)
	if err != nil {
		return false, err
	}
	if !accepted {
		// state/pod/fence 任一不匹配：正常的迟到请求，严格不留痕。
		//
		// “不留痕”指的是 **不留副作用**,不是不留日志:这正是旧连接 / 旧 assignment 的
		// 迟到 Logout 被 fencing 掉的瞬间。RPC 仍返 OK(shrunk=false),access log 记 DEBUG
		// —— 不打这条,「断线上报到底有没有生效」在线上完全无法回答。
		// 当前代际(期望值)在 Lua 内比对取不到,故打出本次 incoming 全量身份,
		// 与同一玩家的 location_state_changed / locator_guard_rejected 对账即可定位是哪一代。
		plog.With(ctx).Warnw("msg", "locator_disconnect_rejected",
			"reason", reasonDisconnectFenceMismatch, "player_id", playerID, "hub_pod", hubPod,
			"req_assignment_id", fence.AssignmentID, "req_admission_id", fence.AdmissionID,
			"req_admission_seq", fence.AdmissionSeq,
			"grace_ms", disconnectGrace.Milliseconds())
		return false, nil
	}

	// location exact 守卫通过后才写 meta。若重连在两步之间推进了 meta，旧 fence
	// 会在 RecordLastSeen 的单 key Lua 内被拒；反之新 Set 会原子清掉旧 left_at。
	recorded, effectiveAtMs, recordErr := u.repo.RecordLastSeen(
		ctx, playerID, fence.toData(), time.Now().UnixMilli(), u.lastSeenRetention)
	if recordErr != nil {
		// TTL 已缩但没 last-seen 只会让后续动作更保守；ReportDisconnect 本身仍是
		// best-effort 优化，不把一次 Redis 部分失败升级成玩家退出失败。
		plog.With(ctx).Warnw("msg", "location_last_seen_write_failed",
			"player_id", playerID, "hub_pod", hubPod, "err", recordErr)
	} else if recorded && u.departure != nil {
		if nerr := u.departure.NotifyLeftHub(ctx, playerID, effectiveAtMs, hubPod); nerr != nil {
			plog.With(ctx).Warnw("msg", "location_departure_event_failed",
				"player_id", playerID, "hub_pod", hubPod, "err", nerr)
		}
	}
	plog.With(ctx).Infow("msg", "location_disconnect_reported",
		"player_id", playerID, "hub_pod", hubPod,
		"assignment_id", fence.AssignmentID, "admission_seq", fence.AdmissionSeq,
		"shrunk", shrunk, "last_seen_recorded", recorded,
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
		plog.With(ctx).Warnw("msg", "locator_query_rejected",
			"reason", reasonQueryPlayerIDZero, "rpc", "ClearLocation", "player_id", playerID)
		return errcode.New(errcode.ErrInvalidArg, "player_id must > 0")
	}
	if err := u.repo.Delete(ctx, playerID); err != nil {
		return err
	}
	// presence fan-out(§13.4):清位置等价离线,通知 hub。
	if u.presence != nil {
		u.presence.Notify(playerID, LocationStateOffline)
	}
	// §11.3 R1:清位置是 presence 投影的不可逆推进(登出 / 强制下线),低频 → Info。
	plog.With(ctx).Infow("msg", "location_cleared",
		"player_id", playerID, "presence_state", LocationStateOffline)
	return nil
}

// SubscribePresence 注册订阅者关注的一批好友在线态(§13.4.1)。
// presence 未启用时为 no-op(纯拉模式),不报错。
func (u *LocatorUsecase) SubscribePresence(subscriberID uint64, watchedIDs []uint64) error {
	if subscriberID == 0 {
		plog.With(context.Background()).Warnw("msg", "locator_query_rejected",
			"reason", reasonSubscriberIDZero, "rpc", "SubscribePresence",
			"watched_count", len(watchedIDs))
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
		plog.With(context.Background()).Warnw("msg", "locator_query_rejected",
			"reason", reasonSubscriberIDZero, "rpc", "UnsubscribePresence")
		return errcode.New(errcode.ErrInvalidArg, "subscriber_id must > 0")
	}
	if u.presence != nil {
		u.presence.Unsubscribe(subscriberID)
	}
	return nil
}
