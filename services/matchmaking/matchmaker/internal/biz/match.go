// Package biz 是 matchmaker 服务的业务逻辑层(W4 ①,2026-06-06)。
//
// 撮合流水线(docs/design/go-services.md §2.8):
//
//	StartMatch(team) → 写排队票据(MMR 入 ZSET)
//	   后台 RunMatchLoop:matchOnce 按 MMR 窗口贪心装箱凑齐 need=2*teamSize(teamSize 按 map_id
//	   读关卡表,如 1v1 / 5v5)→ 建 match → 进确认期
//	   ConfirmMatch:全员 accept → 拉 DS → READY;任一 reject/超时 → FAILED + 其余票据退回队列
//
// 协议铁律(docs/design/protocol-ordering-rules.md):
//   - 4 个 RPC 全"已受理型"(原则 3):客户端 UI 状态机由 pandora.match.progress push 驱动
//   - **原则 3 例外**:match 进度 push 发给所有人(含发起方),callerPlayerID=0
//   - kafka key=player_id(不变量 §9)由 PushToPlayers 保证
//
// 关键不变量(go-services.md §2.8):
//   - 同一玩家只能在一个 match 队列(ClaimPlayer SETNX)
//   - 确认期内有人拒绝 → 其他人退回队列(保留排队时长 enqueued_at_ms)
package biz

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/cellroute"
	"github.com/luyuancpp/pandora/pkg/configtable"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/placement"
	"github.com/luyuancpp/pandora/pkg/safego"
	configpb "github.com/luyuancpp/pandora/proto/gen/go/pandora/config/v1"
	matchv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/match/v1"
	teamv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/team/v1"

	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/conf"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/data"
	"github.com/luyuancpp/pandora/services/matchmaking/matchmaker/internal/model"
)

// ── 解耦接口(biz 不依赖 grpc/kafka 具体实现)─────────────────────────────────

// TeamReader 拉取 team 服务的队伍快照(StartMatch 校验 READY)。
// 实现:data.GrpcTeamReader(team 服务 gRPC client)。nil 时跳过校验。
type TeamReader interface {
	GetTeam(ctx context.Context, teamID uint64) (*teamv1.Team, bool, error)
	// BeginTeamMatch 在 team 自己的乐观锁内原子完成「校验 + 冻结名单 + 返回快照」。
	//
	// **组票必须走它,不能用只读 GetTeam**:GetTeam 与 team 侧的自动摘人(离线超时)
	// 分属两把锁,凑不出共同线性化点 —— 于是存在「读到名单 → 那个人被摘走 → 才建票」
	// 的窗口,结果是人在票据里却已不在队伍里,被拉进一场自己不在场的对局。
	// BeginTeamMatch 上的是秒级自净租约,只需覆盖到 ClaimPlayer 落地。
	// 返回的第二个值是冻结这份名单那一刻的 ready 代际(INC-20260813-001 ①):
	// 必须原样带进 match 记录,并在 EndTeamMatch 回传做 CAS。0 = 旧 team 服务没回。
	BeginTeamMatch(ctx context.Context, teamID, captainID uint64, operationID string, leaseMs int64) (*teamv1.Team, uint64, error)
	// EndTeamMatch 对局结束后复位队伍准备状态(INC-20260813-001)。
	//
	// 与 BeginTeamMatch 成对:Begin 冻结名单开一局,End 在这局释放时把队伍打回 FORMING。
	// **少了 End 那一半**,一局打完队伍仍停在 READY、全员 ready 标记原样保留,
	// 队长可以在队友还卡在结算界面 / 回大厅路上的时候立刻再开一局,把他们冻进新票据。
	//
	// 幂等:ReleaseMatch 由 battle_result outbox 重投到成功为止,本调用必然重复。
	// team 侧对「已复位 / 队伍已解散 / 成员已离队」一律返回成功,不会让 outbox 空转。
	// expectedReadyGeneration 是 Begin 时拿到的代际,team 侧据此做跨代 CAS。
	EndTeamMatch(ctx context.Context, teamID uint64, playerIDs []uint64, expectedReadyGeneration uint64) error
}

// MatchEventPusher 把 match 进度事件推给玩家(kafka pandora.match.progress)。
// 实现:kafkax.KeyOrderedProducer.PushToPlayers 包装。
type MatchEventPusher interface {
	// PushMatchProgress 向 toPlayerIDs 推送进度事件字节。
	// 原则 3 例外:match 进度发给所有人,callerPlayerID 恒传 0。
	PushMatchProgress(ctx context.Context, callerPlayerID uint64, toPlayerIDs []uint64, payload []byte) (sent int, err error)
}

// DSAllocator 申请战斗 DS（W4 ① 打桩，W4 ② 接 ds_allocator gRPC）。
type DSAllocator interface {
	// AllocateBattle 为 match 申请唯一战斗 DS。
	// mapID 是本局副本编号（客户端选择、经票据继承到 match），透传给 ds_allocator 决定 DS 加载哪张关卡；
	// 0 = 让 ds_allocator 用其默认关卡（向后兼容旧客户端 / 未选副本）。
	AllocateBattle(ctx context.Context, matchID uint64, playerIDs []uint64, mapID uint32) (*model.BattleAllocation, error)
	// AbortBattleAllocation durably compensates an allocated target before any
	// Battle ticket was published. The exact allocation operation and target are
	// payload-authenticated by the concrete service client.
	AbortBattleAllocation(ctx context.Context, matchID uint64, operationID string, allocation *model.BattleAllocation) error
	SignBattleTickets(ctx context.Context, matchID uint64, playerIDs []uint64, allocation *model.BattleAllocation) (map[uint64]string, error)

	// SignBattleTicket 给（重连 / 换设备的）玩家现签一张新的 battle DSTicket（新 jti、sub=playerID）。
	// GetMatchProgress 在 READY 阶段调用它下发票据：每次新 jti，避免复用同一张票撞 DS 侧 jti
	// 一次性防重放（换手机 / 掉线重连刚需）；票 sub 锁定调用者本人，比共享票更严。
	SignBattleTicket(ctx context.Context, playerID, matchID uint64, allocation *model.BattleAllocation) (token string, err error)
}

// CombatFactionDSAllocator 是滚动升级后的分配能力。旧实现仍可只实现 DSAllocator；
// 生产 gRPC 实现必须实现本接口，把 MatchMember.side 作为独立的 match-local 战斗阵营下发。
type CombatFactionDSAllocator interface {
	AllocateBattleWithCombatFactions(
		ctx context.Context,
		matchID uint64,
		playerIDs []uint64,
		combatFactionByPlayer map[uint64]uint32,
		mapID uint32,
	) (*model.BattleAllocation, error)
}

// LocationNotifier 把玩家位置变更上报给 player_locator（不变量 §1：玩家同一时刻只在一个 Location）。
//
// 状态权属：matchmaker 是 MATCHING / BATTLE 两个状态的权威（它掌握撮合生命周期）；
// HUB 状态由 hub DS 上报，故撮合失败 / 取消时 matchmaker 不回写 HUB（交回 hub DS）。
// 依赖强度分两类：
//   - 位置上报 NotifyMatching / NotifyBattle：弱依赖，addr 未配 → main 注入 nil，biz 检查 nil 跳过；
//     调用失败仅 Warn 不阻断撮合（上报晚一拍不影响撮合正确性）。
//   - 前置查询 IsInBattle：默认 fail-closed（生产安全），见 ensureNoneInBattle；
//     locator 未注入（nil）仍跳过，但 locator 已注入却查询失败时默认拒绝入队，
//     只有显式 BattleGateFailOpen=true（dev 弱依赖）才降级为 Warn 后放行。
type LocationNotifier interface {
	// NotifyMatching 撮合成局（进入确认期）→ 把成员标记为 MATCHING（带 match_id）。
	NotifyMatching(ctx context.Context, playerIDs []uint64, matchID uint64) error
	// NotifyBattle 全员确认 + DS 就绪 → 把成员标记为 BATTLE（带 match_id + battle_pod）。
	NotifyBattle(ctx context.Context, playerIDs []uint64, matchID uint64, battlePod string) error
	// IsInBattle 查询玩家当前是否正处于 battle DS 中（战斗中禁止重复匹配，不变量 §1）。
	// state==BATTLE 返回 true；非 BATTLE 返回 false；查询失败返回 error（由 ensureNoneInBattle
	// 按 BattleGateFailOpen 决定 fail-closed 拒绝还是 fail-open 放行，此处不吞错误）。
	IsInBattle(ctx context.Context, playerID uint64) (bool, error)
	// FindOfflinePlayers 批量找出已离线的玩家(locator 无记录 / state==OFFLINE)。
	// 成局最终门:onAllConfirmed 分配 DS 前校验全员在线,掉线玩家所在票据判责删除,
	// 其余退回队列,避免给残局白白拉起 Battle DS。弱依赖:查询失败返 error,
	// 调用方跳过校验继续成局(宁可多拉一局,不因 locator 抖动误杀正常对局)。
	FindOfflinePlayers(ctx context.Context, playerIDs []uint64) ([]uint64, error)
}

// PresenceReader 是 StartMatch 在线闸对 player_locator 的只读依赖。
//
// 形状与 pkg/offlinewatch.PresenceReader 一字不差,`*offlinewatch.GrpcPresenceReader`
// 直接满足本接口 —— 这是刻意的:「玩家离开大厅多久了」全仓只有那一份判定口径
// (locator 权威 = BatchGetLocation 判此刻在不在场 + BatchGetLastSeen 判离开了多久),
// team 的自动退队与本闸必须读同一份事实,否则两边会对同一个玩家给出相反的结论。
// 在这里重新定义一遍接口而不是直接 import 类型,只是为了让 biz 保持消费者侧接口的
// 惯例(便于单测注入),不代表另起一套判定。
//
// 三态必须严格区分:整批查不通一律返回 error,**绝不允许把查不通压成空 map** ——
// 那等价于宣布全队离线,会让 locator 抖一下就没人能开局(§9.22)。
type PresenceReader interface {
	// BatchOnline 返回此刻在 locator 有位置记录的玩家集合(在场即在线,
	// 不区分 HUB/MATCHING/BATTLE/LOGIN_PENDING)。
	BatchOnline(ctx context.Context, playerIDs []uint64) (map[uint64]bool, error)
	// BatchLastSeen 返回「最后一次被 Hub DS 观测在场的时刻」(unix ms)。
	// 优先是 left_at_ms(显式离开),退化到 last_alive_ms(心跳 census);缺席 = UNKNOWN。
	BatchLastSeen(ctx context.Context, playerIDs []uint64) (map[uint64]int64, error)
}

// IDGenerator 生成唯一 match_id(snowflake)。
type IDGenerator interface {
	Generate() uint64
}

// ── 常量 ─────────────────────────────────────────────────────────────────────

const (
	stageQueueing   = matchv1.MatchStage_MATCH_STAGE_QUEUEING
	stageFound      = matchv1.MatchStage_MATCH_STAGE_FOUND
	stageConfirm    = matchv1.MatchStage_MATCH_STAGE_CONFIRM
	stageAllocating = matchv1.MatchStage_MATCH_STAGE_ALLOCATING
	stageReady      = matchv1.MatchStage_MATCH_STAGE_READY
	stageFailed     = matchv1.MatchStage_MATCH_STAGE_FAILED

	confirmPending  = matchv1.MatchConfirmStatus_MATCH_CONFIRM_STATUS_PENDING
	confirmAccepted = matchv1.MatchConfirmStatus_MATCH_CONFIRM_STATUS_ACCEPTED
	confirmRejected = matchv1.MatchConfirmStatus_MATCH_CONFIRM_STATUS_REJECTED
)

// ── MatchUsecase ──────────────────────────────────────────────────────────────

// MatchUsecase 是 matchmaker 业务逻辑核心。
type MatchUsecase struct {
	repo      data.MatchRepo
	reader    TeamReader // 可为 nil(本机不起 team 时跳过校验)
	pusher    MatchEventPusher
	allocator DSAllocator
	idGen     IDGenerator
	locator   LocationNotifier // 可为 nil（本机不起 player_locator 时不上报位置）
	cfg       conf.MatchConf

	// presence 是 StartMatch 在线闸的权威读入口(见 ensureAllPresent)。
	// 可为 nil:locator 未配 / 显式关闭本闸时整道跳过,行为与 2026-08-13 之前完全一致。
	presence PresenceReader

	// router 是确定性 region/cell 路由器(scale-cellular-20m.md §4.2 两级撮合)。
	// 可为 nil:单 Cell / dev / 阶段 1~2 不分区,matchOnce 退化为单桶贪心(与历史行为一致)。
	// 多 Region 部署(阶段 3)由 main 经 SetCellRouter 注入,matchOnce 升级为"region 内优先 +
	// 跨 region 溢出"两级撮合。nil-safe,不阻断撮合。
	router *cellroute.Router

	// regionPolicy 是跨 region 溢出策略(阈值 / RTT 惩罚 / 跨区比例上限)。
	// 默认 DefaultRegionMatchPolicy();多 Region 阶段可由 main 从配置覆盖。
	regionPolicy RegionMatchPolicy

	// tables 配置表快照容器(pkg/configtable,读路径无锁)。可为 nil:
	// config_table.dir 未配置时不启用,StartMatch 跳过 map_id 表校验(历史行为)。
	// 由 main 经 SetConfigTables 注入;热更由 ConfigTableAdminService 触发,
	// 本结构每次经 Tables() 取当前批次,天然读到热更后的表。
	tables *configtable.Store

	// entryLimiter 进场侧限流(anti-abuse §6 第 2/3/7/8 项)。可为 nil(dev 无 Redis /
	// 联调不限流,与 chat worldLimiter 同型弱依赖);nil-safe,所有判定 error fail-open。
	entryLimiter EntryRateLimiter

	// lastLivenessSweep 是队列在线扫除(livenessSweepOnce)的上次执行时刻。
	// 只在 RunMatchLoop 单 goroutine 里读写,无需加锁。
	lastLivenessSweep  time.Time
	lastStartReconcile time.Time
	lastMatchReconcile time.Time

	// allocSem 是 DS 分配的有界并发信号量(压测审核【必修-3】)。此前
	// advanceAllocationsOnce 在撮合 leader 单协程内联串行调 advanceAllocation(含最长
	// ~60s 的分配 RPC),同分片分配吞吐被钳到「1 局 / 单次分配时延」。现改为 tick 内
	// 有界并发、返回前 join(见 advanceAllocationsParallel):tick 分配耗时从
	// Σ(每局时延) 降为 max(单局时延)。join 语义同时保证同一 match 绝无跨 tick 并发
	// 尝试(allocationRetryDelay 2~8s 短于最坏 RPC,无 join 时会叠加尝试)。
	// 正确性底线仍是 advanceAllocation 入口的 UpdateMatchWithLock CAS
	// (REQUESTING 抢占 + AllocationNextAttemptAtMs 到期门)+ operation_id 幂等。
	allocSem chan struct{}
}

func allocationOperationID() string {
	return uuid.NewString()
}

// NewMatchUsecase 构造 MatchUsecase。locator 可为 nil（弱依赖，不上报位置）。
func NewMatchUsecase(repo data.MatchRepo, reader TeamReader, pusher MatchEventPusher, allocator DSAllocator, idGen IDGenerator, locator LocationNotifier, cfg conf.MatchConf) *MatchUsecase {
	workers := cfg.AllocationWorkers
	if workers <= 0 {
		workers = 1 // 兜底:未配/非法时退化为串行(等价历史行为)
	}
	return &MatchUsecase{repo: repo, reader: reader, pusher: pusher, allocator: allocator, idGen: idGen, locator: locator, cfg: cfg,
		regionPolicy: DefaultRegionMatchPolicy(),
		allocSem:     make(chan struct{}, workers)}
}

// SetCellRouter 注入确定性 region 路由器(可选,多 Region 部署用)。
//
// nil-safe:不调用 / 传 nil 时,matchOnce 退化为单桶贪心(单 Cell / 阶段 1~2 语义)。
// 用 setter 而非构造参数,避免单 Cell 阶段调用点被迫改签名(与 login 两个 usecase 一致)。
// Router 内部读路径无锁(AtomicTable),并发安全。
func (u *MatchUsecase) SetCellRouter(r *cellroute.Router) {
	u.router = r
}

// SetRegionPolicy 覆盖跨 region 溢出策略(可选,多 Region 阶段从配置装配)。
func (u *MatchUsecase) SetRegionPolicy(p RegionMatchPolicy) {
	u.regionPolicy = p
}

// SetConfigTables 注入配置表容器(可选,config_table.dir 配置时由 main 装配)。
// 用 setter 而非构造参数,与 SetCellRouter 一致,避免未启用的调用点被迫改签名。
func (u *MatchUsecase) SetConfigTables(s *configtable.Store) {
	u.tables = s
}

// EntryRateLimiter 进场侧限流(实现:data.RedisEntryLimiter,委托 pkg/redisx 原语)。
// 背压非权威门(anti-abuse §2 铁律):所有方法 error 时调用方 fail-open + Warn,
// 一人一票 / 一人一 DS 的正确性仍由 durable operation、claim、locator BATTLE 门兜底。
type EntryRateLimiter interface {
	// TryStartCooldown 占用 StartMatch 的队长 + 队伍冷却窗(SETNX;false=窗内拒绝)。
	TryStartCooldown(ctx context.Context, captainID, teamID uint64, window time.Duration) (bool, error)
	// ClearStartCooldown 业务失败时释放冷却(先占坑→干活→失败释放,§9.20)。
	ClearStartCooldown(ctx context.Context, captainID, teamID uint64) error
	// TryFormCooldown 成局提交前占用票据成局冷却窗(首次零延迟)。
	TryFormCooldown(ctx context.Context, ticketID uint64, window time.Duration) (bool, error)
	// InFormCooldown 只读探测票据是否在成局冷却窗内(撮合组队路径)。
	InFormCooldown(ctx context.Context, ticketID uint64) (bool, error)
	// ArmFormCooldown 无条件布设成局冷却窗(容量耗尽退票的静默窗)。
	ArmFormCooldown(ctx context.Context, ticketID uint64, window time.Duration) error
	// NoShowPenaltyRemaining 读 no-show 进入侧退避剩余时长(写者 ds_allocator)。
	NoShowPenaltyRemaining(ctx context.Context, playerID uint64) (time.Duration, error)
}

// SetEntryLimiter 注入进场侧限流器(可选;不注入 = 不限流,dev 无 Redis 联调兼容)。
func (u *MatchUsecase) SetEntryLimiter(l EntryRateLimiter) {
	u.entryLimiter = l
}

// validateMapID StartMatch 入口的 map_id 关卡表准入门(配置表未启用时放行,历史行为)。
//
// mapID==0 表示「用本实例默认副本」,校验的是兜底后的实际值(cfg.MapId,与
// data.GrpcDSAllocator 的 effectiveMapID 兜底口径一致)——热更删掉默认关卡后,
// 新请求也会被立即拦下,而不是等 DS 加载失败。
// 只做入口校验:已入队票据短生命周期(ticket TTL 内)自然流完,不回溯清扫。
func (u *MatchUsecase) validateMapID(mapID uint32) error {
	if u.tables == nil {
		return nil
	}
	effective := mapID
	if effective == 0 {
		effective = u.cfg.MapId
	}
	tb := u.tables.Tables()
	if tb == nil {
		// 启用了配置表却无生效批次:main 启动强依赖保证不会出现;真出现只能 fail-closed。
		return errcode.New(errcode.ErrUnavailable, "config tables enabled but not loaded")
	}
	if !tb.Level.IsBattleLevel(effective) {
		return errcode.New(errcode.ErrMatchInvalidMap,
			"map_id %d not a battle level in level table (version %d)", effective, tb.Version)
	}
	// 玩法模式交叉校验(CLAUDE.md §17.1「差异进表」的服务端一侧)。
	// 关卡表 game_mode 是「这张图属于哪个撮合池」的唯一事实源;本实例的 cfg.GameMode 是
	// 「本部署承接哪个池」。两者不等说明这次请求根本不该落到本实例:
	//   - 客户端按表选错路由头,或路由头被伪造(Envoy 只按 header 选实例,不校验 map 归属);
	//   - 表改了 game_mode 但某一侧尚未热更到同批次。
	// 不校验会退化成「PVE 图进 PVP 池空排队到 ticket TTL」这类无人察觉的静默故障。
	//
	// **留空只跳过校验,绝不据此拒绝**(§9.21 双向兼容):关卡表是热更独立发布的,
	// 新二进制 + 旧批次表(无本列)在滚动升级 / 金丝雀窗口内必然出现,若把"读不到列"
	// 当成"配置错误"拒绝,会让该窗口内所有匹配全失败。语义同 DS 侧关卡门:
	// **Mismatch(读到且不同)才是证据,Unknown(读不到)只是无法判定**。
	// 客户端侧无此顾虑(表随包一起发,不存在错位),故客户端对留空是 fail-closed。
	if row, ok := tb.Level.ByID(effective); ok {
		if rowMode := row.GetGameMode(); rowMode != "" && rowMode != u.cfg.GameMode {
			return errcode.New(errcode.ErrMatchInvalidMap,
				"map_id %d belongs to game_mode %q but this matchmaker serves %q",
				effective, rowMode, u.cfg.GameMode)
		}
	}
	return nil
}

// mapRejectReason 把 validateMapID 的拒绝原因重新归一成固定枚举串(仅供日志,只读、无副作用)。
// 单看 errcode 分不出「不是战斗关卡」与「玩法模式不属本实例」——两者同为 ErrMatchInvalidMap,
// 而排障时这两件事的处置完全不同(改表 vs 改路由头 / 重新热更)。
func (u *MatchUsecase) mapRejectReason(mapID uint32) string {
	if u.tables == nil {
		return "map_gate_unexpected"
	}
	tb := u.tables.Tables()
	if tb == nil {
		return "config_table_not_loaded"
	}
	effective := mapID
	if effective == 0 {
		effective = u.cfg.MapId
	}
	if !tb.Level.IsBattleLevel(effective) {
		return "map_not_battle_level"
	}
	if row, ok := tb.Level.ByID(effective); ok {
		if rowMode := row.GetGameMode(); rowMode != "" && rowMode != u.cfg.GameMode {
			return "map_game_mode_mismatch"
		}
	}
	return "map_gate_unexpected"
}

// teamSizeForMap 取某副本(map_id)的一方人数:关卡表 team_size>0 时按表(「策划填表即用」,
// 每个副本各自 1v1 / 5v5),否则回退服务端全局 cfg.TeamSize。回退是契约明确的合法路径,不是错误:
// 未启用配置表(u.tables==nil,dev / 历史口径)、或表内该行 team_size==0(proto 契约:0=沿用全局兜底),
// 均按预期走全局值,不告警;tb==nil / 行不存在 已由 StartMatch 的 validateMapID 上游 fail-closed。
// 与 validateMapID 同一 effective 兜底:map_id==0 表示用本实例默认副本 cfg.MapId。
func (u *MatchUsecase) teamSizeForMap(mapID uint32) int {
	// fallback 先钳制(复审 P1 收口):全局 YAML cfg.TeamSize 在别处未做上下界校验,而它是
	// 所有回退分支(未启用表 / 表未加载 / 行不存在 / 行 team_size==0)的返回值,又是撮合
	// need=2*teamSize 的输入。巨值会预分配 OOM、负值(int 型 YAML 可为负)会造成 make 负
	// 容量 panic。配置表入口已在加载层拒绝,这里补上全局 fallback 这个未被覆盖的入口。
	fallback := clampTeamSize(u.cfg.TeamSize)
	if u.tables == nil {
		return fallback
	}
	effective := mapID
	if effective == 0 {
		effective = u.cfg.MapId
	}
	tb := u.tables.Tables()
	if tb == nil {
		return fallback
	}
	row, ok := tb.Level.ByID(effective)
	if !ok {
		return fallback
	}
	if ts := int(row.GetTeamSize()); ts > 0 {
		if ts > configtable.MaxLevelTeamSize {
			// 防御性钳制(§16.5 容量/溢出边界):配置加载层已按 configtable.MaxLevelTeamSize
			// 拒绝超大 team_size 并保留旧表(validateLevelRow),正常到不了这里;万一校验被
			// 绕过(手改 dist / 生成器漏校验),也绝不让撮合按超大值 need=2*teamSize 预分配爆内存。
			return configtable.MaxLevelTeamSize
		}
		return ts
	}
	return fallback
}

// allowedEntryModes 取某副本(map_id)**允许哪些进法**:关卡表 entry_mode 列为唯一事实源,
// 未配置(UNSPECIFIED / 表未启用 / 行不存在)时沿用本部署的 cfg.WalkIn 开关。
// 返回值恒是三者之一:MATCHMAKE(只准排队)/ WALK_IN(只准直进)/ BOTH(两种都开放)。
//
// 为什么下沉到表:部署级开关只能表达「整个池要么全直进、要么全撮合」,而
// 「多人撮合进副本」要求同一个 pve 池里有的副本直进、有的副本撮合(CLAUDE.md §17.1);
// BOTH 再进一步,要求**同一张图**两个入口共存,那已经不是图的属性而是玩家的选择。
// 兼容方向(§9.21):新二进制 + 旧批次表(无本列)→ 逐字节保持旧行为,不会误改入口语义。
func (u *MatchUsecase) allowedEntryModes(mapID uint32) configpb.LevelEntryMode {
	fallback := configpb.LevelEntryMode_LEVEL_ENTRY_MODE_MATCHMAKE
	if u.cfg.WalkIn {
		fallback = configpb.LevelEntryMode_LEVEL_ENTRY_MODE_WALK_IN
	}
	if u.tables == nil {
		return fallback
	}
	effective := mapID
	if effective == 0 {
		effective = u.cfg.MapId
	}
	tb := u.tables.Tables()
	if tb == nil {
		return fallback
	}
	row, ok := tb.Level.ByID(effective)
	if !ok {
		return fallback
	}
	switch m := row.GetEntryMode(); m {
	case configpb.LevelEntryMode_LEVEL_ENTRY_MODE_WALK_IN,
		configpb.LevelEntryMode_LEVEL_ENTRY_MODE_MATCHMAKE,
		configpb.LevelEntryMode_LEVEL_ENTRY_MODE_BOTH:
		return m
	default:
		return fallback
	}
}

// isWalkInMap 判断某副本**在没有玩家选择时**是否走直进。只用于兜底:滚动升级期旧 matchmaker
// 写下的、不带 entry_mode 的存量票据(见 isWalkInTicket)。新票据一律以票上落定的进法为准。
//
// 本函数的目标不是"做个合理判断",而是**逐字节复刻旧二进制对同一张票会做的决定**:
// 配置表热更与二进制发布相互独立,"旧二进制 + 含 BOTH 的新表"是真实存在的组合,而旧二进制
// 的 switch 不认识 BOTH,会落 default 用部署级 cfg.WalkIn。撮合循环虽是单写者,leader 仍会
// 在新旧副本间交棒 —— 两边对同一张存量票的分流必须一致,否则票的命运取决于当时谁是 leader
// (§9.21 共存窗口必须双向兼容)。所以这里对 BOTH 也必须回退 cfg.WalkIn,不能自作聪明。
func (u *MatchUsecase) isWalkInMap(mapID uint32) bool {
	switch u.allowedEntryModes(mapID) {
	case configpb.LevelEntryMode_LEVEL_ENTRY_MODE_WALK_IN:
		return true
	case configpb.LevelEntryMode_LEVEL_ENTRY_MODE_MATCHMAKE:
		return false
	default: // BOTH:旧二进制眼里的未知值
		return u.cfg.WalkIn
	}
}

// isWalkInTicket 判断一张票据走直进还是撮合。票上落定的 entry_mode 是权威;为空只可能是
// 滚动升级期旧 matchmaker 写入的存量票(§9.21 共存窗口),按关卡表 / 部署开关兜底,
// 与本字段上线前逐字节等价。存量票排空后本兜底不再有活路径,但不删——旧票据在 Redis 里
// 无 TTL(非终态持久),删兜底等于让升级期的票永远分流不出去。
func (u *MatchUsecase) isWalkInTicket(t *matchv1.MatchTicketStorageRecord) bool {
	switch t.GetEntryMode() {
	case configpb.LevelEntryMode_LEVEL_ENTRY_MODE_WALK_IN:
		return true
	case configpb.LevelEntryMode_LEVEL_ENTRY_MODE_MATCHMAKE:
		return false
	default:
		return u.isWalkInMap(t.GetMapId())
	}
}

// resolveEntryMode 把「关卡表允许什么」× 「玩家选什么」求交,落定本次进法。
// 返回值恒是 MATCHMAKE 或 WALK_IN 之一(BOTH 只是表侧的允许集合,不是可执行的进法)。
//
// fail-closed 规则(§17.3 准入条件只有服务端一份权威判定):
//   - 该图只允许一种进法:请求留空(老客户端)按那一种放行;显式选了别的即拒。
//   - 该图 BOTH:必须明确选 MATCHMAKE 或 WALK_IN,留空即拒——**不替玩家猜入口**。
//     猜错的代价是玩家以为在排队实则已经单刷进本(或反之),而副本进去就消耗了次数/CD,
//     不是一个能靠重试挽回的错误。
//   - 请求填 BOTH 一律拒:它不是一种进法。
func (u *MatchUsecase) resolveEntryMode(mapID uint32, choice configpb.LevelEntryMode) (configpb.LevelEntryMode, error) {
	allowed := u.allowedEntryModes(mapID)
	if allowed == configpb.LevelEntryMode_LEVEL_ENTRY_MODE_BOTH {
		switch choice {
		case configpb.LevelEntryMode_LEVEL_ENTRY_MODE_MATCHMAKE,
			configpb.LevelEntryMode_LEVEL_ENTRY_MODE_WALK_IN:
			return choice, nil
		default:
			return 0, errcode.New(errcode.ErrMatchEntryModeDenied,
				"map %d allows both matchmake and walk-in, request must pick one (got %v)", mapID, choice)
		}
	}
	if choice == configpb.LevelEntryMode_LEVEL_ENTRY_MODE_UNSPECIFIED || choice == allowed {
		return allowed, nil
	}
	return 0, errcode.New(errcode.ErrMatchEntryModeDenied,
		"map %d only allows entry mode %v, request asked for %v", mapID, allowed, choice)
}

// minTeamSizeForMap 取某副本的「直进人数下限」(关卡表 min_team_size;0 / 表未启用 / 行不存在
// = 无下限)。与 teamSizeForMap 同一 effective 兜底口径(map_id==0 用本实例默认副本)。
//
// 上界钳制刻意用 teamSizeForMap 而非 MaxLevelTeamSize:加载期已校验 min ≤ team_size,
// 这里再钳一次是防"手改 dist / 绕过生成器"把下限填得比上限还大——那会让该图**任何人数
// 都进不去**,是个静默的拒服务,不能只靠加载期一道门(§16.5)。
func (u *MatchUsecase) minTeamSizeForMap(mapID uint32) int {
	if u.tables == nil {
		return 0
	}
	effective := mapID
	if effective == 0 {
		effective = u.cfg.MapId
	}
	tb := u.tables.Tables()
	if tb == nil {
		return 0
	}
	row, ok := tb.Level.ByID(effective)
	if !ok {
		return 0
	}
	min := int(row.GetMinTeamSize())
	if min <= 0 {
		return 0
	}
	if max := u.teamSizeForMap(mapID); min > max {
		return max
	}
	return min
}

// sideCountForMap 取某副本(map_id)的对局方数:关卡表 side_count>0 时按表,否则回退 2
// (历史 need=2×team_size 的逐字节等价默认)。PVE 合作副本填 1,多队混战填 N。
// 与 teamSizeForMap 同一 effective 兜底口径(map_id==0 用本实例默认副本)。
func (u *MatchUsecase) sideCountForMap(mapID uint32) int {
	const fallback = 2
	if u.tables == nil {
		return fallback
	}
	effective := mapID
	if effective == 0 {
		effective = u.cfg.MapId
	}
	tb := u.tables.Tables()
	if tb == nil {
		return fallback
	}
	row, ok := tb.Level.ByID(effective)
	if !ok {
		return fallback
	}
	sc := int(row.GetSideCount())
	if sc <= 0 {
		return fallback
	}
	// 防御性钳制(§16.5):方数 × 每方人数 是撮合预分配的输入,坏配置不得放大成 OOM。
	// 上界复用一方人数上限:方数不可能比"一方最多几个人"还离谱。
	if sc > configtable.MaxLevelTeamSize {
		return configtable.MaxLevelTeamSize
	}
	return sc
}

// clampTeamSize 把一方人数钳到 [1, configtable.MaxLevelTeamSize]。撮合按 need=2*teamSize
// 预分配票据切片(greedyFormMatches/formMatchesInPool):负值(YAML 未校验的全局 team_size
// 可为负)会导致 make 负容量 panic,巨值会 OOM。下界取 1(1v1 是最小可成局副本)。
func clampTeamSize(ts int) int {
	if ts < 1 {
		return 1
	}
	if ts > configtable.MaxLevelTeamSize {
		return configtable.MaxLevelTeamSize
	}
	return ts
}

// ticketRegion 解析一张票据的 owner region(以队长 captain_id 为 owner 锚点)。
// router 为 nil(单 Cell / dev)或 Route 报错 → 返回 0(未知 / 单桶),不阻断撮合。
func (u *MatchUsecase) ticketRegion(t *matchv1.MatchTicketStorageRecord) uint32 {
	if u.router == nil || t == nil {
		return 0
	}
	loc, err := u.router.Route(t.CaptainId)
	if err != nil {
		return 0
	}
	return loc.RegionID
}

// ticketTier 返回一张票据的段位档(以 avg_mmr 经 regionPolicy.MmrTier 计算)。
// 高分段档位更高 → 溢出阈值更短(高分段人稀,早点跨 region)。供 selectOverflowTickets 的
// tierOf 入参,统一段位桶口径(decision-revisit-global-matchmaker.md §2.2/§2.3)。
func (u *MatchUsecase) ticketTier(t *matchv1.MatchTicketStorageRecord) int {
	if t == nil {
		return 0
	}
	return u.regionPolicy.MmrTier(t.AvgMmr)
}

// ticketMmrBucket 返回一张票据的 MMR 桶(以 avg_mmr 经 regionPolicy.MmrBucket 计算)。
// 判 localCandidatesEnough 的分组口径:同 region 内须落同一 MMR 桶才算彼此可成局的本地候选
// (溢出池 key,decision-revisit-global-matchmaker.md §2.3)。
func (u *MatchUsecase) ticketMmrBucket(t *matchv1.MatchTicketStorageRecord) uint32 {
	if t == nil {
		return 0
	}
	return u.regionPolicy.MmrBucket(t.AvgMmr)
}

// battlePlacement 计算 battle DS 应落的 (region, cell):参战玩家多数所在落点
// (scale-cellular-20m.md §4.4/§5,让多数玩家就近连入)。
// router 为 nil(单 Cell / dev)或全部玩家路由失败时返回 ok=false,调用方退化为不带放置提示
// (由 ds_allocator 默认选 Cell)。nil-safe,绝不阻断成局。
func (u *MatchUsecase) battlePlacement(playerIDs []uint64) (CellLocation, bool) {
	if u.router == nil {
		return CellLocation{}, false
	}
	locs := make([]CellLocation, 0, len(playerIDs))
	for _, pid := range playerIDs {
		loc, err := u.router.Route(pid)
		if err != nil {
			continue
		}
		locs = append(locs, CellLocation{RegionID: loc.RegionID, CellID: loc.CellID})
	}
	return MajorityCellLocation(locs)
}

// notifyMatching 把 match 成员位置标记为 MATCHING（弱依赖：nil 跳过 / 失败仅 Warn）。
func (u *MatchUsecase) notifyMatching(ctx context.Context, playerIDs []uint64, matchID uint64) {
	if u.locator == nil {
		return
	}
	if err := u.locator.NotifyMatching(ctx, playerIDs, matchID); err != nil {
		plog.With(ctx).Warnw("msg", "locator_notify_matching_failed", "match_id", matchID, "err", err)
	}
}

// notifyBattle 把 match 成员位置标记为 BATTLE（弱依赖：nil 跳过 / 失败仅 Warn）。
func (u *MatchUsecase) notifyBattle(ctx context.Context, playerIDs []uint64, matchID uint64, battlePod string) {
	if u.locator == nil {
		return
	}
	if err := u.locator.NotifyBattle(ctx, playerIDs, matchID, battlePod); err != nil {
		plog.With(ctx).Warnw("msg", "locator_notify_battle_failed", "match_id", matchID, "err", err)
	}
}

// notifyBattleStrict 是 READY 提交前的强依赖 BATTLE 投影写入(P0 修复 2026-07-15)。
// locator 未注入(dev 裸跑)跳过;写入失败返回可重试错误,由 allocation 推进循环重试。
func (u *MatchUsecase) notifyBattleStrict(ctx context.Context, playerIDs []uint64, matchID uint64, battlePod string) error {
	if u.locator == nil {
		return nil
	}
	if err := u.locator.NotifyBattle(ctx, playerIDs, matchID, battlePod); err != nil {
		plog.With(ctx).Errorw("msg", "locator_notify_battle_failed_pre_ready", "match_id", matchID, "err", err)
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"battle location projection must commit before READY for match %d", matchID)
	}
	return nil
}

// ensureNoneInBattle 拦截"战斗中还点匹配"：任一成员正处于 BATTLE 状态则拒绝整队入队。
//
// 权威来源是 player_locator（不变量 §1）。处理规则：
//   - locator 未注入（nil）→ 跳过（本机不起 player_locator 的骨架联调路径）。
//   - 明确查到某成员 state==BATTLE → 返回 ErrMatchInBattle。
//   - locator 查询失败 → 默认 fail-closed（生产安全）：拒绝入队并返回 ErrUnavailable 让客户端重试，
//     只对明确非 BATTLE 的成员放行。避免 locator 短暂抖动叠加旧 claim 过期时，把战斗中玩家二次塞进队列。
//     仅当显式配置 BattleGateFailOpen=true（dev 弱依赖）时，才降级为 Warn 后放行
//     （兜底仍由后续 ClaimPlayer 的 SETNX 保证"一人一队列"）。
func (u *MatchUsecase) ensureNoneInBattle(ctx context.Context, members []*matchv1.MatchMemberStorageRecord) error {
	if u.locator == nil {
		return nil
	}
	for _, m := range members {
		inBattle, err := u.locator.IsInBattle(ctx, m.PlayerId)
		if err != nil {
			if u.cfg.BattleGateFailOpen {
				plog.With(ctx).Warnw("msg", "locator_is_in_battle_failed_fail_open",
					"reason", "battle_gate_locator_unavailable", "fail_open", true,
					"player_id", m.PlayerId, "err", err)
				continue
			}
			plog.With(ctx).Errorw("msg", "locator_is_in_battle_failed_fail_closed",
				"reason", "battle_gate_locator_unavailable", "fail_open", false,
				"player_id", m.PlayerId, "err", err)
			return errcode.New(errcode.ErrUnavailable, "locator unavailable, cannot verify battle state for player %d: %v", m.PlayerId, err)
		}
		if inBattle {
			plog.With(ctx).Warnw("msg", "match_start_member_in_battle",
				"reason", "member_in_battle", "player_id", m.PlayerId, "team_id", m.GetTeamId())
			return errcode.New(errcode.ErrMatchInBattle, "player %d in battle", m.PlayerId)
		}
	}
	return nil
}

// SetPresenceReader 注入 StartMatch 在线闸的权威读入口(main.go 用 locator conn 构造)。
// nil-safe:不注入时整道闸跳过,与本闸落地前行为完全一致。
func (u *MatchUsecase) SetPresenceReader(p PresenceReader) { u.presence = p }

// startPresenceGrace 返回在线闸的宽限窗;<=0 表示整道闸关闭。
func (u *MatchUsecase) startPresenceGrace() time.Duration { return u.cfg.StartPresenceGrace.Std() }

// ensureAllPresent 拦截「把已经离开大厅的队友冻进对局票据」(INC-20260813-001)。
//
// # 这道闸补的是哪个洞
//
// StartMatch 此前的门只有:关卡表 → 进法 → 冻结名单 → 人数下限 → no-show 退避 →
// 战斗中(ensureNoneInBattle)→ claim 预检。**没有任何一道看「人还在不在」**。
// 于是队员关掉客户端后,只要还没到 team 的 offline_leave.threshold(180s),他就仍是
// 队伍里一个 ready 成员,被原样冻进票据 → DS 拿到 N 人 roster 却只进来 N-1 人。
//
// 队伍里保留他是**对的**(那 180s 是留给弱网 / 地铁 / 重连的余量,不该缩)。
// 错的是把「还留在队伍里」当成了「有资格被拉进对局」——这是两件事,本闸把它们分开。
//
// # 判据刻意不是「locator 里查不查得到」
//
// 位置投影在**正常路径**上就会短暂缺席:撮合失败后玩家停在 MATCHING 而 MATCHING 无
// 保活(30s 后整 key 消失,人却好端端坐在大厅);切线换 Hub 也有换手窗口。
// INC-20260724-001 的成局最终门正是按缺席判死,结果是**结构上 100% 假阳性**,
// 玩家静止不动也必被判离线,最后只能整道关掉。
//
// 本闸因此按「离开了多久」判:
//
//	不在场  且  now - lastSeen >= grace   → 拒
//	不在场  且  拿不到任何 lastSeen 基线    → 放行(UNKNOWN 不得冒充 OFFLINE,§9.22)
//	在场                                  → 放行
//
// lastSeen 的两级来源都由 locator 维护(BatchGetLastSeen):显式离开写 left_at_ms,
// 拿不到时退到 last_alive_ms —— 后者由 Hub DS 心跳按 **census 全员** 每 5s 续期
// (2026-08-13 同批修复),所以坐在大厅里的人这个值恒为「刚刚」,与位置投影处于哪一态
// 无关;真关掉客户端的人才会一路涨过 grace。假阳性的结构性来源就此消失。
//
// # 依赖不可用时
//
// 与 ensureNoneInBattle 完全同款:默认 fail-closed 返回 ErrUnavailable 让客户端重试,
// 只有 dev 显式 battle_gate_fail_open=true 才降级 Warn 放行。理由也相同 ——
// 拿不准就别开局,重来一次的代价远小于拉起一场缺员的对局。
//
// # 位置刻意在 resolveMembers 之后
//
// 必须用 BeginTeamMatch **冻结后**的那份名单来查,才和票据里最终写进去的是同一批人;
// 放在冻结之前查等于查了一份可能已经变了的名单。
func (u *MatchUsecase) ensureAllPresent(ctx context.Context, members []*matchv1.MatchMemberStorageRecord) error {
	grace := u.startPresenceGrace()
	if u.presence == nil || grace <= 0 || len(members) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(members))
	for _, m := range members {
		if m.GetPlayerId() != 0 {
			ids = append(ids, m.GetPlayerId())
		}
	}
	if len(ids) == 0 {
		return nil
	}

	offline, longestMs, op, err := u.absentBeyond(ctx, ids, grace)
	if err != nil {
		return u.presenceGateUnavailable(ctx, op, err)
	}
	if len(offline) == 0 {
		return nil
	}
	plog.With(ctx).Warnw("msg", "match_start_member_offline",
		"reason", "member_absent_beyond_grace",
		"offline_players", offline, "members", len(ids),
		"grace", grace.String(), "longest_absent_ms", longestMs)
	// 缺席名单走结构化通道过线(StartMatchResponse.absent_player_ids,2026-08-17 拍板):
	// 光靠 error 文本客户端点不了名,队长只能看见一句「有队员不在大厅」却不知道该等谁。
	return &MemberOfflineError{
		AbsentPlayerIDs: offline,
		cause: errcode.New(errcode.ErrMatchMemberOffline,
			"players %v left the hub more than %s ago; cannot start match", offline, grace),
	}
}

// MemberOfflineError 是在线闸拒绝(4011)的载体:错误码语义不变(Unwrap 链上仍是
// ErrMatchMemberOffline,errcode.As 照常解析),额外携带被判缺席的成员 ——
// service 层据此填 StartMatchResponse.absent_player_ids,让客户端能点名「XX 不在大厅」。
type MemberOfflineError struct {
	AbsentPlayerIDs []uint64
	cause           error
}

func (e *MemberOfflineError) Error() string { return e.cause.Error() }
func (e *MemberOfflineError) Unwrap() error { return e.cause }

// absentBeyond 按「离开了多久」找出这批玩家里已离场超过 window 的人
// (StartMatch 在线闸与排队票离线回收共用的判据核心,契约见 ensureAllPresent 文档):
//
//	在场                          → 放行
//	不在场 且 拿不到任何离开基线    → UNKNOWN 放行(§9.22,不得冒充 OFFLINE)
//	不在场 且 now-lastSeen ≥ window → 判离场
//
// 返回的 op 标记哪一跳查询失败("batch_online"/"batch_last_seen"),供调用方按各自
// 的依赖故障策略收口(入队闸 fail-closed / 回收路径弱依赖跳过)。
// 稳态下全员在线,第二跳 BatchLastSeen 完全省掉。
func (u *MatchUsecase) absentBeyond(ctx context.Context, ids []uint64, window time.Duration) (offline []uint64, longestMs int64, op string, err error) {
	online, err := u.presence.BatchOnline(ctx, ids)
	if err != nil {
		return nil, 0, "batch_online", err
	}
	absent := make([]uint64, 0, len(ids))
	for _, pid := range ids {
		if !online[pid] {
			absent = append(absent, pid)
		}
	}
	if len(absent) == 0 {
		return nil, 0, "", nil
	}
	lastSeen, err := u.presence.BatchLastSeen(ctx, absent)
	if err != nil {
		return nil, 0, "batch_last_seen", err
	}

	nowMs := time.Now().UnixMilli()
	for _, pid := range absent {
		since, ok := lastSeen[pid]
		if !ok || since <= 0 {
			// 没有任何离开基线(从没上过线 / 已超保留期 / Hub DS 整台挂掉时压根没上报)。
			// UNKNOWN 一律放行:这条判据的代价方向是「宁可放过一个,不可误杀在线的」。
			continue
		}
		elapsed := nowMs - since
		if elapsed < window.Milliseconds() {
			continue // 刚离开,可能正在重连
		}
		offline = append(offline, pid)
		if elapsed > longestMs {
			longestMs = elapsed
		}
	}
	return offline, longestMs, "", nil
}

// presenceGateUnavailable 收口在线闸的依赖故障分支(与 ensureNoneInBattle 同策略)。
func (u *MatchUsecase) presenceGateUnavailable(ctx context.Context, op string, err error) error {
	if u.cfg.BattleGateFailOpen {
		plog.With(ctx).Warnw("msg", "match_start_presence_gate_fail_open",
			"reason", "presence_gate_locator_unavailable", "fail_open", true, "op", op, "err", err)
		return nil
	}
	plog.With(ctx).Errorw("msg", "match_start_presence_gate_fail_closed",
		"reason", "presence_gate_locator_unavailable", "fail_open", false, "op", op, "err", err)
	return errcode.New(errcode.ErrUnavailable,
		"locator unavailable, cannot verify hub presence (%s): %v", op, err)
}

func (u *MatchUsecase) ticketTTL() time.Duration { return u.cfg.TicketTTL.Std() }
func (u *MatchUsecase) matchTTL() time.Duration  { return u.cfg.MatchTTL.Std() }

// requireLocalGameMode prevents a cold client routed to the default PVP
// instance from mutating a canonical PVE ticket/match and consequently writing
// the wrong queue/active index. Empty is accepted only for rolling-upgrade
// records written before the additive game_mode field existed; every new
// writer below persists the canonical namespace.
func (u *MatchUsecase) requireLocalGameMode(stored string) error {
	if stored != "" && stored != u.cfg.GameMode {
		return errcode.New(errcode.ErrInvalidState,
			"match belongs to game_mode %q, request reached %q", stored, u.cfg.GameMode)
	}
	return nil
}

// removeActive 把 match 移出 active ZSET,出错仅警告。
func (u *MatchUsecase) removeActive(ctx context.Context, matchID uint64) {
	if err := u.repo.RemoveActive(ctx, matchID); err != nil {
		plog.With(ctx).Warnw("msg", "remove_active_failed", "match_id", matchID, "err", err)
	}
}

// ── RPC 1:StartMatch ─────────────────────────────────────────────────────────

// StartMatch 把 team 作为一张票据入队。ticketID 由 service 层 snowflake 生成。
// 返回的 ticketID 同时作为客户端 QUEUEING 阶段的 match 句柄(CancelMatch/GetMatchProgress 用)。
//
// 前置(reader 非 nil 时):team 必须存在、state=READY、captainID 为队长、成员数 ≤ 一方人数。
// entryChoice 是玩家选的进法(0=未选,老客户端);能不能这么进由 resolveEntryMode 按关卡表判定。
func (u *MatchUsecase) StartMatch(ctx context.Context, ticketID, teamID, captainID uint64, mapID uint32, entryChoice configpb.LevelEntryMode) (uint64, error) {
	// join key(infra.md §11.3 R3):本请求后续日志自动带 team_id。
	if teamID != 0 {
		ctx = plog.WithTeamID(ctx, teamID)
	}
	// 关卡表准入门(不变量 §9.15 接线):客户端上送的 map_id 必须是关卡表里的战斗类关卡,
	// 否则任意 map_id 会一路透传成 DS 的 PANDORA_MAP_ID(拉起加载不存在关卡的 DS)。
	if err := u.validateMapID(mapID); err != nil {
		plog.With(ctx).Warnw("msg", "match_start_rejected",
			"gate", "validate_map", "reason", u.mapRejectReason(mapID),
			"code", int(errcode.As(err)), "ticket_id", ticketID, "team_id", teamID,
			"captain_id", captainID, "map_id", mapID, "game_mode", u.cfg.GameMode, "err", err)
		return 0, err
	}

	// 队列准入上限(压测审核【必修-4】,§9.18 精神):撮合循环每 tick 全量处理 queue,
	// 无准入时突发入队会把 tick 拖过 match_interval 形成正反馈雪崩。软上限即可
	// (并发窗口内少量超入无害,这是背压不是不变量);长度查询失败放行——canonical 写
	// 也在同一 Redis,真故障会在 CreateStartOperation 如实失败,不需要在这里预判。
	if u.cfg.MaxQueueTickets > 0 {
		if qlen, qerr := u.repo.QueueLen(ctx); qerr != nil {
			plog.With(ctx).Warnw("msg", "match_queue_len_check_failed",
				"reason", "queue_len_probe_failed", "team_id", teamID, "captain_id", captainID, "err", qerr)
		} else if qlen >= int64(u.cfg.MaxQueueTickets) {
			plog.With(ctx).Warnw("msg", "match_queue_admission_rejected",
				"reason", "queue_full",
				"queue_len", qlen, "max", u.cfg.MaxQueueTickets, "team_id", teamID,
				"captain_id", captainID, "map_id", mapID, "game_mode", u.cfg.GameMode)
			return 0, errcode.New(errcode.ErrRateLimited,
				"match queue is full (%d), retry later", qlen)
		}
	}

	// per-队长/per-队伍冷却(anti-abuse §6 第 2 项):挡外挂 RPC 速率的 StartMatch↔Cancel
	// 循环。占窗在一切副作用之前;之后任何失败都释放冷却(见函数尾),玩家可立即重试。
	// 背压非权威门:limiter 未注入 / 窗口 <=0 / Redis 故障均放行。
	if !u.tryStartCooldown(ctx, captainID, teamID) {
		return 0, errcode.New(errcode.ErrRateLimited,
			"start match cooldown, retry in %s", u.cfg.StartMatchCooldown.Std())
	}
	id, err := u.startMatchAdmitted(ctx, ticketID, teamID, captainID, mapID, entryChoice)
	if err != nil {
		// 先占坑→干活→失败释放(hub transferToLineInner 同模板,§9.20):
		// 冷却只约束成功受理的频率,业务失败不得让玩家白等一个冷却窗。
		u.releaseStartCooldown(ctx, captainID, teamID)
	}
	return id, err
}

// startMatchAdmitted 是 StartMatch 过冷却门之后的主体(拆出以便失败路径统一释放冷却)。
func (u *MatchUsecase) startMatchAdmitted(ctx context.Context, ticketID, teamID, captainID uint64, mapID uint32, entryChoice configpb.LevelEntryMode) (uint64, error) {
	// 进法先落定:后续的下限判定、票据落库、撮合分流全用这一个结果,不各自再解析一遍。
	// 每道门的拒绝都必须留证。此前所有失败分支都是裸 `return 0, err`,一行日志都不打 ——
	// 服务端因此完全看不见「谁在什么时候被哪道门拒了」,INC-20260813-001 排查时只能靠
	// envoy 访问日志的响应体字节数反推(成功 49B / 只回 code 43B)。
	reject := func(gate, reason string, err error) (uint64, error) {
		plog.With(ctx).Warnw("msg", "match_start_rejected",
			"gate", gate, "reason", reason,
			"code", int(errcode.As(err)), "ticket_id", ticketID, "team_id", teamID,
			"captain_id", captainID, "map_id", mapID, "game_mode", u.cfg.GameMode,
			"entry_choice", entryChoice.String(), "err", err)
		return 0, err
	}

	entryMode, err := u.resolveEntryMode(mapID, entryChoice)
	if err != nil {
		reason := "entry_mode_not_allowed"
		if u.allowedEntryModes(mapID) == configpb.LevelEntryMode_LEVEL_ENTRY_MODE_BOTH {
			reason = "entry_mode_choice_required"
		}
		return reject("entry_mode", reason, err)
	}

	members, avgMMR, err := u.resolveMembers(ctx, teamID, captainID, mapID)
	if err != nil {
		return reject("resolve_members", "roster_unavailable", err)
	}

	// 直进人数下限(关卡表 min_team_size):人没凑够时玩家自己进,至少要够这么多人。
	// 落在这里而不是 resolveMembers 里,是因为 resolveMembers 对 teamID==0(单人入口)
	// 直接返回单人名单、不走任何人数校验——下限若写在那个分支后面,"不组队直接点进"
	// 就能整条绕过去。用 len(members) 判定则两条路径都被覆盖。
	// 撮合入口不判本闸:它的目标恒是凑满 team_size(≥ min,加载期已校验),天然满足。
	if entryMode == configpb.LevelEntryMode_LEVEL_ENTRY_MODE_WALK_IN {
		if min := u.minTeamSizeForMap(mapID); len(members) < min {
			return reject("min_team_size", "walk_in_below_min_team_size",
				errcode.New(errcode.ErrMatchTeamTooSmall,
					"map %d requires at least %d players to walk in, got %d", mapID, min, len(members)))
		}
	}

	// no-show 进入侧退避执行点(anti-abuse §6 第 8 项,温和档):ds_allocator 在空场判弃
	// reason=no_show 时对 roster 记罚(10min 窗内首次免罚,第 2 次起指数退避),这里只读执行。
	// 只有 no_show 记罚——正常结算、断线重连、主动取消都不计,正常玩家无感。
	if err := u.checkNoShowPenalty(ctx, members); err != nil {
		return reject("no_show_penalty", "no_show_backoff_active", err)
	}

	// P0 修复(2026-07-15,codex P0-8):战斗中玩家不得入队。claim(preflight/SETNX)只拦
	// "已在撒配链路里"的玩家;若上一局已 ReleaseMatch 但玩家仍在 DS 内(或 GM 拉入),
	// 唯一能拦住的是 locator BATTLE 状态门(不变量 §1 一人一 DS)。
	if err := u.ensureNoneInBattle(ctx, members); err != nil {
		reason := "member_in_battle"
		if errcode.As(err) == errcode.ErrUnavailable {
			reason = "battle_gate_locator_unavailable"
		}
		return reject("in_battle", reason, err)
	}

	// 在线闸(INC-20260813-001)。位置刻意夹在这里:
	//   - 必须在 resolveMembers 之后 —— 要查的是 BeginTeamMatch **冻结后**的那份名单,
	//     与最终写进票据的是同一批人;
	//   - 放在 preflightStartClaims 之前 —— claim 是有副作用的写(SETNX 占坑),
	//     能在无副作用的读闸上拒掉的,就不要先去占坑再回滚。
	if err := u.ensureAllPresent(ctx, members); err != nil {
		reason := "member_absent_beyond_grace"
		if errcode.As(err) == errcode.ErrUnavailable {
			reason = "presence_gate_locator_unavailable"
		}
		return reject("member_offline", reason, err)
	}

	if err := u.preflightStartClaims(ctx, members); err != nil {
		reason := "claim_probe_failed"
		if errcode.As(err) == errcode.ErrMatchAlreadyMatching {
			reason = "player_already_matching"
		}
		return reject("start_claim", reason, err)
	}

	nowMs := time.Now().UnixMilli()
	op := &matchv1.MatchStartOperationStorageRecord{
		OperationId:     uuid.NewString(),
		TicketId:        ticketID,
		TeamId:          teamID,
		CaptainId:       captainID,
		Members:         members,
		AvgMmr:          avgMMR,
		MapId:           mapID,
		Phase:           matchv1.MatchStartPhase_MATCH_START_PHASE_ACCEPTED,
		NextAttemptAtMs: nowMs,
		CreatedAtMs:     nowMs,
		GameMode:        u.cfg.GameMode,
		EntryMode:       entryMode,
	}

	// RPC 的唯一提交点是 durable operation。票据主体→成员 compare-claim→queue ZADD
	// 由服务生命周期 worker 推进；玩家断线、RPC ctx 取消或进程重启都不会中断 saga。
	if err := u.repo.CreateStartOperation(ctx, op, u.ticketTTL()); err != nil {
		return reject("create_start_operation", "commit_failed", err)
	}

	// R1 阶段推进:StartMatch 的线性化点(durable operation 已落库 = 已受理)。
	// 曾经是 Debug —— 线上默认 info 级下一条都不出,"玩家到底有没有入队成功"无从证明。
	plog.With(ctx).Infow("msg", "match_start_accepted", "ticket_id", ticketID, "operation_id", op.OperationId, "team_id", teamID,
		"captain_id", captainID, "members", len(members), "member_ids", memberPlayerIDs(members),
		"avg_mmr", avgMMR, "map_id", mapID, "game_mode", u.cfg.GameMode, "entry_mode", entryMode.String())
	return ticketID, nil
}

// tryStartCooldown 占用 StartMatch 冷却窗。false = 窗口内重复请求(拒绝)。
// limiter 未注入 / 窗口 <=0 / Redis 故障均放行(背压非权威门,§2 铁律)。
func (u *MatchUsecase) tryStartCooldown(ctx context.Context, captainID, teamID uint64) bool {
	if u.entryLimiter == nil {
		return true
	}
	window := u.cfg.StartMatchCooldown.Std()
	if window <= 0 {
		return true
	}
	ok, err := u.entryLimiter.TryStartCooldown(ctx, captainID, teamID, window)
	if err != nil {
		plog.With(ctx).Warnw("msg", "start_cooldown_check_failed",
			"reason", "cooldown_probe_failed", "fail_open", true,
			"captain_id", captainID, "team_id", teamID, "err", err)
		return true
	}
	if !ok {
		// 拒绝走日志不走 metrics 定位到玩家(§4.4:player_id 绝不能做 label)。
		plog.With(ctx).Warnw("msg", "start_cooldown_rejected",
			"reason", "start_cooldown_window",
			"captain_id", captainID, "team_id", teamID, "window", window.String())
	}
	return ok
}

// releaseStartCooldown 释放 StartMatch 冷却窗(业务失败路径)。best-effort:释放失败仅
// Warn——最坏后果是玩家多等一个冷却窗(秒级),TTL 自愈,不值得为它引入重试链。
func (u *MatchUsecase) releaseStartCooldown(ctx context.Context, captainID, teamID uint64) {
	if u.entryLimiter == nil || u.cfg.StartMatchCooldown.Std() <= 0 {
		return
	}
	if err := u.entryLimiter.ClearStartCooldown(ctx, captainID, teamID); err != nil {
		plog.With(ctx).Warnw("msg", "start_cooldown_release_failed",
			"captain_id", captainID, "team_id", teamID, "err", err)
	}
}

// checkNoShowPenalty 逐成员读 no-show 退避窗;任一成员在罚 → ErrRateLimited(带可见的
// 剩余秒数,客户端按可重试展示倒计时,不静默卡住)。读失败整批 fail-open。
func (u *MatchUsecase) checkNoShowPenalty(ctx context.Context, members []*matchv1.MatchMemberStorageRecord) error {
	if u.entryLimiter == nil {
		return nil
	}
	for _, m := range members {
		remain, err := u.entryLimiter.NoShowPenaltyRemaining(ctx, m.GetPlayerId())
		if err != nil {
			plog.With(ctx).Warnw("msg", "noshow_penalty_check_failed",
				"reason", "noshow_probe_failed", "fail_open", true,
				"player_id", m.GetPlayerId(), "err", err)
			return nil
		}
		if remain > 0 {
			retrySec := int64((remain + time.Second - 1) / time.Second)
			plog.With(ctx).Warnw("msg", "noshow_penalty_rejected",
				"reason", "no_show_backoff_active",
				"player_id", m.GetPlayerId(), "retry_after_sec", retrySec)
			return errcode.New(errcode.ErrRateLimited,
				"player %d no-show cooldown, retry after %ds", m.GetPlayerId(), retrySec)
		}
	}
	return nil
}

// tryFormCooldown 成局提交前占用票据的成局冷却窗。窗内返回 ErrRateLimited(调用方按
// 静默节流处理,票据留在队列等下轮)。limiter 未注入 / 窗口 <=0 / Redis 故障放行。
func (u *MatchUsecase) tryFormCooldown(ctx context.Context, ticketID uint64) error {
	if u.entryLimiter == nil {
		return nil
	}
	window := u.cfg.MatchFormCooldown.Std()
	if window <= 0 {
		return nil
	}
	ok, err := u.entryLimiter.TryFormCooldown(ctx, ticketID, window)
	if err != nil {
		plog.With(ctx).Warnw("msg", "form_cooldown_check_failed", "ticket_id", ticketID, "err", err)
		return nil
	}
	if !ok {
		return errcode.New(errcode.ErrRateLimited, "ticket %d in form cooldown", ticketID)
	}
	return nil
}

// anyTicketInFormCooldown 撮合组队路径的只读探测:组内任一票据仍在成局冷却窗内则
// 本轮放弃该组合(票据留队,窗过自然重组)。探测失败 fail-open 按不在窗内。
func (u *MatchUsecase) anyTicketInFormCooldown(ctx context.Context, sides [][]*matchv1.MatchTicketStorageRecord) bool {
	if u.entryLimiter == nil || u.cfg.MatchFormCooldown.Std() <= 0 {
		return false
	}
	for _, side := range sides {
		for _, t := range side {
			in, err := u.entryLimiter.InFormCooldown(ctx, t.GetTicketId())
			if err != nil {
				plog.With(ctx).Warnw("msg", "form_cooldown_probe_failed", "ticket_id", t.GetTicketId(), "err", err)
				continue
			}
			if in {
				return true
			}
		}
	}
	return false
}

// preflightStartClaims 提前拒绝明确的 live claim，并 CAS 清掉明确不存在票据的僵尸 claim。
// 这只是友好错误的快照检查；真正的一人一票线性化点仍是 durable worker 的 SETNX。
func (u *MatchUsecase) preflightStartClaims(ctx context.Context, members []*matchv1.MatchMemberStorageRecord) error {
	for _, member := range members {
		startTicketID, startFound, err := u.repo.GetStartPlayerOperation(ctx, member.GetPlayerId())
		if err != nil {
			plog.With(ctx).Warnw("msg", "match_start_claim_probe_failed",
				"reason", "read_start_player_index_failed", "player_id", member.GetPlayerId(), "err", err)
			return err
		}
		if startFound {
			op, found, gerr := u.repo.GetStartOperation(ctx, startTicketID)
			if gerr != nil {
				plog.With(ctx).Warnw("msg", "match_start_claim_probe_failed",
					"reason", "read_start_operation_failed", "player_id", member.GetPlayerId(),
					"ticket_id", startTicketID, "err", gerr)
				return gerr
			}
			if found && !startOperationTerminal(op.GetPhase()) {
				plog.With(ctx).Warnw("msg", "match_start_claim_conflict",
					"reason", "start_operation_in_flight", "player_id", member.GetPlayerId(),
					"ticket_id", startTicketID, "phase", op.GetPhase().String())
				return errcode.New(errcode.ErrMatchAlreadyMatching,
					"player %d already has start operation %d", member.GetPlayerId(), startTicketID)
			}
			if err := u.repo.DeleteStartPlayerIfMatches(ctx, member.GetPlayerId(), startTicketID); err != nil {
				plog.With(ctx).Warnw("msg", "match_start_claim_probe_failed",
					"reason", "clear_zombie_start_index_failed", "player_id", member.GetPlayerId(),
					"ticket_id", startTicketID, "err", err)
				return err
			}
		}
		ticketID, found, err := u.repo.GetPlayerTicket(ctx, member.GetPlayerId())
		if err != nil {
			plog.With(ctx).Warnw("msg", "match_start_claim_probe_failed",
				"reason", "read_player_claim_failed", "player_id", member.GetPlayerId(), "err", err)
			return err
		}
		if !found {
			continue
		}
		_, ticketFound, err := u.repo.GetTicket(ctx, ticketID)
		if err != nil {
			plog.With(ctx).Warnw("msg", "match_start_claim_probe_failed",
				"reason", "read_claimed_ticket_failed", "player_id", member.GetPlayerId(),
				"ticket_id", ticketID, "err", err)
			return err
		}
		if ticketFound {
			plog.With(ctx).Warnw("msg", "match_start_claim_conflict",
				"reason", "player_already_matching", "player_id", member.GetPlayerId(),
				"ticket_id", ticketID)
			return errcode.New(errcode.ErrMatchAlreadyMatching, "player %d already matching", member.GetPlayerId())
		}
		if err := u.repo.DeletePlayerIndexIfMatches(ctx, member.GetPlayerId(), ticketID); err != nil {
			plog.With(ctx).Warnw("msg", "match_start_claim_probe_failed",
				"reason", "clear_zombie_claim_failed", "player_id", member.GetPlayerId(),
				"ticket_id", ticketID, "err", err)
			return err
		}
	}
	return nil
}

const (
	startOperationLease     = 15 * time.Second
	canonicalReconcileEvery = 5 * time.Second
)

func startRetryDelay(attempt uint32) time.Duration {
	shift := attempt
	if shift > 4 {
		shift = 4
	}
	d := time.Second * time.Duration(1<<shift)
	if d > 30*time.Second {
		return 30 * time.Second
	}
	return d
}

func cloneStartOperation(op *matchv1.MatchStartOperationStorageRecord) *matchv1.MatchStartOperationStorageRecord {
	return proto.Clone(op).(*matchv1.MatchStartOperationStorageRecord)
}

func startOperationTerminal(phase matchv1.MatchStartPhase) bool {
	return phase == matchv1.MatchStartPhase_MATCH_START_PHASE_QUEUED ||
		phase == matchv1.MatchStartPhase_MATCH_START_PHASE_FAILED
}

func ticketFromStartOperation(op *matchv1.MatchStartOperationStorageRecord) *matchv1.MatchTicketStorageRecord {
	return &matchv1.MatchTicketStorageRecord{
		TicketId:     op.GetTicketId(),
		TeamId:       op.GetTeamId(),
		CaptainId:    op.GetCaptainId(),
		Members:      op.GetMembers(),
		AvgMmr:       op.GetAvgMmr(),
		EnqueuedAtMs: op.GetCreatedAtMs(),
		MapId:        op.GetMapId(),
		GameMode:     op.GetGameMode(),
		EntryMode:    op.GetEntryMode(),
	}
}

// claimPlayerForStart 是 durable saga 版本的 claim：崩溃若发生在 SETNX 成功、phase
// 持久化之前，重放会看到 existing==ticketID，并把它识别为本操作已完成，而非冲突。
func (u *MatchUsecase) claimPlayerForStart(ctx context.Context, playerID, ticketID uint64) (bool, error) {
	for attempt := 0; attempt < 2; attempt++ {
		existing, claimed, err := u.repo.ClaimPlayer(ctx, playerID, ticketID, u.ticketTTL())
		if err != nil {
			return false, err
		}
		if claimed || existing == ticketID {
			return true, nil
		}
		if _, found, gerr := u.repo.GetTicket(ctx, existing); gerr != nil || found {
			return false, gerr
		}
		if err := u.repo.DeletePlayerIndexIfMatches(ctx, playerID, existing); err != nil {
			return false, err
		}
	}
	return false, nil
}

func (u *MatchUsecase) updateOwnedStartOperation(
	ctx context.Context,
	ticketID uint64,
	leaseToken string,
	fn func(*matchv1.MatchStartOperationStorageRecord) error,
) (*matchv1.MatchStartOperationStorageRecord, error) {
	var snapshot *matchv1.MatchStartOperationStorageRecord
	err := u.repo.UpdateStartOperationWithLock(ctx, ticketID, u.cfg.OptimisticRetry, func(op *matchv1.MatchStartOperationStorageRecord) error {
		if op.GetLeaseToken() != leaseToken {
			return errcode.New(errcode.ErrMatchConcurrent, "start operation %d lease changed", ticketID)
		}
		if err := fn(op); err != nil {
			return err
		}
		snapshot = cloneStartOperation(op)
		return nil
	}, u.ticketTTL())
	return snapshot, err
}

func (u *MatchUsecase) deferStartOperation(ctx context.Context, op *matchv1.MatchStartOperationStorageRecord, leaseToken string, cause error) error {
	nextMs := time.Now().Add(startRetryDelay(op.GetAttempt())).UnixMilli()
	updated, uerr := u.updateOwnedStartOperation(ctx, op.GetTicketId(), leaseToken, func(rec *matchv1.MatchStartOperationStorageRecord) error {
		rec.NextAttemptAtMs = nextMs
		rec.LeaseToken = ""
		rec.LeaseDeadlineMs = 0
		return nil
	})
	if uerr != nil {
		return errors.Join(cause, uerr)
	}
	if err := u.repo.EnsureStartActive(ctx, updated.GetTicketId(), updated.GetNextAttemptAtMs()); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (u *MatchUsecase) compensateStartOperation(ctx context.Context, op *matchv1.MatchStartOperationStorageRecord, leaseToken string) error {
	var joined error
	for _, pid := range memberPlayerIDs(op.GetMembers()) {
		if err := u.repo.DeletePlayerIndexIfMatches(ctx, pid, op.GetTicketId()); err != nil {
			joined = errors.Join(joined, fmt.Errorf("rollback player %d: %w", pid, err))
		}
		if err := u.repo.DeleteStartPlayerIfMatches(ctx, pid, op.GetTicketId()); err != nil {
			joined = errors.Join(joined, fmt.Errorf("rollback start player %d: %w", pid, err))
		}
	}
	if joined == nil {
		if err := u.repo.DeleteTicket(ctx, op.GetTicketId()); err != nil {
			joined = errors.Join(joined, err)
		}
	}
	if joined != nil {
		return u.deferStartOperation(ctx, op, leaseToken, joined)
	}
	failed, err := u.updateOwnedStartOperation(ctx, op.GetTicketId(), leaseToken, func(rec *matchv1.MatchStartOperationStorageRecord) error {
		rec.Phase = matchv1.MatchStartPhase_MATCH_START_PHASE_FAILED
		rec.NextAttemptAtMs = 0
		rec.LeaseToken = ""
		rec.LeaseDeadlineMs = 0
		return nil
	})
	if err != nil {
		return err
	}
	u.pushProgress(ctx, failed.GetTicketId(), stageFailed, failed.GetMembers(), "", failed.GetMapId())
	return u.repo.RemoveStartActive(ctx, failed.GetTicketId())
}

// advanceStartOperation 推进一条 StartMatch saga。所有外部写都可幂等重放；lease 只防止
// leader 交接窗口内并行推进，lease 丢失时旧 worker 不能再提交 phase。
func (u *MatchUsecase) advanceStartOperation(ctx context.Context, current *matchv1.MatchStartOperationStorageRecord) error {
	if current == nil || startOperationTerminal(current.GetPhase()) {
		return nil
	}
	if err := u.requireLocalGameMode(current.GetGameMode()); err != nil {
		return err
	}
	nowMs := time.Now().UnixMilli()
	leaseToken := uuid.NewString()
	var op *matchv1.MatchStartOperationStorageRecord
	err := u.repo.UpdateStartOperationWithLock(ctx, current.GetTicketId(), u.cfg.OptimisticRetry, func(rec *matchv1.MatchStartOperationStorageRecord) error {
		if startOperationTerminal(rec.GetPhase()) {
			return errcode.New(errcode.ErrInvalidState, "start operation %d terminal", rec.GetTicketId())
		}
		if rec.GetNextAttemptAtMs() > nowMs || (rec.GetLeaseToken() != "" && rec.GetLeaseDeadlineMs() > nowMs) {
			return errcode.New(errcode.ErrMatchConcurrent, "start operation %d not due or leased", rec.GetTicketId())
		}
		rec.Attempt++
		rec.LeaseToken = leaseToken
		rec.LeaseDeadlineMs = nowMs + startOperationLease.Milliseconds()
		op = cloneStartOperation(rec)
		return nil
	}, u.ticketTTL())
	if err != nil {
		if errcode.As(err) == errcode.ErrInvalidState || errcode.As(err) == errcode.ErrMatchConcurrent {
			return nil
		}
		return err
	}

	if op.GetPhase() == matchv1.MatchStartPhase_MATCH_START_PHASE_COMPENSATING {
		return u.compensateStartOperation(ctx, op, leaseToken)
	}
	for _, member := range op.GetMembers() {
		existing, claimed, ierr := u.repo.ClaimStartPlayer(ctx, member.GetPlayerId(), op.GetTicketId(), u.ticketTTL())
		if ierr != nil {
			return u.deferStartOperation(ctx, op, leaseToken, ierr)
		}
		if !claimed && existing != op.GetTicketId() {
			op, err = u.updateOwnedStartOperation(ctx, op.GetTicketId(), leaseToken, func(rec *matchv1.MatchStartOperationStorageRecord) error {
				rec.Phase = matchv1.MatchStartPhase_MATCH_START_PHASE_COMPENSATING
				return nil
			})
			if err != nil {
				return err
			}
			return u.compensateStartOperation(ctx, op, leaseToken)
		}
	}

	ticket := ticketFromStartOperation(op)
	if op.GetPhase() == matchv1.MatchStartPhase_MATCH_START_PHASE_ACCEPTED {
		if err := u.repo.CreateTicketRecord(ctx, ticket, u.ticketTTL()); err != nil {
			return u.deferStartOperation(ctx, op, leaseToken, err)
		}
		op, err = u.updateOwnedStartOperation(ctx, op.GetTicketId(), leaseToken, func(rec *matchv1.MatchStartOperationStorageRecord) error {
			rec.Phase = matchv1.MatchStartPhase_MATCH_START_PHASE_TICKET_READY
			return nil
		})
		if err != nil {
			return err
		}
	}

	if op.GetPhase() == matchv1.MatchStartPhase_MATCH_START_PHASE_TICKET_READY {
		op, err = u.updateOwnedStartOperation(ctx, op.GetTicketId(), leaseToken, func(rec *matchv1.MatchStartOperationStorageRecord) error {
			rec.Phase = matchv1.MatchStartPhase_MATCH_START_PHASE_CLAIMING
			return nil
		})
		if err != nil {
			return err
		}
	}

	if op.GetPhase() == matchv1.MatchStartPhase_MATCH_START_PHASE_CLAIMING {
		for _, member := range op.GetMembers() {
			claimed, cerr := u.claimPlayerForStart(ctx, member.GetPlayerId(), op.GetTicketId())
			if cerr != nil {
				return u.deferStartOperation(ctx, op, leaseToken, cerr)
			}
			if !claimed {
				op, err = u.updateOwnedStartOperation(ctx, op.GetTicketId(), leaseToken, func(rec *matchv1.MatchStartOperationStorageRecord) error {
					rec.Phase = matchv1.MatchStartPhase_MATCH_START_PHASE_COMPENSATING
					return nil
				})
				if err != nil {
					return err
				}
				return u.compensateStartOperation(ctx, op, leaseToken)
			}
			op, err = u.updateOwnedStartOperation(ctx, op.GetTicketId(), leaseToken, func(rec *matchv1.MatchStartOperationStorageRecord) error {
				for _, pid := range rec.GetClaimedPlayerIds() {
					if pid == member.GetPlayerId() {
						return nil
					}
				}
				rec.ClaimedPlayerIds = append(rec.ClaimedPlayerIds, member.GetPlayerId())
				return nil
			})
			if err != nil {
				return err
			}
		}
		op, err = u.updateOwnedStartOperation(ctx, op.GetTicketId(), leaseToken, func(rec *matchv1.MatchStartOperationStorageRecord) error {
			rec.Phase = matchv1.MatchStartPhase_MATCH_START_PHASE_CLAIMS_READY
			return nil
		})
		if err != nil {
			return err
		}
	}

	if op.GetPhase() == matchv1.MatchStartPhase_MATCH_START_PHASE_CLAIMS_READY {
		if err := u.repo.EnqueueTicket(ctx, ticket); err != nil {
			return u.deferStartOperation(ctx, op, leaseToken, err)
		}
		var cleanupErr error
		for _, playerID := range memberPlayerIDs(op.GetMembers()) {
			cleanupErr = errors.Join(cleanupErr, u.repo.DeleteStartPlayerIfMatches(ctx, playerID, op.GetTicketId()))
		}
		if cleanupErr != nil {
			return u.deferStartOperation(ctx, op, leaseToken, cleanupErr)
		}
		op, err = u.updateOwnedStartOperation(ctx, op.GetTicketId(), leaseToken, func(rec *matchv1.MatchStartOperationStorageRecord) error {
			rec.Phase = matchv1.MatchStartPhase_MATCH_START_PHASE_QUEUED
			rec.NextAttemptAtMs = 0
			rec.LeaseToken = ""
			rec.LeaseDeadlineMs = 0
			return nil
		})
		if err != nil {
			return err
		}
		u.pushProgress(ctx, op.GetTicketId(), stageQueueing, op.GetMembers(), "", op.GetMapId())
		// QUEUED is an explicit ownership handoff: the durable ticket + player
		// claims are now canonical. Delete the start operation instead of waiting
		// for a cache TTL to imply completion.
		if err := u.repo.DeleteStartOperation(ctx, op.GetTicketId()); err != nil {
			return err
		}
		plog.With(ctx).Debugw("msg", "match_start_queued", "ticket_id", op.GetTicketId(), "operation_id", op.GetOperationId())
	}
	return nil
}

// resolveMembers 根据 team 快照构造 match 成员列表 + 计算平均 MMR。
// reader 为 nil 时退化为"仅 captain 单人票据"(本机不起 team 的骨架联调路径)。
func (u *MatchUsecase) resolveMembers(ctx context.Context, teamID, captainID uint64, mapID uint32) ([]*matchv1.MatchMemberStorageRecord, int32, error) {
	// teamID==0 = 单人入口(单排撮合 / 单人直进副本):名单就是调用者本人,不查 team 服务。
	// 「单人」与「单人组队」在协议层是同一件事——都是一张票据带 1 个成员——所以不该
	// 强迫玩家先去组一个 1 人队(那只是用组队机制模拟单人,多一次 RPC 和一个失败点)。
	// 无需按 team_size 设限:撮合按**人数**凑齐 side_count×team_size 后由 binPack 装箱分方,
	// 5 个单排票天然能凑满一方,与「3 人队 + 2 人散排」同一条路径。
	// captainID 取自 JWT sub(service 层),客户端无法伪造他人身份入队。
	if teamID == 0 || u.reader == nil {
		m := []*matchv1.MatchMemberStorageRecord{{PlayerId: captainID, TeamId: teamID, Confirm: confirmPending}}
		return m, 0, nil
	}

	// 在 team 的乐观锁内冻结名单(见 TeamReader.BeginTeamMatch)。队长 / 存在性校验
	// 都在那把锁里 —— 在这里再查一遍只会重新打开刚消灭的窗口。ready 门槛已删
	// (2026-08-17,LoL 式流程):FORMING 队伍直接放行,「不在场者进不了局」由本服务的
	// ensureAllPresent 在线闸与撮合确认期(CONFIRM)承担。
	// 租约用 rosterLockLeaseMs:够覆盖本函数返回后到 ClaimPlayer 落地这一小段即可。
	team, readyGen, err := u.reader.BeginTeamMatch(ctx, teamID, captainID, rosterLockOperationID(teamID, captainID), rosterLockLeaseMs)
	if err != nil {
		plog.With(ctx).Warnw("msg", "match_roster_freeze_failed",
			"reason", "team_begin_match_rejected", "code", int(errcode.As(err)),
			"team_id", teamID, "captain_id", captainID, "map_id", mapID, "err", err)
		return nil, 0, err
	}
	if teamSize := u.teamSizeForMap(mapID); len(team.Members) == 0 || len(team.Members) > teamSize {
		plog.With(ctx).Warnw("msg", "match_roster_size_rejected",
			"reason", "team_size_out_of_range", "team_id", teamID, "captain_id", captainID,
			"members", len(team.Members), "map_id", mapID, "team_size", teamSize)
		return nil, 0, errcode.New(errcode.ErrMatchTeamNotReady, "team %d invalid size %d (map %d team_size %d)",
			teamID, len(team.Members), mapID, teamSize)
	}

	members := make([]*matchv1.MatchMemberStorageRecord, 0, len(team.Members))
	var sum int32
	for _, tm := range team.Members {
		members = append(members, &matchv1.MatchMemberStorageRecord{
			PlayerId: tm.PlayerId,
			TeamId:   teamID,
			Mmr:      tm.Mmr,
			HeroId:   tm.HeroId,
			Confirm:  confirmPending,
			// 冻结名单那一刻的 team ready 代际;ReleaseMatch 回传给 EndTeamMatch 做跨代 CAS。
			TeamReadyGeneration: readyGen,
		})
		sum += tm.Mmr
	}
	avg := sum / int32(len(members))
	return members, avg, nil
}

// ── RPC 2:CancelMatch ────────────────────────────────────────────────────────

// CancelMatch 取消匹配。以 playerID 为准定位其当前票据:
//   - 票据仍在排队(未撮合)→ CAS 条件删票据 + 释放成员归属
//   - 票据已进 match(确认期)→ 等价于该玩家拒绝确认,走 match 失败流程
//
// 排队路径用 DeleteTicketIfUnmatched(WATCH CAS)而非"读到 match_id==0 就盲删":
// 否则在读与删之间撮合循环可能刚好 ReserveTicket,盲删会把已进 match 的票据删掉并释放
// 成员 claim → 玩家可再排队,同人两场(违反不变量 §1)。CAS 撞上并发预留时按拒绝确认处理。
func (u *MatchUsecase) CancelMatch(ctx context.Context, playerID uint64) error {
	// A pre-queue saga may already have created the normal player claim. Prefer
	// cancelling the start operation while its discoverability index exists;
	// deleting only the ticket/claim would let that durable worker recreate them
	// and enqueue after this RPC returned success.
	if handled, startErr := u.cancelStartingMatch(ctx, playerID); startErr != nil {
		if errcode.As(startErr) != errcode.ErrMatchConcurrent {
			return startErr
		}
		// QUEUED handoff raced us: fall through to the canonical ticket path.
	} else if handled {
		return nil
	}

	ticketID, found, err := u.repo.GetPlayerTicket(ctx, playerID)
	if err != nil {
		return err
	}
	if !found {
		return errcode.New(errcode.ErrMatchNotFound, "player %d not in any queue", playerID)
	}
	ticket, found, err := u.repo.GetTicket(ctx, ticketID)
	if err != nil {
		return err
	}
	if !found {
		// 票据已消失(过期),清理残留 player index(CAS:仅当仍指向这张旧票,防误删并发新 claim)
		_ = u.repo.DeletePlayerIndexIfMatches(ctx, playerID, ticketID)
		return errcode.New(errcode.ErrMatchNotFound, "ticket %d gone", ticketID)
	}
	if err := u.requireLocalGameMode(ticket.GetGameMode()); err != nil {
		return err
	}

	// 已被撮合进 match → 视为拒绝确认;match 已死(记录消失/已失败)则清理孤儿票据
	if ticket.MatchId != 0 {
		return u.rejectOrReapOrphan(ctx, playerID, ticket.MatchId)
	}

	// 仍在排队 → CAS 条件删票(仅当仍未撮合)+ 释放全体成员归属
	deleted, reservedMatch, derr := u.repo.DeleteTicketIfUnmatched(ctx, ticketID)
	if derr != nil {
		return derr
	}
	if !deleted {
		if reservedMatch != 0 {
			// 读后被撮合循环抢先预留 → 转拒绝确认路径(match 失败,其余票据退回队列)
			return u.rejectOrReapOrphan(ctx, playerID, reservedMatch)
		}
		// 票据刚好过期/被他处删除:清理残留 player index(CAS 同上),幂等返回
		_ = u.repo.DeletePlayerIndexIfMatches(ctx, playerID, ticketID)
		return errcode.New(errcode.ErrMatchNotFound, "ticket %d gone", ticketID)
	}
	u.rollbackClaims(ctx, ticketID, memberPlayerIDs(ticket.Members))
	// FAILED 补推给票据全体成员:取消可能不是本人发起(队长取消 / team 离队联动撤票),
	// 其余队友的客户端仍停在 QUEUEING,不推会一直转圈直到 GetMatchProgress 兜底轮询。
	u.pushProgress(ctx, ticket.TicketId, stageFailed, ticket.Members, "", ticket.MapId)
	plog.With(ctx).Debugw("msg", "match_cancel", "ticket_id", ticketID, "player_id", playerID)
	return nil
}

// cancelStartingMatch records cancellation against the durable StartMatch saga before
// the normal player->ticket claim exists. This closes the ACCEPTED/TICKET_READY/
// CLAIMING window where a cold-start ResumeContext can expose STARTING but the old
// CancelMatch path used to answer NOT_FOUND and let the worker enqueue afterwards.
//
// The phase CAS is the cancellation commit point. Clearing the lease fences an
// already-running worker: any later phase write made with its stale lease token is
// rejected, while external writes it may already have completed are removed by the
// idempotent COMPENSATING worker. The due index is derived; once the canonical phase
// is committed, an index write failure must not turn an accepted cancellation into a
// false RPC failure because the reconciler can rebuild that index.
func (u *MatchUsecase) cancelStartingMatch(ctx context.Context, playerID uint64) (bool, error) {
	ticketID, found, err := u.repo.GetStartPlayerOperation(ctx, playerID)
	if err != nil || !found {
		return false, err
	}
	op, found, err := u.repo.GetStartOperation(ctx, ticketID)
	if err != nil {
		return false, err
	}
	if !found {
		if err := u.repo.DeleteStartPlayerIfMatches(ctx, playerID, ticketID); err != nil {
			return false, err
		}
		return false, nil
	}
	if memberIndex(op.GetMembers(), playerID) < 0 {
		return false, errcode.New(errcode.ErrUnavailable,
			"start player index %d points to unrelated operation %d", playerID, ticketID)
	}
	if err := u.requireLocalGameMode(op.GetGameMode()); err != nil {
		return false, err
	}

	nowMs := time.Now().UnixMilli()
	committed := false
	err = u.repo.UpdateStartOperationWithLock(ctx, ticketID, u.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStartOperationStorageRecord) error {
			if memberIndex(rec.GetMembers(), playerID) < 0 {
				return errcode.New(errcode.ErrUnavailable,
					"start operation %d no longer owns player %d", ticketID, playerID)
			}
			switch rec.GetPhase() {
			case matchv1.MatchStartPhase_MATCH_START_PHASE_QUEUED:
				// Ownership is handing off to the canonical ticket. The client must
				// requery/retry instead of treating this race as a terminal success.
				return errcode.New(errcode.ErrMatchConcurrent,
					"start operation %d already handed off to queue", ticketID)
			case matchv1.MatchStartPhase_MATCH_START_PHASE_FAILED:
				committed = true
				return nil
			case matchv1.MatchStartPhase_MATCH_START_PHASE_COMPENSATING:
				committed = true
			default:
				rec.Phase = matchv1.MatchStartPhase_MATCH_START_PHASE_COMPENSATING
				committed = true
			}
			rec.NextAttemptAtMs = nowMs
			rec.LeaseToken = ""
			rec.LeaseDeadlineMs = 0
			return nil
		}, u.ticketTTL())
	if err != nil {
		return false, err
	}
	if !committed {
		return false, errcode.New(errcode.ErrMatchConcurrent,
			"start operation %d cancellation not committed", ticketID)
	}
	if err := u.repo.EnsureStartActive(ctx, ticketID, nowMs); err != nil {
		plog.With(ctx).Warnw("msg", "match_start_cancel_index_deferred",
			"ticket_id", ticketID, "player_id", playerID, "err", err)
	}
	plog.With(ctx).Debugw("msg", "match_start_cancel_accepted",
		"ticket_id", ticketID, "player_id", playerID)
	return true, nil
}

// rejectOrReapOrphan 把"已被 match 预留的票据"的取消转成拒绝确认;若 match 已死则收割孤儿票据。
//
// match 已死的两种形态(都是崩溃残留,正常流程不会出现):
//   - ErrMatchNotFound:match 记录不存在——formMatch 已改为「先建 match 再预留票据」,
//     只有回滚中途崩溃 / match 被释放但票据残留才会走到;
//   - ErrMatchDeclined:match 已 FAILED——写 FAILED 后、onMatchFailed 退票完成前崩溃,
//     本票据错过了退回队列。
//
// 两种情况下票据都既不在队列也不受超时扫描,成员 claim 卡到 TTL(30min);玩家意图
// 本就是取消,直接删票 + 释放归属 + 推 FAILED,让全员立刻可再匹配。
// 安全守卫:重读票据,仅当其仍归属该 match 才收割,并发变化时原样返错不误删。
func (u *MatchUsecase) rejectOrReapOrphan(ctx context.Context, playerID, matchID uint64) error {
	err := u.ConfirmMatch(ctx, playerID, matchID, false)
	code := errcode.As(err)
	if err == nil || (code != errcode.ErrMatchNotFound && code != errcode.ErrMatchDeclined) {
		return err
	}
	tid, found, gerr := u.repo.GetPlayerTicket(ctx, playerID)
	if gerr != nil || !found {
		return err // 已无归属可清理,原样返回
	}
	ticket, found, gerr := u.repo.GetTicket(ctx, tid)
	if gerr != nil {
		return gerr
	}
	if !found {
		_ = u.repo.DeletePlayerIndexIfMatches(ctx, playerID, tid)
		return nil // claim 指向已消失的票据:顺手清理(CAS 防误删并发新 claim),取消语义成立
	}
	if ticket.MatchId != matchID {
		return err // 票据已归属他处(并发变化),不误删
	}
	if derr := u.repo.DeleteTicket(ctx, tid); derr != nil {
		return derr
	}
	u.rollbackClaims(ctx, tid, memberPlayerIDs(ticket.Members))
	u.pushProgress(ctx, tid, stageFailed, ticket.Members, "", ticket.MapId)
	plog.With(ctx).Warnw("msg", "match_cancel_reaped_orphan_ticket",
		"ticket_id", tid, "match_id", matchID, "player_id", playerID)
	return nil
}

// ── 对局结束释放:ReleaseMatch ────────────────────────────────────────────────

// ReleaseMatch 释放一场已结束(结算 / abandoned)对局的全部撮合状态,由 battle_result 在
// 结算落库后调用(后端内部接口,不带玩家 JWT)。对局走完 READY → 进战斗 → 结算后,
// onAllConfirmed 故意保留的 player→ticket 归属(SETNX claim)+ 票据 + match 镜像仍是
// **非终态、无 TTL 的持久状态**(MatchRepo.ClaimPlayer 契约:「网络断开或进程停机绝不能靠 TTL
// 暗中释放玩家」;ticket_ttl / match_ttl 只在**显式终态后**用于留存)。
// 因此本接口是这些状态的**唯一释放点**:不调用就不会随时间自愈,玩家回 Hub 再次 StartMatch
// 会被 ClaimPlayer SETNX 撞上残留 claim,永久报 ErrMatchAlreadyMatching(4002)——释放失败必须
// 靠 battle_result outbox 持续重试到成功,不能当作"等等就好"。
//
// 释放对象全部幂等；任一步失败会聚合返回，让 battle_result outbox 持续重试。
//   - 每个成员的 player→ticket 归属(仅当其当前 claim 仍指向本局票据时才删,避免误删
//     玩家结算后已经发起的新一局 claim)
//   - 本局全部排队票据(ticket record + queue ZSET 残留)
//   - match 镜像 + active 索引
//
// fallbackPlayerIDs:battle_result 从 BattleResult.stats 带来的玩家名单。match 镜像若已不存在
// (已终态留存到期 / 曾被部分释放),仍可凭它兜底清掉残留 claim(只删确属本局的,见 releasePlayerClaim)。
func (u *MatchUsecase) ReleaseMatch(ctx context.Context, matchID uint64, fallbackPlayerIDs []uint64) error {
	if matchID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "match_id required")
	}

	// 收集成员 + 本局票据(match 镜像若已过期则仅靠 fallback 兜底清 claim)。
	playerSet := make(map[uint64]struct{})
	// teamRosters:本局 roster 按队伍分组,供释放时复位各队的准备状态
	// (INC-20260813-001 第一根因:此前 team 侧没有任何 match-ended 路径)。
	teamRosters := make(map[uint64]*teamRoster)
	var ticketIDs []uint64
	matchFound := false

	if m, found, err := u.repo.GetMatch(ctx, matchID); err != nil {
		return err
	} else if found {
		matchFound = true
		ticketIDs = m.TicketIds
		for _, pid := range memberPlayerIDs(m.Members) {
			playerSet[pid] = struct{}{}
		}
		collectTeamRosters(teamRosters, m.Members)
	}
	for _, pid := range fallbackPlayerIDs {
		if pid != 0 {
			playerSet[pid] = struct{}{}
		}
	}

	// canonical match 已缺失时，只能从 fallback roster 建立机械证明：claim
	// 精确指向 tid、tid 精确声明 expected match，且 player 确实在该 ticket
	// roster。任何缺票/损坏都是 UNKNOWN，不能猜成“这是旧局 claim”。
	ticketSet := make(map[uint64]struct{}, len(ticketIDs))
	for _, tid := range ticketIDs {
		ticketSet[tid] = struct{}{}
	}
	if !matchFound {
		fallbackSnapshot := make([]uint64, 0, len(playerSet))
		for pid := range playerSet {
			fallbackSnapshot = append(fallbackSnapshot, pid)
		}
		for _, pid := range fallbackSnapshot {
			tid, claimed, claimErr := u.repo.GetPlayerTicket(ctx, pid)
			if claimErr != nil {
				return fmt.Errorf("discover player %d claim for missing match %d: %w", pid, matchID, claimErr)
			}
			if !claimed {
				continue
			}
			ticket, found, ticketErr := u.repo.GetTicket(ctx, tid)
			if ticketErr != nil {
				return fmt.Errorf("discover ticket %d for missing match %d: %w", tid, matchID, ticketErr)
			}
			if !found {
				return errcode.New(errcode.ErrUnavailable,
					"missing match %d player %d claim points to missing ticket %d", matchID, pid, tid)
			}
			if ticket.GetMatchId() != matchID {
				// Exact proof that this is a newer/different operation; leave it intact.
				continue
			}
			if memberIndex(ticket.GetMembers(), pid) < 0 {
				return errcode.New(errcode.ErrUnavailable,
					"missing match %d player %d is not owner/member of claimed ticket %d", matchID, pid, tid)
			}
			if _, exists := ticketSet[tid]; !exists {
				ticketSet[tid] = struct{}{}
				ticketIDs = append(ticketIDs, tid)
			}
			for _, member := range ticket.GetMembers() {
				if member.GetPlayerId() != 0 {
					playerSet[member.GetPlayerId()] = struct{}{}
				}
			}
			collectTeamRosters(teamRosters, ticket.GetMembers())
		}
	}

	var joined error
	// WATCH/CAS 删确属本局的票据。先完成全部 ticket phase；任一漂移/错误时
	// 不进入 claim phase，canonical/outbox 会保留并重试。
	for _, tid := range ticketIDs {
		_, found, currentMatchID, err := u.repo.DeleteTicketIfMatch(ctx, tid, matchID)
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("compare-delete ticket %d for match %d: %w", tid, matchID, err))
			continue
		}
		if found && currentMatchID != matchID {
			joined = errors.Join(joined, errcode.New(errcode.ErrUnavailable,
				"ticket %d drifted from match %d to match %d", tid, matchID, currentMatchID))
		}
	}
	if joined != nil {
		return joined
	}

	// 删每个成员的 player→ticket 归属(仅当确属本局,防误删结算后新一局 claim)。
	for pid := range playerSet {
		if err := u.releasePlayerClaim(ctx, matchID, pid, ticketSet); err != nil {
			joined = errors.Join(joined, err)
		}
	}

	// 任一票据/claim 状态未知时保留 canonical match，供 outbox 下轮按同一证明重试。
	if joined != nil {
		return joined
	}

	// 复位各队的准备状态(INC-20260813-001 第一根因)。
	//
	// 放在 claim 释放**之后**、DeleteMatch **之前**:
	//   - 之后 —— claim 是玩家能不能开下一局的硬闸,不能被队伍状态的可用性拖住;
	//   - 之前 —— 失败时保留 canonical match,让 battle_result 的 outbox 按同一份 roster
	//     重投重试。吞掉这一步就等于让队伍永远停在 READY,那正是本事故的第一根因。
	// team 侧对「已复位 / 队伍已解散 / 成员已离队」一律返回 OK,不会让 outbox 空转。
	if err := u.endTeamMatches(ctx, matchID, teamRosters); err != nil {
		return err
	}

	// 所有成员清理明确成功后，才硬删 match 镜像 + 移出 active。
	if err := u.repo.DeleteMatch(ctx, matchID); err != nil {
		return err
	}

	plog.With(ctx).Infow("msg", "match_released", "match_id", matchID,
		"match_found", matchFound, "players", len(playerSet), "tickets", len(ticketIDs),
		"teams", len(teamRosters))
	return nil
}

// collectTeamRosters 把成员按 team_id 分组累加(去重)。team_id==0 的成员是单人入口,
// 没有队伍可复位,直接跳过。
// teamRoster 是「本局属于某支队伍的成员 + 冻结时的 ready 代际」。
//
// 代际必须跟着 roster 一起带到 EndTeamMatch —— 它是跨代幂等的唯一凭据:
// outbox 重投时若玩家已重新点准备 / 队长已开新局,代际早已前进,CAS 失败即 no-op,
// 不会抹掉新意图(INC-20260813-001 ①)。
type teamRoster struct {
	players []uint64
	// readyGeneration 取自本队任一成员(同队必然同值,都来自同一次 BeginTeamMatch)。
	// 0 = 旧记录 / 旧 team 服务没回代际,EndTeamMatch 退化为「只在还挂着 ready 时复位」。
	readyGeneration uint64
}

func collectTeamRosters(out map[uint64]*teamRoster, members []*matchv1.MatchMemberStorageRecord) {
	for _, m := range members {
		teamID, pid := m.GetTeamId(), m.GetPlayerId()
		if teamID == 0 || pid == 0 {
			continue
		}
		r := out[teamID]
		if r == nil {
			r = &teamRoster{}
			out[teamID] = r
		}
		if r.readyGeneration == 0 {
			r.readyGeneration = m.GetTeamReadyGeneration()
		}
		exists := false
		for _, have := range r.players {
			if have == pid {
				exists = true
				break
			}
		}
		if !exists {
			r.players = append(r.players, pid)
		}
	}
}

// endTeamMatches 逐队复位准备状态。任一队失败即整体返回错误,由上游 outbox 重投。
// reader 未注入(team_addr 未配的骨架联调)时整段跳过,行为与本链落地前一致。
//
// # 滚动升级共存窗口刻意做成「无顺序依赖」(§9.21)
//
// 本调用是 matchmaker 新增的**跨服务出站依赖**。若把 team 未升级(Unimplemented)也当成
// 普通失败,就等于给发布引入一条「team 必须先于 matchmaker 上线」的顺序约束 ——
// 而 §9.21 要求滚动升级双向兼容,不得依赖发布顺序;真依赖顺序的话,顺序一旦搞错,
// outbox 会一直空转、canonical match 持续积压,且**没有任何机械手段能拦住这种搞错**。
//
// 所以这里把两类错误分开:
//   - `ErrNotImplemented`(对端还没这个能力)→ **弱依赖降级**:Warn + 跳过。
//     降级是安全的 —— 跳过的后果恰好等于本修复落地之前的行为(队伍停在 READY),
//     不产生任何新的错误状态;而 team 一旦滚上新版本,此后每一局的释放都会正常复位。
//     反过来若在这里 fail-closed,团队状态**同样**停在 READY(调用根本没成功),
//     只是额外多了一堆积压 —— 严格更差。
//   - 其它任何错误(team 挂了 / 超时 / 业务冲突)→ 如实上抛,由 outbox 重投到成功。
func (u *MatchUsecase) endTeamMatches(ctx context.Context, matchID uint64, rosters map[uint64]*teamRoster) error {
	if u.reader == nil || len(rosters) == 0 {
		return nil
	}
	var joined error
	for teamID, r := range rosters {
		players := r.players
		err := u.reader.EndTeamMatch(ctx, teamID, players, r.readyGeneration)
		if err == nil {
			continue
		}
		if errcode.As(err) == errcode.ErrNotImplemented {
			// 共存窗口:重试永远不会成功,重投只会积压。
			plog.With(ctx).Warnw("msg", "match_release_end_team_unsupported",
				"match_id", matchID, "team_id", teamID, "err", err,
				"hint", "team 尚未滚到带 EndTeamMatch 的版本;本局队伍不复位准备状态(等同修复前行为),team 升级后自动恢复")
			continue
		}
		plog.With(ctx).Warnw("msg", "match_release_end_team_failed",
			"match_id", matchID, "team_id", teamID, "players", len(players), "err", err,
			"hint", "队伍会停在 READY,队长可能带着还没回大厅的队友再开一局;outbox 会重试")
		joined = errors.Join(joined, fmt.Errorf("end team match team=%d: %w", teamID, err))
	}
	return joined
}

// releasePlayerClaim 释放单个玩家的 player→ticket 归属,但仅当其当前 claim 确属本局
// (claim 指向的票据 ∈ 本局票据,或该票据的 match_id == 本局)。玩家若已发起新一局,
// 其 claim 指向新票据(不同 match_id / 不在本局票据集),此处不动,避免误删新 claim。
func (u *MatchUsecase) releasePlayerClaim(ctx context.Context, matchID, playerID uint64, ticketSet map[uint64]struct{}) error {
	tid, ok, err := u.repo.GetPlayerTicket(ctx, playerID)
	if err != nil {
		return fmt.Errorf("get player %d claim for match %d: %w", playerID, matchID, err)
	}
	if !ok {
		return nil // claim 已释放
	}
	belongs := false
	if _, in := ticketSet[tid]; in {
		// Exact ticket IDs are globally non-reusable. Still fail closed if a
		// conflicting record is visible before the claim CAS (defends rollout or
		// operator mistakes that violate that invariant).
		current, found, gerr := u.repo.GetTicket(ctx, tid)
		if gerr != nil {
			return fmt.Errorf("recheck ticket %d for player %d release: %w", tid, playerID, gerr)
		}
		if found && current.GetMatchId() != matchID {
			return errcode.New(errcode.ErrUnavailable,
				"player %d claim ticket %d was reused by match %d", playerID, tid, current.GetMatchId())
		}
		belongs = true
	} else {
		t, found, gerr := u.repo.GetTicket(ctx, tid)
		if gerr != nil {
			return fmt.Errorf("get ticket %d for player %d release: %w", tid, playerID, gerr)
		}
		if !found {
			return errcode.New(errcode.ErrUnavailable,
				"player %d claim points to unproven missing ticket %d while releasing match %d", playerID, tid, matchID)
		}
		if t.GetMatchId() == matchID {
			// A same-match edge appeared after the discovery phase. Retry from the
			// beginning so its ticket is conditionally deleted before its claim.
			return errcode.New(errcode.ErrUnavailable,
				"late ticket %d for match %d requires release rediscovery", tid, matchID)
		}
	}
	if !belongs {
		// claim 指向别的票据(玩家结算后已发起新一局)→ 不误删。
		plog.With(ctx).Infow("msg", "release_skip_stale_claim", "match_id", matchID, "player_id", playerID, "current_ticket", tid)
		return nil
	}
	// CAS 删:读 belongs 判定与删之间 claim 仍可能被替换(过期后新一局写入),仅当仍指向 tid 才删。
	if err := u.repo.DeletePlayerIndexIfMatches(ctx, playerID, tid); err != nil {
		return fmt.Errorf("compare-delete player %d claim ticket %d: %w", playerID, tid, err)
	}
	return nil
}

// ── RPC 3:ConfirmMatch ───────────────────────────────────────────────────────

// ConfirmMatch 确认/拒绝匹配。
//   - accept=false 或任一人拒绝 → match FAILED,其余票据退回队列(保留排队时长)
//   - 全员 accept → ALLOCATING → 拉 DS → READY
func (u *MatchUsecase) ConfirmMatch(ctx context.Context, playerID, matchID uint64, accept bool) error {
	const (
		outcomePending  = 0
		outcomeFailed   = 1
		outcomeAllReady = 2
	)
	outcome := outcomePending
	var snapshot *matchv1.MatchStorageRecord

	err := u.repo.UpdateMatchWithLock(ctx, matchID, u.cfg.OptimisticRetry, func(m *matchv1.MatchStorageRecord) error {
		if err := u.requireLocalGameMode(m.GetGameMode()); err != nil {
			return err
		}
		// 终态幂等:已失败返回 declined。已锁定(分配中/就绪):accept 幂等成功;
		// reject 诚实报错——全员已确认后不可再反悔,若假装成功,客户端以为已取消,
		// 随后却收到 READY 推送被拉进战斗,UI 状态机错乱。
		if m.Stage == stageFailed {
			return errcode.New(errcode.ErrMatchDeclined, "match %d already failed", matchID)
		}
		if m.Stage == stageAllocating || m.Stage == stageReady {
			if !accept {
				// INC-20260724-001:ALLOCATING 期必须给玩家一个真实出口(§9.20 禁止"按钮不可用 /
				// 只能杀进程恢复",§9.23 长期无容量时只能停在可退出的形态)。
				// 背景:DS 缺容 / k8s 控制面超时时分配会长时间重试(ds_allocate_timeout 默认 60s/dev 150s ×
				// 无墙钟上限),而 expireOnce 对 stageAllocating 显式 keepActive 永不判失败;
				// 此前唯一会终止 ALLOCATING 的是成局最终门的 presence 误杀,该门已因本事故关闭
				// ⇒ 不补出口就会把"误杀"换成"永久卡死"。
				// 边界:仅允许 **未 checkpoint** 的 ALLOCATING 取消(BattleTarget==nil 且
				// phase ∈ {PENDING,REQUESTING})。已 checkpoint / ABORTING / READY 一律维持
				// 既有拒绝语义——那时票已签或 DS 已固化,假装取消会让客户端与 READY 推送打架。
				// 并发安全:本闭包在 UpdateMatchWithLock 的 WATCH/CAS 内,冲突会用新快照重跑;
				// 与分配 worker 的 checkpointBattleAllocation 互斥——后者写 BattleTarget 前要过
				// exactUncheckpointedRequestingAllocation(要求 rec 仍是 exact REQUESTING),
				// 本处一旦把 stage/phase 翻成 FAILED,它必返 ErrMatchConcurrent 且不写 target;
				// 反之若它先赢,本处重跑会看到 BattleTarget != nil 而拒绝取消。二者只有一个成功。
				if idx := memberIndex(m.Members, playerID); idx >= 0 && cancelableUncheckpointedAllocation(m) {
					m.Members[idx].Confirm = confirmRejected
					m.Stage = stageFailed
					m.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_FAILED
					outcome = outcomeFailed
					snapshot = cloneMatch(m)
					return nil
				}
				return errcode.New(errcode.ErrInvalidState,
					"match %d locked (stage=%d, alloc_phase=%d), cannot reject",
					matchID, m.Stage, m.AllocationPhase)
			}
			snapshot = cloneMatch(m)
			outcome = outcomePending
			return nil
		}
		idx := memberIndex(m.Members, playerID)
		if idx < 0 {
			return errcode.New(errcode.ErrMatchNotFound, "player %d not in match %d", playerID, matchID)
		}

		if !accept {
			m.Members[idx].Confirm = confirmRejected
			m.Stage = stageFailed
			outcome = outcomeFailed
			snapshot = cloneMatch(m)
			return nil
		}

		m.Members[idx].Confirm = confirmAccepted
		if allAccepted(m.Members) {
			m.Stage = stageAllocating
			if m.AllocationOperationId == "" {
				m.AllocationOperationId = allocationOperationID()
			}
			m.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_PENDING
			m.AllocationNextAttemptAtMs = time.Now().UnixMilli()
			outcome = outcomeAllReady
		} else {
			m.Stage = stageConfirm
			outcome = outcomePending
		}
		snapshot = cloneMatch(m)
		return nil
	}, u.matchTTL())
	if err != nil {
		return err
	}

	switch outcome {
	case outcomeFailed:
		if cleanupErr := u.onMatchFailed(ctx, snapshot, playerID); cleanupErr != nil {
			return cleanupErr
		}
	case outcomeAllReady:
		// durable handoff：最后一名确认者只提交 ALLOCATING job。Allocate/placement/READY
		// 由 RunMatchLoop 的服务生命周期 worker 推进，不再绑定玩家 RPC ctx。
		plog.With(ctx).Debugw("msg", "match_allocation_queued", "match_id", matchID,
			"operation_id", snapshot.GetAllocationOperationId())
	default:
		// 仍有人未确认:推 CONFIRM 进度给全体
		if snapshot != nil && snapshot.Stage == stageConfirm {
			u.pushProgress(ctx, matchID, stageConfirm, snapshot.Members, "", snapshot.MapId)
		}
	}
	plog.With(ctx).Debugw("msg", "match_confirm", "match_id", matchID, "player_id", playerID,
		"accept", accept, "outcome", outcome)
	return nil
}

// queueAcceptedMatchAllocation is the formation commit point for auto-confirm
// and solo matches. The canonical match is first created in CONFIRM, then every
// ticket reservation and player claim is made durable, and only then can this
// helper CAS it to ALLOCATING. A process crash before this point therefore
// cannot start a Battle DS for a partially formed match.
//
// The operation is idempotent so the canonical reconciler can finish the exact
// same handoff after a process crash or a lost Redis acknowledgement.
func (u *MatchUsecase) queueAcceptedMatchAllocation(
	ctx context.Context,
	candidate *matchv1.MatchStorageRecord,
) (*matchv1.MatchStorageRecord, error) {
	if candidate == nil || candidate.GetMatchId() == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "accepted match required")
	}
	if candidate.GetStage() != stageConfirm || !allAccepted(candidate.GetMembers()) {
		return nil, errcode.New(errcode.ErrInvalidState,
			"match %d is not a fully accepted formation", candidate.GetMatchId())
	}
	// This also repairs a claim persistence ACK loss. Missing/drifted ticket
	// reservations remain retryable and block the transition fail-closed.
	if err := u.ensureMatchDiscovery(ctx, candidate); err != nil {
		return nil, err
	}

	var queued *matchv1.MatchStorageRecord
	err := u.repo.UpdateMatchWithLock(ctx, candidate.GetMatchId(), u.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			switch rec.GetStage() {
			case stageAllocating, stageReady:
				// A previous attempt may have committed and lost its ACK.
				queued = cloneMatch(rec)
				return nil
			case stageConfirm:
				if !allAccepted(rec.GetMembers()) {
					return errcode.New(errcode.ErrInvalidState,
						"match %d no longer fully accepted", rec.GetMatchId())
				}
			default:
				return errcode.New(errcode.ErrInvalidState,
					"match %d stage=%d cannot enter allocation", rec.GetMatchId(), rec.GetStage())
			}
			rec.Stage = stageAllocating
			if rec.AllocationOperationId == "" {
				rec.AllocationOperationId = allocationOperationID()
			}
			rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_PENDING
			rec.AllocationNextAttemptAtMs = time.Now().UnixMilli()
			queued = cloneMatch(rec)
			return nil
		}, u.matchTTL())
	if err != nil {
		return nil, err
	}
	if queued == nil {
		return nil, errcode.New(errcode.ErrMatchConcurrent,
			"match %d allocation handoff not committed", candidate.GetMatchId())
	}
	return queued, nil
}

// onMatchFailed 处理确认失败:无过错票据退回队列,过错票据(拒绝者 / 超时未确认者)
// 删除并释放归属,推 FAILED 进度。
//
// 定责规则:
//   - 显式拒绝(rejecterID!=0):仅拒绝者所在票据过错,其余(含尚未点确认的)退回队列。
//   - 超时(rejecterID==0):含未确认(AFK)成员的票据过错,否则低在线时段同一批人 +
//     同一个挂机者会立刻重新凑成同一场 → 15s 超时 → 再凑,无限循环,其余 9 人被永远
//     劫持在“FOUND→超时”里(典型“匹配不了”)。被判责成员收到 FAILED 后可自行再排。
//
// 守卫:只处理仍归属本 match 的票据(match_id 相等)。已被并发退回/归属他局的票据盲写
// 会把他局在进票据抽回队列(违反不变量 §1),一律跳过;也使本函数可幂等重跑
// (expireOnce 对 FAILED 残留的补偿依赖此性质)。
func (u *MatchUsecase) onMatchFailed(ctx context.Context, m *matchv1.MatchStorageRecord, rejecterID uint64) error {
	confirmOf := make(map[uint64]matchv1.MatchConfirmStatus, len(m.Members))
	for _, mem := range m.Members {
		confirmOf[mem.PlayerId] = mem.Confirm
	}

	err := u.failMatch(ctx, m, func(tid uint64, ticket *matchv1.MatchTicketStorageRecord) bool {
		if rejecterID != 0 {
			return memberIndex(ticket.GetMembers(), rejecterID) >= 0
		}
		return !ticketAllAccepted(ticket, confirmOf)
	})
	plog.With(ctx).Infow("msg", "match_failed", "match_id", m.MatchId, "rejecter_id", rejecterID)
	return err
}

// onMatchNoCapacity 容量耗尽(allocator 确定性 5001/5002)的失败收尾(anti-abuse §6
// 第 3 项)。与 onMatchFailed 的区别是玩家视角**不是失败**:票据自动退回队列、撮合循环
// 稍后自动重试,客户端只看到带 estimated_wait_seconds 倒计时的 QUEUEING,不闪 FAILED。
// 同时给每张票布设 no_capacity_requeue_delay 静默窗:满载期把「重成局→分配→再 5001」
// 的空转节拍从 match_interval(2s)压到静默窗,免得撮合循环持续打爆 allocator。
// match 记录本身仍 CAS 成 FAILED(内部终态,调用方已完成)——玩家可见语义与内部终态
// 解耦,新一轮成局会换新 match_id(见 formSoloMatch/formMatch)。
func (u *MatchUsecase) onMatchNoCapacity(ctx context.Context, m *matchv1.MatchStorageRecord) error {
	delay := u.cfg.NoCapacityRequeueDelay.Std()
	if delay <= 0 {
		delay = u.cfg.MatchFormCooldown.Std() // 显式关闭时退化为普通成局节拍
	}
	if u.entryLimiter != nil && delay > 0 {
		for _, tid := range m.GetTicketIds() {
			if err := u.entryLimiter.ArmFormCooldown(ctx, tid, delay); err != nil {
				plog.With(ctx).Warnw("msg", "no_capacity_form_cooldown_arm_failed",
					"ticket_id", tid, "err", err)
			}
		}
	}
	waitSec := int32((delay + time.Second - 1) / time.Second)
	err := u.failMatchOpts(ctx, m,
		func(uint64, *matchv1.MatchTicketStorageRecord) bool { return false }, // 无人有过错,全票退队
		failMatchPushOpts{skipFailedPush: true, requeueWaitSec: waitSec})
	plog.With(ctx).Infow("msg", "match_no_capacity_requeued",
		"match_id", m.GetMatchId(), "tickets", len(m.GetTicketIds()), "retry_after_sec", waitSec)
	return err
}

// failMatch 是失败收尾的公共骨架(onMatchFailed / 成局前在线校验共用):
// 推 FAILED 给全体 → 逐票按 isFaulty 判责(过错删除释放归属 / 无过错退回队列并续 claim
// 补推 QUEUEING) → 移出 active → match 缩短 TTL。
// 若 allocation 已 checkpoint，调用方必须先在 ALLOCATING 阶段取得 signed abort 的
// definitive success，再 CAS FAILED；failMatch 绝不在票据补偿后倒置执行外部回收。
// 守卫:只处理仍归属本 match 的票据(match_id 相等),并发退回/归属他局的票据盲写
// 会把他局在进票据抽回队列(违反不变量 §1),一律跳过;也使本函数可幂等重跑。
func (u *MatchUsecase) failMatch(ctx context.Context, m *matchv1.MatchStorageRecord, isFaulty func(tid uint64, ticket *matchv1.MatchTicketStorageRecord) bool) error {
	return u.failMatchOpts(ctx, m, isFaulty, failMatchPushOpts{})
}

// failMatchPushOpts 控制失败收尾的客户端可见形态(容量耗尽走「排队非失败」语义)。
type failMatchPushOpts struct {
	// skipFailedPush 不推 FAILED:容量耗尽时玩家没有做错任何事、票据会自动重排,
	// 推 FAILED 会让客户端闪一次「匹配失败」UI(§6 第 3 项要修的硬失败体验)。
	skipFailedPush bool
	// requeueWaitSec >0 时,退队补推的 QUEUEING 携带 estimated_wait_seconds,
	// 客户端据此显示「容量排队,约 N 秒后重试」倒计时(§9.23 WAIT 语义,复用既有字段
	// 不加 proto)。
	requeueWaitSec int32
}

func (u *MatchUsecase) failMatchOpts(ctx context.Context, m *matchv1.MatchStorageRecord, isFaulty func(tid uint64, ticket *matchv1.MatchTicketStorageRecord) bool, opts failMatchPushOpts) error {
	// 推 FAILED 给全体(含过错方);容量耗尽路径除外(见 failMatchPushOpts)。
	if !opts.skipFailedPush {
		u.pushProgress(ctx, m.MatchId, stageFailed, m.Members, "", m.MapId)
	}

	var joined error
	for _, tid := range m.TicketIds {
		ticket, found, err := u.repo.GetTicket(ctx, tid)
		if err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		if !found {
			// Ticket deletion may have committed before its response/claim cleanup.
			// Compare-delete only claims still pointing at this exact old ticket.
			for _, mem := range m.Members {
				claimedTicket, claimed, claimErr := u.repo.GetPlayerTicket(ctx, mem.GetPlayerId())
				if claimErr != nil {
					joined = errors.Join(joined, claimErr)
				} else if claimed && claimedTicket == tid {
					joined = errors.Join(joined, u.repo.DeletePlayerIndexIfMatches(ctx, mem.GetPlayerId(), tid))
				}
			}
			continue
		}
		if ticket.MatchId != m.MatchId {
			if ticket.MatchId == 0 {
				for _, playerID := range memberPlayerIDs(ticket.Members) {
					joined = errors.Join(joined, u.repo.RefreshPlayerClaim(ctx, playerID, tid, u.ticketTTL()))
				}
			}
			continue // 已退回队列(0)/已归属他局:绝不盲写
		}
		if isFaulty(tid, ticket) {
			// 过错票据:整队删除并释放归属(不退回队列)
			if deleteErr := u.repo.DeleteTicket(ctx, tid); deleteErr != nil {
				joined = errors.Join(joined, deleteErr)
				continue
			}
			for _, playerID := range memberPlayerIDs(ticket.Members) {
				joined = errors.Join(joined, u.repo.DeletePlayerIndexIfMatches(ctx, playerID, tid))
			}
			continue
		}
		// 其余票据退回队列,保留 enqueued_at_ms(排队时长),清掉 match_id。
		// 守卫退队(封盲写复活竞态):expected=本 match;若并发 CancelMatch 已删该票并释放
		// claim,RequeueTicketIfOwned 会 no-op(不把已取消玩家的票复活进队列)。
		expectedMatchID := ticket.MatchId // == m.MatchId(:1560 守卫已确认归属本局)
		ticket.MatchId = 0
		requeued, err := u.repo.RequeueTicketIfOwned(ctx, ticket, expectedMatchID, u.ticketTTL())
		if err != nil {
			plog.With(ctx).Warnw("msg", "match_requeue_failed", "ticket_id", tid, "err", err)
			joined = errors.Join(joined, err)
			continue
		}
		if !requeued {
			// 票据已被取消删除 / 已归属他局:退出路径已把它处理掉,本局不再补推 QUEUEING。
			continue
		}
		// 退回队列会刷新票据 TTL,claim 必须同步续期(否则 claim 先于票据过期,
		// 玩家可再开新票 → 双票双局,违反不变量 §1)。
		for _, playerID := range memberPlayerIDs(ticket.Members) {
			joined = errors.Join(joined, u.repo.RefreshPlayerClaim(ctx, playerID, tid, u.ticketTTL()))
		}
		// 补推 QUEUEING:客户端刚收到 FAILED,若不告知"你已自动回到队列",其再点匹配
		// 会撞 ErrMatchAlreadyMatching(4002) 卡死在"匹配不了"。句柄仍是 ticket_id。
		// 容量耗尽路径额外携带 estimated_wait_seconds(排队倒计时)。
		if opts.requeueWaitSec > 0 {
			u.pushQueueingWait(ctx, ticket, opts.requeueWaitSec)
		} else {
			u.pushProgress(ctx, ticket.TicketId, stageQueueing, ticket.Members, "", ticket.MapId)
		}
	}
	if joined != nil {
		return joined // active index remains; durable worker retries deterministic cleanup
	}
	if err := u.repo.ExpireMatch(ctx, m.MatchId, u.matchTTL()); err != nil {
		plog.With(ctx).Warnw("msg", "match_expire_failed", "match_id", m.MatchId, "err", err)
		return err
	}
	return nil
}

func failedMatchClassifier(m *matchv1.MatchStorageRecord) func(uint64, *matchv1.MatchTicketStorageRecord) bool {
	confirmOf := make(map[uint64]matchv1.MatchConfirmStatus, len(m.GetMembers()))
	hasRejected := false
	for _, member := range m.GetMembers() {
		confirmOf[member.GetPlayerId()] = member.GetConfirm()
		hasRejected = hasRejected || member.GetConfirm() == confirmRejected
	}
	return func(_ uint64, ticket *matchv1.MatchTicketStorageRecord) bool {
		if hasRejected {
			for _, member := range ticket.GetMembers() {
				if confirmOf[member.GetPlayerId()] == confirmRejected {
					return true
				}
			}
			return false
		}
		return !ticketAllAccepted(ticket, confirmOf)
	}
}

// ticketAllAccepted 判断一张票据的全体成员在 match 里是否都已确认接受。
// 成员不在 confirm 表中按未确认处理(保守判责,行为确定)。
func ticketAllAccepted(ticket *matchv1.MatchTicketStorageRecord, confirmOf map[uint64]matchv1.MatchConfirmStatus) bool {
	for _, m := range ticket.Members {
		if confirmOf[m.PlayerId] != confirmAccepted {
			return false
		}
	}
	return true
}

const allocationRetryMax = 10 * time.Second

func allocationRetryDelay(attempt uint32) time.Duration {
	shift := attempt
	if shift > 3 {
		shift = 3
	}
	d := time.Second * time.Duration(1<<shift)
	if d > allocationRetryMax {
		return allocationRetryMax
	}
	return d
}

// exactAllocationSnapshot fences every ALLOCATING terminal writer to the
// precise durable generation it observed. Stage alone is insufficient: a
// concurrent worker may already have checkpointed a DS or moved the same match
// into ABORTING. Neither a stale liveness result nor an allocator error from an
// older request may erase that newer authority.
func exactAllocationSnapshot(
	rec *matchv1.MatchStorageRecord,
	snapshot *matchv1.MatchStorageRecord,
	phase matchv1.MatchAllocationPhase,
) bool {
	return rec != nil && snapshot != nil &&
		rec.GetMatchId() != 0 && rec.GetMatchId() == snapshot.GetMatchId() &&
		rec.GetStage() == stageAllocating && snapshot.GetStage() == stageAllocating &&
		rec.GetAllocationPhase() == phase && snapshot.GetAllocationPhase() == phase &&
		placement.ValidOperationID(rec.GetAllocationOperationId()) &&
		rec.GetAllocationOperationId() == snapshot.GetAllocationOperationId() &&
		proto.Equal(rec.GetBattleTarget(), snapshot.GetBattleTarget())
}

// cancelableUncheckpointedAllocation 判定一个 ALLOCATING job 是否仍处于「未 checkpoint」阶段,
// 即尚无任何 exact battle target 被固化 —— 此时玩家主动取消不会遗弃一台已交付的 Battle DS。
//
// 与 exactUncheckpointedRequestingAllocation 的区别:后者是 worker 拿着自己的 job 快照做
// 代次比对(证明"还是我那一轮");本函数只对 canonical 记录本身做阶段判定,供没有 job 快照的
// 玩家 RPC 路径(ConfirmMatch 的 reject 分支)使用,判定必须在 UpdateMatchWithLock 闭包内进行。
//
// PENDING:已提交但 worker 尚未取走,必然没有在飞的 AllocateBattle。
// REQUESTING:可能有在飞请求,但 BattleTarget==nil 证明尚未 checkpoint;若 allocator 其实已经
// 拉起 DS,该 DS 由 §9.4 的 15s 心跳超时 abandoned 回收 —— 与既有 liveness 判死路径同构,
// 不新增失败模式。ABORTING/COMPLETED/FAILED 一律不可取消(fence 在途或已终态)。
func cancelableUncheckpointedAllocation(rec *matchv1.MatchStorageRecord) bool {
	if rec.GetStage() != stageAllocating || rec.GetBattleTarget() != nil {
		return false
	}
	switch rec.GetAllocationPhase() {
	case matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_PENDING,
		matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING:
		return true
	default:
		return false
	}
}

func exactUncheckpointedRequestingAllocation(
	rec *matchv1.MatchStorageRecord,
	snapshot *matchv1.MatchStorageRecord,
) bool {
	return exactAllocationSnapshot(rec, snapshot,
		matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING) &&
		rec.GetBattleTarget() == nil && snapshot.GetBattleTarget() == nil
}

// advanceAllocationAbort is the sole legal writer for an ABORTING job. On an
// unknown RPC result it preserves ALLOCATING+ABORTING, all tickets/claims and
// the active index. Only a definitive idempotent allocator ACK permits the CAS
// to FAILED and deterministic termination of every ticket in this invalid run.
func (u *MatchUsecase) advanceAllocationAbort(
	ctx context.Context,
	m *matchv1.MatchStorageRecord,
	cause error,
	scheduleRetry bool,
) error {
	if m == nil || m.GetStage() != stageAllocating ||
		m.GetAllocationPhase() != matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING {
		return cause
	}
	job := cloneMatch(m)
	if scheduleRetry {
		nowMs := time.Now().UnixMilli()
		if job.GetAllocationNextAttemptAtMs() > nowMs {
			return nil
		}
		job = nil
		if err := u.repo.UpdateMatchWithLock(ctx, m.GetMatchId(), u.cfg.OptimisticRetry,
			func(rec *matchv1.MatchStorageRecord) error {
				if !exactAllocationSnapshot(rec, m,
					matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING) {
					return errcode.New(errcode.ErrMatchConcurrent,
						"match %d allocation abort generation changed", rec.GetMatchId())
				}
				rec.AllocationAttempt++
				rec.AllocationNextAttemptAtMs = nowMs + allocationRetryDelay(rec.GetAllocationAttempt()).Milliseconds()
				job = cloneMatch(rec)
				return nil
			}, u.matchTTL()); err != nil {
			if errcode.As(err) == errcode.ErrMatchConcurrent {
				return nil
			}
			return errors.Join(cause, err)
		}
		if job == nil {
			return cause
		}
	}

	allocation, complete := allocationFromStoredTarget(job.GetBattleTarget())
	if !complete || !placement.ValidOperationID(job.GetAllocationOperationId()) {
		return errors.Join(cause, errcode.New(errcode.ErrInvalidState,
			"match %d allocation abort checkpoint is incomplete", job.GetMatchId()))
	}
	if abortErr := u.allocator.AbortBattleAllocation(ctx, job.GetMatchId(),
		job.GetAllocationOperationId(), allocation); abortErr != nil {
		plog.With(ctx).Warnw("msg", "match_allocation_abort_pending",
			"match_id", job.GetMatchId(), "operation_id", job.GetAllocationOperationId(),
			"allocation_id", allocation.Target.AllocationID, "err", abortErr)
		return errors.Join(cause, abortErr)
	}

	var failed *matchv1.MatchStorageRecord
	err := u.repo.UpdateMatchWithLock(ctx, job.GetMatchId(), u.cfg.OptimisticRetry,
		func(rec *matchv1.MatchStorageRecord) error {
			checkpoint, checkpointed := allocationFromStoredTarget(rec.GetBattleTarget())
			if !exactAllocationSnapshot(rec, job,
				matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING) ||
				!checkpointed || !sameBattleAllocation(checkpoint, allocation) {
				return errcode.New(errcode.ErrMatchConcurrent,
					"match %d allocation abort fence changed", rec.GetMatchId())
			}
			rec.Stage = stageFailed
			rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_FAILED
			for _, member := range rec.GetMembers() {
				// A partially allocated run must never auto-requeue. Deleting every
				// ticket+claim forces players back through a fresh StartMatch.
				member.Confirm = confirmRejected
			}
			failed = cloneMatch(rec)
			return nil
		}, u.matchTTL())
	if err != nil {
		if errcode.As(err) == errcode.ErrMatchConcurrent {
			return cause
		}
		return errors.Join(cause, err)
	}
	if failed == nil {
		return cause
	}
	plog.With(ctx).Warnw("msg", "battle_allocation_abort_failed_match",
		"match_id", failed.GetMatchId(), "operation_id", failed.GetAllocationOperationId(), "err", cause)
	return errors.Join(cause, u.failMatch(ctx, failed, failedMatchClassifier(failed)))
}

// advanceAllocation 由服务生命周期 worker 推进 ALLOCATING job。
// AllocateBattle 以 match_id 幂等，placement 以 operation_id 幂等；任一步未知都只延期重试。
func (u *MatchUsecase) advanceAllocation(ctx context.Context, m *matchv1.MatchStorageRecord) error {
	if m == nil || m.Stage != stageAllocating {
		return nil
	}
	if m.GetAllocationPhase() == matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING {
		return u.advanceAllocationAbort(ctx, m, nil, true)
	}
	nowMs := time.Now().UnixMilli()
	if m.AllocationNextAttemptAtMs > nowMs {
		return nil
	}

	var job *matchv1.MatchStorageRecord
	if err := u.repo.UpdateMatchWithLock(ctx, m.MatchId, u.cfg.OptimisticRetry, func(rec *matchv1.MatchStorageRecord) error {
		if rec.Stage != stageAllocating {
			return errcode.New(errcode.ErrInvalidState, "match %d no longer allocating", rec.MatchId)
		}
		if rec.GetAllocationPhase() == matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_ABORTING {
			return errcode.New(errcode.ErrMatchConcurrent,
				"match %d allocation abort already fenced", rec.GetMatchId())
		}
		if rec.AllocationNextAttemptAtMs > nowMs {
			return errcode.New(errcode.ErrMatchConcurrent, "match %d allocation not due", rec.MatchId)
		}
		if rec.AllocationOperationId == "" {
			rec.AllocationOperationId = allocationOperationID()
		}
		rec.AllocationAttempt++
		rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING
		rec.AllocationNextAttemptAtMs = nowMs + allocationRetryDelay(rec.AllocationAttempt).Milliseconds()
		job = cloneMatch(rec)
		return nil
	}, u.matchTTL()); err != nil {
		if errcode.As(err) == errcode.ErrInvalidState || errcode.As(err) == errcode.ErrMatchConcurrent {
			return nil
		}
		return err
	}
	if job == nil {
		return nil
	}

	playerIDs := memberPlayerIDs(job.Members)
	// A legacy/in-flight creator may have written ALLOCATING before all ticket
	// reservations or player claims became durable. Never create an external DS
	// until the exact canonical discovery graph is complete. UNKNOWN remains the
	// same retryable ALLOCATING operation; it is never interpreted as absence.
	if err := u.ensureMatchDiscovery(ctx, job); err != nil {
		return err
	}

	// 成局最终门:分配 DS 前批量校验全员在线(locator 在线保活:Hub DS 心跳捎带续期,
	// 掉线/崩溃 → 断报 ≥30s → locator key 过期 = 离线)。掉线玩家所在票据判责删除,
	// 其余退回队列,避免给残局白白拉起 Battle DS(ds_allocator 15s 心跳超时虽能兜底回收,
	// 但白耗一次分配 + 其余 9 人被拉进残局)。
	// 开关:LivenessGateEnabled 默认关闭(Hub DS player_ids 心跳未联发前会误判全员离线);
	// 弱依赖:开关关闭 / locator 未配(nil)/ 查询失败 → 跳过校验继续成局,不误杀正常对局。
	//
	// ❗ INC-20260724-001:本门的证据来源对「已成局 match 的成员」这一人群**结构性失效**,
	// 全部实跑配置已回退为 false,重开前必须先修证据链(见 etc/matchmaker-dev.yaml 同项注释)。
	// 机械原因:走到这里的成员在 locator 里是 MATCHING(=4) 态(notifyMatching 在成局那一刻
	// 写一次、TTL 30s),而 player_locator 的 RefreshHubLocations 只对 state==3(HUB) 续期,
	// MATCHING 既无续期写者也无释放通道 ⇒ 成局后 30s 内查不到任何缺席(零真阳性),
	// 超过 30s 后必然全员缺席(全假阳性)。本事故实测 solo_match_found → 判死 = 31.07s
	// (30s TTL + 一个 2s 撮合 tick),与玩家是否真的在线无关。
	// 因此 presence key-miss 不得用于终止已成局 match(§9.22:key miss 只说明 presence
	// 不可见,不能单独证明玩家已离开,更不得冒充 OFFLINE)。
	// 另注:本门曾是 stageAllocating 唯一会终止的路径;关闭后玩家出口由 ConfirmMatch 的
	// pre-checkpoint 取消分支承担(见该处 INC-20260724-001 注释),二者必须同批存在。
	if job.GetBattleTarget() == nil {
		if offline := u.findOfflineMembers(ctx, playerIDs); len(offline) > 0 {
			plog.With(ctx).Warnw("msg", "match_liveness_failed",
				"match_id", job.MatchId, "offline_players", offline)
			// 先把 match 记录 CAS 翻成 FAILED。守卫绑定 exact REQUESTING/op/nil-target
			// 快照；并发 checkpoint/ABORTING/新 generation 均由对方继续推进，这里不收尾。
			var failed *matchv1.MatchStorageRecord
			werr := u.repo.UpdateMatchWithLock(ctx, job.MatchId, u.cfg.OptimisticRetry, func(rec *matchv1.MatchStorageRecord) error {
				if !exactUncheckpointedRequestingAllocation(rec, job) {
					return errcode.New(errcode.ErrMatchConcurrent,
						"match %d allocation generation changed before liveness fail", job.GetMatchId())
				}
				rec.Stage = stageFailed
				rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_FAILED
				for _, member := range rec.Members {
					for _, offlinePlayerID := range offline {
						if member.GetPlayerId() == offlinePlayerID {
							member.Confirm = confirmRejected
						}
					}
				}
				failed = cloneMatch(rec)
				return nil
			}, u.matchTTL())
			if werr != nil {
				return werr
			}
			return u.failMatch(ctx, failed, failedMatchClassifier(failed))
		}
	}

	// 两级撮合放置(scale-cellular-20m.md §4.4):算出"参战玩家多数所在 region/cell",
	// 让 battle DS 就近落到该 Cell。当前先作为放置提示落日志(多 region RTT 排障 / 观测);
	// 把它透传进 AllocateBattleRequest(region_id/cell_id)由 ds_allocator 按 Cell 选 k8s,
	// 属 proto + 跨服务改动,留 Codex/人按 §11.1 跟进(见 PROGRESS 落地记录)。
	// router 为 nil(单 Cell / dev)时 ok=false,不打印、行为不变。
	if place, ok := u.battlePlacement(playerIDs); ok {
		plog.With(ctx).Debugw("msg", "battle_placement",
			"match_id", job.MatchId, "region_id", place.RegionID, "cell_id", place.CellID,
			"players", len(playerIDs))
	}

	// AllocateBattle may have succeeded just before this process died.  Once an
	// exact target is checkpointed on the canonical match, every later attempt
	// must reuse it; calling the allocator again and accepting a different target
	// would strand players against an earlier partially-published DS.
	allocation, checkpointed := allocationFromStoredTarget(job.GetBattleTarget())
	if job.GetBattleTarget() != nil && !checkpointed {
		return errcode.New(errcode.ErrInvalidState,
			"match %d has incomplete durable battle target", job.MatchId)
	}
	if !checkpointed {
		var err error
		combatFactionByPlayer, factionErr := combatFactionsFromMembers(job.GetMembers())
		if factionErr != nil {
			return errcode.New(errcode.ErrInvalidState,
				"match %d combat factions invalid: %v", job.GetMatchId(), factionErr)
		}
		factionAllocator, ok := u.allocator.(CombatFactionDSAllocator)
		if !ok {
			// 曾经这里会降级调用不带阵营的 AllocateBattle 并只打一条 Warn。那条路会产出
			// 「有名单、无阵营」的分配，DS 侧只能退化成每人一个独立阵营 —— 队友互相能打，
			// 而且对局照常进行、照常结算，错误完全不可见。阵营是对局定义的一部分，
			// 送不出去就不该开局：这里改为定性失败，让问题在分配阶段就暴露。
			plog.With(ctx).Errorw("msg", "combat_faction_allocator_unsupported",
				"match_id", job.GetMatchId(), "players", len(playerIDs))
			return errcode.New(errcode.ErrInvalidState,
				"match %d allocator cannot carry combat factions", job.GetMatchId())
		}
		allocation, err = factionAllocator.AllocateBattleWithCombatFactions(
			ctx, job.MatchId, playerIDs, combatFactionByPlayer, job.MapId)
		if err != nil {
			plog.With(ctx).Errorw("msg", "ds_allocate_failed", "match_id", job.MatchId, "err", err)
			code := errcode.As(err)
			if code != errcode.ErrDSAllocationFailed && code != errcode.ErrDSNoAvailable {
				// transport/Redis/allocation_uncertain 都是未知结果，只能保持 ALLOCATING。
				return err
			}
			// 只有 allocator 明确证明未产生可用 DS 时，才先 CAS FAILED，再执行退票补偿。
			var failed *matchv1.MatchStorageRecord
			werr := u.repo.UpdateMatchWithLock(ctx, job.MatchId, u.cfg.OptimisticRetry, func(rec *matchv1.MatchStorageRecord) error {
				if !exactUncheckpointedRequestingAllocation(rec, job) {
					return errcode.New(errcode.ErrMatchConcurrent,
						"match %d allocation generation changed before definitive allocator failure", job.GetMatchId())
				}
				rec.Stage = stageFailed
				rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_FAILED
				failed = cloneMatch(rec)
				return nil
			}, u.matchTTL())
			if werr != nil {
				return errors.Join(err, werr)
			}
			// 容量耗尽对玩家不是失败:静默窗 + QUEUEING(带倒计时)退队,不推 FAILED
			// (anti-abuse §6 第 3 项)。返回值仍带 err 保留调用方的失败观测。
			return errors.Join(err, u.onMatchNoCapacity(ctx, failed))
		}
		if allocation == nil || allocation.Address == "" || !allocation.Target.CompleteBattle() {
			return errcode.New(errcode.ErrDSAllocationFailed, "allocator returned incomplete battle target for match %d", job.MatchId)
		}
		allocation, err = u.checkpointBattleAllocation(ctx, job, allocation)
		if err != nil {
			return err
		}
	}

	// Linearize permission for each post-checkpoint external step against the
	// allocator-abort fence. A worker holding an old local clone may continue
	// only while the canonical generation is exact REQUESTING/op/target.
	if _, err := u.fenceRequestingAllocationCheckpoint(ctx, job.GetMatchId(),
		job.GetAllocationOperationId(), allocation); err != nil {
		return err
	}
	tickets, err := u.allocator.SignBattleTickets(ctx, job.MatchId, playerIDs, allocation)
	if err != nil {
		return err
	}
	if err := validateSignedBattleTickets(playerIDs, tickets); err != nil {
		return err
	}
	dsAddr := allocation.Address

	// P0 修复(2026-07-15,codex P0-4):BATTLE 投影必须先于 READY 提交写入(强依赖)。
	// 否则 READY 推送已发、玩家已向 battle 迁移,而 locator 无 BATTLE 租约——这个窗口内
	// 断线重登会被误路由回 Hub(双在场)。失败 → 返回错误,allocation 已 checkpoint,
	// 推进循环重试幂等(同 match 重写 BATTLE 过 guardTransition)。
	// 即使后续 READY CAS 失败,残留投影也只活 ≤30s(TTL),且三态门 fail-closed 可自愈。
	if err := u.notifyBattleStrict(ctx, playerIDs, job.MatchId, dsAddr); err != nil {
		return err
	}

	// 写 match → READY。stage 守卫:仅 ALLOCATING 可推进到 READY——若本 match 在分配期间
	// 已被 expireOnce 判 FAILED(票据已退回队列),盲写会把 FAILED 翻成 READY,
	// 造成"票在队列里但人被拉进战斗"的脏状态。已分配的 DS 由 battle 心跳超时补偿回收(不变量 §4)。
	var ready *matchv1.MatchStorageRecord
	werr := u.repo.UpdateMatchWithLock(ctx, job.MatchId, u.cfg.OptimisticRetry, func(rec *matchv1.MatchStorageRecord) error {
		expected := &matchv1.MatchStorageRecord{
			MatchId: job.GetMatchId(), Stage: stageAllocating,
			AllocationPhase:       matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING,
			AllocationOperationId: job.GetAllocationOperationId(),
			BattleTarget:          battleTargetStorage(allocation),
		}
		if !exactAllocationSnapshot(rec, expected,
			matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_REQUESTING) {
			return errcode.New(errcode.ErrMatchConcurrent,
				"match %d allocation checkpoint changed before READY", job.MatchId)
		}
		rec.Stage = stageReady
		rec.BattleDsAddr = dsAddr
		rec.BattleTarget = battleTargetStorage(allocation)
		rec.AllocationPhase = matchv1.MatchAllocationPhase_MATCH_ALLOCATION_PHASE_COMPLETED
		rec.AllocationNextAttemptAtMs = 0
		ready = cloneMatch(rec)
		return nil
	}, u.matchTTL())
	if werr != nil {
		return werr
	}

	// 投影已在 READY 前强写入(notifyBattleStrict);这里再刷一次纯属弱依赖续期,失败仅 Warn。
	u.notifyBattle(ctx, playerIDs, job.MatchId, dsAddr)

	// 每个玩家单独带自己的 battle_ticket 推 READY 进度。交付是 at-least-once:
	// 全员推送成功才把 match 移出 active ZSET(不变量:READY ∈ active ⟺ 推送交付未确认)。
	// 失败(Kafka 不可用)或本进程在推送前崩溃时,match 滞留 active,由撮合循环
	// stageReady 分支(finalizeReadyMatch)幂等补推——重签新 jti,客户端契约要求容忍
	// 重复回调(CLAUDE.md §9.19)——直到交付或 match TTL 到期。非队长成员没有 match_id,
	// 这条推送是他们得知 READY / Battle 落点的唯一服务端主动通道,不允许静默丢弃。
	if perr := u.pushReadyStrict(ctx, ready, dsAddr, tickets); perr != nil {
		plog.With(ctx).Warnw("msg", "match_ready_push_deferred", "match_id", job.MatchId, "err", perr)
	} else {
		// 确认期结束:移出 active。票据保留到 TTL, 让客户端用 StartMatch 返回的 ticket_id
		// 继续轮询时也能解析到 READY match, 避免错过 push 后 GetMatchProgress 变成 4001。
		u.removeActive(ctx, job.MatchId)
	}
	plog.With(ctx).Infow("msg", "match_ready", "match_id", job.MatchId, "ds_addr", dsAddr, "players", len(playerIDs))
	return nil
}

func battleTargetStorage(allocation *model.BattleAllocation) *matchv1.MatchBattleTargetStorageRecord {
	if allocation == nil {
		return nil
	}
	return &matchv1.MatchBattleTargetStorageRecord{
		DsAddr: allocation.Address, DsPodName: allocation.Target.PodName,
		DsInstanceUid: allocation.Target.InstanceUID, DsInstanceEpoch: allocation.Target.InstanceEpoch,
		AllocationId: allocation.Target.AllocationID, ReleaseTrack: allocation.Target.ReleaseTrack,
	}
}

func allocationFromMatch(m *matchv1.MatchStorageRecord) (*model.BattleAllocation, bool) {
	target := m.GetBattleTarget()
	if target == nil {
		return nil, false
	}
	allocation := &model.BattleAllocation{Address: target.GetDsAddr(), Target: placement.Target{
		PodName: target.GetDsPodName(), InstanceUID: target.GetDsInstanceUid(),
		InstanceEpoch: target.GetDsInstanceEpoch(), AllocationID: target.GetAllocationId(),
		ReleaseTrack: target.GetReleaseTrack(),
	}}
	return allocation, allocation.Target.CompleteBattle()
}

// ResolvePlayerMatchContext reads only the canonical start-operation / player-claim /
// ticket / match graph. Queue and active ZSETs are deliberately excluded because they
// are derived, game-mode-local indexes; this makes a PVP instance able to resolve a
// PVE match (and vice versa) while both modes share the canonical Redis records.
//
// Any broken edge is UNKNOWN, never NONE. This method is read-only: recovery reads
// cannot advance, compensate, delete, or infer a business terminal from Redis TTL.
func (u *MatchUsecase) ResolvePlayerMatchContext(ctx context.Context, playerID uint64) (*matchv1.ResolvePlayerMatchContextResponse, error) {
	unknown := func() *matchv1.ResolvePlayerMatchContextResponse {
		return &matchv1.ResolvePlayerMatchContextResponse{
			State: matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_UNSPECIFIED,
			Stage: matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_UNSPECIFIED,
		}
	}
	if playerID == 0 {
		return unknown(), errcode.New(errcode.ErrInvalidArg, "player_id required")
	}

	startTicketID, startFound, err := u.repo.GetStartPlayerOperation(ctx, playerID)
	if err != nil {
		return unknown(), errcode.NewCause(errcode.ErrUnavailable, err, "read match start player index")
	}
	claimTicketID, claimFound, err := u.repo.GetPlayerTicket(ctx, playerID)
	if err != nil {
		return unknown(), errcode.NewCause(errcode.ErrUnavailable, err, "read match player claim")
	}
	if startFound {
		op, found, readErr := u.repo.GetStartOperation(ctx, startTicketID)
		if readErr != nil {
			return unknown(), errcode.NewCause(errcode.ErrUnavailable, readErr, "read match start operation")
		}
		if !found || op.GetTicketId() != startTicketID || memberIndex(op.GetMembers(), playerID) < 0 ||
			startOperationTerminal(op.GetPhase()) || (claimFound && claimTicketID != startTicketID) {
			return unknown(), nil
		}
		// Cancellation has committed but cleanup is still replayable. Do not report
		// STARTING (which would resurrect the spinner after Cancel succeeded), and do
		// not report NONE until compare-delete cleanup has actually completed.
		if op.GetPhase() == matchv1.MatchStartPhase_MATCH_START_PHASE_COMPENSATING {
			return unknown(), nil
		}
		return &matchv1.ResolvePlayerMatchContextResponse{
			State:    matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE,
			Stage:    matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_STARTING,
			TicketId: startTicketID,
			GameMode: op.GetGameMode(),
			MapId:    op.GetMapId(),
		}, nil
	}
	if !claimFound {
		return &matchv1.ResolvePlayerMatchContextResponse{
			State: matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_NONE,
			Stage: matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_UNSPECIFIED,
		}, nil
	}

	ticket, found, err := u.repo.GetTicket(ctx, claimTicketID)
	if err != nil {
		return unknown(), errcode.NewCause(errcode.ErrUnavailable, err, "read canonical match ticket")
	}
	if !found || ticket.GetTicketId() != claimTicketID || memberIndex(ticket.GetMembers(), playerID) < 0 {
		return unknown(), nil
	}
	base := &matchv1.ResolvePlayerMatchContextResponse{
		State:    matchv1.PlayerMatchContextState_PLAYER_MATCH_CONTEXT_STATE_ACTIVE,
		TicketId: claimTicketID,
		GameMode: ticket.GetGameMode(),
		MapId:    ticket.GetMapId(),
	}
	if ticket.GetMatchId() == 0 {
		base.Stage = matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_QUEUED
		return base, nil
	}

	m, found, err := u.repo.GetMatch(ctx, ticket.GetMatchId())
	if err != nil {
		return unknown(), errcode.NewCause(errcode.ErrUnavailable, err, "read canonical match")
	}
	if !found || m.GetMatchId() != ticket.GetMatchId() || memberIndex(m.GetMembers(), playerID) < 0 ||
		!containsUint64(m.GetTicketIds(), claimTicketID) {
		return unknown(), nil
	}
	if ticket.GetGameMode() != "" && m.GetGameMode() != "" && ticket.GetGameMode() != m.GetGameMode() {
		return unknown(), nil
	}
	if m.GetGameMode() != "" {
		base.GameMode = m.GetGameMode()
	}
	if m.GetMapId() != 0 {
		// match 记录继承自票据;两者都有时以 match 为准(0=未指定,保留票据值)。
		base.MapId = m.GetMapId()
	}
	base.MatchId = m.GetMatchId()
	switch m.GetStage() {
	case stageFound, stageConfirm:
		base.Stage = matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_CONFIRMING
	case stageAllocating:
		base.Stage = matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_ALLOCATING
	case stageReady:
		allocation, ok := allocationFromMatch(m)
		if !ok || allocation.Address == "" || allocation.Address != m.GetBattleDsAddr() || u.allocator == nil {
			return unknown(), nil
		}
		// 冷启动/换设备恢复不能回退到 login 的 roster projection 重新拼票。
		// READY match 中持久化的 exact target 才是唯一可重签输入；签名失败时
		// 整条路由保持 UNKNOWN，绝不返回半票。
		battleTicket, signErr := u.allocator.SignBattleTicket(ctx, playerID, m.GetMatchId(), allocation)
		if signErr != nil || battleTicket == "" {
			return unknown(), errcode.NewCause(errcode.ErrUnavailable, signErr,
				"re-sign canonical READY battle ticket")
		}
		base.Stage = matchv1.PlayerMatchResumeStage_PLAYER_MATCH_RESUME_STAGE_READY
		base.BattleDsAddr = allocation.Address
		base.BattleTicket = battleTicket
	default:
		return unknown(), nil
	}
	return base, nil
}

func containsUint64(values []uint64, want uint64) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// findOfflineMembers 成局前在线校验(弱依赖):开关 LivenessGateEnabled 关闭(默认,
// Hub DS player_ids 心跳未联发前开启会误判全员离线)/ locator 未配(nil)/ 查询失败
// → 返 nil(跳过校验,宁可多拉一局不误杀);查到才返实际离线名单。
func (u *MatchUsecase) findOfflineMembers(ctx context.Context, playerIDs []uint64) []uint64 {
	if !u.cfg.LivenessGateEnabled || u.locator == nil {
		return nil
	}
	offline, err := u.locator.FindOfflinePlayers(ctx, playerIDs)
	if err != nil {
		plog.With(ctx).Warnw("msg", "match_liveness_check_skipped", "err", err)
		return nil
	}
	return offline
}

// ── RPC 4:GetMatchProgress ───────────────────────────────────────────────────

// GetMatchProgress 查询进度。
//   - id 是客户端句柄:match_id(已撮合)或 ticket_id(排队中)。重新登录 / 换设备丢了句柄时
//     传 0,服务端用 callerID 反查其当前所在票据(GetPlayerTicket),解决"重连拿不到自己进度"。
//   - 鉴权(不变量 §14 / 反外挂):callerID 必须是该 match/ticket 的成员才返回进度;否则按
//     "不存在"处理(ErrMatchNotFound),不暴露他人对局的存在性,杜绝外挂用任意 match_id 拉别人
//     的双方名单 / DS 地址。match_id 不是秘密,绝不能再当授权凭证。
//   - READY 阶段且 caller 是本局成员时,给他现签一张新 battle DSTicket(新 jti)下发,支持
//     换手机 / 掉线重连(见 refreshBattleTicket)。
func (u *MatchUsecase) GetMatchProgress(ctx context.Context, callerID, id uint64) (*matchv1.MatchProgress, error) {
	if callerID == 0 {
		return nil, errcode.New(errcode.ErrUnauthorized, "missing caller identity")
	}

	// 重连兜底:句柄丢失(id==0)时先反查 canonical ticket；StartMatch 已受理但
	// worker 尚未完成 ticket handoff 时，再查 durable start-operation 派生索引。
	if id == 0 {
		tid, found, err := u.repo.GetPlayerTicket(ctx, callerID)
		if err != nil {
			return nil, err
		}
		if !found {
			tid, found, err = u.repo.GetStartPlayerOperation(ctx, callerID)
			if err != nil {
				return nil, err
			}
		}
		if !found {
			return nil, errcode.New(errcode.ErrMatchNotFound, "player %d not in any queue", callerID)
		}
		id = tid
	}

	// ⚠️ id 是**跨两个 ID 空间**的句柄:排队中是 ticket_id,已撮合是 match_id,而这两个空间
	// 由同一 nodeID 的两个发号器铸造(main.go 的 MustProvideSnowflakeN(…,2)),按
	// infra.md §8.2① 的契约,同一秒里各自的第 K 个号**逐位相同是常态**——即某玩家的
	// ticket_id 可以恰好等于另一局无关的 match_id。
	// 因此这里探两个空间时,"命中了但 caller 不是成员"只能说明**这一侧撞的是别人的实体**,
	// 绝不能就此判 4001 短路:必须继续探另一侧,否则排队玩家会被无关对局遮蔽,拿到误报的
	// ErrMatchNotFound,再被客户端错误降级成 Hub 路由(与下方 start-operation 窗口同类事故)。
	// 对外可见行为不变:两侧都不属于 caller 时仍在函数末尾统一返回 4001,不泄露他人对局存在性。
	readCanonical := func() (*matchv1.MatchProgress, bool, error) {
		m, matchFound, err := u.repo.GetMatch(ctx, id)
		if err != nil {
			return nil, false, err
		}
		if matchFound && memberIndex(m.Members, callerID) >= 0 {
			if err := u.requireLocalGameMode(m.GetGameMode()); err != nil {
				return nil, false, err
			}
			prog := matchToProgress(m)
			if rerr := u.refreshBattleTicket(ctx, m, callerID, prog); rerr != nil {
				return nil, false, rerr
			}
			return prog, true, nil
		}
		if t, found, err := u.repo.GetTicket(ctx, id); err != nil {
			return nil, false, err
		} else if found {
			if memberIndex(t.Members, callerID) < 0 {
				// 同上:这一侧撞的是别人的票据,交给调用链继续走 start-operation / 末尾 4001。
				return nil, false, nil
			}
			if err := u.requireLocalGameMode(t.GetGameMode()); err != nil {
				return nil, false, err
			}
			if t.MatchId != 0 {
				// 注意:这里的 t.MatchId 是票据里**存下来的真实交叉引用**,不是客户端句柄,
				// 不存在上面那种跨空间混叠。所以"caller 在票据里却不在它指向的 match 里"
				// 是真正的数据不一致,必须 fail-closed 报错,**不能**照抄上面的继续探测写法。
				if m, found, err := u.repo.GetMatch(ctx, t.MatchId); err != nil {
					return nil, false, err
				} else if found {
					if memberIndex(m.Members, callerID) < 0 {
						return nil, false, errcode.New(errcode.ErrMatchNotFound, "match/ticket %d not found", id)
					}
					if err := u.requireLocalGameMode(m.GetGameMode()); err != nil {
						return nil, false, err
					}
					// 票据已撮合进 match,caller 既是票据成员即本局成员,直接给 match 进度。
					prog := matchToProgress(m)
					if rerr := u.refreshBattleTicket(ctx, m, callerID, prog); rerr != nil {
						return nil, false, rerr
					}
					return prog, true, nil
				}
			}
			return ticketToProgress(t), true, nil
		}
		return nil, false, nil
	}

	if prog, found, err := readCanonical(); err != nil || found {
		return prog, err
	}

	// StartMatch 的线性化点是 durable start operation；ticket body 由后台
	// worker 稍后创建。RPC 已返回 ACCEPTED 后立即查询时，不能把这个正常窗口
	// 误报成 4001，否则客户端会把仍在启动的匹配错误降级成 Hub 路由。
	op, startFound, err := u.repo.GetStartOperation(ctx, id)
	if err != nil {
		return nil, err
	}
	if startFound {
		if op.GetTicketId() != id || len(op.GetMembers()) == 0 {
			return nil, errcode.New(errcode.ErrUnavailable, "match start operation %d graph is invalid", id)
		}
		if memberIndex(op.GetMembers(), callerID) < 0 {
			return nil, errcode.New(errcode.ErrMatchNotFound, "match/ticket %d not found", id)
		}
		if err := u.requireLocalGameMode(op.GetGameMode()); err != nil {
			return nil, err
		}
		switch op.GetPhase() {
		case matchv1.MatchStartPhase_MATCH_START_PHASE_ACCEPTED,
			matchv1.MatchStartPhase_MATCH_START_PHASE_TICKET_READY,
			matchv1.MatchStartPhase_MATCH_START_PHASE_CLAIMING,
			matchv1.MatchStartPhase_MATCH_START_PHASE_CLAIMS_READY,
			matchv1.MatchStartPhase_MATCH_START_PHASE_QUEUED:
			return ticketToProgress(ticketFromStartOperation(op)), nil
		case matchv1.MatchStartPhase_MATCH_START_PHASE_COMPENSATING,
			matchv1.MatchStartPhase_MATCH_START_PHASE_FAILED:
			return &matchv1.MatchProgress{MatchId: id, Stage: stageFailed}, nil
		default:
			return nil, errcode.New(errcode.ErrUnavailable,
				"match start operation %d has invalid phase %s", id, op.GetPhase())
		}
	}

	// Worker 按“先写 ticket，后删 start operation”交接；若上面的首次 canonical
	// 读取早于 ticket 写入，而 start-op 读取晚于删除，第二次读取必能看到 ticket
	// 或已经形成的 match，避免在两个权威记录之间制造瞬时 NOT_FOUND。
	if prog, found, err := readCanonical(); err != nil || found {
		return prog, err
	}
	return nil, errcode.New(errcode.ErrMatchNotFound, "match/ticket %d not found", id)
}

// refreshBattleTicket 在 READY 阶段为发起查询的本人现签一张新的 battle DSTicket(新 jti)，
// 覆盖 prog 里来自存储的票字段。这样换手机 / 掉线重连每次都拿新 jti，不会撞 DS 侧 jti 一次性
// 防重放；票 sub 锁定调用者本人。
// 守卫：callerID!=0 且 stage=READY 且有 ds_addr 且 caller 是本局成员才签。
//
// R7 收口(P1):签发链在场但 SignBattleTicket 失败时不再降级保留存储旧票——存量票
// 绑定的是 claim 时刻的 sjti,顶号换机后必被 DS 兑换点拒绝(或撞 jti 一次性防重放),
// 把它交出去只会让客户端拿着废票撞墙。改为整个查询 fail-closed(可重试 Unavailable),
// 客户端按既有错误路径退避重查,签发恢复即拿到新票。
// allocationFromMatch 不完整(legacy/dev 记录无持久化 allocation)保留旧行为:告警 +
// 沿用存储票字段,不阻断 dev 联调。
func (u *MatchUsecase) refreshBattleTicket(ctx context.Context, m *matchv1.MatchStorageRecord, callerID uint64, prog *matchv1.MatchProgress) error {
	if callerID == 0 || m.Stage != stageReady || m.BattleDsAddr == "" {
		return nil
	}
	if memberIndex(m.Members, callerID) < 0 {
		return nil // 非本局成员，不签票
	}
	allocation, ok := allocationFromMatch(m)
	if !ok {
		plog.With(ctx).Warnw("msg", "resign_battle_ticket_missing_persisted_target", "match_id", m.MatchId, "player_id", callerID)
		return nil
	}
	token, err := u.allocator.SignBattleTicket(ctx, callerID, m.MatchId, allocation)
	if err != nil {
		plog.With(ctx).Warnw("msg", "resign_battle_ticket_failed", "match_id", m.MatchId, "player_id", callerID, "err", err)
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"battle ticket resign unavailable for match %d; retry", m.GetMatchId())
	}
	prog.BattleTicket = token
	return nil
}

// ── 后台撮合循环 ──────────────────────────────────────────────────────────────

// RunMatchLoop 启动后台撮合 + 确认期超时扫描,直到 ctx 取消。
func (u *MatchUsecase) RunMatchLoop(ctx context.Context) {
	ticker := time.NewTicker(u.cfg.MatchInterval.Std())
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "match_loop_started", "interval", u.cfg.MatchInterval.String())
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "match_loop_stopped")
			return
		case <-ticker.C:
			u.matchTickOnce(ctx)
		}
	}
}

// matchTickOnce 单个撮合 tick(独立函数使 recover 作用域恰为一轮)。
//
// panic 兜底(压测审核【必修-6】):撮合循环此前无 recover,任一 latent panic 直接崩进程
// = 本分片撮合彻底停摆、玩家匹配卡死(违反 §9.19)。recover 后跳过本轮,下个 tick 继续;
// 所有状态推进都经 Redis CAS 幂等,重入安全。并发 map 写是 runtime fatal,recover 兜不住。
func (u *MatchUsecase) matchTickOnce(ctx context.Context) {
	defer safego.Recover(ctx, "matchmaker_match_loop")
	if err := u.reconcileStartOperationsOnce(ctx); err != nil {
		plog.With(ctx).Warnw("msg", "match_start_reconcile_failed", "err", err)
	}
	if err := u.advanceStartOperationsOnce(ctx); err != nil {
		plog.With(ctx).Warnw("msg", "match_start_batch_failed", "err", err)
	}
	if err := u.matchOnce(ctx); err != nil {
		plog.With(ctx).Warnw("msg", "match_once_failed", "err", err)
	}
	if err := u.reconcileActiveOnce(ctx); err != nil {
		plog.With(ctx).Warnw("msg", "match_active_reconcile_failed", "err", err)
	}
	if err := u.advanceAllocationsOnce(ctx); err != nil {
		plog.With(ctx).Warnw("msg", "match_allocation_batch_failed", "err", err)
	}
	if err := u.expireOnce(ctx); err != nil {
		plog.With(ctx).Warnw("msg", "expire_once_failed", "err", err)
	}
	// 队列在线扫除(节流 livenessSweepInterval):掉线玩家的死票主动清,
	// 不等它被凑进一局再被成局门拦下(白害无辜玩家陪跑一轮 FAILED)。
	if time.Since(u.lastLivenessSweep) >= livenessSweepInterval {
		u.lastLivenessSweep = time.Now()
		if err := u.livenessSweepOnce(ctx); err != nil {
			plog.With(ctx).Warnw("msg", "liveness_sweep_failed", "err", err)
		}
		// 排队票离线回收(INC-20260814-001):与上一道共用节流,证据链却不同——
		// 按「离开多久」判(absentBeyond),liveness_gate 关闭时也生效。
		if err := u.queueAbsenceSweepOnce(ctx); err != nil {
			plog.With(ctx).Warnw("msg", "queue_absence_sweep_failed", "err", err)
		}
	}
}

// reconcileStartOperationsOnce 遍历 Redis Cluster 全 master 的 canonical start operation，
// 修复 due 索引。完整遍历按 5s 节流，避免每个撮合 tick 扫全库。
func (u *MatchUsecase) reconcileStartOperationsOnce(ctx context.Context) error {
	if !u.lastStartReconcile.IsZero() && time.Since(u.lastStartReconcile) < canonicalReconcileEvery {
		return nil
	}
	u.lastStartReconcile = time.Now()
	ids, err := u.repo.ScanStartOperationIDs(ctx, 128)
	if err != nil {
		return err
	}
	var joined error
	for _, ticketID := range ids {
		op, found, gerr := u.repo.GetStartOperation(ctx, ticketID)
		if gerr != nil {
			joined = errors.Join(joined, gerr)
			continue
		}
		if !found {
			continue
		}
		if op.GetGameMode() != "" && op.GetGameMode() != u.cfg.GameMode {
			// Global canonical scan sees every mode; only the owning mode may
			// rebuild its derived due index or claim players.
			continue
		}
		if startOperationTerminal(op.GetPhase()) {
			if rerr := u.repo.RemoveStartActive(ctx, ticketID); rerr != nil {
				joined = errors.Join(joined, rerr)
			}
			continue
		}
		for _, member := range op.GetMembers() {
			existing, claimed, claimErr := u.repo.ClaimStartPlayer(ctx, member.GetPlayerId(), ticketID, u.ticketTTL())
			if claimErr != nil {
				joined = errors.Join(joined, claimErr)
				continue
			}
			if !claimed && existing != ticketID {
				// Keep the operation due. advanceStartOperation will persist
				// COMPENSATING and compare-delete only claims owned by this op.
				joined = errors.Join(joined, errcode.New(errcode.ErrMatchAlreadyMatching,
					"start operation %d player %d owned by %d", ticketID, member.GetPlayerId(), existing))
			}
		}
		dueMs := op.GetNextAttemptAtMs()
		if op.GetLeaseDeadlineMs() > dueMs {
			dueMs = op.GetLeaseDeadlineMs()
		}
		if aerr := u.repo.EnsureStartActive(ctx, ticketID, dueMs); aerr != nil {
			joined = errors.Join(joined, aerr)
		}
	}
	return joined
}

func (u *MatchUsecase) advanceStartOperationsOnce(ctx context.Context) error {
	ids, err := u.repo.RangeDueStartOperations(ctx, time.Now().UnixMilli())
	if err != nil {
		return err
	}
	var joined error
	for _, ticketID := range ids {
		op, found, gerr := u.repo.GetStartOperation(ctx, ticketID)
		if gerr != nil {
			// canonical 状态未知时保留 due 索引。
			joined = errors.Join(joined, gerr)
			continue
		}
		if !found {
			if rerr := u.repo.RemoveStartActive(ctx, ticketID); rerr != nil {
				joined = errors.Join(joined, rerr)
			}
			continue
		}
		if op.GetGameMode() != "" && op.GetGameMode() != u.cfg.GameMode {
			if rerr := u.repo.RemoveStartActive(ctx, ticketID); rerr != nil {
				joined = errors.Join(joined, rerr)
			}
			continue
		}
		if startOperationTerminal(op.GetPhase()) {
			if rerr := u.repo.RemoveStartActive(ctx, ticketID); rerr != nil {
				joined = errors.Join(joined, rerr)
			}
			continue
		}
		if aerr := u.advanceStartOperation(ctx, op); aerr != nil {
			joined = errors.Join(joined, aerr)
		}
	}
	return joined
}

// reconcileActiveOnce 从 canonical match record 修复派生 active ZSET。
// Redis Cluster 必须遍历全部 master；UniversalClient.Scan 只扫单节点会永久漏局。
func (u *MatchUsecase) reconcileActiveOnce(ctx context.Context) error {
	if !u.lastMatchReconcile.IsZero() && time.Since(u.lastMatchReconcile) < canonicalReconcileEvery {
		return nil
	}
	u.lastMatchReconcile = time.Now()
	ids, err := u.repo.ScanMatchIDs(ctx, 128)
	if err != nil {
		return err
	}
	var joined error
	for _, mid := range ids {
		m, found, gerr := u.repo.GetMatch(ctx, mid)
		if gerr != nil {
			joined = errors.Join(joined, gerr)
			continue
		}
		if !found {
			continue
		}
		if m.GetGameMode() != "" && m.GetGameMode() != u.cfg.GameMode {
			// Scan is global but active is mode-local. Never let the PVP
			// reconciler adopt a PVE allocation job (or vice versa).
			continue
		}
		// Auto-confirm/solo formation deliberately lands as fully accepted
		// CONFIRM first. If the creator died after reserving the complete graph but
		// before the ALLOCATING CAS, the canonical scan completes that handoff.
		discoveryChecked := false
		if m.Stage == stageConfirm && allAccepted(m.GetMembers()) {
			queued, queueErr := u.queueAcceptedMatchAllocation(ctx, m)
			discoveryChecked = true // queueAcceptedMatchAllocation performs the exact check.
			if queueErr != nil {
				joined = errors.Join(joined, queueErr)
			} else {
				m = queued
			}
		}
		if !discoveryChecked && (m.Stage == stageConfirm || m.Stage == stageAllocating || m.Stage == stageReady) {
			if discoveryErr := u.ensureMatchDiscovery(ctx, m); discoveryErr != nil {
				joined = errors.Join(joined, discoveryErr)
			}
		}
		// Discovery health never controls derived-index repair. In particular, a
		// crash after CreateMatch but before all reservations must still regain an
		// active entry so expireOnce can durably fail/requeue the partial formation.
		switch m.Stage {
		case stageConfirm, stageAllocating:
			if aerr := u.repo.EnsureActive(ctx, mid, m.ConfirmDeadlineMs); aerr != nil {
				joined = errors.Join(joined, aerr)
			}
		case stageFailed:
			if m.GetAllocationNextAttemptAtMs() == -1 {
				if rerr := u.repo.RemoveActive(ctx, mid); rerr != nil {
					joined = errors.Join(joined, rerr)
				}
			} else if aerr := u.repo.EnsureActive(ctx, mid, m.ConfirmDeadlineMs); aerr != nil {
				joined = errors.Join(joined, aerr)
			}
		case stageReady:
			// READY 的 active 表项语义是「推送交付未确认」,由 advanceAllocationsOnce
			// (finalizeReadyMatch)补推并移出。canonical 扫描无法区分「已交付」与
			// 「未交付」,既不清除(会拆掉补推驱动)也不补建(READY 记录存活到
			// ReleaseMatch,补建会对已交付的局每 5s 重复推送一整场)。
		}
	}
	return joined
}

func (u *MatchUsecase) ensureMatchDiscovery(ctx context.Context, m *matchv1.MatchStorageRecord) error {
	if m == nil || m.GetMatchId() == 0 || len(m.GetMembers()) == 0 || len(m.GetTicketIds()) == 0 {
		return errcode.New(errcode.ErrUnavailable, "incomplete canonical match discovery graph")
	}
	expectedPlayers := make(map[uint64]struct{}, len(m.GetMembers()))
	for _, member := range m.GetMembers() {
		playerID := member.GetPlayerId()
		if playerID == 0 {
			return errcode.New(errcode.ErrUnavailable, "match %d has zero player in canonical roster", m.GetMatchId())
		}
		if _, duplicate := expectedPlayers[playerID]; duplicate {
			return errcode.New(errcode.ErrUnavailable,
				"match %d canonical roster duplicates player %d", m.GetMatchId(), playerID)
		}
		expectedPlayers[playerID] = struct{}{}
	}

	var joined error
	tickets := make([]*matchv1.MatchTicketStorageRecord, 0, len(m.GetTicketIds()))
	seenTicketIDs := make(map[uint64]struct{}, len(m.GetTicketIds()))
	for _, ticketID := range m.GetTicketIds() {
		if ticketID == 0 {
			joined = errors.Join(joined, errcode.New(errcode.ErrUnavailable,
				"match %d has zero ticket id", m.GetMatchId()))
			continue
		}
		if _, duplicate := seenTicketIDs[ticketID]; duplicate {
			joined = errors.Join(joined, errcode.New(errcode.ErrUnavailable,
				"match %d duplicates ticket %d", m.GetMatchId(), ticketID))
			continue
		}
		seenTicketIDs[ticketID] = struct{}{}
		ticket, found, err := u.repo.GetTicket(ctx, ticketID)
		if err != nil {
			joined = errors.Join(joined, err)
			continue
		}
		if !found || ticket.GetMatchId() != m.GetMatchId() {
			joined = errors.Join(joined, errcode.New(errcode.ErrUnavailable,
				"match %d ticket %d discovery edge missing/drifted", m.GetMatchId(), ticketID))
			continue
		}
		tickets = append(tickets, ticket)
	}
	if joined != nil {
		return joined
	}

	// Validate the ticket-union exactly equals the canonical roster before
	// creating or persisting any claim. A subset/superset/duplicate graph is
	// UNKNOWN and must never reach AllocateBattle.
	seenPlayers := make(map[uint64]struct{}, len(expectedPlayers))
	for _, ticket := range tickets {
		if len(ticket.GetMembers()) == 0 {
			joined = errors.Join(joined, errcode.New(errcode.ErrUnavailable,
				"match %d ticket %d has empty roster", m.GetMatchId(), ticket.GetTicketId()))
			continue
		}
		for _, member := range ticket.GetMembers() {
			playerID := member.GetPlayerId()
			if _, expected := expectedPlayers[playerID]; !expected {
				joined = errors.Join(joined, errcode.New(errcode.ErrUnavailable,
					"match %d ticket %d contains unexpected player %d", m.GetMatchId(), ticket.GetTicketId(), playerID))
				continue
			}
			if _, duplicate := seenPlayers[playerID]; duplicate {
				joined = errors.Join(joined, errcode.New(errcode.ErrUnavailable,
					"match %d ticket graph duplicates player %d", m.GetMatchId(), playerID))
				continue
			}
			seenPlayers[playerID] = struct{}{}
		}
	}
	for playerID := range expectedPlayers {
		if _, seen := seenPlayers[playerID]; !seen {
			joined = errors.Join(joined, errcode.New(errcode.ErrUnavailable,
				"match %d ticket graph omits player %d", m.GetMatchId(), playerID))
		}
	}
	if joined != nil {
		return joined
	}

	for _, ticket := range tickets {
		for _, member := range ticket.GetMembers() {
			ticketID := ticket.GetTicketId()
			existing, claimed, claimErr := u.repo.ClaimPlayer(ctx, member.GetPlayerId(), ticketID, u.ticketTTL())
			if claimErr != nil {
				joined = errors.Join(joined, claimErr)
				continue
			}
			if !claimed && existing != ticketID {
				joined = errors.Join(joined, errcode.New(errcode.ErrMatchConcurrent,
					"match %d player %d claim owned by ticket %d", m.GetMatchId(), member.GetPlayerId(), existing))
				continue
			}
			joined = errors.Join(joined, u.repo.PersistPlayerClaim(ctx, member.GetPlayerId(), ticketID))
		}
	}
	return joined
}

// advanceAllocationsOnce 推进 active 中所有到期的 durable allocation jobs。
func (u *MatchUsecase) advanceAllocationsOnce(ctx context.Context) error {
	ids, err := u.repo.RangeActiveMatches(ctx)
	if err != nil {
		return err
	}
	var joined error
	var allocJobs []*matchv1.MatchStorageRecord
	for _, mid := range ids {
		m, found, gerr := u.repo.GetMatch(ctx, mid)
		if gerr != nil {
			// canonical 状态未知时绝不能 ZREM。
			joined = errors.Join(joined, gerr)
			continue
		}
		if !found {
			// canonical 明确不存在时，派生索引才可清理。
			if rerr := u.repo.RemoveActive(ctx, mid); rerr != nil {
				joined = errors.Join(joined, rerr)
			}
			continue
		}
		if m.GetGameMode() != "" && m.GetGameMode() != u.cfg.GameMode {
			if rerr := u.repo.RemoveActive(ctx, mid); rerr != nil {
				joined = errors.Join(joined, rerr)
			}
			continue
		}
		switch m.Stage {
		case stageAllocating:
			// 攒批,listing 循环结束后有界并发推进(压测审核【必修-3】):分配含最长
			// ~60s 的 RPC,不得在 listing 循环内串行内联(READY 补推会被头阻塞)。
			allocJobs = append(allocJobs, m)
		case stageFailed:
			if m.GetAllocationNextAttemptAtMs() == -1 {
				if rerr := u.repo.RemoveActive(ctx, mid); rerr != nil {
					joined = errors.Join(joined, rerr)
				}
			} else if cleanupErr := u.failMatch(ctx, m, failedMatchClassifier(m)); cleanupErr != nil {
				joined = errors.Join(joined, cleanupErr)
			}
		case stageReady:
			// READY 仍在 active = READY 推送交付未确认(崩溃窗口 / Kafka 中断)。
			// 幂等补推,全员成功才移出 active;失败保留下轮重试。
			if ferr := u.finalizeReadyMatch(ctx, m); ferr != nil {
				joined = errors.Join(joined, ferr)
			}
		}
	}
	if aerr := u.advanceAllocationsParallel(ctx, allocJobs); aerr != nil {
		joined = errors.Join(joined, aerr)
	}
	return joined
}

// advanceAllocationsParallel 有界并发推进一批 ALLOCATING match(压测审核【必修-3】)。
//
// 并发上限 = cap(allocSem)(conf.AllocationWorkers,默认 16);返回前 join 全部完成,
// 保持「返回即已尝试完毕」的同步契约 —— 错误可 errors.Join 上抛、调用方/测试可确定
// 断言,且同一 match 绝无跨 tick 并发尝试。tick 分配耗时从 Σ(每局分配时延) 降为
// max(单局分配时延)。每个 worker 经 safego.Go 兜 panic(【必修-6】同源):单局 panic
// 只丢本次尝试(durable job 下 tick 重试),不崩撮合进程。
//
// ctx 是 RunMatchLoop 的服务生命周期 ctx(非请求 ctx,无 §16.7 逃逸问题);进程退出时
// 在途分配被取消,由 durable allocation job 语义保证下任 leader 幂等续推。
func (u *MatchUsecase) advanceAllocationsParallel(ctx context.Context, jobs []*matchv1.MatchStorageRecord) error {
	if len(jobs) == 0 {
		return nil
	}
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		joined error
	)
	for _, m := range jobs {
		wg.Add(1)
		u.allocSem <- struct{}{} // 阻塞抢槽:池满时在此排队,总量仍被 join 收敛
		safego.Go(ctx, "matchmaker_allocation_worker", func() {
			defer func() {
				<-u.allocSem
				wg.Done()
			}()
			if aerr := u.advanceAllocation(ctx, m); aerr != nil {
				mu.Lock()
				joined = errors.Join(joined, aerr)
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	return joined
}

// matchOnce 扫描一次队列,尽可能多地凑出 match(每局 need=2*teamSize 人,teamSize 按 map_id 读关卡表)。
//
// 算法:按 avg_mmr 升序取票据,贪心累积进一个组,当组内总人数达到 2×TeamSize 且 MMR 跨度
// 在动态窗口内时,用 largest-first 装箱拆成两边各 TeamSize。装箱失败则前移起点重试。
//
// 两级撮合(scale-cellular-20m.md §4.4,router 已配时):
//   - 单 Cell / 阶段 1~2(router 未配)→ 单桶贪心(历史行为)。
//   - 多 Region(阶段 3)→ ① 各 owner region 桶内独立贪心(同 region 优先,低延迟);
//     ② 本 region 凑不齐且等待超阈值的剩余票据,进跨 region 溢出贪心(受跨 region 比例上限约束)。
func (u *MatchUsecase) matchOnce(ctx context.Context) error {
	ticketIDs, err := u.repo.RangeQueueTickets(ctx)
	if err != nil {
		return err
	}
	if len(ticketIDs) == 0 {
		return nil
	}

	// 载入票据(过滤已消失的),按 avg_mmr 升序
	tickets := make([]*matchv1.MatchTicketStorageRecord, 0, len(ticketIDs))
	for _, tid := range ticketIDs {
		t, found, gerr := u.repo.GetTicket(ctx, tid)
		if gerr != nil {
			continue
		}
		if !found {
			// 票据 record 已过期/删除但 queue ZSET 残留(Redis Cluster 拆事务后索引漂移的天然兜底):
			// best-effort 补清,避免 queue 无界堆积。失败无妨,下一轮再补。
			_ = u.repo.DeleteTicket(ctx, tid)
			continue
		}
		if t.MatchId != 0 {
			continue
		}
		tickets = append(tickets, t)
	}
	sort.SliceStable(tickets, func(i, j int) bool { return tickets[i].AvgMmr < tickets[j].AvgMmr })

	// 入口模式分流:逐票按**票据上落定的 entry_mode** 决定直进还是撮合。直进票不参与凑局,
	// 立即成局;其余进撮合分组。分流在同一个 loop 内完成,不新开循环/协程——单写者仍是
	// 「每个池一个 leader」(decision-revisit-matchmaker-single-writer.md),本改动不触碰该约束。
	//
	// 为什么读票不读表:关卡表 entry_mode=BOTH 的图两个入口共存,"这张票是排队还是直进"
	// 只有玩家的选择知道,回查表答不了。旧票(滚动升级期写入、无本字段)按表/部署开关兜底。
	pending := make([]*matchv1.MatchTicketStorageRecord, 0, len(tickets))
	for _, t := range tickets {
		if !u.isWalkInTicket(t) {
			pending = append(pending, t)
			continue
		}
		if err := u.formSoloMatch(ctx, t); err != nil {
			if errcode.As(err) == errcode.ErrRateLimited {
				// 成局冷却窗内的静默节流:票据留队等下轮,不是异常。
				plog.With(ctx).Debugw("msg", "form_solo_match_throttled", "ticket_id", t.TicketId)
			} else {
				plog.With(ctx).Warnw("msg", "form_solo_match_failed", "ticket_id", t.TicketId, "err", err)
			}
		}
	}
	if len(pending) == 0 {
		return nil
	}
	tickets = pending

	now := time.Now().UnixMilli()

	// 按 map_id 分组:同一 game_mode 下不同副本(map_id)各自独立撮合,
	// 避免不同副本的玩家被凑进同一局。「策划填表即用」——新增副本(新 map_id)
	// 自然形成新组,matchmaker 无需改代码;组内仍走原 单桶 / 两级 region 撮合。
	for _, group := range u.partitionTicketsByMap(tickets) {
		u.formMatchesInPool(ctx, group, now)
	}
	return nil
}

// formMatchesInPool 在「同一副本(map_id)」的票据组内撮合:单 Cell/dev 走单桶贪心,
// 多 Region 走两级(region 内优先 + 跨 region 溢出兜底)。从 matchOnce 抽出,便于按 map_id 分组复用。
func (u *MatchUsecase) formMatchesInPool(ctx context.Context, tickets []*matchv1.MatchTicketStorageRecord, now int64) {
	// 本池票据同属一个副本(partitionTicketsByMap),一方人数与方数均按该 map_id 读表
	// (表未填分别回退全局 team_size 与 2 方)。need 是一局所需**总人数**,不是票数。
	poolMapID := matchMapID(tickets)
	teamSize := u.teamSizeForMap(poolMapID)
	sideCount := u.sideCountForMap(poolMapID)
	need := sideCount * teamSize
	used := make(map[uint64]bool)

	// 单 Cell / dev / 阶段 1~2(router 未配)→ 单桶贪心(历史行为,零分区开销)。
	if u.router == nil {
		u.greedyFormMatches(ctx, tickets, used, now, teamSize, sideCount, nil)
		return
	}

	// 多 Region(阶段 3)两级撮合(scale-cellular-20m.md §4.4):
	//  ① region 内优先:按 owner region 分桶,各桶内独立贪心(绝大多数对局同 region,低延迟)。
	//  ② 跨 region 溢出:本 region 凑不齐且等待超阈值的剩余票据,进跨 region 兜底贪心,
	//     且每局受"跨 region 玩家比例软上限"约束(WithinCrossRegionCap)。
	buckets, order := partitionTicketsByRegion(tickets, u.ticketRegion)
	for _, region := range order {
		u.greedyFormMatches(ctx, buckets[region], used, now, teamSize, sideCount, nil)
	}

	// 收集本 region 内未成局的剩余票据(保持 MMR 升序),挑出可溢出者跨 region 兜底撮合。
	leftover := make([]*matchv1.MatchTicketStorageRecord, 0, len(tickets))
	for _, t := range tickets {
		if !used[t.TicketId] {
			leftover = append(leftover, t)
		}
	}
	// 本地候选是否充足须基于 region 内撮合**后**的 leftover、按 (region, MMR 桶) 细分判定:
	// region 总人数足够但本轮同段位/MMR 窗口剩余不足时,久等票据仍应放开跨 region(§2.2)。
	leftoverTotals := leftoverRegionBucketTotals(leftover, u.ticketRegion, u.ticketMmrBucket)
	overflow := selectOverflowTickets(leftover, u.ticketRegion, leftoverTotals, u.ticketMmrBucket, need, u.regionPolicy, u.ticketTier, now)
	if len(overflow) > 0 {
		u.greedyFormMatches(ctx, overflow, used, now, teamSize, sideCount, u.withinCrossRegionCap)
	}
}

// withinCrossRegionCap 是跨 region 溢出贪心的成局守卫:一局玩家的 region 分布须满足
// "跨 region 玩家比例软上限"(decision-revisit-global-matchmaker.md §2.2),否则拒绝该组合,
// 防一局横跨多区导致体验崩坏。
func (u *MatchUsecase) withinCrossRegionCap(group []*matchv1.MatchTicketStorageRecord) bool {
	regions := make([]uint32, 0, 2*u.cfg.TeamSize)
	for _, t := range group {
		r := u.ticketRegion(t)
		for range t.Members {
			regions = append(regions, r)
		}
	}
	return u.regionPolicy.WithinCrossRegionCap(regions)
}

// greedyFormMatches 在给定票据切片(已按 MMR 升序)上做"按 MMR 窗口贪心装箱凑 need=2*teamSize"撮合,
// 成局即 formMatch 并把票据标记进 used。validate 非 nil 时,装箱成功后还须通过该守卫才成局
// (跨 region 溢出用它做比例上限校验);validate 为 nil 表示无额外约束(单桶 / region 内)。
//
// 这是原 matchOnce 主循环抽出的可复用核(单桶 / 各 region 桶 / 跨 region 溢出桶共用),
// 行为与抽取前完全一致(validate=nil 时)。
// partitionTicketsByMap 按 map_id(副本编号)把票据分组,保持各组内原有的 MMR 升序。
// 返回顺序按 map_id 升序,保证撮合确定性。同一 game_mode 下不同副本各自成局,互不串池。
//
// 复审 P1:分组 key 用 effective map_id(0→cfg.MapId)归一化——旧客户端省略 map_id(0=用默认
// 副本)与新客户端显式发送默认 map_id 语义相同,若按原始值分组会拆成两个池永不互相撮合。
// 只归一化分组 key,不改票据存储的原始 map_id;成局后 teamSizeForMap / DS 分配对 0 的兜底
// 口径一致(0 与 cfg.MapId 解析到同一副本与人数),故同池混装安全。
func (u *MatchUsecase) partitionTicketsByMap(tickets []*matchv1.MatchTicketStorageRecord) [][]*matchv1.MatchTicketStorageRecord {
	buckets := make(map[uint32][]*matchv1.MatchTicketStorageRecord)
	order := make([]uint32, 0)
	for _, t := range tickets {
		mid := t.GetMapId()
		if mid == 0 {
			mid = u.cfg.MapId // 归一化:0(省略=默认副本)与显式默认 map 同池
		}
		if _, ok := buckets[mid]; !ok {
			order = append(order, mid)
		}
		buckets[mid] = append(buckets[mid], t)
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	groups := make([][]*matchv1.MatchTicketStorageRecord, 0, len(order))
	for _, mid := range order {
		groups = append(groups, buckets[mid])
	}
	return groups
}

// matchMapID 取一场 match 的副本编号:同一局的票据来自同一 map_id 分组(partitionTicketsByMap),
// 故取任一非空票据的 map_id 即可;全空则回退 0(默认副本)。
func matchMapID(sides ...[]*matchv1.MatchTicketStorageRecord) uint32 {
	for _, side := range sides {
		for _, t := range side {
			if t != nil {
				return t.GetMapId()
			}
		}
	}
	return 0
}

func (u *MatchUsecase) greedyFormMatches(
	ctx context.Context,
	tickets []*matchv1.MatchTicketStorageRecord,
	used map[uint64]bool,
	now int64,
	teamSize int,
	sideCount int,
	validate func(group []*matchv1.MatchTicketStorageRecord) bool,
) {
	// need 是一局所需**总人数** = 方数 × 每方人数;凑局按人数累加(见下方 total),
	// 因此「3 人队 + 2 人散排」与「5 个单排」都能凑满一方,无需额外的拼队机制。
	need := sideCount * teamSize
	for start := 0; start < len(tickets); start++ {
		if used[tickets[start].TicketId] {
			continue
		}
		group := make([]*matchv1.MatchTicketStorageRecord, 0, need)
		total := 0
		for j := start; j < len(tickets) && total < need; j++ {
			t := tickets[j]
			if used[t.TicketId] {
				continue
			}
			if len(group) > 0 && !withinWindow(group[0], t, now, u.cfg) {
				break // 已按 MMR 排序,后面只会更远
			}
			group = append(group, t)
			total += len(t.Members)
		}
		if total != need {
			continue
		}
		sides, ok := binPack(group, teamSize, sideCount)
		if !ok {
			continue
		}
		if validate != nil && !validate(group) {
			continue // 跨 region 比例超上限等约束未过,放弃该组合
		}
		if err := u.formMatch(ctx, sides); err != nil {
			if errcode.As(err) == errcode.ErrRateLimited {
				plog.With(ctx).Debugw("msg", "form_match_throttled")
			} else {
				plog.With(ctx).Warnw("msg", "form_match_failed", "err", err)
			}
			continue
		}
		for _, t := range group {
			used[t.TicketId] = true
		}
	}
}

// formSoloMatch 是「即时开局 / walk-in」成局路径(WalkIn=true 时 matchOnce 逐票调用):
// 函数名与其 solo_match_found 日志键沿用旧称未改——日志键是可观测性契约(被 2026-07-24 事故档案
// 时间线当证据引用),正名只落在配置键 walk_in 上,避免无谓 churn。
// 单张队伍票据(单人或整队)直接成局、跳过撮合与确认,不与陌生人凑对手。
// PVE 实例(matchmaker-pve.yaml,game_mode=pve_coop)的生产核心路径 =「组好队 / 单人直进副本」;
// 非仅测试用(历史注释误导,正名建议见 docs/design/decision-dungeon-entry-modes.md)。
// 它仍须先完成票据/claim 图，再提交 durable ALLOCATING job。
func (u *MatchUsecase) formSoloMatch(ctx context.Context, ticket *matchv1.MatchTicketStorageRecord) error {
	if err := u.requireLocalGameMode(ticket.GetGameMode()); err != nil {
		return err
	}
	// 成局级冷却(anti-abuse §6 第 7 项):首次成局 SETNX 占窗零延迟;退票重排队后的
	// 重成局在窗内被压到冷却节拍,替代原「每 match_interval(2s)重成局」风暴
	// (decision-revisit-allocating-bounded-terminal.md §2.3)。fail-open。
	if err := u.tryFormCooldown(ctx, ticket.TicketId); err != nil {
		return err
	}
	// 成局前离场复查(INC-20260814-001):solo/walk-in 票同样可能是隔夜幽灵票
	// (单人排队后关客户端,恰好是本事故的形状),不复查就直接白拉一台 DS。
	if err := u.rejectAbsentTickets(ctx, []*matchv1.MatchTicketStorageRecord{ticket}); err != nil {
		return err
	}
	// 每次成局用新雪花 match_id,与 formMatch(撮合路径)同型。此前 solo 复用
	// ticket_id 做 match_id:退票重排队不换 ticket_id ⇒ 同一 match_id 反复成局,
	// 会撞 ds_allocator 侧保留 2h 的 uncertain/abandoned claim(同上决策文档 §2.3,
	// 2026-08-10 拍板修复)。客户端句柄不变:QUEUEING 阶段句柄仍是 ticket_id,
	// 成局后经 MATCHING/READY 推送与 GetMatchProgress 的票据→match 间接解析拿到
	// match_id——撮合路径二者本就不同,客户端契约早已覆盖。
	matchID := u.idGen.Generate()
	now := time.Now().UnixMilli()

	members := make([]*matchv1.MatchMemberStorageRecord, 0, len(ticket.Members))
	for _, m := range ticket.Members {
		members = append(members, &matchv1.MatchMemberStorageRecord{
			PlayerId: m.PlayerId,
			TeamId:   m.TeamId,
			Mmr:      m.Mmr,
			HeroId:   m.HeroId,
			Side:     0,
			Confirm:  confirmAccepted,
			// 代际必须跟着成员走完「票据 → match 镜像」这一跳:ReleaseMatch 只从 match
			// 成员收 roster(collectTeamRosters),这里掉了它,EndTeamMatch 的跨代 CAS 在
			// 主路径上就恒为退化档(gen=0 = 只要还挂着 ready 就清)——迟到重投会把玩家
			// 结算后新点的准备抹掉,正是 INC-20260813-001 ① 建代际要防的形状。
			TeamReadyGeneration: m.GetTeamReadyGeneration(),
		})
	}
	match := &matchv1.MatchStorageRecord{
		MatchId:           matchID,
		Stage:             stageConfirm,
		Members:           members,
		TicketIds:         []uint64{ticket.TicketId},
		CreatedAtMs:       now,
		ConfirmDeadlineMs: now + u.cfg.ConfirmTimeout.Std().Milliseconds(),
		MapId:             ticket.MapId,
		GameMode:          ticket.GetGameMode(),
	}

	// 一致性顺序(先建 match 再预留,与 formMatch 一致):match 先落库并进 active ZSET,
	// 预留后崩溃也能被 expireOnce 兼带清理,不留“match_id 指向不存在 match”的孤儿票据。
	if err := u.repo.CreateMatch(ctx, match, u.matchTTL()); err != nil {
		return err // 票据未动,仍在队列,下轮重试
	}
	ticket.MatchId = matchID
	if err := u.repo.ReserveTicket(ctx, ticket, u.ticketTTL()); err != nil {
		_ = u.repo.DeleteMatch(ctx, matchID) // 票据未预留成功,删空 match 即可
		return fmt.Errorf("reserve solo ticket %d: %w", ticket.TicketId, err)
	}
	if err := u.persistClaims(ctx, ticket); err != nil {
		// Match + reserved ticket are canonical and remain on active discovery.
		// Do not publish a ready/found edge until every claim is durable; the
		// reconciler will retry this exact graph without manufacturing a new match.
		return fmt.Errorf("persist solo match claims: %w", err)
	}
	queued, err := u.queueAcceptedMatchAllocation(ctx, match)
	if err != nil {
		return fmt.Errorf("queue solo match allocation: %w", err)
	}

	u.notifyMatching(ctx, memberPlayerIDs(members), matchID)
	plog.With(ctx).Infow("msg", "solo_match_found", "match_id", matchID, "ticket_id", ticket.TicketId,
		"players", len(members), "operation_id", queued.GetAllocationOperationId(),
		"ticket_age_ms", oldestTicketAgeMs(now, []*matchv1.MatchTicketStorageRecord{ticket}))
	// 只持久登记 allocation job；后台 worker 负责 Allocate→placement→READY。
	return nil
}

// formMatch 把已装箱的各方票据组成一场 match:写 match record + 预留票据 + 推 FOUND/CONFIRM。
// sides 由 binPack 产出,长度 = 关卡表 side_count(PVE 合作 1 方、常规对抗 2 方、混战 N 方);
// 成员的 Side 即其所在方的下标,DS 侧阵营分配(player_combat_factions)本就支持 >2 方。
func (u *MatchUsecase) formMatch(ctx context.Context, sides [][]*matchv1.MatchTicketStorageRecord) error {
	totalTickets := 0
	for _, side := range sides {
		totalTickets += len(side)
		for _, ticket := range side {
			if err := u.requireLocalGameMode(ticket.GetGameMode()); err != nil {
				return err
			}
		}
	}
	// 成局级冷却探测(anti-abuse §6 第 7 项):容量耗尽退票布设的静默窗对撮合路径同样
	// 生效,否则同一批票据每 tick 重新组局→分配→再失败,与 solo 是同一个风暴。
	if u.anyTicketInFormCooldown(ctx, sides) {
		return errcode.New(errcode.ErrRateLimited, "tickets in form cooldown")
	}
	// 成局前离场复查(INC-20260814-001):扫除是 10s 节流的,旧票可能在两次扫除之间
	// 被凑进局。这里用**装箱后的最终票据集合**再查一次,含离场超窗成员的票当场回收,
	// 无辜票据留在队列由下轮重新凑局,不整组陪葬。
	flat := make([]*matchv1.MatchTicketStorageRecord, 0, totalTickets)
	for _, side := range sides {
		flat = append(flat, side...)
	}
	if err := u.rejectAbsentTickets(ctx, flat); err != nil {
		return err
	}
	matchID := u.idGen.Generate()
	now := time.Now().UnixMilli()
	deadline := now + u.cfg.ConfirmTimeout.Std().Milliseconds()

	members := make([]*matchv1.MatchMemberStorageRecord, 0, len(sides)*u.cfg.TeamSize)
	ticketIDs := make([]uint64, 0, totalTickets)
	initialConfirm := confirmPending
	if u.cfg.AutoConfirmMatch {
		initialConfirm = confirmAccepted
	}
	collect := func(side []*matchv1.MatchTicketStorageRecord, sideIdx int32) {
		for _, t := range side {
			ticketIDs = append(ticketIDs, t.TicketId)
			for _, m := range t.Members {
				members = append(members, &matchv1.MatchMemberStorageRecord{
					PlayerId: m.PlayerId,
					TeamId:   m.TeamId,
					Mmr:      m.Mmr,
					HeroId:   m.HeroId,
					Side:     sideIdx,
					Confirm:  initialConfirm,
					// 同 formSoloMatch:代际断在这一跳,EndTeamMatch 跨代 CAS 主路径全废
					// (且每场组队局释放都会伪报 team_match_end_legacy_generation)。
					TeamReadyGeneration: m.GetTeamReadyGeneration(),
				})
			}
		}
	}
	for i, side := range sides {
		collect(side, int32(i))
	}

	match := &matchv1.MatchStorageRecord{
		MatchId: matchID,
		// Even auto-confirm starts in CONFIRM. ALLOCATING is the commit that
		// proves every reservation and claim below is durable.
		Stage:             stageConfirm,
		Members:           members,
		TicketIds:         ticketIDs,
		CreatedAtMs:       now,
		ConfirmDeadlineMs: deadline,
		MapId:             matchMapID(sides...),
		GameMode:          u.cfg.GameMode,
	}

	// 一致性流程(先建 match,再预留票据):
	//   1. 先 CreateMatch(含写入 active ZSET)。失败则票据未动、全在队列,下轮重试。
	//   2. 逐张预留票据(移出队列 + 写 match_id + 续 claim),防下一轮重复撮合。
	//   3. 任一预留失败 → 先把已预留票据退回队列,再删 match(顺序不可倒:先删 match
	//      会让并发的孤儿清理路径误删即将退回的票据)。
	// 为什么先建 match:若先预留后建 match,两步之间崩溃会留下“match_id 指向不存在
	// match”的孤儿票据——不在队列、不在 active ZSET、matchOnce/expireOnce 都看不见,
	// 成员 claim 卡死 30min。现在任意点崩溃:match 在 active ZSET 里,到期由 expireOnce
	// 判失败退票自愈;未预留的票据仍在队列可重撮(onMatchFailed 只碰 match_id 相符的票)。
	if err := u.repo.CreateMatch(ctx, match, u.matchTTL()); err != nil {
		plog.With(ctx).Errorw("msg", "create_match_failed", "match_id", matchID, "err", err)
		return err
	}
	reserved := make([]*matchv1.MatchTicketStorageRecord, 0, totalTickets)
	var persistErr error
	for _, side := range sides {
		for _, t := range side {
			t.MatchId = matchID
			if err := u.repo.ReserveTicket(ctx, t, u.ticketTTL()); err != nil {
				u.rollbackReservations(ctx, reserved)
				_ = u.repo.DeleteMatch(ctx, matchID)
				plog.With(ctx).Errorw("msg", "reserve_ticket_failed", "match_id", matchID,
					"ticket_id", t.TicketId, "err", err)
				return fmt.Errorf("reserve ticket %d: %w", t.TicketId, err)
			}
			reserved = append(reserved, t)
			if err := u.persistClaims(ctx, t); err != nil {
				// Continue reserving the complete canonical ticket set. Returning in
				// the middle would leave later tickets queued while the match already
				// references them, a shape the reconciler cannot safely guess away.
				persistErr = errors.Join(persistErr, err)
			}
		}
	}
	if persistErr != nil {
		return fmt.Errorf("persist match %d claims before FOUND: %w", matchID, persistErr)
	}
	var queued *matchv1.MatchStorageRecord
	if u.cfg.AutoConfirmMatch {
		var queueErr error
		queued, queueErr = u.queueAcceptedMatchAllocation(ctx, match)
		if queueErr != nil {
			return fmt.Errorf("queue auto-confirm match %d allocation: %w", matchID, queueErr)
		}
	}
	// 撮合成局，成员进入确认期：上报 locator MATCHING（不变量 §1，弱依赖）
	u.notifyMatching(ctx, memberPlayerIDs(members), matchID)
	// 推 FOUND → CONFIRM 进度给全体(原则 3 例外:含发起方)
	u.pushProgress(ctx, matchID, stageFound, members, "", match.MapId)
	u.pushProgress(ctx, matchID, stageConfirm, members, "", match.MapId)
	// ticket_ids + 票龄进日志(INC-20260814-001 可观测性缺口):此前只有 players=N,
	// 幽灵票成局后要跨三个服务的日志反查才能定位到隔夜旧票。
	oldestAgeMs := oldestTicketAgeMs(now, flat)
	plog.With(ctx).Infow("msg", "match_found", "match_id", matchID, "players", len(members),
		"auto_confirm", u.cfg.AutoConfirmMatch, "ticket_ids", ticketIDs,
		"oldest_ticket_age_ms", oldestAgeMs)
	if oldestAgeMs > staleTicketWarnAge.Milliseconds() {
		plog.With(ctx).Warnw("msg", "stale_ticket_matched", "match_id", matchID,
			"oldest_ticket_age_ms", oldestAgeMs, "ticket_ids", ticketIDs)
	}
	if u.cfg.AutoConfirmMatch {
		plog.With(ctx).Debugw("msg", "match_allocation_queued", "match_id", matchID,
			"operation_id", queued.GetAllocationOperationId())
	}
	return nil
}

// rollbackReservations 把一批已预留的票据退回队列(清掉 match_id,保留 enqueued_at_ms),
// 用于 formMatch 中途失败时的补偿,避免票据停留在"已出队但无 match"的悬空状态。
// 调用方须在本函数之后才删 match(先退票再删 match)。
func (u *MatchUsecase) rollbackReservations(ctx context.Context, reserved []*matchv1.MatchTicketStorageRecord) {
	for _, t := range reserved {
		// 守卫退队(同 failMatch):这些票刚被 ReserveTicket 置 MatchId=matchID;若并发
		// CancelMatch 已删其一,守卫退队 no-op 不复活。
		expectedMatchID := t.MatchId
		t.MatchId = 0
		requeued, err := u.repo.RequeueTicketIfOwned(ctx, t, expectedMatchID, u.ticketTTL())
		if err != nil {
			plog.With(ctx).Warnw("msg", "rollback_reservation_failed", "ticket_id", t.TicketId, "err", err)
			continue
		}
		if !requeued {
			continue
		}
		u.refreshClaims(ctx, t) // 票据 TTL 已刷新,claim 同步续期(见 onMatchFailed 注释)
	}
}

// refreshClaims 把滚动升级遗留的 TTL claim 原子升级成 persistent；新版本 claim
// 从创建起即无 TTL。失败只用于补偿路径告警，原 match/ticket canonical 状态仍保留。
func (u *MatchUsecase) refreshClaims(ctx context.Context, ticket *matchv1.MatchTicketStorageRecord) {
	for _, m := range ticket.Members {
		if err := u.repo.RefreshPlayerClaim(ctx, m.PlayerId, ticket.TicketId, u.ticketTTL()); err != nil {
			plog.With(ctx).Warnw("msg", "refresh_claim_failed", "player_id", m.PlayerId, "ticket_id", ticket.TicketId, "err", err)
		}
	}
}

func (u *MatchUsecase) persistClaims(ctx context.Context, ticket *matchv1.MatchTicketStorageRecord) error {
	var joined error
	for _, m := range ticket.Members {
		if err := u.repo.PersistPlayerClaim(ctx, m.PlayerId, ticket.TicketId); err != nil {
			joined = errors.Join(joined, fmt.Errorf("persist player %d ticket %d: %w", m.PlayerId, ticket.TicketId, err))
		}
	}
	return joined
}

// expireOnce 扫描 active ZSET,把确认期已超时的 match 标记失败。
//
// ALLOCATING 特殊处理:确认期截止但仍在分配 DS 属正常(最后一人踩点确认),给 allocatingGrace
// 宽限期并保留在 active ZSET 里继续观察;超宽限仍未到 READY → 判失败(分配方已崩溃)。
func (u *MatchUsecase) expireOnce(ctx context.Context) error {
	now := time.Now().UnixMilli()
	matchIDs, err := u.repo.RangeExpiredMatches(ctx, now)
	if err != nil {
		return err
	}
	for _, mid := range matchIDs {
		var snapshot *matchv1.MatchStorageRecord
		var keepActive bool
		lerr := u.repo.UpdateMatchWithLock(ctx, mid, u.cfg.OptimisticRetry, func(m *matchv1.MatchStorageRecord) error {
			snapshot, keepActive = nil, false
			switch m.Stage {
			case stageReady:
				// READY 滞留 active = 推送交付未确认,由撮合循环 finalizeReadyMatch
				// 补推后移出;确认期 deadline 对 READY 无意义,不清索引也不判失败。
				keepActive = true
				return nil
			case stageFailed:
				if m.GetAllocationNextAttemptAtMs() != -1 {
					snapshot = cloneMatch(m) // cleanup 未 durable ACK，继续重放
				}
				return nil
			case stageAllocating:
				// ALLOCATING 是 durable job。外部结果可能未知（尤其 allocation_uncertain），
				// 本地时间绝不能把未知推断成失败并重排；worker/reconciler 会持续推进。
				keepActive = true
				return nil
			}
			m.Stage = stageFailed
			snapshot = cloneMatch(m)
			return nil
		}, u.matchTTL())
		if lerr != nil {
			plog.With(ctx).Warnw("msg", "expire_lock_failed", "match_id", mid, "err", lerr)
			// 只有 canonical 明确不存在才清派生索引；Redis/CAS 瞬态错误必须保留重试。
			if errcode.As(lerr) == errcode.ErrMatchNotFound {
				u.removeActive(ctx, mid)
			}
			continue
		}
		if keepActive {
			continue
		}
		if snapshot == nil {
			u.removeActive(ctx, mid)
			continue
		}
		// 超时:无明确拒绝者,全部票据退回队列(rejecterID=0)
		if cleanupErr := u.failMatch(ctx, snapshot, failedMatchClassifier(snapshot)); cleanupErr != nil {
			plog.With(ctx).Warnw("msg", "match_failed_cleanup_retry", "match_id", mid, "err", cleanupErr)
			continue
		}
		plog.With(ctx).Infow("msg", "match_confirm_timeout", "match_id", mid)
	}
	return nil
}

// livenessSweepInterval 是队列在线扫除的节流间隔。撮合 tick(秒级)远小于它;
// locator TTL 30s + 断线宽限 10s,10s 一扫意味着死票最多存活 ~40s 就被清,
// 且 BatchGetLocation 的批量查询压力可控。
const livenessSweepInterval = 10 * time.Second

// livenessSweepOnce 主动清扫队列里掉线玩家的死票(取消匹配三层防线的中间层:
// 客户端 CancelMatch → 本扫除 → 成局最终门 findOfflineMembers)。
//
// 没有它,掉线者的死票要等被凑进一局、被成局门拦下才删——白害 9 个无辜玩家陪跑
// 一轮 FAILED。拉取校验而非事件推送:周期扫描幂等、自愈、零新增基础设施,
// 不用处理 locator→matchmaker 事件投递的至少一次/乱序/与 travel 的竞态。
//
// 开关:LivenessGateEnabled 默认关闭——离线判定依赖 Hub DS 心跳捎带 player_ids 续期
// locator HUB 位置,生产端未联发前开启会把全部在线玩家 30s 后误判离线、扫掉排队票据。
// 弱依赖:开关关闭 / locator 未配(nil)→ 直接跳过;查询失败 → Warn 后跳过(不误删)。
//
// ❗ INC-20260724-001:本扫除与成局最终门共用同一份 presence 证据,已一并回退为关闭。
// 除成局门那条 MATCHING 不续期的缺陷外,本扫除还有独立的第二受害面:玩家一旦进过
// MATCHING,该 key 到期消失后 RefreshHubLocations 只 EXPIRE 不创建(见 player_locator
// data/location.go),locator 记录无法重建 ⇒ 该玩家此后被恒判离线,连"匹配失败后重新
// 排队"的新票据也会在 ≤10s 内被本扫除误删。重开前置见 etc/matchmaker-dev.yaml 同项注释。
// 删除守卫:DeleteTicketIfUnmatched(WATCH CAS)——读到 MatchId==0 后、删除前若被
// 撮合循环并发预留,放弃删除(该票已进 match,交给成局最终门处理),绝不盲删。
func (u *MatchUsecase) livenessSweepOnce(ctx context.Context) error {
	if !u.cfg.LivenessGateEnabled || u.locator == nil {
		return nil
	}
	ticketIDs, err := u.repo.RangeQueueTickets(ctx)
	if err != nil {
		return err
	}
	if len(ticketIDs) == 0 {
		return nil
	}

	// 载入仍在排队的票据,汇总全体成员
	tickets := make([]*matchv1.MatchTicketStorageRecord, 0, len(ticketIDs))
	playerIDs := make([]uint64, 0, len(ticketIDs))
	for _, tid := range ticketIDs {
		t, found, gerr := u.repo.GetTicket(ctx, tid)
		if gerr != nil || !found || t.MatchId != 0 {
			continue // 已消失 / 已进 match 的票据不归本扫除管
		}
		tickets = append(tickets, t)
		playerIDs = append(playerIDs, memberPlayerIDs(t.Members)...)
	}
	if len(tickets) == 0 {
		return nil
	}

	offline, err := u.locator.FindOfflinePlayers(ctx, playerIDs)
	if err != nil {
		plog.With(ctx).Warnw("msg", "liveness_sweep_query_skipped", "err", err)
		return nil // 弱依赖:locator 抖动不误删任何票
	}
	if len(offline) == 0 {
		return nil
	}
	offlineSet := make(map[uint64]struct{}, len(offline))
	for _, pid := range offline {
		offlineSet[pid] = struct{}{}
	}

	for _, t := range tickets {
		dead := false
		for _, m := range t.Members {
			if _, off := offlineSet[m.PlayerId]; off {
				dead = true
				break
			}
		}
		if !dead {
			continue
		}
		// CAS 条件删(仅当仍未被撮合预留):撞上并发预留则放弃,交给成局最终门
		deleted, _, derr := u.repo.DeleteTicketIfUnmatched(ctx, t.TicketId)
		if derr != nil || !deleted {
			continue
		}
		u.rollbackClaims(ctx, t.TicketId, memberPlayerIDs(t.Members))
		// FAILED 推给票据全体成员:同队在线的队友(组队票)立刻知道排队被取消,
		// 不至于停在 QUEUEING 干等;掉线者本人收不到,重连后 GetMatchProgress 兜底。
		u.pushProgress(ctx, t.TicketId, stageFailed, t.Members, "", t.MapId)
		plog.With(ctx).Infow("msg", "liveness_sweep_reaped_ticket",
			"ticket_id", t.TicketId, "members", len(t.Members))
	}
	return nil
}

// ── 排队票离线回收(INC-20260814-001) ─────────────────────────────────────────

// queueAbsenceReapAfter 返回排队票离线回收的判死窗;<=0 表示整条回收关闭。
func (u *MatchUsecase) queueAbsenceReapAfter() time.Duration {
	return u.cfg.QueueAbsenceReapAfter.Std()
}

// queueAbsenceSweepOnce 周期回收「玩家已离场超过判死窗」的排队票(INC-20260814-001
// 隔夜幽灵票:排队后关客户端,无任何链路把非终态票转终态,次日被拿去和新玩家成局)。
//
// 与 livenessSweepOnce(已回退关闭)的本质区别是证据:那道门按「locator 此刻查不查得到」
// 判死,对正常路径上的投影缺席是结构性假阳性(INC-20260724-001);本回收按「离开了多久」
// 判(absentBeyond:BatchOnline + BatchGetLastSeen,UNKNOWN 一律放行),证据由 Hub DS
// 心跳按 census 全员持续续期,坐在大厅里的人 last_alive_ms 恒为「刚刚」,不会被误杀。
//
// 弱依赖:presence 未注入 / 窗口关闭 → 跳过;查询失败 → Warn 后整轮跳过,绝不在
// 不确定时删票。删除守卫:reapAbsentTicket 的 DeleteTicketIfUnmatched(WATCH CAS),
// 撞上并发撮合预留则放弃(该票已进 match,交给确认期/DS roster 到齐期限兜底)。
func (u *MatchUsecase) queueAbsenceSweepOnce(ctx context.Context) error {
	window := u.queueAbsenceReapAfter()
	if u.presence == nil || window <= 0 {
		return nil
	}
	ticketIDs, err := u.repo.RangeQueueTickets(ctx)
	if err != nil {
		return err
	}
	if len(ticketIDs) == 0 {
		return nil
	}

	tickets := make([]*matchv1.MatchTicketStorageRecord, 0, len(ticketIDs))
	playerIDs := make([]uint64, 0, len(ticketIDs))
	for _, tid := range ticketIDs {
		t, found, gerr := u.repo.GetTicket(ctx, tid)
		if gerr != nil || !found || t.MatchId != 0 {
			continue // 已消失 / 已进 match 的票据不归本回收管
		}
		tickets = append(tickets, t)
		playerIDs = append(playerIDs, memberPlayerIDs(t.Members)...)
	}
	if len(tickets) == 0 {
		return nil
	}

	offline, _, op, err := u.absentBeyond(ctx, playerIDs, window)
	if err != nil {
		plog.With(ctx).Warnw("msg", "queue_absence_sweep_skipped", "op", op, "err", err)
		return nil // 弱依赖:presence 抖动不误删任何票
	}
	if len(offline) == 0 {
		return nil
	}
	offlineSet := make(map[uint64]struct{}, len(offline))
	for _, pid := range offline {
		offlineSet[pid] = struct{}{}
	}

	for _, t := range tickets {
		absent := ticketAbsentMembers(t, offlineSet)
		if len(absent) == 0 {
			continue
		}
		u.reapAbsentTicket(ctx, t, absent, "sweep")
	}
	return nil
}

// rejectAbsentTickets 成局装箱前的最终复查(与队列扫除同判据、同判死窗):任一票含
// 离场超窗成员 → 当场回收该票并拒绝本次成局,其余无辜票留队下轮再凑。堵的是扫除
// 节流间隔(10s)内旧票恰好被凑进局的竞态窗——没有这道复查,幽灵票在两次扫除之间
// 仍能成局一次。
//
// 弱依赖方向与扫除一致但动作相反:presence 查询失败 → 放行成局(fail-open)。
// 本复查是纵深防御,不给 locator 抖动阻断全部成局的权力;真离线成员被凑进去后,
// 由 DS 侧 roster 到齐期限(roster_join_deadline)兜底回收对局。
func (u *MatchUsecase) rejectAbsentTickets(ctx context.Context, tickets []*matchv1.MatchTicketStorageRecord) error {
	window := u.queueAbsenceReapAfter()
	if u.presence == nil || window <= 0 || len(tickets) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(tickets))
	for _, t := range tickets {
		ids = append(ids, memberPlayerIDs(t.Members)...)
	}
	offline, _, op, err := u.absentBeyond(ctx, ids, window)
	if err != nil {
		plog.With(ctx).Warnw("msg", "match_form_absence_check_skipped", "op", op, "err", err)
		return nil
	}
	if len(offline) == 0 {
		return nil
	}
	offlineSet := make(map[uint64]struct{}, len(offline))
	for _, pid := range offline {
		offlineSet[pid] = struct{}{}
	}
	reaped := 0
	for _, t := range tickets {
		absent := ticketAbsentMembers(t, offlineSet)
		if len(absent) == 0 {
			continue
		}
		if u.reapAbsentTicket(ctx, t, absent, "form_recheck") {
			reaped++
		}
	}
	return errcode.New(errcode.ErrMatchMemberOffline,
		"players %v absent beyond %s; reaped %d stale ticket(s) before forming match",
		offline, window, reaped)
}

// reapAbsentTicket CAS 回收一张仍未撮合的陈旧票并释放全体成员归属。
// 撞上并发预留(票已进 match)或删除失败 → 放弃返回 false,下轮扫除/成局复查兜底。
func (u *MatchUsecase) reapAbsentTicket(ctx context.Context, t *matchv1.MatchTicketStorageRecord, absent []uint64, via string) bool {
	deleted, _, derr := u.repo.DeleteTicketIfUnmatched(ctx, t.TicketId)
	if derr != nil || !deleted {
		return false
	}
	u.rollbackClaims(ctx, t.TicketId, memberPlayerIDs(t.Members))
	// FAILED 推给票据全体成员:同票在线队友立刻知道排队被取消;离场者本人收不到,
	// 重连后 GetMatchProgress 兜底。
	u.pushProgress(ctx, t.TicketId, stageFailed, t.Members, "", t.MapId)
	queuedMs := int64(0)
	if t.GetEnqueuedAtMs() > 0 {
		queuedMs = time.Now().UnixMilli() - t.GetEnqueuedAtMs()
	}
	plog.With(ctx).Infow("msg", "queue_absence_reaped_ticket",
		"ticket_id", t.TicketId, "members", len(t.Members),
		"absent_players", absent, "queued_ms", queuedMs, "via", via)
	return true
}

// ticketAbsentMembers 返回票内命中离场集合的成员列表。
func ticketAbsentMembers(t *matchv1.MatchTicketStorageRecord, offline map[uint64]struct{}) []uint64 {
	var out []uint64
	for _, m := range t.GetMembers() {
		if _, off := offline[m.GetPlayerId()]; off {
			out = append(out, m.GetPlayerId())
		}
	}
	return out
}

// staleTicketWarnAge 成局时最老票据排队时长的告警阈值:超过即打 stale_ticket_matched。
// 正常撮合排队远短于此;命中多半意味着离线回收链路失效(INC-20260814-001 的形状)。
const staleTicketWarnAge = 10 * time.Minute

// oldestTicketAgeMs 返回这批票据里最老一张的排队时长(ms);全部无 enqueued_at_ms
// (滚动升级期旧票)时返回 0,调用方按「无票龄信息」处理,不触发告警。
func oldestTicketAgeMs(nowMs int64, tickets []*matchv1.MatchTicketStorageRecord) int64 {
	var oldestEnqueued int64
	for _, t := range tickets {
		if at := t.GetEnqueuedAtMs(); at > 0 && (oldestEnqueued == 0 || at < oldestEnqueued) {
			oldestEnqueued = at
		}
	}
	if oldestEnqueued == 0 {
		return 0
	}
	return nowMs - oldestEnqueued
}

// ── push 辅助 ─────────────────────────────────────────────────────────────────

// pushProgress 给 members 全体推同一阶段进度(battle 字段为空时不填)。
// mapID 取调用方手头权威记录(ticket/op/match)的 map_id,已终局(FAILED)可为 0。
func (u *MatchUsecase) pushProgress(ctx context.Context, matchID uint64, stage matchv1.MatchStage, members []*matchv1.MatchMemberStorageRecord, dsAddr string, mapID uint32) {
	if u.pusher == nil || len(members) == 0 {
		return
	}
	now := time.Now().UnixMilli()
	for _, m := range members {
		prog := buildProgress(matchID, stage, members, dsAddr, "", mapID)
		u.pushOneProgress(ctx, m.PlayerId, prog, now)
	}
}

// pushQueueingWait 推带 estimated_wait_seconds 的 QUEUEING(容量耗尽静默窗,§6 第 3 项)。
// 句柄仍是 ticket_id;复用 MatchProgress 既有字段,不加 proto(§9.21 加字段留给真实需求)。
func (u *MatchUsecase) pushQueueingWait(ctx context.Context, ticket *matchv1.MatchTicketStorageRecord, waitSec int32) {
	if u.pusher == nil || len(ticket.GetMembers()) == 0 {
		return
	}
	now := time.Now().UnixMilli()
	for _, m := range ticket.GetMembers() {
		prog := buildProgress(ticket.GetTicketId(), stageQueueing, ticket.GetMembers(), "", "", ticket.GetMapId())
		prog.EstimatedWaitSeconds = waitSec
		u.pushOneProgress(ctx, m.GetPlayerId(), prog, now)
	}
}

// pushOne 给单个玩家推 READY 进度(带其专属 battle_ticket)。
func (u *MatchUsecase) pushOne(ctx context.Context, playerID uint64, m *matchv1.MatchStorageRecord, dsAddr, battleTicket string, nowMs int64) {
	if u.pusher == nil {
		return
	}
	prog := buildProgress(m.MatchId, m.Stage, m.Members, dsAddr, battleTicket, m.MapId)
	u.pushOneProgress(ctx, playerID, prog, nowMs)
}

// pushReadyStrict 给全体成员各推一条带其专属 battle_ticket 的 READY 进度并返回聚合错误。
// 与 pushOne(fire-and-forget)不同:READY 是非队长成员进入 Battle 的关键通知,交付失败
// 必须反馈给调用方以保留重试驱动(match 留在 active ZSET),不能静默丢弃。
// pusher 未配置(dev 纯轮询模式)视为无需交付。部分成功也返回错误:下轮对全员重推,
// 已收到的客户端按契约幂等忽略重复(CLAUDE.md §9.19)。
func (u *MatchUsecase) pushReadyStrict(ctx context.Context, m *matchv1.MatchStorageRecord, dsAddr string, tickets map[uint64]string) error {
	if u.pusher == nil {
		return nil
	}
	now := time.Now().UnixMilli()
	var joined error
	for _, member := range m.Members {
		prog := buildProgress(m.MatchId, m.Stage, m.Members, dsAddr, tickets[member.PlayerId], m.MapId)
		event := &matchv1.MatchProgressEvent{Progress: prog, ToPlayerId: member.PlayerId, TsMs: now}
		payload, err := proto.Marshal(event)
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("marshal ready progress for player %d: %w", member.PlayerId, err))
			continue
		}
		// 原则 3 例外:callerID=0 → 发给所有人(含发起方)
		if _, err := u.pusher.PushMatchProgress(ctx, 0, []uint64{member.PlayerId}, payload); err != nil {
			joined = errors.Join(joined, fmt.Errorf("push ready progress to player %d: %w", member.PlayerId, err))
		}
	}
	return joined
}

// finalizeReadyMatch 补推一场 READY 后仍滞留 active ZSET 的 match,交付确认后移出 active。
//
// 不变量:READY ∈ active ZSET ⟺ READY 推送交付未确认。滞留只有两种来源:
//   - READY CAS 提交后、推送完成 / removeActive 前进程崩溃(重启补推,可能对部分成员
//     重复,客户端契约要求容忍重复回调);
//   - 推送时 Kafka 不可用(pushReadyStrict 失败保留 active,本函数每 tick 重试直到恢复)。
//
// 每次补推为全员重签票据(新 jti),与 GetMatchProgress 的 refreshBattleTicket 同口径,
// 不复用旧票撞 DS 侧 jti 一次性防重放。推送成功前绝不 RemoveActive;错误由调用方聚合
// 记日志,match TTL 是重试的自然上限(记录消失 → advanceAllocationsOnce 清索引)。
func (u *MatchUsecase) finalizeReadyMatch(ctx context.Context, m *matchv1.MatchStorageRecord) error {
	if u.pusher == nil {
		return u.repo.RemoveActive(ctx, m.GetMatchId())
	}
	allocation, ok := allocationFromMatch(m)
	if !ok || m.GetBattleDsAddr() == "" {
		return errcode.New(errcode.ErrUnavailable,
			"match %d READY without complete persisted battle target", m.GetMatchId())
	}
	playerIDs := memberPlayerIDs(m.GetMembers())
	tickets, err := u.allocator.SignBattleTickets(ctx, m.GetMatchId(), playerIDs, allocation)
	if err != nil {
		return err
	}
	if err := validateSignedBattleTickets(playerIDs, tickets); err != nil {
		return err
	}
	if err := u.pushReadyStrict(ctx, m, m.GetBattleDsAddr(), tickets); err != nil {
		return err
	}
	return u.repo.RemoveActive(ctx, m.GetMatchId())
}

func (u *MatchUsecase) pushOneProgress(ctx context.Context, playerID uint64, prog *matchv1.MatchProgress, nowMs int64) {
	event := &matchv1.MatchProgressEvent{
		Progress:   prog,
		ToPlayerId: playerID,
		TsMs:       nowMs,
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		plog.With(ctx).Warnw("msg", "match_push_marshal_failed", "err", err, "to_player_id", playerID)
		return
	}
	// 原则 3 例外:callerID=0 → 发给所有人(含发起方)
	if _, err := u.pusher.PushMatchProgress(ctx, 0, []uint64{playerID}, payload); err != nil {
		plog.With(ctx).Warnw("msg", "match_push_failed", "to_player_id", playerID, "err", err)
	}
}

// rollbackClaims 释放一批玩家的队列归属(SETNX 回滚)。CAS 删:仅当 claim 仍指向本票据
// (ticketID)才删,防在「旧 claim 过期 → 同一玩家新一局 claim 写入」窗口误删新 claim。
func (u *MatchUsecase) rollbackClaims(ctx context.Context, ticketID uint64, playerIDs []uint64) {
	for _, pid := range playerIDs {
		if err := u.repo.DeletePlayerIndexIfMatches(ctx, pid, ticketID); err != nil {
			plog.With(ctx).Warnw("msg", "rollback_claim_failed", "player_id", pid, "ticket_id", ticketID, "err", err)
		}
	}
}

// rosterLockLeaseMs 是组票 roster 租约时长(team 侧会钳到 [2s,15s])。
//
// 只需覆盖「BeginTeamMatch 返回 → 本次 StartMatch 把 ClaimPlayer 落地」这一小段:
// 此后对局占用由 matchmaker 自己的 player→ticket claim 负责,不再依赖这把锁。
// 取 5s 是保守值(同机房 RPC + Redis 写通常 <50ms),给 GC / 抖动留两个数量级余量;
// 租约到期自净,取大一点的代价只是「摘人多等一会儿」,取小了才会真漏窗口。
const rosterLockLeaseMs = 5_000

// rosterLockOperationID 生成本次组票的稳定 operation id。
//
// 同一个 (team, captain) 的并发/重试拿到同一个 id,于是 team 侧按幂等续租而不是判冲突
// (§9.23:响应丢失后的重试必须继续同一个 operation,不能竞争创建第二个)。
// 不掺时间戳正是为此 —— 掺了就会让自己的重试变成"另一次组票"而互相顶掉。
func rosterLockOperationID(teamID, captainID uint64) string {
	return "startmatch:" + strconv.FormatUint(teamID, 10) + ":" + strconv.FormatUint(captainID, 10)
}
