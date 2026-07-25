// Package biz 是 ds_allocator 服务的业务逻辑层(W4 ②,2026-06-06)。
//
// 职责(docs/design/go-services.md §2.11):战斗 DS 调度。
//   - AllocateBattle:matchmaker 全员确认后调,申请战斗 DS pod → 写 Redis 镜像 → 回 ds_addr
//   - ReleaseBattle:对局结束/异常,回收 DS pod + 删镜像
//   - Heartbeat:DS 每 5s 主动上报(单向 unary,架构决策 2026-06-03),刷新 last_heartbeat_ms
//   - ListBattles:运维/调试查询当前战斗实例
//   - RunHeartbeatSweep:后台扫描 active ZSET,15s 没心跳 → 标记 abandoned + 回收(不变量 §4)
//   - 空场兜底:Heartbeat 内检测对局活跃但 player_count==0 持续超 EmptyBattleTimeout →
//     标记 abandoned + 回收(全员掉线未归/客户端从未连入,防 DS 空转;2026-07-06)
//
// 关键不变量:
//   - AllocateBattle 幂等(同 match_id 已有镜像 → 直接回已分配地址,不重复 Allocate)
//   - 心跳超时 → abandoned + 发 ds.lifecycle 补偿事件;投递成功才移出 active,
//     失败保留在 active 下一轮重试(W4 ⑧ 可靠补偿,不变量 §4)
package biz

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/battleabort"
	"github.com/luyuancpp/pandora/pkg/dsmetadata"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	"github.com/luyuancpp/pandora/pkg/placement"
	"github.com/luyuancpp/pandora/pkg/releasetrack"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"

	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/ds_allocator/internal/data"
)

// errHeartbeatTerminal 是 Heartbeat 在镜像已是终态(ended/abandoned)时,
// 从乐观锁回调里返回的哨兵错误:中止写回(不刷新 LastHeartbeatMs / TTL / active score),
// 由 Heartbeat 捕获后转成 stop 指令。保证 abandoned 后 DS 继续心跳不会推迟补偿重试、
// 不会刷新 BattleTTL 上界(W4 ⑧ Codex 复审 P1)。
var errHeartbeatTerminal = errors.New("heartbeat on terminal battle")

// errHeartbeatPodMismatch:Heartbeat 上报的 DsPodName 与镜像里记录的不一致(旧 DS / 孤儿 DS /
// 重分配后残留的上一个 pod)。从乐观锁回调返回此哨兵 → 不写回该镜像,并令上报方停机,
// 避免污染新对局的状态(LastHeartbeatMs / state / player_count)。
var errHeartbeatPodMismatch = errors.New("heartbeat pod mismatch")

// errHeartbeatAllocationFenced 表示该 match 正处于 GSA 结果未知或外部 release
// 尚未确认的永久墓碑。即使同版本副本仍跑 legacy 配置，也必须零写入返回 stop，
// 不能让旧 DS 心跳把 state 改回 running 并恢复 TTL。
var errHeartbeatAllocationFenced = errors.New("heartbeat on fenced allocation")

// errReadyWaitTimeout:AllocateBattle 等待 DS ready 心跳超时的哨兵,由 waitBattleReady 返回,
// 调用方据此走回收 pod + 删镜像 + 返回 ErrDSAllocationFailed 的清理路径。
var errReadyWaitTimeout = errors.New("ready wait timeout")

// 战斗 DS 状态常量(对应 proto string state 字段)。
const (
	stateAllocating            = "allocating"
	stateAllocationUncertain   = data.BattleStateAllocationUncertain
	stateAllocationReconciling = data.BattleStateAllocationReconcileReleasePending
	stateAllocationEmptyFence  = data.BattleStateAllocationReconcileEmptyTombstone
	statePreactiveReleasing    = data.BattleStatePreactiveReleasePending
	stateAllocationAbort       = data.BattleStateAllocationAbortPending
	stateWarming               = "warming"
	stateReady                 = "ready"
	stateRunning               = "running"
	stateEnded                 = "ended"
	stateAbandoned             = "abandoned"
)

// Heartbeat 响应控制指令常量。
const (
	commandNone = ""
	commandStop = "stop" // 通知孤儿 DS(无对应镜像)自行停机
)

// 乐观锁重试次数(心跳/状态更新冲突)。
const updateMaxRetry = 3

// readyPollInterval 是 AllocateBattle 等待 DS ready 心跳时轮询 Redis 镜像的间隔。
// 1s 足够:DS 心跳 5s 一跳,ready 等待窗口 10s,1s 轮询既不漏判也不给 Redis 添压。
// 用 var 而非 const,便于单测把它调小以避免慢测(见 allocator_test.go init)。
var readyPollInterval = 1 * time.Second

// activeIndexReconcileInterval bounds the cost of rebuilding the derived
// active ZSET from canonical battle records. The first sweep always runs it;
// later sweeps repeat it so a standalone ZSET write loss cannot strand a
// permanent recovery tombstone forever.
var activeIndexReconcileInterval = 30 * time.Second

// detachedCleanupTimeout 是 ready 等待失败后回收 pod + 删镜像的独立 ctx 预算。
// ready 等待失败的常见原因正是入站 ctx 被取消/超时,复用它做 Release/DeleteBattle 会立刻
// 失败,留下 warming 镜像 + 已分配 pod 泄漏;故清理用一个与入站 ctx 解耦的短超时 ctx。
const detachedCleanupTimeout = 5 * time.Second

// locationRefreshTimeout 是心跳后异步续期玩家 BATTLE 位置的独立 ctx 预算(断线重连,弱依赖)。
// 短超时防 locator 卡死时后台续期 goroutine 泄漏。
const locationRefreshTimeout = 3 * time.Second

// DSLifecyclePusher 发 pandora.ds.lifecycle 事件(W4 ③,2026-06-06)。
//
// 心跳超时标记 abandoned 后,由它把 DSLifecycleEvent{phase=ABANDONED} 发给 battle_result
// 做玩家段位回滚补偿(不变量 §4 DS 崩溃必有补偿)。
//
// W4 ⑧:投递失败不再静默丢——sweepOnce 把对局保留在 active ZSET,下一轮 sweep 重试,
// 直到投递成功或镜像 TTL 过期;配合 battle_result 幂等消费构成 at-least-once 闭环。
// 实现可在内部失败时返回 error(由 sweepOnce 触发重试)。
type DSLifecyclePusher interface {
	PublishLifecycle(ctx context.Context, evt *dsv1.DSLifecycleEvent) error
}

// LocationRefresher 续期玩家 BATTLE 位置 TTL(断线重连,docs/design/battle-reconnect.md §2.2)。
//
// 心跳成功且对局处于 ready/running 时,ds_allocator 用它把该对局玩家的位置刷新为 BATTLE
// (同 match_id 续期,BATTLE→BATTLE),使玩家整局在线期间 login 都能检测到"在战斗中",
// 从而支持中途掉线重登直连回原 battle DS。
//
// 由 player_locator gRPC 客户端实现;可为 nil(未配 locator_addr → 不续期,弱依赖,
// 不影响心跳 / 对局,仅长对局中途重登可能因位置过期退化为回大厅)。
type LocationRefresher interface {
	RefreshBattleLocations(ctx context.Context, playerIDs []uint64, matchID uint64, dsAddr string) error
}

// AllocatorUsecase 是 ds_allocator 业务逻辑核心。
type AllocatorUsecase struct {
	repo      data.BattleRepo
	alloc     GameServerAllocator
	cfg       conf.AllocatorConf
	lifecycle DSLifecyclePusher // 可为 nil；仅显式 local/off 开发配置允许 best-effort 降级
	// lifecycleRequired 在 Redis authority / 生产 enforce 路径为 true。即便启动装配
	// 被未来改坏，nil publisher 也只能保留 active outbox 重试，绝不能把 abandoned
	// 当作已恢复并 Expire 掉 Battle fence。
	lifecycleRequired bool

	// ownerLease / ownerLeaseRequired:owner 权威实例租约双写(owner-authority.md
	// migrate ⑥;nil = 未启用;required 语义见 biz/owner_lease.go renewOwnerLeaseGate)。
	ownerLease         OwnerLeaseRenewer
	ownerLeaseRequired bool

	// ownerAuth / ownerAdmitted:owner 迁移弱依赖调用面 + census 已准入缓存
	// (owner-authority.md migrate ②/③;见 biz/owner_authority.go)。
	ownerAuth     OwnerAuthority
	ownerAdmitted sync.Map
	locator       LocationRefresher // 可为 nil(未配 locator_addr 时不续期 BATTLE 位置)

	// Model B 仅在 agones+enforce+authority_mode=redis 时由 main 注入。Redis authRepo 是
	// 唯一授权权威；K8s annotation 只投递 pending 凭据。
	authRepo                 data.BattleAuthRepo
	abortRepo                data.BattleAllocationAbortRepo
	lifecycleProofRepo       data.BattleAllocationLifecycleRepo
	authoritativeAlloc       AuthoritativeGameServerAllocator
	dsSigner                 BattleCredentialSigner
	dsCredentialTTL          time.Duration
	modelB                   bool
	releasePolicy            releasetrack.Policy
	activeIndexReconciler    data.BattleActiveIndexReconciler
	lastActiveIndexReconcile time.Time

	// killOrphanOnStop:心跳判定某 DS 该停机(orphan / pod_mismatch / 终态)时,是否由后端主动
	// 回收该 pod。local 模式打开——本机 UE DS 没有 Agones,收到 stop 指令不会自杀,残留进程会
	// 幽灵般占着监听端口污染下一局;打开后 stop 时异步调 alloc.Release kill 掉它。Agones 模式默认
	// 关闭:孤儿 GameServer 由 Agones 生命周期回收,避免 Redis 抖动误判 orphan 时误删正常 pod。
	killOrphanOnStop bool
}

// BattleCredentialSigner 把 Allocator 可见能力收窄为 DS callback 凭据签发；生产实现是
// *auth.DSCallbackSigner，无法从该字段调用玩家 Session/DSTicket 签发方法。
type BattleCredentialSigner interface {
	SignBattleCredential(matchID uint64, pod, instanceUID string, epoch uint32, gen uint64, jti string, ttl time.Duration) (auth.HubCredentialResult, error)
}

// NewAllocatorUsecase 构造 AllocatorUsecase。
func NewAllocatorUsecase(repo data.BattleRepo, alloc GameServerAllocator, cfg conf.AllocatorConf) *AllocatorUsecase {
	u := &AllocatorUsecase{repo: repo, alloc: alloc, cfg: cfg}
	if reconciler, ok := repo.(data.BattleActiveIndexReconciler); ok {
		u.activeIndexReconciler = reconciler
	}
	return u
}

// SetLifecyclePusher 注入 ds.lifecycle 事件发送器(main 在 Kafka 就绪时调用)。
func (u *AllocatorUsecase) SetLifecyclePusher(p DSLifecyclePusher) { u.lifecycle = p }

// SetLifecyclePusherRequired 把生产发布策略注入业务层。Redis authority 在
// EnableRedisAuthority 内还会无条件打开此门，避免调用方漏配。
func (u *AllocatorUsecase) SetLifecyclePusherRequired(required bool) {
	u.lifecycleRequired = required
}

// ValidateLifecyclePusherReady 是启动装配的最后一道门。配置宣称可靠发布时，只有
// 非 nil publisher 才允许启动 sweep/RPC。
func (u *AllocatorUsecase) ValidateLifecyclePusherReady() error {
	if u.lifecycleRequired && u.lifecycle == nil {
		return errcode.New(errcode.ErrInvalidState,
			"reliable ds.lifecycle publisher is required before allocator startup")
	}
	return nil
}

// SetLocationRefresher 注入 BATTLE 位置续期器(main 在 locator_addr 已配时调用,弱依赖)。
func (u *AllocatorUsecase) SetLocationRefresher(r LocationRefresher) { u.locator = r }

// SetReleaseTrackPolicy 在启动期注入 match 级确定性 cohort 策略。
func (u *AllocatorUsecase) SetReleaseTrackPolicy(p releasetrack.Policy) { u.releasePolicy = p }

// EnableRedisAuthority 打开 Battle Model B。依赖必须一次完整注入；任何缺失都拒绝启动，
// 禁止出现“配置说 redis authority、实际悄悄回退 legacy”的半开启状态。
func (u *AllocatorUsecase) EnableRedisAuthority(
	repo data.BattleAuthRepo,
	signer BattleCredentialSigner,
	tokenTTL time.Duration,
) error {
	a, ok := u.alloc.(AuthoritativeGameServerAllocator)
	abortRepo, abortOK := repo.(data.BattleAllocationAbortRepo)
	lifecycleProofRepo, lifecycleProofOK := repo.(data.BattleAllocationLifecycleRepo)
	battleStrictWriter, battleStrictOK := u.repo.(data.StrictModelBBattleStorage)
	authStrictWriter, authStrictOK := repo.(data.StrictModelBBattleStorage)
	if repo == nil || signer == nil || tokenTTL <= 0 || !ok || !abortOK || !lifecycleProofOK ||
		u.activeIndexReconciler == nil || !battleStrictOK || !authStrictOK {
		return errcode.New(errcode.ErrInvalidState,
			"battle Model B requires auth/abort/lifecycle repo, signer, positive ttl, authoritative Agones allocator, canonical active-index reconciler and strict storage writers")
	}
	// Irreversible and deliberately last among capability checks: after the
	// startup all-master preflight succeeds, no Model-B RPC/worker can become
	// visible until both battle-writing repository views enforce the same
	// continuous storage invariant.
	battleStrictWriter.EnableStrictModelBWrites()
	authStrictWriter.EnableStrictModelBWrites()
	if !battleStrictWriter.StrictModelBWritesEnabled() || !authStrictWriter.StrictModelBWritesEnabled() {
		return errcode.New(errcode.ErrInvalidState,
			"battle Model B strict storage write gate did not activate")
	}
	u.authRepo = repo
	u.abortRepo = abortRepo
	u.lifecycleProofRepo = lifecycleProofRepo
	u.authoritativeAlloc = a
	u.dsSigner = signer
	u.dsCredentialTTL = tokenTTL
	u.modelB = true
	u.lifecycleRequired = true
	return nil
}

// SetKillOrphanOnStop 打开「心跳 stop 时主动回收该 DS」(main 在 mode=local 时调用)。
// 见 killOrphanOnStop 字段说明:local 模式的 UE DS 收到 stop 不自杀,需后端主动 kill 防幽灵占端口。
func (u *AllocatorUsecase) SetKillOrphanOnStop(v bool) { u.killOrphanOnStop = v }

// killStrandedDS 在心跳判定某 DS 该停机时,异步回收其 pod(local 模式防幽灵 DS 占端口)。
// fire-and-forget:请求 ctx 不得逃逸进 goroutine(§16.7),用 plog.Detach(只复制 trace_id
// 等日志字段,满足不变量 §8)+ 短超时,不给心跳响应加尾延迟。
// killOrphanOnStop 关闭(Agones 模式)时为 no-op,pod 回收交 Agones 生命周期。
func (u *AllocatorUsecase) killStrandedDS(ctx context.Context, matchID uint64, podName, reason string) {
	if !u.killOrphanOnStop || podName == "" {
		return
	}
	go func() {
		kctx, cancel := context.WithTimeout(plog.Detach(ctx), detachedCleanupTimeout)
		defer cancel()
		if err := u.alloc.Release(kctx, podName); err != nil {
			plog.With(kctx).Warnw("msg", "kill_stranded_ds_failed", "match_id", matchID, "pod", podName, "reason", reason, "err", err)
			return
		}
		plog.With(kctx).Infow("msg", "kill_stranded_ds", "match_id", matchID, "pod", podName, "reason", reason)
	}()
}

func (u *AllocatorUsecase) battleTTL() time.Duration { return u.cfg.BattleTTL.Std() }

// readyWaitTimeout 是 AllocateBattle 等待 DS ready 心跳的最长时间(默认 10s)。
func (u *AllocatorUsecase) readyWaitTimeout() time.Duration { return u.cfg.ReadyWaitTimeout.Std() }

// ── RPC 1:AllocateBattle ──────────────────────────────────────────────────────

// AllocateResult 是 AllocateBattle 的出参。
//
// GameserverUID / InstanceEpoch / AllocationID(DSTicket v2,方案 B):matchmaker 签 battle 票
// 时把票绑死到唯一 DS 实例。三者与 DSAddr 来自同一权威快照(Redis BattleStorageRecord),
// 不得从其它时点/其它源拼凑,否则地址与票据可能指向不同实例。
type AllocateResult struct {
	DSAddr        string
	DSPodName     string
	AllocatedAtMs int64
	GameserverUID string
	InstanceEpoch uint32
	AllocationID  string
	ReleaseTrack  string
}

func allocateResultFromBattle(b *dsv1.BattleStorageRecord) *AllocateResult {
	if b == nil {
		return nil
	}
	return &AllocateResult{
		DSAddr: b.GetDsAddr(), DSPodName: b.GetDsPodName(), AllocatedAtMs: b.GetAllocatedAtMs(),
		GameserverUID: b.GetGameserverUid(), InstanceEpoch: b.GetInstanceEpoch(),
		AllocationID: b.GetAllocationId(), ReleaseTrack: b.GetReleaseTrack(),
	}
}

// ResolveBattleTarget 是重连重签的只读权威查询。它只读同一 Redis auth+projection
// 快照，不创建 allocation claim、不调用 Agones、不刷新 TTL/heartbeat/index。
func (u *AllocatorUsecase) ResolveBattleTarget(
	ctx context.Context,
	matchID, playerID uint64,
) (*AllocateResult, error) {
	if matchID == 0 || playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "match_id and player_id required")
	}
	if u == nil || !u.modelB || u.authRepo == nil {
		return nil, errcode.New(errcode.ErrUnavailable, "battle read-only authority unavailable")
	}
	snapshot, err := u.authRepo.ReadAuthority(ctx, matchID)
	if err != nil {
		return nil, err
	}
	ready, reason := snapshot.ReadyAuthorized(
		time.Now().UnixMilli(), u.cfg.HeartbeatTimeout.Std().Milliseconds())
	battle := snapshot.Battle
	if !ready || battle == nil || !slices.Contains(battle.GetPlayerIds(), playerID) {
		return nil, errcode.New(errcode.ErrPermissionDeny,
			"battle target not authorized for reconnect (reason=%s)", reason)
	}
	if battle.GetDsAddr() == "" || battle.GetDsPodName() == "" || battle.GetGameserverUid() == "" ||
		battle.GetInstanceEpoch() == 0 || battle.GetAllocationId() == "" ||
		(battle.GetReleaseTrack() != auth.ReleaseTrackStable && battle.GetReleaseTrack() != auth.ReleaseTrackCanary) {
		return nil, errcode.New(errcode.ErrUnavailable, "battle target projection incomplete")
	}
	return allocateResultFromBattle(battle), nil
}

// AllocateBattle 为 match 申请战斗 DS。
//
// 关键:Agones Allocated(pod 被分配)≠ 战斗 DS Ready。DS 进程要先读到 pandora.dev/match-id
// 才能在 PreLogin 放行客户端票据。所以这里不再一拿到 pod 就回 ds_addr,而是:
//
//	Allocate → CreateBattle(state=warming) → 轮询等 DS Heartbeat 上报正确 match_id/pod 且
//	进入 ready/running → 回 ds_addr;ReadyWaitTimeout 内没等到 → 回收 pod + 删镜像 + 分配失败。
//
// 用 Redis 镜像轮询(而非内存 channel):Heartbeat RPC 可能落到另一个 ds_allocator pod,
// 只有共享的 Redis 镜像能跨 pod 观察到 DS 的就绪心跳。
//
// 幂等:同 match_id 已有镜像时——ready/running 且有有效心跳 → 直接回;warming → 继续等 ready;
// 终态/不可用 → 返回分配失败(绝不把 ds_addr 回给 matchmaker)。
func (u *AllocatorUsecase) AllocateBattle(ctx context.Context, matchID uint64, playerIDs []uint64, mapID uint32, gameMode string) (*AllocateResult, error) {
	return u.AllocateBattleWithCombatFactions(ctx, matchID, playerIDs, nil, mapID, gameMode)
}

// AllocateBattleWithCombatFactions 为 match 申请战斗 DS，并持久化完整的 match-local
// 玩家阵营快照。空映射仅兼容滚动升级中的旧 matchmaker；非空映射必须精确覆盖 roster。
func (u *AllocatorUsecase) AllocateBattleWithCombatFactions(
	ctx context.Context,
	matchID uint64,
	playerIDs []uint64,
	combatFactionByPlayer map[uint64]uint32,
	mapID uint32,
	gameMode string,
) (*AllocateResult, error) {
	if matchID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	canonicalPlayers, _, rosterErr := dsmetadata.CanonicalRoster(playerIDs)
	if rosterErr != nil {
		return nil, errcode.New(errcode.ErrInvalidArg, "invalid battle roster: %v", rosterErr)
	}
	playerIDs = canonicalPlayers
	var combatFactions []*dsv1.BattlePlayerCombatFaction
	if len(combatFactionByPlayer) > 0 {
		canonicalFactionPlayers, _, factionErr := dsmetadata.CanonicalCombatFactions(
			playerIDs, combatFactionByPlayer)
		if factionErr != nil {
			return nil, errcode.New(errcode.ErrInvalidArg, "invalid battle combat factions: %v", factionErr)
		}
		playerIDs = canonicalFactionPlayers
		combatFactions = combatFactionRecords(playerIDs, combatFactionByPlayer)
		// 脱离调用方 map，避免 RPC 返回前外部复用/修改 map 造成 TOCTOU 或数据竞争。
		combatFactionByPlayer = make(map[uint64]uint32, len(combatFactions))
		for _, record := range combatFactions {
			combatFactionByPlayer[record.GetPlayerId()] = record.GetCombatFactionId()
		}
	} else {
		plog.With(ctx).Warnw("msg", "battle_combat_factions_legacy_missing", "match_id", matchID)
	}
	desiredReleaseTrack := u.releasePolicy.Select(matchID)

	// 单 key SET NX claim 是并发 AllocateBattle 的线性化点。只有持有本次 allocation_id
	// 的赢家才允许调用外部 Agones；输家只观察同一权威记录，绝不再分配第二个 Pod。
	claimAt := time.Now().UnixMilli()
	allocationID := uuid.NewString()
	claim := &dsv1.BattleStorageRecord{
		MatchId:              matchID,
		State:                stateAllocating,
		PlayerIds:            append([]uint64(nil), playerIDs...),
		MapId:                mapID,
		GameMode:             gameMode,
		AllocatedAtMs:        claimAt,
		LastHeartbeatMs:      claimAt,
		PlayerCount:          int32(len(playerIDs)),
		AllocationId:         allocationID,
		PlayerCombatFactions: cloneCombatFactionRecords(combatFactions),
	}
	claimed, existing, err := u.repo.ClaimBattle(ctx, claim, u.battleTTL())
	if err != nil {
		return nil, err
	}
	if !claimed {
		if !sameBattleAllocationRequest(existing, claim) {
			plog.With(ctx).Errorw("msg", "battle_allocation_idempotency_snapshot_mismatch",
				"match_id", matchID, "allocation_id", existing.GetAllocationId())
			return nil, errcode.New(errcode.ErrUnavailable,
				"battle %d allocation request conflicts with existing snapshot", matchID)
		}
		return u.awaitExistingAllocation(ctx, matchID, existing)
	}

	var authoritative *data.AuthoritativeGameServerAllocation
	var podName, addr string
	actualReleaseTrack := desiredReleaseTrack
	if u.modelB {
		// 外部 GSA POST 前先把 claim CAS 成永久 allocation_uncertain。该 fence 是
		// “是否允许 POST”的唯一线性化点；失败/响应未知时绝不能访问 K8s。
		fenced, fenceErr := u.repo.FenceBattleAllocation(ctx, matchID, allocationID)
		if fenceErr != nil || !fenced {
			plog.With(ctx).Errorw("msg", "gameserver_preallocation_fence_failed",
				"match_id", matchID, "allocation_id", allocationID,
				"fenced", fenced, "err", fenceErr)
			return nil, errcode.New(errcode.ErrUnavailable,
				"battle %d allocation fence unavailable", matchID)
		}
		authoritative, err = u.authoritativeAlloc.AllocateAuthoritative(
			ctx, matchID, allocationID, playerIDs, combatFactionByPlayer, mapID, gameMode, desiredReleaseTrack)
		if authoritative != nil {
			podName, addr = authoritative.PodName, authoritative.Addr
		}
	} else {
		podName, addr, actualReleaseTrack, err = u.alloc.Allocate(ctx, matchID, mapID, gameMode, desiredReleaseTrack)
	}
	if err != nil {
		plog.With(ctx).Errorw("msg", "gameserver_allocate_failed", "match_id", matchID, "err", err)
		if u.modelB {
			// POST transport、响应解析、严格 GET 任一步失败都可能对应“已应用但结果迟到”。
			// 永久 uncertain claim 是唯一安全结果：不自动 Release/Delete、不恢复 TTL，
			// 后续同 match 重试只能 fail-closed，绝不产生第二次 POST。
			plog.With(ctx).Errorw("msg", "gameserver_allocation_uncertain_retained",
				"match_id", matchID, "allocation_id", allocationID, "pod", podName)
			return nil, errcode.New(errcode.ErrUnavailable,
				"battle %d gameserver allocation result uncertain", matchID)
		}
		u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, authoritative)
		return nil, errcode.New(errcode.ErrDSAllocationFailed, "allocate ds for match %d failed", matchID)
	}
	if u.modelB && (authoritative == nil || authoritative.PodName == "" || authoritative.Addr == "" ||
		authoritative.InstanceUID == "" || authoritative.PodUID == "" || authoritative.ResourceVersion == "" ||
		authoritative.AllocationID != allocationID || !releasetrack.Valid(authoritative.ReleaseTrack)) {
		// data 层虽已做严格 GET，这里仍在副作用边界复核完整身份，防错误实现/测试桩
		// 以 nil error 绕过 UID/RV/allocation_id 确认。保持永久 uncertain，不做清理。
		plog.With(ctx).Errorw("msg", "gameserver_authoritative_identity_incomplete",
			"match_id", matchID, "allocation_id", allocationID, "allocation", authoritative)
		return nil, errcode.New(errcode.ErrUnavailable,
			"battle %d gameserver identity unavailable", matchID)
	}

	now := time.Now().UnixMilli()
	if authoritative != nil {
		actualReleaseTrack = authoritative.ReleaseTrack
	}
	if !releasetrack.Valid(actualReleaseTrack) {
		u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, authoritative)
		return nil, errcode.New(errcode.ErrDSAllocationFailed,
			"allocator returned invalid actual release_track %q", actualReleaseTrack)
	}
	battle := &dsv1.BattleStorageRecord{
		MatchId:              matchID,
		DsPodName:            podName,
		DsAddr:               addr,
		State:                stateWarming, // 等 DS 心跳确认 ready 才回 matchmaker;不把 Agones Allocated 当成 ready
		PlayerIds:            playerIDs,
		MapId:                mapID,
		GameMode:             gameMode,
		AllocatedAtMs:        now,
		LastHeartbeatMs:      now, // 仅作 sweep 宽限基准;ready 判定要求 LastHeartbeatMs 严格大于此(即真实心跳)
		PlayerCount:          int32(len(playerIDs)),
		AllocationId:         allocationID,
		ReleaseTrack:         actualReleaseTrack,
		PlayerCombatFactions: cloneCombatFactionRecords(combatFactions),
	}
	if authoritative != nil {
		battle.GameserverUid = authoritative.InstanceUID
		battle.PodUid = authoritative.PodUID
	}
	var finalized bool
	if u.modelB {
		finalized, err = u.repo.FinalizeFencedBattleAllocation(ctx, battle, u.battleTTL())
	} else {
		finalized, err = u.repo.FinalizeBattleAllocation(ctx, battle, u.battleTTL())
	}
	if err != nil || !finalized {
		if u.modelB {
			// UID/RV 已确认但 Redis finalize 失败/响应未知时仍不自动释放：事务可能已把
			// persistent uncertain 成功改成 warming。后续只能由权威读回/审计收敛，
			// 本请求绝不能凭本地结果删除 claim 或触碰 K8s。
			plog.With(ctx).Errorw("msg", "gameserver_fenced_finalize_unavailable",
				"match_id", matchID, "allocation_id", allocationID,
				"pod", podName, "finalized", finalized, "err", err)
			return nil, errcode.New(errcode.ErrUnavailable,
				"battle %d allocation finalize unavailable", matchID)
		}
		// legacy 路径仍可按 allocation_id 清理；它没有 POST 结果未知的 persistent fence。
		u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, authoritative)
		if err != nil {
			return nil, err
		}
		return nil, errcode.New(errcode.ErrDSAllocationFailed,
			"battle %d allocation claim no longer owned", matchID)
	}
	if u.modelB {
		if err := u.provisionBattleCredential(ctx, matchID, allocationID, authoritative); err != nil {
			plog.With(ctx).Errorw("msg", "battle_credential_provision_failed", "match_id", matchID,
				"pod", podName, "uid", authoritative.InstanceUID, "err", err)
			u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, authoritative)
			return nil, errcode.New(errcode.ErrDSAllocationFailed,
				"battle %d credential delivery failed", matchID)
		}
	}

	plog.With(ctx).Debugw("msg", "battle_warming", "match_id", matchID, "pod", podName, "ds_addr", addr, "players", len(playerIDs))

	// 等 DS 用正确 match_id/pod 的心跳上报 ready/running,后端才把 ds_addr 回给 matchmaker。
	res, werr := u.waitBattleReady(ctx, matchID, podName, allocationID)
	if werr != nil {
		if errors.Is(werr, errReadyWaitTimeout) {
			return nil, u.failReadyWaitTimeout(ctx, matchID, allocationID, podName, authoritative, true)
		}
		// 入站 ctx 取消/超时或 repo 出错等非超时失败:本次刚分配的 pod 由本调用持有,
		// 用独立 cleanup ctx 回收 pod + 删 warming 镜像,避免泄漏(入站 ctx 多半已失效)。
		u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, authoritative)
		return nil, werr
	}

	// owner 迁移双写(owner-authority.md migrate ②):READY 交付前逐玩家弱 Begin(BATTLE)。
	// 失败仅告警,分配照常(路由决策不变,行为切换属 contract);预算限界防 owner 卡顿拖慢分配。
	ownerBeginPlayersWeak(ctx, u.ownerAuth, playerIDs, ownerTypeBattle, data.OwnerTargetView{
		PodName:                  res.DSPodName,
		InstanceUID:              res.GameserverUID,
		InstanceEpoch:            res.InstanceEpoch,
		AssignmentOrAllocationID: res.AllocationID,
		ReleaseTrack:             res.ReleaseTrack,
	}, 3*time.Second)

	plog.With(ctx).Debugw("msg", "battle_ready_after_heartbeat", "match_id", matchID, "pod", podName, "ds_addr", addr)
	return res, nil
}

func combatFactionRecords(
	canonicalPlayers []uint64,
	combatFactionByPlayer map[uint64]uint32,
) []*dsv1.BattlePlayerCombatFaction {
	if len(combatFactionByPlayer) == 0 {
		return nil
	}
	records := make([]*dsv1.BattlePlayerCombatFaction, 0, len(canonicalPlayers))
	for _, playerID := range canonicalPlayers {
		records = append(records, &dsv1.BattlePlayerCombatFaction{
			PlayerId: playerID, CombatFactionId: combatFactionByPlayer[playerID],
		})
	}
	return records
}

func cloneCombatFactionRecords(records []*dsv1.BattlePlayerCombatFaction) []*dsv1.BattlePlayerCombatFaction {
	if len(records) == 0 {
		return nil
	}
	out := make([]*dsv1.BattlePlayerCombatFaction, 0, len(records))
	for _, record := range records {
		out = append(out, &dsv1.BattlePlayerCombatFaction{
			PlayerId: record.GetPlayerId(), CombatFactionId: record.GetCombatFactionId(),
		})
	}
	return out
}

func combatFactionMapFromRecords(
	playerIDs []uint64,
	records []*dsv1.BattlePlayerCombatFaction,
) (map[uint64]uint32, error) {
	if len(records) == 0 {
		return nil, nil
	}
	canonicalPlayers, _, err := dsmetadata.CanonicalRoster(playerIDs)
	if err != nil || !slices.Equal(playerIDs, canonicalPlayers) || len(records) != len(playerIDs) {
		return nil, errors.New("combat faction records must align with canonical battle roster")
	}
	factions := make(map[uint64]uint32, len(records))
	for i, record := range records {
		if record == nil || record.GetPlayerId() != playerIDs[i] {
			return nil, errors.New("combat faction records are not in canonical roster order")
		}
		if record.GetCombatFactionId() > dsmetadata.MaxCombatFactionID {
			return nil, errors.New("combat faction_id exceeds DS camp range")
		}
		factions[record.GetPlayerId()] = record.GetCombatFactionId()
	}
	if _, _, err := dsmetadata.CanonicalCombatFactions(canonicalPlayers, factions); err != nil {
		return nil, err
	}
	return factions, nil
}

func sameBattleAllocationRequest(existing, expected *dsv1.BattleStorageRecord) bool {
	if existing == nil || expected == nil || existing.GetMatchId() != expected.GetMatchId() ||
		existing.GetMapId() != expected.GetMapId() || existing.GetGameMode() != expected.GetGameMode() ||
		!slices.Equal(existing.GetPlayerIds(), expected.GetPlayerIds()) {
		return false
	}
	a, b := existing.GetPlayerCombatFactions(), expected.GetPlayerCombatFactions()
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].GetPlayerId() != b[i].GetPlayerId() ||
			a[i].GetCombatFactionId() != b[i].GetCombatFactionId() {
			return false
		}
	}
	return true
}

const (
	battleTokenAnnotationKey       = "pandora.dev/ds-token"
	battleTokenExpAnnotationKey    = "pandora.dev/ds-token-exp-ms"
	battleTokenGenAnnotationKey    = "pandora.dev/ds-token-gen"
	battleTokenJTIAnnotationKey    = "pandora.dev/ds-token-jti"
	battleInstanceUIDAnnotationKey = "pandora.dev/ds-instance-uid"
	battleInstanceEpochKey         = "pandora.dev/ds-instance-epoch"
	battleWriterEpochKey           = "pandora.dev/ds-writer-epoch"
	battleTokenKidKey              = "pandora.dev/ds-token-kid"
	battleTokenHashKey             = "pandora.dev/ds-token-sha256"
)

// provisionBattleCredential 完成 Redis stage→K8s 条件投递→Redis delivered CAS。任何半失败
// 都不会产生 active；cleanup 只按 expected allocation_id 撤销本轮 warming 实例。
func (u *AllocatorUsecase) provisionBattleCredential(
	ctx context.Context,
	matchID uint64,
	allocationID string,
	allocation *data.AuthoritativeGameServerAllocation,
) error {
	if allocation == nil {
		return errcode.New(errcode.ErrInvalidState, "missing authoritative gameserver allocation")
	}
	seed, err := u.authRepo.PrepareCredential(ctx, data.BattleAuthorityBinding{
		MatchID:             matchID,
		AllocationID:        allocationID,
		PodName:             allocation.PodName,
		InstanceUID:         allocation.InstanceUID,
		RequiredWriterEpoch: data.BattleDSWriterEpochV2,
		AuthTTL:             u.dsCredentialTTL,
		BattleTTL:           u.battleTTL(),
	})
	if err != nil {
		return err
	}
	allocation.InstanceEpoch = seed.InstanceEpoch
	jti := uuid.NewString()
	signed, err := u.dsSigner.SignBattleCredential(
		matchID, allocation.PodName, allocation.InstanceUID,
		seed.InstanceEpoch, seed.Gen, jti, u.dsCredentialTTL)
	if err != nil {
		return err
	}
	if signed.ExpMs <= 0 {
		return errcode.New(errcode.ErrInvalidState, "battle credential signer returned invalid exp")
	}
	credential := &dsv1.BattleDSCredential{
		Gen:           seed.Gen,
		Jti:           jti,
		ExpMs:         uint64(signed.ExpMs),
		Kid:           signed.Kid,
		InstanceUid:   allocation.InstanceUID,
		InstanceEpoch: seed.InstanceEpoch,
		TokenSha256:   signed.TokenSHA256,
		WriterEpoch:   signed.WriterEpoch,
	}
	if _, err := u.authRepo.StagePending(ctx, data.BattleStageInput{
		MatchID: matchID, AllocationID: allocationID,
		Credential: credential, AuthTTL: u.dsCredentialTTL,
	}); err != nil {
		return err
	}
	annotations := map[string]string{
		battleTokenAnnotationKey:       signed.Token,
		battleTokenExpAnnotationKey:    strconv.FormatUint(credential.ExpMs, 10),
		battleTokenGenAnnotationKey:    strconv.FormatUint(credential.Gen, 10),
		battleTokenJTIAnnotationKey:    credential.Jti,
		battleInstanceUIDAnnotationKey: credential.InstanceUid,
		battleInstanceEpochKey:         strconv.FormatUint(uint64(credential.InstanceEpoch), 10),
		battleWriterEpochKey:           strconv.FormatUint(uint64(credential.WriterEpoch), 10),
		battleTokenKidKey:              credential.Kid,
		battleTokenHashKey:             credential.TokenSha256,
	}
	rv, err := u.authoritativeAlloc.DeliverCredential(ctx, allocation, annotations)
	if err != nil {
		return err
	}
	if err := u.authRepo.MarkDelivered(
		ctx, matchID, allocationID, credential, rv, u.dsCredentialTTL); err != nil {
		// Redis 响应不确定时只认权威 read-back；不以本地 expected 或 K8s 镜像猜测成功。
		snapshot, readErr := u.authRepo.ReadAuthority(ctx, matchID)
		if readErr != nil || !battlePendingDelivered(snapshot, allocationID, credential, rv) {
			return err
		}
	}
	return nil
}

func battlePendingDelivered(
	snapshot data.BattleAuthoritySnapshot,
	allocationID string,
	expected *dsv1.BattleDSCredential,
	rv string,
) bool {
	a := snapshot.Auth
	p := a.GetPending()
	return snapshot.AuthFound && a.GetAllocationId() == allocationID && a.GetDeliveredRv() == rv &&
		p.GetGen() == expected.GetGen() && p.GetJti() == expected.GetJti() &&
		p.GetExpMs() == expected.GetExpMs() && p.GetKid() == expected.GetKid() &&
		p.GetInstanceUid() == expected.GetInstanceUid() && p.GetInstanceEpoch() == expected.GetInstanceEpoch() &&
		p.GetTokenSha256() == expected.GetTokenSha256() && p.GetWriterEpoch() == expected.GetWriterEpoch()
}

// awaitExistingAllocation 处理 claim 输家/幂等重试。调用方不拥有 allocation_id，故等待
// 失败时只返回错误，绝不清理记录或释放 Pod；资源生命周期只由 claim 赢家或 sweep 管理。
func (u *AllocatorUsecase) awaitExistingAllocation(
	ctx context.Context,
	matchID uint64,
	existing *dsv1.BattleStorageRecord,
) (*AllocateResult, error) {
	if existing == nil {
		return nil, errcode.New(errcode.ErrDSAllocationFailed, "battle %d allocation claim missing", matchID)
	}
	if existing.State == stateAllocationUncertain || existing.State == stateAllocationReconciling {
		// 该状态表示 GSA POST 可能迟到应用。调用方不等待、不清理、不查删 K8s；
		// 只返回暂不可用，永久 claim 继续阻止同 match 第二次 POST。
		plog.With(ctx).Warnw("msg", "allocate_idempotent_uncertain",
			"match_id", matchID, "allocation_id", existing.AllocationId)
		return nil, errcode.New(errcode.ErrUnavailable,
			"battle %d allocation result requires explicit reconciliation", matchID)
	}
	if existing.State == statePreactiveReleasing || existing.State == stateAllocationAbort {
		// 外部 UID 条件删除尚未得到明确成功；永久 release fence 必须继续
		// 阻止本请求发第二次 GSA POST。回收可由幂等 sweep 重试，但安全不依赖重试。
		return nil, errcode.New(errcode.ErrUnavailable,
			"battle %d preactive gameserver release is not confirmed", matchID)
	}
	if !u.modelB && battleReadyForPod(existing, existing.DsPodName, matchID, existing.AllocatedAtMs) {
		plog.With(ctx).Debugw("msg", "allocate_idempotent_hit", "match_id", matchID,
			"ds_addr", existing.DsAddr, "state", existing.State, "allocation_id", existing.AllocationId)
		return allocateResultFromBattle(existing), nil
	}
	if existing.State != stateAllocating && existing.State != stateWarming &&
		(!u.modelB || (existing.State != stateReady && existing.State != stateRunning)) {
		plog.With(ctx).Warnw("msg", "allocate_idempotent_unusable", "match_id", matchID, "state", existing.State)
		return nil, errcode.New(errcode.ErrDSAllocationFailed,
			"battle %d in state %s, not allocatable", matchID, existing.State)
	}
	plog.With(ctx).Debugw("msg", "allocate_idempotent_wait", "match_id", matchID,
		"pod", existing.DsPodName, "state", existing.State, "allocation_id", existing.AllocationId)
	res, err := u.waitBattleReady(ctx, matchID, existing.DsPodName, existing.AllocationId)
	if err != nil {
		if errors.Is(err, errReadyWaitTimeout) {
			return nil, u.failReadyWaitTimeout(ctx, matchID, existing.AllocationId, existing.DsPodName, nil, false)
		}
		return nil, err
	}
	return res, nil
}

// battleReadyForPod 判定 DS 是否已用 Heartbeat 确认 ready:pod/match 对得上、有分配后的真实心跳
// (LastHeartbeatMs 严格大于 allocatedAtMs)、状态进入 ready 或 running。
// 当前 UE 侧上报的是 running(不一定先发 ready),所以后端先把 running 也视为可进入状态。
func battleReadyForPod(b *dsv1.BattleStorageRecord, podName string, matchID uint64, allocatedAtMs int64) bool {
	return b != nil &&
		b.MatchId == matchID &&
		b.DsPodName == podName &&
		b.LastHeartbeatMs > allocatedAtMs &&
		(b.State == stateReady || b.State == stateRunning)
}

// waitBattleReady 轮询 Redis 镜像直到 DS 心跳确认 ready,或 ReadyWaitTimeout 超时(返回 errReadyWaitTimeout)。
func (u *AllocatorUsecase) waitBattleReady(ctx context.Context, matchID uint64, podName, allocationID string) (*AllocateResult, error) {
	deadline := time.Now().Add(u.readyWaitTimeout())
	ticker := time.NewTicker(readyPollInterval)
	defer ticker.Stop()
	for {
		if u.modelB {
			snapshot, err := u.authRepo.ReadAuthority(ctx, matchID)
			if err != nil {
				return nil, err
			}
			b := snapshot.Battle
			if snapshot.BattleFound && allocationID != "" && b.AllocationId != allocationID {
				return nil, errcode.New(errcode.ErrDSAllocationFailed,
					"battle %d allocation superseded", matchID)
			}
			ready, _ := snapshot.ReadyAuthorized(
				time.Now().UnixMilli(), u.cfg.HeartbeatTimeout.Std().Milliseconds())
			if ready && (podName == "" || b.DsPodName == podName) {
				return allocateResultFromBattle(b), nil
			}
		} else {
			b, found, err := u.repo.GetBattle(ctx, matchID)
			if err != nil {
				return nil, err
			}
			if found && allocationID != "" && b.AllocationId != allocationID {
				return nil, errcode.New(errcode.ErrDSAllocationFailed,
					"battle %d allocation superseded", matchID)
			}
			if found && (podName == "" || b.DsPodName == podName) &&
				battleReadyForPod(b, b.DsPodName, matchID, b.AllocatedAtMs) {
				return allocateResultFromBattle(b), nil
			}
		}
		if !time.Now().Before(deadline) {
			return nil, errReadyWaitTimeout
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}

// failReadyWaitTimeout 处理 ready 等待超时:回收 pod + 删镜像,返回 ErrDSAllocationFailed
// (绝不把 ds_addr 回给 matchmaker,否则客户端连上 match_id 仍为 0 的 DS 会被 PreLogin 拒)。
func (u *AllocatorUsecase) failReadyWaitTimeout(
	ctx context.Context,
	matchID uint64,
	allocationID, podName string,
	allocation *data.AuthoritativeGameServerAllocation,
	owner bool,
) error {
	plog.With(ctx).Warnw("msg", "battle_ready_wait_timeout", "match_id", matchID, "pod", podName)
	if owner {
		u.cleanupAllocatedBattle(ctx, matchID, allocationID, podName, allocation)
	}
	return errcode.New(errcode.ErrDSAllocationFailed, "battle %d ds not ready within wait timeout", matchID)
}

// cleanupAllocatedBattle 用与入站 ctx 解耦的独立 ctx 执行
// 永久 release fence → UID 条件回收 → 明确成功后 purge。
// 入站 ctx 在 ready 等待失败时多半已被取消/超时,直接复用它做 Release/DeleteBattle 会立刻
// 失败,从而留下 warming 镜像 + 已分配 pod 泄漏;故这里用 plog.Detach 出一个短超时 ctx
// 兜底回收(§16.7:detached 路径只复制日志字段,不携带请求级 transport)。
func (u *AllocatorUsecase) cleanupAllocatedBattle(
	ctx context.Context,
	matchID uint64,
	allocationID, podName string,
	allocation *data.AuthoritativeGameServerAllocation,
) {
	cleanupCtx, cancel := context.WithTimeout(plog.Detach(ctx), detachedCleanupTimeout)
	defer cancel()
	if u.modelB {
		// 先把 auth+battle 同槽锁成永久 pre-active release fence，再触碰 K8s。
		// 旧实现先 DeleteExpected 后 ReleaseExpected：DELETE 超时会留下“Redis 已无 claim、
		// GSA 仍可能活着”的窗口，下一请求可对同 match 发第二次 POST。
		expected := data.BattleExpectedInstance{AllocationID: allocationID}
		if allocation != nil {
			expected.InstanceUID = allocation.InstanceUID
			expected.InstanceEpoch = allocation.InstanceEpoch
		}
		fenced, ferr := u.authRepo.FencePreactiveReleaseExpected(cleanupCtx, matchID, expected)
		if ferr != nil || !fenced {
			plog.With(ctx).Warnw("msg", "ready_wait_cleanup_fence_failed", "match_id", matchID,
				"allocation_id", allocationID, "fenced", fenced, "err", ferr)
			return
		}
		if podName == "" || allocation == nil {
			plog.With(ctx).Warnw("msg", "ready_wait_cleanup_identity_missing", "match_id", matchID,
				"allocation_id", allocationID)
			return
		}
		if rerr := u.releaseFencedPreactiveGameServer(cleanupCtx, matchID, podName, allocation); rerr != nil {
			// ReleaseExpected timeout/unknown 必须保留永久 fence；不得 purge。
			plog.With(ctx).Warnw("msg", "ready_wait_cleanup_release_unconfirmed", "match_id", matchID,
				"pod", podName, "err", rerr)
			return
		}
		purged, perr := u.authRepo.PurgePreactiveReleasedExpected(cleanupCtx, matchID, expected)
		if perr != nil || !purged {
			plog.With(ctx).Warnw("msg", "ready_wait_cleanup_purge_failed", "match_id", matchID,
				"allocation_id", allocationID, "purged", purged, "err", perr)
		}
		return
	}

	deleted, derr := u.repo.DeleteBattleIfAllocationMatches(cleanupCtx, matchID, allocationID, podName)
	if derr != nil {
		plog.With(ctx).Warnw("msg", "ready_wait_cleanup_delete_failed", "match_id", matchID, "err", derr)
	}
	if !deleted || podName == "" {
		return
	}
	if rerr := u.releaseGameServer(cleanupCtx, matchID, podName, allocation); rerr != nil {
		plog.With(ctx).Warnw("msg", "ready_wait_cleanup_release_failed", "match_id", matchID, "pod", podName, "err", rerr)
	}
}

func (u *AllocatorUsecase) releaseGameServer(
	ctx context.Context,
	matchID uint64,
	podName string,
	allocation *data.AuthoritativeGameServerAllocation,
) error {
	if !u.modelB {
		return u.alloc.Release(ctx, podName)
	}
	if matchID == 0 || allocation == nil || allocation.InstanceUID == "" ||
		allocation.InstanceEpoch == 0 || allocation.AllocationID == "" || podName == "" ||
		allocation.PodName != podName {
		return errcode.New(errcode.ErrInvalidState,
			"battle Model B release requires complete expected GameServer tuple")
	}
	if allocation.PodUID == "" {
		podUID, err := u.ensureDurableReleasePodUID(ctx, matchID, podName,
			data.BattleExpectedInstance{
				AllocationID: allocation.AllocationID, InstanceUID: allocation.InstanceUID,
				InstanceEpoch: allocation.InstanceEpoch,
			}, allocation.ReleaseTrack)
		if err != nil {
			return err
		}
		allocation.PodUID = podUID
	}
	if allocation.PodUID == "" {
		return errcode.New(errcode.ErrInvalidState,
			"battle Model B release requires durable expected Pod UID")
	}
	if err := u.authoritativeAlloc.ReleaseExpected(ctx, allocation); err != nil {
		return err
	}
	// 外部 UID 条件回收明确成功后先写 durable teardown proof，再允许
	// 上层 purge/expire battle+auth。proof 写失败必须整体返错保留永久
	// release fence；后续重试 ReleaseExpected(404 幂等成功)可补齐证明。
	return u.repo.RecordInstanceTeardown(ctx, matchID, data.BattleDepartureSource{
		DSPodName: podName, GameServerUID: allocation.InstanceUID,
		InstanceEpoch: allocation.InstanceEpoch, AllocationID: allocation.AllocationID,
		PodUID: allocation.PodUID,
	})
}

// releaseFencedPreactiveGameServer handles the crash window after the exact
// GameServer+Pod identity was durably finalized but before PrepareCredential
// assigned an instance epoch. FencePreactiveReleaseExpected is the mandatory
// Redis linearization point before this method.  Epoch zero has never admitted
// a DS or minted a ticket, so it must be physically released and purged without
// fabricating a credentialed instance-teardown proof.
func (u *AllocatorUsecase) releaseFencedPreactiveGameServer(
	ctx context.Context,
	matchID uint64,
	podName string,
	allocation *data.AuthoritativeGameServerAllocation,
) error {
	if allocation == nil || allocation.InstanceEpoch != 0 {
		return u.releaseGameServer(ctx, matchID, podName, allocation)
	}
	if !u.modelB || matchID == 0 || podName == "" || allocation.PodName != podName ||
		allocation.InstanceUID == "" || allocation.AllocationID == "" {
		return errcode.New(errcode.ErrInvalidState,
			"precredential release requires exact fenced GameServer tuple")
	}
	if allocation.PodUID == "" {
		podUID, err := u.ensureDurableReleasePodUID(ctx, matchID, podName,
			data.BattleExpectedInstance{
				AllocationID: allocation.AllocationID, InstanceUID: allocation.InstanceUID,
				InstanceEpoch: 0,
			}, allocation.ReleaseTrack)
		if err != nil {
			return err
		}
		allocation.PodUID = podUID
	}
	if allocation.PodUID == "" {
		return errcode.New(errcode.ErrInvalidState,
			"precredential release requires durable expected Pod UID")
	}
	return u.authoritativeAlloc.ReleaseExpected(ctx, allocation)
}

// reconcilePreactiveRelease 幂等完成永久 pre-active release fence → UID 条件删除 → purge。
// 返回 false 只表示当前仍不可确认回收；墓碑保持永久，安全性不依赖下一轮一定执行。
func (u *AllocatorUsecase) reconcilePreactiveRelease(
	ctx context.Context,
	battle *dsv1.BattleStorageRecord,
) bool {
	if battle == nil || battle.GetAllocationId() == "" || battle.GetGameserverUid() == "" {
		return false
	}
	expected := data.BattleExpectedInstance{
		AllocationID: battle.GetAllocationId(), InstanceUID: battle.GetGameserverUid(),
		InstanceEpoch: battle.GetInstanceEpoch(),
	}
	podUID, preflightErr := u.ensureDurableReleasePodUID(
		ctx, battle.GetMatchId(), battle.GetDsPodName(), expected, battle.GetReleaseTrack())
	if preflightErr != nil {
		plog.With(ctx).Warnw("msg", "preactive_release_pod_uid_preflight_failed",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", preflightErr)
		return false
	}
	battle.PodUid = podUID
	fenced, err := u.authRepo.FencePreactiveReleaseExpected(ctx, battle.GetMatchId(), expected)
	if err != nil || !fenced {
		plog.With(ctx).Warnw("msg", "preactive_release_fence_failed", "match_id", battle.GetMatchId(),
			"allocation_id", battle.GetAllocationId(), "fenced", fenced, "err", err)
		return false
	}
	allocation := &data.AuthoritativeGameServerAllocation{
		PodName: battle.GetDsPodName(), InstanceUID: battle.GetGameserverUid(),
		PodUID: battle.GetPodUid(), InstanceEpoch: battle.GetInstanceEpoch(),
		AllocationID: battle.GetAllocationId(),
	}
	if err := u.releaseFencedPreactiveGameServer(ctx, battle.GetMatchId(), battle.GetDsPodName(), allocation); err != nil {
		plog.With(ctx).Warnw("msg", "preactive_release_unconfirmed", "match_id", battle.GetMatchId(),
			"allocation_id", battle.GetAllocationId(), "err", err)
		return false
	}
	purged, err := u.authRepo.PurgePreactiveReleasedExpected(ctx, battle.GetMatchId(), expected)
	if err != nil {
		plog.With(ctx).Warnw("msg", "preactive_release_purge_failed", "match_id", battle.GetMatchId(),
			"allocation_id", battle.GetAllocationId(), "purged", purged, "err", err)
	}
	return purged
}

// publishReconciledAllocationAbandoned is the durable handoff from a
// physically-confirmed pre-credential allocation cleanup to battle_result.
// The Redis battle record remains permanent until Kafka acknowledges the
// ABANDONED event; only then may it receive terminal retention TTL and leave
// the active recovery index.
func (u *AllocatorUsecase) publishReconciledAllocationAbandoned(
	ctx context.Context,
	battle *dsv1.BattleStorageRecord,
) bool {
	if battle == nil || battle.GetMatchId() == 0 || battle.GetState() != stateAbandoned ||
		battle.GetAllocationId() == "" || battle.GetInstanceEpoch() != 0 || len(battle.GetPlayerIds()) == 0 {
		return false
	}
	snapshot, err := u.authRepo.ReadAuthority(ctx, battle.GetMatchId())
	if err != nil || snapshot.AuthFound || !snapshot.BattleFound || snapshot.Battle == nil ||
		snapshot.Battle.GetState() != stateAbandoned ||
		snapshot.Battle.GetAllocationId() != battle.GetAllocationId() ||
		snapshot.Battle.GetGameserverUid() != battle.GetGameserverUid() {
		plog.With(ctx).Warnw("msg", "allocation_uncertain_terminal_snapshot_rejected",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
		return false
	}
	battle = snapshot.Battle
	if !u.deliverAbandoned(ctx, battle.GetMatchId(), battle.GetDsPodName(),
		battle.GetPlayerIds(), battle.GetMapId(), battle.GetGameMode()) {
		return false
	}
	if battle.GetGameserverUid() == "" {
		// Empty LIST is enough to release the players, but not enough to forget
		// cleanup authority: the original timed-out POST may apply after that
		// LIST. Persist Kafka ACK as a separate tombstone state and keep polling
		// the allocation_id forever (until a future explicit quiescence proof).
		repo, ok := u.repo.(data.AllocationUncertainRepo)
		if !ok {
			return false
		}
		marked, err := repo.MarkAllocationUncertainEmptyLifecyclePublished(ctx,
			battle.GetMatchId(), battle.GetAllocationId())
		if err != nil || !marked {
			plog.With(ctx).Warnw("msg", "allocation_uncertain_empty_tombstone_failed",
				"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(),
				"marked", marked, "err", err)
			return false
		}
		// Delay the next cleanup pass by one heartbeat window without changing
		// the permanent battle record or pretending this is a DS heartbeat.
		if err := u.repo.TouchActive(ctx, battle.GetMatchId(), time.Now().UnixMilli()); err != nil {
			plog.With(ctx).Warnw("msg", "allocation_uncertain_empty_tombstone_index_failed",
				"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
			return false
		}
		plog.With(ctx).Infow("msg", "allocation_uncertain_empty_tombstone_retained",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId())
		return true
	}
	if err := u.repo.ExpireBattle(ctx, battle.GetMatchId(), u.battleTTL()); err != nil {
		plog.With(ctx).Warnw("msg", "allocation_uncertain_terminal_expire_failed",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
		return false
	}
	plog.With(ctx).Infow("msg", "allocation_uncertain_terminal_delivered",
		"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId())
	return true
}

// resumeReconciledAllocationAbandoned re-confirms physical absence after a
// crash between the durable ABANDONED CAS and lifecycle publication. The
// repeat release is exact and idempotent (UID+Pod UID, or allocation_id label
// when the original POST never produced an object).
func (u *AllocatorUsecase) resumeReconciledAllocationAbandoned(
	ctx context.Context,
	battle *dsv1.BattleStorageRecord,
) bool {
	if battle == nil || battle.GetState() != stateAbandoned || battle.GetInstanceEpoch() != 0 ||
		battle.GetAllocationId() == "" {
		return false
	}
	snapshot, err := u.authRepo.ReadAuthority(ctx, battle.GetMatchId())
	if err != nil || snapshot.AuthFound || !snapshot.BattleFound || snapshot.Battle == nil ||
		snapshot.Battle.GetState() != stateAbandoned ||
		snapshot.Battle.GetAllocationId() != battle.GetAllocationId() {
		return false
	}
	battle = snapshot.Battle
	allocation := &data.AuthoritativeGameServerAllocation{AllocationID: battle.GetAllocationId()}
	if battle.GetGameserverUid() != "" {
		if battle.GetDsPodName() == "" || battle.GetPodUid() == "" {
			return false
		}
		allocation.PodName = battle.GetDsPodName()
		allocation.InstanceUID = battle.GetGameserverUid()
		allocation.PodUID = battle.GetPodUid()
		allocation.ReleaseTrack = battle.GetReleaseTrack()
	}
	if err := u.authoritativeAlloc.ReleaseExpected(ctx, allocation); err != nil {
		plog.With(ctx).Warnw("msg", "allocation_uncertain_terminal_release_unconfirmed",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
		return false
	}
	return u.publishReconciledAllocationAbandoned(ctx, battle)
}

// resumeEmptyAllocationTombstone continuously cleans the immutable
// allocation_id after the players' terminal lifecycle has already been
// published. It intentionally never expires/deletes the tombstone: a delayed
// original POST has no credential and cannot admit players, but must still be
// reaped whenever it becomes visible.
func (u *AllocatorUsecase) resumeEmptyAllocationTombstone(
	ctx context.Context,
	battle *dsv1.BattleStorageRecord,
) bool {
	if battle == nil || battle.GetState() != stateAllocationEmptyFence ||
		battle.GetAllocationId() == "" || battle.GetInstanceEpoch() != 0 ||
		battle.GetDsPodName() != "" || battle.GetGameserverUid() != "" || battle.GetPodUid() != "" {
		return false
	}
	snapshot, err := u.authRepo.ReadAuthority(ctx, battle.GetMatchId())
	if err != nil || snapshot.AuthFound || !snapshot.BattleFound || snapshot.Battle == nil ||
		snapshot.Battle.GetState() != stateAllocationEmptyFence ||
		snapshot.Battle.GetAllocationId() != battle.GetAllocationId() {
		return false
	}
	if err := u.authoritativeAlloc.ReleaseExpected(ctx,
		&data.AuthoritativeGameServerAllocation{AllocationID: battle.GetAllocationId()}); err != nil {
		plog.With(ctx).Warnw("msg", "allocation_uncertain_empty_tombstone_cleanup_failed",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
		return false
	}
	if err := u.repo.TouchActive(ctx, battle.GetMatchId(), time.Now().UnixMilli()); err != nil {
		plog.With(ctx).Warnw("msg", "allocation_uncertain_empty_tombstone_reindex_failed",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
		return false
	}
	return true
}

// reconcileAllocationUncertain turns an unknown GSA POST into a bounded,
// restart-safe terminal cancellation without ever issuing a second POST:
// allocation_id LIST -> authoritative empty or one exact GS+Pod -> permanent
// exact release fence -> confirmed physical absence -> durable ABANDONED ->
// lifecycle outbox. Ambiguous/API-unknown results leave the original permanent
// fence and active index untouched for the next sweep.
func (u *AllocatorUsecase) reconcileAllocationUncertain(
	ctx context.Context,
	battle *dsv1.BattleStorageRecord,
) bool {
	if battle == nil || !u.modelB || u.authRepo == nil || u.authoritativeAlloc == nil {
		return false
	}
	repo, repoOK := u.repo.(data.AllocationUncertainRepo)
	resolver, resolverOK := u.authoritativeAlloc.(UncertainGameServerAllocationResolver)
	if !repoOK || !resolverOK {
		plog.With(ctx).Errorw("msg", "allocation_uncertain_reconciler_unavailable_fail_closed",
			"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId())
		return false
	}

	switch battle.GetState() {
	case stateAllocationUncertain:
		combatFactionByPlayer, factionErr := combatFactionMapFromRecords(
			battle.GetPlayerIds(), battle.GetPlayerCombatFactions())
		if factionErr != nil {
			plog.With(ctx).Errorw("msg", "allocation_uncertain_combat_factions_invalid_fail_closed",
				"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", factionErr)
			return false
		}
		allocation, found, err := resolver.ResolveAllocationByID(ctx, battle.GetMatchId(),
			battle.GetAllocationId(), battle.GetPlayerIds(), combatFactionByPlayer,
			battle.GetMapId(), battle.GetGameMode())
		if err != nil {
			plog.With(ctx).Warnw("msg", "allocation_uncertain_resolve_failed_will_retry",
				"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
			return false
		}
		if !found {
			// DeleteCollection+LIST is repeated even after a read-only empty result.
			// This closes a timeout-late-apply window before publishing terminal.
			allocation = &data.AuthoritativeGameServerAllocation{AllocationID: battle.GetAllocationId()}
			if err := u.authoritativeAlloc.ReleaseExpected(ctx, allocation); err != nil {
				plog.With(ctx).Warnw("msg", "allocation_uncertain_empty_release_unconfirmed",
					"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
				return false
			}
			completed, err := repo.CompleteAllocationUncertainRelease(ctx, battle.GetMatchId(),
				battle.GetAllocationId(), "")
			if err != nil || !completed {
				plog.With(ctx).Warnw("msg", "allocation_uncertain_empty_terminal_cas_failed",
					"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(),
					"completed", completed, "err", err)
				return false
			}
		} else {
			fenced, err := repo.FenceAllocationUncertainRelease(ctx, battle.GetMatchId(),
				battle.GetAllocationId(), allocation)
			if err != nil || !fenced {
				plog.With(ctx).Warnw("msg", "allocation_uncertain_exact_release_fence_failed",
					"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(),
					"fenced", fenced, "err", err)
				return false
			}
			if err := u.authoritativeAlloc.ReleaseExpected(ctx, allocation); err != nil {
				plog.With(ctx).Warnw("msg", "allocation_uncertain_exact_release_unconfirmed",
					"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
				return false
			}
			completed, err := repo.CompleteAllocationUncertainRelease(ctx, battle.GetMatchId(),
				battle.GetAllocationId(), allocation.InstanceUID)
			if err != nil || !completed {
				plog.With(ctx).Warnw("msg", "allocation_uncertain_exact_terminal_cas_failed",
					"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(),
					"completed", completed, "err", err)
				return false
			}
		}
	case stateAllocationReconciling:
		if battle.GetAllocationId() == "" || battle.GetDsPodName() == "" ||
			battle.GetGameserverUid() == "" || battle.GetPodUid() == "" || battle.GetInstanceEpoch() != 0 {
			return false
		}
		allocation := &data.AuthoritativeGameServerAllocation{
			PodName: battle.GetDsPodName(), InstanceUID: battle.GetGameserverUid(),
			PodUID: battle.GetPodUid(), AllocationID: battle.GetAllocationId(),
			ReleaseTrack: battle.GetReleaseTrack(),
		}
		if err := u.authoritativeAlloc.ReleaseExpected(ctx, allocation); err != nil {
			plog.With(ctx).Warnw("msg", "allocation_uncertain_exact_release_resume_failed",
				"match_id", battle.GetMatchId(), "allocation_id", battle.GetAllocationId(), "err", err)
			return false
		}
		completed, err := repo.CompleteAllocationUncertainRelease(ctx, battle.GetMatchId(),
			battle.GetAllocationId(), battle.GetGameserverUid())
		if err != nil || !completed {
			return false
		}
	default:
		return false
	}

	terminal, found, err := u.repo.GetBattle(ctx, battle.GetMatchId())
	if err != nil || !found || terminal.GetState() != stateAbandoned {
		return false
	}
	return u.publishReconciledAllocationAbandoned(ctx, terminal)
}

// ── RPC 2:ReleaseBattle ───────────────────────────────────────────────────────

// ReleaseBattle 回收战斗 DS。幂等:镜像不存在视为已释放,返回成功。
func (u *AllocatorUsecase) ReleaseBattle(ctx context.Context, matchID uint64, reason string) error {
	if matchID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	if u.modelB {
		// Model-B 禁止 match_id-only 回读“当前实例”后删除：旧请求可能误杀同 match
		// 重建出的新 UID。正常结算必须走 ReleaseBattleExpected 的 MySQL outbox 证明；
		// abandoned 由内部 sweep 已持有同事务 expected tuple，不经过本 RPC。
		return errcode.New(errcode.ErrInvalidArg,
			"battle %d Model-B release requires terminal outbox expected tuple", matchID)
	}
	battle, found, err := u.repo.GetBattle(ctx, matchID)
	if err != nil {
		return err
	}
	if !found {
		plog.With(ctx).Debugw("msg", "release_idempotent_miss", "match_id", matchID, "reason", reason)
		return nil
	}
	if battle.State == stateAllocationUncertain || battle.State == stateAllocationReconciling ||
		battle.State == stateAllocationEmptyFence ||
		battle.State == statePreactiveReleasing || battle.State == stateAllocationAbort {
		// 即使当前副本仍以 legacy 配置运行，也不能清理由另一个 Model-B writer
		// 写下的永久 POST fence。混跑期间最多返回不可用，绝不 Release/Delete。
		return errcode.New(errcode.ErrUnavailable,
			"battle %d allocation/release result requires explicit reconciliation", matchID)
	}
	if err := u.alloc.Release(ctx, battle.DsPodName); err != nil {
		plog.With(ctx).Warnw("msg", "gameserver_release_failed", "match_id", matchID, "pod", battle.DsPodName, "err", err)
	}
	if err := u.repo.DeleteBattle(ctx, matchID); err != nil {
		return err
	}
	plog.With(ctx).Debugw("msg", "battle_released", "match_id", matchID, "pod", battle.DsPodName, "reason", reason)
	return nil
}

// AbortPreactiveBattle compensates a Matchmaker allocation saga only after a
// payload-authenticated service RPC has supplied the exact operation and DS
// tuple. The Redis fence+journal is committed before Kubernetes is touched;
// every unknown result remains retryable and the permanent journal recognizes
// an ACK-loss retry even after bounded battle/auth records expire.
func (u *AllocatorUsecase) AbortPreactiveBattle(ctx context.Context, request battleabort.Request) error {
	if !request.Complete() {
		return errcode.New(errcode.ErrInvalidArg, "complete battle allocation abort request required")
	}
	if !u.modelB || u.abortRepo == nil || u.authoritativeAlloc == nil {
		return errcode.New(errcode.ErrInvalidState,
			"battle allocation abort requires Redis Model-B authority")
	}

	// Do not create the permanent ABORT fence for a legacy active record until
	// its exact Pod UID is durable.  If battle/auth are already gone, defer to
	// the permanent abort journal below so ACK-loss replay still works.
	preflight, err := u.authRepo.ReadAuthority(ctx, request.MatchID)
	if err != nil {
		return err
	}
	if preflight.BattleFound {
		if _, err := u.ensureDurableReleasePodUID(ctx, request.MatchID, request.Target.PodName,
			data.BattleExpectedInstance{
				AllocationID: request.Target.AllocationID, InstanceUID: request.Target.InstanceUID,
				InstanceEpoch: request.Target.InstanceEpoch,
			}, request.Target.ReleaseTrack); err != nil {
			return err
		}
	}
	fence, err := u.abortRepo.FenceAllocationAbortExpected(ctx, request)
	if err != nil {
		return err
	}
	if fence.Released {
		completed, completeErr := u.abortRepo.CompleteAllocationAbortExpected(
			ctx, request, u.dsCredentialTTL, u.battleTTL())
		if completeErr != nil {
			return completeErr
		}
		if !completed {
			return errcode.New(errcode.ErrUnavailable,
				"battle %d allocation abort ACK cleanup pending", request.MatchID)
		}
		return nil
	}
	battle := fence.Battle
	if battle == nil || battle.GetMatchId() != request.MatchID ||
		battle.GetDsPodName() != request.Target.PodName || battle.GetPodUid() == "" {
		return errcode.New(errcode.ErrInvalidState,
			"battle %d allocation abort fence lacks exact pod authority", request.MatchID)
	}
	allocation := &data.AuthoritativeGameServerAllocation{
		PodName: request.Target.PodName, InstanceUID: request.Target.InstanceUID,
		PodUID: battle.GetPodUid(), InstanceEpoch: request.Target.InstanceEpoch,
		AllocationID: request.Target.AllocationID, ReleaseTrack: request.Target.ReleaseTrack,
	}
	if err := u.releaseGameServer(ctx, request.MatchID, request.Target.PodName, allocation); err != nil {
		return err
	}
	if !u.deliverAbandoned(ctx, request.MatchID, request.Target.PodName,
		battle.GetPlayerIds(), battle.GetMapId(), battle.GetGameMode()) {
		return errcode.New(errcode.ErrUnavailable,
			"battle %d allocation abort lifecycle publish pending", request.MatchID)
	}
	if err := u.lifecycleProofRepo.RecordAllocationLifecyclePublished(
		ctx, request.MatchID, request.Target); err != nil {
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d allocation abort lifecycle marker pending", request.MatchID)
	}
	completed, err := u.abortRepo.CompleteAllocationAbortExpected(
		ctx, request, u.dsCredentialTTL, u.battleTTL())
	if err != nil {
		return err
	}
	if !completed {
		return errcode.New(errcode.ErrUnavailable,
			"battle %d allocation abort completion pending", request.MatchID)
	}
	plog.With(ctx).Infow("msg", "battle_allocation_abort_completed",
		"match_id", request.MatchID, "operation_id", request.OperationID,
		"allocation_id", request.Target.AllocationID)
	return nil
}

// ReleaseBattleExpected 是 Model-B 正常结算 phase1 服务端回收入口。严格顺序：
//  1. MySQL 持久 proof 与当前 Redis stable identity 做 terminal+receipt 原子 CAS；
//  2. 用 Kubernetes GameServer UID delete precondition 回收；
//
// 本方法绝不恢复 Redis TTL。battle_result 必须先把成功 durable CAS 为 released_at_ms，
// 再调用 FinalizeBattleReleaseExpected；DB ACK 长期失败时永久墓碑不会先消失。
// 任一步 timeout/unknown 都返回 error；pending outbox 以同 tuple 幂等重试。
func (u *AllocatorUsecase) ReleaseBattleExpected(
	ctx context.Context,
	matchID uint64,
	reason, podName string,
	expected data.BattleExpectedInstance,
	proof data.BattleResultAuthorizationProof,
) error {
	if !u.modelB || u.authRepo == nil || u.authoritativeAlloc == nil {
		return errcode.New(errcode.ErrInvalidState, "battle terminal release requires Redis authority")
	}
	if matchID == 0 || reason != "completed" || podName == "" ||
		proof.Credential.PodName != podName || proof.Credential.InstanceUID != expected.InstanceUID ||
		proof.Credential.InstanceEpoch != expected.InstanceEpoch {
		return errcode.New(errcode.ErrInvalidArg, "battle terminal release proof is incomplete")
	}
	// Rolling-upgrade gate: pod_uid was added after the first Model-B records.
	// It must be durably present before the terminal CAS.  Legacy records may
	// only be backfilled from exact K8s GameServer/allocation/owned-Pod reads;
	// missing or same-name replacement objects remain retryable and do not
	// create a permanent TERMINATING fence.
	if _, err := u.ensureDurableReleasePodUID(ctx, matchID, podName, expected, ""); err != nil {
		return err
	}
	terminated, err := u.authRepo.TerminateResultExpected(ctx, matchID, expected, proof)
	if err != nil {
		return err
	}
	if !terminated {
		return errcode.New(errcode.ErrDSAllocationFailed,
			"battle %d stable identity changed before terminal release", matchID)
	}
	snapshot, err := u.authRepo.ReadAuthority(ctx, matchID)
	if err != nil {
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d terminal authority reread failed", matchID)
	}
	if !exactTerminatedReleaseSnapshot(snapshot, matchID, podName, expected) {
		return errcode.New(errcode.ErrUnavailable,
			"battle %d terminal authority snapshot is incomplete or changed", matchID)
	}
	allocation := &data.AuthoritativeGameServerAllocation{
		PodName: podName, InstanceUID: expected.InstanceUID,
		InstanceEpoch: expected.InstanceEpoch, AllocationID: expected.AllocationID,
		PodUID: snapshot.Battle.GetPodUid(), ReleaseTrack: snapshot.Battle.GetReleaseTrack(),
	}
	if err := u.releaseGameServer(ctx, matchID, podName, allocation); err != nil {
		plog.With(ctx).Warnw("msg", "terminal_gameserver_release_unconfirmed",
			"match_id", matchID, "allocation_id", expected.AllocationID, "pod", podName, "err", err)
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d terminal gameserver release not confirmed", matchID)
	}
	plog.With(ctx).Infow("msg", "battle_terminal_release_phase1_completed",
		"match_id", matchID, "allocation_id", expected.AllocationID,
		"pod", podName, "uid", expected.InstanceUID)
	return nil
}

func (u *AllocatorUsecase) ensureDurableReleasePodUID(
	ctx context.Context,
	matchID uint64,
	podName string,
	expected data.BattleExpectedInstance,
	expectedReleaseTrack string,
) (string, error) {
	snapshot, err := u.authRepo.ReadAuthority(ctx, matchID)
	if err != nil {
		return "", errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d release preflight authority read failed", matchID)
	}
	if !exactReleaseIdentitySnapshot(snapshot, matchID, podName, expected, expectedReleaseTrack) {
		return "", errcode.New(errcode.ErrDSAllocationFailed,
			"battle %d release preflight identity changed", matchID)
	}
	if snapshot.Battle.GetPodUid() != "" {
		return snapshot.Battle.GetPodUid(), nil
	}
	podUID, err := u.authoritativeAlloc.ResolveExpectedPodUID(ctx,
		&data.AuthoritativeGameServerAllocation{
			PodName: podName, InstanceUID: expected.InstanceUID,
			InstanceEpoch: expected.InstanceEpoch, AllocationID: expected.AllocationID,
		})
	if err != nil || podUID == "" {
		return "", errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d legacy pod UID exact preflight failed", matchID)
	}
	err = u.repo.UpdateBattleKeepTTL(ctx, matchID, updateMaxRetry, func(battle *dsv1.BattleStorageRecord) error {
		if !exactReleaseBattleIdentity(battle, matchID, podName, expected, expectedReleaseTrack) {
			return errcode.New(errcode.ErrDSAllocationFailed,
				"battle %d changed during pod UID backfill", matchID)
		}
		switch battle.GetPodUid() {
		case "":
			battle.PodUid = podUID
		case podUID:
		default:
			return errcode.New(errcode.ErrDSAllocationFailed,
				"battle %d pod UID changed during backfill", matchID)
		}
		return nil
	})
	if err != nil {
		return "", errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d legacy pod UID durable backfill failed", matchID)
	}
	verified, err := u.authRepo.ReadAuthority(ctx, matchID)
	if err != nil || !exactReleaseIdentitySnapshot(verified, matchID, podName, expected, expectedReleaseTrack) ||
		verified.Battle.GetPodUid() != podUID {
		return "", errcode.NewCause(errcode.ErrUnavailable, err,
			"battle %d legacy pod UID backfill verification failed", matchID)
	}
	return podUID, nil
}

func exactReleaseBattleIdentity(
	battle *dsv1.BattleStorageRecord,
	matchID uint64,
	podName string,
	expected data.BattleExpectedInstance,
	expectedReleaseTrack string,
) bool {
	return battle != nil && battle.GetMatchId() == matchID &&
		battle.GetAllocationId() == expected.AllocationID && battle.GetDsPodName() == podName &&
		battle.GetGameserverUid() == expected.InstanceUID &&
		battle.GetInstanceEpoch() == expected.InstanceEpoch &&
		releasetrack.Valid(battle.GetReleaseTrack()) &&
		(expectedReleaseTrack == "" || battle.GetReleaseTrack() == expectedReleaseTrack) &&
		releaseStateAllowsPodUIDBackfill(battle.GetState())
}

func releaseStateAllowsPodUIDBackfill(state string) bool {
	switch state {
	case stateWarming, stateReady, stateRunning, stateEnded, stateAbandoned,
		statePreactiveReleasing, stateAllocationAbort:
		return true
	default:
		return false
	}
}

func exactReleaseIdentitySnapshot(
	snapshot data.BattleAuthoritySnapshot,
	matchID uint64,
	podName string,
	expected data.BattleExpectedInstance,
	expectedReleaseTrack string,
) bool {
	if !snapshot.BattleFound || !exactReleaseBattleIdentity(
		snapshot.Battle, matchID, podName, expected, expectedReleaseTrack) {
		return false
	}
	// auth may have naturally expired before result relay; TerminateResultExpected
	// has a proof-bound reconstruction path.  When present it must still bind the
	// exact stable allocation.
	return !snapshot.AuthFound || (snapshot.Auth != nil && snapshot.Auth.GetMatchId() == matchID &&
		snapshot.Auth.GetAllocationId() == expected.AllocationID && snapshot.Auth.GetDsPodName() == podName &&
		snapshot.Auth.GetInstanceUid() == expected.InstanceUID &&
		snapshot.Auth.GetInstanceEpoch() == expected.InstanceEpoch)
}

func exactTerminatedReleaseSnapshot(
	snapshot data.BattleAuthoritySnapshot,
	matchID uint64,
	podName string,
	expected data.BattleExpectedInstance,
) bool {
	return snapshot.AuthFound && snapshot.Auth != nil && snapshot.BattleFound && snapshot.Battle != nil &&
		exactReleaseBattleIdentity(snapshot.Battle, matchID, podName, expected, "") &&
		snapshot.Battle.GetState() == stateEnded && snapshot.Battle.GetPodUid() != "" &&
		snapshot.Auth.GetMatchId() == matchID && snapshot.Auth.GetAllocationId() == expected.AllocationID &&
		snapshot.Auth.GetDsPodName() == podName && snapshot.Auth.GetInstanceUid() == expected.InstanceUID &&
		snapshot.Auth.GetInstanceEpoch() == expected.InstanceEpoch &&
		snapshot.Auth.GetPhase() == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING
}

// FinalizeBattleReleaseExpected 是 durable released_at_ms 之后的 phase2。它只校验同一
// proof 的 Redis terminal/receipt 墓碑并恢复 TTL，绝不调用 Kubernetes。若上一次
// finalize 响应丢失且 TTL 已把三键全部清空，则按幂等成功返回。
func (u *AllocatorUsecase) FinalizeBattleReleaseExpected(
	ctx context.Context,
	matchID uint64,
	podName string,
	expected data.BattleExpectedInstance,
	proof data.BattleResultAuthorizationProof,
) error {
	if !u.modelB || u.authRepo == nil {
		return errcode.New(errcode.ErrInvalidState, "battle terminal finalize requires Redis authority")
	}
	if matchID == 0 || podName == "" || proof.Credential.PodName != podName ||
		proof.Credential.InstanceUID != expected.InstanceUID ||
		proof.Credential.InstanceEpoch != expected.InstanceEpoch {
		return errcode.New(errcode.ErrInvalidArg, "battle terminal finalize proof is incomplete")
	}
	expired, err := u.authRepo.ExpireResultTerminatedExpected(
		ctx, matchID, expected, proof, u.battleTTL())
	if err != nil {
		return err
	}
	if !expired {
		return errcode.New(errcode.ErrUnavailable,
			"battle %d terminal tombstone retention not confirmed", matchID)
	}
	plog.With(ctx).Infow("msg", "battle_terminal_release_finalized",
		"match_id", matchID, "allocation_id", expected.AllocationID,
		"pod", podName, "uid", expected.InstanceUID)
	return nil
}

// ── RPC 3:Heartbeat ───────────────────────────────────────────────────────────

// HeartbeatResult 是 Heartbeat 的出参(下发给 DS 的控制指令)。
type HeartbeatResult struct {
	Command               string
	AcceptedTokenGen      uint64
	AcceptedTokenJTI      string
	AcceptedInstanceUID   string
	AcceptedInstanceEpoch uint32
	AcceptedWriterEpoch   uint32
	EvictionOrders        []*dsv1.BattleEvictionOrder
}

// RedisAuthorityEnabled 供 service/gm 选择严格 Model B 路径；开启后不存在 legacy fallback。
func (u *AllocatorUsecase) RedisAuthorityEnabled() bool { return u != nil && u.modelB }

// HeartbeatAuthorized 是 Model B 唯一心跳入口。pending 激活、active 幂等续命、battle 投影
// 与服务端接收时间在 Redis 同槽一次 EXEC 完成；请求 ts_ms 不参与任何授权/TTL 判断。
func (u *AllocatorUsecase) HeartbeatAuthorized(
	ctx context.Context,
	matchID uint64,
	id data.BattleCredentialIdentity,
	playerCount int32,
	state string,
	tsMs int64,
) (*HeartbeatResult, error) {
	return u.HeartbeatAuthorizedWithPlayers(ctx, matchID, id, playerCount, state, tsMs,
		false, 0, "", nil, nil)
}

// HeartbeatAuthorizedWithPlayers 是 Battle→Hub 物理离场闭环心跳。只有
// snapshotPresent=true 的新 DS 才能用完整可信 active-player 快照提交
// departure；旧 DS 的 proto3 零值始终 fail-closed，但仍可收到 order 等待升级/
// source UID teardown。
func (u *AllocatorUsecase) HeartbeatAuthorizedWithPlayers(
	ctx context.Context,
	matchID uint64,
	id data.BattleCredentialIdentity,
	playerCount int32,
	state string,
	_ int64,
	snapshotPresent bool,
	censusCapabilityVersion uint32,
	censusID string,
	activePlayerIDs []uint64,
	acknowledgedDepartureIDs []string,
) (*HeartbeatResult, error) {
	if !u.modelB {
		return nil, errcode.New(errcode.ErrInvalidState, "battle Redis authority is not enabled")
	}
	if snapshotPresent && playerCount > int32(len(activePlayerIDs)) {
		return nil, errcode.New(errcode.ErrInvalidArg,
			"battle heartbeat player_count=%d exceeds complete owner census=%d",
			playerCount, len(activePlayerIDs))
	}
	// Rolling Model-B writers may encounter an active record written before
	// pod_uid existed.  Backfill on the first heartbeat, before
	// ActivateHeartbeat can atomically turn an empty battle into ABANDONED and
	// make its authority permanent.  A missing/recreated K8s object rejects this
	// heartbeat with zero Redis state transition.
	preflightSnapshot, preflightReadErr := u.authRepo.ReadAuthority(ctx, matchID)
	if preflightReadErr != nil {
		return nil, preflightReadErr
	}
	if preflightSnapshot.BattleFound && preflightSnapshot.Battle != nil &&
		preflightSnapshot.Battle.GetPodUid() == "" &&
		preflightSnapshot.Battle.GetAllocationId() != "" &&
		preflightSnapshot.Battle.GetDsPodName() == id.PodName &&
		preflightSnapshot.Battle.GetGameserverUid() == id.InstanceUID &&
		preflightSnapshot.Battle.GetInstanceEpoch() == id.InstanceEpoch &&
		legacyPodUIDPreflightCredentialMatches(preflightSnapshot, id) {
		if _, err := u.ensureDurableReleasePodUID(ctx, matchID, id.PodName,
			data.BattleExpectedInstance{
				AllocationID: preflightSnapshot.Battle.GetAllocationId(),
				InstanceUID:  id.InstanceUID, InstanceEpoch: id.InstanceEpoch,
			}, preflightSnapshot.Battle.GetReleaseTrack()); err != nil {
			return nil, err
		}
	}
	out, err := u.authRepo.ActivateHeartbeat(ctx, matchID, id, data.BattleHeartbeatInput{
		PlayerCount:        playerCount,
		State:              state,
		AuthTTL:            u.dsCredentialTTL,
		BattleTTL:          u.battleTTL(),
		EmptyBattleTimeout: u.cfg.EmptyBattleTimeout.Std(),
	})
	if err != nil {
		return nil, err
	}
	// owner 权威实例租约双写(owner-authority.md migrate ⑥):必须在心跳响应返回前完成,
	// 失败语义(弱/强依赖)见 renewOwnerLeaseGate。
	ownerLeaseTrack := ""
	if out.Battle != nil {
		ownerLeaseTrack = out.Battle.GetReleaseTrack()
	}
	if lerr := renewOwnerLeaseGate(ctx, u.ownerLease, u.ownerLeaseRequired,
		id.PodName, id.InstanceUID, id.InstanceEpoch, ownerLeaseTrack); lerr != nil {
		return nil, lerr
	}
	// owner 迁移准入代提交(owner-authority.md migrate ③,近似:授权 census 即准入证据;
	// contract 阶段移交 DS Admission 链)。弱依赖,失败/屏障未开都不影响心跳。
	if snapshotPresent && len(activePlayerIDs) > 0 {
		ownerAdmitCensusWeak(ctx, u.ownerAuth, &u.ownerAdmitted, activePlayerIDs,
			ownerTypeBattle, id.PodName, id.InstanceUID, 2*time.Second)
	}
	result := &HeartbeatResult{
		AcceptedTokenGen:      out.Active.Gen,
		AcceptedTokenJTI:      out.Active.JTI,
		AcceptedInstanceUID:   out.Active.InstanceUID,
		AcceptedInstanceEpoch: out.Active.InstanceEpoch,
		AcceptedWriterEpoch:   out.Active.WriterEpoch,
	}
	if out.FirstActivation {
		plog.With(ctx).Infow("msg", "battle_ds_credential_activated", "match_id", matchID,
			"pod", id.PodName, "uid", id.InstanceUID, "epoch", id.InstanceEpoch,
			"gen", id.Gen, "jti", id.JTI, "writer_epoch", id.WriterEpoch)
	}
	if out.Battle != nil && out.Battle.GetAllocationId() != "" {
		orders, reconcileErr := u.repo.ReconcilePlayerDepartures(ctx, matchID,
			data.BattleDepartureSource{
				DSPodName: id.PodName, GameServerUID: id.InstanceUID,
				InstanceEpoch: id.InstanceEpoch, AllocationID: out.Battle.GetAllocationId(),
			}, snapshotPresent, censusCapabilityVersion, censusID,
			activePlayerIDs, acknowledgedDepartureIDs)
		if reconcileErr != nil {
			return nil, reconcileErr
		}
		result.EvictionOrders = orders
	}
	if out.FirstAbandon && out.Battle != nil {
		finished := u.finishEmptyAbandon(ctx, matchID, out.Battle.DsPodName,
			out.Battle.GameserverUid, out.Battle.PodUid, out.Battle.AllocationId,
			out.Battle.ReleaseTrack, out.Battle.InstanceEpoch,
			out.Battle.PlayerIds, out.Battle.MapId, out.Battle.GameMode)
		result.Command = finished.Command
		return result, nil
	}
	if out.Terminal || out.Battle == nil || out.Battle.State == stateEnded || out.Battle.State == stateAbandoned {
		result.Command = commandStop
		return result, nil
	}
	if u.locator != nil && (out.Battle.State == stateReady || out.Battle.State == stateRunning) &&
		out.Battle.DsAddr != "" && len(out.Battle.PlayerIds) > 0 {
		u.refreshBattleLocations(ctx, out.Battle.PlayerIds, matchID, out.Battle.DsAddr)
	}
	return result, nil
}

func legacyPodUIDPreflightCredentialMatches(
	snapshot data.BattleAuthoritySnapshot,
	id data.BattleCredentialIdentity,
) bool {
	if !snapshot.AuthFound || snapshot.Auth == nil || snapshot.Battle == nil ||
		snapshot.Auth.GetMatchId() != snapshot.Battle.GetMatchId() ||
		snapshot.Auth.GetAllocationId() != snapshot.Battle.GetAllocationId() ||
		snapshot.Auth.GetDsPodName() != id.PodName || snapshot.Auth.GetInstanceUid() != id.InstanceUID ||
		snapshot.Auth.GetInstanceEpoch() != id.InstanceEpoch || id.ExpMs <= uint64(time.Now().UnixMilli()) {
		return false
	}
	matches := func(c *dsv1.BattleDSCredential) bool {
		return c != nil && c.GetGen() == id.Gen && c.GetJti() == id.JTI && c.GetExpMs() == id.ExpMs &&
			c.GetKid() == id.Kid && c.GetInstanceUid() == id.InstanceUID &&
			c.GetInstanceEpoch() == id.InstanceEpoch && c.GetTokenSha256() == id.TokenSHA256 &&
			c.GetWriterEpoch() == id.WriterEpoch
	}
	return matches(snapshot.Auth.GetActive()) || matches(snapshot.Auth.GetPending())
}

// Heartbeat 处理 DS 上报(单向 unary,DS 每 5s 调)。刷新 last_heartbeat_ms + 状态。
// 镜像不存在(孤儿 DS)→ 返回 stop 指令让其自行停机。
//
// 已是终态(ended/abandoned)的镜像:直接返回 stop,且**不写回记录**——不刷新
// LastHeartbeatMs / TTL,也不重新 ZAdd active。否则 abandoned 后仍在心跳的 DS(pod
// release 失败 / 延迟终止)会不断推迟 sweep 补偿重试并刷新 BattleTTL 上界,使 active
// 重新可能无限堆积(W4 ⑧ Codex 复审 P1)。
func (u *AllocatorUsecase) Heartbeat(ctx context.Context, matchID uint64, podName string, playerCount int32, state string, tsMs int64) (*HeartbeatResult, error) {
	if matchID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	now := time.Now().UnixMilli()

	var becameReady bool
	// 断线重连(docs/design/battle-reconnect.md §2.2):捕获对局在 ready/running 时的玩家名单 +
	// ds_addr,心跳成功后续期这些玩家的 BATTLE 位置 TTL。回调可能因 CAS 冲突重跑,故每轮重置。
	var refreshActive bool
	var refreshAddr string
	var refreshPlayers []uint64
	// 空场兜底(2026-07-06):对局活跃但 player_count==0 持续超过 EmptyBattleTimeout → 判 abandoned
	// (全员掉线未归 / 客户端从未连入,DS 空转烧资源)。主路径是 DS 侧空场计时器自结算
	// (agones-dev.md §2.4),这里是后端保险;阈值必须远大于断线重连窗口(~30s),默认 5m。
	var emptyAbandoned bool
	var abandonPod string
	var abandonUID string
	var abandonPodUID string
	var abandonAllocationID string
	var abandonReleaseTrack string
	var abandonInstanceEpoch uint32
	var abandonPlayers []uint64
	var abandonMapID uint32
	var abandonGameMode string
	emptyTimeoutMs := u.cfg.EmptyBattleTimeout.Std().Milliseconds()
	err := u.repo.UpdateBattleWithLock(ctx, matchID, updateMaxRetry, func(b *dsv1.BattleStorageRecord) error {
		// CAS 冲突重跑时以最后一轮为准,每轮重置出参标记
		refreshActive = false
		emptyAbandoned = false
		if b.State == stateAllocationUncertain || b.State == stateAllocationReconciling ||
			b.State == stateAllocationEmptyFence ||
			b.State == statePreactiveReleasing || b.State == stateAllocationAbort {
			return errHeartbeatAllocationFenced
		}
		// 已是终态(ended/abandoned):中止写回(哨兵错误),不刷新 TTL/active,令 DS 停机
		if b.State == stateEnded || b.State == stateAbandoned {
			return errHeartbeatTerminal
		}
		// podName 校验:镜像已绑定某个 pod,但上报方是另一个 pod(旧 DS / 孤儿 DS / 重分配残留)→
		// 不写回该镜像,令上报方停机,避免污染新对局(防进错对局的 DS 刷 state/心跳)。
		if b.DsPodName != "" && podName != "" && b.DsPodName != podName {
			return errHeartbeatPodMismatch
		}
		prevState := b.State
		b.LastHeartbeatMs = now
		b.PlayerCount = playerCount
		if state != "" {
			b.State = state
		}
		// warming → ready/running:DS 首次确认就绪,这一跳让 AllocateBattle 得以放行 matchmaker。
		if prevState == stateWarming && (b.State == stateReady || b.State == stateRunning) {
			becameReady = true
		}
		// 空场跟踪:活跃对局无人 → 盖 EmptySinceMs 起计时;有人回来 → 清零;
		// 持续空场超阈值 → 同一 CAS 内直接写 abandoned(与心跳写回原子,无额外竞态窗口)。
		if b.State == stateReady || b.State == stateRunning {
			switch {
			case playerCount > 0:
				b.EmptySinceMs = 0
			case b.EmptySinceMs == 0:
				b.EmptySinceMs = now
			case emptyTimeoutMs > 0 && now-b.EmptySinceMs >= emptyTimeoutMs:
				b.State = stateAbandoned
				emptyAbandoned = true
				abandonPod = b.DsPodName
				abandonUID = b.GameserverUid
				abandonPodUID = b.PodUid
				abandonAllocationID = b.AllocationId
				abandonReleaseTrack = b.ReleaseTrack
				abandonInstanceEpoch = b.InstanceEpoch
				abandonPlayers = append([]uint64(nil), b.PlayerIds...)
				abandonMapID = b.MapId
				abandonGameMode = b.GameMode
			}
		}
		// 对局活跃(ready/running,且未被空场超时判弃):记下玩家名单 + ds_addr,供心跳后续期 BATTLE 位置。
		if b.State == stateReady || b.State == stateRunning {
			refreshActive = true
			refreshAddr = b.DsAddr
			refreshPlayers = append(refreshPlayers[:0], b.PlayerIds...)
		}
		return nil
	}, u.battleTTL())

	if err != nil {
		switch {
		case errors.Is(err, errHeartbeatAllocationFenced):
			plog.With(ctx).Warnw("msg", "heartbeat_allocation_fenced_stop", "match_id", matchID, "pod", podName)
			return &HeartbeatResult{Command: commandStop}, nil
		case errors.Is(err, errHeartbeatTerminal):
			// 终态 DS:不写回、通知停机,补偿重试与 TTL 上界不受影响
			plog.With(ctx).Infow("msg", "heartbeat_terminal_stop", "match_id", matchID, "pod", podName)
			u.killStrandedDS(ctx, matchID, podName, "terminal")
			return &HeartbeatResult{Command: commandStop}, nil
		case errors.Is(err, errHeartbeatPodMismatch):
			// pod 不匹配:不写回镜像,令旧/孤儿 DS 停机(防污染新对局)
			plog.With(ctx).Warnw("msg", "heartbeat_pod_mismatch", "match_id", matchID, "pod", podName)
			u.killStrandedDS(ctx, matchID, podName, "pod_mismatch")
			return &HeartbeatResult{Command: commandStop}, nil
		case errcode.As(err) == errcode.ErrDSPodNotFound:
			// 孤儿 DS:无镜像,通知停机
			plog.With(ctx).Warnw("msg", "heartbeat_orphan_ds", "match_id", matchID, "pod", podName)
			u.killStrandedDS(ctx, matchID, podName, "orphan")
			return &HeartbeatResult{Command: commandStop}, nil
		default:
			return nil, err
		}
	}
	if becameReady {
		// 验收日志:Battle DS heartbeat match_id=<id> pod=<pod> state=running/ready
		plog.With(ctx).Debugw("msg", "battle_ds_heartbeat_ready", "match_id", matchID, "pod", podName, "state", state)
	}
	if emptyAbandoned {
		// 空场超时判弃:回收 pod + 投递补偿 + 移出 active,回 stop 指令令 DS 停机。
		return u.finishEmptyAbandon(ctx, matchID, abandonPod, abandonUID,
			abandonPodUID, abandonAllocationID, abandonReleaseTrack, abandonInstanceEpoch,
			abandonPlayers, abandonMapID, abandonGameMode), nil
	}
	// 断线重连(docs/design/battle-reconnect.md §2.2):对局活跃时续期玩家 BATTLE 位置 TTL,
	// 使玩家整局在线期间 login 都能检测到"在战斗中",支持中途掉线重登直连回原 battle DS。
	if u.locator != nil && refreshActive && refreshAddr != "" && len(refreshPlayers) > 0 {
		u.refreshBattleLocations(ctx, refreshPlayers, matchID, refreshAddr)
	}
	return &HeartbeatResult{Command: commandNone}, nil
}

// finishEmptyAbandon 完成空场超时判弃的收尾(abandoned 已在心跳 CAS 内写入镜像):
// 回收 pod + 投递 ds.lifecycle{ABANDONED} 补偿事件 + 移出 active,并令 DS 停机。
//
// 投递失败的重试闭环(不变量 §4 可靠补偿):投递失败时对局保留在 active；Model B
// 的 TERMINATING 两键保持永久，直到外部 release 与 lifecycle 都明确成功后才恢复
// 有界 TTL；legacy 仍以原 BattleTTL 为天然上界。
func (u *AllocatorUsecase) finishEmptyAbandon(
	ctx context.Context,
	matchID uint64,
	podName, instanceUID, podUID, allocationID, releaseTrack string,
	instanceEpoch uint32,
	playerIDs []uint64,
	mapID uint32,
	gameMode string,
) *HeartbeatResult {
	plog.With(ctx).Warnw("msg", "battle_abandoned_empty_timeout",
		"match_id", matchID, "pod", podName, "empty_timeout", u.cfg.EmptyBattleTimeout.String())
	var expected *data.AuthoritativeGameServerAllocation
	if u.modelB {
		fence := data.BattleExpectedInstance{
			AllocationID: allocationID, InstanceUID: instanceUID, InstanceEpoch: instanceEpoch,
		}
		resolvedPodUID, preflightErr := u.ensureDurableReleasePodUID(
			ctx, matchID, podName, fence, releaseTrack)
		if preflightErr != nil {
			plog.With(ctx).Warnw("msg", "empty_abandon_pod_uid_preflight_failed",
				"match_id", matchID, "pod", podName, "err", preflightErr)
			return &HeartbeatResult{Command: commandStop}
		}
		podUID = resolvedPodUID
		terminated, terr := u.authRepo.TerminateExpected(
			ctx, matchID, fence, stateAbandoned, u.dsCredentialTTL, u.battleTTL())
		if !terminated {
			plog.With(ctx).Warnw("msg", "empty_abandon_terminate_fence_failed", "match_id", matchID,
				"pod", podName, "err", terr)
			return &HeartbeatResult{Command: commandStop}
		}
		if terr != nil {
			plog.With(ctx).Warnw("msg", "empty_abandon_index_cleanup_failed", "match_id", matchID, "err", terr)
		}
		expected = &data.AuthoritativeGameServerAllocation{
			PodName: podName, InstanceUID: instanceUID, AllocationID: allocationID,
			PodUID: podUID, InstanceEpoch: instanceEpoch, ReleaseTrack: releaseTrack,
		}
	}
	if rerr := u.releaseGameServer(ctx, matchID, podName, expected); rerr != nil {
		plog.With(ctx).Warnw("msg", "empty_abandon_release_failed", "match_id", matchID, "pod", podName, "err", rerr)
		if u.modelB {
			// 永久 TERMINATING fence 留在 Redis；不得继续投递/Expire 后让墓碑消失。
			return &HeartbeatResult{Command: commandStop}
		}
	}
	if u.deliverAbandoned(ctx, matchID, podName, playerIDs, mapID, gameMode) {
		if u.modelB {
			target := placement.Target{
				PodName: podName, InstanceUID: instanceUID, InstanceEpoch: instanceEpoch,
				AllocationID: allocationID, ReleaseTrack: releaseTrack,
			}
			if err := u.lifecycleProofRepo.RecordAllocationLifecyclePublished(ctx, matchID, target); err != nil {
				plog.With(ctx).Warnw("msg", "empty_abandon_lifecycle_marker_failed",
					"match_id", matchID, "allocation_id", allocationID, "err", err)
				return &HeartbeatResult{Command: commandStop}
			}
			fence := data.BattleExpectedInstance{
				AllocationID: allocationID, InstanceUID: instanceUID, InstanceEpoch: instanceEpoch,
			}
			if expired, eerr := u.authRepo.ExpireTerminatedExpected(
				ctx, matchID, fence, u.dsCredentialTTL, u.battleTTL()); eerr != nil || !expired {
				plog.With(ctx).Warnw("msg", "empty_abandon_expire_failed", "match_id", matchID,
					"expired", expired, "err", eerr)
			}
		} else if eerr := u.repo.ExpireBattle(ctx, matchID, u.battleTTL()); eerr != nil {
			plog.With(ctx).Warnw("msg", "empty_abandon_expire_failed", "match_id", matchID, "err", eerr)
		}
	}
	return &HeartbeatResult{Command: commandStop}
}

// refreshBattleLocations 异步续期一批玩家的 BATTLE 位置 TTL(断线重连,弱依赖)。
//
// fire-and-forget:不给心跳响应加尾延迟;用 plog.Detach(只复制 trace_id 等日志字段,
// 满足不变量 §8 写带 trace_id,同时剥离心跳 RPC 的取消与 server transport——下游是
// 挂 Trace middleware 的 gRPC client,不得继承入站请求 transport)+ 短超时防 locator
// 卡死泄漏 goroutine。失败只 Warn,绝不影响心跳 / 对局。
func (u *AllocatorUsecase) refreshBattleLocations(ctx context.Context, playerIDs []uint64, matchID uint64, dsAddr string) {
	players := append([]uint64(nil), playerIDs...) // 拷贝,脱离调用方切片复用
	go func() {
		rctx, cancel := context.WithTimeout(plog.Detach(ctx), locationRefreshTimeout)
		defer cancel()
		if err := u.locator.RefreshBattleLocations(rctx, players, matchID, dsAddr); err != nil {
			plog.With(rctx).Warnw("msg", "refresh_battle_locations_failed", "match_id", matchID, "err", err)
		}
	}()
}

// ── RPC 4:ListBattles ─────────────────────────────────────────────────────────

// ListBattles 列出当前战斗实例,stateFilter 非空时按 state 过滤。
func (u *AllocatorUsecase) ListBattles(ctx context.Context, stateFilter string) ([]*dsv1.BattleInfo, error) {
	matchIDs, err := u.repo.RangeActiveBattles(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*dsv1.BattleInfo, 0, len(matchIDs))
	for _, mid := range matchIDs {
		b, found, gerr := u.repo.GetBattle(ctx, mid)
		if gerr != nil || !found {
			continue
		}
		if stateFilter != "" && b.State != stateFilter {
			continue
		}
		out = append(out, &dsv1.BattleInfo{
			MatchId:       b.MatchId,
			DsPodName:     b.DsPodName,
			DsAddr:        b.DsAddr,
			State:         b.State,
			PlayerCount:   b.PlayerCount,
			AllocatedAtMs: b.AllocatedAtMs,
		})
	}
	return out, nil
}

// ── 后台心跳超时扫描 ──────────────────────────────────────────────────────────

// RunHeartbeatSweep 启动后台心跳超时扫描,直到 ctx 取消(不变量 §4)。
func (u *AllocatorUsecase) RunHeartbeatSweep(ctx context.Context) {
	ticker := time.NewTicker(u.cfg.SweepInterval.Std())
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "heartbeat_sweep_started",
		"interval", u.cfg.SweepInterval.String(), "timeout", u.cfg.HeartbeatTimeout.String())
	// Recover the derived index immediately on process start. Waiting for the
	// first ticker would leave a lost permanent tombstone invisible for a full
	// sweep interval after every restart.
	if err := u.sweepOnce(ctx); err != nil {
		plog.With(ctx).Warnw("msg", "heartbeat_initial_sweep_failed", "err", err)
	}
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "heartbeat_sweep_stopped")
			return
		case <-ticker.C:
			// 压测前审核 P1:census 准入缓存按 last-touch TTL 清死实例项(对齐 hub_allocator)。
			// 纯本地内存卫生,不经存储、不依赖 writer 身份,故每 tick 无条件先执行——否则本副本
			// 处理过心跳的已销毁 Battle 实例(UID 永不复用)留下的 admitted 项永不回收,长压测 OOM。
			// 活实例项每心跳 census 续期,仅超 TTL 未续期(实例已销毁)的项被清。
			sweepStaleOwnerAdmitted(&u.ownerAdmitted, time.Now().Add(-ownerAdmittedStaleTTL))
			if err := u.sweepOnce(ctx); err != nil {
				plog.With(ctx).Warnw("msg", "heartbeat_sweep_failed", "err", err)
			}
		}
	}
}

// sweepOnce 扫描一次:last_heartbeat_ms 早于阈值的战斗 → 标记 abandoned + 回收 + 可靠补偿。
//
// W4 ⑧ 可靠补偿(不变量 §4 DS 崩溃必有补偿):
// 把 active ZSET 自身当作补偿事件的「outbox」——abandoned 的对局在 ds.lifecycle 事件
// 成功投递前**不移出 active**,故下一轮 sweep 会再次命中并重试投递;只有投递成功(或显式
// local/off 开发配置的 best-effort 回退)才 ExpireBattle 移出 active。配合 battle_result 幂等消费
// (不变量 §2),整条补偿链是 at-least-once 闭环,可穿越 Kafka 临时不可用。
//
// legacy 天然上界靠 UpdateBattleKeepTTL(KEEPTTL)。Model B 则在任何外部 Release 前
// 把 TERMINATING auth+battle 置为永久，只有 ReleaseExpected 与 lifecycle 投递都明确
// 成功后才由 ExpireTerminatedExpected 恢复有界 TTL；未知结果宁可不可用也不丢 fence。
// stuckReconcileState 列出「靠重试收敛、外部依赖不可用时会原地打转」的 sweep 状态。
// 这些项在每轮被处理前先退避,让出队头,避免饿死队尾的 §9.4 abandoned 补偿
// (INC-20260724-001)。
//
// 刻意**不含** stateAbandoned+epoch==0 的 resume 分支:那条本身就是 §9.4 补偿的最后一棒,
// 让它保持最高优先级重试;它不产生外部 GSA POST,单次耗时也远小于上面几种。
func stuckReconcileState(state string) bool {
	switch state {
	case stateAllocationUncertain, stateAllocationReconciling, stateAllocationEmptyFence,
		statePreactiveReleasing, stateAllocationAbort:
		return true
	}
	return false
}

// sweepRoundBudget 返回单轮 sweep 的墙钟预算。
//
// 上界口径(可断言):预算只在**开始下一项之前**检查,故单轮实际上界是
// `SweepInterval + 单项最坏耗时`,不是 `SweepInterval`——单项最坏 ≈
// LIST + getPod + DELETE + waitExpectedInstanceGone,各自受 allocate_timeout 约束。
// 写清这一点是为了让验收断言写得出来,而不是放宽成无意义的"大概不会太久"。
func (u *AllocatorUsecase) sweepRoundBudget() time.Duration {
	if d := u.cfg.SweepInterval.Std(); d > 0 {
		return d
	}
	return 5 * time.Second
}

func (u *AllocatorUsecase) sweepOnce(ctx context.Context) error {
	if err := u.reconcileActiveIndexIfDue(ctx); err != nil {
		return err
	}
	threshold := time.Now().Add(-u.cfg.HeartbeatTimeout.Std()).UnixMilli()
	stale, err := u.repo.RangeStaleBattles(ctx, threshold)
	if err != nil {
		return err
	}
	roundStart := time.Now()
	budget := u.sweepRoundBudget()
	processed := 0
	for _, mid := range stale {
		// 单轮墙钟预算(INC-20260724-001):控制面持续超时时单项可耗时数十秒,
		// 无预算的一轮会把下一 tick 直接叠上来,且队尾的 §9.4 abandoned 补偿被无限推后。
		// processed > 0 保证每轮至少推进一项(预算再小也不会活锁);未处理项留在 active
		// ZSET,下一 tick 继续(outbox 语义,进程重启即恢复)。
		if processed > 0 && time.Since(roundStart) >= budget {
			plog.With(ctx).Warnw("msg", "allocation_sweep_round_budget_exhausted",
				"processed", processed, "deferred_to_next_tick", len(stale)-processed,
				"budget", budget.String(), "elapsed_ms", time.Since(roundStart).Milliseconds())
			break
		}
		processed++
		// 先于 authority-mode 分支识别永久 fence：这样同版本但仍跑 legacy 配置的
		// writer 也只读跳过，不能把 uncertain 改成 abandoned 后 Release/Delete。
		inflight, found, readErr := u.repo.GetBattle(ctx, mid)
		if readErr != nil {
			plog.With(ctx).Warnw("msg", "allocation_sweep_read_failed",
				"match_id", mid, "err", readErr)
			continue
		}
		// sweep 公平性(INC-20260724-001):下面几种是「永久墓碑 / 靠重试收敛」状态,
		// 外部依赖(k8s/Agones 控制面)持续不可用时它们永远收敛不了,而其 active ZSET
		// score 不变 ⇒ 恒排在 RangeStaleBattles 的队头,串行吃掉整轮预算,把队尾
		// abandoned 对局的 §9.4 补偿无限推后(事故当天控制面超时约 40s 期间正是此形态)。
		// 故在进入分支工作**之前**先把 score 推到当前时刻(退避约一个 HeartbeatTimeout)。
		//
		// ⚠️ 顺序是正确性关键:ZAdd(退避)→ 对账工作 →(成功则)ZRem,最终态仍是「已移除」。
		// 反过来在工作之后 TouchActive 会把刚被 RemoveActive 的项**重新插回** active 集合。
		//
		// 已知代价(诚实登记):削弱 RunHeartbeatSweep 开头「重启即扫」的意图 ——
		// 上个进程刚退避过的项,重启后最多一个 HeartbeatTimeout 内不在结果集。
		// 这些是永久墓碑不会消失,只是可见性延迟,不丢补偿。
		if found && stuckReconcileState(inflight.GetState()) {
			if terr := u.repo.TouchActive(ctx, mid, time.Now().UnixMilli()); terr != nil {
				plog.With(ctx).Warnw("msg", "allocation_sweep_defer_failed",
					"match_id", mid, "state", inflight.GetState(), "err", terr)
			}
		}
		if found && (inflight.State == stateAllocationUncertain ||
			inflight.State == stateAllocationReconciling) {
			if u.modelB {
				u.reconcileAllocationUncertain(ctx, inflight)
			} else {
				plog.With(ctx).Debugw("msg", "allocation_uncertain_retained_legacy_writer",
					"match_id", mid, "allocation_id", inflight.AllocationId)
			}
			continue
		}
		if found && inflight.State == stateAllocationEmptyFence {
			if u.modelB {
				u.resumeEmptyAllocationTombstone(ctx, inflight)
			}
			// Old writers do not know this state and must remain read-only.
			continue
		}
		if u.modelB && found && inflight.State == stateAbandoned && inflight.GetInstanceEpoch() == 0 {
			// allocation_uncertain reconciliation committed ABANDONED but crashed
			// before Kafka ACK/Expire. Reconfirm exact physical absence, then resume
			// the durable lifecycle handoff.
			u.resumeReconciledAllocationAbandoned(ctx, inflight)
			continue
		}
		if found && inflight.State == statePreactiveReleasing {
			// cleanup 在外部 DELETE 响应未知后留下的永久墓碑。重试只会再次
			// 做 UID 条件 Release 并在明确成功后 purge，不会再次 GSA POST。
			if u.modelB {
				u.reconcilePreactiveRelease(ctx, inflight)
			}
			continue
		}
		if found && inflight.State == stateAllocationAbort {
			if !u.modelB || u.abortRepo == nil {
				continue
			}
			request, released, journalFound, journalErr := u.abortRepo.ReadAllocationAbort(ctx, mid)
			if journalErr != nil || !journalFound {
				plog.With(ctx).Warnw("msg", "allocation_abort_journal_unavailable",
					"match_id", mid, "found", journalFound, "err", journalErr)
				continue
			}
			if abortErr := u.AbortPreactiveBattle(ctx, request); abortErr != nil {
				plog.With(ctx).Warnw("msg", "allocation_abort_reconcile_pending",
					"match_id", mid, "released", released, "err", abortErr)
			}
			continue
		}
		if u.modelB && found && inflight.State == stateAllocating {
			// Model B 只有 FenceBattleAllocation 成功把 state 改成
			// allocation_uncertain 后才允许 GSA POST。仍为 allocating 的陈旧
			// claim 机械证明外部副作用尚未开始，可按 allocation_id 直接撤销；
			// 无需、也不能伪造一个缺 UID 的外部 release。
			deleted, deleteErr := u.repo.DeleteBattleIfAllocationMatches(
				ctx, mid, inflight.GetAllocationId(), inflight.GetDsPodName())
			if deleteErr != nil {
				plog.With(ctx).Warnw("msg", "model_b_prepost_claim_delete_failed",
					"match_id", mid, "allocation_id", inflight.GetAllocationId(),
					"deleted", deleted, "err", deleteErr)
			}
			continue
		}
		if u.modelB && found && inflight.GetPodUid() == "" &&
			inflight.GetDsPodName() != "" && inflight.GetGameserverUid() != "" &&
			inflight.GetInstanceEpoch() > 0 && inflight.GetAllocationId() != "" {
			resolvedPodUID, preflightErr := u.ensureDurableReleasePodUID(
				ctx, mid, inflight.GetDsPodName(), data.BattleExpectedInstance{
					AllocationID: inflight.GetAllocationId(), InstanceUID: inflight.GetGameserverUid(),
					InstanceEpoch: inflight.GetInstanceEpoch(),
				}, inflight.GetReleaseTrack())
			if preflightErr != nil {
				plog.With(ctx).Warnw("msg", "model_b_stale_pod_uid_preflight_failed",
					"match_id", mid, "pod", inflight.GetDsPodName(), "err", preflightErr)
				continue
			}
			inflight.PodUid = resolvedPodUID
		}
		if u.modelB {
			out, aerr := u.authRepo.AbandonIfStale(
				ctx, mid, threshold, u.dsCredentialTTL, u.battleTTL())
			if aerr != nil {
				// Redis 权威不可读/状态不一致时 fail-closed：绝不凭派生 ZSET 直接 Release。
				// 仅 battle 已随 TTL 消失时可安全清残留索引。
				if _, found, gerr := u.repo.GetBattle(ctx, mid); gerr == nil && !found {
					_ = u.repo.RemoveActive(ctx, mid)
				}
				plog.With(ctx).Warnw("msg", "model_b_sweep_authority_check_failed",
					"match_id", mid, "err", aerr)
				continue
			}
			b := out.Battle
			if !out.Abandoned && !out.AlreadyTerminal {
				// 只是跨 slot ZSET score 陈旧；用事务快照里的服务端 auth heartbeat 修索引。
				if b != nil {
					_ = u.repo.TouchActive(ctx, mid, b.LastHeartbeatMs)
				}
				continue
			}
			if b == nil {
				continue
			}
			if b.State == stateEnded {
				_ = u.repo.RemoveActive(ctx, mid)
				continue
			}
			if b.State != stateAbandoned {
				continue
			}
			if !out.AuthFound || !out.ActiveFound {
				// Prepare 前崩溃的分配也必须先进入永久 release fence。旧实现
				// Release→Delete 虽顺序较安全，但有限 TTL 仍会在 Release 响应未知后
				// 自行开放第二次 POST；统一走同一个 fenced 回收状态机。
				if u.reconcilePreactiveRelease(ctx, b) {
					plog.With(ctx).Infow("msg", "model_b_inflight_reconciled",
						"match_id", mid, "allocation_id", b.AllocationId)
				}
				continue
			}
			// abandoned 的外部回收按 UID/allocation_id 幂等执行到确认成功；失败必须保留
			// active outbox，不能先投补偿并移出索引后永久泄漏 GameServer。
			expectedInstance := data.BattleExpectedInstance{
				AllocationID: b.GetAllocationId(), InstanceUID: b.GetGameserverUid(),
				InstanceEpoch: b.GetInstanceEpoch(),
			}
			resolvedPodUID, preflightErr := u.ensureDurableReleasePodUID(
				ctx, mid, b.GetDsPodName(), expectedInstance, b.GetReleaseTrack())
			if preflightErr != nil {
				plog.With(ctx).Warnw("msg", "model_b_sweep_pod_uid_preflight_failed",
					"match_id", mid, "pod", b.GetDsPodName(), "err", preflightErr)
				continue
			}
			b.PodUid = resolvedPodUID
			terminated, terminateErr := u.authRepo.TerminateExpected(
				ctx, mid, expectedInstance, stateAbandoned, u.dsCredentialTTL, u.battleTTL())
			if !terminated {
				plog.With(ctx).Warnw("msg", "model_b_sweep_terminate_fence_failed",
					"match_id", mid, "pod", b.DsPodName, "err", terminateErr)
				continue
			}
			if terminateErr != nil {
				plog.With(ctx).Warnw("msg", "model_b_sweep_index_cleanup_failed",
					"match_id", mid, "err", terminateErr)
			}
			if rerr := u.releaseGameServer(ctx, mid, b.DsPodName, &data.AuthoritativeGameServerAllocation{
				PodName: b.DsPodName, InstanceUID: b.GameserverUid, AllocationID: b.AllocationId,
				PodUID: b.PodUid, InstanceEpoch: b.InstanceEpoch,
			}); rerr != nil {
				plog.With(ctx).Warnw("msg", "model_b_sweep_release_failed",
					"match_id", mid, "pod", b.DsPodName, "err", rerr)
				continue
			}
			if out.Abandoned {
				plog.With(ctx).Infow("msg", "battle_abandoned_heartbeat_timeout",
					"match_id", mid, "pod", b.DsPodName, "authority", "redis")
			}
			if u.deliverAbandoned(ctx, mid, b.DsPodName, b.PlayerIds, b.MapId, b.GameMode) {
				target := placement.Target{
					PodName: b.DsPodName, InstanceUID: b.GameserverUid,
					InstanceEpoch: b.InstanceEpoch, AllocationID: b.AllocationId,
					ReleaseTrack: b.ReleaseTrack,
				}
				if markerErr := u.lifecycleProofRepo.RecordAllocationLifecyclePublished(
					ctx, mid, target); markerErr != nil {
					plog.With(ctx).Warnw("msg", "model_b_sweep_lifecycle_marker_failed",
						"match_id", mid, "allocation_id", b.AllocationId, "err", markerErr)
					continue
				}
				if expired, eerr := u.authRepo.ExpireTerminatedExpected(
					ctx, mid, expectedInstance, u.dsCredentialTTL, u.battleTTL()); eerr != nil || !expired {
					plog.With(ctx).Warnw("msg", "model_b_sweep_expire_failed", "match_id", mid,
						"expired", expired, "err", eerr)
				}
			}
			continue
		}
		var podName string
		var staleIndexOnly bool
		var endedSkip bool
		var firstAbandon bool // 本次成功事务是否执行了 →abandoned 的首次迁移(全局恰好一次,见闭包内注释)
		var playerIDs []uint64
		var mapID uint32
		var gameMode string
		// KEEPTTL:标记 abandoned / 每轮重试不刷新 battle key TTL,保证 BattleTTL 是补偿重试上界。
		lerr := u.repo.UpdateBattleKeepTTL(ctx, mid, updateMaxRetry, func(b *dsv1.BattleStorageRecord) error {
			// 出参每轮重置:CAS 冲突时闭包基于重新 GET 的最新镜像整体重跑,以最后一次成功事务为准
			// (BattleRepo.UpdateBattleWithLock 的 fn 重跑契约)。
			staleIndexOnly = false
			endedSkip = false
			firstAbandon = false
			// active ZSET 是跨 slot 派生索引。心跳可能已成功更新权威 record，但后续
			// ZADD 失败，留下旧 score；必须在任何终态写/Release 前重新核验 record。
			// UpdateBattleKeepTTL 成功后会以该真实时间补写 ZSET，故这里只修索引、零副作用。
			if b.LastHeartbeatMs > threshold {
				staleIndexOnly = true
				return nil
			}
			if b.State == stateEnded {
				endedSkip = true      // 正常结算,移出 active 不补偿
				podName = b.DsPodName // 捕获用于 local 幽灵 DS 收尾(见下方 killStrandedDS)
				return nil
			}
			// firstAbandon 仅在本事务把状态从非 abandoned 首次写成 abandoned 时为 true。
			// WATCH CAS 保证该迁移跨副本/跨 sweep 轮次全局只成功一次:并发副本撞 TxFailedErr
			// 重跑后读到 abandoned → false;补偿重试轮次读到 abandoned → false。
			// 因此下方 Release 恰好执行一次,不存在 double-release。
			firstAbandon = b.State != stateAbandoned
			b.State = stateAbandoned
			podName = b.DsPodName
			playerIDs = b.PlayerIds
			mapID = b.MapId
			gameMode = b.GameMode
			return nil
		})
		if lerr != nil {
			if errcode.As(lerr) == errcode.ErrDSPodNotFound {
				_ = u.repo.RemoveActive(ctx, mid) // 镜像 TTL 过期:清理残留 active(补偿重试的天然上界)
				continue
			}
			plog.With(ctx).Warnw("msg", "sweep_lock_failed", "match_id", mid, "err", lerr)
			continue
		}
		if staleIndexOnly {
			plog.With(ctx).Infow("msg", "sweep_repaired_stale_index", "match_id", mid)
			continue
		}
		if endedSkip {
			// 正常结算的 DS 收尾回收(2026-07-03):battle_result 不再在结算响应路径直杀 DS(那会抢在
			// DS 通知客户端回大厅之前把它杀掉),DS 生命周期归此处兜底。DS 发完 ended 心跳后即
			// StopBattleHeartbeat(无第二跳,心跳终态 killStrandedDS 永不触发),且 local 模式 DS 的
			// Agones Shutdown 是 no-op → 进程不会自退。故这里在 ended 且失联(≥HeartbeatTimeout 未心跳,
			// 此时 DS 早已通知客户端回大厅)时主动 taskkill,防幽灵 DS 占端口耗尽端口池。
			// killOrphanOnStop 门控:仅 local 打开;Agones 关(DS 已自身 Shutdown,pod 交 Fleet 回收)。
			u.killStrandedDS(ctx, mid, podName, "ended")
			_ = u.repo.RemoveActive(ctx, mid)
			continue
		}
		// 仅首次迁移 abandoned 的赢家事务回收 pod(并发副本 / 补偿重试轮次 firstAbandon=false 跳过,
		// 不会重复 Release;Release 本身对已消失的 GameServer 幂等,双重保险)。
		if firstAbandon {
			if rerr := u.alloc.Release(ctx, podName); rerr != nil {
				plog.With(ctx).Warnw("msg", "sweep_release_failed", "match_id", mid, "pod", podName, "err", rerr)
			}
			plog.With(ctx).Infow("msg", "battle_abandoned_heartbeat_timeout", "match_id", mid, "pod", podName)
		}
		// 投递 abandoned 补偿事件:成功(或显式 local/off 开发回退)才移出 active;
		// 失败则保留在 active,下一轮 sweep 重试(可靠补偿,不变量 §4)。
		if u.deliverAbandoned(ctx, mid, podName, playerIDs, mapID, gameMode) {
			// 终态镜像保留一段供查询,移出 active 不再扫描
			if eerr := u.repo.ExpireBattle(ctx, mid, u.battleTTL()); eerr != nil {
				plog.With(ctx).Warnw("msg", "sweep_expire_failed", "match_id", mid, "err", eerr)
			}
		}
	}
	return nil
}

func (u *AllocatorUsecase) reconcileActiveIndexIfDue(ctx context.Context) error {
	if u.activeIndexReconciler == nil {
		if u.modelB {
			return errcode.New(errcode.ErrInvalidState,
				"canonical battle active-index reconciler unavailable")
		}
		return nil
	}
	now := time.Now()
	if !u.lastActiveIndexReconcile.IsZero() && activeIndexReconcileInterval > 0 &&
		now.Sub(u.lastActiveIndexReconcile) < activeIndexReconcileInterval {
		return nil
	}
	if err := u.activeIndexReconciler.ReconcileBattleActiveIndex(ctx, 256); err != nil {
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"rebuild canonical battle active index")
	}
	u.lastActiveIndexReconcile = now
	return nil
}

// deliverAbandoned 发 DSLifecycleEvent{phase=ABANDONED} 给 battle_result 做玩家段位回滚补偿。
//
// 返回值语义(给 sweepOnce 决定是否移出 active):
//   - true  → 可移出 active:已成功投递,或显式 local/off 开发配置未接 Kafka。
//   - false → 投递失败,保留在 active 下一轮 sweep 重试(可靠补偿,不变量 §4)。
//
// 生产 required 但 publisher 意外为 nil 时必须返回 false：这是一道独立于 main 启动
// 校验的 fail-closed 保险，禁止 abandoned 在没有 match release / exit proof 时被过期。
func (u *AllocatorUsecase) deliverAbandoned(ctx context.Context, matchID uint64, podName string, playerIDs []uint64, mapID uint32, gameMode string) bool {
	if u.lifecycle == nil {
		if u.lifecycleRequired {
			plog.With(ctx).Errorw("msg", "ds_lifecycle_publisher_missing_fail_closed",
				"match_id", matchID, "hint", "retain active recovery outbox; restart with a healthy Kafka producer")
			return false
		}
		plog.With(ctx).Warnw("msg", "ds_lifecycle_disabled_dev_best_effort",
			"match_id", matchID, "hint", "local/off development only; no battle_result recovery event will be produced")
		return true
	}
	evt := &dsv1.DSLifecycleEvent{
		MatchId:   matchID,
		DsPodName: podName,
		Phase:     dsv1.DSLifecyclePhase_DS_LIFECYCLE_PHASE_ABANDONED,
		PlayerIds: playerIDs,
		MapId:     mapID,
		GameMode:  gameMode,
		TsMs:      time.Now().UnixMilli(),
	}
	if err := u.lifecycle.PublishLifecycle(ctx, evt); err != nil {
		// 保留在 active,下轮 sweep 重试(穿越 Kafka 临时不可用)
		plog.With(ctx).Warnw("msg", "ds_lifecycle_publish_failed_will_retry", "match_id", matchID, "err", err)
		return false
	}
	plog.With(ctx).Debugw("msg", "ds_lifecycle_published", "match_id", matchID)
	return true
}
