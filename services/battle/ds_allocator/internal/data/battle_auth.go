// battle_auth.go 实现 Battle DS Model B 的 Redis 单一授权权威。
//
// 三把权威键共享 {match_id} hashtag，因此 Prepare/Stage/Activate/Delete 都能在
// Redis Cluster 的单个 slot 内用 WATCH/MULTI/EXEC 建立线性化点：
//
//	pandora:ds:battle:{<match_id>}   BattleStorageRecord
//	pandora:ds:auth:{<match_id>}     BattleDSAuthStorageRecord
//	pandora:ds:authgen:{<match_id>}  永不过期的凭据代际高水位计数器
//	pandora:ds:result-receipt:{<match_id>} battle_result 已落库的完整凭据
//
// K8s annotation 只负责投递，不参与任何授权判断。read-modify-write 使用默认
// proto.Unmarshal，保留滚动更新期间旧 writer 不认识的 unknown fields。
package data

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/dsauthrecord"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	dsv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/ds/v1"
)

const (
	// BattleDSWriterEpochV2 是 Model B writer 的机械激活代际。旧 writer 无法生成满足
	// required_writer_epoch=2 的记录，也无法在激活后回写/授权。
	BattleDSWriterEpochV2 uint32 = auth.DSAuthWriterEpochV2
	// 高并发 Allocate/Heartbeat 会同时争用单个 match 的三把同槽键；64 次只是
	// WATCH 冲突后的重新读取，不重放任何外部副作用。
	battleAuthCASRetries = 64
)

func battleAuthKey(matchID uint64) string {
	return fmt.Sprintf("pandora:ds:auth:{%d}", matchID)
}

func battleAuthGenKey(matchID uint64) string {
	return fmt.Sprintf("pandora:ds:authgen:{%d}", matchID)
}

// BattleCredentialIdentity 是中间件验签后交给权威仓的完整凭据身份。ExpMs/Kid/Hash
// 也属于身份，禁止退化成只比较 gen/jti。
type BattleCredentialIdentity struct {
	PodName       string
	InstanceUID   string
	InstanceEpoch uint32
	Gen           uint64
	JTI           string
	ExpMs         uint64
	Kid           string
	TokenSHA256   string
	WriterEpoch   uint32
}

// BattleAuthorityBinding 把一个 allocation_id 钉到一个真实 GameServer UID。
type BattleAuthorityBinding struct {
	MatchID             uint64
	AllocationID        string
	PodName             string
	InstanceUID         string
	RequiredWriterEpoch uint32
	AuthTTL             time.Duration
	BattleTTL           time.Duration
}

// BattleCredentialSeed 是签发 JWT 前从 Redis 领取的实例纪元和全局单调代际。
type BattleCredentialSeed struct {
	InstanceEpoch uint32
	Gen           uint64
}

// BattleExpectedInstance 是终止/清理 fencing token。allocation_id 防旧请求，UID+epoch
// 防同名 GameServer 重建；三者必须与 Redis auth+battle 绑定同时匹配。
type BattleExpectedInstance struct {
	AllocationID  string
	InstanceUID   string
	InstanceEpoch uint32
}

// BattleResultAuthorizationProof 是 battle_result 已在服务端完成 Guard + Redis active
// 校验并持久化进 MySQL outbox 的证明。AuthorizedAtMs 必须早于 ExpMs；relay 当前时刻
// 可以晚于 ExpMs。普通 credential gen/jti 轮换不改变 stable GameServer identity，
// 因而不会阻断已提交结算的资源回收。
type BattleResultAuthorizationProof struct {
	Credential     BattleCredentialIdentity
	AuthorizedAtMs int64
}

// BattleQuarantineExpected 防旧运维请求误隔离同名重建实例：allocation_id 与完整 active
// credential 必须在同一事务中仍匹配，才允许紧急吊销。
type BattleQuarantineExpected struct {
	AllocationID string
	Credential   BattleCredentialIdentity
}

// BattleQuarantineResult 区分 Redis 唯一授权权威吊销与派生 battle 投影补偿。
// AuthQuarantined=true 后泄露 token 已失效；ProjectionAbandoned=false 只表示投影需独立审计。
type BattleQuarantineResult struct {
	AuthQuarantined     bool
	ProjectionAbandoned bool
}

// BattleStageInput 是签发完成后暂存 pending 凭据的输入。
type BattleStageInput struct {
	MatchID      uint64
	AllocationID string
	Credential   *dsv1.BattleDSCredential
	AuthTTL      time.Duration
}

// BattleHeartbeatInput 是已验签 DS 心跳的业务负载。TsMs 故意不存在：权威心跳时间只取
// Redis writer 的服务端接收时间，客户端时间只能在仓外做遥测。
type BattleHeartbeatInput struct {
	PlayerCount        int32
	State              string
	AuthTTL            time.Duration
	BattleTTL          time.Duration
	EmptyBattleTimeout time.Duration
	// NoShowTimeout 是「从未连入」(BattleStorageRecord.ever_had_players=false)局的空场
	// 回收阈值,由 conf.AllocatorConf.ResolveNoShowTimeout 解析(已含钳制与禁用语义)。
	// 与 EmptyBattleTimeout 二选一,选择依据只看 ever_had_players,不看当前 PlayerCount:
	// 有人连入过的局即便此刻空了也要给断线重连留路(长阈值),从未连入的局没人要回来(短阈值)。
	// 零值 = 不区分,退化为 EmptyBattleTimeout 单阈值(改动前行为)。
	NoShowTimeout time.Duration
	// StabilityBeats/StabilitySpanMs 是两阶段激活稳定性门(INC-20260727-001 第三 P0,
	// 推导见 conf.ActivationStabilityBeats):staged→ACTIVE 提升要求 ≥Beats 次实收
	// 心跳且首尾跨度 ≥SpanMs。Beats≤1 且 SpanMs≤0 时门关闭(零值兼容旧行为)。
	StabilityBeats  int
	StabilitySpanMs int64
	// RosterJoinDeadline 花名册到齐期限(INC-20260813-001),0 = 关闭本闸。
	// 上面两个空场阈值都只管「一个人都没有」,拦不住「来了但没来齐」。
	RosterJoinDeadline time.Duration
	// RosterJoinArmWindow 是本闸「还允不允许被武装」的时间窗(自 allocated_at 起算)。
	// 滚动升级中新副本接手一局**老** battle 时,过窗即不动手 —— 否则会把「局中掉线」
	// 误判成「开局没到齐」,判弃一场正在打的对局(推导见 conf.ResolveRosterJoinArmWindow)。
	RosterJoinArmWindow time.Duration
	// CensusPresent / ActivePlayerIDs 是 DS 上报的真实在场名单。
	// **CensusPresent=false 时本闸整道跳过** —— 拿 PlayerCount 硬猜既说不出缺的是谁
	// (就没法只罚缺席者),也会在 legacy / local-off-v1 档把每一局都判成缺员。
	CensusPresent   bool
	ActivePlayerIDs []uint64
}

// BattleActivateResult 同时承载 pending→active ACK 和事务提交后的 Battle 镜像。
type BattleActivateResult struct {
	// ActivationPending:staged 心跳已实收但稳定性证据不足,未提升 ACTIVE、零状态
	// 转移(battle 保持 warming);响应不得携带 ACK,DS 每 tick 幂等重试。
	ActivationPending bool
	FirstActivation   bool
	// FirstAbandon 只在本事务首次把 active battle 推进为 abandoned 时为 true；
	// 外层仅赢家执行一次 Pod 回收，补偿投递仍可由 sweep 幂等重试。
	FirstAbandon bool
	// RosterIncomplete 表示本次判弃的原因是「花名册没到齐」(INC-20260813-001),
	// 而不是空场。外层据此改记罚对象:只罚缺席者,不罚按时到场的人。
	RosterIncomplete bool
	Terminal         bool
	HeartbeatMs      int64
	Active           BattleCredentialIdentity
	Battle           *dsv1.BattleStorageRecord
}

// BattleAbandonResult 是 sweep 原子 stale 判定与终止结果。
type BattleAbandonResult struct {
	Abandoned       bool
	AlreadyTerminal bool
	AuthFound       bool
	ActiveFound     bool
	Battle          *dsv1.BattleStorageRecord
}

// BattleStaleCutoffs 是 sweep 判弃的双阈值(均为 UnixMilli,早于该值视为失联):
//   - ActiveHeartbeatMs:已激活(auth.Active 存在)或 allocating 快速清理路径,
//     对应 HeartbeatTimeout(不变量 §4,默认 15s)。
//   - WarmingHeartbeatMs:尚未激活的 warming 冷加载窗口(大图 ServerTravel 到
//     GameMode BeginPlay 前没有业务心跳是正常行为),对应 ReadyWaitTimeout,
//     与 AllocateBattle 在途的 waitBattleReady 同一口径。
//
// 选择必须由 AbandonIfStale 在 WATCH auth+battle 事务内依据同一权威快照做:
// 外层 sweep 读到的 State 快照会与首次 ActivateHeartbeat 并发(TOCTOU),
// 不得用它决定单一阈值。
type BattleStaleCutoffs struct {
	ActiveHeartbeatMs  int64
	WarmingHeartbeatMs int64
	// WarmingForfeit 非空时表示编排层已权威确认某 exact 实例死亡,warming 可放弃
	// 时间宽限提前判弃 —— 但判死结果只对被探测的那个实例有效(见其注释)。
	WarmingForfeit *BattleWarmingForfeit
}

// BattleWarmingForfeit 把「编排层判死」绑定到 exact 分配身份,防 ABA:probe 探的是
// 旧分配 A,事务执行时同 match_id 可能已换成新分配 B(A 被清理→matchmaker 重试)。
// 事务内 battle 的 allocation_id+gameserver_uid+instance_epoch 与 Instance 精确一致
// 时才允许用 HeartbeatMs(判死时刻)替代常规冷加载宽限;身份不一致即视为旧 probe
// 结果作废,B 仍按常规宽限判断,绝不被 A 的判死误杀。
type BattleWarmingForfeit struct {
	Instance    BattleExpectedInstance
	HeartbeatMs int64
}

// warmingCutoffFor 在事务内按 battle 精确身份选择 warming 阈值(ABA 防护本体)。
func (c BattleStaleCutoffs) warmingCutoffFor(battle *dsv1.BattleStorageRecord) int64 {
	if f := c.WarmingForfeit; f != nil &&
		battle.GetAllocationId() == f.Instance.AllocationID &&
		battle.GetGameserverUid() == f.Instance.InstanceUID &&
		battle.GetInstanceEpoch() == f.Instance.InstanceEpoch {
		return f.HeartbeatMs
	}
	return c.WarmingHeartbeatMs
}

// BattleAuthoritySnapshot 是 auth+battle 的同一 Redis 快照。
type BattleAuthoritySnapshot struct {
	Auth        *dsv1.BattleDSAuthStorageRecord
	Battle      *dsv1.BattleStorageRecord
	AuthFound   bool
	BattleFound bool
}

// ReadyAuthorized 是 waitBattleReady 的最终分配门：授权 active、实例/投影/心跳完全一致、
// battle ready/running 且服务端心跳新鲜。任一字段缺失均 fail-closed。
func (s BattleAuthoritySnapshot) ReadyAuthorized(nowMs, maxHeartbeatAgeMs int64) (bool, string) {
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	if ok, reason := s.activeProjectionConsistent(nowMs); !ok {
		return false, reason
	}
	if s.Battle.State != "ready" && s.Battle.State != "running" {
		return false, "battle-not-ready"
	}
	// Model B 不再用“必须严格晚 1ms”区分初始化时间和真实心跳；active 投影与
	// auth.last_active_heartbeat_ms 已证明这确实来自一次授权心跳，同毫秒也合法。
	if s.Battle.LastHeartbeatMs < s.Battle.AllocatedAtMs {
		return false, "no-post-allocation-heartbeat"
	}
	if maxHeartbeatAgeMs > 0 && nowMs-s.Auth.LastActiveHeartbeatMs > maxHeartbeatAgeMs {
		return false, "heartbeat-stale"
	}
	return true, ""
}

// HeartbeatFresh 用于 sweep 命中陈旧 ZSET member 后二次核验 Redis 权威记录。
func (s BattleAuthoritySnapshot) HeartbeatFresh(nowMs, thresholdMs int64) bool {
	if nowMs <= 0 {
		nowMs = time.Now().UnixMilli()
	}
	ok, _ := s.activeProjectionConsistent(nowMs)
	return ok && s.Auth.LastActiveHeartbeatMs > thresholdMs
}

func (s BattleAuthoritySnapshot) activeProjectionConsistent(nowMs int64) (bool, string) {
	if !s.AuthFound || s.Auth == nil {
		return false, "auth-missing"
	}
	if !s.BattleFound || s.Battle == nil {
		return false, "battle-missing"
	}
	a, b := s.Auth, s.Battle
	if a.MatchId == 0 || a.MatchId != b.MatchId {
		return false, "match-mismatch"
	}
	if a.AllocationId == "" || a.AllocationId != b.AllocationId {
		return false, "allocation-mismatch"
	}
	if a.DsPodName == "" || a.DsPodName != b.DsPodName {
		return false, "pod-mismatch"
	}
	if a.InstanceUid == "" || a.InstanceUid != b.GameserverUid ||
		a.InstanceEpoch == 0 || a.InstanceEpoch != b.InstanceEpoch {
		return false, "instance-mismatch"
	}
	if a.Phase != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE &&
		a.Phase != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING {
		return false, "phase-not-active"
	}
	if !battleCredentialComplete(a.Active, a, nowMs) || a.HighWaterGen < a.Active.Gen {
		return false, "active-incomplete"
	}
	if b.LastVerifiedGen != a.Active.Gen || b.LastVerifiedJti != a.Active.Jti ||
		b.LastVerifiedWriterEpoch != a.Active.WriterEpoch {
		return false, "projection-mismatch"
	}
	if a.LastActiveHeartbeatMs <= 0 || a.LastActiveHeartbeatMs > nowMs ||
		b.LastHeartbeatMs != a.LastActiveHeartbeatMs {
		return false, "heartbeat-mismatch"
	}
	return true, ""
}

// BattleAuthRepo 是 Battle Model B 权威仓接口。
type BattleAuthRepo interface {
	// PrepareCredential 原子绑定当前 GameServer UID、决定 instance_epoch，并从永不过期的
	// counter 领取一个严格递增 gen。返回后签发失败允许跳号，绝不回收/复用。
	PrepareCredential(context.Context, BattleAuthorityBinding) (BattleCredentialSeed, error)
	StagePending(context.Context, BattleStageInput) (*dsv1.BattleDSAuthStorageRecord, error)
	MarkDelivered(context.Context, uint64, string, *dsv1.BattleDSCredential, string, time.Duration) error
	ActivateHeartbeat(context.Context, uint64, BattleCredentialIdentity, BattleHeartbeatInput) (BattleActivateResult, error)
	ReadAuthority(context.Context, uint64) (BattleAuthoritySnapshot, error)
	CheckActive(context.Context, uint64, BattleCredentialIdentity) error
	// PopCommandsIfActive 把 active 校验与 RPOP 压进同一个事务，消除校验后轮换/吊销
	// 仍然弹出 GM 命令的 TOCTOU 窗口。queueKey 必须与 auth key 共享 {match_id} slot。
	PopCommandsIfActive(context.Context, uint64, BattleCredentialIdentity, string, int64) ([]string, error)
	CheckHeartbeatFresh(context.Context, uint64, int64) (bool, error)
	AbandonIfStale(context.Context, uint64, BattleStaleCutoffs, time.Duration, time.Duration) (BattleAbandonResult, error)
	TerminateExpected(context.Context, uint64, BattleExpectedInstance, string, time.Duration, time.Duration) (bool, error)
	// TerminateResultExpected 只接受 MySQL terminal-release outbox 的持久证明；它把
	// receipt 与 ended 终态在同一 Redis CAS 写成永久墓碑。当前 gen/jti 可已轮换，
	// stable identity 与 writer fence 必须仍精确一致。
	TerminateResultExpected(context.Context, uint64, BattleExpectedInstance, BattleResultAuthorizationProof) (bool, error)
	// ExpireResultTerminatedExpected 只能在 UID 条件 Release 明确成功后给上述永久墓碑
	// 恢复有界审计 TTL。response unknown 时墓碑保持永久，供 outbox 重试重认。
	ExpireResultTerminatedExpected(context.Context, uint64, BattleExpectedInstance, BattleResultAuthorizationProof, time.Duration) (bool, error)
	QuarantineExpected(context.Context, uint64, BattleQuarantineExpected, time.Duration, time.Duration) (BattleQuarantineResult, error)
	// FencePreactiveReleaseExpected 在任何外部 ReleaseExpected 前把未激活实例原子锁成
	// 永久 release-pending；ACTIVE/ready 赢家不可进入该状态。
	FencePreactiveReleaseExpected(context.Context, uint64, BattleExpectedInstance) (bool, error)
	// PurgePreactiveReleasedExpected 只供外部 UID 条件删除已被明确确认成功后调用。
	PurgePreactiveReleasedExpected(context.Context, uint64, BattleExpectedInstance) (bool, error)
	// PurgeTerminatedExpected 只物理删除已由 TerminateExpected 锁死的同一 allocation。
	PurgeTerminatedExpected(context.Context, uint64, BattleExpectedInstance) (bool, error)
	// ExpireTerminatedExpected 只在外部 release 与可靠 lifecycle 投递都明确成功后，
	// 给永久终止墓碑恢复有界保留期。
	ExpireTerminatedExpected(context.Context, uint64, BattleExpectedInstance, time.Duration, time.Duration) (bool, error)
}

// RedisBattleAuthRepo 是 BattleAuthRepo 的 Redis 实现。
type RedisBattleAuthRepo struct {
	rdb                redis.UniversalClient
	now                func() time.Time
	strictModelBWrites atomic.Bool
}

func NewRedisBattleAuthRepo(rdb redis.UniversalClient) *RedisBattleAuthRepo {
	return &RedisBattleAuthRepo{rdb: rdb, now: time.Now}
}

func (r *RedisBattleAuthRepo) EnableStrictModelBWrites() { r.strictModelBWrites.Store(true) }

func (r *RedisBattleAuthRepo) StrictModelBWritesEnabled() bool {
	return r != nil && r.strictModelBWrites.Load()
}

func (r *RedisBattleAuthRepo) marshalBattleTransition(previous, next *dsv1.BattleStorageRecord) ([]byte, error) {
	if r.StrictModelBWritesEnabled() {
		return marshalBattleTransition(previous, next)
	}
	return proto.Marshal(next)
}

var errBattleAuthStale = errcode.New(errcode.ErrUnauthorized, "battle ds credential not authoritative")

// battleAuthStale 打一条带**枚举 reason** 的授权拒绝日志,再返回同一个 errBattleAuthStale。
//
// 为什么必须有:ActivateHeartbeat 里有近十条互不相同的 fencing 规则,全部收敛成同一个
// errBattleAuthStale(= ErrUnauthorized)。而 ErrUnauthorized 不属 errcode.IsServerFault,
// pkg/middleware/logging.go 只把它记成 rpc_ok=DEBUG —— 线上默认 info 级下,
// 「DS 的心跳被拒了」和「为什么被拒」**两件事都看不见**。这正是 §11.3 R2 点名的形态:
// "一个 if 收敛了 N 个条件的,必须拆成 N 个 reason"。
//
// 返回值与行为**完全不变**(同一个 sentinel,同一个错误码),只多一条日志。
// 每个拒绝点至多打一条:bizErr 一旦置位,外层 CAS 循环立即 return,不会因 WATCH 重跑重复。
func battleAuthStale(
	ctx context.Context, matchID uint64, id BattleCredentialIdentity, reason string, kv ...any,
) error {
	fields := []any{
		"msg", "battle_ds_auth_rejected", "match_id", matchID, "reason", reason,
		"pod", id.PodName, "uid", id.InstanceUID, "epoch", id.InstanceEpoch,
		"gen", id.Gen, "jti", id.JTI, "writer_epoch", id.WriterEpoch,
	}
	plog.With(ctx).Warnw(append(fields, kv...)...)
	return errBattleAuthStale
}

// ActivateHeartbeat 的授权拒绝 reason 枚举(§11.3 R2)。命名对齐各自的判定依据,
// 一条规则一个值,snake_case 稳定不变。
const (
	authRejectIdentityInvalid    = "credential_identity_invalid"   // 五元组不全 / 已过期
	authRejectBindingMismatch    = "authority_binding_mismatch"    // auth↔battle 绑定或 pod/uid/epoch/writer_epoch 对不上
	authRejectTerminatingPhase   = "auth_terminating_non_terminal" // TERMINATING 但对局非终态或凭据不匹配
	authRejectPhaseLocked        = "auth_phase_locked"             // QUARANTINED 等锁定相位
	authRejectNoUsableCredential = "no_usable_credential"          // 相位/pending/active 组合都不构成可用凭据
	authRejectGenBelowHighWater  = "gen_below_high_water"          // 代际低于高水位(旧凭据重放)
	authRejectPromoteOnTerminal  = "promote_on_terminal_battle"    // 终态对局上提升新凭据
	authRejectStateNotReadyRun   = "promote_state_not_ready"       // 首次激活自报的 state 不是 ready/running
)

var errBattleResultNotRecorded = errcode.New(errcode.ErrInvalidState, "battle result is not authoritatively recorded")
var errBattleResultCommitted = errcode.New(errcode.ErrInvalidState, "battle result already committed; credential rotation is fenced")

func battleAuthPhaseLocked(p dsv1.BattleAuthPhase) bool {
	return p == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_QUARANTINED ||
		p == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING
}

func battleAuthPhaseStageable(p dsv1.BattleAuthPhase) bool {
	return p == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_BOOTSTRAP ||
		p == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE ||
		p == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING
}

// PrepareCredential 在 auth+battle+counter 同槽事务内完成实例绑定与取号。
func (r *RedisBattleAuthRepo) PrepareCredential(ctx context.Context, in BattleAuthorityBinding) (BattleCredentialSeed, error) {
	if err := validateBattleBinding(in); err != nil {
		return BattleCredentialSeed{}, err
	}
	aKey, bKey, gKey := battleAuthKey(in.MatchID), battleKey(in.MatchID), battleAuthGenKey(in.MatchID)
	rKey := dsauthrecord.BattleResultReceiptKey(in.MatchID)
	var out BattleCredentialSeed
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		var bizErr error
		out = BattleCredentialSeed{}
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			receiptExists, receiptErr := tx.Exists(ctx, rKey).Result()
			if receiptErr != nil {
				return receiptErr
			}
			if receiptExists != 0 {
				bizErr = errBattleResultCommitted
				return bizErr
			}
			battle, err := readBattleFrom(tx, ctx, in.MatchID, bKey)
			if err != nil {
				bizErr = err
				return err
			}
			battleBefore := proto.Clone(battle).(*dsv1.BattleStorageRecord)
			if battle.AllocationId != in.AllocationID || battle.DsPodName != in.PodName ||
				(battle.GameserverUid != "" && battle.GameserverUid != in.InstanceUID) ||
				!battleCredentialPreparableState(battle.State) {
				bizErr = errBattleAuthStale
				return bizErr
			}

			auth := &dsv1.BattleDSAuthStorageRecord{}
			ab, aerr := tx.Get(ctx, aKey).Bytes()
			switch {
			case aerr == redis.Nil:
				auth.MatchId = in.MatchID
				auth.DsPodName = in.PodName
				auth.InstanceUid = in.InstanceUID
				auth.InstanceEpoch = 1
				auth.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_BOOTSTRAP
				auth.AllocationId = in.AllocationID
			case aerr != nil:
				return aerr
			default:
				if err := unmarshalBattleAuth(in.MatchID, ab, auth); err != nil {
					return err
				}
				if auth.RequiredWriterEpoch != BattleDSWriterEpochV2 || !battleStoredCredentialEpochsV2(auth) {
					bizErr = errBattleAuthStale
					return bizErr
				}
				// QUARANTINED/TERMINATING 是实例级永久墓碑。必须先于
				// sameInstance 判断拒绝，否则同 match 下伪造一个不同 UID 会走
				// “换实例”分支清空 tombstone，重新开放凭据签发。
				if battleAuthPhaseLocked(auth.Phase) {
					bizErr = errBattleAuthStale
					return bizErr
				}
				sameInstance := auth.AllocationId == in.AllocationID && auth.DsPodName == in.PodName &&
					auth.InstanceUid == in.InstanceUID
				if !sameInstance {
					auth.InstanceEpoch++
					if auth.InstanceEpoch == 0 {
						return errcode.New(errcode.ErrInvalidState, "battle %d instance epoch overflow", in.MatchID)
					}
					auth.MatchId = in.MatchID
					auth.DsPodName = in.PodName
					auth.InstanceUid = in.InstanceUID
					auth.AllocationId = in.AllocationID
					auth.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_BOOTSTRAP
					auth.Active = nil
					auth.Pending = nil
					auth.PendingStartedMs = 0
					auth.DeliveredRv = ""
					auth.LastActiveHeartbeatMs = 0
				} else if !battleAuthPhaseStageable(auth.Phase) {
					bizErr = errBattleAuthStale
					return bizErr
				}
			}
			if auth.InstanceEpoch == 0 {
				auth.InstanceEpoch = 1
			}
			if auth.RequiredWriterEpoch == 0 {
				// 仅 auth key 不存在的首建路径可写入当前精确 epoch；已存在的低/未来
				// record 已在上方拒绝，不能由普通分配路径承担隐式迁移。
				auth.RequiredWriterEpoch = in.RequiredWriterEpoch
			}
			if auth.RequiredWriterEpoch != BattleDSWriterEpochV2 {
				bizErr = errBattleAuthStale
				return bizErr
			}

			counter := uint64(0)
			gv, gerr := tx.Get(ctx, gKey).Result()
			if gerr != nil && gerr != redis.Nil {
				return gerr
			}
			if gerr == nil {
				counter, err = strconv.ParseUint(gv, 10, 64)
				if err != nil {
					return fmt.Errorf("battle %d bad auth generation counter %q: %w", in.MatchID, gv, err)
				}
			}
			if counter < auth.HighWaterGen {
				counter = auth.HighWaterGen
			}
			if counter == ^uint64(0) {
				return errcode.New(errcode.ErrInvalidState, "battle %d credential generation overflow", in.MatchID)
			}
			counter++

			nowMs := r.now().UnixMilli()
			auth.UpdatedAtMs = nowMs
			battle.GameserverUid = in.InstanceUID
			battle.InstanceEpoch = auth.InstanceEpoch
			// 换实例时必须清掉旧 active 投影；否则 warming 镜像会短暂伪装成已验证实例。
			if battle.LastVerifiedGen != 0 && (battle.LastVerifiedGen != auth.GetActive().GetGen() ||
				battle.GameserverUid != auth.InstanceUid) {
				battle.LastVerifiedGen = 0
				battle.LastVerifiedJti = ""
				battle.LastVerifiedWriterEpoch = 0
			}
			aPayload, err := proto.Marshal(auth)
			if err != nil {
				return err
			}
			bPayload, err := r.marshalBattleTransition(battleBefore, battle)
			if err != nil {
				return err
			}
			preactive := battlePreactiveAuthority(auth, battle)
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				aTTL, bTTL := in.AuthTTL, in.BattleTTL
				if preactive {
					// 从 GSA POST 前的永久 uncertain fence 到首个 active 心跳之间，
					// 任何 auth/battle 写都必须继续无 TTL。否则 Stage/PATCH/进程崩溃
					// 后两键过期，会重新开放同 match 的第二次 GSA POST。
					aTTL, bTTL = 0, 0
				}
				pipe.Set(ctx, aKey, aPayload, aTTL)
				pipe.Set(ctx, bKey, bPayload, bTTL)
				// 代际 counter 故意不设 TTL；auth 过期/删除也不能使 gen 回退。
				pipe.Set(ctx, gKey, counter, 0)
				return nil
			})
			if err == nil {
				out = BattleCredentialSeed{InstanceEpoch: auth.InstanceEpoch, Gen: counter}
			}
			return err
		}, aKey, bKey, gKey, rKey)
		if txErr == nil {
			return out, nil
		}
		if bizErr != nil {
			return BattleCredentialSeed{}, bizErr
		}
		if txErr == redis.TxFailedErr {
			continue
		}
		return BattleCredentialSeed{}, txErr
	}
	return BattleCredentialSeed{}, errcode.New(errcode.ErrInternal, "battle %d prepare credential cas retry exhausted", in.MatchID)
}

func (r *RedisBattleAuthRepo) StagePending(ctx context.Context, in BattleStageInput) (*dsv1.BattleDSAuthStorageRecord, error) {
	nowMs := r.now().UnixMilli()
	if in.MatchID == 0 || in.AllocationID == "" || in.AuthTTL <= 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "battle auth stage requires match/allocation/auth_ttl")
	}
	if err := validateBattleCredential(in.Credential, nowMs); err != nil {
		return nil, err
	}
	aKey, bKey, gKey := battleAuthKey(in.MatchID), battleKey(in.MatchID), battleAuthGenKey(in.MatchID)
	rKey := dsauthrecord.BattleResultReceiptKey(in.MatchID)
	var out *dsv1.BattleDSAuthStorageRecord
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		var bizErr error
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			auth, battle, err := readBoundAuthority(tx, ctx, in.MatchID, aKey, bKey)
			if err != nil {
				bizErr = err
				return err
			}
			if auth.AllocationId != in.AllocationID || !authorityBindingMatches(auth, battle) ||
				battleTerminal(battle.State) || !battleAuthPhaseStageable(auth.Phase) ||
				auth.InstanceUid != in.Credential.InstanceUid || auth.InstanceEpoch != in.Credential.InstanceEpoch ||
				in.Credential.WriterEpoch != BattleDSWriterEpochV2 ||
				!battleAuthRecordV2Exact(auth) {
				bizErr = errBattleAuthStale
				return bizErr
			}
			receiptExists, receiptErr := tx.Exists(ctx, rKey).Result()
			if receiptErr != nil {
				return receiptErr
			}
			if receiptExists != 0 && !battleCredentialEqual(auth.Active, in.Credential) {
				bizErr = errBattleResultCommitted
				return bizErr
			}
			// 响应丢失可幂等重试；已经被同一凭据激活也视为此前 Stage 成功。
			if battleCredentialEqual(auth.Pending, in.Credential) || battleCredentialEqual(auth.Active, in.Credential) {
				out = proto.Clone(auth).(*dsv1.BattleDSAuthStorageRecord)
				_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					if battlePreactiveAuthority(auth, battle) {
						// 兼容事务响应丢失后的幂等重入：即使前一次写来自旧的
						// 有限 TTL 实现，也先把两键恢复为永久 fence 再返回成功。
						pipe.Persist(ctx, aKey)
						pipe.Persist(ctx, bKey)
					} else {
						pipe.Exists(ctx, aKey, bKey)
					}
					return nil
				})
				return err
			}
			// counter 永不过期并代表本 match 已领取的最新签发号。auth 曾被失败清理删除时，
			// 仅比较新建记录的 high_water=0 会让旧 gen 重放；要求待 Stage 的 gen 正好是
			// 最新领取号，响应丢失后重新 Prepare 会自然淘汰更旧但尚未 Stage 的 candidate。
			gv, gerr := tx.Get(ctx, gKey).Uint64()
			if gerr != nil || gv != in.Credential.Gen {
				bizErr = errBattleAuthStale
				return bizErr
			}
			if in.Credential.Gen <= auth.HighWaterGen ||
				(auth.Active != nil && in.Credential.Gen <= auth.Active.Gen) {
				bizErr = errBattleAuthStale
				return bizErr
			}
			auth.Pending = proto.Clone(in.Credential).(*dsv1.BattleDSCredential)
			auth.HighWaterGen = in.Credential.Gen
			auth.PendingStartedMs = nowMs
			auth.DeliveredRv = ""
			if auth.Active == nil {
				auth.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_BOOTSTRAP
			} else {
				auth.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING
			}
			auth.UpdatedAtMs = nowMs
			payload, err := proto.Marshal(auth)
			if err != nil {
				return err
			}
			ttl := in.AuthTTL
			if battlePreactiveAuthority(auth, battle) {
				ttl = 0
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, aKey, payload, ttl)
				if ttl == 0 {
					pipe.Persist(ctx, bKey)
				}
				return nil
			})
			if err == nil {
				out = proto.Clone(auth).(*dsv1.BattleDSAuthStorageRecord)
			}
			return err
		}, aKey, bKey, gKey, rKey)
		if txErr == nil {
			return out, nil
		}
		if bizErr != nil {
			return nil, bizErr
		}
		if txErr == redis.TxFailedErr {
			continue
		}
		return nil, txErr
	}
	return nil, errcode.New(errcode.ErrInternal, "battle %d stage pending cas retry exhausted", in.MatchID)
}

// MarkDelivered 只允许当前 expected pending 写 delivered_rv；旧 PATCH 的晚响应零变更。
func (r *RedisBattleAuthRepo) MarkDelivered(ctx context.Context, matchID uint64, allocationID string, expected *dsv1.BattleDSCredential, rv string, authTTL time.Duration) error {
	nowMs := r.now().UnixMilli()
	if matchID == 0 || allocationID == "" || rv == "" || authTTL <= 0 {
		return errcode.New(errcode.ErrInvalidArg, "battle auth mark delivered requires match/allocation/rv/auth_ttl")
	}
	if err := validateBattleCredential(expected, nowMs); err != nil {
		return err
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		var bizErr error
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			auth, battle, err := readBoundAuthority(tx, ctx, matchID, aKey, bKey)
			if err != nil {
				bizErr = err
				return err
			}
			if auth.AllocationId != allocationID || !authorityBindingMatches(auth, battle) ||
				!battleAuthRecordV2Exact(auth) ||
				!battleAuthPhaseStageable(auth.Phase) || !battleCredentialEqual(auth.Pending, expected) {
				bizErr = errBattleAuthStale
				return bizErr
			}
			auth.DeliveredRv = rv
			auth.UpdatedAtMs = nowMs
			payload, err := proto.Marshal(auth)
			if err != nil {
				return err
			}
			ttl := authTTL
			if battlePreactiveAuthority(auth, battle) {
				ttl = 0
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, aKey, payload, ttl)
				if ttl == 0 {
					pipe.Persist(ctx, bKey)
				}
				return nil
			})
			return err
		}, aKey, bKey)
		if txErr == nil {
			return nil
		}
		if bizErr != nil {
			return bizErr
		}
		if txErr == redis.TxFailedErr {
			continue
		}
		return txErr
	}
	return errcode.New(errcode.ErrInternal, "battle %d mark delivered cas retry exhausted", matchID)
}

// ActivateHeartbeat 是 pending→active、服务端心跳时刻、battle ready/投影的唯一线性化点。
// battleActivationEvidenceKey 是两阶段激活稳定性门的证据键(与 auth/battle 同槽同
// WATCH):`gen|jti|firstMs|count`。凭据身份变化(轮换/新分配)即整体重计;提升成功
// 即删除;TTL=BattleTTL 兜底防泄漏(§9.24 有界)。
func battleActivationEvidenceKey(matchID uint64) string {
	return fmt.Sprintf("pandora:ds:authstab:{%d}", matchID)
}

// battleActivationStabilityPending 在 WATCH 事务内推进激活稳定性证据并判定是否仍
// pending(INC-20260727-001 第三 P0,推导见 conf.ActivationStabilityBeats)。
// 返回 (pending, 新证据 payload, err);门关闭时恒 (false, "", nil)。
func battleActivationStabilityPending(
	tx *redis.Tx,
	ctx context.Context,
	sKey string,
	id BattleCredentialIdentity,
	in BattleHeartbeatInput,
	nowMs int64,
) (bool, string, error) {
	if in.StabilityBeats <= 1 && in.StabilitySpanMs <= 0 {
		return false, "", nil
	}
	beats := int64(in.StabilityBeats)
	if beats < 1 {
		beats = 1
	}
	firstMs, count := nowMs, int64(1)
	raw, err := tx.Get(ctx, sKey).Result()
	switch {
	case err == redis.Nil:
	case err != nil:
		return false, "", err
	default:
		parts := strings.Split(raw, "|")
		if len(parts) == 4 && parts[0] == strconv.FormatUint(id.Gen, 10) && parts[1] == id.JTI {
			if f, ferr := strconv.ParseInt(parts[2], 10, 64); ferr == nil && f > 0 && f <= nowMs {
				firstMs = f
			}
			if c, cerr := strconv.ParseInt(parts[3], 10, 64); cerr == nil && c > 0 {
				count = c + 1
			}
		}
		// 身份不匹配(凭据轮换/同名新分配):旧证据作废,从本拍重计——绝不让旧实例的
		// 心跳历史给新实例代付稳定性证据。
	}
	pending := count < beats || nowMs-firstMs < in.StabilitySpanMs
	payload := fmt.Sprintf("%d|%s|%d|%d", id.Gen, id.JTI, firstMs, count)
	return pending, payload, nil
}

func (r *RedisBattleAuthRepo) ActivateHeartbeat(ctx context.Context, matchID uint64, id BattleCredentialIdentity, in BattleHeartbeatInput) (BattleActivateResult, error) {
	serverNowMs := r.now().UnixMilli()
	if matchID == 0 || in.PlayerCount < 0 || in.AuthTTL <= 0 || in.BattleTTL <= 0 ||
		!validBattleHeartbeatState(in.State) {
		return BattleActivateResult{}, errcode.New(errcode.ErrInvalidArg, "battle heartbeat requires match/ttls/player_count/state")
	}
	if !validBattleIdentity(id, serverNowMs) {
		return BattleActivateResult{}, battleAuthStale(ctx, matchID, id, authRejectIdentityInvalid,
			"exp_ms", id.ExpMs, "server_now_ms", serverNowMs)
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	rKey := dsauthrecord.BattleResultReceiptKey(matchID)
	sKey := battleActivationEvidenceKey(matchID)
	var out BattleActivateResult
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		serverNowMs = r.now().UnixMilli()
		if !validBattleIdentity(id, serverNowMs) {
			// 重试期间凭据过期:与入口那条同 reason,靠 attempt 字段区分。
			return BattleActivateResult{}, battleAuthStale(ctx, matchID, id, authRejectIdentityInvalid,
				"exp_ms", id.ExpMs, "server_now_ms", serverNowMs, "attempt", attempt)
		}
		var bizErr error
		out = BattleActivateResult{}
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			auth, battle, err := readBoundAuthority(tx, ctx, matchID, aKey, bKey)
			if err != nil {
				bizErr = err
				return err
			}
			battleBefore := proto.Clone(battle).(*dsv1.BattleStorageRecord)
			if !authorityBindingMatches(auth, battle) ||
				auth.DsPodName != id.PodName || auth.InstanceUid != id.InstanceUID ||
				auth.InstanceEpoch != id.InstanceEpoch || id.WriterEpoch != BattleDSWriterEpochV2 ||
				!battleAuthRecordV2Exact(auth) {
				// 最常见的一条:pod 重建 / 重分配后旧 DS 还在心跳,或 writer_epoch 不是 v2。
				// 把权威侧的实际值一并打出来,才能一眼看出是哪一格对不上。
				bizErr = battleAuthStale(ctx, matchID, id, authRejectBindingMismatch,
					"auth_pod", auth.DsPodName, "auth_uid", auth.InstanceUid,
					"auth_epoch", auth.InstanceEpoch, "auth_allocation_id", auth.AllocationId,
					"battle_allocation_id", battle.GetAllocationId(), "battle_state", battle.GetState())
				return bizErr
			}
			// 终态后的同一 active 凭据允许拿到 stop/ACK，但永不续心跳或刷新 TTL；
			// QUARANTINED 及其它锁定组合仍严格拒绝。
			if auth.Phase == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING {
				if battleTerminal(battle.State) && battleCredentialMatches(auth.Active, id, serverNowMs) {
					out = activateResult(auth, battle, false, false, false, true)
					_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
						pipe.Exists(ctx, aKey, bKey)
						return nil
					})
					return err
				}
				bizErr = battleAuthStale(ctx, matchID, id, authRejectTerminatingPhase,
					"battle_state", battle.GetState())
				return bizErr
			}
			if battleAuthPhaseLocked(auth.Phase) {
				bizErr = battleAuthStale(ctx, matchID, id, authRejectPhaseLocked,
					"phase", auth.Phase.String(), "battle_state", battle.GetState(),
					"hint", "授权已被隔离/回收链锁死,该实例不可能再被授权;这台 DS 会收到 stop")
				return bizErr
			}

			promote := false
			switch {
			case (auth.Phase == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_BOOTSTRAP ||
				auth.Phase == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING) &&
				auth.DeliveredRv != "" && battleCredentialComplete(auth.Pending, auth, serverNowMs) &&
				battleCredentialMatches(auth.Pending, id, serverNowMs):
				promote = true
			case (auth.Phase == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE ||
				auth.Phase == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING) &&
				battleCredentialComplete(auth.Active, auth, serverNowMs) &&
				battleCredentialMatches(auth.Active, id, serverNowMs):
			default:
				bizErr = battleAuthStale(ctx, matchID, id, authRejectNoUsableCredential,
					"phase", auth.Phase.String(), "delivered_rv", auth.DeliveredRv,
					"has_pending", auth.GetPending() != nil, "has_active", auth.GetActive() != nil,
					"hint", "凭据既不匹配 pending 也不匹配 active:多半是凭据投递未完成或 DS 拿的是上一代")
				return bizErr
			}
			if auth.HighWaterGen < id.Gen {
				bizErr = battleAuthStale(ctx, matchID, id, authRejectGenBelowHighWater,
					"high_water_gen", auth.HighWaterGen,
					"hint", "上报代际高于权威高水位 = 凭据不可能出自本权威")
				return bizErr
			}
			if promote {
				receiptExists, receiptErr := tx.Exists(ctx, rKey).Result()
				if receiptErr != nil {
					return receiptErr
				}
				if receiptExists != 0 {
					bizErr = errBattleResultCommitted
					return bizErr
				}
			}
			if in.State == "ended" {
				recorded, receiptErr := battleResultReceiptMatches(tx, ctx, matchID, rKey, auth, battle, id, serverNowMs)
				if receiptErr != nil {
					return receiptErr
				}
				if !recorded {
					bizErr = errBattleResultNotRecorded
					return bizErr
				}
			}
			if battleTerminal(battle.State) {
				if promote {
					bizErr = battleAuthStale(ctx, matchID, id, authRejectPromoteOnTerminal,
						"battle_state", battle.State)
					return bizErr
				}
				auth.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING
				auth.UpdatedAtMs = serverNowMs
				aPayload, err := proto.Marshal(auth)
				if err != nil {
					return err
				}
				var bPayload []byte
				if battle.State == "abandoned" {
					bPayload, err = r.marshalBattleTransition(battleBefore, battle)
					if err != nil {
						return err
					}
				}
				_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					if battle.State == "abandoned" {
						pipe.Set(ctx, aKey, aPayload, 0)
						pipe.Set(ctx, bKey, bPayload, 0)
					} else {
						pipe.Set(ctx, aKey, aPayload, in.AuthTTL)
					}
					return nil
				})
				if err == nil {
					out = activateResult(auth, battle, false, false, false, true)
				}
				return err
			}
			if promote && in.State != "ready" && in.State != "running" {
				bizErr = battleAuthStale(ctx, matchID, id, authRejectStateNotReadyRun,
					"reported_state", in.State, "battle_state", battle.GetState(),
					"hint", "首次激活只接受 DS 自报 ready/running;其它值(含被白名单归一成空串的)一律拒")
				return bizErr
			}
			if promote && auth.Active == nil {
				// 两阶段激活稳定性门(INC-20260727-001 第三 P0),**只作用于首次激活**:
				// 首拍只能证明"此刻活着",证据不足时零状态转移——不提升 ACTIVE、不写
				// auth/battle、不刷新 TTL、battle 保持 warming(120s 分配宽限兜底,pending
				// 心跳仍不续命),仅原子推进同槽证据键;响应无 ACK,DS 每 tick 幂等重试
				// staged 心跳。ROTATING 轮换发生在已被持续心跳证明存活的 ACTIVE 实例上
				// (旧凭据在轮换完成前持续有效),不需要也不应该再付一次稳定性证据——
				// 拦轮换只会无谓延迟 ~span,属过度修复。
				stabilityPending, evidence, gerr := battleActivationStabilityPending(
					tx, ctx, sKey, id, in, serverNowMs)
				if gerr != nil {
					return gerr
				}
				if stabilityPending {
					_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
						pipe.Set(ctx, sKey, evidence, in.BattleTTL)
						return nil
					})
					if err == nil {
						out = BattleActivateResult{
							ActivationPending: true,
							Battle:            proto.Clone(battle).(*dsv1.BattleStorageRecord),
						}
					}
					return err
				}
			}
			// 与 promote 同一层作用域:WATCH 冲突重跑时整个闭包重来,标记天然按轮重置。
			rosterIncomplete := false
			if promote {
				auth.Active = auth.Pending
				auth.Pending = nil
				auth.PendingStartedMs = 0
				auth.DeliveredRv = ""
				auth.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE
			}
			auth.UpdatedAtMs = serverNowMs
			auth.LastActiveHeartbeatMs = serverNowMs

			previousState := battle.State
			battle.LastHeartbeatMs = serverNowMs
			battle.PlayerCount = in.PlayerCount
			if in.State != "" {
				battle.State = in.State
			}
			if battle.State == "ready" || battle.State == "running" {
				// 双阈值空场回收(2026-08-07,anti-abuse-scene-entry.md §3.2.1):
				// 从未连入(EverHadPlayers=false)的局没有“谁要回来”的问题,走短阈值;
				// 有人连入过的局必须给断线重连留路,走长阈值。EverHadPlayers 一经置位永不清零。
				//
				// NoShowTimeout<=0 一律回退 EmptyBattleTimeout:未配置差异化时必须退化成
				// 改动前的单阈值行为,绝不能让 timeout 变成 0 —— 那会让 no-show 局**永不回收**,
				// 比改动前更糟(fail-safe 方向:宁可回收得晚,不可不回收)。
				timeout := in.EmptyBattleTimeout
				if !battle.EverHadPlayers && in.NoShowTimeout > 0 {
					timeout = in.NoShowTimeout
				}
				switch {
				case in.PlayerCount > 0:
					battle.EmptySinceMs = 0
					battle.EverHadPlayers = true
				case battle.EmptySinceMs == 0:
					battle.EmptySinceMs = serverNowMs
				case timeout > 0 && serverNowMs-battle.EmptySinceMs >= timeout.Milliseconds():
					battle.State = "abandoned"
				}

				// 花名册到齐期限(INC-20260813-001)。与上面的空场两档并列:那两档看
				// PlayerCount==0,本档看 census 与 roster 的差集 —— 「6 人 roster 只进来 5 个」
				// 时 PlayerCount 恒为 5(非 0),空场计时器一次都不会起。
				//
				// 只在开局阶段生效(!RosterEverComplete):曾经全员同时在场过就永久豁免,
				// 此后的掉线交回 EmptyBattleTimeout。少了这条,局中掉线会在 deadline 后
				// 判弃一场正打着的对局 —— 比本事故严重得多。
				// 武装窗(滚动升级纵深防御):roster_ever_complete 只有**新版**副本会写。旧副本手里
				// 已到齐过的局被新副本接手时,光看标记会把「局中掉线」误判成「开局没到齐」,
				// deadline 到就判弃一场正在打的对局。按 battle 年龄判定,对任何副本同一答案。
				if battle.State != "abandoned" && !battle.RosterEverComplete &&
					in.RosterJoinDeadline > 0 && in.CensusPresent &&
					rosterGateArmable(battle.AllocatedAtMs, serverNowMs, in.RosterJoinArmWindow.Milliseconds()) {
					switch {
					case len(rosterAbsentIDs(battle.PlayerIds, in.ActivePlayerIDs)) == 0:
						battle.RosterEverComplete = true
						battle.RosterIncompleteSinceMs = 0
					case battle.RosterIncompleteSinceMs == 0:
						battle.RosterIncompleteSinceMs = serverNowMs
					case serverNowMs-battle.RosterIncompleteSinceMs >= in.RosterJoinDeadline.Milliseconds():
						battle.State = "abandoned"
						rosterIncomplete = true
					}
				}
			}
			battle.GameserverUid = auth.InstanceUid
			battle.InstanceEpoch = auth.InstanceEpoch
			battle.LastVerifiedGen = auth.Active.Gen
			battle.LastVerifiedJti = auth.Active.Jti
			battle.LastVerifiedWriterEpoch = auth.Active.WriterEpoch
			firstAbandon := previousState != "abandoned" && battle.State == "abandoned"
			terminal := battleTerminal(battle.State)
			if terminal {
				// ended/abandoned 与授权锁死必须是同一个 EXEC；否则终态窗口内 GM/结果写
				// 仍可能凭 active token 产生副作用。
				auth.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING
			}
			aPayload, err := proto.Marshal(auth)
			if err != nil {
				return err
			}
			bPayload, err := r.marshalBattleTransition(battleBefore, battle)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				aTTL, bTTL := in.AuthTTL, in.BattleTTL
				if battle.State == "abandoned" {
					aTTL, bTTL = 0, 0
				}
				pipe.Set(ctx, aKey, aPayload, aTTL)
				pipe.Set(ctx, bKey, bPayload, bTTL)
				if battle.State == "ended" {
					pipe.Del(ctx, rKey)
				}
				if promote {
					// 提升成功即清稳定性证据键(与提升同一 EXEC,原子)。
					pipe.Del(ctx, sKey)
				}
				return nil
			})
			if err == nil {
				out = activateResult(auth, battle, promote, firstAbandon, rosterIncomplete, terminal)
			}
			return err
		}, aKey, bKey, rKey, sKey)
		if txErr == nil {
			if out.Terminal {
				// abandoned 的 active member 同时是 lifecycle outbox：投递成功前必须保留，
				// 让 sweep 能重投。正常 ended 不需要 abandoned 补偿，可直接移出扫描集。
				if out.Battle.State == "ended" {
					if err := r.rdb.ZRem(ctx, activeKey, matchID).Err(); err != nil {
						return out, err
					}
				}
				return out, nil
			}
			// 全局索引跨 slot，只能在权威事务后幂等更新。失败时返回错误让 DS 重试；
			// auth+battle 已原子成功，不会出现误分配，下一心跳会修复索引。
			if err := r.rdb.ZAdd(ctx, activeKey, redis.Z{Score: float64(serverNowMs), Member: matchID}).Err(); err != nil {
				return out, err
			}
			return out, nil
		}
		if bizErr != nil {
			return BattleActivateResult{}, bizErr
		}
		if txErr == redis.TxFailedErr {
			continue
		}
		return BattleActivateResult{}, txErr
	}
	return BattleActivateResult{}, errcode.New(errcode.ErrInternal, "battle %d activate heartbeat cas retry exhausted", matchID)
}

func activateResult(auth *dsv1.BattleDSAuthStorageRecord, battle *dsv1.BattleStorageRecord, first, firstAbandon, rosterIncomplete, terminal bool) BattleActivateResult {
	return BattleActivateResult{
		FirstActivation:  first,
		FirstAbandon:     firstAbandon,
		RosterIncomplete: rosterIncomplete,
		Terminal:         terminal,
		HeartbeatMs:      auth.LastActiveHeartbeatMs,
		Active: BattleCredentialIdentity{
			PodName:       auth.DsPodName,
			InstanceUID:   auth.Active.GetInstanceUid(),
			InstanceEpoch: auth.Active.GetInstanceEpoch(),
			Gen:           auth.Active.GetGen(),
			JTI:           auth.Active.GetJti(),
			ExpMs:         auth.Active.GetExpMs(),
			Kid:           auth.Active.GetKid(),
			TokenSHA256:   auth.Active.GetTokenSha256(),
			WriterEpoch:   auth.Active.GetWriterEpoch(),
		},
		Battle: proto.Clone(battle).(*dsv1.BattleStorageRecord),
	}
}

// ReadAuthority 通过 WATCH + 空写事务取得 auth+battle 的一致快照。
func (r *RedisBattleAuthRepo) ReadAuthority(ctx context.Context, matchID uint64) (BattleAuthoritySnapshot, error) {
	if matchID == 0 {
		return BattleAuthoritySnapshot{}, errcode.New(errcode.ErrInvalidArg, "match_id required")
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		var out BattleAuthoritySnapshot
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			ab, err := tx.Get(ctx, aKey).Bytes()
			switch {
			case err == redis.Nil:
			case err != nil:
				return err
			default:
				out.Auth = &dsv1.BattleDSAuthStorageRecord{}
				if err := unmarshalBattleAuth(matchID, ab, out.Auth); err != nil {
					return err
				}
				out.AuthFound = true
			}
			bb, err := tx.Get(ctx, bKey).Bytes()
			switch {
			case err == redis.Nil:
			case err != nil:
				return err
			default:
				out.Battle, err = unmarshalBattle(matchID, bb)
				if err != nil {
					return err
				}
				out.BattleFound = true
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Exists(ctx, aKey, bKey)
				return nil
			})
			return err
		}, aKey, bKey)
		if txErr == nil {
			return out, nil
		}
		if txErr == redis.TxFailedErr {
			continue
		}
		return BattleAuthoritySnapshot{}, txErr
	}
	return BattleAuthoritySnapshot{}, errcode.New(errcode.ErrInternal, "battle %d read authority cas retry exhausted", matchID)
}

// CheckActive 是所有受保护 Battle DS 副作用 RPC 的前置门。
func (r *RedisBattleAuthRepo) CheckActive(ctx context.Context, matchID uint64, id BattleCredentialIdentity) error {
	nowMs := r.now().UnixMilli()
	if !validBattleIdentity(id, nowMs) {
		return errBattleAuthStale
	}
	snapshot, err := r.ReadAuthority(ctx, matchID)
	if err != nil {
		return err
	}
	nowMs = r.now().UnixMilli()
	if !validBattleIdentity(id, nowMs) {
		return errBattleAuthStale
	}
	if ok, _ := snapshot.activeProjectionConsistent(nowMs); !ok ||
		snapshot.Auth.DsPodName != id.PodName || !battleCredentialMatches(snapshot.Auth.Active, id, nowMs) {
		return errBattleAuthStale
	}
	return nil
}

// PopCommandsIfActive 在 auth+queue 同槽事务里重新校验完整 active tuple 后才 RPOP。
// WATCH 包含队列键，因此并发消费者、轮换或隔离发生时 EXEC 冲突并从最新 auth 重试；
// 权威校验失败时不会发送 MULTI/EXEC，队列保持逐字节不变。
func (r *RedisBattleAuthRepo) PopCommandsIfActive(ctx context.Context, matchID uint64, id BattleCredentialIdentity, queueKey string, max int64) ([]string, error) {
	serverNowMs := r.now().UnixMilli()
	if matchID == 0 || max <= 0 || max > 1000 || queueKey == "" || !containsBattleHashTag(queueKey, matchID) {
		return nil, errcode.New(errcode.ErrInvalidArg, "battle command pop requires match/same-slot queue/max")
	}
	if !validBattleIdentity(id, serverNowMs) {
		return nil, errBattleAuthStale
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		serverNowMs = r.now().UnixMilli()
		if !validBattleIdentity(id, serverNowMs) {
			return nil, errBattleAuthStale
		}
		var out []string
		var bizErr error
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			ab, err := tx.Get(ctx, aKey).Bytes()
			if err == redis.Nil {
				bizErr = errBattleAuthStale
				return bizErr
			}
			if err != nil {
				return err
			}
			auth := &dsv1.BattleDSAuthStorageRecord{}
			if err := unmarshalBattleAuth(matchID, ab, auth); err != nil {
				return err
			}
			bb, err := tx.Get(ctx, bKey).Bytes()
			if err == redis.Nil {
				bizErr = errBattleAuthStale
				return bizErr
			}
			if err != nil {
				return err
			}
			battle, err := unmarshalBattle(matchID, bb)
			if err != nil {
				return err
			}
			snapshot := BattleAuthoritySnapshot{
				Auth: auth, Battle: battle, AuthFound: true, BattleFound: true,
			}
			if ok, _ := snapshot.activeProjectionConsistent(serverNowMs); !ok || battleTerminal(battle.State) {
				bizErr = errBattleAuthStale
				return bizErr
			}
			if auth.DsPodName != id.PodName || auth.InstanceUid != id.InstanceUID ||
				auth.InstanceEpoch != id.InstanceEpoch || auth.HighWaterGen < id.Gen ||
				!battleCredentialComplete(auth.Active, auth, serverNowMs) ||
				!battleCredentialMatches(auth.Active, id, serverNowMs) {
				bizErr = errBattleAuthStale
				return bizErr
			}
			var pop *redis.StringSliceCmd
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pop = pipe.RPopCount(ctx, queueKey, int(max))
				return nil
			})
			if err != nil && err != redis.Nil {
				return err
			}
			if pop == nil {
				return fmt.Errorf("battle %d command pop missing transaction result", matchID)
			}
			out, err = pop.Result()
			if err == redis.Nil {
				out = []string{}
				return nil
			}
			return err
		}, aKey, bKey, queueKey)
		if txErr == nil {
			return out, nil
		}
		if bizErr != nil {
			return nil, bizErr
		}
		if txErr == redis.TxFailedErr {
			continue
		}
		return nil, txErr
	}
	return nil, errcode.New(errcode.ErrInternal, "battle %d command pop cas retry exhausted", matchID)
}

func (r *RedisBattleAuthRepo) CheckHeartbeatFresh(ctx context.Context, matchID uint64, thresholdMs int64) (bool, error) {
	snapshot, err := r.ReadAuthority(ctx, matchID)
	if err != nil {
		return false, err
	}
	return snapshot.HeartbeatFresh(r.now().UnixMilli(), thresholdMs), nil
}

// QuarantineExpected 是泄露 token 的紧急路径：同槽原子锁 auth、清 pending，并把 battle
// 转 abandoned 进入可靠补偿 outbox。墓碑永久保留；即使投影漂移无法安全改 state，也会
// PERSIST 现有 battle，防两键过期后不同 UID 通过 Prepare 重建授权。普通平滑轮换仍走 ROTATING。
func (r *RedisBattleAuthRepo) QuarantineExpected(
	ctx context.Context,
	matchID uint64,
	expected BattleQuarantineExpected,
	authTTL, battleTTL time.Duration,
) (BattleQuarantineResult, error) {
	nowMs := r.now().UnixMilli()
	if matchID == 0 || expected.AllocationID == "" || !validBattleIdentity(expected.Credential, nowMs) ||
		authTTL <= 0 || battleTTL <= 0 {
		return BattleQuarantineResult{}, errcode.New(errcode.ErrInvalidArg, "battle quarantine requires full expected credential/allocation/ttls")
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		result := BattleQuarantineResult{}
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			authRaw, err := tx.Get(ctx, aKey).Bytes()
			if err == redis.Nil {
				return nil
			}
			if err != nil {
				return err
			}
			authRecord := &dsv1.BattleDSAuthStorageRecord{}
			if err := unmarshalBattleAuth(matchID, authRaw, authRecord); err != nil {
				return err
			}
			if !battleAuthRecordV2Exact(authRecord) ||
				authRecord.GetAllocationId() != expected.AllocationID ||
				authRecord.GetDsPodName() != expected.Credential.PodName ||
				authRecord.GetInstanceUid() != expected.Credential.InstanceUID ||
				authRecord.GetInstanceEpoch() != expected.Credential.InstanceEpoch ||
				!battleCredentialMatches(authRecord.GetActive(), expected.Credential, nowMs) ||
				(authRecord.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE &&
					authRecord.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING &&
					authRecord.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_QUARANTINED) {
				return nil
			}
			var battleRecord *dsv1.BattleStorageRecord
			battleRaw, battleErr := tx.Get(ctx, bKey).Bytes()
			switch {
			case battleErr == redis.Nil:
			case battleErr != nil:
				return battleErr
			default:
				battleRecord, err = unmarshalBattle(matchID, battleRaw)
				if err != nil {
					return err
				}
			}
			var battleBefore *dsv1.BattleStorageRecord
			if battleRecord != nil {
				battleBefore = proto.Clone(battleRecord).(*dsv1.BattleStorageRecord)
				if invariantErr := validateBattleStorageTransition(battleRecord, battleRecord); r.StrictModelBWritesEnabled() && invariantErr != nil {
					return errcode.NewCause(errcode.ErrInvalidState, invariantErr,
						"battle %d quarantine storage invariant failed", matchID)
				}
			}
			projectionMatches := battleProjectionMatchesCredential(authRecord, battleRecord, expected.Credential)
			authRecord.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_QUARANTINED
			authRecord.Pending = nil
			authRecord.PendingStartedMs = 0
			authRecord.DeliveredRv = ""
			authRecord.UpdatedAtMs = nowMs
			if projectionMatches && !battleTerminal(battleRecord.GetState()) {
				battleRecord.State = "abandoned"
			}
			authPayload, err := proto.Marshal(authRecord)
			if err != nil {
				return err
			}
			var battlePayload []byte
			if projectionMatches {
				battlePayload, err = r.marshalBattleTransition(battleBefore, battleRecord)
				if err != nil {
					return err
				}
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, aKey, authPayload, 0)
				if battlePayload != nil {
					pipe.Set(ctx, bKey, battlePayload, 0)
				} else if battleRecord != nil {
					pipe.Persist(ctx, bKey)
				}
				return nil
			})
			if err == nil {
				result.AuthQuarantined = true
				result.ProjectionAbandoned = projectionMatches
			}
			return err
		}, aKey, bKey)
		if txErr == redis.TxFailedErr {
			continue
		}
		if txErr != nil || !result.AuthQuarantined {
			return result, txErr
		}
		if !result.ProjectionAbandoned {
			return result, nil
		}
		// 全局 active 是可靠补偿 outbox；score=0 令下一次 sweep 立即对账/回收。
		if err := r.rdb.ZAdd(ctx, activeKey, redis.Z{Score: 0, Member: matchID}).Err(); err != nil {
			return result, err
		}
		return result, nil
	}
	return BattleQuarantineResult{}, errcode.New(errcode.ErrInternal, "battle %d quarantine cas retry exhausted", matchID)
}

func battleProjectionMatchesCredential(
	authRecord *dsv1.BattleDSAuthStorageRecord,
	battleRecord *dsv1.BattleStorageRecord,
	id BattleCredentialIdentity,
) bool {
	active := authRecord.GetActive()
	return authorityBindingMatches(authRecord, battleRecord) && active != nil &&
		active.GetInstanceUid() == id.InstanceUID && active.GetInstanceEpoch() == id.InstanceEpoch &&
		active.GetGen() == id.Gen && active.GetJti() == id.JTI &&
		active.GetWriterEpoch() == id.WriterEpoch && authRecord.GetHighWaterGen() >= id.Gen &&
		battleRecord.GetLastVerifiedGen() == id.Gen && battleRecord.GetLastVerifiedJti() == id.JTI &&
		battleRecord.GetLastVerifiedWriterEpoch() == id.WriterEpoch
}

// AbandonIfStale 把 sweep 的二次核验和 active→TERMINATING / battle→abandoned 放进
// auth+battle 同槽事务。并发新心跳会使 WATCH 失败；重试读取新 heartbeat 后返回不判弃。
// 阈值在事务内按同一权威快照选择(BattleStaleCutoffs 注释),首次 ActivateHeartbeat
// 与本函数并发时由 WATCH 保证只有一方生效:激活赢则按 ACTIVE 阈值重判(新鲜不弃),
// 判弃赢则激活读到 TERMINATING 终态返回 Unauthorized,不存在"激活成功又被弃"。
func (r *RedisBattleAuthRepo) AbandonIfStale(ctx context.Context, matchID uint64, cutoffs BattleStaleCutoffs, authTTL, battleTTL time.Duration) (BattleAbandonResult, error) {
	if matchID == 0 || cutoffs.ActiveHeartbeatMs <= 0 || cutoffs.WarmingHeartbeatMs <= 0 ||
		authTTL <= 0 || battleTTL <= 0 {
		return BattleAbandonResult{}, errcode.New(errcode.ErrInvalidArg, "battle abandon requires match/cutoffs/ttls")
	}
	if f := cutoffs.WarmingForfeit; f != nil &&
		(f.HeartbeatMs <= 0 || f.Instance.AllocationID == "" || f.Instance.InstanceUID == "") {
		return BattleAbandonResult{}, errcode.New(errcode.ErrInvalidArg,
			"battle abandon warming forfeit requires probed instance identity and timestamp")
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		var out BattleAbandonResult
		var bizErr error
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			// 单条 MGET 原子读:两键分次 GET 会与首次 ActivateHeartbeat 撕裂
			//(见 readAuthorityPairAtomic 注释,race 实测)。
			auth, battle, err := readAuthorityPairAtomic(tx, ctx, matchID, aKey, bKey)
			if err != nil {
				return err
			}
			if battle == nil {
				// 与原 readBattleFrom 的 redis.Nil 语义一致:权威不可读,fail-closed。
				bizErr = errBattleAuthStale
				return bizErr
			}
			battleBefore := proto.Clone(battle).(*dsv1.BattleStorageRecord)
			out.AuthFound = auth != nil
			out.ActiveFound = auth != nil && auth.GetActive() != nil
			out.Battle = proto.Clone(battle).(*dsv1.BattleStorageRecord)
			// PrepareCredential 之前 Redis/K8s 半成功可能只有 warming battle。auth 缺失
			// 本身不可授权；同一 WATCH 仍可按 allocation grace 安全判弃。若并发 Prepare
			// 建 auth，EXEC 必冲突并重读，不能误杀刚激活实例。
			if auth == nil {
				if battleTerminal(battle.State) {
					out.AlreadyTerminal = true
					_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
						if battle.State == "abandoned" {
							payload, marshalErr := r.marshalBattleTransition(battleBefore, battle)
							if marshalErr != nil {
								return marshalErr
							}
							pipe.Set(ctx, bKey, payload, 0)
						} else {
							pipe.Exists(ctx, aKey, bKey)
						}
						return nil
					})
					return err
				}
				if battle.State != "allocating" && battle.State != "warming" {
					bizErr = errcode.New(errcode.ErrInvalidState, "battle %d missing auth outside allocation grace", matchID)
					return bizErr
				}
				// warming 是冷加载窗口(无业务心跳属正常),用 ready 等待阈值(判死 forfeit
				// 须与本事务快照的 exact 身份一致才生效);allocating 尚无外部实例交付,
				// 保持快速清理语义。
				cutoffMs := cutoffs.ActiveHeartbeatMs
				if battle.State == "warming" {
					cutoffMs = cutoffs.warmingCutoffFor(battle)
				}
				if battle.LastHeartbeatMs > cutoffMs {
					_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
						pipe.Exists(ctx, aKey, bKey)
						return nil
					})
					return err
				}
				battle.State = "abandoned"
				bPayload, err := r.marshalBattleTransition(battleBefore, battle)
				if err != nil {
					return err
				}
				_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Set(ctx, bKey, bPayload, 0)
					return nil
				})
				if err == nil {
					out.Abandoned = true
					out.Battle = proto.Clone(battle).(*dsv1.BattleStorageRecord)
				}
				return err
			}
			// 这两条挡住的是 sweep 的**判弃权**:拒了就代表这局既不会被判弃、
			// 也不会回收 GameServer(fail-closed,方向正确),但以前只回一个
			// ErrUnauthorized,上层 model_b_sweep_authority_check_failed 打出来的
			// err 文本对两种情况完全一样,分不清是记录格式旧还是 auth↔battle 串了。
			if !battleAuthRecordV2Exact(auth) {
				plog.With(ctx).Warnw("msg", "battle_sweep_authority_rejected",
					"match_id", matchID, "reason", authRejectBindingMismatch,
					"detail", "auth_record_not_v2_exact", "pod", battle.GetDsPodName(),
					"battle_state", battle.GetState(), "auth_phase", auth.GetPhase().String(),
					"hint", "本局不会被判弃也不会回收 GS(fail-closed);持续出现即 GS 占位泄漏来源")
				bizErr = errBattleAuthStale
				return bizErr
			}
			if !authorityBindingMatches(auth, battle) {
				plog.With(ctx).Warnw("msg", "battle_sweep_authority_rejected",
					"match_id", matchID, "reason", authRejectBindingMismatch,
					"detail", "auth_battle_binding_mismatch", "pod", battle.GetDsPodName(),
					"auth_pod", auth.GetDsPodName(), "auth_uid", auth.GetInstanceUid(),
					"auth_allocation_id", auth.GetAllocationId(),
					"battle_allocation_id", battle.GetAllocationId(),
					"battle_state", battle.GetState())
				bizErr = errBattleAuthStale
				return bizErr
			}
			if battleTerminal(battle.State) {
				out.AlreadyTerminal = true
				if auth.Phase != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING &&
					auth.Phase != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_QUARANTINED {
					// 防旧 writer/历史半状态只把 battle 写终态却仍留下 active 授权。
					auth.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING
					auth.UpdatedAtMs = r.now().UnixMilli()
					aPayload, err := proto.Marshal(auth)
					if err != nil {
						return err
					}
					if battle.State != "abandoned" {
						_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
							pipe.Set(ctx, aKey, aPayload, redis.KeepTTL)
							return nil
						})
						return err
					}
				}
				if battle.State == "abandoned" {
					aPayload, err := proto.Marshal(auth)
					if err != nil {
						return err
					}
					bPayload, err := r.marshalBattleTransition(battleBefore, battle)
					if err != nil {
						return err
					}
					_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
						pipe.Set(ctx, aKey, aPayload, 0)
						pipe.Set(ctx, bKey, bPayload, 0)
						return nil
					})
					return err
				}
				_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Exists(ctx, aKey, bKey)
					return nil
				})
				return err
			}
			if auth.Phase == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING {
				bizErr = errcode.New(errcode.ErrInvalidState, "battle %d terminating auth with non-terminal battle", matchID)
				return bizErr
			}

			stale := false
			switch {
			case auth.Active != nil:
				if !battleActiveProjectionStructurallyConsistent(auth, battle) {
					bizErr = errcode.New(errcode.ErrInvalidState, "battle %d active projection corrupt", matchID)
					return bizErr
				}
				// 已激活过的实例(含 ready/running/rotating)一律按业务心跳阈值,
				// 停跳即回收,不因 warming 宽限延后。
				stale = auth.LastActiveHeartbeatMs <= cutoffs.ActiveHeartbeatMs
			case auth.Phase == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_BOOTSTRAP &&
				(battle.State == "allocating" || battle.State == "warming"):
				// 尚未激活的分配只认后端写入的 allocation grace 时间；pending 本身不能续命。
				// warming 冷加载用 ready 等待阈值(判死 forfeit 须身份精确一致),
				// allocating 保持快速清理。
				cutoffMs := cutoffs.ActiveHeartbeatMs
				if battle.State == "warming" {
					cutoffMs = cutoffs.warmingCutoffFor(battle)
				}
				stale = battle.LastHeartbeatMs <= cutoffMs
			default:
				bizErr = errcode.New(errcode.ErrInvalidState, "battle %d auth state cannot be swept safely", matchID)
				return bizErr
			}
			if !stale {
				_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Exists(ctx, aKey, bKey)
					return nil
				})
				return err
			}
			nowMs := r.now().UnixMilli()
			auth.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING
			auth.UpdatedAtMs = nowMs
			battle.State = "abandoned"
			aPayload, err := proto.Marshal(auth)
			if err != nil {
				return err
			}
			bPayload, err := r.marshalBattleTransition(battleBefore, battle)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				// 外部 UID 条件 Release 尚未开始；两键必须先成为永久墓碑。
				// Release 与 lifecycle 都明确成功后，ExpireTerminatedExpected 才恢复 TTL。
				pipe.Set(ctx, aKey, aPayload, 0)
				pipe.Set(ctx, bKey, bPayload, 0)
				return nil
			})
			if err == nil {
				out.Abandoned = true
				out.Battle = proto.Clone(battle).(*dsv1.BattleStorageRecord)
			}
			return err
		}, aKey, bKey)
		if txErr == redis.TxFailedErr {
			continue
		}
		if txErr != nil {
			if bizErr != nil {
				return BattleAbandonResult{}, bizErr
			}
			return BattleAbandonResult{}, txErr
		}
		// 不 ZREM：active member 是 abandoned lifecycle 的 Redis outbox。外层只有在
		// Kafka 投递成功后才能 RemoveActive；失败则下轮 sweep 读 AlreadyTerminal 重投。
		return out, nil
	}
	return BattleAbandonResult{}, errcode.New(errcode.ErrInternal, "battle %d abandon cas retry exhausted", matchID)
}

// TerminateExpected 仅在 allocation_id 仍为调用方持有的实例时锁死授权并写终态。
// 这是外部 ReleaseExpected 之前的永久线性化点：两键无 TTL，Release 失败/响应未知
// 时墓碑不会自行消失。重复调用会重新确认并保持永久，供幂等 release 重试。
func (r *RedisBattleAuthRepo) TerminateExpected(ctx context.Context, matchID uint64, expected BattleExpectedInstance, terminalState string, authTTL, battleTTL time.Duration) (bool, error) {
	if matchID == 0 || !completeExpectedBattleInstance(expected) || authTTL <= 0 || battleTTL <= 0 ||
		(terminalState != "ended" && terminalState != "abandoned") {
		return false, errcode.New(errcode.ErrInvalidArg, "battle terminate requires expected allocation and terminal state")
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	rKey := dsauthrecord.BattleResultReceiptKey(matchID)
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		terminated := false
		var bizErr error
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			auth, battle, err := readBoundAuthority(tx, ctx, matchID, aKey, bKey)
			if err != nil {
				if errcode.As(err) == errcode.ErrUnauthorized {
					return nil
				}
				return err
			}
			battleBefore := proto.Clone(battle).(*dsv1.BattleStorageRecord)
			if !battleAuthRecordV2Exact(auth) ||
				!expectedBattleInstanceMatches(auth, battle, expected) {
				return nil
			}
			alreadyTerminated := auth.Phase == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING &&
				battleTerminal(battle.State)
			if terminalState == "ended" && !alreadyTerminated {
				active := auth.GetActive()
				id := BattleCredentialIdentity{
					PodName: auth.GetDsPodName(), InstanceUID: active.GetInstanceUid(),
					InstanceEpoch: active.GetInstanceEpoch(), Gen: active.GetGen(), JTI: active.GetJti(),
					ExpMs: active.GetExpMs(), Kid: active.GetKid(), TokenSHA256: active.GetTokenSha256(),
					WriterEpoch: active.GetWriterEpoch(),
				}
				recorded, receiptErr := battleResultReceiptMatches(
					tx, ctx, matchID, rKey, auth, battle, id, r.now().UnixMilli())
				if receiptErr != nil {
					return receiptErr
				}
				if !recorded {
					bizErr = errBattleResultNotRecorded
					return bizErr
				}
			}
			auth.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING
			auth.UpdatedAtMs = r.now().UnixMilli()
			if !battleTerminal(battle.State) {
				battle.State = terminalState
			}
			aPayload, err := proto.Marshal(auth)
			if err != nil {
				return err
			}
			bPayload, err := r.marshalBattleTransition(battleBefore, battle)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, aKey, aPayload, 0)
				pipe.Set(ctx, bKey, bPayload, 0)
				pipe.Del(ctx, rKey)
				return nil
			})
			terminated = err == nil
			return err
		}, aKey, bKey, rKey)
		if txErr == redis.TxFailedErr {
			continue
		}
		if txErr != nil {
			if bizErr != nil {
				return false, bizErr
			}
			return false, txErr
		}
		if !terminated {
			return false, nil
		}
		// active ZSET 在这里还是“外部 release + lifecycle”待办索引；
		// 只有 Purge/Expire 明确完成后才能移除。提前 ZREM 会让进程崩溃时
		// 永久墓碑失去自动重入入口（安全仍在，但回收/补偿永远停滞）。
		return true, nil
	}
	return false, errcode.New(errcode.ErrInternal, "battle %d terminate cas retry exhausted", matchID)
}

// TerminateResultExpected 是正常结算 outbox 的 Redis 线性化点。与旧 TerminateExpected
// 不同，它不要求 relay 时 callback token 仍未过期，也不要求 outbox 的 gen/jti 仍是
// current active；MySQL 行证明该凭据在 AuthorizedAtMs 已通过服务端校验。当前 Redis
// stable identity 与 writer fence 必须一致，allocation/UID 漂移零副作用。
func (r *RedisBattleAuthRepo) TerminateResultExpected(
	ctx context.Context,
	matchID uint64,
	expected BattleExpectedInstance,
	proof BattleResultAuthorizationProof,
) (bool, error) {
	if r == nil || r.rdb == nil || r.now == nil {
		return false, errcode.New(errcode.ErrUnavailable, "battle terminal result authority unavailable")
	}
	nowMs := r.now().UnixMilli()
	if !validBattleResultAuthorizationProof(matchID, expected, proof, nowMs) {
		return false, errcode.New(errcode.ErrInvalidArg, "battle terminal result proof is incomplete")
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	rKey := dsauthrecord.BattleResultReceiptKey(matchID)
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		fenced := false
		var bizErr error
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			battleRaw, err := tx.Get(ctx, bKey).Bytes()
			if err == redis.Nil {
				bizErr = errcode.New(errcode.ErrInvalidState, "battle %d projection missing before terminal release", matchID)
				return bizErr
			}
			if err != nil {
				return err
			}
			battle, err := unmarshalBattle(matchID, battleRaw)
			if err != nil {
				return err
			}
			battleBefore := proto.Clone(battle).(*dsv1.BattleStorageRecord)
			if !battleResultStableProjectionMatches(battle, expected, proof) || battle.GetState() == "abandoned" {
				bizErr = errcode.New(errcode.ErrUnauthorized, "battle %d stable identity changed before terminal release", matchID)
				return bizErr
			}

			var authRecord *dsv1.BattleDSAuthStorageRecord
			authRaw, authErr := tx.Get(ctx, aKey).Bytes()
			switch {
			case authErr == redis.Nil:
				// callback auth TTL 可以早于 BattleTTL。只在 projection 仍携带精确 stable
				// identity + writer=2 时重建“只可终止”的 auth 墓碑；phase=TERMINATING
				// 保证旧/泄漏 token 永远不能借此恢复写权限。
				authRecord = terminalAuthFromResultProof(matchID, battle, proof, nowMs)
			case authErr != nil:
				return authErr
			default:
				authRecord = &dsv1.BattleDSAuthStorageRecord{}
				if err := unmarshalBattleAuth(matchID, authRaw, authRecord); err != nil {
					return err
				}
				if !battleResultStableAuthorityMatches(authRecord, battle, expected) {
					bizErr = errcode.New(errcode.ErrUnauthorized, "battle %d authority changed before terminal release", matchID)
					return bizErr
				}
			}

			recordedAtMs := proof.AuthorizedAtMs
			receipt := dsauthrecord.NewBattleResultReceipt(
				matchID, expected.AllocationID, proof.Credential.PodName,
				proof.Credential.InstanceUID, proof.Credential.InstanceEpoch,
				proof.Credential.Gen, proof.Credential.JTI, int64(proof.Credential.ExpMs),
				proof.Credential.Kid, proof.Credential.TokenSHA256,
				proof.Credential.WriterEpoch, recordedAtMs)
			if oldRaw, oldErr := tx.Get(ctx, rKey).Bytes(); oldErr == nil {
				old, decodeErr := dsauthrecord.UnmarshalBattleResultReceipt(oldRaw)
				if decodeErr != nil || !old.Valid(nowMs) || !old.SameCredential(receipt) {
					bizErr = errcode.New(errcode.ErrUnauthorized, "battle %d terminal receipt belongs to another proof", matchID)
					return bizErr
				}
				// immediate receipt 可能在 DB commit 后写入，保留其真实 recorded_at。
				receipt.RecordedAtMs = old.RecordedAtMs
			} else if oldErr != redis.Nil {
				return oldErr
			}

			authRecord.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING
			authRecord.Pending = nil
			authRecord.PendingStartedMs = 0
			authRecord.DeliveredRv = ""
			authRecord.UpdatedAtMs = nowMs
			battle.State = "ended"
			aPayload, err := proto.Marshal(authRecord)
			if err != nil {
				return err
			}
			bPayload, err := r.marshalBattleTransition(battleBefore, battle)
			if err != nil {
				return err
			}
			rPayload, err := dsauthrecord.MarshalBattleResultReceipt(receipt)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				// UID Release 结果明确前，三键均为永久墓碑；任何 timeout/崩溃都可重入。
				pipe.Set(ctx, aKey, aPayload, 0)
				pipe.Set(ctx, bKey, bPayload, 0)
				pipe.Set(ctx, rKey, rPayload, 0)
				return nil
			})
			fenced = err == nil
			return err
		}, aKey, bKey, rKey)
		if txErr == redis.TxFailedErr {
			continue
		}
		if txErr != nil {
			if bizErr != nil {
				return false, bizErr
			}
			return false, txErr
		}
		return fenced, nil
	}
	return false, errcode.New(errcode.ErrInternal, "battle %d terminal result cas retry exhausted", matchID)
}

// ExpireResultTerminatedExpected 只在 K8s UID 条件删除明确成功、且 battle_result 已
// durable CAS released_at_ms 后调用。它只确认同 proof 并恢复 tombstone TTL，绝不再次
// 触碰 Kubernetes；response 丢失后可安全重放，三键已跨 TTL 全部消失也幂等成功。
func (r *RedisBattleAuthRepo) ExpireResultTerminatedExpected(
	ctx context.Context,
	matchID uint64,
	expected BattleExpectedInstance,
	proof BattleResultAuthorizationProof,
	retention time.Duration,
) (bool, error) {
	if r == nil || r.rdb == nil || r.now == nil || retention <= 0 ||
		!validBattleResultAuthorizationProof(matchID, expected, proof, r.now().UnixMilli()) {
		return false, errcode.New(errcode.ErrInvalidArg, "battle terminal result expire requires proof and retention")
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	rKey := dsauthrecord.BattleResultReceiptKey(matchID)
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		expired := false
		var bizErr error
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			authRaw, authErr := tx.Get(ctx, aKey).Bytes()
			battleRaw, battleErr := tx.Get(ctx, bKey).Bytes()
			receiptRaw, receiptErr := tx.Get(ctx, rKey).Bytes()
			authMissing, battleMissing, receiptMissing :=
				authErr == redis.Nil, battleErr == redis.Nil, receiptErr == redis.Nil
			if authMissing && battleMissing && receiptMissing {
				// 上一次 finalize 可能已经成功但响应丢失，MySQL released 行尚未 DELETE；
				// TTL 到期后三键全无等价于 cleanup 已完成，按幂等成功重认。仍通过
				// WATCH+EXEC no-op 锁定“确实同时为空”的线性化快照。
				_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Exists(ctx, aKey, bKey, rKey)
					return nil
				})
				expired = err == nil
				return err
			}
			if authErr != nil || battleErr != nil || receiptErr != nil {
				if (authErr != nil && authErr != redis.Nil) ||
					(battleErr != nil && battleErr != redis.Nil) ||
					(receiptErr != nil && receiptErr != redis.Nil) {
					for _, readErr := range []error{authErr, battleErr, receiptErr} {
						if readErr != nil && readErr != redis.Nil {
							return readErr
						}
					}
				}
				bizErr = errcode.New(errcode.ErrInvalidState,
					"battle %d terminal tombstone partially missing", matchID)
				return bizErr
			}
			authRecord := &dsv1.BattleDSAuthStorageRecord{}
			if err := unmarshalBattleAuth(matchID, authRaw, authRecord); err != nil {
				return err
			}
			battle, err := unmarshalBattle(matchID, battleRaw)
			if err != nil {
				return err
			}
			if !battleResultStableAuthorityMatches(authRecord, battle, expected) ||
				authRecord.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
				battle.GetState() != "ended" {
				bizErr = errcode.New(errcode.ErrUnauthorized, "battle %d terminal tombstone changed", matchID)
				return bizErr
			}
			receipt, err := dsauthrecord.UnmarshalBattleResultReceipt(receiptRaw)
			if err != nil || !receipt.Valid(r.now().UnixMilli()) || !receiptMatchesResultProof(receipt, matchID, expected, proof) {
				bizErr = errcode.New(errcode.ErrUnauthorized, "battle %d terminal receipt changed", matchID)
				return bizErr
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Expire(ctx, aKey, retention)
				pipe.Expire(ctx, bKey, retention)
				pipe.Expire(ctx, rKey, retention)
				return nil
			})
			expired = err == nil
			return err
		}, aKey, bKey, rKey)
		if txErr == redis.TxFailedErr {
			continue
		}
		if txErr != nil {
			if bizErr != nil {
				return false, bizErr
			}
			return false, txErr
		}
		if !expired {
			return false, nil
		}
		if err := r.rdb.ZRem(ctx, activeKey, matchID).Err(); err != nil {
			return true, err
		}
		return true, nil
	}
	return false, errcode.New(errcode.ErrInternal, "battle %d terminal result expire cas retry exhausted", matchID)
}

// FencePreactiveReleaseExpected 是未激活分配回收的线性化点。它不删除任何权威键，
// 而是先把 battle 置为不可路由的 release-pending，并把已有 auth 锁成 TERMINATING；
// 两键在同一 EXEC 内写成永久。外部 ReleaseExpected 超时/响应未知时，该墓碑会一直
// 阻止同 match 第二次 GSA POST，直到一次幂等删除得到明确成功。
func (r *RedisBattleAuthRepo) FencePreactiveReleaseExpected(
	ctx context.Context,
	matchID uint64,
	expected BattleExpectedInstance,
) (bool, error) {
	if matchID == 0 || expected.AllocationID == "" || expected.InstanceUID == "" {
		return false, errcode.New(errcode.ErrInvalidArg,
			"battle preactive release fence requires allocation_id and instance_uid")
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		fenced := false
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			battle, err := readBattleFrom(tx, ctx, matchID, bKey)
			if err != nil {
				if errcode.As(err) == errcode.ErrUnauthorized {
					return nil
				}
				return err
			}
			battleBefore := proto.Clone(battle).(*dsv1.BattleStorageRecord)
			if battle.GetAllocationId() != expected.AllocationID ||
				battle.GetGameserverUid() != expected.InstanceUID ||
				(battle.GetState() != "warming" && battle.GetState() != "abandoned" &&
					battle.GetState() != BattleStatePreactiveReleasePending) {
				return nil
			}

			var authRecord *dsv1.BattleDSAuthStorageRecord
			authRaw, authErr := tx.Get(ctx, aKey).Bytes()
			switch {
			case authErr == redis.Nil:
				if expected.InstanceEpoch != 0 || battle.GetInstanceEpoch() != 0 {
					return nil
				}
			case authErr != nil:
				return authErr
			default:
				authRecord = &dsv1.BattleDSAuthStorageRecord{}
				if err := unmarshalBattleAuth(matchID, authRaw, authRecord); err != nil {
					return err
				}
				if !completeExpectedBattleInstance(expected) ||
					!battleAuthRecordV2Exact(authRecord) ||
					!expectedBattleInstanceMatches(authRecord, battle, expected) ||
					authRecord.GetActive() != nil ||
					(authRecord.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_BOOTSTRAP &&
						authRecord.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING) {
					return nil
				}
			}

			battle.State = BattleStatePreactiveReleasePending
			battlePayload, err := r.marshalBattleTransition(battleBefore, battle)
			if err != nil {
				return err
			}
			var authPayload []byte
			if authRecord != nil {
				authRecord.Phase = dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING
				authRecord.Pending = nil
				authRecord.PendingStartedMs = 0
				authRecord.DeliveredRv = ""
				authRecord.UpdatedAtMs = r.now().UnixMilli()
				authPayload, err = proto.Marshal(authRecord)
				if err != nil {
					return err
				}
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				if authPayload != nil {
					pipe.Set(ctx, aKey, authPayload, 0)
				}
				pipe.Set(ctx, bKey, battlePayload, 0)
				return nil
			})
			fenced = err == nil
			return err
		}, aKey, bKey)
		if txErr == redis.TxFailedErr {
			continue
		}
		if txErr != nil || !fenced {
			return fenced, txErr
		}
		return true, nil
	}
	return false, errcode.New(errcode.ErrInternal,
		"battle %d preactive release fence cas retry exhausted", matchID)
}

// PurgePreactiveReleasedExpected 只能在调用方已经明确确认外部 UID 条件删除成功后执行。
// 仓内再核验 expected tuple、release-pending 状态和两键 PTTL=-1，防止有限 TTL 的
// 历史半状态或已激活赢家被误删。generation counter 永不删除。
func (r *RedisBattleAuthRepo) PurgePreactiveReleasedExpected(
	ctx context.Context,
	matchID uint64,
	expected BattleExpectedInstance,
) (bool, error) {
	if matchID == 0 || expected.AllocationID == "" || expected.InstanceUID == "" {
		return false, errcode.New(errcode.ErrInvalidArg,
			"battle preactive purge requires allocation_id and instance_uid")
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		purged := false
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			battle, err := readBattleFrom(tx, ctx, matchID, bKey)
			if err != nil {
				if errcode.As(err) == errcode.ErrUnauthorized {
					return nil
				}
				return err
			}
			if battle.GetAllocationId() != expected.AllocationID ||
				battle.GetGameserverUid() != expected.InstanceUID ||
				battle.GetState() != BattleStatePreactiveReleasePending {
				return nil
			}
			bTTL, err := tx.PTTL(ctx, bKey).Result()
			if err != nil || bTTL != -1 {
				return err
			}

			authRaw, authErr := tx.Get(ctx, aKey).Bytes()
			switch {
			case authErr == redis.Nil:
				if expected.InstanceEpoch != 0 || battle.GetInstanceEpoch() != 0 {
					return nil
				}
			case authErr != nil:
				return authErr
			default:
				authRecord := &dsv1.BattleDSAuthStorageRecord{}
				if err := unmarshalBattleAuth(matchID, authRaw, authRecord); err != nil {
					return err
				}
				if !completeExpectedBattleInstance(expected) ||
					!battleAuthRecordV2Exact(authRecord) ||
					!expectedBattleInstanceMatches(authRecord, battle, expected) ||
					authRecord.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
					authRecord.GetActive() != nil {
					return nil
				}
				aTTL, err := tx.PTTL(ctx, aKey).Result()
				if err != nil || aTTL != -1 {
					return err
				}
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Del(ctx, aKey, bKey)
				return nil
			})
			purged = err == nil
			return err
		}, aKey, bKey)
		if txErr == redis.TxFailedErr {
			continue
		}
		if txErr != nil || !purged {
			return purged, txErr
		}
		if err := r.rdb.ZRem(ctx, activeKey, matchID).Err(); err != nil {
			return true, err
		}
		return true, nil
	}
	return false, errcode.New(errcode.ErrInternal,
		"battle %d preactive purge cas retry exhausted", matchID)
}

// PurgeTerminatedExpected 仅清理已锁死且 battle 已终态的同一 allocation。
// 调用方必须先明确确认外部 ReleaseExpected 成功；仓内要求两键仍为永久墓碑，
// 拒绝删除历史有限 TTL 半状态。
func (r *RedisBattleAuthRepo) PurgeTerminatedExpected(ctx context.Context, matchID uint64, expected BattleExpectedInstance) (bool, error) {
	if matchID == 0 || !completeExpectedBattleInstance(expected) {
		return false, errcode.New(errcode.ErrInvalidArg, "battle purge requires expected allocation")
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		purged := false
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			auth, battle, err := readBoundAuthority(tx, ctx, matchID, aKey, bKey)
			if err != nil {
				if errcode.As(err) == errcode.ErrUnauthorized {
					return nil
				}
				return err
			}
			if !battleAuthRecordV2Exact(auth) ||
				!expectedBattleInstanceMatches(auth, battle, expected) ||
				auth.Phase != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
				!battleTerminal(battle.State) {
				return nil
			}
			aTTL, err := tx.PTTL(ctx, aKey).Result()
			if err != nil || aTTL != -1 {
				return err
			}
			bTTL, err := tx.PTTL(ctx, bKey).Result()
			if err != nil || bTTL != -1 {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Del(ctx, aKey, bKey)
				return nil
			})
			purged = err == nil
			return err
		}, aKey, bKey)
		if txErr == redis.TxFailedErr {
			continue
		}
		if txErr != nil {
			return false, txErr
		}
		if !purged {
			return false, nil
		}
		if err := r.rdb.ZRem(ctx, activeKey, matchID).Err(); err != nil {
			return true, err
		}
		return true, nil
	}
	return false, errcode.New(errcode.ErrInternal, "battle %d purge cas retry exhausted", matchID)
}

// ExpireTerminatedExpected 在外部 GameServer release 与 abandoned lifecycle 投递均
// 已明确成功后，把永久墓碑改为有界审计保留。它不改变任何 protobuf bytes；完整
// expected tuple/TERMINATING/终态仍须在同一 WATCH 中成立。
func (r *RedisBattleAuthRepo) ExpireTerminatedExpected(
	ctx context.Context,
	matchID uint64,
	expected BattleExpectedInstance,
	authTTL, battleTTL time.Duration,
) (bool, error) {
	if matchID == 0 || !completeExpectedBattleInstance(expected) || authTTL <= 0 || battleTTL <= 0 {
		return false, errcode.New(errcode.ErrInvalidArg,
			"battle expire terminated requires expected allocation and positive ttls")
	}
	aKey, bKey := battleAuthKey(matchID), battleKey(matchID)
	for attempt := 0; attempt < battleAuthCASRetries; attempt++ {
		expired := false
		txErr := r.rdb.Watch(ctx, func(tx *redis.Tx) error {
			authRecord, battle, err := readBoundAuthority(tx, ctx, matchID, aKey, bKey)
			if err != nil {
				if errcode.As(err) == errcode.ErrUnauthorized {
					return nil
				}
				return err
			}
			if !battleAuthRecordV2Exact(authRecord) ||
				!expectedBattleInstanceMatches(authRecord, battle, expected) ||
				authRecord.GetPhase() != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING ||
				!battleTerminal(battle.GetState()) {
				return nil
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Expire(ctx, aKey, authTTL)
				pipe.Expire(ctx, bKey, battleTTL)
				return nil
			})
			expired = err == nil
			return err
		}, aKey, bKey)
		if txErr == redis.TxFailedErr {
			continue
		}
		if txErr != nil || !expired {
			return expired, txErr
		}
		if err := r.rdb.ZRem(ctx, activeKey, matchID).Err(); err != nil {
			return true, err
		}
		return true, nil
	}
	return false, errcode.New(errcode.ErrInternal,
		"battle %d expire terminated cas retry exhausted", matchID)
}

func validateBattleBinding(in BattleAuthorityBinding) error {
	if in.MatchID == 0 || in.AllocationID == "" || in.PodName == "" || in.InstanceUID == "" ||
		in.AuthTTL <= 0 || in.BattleTTL <= 0 || in.RequiredWriterEpoch != BattleDSWriterEpochV2 {
		return errcode.New(errcode.ErrInvalidArg, "battle prepare requires match/allocation/pod/uid/ttl/writer_epoch")
	}
	return nil
}

func completeExpectedBattleInstance(expected BattleExpectedInstance) bool {
	return expected.AllocationID != "" && expected.InstanceUID != "" && expected.InstanceEpoch > 0
}

func expectedBattleInstanceMatches(
	authRecord *dsv1.BattleDSAuthStorageRecord,
	battle *dsv1.BattleStorageRecord,
	expected BattleExpectedInstance,
) bool {
	return completeExpectedBattleInstance(expected) && authorityBindingMatches(authRecord, battle) &&
		authRecord.AllocationId == expected.AllocationID && battle.AllocationId == expected.AllocationID &&
		authRecord.InstanceUid == expected.InstanceUID && battle.GameserverUid == expected.InstanceUID &&
		authRecord.InstanceEpoch == expected.InstanceEpoch && battle.InstanceEpoch == expected.InstanceEpoch
}

func validBattleResultAuthorizationProof(
	matchID uint64,
	expected BattleExpectedInstance,
	proof BattleResultAuthorizationProof,
	nowMs int64,
) bool {
	id := proof.Credential
	return matchID != 0 && completeExpectedBattleInstance(expected) && nowMs > 0 &&
		proof.AuthorizedAtMs > 0 && proof.AuthorizedAtMs <= nowMs &&
		id.ExpMs <= uint64(1<<63-1) && proof.AuthorizedAtMs < int64(id.ExpMs) &&
		id.PodName != "" && id.InstanceUID == expected.InstanceUID &&
		id.InstanceEpoch == expected.InstanceEpoch && id.Gen > 0 && id.JTI != "" &&
		id.ExpMs > 0 && id.Kid != "" && id.TokenSHA256 != "" &&
		id.WriterEpoch == BattleDSWriterEpochV2
}

func battleResultStableProjectionMatches(
	battle *dsv1.BattleStorageRecord,
	expected BattleExpectedInstance,
	proof BattleResultAuthorizationProof,
) bool {
	if battle == nil || (battle.GetState() != "ready" && battle.GetState() != "running" && battle.GetState() != "ended") {
		return false
	}
	id := proof.Credential
	return battle.GetMatchId() != 0 && battle.GetAllocationId() == expected.AllocationID &&
		battle.GetDsPodName() == id.PodName && battle.GetGameserverUid() == expected.InstanceUID &&
		battle.GetInstanceEpoch() == expected.InstanceEpoch &&
		battle.GetLastVerifiedGen() > 0 && battle.GetLastVerifiedJti() != "" &&
		battle.GetLastVerifiedWriterEpoch() == BattleDSWriterEpochV2
}

func battleResultStableAuthorityMatches(
	authRecord *dsv1.BattleDSAuthStorageRecord,
	battle *dsv1.BattleStorageRecord,
	expected BattleExpectedInstance,
) bool {
	if !battleAuthRecordV2Exact(authRecord) || !expectedBattleInstanceMatches(authRecord, battle, expected) ||
		authRecord.GetPhase() == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_QUARANTINED ||
		authRecord.GetActive() == nil {
		return false
	}
	active := authRecord.GetActive()
	if active.GetGen() == 0 || active.GetJti() == "" || active.GetExpMs() == 0 ||
		active.GetKid() == "" || active.GetTokenSha256() == "" ||
		active.GetInstanceUid() != expected.InstanceUID ||
		active.GetInstanceEpoch() != expected.InstanceEpoch ||
		active.GetWriterEpoch() != BattleDSWriterEpochV2 || authRecord.GetHighWaterGen() < active.GetGen() {
		return false
	}
	if pending := authRecord.GetPending(); pending != nil &&
		(pending.GetInstanceUid() != expected.InstanceUID || pending.GetInstanceEpoch() != expected.InstanceEpoch ||
			pending.GetWriterEpoch() != BattleDSWriterEpochV2) {
		return false
	}
	switch authRecord.GetPhase() {
	case dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE, dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING:
		// current active/projection 仍需自洽，但允许它已经从 outbox proof 的 gen/jti 轮换。
		return battleActiveProjectionStructurallyConsistent(authRecord, battle)
	case dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING:
		return battle.GetState() == "ended"
	default:
		return false
	}
}

func terminalAuthFromResultProof(
	matchID uint64,
	battle *dsv1.BattleStorageRecord,
	proof BattleResultAuthorizationProof,
	nowMs int64,
) *dsv1.BattleDSAuthStorageRecord {
	id := proof.Credential
	return &dsv1.BattleDSAuthStorageRecord{
		MatchId: matchID, DsPodName: id.PodName, InstanceUid: id.InstanceUID,
		InstanceEpoch: id.InstanceEpoch, Phase: dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_TERMINATING,
		Active: &dsv1.BattleDSCredential{
			Gen: id.Gen, Jti: id.JTI, ExpMs: id.ExpMs, Kid: id.Kid,
			InstanceUid: id.InstanceUID, InstanceEpoch: id.InstanceEpoch,
			TokenSha256: id.TokenSHA256, WriterEpoch: id.WriterEpoch,
		},
		HighWaterGen: max(id.Gen, battle.GetLastVerifiedGen()), UpdatedAtMs: nowMs,
		RequiredWriterEpoch: BattleDSWriterEpochV2, AllocationId: battle.GetAllocationId(),
		LastActiveHeartbeatMs: battle.GetLastHeartbeatMs(),
	}
}

func receiptMatchesResultProof(
	receipt dsauthrecord.BattleResultReceipt,
	matchID uint64,
	expected BattleExpectedInstance,
	proof BattleResultAuthorizationProof,
) bool {
	id := proof.Credential
	want := dsauthrecord.NewBattleResultReceipt(
		matchID, expected.AllocationID, id.PodName, id.InstanceUID, id.InstanceEpoch,
		id.Gen, id.JTI, int64(id.ExpMs), id.Kid, id.TokenSHA256, id.WriterEpoch,
		receipt.RecordedAtMs)
	return receipt.SameCredential(want)
}

func validateBattleCredential(c *dsv1.BattleDSCredential, nowMs int64) error {
	if c == nil || c.Gen == 0 || c.Jti == "" || c.ExpMs == 0 || c.Kid == "" || c.InstanceUid == "" ||
		c.InstanceEpoch == 0 || c.TokenSha256 == "" || c.WriterEpoch != BattleDSWriterEpochV2 {
		return errcode.New(errcode.ErrInvalidArg, "battle credential requires uid/epoch/gen/jti/kid/hash/exp/writer_epoch=2")
	}
	if c.ExpMs <= uint64(nowMs) {
		return errcode.New(errcode.ErrInvalidArg, "battle credential already expired")
	}
	return nil
}

func validBattleIdentity(id BattleCredentialIdentity, nowMs int64) bool {
	return id.PodName != "" && id.InstanceUID != "" && id.InstanceEpoch > 0 && id.Gen > 0 && id.JTI != "" &&
		id.ExpMs > uint64(nowMs) && id.Kid != "" && id.TokenSHA256 != "" && id.WriterEpoch == BattleDSWriterEpochV2
}

func battleStoredCredentialEpochsV2(rec *dsv1.BattleDSAuthStorageRecord) bool {
	return rec != nil &&
		(rec.Active == nil || rec.Active.WriterEpoch == BattleDSWriterEpochV2) &&
		(rec.Pending == nil || rec.Pending.WriterEpoch == BattleDSWriterEpochV2)
}

func battleAuthRecordV2Exact(rec *dsv1.BattleDSAuthStorageRecord) bool {
	return rec != nil && rec.RequiredWriterEpoch == BattleDSWriterEpochV2 &&
		battleStoredCredentialEpochsV2(rec)
}

func battleCredentialEqual(a, b *dsv1.BattleDSCredential) bool {
	return a != nil && b != nil && a.Gen == b.Gen && a.Jti == b.Jti && a.ExpMs == b.ExpMs &&
		a.Kid == b.Kid && a.InstanceUid == b.InstanceUid && a.InstanceEpoch == b.InstanceEpoch &&
		a.TokenSha256 == b.TokenSha256 && a.WriterEpoch == b.WriterEpoch
}

func battleCredentialMatches(c *dsv1.BattleDSCredential, id BattleCredentialIdentity, nowMs int64) bool {
	return c != nil && validBattleIdentity(id, nowMs) && c.ExpMs > uint64(nowMs) &&
		c.Gen == id.Gen && c.Jti == id.JTI && c.ExpMs == id.ExpMs && c.Kid == id.Kid &&
		c.InstanceUid == id.InstanceUID && c.InstanceEpoch == id.InstanceEpoch &&
		c.TokenSha256 == id.TokenSHA256 && c.WriterEpoch == id.WriterEpoch
}

func battleCredentialComplete(c *dsv1.BattleDSCredential, auth *dsv1.BattleDSAuthStorageRecord, nowMs int64) bool {
	return c != nil && auth != nil && auth.MatchId > 0 && auth.AllocationId != "" && auth.DsPodName != "" &&
		auth.InstanceUid != "" && auth.InstanceEpoch > 0 && c.Gen > 0 && c.Jti != "" &&
		c.ExpMs > uint64(nowMs) && c.Kid != "" && c.TokenSha256 != "" &&
		c.InstanceUid == auth.InstanceUid && c.InstanceEpoch == auth.InstanceEpoch &&
		battleAuthRecordV2Exact(auth) && c.WriterEpoch == BattleDSWriterEpochV2
}

// battleActiveProjectionStructurallyConsistent 用于 sweep：允许 active token 已自然过期，
// 但身份字段、writer fence、battle 投影与最后服务端心跳必须仍严格一致。
func battleActiveProjectionStructurallyConsistent(auth *dsv1.BattleDSAuthStorageRecord, battle *dsv1.BattleStorageRecord) bool {
	if !authorityBindingMatches(auth, battle) || auth.Active == nil ||
		(auth.Phase != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ACTIVE &&
			auth.Phase != dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_ROTATING) {
		return false
	}
	c := auth.Active
	return c.Gen > 0 && c.Jti != "" && c.ExpMs > 0 && c.Kid != "" && c.TokenSha256 != "" &&
		c.InstanceUid == auth.InstanceUid && c.InstanceEpoch == auth.InstanceEpoch &&
		battleAuthRecordV2Exact(auth) && c.WriterEpoch == BattleDSWriterEpochV2 &&
		auth.HighWaterGen >= c.Gen && battle.LastVerifiedGen == c.Gen && battle.LastVerifiedJti == c.Jti &&
		battle.LastVerifiedWriterEpoch == c.WriterEpoch && auth.LastActiveHeartbeatMs > 0 &&
		battle.LastHeartbeatMs == auth.LastActiveHeartbeatMs
}

func authorityBindingMatches(auth *dsv1.BattleDSAuthStorageRecord, battle *dsv1.BattleStorageRecord) bool {
	return auth != nil && battle != nil && auth.MatchId == battle.MatchId && auth.AllocationId != "" &&
		auth.AllocationId == battle.AllocationId && auth.DsPodName != "" && auth.DsPodName == battle.DsPodName &&
		auth.InstanceUid != "" && auth.InstanceUid == battle.GameserverUid && auth.InstanceEpoch > 0 &&
		auth.InstanceEpoch == battle.InstanceEpoch
}

func battleResultReceiptMatches(
	tx *redis.Tx,
	ctx context.Context,
	matchID uint64,
	key string,
	authRecord *dsv1.BattleDSAuthStorageRecord,
	battleRecord *dsv1.BattleStorageRecord,
	id BattleCredentialIdentity,
	nowMs int64,
) (bool, error) {
	payload, err := tx.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	receipt, err := dsauthrecord.UnmarshalBattleResultReceipt(payload)
	if err != nil || !receipt.Valid(nowMs) || id.ExpMs > uint64(1<<63-1) {
		return false, nil
	}
	expected := dsauthrecord.NewBattleResultReceipt(
		matchID, authRecord.GetAllocationId(), id.PodName, id.InstanceUID, id.InstanceEpoch,
		id.Gen, id.JTI, int64(id.ExpMs), id.Kid, id.TokenSHA256, id.WriterEpoch,
		receipt.RecordedAtMs)
	return authorityBindingMatches(authRecord, battleRecord) && receipt.SameCredential(expected), nil
}

func readBattleFrom(tx *redis.Tx, ctx context.Context, matchID uint64, key string) (*dsv1.BattleStorageRecord, error) {
	b, err := tx.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, errBattleAuthStale
	}
	if err != nil {
		return nil, err
	}
	return unmarshalBattle(matchID, b)
}

// battleAuthorityPairScript 在单条命令内 GET 同槽 auth+battle 两键。
// 为什么是 Lua 而不是 MGET(复审 P1-1):MGET 对**存在但类型错误**的 key 静默返回
// nil,会把 WRONGTYPE 伪装成"合法缺失"(fail-open,严重时凭错误前提继续写
// abandoned/release);Lua 内 redis.call('GET') 遇到非 string 键直接抛 WRONGTYPE,
// 错误语义与逐键 GET 完全一致。Redis Lua 里缺失键的 GET 返回 false(非 nil),
// {false, value} 不会截断表,两个位置始终齐全。
var battleAuthorityPairScript = redis.NewScript(
	`return {redis.call('GET', KEYS[1]), redis.call('GET', KEYS[2])}`)

// readAuthorityPairAtomic 用**单条 Lua 命令**读取同槽 auth+battle 快照(键含
// {match_id} 同 hash tag,cluster 下合法)。两次独立 GET 之间并发写(如首次
// ActivateHeartbeat 的 EXEC)会造成撕裂快照:auth 已 ACTIVE 而 battle 仍是旧
// warming 镜像,跨键一致性校验在 EXEC 之前就把它误判成 "active projection
// corrupt" 返回(race 实测 8/50)——WATCH 只保护到 EXEC,保护不了闭包内分次读。
// 单条命令原子,撕裂读从根上不可能;WRONGTYPE 照常返回错误(fail-closed)。
// 返回值:任一键缺失返回对应 nil(不设错),由调用方按语义处理。
func readAuthorityPairAtomic(
	tx *redis.Tx,
	ctx context.Context,
	matchID uint64,
	aKey, bKey string,
) (*dsv1.BattleDSAuthStorageRecord, *dsv1.BattleStorageRecord, error) {
	res, err := battleAuthorityPairScript.Run(ctx, tx, []string{aKey, bKey}).Result()
	if err != nil {
		return nil, nil, err
	}
	vals, ok := res.([]interface{})
	if !ok || len(vals) != 2 {
		return nil, nil, fmt.Errorf("battle %d authority pair read returned %T", matchID, res)
	}
	var auth *dsv1.BattleDSAuthStorageRecord
	if raw, ok := vals[0].(string); ok {
		auth = &dsv1.BattleDSAuthStorageRecord{}
		if err := unmarshalBattleAuth(matchID, []byte(raw), auth); err != nil {
			return nil, nil, err
		}
	}
	var battle *dsv1.BattleStorageRecord
	if raw, ok := vals[1].(string); ok {
		battle, err = unmarshalBattle(matchID, []byte(raw))
		if err != nil {
			return nil, nil, err
		}
	}
	return auth, battle, nil
}

func readBoundAuthority(tx *redis.Tx, ctx context.Context, matchID uint64, aKey, bKey string) (*dsv1.BattleDSAuthStorageRecord, *dsv1.BattleStorageRecord, error) {
	auth, battle, err := readAuthorityPairAtomic(tx, ctx, matchID, aKey, bKey)
	if err != nil {
		return nil, nil, err
	}
	if auth == nil || battle == nil {
		return nil, nil, errBattleAuthStale
	}
	return auth, battle, nil
}

func unmarshalBattleAuth(matchID uint64, payload []byte, out *dsv1.BattleDSAuthStorageRecord) error {
	if err := proto.Unmarshal(payload, out); err != nil {
		return fmt.Errorf("battle %d bad auth proto: %w", matchID, err)
	}
	if out.MatchId == 0 {
		out.MatchId = matchID
	}
	if out.MatchId != matchID {
		return fmt.Errorf("battle %d auth id mismatch: %d", matchID, out.MatchId)
	}
	return nil
}

func battleTerminal(state string) bool {
	return state == "ended" || state == "abandoned"
}

func battleCredentialPreparableState(state string) bool {
	return state == "warming" || state == "ready" || state == "running"
}

// battlePreactiveAuthority 定义“GSA 已确认、但尚无任何 active 凭据”的窗口。
// 该窗口从 persistent allocation fence 继承而来，Prepare/Stage/MarkDelivered
// 都只能改变内容，不能给 auth/battle 增加 TTL；首个 ActivateHeartbeat 才结束它。
func battlePreactiveAuthority(authRecord *dsv1.BattleDSAuthStorageRecord, battle *dsv1.BattleStorageRecord) bool {
	return authRecord != nil && battle != nil &&
		authRecord.GetActive() == nil &&
		authRecord.GetPhase() == dsv1.BattleAuthPhase_BATTLE_AUTH_PHASE_BOOTSTRAP &&
		battle.GetState() == "warming" &&
		battle.GetLastVerifiedGen() == 0 && battle.GetLastVerifiedJti() == "" &&
		battle.GetLastVerifiedWriterEpoch() == 0
}

func validBattleHeartbeatState(state string) bool {
	return state == "" || state == "ready" || state == "running" || state == "ended"
}

func containsBattleHashTag(key string, matchID uint64) bool {
	open := strings.IndexByte(key, '{')
	if open < 0 {
		return false
	}
	closeOffset := strings.IndexByte(key[open+1:], '}')
	if closeOffset < 0 {
		return false
	}
	close := open + 1 + closeOffset
	return key[open+1:close] == strconv.FormatUint(matchID, 10)
}

// rosterAbsentIDs 返回 roster 里没出现在 census 中的玩家(保持 roster 原序)。
//
// 只在 BattleHeartbeatInput.CensusPresent 为真时调用:census 缺席时返回「全员缺席」
// 会把每一局都判弃,那道守卫必须留在调用点,本函数不替它兜底。
func rosterAbsentIDs(roster, census []uint64) []uint64 {
	if len(roster) == 0 {
		return nil
	}
	present := make(map[uint64]struct{}, len(census))
	for _, id := range census {
		present[id] = struct{}{}
	}
	var absent []uint64
	for _, id := range roster {
		if _, ok := present[id]; !ok {
			absent = append(absent, id)
		}
	}
	return absent
}

// rosterGateArmable 与 biz 侧同名函数同语义(判据是 battle 年龄,对任何副本同一答案)。
// 刻意各留一份而不跨层共享:两条心跳路径(legacy / Model B)本就分处两层,
// 为一个 3 行纯函数建跨层依赖不值当;语义漂移由两侧各自的用例钉死。
func rosterGateArmable(allocatedAtMs, nowMs, armWindowMs int64) bool {
	if armWindowMs <= 0 || allocatedAtMs <= 0 {
		return false
	}
	age := nowMs - allocatedAtMs
	return age >= 0 && age <= armWindowMs
}
