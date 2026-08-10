// Package biz 是 hub_allocator 服务的业务逻辑层(W4 ⑤,2026-06-06)。
//
// 职责(docs/design/go-services.md §2.12):大厅 DS 分片调度。
//   - AssignHub:玩家进大厅,按 region + 队友 + 最空分片选一个 hub DS,签 hub DSTicket
//   - ReleaseHub:玩家离开大厅,退分片占位
//   - TransferHub:跨分片传送,先占新分片再切归属,最后退旧分片,重签票据
//   - ListHubs:运维/调试查询分片负载
//   - Heartbeat:Hub DS 每 5s 主动上报(单向 unary),刷新在线数 + 心跳时刻
//   - RunHeartbeatSweep:后台扫描 active ZSET,心跳超时 → 标记 draining 停止分配(不变量 §4)
//
// 关键不变量:
//   - 玩家在线只在一个 hub(不变量 §1,GetAssignment 幂等;已分配 → 重签票不重复占位)
//   - hub DSTicket 短时效(不变量 §3,由 TicketSigner 经 pkg/auth 签 5min)
//
// 容量计数说明:player_count 由 hub_allocator 维护(Assign 自增 / Release 自减,容量判定基准);
// 真实 Hub DS Heartbeat 上报的在线数会回写对账(W4 ⑤ Mock 期无真实 DS,仅由分配计数维护)。
package biz

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/luyuancpp/pandora/pkg/auth"
	"github.com/luyuancpp/pandora/pkg/errcode"
	plog "github.com/luyuancpp/pandora/pkg/log"
	pmw "github.com/luyuancpp/pandora/pkg/middleware"
	"github.com/luyuancpp/pandora/pkg/releasetrack"
	"github.com/luyuancpp/pandora/pkg/safego"
	"github.com/luyuancpp/pandora/pkg/sessiongate"
	hubv1 "github.com/luyuancpp/pandora/proto/gen/go/pandora/hub/v1"

	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/conf"
	"github.com/luyuancpp/pandora/services/battle/hub_allocator/internal/data"
)

// 分片状态常量(对应 proto string state 字段)。
const (
	stateWarming  = "warming" // 分片已播种但尚未收到首个(鉴权)心跳:不可被 AssignHub 选中
	stateReady    = "ready"
	stateDraining = "draining"
	stateStopping = "stopping"
)

// Heartbeat 响应控制指令常量。
const (
	commandNone  = ""
	commandStop  = "stop"  // 通知孤儿 Hub DS(无对应分片镜像)自行停机
	commandDrain = "drain" // 通知 draining 分片上的 Hub DS 开始优雅迁移(下发 grace_seconds 倒计时)
)

// 迁移原因常量(HubMigrateEvent.reason)。
const migrateReasonConsolidation = "consolidation"

// presenceRefreshTimeout 是心跳后异步续期在场玩家 HUB 位置的独立 ctx 预算(在线保活,弱依赖)。
const presenceRefreshTimeout = 3 * time.Second

// TicketSigner 抽象 hub DSTicket 签发(biz 不依赖 pkg/auth 具体实现,便于测试)。
type TicketSigner interface {
	// SignHubTicket 给 playerID 签一张 hub DSTicket,返回 token + 过期毫秒。
	// roleID(选角权威化 2026-07-08):玩家已选角色,>0 时盖进票据 role_id claim
	// (DS 验签后直接用它 spawn);0 = 未选角,claim 不序列化(与旧票兼容)。
	SignHubTicket(playerID uint64, roleID uint32, binding HubTicketBinding) (token string, expiresAtMs int64, err error)
}

// HubTicketBinding 把 hub 入场票绑定到当前归属版本和目标 DS active 凭据。
// legacy 模式使用零值；Model B 必须六项完整。
type HubTicketBinding struct {
	PodName         string
	InstanceUID     string
	ProtocolEpoch   uint32
	CredentialGen   uint64
	CredentialJTI   string
	HubAssignmentID string
	WriterEpoch     uint32
	ReleaseTrack    string
	// SourceMatchID:Battle→Hub 回流 fence(pkg/auth DSTicketClaims*.SourceMatchID)。
	// 仅 AssignHub 回流路径 >0;Transfer/迁移重签为 0。
	SourceMatchID uint64
	// SessionJTI:请求方登录会话 jti(R6 复审 P0-3 → R7 收口,盖进 v2 票据 sjti claim;
	// login VerifyDSTicket 在线核销时复核会话现行性)。AssignHub(login 透传)、
	// Transfer(请求方会话证据)、迁移重签(会话权威当前代)均应非空;
	// 空只剩 dev 无证据链路,prod 兑换点(RequireTicketSessionCurrent)对空 sjti 硬拒。
	SessionJTI string
}

// HubMigratePusher 抽象强制整合迁移通知推送(走 Kafka topic pandora.hub.migrate,key=player_id)。
// 弱依赖:nil 时跳过推送(整合仍做服务端权威搬迁,Hub DS drain 心跳指令兼底客户端重连)。
type HubMigratePusher interface {
	// PushMigrate 把 HubMigrateEvent 序列化后的 payload 推给单个玩家。
	PushMigrate(ctx context.Context, playerID uint64, payload []byte) error
}

// HubUsecase 是 hub_allocator 业务逻辑核心。
type HubUsecase struct {
	repo    data.HubRepo
	fleet   HubFleetProvider
	scaler  HubFleetScaler
	signer  TicketSigner
	migrate HubMigratePusher
	locator data.HubLocationChecker
	cfg     conf.HubConf

	// ownerLease / ownerLeaseRequired:owner 权威实例租约双写(owner-authority.md
	// migrate ⑥;nil = 未启用;required 语义见 biz/owner_lease.go renewOwnerLeaseGate)。
	ownerLease         OwnerLeaseRenewer
	ownerLeaseRequired bool

	// ownerAuth / ownerAdmitted:owner 迁移弱依赖调用面 + census 已准入缓存
	// (owner-authority.md migrate ①/③④;见 biz/owner_authority.go)。
	ownerAuth     OwnerAuthority
	ownerAdmitted sync.Map

	// sessGateRequireSJTI 票据 sjti 绑定强制门(R7 收口,SetSessionGateRequireSJTI 注入)。
	// false(默认)= 兼容档:ACK 收到空 sjti 告警放行;true = 硬拒(旧 DS 排空后激活)。
	sessGateRequireSJTI bool

	// sessGate 会话现行性权威只读视图(R7 复审 P0-3,SetSessionGate 注入;nil = dev 无权威)。
	// 两个用途:①系统发起的迁移重签(migratePlayer)读玩家当前会话 jti 签进 sjti claim
	// (推送目标就是当前会话持有者);②AcknowledgeAdmission 入场确认时复核票据携带的
	// sjti 仍是当前一代(v2 Hub 本地验票不经 Login 在线兑换,此处是唯一在线会话门)。
	sessGate sessiongate.Gate
	// requireHeartbeatReady:播种分片镜像时先置 warming,等首个通过 Guard 的 Hub DS 心跳才转 ready
	// (审核 P1:agones PATCH/进程拉起成功 ≠ DS 已真正鉴权回调,不能直接当 ready 否则会把玩家路由到
	// 一个从未成功心跳的 Hub)。mode=agones 置 true;mock/local 不置(无真实心跳/保 dev 自测不坏)。
	requireHeartbeatReady bool
	// dsTokenGeneration:令牌代际绑定开关(审核 P1-6/P1-8;仅 agones + ds_auth.mode=enforce 置 true)。
	// 开启后拓扑对账把候选令牌代际(Redis INCR 单调值)写进镜像 CurrentTokenGen,重签(gen 递增)时复位分片 warming;
	// 心跳侧只有携带当前代际已验签令牌的心跳才能把 warming 翻回 ready(gen 精确相等)。off/permissive 不开:
	// 守卫不验签、心跳无可信 gen,开了会自锁。
	dsTokenGeneration bool
	// authRepo:Model B「Redis 唯一授权权威」授权记录仓(decision-revisit-ds-callback-auth §7)。
	// 仅 ds_auth.authority_mode=redis(agones+enforce)时由 main 注入;nil = legacy 代际门路径
	// (mock/local/off 及默认 legacy 权威模式)。装配后:①心跳走 ActivateHeartbeat 单事务线性化点
	// (授权 promote + 分片 warming→ready + 投影 active 元组同一 EXEC),stale 一律 fail-closed;
	// ②AssignHub/TransferHub 走 ReserveRoutableSeat/CheckRoutable 原子终态门。
	// 此时 dsTokenGeneration 关闭(Model B 授权记录取代 legacy 镜像代际门)。
	authRepo data.HubAuthRepo
	// authTTL:Model B 授权记录键 TTL(CE8:必须独立于 shardTTL,授权寿命远长于分片镜像 TTL,
	// 否则授权键被 shardTTL 提前过期会导致「有效凭据被判 stale」)。main 注入(默认 2×HubTokenTTL,floor 48h)。
	authTTL time.Duration
	// releasePolicy 只决定无 assignment 玩家首次尝试的轨；实际命中轨写入 assignment 后粘性。
	releasePolicy releasetrack.Policy
	// writerFence:写者继任租约视图(R9 P0-7,session-generation-rollout.md §5;
	// SetWriterFence 注入,nil = 未启用——dev/mock 或单副本 Recreate 部署)。入口层
	// requireWriter 快速拒写 + 后台 sweep 非写者跳 tick;存储级最终防线见
	// data/writer_fence.go(同事务 fencing token 比较)。
	writerFence data.WriterFence
}

// HubCredential 是 service 层从**验签通过**的 Model B hub 令牌抽出的凭据身份(§7),
// 由 HeartbeatWithCredential 透传到 authRepo.ActivateHeartbeat 做 promote/validate 匹配。
type HubCredential struct {
	InstanceUID   string
	ProtocolEpoch uint32
	Gen           uint64
	JTI           string
	TokenSHA256   string
	Kid           string
	WriterEpoch   uint32
}

// NewHubUsecase 构造 HubUsecase。
func NewHubUsecase(repo data.HubRepo, fleet HubFleetProvider, signer TicketSigner, cfg conf.HubConf) *HubUsecase {
	var scaler HubFleetScaler
	if s, ok := fleet.(HubFleetScaler); ok {
		scaler = s
	}
	return &HubUsecase{repo: repo, fleet: fleet, scaler: scaler, signer: signer, cfg: cfg}
}

// SetMigratePusher 注入强制整合迁移通知推送器(弱依赖,不改 NewHubUsecase 签名以不破现有测试/调用方)。
func (u *HubUsecase) SetMigratePusher(p HubMigratePusher) { u.migrate = p }

// SetLocationChecker 注入 player_locator 位置检查器(弱依赖:玩家切线护栏,nil 时跳过战斗/匹配中检查)。
func (u *HubUsecase) SetLocationChecker(c data.HubLocationChecker) { u.locator = c }

// SetRequireHeartbeatReady 开启「先 warming、首个鉴权心跳才 ready」(agones 真 DS 链路置 true)。
// off/mock/local 不置:无真实心跳的模式仍直接播种 ready,保持现有 dev/离线联调行为不变。
func (u *HubUsecase) SetRequireHeartbeatReady(b bool) { u.requireHeartbeatReady = b }

// SetDSTokenGeneration 开启令牌代际绑定(仅 agones + enforce;见字段注释)。
func (u *HubUsecase) SetDSTokenGeneration(b bool) { u.dsTokenGeneration = b }

// SetAuthRepo 注入 Model B 授权记录仓(§7;仅 ds_auth.authority_mode=redis 时装配)。
// 装配后心跳走 ActivateHeartbeat 单事务线性化点、AssignHub/TransferHub 走 ReserveRoutableSeat/
// CheckRoutable 原子终态门;nil 保持 legacy 代际门路径不变。
func (u *HubUsecase) SetAuthRepo(r data.HubAuthRepo) { u.authRepo = r }

// SetAuthTTL 注入 Model B 授权记录键 TTL(CE8;独立于 shardTTL,授权寿命更长)。
func (u *HubUsecase) SetAuthTTL(d time.Duration) { u.authTTL = d }

// SetReleaseTrackPolicy 注入 player_id 级确定性 cohort 策略。
func (u *HubUsecase) SetReleaseTrackPolicy(p releasetrack.Policy) { u.releasePolicy = p }

// SetWriterFence 注入写者继任租约视图(R9 P0-7;仅 Model B 生产由 main 注入)。
func (u *HubUsecase) SetWriterFence(f data.WriterFence) { u.writerFence = f }

// requireWriter 写路径入口门:未持有写者租约的副本快速拒写(ErrUnavailable 可重试,
// 重试会被路由到当前写者副本)。注意这只是快路径礼貌拒绝;防住「检查后失主」
// 竞态的最终防线是 data/writer_fence.go 的同事务存储级 fencing。
func (u *HubUsecase) requireWriter() error {
	if u.writerFence == nil {
		return nil
	}
	if _, held := u.writerFence.Current(); !held {
		return errcode.New(errcode.ErrUnavailable,
			"hub allocator writer lease not held on this replica; retry")
	}
	return nil
}

// confirmWriterForTicket 出票前写者复核(writer_fence.go 覆盖边界 ④):assignment 单键
// 无法进 {pod} slot fence 事务,票据只在「入口到返回全程持有租约」时交付。入口后失主
// 的在途请求走到这里被拦——存储侧可能已留下 assignment/席位(合法数据,继任者 CAS
// 接续或 TTL 回收),但票绝不交给调用方;ErrUnavailable 引导重试路由到新写者重签。
func (u *HubUsecase) confirmWriterForTicket(ctx context.Context, playerID uint64) error {
	if err := u.requireWriter(); err != nil {
		plog.With(ctx).Warnw("msg", "hub_ticket_withheld_writer_lost", "player_id", playerID)
		return err
	}
	return nil
}

// SetSessionGate 注入会话现行性权威只读视图(R7 复审 P0-3;nil = dev 无权威)。
// 迁移重签取玩家当前会话 jti 签进 sjti;AcknowledgeAdmission 复核票据 sjti 现行性。
func (u *HubUsecase) SetSessionGate(g sessiongate.Gate) { u.sessGate = g }

// SetSessionGateRequireSJTI 设置票据 sjti 绑定强制门(R7 收口,默认 false=兼容档)。
// false:空 sjti 告警放行(旧 Hub DS/旧签发面残票混版兼容);true:空 sjti 硬拒。
// 激活前提:全 fleet Hub DS 已转发 sjti、旧 DS 排空、等满一个票据最大 TTL。
func (u *HubUsecase) SetSessionGateRequireSJTI(require bool) { u.sessGateRequireSJTI = require }

// migrateResignSessionJTI 为系统发起的迁移重签解析玩家当前会话 jti(R7 复审 P0-3)。
// 返回 (jti, ok):
//   - sessGate nil(dev 无权威)→ ("", true):签空 sjti,与无权威部署语义一致;
//   - 权威不可达 → ("", false):fail-closed,本 tick 跳过,下个 tick 重试;
//   - 无会话(已登出)→ ("", true):照常完成服务端搬迁,签空 sjti——该票在 prod
//     兑换点必拒,但玩家重登后 login 会按新归属重发新票,推送对象本就不存在;
//   - 有会话 → (当前 jti, true):推送目标就是当前会话持有者,票据绑定其代际。
func (u *HubUsecase) migrateResignSessionJTI(ctx context.Context, playerID uint64) (string, bool) {
	if u.sessGate == nil {
		return "", true
	}
	jti, found, err := u.sessGate.CurrentJTI(ctx, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "migrate_resign_session_gate_unavailable",
			"player_id", playerID, "err", err)
		return "", false
	}
	if !found {
		return "", true
	}
	return jti, true
}

// authTTLDur 返回授权键 TTL:main 已注入用注入值;未注入(测试/兜底)回退 2×shardTTL,
// 绝不返回 0(0 = Redis 永不过期,授权键会泄漏)。
func (u *HubUsecase) authTTLDur() time.Duration {
	if u.authTTL > 0 {
		return u.authTTL
	}
	return u.shardTTL() * 2
}

// heartbeatMaxAgeMs 返回「分片心跳仍算新鲜」的最大毫秒(= 心跳超时阈值)。
// ReserveRoutableSeat/CheckRoutable 用它拒绝把玩家分到心跳已陈旧但镜像尚未被 sweep 标 draining 的分片。
func (u *HubUsecase) heartbeatMaxAgeMs() int64 {
	return u.cfg.HeartbeatTimeout.Std().Milliseconds()
}

// candidateTokenExp 返回播种/对账时写入镜像的令牌 exp 镜像值(仅调试/兼容,不再当代际):
// 仅代际绑定开启且候选带有效 exp 时记录,其余恒 0。代际识别已改用 candidateTokenGen。
func (u *HubUsecase) candidateTokenExp(expMs int64) uint64 {
	if u.dsTokenGeneration && expMs > 0 {
		return uint64(expMs)
	}
	return 0
}

// candidateTokenGen 返回播种/对账时写入镜像的令牌「代际」(Redis INCR 单调值):仅代际绑定
// 开启时记录候选 gen,其余恒 0(= 不启用;off/permissive 心跳无已验签 claims,开了会自锁)。
// 替代秒级 exp 代际:gen 来自 Redis INCR 单调,同秒多次重签也不碰撞(审核 P1-6),且可精确相等比较。
func (u *HubUsecase) candidateTokenGen(gen uint64) uint64 {
	if u.dsTokenGeneration {
		return gen
	}
	return 0
}

// initialShardState 返回播种新分片镜像的初始状态:需心跳确认时 warming,否则直接 ready。
func (u *HubUsecase) initialShardState() string {
	if u.requireHeartbeatReady {
		return stateWarming
	}
	return stateReady
}

func (u *HubUsecase) shardTTL() time.Duration         { return u.cfg.ShardTTL.Std() }
func (u *HubUsecase) assignTTL() time.Duration        { return u.cfg.AssignmentTTL.Std() }
func (u *HubUsecase) reservationTTL() time.Duration   { return u.cfg.ReservationTTL.Std() }
func (u *HubUsecase) transferCooldown() time.Duration { return u.cfg.TransferCooldown.Std() }
func (u *HubUsecase) retry() int                      { return u.cfg.OptimisticRetry }

// assignmentSagaTTL 是 assignment 记录(含 owner-cleanup saga 阶段字段)的持久化 TTL。
// Release/Departure/transfer cleanup 是显式精确操作,从不按时间推断;TTL 只兜底泄漏。
func (u *HubUsecase) assignmentSagaTTL() time.Duration {
	return u.assignTTL()
}

// ── RPC 1:AssignHub ───────────────────────────────────────────────────────────

// AssignResult 是 AssignHub 的出参。
type AssignResult struct {
	HubDSAddr   string
	HubTicket   string
	HubPodName  string
	ShardID     uint32
	TicketExpMs int64
}

// AssignHub 为玩家分配一个大厅 DS 分片。幂等:已分配且分片可用 → 重签票返回。
//
// roleID(选角权威化 2026-07-08):玩家已选角色(login 从 player_roles 读出透传)。
// >0 时覆盖归属镜像里的 role_id(login 是角色数据权威,换角重选以新值为准);
// 0 = 调用方不知角色,保留已存镜像值(Transfer/重签路径不丢角色)。
// 最终生效的 role 盖进本次签发的 hub 票据 claim(票据单一签发权威在本服务)。
//
// sourceMatchID(Battle→Hub 回流 fence,2026-07-21):login 三态门证明玩家原对局已
// 终局(ended/abandoned)时透传的原 Battle match_id;>0 时盖进本次 hub 票据的
// source_match_id claim,Hub DS 准入后用它写 SetLocation(HUB, fence) 通过 locator 的
// BATTLE→HUB guard。0 = 普通登录/非回流。仅影响票据 claim,不进归属镜像
// (fence 是一次性回流凭证,Transfer/迁移重签不携带)。
// sessionJTI(R6 复审 P0-3):login 透传的请求方会话 jti,盖进本次 hub 票据 sjti claim
// (VerifyDSTicket 在线核销时复核现行性,响应窗口交付的旧票在兑换点作废)。
// 空 = 旧调用方/dev 直连,票据不带 sjti(兼容窗)。
func (u *HubUsecase) AssignHub(ctx context.Context, playerID uint64, region string, teamID uint64, roleID uint32, sourceMatchID uint64, sessionJTI string) (*AssignResult, error) {
	if err := u.requireWriter(); err != nil {
		return nil, err
	}
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	if region == "" {
		region = u.cfg.DefaultRegion
	}

	for attempt := 0; attempt < 8; attempt++ {
		existing, found, err := u.repo.GetAssignment(ctx, playerID)
		if err != nil {
			return nil, err
		}
		if found && (existing.GetTransferCleanupPending() || existing.GetReleaseCleanupPending()) {
			var stillFound bool
			existing, stillFound, err = u.resumeAssignmentCleanup(ctx, playerID, existing.GetAssignmentId())
			if err != nil {
				return nil, err
			}
			if !stillFound {
				continue
			}
		}
		effectiveRole := roleID
		desiredTrack := u.releasePolicy.Select(playerID)
		if found {
			var trackErr error
			desiredTrack, trackErr = stickyReleaseTrack(existing.GetReleaseTrack())
			if trackErr != nil {
				return nil, trackErr
			}
			effectiveRole = effectiveRoleID(roleID, existing.RoleId)
			current, reusable, rerr := u.assignmentRoutable(ctx, playerID, existing)
			if rerr != nil {
				return nil, rerr // Redis/授权读取失败不能降级成另分配
			}
			if reusable || assignmentSameInstance(existing, &current) {
				// assignment 可以比 admission ledger 活得更久：clean Logout 会精确删除 session，
				// 未进场 reservation 也会绝对到期。重签票前必须在 {pod} 同槽事务里重新确保
				// “已有 session 或新鲜 reservation”；否则会返回一张必被 Admission 拒绝的票。
				// ReserveAssignment 对已有 session/reservation 幂等，不重复计数；这里也绝不能在
				// 后续跨槽 assignment CAS loser 时补偿删除，因为该 seat 可能是原连接的共享 session。
				ensured, ensureErr := u.ensureExistingAssignmentSeat(ctx, playerID, existing, &current)
				if ensureErr == nil {
					next := proto.Clone(existing).(*hubv1.HubAssignmentStorageRecord)
					next.HubAddr, next.ShardId, next.Region = ensured.HubAddr, ensured.ShardID, ensured.Region
					next.ReleaseTrack = ensured.ReleaseTrack
					next.RoleId = effectiveRole
					if next.AssignmentId == "" {
						next.AssignmentId = uuid.NewString()
					}
					bindAssignmentAuth(next, ensured)
					// 即使归属 bytes 完全相同也必须走 CAS SET 刷新 assignment TTL。CAS 仍以
					// 完整旧 bytes 为前置；失败时不清理 ensure 的共享 seat，交 winner 精确释放
					// 或让新建 reservation 的有界 TTL 回收。
					swapped, serr := u.repo.CompareAndSwapAssignment(ctx, playerID, existing, next, u.assignmentSagaTTL())
					if serr != nil {
						return nil, serr
					}
					if !swapped {
						continue
					}
					u.addShardMember(ctx, next.HubPodName, playerID)
					if werr := u.confirmWriterForTicket(ctx, playerID); werr != nil {
						return nil, werr
					}
					return u.signResult(ctx, playerID, effectiveRole, next, sourceMatchID, sessionJTI)
				}
				if errcode.As(ensureErr) != errcode.ErrHubNoAvailable {
					return nil, ensureErr
				}
				// 旧 assignment 已无 seat 且原分片已满/漂移时，继续走新 assignment 选择；
				// 不能反复刷新旧归属后返回永远无法 Admission 的票。
			}
		}

		if err := u.ensureShards(ctx, region, desiredTrack); err != nil {
			return nil, err
		}
		assignmentID := uuid.NewString()
		target, seat, err := u.selectAndReserveShard(ctx, playerID, assignmentID, region, teamID, "", desiredTrack)
		// canary 明确无可用容量时回退 stable；stable 绝不反向进入 canary。
		//
		// 这里**不再**限定“首次分配”(原 !found 条件)。走到本行时已经在为玩家新建
		// assignment(上方 assignmentID = uuid.NewString())：旧归属要么不可路由、要么其
		// 分片已满/漂移，粘性本就无从保留。此时再坚持 canary 只会让整个 canary cohort
		// 里“座位已失效”的那批玩家反复拿 ErrHubNoAvailable，直到 assignment TTL 过期
		// 才自愈——而 canary 无容量最典型的成因恰恰是金丝雀存在的理由本身(canary 镜像
		// CrashLoop / 永不心跳 / 回滚把 replicas 调 0)。
		//
		// 更关键的是运维止血手段会失灵：把 canary_percent 调 0 只改 releasePolicy.Select，
		// 而已有归属走的是 stickyReleaseTrack(existing) 这条持久化路径、根本不读策略。
		// §9.21 与验收底线第 7 条要求“异常时能立即把 Canary 权重归零、Stable 继续服务”，
		// 该要求必须对已粘住的玩家同样成立。
		//
		// 回退方向是 canary→stable(降级到安全轨)，与 §9.21“同一玩家固定 release track”
		// 的意图不冲突：那条约束是为了防止玩家在共存窗口内被反复甩到未验证的新版本，
		// 而不是把玩家钉死在一个已经不可用的轨道上。反向(stable→canary)仍然禁止。
		// 对照实现：Battle 侧同场景的回退本就是无条件的(ds_allocator
		// internal/data/agones_allocator.go 的 tracks = [desired, stable])。
		if err != nil && desiredTrack == releasetrack.Canary && errcode.As(err) == errcode.ErrHubNoAvailable {
			if ensureErr := u.ensureShards(ctx, region, releasetrack.Stable); ensureErr != nil {
				return nil, ensureErr
			}
			target, seat, err = u.selectAndReserveShard(ctx, playerID, assignmentID, region, teamID, "", releasetrack.Stable)
		}
		if err != nil {
			if errcode.As(err) == errcode.ErrHubNoAvailable {
				u.tryScaleOutOnNoCapacity(ctx, region)
			}
			return nil, err
		}
		assignment := &hubv1.HubAssignmentStorageRecord{}
		if found {
			assignment = proto.Clone(existing).(*hubv1.HubAssignmentStorageRecord)
		}
		assignment.PlayerId = playerID
		assignment.HubPodName = target.HubPodName
		assignment.HubAddr = target.HubAddr
		assignment.ShardId = target.ShardId
		assignment.Region = target.Region
		assignment.TeamId = teamID
		assignment.AssignedAtMs = time.Now().UnixMilli()
		assignment.RoleId = effectiveRole
		assignment.AssignmentId = assignmentID
		assignment.ReleaseTrack = target.ReleaseTrack
		bindAssignmentAuth(assignment, seat)
		// 先签票、再发布 assignment:签名器失败时可以用 reservation identity
		// 精确补偿,既不暴露拿不到票的归属,也不泄漏容量。
		signedResult, signErr := u.signResult(ctx, playerID, effectiveRole, assignment, sourceMatchID, sessionJTI)
		if signErr != nil {
			u.compensateReservedSeat(ctx, target.HubPodName, playerID, assignmentID, seat)
			return nil, signErr
		}
		var expected *hubv1.HubAssignmentStorageRecord
		if found {
			expected = existing
		}
		retrySaga, sagaErr := u.replaceAssignmentSaga(ctx, playerID, expected, assignment, seat,
			func() {
				if teamID != 0 {
					if terr := u.repo.SetTeamShard(ctx, teamID, target.HubPodName, u.assignTTL()); terr != nil {
						plog.With(ctx).Warnw("msg", "set_team_shard_failed", "team_id", teamID, "err", terr)
					}
				}
			},
			"Hub replacement assignment disappeared during cleanup")
		if sagaErr != nil {
			return nil, sagaErr
		}
		if retrySaga {
			continue
		}
		plog.With(ctx).Infow("msg", "hub_assigned",
			"player_id", playerID, "pod", target.HubPodName, "shard_id", target.ShardId,
			"region", target.Region, "release_track", target.ReleaseTrack)
		return signedResult, nil
	}
	return nil, errcode.New(errcode.ErrHubNoAvailable, "player %d assignment changed concurrently", playerID)
}

// ── RPC 2:ReleaseHub ──────────────────────────────────────────────────────────

// ReleaseHub 玩家离开大厅,退分片占位 + 删归属。幂等:无归属视为已离开。
func (u *HubUsecase) ReleaseHub(ctx context.Context, playerID uint64) error {
	if err := u.requireWriter(); err != nil {
		return err
	}
	if playerID == 0 {
		return errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	for attempt := 0; attempt < 8; attempt++ {
		assignment, found, err := u.repo.GetAssignment(ctx, playerID)
		if err != nil {
			return err
		}
		if !found {
			return nil // 幂等
		}
		if assignment.GetTransferCleanupPending() || assignment.GetReleaseCleanupPending() {
			_, stillFound, cleanupErr := u.resumeAssignmentCleanup(ctx, playerID, assignment.GetAssignmentId())
			if cleanupErr != nil {
				return cleanupErr
			}
			if !stillFound {
				return nil
			}
			continue
		}
		if u.authRepo != nil {
			current, reusable, routeErr := u.assignmentRoutable(ctx, playerID, assignment)
			if routeErr != nil {
				return routeErr
			}
			if !reusable {
				if assignmentSameInstance(assignment, &current) {
					// 同实例普通凭据轮换：先把归属 CAS 到当前 active，再重新进入精确
					// Release；不占新座，也不会拿旧 tuple 删除归属后退座失败。
					next := proto.Clone(assignment).(*hubv1.HubAssignmentStorageRecord)
					bindAssignmentAuth(next, &current)
					swapped, swapErr := u.repo.CompareAndSwapAssignment(ctx, playerID, assignment, next, u.assignmentSagaTTL())
					if swapErr != nil {
						return swapErr
					}
					if !swapped {
						continue
					}
					continue
				}
				return errcode.New(errcode.ErrInvalidState,
					"hub assignment is not bound to the current active credential")
			}
			ref := transferCleanupRef(assignment)
			// Index-first: after this succeeds, a process crash before/after the
			// tombstone CAS remains enumerable. CAS losers remove only their exact
			// assignment ref.
			if indexErr := u.repo.RegisterTransferCleanup(ctx, assignment.GetHubPodName(), ref); indexErr != nil {
				return indexErr
			}
			next := proto.Clone(assignment).(*hubv1.HubAssignmentStorageRecord)
			next.ReleaseCleanupPending = true
			next.ReleaseCleanupMatchId = 0
			next.ReleaseCleanupPlacementVersion = 0
			next.ReleaseCleanupOperationId = ""
			marked, markErr := u.repo.CompareAndSwapAssignment(ctx, playerID, assignment, next, u.assignmentSagaTTL())
			if markErr != nil {
				// Unknown CAS result: retain the ref. Reconciler removes an orphan or
				// resumes the durable tombstone; never report success here.
				return markErr
			}
			if !marked {
				u.removeTransferCleanupRef(ctx, assignment.GetHubPodName(), ref)
				continue
			}
			_, stillFound, cleanupErr := u.resumeAssignmentCleanup(ctx, playerID, assignment.GetAssignmentId())
			if cleanupErr != nil {
				return cleanupErr
			}
			if stillFound {
				return errcode.New(errcode.ErrInvalidState, "Hub release cleanup did not delete assignment")
			}
			plog.With(ctx).Infow("msg", "hub_released", "player_id", playerID, "pod", assignment.HubPodName)
			return nil
		}
		// Legacy/off path has no exact Model-B owner. Preserve the historical
		// CAS/delete behavior; placement enforce cannot reach this branch because
		// strict Admission itself requires authRepo.
		deleted, derr := u.repo.CompareAndSwapAssignment(ctx, playerID, assignment, nil, 0)
		if derr != nil {
			return derr
		}
		if !deleted {
			continue
		}
		u.releaseAssignmentSeat(ctx, assignment)
		u.removeShardMember(ctx, assignment.HubPodName, playerID)
		plog.With(ctx).Infow("msg", "hub_released", "player_id", playerID, "pod", assignment.HubPodName)
		return nil
	}
	return errcode.New(errcode.ErrInternal, "player %d release CAS retry exhausted", playerID)
}

// ── RPC 3:TransferHub ─────────────────────────────────────────────────────────

// TransferResult 是 TransferHub 的出参。
type TransferResult struct {
	NewHubDSAddr  string
	NewHubTicket  string
	NewHubPodName string
	TicketExpMs   int64
	// NewAssignmentID 本次迁移落地后的 assignment 标识(R9 复审 P0-6):供调用方
	// 在 post-check 失败时做「仍是本次迁移产物才回退」的条件补偿。
	NewAssignmentID string
}

// TransferHub 跨分片传送:先占新分片(失败不动旧分片),再切归属到新分片,最后退旧分片占位,重签票据。
func (u *HubUsecase) TransferHub(ctx context.Context, playerID uint64, targetHubID uint64) (*TransferResult, error) {
	if err := u.requireWriter(); err != nil {
		return nil, err
	}
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	for attempt := 0; attempt < 8; attempt++ {
		assignment, found, err := u.repo.GetAssignment(ctx, playerID)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, errcode.New(errcode.ErrHubTransferFailed, "player %d not in any hub", playerID)
		}
		if assignment.GetTransferCleanupPending() || assignment.GetReleaseCleanupPending() {
			var stillFound bool
			assignment, stillFound, err = u.resumeAssignmentCleanup(ctx, playerID, assignment.GetAssignmentId())
			if err != nil {
				return nil, err
			}
			if !stillFound {
				return nil, errcode.New(errcode.ErrHubTransferFailed,
					"player %d Hub assignment was released", playerID)
			}
		}
		if _, trackErr := stickyReleaseTrack(assignment.GetReleaseTrack()); trackErr != nil {
			return nil, trackErr
		}
		if u.authRepo != nil && !assignmentBindingV2Complete(assignment, playerID) {
			return nil, errcode.New(errcode.ErrHubTransferFailed,
				"player %d assignment is not a complete writer-v2 binding", playerID)
		}
		shards, err := u.repo.ListShards(ctx)
		if err != nil {
			return nil, err
		}
		target := selectTransferTarget(shards, assignment, targetHubID)
		if target == nil {
			return nil, errcode.New(errcode.ErrHubTransferFailed,
				"no ready target shard for player %d (target_hub_id=%d)", playerID, targetHubID)
		}

		if target.HubPodName == assignment.HubPodName {
			current, reusable, rerr := u.assignmentRoutable(ctx, playerID, assignment)
			if rerr != nil {
				return nil, errcode.New(errcode.ErrHubTransferFailed, "check current shard: %v", rerr)
			}
			if reusable || assignmentSameInstance(assignment, &current) {
				ensured, ensureErr := u.ensureExistingAssignmentSeat(ctx, playerID, assignment, &current)
				if ensureErr != nil {
					return nil, errcode.New(errcode.ErrHubTransferFailed,
						"ensure current shard %s admission seat: %v", target.HubPodName, ensureErr)
				}
				next := proto.Clone(assignment).(*hubv1.HubAssignmentStorageRecord)
				next.HubAddr, next.ShardId, next.Region = ensured.HubAddr, ensured.ShardID, ensured.Region
				next.ReleaseTrack = ensured.ReleaseTrack
				if next.AssignmentId == "" {
					next.AssignmentId = uuid.NewString()
				}
				bindAssignmentAuth(next, ensured)
				signedResult, signErr := u.transferResult(ctx, playerID, next.RoleId, next)
				if signErr != nil {
					return nil, signErr
				}
				swapped, serr := u.repo.CompareAndSwapAssignment(ctx, playerID, assignment, next, u.assignmentSagaTTL())
				if serr != nil {
					return nil, serr
				}
				if !swapped {
					continue
				}
				if werr := u.confirmWriterForTicket(ctx, playerID); werr != nil {
					return nil, werr
				}
				return signedResult, nil
			}
		}

		newAssignmentID := uuid.NewString()
		seat, rerr := u.reserveRoutableSeat(ctx, target.HubPodName, playerID, newAssignmentID)
		if rerr != nil {
			return nil, errcode.New(errcode.ErrHubTransferFailed,
				"reserve target shard %s failed: %v", target.HubPodName, rerr)
		}
		target = authoritativeShard(target, seat)
		newAssignment := proto.Clone(assignment).(*hubv1.HubAssignmentStorageRecord)
		newAssignment.PlayerId = playerID
		newAssignment.HubPodName = target.HubPodName
		newAssignment.HubAddr = target.HubAddr
		newAssignment.ShardId = target.ShardId
		newAssignment.Region = target.Region
		newAssignment.TeamId = assignment.TeamId
		newAssignment.AssignedAtMs = time.Now().UnixMilli()
		newAssignment.RoleId = assignment.RoleId
		newAssignment.AssignmentId = newAssignmentID
		newAssignment.ReleaseTrack = target.ReleaseTrack
		bindAssignmentAuth(newAssignment, seat)
		signedResult, signErr := u.transferResult(ctx, playerID, assignment.RoleId, newAssignment)
		if signErr != nil {
			u.compensateReservedSeat(ctx, target.HubPodName, playerID, newAssignmentID, seat)
			return nil, signErr
		}
		retrySaga, sagaErr := u.replaceAssignmentSaga(ctx, playerID, assignment, newAssignment, seat,
			nil, "Hub transfer assignment disappeared during cleanup")
		if sagaErr != nil {
			return nil, sagaErr
		}
		if retrySaga {
			continue
		}
		plog.With(ctx).Infow("msg", "hub_transferred",
			"player_id", playerID, "from", assignment.HubPodName, "to", target.HubPodName)
		return signedResult, nil
	}
	return nil, errcode.New(errcode.ErrHubTransferFailed, "player %d assignment changed concurrently", playerID)
}

// ── 玩家侧:线路列表 + 主动切线 ────────────────────────────────────────────────
// 经 Envoy :8443 客户端面(jwt_authn 注入 x-pandora-player-id),player_id 取自 JWT sub,
// 不信请求体。ListHubs/TransferHub 是后端内部/DS 调用,不经客户端面路由。

// HubLineView 是 ListHubLinesForPlayer 的单条出参(service 层转 proto HubLine)。
type HubLineView struct {
	LineNo      uint32
	ShardID     uint32
	PlayerCount int32
	Capacity    int32
	IsFull      bool
	IsCurrent   bool
}

// ListHubLinesForPlayer 列出玩家当前 region 可切换的大厅线路(客户端可见视图,隐藏 pod 名)。
// region 留空 = 用玩家当前归属的 region(服务端权威,忽略客户端申报);
// 玩家无归属时回退 reqRegion 或默认 region。
func (u *HubUsecase) ListHubLinesForPlayer(ctx context.Context, playerID uint64, reqRegion string) ([]*HubLineView, error) {
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}
	region, curPod := reqRegion, ""
	releaseTrack := u.releasePolicy.Select(playerID)
	if assignment, found, err := u.repo.GetAssignment(ctx, playerID); err != nil {
		return nil, err
	} else if found {
		if u.authRepo != nil {
			_, reusable, rerr := u.assignmentRoutable(ctx, playerID, assignment)
			if rerr != nil {
				return nil, rerr
			}
			if !reusable {
				return nil, errcode.New(errcode.ErrInvalidState,
					"hub assignment is not bound to the current active credential")
			}
		}
		region = assignment.Region // 归属 region 权威
		curPod = assignment.HubPodName
		var trackErr error
		releaseTrack, trackErr = stickyReleaseTrack(assignment.GetReleaseTrack())
		if trackErr != nil {
			return nil, trackErr
		}
	}
	if region == "" {
		region = u.cfg.DefaultRegion
	}
	shards, err := u.repo.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	shards, err = u.routableShardViews(ctx, shards)
	if err != nil {
		return nil, err
	}
	return buildHubLinesForTrack(shards, region, curPod, releaseTrack), nil
}

// buildHubLines 把某 region 的 ready 分片按 shard_id 升序编成 1-based 线路视图("1线/2线/…")。
func buildHubLines(shards []*hubv1.HubShardStorageRecord, region, curPod string) []*HubLineView {
	return buildHubLinesForTrack(shards, region, curPod, "")
}

func buildHubLinesForTrack(shards []*hubv1.HubShardStorageRecord, region, curPod, releaseTrack string) []*HubLineView {
	ready := make([]*hubv1.HubShardStorageRecord, 0, len(shards))
	for _, s := range shards {
		track, err := stickyReleaseTrack(s.GetReleaseTrack())
		if err == nil && s.Region == region && s.State == stateReady && (releaseTrack == "" || track == releaseTrack) {
			ready = append(ready, s)
		}
	}
	sort.Slice(ready, func(i, j int) bool { return ready[i].ShardId < ready[j].ShardId })
	out := make([]*HubLineView, 0, len(ready))
	for i, s := range ready {
		out = append(out, &HubLineView{
			LineNo:      uint32(i + 1),
			ShardID:     s.ShardId,
			PlayerCount: s.PlayerCount,
			Capacity:    s.Capacity,
			IsFull:      s.PlayerCount >= s.Capacity,
			IsCurrent:   curPod != "" && s.HubPodName == curPod,
		})
	}
	return out
}

// lineNoOfShard 返回某 region 内目标 shard_id 的 1-based 线路号(不在 ready 列表返 0)。
func lineNoOfShard(shards []*hubv1.HubShardStorageRecord, region, releaseTrack string, shardID uint32) uint32 {
	for _, v := range buildHubLinesForTrack(shards, region, "", releaseTrack) {
		if v.ShardID == shardID {
			return v.LineNo
		}
	}
	return 0
}

// TransferToLineResult 是 TransferToLineForPlayer 的出参。
type TransferToLineResult struct {
	NewHubDSAddr string
	NewHubTicket string
	NewShardID   uint32
	LineNo       uint32
}

// TransferToLineForPlayer 玩家主动切换到指定线路(换实例,AB 互不可见)。护栏:
//  1. 战斗/匹配中禁切(查 player_locator,fail-closed:presence 不确定即拒,INC-20260722-002)
//  2. 冷却防刷(SET NX EX,窗口内再切拒绝;失败释放占坑让玩家可重试)
//  3. 目标线路不存在/非本 region → ErrHubTransferFailed;已满 → ErrHubLineFull
//  4. 复用内部 TransferHub 完成 占新→切归属→退旧→重签票
func (u *HubUsecase) TransferToLineForPlayer(ctx context.Context, playerID uint64, targetShardID uint32) (*TransferToLineResult, error) {
	if err := u.requireWriter(); err != nil {
		return nil, err
	}
	if playerID == 0 {
		return nil, errcode.New(errcode.ErrInvalidArg, "player_id required")
	}

	// 护栏 1:战斗/匹配中禁切。切线 = 进入另一台 Hub DS:locator RPC 失败、非 OK、
	// OFFLINE/未知状态都不能证明玩家不在旧 DS 战斗/匹配,必须在任何副作用(冷却
	// 占坑/占座/签票)之前 fail-closed 拒绝,客户端退避重试(§9.22 UNKNOWN 不得授权
	// 新归属;INC-20260722-002 废止原"弱依赖告警放行"契约)。彻底关死双 DS 口子仍
	// 需 Owner Authority 全链路接线(owner-authority migrate,独立工作流)。
	if u.locator != nil {
		blocked, lerr := u.locator.InBattleOrMatching(ctx, playerID)
		if lerr != nil {
			plog.With(ctx).Warnw("msg", "transfer_locator_check_failed_fail_closed",
				"player_id", playerID, "err", lerr)
			return nil, errcode.New(errcode.ErrUnavailable,
				"player %d presence unknown, hub line switch rejected, retry later", playerID)
		}
		if blocked {
			return nil, errcode.New(errcode.ErrHubTransferNotInHub,
				"player %d in battle/matching, cannot switch hub line", playerID)
		}
	} else {
		// nil checker = dev 联调模式(locator 未配)。生产装配缺失属部署错误:
		// 每次放行都留痕,防静默跳过护栏(INC-20260722-002 放大因素)。
		plog.With(ctx).Warnw("msg", "transfer_locator_checker_absent_dev_only", "player_id", playerID)
	}

	// 任何 cooldown SET 副作用之前先证明当前 assignment 是完整 writer-v2 且仍精确绑定
	// Redis active。legacy/future/缺 JTI 的旧 writer 记录必须零变更拒绝。
	if u.authRepo != nil {
		assignment, found, readErr := u.repo.GetAssignment(ctx, playerID)
		if readErr != nil {
			return nil, readErr
		}
		if !found {
			return nil, errcode.New(errcode.ErrHubTransferNotInHub, "player %d not in any hub", playerID)
		}
		_, reusable, routeErr := u.assignmentRoutable(ctx, playerID, assignment)
		if routeErr != nil {
			return nil, routeErr
		}
		if !reusable {
			return nil, errcode.New(errcode.ErrInvalidState,
				"hub assignment is not bound to the current active credential")
		}
	}

	// 护栏 2:冷却防刷(先占坑;后续失败再释放让玩家可立即重试)
	ok, err := u.repo.TryTransferCooldown(ctx, playerID, u.transferCooldown())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errcode.New(errcode.ErrHubTransferCooldown, "player %d transfer on cooldown", playerID)
	}

	res, terr := u.transferToLineInner(ctx, playerID, targetShardID)
	if terr != nil {
		if cerr := u.repo.ClearTransferCooldown(ctx, playerID); cerr != nil {
			plog.With(ctx).Warnw("msg", "clear_transfer_cooldown_failed", "player_id", playerID, "err", cerr)
		}
		return nil, terr
	}
	return res, nil
}

// requireCallerSessionCurrent(R7 收口 P0-4):玩家侧写路径临界区内的会话终检。
// RPC 入口的 SessionCurrent 中间件只保证"进门时现行",到内部占坑/CAS 之间是开放窗口;
// 本检查在副作用临界点复核请求方自证 jti(Envoy 验签 payload 头)仍是权威当前代。
// callerJTI 空(内网直连/dev 无证据)或 sessGate nil → 跳过,保持 dev 语义。
func (u *HubUsecase) requireCallerSessionCurrent(ctx context.Context, playerID uint64) error {
	callerJTI := pmw.SessionJTIFromContext(ctx)
	if u.sessGate == nil || callerJTI == "" {
		return nil
	}
	cur, found, err := u.sessGate.CurrentJTI(ctx, playerID)
	if err != nil {
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"session authority unavailable during hub line transfer")
	}
	if !found {
		return errcode.New(errcode.ErrUnauthorized,
			"player %d has no current session; hub line transfer rejected", playerID)
	}
	if cur != callerJTI {
		plog.With(ctx).Warnw("msg", "hub_transfer_session_superseded", "player_id", playerID)
		return errcode.New(errcode.ErrSessionSuperseded,
			"hub line transfer requested by a superseded session")
	}
	return nil
}

// transferToLineInner 做目标解析 + 满员判定 + 委托内部 TransferHub。
func (u *HubUsecase) transferToLineInner(ctx context.Context, playerID uint64, targetShardID uint32) (*TransferToLineResult, error) {
	assignment, found, err := u.repo.GetAssignment(ctx, playerID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errcode.New(errcode.ErrHubTransferNotInHub, "player %d not in any hub", playerID)
	}
	if u.authRepo != nil {
		_, reusable, routeErr := u.assignmentRoutable(ctx, playerID, assignment)
		if routeErr != nil {
			return nil, routeErr
		}
		if !reusable {
			return nil, errcode.New(errcode.ErrInvalidState,
				"hub assignment is not bound to the current active credential")
		}
	}

	shards, err := u.repo.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	// 目标线路必须是本 region 的 ready 分片
	var target *hubv1.HubShardStorageRecord
	assignmentTrack, trackErr := stickyReleaseTrack(assignment.GetReleaseTrack())
	if trackErr != nil {
		return nil, trackErr
	}
	for _, s := range shards {
		shardTrack, shardTrackErr := stickyReleaseTrack(s.GetReleaseTrack())
		if shardTrackErr == nil && shardTrack == assignmentTrack && s.ShardId == targetShardID && s.Region == assignment.Region && s.State == stateReady {
			target = s
			break
		}
	}
	if target == nil {
		return nil, errcode.New(errcode.ErrHubTransferFailed,
			"line shard_id=%d not available in region %s", targetShardID, assignment.Region)
	}
	// 已满且不是当前线路 → 明确"线路已满"
	if target.HubPodName != assignment.HubPodName && target.PlayerCount >= target.Capacity {
		return nil, errcode.New(errcode.ErrHubLineFull, "line shard_id=%d is full", targetShardID)
	}

	// R7 收口(P0-4):进入不可逆迁移(占新→切归属→退旧)前的会话终检。入口中间件
	// 检查后到此处的窗口内若发生顶号,旧会话请求在这里被拒,零 assignment/容量/清退
	// 副作用。此检查到 CAS 之间的残余毫秒窗由下面的 post-check + 票据绑请求方 jti
	//(transferResult)+ ACK 消费点复核(AcknowledgeAdmission)三层兜底。
	if err := u.requireCallerSessionCurrent(ctx, playerID); err != nil {
		return nil, err
	}

	// 迁移前先固定原线路:post-check 发现被顶时用它做条件回退(R9 复审 P0-6)。
	originalShardID := assignment.ShardId

	tr, err := u.TransferHub(ctx, playerID, uint64(targetShardID))
	if err != nil {
		return nil, err
	}
	// post-check:CAS 已落地后复核。此刻发现被顶,说明轮换落在上面终检与 CAS 之间的
	// 毫秒窗内。除扣票外(票本就绑旧 jti,兑换点必拒;扣留只是提前失败),还尝试把
	// 路由副作用回退到原线路(R9 复审 P0-6):否则旧会话的失败请求仍把新会话的归属
	// 搬去目标线路(卡容量/改位置)。回退是条件化 best-effort:仅当归属仍是本次迁移
	// 产物时才回退,失败只记日志(新会话下次 resolve/自行切线即可收敛,不影响安全面)。
	if err := u.requireCallerSessionCurrent(ctx, playerID); err != nil {
		u.revertLineTransfer(ctx, playerID, originalShardID, tr)
		return nil, err
	}
	lineNo := lineNoOfShard(shards, assignment.Region, assignmentTrack, targetShardID)
	return &TransferToLineResult{
		NewHubDSAddr: tr.NewHubDSAddr,
		NewHubTicket: tr.NewHubTicket,
		NewShardID:   targetShardID,
		LineNo:       lineNo,
	}, nil
}

// revertLineTransfer(R9 复审 P0-6):TransferToLine post-check 判被顶后,条件回退本次
// 迁移的路由副作用。仅当当前归属仍是本次迁移落地的 assignment(assignment_id 精确
// 匹配)时才把玩家迁回原线路;归属已被并发操作(新会话切线/清退链)推进则跳过,
// 绝不回滚别人的落地结果。回退动作本身复用 TransferHub 全套占坑/清退/重签语义;
// check 到内部 CAS 之间的残余毫秒窗只影响路由位置(票据始终绑 jti,无凭据面影响),
// 最坏情况新会话被多弹一次线路、可自行再切。失败仅告警,不改写调用方错误。
func (u *HubUsecase) revertLineTransfer(ctx context.Context, playerID uint64, originalShardID uint32, tr *TransferResult) {
	if tr == nil || tr.NewAssignmentID == "" {
		return
	}
	cur, found, err := u.repo.GetAssignment(ctx, playerID)
	if err != nil {
		plog.With(ctx).Warnw("msg", "transfer_supersede_revert_read_failed",
			"player_id", playerID, "err", err)
		return
	}
	if !found || cur.GetAssignmentId() != tr.NewAssignmentID {
		plog.With(ctx).Infow("msg", "transfer_supersede_revert_skipped",
			"player_id", playerID, "reason", "assignment already advanced by another actor")
		return
	}
	if _, rerr := u.TransferHub(ctx, playerID, uint64(originalShardID)); rerr != nil {
		plog.With(ctx).Warnw("msg", "transfer_supersede_revert_failed",
			"player_id", playerID, "original_shard_id", originalShardID, "err", rerr)
		return
	}
	plog.With(ctx).Infow("msg", "transfer_supersede_reverted",
		"player_id", playerID, "original_shard_id", originalShardID)
}

// ── RPC 4:ListHubs ────────────────────────────────────────────────────────────

// ListHubs 列出分片负载,region 非空时过滤。
func (u *HubUsecase) ListHubs(ctx context.Context, region string) ([]*hubv1.HubInfo, error) {
	shards, err := u.repo.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*hubv1.HubInfo, 0, len(shards))
	for _, s := range shards {
		if region != "" && s.Region != region {
			continue
		}
		out = append(out, &hubv1.HubInfo{
			HubPodName:  s.HubPodName,
			HubAddr:     s.HubAddr,
			Region:      s.Region,
			PlayerCount: s.PlayerCount,
			Capacity:    s.Capacity,
			State:       s.State,
		})
	}
	return out, nil
}

// ── RPC 5:Heartbeat ───────────────────────────────────────────────────────────

// HeartbeatResult 是 Heartbeat 的出参(下发给 Hub DS 的控制指令)。
type HeartbeatResult struct {
	Command        string
	GraceSeconds   int32 // command=="drain"/"stop" 时的优雅迁移倒计时(秒),其余为 0
	EvictionOrders []HubEvictionOrder
	// AcceptedTokenGen:Model B 令牌激活确认(§7)。promote/validate 通过后回显当前 active
	// 代际,DS 据此确认「本令牌已被服务端接纳为权威」;legacy/off 路径恒 0。
	AcceptedTokenGen      uint64
	AcceptedTokenJTI      string
	AcceptedInstanceUID   string
	AcceptedProtocolEpoch uint32
	AcceptedWriterEpoch   uint32
}

// HubEvictionOrder names one exact physical source connection. It is returned
// only on a heartbeat authenticated as that source GameServer instance.
type HubEvictionOrder struct {
	PlayerID            uint64
	AssignmentID        string
	AdmissionID         string
	AdmissionSeq        uint64
	SourceInstanceUID   string
	SourceProtocolEpoch uint32
	SourceWriterEpoch   uint32
	CleanupAssignmentID string
}

// Heartbeat 处理 Hub DS 上报(单向 unary,DS 每 5s 调)。刷新在线数 + 心跳时刻。
// 分片镜像不存在(孤儿 DS)→ 返回 stop 指令让其自行停机。
// 分片已被强制整合标记 draining → 下发 drain + grace_seconds,Hub DS 引导在场玩家倒计时切大厅。
// tokenGen:本次心跳携带的**已验签**DS 回调令牌代际(service 层从 Guard claims 的 ds_gen 取,
// 无已验签令牌时为 0)。代际绑定下 warming→ready 只接受与镜像代际**精确相等**的心跳(审核 P1-6/P1-8)。
func (u *HubUsecase) Heartbeat(ctx context.Context, pod string, playerCount int32, state string, tsMs int64, tokenGen uint64) (*HeartbeatResult, error) {
	res, err := u.heartbeat(ctx, pod, playerCount, nil, 0, state, tsMs, tokenGen, nil)
	if err != nil {
		return nil, err
	}
	u.applyLocalCredentialACK(pod, res)
	return res, nil
}

// localHubCredentialSource 由 mode=local 的 fleet provider 实现,给出本机 Hub DS 的完整凭据身份。
// 只有 LocalHubFleetProvider 实现它:agones / mock provider 都不实现,故下面的回显分支
// 在生产(Model B)与离线 mock 下是死代码,两条路径的应答逐字节不变。
type localHubCredentialSource interface {
	LocalCredentialACK(pod string) (LocalHubCredential, bool)
}

// applyLocalCredentialACK 在 mode=local 回显本机 Hub DS 的凭据 ACK。
//
// 为什么必须回显:UE 的 SendHubHeartbeat 无条件用 IsBoundToRequest 校验应答里的
// CredentialAck(InstanceUID/InstanceEpoch/Gen/JTI/WriterEpoch 五项须与 DS 自持凭据
// 逐字段相等),不通过就不调 NotifyAuthorizedActiveHeartbeat;而 IsAcceptingNewPlayers()
// 对 local-off-v1 **没有豁免** —— 拿不到绑定式 ACK,准入租约永不打开,玩家连上大厅
// 也会被拒。local-off-v1 只免掉了 staged→active 的 Redis 提升,没免掉这道租约门。
//
// 为什么这不是"伪造回显":ACK 的值直接取自本进程签发、并经 env 下发给该 DS 的**同一份**
// 凭据,确是"服务端仍授权本实例"的真实证据。且严格 fail-closed:
//   - pod 与本机 DS 不匹配、或凭据五元组不全 → 不回显,DS 继续拒新玩家;
//   - 已有 Accepted 身份(Model B promote 已回填)→ 不覆盖;
//   - 只有 legacy 心跳入口会走到这里,Model B 心跳有自己的 ActivateHeartbeat 线性化点。
func (u *HubUsecase) applyLocalCredentialACK(pod string, res *HeartbeatResult) {
	if res == nil || res.AcceptedInstanceUID != "" {
		return
	}
	src, ok := u.fleet.(localHubCredentialSource)
	if !ok {
		return
	}
	cred, ok := src.LocalCredentialACK(pod)
	if !ok {
		return
	}
	res.AcceptedTokenGen = cred.Gen
	res.AcceptedTokenJTI = cred.JTI
	res.AcceptedInstanceUID = cred.InstanceUID
	res.AcceptedProtocolEpoch = cred.ProtocolEpoch
	res.AcceptedWriterEpoch = cred.WriterEpoch
}

// HeartbeatWithCredential 是 Model B 心跳入口(§7):service 层验签抽出 Model B 凭据后调用。
// authRepo 已装配 → cred 携带的 (uid,epoch,gen,jti) 走 ActivateHeartbeat 单事务线性化点:
// 首个合法 pending 心跳在 authKey+shardKey 同事务内原子完成 promote(pending→active)+ 分片
// warming→ready + 投影 active 元组;stale(无授权记录/uid|epoch 不符/相位锁定/都不匹配)一律
// fail-closed 返回 ErrUnauthorized(两键零变更)。分片镜像缺失 → reconcile 拓扑后重试一次,
// 保证 promote 与 ready 恒同事务(杜绝半激活)。返回 accepted gen 供 DS 回显。
func (u *HubUsecase) HeartbeatWithCredential(ctx context.Context, pod string, playerCount int32,
	playerIDs []uint64, maxPlayers uint32, state string, tsMs int64, cred *HubCredential) (*HeartbeatResult, error) {
	return u.heartbeat(ctx, pod, playerCount, playerIDs, maxPlayers, state, tsMs, 0, cred)
}

func (u *HubUsecase) heartbeat(ctx context.Context, pod string, playerCount int32, playerIDs []uint64,
	maxPlayers uint32, state string, tsMs int64, tokenGen uint64, cred *HubCredential) (*HeartbeatResult, error) {
	if err := u.requireWriter(); err != nil {
		return nil, err
	}
	if pod == "" {
		return nil, errcode.New(errcode.ErrInvalidArg, "hub_pod_name required")
	}
	// 请求 ts_ms 不参与授权/存活权威；统一用服务端接收时间，future ts 不能延长可路由窗口。
	tsMs = time.Now().UnixMilli()
	// Model B:授权记录仓已装配 → 走 ActivateHeartbeat 单事务线性化点(authRepo=nil 时落 legacy 分支)。
	if u.authRepo != nil {
		return u.heartbeatModelB(ctx, pod, playerCount, playerIDs, maxPlayers, state, tsMs, cred)
	}
	found, err := u.repo.HeartbeatShard(ctx, pod, playerCount, state, tsMs, tokenGen, u.dsTokenGeneration, u.shardTTL())
	if err != nil {
		return nil, err
	}
	if !found {
		// 新建/重建的 Hub GameServer 可能早于周期拓扑刷新发来业务心跳。
		// 这里先主动刷新一次拓扑,避免首跳把健康 pod 误判成孤儿并下发 stop。
		if rerr := u.reconcileShardTopology(ctx); rerr != nil {
			plog.With(ctx).Warnw("msg", "heartbeat_topology_reconcile_failed", "pod", pod, "err", rerr)
		} else {
			found, err = u.repo.HeartbeatShard(ctx, pod, playerCount, state, tsMs, tokenGen, u.dsTokenGeneration, u.shardTTL())
			if err != nil {
				return nil, err
			}
		}
		if !found {
			plog.With(ctx).Warnw("msg", "heartbeat_unknown_hub_waiting_topology", "pod", pod)
			return nil, errcode.New(errcode.ErrUnavailable, "hub shard %s topology not confirmed", pod)
		}
	}
	// 分片被标记 draining/stopping → 下发迁移/停机指令(与 Kafka 推送双通道)。
	if shard, ok, gerr := u.repo.GetShard(ctx, pod); gerr == nil && ok {
		// legacy 面 owner 实例租约续写(与 Model B 心跳同语义、同时序约束,2026-08-04):
		// login 的 applyOwnerPlacement 只在「ADMITTED 且实例租约剩余 > 安全余量」时报 STABLE,
		// legacy 心跳不续租 = 租约恒过期,客户端 Admission ACK 已到手却永远等不到 STABLE,
		// 30s deadline 后反复重查(mode=local 实测的第三道墙)。实例身份回源分片镜像
		// (LocalHubFleetProvider 播种,与 ownerTargetForHubTicket 的 legacy 回源同源同值);
		// mock 镜像无实例身份 → uid 为空跳过,行为不变。Model B(authRepo != nil)不进本分支。
		if uid := shard.GetGameserverUid(); uid != "" {
			if lerr := renewOwnerLeaseGate(ctx, u.ownerLease, u.ownerLeaseRequired,
				pod, uid, 0, ""); lerr != nil {
				return nil, lerr
			}
		}
		switch shard.State {
		case stateDraining:
			return &HeartbeatResult{Command: commandDrain, GraceSeconds: u.cfg.MigrateGraceSeconds}, nil
		case stateStopping:
			return &HeartbeatResult{Command: commandStop, GraceSeconds: u.cfg.MigrateGraceSeconds}, nil
		}
	}
	return &HeartbeatResult{Command: commandNone}, nil
}

// heartbeatModelB 处理 Model B 心跳(authRepo 已装配)。
// cred==nil(legacy 令牌 / 无凭据)在 Model B 下一律拒:Redis 授权权威模式不接受未携带 Model B
// 凭据的心跳借旧令牌保活或翻 ready(纵深防御,service 层也会拦;审核二轮 CE1/CE2)。
func (u *HubUsecase) heartbeatModelB(ctx context.Context, pod string, playerCount int32, playerIDs []uint64,
	maxPlayers uint32, state string, tsMs int64, cred *HubCredential) (*HeartbeatResult, error) {
	if cred == nil {
		return nil, errcode.New(errcode.ErrUnauthorized, "hub heartbeat requires model B credential under redis authority")
	}
	in := data.ActivateHeartbeatInput{
		PlayerCount: playerCount,
		PlayerIDs:   append([]uint64(nil), playerIDs...),
		MaxPlayers:  maxPlayers,
		State:       state,
		TsMs:        tsMs,
		AuthTTL:     u.authTTLDur(),
		ShardTTL:    u.shardTTL(),
	}
	id := data.CredentialIdentity{
		Gen:           cred.Gen,
		JTI:           cred.JTI,
		InstanceUID:   cred.InstanceUID,
		ProtocolEpoch: cred.ProtocolEpoch,
		TokenSHA256:   cred.TokenSHA256,
		Kid:           cred.Kid,
		WriterEpoch:   cred.WriterEpoch,
	}
	res, err := u.authRepo.ActivateHeartbeat(ctx, pod, id, in)
	if err != nil {
		return nil, err // ErrUnauthorized:授权未激活/不匹配/相位锁定,fail-closed
	}
	if !res.ShardFound {
		// 分片镜像缺失(孤儿 / 早于拓扑种子):先刷一次拓扑再重试,保证 promote 与 ready 同事务。
		if rerr := u.reconcileShardTopology(ctx); rerr != nil {
			plog.With(ctx).Warnw("msg", "heartbeat_topology_reconcile_failed", "pod", pod, "err", rerr)
			return nil, errcode.New(errcode.ErrUnavailable, "hub shard %s topology reconcile: %v", pod, rerr)
		}
		res, err = u.authRepo.ActivateHeartbeat(ctx, pod, id, in)
		if err != nil {
			return nil, err
		}
		if !res.ShardFound {
			plog.With(ctx).Warnw("msg", "heartbeat_unknown_hub_waiting_topology", "pod", pod)
			return nil, errcode.New(errcode.ErrUnavailable, "hub shard %s topology not confirmed", pod)
		}
	}
	// owner 权威实例租约双写(owner-authority.md migrate ⑥):必须在心跳响应返回前完成,
	// 失败语义(弱/强依赖)见 renewOwnerLeaseGate。hub 凭据无实例纪元 → epoch 传 0。
	if lerr := renewOwnerLeaseGate(ctx, u.ownerLease, u.ownerLeaseRequired,
		pod, res.InstanceUID, 0, ""); lerr != nil {
		return nil, lerr
	}
	// owner 迁移准入代提交(owner-authority.md migrate ③,近似:授权 census 即准入证据;
	// contract 阶段移交 DS Admission 链)。弱依赖,失败/屏障未开都不影响心跳。
	// 复审 P1-4:无条件调用(不再用 len(playerIDs)>0 守卫)——空 census(最后一名玩家离场)
	// 也必须进函数按本实例剪枝 admitted 缓存,否则 stale 项残留、玩家回流同实例被误吞跳过 Admit。
	ownerAdmitCensusWeak(ctx, u.ownerAuth, &u.ownerAdmitted, playerIDs,
		ownerTypeHub, pod, res.InstanceUID, 2*time.Second, u.resolveOwnerTargetFromAssignment)
	command, graceSeconds := commandNone, int32(0)
	switch res.ShardState {
	case stateDraining:
		command, graceSeconds = commandDrain, u.cfg.MigrateGraceSeconds
	case stateStopping:
		command, graceSeconds = commandStop, u.cfg.MigrateGraceSeconds
	}
	out := modelBHeartbeatResult(res, command, graceSeconds)
	orders, orderErr := u.pendingHubEvictionOrders(ctx, pod, res.InstanceUID,
		res.ProtocolEpoch, res.WriterEpoch)
	if orderErr != nil {
		// Heartbeat/auth was already committed. Keep the DS authorization lease
		// healthy and retry order discovery next tick; never downgrade a healthy
		// heartbeat into an apparent credential failure after that commit.
		plog.With(ctx).Warnw("msg", "hub_eviction_order_discovery_failed", "pod", pod, "err", orderErr)
	} else {
		out.EvictionOrders = orders
	}
	return out, nil
}

func modelBHeartbeatResult(res data.ActivateResult, command string, graceSeconds int32) *HeartbeatResult {
	return &HeartbeatResult{
		Command: command, GraceSeconds: graceSeconds,
		AcceptedTokenGen: res.ActiveGen, AcceptedTokenJTI: res.ActiveJTI,
		AcceptedInstanceUID: res.InstanceUID, AcceptedProtocolEpoch: res.ProtocolEpoch,
		AcceptedWriterEpoch: res.WriterEpoch,
	}
}

const maxHubEvictionOrdersPerHeartbeat = 256

// pendingHubEvictionOrders projects the durable cleanup index into the source
// DS control plane. Connected ledger state is read-only here: the order is not
// complete until the DS kicks the exact local admission and AcknowledgeDeparture
// removes it (or confirmed GameServer teardown removes the whole UID ledger).
func (u *HubUsecase) pendingHubEvictionOrders(ctx context.Context, sourcePod, instanceUID string,
	protocolEpoch, writerEpoch uint32) ([]HubEvictionOrder, error) {
	refs, err := u.repo.ListTransferCleanups(ctx, sourcePod)
	if err != nil {
		return nil, err
	}
	orders := make([]HubEvictionOrder, 0, len(refs))
	for _, ref := range refs {
		if len(orders) >= maxHubEvictionOrdersPerHeartbeat {
			break
		}
		assignment, found, readErr := u.repo.GetAssignment(ctx, ref.PlayerID)
		if readErr != nil {
			return nil, readErr
		}
		if !found || assignment.GetAssignmentId() != ref.TargetAssignmentID {
			continue // orphan cleanup is removed by the reconciler
		}
		var source *hubv1.HubAssignmentStorageRecord
		switch {
		case assignment.GetTransferCleanupPending():
			if !assignment.GetTransferTargetBound() {
				continue // never evict source before the exact target Bind is durable
			}
			source, readErr = transferCleanupSource(assignment)
		case assignment.GetReleaseCleanupPending():
			source = assignment
		default:
			continue
		}
		if readErr != nil {
			return nil, readErr
		}
		if source.GetHubPodName() != sourcePod || source.GetHubInstanceUid() != instanceUID ||
			source.GetAuthEpoch() != protocolEpoch || source.GetAuthWriterEpoch() != writerEpoch {
			// A replacement DS must never receive an order for the dead old UID.
			// Authoritative GameServer teardown owns that ledger cleanup proof.
			continue
		}
		seat, inspectErr := u.authRepo.InspectAssignmentSeat(ctx, sourcePod,
			assignmentInstanceIdentity(source))
		if inspectErr != nil {
			return nil, inspectErr
		}
		if seat.Conflict {
			// 源席位账本上出现与预期不符的 owner 身份 = 潜在双 owner / 脑裂(§9.22 核心不变量)。
			// 上层只把它泛化成 hub_eviction_order_discovery_failed,丢了"是身份冲突"这一关键区分 →
			// 在此打 ERROR 带精确 fencing 身份,便于直接定位脑裂。
			plog.With(ctx).Errorw("msg", "hub_eviction_owner_conflict",
				"source_pod", sourcePod, "instance_uid", instanceUID,
				"protocol_epoch", protocolEpoch, "writer_epoch", writerEpoch)
			return nil, errcode.New(errcode.ErrInvalidState,
				"Hub eviction source owner identity conflict")
		}
		if !seat.Connected {
			continue
		}
		orders = append(orders, HubEvictionOrder{
			PlayerID: ref.PlayerID, AssignmentID: source.GetAssignmentId(),
			AdmissionID: seat.AdmissionID, AdmissionSeq: seat.AdmissionSeq,
			SourceInstanceUID: source.GetHubInstanceUid(), SourceProtocolEpoch: source.GetAuthEpoch(),
			SourceWriterEpoch: source.GetAuthWriterEpoch(), CleanupAssignmentID: ref.TargetAssignmentID,
		})
	}
	return orders, nil
}

// AcknowledgeAdmissionResult / AcknowledgeDepartureResult 只暴露 DS 状态机需要的结果。
type AcknowledgeAdmissionResult struct {
	Admitted bool
}
type AcknowledgeDepartureResult struct {
	Departed bool
	Conflict bool
}

// AcknowledgeAdmission 把本地已验签 Hub DSTicket 对应 reservation 原子转为 connected owner。
// ticketSessionJTI 为票据 sjti claim(R7 复审 P0-3):v2 Hub 本地验票不经 Login 在线兑换,
// ACK 是唯一在线权威接触点,装配 sessGate 后在消费 reservation 之前复核会话现行性。
func (u *HubUsecase) AcknowledgeAdmission(ctx context.Context, playerID uint64, assignmentID, pod,
	admissionID string, admissionSeq uint64, ticketSessionJTI string,
	cred *HubCredential) (*AcknowledgeAdmissionResult, error) {
	if err := u.requireWriter(); err != nil {
		return nil, err
	}
	if u.authRepo == nil || cred == nil {
		return nil, errcode.New(errcode.ErrUnauthorized, "hub admission requires model B authority")
	}
	// R7 复审 P0-3:会话现行性前置复核,失败时不消费 reservation、不产生任何副作用。
	// 空 sjti 由 require_ticket_sjti 门控制(R7 收口,P0-5 滚动兼容):默认兼容档告警
	// 放行(旧 Hub DS 不转发 sjti/旧签发面残票,行为与旧版一致);全 fleet DS 排空 +
	// 票据最大 TTL 过后由运维置 true 硬拒。非空 sjti 无论档位都全量复核,不可达 fail-closed。
	if u.sessGate != nil {
		if ticketSessionJTI == "" {
			if u.sessGateRequireSJTI {
				return nil, errcode.New(errcode.ErrUnauthorized,
					"hub admission ticket lacks session binding (sjti); reissue required")
			}
			plog.With(ctx).Warnw("msg", "hub_admission_missing_sjti_tolerated",
				"player_id", playerID, "assignment_id", assignmentID, "pod", pod,
				"hint", "混版兼容窗;旧 DS 排空后开 session_gate.require_ticket_sjti 收口")
		} else {
			curJTI, curFound, gerr := u.sessGate.CurrentJTI(ctx, playerID)
			if gerr != nil {
				return nil, errcode.NewCause(errcode.ErrUnavailable, gerr,
					"session authority unavailable during hub admission")
			}
			if !curFound {
				return nil, errcode.New(errcode.ErrUnauthorized,
					"player %d has no current session; hub admission rejected", playerID)
			}
			if curJTI != ticketSessionJTI {
				plog.With(ctx).Warnw("msg", "hub_admission_session_superseded",
					"player_id", playerID, "assignment_id", assignmentID, "pod", pod)
				return nil, errcode.New(errcode.ErrSessionSuperseded,
					"hub admission ticket was issued for a superseded session")
			}
		}
	}
	assignment, found, err := u.repo.GetAssignment(ctx, playerID)
	if err != nil {
		return nil, err
	}
	if found && (assignment.GetTransferCleanupPending() || assignment.GetReleaseCleanupPending()) {
		var stillFound bool
		assignment, stillFound, err = u.resumeAssignmentCleanup(ctx, playerID, assignment.GetAssignmentId())
		if err != nil {
			// This is before target AcknowledgeAdmission: Redis/source cleanup
			// failure is retryable and creates no new target session/spawn.
			return nil, err
		}
		if !stillFound {
			return nil, errcode.New(errcode.ErrInvalidState,
				"Hub admission assignment was released during owner cleanup")
		}
	}
	if !found || !assignmentMatchesAdmission(assignment, playerID, assignmentID, pod, cred) {
		return nil, errcode.New(errcode.ErrInvalidState, "hub admission assignment is no longer current")
	}
	id := hubCredentialIdentity(cred)
	reservation := data.ReservationIdentity{
		PlayerID: playerID, AssignmentID: assignmentID, InstanceUID: cred.InstanceUID,
		ProtocolEpoch: cred.ProtocolEpoch, WriterEpoch: cred.WriterEpoch,
	}
	result, err := u.authRepo.AcknowledgeAdmission(ctx, pod, id, reservation,
		admissionID, admissionSeq, time.Now().UnixMilli(), u.shardTTL())
	if err != nil {
		return nil, err
	}
	if !result.Admitted {
		return &AcknowledgeAdmissionResult{Admitted: false}, nil
	}
	// R7 收口(P0-2):durable ledger 写是入场线性化点,写成功后必须再复核一次会话现行性,
	// 关闭「预检通过 → 消费 reservation 之间轮换」的 TOCTOU。
	//
	// R9 复审 P1(结果分型,回退只用于确定性否定):
	//   - 权威不可达(gerr)= 结果未知:**不回退** connected owner,返回 Unavailable。
	//     ledger 的 ACK 对相同 (admission_id, seq) 幂等(AlreadyAdmitted),DS 用同一
	//     identity 重试会完整重跑两次会话复核拿到确定结果;若回退,普通 reservation
	//     已被消费且 Departure 不会恢复它,重试必然 fail-closed 死路,玩家被迫整链重
	//     resolve。owner 保留期间客户端仍未过 spawn gate,无授权面影响。
	//   - 确定性否定(会话消失/已被顶):exact 回退刚建立的 connected owner
	//     (AcknowledgeDeparture 同 identity,幂等)。回退后同票重试本就该失败
	//     (持票会话已死),新会话走完整 resolve;这不是可重试场景。
	//   - 回退本身失败也拒绝:seat 残留由 DS Kick 后的物理 Logout proof(Departure
	//     幂等重试)收敛,绝不向已判定非现行的会话开门。
	// 本复核之后的轮换 = 正常「已在 Hub 中被顶号」,由 successor 替换(新 admission
	// 更大 seq 接管)+ push 顶号清退链处理。空 sjti(兼容窗放行)无绑定可比,跳过。
	if u.sessGate != nil && ticketSessionJTI != "" {
		curJTI, curFound, gerr := u.sessGate.CurrentJTI(ctx, playerID)
		if gerr != nil {
			plog.With(ctx).Warnw("msg", "hub_admission_postcheck_indeterminate",
				"player_id", playerID, "assignment_id", assignmentID, "pod", pod, "err", gerr,
				"hint", "owner 保留,DS 以同 identity 重试 ACK 重跑复核;spawn gate 未开")
			return nil, errcode.NewCause(errcode.ErrUnavailable, gerr,
				"session authority unavailable during hub admission post-check")
		}
		if !curFound || curJTI != ticketSessionJTI {
			if _, derr := u.authRepo.AcknowledgeDeparture(ctx, pod, id, reservation,
				admissionID, admissionSeq, time.Now().UnixMilli(), u.shardTTL()); derr != nil {
				plog.With(ctx).Errorw("msg", "hub_admission_postcheck_revert_failed",
					"player_id", playerID, "assignment_id", assignmentID, "pod", pod, "err", derr,
					"hint", "connected owner 残留,等待 DS Kick 后物理 Logout proof 收敛")
			}
			if !curFound {
				return nil, errcode.New(errcode.ErrUnauthorized,
					"player %d session vanished during hub admission; spawn refused", playerID)
			}
			plog.With(ctx).Warnw("msg", "hub_admission_postcheck_superseded",
				"player_id", playerID, "assignment_id", assignmentID, "pod", pod)
			return nil, errcode.New(errcode.ErrSessionSuperseded,
				"hub admission superseded by a newer login before spawn gate opened")
		}
	}
	// assignment 与 {pod} ledger 不同 slot：ACK 后必须再查一次。若 Transfer/Release
	// 已赢得 CAS，保留 exact connected owner 并拒绝开放 spawn gate。DS 收到拒绝后必须
	// Kick，等 Logout 的物理 proof 才能删除；服务端不能在 PC/Pawn 尚存时自证 Departure。
	current, stillFound, postErr := u.repo.GetAssignment(ctx, playerID)
	if postErr != nil || !stillFound || !assignmentMatchesAdmission(current, playerID, assignmentID, pod, cred) {
		if postErr != nil {
			return nil, postErr
		}
		return nil, errcode.New(errcode.ErrInvalidState, "hub admission assignment changed during acknowledge")
	}
	// owner 权威准入(§9.23 服务端完成点)。这里是 Admission 链的原生提交点,取代原先由
	// 心跳 census 代提交的近似——census 只能证明"该实例正在服务该玩家",而本处是玩家
	// **本次**进场的线性化点,exact identity 就在手上。
	//
	// 强依赖:Admit 不成功就不开 spawn gate。§9.22 明文「新 DS 在屏障打开前不得创建可操作
	// Pawn / 玩家态、向客户端确认 PLAYABLE」,而 Admitted=true 正是 DS 开 spawn gate 的信号。
	// 屏障未开是预期内的暂时态(旧 DS 仍在可玩窗口),返回可重试错误让 DS 用同 identity 重放
	// ACK —— Admit 幂等,重放不会产生第二个 owner。
	if err := u.admitOwnerForAdmission(ctx, playerID, data.OwnerTargetView{
		PodName:                  pod,
		InstanceUID:              cred.InstanceUID,
		InstanceEpoch:            cred.ProtocolEpoch,
		AssignmentOrAllocationID: assignmentID,
		ReleaseTrack:             current.GetReleaseTrack(),
	}); err != nil {
		return nil, err
	}
	return &AcknowledgeAdmissionResult{Admitted: result.Admitted}, nil
}

// admitOwnerForAdmission 把 owner 记录从 PENDING 推进到 ADMITTED(§9.23 完成点)。
//
// 先 Query 再 Admit 不是"先查再存"的 TOCTOU:Admit 自身在 owner 的行锁事务内按 exact
// (player_id, owner_epoch, operation_id, 实例四元组)做 CAS,查到的值只是拿来当 CAS 期望;
// 期间记录若被推进,Admit 会以 IDENTITY_MISMATCH / EPOCH_CONFLICT 拒绝,不会误准入。
//
// ownerAuth == nil = owner 未部署(部署形态问题),不在本函数收敛。
func (u *HubUsecase) admitOwnerForAdmission(ctx context.Context, playerID uint64,
	target data.OwnerTargetView) error {
	if u.ownerAuth == nil {
		return nil
	}
	rec, err := u.ownerAuth.QueryOwner(ctx, playerID)
	if err != nil {
		// 结果不可判定 → fail-closed,绝不冒充"已准入"开门(§9.22)。
		return errcode.NewCause(errcode.ErrUnavailable, err,
			"owner authority unavailable during hub admission player=%d", playerID)
	}
	if rec.OwnerType != ownerTypeHub || rec.PodName != target.PodName ||
		rec.InstanceUID != target.InstanceUID || rec.InstanceEpoch != target.InstanceEpoch {
		// 归属已经不指向本实例(Transfer / 顶号 / 灾备接管都会走到这里)。
		// 不 Admit、不开门:玩家的 owner 在别处,这台 DS 无权创建可操作玩家态。
		plog.With(ctx).Warnw("msg", "hub_admission_owner_points_elsewhere",
			"player_id", playerID, "want_pod", target.PodName, "owner_pod", rec.PodName,
			"owner_uid", rec.InstanceUID, "owner_epoch", rec.OwnerEpoch)
		return errcode.New(errcode.ErrInvalidState,
			"owner no longer points at this hub instance; admission refused player=%d", playerID)
	}
	if rec.Phase == ownerPhaseAdmitted {
		return nil // 幂等:ACK 重放 / 回包丢失后重试,原样返回已准入,不产生第二个 owner。
	}
	retryAfterMs, aerr := u.ownerAuth.Admit(ctx, playerID, rec.OwnerEpoch, rec.OperationID, target)
	if aerr == nil {
		return nil
	}
	if retryAfterMs > 0 {
		// admit_not_before 屏障未开:旧 DS 最晚安全截止时间之前放行就是双 DS。
		// 可重试错误 + 剩余毫秒,DS 退避后用同 identity 重放 ACK(§9.23 WAIT 语义)。
		plog.With(ctx).Debugw("msg", "hub_admission_barrier_not_open",
			"player_id", playerID, "retry_after_ms", retryAfterMs, "owner_epoch", rec.OwnerEpoch)
		return errcode.NewCause(errcode.ErrUnavailable, aerr,
			"owner admit barrier not open; retry after %dms", retryAfterMs)
	}
	return errcode.NewCause(errcode.ErrUnavailable, aerr,
		"owner admit failed during hub admission player=%d", playerID)
}

func assignmentMatchesAdmission(a *hubv1.HubAssignmentStorageRecord, playerID uint64,
	assignmentID, pod string, cred *HubCredential) bool {
	return a != nil && cred != nil && playerID != 0 && a.GetPlayerId() == playerID &&
		a.GetAssignmentId() == assignmentID && a.GetHubPodName() == pod &&
		a.GetHubInstanceUid() == cred.InstanceUID && a.GetAuthEpoch() == cred.ProtocolEpoch &&
		a.GetAuthWriterEpoch() == cred.WriterEpoch
}

// AcknowledgeLocalAdmission 是 mode=local(legacy 权威面)下的准入完成点。
//
// **为什么必须单开一条路径**:上面的 AcknowledgeAdmission 消费的是 Redis 授权面
// (authRepo)的 reservation,而 local-off-v1 压根没有 authRepo —— 走那条路第一行就撞
// "hub admission requires model B authority" 返回 ErrUnauthorized。DS 侧
// FPandoraHubAdmissionRetryPolicy::ShouldRetry 只把传输失败/负码/瞬态码判为可重试,
// UNAUTHORIZED/INVALID_ARG 都是"明确拒绝" → FailAdmission → GameSession->KickPlayer。
// 于是玩家过了 PostLogin 的可信 claims 门,却在紧接着的 Admission ACK 上被踢,
// 表现为"连上大厅立刻掉线"(2026-08-04 mode=local 实测的第二道墙)。
//
// **本路径不发明新的授权语义**,只做 Model B 也做的两件事:
//  1. 用归属记录复核 (player, assignment, pod) 三元组仍是当前分配;
//  2. 调用与 Model B **完全相同**的 admitOwnerForAdmission,把 owner 记录从 PENDING
//     推进到 ADMITTED(§9.23 完成点)。目标由 ownerTargetForHubTicket 推导,与签票点
//     ownerBeginPlayer 的 exact 四元组逐字段同源,Admit 的行锁 CAS 因此自洽;
//     归属若已指向别处(顶号/传送),那里会 fail-closed 拒绝,不会误开门。
//
// **被刻意省略的是 reservation 消费与 admission_id/seq 排序**:它们是 Redis 授权面的
// 对象,用来在多实例下仲裁"谁是当前 owner"。local 下 LocalHubFleetProvider 只拉起一台
// Hub DS,不存在第二个竞争者,没有可仲裁的对象。真要多实例就该上 Model B,而不是给
// legacy 面打补丁伪造一套 reservation。
//
// 调用方 service 层已按 mode=local 硬门控;这里再自查一次 authRepo,双保险。
func (u *HubUsecase) AcknowledgeLocalAdmission(ctx context.Context, playerID uint64,
	assignmentID, pod string) (*AcknowledgeAdmissionResult, error) {
	if err := u.requireWriter(); err != nil {
		return nil, err
	}
	if u.authRepo != nil {
		// 装了 Redis 授权面还走 legacy 准入 = 绕过 reservation 仲裁,fail-closed。
		return nil, errcode.New(errcode.ErrUnauthorized,
			"local hub admission is rejected while model B authority is configured")
	}
	if playerID == 0 || assignmentID == "" || pod == "" {
		return nil, errcode.New(errcode.ErrInvalidArg, "incomplete local hub admission identity")
	}
	assignment, found, err := u.repo.GetAssignment(ctx, playerID)
	if err != nil {
		return nil, err
	}
	if !found || assignment.GetPlayerId() != playerID ||
		assignment.GetAssignmentId() != assignmentID || assignment.GetHubPodName() != pod {
		// 与 Model B 同语义:票据签发后玩家已被重新分配/释放,这张票不再代表当前归属。
		return nil, errcode.New(errcode.ErrInvalidState,
			"hub admission assignment is no longer current player=%d", playerID)
	}
	target, ok := u.ownerTargetForHubTicket(ctx, assignment)
	if !ok {
		// 拼不出 exact 身份就不能 Admit(§9.22)。签票点同样拼不出时压根不会 Begin,
		// 此刻 owner 面无记录,开门等于无权威背书 —— 宁可拒一次让客户端重试。
		return nil, errcode.New(errcode.ErrInvalidState,
			"hub admission lacks exact owner identity player=%d", playerID)
	}
	if err := u.admitOwnerForAdmission(ctx, playerID, target); err != nil {
		return nil, err
	}
	return &AcknowledgeAdmissionResult{Admitted: true}, nil
}

// AcknowledgeLocalDeparture 是 mode=local(legacy 权威面)下的离场确认点。
//
// **为什么必须单开一条路径**:与 AcknowledgeLocalAdmission 完全同源 —— local-off-v1 没有
// authRepo,走下面那条 AcknowledgeDeparture 第一道门就撞 "hub departure requires model B
// authority"。而 DS 侧 UPandoraDSBackendSubsystem::ShouldCompleteQueuedHubDeparture 只认
// 传输 + gRPC + 业务码三层全 OK(Result.IsFullySucceeded()),拿不到 code=0 就按
// HubDepartureRetryIntervalSeconds=1s 周期无限重试:DS 日志被每秒一条
// "DS call failed: .../AcknowledgeDeparture code=4" 灌爆,PendingHubDepartures 队列项
// 也永不出队(2026-08-06 mode=local 实测;08-04 只补了 Admission 半边,离场被落下)。
//
// **本路径没有可删对象**:Model B 的 AcknowledgeDeparture 删的是 Redis 授权面(authRepo)
// 里的 connected ownership 记录,并不碰 §9.22 owner 权威;local 下 authRepo=nil,那条记录
// 压根不存在。玩家在线态由 player_locator 的 30s TTL 自然收敛,大厅归属由 Release/Transfer
// 显式替换。因此这里只做"确认收到、让 DS 停止重试",返回 Departed=true 表达幂等语义:
// 该 exact identity 的 connected owner 此刻确实不存在。
//
// **刻意不复核归属三元组**(与 AcknowledgeLocalAdmission 相反):准入是开门,归属过期必须
// fail-closed 拒(否则一人两 DS);离场是关门,没有可授权的对象。且玩家离场最常见的原因
// 就是归属已被改写(进 Battle / 顶号 / 切线),此时复核必然失败 → 返回非 OK → DS 又回到
// 每秒重试,正好把本函数要修的 bug 原样重建。
//
// 调用方 service 层已按 mode=local 硬门控;这里再自查一次 authRepo,双保险。
func (u *HubUsecase) AcknowledgeLocalDeparture(ctx context.Context, playerID uint64,
	assignmentID, pod string) (*AcknowledgeDepartureResult, error) {
	if err := u.requireWriter(); err != nil {
		return nil, err
	}
	if u.authRepo != nil {
		// 装了 Redis 授权面还走 legacy 离场 = 绕过 exact admission 仲裁,fail-closed。
		return nil, errcode.New(errcode.ErrUnauthorized,
			"local hub departure is rejected while model B authority is configured")
	}
	if playerID == 0 || assignmentID == "" || pod == "" {
		return nil, errcode.New(errcode.ErrInvalidArg, "incomplete local hub departure identity")
	}
	return &AcknowledgeDepartureResult{Departed: true}, nil
}

// AcknowledgeDeparture exact 删除当前 admission owner；Conflict 由旧连接晚到 Logout 触发。
func (u *HubUsecase) AcknowledgeDeparture(ctx context.Context, playerID uint64, assignmentID, pod,
	admissionID string, admissionSeq uint64, cred *HubCredential) (*AcknowledgeDepartureResult, error) {
	if err := u.requireWriter(); err != nil {
		return nil, err
	}
	if u.authRepo == nil || cred == nil {
		return nil, errcode.New(errcode.ErrUnauthorized, "hub departure requires model B authority")
	}
	id := hubCredentialIdentity(cred)
	result, err := u.authRepo.AcknowledgeDeparture(ctx, pod, id, data.ReservationIdentity{
		PlayerID: playerID, AssignmentID: assignmentID, InstanceUID: cred.InstanceUID,
		ProtocolEpoch: cred.ProtocolEpoch, WriterEpoch: cred.WriterEpoch,
	}, admissionID, admissionSeq, time.Now().UnixMilli(), u.shardTTL())
	if err != nil {
		return nil, err
	}
	// Departure removes physical connected ownership only. The durable
	// assignment/member index remains until an exact Release/Transfer phase
	// replaces or deletes it, so an offline player is still discoverable when
	// this shard drains.
	return &AcknowledgeDepartureResult{Departed: result.Departed, Conflict: result.Conflict}, nil
}

func hubCredentialIdentity(cred *HubCredential) data.CredentialIdentity {
	return data.CredentialIdentity{
		Gen: cred.Gen, JTI: cred.JTI, InstanceUID: cred.InstanceUID, ProtocolEpoch: cred.ProtocolEpoch,
		TokenSHA256: cred.TokenSHA256, Kid: cred.Kid, WriterEpoch: cred.WriterEpoch,
	}
}

// RefreshHubPresence 把 Hub DS 心跳捎带的在场 player_ids 转发给 player_locator
// 批量续期 HUB 位置 TTL(在线保活链路:DS 每 5s 上报,locator TTL 30s,
// 玩家掉线 → DS 停报该 id → 30s 自然过期 = 好友视角离线)。
//
// fire-and-forget(同 ds_allocator.refreshBattleLocations):goroutine + plog.Detach
// (只复制 trace_id 等日志字段,满足不变量 §8,剥离心跳 RPC 的取消与 server transport——
// 下游是挂 Trace middleware 的 gRPC client,不得继承入站请求 transport)+ 独立短超时,
// locator 抖动/卡死既不拖慢心跳响应尾延迟,也不泄漏 goroutine。
// best-effort 弱依赖:locator 未配(nil)/ 转发失败只记 Warn,绝不影响心跳主流程
// (心跳是分片存活信号,不能因旁路观测链路抖动而失败)。
func (u *HubUsecase) RefreshHubPresence(ctx context.Context, pod string, playerIDs []uint64, bearerToken string) {
	if u.locator == nil || pod == "" || len(playerIDs) == 0 {
		return
	}
	players := append([]uint64(nil), playerIDs...) // 拷贝,脱离调用方切片复用
	token := bearerToken                           // 仅闭包内存中短暂转发；禁止日志/持久化
	go func() {
		rctx, cancel := context.WithTimeout(plog.Detach(ctx), presenceRefreshTimeout)
		defer cancel()
		if _, err := u.locator.RefreshHubLocations(rctx, pod, players, token); err != nil {
			plog.With(rctx).Warnw("msg", "hub_presence_refresh_failed",
				"pod", pod, "players", len(players), "err", err)
		}
	}()
}

// ── 后台心跳超时扫描 ──────────────────────────────────────────────────────────

// RunHeartbeatSweep 启动后台心跳超时扫描,直到 ctx 取消(不变量 §4)。
func (u *HubUsecase) RunHeartbeatSweep(ctx context.Context) {
	ticker := time.NewTicker(u.cfg.SweepInterval.Std())
	defer ticker.Stop()
	plog.With(ctx).Infow("msg", "hub_heartbeat_sweep_started",
		"interval", u.cfg.SweepInterval.String(), "timeout", u.cfg.HeartbeatTimeout.String())
	wasWriter := true
	var sweptToken uint64 // 本届已完成 fence 水位推扫的 token(writer_fence.go 覆盖边界 ③)
	for {
		select {
		case <-ctx.Done():
			plog.With(ctx).Infow("msg", "hub_heartbeat_sweep_stopped")
			return
		case <-ticker.C:
			u.heartbeatSweepTick(ctx, &wasWriter, &sweptToken)
		}
	}
}

// heartbeatSweepTick 单个清扫 tick(独立函数使 recover 作用域恰为一轮)。
//
// panic 兜底(压测审核【必修-6】):此前 tick 内任一 latent panic 崩整进程 = 心跳超时
// 补偿(§9 不变量 4)、fence 水位推扫、owner 清理 saga 全部停摆。recover 后跳过本轮,
// 下 tick 继续;各步骤均幂等可重入。并发 map 写是 runtime fatal,recover 兜不住。
func (u *HubUsecase) heartbeatSweepTick(ctx context.Context, wasWriter *bool, sweptToken *uint64) {
	defer safego.Recover(ctx, "hub_heartbeat_sweep")
	// 复审 P1-5:census 准入缓存按 last-touch TTL 清死实例项。本地内存卫生,不经存储、
	// 不依赖 writer 身份,故置于 writer 门控之前每 tick 无条件执行(否则热备副本自身
	// 处理过的心跳留下的缓存项永不回收)。活实例项每心跳续期,仅已销毁实例项会被清。
	sweepStaleOwnerAdmitted(&u.ownerAdmitted, time.Now().Add(-ownerAdmittedStaleTTL))
	// R9 P0-7:非写者副本跳 tick,避免 RollingUpdate 重叠窗口内双写者并发
	// reconcile/sweep(存储级 fence 是最终防线,这里是快路径 + 降噪)。
	if u.writerFence != nil {
		token, held := u.writerFence.Current()
		if !held {
			if *wasWriter {
				plog.With(ctx).Warnw("msg", "hub_heartbeat_sweep_paused_not_writer")
				*wasWriter = false
			}
			return
		}
		if !*wasWriter {
			plog.With(ctx).Infow("msg", "hub_heartbeat_sweep_resumed_writer")
			*wasWriter = true
		}
		// 继任者水位推扫的**再断言**:R10 P0-4 起推扫已前移为接流前硬门
		// (writerlease.Config.OnElected,当选后宣告持有前必须跑成功),故正常情况下
		// 这里恒是 cur==mine 的零写入 no-op。保留它是为 fence 已注入但未走激活钩子
		// 的装配(dev/测试/warmup 误配)兜底;失败下个 tick 重试,不阻塞扫描。
		if token != *sweptToken {
			if err := u.repo.AdvanceWriterFences(ctx); err != nil {
				plog.With(ctx).Warnw("msg", "hub_writer_fence_sweep_failed", "token", token, "err", err)
			} else {
				*sweptToken = token
				plog.With(ctx).Infow("msg", "hub_writer_fence_swept", "token", token)
			}
		}
	}
	if err := u.reconcileOwnerCleanups(ctx); err != nil {
		plog.With(ctx).Warnw("msg", "hub_owner_cleanup_reconcile_failed", "err", err)
	}
	if err := u.reconcileShardTopology(ctx); err != nil {
		plog.With(ctx).Warnw("msg", "hub_reconcile_topology_failed", "err", err)
	}
	if err := u.sweepOnce(ctx); err != nil {
		plog.With(ctx).Warnw("msg", "hub_heartbeat_sweep_failed", "err", err)
	}
	if err := u.reconcileFleetReplicas(ctx); err != nil {
		plog.With(ctx).Warnw("msg", "hub_reconcile_replicas_failed", "err", err)
	}
}

// reconcileOwnerCleanups is restart recovery for index-first transfer/release
// sagas. The global pod index is a persistent superset; stale refs are removed
// only by exact (player,target-assignment) identity and can never delete a
// concurrent winner's ref.
func (u *HubUsecase) reconcileOwnerCleanups(ctx context.Context) error {
	pods, err := u.repo.ListTransferCleanupPods(ctx)
	if err != nil {
		return err
	}
	var firstErr error
	for _, sourcePod := range pods {
		refs, listErr := u.repo.ListTransferCleanups(ctx, sourcePod)
		if listErr != nil {
			if firstErr == nil {
				firstErr = listErr
			}
			continue
		}
		for _, ref := range refs {
			assignment, found, readErr := u.repo.GetAssignment(ctx, ref.PlayerID)
			if readErr != nil {
				if firstErr == nil {
					firstErr = readErr
				}
				continue
			}
			expectedSource := ""
			if found && assignment.GetAssignmentId() == ref.TargetAssignmentID {
				switch {
				case assignment.GetTransferCleanupPending():
					expectedSource = assignment.GetTransferSourceHubPodName()
				case assignment.GetReleaseCleanupPending():
					expectedSource = assignment.GetHubPodName()
				}
			}
			if expectedSource == "" || expectedSource != sourcePod {
				u.removeTransferCleanupRef(ctx, sourcePod, ref)
				continue
			}
			if _, _, cleanupErr := u.resumeAssignmentCleanup(ctx, ref.PlayerID, ref.TargetAssignmentID); cleanupErr != nil {
				plog.With(ctx).Warnw("msg", "hub_owner_cleanup_retry_failed", "source_pod", sourcePod,
					"player_id", ref.PlayerID, "assignment_id", ref.TargetAssignmentID, "err", cleanupErr)
				if firstErr == nil {
					firstErr = cleanupErr
				}
			}
		}
	}
	return firstErr
}

// sweepOnce 扫描一次:last_heartbeat_ms 早于阈值的分片 → 标记 draining + 移出 active(停止分配)。
// 注意:从未心跳的 Mock 种子分片(score=0)被 RangeStaleShards 排除,不会被误标 draining。
func (u *HubUsecase) sweepOnce(ctx context.Context) error {
	threshold := time.Now().Add(-u.cfg.HeartbeatTimeout.Std()).UnixMilli()
	stale, err := u.repo.RangeStaleShards(ctx, threshold)
	if err != nil {
		return err
	}
	for _, pod := range stale {
		lerr := u.repo.UpdateShardWithLock(ctx, pod, u.retry(), func(s *hubv1.HubShardStorageRecord) error {
			if s.State == stateReady {
				s.State = stateDraining // 心跳超时:停止向其分配新玩家
			}
			return nil
		}, u.shardTTL())
		if lerr != nil && errcode.As(lerr) != errcode.ErrHubNoAvailable {
			plog.With(ctx).Warnw("msg", "sweep_mark_draining_failed", "pod", pod, "err", lerr)
		}
		if rerr := u.repo.RemoveActive(ctx, pod); rerr != nil {
			plog.With(ctx).Warnw("msg", "sweep_remove_active_failed", "pod", pod, "err", rerr)
		}
		plog.With(ctx).Warnw("msg", "hub_shard_heartbeat_timeout", "pod", pod)
	}
	return nil
}

// ── 内部辅助 ──────────────────────────────────────────────────────────────────

// ensureShards:region 无候选分片时,按 Fleet 拓扑种入 Redis(W4 ⑤ Mock 期 lazy-seed)。
// 热路径只在该 region 首次无分片时打 Fleet 拉起种子;已有分片直接返回(不打 k8s,保持 AssignHub 轻量)。
// 拓扑漂移(pod 改名/下线)的对账交后台 reconcileShardTopology 处理,避免每次登录都查 apiserver。
func (u *HubUsecase) ensureShards(ctx context.Context, region, releaseTrack string) error {
	if !releasetrack.Valid(releaseTrack) {
		return errcode.New(errcode.ErrInvalidArg, "invalid hub release_track %q", releaseTrack)
	}
	shards, err := u.repo.ListShards(ctx)
	if err != nil {
		return err
	}
	for _, s := range shards {
		track, trackErr := stickyReleaseTrack(s.GetReleaseTrack())
		if trackErr == nil && s.Region == region && track == releaseTrack {
			return nil // 已有该 region + track 分片
		}
	}
	cands, err := u.fleet.ListShards(ctx, region)
	if err != nil {
		return err
	}
	now := time.Now().UnixMilli()
	for _, c := range cands {
		if !c.TokenReady || !releasetrack.Valid(c.ReleaseTrack) {
			// enforce 下令牌不可用的分片:不种 ready 镜像(否则 AssignHub 会分到回调被全拒的 Hub)。
			continue
		}
		rec := &hubv1.HubShardStorageRecord{
			HubPodName:        c.PodName,
			HubAddr:           c.Addr,
			Region:            c.Region,
			ShardId:           c.ShardID,
			PlayerCount:       0,
			Capacity:          c.Capacity,
			State:             u.initialShardState(),
			LastHeartbeatMs:   0, // 种子:从未心跳(扫描排除;requireHeartbeatReady 时为 warming 不可分配)
			CreatedAtMs:       now,
			CurrentTokenExpMs: u.candidateTokenExp(c.TokenExpMs), // exp 镜像(调试用,仅 enforce 门控下非 0)
			CurrentTokenGen:   u.candidateTokenGen(c.TokenGen),   // 令牌代际(仅 enforce 门控下非 0)
			ReleaseTrack:      c.ReleaseTrack,
			// exact 实例身份:仅 mode=local 非空(见 ShardCandidate.InstanceUID)。agones 留空,
			// 其身份由 Model B promote 后投影,不能由拓扑发现抢先写。
			GameserverUid: c.InstanceUID,
			AuthEpoch:     c.ProtocolEpoch,
		}
		if cerr := u.repo.CreateShard(ctx, rec, u.shardTTL()); cerr != nil {
			return cerr
		}
	}
	return nil
}

// reconcileShardTopology 后台按 Fleet 拓扑对账 Redis 分片镜像(每个 sweep tick 一次)。
// 解决:minikube/Agones 重启后 pod 名/端口变化,旧分片在 Redis 里成为孤儿 —— 心跳超时只会把它
// 标 draining(无 draining_since_ms),reclaimDrainedShards 跳过、sweep 又每 tick 续期 TTL,导致
// 永久残留并让重登玩家拿到过期 hub_ds_addr。这里以 Fleet 为权威补齐 live 分片并清理 stale 孤儿。
// 放后台而非 AssignHub 热路径:避免每次登录都打 k8s apiserver。
// Fleet 暂不可用或某 region 候选为空时,保留现有镜像作为降级(绝不误删)。
func (u *HubUsecase) reconcileShardTopology(ctx context.Context) error {
	shards, err := u.repo.ListShards(ctx)
	if err != nil {
		return err
	}
	// 需对账的 region:已存在分片的 region + 默认 region(便于发现首个分片)。
	regions := map[string]struct{}{u.cfg.DefaultRegion: {}}
	for _, s := range shards {
		if s.Region != "" {
			regions[s.Region] = struct{}{}
		}
	}
	now := time.Now().UnixMilli()
	for region := range regions {
		cands, lerr := u.fleet.ListShards(ctx, region)
		if lerr != nil {
			plog.With(ctx).Warnw("msg", "reconcile_topology_list_failed", "region", region, "err", lerr)
			continue // 降级:Fleet 不可用时保留现有镜像
		}
		// An empty routable list is not physical teardown.  We still walk the
		// existing mirrors below, mark them unroutable, and require an exact
		// GameServer+Pod observation before minting teardown proof.
		live := make(map[string]struct{}, len(cands))
		presentCandidate := make(map[string]struct{}, len(cands))
		for _, c := range cands {
			presentCandidate[c.PodName] = struct{}{}
			if !c.TokenReady || !releasetrack.Valid(c.ReleaseTrack) {
				// Credential/release metadata failure removes routing eligibility but
				// says nothing about the process.  Keep all ownership ledgers and
				// force a fresh authenticated heartbeat after recovery.
				_ = u.repo.UpdateShardWithLock(ctx, c.PodName, u.retry(), func(s *hubv1.HubShardStorageRecord) error {
					if s.State != stateStopping && !(s.State == stateDraining && s.DrainingSinceMs > 0) {
						s.State = stateWarming
					}
					return nil
				}, u.shardTTL())
				continue
			}
			live[c.PodName] = struct{}{}
			_, found, gerr := u.repo.GetShard(ctx, c.PodName)
			if gerr != nil {
				plog.With(ctx).Warnw("msg", "reconcile_topology_get_failed", "pod", c.PodName, "err", gerr)
				continue
			}
			if found {
				// 已有镜像:刷新地址/容量(pod 复用旧名但换端口/扩缩容时同步)。
				if uerr := u.repo.UpdateShardWithLock(ctx, c.PodName, u.retry(), func(s *hubv1.HubShardStorageRecord) error {
					if s.ReleaseTrack == "" {
						s.ReleaseTrack = c.ReleaseTrack // additive rollout:旧镜像只迁为 stable/实际 metadata 轨
					} else if s.ReleaseTrack != c.ReleaseTrack {
						s.State = stateDraining
						return nil // metadata 轨漂移：保留原轨证据并退出可分配集
					}
					s.HubAddr = c.Addr
					s.Region = c.Region
					s.ShardId = c.ShardID
					s.Capacity = c.Capacity
					// 滚动升级投毒防护(审核 P1 #2):旧镜像 allocator(requireHeartbeatReady=false)
					// 可能把分片建成 ready+LastHeartbeatMs=0。新镜像开启心跳门控后,该分片从未发过
					// (鉴权)心跳却是 ready,会被 AssignHub 直接选中 —— 把玩家分到 DS 令牌握手尚未
					// 经真实鉴权心跳确认的 Hub。这里把「从未心跳且非排空态」的分片降级回 warming,
					// 等首个通过 Guard 的心跳(HeartbeatShard 里 warming→ready)再放行分配。
					if u.requireHeartbeatReady && s.LastHeartbeatMs == 0 &&
						s.State != stateDraining && s.State != stateStopping {
						s.State = stateWarming
					}
					// Topology/heartbeat-loss draining has no consolidation timestamp.
					// A routable candidate may re-enter only through a fresh heartbeat;
					// deliberate scale-in draining (timestamped) remains irreversible.
					if s.State == stateDraining && s.DrainingSinceMs == 0 {
						if u.requireHeartbeatReady {
							s.State = stateWarming
						} else {
							s.State = stateReady
						}
					}
					// 令牌代际单调推进(审核 P1 #3/#12):CurrentTokenGen **只增不减**,绝不被 0/低代际清除或回退。
					// permissive 副本 / annotation 缺失 → 候选 gen=0 → **保持镜像既有代际不变**(不降级已生效的
					// enforce 代际门;旧方案的「off/permissive 清 0 自愈」是 fail-open 降级向量,已废弃)。
					// 只有严格更高的候选代际(重签/轮换,含密钥轮换验签失败触发的重签)才推进,并复位 warming
					// 等新代际鉴权心跳——挡住旧令牌迟到心跳把轮换后的分片重新置 ready。gen 精确单调(非秒级)。
					if gen := u.candidateTokenGen(c.TokenGen); gen > s.CurrentTokenGen {
						s.CurrentTokenGen = gen
						s.CurrentTokenExpMs = u.candidateTokenExp(c.TokenExpMs) // 同步 exp 镜像(调试用)
						if u.requireHeartbeatReady && s.State != stateDraining && s.State != stateStopping {
							s.State = stateWarming // 新代际未证明:等带新令牌的鉴权心跳再放行分配
						}
					}
					return nil
				}, u.shardTTL()); uerr != nil && errcode.As(uerr) != errcode.ErrHubNoAvailable {
					plog.With(ctx).Warnw("msg", "reconcile_topology_update_failed", "pod", c.PodName, "err", uerr)
				}
				continue
			}
			// 新 pod:补齐镜像。
			rec := &hubv1.HubShardStorageRecord{
				HubPodName:        c.PodName,
				HubAddr:           c.Addr,
				Region:            c.Region,
				ShardId:           c.ShardID,
				PlayerCount:       0,
				Capacity:          c.Capacity,
				State:             u.initialShardState(),
				LastHeartbeatMs:   0,
				CreatedAtMs:       now,
				CurrentTokenExpMs: u.candidateTokenExp(c.TokenExpMs),
				CurrentTokenGen:   u.candidateTokenGen(c.TokenGen),
				ReleaseTrack:      c.ReleaseTrack,
				GameserverUid:     c.InstanceUID,
				AuthEpoch:         c.ProtocolEpoch,
			}
			if cerr := u.repo.CreateShard(ctx, rec, u.shardTTL()); cerr != nil {
				plog.With(ctx).Warnw("msg", "reconcile_topology_create_failed", "pod", c.PodName, "err", cerr)
			}
		}
		// A shard absent from the routable set is retained as a physical-owner
		// fence.  Candidate absence includes Scheduled/Unhealthy/token failures
		// and must never erase sessions.  Only an optional exact observer can
		// record UID-specific teardown proof; durable assignment cleanup consumes
		// that proof one owner at a time.
		for _, s := range shards {
			if s.Region != region {
				continue
			}
			if _, ok := live[s.HubPodName]; ok {
				continue
			}
			if _, returnedButUnroutable := presentCandidate[s.HubPodName]; !returnedButUnroutable {
				if rerr := u.repo.UpdateShardWithLock(ctx, s.HubPodName, u.retry(), func(current *hubv1.HubShardStorageRecord) error {
					if current.State != stateStopping && !(current.State == stateDraining && current.DrainingSinceMs > 0) {
						current.State = stateDraining
					}
					return nil
				}, u.shardTTL()); rerr != nil && errcode.As(rerr) != errcode.ErrHubNoAvailable {
					plog.With(ctx).Warnw("msg", "reconcile_topology_fence_stale_failed", "pod", s.HubPodName, "region", region, "err", rerr)
				}
			}
			observer, observerOK := u.fleet.(HubFleetPhysicalObserver)
			proofRepo, proofOK := u.authRepo.(data.HubInstanceTeardownProofRepo)
			if observerOK && proofOK && s.GameserverUid != "" {
				observation, observeErr := observer.ObserveShardInstance(ctx, s.HubPodName)
				if observeErr != nil {
					plog.With(ctx).Warnw("msg", "reconcile_topology_physical_observation_failed",
						"pod", s.HubPodName, "expected_uid", s.GameserverUid, "err", observeErr)
				} else if observation.ProvesTeardown(s.GameserverUid) {
					if proofErr := proofRepo.RecordInstanceTeardownProof(ctx, s.HubPodName,
						s.GameserverUid, u.assignmentSagaTTL()); proofErr != nil {
						plog.With(ctx).Warnw("msg", "reconcile_topology_record_teardown_failed",
							"pod", s.HubPodName, "expected_uid", s.GameserverUid, "err", proofErr)
					} else {
						plog.With(ctx).Warnw("msg", "reconcile_topology_exact_uid_teardown_confirmed",
							"pod", s.HubPodName, "expected_uid", s.GameserverUid,
							"observed_uid", observation.GameServerUID)
					}
				}
			}
		}
	}
	return nil
}

// selectShard:队友所在分片优先,否则同 region 最空 ready 分片(并列取 shard_id 小者,稳定)。
func (u *HubUsecase) selectShard(ctx context.Context, region string, teamID uint64) (*hubv1.HubShardStorageRecord, error) {
	shards, err := u.repo.ListShards(ctx)
	if err != nil {
		return nil, err
	}
	if teamID != 0 {
		if pod, ok, gerr := u.repo.GetTeamShard(ctx, teamID); gerr == nil && ok {
			for _, s := range shards {
				if s.HubPodName == pod && s.Region == region && s.State == stateReady && s.PlayerCount < s.Capacity {
					return s, nil
				}
			}
		}
	}
	best := leastLoaded(shards, region, releasetrack.Stable, "")
	if best == nil {
		return nil, errcode.New(errcode.ErrHubNoAvailable, "no ready hub shard with capacity in region %s", region)
	}
	return best, nil
}

// reserveSeat:乐观锁占一个座位(复核 ready + 容量,player_count++)。
func (u *HubUsecase) reserveSeat(ctx context.Context, pod string) error {
	return u.repo.UpdateShardWithLock(ctx, pod, u.retry(), func(s *hubv1.HubShardStorageRecord) error {
		if s.State != stateReady {
			return errcode.New(errcode.ErrHubNoAvailable, "hub shard %s not ready", pod)
		}
		if s.PlayerCount >= s.Capacity {
			return errcode.New(errcode.ErrHubNoAvailable, "hub shard %s full", pod)
		}
		s.PlayerCount++
		return nil
	}, u.shardTTL())
}

// releaseFromShard:退一个座位(floor 0)。分片不存在/锁冲突静默(幂等退位)。
func (u *HubUsecase) releaseFromShard(ctx context.Context, pod string) {
	err := u.repo.UpdateShardWithLock(ctx, pod, u.retry(), func(s *hubv1.HubShardStorageRecord) error {
		if s.PlayerCount > 0 {
			s.PlayerCount--
		}
		return nil
	}, u.shardTTL())
	if err != nil && errcode.As(err) != errcode.ErrHubNoAvailable {
		plog.With(ctx).Warnw("msg", "release_from_shard_failed", "pod", pod, "err", err)
	}
}

// effectiveRoleID 选角生效值:调用方显式传的 roleID(>0)优先(login 是角色数据权威),
// 否则回退归属镜像已存值(Transfer/重签路径不丢角色)。
func effectiveRoleID(requested, stored uint32) uint32 {
	if requested > 0 {
		return requested
	}
	return stored
}

// reserveRoutableSeat 原子占一个座位:Model B(authRepo 装配)走 ReserveRoutableSeat 单事务
// 授权+路由+占座门(审核二轮 CE6),返回本次绑定的 active 元组供钉进归属;legacy 走单纯 reserveSeat。
// 不可路由(授权未激活 / 分片非 ready / 元组不符 / 心跳陈旧 / 已满)→ ErrHubNoAvailable(fail-closed)。
func (u *HubUsecase) reserveRoutableSeat(ctx context.Context, pod string, playerID uint64, assignmentID string) (*data.ReserveResult, error) {
	if u.authRepo == nil {
		return nil, u.reserveSeat(ctx, pod) // legacy:纯容量占座
	}
	nowMs := time.Now().UnixMilli()
	current, err := u.authRepo.CheckRoutable(ctx, pod, nowMs, u.heartbeatMaxAgeMs())
	if err != nil {
		return nil, err
	}
	if !current.OK {
		plog.With(ctx).Warnw("msg", "hub_reserve_not_routable", "pod", pod, "reason", current.Reason)
		return nil, errcode.New(errcode.ErrHubNoAvailable, "hub shard %s not routable: %s", pod, current.Reason)
	}
	res, err := u.authRepo.ReserveAssignment(ctx, pod, data.ReservationIdentity{
		PlayerID: playerID, AssignmentID: assignmentID, InstanceUID: current.InstanceUID,
		ProtocolEpoch: current.ProtocolEpoch, WriterEpoch: current.WriterEpoch,
		ExpiresAtMs:           nowMs + u.reservationTTL().Milliseconds(),
		AssignmentExpiresAtMs: nowMs + u.assignTTL().Milliseconds(),
	}, nowMs, u.heartbeatMaxAgeMs(), u.shardTTL())
	if err != nil {
		return nil, err
	}
	if !res.OK {
		plog.With(ctx).Warnw("msg", "hub_reserve_not_routable", "pod", pod, "reason", res.Reason)
		return nil, errcode.New(errcode.ErrHubNoAvailable, "hub shard %s not routable: %s", pod, res.Reason)
	}
	return &res, nil
}

// ensureExistingAssignmentSeat 是旧 assignment 重签/同实例凭据重绑前的最终容量门。
// assignment key 与 {pod} ledger 不同 slot，故调用方仍需随后对完整旧 assignment 做 CAS；
// 本函数只在线性化的 {pod} 事务中保证以下二选一成立：
//   - exact assignment 已是 connected session：幂等返回，不增加容量；
//   - exact assignment 尚无 ledger owner：在仍可路由且未满时创建/刷新有界 reservation。
//
// 调用方在后续 assignment CAS 失败时不得盲目 ReleaseAssignmentSeat：返回的 seat 可能是
// 另一条仍存活连接的原 session。CAS winner 会按旧 assignment 精确释放；极端反序中新建但未被
// winner 观察到的 reservation 也只存活 ReservationTTL。
func (u *HubUsecase) ensureExistingAssignmentSeat(ctx context.Context, playerID uint64,
	assignment *hubv1.HubAssignmentStorageRecord, current *data.ReserveResult) (*data.ReserveResult, error) {
	if u.authRepo == nil {
		return current, nil
	}
	if assignment == nil || current == nil || !assignmentSameInstance(assignment, current) ||
		assignment.GetPlayerId() != playerID || assignment.GetAssignmentId() == "" {
		return nil, errcode.New(errcode.ErrInvalidState, "hub existing assignment identity is not reusable")
	}
	nowMs := time.Now().UnixMilli()
	res, err := u.authRepo.ReserveAssignment(ctx, assignment.GetHubPodName(), data.ReservationIdentity{
		PlayerID: playerID, AssignmentID: assignment.GetAssignmentId(), InstanceUID: current.InstanceUID,
		ProtocolEpoch: current.ProtocolEpoch, WriterEpoch: current.WriterEpoch,
		ExpiresAtMs:           nowMs + u.reservationTTL().Milliseconds(),
		AssignmentExpiresAtMs: nowMs + u.assignTTL().Milliseconds(),
	}, nowMs, u.heartbeatMaxAgeMs(), u.shardTTL())
	if err != nil {
		return nil, err
	}
	if !res.OK {
		return nil, errcode.New(errcode.ErrHubNoAvailable,
			"hub shard %s cannot ensure existing assignment seat: %s", assignment.GetHubPodName(), res.Reason)
	}
	return &res, nil
}

// assignmentRoutable 返回归属目标的权威路由快照，并验证归属钉住的完整 active 身份仍为当前值。
// Model B 数据面永久只接受 writer=2 的完整 assignment tuple；legacy/缺字段/future writer 的
// 一次性迁移必须由 activation 控制面在开放业务流量前完成，不能在请求路径里静默“升级”。
func (u *HubUsecase) assignmentRoutable(
	ctx context.Context,
	playerID uint64,
	a *hubv1.HubAssignmentStorageRecord,
) (data.ReserveResult, bool, error) {
	assignmentTrack, trackErr := stickyReleaseTrack(a.GetReleaseTrack())
	if trackErr != nil {
		return data.ReserveResult{}, false, trackErr
	}
	if u.authRepo == nil {
		shard, found, err := u.repo.GetShard(ctx, a.HubPodName)
		if err != nil || !found || shard.State != stateReady {
			return data.ReserveResult{}, false, err
		}
		shardTrack, shardTrackErr := stickyReleaseTrack(shard.GetReleaseTrack())
		if shardTrackErr != nil || shardTrack != assignmentTrack {
			return data.ReserveResult{}, false, shardTrackErr
		}
		return data.ReserveResult{
			OK: true, ShardID: shard.ShardId, HubAddr: shard.HubAddr, Region: shard.Region,
			PlayerCount: shard.PlayerCount, Capacity: shard.Capacity, ReleaseTrack: shardTrack,
		}, true, nil
	}
	if !assignmentBindingV2Complete(a, playerID) {
		return data.ReserveResult{}, false, errcode.New(errcode.ErrInvalidState,
			"hub assignment is not a complete writer-v2 binding")
	}
	info, err := u.authRepo.CheckRoutable(ctx, a.HubPodName, time.Now().UnixMilli(), u.heartbeatMaxAgeMs())
	if err != nil {
		return data.ReserveResult{}, false, err
	}
	if !info.OK {
		return info, false, nil
	}
	infoTrack, infoTrackErr := stickyReleaseTrack(info.ReleaseTrack)
	if infoTrackErr != nil || infoTrack != assignmentTrack {
		return info, false, infoTrackErr
	}
	info.ReleaseTrack = infoTrack
	if a.HubInstanceUid != info.InstanceUID || a.AuthEpoch != info.ProtocolEpoch || a.AuthGen != info.ActiveGen {
		return info, false, nil
	}
	if a.AuthJti != info.ActiveJTI {
		return info, false, nil
	}
	if a.AuthWriterEpoch != info.WriterEpoch {
		return info, false, nil
	}
	return info, true, nil
}

func assignmentBindingV2Complete(a *hubv1.HubAssignmentStorageRecord, playerID uint64) bool {
	_, trackErr := stickyReleaseTrack(a.GetReleaseTrack())
	return a != nil && trackErr == nil && playerID != 0 && a.GetPlayerId() == playerID && a.GetHubPodName() != "" &&
		a.GetHubInstanceUid() != "" && a.GetAuthEpoch() != 0 && a.GetAuthGen() != 0 &&
		a.GetAuthJti() != "" && a.GetAssignmentId() != "" &&
		a.GetAuthWriterEpoch() == auth.DSAuthWriterEpochV2
}

// assignmentSameInstance 区分“同名 Pod 的凭据轮换”和“同名 GameServer 重建”。
// 只有 UID+protocol epoch 仍完全相同且当前权威可路由时，旧 assignment 的座位才可原地重绑；
// UID/epoch 任一变化必须走新占座，旧 token/assignment 永不复用。
func assignmentSameInstance(a *hubv1.HubAssignmentStorageRecord, current *data.ReserveResult) bool {
	if a == nil || current == nil {
		return false
	}
	aTrack, aTrackErr := stickyReleaseTrack(a.GetReleaseTrack())
	currentTrack, currentTrackErr := stickyReleaseTrack(current.ReleaseTrack)
	return current.OK && a.HubPodName != "" &&
		aTrackErr == nil && currentTrackErr == nil && aTrack == currentTrack &&
		a.HubInstanceUid != "" && a.HubInstanceUid == current.InstanceUID &&
		a.AuthEpoch != 0 && a.AuthEpoch == current.ProtocolEpoch && a.AuthGen != 0 &&
		a.AuthJti != "" && a.AuthWriterEpoch == auth.DSAuthWriterEpochV2 &&
		current.WriterEpoch == auth.DSAuthWriterEpochV2
}

// selectAndReserveShard 按队友优先、负载升序尝试所有候选；每个候选都必须通过最终原子授权+占座门。
func (u *HubUsecase) selectAndReserveShard(ctx context.Context, playerID uint64, assignmentID, region string, teamID uint64, excludePod, releaseTrack string) (*hubv1.HubShardStorageRecord, *data.ReserveResult, error) {
	if !releasetrack.Valid(releaseTrack) {
		return nil, nil, errcode.New(errcode.ErrInvalidArg, "invalid hub release_track %q", releaseTrack)
	}
	shards, err := u.repo.ListShards(ctx)
	if err != nil {
		return nil, nil, err
	}
	candidates := make([]*hubv1.HubShardStorageRecord, 0, len(shards))
	for _, shard := range shards {
		track, trackErr := stickyReleaseTrack(shard.GetReleaseTrack())
		if trackErr == nil && track == releaseTrack && shard.Region == region && shard.HubPodName != excludePod && shard.State == stateReady && shard.PlayerCount < shard.Capacity {
			candidates = append(candidates, shard)
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].PlayerCount != candidates[j].PlayerCount {
			return candidates[i].PlayerCount < candidates[j].PlayerCount
		}
		return candidates[i].ShardId < candidates[j].ShardId
	})
	if teamID != 0 {
		if pod, found, gerr := u.repo.GetTeamShard(ctx, teamID); gerr != nil {
			return nil, nil, gerr
		} else if found {
			for i, candidate := range candidates {
				if candidate.HubPodName == pod {
					candidates[0], candidates[i] = candidates[i], candidates[0]
					break
				}
			}
		}
	}
	for _, candidate := range candidates {
		seat, rerr := u.reserveRoutableSeat(ctx, candidate.HubPodName, playerID, assignmentID)
		if rerr != nil {
			if errcode.As(rerr) == errcode.ErrHubNoAvailable {
				continue
			}
			return nil, nil, rerr
		}
		return authoritativeShard(candidate, seat), seat, nil
	}
	return nil, nil, errcode.New(errcode.ErrHubNoAvailable, "no authoritatively routable hub shard in region %s", region)
}

func authoritativeShard(shard *hubv1.HubShardStorageRecord, seat *data.ReserveResult) *hubv1.HubShardStorageRecord {
	out := proto.Clone(shard).(*hubv1.HubShardStorageRecord)
	if seat != nil {
		out.HubAddr = seat.HubAddr
		out.Region = seat.Region
		out.ShardId = seat.ShardID
		out.PlayerCount = seat.PlayerCount
		out.Capacity = seat.Capacity
		out.ReleaseTrack = seat.ReleaseTrack
	}
	return out
}

func (u *HubUsecase) compensateReservedSeat(ctx context.Context, pod string, playerID uint64, assignmentID string, seat *data.ReserveResult) {
	if u.authRepo == nil {
		u.releaseFromShard(ctx, pod)
		return
	}
	if seat == nil {
		return
	}
	released, err := u.authRepo.ReleaseAssignmentSeat(ctx, pod, data.AssignmentInstanceIdentity{
		PlayerID: playerID, AssignmentID: assignmentID, InstanceUID: seat.InstanceUID,
		ProtocolEpoch: seat.ProtocolEpoch, WriterEpoch: seat.WriterEpoch,
	}, u.shardTTL())
	if err != nil || !released {
		plog.With(ctx).Warnw("msg", "hub_reserved_seat_compensation_failed", "pod", pod, "released", released, "err", err)
	}
}

func (u *HubUsecase) releaseAssignmentSeat(ctx context.Context, assignment *hubv1.HubAssignmentStorageRecord) {
	if u.authRepo == nil {
		u.releaseFromShard(ctx, assignment.HubPodName)
		return
	}
	released, err := u.authRepo.ReleaseAssignmentSeat(ctx, assignment.HubPodName,
		assignmentInstanceIdentity(assignment), u.shardTTL())
	if err != nil {
		plog.With(ctx).Warnw("msg", "hub_assignment_seat_release_failed", "pod", assignment.HubPodName, "err", err)
	} else if !released {
		plog.With(ctx).Infow("msg", "hub_assignment_seat_release_skipped_stale_instance", "pod", assignment.HubPodName)
	}
}

func (u *HubUsecase) routableShardViews(ctx context.Context, shards []*hubv1.HubShardStorageRecord) ([]*hubv1.HubShardStorageRecord, error) {
	if u.authRepo == nil {
		return shards, nil
	}
	out := make([]*hubv1.HubShardStorageRecord, 0, len(shards))
	for _, shard := range shards {
		if shard.State != stateReady {
			continue
		}
		info, err := u.authRepo.CheckRoutable(ctx, shard.HubPodName, time.Now().UnixMilli(), u.heartbeatMaxAgeMs())
		if err != nil {
			return nil, err
		}
		if info.OK {
			out = append(out, authoritativeShard(shard, &info))
		}
	}
	return out, nil
}

// bindAssignmentAuth 把 Model B 占座时确认的 active 元组钉进归属记录(审计 + 复用漂移检测,§7)。
// seat=nil(legacy/off,reserveSeat 返回 nil)时不动。
func bindAssignmentAuth(a *hubv1.HubAssignmentStorageRecord, seat *data.ReserveResult) {
	if seat == nil {
		return
	}
	a.HubInstanceUid = seat.InstanceUID
	a.AuthEpoch = seat.ProtocolEpoch
	a.AuthGen = seat.ActiveGen
	a.AuthJti = seat.ActiveJTI
	a.AuthWriterEpoch = seat.WriterEpoch
}

func transferCleanupRef(a *hubv1.HubAssignmentStorageRecord) data.TransferCleanupRef {
	if a == nil {
		return data.TransferCleanupRef{}
	}
	return data.TransferCleanupRef{PlayerID: a.GetPlayerId(), TargetAssignmentID: a.GetAssignmentId()}
}

func bindTransferCleanupSource(target, source *hubv1.HubAssignmentStorageRecord) error {
	if target == nil || source == nil || target.GetPlayerId() == 0 || target.GetPlayerId() != source.GetPlayerId() ||
		target.GetAssignmentId() == "" || source.GetAssignmentId() == "" ||
		target.GetAssignmentId() == source.GetAssignmentId() || source.GetHubPodName() == "" ||
		source.GetHubInstanceUid() == "" || source.GetAuthEpoch() == 0 ||
		source.GetAuthWriterEpoch() != auth.DSAuthWriterEpochV2 {
		return errcode.New(errcode.ErrInvalidState, "complete exact Hub transfer source owner required")
	}
	target.TransferCleanupPending = true
	target.TransferTargetBound = false
	target.TransferSourceHubPodName = source.GetHubPodName()
	target.TransferSourceAssignmentId = source.GetAssignmentId()
	target.TransferSourceInstanceUid = source.GetHubInstanceUid()
	target.TransferSourceAuthEpoch = source.GetAuthEpoch()
	target.TransferSourceAuthWriterEpoch = source.GetAuthWriterEpoch()
	target.ReleaseCleanupPending = false
	return nil
}

func transferCleanupSource(a *hubv1.HubAssignmentStorageRecord) (*hubv1.HubAssignmentStorageRecord, error) {
	if a == nil || !a.GetTransferCleanupPending() || a.GetReleaseCleanupPending() ||
		a.GetPlayerId() == 0 || a.GetAssignmentId() == "" ||
		a.GetTransferSourceHubPodName() == "" || a.GetTransferSourceAssignmentId() == "" ||
		a.GetTransferSourceAssignmentId() == a.GetAssignmentId() || a.GetTransferSourceInstanceUid() == "" ||
		a.GetTransferSourceAuthEpoch() == 0 ||
		a.GetTransferSourceAuthWriterEpoch() != auth.DSAuthWriterEpochV2 {
		return nil, errcode.New(errcode.ErrInvalidState, "Hub transfer cleanup source identity invalid")
	}
	return &hubv1.HubAssignmentStorageRecord{
		PlayerId: a.GetPlayerId(), HubPodName: a.GetTransferSourceHubPodName(),
		AssignmentId: a.GetTransferSourceAssignmentId(), HubInstanceUid: a.GetTransferSourceInstanceUid(),
		AuthEpoch: a.GetTransferSourceAuthEpoch(), AuthWriterEpoch: a.GetTransferSourceAuthWriterEpoch(),
	}, nil
}

func clearTransferCleanup(a *hubv1.HubAssignmentStorageRecord) {
	if a == nil {
		return
	}
	a.TransferCleanupPending = false
	a.TransferTargetBound = false
	a.TransferSourceHubPodName = ""
	a.TransferSourceAssignmentId = ""
	a.TransferSourceInstanceUid = ""
	a.TransferSourceAuthEpoch = 0
	a.TransferSourceAuthWriterEpoch = 0
}

func assignmentInstanceIdentity(a *hubv1.HubAssignmentStorageRecord) data.AssignmentInstanceIdentity {
	if a == nil {
		return data.AssignmentInstanceIdentity{}
	}
	return data.AssignmentInstanceIdentity{PlayerID: a.GetPlayerId(), AssignmentID: a.GetAssignmentId(),
		InstanceUID: a.GetHubInstanceUid(), ProtocolEpoch: a.GetAuthEpoch(), WriterEpoch: a.GetAuthWriterEpoch()}
}

// replaceAssignmentSaga 是「把玩家归属置换为 next」的唯一 saga 实现,AssignHub 的置换路径与
// TransferHub 共用(此前是两份人工镜像的拷贝,补偿规则漂移即座位泄漏 / 提前释放已提交的新 owner)。
//
// 调用前提:新座位已占(seat)、票已签成功;old == nil 表示新建(无旧 owner)。
// 返回 (retry=true, nil) 表示 CAS 输给并发写者、已补偿干净,调用方 continue 重试;
// 返回 err 时调用方原样上抛(补偿责任已在本方法内履行完毕,或刻意不补偿交重启对账)。
//
// ⚠️ migratePlayer(本文件下方的 drain 迁移)是本 saga 的**变体而非拷贝**:它签票在
// registerTransferCleanup 之后(与这里相反,签票失败会留孤儿 ref 靠重启对账兜底)、
// CAS 输者不重试、resume 失败走「回加源 member 索引」而非上抛、且无写者复核。
// 并入前须先补 drain 侧故障注入测试,不得机械合并。
func (u *HubUsecase) replaceAssignmentSaga(
	ctx context.Context,
	playerID uint64,
	old, next *hubv1.HubAssignmentStorageRecord,
	seat *data.ReserveResult,
	onSwapped func(),
	disappearedMsg string,
) (retry bool, err error) {
	// Model B(authRepo 注入)下换分片必须走 owner-cleanup saga:先登记旧 owner 的
	// 精确清理(index-first ref + 阶段字段),CAS 落盘后由 resumeAssignmentCleanup
	// 驱逐旧物理席位;legacy 路径仍是 CAS 后立即释放旧席位。
	cleanupRegistered := false
	if old != nil && u.authRepo != nil {
		if cleanupErr := u.registerTransferCleanup(ctx, next, old); cleanupErr != nil {
			u.compensateReservedSeat(ctx, next.GetHubPodName(), playerID, next.GetAssignmentId(), seat)
			return false, cleanupErr
		}
		cleanupRegistered = true
	}
	swapped, serr := u.repo.CompareAndSwapAssignment(ctx, playerID, old, next, u.assignmentSagaTTL())
	if serr != nil {
		// The CAS result may be unknown. Keep the index-first ref and exact
		// reservation; restart reconciliation distinguishes a committed saga
		// from an orphan without risking the new owner.
		if !cleanupRegistered {
			u.compensateReservedSeat(ctx, next.GetHubPodName(), playerID, next.GetAssignmentId(), seat)
		}
		return false, serr
	}
	if !swapped {
		if cleanupRegistered {
			u.removeTransferCleanupRef(ctx, old.GetHubPodName(), transferCleanupRef(next))
		}
		u.compensateReservedSeat(ctx, next.GetHubPodName(), playerID, next.GetAssignmentId(), seat)
		return true, nil
	}

	u.addShardMember(ctx, next.GetHubPodName(), playerID)
	if onSwapped != nil {
		onSwapped()
	}
	if cleanupRegistered {
		// 旧 owner 驱逐是显式 saga:源席位物理未离场时返回 ErrUnavailable,
		// 保留持久化的新 assignment 供 Login/reconcile 恢复,绝不双 owner。
		_, stillFound, resumeErr := u.resumeAssignmentCleanup(ctx, playerID, next.GetAssignmentId())
		if resumeErr != nil {
			return false, resumeErr
		}
		if !stillFound {
			return false, errcode.New(errcode.ErrInvalidState, disappearedMsg)
		}
	} else if old != nil {
		u.releaseAssignmentSeat(ctx, old)
		u.removeShardMember(ctx, old.GetHubPodName(), playerID)
	}
	return false, u.confirmWriterForTicket(ctx, playerID)
}

func (u *HubUsecase) registerTransferCleanup(ctx context.Context, target,
	source *hubv1.HubAssignmentStorageRecord) error {
	if u.authRepo == nil {
		return errcode.New(errcode.ErrUnavailable, "Hub owner cleanup authority unavailable")
	}
	if source.GetTransferCleanupPending() || source.GetReleaseCleanupPending() {
		return errcode.New(errcode.ErrInvalidState, "previous Hub owner cleanup is still pending")
	}
	if err := bindTransferCleanupSource(target, source); err != nil {
		return err
	}
	// Index-first is deliberate. A crash/CAS loser can leave only an orphan
	// ref; a successful assignment CAS can never become invisible to restart.
	return u.repo.RegisterTransferCleanup(ctx, source.GetHubPodName(), transferCleanupRef(target))
}

func (u *HubUsecase) removeTransferCleanupRef(ctx context.Context, sourcePod string,
	ref data.TransferCleanupRef) {
	if err := u.repo.RemoveTransferCleanup(ctx, sourcePod, ref); err != nil {
		plog.With(ctx).Warnw("msg", "hub_owner_cleanup_index_remove_failed", "source_pod", sourcePod,
			"player_id", ref.PlayerID, "assignment_id", ref.TargetAssignmentID, "err", err)
	}
}

// resumeAssignmentCleanup is the only phase driver for transfer/release owner
// cleanup. It uses no process-local state: every retry starts from the current
// assignment, confirms the same target Bind before source release, and advances
// with exact CAS. A changed assignment id is never touched.
func (u *HubUsecase) resumeAssignmentCleanup(ctx context.Context, playerID uint64,
	assignmentID string) (*hubv1.HubAssignmentStorageRecord, bool, error) {
	if playerID == 0 || assignmentID == "" {
		return nil, false, errcode.New(errcode.ErrInvalidArg, "Hub cleanup assignment identity required")
	}
	if u.authRepo == nil {
		return nil, false, errcode.New(errcode.ErrUnavailable, "Hub owner cleanup authority unavailable")
	}
	for attempt := 0; attempt < 16; attempt++ {
		current, found, err := u.repo.GetAssignment(ctx, playerID)
		if err != nil {
			return nil, false, err
		}
		if !found {
			return nil, false, nil
		}
		if current.GetAssignmentId() != assignmentID {
			return nil, false, errcode.New(errcode.ErrLocatorConflict,
				"Hub cleanup assignment was superseded")
		}
		if current.GetTransferCleanupPending() && current.GetReleaseCleanupPending() {
			return nil, false, errcode.New(errcode.ErrInvalidState,
				"Hub assignment has conflicting cleanup phases")
		}
		if current.GetReleaseCleanupPending() {
			ref := transferCleanupRef(current)
			seat, inspectErr := u.authRepo.InspectAssignmentSeat(ctx, current.GetHubPodName(),
				assignmentInstanceIdentity(current))
			if inspectErr != nil {
				return nil, false, inspectErr
			}
			if seat.Conflict || (!seat.Reserved && !seat.Connected && !seat.AlreadyAbsent) {
				return nil, false, errcode.New(errcode.ErrInvalidState,
					"Hub release cleanup exact owner conflict")
			}
			result, releaseErr := u.authRepo.ReleaseAssignmentSeatExact(ctx, current.GetHubPodName(),
				assignmentInstanceIdentity(current), u.shardTTL())
			if releaseErr != nil {
				return nil, false, releaseErr
			}
			if result.DepartureRequired {
				return nil, false, errcode.New(errcode.ErrUnavailable,
					"source Hub became connected while release cleanup was running")
			}
			if result.Conflict || (!result.Released && !result.AlreadyAbsent) {
				return nil, false, errcode.New(errcode.ErrInvalidState,
					"Hub release cleanup exact owner conflict")
			}
			deleted, deleteErr := u.repo.CompareAndSwapAssignment(ctx, playerID, current, nil, 0)
			if deleteErr != nil {
				return nil, false, deleteErr
			}
			if !deleted {
				continue
			}
			u.removeShardMember(ctx, current.GetHubPodName(), playerID)
			u.removeTransferCleanupRef(ctx, current.GetHubPodName(), ref)
			return nil, false, nil
		}
		if !current.GetTransferCleanupPending() {
			if current.GetTransferTargetBound() || current.GetTransferSourceHubPodName() != "" ||
				current.GetTransferSourceAssignmentId() != "" || current.GetTransferSourceInstanceUid() != "" ||
				current.GetTransferSourceAuthEpoch() != 0 || current.GetTransferSourceAuthWriterEpoch() != 0 ||
				current.GetReleaseCleanupMatchId() != 0 || current.GetReleaseCleanupPlacementVersion() != 0 ||
				current.GetReleaseCleanupOperationId() != "" {
				return nil, false, errcode.New(errcode.ErrInvalidState,
					"Hub assignment has orphan cleanup fields")
			}
			return current, true, nil
		}
		source, sourceErr := transferCleanupSource(current)
		if sourceErr != nil {
			return nil, false, sourceErr
		}
		ref := transferCleanupRef(current)
		if !current.GetTransferTargetBound() {
			next := proto.Clone(current).(*hubv1.HubAssignmentStorageRecord)
			next.TransferTargetBound = true
			marked, markErr := u.repo.CompareAndSwapAssignment(ctx, playerID, current, next, u.assignmentSagaTTL())
			if markErr != nil {
				return nil, false, markErr
			}
			if !marked {
				continue
			}
			current = next
		}
		seat, inspectErr := u.authRepo.InspectAssignmentSeat(ctx, source.GetHubPodName(),
			assignmentInstanceIdentity(source))
		if inspectErr != nil {
			return nil, false, inspectErr
		}
		if seat.Conflict || (!seat.Reserved && !seat.Connected && !seat.AlreadyAbsent) {
			return nil, false, errcode.New(errcode.ErrInvalidState,
				"Hub transfer source cleanup exact owner conflict")
		}
		result, releaseErr := u.authRepo.ReleaseAssignmentSeatExact(ctx, source.GetHubPodName(),
			assignmentInstanceIdentity(source), u.shardTTL())
		if releaseErr != nil {
			return nil, false, releaseErr
		}
		if result.DepartureRequired {
			return nil, false, errcode.New(errcode.ErrUnavailable,
				"source Hub became connected while transfer cleanup was running")
		}
		if result.Conflict || (!result.Released && !result.AlreadyAbsent) {
			return nil, false, errcode.New(errcode.ErrInvalidState,
				"Hub transfer source cleanup exact owner conflict")
		}
		next := proto.Clone(current).(*hubv1.HubAssignmentStorageRecord)
		clearTransferCleanup(next)
		cleared, clearErr := u.repo.CompareAndSwapAssignment(ctx, playerID, current, next, u.assignmentSagaTTL())
		if clearErr != nil {
			return nil, false, clearErr
		}
		if !cleared {
			continue
		}
		u.removeShardMember(ctx, source.GetHubPodName(), playerID)
		u.removeTransferCleanupRef(ctx, source.GetHubPodName(), ref)
		return next, true, nil
	}
	return nil, false, errcode.New(errcode.ErrInternal, "Hub owner cleanup CAS retry exhausted")
}

func ticketBindingFromAssignment(a *hubv1.HubAssignmentStorageRecord) HubTicketBinding {
	releaseTrack, trackErr := stickyReleaseTrack(a.GetReleaseTrack())
	if a == nil || a.HubPodName == "" || a.HubInstanceUid == "" || a.AuthEpoch == 0 || a.AuthGen == 0 ||
		a.AuthJti == "" || a.AssignmentId == "" || a.AuthWriterEpoch != auth.DSAuthWriterEpochV2 || trackErr != nil {
		return HubTicketBinding{}
	}
	return HubTicketBinding{
		PodName: a.HubPodName, InstanceUID: a.HubInstanceUid, ProtocolEpoch: a.AuthEpoch,
		CredentialGen: a.AuthGen, CredentialJTI: a.AuthJti, HubAssignmentID: a.AssignmentId,
		WriterEpoch:  a.AuthWriterEpoch,
		ReleaseTrack: releaseTrack,
	}
}

// localTicketBinding 在 legacy 本地面为 hub 票据补齐完整实例绑定(七元组)。
//
// legacy 面 writer-v2 绑定的唯一写入点 bindAssignmentAuth 在 seat==nil 时不写,归属记录
// 恒为零值绑定,ticketBindingFromAssignment 只能返回零值 → 签出的票缺 hub_assignment_id
// 与实例绑定,UE Hub DS PostLogin fail-closed 踢人,客户端陷入 7s 重连循环
// (2026-08-04 mode=local 实测)。这里取 LocalCredentialACK —— 与 env 一次性下发给本机
// Hub DS 的**同一份**凭据(心跳应答 ACK 回显的也是它),拼出的绑定与 DS 自持身份逐字段
// 相等,与 Model B 占座绑定语义等价,不是伪造;WriterEpoch 由 SignHubCredential 恒盖
// DSAuthWriterEpochV2,login 回验的 requireHubAssignmentBinding 分支同样能过。
//
// 线上隔离(双重机械门,均与 applyLocalCredentialACK 同手法):
//   - u.authRepo != nil(Model B 权威面)直接返回零值,不碰本地凭据源;
//   - localHubCredentialSource 仅 LocalHubFleetProvider 实现,Agones/Mock fleet 类型断言
//     恒 false。任一门不过都保持旧行为(签无绑定票),k8s/Agones 生产路径零变更。
func (u *HubUsecase) localTicketBinding(a *hubv1.HubAssignmentStorageRecord) HubTicketBinding {
	if u.authRepo != nil {
		return HubTicketBinding{}
	}
	src, ok := u.fleet.(localHubCredentialSource)
	if !ok {
		return HubTicketBinding{}
	}
	releaseTrack, trackErr := stickyReleaseTrack(a.GetReleaseTrack())
	if trackErr != nil || a.GetHubPodName() == "" || a.GetAssignmentId() == "" {
		return HubTicketBinding{}
	}
	cred, ok := src.LocalCredentialACK(a.GetHubPodName())
	if !ok {
		return HubTicketBinding{}
	}
	return HubTicketBinding{
		PodName: a.GetHubPodName(), InstanceUID: cred.InstanceUID, ProtocolEpoch: cred.ProtocolEpoch,
		CredentialGen: cred.Gen, CredentialJTI: cred.JTI, HubAssignmentID: a.GetAssignmentId(),
		WriterEpoch:  cred.WriterEpoch,
		ReleaseTrack: releaseTrack,
	}
}

// ownerTargetForHubTicket 推导签票点 owner Begin 的 exact 实例目标(§9.22 四元组 + 分配 ID + 轨道)。
//
// **Model B(authRepo != nil)**:调用方已过 assignmentBindingV2Complete,归属记录必带完整
// writer-v2 绑定,目标逐字段取自归属记录 —— 与历史(取自 ticketBindingFromAssignment)完全
// 同源同值,生产路径行为零变更。取不出 = 数据不自洽,返回 false 由调用方 fail-closed 拒签。
//
// **legacy(authRepo == nil,即 mock / local)**:writer-v2 绑定的唯一写入点 bindAssignmentAuth
// 在 seat==nil 时直接 return,归属记录恒无 instance_uid/auth_epoch,历史上因此拼不出 exact
// 身份、owner Begin 必被判 15005,只能把整个权威面关掉 —— 代价是 owner 权威里永远不存在任何
// 记录,而 login 的 §9.23 query-first 回查以 owner 为第一权威:玩家选完角、Hub 也分配成功,
// GetResumeContext 仍恒落 WAIT/OWNER_UNKNOWN,客户端退避重查到超时,永远进不去大厅
// (2026-08-04 mode=local 实测)。
// 这里回源分片镜像补齐 uid/epoch:mode=local 的分片带真实进程实例身份(LocalHubFleetProvider
// 播种,与下发给 Hub DS 的凭据同源),据此拼出的目标与 §9.22 语义等价,不是伪造。
// mock 模式镜像里没有实例身份 → 返回 false,调用方跳过 Begin,保持历史行为不变。
func (u *HubUsecase) ownerTargetForHubTicket(ctx context.Context,
	a *hubv1.HubAssignmentStorageRecord) (data.OwnerTargetView, bool) {
	releaseTrack, trackErr := stickyReleaseTrack(a.GetReleaseTrack())
	if trackErr != nil || a.GetHubPodName() == "" || a.GetAssignmentId() == "" {
		return data.OwnerTargetView{}, false
	}
	uid, epoch := a.GetHubInstanceUid(), a.GetAuthEpoch()
	if uid == "" || epoch == 0 {
		if u.authRepo != nil {
			// Model B 不允许回源降级:授权权威是 Redis 授权记录,分片镜像只是投影。
			return data.OwnerTargetView{}, false
		}
		shard, found, err := u.repo.GetShard(ctx, a.GetHubPodName())
		if err != nil || !found {
			return data.OwnerTargetView{}, false
		}
		uid, epoch = shard.GetGameserverUid(), shard.GetAuthEpoch()
		if uid == "" || epoch == 0 {
			return data.OwnerTargetView{}, false
		}
	}
	return data.OwnerTargetView{
		PodName:                  a.GetHubPodName(),
		InstanceUID:              uid,
		InstanceEpoch:            epoch,
		AssignmentOrAllocationID: a.GetAssignmentId(),
		ReleaseTrack:             releaseTrack,
	}, true
}

func (u *HubUsecase) signHubTicket(ctx context.Context, playerID uint64, roleID uint32,
	assignment *hubv1.HubAssignmentStorageRecord, sourceMatchID uint64, sessionJTI string,
) (string, int64, error) {
	if assignment == nil || u.signer == nil {
		return "", 0, errcode.New(errcode.ErrUnavailable, "Hub ticket signer unavailable")
	}
	if u.authRepo != nil && !assignmentBindingV2Complete(assignment, playerID) {
		return "", 0, errcode.New(errcode.ErrInvalidState,
			"refuse to sign hub ticket from incomplete writer-v2 assignment")
	}
	binding := ticketBindingFromAssignment(assignment)
	if binding.PodName == "" && u.authRepo == nil {
		binding = u.localTicketBinding(assignment)
	}
	binding.SourceMatchID = sourceMatchID
	binding.SessionJTI = sessionJTI
	token, expMs, err := u.signer.SignHubTicket(playerID, roleID, binding)
	if err != nil {
		plog.With(ctx).Errorw("msg", "sign_hub_ticket_failed", "player_id", playerID, "err", err)
		return "", 0, errcode.New(errcode.ErrInternal, "sign hub ticket failed")
	}
	// owner 归属定案(owner-authority.md ①/④;contract 阶段=强依赖):签票是 hub 归属定案的
	// 统一出口(分配/恢复/转移/Battle→Hub 回流全路径过此),此处 Begin(HUB) 一处覆盖全部。
	// hub 无独立实例纪元语义,以 ProtocolEpoch 充当(census Admit 侧同源,exact 等值自洽)。
	//
	// **写不进 owner 就不发票**:票据是把"已由权威算好的判定结果"搬到 DS 的唯一通道(§9.3),
	// 归属还没定案就签票,等于把一张无权威背书的入场券交出去——玩家可能同时被两台 DS 认领。
	// 四个调用方都已能安全承接这个错误:signResult/transferResult 上抛(客户端退避重查),
	// 两条 migrate 扫描路径回源索引 / 补偿座位后下个 tick 重试,不漏座位也不卡玩家。
	//
	// 预算 3s:单玩家最多 2 轮 (Query+Begin)(EPOCH_CONFLICT 重查一次),留足跨服务往返与
	// 一次 TiDB 事务的余量。超时按失败处理,调用方重试——待实测复核。
	target, targetOK := u.ownerTargetForHubTicket(ctx, assignment)
	if !targetOK && u.authRepo != nil {
		// Model B 下 exact 身份必然可得(上方 assignmentBindingV2Complete 已保证);取不出
		// 说明数据不自洽,照 §9.22 fail-closed,绝不用不完整身份去写权威。
		return "", 0, errcode.New(errcode.ErrInvalidState,
			"refuse to sign hub ticket without exact owner identity")
	}
	if targetOK {
		if err := ownerBeginPlayer(ctx, u.ownerAuth, playerID, ownerTypeHub, target, 3*time.Second); err != nil {
			plog.With(ctx).Warnw("msg", "hub_ticket_refused_owner_begin_failed",
				"player_id", playerID, "pod", target.PodName, "err", err,
				"hint", "归属未定案不签票(§9.3/§9.22);调用方重试")
			return "", 0, err
		}
	}
	return token, expMs, nil
}

// resolveOwnerTargetFromAssignment 从归属镜像重建 owner Begin 目标(census 自愈路径,
// 复审 P1-3)。与 signHubTicket 的 Begin 目标同源(ticketBindingFromAssignment),保证
// exact 等值自洽;绑定不完整/无归属 → (zero, false) 不自愈。
func (u *HubUsecase) resolveOwnerTargetFromAssignment(ctx context.Context, playerID uint64) (data.OwnerTargetView, bool) {
	assignment, found, err := u.repo.GetAssignment(ctx, playerID)
	if err != nil || !found {
		return data.OwnerTargetView{}, false
	}
	binding := ticketBindingFromAssignment(assignment)
	if binding.PodName == "" || binding.InstanceUID == "" {
		return data.OwnerTargetView{}, false
	}
	return data.OwnerTargetView{
		PodName:                  binding.PodName,
		InstanceUID:              binding.InstanceUID,
		InstanceEpoch:            binding.ProtocolEpoch,
		AssignmentOrAllocationID: binding.HubAssignmentID,
		ReleaseTrack:             binding.ReleaseTrack,
	}, true
}

func (u *HubUsecase) signResult(ctx context.Context, playerID uint64, roleID uint32, assignment *hubv1.HubAssignmentStorageRecord, sourceMatchID uint64, sessionJTI string) (*AssignResult, error) {
	token, expMs, err := u.signHubTicket(ctx, playerID, roleID, assignment, sourceMatchID, sessionJTI)
	if err != nil {
		return nil, err
	}
	return &AssignResult{
		HubDSAddr:   assignment.HubAddr,
		HubTicket:   token,
		HubPodName:  assignment.HubPodName,
		ShardID:     assignment.ShardId,
		TicketExpMs: expMs,
	}, nil
}

func (u *HubUsecase) transferResult(ctx context.Context, playerID uint64, roleID uint32, assignment *hubv1.HubAssignmentStorageRecord) (*TransferResult, error) {
	// Hub→Hub 切换/重签:玩家已在大厅,无 Battle 回流 fence。
	// R7 复审 P0-3:重签票绑定**请求方**会话 jti(Envoy 验签 payload 头,中间件提取)。
	// 用请求方自证的 jti 而非权威当前代:若调用方已被顶,签出的票携带旧 jti,
	// 兑换点必拒——绝不把绑定新会话的有效票交给旧设备。空(内网直连/dev 无证据)
	// → 签空 sjti,prod 兑换点硬拒,dev(无会话权威)放行。
	token, expMs, err := u.signHubTicket(ctx, playerID, roleID, assignment, 0, pmw.SessionJTIFromContext(ctx))
	if err != nil {
		return nil, err
	}
	return &TransferResult{
		NewHubDSAddr:    assignment.HubAddr,
		NewHubTicket:    token,
		NewHubPodName:   assignment.HubPodName,
		TicketExpMs:     expMs,
		NewAssignmentID: assignment.GetAssignmentId(),
	}, nil
}

// stickyReleaseTrack 是 additive rollout 的唯一旧值迁移规则：空轨按 stable 解释；
// 任意其他未知值 fail-closed，绝不被 cohort 策略重算。
func stickyReleaseTrack(track string) (string, error) {
	if track == "" {
		return releasetrack.Stable, nil
	}
	if !releasetrack.Valid(track) {
		return "", errcode.New(errcode.ErrInvalidState, "invalid persisted hub release_track %q", track)
	}
	return track, nil
}

// selectTransferTarget:targetHubID!=0 点名 shard_id 匹配的分片;否则同 region 最空「非当前」ready 分片。
func selectTransferTarget(shards []*hubv1.HubShardStorageRecord, cur *hubv1.HubAssignmentStorageRecord, targetHubID uint64) *hubv1.HubShardStorageRecord {
	curTrack, curTrackErr := stickyReleaseTrack(cur.GetReleaseTrack())
	if curTrackErr != nil {
		return nil
	}
	if targetHubID != 0 {
		if targetHubID > math.MaxUint32 {
			// shard_id 是 uint32(配置 ID),超出范围必然无匹配,直接返回避免截断误匹配
			return nil
		}
		want := uint32(targetHubID)
		for _, s := range shards {
			track, trackErr := stickyReleaseTrack(s.GetReleaseTrack())
			if trackErr == nil && track == curTrack && s.ShardId == want && s.Region == cur.Region && s.State == stateReady {
				if s.HubPodName == cur.HubPodName || s.PlayerCount < s.Capacity {
					return s
				}
			}
		}
		return nil
	}
	return leastLoaded(shards, cur.Region, curTrack, cur.HubPodName)
}

// leastLoaded:返回 region 内最空的 ready 且未满分片;excludePod 非空时排除它。并列取 shard_id 小者。
func leastLoaded(shards []*hubv1.HubShardStorageRecord, region, releaseTrack, excludePod string) *hubv1.HubShardStorageRecord {
	var best *hubv1.HubShardStorageRecord
	for _, s := range shards {
		track, trackErr := stickyReleaseTrack(s.GetReleaseTrack())
		if trackErr != nil || track != releaseTrack || s.Region != region || s.State != stateReady || s.PlayerCount >= s.Capacity {
			continue
		}
		if excludePod != "" && s.HubPodName == excludePod {
			continue
		}
		if best == nil || s.PlayerCount < best.PlayerCount ||
			(s.PlayerCount == best.PlayerCount && s.ShardId < best.ShardId) {
			best = s
		}
	}
	return best
}

// autoScaleEnabled 需同时满足:配置开启 + 存在真实 Fleet scaler。
// scaler 只有真 Agones provider(AgonesHubFleetProvider)才实现 HubFleetScaler;
// Mock provider 是拓扑-only 不实现该接口 → Mock 模式下 scaler==nil,
// 自动扩缩容/强制整合恒不运行(不会跑退化 no-op 误导评估)。
func (u *HubUsecase) autoScaleEnabled() bool {
	return u.cfg.AutoScaleEnabled && u.scaler != nil
}

// tryScaleOutOnNoCapacity 在当前 region 无可用分片时触发兜底扩容(+1)。
// 触发后调用方仍会返回 ErrHubNoAvailable,由上游重试进新副本。
func (u *HubUsecase) tryScaleOutOnNoCapacity(ctx context.Context, region string) {
	if !u.autoScaleEnabled() {
		return
	}
	current, err := u.scaler.GetFleetReplicas(ctx)
	if err != nil {
		plog.With(ctx).Warnw("msg", "hub_scaleout_get_replicas_failed", "region", region, "err", err)
		return
	}
	desired := current + 1
	if desired < u.cfg.MinReplicas {
		desired = u.cfg.MinReplicas
	}
	if desired > u.cfg.MaxReplicas {
		desired = u.cfg.MaxReplicas
	}
	if desired == current {
		return
	}
	if err := u.scaler.SetFleetReplicas(ctx, desired); err != nil {
		plog.With(ctx).Warnw("msg", "hub_scaleout_set_replicas_failed",
			"region", region, "current", current, "desired", desired, "err", err)
		return
	}
	plog.With(ctx).Infow("msg", "hub_scaleout_triggered", "region", region, "from", current, "to", desired)
}

// reconcileFleetReplicas 周期性副本治理(每个 sweep tick 调一次):
//   - ① 扩容(立即,仅向上):总在线 > 0 → ceil(total/players_per_hub) > current 时扩容
//   - ② 强制整合(可选,consolidation_enabled):ready 分片多于负载所需 → 排空最空的多余分片,
//     把分片上的玩家做服务端权威搬迁到目标分片,并下发迁移通知(Hub DS drain 心跳 + Kafka 推送双通道)
//   - ③ 回收 + 缩容:已排空且过 grace 的 draining 分片 → 删镜像 + 把 Fleet 副本降到仍需存活的分片数
//
// 缩容到副本数后由 Agones 决定删哪个 GameServer(可能不是被排空那个),这是当前阶段的已知限制
// (docs/design/agones-dev.md):缩容只在 draining 分片已排空且过 grace 后触发,被删 pod 已无在场玩家。
func (u *HubUsecase) reconcileFleetReplicas(ctx context.Context) error {
	if !u.autoScaleEnabled() {
		return nil
	}
	shards, err := u.repo.ListShards(ctx)
	if err != nil {
		return err
	}

	totalPlayers := sumPlayers(shards)

	current, err := u.scaler.GetFleetReplicas(ctx)
	if err != nil {
		return err
	}

	minReplicas := u.cfg.MinReplicas
	maxReplicas := u.cfg.MaxReplicas
	playersPerHub := u.cfg.PlayersPerHub
	if playersPerHub <= 0 {
		playersPerHub = 500
	}

	// 负载所需 ready 分片数(总在线=0 → min)。
	need := minReplicas
	if totalPlayers > 0 {
		// 在 int64 内先夹紧到 maxReplicas 再转 int32,防止 totalPlayers 极大时
		// 除法结果超 int32 范围转换回绕成负数
		needed := (totalPlayers + int64(playersPerHub) - 1) / int64(playersPerHub)
		if needed > int64(maxReplicas) {
			needed = int64(maxReplicas)
		}
		need = int32(needed)
		if need < minReplicas {
			need = minReplicas
		}
	}

	// ① 扩容(立即,仅向上)。扩容当 tick 不再缩容,等新 pod ready 后下个 tick 再治理。
	if need > current {
		if serr := u.scaler.SetFleetReplicas(ctx, need); serr != nil {
			return serr
		}
		plog.With(ctx).Infow("msg", "hub_fleet_scaled_out",
			"current", current, "desired", need, "players", totalPlayers,
			"players_per_hub", playersPerHub, "min", minReplicas, "max", maxReplicas)
		return nil
	}

	// ② 排空多余分片(标 draining + 盖 draining_since_ms,统一交 ③ 回收):
	//   - 总在线>0 且开启强制整合:搬迁最空多余分片的玩家到目标分片再排空。
	//   - 总在线=0:把超出 min_replicas 的空 ready 分片标 draining 盖戳。
	//     必须盖戳走回收路径删镜像 —— 否则直接把 Fleet 缩到 min 后,Agones 删掉的 pod
	//     只会被心跳超时扫成「无 draining_since_ms」的 draining 分片,reclaimDrainedShards
	//     跳过它,镜像就成了不可回收的 stale shard 永久残留在 shards 集合里。
	drained := false
	if totalPlayers > 0 && u.consolidationEnabled() {
		drained = u.consolidateOnce(ctx, shards, need)
	} else if totalPlayers == 0 {
		drained = u.drainEmptyShards(ctx, shards, minReplicas)
	}
	if drained {
		if fresh, ferr := u.repo.ListShards(ctx); ferr == nil {
			shards = fresh // 重读快照供回收判断
		}
	}

	// ③ 回收已排空且过 grace 的 draining 分片 + 缩容(只在镜像回收后才把 Fleet 降到存活分片数,
	// 保持 Fleet 副本数与镜像一致,避免缩 Fleet 后留下不可回收的 stale 镜像)。
	reclaimed := u.reclaimDrainedShards(ctx, shards)
	if reclaimed == 0 {
		// Never derive scale-in authority from a missing/short Redis mirror list.
		// Exact per-instance teardown is the only future path that may return a
		// positive reclaimed count.
		return nil
	}
	live := int32(len(shards)) - reclaimed
	desired := current
	target := live
	if target < need {
		target = need
	}
	if target < minReplicas {
		target = minReplicas
	}
	if target > maxReplicas {
		target = maxReplicas
	}
	if target < current {
		desired = target // 只在此处缩容
	}

	if desired != current {
		if serr := u.scaler.SetFleetReplicas(ctx, desired); serr != nil {
			return serr
		}
		plog.With(ctx).Infow("msg", "hub_fleet_scaled_in",
			"current", current, "desired", desired, "players", totalPlayers,
			"reclaimed", reclaimed, "min", minReplicas, "max", maxReplicas)
	}
	return nil
}

// consolidationEnabled 强制整合开关(需自动扩缩容已开)。
// 不强制要求 migrate pusher:即便没接 Kafka,服务端权威搬迁 + Hub DS drain 心跳仍能让玩家重连到新分片。
func (u *HubUsecase) consolidationEnabled() bool {
	return u.autoScaleEnabled() && u.cfg.ConsolidationEnabled
}

// consolidateOnce:ready 分片多于 need 时,把最空的多余分片标 draining 并搬迁其玩家。
// 返回是否有分片被排空(供调用方决定是否重读快照)。
func (u *HubUsecase) consolidateOnce(ctx context.Context, shards []*hubv1.HubShardStorageRecord, need int32) bool {
	ready := make([]*hubv1.HubShardStorageRecord, 0, len(shards))
	for _, s := range shards {
		if s.State == stateReady {
			ready = append(ready, s)
		}
	}
	if int32(len(ready)) <= need {
		return false // 没有多余 ready 分片
	}
	// 按负载升序(并列 shard_id 小者优先)排,排空最空的多余分片(保留最满的 need 个分片承接玩家)。
	sort.Slice(ready, func(i, j int) bool {
		if ready[i].PlayerCount != ready[j].PlayerCount {
			return ready[i].PlayerCount < ready[j].PlayerCount
		}
		return ready[i].ShardId < ready[j].ShardId
	})
	surplus := ready[:int32(len(ready))-need] // 升序前段=最空的多余分片
	drained := false
	for _, s := range surplus {
		if u.drainAndMigrate(ctx, s) {
			drained = true
		}
	}
	return drained
}

// drainEmptyShards:大厅没人(总在线=0)时,把超出 keep 的空 ready 分片标 draining + 盖戳,
// 交 reclaimDrainedShards 统一回收镜像(见 reconcileFleetReplicas ② 的说明)。返回是否有分片被排空。
func (u *HubUsecase) drainEmptyShards(ctx context.Context, shards []*hubv1.HubShardStorageRecord, keep int32) bool {
	ready := make([]*hubv1.HubShardStorageRecord, 0, len(shards))
	for _, s := range shards {
		if s.State == stateReady {
			ready = append(ready, s)
		}
	}
	if int32(len(ready)) <= keep {
		return false // 不超过保底,无需排空
	}
	// 保留 shard_id 最小的 keep 个,排空其余空分片(全空,排序仅取确定性)。
	sort.Slice(ready, func(i, j int) bool { return ready[i].ShardId < ready[j].ShardId })
	surplus := ready[keep:]
	drained := false
	for _, s := range surplus {
		if u.drainAndMigrate(ctx, s) {
			drained = true
		}
	}
	return drained
}

// drainAndMigrate:把分片标记 draining(盖时间戳)并服务端权威搬迁其在册玩家到目标分片。
// 单 tick 每分片最多搬 ConsolidationBatch 人(防抢占),剩余留下个 tick 续搬。
func (u *HubUsecase) drainAndMigrate(ctx context.Context, shard *hubv1.HubShardStorageRecord) bool {
	now := time.Now().UnixMilli()
	merr := u.repo.UpdateShardWithLock(ctx, shard.HubPodName, u.retry(), func(s *hubv1.HubShardStorageRecord) error {
		if s.State == stateReady {
			s.State = stateDraining
			s.DrainingSinceMs = now
		}
		return nil
	}, u.shardTTL())
	if merr != nil && errcode.As(merr) != errcode.ErrHubNoAvailable {
		plog.With(ctx).Warnw("msg", "drain_mark_failed", "pod", shard.HubPodName, "err", merr)
	}

	members, lerr := u.repo.ListShardMembers(ctx, shard.HubPodName)
	if lerr != nil {
		plog.With(ctx).Warnw("msg", "drain_list_members_failed", "pod", shard.HubPodName, "err", lerr)
	}
	// 成员反向索引是 best-effort 优化(只在 AssignHub/TransferHub 维护):部署前已在线、索引里
	// 没有的老玩家不会被这里服务端权威搬迁,而是靠 Hub DS drain 心跳兜底 —— 客户端收到 drain 指令
	// 后重连 AssignHub,幂等路径发现旧分片非 ready 即释放旧位重分到 ready 分片,旧分片 player_count
	// 随之递减,最终仍可被回收。member<player_count 时这里只少了对老玩家的无缝推送,不影响最终一致性。
	// 索引数明显少于在册人数时告警,便于观测首次整合的降级范围(详见 docs/design/agones-dev.md §2.2)。
	if shard.PlayerCount > 0 && len(members) < int(shard.PlayerCount) {
		plog.With(ctx).Warnw("msg", "drain_members_index_incomplete",
			"pod", shard.HubPodName, "indexed", len(members), "player_count", shard.PlayerCount)
	}

	fresh, ferr := u.repo.ListShards(ctx)
	if ferr != nil {
		plog.With(ctx).Warnw("msg", "drain_list_shards_failed", "pod", shard.HubPodName, "err", ferr)
		return true // 已标 draining,搬迁留下个 tick
	}
	fresh, ferr = u.routableShardViews(ctx, fresh)
	if ferr != nil {
		plog.With(ctx).Warnw("msg", "drain_authoritative_routes_failed", "pod", shard.HubPodName, "err", ferr)
		return true
	}

	batch := u.cfg.ConsolidationBatch
	if batch <= 0 {
		batch = 50
	}
	moved := 0
	for _, pid := range members {
		if moved >= batch {
			break
		}
		track, trackErr := stickyReleaseTrack(shard.GetReleaseTrack())
		if trackErr != nil {
			plog.With(ctx).Warnw("msg", "drain_invalid_release_track", "pod", shard.HubPodName, "err", trackErr)
			break
		}
		target := leastLoaded(fresh, shard.Region, track, shard.HubPodName)
		if target == nil {
			plog.With(ctx).Warnw("msg", "drain_no_target", "pod", shard.HubPodName, "region", shard.Region)
			break // 无空闲目标分片,留下个 tick
		}
		if u.migratePlayer(ctx, pid, shard, target) {
			moved++
			target.PlayerCount++ // 本地快照计数同步,均衡后续选择
		}
	}
	plog.With(ctx).Infow("msg", "hub_shard_draining",
		"pod", shard.HubPodName, "region", shard.Region, "members", len(members), "moved", moved)
	return true
}

// migratePlayer:把单个玩家从 from 分片服务端权威搬迁到 target 分片(镜像 TransferHub 的占位/切归属/退位顺序),
// 重签 hub 票据并推送 HubMigrateEvent(best-effort)。返回是否搬迁成功。
func (u *HubUsecase) migratePlayer(ctx context.Context, playerID uint64, from, target *hubv1.HubShardStorageRecord) bool {
	// 复核玩家仍在 from 分片(避免与玩家自身 Release/Transfer 竞争)。
	assign, found, err := u.repo.GetAssignment(ctx, playerID)
	if err != nil {
		// 读权威失败 ≠ 玩家已离开:此时删索引会让该玩家永远退出 drain 扫描(§9.22
		// UNKNOWN 不得当 OFFLINE)。保留索引,下个 tick 重试。
		plog.With(ctx).Warnw("msg", "drain_assignment_read_failed", "player_id", playerID, "err", err)
		return false
	}
	if !found {
		u.removeShardMember(ctx, from.HubPodName, playerID) // 确认已不在此分片,清理残留索引
		return false
	}
	if assign.GetTransferCleanupPending() || assign.GetReleaseCleanupPending() {
		var stillFound bool
		assign, stillFound, err = u.resumeAssignmentCleanup(ctx, playerID, assign.GetAssignmentId())
		if err != nil {
			plog.With(ctx).Warnw("msg", "drain_owner_cleanup_resume_failed", "player_id", playerID, "err", err)
			return false
		}
		if !stillFound {
			u.removeShardMember(ctx, from.HubPodName, playerID)
			return false
		}
	}
	if assign.HubPodName != from.HubPodName {
		if target != nil && assign.GetHubPodName() == target.GetHubPodName() {
			// A crash/physical-departure wait can complete the durable cleanup on a
			// later drain tick. Publish that exact already-selected target instead
			// of treating the old source member as a stale index and losing the only
			// migration notification. Refresh an expired target reservation first.
			//
			// R7 收口(P1):通知未送达之前,玩家必须留在源 member 索引里(drain 扫描的
			// 唯一来源)。上面 resumeAssignmentCleanup 完成 owner cleanup 时会把源索引
			// 一并清掉,因此本分支任何失败路径都要**重新加回**源索引(幂等 best-effort),
			// 否则玩家退出扫描,"下个 tick 重试"永不发生 = 迁移通知永久丢失。
			// 进入条件也不再依赖"本 tick 恰好恢复了 cleanup"(上一次失败尝试可能已把
			// cleanup 做完,pending 位已清):凡「仍在源索引 + 归属已在 drain 目标」都按
			// 补发通知处理;对玩家自迁到同一目标的罕见崩溃残留,重复 migrate 推送指向
			// 其当前精确归属,客户端契约容忍重复,索引随之收敛。
			keepScanned := func() bool {
				u.addShardMember(ctx, from.HubPodName, playerID)
				return false
			}
			current, reusable, routeErr := u.assignmentRoutable(ctx, playerID, assign)
			if routeErr != nil || (!reusable && !assignmentSameInstance(assign, &current)) {
				return keepScanned()
			}
			ensured, ensureErr := u.ensureExistingAssignmentSeat(ctx, playerID, assign, &current)
			if ensureErr != nil {
				return keepScanned()
			}
			next := proto.Clone(assign).(*hubv1.HubAssignmentStorageRecord)
			next.HubAddr, next.ShardId, next.Region = ensured.HubAddr, ensured.ShardID, ensured.Region
			next.ReleaseTrack = ensured.ReleaseTrack
			bindAssignmentAuth(next, ensured)
			swapped, swapErr := u.repo.CompareAndSwapAssignment(ctx, playerID, assign, next,
				u.assignmentSagaTTL())
			if swapErr != nil || !swapped {
				return keepScanned()
			}
			sessJTI, jok := u.migrateResignSessionJTI(ctx, playerID)
			if !jok {
				return keepScanned() // 会话权威不可达:回源索引,下个 tick 重扫补发(迁移已落地)
			}
			token, _, signErr := u.signHubTicket(ctx, playerID, next.GetRoleId(), next, 0, sessJTI)
			if signErr != nil {
				return keepScanned() // 同上:回源索引,重扫重试
			}
			if !u.pushMigrate(ctx, playerID, from, authoritativeShard(target, ensured), token) {
				return keepScanned() // 真实发布失败(R9 复审 P2):回源索引,下个 tick 重签补发
			}
			// 通知路径已走完(发布已确认或功能关闭),此时才清源索引,退出 drain 扫描。
			u.removeShardMember(ctx, from.HubPodName, playerID)
			return true
		}
		// 归属在其它分片(玩家自身 Release/Transfer 已带走),纯陈旧索引,清理。
		u.removeShardMember(ctx, from.HubPodName, playerID)
		return false
	}
	if u.authRepo != nil && !assignmentBindingV2Complete(assign, playerID) {
		plog.With(ctx).Warnw("msg", "drain_migration_rejected_invalid_assignment", "player_id", playerID)
		return false
	}
	newAssignmentID := uuid.NewString()
	seat, rerr := u.reserveRoutableSeat(ctx, target.HubPodName, playerID, newAssignmentID)
	if rerr != nil {
		return false // 目标没位置/非 ready,留下个 tick 重试
	}
	target = authoritativeShard(target, seat)
	now := time.Now().UnixMilli()
	newAssign := proto.Clone(assign).(*hubv1.HubAssignmentStorageRecord)
	newAssign.PlayerId = playerID
	newAssign.HubPodName = target.HubPodName
	newAssign.HubAddr = target.HubAddr
	newAssign.ShardId = target.ShardId
	newAssign.Region = target.Region
	newAssign.TeamId = assign.TeamId
	newAssign.AssignedAtMs = now
	newAssign.RoleId = assign.RoleId // 选角镜像随强制整合搬迁
	newAssign.AssignmentId = newAssignmentID
	newAssign.ReleaseTrack = target.ReleaseTrack
	bindAssignmentAuth(newAssign, seat)
	if u.authRepo != nil && !assignmentBindingV2Complete(newAssign, playerID) {
		plog.With(ctx).Errorw("msg", "migrate_assignment_missing_writer_v2_binding", "player_id", playerID)
		u.compensateReservedSeat(ctx, target.HubPodName, playerID, newAssignmentID, seat)
		return false
	}
	cleanupRegistered := false
	if u.authRepo != nil {
		if cleanupErr := u.registerTransferCleanup(ctx, newAssign, assign); cleanupErr != nil {
			plog.With(ctx).Warnw("msg", "drain_owner_cleanup_register_failed", "player_id", playerID,
				"err", cleanupErr)
			u.compensateReservedSeat(ctx, target.HubPodName, playerID, newAssignmentID, seat)
			return false
		}
		cleanupRegistered = true
	}
	sessJTI, jok := u.migrateResignSessionJTI(ctx, playerID)
	if !jok {
		u.compensateReservedSeat(ctx, target.HubPodName, playerID, newAssignmentID, seat)
		return false // 会话权威不可达:fail-closed,下个 tick 重试
	}
	token, _, terr := u.signHubTicket(ctx, playerID, assign.RoleId, newAssign, 0, sessJTI)
	if terr != nil {
		plog.With(ctx).Warnw("msg", "migrate_sign_ticket_failed", "player_id", playerID, "err", terr)
		u.compensateReservedSeat(ctx, target.HubPodName, playerID, newAssignmentID, seat)
		return false
	}
	swapped, serr := u.repo.CompareAndSwapAssignment(ctx, playerID, assign, newAssign, u.assignmentSagaTTL())
	if serr != nil {
		if !cleanupRegistered {
			u.compensateReservedSeat(ctx, target.HubPodName, playerID, newAssignmentID, seat)
		}
		return false
	}
	if !swapped {
		if cleanupRegistered {
			u.removeTransferCleanupRef(ctx, assign.GetHubPodName(), transferCleanupRef(newAssign))
		}
		u.compensateReservedSeat(ctx, target.HubPodName, playerID, newAssignmentID, seat)
		return false
	}
	u.addShardMember(ctx, target.HubPodName, playerID)
	if cleanupRegistered {
		var stillFound bool
		_, stillFound, err = u.resumeAssignmentCleanup(ctx, playerID, newAssignmentID)
		if err != nil || !stillFound {
			plog.With(ctx).Warnw("msg", "drain_migration_owner_cleanup_failed",
				"player_id", playerID, "from", from.HubPodName, "to", target.HubPodName, "err", err)
			// R7 收口(P1):CAS 已落地但 cleanup/通知未完成;cleanup 可能已部分清掉源
			// member 索引,重新加回保证下个 tick 仍能扫到该玩家补发通知(幂等 best-effort)。
			u.addShardMember(ctx, from.HubPodName, playerID)
			return false
		}
	} else {
		u.releaseAssignmentSeat(ctx, assign)
		u.removeShardMember(ctx, from.HubPodName, playerID)
	}

	// 通知仍是异步交付;若发布失败,把玩家加回源 member 索引(R9 复审 P2):迁移已
	// 落地,下个 tick「归属已在 drain 目标」分支会重签票据并补发通知;Login 从 durable
	// assignment 重签恢复仍是最终兜底,但不再把「发布失败」静默当作已送达。
	if !u.pushMigrate(ctx, playerID, from, target, token) {
		u.addShardMember(ctx, from.HubPodName, playerID)
		return false
	}
	return true
}

// pushMigrate 推送 HubMigrateEvent 给被迁移玩家(migrate pusher 未接时静默跳过)。
// 返回是否可视为「通知路径已走完」(R9 复审 P2):pusher 未装配=true(功能关闭,
// 兜底链接管);真实发布失败=false,调用方必须把玩家留在/加回源 member 索引,
// 下个 tick「归属已在 drain 目标」分支重签重发,不得静默丢失唯一迁移通知。
func (u *HubUsecase) pushMigrate(ctx context.Context, playerID uint64, from, target *hubv1.HubShardStorageRecord, token string) bool {
	if u.migrate == nil {
		return true
	}
	ev := &hubv1.HubMigrateEvent{
		PlayerId:     playerID,
		FromHubPod:   from.HubPodName,
		ToHubDsAddr:  target.HubAddr,
		ToHubTicket:  token,
		ToHubPodName: target.HubPodName,
		ToShardId:    target.ShardId,
		GraceSeconds: u.cfg.MigrateGraceSeconds,
		Reason:       migrateReasonConsolidation,
		TsMs:         time.Now().UnixMilli(),
	}
	payload, merr := proto.Marshal(ev)
	if merr != nil {
		plog.With(ctx).Warnw("msg", "migrate_marshal_failed", "player_id", playerID, "err", merr)
		return false
	}
	if perr := u.migrate.PushMigrate(ctx, playerID, payload); perr != nil {
		plog.With(ctx).Warnw("msg", "migrate_push_failed", "player_id", playerID, "err", perr)
		return false
	}
	return true
}

// reclaimDrainedShards intentionally does not erase a mirror merely because a
// logical drain/grace timer completed.  Fleet scale-in does not select a
// particular GameServer, and Kubernetes DELETE acceptance is not process
// teardown.  Until scale-in is upgraded to an exact per-instance teardown
// saga, correctness wins over resource reclamation: the physical-owner fence
// stays durable and this function reports zero reclaimed replicas.
func (u *HubUsecase) reclaimDrainedShards(ctx context.Context, shards []*hubv1.HubShardStorageRecord) int32 {
	graceMs := int64(u.cfg.MigrateGraceSeconds) * 1000
	now := time.Now().UnixMilli()
	for _, s := range shards {
		if s.State != stateDraining || s.PlayerCount > 0 || s.DrainingSinceMs <= 0 {
			continue
		}
		if now-s.DrainingSinceMs < graceMs {
			continue // 未过 grace,保持 pod 存活让在场玩家完成倒计时切换
		}
		plog.With(ctx).Warnw("msg", "hub_scalein_waiting_exact_instance_teardown",
			"pod", s.HubPodName, "region", s.Region, "gameserver_uid", s.GameserverUid)
	}
	return 0
}

// addShardMember / removeShardMember:成员反向索引维护(best-effort,失败仅 Warn 不阻断主流程)。
func (u *HubUsecase) addShardMember(ctx context.Context, pod string, playerID uint64) {
	if err := u.repo.AddShardMember(ctx, pod, playerID, u.assignmentSagaTTL()); err != nil {
		plog.With(ctx).Warnw("msg", "add_shard_member_failed", "pod", pod, "player_id", playerID, "err", err)
	}
}

func (u *HubUsecase) removeShardMember(ctx context.Context, pod string, playerID uint64) {
	if err := u.repo.RemoveShardMember(ctx, pod, playerID); err != nil {
		plog.With(ctx).Warnw("msg", "remove_shard_member_failed", "pod", pod, "player_id", playerID, "err", err)
	}
}

// sumPlayers 汇总分片在册人数(负数视为 0)。
func sumPlayers(shards []*hubv1.HubShardStorageRecord) int64 {
	var total int64
	for _, s := range shards {
		if s.PlayerCount > 0 {
			total += int64(s.PlayerCount)
		}
	}
	return total
}
